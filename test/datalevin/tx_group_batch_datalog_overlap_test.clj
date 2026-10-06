(ns datalevin.tx-group-batch-datalog-overlap-test
  (:require [clojure.test :refer [deftest is]]
            [datalevin.core :as d]
            [datalevin.constants :as c]
            [datalevin.interface :as i]
            [datalevin.lmdb :as l]
            [datalevin.storage :as s]
            [datalevin.tx-group.batch :as batch]
            [datalevin.tx-group.batch.embedded :as embedded]
            [datalevin.tx-group.phase :as phase]
            [datalevin.txlog :as wal]
            [datalevin.util :as u])
  (:import [java.io IOException]
           [java.util.concurrent CountDownLatch TimeUnit]))

(def ^:private opts
  {:wal? true :wal-durability-profile :strict :wal-segment-prealloc? false
   :snapshot-scheduler? false :background-sampling? false})

(defn- with-conn [opts f]
  (let [dir (u/tmp-dir (str "datalog-native-overlap-" (random-uuid)))
        conn (d/create-conn dir {:value {:db/valueType :db.type/long}} opts)]
    (try (f conn dir)
         (finally (d/close conn) (u/delete-files dir)))))

(defn- collected! [conn ops]
  (let [collector (get-in @(i/kv-info (d/datalog-kv conn)) [:independent-control :collector])
        queue (.-ready ^datalevin.tx_group.batch.Collector collector)
        entered (promise) release (promise) jobs (atom [])
        previous (phase/current-observer)
        unobserve (phase/observe!
                    (fn [event b]
                      (when (and (= :batch-sealed event)
                                 (identical? collector (batch/batch-collector b))
                                 (not (realized? entered)))
                        (deliver entered true)
                        (assert (deref release 10000 false)))
                      (when previous (previous event b))))]
    (try
      (doseq [[idx op] (map-indexed vector ops)]
        (swap! jobs conj (future (try (op) (catch Throwable t t))))
        (when (zero? (long idx)) (is (deref entered 10000 false)))
        (is (loop [attempt 0]
              (cond (= (dec (count @jobs)) (.size ^java.util.Queue queue)) true
                    (= attempt 1000) false
                    :else (do (Thread/sleep 5) (recur (inc attempt)))))))
      (deliver release true)
      (mapv #(deref % 10000 ::timeout) @jobs)
      (finally (deliver release true)
               (doseq [job @jobs] (deref job 10000 nil))
               (unobserve)))))

(deftest resolved-batch-overlaps-general-and-dependent-requests
  (let [dir (u/tmp-dir (str "datalog-resolved-batch-" (random-uuid)))
        conn (d/create-conn dir
                            {:value {:db/valueType :db.type/long}
                             :key {:db/unique :db.unique/identity}
                             :tags {:db/cardinality :db.cardinality/many}
                             :link {:db/valueType :db.type/ref}} opts)]
    (try
      (d/transact! conn [{:db/id 1 :key "a" :value 10 :tags #{"old"}}
                         {:db/id 2 :key "b" :value 20 :link 1}])
      (let [wal-started (CountDownLatch. 1) native-done (CountDownLatch. 1)
            events (atom [])
            unobserve (phase/observe!
                        (fn [event _]
                          (swap! events conj event)
                          (case event
                            :native-tail-start (is (.await wal-started 5 TimeUnit/SECONDS))
                            :native-tail-applied (.countDown native-done)
                            :wal-start (do (.countDown wal-started)
                                           (assert (.await native-done 5 TimeUnit/SECONDS)))
                            nil)))]
        (try
          (let [reports (collected! conn
                                   [#(d/transact! conn [[:db/add 1 :value 11]])
                                    #(d/transact! conn [[:db.fn/cas 1 :value 11 12]])
                                    #(d/transact! conn [[:db/retract 1 :value 12]
                                                       [:db/retract 1 :key "a"]
                                                       [:db/retract 1 :tags "old"]])
                                    #(d/transact! conn [{:db/id 1 :value 10 :key "c" :tags #{"new"}}])
                                    #(d/transact! conn [[:db/add [:key "c"] :value 13]])
                                    #(d/transact! conn [{:db/id -1 :key "a" :value 30 :link [:key "c"]}])
                                    #(d/transact! conn [[:db/retractEntity 1]])
                                    #(d/transact! conn [[:db/add 1 :value 99]])])]
            (is (every? map? reports) (pr-str reports))
            (is (= 1 (count (filter #{:native-tail-start} @events))))
            (is (= 1 (count (filter #{:worker-dispatch} @events))))
            (is (= 99 (:value (d/entity @conn 1))))
            (is (nil? (:key (d/entity @conn 1))))
            (is (nil? (:tags (d/entity @conn 1))))
            (is (nil? (:link (d/entity @conn 2))))
            (is (= 30 (:value (d/entity @conn [:key "a"]))))
            (is (nil? (:link (d/entity @conn [:key "a"])))))
          (finally (unobserve))))
      (d/close conn)
      (let [reopened (d/create-conn dir)]
        (try (is (= 99 (:value (d/entity @reopened 1))))
             (is (= 30 (:value (d/entity @reopened [:key "a"]))))
             (finally (d/close reopened))))
      (finally (d/close conn) (u/delete-files dir)))))

(deftest transaction-functions-drain-preceding-resolved-writes-before-native-reads
  (with-conn opts
    (fn [conn _]
      (d/transact! conn [{:db/id 1 :value 0}])
      (let [calls (atom 0)
            reports (collected! conn
                               [#(d/transact! conn [[:db/add 1 :value 1]])
                                #(d/transact! conn
                                              [[:db.fn/call
                                                (fn [tx-db]
                                                  (swap! calls inc)
                                                  (is (= 1 (d/q '[:find ?v . :where [1 :value ?v]] tx-db)))
                                                  [[:db/add 1 :value 2]])]])
                                #(d/transact! conn [[:db/add 1 :value 3]])])]
        (is (every? map? reports) (pr-str reports))
        (is (= 1 @calls))
        (is (= 3 (:value (d/entity @conn 1))))))))

(deftest batch-encoding-refreshes-after-schema-changes
  (with-conn opts
    (fn [conn dir]
      (d/transact! conn [{:db/id 1 :value 0}])
      (let [reports (collected! conn
                               [#(d/transact! conn [[:db/retract 1 :value 0]])
                                #(d/with-transaction [tx conn]
                                   (d/update-schema tx {:value {:db/noindex true}})
                                   (d/transact! tx [[:db/add 1 :value 2]]))])]
        (is (every? map? reports) (pr-str reports))
        (is (= 2 (:value (d/entity @conn 1))))
        (is (zero? (i/entries (d/datalog-kv conn) c/ave))))
      (d/close conn)
      (let [reopened (d/create-conn dir)]
        (try (is (= 2 (:value (d/entity @reopened 1))))
             (is (zero? (i/entries (d/datalog-kv reopened) c/ave)))
             (finally (d/close reopened)))))))

(deftest resolved-batch-retains-false-scalar-values
  (with-conn opts
    (fn [conn _]
      (d/update-schema conn {:flag {:db/valueType :db.type/boolean}})
      (d/transact! conn [{:db/id 1 :flag true}])
      (let [reports (collected! conn
                               [#(d/transact! conn [[:db/add 1 :flag false]])
                                #(d/transact! conn [[:db/add 1 :flag true]])
                                #(d/transact! conn [[:db/add 1 :flag false]])])]
        (is (every? map? reports) (pr-str reports))
        (is (some #(and (= false (:v %)) (not (:added %)))
                  (:tx-data (second reports))))
        (is (= false (:flag (d/entity @conn 1))))
        (is (= #{[false]} (d/q '[:find ?v :where [1 :flag ?v]] @conn)))
        (is (= 1 (i/entries (d/datalog-kv conn) c/ave)))))))

(deftest collected-scalars-share-one-storage-preparation
  (with-conn opts
    (fn [conn _]
      (d/transact! conn [{:db/id 1 :value 0}])
      (let [before-tx (:max-tx @conn)
            prepared-counts (atom [])
            prepare @#'s/prepare-datoms-kv-plan
            reports (with-redefs-fn
                      {#'s/prepare-datoms-kv-plan
                       (fn [store datoms & args]
                         (when (= 5 (count args))
                           (swap! prepared-counts conj (count datoms)))
                         (apply prepare store datoms args))}
                      #(collected! conn
                                   (mapv (fn [value]
                                           (fn []
                                             (d/transact! conn [[:db/add 1 :value value]]
                                                          {:request value})))
                                         (range 1 9))))
            tx-ids (mapv #(get-in % [:tempids :db/current-tx]) reports)
            lmdb (d/datalog-kv conn)]
        (is (every? map? reports) (pr-str reports))
        (is (= [16] @prepared-counts))
        (is (= (vec (range (inc (long before-tx)) (+ (long before-tx) 9))) tx-ids))
        (is (= (mapv #(hash-map :request %) (range 1 9)) (mapv :tx-meta reports)))
        (doseq [[value report] (map vector (range 1 9) reports)]
          (is (= #{[(dec value) false] [value true]}
                 (set (map (juxt :v :added) (:tx-data report)))))
          (is (every? #(= (get-in report [:tempids :db/current-tx]) (:tx %))
                      (:tx-data report))))
        (is (= 8 (:value (d/entity @conn 1))))
        (is (= (last tx-ids) (:max-tx @conn)
               (i/get-value lmdb c/meta :max-tx :attr :long)))
        (is (= (i/last-modified (:store @conn))
               (i/get-value lmdb c/meta :last-modified :attr :long)))))))

(deftest collected-giant-datoms-retain-cross-request-order
  (let [dir (u/tmp-dir (str "datalog-batched-giants-" (random-uuid)))
        conn (d/create-conn dir {:text {:db/valueType :db.type/string}} opts)
        prefix (.repeat "shared" 300)
        old (str prefix "old")
        final (str prefix "final")
        check! (fn [conn]
                 (let [lmdb (d/datalog-kv conn)]
                   (is (= final (:text (d/entity @conn 1))))
                   (is (= [[1 final]] (mapv (juxt :e :v) (d/datoms @conn :eav 1 :text))))
                   (is (= [[1 final]] (mapv (juxt :e :v) (d/datoms @conn :ave :text))))
                   (is (empty? (d/q '[:find ?e :in $ ?v :where [?e :text ?v]]
                                    @conn old)))
                   (is (= #{[1]} (d/q '[:find ?e :in $ ?v :where [?e :text ?v]]
                                      @conn final)))
                   (is (= 1 (i/entries lmdb c/eav)))
                   (is (= 1 (i/entries lmdb c/ave)))
                   (is (pos? (i/entries lmdb c/giants)))))]
    (try
      (let [reports (collected! conn
                               [#(d/transact! conn [[:db/add 1 :text old]])
                                #(d/transact! conn [[:db/retract 1 :text old]])
                                #(d/transact! conn [[:db/add 1 :text final]])])]
        (is (every? map? reports) (pr-str reports))
        (is (= [[old true] [old false] [final true]]
               (mapv (fn [report]
                       (let [datom (first (:tx-data report))]
                         [(:v datom) (:added datom)]))
                     reports))))
      (check! conn)
      (d/close conn)
      (let [reopened (d/create-conn dir)]
        (try (check! reopened) (finally (d/close reopened))))
      (finally (d/close conn) (u/delete-files dir)))))

(deftest caught-storage-drain-failures-still-abort-the-whole-batch
  (doseq [infrastructure? [false true]]
    (with-conn opts
      (fn [conn _]
        (d/transact! conn [{:db/id 1 :value 0}])
        (let [lmdb (d/datalog-kv conn)
              control (:independent-control @(i/kv-info lmdb))
              collector (:collector control)
              state (wal/state lmdb)
              before @conn
              before-lsn @(:next-lsn state)
              injected? (atom false)
              caught (atom [])
              successors (atom 0)
              failure (if infrastructure?
                        (IOException. "Forced late storage failure")
                        (ex-info "Forced late storage rejection" {}))
              prepare @#'s/prepare-datoms-kv-plan
              results
              (with-redefs-fn
                {#'s/prepare-datoms-kv-plan
                 (fn [store datoms & args]
                   (if (and (= 5 (count args)) (last args)
                            (compare-and-set! injected? false true))
                     (throw failure)
                     (apply prepare store datoms args)))}
                (fn []
                  (collected! conn
                              [#(d/transact! conn [[:db/add 1 :value 1]])
                               (fn []
                                 ((:internal-body! control)
                                  (fn [native]
                                    (try
                                      ((:native-prepare-flush! (meta native)))
                                      (catch Throwable t (swap! caught conj t)))
                                    [])
                                  {:datalog-conn conn :datalog-prepare? true}))
                               #(d/transact! conn
                                             [[:db.fn/call
                                               (fn [_]
                                                 (swap! successors inc)
                                                 [[:db/add 1 :value 2]])]])])))]
          (is @injected?)
          (is (= 1 (count @caught)))
          (is (every? #(instance? Throwable %) results) (pr-str results))
          (is (zero? @successors))
          (is (identical? before @conn))
          (is (= 0 (:value (d/entity @conn 1))))
          (is (= before-lsn @(:next-lsn state)) "no WAL append after the failed drain")
          (is (zero? (:requests (batch/usage collector))))
          (if infrastructure?
            (do
              (is (not (batch/serving? collector)))
              (is (thrown? Throwable (d/transact! conn [[:db/add 1 :value 3]]))))
            (do
              (is (batch/serving? collector))
              (is (map? (d/transact! conn [[:db/add 1 :value 3]])))
              (is (= 3 (:value (d/entity @conn 1)))))))))))

(deftest no-op-native-read-does-not-inherit-preceding-storage-write-count
  (with-conn (assoc opts :wal-durability-profile :relaxed :wal-shared? false
                        :wal-group-commit 64 :wal-group-commit-ms 0)
    (fn [conn _]
      (d/transact! conn [{:db/id 1 :value 0}])
      (let [lmdb (d/datalog-kv conn)
            state (wal/state lmdb)]
        (i/force-txlog-sync! lmdb)
        (let [before-lsn @(:next-lsn state)
              reports (collected! conn
                                  [#(d/transact! conn [[:db/add 1 :value 1]])
                                   #(d/transact! conn
                                                 [[:db.fn/call
                                                   (fn [tx-db]
                                                     (is (= 1 (:value (d/entity tx-db 1))))
                                                     [])]])
                                   #(d/transact! conn [[:db/add 1 :value 2]])])
              metrics (wal/sync-manager-state (:sync-manager state))]
          (is (every? map? reports) (pr-str reports))
          (is (empty? (:tx-data (second reports))))
          (is (= 2 (:value (d/entity @conn 1))))
          (is (= (inc (long before-lsn)) @(:next-lsn state)))
          (is (= 1 (:pending-count metrics)))
          (is (= 2 (:unsynced-count metrics))))))))

(deftest transaction-functions-can-read-their-own-native-kv-writes
  (with-conn opts
    (fn [conn _]
      (d/open-dbi (d/datalog-kv conn) "aux")
      (let [reports (collected! conn
                               [#(d/transact! conn [{:db/id 1 :value 1}])
                                #(d/transact! conn
                                              [[:db.fn/call
                                                (fn [tx-db]
                                                  (let [native (d/datalog-kv tx-db)]
                                                    (d/transact-kv native [[:put "aux" :key :value]])
                                                    (is (= :value (d/get-value native "aux" :key))))
                                                  [[:db/add 1 :value 2]])]])
                                #(d/transact! conn [[:db/add 1 :value 3]])])]
        (is (every? map? reports) (pr-str reports))
        (is (= 3 (:value (d/entity @conn 1))))
        (is (= :value (d/get-value (d/datalog-kv conn) "aux" :key)))))))

(defn- datoms [^long start ^long n]
  (mapv #(hash-map :db/id % :value %) (range start (+ start n))))

(deftest conditional-native-writes-cannot-enter-the-deferred-tail
  (let [avg (byte-array 4097)
        unconditional (l/datom-kv-tx 1 avg true false false)
        conditional (l/datom-kv-tx 1 avg true true false)
        prepare (fn [rows] (get (#'embedded/prepare nil nil rows :data :data) :native-tail-bytes 0))]
    (is (> (long (prepare [unconditional])) 4096))
    (is (zero? (long (prepare [conditional]))))
    (is (zero? (long (prepare [unconditional
                              [:put c/kv-info :probe 1 :keyword :long [:nooverwrite]]]))))))

(deftest large-frozen-datalog-tail-applies-while-wal-worker-is-running
  (with-conn opts
    (fn [conn dir]
      (d/transact! conn (datoms 1 1))
      (let [wal-started (CountDownLatch. 1) native-done (CountDownLatch. 1)
            bodies (atom 0) events (atom [])
            uninstall (phase/observe!
                        (fn [event _]
                          (swap! events conj event)
                          (case event
                            :native-tail-start
                            (is (.await wal-started 5 TimeUnit/SECONDS))
                            :native-tail-applied (.countDown native-done)
                            :wal-start
                            (do (.countDown wal-started)
                                (when-not (.await native-done 5 TimeUnit/SECONDS)
                                  (throw (ex-info "Native tail did not overlap WAL" {}))))
                            nil)))]
        (try
          (d/transact! conn [[:db.fn/call
                             (fn [_] (swap! bodies inc) (datoms 2 600))]])
          (is (= 1 @bodies))
          (is (= 1 (count (filter #{:worker-dispatch} @events))))
          (is (= 601 (d/q '[:find (count ?e) . :where [?e :value]] @conn)))
          (is (< (.indexOf ^java.util.List @events :native-applied)
                 (.indexOf ^java.util.List @events :native-committed)))
          (finally (uninstall)))
        (d/close conn)
        (let [reopened (d/create-conn dir)]
          (try (is (= 601 (d/q '[:find (count ?e) . :where [?e :value]] @reopened)))
               (finally (d/close reopened))))))))

(deftest transaction-reads-flush-large-tails-and-small-writes-stay-inline
  (with-conn opts
    (fn [conn _]
      (let [dispatches (atom 0)
            uninstall (phase/observe! (fn [event _]
                                        (when (= :worker-dispatch event)
                                          (swap! dispatches inc))))]
        (try
          (d/transact! conn (datoms 1 1))
          (is (zero? @dispatches))
          (d/with-transaction [tx conn]
            (d/transact! tx (datoms 2 600))
            (is (= 601 (d/q '[:find (count ?e) . :where [?e :value]] @tx)))
            (d/transact! tx [[:db/add 2 :value 999]])
            (is (= 999 (:value (d/entity @tx 2)))))
          (is (zero? @dispatches) "a read consumes the deferred tail before dispatch")
          (is (= 999 (:value (d/entity @conn 2))))
          (finally (uninstall)))))))

(deftest deferred-native-tail-resizes-without-rerunning-the-body
  (with-conn (assoc opts :kv-opts {:mapsize 1})
    (fn [conn _]
      (let [bodies (atom 0) resizes (atom 0)
            uninstall (phase/observe! (fn [event _]
                                        (when (= :native-resized event)
                                          (swap! resizes inc))))]
        (try
          (d/transact! conn [[:db.fn/call
                             (fn [_] (swap! bodies inc) (datoms 1 30000))]])
          (is (= 1 @bodies))
          (is (pos? @resizes))
          (is (= 30000 (d/q '[:find (count ?e) . :where [?e :value]] @conn)))
          (finally (uninstall)))))))

(deftest wal-failure-aborts-an-applied-tail-without-publishing-the-connection
  (with-conn opts
    (fn [conn _]
      (d/transact! conn (datoms 1 1))
      (let [before @conn bodies (atom 0) native-done (CountDownLatch. 1)
            uninstall (phase/observe!
                        (fn [event _]
                          (case event
                            :native-tail-applied (.countDown native-done)
                            :wal-start
                            (do
                              (when-not (.await native-done 5 TimeUnit/SECONDS)
                                (throw (ex-info "Native tail was not applied" {})))
                              (throw (ex-info "Forced WAL failure" {})))
                            nil)))]
        (try
          (is (thrown? Exception
                       (d/transact! conn [[:db.fn/call
                                          (fn [_] (swap! bodies inc) (datoms 2 600))]])))
          (is (zero? (.getCount native-done)))
          (is (= 1 @bodies))
          (is (identical? before @conn))
          (is (= 1 (d/q '[:find (count ?e) . :where [?e :value]] @conn)))
          (finally (uninstall)))))))
