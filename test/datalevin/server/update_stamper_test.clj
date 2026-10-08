(ns datalevin.server.update-stamper-test
  (:require [clojure.test :refer [deftest is use-fixtures]]
            [datalevin.core :as d]
            [datalevin.datom :as datom]
            [datalevin.interface :as i]
            [datalevin.interpret :as inter]
            [datalevin.kv.txlog :as kvtx]
            [datalevin.lmdb :as l]
            [datalevin.server :as server]
            [datalevin.server.handlers :as handlers]
            [datalevin.test.core :refer [allocate-port db-fixture]]
            [datalevin.util :as u])
  (:import [datalevin.storage Store]
           [datalevin.tx_group.batch Collector]
           [java.util.concurrent ConcurrentLinkedQueue]))

(def ^:dynamic *uri* nil)
(def ^:dynamic *server* nil)

(use-fixtures :once
  db-fixture
  (fn [f]
    (let [root (u/tmp-dir (str "server-stampers-" (random-uuid)))
          port (allocate-port)
          srv (server/create {:root root :port port})]
      (try
        (server/start srv)
        (binding [*uri* (str "dtlv://datalevin:datalevin@localhost:" port "/")
                  *server* srv]
          (f))
        (finally (server/stop srv) (u/delete-files root))))))

(def fields (mapv #(keyword "item" (str "value" %)) (range 8)))
(def schema
  (assoc (zipmap fields (repeat {:db/valueType :db.type/string :db/noindex true}))
         :item/key {:db/valueType :db.type/string :db/unique :db.unique/identity}
         :item/n {:db/valueType :db.type/long}
         :item/token {:db/valueType :db.type/string :db/unique :db.unique/value}))

(defn- open-conn [schema opts]
  (d/create-conn (str *uri* (random-uuid)) schema
                 (merge {:background-sampling? false :client-opts {:pool-size 1}} opts)))

(defn- report-data [report]
  {:datoms (mapv (juxt :e :a :v :tx datom/datom-added) (:tx-data report))
   :tempids (:tempids report) :tx-meta (:tx-meta report)})

(deftest remote-kv-uses-local-write-entry-point
  (doseq [profile [:strict :relaxed]]
    (let [name (str (random-uuid))
          remote (d/open-kv (str *uri* name) {:wal? true :wal-durability-profile profile})]
      (try
        (d/open-dbi remote "data")
        (d/transact-kv remote "data" [[:put 1 "initial"]] :long :string)
        (let [local (#'server/get-kv-store *server* name)
              info (i/kv-info local)
              control (:independent-control @info)
              calls (atom 0)
              transact! (:transact! control)]
          (is (:server? control))
          (vswap! info assoc :independent-control
                  (assoc control
                         :transact! (fn [& args]
                                      (swap! calls inc)
                                      (apply transact! args))
                         :body! (fn [& _]
                                  (throw (ex-info "Unexpected transaction body" {})))))
          (try
            (d/transact-kv remote "data" [[:put 1 "named"]] :long :string)
            (d/transact-kv remote [[:put "data" 2 "mixed" :long :string]])
            (is (= 2 @calls))
            (is (= "named" (d/get-value remote "data" 1 :long :string)))
            (is (= "mixed" (d/get-value remote "data" 2 :long :string)))
            (finally (vswap! info assoc :independent-control control))))
        (finally (d/close-kv remote))))))

(deftest remote-wal-kv-retains-local-custom-type-fallback
  (let [remote (d/open-kv (str *uri* (random-uuid)) {:wal? true})
        a {:rank 1 :name "a"}
        b {:rank 2 :name "b"}]
    (try
      (d/register-type remote :app/task
                       {:index {:type :long
                                :order-fn (inter/inter-fn [v] (:rank v))}})
      (d/open-dbi remote "tasks" {:key-type :app/task})
      (d/transact-kv remote "tasks" [[:put a :a] [:put b :b]])
      (is (= :a (d/get-value remote "tasks" a)))
      (is (= :b (d/get-value remote "tasks" b)))
      (d/transact-kv remote [[:del "tasks" a]])
      (is (nil? (d/get-value remote "tasks" a)))
      (is (= :b (d/get-value remote "tasks" b)))
      (finally (d/close-kv remote)))))

(defn- trace-stamping [f]
  (let [prepare @#'handlers/prepare-server-tx
        stamp @#'handlers/transact-prepared
        events (atom [])]
    (with-redefs-fn
      {#'handlers/prepare-server-tx
       (fn [db txs]
         (let [prepared (prepare db txs)
               kv (.-lmdb ^Store (:store db))]
           (swap! events conj {:phase :prepare :kind (first prepared)
                               :writer-held? (Thread/holdsLock (l/write-txn kv))})
           prepared))
       #'handlers/transact-prepared
       (fn [db prepared tx-meta]
         (let [report (stamp db prepared tx-meta)
               kv (.-lmdb ^Store (:store db))]
           (swap! events conj {:phase :stamp :kind (first prepared)
                               :used? (some? report) :writing? (l/writing? kv)})
           report))}
      #(do (f) @events))))

(deftest server-stampers-match-general-reports
  (doseq [wal? [false true]]
    (let [fast (open-conn schema {:wal? wal?})
          general (open-conn schema {:wal? wal?})
          initial (assoc (zipmap fields (repeat "old")) :db/id "one" :item/key "one")]
      (try
        (doseq [conn [fast general]] (d/transact! conn [initial]))
        (let [eid (d/entid @fast [:item/key "one"])
              changes (assoc (zipmap fields (repeat "new"))
                             (fields 2) (apply str (repeat 2000 "g")))
              transactions [(mapv #(vector :db/add eid % (changes %)) fields)
                            [(assoc (zipmap fields (repeat "upsert"))
                                    :db/id "upsert" :item/key "one")]
                            [(assoc (zipmap fields (repeat "upsert"))
                                    :db/id "upsert" :item/key "one")]
                            [[:db/add [:item/key "one"] :item/n 9]]
                            [[:db/add eid :item/n 10]
                             [:db/add [:item/key "one"] :item/n 11]]]
              events
              (trace-stamping
               #(doseq [tx transactions]
                  (let [expected (with-redefs-fn {#'handlers/prepare-server-tx (constantly nil)}
                                   (fn [] (d/transact! general tx {:test :report})))
                        actual (d/transact! fast tx {:test :report})]
                    (is (= (report-data expected) (report-data actual))))))
              prepared (filter #(= :prepare (:phase %)) events)
              used (filter :used? events)]
          (is (= [:scalar-update :blind :blind :scalar-update :scalar-update]
                 (mapv :kind prepared)))
          (is (every? (complement :writer-held?) prepared))
          (is (= [:scalar-update :blind :blind :scalar-update] (mapv :kind used)))
          (is (every? :writing? used))
          (is (= 11 (:item/n (d/entity @fast eid))))
          (is (= (:max-eid @general) (:max-eid @fast))))
        (finally (d/close fast) (d/close general))))))

(defn- while-prepared [conn txs intervene!]
  (let [prepare @#'handlers/prepare-server-tx
        ready (promise) resume (promise)
        captured (promise)]
    (with-redefs-fn
      {#'handlers/prepare-server-tx
       (fn [db data]
         (let [prepared (prepare db data)]
           (when (= data txs)
             (deliver captured prepared)
             (deliver ready true)
             (when-not (deref resume 10000 false)
               (throw (ex-info "timed out waiting for intervening write" {}))))
           prepared))}
      #(let [request (future (d/transact! conn txs))]
         (try
           (is (deref ready 10000 false))
           (is (some? (deref captured 1000 nil)))
           (intervene!)
           (deliver resume true)
           (deref request 10000 ::timeout)
           (finally (deliver resume true)))))))

(deftest waiting-requests-resolve-current-values-and-identities
  (let [uri (str *uri* (random-uuid))
        opts {:wal? true :client-opts {:pool-size 1}}
        first (d/create-conn uri schema opts)
        second (d/create-conn uri schema opts)]
    (try
      (let [seed (d/transact! first [{:db/id "a" :item/key "a" :item/n 1}
                                     {:db/id "b" :item/key "b" :item/n 2}])
            a (get (:tempids seed) "a")
            b (get (:tempids seed) "b")
            report (while-prepared
                    first [[:db/add [:item/key "a"] :item/n 100]]
                    #(do (d/transact! second [[:db/retract a :item/key "a"]])
                         (d/transact! second [[:db/add b :item/key "a"]
                                              [:db/add b :item/n 50]])))]
        (is (map? report))
        (is (= #{b} (set (map :e (:tx-data report)))))
        (is (some #(and (= 50 (:v %)) (not (datom/datom-added %))) (:tx-data report)))
        (is (= 1 (:item/n (d/entity @first a))))
        (is (= 100 (:item/n (d/entity @first b)))))
      (let [inserted (atom nil)
            report (while-prepared
                     first [{:db/id "upsert" :item/key "new" :item/n 200}]
                     #(reset! inserted
                              (d/transact! second [{:db/id "insert" :item/key "new"
                                                   :item/n 150}])))]
        (is (map? report))
        (is (= (get-in @inserted [:tempids "insert"])
               (get-in report [:tempids "upsert"])))
        (is (some #(and (= 150 (:v %)) (not (datom/datom-added %))) (:tx-data report)))
        (is (= 200 (:item/n (d/entity @first [:item/key "new"])))))
      (finally (d/close first) (d/close second)))))

(deftest waiting-preparations-fall-back-after-schema-change
  (doseq [tx-kind [:scalar :identity]]
    (let [uri (str *uri* (random-uuid))
          conn (d/create-conn uri schema {:wal? true :client-opts {:pool-size 1}})
          other (d/create-conn uri schema {:wal? true :client-opts {:pool-size 1}})]
      (try
        (let [report (d/transact! conn [{:item/key "one" :item/n 1}])
              eid (:e (first (:tx-data report)))
              txs (if (= tx-kind :scalar) [[:db/add eid :item/n 2]]
                      [{:item/key "one" :item/n 2}])
              events
              (trace-stamping
                #(is (map? (while-prepared
                            conn txs
                            (fn [] (d/update-schema other
                                     {:item/n {:db/valueType :db.type/long
                                               :db/cardinality :db.cardinality/many}}))))))]
          (is (not-any? :used? events))
          (is (= #{1 2} (:item/n (d/entity @conn eid))))
          (is (= #{1 2} (:item/n (d/entity @other eid)))))
        (finally (d/close conn) (d/close other))))))

(deftest server-update-guards-and-explicit-rollback
  (let [schema (assoc schema :item/pair {:db/valueType :db.type/tuple
                                        :db/tupleAttrs [:item/n :item/token]})
        conn (open-conn schema {:wal? true})]
    (try
      (let [seed (d/transact! conn [{:db/id "one" :item/key "one" :item/n 1 :item/token "a"}
                                    {:item/key "two" :item/n 2 :item/token "b"}])
            eid (get (:tempids seed) "one")
            events (trace-stamping
                    #(do (d/transact! conn [[:db/add eid :item/n 3]])
                         (is (thrown? Exception
                                      (d/transact! conn [{:item/key "one" :item/token "b"}])))))]
        (is (not-any? :used? events))
        (is (= [3 "a"] (:item/pair (d/entity @conn eid))))
        (is (= "a" (:item/token (d/entity @conn eid))))
        (d/with-transaction [tx conn]
          (d/transact! tx [[:db/add eid (fields 0) "first"]])
          (let [report (d/transact! tx [{:item/key "one" (fields 0) "second"}])]
            (is (some #(and (= "first" (:v %)) (not (datom/datom-added %))) (:tx-data report))))
          (is (= "second" (get (d/entity @tx eid) (fields 0))))
          (d/abort-transact tx))
        (is (nil? (get (d/entity @conn eid) (fields 0)))))
      (finally (d/close conn))))
  (let [conn (open-conn schema {:wal? true :auto-entity-time? true})]
    (try
      (let [seed (d/transact! conn [{:item/key "one" :item/n 1}])
            eid (:e (first (:tx-data seed)))
            events (trace-stamping
                    #(let [report (d/transact! conn [[:db/add eid :item/n 2]])]
                       (is (some (fn [dt] (= :db/updated-at (:a dt))) (:tx-data report)))))]
        (is (not-any? :used? events)))
      (finally (d/close conn)))))

(defn- await-queued! [^Collector collector n]
  (let [deadline (+ (System/nanoTime) 10000000000)]
    (loop []
      (cond
        (= n (.size ^ConcurrentLinkedQueue (.-ready collector))) true
        (> (System/nanoTime) deadline) false
        :else (do (Thread/sleep 1) (recur))))))

(deftest grouped-server-stampers-observe-preceding-requests
  (let [name (str (random-uuid))
        connections (mapv (fn [_] (d/create-conn (str *uri* name) schema
                                   {:wal? true :wal-durability-profile :strict
                                    :client-opts {:pool-size 1}})) (range 4))
        entered (promise) release (promise)
        first? (atom true) jobs (atom [])]
    (try
      (d/transact! (first connections) [{:db/id 1 :item/key "one" :item/n 0}])
      (let [store ^Store (#'server/get-store *server* name false)
            lmdb (.-lmdb store)
            control (:independent-control @(i/kv-info lmdb))
            collector (:collector control)
            before (:last-committed-lsn (d/txlog-watermarks lmdb))
            txs [[[:db/add 1 :item/n 1]]
                 [{:item/key "new" :item/n 2}]
                 [[:db/add [:item/key "new"] :item/n 3]]
                 [{:item/key "new" :item/n 4}]]]
        (is (:server? control))
        (is (some? collector))
        (kvtx/set-storage-fault-hook!
         (fn [{:keys [stage]}]
           (when (and (= stage :txlog-sync) (compare-and-set! first? true false))
             (deliver entered true)
             (assert (deref release 10000 false)))))
        (let [events
              (trace-stamping
               #(do
                  (swap! jobs conj (future (d/transact! (connections 0) (txs 0))))
                  (is (deref entered 10000 false))
                  (doseq [idx (range 1 4)]
                    (swap! jobs conj (future (d/transact! (connections idx) (txs idx))))
                    (is (await-queued! collector idx)))
                  (deliver release true)
                  (let [reports (mapv (fn [job] (deref job 10000 ::timeout)) @jobs)]
                    (is (every? map? reports))
                    (is (= [0 nil 2 3]
                           (mapv (fn [report]
                                   (:v (first (remove datom/datom-added (:tx-data report)))))
                                 reports))))))]
          (is (= 4 (count (filter :used? events))))
          (is (not-any? :writer-held? (filter #(= :prepare (:phase %)) events)))
          (is (= (+ (long before) 2)
                 (:last-committed-lsn (d/txlog-watermarks lmdb))))
          (is (= 4 (:item/n (d/entity @(first connections) [:item/key "new"]))))))
      (finally
        (deliver release true)
        (doseq [job @jobs] (deref job 10000 nil))
        (kvtx/clear-storage-fault-hook!)
        (doseq [conn connections] (d/close conn))))))
