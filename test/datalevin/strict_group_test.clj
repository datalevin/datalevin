(ns datalevin.strict-group-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [datalevin.binding.cpp :as cpp]
            [datalevin.client :as client]
            [datalevin.client-op :as cop]
            [datalevin.core :as d]
            [datalevin.constants :as c]
            [datalevin.interpret :as inter]
            [datalevin.kv :as kv]
            [datalevin.kv.txlog :as kvtx]
            [datalevin.server :as server]
            [datalevin.test.core :refer [allocate-port db-fixture]]
            [datalevin.txlog :as wal]
            [datalevin.util :as u])
  (:import [datalevin.tx_group Group]
           [datalevin.remote DatalogStore]
           [datalevin.storage Store]
           [java.io IOException]
           [java.util.concurrent ConcurrentLinkedQueue]))

(use-fixtures :each db-fixture)

(def opts {:wal? true :wal-durability-profile :strict :wal-shared? false
           :wal-segment-prealloc? false :snapshot-bootstrap-force? false
           :flags (conj c/default-env-flags :writemap)})

(def ^:dynamic *submitted-value* nil)

(defn- await! [pred]
  (let [deadline (+ (System/nanoTime) 10000000000)]
    (loop []
      (cond (pred) true
            (> (System/nanoTime) deadline) false
            :else (do (Thread/sleep 1) (recur))))))

(defn- queued [^Group g]
  (.size ^ConcurrentLinkedQueue (.-queue g)))

(defn- with-kv [f]
  (let [path (u/tmp-dir (str "strict-group-" (random-uuid)))
        db (d/open-kv path opts)]
    (try
      (d/open-dbi db "counter")
      (d/transact-kv db [[:put "counter" 1 0 :id :long]])
      (f db path)
      (finally (kvtx/clear-storage-fault-hook!)
               (d/close-kv db) (u/delete-files path)))))

(deftest queued-rmw-shares-a-durable-commit-and-preserves-every-update
  (with-kv
    (fn [db path]
      (let [g (kv/strict-write-group db :kv)
            entered (promise)
            release (promise)
            before (:last-committed-lsn (d/txlog-watermarks db))
            first-job (future (d/update-kv db "counter" 1
                                          (fn [old]
                                            (deliver entered true)
                                            (assert (deref release 10000 false))
                                            (inc old)) :id :long))
            jobs (atom [])]
        (try
          (is (true? (deref entered 10000 false)))
          (reset! jobs (mapv (fn [n]
                              (future
                                (binding [*submitted-value* n]
                                  (d/update-kv db "counter" 1
                                               (fn [old]
                                                 (is (= n *submitted-value*))
                                                 (inc old)) :id :long))))
                            (range 7)))
          (is (await! #(= 7 (queued g))))
          (is (= 0 (d/get-value db "counter" 1 :id :long)))
          (is (not-any? realized? @jobs))
          (deliver release true)
          (doseq [job (cons first-job @jobs)]
            (is (= :transacted (deref job 10000 ::timeout))))
          (is (= 8 (d/get-value db "counter" 1 :id :long)))
          (let [marks (d/txlog-watermarks db)]
            (is (= (+ before 2) (:last-committed-lsn marks)))
            (is (= (:last-committed-lsn marks) (:last-durable-lsn marks))))
          (d/close-kv db)
          (let [reopened (d/open-kv path opts)]
            (try (is (= 8 (d/get-value reopened "counter" 1 :id :long)))
                 (finally (d/close-kv reopened))))
          (finally
            (deliver release true)
            (doseq [job (cons first-job @jobs)] (deref job 10000 nil))))))))

(deftest bad-request-rolls-back-group-and-does-not-fail-other-callers
  (with-kv
    (fn [db _]
      (let [g (kv/strict-write-group db :kv)
            entered (promise) release (promise)
            first-job (future (d/update-kv db "counter" 1
                                          (fn [old] (deliver entered true)
                                            (assert (deref release 10000 false))
                                            (inc old)) :id :long))
            jobs (atom [])
            failure (IllegalArgumentException. "bad update")]
        (try
          (is (deref entered 10000 false))
          (doseq [f [inc (fn [_] (throw failure)) inc]]
            (swap! jobs conj (future (try (d/update-kv db "counter" 1 f :id :long)
                                         (catch Throwable t t))))
            (is (await! #(= (count @jobs) (queued g)))))
          (deliver release true)
          (is (= :transacted (deref first-job 10000 ::timeout)))
          (let [[a b c] (mapv #(deref % 10000 ::timeout) @jobs)]
            (is (= :transacted a c))
            (is (identical? failure b)))
          (is (= 3 (d/get-value db "counter" 1 :id :long)))
          (finally (deliver release true)
                   (doseq [job (cons first-job @jobs)] (deref job 10000 nil))))))))

(deftest custom-commit-hooks-and-explicit-transactions-retain-their-path
  (with-kv
    (fn [db _]
      (let [before (atom 0) after (atom [])]
        (binding [cpp/*before-write-commit-fn* (fn [_] (swap! before inc))
                  kvtx/*after-txlog-append-fn* #(swap! after conj (:txlog-lsn %))]
          (is (nil? (kv/strict-write-group db :kv)))
          (is (= :transacted (d/update-kv db "counter" 1 inc :id :long))))
        (is (= 1 @before (count @after))))
      (d/with-transaction-kv [tx db]
        (is (nil? (kv/strict-write-group tx :kv)))
        (d/update-kv tx "counter" 1 inc :id :long)
        (d/abort-transact-kv tx))
      (is (= 1 (d/get-value db "counter" 1 :id :long))))))

(deftest flush-completion-blocks-success-and-native-publication
  (doseq [fail? [false true]]
    (testing (str "failure=" fail?)
      (with-kv
        (fn [db _]
          (let [entered (promise) release (promise)
                failure (IOException. "injected sync failure")
                before (:last-durable-lsn (d/txlog-watermarks db))]
            (kvtx/set-storage-fault-hook!
              (fn [{:keys [stage]}]
                (when (= stage :txlog-sync)
                  (deliver entered true)
                  (assert (deref release 10000 false))
                  (when fail? (throw failure)))))
            (let [job (future (try (d/update-kv db "counter" 1 inc :id :long)
                                   (catch Throwable t t)))]
              (try
                (is (deref entered 10000 false))
                (is (= ::waiting (deref job 20 ::waiting)))
                (is (= before @(:last-durable-lsn (:sync-manager (wal/state db)))))
                (is (= 0 (d/get-value db "counter" 1 :id :long)))
                (deliver release true)
                (let [result (deref job 10000 ::timeout)]
                  (if fail?
                    (is (instance? Throwable result))
                    (do (is (= :transacted result))
                        (is (= 1 (d/get-value db "counter" 1 :id :long))))))
                (finally (deliver release true) (deref job 10000 nil))))))))))

(def remote-inc (inter/inter-fn [old] (inc (long old))))
(def datalog-inc
  (inter/inter-fn [db eid]
    [[:db/add eid :counter (inc (long (:counter (datalevin.core/pull db [:counter] eid))))]]))

(deftest remote-concurrent-rmw-preserves-values-and-datalog-identity
  (let [root (u/tmp-dir (str "strict-group-remote-" (random-uuid)))
        port (allocate-port)
        srv (server/create {:root root :port port})
        uri #(str "dtlv://datalevin:datalevin@localhost:" port "/" %)]
    (try
      (server/start srv)
      (doseq [api [:kv :datalog]]
        (testing (name api)
          (let [options (assoc opts :client-opts {:pool-size 1})
                handles (mapv (fn [_]
                                (if (= api :kv)
                                  (d/open-kv (uri "kv") options)
                                  (d/create-conn (uri "dl")
                                                 {:counter {:db/valueType :db.type/long}
                                                  :key {:db/unique :db.unique/identity}}
                                                 options)))
                              (range 8))]
            (try
              (let [first-handle (first handles)]
                (if (= api :kv)
                  (do (d/open-dbi first-handle "counter")
                      (d/transact-kv first-handle [[:put "counter" 1 0 :id :long]]))
                  (d/transact! first-handle [{:db/id 1 :key "one" :counter 0}
                                             {:db/ident :inc-counter :db/fn datalog-inc}]))
                (let [start (promise)
                      jobs (mapv (fn [handle]
                                   (future @start
                                           (dotimes [_ 20]
                                             (if (= api :kv)
                                               (d/update-kv handle "counter" 1 remote-inc :id :long)
                                               (d/transact! handle [[:inc-counter 1]])))
                                           true)) handles)]
                  (deliver start true)
                  (doseq [job jobs] (is (true? (deref job 30000 ::timeout)))))
                (if (= api :kv)
                  (is (= 160 (d/get-value first-handle "counter" 1 :id :long)))
                  (is (= {:counter 160 :key "one"}
                         (d/pull @first-handle [:counter :key] [:key "one"])))))
              (finally (doseq [handle handles]
                         (if (= api :kv) (d/close-kv handle) (d/close handle))))))))
      (finally (server/stop srv) (u/delete-files root)))))

(deftest grouped-datalog-responses-match-their-durable-replay-records
  (let [root (u/tmp-dir (str "strict-group-replay-" (random-uuid)))
        port (allocate-port)
        srv (server/create {:root root :port port})
        handles (atom [])
        jobs (atom [])
        entered (promise) release (promise)]
    (try
      (server/start srv)
      (reset! handles
              (mapv (fn [_]
                      (d/create-conn
                        (str "dtlv://datalevin:datalevin@localhost:" port "/dl")
                        {:counter {:db/valueType :db.type/long}}
                        (assoc opts :client-opts {:pool-size 1})))
                    (range 4)))
      (let [store ^Store (#'server/get-store srv "dl" false)
            lmdb (.-lmdb store)
            g (kv/strict-write-group lmdb :server-datalog)
            before (:last-committed-lsn (d/txlog-watermarks lmdb))
            clients (mapv #(.-client ^DatalogStore (:store @%)) @handles)
            messages (mapv (fn [idx]
                             (let [kind (if (even? idx) :tx-data :tx-data+db-info)
                                   txs [[:db/add (inc idx) :counter idx]]]
                               {:type kind :mode :request :writing? false
                                :args ["dl" txs false]
                                :client-op-id (str (random-uuid))
                                :client-op-hash (cop/request-hash
                                                 (cop/tx-request-payload kind "dl" txs false))
                                :client-op-response-kind kind}))
                           (range 4))
            first? (atom true)]
        (kvtx/set-storage-fault-hook!
          (fn [{:keys [stage]}]
            (when (and (= stage :txlog-sync)
                       (compare-and-set! first? true false))
              (deliver entered true)
              (assert (deref release 10000 false)))))
        (swap! jobs conj (future (client/request (clients 0) (messages 0))))
        (is (deref entered 10000 false))
        (doseq [idx (range 1 4)]
          (swap! jobs conj (future (client/request (clients idx) (messages idx))))
          (is (await! #(= idx (queued g)))))
        (deliver release true)
        (let [responses (mapv #(deref % 10000 ::timeout) @jobs)]
          (is (= (+ before 2) (:last-committed-lsn (d/txlog-watermarks lmdb))))
          (doseq [idx (range 4)]
            (let [response (responses idx)
                  message (messages idx)
                  record (d/get-value lmdb c/ha-client-ops
                                      (cop/kv-info-key (:client-op-id message))
                                      :string :data)
                  replay (client/request (clients idx) message)
                  metadata #(select-keys % [:tempids :db-info :new-attributes])]
              (is (= :command-complete (:type response) (:type replay)))
              (is (= (metadata (:result response))
                     (metadata (cop/record-response record))
                     (metadata (:result replay))))))))
      (finally
        (deliver release true)
        (doseq [job @jobs] (deref job 10000 nil))
        (kvtx/clear-storage-fault-hook!)
        (doseq [conn @handles] (d/close conn))
        (server/stop srv)
        (u/delete-files root)))))
