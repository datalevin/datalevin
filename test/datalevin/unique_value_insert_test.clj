(ns datalevin.unique-value-insert-test
  (:require
   [clojure.test :refer [deftest is testing use-fixtures]]
   [datalevin.client :as client]
   [datalevin.client-op :as cop]
   [datalevin.conn :as conn]
   [datalevin.core :as d]
   [datalevin.db :as db]
   [datalevin.kv :as kv]
   [datalevin.server :as server]
   [datalevin.server.handlers :as handlers]
   [datalevin.test.core :refer [allocate-port db-fixture]]
   [datalevin.util :as u])
  (:import
   [datalevin.remote DatalogStore]
   [datalevin.storage Store]
   [datalevin.tx_group Group]
   [java.util.concurrent ConcurrentLinkedQueue]
   [java.util.concurrent.locks ReentrantLock]
   [org.eclipse.collections.impl.list.mutable FastList]))

(use-fixtures :each db-fixture)

(def ^:private schema
  {:item/key {:db/valueType :db.type/string :db/unique :db.unique/value}
   :item/value {:db/valueType :db.type/string :db/noindex true}})

(defn- item [key value]
  {:item/key key :item/value value})

(deftest unique-value-blind-preparation
  (let [conn (d/create-conn nil schema {:kv-opts {:inmemory? true :wal? false}})]
    (try
      (let [prepared (db/prepare-blind-local-tx @conn [(item "new" "value")])]
        (is (some? prepared) "A single non-WAL insert is eligible")
        (is (= [[:item/key "new"]] (vec (:unique-avs prepared))))
        (is (true? (:fuse-unique-inserts? prepared)))
        (is (nil? (:identity-upsert-av prepared))))
      (is (nil? (db/prepare-blind-local-tx
                  @conn [(item "same" "a") (item "same" "b")])))
      (is (nil? (db/prepare-blind-local-tx
                  @conn [(item (apply str (repeat 600 "x")) "giant")])))
      (is (nil? (db/prepare-blind-local-tx
                  @conn [(assoc (item "a" "a") :db/id "same")
                         (assoc (item "b" "b") :db/id "same")])))
      (finally (d/close conn)))))

(deftest local-unique-value-inserts-reject-duplicates-atomically
  (doseq [wal? [false true]
          fulltext? [false true]]
    (testing (str "WAL " wal? ", fulltext " fulltext?)
      (let [dir (u/tmp-dir (str "unique-insert-" (random-uuid)))
            schema (cond-> schema
                     fulltext? (assoc :item/value
                                      {:db/valueType :db.type/string
                                       :db/fulltext true}))
            conn (d/create-conn dir schema {:wal? wal?})
            paths (atom [])]
        (try
          (let [prepared (db/prepare-blind-local-tx
                           @conn [(item "first" "original")] true false)]
            (is (= (not fulltext?) (:fuse-unique-inserts? prepared)))
            (when fulltext?
              (is (= [[:item/key "first"]] (vec (:unique-avs prepared))))))
          (let [report (binding [conn/*local-wal-tx-path-observer*
                                 #(swap! paths conj %)]
                         (d/transact! conn
                                      [(assoc (item "first" "original")
                                              :db/id "new")]
                                      {:test :insert}))
                eid (get-in report [:tempids "new"])]
            (is (pos-int? eid))
            (is (= {:test :insert} (:tx-meta report)))
            (is (= "original" (:item/value (d/entity @conn eid))))
            (when wal? (is (= [:blind-insert] @paths))))
          (doseq [txs [[(item "first" "duplicate")]
                       [(item "discarded" "fresh") (item "first" "duplicate")]
                       [(item "twice" "a") (item "twice" "b")]]]
            (is (thrown-with-msg? Exception #"unique constraint"
                  (d/transact! conn txs)))
            (is (= #{["first" "original"]}
                   (d/q '[:find ?key ?value
                          :where [?e :item/key ?key] [?e :item/value ?value]]
                        @conn))))
          (d/transact! conn [(item "second" "valid")])
          (d/close conn)
          (let [reopened (d/create-conn dir)]
            (try
              (is (= #{["first" "original"] ["second" "valid"]}
                     (d/q '[:find ?key ?value
                            :where [?e :item/key ?key] [?e :item/value ?value]]
                          @reopened)))
              (finally (d/close reopened))))
          (finally (d/close conn) (u/delete-files dir)))))))

(deftest identity-upserts-preserve-additional-value-uniqueness
  (let [conn (d/create-conn nil
                            (assoc schema :item/id
                                   {:db/valueType :db.type/long
                                    :db/unique :db.unique/identity})
                            {:wal? true :kv-opts {:inmemory? true}})]
    (try
      (d/transact! conn [(assoc (item "one" "initial") :item/id 1)
                         (assoc (item "two" "other") :item/id 2)])
      (let [txs [(assoc (item "one" "updated") :item/id 1)]
            prepared (db/prepare-blind-local-tx @conn txs true false)]
        (is (some? prepared))
        (is (nil? (:identity-upsert-av prepared)))
        (d/transact! conn txs))
      (is (thrown-with-msg? Exception #"unique constraint"
            (d/transact! conn [(assoc (item "two" "conflict") :item/id 1)])))
      (is (= "updated" (:item/value (d/entity @conn [:item/key "one"]))))
      (is (= "other" (:item/value (d/entity @conn [:item/key "two"]))))
      (finally (d/close conn)))))

(deftest cardinality-many-unique-values-retain-set-semantics
  (doseq [wal? [false true]]
    (let [conn (d/create-conn nil
                              (assoc-in schema [:item/key :db/cardinality]
                                        :db.cardinality/many)
                              {:wal? wal? :kv-opts {:inmemory? true}})]
      (try
        (d/transact! conn [{:item/key ["a" "a" "b"] :item/value "original"}])
        (is (= #{"a" "b"} (:item/key (d/entity @conn [:item/key "a"]))))
        (is (thrown-with-msg? Exception #"unique constraint"
              (d/transact! conn [{:item/key ["c" "b"] :item/value "conflict"}])))
        (is (nil? (d/entity @conn [:item/key "c"])))
        (is (= "original" (:item/value (d/entity @conn [:item/key "b"]))))
        (finally (d/close conn))))))

(deftest queued-unique-value-inserts-rollback-before-fallback
  (let [conn (d/create-conn nil schema {:wal? true :kv-opts {:inmemory? true}})
        requests (fn [items]
                   (let [requests (FastList.)]
                     (doseq [entity items]
                       (.add requests (conn/->SyncQueuedReq [entity] nil (promise))))
                     requests))]
    (try
      (is (nil? (#'conn/prepare-sync-queued-blind-batch
                   conn (requests [(item "same" "a") (item "same" "b")]))))
      (d/transact! conn [(item "existing" "original")])
      (let [requests (requests [(item "discarded" "fresh")
                                (item "existing" "duplicate")])
            prepared (#'conn/prepare-sync-queued-blind-batch conn requests)]
        (is (some? prepared))
        (is (false? (#'conn/try-commit-sync-queued-blind-batch!
                      conn requests prepared (object-array 2)))))
      (is (= #{["existing" "original"]}
             (d/q '[:find ?key ?value
                    :where [?e :item/key ?key] [?e :item/value ?value]] @conn)))
      (finally (d/close conn)))))

(deftest server-blind-inserts-use-the-current-write-transaction
  (let [conn (d/create-conn nil schema {:kv-opts {:inmemory? true :wal? false}})]
    (try
      (d/with-transaction [tx conn]
        (let [report (#'handlers/transact-blind-insert
                       @tx [(assoc (item "new" "value") :db/id "new")]
                       {:test :server})]
          (is (some? report))
          (is (= {:test :server} (:tx-meta report)))
          (is (pos-int? (get-in report [:tempids "new"])))
          (reset! tx (:db-after report)))
        (is (nil? (#'handlers/transact-blind-insert
                    @tx [(item "discarded" "fresh") (item "new" "duplicate")]
                    nil)))
        (is (nil? (d/entity @tx [:item/key "discarded"]))))
      (is (= "value" (:item/value (d/entity @conn [:item/key "new"]))))
      (finally (d/close conn)))))

(deftest remote-unique-value-inserts-preserve-replay-and-duplicate-rejection
  (doseq [wal? [false true]]
    (let [root (u/tmp-dir (str "remote-unique-insert-" (random-uuid)))
          port (allocate-port)
          srv (server/create {:root root :port port})]
      (try
        (server/start srv)
        (let [conn (d/create-conn
                     (str "dtlv://datalevin:datalevin@localhost:" port "/items")
                     schema {:wal? wal? :client-opts {:pool-size 1}})
              client (.-client ^DatalogStore (:store @conn))]
          (try
            (doseq [kind [:tx-data :tx-data+db-info]]
              (let [txs [(assoc (item (name kind) "original") :db/id "new")]
                    message {:type kind :mode :request :writing? false
                             :args ["items" txs false]
                             :client-op-id (str (random-uuid))
                             :client-op-hash (cop/request-hash
                                              (cop/tx-request-payload
                                                kind "items" txs false))
                             :client-op-response-kind kind}
                    response (client/request client message)
                    replay (client/request client message)
                    duplicate (client/request client
                                              (assoc message :client-op-id
                                                     (str (random-uuid))))]
                (is (= :command-complete (:type response) (:type replay)))
                (is (= (update (:result response) :tx-data
                               #(mapv (juxt :e :a :v :tx :added) %))
                       (:result replay)))
                (is (pos-int? (get-in response [:result :tempids "new"])))
                (is (= :error-response (:type duplicate)))
                (when (= kind :tx-data+db-info)
                  (is (pos-int? (get-in response [:result :db-info :max-eid]))))))
            (d/with-transaction [tx conn]
              (d/transact! tx [(item "explicit" "kept")])
              (is (thrown-with-msg? Exception #"unique constraint"
                    (d/transact! tx [(item "explicit" "duplicate")])))
              (d/transact! tx [(item "after-error" "kept")]))
            (is (thrown-with-msg? Exception #"unique constraint"
                  (d/transact! conn [(item "twice" "a") (item "twice" "b")])))
            (is (= #{["tx-data" "original"] ["tx-data+db-info" "original"]
                     ["explicit" "kept"] ["after-error" "kept"]}
                   (d/q '[:find ?key ?value
                          :where [?e :item/key ?key] [?e :item/value ?value]]
                        @conn)))
            (finally (d/close conn))))
        (finally (server/stop srv) (u/delete-files root))))))

(deftest grouped-remote-unique-inserts-reject-only-the-duplicate-request
  (let [root (u/tmp-dir (str "grouped-unique-insert-" (random-uuid)))
        port (allocate-port)
        srv (server/create {:root root :port port})
        jobs (atom [])]
    (try
      (server/start srv)
      (let [conn (d/create-conn
                   (str "dtlv://datalevin:datalevin@localhost:" port "/items")
                   schema {:wal? true :wal-shared? false
                           :client-opts {:pool-size 3}})
            client (.-client ^DatalogStore (:store @conn))
            store ^Store (#'server/get-store srv "items" false)
            g ^Group (kv/strict-write-group (.-lmdb store) :server-datalog)]
        (try
          (is (some? g))
          ;; Hold admission until all requests queue, forcing one shared native
          ;; transaction and exercising rollback plus individual fallback.
          (let [lock ^ReentrantLock (.-lock g)
                queue ^ConcurrentLinkedQueue (.-queue g)]
            (.lock lock)
            (try
              (doseq [entity [(item "same" "a") (item "same" "b")
                              (item "other" "kept")]]
                (swap! jobs conj
                       (future
                         (client/request client
                                         {:type :tx-data :mode :request
                                          :args ["items" [entity] false]}))))
              (is (loop [attempt 0]
                    (cond
                      (= 3 (.size queue)) true
                      (= 500 attempt) false
                      :else (do (Thread/sleep 10) (recur (inc attempt))))))
              (finally (.unlock lock))))
          (let [responses (mapv #(deref % 10000 ::timeout) @jobs)]
            (is (= {:command-complete 2 :error-response 1}
                   (frequencies (map :type responses)))))
          (is (= 2 (d/count-datoms @conn nil :item/key nil)))
          (is (contains? #{"a" "b"}
                         (:item/value (d/entity @conn [:item/key "same"]))))
          (is (= "kept" (:item/value (d/entity @conn [:item/key "other"]))))
          (finally
            (doseq [job @jobs] (deref job 10000 nil))
            (d/close conn))))
      (finally (server/stop srv) (u/delete-files root)))))
