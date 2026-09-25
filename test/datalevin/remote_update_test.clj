(ns datalevin.remote-update-test
  (:require [clojure.string :as str]
            [clojure.test :refer [deftest is testing use-fixtures]]
            [datalevin.client :as client]
            [datalevin.constants :as c]
            [datalevin.core :as d]
            [datalevin.interpret :as inter]
            [datalevin.kv :as kv]
            [datalevin.server :as server]
            [datalevin.test.core :refer [allocate-port db-fixture]]
            [datalevin.util :as u])
  (:import [datalevin.remote KVStore DatalogStore]
           [java.util.concurrent Callable Executors Future TimeUnit]))

(def ^:dynamic *base-uri* nil)

(use-fixtures :once
  db-fixture
  (fn [f]
    (let [root (u/tmp-dir (str "remote-update-" (random-uuid)))
          port (allocate-port)
          srv (server/create {:root root :port port})]
      (try
        (server/start srv)
        (binding [*base-uri* (str "dtlv://datalevin:datalevin@localhost:" port "/")]
          (f))
        (finally (server/stop srv) (u/delete-files root))))))

(defn- uri [] (str *base-uri* (random-uuid)))

(deftest remote-index-attr
  (let [conn (d/create-conn (uri)
                            {:body {:db/valueType :db.type/string :db/noindex true}})
        query '[:find ?e :where [?e :body "text"]]
        prepared (d/prepare-q @conn query)]
    (try
      (d/transact! conn [{:db/id 1 :body "text"}])
      (is (= {:body "text"} (d/pull @conn [:body] 1)))
      (is (thrown-with-msg? Exception #"index-attr" (d/q query @conn)))
      (is (thrown-with-msg? Exception #"index-attr" (prepared [])))
      (d/index-attr conn :body)
      (is (nil? (get-in (d/schema conn) [:body :db/noindex])))
      (is (= #{[1]} (d/q query @conn)))
      (is (= #{[1]} (prepared [])))
      (d/transact! conn [{:db/id 2 :body "text"}])
      (is (= #{[1] [2]} (d/q query @conn)))
      (is (= (d/schema conn) (d/index-attr conn :body)))
      (finally (d/close conn)))))

(defn- trace-requests [f]
  (let [calls (atom [])
        handlers @#'server/message-handler-map
        tracked (into {} (map (fn [[type handler]]
                               [type (fn [srv key message]
                                       (swap! calls conj message)
                                       (handler srv key message))])) handlers)]
    (with-redefs-fn {#'server/message-handler-map tracked}
      #(let [result (f)] {:result result :requests @calls}))))

(def increment-value
  (inter/inter-fn [old n] (+ (long (or old 0)) (long n))))

(def increment-entity
  (inter/inter-fn [db eid]
    (let [old (:value (datalevin.core/pull db [:value] eid))]
      (Thread/sleep 2)
      [[:db/add eid :value (inc (long old))]])))

(defn- concurrent! [handles f]
  (let [executor (Executors/newFixedThreadPool (count handles))
        start (promise)]
    (try
      (let [tasks (mapv (fn [handle]
                          (.submit executor ^Callable
                                   (fn [] @start (dotimes [_ 10] (f handle)))))
                        handles)]
        (deliver start true)
        (doseq [^Future task tasks] (.get task 30 TimeUnit/SECONDS)))
      (finally (.shutdownNow executor)))))

(deftest kv-update-atomicity-types-and-rollback
  (doseq [wal? [false true]
          remote? [false true]]
    (testing (str "WAL " wal? ", remote " remote?)
      (let [dir (u/tmp-dir (str "local-update-" (random-uuid)))
            db (d/open-kv (if remote? (uri) dir) {:wal? wal?})]
        (try
          (d/open-dbi db "values")
          (is (= :transacted (d/update-kv db "values" 1 increment-value :id :long 2)))
          (is (= 2 (d/get-value db "values" 1 :id :long)))
          (d/with-transaction-kv [tx db]
            (d/update-kv tx "values" 1 increment-value :id :long 3)
            (is (= 5 (d/get-value tx "values" 1 :id :long)))
            (d/abort-transact-kv tx))
          (is (= 2 (d/get-value db "values" 1 :id :long)))
          (is (thrown? Exception
                       (d/update-kv db "values" 1
                         (inter/inter-fn [_] (throw (ex-info "stop" {}))) :id :long)))
          (is (= 2 (d/get-value db "values" 1 :id :long)))
          (is (thrown? Exception
                       (d/update-kv db "values" 1 (inter/inter-fn [_] "wrong") :id :long)))
          (is (= 2 (d/get-value db "values" 1 :id :long)))
          (d/open-dbi db "docs")
          (d/update-kv db "docs" :doc
            (inter/inter-fn [old k v] (assoc old k v)) :data :data :option 42)
          (is (= {:option 42} (d/get-value db "docs" :doc)))
          (d/register-type db :app/document
            {:index {:type :long :order-fn (inter/inter-fn [v] (:rank v))}})
          (d/open-dbi db "custom" {:key-type :app/document})
          (d/update-kv db "custom" {:rank 1 :label "key"}
            (inter/inter-fn [_] {:option "old"}))
          (d/update-kv db "custom" {:rank 1 :label "key"}
            (inter/inter-fn [v] (assoc v :option "new")))
          (is (= {:option "new"}
                 (d/get-value db "custom" {:rank 1 :label "key"})))
          (is (thrown? Exception
                       (d/update-kv db "docs" :doc (inter/inter-fn [_] nil))))
          (is (= {:option 42} (d/get-value db "docs" :doc)))
          (d/open-list-dbi db "duplicates")
          (is (thrown-with-msg? Exception #"single-value"
                (d/update-kv db "duplicates" 1 increment-value :id :long 1)))
          (when-not remote?
            (d/update-kv db "values" 1 + :id :long 4)
            (is (= 6 (d/get-value db "values" 1 :id :long))))
          (finally
            (d/close-kv db)
            (when-not remote? (u/delete-files dir))))))))

(deftest remote-kv-one-request-replay-and-concurrency
  (let [path (uri)
        handles (mapv (fn [_] (d/open-kv path {:wal? true :client-opts {:pool-size 1}}))
                      (range 4))
        db (first handles)]
    (try
      (doseq [db handles] (d/open-dbi db "values"))
      (let [{:keys [result requests]}
            (trace-requests #(d/update-kv db "values" 1 increment-value :id :long 1))]
        (is (= :transacted result))
        (is (= [:update-kv] (mapv :type requests)))
        (is (= :transacted
               (:result (client/request (.-client ^KVStore db) (first requests)))))
        (is (= 1 (d/get-value db "values" 1 :id :long))))
      (concurrent! handles #(d/update-kv % "values" 1 increment-value :id :long 1))
      (is (= 41 (d/get-value db "values" 1 :id :long)))
      (let [watermarks (kv/txlog-watermarks db)]
        (is (= (:last-committed-lsn watermarks) (:last-durable-lsn watermarks))))
      (finally (doseq [db handles] (d/close-kv db))))))

(deftest remote-datalog-one-request-reports-and-rollback
  (doseq [wal? [false true]
          prepare? [false true]]
    (with-redefs [c/*use-prepare-path* prepare?]
      (let [conn (d/create-conn (uri) {:value {:db/valueType :db.type/long}}
                                {:wal? wal? :background-sampling? false})
            events (atom [])]
        (try
          (d/listen! conn :test #(swap! events conj %))
          (let [before @conn
                {:keys [result requests]}
                (trace-requests #(d/transact! conn [{:db/id -1 :value 0}] {:test :metadata}))
                eid (get (:tempids result) -1)]
            (is (= [:tx-data+db-info] (mapv :type requests)))
            (is (identical? before (:db-before result)))
            (is (identical? @conn (:db-after result)))
            (is (= {:test :metadata} (:tx-meta result)))
            (is (= [result] @events))
            (is (= 0 (:value (d/pull @conn [:value] eid))))
            (let [txs [[:db.fn/call increment-entity eid]]
                  {:keys [requests]} (trace-requests #(d/transact! conn txs))
                  store (:store @conn)]
              (is (= [:tx-data+db-info] (mapv :type requests)))
              (client/request (.-client ^DatalogStore store) (first requests))
              (is (= 1 (:value (d/pull @conn [:value] eid)))))
            (is (thrown? Exception
                         (d/transact! conn [[:db/add eid :value 100]
                                            [:db/ensure (inter/inter-fn [_] false)]])))
            (is (= 1 (:value (d/pull @conn [:value] eid))))
            (d/with-transaction [tx conn]
              (d/transact! tx [[:db.fn/call increment-entity eid]])
              (is (= 2 (:value (d/pull @tx [:value] eid))))
              (d/abort-transact tx))
            (is (= 1 (:value (d/pull @conn [:value] eid))))
            (d/transact! conn [[:db/add eid :new-attribute "visible"]])
            (is (= "visible" (:new-attribute (d/pull @conn [:new-attribute] eid))))
            (is (contains? (d/schema conn) :new-attribute)))
          (finally (d/close conn)))))))

(deftest remote-datalog-concurrent-functions-see-latest-commit
  (let [path (uri)
        handles (mapv (fn [_]
                        (d/create-conn path {:value {:db/valueType :db.type/long}}
                                       {:wal? true :background-sampling? false
                                        :client-opts {:pool-size 1}}))
                      (range 4))
        conn (first handles)]
    (try
      (d/transact! conn [{:db/id 1 :value 0}])
      (concurrent! handles #(d/transact! % [[:db.fn/call increment-entity 1]]))
      (is (= 40 (:value (d/pull @conn [:value] 1))))
      (concurrent! (vec (repeat 4 conn))
                   #(d/transact! % [[:db.fn/call increment-entity 1]]))
      (is (= 80 (:value (d/pull @conn [:value] 1))))
      (finally (doseq [conn handles] (d/close conn))))))

(deftest remote-kv-update-requires-write-permission
  (let [path (uri)
        name (last (str/split path #"/"))
        username (str "reader-" (random-uuid))
        role (keyword username)
        admin (client/new-client *base-uri*)
        db (d/open-kv path)]
    (try
      (d/open-dbi db "values")
      (d/update-kv db "values" 1 increment-value :id :long 1)
      (client/create-user admin username "password")
      (client/create-role admin role)
      (client/assign-role admin role username)
      (client/grant-permission admin role :datalevin.server/view
                               :datalevin.server/database name)
      (let [reader (d/open-kv (str/replace path "datalevin:datalevin@"
                                              (str username ":password@")))]
        (try
          (is (= 1 (d/get-value reader "values" 1 :id :long)))
          (is (thrown-with-msg? Exception #"permission"
                (d/update-kv reader "values" 1 increment-value :id :long 1)))
          (is (= 1 (d/get-value db "values" 1 :id :long)))
          (finally (d/close-kv reader))))
      (finally (d/close-kv db) (client/disconnect admin)))))
