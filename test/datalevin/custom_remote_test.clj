(ns datalevin.custom-remote-test
  (:require [clojure.test :refer [deftest is use-fixtures]]
            [datalevin.bits :as b]
            [datalevin.client :as client]
            [datalevin.constants :as c]
            [datalevin.core :as d]
            [datalevin.custom-data :as custom]
            [datalevin.interface :as i]
            [datalevin.interpret :as inter]
            [datalevin.server :as server]
            [datalevin.test.core :refer [allocate-port]]
            [datalevin.udf :as udf]
            [datalevin.util :as u]
            [taoensso.timbre :as log])
  (:import [datalevin.server Server]
           [datalevin.storage Store]
           [java.lang AutoCloseable]
           [java.util UUID]))

(def ^:dynamic *server* nil)
(def ^:dynamic *registry* nil)

(use-fixtures :each
  (fn [f]
    (let [root (u/tmp-dir (str "custom-remote-" (UUID/randomUUID)))
          port (allocate-port)
          registry (udf/create-registry)]
      (log/set-min-level! :report)
      (binding [c/*db-background-sampling?* false
                client/*default-port* port
                *registry* registry]
        (with-redefs [server/*server-runtime-opts-fn*
                      (fn [_ _ _ _] {:udf-registry registry})]
          (binding [*server* (server/create {:root root :port port})]
            (try
              (server/start *server*)
              (f)
              (finally
                (when (.isOpen ^java.nio.channels.Selector
                               (.-selector ^Server *server*))
                  (server/stop *server*))
                (u/delete-files root)))))))))

(defn- uri [name]
  (str "dtlv://datalevin:datalevin@localhost/" name))

(defn- local-kv [name]
  (let [store (get-in (.-dbs ^Server *server*) [name :store])]
    (if (instance? Store store) (.-lmdb ^Store store) store)))

(defn- close-server-database! [name]
  (let [admin (client/new-client (uri ""))]
    (try
      (client/close-database admin name)
      (finally (client/disconnect admin)))))

(def task-type
  (let [field :rank]
    {:index {:type :long :order-fn (inter/inter-fn [v] (get v field))}}))

(def a {:rank 1 :name "a"})
(def b {:rank 1 :name "b"})
(def z {:rank 2 :name "z"})

(deftest remote-custom-kv
  (let [name "custom-kv"
        kv (d/open-kv (uri name))]
    (try
      (is (= :app/task (d/register-type kv :app/task task-type)))
      (is (= :app/task (d/register-type kv :app/task task-type)))
      (is (thrown-with-msg? Exception #"different definition"
                            (d/register-type kv :app/task
                                             (assoc task-type :version 2))))
      (d/open-dbi kv "tasks" {:key-type :app/task})
      (d/open-list-dbi kv "owners" {:value-type :app/task})
      (d/transact-kv kv "tasks" [[:put a :a] [:put b :b] [:put z :z]])
      (d/put-list-items kv "owners" :alice [a b z] :keyword :app/task)
      (is (= :a (d/get-value kv "tasks" a)))
      (is (= :b (d/get-value kv "tasks" b)))
      (is (= [[a :a] [b :b]] (d/get-range kv "tasks" [:closed a b])))
      (is (= [[z :z]] (d/get-range kv "tasks" [:greater-than a])))
      (is (= [a b z] (d/get-list kv "owners" :alice :keyword :app/task)))
      (is (= [[a :a] [b :b] [z :z]]
             (with-open [^AutoCloseable items
                         (d/range-seq kv "tasks" [:all] :data :data false
                                      {:batch-size 1})]
               (vec (seq items)))))
      (d/with-transaction-kv [tx kv]
        (d/register-type tx :app/committed task-type)
        (d/transact-kv tx "tasks" [[:put a :updated]])
        (is (= :updated (d/get-value tx "tasks" a))))
      (is (contains? (:types (custom/registry (local-kv name))) :app/committed))
      (is (thrown-with-msg? Exception #"abort"
                            (d/with-transaction-kv [tx kv]
                              (d/register-type tx :app/aborted task-type)
                              (d/transact-kv tx "tasks" [[:del b]])
                              (throw (ex-info "abort" {})))))
      (is (not (contains? (:types (custom/registry (local-kv name))) :app/aborted)))
      (is (= :b (d/get-value kv "tasks" b)))
      (d/transact-kv kv "tasks" [[:del a]])
      (is (nil? (d/get-value kv "tasks" a)))
      (is (= :b (d/get-value kv "tasks" b)))
      (finally (d/close-kv kv)))
    (close-server-database! name)
    (let [reopened (d/open-kv (uri name))]
      (try
        (d/open-dbi reopened "tasks")
        (is (= :app/task (d/register-type reopened :app/task task-type)))
        (is (= [[b :b] [z :z]] (d/get-range reopened "tasks" [:all])))
        (finally (d/close-kv reopened))))))

(deftest remote-custom-datalog
  (let [name "custom-datalog"
        conn (d/create-conn (uri name))]
    (try
      (is (= :app/task (d/register-type conn :app/task task-type)))
      (d/update-schema conn {:task/value {:db/valueType :app/task}
                             :task/id {:db/valueType :app/task
                                       :db/unique :db.unique/identity}
                             :task/many {:db/valueType :app/task
                                         :db/cardinality :db.cardinality/many}})
      (d/transact! conn [{:db/id 1 :task/value a :task/id a :task/many [a b z]}
                         {:db/id 2 :task/value b :task/id b}])
      (is (= #{[1 a] [2 b]} (d/q '[:find ?e ?v :where [?e :task/value ?v]] @conn)))
      (is (= #{[1]} (d/q '[:find ?e :in $ ?v :where [?e :task/value ?v]] @conn a)))
      (is (= b (:task/value (d/entity @conn [:task/id b]))))
      (is (= #{a b z} (set (:task/many (d/pull @conn '[*] 1)))))
      (is (= [a b] (mapv :v (d/index-range @conn :task/many a b))))
      (d/with-transaction [tx conn]
        (is (= :app/committed (d/register-type tx :app/committed task-type)))
        (d/transact! tx [[:db/retract 1 :task/many b]])
        (is (= #{a z} (set (:task/many (d/entity @tx 1))))))
      (is (contains? (:types (custom/registry (local-kv name))) :app/committed))
      (is (thrown-with-msg? Exception #"abort"
                            (d/with-transaction [tx conn]
                              (d/register-type tx :app/aborted task-type)
                              (d/transact! tx [[:db/add 2 :task/value z]])
                              (throw (ex-info "abort" {})))))
      (is (not (contains? (:types (custom/registry (local-kv name))) :app/aborted)))
      (is (= b (:task/value (d/entity @conn 2))))
      (finally (d/close conn)))
    (close-server-database! name)
    (let [reopened (d/create-conn (uri name))]
      (try
        (is (= #{[1 a] [2 b]}
               (d/q '[:find ?e ?v :where [?e :task/value ?v]] @reopened)))
        (finally (d/close reopened))))))

(defn- descriptor [kind]
  {:udf/lang :java :udf/kind kind :udf/id :app/task :udf/version 1})

(deftest remote-custom-server-udfs
  (let [name "custom-udf"
        definition {:index {:type :long :order-fn (descriptor :order-fn)}
                    :payload {:serialize (descriptor :serializer)
                              :deserialize (descriptor :deserializer)}}
        kv (d/open-kv (uri name))]
    (try
      ;; Registration and opening metadata need no loaded implementations.
      (is (= :app/task (d/register-type kv :app/task definition)))
      (d/open-dbi kv "tasks" {:key-type :app/task})
      (is (thrown-with-msg? Exception #"UDF|binding"
                            (d/transact-kv kv "tasks" [[:put a :a]])))
      (is (zero? (d/entries kv "tasks")))
      (udf/register! *registry* (descriptor :order-fn) :rank)
      (udf/register! *registry* (descriptor :serializer)
                     (fn [v]
                       (if (= "bad" (:name v))
                         (throw (ex-info "bad payload" {}))
                         (b/serialize v))))
      (udf/register! *registry* (descriptor :deserializer) b/deserialize)
      (d/transact-kv kv "tasks" [[:put a :a] [:put b :b]])
      (is (= [[a :a] [b :b]] (d/get-range kv "tasks" [:all])))
      (is (thrown-with-msg? Exception #"bad payload"
                            (d/transact-kv kv "tasks"
                                           [[:put z :z]
                                            [:put {:rank 3 :name "bad"} :bad]])))
      (is (= [[a :a] [b :b]] (d/get-range kv "tasks" [:all])))
      (udf/unregister! *registry* (descriptor :deserializer))
      (is (thrown-with-msg? Exception #"UDF|binding"
                            (d/get-range kv "tasks" [:all])))
      (udf/register! *registry* (descriptor :deserializer) b/deserialize)
      (is (= :a (d/get-value kv "tasks" a)))
      (is (nil? (:runtime-opts (i/env-opts (local-kv name)))))
      (finally (d/close-kv kv)))
    (close-server-database! name)
    (let [reopened (d/open-kv (uri name))]
      (try
        (d/open-dbi reopened "tasks")
        (is (= [[a :a] [b :b]] (d/get-range reopened "tasks" [:all])))
        (finally (d/close-kv reopened))))))

(deftest remote-custom-datalog-server-udfs
  (let [name "custom-datalog-udf"
        definition {:index {:type :long :order-fn (descriptor :order-fn)}
                    :payload {:serialize (descriptor :serializer)
                              :deserialize (descriptor :deserializer)}}
        conn (d/create-conn (uri name))
        order-threads (atom #{})
        main-thread (.getId (Thread/currentThread))]
    (try
      (d/register-type conn :app/task definition)
      (d/update-schema conn {:task/value {:db/valueType :app/task}})
      (is (thrown-with-msg? Exception #"UDF"
                            (d/transact! conn [{:db/id 1 :task/value a}])))
      (is (empty? (d/datoms @conn :eav)))
      (udf/register! *registry* (descriptor :order-fn)
                     (fn [v]
                       (swap! order-threads conj (.getId (Thread/currentThread)))
                       (:rank v)))
      (udf/register! *registry* (descriptor :serializer) b/serialize)
      (udf/register! *registry* (descriptor :deserializer) b/deserialize)
      (d/transact! conn [{:db/id 1 :task/value a} {:db/id 2 :task/value b}])
      (is (seq @order-threads))
      (is (not (contains? @order-threads main-thread)))
      (is (= #{[1]} (d/q '[:find ?e :in $ ?v :where [?e :task/value ?v]] @conn a)))
      (is (= b (:task/value (d/pull @conn '[*] 2))))
      ;; Both remote APIs address the same persisted registry and payload store.
      (let [kv (d/open-kv (uri name))]
        (try
          (is (= :app/task (d/register-type kv :app/task definition)))
          (d/open-dbi kv "tasks" {:key-type :app/task})
          (d/transact-kv kv "tasks" [[:put a :a]])
          (is (= :a (d/get-value kv "tasks" a)))
          (finally (d/close-kv kv))))
      (finally (d/close conn)))))

(deftest remote-custom-registration-permissions
  (let [admin (client/new-client (uri ""))]
    (try
      (client/create-user admin "reader" "reader-password")
      (client/create-role admin :custom-reader)
      (client/assign-role admin :custom-reader "reader")
      (doseq [[name db-type] [["custom-permission-kv" :key-value]
                             ["custom-permission-datalog" :datalog]]]
        (client/create-database admin name db-type)
        (client/grant-permission admin :custom-reader :datalevin.server/view
                                 :datalevin.server/database name)
        (let [url (str "dtlv://reader:reader-password@localhost/" name)
              handle (if (= db-type :datalog) (d/create-conn url) (d/open-kv url))]
          (try
            (is (thrown-with-msg? Exception #"permission"
                                  (d/register-type handle :app/task task-type)))
            (is (empty? (:types (custom/registry (local-kv name)))))
            (finally
              (if (= db-type :datalog) (d/close handle) (d/close-kv handle))))))
      (finally (client/disconnect admin)))))

(deftest remote-custom-registration-on-read-only-replica
  (doseq [datalog? [false true]]
    (let [name (if datalog? "custom-replica-datalog" "custom-replica-kv")
          handle (if datalog? (d/create-conn (uri name)) (d/open-kv (uri name)))
          dbs (.-dbs ^Server *server*)
          state (get dbs name)]
      (try
        (.put ^java.util.Map dbs name (assoc state :replica/read-only? true))
        (is (thrown-with-msg? Exception #"read-only"
                              (d/register-type handle :app/task task-type)))
        (is (empty? (:types (custom/registry (local-kv name)))))
        (finally
          (.put ^java.util.Map dbs name state)
          (if datalog? (d/close handle) (d/close-kv handle)))))))

(deftest remote-custom-session-rebind
  (let [definition {:index {:type :long :order-fn (descriptor :order-fn)}}
        kv (d/open-kv (uri "custom-restart-kv"))
        conn (d/create-conn (uri "custom-restart-datalog"))
        original ^Server *server*]
    (try
      (udf/register! *registry* (descriptor :order-fn) :rank)
      (d/register-type kv :app/task definition)
      (d/open-dbi kv "tasks" {:key-type :app/task})
      (d/transact-kv kv "tasks" [[:put a :a]])
      (d/register-type conn :app/task definition)
      (d/update-schema conn {:task/value {:db/valueType :app/task}})
      (d/transact! conn [{:db/id 1 :task/value a}])
      (server/stop original)
      (binding [*server* (server/create {:root (.-root original)
                                         :port (.-port original)})]
        (try
          (server/start *server*)
          ;; Persisted sessions reopen both stores before a new client request.
          (is (some? (local-kv "custom-restart-kv")))
          (is (some? (local-kv "custom-restart-datalog")))
          (is (= :a (d/get-value kv "tasks" a)))
          (is (= #{[1]}
                 (d/q '[:find ?e :in $ ?v :where [?e :task/value ?v]] @conn a)))
          (finally
            (d/close-kv kv)
            (d/close conn)
            (server/stop *server*))))
      (finally
        ;; Close is idempotent, including after a failed restart.
        (d/close-kv kv)
        (d/close conn)))))
