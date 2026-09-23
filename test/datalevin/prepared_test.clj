(ns datalevin.prepared-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [datalevin.client :as client]
            [datalevin.core :as d]
            [datalevin.kv :as kv]
            [datalevin.prepared :as prepared]
            [datalevin.protocol :as p]
            [datalevin.remote :as remote]
            [datalevin.server :as server]
            [datalevin.test.core :refer [allocate-port db-fixture]]
            [datalevin.timeout :as timeout]
            [datalevin.util :as u])
  (:import [datalevin.client Connection]
           [datalevin.remote KVStore]
           [datalevin.server Server]
           [java.nio ByteBuffer BufferOverflowException]
           [java.nio.channels SelectionKey SocketChannel]
           [java.util.concurrent ConcurrentHashMap]
           [java.util.concurrent.atomic AtomicLong]
           [java.util UUID]))

(use-fixtures :each db-fixture)

(deftest prepared-datalog-requests-preserve-ha-read-floors
  (let [request (prepared/request ["db" [:name] nil nil])
        client (reify client/IClient
                 (request [_ req]
                   (is (some? (::prepared/id (meta req))))
                   (is (= ["db" [:name] 1 nil] (:args req)))
                   {:type :command-complete :result (:ha-read-min-tx req)}))]
    (doseq [outer [nil 4 9] current [0 4 9] writing? [false true]]
      (binding [client/*ha-read-min-tx* outer]
        (is (= (when (and (not writing?) (pos? (long current))) current)
               (#'remote/datalog-prepared-request
                 (AtomicLong. (long current)) client :pull request 1 writing?)))
        (is (= outer client/*ha-read-min-tx*))))))

(deftest prepared-kv-refreshes-dbis-and-owns-each-read
  (let [dir (u/tmp-dir (str "prepared-kv-" (UUID/randomUUID)))
        db (d/open-kv dir)]
    (try
      (d/open-dbi db "docs")
      (d/transact-kv db [[:put "docs" 1 {:data false} :long :data]
                         [:put "docs" 2 false :long :data]])
      (let [read-doc (d/prepare-get-value db "docs" :long :data)
            read-pair (d/prepare-get-value db "docs" :long :data false)]
        (is (= {:data false} (d/execute-prepared read-doc 1)))
        (is (= [1 {:data false}] (apply read-pair [1])))
        (is (= [2 false] (read-pair 2)))
        (is (nil? (read-pair 99)))
        (is (= {:data false} (read-doc 1)))
        (d/transact-kv db [[:put "docs" 1 {:data :updated} :long :data]])
        (is (= {:data :updated} (read-doc 1)))
        (is (every? #(= {:data :updated} %)
                    (mapv deref (repeatedly 4 #(future (read-doc 1))))))
        (d/drop-dbi db "docs")
        (is (thrown? Exception (read-doc 1)))
        (d/open-dbi db "docs")
        (d/transact-kv db [[:put "docs" 1 {:data :recreated} :long :data]])
        (is (= {:data :recreated} (read-doc 1)))
        (d/with-transaction-kv [tx db]
          (let [read-tx (d/prepare-get-value tx "docs" :long :data)]
            (is (= {:data :recreated} (read-tx 1)))
            (d/transact-kv tx [[:put "docs" 1 :in-transaction :long :data]])
            (is (= :in-transaction (read-tx 1)))))
        (is (= :in-transaction (read-doc 1)))
        (d/close-kv db)
        (is (thrown? Exception (read-doc 1))))
      (finally (d/close-kv db) (u/delete-files dir)))))

(deftest prepared-pull-refreshes-schema-and-preserves-options
  (let [dir (u/tmp-dir (str "prepared-pull-" (UUID/randomUUID)))
        conn (d/create-conn dir {:key {:db/unique :db.unique/identity}
                                :name {} :value {}
                                :friend {:db/valueType :db.type/ref}})]
    (try
      (d/transact! conn [{:db/id 1 :key "one" :name "one" :value 1 :friend 2}
                        {:db/id 2 :name "two"}])
      (let [view @conn
            read-entity (d/prepare-pull view [:db/id :name :value])]
        (is (= (d/pull view [:db/id :name :value] 1) (read-entity 1)))
        (is (= {:db/id 1 :name "one" :value 1} (read-entity [:key "one"])))
        (is (nil? (read-entity [:key "absent"])))
        (d/update-schema conn {:value {:db/cardinality :db.cardinality/many}})
        (d/transact! conn [[:db/add 1 :value 2]])
        (is (= (d/pull view [:db/id :name :value] 1) (read-entity 1)))
        (is (= [1 2] (:value (read-entity 1)))))
      (doseq [pattern ['[*] [] [:unknown] '[[:unknown :default false]]
                       '[[:name :as :label]] '[{:friend [:name]}]]]
        (let [read-entity (d/prepare-pull @conn pattern)]
          (doseq [id [1 2 99 [:key "one"] [:key "absent"]]]
            (is (= (d/pull @conn pattern id) (read-entity id)) (str pattern id)))))
      (let [visits (atom [])
            reader (d/prepare-pull @conn [:name] {:visitor #(swap! visits conj [%1 %2 %3 %4])})]
        (is (= {:name "one"} (reader 1)))
        (is (= {:name "two"} (reader 2)))
        (is (= 2 (count @visits))))
      (is (thrown? Exception ((d/prepare-pull @conn [:name] {:timeout -1}) 1)))
      (binding [timeout/*deadline* 1]
        (is (= {:name "one"} ((d/prepare-pull @conn [:name]) 1)))
        (is (= 1 timeout/*deadline*)))
      (d/with-transaction [tx conn]
        (let [reader (d/prepare-pull @tx [:name])]
          (is (= {:name "one"} (reader 1)))
          (d/transact! tx [[:db/add 1 :name (.repeat "x" 3000)]])
          (is (= {:name (.repeat "x" 3000)} (reader 1)))))
      (finally (d/close conn) (u/delete-files dir)))))

(deftest prepared-encoding-retries-release-the-snapshot
  (let [dir (u/tmp-dir (str "prepared-encode-" (UUID/randomUUID)))
        db (d/open-kv dir)]
    (try
      (d/open-dbi db "docs")
      (d/transact-kv db [[:put "docs" 1 {:text (.repeat "x" 10000)} :long :data]])
      (let [reader (kv/value-reader "docs" :long :data true)
            result (reader db 1 true)
            small (ByteBuffer/allocate 16)
            large (ByteBuffer/allocate 20000)]
        (is (thrown? BufferOverflowException (p/write-message-bf small {:result result})))
        (d/transact-kv db [[:put "docs" 1 {:text "new snapshot"} :long :data]])
        (p/write-message-bf large {:result result})
        (is (= {:text "new snapshot"} (:result (first (p/receive-one-message large))))))
      (finally (d/close-kv db) (u/delete-files dir)))))

(defn- server-handles [^Server srv db]
  ;; Inspect the idle connection after its reply. Copy the map without touching
  ;; its access order; the test sends no next request until inspection finishes.
  (let [pool (client/get-pool (.-client ^KVStore db))
        ^Connection socket (client/get-connection pool)]
    (try
      (let [address (.getLocalAddress ^SocketChannel (.-ch socket))
            keys ^ConcurrentHashMap (:connection-keys (.-execution srv))]
        (some (fn [^SelectionKey key]
                (when (= address (.getRemoteAddress ^SocketChannel (.channel key)))
                  (into {} (:prepared-handles @(.attachment key)))))
              (.values keys)))
      (finally (client/release-connection pool socket)))))

(deftest remote-prepared-reads-register-reuse-evict-and-reconnect
  (let [root (u/tmp-dir (str "prepared-server-" (UUID/randomUUID)))
        port (allocate-port)
        srv (server/create {:root root :port port})
        base (str "dtlv://datalevin:datalevin@localhost:" port)]
    (try
      (server/start srv)
      (let [db (d/open-kv (str base "/kv") {:client-opts {:pool-size 1}})
            conn (d/create-conn (str base "/dl") {:key {:db/unique :db.unique/identity}
                                                  :name {} :value {}}
                                {:client-opts {:pool-size 1}})]
        (try
          (d/open-dbi db "docs")
          (d/transact-kv db [[:put "docs" 1 {:value :one} :long :data]
                             [:put "docs" 2 {:value :two} :long :data]])
          (d/transact! conn [{:db/id 1 :key "one" :name "one" :value 1}
                            {:db/id 2 :key "two" :name "two" :value 2}])
          (let [reader (d/prepare-get-value db "docs" :long :data)
                puller (d/prepare-pull @conn [:name :value])]
            (is (= {:value :one} (reader 1)))
            (is (= {:name "one" :value 1} (puller [:key "one"])))
            (is (= {:name "two" :value 2} (puller [:key "two"])))
            (is (= 2 (d/count-datoms @conn nil :name nil)))
            (is (= 2 (d/count-datoms @conn nil :value nil)))
            (testing "remote counts forward to the matching storage size"
              (is (= 3 (d/count-datoms @conn 1 nil nil)))
              (is (= 1 (d/count-datoms @conn nil :name "one")))
              (is (= 1 (d/count-datoms @conn nil :value 1)))
              (is (= 0 (d/count-datoms @conn nil nil "one"))))
            (let [[id entry] (first (server-handles srv db))]
              (is (some? id))
              (is (= {:value :two} (reader 2)))
              (is (identical? entry (get (server-handles srv db) id)))
              (testing "another socket cannot use this connection's handle"
                (let [other (#'client/new-connection "localhost" port 2000)]
                  (try
                    (client/send-n-receive other {:type :set-client-id
                                                  :client-id (client/get-id (.-client ^KVStore db))
                                                  :wire-capabilities (p/local-wire-capabilities)})
                    (is (= :prepared/missing
                           (get-in (client/send-n-receive
                                     other {:type :execute-prepared :handle id
                                            :value 1}) [:err-data :error])))
                    (finally (client/close other)))))
              (testing "server eviction triggers one registration retry"
                (let [pool (client/get-pool (.-client ^KVStore db))
                      socket (client/get-connection pool)]
                  (try
                    (dotimes [n prepared/max-handles]
                      (is (= :command-complete
                             (:type (client/send-n-receive
                                      socket {:type :get-value :args ["kv" "docs" 1 :long :data true]
                                              :prepare-id (+ 100000 n)})))))
                    (finally (client/release-connection pool socket))))
                (is (= prepared/max-handles (count (server-handles srv db))))
                (is (not (contains? (server-handles srv db) id)))
                (is (= {:value :one} (reader 1)))
                (is (contains? (server-handles srv db) id))
                (is (not (identical? entry (get (server-handles srv db) id)))))
              (testing "a replacement connection registers automatically"
                (let [pool (client/get-pool (.-client ^KVStore db))
                      socket (client/get-connection pool)]
                  (client/close socket)
                  (client/release-connection pool socket))
                (is (= {:value :two} (reader 2)))
                (is (= #{id} (set (keys (server-handles srv db))))))
              (d/drop-dbi db "docs")
              (d/open-dbi db "docs")
              (d/transact-kv db [[:put "docs" 1 :recreated :long :data]])
              (is (= :recreated (reader 1)))
              (d/update-schema conn {:value {:db/cardinality :db.cardinality/many}})
              (d/transact! conn [[:db/add 1 :value 3]])
              (is (= {:name "one" :value [1 3]} (puller [:key "one"])))
              (testing "a forwarded remote attribute count reflects committed writes"
                (is (= 2 (d/count-datoms @conn nil :name nil)))
                (is (= 3 (d/count-datoms @conn nil :value nil)))
                (is (= 4 (d/count-datoms @conn 1 nil nil)))
                (is (= 1 (d/count-datoms @conn nil :value 3)))
                (is (= 0 (d/count-datoms @conn nil nil "one"))))
              (d/with-transaction-kv [tx db]
                (let [tx-reader (d/prepare-get-value tx "docs" :long :data)]
                  (is (= :recreated (tx-reader 1)))
                  (d/transact-kv tx [[:put "docs" 1 :uncommitted :long :data]])
                  (is (= :uncommitted (tx-reader 1)))))
              (d/with-transaction [tx conn]
                (let [tx-puller (d/prepare-pull @tx [:name])]
                  (is (= {:name "one"} (tx-puller 1)))
                  (d/transact! tx [[:db/add 1 :name "uncommitted"]])
                  (is (= {:name "uncommitted"} (tx-puller 1)))))
              (testing "disabling preparation on a connection preserves read semantics"
                (let [pool (client/get-pool (.-client ^KVStore db))
                      socket (client/get-connection pool)]
                  (try
                    (#'client/set-conn-wire-opts!
                      socket (dissoc (p/negotiate-wire-opts (p/local-wire-capabilities))
                                     :prepared-read?))
                    (finally (client/release-connection pool socket))))
                (dotimes [_ 2] (is (= :uncommitted (reader 1)))))
              (d/close-kv db)
              (is (thrown? Exception (reader 1)))))
          (finally (d/close-kv db) (d/close conn))))
      (finally (server/stop srv) (u/delete-files root)))))
