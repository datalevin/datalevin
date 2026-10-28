(ns datalevin.remote-transaction-metadata-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [datalevin.client :as client]
            [datalevin.client-op :as cop]
            [datalevin.constants :as c]
            [datalevin.core :as d]
            [datalevin.interface :as i]
            [datalevin.test.core :refer [server-fixture]])
  (:import [datalevin.remote DatalogStore]))

(use-fixtures :once server-fixture)

(def ^:private scalar-schema
  {:user/id {:db/valueType :db.type/string :db/unique :db.unique/identity}
   :value {:db/valueType :db.type/long}})

(defn- create-conn [wal?]
  (d/create-conn
    (str "dtlv://datalevin:datalevin@localhost/tx-metadata-" (random-uuid))
    scalar-schema {:wal? wal? :background-sampling? false}))

(deftest scalar-transactions-preserve-warmed-schema
  (binding [c/*remote-db-last-modified-check-interval-ms* 60000]
    (doseq [wal? [false true]
            transact [d/transact! d/transact-ack!]]
      (testing (str "WAL " wal? ", " transact)
        (with-open [^java.io.Closeable conn (create-conn wal?)]
          (d/transact! conn [{:db/id 1 :user/id "one" :value 0}])
          (let [store (:store @conn)
                snapshot (i/schema-snapshot store)
                schema (:schema snapshot)
                rschema (i/rschema store)
                revision (:revision snapshot)]
            (doseq [value (range 1 4)]
              (transact conn [[:db/add 1 :value value]])
              (is (= revision (:schema-revision (i/db-info store))))
              (is (identical? schema (i/schema store)))
              (is (identical? rschema (i/rschema store)))
              (is (= 1 (d/entid @conn [:user/id "one"])))
              (is (= value (:value (d/pull @conn [:value] [:user/id "one"])))))
            (testing "inferred schema changes still invalidate both maps"
              (transact conn [[:db/add 1 :inferred 42]])
              (is (< revision (:schema-revision (i/db-info store))))
              (is (contains? (i/schema store) :inferred))
              (is (not (identical? schema (i/schema store))))
              (is (not (identical? rschema (i/rschema store)))))))))))

(deftest transaction-replays-retain-the-committed-schema-revision
  (doseq [wal? [false true]
          kind [:tx-data+db-info :tx-data-ack]]
    (testing (str "WAL " wal? ", " kind)
      (with-open [^java.io.Closeable conn (create-conn wal?)]
        (let [^DatalogStore store (:store @conn)
              client (.-client store)
              db-name (i/db-name store)
              before (:revision (i/schema-snapshot store))
              txs [{:db/id 1 :user/id "one" :inferred 42}]
              request {:type kind :mode :request :writing? false
                       :args [db-name txs false]
                       :client-op-id (str (random-uuid))
                       :client-op-hash (cop/request-hash
                                         (cop/tx-request-payload kind db-name txs false))
                       :client-op-response-kind kind}
              response (client/request client request)
              info (get-in response [:result :db-info])
              revision (:revision (i/schema-snapshot store))]
          (is (= :command-complete (:type response)))
          (is (< before revision))
          (is (= revision (:schema-revision info)))
          (d/transact! conn [[:db/add 1 :later-attribute 7]])
          (is (< revision (:revision (i/schema-snapshot store))))
          (let [replay (client/request client request)]
            (is (= :command-complete (:type replay)))
            (is (= info (get-in replay [:result :db-info])))))))))
