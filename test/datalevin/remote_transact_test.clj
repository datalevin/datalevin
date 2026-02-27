(ns datalevin.remote-transact-test
  (:require
   [datalevin.conn :as conn]
   [datalevin.core :as d]
   [datalevin.util :as u]
   [datalevin.datom :as dd]
   [datalevin.constants :as c]
   [datalevin.test.core :as tdc :refer [server-fixture]]
   [clojure.test :as t :refer [is are deftest testing use-fixtures]])
  (:import
   [java.util UUID]))

(use-fixtures :each server-fixture)

(deftest datalog-larger-tx-test
  (let [dir  "dtlv://datalevin:datalevin@localhost/large-tx-test"
        end  3000
        conn (d/create-conn dir nil {:auto-entity-time? true})
        vs   (range 0 end)
        txs  (map (fn [a v] {a v}) (repeat :id) vs)]
    (d/transact! conn txs)
    (is (= (d/q '[:find (count ?e)
                  :where [?e :id]]
                @conn)
           [[end]]))
    (let [[c u] (d/q '[:find [?c ?u]
                       :in $ ?i
                       :where
                       [?e :id ?i]
                       [?e :db/created-at ?c]
                       [?e :db/updated-at ?u]]
                     @conn 1)]
      (is c)
      (is u)
      (is (= c u)))
    (d/close conn)))

(deftest simulated-tx-test
  (let [dir  "dtlv://datalevin:datalevin@localhost/simulated-tx"
        conn (d/create-conn dir
                            {:id {:db/unique    :db.unique/identity
                                  :db/valueType :db.type/long}})]
    (let [rp (d/transact! conn [{:id 1}])]
      (is (= (:tx-data rp) [(d/datom 1 :id 1)]))
      (is (= (dd/datom-tx (first (:tx-data rp))) 2)))
    (is (= (d/datoms @conn :eav) [(d/datom 1 :id 1)]))
    (is (= (:max-eid @conn) 1))
    (is (= (:max-tx @conn) 2))

    (let [rp (d/tx-data->simulated-report @conn [{:id 2}])]
      (is (= (:tx-data rp) [(d/datom 2 :id 2)]))
      (is (= (dd/datom-tx (first (:tx-data rp))) 3))
      (is (= (:max-eid (:db-after rp)) 2))
      (is (= (:max-tx (:db-after rp)) 3)))

    (is (= (d/datoms @conn :eav) [(d/datom 1 :id 1)]))
    (is (= (:max-eid @conn) 1))
    (is (= (:max-tx @conn) 2))

    (d/close conn)))

(deftest txn-test
  (let [schema {:country/short {:db/valueType :db.type/string
                                :db/unique    :db.unique/identity}
                :country/long  {:db/valueType :db.type/string}}
        conn-r (d/create-conn "dtlv://datalevin:datalevin@localhost:8898/dl-test"
                              schema)
        dir    (u/tmp-dir (str "lmdb-test-" (UUID/randomUUID)))
        conn-l (d/create-conn dir schema)]

    (is (= (:max-eid @conn-r) (:max-eid @conn-l) c/e0))
    (d/transact! conn-r [{:country/short "RU" :country/long "Russia"}
                         {:country/short "FR" :country/long "France"}
                         {:country/short "DE" :country/long "Germany"}])
    (d/transact! conn-l [{:country/short "RU" :country/long "Russia"}
                         {:country/short "FR" :country/long "France"}
                         {:country/short "DE" :country/long "Germany"}])
    (is (= (:max-eid @conn-r) (:max-eid @conn-l) 3))

    (d/transact! conn-r [{:country/short "AZ" :country/long "Azerbaijan"}])
    (d/transact! conn-l [{:country/short "AZ" :country/long "Azerbaijan"}])
    (is (= (:max-eid @conn-r) (:max-eid @conn-l) 4))
    (is (= 4 (d/q '[:find (count ?e) . :in $ :where [?e]] (d/db conn-r))))
    (is (= 4 (d/q '[:find (count ?e) . :in $ :where [?e]] (d/db conn-l))))

    (d/close conn-r)
    (d/close conn-l)
    (u/delete-files dir)))

(deftest test-transact-async
  (let [dir  "dtlv://datalevin:datalevin@localhost/transact-async"
        conn (d/create-conn
               dir
               {:aka {:db/cardinality :db.cardinality/many}}
               {:kv-opts {:flags (conj c/default-env-flags :nosync)}})]
    @(d/transact-async conn [[:db/add 1 :name "Ivan"]])
    @(d/transact-async conn [[:db/add 1 :name "Petr"]])
    @(d/transact-async conn [[:db/add 1 :aka "Devil"]])
    @(d/transact-async conn [[:db/add 1 :aka "Tupen"]])
    (is (= (d/q '[:find ?v
                  :where [1 :name ?v]] @conn)
           #{["Petr"]}))
    (is (= (d/q '[:find ?v
                  :where [1 :aka ?v]] @conn)
           #{["Devil"] ["Tupen"]}))
    (d/close conn)))

(deftest test-transact
  (let [dir  "dtlv://datalevin:datalevin@localhost/transact"
        conn (d/create-conn
               dir
               {:aka {:db/cardinality :db.cardinality/many}}
               {:kv-opts {:flags (conj c/default-env-flags :nosync)}})]
    (d/transact conn [[:db/add 1 :name "Ivan"]])
    (d/transact conn [[:db/add 1 :name "Petr"]])
    (d/transact conn [[:db/add 1 :aka "Devil"]])
    (d/transact conn [[:db/add 1 :aka "Tupen"]])
    (is (= (d/q '[:find ?v
                  :where [1 :name ?v]] @conn)
           #{["Petr"]}))
    (is (= (d/q '[:find ?v
                  :where [1 :aka ?v]] @conn)
           #{["Devil"] ["Tupen"]}))
    (d/close conn)))

(deftest test-transact-after-async
  (let [dir  "dtlv://datalevin:datalevin@localhost/transact-after"
        conn (d/create-conn
               dir
               {:aka {:db/cardinality :db.cardinality/many}}
               {:kv-opts {:flags (conj c/default-env-flags :nosync)}})]
    (d/transact-async conn [[:db/add 1 :name "Ivan"]])
    (d/transact-async conn [[:db/add 1 :name "Petr"]])
    (d/transact-async conn [[:db/add 1 :aka "Devil"]])
    (d/transact conn [[:db/add 1 :aka "Tupen"]])
    (is (= (d/q '[:find ?v
                  :where [1 :name ?v]] @conn)
           #{["Petr"]}))
    (is (= (d/q '[:find ?v
                  :where [1 :aka ?v]] @conn)
           #{["Devil"] ["Tupen"]}))
    (d/close conn)))

(deftest remote-non-explicit-writes-use-sync-queue-test
  (let [dir   (str "dtlv://datalevin:datalevin@localhost/"
                   (UUID/randomUUID))
        conn  (d/create-conn
                dir
                {:id {:db/valueType :db.type/long}}
                {:kv-opts {:wal? true
                           :wal-durability-profile :strict}})
        paths (atom [])]
    (try
      (binding [conn/*txlog-sync-path-observer*
                #(swap! paths conj %)]
        (d/transact! conn [{:id 1}]))
      (is (= [:queued-strict] @paths))
      (is (= 1 (d/q '[:find (count ?e) .
                      :where [?e :id]]
                    (d/db conn))))
      (finally
        (d/close conn)))))
