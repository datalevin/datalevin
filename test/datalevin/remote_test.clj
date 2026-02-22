(ns datalevin.remote-test
  (:require
   [datalevin.remote :as sut]
   [datalevin.interpret :as i]
   [datalevin.interface :as if]
   [datalevin.datom :as d]
   [datalevin.core :as dc]
   [datalevin.db :as db]
   [datalevin.constants :as c]
   [datalevin.client :as cl]
   [datalevin.test.core :refer [server-fixture]]
   [clojure.test :refer [is testing deftest use-fixtures]])
  (:import
   [java.util UUID]
   [datalevin.datom Datom]))

(use-fixtures :each server-fixture)

(deftest remote-idoc-test
  (let [dir    "dtlv://datalevin:datalevin@localhost/remote-idoc-test"
        schema {:doc/idoc {:db/valueType :db.type/idoc
                           :db/domain    "profiles"}}
        conn   (dc/create-conn
                 dir
                 schema
                 {:kv-opts {:flags (conj c/default-env-flags :nosync :nolock)}})]
    (dc/transact!
      conn
      [{:db/id    1
        :doc/idoc {:status  "active"
                   :profile {:age 30}
                   :tags    ["a" "b"]}}
       {:db/id    2
        :doc/idoc {:status  "inactive"
                   :profile {:age 40}}}])
    (let [db (dc/db conn)]
      (is (= #{[1]}
             (dc/q '[:find ?e
                     :in $
                     :where
                     [(idoc-match $ :doc/idoc {:status "active"})
                      [[?e ?a ?v]]]]
                   db)))
      (is (= #{[1]}
             (dc/q '[:find ?e
                     :in $
                     :where
                     [(idoc-match $ :doc/idoc {:tags "b"})
                      [[?e ?a ?v]]]]
                   db)))
      (is (= #{[2]}
             (dc/q '[:find ?e
                     :in $ ?q
                     :where
                     [(idoc-match $ :doc/idoc ?q) [[?e ?a ?v]]]]
                   db
                   '(> [:profile :age] 35))))
      (is (= #{[1]}
             (dc/q '[:find ?e
                     :in $ ?q
                     :where
                     [(idoc-match $ ?q {:domains ["profiles"]})
                      [[?e ?a ?v]]]]
                   db
                   {:status "active"}))))
    (dc/close conn)))

(deftest dt-store-ops-test
  (testing "permission"
    (is (thrown? Exception (sut/open "dtlv://someone:wrong@localhost/nodb")))

    (let [client (cl/new-client "dtlv://datalevin:datalevin@localhost")]
      ;; TODO fix this
      ;; (is (= 0 (count (cl/list-databases client))))

      (cl/create-user client "dbadmin" "secret")
      (cl/grant-permission client :datalevin.role/dbadmin
                           :datalevin.server/create
                           :datalevin.server/database
                           nil)
      (is (= 3 (count (cl/list-user-permissions client "dbadmin"))))

      (let [client1 (cl/new-client "dtlv://dbadmin:secret@localhost")]
        (cl/create-database client1 "ops-test" :datalog)
        (is (= 1 (count (cl/list-databases client1)))))

      (cl/create-user client "db-user" "secret")
      (cl/grant-permission client :datalevin.role/db-user
                           :datalevin.server/alter
                           :datalevin.server/database
                           "ops-test")
      (is (= 3 (count (cl/list-user-permissions client "db-user"))))
      (is (thrown? Exception
                   (sut/open "dtlv://db-user:secret@localhost/nodb")))

      (cl/create-user client "viewer" "secret")
      (cl/grant-permission client :datalevin.role/viewer
                           :datalevin.server/view
                           :datalevin.server/database
                           nil)
      (is (thrown? Exception
                   (sut/open "dtlv://viewer:secret@localhost/nodb")))
      ))

  (testing "datalog store ops"
    (let [dir   "dtlv://db-user:secret@localhost/ops-test"
          store (sut/open dir)]
      (is (instance? datalevin.remote.DatalogStore store))
      (is (= c/implicit-schema (if/schema store)))
      (is (= c/e0 (if/init-max-eid store)))
      (is (thrown-with-msg?
            Exception #"Trusted internal apply is unavailable on remote stores"
            (if/apply-prepared-datoms store [(d/datom c/e0 :a/b "v")])))
      (is (thrown? Exception
                   (if/load-datoms store [[:db/add c/e0 :bad "payload"]])))
      (let [a  :a/b
            v  (UUID/randomUUID)
            d  (d/datom c/e0 a v)
            s  (assoc (if/schema store) a {:db/aid 3})
            b  :b/c
            p1 {:db/valueType :db.type/uuid}
            v1 (UUID/randomUUID)
            d1 (d/datom c/e0 b v1)
            s1 (assoc s b (merge p1 {:db/aid 4}))
            c  :c/d
            p2 {:db/valueType :db.type/ref}
            v2 (long (rand c/emax))
            d2 (d/datom c/e0 c v2)
            s2 (assoc s1 c (merge p2 {:db/aid 5}))
            t1 (if/last-modified store)]
        (if/load-datoms store [d])
        (is (<= t1 (if/last-modified store)))
        (is (= s (if/schema store)))
        (is (= 1 (if/datom-count store :eav)))
        (is (= 1 (if/datom-count store :ave)))
        (is (= [d] (if/fetch store d)))
        (is (= [d] (if/slice store :eav d d)))
        (is (if/populated? store :eav d d))
        (is (= 1 (if/size store :eav d d)))
        (is (= d (if/head store :eav d d)))
        (if/swap-attr store b (i/inter-fn [& ms] (apply merge ms)) p1)
        (if/load-datoms store [d1])
        (is (= s1 (if/schema store)))
        (is (= 2 (if/datom-count store :eav)))
        (is (= 2 (if/datom-count store :ave)))
        (is (= [] (if/slice store :eav d (d/datom c/e0 :non-exist v1))))
        (is (nil? (if/populated? store :eav d (d/datom c/e0 :non-exist v1))))
        (is (= d (if/head store :eav d d1)))
        (is (= 2 (if/size store :eav d d1)))
        (is (= [d d1] (if/slice store :eav d d1)))
        (is (= [d d1] (if/slice store :ave d d1)))
        (is (= [d1 d] (if/rslice store :eav d1 d)))
        (is (= [d d1] (if/slice store :eav
                                (d/datom c/e0 a nil)
                                (d/datom c/e0 nil nil))))
        (is (= [d1 d] (if/rslice store :eav
                                 (d/datom c/e0 b nil)
                                 (d/datom c/e0 nil nil))))
        (is (= 1 (if/size-filter store :eav
                                 (i/inter-fn [^Datom d] (= v (dc/datom-v d)))
                                 (d/datom c/e0 nil nil)
                                 (d/datom c/e0 nil nil))))
        (is (= d (if/head-filter store :eav
                                 (i/inter-fn [^Datom d]
                                             (when (= v (dc/datom-v d))
                                               d))
                                 (d/datom c/e0 nil nil)
                                 (d/datom c/e0 nil nil))))
        (is (= [d] (if/slice-filter store :eav
                                    (i/inter-fn [^Datom d]
                                                (when (= v (dc/datom-v d)) d))
                                    (d/datom c/e0 nil nil)
                                    (d/datom c/e0 nil nil))))
        (is (= [d1 d] (if/rslice store :ave d1 d)))
        (is (= [d d1] (if/slice store :ave
                                (d/datom c/e0 a nil)
                                (d/datom c/e0 nil nil))))
        (is (= [d1 d] (if/rslice store :ave
                                 (d/datom c/e0 b nil)
                                 (d/datom c/e0 nil nil))))
        (is (= [d] (if/slice-filter store :ave
                                    (i/inter-fn [^Datom d]
                                                (when (= v (dc/datom-v d)) d))
                                    (d/datom c/e0 nil nil)
                                    (d/datom c/e0 nil nil))))
        (if/swap-attr store c (i/inter-fn [& ms] (apply merge ms)) p2)
        (if/load-datoms store [d2])
        (is (= s2 (if/schema store)))
        (is (= 3 (if/datom-count store c/eav)))
        (is (= 3 (if/datom-count store c/ave)))
        (if/load-datoms store [(d/delete d)])
        (is (= 2 (if/datom-count store c/eav)))
        (is (= 2 (if/datom-count store c/ave)))
        (if/close store)
        (is (if/closed? store))
        (let [store (sut/open dir)]
          (is (= [d1] (if/slice store :eav d1 d1)))
          (if/load-datoms store [(d/delete d1)])
          (is (= 1 (if/datom-count store c/eav)))
          (if/load-datoms store [d d1])
          (is (= 3 (if/datom-count store c/eav)))
          (if/close store))
        (let [d     :d/e
              p3    {:db/valueType :db.type/long}
              s3    (assoc s2 d (merge p3 {:db/aid 6}))
              s4    (assoc s3 :f/g {:db/aid 7 :db/valueType :db.type/string})
              store (sut/open dir {d p3})]
          (is (= s3 (if/schema store)))
          (if/set-schema store {:f/g {:db/valueType :db.type/string}})
          (is (= s4 (if/schema store)))
          (if/close store)))))

  (testing "data viewer permission"
    (let [dir   "dtlv://viewer:secret@localhost/ops-test"
          store (sut/open dir)]
      (is (instance? datalevin.remote.DatalogStore store))
      (is (not= c/implicit-schema (if/schema store)))
      (is (= 3 (if/datom-count store c/eav)))

      (is (thrown? Exception (if/set-schema store {:o/p {}})))
      (is (thrown? Exception (if/load-datoms store [])))
      (if/close store))))

(deftest dt-store-larger-test
  (let [dir   "dtlv://datalevin:datalevin@localhost/larger-test"
        end   1000
        store (sut/open dir)
        vs    (range 0 end)
        txs   (mapv d/datom (range c/e0 (+ c/e0 end)) (repeat :id)
                    vs)
        pred  (i/inter-fn [d] (odd? (dc/datom-v d)))
        pred1 (i/inter-fn [d] (when (odd? (dc/datom-v d)) d))]
    (is (instance? datalevin.remote.DatalogStore store))
    (if/load-datoms store txs)
    (is (= (d/datom c/e0 :id 0)
           (if/head store :eav (d/datom c/e0 :id nil)
                    (d/datom c/emax :id nil))))
    (is (= (d/datom (dec (+ c/e0 end) ) :id (dec (+ c/e0 end)))
           (if/tail store :ave (d/datom c/emax :id nil)
                    (d/datom c/e0 :id nil))))
    (is (= (filter pred txs)
           (if/slice-filter store :eav pred1
                            (d/datom c/e0 nil nil)
                            (d/datom c/emax nil nil))))
    (is (= (reverse txs)
           (if/rslice store :eav
                      (d/datom c/emax nil nil) (d/datom c/e0 nil nil))))
    (if/close store)))

(deftest same-client-multiple-dbs-test
  (let [uri-str "dtlv://datalevin:datalevin@localhost"
        client  (cl/new-client uri-str)
        store1  (sut/open-kv client (str uri-str "/mykv") nil)
        store2  (sut/open client (str uri-str "/mydt") nil nil)]
    (is (instance? datalevin.remote.KVStore store1))
    (is (instance? datalevin.remote.DatalogStore store2))

    (dc/open-dbi store1 "a")
    (dc/transact-kv store1 [[:put "a" "hello" "world"]])
    (is (= (dc/get-value store1 "a" "hello") "world"))

    (let [conn (dc/conn-from-db (db/new-db store2))]
      (dc/transact! conn [{:hello "world"}])
      (is (= (dc/q '[:find ?w .
                     :where
                     [_ :hello ?w]]
                   @conn)
             "world"))
      (is (= (:actual-result-size (dc/explain {:run? true}
                                              '[:find ?w .
                                                :where
                                                [_ :hello ?w]]
                                              @conn))
             1))
      (dc/close conn))
    (dc/close-kv store1)))
