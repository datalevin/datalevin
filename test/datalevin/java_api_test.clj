;;
;; Copyright (c) Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns datalevin.java-api-test
  (:require
   [clojure.test :refer [deftest is use-fixtures]]
   [datalevin.json-api :as sut]
   [datalevin.test.core :as tdc]
   [datalevin.util :as u])
  (:import
   [datalevin Client Connection DatalogQuery Datalevin KV PullSelector QueryClause Rules Schema Tx TxData]
   [java.util List Map UUID]))

(defn- clean-java-api-state
  [f]
  (#'sut/clear-handles!)
  (try
    (f)
    (finally
      (#'sut/clear-handles!))))

(use-fixtures :each clean-java-api-state)

(deftest java-friendly-local-api-test
  (let [conn-dir (u/tmp-dir "java-friendly-conn")
        kv-dir   (u/tmp-dir "java-friendly-kv")
        schema   (doto (Datalevin/schema)
                   (.attr "name" (doto (Schema/attribute)
                                   (.valueType "db.type/string")
                                   (.unique "db.unique/identity")))
                   (.attr "age" (doto (Schema/attribute)
                                  (.valueType "db.type/long")))
                   (.attr "friend" (doto (Schema/attribute)
                                     (.valueType "db.type/ref")
                                     (.cardinality "db.cardinality/many"))))
        conn     (Datalevin/createConn conn-dir schema)
        kv       (Datalevin/openKV kv-dir)]
    (try
      (is (instance? Map (Datalevin/apiInfo)))
      (with-open [^Connection c conn
                  ^KV k kv]
        (let [e-var     (Datalevin/var "e")
              name-var  (Datalevin/var "name")
              limit-var (Datalevin/var "limit")
              from-var  (Datalevin/var "from")
              to-var    (Datalevin/var "to")
              mid-var   (Datalevin/var "mid")
              age-var   (Datalevin/var "age")
              schema*   (.updateSchema c (doto (Datalevin/schema)
                                           (.attr "city" (doto (Schema/attribute)
                                                           (.valueType "db.type/string")
                                                           (.doc "City name")))
                                           (.attr "nickname" (doto (Schema/attribute)
                                                               (.valueType "db.type/string")))
                                           (.attr "tag" (doto (Schema/attribute)
                                                          (.valueType "db.type/string")))))
              ^TxData tx (doto (Datalevin/tx)
                           (.entity (doto (Tx/entity -1)
                                      (.put "name" "Alice")
                                      (.put "age" 30)))
                           (.entity (doto (Tx/entity -2)
                                      (.put "name" "Bob")
                                      (.put "age" 25)))
                           (.entity (doto (Tx/entity -3)
                                      (.put "name" "Charlie")
                                      (.put "age" 40)))
                           (.add -2 "friend" -1)
                           (.add -3 "friend" -2)
                           (.add [":name" "Alice"] "city" "Paris")
                           (.add [":name" "Alice"] "tag" ":vip"))
              ^DatalogQuery query (doto (Datalevin/query)
                                    (.findAll "?name")
                                    (.whereDatom e-var "name" name-var))
              ^DatalogQuery scalar (doto (Datalevin/query)
                                     (.findScalar "?limit")
                                     (.whereBind "ground" limit-var (into-array Object [30])))
              ^Rules rules (-> (Datalevin/rules)
                               (.rule "linked" (into-array String ["?from" "?to"]))
                               (.whereDatom from-var "friend" to-var)
                               (.end)
                               (.rule "linked" (into-array String ["?from" "?to"]))
                               (.whereDatom from-var "friend" mid-var)
                               (.whereRule "linked" (into-array Object [mid-var to-var]))
                               (.end))
              ^DatalogQuery rule-query (doto (Datalevin/query)
                                         (.findAll "?name")
                                         (.rules rules)
                                         (.whereRule "linked"
                                                     (into-array Object
                                                                 [[(Datalevin/kw "name")
                                                                   "Charlie"]
                                                                  e-var]))
                                         (.whereDatom e-var "name" name-var))
              ^DatalogQuery or-query (doto (Datalevin/query)
                                       (.findAll "?name")
                                       (.whereDatom e-var "name" name-var)
                                       (.whereDatom e-var "age" age-var)
                                       (.whereOr (into-array QueryClause
                                                             [(QueryClause/predicate ">=" (into-array Object [age-var 30]))
                                                              (QueryClause/predicate "=" (into-array Object [name-var "Bob"]))])))
              ^DatalogQuery vip-query (doto (Datalevin/query)
                                        (.findAll "?name")
                                        (.whereDatom e-var "tag" ":vip")
                                        (.whereDatom e-var "name" name-var))
              ^Map no-schema nil
              empty-inputs (java.util.ArrayList.)
              report (.transact c tx)
              schema2 (.updateSchema c no-schema
                                     (Datalevin/setOf (into-array Object
                                                                  ["nickname"]))
                                     (Datalevin/mapOf (into-array Object
                                                                  ["city"
                                                                   "home-city"])))
              ^PullSelector selector (doto (Datalevin/pull)
                                           (.attr "name")
                                           (.attr "home-city")
                                           (.nested (doto (PullSelector/attribute "friend")
                                                      (.limit 1))
                                                (doto (Datalevin/pull)
                                                  (.attr "name"))))
              names  (set (.query c query empty-inputs))
              linked (set (.query c rule-query empty-inputs))
              selected (set (.query c or-query empty-inputs))
              vip-names (set (.query c vip-query empty-inputs))
              alice  (.pull c selector [":name" "Alice"])
              bob    (.pull c selector [":name" "Bob"])
              _      (.openDbi k "items")
              txs    [[":put" "a" "alpha"]
                      [":put" "b" "beta"]
                      [":put" "c" "gamma"]]
              tx-r   (.transact k "items" txs ":string" ":string")
              value  (.getValue k "items" "b" ":string" ":string" true)
              by-rank (.getByRank k "items" (long 2) ":string" ":string" false)
              range  (.getRange k "items" [":all"]
                                ":string" ":string"
                                (Integer/valueOf 2)
                                (Integer/valueOf 1))]
          (is (instance? Map report))
          (is (= ":db.type/string" (get-in schema* [":city" ":db/valueType"])))
          (is (= ":db.type/string" (get-in schema* [":tag" ":db/valueType"])))
          (is (contains? (set (keys schema*)) ":nickname"))
          (is (contains? (set (keys schema2)) ":home-city"))
          (is (not (contains? (set (keys schema2)) ":city")))
          (is (not (contains? (set (keys schema2)) ":nickname")))
          (is (= "[:find [?name ...] :where [?e :name ?name]]"
                 (.toEdn query)))
          (is (= "[:find ?limit . :where [(ground 30) ?limit]]"
                 (.toEdn scalar)))
          (is (= "[:find [?name ...] :where [?e :tag \":vip\"] [?e :name ?name]]"
                 (.toEdn vip-query)))
          (is (= [":db/add" -1 ":age" 30]
                 (vec (Tx/add -1 "age" 30))))
          (is (= #{"Alice" "Bob" "Charlie"} names))
          (is (= #{"Alice" "Bob"} linked))
          (is (= #{"Alice" "Bob" "Charlie"} selected))
          (is (= #{"Alice"} vip-names))
          (is (= "Alice" (get alice ":name")))
          (is (= "Paris" (get alice ":home-city")))
          (is (= "Bob" (get bob ":name")))
          (is (= ["Alice"] (mapv #(get % ":name") (get bob ":friend"))))
          (is (= ":transacted" tx-r))
          (is (= "beta" value))
          (is (= ["c" "gamma"] (vec by-rank)))
          (is (= [["b" "beta"] ["c" "gamma"]]
                 (mapv vec range)))))
      (is (.closed conn))
      (is (.closed kv))
      (finally
        (u/delete-files conn-dir)
        (u/delete-files kv-dir)))))

(deftest java-friendly-client-api-test
  (tdc/server-fixture
    (fn []
      (let [suffix  (str (UUID/randomUUID))
            db-name (str "java-friendly-" suffix)
            client  (Datalevin/newClient "dtlv://datalevin:datalevin@localhost")]
        (try
          (with-open [^Client c client]
            (let [client-id (.clientId c)
                  _         (.createDatabase c db-name Datalevin/DB_DATALOG)
                  open-info (.openDatabaseInfo c db-name Datalevin/DB_DATALOG nil nil)
                  dbs       (set (.listDatabases c))]
              (is (instance? UUID client-id))
              (is (= false (.disconnected c)))
              (is (instance? Map open-info))
              (is (contains? dbs db-name))
              (.closeDatabase c db-name)
              (.dropDatabase c db-name)))
          (is (.disconnected client))
          (finally
            (when-not (.disconnected client)
              (.close client))))))))
