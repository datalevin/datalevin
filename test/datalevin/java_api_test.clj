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
   [clojure.edn :as edn]
   [clojure.string :as str]
   [clojure.test :refer [deftest is use-fixtures]]
   [datalevin.core :as d]
   [datalevin.json-api :as sut]
   [datalevin.json-api.shared :as shared]
   [datalevin.json-convert :as jc]
   [datalevin.test.core :as tdc]
   [datalevin.util :as u])
  (:import
   [datalevin Client Connection DatabaseType DatalogQuery Datalevin DatalevinException DatalevinInterop JSONApi KV KVType PermissionAction PermissionObject PullSelector QueryClause RangeSpec Rules Schema Schema$Cardinality Schema$Unique Schema$ValueType Tx TxData UdfFunction]
   [java.util List Map UUID]
   [java.util.concurrent CountDownLatch TimeUnit]
   [java.util.function BiConsumer BiFunction BiPredicate Consumer]))

(defn- clean-java-api-state
  [f]
  (#'sut/clear-handles!)
  (try
    (f)
    (finally
      (#'sut/clear-handles!))))

(use-fixtures :each clean-java-api-state)

(defn- handle-count []
  (.size ^java.util.Map
         (:handles (var-get #'shared/default-session-state))))

(defn- java-map
  [& key-values]
  (Datalevin/mapOf (into-array Object key-values)))

(defn- delete-files-retry
  [path]
  (loop [attempt 0]
    (let [result (try
                   (u/delete-files path)
                   ::deleted
                   (catch Exception e
                     e))]
        (if (= ::deleted result)
          nil
          (if (< attempt 40)
            (do
              (System/gc)
              (Thread/sleep 100)
              (recur (inc attempt)))
          nil)))))

(defn- test-dir
  [prefix]
  (u/tmp-dir (str prefix "-" (UUID/randomUUID))))

(defn- public-method-names
  [^Class class]
  (->> (.getDeclaredMethods class)
       (filter #(java.lang.reflect.Modifier/isPublic (.getModifiers ^java.lang.reflect.Method %)))
       (remove #(.isSynthetic ^java.lang.reflect.Method %))
       (map #(.getName ^java.lang.reflect.Method %))
       set))

(defn- mget
  [m k]
  (or (get m k)
      (when (string? k)
        (let [n (str/replace-first k #"^:" "")
              kw (keyword n)
              sym (symbol n)]
          (or (get m kw)
              (get m (str ":" n))
              (get m n)
              (get m sym))))
      (when (keyword? k)
        (let [n (name k)
              sym (symbol n)]
          (or (get m (str ":" n))
              (get m n)
              (get m sym))))
      (when (symbol? k)
        (let [n (name k)
              kw (keyword n)]
          (or (get m kw)
              (get m (str ":" n))
              (get m n))))))

(defn- mget-in
  [m ks]
  (reduce (fn [acc k]
            (when acc
              (mget acc k)))
          m
          ks))

(defn- pair-rows
  [rows]
  (mapv vec rows))

(defn- permission-matches?
  [perm act obj]
  (cond
    (map? perm) (and (= act (mget perm :permission/act))
                     (= obj (mget perm :permission/obj)))
    (sequential? perm) (and (= act (first perm))
                            (= obj (second perm)))
    :else false))

(def java-friendly-surface
  {Datalevin        #{"apiInfo" "exec" "createConn" "getConn" "openKV" "newClient"
                      "newLlamaEmbedder" "query" "tx" "rules" "pull" "schema" "createUdfRegistry"
                      "udfDescriptor" "registerUdf" "unregisterUdf"
                      "registeredUdf" "allRange"
                      "edn" "kw" "var" "mapOf" "orderedMap"
                      "listOf" "setOf" "mapResult" "listResult" "setResult"}
   Connection       #{"closed" "schema" "updateSchema" "opts" "clear"
                      "maxEid" "datalogIndexCacheLimit" "entid" "entity"
                      "entityMap" "pull" "pullMany" "query" "queryForm"
                      "queryScalar" "queryCollection" "queryTuple"
                      "queryRelation" "queryKeyed" "explain" "explainForm"
                      "transact" "exec"}
   KV               #{"closed" "dir" "openDbi" "openListDbi" "putListItems"
                      "delListItems" "deleteListItems" "getList" "visitList"
                      "listCount" "inList" "listRange" "listRangeCount"
                      "listRangeFilter" "listRangeKeep" "listRangeSome"
                      "listRangeFilterCount" "visitListRange" "listRangeFirst"
                      "listRangeFirstN" "keyRangeListCount" "clearDbi"
                      "dropDbi" "listDbis" "stat" "entries" "transact"
                      "getValue" "getRank" "getByRank" "getRange"
                      "getEntryByRank" "keyRange" "keyRangeCount"
                      "rangeCount" "sync" "exec"}
   Client           #{"disconnect" "disconnected" "clientId" "openDatabase"
                      "openDatabaseInfo" "closeDatabase" "createDatabase"
                      "dropDatabase" "listDatabases" "listDatabasesInUse"
                      "createUser" "dropUser" "resetPassword" "listUsers"
                      "createRole" "dropRole" "listRoles" "assignRole"
                      "withdrawRole" "listUserRoles" "grantPermission"
                      "revokePermission" "listRolePermissions"
                      "listUserPermissions" "querySystem" "querySystemForm"
                      "showClients" "disconnectClient" "exec"}
   DatalevinInterop #{"coreInvoke" "coreInvokeBridge"
                      "clientInvoke" "clientInvokeBridge"
                      "readEdn" "currentClassLoader" "bridgeResult"
                      "createConnection" "getConnection" "closeConnection"
                      "connectionClosed" "connectionDb" "openKeyValue"
                      "closeKeyValue" "keyValueClosed" "newClient"
                      "closeClient" "clientDisconnected" "keyword"
                      "symbol" "schema" "options" "udfDescriptor"
                      "createUdfRegistry" "registerUdf" "unregisterUdf"
                      "registeredUdf" "renameMap" "deleteAttrs"
                      "lookupRef" "txData" "kvTxs" "kvInput" "kvRange"
                      "kvType" "databaseType" "role"
                      "permissionKeyword" "permissionTarget"}})

(deftest java-friendly-local-api-test
  (let [conn-dir (test-dir "java-friendly-conn")
        kv-dir   (test-dir "java-friendly-kv")
        schema   (doto (Datalevin/schema)
                   (.attr "name" (doto (Schema/attribute)
                                   (.valueType Schema$ValueType/STRING)
                                   (.unique Schema$Unique/IDENTITY)))
                   (.attr "age" (doto (Schema/attribute)
                                  (.valueType Schema$ValueType/LONG)))
                   (.attr "friend" (doto (Schema/attribute)
                                     (.valueType Schema$ValueType/REF)
                                     (.cardinality Schema$Cardinality/MANY))))
        conn     (Datalevin/createConn conn-dir schema)
        kv       (Datalevin/openKV kv-dir)]
    (try
      (is (instance? Map (Datalevin/apiInfo)))
      (is (zero? (handle-count)))
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
                                                           (.valueType Schema$ValueType/STRING)
                                                           (.doc "City name")))
                                           (.attr "nickname" (doto (Schema/attribute)
                                                               (.valueType Schema$ValueType/STRING)))
                                           (.attr "tag" (doto (Schema/attribute)
                                                          (.valueType Schema$ValueType/STRING)))))
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
              ^DatalogQuery tuple-query (doto (Datalevin/query)
                                          (.findTuple (into-array String ["?name" "?age"]))
                                          (.whereDatom e-var "name" name-var)
                                          (.whereDatom e-var "age" age-var)
                                          (.wherePredicate "=" (into-array Object [name-var "Bob"])))
	              ^DatalogQuery relation-query (doto (Datalevin/query)
	                                             (.find (into-array String ["?name" "?age"]))
	                                             (.whereDatom e-var "name" name-var)
	                                             (.whereDatom e-var "age" age-var))
	              ^DatalogQuery keyed-query (doto (Datalevin/query)
	                                          (.find (into-array String ["?name" "?age"]))
	                                          (.keys (into-array String ["name" "age"]))
	                                          (.whereDatom e-var "name" name-var)
	                                          (.whereDatom e-var "age" age-var))
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
              keyword-lookup (Datalevin/listOf (into-array Object
                                                           [(Datalevin/kw "name")
                                                            "Alice"]))
              ^PullSelector selector (doto (Datalevin/pull)
                                           (.attr "name")
                                           (.attr "home-city")
                                           (.nested (doto (PullSelector/attribute "friend")
                                                      (.limit 1))
                                                (doto (Datalevin/pull)
                                                  (.attr "name"))))
              read-count (atom 0)
              read-string* edn/read-string
              {:keys [names linked linked-immutable selected vip-names alice bob]}
              (with-redefs [edn/read-string
                            (fn [& xs]
                              (swap! read-count inc)
                              (apply read-string* xs))]
                {:names     (set (.query c query empty-inputs))
                 :linked    (set (.query c rule-query empty-inputs))
                 :linked-immutable
                 (set (.query c rule-query (List/of)))
                 :selected  (set (.query c or-query empty-inputs))
                 :vip-names (set (.query c vip-query empty-inputs))
                 :alice     (.pull c selector [":name" "Alice"])
                 :bob       (.pull c selector [":name" "Bob"])})
	              scalar-value (.queryScalar c scalar Long empty-inputs)
	              coll-names   (.queryCollection c query String empty-inputs)
	              tuple-result (.queryTuple c tuple-query empty-inputs)
	              relation     (.queryRelation c relation-query empty-inputs)
	              keyed-result (.queryKeyed c keyed-query empty-inputs)
	              _      (.openDbi k "items")
              txs    [[":put" "a" "alpha"]
                      [":put" "b" "beta"]
                      [":put" "c" "gamma"]]
              tx-r   (.transact k "items" txs KVType/STRING KVType/STRING)
              value  (.getValue k "items" "b" KVType/STRING KVType/STRING true)
              entry   (.getEntryByRank k "items" (long 2) KVType/STRING KVType/STRING)
	              range  (.getRange k "items" (RangeSpec/all)
	                                KVType/STRING KVType/STRING
	                                (Integer/valueOf 2)
	                                (Integer/valueOf 1))
              tuple-key-type (KVType/tuple (into-array KVType [KVType/LONG KVType/STRING]))
              _      (.openDbi k "pairs")
              _      (.transact k "pairs"
                                [[":put" [-1 "alpha"] 7]
                                 [":put" [2 "beta"] 9]]
                                tuple-key-type
                                KVType/LONG)
              tuple-value (.getValue k "pairs" [-1 "alpha"] tuple-key-type KVType/LONG true)
              tuple-entry (.getEntryByRank k "pairs" (long 1) tuple-key-type KVType/LONG)
              _      (.openListDbi k "list")
              _      (.putListItems k "list" "a" [1 2 3 4] KVType/STRING KVType/LONG)
              _      (.putListItems k "list" "b" [5 6 7] KVType/STRING KVType/LONG)
              _      (.putListItems k "list" "c" [3 6 9] KVType/STRING KVType/LONG)
              visited-list (atom [])
              _      (.visitList
                      k "list"
                      (reify Consumer
                        (accept [_ value]
                          (swap! visited-list conj value)))
                      "b" KVType/STRING KVType/LONG)
              list-b  (.getList k "list" "b" KVType/STRING KVType/LONG
                                (Integer/valueOf 1) (Integer/valueOf 1))
              count-a (.listCount k "list" "a" KVType/STRING)
              in-list? (.inList k "list" "b" 6 KVType/STRING KVType/LONG)
	              list-range* (.listRange k "list"
	                                      (RangeSpec/closed "a" "c") KVType/STRING
	                                      (RangeSpec/closed 2 4) KVType/LONG
	                                      (Integer/valueOf 2) (Integer/valueOf 1))
	              list-range-cnt (.listRangeCount k "list" (RangeSpec/closed "a" "c") KVType/STRING)
                      paged-filter-calls (atom 0)
		              list-filter (.listRangeFilter
		                           k "list"
		                           (reify BiPredicate
		                             (test [_ key value]
		                               (and (= "c" key) (>= (long value) 6))))
		                           (RangeSpec/all) KVType/STRING
		                           (RangeSpec/all) KVType/LONG
		                           nil nil)
                      paged-filter (.listRangeFilter
                                    k "list"
                                    (reify BiPredicate
                                      (test [_ key value]
                                        (swap! paged-filter-calls inc)
                                        (>= (long value) 3)))
                                    (RangeSpec/all) KVType/STRING
                                    (RangeSpec/all) KVType/LONG
                                    (Integer/valueOf 2)
                                    (Integer/valueOf 2))
                      paged-keep-calls (atom 0)
	              list-keep (.listRangeKeep
	                         k "list"
	                         (reify BiFunction
	                           (apply [_ key value]
	                             (when (>= (long value) 6)
	                               (str key ":" value))))
		                         (RangeSpec/all) KVType/STRING
		                         (RangeSpec/all) KVType/LONG
		                         nil nil)
                      paged-keep (.listRangeKeep
                                  k "list"
                                  (reify BiFunction
                                    (apply [_ key value]
                                      (swap! paged-keep-calls inc)
                                      (when (>= (long value) 3)
                                        (str key ":" value))))
                                  (RangeSpec/all) KVType/STRING
                                  (RangeSpec/all) KVType/LONG
                                  (Integer/valueOf 2)
                                  (Integer/valueOf 1))
	              list-some (.listRangeSome
	                         k "list"
	                         (reify BiFunction
                           (apply [_ key value]
                             (when (= 7 (long value))
                               (str key ":" value))))
	                         (RangeSpec/all) KVType/STRING
	                         (RangeSpec/all) KVType/LONG)
              list-filter-count (.listRangeFilterCount
                                 k "list"
                                 (reify BiPredicate
                                   (test [_ key value]
                                     (and (= "a" key) (>= (long value) 3))))
	                                 (RangeSpec/all) KVType/STRING
	                                 (RangeSpec/all) KVType/LONG)
              visited  (atom [])
              _      (.visitListRange
                      k "list"
                      (reify BiConsumer
                        (accept [_ key value]
                          (swap! visited conj [key value])))
	                      (RangeSpec/closed "a" "b") KVType/STRING
	                      (RangeSpec/closed 2 6) KVType/LONG)
	              list-first (.listRangeFirst k "list"
	                                          (RangeSpec/closed "a" "c") KVType/STRING
	                                          (RangeSpec/closed 2 4) KVType/LONG)
	              list-first-n (.listRangeFirstN k "list" (long 2)
	                                             (RangeSpec/closed "a" "c") KVType/STRING
	                                             (RangeSpec/closed 2 4) KVType/LONG)
	              key-list-cnt (.keyRangeListCount k "list" (RangeSpec/all) KVType/STRING)
              _      (.deleteListItems k "list" "b" [5 7] KVType/STRING KVType/LONG)
              list-b2 (.getList k "list" "b" KVType/STRING KVType/LONG)
              _      (.deleteListItems k "list" "a" KVType/STRING)
              list-a  (.getList k "list" "a" KVType/STRING KVType/LONG)
              tuple-list-type (KVType/tuple (into-array KVType [KVType/LONG KVType/STRING]))
              _      (.openListDbi k "tuple-list")
              _      (.putListItems k "tuple-list" "t"
                                    [[1 "one"] [2 "two"]]
                                    KVType/STRING tuple-list-type)
              tuple-list (.getList k "tuple-list" "t" KVType/STRING tuple-list-type)]
          (is (instance? Map report))
          (is (seq (mget report :tx-data)))
          (is (pos? (long (mget (first (mget report :tx-data)) :tx))))
          (is (= :db.type/string (mget-in schema* [:city :db/valueType])))
          (is (= :db.type/string (mget-in schema* [:tag :db/valueType])))
          (is (contains? (set (keys schema*)) :nickname))
          (is (contains? (set (keys schema2)) :home-city))
          (is (not (contains? (set (keys schema2)) :city)))
          (is (not (contains? (set (keys schema2)) :nickname)))
	          (is (= "[:find [?name ...] :where [?e :name ?name]]"
	                 (.toEdn query)))
	          (is (= "[:find ?limit . :where [(ground 30) ?limit]]"
	                 (.toEdn scalar)))
	          (is (= "[:find ?name ?age :keys name age :where [?e :name ?name] [?e :age ?age]]"
	                 (.toEdn keyed-query)))
	          (is (= "[:find [?name ...] :where [?e :tag \":vip\"] [?e :name ?name]]"
	                 (.toEdn vip-query)))
          (is (zero? @read-count))
          (is (= 30 scalar-value))
          (is (= #{"Alice" "Bob" "Charlie"} (set coll-names)))
          (is (= ["Bob" 25] (vec tuple-result)))
	          (is (= 3 (count relation)))
	          (is (= #{{:name "Alice" :age 30}
	                   {:name "Bob" :age 25}
	                   {:name "Charlie" :age 40}}
	                 (into #{} keyed-result)))
	          (is (every? #(contains? (set (keys %)) :name) keyed-result))
	          (is (= #{["Alice" 30] ["Bob" 25] ["Charlie" 40]}
	                 (into #{} (map vec) relation)))
          (is (= [:db/add -1 :age 30]
                 (vec (Tx/add -1 "age" 30))))
          (is (= #{"Alice" "Bob" "Charlie"} names))
          (is (= #{"Alice" "Bob"} linked))
          (is (= #{"Alice" "Bob"} linked-immutable))
          (is (= #{"Alice" "Bob" "Charlie"} selected))
          (is (= #{"Alice"} vip-names))
          (is (= "Alice" (mget alice :name)))
          (is (= "Paris" (mget alice :home-city)))
          (is (= "Bob" (mget bob :name)))
          (is (= ["Alice"] (mapv #(mget % :name) (mget bob :friend))))
          (is (= :transacted tx-r))
          (is (= "beta" value))
          (is (= ["c" "gamma"] (vec entry)))
          (is (= 7 tuple-value))
          (is (= [[2 "beta"] 9] [(first tuple-entry) (second tuple-entry)]))
          (is (= [5 6 7]
                 @visited-list))
          (is (= [6] list-b))
          (is (= 4 count-a))
          (is (= true in-list?))
          (is (= [["a" 3] ["a" 4]] (pair-rows list-range*)))
	          (is (= 10 list-range-cnt))
	          (is (= [["c" 6] ["c" 9]] (pair-rows list-filter)))
              (is (= [["b" 5] ["b" 6]] (pair-rows paged-filter)))
              (is (= 6 @paged-filter-calls))
	          (is (= ["b:6" "b:7" "c:6" "c:9"] list-keep))
              (is (= ["a:4" "b:5"] paged-keep))
              (is (= 5 @paged-keep-calls))
	          (is (= "b:7" list-some))
          (is (= 2 list-filter-count))
          (is (= [["a" 2] ["a" 3] ["a" 4] ["b" 5] ["b" 6]]
                 @visited))
          (is (= ["a" 2] (vec list-first)))
          (is (= [["a" 2] ["a" 3]] (pair-rows list-first-n)))
          (is (= 10 key-list-cnt))
          (is (= [6] list-b2))
          (is (= [] list-a))
          (is (= [[1 "one"] [2 "two"]] tuple-list))
          (is (= [["b" "beta"] ["c" "gamma"]] (pair-rows range)))))
      (is (zero? (handle-count)))
      (is (.closed conn))
      (is (.closed kv))
      (finally
        (delete-files-retry conn-dir)
        (delete-files-retry kv-dir)))))

(deftest java-friendly-local-wrapper-coverage-test
  (let [conn-dir (test-dir "java-friendly-wrapper-conn")
        kv-dir   (test-dir "java-friendly-wrapper-kv")
        schema   (doto (Datalevin/schema)
                   (.attr "name" (doto (Schema/attribute)
                                   (.valueType Schema$ValueType/STRING)
                                   (.unique Schema$Unique/IDENTITY)))
                   (.attr "age" (doto (Schema/attribute)
                                  (.valueType Schema$ValueType/LONG))))]
    (try
      (with-open [^Connection c (Datalevin/createConn conn-dir schema)
                  ^KV k (Datalevin/openKV kv-dir)]
        (let [selector       (doto (Datalevin/pull)
                               (.attr "name")
                               (.attr "age"))
              empty-inputs   (java.util.ArrayList.)
              initial-limit  (.datalogIndexCacheLimit c)
              report         (.transact c
                                        (doto (Datalevin/tx)
                                          (.entity (doto (Tx/entity -1)
                                                     (.put "name" "Alice")
                                                     (.put "age" 30))))
                                        (java-map "source" "wrapper"))
              keyword-lookup (Datalevin/listOf (into-array Object
                                                           [(Datalevin/kw "name")
                                                            "Alice"]))
              eid            (.entid c [":name" "Alice"])
              entity         (.entity c [":name" "Alice"])
              pulled         (.pull c selector [":name" "Alice"])
              pulled-many    (.pullMany c selector (List/of [":name" "Alice"]))
              raw-query      (.query c
                                     "[:find [?name ...] :where [?e :name ?name]]"
                                     empty-inputs)
              explain-plan   (.explain c
                                       "[:find [?name ...] :where [?e :name ?name]]"
                                       empty-inputs)
              exec-max-eid   (.exec c "max-eid" nil)
              _              (.openDbi k "items")
              _              (.openDbi k "temp")
              _              (.transact k "items"
                                        [[":put" "a" "alpha"]
                                         [":put" "b" "beta"]
                                         [":put" "c" "gamma"]]
                                        ":string"
                                        ":string")
              _              (.transact k "temp"
                                        [[":put" "z" "zeta"]]
                                        ":string"
                                        ":string")
              dbis           (set (.listDbis k))
              stat           (.stat k "items")
              rank           (.getRank k "items" "b" ":string")
              by-rank        (.getByRank k "items" (long 1) ":string" ":string" false)
              ranked-value   (.getByRank k "items" (long 1) ":string" ":string" true)
              key-range      (.keyRange k "items"
                                        (RangeSpec/closedOpen "a" "c")
                                        KVType/STRING
                                        nil
                                        nil)]
          (is (= :db.type/string (mget-in (.schema c) [:name :db/valueType])))
          (is (= initial-limit (mget (.opts c) :cache-limit)))
          (is (= 1024 (.datalogIndexCacheLimit c 1024)))
          (is (= 1024 (.datalogIndexCacheLimit c)))
          (is (instance? Map report))
          (is (= "wrapper" (get (mget report :tx-meta) "source")))
          (is (= 1 (.maxEid c)))
          (is (= 1 eid))
          (is (= 1 (.entid c keyword-lookup)))
          (is (= 1 exec-max-eid))
          (is (= "Alice" (mget entity :name)))
          (is (= 30 (mget entity :age)))
          (is (= "Alice" (mget pulled :name)))
          (is (= "Alice" (mget (.pull c selector keyword-lookup) :name)))
          (is (= [{:name "Alice" :age 30}]
                 (vec pulled-many)))
          (is (= [{:name "Alice" :age 30}]
                 (vec (.pullMany c selector
                                 (Datalevin/listOf (into-array Object [keyword-lookup]))))))
          (is (= ["Alice"] (vec raw-query)))
          (is (instance? Map explain-plan))
          (is (contains? (set (keys explain-plan)) :plan))
          (let [_ (.query c
                          "[:find [?name ...] :where [?e :name ?name]]"
                          empty-inputs)
                _ (.transact c
                             (doto (Datalevin/tx)
                               (.entity (doto (Tx/entity -2)
                                          (.put "name" "Bob")
                                          (.put "age" 25)))))
                refreshed (set (.query c
                                       "[:find [?name ...] :where [?e :name ?name]]"
                                       empty-inputs))]
            (is (= #{"Alice" "Bob"} refreshed)))

          (is (= kv-dir (.dir k)))
          (is (contains? dbis "items"))
          (is (contains? dbis "temp"))
          (is (= 3 (.entries k "items")))
          (is (= 1 (get stat :depth)))
          (is (= 3 (get stat :entries)))
          (is (= 1 rank))
          (is (= ["b" "beta"] (vec by-rank)))
          (is (= "beta" ranked-value))
          (is (= ["a" "b"] (vec key-range)))
	          (let [range (RangeSpec/open "a" "d")]
	            (is (identical? (.build range) (.build range)))
	            (let [backward (.backward range)]
	              (is (= (Datalevin/kw ":open-back")
	                     (first (.build backward))))
	              (is (= [:open-back "a" "d"]
	                     (vec (.build backward))))))
	          (is (= "[:closed \"#hashtag\" \"(parenthetical)\"]"
	                 (str (RangeSpec/closed "#hashtag" "(parenthetical)"))))

	          (.sync k)
          (.clearDbi k "temp")
          (is (zero? (.entries k "temp")))
          (.dropDbi k "temp")
          (is (not (contains? (set (.listDbis k)) "temp"))))

        (with-open [^Connection reopened (Datalevin/getConn conn-dir)]
          (is (= #{"Alice" "Bob"}
                 (set (.query reopened
                              "[:find [?name ...] :where [?e :name ?name]]"
                              (java.util.ArrayList.)))))
          (.clear reopened))
        (with-open [^Connection cleared (Datalevin/getConn conn-dir)]
          (is (= []
                 (vec (.query cleared
                              "[:find [?name ...] :where [?e :name ?name]]"
                              (java.util.ArrayList.)))))))
      (is (zero? (handle-count)))
      (finally
        (delete-files-retry conn-dir)
        (delete-files-retry kv-dir)))))

(deftest java-connection-shared-wrapper-refresh-test
  (let [conn-dir (test-dir "java-shared-wrapper-refresh")
        schema   (doto (Datalevin/schema)
                   (.attr "name" (doto (Schema/attribute)
                                   (.valueType Schema$ValueType/STRING))))
        query    "[:find [?name ...] :where [?e :name ?name]]"
        inputs   (java.util.ArrayList.)]
    (try
      (with-open [^Connection reader (Datalevin/getConn conn-dir schema)
                  ^Connection writer (Datalevin/getConn conn-dir)]
        (.transact writer
                   (doto (Datalevin/tx)
                     (.entity (doto (Tx/entity -1)
                                (.put "name" "Alice")))))
        (is (= ["Alice"]
               (vec (.query reader query inputs))))

        (.transact writer
                   (doto (Datalevin/tx)
                     (.entity (doto (Tx/entity -2)
                                (.put "name" "Bob")))))
        (is (= #{"Alice" "Bob"}
               (set (.query reader query inputs)))))
      (finally
        (delete-files-retry conn-dir)))))

(deftest java-handle-close-race-regression-test
  (let [conn-dir      (test-dir "java-handle-close-race")
        ^Connection c (Datalevin/createConn conn-dir)
        first-entered (CountDownLatch. 1)
        double-close  (CountDownLatch. 2)
        release-close (CountDownLatch. 1)
        calls         (atom 0)
        real-close    d/close]
    (try
      (with-redefs [d/close (fn [resource]
                              (swap! calls inc)
                              (.countDown first-entered)
                              (.countDown double-close)
                              (.await release-close 1 TimeUnit/SECONDS)
                              (real-close resource))]
        (let [close-a (future (.close c))
              _       (is (.await first-entered 1 TimeUnit/SECONDS))
              close-b (future (.close c))]
          (is (false? (.await double-close 1 TimeUnit/SECONDS)))
          (.countDown release-close)
          @close-a
          @close-b))
      (is (= 1 @calls))
      (is (.closed c))
      (finally
        (delete-files-retry conn-dir)))))

(deftest java-handle-exec-json-concurrency-regression-test
  (let [conn-dir        (test-dir "java-handle-exec-json-concurrency")
        ^Connection c   (Datalevin/createConn conn-dir)
        first-entered   (CountDownLatch. 1)
        second-entered  (CountDownLatch. 1)
        release-second  (CountDownLatch. 1)
        seen            (atom 0)
        real-exec       sut/exec-request]
    (try
      (with-redefs [sut/exec-request
                    (fn
                      ([request]
                       (real-exec request))
                      ([session request]
                       (let [n (swap! seen inc)]
                         (case n
                           1 (do
                               (.countDown first-entered)
                               (real-exec session request))
                           2 (do
                               (.countDown second-entered)
                               (.await release-second 1 TimeUnit/SECONDS)
                               (real-exec session request))
                           (real-exec session request)))))]
        (let [first  (future (.exec c "max-eid" nil))
              _      (is (.await first-entered 1 TimeUnit/SECONDS))
              second (future (.exec c "max-eid" nil))
              _      (is (.await second-entered 1 TimeUnit/SECONDS))]
          (is (= 0 @first))
          (is (= ::timeout (deref second 100 ::timeout)))
          (is (zero? (handle-count)))
          (.countDown release-second)
          (is (= 0 @second))
          (is (zero? (handle-count)))))
      (finally
        (.close c)
        (delete-files-retry conn-dir)))))

(deftest java-handle-exec-json-close-coordination-test
  (let [conn-dir     (test-dir "java-handle-exec-json-close")
        ^Connection c (Datalevin/createConn conn-dir)
        exec-entered (CountDownLatch. 1)
        release-exec (CountDownLatch. 1)
        real-exec    sut/exec-request]
    (try
      (with-redefs [sut/exec-request
                    (fn
                      ([request]
                       (real-exec request))
                      ([session request]
                       (.countDown exec-entered)
                       (.await release-exec 1 TimeUnit/SECONDS)
                       (real-exec session request)))]
        (let [exec-f  (future (.exec c "max-eid" nil))
              _       (is (.await exec-entered 1 TimeUnit/SECONDS))
              close-f (future (.close c))]
          (is (= ::timeout (deref close-f 100 ::timeout)))
          (is (zero? (handle-count)))
          (.countDown release-exec)
          (is (= 0 @exec-f))
          @close-f
          (is (.closed c))
          (is (zero? (handle-count)))))
      (finally
        (.close c)
        (delete-files-retry conn-dir)))))

(deftest java-json-api-direct-bridge-test
  (let [response (jc/read-json-string (JSONApi/exec "{\"op\":\"api-info\",\"args\":{}}"))]
    (is (= true (get response "ok")))
    (is (= (Datalevin/apiInfo) (get response "result")))))

(deftest java-datalevin-exception-preserves-cause-test
  (let [conn-dir (test-dir "java-exception-cause")
        conn     (Datalevin/createConn conn-dir)]
    (try
      (with-open [^Connection c conn]
        (let [error (try
                      (.pull c "[42]" 1)
                      nil
                      (catch DatalevinException e
                        e))]
          (is (instance? DatalevinException error))
          (is (instance? clojure.lang.ExceptionInfo (.getCause ^DatalevinException error)))
          (is (= (.getMessage ^DatalevinException error)
                 (.getMessage ^clojure.lang.ExceptionInfo (.getCause ^DatalevinException error))))
          (is (= ":parser/pull" (.getErrorType ^DatalevinException error)))
          (is (= (.getData ^DatalevinException error)
                 (.getData ^clojure.lang.ExceptionInfo (.getCause ^DatalevinException error))))))
      (finally
        (delete-files-retry conn-dir)))))

(deftest java-value-type-equality-test
  (letfn [(same-value [a b]
            (is (= a b))
            (is (= b a))
            (is (= (hash a) (hash b)))
            (is (= ::present (get {a ::present} b)))
            (is (= 1 (count (set [a b])))))
          (adult-rules []
            (-> (Datalevin/rules)
                (.rule "adult" (into-array String ["?e"]))
                (.whereDatom (Datalevin/var "e") "age" (Datalevin/var "age"))
                (.wherePredicate ">=" (into-array Object [(Datalevin/var "age") 18]))
                (.end)))
          (adult-query [rules]
            (doto (Datalevin/query)
              (.findAll "?name")
              (.rules rules)
              (.whereRule "adult" (into-array Object [(Datalevin/var "e")]))
              (.whereDatom (Datalevin/var "e") "name" (Datalevin/var "name"))))
          (friend-attr []
            (doto (PullSelector/attribute "friend")
              (.limit 1)
              (.as "best-friend")))
          (friend-selector []
            (doto (Datalevin/pull)
              (.attr "name")
              (.attr (friend-attr))
              (.nested "friend" (doto (Datalevin/pull)
                                  (.attr "name")))))
          (person-schema []
            (doto (Datalevin/schema)
              (.attr "name" (doto (Schema/attribute)
                              (.valueType Schema$ValueType/STRING)))
              (.attr "age" (doto (Schema/attribute)
                             (.valueType Schema$ValueType/LONG)))))
          (person-entity []
            (doto (Tx/entity 1)
              (.put "name" "Alice")
              (.put "age" 30)))
          (person-tx []
            (doto (Datalevin/tx)
              (.entity (person-entity))
              (.add 1 "tag" "vip")))]
    (let [edn-a      (Datalevin/edn "[:name \"Alice\"]")
          edn-b      (Datalevin/edn "[:name \"Alice\"]")
          kv-a       (KVType/tuple (into-array KVType [KVType/LONG KVType/STRING]))
          kv-b       (KVType/tuple (into-array KVType [KVType/LONG KVType/STRING]))
          range-a    (RangeSpec/closed "a" "z")
          range-b    (RangeSpec/closed "a" "z")
          clause-a   (QueryClause/predicate "=" (into-array Object [(Datalevin/var "x") 1]))
          clause-b   (QueryClause/predicate "=" (into-array Object [(Datalevin/var "x") 1]))
          rules-a    (adult-rules)
          rules-b    (adult-rules)
          query-a    (adult-query rules-a)
          query-b    (adult-query rules-b)
          attr-a     (friend-attr)
          attr-b     (friend-attr)
          selector-a (friend-selector)
          selector-b (friend-selector)
          schema-a   (person-schema)
          schema-b   (person-schema)
          entity-a   (person-entity)
          entity-b   (person-entity)
          tx-a       (person-tx)
          tx-b       (person-tx)]
      (same-value edn-a edn-b)
      (same-value kv-a kv-b)
      (same-value range-a range-b)
      (same-value clause-a clause-b)
      (same-value rules-a rules-b)
      (same-value query-a query-b)
      (same-value attr-a attr-b)
      (same-value selector-a selector-b)
      (same-value schema-a schema-b)
      (same-value entity-a entity-b)
      (same-value tx-a tx-b)
      (is (not= range-a (RangeSpec/open "a" "z")))
      (is (not= query-a
                (doto (Datalevin/query)
                  (.findAll "?name")
                  (.whereDatom (Datalevin/var "e") "name" (Datalevin/var "name"))))))))

(deftest java-friendly-helper-overload-coverage-test
  (let [plain-dir      (test-dir "java-helper-plain-conn")
        map-dir        (test-dir "java-helper-map-conn")
        typed-dir      (test-dir "java-helper-typed-conn")
        typed-opts-dir (test-dir "java-helper-typed-opts-conn")
        kv-dir         (test-dir "java-helper-kv")
        schema-map     (java-map "name"     (java-map ":db/valueType" ":db.type/string"
                                                      ":db/unique" ":db.unique/identity")
                                 "age"      (java-map ":db/valueType" ":db.type/long")
                                 "nickname" (java-map ":db/valueType" ":db.type/string"))
        typed-schema   (doto (Datalevin/schema)
                         (.attr "title" (doto (Schema/attribute)
                                          (.valueType Schema$ValueType/STRING)))
                         (.attr "score" (doto (Schema/attribute)
                                          (.valueType Schema$ValueType/LONG))))
        opts           (java-map ":cache-limit" 256)]
    (try
      (let [ordered         (Datalevin/orderedMap
                             (into-array Object ["a" 1 (Datalevin/kw "b") 2]))
            listed          (Datalevin/listOf (into-array Object [1 2 3]))
            setted          (Datalevin/setOf (into-array Object ["x" "x" "y"]))
            literal-query   (doto (Datalevin/query)
                              (.findScalar "?tag")
                              (.whereBind "ground"
                                          (Datalevin/var "tag")
                                          (into-array Object [(Datalevin/edn ":vip")])))
            symbol-keyed-q  (doto (Datalevin/query)
                              (.find (into-array String ["?name" "?age"]))
                              (.syms (into-array String ["name" "age"]))
                              (.whereClause "[?e :name ?name]")
                              (.whereClause "[?e :age ?age]")
                              (.with (into-array String ["?e"])))]
        (is (= (Datalevin/apiInfo) (Datalevin/mapResult (Datalevin/exec "api-info" nil))))
        (is (= ":name" (str (Datalevin/kw "name"))))
        (is (= ":person/name" (str (Datalevin/kw "person/name"))))
        (is (= "?age" (str (Datalevin/var "age"))))
        (is (= "[:find ?tag . :where [(ground :vip) ?tag]]"
               (.toEdn literal-query)))
        (is (= "[:find ?name ?age :syms name age :with ?e :where [?e :name ?name] [?e :age ?age]]"
               (.toEdn symbol-keyed-q)))
        (is (= [:all] (vec (.build (Datalevin/allRange)))))
        (let [^java.util.Map$Entry entry (first (seq (Datalevin/mapResult ordered)))]
          (is (= ["a" 1] [(.getKey entry) (.getValue entry)])))
        (is (= [1 2 3] (vec (Datalevin/listResult listed))))
        (is (= #{"x" "y"} (set (Datalevin/setResult setted)))))

      (with-open [^Connection anon (Datalevin/createConn)
                  ^Connection shared-anon (Datalevin/getConn)]
        (is (instance? Connection anon))
        (is (instance? Connection shared-anon))
        (is (false? (.closed anon)))
        (is (false? (.closed shared-anon))))

      (with-open [^Connection plain (Datalevin/createConn plain-dir)]
        (is (instance? Connection plain))
        (is (contains? (set (keys (.schema plain))) :db/ident)))

      (with-open [^Connection plain-shared (Datalevin/getConn plain-dir)]
        (is (instance? Connection plain-shared))
        (is (contains? (set (keys (.schema plain-shared))) :db/created-at)))

      (with-open [^Connection typed (Datalevin/createConn typed-dir typed-schema)]
        (is (= :db.type/string (mget-in (.schema typed) [:title :db/valueType]))))

      (with-open [^Connection typed-shared (Datalevin/getConn typed-dir typed-schema)]
        (is (= :db.type/long (mget-in (.schema typed-shared) [:score :db/valueType]))))

      (with-open [^Connection typed-opts (Datalevin/createConn typed-opts-dir typed-schema opts)]
        (is (= 256 (mget (.opts typed-opts) :cache-limit))))

      (with-open [^Connection typed-opts-shared (Datalevin/getConn typed-opts-dir typed-schema opts)]
        (is (= 256 (mget (.opts typed-opts-shared) :cache-limit))))

      (with-open [^Connection c (Datalevin/createConn map-dir schema-map opts)]
        (let [e-var           (Datalevin/var "e")
              name-var        (Datalevin/var "name")
              age-var         (Datalevin/var "age")
              min-var         (Datalevin/var "min")
              out-var         (Datalevin/var "out")
              all-query       (doto (Datalevin/query)
                                (.findAll "?name")
                                (.whereDatom e-var "name" name-var))
              min-query       (doto (Datalevin/query)
                                (.findAll "?name")
                                (.in (into-array String ["?min"]))
                                (.whereDatom e-var "name" name-var)
                                (.whereDatom e-var "age" age-var)
                                (.wherePredicate ">="
                                                 (into-array Object [age-var min-var])))
              tuple-query     (doto (Datalevin/query)
                                (.findTuple (into-array String ["?name" "?age"]))
                                (.whereDatom e-var "name" name-var)
                                (.whereDatom e-var "age" age-var)
                                (.wherePredicate "="
                                                 (into-array Object [name-var "Ada"])))
              relation-query  (doto (Datalevin/query)
                                (.find (into-array String ["?name" "?age"]))
                                (.in (into-array String ["?min"]))
                                (.whereDatom e-var "name" name-var)
                                (.whereDatom e-var "age" age-var)
                                (.wherePredicate ">="
                                                 (into-array Object [age-var min-var])))
              keyed-query     (doto (Datalevin/query)
                                (.find (into-array String ["?name" "?age"]))
                                (.strs (into-array String ["name" "age"]))
                                (.in (into-array String ["?min"]))
                                (.whereDatom e-var "name" name-var)
                                (.whereDatom e-var "age" age-var)
                                (.wherePredicate ">="
                                                 (into-array Object [age-var min-var])))
              scalar-ground   (doto (Datalevin/query)
                                (.findScalar "?v")
                                (.whereBind "ground" (Datalevin/var "v")
                                            (into-array Object [42])))
              scalar-input    (doto (Datalevin/query)
                                (.findScalar "?out")
                                (.in (into-array String ["?min"]))
                                (.whereBind "identity" out-var
                                            (into-array Object [min-var])))
              _               (.updateSchema c (java-map "city" (java-map ":db/valueType"
                                                                          ":db.type/string")))
              _               (.updateSchema c (doto (Datalevin/schema)
                                                 (.attr "tag" (doto (Schema/attribute)
                                                                (.valueType Schema$ValueType/STRING)))))
              updated         (.updateSchema c (cast java.util.Map nil)
                                             (Datalevin/setOf (into-array Object ["nickname"]))
                                             (Datalevin/mapOf (into-array Object ["city"
                                                                                  "home-city"])))
              report-raw      (.transact c
                                         (Datalevin/listOf
                                          (into-array Object
                                                      [(java-map ":db/id" -1
                                                                 "name" "Ada"
                                                                 "age" 10
                                                                 "home-city" "Rome")
                                                       (java-map ":db/id" -2
                                                                 "name" "Bea"
                                                                 "age" 20)])))
              report-typed    (.transact c
                                         (doto (Datalevin/tx)
                                           (.entity (doto (Tx/entity -3)
                                                      (.put "name" "Cid")
                                                      (.put "age" 30)
                                                      (.put "tag" "vip"))))
                                         (java-map "source" "helper-overloads"))
              query-form      (DatalevinInterop/readEdn
                               "[:find ?name . :in $ ?target :where [?e :name ?target] [?e :name ?name]]")
              raw-result      (.query c
                                      "[:find ?name . :in $ ?target :where [?e :name ?target] [?e :name ?name]]"
                                      (into-array Object ["Ada"]))
              form-result     (.queryForm c query-form
                                          (java.util.ArrayList. ["Ada"]))
              typed-result    (set (.query c all-query))
              typed-varargs   (set (.query c min-query (into-array Object [20])))
              scalar-no-input (.queryScalar c scalar-ground Long)
              scalar-varargs  (.queryScalar c scalar-input Long
                                            (into-array Object [20]))
              coll-no-input   (set (.queryCollection c all-query String))
              coll-varargs    (set (.queryCollection c min-query String
                                                     (into-array Object [20])))
              tuple-no-input  (.queryTuple c tuple-query)
              relation-input  (.queryRelation c relation-query
                                              (into-array Object [20]))
              keyed-input     (.queryKeyed c keyed-query
                                           (into-array Object [20]))
              entity-map      (.entityMap c ["name" "Ada"])
              explain-raw     (.explain c
                                        "[:find ?name . :in $ ?target :where [?e :name ?target] [?e :name ?name]]"
                                        (into-array Object ["Ada"]))
              explain-form    (.explainForm c query-form
                                            (java.util.ArrayList. ["Ada"]))
              explain-typed   (.explain c all-query)
              explain-input   (.explain c relation-query
                                        (into-array Object [20]))
              explain-opts    (.explain c "{:analyze true}" all-query
                                        (java.util.ArrayList.))
              explain-form-opts (.explainForm c "{:analyze true}" query-form
                                              (java.util.ArrayList. ["Ada"]))
              exec-schema     (.exec c "schema" nil)]
          (is (instance? Map report-raw))
          (is (instance? Map report-typed))
          (is (= "helper-overloads" (get (mget report-typed :tx-meta) "source")))
          (is (contains? (set (keys updated)) :home-city))
          (is (not (contains? (set (keys updated)) :nickname)))
          (is (= "Ada" raw-result))
          (is (= "Ada" form-result))
          (is (= #{"Ada" "Bea" "Cid"} typed-result))
          (is (= #{"Bea" "Cid"} typed-varargs))
          (is (= 42 scalar-no-input))
          (is (= 20 scalar-varargs))
          (is (= #{"Ada" "Bea" "Cid"} coll-no-input))
          (is (= #{"Bea" "Cid"} coll-varargs))
          (is (= ["Ada" 10] (vec tuple-no-input)))
          (is (= #{["Bea" 20] ["Cid" 30]}
                 (into #{} (map vec) relation-input)))
          (is (= #{{"name" "Bea" "age" 20}
                   {"name" "Cid" "age" 30}}
                 (into #{} keyed-input)))
          (is (= "Ada" (mget entity-map :name)))
          (is (= 10 (mget entity-map :age)))
          (is (instance? Map explain-raw))
          (is (instance? Map explain-form))
          (is (instance? Map explain-typed))
          (is (instance? Map explain-input))
          (is (instance? Map explain-opts))
          (is (instance? Map explain-form-opts))
          (is (= ":db.type/string" (get-in exec-schema [":name" ":db/valueType"])))))

      (with-open [^Connection shared-map (Datalevin/getConn map-dir schema-map opts)]
        (is (= #{"Ada" "Bea" "Cid"}
               (set (.query shared-map
                            "[:find [?name ...] :where [?e :name ?name]]"
                            (java.util.ArrayList.))))))

      (with-open [^KV kv (Datalevin/openKV kv-dir (java-map))]
        (.openDbi kv "raw-items" (java-map))
        (.openDbi kv "typed-items" (java-map))
        (.openDbi kv "typed-data-items" (java-map))
        (.openListDbi kv "list-opts" (java-map))
        (is (= :transacted
               (.transact kv
                          [[":put" "raw-items" "a" "alpha"]
                           [":put" "raw-items" "b" "beta"]])))
        (is (= :transacted
               (.transact kv "raw-items" [[":put" "c" "gamma"]])))
        (is (= :transacted
               (.transact kv "typed-items" [[":put" "a" "alpha"]]
                          ":string" ":string")))
        (is (= :transacted
               (.transact kv "typed-items" [[":put" "b" "beta"]]
                          KVType/STRING KVType/STRING)))
        (is (= :transacted
               (.transact kv "typed-data-items" [[":put" "c" "charlie"]]
                          ":string")))
        (is (= :transacted
               (.transact kv "typed-data-items" [[":put" "d" "delta"]]
                          KVType/STRING)))

        (.putListItems kv "list-opts" "a" [1 2 3 4] ":string" ":long")
        (.putListItems kv "list-opts" "b" [5 6] ":string" ":long")

        (let [visited         (atom [])
              _               (.visitList kv "list-opts"
                                          (reify Consumer
                                            (accept [_ value]
                                              (swap! visited conj value)))
                                          "a" ":string" ":long")
              list-range      (.build (RangeSpec/closed "a" "b"))
              value-range     (.build (RangeSpec/closed 2 6))
              range-read-count (atom 0)
              read-string*    edn/read-string
              visited-range  (atom [])
              {:keys [filtered kept some-value filter-count
                      list-first list-first-n list-page key-page raw-range]}
              (with-redefs [edn/read-string
                            (fn [& xs]
                              (swap! range-read-count inc)
                              (apply read-string* xs))]
                (let [filtered (.listRangeFilter
                                kv "list-opts"
                                (reify BiPredicate
                                  (test [_ key value]
                                    (and (= "a" key) (>= (long value) 3))))
                                list-range ":string" value-range ":long"
                                nil nil)
                      kept     (.listRangeKeep
                                kv "list-opts"
                                (reify BiFunction
                                  (apply [_ key value]
                                    (when (odd? (long value))
                                      (str key ":" value))))
                                list-range ":string" value-range ":long"
                                nil nil)
                      some-value (.listRangeSome
                                  kv "list-opts"
                                  (reify BiFunction
                                    (apply [_ key value]
                                      (when (= 6 (long value))
                                        (str key ":" value))))
                                  list-range ":string" value-range ":long")
                      filter-count (.listRangeFilterCount
                                    kv "list-opts"
                                    (reify BiPredicate
                                      (test [_ key value]
                                        (and (= "b" key) (>= (long value) 5))))
                                    list-range ":string" value-range ":long")
                      _        (.visitListRange
                                 kv "list-opts"
                                 (reify BiConsumer
                                   (accept [_ key value]
                                     (swap! visited-range conj [key value])))
                                 list-range ":string" value-range ":long")
                      list-first (.listRangeFirst kv "list-opts"
                                                  list-range ":string"
                                                  value-range ":long")
                      list-first-n (.listRangeFirstN kv "list-opts" (long 2)
                                                     list-range ":string"
                                                     value-range ":long")
                      list-page (.listRange kv "list-opts"
                                            list-range ":string"
                                            value-range ":long"
                                            (Integer/valueOf 2)
                                            (Integer/valueOf 1))
                      key-page (.keyRange kv "typed-items"
                                          (.build (RangeSpec/closed "a" "d"))
                                          ":string"
                                          (Integer/valueOf 2)
                                          (Integer/valueOf 1))
                      raw-range (.getRange kv "raw-items"
                                           (.build (RangeSpec/closed "a" "b")))]
                  {:filtered filtered
                   :kept kept
                   :some-value some-value
                   :filter-count filter-count
                   :list-first list-first
                   :list-first-n list-first-n
                   :list-page list-page
                   :key-page key-page
                   :raw-range raw-range}))]
          (is (= "alpha" (.getValue kv "raw-items" "a")))
          (is (= 1 (.getRank kv "raw-items" "b")))
          (is (= "alpha" (.getByRank kv "raw-items" 0)))
          (is (= [["a" "alpha"] ["b" "beta"]]
                 (pair-rows raw-range)))
          (is (= "alpha" (.getValue kv "typed-items" "a"
                                    ":string" ":string" true)))
          (is (= 0 (.getRank kv "typed-items" "a" ":string")))
          (is (= ["a" "alpha"] (vec (.getByRank kv "typed-items" 0
                                                ":string" ":string" false))))
          (is (= ["b" "beta"]
                 (vec (.getEntryByRank kv "typed-items" 1
                                       ":string" ":string"))))
          (is (= [["b" "beta"]]
                 (pair-rows (.getRange kv "typed-items"
                                       (RangeSpec/closed "a" "c")
                                       ":string" ":string"
                                       (Integer/valueOf 1)
                                       (Integer/valueOf 1)))))
          (is (= ["b"] key-page))
          (is (zero? @range-read-count))
          (is (= 2 (.keyRangeCount kv "typed-items"
                                   (RangeSpec/all) ":string")))
          (is (= 3 (.rangeCount kv "raw-items"
                                (.build (RangeSpec/all))
                                (cast String nil))))
          (is (pos? (get (.stat kv) :psize)))
          (is (= 2 (get (.stat kv "typed-items") :entries)))
          (is (= [1 2 3 4] @visited))
          (is (= [2 3 4] (.getList kv "list-opts" "a"
                                   ":string" ":long"
                                   (Integer/valueOf 3)
                                   (Integer/valueOf 1))))
          (is (= 4 (.listCount kv "list-opts" "a" ":string")))
          (is (true? (.inList kv "list-opts" "b" 6 ":string" ":long")))
          (is (= [["a" 3] ["a" 4]]
                 (pair-rows filtered)))
          (is (= ["a:3" "b:5"] kept))
          (is (= "b:6" some-value))
          (is (= 2 filter-count))
          (is (= [["a" 2] ["a" 3] ["a" 4] ["b" 5] ["b" 6]]
                 @visited-range))
          (is (= ["a" 2] (vec list-first)))
          (is (= [["a" 2] ["a" 3]]
                 (pair-rows list-first-n)))
          (is (= [["a" 3] ["a" 4]]
                 (pair-rows list-page)))
          (is (= 6 (.listRangeCount kv "list-opts" list-range ":string")))
          (is (= 6 (.keyRangeListCount kv "list-opts" list-range ":string")))
          (.delListItems kv "list-opts" "a" [1 4] ":string" ":long")
          (.deleteListItems kv "list-opts" "b" ":string")
          (is (= [2 3] (.getList kv "list-opts" "a" ":string" ":long")))
          (is (= [] (.getList kv "list-opts" "b" ":string" ":long")))
          (.sync kv 0)
          (is (= 2 (.exec kv "entries" (java-map "dbi-name" "typed-items"))))))
      (finally
        (delete-files-retry plain-dir)
        (delete-files-retry map-dir)
        (delete-files-retry typed-dir)
        (delete-files-retry typed-opts-dir)
        (delete-files-retry kv-dir)))))

(deftest java-runtime-hot-path-regression-test
  (let [conn-dir (test-dir "java-runtime-hot-path-conn")
        kv-dir   (test-dir "java-runtime-hot-path-kv")
        conn     (Datalevin/createConn conn-dir)
        kv       (Datalevin/openKV kv-dir)]
    (try
      (with-open [^Connection c conn
                  ^KV k kv]
        (let [many-input-query (str "[:find ?v0 . :in $ "
                                    (str/join " " (map #(str "?v" %) (range 21)))
                                    " :where [(= ?v0 ?v0)]]")
              many-inputs (into-array Object (range 21))]
          (is (= 0 (.query c many-input-query many-inputs))))

        (.openDbi k "items")
        (.transact k "items"
                   [[":put" "a" "alpha"]
                    [":put" "b" "beta"]]
                   KVType/STRING
                   KVType/STRING)
        (.openListDbi k "list")
        (.putListItems k "list" "a" [1 2 3] KVType/STRING KVType/LONG)

        (let [read-count   (atom 0)
              read-string* edn/read-string
              datom-called? (atom false)
              visited      (atom [])
              {:keys [raw-range list-range relation]}
              (with-redefs [edn/read-string
                            (fn [& xs]
                              (swap! read-count inc)
                              (apply read-string* xs))
                            datalevin.datom/datom?
                            (fn [& _]
                              (reset! datom-called? true)
                              (throw (ex-info "datom? should not be called for non-datom query/range results" {})))
                            datalevin.datom/datom-e
                            (fn [& _]
                              (throw (ex-info "datom-e should not be called here" {})))
                            datalevin.datom/datom-a
                            (fn [& _]
                              (throw (ex-info "datom-a should not be called here" {})))
                            datalevin.datom/datom-v
                            (fn [& _]
                              (throw (ex-info "datom-v should not be called here" {})))
                            datalevin.datom/datom-tx
                            (fn [& _]
                              (throw (ex-info "datom-tx should not be called here" {})))
                            datalevin.datom/datom-added
                            (fn [& _]
                              (throw (ex-info "datom-added should not be called here" {})))]
                (let [raw-range (.getRange k "items"
                                           (RangeSpec/closed "a" "b")
                                           ":string"
                                           ":string"
                                           nil
                                           nil)
                      relation (.queryRelation c
                                               (doto (Datalevin/query)
                                                 (.find (into-array String ["?k" "?v"]))
                                                 (.whereBind "ground"
                                                             (Datalevin/var "k")
                                                             (into-array Object ["ok"]))
                                                 (.whereBind "ground"
                                                             (Datalevin/var "v")
                                                             (into-array Object [42]))))
                      list-range (.listRange k "list"
                                             (RangeSpec/closed "a" "a")
                                             ":string"
                                             (RangeSpec/closed 1 3)
                                             ":long"
                                             nil
                                             nil)
                      _ (.visitListRange k "list"
                                         (reify BiConsumer
                                           (accept [_ key value]
                                             (swap! visited conj [key value])))
                                         (RangeSpec/closed "a" "a")
                                         ":string"
                                         (RangeSpec/closed 1 3)
                                         ":long")]
                  {:raw-range raw-range
                   :list-range list-range
                   :relation relation}))]
          (is (zero? @read-count))
          (is (false? @datom-called?))
          (is (= [["a" "alpha"] ["b" "beta"]]
                 (pair-rows raw-range)))
          (is (= #{["ok" 42]}
                 (into #{} (map vec) relation)))
          (is (= [["a" 1] ["a" 2] ["a" 3]]
                 (pair-rows list-range)))
          (is (= [["a" 1] ["a" 2] ["a" 3]]
                 @visited)))

        (let [query-read-count (atom 0)
              read-string*     edn/read-string
              token            (str "java-query-cache-" (java.util.UUID/randomUUID))
              query-edn        (str "[:find ?v . :where [(ground \"" token "\") ?v]]")]
          (with-redefs [edn/read-string
                        (fn [& xs]
                          (swap! query-read-count inc)
                          (apply read-string* xs))]
            (let [parsed-1 (DatalevinInterop/readEdn query-edn)
                  parsed-2 (DatalevinInterop/readEdn query-edn)]
              (is (identical? parsed-1 parsed-2))
              (is (= query-edn (pr-str parsed-1))))
            (is (= 1 @query-read-count)))))
      (finally
        (delete-files-retry conn-dir)
        (delete-files-retry kv-dir)))))

(deftest java-interop-local-api-test
  (let [conn-dir (test-dir "java-interop-conn")
        kv-dir   (test-dir "java-interop-kv")
        schema   (java-map "name" (java-map ":db/valueType" ":db.type/string"
                                            ":db/unique" ":db.unique/identity")
                           "age"  (java-map ":db/valueType" ":db.type/long"))
        conn     (DatalevinInterop/createConnection conn-dir schema nil)
        kv       (DatalevinInterop/openKeyValue kv-dir nil)]
    (try
      (let [db         (DatalevinInterop/connectionDb conn)
            tx         (DatalevinInterop/txData
                        [{":db/id" -1 "name" "Alice" "age" 30}
                         {":db/id" -2 "name" "Bob" "age" 25}])
            report     (DatalevinInterop/coreInvoke "transact!"
                                                    [conn tx (java-map "source" "interop")])
            query      (DatalevinInterop/readEdn
                        "[:find [?name ...] :where [?e :name ?name]]")
            names      (set (DatalevinInterop/coreInvoke "q" [query db]))
            names-bridge (set (DatalevinInterop/coreInvokeBridge "q" [query db]))
            lookup     (DatalevinInterop/lookupRef ["name" "Alice"])
            alice      (DatalevinInterop/coreInvoke
                        "pull"
                        [db (DatalevinInterop/readEdn "[:name :age]") lookup])
            alice-bridge (DatalevinInterop/bridgeResult alice)
            loader     (DatalevinInterop/currentClassLoader)
            string-type (DatalevinInterop/kvType ":string")
            kv-txs     (DatalevinInterop/kvTxs
                        [[:put "a" "alpha"]
                         [:put "b" "beta"]])
            _          (DatalevinInterop/coreInvoke "open-dbi" [kv "items"])
            _          (DatalevinInterop/coreInvoke
                        "transact-kv"
                        [kv "items" kv-txs string-type string-type])
            kv-value   (DatalevinInterop/coreInvoke
                        "get-value"
                        [kv "items" "b" string-type string-type true])
            kv-entries (DatalevinInterop/coreInvoke "entries" [kv "items"])]
        (is (= ":name" (str (DatalevinInterop/keyword "name"))))
        (is (= "?e" (str (DatalevinInterop/symbol "?e"))))
        (is (= ":string" (str string-type)))
        (is (instance? Map report))
        (is (= "interop" (mget (mget report :tx-meta) "source")))
        (is (= #{"Alice" "Bob"} names))
        (is (= #{"Alice" "Bob"} names-bridge))
        (is (= "Alice" (mget alice :name)))
        (is (= 30 (mget alice :age)))
        (is (= "Alice" (mget alice-bridge :name)))
        (is (= 30 (mget alice-bridge :age)))
        (is (= (.getContextClassLoader (Thread/currentThread)) loader))
        (is (= "beta" kv-value))
        (is (= 2 kv-entries))
        (is (= ["items"] (vec (DatalevinInterop/coreInvoke "list-dbis" [kv]))))
        (is (false? (DatalevinInterop/connectionClosed conn)))
        (is (false? (DatalevinInterop/keyValueClosed kv))))
      (finally
        (DatalevinInterop/closeConnection conn)
        (DatalevinInterop/closeKeyValue kv)
        (is (DatalevinInterop/connectionClosed conn))
        (is (DatalevinInterop/keyValueClosed kv))
        (delete-files-retry conn-dir)
        (delete-files-retry kv-dir)))))

(deftest java-interop-helper-coverage-test
  (let [conn-dir   (test-dir "java-interop-helper-conn")
        kv-dir     (test-dir "java-interop-helper-kv")
        byte-array-class (class (byte-array 0))
        schema-map (java-map "name" (java-map ":db/valueType" ":db.type/string"
                                              ":db/unique" ":db.unique/identity")
                             "age"  (java-map ":db/valueType" ":db.type/long"))
        opts       (java-map ":cache-limit" 64)]
    (try
      (let [raw-schema    (DatalevinInterop/schema schema-map)
            raw-opts      (DatalevinInterop/options opts)
            raw-udf       (DatalevinInterop/udfDescriptor
                           (java-map ":udf/lang" ":java"
                                     ":udf/kind" ":query-fn"
                                     ":udf/id" ":math/inc"))
            raw-rename    (DatalevinInterop/renameMap
                           (java-map "name" "full-name"))
            raw-delete    (DatalevinInterop/deleteAttrs
                           (Datalevin/listOf (into-array Object ["age"])))
            normalized-schema {:name {:db/valueType :db.type/string}}
            normalized-opts   {:cache-limit 64}
            normalized-udf    {:udf/lang :java
                               :udf/kind :query-fn
                               :udf/id   :math/inc}
            normalized-rename {:name :full-name}
            normalized-delete #{:age}
            normalized-tx     [{:db/id -1 :name "Ada"}]
            normalized-kv-txs [[:put "a" "alpha"]]
            normalized-kv-type [:string :long]
            normalized-bytes-tx (DatalevinInterop/kvTxs
                                 [[:put [1 2] [3 255]]]
                                 ":bytes"
                                 ":bytes")
            normalized-bytes-range (DatalevinInterop/kvRange
                                    [":closed" [1] [3 255]]
                                    ":bytes")]
        (is (= :db.type/string (mget (mget raw-schema :name) :db/valueType)))
        (is (= :db.unique/identity (mget (mget raw-schema :name) :db/unique)))
        (is (= :db.type/long (mget (mget raw-schema :age) :db/valueType)))
        (is (= 64 (mget raw-opts :cache-limit)))
        (is (= :java (mget raw-udf :udf/lang)))
        (is (= :query-fn (mget raw-udf :udf/kind)))
        (is (= :math/inc (mget raw-udf :udf/id)))
        (is (= :full-name (mget raw-rename :name)))
        (is (= #{:age} (set raw-delete)))
        (is (identical? normalized-schema
                        (DatalevinInterop/schema normalized-schema)))
        (is (identical? normalized-opts
                        (DatalevinInterop/options normalized-opts)))
        (is (identical? normalized-udf
                        (DatalevinInterop/udfDescriptor normalized-udf)))
        (is (identical? normalized-rename
                        (DatalevinInterop/renameMap normalized-rename)))
        (is (identical? normalized-delete
                        (DatalevinInterop/deleteAttrs normalized-delete)))
        (is (identical? normalized-tx
                        (DatalevinInterop/txData normalized-tx)))
        (is (identical? normalized-kv-txs
                        (DatalevinInterop/kvTxs normalized-kv-txs)))
        (is (identical? normalized-kv-type
                        (DatalevinInterop/kvType normalized-kv-type)))
        (is (instance? byte-array-class (nth (first normalized-bytes-tx) 1)))
        (is (instance? byte-array-class (nth (first normalized-bytes-tx) 2)))
        (is (instance? byte-array-class (nth normalized-bytes-range 1)))
        (is (instance? byte-array-class (nth normalized-bytes-range 2))))

      (let [anon (DatalevinInterop/getConnection nil nil nil)]
        (try
          (is (false? (DatalevinInterop/connectionClosed anon)))
          (finally
            (DatalevinInterop/closeConnection anon)
            (is (DatalevinInterop/connectionClosed anon)))))

      (let [conn        (DatalevinInterop/createConnection conn-dir schema-map opts)
            raw-schema  (DatalevinInterop/coreInvoke "schema" [conn])
            db          (DatalevinInterop/connectionDb conn)
            kv          (DatalevinInterop/openKeyValue kv-dir (java-map))
            string-type (DatalevinInterop/kvType ":string")]
        (try
          (is (re-find #"clojure\.lang"
                       (.getName (.getPackage (.getClass raw-schema)))))
          (DatalevinInterop/coreInvoke
           "update-schema"
           [conn
            nil
            (DatalevinInterop/deleteAttrs
             (Datalevin/listOf (into-array Object ["age"])))
            (DatalevinInterop/renameMap
             (java-map "name" "full-name"))])
          (DatalevinInterop/coreInvoke
           "transact!"
           [conn
            (DatalevinInterop/txData
             [{":db/id" -1 "full-name" "Ada"}])])
          (is (= "Ada"
                 (mget (DatalevinInterop/coreInvoke
                        "pull"
                        [db
                         (DatalevinInterop/readEdn "[:full-name]")
                         (DatalevinInterop/lookupRef ["full-name" "Ada"])])
                       :full-name)))
          (is (= #{"Ada"}
                 (set (DatalevinInterop/coreInvoke
                       "q"
                       [(DatalevinInterop/readEdn
                         "[:find [?name ...] :where [?e :full-name ?name]]")
                        db]))))
          (DatalevinInterop/coreInvoke "open-dbi" [kv "items"])
          (DatalevinInterop/coreInvoke
           "transact-kv"
           [kv
            "items"
            (DatalevinInterop/kvTxs
             [[:put "a" "alpha"]
              [:put "b" "beta"]])
            string-type
            string-type])
          (is (= "beta"
                 (DatalevinInterop/coreInvoke
                  "get-value"
                  [kv "items" "b" string-type string-type true])))
          (is (= ["items"]
                 (vec (DatalevinInterop/coreInvoke "list-dbis" [kv]))))
          (let [shared (DatalevinInterop/getConnection conn-dir schema-map opts)]
            (try
              (is (= #{"Ada"}
                     (set (DatalevinInterop/coreInvoke
                           "q"
                           [(DatalevinInterop/readEdn
                             "[:find [?name ...] :where [?e :full-name ?name]]")
                            (DatalevinInterop/connectionDb shared)]))))
              (finally
                (try
                  (DatalevinInterop/closeConnection shared)
                  (catch Exception _)))))
          (finally
            (try
              (DatalevinInterop/closeConnection conn)
              (catch Exception _))
            (try
              (DatalevinInterop/closeKeyValue kv)
              (catch Exception _))
            (is (DatalevinInterop/connectionClosed conn))
            (is (DatalevinInterop/keyValueClosed kv)))))
      (finally
        (delete-files-retry conn-dir)
        (delete-files-retry kv-dir)))))

(deftest java-interop-udf-test
  (let [conn-dir          (test-dir "java-interop-udf")
        schema            (java-map "name"  (java-map ":db/valueType" ":db.type/string"
                                                      ":db/unique" ":db.unique/identity")
                                    "score" (java-map ":db/valueType" ":db.type/long"))
        tx-descriptor     (java-map ":udf/lang" ":java"
                                    ":udf/kind" ":tx-fn"
                                    ":udf/id"   ":person/bootstrap")
        query-descriptor  (java-map ":udf/lang" ":java"
                                    ":udf/kind" ":query-fn"
                                    ":udf/id"   ":math/inc")
        registry          (Datalevin/createUdfRegistry)
        conn              (DatalevinInterop/createConnection
                           conn-dir
                           schema
                           (java-map ":runtime-opts"
                                     (java-map ":udf-registry" registry)))]
    (try
      (is (false? (DatalevinInterop/registeredUdf registry tx-descriptor)))
      (DatalevinInterop/registerUdf
       registry
       tx-descriptor
       (reify UdfFunction
         (invoke [_ args]
           (let [name (.get ^List args 1)]
             (Datalevin/listOf
              (into-array Object
                          [(Datalevin/mapOf
                            (into-array Object
                                        [":db/id" -1
                                         "name" name
                                         "score" 10]))]))))))
      (Datalevin/registerUdf
       registry
       query-descriptor
       (reify UdfFunction
         (invoke [_ args]
           (+ 1 (long (.get ^List args 0))))))
      (is (DatalevinInterop/registeredUdf registry tx-descriptor))
      (is (Datalevin/registeredUdf registry query-descriptor))
      (is (= :java
             (mget (Datalevin/udfDescriptor query-descriptor) :udf/lang)))

      (DatalevinInterop/coreInvoke
       "transact!"
       [conn
        (DatalevinInterop/txData
         [{":db/ident" (Datalevin/kw "math/inc")
           ":db/udf"   query-descriptor}
          (Datalevin/listOf
           (into-array Object
                       [":db.fn/call"
                        (DatalevinInterop/udfDescriptor tx-descriptor)
                        "Ada"]))])])

      (let [db          (DatalevinInterop/connectionDb conn)
            names       (set (DatalevinInterop/coreInvoke
                              "q"
                              [(DatalevinInterop/readEdn
                                "[:find [?name ...] :where [?e :name ?name]]")
                               db]))
            inline-v    (DatalevinInterop/coreInvoke
                         "q"
                         [(DatalevinInterop/readEdn
                           "[:find ?v . :in $ ?desc ?n :where [(udf ?desc ?n) ?v]]")
                          db
                          (Datalevin/udfDescriptor query-descriptor)
                          9])
            installed-v (DatalevinInterop/coreInvoke
                         "q"
                         [(DatalevinInterop/readEdn
                           "[:find ?v . :in $ ?n :where [(udf :math/inc ?n) ?v]]")
                          db
                          41])]
        (is (= #{"Ada"} names))
        (is (= 10 inline-v))
        (is (= 42 installed-v)))

      (Datalevin/unregisterUdf registry query-descriptor)
      (DatalevinInterop/unregisterUdf registry tx-descriptor)
      (is (false? (Datalevin/registeredUdf registry query-descriptor)))
      (is (false? (DatalevinInterop/registeredUdf registry tx-descriptor)))
      (finally
        (DatalevinInterop/closeConnection conn)
        (delete-files-retry conn-dir)))))

(deftest java-friendly-client-api-test
  (tdc/server-fixture
    (fn []
      (let [suffix   (str (UUID/randomUUID))
            uri      "dtlv://datalevin:datalevin@localhost"
            db-name  (str "java-friendly-" suffix)
            db-name2 (str "java-friendly-extra-" suffix)
            username (str "java-user-" suffix)
            password "secret"
            role     (str "java-role-" suffix)
            client   (Datalevin/newClient uri (java-map ":pool-size" 1))
            other-client (Datalevin/newClient uri (java-map ":pool-size" 1))
            alias-client (Datalevin/newClient uri (java-map ":pool-size" 1))]
        (try
          (with-open [^Client c client]
            (let [client-id (.clientId c)
                  _         (.createDatabase c db-name "datalog")
                  _         (.createDatabase c db-name2 DatabaseType/DATALOG)
                  _         (.createUser c username password)
                  _         (.resetPassword c username (str password "-2"))
                  _         (.createRole c role)
                  _         (.assignRole c role username)
                  _         (.grantPermission c role
                                              PermissionAction/ALTER
                                              PermissionObject/DATABASE
                                              db-name)
                  _         (.grantPermission c role
                                              ":datalevin.server/view"
                                              ":datalevin.server/database"
                                              db-name2)
                  open-info (.openDatabaseInfo c db-name DatabaseType/DATALOG nil nil)
                  open-info2 (.openDatabaseInfo c db-name2 "datalog" nil nil)
                  dbs       (set (.listDatabases c))
                  dbs-in-use (set (.listDatabasesInUse c))
                  users     (set (.listUsers c))
                  roles     (set (.listRoles c))
                  user-roles (set (.listUserRoles c username))
                  role-perms (.listRolePermissions c role)
                  user-perms (.listUserPermissions c username)
                  user-query-form
                  (DatalevinInterop/readEdn
                   "[:find ?u . :in $ ?u :where [?e :user/name ?u]]")
                  system-users (set (.querySystem c
                                                  "[:find [?u ...] :where [?e :user/name ?u]]"
                                                  (java.util.ArrayList.)))
                  system-user-form (.querySystemForm c user-query-form
                                                    (java.util.ArrayList.
                                                     [username]))
                  clients   (.showClients c)
                  client-info (get clients client-id)
                  open-db-info (get (mget client-info :open-dbs) db-name)
                  raw-users (set (.exec c "list-users" nil))]
              (is (zero? (handle-count)))
              (is (instance? UUID client-id))
              (is (= false (.disconnected c)))
              (is (instance? Map open-info))
              (is (instance? Map open-info2))
              (is (instance? Map clients))
              (is (every? map? (vals clients)))
              (is (contains? dbs-in-use db-name))
              (is (contains? users username))
              (is (contains? raw-users username))
              (is (contains? roles (keyword role)))
              (is (contains? user-roles (keyword role)))
              (is (contains? system-users username))
              (is (= username system-user-form))
              (is (some (fn [perm]
                          (permission-matches? perm
                                               :datalevin.server/alter
                                               :datalevin.server/database))
                        role-perms))
              (is (some (fn [perm]
                          (permission-matches? perm
                                               :datalevin.server/alter
                                               :datalevin.server/database))
                        user-perms))
              (is (map? client-info))
              (is (map? open-db-info))
              (is (true? (mget open-db-info :datalog?)))
              (is (contains? dbs db-name))
              (is (contains? dbs db-name2))
              (is (= username
                     (.querySystem c
                                   "[:find ?u . :in $ ?u :where [?e :user/name ?u]]"
                                   (into-array Object [username]))))
              (.closeDatabase c db-name2)
              (.openDatabase c db-name2 DatabaseType/DATALOG)
              (.closeDatabase c db-name2)
              (.revokePermission c role
                                 PermissionAction/ALTER
                                 PermissionObject/DATABASE
                                 db-name)
              (is (not (some (fn [perm]
                               (permission-matches? perm
                                                    :datalevin.server/alter
                                                    :datalevin.server/database))
                             (.listRolePermissions c role))))
              (.revokePermission c role
                                 ":datalevin.server/view"
                                 ":datalevin.server/database"
                                 db-name2)
              (is (not (some (fn [perm]
                               (permission-matches? perm
                                                    :datalevin.server/view
                                                    :datalevin.server/database))
                             (.listRolePermissions c role))))
              (.disconnectClient c (.clientId other-client))
              (dotimes [_ 40]
                (when (contains? (set (keys (.showClients c)))
                                 (.clientId other-client))
                  (Thread/sleep 25)))
              (is (not (contains? (set (keys (.showClients c)))
                                  (.clientId other-client))))
              (.disconnect alias-client)
              (is (.disconnected alias-client))
              (.withdrawRole c role username)
              (is (not (contains? (set (.listUserRoles c username))
                                  (str ":" role))))
              (.dropRole c role)
              (.dropUser c username)
              (.closeDatabase c db-name)
              (.dropDatabase c db-name)
              (.dropDatabase c db-name2)))
          (is (zero? (handle-count)))
          (is (.disconnected client))
          (finally
            (when-not (.disconnected other-client)
              (.close other-client))
            (when-not (.disconnected alias-client)
              (.disconnect alias-client))
            (when-not (.disconnected client)
              (.close client))))))))

(deftest java-interop-client-api-test
  (tdc/server-fixture
    (fn []
      (let [suffix   (str (UUID/randomUUID))
            db-name  (str "java-interop-" suffix)
            username (str "java-interop-user-" suffix)
            password "secret"
            role     (str "java-interop-role-" suffix)
            client   (DatalevinInterop/newClient "dtlv://datalevin:datalevin@localhost"
                                                 (java-map ":pool-size" 1))
            role-key (DatalevinInterop/role role)
            db-type  (DatalevinInterop/databaseType "datalog")
            db-perm  (DatalevinInterop/permissionKeyword ":datalevin.server/database")
            alter    (DatalevinInterop/permissionKeyword ":datalevin.server/alter")
            target   (DatalevinInterop/permissionTarget ":datalevin.server/database" db-name)]
        (try
          (DatalevinInterop/clientInvoke "create-database" [client db-name db-type])
          (DatalevinInterop/clientInvoke "create-user" [client username password])
          (DatalevinInterop/clientInvoke "create-role" [client role-key])
          (DatalevinInterop/clientInvoke "assign-role" [client role-key username])
          (DatalevinInterop/clientInvoke "grant-permission"
                                           [client role-key alter db-perm target])
          (DatalevinInterop/clientInvoke "open-database"
                                           [client db-name "datalog" nil nil false])
          (let [user-query  (DatalevinInterop/readEdn
                             "[:find ?u . :in $ ?u :where [?e :user/name ?u]]")
                dbs        (set (DatalevinInterop/clientInvoke "list-databases" [client]))
                dbs-in-use (set (DatalevinInterop/clientInvoke "list-databases-in-use" [client]))
                users      (set (DatalevinInterop/clientInvoke "list-users" [client]))
                users-bridge (set (DatalevinInterop/clientInvokeBridge
                                   "list-users"
                                   [client]))
                roles      (set (DatalevinInterop/clientInvoke "list-roles" [client]))
                user-roles (set (DatalevinInterop/clientInvoke "list-user-roles"
                                                               [client username]))
                user-name  (DatalevinInterop/clientInvoke
                            "query-system"
                            [client user-query username])
                user-name-bridge (DatalevinInterop/clientInvokeBridge
                                  "query-system"
                                  [client user-query username])
                role-perms (DatalevinInterop/clientInvoke "list-role-permissions"
                                                          [client role-key])]
            (is (contains? dbs db-name))
            (is (contains? dbs-in-use db-name))
            (is (contains? users username))
            (is (contains? users-bridge username))
            (is (contains? roles role-key))
            (is (contains? user-roles role-key))
            (is (= username user-name))
            (is (= username user-name-bridge))
            (is (some #(permission-matches? % alter db-perm) role-perms))
            (is (false? (DatalevinInterop/clientDisconnected client))))
          (finally
            (try
              (DatalevinInterop/clientInvoke "revoke-permission"
                                               [client role-key alter db-perm target])
              (catch Exception _))
            (try
              (DatalevinInterop/clientInvoke "withdraw-role" [client role-key username])
              (catch Exception _))
            (try
              (DatalevinInterop/clientInvoke "drop-role" [client role-key])
              (catch Exception _))
            (try
              (DatalevinInterop/clientInvoke "drop-user" [client username])
              (catch Exception _))
            (try
              (DatalevinInterop/clientInvoke "close-database" [client db-name])
              (catch Exception _))
            (try
              (DatalevinInterop/clientInvoke "drop-database" [client db-name])
              (catch Exception _))
            (DatalevinInterop/closeClient client)
            (is (DatalevinInterop/clientDisconnected client))))))))

(deftest java-public-surface-guard-test
  (doseq [[class expected] java-friendly-surface]
    (is (= expected (public-method-names class))
        (str "Public surface changed for " (.getSimpleName ^Class class)
             "; update tests for the new method family."))))
