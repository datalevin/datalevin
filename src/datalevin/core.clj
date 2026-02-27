;;
;; Copyright (c) Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns datalevin.core
  "User facing API for Datalevin library features"
  (:refer-clojure :exclude [sync load])
  (:require
   [datalevin.util :as u]
   [datalevin.conn :as conn]
   [datalevin.dump :as dump]
   [datalevin.csv :as csv]
   [datalevin.search :as sc]
   [datalevin.vector :as v]
   [datalevin.db :as db]
   [datalevin.datom :as dd]
   [datalevin.interface :as i]
   [datalevin.lmdb :as l]
   [datalevin.pull-parser]
   [datalevin.pull-api :as dp]
   [datalevin.query :as dq]
   [datalevin.built-ins :as dbq]
   [datalevin.entity :as de]
   [datalevin.bits :as b]
   [datalevin.kv :as kv]
   [datalevin.binding.cpp]
   [datalevin.datafy]))

;; Entities

(def ^{:arglists '([db eid])
       :doc      "Retrieves an entity by its id from a Datalog database. Entities
  are map-like structures to navigate Datalevin database content.

  `db` is a Datalog database.

  For `eid` pass entity id or lookup ref:

      (entity db 1)
      (entity db [:unique-attr :value])

  Creating an entity by id is very cheap, almost no-op, as attr access
  is on-demand:

      (entity db 1) ; => {:db/id 1}

  Entity attributes can be lazily accessed through key lookups:

      (:attr (entity db 1)) ; => :value
      (get (entity db 1) :attr) ; => :value

  Cardinality many attributes are returned sequences:

      (:attrs (entity db 1)) ; => [:v1 :v2 :v3]

  Reference attributes are returned as another entities:

      (:ref (entity db 1)) ; => {:db/id 2}
      (:ns/ref (entity db 1)) ; => {:db/id 2}

  References can be walked backwards by prepending `_` to name part
  of an attribute:

      (:_ref (entity db 2)) ; => [{:db/id 1}]
      (:ns/_ref (entity db 2)) ; => [{:db/id 1}]

  Reverse reference lookup returns sequence of entities unless
  attribute is marked as `:db/isComponent`:

      (:_component-ref (entity db 2)) ; => {:db/id 1}

  Entity gotchas:

    - Entities print as map, but are not exactly maps (they have
    compatible get interface though).
    - Entities retain reference to the database.
    - Creating an entity by id is very cheap, almost no-op
    (attributes are looked up on demand).
    - Comparing entities just compares their ids. Be careful when
    comparing entities taken from different dbs or from different versions of the
    same db.
    - Accessed entity attributes are cached on entity itself (except
    backward references).
    - When printing, only cached attributes (the ones you have accessed
      before) are printed. See [[touch]]."}
  entity de/entity)


(def ^{:arglists '([ent attr value])
       :doc      "Add an attribute value to an entity of a Datalog database"}
  add de/add)

(def ^{:arglists '([ent attr]
                   [ent attr value])
       :doc      "Remove an attribute from an entity of a Datalog database"}
  retract de/retract)

(def ^{:arglists '([db eid])
       :doc      "Given lookup ref `[unique-attr value]`, returns numberic entity id.

  `db` is a Datalog database.

  If entity does not exist, returns `nil`.

  For numeric `eid` returns `eid` itself (does not check for entity
  existence in that case)."}
  entid db/entid)

(def ^{:arglists '([entity])
       :doc      "Returns the Datalog DB that this entity was created from."}
  entity-db de/entity-db)

(def ^{:arglists '([e])
       :doc      "Forces all entity attributes to be eagerly fetched and cached.
Only usable for debug output.

  Usage:

             (entity db 1) ; => {:db/id 1}
             (touch (entity db 1)) ; => {:db/id 1, :dislikes [:pie], :likes [:pizza]}
             "}
  touch de/touch)

;; Pull API

(def ^{:arglists '([db pattern id opts]
                   [db pattern id])
       :doc      "Fetches data from a Datalog database using recursive declarative
  description. See [docs.datomic.com/on-prem/pull.html](https://docs.datomic.com/on-prem/pull.html).

  Unlike [[entity]], returns plain Clojure map (not lazy).

  Supported opts:

   `:visitor` a fn of 4 arguments, `pattern`, `e`, `a`, and `v`. It will be called for every entity/attribute pull touches, for side effect.

   `(:db.pull/attr e a nil)` when pulling a normal attribute, no matter if it has value or not

   `(:db.pull/wildcard e nil nil)` when pulling every attribute on an entity

   `(:db.pull/reverse nil a v)` when pulling reverse attribute

  Usage:

                (pull db [:db/id, :name, :likes, {:friends [:db/id :name]}] 1)
                ; => {:db/id   1,
                ;     :name    \"Ivan\"
                ;     :likes   [:pizza]
                ;     :friends [{:db/id 2, :name \"Oleg\"}]}"}
  pull dp/pull)

(def ^{:arglists '([db pattern id opts]
                   [db pattern id])
       :doc      "Same as [[pull]], but accepts sequence of ids and returns
  sequence of maps.

  Usage:

             (pull-many db [:db/id :name] [1 2])
             ; => [{:db/id 1, :name \"Ivan\"}
             ;     {:db/id 2, :name \"Oleg\"}]"}
  pull-many dp/pull-many)

;; Creating DB

(def ^{:arglists '([]
                   [dir]
                   [dir schema]
                   [dir schema opts])
       :doc      "Open a Datalog database at the given location.

 `dir` could be a local directory path or a dtlv connection URI string.
  Creates an empty database there if it does not exist yet. Update the
  schema if one is given. Return reference to the database.

 `opts` map has keys:

   * `:validate-data?`, a boolean, instructing the system to validate data
 type during transaction. Default is `false`.

   * `:closed-schema?`, a boolean, instructing the system to only allow entity attributes defined in the schema during transaction. Default is `false`.

   * `:auto-entity-time?`, a boolean indicating whether to maintain
 `:db/created-at` and `:db/updated-at` values for each entity. Default
 is `false`.

   * `:search-domains`, an option map from domain names to search option maps of those domains, which will be passed to the corresponding full-text search engines. See [[new-search-engine]]

   * `:search-opts` is the default options passed to [[fulltext]] function for the default search domains in case `:search-domains` above is not specified.

   * `:kv-opts`, an option map that will be passed to the underlying kV store

   * `:wal?`, a boolean, enabling WAL mode for the underlying KV store.

   * `:wal-durability-profile`, one of `:strict` or `:relaxed`. `:strict` waits for durable WAL acknowledgement per transaction; `:relaxed` improves throughput by batching durability and may lose a small tail of recent transactions on a crash.

   * `:wal-group-commit`, maximum transactions per durability batch in
 `:relaxed` mode. Default comes from
 `datalevin.constants/*wal-group-commit*`.

   * `:wal-group-commit-ms`, maximum milliseconds per durability batch in
 `:relaxed` mode. Default comes from
 `datalevin.constants/*wal-group-commit-ms*`.

   * `:client-opts` is the option map passed to the client if `dir` is a
 remote URI string.

  For Datalog stores opened through this API, the default for a new local
  store is `:wal? true` with `:wal-durability-profile :strict`.


  Usage:

             (empty-db)

             (empty-db \"/tmp/test-empty-db\")

             (empty-db \"/tmp/test-empty-db\" {:likes {:db/cardinality :db.cardinality/many}})

             (empty-db \"dtlv://datalevin:secret@example.host/mydb\" {} {:auto-entity-time? true :search-opts {:analyzer blank-space-analyzer}})"}
  empty-db db/empty-db)

(def ^{:arglists '([x])
       :doc      "Returns `true` if the given value is a Datalog database.
  Has the side effect of updating the cache of the db to the most recent.
  Return `false` otherwise. "}
  db? db/db?)

(def ^{:arglists '([e a v]
                   [e a v tx]
                   [e a v tx added])
       :doc      "Low-level fn to create raw datoms in a Datalog db.

             Optionally with transaction id (number) and `added` flag (`true` for addition, `false` for retraction).

             See also [[init-db]]."}
  datom dd/datom)

(def ^{:arglists '([x])
       :doc      "Returns `true` if the given value is a datom, `false` otherwise."}
  datom? dd/datom?)

(def ^{:arglists '([d])
       :doc      "Return the entity id of a datom"}
  datom-e dd/datom-e)

(def ^{:arglists '([d])
       :doc      "Return the attribute of a datom"}
  datom-a dd/datom-a)

(def ^{:arglists '([d])
       :doc      "Return the value of a datom"}
  datom-v dd/datom-v)

(def ^{:arglists '([datoms]
                   [datoms dir]
                   [datoms dir schema]
                   [datoms dir schema opts])
       :doc      "Low-level function for creating a Datalog database quickly from a sequence of trusted datoms, useful for bulk data loading. `dir` could be a local directory path or a dtlv connection URI string. Does no validation on inputs, so `datoms` must be well-formed and match schema.

 `opts` map has keys:

   * `:validate-data?`, a boolean, instructing the system to validate data type during transaction. Default is `false`.

   * `:closed-schema?`, a boolean, instructing the system to only allow entity attributes defined in the schema during transaction. Default is `false`.

   * `:auto-entity-time?`, a boolean indicating whether to maintain `:db/created-at` and `:db/updated-at` values for each entity. Default is `false`.

   * `:search-domains`, an option map from domain names to search option maps of those domains, which will be passed to the corresponding full-text search engines. See [[new-search-engine]]

   * `:search-opts` is the default options passed to [[fulltext]] function for the default search domains in case `:search-domains` above is not specified.

   * `:kv-opts`, an option map that will be passed to the underlying kV store

   * `:wal?`, a boolean, enabling WAL mode for the underlying KV store.

   * `:wal-durability-profile`, one of `:strict` or `:relaxed`.

   * `:wal-group-commit`, maximum transactions per durability batch in
 `:relaxed` mode. Default comes from
 `datalevin.constants/*wal-group-commit*`.

   * `:wal-group-commit-ms`, maximum milliseconds per durability batch in
 `:relaxed` mode. Default comes from
 `datalevin.constants/*wal-group-commit-ms*`.

 See also [[datom]], [[new-search-engine]]."}
  init-db db/init-db)

(def ^{:arglists '([db datoms])
       :doc      "Low-level function for filling a Datalog database quickly with a sequence of trusted datoms, useful for bulk data loading. Does no validation on inputs, so `datoms` must be well-formed and match schema.

 See also [[datom]], [[init-db]]."}
  fill-db db/fill-db)

(def ^{:arglists '([db])
       :doc      "Close the Datalog database"}
  close-db db/close-db)

;; Changing DB

(u/import-macro l/with-transaction-kv)
(u/import-macro conn/with-transaction)
(u/import-macro conn/with-conn)

(def ^{:arglists '([db]
                   [db n])
       :doc      "Get or set the cache limit of a Datalog DB. Default is 256. Set to 0 to
   disable the cache, useful when transacting bulk data as it saves memory."}
  datalog-index-cache-limit db/datalog-index-cache-limit)

(def ^{:arglists '([conn])
       :doc      "Rollback writes of the transaction from inside [[with-transaction]]."}
  abort-transact db/abort-transact)

(def ^:no-doc with conn/with)

(def ^{:arglists '([db tx-data])
       :doc      "Returns a transaction report without side-effects. Useful for obtaining
  the would-be db state and the would-be set of datoms."}
  tx-data->simulated-report db/tx-data->simulated-report)

(def ^:no-doc db-with conn/db-with)

;; Query

(def ^{:arglists '([query & inputs])
       :doc      "Executes a Datalog query, which supports [Datomic Query Format](https://docs.datomic.com/query/query-data-reference.html).

  In addition, when `:find` spec is a relation, `:order-by` clause is supported, which can be followed by a single variable or a vector. The vector includes one or more variables, each optionally followed by a keyword `:asc` or `:desc`, specifying ascending or descending order, respectively. The default is `:asc`. `:limit` is also supported to specify the number of tuples to be returned.

          Usage:

          ```
          (q '[:find ?value
               :where [_ :likes ?value]
               :order-by [?value :desc]
               :timeout 5000]
             db)
          ; => #{[\"pizza\"] [\"pie\"] [\"fries\"] [\"candy\"]}
          ```"}
  q dq/q)

(def ^{:arglists '([opts query & inputs])
       :doc      "Explain the query plan for a Datalog query.

    `opts` is a map, with these keys:

      * `:run?` indicate whether to actually run the query. Default is `false`, so only query plan is produced.

    `query` and `inputs` are the same as that of [[q]]

     Return a map looks like this:

          {:planning-time \"1.384 ms\",
           :actual-result-size 2,
           :execution-time \"0.202 ms\",
           :opt-clauses
           [[?e1 :person/city \"Fremont\"]
            [?e :school/city \"Fremont\"]
            [?e1 :person/name ?n1]
            [?e1 :person/age ?a]
            [?e2 :person/age ?a]
            [?e2 :person/school ?e]
            [?e2 :person/name ?n2]],
           :query-graph
           {$
            {?e1
             {:links
              [{:type :val-eq,
                :tgt ?e2,
                :var ?a,
                :attrs {?e1 :person/age, ?e2 :person/age}}],
              :mpath [:bound :person/city],
              :mcount 2,
              :bound #:person{:city {:val \"Fremont\", :count 2}},
              :free
              #:person{:name {:var ?n1, :count 3}, :age {:var ?a, :count 3}}},
             ?e
             {:links [{:type :_ref, :tgt ?e2, :attr :person/school}],
              :mpath [:bound :school/city],
              :mcount 1,
              :bound #:school{:city {:val \"Fremont\", :count 1}}},
             ?e2
             {:links
              [{:type :ref, :tgt ?e, :attr :person/school}
               {:type :val-eq,
                :tgt ?e1,
                :var ?a,
                :attrs {?e1 :person/age, ?e2 :person/age}}],
              :mpath [:free :person/school],
              :mcount 4,
              :free
              #:person{:age {:var ?a, :count 5},
                       :school {:var ?e, :count 4},
                       :name {:var ?n2, :count 5}}}}},
           :plan
           {$
            [({:steps
               [\"Initialize [?e1] by :person/city = Fremont.\"
                \"Merge [?a ?n1] by scanning [:person/age :person/name].\"],
               :cost 6,
               :size 2,
               :actual-size 2}
              {:steps
               [\"Merge ?e2 by equal values of :person/age.\"
                \"Merge [?e ?n2] by scanning [:person/school :person/name].\"],
               :cost 15,
               :size 3,
               :actual-size 4}
              {:steps [\"Merge [?e] by scanning [:school/city].\"],
               :cost 18,
               :size 3,
               :actual-size 4})]},
           :late-clauses [[(not= ?n1 ?n2)]]}


  * `:parsing-time` time spent parsing the query.
  * `:build-time` time spent building the query graph.
  * `:planning-time` time spent planning the query.
  * `:prepare-time` includes all the time spent up to when the plan is generated.
  * `:execution-time` is the time between the generated plan and result return.
  * `:actual-result-size` is the number of tuples generated.
  * `:opt-cluases` includes all the clauses that the optimizer worked on.
  * `:late-clauses` are the clauses that are ignored by the optimizer and are processed afterwards.
  * `:query-graph` is a graph data structure parsed from the query, annotated with `:count`, i.e. estimated number of datoms matching a query clause. The optimizer builds its plan using this graph.
  * `:plan` are grouped by data sources, then by the connected component of the query graph.
      - `:steps` are the descriptions of the processing steps planned.
      - `:cost` is the accumulated estimated cost, which determines the plan.
      - `:size` is the estimated number of resulting tuples for the steps.
      - `:actual-size` is the actual number of resulting tuples after the steps are executed. "}
  explain dq/explain)

;; Index lookups

(def ^{:arglists '([db index]
                   [db index c1]
                   [db index c1 c2]
                   [db index c1 c2 c3]
                   [db index c1 c2 c3 n])
       :doc      "Lookup datoms in specified index of Datalog db. Returns a sequence of datoms (iterator over actual DB index) whose components (e, a, v) match passed arguments.

   Datoms are sorted in index sort order. Possible `index` values are: `:eav` or `:ave`.

   `n` is the number of datoms desired.

   Usage:

       ; find all datoms for entity id == 1 (any attrs and values)
       ; sort by attribute, then value
       (datoms db :eav 1)
       ; => (#datalevin/Datom [1 :friends 2]
       ;     #datalevin/Datom [1 :likes \"fries\"]
       ;     #datalevin/Datom [1 :likes \"pizza\"]
       ;     #datalevin/Datom [1 :name \"Ivan\"])

       ; find all datoms for entity id == 1 and attribute == :likes (any values)
       ; sorted by value
       (datoms db :eav 1 :likes)
       ; => (#datalevin/Datom [1 :likes \"fries\"]
       ;     #datalevin/Datom [1 :likes \"pizza\"])

       ; find all datoms for entity id == 1, attribute == :likes and value == \"pizza\"
       (datoms db :eav 1 :likes \"pizza\")
       ; => (#datalevin/Datom [1 :likes \"pizza\"])

       ; find all datoms that have attribute == `:likes` and value == `\"pizza\"` (any entity id)
       (datoms db :ave :likes \"pizza\")
       ; => (#datalevin/Datom [1 :likes \"pizza\"]
       ;     #datalevin/Datom [2 :likes \"pizza\"])

       ; find all datoms sorted by entity id, then attribute, then value
       (datoms db :eav) ; => (...)

   Useful patterns:

       ; get all values of :db.cardinality/many attribute
       (->> (datoms db :eav eid attr) (map :v))

       ; lookup entity ids by attribute value
       (->> (datoms db :ave attr value) (map :e))

       ; find N entities with lowest attr value (e.g. 10 earliest posts)
       (->> (datoms db :ave attr) (take N))

       ; find N entities with highest attr value (e.g. 10 latest posts)
       (->> (datoms db :ave attr) (reverse) (take N)) "}
  datoms db/-datoms)

(def ^{:arglists '([db e a v])
       :doc      "Datom lookup in Datalog db. Returns a sequence of datoms matching the passed e, a, v components. When any of the components is `nil`, it is considered a wildcard. This function chooses the most efficient index to look up the datoms. The order of the returned datoms depends on the index chosen.

   Usage:

       ; find all datoms for entity id == 1 (any attrs and values)
       ; sort by attribute, then value
       (search-datoms db 1 nil nil)
       ; => (#datalevin/Datom [1 :friends 2]
       ;     #datalevin/Datom [1 :likes \"fries\"]
       ;     #datalevin/Datom [1 :likes \"pizza\"]
       ;     #datalevin/Datom [1 :name \"Ivan\"])

       ; find all datoms for entity id == 1 and attribute == :likes (any values)
       ; sorted by value
       (search-datoms db 1 :likes nil)
       ; => (#datalevin/Datom [1 :likes \"fries\"]
       ;     #datalevin/Datom [1 :likes \"pizza\"])

       ; find all datoms for entity id == 1, attribute == :likes and value == \"pizza\"
       (search-datoms db 1 :likes \"pizza\")
       ; => (#datalevin/Datom [1 :likes \"pizza\"])

       ; find all datoms that have attribute == `:likes` and value == `\"pizza\"` (any entity id)
       (search-datoms db nil :likes \"pizza\")
       ; => (#datalevin/Datom [1 :likes \"pizza\"]
       ;     #datalevin/Datom [2 :likes \"pizza\"]) "}
  search-datoms db/search-datoms)

(def ^{:arglists '([db e a v])
       :doc      "Count datoms in Datalog db that match the passed e, a, v components. When any of the components is `nil`, it is considered a wildcard. This function is more efficient than calling `count` on `search-datoms` results.

   Usage:

       ; count datoms for entity id == 1 (any attrs and values)
       (count-datoms db 1 nil nil)
       ; => 9

       ; count datoms for entity id == 1 and attribute == :likes (any values)
       (count-datoms db 1 :likes nil)
       ; => 4

       ; count datoms for entity id == 1, attribute == :likes and value == \"pizza\"
       (count-datoms db 1 :likes \"pizza\")
       ; => 2

       ; count datoms that have attribute == `:likes` and value == `\"pizza\"` (any entity id)
       (count-datoms db nil :likes \"pizza\")
       ; => 10 "}
  count-datoms db/count-datoms)

(def ^{:arglists '([db a])
       :doc      "Count the number of unique values of an attribute in a Datalog db."}
  cardinality db/-cardinality)

(def ^{:arglists '([db a])
       :doc      "Return the current maximal entity id of a Datalog db"}
  max-eid db/max-eid)

(def ^{:arglists '([db]
                   [db attr])
       :doc      "Collect statistics for an attribute `attr` that are helpful for Datalog query planner.
  When `attr` is not given, collect statistics for all attributes. Return `:done`.

  Datalevin runs this function in the background periodically. "}
  analyze db/analyze)

(def ^{:arglists '([db index]
                   [db index c1]
                   [db index c1 c2]
                   [db index c1 c2 c3]
                   [db index c1 c2 c3 n])
       :doc      "Similar to [[datoms]], but will return datoms starting from specified components.

   If no datom matches passed arguments exactly, iterator will start from first datom that could be considered “greater” in index order.

   If `n` is specified, only up to `n` datoms will be returned.

   Usage:

       (seek-datoms db :eav 1)
       ; => (#datalevin/Datom [1 :friends 2]
       ;     #datalevin/Datom [1 :likes \"fries\"]
       ;     #datalevin/Datom [1 :likes \"pizza\"]
       ;     #datalevin/Datom [1 :name \"Ivan\"]
       ;     #datalevin/Datom [2 :likes \"candy\"]
       ;     #datalevin/Datom [2 :likes \"pie\"]
       ;     #datalevin/Datom [2 :likes \"pizza\"])

       (seek-datoms db :eav 1 :name)
       ; => (#datalevin/Datom [1 :name \"Ivan\"])

       (seek-datoms db :eav 2)
       ; => (#datalevin/Datom [2 :likes \"candy\"]
       ;     #datalevin/Datom [2 :likes \"pie\"]
       ;     #datalevin/Datom [2 :likes \"pizza\"])

       ; no datom [2 :likes \"fish\"], so starts with one immediately following such in index
       (seek-datoms db :eav 2 :likes \"fish\")
       ; => (#datalevin/Datom [2 :likes \"pie\"]
       ;     #datalevin/Datom [2 :likes \"pizza\"])"}
  seek-datoms db/seek-datoms)

(def ^{:arglists '([db index]
                   [db index c1]
                   [db index c1 c2]
                   [db index c1 c2 c3]
                   [db index c1 c2 c3 n])
       :doc      "Same as [[seek-datoms]], but goes backwards."}
  rseek-datoms db/rseek-datoms)

(def ^{:arglists '([db query]
                   [db query opts])
       :doc      "Return datoms that found by the given fulltext search query"}
  fulltext-datoms dbq/fulltext-datoms)

(def ^{:arglists '([db attr start end])
       :doc      "Returns part of `:ave` index between `[_ attr start]` and `[_ attr end]` in AVE sort order.

   Same properties as [[datoms]].

   Usage:

       (index-range db :likes \"a\" \"zzzzzzzzz\")
       ; => (#datalevin/Datom [2 :likes \"candy\"]
       ;     #datalevin/Datom [1 :likes \"fries\"]
       ;     #datalevin/Datom [2 :likes \"pie\"]
       ;     #datalevin/Datom [1 :likes \"pizza\"]
       ;     #datalevin/Datom [2 :likes \"pizza\"])

       (index-range db :likes \"egg\" \"pineapple\")
       ; => (#datalevin/Datom [1 :likes \"fries\"]
       ;     #datalevin/Datom [2 :likes \"pie\"])

   Useful patterns:

       ; find all entities with age in a specific range (inclusive)
       (->> (index-range db :age 18 60) (map :e))"}
  index-range db/-index-range)


;; Conn
(def ^{:arglists '([conn])
       :doc      "Returns `true` if this is an open connection to a local Datalog db, `false`
  otherwise."}
  conn? conn/conn?)

(def ^{:arglists '([db])
       :doc      "Creates a mutable reference to a given Datalog database. See [[create-conn]]."}
  conn-from-db conn/conn-from-db)

(def ^{:arglists '([datoms]
                   [datoms dir]
                   [datoms dir schema]
                   [datoms dir schema opts])
       :doc      "Create a mutable reference to a Datalog database with the given datoms added to it.
  `dir` could be a local directory path or a dtlv connection URI string.

  `opts` map has keys:

   * `:validate-data?`, a boolean, instructing the system to validate data type during transaction. Default is `false`.

   * `:closed-schema?`, a boolean, instructing the system to only allow entity attributes defined in the schema during transaction. Default is `false`.

   * `:auto-entity-time?`, a boolean indicating whether to maintain `:db/created-at` and `:db/updated-at` values for each entity. Default is `false`.

   * `:search-domains`, an option map from domain names to search option maps of those domains, which will be passed to the corresponding full-text search engines. See [[new-search-engine]]

   * `:search-opts` is the default options passed to [[fulltext]] function for the default search domains in case `:search-domains` above is not specified.

   * `:kv-opts`, an option map that will be passed to the underlying kV store

   * `:wal?`, a boolean, enabling WAL mode for the underlying KV store.

   * `:wal-durability-profile`, one of `:strict` or `:relaxed`.

   * `:wal-group-commit`, maximum transactions per durability batch in
 `:relaxed` mode. Default comes from
 `datalevin.constants/*wal-group-commit*`.

   * `:wal-group-commit-ms`, maximum milliseconds per durability batch in
 `:relaxed` mode. Default comes from
 `datalevin.constants/*wal-group-commit-ms*`.

  "}
  conn-from-datoms conn/conn-from-datoms)

(def ^{:arglists '([]
                   [dir]
                   [dir schema]
                   [dir schema opts])
       :doc      "Creates a mutable reference (a “connection”) to a Datalog database at the given
  location and opens the database. Creates the database if it doesn't
  exist yet. Update the schema if one is given. Return the connection.

  `dir` could be a local directory path or a dtlv connection URI string.

  `opts` map may have keys:

   * `:validate-data?`, a boolean, instructing the system to validate data type during transaction. Default is `false`.

   * `:closed-schema?`, a boolean, instructing the system to only allow entity attributes defined in the schema during transaction. Default is `false`.

   * `:auto-entity-time?`, a boolean indicating whether to maintain `:db/created-at` and `:db/updated-at` values for each entity. Default is `false`.

   * `:search-domains`, an option map from domain names to search option maps of those domains, which will be passed to the corresponding full-text search engines. [[new-search-engine]]

   * `:search-opts` is the default options passed to [[fulltext]] function for the default search domains in case `:search-domains` above is not specified.

   * `:kv-opts`, an option map that will be passed to the underlying kV store

   * `:wal?`, a boolean, enabling WAL mode for the underlying KV store.

   * `:wal-durability-profile`, one of `:strict` or `:relaxed`. `:strict` maximizes durability; `:relaxed`
 improves write throughput with a small risk of losing the latest transactions
 on a crash.

   * `:wal-group-commit`, maximum transactions per durability batch in
 `:relaxed` mode. Default comes from
 `datalevin.constants/*wal-group-commit*`.

   * `:wal-group-commit-ms`, maximum milliseconds per durability batch in
 `:relaxed` mode. Default comes from
 `datalevin.constants/*wal-group-commit-ms*`.

   * `:client-opts` is the option map passed to the client if `dir` is a remote URI string.

  For Datalog stores opened through this API, the default for a new local
  store is `:wal? true` with `:wal-durability-profile :strict`.

  Please note that the connection should be managed like a stateful resource.
  Application should hold on to the same connection rather than opening
  multiple connections to the same database in the same process.

  Connections are lightweight in-memory structures (atoms). To access underlying DB, call [[db]] on it.

  See also [[transact!]], [[db]], [[close]], [[get-conn]], and [[open-kv]].


  Usage:


             (create-conn)

             (create-conn \"/tmp/test-create-conn\")

             (create-conn \"/tmp/test-create-conn\" {:likes {:db/cardinality :db.cardinality/many}})

             (create-conn \"dtlv://datalevin:secret@example.host/mydb\" {} {:auto-entity-time? true})"}
  create-conn conn/create-conn)

(def ^{:arglists '([conn])
       :doc      "Close the connection to a Datalog db"}
  close conn/close)

(def ^{:arglists '([conn])
       :doc      "Return true when the underlying Datalog DB is closed or when `conn` is nil or contains nil"}
  closed? conn/closed?)

(def ^{:arglists '([conn tx-data]
                   [conn tx-data tx-meta])
       :doc      "Synchronously applies transaction to the underlying Datalog database of a
  connection.

  Returns a transaction report, a map:

       { :db-before ...
         :db-after  ...
         :tx-data   [...]     ; plain datoms that were added/retracted from db-before
         :tempids   {...}     ; map of tempid from tx-data => assigned entid in db-after
         :tx-meta   tx-meta } ; the exact value you passed as `tx-meta`

  Note! `conn` will be updated in-place and is not returned from [[transact!]].

  Usage:

      ; add a single datom to an existing entity (1)
      (transact! conn [[:db/add 1 :name \"Ivan\"]])

      ; retract a single datom
      (transact! conn [[:db/retract 1 :name \"Ivan\"]])

      ; retract single entity attribute
      (transact! conn [[:db.fn/retractAttribute 1 :name]])

      ; ... or equivalently (since Datomic changed its API to support this):
      (transact! conn [[:db/retract 1 :name]])

      ; retract all entity attributes (effectively deletes entity)
      (transact! conn [[:db.fn/retractEntity 1]])

      ; create a new entity (`-1`, as any other negative value, is a tempid
      ; that will be replaced with Datalevin to a next unused eid)
      (transact! conn [[:db/add -1 :name \"Ivan\"]])

      ; check assigned id (here `*1` is a result returned from previous `transact!` call)
      (def report *1)
      (:tempids report) ; => {-1 296}

      ; check actual datoms inserted
      (:tx-data report) ; => [#datalevin/Datom [296 :name \"Ivan\"]]

      ; tempid can also be a string
      (transact! conn [[:db/add \"ivan\" :name \"Ivan\"]])
      (:tempids *1) ; => {\"ivan\" 297}

      ; reference another entity (must exist)
      (transact! conn [[:db/add -1 :friend 296]])

      ; create an entity and set multiple attributes (in a single transaction
      ; equal tempids will be replaced with the same yet unused yet entid)
      (transact! conn [[:db/add -1 :name \"Ivan\"]
                       [:db/add -1 :likes \"fries\"]
                       [:db/add -1 :likes \"pizza\"]
                       [:db/add -1 :friend 296]])

      ; create an entity and set multiple attributes (alternative map form)
      (transact! conn [{:db/id  -1
                        :name   \"Ivan\"
                        :likes  [\"fries\" \"pizza\"]
                        :friend 296}])

      ; update an entity (alternative map form). Can’t retract attributes in
      ; map form. For cardinality many attrs, value (fish in this example)
      ; will be added to the list of existing values
      (transact! conn [{:db/id  296
                        :name   \"Oleg\"
                        :likes  [\"fish\"]}])

      ; ref attributes can be specified as nested map, that will create nested entity as well
      (transact! conn [{:db/id  -1
                        :name   \"Oleg\"
                        :friend {:db/id -2
                                 :name \"Sergey\"}}])

      ; reverse attribute name can be used if you want created entity to become
      ; a value in another entity reference
      (transact! conn [{:db/id  -1
                        :name   \"Oleg\"
                        :_friend 296}])
      ; equivalent to
      (transact! conn [{:db/id  -1, :name   \"Oleg\"}
                       {:db/id 296, :friend -1}])
      ; equivalent to
      (transact! conn [[:db/add  -1 :name   \"Oleg\"]
                       [:db/add 296 :friend -1]])"}
  transact! conn/transact!)

(def ^{:arglists '([conn db]
                   [conn db tx-meta])
       :doc      "Forces underlying `conn` value to become a Datalog `db`. Will generate a tx-report that will remove everything from old value and insert everything from the new one."}
  reset-conn! conn/reset-conn!)

(def ^{:arglists '([conn callback]
                   [conn key callback])
       :doc      "Listen for changes on the given connection to a Datalog db. Whenever a transaction is applied
  to the database via [[transact!]], the callback is called with the transaction
  report. `key` is any opaque unique value.

  Idempotent. Calling [[listen!]] with the same key twice will override old
  callback with the new value.

   Returns the key under which this listener is registered. See also [[unlisten!]]."}
  listen! conn/listen!)

(def ^{:arglists '([conn key])
       :doc      "Removes registered listener from connection. See also [[listen!]]."}
  unlisten! conn/unlisten!)

(def ^{:arglists '([conn])
       :doc      "Returns the underlying Datalog database object from a connection. Note that Datalevin does not have \"db as a value\" feature, the returned object is NOT a database value, but a reference to the database object. "}
  db conn/db)

(def ^{:arglists '([conn])
       :doc      "Return the option map of the Datalog DB"}
  opts conn/opts)

(def ^{:arglists '([conn])
       :doc      "Return the schema of Datalog DB"}
  schema conn/schema)

(def ^{:arglists '([conn schema-update]
                   [conn schema-update del-attrs]
                   [conn schema-update del-attrs rename-map])
       :doc      "Update the schema of an open connection to a Datalog db.

  * `schema-update` is a map from attribute keywords to maps of corresponding
  properties.

  * `del-attrs` is a set of attributes to be removed from the schema, if there is
  no datoms associated with them, otherwise an exception will be thrown.

  * `rename-map` is a map of old attributes to new attributes, for renaming
  attributes

  Return the updated schema.

  Example:

        (update-schema conn {:new/attr {:db/valueType :db.type/string}})
        (update-schema conn {:new/attr {:db/valueType :db.type/string}}
                            #{:old/attr1 :old/attr2})
        (update-schema conn nil nil {:old/attr :new/attr}) "}
  update-schema conn/update-schema)

(def ^{:arglists '([dir] [dir schema] [dir schema opts])
       :doc      "Obtain an open connection to a Datalog database. `dir` could be a local directory path or a dtlv connection URI string. Create the database if it does not exist. Reuse the same connection if a connection to the same database already exists. Open the database if it is closed. Return the connection.

  `opts` accepts the same WAL settings as [[create-conn]], including `:wal?`
  and `:wal-durability-profile`.

  See also [[create-conn]] and [[with-conn]]"}
  get-conn conn/get-conn)

(def ^{:arglists '([conn tx-data]
                   [conn tx-data tx-meta]
                   [conn tx-data tx-meta callback])
       :doc      "Datalog transaction that returns a future immediately. The future will
  eventually contain the transaction report when this asynchronous transaction
  successfully commits, otherwise, an exception will be thrown when the future
  is deref'ed.

  Use an adaptive batch transaction algorithm that adjusts batch size
  according to workload: the higher the load, the larger the batch size.

  The 4-arity version of the function takes a `callback` function that will
  be called when the transaction commits, which takes the transaction result
  (possibly an exception) as the single argument. Babashka pod only supports
  this version as callback is required for async pod function.

  This function has higher throughput than [[transact!]] in high write rate use
  cases."}
  transact-async conn/transact-async)

(def ^{:arglists '([conn tx-data] [conn tx-data tx-meta])
       :doc      "Datalog transaction that returns an already realized future that contains
  the transaction report when the transaction succeeds, otherwise an exception
  will be thrown.

  It is the same as [[transact-async]], but will block until the future is
  realized, i.e. when the transaction commits.

  One use of this function is to end a consecutive sequence of `transact-async`
  calls, and when this function returns, it indicates that all those previous
  async transactions are also committed."}
  transact conn/transact)
;; Datomic compatibility layer

(def ^{:arglists '([part]
                   [part x])
       :doc      "allocates and returns an unique temporary id (a negative integer). ignores `part`. returns `x` if it is specified.

   exists for datomic api compatibility. prefer using negative integers directly if possible."}
  tempid u/tempid)

(def ^{:arglists '([_db
                    tempids tempid])
       :doc      "Does a lookup in tempids map, returning an entity id that tempid was resolved to.

   Exists for Datomic API compatibility. Prefer using map lookup directly if possible."}
  resolve-tempid u/resolve-tempid)

(def ^{:arglists '([]
                   [msec])
       :doc      "Generates a UUID that grow with time."}
  squuid u/squuid)

(def ^{:arglists '([uuid])
       :doc      "Returns time that was used in [[squuid]] call, in milliseconds,
  rounded to the closest second."}
  squuid-time-millis u/squuid-time-millis)

;; -------------------------------------

;; key value store API

(def ^{:arglists '([kv])
       :doc      "Key of a key value pair"}
  k l/k)

(def ^{:arglists '([kv])
       :doc      "Value of a key value pair"}
  v l/v)

(def ^{:arglists '([dir]
                   [dir opts])
       :doc      "Open a LMDB key-value database, return the connection.

  `dir` is a directory path or a dtlv connection URI string.

  `opts` is an option map that may have the following keys:

  * `:mapsize` is the initial size of the database, in MiB. Default is 100.
   This will be automatically expanded as needed, albeit with some slow down
   when the expansion happens.
  * `:max-readers` specifies the maximal number of concurrent readers
   allowed for the db file. Default is 512.
  * `:max-dbs` specifies the maximal number of sub-databases (DBIs) allowed
   for the db file. Default is 128. It may induce slowness if too big a
   number of DBIs are created, as a linear scan is used to look up a DBI.
  * `:flags` is a set of keywords corresponding to LMDB environment flags,
   e.g. `:rdonly-env`, `:nosubdir`, and so on. See [[set-env-flags]].
  * `:temp?` a boolean, indicating if this db is temporary, if so, the file
   will be deleted on JVM exit.
  * `:inmemory?` a boolean, indicating if this db is in-memory only, with no
   data persisted to disk. `dir` may be `nil`, in which case a unique temporary
   path is generated automatically. If `dir` is provided, it is used as the
   environment path and will be created if it does not exist. Passing `nil` as
   `dir` also implies in-memory mode without needing to set this option.
  * `:wal?` is a boolean, enabling WAL mode for direct KV writes. Default is
   `false` for `open-kv`.
  * `:wal-durability-profile` is one of `:strict` or `:relaxed`. `:strict` waits for durable WAL
   acknowledgement per write transaction; `:relaxed` batches durability for
   higher throughput with a small crash window.
  * `:wal-group-commit` sets the max transactions per durability batch in
   `:relaxed` mode. Default comes from
   `datalevin.constants/*wal-group-commit*`.
  * `:wal-group-commit-ms` sets the max milliseconds per durability batch in
   `:relaxed` mode. Default comes from
   `datalevin.constants/*wal-group-commit-ms*`.
  * `:client-opts` is the option map passed to the client if `dir` is a
   remote server URI string.
  * `:spill-opts` is the option map that controls the spill-to-disk behavior
   for `get-range` and `range-filter` functions, which may have the following
   keys:
      - `:spill-threshold`, memory pressure in percentage of JVM `-Xmx`
        (default 80), above which spill-to-disk will be triggered.
      - `:spill-root`, a file directory, in which the spilled data is written
        (default is the system temporary directory).


  Please note:

  > LMDB uses POSIX locks on files, and these locks have issues if one process
  > opens a file multiple times. Because of this, do not mdb_env_open() a file
  > multiple times from a single process. Instead, share the LMDB environment
  > that has opened the file across all threads. Otherwise, if a single process
  > opens the same environment multiple times, closing it once will remove all
  > the locks held on it, and the other instances will be vulnerable to
  > corruption from other processes.'

  Therefore, a LMDB connection should be managed as a stateful resource.
  Multiple connections to the same DB in the same process are not recommended.
  The recommendation is to use a mutable state management library, for
  example, in Clojure, use [component](https://github.com/stuartsierra/component),
  [mount](https://github.com/tolitius/mount),
  [integrant](https://github.com/weavejester/integrant), or something similar
  to hold on to and manage the connection. "}
  open-kv conn/open-kv)

(def ^{:arglists '([db])
       :doc      "Close this key-value store"}
  close-kv i/close-kv)

(def ^{:arglists '([db])
       :doc      "Return true if this key-value store is closed"}
  closed-kv? i/closed-kv?)

(def ^{:arglists '([db])
       :doc      "Return the path or URI string of the key-value store"}
  dir i/env-dir)

(def ^{:arglists '([db dbi-name]
                   [db dbi-name opts])
       :doc      "Open a named DBI (i.e. sub-db) in the key-value store. `opts` is an option map that may have the following keys:

      * `:validate-data?`, a boolean, instructing the system to validate data type during transaction. Default is `false`.

      * `:closed-schema?`, a boolean, instructing the system to only allow entity attributes defined in the schema during transaction. Default is `false`.

      * `:key-size` is the max size of the key in bytes, cannot be greater than 511, default is 511.

      * `:val-size` is the default size of the value in bytes, Datalevin will automatically increase the size if a larger value is transacted.

      * `:flags` is a set of LMDB Dbi flag keywords, may include `:reversekey`, `:dupsort`, `integerkey`, `dupfixed`, `integerdup`, `reversedup`, or `create`, default is `#{:create}`, see [LMDB documentation](http://www.lmdb.tech/doc/group__mdb__dbi__open.html)."}
  open-dbi i/open-dbi)

(def ^{:arglists '([db dbi-name])
       :doc      "Clear data in the DBI (i.e sub-database) of the key-value store, but leave it open"}
  clear-dbi i/clear-dbi)

(def ^{:arglists '([conn])
       :doc      "Close the Datalog database, then clear all data, including schema."}
  clear conn/clear)

(def ^{:arglists '([db dbi-name])
       :doc      "Clear data in the DBI (i.e. sub-database) of the key-value store, then delete it"}
  drop-dbi i/drop-dbi)

(def ^{:arglists '([db])
       :doc      "List the names of the sub-databases in the key-value store"}
  list-dbis i/list-dbis)

(def ^{:arglists '([db dest]
                   [db dest compact?])
       :doc      "Copy a Datalog or a key-value database to a destination directory path,
  optionally compact while copying, default not compact. "}
  copy dump/copy)

(def ^{:arglists '([db]
                   [db dbi-name])
       :doc      "Return the statitics of the unnamed top level database or a named DBI (i.e. sub-database) of the key-value store as a map:
  * `:psize` is the size of database page
  * `:depth` is the depth of the B-tree
  * `:branch-pages` is the number of internal pages
  * `:leaf-pages` is the number of leaf pages
  * `:overflow-pages` is the number of overflow-pages
  * `:entries` is the number of data entries"}
  stat i/stat)

(def ^{:arglists '([db dbi-name])
       :doc      "Get the number of data entries in a DBI (i.e. sub-db) of the key-value store"}
  entries i/entries)

(def ^{:arglists '([db txs]
                   [db dbi-name txs]
                   [db dbi-name txs k-type]
                   [db dbi-name txs k-type v-type])
       :doc      "Synchronously update key-value DB, insert or delete key value pairs.

  `txs` is a seq of Clojure vectors, `[op dbi-name k v k-type v-type flags]`
  when `op` is `:put`, for insertion of a key value pair `k` and `v`;
  or `[op dbi-name k k-type]` when `op` is `:del`, for deletion of key `k`.

  `dbi-name` is the name of the DBI (i.e sub-db) to be updated, a string.

  `k-type`, `v-type` and `flags` are optional.

  `k-type` indicates the data type of `k`, and `v-type` indicates the data type
  of `v`. The allowed data types are described in [[put-buffer]].

  When all vectors in the `txs` are updating the same DBI, it can be pulled out
  of `txs` and passed in as `dbi-name` argument. Similarly, `k-type` and `v-type` can be
  pulled out and passed in as arguments if the whole `txs` have the same data types.

  `:flags` is a set of LMDB Write flag keywords, may include `:nooverwrite`, `:nodupdata`, `:current`, `:reserve`, `:append`, `:appenddup`, `:multiple`, see [LMDB documentation](http://www.lmdb.tech/doc/group__mdb__put.html).
       Pass in `:append` when the data is already sorted to gain better write performance.

  Example:

          (transact-kv
            lmdb
            [ [:put \"a\" 1 2]
              [:put \"a\" 'a 1]
              [:put \"a\" 5 {}]
              [:put \"a\" :annunaki/enki true :attr :data]
              [:put \"a\" :datalevin [\"hello\" \"world\"]]
              [:put \"a\" 42 (d/datom 1 :a/b {:id 4}) :long :datom]
              [:put \"a\" (byte 0x01) #{1 2} :byte :data]
              [:put \"a\" (byte-array [0x41 0x42]) :bk :bytes :data]
              [:put \"a\" [-1 -235254457N] 5]
              [:put \"a\" :a 4]
              [:put \"a\" :bv (byte-array [0x41 0x42 0x43]) :data :bytes]
              [:put \"a\" :long 1 :data :long]
              [:put \"a\" 2 3 :long :long]
              [:del \"a\" 1]
              [:del \"a\" :non-exist] ])

See also: [[open-kv]], [[sync]]"}
  transact-kv i/transact-kv)

(def ^{:arglists '([db])
       :doc      "Rollback writes of the transaction from inside
  [[with-transaction-kv]]."}
  abort-transact-kv i/abort-transact-kv)

(def ^{:arglists '([this txs]
                   [this dbi-name txs]
                   [this dbi-name txs k-type]
                   [this dbi-name txs k-type v-type]
                   [this dbi-name txs k-type v-type callback])
       :doc      "Asynchronously update key-value DB, insert or delete key value pairs,
  return a future. The future eventually contains `:transacted` if transaction
  succeeds, otherwise an exception will be thrown when the future is deref'ed.

  The asynchronous transactions are batched. Batch size is adaptive to the load,
  so the write throughput is higher than `transact-kv`.

  The 6-arity version of the function takes a `callback` function that will
  be called when the transaction commits, which takes the transaction result
  (possibly an exception) as the single argument. Babashka pod only supports
  this version as callback is required for async pod function.

  See also: [[transact-kv]]"}
  transact-kv-async l/transact-kv-async)

(def ^{:arglists '([db ks on-off])
       :doc      "Set LMDB environment flags. `ks` is a set of keywords, when `on-off` is truthy, these flags are set, otherwise, they are cleared. These are the keywords:

         * `:fixedmap`, mmap at a fixed address (experimental)

         * `:nosubdir`, no environment directory, DB is just a file

         * `:nosync`, don't fsync after commit

         * `:rdonly-env`, read only DB

         * `:nometasync`, don't fsync metapage after commit

         * `:writemap`, use writable mmap

         * `:mapasync`, use asynchronous msync when `:writemap` is used

         * `:notls`, tie reader locktable slots to txn objects instead of to threads

         * `:nolock`, don't do any locking, caller must manage their own locks

         * `:nordahead`, don't do readahead (no effect on Windows), set in Datalevin by default

         * `:nomeminit`, don't initialize malloc'd memory before writing to datafile "}
  set-env-flags i/set-env-flags)

(def ^{:arglists '([db])
       :doc      "Get LMDB environment flags that are currently in effect. Return a
set of keywords. See [[set-env-flags]] for their meanings."}
  get-env-flags i/get-env-flags)

(def ^{:arglists '([db])
       :doc      "Force a synchronous flush to disk. Useful when non-default flags for write are included in the `:flags` option when opening the KV store, such as `:nosync`, `:mapasync`, etc. See [[open-kv]]"}
  sync i/sync)

(def ^{:arglists '([db])
       :doc      "Return WAL watermarks and runtime sync state for a KV store opened with `:wal? true`."}
  txlog-watermarks kv/txlog-watermarks)

(def ^{:arglists '([db from-lsn]
                   [db from-lsn upto-lsn])
       :doc      "Read committed WAL records from `from-lsn` (inclusive), optionally capped at `upto-lsn` (inclusive)."}
  open-tx-log kv/open-tx-log)

(def ^{:arglists '([db])
       :doc      "Create/rotate LMDB snapshots (`current`/`previous`) and update WAL snapshot floor bookkeeping."}
  create-snapshot! kv/create-snapshot!)

(def ^{:arglists '([db])
       :doc      "List available LMDB snapshots and metadata."}
  list-snapshots kv/list-snapshots)

(def ^{:arglists '([db]
                   [db retain-floor-lsn])
       :doc      "Run WAL segment GC. Optional `retain-floor-lsn` keeps records from that floor and newer."}
  gc-txlog-segments! kv/gc-txlog-segments!)

(def ^{:arglists '([db dbi-name k]
                   [db dbi-name k k-type]
                   [db dbi-name k k-type v-type]
                   [db dbi-name k k-type v-type ignore-key?])
       :doc      "Get kv pair of the specified key `k` in the key-value store.

  `k-type` and `v-type` are data types of `k` and `v`, respectively.
  The allowed data types are described in [[read-buffer]].

  If `ignore-key?` is `true` (default), only return the value,
  otherwise return `[k v]`, where `v` is the value

  Examples:

        (get-value lmdb \"a\" 1)
        ;;==> 2

        ;; specify data types
        (get-value lmdb \"a\" :annunaki/enki :attr :data)
        ;;==> true

        ;; return key value pair
        (get-value lmdb \"a\" 1 :data :data false)
        ;;==> [1 2]

        ;; key doesn't exist
        (gkvet-value lmdb \"a\" 2)
        ;;==> nil "}
  get-value i/get-value)

(def ^{:arglists '([db dbi-name k]
                   [db dbi-name k k-type])
       :doc      "Get the rank (0-based position) of the key `k` in the sorted key order
  of the key-value store.

  `k-type` is the data type of `k`. The allowed data types are described
  in [[read-buffer]]. Default is `:data`.

  Returns nil if the key does not exist.

  Examples:

        (get-rank lmdb \"a\" 1)
        ;;==> 0

        ;; specify data type
        (get-rank lmdb \"a\" :annunaki/enki :attr)
        ;;==> 5

        ;; key doesn't exist
        (get-rank lmdb \"a\" 999)
        ;;==> nil "}
  get-rank i/get-rank)

(def ^{:arglists '([db dbi-name rank]
                   [db dbi-name rank k-type]
                   [db dbi-name rank k-type v-type]
                   [db dbi-name rank k-type v-type ignore-key?])
       :doc      "Get the key-value pair at the given rank (0-based position) in sorted
  key order of the key-value store.

  `rank` is the 0-based position in the sorted key order.

  `k-type` and `v-type` are data types of the key and value, respectively.
  The allowed data types are described in [[read-buffer]].

  If `ignore-key?` is `true` (default), only return the value,
  otherwise return `[k v]`.

  Returns nil if the rank is out of bounds.

  Examples:

        (get-by-rank lmdb \"a\" 0)
        ;;==> returns value of first key

        ;; specify data types
        (get-by-rank lmdb \"a\" 0 :long :string)
        ;;==> \"first-value\"

        ;; return key value pair
        (get-by-rank lmdb \"a\" 0 :long :string false)
        ;;==> [1 \"first-value\"]

        ;; rank out of bounds
        (get-by-rank lmdb \"a\" 999999)
        ;;==> nil "}
  get-by-rank i/get-by-rank)

(def ^{:arglists '([db dbi-name n]
                   [db dbi-name n k-type]
                   [db dbi-name n k-type v-type]
                   [db dbi-name n k-type v-type ignore-key?])
       :doc      "Return n random samples of key-value pairs from the key-value store.

  `n` is the number of samples to return.

  `k-type` and `v-type` are data types of the key and value, respectively.
  The allowed data types are described in [[read-buffer]].

  If `ignore-key?` is `true` (default), only return values,
  otherwise return `[k v]` pairs.

  Returns nil if n is greater than the number of entries.

  Examples:

        (sample-kv lmdb \"a\" 3)
        ;;==> [\"val1\" \"val2\" \"val3\"]

        ;; return key value pairs
        (sample-kv lmdb \"a\" 3 :long :string false)
        ;;==> [[1 \"val1\"] [5 \"val2\"] [10 \"val3\"]] "}
  sample-kv i/sample-kv)

(def ^{:arglists '([db dbi-name k-range]
                   [db dbi-name k-range k-type]
                   [db dbi-name k-range k-type v-type]
                   [db dbi-name k-range k-type v-type ignore-key?])
       :doc      "Return the first kv pair in the specified key range in the key-value store;

     `k-range` is a vector `[range-type k1 k2]`, `range-type` can be one of
     `:all`, `:at-least`, `:at-most`, `:closed`, `:closed-open`, `:greater-than`,
     `:less-than`, `:open`, `:open-closed`, plus backward variants that put a
     `-back` suffix to each of the above, e.g. `:all-back`;

    `k-type` and `v-type` are data types of `k` and `v`, respectively.
     The allowed data types are described in [[read-buffer]].

     Only the value will be returned if `ignore-key?` is `true`
     (default if `false`);
     If value is to be ignored, put `:ignore` as `v-type`

     If both key and value are ignored, return true if found an entry, otherwise
     return `nil`.

     Examples:


              (get-first lmdb \"c\" [:all] :long :long)
              ;;==> [0 1]

              ;; ignore value
              (get-first lmdb \"c\" [:all-back] :long :ignore)
              ;;==> [999 nil]

              ;; ignore key
              (get-first lmdb \"a\" [:greater-than 9] :long :data true)
              ;;==> {:some :data}

              ;; ignore both, this is like testing if the range is empty
              (get-first lmdb \"a\" [:greater-than 5] :long :ignore true)
              ;;==> true"}
  get-first i/get-first)

(def ^{:arglists '([db dbi-name n k-range]
                   [db dbi-name n k-range k-type]
                   [db dbi-name n k-range k-type v-type]
                   [db dbi-name n k-range k-type v-type ignore-key?])
       :doc      "Return the first `n` kv pairs in the specified key range in the key-value store;

     `n` is a positive natural number.

     `k-range` is a vector `[range-type k1 k2]`, `range-type` can be one of
     `:all`, `:at-least`, `:at-most`, `:closed`, `:closed-open`, `:greater-than`,
     `:less-than`, `:open`, `:open-closed`, plus backward variants that put a
     `-back` suffix to each of the above, e.g. `:all-back`;

    `k-type` and `v-type` are data types of `k` and `v`, respectively.
     The allowed data types are described in [[read-buffer]].

     Only the value will be returned if `ignore-key?` is `true`
     (default if `false`);
     If value is to be ignored, put `:ignore` as `v-type`

     See also [[get-first]]

     Examples:


              (get-first-n lmdb \"c\" 2 [:all] :long :long)
              ;;==> [[0 1] [2 9]]"}
  get-first-n i/get-first-n)

(def ^{:arglists '([db dbi-name k-range]
                   [db dbi-name k-range k-type]
                   [db dbi-name k-range k-type v-type]
                   [db dbi-name k-range k-type v-type ignore-key?])
       :doc      "Return a seq of kv pairs in the specified key range in the key-value store.

This function is eager and attempts to load all data in range into memory. When the memory pressure is high, the remaining data is spilled on to a temporary disk file. The spill-to-disk mechanism is controlled by `:spill-opts` map passed to [[open-kv]]. See [[range-seq]] for a lazy version of this function.

`k-range` is a vector `[range-type k1 k2]`, `range-type` can be one of `:all`, `:at-least`, `:at-most`, `:closed`, `:closed-open`, `:greater-than`, `:less-than`, `:open`, `:open-closed`, plus backward variants that put a `-back` suffix to each of the above, e.g. `:all-back`.

`k-type` and `v-type` are data types of `k` and `v`, respectively. The allowed data types are described in [[read-buffer]].

Only the value will be returned if `ignore-key?` is `true`, default is `false`.

If value is to be ignored, put `:ignore` as `v-type`.


     Examples:


              (get-range lmdb \"c\" [:at-least 9] :long :long)
              ;;==> [[10 11] [11 15] [13 14]]

              ;; ignore value
              (get-range lmdb \"c\" [:all-back] :long :ignore)
              ;;==> [[999 nil] [998 nil]]

              ;; ignore keys, only return values
              (get-range lmdb \"a\" [:closed 9 11] :long :long true)
              ;;==> [10 11 12]

              ;; out of range
              (get-range lmdb \"c\" [:greater-than 1500] :long :ignore)
              ;;==> [] "}
  get-range i/get-range)

(def ^{:arglists '([db dbi-name k-range]
                   [db dbi-name k-range k-type])
       :doc      "Returns a seq of keys in the specified key range in the key-value store. This does not read the values, so it is faster than [[get-range]].

This function is eager and attempts to load all data in range into memory. When the memory pressure is high, the remaining data is spilled on to a temporary disk file. The spill-to-disk mechanism is controlled by `:spill-opts` map passed to [[open-kv]].

`k-range` is a vector `[range-type k1 k2]`, `range-type` can be one of `:all`, `:at-least`, `:at-most`, `:closed`, `:closed-open`, `:greater-than`, `:less-than`, `:open`, `:open-closed`, plus backward variants that put a `-back` suffix to each of the above, e.g. `:all-back`.

`k-type` is data type of `k`. The allowed data types are described in [[read-buffer]].

     Examples:

              (key-range lmdb \"c\" [:greater-than 9] :long)
              ;;==> [11 15 14]"}
  key-range i/key-range)

(def ^{:arglists '([db dbi-name k-range]
                   [db dbi-name k-range k-type]
                   [db dbi-name k-range k-type cap])
       :doc      "Returns the number of keys in the specified key range in the key-value store.

`k-range` is a vector `[range-type k1 k2]`, `range-type` can be one of `:all`, `:at-least`, `:at-most`, `:closed`, `:closed-open`, `:greater-than`, `:less-than`, `:open`, `:open-closed`, plus backward variants that put a `-back` suffix to each of the above, e.g. `:all-back`.

`k-type` is data type of key. The allowed data types are described in [[read-buffer]].

`cap` is a number, over which the count will stop.

     Examples:

              (key-range lmdb \"c\" [:greater-than 9] :long)
              ;;==> 1002"}
  key-range-count i/key-range-count)

(def ^{:arglists '([db dbi-name k-range k-type]
                   [db dbi-name k-range k-type cap])
       :doc      "Returns the number of list items for the specified key range in the key-value store. This function is only applicable for dbi opened with `open-list-dbi`.

`k-range` is a vector `[range-type k1 k2]`, `range-type` can be one of `:all`, `:at-least`, `:at-most`, `:closed`, `:closed-open`, `:greater-than`, `:less-than`, `:open`, `:open-closed`, plus backward variants that put a `-back` suffix to each of the above, e.g. `:all-back`.

`k-type` is data type of key. The allowed data types are described in [[read-buffer]].

`cap` is a number, over which the count will stop."}
  key-range-list-count i/key-range-list-count)

(def ^{:arglists '([db dbi-name visitor k-range]
                   [db dbi-name visitor k-range k-type]
                   [db dbi-name visitor k-range k-type raw-pred?])
       :doc      "Call `visitor` function on each key in the specified key range, presumably for side effects. Return `nil`. If `raw-pred?` is true (default), `visitor` takes a single `ByteBuffer` as the key, otherwise, takes the decoded value of the key.

      If `visitor` function returns a special value `:datalevin/terminate-visit`, the visit will stop immediately.

      For client/server usage, [[datalevin.interpret/inter-fn]] should be used to define the `visitor` function. For babashka pod usage, `defpodfn` should be used.

    `k-type` indicates data type of `k` and the allowed data types are described
    in [[read-buffer]].

     `k-range` is a vector `[range-type k1 k2]`, `range-type` can be one of
     `:all`, `:at-least`, `:at-most`, `:closed`, `:closed-open`, `:greater-than`,
     `:less-than`, `:open`, `:open-closed`, plus backward variants that put a
     `-back` suffix to each of the above, e.g. `:all-back`;"}
  visit-key-range i/visit-key-range)

(def ^{:arglists '([db dbi-name k-range]
                   [db dbi-name k-range k-type]
                   [db dbi-name k-range k-type v-type]
                   [db dbi-name k-range k-type v-type ignore-key?]
                   [db dbi-name k-range k-type v-type ignore-key? opts])
       :doc      "Return a seq of kv pairs in the specified key range in the key-value store. This function is similar to `get-range`, but the result is lazy, as it loads the data items in batches into memory. `:batch-size` in `opts` controls the batch size (default 100).

The returned data structure implements `Seqable` and `IReduceInit`. It represents only one pass over the data range, and `seq` function needs to be called to obtain a persistent collection.

Be aware that the returned structure holds an open read transaction. It implements `AutoCloseable`, and `close` should be invoked on it after done with data access, otherwise an open read transaction may blow up the database size and return stale data. It is strongly recommended to use it in `with-open`.

`k-range` is a vector `[range-type k1 k2]`, `range-type` can be one of `:all`, `:at-least`, `:at-most`, `:closed`, `:closed-open`, `:greater-than`, `:less-than`, `:open`, `:open-closed`, plus backward variants that put a `-back` suffix to each of the above, e.g. `:all-back`;

`k-type` and `v-type` are data types of `k` and `v`, respectively.

The allowed data types are described in [[read-buffer]].

Only the value will be returned if `ignore-key?` is `true`, default is `false`;

If value is to be ignored, put `:ignore` as `v-type`

See [[get-range]] for usage of the augments.

     Examples:

              (with-open [^AutoCloseable range (range-seq db \"c\" [:at-least 9] :long)]
                (doseq [item range]
                  ;; do processing on each item
                  ))"}
  range-seq i/range-seq)

(def ^{:arglists '([db dbi-name k-range]
                   [db dbi-name k-range k-type])
       :doc      "Return the number of kv pairs in the specified key range in the key-value store, does not process the kv pairs.

     `k-range` is a vector `[range-type k1 k2]`, `range-type` can be one of
     `:all`, `:at-least`, `:at-most`, `:closed`, `:closed-open`, `:greater-than`,
     `:less-than`, `:open`, `:open-closed`, plus backward variants that put a
     `-back` suffix to each of the above, e.g. `:all-back`;

    `k-type` and `v-type` are data types of `k` and `v`, respectively.
     The allowed data types are described in [[read-buffer]].

     Examples:


              (range-count lmdb \"c\" [:at-least 9] :long)
              ;;==> 10 "}
  range-count i/range-count)

(def ^{:arglists '([db dbi-name pred k-range]
                   [db dbi-name pred k-range k-type]
                   [db dbi-name pred k-range k-type v-type]
                   [db dbi-name pred k-range k-type v-type ignore-key?]
                   [db dbi-name pred k-range k-type v-type ignore-key? raw-pred?])
       :doc
       "Return the first kv pair that has logical true value of calling `pred` in
        the key-value store, where `pred` is a function. When `raw-pred?` is true (default), `pred` takes an `IKV`
        fetched from the store, with both key and value fields being a
        `ByteBuffer`, otherwise, it takes already decoded `k` and `v`.

      To access store on a server, [[interpret.inter-fn]] should be used to define the `pred`.

     `k-range` is a vector `[range-type k1 k2]`, `range-type` can be one of
     `:all`, `:at-least`, `:at-most`, `:closed`, `:closed-open`, `:greater-than`,
     `:less-than`, `:open`, `:open-closed`, plus backward variants that put a
     `-back` suffix to each of the above, e.g. `:all-back`;

    `k-type` and `v-type` are data types of `k` and `v`, respectively.
     The allowed data types are described in [[read-buffer]].

     Only the value will be returned if `ignore-key?` is `true`;
     If value is to be ignored, put `:ignore` as `v-type`

     Examples:

              (require '[datalevin.interpret :as i])

              (def pred (i/inter-fn [kv]
                         (let [^long lk (read-buffer (k kv) :long)]
                          (> lk 15)))

              (get-some lmdb \"c\" pred [:less-than 20] :long :long)
              ;;==> [16 2]

              ;; ignore key
              (get-some lmdb \"c\" pred [:greater-than 9] :long :data true)
              ;;==> 16 "}
  get-some i/get-some)

(def ^{:arglists '([db dbi-name pred k-range]
                   [db dbi-name pred k-range k-type]
                   [db dbi-name pred k-range k-type v-type]
                   [db dbi-name pred k-range k-type v-type ignore-key?]
                   [db dbi-name pred k-range k-type v-type ignore-key? raw-pred?])
       :doc      "Return a seq of kv pair in the specified key range in the key-value store, for only those
     return true value for `pred` call, where `pred` is a function. When `raw-pred?` is true (default), the call is `(pred kv)`, where `kv`
     is a raw `IKV` object, with both key and value fields being a `ByteBuffer`; otherwise, the call is `(pred k v)`, where `k` and `v` are already decoded key and value.

This function is eager and attempts to load all matching data in range into memory. When the memory pressure is high, the remaining data is spilled on to a temporary disk file. The spill-to-disk mechanism is controlled by `:spill-opts` map passed to [[open-kv]].

To access store on a server, [[interpret.inter-fn]] should be used to define the `pred`.

`k-range` is a vector `[range-type k1 k2]`, `range-type` can be one of `:all`, `:at-least`, `:at-most`, `:closed`, `:closed-open`, `:greater-than`, `:less-than`, `:open`, `:open-closed`, plus backward variants that put a `-back` suffix to each of the above, e.g. `:all-back`;

`k-type` and `v-type` are data types of `k` and `v`, respectively. The allowed data types are described in [[read-buffer]].

Only the value will be returned if `ignore-key?` is `true`;

If value is to be ignored, put `:ignore` as `v-type`

     Examples:

              (require '[datalevin.interpret :as i])

              (def pred (i/inter-fn [kv]
                         (let [^long lk (read-buffer (k kv) :long)]
                          (> lk 15)))

              (range-filter lmdb \"a\" pred [:less-than 20] :long :long)
              ;;==> [[16 2] [17 3]]

              ;; ignore key
              (range-filter lmdb \"a\" pred [:greater-than 9] :long :data true)
              ;;==> [16 17] "}
  range-filter i/range-filter)

(def ^{:arglists '([db dbi-name pred k-range]
                   [db dbi-name pred k-range k-type]
                   [db dbi-name pred k-range k-type v-type]
                   [db dbi-name pred k-range k-type v-type raw-pred?])
       :doc      "Return a seq of non-nil results of calling `pred` in the specified key range in the key-value store, where `pred` is a function. If `raw-pred?` is true (default), `pred` takes a single raw KV object, otherwise, takes a pair of decoded `k` and `v` values.

This function is eager and attempts to load all matching data in range into memory. When the memory pressure is high, the remaining data is spilled on to a temporary disk file. The spill-to-disk mechanism is controlled by `:spill-opts` map passed to [[open-kv]].

To access store on a server, [[interpret.inter-fn]] should be used to define the `pred`.

`k-range` is a vector `[range-type k1 k2]`, `range-type` can be one of `:all`, `:at-least`, `:at-most`, `:closed`, `:closed-open`, `:greater-than`, `:less-than`, `:open`, `:open-closed`, plus backward variants that put a `-back` suffix to each of the above, e.g. `:all-back`;

`k-type` and `v-type` are data types of `k` and `v`, respectively. The allowed data types are described in [[read-buffer]].

See also [[range-filter]] "}
  range-keep i/range-keep)

(def ^{:arglists '([db dbi-name pred k-range]
                   [db dbi-name pred k-range k-type]
                   [db dbi-name pred k-range k-type v-type]
                   [db dbi-name pred k-range k-type v-type raw-pred?])
       :doc      "Return the first logical true esult of calling `pred` in the specified key range in the key-value store, where `pred` is a function. If `raw-pred?` is true (default), `pred` takes a single raw `IKV` object, otherwise, takes a pair of decoded `k` and `v` values.

To access store on a server, [[interpret.inter-fn]] should be used to define the `pred`.

`k-range` is a vector `[range-type k1 k2]`, `range-type` can be one of `:all`, `:at-least`, `:at-most`, `:closed`, `:closed-open`, `:greater-than`, `:less-than`, `:open`, `:open-closed`, plus backward variants that put a `-back` suffix to each of the above, e.g. `:all-back`;

`k-type` and `v-type` are data types of `k` and `v`, respectively. The allowed data types are described in [[read-buffer]].

See also [[range-filter]] "}
  range-some i/range-some)

(def ^{:arglists '([db dbi-name pred k-range]
                   [db dbi-name pred k-range k-type]
                   [db dbi-name pred k-range k-type v-type]
                   [db dbi-name pred k-range k-type v-type raw-pred?])
       :doc      "Return the number of kv pairs in the specified key range in the
key-value store, for only those return true value for `pred` function call. If `raw-pred?` is true (default), `pred` takes a single raw `IKV` object, otherwise, takes a pair of decoded `k` and `v` values.

To access store on a server, [[interpret.inter-fn]] should be used to define the `pred`.

`k-type` indicates data type of `k` and the allowed data types are described in [[read-buffer]].

`k-range` is a vector `[range-type k1 k2]`, `range-type` can be one of `:all`, `:at-least`, `:at-most`, `:closed`, `:closed-open`, `:greater-than`,
`:less-than`, `:open`, `:open-closed`, plus backward variants that put a `-back` suffix to each of the above, e.g. `:all-back`;

     Examples:

              (require '[datalevin.interpret :as i])

              (def pred (i/inter-fn [kv]
                         (let [^long lk (read-buffer (k kv) :long)]
                          (> lk 15))))

              (range-filter-count lmdb \"a\" pred [:less-than 20] :long)
              ;;==> 3"}
  range-filter-count i/range-filter-count)

(def ^{:arglists '([db dbi-name pred k-range]
                   [db dbi-name pred k-range k-type]
                   [db dbi-name visitor k-range k-type v-type]
                   [db dbi-name visitor k-range k-type v-type raw-pred?])
       :doc      "Call `visitor` function on each kv pairs in the specified key range, presumably for side effects. Return `nil`. Each kv pair is an `IKV`, with both key and value fields being a `ByteBuffer`. If `raw-pred?` is true (default), `visitor` takes a single raw `IKV` object, otherwise, takes a pair of decoded `k` and `v` values.

      If `visitor` function returns a special value `:datalevin/terminate-visit`, the visit will stop immediately.

      For client/server usage, [[datalevin.interpret/inter-fn]] should be used to define the `visitor` function. For babashka pod usage, `defpodfn` should be used.

    `k-type` indicates data type of `k` and the allowed data types are described
    in [[read-buffer]].

     `k-range` is a vector `[range-type k1 k2]`, `range-type` can be one of
     `:all`, `:at-least`, `:at-most`, `:closed`, `:closed-open`, `:greater-than`,
     `:less-than`, `:open`, `:open-closed`, plus backward variants that put a
     `-back` suffix to each of the above, e.g. `:all-back`;

     Examples:

              (require '[datalevin.interpret :as i])
              (import '[java.util Hashmap])

              (def hashmap (HashMap.))
              (def visitor (i/inter-fn [kv]
                             (let [^long k (b/read-buffer (l/k kv) :long)
                                   ^long v (b/read-buffer (l/v kv) :long)]
                                  (.put hashmap k v))))
              (visit lmdb \"a\" visitor [:closed 11 19] :long)
              "}
  visit i/visit)

;; List related functions

(def ^{:arglists '([db list-name]
                   [db list-name opts])
       :doc      "Open a special kind of DBI, that permits a list of
  values for the same key.

  `list-name` is a string, the DBI name. `opts` is the same option map as
  in [[open-dbi]].

  These values (the list) will be stored together in a sorted set.
  They should be of the same type. Each list item cannot be
  larger than 511 bytes. Point and range queries on these values are
  supported.

  See [[put-list-items]], [[get-list]], [[list-range]], and so on."}
  open-list-dbi i/open-list-dbi)

(def ^{:arglists '([db list-name k vs k-type v-type])
       :doc      "Put some list items to a key.
  `list-name` is the name of the sub-database that is opened by
  [[open-list-dbi]].

  `vs` is a seq of values that will be associated with the key `k`.

  These values (the list) will be stored together in a sorted set.
  They should be of the same type. Each list item cannot be
  larger than 511 bytes. Point and range queries on these values are
  supported.

  See [[get-list]], [[list-range]], and so on."}
  put-list-items i/put-list-items)

(def ^{:arglists '([db list-name k k-type]
                   [db list-name k vs k-type v-type])
       :doc      "Delete a list or some items of the list by the key.
  `list-name` is the name of the sub-database that is opened by
  [[open-list-dbi]].

  `vs` is a seq of values that are associated with the key `k`, and will
   be deleted. If unspecified, all list items and the key will be deleted.

   See also [[put-list-items]]."}
  del-list-items i/del-list-items)

(def ^{:arglists '([db list-name k k-type v-type])
       :doc      "Get a list by the key. The list items were added
  by [[put-list-items]]."}
  get-list i/get-list)

(def ^{:arglists '([db list-name visitor k k-type]
                   [db list-name visitor k k-type v-type]
                   [db list-name visitor k k-type v-type raw-pred?])
       :doc      "Visit the list associated with a key, presumably for
  side effects. The list items were added by [[put-list-items]]. When `raw-pred?` is true (default), the visitor call is `(visitor kv)`, where `kv`
     is a raw `IKV` object, with both key and value fields being a `ByteBuffer`; otherwise, the call is `(visitor k v)`, where `k` and `v` are already decoded key and value. "}
  visit-list i/visit-list)

(def ^{:arglists '([db list-name k k-type])
       :doc      "Return the number of items in the list associated with a
  key. The list items were added by [[put-list-items]]."}
  list-count i/list-count)

(def ^{:arglists '([db list-name k v k-type v-type])
       :doc      "Return true if an item is in the list associated with the
 key. The list items were added by [[put-list-items]]."}
  in-list? i/in-list?)

(def ^{:arglists '([db list-name k-range k-type v-range v-type])
       :doc      "Return a seq of key-values in the specified value range
     and the specified key range of a sub-database opened by
     [[open-list-dbi]].

     The same range specification as `k-range` in [[get-range]] is
     supported for both `k-range` and `v-range`."}
  list-range i/list-range)

(def ^{:arglists '([db list-name k-range k-type v-range v-type])
       :doc      "Return the number of key-values in the specified value
     range and the specified key range of a sub-database opened by
     [[open-list-dbi]].

     The same range specification as `k-range` in [[get-range]] is
     supported for both `k-range` and `v-range`."}
  list-range-count i/list-range-count)

(def ^{:arglists '([db list-name k-range k-type v-range v-type])
       :doc      "Return the first key-values in the specified value range
     and the specified key range of a sub-database opened by
     [[open-list-dbi]].

     The same range specification as `k-range` in [[get-range]] is
     supported for both `k-range` and `v-range`."}
  list-range-first i/list-range-first)

(def ^{:arglists '([db list-name n k-range k-type v-range v-type])
       :doc      "Return the first n key-values in the specified value range
     and the specified key range of a sub-database opened by
     [[open-list-dbi]].

     The same range specification as `k-range` in [[get-range]] is
     supported for both `k-range` and `v-range`."}
  list-range-first-n i/list-range-first-n)

(def ^{:arglists '([db list-name pred k-range k-type v-range v-type]
                   [db list-name pred k-range k-type v-range v-type raw-pred?])
       :doc      "Return a seq of key-values in the specified value range
     and the specified key range of a sub-database opened by
     [[open-list-dbi]], filtered by a predicate `pred`. When `raw-pred?` is true (default), the predicate call is `(pred kv)`, where `kv`
     is a raw `IKV` object, with both key and value fields being a `ByteBuffer`; otherwise, the call is `(pred k v)`, where `k` and `v` are already decoded key and value.

     The same range specification as `k-range` in [[get-range]] is
     supported for both `k-range` and `v-range`."}
  list-range-filter i/list-range-filter)

(def ^{:arglists '([db list-name pred k-range k-type v-range v-type]
                   [db list-name pred k-range k-type v-range v-type raw-pred?])
       :doc      "Return a seq of logical true results of calling `pred` in the specified value range and the specified key range of a sub-database opened by [[open-list-dbi]]. If `raw-pred?` is true (default), `pred` takes a single raw KV object, otherwise, takes a pair of decoded `k` and `v` values.

To access store on a server, [[interpret.inter-fn]] should be used to define the `pred`.

This function is eager and attempts to load all matching data in range into memory. When the memory pressure is high, the remaining data is spilled on to a temporary disk file. The spill-to-disk mechanism is controlled by `:spill-opts` map passed to [[open-kv]].

     The same range specification as `k-range` in [[get-range]] is
     supported for both `k-range` and `v-range`."}
  list-range-keep i/list-range-keep)

(def ^{:arglists '([db list-name pred k-range k-type v-range v-type]
                   [db list-name pred k-range k-type v-range v-type raw-pred?])
       :doc      "Return the number of key-values in the specified value range
     and the specified key range of a sub-database opened by
     [[open-list-dbi]], filtered by a predicate `pred`. If `raw-pred?` is true (default), `pred` takes a single raw KV object, otherwise, takes a pair of decoded `k` and `v` values.

     The same range specification as `k-range` in [[get-range]] is
     supported for both `k-range` and `v-range`."}
  list-range-filter-count i/list-range-filter-count)

(def ^{:arglists '([db list-name pred k-range k-type v-range v-type]
                   [db list-name pred k-range k-type v-range v-type raw-pred?])
       :doc      "Return the first logical true result of `pred` calls in
       the specified value range and the specified key range of a
       sub-database opened by [[open-list-dbi]]. If `raw-pred?` is true (default), `pred` takes a single raw KV object, otherwise, takes a pair of decoded `k` and `v` values.

     The same range specification as `k-range` in [[get-range]] is
     supported for both `k-range` and `v-range`."}
  list-range-some i/list-range-some)

(def ^{:arglists '([db list-name visitor k-range k-type v-range v-type]
                   [db list-name visitor k-range k-type v-range v-type raw-pred?])
       :doc      "Visit a list range, presumably for side effects, in a
     sub-database opened by [[open-list-dbi]]. If `raw-pred?` is true (default), `visitor` function takes a single raw KV object, otherwise, it takes a pair of decoded `k` and `v` values.

     The same range specification as `k-range` in [[get-range]] is
     supported for both `k-range` and `v-range`."}
  visit-list-range i/visit-list-range)

(def ^{:arglists '([db opts] [db schema opts])
       :doc      "Close the `db`, dump the data, clear the `db`, then
  reload the data and re-index using the given option. Return a new
  re-indexed `db`.

  The `db` can be a Datalog connection, a key-value database, a full-text
  search engine, or a vector index.

  The `opts` is the corresponding option map.

  If an additional option `:backup?` is true in the option map,
  a backup DB `dtlv-re-index-<timestamp>` will be created in system temporary
  directory.

  If `db` is a search engine, its `:include-text?` option must be
  `true` or an exception will be thrown.

  If `db` is a Datalog connection, `schema` can be used to supply a
  new schema.

  This function is only safe to call when other threads or programs are
  not accessing the same `db`."}
  re-index dump/re-index)


;; -------------------------------------
;; Search API

(def ^{:arglists '([lmdb]
                   [lmdb opts])
       :doc      "Create a full-text search engine.

   The search index is stored in the passed-in key-value database
   opened by [[open-kv]].

  `opts` is an option map that may contain these keys:

   * `:index-position?` indicates whether to index term positions. Default
     is `false`.

   * `:include-text?` indicates whether to store original text in the
     search engine. Required for [[re-index]]. Default is `false`.

   * `:domain` is an identifier string, indicates the domain of this search
     engine. This way, multiple independent search engines can reside in the
     same key-value database, each with its own domain identifier.

   * `:analyzer` is a function that takes a text string and return a seq of
    [term, position, offset], where term is a word, position is the sequence
     number of the term in the document, and offset is the character offset
     of the term in the document. E.g. for a blank space analyzer and the
     document \"The quick brown fox jumps over the lazy dog\",
     [\"quick\" 1 4] would be the second entry of the resulting seq.

   * `:query-analyzer` is a similar function that overrides the analyzer at
    query time (and not indexing time). Mostly useful for autocomplete search in
    conjunction with the `datalevin.search-utils/prefix-token-filter`.

   * `:search-opts` is the default options passed to [[search]] function.

  See [[datalevin.search-utils]] for some functions to customize search."}
  new-search-engine sc/new-search-engine)

(def ^{:arglists '([engine doc-ref doc-text]
                   [engine doc-ref doc-text check-exist?])
       :doc      "Add a document to the full-text search engine.

  `doc-ref` can be arbitrary Clojure data that uniquely refers to the document
in the system.

  `doc-text` is the content of the document as a string.  By default, search
   engine does not store the original text and assumes that caller can
   retrieve them by `doc-ref`. Set search engine option `:include-text?` to
   `true` to change that.

  `check-exist?` indicating whether to check the existence of this document in
the search index. Default is `true` and search index will be updated with
the new text. For better ingestion speed, set it to `false` when importing
data, i.e. when it is known that `doc-ref` does not already exist in the
search index."}
  add-doc i/add-doc)

(def ^{:arglists '([engine doc-ref])
       :doc      "Remove a document referred to by `doc-ref` from the search
engine index."}
  remove-doc i/remove-doc)

(def ^{:arglists '([engine])
       :doc      "Remove all documents from the search engine index."}
  clear-docs i/clear-docs)

(def ^{:arglists '([engine doc-ref])
       :doc      "Test if a `doc-ref` is already in the search index"}
  doc-indexed? i/doc-indexed?)

(def ^{:arglists '([engine])
       :doc      "Return the number of documents in the search index"}
  doc-count i/doc-count)

(def ^{:arglists '([engine query]
                   [engine query opts])
       :doc      "Issue a `query` to the search engine. `query` could be a
       string of words or a search data structure. The search data structure
       is a boolean expression, formally with the following grammar:

    <expression> ::= <term> | [ <operator> <operands> ]
    <operator>   ::= :or | :and | :not
    <operands>   ::= <expression>+
    <term>       ::= string | { <pair>+ }
    <pair>       ::= <key> string
    <key>        ::= :phrase | :term

For example, `[:or [:and \"red\" \"fox\" [:not \"lazy\"]] {:phrase \"jump over\"}]`

If the query is a string of words, e.g. `\"word1 word2 word3\"`, it is equivalent
to `[:or \"word1\" \"word2\" \"word3\"]` when using the default analyzer.

`opts` map may have these keys:

  * `:display` can be one of `:refs` (default), `:offsets`, `:texts`,
    or `:texts+offsets`.
    - `:refs` returns a lazy sequence of `doc-ref` ordered by relevance.
    - `:offsets` returns a lazy sequence of
      `[doc-ref [term1 [offset ...]] [term2 [...]] ...]`,
      ordered by relevance, if search engine option `:index-position?`
      is `true`. `term` and `offset` can be used to highlight the
      matched terms and their locations in the documents.
    - `:texts` returns a lazy sequence of `[doc-ref doc-text]` ordered
      by relevance, if search engine option `:include-text?` is `true`.
    - `:texts+offsets` returns a lazy sequence of
      `[doc-ref doc-text [term1 [offset ...]] [term2 [...]] ...]`,
      ordered by relevance, if search engine option `:index-position?`
      and `:include-text?` are both `true`.
  * `:top` is an integer (default 10), the number of results desired.
  * `:doc-filter` is a boolean function that takes a `doc-ref` and
    determines whether or not to include the corresponding document in the
    results (default is `(constantly true)`)
  * `proximity-expansion` can be used to adjust the search quality vs. time
    trade-off: the bigger the number, the higher is the quality, but the longer is
    the search time. Default value is `2`. It is only applicable when
    `index-position?` is `true`.
  * `proximity-max-dist`  can be used to control the maximal distance between
    terms that would still be considered as belonging to the same span. Default
    value is `45`. It is only applicable when `index-position?` is `true`"}
  search i/search)

(def ^{:arglists '([writer doc-ref doc-text]
                   [writer doc-ref doc-text opts])
       :doc      "Create a writer for writing documents to the search index
  in bulk. Note that this is not supported in the client/server mode.

  The search index is stored in the passed-in key value database opened
  by [[open-kv]]. See also [[write]] and [[commit]].

  `opts` is an option map that may contain these keys:
  * `:domain` is an identifier string, indicates the domain of this search
  engine.
      This way, multiple independent search engines can reside in the same
      key-value database, each with its own domain identifier.
  * `:analyzer` is a function that takes a text string and return a seq of
    [term, position, offset], where term is a word, position is the sequence
     number of the term, and offset is the character offset of this term.
  * `:index-position?` indicating whether to index positions of terms in the
     documents. Default is `false`.
  * `:include-text?` indicating whether to store original text in the search
     engine. Default is `false`."}
  search-index-writer sc/search-index-writer)

(def ^{:arglists '([writer doc-ref doc-text])
       :doc      "Write a document to search index.

  Used only with [[search-index-writer]]"}
  write sc/write)

(def ^{:arglists '([writer])
       :doc      "Commit writes to search index, must be called after writing
  all documents. Used only with [[search-index-writer]]"}
  commit sc/commit)

;; -------------------------------------
;; vector

(def ^{:arglists '([lmdb opts])
       :doc      "Create a Hierarchical Navigable Small World (HNSW) graph index
  for vectors.

  The mapping of semantic references to vectors is stored in the
  passed-in key-value DB opened by [[open-kv]], and the HNSW index blob is
  also stored in LMDB (DBIs `datalevin/vec-index` and `datalevin/vec-meta`).
  Legacy `.vid` files are migrated automatically when present.

  `opts` is a map that may contain these keys:

   * `:dimensions` is the number of dimensions of the vectors. Required.

   * `:metric-type` is a keyword of the metric used to calculate vector
     similarity. The following metric is supported:
      - `:euclidean` This is the default.
      - `:cosine`
      - `:dot-product`
      - `:haversine`
      - `:divergence`
      - `:pearson`
      - `:jaccard`
      - `:hamming`
      - `:tanimoto`
      - `:sorensen`

   * `:quantization` is a keyword of the scalar value type of the vectors:
      - `:float` This is the default.
      - `:double`
      - `:float16`
      - `:int8`
      - `:byte`

   * `:connectivity` is the number of connected nodes in the index graph.

   * `:expansion-add` is the number of candidates considered when adding vector
     to the index.

   * `:expansion-search` is the number of candidates considered when searching
     the index.

   * `:search-opts` is an option map having these keys:
      ` `:top` is the number of results desired. Default is 10.
      - `:display` is a keyword indicating what is in each result.
         * `:refs` returning only semantic reference. Default.
         * `:refs+dists` returning both semantic reference and vector distance.
      - `:vec-filter` is a function that takes a semantic reference and returns
        `true` only for those references that need to be in the results.

   * `:domain` is a string, indicates the domain of this vector index."}
  new-vector-index v/new-vector-index)

(def ^{:arglists '([index])
       :doc      "Close the vector index and free memory"}
  close-vector-index i/close-vecs)

(def ^{:arglists '([index])
       :doc      "Close the vector index and delete all vectors"}
  clear-vector-index i/clear-vecs)

(def ^{:arglists '([index])
       :doc      "Force vector checkpoint persistence to LMDB (`datalevin/vec-index` and `datalevin/vec-meta`)."}
  force-vec-checkpoint! i/persist-vecs)

(defn ^{:arglists '([index])
        :doc      "Return vector checkpoint metadata from LMDB, including snapshot/replay LSN coverage and chunk statistics."}
  vector-checkpoint-state
  [index]
  (:checkpoint (i/vecs-info index)))

(def ^{:arglists '([index])
       :doc      "Return a map of information about the vector index:

     * `:size` the number of vectors indexed
     * `:memory` the memory usage of the vector index in bytes
     * `:capacity` the capacity of the vector index at the moment
     * `:hardware` the vector Instruction Set Architecture (ISA) name
     * `:filename` the full path file name of the vector index
     * `:domain` the vector index domain name
     * `:dimensions` see [[new-vector-index]]
     * `:metric-type` see [[new-vector-index]]
     * `:quantization` see [[new-vector-index]]
     * `:connectivity`  see [[new-vector-index]]
     * `:expansion-add` see [[new-vector-index]]
     * `:expansion-search`see [[new-vector-index]]
     * `:checkpoint` checkpoint metadata map from LMDB"}
  vector-index-info i/vecs-info)

(def ^{:arglists '([index vec-ref vec-data])
       :doc      "Add a vector to the vector index.

       `vec-ref` can be any Clojure value that is semantically meaningful.
       `vec-data` is the vector, e.g. a seq or an array of numbers that has
        consistent dimensions and value type as that of the `index`.

        See [[new-vector-index]]"}
  add-vec i/add-vec)

(def ^{:arglists '([index vec-ref])
       :doc      "Remove all the vectors associated with the `vec-ref`
  from the `index`"}
  remove-vec i/remove-vec)

(def ^{:arglists '([index query-vec] [index query-vec opts])
       :doc      "Search the vector index with a query vector, return the
   `vec-ref` of its nearest neighbors, optionally, the distances.

  `query-vec` is the query vector. It should have the same dimensions and
  quantization as that of the `index`, see [[new-vector-index]].

  `opts` may have these keys:

   * `:top` is the number of neighbors desired, default is 10.
   * `:display` indicates what to include in the result:
      - `:refs` returns only `vec-ref`, and is the default.
      - `:refs+dists` returns `vec-ref` and distances to the query vector
        together.
   * `:vec-filter` is a boolean function that takes the `vec-ref` and decides if
     it should be in the results."}
  search-vec i/search-vec)

;; -------------------------------------
;; byte buffer

(def ^{:arglists '([bf x] [bf x x-type])
       :doc      "Put the given type of data `x` in buffer `bf`. `x-type` can be
    one of following scalar data types, a vector of these scalars to indicate a heterogeneous tuple data type, or a vector of a single scalar to indicate a homogeneous tuple data type:

    - `:data` (default), arbitrary EDN data. Avoid this as keys for range queries. Further more, `:data` is not permitted in a tuple.
    - `:string`, UTF-8 string
    - `:long`, 64 bits integer
    - `:float`, 32 bits IEEE754 floating point number
    - `:double`, 64 bits IEEE754 floating point number
    - `:bigint`, a `java.math.BigInteger` in range `[-2^1015, 2^1015-1]`
    - `:bigdec`, a `java.math.BigDecimal`, the unscaled value is in range `[-2^1015, 2^1015-1]`
    - `:bytes`, byte array
    - `:keyword`, EDN keyword
    - `:symbol`, EDN symbol
    - `:boolean`, `true` or `false`
    - `:instant`, a `java.util.Date`
    - `:uuid`, a `java.util.UUID`

  No tuple element can be more than 255 bytes in size.

  If the value is to be put in a LMDB key buffer, it must be less than
  511 bytes."}
  put-buffer b/put-buffer)

(def ^{:arglists '([bf] [bf v-type])
       :doc      "Get the given type of data from buffer `bf`, `v-type` can be
one of the following scalar data types, a vector of these scalars to indicate a heterogeneous tuple data type, or a vector of a single scalar to indicate a homogeneous tuple data type:

  - `:data` (default), arbitrary serialized EDN data.
  - `:string`, UTF-8 string
  - `:long`, 64 bits integer
  - `:float`, 32 bits IEEE754 floating point number
  - `:double`, 64 bits IEEE754 floating point number
  - `:bigint`, a `java.math.BigInteger` in range `[-2^1015, 2^1015-1]`,
  - `:bigdec`, a `java.math.BigDecimal`, the unscaled value is in range `[-2^1015, 2^1015-1]`,
  - `:bytes`, an byte array
  - `:keyword`, EDN keyword
  - `:symbol`, EDN symbol
  - `:boolean`, `true` or `false`
  - `:instant`, timestamp, same as `java.util.Date`
  - `:uuid`, UUID, same as `java.util.UUID`"}
  read-buffer b/read-buffer)

(def ^{:arglists '([input & opts])
       :doc      "Reads CSV-data from input (String or java.io.Reader) into a lazy sequence of vectors of strings. This function is faster than clojure.data.csv/read-csv, and is more robust in dealing with quoted data.

   Valid options are
     :separator (default \\,)
     :quote (default \\\")"}
  read-csv csv/read-csv)

(def ^{:arglists '([writer data & opts])
       :doc      "Writes data to writer in CSV-format. Same as clojure.data.csv/write-csv.

   Valid options are
     :separator (Default \\,)
     :quote (Default \\\")
     :quote? (A predicate function which determines if a string should be quoted. Defaults to quoting only when necessary.)
     :newline (:lf (default) or :cr+lf)"}
  write-csv csv/write-csv)

(def ^{:arglists '([s])
       :doc      "Turn a string into a hexified string"}
  hexify-string u/hexify-string)

(def ^{:arglists '([s])
       :doc      "Turn a hexified string back into a normal string"}
  unhexify-string u/unhexify-string)
