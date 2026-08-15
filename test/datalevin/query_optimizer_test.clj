(ns datalevin.query-optimizer-test
  (:require
   [clojure.test :refer [deftest is testing]]
   [datalevin.constants :as c]
   [datalevin.core :as d]
   [datalevin.db :as db]
   [datalevin.join :as j]
   [datalevin.parser :as dp]
   [datalevin.query.execute :as qexec]
   [datalevin.query-optimizer :as qo]
   [datalevin.query.plan :as qplan]
   [datalevin.query.resolve :as qresolve]
   [datalevin.query-util :as qu]
   [datalevin.relation :as r]
   [datalevin.util :as u])
  (:import
   [java.util List UUID]
   [org.eclipse.collections.impl.list.mutable FastList]))

(def late-expansion-query
  '[:find ?id ?score
    :where
    [?start :start/id 1]
    (or-join [?start ?item]
      (and [?link :link/from ?start]
           [?link :link/to ?item])
      (and [?link1 :link/from ?start]
           [?link1 :link/to ?mid]
           [?link2 :link/from ?mid]
           [?link2 :link/to ?item]))
    [?item :item/id ?id]
    [?item :item/score ?score]
    [(< ?score 3000)]
    :order-by [?score :desc ?id :asc]
    :limit 5])

(def unique-endpoint-query
  '[:find ?start ?middle ?end
    :where
    [?start :node/id 1]
    [?end :node/code "three"]
    [?edge1 :edge/from ?start]
    [?edge1 :edge/to ?middle]
    [?edge2 :edge/from ?middle]
    [?edge2 :edge/to ?end]])

(def equality-disjunction-query
  '[:find ?message ?x-inc ?y-inc
    :where
    [?country-x :place/name "X"]
    [?country-y :place/name "Y"]
    [?person :person/name "P"]
    [?message :message/hasCreator ?person]
    [?message :message/isLocatedIn ?loc]
    (or-join [?loc ?country-x ?country-y ?x-inc ?y-inc]
      (and [(= ?loc ?country-x)]
           [(ground 1) ?x-inc]
           [(ground 0) ?y-inc])
      (and [(= ?country-y ?loc)]
           [(ground 0) ?x-inc]
           [(ground 1) ?y-inc]))])

(def pushed-equality-disjunction-clause
  '(or-join [?message ?country-x ?country-y ?x-inc ?y-inc]
    (and [?message :message/isLocatedIn ?country-x]
         [(ground 1) ?x-inc]
         [(ground 0) ?y-inc])
    (and [?message :message/isLocatedIn ?country-y]
         [(ground 0) ?x-inc]
         [(ground 1) ?y-inc])))

(defn- materialized-context
  [db-value query]
  (let [parsed-q          (dp/parse-query query)
        [parsed-q inputs] (qo/plugin-inputs parsed-q [db-value])]
    (-> (qplan/make-context parsed-q false)
        (qresolve/resolve-ins inputs)
        (qo/materialize-input-bound-patterns))))

(defn- equality-rewritten-context
  [db-value query]
  (-> (materialized-context db-value query)
      (qo/push-down-equality-disjunctions)))

(defn- redundant-resolved-context
  [db-value query]
  (let [parsed-q          (dp/parse-query query)
        [parsed-q inputs] (qo/plugin-inputs parsed-q [db-value])]
    (-> (qplan/make-context parsed-q false)
        (qresolve/resolve-ins inputs)
        (qexec/resolve-redudants))))

(deftest equality-disjunction-pattern-pushdown-test
  (let [dir    (u/tmp-dir (str "equality-disjunction-pushdown-"
                               (UUID/randomUUID)))
        schema {:place/name          {:db/valueType :db.type/string}
                :person/name         {:db/valueType :db.type/string}
                :message/id          {:db/valueType :db.type/long
                                      :db/unique    :db.unique/identity}
                :message/hasCreator  {:db/valueType :db.type/ref}
                :message/isLocatedIn {:db/valueType :db.type/ref}}
        conn   (d/get-conn dir schema)]
    (try
      (d/transact! conn
                   [{:db/id 1 :place/name "X"}
                    {:db/id 2 :place/name "Y"}
                    {:db/id 3 :place/name "Z"}
                    {:db/id 10 :person/name "P"}
                    {:db/id 100 :message/id 100
                     :message/hasCreator 10 :message/isLocatedIn 1}
                    {:db/id 101 :message/id 101
                     :message/hasCreator 10 :message/isLocatedIn 2}
                    {:db/id 102 :message/id 102
                     :message/hasCreator 10 :message/isLocatedIn 3}])
      (let [db-value  (d/db conn)
            original  (:qorig-where (dp/parse-query
                                      equality-disjunction-query))
            rewritten (get-in
                        (equality-rewritten-context
                          db-value equality-disjunction-query)
                        [:parsed-q :qorig-where])]
        (testing "a filter-only value pattern is distributed into AV branches"
          (is (not (some #{'[?message :message/isLocatedIn ?loc]}
                         rewritten)))
          (is (some #{pushed-equality-disjunction-clause} rewritten))
          (is (= (dec (count original)) (count rewritten))))
        (testing "the rewrite preserves branch bindings and query results"
          (is (= #{[100 1 0] [101 0 1]}
                 (d/q equality-disjunction-query db-value))))
        (testing "a value needed by the result remains outside the rewrite"
          (let [query     {:find  ['?message '?loc]
                           :where original}
                remaining (get-in (equality-rewritten-context db-value query)
                                  [:parsed-q :qorig-where])]
            (is (= original remaining))))
        (testing "a value used by another where clause remains available"
          (let [where     (conj original '[?loc :place/name ?loc-name])
                query     {:find  ['?message]
                           :where where}
                remaining (get-in (equality-rewritten-context db-value query)
                                  [:parsed-q :qorig-where])]
            (is (= where remaining))))
        (testing "an entity with a selective constant anchor keeps one probe"
          (let [where     (conj original '[?message :message/id 100])
                query     {:find  ['?message]
                           :where where}
                remaining (get-in (equality-rewritten-context db-value query)
                                  [:parsed-q :qorig-where])]
            (is (= where remaining))))
        (testing "branch targets without selective outer bindings are skipped"
          (let [where     (-> original
                              (assoc 0 '[?country-x :place/name ?x-name])
                              (assoc 1 '[?country-y :place/name ?y-name]))
                query     {:find  ['?message]
                           :where where}
                remaining (get-in (equality-rewritten-context db-value query)
                                  [:parsed-q :qorig-where])]
            (is (= where remaining)))))
      (finally
        (d/close conn)
        (u/delete-files dir)))))

(deftest costed-indexed-union-order-test
  (let [dir    (u/tmp-dir (str "indexed-union-order-" (UUID/randomUUID)))
        schema {:message/hasCreator  {:db/valueType :db.type/ref}
                :message/isLocatedIn {:db/valueType :db.type/ref}}
        conn   (d/get-conn dir schema)
        creator-clause '[?message :message/hasCreator ?person]
        union-clause   pushed-equality-disjunction-clause
        resolve-late   @(ns-resolve 'datalevin.query.execute
                                    'resolve-late-clauses)
        choose-union   @(ns-resolve 'datalevin.query.execute
                                    'cheaper-indexed-union)
        make-context
        (fn [people]
          {:sources {'$ (d/db conn)}
           :rules   nil
           :rels
           [(r/relation!
              {'?person 0 '?country-x 1 '?country-y 2}
              (FastList. ^java.util.Collection
                         (mapv #(object-array [% 100 101]) people)))]})
        run
        (fn [people]
          (let [explain (volatile! {})
                context (make-context people)
                result  (binding [qplan/*explain* explain
                                  qu/*implicit-source* (get-in context
                                                               [:sources '$])]
                          (resolve-late context
                                        [creator-clause union-clause]))
                rel     (if (< 1 (count (:rels result)))
                          (reduce j/hash-join (:rels result))
                          (first (:rels result)))
                attrs   (:attrs rel)]
            {:decision (first (:late-clause-decisions @explain))
             :order    (:late-clauses result)
             :rows     (into #{}
                             (map (fn [^objects tuple]
                                    (mapv #(aget tuple (long (attrs %)))
                                          ['?message '?person
                                           '?x-inc '?y-inc])))
                             (:tuples rel))}))]
    (try
      (let [messages
            (into
              [{:db/id 1000 :message/hasCreator 10
                :message/isLocatedIn 100}
               {:db/id 1001 :message/hasCreator 10
                :message/isLocatedIn 100}
               {:db/id 1002 :message/hasCreator 10
                :message/isLocatedIn 101}
               {:db/id 1003 :message/hasCreator 10
                :message/isLocatedIn 101}
               {:db/id 2000 :message/hasCreator 11
                :message/isLocatedIn 100}
               {:db/id 2001 :message/hasCreator 11
                :message/isLocatedIn 101}]
              (map (fn [^long id]
                     {:db/id id :message/hasCreator 10
                      :message/isLocatedIn 102}))
              (range 1004 1020))]
        (d/transact! conn messages))
      (let [wide          (run [10 11])
            narrow        (run [11])
            wide-choice   (:decision wide)
            narrow-choice (:decision narrow)]
        (testing "the smaller indexed country union runs before creator fanout"
          (is (= :indexed-union-first (:strategy wide-choice)))
          (is (= 22 (:pattern-fanout wide-choice)))
          (is (= 6 (:union-fanout wide-choice)))
          (is (= union-clause (first (:order wide))))
          (is (= #{[1000 10 1 0] [1001 10 1 0]
                   [1002 10 0 1] [1003 10 0 1]
                   [2000 11 1 0] [2001 11 0 1]}
                 (:rows wide))))
        (testing "a selective creator binding retains creator-first order"
          (is (= :bound-pattern-first (:strategy narrow-choice)))
          (is (= 2 (:pattern-fanout narrow-choice)))
          (is (= 6 (:union-fanout narrow-choice)))
          (is (= creator-clause (first (:order narrow))))
          (is (= #{[2000 11 1 0] [2001 11 0 1]}
                 (:rows narrow))))
        (testing "ordinary late clauses cause no speculative index counts"
          (is (nil?
                (with-redefs [db/-count
                              (fn [& _]
                                (throw (ex-info "unexpected count" {})))]
                  (choose-union
                    (make-context [10 11]) creator-clause
                    ['[?message :message/isLocatedIn ?country-x]]))))))
      (finally
        (d/close conn)
        (u/delete-files dir)))))

(deftest unique-constant-lookups-materialized-test
  (let [dir    (u/tmp-dir (str "unique-constant-lookup-"
                               (UUID/randomUUID)))
        schema {:node/id    {:db/valueType :db.type/long
                             :db/unique    :db.unique/identity}
                :node/code  {:db/valueType :db.type/string
                             :db/unique    :db.unique/value}
                :node/group {:db/valueType :db.type/string}
                :edge/from  {:db/valueType :db.type/ref}
                :edge/to    {:db/valueType :db.type/ref}}
        conn   (d/get-conn dir schema)]
    (try
      (d/transact! conn
                   [{:db/id 1001 :node/id 1 :node/code "one"
                     :node/group "odd"}
                    {:db/id 1002 :node/id 2 :node/code "two"
                     :node/group "even"}
                    {:db/id 1003 :node/id 3 :node/code "three"
                     :node/group "odd"}
                    {:db/id 2001 :edge/from 1001 :edge/to 1002}
                    {:db/id 2002 :edge/from 1002 :edge/to 1003}])
      (let [db-value (d/db conn)
            context  (materialized-context db-value unique-endpoint-query)
            rels     (:rels context)
            rel-vars (into #{} (mapcat (comp keys :attrs)) rels)
            remaining (set (get-in context [:parsed-q :qorig-where]))]
        (testing "multiple protected unique lookups seed bound entities"
          (is (every? rel-vars ['?start '?end]))
          (is (every? #(= 1 (.size ^List (:tuples %))) rels))
          (is (not (contains? remaining '[?start :node/id 1])))
          (is (not (contains? remaining '[?end :node/code "three"]))))
        (testing "the rewritten planning context preserves results"
          (is (= #{[1001 1002 1003]}
                 (d/q unique-endpoint-query db-value))))
        (testing "a missing unique value short-circuits to an empty result"
          (is (= #{}
                 (d/q '[:find ?start ?end
                        :where
                        [?start :node/id 1]
                        [?end :node/code "missing"]
                        [?edge :edge/from ?start]
                        [?edge :edge/to ?end]]
                      db-value))))
        (testing "non-unique constant lookups remain planner clauses"
          (let [query   '[:find ?e :where [?e :node/group "odd"]]
                context (materialized-context db-value query)]
            (is (empty? (:rels context)))
            (is (contains? (set (get-in context [:parsed-q :qorig-where]))
                           '[?e :node/group "odd"]))))
        (testing "a single unique root remains a planner clause"
          (let [query   '[:find ?e ?group
                          :where
                          [?e :node/code "two"]
                          [?e :node/group ?group]]
                context (materialized-context db-value query)]
            (is (empty? (:rels context)))
            (is (contains? (set (get-in context [:parsed-q :qorig-where]))
                           '[?e :node/code "two"]))))
        (testing "unique anchors in separate components stay with the planner"
          (let [query   '[:find ?left ?right
                          :where
                          [?left :node/id 1]
                          [?right :node/id 3]]
                context (materialized-context db-value query)]
            (is (empty? (:rels context)))
            (is (= (set (:qorig-where (dp/parse-query query)))
                   (set (get-in context [:parsed-q :qorig-where])))))))
      (finally
        (d/close conn)
        (u/delete-files dir)))))

(deftest redundant-value-pattern-cardinality-test
  (let [dir    (u/tmp-dir (str "redundant-value-pattern-"
                               (UUID/randomUUID)))
        schema {:item/state {:db/valueType :db.type/keyword}
                :item/tags  {:db/valueType   :db.type/keyword
                             :db/cardinality :db.cardinality/many}}
        conn   (d/get-conn dir schema)
        one-q  '[:find ?e ?state
                 :where
                 [?e :item/state :active]
                 [?e :item/state ?state]]
        many-q '[:find ?e ?tag
                 :where
                 [?e :item/tags ?tag]
                 [?e :item/tags :a]]]
    (try
      (d/transact! conn
                   [{:db/id 1 :item/state :active :item/tags [:a :b]}
                    {:db/id 2 :item/state :inactive :item/tags [:a]}])
      (let [db-value    (d/db conn)
            one-context (redundant-resolved-context db-value one-q)
            many-context (redundant-resolved-context db-value many-q)]
        (testing "a cardinality-one constant determines the duplicate value"
          (is (= ['[?e :item/state :active]]
                 (get-in one-context [:parsed-q :qorig-where])))
          (is (= #{[1 :active]} (d/q one-q db-value))))
        (testing "a cardinality-many value pattern retains all matching values"
          (is (empty? (get-in many-context [:parsed-q :qorig-where])))
          (let [rel (first (:rels many-context))]
            (is (= #{'?e '?tag} (set (keys (:attrs rel)))))
            (is (= 3 (.size ^List (:tuples rel)))))
          (is (= #{[1 :a] [1 :b] [2 :a]}
                 (d/q many-q db-value)))))
      (finally
        (d/close conn)
        (u/delete-files dir)))))

(deftest hash-join-output-materialization-cost-test
  (let [estimate-cost @(ns-resolve 'datalevin.query-optimizer
                                   'estimate-hash-join-cost)]
    (binding [c/magic-cost-hash-join              10.0
              c/magic-cost-hash-join-output-tuple 4.0
              c/magic-cost-hash-join-output-cell  1.0]
      (testing "the compatibility arity prices hash inputs"
        (is (= 300 (estimate-cost 10 20))))
      (testing "ordinary output is covered by the input work"
        (is (= 300 (estimate-cost 10 20 30 3))))
      (testing "materialization dominates when fanout is high enough"
        (is (= 350 (estimate-cost 10 20 50 3)))
        (is (= 1050 (estimate-cost 10 20 150 3)))))))

(deftest sampled-late-expansion-cost-test
  (let [dir    (u/tmp-dir (str "late-expansion-cost-" (UUID/randomUUID)))
        schema {:start/id   {:db/valueType :db.type/long
                             :db/unique    :db.unique/identity}
                :item/id    {:db/valueType :db.type/long
                             :db/unique    :db.unique/identity}
                :item/score {:db/valueType :db.type/long}
                :link/from  {:db/valueType :db.type/ref}
                :link/to    {:db/valueType :db.type/ref}}
        conn   (d/get-conn dir schema)]
    (try
      (let [start     1
            item-base 10000
            link-base 100000
            item-count 2000
            tx-data
            (into [{:db/id start :start/id 1}]
                  (mapcat
                    (fn [^long i]
                      (let [item (+ item-base i)]
                        [{:db/id item :item/id i :item/score i}
                         {:db/id (+ link-base i)
                          :link/from start
                          :link/to item}]))
                    (range item-count)))]
        (d/transact! conn tx-data)
        ;; Ordered access deliberately excludes an unmerged transaction cache.
        ;; The transaction is durable here, so use the persisted-index view.
        (let [db-value     (db/-clear-tx-cache (d/db conn))
              explain      (d/explain {:run? false}
                                      late-expansion-query db-value)
              conventional (some #(when (= :conventional (:kind %)) %)
                                 (:physical-plan-alternatives explain))
              expansion    (some #(when (= :sampled-late-expansion
                                           (:operation %))
                                     %)
                                 (get-in conventional
                                         [:cost-breakdown :late-stages]))]
          (testing "sampled expansion corrects conventional cardinality"
            (is (= :sampled (:confidence expansion)))
            (is (= item-count (:output expansion)))
            (is (= item-count (:size conventional))))
          (testing "the ordered top-k alternative is selected"
            (is (= :access
                   (get-in explain [:selected-plan-alternative :kind]))))
          (testing "the selected plan preserves query results"
            (is (= [[1999 1999]
                    [1998 1998]
                    [1997 1997]
                    [1996 1996]
                    [1995 1995]]
                   (d/q late-expansion-query db-value))))))
      (finally
        (d/close conn)
        (u/delete-files dir)))))
