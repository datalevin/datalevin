(ns datalevin.query-optimizer-test
  (:require
   [clojure.test :refer [deftest is testing]]
   [clojure.walk :as walk]
   [datalevin.constants :as c]
   [datalevin.core :as d]
   [datalevin.db :as db]
   [datalevin.join :as j]
   [datalevin.parser :as dp]
   [datalevin.query.execute :as qexec]
   [datalevin.query.optimizer.range :as qor]
   [datalevin.query.predicate :as qpred]
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

(def bounded-access-sample-query
  '[:find ?friend-id ?message-id ?date
    :in $ ?person-id ?max-date
    :where
    [?start :person/id ?person-id]
    [?k :knows/from ?start]
    [?k :knows/to ?friend]
    [?friend :person/id ?friend-id]
    [?message :message/creator ?friend]
    [?message :message/id ?message-id]
    [?message :message/date ?date]
    [(< ?date ?max-date)]
    :order-by [?date :desc ?message-id :asc]
    :limit 20])

(def deferred-eav-group-query
  '[:find ?forum-id ?forum-title ?post
    :where
    [?start :start/id 1]
    [?edge :edge/from ?start]
    [?edge :edge/to ?person]
    [?membership :membership/person ?person]
    [?membership :membership/forum ?forum]
    [?membership :membership/date ?date]
    [(> ?date 0)]
    [?post :post/person ?person]
    [?post :post/forum ?forum]
    [?forum :forum/id ?forum-id]
    [?forum :forum/title ?forum-title]])

(def dominated-projection-root-query
  '[:find ?friend-id ?first-name ?last-name
    :where
    [?start :start/id 1]
    [?edge :edge/from ?start]
    [?edge :edge/to ?friend]
    [?friend :person/id ?friend-id]
    [?friend :person/firstName ?first-name]
    [?friend :person/lastName ?last-name]])

(def filtered-projection-root-query
  '[:find ?friend-id ?score
    :where
    [?start :start/id 1]
    [?edge :edge/from ?start]
    [?edge :edge/to ?friend]
    [?friend :person/id ?friend-id]
    [?friend :person/score ?score]
    [(> ?score 1100)]])

(def post-top-k-enrichment-query
  '[:find ?id ?content ?score
    :where
    [?item :item/id ?id]
    [?item :item/score ?score]
    [(get-some-else $ ?item nil :item/content :item/image) [_ ?content]]
    :order-by [?score :desc ?id :asc]
    :limit 5])

(def post-top-k-enrichment-offset-query
  '[:find ?id ?content ?score
    :where
    [?item :item/id ?id]
    [?item :item/score ?score]
    [(get-some-else $ ?item nil :item/content :item/image) [_ ?content]]
    :order-by [2 :desc 0 :asc]
    :limit 3
    :offset 2])

(def filtering-enrichment-query
  '[:find ?id ?content ?score
    :where
    [?item :item/id ?id]
    [?item :item/score ?score]
    [(get-some $ ?item :item/content :item/image) [_ ?content]]
    :order-by [?score :desc ?id :asc]
    :limit 5])

(def unkeyed-enrichment-query
  '[:find ?content ?score
    :where
    [?item :item/score ?score]
    [(get-some-else $ ?item nil :item/content :item/image) [_ ?content]]
    :order-by [?score :desc]
    :limit 5])

(def ordered-output-enrichment-query
  '[:find ?id ?content ?score
    :where
    [?item :item/id ?id]
    [?item :item/score ?score]
    [(get-some-else $ ?item nil :item/content :item/image) [_ ?content]]
    :order-by [1 :asc]
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

(def selective-tag-anchor-query
  '[:find ?message
    :where
    [?start :start/id 1]
    (or-join [?start ?person]
      (and [?k :knows/from ?start]
           [?k :knows/to ?person])
      (and [?k1 :knows/from ?start]
           [?k1 :knows/to ?mid]
           [?k2 :knows/from ?mid]
           [?k2 :knows/to ?person]))
    [?tag :tag/name "needle"]
    [?message :message/hasTag ?tag]
    [?message :message/hasCreator ?person]
    [?message :message/marker _]])

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

(defn- selectively-materialized-context
  [db-value query]
  (-> (materialized-context db-value query)
      (qexec/resolve-redudants)
      (qo/push-down-equality-disjunctions)
      (qo/rewrite-unused-vars)
      (qo/materialize-selective-value-lookups)))

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
        choose-late    @(ns-resolve 'datalevin.query.execute
                                    'cheaper-late-producer)
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
                  (choose-late
                    (make-context [10 11])
                    [creator-clause
                     '[?message :message/isLocatedIn ?country-x]]))))))
      (finally
        (d/close conn)
        (u/delete-files dir)))))

(deftest late-or-join-branch-order-test
  (let [dir      (u/tmp-dir (str "late-or-join-branch-"
                                 (UUID/randomUUID)))
        schema   {:edge/from {:db/valueType :db.type/ref}
                  :edge/to   {:db/valueType :db.type/ref}}
        conn     (d/get-conn dir schema)
        forward '[?k3 :edge/from ?mid2]
        reverse '[?k3 :edge/to ?friend]
        clause   '(or-join [?start ?friend ?dist]
                    (and [?k1 :edge/from ?start]
                         [?k1 :edge/to ?mid1]
                         [?k2 :edge/from ?mid1]
                         [?k2 :edge/to ?mid2]
                         [?k3 :edge/from ?mid2]
                         [?k3 :edge/to ?friend]
                         [(ground 3) ?dist]))
        resolve-late @(ns-resolve 'datalevin.query.execute
                                  'resolve-late-clauses)]
    (try
      (d/transact!
        conn
        (concat [{:db/id 100 :edge/from 1 :edge/to 2}
                 {:db/id 101 :edge/from 2 :edge/to 3}
                 {:db/id 200 :edge/from 3 :edge/to 999}
                 {:db/id 300 :edge/from 500 :edge/to 999}
                 {:db/id 301 :edge/from 501 :edge/to 999}
                 {:db/id 302 :edge/from 502 :edge/to 999}
                 {:db/id 303 :edge/from 503 :edge/to 999}]
                (map (fn [^long i]
                       {:db/id (+ 200 i)
                        :edge/from 3
                        :edge/to   (+ 1000 i)})
                     (range 1 50))))
      (let [context {:sources {'$ (d/db conn)}
                     :rules   nil
                     :rels
                     [(r/relation! {'?start 0}
                                   (doto (FastList.)
                                     (.add (object-array [1]))))
                      (r/relation! {'?friend 0}
                                   (doto (FastList.)
                                     (.add (object-array [999]))))]}
            explain (volatile! {})
            result  (binding [qplan/*explain* explain
                              qu/*implicit-source* (get-in context
                                                           [:sources '$])]
                      (resolve-late context [clause]))
            rel     (if (< 1 (count (:rels result)))
                      (reduce j/hash-join (:rels result))
                      (first (:rels result)))
            attrs   (:attrs rel)
            rows    (into #{}
                          (map (fn [^objects tuple]
                                 (mapv #(aget tuple (long (attrs %)))
                                       ['?start '?friend '?dist])))
                          (:tuples rel))
            decision
            (some #(when (= reverse (:selected-clause %)) %)
                  (:late-or-join-branch-decisions @explain))
            alternatives (into {}
                               (map (juxt :clause identity))
                               (:alternatives decision))]
        (is (= #{[1 999 3]} rows))
        (is (some? decision))
        (is (= 50 (get-in alternatives [forward :fanout])))
        (is (= 5 (get-in alternatives [reverse :fanout])))
        (is (= :value (get-in alternatives [reverse :side]))))
      (finally
        (d/close conn)
        (u/delete-files dir)))))

(deftest costed-indexed-date-range-order-test
  (let [dir            (u/tmp-dir
                         (str "indexed-date-range-order-" (UUID/randomUUID)))
        schema         {:message/hasCreator
                        {:db/valueType :db.type/ref}
                        :message/creationDate
                        {:db/valueType :db.type/instant}}
        conn           (d/get-conn dir schema)
        creator-clause '[?message :message/hasCreator ?person]
        date-clause    '[?message :message/creationDate ?date]
        lower-clause   '[(<= #inst "2020-01-01T00:00:00.000-00:00" ?date)]
        upper-clause   '[(< ?date #inst "2021-01-01T00:00:00.000-00:00")]
        clauses        [creator-clause date-clause lower-clause upper-clause]
        parsed-q       (dp/parse-query {:find  ['?message '?person]
                                        :where clauses})
        resolve-late   @(ns-resolve 'datalevin.query.execute
                                    'resolve-late-clauses)
        choose-late    @(ns-resolve 'datalevin.query.execute
                                    'cheaper-late-producer)
        make-context
        (fn [people]
          {:sources  {'$ (d/db conn)}
           :rules    nil
           :parsed-q parsed-q
           :rels
           [(r/relation!
              {'?person 0}
              (FastList. ^java.util.Collection
                         (mapv #(object-array [%]) people)))]})
        run
        (fn [people]
          (let [explain (volatile! {})
                context (make-context people)
                result  (binding [qplan/*explain* explain
                                  qu/*implicit-source* (get-in context
                                                               [:sources '$])]
                          (resolve-late context clauses))
                rel     (if (< 1 (count (:rels result)))
                          (reduce j/hash-join (:rels result))
                          (first (:rels result)))
                attrs   (:attrs rel)]
            {:decision (first (:late-clause-decisions @explain))
             :order    (:late-clauses result)
             :rows     (into #{}
                             (map (fn [^objects tuple]
                                    [(aget tuple (long (attrs '?message)))
                                     (aget tuple (long (attrs '?person)))]))
                             (:tuples rel))}))]
    (try
      (d/transact!
        conn
        (concat
          [{:db/id 1000 :message/hasCreator 10
            :message/creationDate
            #inst "2020-01-01T00:00:00.000-00:00"}
           {:db/id 1001 :message/hasCreator 10
            :message/creationDate
            #inst "2020-06-01T00:00:00.000-00:00"}
           {:db/id 1002 :message/hasCreator 10
            :message/creationDate
            #inst "2021-01-01T00:00:00.000-00:00"}
           {:db/id 2000 :message/hasCreator 11
            :message/creationDate
            #inst "2020-03-01T00:00:00.000-00:00"}
           {:db/id 2001 :message/hasCreator 11
            :message/creationDate
            #inst "2020-12-01T00:00:00.000-00:00"}]
          (map (fn [^long id]
                 {:db/id id :message/hasCreator 10
                  :message/creationDate
                  #inst "2019-01-01T00:00:00.000-00:00"})
               (range 1003 1020))))
      (let [wide          (run [10 11])
            narrow        (run [11])
            wide-choice   (:decision wide)
            narrow-choice (:decision narrow)]
        (testing "a selective AVE date range runs before creator fanout"
          (is (= :indexed-range-first (:strategy wide-choice)))
          (is (= 22 (:pattern-fanout wide-choice)))
          (is (= 4 (:range-fanout wide-choice)))
          (is (= [date-clause lower-clause upper-clause creator-clause]
                 (:order wide)))
          (is (= #{[1000 10] [1001 10] [2000 11] [2001 11]}
                 (:rows wide))))
        (testing "a selective creator binding remains creator-first"
          (is (= :bound-pattern-first (:strategy narrow-choice)))
          (is (= 2 (:pattern-fanout narrow-choice)))
          (is (= 4 (:range-fanout narrow-choice)))
          (is (= creator-clause (first (:order narrow))))
          (is (= #{[2000 11] [2001 11]} (:rows narrow))))
        (testing "the range variable must be fully consumed and not returned"
          (let [output-context
                (assoc (make-context [10 11])
                       :parsed-q
                       (dp/parse-query {:find  ['?message '?date]
                                        :where clauses}))]
            (is (nil? (choose-late output-context clauses))))
          (is (nil?
                (with-redefs [db/-index-range-size
                              (fn [& _]
                                (throw (ex-info "unexpected range count" {})))]
                  (choose-late (make-context [10 11])
                               [creator-clause date-clause lower-clause]))))))
      (finally
        (d/close conn)
        (u/delete-files dir)))))

(deftest costed-indexed-scalar-range-order-test
  (doseq [{:keys [label value-type lower upper values outside]}
          [{:label      "long"
            :value-type :db.type/long
            :lower      10
            :upper      20
            :values     [10 12 15 19]
            :outside    0}
           {:label      "string"
            :value-type :db.type/string
            :lower      "b"
            :upper      "d"
            :values     ["b" "bc" "c" "cz"]
            :outside    "a"}
           {:label      "bytes"
            :value-type :db.type/bytes
            :lower      (byte-array [1])
            :upper      (byte-array [4])
            :values     [(byte-array [1]) (byte-array [2])
                         (byte-array [3]) (byte-array [3 1])]
            :outside    (byte-array [0])}]]
    (testing label
      (let [dir          (u/tmp-dir
                           (str "indexed-scalar-range-order-"
                                (UUID/randomUUID)))
            schema       {:item/owner {:db/valueType :db.type/ref}
                          :item/rank  {:db/valueType value-type}}
            conn         (d/get-conn dir schema)
            owner-clause '[?item :item/owner ?owner]
            range-clause '[?item :item/rank ?rank]
            lower-clause [(list '<= lower '?rank)]
            upper-clause [(list '< '?rank upper)]
            clauses      [owner-clause range-clause
                          lower-clause upper-clause]
            parsed-q     (dp/parse-query {:find  ['?item '?owner]
                                          :where clauses})
            resolve-late @(ns-resolve 'datalevin.query.execute
                                      'resolve-late-clauses)
            make-context
            (fn []
              {:sources  {'$ (d/db conn)}
               :rules    nil
               :parsed-q parsed-q
               :rels
               [(r/relation!
                  {'?owner 0}
                  (FastList. ^java.util.Collection
                             (mapv #(object-array [%]) [10 11])))]})]
        (try
          (d/transact!
            conn
            (concat
              [(assoc {:db/id 1000 :item/owner 10} :item/rank (values 0))
               (assoc {:db/id 1001 :item/owner 10} :item/rank (values 1))
               (assoc {:db/id 1002 :item/owner 10} :item/rank upper)
               (assoc {:db/id 2000 :item/owner 11} :item/rank (values 2))
               (assoc {:db/id 2001 :item/owner 11} :item/rank (values 3))]
              (map (fn [^long id]
                     {:db/id id :item/owner 10 :item/rank outside})
                   (range 1003 1020))))
          (let [explain (volatile! {})
                context (make-context)
                result  (binding [qplan/*explain* explain
                                  qu/*implicit-source* (get-in context
                                                               [:sources '$])]
                          (resolve-late context clauses))
                rel     (if (< 1 (count (:rels result)))
                          (reduce j/hash-join (:rels result))
                          (first (:rels result)))
                attrs   (:attrs rel)
                choice  (first (:late-clause-decisions @explain))
                rows    (into #{}
                              (map (fn [^objects tuple]
                                     [(aget tuple (long (attrs '?item)))
                                      (aget tuple (long (attrs '?owner)))]))
                              (:tuples rel))]
            (is (= :indexed-range-first (:strategy choice)))
            (is (= 22 (:pattern-fanout choice)))
            (is (= 4 (:range-fanout choice)))
            (is (= [range-clause lower-clause upper-clause owner-clause]
                   (:late-clauses result)))
            (is (= #{[1000 10] [1001 10] [2000 11] [2001 11]} rows)))
          (finally
            (d/close conn)
            (u/delete-files dir)))))))

(deftest exact-inequality-range-capability-test
  (testing "ordinary ordered scalar values can use exact AVE bounds"
    (is (qor/exact-inequality-range? :db.type/instant))
    (is (qor/exact-inequality-range? :db.type/long))
    (is (qor/exact-inequality-range? :db.type/string))
    (is (qor/exact-inequality-range? :db.type/bytes)))
  (testing "BigDecimal retains its exact residual predicate"
    (is (not (qor/exact-inequality-range? :db.type/bigdec)))))

(deftest forkable-predicate-test
  (let [instances (atom 0)
        pred      (qpred/forkable-predicate
                    (fn []
                      (let [instance (swap! instances inc)
                            args     (object-array [nil 10])]
                        (fn [v]
                          (aset args 0 v)
                          [(aget args 0) (aget args 1) instance]))))
        fork-1    (qpred/fork-predicate pred)
        fork-2    (qpred/fork-predicate pred)]
    (is (qpred/forkable-predicate? pred))
    (is (= [:a 10 2] (fork-1 :a)))
    (is (= [:b 10 3] (fork-2 :b)))
    (is (= 3 @instances))
    (testing "composition forks both child predicates"
      (let [upper    (qpred/shareable-predicate #(< ^long % 10))
            positive (qpred/shareable-predicate #(pos? ^long %))
            combined (qor/add-pred upper positive)
            forked   (qpred/fork-predicate combined)]
        (is (qpred/forkable-predicate? combined))
        (is (forked 5))
        (is (not (forked 0)))
        (is (not (forked 12)))))
    (testing "composition with an opaque predicate remains opaque"
      (is (not (qpred/forkable-predicate?
                 (qor/add-pred pred (fn [_] true))))))))

(deftest activated-variable-predicate-is-forkable-test
  (let [pred (qor/activate-var-pred
               {:make-call qresolve/make-call
                :resolve-pred qresolve/resolve-pred}
               '?value '(< ?value 10))
        fork-1 (qpred/fork-predicate pred)
        fork-2 (qpred/fork-predicate pred)]
    (is (qpred/forkable-predicate? pred))
    (is (fork-1 9))
    (is (not (fork-2 10)))))

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
        (testing "a standalone non-unique lookup remains a planner clause"
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

(deftest costed-selective-value-pre-materialization-test
  (let [dir    (u/tmp-dir (str "selective-value-anchor-"
                               (UUID/randomUUID)))
        schema {:start/id            {:db/valueType :db.type/long
                                      :db/unique :db.unique/identity}
                :knows/from          {:db/valueType :db.type/ref}
                :knows/to            {:db/valueType :db.type/ref}
                :tag/name            {:db/valueType :db.type/string}
                :message/hasTag      {:db/valueType :db.type/ref}
                :message/hasCreator  {:db/valueType :db.type/ref}
                :message/marker      {:db/valueType :db.type/keyword}}
        conn   (d/get-conn dir schema)
        people (range 100 110)
        messages
        (map-indexed
          (fn [idx person]
            {:db/id (+ 10000 (long idx))
             :message/hasCreator person
             :message/hasTag (cond
                               (= idx 0) 9001
                               (= idx 1) 9002
                               :else     9999)
             :message/marker :present})
          (take 100 (cycle people)))]
    (try
      (d/transact! conn
                   (into [{:db/id 1 :start/id 1}
                          {:db/id 9001 :tag/name "needle"}
                          {:db/id 9002 :tag/name "needle"}
                          {:db/id 9999 :tag/name "common"}]
                         (concat
                           (map-indexed
                             (fn [idx person]
                               {:db/id (+ 2000 (long idx))
                                :knows/from 1
                                :knows/to person})
                             people)
                           messages)))
      (let [db-value       (d/db conn)
            selected       (selectively-materialized-context
                             db-value selective-tag-anchor-query)
            tag-rel        (some #(when (contains? (:attrs %) '?tag) %)
                                 (:rels selected))
            remaining      (set (get-in selected
                                        [:parsed-q :qorig-where]))
            selective-plan (d/explain {:run? false}
                                      selective-tag-anchor-query db-value)
            selective-pick (first
                             (:pre-materialization-decisions selective-plan))
            common-query   (walk/postwalk-replace
                             {"needle" "common"}
                             selective-tag-anchor-query)
            common-plan    (d/explain {:run? false} common-query db-value)
            common-pick    (first
                             (:pre-materialization-decisions common-plan))]
        (testing "all entities for a selective non-unique value are retained"
          (is (= 2 (:fanout selective-pick)))
          (is (= :pre-materialized-value-lookup
                 (:strategy selective-pick)))
          (is (< (:candidate-cost selective-pick)
                 (:baseline-cost selective-pick)))
          (is (= 2 (.size ^List (:tuples tag-rel))))
          (is (not (contains? remaining '[?tag :tag/name "needle"])))
          (is (= #{[10000] [10001]}
                 (d/q selective-tag-anchor-query db-value))))
        (testing "downstream work can stop a point-value trial"
          (is (= 1 (:fanout common-pick)))
          (is (= :planner-value-lookup (:strategy common-pick)))
          (is (nil? (:candidate-cost common-pick)))
          (is (= :propagation-preflight
                 (get-in common-pick [:guardrail :phase])))
          (is (= (:lookup-cost common-pick)
                 (:materialization-cost common-pick)))
          (is (<= (get-in common-pick [:guardrail :budget])
                  (+ (double
                       (get-in common-pick
                               [:guardrail :accumulated-cost]))
                     (double
                       (get-in common-pick
                               [:guardrail :projected-cost])))))
          (let [common-context (selectively-materialized-context
                                 db-value common-query)]
            (is (contains?
                  (set (get-in common-context [:parsed-q :qorig-where]))
                  '[?tag :tag/name "common"])))))
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

(deftest input-bound-pre-materialization-regression-test
  (let [dir    (u/tmp-dir (str "input-bound-pre-materialization-"
                               (UUID/randomUUID)))
        schema {:name           {:db/unique :db.unique/identity}
                :friend         {:db/valueType :db.type/ref}
                :permission/act {:db/valueType :db.type/keyword}
                :permission/obj {:db/valueType :db.type/keyword}
                :permission/tgt {:db/valueType :db.type/string}}
        db     (d/db-with
                 (d/empty-db dir schema)
                 [{:db/id 1 :name "Ivan" :age 15 :friend 2}
                  {:db/id 2 :name "Petr" :age 22 :friend 3}
                  {:db/id 3 :name "Oleg" :age 33}
                  {:db/id 10
                   :permission/act :alter
                   :permission/obj :database
                   :permission/tgt "other"}])]
    (try
      (testing "two-element existence patterns remain plannable"
        (is (= #{[15]}
               (d/q '[:find ?age
                      :in $ ?name
                      :where
                      [?e :friend]
                      [?e :name ?name]
                      [?e :age ?age]]
                    db "Ivan"))))
      (testing "lookup-ref inputs retain their query values"
        (is (= #{[[:name "Ivan"] 15]
                 [[:name "Petr"] 22]}
               (d/q '[:find ?e ?age
                      :in $ [?e ...]
                      :where
                      [?e :age ?age]]
                    db [[:name "Ivan"] [:name "Petr"]])))
        (is (= #{[1 [:name "Petr"]]
                 [2 [:name "Oleg"]]}
               (d/q '[:find ?e ?friend
                      :in $ [?friend ...]
                      :where
                      [?e :friend ?friend]]
                    db [[:name "Petr"] [:name "Oleg"]]))))
      (testing "cost estimation accepts an empty materialized relation"
        (is (nil?
              (d/q '[:find ?permission .
                     :in $ ?act ?obj ?target
                     :where
                     [?permission :permission/act ?act]
                     [?permission :permission/obj ?obj]
                     [?permission :permission/tgt ?target]]
                   db :alter :database "missing"))))
      (finally
        (d/close-db db)
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

(deftest zero-cardinality-linked-node-test
  (let [dir  (u/tmp-dir (str "zero-linked-node-" (UUID/randomUUID)))
        conn (d/get-conn
               dir {:node/title {:db/unique :db.unique/identity}
                    :block/refs {:db/valueType   :db.type/ref
                                 :db/cardinality :db.cardinality/many}})]
    (try
      (d/transact! conn [{:db/id 1 :node/title "Present"}
                         {:db/id 2 :block/refs 1}])
      (let [query '[:find ?block
                    :where
                    [?page :node/title "Missing"]
                    [?block :block/refs ?page]]
            db    (d/db conn)]
        (is (= #{} (d/q query db)))
        (is (= 0 (:actual-result-size
                   (d/explain {:run? true} query db))))
        (is (nil? (:plan (d/explain {:run? false} query db)))))
      (finally
        (d/close conn)
        (u/delete-files dir)))))

(deftest known-entity-bound-clause-test
  (let [dir (u/tmp-dir (str "known-entity-bound-" (UUID/randomUUID)))
        db  (-> (d/empty-db
                  dir {:name {:db/unique :db.unique/identity}
                       :aka  {:db/cardinality :db.cardinality/many}})
                (d/db-with [{:db/id 1 :name "Ivan" :age 15
                             :aka ["robot" "ai"]}]))
        query '[:find ?a ?v
                :in $ ?e ?alias
                :where
                [?e ?a ?v]
                [?e :aka ?alias]]]
    (try
      (testing "a true literal clause seeds late dynamic-attribute work"
        (is (= #{[:name "Ivan"] [:age 15]
                 [:aka "robot"] [:aka "ai"]}
               (d/q query db 1 "ai")))
        (is (= 4 (:actual-result-size
                   (d/explain {:run? true} query db 1 "ai")))))
      (testing "the known entity is still checked against the literal"
        (is (= #{} (d/q query db 1 "missing"))))
      (testing "known-entity value predicates are applied during EAV scan"
        (is (= #{}
               (d/q '[:find ?age
                      :in $ ?e
                      :where
                      [?e :age ?age]
                      [(> ?age 20)]]
                    db 1))))
      (finally
        (d/close-db db)
        (u/delete-files dir)))))

(deftest multi-key-join-cardinality-test
  (let [estimate-size @(ns-resolve 'datalevin.query-optimizer
                                   'multi-key-result-size)
        prev-plan     {:steps [{:cols ['?left
                                       #{:left/a '?a}
                                       #{:left/b '?b}]}]}
        target-plan   {:steps [{:cols ['?right
                                       #{:right/a '?a}
                                       #{:right/b '?b}]}]}
        single-target {:steps [{:cols ['?right #{:right/a '?a}]}]}
        link          {:type :val-eq :var '?a}]
    (with-redefs [db/-cardinality (fn [_ attr]
                                    (if (= attr :right/b) 10000 1))]
      (testing "an unrepresented equality key dampens the join fanout"
        (is (= 10000
               (estimate-size nil '?left link prev-plan target-plan
                              1000000))))
      (testing "the selected graph key is not counted twice"
        (is (= 1000000
               (estimate-size nil '?left link prev-plan single-target
                              1000000)))))))

(deftest merge-range-predicate-cost-test
  (let [merge-pred-options @(ns-resolve 'datalevin.query-optimizer
                                        'merge-pred-options)
        estimate-cost      @(ns-resolve 'datalevin.query-optimizer
                                        'estimate-scan-v-cost)
        interval           [[:open 10] [:closed c/vmax]]
        range-options      (merge-pred-options '?date
                                               {:range [interval]})
        residual-options   (merge-pred-options '?date
                                               {:range [interval]
                                                :pred  (constantly true)})
        disjoint-options   (merge-pred-options
                             '?date
                             {:range [interval
                                      [[:closed 1] [:closed 2]]]})]
    (testing "only a single pure synthesized range retains provenance"
      (is (true? (:range-pred? range-options)))
      (is (nil? (:range-pred? residual-options)))
      (is (nil? (:range-pred? disjoint-options))))
    (binding [c/magic-cost-merge-scan-v 2.0
              c/magic-cost-var          1.0
              c/magic-cost-pred         3.0
              c/magic-cost-fidx         1.0]
      (testing "a pure range check does not multiply the whole merge scan"
        (is (= 20.0 (estimate-cost {:attrs-v [[:date range-options]]
                                    :vars    []}
                                   10))))
      (testing "a residual predicate keeps the existing predicate factor"
        (is (= 60.0 (estimate-cost {:attrs-v [[:date residual-options]]
                                    :vars    []}
                                   10)))))))

(deftest deferred-eav-attribute-group-planning-test
  (let [dir    (u/tmp-dir (str "deferred-eav-group-" (UUID/randomUUID)))
        schema {:start/id          {:db/valueType :db.type/long
                                    :db/unique    :db.unique/identity}
                :edge/from         {:db/valueType :db.type/ref}
                :edge/to           {:db/valueType :db.type/ref}
                :membership/person {:db/valueType :db.type/ref}
                :membership/forum  {:db/valueType :db.type/ref}
                :membership/date   {:db/valueType :db.type/long}
                :post/person       {:db/valueType :db.type/ref}
                :post/forum        {:db/valueType :db.type/ref}
                :forum/id          {:db/valueType :db.type/long
                                    :db/unique    :db.unique/identity}
                :forum/title       {:db/valueType :db.type/string}}
        conn   (d/get-conn dir schema)
        person-count (long 20)
        forum-count  (long 10)]
    (try
      (d/transact!
        conn
        (into [{:db/id 1 :start/id 1}]
              (concat
                (map (fn [^long i]
                       {:db/id (+ 2000 i)
                        :forum/id i
                        :forum/title (str "forum-" i)})
                     (range forum-count))
                (map (fn [^long i]
                       {:db/id (+ 10000 i)
                        :edge/from 1
                        :edge/to (+ 1000 i)})
                     (range person-count))
                (for [^long i (range person-count)
                      ^long j (range forum-count)]
                  {:db/id (+ 100000 (* i forum-count) j)
                   :membership/person (+ 1000 i)
                   :membership/forum (+ 2000 j)
                   :membership/date (inc j)})
                (map (fn [^long i]
                       {:db/id (+ 200000 i)
                        :post/person (+ 1000 i)
                        :post/forum (+ 2000 (long (mod i forum-count)))})
                     (range person-count)))))
      (let [db-value (db/-clear-tx-cache (d/db conn))
            common-bindings
            {#'c/init-exec-size-threshold 5
             #'c/hash-join-min-input-size 1
             #'c/magic-cost-hash-join 0.1}]
        (with-bindings (assoc common-bindings
                              #'c/deferred-eav-min-cost-improvement 1.0)
          (let [explain  (d/explain {:run? false}
                                    deferred-eav-group-query db-value)
                planning (-> explain :attribute-group-planning vals first first)]
            (testing "a high required improvement retains the eager plan"
              (is (= :eager (:selected planning)))
              (is (= [:membership/date]
                     (get-in planning [:groups 0 :attrs]))))))
        (.clear ^datalevin.utl.LRUCache qo/*plan-cache*)
        (with-bindings (assoc common-bindings
                              #'c/deferred-eav-min-cost-improvement 0.0)
          (let [explain  (d/explain {:run? true}
                                    deferred-eav-group-query db-value)
                planning (-> explain :attribute-group-planning vals first first)
                result   (d/q deferred-eav-group-query db-value)]
            (testing "the fixed join DAG can place a legal group after hash"
              (is (= :deferred (:selected planning)))
              (is (true? (get-in planning
                                 [:groups 0 :after-hash-join?])))
              (is (= 20 (:actual-result-size explain))))
            (testing "late cardinality-one filtering preserves query results"
              (is (= 20 (count result)))
              (is (contains? result [0 "forum-0" 200000]))))))
      (finally
        (d/close conn)
        (u/delete-files dir)))))

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

(deftest bounded-access-sample-cost-test
  (let [dir    (u/tmp-dir (str "bounded-access-sample-"
                               (UUID/randomUUID)))
        schema {:person/id       {:db/valueType :db.type/long
                                  :db/unique    :db.unique/identity}
                :knows/from      {:db/valueType :db.type/ref}
                :knows/to        {:db/valueType :db.type/ref}
                :message/creator {:db/valueType :db.type/ref}
                :message/id      {:db/valueType :db.type/long
                                  :db/unique    :db.unique/identity}
                :message/date    {:db/valueType :db.type/long}}
        conn   (d/get-conn dir schema)
        creator-count        (long 50)
        messages-per-creator (long 21)
        knows-per-creator    (long 30)
        friend-count         (long 20)]
    (try
      (d/transact!
        conn
        (into [{:db/id 1 :person/id 1}]
              (concat
                (map (fn [^long i]
                       {:db/id (+ 1000 i) :person/id (+ 100 i)})
                     (range creator-count))
                (map (fn [^long i]
                       {:db/id            (+ 10000 i)
                        :message/id       i
                        :message/date     i
                        :message/creator  (+ 1000
                                             (long (mod i creator-count)))})
                     (range (* creator-count messages-per-creator)))
                (for [^long i (range creator-count)
                      ^long k (range knows-per-creator)]
                  {:db/id      (+ 100000 (* i knows-per-creator) k)
                   :knows/from (if (and (< i friend-count) (zero? k))
                                 1
                                 (+ 1000000 (* i knows-per-creator) k))
                   :knows/to   (+ 1000 i)}))))
      (let [db-value  (db/-clear-tx-cache (d/db conn))
            explain   (d/explain {:run? false}
                                 bounded-access-sample-query
                                 db-value 1 2000)
            preferred (:preferred-access-plan explain)
            abort     (get-in preferred [:estimate :sampling-abort])
            result    (d/q bounded-access-sample-query db-value 1 2000)]
        (testing "a projected fanout cannot outspend the conventional plan"
          (is (true? (:unavailable? preferred)))
          (is (= :sample-work-budget (:unavailable-reason preferred)))
          (is (= :access-propagation-preflight (:phase abort)))
          (is (zero? (long (get-in preferred [:estimate :sample-rows]))))
          (is (zero? (double
                       (get-in preferred
                               [:estimate :planning-sample-cost]))))
          (is (= :person/id (:attr abort)))
          (is (< (double (:budget abort))
                 (+ (double (:cost abort))
                    (double (:projected-cost abort)))))
          (is (= :conventional
                 (get-in explain [:selected-plan-alternative :kind]))))
        (testing "aborting the planning sample preserves ordered results"
          (is (= 20 (count result)))
          (is (= [119 1019 1019] (first result)))
          (is (= [100 1000 1000] (peek result)))))
      (finally
        (d/close conn)
        (u/delete-files dir)))))

(deftest terminal-access-count-sample-test
  (let [dir    (u/tmp-dir (str "terminal-access-count-"
                               (UUID/randomUUID)))
        schema {:sample/rank {:db/valueType :db.type/long}
                :sample/keep {:db/valueType :db.type/boolean}
                :sample/tag  {:db/valueType   :db.type/string
                              :db/cardinality :db.cardinality/many}}
        conn   (d/get-conn dir schema)
        query  '[:find ?e ?rank ?tag
                 :in $ ?max-rank
                 :where
                 [?e :sample/rank ?rank]
                 [(<= ?rank ?max-rank)]
                 [?e :sample/keep true]
                 [?e :sample/tag ?tag]
                 :order-by [?rank :desc ?e :asc ?tag :asc]
                 :limit 6]]
    (try
      (d/transact!
        conn
        (mapv
          (fn [^long e]
            (let [tag-count (unchecked-inc (long (mod (quot e 5) 5)))]
              (cond-> {:db/id e :sample/rank e}
                (zero? (rem e 5))
                (assoc :sample/keep true
                       :sample/tag
                       (mapv #(str "tag-" %) (range tag-count))))))
          (range 1 361)))
      (let [db-value  (db/-clear-tx-cache (d/db conn))
            explain   (d/explain {:run? false} query db-value 360)
            preferred (:preferred-access-plan explain)
            counted   (some #(when (= :counted (:sampling %)) %)
                            (get-in preferred [:estimate :join-stages]))]
        (testing "a terminal projection can be sampled without materializing"
          (is (true? (:access-path-selected? explain)))
          (is (= :adaptive-top-k
                 (get-in explain [:selected-plan-alternative :mode])))
          (is (= :sample/tag (:attr counted)))
          (is (pos? (long (:output counted))))
          (is (<= (double
                    (get-in preferred [:estimate :planning-sample-cost]))
                  (double (:conventional-plan-cost explain))))
          (is (= 6 (count (d/q query db-value 360))))))
      (finally
        (d/close conn)
        (u/delete-files dir)))))

(deftest dominated-projection-base-sample-test
  (let [dir    (u/tmp-dir (str "dominated-projection-root-"
                               (UUID/randomUUID)))
        schema {:start/id         {:db/valueType :db.type/long
                                   :db/unique    :db.unique/identity}
                :edge/from        {:db/valueType :db.type/ref}
                :edge/to          {:db/valueType :db.type/ref}
                :person/id        {:db/valueType :db.type/long
                                   :db/unique    :db.unique/identity}
                :person/firstName {:db/valueType :db.type/string}
                :person/lastName  {:db/valueType :db.type/string}
                :person/score     {:db/valueType :db.type/long}}
        conn   (d/get-conn dir schema)
        people (mapv (fn [^long i]
                       {:db/id            (+ 1000 i)
                        :person/id        i
                        :person/firstName (str "first-" i)
                        :person/lastName  (str "last-" i)
                        :person/score     i})
                     (range 1200))
        edges  (mapv (fn [^long i]
                       {:db/id     (+ 10000 i)
                        :edge/from 1
                        :edge/to   (+ 1000 i)})
                     (range 29))]
    (try
      (d/transact! conn (into [{:db/id 1 :start/id 1}]
                              (concat people edges)))
      (let [db-value (d/db conn)
            explain  (d/explain {:run? false}
                                dominated-projection-root-query db-value)
            result   (d/q dominated-projection-root-query db-value)
            filtered (d/explain {:run? false}
                                filtered-projection-root-query db-value)]
        (testing "pure output properties defer their global root sample"
          (is (= ['?friend]
                 (get (:deferred-base-samples explain) '$)))
          (is (= 29 (count result)))
          (is (contains? result [0 "first-0" "last-0"]))
          (is (contains? result [28 "first-28" "last-28"])))
        (testing "a projected property used by a predicate is still sampled"
          (is (nil? (:deferred-base-samples filtered)))))
      (finally
        (d/close conn)
        (u/delete-files dir)))))

(deftest post-top-k-property-enrichment-test
  (let [dir    (u/tmp-dir (str "post-top-k-enrichment-"
                               (UUID/randomUUID)))
        schema {:item/id      {:db/valueType :db.type/long
                               :db/unique    :db.unique/identity}
                :item/score   {:db/valueType :db.type/long}
                :item/content {:db/valueType :db.type/string}
                :item/image   {:db/valueType :db.type/string}}
        conn   (d/get-conn dir schema)
        items  (mapv (fn [^long i]
                       (cond-> {:db/id       (+ 100 i)
                                :item/id      i
                                :item/score   i}
                         (< i 28) (assoc :item/content (str "content-" i))
                         (= i 28) (assoc :item/image "image-28")))
                     (range 30))]
    (try
      (d/transact! conn items)
      (let [db-value        (d/db conn)
            explain         (d/explain {:run? true}
                                       post-top-k-enrichment-query db-value)
            result          (d/q post-top-k-enrichment-query db-value)
            offset          (d/q post-top-k-enrichment-offset-query db-value)
            filtering       (d/q filtering-enrichment-query db-value)
            unkeyed         (d/explain {:run? false}
                                       unkeyed-enrichment-query db-value)
            unkeyed-result  (d/q unkeyed-enrichment-query db-value)
            ordered-output  (d/explain
                              {:run? false}
                              ordered-output-enrichment-query db-value)
            enrichment      (:post-top-k-enrichment explain)]
        (testing "the total fallback preserves missing-property rows"
          (is (= [[29 nil 29]
                  [28 "image-28" 28]
                  [27 "content-27" 27]
                  [26 "content-26" 26]
                  [25 "content-25" 25]]
                 result))
          (is (= [[27 "content-27" 27]
                  [26 "content-26" 26]
                  [25 "content-25" 25]]
                 offset)))
        (testing "only the selected top-k rows are enriched"
          (is (= ['get-some-else] (:functions enrichment)))
          (is (= 30 (:candidate-count enrichment)))
          (is (= 5 (:selected-count enrichment)))
          (is (= {:cardinality-preserving true
                  :projection-only true
                  :stable-distinct-key true}
                 (:proof enrichment))))
        (testing "the filtering function retains its original semantics"
          (is (= [28 "image-28" 28] (first filtering)))
          (is (= 5 (count filtering))))
        (testing "deferral requires a projected stable row key"
          (is (nil? (:post-top-k-enrichment unkeyed)))
          (is (= [nil 29] (first unkeyed-result))))
        (testing "deferral rejects outputs referenced by indexed ordering"
          (is (nil? (:post-top-k-enrichment ordered-output)))))
      (finally
        (d/close conn)
        (u/delete-files dir)))))
