(ns datalevin.query-optimizer-test
  (:require
   [clojure.test :refer [deftest is testing]]
   [datalevin.constants :as c]
   [datalevin.core :as d]
   [datalevin.db :as db]
   [datalevin.parser :as dp]
   [datalevin.query-optimizer :as qo]
   [datalevin.query.plan :as qplan]
   [datalevin.query.resolve :as qresolve]
   [datalevin.util :as u])
  (:import
   [java.util List UUID]))

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

(defn- materialized-context
  [db-value query]
  (let [parsed-q          (dp/parse-query query)
        [parsed-q inputs] (qo/plugin-inputs parsed-q [db-value])]
    (-> (qplan/make-context parsed-q false)
        (qresolve/resolve-ins inputs)
        (qo/materialize-input-bound-patterns))))

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
