;;
;; Copyright (c) Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns datalevin.query-not-test
  (:require
   [clojure.test :refer [deftest is testing]]
   [datalevin.constants :as c]
   [datalevin.core :as d]
   [datalevin.db :as db]
   [datalevin.relation :as r]
   [datalevin.rules]
   [datalevin.util :as u])
  (:import
   [java.util ArrayList UUID]))

(defn- tuples
  [& rows]
  (let [result (ArrayList.)]
    (doseq [row rows]
      (.add result (object-array row)))
    result))

(defn- rows
  [rel]
  (mapv vec (:tuples rel)))

(deftest rule-bound-value-cardinality-order-test
  (let [estimate-clause-size @(ns-resolve 'datalevin.rules
                                          'estimate-clause-size)
        reorder-clauses      @(ns-resolve 'datalevin.rules
                                          'reorder-clauses)
        dir                  (u/tmp-dir
                               (str "rule-bound-value-cost-"
                                    (UUID/randomUUID)))
        conn                 (d/get-conn
                               dir
                               {:edge/from {:db/valueType :db.type/long}
                                :edge/to   {:db/valueType :db.type/long}})
        middle-values        (vec (range 1000 1012))
        target               4242
        bridge-clause        '[?edge :edge/from ?middle]
        endpoint-clause      '[?edge :edge/to ?target]]
    (try
      (d/transact!
        conn
        (mapv (fn [^long i middle]
                {:db/id     (+ 100 i)
                 :edge/from middle
                 :edge/to   (if (zero? i) target (+ 5000 i))})
              (range (count middle-values)) middle-values))
      (let [database (d/db conn)
            context  {:sources {'$ database}
                      :rels
                      [(r/relation! {'?middle 0}
                                    (apply tuples (map vector middle-values)))
                       (r/relation! {'?target 0}
                                    (tuples [target]))]}
            bound    #{'?middle '?target}]
        (testing "small materialized domains produce exact fan-out estimates"
          (is (= 12 (estimate-clause-size bridge-clause context bound)))
          (is (= 1 (estimate-clause-size endpoint-clause context bound))))
        (testing "the selective endpoint wins an equal-connectivity tie"
          (is (= [endpoint-clause bridge-clause]
                 (reorder-clauses [bridge-clause endpoint-clause]
                                  context)))))
      (finally
        (d/close conn)
        (u/delete-files dir)))))

(deftest multi-lookup-cost-selection-test
  (let [cheaper? @(ns-resolve 'datalevin.query.resolve
                              'multi-lookup-cheaper?)]
    (binding [c/magic-link-ratio           1.0
              c/magic-cost-link-probe      2.5
              c/magic-cost-link-retrieval  1.2
              c/magic-cost-init-scan-e     2.0
              c/magic-cost-hash-join       60.0]
      (testing "indexed probes win when the attribute scan is much larger"
        (is (cheaper? 110918 3509083)))
      (testing "a small attribute scan wins over many indexed probes"
        (is (not (cheaper? 150000 1000)))
        (is (not (cheaper? 1000 1000))))
      (testing "the safety limit is independent of the cost comparison"
        (is (not (cheaper? 1000001 100000000)))))))

(deftest wildcard-multi-lookup-uses-existence-test
  (let [lookup @(ns-resolve 'datalevin.query.resolve
                            'lookup-pattern-multi-entity)
        dir    (u/tmp-dir (str "wildcard-existence-" (UUID/randomUUID)))
        conn   (d/get-conn
                 dir {:item/tags {:db/cardinality :db.cardinality/many}})]
    (try
      (d/transact! conn [{:db/id 1 :item/tags [:a :b]}
                         {:db/id 2 :item/name "no tags"}])
      (let [database (d/db conn)
            wildcard (lookup database ['?item :item/tags '_]
                             [[1 1] [2 2]] true)
            values   (lookup database ['?item :item/tags '?tag]
                             [[1 1] [2 2]] true)
            exact    (lookup database ['?item :item/tags :a]
                             [[1 1] [2 2]] false)]
        (is (db/-ea-populated? database 1 :item/tags))
        (is (nil? (db/-ea-populated? database 2 :item/tags)))
        (is (= [[1]] (mapv vec wildcard)))
        (is (every? #(= 1 (alength ^objects %)) wildcard))
        (is (= #{[1 :a] [1 :b]} (set (mapv vec values))))
        (is (= [[1]] (mapv vec exact))))
      (finally
        (d/close conn)
        (u/delete-files dir)))))

(deftest project-distinct-test
  (let [rel (r/relation! {'?entity 0 '?group 1 '?kind 2}
                         (tuples [1 "g" :x]
                                 [2 "g" :x]
                                 [3 "g" :y]
                                 [4 "h" :x]))]
    (testing "one-column keys are physically projected and deduplicated"
      (let [projected (r/project-distinct rel ['?group])]
        (is (= {'?group 0} (:attrs projected)))
        (is (= [["g"] ["h"]] (rows projected)))
        (is (every? #(= 1 (alength ^objects %)) (:tuples projected)))))
    (testing "compound keys retain their declared column order"
      (let [projected (r/project-distinct rel ['?kind '?group])]
        (is (= {'?kind 0 '?group 1} (:attrs projected)))
        (is (= [[:x "g"] [:y "g"] [:x "h"]]
               (rows projected)))
        (is (every? #(= 2 (alength ^objects %)) (:tuples projected)))))))

(deftest not-join-distinct-key-semantics-test
  (let [dir  (u/tmp-dir (str "not-join-distinct-" (UUID/randomUUID)))
        conn (d/get-conn dir)]
    (try
      (d/transact!
        conn
        [{:db/id 1 :item/group "keep" :item/kind :x}
         {:db/id 2 :item/group "drop" :item/kind :x}
         {:db/id 3 :item/group "drop" :item/kind :x}
         {:db/id 4 :item/group "drop" :item/kind :y}
         {:db/id 5 :item/group "keep" :item/kind :x}
         {:db/id 101 :block/group "drop" :block/kind :x :block/score 1}
         {:db/id 102 :block/group "drop" :block/kind :x :block/score 2}])
      (let [db (d/db conn)]
        (testing "a duplicate single key excludes every matching outer tuple"
          (is (= #{[1 "keep"] [5 "keep"]}
                 (set
                   (d/q
                     '[:find ?item ?group
                       :where
                       [?item :item/group ?group]
                       (not-join [?group]
                         [?block :block/group ?group])]
                     db)))))
        (testing "compound keys exclude only the matching key combination"
          (is (= #{[1 "keep" :x] [4 "drop" :y] [5 "keep" :x]}
                 (set
                   (d/q
                     '[:find ?item ?group ?kind
                       :where
                       [?item :item/group ?group]
                       [?item :item/kind ?kind]
                       (not-join [?group ?kind]
                         [?block :block/group ?group]
                         [?block :block/kind ?kind])]
                     db)))))
        (testing "complex not-join clauses use the same key semantics"
          (is (= #{[1 "keep"] [5 "keep"]}
                 (set
                   (d/q
                     '[:find ?item ?group
                       :where
                       [?item :item/group ?group]
                       (not-join [?group]
                         [?block :block/group ?group]
                         [?block :block/score ?score]
                         [(> ?score 0)])]
                     db))))))
      (finally
        (d/close conn)
        (u/delete-files dir)))))
