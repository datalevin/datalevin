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
   [datalevin.pipe :as p]
   [datalevin.query.predicate :as qpred]
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

(deftest seeded-magic-rule-cardinality-order-test
  (let [cached-rule-rel-size @(ns-resolve 'datalevin.rules
                                          'cached-rule-rel-size)
        reorder-clauses      @(ns-resolve 'datalevin.rules
                                          'reorder-clauses)
        magic-call           '(magic__walk ?message)
        global-clause        '[?message :message/replyOf ?parent]
        seed-rel             (r/relation! {'?magic 0} (tuples [42]))
        context              {:rules
                              {'magic__walk
                               '[[(magic__walk ?message)
                                  [?message :message/replyOf ?parent]
                                  (magic__walk ?parent)]]}
                              :magic-seeds {'magic__walk seed-rel}
                              :rels        []}]
    (testing "an explicit magic seed supplies a stable rule-call estimate"
      (is (= 1 (cached-rule-rel-size context magic-call))))
    (testing "the seed binds the entity before an otherwise global scan"
      (is (= [magic-call global-clause]
             (reorder-clauses [global-clause magic-call] context))))))

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

(deftest adaptive-parallel-scan-participant-count-test
  (let [participant-capacity @(ns-resolve
                                'datalevin.storage
                                'parallel-scan-participant-capacity)]
    (testing "one CPU always stays serial"
      (is (= 1 (participant-capacity 200000 1 11))))
    (testing "the caller participates alongside available pool threads"
      (is (= 2 (participant-capacity 200000 2 1)))
      (is (= 4 (participant-capacity 200000 12 3))))
    (testing "executor capacity and useful work both limit participation"
      (is (= 1 (participant-capacity 200000 12 0)))
      (is (= 1 (participant-capacity 4000 64 63)))
      (is (= 3 (participant-capacity 8001 64 63)))
      (is (= 64 (participant-capacity 1000000 64 63))))))

(deftest batched-presence-filter-test
  (let [dir  (u/tmp-dir (str "batched-presence-" (UUID/randomUUID)))
        conn (d/get-conn
               dir {:item/tags {:db/cardinality :db.cardinality/many}})]
    (try
      (d/transact! conn [{:db/id 1 :item/tags [:a :b]}
                         {:db/id 2 :item/name "no tags"}
                         {:db/id 3 :item/tags [:c]}])
      (let [database (d/db conn)
            input    (tuples [:first 1]
                             [:missing-first 2]
                             [:second 1]
                             [:missing-second 2]
                             [:third 3])]
        (is (= [[:first 1] [:second 1] [:third 3]]
               (mapv vec (db/-eav-filter-presence-list
                           database input 1 :item/tags)))
            "matching duplicates survive and missing duplicates are removed")
        (testing "parallel chunks preserve duplicates split across boundaries"
          (let [parallel-chunks
                (ns-resolve 'datalevin.storage
                            'ordered-parallel-list-chunks)
                input  (tuples [:one-a 1]
                               [:one-b 1]
                               [:one-c 1]
                               [:one-d 1]
                               [:one-e 1]
                               [:missing-a 2]
                               [:missing-b 2]
                               [:missing-c 2]
                               [:three-a 3]
                               [:three-b 3])
                result (parallel-chunks
                         input 3
                         #(db/-eav-filter-presence-list
                            database % 1 :item/tags))]
            (is (= [[:one-a 1] [:one-b 1] [:one-c 1] [:one-d 1] [:one-e 1]
                    [:three-a 3] [:three-b 3]]
                   (mapv vec result))))))
      (finally
        (d/close conn)
        (u/delete-files dir)))))

(deftest batched-eav-merge-scan-test
  (let [participant-count
        @(ns-resolve 'datalevin.storage 'parallel-scan-participant-count)
        parallel-chunks
        @(ns-resolve 'datalevin.storage 'ordered-parallel-list-chunks)
        run-scan
        (fn [database input attrs-v]
          (db/-eav-scan-v-list database input 1 attrs-v))
        run-chunked-scan
        (fn [database participants input attrs-v]
          (parallel-chunks
            input participants
            #(db/-eav-scan-v-list database % 1 attrs-v)))
        repeated-input
        (fn [^long n]
          (let [input (ArrayList. n)]
            (dotimes [i n]
              (.add input (object-array [i 1])))
            input))
        dir  (u/tmp-dir (str "batched-eav-merge-" (UUID/randomUUID)))
        conn (d/get-conn
               dir {:item/tags {:db/cardinality :db.cardinality/many}})]
    (try
      (d/transact! conn [{:db/id 1 :item/name "one" :item/tags [:a :b]}
                         {:db/id 2 :item/name "two"}
                         {:db/id 3 :item/name "three" :item/tags [:c]}])
      (let [database (d/db conn)
            rows     [[:one-a 1]
                      [:one-b 1]
                      [:one-c 1]
                      [:one-d 1]
                      [:one-e 1]
                      [:one-f 1]
                      [:missing 2]
                      [:three-a 3]
                      [:three-b 3]]]
        (testing "parallel single-value chunks match serial order exactly"
          (let [attrs-v  [[:item/name {:skip? false}]]
                serial   (run-scan database (apply tuples rows) attrs-v)
                parallel (run-chunked-scan
                           database 3 (apply tuples rows) attrs-v)]
            (is (= (mapv vec serial) (mapv vec parallel)))
            (is (= 9 (.size ^java.util.List parallel)))))
        (testing "cardinality-many duplicates can cross chunk boundaries"
          (let [attrs-v  [[:item/tags {:skip? false}]]
                serial   (run-scan database (apply tuples rows) attrs-v)
                parallel (run-chunked-scan
                           database 3 (apply tuples rows) attrs-v)]
            (is (= (mapv vec serial) (mapv vec parallel)))
            (is (= 14 (.size ^java.util.List parallel)))))
        (testing "residual predicates stay on the calling thread"
          (let [n       4001
                threads (atom #{})
                caller  (.getName (Thread/currentThread))
                pred    (fn [_]
                          (swap! threads conj
                                 (.getName (Thread/currentThread)))
                          true)]
            (is (= n (.size ^java.util.List
                            (run-scan
                              database (repeated-input n)
                              [[:item/name {:skip? false :pred pred}]]))))
            (is (= #{caller} @threads))))
        (testing "forkable predicates create one instance per scan chunk"
          (let [n             4001
                ^long participants (participant-count n)
                factory-count (atom 0)
                instances    (atom #{})
                pred
                (qpred/forkable-predicate
                  (fn []
                    (let [instance (Object.)]
                      (swap! factory-count inc)
                      (fn [_]
                        (swap! instances conj instance)
                        true))))
                result
                (run-scan
                  database (repeated-input n)
                  [[:item/name {:skip? false :pred pred}]])]
            (is (= n (.size ^java.util.List result)))
            (is (= (inc participants) @factory-count)
                "one initial predicate plus one predicate per scan chunk")
            (is (= participants (count @instances))))))
      (finally
        (d/close conn)
        (u/delete-files dir)))))

(deftest ave-exact-membership-filter-test
  (let [dir  (u/tmp-dir (str "ave-exact-membership-" (UUID/randomUUID)))
        conn (d/get-conn dir {:edge/to {:db/valueType :db.type/ref}})]
    (try
      (d/transact! conn [{:db/id 100 :node/name "target"}
                         {:db/id 101 :node/name "other target"}
                         {:db/id 1 :edge/to 100}
                         {:db/id 2 :edge/to 100}
                         {:db/id 3 :edge/to 101}
                         {:db/id 4 :node/name "no edge"}])
      (let [database (d/db conn)]
        (testing "a fixed entity is appended only for exact AVE membership"
          (let [input (tuples [100 :first]
                              [102 :missing-value]
                              [100 :duplicate]
                              [101 :wrong-bound])]
            (is (= [[100 :first 1] [100 :duplicate 1]]
                   (mapv vec
                         (db/-val-eq-scan-e-list
                           database input 0 :edge/to 1))))))
        (testing "tuple entities filter exact pairs while preserving payloads"
          (let [input (tuples [100 1 :first]
                              [100 3 :wrong-entity]
                              [100 2 :second]
                              [101 3 :third]
                              [101 2 :wrong-value]
                              [102 1 :missing-value])]
            (is (= [[100 1 :first] [100 2 :second] [101 3 :third]]
                   (mapv vec
                         (db/-val-eq-filter-e-list
                           database input 0 :edge/to 1))))))
        (testing "streaming variants use the same exact membership semantics"
          (let [bound-out  (ArrayList.)
                filter-out (ArrayList.)]
            (db/-val-eq-scan-e
              database
              (p/list-tuple-pipe (tuples [100 :hit] [101 :miss]))
              bound-out 0 :edge/to 1)
            (db/-val-eq-filter-e
              database
              (p/list-tuple-pipe
                (tuples [100 2 :hit] [100 3 :miss] [101 3 :hit]))
              filter-out 0 :edge/to 1)
            (is (= [[100 :hit 1]] (mapv vec bound-out)))
            (is (= [[100 2 :hit] [101 3 :hit]]
                   (mapv vec filter-out))))))
      (finally
        (d/close conn)
        (u/delete-files dir)))))

(deftest bound-presence-semi-join-test
  (let [filter-presence @(ns-resolve 'datalevin.query.resolve
                                     'filter-bound-entity-presence)
        dir             (u/tmp-dir (str "presence-semi-join-"
                                        (UUID/randomUUID)))
        conn            (d/get-conn dir {:item/key    {:db/unique
                                                       :db.unique/identity}
                                        :item/active {}})]
    (try
      (d/transact! conn [{:db/id 1 :item/key "one" :item/active true}
                         {:db/id 2 :item/key "two" :item/name "inactive"}
                         {:db/id 3 :item/key "three" :item/active false}])
      (let [database (d/db conn)
            rel      (r/relation! {'?item 0 '?payload 1}
                                  (tuples [1 :a] [1 :b] [2 :c] [3 :d]))
            context  {:rels             [rel]
                      :rels-bound-cache (volatile! {})}
            result   (binding [c/magic-cost-link-probe      0.0
                               c/magic-cost-link-retrieval  0.0
                               c/magic-cost-init-scan-e     100.0
                               c/magic-cost-hash-join       0.0]
                       (filter-presence
                         context database '[?item :item/active _]))]
        (is (= {'?item 0 '?payload 1} (-> result :rels first :attrs)))
        (is (= [[1 :a] [1 :b] [3 :d]]
               (rows (first (:rels result)))))
        (testing "lookup refs retain their original query values"
          (is (= #{[[:item/key "one"]]}
                 (d/q '[:find ?item
                        :in $ [?item ...]
                        :where
                        [?item :item/active _]]
                      database
                      [[:item/key "one"] [:item/key "two"]])))))
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
