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
   [datalevin.join :as j]
   [datalevin.pipe :as p]
   [datalevin.query :as dq]
   [datalevin.query.aggregate :as qagg]
   [datalevin.query.plan]
   [datalevin.query.predicate :as qpred]
   [datalevin.query.resolve :as qresolve]
   [datalevin.query-util :as qu]
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

(defn ground
  "Test query function whose qualified name must not be treated as the
  built-in `ground` by planner proofs."
  [_]
  1)

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

(deftest large-multi-entity-lookup-uses-merged-eav-test
  (let [lookup        @(ns-resolve 'datalevin.query.resolve
                                   'lookup-pattern-multi-entity)
        resolve-pairs @(ns-resolve 'datalevin.query.resolve
                                   'resolve-entity-pairs)
        threshold     @(ns-resolve 'datalevin.query.resolve
                                   'merged-eav-lookup-threshold)
        n             (inc (long threshold))
        dir           (u/tmp-dir (str "merged-eav-lookup-"
                                      (UUID/randomUUID)))
        conn          (d/get-conn
                        dir
                        {:item/key {:db/unique :db.unique/identity}})]
    (try
      (d/transact!
        conn
        (mapv (fn [^long i]
                {:db/id      i
                 :item/key   (str "item-" i)
                 :item/value (mod i 17)})
              (range 1 (inc n))))
      (let [database    (d/db conn)
            originals   (mapv (fn [^long i]
                                [:item/key (str "item-" i)])
                              (range 1 (inc n)))
            entity-pairs (vec (resolve-pairs database originals))
            result       (lookup database ['?item :item/value '?value]
                                 entity-pairs true)]
        (is (= n (.size ^java.util.List result)))
        (is (= (set (map (fn [lookup-ref ^long i]
                           [lookup-ref (mod i 17)])
                         originals (range 1 (inc n))))
               (set (mapv vec result))))
        (is (every? #(= 2 (alength ^objects %)) result)))
      (finally
        (d/close conn)
        (u/delete-files dir)))))

(deftest disjunction-branches-project-hidden-cells-test
  (let [data    [[10 :left/person 4]
                 [10 :right/person 4]
                 [20 :path/start 1]
                 [20 :path/end 4]
                 [21 :path/start 1]
                 [21 :path/end 4]]
        context {:sources {'$ data}
                 :rules   {}
                 :rels    [(r/relation! {'?entity 0 '?start 1}
                                        (tuples [10 1]))]}
        resolve (fn [clause]
                  (binding [qu/*implicit-source* data]
                    (-> (qresolve/resolve-clause context clause)
                        :rels
                        (#(reduce j/hash-join %)))))
        assert-single-visible!
        (fn [rel expected]
          (let [attrs (:attrs rel)
                rows  (:tuples rel)]
            (is (= (set (keys expected)) (set (keys attrs))))
            (is (= 1 (.size ^java.util.List rows)))
            (is (= (count attrs)
                   (alength ^objects (.get ^java.util.List rows 0))))
            (is (= expected
                   (into {}
                         (map (fn [[var idx]]
                                [var (aget ^objects
                                           (.get ^java.util.List rows 0) idx)]))
                         attrs)))))]
    (testing "or dedupes by visible variables, not branch-local tuple cells"
      (assert-single-visible!
        (resolve
          '(or [?entity :left/person ?person]
               [?entity :right/person ?person]))
        {'?entity 10 '?start 1 '?person 4}))
    (testing "or-join projects and dedupes repeated paths within a branch"
      (assert-single-visible!
        (resolve
          '(or-join [?start ?person]
             (and [?path :path/start ?start]
                  [?path :path/end ?person])))
        {'?entity 10 '?start 1 '?person 4}))))

(deftest terminal-keyed-distinct-sum-test
  (let [dir   (u/tmp-dir (str "keyed-distinct-sum-" (UUID/randomUUID)))
        conn  (d/get-conn
                dir
                {:item/owner {:db/valueType   :db.type/ref
                              :db/cardinality :db.cardinality/many}
                 :copy/owner {:db/valueType :db.type/ref}
                 :copy/item  {:db/valueType :db.type/ref}})
        query '[:find ?group (sum ?delta)
                :with ?item
                :where
                [?owner :owner/group ?group]
                (or-join [?owner ?item ?delta]
                  (and [(ground :base) ?item]
                       [(ground 0) ?delta])
                  (and [?item :item/owner ?owner]
                       [?item :item/delta ?delta])
                  (and [?copy :copy/owner ?owner]
                       [?copy :copy/item ?item]
                       [?copy :copy/delta ?delta]))]
        injective-query
        '[:find ?owner (sum ?delta)
          :with ?item
          :where
          [?owner :owner/group _]
          (or-join [?owner ?item ?delta]
            (and [(ground :base) ?item]
                 [(ground 0) ?delta])
            (and [?item :item/owner ?owner]
                 [?item :item/delta ?delta])
            (and [?copy :copy/owner ?owner]
                 [?copy :copy/item ?item]
                 [?copy :copy/delta ?delta]))]
        disjoint-query
        '[:find ?owner (sum ?score)
          :with ?item
          :where
          [?owner :owner/group _]
          (or-join [?owner ?item ?score]
            (and [(ground :base) ?item]
                 [(ground 0) ?score])
            (and [?item :item/owner ?owner]
                 [(ground 1) ?score]))]
        qualified-ground-query
        '[:find ?owner (sum ?score)
          :with ?item
          :where
          [?owner :owner/group _]
          (or-join [?owner ?item ?score]
            (and [(ground :base) ?item]
                 [(datalevin.query-not-test/ground 0) ?score])
            (and [(ground :base) ?item]
                 [(datalevin.query-not-test/ground 2) ?score]))]
        expected           #{["A" 4] ["B" 5] ["C" 0]}
        injective-expected #{[1 1] [2 5] [3 5] [4 0]}
        disjoint-expected  #{[1 2] [2 2] [3 1] [4 0]}
        qualified-expected #{[1 1] [2 1] [3 1] [4 1]}]
    (try
      (d/transact! conn [{:db/id 1 :owner/group "A"}
                         {:db/id 2 :owner/group "A"}
                         {:db/id 3 :owner/group "B"}
                         {:db/id 4 :owner/group "C"}
                         {:db/id 10 :item/owner [1 2] :item/delta 2}
                         {:db/id 11 :item/owner 1 :item/delta -1}
                         {:db/id 12 :item/owner 2 :item/delta 3}
                         {:db/id 20 :item/owner 3 :item/delta 5}
                         ;; These rows duplicate one projected branch result,
                         ;; which also overlaps the direct item branch.
                         {:db/id 100
                          :copy/owner 1 :copy/item 10 :copy/delta 2}
                         {:db/id 101
                          :copy/owner 1 :copy/item 10 :copy/delta 2}])
      (let [database (d/db conn)
            ordinary
            (binding [dq/*cache?* false
                      qagg/*keyed-group-reduction?* false]
              (d/q query database))
            reduced-explain
            (binding [dq/*cache?* false
                      qagg/*keyed-group-reduction-min-input* 1
                      qagg/*keyed-group-reduction-min-ratio* 1.0]
              (d/explain {:run? true} query database))
            reduced
            (binding [dq/*cache?* false
                      qagg/*keyed-group-reduction-min-input* 1
                      qagg/*keyed-group-reduction-min-ratio* 1.0]
              (d/q query database))
            fallback-explain
            (binding [dq/*cache?* false
                      qagg/*keyed-group-reduction-min-input* 1000]
              (d/explain {:run? true} query database))
            injective-ordinary
            (binding [dq/*cache?* false
                      qagg/*keyed-group-reduction?* false]
              (d/q injective-query database))
            injective-explain
            (binding [dq/*cache?* false
                      qagg/*keyed-group-reduction-min-input* 1
                      qagg/*keyed-group-reduction-min-ratio* 1.0]
              (d/explain {:run? true} injective-query database))
            injective-reduced
            (binding [dq/*cache?* false
                      qagg/*keyed-group-reduction-min-input* 1
                      qagg/*keyed-group-reduction-min-ratio* 1.0]
              (d/q injective-query database))
            disjoint-explain
            (binding [dq/*cache?* false
                      qagg/*keyed-group-reduction-min-input* 1
                      qagg/*keyed-group-reduction-min-ratio* 1.0]
              (d/explain {:run? true} disjoint-query database))
            disjoint-result
            (binding [dq/*cache?* false
                      qagg/*keyed-group-reduction-min-input* 1
                      qagg/*keyed-group-reduction-min-ratio* 1.0]
              (d/q disjoint-query database))
            qualified-explain
            (binding [dq/*cache?* false
                      qagg/*keyed-group-reduction-min-input* 1
                      qagg/*keyed-group-reduction-min-ratio* 1.0]
              (d/explain {:run? true} qualified-ground-query database))
            qualified-result
            (binding [dq/*cache?* false
                      qagg/*keyed-group-reduction-min-input* 1
                      qagg/*keyed-group-reduction-min-ratio* 1.0]
              (d/q qualified-ground-query database))]
        (is (= expected (set ordinary) (set reduced)))
        (testing "logical find/with identities dedupe across physical keys"
          (is (= 7 (get-in reduced-explain
                           [:keyed-group-reduction :identity-count])))
          (is (= 3 (get-in reduced-explain
                           [:keyed-group-reduction :group-count])))
          (is (= :seen-set
                 (get-in reduced-explain
                         [:keyed-group-reduction :identity-mode]))))
        (testing "the reducer is runtime-gated with an exact fallback"
          (is (true? (get-in reduced-explain
                             [:keyed-group-reduction :executed?])))
          (is (true? (get-in reduced-explain
                             [:keyed-group-reduction :direct-feed?])))
          (is (false? (get-in reduced-explain
                              [:keyed-group-reduction :union-materialized?])))
          (is (false? (get-in fallback-explain
                              [:keyed-group-reduction :executed?])))
          (is (true? (get-in fallback-explain
                             [:keyed-group-reduction :union-materialized?])))
          (is (= :below-runtime-cost-threshold
                 (get-in fallback-explain
                         [:keyed-group-reduction :reason]))))
        (testing "injective correlations dedupe across direct producers"
          (is (= injective-expected
                 (set injective-ordinary)
                 (set injective-reduced)))
          (is (= :cross-producer-seen-set
                 (get-in injective-explain
                         [:keyed-group-reduction :identity-mode])))
          (is (true? (get-in injective-explain
                             [:keyed-group-reduction :direct-feed?]))))
        (testing "constant domains prove direct producers disjoint"
          (is (= disjoint-expected (set disjoint-result)))
          (is (= :producer-disjoint
                 (get-in disjoint-explain
                         [:keyed-group-reduction :identity-mode])))
          (is (= {:var '?score :domains [#{0} #{1}]}
                 (get-in disjoint-explain
                         [:keyed-group-reduction :proof
                          :producer-disjoint]))))
        (testing "qualified functions named ground are not proof constants"
          (is (= qualified-expected (set qualified-result)))
          (is (= :cross-producer-seen-set
                 (get-in qualified-explain
                         [:keyed-group-reduction :identity-mode])))
          (is (nil? (get-in qualified-explain
                            [:keyed-group-reduction :proof
                             :producer-disjoint])))))
      (finally
        (d/close conn)
        (u/delete-files dir)))))

(deftest terminal-virtual-producer-sum-test
  (let [dir  (u/tmp-dir (str "virtual-producer-sum-" (UUID/randomUUID)))
        conn (d/get-conn
               dir
               {:item/owner {:db/valueType   :db.type/ref
                             :db/cardinality :db.cardinality/many}})
        query
        '[:find ?owner (sum ?score)
          :with ?item
          :where
          [?owner :owner/group _]
          (or-join [?owner ?item ?score]
            (and [(ground :base) ?item]
                 [(ground 0) ?score])
            (and [?item :item/owner ?owner]
                 [?item :item/delta _]
                 (or-join [?item ?score]
                   [(ground -1) ?score]
                   (and [?item :item/delta 2]
                        [(ground 2) ?score]))))]
        overlap-query
        '[:find ?owner (sum ?score)
          :with ?item
          :where
          [?owner :owner/group _]
          (or-join [?owner ?item ?score]
            (and [(ground :base) ?item]
                 [(ground 0) ?score])
            (and [?item :item/owner ?owner]
                 [?item :item/delta _]
                 (or-join [?item ?score]
                   [(ground -1) ?score]
                   (and [?item :item/delta 2]
                        [(ground -1) ?score]))))]
        expected         #{[1 0] [2 1] [3 0]}
        overlap-expected #{[1 -2] [2 -1] [3 0]}]
    (try
      (d/transact! conn [{:db/id 1 :owner/group "A"}
                         {:db/id 2 :owner/group "B"}
                         {:db/id 3 :owner/group "C"}
                         {:db/id 10 :item/owner [1 2] :item/delta 2}
                         {:db/id 11 :item/owner 1 :item/delta 3}])
      (let [database (d/db conn)
            ordinary
            (binding [dq/*cache?* false
                      qagg/*keyed-group-reduction?* false]
              (d/q query database))
            materialized
            (binding [dq/*cache?* false
                      qagg/*keyed-group-virtual-producers?* false
                      qagg/*keyed-group-reduction-min-input* 1
                      qagg/*keyed-group-reduction-min-ratio* 1.0]
              (d/q query database))
            virtual-explain
            (binding [dq/*cache?* false
                      qagg/*keyed-group-reduction-min-input* 1
                      qagg/*keyed-group-reduction-min-ratio* 1.0]
              (d/explain {:run? true} query database))
            virtual
            (binding [dq/*cache?* false
                      qagg/*keyed-group-reduction-min-input* 1
                      qagg/*keyed-group-reduction-min-ratio* 1.0]
              (d/q query database))
            overlap-ordinary
            (binding [dq/*cache?* false
                      qagg/*keyed-group-reduction?* false]
              (d/q overlap-query database))
            overlap-explain
            (binding [dq/*cache?* false
                      qagg/*keyed-group-reduction-min-input* 1
                      qagg/*keyed-group-reduction-min-ratio* 1.0]
              (d/explain {:run? true} overlap-query database))
            overlap-virtual
            (binding [dq/*cache?* false
                      qagg/*keyed-group-reduction-min-input* 1
                      qagg/*keyed-group-reduction-min-ratio* 1.0]
              (d/q overlap-query database))
            fallback-explain
            (binding [dq/*cache?* false
                      qagg/*keyed-group-reduction-min-input* 1000]
              (d/explain {:run? true} query database))
            fallback
            (binding [dq/*cache?* false
                      qagg/*keyed-group-reduction-min-input* 1000]
              (d/q query database))]
        (testing "constant columns feed the terminal sink without a union"
          (is (= expected
                 (set ordinary)
                 (set materialized)
                 (set virtual)))
          (is (true? (get-in virtual-explain
                             [:keyed-group-reduction
                              :virtual-producer-feed?])))
          (is (false? (get-in virtual-explain
                              [:keyed-group-reduction
                               :union-materialized?])))
          (is (= 2 (get-in virtual-explain
                           [:keyed-group-reduction
                            :virtual-producer-count])))
          (is (= [#{0} #{-1} #{2}]
                 (get-in virtual-explain
                         [:keyed-group-reduction :proof
                          :stream-producer-domains])))
          (is (= {:var '?score :domains [#{0} #{-1} #{2}]}
                 (get-in virtual-explain
                         [:keyed-group-reduction :proof
                          :producer-disjoint]))))
        (testing "overlapping virtual and materialized identities dedupe"
          (is (= overlap-expected
                 (set overlap-ordinary)
                 (set overlap-virtual)))
          (is (= :cross-producer-seen-set
                 (get-in overlap-explain
                         [:keyed-group-reduction :identity-mode])))
          (is (false? (get-in overlap-explain
                              [:keyed-group-reduction
                               :producer-disjoint?])))
          (is (nil? (get-in overlap-explain
                            [:keyed-group-reduction :proof
                             :producer-disjoint]))))
        (testing "a rejected virtual reduction materializes without rescanning"
          (is (= expected (set fallback)))
          (is (false? (get-in fallback-explain
                              [:keyed-group-reduction :executed?])))
          (is (false? (get-in fallback-explain
                              [:keyed-group-reduction
                               :virtual-producer-feed?])))
          (is (true? (get-in fallback-explain
                             [:keyed-group-reduction
                              :union-materialized?])))
          (is (= :below-runtime-cost-threshold
                 (get-in fallback-explain
                         [:keyed-group-reduction :reason])))))
      (finally
        (d/close conn)
        (u/delete-files dir)))))

(deftest distinct-sum-sink-fork-merge-test
  (let [sink (qagg/distinct-sum-sink ['?group] '?score)
        fork (qagg/-fork-keyed sink)]
    (qagg/-accept-keyed! sink ["A"] [:same 2] 2)
    (qagg/-accept-keyed! fork ["A"] [:same 2] 2)
    (qagg/-accept-keyed! fork ["A"] [:other 3] 3)
    (qagg/-accept-keyed! fork ["B"] [:only 7] 7)
    (qagg/-merge-keyed! sink fork)
    (is (= {:identity-count 3 :group-count 2}
           (qagg/-keyed-stats sink)))
    (is (= #{["A" 5] ["B" 7]}
           (set (rows (qagg/-keyed-relation sink)))))))

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

(deftest bound-value-expansion-presence-fusion-test
  (let [dir      (u/tmp-dir (str "ave-presence-fusion-" (UUID/randomUUID)))
        conn     (d/get-conn dir {:post/creator {:db/valueType :db.type/ref}})
        input    [[1 :first] [1 :second] [2 :third]]
        query    '[:find ?payload ?post
                   :in $ [[?person ?payload]]
                   :where
                   (or-join [?person ?payload ?post]
                     (and [?post :post/creator ?person]
                          [?post :post/container _]
                          [?post :post/visible _]))]
        explicit '[:find ?payload ?post
                   :in $db [[?person ?payload]]
                   :where
                   (or-join [?person ?payload ?post]
                     (and [$db ?post :post/creator ?person]
                          [$db ?post :post/container _]
                          [$db ?post :post/visible _]))]
        expected #{[:first 100] [:second 100] [:third 103]}]
    (try
      (d/transact! conn [{:db/id 1 :person/id 1}
                         {:db/id 2 :person/id 2}
                         {:db/id 3 :person/id 3}
                         {:db/id 100
                          :post/creator 1
                          :post/container 10
                          :post/visible true}
                         {:db/id 101
                          :post/creator 1
                          :post/container 10}
                         {:db/id 102
                          :post/creator 1
                          :post/visible true}
                         {:db/id 103
                          :post/creator 2
                          :post/container 20
                          :post/visible false}
                         {:db/id 104
                          :post/creator 3
                          :post/container 30
                          :post/visible true}])
      (let [database (d/db conn)
            rel      (r/relation! {'?person 0 '?payload 1}
                                  (tuples [1 :first]
                                          [1 :second]
                                          [2 :third]))
            context  {:sources {'$      database
                                '$db    database
                                '$other [[100 :post/container 10]]}
                      :rules   {}
                      :rels    [rel]}
            clauses  '[[?post :post/creator ?person]
                       [?post :post/container _]
                       [?post :post/visible _]]
            explicit-clauses
            '[[$db ?post :post/creator ?person]
              [$db ?post :post/container _]
              [$db ?post :post/visible _]]
            result-set
            (fn [context]
              (let [rel   (reduce j/hash-join (:rels context))
                    attrs (:attrs rel)]
                (into #{}
                      (map (fn [^objects tuple]
                             [(aget tuple (attrs '?payload))
                              (aget tuple (attrs '?post))]))
                      (:tuples rel))))
            fuse
            (fn [context clauses]
              (binding [qu/*implicit-source* database
                        c/magic-cost-link-probe      0.0
                        c/magic-cost-link-retrieval  0.0
                        c/magic-cost-init-scan-e     100.0
                        c/magic-cost-hash-join       0.0]
                (qresolve/resolve-bound-value-presence-prefix
                  context clauses 0)))]
        (testing "the compact producer consumes all contiguous checks"
          (let [{:keys [context idxs]} (fuse context clauses)]
            (is (= [0 1 2] idxs))
            (is (= expected (result-set context)))))
        (testing "a singleton value binding is also eligible"
          (let [singleton (assoc context :rels
                                 [(r/relation! {'?person 0 '?payload 1}
                                               (tuples [1 :only]))])
                result    (fuse singleton clauses)]
            (is (= [0 1 2] (:idxs result)))
            (is (= #{[:only 100]} (result-set (:context result))))))
        (testing "a value-producing EAV clause is not a presence check"
          (is (nil? (fuse context
                          (assoc clauses 1
                                 '[?post :post/container ?container])))))
        (testing "presence checks from another source are not fused"
          (is (nil? (fuse context
                          (assoc clauses 1
                                 '[$other ?post :post/container _])))))
        (testing "disabled and fused query execution agree"
          (is (= expected
                 (binding [dq/*cache?* false
                           qresolve/*bound-value-presence-fusion?* false]
                   (d/q query database input))))
          (is (= expected
                 (binding [dq/*cache?* false]
                   (d/q query database input)))))
        (testing "explicit database sources use the same fused prefix"
          (let [{:keys [context idxs]} (fuse context explicit-clauses)]
            (is (= [0 1 2] idxs))
            (is (= expected (result-set context))))
          (is (= expected
                 (binding [dq/*cache?* false]
                   (d/q explicit database input))))))
      (finally
        (d/close conn)
        (u/delete-files dir)))))

(deftest singleton-domain-scan-test
  (let [dir      (u/tmp-dir (str "singleton-domain-scan-"
                                 (UUID/randomUUID)))
        conn     (d/get-conn
                   dir
                   {:person/interest {:db/valueType   :db.type/ref
                                      :db/cardinality :db.cardinality/many}
                    :post/tag        {:db/valueType   :db.type/ref
                                      :db/cardinality :db.cardinality/many}})
        input    (into [[100 1 :first]
                       [100 1 :second]
                       [101 1 :third]
                       [102 1 :fourth]]
                      (map (fn [post] [post 1 post]))
                      (range 103 170))
        query    '[:find ?payload ?post ?tag
                   :in $ [[?post ?start ?payload]]
                   :where
                   (or-join [?post ?start ?payload ?tag]
                     (and [?post :post/tag ?tag]
                          [?start :person/interest ?tag]))]
        expected #{[:first 100 10]
                   [:second 100 10]
                   [:fourth 102 20]}]
    (try
      (d/transact!
        conn
        [{:db/id 1 :person/interest [10 20]}
         {:db/id 2 :person/interest [20 30]}
         {:db/id 10 :tag/name "ten"}
         {:db/id 20 :tag/name "twenty"}
         {:db/id 30 :tag/name "thirty"}
         {:db/id 100 :post/tag 10}
         {:db/id 101 :post/tag 30}
         {:db/id 102 :post/tag [20 30]}])
      (let [database (d/db conn)
            rel      (r/relation! {'?post 0 '?start 1 '?payload 2}
                                  (apply tuples input))
            context  {:sources {'$      database
                                '$db    database
                                '$other [[100 :post/tag 10]]}
                      :rules   {}
                      :rels    [rel]}
            clauses  '[[?post :post/tag ?tag]
                       [?start :person/interest ?tag]]
            explicit-clauses
            '[[$db ?post :post/tag ?tag]
              [$db ?start :person/interest ?tag]]
            reversed (vec (reverse clauses))
            result-set
            (fn [context]
              (let [rel   (reduce j/hash-join (:rels context))
                    attrs (:attrs rel)]
                (into #{}
                      (map (fn [^objects tuple]
                             [(aget tuple (attrs '?payload))
                              (aget tuple (attrs '?post))
                              (aget tuple (attrs '?tag))]))
                      (:tuples rel))))
            specialize
            (fn [context clauses selected-idx]
              (binding [qu/*implicit-source* database]
                (qresolve/resolve-singleton-domain-scan
                  context clauses selected-idx)))]
        (testing "the singleton-owned domain constrains the other bound scan"
          (let [{:keys [context idxs domain-size matched-size]}
                (specialize context clauses 0)]
            (is (= [0 1] idxs))
            (is (= 2 domain-size))
            (is (= 2 matched-size))
            (is (= expected (result-set context)))))
        (testing "the rewrite is independent of which pattern is selected"
          (let [result (specialize context reversed 0)]
            (is (= [0 1] (:idxs result)))
            (is (= expected (result-set (:context result))))))
        (testing "an explicit source uses the same runtime-domain scan"
          (let [{:keys [context idxs]}
                (specialize context explicit-clauses 0)]
            (is (= [0 1] idxs))
            (is (= expected (result-set context)))))
        (testing "large entity inputs use the parallel-safe predicate path"
          (let [large   (assoc context :rels
                               [(r/relation!
                                  {'?post 0 '?start 1 '?payload 2}
                                  (apply tuples
                                         (map (fn [post] [post 1 post])
                                              (range 100 4101))))])
                result  (specialize large clauses 0)]
            (is (= 2 (:matched-size result)))
            (is (= #{[100 100 10] [102 102 20]}
                   (result-set (:context result))))))
        (testing "an empty singleton-owned domain annihilates the conjunction"
          (let [missing (assoc context :rels
                               [(r/relation!
                                  {'?post 0 '?start 1 '?payload 2}
                                  (apply tuples
                                         (map (fn [[post _ payload]]
                                                [post 999 payload])
                                              input)))])
                result  (specialize missing clauses 0)]
            (is (= 0 (:domain-size result)))
            (is (empty? (result-set (:context result))))))
        (testing "more than one owner is not a semi-known domain"
          (let [multiple (assoc context :rels
                                [(r/relation!
                                   {'?post 0 '?start 1 '?payload 2}
                                   (tuples [100 1 :first]
                                           [102 2 :second]))])]
            (is (nil? (specialize multiple clauses 0)))))
        (testing "small consumers retain ordinary clause resolution"
          (let [small (assoc context :rels
                             [(r/relation!
                                {'?post 0 '?start 1 '?payload 2}
                                (apply tuples (take 10 input)))])]
            (is (nil? (specialize small clauses 0)))))
        (testing "the runtime size guard retains ordinary resolution"
          (is (nil?
                (binding [c/sip-range-threshold 1]
                  (specialize context clauses 0))))
          (is (nil?
                (binding [c/sip-ratio-threshold 36]
                  (specialize context clauses 0)))))
        (testing "patterns from different sources are not combined"
          (is (nil?
                (specialize
                  context
                  '[[$db ?post :post/tag ?tag]
                    [$other ?start :person/interest ?tag]]
                  0))))
        (testing "both execution paths preserve the visible shared value"
          (is (= expected
                 (binding [dq/*cache?* false
                           qresolve/*singleton-domain-scan?* false]
                   (d/q query database input))))
          (is (= expected
                 (binding [dq/*cache?* false]
                   (d/q query database input))))))
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

(deftest not-join-reusable-prefix-test
  (let [dir       (u/tmp-dir (str "not-join-prefix-" (UUID/randomUUID)))
        conn      (d/get-conn
                    dir
                    {:message/hasCreator {:db/valueType :db.type/ref}
                     :likes/message      {:db/valueType :db.type/ref}
                     :likes/person       {:db/valueType :db.type/ref}})
        start     1
        message-a 1000
        message-b 1001
        likers    (range 2000 2020)
        query
        '[:find ?liker-id ?message-id ?like-date
          :where
          [?start :person/id 100]
          [?message :message/hasCreator ?start]
          [?message :message/id ?message-id]
          [?like :likes/message ?message]
          [?like :likes/person ?liker]
          [?like :likes/creationDate ?like-date]
          [?liker :person/id ?liker-id]
          (not-join [?liker ?like-date ?message-id ?start]
                    [?other-message :message/hasCreator ?start]
                    [?other-message :message/id ?other-message-id]
                    [?other-like :likes/message ?other-message]
                    [?other-like :likes/person ?liker]
                    [?other-like :likes/creationDate ?other-like-date]
                    (or-join
                      [?like-date ?other-like-date
                       ?message-id ?other-message-id]
                      [(> ?other-like-date ?like-date)]
                      (and [(= ?other-like-date ?like-date)]
                           [(< ?other-message-id ?message-id)])))]
        not-form  (last query)
        prefix-plan
        ((ns-resolve 'datalevin.query.resolve 'not-join-prefix-plan)
         (second not-form) (drop 2 not-form))]
    (try
      (d/transact!
        conn
        (into
          [{:db/id start :person/id 100}
           {:db/id message-a
            :message/hasCreator start
            :message/id 10}
           {:db/id message-b
            :message/hasCreator start
            :message/id 20}]
          (mapcat
            (fn [^long liker]
              (let [liker-id (- liker 1900)
                    like-a   (+ 3000 (* 2 liker-id))
                    like-b   (inc like-a)
                    date-b   (if (even? liker-id) 300 200)]
                [{:db/id liker :person/id liker-id}
                 {:db/id like-a
                  :likes/message message-a
                  :likes/person liker
                  :likes/creationDate 200}
                 {:db/id like-b
                  :likes/message message-b
                  :likes/person liker
                  :likes/creationDate date-b}]))
            likers)))
      (testing "the connected prefix is anchored only by the reusable key"
        (is (= ['?start] (:anchor-vars prefix-plan)))
        (is (= 5 (count (:prefix prefix-plan))))
        (is (= 1 (count (:residual prefix-plan)))))
      (let [database     (d/db conn)
            conventional (binding [qresolve/*not-join-prefix-specialization?*
                                   false]
                           (d/q query database))
            specialized  (d/q query database)
            expected
            (into #{}
                  (map (fn [^long liker]
                         (let [liker-id (- liker 1900)]
                           (if (even? liker-id)
                             [liker-id 20 300]
                             [liker-id 10 200]))))
                  likers)]
        (testing "decorrelation preserves date and tie-break semantics"
          (is (= expected conventional specialized))))
      (finally
        (d/close conn)
        (u/delete-files dir)))))

(deftest partitioned-linear-execution-test
  (let [dir   (u/tmp-dir (str "partitioned-linear-" (UUID/randomUUID)))
        conn  (d/get-conn
                dir
                {:start/id    {:db/valueType :db.type/long
                               :db/unique    :db.unique/identity}
                 :edge/from   {:db/valueType :db.type/ref}
                 :edge/to     {:db/valueType :db.type/ref}
                 :person/blocked {}
                 :post/person {:db/valueType :db.type/ref}
                 :post/forum  {:db/valueType :db.type/ref}
                 :forum/title {:db/valueType :db.type/string}
                 :forum/owner {:db/valueType :db.type/ref}
                 :owner/name  {:db/valueType :db.type/string}})
        query '[:find ?person ?title (count ?post)
                :where
                [?start :start/id 1]
                [?edge :edge/from ?start]
                [?edge :edge/to ?person]
                [?post :post/person ?person]
                [?post :post/forum ?forum]
                [?forum :forum/title ?title]]
        nonlinear-query
        '[:find ?person ?title (count ?post)
          :where
          [?start :start/id 1]
          [?edge :edge/from ?start]
          [?edge :edge/to ?person]
          [?post :post/person ?person]
          [?post :post/forum ?forum]
          [?forum :forum/title ?title]
          (not-join [?person]
            [?person :person/blocked true])]
        nonlinear-prefix-query
        '[:find ?person ?title ?owner-name (count ?post)
          :where
          [?start :start/id 1]
          (or-join [?start ?person]
            (and [?edge :edge/from ?start]
                 [?edge :edge/to ?person]))
          [?post :post/person ?person]
          [?post :post/forum ?forum]
          [?forum :forum/title ?title]
          [?forum :forum/owner ?owner]
          [?owner :owner/name ?owner-name]]
        participant-capacity
        @(ns-resolve 'datalevin.query.plan
                     'partition-participant-capacity)
        participant-count
        (ns-resolve 'datalevin.query.plan 'partition-participant-count)]
    (try
      (d/transact!
        conn
        (into
          [{:db/id 1 :start/id 1}
           {:db/id 100 :person/blocked true}]
          (concat
            (map (fn [^long i]
                   {:db/id (+ 3000 i)
                    :forum/title (str "forum-" i)
                    :forum/owner (+ 4000 i)})
                 (range 4))
            (map (fn [^long i]
                   {:db/id (+ 4000 i) :owner/name (str "owner-" i)})
                 (range 4))
            (map (fn [^long i]
                   {:db/id (+ 1000 i)
                    :edge/from 1
                    :edge/to (+ 100 i)})
                 (range 8))
            (for [^long person (range 8), ^long forum (range 4)]
              {:db/id (+ 10000 (* person 4) forum)
               :post/person (+ 100 person)
               :post/forum (+ 3000 forum)}))))
      (let [database (d/db conn)
            pipeline (binding [c/query-partitioned-execution? false]
                       (d/q query database))
            run-partitioned
            (fn [f]
              (binding [c/query-partitioned-execution? true
                        c/query-partition-min-input-size 5
                        c/query-partition-target-size 2
                        c/query-partition-min-step-count 3]
                (with-redefs-fn
                  {participant-count
                   (fn ^long [^long n]
                     (if (< n 5) 1 4))}
                  f)))
            partitioned (run-partitioned #(d/q query database))
            explain     (run-partitioned
                          #(d/explain {:run? true} query database))
            decision    (first (:partitioned-execution explain))
            nonlinear-pipeline
            (binding [c/query-partitioned-execution? false]
              (d/q nonlinear-query database))
            nonlinear-explain
            (run-partitioned
              #(d/explain {:run? true} nonlinear-query database))
            nonlinear-decision
            (first (:partitioned-execution nonlinear-explain))
            nonlinear-prefix-pipeline
            (binding [c/query-partitioned-execution? false]
              (d/q nonlinear-prefix-query database))
            nonlinear-prefix-explain
            (run-partitioned
              #(d/explain {:run? true} nonlinear-prefix-query database))
            nonlinear-prefix-decision
            (first (:partitioned-execution nonlinear-prefix-explain))]
        (testing "a sorted partition traverses the unary suffix exactly once"
          (is (= pipeline partitioned))
          (is (= :partitioned-segment (:mode decision)))
          (is (= 8 (:input-rows decision)))
          (is (= 4 (:partitions decision)))
          (is (= [:merge :merge :link :merge :merge]
                 (:partitioned-step-types decision))))
        (testing "a nonlinear suffix does not disqualify its safe segment"
          (is (= nonlinear-pipeline (:result nonlinear-explain)))
          (is (= :partitioned-segment (:mode nonlinear-decision)))
          (is (= :not-join (:next-step-type nonlinear-decision))))
        (testing "a nonlinear prefix does not disqualify its safe segment"
          (is (= nonlinear-prefix-pipeline (:result nonlinear-prefix-explain)))
          (is (= :partitioned-segment (:mode nonlinear-prefix-decision)))
          (is (= :or-join
                 (:previous-step-type nonlinear-prefix-decision))))
        (testing "the capacity model works on one core and respects pool slots"
          (is (= 1 (participant-capacity 10000 1 10 2000)))
          (is (= 2 (participant-capacity 10000 8 1 2000)))
          (is (= 3 (participant-capacity 5000 8 8 2000)))))
      (finally
        (d/close conn)
        (u/delete-files dir)))))
