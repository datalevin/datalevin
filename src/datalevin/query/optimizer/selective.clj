;;
;; Copyright (c) Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns ^:no-doc datalevin.query.optimizer.selective
  "Cost-based selection of value lookups and rule anchors to materialize
  before join planning."
  (:require
   [clojure.set :as set]
   [datalevin.db :as db]
   [datalevin.interface
    :refer [av-size]]
   [datalevin.parser :as dp]
   [datalevin.query-util :as qu]
   [datalevin.query.optimizer.bound-patterns
    :refer [materialize-bound-patterns-with-cost
            project-bound-patterns-with-cost remove-materialized-clause]]
   [datalevin.query.optimizer.estimates
    :refer [estimated-plan-size materialize-pattern-with-cost
            materialized-output-cost relation-size reset-cost-planning
            selective-anchor-cost selective-rule-anchor-max-inline-size
            selective-rule-anchor-max-output-size
            selective-rule-anchor-max-rule-size
            selective-rule-anchor-min-plan-size variable-ref-count]]
   [datalevin.query.optimizer.plan-build
    :refer [build-plan plan-not-joins]]
   [datalevin.query.optimizer.plan-cost
    :refer [estimated-late-cost estimated-plan-cost final-plan-cost
            late-expansion-operation? late-operation]]
   [datalevin.query.optimizer.rewrite
    :refer [build-graph clause-source-symbol pattern-form pattern-var-symbol
            rel-bound-var?]]
   [datalevin.query.plan :as qplan]
   [datalevin.query.resolve :as qresolve]
   [datalevin.rules :as rules]
   [datalevin.util :as u])
  (:import
   [datalevin.db DB]
   [datalevin.parser Constant Variable Pattern RuleExpr]))

(def ^:private ^:const ^double selective-anchor-dominance-margin
  2.0)

(def ^:private ^:const ^double selective-rule-anchor-dominance-margin
  4.0)

(defn- selective-value-candidates
  [context]
  (let [{:keys [parsed-q sources]} context]
    (into []
          (keep-indexed
            (fn [clause-idx [parsed-clause orig-clause]]
              (when (and (instance? Pattern parsed-clause)
                         (vector? orig-clause)
                         (= 3 (count (:pattern parsed-clause))))
                (let [parsed-pattern (:pattern parsed-clause)
                      e-sym          (pattern-var-symbol
                                       (first parsed-pattern))
                      attr           (second parsed-pattern)
                      value          (nth parsed-pattern 2)]
                  (when (and e-sym
                             (not (rel-bound-var? context e-sym))
                             (< 1 (variable-ref-count
                                    (:qwhere parsed-q) e-sym))
                             (instance? Constant attr)
                             (keyword? (:value attr))
                             (instance? Constant value))
                    (let [source-sym (clause-source-symbol
                                       (:source parsed-clause))]
                      (when-let [source (get sources source-sym)]
                        (when (db/-searchable? source)
                          (let [pattern (qresolve/resolve-pattern-lookup-refs
                                          source
                                          (pattern-form orig-clause))
                                attr    (second pattern)
                                value   (nth pattern 2)
                                fanout  (av-size (.-store ^DB source)
                                                 attr value)]
                            {:key         [source-sym pattern]
                             :clause-idx  clause-idx
                             :source-sym  source-sym
                             :source      source
                             :pattern     pattern
                             :entity-sym  e-sym
                             :attr        attr
                             :value       value
                             :fanout      (long fanout)})))))))))
          (map vector (:qwhere parsed-q)
               (:qorig-where parsed-q)))))

(defn- plan-binds-var?
  [plan sym]
  (when-let [cols (some-> plan :steps last :cols)]
    (some? (qplan/find-index sym cols))))

(defn- plan-entry-cost
  ^double [plan]
  (final-plan-cost [plan]))

(defn- delayed-anchor-cost
  ^double [planned source-sym entity-sym]
  (reduce
    (fn [^double best trace]
      (if-let [idx' (first (keep-indexed
                             (fn [idx plan]
                               (when (plan-binds-var? plan entity-sym) idx))
                             trace))]
        (let [idx     (long idx')
              current (double (plan-entry-cost (nth trace idx)))
              prior   (double (if (zero? idx)
                                0.0
                                (plan-entry-cost
                                  (nth trace (dec (long idx))))))]
          (max best (max 0.0 (- current prior))))
        best))
    0.0
    (get-in planned [:plan source-sym])))

(defn- planned-context-for-cost
  [context]
  (let [context (build-graph context)]
    ;; The first graph construction ends the normal building phase. Candidate
    ;; materialization and any residual replanning belong to planning time.
    (when (and qplan/*explain*
               (not (contains? @qplan/*explain* :building-time)))
      (let [parsing-time (long (or (:parsing-time @qplan/*explain*) 0))]
        (vswap! qplan/*explain* assoc :building-time
                (- (System/nanoTime)
                   (+ (long qplan/*start-time*) parsing-time)))))
    (-> context build-plan plan-not-joins)))

(defn- estimated-context-work
  ^double [planned]
  (if (= #{} (:result-set planned))
    0.0
    (let [plan-size (estimated-plan-size planned)
          late      (estimated-late-cost
                      planned plan-size (:access-plans planned))]
      (+ (double (estimated-plan-cost planned))
         (double (:cost late))))))

(defn- costed-selective-value-candidates
  [planned ^double base-cost candidates]
  (into []
        (comp
          (map (fn [{:keys [source-sym entity-sym fanout] :as candidate}]
                 (let [delayed-cost (delayed-anchor-cost
                                      planned source-sym entity-sym)
                       anchor-cost  (selective-anchor-cost fanout)]
                   (assoc candidate
                          :delayed-cost delayed-cost
                          :anchor-cost anchor-cost
                          :remaining-cost (max 0.0
                                               (- base-cost delayed-cost))
                          :saving (- delayed-cost anchor-cost)))))
          ;; This is the cheap decision boundary. Only a value lookup cheaper
          ;; than a delayed step which dominates the rest of the plan is
          ;; allowed to perform speculative materialization. The margin keeps
          ;; ordinary graph plans from paying execution work merely to reject
          ;; a close alternative.
          (filter
            (fn [candidate]
              (and (pos? (double (:saving candidate)))
                   (< (* selective-anchor-dominance-margin
                         (double (:remaining-cost candidate)))
                      (double (:delayed-cost candidate)))))))
        candidates))

(defn- try-selective-value-candidate
  [context {:keys [^long clause-idx source pattern fanout anchor-cost]
            :as candidate}
   ^double budget]
  (if (<= budget (double anchor-cost))
    (assoc candidate
           :context context
           :eligible? false
           :guardrail {:pattern pattern
                       :projected-rows fanout
                       :projected-cost anchor-cost
                       :accumulated-cost 0.0
                       :budget budget}
           :materialization-cost 0.0
           :materialization-stages [])
    (let [seed         (materialize-pattern-with-cost context source pattern 1)
          seed-context (remove-materialized-clause
                         (:context seed) clause-idx)
          budget       (max 0.0 (- budget (double (:cost seed))))
          preflight    (project-bound-patterns-with-cost
                         seed-context budget)]
      (if-not (:eligible? preflight)
        (assoc candidate
               :context context
               :eligible? false
               :guardrail (:guardrail preflight)
               :materialization-cost (double (:cost seed))
               :materialization-stages [(:stage seed)])
        (let [propagated (materialize-bound-patterns-with-cost
                           seed-context budget)]
          (assoc candidate
                 :context              (:context propagated)
                 :eligible?            (:eligible? propagated)
                 :guardrail            (:guardrail propagated)
                 :materialization-cost (+ (double (:cost seed))
                                          (double (:cost propagated)))
                 :materialization-stages
                 (into [(:stage seed)] (:stages propagated))))))))

(defn- record-selective-value-decision!
  [decision]
  (when qplan/*explain*
    (vswap! qplan/*explain* update :pre-materialization-decisions
            (fnil conj []) decision)))

(defn- splice-rule-expansion
  [context ^long clause-idx expansion]
  (let [parsed-q (get context :parsed-q)
        clauses  (:qorig-where parsed-q)
        before   (subvec clauses 0 clause-idx)
        after    (subvec clauses (unchecked-inc clause-idx))
        clauses  (into before (concat expansion after))]
    (-> context
        (assoc :parsed-q
               (assoc parsed-q
                      :qorig-where clauses
                      :qwhere (dp/parse-where clauses)))
        (reset-cost-planning))))

(defn- isolate-rule-expansion
  [context {:keys [rule-vars expansion]}]
  (let [parsed-q (:parsed-q context)
        parsed-q (assoc parsed-q
                        :qfind (dp/parse-find rule-vars)
                        :qorig-find rule-vars
                        :qwith nil
                        :qreturn-map nil
                        :qwhere (dp/parse-where expansion)
                        :qorig-where expansion
                        :qhaving nil
                        :qorder nil
                        :qlimit nil
                        :qoffset nil)]
    (-> context
        (assoc :parsed-q parsed-q :rels [])
        (reset-cost-planning))))

(defn- variable-rule-args
  [^RuleExpr clause]
  (let [args (:args clause)
        vars (mapv #(when (instance? Variable %) (:symbol ^Variable %)) args)]
    (when (and (every? some? vars)
               (<= 2 (count (set vars))))
      vars)))

(defn- selective-rule-anchor-candidates
  [{:keys [parsed-q rels late-clauses] :as context}]
  (let [clauses    (:qorig-where parsed-q)
        late       (set late-clauses)
        bound-vars (into #{} (mapcat (comp keys :attrs)) rels)]
    (into
      []
      (keep-indexed
        (fn [clause-idx [parsed-clause orig-clause]]
          (when (and (instance? RuleExpr parsed-clause)
                     (or (nil? late-clauses)
                         (contains? late orig-clause)))
            (when-let [rule-vars (variable-rule-args parsed-clause)]
              (let [rule-var-set (set rule-vars)
                    outside-vars
                    (into #{}
                          (mapcat qu/collect-vars)
                          (u/remove-idxs #{clause-idx} clauses))]
                (when (and (not-any? bound-vars rule-vars)
                           (<= 2 (count (set/intersection
                                         rule-var-set outside-vars))))
                  (when-let [expansion
                             (rules/expand-nonrecursive-rule-call-for-planning
                               context orig-clause)]
                    {:clause-idx clause-idx
                     :clause     orig-clause
                     :rule       (get-in parsed-clause [:name :symbol])
                     :rule-vars  rule-vars
                     :expansion  expansion}))))))
      (map vector (:qwhere parsed-q) clauses)))))

(defn- cost-selective-rule-anchor
  [planned ^long baseline-size ^double baseline-cost candidate]
  (let [inline-planned
        (-> planned
            (splice-rule-expansion
              (:clause-idx candidate) (:expansion candidate))
            (planned-context-for-cost))
        rule-planned   (-> planned
                           (isolate-rule-expansion candidate)
                           (planned-context-for-cost))
        inline-size    (long (estimated-plan-size inline-planned))
        inline-cost    (double (estimated-context-work inline-planned))
        rule-plan-size (long (estimated-plan-size rule-planned))
        rule-plan-cost (double (estimated-context-work rule-planned))
        inline-late-expansion?
        (some (comp late-expansion-operation? late-operation)
              (:late-clauses inline-planned))
        rule-late-expansion?
        (some (comp late-expansion-operation? late-operation)
              (:late-clauses rule-planned))]
    (when (and (pos? inline-size)
               (pos? rule-plan-size)
               (<= inline-size selective-rule-anchor-max-inline-size)
               (<= rule-plan-size selective-rule-anchor-max-rule-size)
               (not inline-late-expansion?)
               (not rule-late-expansion?)
               (< (* selective-rule-anchor-dominance-margin
                     (double inline-size))
                  (double baseline-size))
               (< (* selective-rule-anchor-dominance-margin inline-cost)
                  baseline-cost))
      (assoc candidate
             :inline-size inline-size
             :inline-cost inline-cost
             :rule-plan-size rule-plan-size
             :rule-plan-cost rule-plan-cost
             :saving (- baseline-cost (+ inline-cost rule-plan-cost))))))

(defn- relation-covering-vars
  [rels vars]
  (some
    (fn [rel]
      (when (every? #(contains? (:attrs rel) %) vars) rel))
    rels))

(defn- try-selective-rule-anchor
  [{:keys [sources] :as planned} candidate ^double baseline-cost]
  (let [{:keys [^long clause-idx clause rule-vars]
         rule-plan-cost-value :rule-plan-cost} candidate
        rule-plan-cost (double rule-plan-cost-value)
        resolved
        (binding [qu/*implicit-source* (get sources '$)]
          (qresolve/resolve-clause planned clause))
        seed      (remove-materialized-clause resolved clause-idx)
        rule-rel  (relation-covering-vars (:rels seed) rule-vars)
        rule-rows (when rule-rel (relation-size rule-rel))]
    (cond
      (nil? rule-rows)
      (assoc candidate
             :context planned
             :eligible? false
             :guardrail {:reason :missing-rule-relation})

      (> (long rule-rows) selective-rule-anchor-max-output-size)
      (assoc candidate
             :context planned
             :rule-rows rule-rows
             :eligible? false
             :guardrail {:reason :rule-output-too-large
                         :limit selective-rule-anchor-max-output-size
                         :actual rule-rows})

      (zero? (long rule-rows))
      (assoc candidate
             :context (-> seed
                          (reset-cost-planning)
                          (assoc :result-set #{}))
             :rule-rows 0
             :materialization-cost rule-plan-cost
             :residual-cost 0.0
             :candidate-cost rule-plan-cost
             :materialization-stages []
             :eligible? true)

      :else
      (let [output-cost
            (materialized-output-cost
              (long rule-rows) (count (:attrs rule-rel)))
            rule-cost   (+ rule-plan-cost output-cost)
            budget      (max 0.0 (- baseline-cost rule-cost))
            propagated  (materialize-bound-patterns-with-cost seed budget)
            propagation-cost (double (:cost propagated))
            residual    (when (:eligible? propagated)
                          (-> (:context propagated)
                              (reset-cost-planning)
                              (planned-context-for-cost)))
            residual-cost (when residual (estimated-context-work residual))
            candidate-cost
            (when residual-cost
              (+ rule-cost propagation-cost (double residual-cost)))]
        (assoc candidate
               :context (or residual planned)
               :rule-rows rule-rows
               :materialization-cost
               (+ rule-cost propagation-cost)
               :materialization-stages (:stages propagated)
               :residual-cost residual-cost
               :candidate-cost candidate-cost
               :eligible? (and candidate-cost
                               (< (double candidate-cost) baseline-cost))
               :guardrail (:guardrail propagated))))))

;; Preserve the qualified context marker from plan-build across the split.
(defn materialize-selective-rule-anchors
  "Materialize an exact non-recursive rule relation before planning when the
   late rule would otherwise permit a much larger physical intermediate. A
   forced inline expansion is used only to cost the alternative; execution
   retains the rule's set-valued boundary."
  [context]
  (let [raw-candidates (selective-rule-anchor-candidates context)]
    (if (empty? raw-candidates)
      context
      (let [planned       (if (some? (:late-clauses context))
                            context
                            (planned-context-for-cost context))
            baseline-size (long (estimated-plan-size planned))
            baseline-cost (double (estimated-context-work planned))]
        (if (< baseline-size selective-rule-anchor-min-plan-size)
          (assoc planned :datalevin.query.optimizer.plan-build/selective-preplanned? true)
          (let [candidates
                (into []
                      (keep #(cost-selective-rule-anchor
                               planned baseline-size baseline-cost %))
                      (selective-rule-anchor-candidates planned))]
            (if (empty? candidates)
              (assoc planned :datalevin.query.optimizer.plan-build/selective-preplanned? true)
              (let [candidate (apply max-key :saving candidates)
                    trial     (try-selective-rule-anchor
                                planned candidate baseline-cost)
                    selected? (:eligible? trial)
                    decision
                    {:strategy (if selected?
                                 :pre-materialized-rule-anchor
                                 :planner-late-rule)
                     :rule (:rule candidate)
                     :clause (:clause candidate)
                     :baseline-size baseline-size
                     :baseline-cost baseline-cost
                     :inline-size (:inline-size candidate)
                     :inline-cost (:inline-cost candidate)
                     :rule-plan-size (:rule-plan-size candidate)
                     :rule-plan-cost (:rule-plan-cost candidate)
                     :rule-rows (:rule-rows trial)
                     :materialization-cost (:materialization-cost trial)
                     :materialization-stages (:materialization-stages trial)
                     :residual-cost (:residual-cost trial)
                     :candidate-cost (:candidate-cost trial)
                     :guardrail (:guardrail trial)}]
                (record-selective-value-decision! decision)
                (if selected?
                  (assoc (:context trial) :datalevin.query.optimizer.plan-build/selective-preplanned? true)
                  (assoc planned :datalevin.query.optimizer.plan-build/selective-preplanned? true))))))))))

(defn materialize-selective-value-lookups
  "Cost non-unique constant AVE lookups as possible pre-planning entity
   relations. A candidate first has to beat the normal plan step that would
   introduce its entity. The fully propagated relation and residual plan must
   then beat the unchanged plan before the rewrite is accepted."
  [context]
  (loop [context  context
         rejected #{}
         preplanned nil]
    (let [raw-candidates (remove #(contains? rejected (:key %))
                                 (selective-value-candidates context))]
      (if (empty? raw-candidates)
        (cond-> (or preplanned context)
          preplanned (assoc :datalevin.query.optimizer.plan-build/selective-preplanned? true))
        (let [planned        (or preplanned
                                 (planned-context-for-cost context))
              base-cost      (estimated-context-work planned)
              candidates     (costed-selective-value-candidates
                               planned base-cost raw-candidates)]
          (if (empty? candidates)
            (assoc planned :datalevin.query.optimizer.plan-build/selective-preplanned? true)
            (let [candidate     (apply max-key :saving candidates)
                  trial         (try-selective-value-candidate
                                  context candidate
                                  (min (double base-cost)
                                       (double (:delayed-cost candidate))))
                  trial-planned (when (:eligible? trial)
                                  (planned-context-for-cost (:context trial)))
                  residual-cost (when trial-planned
                                  (estimated-context-work trial-planned))
                  trial-cost    (when residual-cost
                                  (+ (double (:materialization-cost trial))
                                     (double residual-cost)))
                  selected?     (and trial-cost
                                     (< (double trial-cost)
                                        (double base-cost)))
                  decision      {:strategy
                                 (if selected?
                                   :pre-materialized-value-lookup
                                   :planner-value-lookup)
                                 :pattern  (:pattern candidate)
                                 :entity   (:entity-sym candidate)
                                 :fanout   (:fanout candidate)
                                 :lookup-cost (:anchor-cost candidate)
                                 :delayed-cost (:delayed-cost candidate)
                                 :materialization-cost
                                 (:materialization-cost trial)
                                 :materialization-stages
                                 (:materialization-stages trial)
                                 :residual-cost residual-cost
                                 :guardrail (:guardrail trial)
                                 :baseline-cost base-cost
                                 :candidate-cost trial-cost}]
              (record-selective-value-decision! decision)
              (if selected?
                (recur (:context trial) #{} trial-planned)
                (recur context (conj rejected (:key candidate))
                       planned)))))))))
