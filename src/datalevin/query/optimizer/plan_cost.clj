;;
;; Copyright (c) Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns ^:no-doc datalevin.query.optimizer.plan-cost
  "Clause cardinalities and costs for scan plans, late operations, and
  access sampling budgets."
  (:require
   [datalevin.constants :as c]
   [datalevin.datom :as dd]
   [datalevin.db :as db]
   [datalevin.interface
    :refer [av-size populated?]]
   [datalevin.parser :as dp]
   [datalevin.query.optimizer.estimates
    :refer [estimate-link-cost estimate-round estimate-row-evaluation-cost
            n-costly-preds verified-non-empty-size]]
   [datalevin.query.optimizer.sampling
    :refer [estimated-late-input-size sampled-access-cardinality]]
   [datalevin.util
    :refer [cond+]])
  (:import
   [java.util List]
   [datalevin.db DB]
   [datalevin.parser Function Or Pattern Predicate Not RuleExpr]))

(defn- nillify [v] (if (or (identical? v c/v0) (identical? v c/vmax)) nil v))

(defn- range->start-end [[[_ lv] [_ hv]]] [(nillify lv) (nillify hv)])

(defn- range-count
  [db attr ranges ^long cap]
  (if (identical? ranges :empty-range)
    0
    (unreduced
      (reduce
        (fn [^long sum range]
          (let [s (+ sum (let [[lv hv] (range->start-end range)]
                           ^long (db/-index-range-size db attr lv hv)))]
            (if (< s cap) s (reduced cap))))
        0 ranges))))

(defn zero-count-clause-size
  "Fast clause counts come from the counted-index metadata, which has been
  observed to report 0 for an attribute that still holds datoms (issue #371).
  A zero count is a correctness decision, not an estimate -- it short-circuits
  the whole query to an empty result -- so it must be confirmed against the
  actual index before being trusted. Returns 0 only when the clause is truly
  empty; otherwise returns a conservative non-empty size that keeps planning on
  the sampled path."
  ^long [^DB db e {:keys [attr val range]}]
  (let [store (.-store db)]
    (if (and (some-> ((db/-schema db) attr) :db/aid)
             (cond
               (int? e)
               (populated? store :eav (dd/datom e attr c/v0)
                           (dd/datom e attr c/vmax))

               (some? val)
               (populated? store :ave (dd/datom c/e0 attr val)
                           (dd/datom c/emax attr val))

               range
               (when-not (identical? range :empty-range)
                 (some (fn [r]
                         (let [[lv hv] (range->start-end r)]
                           (populated? store :ave
                                       (dd/datom c/e0 attr lv)
                                       (dd/datom c/emax attr hv))))
                       range))

               :else
               (populated? store :ave (dd/datom c/e0 attr nil)
                           (dd/datom c/emax attr nil))))
      verified-non-empty-size
      0)))

(defn ^:redef fast-clause-count
  "Fast datom count for a clause, backed by the counted-index metadata.
  May be inaccurate; a zero result must be verified by
  `zero-count-clause-size` before it short-circuits planning (issue #371).
  ^:redef so tests can simulate counted-index metadata drift."
  ^long [^DB db e {:keys [attr val range]} ^long mcount]
  (let [store (.-store db)]
    (cond
      (int? e)    (db/-count db [e attr nil] mcount)
      (some? val) (av-size store attr val)
      range       (range-count db attr range mcount)
      :else       (db/-count db [nil attr nil] mcount))))

(defn adjusted-scan-ratio
  ^double
  [^long input-size ^long output-size]
  (if (and (pos? input-size) (pos? output-size))
    (/ (double output-size) input-size)
    (double c/magic-scan-ratio)))

(defn- n-items
  [attrs-v k]
  (reduce
    (fn [^long c [_ m]] (if (m k) (inc c) c))
    0 attrs-v))

(defn estimate-scan-v-size
  [^long e-size steps]
  (cond+
    (<= (count steps) 1) e-size ; no merge step

    :let [{:keys [know-e?] res1 :result sp1 :sample} (first steps)
          {:keys [attrs-v result sample]} (peek steps)]

    (:deferred-projection? (peek steps)) e-size

    know-e? (count attrs-v)

    :else
    (estimate-round
      (* e-size (double
                  (cond
                    result (adjusted-scan-ratio
                             (.size ^List res1)
                             (.size ^List result))
                    sample (adjusted-scan-ratio
                             (.size ^List sp1)
                             (.size ^List sample))
                    :else c/magic-scan-ratio))))))

(defn- factor
  [magic ^long n]
  (if (zero? n) 1 ^long (estimate-round (* ^double magic n))))

(defn- fused-var-factor
  [^long n]
  (if (zero? n)
    1
    (estimate-round
      (* ^double c/magic-cost-var
         (+ 1.0 (* ^double c/magic-cost-fused-var-marginal
                   (double (dec n))))))))

(defn estimate-scan-v-cost
  [{:keys [attrs-v vars cost-attrs-v cost-var-count]} ^long size]
  (let [cost-attrs-v (or cost-attrs-v attrs-v)
        var-count    (long (or cost-var-count (count vars)))]
    (* size
       ^double c/magic-cost-merge-scan-v
       ^long (fused-var-factor var-count)
       ^long (factor c/magic-cost-pred (n-costly-preds cost-attrs-v))
       ^long (factor c/magic-cost-fidx (n-items cost-attrs-v :fidx)))))

(defn estimate-base-cost
  [{:keys [mcount]} steps]
  (let [{:keys [pred]} (first steps)
        init-cost      (estimate-round
                         (cond-> (* ^double c/magic-cost-init-scan-e
                                    ^long mcount)
                           pred (* ^double c/magic-cost-pred)))]
    (if (< 1 (count steps))
      (+ ^long init-cost ^long (estimate-scan-v-cost (peek steps) mcount))
      init-cost)))

(defn final-plan-cost ^double
  [plan-trace]
  (if-let [{:keys [steps cost]} (last plan-trace)]
    (if (some? cost)
      (double cost)
      (if (seq steps)
        (double
          (estimate-base-cost
            {:mcount (long (or (:mcount (first steps)) 1))}
            steps))
        0.0))
    0.0))

(defn estimated-plan-cost
  "Return the existing planner's estimated cost for all final component plans."
  [{:keys [plan result-set]}]
  (if (= result-set #{})
    0.0
    (reduce
      (fn [cost [_src components]]
        (+ (double cost)
           (double
             (reduce
               (fn [cost plan-trace]
                 (+ (double cost) (final-plan-cost plan-trace)))
               0.0 components))))
      0.0 plan)))

(defn late-operation
  [clause]
  (let [parsed (if (or (instance? Predicate clause)
                       (instance? Function clause)
                       (instance? Pattern clause)
                       (instance? Not clause)
                       (instance? Or clause)
                       (instance? RuleExpr clause))
                 clause
                 (dp/parse-clause clause))]
    (cond
      (instance? Pattern parsed)   :indexed-join
      (instance? Predicate parsed) :predicate
      (instance? Function parsed)  :function
      (instance? Not parsed)       :not
      (instance? Or parsed)        :or
      (instance? RuleExpr parsed)  :rule
      :else                        :residual)))

(defn- late-row-operation?
  [operation]
  (#{:predicate :function} operation))

(defn late-expansion-operation?
  [operation]
  (#{:indexed-join :or :rule} operation))

(defn access-sample-cost-budget
  "Bound physical-access sampling by the conventional plan when that plan has
  a complete cardinality estimate. An unresolved late join/rule is precisely
  where sampling supplies missing expansion evidence, so its underestimated
  conventional cost is not a sound boundary."
  [context]
  (when-not (some (comp late-expansion-operation? late-operation)
                  (:late-clauses context))
    (let [cost (estimated-plan-cost context)]
      ;; A zero cost means the conventional root has not supplied a useful
      ;; boundary (for example, a query planned entirely from existing
      ;; relations). Treating it as a real budget would reject even a
      ;; one-row access sample.
      (when (pos? (double cost)) cost))))

(defn estimated-late-cost
  [context ^long plan-size access-plans]
  (let [input-size  (estimated-late-input-size context plan-size)
        operations  (mapv late-operation (:late-clauses context))
        deferred    (set (get-in context
                                 [:post-top-k-enrichment :clauses]))
        expansion?  (some late-expansion-operation? operations)
        sampled     (when expansion?
                      (sampled-access-cardinality access-plans))
        output-size (max input-size (long (or (:rows sampled) 0)))
        expanded?   (< input-size output-size)
        expansion-cost
        (if expanded?
          (estimate-link-cost input-size output-size)
          0)
        initial
        {:cost expansion-cost
         :output-size output-size
         :stages
         (cond-> []
           expanded?
           (conj (assoc sampled
                        :operation :sampled-late-expansion
                        :input input-size
                        :output output-size
                        :cost expansion-cost)))}]
    (reduce
      (fn [{:keys [cost stages] :as estimate} [clause operation]]
        (if (late-row-operation? operation)
          (let [post-top-k? (contains? deferred clause)
                stage-input (if post-top-k?
                              (min output-size
                                   (long
                                     (get-in context
                                             [:post-top-k-enrichment :limit]
                                             output-size)))
                              output-size)
                stage-cost (estimate-row-evaluation-cost stage-input)]
            {:cost   (+ (double cost) (double stage-cost))
             :output-size output-size
             :stages (conj stages
                           (cond-> {:operation operation
                                    :input     stage-input
                                    :cost      stage-cost
                                    :clause    clause}
                             post-top-k? (assoc :post-top-k? true)))})
          estimate))
      initial
      (map vector (:late-clauses context) operations))))
