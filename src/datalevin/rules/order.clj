;;
;; Copyright (c) Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns ^:no-doc datalevin.rules.order
  "Cardinality estimates and dependency-aware ordering of rule-body clauses."
  (:require
   [clojure.set :as set]
   [datalevin.constants :as c]
   [datalevin.db :as db]
   [datalevin.parser :as dp]
   [datalevin.query-util :as qu]
   [datalevin.rules.clause
    :refer [clause->pattern clause-bound-vars clause-free-vars clause-pattern
            clause-required-vars clause-source context-bound-vars rule-call?
            rule-head values-for-var]]
   [datalevin.util :refer [concatv]])
  (:import
   [datalevin.db DB]
   [java.util List]))

(defn- rel-size
  [rel]
  (let [^List tuples (:tuples rel)]
    (if tuples (.size tuples) 0)))

(defn cached-rule-rel-size
  "Get a stable size for a rule call. Uses either :rule-totals, which contains
   pre-computed results for non-recursive external rules, or an explicit magic
   seed. Does NOT fall back to :rule-rels, which may contain deltas during
   iteration."
  [context clause]
  (when (rule-call? context clause)
    (when-let [rel (or (get-in context [:rule-totals (rule-head clause)])
                       (get-in context [:magic-seeds (rule-head clause)]))]
      (rel-size rel))))

(defn- unbound-eav-pattern?
  [clause bound]
  (let [[e _ v] (clause-pattern clause)]
    (and (or (qu/free-var? e) (= e '_))
         (or (qu/free-var? v) (= v '_))
         (not (contains? bound e))
         (not (contains? bound v)))))

(defn- scale-estimate
  [^long n ^long factor]
  (if (or (= n Long/MAX_VALUE) (= factor 1))
    n
    (let [limit (quot Long/MAX_VALUE factor)]
      (if (> n limit)
        Long/MAX_VALUE
        (unchecked-multiply n factor)))))

(def ^:private ^:const rule-bound-count-limit
  "Maximum materialized values probed when costing a bound rule pattern."
  128)

(defn- saturating-count-add
  ^long [^long x ^long y]
  (if (> y (- Long/MAX_VALUE x))
    Long/MAX_VALUE
    (+ x y)))

(defn- pattern-count
  ^long [^DB source pattern]
  (long (or (db/-count source pattern) Long/MAX_VALUE)))

(defn- bounded-pattern-count-sum
  [^DB source values pattern-fn]
  (when (<= (long (count values)) (long rule-bound-count-limit))
    (reduce
      (fn [^long total value]
        (saturating-count-add total
                              (pattern-count source (pattern-fn value))))
      0 values)))

(defn- context-values-for-term
  [context term]
  (when (and (qu/binding-var? term)
             (contains? (context-bound-vars context) term))
    (values-for-var context term)))

(defn- bound-pattern-size
  "Estimate an EAV clause from small value domains already materialized in
   the rule context. Returns nil when the domains are too large to probe."
  [^DB source clause context]
  (let [[e a v]   (clause-pattern clause)
        a          (if (or (qu/free-var? a) (= a '_)) nil a)
        e-values   (context-values-for-term context e)
        v-values   (context-values-for-term context v)
        fixed-e    (when-not (or (qu/free-var? e) (= e '_)) e)
        fixed-v    (when-not (or (qu/free-var? v) (= v '_)) v)
        from-e     (when (some? e-values)
                     (bounded-pattern-count-sum
                       source e-values #(vector % a fixed-v)))
        from-v     (when (some? v-values)
                     (bounded-pattern-count-sum
                       source v-values #(vector fixed-e a %)))]
    (cond
      (and (some? from-e) (some? from-v))
      (min (long from-e) (long from-v))
      (some? from-e) from-e
      (some? from-v) from-v)))

(defn estimate-clause-size
  [clause context bound]
  (cond
    (and (vector? clause)
         (dp/parse-pattern clause)
         (not (dp/parse-pred clause))
         (not (dp/parse-fn clause)))
    (if-let [^DB db (clause-source clause context)]
      (try
        (let [n (or (bound-pattern-size db clause context)
                    (pattern-count db (clause->pattern clause)))]
          (if (unbound-eav-pattern? clause bound)
            (scale-estimate n c/rule-unbound-pattern-penalty)
            n))
        (catch Exception _ Long/MAX_VALUE))
      Long/MAX_VALUE)

    (sequential? clause)
    (if (rule-call? context clause)
      (or (cached-rule-rel-size context clause) Long/MAX_VALUE)
      0)

    :else Long/MAX_VALUE))

(defn reorder-clauses
  [clauses context]
  (let [bound (volatile! (context-bound-vars context))]
    (loop [remaining clauses
           ordered   []]
      (if (empty? remaining)
        ordered
        (let [candidates-indices
              (keep-indexed
                (fn [idx clause]
                  (when (set/subset?
                          (clause-required-vars clause context) @bound)
                    idx))
                remaining)

              candidates-indices
              (if (seq candidates-indices)
                candidates-indices
                (range (count remaining)))

              scored-candidates
              (mapv
                (fn [idx]
                  (let [clause (nth remaining idx)]
                    [idx
                     [(- (count (set/intersection
                                  (clause-free-vars clause) @bound)))
                      (estimate-clause-size clause context @bound)]]))
                candidates-indices)

              ^long best-idx (ffirst (sort-by second scored-candidates))

              best-clause (nth remaining best-idx)]

          (vswap! bound set/union (clause-bound-vars best-clause context))
          (recur (concatv (take best-idx remaining)
                          (drop (inc best-idx) remaining))
                 (conj ordered best-clause)))))))
