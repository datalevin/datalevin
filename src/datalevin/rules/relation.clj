;;
;; Copyright (c) Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns ^:no-doc datalevin.rules.relation
  "Rule-result projection, argument unification, delta bindings, and seed relations."
  (:require
   [datalevin.constants :as c]
   [datalevin.query-util :as qu]
   [datalevin.relation :as r]
   [datalevin.rules.clause :refer [rule-args rule-call? rule-head]]
   [datalevin.util :as u :refer [raise]])
  (:import
   [org.eclipse.collections.impl.list.mutable FastList]
   [java.util List HashSet]))

(defn empty-rel-for-rule
  "Create an empty relation with attributes matching the rule head args"
  [rule-name rules]
  (let [head-vars (rest (ffirst (get rules rule-name)))]
    (r/relation! (zipmap head-vars (range)) (FastList.))))

(defn project-rule-result
  "Project body-rel to head-vars"
  [body-rel head-vars]
  (if (seq head-vars)
    (let [tuples      ^List (:tuples body-rel)
          m           (.size tuples)
          final-attrs (zipmap head-vars (range))]
      (if (zero? m)
        (r/relation! final-attrs (FastList.))
        (let [attrs      (:attrs body-rel)
              ^ints idxs (int-array (map attrs head-vars))
              n          (alength idxs)]
          (r/relation!
            final-attrs
            (let [res (FastList. m)]
              (dotimes [i m]
                (let [from ^objects (.get tuples i)
                      to   (object-array n)]
                  (dotimes [j n]
                    (aset to j (aget from (aget idxs j))))
                  (.add res to)))
              res)))))
    body-rel))

(defn project-rule-result-distinct
  "Project a completed rule body to its set-valued predicate relation."
  [body-rel head-vars]
  (cond
    (r/rel-empty body-rel)
    (r/relation! (zipmap head-vars (range)) (FastList.))

    (seq head-vars)
    (r/project-distinct body-rel head-vars)

    :else
    (r/dedupe-rel body-rel)))

(defn- head-indices
  [attrs rule-head-vars]
  (int-array
    (mapv (fn [v]
            (if-let [i (attrs v)]
              i
              (raise "Missing var in rule-rel attrs"
                     {:var v :attrs attrs :head rule-head-vars})))
          rule-head-vars)))

(defn- const-indices
  [^ints head-idxs call-args]
  (let [checks (into []
                     (keep-indexed
                       (fn [idx arg]
                         (when-not (qu/free-var? arg)
                           [(aget head-idxs idx) arg])))
                     call-args)
        n      (count checks)
        idxs   (int-array n)
        values (object-array n)]
    (dotimes [i n]
      (let [[idx value] (checks i)]
        (aset idxs i (int idx))
        (aset values i value)))
    [idxs values]))

(defn- const-check
  [^ints idxs ^objects values ^objects tuple]
  (let [n (alength idxs)]
    (loop [i 0]
      (if (< i n)
        (if (= (aget tuple (aget idxs i)) (aget values i))
          (recur (unchecked-inc-int i))
          false)
        true))))

(defn- var-positions
  [call-args]
  (u/reduce-indexed
    (fn [m arg idx]
      (if (qu/free-var? arg)
        (update m arg (fnil conj []) idx)
        m))
    {} call-args))

(defn- equality-indices
  [^ints head-idxs var->positions]
  (let [pairs (into []
                    (mapcat
                      (fn [[_ positions]]
                        (when (< 1 (count positions))
                          (let [first-idx (aget head-idxs (first positions))]
                            (map (fn [position]
                                   [first-idx (aget head-idxs position)])
                                 (rest positions))))))
                    var->positions)
        n     (count pairs)
        left  (int-array n)
        right (int-array n)]
    (dotimes [i n]
      (let [[left-idx right-idx] (pairs i)]
        (aset left i (int left-idx))
        (aset right i (int right-idx))))
    [left right]))

(defn- unique-vars
  [call-args]
  (into []
        (comp (filter qu/free-var? )
           (distinct))
        call-args))

(defn- projection-indices
  [var->positions ^ints head-idxs unique-call-vars]
  (int-array (mapv #(aget head-idxs (first (var->positions %)))
                   unique-call-vars)))

(defn- equality-check
  [^ints left ^ints right ^objects tuple]
  (let [n (alength left)]
    (loop [i 0]
      (if (< i n)
        (if (= (aget tuple (aget left i)) (aget tuple (aget right i)))
          (recur (unchecked-inc-int i))
          false)
        true))))

(defn- identity-indices?
  [^ints idxs]
  (let [n (alength idxs)]
    (loop [i 0]
      (if (< i n)
        (and (= i (aget idxs i))
             (recur (unchecked-inc-int i)))
        true))))

(defn map-rule-result
  "transforms evaluated rule-rel into a result rel tailored to the rule call"
  [rule-rel rule-head-vars call-args]
  (let [attrs            (:attrs rule-rel)
        ^List tuples     (:tuples rule-rel)
        ^ints head-idxs  (head-indices attrs rule-head-vars)
        [^ints const-idxs ^objects const-values]
        (const-indices head-idxs call-args)
        var->positions   (var-positions call-args)
        [^ints equality-left ^ints equality-right]
        (equality-indices head-idxs var->positions)
        unique-call-vars (unique-vars call-args)
        ^ints projection-idxs
        (projection-indices var->positions head-idxs unique-call-vars)
        n                (alength projection-idxs)
        result-attrs     (zipmap unique-call-vars (range))]
    (r/with-unique-key
      (if (and (= n (count attrs))
               (zero? (alength const-idxs))
               (zero? (alength equality-left))
               (identity-indices? projection-idxs))
        (r/relation! result-attrs tuples)
        (r/relation!
          result-attrs
          (let [size (.size tuples)
                acc  (FastList. size)
                seen (HashSet. size)
                key  (r/array-lookup)]
            (dotimes [i size]
              (let [^objects tuple (.get tuples i)]
                (when (and (const-check const-idxs const-values tuple)
                           (equality-check equality-left equality-right tuple))
                  (let [to (object-array n)]
                    (dotimes [j n]
                      (aset to j (aget tuple (aget projection-idxs j))))
                    (when-not (.contains seen (r/reset-array-lookup! key to))
                      (.add seen (r/wrap-array to))
                      (.add acc to))))))
            acc)))
      unique-call-vars)))

(defn- delta-values-for-column
  "Return distinct values in a delta column, or nil when the cap is exceeded."
  [^List tuples ^long idx ^long limit]
  (let [res (HashSet.)
        n   (.size tuples)]
    (loop [i 0]
      (if (< i n)
        (do
          (.add res (aget ^objects (.get tuples i) idx))
          (if (> (.size res) limit)
            nil
            (recur (unchecked-inc i))))
        res))))

(defn- intersect-hash-sets
  [^HashSet a ^HashSet b]
  (let [a-size (.size a)
        b-size (.size b)
        small  (if (< a-size b-size) a b)
        large  (if (< a-size b-size) b a)
        res    (HashSet. (min a-size b-size))]
    (doseq [v small]
      (when (.contains large v)
        (.add res v)))
    res))

(defn- merge-delta-bound-set
  [acc var ^HashSet values]
  (if-let [^HashSet existing (get acc var)]
    (assoc acc var (intersect-hash-sets existing values))
    (assoc acc var values)))

(defn extract-delta-bound-values
  "Pre-extract bounded delta values for vars used as recursive call args.
   The bound variables must be derived from the branch's rule-call arguments,
   not from the caller rule's head vars."
  [context branch]
  (let [limit (long c/rule-delta-index-threshold)
        res
        (reduce
          (fn [acc clause]
            (if (and (sequential? clause) (rule-call? context clause))
              (let [rname     (rule-head clause)
                    rel       (get (:rule-rels context) rname)
                    branches  (get (:rules context) rname)
                    head-vars (when branches (rest (ffirst branches)))
                    call-args (rule-args clause)
                    ^List tuples (:tuples rel)]
                (if (and tuples (pos? (.size tuples)) (seq head-vars))
                  (reduce
                    (fn [acc [head-var arg]]
                      (if (qu/binding-var? arg)
                        (if-let [idx ((:attrs rel) head-var)]
                          (if-let [values (delta-values-for-column
                                            tuples (long idx) limit)]
                            (merge-delta-bound-set acc arg values)
                            acc)
                          acc)
                        acc))
                    acc (map vector head-vars call-args))
                  acc))
              acc))
          {} (rest branch))]
    (when (seq res) res)))

(defn unique-seeds
  [rel idx]
  (let [^List tuples (:tuples rel)
        size         (.size tuples)]
    (if tuples
      (let [seen (HashSet. size)
            res  (FastList. size)]
        (dotimes [i size]
          (let [^objects tuple (.get tuples i)
                v              (aget tuple idx)]
            (when (.add seen v)
              (.add res (object-array [v])))))
        res)
      (FastList.))))

(defn rename-rel-attrs
  [rel head-vars]
  (assoc rel :attrs (zipmap head-vars (range))))
