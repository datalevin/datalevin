;;
;; Copyright (c) Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns ^:no-doc datalevin.rules.clause
  "Rule heads, source propagation, variable analysis, and renaming."
  (:require
   [clojure.walk :as walk]
   [datalevin.parser :as dp]
   [datalevin.query-util :as qu])
  (:import
   [java.util List HashSet]))

(defn rule-head
  [clause]
  (if (qu/source? (first clause))
    (second clause)
    (first clause)))

(defn rule-args
  [clause]
  (if (qu/source? (first clause))
    (nnext clause)
    (rest clause)))

(defn source [clause] (let [src (first clause)] (when (qu/source? src) src)))

(defn ensure-src
  [src clause]
  (if (nil? src)
    clause
    (if (vector? clause)
      (into [src] clause)
      (cons src clause))))

(defn- sourceable-clause?
  "Return true if a clause can be prefixed with a source.
   Predicate and function clauses should not be source-prefixed since
   the parser does not treat them as source-aware."
  [clause]
  (not (or (dp/parse-pred clause)
           (dp/parse-fn clause))))

(defn ensure-src-where
  [src clause]
  (if (and src (sourceable-clause? clause))
    (ensure-src src clause)
    clause))

;; stratification

(defn rename-rule
  [branches]
  (let [mapping (volatile! {})]
    (walk/postwalk
      (fn [x]
        (if (qu/free-var? x)
          (if-let [n (@mapping x)]
            n
            (let [n (gensym (name x))]
              (vswap! mapping assoc x n)
              n))
          x))
      branches)))

(defn rule-call?
  [context clause]
  (when (sequential? clause)
    (let [head (rule-head clause)]
      (and (symbol? head)
           (not (qu/free-var? head))
           (not (qu/rule-head head))
           ((:rules context) head)))))

(defn clause-required-vars
  "Vars that must be bound before evaluating a clause."
  [clause context]
  (cond
    ;; or-join: only the binding vector vars that appear in input positions
    ;; are required, not all internal vars
    (and (sequential? clause)
         (not (vector? clause))
         (= 'or-join (first clause)))
    (let [[_ binding-vars & branches] clause]
      ;; For or-join, we need the first binding var(s) that will be used
      ;; to filter/seed the branches. Analyze which vars appear in E position
      ;; (required) vs V position (produced) in the branches.
      (into #{}
            (filter
              (fn [v]
                (and (qu/free-var? v)
                     ;; Check if this var appears in E position in any branch
                     (some (fn [branch]
                             (let [clauses (if (and (seq? branch)
                                                    (= 'and (first branch)))
                                             (rest branch)
                                             [branch])]
                               (some (fn [c] (and (vector? c) (= v (first c))))
                                     clauses)))
                           branches))))
            binding-vars))

    ;; predicate style list, but not a rule call
    (and (sequential? clause)
         (not (vector? clause))
         (not (rule-call? context clause)))
    (into #{} (filter qu/free-var?) (flatten clause))

    ;; function binding clause: [(f ?in ...) ?out]
    (and (vector? clause) (sequential? (first clause)))
    (into #{} (filter qu/free-var?) (rest (first clause)))

    :else #{}))

(defn clause-bound-vars
  "Vars that become bound after a clause is evaluated."
  [clause context]
  (cond
    ;; or-join: only the binding vector vars become bound, not internal vars
    (and (sequential? clause)
         (not (vector? clause))
         (= 'or-join (first clause)))
    (let [[_ binding-vars & _] clause]
      (into #{} (filter qu/free-var?) binding-vars))

    (and (sequential? clause)
         (not (vector? clause)))
    (if (rule-call? context clause)
      (into #{} (filter qu/free-var?) (rest clause))
      (into #{} (filter qu/free-var?) clause))

    (vector? clause)
    (into #{} (filter qu/free-var?) (flatten clause))

    :else #{}))

(defn clause-free-vars
  [clause]
  (if (and (sequential? clause)
           (not (vector? clause))
           (= 'or-join (first clause)))
    ;; For or-join, only consider binding vector vars for reordering purposes
    (let [[_ binding-vars & _] clause]
      (into #{} (filter qu/free-var?) binding-vars))
    (into #{} (filter qu/free-var?) (flatten clause))))

(defn context-bound-vars
  [context]
  (into #{} (mapcat #(keys (:attrs %))) (:rels context)))

(defn values-for-var
  [context v]
  (let [values (HashSet.)]
    (doseq [rel (:rels context)
            :let [idx ((:attrs rel) v)]
            :when (some? idx)]
      (let [tuples ^List (:tuples rel)]
        (dotimes [i (.size tuples)]
          (.add values (aget ^objects (.get tuples i) ^long idx)))))
    (vec values)))

(defn clause-source
  [clause context]
  (let [src-sym (if (qu/source? (first clause))
                  (first clause)
                  '$)]
    (get-in context [:sources src-sym])))

(defn clause-pattern
  [clause]
  (if (qu/source? (first clause))
    (subvec clause 1)
    clause))

(defn clause->pattern
  [clause]
  (mapv #(if (or (qu/free-var? %) (= % '_)) nil %)
        (clause-pattern clause)))

(defn bound-arg-indices
  "Indices of args that are already bound (via outer context or constants)."
  [args context]
  (let [bound (context-bound-vars context)]
    (into #{}
          (keep-indexed (fn [idx arg]
                          (cond
                            (not (qu/free-var? arg)) idx
                            (bound arg)              idx
                            :else                    nil)))
          args)))

(defn recursive-branch?
  [branch scc]
  (let [clauses (rest branch)]
    (some
      (fn [clause]
        (when (sequential? clause)
          (scc (rule-head clause))))
      clauses)))

(defn- rule-call-heads
  [form rules-context]
  (into #{}
        (keep (fn [node]
                (when (rule-call? rules-context node)
                  (rule-head node))))
        (tree-seq sequential? seq form)))

(defn external-rule-heads
  [branches rules-context stratum-set]
  (into #{}
        (comp
          (mapcat rest)
          (mapcat #(rule-call-heads % rules-context))
          (remove stratum-set))
        branches))

(defn- vector-binds-var?
  "True if a binding clause produces the given var as an output."
  [v clause]
  (and (vector? clause)
       (some #(and (symbol? %) (= v %))
             (remove sequential? (rest clause)))))

(defn stable-head-idxs
  "Conservative check for head vars that stay unchanged through recursion.
   Only used for single-rule strata to safely push bound arguments
   (magic-ish seeds)."
  [branches stratum-set]
  (if (not= 1 (count stratum-set))
    #{}
    (let [head-vars (rest (ffirst branches))]
      (into #{}
            (keep-indexed
              (fn [idx hv]
                (when (every?
                        (fn [branch]
                          (let [clauses (rest branch)]
                            (and
                              (not-any? #(vector-binds-var? hv %) clauses)
                              (every? (fn [clause]
                                        (let [args (rule-args clause)]
                                          (= hv (nth args idx))))
                                      ;; recursive calls
                                      (filterv
                                        (fn [clause]
                                          (when (sequential? clause)
                                            (stratum-set (rule-head clause))))
                                        clauses)))))
                        branches)
                  idx)))
            head-vars))))
