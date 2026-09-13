;;
;; Copyright (c) Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns ^:no-doc datalevin.rules.magic
  "Magic-set adornment, seed construction, and rule-program rewriting."
  (:require
   [clojure.set :as set]
   [clojure.string :as str]
   [datalevin.query-util :as qu]
   [datalevin.relation :as r]
   [datalevin.rules.clause
    :refer [clause-free-vars rule-args rule-call? rule-head stable-head-idxs
            values-for-var]]
   [datalevin.util :refer [concatv]])
  (:import
   [org.eclipse.collections.impl.list.mutable FastList]
   [java.util List]))

(def ^:private magic-special-heads #{'or 'or-join 'and 'not 'not-join})

(defn magic-head? [sym] (str/starts-with? (name sym) "magic__"))

(defn magic-rules-size
  "Sum the tuple count of all magic-prefixed rules in the given relations map."
  ^long [rule-rels]
  (reduce-kv
    (fn [^long acc rname rel]
      (if (and (symbol? rname) (magic-head? rname))
        (let [^List tuples (:tuples rel)]
          (if tuples
            (+ acc (.size tuples))
            acc))
        acc))
    0 rule-rels))

(defn- flatten-head-vars
  [head-clause]
  (let [args (rest head-clause)]
    (if (and (seq args) (vector? (first args)))
      (concatv (first args) (rest args))
      (vec args))))

(defn- bound-arg?
  [arg bound-vars]
  (and (not= arg '_)
       (or (not (qu/free-var? arg))
           (contains? bound-vars arg))))

(defn binding-pattern
  [args bound-vars]
  (mapv (fn [arg] (if (bound-arg? arg bound-vars) :b :f)) args))

(defn bound-indices
  [pattern]
  (keep-indexed (fn [idx p] (when (= p :b) idx)) pattern))

(defn- pattern-suffix
  [pattern]
  (apply str (mapv #(if (= % :b) "b" "f") pattern)))

(defn- adorned-name
  [rule-name pattern]
  (symbol (str (name rule-name) "__" (pattern-suffix pattern))))

(defn magic-name
  [adorned-rule]
  (symbol (str "magic__" (name adorned-rule))))

(defn- replace-rule-head
  [clause new-head]
  (if (qu/source? (first clause))
    (list* (first clause) new-head (nnext clause))
    (list* new-head (rest clause))))

(defn- predicate-clause?
  [clause]
  (and (vector? clause)
       (sequential? (first clause))
       (= 1 (count clause))))

(defn- fn-binding-clause?
  [clause]
  (and (vector? clause)
       (sequential? (first clause))
       (< 1 (count clause))))

(defn- complex-clause?
  [clause]
  (and (sequential? clause)
       (let [head (if (qu/source? (first clause))
                    (second clause)
                    (first clause))]
         (contains? magic-special-heads head))))

(defn- clause-output-vars
  [clause context]
  (cond
    (rule-call? context clause)
    (into #{} (filter qu/free-var?) (rule-args clause))

    (fn-binding-clause? clause)
    (into #{} (filter qu/free-var?) (rest clause))

    (predicate-clause? clause)
    #{}

    (vector? clause)
    (into #{} (filter qu/free-var?) clause)

    :else #{}))

(defn magic-seed-rel
  [context head-vars bound-args]
  (let [values (mapv (fn [arg]
                       (if (qu/free-var? arg)
                         (values-for-var context arg)
                         [arg]))
                     bound-args)
        attrs  (zipmap head-vars (range))]
    (if (or (empty? head-vars) (some empty? values))
      (r/relation! attrs (FastList.))
      (r/relation! attrs (r/many-tuples values)))))

(defn- magic-bind-clauses
  [head-vars bound-args]
  (mapv (fn [hv arg]
          [(list 'identity arg) hv])
        head-vars bound-args))

(defn- simple-branches?
  [branches]
  (every?
    (fn [branch]
      (every? (complement complex-clause?) (rest branch)))
    branches))

(defn positive-recursive?
  [rules stratum]
  (let [stratum-set (set stratum)]
    (and
      (every?
        (fn [rname]
          (every?
            (fn [branch]
              (every?
                (fn [clause]
                  (not (and (sequential? clause)
                            (let [head (if (qu/source? (first clause))
                                         (second clause)
                                         (first clause))]
                              (#{'not 'not-join} head)))))
                (rest branch)))
            (rules rname)))
        stratum-set)
      (every? (comp simple-branches? rules) stratum-set))))

(defn magic-effective?
  "Check if magic set rewrite would be effective for the given rule and bound
   pattern. Magic is ineffective when bound head vars don't appear in any
   non-recursive body clause of a recursive branch - they can't filter
   intermediate results, causing cross-product scans."
  [rules rule-name bound-idxs stratum-set]
  (let [branches  (rules rule-name)
        head-vars (vec (rest (ffirst branches)))]
    (every?
      (fn [branch]
        (let [body-clauses (rest branch)
              ;; Check if this branch has any recursive calls
              has-recursive? (some (fn [clause]
                                     (and (sequential? clause)
                                          (stratum-set (rule-head clause))))
                                   body-clauses)]
          (if has-recursive?
            ;; For recursive branches, bound vars must appear in non-recursive
            ;; clauses to effectively filter intermediate results
            (let [non-rec-clauses (remove (fn [clause]
                                            (and (sequential? clause)
                                                 (stratum-set (rule-head clause))))
                                          body-clauses)
                  non-rec-vars    (into #{} (mapcat clause-free-vars)
                                        non-rec-clauses)
                  bound-vars      (into #{} (map head-vars) bound-idxs)]
              ;; At least one bound var must appear in non-recursive clauses
              (seq (set/intersection bound-vars non-rec-vars)))
            ;; Non-recursive branches are always OK
            true)))
      branches)))

(defn magic-rewrite-program
  [rules goal-name goal-pattern stratum-set]
  (let [rules-context {:rules rules}
        seen          (volatile! #{})
        queue         (volatile! [[goal-name goal-pattern]])
        adorned-rules (volatile! {})
        magic-rules   (volatile! {})
        magic-heads   (volatile! {})
        ;; Precompute stable indices for rules in the stratum to avoid
        ;; over-adornment of recursive calls
        stable-cache  (reduce (fn [m rname]
                                (let [branches (rules rname)]
                                  (assoc m rname
                                         (if branches
                                           (stable-head-idxs branches stratum-set)
                                           #{}))))
                              {} stratum-set)]
    (letfn [(ensure-magic-heads
              [magic-name n]
              (if-let [vars (@magic-heads magic-name)]
                vars
                (let [vars (mapv (fn [_] (gensym "?magic")) (range n))]
                  (vswap! magic-heads assoc magic-name vars)
                  vars)))

            (add-magic-branch
              [magic-name branch]
              (vswap! magic-rules
                      (fn [m]
                        (update m magic-name (fnil conj []) branch))))]
      (loop []
        (if-let [[rule-name pattern] (first @queue)]
          (do
            (vswap! queue subvec 1)
            (when-not (contains? @seen [rule-name pattern])
              (vswap! seen conj [rule-name pattern])
              (let [adorned-rule (adorned-name rule-name pattern)
                    magic-rule   (magic-name adorned-rule)
                    branches     (get rules rule-name)
                    head-clause  (ffirst branches)
                    head-vars    (flatten-head-vars head-clause)
                    b-idxs       (vec (bound-indices pattern))
                    bound-vars   (into #{} (map head-vars) b-idxs)
                    magic-call   (when (seq b-idxs)
                                   (list* magic-rule (map head-vars b-idxs)))]
                (when (seq b-idxs)
                  (ensure-magic-heads magic-rule (count b-idxs)))
                (doseq [branch branches]
                  (let [head     (first branch)
                        body     (rest branch)
                        new-head (replace-rule-head head adorned-rule)]
                    (loop [clauses      body
                           prefix       (cond-> [] magic-call (conj magic-call))
                           magic-prefix (cond-> [] magic-call (conj magic-call))
                           bound        bound-vars
                           out          []]
                      (if (empty? clauses)
                        (vswap! adorned-rules
                                (fn [m]
                                  (update m adorned-rule (fnil conj [])
                                          (into [new-head]
                                                (concat (when magic-call
                                                          [magic-call])
                                                        out)))))
                        (let [clause (first clauses)]
                          (if (rule-call? rules-context clause)
                            (let [args        (rule-args clause)
                                  call-head   (rule-head clause)
                                  raw-pattern (binding-pattern args bound)
                                  ;; For recursive calls within stratum, filter
                                  ;; pattern to stable indices only to avoid
                                  ;; over-adornment that causes seed explosion.
                                  ;; Only filter when stable indices exist; if
                                  ;; empty, keep original pattern to allow magic
                                  ;; propagation.
                                  call-pattern
                                  (let [stable (stable-cache call-head)]
                                    (if (seq stable)
                                      (vec
                                        (map-indexed
                                          (fn [idx p]
                                            (if (and (= p :b)
                                                     (not (contains?
                                                            stable idx)))
                                              :f
                                              p))
                                          raw-pattern))
                                      raw-pattern))
                                  call-bound? (some #{:b} call-pattern)
                                  call-name   (if call-bound?
                                                (adorned-name call-head
                                                              call-pattern)
                                                call-head)
                                  replaced    (replace-rule-head clause
                                                                 call-name)]
                              (when call-bound?
                                (let [call-magic-name (magic-name call-name)
                                      call-b-idxs     (vec (bound-indices
                                                             call-pattern))
                                      bound-args      (mapv #(nth args %)
                                                            call-b-idxs)
                                      magic-vars      (ensure-magic-heads
                                                        call-magic-name
                                                        (count call-b-idxs))
                                      bind-clauses    (magic-bind-clauses
                                                        magic-vars bound-args)
                                      magic-body      (concat magic-prefix
                                                              bind-clauses)]
                                  (add-magic-branch
                                    call-magic-name
                                    (into [(list* call-magic-name magic-vars)]
                                          magic-body))
                                  (vswap! queue conj [(rule-head clause)
                                                      call-pattern])))
                              (recur (rest clauses)
                                     (conj prefix replaced)
                                     (conj magic-prefix clause)
                                     (into bound (clause-output-vars
                                                   clause rules-context))
                                     (conj out replaced)))
                            (recur (rest clauses)
                                   (conj prefix clause)
                                   (conj magic-prefix clause)
                                   (into bound (clause-output-vars
                                                 clause rules-context))
                                   (conj out clause))))))))))
            (recur))
          (let [final-magic-rules
                (reduce-kv
                  (fn [m magic-name magic-vars]
                    (if (contains? m magic-name)
                      m
                      (assoc m magic-name
                             [[(list* magic-name magic-vars)
                               [(list '= 0 1)]]])))
                  @magic-rules @magic-heads)]
            {:rules       (merge rules @adorned-rules final-magic-rules)
             :magic-heads @magic-heads
             :goal        (adorned-name goal-name goal-pattern)}))))))
