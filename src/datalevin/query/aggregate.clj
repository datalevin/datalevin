;;
;; Copyright (c) Huahai Yang, Nikita Prokopov. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns ^:no-doc datalevin.query.aggregate
  "Aggregate results"
  (:require
   [datalevin.built-ins :as built-ins]
   [datalevin.parser :as dp]
   [datalevin.query.resolve :as qresolve]
   [datalevin.util :as u])
  (:import
   [datalevin.parser Constant PlainSymbol SrcVar Variable]
   [org.eclipse.collections.impl.list.mutable FastList]))

(defprotocol IContextResolve
  (-context-resolve [var context]))

(extend-protocol IContextResolve
  Variable
  (-context-resolve [var context]
    (qresolve/context-resolve-val context (.-symbol var)))
  SrcVar
  (-context-resolve [var context]
    (get-in context [:sources (.-symbol var)]))
  PlainSymbol
  (-context-resolve [var _]
    (or (get built-ins/aggregates (.-symbol var))
        (qresolve/resolve-sym (.-symbol var))))
  Constant
  (-context-resolve [var _]
    (.-value var)))

(defn- compute-aggregate
  "Compute an aggregate over tuples at the given tuple index."
  [element context tuples tuple-idx]
  (let [f    (-context-resolve (:fn element) context)
        args (mapv #(-context-resolve % context)
                   (butlast (:args element)))
        vals (map #(nth % tuple-idx) tuples)]
    (apply f (conj args vals))))

(defn- eval-find-expr
  "Evaluate a FindExpr by computing its inner aggregates and applying the
  operator."
  [expr context tuples var->idx]
  (let [op   (get built-ins/query-fns (:symbol (:fn expr)))
        args (mapv (fn [arg]
                     (cond
                       (dp/aggregate? arg)
                       (let [var-sym (-> arg :args last :symbol)
                             idx     (get var->idx var-sym)]
                         (compute-aggregate arg context tuples idx))

                       (dp/find-expr? arg)
                       (eval-find-expr arg context tuples var->idx)

                       :else
                       (-context-resolve arg context)))
                   (:args expr))]
    (apply op args)))

(defn- build-var->idx
  "Build a mapping from variable symbols to tuple indices."
  [find-elements]
  (loop [elements find-elements
         idx      0
         result   {}]
    (if (empty? elements)
      result
      (let [elem (first elements)
            vars (dp/-find-vars elem)]
        (recur (rest elements)
               (+ idx (count vars))
               (into result
                     (map vector vars (range idx (+ idx (count vars))))))))))

(defn -aggregate
  ([find-elements context tuples]
   (-aggregate find-elements context tuples (build-var->idx find-elements)))
  ([find-elements context tuples var->idx]
   (let [first-tuple (first tuples)]
     (loop [elements  find-elements
            tuple-idx 0
            result    []]
       (if (empty? elements)
         result
         (let [elem     (first elements)
               num-vars (count (dp/-find-vars elem))]
           (cond
             (dp/find-expr? elem)
             (recur (rest elements)
                    (+ tuple-idx num-vars)
                    (conj result (eval-find-expr elem context tuples var->idx)))

             (dp/aggregate? elem)
             (recur (rest elements)
                    (inc tuple-idx)
                    (conj result
                          (compute-aggregate elem context tuples tuple-idx)))

             :else
             (recur (rest elements)
                    (inc tuple-idx)
                    (conj result (nth first-tuple tuple-idx))))))))))

(defn- groupable-elem?
  "Check if an element should be used for grouping
  (not an aggregate or find-expr)."
  [elem]
  (not (or (dp/aggregate? elem) (dp/find-expr? elem))))

(defn- group-key
  [tuple ^ints group-idxs]
  (let [n (alength group-idxs)]
    (cond
      (zero? n)
      nil

      (= 1 n)
      (nth tuple (aget group-idxs 0))

      :else
      (persistent!
       (loop [i   (int 0)
              key (transient [])]
         (if (< i n)
           (recur (unchecked-inc-int i)
                  (conj! key (nth tuple (aget group-idxs i))))
           key))))))

(defn- group-tuples
  [resultset ^ints group-idxs]
  (if (zero? (alength group-idxs))
    (when (seq resultset)
      (list resultset))
    (let [groups (transient {})]
      (doseq [tuple resultset]
        (let [key (group-key tuple group-idxs)]
          (if-let [bucket (get groups key)]
            (.add ^FastList bucket tuple)
            (assoc! groups key (doto (FastList.) (.add tuple))))))
      (vals (persistent! groups)))))

(defn aggregate
  [find-elements context resultset]
  (let [^ints group-idxs (int-array (u/idxs-of groupable-elem? find-elements))
        var->idx         (build-var->idx find-elements)]
    (map #(-aggregate find-elements context % var->idx)
         (group-tuples resultset group-idxs))))

(defn- find-aggregate-idx
  "Find the index of an aggregate in find-elements by matching structure."
  [aggregate find-elements]
  (let [agg-var (-> aggregate :args last :symbol)]
    (loop [elems find-elements
           idx   0]
      (when (seq elems)
        (let [elem (first elems)]
          (cond
            (and (dp/aggregate? elem)
                 (= (-> elem :fn :symbol) (-> aggregate :fn :symbol))
                 (= (-> elem :args last :symbol) agg-var))
            idx

            :else
            (recur (rest elems) (inc idx))))))))

(defn- eval-having-arg
  "Evaluate a having predicate argument against an aggregated result tuple."
  [arg find-elements result-tuple]
  (cond
    (dp/aggregate? arg)
    (let [idx (find-aggregate-idx arg find-elements)]
      (when idx (nth result-tuple idx)))

    (dp/find-expr? arg)
    (let [idx (u/index-of #(and (dp/find-expr? %)
                                (= (:fn %) (:fn arg)))
                          find-elements)]
      (when idx (nth result-tuple idx)))

    (instance? Constant arg)
    (:value arg)

    :else
    arg))

(defn- eval-having-pred
  "Evaluate a single having predicate on an aggregated result tuple."
  [pred find-elements result-tuple]
  (let [pred-fn (get built-ins/query-fns (-> pred :fn :symbol))
        args    (mapv #(eval-having-arg % find-elements result-tuple)
                      (:args pred))]
    (when (and pred-fn (every? some? args))
      (apply pred-fn args))))

(defn apply-having
  "Filter aggregated results by having predicates."
  [having find-elements results]
  (if (seq having)
    (filter (fn [result-tuple]
              (every? #(eval-having-pred % find-elements result-tuple)
                      having))
            results)
    results))
