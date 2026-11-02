;;
;; Copyright (c) Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns ^:no-doc datalevin.query.execute.rewrite
  "Elimination and materialization of redundant data patterns."
  (:refer-clojure :exclude [update])
  (:require
   [datalevin.db :as db]
   [datalevin.inline :refer [update]]
   [datalevin.parser :as dp]
   [datalevin.query-util :as qu]
   [datalevin.query.plan :as qplan]
   [datalevin.query.resolve :as qresolve]
   [datalevin.relation :as r]
   [datalevin.util :as u])
  (:import
   [java.util List]
   [datalevin.parser BindColl BindScalar BindTuple Constant FindScalar Function
    Pattern Predicate Variable]
   [org.eclipse.collections.impl.list.mutable FastList]))

(defn- native-subquery-output?
  [context outputs]
  (some
    (fn [clause]
      (when (instance? Pattern clause)
        (let [source (get (:sources context)
                          (or (get-in clause [:source :symbol]) '$))]
          (and source (db/-searchable? source)
               (some #(outputs (:symbol %)) (:pattern clause))))))
    (get-in context [:parsed-q :qwhere])))

(defn- indexed-group-output?
  [context elements bindings]
  (let [outputs (into #{}
                      (keep (fn [[element binding]]
                              (when (and (instance? Variable element)
                                         (qu/binding-var? binding))
                                binding)))
                      (map vector elements bindings))]
    (some
      (fn [clause]
        (when (instance? Pattern clause)
          (let [[entity attr value] (:pattern clause)
                source (get (:sources context)
                            (or (get-in clause [:source :symbol]) '$))]
            (and source (db/-searchable? source)
                 (or (outputs (:symbol entity))
                     (and (keyword? (:value attr))
                          (not (get-in (db/-schema source)
                                       [(:value attr) :db/noindex]))
                          (outputs (:symbol value))))))))
      (get-in context [:parsed-q :qwhere]))))

(defn- independent-grouped-subquery?
  [context parsed clause]
  (when (and (instance? Function parsed)
             (= 'q (get-in parsed [:fn :symbol]))
             (instance? BindColl (:binding parsed))
             (instance? BindTuple (get-in parsed [:binding :binding])))
    (let [[[_ query & inputs] _] clause
          outputs (qu/collect-vars (second clause))]
      (when (and (or (vector? query) (map? query))
                 ;; Query literals contain their own logic variables. Only
                 ;; the call's inputs can depend on outer tuple bindings.
                 (every? #(if (qu/source? %)
                            (contains? (:sources context) %)
                            (and (not (symbol? %)) (not (list? %))))
                         inputs)
                 (native-subquery-output? context outputs))
        (let [query    (dp/parse-query query)
              elements (dp/find-elements (:qfind query))]
          (and (seq (dp/collect dp/aggregate? elements))
               ;; HAVING already produces an early driver. Without it, a
               ;; grouping key must supply an entity or indexed value lookup;
               ;; an aggregate used only by a late predicate is not enough.
               (or (seq (:qhaving query))
                   (indexed-group-output? context elements
                                          (first (second clause))))))))))

(defn- scalar-comparison?
  [parsed output]
  (and (instance? Predicate parsed)
       (contains? '#{< <= > >=} (get-in parsed [:fn :symbol]))
       (= 2 (count (:args parsed)))
       (every? #(or (instance? Variable %) (instance? Constant %)) (:args parsed))
       (some #(= output (:symbol %)) (:args parsed))))

(defn- independent-scalar-subquery?
  [context parsed clause]
  (when (and (instance? Function parsed)
             (= 'q (get-in parsed [:fn :symbol]))
             (instance? BindScalar (:binding parsed)))
    (let [[[_ query & inputs] output] clause
          clauses (get-in context [:parsed-q :qwhere])]
      (when (and (qu/binding-var? output)
                 (or (vector? query) (map? query))
                 (not (contains? (qresolve/bound-vars context) output))
                 (every? #(if (qu/source? %)
                            (contains? (:sources context) %)
                            (and (not (symbol? %)) (not (list? %))))
                         inputs)
                 ;; A comparison supplies a native access bound. Keep other
                 ;; producers and scoped uses on the ordinary execution path.
                 (every? #(or (= parsed %) (instance? Predicate %)
                              (not (contains? (qu/collect-vars %) output)))
                         clauses)
                 (some (fn [pred]
                         (and (scalar-comparison? pred output)
                              (native-subquery-output?
                                context (disj (qu/collect-vars (:args pred)) output))))
                       clauses))
        (let [query (dp/parse-query query)]
          (and (instance? FindScalar (:qfind query))
               (dp/aggregate? (:element (:qfind query)))))))))

(defn- compatible-scalar-bound?
  [context pred output value]
  (let [vars (disj (qu/collect-vars (:args pred)) output)]
    (every?
      (fn [pattern]
        (let [[_ attr term] (:pattern pattern)
              source (get (:sources context)
                          (or (get-in pattern [:source :symbol]) '$))]
          (or (not (vars (:symbol term)))
              (not (and source (db/-searchable? source)))
              ;; Do not narrow a fractional mean to an integer/float index
              ;; boundary. Keeping the binding preserves exact comparison.
              (case (get-in (db/-schema source) [(:value attr) :db/valueType])
                (:db.type/long :db.type/ref) (int? value)
                :db.type/double (instance? Double value)
                :db.type/float (instance? Float value)
                (nil :db.type/data :db.type/bigdec) true
                false))))
      (filter #(instance? Pattern %) (get-in context [:parsed-q :qwhere])))))

(defn- specialize-scalar-comparisons
  [context parsed rels]
  (let [output (get-in parsed [:binding :variable :symbol])
        rel (first rels)
        tuples ^List (:tuples rel)]
    (if (and output (= 1 (.size tuples)))
      (let [value (aget ^objects (.get tuples 0) (int ((:attrs rel) output)))]
        (if (number? value)
          (let [forms (mapv
                        (fn [pred form]
                          (if (and (scalar-comparison? pred output)
                                   (compatible-scalar-bound? context pred output value))
                            [(apply list (ffirst form)
                                    (map #(if (= output %) value %)
                                         (rest (first form))))]
                            form))
                        (get-in context [:parsed-q :qwhere])
                        (get-in context [:parsed-q :qorig-where]))
                needed? (or (contains? (qu/collect-vars forms) output)
                            (some #{output} (dp/find-vars (get-in context [:parsed-q :qfind])))
                            (some #(= output (:symbol %))
                                  (get-in context [:parsed-q :qwith])))]
            ;; Preserve projected bindings; a bound used only for planning
            ;; need not be cross-joined back onto every candidate tuple.
            (cond-> (-> context
                        (assoc-in [:parsed-q :qorig-where] forms)
                        (assoc-in [:parsed-q :qwhere] (dp/parse-where forms)))
              (not needed?) (update :rels #(filterv (fn [r] (not (identical? r rel))) %))))
          context))
      context)))

(defn materialize-independent-subqueries
  "Resolve independent aggregates before native planning. Grouped relations
  seed indexed lookups; scalar results specialize comparisons into ranges.
  Correlated calls and plan-only explanations retain late-clause execution."
  [context]
  (cond
    (not (:run? context)) context
    (some r/rel-empty (:rels context)) (assoc context :result-set #{})
    :else
    (loop [context context]
      (if-let [idx (when (not= #{} (:result-set context))
                     (first
                       (keep-indexed
                         (fn [idx [parsed clause]]
                           (when (or (independent-grouped-subquery?
                                       context parsed clause)
                                     (independent-scalar-subquery?
                                       context parsed clause))
                             idx))
                         (map vector
                              (get-in context [:parsed-q :qwhere])
                              (get-in context [:parsed-q :qorig-where])))))]
        (let [clause (nth (get-in context [:parsed-q :qorig-where]) idx)
              parsed (nth (get-in context [:parsed-q :qwhere]) idx)
              resolved (binding [qu/*implicit-source* (get (:sources context) '$)]
                         (qresolve/resolve-clause (assoc context :rels []) clause))
              rels (:rels resolved)
              bound-rels (reduce qresolve/collapse-rels (:rels context) rels)
              rows (.size ^List (:tuples (first rels)))
              stage {:clause clause :rows rows
                     :vars (vec (keys (:attrs (first rels))))}]
          (when qplan/*explain*
            (vswap! qplan/*explain* update :pre-materialized-subqueries
                    (fnil conj []) stage))
          (recur
            (cond-> (-> context
                        (assoc :rels bound-rels)
                        (update-in [:parsed-q :qwhere]
                                   #(u/remove-idxs #{idx} %))
                        (update-in [:parsed-q :qorig-where]
                                   #(u/remove-idxs #{idx} %))
                        (specialize-scalar-comparisons parsed rels))
              (some r/rel-empty bound-rels) (assoc :result-set #{}))))
        context))))

(defn- redundant-pattern-group-strategy
  [sources patterns]
  (let [{:keys [source pattern]} (first patterns)
        attr-term               (nth pattern 1 nil)
        attr                    (when (instance? Constant attr-term)
                                  (:value attr-term))
        source                  (get sources (or (:symbol source) '$))]
    (when source
      (if (and (keyword? attr) (db/-searchable? source))
        (if (identical? :db.cardinality/many
                        (get-in (db/-schema source) [attr :db/cardinality]))
          :materialize
          :elide)
        :materialize))))

(defn- pattern-value
  [pattern]
  (nth (:pattern pattern) 2 nil))

(defn- constant-value-pattern
  [patterns]
  (some #(when (instance? Constant (pattern-value %)) %) patterns))

(defn- remove-pattern-clause
  [context pattern]
  (let [qwhere (get-in context [:parsed-q :qwhere])]
    (if-some [idx (u/index-of #(= pattern %) qwhere)]
      (-> context
          (update-in [:parsed-q :qwhere] #(u/remove-idxs #{idx} %))
          (update-in [:parsed-q :qorig-where] #(u/remove-idxs #{idx} %)))
      context)))

(defn- elide-cardinality-one-patterns
  [context patterns constant-pattern]
  (let [v (:value ^Constant (pattern-value constant-pattern))]
    (reduce
      (fn [context pattern]
        (let [value (pattern-value pattern)]
          (cond
            (instance? Variable value)
            (-> context
                (remove-pattern-clause pattern)
                (update :rels conj
                        (r/relation! {(:symbol ^Variable value) 0}
                                     (doto (FastList.)
                                       (.add (object-array [v]))))))

            (not (instance? Constant value))
            (remove-pattern-clause context pattern)

            :else
            context)))
      context patterns)))

(defn- materialize-repeated-patterns
  [context patterns]
  (let [patterns (sort-by #(if (instance? Constant (pattern-value %)) 0 1)
                          patterns)
        context  (binding [qu/*implicit-source* (get (:sources context) '$)]
                   (reduce (fn [context pattern]
                             (qresolve/resolve-clause context
                                                      (dp/source pattern)))
                           context patterns))]
    (reduce remove-pattern-clause context patterns)))

(defn resolve-redudants
  "Resolve repeated source/entity/attribute patterns containing a constant.
  Cardinality-one variable values are determined by the constant and can be
  elided. Cardinality-many and schema-unknown groups are materialized with the
  constant pattern first so every matching variable value is retained."
  [{:keys [parsed-q sources] :as context}]
  (let [{:keys [qwhere]} parsed-q
        redundant-groups
        (into []
              (->> qwhere
                   (eduction (filter #(instance? Pattern %)))
                   (group-by (fn [{:keys [source pattern]}]
                               [source (first pattern) (second pattern)]))
                   (eduction (filter
                               #(let [ps (val %)]
                                  (and (< 1 (count ps))
                                       (constant-value-pattern ps)
                                       (redundant-pattern-group-strategy
                                         sources ps)))))))]
    (reduce
      (fn [c [_ patterns]]
        (let [constant-pattern (constant-value-pattern patterns)]
          (case (redundant-pattern-group-strategy sources patterns)
            :elide
            (elide-cardinality-one-patterns c patterns constant-pattern)

            :materialize
            (materialize-repeated-patterns c patterns)

            c)))
      context
      redundant-groups)))
