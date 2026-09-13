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
   [datalevin.query.resolve :as qresolve]
   [datalevin.relation :as r]
   [datalevin.util :as u])
  (:import
   [datalevin.parser Constant Pattern Variable]
   [org.eclipse.collections.impl.list.mutable FastList]))

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
