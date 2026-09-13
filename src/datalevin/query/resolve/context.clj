;;
;; Copyright (c) Huahai Yang, Nikita Prokopov. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns ^:no-doc datalevin.query.resolve.context
  "Relation combination, projection, and binding checks for clause resolution."
  (:refer-clojure :exclude [update assoc])
  (:require
   [clojure.set :as set]
   [datalevin.inline :refer [update assoc]]
   [datalevin.join :as j]
   [datalevin.query-util :as qu]
   [datalevin.relation :as r]
   [datalevin.util :refer [raise]])
  (:import
   [java.util List]))

(defn rel-with-attr [context sym]
  (some #(when ((:attrs %) sym) %) (:rels context)))

(defn collapse-rels
  [rels new-rel]
  (persistent!
    (loop [rels          rels
           new-rel       new-rel
           new-rel-attrs (:attrs new-rel)
           acc           (transient [])]
      (if-some [rel (first rels)]
        (if (not-empty (qu/intersect-keys new-rel-attrs (:attrs rel)))
          (let [joined (j/hash-join rel new-rel)]
            (recur (next rels) joined (:attrs joined) acc))
          (recur (next rels) new-rel new-rel-attrs (conj! acc rel)))
        (conj! acc new-rel)))))

(defn add-resolved-relation
  "Add a normally resolved relation, or stream a terminal rule-body join into
   its distinct head projection when the rule evaluator requested that sink."
  [context new-rel]
  (let [projected-vars (:datalevin.rules/distinct-projection-vars context)
        rels           (:rels context)
        available      (when projected-vars
                         (into (set (keys (:attrs new-rel)))
                               (mapcat #(keys (:attrs %))) rels))]
    (if (and projected-vars (every? available projected-vars))
      (assoc context :rels
             [(if (seq rels)
                (j/hash-join-project-distinct
                  (reduce j/hash-join rels) new-rel projected-vars)
                (r/project-distinct new-rel projected-vars))])
      (update context :rels collapse-rels new-rel))))

(defn context-resolve-val
  [context sym]
  (when-some [rel (rel-with-attr context sym)]
    (when-some [^objects tuple (.get ^List (:tuples rel) 0)]
      (aget tuple ((:attrs rel) sym)))))

(defn- rel-contains-attrs?
  [rel attrs]
  (let [rel-attrs (:attrs rel)]
    (some #(rel-attrs %) attrs)))

(defn rel-prod-by-attrs
  [context attrs]
  (let [rels       (into #{}
                         (filter #(rel-contains-attrs? % attrs))
                         (:rels context))
        production (reduce r/prod-rel rels)]
    [(update context :rels #(remove rels %)) production]))

(defn limit-rel
  [rel vars]
  (if-some [attrs (not-empty (select-keys (:attrs rel) vars))]
    (assoc rel :attrs attrs)
    ;; Projecting away every column of a non-empty relation is existential
    ;; success, so that relation can be dropped. An empty relation is branch
    ;; failure, however, and must remain as an attribute-free annihilator.
    (when (r/rel-empty rel)
      (assoc rel :attrs {}))))

(defn limit-context
  [context vars]
  (assoc context :rels (keep #(limit-rel % vars) (:rels context))))

(defn project-visible-distinct
  "Physically remove tuple cells hidden from a relation's attrs, then dedupe
   by those visible attrs. Logical attr limiting alone is insufficient at a
  set-union boundary because hidden branch-local values distinguish arrays."
  [rel]
  (r/project-distinct rel
                      (->> (:attrs rel)
                           (sort-by val)
                           (mapv key))))

(defn bound-vars
  [context]
  (into #{} (mapcat #(keys (:attrs %))) (:rels context)))

(defn check-bound
  [bound vars form]
  (when-not (set/subset? vars bound)
    (let [missing (set/difference vars bound)]
      (raise "Insufficient bindings: " missing " not bound in " form
             {:error :query/where :form form :vars missing}))))

(defn check-free-same
  [bound branches form]
  (let [free (mapv #(set/difference (qu/collect-vars %) bound) branches)]
    (when-not (apply = free)
      (raise "All clauses in 'or' must use same set of free vars, had " free
             " in " form
             {:error :query/where :form form :vars free}))))

(defn check-free-subset
  [bound vars branches]
  (let [free (into #{} (remove bound) vars)]
    (doseq [branch branches]
      (when-some [missing (not-empty
                            (set/difference free (qu/collect-vars branch)))]
        (raise "All clauses in 'or' must use same set of free vars, had "
               missing " not bound in " branch
               {:error :query/where :form branch :vars missing})))))

(defn single
  [coll]
  (assert (nil? (next coll)) "Expected single element")
  (first coll))
