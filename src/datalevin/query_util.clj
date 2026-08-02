;;
;; Copyright (c) Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns ^:no-doc datalevin.query-util
  "Some utility functions for Datalog query processing"
  (:require
   [clojure.string :as str]
   [datalevin.util :as u :refer [cond+ raise]]))

(def ^{:dynamic true
       :doc     "List of symbols in current pattern that might potentiall be resolved to refs"}
  *lookup-attrs* nil)

(def ^{:dynamic true
       :doc     "Default pattern source. Lookup refs, patterns, rules will be resolved with it"}
  *implicit-source* nil)

(defn intersect-keys
  [attrs1 attrs2]
  (u/intersection (set (keys attrs1)) (set (keys attrs2))))

(defn source? [sym] (and (symbol? sym) (= \$ (first (name sym)))))

(defn free-var? [sym] (and (symbol? sym) (= \? (first (name sym)))))

(defn placeholder?
  [sym]
  (and (symbol? sym) (str/starts-with? (name sym) "?placeholder")))

(defn placeholder-sym
  [sym]
  (let [raw  (str sym)
        base (-> raw
                 (str/replace "/" "_")
                 (#(if (str/starts-with? % "?") (subs % 1) %)))]
    (symbol (str "?placeholder__" base))))

(defn binding-var? [sym] (and (free-var? sym) (not (placeholder? sym))))

(defn get-v
  [pattern]
  (when (< 2 (count pattern))
    (let [v (nth pattern 2)]
      (if (= v '_)
        (gensym "?placeholder")
        v))))

(defn replace-blanks
  "Replace all _ (blank) placeholders in a pattern with unique gensym'd variables.
   This ensures that _ in different patterns (or different positions) are not
   incorrectly unified."
  [pattern]
  (mapv (fn [el] (if (= el '_) (gensym "?blank") el)) pattern))

(defn lookup-ref? [x]
  (and (vector? x)
       (= 2 (count x))
       (keyword? (first x))))

(def rule-head #{'_ 'or 'or-join 'and 'not 'not-join})

(defn clause-head
  [clause]
  (if (source? (first clause))
    (second clause)
    (first clause)))

(defn clause-args
  [clause]
  (if (source? (first clause))
    (nnext clause)
    (rest clause)))

(defn rule?
  [context clause]
  (cond+
    (not (sequential? clause))
    false

    :let [head (clause-head clause)]

    (not (symbol? head))
    false

    (free-var? head)
    false

    (contains? rule-head head)
    false

    (not (contains? (:rules context) head))
    (raise "Unknown rule '" head " in " clause
           {:error :query/where :form clause})

    :else true))

(defn quoted-form? [form]
  (and (seq? form)
       (symbol? (first form))
       (#{'quote 'clojure.core/quote} (first form))
       (= 2 (count form))))

(defn collect-vars [clause]
  (let [vars (volatile! #{})]
    (letfn [(walk [form]
              (cond
                (quoted-form? form) nil
                (binding-var? form) (vswap! vars conj form)
                (map? form)         (doseq [[k v] form] (walk k) (walk v))
                (coll? form)        (doseq [x form] (walk x))
                :else               nil))]
      (walk clause)
      @vars)))

(defn collect-fn-arg-vars
  "Collect variables that a query function call reads from its arguments.
   Collection literals are constants in the query grammar, even when they
   contain symbols such as ?x. Lists are the exception because the executor
   treats them as nested function calls."
  [args]
  (let [vars (volatile! #{})]
    (letfn [(walk [arg]
              (cond
                (binding-var? arg)
                (vswap! vars conj arg)

                (list? arg)
                (do
                  (when (binding-var? (first arg))
                    (vswap! vars conj (first arg)))
                  (doseq [nested-arg (next arg)]
                    (walk nested-arg)))

                :else nil))]
      (doseq [arg args]
        (walk arg))
      @vars)))
