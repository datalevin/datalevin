(ns ^:no-doc datalevin.query.optimizer.or-branches
  "Admit independent native conjunctions to ordinary planning inside a union."
  (:require [clojure.set :as set]
            [datalevin.db :as db]
            [datalevin.parser :as dp]
            [datalevin.query-util :as qu]
            [datalevin.query.plan :as plan])
  (:import [datalevin.db DB]
           [datalevin.parser Function Predicate]
           [java.util Date UUID]))

(def ^:dynamic *enabled?* true)

(def ^:private native-types
  {:db.type/ref :number :db.type/long :number :db.type/float :number
   :db.type/double :number :db.type/bigint :number :db.type/bigdec :number
   :db.type/string :string :db.type/instant :instant
   :db.type/boolean :boolean :db.type/keyword :keyword :db.type/symbol :symbol
   :db.type/uuid :uuid})

(defn- scalar-type [x]
  (cond (number? x) :number (string? x) :string (boolean? x) :boolean
        (keyword? x) :keyword (instance? Date x) :instant
        (instance? UUID x) :uuid))

(defn- pattern? [schema clause]
  (and (vector? clause) (= 3 (count clause))
       (qu/binding-var? (first clause)) (keyword? (second clause))
       (let [props (schema (second clause)) value (nth clause 2)]
         (and (native-types (:db/valueType props)) (not (:db/noindex props))
              (or (qu/binding-var? value) (= '_ value) (scalar-type value))))))

(defn- variable-types [schema patterns]
  (reduce (fn [types [entity attr value]]
            (cond-> (update types entity (fnil conj #{}) :number)
              (qu/binding-var? value)
              (update value (fnil conj #{}) (native-types (:db/valueType (schema attr))))))
          {} patterns))

(defn- scalar-comparison? [types expr]
  (when (seq? expr)
    (let [[op & args] expr]
      (cond
        (#{'and 'or} op)
        (and (seq args) (every? #(scalar-comparison? types %) args))

        (#{'= 'not= '< '<= '> '>=} op)
        (let [arg-types (map #(if (qu/binding-var? %) (types %) #{(scalar-type %)}) args)]
          (and (= 2 (count args))
               (every? #(and (= 1 (count %)) (not (contains? % nil))) arg-types)
               (or (#{'= 'not=} op)
                   (and (apply = arg-types)
                        (contains? #{:number :string :instant} (ffirst arg-types))))))

        :else false))))

(defn- native-branch? [schema exports branch]
  (when (and (seq? branch) (= 'and (first branch)))
    (let [clauses (rest branch)
          patterns (filter #(pattern? schema %) clauses)
          types (variable-types schema patterns)]
      (and (<= 3 (count patterns))
           (< 1 (count (set (map first patterns))))
           ;; Bound index anchors make this a useful join-planning problem;
           ;; single lookups stay with the resolver.
           (some #(scalar-type (nth % 2)) patterns)
           (set/subset? (set exports) (set (keys types)))
           (every? #(or (pattern? schema %)
                        (and (vector? %) (= 1 (count %))
                             (scalar-comparison? types (first %)))) clauses)))))

(defn- produced-vars [clause]
  (cond
    (instance? Predicate clause) #{}
    (instance? Function clause) (qu/collect-vars (:binding clause))
    :else (qu/collect-vars clause)))

(defn rewrite
  "Use existing nested-query execution and exact union for independent native
  branches. Correlated scopes, functions, rules, alternate sources, and complex
  predicates keep their resolver path. No branch results are cached here."
  [{:keys [parsed-q sources rels] :as context}]
  (let [database (sources '$)]
    (if-not (and *enabled?* (instance? DB database)
                 (not (db/pending-tx-cache? database)))
      context
      (let [schema (db/-schema database)
            forms (:qorig-where parsed-q)
            parsed (:qwhere parsed-q)
            input-vars (into #{} (mapcat (comp keys :attrs)) rels)
            choices
            (into {}
              (keep-indexed
                (fn [idx clause]
                  (when (and (seq? clause) (= 'or-join (first clause))
                             (vector? (second clause)) (seq (second clause))
                             (every? qu/binding-var? (second clause)))
                    (let [[_ exports & branches] clause
                          outside (into input-vars
                                        (mapcat (fn [[i parsed]]
                                                  (when (not= i idx) (produced-vars parsed))))
                                        (map-indexed vector parsed))]
                      (when (and (empty? (set/intersection (set exports) outside))
                                 (every? #(native-branch? schema exports %) branches))
                        [idx (list* 'or-join exports
                                    (map (fn [branch]
                                           (let [query {:find exports :where (vec (rest branch))}]
                                             (list 'and [(list 'q query '$) [exports]])))
                                         branches))]))))) forms)]
        (if (empty? choices)
          context
          (let [forms (reduce-kv assoc forms choices)]
            (when plan/*explain*
              (vswap! plan/*explain* assoc :planned-or-join-branches
                      (mapv (fn [[idx clause]]
                              {:clause-index idx :variables (second clause)
                               :branch-count (count (nnext clause))
                               :strategy :independent-subqueries})
                            (sort-by key choices))))
            (assoc context :parsed-q
                   (assoc parsed-q :qorig-where forms :qwhere (dp/parse-where forms)))))))))
