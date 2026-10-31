(ns ^:no-doc datalevin.query.optimizer.existence
  "Keyed existence rewrites for indexed pattern groups with local witnesses."
  (:require [clojure.set :as set]
            [datalevin.db :as db]
            [datalevin.parser :as dp]
            [datalevin.query-util :as qu])
  (:import [datalevin.parser Constant Not Pattern Predicate]))

(defn simple-predicate?
  "Pure scalar comparisons whose operands require no nested evaluation."
  [clause]
  (and (vector? clause) (= 1 (count clause))
       (seq? (first clause))
       (contains? '#{= not= < <= > >=} (ffirst clause))
       (every? #(or (qu/binding-var? %) (not (symbol? %)))
               (rest (first clause)))
       (not-any? coll? (rest (first clause)))))

(defn- replace-where [context forms]
  (-> context
      (assoc-in [:parsed-q :qorig-where] forms)
      (assoc-in [:parsed-q :qwhere] (dp/parse-where forms))))

(defn- pattern-source [pattern]
  (or (get-in pattern [:source :symbol]) '$))

(defn- indexed-pattern? [sources clause]
  (when (instance? Pattern clause)
    (let [attr (second (:pattern clause))
          source (get sources (pattern-source clause))]
      (and source (db/-searchable? source)
           (= 3 (count (:pattern clause)))
           (instance? Constant attr)
           (keyword? (:value attr))
           (not (get-in (db/-schema source) [(:value attr) :db/noindex]))))))

(defn- keyed-not [keys body]
  (list* 'not-join (vec (sort keys)) body))

(defn- witness-group
  [context entity patterns]
  (let [{:keys [parsed-q sources rels]} context
        forms (:qorig-where parsed-q)
        parsed (:qwhere parsed-q)
        pattern-idxs (set (map first patterns))
        protected (set/union (set (dp/find-vars (:qfind parsed-q)))
                             (set (map :symbol (:qwith parsed-q)))
                             (into #{} (mapcat (comp keys :attrs)) rels))
        ;; Only pattern and comparison sites may consume a local witness.
        outside (into protected
                      (mapcat (fn [[i clause]]
                                (when-not (or (pattern-idxs i)
                                              (instance? Predicate clause))
                                  (qu/collect-vars (nth forms i)))))
                      (map-indexed vector parsed))
        pattern-vars (qu/collect-vars (map second patterns))
        other-pattern-vars
        (into #{} (mapcat (fn [[i clause]]
                           (when (and (instance? Pattern clause)
                                      (not (pattern-idxs i)))
                             (qu/collect-vars clause))))
              (map-indexed vector parsed))
        locals (set/difference pattern-vars outside other-pattern-vars)
        pred-idxs (into #{}
                        (keep-indexed
                          (fn [i clause]
                            (when (and (instance? Predicate clause)
                                       (seq (set/intersection
                                              locals (qu/collect-vars clause))))
                              i)))
                        parsed)
        idxs (set/union pattern-idxs pred-idxs)
        body (mapv forms (concat (sort pattern-idxs) (sort pred-idxs)))
        keys (set/difference (qu/collect-vars body) locals)
        available (into other-pattern-vars (mapcat (comp clojure.core/keys :attrs)) rels)]
    (when (and (contains? locals entity)
               (< 1 (count patterns))
               (seq pred-idxs)
               (every? #(indexed-pattern? sources (second %)) patterns)
               (apply = (map #(pattern-source (second %)) patterns))
               (= '$ (pattern-source (second (first patterns))))
               (every? #(simple-predicate? (forms %)) pred-idxs)
               ;; Leave local range scans to the cost-based join planner.
               ;; This rewrite targets comparisons with a correlation key.
               (some #(seq (set/intersection keys (qu/collect-vars (forms %))))
                     pred-idxs)
               (seq keys) (set/subset? keys available))
      {:idxs idxs :form (keyed-not keys [(keyed-not keys body)])})))

(defn rewrite
  "Remove existential witness fanout and project ordinary negation onto its
  actual outer keys. Projected variables and :with witnesses remain visible."
  [{:keys [parsed-q rels] :as context}]
  (let [forms (:qorig-where parsed-q)
        parsed (:qwhere parsed-q)
        ;; Keep scope and function-output dependencies on their ordinary path.
        simple? (every? #(or (instance? Pattern %) (instance? Predicate %)
                             (instance? Not %)) parsed)]
    (if-not simple?
      context
      (let [outer-vars (into (into #{} (mapcat (comp keys :attrs)) rels)
                             (mapcat qu/collect-vars)
                             (filter #(instance? Pattern %) parsed))
            keyed (mapv (fn [clause]
                          (if (and (seq? clause) (= 'not (first clause)))
                            (let [keys (set/intersection outer-vars
                                                         (qu/collect-vars (rest clause)))]
                              (if (seq keys) (keyed-not keys (rest clause)) clause))
                            clause))
                        forms)
            context (if (= keyed forms) context (replace-where context keyed))
            groups (->> (get-in context [:parsed-q :qwhere])
                        (map-indexed vector)
                        (filter #(instance? Pattern (second %)))
                        (group-by #(get-in (second %) [:pattern 0 :symbol])))
            candidate (some (fn [[entity patterns]]
                              (when (qu/binding-var? entity)
                                (witness-group context entity patterns)))
                            (sort-by (comp str key) groups))]
        (if-let [{:keys [idxs form]} candidate]
          (replace-where context
                         (conj (into [] (keep-indexed
                                          #(when-not (idxs %1) %2)) keyed)
                               form))
          context)))))
