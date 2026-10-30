;; Copyright (c) Huahai Yang. All rights reserved.
;; Distributed under the Eclipse Public License 2.0.
(ns ^:no-doc datalevin.query.execute.fused-aggregate
  "Streaming scan/expression/group reduction for proven-distinct native rows."
  (:require [clojure.set :as set]
            [datalevin.client.request :as request]
            [datalevin.db :as db]
            [datalevin.interface :as i]
            [datalevin.lmdb :as lmdb]
            [datalevin.parser :as parser]
            [datalevin.query.resolve :as resolve]
            [datalevin.relation :as relation]
            [datalevin.storage.scan :as scan]
            [datalevin.timeout :as timeout])
  (:import [datalevin.db DB]
           [datalevin.storage Store]
           [datalevin.parser Aggregate BindScalar Constant DefaultSrc FindRel
            Function Pattern PlainSymbol Predicate SrcVar Variable]
           [datalevin.utl GroupedAggregate]
           [org.eclipse.collections.impl.list.mutable FastList]))

(def ^:dynamic *enabled?*
  "Enable the native scan/expression/aggregate alternative."
  true)
(def ^:dynamic *min-input*
  "Minimum estimated qualifying entities before a full EAV walk is considered."
  4096)
(def ^:dynamic *min-scan-fraction*
  "Minimum estimated coverage of both the anchor population and entity-id span.
  Sparse identifiers conservatively keep the existing indexed executor."
  0.5)

(def ^:dynamic *max-groups*
  "Conservative product of grouping attribute domains admitted by this scan."
  256)

(def ^:private pure-functions
  '#{+ - * / inc dec = == not= < <= > >=})

(def ^:private value-types
  #{:db.type/long :db.type/double :db.type/float :db.type/string
    :db.type/keyword :db.type/symbol :db.type/boolean :db.type/instant
    :db.type/uuid :db.type/ref})

(defn- scalar? [x]
  (or (nil? x) (string? x) (keyword? x) (boolean? x) (char? x)
      (uuid? x) (integer? x) (float? x) (decimal? x) (ratio? x)))

(defn- pure-expression? [bound expression]
  (cond
    (symbol? expression) (contains? bound expression)
    (list? expression) (and (contains? pure-functions (first expression))
                            (every? #(pure-expression? bound %) (rest expression)))
    :else (scalar? expression)))

(defn- expression-arguments [clause]
  ;; Specialization changes parsed arguments while source metadata can retain
  ;; the original input symbols. Nested arithmetic remains inside Constants.
  (mapv #(if (instance? Constant %) (:value %) (:symbol %)) (:args clause)))

(defn- row-program [clauses columns context]
  (let [base-vars (set (keys columns))]
    (loop [clauses (seq clauses) columns columns operations []]
      (if-let [clause (first clauses)]
        (let [f (get-in clause [:fn :symbol])
              args (expression-arguments clause)
              output (when (instance? Function clause)
                       (when (and (instance? BindScalar (:binding clause))
                                  (instance? Variable (get-in clause [:binding :variable])))
                         (get-in clause [:binding :variable :symbol])))]
          (when (and (or (instance? Predicate clause) output)
                     (contains? pure-functions f)
                     (every? #(pure-expression? (set (keys columns)) %) args)
                     ;; Native planning pushes base predicates ahead of scalar
                     ;; arithmetic. Keep that error/filter ordering, falling back
                     ;; when the source clause order would require reordering.
                     (or output
                         (and (not-any? list? args)
                              (or (not-any? first operations)
                                  (some #(and (symbol? %) (not (base-vars %))) args))))
                     (or (nil? output) (not (contains? columns output))))
            (let [call (resolve/-call-fn context
                         (relation/relation! columns (FastList.)) f args)
                  index (when output (count columns))]
              (recur (next clauses)
                     (if output (assoc columns output index) columns)
                     (conj operations [index call])))))
        {:columns columns :operations operations}))))

(defn- aggregate-plan [parsed columns entity]
  (let [elements (parser/find-elements (:qfind parsed))
        group? (some #(instance? Variable %) elements)
        retained (set (concat (parser/find-vars (:qfind parsed))
                             (map :symbol (:qwith parsed))))
        plans (mapv
                (fn [element]
                  (cond
                    (instance? Variable element)
                    (when-some [index (get columns (:symbol element))]
                      [index GroupedAggregate/KEY])
                    (and (instance? Aggregate element)
                         (instance? PlainSymbol (:fn element))
                         (= 1 (count (:args element)))
                         (instance? Variable (first (:args element))))
                    (when-let [op ({'sum GroupedAggregate/SUM
                                   'avg GroupedAggregate/AVG
                                   'count GroupedAggregate/COUNT}
                                  (get-in element [:fn :symbol]))]
                      (when-some [index (get columns (get-in element [:args 0 :symbol]))]
                        [index op])))) elements)]
    (when (and (instance? FindRel (:qfind parsed)) group?
               (some #(instance? Aggregate %) elements)
               (contains? retained entity)
               (set/subset? retained (set (keys columns)))
               (every? some? plans))
      {:aggregate-columns (int-array (map first plans))
       :aggregate-operations (int-array (map second plans))
       ;; Counting rows and proving their identity do not read the entity value.
       :entity-value? (or (some #(and (zero? (first %))
                                     (not= GroupedAggregate/COUNT (second %))) plans)
                         (some #(= entity %)
                               (tree-seq sequential? seq
                                 (mapcat expression-arguments
                                   (remove #(instance? Pattern %) (:qwhere parsed))))))})))

(defn- small-group-domain? [parsed fields database schema]
  (let [attributes (into {} (map (juxt :variable :attr)) fields)
        groups (filter #(instance? Variable %) (parser/find-elements (:qfind parsed)))
        lmdb (.-lmdb ^Store (.-store ^DB database))]
    (loop [groups (seq groups) product (long 1)]
      (if-let [group (first groups)]
        (when-let [attr (attributes (:symbol group))]
          (let [budget (quot (long *max-groups*) product)
                cardinality (scan/bounded-attribute-cardinality lmdb schema attr budget)]
            (when (and (pos? cardinality) (<= cardinality budget))
              (recur (next groups) (* product cardinality)))))
        true))))

(defn plan
  "Admit a broad, single-entity native scan. Schema, bindings and the ordinary
  plan's selectivity estimate are checked for each execution. Return nil when
  the existing executor must retain control."
  [{:keys [parsed-q sources rels rules access-plans] :as context}]
  (when (and *enabled?* (empty? rules) (empty? access-plans)
             (= #{'$} (set (keys sources))))
    (let [database (get sources '$)
          store (when (instance? DB database) (.-store ^DB database))
          schema (when (instance? Store store) (db/-schema database))
          patterns (filterv #(instance? Pattern %) (:qwhere parsed-q))
          fields (mapv
                   (fn [pattern]
                     (let [[entity attr value] (:pattern pattern)]
                       (when (and (= 3 (count (:pattern pattern)))
                                  (or (instance? DefaultSrc (:source pattern))
                                      (= '$ (get-in pattern [:source :symbol])))
                                  (instance? Variable entity)
                                  (instance? Constant attr) (keyword? (:value attr))
                                  (instance? Variable value))
                         {:entity (:symbol entity) :attr (:value attr)
                          :variable (:symbol value)}))) patterns)
          inputs (rest (:qin parsed-q))
          input-vars (mapv #(get-in % [:variable :symbol]) inputs)
          first-input (first (:qin parsed-q))
          steps (vec (mapcat :steps (ffirst (vals (:plan context)))))
          anchor (first steps)
          size (:mcount anchor)]
      (when (and schema (not (db/pending-tx-cache? database))
                 (not (lmdb/writing? (.-lmdb ^Store store)))
                 (instance? BindScalar first-input)
                 (instance? SrcVar (:variable first-input))
                 (= '$ (get-in first-input [:variable :symbol]))
                 (every? #(and (instance? BindScalar %)
                               (instance? Variable (:variable %))) inputs)
                 (seq fields) (<= (count fields) 32) (every? some? fields)
                 (= 1 (count (set (map :entity fields))))
                 (apply distinct? (concat (map :variable fields)
                                          [(-> fields first :entity)] input-vars))
                 (apply distinct? (map :attr fields))
                 (every? #(and (contains? value-types (get-in schema [(:attr %) :db/valueType]))
                               (not= :db.cardinality/many (get-in schema [(:attr %) :db/cardinality]))
                               (not (get-in schema [(:attr %) :db/noindex]))
                               (integer? (get-in schema [(:attr %) :db/aid]))) fields)
                 (every? #(and (= 1 (count (:tuples %)))
                               (set/subset? (set (keys (:attrs %))) (set input-vars))) rels)
                 (number? size) (<= *min-input* size))
        (let [total (max (long (i/a-size store (:attr anchor)))
                         (long (.-max-eid ^DB database)))
              fields (sort-by #(get-in schema [(:attr %) :db/aid]) fields)
              aids (mapv #(get-in schema [(:attr %) :db/aid]) fields)
              entity (:entity (first fields))
              variables (into [entity] (concat (map :variable fields) input-vars))
              columns (zipmap variables (range))
              values (mapv #(resolve/context-resolve-val context %) input-vars)
              clauses (remove #(instance? Pattern %) (:qwhere parsed-q))]
          (when (and total (pos? total)
                     (<= *min-scan-fraction* (/ (double size) (double total)))
                     (< (long (last aids)) Integer/MAX_VALUE)
                     (<= (inc (- (last aids) (first aids))) 4096)
                     (every? scalar? values))
            (when-let [program (row-program clauses columns context)]
              (when-let [aggregate (aggregate-plan parsed-q (:columns program) entity)]
                (when (small-group-domain? parsed-q fields database schema)
                  (let [snapshot (i/schema-snapshot store)]
                    (when (= schema (:schema snapshot))
                      (merge aggregate program
                        {:database database :aids (int-array aids)
                         :schema-revision (:revision snapshot)
                         :scan-columns (int-array (map columns (map :variable fields)))
                         :input-columns (int-array (map columns input-vars))
                         :input-values values
                         :summary {:operator :fused-aggregate :scan :eav
                                   :attributes (mapv :attr fields)
                                   :expressions (count (filter #(instance? Function %) clauses))
                                   :estimated-input size :retained-input-tuples 0
                                   :scratch-width (count (:columns program))}}))))))))))))

(defn- check! []
  (when (.isInterrupted (Thread/currentThread))
    (throw (InterruptedException. "Query interrupted")))
  (timeout/assert-time-left)
  (request/check! :request))

(defn execute
  "Consume borrowed rows synchronously and retain only aggregate states.
  Return nil when the admitted schema differs from the read transaction."
  [context {:keys [database aids scan-columns columns input-columns input-values
                   aggregate-columns aggregate-operations operations summary
                   schema-revision entity-value?]}]
  (let [accumulator (GroupedAggregate. aggregate-columns aggregate-operations)
        row (object-array (count columns))
        input-count (long-array 1)
        operations (object-array operations)
        n (alength operations)]
    (dotimes [i (count input-values)]
      (aset row (aget ^ints input-columns i) (nth input-values i)))
    (when (scan/fold-entity-rows
        (.-lmdb ^Store (.-store ^DB database)) aids scan-columns row
        (fn [^objects row]
          (loop [i 0]
            (if (= i n)
              (do (.add accumulator row)
                  (aset input-count 0 (unchecked-inc (aget input-count 0)))
                  nil)
              (let [[index call] (aget operations i)
                    value (call row)]
                (if (nil? index)
                  (when value (recur (inc i)))
                  (when-not (nil? value)
                    (aset row (int index) value)
                    (recur (inc i))))))))
        check! schema-revision (boolean entity-value?))
      (let [result {:rows (mapv vec (.results accumulator))
                    :input-tuples (aget input-count 0)
                    :group-count (.groupCount accumulator)}]
        (assoc context :rels [] :late-clauses []
               :fused-aggregation result
               :fused-aggregation-plan (merge summary (dissoc result :rows)))))))
