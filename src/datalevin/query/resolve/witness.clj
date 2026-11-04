(ns ^:no-doc datalevin.query.resolve.witness
  "Bounded index probes and grouped reads for single-entity existence bodies."
  (:require [clojure.set :as set]
            [datalevin.db :as db]
            [datalevin.interface :as i]
            [datalevin.lmdb :as lmdb]
            [datalevin.query.optimizer.existence :as existence]
            [datalevin.query-util :as qu]
            [datalevin.relation :as r]
            [datalevin.storage :as storage]
            [datalevin.storage.scan :as scan]
            [datalevin.timeout :as timeout])
  (:import [java.util List Map]
           [datalevin.storage Store]
           [datalevin.utl QueryKey QueryKey$KeyMap]
           [org.eclipse.collections.impl.list.mutable FastList]))

(def ^:dynamic *enabled?* true)
(def ^:dynamic *explain* nil)
(def ^:private ^:const ^long batch-size 256)

(defn- native-read?
  [context database]
  (and (db/db? database)
       (instance? Store (:store database))
       (not (lmdb/writing? (.-lmdb ^Store (:store database))))
       (not (db/pending-tx-cache? database))
       (nil? (:delta-bound-values context))))

(defn resolve-single-pattern
  "Match distinct correlation keys with one bounded index probe per key.
  Admit one bound entity or value and a local witness; leave equality between
  pattern positions, lookup refs and unsupported value domains to the resolver."
  [context outer vars clauses]
  (when (and *enabled?* (= 1 (count clauses)) (= 1 (count vars)))
    (let [clause   (first clauses)
          sourced? (and (vector? clause) (qu/source? (first clause)))
          database (if sourced? (get (:sources context) (first clause))
                       qu/*implicit-source*)
          pattern  (if sourced? (subvec clause 1) clause)
          key-var  (first vars)]
      (when (and (vector? pattern) (= 3 (count pattern))
                 (keyword? (second pattern)) (native-read? context database))
        (let [[entity attr value] pattern
              props (get (db/-schema database) attr)
              local? #(or (and (qu/binding-var? %) (not= key-var %))
                          (= '_ %) (qu/placeholder? %))
              index (when (:db/aid props)
                      (cond
                        (and (= entity key-var) (local? value)) :eav
                        (and (= value key-var) (local? entity)
                             (not (:db/noindex props))
                             (#{:db.type/long :db.type/ref :db.type/string}
                              (:db/valueType props))) :ave))]
          (when index
            (let [keys (r/project-distinct outer vars)
                  ^List tuples (:tuples keys)
                  key-idx (int ((:attrs keys) key-var))
                  valid? (if (= :eav index)
                           #(and (int? %) (not (neg? (long %))))
                           (if (= :db.type/string (:db/valueType props))
                             string? int?))]
              ;; Check the entire domain before taking the specialized path.
              (when (every? #(valid? (aget ^objects % key-idx)) tuples)
                (let [matches (FastList.)
                      store (:store database)]
                  (doseq [^objects tuple tuples]
                    (timeout/assert-time-left)
                    (let [key (aget tuple key-idx)]
                      (when (if (= :eav index)
                              (db/-ea-populated? database key attr)
                              (some? (i/av-first-e store attr key)))
                        (.add matches tuple))))
                  (when *explain*
                    (vswap! *explain* update :existence-witness-plans (fnil conj [])
                            {:strategy :index-presence :index index :attribute attr
                             :correlation-vars vars :input-keys (.size tuples)
                             :probes (.size tuples) :matched-keys (.size matches)}))
                  (r/relation! (:attrs keys) matches))))))))))

(defn- pattern? [clause]
  (and (vector? clause) (= 3 (count clause))
       (qu/binding-var? (first clause)) (keyword? (second clause))))

(defn- value-kind [value-type]
  (case value-type
    (:db.type/long :db.type/ref :db.type/double :db.type/float) :number
    :db.type/string :string
    :db.type/instant :instant
    :db.type/boolean :boolean
    nil))

(defn- operand-kind [types arg]
  (if (qu/binding-var? arg)
    (get types arg)
    (cond (number? arg) :number
          (string? arg) :string
          (instance? java.util.Date arg) :instant)))

(defn- compatible-constant? [value-type value]
  (case value-type
    (:db.type/long :db.type/ref) (integer? value)
    :db.type/double (instance? Double value)
    :db.type/float (instance? Float value)
    :db.type/string (string? value)
    :db.type/boolean (instance? Boolean value)
    :db.type/instant (= java.util.Date (class value))
    false))

(defn- safe-comparison? [types clause]
  (or (contains? '#{= not=} (ffirst clause))
      (let [kinds (mapv #(operand-kind types %) (rest (first clause)))]
        (and (seq kinds) (every? #{:number :string :instant} kinds)
             (apply = kinds)))))

(defn- bindings-ready? [vars clauses]
  (loop [known (set vars) clauses (seq clauses)]
    (if-let [clause (first clauses)]
      (if (pattern? clause)
        (recur (into known (qu/collect-vars clause)) (next clauses))
        (and (set/subset? (qu/collect-vars clause) known)
             (recur known (next clauses))))
      true)))

(defn- shape [database vars clauses]
  (let [patterns (filterv pattern? clauses)
        predicates (filterv existence/simple-predicate? clauses)
        [entity anchor-attr anchor-var :as anchor] (first patterns)
        schema (db/-schema database)
        props (get schema anchor-attr)
        types (into {entity :number}
                    (keep (fn [[_ attr value]]
                            (when (qu/binding-var? value)
                              [value (value-kind (:db/valueType (get schema attr)))])))
                    patterns)
        known (into (set vars) (mapcat qu/collect-vars) patterns)]
    (when (and anchor (= (count clauses) (+ (count patterns) (count predicates)))
               (= :number (types entity))
               (bindings-ready? vars clauses)
               (not (contains? (set vars) entity))
               (< 1 (count (distinct (map second patterns))))
               (contains? (set vars) anchor-var)
               (#{:db.type/long :db.type/ref} (:db/valueType props))
               (not (:db/noindex props))
               (every? (fn [[e attr term]]
                         (let [p (get schema attr)]
                           (and (= entity e) (:db/aid p)
                                (not= :db.cardinality/many (:db/cardinality p))
                                (value-kind (:db/valueType p))
                                (or (not (qu/binding-var? term))
                                    (= (types term) (value-kind (:db/valueType p))))
                                (or (qu/binding-var? term) (qu/placeholder? term)
                                    (= term '_)
                                    (compatible-constant? (:db/valueType p) term))))) patterns)
               (every? #(and (set/subset? (qu/collect-vars %) known)
                              (= 3 (count (first %)))
                              (safe-comparison? types %)) predicates))
      {:entity entity :anchor anchor :schema schema
       :patterns patterns :predicates predicates})))

(defn- prepare [shape vars compile-predicate context]
  (let [{:keys [entity anchor schema patterns predicates]} shape
        [_ anchor-attr anchor-var] anchor
        width (count vars)
        attributes (->> patterns (map second) distinct (remove #{anchor-attr})
                        (sort-by #(get-in schema [% :db/aid])) vec)
        slots (zipmap attributes (range (inc width) (+ (inc width) (count attributes))))
        initial (assoc (zipmap vars (range)) entity width)
        state
        (reduce
          (fn [{:keys [attrs] :as state} [_ attr term]]
            (let [slot (if (= attr anchor-attr) (attrs anchor-var) (slots attr))]
              (cond
                (or (= term '_) (qu/placeholder? term)) state
                (qu/binding-var? term)
                (if-some [previous (get attrs term)]
                  (if (== (long slot) (long previous)) state
                      (update state :checks conj [slot previous]))
                  (assoc state :attrs (assoc attrs term slot)))
                :else (update state :checks conj [slot term :constant]))))
          {:attrs initial :checks []} patterns)
        relation (r/relation! (:attrs state) nil)
        preds (mapv #(compile-predicate context relation (ffirst %) (rest (first %))) predicates)
        checks (:checks state)]
    (assoc shape
           :attributes attributes :width width :slots (:attrs state)
           :attrs-v (mapv #(vector % {:skip? false}) attributes)
           :reader (when (seq attributes)
                     (storage/prepare-eav-scan-v-list
                       schema (mapv #(vector % {:skip? false}) attributes)))
           :matches?
           (fn [^objects row]
             (and (every? (fn [[slot value constant?]]
                            (QueryKey/equiv (aget row (int slot))
                                            (if constant? value (aget row (int value))))) checks)
                  (every? #(% row) preds))))))

(defn- execute [database outer vars plan]
  (let [{:keys [anchor schema width attributes attrs-v reader matches?]} plan
        [_ attr anchor-var] anchor
        width (long width)
        props (schema attr)
        outer-keys (r/project-distinct outer vars)
        anchor-idx (int ((:attrs outer-keys) anchor-var))
        ^List tuples (:tuples outer-keys)]
    ;; A mismatched input domain stays on the ordinary resolver path.
    (when (every? #(int? (aget ^objects % anchor-idx)) tuples)
      (let [^Map active (QueryKey$KeyMap.)
            keys (FastList.)
            matches (FastList.)
            buffer (FastList.)
            ;; visited, grouped-read inputs, groups, maximum buffer, matches
            ^longs counts (long-array 5)
            row (object-array (+ width 1 (count attributes)))]
        (doseq [^objects key tuples]
          (timeout/assert-time-left)
          (let [value (aget key anchor-idx)
                ^List bucket (or (.get active value)
                                 (let [bucket (FastList.)]
                                   (.put active value bucket)
                                   (.add keys (object-array [value]))
                                   bucket))]
            (.add bucket key)))
        (let [flush!
              (fn []
                (when-not (.isEmpty buffer)
                  (timeout/assert-time-left)
                  (aset-long counts 1 (+ (aget counts 1) (.size buffer)))
                  (aset-long counts 2 (inc (aget counts 2)))
                  (let [^List rows (if reader
                                     (db/-eav-scan-v-list database buffer 1 attrs-v reader)
                                     buffer)]
                    (dotimes [i (.size rows)]
                      (timeout/assert-time-left)
                      (let [^objects witness (.get rows i)
                            value (aget witness 0)
                            ^List bucket (.get active value)]
                        (when (and bucket (not (.isEmpty bucket)))
                          (System/arraycopy witness 1 row width (inc (count attributes)))
                          (let [iterator (.iterator bucket)]
                            (loop []
                              (when (.hasNext iterator)
                                (timeout/assert-time-left)
                                (let [key (.next iterator)]
                                  (System/arraycopy key 0 row 0 width)
                                  (when (matches? row)
                                    (.add matches key)
                                    (.remove iterator)
                                    (aset-long counts 4 (inc (aget counts 4)))))
                                (recur))))
                          (when (.isEmpty bucket) (.remove active value))))))
                  (.clear buffer)))
              visit!
              (fn [value eid]
                 (aset-long counts 0 (inc (aget counts 0)))
                 (if (.containsKey active value)
                   (do (.add buffer (object-array [value eid]))
                       (aset-long counts 3 (max (aget counts 3) (.size buffer)))
                       (when (<= batch-size (.size buffer)) (flush!))
                       (when-not (.containsKey active value) :datalevin/terminate-visit))
                   :datalevin/terminate-visit))]
          (scan/visit-ave-entities
            (.-lmdb ^Store (:store database)) (:db/aid props) (:db/valueType props)
            keys timeout/assert-time-left visit!)
          (flush!)
          (when *explain*
            (vswap! *explain* update :existence-witness-plans (fnil conj [])
                    {:strategy :grouped-witness :anchor-attribute attr
                     :correlation-vars vars :attributes attributes
                     :input-keys (.size tuples) :anchor-keys (.size keys)
                     :visited-witnesses (aget counts 0)
                     :grouped-read-witnesses (aget counts 1)
                     :read-batches (aget counts 2)
                     :max-buffered-witnesses (aget counts 3)
                     :matched-keys (aget counts 4)}))
          (r/relation! (:attrs outer-keys) matches))))))

(defn resolve-existence
  "Return distinct matched correlation keys without retaining complete witness
  relations. Unsupported bodies and transactional overlays use the resolver."
  [context outer vars clauses compile-predicate]
  (let [database qu/*implicit-source*]
    (or (resolve-single-pattern context outer vars clauses)
        (when (and *enabled?* (native-read? context database))
          (when-let [shape (shape database vars clauses)]
            (execute database outer vars
                     (prepare shape vars compile-predicate context)))))))
