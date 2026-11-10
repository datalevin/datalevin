;; Copyright (c) Huahai Yang. All rights reserved.
;; Distributed under the Eclipse Public License 2.0.
(ns ^:no-doc datalevin.db.tx.compile
  "Pure transaction analysis. No database, writer, entity allocation or pending view."
  (:require [datalevin.bits :as b]
            [datalevin.custom-datalog :as cd]
            [datalevin.db.tx.plan :as plan]
            [datalevin.prepare])
  (:import [datalevin.db.tx.plan MixedAttrInfo ScalarEntityRead]
           [datalevin.prepare PrepareCtx]
           [java.util HashMap]
           [org.eclipse.collections.impl.list.mutable FastList]))

(def ^:private ordinary-scalar-types
  #{nil :db.type/long :db.type/string :db.type/keyword :db.type/symbol
    :db.type/float :db.type/double :db.type/boolean :db.type/instant
    :db.type/uuid :db.type/bytes :db.type/bigint :db.type/bigdec})

(def ^:private ordinary-unique-types
  #{:db.type/boolean :db.type/instant :db.type/keyword :db.type/long
    :db.type/string :db.type/symbol :db.type/uuid :db.type/ref})

(defn- ordinary-unique? [schema props]
  (or (nil? (:db/unique props))
      (contains? ordinary-unique-types (:db/valueType props))
      (and (= :db.type/tuple (:db/valueType props))
           (every? ordinary-unique-types
                   (or (some->> (:db/tupleAttrs props)
                                (map #(:db/valueType (schema %))))
                       (:db/tupleTypes props)
                       [(:db/tupleType props)])))))

(defn- ordinary-attr? [a props]
  (and props (keyword? a) (not= "db" (namespace a))
       (not (plan/reverse-ref-attr? a))
       (not= :db.cardinality/many (:db/cardinality props))
       (not= :db.unique/identity (:db/unique props))
       (not (:db.attr/preds props))
       (or (contains? ordinary-scalar-types (:db/valueType props))
           (#{:db.type/ref :db.type/tuple} (:db/valueType props)))))

(def ^:private ordinary-unique-attr-marker
  ;; Attribute keys are keywords. Reserve a distinct key for this request flag.
  (Object.))

(defn ordinary-attr-info!
  "Bind ordinary addition metadata in request-owned scratch. Schema and tuple
  sources are immutable PrepareCtx inputs; no value correction or writer reads."
  [^HashMap infos schema sources a]
  (or (.get infos a)
      (let [props (schema a)
            ordinary? (and (ordinary-attr? a props)
                           (ordinary-unique? schema props))
            source-tuples (get sources a)
            ;; Composite tuples cannot themselves be tuple sources.
            ordinary-tuples?
            (and ordinary?
                 (reduce-kv
                   (fn [_ tuple _]
                     (let [^MixedAttrInfo info (ordinary-attr-info! infos schema sources tuple)]
                       (if (.-ordinary? info) true (reduced false))))
                   true source-tuples))
            info (MixedAttrInfo.
                   props (boolean ordinary?)
                   (boolean (and ordinary? (not (:db/tupleAttrs props))
                                 ordinary-tuples?))
                   (boolean (and ordinary?
                                 (contains? ordinary-scalar-types (:db/valueType props))
                                 (nil? (:db/unique props))
                                 (not (:db/tupleAttrs props)) (not source-tuples)))
                   source-tuples (some? (:db/valueType props)))]
        (when (:db/unique props) (.put infos ordinary-unique-attr-marker true))
        (.put infos a info)
        info)))

(defn ordinary-unique-attrs-present?
  "Whether these request-owned bindings contain a unique attribute."
  [^HashMap infos]
  (.containsKey infos ordinary-unique-attr-marker))

(def ^:private blind-unique-types
  #{:db.type/boolean :db.type/instant :db.type/keyword :db.type/long
    :db.type/string :db.type/symbol :db.type/uuid})

(defn- blind-attr-supported?
  [attr props tuple-source-attrs allow-unique?]
  (and props
       (keyword? attr)
       (not (contains? #{:db/id :db/created-at :db/updated-at} attr))
       (not= "db" (namespace attr))
       (not (plan/reverse-ref-attr? attr))
       (or (nil? (:db/unique props))
           (and (or (identical? (:db/unique props) :db.unique/value)
                    (and allow-unique?
                         (identical? (:db/unique props) :db.unique/identity)))
                (contains? blind-unique-types (:db/valueType props))))
       (not (:db.attr/preds props))
       (not (contains? props :db/tupleAttrs))
       (not (contains? props :db/tupleType))
       (not (contains? props :db/tupleTypes))
       (not (contains? tuple-source-attrs attr))))

(defn blind-attr-info!
  "Bind blind-insert flags from immutable schema inputs in request-owned scratch.
  Value normalization, uniqueness probes and entity binding remain runtime work."
  [^HashMap infos schema tuple-source-attrs allow-unique? attr]
  (or (.get infos attr)
      (let [props (schema attr)
            vt (:db/valueType props)
            ^objects info (object-array 6)]
        (aset info 0 props)
        ;; Nil rejects an attribute; zero is a supported cardinality-one proof.
        (aset info 1 (when (blind-attr-supported?
                            attr props tuple-source-attrs allow-unique?)
                       (bit-or (if (:db/unique props) 1 0)
                               (if (identical? :db.cardinality/many
                                               (:db/cardinality props)) 2 0))))
        (aset info 2 (not (or (:db/fulltext props) (:db/embedding props)
                             (:db/noindex props) (map? vt)
                             (#{:db.type/vec :db.type/idoc :data} vt))))
        (aset info 3 (Boolean/valueOf
                      (boolean (#{:db.type/string :db.type/keyword
                                  :db.type/symbol} vt))))
        (aset info 4 vt)
        (aset info 5 (not (or (cd/custom-type? vt) (map? vt)
                             (identical? vt :db.type/idoc)
                             (identical? vt :db.type/tuple))))
        (.put infos attr info)
        info)))

(defn blind-value-ready?
  "Recognize existing no-correction cases without calling a value conversion."
  [^objects info value]
  (case (aget info 4)
    (nil :data) (and (some? value) (not (coll? value)))
    :db.type/long (instance? Long value)
    :db.type/ref (and (instance? Long value) (pos? (long value)))
    :db.type/string (string? value)
    :db.type/boolean (boolean? value)
    :db.type/keyword (keyword? value)
    :db.type/symbol (identical? clojure.lang.Symbol (class value))
    :db.type/uuid (uuid? value)
    :db.type/instant (identical? java.util.Date (class value))
    :db.type/bytes (bytes? value)
    :db.type/double (and (instance? Double value) (Double/isFinite (double value)))
    :db.type/float (and (instance? Float value) (Float/isFinite (float value)))
    false))

(def scalar-update-value-types
  #{:db.type/long :db.type/string :db.type/keyword :db.type/symbol
    :db.type/float :db.type/double :db.type/boolean :db.type/instant
    :db.type/uuid :db.type/bytes :db.type/bigint :db.type/bigdec})

(defn scalar-attribute-props
  "Return existing scalar eligibility metadata without resolving an entity."
  ([store-schema tuple-source-attrs attr value]
   (scalar-attribute-props store-schema tuple-source-attrs attr value false))
  ([store-schema tuple-source-attrs attr value literal?]
   (let [props (store-schema attr)
         vt (:db/valueType props)]
     (when (and (keyword? attr) props
                (not= "db" (namespace attr))
                (some? (:db/aid props))
                (some? value)
                (not (contains? tuple-source-attrs attr))
                ;; Exactly aid/type proves that all exclusion flags are absent.
                ;; Rich schemas retain the same individual eligibility checks.
                (or (and (= 2 (count props)) (contains? props :db/valueType))
                    (and (not (identical? (:db/cardinality props) :db.cardinality/many))
                         (nil? (:db/unique props))
                         (nil? (:db/tupleAttrs props))
                         (not (contains? props :db/tupleType))
                         (nil? (:db/tupleTypes props))
                         (nil? (:db.attr/preds props))
                         (or (not literal?)
                             (not (or (:db/fulltext props) (:db/vector props)
                                      (:db/embedding props))))))
                ;; Legacy reference operands may be host numbers. Preserve
                ;; exclusion checks before any conversion can invoke user code.
                (or (identical? vt :db.type/long) (nil? vt)
                    (contains? scalar-update-value-types vt)
                    (and (not literal?) (identical? vt :db.type/ref)
                         (plan/resolved-positive-ref? value))))
       props))))

(defn append-scalar-candidate!
  "Append one raw operand after eligibility and optional distinctness checks."
  [^FastList candidates ^java.util.HashSet seen e attr value vt]
  (when (or (nil? seen) (.add seen [e attr]))
    (.add candidates e)
    (.add candidates attr)
    (.add candidates value)
    (.add candidates vt)
    e))

(defn- canonical-scalar-candidates
  "Copy identity-proven operands without entering the legacy correction path."
  [^FastList candidates]
  (loop [i 0 entries (transient [])]
    (if (< i (.size candidates))
      (recur (+ i 4)
             (conj! entries [(.get candidates i) (.get candidates (+ i 1))
                            (.get candidates (+ i 2))]))
      (persistent! entries))))

(def ^:const scalar-entity-read-min-attrs 8)

(defn prepare-scalar-entity-read
  [store-schema attributes]
  ;; Four-attribute projections cost more CPU on Linux. Use point seeks for
  ;; narrow updates; retain a layout for wider updates so stamping can read
  ;; their attributes with one shared cursor inside the write transaction.
  (when (>= (count attributes) scalar-entity-read-min-attrs)
    (let [names (sort-by #(:db/aid (store-schema %)) attributes)]
      (ScalarEntityRead. (object-array names)
                         (long-array (map #(:db/aid (store-schema %)) names))))))

(defn scalar-update-entity-reads
  ([store-schema entries] (scalar-update-entity-reads store-schema entries 0))
  ([store-schema entries entry-offset]
   (when (>= (count entries) scalar-entity-read-min-attrs)
     (let [widths (java.util.HashMap.)]
       (doseq [entry entries]
         (let [eid (nth entry entry-offset)]
           (.put widths eid (inc (long (or (.get widths eid) 0))))))
      ;; Count first: narrow entities need neither grouped entries nor an
      ;; attribute collection. Keep nil when all reads can use point seeks.
       (when-let [attributes
                  (reduce
                   (fn [attributes ^java.util.Map$Entry entry]
                     (let [width (long (.getValue entry))]
                       (if (>= width scalar-entity-read-min-attrs)
                         (assoc attributes (.getKey entry) (FastList. (int width)))
                         attributes)))
                   nil (.entrySet widths))]
         (doseq [entry entries]
           (let [eid (nth entry entry-offset)
                 attr (nth entry (inc entry-offset))]
             (when-let [^FastList names (get attributes eid)]
               (.add names attr))))
         (reduce-kv (fn [layouts eid names]
                      (assoc layouts eid (prepare-scalar-entity-read store-schema names)))
                    {} attributes))))))

(defn scalar-update-request?
  "Reject incompatible forms before candidate construction or identity reads."
  [initial-es]
  (reduce
   (fn [_ entity]
     (if (or (and (sequential? entity) (nil? (meta entity)) (= 4 (count entity))
                  (identical? :db/add (nth entity 0)))
             (and (map? entity) (nil? (meta entity)) (> (count entity) 1)
                  (plan/resolved-positive-ref? (:db/id entity))))
       true
       (reduced false)))
   true initial-es))

(def ^:private inert-value-classes
  #{String Long Integer Short Byte Double Float Boolean
    clojure.lang.Keyword clojure.lang.Symbol clojure.lang.BigInt
    clojure.lang.Ratio java.math.BigInteger java.math.BigDecimal
    java.util.UUID java.util.Date java.sql.Date java.sql.Timestamp
    java.time.Instant (class (byte-array 0))})

(def ^:private inert-id-classes
  #{Long Integer Short Byte clojure.lang.BigInt java.math.BigInteger})

(defn- inert-scalar-value?
  "Reject host numeric subclasses, including ones wrapped by a Clojure number."
  [value]
  (or (instance? Long value) (string? value)
      (and (contains? inert-value-classes (class value))
           (cond
             (identical? clojure.lang.BigInt (class value))
             (identical? java.math.BigInteger
                         (class (.toBigInteger ^clojure.lang.BigInt value)))

             (identical? clojure.lang.Ratio (class value))
             (and (identical? java.math.BigInteger
                              (class (.-numerator ^clojure.lang.Ratio value)))
                  (identical? java.math.BigInteger
                              (class (.-denominator ^clojure.lang.Ratio value))))

             (identical? java.math.BigDecimal (class value))
             (identical? java.math.BigInteger
                         (class (.unscaledValue ^java.math.BigDecimal value)))

             :else true))))

(defn- plain-map? [value]
  (or (identical? clojure.lang.PersistentArrayMap (class value))
      (identical? clojure.lang.PersistentHashMap (class value))))

(defn- positive-literal-id? [e]
  (if (instance? Long e)
    (pos? (long e))
    (and (contains? inert-id-classes (class e))
         (inert-scalar-value? e)
         (plan/resolved-positive-ref? e))))

(defn literal-scalar-shape
  "Recognize inert materialized literal forms without schema reads or scratch.
  The caller may carry this structural proof into compile-tx."
  [forms]
  (when (and (identical? clojure.lang.PersistentVector (class forms))
             (nil? (meta forms)) (pos? (count forms)))
    (loop [i 0 operations? true]
      (if (< i (count forms))
        (let [form (nth forms i)]
          (when (or (and (identical? clojure.lang.PersistentVector (class form))
                         (nil? (meta form)) (= 4 (count form))
                         (identical? :db/add (nth form 0))
                         (positive-literal-id? (nth form 1)))
                    (and (plain-map? form) (nil? (meta form)) (> (count form) 1)
                         (positive-literal-id? (:db/id form))))
            (recur (unchecked-inc i) (and operations? (not (map? form))))))
        (if operations?
          ;; A repeated pair cannot form a distinct-E/A region. Decline the
          ;; small ordered canary before allocating context or HashSet scratch.
          (when-not (and (= 2 (count forms))
                         (let [a (nth forms 0) b (nth forms 1)
                               attr (nth a 2)]
                           (and (keyword? attr) (identical? attr (nth b 2))
                                (== (long (nth a 1)) (long (nth b 1))))))
            :scalar-operations)
          :scalar)))))

(defn- literal-scalar-props
  [store-schema tuple-source-attrs attr value]
  ;; Structural analysis proved the ID; inspect only inert values before any
  ;; hashing or correction. Attribute and value eligibility stays compiler-owned.
  (when (and (keyword? attr) (pos? (count (name attr)))
             (not (plan/reverse-ref-attr? attr))
             (inert-scalar-value? value))
    (when-let [props (scalar-attribute-props
                      store-schema tuple-source-attrs attr value true)]
      ;; Date formatting can invoke a user-supplied default TimeZone.
      ;; Identity preparation for dates remains safe.
      (when-not
        (and (instance? java.util.Date value)
             (some? (:db/valueType props))
             (not (identical? :db.type/instant (:db/valueType props))))
        props))))

(defn- append-literal-candidate!
  [store-schema tuple-source-attrs candidates seen e attr value]
  (when-let [props (literal-scalar-props store-schema tuple-source-attrs attr value)]
    (append-scalar-candidate! candidates seen (long e) attr value (:db/valueType props))))

(defn- canonical-scalar-value?
  "Prove correction is the identity and cannot reject this inert value."
  [validate? vt value]
  (if (identical? vt :db.type/long)
    (instance? Long value)
    (case vt
      nil true
      :db.type/string (string? value)
      :db.type/keyword (keyword? value)
      :db.type/symbol (identical? clojure.lang.Symbol (class value))
      :db.type/boolean (boolean? value)
      :db.type/uuid (uuid? value)
      :db.type/instant (instance? java.util.Date value)
      :db.type/bytes (bytes? value)
      :db.type/bigint (and (instance? java.math.BigInteger value)
                           (or (not validate?) (b/valid-data? value vt)))
      :db.type/bigdec (and (instance? java.math.BigDecimal value)
                           (or (not validate?) (b/valid-data? value vt)))
      :db.type/float (and (instance? Float value)
                          (or (not validate?) (Float/isFinite (float value))))
      :db.type/double (and (instance? Double value)
                           (or (not validate?) (Double/isFinite (double value))))
      false)))

(defn- scalar-validation-rejection?
  "Recognize a provisional rejection without formatting or coercing the value."
  [validate? vt value]
  (and validate?
       (try (not (b/valid-data? value (or vt :data)))
            (catch IllegalArgumentException _ true)
            (catch ClassCastException _ true)
            (catch ArithmeticException _ true))))

(defn- compile-one-scalar
  [^PrepareCtx ctx forms e attr value operation?]
  (when-let [props (literal-scalar-props
                    (.-schema ctx) (:db/attrTuples (.-rschema ctx)) attr value)]
    (let [vt (:db/valueType props)
          validate? (:validate-data? (.-opts ctx))]
      (if (canonical-scalar-value? validate? vt value)
        (if (and operation? (instance? Long e))
          (plan/->TxPlan ctx :scalar-operations forms nil)
          (plan/->TxPlan ctx :scalar [[(long e) attr value]] nil))
        (when (scalar-validation-rejection? validate? vt value)
          {:outcome :needs-writer :ctx ctx :input forms})))))

(defn- compile-scalar-operations
  [ctx forms]
  (if (= 1 (count forms))
    (let [form (nth forms 0)]
      (compile-one-scalar ctx forms (nth form 1) (nth form 2) (nth form 3) true))
    (let [store-schema (:schema ctx)
          tuple-source-attrs (:db/attrTuples (:rschema ctx))
          validate? (:validate-data? (:opts ctx))
          n (count forms)
          seen (when (> n 1) (java.util.HashSet.))]
      ;; Complete eligibility before returning a rejection: a late unsupported
      ;; attribute must retain the original caller/batch failure boundary.
      (loop [i 0 canonical? true long-ids? true rejected? false]
        (if (< i n)
          (let [[_ e attr value] (nth forms i)]
            (when-let [props (literal-scalar-props store-schema tuple-source-attrs attr value)]
              (when (or (nil? seen) (.add seen [(long e) attr]))
                (let [vt (:db/valueType props)
                      ready? (canonical-scalar-value? validate? vt value)]
                  (recur (unchecked-inc i) (and canonical? ready?)
                         (and long-ids? (instance? Long e))
                         (or rejected? (and (not ready?)
                                            (scalar-validation-rejection? validate? vt value))))))))
          (cond
            rejected? {:outcome :needs-writer :ctx ctx :input forms}
            (not canonical?) nil
            long-ids?
            ;; Runtime reads E/A/V at offset one, without interpreting keywords.
            (plan/->TxPlan ctx :scalar-operations forms
                           (scalar-update-entity-reads store-schema forms 1))
            :else
            (let [entries (mapv (fn [[_ e attr value]] [(long e) attr value]) forms)]
              (plan/->TxPlan ctx :scalar entries
                             (scalar-update-entity-reads store-schema entries)))))))))

(defn- candidate-value-outcome
  [ctx forms ^FastList candidates]
  (let [validate? (:validate-data? (:opts ctx))]
    (loop [i 0 canonical? true rejected? false]
      (if (< i (.size candidates))
        (let [value (.get candidates (+ i 2))
              vt (.get candidates (+ i 3))
              ready? (canonical-scalar-value? validate? vt value)]
          (recur (+ i 4) (and canonical? ready?)
                 (or rejected? (and (not ready?)
                                    (scalar-validation-rejection? validate? vt value)))))
        (cond
          rejected? {:outcome :needs-writer :ctx ctx :input forms}
          canonical?
          (let [entries (canonical-scalar-candidates candidates)]
            (plan/->TxPlan ctx :scalar entries
                           (scalar-update-entity-reads (:schema ctx) entries)))
          :else nil)))))

(defn- compile-canonical-scalar-region
  "Compile the Stage 1 literal scalar family. Nil means a pure shape decline;
  needs-writer retains the original forms after a pure correction failure."
  ([ctx forms] (compile-canonical-scalar-region ctx forms false))
  ([ctx forms literal-shape-checked?]
   (when (and (or literal-shape-checked? (literal-scalar-shape forms))
              (not (:auto-entity-time? (:opts ctx))))
     (if (and (= 1 (count forms)) (= 2 (count (nth forms 0))))
       ;; A single-attribute map needs neither distinctness scratch nor an
       ;; intermediate candidate buffer. Its final operands use the shared layout.
       (let [form (nth forms 0)
             ^java.util.Iterator keys (.keyIterator ^clojure.lang.IMapIterable form)
             first-key (.next keys)
             attr (if (identical? :db/id first-key) (.next keys) first-key)]
         (when (keyword? attr)
           (compile-one-scalar ctx forms (:db/id form) attr (get form attr) false)))
       (let [n (count forms)
             store-schema (:schema ctx)
             tuple-source-attrs (:db/attrTuples (:rschema ctx))
             candidates (FastList. (int (* 4 n)))
             seen (when (> n 1) (java.util.HashSet.))]
         (loop [i 0]
           (if (< i n)
             (let [form (nth forms i)]
               (when
                (if (map? form)
                  (let [e (:db/id form)]
                     ;; One literal map already has distinct attribute keys.
                    (reduce-kv
                     (fn [_ attr value]
                       (if (or (identical? :db/id attr)
                               (append-literal-candidate!
                                store-schema tuple-source-attrs candidates seen e attr value))
                         true (reduced false)))
                     true form))
                  (append-literal-candidate! store-schema tuple-source-attrs candidates seen
                                             (nth form 1) (nth form 2) (nth form 3)))
                 (recur (unchecked-inc i))))
             (candidate-value-outcome ctx forms candidates))))))))

(defn compile-scalar-region
  "Select one scalar operand layout without constructing parallel plans."
  ([ctx forms] (compile-scalar-region ctx forms (literal-scalar-shape forms)))
  ([ctx forms shape]
   (when-not (:auto-entity-time? (:opts ctx))
     (case shape
       :scalar-operations (compile-scalar-operations ctx forms)
       :scalar (compile-canonical-scalar-region ctx forms true)
       nil))))

(defn compile-tx
  "Select a pure materialized region. Unsupported input is left untouched."
  ([ctx forms] (compile-scalar-region ctx forms))
  ([ctx forms {:keys [literal-shape]}]
   (if literal-shape
     (compile-scalar-region ctx forms literal-shape)
     (compile-scalar-region ctx forms))))
