;; Copyright (c) Huahai Yang. All rights reserved.
;; Distributed under the Eclipse Public License 2.0.
(ns ^:no-doc datalevin.db.tx.compile
  "Pure transaction analysis. No database, writer, entity allocation or pending view."
  (:require [datalevin.bits :as b]
            [datalevin.custom-datalog :as cd]
            [datalevin.db.tx.plan :as plan]
            [datalevin.prepare :as prepare])
  (:import [datalevin.db.tx.plan BlindEntity MixedAttrInfo MixedInsert MixedOperands MixedUpdate
            ScalarEntityRead]
           [datalevin.prepare PrepareCtx]
           [datalevin.utl QueryKey$EquivSet QueryKey$KeySet]
           [clojure.lang MapEntry]
           [java.util ArrayList HashMap HashSet Set]
           [org.eclipse.collections.api.block HashingStrategy]
           [org.eclipse.collections.impl.set.strategy.mutable UnifiedSetWithHashingStrategy]
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
  Callers normalize values separately; ownership and entity binding stay runtime work."
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

(defn blind-unique-inline-value?
  "Conservatively reject values that can spill into the giants DB. Giant
  values include an allocated giant ID in their AVE key, so LMDB key equality
  cannot enforce their logical uniqueness."
  [textual? value]
  (if textual?
    ;; UTF-8 needs at most four bytes per UTF-16 code unit. This leaves ample
    ;; room below LMDB's 511-byte key limit for the AVG type/attribute prefix.
    (<= (count (str value)) 100)

    true))

(defn append-blind-canonical-value!
  "Collect a normalized value and its unique AV using request-owned scratch.
  Compilation proves inert canonical values before calling this worker; legacy
  preparation performs its correction first. No writer/ownership reads occur."
  [^FastList attrs ^FastList unique-avs seen-avs set-capacity
   fuse-unique-inserts? ^objects info attr value']
  (let [props (aget info 0)
        unique? (some? (:db/unique props))
        inline? (or (not unique?)
                    (blind-unique-inline-value? (aget info 3) value'))
        av (when unique?
             ;; Compact deferred pairs; vectors hash faster in eager checks.
             (if seen-avs [attr value'] (MapEntry/create attr value')))]
    (if (and unique?
             (or (not inline?)
                 (and seen-avs
                      (let [^HashSet seen
                            (or @seen-avs
                                (vreset! seen-avs (HashSet. (int set-capacity))))]
                        (not (.add seen av))))))
      false
      (do
        (when-not inline? (vreset! fuse-unique-inserts? false))
        (when av (.add unique-avs av))
        (.add attrs attr)
        (.add attrs value')
        true))))

(def ^:private instant-hashing-strategy
  ;; Storage keys retain milliseconds, unlike asymmetric Date/Timestamp
  ;; equality and Timestamp's additional nanosecond precision.
  (reify HashingStrategy
    (computeHashCode [_ value] (Long/hashCode (long (inst-ms value))))
    (equals [_ left right] (== (long (inst-ms left)) (long (inst-ms right))))))

(defn blind-value-set
  "Reuse normalized many-value equivalence in request-owned deduplication scratch."
  [vt]
  (case vt
    :db.type/instant
    (UnifiedSetWithHashingStrategy. instant-hashing-strategy)
    (:db.type/double :db.type/float :db.type/bigdec) (QueryKey$EquivSet.)
    (if (or (nil? vt) (map? vt) (identical? vt :data) (identical? vt :db.type/bytes))
      (QueryKey$KeySet.)
      (HashSet.))))

(defn blind-identity-upsert-av
  "Prove one scalar identity AV from normalized entity operands and schema."
  [store-schema ^BlindEntity entity]
  (let [^FastList attrs (.-attrs entity)]
    (loop [i           0
           identity-av nil]
      (if (< i (.size attrs))
        (let [attr  (.get attrs i)
              value (.get attrs (unchecked-inc i))
              props (store-schema attr)]
          ;; Cardinality-many map values are additive and need exact EAV
          ;; membership checks. Leave those, and entities with ambiguous
          ;; identities or additional uniqueness constraints, to the general
          ;; transaction engine.
          (cond
            (identical? (:db/cardinality props) :db.cardinality/many)
            nil

            (identical? (:db/unique props) :db.unique/value)
            nil

            (identical? (:db/unique props) :db.unique/identity)
            (when-not identity-av
              (recur (+ i 2) [attr value]))

            :else
            (recur (+ i 2) identity-av)))
        identity-av))))

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
                    (and (or (not literal?) (identical? literal? :references))
                         (identical? vt :db.type/ref)
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
                 attr (nth entry (inc (long entry-offset)))]
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

(defn distinct-mixed-update?
  "Prove canonical distinct E/A keys in request-owned scratch. Values are not
  hashed; both compiled operands and legacy mixed preflight use this proof."
  [^HashSet updates e attr]
  (and (instance? Long e) (pos? (long e)) (keyword? attr)
       (.add updates [e attr])))

(defn mixed-preflight-checker
  "Build a shape/attribute checker over captured metadata and owned scratch.
  Each call checks one already-demanded plain form, without correcting values
  or proving writer eligibility. Opaque values are never inspected or hashed.
  Unsupported containers, keys and numeric tempids return
  ::unsupported-mixed-preflight before scratch mutation so runtime can retain
  their existing validation and callback order. No input sequence is consumed."
  [ctx ^HashMap infos ^HashSet updates]
  (let [schema (:schema ctx) sources (:db/attrTuples (:rschema ctx))
        enabled? (not (:auto-entity-time? (:opts ctx)))
        safe-key? (fn [_ attr _]
                    (if (and (keyword? attr) (pos? (count (name attr))))
                      true (reduced false)))
        insert? (fn [_ attr _]
                  (if (identical? attr :db/id)
                    true
                    (if (.-insert? ^MixedAttrInfo (ordinary-attr-info! infos schema sources attr))
                      true (reduced false))))]
    (fn [entity kind]
      (if (identical? kind :insert)
        (if (and (plain-map? entity) (reduce-kv safe-key? true entity))
          (let [explicit? (contains? entity :db/id)
                tempid (:db/id entity)]
            (if (and (number? tempid) (not (instance? Long tempid)))
              ::unsupported-mixed-preflight
              (boolean
                (and (nil? (meta entity)) enabled?
                     (or (not explicit?) (string? tempid)
                         (and (instance? Long tempid) (neg? (long tempid))))
                     (> (count entity) (if explicit? 1 0))
                     (reduce-kv insert? true entity)))))
          ::unsupported-mixed-preflight)
        (if (identical? clojure.lang.PersistentVector (class entity))
          (let [op (nth entity 0 nil) attr (nth entity 2 nil)]
            (if (and (= 4 (count entity))
                     (or (not (keyword? op))
                         (and (keyword? attr) (zero? (count (name attr))))))
              ::unsupported-mixed-preflight
              (boolean
                (and (nil? (meta entity)) enabled?
                     (= 4 (count entity)) (identical? :db/add op)
                     (let [[_ e attr value] entity]
                       (and (instance? Long e) (pos? (long e)) (keyword? attr)
                            (some? value)
                            (.-update? ^MixedAttrInfo (ordinary-attr-info! infos schema sources attr))
                            (distinct-mixed-update? updates e attr)))))))
          ::unsupported-mixed-preflight)))))

(defn- positive-literal-id? [e]
  (if (instance? Long e)
    (pos? (long e))
    (and (contains? inert-id-classes (class e))
         (inert-scalar-value? e)
         (plan/resolved-positive-ref? e))))

(defn ordinary-analysis-enabled?
  "Whether the captured context permits the interpreter's ordinary-add proof."
  [ctx]
  (and (not (:auto-entity-time? (:opts ctx)))
       (empty? (:db.type/tuple (:rschema ctx)))))

(defn- ordinary-nonnegative-id?
  [value]
  (and (inert-scalar-value? value) (integer? value)
       (try (not (neg? (long value)))
            (catch IllegalArgumentException _ false))))

(defn- ordinary-value-flags
  [schema attr value]
  (when (and (keyword? attr) (pos? (count (name attr)))
             (inert-scalar-value? value))
    (let [props (schema attr)]
      (when (and (not= "db" (namespace attr))
                 (not (plan/reverse-ref-attr? attr))
                 (not (identical? (:db/unique props) :db.unique/identity))
                 (or (contains? ordinary-scalar-types (:db/valueType props))
                     (and (identical? (:db/valueType props) :db.type/ref)
                          (ordinary-nonnegative-id? value)))
                 (not (:db.attr/preds props)))
        (if (:db/unique props) 1 0)))))

(defn ordinary-addition-flags
  "Analyze one already-demanded plain form without correction or store reads.
  Bit zero marks uniqueness, bit one an implicit ID. The runtime may supply a
  private-ID proof for an AutoTempid it has already allocated; no allocator or
  callback enters this worker. Root sequence demand remains runtime-owned."
  ([ctx entity] (ordinary-addition-flags ctx entity false false))
  ([ctx entity implicit-ids? private-tempid?]
   (when (and (or (plain-map? entity)
                  (identical? clojure.lang.PersistentVector (class entity)))
              (nil? (meta entity)))
     (let [schema (:schema ctx)]
       (if (map? entity)
         (let [e (:db/id entity)
               implicit? (and implicit-ids? (not (contains? entity :db/id)))]
           (when (or implicit? private-tempid?
                     (and (inert-scalar-value? e)
                          (or (string? e) (ordinary-nonnegative-id? e)
                              (and (number? e)
                                   (try (neg? (long e))
                                        (catch IllegalArgumentException _ false))))))
             (reduce-kv
               (fn [flags attr value]
                 (if (identical? :db/id attr)
                   flags
                   (if-some [attr-flags (ordinary-value-flags schema attr value)]
                     (if (not= :db.cardinality/many (:db/cardinality (schema attr)))
                       (bit-or (long flags) (long attr-flags))
                       (reduced nil))
                     (reduced nil))))
               (if implicit? 2 0) entity)))
         (let [[op e attr value tx] entity]
           (when (and (identical? :db/add op) (<= 4 (count entity) 5)
                      (ordinary-nonnegative-id? e)
                      (or (nil? tx) (and (inert-scalar-value? tx) (integer? tx))))
             (ordinary-value-flags schema attr value))))))))

(defn ordinary-addition-proof
  "Attach pure eligibility flags to the runtime's demanded input and staging
  decision. This is a temporary interpreter proof, not a new execution region."
  [entities flags stage-resolver?]
  (cond-> {:entities entities :has-unique? (bit-test flags 0)
           :stage-resolver? stage-resolver?}
    (bit-test flags 1) (assoc :implicit-ids? true)))

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
  [store-schema tuple-source-attrs attr value allow-references?]
  ;; Structural analysis proved the ID; inspect only inert values before any
  ;; hashing or correction. Attribute and value eligibility stays compiler-owned.
  (when (and (keyword? attr) (pos? (count (name attr)))
             (not (plan/reverse-ref-attr? attr))
             (inert-scalar-value? value))
    (when-let [props (scalar-attribute-props
                      store-schema tuple-source-attrs attr value
                      (if allow-references? :references true))]
      ;; Date formatting can invoke a user-supplied default TimeZone.
      ;; Identity preparation for dates remains safe.
      (when-not
        (and (instance? java.util.Date value)
             (some? (:db/valueType props))
             (not (identical? :db.type/instant (:db/valueType props))))
        props))))

(defn- append-literal-candidate!
  [store-schema tuple-source-attrs candidates seen e attr value allow-references?]
  (when-let [props (literal-scalar-props store-schema tuple-source-attrs attr value allow-references?)]
    (append-scalar-candidate! candidates seen (long e) attr value (:db/valueType props))))

(defn- canonical-scalar-value?
  "Prove correction is the identity and cannot reject this inert value."
  [validate? vt value]
  (if (identical? vt :db.type/long)
    (instance? Long value)
    (case vt
      nil true
      :db.type/string (string? value)
      :db.type/ref (and (instance? Long value) (pos? (long value)))
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
  [^PrepareCtx ctx forms e attr value operation? allow-references?]
  (when-let [props (literal-scalar-props
                    (.-schema ctx) (:db/attrTuples (.-rschema ctx)) attr value allow-references?)]
    (let [vt (:db/valueType props)
          validate? (:validate-data? (.-opts ctx))]
      (if (canonical-scalar-value? validate? vt value)
        (if (and operation? (instance? Long e))
          (plan/->TxPlan ctx :scalar-operations forms nil)
          (plan/->TxPlan ctx :scalar [[(long e) attr value]] nil))
        (when (scalar-validation-rejection? validate? vt value)
          {:outcome :needs-writer :ctx ctx :input forms})))))

(defn- compile-scalar-operations
  [ctx forms allow-references?]
  (if (= 1 (count forms))
    (let [form (nth forms 0)]
      (compile-one-scalar ctx forms (nth form 1) (nth form 2) (nth form 3) true allow-references?))
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
            (when-let [props (literal-scalar-props store-schema tuple-source-attrs attr value allow-references?)]
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
  "Compile the literal scalar family. Nil means a pure shape decline;
  needs-writer retains the original forms after a pure correction failure."
  [ctx forms allow-references?]
  (when-not (:auto-entity-time? (:opts ctx))
    (if (and (= 1 (count forms)) (= 2 (count (nth forms 0))))
      ;; A single-attribute map needs neither distinctness scratch nor an
      ;; intermediate candidate buffer. Its final operands use the shared layout.
      (let [form (nth forms 0)
            ^java.util.Iterator keys (.keyIterator ^clojure.lang.IMapIterable form)
            first-key (.next keys)
            attr (if (identical? :db/id first-key) (.next keys) first-key)]
        (when (keyword? attr)
          (compile-one-scalar ctx forms (:db/id form) attr (get form attr) false allow-references?)))
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
                               store-schema tuple-source-attrs candidates seen e attr value allow-references?))
                        true (reduced false)))
                    true form))
                 (append-literal-candidate! store-schema tuple-source-attrs candidates seen
                                            (nth form 1) (nth form 2) (nth form 3) allow-references?))
                (recur (unchecked-inc i))))
            (candidate-value-outcome ctx forms candidates)))))))

(defn compile-scalar-region
  "Select one scalar operand layout without constructing parallel plans."
  ([ctx forms] (compile-scalar-region ctx forms (literal-scalar-shape forms)))
  ([ctx forms shape]
   (compile-scalar-region ctx forms shape false))
  ([ctx forms shape allow-references?]
   (when-not (:auto-entity-time? (:opts ctx))
     (case shape
       :scalar-operations (compile-scalar-operations ctx forms allow-references?)
       :scalar (compile-canonical-scalar-region ctx forms allow-references?)
       nil))))

(defn- literal-tempid? [value]
  (or (string? value) (and (instance? Long value) (neg? (long value)))))

(defn- pure-scalar-coercion?
  "Recognize conversions with no host callbacks, interning or default-zone work.
  The caller has already proved the exact input value class is inert."
  [vt value]
  (case vt
    (:db.type/long :db.type/ref :db.type/float :db.type/double
     :db.type/bigint :db.type/bigdec) (number? value)
    :db.type/string (or (number? value) (boolean? value) (keyword? value)
                        (symbol? value) (uuid? value)
                        (identical? java.time.Instant (class value)))
    :db.type/boolean true
    :db.type/instant (or (integer? value)
                         (identical? java.time.Instant (class value)))
    :db.type/uuid (string? value)
    false))

(defn- canonical-blind-value? [ctx ^objects info value]
  (and (inert-scalar-value? value)
       (let [vt (aget info 4)]
         (if (identical? vt :db.type/ref)
           (and (instance? Long value) (pos? (long value)))
           (canonical-scalar-value? (:validate-data? (:opts ctx)) vt value)))))

(defn- literal-many-values? [value]
  (or (identical? clojure.lang.PersistentVector (class value))
      (identical? clojure.lang.PersistentHashSet (class value))))

(defn- coercible-blind-infos
  "Prove the complete inert materialized request before any correction. Reuse
  these request-owned metadata bindings during normalized operand construction."
  [ctx forms allow-small-unique?]
  (when (and (identical? clojure.lang.PersistentVector (class forms))
             (nil? (meta forms)) (pos? (count forms)))
    (let [infos (HashMap.) tempids (HashSet.)
          allow-unique? (or allow-small-unique? (>= (count forms) 2))
          value? (fn [^objects info value]
                   (let [vt (aget info 4)]
                     (and (inert-scalar-value? value)
                          (or (not (instance? java.util.Date value))
                              (identical? vt :db.type/instant))
                          (or (not (identical? vt :db.type/ref))
                              (plan/resolved-positive-ref? value))
                          (or (canonical-blind-value? ctx info value)
                              (pure-scalar-coercion? vt value)))))
          attribute? (fn [attr raw]
                       (when (and (keyword? attr) (pos? (count (name attr))))
                         (let [^objects info (blind-attr-info!
                                               infos (:schema ctx)
                                               (:db/attrTuples (:rschema ctx)) allow-unique? attr)]
                           (and (some? (aget info 1))
                                (if (and (identical? :db.cardinality/many
                                                    (:db/cardinality (aget info 0)))
                                         (literal-many-values? raw))
                                  (and (not (and (= 2 (count raw)) (keyword? (first raw))
                                                 (contains? (:db.unique/identity (:rschema ctx))
                                                            (first raw))))
                                       (every? #(value? info %) raw))
                                  (value? info raw))))))]
      (when (reduce
              (fn [_ entity]
                (if (and (plain-map? entity) (nil? (meta entity))
                         (or (not (contains? entity :db/id))
                             (let [e (:db/id entity)]
                               (and (literal-tempid? e) (.add tempids e))))
                         (reduce-kv (fn [_ attr raw]
                                      (if (or (identical? :db/id attr) (attribute? attr raw))
                                        true (reduced false)))
                                    true entity))
                  true (reduced false)))
              true forms)
        infos))))

(defn- compile-blind-attribute!
  [ctx ^FastList attrs ^objects info attr raw-value
   {:keys [unique-avs seen-avs set-capacity has-many? fuse-unique-inserts? coerce?]}]
  (let [props (aget info 0)
        many? (identical? :db.cardinality/many (:db/cardinality props))
        unique? (some? (:db/unique props))
        ^FastList avs (when unique?
                        (or @unique-avs (vreset! unique-avs (FastList.))))
        append! (fn [raw]
                  (let [value (if (canonical-blind-value? ctx info raw)
                                raw
                                (when coerce?
                                  (try (prepare/correct-scalar-value
                                         (:validate-data? (:opts ctx)) (aget info 4) raw)
                                       (catch Exception _ nil))))]
                    (and (some? value)
                         (append-blind-canonical-value!
                           attrs avs seen-avs set-capacity fuse-unique-inserts? info attr value))))]
    (when-not (aget info 2) (vreset! fuse-unique-inserts? false))
    (if (and many? (literal-many-values? raw-value))
      ;; A two-element lookup reference remains a scalar resolver operand.
      ;; Test the keyword before membership, so host values cannot be hashed.
      (when-not (and (= 2 (count raw-value)) (keyword? (first raw-value))
                     (contains? (:db.unique/identity (:rschema ctx)) (first raw-value)))
        (let [^Set seen (blind-value-set (aget info 4))
              values (seq raw-value)]
          (when values (vreset! has-many? true))
          (loop [values values]
            (if values
              (when (append! (first values))
                (if (.add seen (.get attrs (dec (.size attrs))))
                  (recur (next values))
                  (when-not unique?
                    (.remove attrs (int (dec (.size attrs))))
                    (.remove attrs (int (dec (.size attrs))))
                    (recur (next values)))))
              true))))
      (when (append! raw-value)
        (when many? (vreset! has-many? true))
        true))))

(defn- compile-blind-entity
  [ctx ^HashMap infos ^HashSet tempids state entity]
  (let [explicit? (contains? entity :db/id)
        tempid (:db/id entity)]
    (when (or (not explicit?)
              (and (literal-tempid? tempid) (.add tempids tempid)))
      (let [attrs (FastList. (int (* 2 (count entity))))
            supported?
            (reduce-kv
              (fn [_ attr value]
                (if (identical? :db/id attr)
                  true
                  (if (and (keyword? attr) (pos? (count (name attr))))
                    (let [^objects info (blind-attr-info!
                                           infos (:schema ctx)
                                           (:db/attrTuples (:rschema ctx))
                                           (:allow-unique? state) attr)]
                      (if (and (some? (aget info 1))
                               (compile-blind-attribute! ctx attrs info attr value state))
                        true
                        (reduced false)))
                    (reduced false))))
              true entity)]
        (when supported? (BlindEntity. tempid attrs))))))

(defn- compile-blind-region*
  [ctx forms {:keys [allow-small-unique? check-unique-avs?]
              :or {check-unique-avs? true}} infos coerce?]
   (when (and (identical? clojure.lang.PersistentVector (class forms))
              (nil? (meta forms)) (pos? (count forms)))
     (let [entities (FastList. (count forms))
           infos (or infos (HashMap.)) tempids (HashSet.)
           auto-time? (boolean (:auto-entity-time? (:opts ctx)))
           state {:allow-unique? (or allow-small-unique? (>= (count forms) 2))
                  :coerce? coerce?
                  :unique-avs (volatile! nil)
                  :seen-avs (when check-unique-avs? (volatile! nil))
                  :set-capacity (Integer/valueOf (int (max 1 (inc (quot (* 4 (count forms)) 3)))))
                  :has-many? (volatile! false)
                  :fuse-unique-inserts? (volatile! true)}]
       (loop [index 0]
         (if (< index (count forms))
           (let [form (nth forms index)]
             (when (and (plain-map? form) (nil? (meta form)))
               (when-let [^BlindEntity entity (compile-blind-entity ctx infos tempids state form)]
                 ;; Empty maps still participate in explicit-tempid detection.
                 (when (or auto-time? (not (.isEmpty ^FastList (.-attrs entity))))
                   (.add entities entity))
                 (recur (unchecked-inc index)))))
           (let [^FastList avs @(:unique-avs state)
                 has-unique? (boolean (and avs (not (.isEmpty avs))))
                 has-many? @(:has-many? state)
                 fuse? (and has-unique? @(:fuse-unique-inserts? state))
                 independent-identities?
                 (boolean
                   (and has-unique? (not auto-time?) (not has-many?)
                        (= (.size entities) (.size avs))
                        (loop [offset 0]
                          (if (< offset (.size entities))
                            (when (blind-identity-upsert-av (:schema ctx) (.get entities offset))
                              (recur (unchecked-inc offset)))
                            true))))
                 ;; Each proven entity contributes exactly one AV in entity
                 ;; order. Reuse that buffer as its writer binding operand.
                 identity-av (when (and independent-identities? (= 1 (.size entities)))
                               (.get avs 0))]
             (when (and (not (.isEmpty entities))
                        (or check-unique-avs? fuse? (not has-unique?)
                            (= (.size avs) (.size (HashSet. ^java.util.Collection avs)))))
               (assoc (plan/->TxPlan ctx :blind entities nil)
                      :auto-entity-time? auto-time?
                      :has-unique? has-unique? :has-many? has-many?
                      :fuse-unique-inserts? fuse? :unique-avs avs
                      :independent-identity-upserts? independent-identities?
                      :identity-upsert-av identity-av
                      :identity-upsert-read
                      (when identity-av
                        (let [^BlindEntity entity (.get entities 0)
                              ^FastList attrs (.-attrs entity)
                              identity-attr (first identity-av)]
                          (prepare-scalar-entity-read
                            (:schema ctx)
                            (for [offset (range 0 (.size attrs) 2)
                                  :let [attr (.get attrs offset)]
                                  :when (not= attr identity-attr)] attr))))))))))))

(defn compile-blind-region
  "Compile canonical or safely coercible new maps, materialized many values
  and normalized uniqueness proofs. IDs, ownership and timestamps remain writer
  work. Failed corrections and effectful inputs retain their existing route."
  ([ctx forms] (compile-blind-region ctx forms {}))
  ([ctx forms options]
   (or (compile-blind-region* ctx forms options nil false)
       (when-let [infos (coercible-blind-infos ctx forms (:allow-small-unique? options))]
         (compile-blind-region* ctx forms options infos true)))))

(defn- mixed-scalar-ready? [validate? vt value coerce?]
  (and (inert-scalar-value? value)
       (or (canonical-scalar-value? validate? vt value)
           (and coerce?
                (or (not (instance? java.util.Date value))
                    (identical? vt :db.type/instant))
                (pure-scalar-coercion? vt value)))))

(defn- mixed-value-ready? [validate? props ^HashSet tempids value coerce?]
  (if (identical? :db.type/ref (:db/valueType props))
    (or (and (instance? Long value) (not (neg? (long value))))
        (and (literal-tempid? value) (.contains tempids value))
        (and coerce? (ordinary-nonnegative-id? value)))
    (mixed-scalar-ready? validate? (:db/valueType props) value coerce?)))

(defn- correct-mixed-value
  [validate? vt value]
  (if (or (and (identical? vt :db.type/ref)
               ;; Self/back tempids stay symbolic until writer binding. Long
               ;; concrete references, including zero, already need no coercion.
               (or (instance? Long value) (literal-tempid? value)))
          (canonical-scalar-value? validate? vt value))
    value
    (try (prepare/correct-scalar-value validate? vt value)
         (catch Exception _ nil))))

(defn- correct-mixed-entries!
  "Normalize only the fully proven request-owned operand stream. Nothing has
  escaped to a writer; correction failure discards it and retains original input."
  [validate? ^HashMap infos ^ArrayList entries]
  (loop [index 0]
    (if (< index (.size entries))
      (let [entry (.get entries index)]
        (when
          (if (instance? MixedInsert entry)
            (let [^objects attrs (.-attrs ^MixedInsert entry)]
              (loop [offset 0]
                (if (< offset (alength attrs))
                  (let [^MixedAttrInfo info (aget attrs (+ offset 2))]
                    (when-some [value (correct-mixed-value
                                       validate? (:db/valueType (.-props info))
                                       (aget attrs (inc offset)))]
                      (aset attrs (inc offset) value)
                      (recur (+ offset 3))))
                  true)))
            (let [^MixedUpdate entry entry
                  ^MixedAttrInfo info (.get infos (.-attr entry))]
              (when-some [value (correct-mixed-value
                                 validate? (:db/valueType (.-props info)) (.-value entry))]
                (when-not (identical? value (.-value entry))
                  (.set entries index (MixedUpdate. (.-e entry) (.-attr entry) value)))
                true)))
          (recur (unchecked-inc index))))
      true)))

(defn mixed-insert-operands
  "Assemble symbolic insert attributes from one plain map and captured metadata.
  Values stay untouched: this grants no value/ownership proof, performs no
  correction and binds no persistent IDs. Runtime adapters may normalize these
  operands in order; compilation must additionally prove value eligibility."
  [ctx ^HashMap infos entity]
  (when (and (plain-map? entity) (nil? (meta entity))
             (not (:auto-entity-time? (:opts ctx))))
    (let [explicit? (contains? entity :db/id)
          tempid (:db/id entity)
          size (- (count entity) (if explicit? 1 0))]
      (when (and (pos? size) (or (not explicit?) (literal-tempid? tempid)))
        (let [attrs (object-array (* 3 size))
              end (reduce-kv
                    (fn [offset a value]
                      (if (identical? :db/id a)
                        offset
                        (if (and (keyword? a) (pos? (count (name a))))
                          (let [^MixedAttrInfo info
                                (ordinary-attr-info! infos (:schema ctx)
                                  (:db/attrTuples (:rschema ctx)) a)]
                            (if (.-insert? info)
                              (do (aset attrs offset a)
                                  (aset attrs (inc (long offset)) value)
                                  (aset attrs (+ (long offset) 2) info)
                                  (+ (long offset) 3))
                              (reduced nil)))
                          (reduced nil))))
                    0 entity)]
          (when end (MixedInsert. tempid attrs)))))))

(defn- compile-mixed-insert
  [ctx ^HashMap infos ^HashSet tempids entity coerce?]
  (let [explicit? (contains? entity :db/id)
        tempid (:db/id entity)]
    (when (or (not explicit?)
              (and (literal-tempid? tempid) (.add tempids tempid)))
      (when-let [^MixedInsert entry (mixed-insert-operands ctx infos entity)]
        (let [^objects attrs (.-attrs entry)]
          (loop [offset 0]
            (if (< offset (alength attrs))
              (when (mixed-value-ready?
                      (:validate-data? (:opts ctx))
                      (.-props ^MixedAttrInfo (aget attrs (+ offset 2)))
                      tempids (aget attrs (inc offset)) coerce?)
                (recur (+ offset 3)))
              entry)))))))

(defn mixed-update-operand
  "Assemble one raw scalar replacement over captured metadata. The Long ID
  stays unbound and values remain opaque; this proves neither writer bounds,
  E/A distinctness nor value readiness. Scratch remains request-owned."
  [ctx ^HashMap infos e attr value]
  (when (and (instance? Long e) (pos? (long e))
             (keyword? attr) (pos? (count (name attr))) (some? value)
             (not (:auto-entity-time? (:opts ctx))))
    (let [^MixedAttrInfo info (ordinary-attr-info!
                              infos (:schema ctx) (:db/attrTuples (:rschema ctx)) attr)]
      (when (.-update? info)
        (MixedUpdate. e attr value)))))

(defn- append-mixed-update!
  [ctx ^HashMap infos ^HashSet updates ^ArrayList entries e attr value coerce?]
  (when-let [entry (mixed-update-operand ctx infos e attr value)]
    (let [^MixedAttrInfo info (.get infos attr)]
      (when (and (mixed-scalar-ready? (:validate-data? (:opts ctx))
                   (:db/valueType (.-props info)) value coerce?)
                 (distinct-mixed-update? updates e attr))
        (.add entries entry)
        info))))

(defn- compile-mixed-region*
  [ctx forms coerce?]
  (when (and (identical? clojure.lang.PersistentVector (class forms))
             (nil? (meta forms)) (> (count forms) 1)
             (not (:auto-entity-time? (:opts ctx))))
    (let [validate? (:validate-data? (:opts ctx))
          infos (HashMap.) tempids (HashSet.) updates (HashSet.)
          entries (ArrayList.)]
      (loop [index 0 inserts? false required-eid 0 typed? true]
        (if (< index (count forms))
          (let [form (nth forms index)]
            (cond
              (and (plain-map? form) (nil? (meta form)))
              (if (positive-literal-id? (:db/id form))
                (let [e (long (:db/id form))
                      typed (when (> (count form) 1)
                              (reduce-kv
                                (fn [typed attr value]
                                  (if (identical? :db/id attr)
                                    typed
                                    (if-let [^MixedAttrInfo info
                                             (append-mixed-update!
                                               ctx infos updates entries
                                               e attr value coerce?)]
                                      (and typed (.-typed? info))
                                      (reduced nil))))
                                typed? form))]
                  (when (some? typed)
                    (recur (unchecked-inc index) inserts? (max required-eid e) typed)))
                (when-let [entry (compile-mixed-insert ctx infos tempids form coerce?)]
                  (.add entries entry)
                  (recur (unchecked-inc index) true required-eid typed?)))

              (and (identical? clojure.lang.PersistentVector (class form))
                   (nil? (meta form)) (= 4 (count form)) (identical? :db/add (nth form 0)))
              (let [[_ e a v] form]
                (when (positive-literal-id? e)
                  (when-let [^MixedAttrInfo info
                             (append-mixed-update!
                               ctx infos updates entries (long e) a v coerce?)]
                    (recur (unchecked-inc index) inserts? (max required-eid (long e))
                           (and typed? (.-typed? info))))))
              :else nil))
          (when (and inserts? (pos? (.size updates))
                     ;; Complete all shape, metadata, tempid and E/A proofs
                     ;; before invoking even a store-free conversion.
                     (or (not coerce?) (correct-mixed-entries! validate? infos entries)))
            (plan/->TxPlan ctx :mixed
                           (MixedOperands. entries infos required-eid
                                           (and typed? (> (.size updates) 1)))
                           nil)))))))

(defn compile-mixed-region
  "Compile new maps and distinct scalar replacements from maps or add vectors.
  Values may use safe built-in coercions; existing literal IDs normalize to Long.
  Writer bounds, uniqueness and old values remain runtime work."
  [ctx forms]
  (or (compile-mixed-region* ctx forms false)
      (compile-mixed-region* ctx forms true)))

(defn- literal-scalar-alias?
  [value]
  (or (keyword? value)
      (and (identical? clojure.lang.PersistentVector (class value))
           (nil? (meta value)) (= 2 (count value))
           (keyword? (nth value 0)) (inert-scalar-value? (nth value 1)))))

(defn literal-request-shape
  "Select one materialized request family without schema reads or host methods."
  [forms]
  (when (and (identical? clojure.lang.PersistentVector (class forms))
             (nil? (meta forms)) (pos? (count forms)))
    (loop [index 0 kinds 0]
      (if (< index (count forms))
        (let [form (nth forms index)
              kind (cond
                     (and (plain-map? form) (nil? (meta form)))
                     (let [explicit? (contains? form :db/id) eid (:db/id form)]
                       (cond
                         (or (not explicit?) (literal-tempid? eid)) 2
                         (and (> (count form) 1) (positive-literal-id? eid)) 1
                         :else nil))
                     (and (identical? clojure.lang.PersistentVector (class form))
                          (nil? (meta form)) (= 4 (count form))
                          (identical? :db/add (nth form 0)))
                     (let [e (nth form 1)]
                       (cond (positive-literal-id? e) 4
                             (literal-scalar-alias? e) 8)))]
          (when kind
            (let [kinds (bit-or kinds (long kind))]
              ;; Symbolic existing IDs still need ordered mixed resolution.
              (when-not (and (pos? (bit-and kinds 2))
                             (pos? (bit-and kinds 8)))
                (recur (unchecked-inc index) kinds)))))
        (cond
          (pos? (bit-and kinds 8)) :scalar-aliases
          (pos? (bit-and kinds 2)) (if (pos? (bit-and kinds 5)) :mixed :blind)
          (pos? (bit-and kinds 1)) :scalar
          ;; Preserve the small repeated-E/A decline before context allocation.
          (= 2 (count forms)) (literal-scalar-shape forms)
          :else :scalar-operations)))))

(defn- canonical-scalar-alias?
  [ctx e]
  (or (keyword? e)
      (let [props (get (:schema ctx) (nth e 0))]
        (and (#{:db.unique/identity :db.unique/value} (:db/unique props))
             (contains? ordinary-unique-types (:db/valueType props))
             (canonical-scalar-value? (:validate-data? (:opts ctx))
                                      (:db/valueType props) (nth e 1))))))

(defn- compile-scalar-aliases
  "Retain canonical symbolic entity operands for writer-time binding. Raw
  distinctness is provisional: the writer must reject aliases of one E/A."
  [ctx forms]
  (when-not (:auto-entity-time? (:opts ctx))
    (let [store-schema (:schema ctx)
          tuple-source-attrs (:db/attrTuples (:rschema ctx))
          candidates (FastList.)
          seen (HashSet.)
          append! (fn [e attr value]
                    (when-let [props (literal-scalar-props
                                      store-schema tuple-source-attrs attr value true)]
                      ;; Noncanonical values retain the existing caller's
                      ;; correction/rejection order, including missing aliases.
                      (when (canonical-scalar-value? (:validate-data? (:opts ctx))
                                                     (:db/valueType props) value)
                        (append-scalar-candidate! candidates seen e attr value
                                                  (:db/valueType props)))))]
      (when (reduce
              (fn [_ form]
                (let [raw-e (if (map? form) (:db/id form) (nth form 1))
                      e (if (positive-literal-id? raw-e) (long raw-e) raw-e)]
                  (if (and (or (integer? e) (canonical-scalar-alias? ctx e))
                           (if (map? form)
                             (reduce-kv (fn [_ attr value]
                                          (if (or (identical? :db/id attr)
                                                  (append! e attr value))
                                            true (reduced false)))
                                        true form)
                             (append! e (nth form 2) (nth form 3))))
                    true (reduced false))))
              true forms)
        (plan/->TxPlan ctx :scalar-aliases
                       (canonical-scalar-candidates candidates) nil)))))

(defn- compile-coerced-scalar-region
  "Normalize only after complete inert eligibility and raw E/A distinctness.
  Correction failures decline without surfacing an error or replacing input."
  [ctx forms aliases?]
  (when-not (:auto-entity-time? (:opts ctx))
    (let [store-schema (:schema ctx)
          tuple-source-attrs (:db/attrTuples (:rschema ctx))
          validate? (:validate-data? (:opts ctx))
          candidates (FastList.)
          seen (HashSet.)
          append! (fn [e attr value]
                    (when-let [props (literal-scalar-props
                                      store-schema tuple-source-attrs attr value true)]
                      (let [vt (:db/valueType props)]
                        (when (or (canonical-scalar-value? validate? vt value)
                                  (pure-scalar-coercion? vt value))
                          (append-scalar-candidate! candidates seen e attr value vt)))))]
      (when (reduce
              (fn [_ form]
                (let [raw-e (if (map? form) (:db/id form) (nth form 1))
                      e (if (positive-literal-id? raw-e) (long raw-e) raw-e)]
                  (if (and (or (integer? e) (canonical-scalar-alias? ctx e))
                           (if (map? form)
                             (reduce-kv (fn [_ attr value]
                                          (if (or (identical? :db/id attr)
                                                  (append! e attr value))
                                            true (reduced false)))
                                        true form)
                             (append! e (nth form 2) (nth form 3))))
                    true (reduced false))))
              true forms)
        (when-let [entries
                   (loop [i 0 entries (transient [])]
                     (if (< i (.size candidates))
                       (let [raw (.get candidates (+ i 2))
                             vt (.get candidates (+ i 3))
                             value (if (canonical-scalar-value? validate? vt raw)
                                     raw
                                     (try (prepare/correct-scalar-value validate? vt raw)
                                          (catch Exception _ nil)))]
                         (when (some? value)
                           (recur (+ i 4)
                                  (conj! entries [(.get candidates i)
                                                  (.get candidates (inc i)) value]))))
                       (persistent! entries)))]
          (plan/->TxPlan ctx (if aliases? :scalar-aliases :scalar) entries
                         (when-not aliases?
                           (scalar-update-entity-reads store-schema entries))))))))

(defn compile-tx
  "Select one scalar, blind or mixed rule from a pure structural decision.
  Known-family adapters may carry their shape proof; unsupported input is untouched."
  ([ctx forms] (compile-tx ctx forms {}))
  ([ctx forms {:keys [literal-shape request-shape] :as options}]
   (let [shape (or literal-shape request-shape (literal-request-shape forms))]
     (case shape
       ;; The protected Stage 1 adapter carries :literal-shape. Successful
       ;; Stage 2 preparation additionally accepts canonical concrete references.
       (:scalar :scalar-operations)
       (or (compile-scalar-region ctx forms shape (nil? literal-shape))
           (when (nil? literal-shape) (compile-coerced-scalar-region ctx forms false)))
       :scalar-aliases
       (or (compile-scalar-aliases ctx forms)
           (compile-coerced-scalar-region ctx forms true))
       :blind (compile-blind-region ctx forms options)
       :mixed (compile-mixed-region ctx forms)
       nil))))
