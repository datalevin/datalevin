;; Copyright (c) Huahai Yang. All rights reserved.
;; Distributed under the Eclipse Public License 2.0.
(ns ^:no-doc datalevin.db.tx.compile
  "Transaction analysis and owned operand lowering. compile-tx is a pure probe:
  no database, writer, entity allocation, pending view or runtime callbacks."
  (:require [datalevin.bits :as b]
            [datalevin.custom-datalog :as cd]
            [datalevin.datom :as d]
            [datalevin.db.tx.plan :as plan]
            [datalevin.index :as idx]
            [datalevin.prepare :as prepare])
  (:import [datalevin.datom Datom]
           [datalevin.db.tx.plan AttrInfo BlindEntity BlindSource BlindTemplate
            MixedInsert MixedOperands MixedUpdate ScalarEntityRead]
           [datalevin.utl QueryKey$EquivSet QueryKey$KeySet]
           [clojure.lang MapEntry]
           [java.util ArrayList Collection HashMap HashSet Set]
           [org.eclipse.collections.api.block HashingStrategy]
           [org.eclipse.collections.impl.set.strategy.mutable UnifiedSetWithHashingStrategy]
           [org.eclipse.collections.impl.set.mutable.primitive LongHashSet]
           [org.eclipse.collections.impl.list.mutable FastList]))

(def ^:private ordinary-scalar-types
  #{nil :db.type/long :db.type/string :db.type/keyword :db.type/symbol
    :db.type/float :db.type/double :db.type/boolean :db.type/instant
    :db.type/uuid :db.type/bytes :db.type/bigint :db.type/bigdec})

(def operand-decline
  "Private binding result, distinct from every possible user value."
  (Object.))

(def ^:private ordinary-unique-types
  #{:db.type/boolean :db.type/instant :db.type/keyword :db.type/long
    :db.type/string :db.type/symbol :db.type/uuid :db.type/ref})

(def ^:private ordinary-tuple-types
  (conj (disj ordinary-scalar-types nil :db.type/bytes) :db.type/ref))

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

(declare attribute-info!)

(defn- make-attribute-info
  [infos schema sources attr props]
  (let [vt (:db/valueType props)
        ordinary? (and (ordinary-attr? attr props) (ordinary-unique? schema props))
        source-tuples (get sources attr)
        ;; Composite tuples cannot themselves be tuple sources.
        ordinary-tuples? (and ordinary?
                              (let [infos (when (seq source-tuples) (or infos (HashMap.)))]
                                (reduce-kv
                                  (fn [_ tuple _]
                                    (let [^AttrInfo info (attribute-info! infos schema sources tuple)]
                                      (if (.-ordinary? info) true (reduced false))))
                                  true source-tuples)))]
    (AttrInfo.
      props vt (idx/value-type props) source-tuples (boolean ordinary?)
      (boolean (and ordinary? (not (:db/tupleAttrs props)) ordinary-tuples?))
      (boolean (and ordinary? (contains? ordinary-scalar-types vt)
                    (nil? (:db/unique props)) (not (:db/tupleAttrs props)) (not source-tuples)))
      (some? vt)
      (when (blind-attr-supported? attr props sources true)
        (bit-or (if (:db/unique props) 1 0)
                (if (identical? :db.cardinality/many (:db/cardinality props)) 2 0)))
      (not (or (:db/fulltext props) (:db/embedding props) (:db/noindex props)
               (map? vt) (#{:db.type/vec :db.type/idoc :data} vt)))
      (boolean (#{:db.type/string :db.type/keyword :db.type/symbol} vt))
      (not (or (cd/custom-type? vt) (map? vt)
               (identical? vt :db.type/idoc) (identical? vt :db.type/tuple)))
      nil)))

(defn attribute-info!
  "Bind an attribute once in the plan's owned metadata table. No value work."
  [^HashMap infos schema sources attr]
  (or (.get infos attr)
      (let [props (schema attr)
            info (make-attribute-info infos schema sources attr props)]
        (when (:db/unique props) (.put infos ordinary-unique-attr-marker true))
        (.put infos attr info)
        info)))

(defn ordinary-unique-attrs-present?
  "Whether these request-owned bindings contain a unique attribute."
  [^HashMap infos]
  (.containsKey infos ordinary-unique-attr-marker))

(defn blind-attr-info!
  "Apply the request's identity policy to one shared logical attribute binding."
  [infos schema tuple-source-attrs allow-unique? attr]
  (let [^AttrInfo info (attribute-info! infos schema tuple-source-attrs attr)]
    (when (and (some? (.-blind-kinds info))
               (or allow-unique?
                   (not (identical? :db.unique/identity (:db/unique (.-props info))))))
      info)))

(defn blind-value-ready?
  "Recognize existing no-correction cases without calling a value conversion."
  [^AttrInfo info value]
  (case (.-value-type info)
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

(defn blind-unique-avs-distinct?
  "Prove request-local uniqueness independently of native insert enforcement.
  Match blind-value-set's instant and numeric equivalence without changing AVs."
  [^FastList avs]
  (or (nil? avs) (<= (.size avs) 1)
      (let [seen (QueryKey$EquivSet.)]
        (loop [index 0]
          (or (= index (.size avs))
              (and (.add seen (plan/unique-av-key (.get avs index)))
                   (recur (unchecked-inc index))))))))

(defn- collect-blind-unique-value!
  [^FastList unique-avs seen-avs set-capacity fuse-unique-inserts? ^AttrInfo info attr value]
  (let [unique? (some? (:db/unique (.-props info)))
        inline? (or (not unique?) (blind-unique-inline-value? (.-textual? info) value))
        av (when unique? (if seen-avs [attr value] (MapEntry/create attr value)))]
    (if (and unique?
             (or (not inline?)
                 (and seen-avs
                      (let [^HashSet seen (or @seen-avs
                                             (vreset! seen-avs (HashSet. (int set-capacity))))]
                        (not (.add seen (plan/unique-av-key av)))))))
      false
      (do
        (when-not inline? (vreset! fuse-unique-inserts? false))
        (when av (.add unique-avs av))
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

(defn scalar-attribute-info
  "Return the shared binding after scalar eligibility, without entity resolution."
  ([store-schema tuple-source-attrs attr value]
   (scalar-attribute-info store-schema tuple-source-attrs attr value false nil))
  ([store-schema tuple-source-attrs attr value literal?]
   (scalar-attribute-info store-schema tuple-source-attrs attr value literal? nil))
  ([store-schema tuple-source-attrs attr value literal? infos]
   (let [^AttrInfo info (if infos
                          (attribute-info! infos store-schema tuple-source-attrs attr)
                          (make-attribute-info nil store-schema tuple-source-attrs attr (store-schema attr)))
         props (.-props info)
         vt (.-value-type info)]
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
                             (not (or (and (:db/fulltext props)
                                           ;; Stage 2 retains ordinary fulltext
                                           ;; strings on the shared scalar loop.
                                           ;; Stage 1 keeps its original scope.
                                           (not (and (identical? literal? :references)
                                                     (identical? vt :db.type/string))))
                                      (:db/vector props)
                                      (:db/embedding props))))))
                ;; Legacy reference operands may be host numbers. Preserve
                ;; exclusion checks before any conversion can invoke user code.
                (or (identical? vt :db.type/long) (nil? vt)
                    (contains? scalar-update-value-types vt)
                    (and (or (not literal?) (identical? literal? :references))
                         (identical? vt :db.type/ref)
                         (plan/resolved-positive-ref? value))))
       info))))

(def ^:private scalar-aliases-marker (Object.))

(defn- distinct-scalar-entity!
  "Prove E/A distinctness in compiler-owned scratch. Numeric IDs use primitive
  sets; symbolic operands retain their original pair hashing. Caller binding
  keeps its own existing HashSet proof."
  [^HashMap seen e attr]
  (if (instance? Long e)
    (let [owners (.get seen attr)]
      (cond
        (nil? owners) (do (.put seen attr e) true)
        (instance? LongHashSet owners) (.add ^LongHashSet owners (long e))
        (== (long owners) (long e)) false
        :else (do (.put seen attr (doto (LongHashSet.)
                                   (.add (long owners)) (.add (long e))))
                  true)))
    (let [^HashSet aliases (or (.get seen scalar-aliases-marker)
                               (let [aliases (HashSet.)]
                                 (.put seen scalar-aliases-marker aliases) aliases))]
      (.add aliases [e attr]))))

(defn- scalar-distinct-count
  "Count a proven raw prefix only when later operands require materialization."
  [seen]
  (cond
    (nil? seen) 0
    (instance? HashMap seen)
    (reduce (fn [n owners]
              (+ (long n) (if (instance? LongHashSet owners)
                            (.size ^LongHashSet owners)
                            (if (instance? HashSet owners) (.size ^HashSet owners) 1))))
            0 (.values ^HashMap seen))
    :else (.size ^HashSet seen)))

(defn append-scalar-candidate!
  "Append one raw operand after eligibility and optional distinctness checks."
  [^FastList candidates seen e attr value vt]
  (when (or (nil? seen)
            (if (instance? HashMap seen) (distinct-scalar-entity! seen e attr)
                (.add ^HashSet seen [e attr])))
    (when candidates
      (.add candidates e)
      (.add candidates attr)
      (.add candidates value)
      (.add candidates vt))
    e))

(defn collect-scalar-candidates
  "Compile scalar source structure into owned correction operands. Entity and
  attribute policies are explicit so caller binding preserves its demand/ID
  checkpoints without maintaining a second source walker."
  ([forms map-form? map-entity map-ready? append!]
   (collect-scalar-candidates forms map-form? map-entity map-ready? append! nil))
  ([forms map-form? map-entity map-ready? append! capacity]
   (collect-scalar-candidates forms map-form? map-entity map-ready? append! capacity nil))
  ([forms map-form? map-entity map-ready? append! capacity seen]
   (let [candidates (FastList. (int (or capacity
                                      (* 4 (if (and (counted? forms) (instance? Collection forms))
                                             (.size ^Collection forms) 0)))))
         forms (seq forms)]
     (when forms
       (loop [es forms seen seen]
         (if es
           (let [form (first es)]
             (cond
               (and (map-form? form) (nil? (meta form)) (> (count form) 1))
               (when-let [e (map-entity (:db/id form))]
                 (let [remaining (next es)
                       ;; One map already has distinct attribute keys.
                       seen (or seen (when remaining (HashSet.)))]
                   (when (and (map-ready? e)
                              (reduce-kv
                                (fn [_ attr value]
                                  (if (or (identical? attr :db/id)
                                          (and (keyword? attr) (pos? (count (name attr)))
                                               (not (plan/reverse-ref-attr? attr))
                                               (append! candidates seen e attr value true)))
                                    true (reduced false)))
                                true form))
                     (recur remaining seen))))

               (and (sequential? form) (nil? (meta form)) (= 4 (count form))
                    (identical? :db/add (nth form 0)))
               (let [remaining (next es) [_ e attr value] form]
                 (when-let [eid (append! candidates seen e attr value false)]
                   (recur remaining (or seen (when remaining (doto (HashSet.) (.add [eid attr])))))))

               :else nil))
           candidates))))))

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

(defn blind-identity-entity-read
  "Select the existing single-identity projection from normalized operands and
  captured schema. Header/provenance and current ownership remain caller work."
  [store-schema ^FastList entities identity-av]
  (when identity-av
    (let [^BlindEntity entity (.get entities 0)
          ^FastList attrs (.-attrs entity)
          identity-attr (first identity-av)]
      (prepare-scalar-entity-read
        store-schema
        (for [offset (range 0 (.size attrs) 2)
              :let [attr (.get attrs offset)]
              :when (not= attr identity-attr)] attr)))))

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
                    (if (.-insert? ^AttrInfo (attribute-info! infos schema sources attr))
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
                            (.-update? ^AttrInfo (attribute-info! infos schema sources attr))
                            (distinct-mixed-update? updates e attr)))))))
          ::unsupported-mixed-preflight)))))

(defn- positive-literal-id? [e]
  (if (instance? Long e)
    (pos? (long e))
    (and (contains? inert-id-classes (class e))
         (inert-scalar-value? e)
         (plan/resolved-positive-ref? e))))

(defn ordinary-analysis-enabled?
  "Whether the captured context permits ordinary-addition analysis."
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
  "Attach compiler eligibility flags to demanded input and its resolver-staging
  decision in the dynamic executor."
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

(defn- literal-scalar-info
  [store-schema tuple-source-attrs attr value allow-references? infos]
  ;; Structural analysis proved the ID; inspect only inert values before any
  ;; hashing or correction. Attribute and value eligibility stays compiler-owned.
  (when (and (keyword? attr) (pos? (count (name attr)))
             (not (plan/reverse-ref-attr? attr)) (inert-scalar-value? value))
    (when-let [^AttrInfo info (scalar-attribute-info
                              store-schema tuple-source-attrs attr value
                              (if allow-references? :references true) infos)]
      ;; Date formatting can invoke a user-supplied default TimeZone.
      (when-not (and (instance? java.util.Date value)
                     (some? (.-value-type info))
                     (not (identical? :db.type/instant (.-value-type info))))
        info))))

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

(defn- literal-tempid-key
  "Prove an inert negative integer within the long range. Canonicalize only
  compiler scratch; operands retain the caller's tempid object."
  [value]
  (cond
    (string? value) value
    (instance? Long value) (when (neg? (long value)) value)
    (and (contains? inert-id-classes (class value)) (inert-scalar-value? value))
    (try
      (let [id (long value)] (when (neg? id) id))
      (catch IllegalArgumentException _ nil))))

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

(defn- canonical-blind-value? [ctx ^AttrInfo info value]
  (and (inert-scalar-value? value)
       (let [vt (.-value-type info)]
         (if (identical? vt :db.type/ref)
           (and (instance? Long value) (pos? (long value)))
           (canonical-scalar-value? (:validate-data? (:opts ctx)) vt value)))))

(defn- literal-many-values? [value]
  (or (identical? clojure.lang.PersistentVector (class value))
      (identical? clojure.lang.PersistentHashSet (class value))))

(defn- blind-value-conversion
  "Nil keeps an inert canonical value; a type requests correction; false declines."
  [ctx ^AttrInfo info value]
  (cond
    (canonical-blind-value? ctx info value) nil
    (and (inert-scalar-value? value)
         (or (not (instance? java.util.Date value))
             (identical? :db.type/instant (.-value-type info)))
         (or (not (identical? :db.type/ref (.-value-type info)))
             (plan/resolved-positive-ref? value))
         (pure-scalar-coercion? (.-value-type info) value)) (.-value-type info)
    :else false))

(def ^:private runtime-blind-value
  "A raw caller-bound operand. Its correction runs only during binding."
  ::runtime-blind-value)

(defn- blind-multiple-values?
  "Distinguish additive many values from a single identity lookup operand."
  [many? identity-attrs value]
  (and many? (coll? value) (not (map? value))
       (not (and (= 2 (count value)) (contains? identity-attrs (first value))))))

(defn- append-blind-source-value!
  [^FastList attrs attr value conversion]
  (.add attrs attr)
  (.add attrs value)
  (.add attrs conversion)
  true)

(defn- compile-blind-attribute!
  [ctx ^FastList attrs ^AttrInfo info attr raw caller-bound? literal-proof]
  (let [many? (identical? :db.cardinality/many (:db/cardinality (.-props info)))
        ref? (identical? :db.type/ref (.-value-type info))
        identity-attrs (:db.unique/identity (:rschema ctx))]
    (if (or caller-bound? (and literal-proof (not @literal-proof) (not ref?)))
      ;; Reference eligibility precedes all correction; other nested collections
      ;; stay undemanded until binding, in their original correction order.
      (when (or (not ref?)
                (if (blind-multiple-values? many? identity-attrs raw)
                  (every? plan/resolved-positive-ref? raw)
                  (plan/resolved-positive-ref? raw)))
        (append-blind-source-value! attrs attr raw runtime-blind-value))
      (let [start (when literal-proof (.size attrs))
            literal?
            (when-not (and many? (literal-many-values? raw) (= 2 (count raw))
                           (contains? identity-attrs (first raw)))
              (if (and many? (literal-many-values? raw))
                (reduce (fn [_ value]
                          (let [conversion (blind-value-conversion ctx info value)]
                            (if (false? conversion) (reduced false)
                                (append-blind-source-value! attrs attr value conversion))))
                        true raw)
                (let [conversion (blind-value-conversion ctx info raw)]
                  (when-not (false? conversion)
                    (append-blind-source-value! attrs attr raw conversion)))))]
        ;; Runtime reference admission can demand lazy input or host IDs.
        ;; Leave it to the normal fallback after its full ID preflight.
        (if (or literal? (nil? literal-proof) ref?)
          literal?
          (do
            ;; An admitted caller can bind runtime values. Roll back only this
            ;; field's partial many expansion, then retain its undemanded input.
            (.removeRange attrs (int start) (.size attrs))
            (vreset! literal-proof false)
            (compile-blind-attribute! ctx attrs info attr raw true nil)))))))

(defn compile-blind-template
  "Build raw operands and shared metadata once, without correcting values.
  Literal probes prove inert IDs/values. Caller-bound forms already passed ID
  admission; retain collections and host values for the binding checkpoint."
  [ctx forms {:keys [caller-bound? allow-small-unique? literal-proof]}]
  (let [n (if (and (counted? forms) (instance? Collection forms))
            (.size ^Collection forms) 0)
        allow-identity? (or (not caller-bound?) allow-small-unique?
                            (>= (long (bounded-count 2 forms)) 2))
        infos (HashMap.) tempids (when-not caller-bound? (HashSet.))
        identity? (volatile! false) fuse? (volatile! true)
        sources (:db/attrTuples (:rschema ctx))
        entries
        (reduce
          (fn [^FastList entries source]
            (if-not (if caller-bound? (map? source)
                        (and (plain-map? source) (nil? (meta source))))
              (reduced nil)
              (let [tempid (:db/id source)
                    id? (or caller-bound? (not (contains? source :db/id))
                            (when-let [key (literal-tempid-key tempid)] (.add tempids key)))
                    attrs (FastList. (int (* 3 (- (count source)
                                                  (if (and (not caller-bound?)
                                                           (contains? source :db/id)) 1 0)))))]
                (if (and id?
                         (reduce-kv
                           (fn [_ attr raw]
                             (if (identical? attr :db/id)
                               true
                               (if-let [^AttrInfo info
                                        (when (and (keyword? attr) (pos? (count (name attr))))
                                          (blind-attr-info! infos (:schema ctx) sources allow-identity? attr))]
                                 (do
                                   (when-not (.-fuse-blind? info) (vreset! fuse? false))
                                   (when (identical? :db.unique/identity (:db/unique (.-props info)))
                                     (vreset! identity? true))
                                   (if (compile-blind-attribute! ctx attrs info attr raw caller-bound? literal-proof)
                                     true (reduced false)))
                                 (reduced false))))
                           true source))
                  (doto entries (.add (BlindSource. source attrs)))
                  (reduced nil)))))
          (FastList. (int n)) forms)]
    (when entries (BlindTemplate. ctx entries infos @identity? @fuse?))))

(defn- finish-blind-plan
  "Finalize all blind plans from the same normalized operands and AV proof."
  [ctx ^HashMap infos ^FastList entities ^FastList avs has-many? fuse?]
  (let [schema #(.-props ^AttrInfo (.get infos %))
        auto-time? (boolean (:auto-entity-time? (:opts ctx)))
        has-unique? (boolean (and avs (not (.isEmpty avs))))
        identity-av (when (and has-unique? (not auto-time?) (= 1 (.size entities)))
                      (blind-identity-upsert-av schema (.get entities 0)))
        independent-identities?
        (boolean
          (and has-unique? (not auto-time?) (not has-many?)
               (= (.size entities) (.size avs))
               (if (= 1 (.size entities)) (some? identity-av)
                   (every? #(blind-identity-upsert-av schema %) entities))))]
    (when (and (not (.isEmpty entities)) (blind-unique-avs-distinct? avs))
      (assoc (plan/->TxPlan ctx :blind entities nil)
             :auto-entity-time? auto-time? :has-unique? has-unique? :has-many? has-many?
             :fuse-unique-inserts? (and has-unique? fuse?) :unique-avs avs
             :independent-identity-upserts? independent-identities?
             :identity-upsert-av identity-av
             :identity-upsert-read (blind-identity-entity-read schema entities identity-av)))))

(defn- accept-bound-blind-value!
  "Prove one corrected value before emission. True emits, ::duplicate skips a
  nonunique many value, and nil declines. AV collection precedes deduplication."
  [avs seen-avs capacity fuse? ^AttrInfo info attr value ^Set seen]
  (when (and (not (identical? operand-decline value))
             (collect-blind-unique-value! avs seen-avs capacity fuse? info attr value))
    (if (and seen (not (.add seen value)))
      (when-not (:db/unique (.-props info)) ::duplicate)
      true)))

(defn- append-bound-blind-value!
  [^FastList attrs avs seen-avs capacity fuse? info attr value seen]
  (when-let [accepted (accept-bound-blind-value! avs seen-avs capacity fuse? info attr value seen)]
    (when (true? accepted) (.add attrs attr) (.add attrs value))
    accepted))

(defn- lower-blind-template
  "Lower owned triples once using explicit correction and ID-admission hooks.
  Only a fresh private literal template may be consumed: its many values are
  already expanded, IDs proven distinct and triples compactable into pairs.
  Public binding preserves templates and the runtime correction checkpoints."
  [^BlindTemplate template check-unique-avs? valid-id? correct-value consume?]
  (let [ctx (.-ctx template) ^FastList sources (.-entries template)
        ^HashMap infos (.-infos template)
        auto-time? (boolean (:auto-entity-time? (:opts ctx)))
        identity-attrs (:db.unique/identity (:rschema ctx))
        entities (FastList. (.size sources)) tempids (when-not consume? (HashSet.))
        avs (FastList.) seen-avs (when check-unique-avs? (volatile! nil))
        capacity (Integer/valueOf (int (max 1 (inc (quot (* 4 (.size sources)) 3)))))
        fuse? (volatile! (.-fuse? template)) has-many? (volatile! false)]
    (when
      (reduce
        (fn [_ ^BlindSource entry]
          (let [source (.-source entry) tempid (:db/id source)
                ^FastList raw-attrs (.-attrs entry)
                attrs (if consume? raw-attrs
                          (FastList. (int (* 2 (quot (.size raw-attrs) 3)))))]
            (if (and (valid-id? source)
                     (loop [i 0 out 0 previous nil seen nil]
                       (if (< i (.size raw-attrs))
                         (let [attr (.get raw-attrs i) raw (.get raw-attrs (inc i))
                               conversion (.get raw-attrs (+ i 2))
                               ^AttrInfo info (.get infos attr) props (.-props info)
                               many? (identical? :db.cardinality/many (:db/cardinality props))
                               ^Set seen (when many?
                                           (if (identical? previous attr) seen
                                               (blind-value-set (.-value-type info))))
                               multiple? (and (identical? runtime-blind-value conversion)
                                              (blind-multiple-values? many? identity-attrs raw))
                               values (when multiple? (seq raw))]
                           (when (and many? (or (not multiple?) values))
                             (vreset! has-many? true))
                           (if multiple?
                             (when (reduce
                                     (fn [_ raw]
                                       (if (append-bound-blind-value!
                                             attrs avs seen-avs capacity fuse? info attr
                                             (correct-value info attr source conversion raw) seen)
                                         true (reduced false)))
                                     true values)
                               (recur (+ i 3) out attr seen))
                             (let [value (correct-value info attr source conversion raw)]
                               (when-let [accepted (accept-bound-blind-value!
                                                     avs seen-avs capacity fuse? info attr value seen)]
                                 (when (true? accepted)
                                   (if consume?
                                     ;; Both source slots have been read, and
                                     ;; output never reaches an unread triple.
                                     (do (.set attrs (int out) attr)
                                         (.set attrs (int (inc out)) value))
                                     (do (.add attrs attr) (.add attrs value))))
                                 (recur (+ i 3) (if (true? accepted) (+ out 2) out) attr seen)))))
                         (do (when consume?
                               (.removeRange attrs (int out) (.size attrs))
                               ;; Avoid retaining a large raw buffer after
                               ;; substantial many-value deduplication.
                               (.trimToSizeIfGreaterThanPercent attrs 0.5))
                             true)))
                     ;; Runtime duplicate IDs retain the correction checkpoint.
                     (or consume? (nil? tempid) (.add tempids tempid)))
              (do (when (or auto-time? (not (.isEmpty attrs)))
                    (.add entities (BlindEntity. tempid attrs)))
                  true)
              (reduced false))))
        true sources)
      (finish-blind-plan ctx infos entities (when-not (.isEmpty avs) avs) @has-many? @fuse?))))

(defn bind-blind-template
  "Bind reusable raw operands without consuming them. Only compiler-private
  literal scratch can use in-place lowering; caller callbacks retain their order."
  [template check-unique-avs? valid-id? correct-value]
  (lower-blind-template template check-unique-avs? valid-id? correct-value false))

(defn- lower-literal-blind-template
  [^BlindTemplate template check-unique-avs?]
  (let [validate? (:validate-data? (:opts (.-ctx template)))]
    (lower-blind-template
      template check-unique-avs? (constantly true)
      (fn [_ _ _ conversion raw]
        (if conversion
          (if-some [value (try (prepare/correct-scalar-value validate? conversion raw)
                              (catch Exception _ nil))]
            value operand-decline)
          raw)) true)))

(defn- compile-blind-candidate
  [ctx forms {:keys [allow-small-unique? check-unique-avs?]
              :or {check-unique-avs? true}} binding?]
  (when (and (identical? clojure.lang.PersistentVector (class forms))
             (nil? (meta forms)) (pos? (count forms)))
    (let [literal-proof (when binding? (volatile! true))]
      (when-let [^BlindTemplate template
                 (compile-blind-template ctx forms {:literal-proof literal-proof})]
        (cond
          (and (not allow-small-unique?) (= 1 (count forms)) (.-identity? template))
          ::small-unique-decline
          (and literal-proof (not @literal-proof)) template
          :else (lower-literal-blind-template template check-unique-avs?))))))

(defn- compile-blind-plan [ctx forms options]
  (compile-blind-candidate ctx forms options false))

(declare compile-tx)

(defn compile-blind-preparation
  "Select a literal plan or runtime-binding template in one operand walk.
  Call only at an admitted binding checkpoint. Strict literal structure/ID
  eligibility is retained; structural declines keep normal family selection.
  The pure compile-tx entry never returns a runtime-binding template."
  [ctx forms options]
  (let [candidate (compile-blind-candidate ctx forms options true)]
    (cond
      (identical? ::small-unique-decline candidate) nil
      candidate candidate
      ;; This same captured attempt has already declined the blind rule. Keep
      ;; normal mixed/ordered priority without rebuilding its blind operands.
      :else (compile-tx ctx forms (assoc options :skip-blind? true)))))

(defn compile-blind-region
  "Build new-map operands once, then correct, deduplicate and collect owned AVs.
  All eligibility and ID checks finish before correction starts."
  ([ctx forms] (compile-blind-region ctx forms {}))
  ([ctx forms options]
   (let [outcome (compile-blind-plan ctx forms options)]
     (when (instance? datalevin.db.tx.plan.TxPlan outcome) outcome))))

(defn- mixed-scalar-ready? [validate? vt value corrections?]
  (and (inert-scalar-value? value)
       (or (canonical-scalar-value? validate? vt value)
           (and (or (not (instance? java.util.Date value))
                    (identical? vt :db.type/instant))
                (pure-scalar-coercion? vt value)
                (do (vreset! corrections? true) true)))))

(defn- mixed-tuple-ready?
  "Prove a materialized tuple without component coercion. Tuple preparation
  retains its values; reference components must already be concrete IDs."
  [props value]
  (let [types (or (:db/tupleTypes props)
                  (when-let [t (:db/tupleType props)] [t]))]
    (and (identical? clojure.lang.PersistentVector (class value))
         (nil? (meta value)) (vector? types) (seq types)
         (every? ordinary-tuple-types types)
         (every? #(or (nil? %) (inert-scalar-value? %)) value)
         (try
           (and (b/valid-data? value types)
                (every? (fn [[t component]]
                          (and (or (not (identical? t :db.type/ref))
                                   (and (int? component) (not (neg? (long component)))))
                               ;; The shared unique-AV scratch uses Java equality.
                               ;; Preserve tuple components rather than coercing
                               ;; them, and decline representations it cannot prove.
                               (or (not (:db/unique props)) (nil? component)
                                   (case t
                                     (:db.type/long :db.type/ref) (instance? Long component)
                                     :db.type/instant (identical? java.util.Date (class component))
                                     true))))
                        (map vector (if (= 1 (count types)) (repeat (first types)) types)
                             value)))
           (catch Exception _ false)))))

(defn- mixed-value-ready? [validate? props ^HashSet tempids value corrections?]
  (case (:db/valueType props)
    :db.type/tuple (mixed-tuple-ready? props value)
    :db.type/ref
    (or (and (instance? Long value) (not (neg? (long value))))
        (when-let [key (literal-tempid-key value)] (.contains tempids key))
        (and (ordinary-nonnegative-id? value)
             (do (vreset! corrections? true) true)))
    (mixed-scalar-ready? validate? (:db/valueType props) value corrections?)))

(defn- correct-mixed-value
  [validate? props value]
  (let [vt (:db/valueType props)]
    (if (or (identical? vt :db.type/tuple)
            (and (identical? vt :db.type/ref)
                 ;; Self/back tempids stay symbolic until writer binding. Long
                 ;; concrete references, including zero, already need no coercion.
                 (or (instance? Long value) (literal-tempid-key value)))
            (canonical-scalar-value? validate? vt value))
      value
      (try (prepare/correct-scalar-value validate? vt value)
           (catch Exception _ nil)))))

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
                  (let [^AttrInfo info (aget attrs (+ offset 2))]
                    (when-some [value (correct-mixed-value
                                       validate? (.-props info)
                                       (aget attrs (inc offset)))]
                      (aset attrs (inc offset) value)
                      (recur (+ offset 3))))
                  true)))
            (let [^MixedUpdate entry entry
                  ^AttrInfo info (.get infos (.-attr entry))]
              (when-some [value (correct-mixed-value
                                 validate? (.-props info) (.-value entry))]
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
      (when (and (pos? size) (or (not explicit?) (literal-tempid-key tempid)))
        (let [attrs (object-array (* 3 size))
              end (reduce-kv
                    (fn [offset a value]
                      (if (identical? :db/id a)
                        offset
                        (if (and (keyword? a) (pos? (count (name a))))
                          (let [^AttrInfo info
                                (attribute-info! infos (:schema ctx)
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

(defn mixed-insert-values-ready?
  "Prove prepared insert values against captured metadata and the ordered
  self/back-tempid set. Corrections remain deferred until the whole plan passes."
  [ctx ^objects attrs tempids corrections?]
  (loop [offset 0]
    (if (< offset (alength attrs))
      (let [^AttrInfo info (aget attrs (+ offset 2))]
        (when (and (.-insert? info)
                   (mixed-value-ready? (:validate-data? (:opts ctx)) (.-props info)
                                       tempids (aget attrs (inc offset)) corrections?))
          (recur (+ offset 3))))
      true)))

(defn- compile-mixed-insert
  [ctx ^HashMap infos ^HashSet tempids entity corrections?]
  (let [explicit? (contains? entity :db/id)
        tempid (:db/id entity)]
    (when (or (not explicit?)
              (when-let [key (literal-tempid-key tempid)] (.add tempids key)))
      (when-let [^MixedInsert entry (mixed-insert-operands ctx infos entity)]
        (when (mixed-insert-values-ready? ctx (.-attrs entry) tempids corrections?) entry)))))

(defn- independent-tuple-info? [^AttrInfo info]
  (and (.-ordinary? info) (.-insert? info)
       (identical? :db.type/tuple (:db/valueType (.-props info)))
       (nil? (.-tuples info)) (nil? (:db/unique (.-props info)))))

(defn mixed-update-operand
  "Assemble one raw replacement over captured metadata. The Long ID
  stays unbound and values remain opaque; this proves neither writer bounds,
  E/A distinctness nor value readiness. Compilation can additionally admit
  inert positive references and independent explicit tuple attributes. Raw
  legacy callers retain their earlier roles. Scratch remains request-owned."
  ([ctx infos e attr value]
   (mixed-update-operand ctx infos e attr value false))
  ([ctx ^HashMap infos e attr value allow-extended?]
   (when (and (instance? Long e) (pos? (long e))
              (keyword? attr) (pos? (count (name attr))) (some? value)
              (not (:auto-entity-time? (:opts ctx))))
     (let [schema (:schema ctx) sources (:db/attrTuples (:rschema ctx))
           ^AttrInfo info (attribute-info! infos schema sources attr)]
       (when (or (.-update? info)
                 (and allow-extended?
                      (.-ordinary? info)
                      (case (:db/valueType (.-props info))
                        :db.type/ref
                        (and (positive-literal-id? value)
                             (scalar-attribute-info schema sources attr value :references infos))
                        :db.type/tuple
                        (independent-tuple-info? info)
                        false)))
         (MixedUpdate. e attr value))))))

(defn append-mixed-update!
  "Append one proven replacement using the compiler-owned metadata table.
  Both raw compilation and prepared binding share value and distinctness rules."
  [ctx ^HashMap infos ^HashSet updates ^ArrayList entries e attr value corrections?]
  (when-let [entry (mixed-update-operand ctx infos e attr value true)]
    (let [^AttrInfo info (.get infos attr)]
      (when (and (if (identical? :db.type/tuple (:db/valueType (.-props info)))
                   (mixed-tuple-ready? (.-props info) value)
                   (mixed-scalar-ready? (:validate-data? (:opts ctx))
                     (:db/valueType (.-props info)) value corrections?))
                 (distinct-mixed-update? updates e attr))
        (.add entries entry)
        info))))

(defn finish-mixed-plan
  "Finish one owned mixed stream after all eligibility checks. The same
  correction and read/storage decisions serve raw and reusable programs."
  [ctx ^HashMap infos ^HashSet updates ^ArrayList entries corrections? inserts? required-eid typed?]
  (when (and (or inserts?
                 (some (fn [^MixedUpdate entry]
                         (identical? :db.type/tuple (.-value-type ^AttrInfo (.get infos (.-attr entry)))))
                       entries))
             (or (not @corrections?)
                 (correct-mixed-entries! (:validate-data? (:opts ctx)) infos entries)))
    (plan/->TxPlan ctx :mixed
                   (MixedOperands. entries infos required-eid
                                   (and typed? (> (.size updates) 1))
                                   (>= (.size infos) 8)) nil)))

(defn- compile-mixed-region*
  "Build one owned mixed operand stream, then correct it after full eligibility.
  Writer bounds, uniqueness and old values remain runtime work."
  [ctx forms infos]
  (when (and (identical? clojure.lang.PersistentVector (class forms))
             (nil? (meta forms)) (pos? (count forms))
             (not (:auto-entity-time? (:opts ctx))))
    (let [infos (or infos (HashMap.)) tempids (HashSet.) updates (HashSet.)
          entries (ArrayList.) corrections? (volatile! false)]
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
                                    (if-let [^AttrInfo info
                                             (append-mixed-update!
                                               ctx infos updates entries
                                               e attr value corrections?)]
                                      (and typed (.-typed? info))
                                      (reduced nil))))
                                typed? form))]
                  (when (some? typed)
                    (recur (unchecked-inc index) inserts? (max required-eid e) typed)))
                (when-let [entry (compile-mixed-insert ctx infos tempids form corrections?)]
                  (.add entries entry)
                  (recur (unchecked-inc index) true required-eid typed?)))

              (and (identical? clojure.lang.PersistentVector (class form))
                   (nil? (meta form)) (= 4 (count form)) (identical? :db/add (nth form 0)))
              (let [[_ e a v] form]
                (when (positive-literal-id? e)
                  (when-let [^AttrInfo info
                             (append-mixed-update!
                               ctx infos updates entries (long e) a v corrections?)]
                    (recur (unchecked-inc index) inserts? (max required-eid (long e))
                           (and typed? (.-typed? info))))))
              :else nil))
          (finish-mixed-plan ctx infos updates entries corrections? inserts? required-eid typed?))))))

(defn compile-mixed-region
  "Build a mixed plan with fresh compiler-owned attribute bindings."
  [ctx forms]
  (compile-mixed-region* ctx forms nil))

(defn- needs-ordinary-insert-plan?
  "Select the shared ordinary loop only for reference/tuple work that
  the blind rule cannot handle. Full eligibility remains in the region compiler;
  ordinary blind-policy declines do not acquire a different proof."
  [ctx forms]
  (when (and (identical? clojure.lang.PersistentVector (class forms))
             (nil? (meta forms)))
    (let [schema (:schema ctx) sources (:db/attrTuples (:rschema ctx))]
      (boolean
        (some (fn [form]
                (when (and (plain-map? form) (nil? (meta form)))
                  (reduce-kv
                    (fn [_ attr value]
                      (if (and (keyword? attr)
                               (or (contains? sources attr)
                                   (identical? :db.type/tuple (:db/valueType (schema attr)))
                                   (and (identical? :db.type/ref (:db/valueType (schema attr)))
                                        (literal-tempid-key value))))
                        (reduced true)
                        false))
                    false form))) forms)))))

(defn- literal-scalar-alias?
  [value]
  (or (keyword? value)
      (and (identical? clojure.lang.PersistentVector (class value))
           (nil? (meta value)) (= 2 (count value))
           (keyword? (nth value 0)) (inert-scalar-value? (nth value 1)))))

(defn- application-add-metadata?
  "Prove keyword-keyed application annotations without inspecting values.
  Custom maps and reserved engine/control namespaces retain their old route."
  [metadata]
  (and (plain-map? metadata)
       (reduce-kv
         (fn [_ key _value]
           (if (and (keyword? key)
                    (let [n (namespace key)]
                      (not (and n (or (= n "db") (= n "db.fn") (= n "idoc")
                                      (= n "datalevin") (.startsWith ^String n "datalevin."))))))
             true (reduced false)))
         true metadata)))

(defn independent-request-shape
  "Select one materialized request family without schema reads or host methods."
  [forms]
  (when (and (identical? clojure.lang.PersistentVector (class forms))
             (nil? (meta forms)) (pos? (count forms)))
    (loop [index 0 kinds 0]
      (if (< index (count forms))
        (let [form (nth forms index)
              kind (cond
                     (identical? Datom (class form))
                     (when (and (pos? (.-e ^Datom form)) (d/datom-added form)) 20)

                     (plain-map? form)
                     (let [explicit? (contains? form :db/id) eid (:db/id form)
                           kind (cond
                                  (or (not explicit?) (literal-tempid-key eid)) 2
                                  (and (> (count form) 1) (positive-literal-id? eid)) 1
                                  ;; Map IDs accept lookup refs, but unlike operation
                                  ;; vectors their syntax excludes bare ident keywords.
                                  (and (> (count form) 1) (vector? eid) (literal-scalar-alias? eid)) 8)]
                       (when kind
                         (cond
                           ;; New-map binding ignores annotations and retains its
                           ;; implicit-ID allocation checkpoint when compilation declines.
                           (= kind 2) kind
                           (nil? (meta form)) kind
                           (application-add-metadata? (meta form)) (bit-or kind 16))))
                     (and (identical? clojure.lang.PersistentVector (class form))
                          (or (nil? (meta form)) (application-add-metadata? (meta form)))
                          (<= 4 (count form) 5)
                          (identical? :db/add (nth form 0)))
                     (let [e (nth form 1)
                           kind (cond (positive-literal-id? e) 4
                                      (literal-scalar-alias? e) 8)]
                       (when kind
                         (if (and (= 4 (count form)) (nil? (meta form))) kind
                             (when (or (= 4 (count form))
                                       (nil? (nth form 4)) (positive-literal-id? (nth form 4)))
                               (bit-or kind 16))))))]
          (when kind
            (let [kinds (bit-or kinds (long kind))]
              ;; New entities mixed with aliases or supplied transaction IDs
              ;; still require ordered resolution and allocation.
              (when-not (and (pos? (bit-and kinds 2))
                             (pos? (bit-and kinds 24)))
                (recur (unchecked-inc index) kinds)))))
        (cond
          (pos? (bit-and kinds 16)) :explicit-additions
          (pos? (bit-and kinds 8)) :scalar-aliases
          (pos? (bit-and kinds 2)) (if (pos? (bit-and kinds 5)) :mixed :blind)
          (pos? (bit-and kinds 1)) :scalar
          ;; Preserve the small repeated-E/A decline before context allocation.
          (= 2 (count forms)) (literal-scalar-shape forms)
          :else :scalar-operations)))))

(defn- inert-patch-value? [value]
  (or (nil? value) (inert-scalar-value? value)
      (and (plain-map? value)
           (reduce-kv (fn [_ key value]
                        (if (and (inert-patch-value? key) (inert-patch-value? value))
                          true (reduced false))) true value))
      (and (or (identical? clojure.lang.PersistentVector (class value))
               (identical? clojure.lang.APersistentVector$SubVector (class value)))
           (every? inert-patch-value? value))))

(defn patch-attribute-info
  "Select the ordinary patch worker. Caller binding and demanded execution use
  the same schema/options rule; document reads and patch operations stay deferred."
  [ctx attr operations]
  (let [^AttrInfo info (make-attribute-info
                        nil (:schema ctx) (:db/attrTuples (:rschema ctx))
                        attr (get (:schema ctx) attr))
        props (.-props info)]
    (when (and (not (:auto-entity-time? (:opts ctx)))
               (keyword? attr) (identical? :db.type/idoc (.-value-type info))
               (not (identical? :db.cardinality/many (:db/cardinality props)))
               (nil? (:db/unique props)) (not (:db.attr/preds props))
               ;; Lazy/host operands can change schema or options while being
               ;; demanded. Their current constraints belong to ordered execution.
               (or (nil? operations)
                   (and (vector? operations) (inert-patch-value? operations))))
      info)))

(defn compile-patch-region
  "Own inert single-document patch operands. The binding adapter retains
  opaque sequence/numeric conversion at its established caller checkpoint."
  [ctx source e attr operations]
  (when (and (instance? Long e) (pos? (long e))
             (patch-attribute-info ctx attr operations))
    (plan/patch-plan ctx source e attr operations)))

(defn- declarative-entity-syntax?
  "Keep host IDs and overflowing numbers outside admission without resolving IDs."
  [value]
  (or (string? value) (keyword? value)
      (and (contains? inert-id-classes (class value))
           (inert-scalar-value? value)
           (try (<= Long/MIN_VALUE (long value) Long/MAX_VALUE)
                (catch IllegalArgumentException _ false)))
      (and (identical? clojure.lang.PersistentVector (class value))
           (nil? (meta value)) (= 2 (count value)) (keyword? (nth value 0))
           (let [key (nth value 1)]
             (or (inert-scalar-value? key)
                 (and (identical? clojure.lang.PersistentVector (class key))
                      (nil? (meta key)) (<= 1 (count key) 8)
                      (every? #(or (nil? %) (inert-scalar-value? %)) key)))))))

(defn- declarative-map-syntax?
  [form]
  (and (plain-map? form)
       (or (not (contains? form :db/id))
           (and (not (keyword? (:db/id form)))
                (declarative-entity-syntax? (:db/id form))))
       (or (nil? (meta form))
           ;; New annotated maps retain the blind normalizer's allocation path.
           (and (application-add-metadata? (meta form))
                (let [id (:db/id form)]
                  (or (positive-literal-id? id)
                      (identical? clojure.lang.PersistentVector (class id))))))))

(defn- literal-patch-form
  "Prove patch syntax once; also validate an explicitly supplied shape hint."
  [forms]
  (when (and (identical? clojure.lang.PersistentVector (class forms))
             (nil? (meta forms)) (= 1 (count forms)))
    (let [form (nth forms 0)]
      (when (and (identical? clojure.lang.PersistentVector (class form))
                 (= 4 (count form)) (identical? :db.fn/patchIdoc (nth form 0))
                 (positive-literal-id? (nth form 1)) (keyword? (nth form 2))
                 (or (nil? (nth form 3)) (sequential? (nth form 3))))
        form))))

(defn literal-request-shape
  "Recognize a materialized independent family or declarative operation program.
  Full value/schema eligibility remains in the compiler; no sequences are forced."
  [forms]
  (or (independent-request-shape forms)
      (when (literal-patch-form forms) :patch)
      (when (and (identical? clojure.lang.PersistentVector (class forms))
                 (nil? (meta forms)) (seq forms)
                 (every? (fn [form]
                           (or (identical? Datom (class form))
                               (declarative-map-syntax? form)
                               (and (identical? clojure.lang.PersistentVector (class form))
                                    (or (nil? (meta form)) (application-add-metadata? (meta form)))
                                    (declarative-entity-syntax? (nth form 1 nil))
                                    (let [n (count form)]
                                      (case (nth form 0 nil)
                                        :db/add (<= 4 n 5)
                                        :db/retract (<= 3 n 4)
                                        (:db.fn/cas :db/cas) (= n 5)
                                        :db.fn/retractAttribute (= n 3)
                                        (:db.fn/retractEntity :db/retractEntity) (= n 2)
                                        false)))))
                         forms))
        :ordered)))

(defn stable-request-shape
  "Carry an immutable request's structural decision across admission. :dynamic
  records an ordinary decline. Mutable FastLists need a fresh writer snapshot."
  [forms]
  (when-not (identical? FastList (class forms))
    (or (literal-request-shape forms) :dynamic)))

(defn- canonical-scalar-alias?
  [ctx e]
  (or (keyword? e)
      (let [props (get (:schema ctx) (nth e 0))]
        (and (#{:db.unique/identity :db.unique/value} (:db/unique props))
             (contains? ordinary-unique-types (:db/valueType props))
             (canonical-scalar-value? (:validate-data? (:opts ctx))
                                      (:db/valueType props) (nth e 1))))))

(defn- alias-value-info
  "Share scalar eligibility and the independent tuple proof. Entity binding
  stays symbolic, and unsupported values cannot invoke callbacks here."
  [store-schema tuple-source-attrs ^HashMap infos attr value]
  (or (literal-scalar-info store-schema tuple-source-attrs attr value true infos)
      (when (and (keyword? attr) (pos? (count (name attr))))
        (let [^AttrInfo info (attribute-info! infos store-schema tuple-source-attrs attr)]
          (when (and (independent-tuple-info? info)
                     (mixed-tuple-ready? (.-props info) value))
            info)))))

(defn- alias-value-unchanged?
  "Tuple readiness was already proven by alias-value-info; its components
  retain their representation instead of entering scalar correction."
  [validate? vt value]
  (or (identical? vt :db.type/tuple)
      (canonical-scalar-value? validate? vt value)))

(defn- scalar-entity-operand
  [ctx e aliases?]
  (cond
    (positive-literal-id? e) (if (instance? Long e) e (long e))
    (and aliases? (literal-scalar-alias? e) (canonical-scalar-alias? ctx e)) e))

(defn- scalar-entry-plan
  [ctx forms e attr value raw-operations? aliases?]
  (cond
    aliases? (plan/->TxPlan ctx :scalar-aliases [[e attr value]] nil)
    raw-operations? (plan/->TxPlan ctx :scalar-operations forms nil)
    :else (plan/->TxPlan ctx :scalar [[e attr value]] nil)))

(defn- compile-one-scalar
  [ctx forms raw-e attr value operation? allow-references? allow-coercion? aliases?]
  (when-let [e (scalar-entity-operand ctx raw-e aliases?)]
    (when-let [^AttrInfo info (if aliases?
                      (alias-value-info (:schema ctx) (:db/attrTuples (:rschema ctx))
                                         (HashMap.) attr value)
                      (literal-scalar-info (:schema ctx) (:db/attrTuples (:rschema ctx))
                                            attr value allow-references? nil))]
      (let [vt (.-value-type info) validate? (:validate-data? (:opts ctx))]
        (cond
          (if aliases? (alias-value-unchanged? validate? vt value)
              (canonical-scalar-value? validate? vt value))
          (scalar-entry-plan ctx forms e attr value
                             (and operation? (instance? Long raw-e)) aliases?)
          (and (not aliases?) (scalar-validation-rejection? validate? vt value))
          {:outcome :needs-writer :ctx ctx :input forms}
          (and allow-coercion? (pure-scalar-coercion? vt value))
          (when-some [corrected (try (prepare/correct-scalar-value validate? vt value)
                                    (catch Exception _ nil))]
            (scalar-entry-plan ctx forms e attr corrected false aliases?)))))))

(defn correct-scalar-candidates!
  "Lower saved conversion instructions once in owned scratch. Nil instructions
  preserve values; the explicit decline marker distinguishes rejection from a
  legitimate nil or false correction at a runtime binding checkpoint."
  [^FastList candidates correct-value]
  (loop [index 0]
    (if (< index (.size candidates))
      (if-let [vt (.get candidates (+ index 3))]
        (let [value (correct-value vt (.get candidates (+ index 2)))]
          (when-not (identical? operand-decline value)
            (.set candidates (+ index 2) value)
            (recur (+ index 4))))
        (recur (+ index 4)))
      true)))

(defn- materialize-scalar-prefix!
  "Copy a proven canonical prefix only when later operands need lowering.
  Eligibility and distinctness were already checked; no value work is repeated."
  [^FastList candidates forms n]
  (dotimes [i n]
    (let [form (nth forms i)]
      (.add candidates (nth form 1))
      (.add candidates (nth form 2))
      (.add candidates (nth form 3))
      (.add candidates nil))))

(defn- collect-explicit-candidates
  "Feed explicit additions directly to the scalar binder. Stamps stay in the
  original materialized forms until final projection, without a side buffer."
  [forms append!]
  (let [candidates (FastList. (int (* 4 (count forms))))
        seen (when (> (count forms) 1) (HashMap.))]
    (when (reduce
            (fn [_ form]
              (if (cond
                    (identical? Datom (class form))
                    (and (d/datom-added form)
                         (append! candidates seen (.-e ^Datom form) (.-a ^Datom form)
                                  (.-v ^Datom form) false))
                    (plain-map? form)
                    (and (> (count form) 1)
                         (reduce-kv
                           (fn [_ attr value]
                             (if (or (identical? attr :db/id)
                                     (append! candidates seen (:db/id form) attr value false))
                               true (reduced false)))
                           true form))
                    (and (sequential? form) (<= 4 (count form) 5)
                         (identical? :db/add (nth form 0))
                         (or (= 4 (count form)) (nil? (nth form 4))
                             (positive-literal-id? (nth form 4))))
                    (append! candidates seen (nth form 1) (nth form 2) (nth form 3) false))
                true (reduced false)))
            true forms)
      (when-not (.isEmpty candidates) candidates))))

(defn- explicit-candidate-entries
  "Project corrected operands with their source stamps and annotations. Map
  widths align the streams; eligibility and values are never analyzed again."
  [^FastList candidates forms refs rebuild-reference-vectors?]
  (loop [offset 0 source-index 0 remaining 0 form nil entries (transient [])]
    (if (< offset (.size candidates))
      (let [same-source? (pos? remaining)
            form (if same-source? form (nth forms source-index))
            source-index (if same-source? source-index (inc source-index))
            remaining (if same-source? remaining (if (map? form) (dec (count form)) 1))
            e (.get candidates offset) attr (.get candidates (+ offset 1))
            value (.get candidates (+ offset 2))
            rebuild? (and rebuild-reference-vectors? (contains? refs attr))
            tx (cond
                 (identical? Datom (class form)) (d/datom-tx form)
                 (map? form) nil
                 (not rebuild?) (when-some [tx (nth form 4 nil)] (long tx)))
            annotations (when (and (not (map? form)) (not (identical? Datom (class form)))
                                   (not rebuild?)) (meta form))]
        (recur (+ offset 4) source-index (dec remaining) form
               (conj! entries (if annotations [e attr value tx annotations] [e attr value tx]))))
      (persistent! entries))))

(defn- compile-scalar-plan
  "Bind each source operand once. Correct only the fully eligible owned buffer."
  ([ctx forms shape allow-references? allow-coercion? aliases?]
   (compile-scalar-plan ctx forms shape allow-references? allow-coercion? aliases? nil))
  ([ctx forms shape allow-references? allow-coercion? aliases? explicit-rebuild?]
   (compile-scalar-plan ctx forms shape allow-references? allow-coercion? aliases? explicit-rebuild? nil))
  ([ctx forms shape allow-references? allow-coercion? aliases? explicit-rebuild? infos]
   (when (and (seq forms) (not (:auto-entity-time? (:opts ctx))))
     (let [n (count forms) form (nth forms 0)
           explicit? (some? explicit-rebuild?)
           operation? (and (sequential? form) (= 4 (count form))
                           (nil? (meta form)) (identical? :db/add (nth form 0)))]
       (cond
         (and (nil? explicit-rebuild?) (= n 1) operation?)
         (compile-one-scalar ctx forms (nth form 1) (nth form 2) (nth form 3)
                             (identical? shape :scalar-operations)
                             allow-references? allow-coercion? aliases?)
         (and (nil? explicit-rebuild?) (= n 1) (plain-map? form) (nil? (meta form)) (= 2 (count form)))
         (let [^java.util.Iterator keys (.keyIterator ^clojure.lang.IMapIterable form)
               first-key (.next keys)
               attr (if (identical? :db/id first-key) (.next keys) first-key)]
           (compile-one-scalar ctx forms (:db/id form) attr (get form attr) false
                               allow-references? allow-coercion? aliases?))
         :else
         (let [store-schema (:schema ctx) sources (:db/attrTuples (:rschema ctx))
               validate? (:validate-data? (:opts ctx)) infos (or infos (HashMap.))
               canonical? (volatile! true) rejected? (volatile! false) coercible? (volatile! true)
               first-attr (cond
                            operation? (nth form 2)
                            (and explicit? (identical? Datom (class form))) (.-a ^Datom form)
                            (and explicit? (sequential? form)) (nth form 2 nil))
               uniform-attr? (volatile! (boolean (and first-attr (or explicit? (not aliases?)))))
               resolved-entities? (when explicit? (volatile! true))
               raw-operations? (volatile! (and (not aliases?) (identical? shape :scalar-operations)))
               append! (fn [candidates seen raw-e attr value map-form?]
                         (let [retained? (and @canonical? @raw-operations?)]
                           (when (and @uniform-attr? (not (identical? attr first-attr)))
                             (vreset! uniform-attr? false))
                           (when (and @raw-operations? (or map-form? (not (instance? Long raw-e))))
                             (vreset! raw-operations? false))
                           (when-let [e (if map-form? raw-e (scalar-entity-operand ctx raw-e aliases?))]
                             (when (and resolved-entities? @resolved-entities? (not (instance? Long e)))
                               (vreset! resolved-entities? false))
                             (when-let [^AttrInfo info
                                        (if aliases?
                                          (alias-value-info store-schema sources infos attr value)
                                          (literal-scalar-info store-schema sources attr value allow-references? infos))]
                               (let [vt (.-value-type info)
                                     ready? (if aliases? (alias-value-unchanged? validate? vt value)
                                                (canonical-scalar-value? validate? vt value))]
                                 (when-not ready?
                                   (when @canonical? (vreset! canonical? false))
                                   (when (and (not aliases?) (not @rejected?)
                                              (scalar-validation-rejection? validate? vt value))
                                     (vreset! rejected? true))
                                   (when (and @coercible? (not (pure-scalar-coercion? vt value)))
                                     (vreset! coercible? false)))
                                 (let [retain? (and @canonical? @raw-operations?)]
                                   (when (and retained? (not retain?))
                                     (materialize-scalar-prefix! candidates forms
                                                                 (scalar-distinct-count seen)))
                                   (append-scalar-candidate! (when-not retain? candidates)
                                                             seen e attr value (when-not ready? vt))))))))]
           (when-let [candidates (if explicit?
                                  (collect-explicit-candidates forms append!)
                                  (collect-scalar-candidates
                                    forms plain-map? #(scalar-entity-operand ctx % aliases?)
                                    (constantly true) append! (when @raw-operations? 0)
                                    (when (and (not aliases?) (> n 1)) (HashMap.))))]
             (cond
               @rejected? {:outcome :needs-writer :ctx ctx :input forms}
               (and (not @canonical?) (not (and allow-coercion? @coercible?))) nil
               :else
               (when (or @canonical? (correct-scalar-candidates!
                                        candidates
                                        (fn [vt value]
                                          (if-some [corrected
                                                    (try (prepare/correct-scalar-value validate? vt value)
                                                         (catch Exception _ nil))]
                                            corrected operand-decline))))
                 (if (and @canonical? @raw-operations?)
                   (plan/->TxPlan ctx :scalar-operations forms
                                  ;; Distinct E/A plus one attribute implies one
                                  ;; operand per entity, so no wide read is useful.
                                  (when-not @uniform-attr?
                                    (scalar-update-entity-reads
                                      #(.-props ^AttrInfo (.get infos %)) forms 1)))
                   (let [entries (if explicit?
                                   (explicit-candidate-entries candidates forms
                                     (:db.type/ref (:rschema ctx)) explicit-rebuild?)
                                   (plan/scalar-candidate-entries candidates))
                         bind-aliases? (and aliases? (not (and resolved-entities? @resolved-entities?)))]
                     (plan/->TxPlan ctx (if bind-aliases? :scalar-aliases :scalar) entries
                                    (when-not (or bind-aliases? @uniform-attr?)
                                      (scalar-update-entity-reads
                                        #(.-props ^AttrInfo (.get infos %))
                                        entries))))))))))))))

(defn compile-scalar-region
  "Compile the protected literal scalar region using the shared operand builder."
  ([ctx forms] (compile-scalar-region ctx forms (literal-scalar-shape forms)))
  ([ctx forms shape] (compile-scalar-region ctx forms shape false))
  ([ctx forms shape allow-references?]
   (when (#{:scalar :scalar-operations} shape)
     (compile-scalar-plan ctx forms shape allow-references? false false))))

(defn- compile-explicit-additions
  "Normalize materialized additions directly into independent scalar operands.
  Reference preprocessing can discard vector stamps/annotations; Datoms retain
  theirs. Correction still follows complete eligibility and distinctness."
  [ctx forms infos]
  (let [rebuild-reference-vectors? (not (and (ordinary-analysis-enabled? ctx)
                           (every? #(some? (ordinary-addition-flags ctx % true false)) forms)))
        prepared (compile-scalar-plan ctx forms :scalar-aliases true true true
                                       rebuild-reference-vectors? infos)]
    (when (instance? datalevin.db.tx.plan.TxPlan prepared)
      (assoc prepared :explicit-tx? true))))

(defn prepared-write-attribute
  "Bind reusable addition metadata without consulting parameter values.
  Reference and tuple scalar preprocessing retain the ordinary fallback."
  [ctx infos attr strategy]
  (when (and (keyword? attr) (pos? (count (name attr))))
    (case strategy
      :scalar (when-let [^AttrInfo info (scalar-attribute-info
                                        (:schema ctx) (:db/attrTuples (:rschema ctx))
                                        attr 1 :references infos)]
                (when-not (#{:db.type/ref :db.type/tuple} (.-value-type info)) info))
      :scalar-program (when-let [^AttrInfo info (scalar-attribute-info
                                                (:schema ctx) (:db/attrTuples (:rschema ctx))
                                                attr 1 :references infos)]
                        (when (and (.-typed? info) (not (:db/isComponent (.-props info)))) info))
      :blind (blind-attr-info! infos (:schema ctx) (:db/attrTuples (:rschema ctx)) true attr)
      :mixed (let [^AttrInfo info (attribute-info! infos (:schema ctx)
                                                 (:db/attrTuples (:rschema ctx)) attr)]
               (when (.-insert? info) info))
      nil)))

(defn prepared-write-metadata?
  "Whether annotations can retain the pure additive preparation route."
  [annotations]
  (or (nil? annotations) (application-add-metadata? annotations)))

(defn prepared-scalar-match?
  "Prove a scalar match operand without correction. CAS compares its original
  expected value; native retractions require a canonical storage operand."
  [ctx ^AttrInfo info value cas?]
  (or (nil? value)
      (and (inert-scalar-value? value)
           (or cas? (canonical-scalar-value? (:validate-data? (:opts ctx))
                                            (.-value-type info) value)))))

(defn prepared-program-entity
  "Keep existing IDs and canonical lookup operands symbolic until the writer.
  Transaction IDs, tempids and opaque lookup values retain demanded execution."
  [ctx value]
  (when-not (identical? :db/current-tx value)
    (scalar-entity-operand ctx value true)))

(defn prepared-program-reference
  "Keep canonical reference IDs and lookups. Other numeric representations
  retain ordinary CAS diagnostics and correction through the demanded route."
  [ctx value]
  (when (or (instance? Long value) (keyword? value)
            (identical? clojure.lang.PersistentVector (class value)))
    (prepared-program-entity ctx value)))

(defn append-prepared-scalar!
  "Bind one reusable scalar instruction into fresh correction scratch.
  All proofs are value-specific; this worker invokes no runtime callbacks."
  [ctx ^AttrInfo info candidates seen raw-e attr value]
  (when (and info (inert-scalar-value? value)
             (not (and (instance? java.util.Date value)
                       (some? (.-value-type info))
                       (not (identical? :db.type/instant (.-value-type info))))))
    (when-let [e (scalar-entity-operand ctx raw-e true)]
      (let [vt (.-value-type info)
            ready? (canonical-scalar-value? (:validate-data? (:opts ctx)) vt value)]
        (when (or ready?
                  (and (not (scalar-validation-rejection? (:validate-data? (:opts ctx)) vt value))
                       (pure-scalar-coercion? vt value)))
          (or (append-scalar-candidate! candidates seen e attr value (when-not ready? vt))
              ;; A typed replacement chain can use execution-local value slots.
              ;; Aliases are excluded by the template binder before selection.
              (when (.-typed? info)
                (append-scalar-candidate! candidates nil e attr value (when-not ready? vt))
                ::dependent-scalar)))))))

(defn correct-prepared-scalars!
  "Correct a fully proven prepared binding, preserving ordinary rejection."
  [ctx candidates]
  (correct-scalar-candidates!
    candidates (fn [vt value]
                 (if-some [corrected (try (prepare/correct-scalar-value
                                           (:validate-data? (:opts ctx)) vt value)
                                         (catch Exception _ nil))]
                   corrected operand-decline))))

(defn prepared-tempid-key
  "Prove an inert new-entity ID without host methods."
  [value]
  (literal-tempid-key value))

(defn prepared-positive-id?
  "Prove an inert supplied transaction or concrete entity ID."
  [value]
  (positive-literal-id? value))

(defn prepared-literal?
  "Prove a definition's scalar map key without invoking a host method."
  [value]
  (inert-scalar-value? value))

(defn append-prepared-blind!
  "Collect one parameter value with the existing blind proof/expansion rule."
  [ctx attrs info attr value]
  (compile-blind-attribute! ctx attrs info attr value false nil))

(defn finish-prepared-blind
  "Lower fresh, fully proven private operands with the shared uniqueness rules.
  Sources have distinct inert tempids and no runtime correction instructions."
  [ctx entries infos identity? fuse?]
  (lower-literal-blind-template (BlindTemplate. ctx entries infos identity? fuse?) true))

(defn flat-upsert-map-proof
  "Prove one already-demanded plain map using value classes only.
  Bit zero permits cached identity ownership; bit one proves non-nil map syntax.
  Runtime analysis owns sequence demand and supplies the private-tempid flag."
  [ctx entity private-tempid?]
  (if (plain-map? entity)
    (reduce-kv
      (fn [proof attr value]
        (if (and (keyword? attr)
                 (if (identical? attr :db/id)
                   (or private-tempid? (contains? inert-value-classes (class value)))
                   (and (or (contains? ordinary-scalar-types
                                        (:db/valueType (get (:schema ctx) attr)))
                            (identical? :db.type/ref (:db/valueType (get (:schema ctx) attr))))
                        (or (nil? value) (contains? inert-value-classes (class value))))))
          (if (nil? value) (bit-and (long proof) 1) proof)
          (reduced 0)))
      3 entity)
    0))

(defn- datom-operation-form
  [^Datom form]
  (if (d/datom-added form)
    [:db/add (.-e form) (.-a form) (.-v form) (d/datom-tx form)]
    [:db/retract (.-e form) (.-a form) (.-v form)]))

(defn compile-program
  "Retain original input for demanded execution by its realization driver.
  No input methods, metadata, schema values or callbacks are consulted."
  [ctx forms]
  (plan/->TxPlan ctx :dynamic forms nil))

(defn- ordered-bound-props
  [ctx infos attr]
  (.-props ^AttrInfo (attribute-info! infos (:schema ctx)
                                     (:db/attrTuples (:rschema ctx)) attr)))

(defn- ordered-tuple-props? [props]
  (or (identical? :db.type/tuple (:db/valueType props))
      (:db/tupleTypes props) (:db/tupleType props) (:db/tupleAttrs props)))

(defn- ordered-tuple-types
  [ctx infos props size]
  (or (:db/tupleTypes props)
      (when-let [sources (:db/tupleAttrs props)]
        (mapv #(:db/valueType (ordered-bound-props ctx infos %)) sources))
      (when-let [type (:db/tupleType props)] (repeat size type))))

(defn- ordered-tuple-types? [types]
  (and (seq types)
       (every? #(and (keyword? %) (or (contains? ordinary-tuple-types %)
                                      (identical? :db.type/bytes %))) types)))

(declare ordered-tuple-value?)

(defn- ordered-lookup?
  [ctx infos value]
  (when (and (identical? clojure.lang.PersistentVector (class value))
             (nil? (meta value)) (= 2 (count value)) (keyword? (nth value 0)))
    (let [props (ordered-bound-props ctx infos (nth value 0)) vt (:db/valueType props)]
      (and (#{:db.unique/identity :db.unique/value} (:db/unique props))
           (if (ordered-tuple-props? props)
             (ordered-tuple-value? ctx infos props (nth value 1))
             (and (contains? ordinary-unique-types vt)
                  (canonical-scalar-value? (:validate-data? (:opts ctx)) vt (nth value 1))))))))

(defn- ordered-entity?
  [ctx infos e]
  (or (and (inert-scalar-value? e) (integer? e)
           (try (<= Long/MIN_VALUE (long e) Long/MAX_VALUE)
                (catch IllegalArgumentException _ false)))
      (string? e) (keyword? e)
      (ordered-lookup? ctx infos e)))

(defn- ordered-attribute-props
  [ctx ^HashMap infos attr]
  (when (and (keyword? attr) (pos? (count (name attr)))
             (not= "db" (namespace attr)) (not (plan/reverse-ref-attr? attr)))
    (let [^AttrInfo info (attribute-info! infos (:schema ctx)
                                       (:db/attrTuples (:rschema ctx)) attr)]
      (if-some [eligible? (.-ordered? info)]
        (when eligible? (.-props info))
        (let [props (.-props info) vt (.-value-type info)
              tuple? (ordered-tuple-props? props)
              eligible?
              (and props (not (:db.attr/preds props))
                   ;; Untyped values retain demanded execution. Tuple metadata
                   ;; must prove every component type before inspecting values.
                   (or (some? vt) tuple?)
                   (or (not tuple?)
                       (ordered-tuple-types? (ordered-tuple-types ctx infos props 8)))
                   (or (contains? ordinary-scalar-types vt)
                       (#{:db.type/ref :db.type/tuple} vt))
                   (not (:db/embedding props)) (not (:db/vector props))
                   (every? (fn [[tuple _]]
                             (let [p (ordered-bound-props ctx infos tuple)]
                               (and (not (:db.attr/preds p))
                                    (not (:db/embedding p)) (not (:db/vector p))
                                    (or (nil? (:db/valueType p))
                                        (identical? :db.type/tuple (:db/valueType p)))
                                    (ordered-tuple-props? p)
                                    (ordered-tuple-types? (ordered-tuple-types ctx infos p 8)))))
                           (.-tuples info)))]
          ;; Complete the existing immutable binding in this attempt's table.
          ;; Executors consume props and retain all writer-side obligations.
          (.put infos attr (plan/with-ordered-proof info eligible?))
          (when eligible? props))))))

(defn- ordered-tuple-value?
  [ctx infos props value]
  (when (and (identical? clojure.lang.PersistentVector (class value))
             (nil? (meta value)) (<= 1 (count value) 8))
    (let [types (ordered-tuple-types ctx infos props (count value))]
      (and (ordered-tuple-types? types) (= (count types) (count value))
           (every? (fn [[type component]]
                     (or (nil? component)
                         (if (identical? type :db.type/ref)
                           (and (not (coll? component)) (ordered-entity? ctx infos component))
                           (and (inert-scalar-value? component)
                                (canonical-scalar-value? true type component)))))
                   (map vector types value))))))

(defn- ordered-value?
  [ctx infos props value]
  (let [vt (:db/valueType props)]
    (cond
      (ordered-tuple-props? props) (ordered-tuple-value? ctx infos props value)
      (identical? vt :db.type/ref) (ordered-entity? ctx infos value)
      :else
      (and (inert-scalar-value? value) (some? value)
           ;; Date formatting and effectful custom conversions stay ordered.
           (or (canonical-scalar-value? (:validate-data? (:opts ctx)) vt value)
               (pure-scalar-coercion? vt value))))))

(declare ordered-map?)

(defn- ordered-map-field?
  [ctx infos props value depth]
  (if (and (identical? :db.type/ref (:db/valueType props)) (plain-map? value))
    (ordered-map? ctx infos value (inc depth))
    (ordered-value? ctx infos props value)))

(defn- ordered-map?
  [ctx infos form depth]
  (and (< depth 64) (plain-map? form)
       (or (nil? (meta form))
           (and (application-add-metadata? (meta form))
                (or (positive-literal-id? (:db/id form))
                    (ordered-lookup? ctx infos (:db/id form)))))
       (or (not (contains? form :db/id))
           (let [e (:db/id form)]
             (and (not (keyword? e)) (ordered-entity? ctx infos e))))
       (reduce-kv
         (fn [_ attr value]
           (if (identical? attr :db/id)
             true
             (if (and (keyword? attr) (pos? (count (name attr))))
               (let [reverse? (plan/reverse-ref-attr? attr)
                     straight (if reverse?
                                (some (fn [a]
                                        (when (and (= (namespace a) (namespace attr))
                                                   (= (name a) (subs (name attr) 1))) a))
                                      (keys (:schema ctx)))
                                attr)
                     props (ordered-attribute-props ctx infos straight)
                     lookup? (ordered-lookup? ctx infos value)]
                 (if (and props (or (not reverse?) (identical? :db.type/ref (:db/valueType props)))
                          (if (and (or reverse? (identical? :db.cardinality/many (:db/cardinality props)))
                                   (literal-many-values? value) (not lookup?))
                            (every? #(ordered-map-field? ctx infos props % depth) value)
                            (ordered-map-field? ctx infos props value depth)))
                   true (reduced false)))
               (reduced false))))
         true form)))

(defn- ordered-form?
  [ctx infos form]
  (let [form (if (identical? Datom (class form)) (datom-operation-form form) form)]
  (when (and (identical? clojure.lang.PersistentVector (class form))
             (or (nil? (meta form)) (application-add-metadata? (meta form))))
    (let [[op e attr value extra] form n (count form)]
      (and (ordered-entity? ctx infos e)
           (case op
             (:db.fn/retractEntity :db/retractEntity)
             ;; Cascades may visit any attribute. Unknown/custom decoding and
             ;; predicates remain outside this declarative proof.
             (and (= n 2)
                  (every? (fn [[attr _]]
                            (or (= "db" (namespace attr))
                                (ordered-attribute-props ctx infos attr)))
                          (:schema ctx)))
             (:db/add :db/retract :db.fn/cas :db/cas :db.fn/retractAttribute)
             (when-let [props (ordered-attribute-props ctx infos attr)]
               (and (or (not (:db/isComponent props))
                        (every? (fn [[a _]]
                                  (or (= "db" (namespace a))
                                      (ordered-attribute-props ctx infos a))) (:schema ctx)))
                    (case op
                      :db/add (and (<= 4 n 5) (ordered-value? ctx infos props value)
                                   (or (= n 4) (nil? extra) (positive-literal-id? extra)))
                      :db/retract (and (<= 3 n 4)
                                       (or (nil? value) (ordered-value? ctx infos props value)))
                      :db.fn/retractAttribute (= n 3)
                      (:db.fn/cas :db/cas)
                      (and (= n 5) (or (nil? value) (ordered-value? ctx infos props value))
                           (ordered-value? ctx infos props extra)))))
             false))))))

(def ^:private ordered-schema-proof-marker (Object.))

(defn- ordered-schema-proof?
  "Prove cascade eligibility once in the existing compilation-owned metadata
  table. No schema-wide proof or table is rebuilt for each operation."
  [ctx ^HashMap infos]
  (if-some [proof (.get infos ordered-schema-proof-marker)]
    proof
    (let [proof (boolean (every? (fn [[a _]]
                                  (or (= "db" (namespace a)) (ordered-attribute-props ctx infos a)))
                                (:schema ctx)))]
      (.put infos ordered-schema-proof-marker proof)
      proof)))

(defn prepared-ordered-operation
  "Capture the ordinary ordered worker's attribute/cascade proof once. Values
  and lookup ownership remain specific to each invocation and live operation."
  [ctx infos opcode attr]
  (when (and (not (:auto-entity-time? (:opts ctx)))
             (#{:db/add :db/retract :db/cas :db.fn/cas :db.fn/retractAttribute
                :db/retractEntity :db.fn/retractEntity} opcode))
    (let [entity? (#{:db/retractEntity :db.fn/retractEntity} opcode)
          props (when-not entity? (ordered-attribute-props ctx infos attr))]
      (when (and (or entity? props)
                 (or (not (or entity? (:db/isComponent props)))
                     (ordered-schema-proof? ctx infos)))
        {:opcode opcode :info (when-not entity? (.get ^HashMap infos attr))}))))

(defn prepared-ordered-operands?
  "Check inert bound operands with the existing ordered value rules. No
  correction, lookup, allocation, pending mutation or host methods occur."
  [ctx infos {:keys [opcode info]} e value extra]
  (and (ordered-entity? ctx infos e)
       ;; Invalid tempid operations retain their original diagnostic form.
       (or (identical? opcode :db/add)
           (not (or (string? e) (and (integer? e) (neg? (long e))))))
       (if (#{:db/retractEntity :db.fn/retractEntity} opcode)
         true
         (let [props (.-props ^AttrInfo info)]
           (case opcode
             :db/add (and (ordered-value? ctx infos props value)
                          (or (nil? extra) (positive-literal-id? extra)))
             :db/retract (or (nil? value) (ordered-value? ctx infos props value))
             :db.fn/retractAttribute true
             (:db/cas :db.fn/cas) (and (or (nil? value) (ordered-value? ctx infos props value))
                                       (ordered-value? ctx infos props extra)))))))

(defn prepared-ordered-map
  "Capture flat map attribute proofs without inspecting parameter markers.
  Map allocation and identity ownership remain writer-attempt operations."
  [ctx infos source]
  (when (and (plain-map? source) (not (:auto-entity-time? (:opts ctx)))
             (prepared-write-metadata? (meta source))
             (every? #(or (identical? :db/id %) (ordered-attribute-props ctx infos %)) (keys source)))
    {:implicit? (not (contains? source :db/id))}))

(defn prepared-ordered-map-operands?
  "Check flat value containers with the shared ordered map rules. Nested
  entity expansion retains ordinary preprocessing and private-ID assignment."
  [ctx infos entity]
  (and (ordered-map? ctx infos entity 0)
       (every? (fn [[a value]]
                 (or (identical? :db/id a)
                     (not (identical? :db.type/ref (.-value-type ^AttrInfo (.get ^HashMap infos a))))
                     (and (not (map? value))
                          (or (not (literal-many-values? value)) (not-any? map? value))))) entity)))

(defn- ordered-independent-prefix
  "Select a bounded, existing-EID scalar prefix. These operands cannot be a
  future value-tempid target, queue tuples or require redundant-add evidence."
  [ctx infos forms]
  (let [seen (HashSet.)
        prefix (into []
                 (take-while
                   (fn [form]
                     (and (identical? clojure.lang.PersistentVector (class form))
                          (nil? (meta form)) (= 4 (count form))
                          (identical? :db/add (nth form 0))
                          (instance? Long (nth form 1)) (pos? (long (nth form 1)))
                          (when-let [^AttrInfo info (literal-scalar-info
                                            (:schema ctx) (:db/attrTuples (:rschema ctx))
                                            (nth form 2) (nth form 3) true infos)]
                            (and (.-typed? info)
                                 (not (identical? :db.type/ref (.-value-type info)))
                                 (canonical-scalar-value? true (.-value-type info) (nth form 3))
                                 (.add seen [(nth form 1) (nth form 2)]))))))
                 (take 8 forms))]
    (when (>= (count prefix) 2)
      (when-let [prepared (compile-scalar-plan ctx prefix :scalar true false false nil infos)]
        (when (instance? datalevin.db.tx.plan.TxPlan prepared)
          {:prefix-plan prepared :prefix-count (count prefix)})))))

(defn- compile-ordered-region
  "Retain original declarative forms and an optional independent prefix proof.
  Preparation owns expansion, reference stamps, pending state and retries."
  [ctx forms infos]
  (when (and (identical? clojure.lang.PersistentVector (class forms))
             (nil? (meta forms)) (seq forms)
             (not (:auto-entity-time? (:opts ctx))))
    (let [infos (or infos (HashMap.))]
      (when (every? #(if (plain-map? %) (ordered-map? ctx infos % 0) (ordered-form? ctx infos %)) forms)
        (merge (plan/->TxPlan ctx :ordered forms nil)
               (ordered-independent-prefix ctx infos forms))))))

(defn snapshot-request
  "Snapshot an exact FastList for planning without inspecting its elements.
  Custom and lazy containers retain their original identity and demand order."
  [forms]
  (if (identical? FastList (class forms)) (vec forms) forms))

(defn compile-tx
  "Select an independent or ordered rule from a pure structural decision.
  With :dynamic? true, retain all remaining input for demanded execution."
  ([ctx forms] (compile-tx ctx forms {}))
  ([ctx forms {:keys [literal-shape request-shape] :as options}]
   (let [original forms
         forms (snapshot-request forms)
         shape (or literal-shape request-shape (literal-request-shape forms))
         ;; Reuse each multi-form independent rule's existing metadata table if
         ;; selection proceeds to mixed/ordered rules. Singletons keep direct
         ;; attribute binding; each table belongs to this captured context.
         infos (when (and (identical? clojure.lang.PersistentVector (class forms))
                          (> (count forms) 1)
                          (#{:scalar :scalar-operations :scalar-aliases :explicit-additions :mixed} shape))
                 (HashMap.))
         selected
         (case shape
           :explicit-additions (compile-explicit-additions ctx forms infos)
           :patch
           (when-let [form (if (or literal-shape request-shape)
                            (literal-patch-form forms)
                            (nth forms 0))]
             (compile-patch-region ctx form (long (nth form 1)) (nth form 2) (nth form 3)))
           (:scalar :scalar-operations)
           (or (compile-scalar-plan ctx forms shape (nil? literal-shape) (nil? literal-shape) false nil infos)
               (when (nil? literal-shape) (compile-mixed-region* ctx forms infos)))
           :scalar-aliases
           (compile-scalar-plan ctx forms shape true true true nil infos)
           :blind
           (or (when-not (:skip-blind? options) (compile-blind-plan ctx forms options))
               (when (needs-ordinary-insert-plan? ctx forms) (compile-mixed-region ctx forms)))
           :mixed (compile-mixed-region* ctx forms infos)
           nil)]
     (or (when-not (identical? ::small-unique-decline selected) selected)
         (when (and (nil? literal-shape) (not (identical? :dynamic shape))
                    (not= false (:ordered? options))
                    (not (identical? ::small-unique-decline selected)))
           (compile-ordered-region ctx forms infos))
         (when (and (nil? literal-shape) (:dynamic? options))
           ;; A declined snapshot must not freeze the live dynamic cursor.
           (compile-program ctx original))))))
