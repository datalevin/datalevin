;;
;; Copyright (c) Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns ^:no-doc datalevin.validate
  "All validation functions for Datalevin.
   Pure checks that raise on invalid input — no data transformation."
  (:require
   [datalevin.interface :as i
    :refer [schema opts populated? visit-list-range]]
   [datalevin.index :as idx]
   [datalevin.datom :as d]
   [datalevin.constants :as c]
   [datalevin.util :as u]
   [datalevin.bits :as b]
   [datalevin.lmdb :as lmdb])
  (:import
   [datalevin.bits Retrieved]
   [datalevin.datom Datom]
   [datalevin.lmdb KVTxData]))

;; ---- Storage / schema validators ----

(defn validate-closed-schema
  "Validate that attribute is defined in schema when :closed-schema? is true."
  [schema opts attr value]
  (when (and (opts :closed-schema?) (not (schema attr)))
    (u/raise "Attribute is not defined in schema when
`:closed-schema?` is true: " attr {:attr attr :value value})))

(defn validate-cardinality-change
  "Validate cardinality change from many to one."
  [store attr old new]
  (when (and (identical? old :db.cardinality/many)
             (identical? new :db.cardinality/one))
    (let [low-datom  (d/datom c/e0 attr c/v0)
          high-datom (d/datom c/emax attr c/vmax)]
      (when (populated? store :ave low-datom high-datom)
        (u/raise "Cardinality change is not allowed when data exist"
                 {:attribute attr})))))

(defn validate-value-type-change
  "Validate value type change when data exist.
   Allows migration from untyped (:data) to a specific type."
  [store attr old new]
  (when (not= old new)
    (when-let [props ((schema store) attr)]
      (let [old-vt (idx/value-type props)]
        (when-not (identical? old-vt :data)
          (let [low-datom  (d/datom c/e0 attr c/v0)
                high-datom (d/datom c/emax attr c/vmax)]
            (when (populated? store :ave low-datom high-datom)
              (u/raise "Value type change is not allowed when data exist"
                       {:attribute attr}))))))))

(defn violate-unique?
  "Check if adding uniqueness to an attribute would violate existing data."
  [lmdb idx-schema low-datom high-datom]
  (let [prev-v   (volatile! nil)
        violate? (volatile! false)
        visitor  (fn [kv]
                   (let [avg ^Retrieved (b/read-buffer (lmdb/k kv) :avg)
                         v   (idx/retrieved->v lmdb avg)]
                     (if (= @prev-v v)
                       (do (vreset! violate? true)
                           :datalevin/terminate-visit)
                       (vreset! prev-v v))))]
    (visit-list-range
      lmdb c/ave visitor
      [:closed (idx/index->k :ave idx-schema low-datom false)
       (idx/index->k :ave idx-schema high-datom true)] :avg
      [:closed c/e0 c/emax] :id)
    @violate?))

(defn validate-uniqueness-change
  "Validate uniqueness change is consistent with existing data."
  [store lmdb attr old new]
  (when (and (not old) new)
    (let [low-datom  (d/datom c/e0 attr c/v0)
          high-datom (d/datom c/emax attr c/vmax)]
      (when (populated? store :ave low-datom high-datom)
        (when (violate-unique? lmdb (schema store) low-datom high-datom)
          (u/raise "Attribute uniqueness change is inconsistent with data"
                   {:attribute attr}))))))

(defn validate-schema-mutation
  "Validate schema attribute changes (cardinality, value type, uniqueness)."
  [store lmdb attr old-props new-props]
  (doseq [[k v] new-props
          :let  [v' (old-props k)]]
    (case k
      :db/cardinality (validate-cardinality-change store attr v' v)
      :db/valueType   (validate-value-type-change store attr v' v)
      :db/unique      (validate-uniqueness-change store lmdb attr v' v)
      :pass-through)))

(def ^:private boolean-opts
  #{:validate-data? :auto-entity-time? :closed-schema? :background-sampling?
    :txn-log? :txn-log-sync-adaptive?
    :txn-log-segment-prealloc? :txn-log-commit-marker?
    :txn-log-rollback?})

(def ^:private non-negative-int-opts
  #{:cache-limit
    :txn-log-group-commit
    :txn-log-group-commit-ms
    :txn-log-meta-flush-max-txs
    :txn-log-meta-flush-max-ms})

(def ^:private positive-int-opts
  #{:txn-log-commit-wait-ms
    :txn-log-replica-floor-ttl-ms
    :txn-log-retention-pin-backpressure-threshold-ms
    :txn-log-commit-marker-version
    :txn-log-segment-max-bytes
    :txn-log-segment-max-ms
    :txn-log-segment-prealloc-bytes
    :txn-log-retention-bytes
    :txn-log-retention-ms
    :txn-log-vec-checkpoint-interval-ms
    :txn-log-vec-max-lsn-delta
    :txn-log-vec-max-buffer-bytes
    :txn-log-vec-chunk-bytes})

(def ^:private keyword-enum-opts
  {:txn-log-durability-profile #{:strict :relaxed}
   :txn-log-sync-mode          #{:fsync :fdatasync :none}
   :txn-log-segment-prealloc-mode #{:native :mmap :none}
   :txn-log-rollout-mode       #{:active :rollback}})

(defn validate-option-mutation
  "Validate option key/value before commit."
  [k v]
  (cond
    (boolean-opts k)
    (when-not (or (true? v) (false? v))
      (u/raise "Option " k " expects a boolean, got " v
               {:option k :value v}))

    (non-negative-int-opts k)
    (when-not (and (integer? v) (not (neg? ^long v)))
      (u/raise "Option " k " expects a non-negative integer, got " v
               {:option k :value v}))

    (positive-int-opts k)
    (when-not (and (integer? v) (pos? ^long v))
      (u/raise "Option " k " expects a positive integer, got " v
               {:option k :value v}))

    (= k :db-name)
    (when-not (string? v)
      (u/raise "Option :db-name expects a string, got " v
               {:option k :value v}))

    :else
    (when-let [allowed (keyword-enum-opts k)]
      (when-not (allowed v)
        (u/raise "Option " k " expects one of " allowed ", got " v
                 {:option k :value v :allowed allowed})))))

;; ---- Key size validation ----

(def ^:private size-exempt-key-types
  "Key types that are either fixed-size or manage their own sizing internally.
   Datalog keys use the giant mechanism and never overflow."
  #{:long :id :int :short :byte :int-int :avg :attr :raw
    :float :double :boolean :instant :uuid
    :ints :bitmap :term-info :doc-info :pos-info :instant-pre-06})

(defn validate-key-size
  "Validate that a key does not exceed the LMDB max key size (511 bytes).
   For KV API use — Datalog keys use the giant mechanism and never overflow."
  [key key-type]
  (when (and key (not (size-exempt-key-types key-type)))
    (when (> ^long (b/measure-size key) c/+max-key-size+)
      (u/raise "Key cannot be larger than 511 bytes" {:input key}))))

;; ---- KV validation ----

(def ^:private kv-ops #{:put :del :put-list :del-list})

(defn validate-kv-op
  "Validate that the KV operation is a known operator."
  [op]
  (when-not (kv-ops op)
    (u/raise "Unknown kv transact operator: " op {})))

(defn validate-kv-key
  "Validate a KV key: must not be nil; optionally check data type."
  [k kt validate-data?]
  (when (nil? k)
    (u/raise "Key cannot be nil" {}))
  (when validate-data?
    (when-not (b/valid-data? k kt)
      (u/raise "Invalid data, expecting " kt " got " k {:input k}))))

(defn validate-kv-value
  "Validate a KV value: must not be nil; optionally check data type."
  [v vt validate-data?]
  (when (nil? v)
    (u/raise "Value cannot be nil" {}))
  (when validate-data?
    (when-not (b/valid-data? v vt)
      (u/raise "Invalid data, expecting " vt " got " v {:input v}))))

(defn validate-kv-tx-data
  "Validate a single KVTxData: op shape, key, value, and key size."
  [^KVTxData tx validate-data?]
  (let [op (.-op tx)
        k  (.-k tx)
        kt (.-kt tx)
        v  (.-v tx)
        vt (.-vt tx)]
    (validate-kv-op op)
    (validate-kv-key k kt validate-data?)
    (validate-key-size k kt)
    (case op
      :put      (validate-kv-value v vt validate-data?)
      :put-list (do (when-not (or (sequential? v) (instance? java.util.List v))
                      (u/raise "List value must be a sequential collection, got "
                               (if (nil? v) "nil" (type v)) {:input v}))
                    (doseq [vi v]
                      (validate-kv-value vi vt validate-data?)))
      :del-list (do (when-not (or (sequential? v) (instance? java.util.List v))
                      (u/raise "List value must be a sequential collection, got "
                               (if (nil? v) "nil" (type v)) {:input v}))
                    (when validate-data?
                      (doseq [vi v]
                        (when-not (b/valid-data? vi vt)
                          (u/raise "Invalid data, expecting " vt " got " vi
                                   {:input vi})))))
      :del      nil)))

;; ---- DB validators ----

(defn validate-schema-key
  "Validate a single schema key-value pair against expected values."
  [a k v expected]
  (when-not (or (nil? v) (contains? expected v))
    (u/raise "Bad attribute specification for " {a {k v}}
             ", expected one of " expected
             {:error     :schema/validation
              :attribute a
              :key       k
              :value     v})))

(def tuple-props #{:db/tupleAttrs :db/tupleTypes :db/tupleType})

(defn validate-schema
  "Validate full schema structure."
  [schema]
  (doseq [[a kv] schema]
    (let [comp? (:db/isComponent kv false)]
      (validate-schema-key a :db/isComponent (:db/isComponent kv) #{true false})
      (when (and comp? (not (identical? (:db/valueType kv) :db.type/ref)))
        (u/raise
          "Bad attribute specification for " a
          ": {:db/isComponent true} should also have {:db/valueType :db.type/ref}"
          {:error     :schema/validation
           :attribute a
           :key       :db/isComponent})))
    (validate-schema-key a :db/unique (:db/unique kv)
                         #{:db.unique/value :db.unique/identity})
    (validate-schema-key a :db/valueType (:db/valueType kv)
                         c/datalog-value-types)
    (validate-schema-key a :db/cardinality (:db/cardinality kv)
                         #{:db.cardinality/one :db.cardinality/many})
    (validate-schema-key a :db/fulltext (:db/fulltext kv)
                         #{true false})

    ;; tuple should have one of tuple-props
    (when (and (identical? :db.type/tuple (:db/valueType kv))
               (not (some tuple-props (keys kv))))
      (u/raise
        "Bad attribute specification for " a
        ": {:db/valueType :db.type/tuple} should also have :db/tupleAttrs, :db/tupleTypes, or :db/tupleType"
        {:error     :schema/validation
         :attribute a
         :key       :db/valueType}))

    ;; :db/tupleAttrs is a non-empty sequential coll
    (when (contains? kv :db/tupleAttrs)
      (let [ex-data {:error     :schema/validation
                     :attribute a
                     :key       :db/tupleAttrs}]
        (when (identical? :db.cardinality/many (:db/cardinality kv))
          (u/raise a " has :db/tupleAttrs, must be :db.cardinality/one" ex-data))

        (let [attrs (:db/tupleAttrs kv)]
          (when-not (sequential? attrs)
            (u/raise a " :db/tupleAttrs must be a sequential collection, got: " attrs ex-data))

          (when (empty? attrs)
            (u/raise a " :db/tupleAttrs can\u2019t be empty" ex-data))

          (doseq [attr attrs
                  :let [ex-data (assoc ex-data :value attr)]]
            (when (contains? (schema attr) :db/tupleAttrs)
              (u/raise a " :db/tupleAttrs can\u2019t depend on another tuple attribute: " attr ex-data))

            (when (identical? :db.cardinality/many (:db/cardinality (schema attr)))
              (u/raise a " :db/tupleAttrs can\u2019t depend on :db.cardinality/many attribute: " attr ex-data))))))

    (when (contains? kv :db/tupleType)
      (let [ex-data {:error     :schema/validation
                     :attribute a
                     :key       :db/tupleType}
            attr    (:db/tupleType kv)]
        (when-not (c/datalog-value-types attr)
          (u/raise a " :db/tupleType must be a single value type, got: " attr ex-data))
        (when (identical? attr :db.type/tuple)
          (u/raise a " :db/tupleType cannot be :db.type/tuple" ex-data))))

    (when (contains? kv :db/tupleTypes)
      (let [ex-data {:error     :schema/validation
                     :attribute a
                     :key       :db/tupleTypes}
            attrs   (:db/tupleTypes kv)]
        (when-not (and (sequential? attrs) (< 1 (count attrs))
                       (every? c/datalog-value-types attrs)
                       (not (some #(identical? :db.type/tuple %) attrs)))
          (u/raise a " :db/tupleTypes must be a sequential collection of more than one value types, got: " attrs ex-data))))))

(defn validate-attr
  "Validate that an attribute is a keyword."
  [attr at]
  (when-not (keyword? attr)
    (u/raise "Bad entity attribute " attr " at " at ", expected keyword"
             {:error :transact/syntax, :attribute attr, :context at})))

(defn validate-val
  "Validate that a value is not nil."
  [v at]
  (when (nil? v)
    (u/raise "Cannot store nil as a value at " at
             {:error :transact/syntax, :value v, :context at})))

(defn validate-datom-unique
  "Validate unique constraint on a datom. Called from db.clj's validate-datom
   with pre-resolved unique? and found? predicates to avoid circular dependency."
  [unique? ^Datom datom found?]
  (let [a (.-a datom)
        v (.-v datom)]
    (when (and unique? (d/datom-added datom))
      (when-some [found (found?)]
        (u/raise "Cannot add " datom " because of unique constraint: " found
                 {:error     :transact/unique
                  :attribute a
                  :datom     datom})))
    v))

(defn validate-upserts
  "Throws if not all upserts point to the same entity.
   Returns single eid that all upserts point to, or null.
   tempid-fn? is a predicate that checks if a value is a tempid."
  [entity upserts tempid-fn?]
  (let [upsert-ids (reduce-kv
                     (fn [m a v->e]
                       (reduce-kv
                         (fn [m v e]
                           (assoc m e [a v]))
                         m v->e))
                     {} upserts)]
    (if (<= 2 (count upsert-ids))
      (let [[e1 [a1 v1]] (first upsert-ids)
            [e2 [a2 v2]] (second upsert-ids)]
        (u/raise "Conflicting upserts: " [a1 v1] " resolves to " e1 ", but " [a2 v2] " resolves to " e2
               {:error     :transact/upsert
                :assertion [e1 a1 v1]
                :conflict  [e2 a2 v2]}))
      (let [[upsert-id [a v]] (first upsert-ids)
            eid               (:db/id entity)]
        (when (and
                (some? upsert-id)
                (some? eid)
                (not (tempid-fn? eid))
                (not= upsert-id eid))
          (u/raise "Conflicting upsert: " [a v] " resolves to " upsert-id ", but entity already has :db/id " eid
                 {:error     :transact/upsert
                  :assertion [upsert-id a v]
                  :conflict  {:db/id eid}}))
        upsert-id))))

;; ---- Type validation ----

(defn validate-type
  "Validate that data matches declared value type when :validate-data? is set.
   Returns the value type."
  [store a v]
  (let [st-opts   (opts store)
        st-schema (schema store)
        vt        (idx/value-type (st-schema a))]
    (or (not (st-opts :validate-data?))
        (b/valid-data? v vt)
        (u/raise "Invalid data, expecting" vt " got " v {:input v}))
    vt))

;; ---- Transaction form validators ----

(defn validate-tempid-op
  "Validate that tempids are only used with :db/add."
  [tempid? op entity]
  (when (and tempid? (not (identical? op :db/add)))
    (u/raise "Can't use tempid in '" entity "'. Tempids are allowed in :db/add only"
             {:error :transact/syntax, :op entity})))

(defn validate-cas-value
  "Validate CAS compare-and-swap: existing value must match expected old value.
   For multival attrs, checks if ov is among datom values.
   For single-val attrs, checks if the single datom value equals ov."
  [multival? e a ov nv datoms]
  (if multival?
    (when-not (some (fn [^Datom d] (= (.-v d) ov)) datoms)
      (u/raise ":db.fn/cas failed on datom [" e " " a " " (map :v datoms) "], expected " ov
               {:error :transact/cas, :old datoms, :expected ov, :new nv}))
    (let [v (:v (first datoms))]
      (when-not (= v ov)
        (u/raise ":db.fn/cas failed on datom [" e " " a " " v "], expected " ov
                 {:error :transact/cas, :old (first datoms), :expected ov, :new nv})))))

(defn validate-patch-idoc-arity
  "Validate patchIdoc arity: expected 4 or 5 items."
  [argc entity op]
  (when-not (or (= argc 4) (= argc 5))
    (u/raise "Bad arity for :db.fn/patchIdoc, expected 4 or 5 items: "
             entity
             {:error :transact/syntax, :operation op, :tx-data entity})))

(defn validate-patch-idoc-type
  "Validate that the attribute has idoc value type."
  [value-type a]
  (when-not (identical? value-type :db.type/idoc)
    (u/raise "Attribute is not an idoc type: " a
             {:attribute a})))

(defn validate-patch-idoc-cardinality
  "Validate patchIdoc cardinality rules for old value."
  [many? old-v a]
  (when (and many? (nil? old-v))
    (u/raise "Idoc patch requires old value for cardinality many attribute: "
             a {:attribute a}))
  (when (and (not many?) (some? old-v))
    (u/raise "Idoc patch old value is only supported for cardinality many attribute: "
             a {:attribute a})))

(defn validate-patch-idoc-old-value
  "Validate that old value exists for cardinality-many idoc patch."
  [old-datom old-v a]
  (when-not old-datom
    (u/raise "Idoc patch old value not found: " old-v
             {:attribute a :value old-v})))

(defn validate-custom-tx-fn-value
  "Validate that a resolved entity has a fn? :db/fn attribute."
  [fun op entity]
  (when-not (fn? fun)
    (u/raise "Entity " op " expected to have :db/fn attribute with fn? value"
             {:error :transact/syntax, :operation :db.fn/call, :tx-data entity})))

(defn validate-custom-tx-fn-entity
  "Validate that an entity exists for a custom transaction function."
  [ident op entity]
  (when-not ident
    (u/raise "Can\u2019t find entity for transaction fn " op
             {:error :transact/syntax, :operation :db.fn/call, :tx-data entity})))

(defn validate-tuple-direct-write
  "Validate that tuple attrs cannot be modified directly."
  [match? entity]
  (when-not match?
    (u/raise "Can\u2019t modify tuple attrs directly: " entity
             {:error :transact/syntax, :tx-data entity})))

(defn validate-tx-op
  "Validate that the operation is a known transaction operation."
  [op entity]
  (u/raise "Unknown operation at " entity ", expected :db/add, :db/retract, :db.fn/call, :db.fn/retractAttribute, :db.fn/retractEntity or an ident corresponding to an installed transaction function (e.g. {:db/ident <keyword> :db/fn <Ifn>}, usage of :db/ident requires {:db/unique :db.unique/identity} in schema)"
           {:error :transact/syntax, :operation op, :tx-data entity}))

(defn validate-tx-entity-type
  "Validate that the entity is a valid type (map, vector, or datom)."
  [entity]
  (u/raise "Bad entity type at " entity ", expected map, vector, or datom"
           {:error :transact/syntax, :tx-data entity}))

;; ---- Entity-id / lookup-ref validators ----

(defn validate-lookup-ref-shape
  "Validate that a lookup ref contains exactly 2 elements."
  [eid]
  (when (not= (count eid) 2)
    (u/raise "Lookup ref should contain 2 elements: " eid
             {:error :lookup-ref/syntax, :entity-id eid})))

(defn validate-lookup-ref-unique
  "Validate that a lookup ref attribute is marked as :db/unique."
  [unique? eid]
  (when-not unique?
    (u/raise "Lookup ref attribute should be marked as :db/unique: " eid
             {:error :lookup-ref/unique, :entity-id eid})))

(defn validate-entity-id-syntax
  "Validate entity id syntax: must be a number or lookup ref."
  [eid]
  (u/raise "Expected number or lookup ref for entity id, got " eid
           {:error :entity-id/syntax, :entity-id eid}))

(defn validate-map-entity-id-syntax
  "Validate :db/id in a map entity: must be a number, string, or lookup ref."
  [eid]
  (u/raise "Expected number, string or lookup ref for :db/id, got " eid
           {:error :entity-id/syntax, :entity-id eid}))

(defn validate-entity-id-exists
  "Validate that an entity id resolves to an existing entity."
  [result eid]
  (when-not result
    (u/raise "Nothing found for entity id " eid
             {:error     :entity-id/missing
              :entity-id eid})))

(defn validate-reverse-ref-attr
  "Validate that a reverse-ref attribute is a keyword."
  [attr]
  (when-not (keyword? attr)
    (u/raise "Bad entity attribute: " attr ", expected keyword"
             {:error :transact/syntax, :attribute attr})))

(defn validate-reverse-ref-type
  "Validate that a reverse attribute has :db/valueType :db.type/ref in schema."
  [ref? a eid vs]
  (when-not ref?
    (u/raise "Bad attribute " a ": reverse attribute name requires {:db/valueType :db.type/ref} in schema"
             {:error   :transact/syntax, :attribute a,
              :context {:db/id eid, a vs}})))

;; ---- Finalize-phase consistency validators ----

(defn validate-value-tempids
  "Validate that all tempids used as ref values were also used as entity ids.
   unused is a collection of tempid values that were never added as entities."
  [unused]
  (when (seq unused)
    (u/raise "Tempids used only as value in transaction: " (sort unused)
             {:error :transact/syntax, :tempids unused})))

(defn validate-upsert-retry-conflict
  "Validate that a tempid does not conflict during upsert retry.
   Raised when a tempid resolves to two different eids across retries."
  [eid tempid upserted-eid]
  (when eid
    (u/raise "Conflicting upsert: " tempid " resolves"
             " both to " upserted-eid " and " eid
             {:error :transact/upsert})))

(defn validate-upsert-conflict
  "Validate that an upserted eid does not conflict with an existing resolution.
   Raised when no unprocessed tempid is available to retry."
  [tempid e upserted-eid entity]
  (when-not tempid
    (u/raise "Conflicting upsert: " e " resolves to " upserted-eid
             " via " entity {:error :transact/upsert})))

(defn validate-tx-data-shape
  "Validate that tx-data is nil or a sequential collection."
  [tx-data]
  (when-not (or (nil? tx-data)
                (sequential? tx-data))
    (u/raise "Bad transaction data " tx-data ", expected sequential collection"
             {:error :transact/syntax, :tx-data tx-data})))

(defn validate-attr-deletable
  "Validate that an attribute can be deleted (no datoms exist for it)."
  [populated?]
  (when populated?
    (u/raise "Cannot delete attribute with datoms" {})))

(defn validate-datom-list
  "Validate that every element in a collection is a Datom."
  [datoms]
  (when-some [not-datom (first (drop-while d/datom? datoms))]
    (u/raise "init-db expects list of Datoms, got " (type not-datom)
             {:error :init-db})))
