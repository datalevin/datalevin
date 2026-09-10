;;
;; Copyright (c) Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0/)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns ^:no-doc datalevin.custom-datalog
  "Custom Datalog references, payload resolution, and logical value ordering."
  (:require [datalevin.bits :as b]
            [datalevin.constants :as c]
            [datalevin.custom-data :as custom]
            [datalevin.custom-value :as cv]
            [datalevin.datom :as d]
            [datalevin.interface :as i]
            [datalevin.lmdb :as l]
            [datalevin.udf :as udf]
            [datalevin.util :refer [raise]])
  (:import [java.util Arrays HashMap IdentityHashMap]
           [java.nio ByteBuffer]
           [datalevin.bits Indexable Retrieved CustomReference]
           [datalevin.lmdb DatomKVTxData]))

;; AVG adds a four-byte attribute ID and its usual two-byte inline trailer.
(def ^:const reference-budget (- c/+max-key-size+ 6))

(defn custom-type? [t]
  (and (qualified-keyword? t) (not (c/datalog-value-types t))))

(defn custom-schema? [schema]
  (boolean (some #(custom-type? (:db/valueType %)) (vals schema))))

(defn validate-schema! [kv schema]
  (doseq [[attr props] schema
          :let [t (:db/valueType props)] :when (custom-type? t)]
    (try
      (custom/resolve-type kv t)
      (catch clojure.lang.ExceptionInfo e
        (if (= :custom-type/not-found (:error (ex-data e)))
          (throw (ex-info
                   (str "Bad attribute specification for "
                        (pr-str {attr {:db/valueType t}}) ": " (ex-message e))
                   {:error :schema/validation :attribute attr
                    :key :db/valueType :value t}
                   e))
          (throw e))))
    (when (:db/fulltext props)
      (raise "Custom attributes do not support fulltext indexing"
             {:error :schema/validation :attribute attr :key :db/fulltext})))
  schema)

(defn initialize! [kv schema]
  (when (custom-schema? schema) (cv/open-store! kv))
  schema)

(defn indexable
  "Build an AVG indexable whose inline value is a shared custom reference."
  [e aid ^bytes ref]
  (Indexable. e aid (CustomReference. ref) c/type-custom
              (Arrays/copyOfRange ref 1 (alength ref)) c/normal))

(deftype ReadContext [info rtx ^HashMap types ^HashMap readers])

;; Do not convey native transactions through futures or escaped bound-fns.
(def ^ThreadLocal read-context (ThreadLocal.))

(defmacro with-snapshot [kv & body]
  `(let [kv# ~kv
         rtx# (when-not (l/writing? kv#) (i/get-rtx kv#))
         previous# (.get read-context)]
     (try
       (when rtx#
         (.set read-context (ReadContext. (i/kv-info kv#) rtx# (HashMap.) (HashMap.))))
       ~@body
       (finally
         (.set read-context previous#)
         (when rtx# (i/return-rtx kv# rtx#))))))

(defn- context-for [kv]
  (let [^ReadContext context (.get read-context)]
    (when (and context (not (l/writing? kv))
               (identical? (i/kv-info kv) (.-info context)))
      context)))

(defn- snapshot-type [kv type-name ^ReadContext context]
  (let [registry (get-in @(.-info context) [:runtime-opts :udf-registry])
        generation (when registry (udf/generation registry))
        ^HashMap types (.-types context)
        cached (.get types type-name)]
    (if (and cached (identical? registry (:registry cached))
             (= generation (:generation cached)))
      (:type cached)
      (let [type (custom/resolve-type-at kv type-name (.-rtx context))]
        (.put types type-name {:registry registry :generation generation :type type})
        type))))

(defn descriptor [kv type-name]
  (let [type (if-let [context (context-for kv)]
               (snapshot-type kv type-name context)
               (custom/resolve-type kv type-name))]
    {:custom/type type
     :custom/indexable
     (fn [e aid value id]
       (indexable e aid
                  (cond
                    (identical? value c/v0) (byte-array [(byte c/type-custom) 0])
                    (identical? value c/vmax) (byte-array [(byte c/type-custom) -1])
                    :else (cv/reference (cv/order-prefix type value reference-budget)
                                        (or id cv/min-id)))))}))

(defn- type-for-aid
  "Read the type declaration in the index reader's snapshot. Cache only the
  attribute name; a rename or a different snapshot falls back to the schema."
  [kv aid rtx]
  (let [cache (:custom-type-cache @(i/kv-info kv))
        attr (get-in @cache [:datalog-attrs aid])
        props (when attr (custom/metadata-value-at kv rtx c/schema attr :attr))
        [attr props] (if (= aid (:db/aid props))
                       [attr props]
                       (first (filter #(= aid (:db/aid (second %)))
                                      (custom/metadata-range-at kv rtx c/schema [:all] :attr))))]
    (when-not (custom-type? (:db/valueType props))
      (raise "Missing custom attribute declaration"
             {:error :custom-type/missing-attribute :aid aid}))
    (when-not (l/writing? kv)
      (swap! cache assoc-in [:datalog-attrs aid] attr))
    (if-let [context (context-for kv)]
      (snapshot-type kv (:db/valueType props) context)
      (custom/resolve-type-at kv (:db/valueType props) rtx))))

(defn- snapshot-reader [kv aid ^ReadContext context]
  (let [registry (get-in @(.-info context) [:runtime-opts :udf-registry])
        generation (when registry (udf/generation registry))
        ^HashMap readers (.-readers context)
        cached (.get readers aid)]
    (if (and cached (identical? registry (:registry cached))
             (= generation (:generation cached)))
      (:read cached)
      (let [rtx (.-rtx context)
            read (cv/value-reader kv (type-for-aid kv aid rtx) rtx)]
        (.put readers aid {:registry registry :generation generation :read read})
        read))))

(defn read-value [kv aid ^CustomReference value]
  (if-let [context (context-for kv)]
    ((snapshot-reader kv aid context) (.-reference value))
    ;; Writers must observe staged schema changes. Readers outside an eager
    ;; scan get a short-lived context, never one from another environment.
    (if (l/writing? kv)
      (let [rtx @(l/write-txn kv)]
        (cv/read-value-at kv (type-for-aid kv aid rtx) (.-reference value) rtx))
      (with-snapshot kv
        ((snapshot-reader kv aid (.get read-context)) (.-reference value))))))

(defn copy-kv
  "Detach a cursor row before schema or payload lookups reuse native buffers."
  [entry]
  (let [k (ByteBuffer/wrap ^bytes (b/read-buffer (l/k entry) :raw))
        v (ByteBuffer/wrap ^bytes (b/read-buffer (l/v entry) :raw))]
    (reify l/IKV
      (k [_] (.duplicate k))
      (v [_] (.duplicate v)))))

(defn match-reference
  "Find a complete value in one entity/attribute order bucket."
  [kv e aid type value]
  (let [prefix (cv/order-prefix type value reference-budget)]
    (i/list-range-some
     kv c/eav
     (fn [entry]
       (let [^Retrieved r (b/read-buffer (l/v entry) :avg)
             ^CustomReference v (.-v r)
             ref (.-reference v)]
         (when (= value (cv/read-value kv type ref)) ref)))
     [:closed e e] :id
     [:closed (indexable nil aid (cv/reference prefix cv/min-id))
      (indexable nil aid (cv/reference prefix cv/max-id))] :avg true)))

(defn put-datom! [kv e aid type value]
  (let [payload ((:serialize type) value)
        prefix (cv/order-prefix type value reference-budget)
        old (match-reference kv e aid type value)
        {:keys [id txs]} (if old
                          (let [id (cv/reference-id old)]
                            {:id id :txs [(l/kv-tx :put c/custom-values id payload :id :raw)]})
                          (cv/allocate-payload kv payload))
        ref (or old (cv/reference prefix id))]
    (cv/transact! kv (into [(DatomKVTxData. (long e)
                                           (b/indexable-bytes (indexable e aid ref))
                                           true false)] txs))))

(defn delete-datom! [kv e aid type value]
  (when-let [ref (match-reference kv e aid type value)]
    (cv/transact! kv [(DatomKVTxData. (long e)
                                     (b/indexable-bytes (indexable e aid ref))
                                     false false)
                     (cv/delete-payload-tx ref)])))

(defn exact-entities
  "Resolve exact AV matches across all IDs in the value's order bucket."
  [kv aid descriptor value]
  (let [type (:custom/type descriptor)
        prefix (cv/order-prefix type value reference-budget)]
    (i/list-range-keep
     kv c/ave
     (fn [entry]
       (let [e (b/read-buffer (l/v entry) :id)
             ^Retrieved r (b/read-buffer (l/k entry) :avg)
             ^CustomReference v (.-v r)]
         (when (= value (cv/read-value kv type (.-reference v))) e)))
     [:closed (indexable nil aid (cv/reference prefix cv/min-id))
      (indexable nil aid (cv/reference prefix cv/max-id))] :avg
     [:all] :id true)))

(defn order-comparator
  "Compare range endpoints by order bucket, without complete-value tie breaks.
  The comparator belongs to a transaction overlay or a single range operation.
  Immutable type definitions and stable values need resolving/encoding only once;
  new UDF bindings or a replaced native writer invalidate the cached keys."
  [kv schema-fn]
  (let [types (HashMap.)]
    (fn [a x y]
      (if (or (nil? x) (nil? y))
        0
        (if-let [t (let [t (:db/valueType ((schema-fn) a))]
                     (when (custom-type? t) t))]
          (cond
            (= x y) 0
            (or (identical? x c/v0) (identical? y c/vmax)) -1
            (or (identical? x c/vmax) (identical? y c/v0)) 1
            :else
            (let [registry (get-in @(i/kv-info kv) [:runtime-opts :udf-registry])
                  generation (udf/generation registry)
                  txn (when (l/writing? kv) @(l/write-txn kv))
                  cached (.get types t)
                  entry (if (and cached (identical? registry (:registry cached))
                                 (= generation (:generation cached))
                                 (identical? txn (:txn cached)))
                          cached
                          (let [entry {:registry registry :generation generation :txn txn
                                       :type (custom/resolve-type kv t)
                                       :keys (IdentityHashMap.)}]
                            (.put types t entry)
                            entry))
                  type (:type entry)
                  ^IdentityHashMap keys (:keys entry)
                  prefix (fn [v]
                           (or (.get keys v)
                               (let [key (cv/order-prefix type v reference-budget)]
                                 (.put keys v key)
                                 key)))]
              (Arrays/compareUnsigned ^bytes (prefix x) ^bytes (prefix y))))
          (d/compare-with-type x y))))))

(defn value-comparator
  "Order complete values by their native order prefix, then by a stable
  transaction-local ordinal. Equal values share an ordinal; hash collisions
  and non-Comparable application objects never collapse distinct entries."
  [kv schema-fn]
  (let [order (order-comparator kv schema-fn)
        ordinals (HashMap.)
        ordinal (fn [a v]
                  (let [k [a v]]
                    (or (.get ordinals k)
                        (let [n (long (.size ordinals))]
                          (.put ordinals k n)
                          n))))]
    (fn [a x y]
      (let [n (long (order a x y))]
        (if (and (zero? n) (some? x) (some? y) (not= x y)
                 (custom-type? (:db/valueType ((schema-fn) a))))
          (Long/compare (long (ordinal a x)) (long (ordinal a y)))
          n)))))
