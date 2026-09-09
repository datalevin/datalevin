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
            [datalevin.util :refer [raise]])
  (:import [java.util Arrays HashMap]
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
    (custom/resolve-type kv t)
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

(defn descriptor [kv type-name]
  (let [type (custom/resolve-type kv type-name)]
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
  [kv aid]
  (let [cache (:custom-type-cache @(i/kv-info kv))
        attr (get-in @cache [:datalog-attrs aid])
        props (when attr (i/get-value kv c/schema attr :attr :data))
        [attr props] (if (= aid (:db/aid props))
                       [attr props]
                       (first (filter #(= aid (:db/aid (second %)))
                                      (i/get-range kv c/schema [:all] :attr :data))))]
    (when-not (custom-type? (:db/valueType props))
      (raise "Missing custom attribute declaration"
             {:error :custom-type/missing-attribute :aid aid}))
    (when-not (l/writing? kv)
      (swap! cache assoc-in [:datalog-attrs aid] attr))
    (custom/resolve-type kv (:db/valueType props))))

(defn read-value [kv aid ^CustomReference value]
  (cv/read-value kv (type-for-aid kv aid) (.-reference value)))

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

(defmacro with-snapshot [kv & body]
  `(let [kv# ~kv
         rtx# (when-not (l/writing? kv#) (i/get-rtx kv#))]
     (try ~@body
          (finally (when rtx# (i/return-rtx kv# rtx#))))))

(defn order-comparator
  "Compare range endpoints by order bucket, without complete-value tie breaks."
  [kv schema-fn]
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
          (let [type (custom/resolve-type kv t)]
            (Arrays/compareUnsigned
             ^bytes (cv/order-prefix type x reference-budget)
             ^bytes (cv/order-prefix type y reference-budget))))
        (d/compare-with-type x y)))))

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
