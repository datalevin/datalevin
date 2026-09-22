;; Copyright (c) Huahai Yang. All rights reserved.
;; Distributed under the Eclipse Public License 2.0.
(ns ^:no-doc datalevin.pull-wire
  "Connection-owned layouts for scalar prepared pull replies. Values remain
  ordinary Nippy values; only the repeated result keys are omitted."
  (:require [datalevin.native-value :as nv]
            [datalevin.prepared :as prepared]
            [datalevin.protocol.context :as context]
            [datalevin.read-encode :as enc]
            [taoensso.nippy :as nippy]
            [taoensso.nippy.impl :as impl]
            [taoensso.nippy.schema :as schema])
  (:import [clojure.lang PersistentArrayMap]
           [java.io DataInput]
           [java.nio ByteBuffer]
           [java.util LinkedHashMap]))

(deftype Layout [keys ^objects template ^long mask])

(defn layout
  "Compile at most 64 distinct keyword keys into an immutable map template."
  [keys]
  (when (and (vector? keys) (<= 1 (count keys) 64)
             (every? keyword? keys) (= (count keys) (count (distinct keys))))
    (let [n (count keys)
          template (object-array (* 2 n))]
      (dotimes [index n] (aset template (* 2 index) (nth keys index)))
      (Layout. keys template
               (if (= n 64) -1 (unchecked-dec (bit-shift-left 1 n)))))))

(def ^:private result-id (impl/coerce-custom-type-id ::result))

(defprotocol IWriter
  (begin-response! [writer])
  (select-layout! [writer layout])
  (start! [writer out])
  (finish! [writer out position mask])
  (response-written! [writer]))

(deftype Writer [^long id
                 ^:unsynchronized-mutable current
                 ^:unsynchronized-mutable sent
                 ^:unsynchronized-mutable pending]
  IWriter
  (begin-response! [_] (set! pending nil))
  (select-layout! [_ value] (set! current value))
  (start! [_ out]
    (let [^ByteBuffer out out
          ^Layout layout current]
      (.put out (unchecked-byte schema/id-prefixed-custom-md))
      (.putShort out (short result-id))
      (.putLong out id)
      (enc/write-value! out (when-not (identical? layout sent) (.-keys layout)))
      (let [position (.position out)]
        (.putLong out 0)
        position)))
  (finish! [_ out position mask]
    (.putLong ^ByteBuffer out (int position) (long mask))
    (set! pending current))
  (response-written! [_]
    ;; A failed or retried encode cannot make an unsent definition reusable.
    ;; The handler acknowledges only after the whole frame has been written.
    (when pending (set! sent pending))
    (set! pending nil)))

(defn writer [id] (Writer. (long id) nil nil nil))

(defn- read-map
  [^DataInput in ^Layout layout ^long mask]
  (when-not (zero? mask)
    (let [^objects template (.-template layout)
          full? (= mask (.-mask layout))
          ^objects entries (if full? (aclone template)
                      (object-array (* 2 (Long/bitCount mask))))]
      (loop [remaining mask, offset 0]
        (when-not (zero? remaining)
          (let [index (Long/numberOfTrailingZeros remaining)]
            (when-not full?
              (aset entries offset (aget template (* 2 index))))
            (aset entries (inc offset) (nippy/thaw-from-in! in))
            (recur (bit-and remaining (unchecked-dec remaining)) (+ offset 2)))))
      ;; Array maps are ordinary immutable Clojure maps. A bounded projection
      ;; can construct one directly without hashing the same keys on every read.
      (PersistentArrayMap. entries))))

(nippy/extend-thaw ::result
  [^DataInput in]
  (try
    (let [^LinkedHashMap handles (when (and nv/*wire-native-value* context/*context*)
                                  (context/prepared-handles context/*context*))]
      (when-not handles
        (throw (ex-info "Prepared pull requires its receiving connection" {})))
      (let [id (.readLong in)
            definition (nippy/thaw-from-in! in)
            ^Layout current (if (nil? definition)
                              (let [cached (.get handles id)]
                                (when (instance? Layout cached) cached))
                              (layout definition))
            mask (.readLong in)]
        (when-not (and (pos? id) current
                       (zero? (bit-and mask (bit-not (.-mask current)))))
          (throw (ex-info "Invalid or missing prepared pull layout" {:handle id})))
        (let [result (read-map in current mask)]
          (when definition (prepared/remember! handles id current))
          result)))
    (catch Exception e
      ;; Preserve the codec failure through Nippy's legacy-format fallback and
      ;; the client's transport retry boundary, just like native reader failures.
      (throw (ex-info "Prepared pull result decoding failed"
                      {:error :native-value/decode :type-name ::result} e)))))
