(ns datalevin.migration-kv-codec
  (:require [datalevin.bits :as b])
  (:import [java.util Arrays]))

(defn- decode-index-value
  [^bytes bytes type]
  (case type
    :data [(b/deserialize bytes) :data]
    :auto (or (try
                (let [value (b/deserialize bytes)]
                  ;; Old DBIs did not record per-operation types. Accept only
                  ;; complete, canonical Nippy encodings; typed bytes and data
                  ;; with trailing bytes must remain byte-for-byte intact.
                  (when (Arrays/equals bytes ^bytes (b/serialize value))
                    [value :data]))
                (catch Exception _ nil))
              [bytes :raw])
    [bytes :raw]))

(defn entry-encoder
  [opts overrides]
  (let [dupsort? (or (:dupsort? opts) (contains? (:flags opts) :dupsort))
        key-type (get overrides :key-type (or (:key-type opts) :auto))
        val-type (get overrides :val-type
                      (or (:val-type opts) (if dupsort? :auto :raw)))]
    (fn [[k v]]
      (let [[k kt] (decode-index-value k key-type)
            [v vt] (decode-index-value v val-type)]
        [k v kt vt]))))
