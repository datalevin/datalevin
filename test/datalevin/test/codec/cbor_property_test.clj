;;
;; Copyright (c) Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns datalevin.test.codec.cbor-property-test
  (:require
   [clojure.test.check.clojure-test :refer [defspec]]
   [clojure.test.check.generators :as gen]
   [clojure.test.check.properties :as prop]
   [datalevin.codec.cbor :as cbor]
   [datalevin.test.codec.cbor-test-support :as support])
  (:import
   [datalevin.codec DLCbor$CodecException DLCbor$ErrorCode]
   [java.nio ByteBuffer ByteOrder]
   [java.util Arrays LinkedHashMap LinkedHashSet]))

(defn- buffer-bytes ^bytes [^ByteBuffer buffer ^long length]
  (.flip buffer)
  (let [result (byte-array length)]
    (.get buffer result)
    result))

(defn- write-buffer ^bytes [value direct? little-endian?]
  (let [length (cbor/encoded-size value)
        buffer (if direct?
                 (ByteBuffer/allocateDirect length)
                 (ByteBuffer/allocate length))]
    (when little-endian?
      (.order buffer ByteOrder/LITTLE_ENDIAN))
    (let [written (cbor/write-item! buffer value)]
      (assert (= length written))
      (buffer-bytes buffer written))))

(defn- linked-map [entries]
  (let [result (LinkedHashMap.)]
    (doseq [[key value] entries]
      (.put result key value))
    result))

(defn- linked-set [values]
  (let [result (LinkedHashSet.)]
    (doseq [value values]
      (.add result value))
    result))

(defn- codec-error-code [f]
  (try
    (f)
    nil
    (catch DLCbor$CodecException exception
      (.code exception))))

(defspec canonical-roundtrip-and-buffer-equivalence-property 500
  (prop/for-all [value support/value-gen
                 little-endian? gen/boolean]
    (let [encoded (cbor/encode value)
          decoded (cbor/decode encoded)
          heap-encoded (write-buffer value false little-endian?)
          direct-encoded (write-buffer value true little-endian?)
          storage-encoded (cbor/encode-storage value)
          storage-decoded (cbor/decode-storage storage-encoded)]
      (and (= (alength ^bytes encoded) (cbor/encoded-size value))
           (Arrays/equals encoded heap-encoded)
           (Arrays/equals encoded direct-encoded)
           (support/value= value decoded)
           (support/value= value storage-decoded)
           (Arrays/equals encoded (cbor/encode decoded))))))

(defspec fast-mode-portability-property 400
  (prop/for-all [value support/value-gen]
    (let [canonical (cbor/encode value)
          fast (cbor/encode value cbor/fast)
          decoded (cbor/decode fast false)]
      (and (support/value= value decoded)
           (Arrays/equals canonical (cbor/encode decoded))))))

(defspec canonical-map-and-set-order-property 300
  (prop/for-all [map-value (gen/map support/map-key-gen support/leaf-gen
                                    {:max-elements 12})
                 set-value (gen/set support/set-element-gen
                                    {:max-elements 16})]
    (let [map-entries (vec map-value)
          set-values (vec set-value)]
      (and (Arrays/equals
            (cbor/encode (linked-map map-entries))
            (cbor/encode (linked-map (reverse map-entries))))
           (Arrays/equals
            (cbor/encode (linked-set set-values))
            (cbor/encode (linked-set (reverse set-values))))))))

(defspec truncation-and-trailing-data-property 300
  (prop/for-all [value support/value-gen
                 cut-selector gen/nat
                 trailing-byte gen/byte]
    (let [encoded (cbor/encode value)
          cut (mod (long cut-selector) (alength ^bytes encoded))
          truncated (Arrays/copyOf encoded (int cut))
          with-trailing (Arrays/copyOf encoded (inc (alength ^bytes encoded)))]
      (aset-byte with-trailing (alength ^bytes encoded) (byte trailing-byte))
      (and (= DLCbor$ErrorCode/TRUNCATED
              (codec-error-code #(cbor/decode truncated)))
           (= DLCbor$ErrorCode/TRAILING_BYTES
              (codec-error-code #(cbor/decode with-trailing)))))))

(defspec arbitrary-input-never-leaks-runtime-exceptions-property 500
  (prop/for-all [input support/bytes-gen]
    (try
      (cbor/decode input)
      true
      (catch DLCbor$CodecException _exception
        true)
      (catch Throwable _unexpected
        false))))
