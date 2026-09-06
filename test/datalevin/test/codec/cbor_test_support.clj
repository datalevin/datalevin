;;
;; Copyright (c) Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns datalevin.test.codec.cbor-test-support
  (:require
   [clojure.java.io :as io]
   [clojure.string :as str]
   [clojure.test.check.generators :as gen]
   [datalevin.codec.cbor :as cbor])
  (:import
   [clojure.lang Keyword MapEntry PersistentQueue Symbol]
   [datalevin.codec DLCbor DLCbor$ExtensionValue DLCbor$Limits
    DLCbor$MapValue DLCbor$SetValue]
   [java.math BigDecimal BigInteger]
   [java.net URI]
   [java.nio ByteBuffer]
   [java.util Arrays Date List Map Objects Set UUID]
   [java.util.regex Pattern]))

(defn collection-identity-rows
  "Returns shared valid collections, including values host equality merges."
  []
  (with-open [reader (io/reader
                      (io/resource
                       "datalevin/cbor/v1/collection-identity-vectors.tsv"))]
    (->> (line-seq reader)
         (remove #(or (str/blank? %) (str/starts-with? % "#")))
         (mapv #(str/split % #"\t" -1)))))

(defn map-value
  "Builds a lossless DL-CBOR map from pairs without applying host equality."
  ^DLCbor$MapValue [entries]
  (DLCbor$MapValue.
   (mapv (fn [[key value]] (MapEntry/create key value)) entries)))

(defn collection-identity-malformed-rows
  "Returns shared duplicate cases that exercise lossless collection decoding."
  []
  (with-open [reader (io/reader
                      (io/resource
                       (str "datalevin/cbor/v1/"
                            "collection-identity-malformed-vectors.tsv")))]
    (->> (line-seq reader)
         (remove #(or (str/blank? %) (str/starts-with? % "#")))
         (mapv #(str/split % #"\t" -1)))))

(defn- unique-by-bytes [key-fn values]
  (vec (vals (into {} (map (fn [value]
                            [(vec (cbor/encode (key-fn value))) value])) values))))

(defn malformed-rows
  "Returns the shared malformed DL-CBOR corpus as five-column rows."
  []
  (with-open [reader (io/reader
                      (io/resource
                       "datalevin/cbor/v1/malformed-vectors.tsv"))]
    (->> (line-seq reader)
         (remove #(or (str/blank? %) (str/starts-with? % "#")))
         (mapv #(str/split % #"\t" -1)))))

(defn draft-extension-malformed-rows
  "Returns malformed compact scalars and draft tagged-extension cases."
  []
  (with-open [reader (io/reader
                      (io/resource
                       (str "datalevin/cbor/v1/"
                            "draft-extension-malformed-vectors.tsv")))]
    (->> (line-seq reader)
         (remove #(or (str/blank? %) (str/starts-with? % "#")))
         (mapv #(str/split % #"\t" -1)))))

(defn- malformed-limits [operation]
  (case operation
    "limit-input"      (DLCbor$Limits. 4 256 1000000 (* 16 1024 1024) 4096)
    "limit-depth"      (DLCbor$Limits. (* 64 1024 1024) 2 1000000
                                        (* 16 1024 1024) 4096)
    "limit-collection" (DLCbor$Limits. (* 64 1024 1024) 256 2
                                        (* 16 1024 1024) 4096)
    "limit-string"     (DLCbor$Limits. (* 64 1024 1024) 256 1000000 2 4096)
    "limit-bignum"     (DLCbor$Limits. (* 64 1024 1024) 256 1000000 64 8)
    "limit-extension"  (DLCbor$Limits. (* 64 1024 1024) 256 1000000
                                             (* 16 1024 1024) 4096 4)))

(defn decode-malformed
  "Decodes a malformed-corpus input under its named operation and limits."
  [operation ^bytes input]
  (case operation
    "canonical" (cbor/decode input)
    "fast"      (cbor/decode input false)
    "storage"   (cbor/decode-storage input)
    (DLCbor/decode (ByteBuffer/wrap input) true
                   (malformed-limits operation))))

(def ^:private long-gen
  (gen/frequency
   [[1 (gen/elements [Long/MIN_VALUE -4294967297 -25 -24 -1 0 23 24
                      255 256 65535 65536 4294967295 4294967296
                      Long/MAX_VALUE])]
    [9 gen/large-integer]]))

(def ^:private ^BigInteger beyond-long-magnitude
  (BigInteger. "9223372036854775808"))

(def ^:private bigint-gen
  (gen/fmap
   (fn [[negative? offset]]
     (let [^BigInteger magnitude
           (.add beyond-long-magnitude (BigInteger/valueOf (long offset)))]
       (if negative? (.negate magnitude) magnitude)))
   (gen/tuple gen/boolean (gen/choose 0 1000000))))

(def ^:private int32-gen
  (gen/choose Integer/MIN_VALUE Integer/MAX_VALUE))

(def ^:private float-gen
  (gen/fmap #(Float/intBitsToFloat (int %)) int32-gen))

(def ^:private double-gen
  (gen/fmap
   (fn [[high low]]
     (Double/longBitsToDouble
      (bit-or (bit-shift-left (long high) 32)
              (bit-and (long low) 0xffffffff))))
   (gen/tuple int32-gen int32-gen)))

(def ^:private unicode-scalar-gen
  (gen/frequency
   [[12 (gen/choose 0 0x7f)]
    [4 (gen/choose 0x80 0xd7ff)]
    [4 (gen/choose 0xe000 0xffff)]
    [1 (gen/choose 0x10000 0x10ffff)]]))

(def ^:private string-gen
  (gen/fmap
   (fn [code-points]
     (let [builder (StringBuilder.)]
       (doseq [code-point code-points]
         (.appendCodePoint builder (int code-point)))
       (.toString builder)))
   (gen/vector unicode-scalar-gen 0 32)))

(def bytes-gen
  "Generated byte arrays bounded for codec and malformed-input properties."
  (gen/fmap byte-array (gen/vector gen/byte 0 64)))

(def ^:private instant-gen
  (gen/fmap #(Date. (long %)) long-gen))

(def ^:private char-array-gen
  (gen/fmap #(char-array (map char %))
            (gen/vector (gen/choose 0 0xffff) 0 32)))

(def ^:private short-array-gen
  (gen/fmap short-array
            (gen/vector (gen/choose Short/MIN_VALUE Short/MAX_VALUE) 0 32)))

(def ^:private int-array-gen
  (gen/fmap int-array (gen/vector int32-gen 0 32)))

(def ^:private long-array-gen
  (gen/fmap long-array (gen/vector long-gen 0 32)))

(def ^:private float-array-gen
  (gen/fmap float-array (gen/vector float-gen 0 32)))

(def ^:private double-array-gen
  (gen/fmap double-array (gen/vector double-gen 0 32)))

(def ^:private decimal-gen
  (gen/fmap
   (fn [[unscaled scale]]
     (BigDecimal. (BigInteger/valueOf (long unscaled)) (int scale)))
   (gen/tuple gen/large-integer (gen/choose -32 32))))

(def ^:private ratio-gen
  (gen/fmap
   (fn [[numerator denominator]]
     (/ (long numerator) (long denominator)))
   (gen/tuple (gen/choose -1000000 1000000)
              (gen/choose 1 1000000))))

(def ^:private uuid-gen
  (gen/fmap (fn [[high low]] (UUID. (long high) (long low)))
            (gen/tuple long-gen long-gen)))

(def ^:private uri-gen
  (gen/fmap #(URI/create (str "https://example.test/" %))
            gen/string-alphanumeric))

(def ^:private named-part-gen
  (gen/frequency
   [[8 string-gen]
    [2 (gen/elements ["" "a\u0000b" "ns/name" "λ😀"
                      (apply str (repeat 127 "x"))
                      (apply str (repeat 128 "x"))])]]))

(def ^:private keyword-gen
  (gen/fmap
   (fn [[qualified? namespace name]]
     (Keyword/intern (when qualified? namespace) name))
   (gen/tuple gen/boolean named-part-gen named-part-gen)))

(def ^:private symbol-gen
  (gen/fmap
   (fn [[qualified? namespace name]]
     (Symbol/intern (when qualified? namespace) name))
   (gen/tuple gen/boolean named-part-gen named-part-gen)))

(def ^:private character-gen
  (gen/fmap char (gen/choose 0 0xffff)))

(def ^:private regex-gen
  (gen/fmap (fn [[source flags]]
              (Pattern/compile source (int flags)))
            (gen/tuple gen/string-alphanumeric
                       (gen/elements [0 1 2 4 8 16 32 64 320 383]))))

(def map-key-gen
  "Generated scalar values suitable for portable map keys."
  (gen/frequency
   [[1 (gen/return nil)]
    [2 gen/boolean]
    [5 long-gen]
    [2 bigint-gen]
    [5 string-gen]
    [2 keyword-gen]
    [2 symbol-gen]
    [1 character-gen]
    [1 uuid-gen]
    [1 uri-gen]]))

(def leaf-gen
  "Generated non-collection values in the portable DL-CBOR v1 domain."
  (gen/frequency
   [[1 (gen/return nil)]
    [2 gen/boolean]
    [6 long-gen]
    [2 bigint-gen]
    [2 float-gen]
    [2 double-gen]
    [4 string-gen]
    [2 keyword-gen]
    [2 symbol-gen]
    [1 character-gen]
    [1 regex-gen]
    [3 bytes-gen]
    [2 decimal-gen]
    [2 ratio-gen]
    [1 uuid-gen]
    [1 uri-gen]
    [2 instant-gen]
    [1 char-array-gen]
    [1 short-array-gen]
    [1 int-array-gen]
    [1 long-array-gen]
    [1 float-array-gen]
    [1 double-array-gen]]))

(def set-element-gen
  "Generated values for ordinary Clojure sets; value-gen also uses holders."
  (gen/one-of [map-key-gen (gen/vector map-key-gen 0 4)]))

(def ^:private identity-boundary-gen
  (gen/elements [[] (list) PersistentQueue/EMPTY
                 (float 0.0) (float -0.0) 0.0 -0.0
                 (float 1.0) 1.0]))

(def value-gen
  "Generated recursive values in the portable DL-CBOR v1 domain."
  (gen/recursive-gen
   (fn [inner]
     (gen/frequency
      [[5 (gen/vector inner 0 6)]
       [2 (gen/fmap #(apply list %) (gen/vector inner 0 6))]
       [2 (gen/fmap #(reduce conj PersistentQueue/EMPTY %)
                    (gen/vector inner 0 6))]
       [3 (gen/map map-key-gen inner {:max-elements 6})]
       [2 (gen/set set-element-gen {:max-elements 8})]
       [2 (gen/fmap #(map-value (unique-by-bytes first %))
                    (gen/vector (gen/tuple
                                 (gen/one-of [inner identity-boundary-gen])
                                 inner) 0 10))]
       [2 (gen/fmap #(DLCbor$SetValue. (unique-by-bytes identity %))
                    (gen/vector (gen/one-of [inner identity-boundary-gen])
                                0 10))]
       [1 (gen/fmap (fn [[named? integer-id arguments]]
                      (let [^java.util.List arguments arguments]
                        (if named?
                          (let [^String type-id "org.example/generated"]
                            (DLCbor$ExtensionValue. type-id arguments))
                          (DLCbor$ExtensionValue. (long integer-id)
                                                 arguments))))
                    (gen/tuple gen/boolean
                               (gen/choose 7 1000000)
                               (gen/vector inner 0 4)))]]))
   leaf-gen))

(defn value=
  "Compares portable values while preserving array and float semantics."
  [left right]
  (cond
    (and (or (instance? Map left) (instance? DLCbor$MapValue left))
         (or (instance? Map right) (instance? DLCbor$MapValue right)))
    (Arrays/equals (cbor/encode left) (cbor/encode right))

    (and (or (instance? Set left) (instance? DLCbor$SetValue left))
         (or (instance? Set right) (instance? DLCbor$SetValue right)))
    (Arrays/equals (cbor/encode left) (cbor/encode right))

    (and (instance? DLCbor$ExtensionValue left)
         (instance? DLCbor$ExtensionValue right))
    (let [^DLCbor$ExtensionValue left  left
          ^DLCbor$ExtensionValue right right]
      (and (value= (.typeId left) (.typeId right))
           (value= (.arguments left) (.arguments right))))

    (and (some? left) (some? right)
         (.isArray ^Class (class left))
         (.isArray ^Class (class right)))
    (Objects/deepEquals left right)

    (and (instance? Float left) (instance? Float right))
    (= (Float/floatToIntBits (float left))
       (Float/floatToIntBits (float right)))

    (and (instance? Double left) (instance? Double right))
    (= (Double/doubleToLongBits (double left))
       (Double/doubleToLongBits (double right)))

    (and (instance? BigDecimal left) (instance? BigDecimal right))
    (zero? (.compareTo ^BigDecimal left ^BigDecimal right))

    (and (instance? Pattern left) (instance? Pattern right))
    (and (= (.pattern ^Pattern left) (.pattern ^Pattern right))
         (= (.flags ^Pattern left) (.flags ^Pattern right)))

    (and (instance? List left) (instance? List right))
    (and (= (count left) (count right))
         (every? true? (map value= left right)))

    (and (sequential? left) (sequential? right))
    (and (= (count left) (count right))
         (every? true? (map value= left right)))

    :else
    (= left right)))
