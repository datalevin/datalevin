(ns datalevin.test.codec.cbor
  (:require
   [clojure.java.io :as io]
   [clojure.string :as str]
   [clojure.test :refer [deftest is testing]]
  [datalevin.codec.cbor :as cbor])
  (:import
   [datalevin.codec DLCbor$CodecException DLCbor$ErrorCode]
   [java.math BigDecimal]
   [java.net URI]
   [java.nio ByteBuffer]
   [java.util Arrays LinkedHashSet UUID]))

(defn- hex->bytes ^bytes [^String value]
  (let [length (.length value)]
    (assert (even? length) (str "Odd hex length: " value))
    (let [result (byte-array (quot length 2))]
      (dotimes [index (alength result)]
        (let [offset (* 2 index)
              high   (Character/digit (.charAt value offset) 16)
              low    (Character/digit (.charAt value (inc offset)) 16)]
          (assert (and (not= -1 high) (not= -1 low))
                  (str "Invalid hex: " value))
          (aset-byte result index (unchecked-byte (bit-or (bit-shift-left high 4)
                                                          low)))))
      result)))

(defn- golden-rows []
  (with-open [reader (io/reader
                      (io/resource
                       "datalevin/cbor/v1/golden-vectors.tsv"))]
    (->> (line-seq reader)
         (remove #(or (str/blank? %) (str/starts-with? % "#")))
         (mapv #(str/split % #"\t" -1)))))

(def fixture-values
  {"null"                      nil
   "false"                     false
   "true"                      true
   "uint-0"                    0
   "uint-23"                   23
   "uint-24"                   24
   "uint-255"                  255
   "uint-256"                  256
   "uint-65535"                65535
   "uint-65536"                65536
   "uint-u32-max"              4294967295
   "uint-u32-plus-1"           4294967296
   "int64-max"                 Long/MAX_VALUE
   "nint-1"                    -1
   "nint-24"                   -24
   "nint-25"                   -25
   "int64-min"                 Long/MIN_VALUE
   "bigint-positive"           9223372036854775808N
   "bigint-negative"           -9223372036854775809N
   "float32-one-half"          (float 1.5)
   "float32-negative-zero"     (Float/intBitsToFloat (unchecked-int 0x80000000))
   "float32-positive-infinity" Float/POSITIVE_INFINITY
   "float32-nan"               Float/NaN
   "float64-one-half"          1.5
   "float64-negative-zero"     (Double/longBitsToDouble
                                 (unchecked-long 0x8000000000000000))
   "float64-negative-infinity" Double/NEGATIVE_INFINITY
   "float64-nan"               Double/NaN
   "bytes-empty"               (byte-array 0)
   "bytes-two"                 (byte-array [(byte 0) (unchecked-byte 0xff)])
   "text-empty"                ""
   "text-ascii"                "hello"
   "text-lambda"               "λ"
   "text-emoji"                "😀"
   "vector-empty"              []
   "vector-mixed"              [1 -1 "a"]
   "map-empty"                 {}
   "map-length-first"          (array-map "aa" 2 "b" 1)
   "map-arbitrary-keys"        (array-map "a" "A" 10 "ten")
   "set-mixed"                 #{"a" -1 10}
   "decimal-zero"              0M
   "decimal-one-half"          1.5M
   "decimal-thousand"          (BigDecimal. "1E+3")
   "ratio-one-third"           1/3
   "uri-https"                 (URI. "https://example.com")
   "uuid-sequence"             (UUID/fromString
                                 "00010203-0405-0607-0809-0a0b0c0d0e0f")})

(defn- value=
  [left right]
  (cond
    (and (bytes? left) (bytes? right))
    (Arrays/equals ^bytes left ^bytes right)

    (and (instance? Float left) (instance? Float right))
    (= (Float/floatToIntBits (float left))
       (Float/floatToIntBits (float right)))

    (and (instance? Double left) (instance? Double right))
    (= (Double/doubleToLongBits (double left))
       (Double/doubleToLongBits (double right)))

    (and (map? left) (map? right))
    (and (= (count left) (count right))
         (every? (fn [[key value]]
                   (and (contains? right key)
                        (value= value (get right key))))
                 left))

    (and (sequential? left) (sequential? right))
    (and (= (count left) (count right))
         (every? true? (map value= left right)))

    :else
    (= left right)))

(defn- error-code [f]
  (try
    (f)
    nil
    (catch DLCbor$CodecException exception
      (.code exception))))

(deftest shared-golden-corpus-test
  (let [rows (golden-rows)]
    (is (= 45 (count rows)))
    (doseq [[id _profiles _type _diagnostic canonical-hex storage-hex
             _clojure _rust :as row] rows]
      (testing id
        (is (= 8 (count row)))
        (is (contains? fixture-values id))
        (let [value            (get fixture-values id)
              canonical-bytes (hex->bytes canonical-hex)
              storage-bytes   (hex->bytes storage-hex)
              encoded         (cbor/encode value)
              direct          (ByteBuffer/allocateDirect
                               (cbor/encoded-size value))]
          (is (Arrays/equals canonical-bytes encoded))
          (is (= (alength canonical-bytes)
                 (cbor/write-item! direct value)))
          (.flip direct)
          (is (value= value (cbor/decode direct)))
          (let [decoded (cbor/decode canonical-bytes)]
            (is (value= value decoded))
            (is (Arrays/equals canonical-bytes (cbor/encode decoded))))
          (is (Arrays/equals storage-bytes (cbor/encode-storage value)))
          (is (value= value (cbor/decode-storage storage-bytes))))))))

(deftest normalization-test
  (testing "JVM fixed-width integer wrappers normalize to Long"
    (doseq [value [(byte 1) (short 1) (int 1) (long 1)]]
      (is (Arrays/equals (byte-array [(byte 1)]) (cbor/encode value)))
      (is (instance? Long (cbor/decode (cbor/encode value))))))
  (testing "BigDecimal scale is not semantic"
    (is (Arrays/equals (cbor/encode 1M)
                       (cbor/encode 1.0M)))
    (is (Arrays/equals (cbor/encode 1M)
                       (cbor/encode 1.00M))))
  (testing "map and set iteration order is not canonical identity"
    (is (Arrays/equals (cbor/encode (array-map "aa" 2 "b" 1))
                       (cbor/encode (array-map "b" 1 "aa" 2))))
    (let [left  (doto (LinkedHashSet.) (.add 10) (.add -1))
          right (doto (LinkedHashSet.) (.add -1) (.add 10))]
      (is (Arrays/equals (cbor/encode left) (cbor/encode right)))))
  (testing "float width and signed zero remain semantic"
    (is (not (Arrays/equals (cbor/encode (float 1.5))
                            (cbor/encode (double 1.5)))))
    (is (not (Arrays/equals (cbor/encode 0.0)
                            (cbor/encode -0.0))))))

(deftest fast-mode-test
  (let [value   (array-map "aa" 2 "b" 1)
        encoded (cbor/encode value cbor/fast)]
    (is (Arrays/equals (hex->bytes "a262616102616201") encoded))
    (is (value= value (cbor/decode encoded false)))
    (is (Arrays/equals (hex->bytes "a261620162616102")
                       (cbor/encode (cbor/decode encoded false))))))

(deftest malformed-input-test
  (is (= DLCbor$ErrorCode/NON_SHORTEST
         (error-code #(cbor/decode (hex->bytes "1817")))))
  (is (= DLCbor$ErrorCode/TRAILING_BYTES
         (error-code #(cbor/decode (hex->bytes "0000")))))
  (is (= DLCbor$ErrorCode/INDEFINITE_LENGTH
         (error-code #(cbor/decode (hex->bytes "9fff")))))
  (is (= DLCbor$ErrorCode/INVALID_UTF8
         (error-code #(cbor/decode (hex->bytes "61ff")))))
  (is (= DLCbor$ErrorCode/NON_CANONICAL
         (error-code #(cbor/decode
                       (hex->bytes "a262616101616202")))))
  (is (= DLCbor$ErrorCode/DUPLICATE_KEY
         (error-code #(cbor/decode (hex->bytes "a201000101")))))
  (is (= DLCbor$ErrorCode/UNESCAPED_TYPED_HEADER
         (error-code #(cbor/decode-storage (hex->bytes "f6")))))
  (is (= DLCbor$ErrorCode/UNNECESSARY_STORAGE_ESCAPE
         (error-code #(cbor/decode-storage (hex->bytes "ff00")))))
  (is (= DLCbor$ErrorCode/INVALID_UNICODE
         (error-code #(cbor/encode (String. (char-array [(char 0xd800)])))))))
