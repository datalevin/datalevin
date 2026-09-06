(ns datalevin.test.codec.cbor
  (:require
   [clojure.java.io :as io]
   [clojure.string :as str]
   [clojure.test :refer [deftest is testing]]
   [datalevin.codec.cbor :as cbor]
   [datalevin.test.codec.cbor-test-support :as support])
  (:import
   [clojure.lang Keyword PersistentQueue]
   [datalevin.codec DLCbor$CodecException DLCbor$ErrorCode
    DLCbor$ExtensionValue DLCbor$MapValue DLCbor$SetValue DLCbor$TaggedValue]
   [java.math BigDecimal]
   [java.net URI]
   [java.nio ByteBuffer ByteOrder]
   [java.time Instant]
   [java.util ArrayList Arrays Collections Date IdentityHashMap LinkedHashSet
    Map$Entry Objects UUID]
   [java.util.regex Pattern]))

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

(defn- draft-extension-rows []
  (with-open [reader (io/reader
                      (io/resource
                       (str "datalevin/cbor/v1/"
                            "draft-extension-vectors.tsv")))]
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
   "bytes-subtype-collisions"   (byte-array (map unchecked-byte
                                                [251 252 254 224 225 0]))
   "bytes-length-22"           (byte-array (repeat 22 97))
   "bytes-length-23"           (byte-array (repeat 23 97))
   "boolean-array-empty"       (boolean-array 0)
   "boolean-array-false"       (boolean-array [false])
   "boolean-array-true"        (boolean-array [true])
   "boolean-array-mixed"       (boolean-array [true false true])
   "boolean-array-7"           (boolean-array (repeat 7 true))
   "boolean-array-8"           (boolean-array (repeat 8 true))
   "boolean-array-9"           (boolean-array (concat (repeat 8 false) [true]))
   "boolean-array-16"          (boolean-array (take 16 (cycle [true false])))
   "boolean-array-17"          (boolean-array (concat [true] (repeat 15 false)
                                                    [true]))
   "boolean-array-168"         (boolean-array 168)
   "boolean-array-169"         (boolean-array (repeat 169 true))
   "boolean-array-2032"        (boolean-array 2032)
   "boolean-array-2033"        (boolean-array (concat (repeat 2032 false) [true]))
   "boolean-array-nested"      [(boolean-array [true false true]) []]
   "boolean-array-map-keys"    (array-map [] 0 (boolean-array 0) 1
                                         (byte-array [-3 0]) 2)
   "boolean-array-set-members" #{[] (boolean-array 0)}
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
                                 "00010203-0405-0607-0809-0a0b0c0d0e0f")
   "instant-epoch"             (Date. 0)
   "instant-positive-millis"   (Date. 1234)
   "instant-negative-millis"   (Date. -1)
   "instant-int64-min"         (Date. Long/MIN_VALUE)
   "instant-int64-max"         (Date. Long/MAX_VALUE)
   "uint16-array"              (char-array [(char 0) (char 0x03bb)
                                              (char 0xd800) (char 0xffff)])
   "int16-array"               (short-array [Short/MIN_VALUE -1 0
                                               Short/MAX_VALUE])
   "int32-array"               (int-array [Integer/MIN_VALUE -1 0
                                             Integer/MAX_VALUE])
   "int64-array"               (long-array [Long/MIN_VALUE -1 0
                                              Long/MAX_VALUE])
   "float32-array"             (float-array
                                 [(float 1.5)
                                  (Float/intBitsToFloat
                                   (unchecked-int 0x80000000))
                                  Float/POSITIVE_INFINITY
                                  (Float/intBitsToFloat
                                   (unchecked-int 0x7fc00001))])
   "float64-array"             (double-array
                                 [1.5
                                  (Double/longBitsToDouble
                                   (unchecked-long 0x8000000000000000))
                                  Double/NEGATIVE_INFINITY
                                  (Double/longBitsToDouble
                                   0x7ff8000000000001)])
   "keyword-unqualified"       :a
   "keyword-qualified"         :ns/a
   "keyword-empty-namespace"   (Keyword/intern "" "a")
   "symbol-unqualified"        (symbol "a")
   "symbol-qualified"          (symbol "ns" "a")
   "keyword-qualified-example" :user/id
   "keyword-empty-name"        (Keyword/intern nil "")
   "keyword-qualified-empty-name" (Keyword/intern "ns" "")
   "keyword-empty-parts"       (Keyword/intern "" "")
   "keyword-unqualified-slash" (Keyword/intern nil "user/id")
   "keyword-embedded-nul"      (Keyword/intern "n\u0000s" "a\u0000b")
   "keyword-unqualified-nul"   (Keyword/intern nil "a\u0000b")
   "keyword-unicode"           (Keyword/intern "λ" "😀")
   "keyword-namespace-127"     (Keyword/intern (apply str (repeat 127 "x")) "a")
   "keyword-namespace-128"     (Keyword/intern (apply str (repeat 128 "x")) "a")
   "symbol-empty-name"         (symbol nil "")
   "symbol-empty-namespace"    (symbol "" "a")
   "symbol-embedded-nul"       (symbol "n\u0000s" "a\u0000b")
   "symbol-unqualified-slash"  (symbol nil "user/id")
   "symbol-unicode"            (symbol "λ" "😀")
   "symbol-namespace-128"      (symbol (apply str (repeat 128 "x")) "a")
   "keyword-name-22"           (Keyword/intern nil (apply str (repeat 22 "a")))
   "keyword-name-23"           (Keyword/intern nil (apply str (repeat 23 "a")))
   "character-ascii"           (char 0x0061)
   "character-surrogate"       (char 0xd800)
   "character-zero"            (char 0)
   "character-byte-max"        (char 0xff)
   "character-two-byte-min"    (char 0x100)
   "character-max"             (char 0xffff)
   "character-low-surrogate"   (char 0xdc00)
   "list-empty"                ()
   "list-mixed"                (list 1 "a")
   "queue-mixed"               (into PersistentQueue/EMPTY [1 "a"])
   "regex-no-flags"            (Pattern/compile "a+")
   "regex-empty"               (Pattern/compile "")
   "regex-unicode"             (Pattern/compile "λ😀" Pattern/CASE_INSENSITIVE)
   "regex-embedded-nul"        (Pattern/compile "a\u0000b")
   "regex-source-21"           (Pattern/compile (apply str (repeat 21 "a")))
   "regex-source-22"           (Pattern/compile (apply str (repeat 22 "a")))
   "regex-unix-lines"          (Pattern/compile "a" Pattern/UNIX_LINES)
   "regex-case-insensitive"    (Pattern/compile "a"
                                                  Pattern/CASE_INSENSITIVE)
   "regex-comments"            (Pattern/compile "a" Pattern/COMMENTS)
   "regex-multiline"           (Pattern/compile "a" Pattern/MULTILINE)
   "regex-literal"             (Pattern/compile "a" Pattern/LITERAL)
   "regex-dotall"              (Pattern/compile "a" Pattern/DOTALL)
   "regex-unicode-case"        (Pattern/compile "a" Pattern/UNICODE_CASE)
   "regex-unicode-character-class"
   (Pattern/compile "\\w+" Pattern/UNICODE_CHARACTER_CLASS)
   "regex-all-supported-flags" (Pattern/compile "a" 383)
   "extension-unknown-integer" (DLCbor$ExtensionValue. 42 [1 nil])
   "extension-unknown-name"    (DLCbor$ExtensionValue.
                                 "org.example/x" [1 nil])})

(defn- value=
  [left right]
  (cond
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

    (and (instance? Pattern left) (instance? Pattern right))
    (and (= (.pattern ^Pattern left) (.pattern ^Pattern right))
         (= (.flags ^Pattern left) (.flags ^Pattern right)))

    (or (and (map? left) (map? right))
        (and (set? left) (set? right)))
    (support/value= left right)

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

(deftest packed-boolean-bit-pattern-test
  (doseq [length (range 13)
          mode [cbor/canonical cbor/fast]]
    (testing (str length " booleans, " mode)
      (is (every?
           (fn [mask]
             (let [value (boolean-array
                          (map #(bit-test mask %) (range length)))
                   width (quot (+ length 7) 8)
                   expected (byte-array
                             (map unchecked-byte
                                  (concat [(+ 0x42 width) 0xfd
                                           (mod (- 8 (mod length 8)) 8)]
                                          (take width [mask
                                                       (bit-shift-right mask 8)]))))
                   encoded (cbor/encode value mode)]
               (and (Arrays/equals expected encoded)
                    (= (alength expected) (cbor/encoded-size value mode))
                    (value= value (cbor/decode expected (= mode cbor/canonical))))))
           (range (bit-shift-left 1 length))))))
  (is (value= (boolean-array 0) (cbor/decode (hex->bytes "5802fd00") false))))

(deftest packed-boolean-buffer-test
  (doseq [length [0 1 7 8 9 16 17 63 64 65 168 169 2032 2033 65536]
          mode [cbor/canonical cbor/fast]
          direct? [false true]
          order [ByteOrder/BIG_ENDIAN ByteOrder/LITTLE_ENDIAN]]
    (testing (str length " " mode " direct=" direct? " " order)
      (let [value (boolean-array (map #(zero? (mod % 3)) (range length)))
            encoded (cbor/encode value mode)
            size (alength encoded)
            buffer (if direct?
                     (ByteBuffer/allocateDirect (+ size 6))
                     (ByteBuffer/allocate (+ size 6)))
            canonical? (= mode cbor/canonical)]
        (.order buffer order)
        (.position buffer 3)
        (.limit buffer (+ 3 size))
        (is (= size (cbor/write-item! buffer value mode)))
        (is (= (+ 3 size) (.position buffer)))
        (.position buffer 3)
        (let [input (.asReadOnlyBuffer (.slice buffer))
              bytes (byte-array size)]
          (.get (.duplicate input) bytes)
          (is (Arrays/equals encoded bytes))
          (is (value= value (cbor/decode input canonical?)))
          (is (value= value (cbor/decode buffer canonical?))))))))

(deftest packed-boolean-identity-test
  (doseq [mode [cbor/canonical cbor/fast]]
    (let [left (boolean-array [true])
          right (boolean-array [true])
          duplicate-map (support/map-value [[left 0] [right 1]])
          duplicate-set (DLCbor$SetValue. [left right])
          distinct-values [true [true] left (byte-array [-3 7 1])]
          distinct-set (DLCbor$SetValue. distinct-values)
          encoded (cbor/encode distinct-set mode)]
      (is (= DLCbor$ErrorCode/DUPLICATE_KEY
             (error-code #(cbor/encode duplicate-map mode))))
      (is (= DLCbor$ErrorCode/DUPLICATE_SET_MEMBER
             (error-code #(cbor/encode duplicate-set mode))))
      (is (= 4 (count (cbor/decode encoded (= mode cbor/canonical))))))
    (let [value (boolean-array [true false true])]
      (is (value= value (cbor/decode-storage (cbor/encode-storage value mode)
                                            (= mode cbor/canonical)))))))

(deftest shared-golden-corpus-test
  (let [rows (golden-rows)]
    (is (= 75 (count rows)))
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

(deftest draft-extension-corpus-test
  (let [rows (draft-extension-rows)]
    (is (= 50 (count rows)))
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
  (testing "Instant normalizes to millisecond Date identity"
    (let [instant (Instant/ofEpochSecond -1 999000000)]
      (is (= (Date. -1) (cbor/decode (cbor/encode instant))))
      (is (Arrays/equals (cbor/encode instant)
                         (cbor/encode (Date. -1))))))
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
                       (cbor/encode (cbor/decode encoded false)))))
  (testing "fast readers accept and writers normalize typed-array NaNs"
    (let [decoded (cbor/decode (hex->bytes "d855440100c07f") false)]
      (is (instance? (class (float-array 0)) decoded))
      (is (Float/isNaN (aget ^floats decoded 0)))
      (is (Arrays/equals (hex->bytes "d855440000c07f")
                         (cbor/encode decoded))))))

(deftest shared-malformed-corpus-test
  (let [rows (support/malformed-rows)]
    (is (= 93 (count rows)))
    (doseq [[id operation hex expected _note :as row] rows]
      (testing id
        (is (= 5 (count row)))
        (is (= (DLCbor$ErrorCode/valueOf expected)
               (error-code #(support/decode-malformed operation
                                                      (hex->bytes hex)))))))))

(deftest draft-extension-malformed-corpus-test
  (let [rows (support/draft-extension-malformed-rows)]
    (is (= 64 (count rows)))
    (doseq [[id operation hex expected _note :as row] rows]
      (testing id
        (is (= 5 (count row)))
        (is (= (DLCbor$ErrorCode/valueOf expected)
               (error-code #(support/decode-malformed operation
                                                      (hex->bytes hex)))))))))

(deftest invalid-host-unicode-test
  (is (= DLCbor$ErrorCode/INVALID_UNICODE
         (error-code #(cbor/encode (String. (char-array [(char 0xd800)]))))))
  (let [invalid (String. (char-array [(char 0xd800)]))]
    (doseq [make-name [keyword symbol]
            value [(make-name nil invalid) (make-name invalid "a")]]
      (is (= DLCbor$ErrorCode/INVALID_UNICODE
             (error-code #(cbor/encode value)))))))

(deftest named-value-buffer-boundaries-test
  (testing "length fields cross varint widths in sliced/direct buffers"
    (doseq [length [127 128 16383 16384]
            make-name [keyword symbol]
            direct? [false true]
            mode [cbor/canonical cbor/fast]]
      (let [value   (make-name (apply str (repeat length "x")) "λ\u0000/😀")
            size    (cbor/encoded-size value)
            backing (if direct?
                      (ByteBuffer/allocateDirect (+ size 11))
                      (ByteBuffer/allocate (+ size 11)))]
        (.position backing 7)
        (let [buffer (.slice backing)]
          (.order buffer java.nio.ByteOrder/LITTLE_ENDIAN)
          (is (= size (cbor/write-item! buffer value mode)))
          (.flip buffer)
          (is (support/value= value (cbor/decode buffer))))))))

(deftest tagged-byte-payload-context-test
  (doseq [[tag payload expected]
          [[2 "8000000000000000" "c2488000000000000000"]
           [3 "8000000000000000" "c3488000000000000000"]
           [37 "fbfcfee0e1000102030405060708090a"
            "d82550fbfcfee0e1000102030405060708090a"]
           [69 "fbfc" "d84542fbfc"]
           [10000 "fb61" "d9271043fefb61"]]
          mode [cbor/canonical cbor/fast]]
    (let [value (DLCbor$TaggedValue. (long tag) (hex->bytes payload))
          encoded (cbor/encode value mode)]
      (is (Arrays/equals (hex->bytes expected) encoded))
      (is (= (alength encoded) (cbor/encoded-size value)))
      (is (Arrays/equals encoded (cbor/encode (cbor/decode encoded)))))))

(deftest extension-host-validation-test
  (testing "retired tagged scalars cannot introduce a second wire spelling"
    (doseq [id [1 2 3 6]
            mode [cbor/canonical cbor/fast]]
      (is (= DLCbor$ErrorCode/INVALID_EXTENSION
             (error-code #(cbor/encode (DLCbor$ExtensionValue. (long id) ["a"])
                                      mode))))))
  (testing "extension identifiers and arguments are validated at construction"
    (is (thrown? IllegalArgumentException (DLCbor$ExtensionValue. -1 [])))
    (is (thrown? IllegalArgumentException (DLCbor$ExtensionValue. "" [])))
    (is (thrown? NullPointerException
                 (let [^String type-id nil]
                   (DLCbor$ExtensionValue. type-id []))))
    (is (thrown? NullPointerException
                 (DLCbor$ExtensionValue. 42 nil)))))

(deftest regex-extension-validation-test
  (testing "the JVM materializer rejects invalid Java Pattern syntax"
    (is (= DLCbor$ErrorCode/INVALID_REGEX
           (error-code #(cbor/decode
                         (hex->bytes "43e3005b"))))))
  (testing "regex source preserves the Unicode-scalar rule"
    (let [source (String. (char-array [(char 0xd800)]))
          pattern (Pattern/compile source Pattern/LITERAL)]
      (is (= DLCbor$ErrorCode/INVALID_UNICODE
             (error-code #(cbor/encode pattern))))))
  (testing "CANON_EQ patterns are deliberately outside the portable profile"
    (let [pattern (Pattern/compile "a" Pattern/CANON_EQ)]
      (is (= DLCbor$ErrorCode/INVALID_REGEX
             (error-code #(cbor/encode pattern)))))))

(deftest collection-identity-corpus-test
  (let [rows (support/collection-identity-rows)]
    (is (= 15 (count rows)))
    (doseq [[id kind size canonical-hex fast-hex _note] rows
            [canonical? hex] [[true canonical-hex] [false fast-hex]]
            input-kind [:array :heap :direct :read-only]]
      (testing (str id " " canonical? " " input-kind)
        (let [encoded (hex->bytes hex)
              expected (hex->bytes canonical-hex)
              input (if (= input-kind :array)
                      encoded
                      (let [buffer (if (= input-kind :heap)
                                     (ByteBuffer/allocate (+ 6 (alength encoded)))
                                     (ByteBuffer/allocateDirect
                                      (+ 6 (alength encoded))))]
                        (.position buffer 3)
                        (.put buffer encoded)
                        (.limit buffer (.position buffer))
                        (.position buffer 3)
                        (if (= input-kind :read-only)
                          (.asReadOnlyBuffer (.slice buffer))
                          buffer)))
              decoded (cbor/decode input canonical?)
              items (if (= kind "map")
                      (.entries ^DLCbor$MapValue decoded)
                      (.members ^DLCbor$SetValue decoded))
              mode (if canonical? cbor/canonical cbor/fast)
              output (ByteBuffer/allocateDirect (cbor/encoded-size decoded mode))]
          (is (= (Long/parseLong size) (count items)))
          (is (Arrays/equals expected (cbor/encode decoded)))
          (is (= (alength expected) (cbor/write-item! output decoded mode)))
          (.flip output)
          (is (Arrays/equals expected
                             (cbor/encode (cbor/decode output canonical?))))
          (is (Arrays/equals
               expected
               (cbor/encode (cbor/decode-storage
                             (cbor/encode-storage decoded mode) canonical?)))))))))

(deftest collection-identity-duplicate-test
  (let [rows (support/collection-identity-malformed-rows)]
    (is (= 8 (count rows)))
    (doseq [[id operation hex expected _note] rows]
      (testing id
        (is (= (DLCbor$ErrorCode/valueOf expected)
               (error-code #(support/decode-malformed
                             operation (hex->bytes hex))))))))
  (testing "fast decoding still detects duplicate identity-equality values"
    (is (= DLCbor$ErrorCode/DUPLICATE_SET_MEMBER
           (error-code #(cbor/decode
                         (hex->bytes "d901028242fe0142fe01") false)))))
  (testing "writers reject canonical duplicates regardless of host equality"
    (let [duplicate-map (doto (IdentityHashMap.)
                          (.put (String. "a") 0)
                          (.put (String. "a") 1))
          duplicate-set (doto (Collections/newSetFromMap (IdentityHashMap.))
                          (.add (Long/valueOf "1000"))
                          (.add (Long/valueOf "1000")))]
      (is (= 2 (.size duplicate-map)))
      (is (= 2 (.size duplicate-set)))
      (doseq [mode [cbor/canonical cbor/fast]]
        (doseq [value [duplicate-map (support/map-value [[[] 0] [[] 1]])]]
          (is (= DLCbor$ErrorCode/DUPLICATE_KEY
                 (error-code #(cbor/encode value mode)))))
        (doseq [value [duplicate-set (DLCbor$SetValue. [[] []])]]
          (is (= DLCbor$ErrorCode/DUPLICATE_SET_MEMBER
                 (error-code #(cbor/encode value mode)))))))))

(deftest lossless-collection-value-test
  (testing "ordinary decoded collections keep their Clojure representation"
    (is (map? (cbor/decode (cbor/encode {:a 1 :b 2}))))
    (is (set? (cbor/decode (cbor/encode #{1 2 3}))))
    (is (map? (cbor/decode (cbor/encode (zipmap (range 16) (range 16)))))))
  (testing "entries and members are available without a host map/set conversion"
    (let [decoded-map (cbor/decode (hex->bytes "a28000d9444c810401"))
          decoded-set (cbor/decode (hex->bytes "d901028280d9444c8104"))
          entries (.entries ^DLCbor$MapValue decoded-map)
          members (.members ^DLCbor$SetValue decoded-set)]
      (is (vector? (.getKey ^Map$Entry (first entries))))
      (is (list? (.getKey ^Map$Entry (second entries))))
      (is (= [0 1] (mapv #(.getValue ^Map$Entry %) entries)))
      (is (vector? (first members)))
      (is (list? (second members)))
      (is (thrown? UnsupportedOperationException (.clear entries)))
      (is (thrown? UnsupportedOperationException (.clear members)))
      (is (thrown? UnsupportedOperationException
                   (.setValue ^Map$Entry (first entries) 42)))))
  (testing "holder equality and hashing use canonical identity and ignore order"
    (let [entries [[[] 0] [(list) 1]]
          map-a (support/map-value entries)
          map-b (support/map-value (reverse entries))
          set-a (DLCbor$SetValue. [[] (list)])
          set-b (DLCbor$SetValue. [(list) []])]
      (is (= map-a map-b))
      (is (= (hash map-a) (hash map-b)))
      (is (= set-a set-b))
      (is (= (hash set-a) (hash set-b)))
      (is (not= (DLCbor$SetValue. [[]]) (DLCbor$SetValue. [(list)])))
      (is (not= (support/map-value [[0 []]])
                (support/map-value [[0 (list)]])))
      (doseq [value [[map-a set-a]
                     (support/map-value [[map-a set-a] [set-a map-a]])
                     (DLCbor$SetValue. [map-a set-a])]
              mode [cbor/canonical cbor/fast]]
        (is (Arrays/equals
             (cbor/encode value)
             (cbor/encode (cbor/decode (cbor/encode value mode)
                                       (= mode cbor/canonical))))))))
  (testing "holders copy their input containers"
    (let [members (ArrayList. [nil [] (list)])
          holder (DLCbor$SetValue. members)]
      (.clear members)
      (is (= 3 (count (.members holder))))))
  (testing "writers accept wire-distinct values from other JVM collections"
    (let [identity-map (doto (IdentityHashMap.)
                         (.put [] 0)
                         (.put (list) 1))
          identity-set (doto (Collections/newSetFromMap (IdentityHashMap.))
                         (.add [])
                         (.add (list)))
          signed-zero-set (doto (LinkedHashSet.)
                            (.add (float 0.0))
                            (.add (float -0.0)))]
      (is (= 2 (.size identity-map)))
      (is (= 2 (.size identity-set)))
      (is (= 2 (.size signed-zero-set)))
      (doseq [mode [cbor/canonical cbor/fast]
              value [identity-map identity-set signed-zero-set]]
        (is (Arrays/equals
             (cbor/encode value)
             (cbor/encode (cbor/decode (cbor/encode value mode)
                                       (= mode cbor/canonical)))))))))
