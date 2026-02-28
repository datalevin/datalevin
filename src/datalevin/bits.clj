;;
;; Copyright (c) Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns ^:no-doc datalevin.bits
  "Handle binary encoding of data, and read/write to buffers, etc."
  (:require
   [datalevin.constants :as c]
   [datalevin.buffer :as bf]
   [datalevin.util :as u]
   [datalevin.ints :as i]
   [datalevin.sparselist :as sl]
   [clojure.string :as s]
   [taoensso.nippy :as nippy])
  (:import
   [java.util Arrays UUID Date Base64 Base64$Decoder Base64$Encoder]
   [java.util.regex Pattern]
   [java.math BigInteger BigDecimal]
   [java.io Writer]
   [java.nio ByteBuffer]
   [java.nio.charset StandardCharsets]
   [java.lang String]
   [org.roaringbitmap RoaringBitmap RoaringBitmapWriter  FastAggregation]
   [org.roaringbitmap.longlong Roaring64Bitmap]
   [datalevin.utl BitOps]))

;; base64

(defonce base64-encoder (.withoutPadding (Base64/getEncoder)))

(defonce base64-decoder (Base64/getDecoder))

(defn encode-base64
  "encode bytes into a base64 string"
  [bs]
  (.encodeToString ^Base64$Encoder base64-encoder bs))

(defn decode-base64
  "decode a base64 string to return the bytes"
  [^String s]
  (.decode ^Base64$Decoder base64-decoder s))

(defn text-ba->str
  "Convert a byte array to string, the array is known to contain text data"
  [^bytes ba]
  (String. ba StandardCharsets/UTF_8))

;; writer/reader

(defonce std-print-bytes (get-method print-method (Class/forName "[B")))

(defmethod print-method (Class/forName "[B")
  [^bytes bs, ^Writer w]
  (if u/*datalevin-print*
    (doto w
      (.write "#datalevin/bytes ")
      (.write "\"")
      (.write ^String (encode-base64 bs))
      (.write "\""))
    (std-print-bytes bs w)))

(defn bytes-from-reader ^bytes [s] (decode-base64 s))

(defonce std-print-pattern (get-method print-method Pattern))

(defmethod print-method Pattern
  [^Pattern p, ^Writer w]
  (if u/*datalevin-print*
    (doto w
      (.write "#datalevin/regex ")
      (.write "\"")
      (.write ^String (s/escape (.toString p) {\\ "\\\\"}))
      (.write "\""))
    (std-print-pattern p w)))

(defn regex-from-reader ^Pattern [s] (Pattern/compile s))

(defonce std-print-big-integer (get-method print-method BigInteger))

(defmethod print-method BigInteger
  [^BigInteger bi, ^Writer w]
  (if u/*datalevin-print*
    (doto w
      (.write "#datalevin/bigint ")
      (.write "\"")
      (.write ^String (.toString bi))
      (.write "\""))
    (std-print-big-integer bi w)))

(defn bigint-from-reader ^BigInteger [^String s] (BigInteger. s))

;; nippy

(defn serialize ^bytes
  [x]
  (binding [nippy/*freeze-serializable-allowlist*
            (if (seq c/*data-serializable-classes*)
              (into nippy/*thaw-serializable-allowlist*
                    c/*data-serializable-classes*)
              nippy/*thaw-serializable-allowlist*)]
    (if (instance? java.lang.Class x)
      (u/raise "Unfreezable type: java.lang.Class" {})
      (nippy/fast-freeze x))))

(defn deserialize
  "Deserialize from bytes. "
  [^bytes bs]
  (binding [nippy/*thaw-serializable-allowlist*
            (if (seq c/*data-serializable-classes*)
              (into nippy/*thaw-serializable-allowlist*
                    c/*data-serializable-classes*)
              nippy/*thaw-serializable-allowlist*)]
    (nippy/fast-thaw bs)))

;; bitmap

(defn bitmap
  "Create a roaringbitmap. Expect a sorted integer collection"
  ([]
   (RoaringBitmap.))
  ([ints]
   (let [^RoaringBitmapWriter writer (-> (RoaringBitmapWriter/writer)
                                         (.initialCapacity (count ints))
                                         (.get))]
     (doseq [^int i ints] (.add writer i))
     (.get writer))))

(defn bitmap? [x] (instance? RoaringBitmap x))

(defn bitmap-del
  "Delete an int from the bitmap"
  [^RoaringBitmap bm i]
  (.remove bm ^int i)
  bm)

(defn bitmap-add
  "Add an int from the bitmap"
  [^RoaringBitmap bm i]
  (.add bm ^int i)
  bm)

(defn bitmap-and
  [a b]
  (cond
    (nil? a) b
    (nil? b) a
    :else    (.and ^RoaringBitmap a ^RoaringBitmap b)))

(defn bitmaps-and
  [bitmaps]
  (when-not (empty? bitmaps)
    (FastAggregation/and
      ^"[Lorg.roaringbitmap.RoaringBitmap;"
      (into-array RoaringBitmap bitmaps))))

(defn bitmaps-or
  [bitmaps]
  (when-not (empty? bitmaps)
    (FastAggregation/or
      ^"[Lorg.roaringbitmap.RoaringBitmap;"
      (into-array RoaringBitmap bitmaps))))

(defn bitmap-or
  [a b]
  (cond
    (nil? a) nil
    (nil? b) nil
    :else    (.or ^RoaringBitmap a ^RoaringBitmap b)))

(defn bitmap-empty? [bm] (or (nil? bm) (.isEmpty ^RoaringBitmap bm)))

(defn- get-bitmap
  [^ByteBuffer bf]
  (let [bm (RoaringBitmap.)] (.deserialize bm bf) bm))

(defn- put-bitmap [^ByteBuffer bf ^RoaringBitmap x] (.serialize x bf))

;; 64-bit bitmap

(defn bitmap64
  "Create a 64-bit roaring bitmap from a collection of longs"
  ([]
   (Roaring64Bitmap.))
  ([longs]
   (let [bm (Roaring64Bitmap.)]
     (doseq [l longs] (.addLong bm (long l)))
     bm)))

(defn bitmap64-add
  "Add a long to the 64-bit bitmap"
  [^Roaring64Bitmap bm ^long l]
  (.addLong bm l)
  bm)

(defn bitmap64-contains?
  "Check if the 64-bit bitmap contains the given long"
  [^Roaring64Bitmap bm ^long l]
  (.contains bm l))

(defn bitmap64-cardinality
  "Return the number of elements in the 64-bit bitmap"
  ^long [^Roaring64Bitmap bm]
  (.getLongCardinality bm))

(defn bitmap64-min
  "Return the minimum value in the 64-bit bitmap"
  ^long [^Roaring64Bitmap bm]
  (.first bm))

(defn bitmap64-max
  "Return the maximum value in the 64-bit bitmap"
  ^long [^Roaring64Bitmap bm]
  (.last bm))

(defn bitmap64->longs
  "Return the values in the 64-bit bitmap as a long array"
  [^Roaring64Bitmap bm]
  (.toArray bm))

;; data read/write from/to buffer

(defn- get-long
  "Get a long from a ByteBuffer"
  [^ByteBuffer bb]
  (.getLong bb))

(defn get-int
  "Get an int from a ByteBuffer"
  [^ByteBuffer bb]
  (.getInt bb))

(defn get-short
  "Get a short from a ByteBuffer"
  [^ByteBuffer bb]
  (.getShort bb))

(defn- get-byte
  "Get a byte from a ByteBuffer"
  [^ByteBuffer bb]
  (.get bb))

(defn get-bytes
  "Copy content from a ByteBuffer to a byte array, useful for
   e.g. read txn result, as buffer content is gone when txn is done"
  ([^ByteBuffer bb]
   (let [ba (byte-array (.remaining bb))]
     (.get bb ba)
     ba))
  ([^ByteBuffer bb n]
   (let [ba (byte-array n)]
     (.get bb ba 0 n)
     ba)))

(defn- get-bytes-val
  [^ByteBuffer bf ^long post-v]
  (get-bytes bf (- (.remaining bf) post-v)))

(defn- get-data
  "Read data from a ByteBuffer"
  ([^ByteBuffer bb]
   (when-let [bs (get-bytes bb)]
     (deserialize bs)))
  ([^ByteBuffer bb post-v]
   (when-let [bs (get-bytes-val bb post-v)] (deserialize bs))))

(def ^:no-doc ^:const float-sign-idx 31)
(def ^:no-doc ^:const double-sign-idx 63)
(def ^:no-doc ^:const instant-sign-idx 63)

(defn- code-instant [^long x] (bit-flip x instant-sign-idx))

(defn- get-instant
  [^ByteBuffer bf]
  (Date. ^long (code-instant (.getLong bf))))

(defn- put-instant
  [^ByteBuffer bf x]
  (.putLong bf (code-instant
                 (if (inst? x)
                   (inst-ms x)
                   (u/raise "Expect an inst? value" {:value x})))))

(defn- decode-float
  [x]
  (if (bit-test x float-sign-idx)
    (Float/intBitsToFloat (BitOps/intFlip x float-sign-idx))
    (Float/intBitsToFloat (BitOps/intNot ^int x))))

(defn- get-float
  [^ByteBuffer bb]
  (decode-float (.getInt bb)))

(defn- decode-double
  [x]
  (if (bit-test x double-sign-idx)
    (Double/longBitsToDouble (bit-flip x double-sign-idx))
    (Double/longBitsToDouble (bit-not ^long x))))

(defn- get-double
  [^ByteBuffer bb]
  (decode-double (.getLong bb)))

(defn boolean-value
  "Datalevin's boolean value in byte"
  [b]
  (case (short b)
    2 true
    1 false
    (u/raise "Illegal value, expecting a Datalevin boolean value" {})))

(defn- get-boolean [^ByteBuffer bb] (boolean-value (get-byte bb)))

(defn- put-long [^ByteBuffer bb n] (.putLong bb ^long n))

(defn- encode-float
  [x]
  (assert (not (Float/isNaN x)) "Cannot index NaN")
  (let [x (if (= x -0.0) 0.0 x)]
    (if (neg? ^float x)
      (BitOps/intNot (Float/floatToIntBits x))
      (BitOps/intFlip (Float/floatToIntBits x) ^long float-sign-idx))))

(defn- put-float [^ByteBuffer bb x] (.putInt bb (encode-float (float x))))

(defn- encode-double
  [^double x]
  (assert (not (Double/isNaN x)) "Cannot index NaN")
  (let [x (if (= x -0.0) 0.0 x)]
    (if (neg? x)
      (bit-not (Double/doubleToLongBits x))
      (bit-flip (Double/doubleToLongBits x) double-sign-idx))))

(defn- put-double [^ByteBuffer bb x] (.putLong bb (encode-double (double x))))

(defn put-int [^ByteBuffer bb n] (.putInt bb ^int (int n)))

(defn put-short [^ByteBuffer bb n] (.putShort bb ^short (short n)))

(defn put-bytes [^ByteBuffer bb ^bytes bs] (.put bb bs))

(defn- put-byte-buffer [^ByteBuffer bb ^ByteBuffer src]
  (.put bb (.duplicate src)))

(defn- put-byte [^ByteBuffer bb b] (.put bb ^byte (unchecked-byte b)))

(defn encode-bigint
  [^BigInteger x]
  (let [bs (.toByteArray x)
        n  (alength bs)]
    (assert (< n 128)
            "Does not support integer beyond the range of [-2^1015, 2^1015-1]")
    (let [bs1 (byte-array (inc n))]
      (aset bs1 0 (unchecked-byte (if (= (.signum x) -1)
                                    (- 127 n)
                                    (bit-flip n 7))))
      (System/arraycopy bs 0 bs1 1 n)
      bs1)))

(defn- put-bigint
  [^ByteBuffer bb ^BigInteger x]
  (put-bytes bb (encode-bigint x)))

(defn decode-bigint
  ^BigInteger [^bytes bs]
  (BigInteger. ^bytes (Arrays/copyOfRange bs 1 (alength bs))))

(defn- get-bigint
  [^ByteBuffer bb]
  (let [^byte b   (get-byte bb)
        n         (if (bit-test b 7)
                    (unchecked-byte (bit-flip b 7))
                    (unchecked-byte (- 127 b)))
        ^bytes bs (get-bytes bb n)]
    (BigInteger. bs)))

(defn- put-bigdec
  [^ByteBuffer bb ^BigDecimal x]
  (put-double bb (.doubleValue x))
  (put-bigint bb (.unscaledValue x))
  (put-byte bb c/separator)
  (put-int bb (.scale x)))

(defn- encode-bigdec
  [^BigDecimal x]
  (let [^BigInteger v (.unscaledValue x)
        bs            (.toByteArray v)
        n             (alength bs)]
    (assert (< n 128)
            "Unscaled value of big decimal cannot go beyond the range of
             [-2^1015, 2^1015-1]")
    (let [bf (ByteBuffer/allocate (+ n 14))]
      (put-bigdec bf x)
      (.array bf))))

(defn- get-bigdec
  [^ByteBuffer bb]
  (get-double bb)
  (let [^BigInteger value (get-bigint bb)
        _                 (get-byte bb)
        ^int scale        (get-int bb)]
    (BigDecimal. value scale)))

(defn- put-data [^ByteBuffer bb x] (put-bytes bb (serialize x)))

(defn- put-uuid
  [bf ^UUID val]
  (put-long bf (.getMostSignificantBits val))
  (put-long bf (.getLeastSignificantBits val)))

(defn- get-uuid [bf] (UUID. (get-long bf) (get-long bf)))

(defn- sep->slash
  [^bytes bs]
  (let [n-1 (dec (alength bs))]
    (loop [i 0]
      (cond
        (= (aget bs i) c/separator) (aset-byte bs i c/slash)
        (< i n-1)                   (recur (inc i)))))
  bs)

(defn- get-string
  ([^ByteBuffer bf]
   (String. ^bytes (get-bytes bf) StandardCharsets/UTF_8))
  ([^ByteBuffer bf ^long post-v]
   (String. ^bytes (get-bytes-val bf post-v) StandardCharsets/UTF_8)))

(defn- get-key-sym-str
  [^ByteBuffer bf ^long post-v]
  (let [^bytes ba (sep->slash (get-bytes-val bf post-v))
        s         (String. ^bytes ba StandardCharsets/UTF_8)]
    (if (= (aget ba 0) c/slash) (subs s 1) s)))

(defn- get-keyword
  [^ByteBuffer bf ^long post-v]
  (keyword (get-key-sym-str bf post-v)))

(defn- get-symbol
  [^ByteBuffer bf ^long post-v]
  (symbol (get-key-sym-str bf post-v)))

;; bytes

(defn type-size
  "Return the storage size (bytes) of the fixed length data type (a keyword).
  Useful when calling `open-dbi`. E.g. return 9 for `:long` (i.e. 8 bytes plus
  an 1 byte header). Return nil if the given data type has no fixed size."
  [type]
  (case type
    :int     4
    :long    9
    :id      8
    :float   5
    :double  9
    :byte    1
    :boolean 2
    :instant 9
    :uuid    17
    nil))

(defn measure-size
  "measure size of x in number of bytes"
  [x]
  (cond
    (bytes? x)         (inc (alength ^bytes x))
    (int? x)           8
    (instance? Byte x) 1
    :else              (alength ^bytes (serialize x))))

(defmacro wrap-extrema
  [v vmin vmax b]
  `(if (keyword? ~v)
     (case ~v
       :db.value/sysMin ~vmin
       :db.value/sysMax ~vmax
       (u/raise "Expect other data types, got keyword instead: " ~v {}))
     ~b))

(defn- string-bytes
  [^String v]
  (wrap-extrema
    v c/min-bytes c/max-bytes (.getBytes v StandardCharsets/UTF_8)))

(defn- key-sym-bytes
  [x]
  (let [^bytes nmb (string-bytes (name x))
        nml        (alength nmb)]
    (if-let [ns (namespace x)]
      (let [^bytes nsb (string-bytes ns)
            nsl        (alength nsb)
            res        (byte-array (+ nsl nml 1))]
        (System/arraycopy nsb 0 res 0 nsl)
        (System/arraycopy c/separator-ba 0 res nsl 1)
        (System/arraycopy nmb 0 res (inc nsl) nml)
        res)
      (let [res (byte-array (inc nml))]
        (System/arraycopy c/separator-ba 0 res 0 1)
        (System/arraycopy nmb 0 res 1 nml)
        res))))

(defn- keyword-bytes
  [x]
  (case x
    :db.value/sysMin c/min-bytes
    :db.value/sysMax c/max-bytes
    (key-sym-bytes x)))

(defn- symbol-bytes
  [x]
  (wrap-extrema x c/min-bytes c/max-bytes (key-sym-bytes x)))

(defn- data-bytes
  [v]
  (case v
    :db.value/sysMin c/min-bytes
    :db.value/sysMax c/max-bytes
    (serialize v)))

(defn- bigint-bytes
  [v]
  (wrap-extrema v c/min-bytes c/max-bytes (encode-bigint v)))

(defn- bigdec-bytes
  [v]
  (wrap-extrema v c/min-bytes c/max-bytes (encode-bigdec v)))

(defn- get-sparse-list
  [bf]
  (let [sl (sl/sparse-arraylist)]
    (sl/deserialize sl bf)
    sl))

(defn- get-tuple-sizes
  [^ByteBuffer bf ^long c ^long post-v]
  (let [ss (volatile! [])]
    (.position bf (- (.limit bf) c post-v))
    (dotimes [_ c] (vswap! ss conj (short (get-byte bf))))
    @ss))

(defn- long-header
  [v]
  (if (keyword? v)
    (case v
      :db.value/sysMin c/type-long-neg
      :db.value/sysMax c/type-long-pos
      (u/raise "Expecting long, got keyword" v {}))
    (if (neg? ^long v) c/type-long-neg c/type-long-pos)))

(defn- raw-header
  [v t]
  (case t
    :keyword c/type-keyword
    :symbol  c/type-symbol
    :string  c/type-string
    :boolean c/type-boolean
    :float   c/type-float
    :double  c/type-double
    :instant c/type-instant
    :uuid    c/type-uuid
    :bytes   c/type-bytes
    :bigint  c/type-bigint
    :bigdec  c/type-bigdec
    :long    (long-header v)
    nil))

(def known-size-types #{:boolean :long :double :float :instant :uuid})

(defn- put-fixed
  [bf v hdr]
  (case (short hdr)
    -64 (put-long bf (wrap-extrema v Long/MIN_VALUE -1 v))
    -63 (put-long bf (wrap-extrema v 0 Long/MAX_VALUE v))
    -11 (put-float bf (wrap-extrema v Float/NEGATIVE_INFINITY
                                    Float/POSITIVE_INFINITY v))
    -10 (put-double bf (wrap-extrema v Double/NEGATIVE_INFINITY
                                     Double/POSITIVE_INFINITY v))
    -9  (put-long bf (code-instant
                       (wrap-extrema v Long/MIN_VALUE Long/MAX_VALUE
                                     (.getTime ^Date v))))
    -8  (put-long bf (wrap-extrema v c/e0 Long/MAX_VALUE v))
    -7  (put-uuid bf (wrap-extrema v c/min-uuid c/max-uuid v))
    -3  (put-byte bf (wrap-extrema v c/false-value c/true-value
                                   (if v c/true-value c/false-value)))))

(defn- put-varied
  [bf v hdr]
  (case (short hdr)
    -2  (put-bytes bf (wrap-extrema v c/min-bytes c/tuple-max-bytes v))
    -4  (put-bytes bf (wrap-extrema v c/min-bytes c/tuple-max-bytes
                                    (key-sym-bytes v)))
    -5  (put-bytes bf (case v
                        :db.value/sysMin c/min-bytes
                        :db.value/sysMax c/tuple-max-bytes
                        (key-sym-bytes v)))
    -6  (put-bytes bf (wrap-extrema
                        v c/min-bytes c/tuple-max-bytes
                        (.getBytes ^String v StandardCharsets/UTF_8)))
    -14 (put-bytes bf (wrap-extrema v c/min-bytes c/tuple-max-bytes
                                    (encode-bigdec v)))
    -15 (put-bytes bf (wrap-extrema v c/min-bytes c/tuple-max-bytes
                                    (encode-bigint v)))))

(defn put-homo-tuple
  "x-type is a single raw value type"
  [^ByteBuffer bf x x-type]
  (let [c   (count x)
        f   (nth x 0)
        hdr (raw-header f x-type)]
    (put-byte bf hdr)
    (put-byte bf (unchecked-byte c))
    (if (known-size-types x-type)
      (dotimes [i c] (put-fixed bf (nth x i) hdr))
      (let [c-1   ^long (dec c)
            sizes (loop [ss [] i 0 cp (.position bf)]
                    (if (< i c)
                      (let [v (nth x i)]
                        (put-varied bf v hdr)
                        (let [np   (.position bf)
                              size (- np cp)]
                          (when (< c/+tuple-max+ (- size 2))
                            (u/raise "The maximal tuple element size is
                              255 bytes" {:too-large v}))
                          (when (< i c-1) (put-byte bf c/separator))
                          (recur (conj ss size) (inc i) (inc np))))
                      ss))]
        (put-byte bf c/truncator)
        (doseq [s sizes] (put-byte bf (unchecked-byte s)))))))

(defn put-hete-tuple
  "x-type is a vector of raw value types"
  [^ByteBuffer bf x x-type]
  (let [c (count x-type)]
    (put-byte bf (unchecked-byte c))
    (let [c-1   ^long (dec c)
          sizes (loop [ss [] i 0 cp (.position bf)]
                  (if (< i c)
                    (let [v     (nth x i)
                          t     (nth x-type i)
                          hdr   (raw-header v t)
                          known (known-size-types t)]
                      (put-byte bf hdr)
                      (if known (put-fixed bf v hdr) (put-varied bf v hdr))
                      (when (and (not known) (< i c-1))
                        (put-byte bf c/separator))
                      (let [np   (.position bf)
                            size (- np cp)]
                        (when (< c/+tuple-max+ (- size 2))
                          (u/raise "The maximal tuple element size is
                              255 bytes" {:too-large v}))
                        (recur (conj ss size) (inc i) np)))
                    ss))]
      (put-byte bf c/truncator)
      (doseq [s sizes] (put-byte bf (unchecked-byte s))))))

(defn- header->type
  [header]
  (case (short header)
    (-64 -63 -8) :long
    -15          :bigint
    -14          :bigdec
    -11          :float
    -10          :double
    -9           :instant
    -7           :uuid
    -6           :string
    -5           :keyword
    -4           :symbol
    -3           :boolean
    -2           :bytes))

(declare get-value*)

(defn- get-homo-tuple
  ([bf] (get-homo-tuple bf 0))
  ([^ByteBuffer bf ^long outer-post-v]
   (let [hdr   (get-byte bf)
         c     (short (get-byte bf))
         c-1   (dec c)
         tuple (volatile! [])]
     (if (known-size-types (header->type hdr))
       (dotimes [_ c] (vswap! tuple conj (get-value* bf hdr)))
       (let [op      (.position bf)
             post-vs (->> (get-tuple-sizes bf c outer-post-v)
                          reverse
                          (reduce
                            (fn [vs s]
                              (conj vs (+ ^long s (inc ^long (first vs)))))
                            '(0))
                          rest
                          (map #(+ ^long % c 1 outer-post-v))
                          vec)
             np      (.position bf)]
         (.position bf op)
         (dotimes [i c]
           (vswap! tuple conj (get-value* bf (nth post-vs i) hdr))
           (when-not (= i c-1) (get-byte bf)))
         (.position bf np)))
     @tuple)))

(defn- get-hete-tuple
  ([bf] (get-hete-tuple bf 0))
  ([^ByteBuffer bf ^long outer-post-v]
   (let [c       (short (get-byte bf))
         c-1     (dec c)
         tuple   (volatile! [])
         op      (.position bf)
         post-vs (->> (get-tuple-sizes bf c outer-post-v)
                      reverse
                      (reduce
                        (fn [vs s] (conj vs (+ ^long s ^long (first vs))))
                        '(0))
                      rest
                      (map #(+ ^long % c 1 outer-post-v))
                      vec)
         np      (.position bf)]
     (.position bf op)
     (dotimes [i c]
       (let [hdr    (get-byte bf)
             known  (known-size-types (header->type hdr))
             post-v (if (or known (= i c-1))
                      (nth post-vs i)
                      (inc ^long (nth post-vs i)))]
         (vswap! tuple conj (get-value* bf post-v hdr))
         (when-not known (get-byte bf))))
     (.position bf np)
     @tuple)))

(defn- dl-type->raw
  [t]
  (get {:db.type/string  :string
        :db.type/bigdec  :bigdec
        :db.type/bigint  :bigint
        :db.type/boolean :boolean
        :db.type/bytes   :bytes
        :db.type/double  :double
        :db.type/float   :float
        :db.type/long    :long
        :db.type/ref     :long
        :db.type/instant :instant
        :db.type/uuid    :uuid
        :db.type/keyword :keyword
        :db.type/symbol  :symbol} t t))

(defn- tuple-bytes
  [v t]
  (wrap-extrema
    v c/min-bytes c/max-bytes
    (let [t        (into [] (map dl-type->raw) t)
          tuple-bf ^ByteBuffer (bf/get-array-buffer)]
      (if (= 1 (count t))
        (put-homo-tuple tuple-bf v (nth t 0))
        (put-hete-tuple tuple-bf v t))
      (let [res (Arrays/copyOfRange (.array tuple-bf)
                                    0 (.position tuple-bf))]
        (bf/return-array-buffer tuple-bf)
        res))))

(defn- val-bytes
  "Turn datalog value into bytes according to :db/valueType"
  [v t]
  (case t
    (:db.type/boolean :db.type/long :db.type/double :db.type/float
                      :db.type/ref :db.type/instant :db.type/uuid) nil

    :db.type/keyword (keyword-bytes v)
    :db.type/symbol  (symbol-bytes v)
    :db.type/string  (string-bytes v)
    :db.type/bigint  (bigint-bytes v)
    :db.type/bigdec  (bigdec-bytes v)
    :db.type/bytes   (wrap-extrema v c/min-bytes c/max-bytes v)
    (if (vector? t) (tuple-bytes v t) (data-bytes v))))

(defn- val-header
  [v t]
  (case t
    :db.type/keyword c/type-keyword
    :db.type/symbol  c/type-symbol
    :db.type/string  c/type-string
    :db.type/boolean c/type-boolean
    :db.type/float   c/type-float
    :db.type/double  c/type-double
    :db.type/ref     c/type-ref
    :db.type/instant c/type-instant
    :db.type/uuid    c/type-uuid
    :db.type/bytes   c/type-bytes
    :db.type/bigint  c/type-bigint
    :db.type/bigdec  c/type-bigdec
    :db.type/long    (long-header v)
    (when (vector? t)
      (if (= 1 (count t)) c/type-homo-tuple c/type-hete-tuple))))

(defn- get-value*
  ([bf hdr] (get-value* bf 0 hdr))
  ([^ByteBuffer bf post-v hdr]
   (case (short hdr)
     -64 (get-long bf)
     -63 (get-long bf)
     -15 (get-bigint bf)
     -14 (get-bigdec bf)
     -13 (get-homo-tuple bf post-v)
     -12 (get-hete-tuple bf post-v)
     -11 (get-float bf)
     -10 (get-double bf)
     -9  (get-instant bf)
     -8  (get-long bf)
     -7  (get-uuid bf)
     -6  (get-string bf post-v)
     -5  (get-keyword bf post-v)
     -4  (get-symbol bf post-v)
     -3  (get-boolean bf)
     -2  (get-bytes-val bf post-v)
     (do (.reset bf)
         (get-data bf post-v)))))

(defn- get-value
  [^ByteBuffer bf post-v]
  (.mark bf)
  (get-value* bf post-v (get-byte bf)))

(defn- put-attr
  "NB. not going to do range query on attr names"
  [bf x]
  (let [^bytes a (-> x str (subs 1) string-bytes)
        al       (alength a)]
    (assert (<= al c/+max-key-size+)
            "Attribute cannot be longer than 511 bytes")
    (put-bytes bf a)))

(defn- get-attr
  [bf]
  (let [^bytes bs (get-bytes bf)]
    (keyword (String. bs StandardCharsets/UTF_8))))

(deftype Indexable [e a v f b g])

(defn pr-indexable [^Indexable i]
  [(.-e i) (.-a i) (.-v i) (.-f i) (u/hexify (.-b i)) (.-g i)])

(defmethod print-method Indexable
  [^Indexable i, ^Writer w]
  (.write w "#datalevin/Indexable ")
  (.write w (pr-str (pr-indexable i))))

(defn indexable
  "Turn datom parts into a form that is suitable for putting in indices,
  where aid is the integer id of an attribute, vt is its :db/valueType
  max-gt is current max giant id"
  [eid aid val vt max-gt]
  (let [hdr (val-header val vt)]
    (if-let [vb (val-bytes val vt)]
      (let [bl   (alength ^bytes vb)
            cut? (> bl c/+val-bytes-wo-hdr+)
            bas  (if cut?
                   (Arrays/copyOf ^bytes vb c/+val-bytes-trunc+)
                   vb)
            gid  (if cut? max-gt c/normal)]
        (Indexable. eid aid val hdr bas gid))
      (Indexable. eid aid val hdr nil c/normal))))

(defn giant? [^Indexable i] (not= (.-g i) c/normal))

(defn- put-avg
  [bf ^Indexable x]
  (put-int bf (.-a x))
  (when-let [hdr (.-f x)] (put-byte bf hdr))
  (if-let [bs (.-b x)]
    (put-bytes bf bs)
    (put-fixed bf (.-v x) (.-f x)))
  (if (giant? x)
    (do (put-byte bf c/truncator)
        (put-long bf (.-g x))
        (put-byte bf c/true-value))
    (do (put-byte bf c/separator)
        (put-byte bf c/false-value))))

(deftype Retrieved [e a v g])

(defn pr-retrieved [^Retrieved r]
  [(.-e r) (.-a r) (.-v r) (.-g r)])

(defmethod print-method Retrieved
  [^Retrieved r, ^Writer w]
  (.write w "#datalevin/Retrieved ")
  (.write w (pr-str (pr-retrieved r))))

(defn- get-avg
  [^ByteBuffer bf]
  (let [limit (.limit bf)
        a     (get-int bf)]
    (.position bf (- limit 1))
    (if (= c/true-value (get-byte bf))
      (do (.position bf (- limit 9))
          (Retrieved. nil a nil (get-long bf)))
      (do (.position bf 4)
          (Retrieved. nil a (get-value bf 2) c/normal)))))

(defn avg->r
  [^ByteBuffer bf]
  (let [limit (.limit bf)]
    (.position bf (- limit 1))
    (if (= c/true-value (get-byte bf))
      (do (.position bf (- limit 9))
          (Retrieved. nil nil nil (get-long bf)))
      (do (.position bf 4)
          (Retrieved. nil nil (get-value bf 2) c/normal)))))

(defn- indexable->retrieved
  [^Indexable i]
  (Retrieved. (.-e i) (.-a i) (.-v i) (.-g i)))

(defn expected-return
  "Given what's put in, return the expected output from storage"
  [x x-type]
  (case x-type
    (:eav :ave) (indexable->retrieved x)
    x))

(defn put-buffer
  ([bf x]
   (put-buffer bf x :data))
  ([^ByteBuffer bf x x-type]
   (case x-type
     ;; user facing
     :string         (do (put-byte bf (raw-header x :string))
                         (put-bytes
                           bf (.getBytes ^String x StandardCharsets/UTF_8)))
     :bigint         (do (put-byte bf (raw-header x :bigint))
                         (put-bigint bf x))
     :bigdec         (do (put-byte bf (raw-header x :bigdec))
                         (put-bigdec bf x))
     :long           (do (put-byte bf (raw-header x :long))
                         (put-long bf x))
     :float          (do (put-byte bf (raw-header x :float))
                         (put-float bf x))
     :double         (do (put-byte bf (raw-header x :double))
                         (put-double bf x))
     :bytes          (do (put-byte bf (raw-header x :bytes))
                         (put-bytes bf x))
     :keyword        (do (put-byte bf (raw-header x :keyword))
                         (put-bytes bf (key-sym-bytes x)))
     :symbol         (do (put-byte bf (raw-header x :symbol))
                         (put-bytes bf (key-sym-bytes x)))
     :boolean        (do (put-byte bf (raw-header x :boolean))
                         (put-byte bf (if x c/true-value c/false-value)))
     :instant        (do (put-byte bf (raw-header x :instant))
                         (put-instant bf x))
     :uuid           (do (put-byte bf (raw-header x :uuid))
                         (put-uuid bf x))
     ;; internal use
     :instant-pre-06 (do (put-byte bf (raw-header x :instant))
                         (.putLong bf (.getTime ^Date x)))
     :byte           (put-byte bf x)
     :short          (put-short bf x)
     :int            (put-int bf x)
     :id             (put-long bf x)
     :int-int        (let [[i1 i2] x]
                       (put-int bf i1)
                       (put-int bf i2))
     :ints           (i/put-ints bf x)
     :bitmap         (put-bitmap bf x)
     :term-info      (let [[i1 i2 i3] x]
                       (put-int bf i1)
                       (.putFloat bf (float i2))
                       (sl/serialize i3 bf))
     :doc-info       (let [[i1 i2 i3] x]
                       (put-int bf i1)
                       (put-short bf i2)
                       (i/put-ints bf i3))
     :pos-info       (let [[i1 i2] x]
                       (i/put-sorted-ints bf i1)
                       (i/put-sorted-ints bf i2))
     :attr           (put-attr bf x)
     :avg            (put-avg bf x)
     :raw            (cond
                       (bytes? x)
                       (put-bytes bf x)

                       (instance? ByteBuffer x)
                       (put-byte-buffer bf x)

                       :else
                       (u/raise "Raw value must be bytes or ByteBuffer, got "
                                (type x)
                                {:input x}))
     (if (vector? x-type)
       (if (= 1 (count x-type))
         (do (put-byte bf c/type-homo-tuple)
             (put-homo-tuple bf x (nth x-type 0)))
         (do (put-byte bf c/type-hete-tuple)
             (put-hete-tuple bf x x-type)))
       (put-data bf x)))))

(defn put-bf
  "clear the buffer, put in the data, and prepare it for reading"
  [^ByteBuffer bf data type]
  (when-some [x data]
    (.clear bf)
    (put-buffer bf x type )
    (.flip bf)))

(defn read-buffer
  ([bf]
   (read-buffer bf :data))
  ([^ByteBuffer bf v-type]
   (case v-type
     ;; user facing
     :string         (do (get-byte bf) (get-string bf))
     :bigint         (do (get-byte bf) (get-bigint bf))
     :bigdec         (do (get-byte bf) (get-bigdec bf))
     :long           (do (get-byte bf) (get-long bf))
     :float          (do (get-byte bf) (get-float bf))
     :double         (do (get-byte bf) (get-double bf))
     :bytes          (do (get-byte bf) (get-bytes bf))
     :keyword        (do (get-byte bf) (get-keyword bf 0))
     :symbol         (do (get-byte bf) (get-symbol bf 0))
     :boolean        (do (get-byte bf) (get-boolean bf))
     :instant        (do (get-byte bf) (get-instant bf))
     :uuid           (do (get-byte bf) (get-uuid bf))
     ;; internal use
     :instant-pre-06 (do (get-byte bf) (Date. (.getLong bf)))
     :id             (get-long bf)
     :short          (get-short bf)
     :int            (get-int bf)
     :attr           (get-attr bf)
     :avg            (get-avg bf)
     :byte           (get-byte bf)
     :raw            (get-bytes bf)
     ;; range query are NOT supported on these
     :int-int        [(get-int bf) (get-int bf)]
     :ints           (i/get-ints bf)
     :bitmap         (get-bitmap bf)
     :term-info      [(get-int bf) (.getFloat bf) (get-sparse-list bf)]
     :doc-info       [(get-int bf) (get-short bf) (i/get-ints bf)]
     :pos-info       [(i/get-sorted-ints bf) (i/get-sorted-ints bf)]
     (if (vector? v-type)
       (do (get-byte bf)
           (if (= 1 (count v-type))
             (get-homo-tuple bf)
             (get-hete-tuple bf)))
       (get-data bf)))))

;; data validation

(defn- valid-data*
  [x t]
  (if (or (c/datalog-value-types t) (c/kv-value-types t))
    (case t
      (:db.type/string :string)   (string? x)
      (:db.type/bigint :bigint)   (<= c/min-bigint x c/max-bigint)
      (:db.type/bigdec :bigdec)   (<= c/min-bigdec x c/max-bigdec)
      (:db.type/long :db.type/ref
                     :long :id)   (int? x)
      (:db.type/float :float)     (<= (- Float/MAX_VALUE) x Float/MAX_VALUE)
      (:db.type/double :double)   (<= (- Double/MAX_VALUE) x Double/MAX_VALUE)
      (:db.type/bytes :bytes)     (bytes? x)
      (:db.type/keyword :keyword) (keyword? x)
      (:db.type/symbol :symbol)   (symbol? x)
      (:db.type/boolean :boolean) (boolean? x)
      (:db.type/instant :instant) (inst? x)
      (:db.type/uuid :uuid)       (uuid? x)
      :db.type/tuple              (vector? x)
      true)
    (if (identical? t :data) true false)))

(defn valid-data?
  "validate data type"
  [x t]
  (if (vector? t)
    (and (vector? x)
         (not (some #(= % :data) t))
         (let [ct (count t)]
           (if (= 1 ct)
             (let [t' (first t)] (every? #(valid-data* % t') x))
             (and (= ct (count x))
                  (every? true? (map #(valid-data* %1 %2) x t))))))
    (valid-data* x t)))
