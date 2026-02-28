(ns datalevin.bits-test
  (:require
   [datalevin.bits :as sut]
   [datalevin.buffer :as bf]
   [datalevin.sparselist :as sl]
   [datalevin.datom :as d]
   [datalevin.constants :as c]
   [clojure.test :refer [deftest is]]
   [clojure.test.check.generators :as gen]
   [clojure.test.check.clojure-test :as test]
   [clojure.test.check.properties :as prop]
   [datalevin.util :as u])
  (:import
   [java.util Arrays Date UUID]
   [java.util.concurrent Semaphore]
   [java.nio ByteBuffer]
   [java.nio.charset StandardCharsets]
   [org.joda.time DateTime]
   [org.roaringbitmap RoaringBitmap]
   [datalevin.sparselist SparseIntArrayList]
   [datalevin.bits Indexable Retrieved]))

;; binary index preserves the order of values

(def e 123456)
(def a 235)
(def g 4792)

;; buffer read/write

(defn- bytes-size-less-than?
  [^long limit ^bytes bs]
  (< (alength bs) limit))

(defn- string-size-less-than?
  [^long limit ^String s]
  (< (alength (.getBytes s StandardCharsets/UTF_8)) limit))

(test/defspec data-generative-test
  100
  (prop/for-all [k gen/any-equatable]
                (let [^ByteBuffer bf (bf/allocate-buffer 16384)]
                  (sut/put-bf bf k :data)
                  (= k (sut/read-buffer bf :data)))))

(test/defspec int-generative-test
  100
  (prop/for-all [k gen/int]
                (let [^ByteBuffer bf (bf/allocate-buffer 16384)]
                  (sut/put-bf bf k :int)
                  (= k (sut/read-buffer bf :int)))))

(test/defspec int-int-generative-test
  100
  (prop/for-all [k1 gen/int
                 k2 gen/int]
                (let [^ByteBuffer bf (bf/allocate-buffer 16384)]
                  (sut/put-bf bf [k1 k2] :int-int)
                  (= [k1 k2] (sut/read-buffer bf :int-int)))))

(test/defspec doc-info-generative-test
  100
  (prop/for-all [k1 gen/small-integer
                 k2 gen/small-integer
                 k3 (gen/list-distinct gen/small-integer)]
                (let [^ByteBuffer bf (bf/allocate-buffer 16384)
                      ^ints ar       (int-array k3)]
                  (sut/put-bf bf [k1 k2 ar] :doc-info)
                  (let [[k1' k2' ar'] (sut/read-buffer bf :doc-info)]
                    (is (and (= k1 k1')
                             (= k2 k2')
                             (Arrays/equals ar ^ints ar')))))))

(test/defspec pos-info-generative-test
  100
  (prop/for-all [k1 (gen/not-empty (gen/list-distinct gen/small-integer))
                 k2 (gen/not-empty (gen/list-distinct gen/small-integer))]
                (let [^ByteBuffer bf (bf/allocate-buffer 16384)
                      ^ints ar1      (int-array (sort k1))
                      ^ints ar2      (int-array (sort k2))]
                  (sut/put-bf bf [ar1 ar2] :pos-info)
                  (let [[ar1' ar2'] (sut/read-buffer bf :pos-info)]
                    (is (and  (Arrays/equals ar1 ^ints ar1')
                              (Arrays/equals ar2 ^ints ar2')))))))

(test/defspec term-info-generative-test
  100
  (prop/for-all [k1 gen/int
                 k2 (gen/double* {:NaN? false})
                 k3 (gen/vector gen/int)
                 k4 (gen/vector gen/int)]
                (let [^ByteBuffer bf         (bf/allocate-buffer 16384)
                      k2                     (float k2)
                      k3                     (sort k3)
                      ^SparseIntArrayList sl (sl/sparse-arraylist k3 k4)]
                  (sut/put-bf bf [k1 k2 sl] :term-info)
                  (= [k1 k2 sl] (sut/read-buffer bf :term-info)))))

(test/defspec long-generative-test
  100
  (prop/for-all [k gen/large-integer]
                (let [^ByteBuffer bf (bf/allocate-buffer 16384)]
                  (sut/put-bf bf k :long)
                  (= k (sut/read-buffer bf :long)))))

(test/defspec compressed-long-generative-test
  100
  (prop/for-all [k gen/large-integer]
                (let [^ByteBuffer bf (bf/allocate-buffer 16)]
                  (sut/put-bf bf k :long)
                  (= k (sut/read-buffer bf :long)))))

(test/defspec double-generative-test
  100
  (prop/for-all [k (gen/double* {:NaN? false})]
                (let [^ByteBuffer bf (bf/allocate-buffer 16384)]
                  (sut/put-bf bf k :double)
                  (= k (sut/read-buffer bf :double)))))

(test/defspec float-generative-test
  100
  (prop/for-all [k (gen/such-that
                     #(< Float/MIN_VALUE % Float/MAX_VALUE)
                     (gen/double* {:NaN? false :infinite? false})
                     1000)]
                (let [f              (float k)
                      ^ByteBuffer bf (bf/allocate-buffer 16384)]
                  (sut/put-bf bf k :float)
                  (= f (sut/read-buffer bf :float)))))

(def gen-bigint (gen/such-that
                  #(= clojure.lang.BigInt (type %))
                  gen/size-bounded-bigint))

(defn bigint-test
  [i j]
  (let [^ByteBuffer bf  (bf/allocate-buffer 816384)
        ^ByteBuffer bf1 (bf/allocate-buffer 816384)
        ^BigInteger m   (.toBigInteger ^clojure.lang.BigInt i)
        ^BigInteger n   (.toBigInteger ^clojure.lang.BigInt j)]
    (.clear bf)
    (sut/put-buffer bf m :bigint)
    (.flip bf)
    (.clear bf1)
    (sut/put-buffer bf1 n :bigint)
    (.flip bf1)
    (if (< (.compareTo m n) 0)
      (is (< (bf/compare-buffer bf bf1) 0))
      (if (= (.compareTo m n) 0)
        (is (= (bf/compare-buffer bf bf1) 0))
        (is (> (bf/compare-buffer bf bf1) 0))))
    (.rewind bf)
    (.rewind bf1)
    (let [^BigInteger m1 (sut/read-buffer bf :bigint)
          ^BigInteger n1 (sut/read-buffer bf1 :bigint)]
      (is (= (.compareTo m m1) 0))
      (is (= (.compareTo n n1) 0))
      (if (< (.compareTo m n) 0)
        (is (< (.compareTo m1 n1) 0))
        (if (= (.compareTo m n) 0)
          (is (= (.compareTo m1 n1) 0))
          (is (> (.compareTo m1 n1) 0)))))))

(test/defspec bigint-generative-test
  100
  (prop/for-all [i gen-bigint
                 j gen-bigint]
                (bigint-test i j)))

(defn bigdec-test
  [vi vj si sj]
  (let [^ByteBuffer bf  (bf/allocate-buffer 816384)
        ^ByteBuffer bf1 (bf/allocate-buffer 816384)
        ^BigInteger vi  (.toBigInteger ^clojure.lang.BigInt vi)
        ^BigInteger vj  (.toBigInteger ^clojure.lang.BigInt vj)
        ^BigDecimal m   (BigDecimal. vi ^int si)
        ^BigDecimal n   (BigDecimal. vj ^int sj)]
    (.clear bf)
    (sut/put-buffer bf m :bigdec)
    (.flip bf)
    (.clear bf1)
    (sut/put-buffer bf1 n :bigdec)
    (.flip bf1)
    (if (< (.compareTo m n) 0)
      (is (< (bf/compare-buffer bf bf1) 0))
      (if (= (.compareTo m n) 0)
        (is (= (bf/compare-buffer bf bf1) 0))
        (is (> (bf/compare-buffer bf bf1) 0))))
    (.rewind bf)
    (.rewind bf1)
    (let [^BigDecimal m1 (sut/read-buffer bf :bigdec)
          ^BigDecimal n1 (sut/read-buffer bf1 :bigdec)]
      (is (= (.compareTo m m1) 0))
      (is (= (.compareTo n n1) 0))
      (if (< (.compareTo m n) 0)
        (is (< (.compareTo m1 n1) 0))
        (if (= (.compareTo m n) 0)
          (is (= (.compareTo m1 n1) 0))
          (is (> (.compareTo m1 n1) 0)))))))

(test/defspec bigdec-generative-test
  100
  (prop/for-all [vi gen-bigint
                 vj gen-bigint
                 si gen/small-integer
                 sj gen/small-integer]
                (bigdec-test vi vj si sj)))

(test/defspec bytes-generative-test
  100
  (prop/for-all [^bytes k (gen/not-empty gen/bytes)]
                (let [^ByteBuffer bf (bf/allocate-buffer 16384)]
                  (sut/put-bf bf k :bytes)
                  (Arrays/equals k ^bytes (sut/read-buffer bf :bytes)))))

(test/defspec byte-generative-test
  100
  (prop/for-all [k gen/byte]
                (let [^ByteBuffer bf (bf/allocate-buffer 16384)]
                  (sut/put-bf bf k :byte)
                  (= k (sut/read-buffer bf :byte)))))

(test/defspec keyword-generative-test
  100
  (prop/for-all [k gen/keyword-ns]
                (let [^ByteBuffer bf (bf/allocate-buffer 16384)]
                  (sut/put-bf bf k :keyword)
                  (= k (sut/read-buffer bf :keyword)))))

(test/defspec symbol-generative-test
  100
  (prop/for-all [k gen/symbol-ns]
                (let [^ByteBuffer bf (bf/allocate-buffer 16384)]
                  (sut/put-bf bf k :symbol)
                  (= k (sut/read-buffer bf :symbol)))))

(test/defspec boolean-generative-test
  100
  (prop/for-all [k gen/boolean]
                (let [^ByteBuffer bf (bf/allocate-buffer 16384)]
                  (sut/put-bf bf k :boolean)
                  (= k (sut/read-buffer bf :boolean)))))

(test/defspec instant-generative-test
  100
  (prop/for-all [k gen/int]
                (let [^ByteBuffer bf (bf/allocate-buffer 16384)
                      d              (Date. ^long k)]
                  (sut/put-bf bf d :instant)
                  (= d (sut/read-buffer bf :instant)))))

(test/defspec instant-compare-test
  100
  (prop/for-all [k gen/int
                 k1 gen/int]
                (let [^ByteBuffer bf  (bf/allocate-buffer (inc Long/BYTES))
                      ^ByteBuffer bf1 (bf/allocate-buffer (inc Long/BYTES))
                      d               (Date. ^long k)
                      d1              (Date. ^long k1)
                      sign            (fn [^long diff]
                                        (cond
                                          (< 0 diff) 1
                                          (= 0 diff) 0
                                          (< diff 0) -1))]
                  (sut/put-bf bf d :instant)
                  (sut/put-bf bf1 d1 :instant)
                  (= (sign (- ^long k ^long k1))
                     (sign (bf/compare-buffer bf bf1))))))

(test/defspec uuid-generative-test
  100
  (prop/for-all [k gen/uuid]
                (let [^ByteBuffer bf (bf/allocate-buffer 16384)]
                  (sut/put-bf bf k :uuid)
                  (= k (sut/read-buffer bf :uuid)))))

(deftest datom-test
  (let [d1             (d/datom 1 :pet/name "Mr. Kitty")
        ^ByteBuffer bf (bf/allocate-buffer 16384)]
    (sut/put-bf bf d1 :datom)
    (is (= d1 (sut/deserialize (sut/serialize d1))))
    (is (= d1 (sut/read-buffer bf :datom)))))

(test/defspec datom-generative-test
  100
  (prop/for-all [e gen/large-integer
                 a gen/keyword-ns
                 v gen/any-equatable
                 t gen/large-integer]
                (let [^ByteBuffer bf (bf/allocate-buffer 16384)
                      d              (d/datom e a v t)]
                  (sut/put-bf bf d :datom)
                  (is (= d (sut/read-buffer bf :datom))))))

(test/defspec attr-generative-test
  100
  (prop/for-all [k gen/keyword-ns]
                (let [^ByteBuffer bf (bf/allocate-buffer 16384)]
                  (sut/put-bf bf k :attr)
                  (= k (sut/read-buffer bf :attr)))))

;; extrema bounds

(defn test-extrema
  [v d dmin dmax]
  (let [^ByteBuffer bf  (bf/allocate-buffer 16384)
        ^ByteBuffer bf1 (bf/allocate-buffer 16384)]
    (sut/put-bf bf d :avg)
    (sut/put-bf bf1 dmin :avg)
    (is (>= (bf/compare-buffer bf bf1) 0)
        (do
          (.rewind bf)
          (.rewind bf1)
          (str "v: " v
               " d: " (u/hexify (sut/get-bytes bf))
               " dmin: " (u/hexify (sut/get-bytes bf1)))))
    (sut/put-bf bf1 dmax :avg)
    (.rewind bf)
    (is (<= (bf/compare-buffer bf bf1) 0))))

(test/defspec keyword-extrema-generative-test
  100
  (prop/for-all
    [v  gen/keyword-ns]
    (test-extrema v
                  (sut/indexable e a v :db.type/keyword g)
                  (sut/indexable e a c/v0 :db.type/keyword c/g0)
                  (sut/indexable e a c/vmax :db.type/keyword c/gmax))))

(test/defspec symbol-extrema-generative-test
  100
  (prop/for-all
    [v  gen/symbol-ns]
    (test-extrema v
                  (sut/indexable e a v :db.type/symbol g)
                  (sut/indexable e a c/v0 :db.type/symbol c/g0)
                  (sut/indexable e a c/vmax :db.type/symbol c/gmax))))

;; null character "^@" is a special case
(test/defspec string-extrema-generative-test
  100
  (prop/for-all
    [v  (gen/such-that #(and (string-size-less-than? c/+val-bytes-wo-hdr+ %)
                             (not= "^@" %)
                             (not= "" %))
                       gen/string)]
    (test-extrema v
                  (sut/indexable e a v :db.type/string g)
                  (sut/indexable e a c/v0 :db.type/string c/g0)
                  (sut/indexable e a c/vmax :db.type/string c/gmax))))

(test/defspec boolean-extrema-generative-test
  100
  (prop/for-all
    [v  gen/boolean]
    (test-extrema v
                  (sut/indexable e a v :db.type/boolean g)
                  (sut/indexable e a c/v0 :db.type/boolean c/g0)
                  (sut/indexable e a c/vmax :db.type/boolean c/gmax))))

(test/defspec long-extrema-generative-test
  100
  (prop/for-all
    [v  gen/large-integer]
    (test-extrema v
                  (sut/indexable e a v :db.type/long g)
                  (sut/indexable e a c/v0 :db.type/long c/g0)
                  (sut/indexable e a c/vmax :db.type/long c/gmax))))

(test/defspec double-extrema-generative-test
  100
  (prop/for-all
    [v (gen/double* {:NaN? false})]
    (test-extrema v
                  (sut/indexable e a v :db.type/double g)
                  (sut/indexable e a c/v0 :db.type/double c/g0)
                  (sut/indexable e a c/vmax :db.type/double c/gmax))))

(test/defspec float-extrema-generative-test
  100
  (prop/for-all
    [v (gen/double* {:NaN? false})]
    (let [f (float v)]
      (test-extrema f
                    (sut/indexable e a f :db.type/float g)
                    (sut/indexable e a c/v0 :db.type/float c/g0)
                    (sut/indexable e a c/vmax :db.type/float c/gmax)))))

(test/defspec ref-extrema-generative-test
  100
  (prop/for-all
    [v  gen/nat]
    (test-extrema v
                  (sut/indexable e a v :db.type/ref g)
                  (sut/indexable e a c/v0 :db.type/ref c/g0)
                  (sut/indexable e a c/vmax :db.type/ref c/gmax))))

(test/defspec uuid-extrema-generative-test
  100
  (prop/for-all
    [v  gen/uuid]
    (test-extrema v
                  (sut/indexable e a v :db.type/uuid g)
                  (sut/indexable e a c/v0 :db.type/uuid c/g0)
                  (sut/indexable e a c/vmax :db.type/uuid c/gmax))))

(test/defspec bigint-extrema-generative-test
  100
  (prop/for-all
    [i  gen-bigint]
    (let [^BigInteger v (.toBigInteger ^clojure.lang.BigInt i)]
      (test-extrema v
                    (sut/indexable e a v :db.type/bigint g)
                    (sut/indexable e a c/v0 :db.type/bigint c/g0)
                    (sut/indexable e a c/vmax :db.type/bigint c/gmax)))))

(test/defspec bigdec-extrema-generative-test
  100
  (prop/for-all
    [i  gen-bigint
     s gen/small-integer]
    (let [^BigInteger n (.toBigInteger ^clojure.lang.BigInt i)
          ^BigDecimal v (BigDecimal. n ^int s)]
      (test-extrema v
                    (sut/indexable e a v :db.type/bigdec g)
                    (sut/indexable e a c/v0 :db.type/bigdec c/g0)
                    (sut/indexable e a c/vmax :db.type/bigdec c/gmax)))))

;; orders

(defn- avg-test
  [v a1 v1 ^Indexable d ^Indexable d1]
  (let [^ByteBuffer bf  (bf/allocate-buffer 16384)
        ^ByteBuffer bf1 (bf/allocate-buffer 16384)
        _               (.clear ^ByteBuffer bf)
        _               (sut/put-buffer bf d :avg)
        _               (.flip ^ByteBuffer bf)
        _               (.clear ^ByteBuffer bf1)
        _               (sut/put-buffer bf1 d1 :avg)
        _               (.flip ^ByteBuffer bf1)
        res             (bf/compare-buffer bf bf1)]
    (is (u/same-sign? res (u/combine-cmp (compare a a1)
                                         (compare v v1))))
    (.rewind ^ByteBuffer bf)
    (let [^Retrieved r (sut/read-buffer bf :avg)]
      (is (= a (.-a r)))
      (is (= v (.-v r))))
    (.rewind ^ByteBuffer bf1)
    (let [^Retrieved r (sut/read-buffer bf1 :avg)]
      (is (= a1 (.-a r)))
      (is (= v1 (.-v r))))))

(test/defspec instant-avg-generative-test
  100
  (prop/for-all
    [e1 (gen/large-integer* {:min c/e0})
     a1 gen/nat
     v1 gen/int
     v gen/int]
    (let [v'  (Date. ^long v)
          v1' (Date. ^long v1)]
      (avg-test v' a1 v1'
                (sut/indexable e a v' :db.type/instant g)
                (sut/indexable e1 a1 v1' :db.type/instant g)))))

(test/defspec keyword-avg-generative-test
  100
  (prop/for-all
    [e1 (gen/large-integer* {:min c/e0})
     a1 gen/nat
     v1 gen/keyword-ns
     v gen/keyword-ns]
    (avg-test v a1 v1
              (sut/indexable e a v :db.type/keyword g)
              (sut/indexable e1 a1 v1 :db.type/keyword g))))

(test/defspec symbol-avg-generative-test
  100
  (prop/for-all
    [e1 (gen/large-integer* {:min c/e0})
     a1 gen/nat
     v1 gen/symbol-ns
     v gen/symbol-ns]
    (avg-test v a1 v1
              (sut/indexable e a v :db.type/symbol g)
              (sut/indexable e1 a1 v1 :db.type/symbol g))))

(test/defspec string-avg-generative-test
  100
  (prop/for-all
    [e1 (gen/large-integer* {:min c/e0})
     a1 gen/nat
     v1 gen/string
     v  gen/string]
    (avg-test v a1 v1
              (sut/indexable e a v :db.type/string g)
              (sut/indexable e1 a1 v1 :db.type/string g))))

(test/defspec bigint-avg-generative-test
  100
  (prop/for-all
    [e1 (gen/large-integer* {:min c/e0})
     a1 gen/nat
     i1 gen-bigint
     i  gen-bigint]
    (let [^BigInteger v1 (.toBigInteger ^clojure.lang.BigInt i1)
          ^BigInteger v  (.toBigInteger ^clojure.lang.BigInt i)]
      (avg-test v a1 v1
                (sut/indexable e a v :db.type/bigint g)
                (sut/indexable e1 a1 v1 :db.type/bigint g)))))

(test/defspec bigdec-avg-generative-test
  100
  (prop/for-all
    [e1 (gen/large-integer* {:min c/e0})
     a1 gen/nat
     i1 gen-bigint
     i  gen-bigint]
    (let [^BigInteger u1 (.toBigInteger ^clojure.lang.BigInt i1)
          ^BigInteger u  (.toBigInteger ^clojure.lang.BigInt i)
          v1             (BigDecimal. u1 -10)
          v              (BigDecimal. u -10)]
      (avg-test v a1 v1
                (sut/indexable e a v :db.type/bigdec g)
                (sut/indexable e1 a1 v1 :db.type/bigdec g)))))

(test/defspec boolean-avg-generative-test
  100
  (prop/for-all
    [e1 (gen/large-integer* {:min c/e0})
     a1 gen/nat
     v1 gen/boolean
     v  gen/boolean]
    (avg-test v a1 v1
              (sut/indexable e a v :db.type/boolean g)
              (sut/indexable e1 a1 v1 :db.type/boolean g))))

(test/defspec long-avg-generative-test
  100
  (prop/for-all
    [e1 (gen/large-integer* {:min c/e0})
     a1 gen/nat
     v1 gen/large-integer
     v  gen/large-integer]
    (avg-test v a1 v1
              (sut/indexable e a v :db.type/long g)
              (sut/indexable e1 a1 v1 :db.type/long g))))

(test/defspec double-avg-generative-test
  100
  (prop/for-all
    [e1 (gen/large-integer* {:min c/e0})
     a1 gen/nat
     v1 (gen/double* {:NaN? false})
     v  (gen/double* {:NaN? false})]
    (avg-test v a1 v1
              (sut/indexable e a v :db.type/double g)
              (sut/indexable e1 a1 v1 :db.type/double g))))

(test/defspec float-avg-generative-test
  100
  (prop/for-all
    [e1 (gen/large-integer* {:min c/e0})
     a1 gen/nat
     v1 (gen/double* {:NaN? false})
     v  (gen/double* {:NaN? false})]
    (let [f1 (float v1)
          f  (float v)]
      (avg-test f a1 f1
                (sut/indexable e a f :db.type/float g)
                (sut/indexable e1 a1 f1 :db.type/float g)))))

(test/defspec uuid-avg-generative-test
  100
  (prop/for-all
    [v  gen/uuid]
    (let [^ByteBuffer bf (bf/allocate-buffer 16384)
          ^Indexable d   (sut/indexable e a v :db.type/uuid g)
          _              (.clear ^ByteBuffer bf)
          _              (sut/put-buffer bf d :avg)
          _              (.flip ^ByteBuffer bf)
          ^Retrieved r   (sut/read-buffer bf :avg)]
      (is (= a (.-a r)))
      (is (= v (.-v r))))))

(test/defspec bytes-avg-generative-test
  100
  (prop/for-all
    [v  (gen/such-that (partial bytes-size-less-than? c/+val-bytes-wo-hdr+)
                       gen/bytes)]
    (let [^ByteBuffer bf (bf/allocate-buffer 16384)
          ^Indexable d   (sut/indexable e a v :db.type/bytes g)
          _              (.clear ^ByteBuffer bf)
          _              (sut/put-buffer bf d :avg)
          _              (.flip ^ByteBuffer bf)
          ^Retrieved r   (sut/read-buffer bf :avg)]
      (is (= a (.-a r)))
      (is (Arrays/equals ^bytes v ^bytes (.-v r))))))

(defn data-size-less-than?
  [^long limit data]
  (< (alength ^bytes (sut/serialize data)) limit))

(test/defspec data-avg-generative-test
  100
  (prop/for-all
    [v  (gen/such-that (partial data-size-less-than? c/+val-bytes-wo-hdr+)
                       gen/any-equatable)]
    (let [^ByteBuffer bf (bf/allocate-buffer 16384)
          ^Indexable d   (sut/indexable e a v nil g)
          _              (.clear ^ByteBuffer bf)
          _              (sut/put-buffer bf d :avg)
          _              (.flip ^ByteBuffer bf)
          ^Retrieved r   (sut/read-buffer bf :avg)]
      (is (= a (.-a r)))
      (is (= v (.-v r))))))

(deftest bigint-roundtrip-test
  (let [x (BigInteger. "123456789123456789")]
    (is (= x (sut/decode-bigint (sut/encode-bigint x))))))

(deftest bitmap-roundtrip-test
  (let [rr  (RoaringBitmap/bitmapOf (int-array [1 2 3 1000]))
        rr1 (sut/bitmap [1 2 3 1000])
        bf  (bf/allocate-buffer 16384)]
    (is (= rr rr1))
    (is (= 1000 (.select rr 3)))
    (is (= (.getCardinality rr) 4))
    (sut/put-buffer bf rr :bitmap)
    (.flip bf)
    (let [^RoaringBitmap rr1 (sut/read-buffer bf :bitmap)]
      (is (.equals rr rr1)))
    (sut/bitmap-add rr 4)
    (is (= 4 (.select rr 3)))
    (sut/bitmap-del rr 4)
    (is (= 1000 (.select rr 3)))))

(deftest data-serialize-test
  ;; TODO somehow this doesn't work in graal
  (when-not (u/graal?)
    (let [d1  (DateTime.)
          bs1 (sut/serialize d1)]
      (is (instance? org.joda.time.DateTime (sut/deserialize bs1))))
    (is (thrown? Exception (sut/serialize (type :a))))
    (is (thrown? Exception (sut/serialize {:bar (defn bar [] :bar)})))
    (let [d (Semaphore. 1)]
      (is (thrown? Exception (sut/serialize d)))
      (binding [c/*data-serializable-classes*
                #{"java.util.concurrent.Semaphore"}]
        (let [bs (sut/serialize d)]
          (is (instance? java.util.concurrent.Semaphore
                         (sut/deserialize bs))))))))

(test/defspec base64-test
  100
  (prop/for-all [^bytes k (gen/not-empty gen/bytes)]
                (Arrays/equals k
                               ^bytes (sut/decode-base64
                                        (sut/encode-base64 k)))))

(deftest homo-tuple-round-trip-test
  (let [bf (ByteBuffer/allocateDirect c/+max-key-size+)
        t1 [:entities :id :name]
        t2 ["docs" "type" "mac"]
        t3 [-23 42 -97 10 10 1 24 8 1 9 39 19 4]
        t4 [(UUID/randomUUID) (UUID/randomUUID)]]
    (sut/put-buffer bf t1 [:keyword])
    (.flip bf)
    (is (= t1 (sut/read-buffer bf [:keyword])))
    (.clear bf)
    (sut/put-buffer bf t2 [:string])
    (.flip bf)
    (is (= t2 (sut/read-buffer bf [:string])))
    (.clear bf)
    (sut/put-buffer bf t3 [:long])
    (.flip bf)
    (is (= t3 (sut/read-buffer bf [:long])))
    (.clear bf)
    (sut/put-buffer bf t4 [:uuid])
    (.flip bf)
    (is (= t4 (sut/read-buffer bf [:uuid])))))

(deftest hete-tuple-round-trip-test
  (let [bf (ByteBuffer/allocateDirect c/+max-key-size+)
        t1 [:entities 1 :names "John" :id]
        t2 ["docs" 1 "types" :mac :id (UUID/randomUUID)]
        t3 [42 0.5 "id"]
        t4 [(UUID/randomUUID) :key 1]]
    (sut/put-buffer bf t1 [:keyword :long :keyword :string :keyword])
    (.flip bf)
    (is (= t1
           (sut/read-buffer bf [:keyword :long :keyword :string :keyword])))
    (.clear bf)
    (sut/put-buffer bf t2 [:string :long :string :keyword :keyword :uuid])
    (.flip bf)
    (is (= t2
           (sut/read-buffer
             bf [:string :long :string :keyword :keyword :uuid])))
    (.clear bf)
    (sut/put-buffer bf t3 [:long :float :string])
    (.flip bf)
    (is (= t3 (sut/read-buffer bf [:long :float :string])))
    (.clear bf)
    (sut/put-buffer bf t4 [:uuid :keyword :long])
    (.flip bf)
    (is (= t4 (sut/read-buffer bf [:uuid :keyword :long])))))

(def ts1 "datalevin")
(def tk1 :rocks)
(def tl1 42)
(def tf1 10.0)

(test/defspec hete-tuple-generative-test
  100
  (prop/for-all
    [ts gen/string-alphanumeric
     tk gen/keyword
     tl gen/int
     tf (gen/double* {:NaN? false :min Float/MIN_VALUE :max Float/MAX_VALUE})]
    (let [^ByteBuffer bf  (bf/allocate-buffer c/+max-key-size+)
          ^ByteBuffer bf1 (bf/allocate-buffer c/+max-key-size+)
          _               (.clear ^ByteBuffer bf)
          _               (sut/put-buffer bf [ts tk tl tf]
                                          [:string :keyword :long :float])
          _               (.flip ^ByteBuffer bf)
          _               (.clear ^ByteBuffer bf1)
          _               (sut/put-buffer bf1 [ts1 tk1 tl1 tf1]
                                          [:string :keyword :long :float])
          _               (.flip ^ByteBuffer bf1)
          res             (bf/compare-buffer bf bf1)]
      (is (u/same-sign? res (u/combine-cmp (compare ts ts1)
                                           (compare tk tk1)
                                           (compare tl tl1)
                                           (compare tf tf1)))))))

(deftest raw-byte-buffer-input-test
  (let [^bytes payload (.getBytes "raw-bytes" StandardCharsets/UTF_8)
        expected (Arrays/copyOfRange payload 1 7)
        ^ByteBuffer src (doto (ByteBuffer/wrap payload)
                          (.position 1)
                          (.limit 7))
        ^ByteBuffer dst (bf/allocate-buffer 16)]
    (sut/put-bf dst src :raw)
    (is (= 6 (.remaining dst)))
    (is (Arrays/equals expected ^bytes (sut/read-buffer dst :raw)))
    ;; :raw should not mutate the source ByteBuffer position/limit.
    (is (= 1 (.position src)))
    (is (= 7 (.limit src)))))
