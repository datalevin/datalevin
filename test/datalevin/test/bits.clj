(ns datalevin.test.bits
  (:require
   [clojure.test :refer [deftest is]]
   [datalevin.bits :as b]
   [datalevin.constants :as c])
  (:import
   [java.nio ByteBuffer]
   [java.util Arrays]))

(defn- avg-buffer
  ^ByteBuffer [value value-type giant-id]
  (let [bf (ByteBuffer/allocate 1024)]
    (b/put-buffer bf (b/indexable 1 2 value value-type giant-id) :avg)
    (.flip bf)
    bf))

(deftest direct-avg-decode-test
  (let [normal (avg-buffer "inline" :db.type/string 42)
        giant  (avg-buffer (apply str (repeat 1024 "x"))
                           :db.type/string 42)]
    (is (= 2 (b/avg->aid normal)))
    (is (= c/normal (b/avg->giant-id normal)))
    (is (= "inline" (b/avg->inline-value normal)))
    (is (= 2 (b/avg->aid giant)))
    (is (= 42 (b/avg->giant-id giant)))))

(deftest empty-homogeneous-tuple-test
  (is (not (b/valid-data? [] [:string])))
  (let [bf    (ByteBuffer/allocate 16)
        ^Throwable error (try
                (b/put-buffer bf [] [:string])
                nil
                (catch clojure.lang.ExceptionInfo e e))]
    (is (= "Cannot store an empty homogeneous tuple" (.getMessage error)))
    (is (= {:error      :data/validation
            :value      []
            :tuple-type :string}
           (ex-data error)))))

(defn- tuple-bytes [value types]
  (let [bf (ByteBuffer/allocate 1024)]
    (b/put-buffer bf value types)
    (Arrays/copyOf (.array bf) (.position bf))))

(deftest int-tuple-codec-test
  (let [ints [Integer/MIN_VALUE -65536 -1 0 127 128 256 Integer/MAX_VALUE]
        heterogeneous (vec (for [i (cons nil ints), s ["" "a" "aa" "b"]] [i s]))
        homogeneous (vec (for [a ints, b ints] [a b]))]
    (doseq [[values types] [[heterogeneous [:int :string]]
                            [homogeneous [:int]]]]
      (let [encoded (mapv #(vector % (tuple-bytes % types)) values)]
        (doseq [[value ^bytes bytes] encoded]
          (is (b/valid-data? value types))
          (is (= value (b/read-buffer (ByteBuffer/wrap bytes) types))))
        (is (= (sort values)
               (map first (sort (fn [[_ ^bytes a] [_ ^bytes b]]
                                  (Arrays/compareUnsigned a b)) encoded))))))
    (is (= 4 (- (alength ^bytes (tuple-bytes [0 "value"] [:long :string]))
                (alength ^bytes (tuple-bytes [0 "value"] [:int :string])))))
    (doseq [i [(dec (long Integer/MIN_VALUE)) (inc (long Integer/MAX_VALUE))]]
      (is (not (b/valid-data? [i "value"] [:int :string])))
      (is (thrown? ArithmeticException (tuple-bytes [i "value"] [:int :string]))))
    (is (not (b/valid-data? [1.5 "value"] [:int :string])))
    (doseq [[sentinel value] [[:db.value/sysMin Integer/MIN_VALUE]
                              [:db.value/sysMax Integer/MAX_VALUE]]]
      (is (Arrays/equals ^bytes (tuple-bytes [sentinel ""] [:int :string])
                          ^bytes (tuple-bytes [value ""] [:int :string]))))
    ;; Existing standalone :int payloads are still raw big-endian integers.
    (let [bf (ByteBuffer/allocate 4)]
      (b/put-buffer bf -1 :int)
      (is (= [-1 -1 -1 -1] (vec (.array bf)))))))
