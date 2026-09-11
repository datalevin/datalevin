(ns datalevin.compression-test
  (:require [clojure.java.io :as io]
            [clojure.test :refer [deftest is testing]]
            [datalevin.binding.cpp]
            [datalevin.compress :as cp]
            [datalevin.constants :as c]
            [datalevin.hu :as hu]
            [datalevin.interface :as i]
            [datalevin.lmdb :as l]
            [datalevin.util :as u])
  (:import [java.io ByteArrayInputStream ByteArrayOutputStream DataOutputStream]
           [java.nio ByteBuffer ByteOrder]
           [java.nio.file Files]
           [java.util Arrays Random]))

(defn- balanced-codes!
  "Build a full alphabetic tree directly, independently of the code trainer."
  [^bytes lens ^ints codes start n prefix prefix-length]
  (let [depth (- 31 (Integer/numberOfLeadingZeros n))
        extra (- n (bit-shift-left 1 depth))]
    (dotimes [i n]
      (let [split? (< i (* 2 extra))
            len (if split? (inc depth) depth)
            code (if split? i (- i extra))]
        (aset-byte lens (+ start i) (byte (+ prefix-length len)))
        (aset-int codes (+ start i)
                  (unchecked-int (bit-or (bit-shift-left prefix len) code)))))))

(defn- dictionary [hot]
  (let [n hu/symbol-count lens (byte-array n) codes (int-array n)]
    (case hot
      :last
      (do (balanced-codes! lens codes 0 (dec n) 0 1)
          (aset-byte lens (dec n) (byte 1))
          (aset-int codes (dec n) 1))
      :deep
      (do (dotimes [i 15]
            (aset-byte lens i (byte (inc i)))
            (aset-int codes i (int (- (bit-shift-left 1 (inc i)) 2))))
          (balanced-codes! lens codes 15 (- n 15) 32767 15))
      (balanced-codes! lens codes 0 n 0 0))
    (hu/codes->hu-tucker lens codes)))

(def ^:private ordered (delay (dictionary nil)))
(def ^:private ordered-short (delay (dictionary :last)))
(def ^:private ordered-deep (delay (dictionary :deep)))

(defn- bytes-of [xs] (byte-array (map unchecked-byte xs)))

(defn- encode [ht ^bytes raw]
  (let [src (ByteBuffer/wrap raw) dst (ByteBuffer/allocate (+ 32 (* 5 (alength raw))))]
    (hu/encode ht src dst)
    (Arrays/copyOf (.array dst) (.position dst))))

(defn- decode [ht ^bytes encoded size]
  (let [src (ByteBuffer/wrap encoded) dst (ByteBuffer/allocate size)]
    (hu/decode ht src dst)
    (is (= (.limit src) (.position src)))
    (Arrays/copyOf (.array dst) (.position dst))))

(defn- words [n]
  (if (zero? n) [[]]
      (for [prefix (words (dec n)) b [0 1 128 255]] (conj prefix b))))

(deftest ordered-prefixes-and-unsigned-bytes
  (let [raw (map bytes-of
                 (concat (mapcat words (range 5))
                         (map vector (range 256))
                         [[1] [1 0 0] [255] [255 0] [255 0 0]]
                         [(repeat 510 0) (repeat 511 0)
                          (repeat 510 255) (repeat 511 255)]))
        sorted (sort #(Arrays/compareUnsigned ^bytes %1 ^bytes %2) raw)]
    (doseq [ht [@ordered @ordered-short @ordered-deep]]
      (let [pairs (mapv #(vector % (encode ht %)) sorted)]
        (doseq [[[a ea] [b eb]] (partition 2 1 pairs)]
          (is (= (Integer/signum (Arrays/compareUnsigned ^bytes a ^bytes b))
                 (Integer/signum (Arrays/compareUnsigned ^bytes ea ^bytes eb)))))))))

(deftest short-codes-and-exact-output-buffers
  (let [rng (Random. 20260910)
        random-bytes (for [n (range 512)]
                       (let [raw (byte-array n)]
                         (.nextBytes rng raw)
                         raw))
        cases (concat random-bytes
                      (for [n [0 1 2 3 8 16 63 64 510 511] b [0 1 128 255]]
                        (bytes-of (repeat n b))))]
    (doseq [ht [@ordered @ordered-short @ordered-deep]
            ^bytes raw cases]
      (is (Arrays/equals raw (decode ht (encode ht raw) (alength raw)))))))

(deftest uniform-trained-dictionary
  ;; This complete alphabet previously exhausted a 1 GiB heap in the builder.
  (let [ht (hu/new-hu-tucker (cp/init-key-freqs))
        keys (mapv bytes-of [[] [0] [0 0] [0 0 0] [1] [1 0] [255] [255 255]])]
    (doseq [^bytes raw keys]
      (is (Arrays/equals raw (decode ht (encode ht raw) (alength raw)))))
    (doseq [[a b] (partition 2 1 keys)]
      (is (neg? (Arrays/compareUnsigned ^bytes (encode ht a) ^bytes (encode ht b)))))))

(deftest short-code-fixtures
  ;; The FF FF pair has code 1; the end-of-key code is eighteen zero bits.
  (doseq [[raw encoded] [[[255 255 255 255 255 255 255 255] [240 0 0]]
                         [[255 255 255 255 255 255 255] [239 240 0]]
                         [[255 255 255 255 255 255 0 0] [224 0 16 0 0]]]]
    (is (Arrays/equals (bytes-of raw)
                       (decode @ordered-short (bytes-of encoded) (count raw))))
    (is (Arrays/equals (bytes-of encoded) (encode @ordered-short (bytes-of raw))))))

(deftest buffer-positions-and-byte-order
  (doseq [ht [@ordered @ordered-short @ordered-deep]
          raw [[0 0 255] [255 128 0 0] []]]
    (let [raw (bytes-of raw)
          src (doto (ByteBuffer/allocate (+ 5 (alength raw)))
                (.order ByteOrder/LITTLE_ENDIAN) (.position 5) (.put raw)
                (.flip) (.position 5))
          dst (doto (ByteBuffer/allocate 100) (.order ByteOrder/LITTLE_ENDIAN)
                (.position 7))]
      (hu/encode ht src dst)
      (let [end (.position dst)
            out (doto (ByteBuffer/allocate (+ 9 (alength raw)))
                  (.order ByteOrder/LITTLE_ENDIAN) (.position 9))]
        (.limit dst end)
        (.position dst 7)
        (hu/decode ht dst out)
        (is (= end (.position dst)))
        (is (= (+ 9 (alength raw)) (.position out)))
        (is (Arrays/equals raw (Arrays/copyOfRange (.array out) 9 (.position out))))))))

(deftest terminal-symbol-frequencies
  (let [freqs (cp/init-key-freqs)]
    (is (= hu/symbol-count c/+key-compress-num-symbols+ (alength freqs)))
    (doseq [raw [[] [255] [0 0] [0 0 255]]]
      (#'cp/collect-keys freqs (ByteBuffer/wrap (bytes-of raw))))
    (is (= 3 (aget freqs hu/end-symbol))))
  (let [freqs (cp/init-key-freqs)]
    (#'cp/collect-keys freqs (ByteBuffer/wrap (bytes-of [0 0 255])))
    (is (= 2 (aget freqs (hu/pair-symbol 0))))
    (is (= 2 (aget freqs (hu/final-byte-symbol 255))))
    (is (= 1 (aget freqs hu/end-symbol)))))

(defn- serialized-dictionary [^datalevin.hu.HuTucker ht]
  (let [out (ByteArrayOutputStream.)]
    (with-open [data (DataOutputStream. out)]
      (.write data (.getBytes "HUTU" "US-ASCII"))
      (.writeByte data 1)
      (.writeByte data 0)
      (.writeShort data 0)
      (.writeInt data (alength ^bytes (.-lens ht)))
      (.writeInt data (alength ^ints (.-codes ht)))
      (.write data ^bytes (.-lens ht))
      (doseq [code (.-codes ht)] (.writeInt data (Integer/reverseBytes code))))
    (.toByteArray out)))

(deftest dictionary-roundtrip
  (doseq [ht [@ordered @ordered-short]]
    (let [ht ^datalevin.hu.HuTucker ht
          fixture (serialized-dictionary ht)
          loaded (hu/load-hu-tucker (ByteArrayInputStream. fixture))
          path (Files/createTempFile "datalevin-hu-" ".bin"
                                     (make-array java.nio.file.attribute.FileAttribute 0))]
      (try
        (hu/dump-hu-tucker loaded (str path))
        (is (Arrays/equals fixture (Files/readAllBytes path)))
        (let [reopened (hu/load-hu-tucker (io/input-stream (str path)))
              raw (bytes-of [0 0 0 0 255])]
          (is (Arrays/equals raw (decode reopened (encode ht raw) 5))))
        (finally (Files/deleteIfExists path))))))

(deftest compressed-prefix-keys
  (let [dir (str (Files/createTempDirectory
                   "datalevin-compressed-prefix-"
                   (make-array java.nio.file.attribute.FileAttribute 0)))
        keys [[0] [0 0] [0 0 0] [1] [1 0] [1 0 0] [128] [255] [255 0]]
        opts {:key-compress :hu :flags (conj c/default-env-flags :nosync)}
        check (fn [db]
                (is (= keys (mapv (fn [[k _]] (mapv #(bit-and 255 %) k))
                                 (i/get-range db "keys" [:all] :bytes :long))))
                (is (= [3 4 5] (mapv second
                                     (i/get-range db "keys"
                                                  [:closed (bytes-of [1])
                                                   (bytes-of [1 0 0])]
                                                  :bytes :long))))
                (doseq [[id k] (map-indexed vector keys)]
                  (is (= id (i/get-value db "keys" (bytes-of k) :bytes :long)))))]
    (try
      (hu/dump-hu-tucker @ordered (str dir u/+separator+ c/keycode-file-name))
      (let [db (l/open-kv dir opts)]
        (try
          (i/open-dbi db "keys")
          (i/transact-kv db (reverse (map-indexed
                                      (fn [id k] [:put "keys" (bytes-of k) id :bytes :long])
                                      keys)))
          (check db)
          (finally (i/close-kv db))))
      (let [db (l/open-kv dir)]
        (try (check db) (finally (i/close-kv db))))
      (finally (u/delete-files dir)))))

(defmacro with-kv [[binding dir opts] & body]
  `(let [~binding (l/open-kv ~dir ~opts)]
     (try ~@body (finally (i/close-kv ~binding)))))

(defmacro with-directory [[binding] & body]
  `(let [~binding (str (Files/createTempDirectory
                        "datalevin-compression-"
                        (make-array java.nio.file.attribute.FileAttribute 0)))]
     (try ~@body (finally (u/delete-files ~binding)))))

(defn- write-dictionaries! [dir]
  (hu/dump-hu-tucker @ordered (str (io/file dir c/keycode-file-name)))
  ;; Zstd accepts a raw-content dictionary; no training is needed for this test.
  (u/dump-bytes (str (io/file dir c/valcode-file-name))
                (.getBytes "repeatable test payload dictionary" "UTF-8")))

(deftest ordered-duplicate-value-compression-rejected
  (with-directory [dir]
    (write-dictionaries! dir)
    (with-kv [db dir {:val-compress :zstd}]
      (is (thrown-with-msg? Exception #"ordered duplicate"
                           (i/open-dbi db "strings" {:flags [:create :dupsort]})))
      (is (not (some #{"strings"} (i/list-dbis db))))
      (is (nil? (get-in @(i/kv-info db) [:dbis "strings"])))
      (i/open-dbi db "ids" {:flags [:create :dupsort :dupfixed]})
      (i/transact-kv db [[:put-list "ids" 1 [4 1 3 2] :long :long]])
      (is (= [[1 2] [1 3]]
             (i/list-range db "ids" [:all] :long [:closed 2 3] :long))))
    (with-kv [db dir {}]
      (is (thrown-with-msg? Exception #"ordered duplicate"
                           (i/open-dbi db "strings" {:flags [:create :dupsort]})))
      (is (= [[1 2] [1 3]]
             (i/list-range db "ids" [:all] :long [:closed 2 3] :long)))))
  (with-directory [dir]
    (with-kv [db dir {}]
      (i/open-dbi db "strings" {:flags [:create :dupsort]})
      (i/transact-kv db [[:put-list "strings" 1 ["aa" "ab" "b" "ba" "bb" "c"]
                         :long :string]])
      (is (= [[1 "b"] [1 "ba"] [1 "bb"]]
             (i/list-range db "strings" [:all] :long [:closed "b" "bb"] :string))))
    (write-dictionaries! dir)
    (is (thrown-with-msg? Exception #"conflicts|rebuild"
                         (l/open-kv dir {:val-compress :zstd})))
    (with-kv [db dir {}]
      (is (= [[1 "b"] [1 "ba"] [1 "bb"]]
             (i/list-range db "strings" [:all] :long [:closed "b" "bb"] :string))))))

(deftest compression-reopen-and-copy
  (doseq [opts [{:key-compress :hu} {:val-compress :zstd}
               {:key-compress :hu :val-compress :zstd}]]
    (testing (str opts)
      (with-directory [dir]
        (with-directory [dest]
          (write-dictionaries! dir)
          (let [payload (apply str (repeat 2000 "repeatable test payload"))
                check (fn [db]
                        (is (= payload (i/get-value db "records" "a" :string :string)))
                        (is (= [["a" payload] ["b" "second"]]
                               (i/get-range db "records" [:all] :string :string)))
                        (is (= 2 (i/key-range-count db "records" [:closed "a" "b"] :string)))
                        (is (= 1 (i/get-rank db "records" "b" :string)))
                        (is (= ["b" "second"]
                               (i/get-by-rank db "records" 1 :string :string false)))
                        ;; Both keyed and bounded metadata reads must remain raw.
                        (is (= c/default-dbi-flags (:flags (i/get-value db c/kv-info
                                                [:dbis "records"] [:keyword :string] :data))))
                        (is (= 1 (count (i/get-range db c/kv-info
                                         [:closed [:dbis "records"] [:dbis "records"]]
                                         [:keyword :string] :data)))))]
            (with-kv [db dir opts]
              (i/open-dbi db "records")
              (i/transact-kv db [[:put "records" "a" payload :string :string]
                                 [:put "records" "b" "second" :string :string]])
              (check db))
            (with-kv [db dir {}]
              (check db)
              (i/open-dbi db "later")
              (i/transact-kv db [[:put "later" :k :v]])
              (i/copy db dest))
            (with-kv [db dest {}]
              (check db)
              (is (= :v (i/get-value db "later" :k)))))
          (with-kv [db dir opts]
            (is (= :v (i/get-value db "later" :k)))))))))

(deftest compression-open-rejects-missing-or-mismatched-dictionaries
  (doseq [[option filename] [[:key-compress c/keycode-file-name]
                             [:val-compress c/valcode-file-name]]]
    (with-directory [dir]
      (let [opts {option (if (= option :key-compress) :hu :zstd)}
            file (io/file dir filename)]
        (is (thrown-with-msg? Exception #"dictionary is missing" (l/open-kv dir opts)))
        (write-dictionaries! dir)
        ;; A failed open must release native resources and the local handle.
        (with-kv [db dir opts]
          (i/open-dbi db "data")
          (i/transact-kv db [[:put "data" :hello :world]]))
        (is (thrown-with-msg? Exception #"conflicts" (l/open-kv dir {option :none})))
        (let [original (Files/readAllBytes (.toPath file))]
          (Files/delete (.toPath file))
          (is (thrown-with-msg? Exception #"dictionary is missing" (l/open-kv dir)))
          (u/dump-bytes (str file) (byte-array [1 2 3]))
          (is (thrown-with-msg? Exception #"checksum" (l/open-kv dir)))
          (u/dump-bytes (str file) original)
          (with-kv [db dir {}]
            (is (= :world (i/get-value db "data" :hello)))))))))

(deftest compression-manifest-validation
  (with-directory [dir]
    (write-dictionaries! dir)
    (let [manifest (with-kv [db dir {:key-compress :hu}]
                     (i/open-dbi db "data")
                     (i/transact-kv db [[:put "data" :hello :world]])
                     (i/get-value db c/kv-info :compression))]
      (with-kv [db dir {}]
        (is (= manifest (i/get-value db c/kv-info :compression)))
        (i/transact-kv db [[:put c/kv-info :compression
                           (dissoc manifest :generation)]]))
      (is (thrown-with-msg? Exception #"Invalid or unsupported compression manifest"
                           (l/open-kv dir)))))
  (with-directory [dir]
    (with-kv [db dir {}]
      (i/open-dbi db "data")
      (i/transact-kv db [[:put "data" :hello :world]
                         [:del c/kv-info :compression]]))
    (write-dictionaries! dir)
    (is (thrown-with-msg? Exception #"rebuilding into a new environment"
                         (l/open-kv dir {:key-compress :hu})))
    ;; Existing raw environments can acquire raw metadata without a data rewrite.
    (with-kv [db dir {}]
      (is (= :world (i/get-value db "data" :hello)))
      (is (= :none (get-in (i/get-value db c/kv-info :compression) [:key :method]))))))

(deftest compressed-key-binding-size-limit
  ;; At most 32 bits per pair/terminal: 253 raw bytes always fit in 508 bytes.
  ;; A 511-byte raw key need not fit; compression cannot promise otherwise.
  (with-directory [dir]
    (hu/dump-hu-tucker @ordered-deep (str (io/file dir c/keycode-file-name)))
    (with-kv [db dir {:key-compress :hu}]
      (i/open-dbi db "keys")
      (let [;; The 00 20 pair has a 32-bit code, the final 20 a 31-bit code.
            fits (bytes-of (concat (mapcat identity (repeat 126 [0 32])) [32]))
            overflows (bytes-of (concat (mapcat identity (repeat 255 [0 32])) [32]))]
        (is (= 508 (alength ^bytes (encode @ordered-deep fits))))
        (is (> (alength ^bytes (encode @ordered-deep overflows)) 511))
        (i/transact-kv db [[:put "keys" fits 1 :raw :long]])
        (is (= 1 (i/get-value db "keys" fits :raw :long)))
        (is (thrown-with-msg? Exception #"511 bytes"
                             (i/transact-kv db [[:put "keys" fits 2 :raw :long]
                                                [:put "keys" overflows 3 :raw :long]])))
        (is (= 1 (i/get-value db "keys" fits :raw :long)))
        (is (= 1 (i/entries db "keys")))))))

(deftest invalid-dictionaries-and-keys
  (let [fixture (serialized-dictionary @ordered)]
    (doseq [[offset value] [[4 2] [5 1] [7 1] [11 0]]]
      (let [bad (aclone fixture)]
        (aset-byte bad offset (byte value))
        (is (thrown? Exception (hu/load-hu-tucker (ByteArrayInputStream. bad)))))))
  (is (thrown? Exception (decode @ordered (byte-array 0) 1)))
  (let [encoded (encode @ordered (bytes-of [1 0 0]))]
    (is (thrown? Exception (decode @ordered (Arrays/copyOf encoded (dec (alength encoded))) 3)))
    (is (thrown? Exception (decode @ordered (Arrays/copyOf encoded (inc (alength encoded))) 3)))))
