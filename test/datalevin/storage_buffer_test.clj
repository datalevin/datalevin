(ns datalevin.storage-buffer-test
  (:require
   [clojure.test :refer [deftest is testing]]
   [datalevin.bits :as b]
   [datalevin.constants :as c]
   [datalevin.core :as d]
   [datalevin.datom :as datom]
   [datalevin.util :as u]
   [taoensso.nippy :as nippy])
  (:import
   [java.io EOFException]
   [java.nio BufferOverflowException ByteBuffer]
   [java.util UUID]))

(defn- buffer ^ByteBuffer [direct? size]
  (if direct? (ByteBuffer/allocateDirect size) (ByteBuffer/allocate size)))

(deftest storage-buffer-byte-compatibility
  (let [cached (apply str (repeat 100 "cached"))
        values [nil false {:data [1 2 "東京 👋"]}
                (with-meta [{:same :same} #{:same :other}] {:same :same})
                [(nippy/cache cached) (nippy/cache cached)]
                (datom/datom 1 :name "example")]]
    (doseq [direct? [false true]
            value values]
      (let [bytes (b/serialize value)
            size (alength bytes)
            expected (b/deserialize bytes)
            bf (doto (buffer direct? (+ 4 (* 2 size)))
                 (.putInt 1234))]
        ;; Each stored value gets its own cache, even when appended inside a
        ;; caller's cache scope. The old byte-array API is the format oracle.
        (nippy/with-cache (dotimes [_ 2] (b/put-buffer bf value :data)))
        (.flip bf)
        (is (= 1234 (.getInt bf)))
        (dotimes [i 2]
          (.limit bf (+ 4 (* (inc i) size)))
          (is (= (seq bytes) (seq (b/get-bytes (.duplicate bf)))))
          (let [restored (nippy/with-cache (b/read-buffer bf :data))]
            (is (= expected restored))
            (is (= (meta expected) (meta restored))))
          (is (= (.limit bf) (.position bf))))))))

(deftest storage-buffer-legacy-and-ownership
  (doseq [direct? [false true]
          freeze [b/serialize nippy/freeze]]
    (let [value {:text (apply str (repeat 2000 "legacy"))
                 :bytes (byte-array [1 2 3])}
          payload ^bytes (freeze value)
          bf (doto (buffer direct? (+ 4 (alength payload)))
               (.putInt 1234) (.put payload) (.flip) (.position 4))
          ;; Read-only slices exercise heap array offsets and direct buffers.
          slice (.asReadOnlyBuffer (.slice bf))
          restored (b/read-buffer slice :data)
          raw (b/read-buffer bf :raw)]
      (is (= (.limit slice) (.position slice)))
      (.clear bf)
      (while (.hasRemaining bf) (.put bf (byte 0)))
      (is (= (:text value) (:text restored)))
      (is (= [1 2 3] (vec (:bytes restored))))
      (is (= (seq payload) (seq raw)))))
  (testing "the full stored payload is consumed even if it has trailing bytes"
    (let [bf (doto (ByteBuffer/allocate 32)
               (b/put-buffer :value) (.putInt 1234) (.flip))]
      (is (= :value (b/read-buffer bf)))
      (is (not (.hasRemaining bf))))))

(deftest storage-buffer-excludes-index-trailers
  (doseq [direct? [false true]
          freeze [b/serialize nippy/freeze]
          truncated? [false true]]
    (let [value {:value "bounded"}
          payload ^bytes (freeze value)
          size (- (alength payload) (if truncated? 1 0))
          bf (doto (buffer direct? 1024)
               (.putInt 2) (.put payload 0 size)
               (.put (byte c/separator)) (.put (byte c/false-value)) (.flip))
          limit (.limit bf)]
      (if truncated?
        (is (thrown? Exception (b/avg->inline-value bf)))
        (is (= value (b/avg->inline-value bf))))
      (is (= limit (.limit bf)))
      (is (= (- limit 2) (.position bf)))
      (is (= c/separator (.get bf)))
      (is (= c/false-value (.get bf))))))

(deftest storage-decode-retains-original-error
  (let [payload (b/serialize {:data "truncated"})
        truncated (java.util.Arrays/copyOf payload (dec (alength payload)))]
    (doseq [decode [#(b/deserialize truncated)
                   #(b/deserialize-bf (ByteBuffer/wrap truncated))
                   #(b/deserialize-bf
                      (doto (ByteBuffer/allocateDirect (alength truncated))
                        (.put truncated) (.flip)))]]
      (let [error (try (decode) nil (catch Exception e e))
            original (some-> ^Throwable error .getSuppressed first)]
        (is (some? error))
        (is (re-find #"Thaw failed" (ex-message error)))
        (is (some? original))
        (is (some #(instance? EOFException %)
                  (take-while some? (iterate ex-cause original))))))))

(deftype BrokenStorageValue [])

(nippy/extend-freeze BrokenStorageValue ::broken-storage-value
  [_value _out] (throw (EOFException. "custom storage serializer failure")))

(deftest storage-buffer-overflow-and-retry
  (doseq [direct? [false true]]
    (let [value {:data (apply str (repeat 1000 "overflow")) :same :same}
          bytes (b/serialize value)
          bf (doto (buffer direct? (+ 4 (alength bytes)))
               (.putInt 1234) (.limit 16))]
      (is (thrown? BufferOverflowException (b/put-buffer bf value)))
      (is (= 4 (.position bf)))
      (is (= 1234 (.getInt bf 0)))
      (.limit bf (.capacity bf))
      (b/put-buffer bf value)
      (.flip bf)
      (.position bf 4)
      (is (= (seq bytes) (seq (b/get-bytes (.duplicate bf)))))
      (is (= value (b/read-buffer bf)))
      (.clear bf)
      (is (thrown-with-msg? EOFException #"custom storage serializer failure"
                           (b/put-buffer bf (BrokenStorageValue.))))
      (is (zero? (.position bf)))
      (is (thrown? Exception (b/put-buffer bf String))))))

(deftest storage-buffer-value-growth-and-reopen
  (let [dir (u/tmp-dir (str "storage-buffer-" (UUID/randomUUID)))
        value {:data (apply str (repeat 10000 "growth"))}
        legacy {:old [1 2 "three"]}
        db (d/open-kv dir {:wal? false})]
    (try
      (d/open-dbi db "data" {:val-size 8})
      (d/transact-kv db [[:put "data" :large value]
                         [:put "data" :legacy (nippy/freeze legacy) :data :raw]])
      (is (= value (d/get-value db "data" :large)))
      (is (= legacy (d/get-value db "data" :legacy)))
      (is (thrown-with-msg? Exception #"Key cannot be larger than 511 bytes"
                           (d/transact-kv db [[:put "data" value :too-large]])))
      (d/close-kv db)
      (let [reopened (d/open-kv dir {:wal? false})]
        (try
          (d/open-dbi reopened "data")
          (is (= value (d/get-value reopened "data" :large)))
          (is (= legacy (d/get-value reopened "data" :legacy)))
          (is (= (seq (b/serialize value))
                 (seq (d/get-value reopened "data" :large :data :raw))))
          (finally (d/close-kv reopened))))
      (finally
        (d/close-kv db)
        (u/delete-files dir)))))
