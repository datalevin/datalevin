(ns datalevin.wal-append-test
  (:require [clojure.test :refer [deftest is testing]]
            [datalevin.txlog.codec :as codec]
            [datalevin.txlog.segment :as seg]
            [datalevin.util :as u])
  (:import [java.io ByteArrayOutputStream IOException RandomAccessFile]
           [java.nio ByteBuffer]
           [java.nio.channels FileChannel]
           [java.nio.file Files]
           [java.util Arrays]))

(deftest positioned-append-matches-record-format-and-large-fallback
  (let [dir (u/tmp-dir (str "wal-append-" (random-uuid)))
        path (str dir "/records.wal")
        expected (ByteArrayOutputStream.)]
    (u/create-dirs dir)
    (try
      (with-open [^FileChannel ch (seg/open-segment-channel path)]
        (loop [sizes [0 1 1024 8178 8179 65522 65523 1048577 1024]
               offset 0
               index 0]
          (when-let [size (first sizes)]
            (let [body (byte-array size (unchecked-byte index))
                  opts {:compressed? (odd? index)}
                  record (codec/encode-record body opts)
                  result (seg/write-record-at! ch offset body opts)]
              (.write expected record)
              (is (= offset (:offset result)))
              (is (= (alength ^bytes record) (:size result)))
              (is (= (:checksum (codec/decode-record-bytes record)) (:checksum result)))
              (recur (next sizes) (+ offset (:size result)) (inc index)))))
        ;; The legacy append API still uses file size and advances position,
        ;; including after the positioned writer left a different cursor.
        (let [body (byte-array [1 2 3])
              result (seg/append-record! ch body)]
          (.write expected (codec/encode-record body))
          (is (= (.position ch) (+ (:offset result) (:size result))))))
      (is (Arrays/equals (.toByteArray expected)
                         (Files/readAllBytes (.toPath (u/file path)))))
      (let [{:keys [records valid-end partial-tail?]} (seg/scan-segment path)]
        (is (= 10 (count records)))
        (is (= (.size expected) valid-end))
        (is (false? partial-tail?)))
      (is (<= (.capacity ^ByteBuffer (.get ^ThreadLocal @#'seg/tl-record-buffer)) 65536))
      (finally (u/delete-files dir)))))

(deftest positioned-append-uses-tracked-offset-in-preallocated-segment
  (let [dir (u/tmp-dir (str "wal-append-prealloc-" (random-uuid)))
        path (str dir "/records.wal")]
    (u/create-dirs dir)
    (try
      (with-open [file (RandomAccessFile. path "rw")]
        (.setLength file 65536))
      (with-open [^FileChannel ch (seg/open-segment-channel path true)]
        (.position ch 12345)
        (let [first-record (seg/write-record-at! ch 0 (byte-array [1 2]))
              last-record (seg/write-record-at! ch (:size first-record) (byte-array [3 4]))]
          (is (= 12345 (.position ch)))
          (is (= 65536 (.size ch)))
          (let [scan (seg/scan-segment path {:allow-preallocated-tail? true})]
            (is (= [[1 2] [3 4]] (mapv #(vec (:body %)) (:records scan))))
            (is (= (+ (:size first-record) (:size last-record)) (:valid-end scan))))))
      (finally (u/delete-files dir)))))

(defn- partial-channel [steps output calls]
  (proxy [FileChannel] []
    (write
      ([buffer]
       (throw (IOException. "Unexpected relative write")))
      ([^ByteBuffer buffer position]
       (let [step (first @steps)]
         (swap! steps next)
         (swap! calls conj position)
         (if (instance? Exception step)
           (throw step)
           (let [n (min (long (or step Long/MAX_VALUE)) (.remaining buffer))]
             (when (pos? n)
               (let [bs (byte-array n)]
                 (.get buffer bs)
                 (.write ^ByteArrayOutputStream output bs)))
             n)))))
    (position
      ([] (throw (IOException. "Unexpected channel position lookup")))
      ([_] (throw (IOException. "Unexpected seek"))))
    (implCloseChannel [])))

(deftest positioned-append-retries-partial-writes-and-propagates-failure
  (let [body (byte-array [10 20 30 40])
        record (codec/encode-record body)]
    (testing "partial writes advance their explicit offset"
      (let [output (ByteArrayOutputStream.)
            calls (atom [])]
        (with-open [ch (partial-channel (atom [3 4 5]) output calls)]
          (is (= (alength ^bytes record) (:size (seg/write-record-at! ch 100 body))))
          (is (= [100 103 107 112] @calls))
          (is (Arrays/equals record (.toByteArray output))))))
    (doseq [failure [0 -1 (IOException. "Disk failure")]]
      (testing (str "failed write " failure)
        (let [output (ByteArrayOutputStream.)
              calls (atom [])]
          (with-open [ch (partial-channel (atom [5 failure]) output calls)]
            (is (thrown? Exception (seg/write-record-at! ch 50 body)))
            (is (= [50 55] @calls))
            (is (= 5 (.size output)))))))
    (testing "the reusable buffer resets after a failed append"
      (let [output (ByteArrayOutputStream.)]
        (with-open [ch (partial-channel (atom []) output (atom []))]
          (seg/write-record-at! ch 0 (byte-array [1]))
          (is (Arrays/equals (codec/encode-record (byte-array [1]))
                             (.toByteArray output))))))))

(deftest append-buffers-are-isolated-between-writer-threads
  (let [dir (u/tmp-dir (str "wal-append-threads-" (random-uuid)))]
    (u/create-dirs dir)
    (try
      (let [workers (mapv
                      (fn [worker]
                        (future
                          (let [path (str dir "/" worker ".wal")]
                            (with-open [^FileChannel ch (seg/open-segment-channel path)]
                              (loop [index 0 offset 0]
                                (when (< index 100)
                                  (let [body (byte-array (+ 1000 index) (byte worker))
                                        result (seg/write-record-at! ch offset body)]
                                    (recur (inc index) (+ offset (:size result)))))))
                            (seg/scan-segment path))))
                      (range 4))]
        (doseq [[worker pending] (map-indexed vector workers)]
          (let [records (:records (deref pending 10000 nil))]
            (is (= 100 (count records)))
            (is (every? (fn [[index record]]
                          (Arrays/equals
                            (byte-array (+ 1000 index) (byte worker))
                            ^bytes (:body record)))
                        (map-indexed vector records))))))
      (finally (u/delete-files dir)))))

(deftest positioned-payload-buffer-handles-partial-and-failed-writes
  (doseq [steps [[2 3 4] [5 0] [5 -1] [5 (IOException. "Disk failure")]]]
    (let [bytes (byte-array [10 20 30 40])
          body (ByteBuffer/wrap (byte-array [99 10 20 30 40 99]))
          output (ByteArrayOutputStream.)
          calls (atom [])
          success? (= steps [2 3 4])]
      (.position body 1)
      (.limit body 5)
      (with-open [ch (partial-channel (atom steps) output calls)]
        (if success?
          (do
            (seg/write-record-at! ch 100 body)
            (is (= [100 102 105 109] @calls))
            (is (Arrays/equals (codec/encode-record bytes) (.toByteArray output))))
          (do
            (is (thrown? Exception (seg/write-record-at! ch 100 body)))
            (is (= [100 105] @calls))
            (is (= 5 (.size output)))))
        (is (= 5 (.position body) (.limit body)))))))
