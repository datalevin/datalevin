(ns datalevin.tx-group-batch-range-test
  (:require [clojure.test :refer [deftest is]]
            [datalevin.interface :as i]
            [datalevin.lmdb :as l]
            [datalevin.tx-group.batch :as batch]
            [datalevin.tx-group.batch.env :as env]
            [datalevin.tx-group.batch.private :as private]
            [datalevin.tx-group.batch.recovery :as recovery]
            [datalevin.tx-group.batch.stage :as stage]
            [datalevin.txlog :as wal]
            [datalevin.util :as u])
  (:import [java.io RandomAccessFile]
           [java.nio ByteBuffer]
           [java.nio.file Files]))

(defn- encoded ^bytes [n] (.array (.putLong (ByteBuffer/allocate 8) (long n))))
(defn- decoded [^bytes bytes] (.getLong (ByteBuffer/wrap bytes)))
(defn- pairs [rows] (mapv (fn [[key value]] [(decoded key) (decoded value)]) rows))
(deftest pinned-native-range-and-presence-share-the-transaction-view
  (let [dir (u/tmp-dir (str "m0-range-" (random-uuid)))
        environment (private/open! {:dir dir :db-identity (str (random-uuid))
                                    :snapshot-scheduler? false})
        collector (env/collector environment)]
    (try
      (batch/submit! collector
                     {:op (fn [tx]
                            (doseq [k [1 3 5 7]]
                              (stage/tx-put! tx "data" (encoded k) (encoded (* k 10)))))})
      (let [retained (volatile! nil)
            result (batch/submit!
                    collector
                    {:op (fn [tx]
                           (vreset! retained tx)
                           (stage/tx-del! tx "data" (encoded 1))
                           (stage/tx-put! tx "data" (encoded 2) (encoded 20))
                           (stage/tx-put! tx "data" (encoded 3) (encoded 33))
                           (is (stage/tx-exists? tx "data" (encoded 2)))
                           (is (not (stage/tx-exists? tx "data" (encoded 1))))
                           (is (stage/tx-exists? tx "data" (encoded 7)))
                           (stage/tx-range tx "data" (encoded 1) (encoded 7) 3))})]
        (is (= [[2 20] [3 33] [5 50]] (pairs result)))
        (aset-byte ^bytes (second (first result)) 7 (byte 99))
        (is (= 20 (decoded (i/get-value (:raw (env/resources environment))
                                      "data" (encoded 2) :raw :raw))))
        (is (= :txlog/transaction-view-invalidated
               (:error (try (stage/tx-exists? @retained "data" (encoded 2))
                            nil (catch Throwable t (ex-data t))))))
        (is (= :txlog/transaction-view-invalidated
               (:error (try (stage/tx-range @retained "data" (encoded 1) (encoded 7) 3)
                            nil (catch Throwable t (ex-data t)))))))
      (finally (env/close! environment) (u/delete-files dir)))))

(deftest crc-valid-history-still-rejects-unsupported-physical-rows
  (doseq [row [[:put "data" (encoded 1) (encoded 9) :raw :raw [:nooverwrite]]
               [:put "data" (byte-array 512) (encoded 9) :raw :raw]]]
    (let [dir (u/tmp-dir (str "m0-replay-shape-" (random-uuid)))
          opts {:dir dir :db-identity (str (random-uuid)) :snapshot-scheduler? false
                :wal-segment-prealloc? false}
          environment (private/open! opts)]
      (try
        (batch/submit! (env/collector environment)
                       {:op #(stage/tx-put! % "data" (encoded 1) (encoded 7))})
        (env/close! environment)
        (let [file (:file (last (wal/segment-files (recovery/wal-dir opts))))
              offset (:offset (first (:records (wal/scan-segment (.getPath file)))))
              replacement (wal/encode-record (wal/encode-commit-row-payload 1 0 [row]))]
          (with-open [output (RandomAccessFile. file "rw")]
            (.seek output (long offset))
            (.write output ^bytes replacement)
            (.setLength output (+ (long offset) (alength ^bytes replacement))))
          (is (= 1 (count (:records (wal/scan-segment (.getPath file))))))
          (let [before (Files/readAllBytes (.toPath file))
                error (try (let [unexpected (private/open! opts)]
                             (env/close! unexpected) nil)
                           (catch Throwable t (ex-data t)))]
            (is (= :txlog/recovery-history-invalid (:error error)))
            (is (= :corrupt-record (:reason error)))
            (is (java.util.Arrays/equals before (Files/readAllBytes (.toPath file))))))
        (finally (env/close! environment) (u/delete-files dir))))))

(deftest name-bytes-are-covered-before-encoding-rmw-rows
  (doseq [name [(apply str (repeat 511 "a"))
                (apply str (repeat 169 "\u4e00"))]
          count [1 8 128]]
    (let [rows (vec (repeat count (l/kv-tx :put name (byte-array [1])
                                          (byte-array 0) :raw :raw)))
          estimate (#'private/rows-cost rows)
          body (wal/prepare-append-body rows {})]
      (is (<= (alength ^bytes body) estimate)))))
