(ns datalevin.tx-group-batch-recovery-test
  (:require [clojure.java.io :as io]
            [clojure.test :refer [deftest is]]
            [datalevin.bits :as bits]
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
           [java.nio.file Files]
           [java.util Arrays]
           [java.util.concurrent CountDownLatch TimeUnit]))

(defn- encoded [n]
  (let [buffer (ByteBuffer/allocate 9)]
    (bits/put-buffer buffer n :long)
    (.array buffer)))

(defn- options []
  {:dir (u/tmp-dir (str "batch-recovery-" (random-uuid)))
   :db-identity (str (random-uuid)) :wal-sync-mode :fsync
   :wal-durability-profile :strict :wal-segment-prealloc? false
   :snapshot-scheduler? false})

(defn- write! [environment n]
  (batch/submit! (env/collector environment)
                 {:op (fn [tx]
                        (stage/tx-put! tx "data" (encoded 1) (encoded n))
                        n)}))

(defn- value [environment]
  (i/get-value (:raw (env/resources environment)) "data" 1 :long :long))

(defn- snapshot! [environment]
  ((:snapshot! (env/resources environment))))

(defn- wal-file [opts]
  (:file (last (wal/segment-files (recovery/wal-dir opts)))))

(defn- bytes-of [file] (Files/readAllBytes (.toPath (io/file file))))

(defn- flip! [file offset]
  (with-open [f (RandomAccessFile. (io/file file) "rw")]
    (.seek f offset)
    (let [byte (.read f)]
      (.seek f offset)
      (.write f (int (bit-xor byte 1))))))

(deftest malformed-current-manifest-uses-verified-previous
  (let [opts (options) environment (private/open! opts)]
    (try
      (write! environment 7)
      (snapshot! environment)
      (env/close! environment)
      (spit (io/file (recovery/root opts) "current" "snapshot.edn") "{:payload [")
      (let [reopened (private/open! opts)]
        (try
          (is (= 7 (value reopened)))
          (is (= 0 (:floor-lsn (:recovery (env/resources reopened)))))
          (finally (env/close! reopened))))
      (finally (env/close! environment) (u/delete-files (:dir opts))))))

(deftest unsupported-catalog-rejects-before-wal-without-fencing
  (let [opts (options) environment (private/open! opts)
        c (env/collector environment)
        state (:wal-state (env/resources environment))]
    (try
      (let [error (try
                    (batch/submit! c {:op #(stage/tx-put! % "undeclared" (encoded 1) (encoded 2))})
                    nil (catch Throwable t (ex-data t)))]
        (is (= :txlog/unsupported-private-operation (:error error)))
        (is (= :not-committed (:outcome error)))
        (is (= 1 @(:next-lsn state)))
        (is (batch/serving? c))
        (is (= 9 (write! environment 9))))
      (finally (env/close! environment) (u/delete-files (:dir opts))))))

(deftest invalid-physical-rows-cannot-poison-required-history
  (let [opts (options) environment (private/open! opts)
        c (env/collector environment) state (:wal-state (env/resources environment))]
    (try
      (doseq [key [(byte-array 0) (byte-array 512)]]
        (let [error (try
                      (batch/submit! c {:allowance 65536
                                        :prepare (fn [_]
                                                   {:rows [(l/kv-tx :put "data" key (encoded 2) :raw :raw)]
                                                    :wal-body (byte-array 1)})})
                      nil (catch Throwable t (ex-data t)))]
          (is (= :kv/invalid-encoded-size (:error error)))
          (is (= :not-committed (:outcome error)))
          (is (= 1 @(:next-lsn state)))
          (is (batch/serving? c))))
      (let [error (try (batch/submit! c {:op #(stage/tx-put! % "data" (byte-array 512) (encoded 2))})
                       nil (catch Throwable t (ex-data t)))]
        (is (= :kv/invalid-encoded-size (:error error)))
        (is (= 1 @(:next-lsn state))))
      (write! environment 7)
      (env/close! environment)
      (let [reopened (private/open! opts)]
        (try (is (= 7 (value reopened)))
             (finally (env/close! reopened))))
      (finally (env/close! environment) (u/delete-files (:dir opts))))))

(deftest unrecoverable-capacity-options-fail-before-opening
  (let [opts (options)]
    (try
      (doseq [overrides [{:wal-retained-max-bytes 511 :wal-segment-max-bytes 256}
                         {:write-batch-max-bytes (* 16 1024 1024)}]]
        (is (= :txlog/write-protocol-limits
               (:error (try (private/open! (merge opts overrides))
                            nil (catch Throwable t (ex-data t)))))))
      (is (not (.exists (io/file (:dir opts) "data.mdb"))))
      (finally (when (.exists (io/file (:dir opts)))
                 (u/delete-files (:dir opts)))))))

(deftest every-start-replays-wal-and-discards-overlay
  (let [opts (options) first (private/open! opts) calls (atom 0)]
    (try
      (batch/submit! (env/collector first)
                     {:op (fn [tx]
                            (swap! calls inc)
                            (stage/tx-put! tx "data" (encoded 1) (encoded 7)))})
      ;; Deliberately poison the disposable overlay without adding WAL.
      (i/transact-kv (:raw (env/resources first))
                     [[:put "data" 1 999 :long :long]
                      [:put "data" 2 999 :long :long]])
      (env/close! first)
      (let [second (private/open! opts)]
        (try
          (is (= 7 (value second)))
          (is (nil? (i/get-value (:raw (env/resources second)) "data" 2 :long :long)))
          (is (= 1 @calls) "recovery never invokes a body")
          (is (= 1 (batch/published-lsn (env/collector second))))
          (let [state (:wal-state (env/resources second))
                sync (wal/sync-manager-state (:sync-manager state))]
            (is (= 2 @(:next-lsn state)))
            (is (= 1 (:last-appended-lsn sync) (:last-durable-lsn sync)))
            (is (zero? (:unsynced-count sync))))
          (batch/submit! (env/collector second)
                         {:op #(stage/tx-del! % "data" (encoded 1))})
          (finally (env/close! second))))
      (let [third (private/open! opts)]
        (try (is (nil? (value third)))
             (is (= 2 (:records (:recovery (env/resources third)))))
             (finally (env/close! third))))
      (finally (u/delete-files (:dir opts))))))

(deftest conservative-floor-and-post-copy-force
  (let [opts (options) environment (private/open! opts)
        copied (CountDownLatch. 1) release (CountDownLatch. 1)]
    (try
      (write! environment 1)
      (let [copy (with-redefs [recovery/phase!
                              (fn [event _]
                                (when (= event :after-copy)
                                  (.countDown copied)
                                  (.await release 5 TimeUnit/SECONDS)))]
                   ;; Keep the redefinition live until the asynchronous copy
                   ;; has finished; futures do not inherit this test's bindings.
                   (let [copy (future (snapshot! environment))]
                     (is (.await copied 5 TimeUnit/SECONDS))
                     (is (= 2 (write! environment 2))
                         "no activation barrier while the snapshot is in flight")
                     (.countDown release)
                     @copy))]
        (is (= 1 (:floor-lsn copy)))
        (is (<= 2 (:durable-lsn copy))))
      (env/close! environment)
      (let [reopened (private/open! opts)]
        (try
          (is (= 2 (value reopened)))
          (is (= 1 (:floor-lsn (:recovery (env/resources reopened)))))
          (is (= 1 (:records (:recovery (env/resources reopened)))))
          (finally (env/close! reopened))))
      (finally (.countDown release) (u/delete-files (:dir opts))))))

(deftest required-interior-and-complete-final-corruption-fail-closed
  (doseq [record-idx [1 2]]
    (let [opts (options) environment (private/open! opts)]
      (try
        (doseq [n [1 2 3]] (write! environment n))
        (env/close! environment)
        (let [file (wal-file opts)
              records (:records (wal/scan-segment (.getPath file)))
              offset (+ (:offset (nth records record-idx)) 16 10)]
          (flip! file offset)
          (let [before (bytes-of file)
                error (try (private/open! opts) (catch Throwable t t))]
            (is (= :txlog/recovery-history-invalid (:type (ex-data error))))
            (is (= :corrupt-record (:reason (ex-data error))))
            (is (Arrays/equals before (bytes-of file))
                "invalid required WAL is preserved, including its final frame")
            (is (not (some #{(.getCanonicalPath (io/file (:dir opts)))}
                           (env/active-environments))))))
        (finally (u/delete-files (:dir opts)))))))

(deftest interior-lsn-gap-is-not-tail-repair
  (let [opts (options) environment (private/open! opts)]
    (try
      (doseq [n [1 2 3]] (write! environment n))
      (env/close! environment)
      (let [file (wal-file opts) record (second (:records (wal/scan-segment (.getPath file))))
            payload (wal/decode-commit-row-payload (:body record))
            replacement (wal/encode-record
                         (wal/encode-commit-row-payload 99 0 (:ops payload)))]
        (with-open [f (RandomAccessFile. file "rw")]
          (.seek f (long (:offset record)))
          (.write f ^bytes replacement))
        (let [before (bytes-of file)
              error (try (private/open! opts) (catch Throwable t t))]
          (is (= :txlog/recovery-history-invalid (:type (ex-data error))))
          (is (= :gap (:reason (ex-data error))))
          (is (Arrays/equals before (bytes-of file)))))
      (finally (u/delete-files (:dir opts))))))

(deftest incomplete-final-frame-is-repaired-after-successful-replay
  (let [opts (options) environment (private/open! opts)]
    (try
      (write! environment 8)
      (env/close! environment)
      (let [file (wal-file opts) complete (.length file)]
        (with-open [f (RandomAccessFile. file "rw")]
          (.seek f complete)
          (.write f (byte-array [1 2 3])))
        (let [reopened (private/open! opts)]
          (try (is (= 8 (value reopened)))
               (is (= complete (.length file)))
               (is (= 1 (:records (:recovery (env/resources reopened)))))
               (finally (env/close! reopened)))))
      (finally (u/delete-files (:dir opts))))))

(deftest corrupt-current-falls-back-only-with-complete-previous-history
  (let [opts (options) environment (private/open! opts)]
    (try
      (write! environment 1)
      (snapshot! environment)
      (write! environment 2)
      (snapshot! environment)
      (write! environment 3)
      (env/close! environment)
      (let [current (io/file (recovery/root opts) "current/data.mdb")]
        (flip! current 0)
        (let [reopened (private/open! opts)]
          (try (is (= 3 (value reopened)))
               (is (= 1 (:floor-lsn (:recovery (env/resources reopened)))))
               (is (= 2 (:records (:recovery (env/resources reopened)))))
               (finally (env/close! reopened))))
        (let [file (wal-file opts)
              records (:records (wal/scan-segment (.getPath file)))]
          (flip! file (+ (:offset (second records)) 16 10))
          (let [error (try (private/open! opts) (catch Throwable t t))]
            (is (= :txlog/recovery-history-invalid (:type (ex-data error)))))))
      (finally (u/delete-files (:dir opts))))))

(deftest failed-snapshot-publication-and-installation-preserve-recovery-sources
  (let [opts (options) environment (private/open! opts)]
    (try
      (write! environment 5)
      (with-redefs [recovery/phase! (fn [event _]
                                    (when (= event :before-snapshot-publish)
                                      (throw (ex-info "snapshot publication fault" {}))))]
        (is (thrown? clojure.lang.ExceptionInfo (snapshot! environment))))
      (is (batch/serving? (env/collector environment)))
      (env/close! environment)
      (let [before (bytes-of (wal-file opts))]
        (with-redefs [recovery/phase! (fn [event _]
                                      (when (= event :before-install)
                                        (throw (ex-info "install fault" {}))))]
          (is (thrown? clojure.lang.ExceptionInfo (private/open! opts))))
        (is (Arrays/equals before (bytes-of (wal-file opts))))
        (let [reopened (private/open! opts)]
          (try (is (= 5 (value reopened)))
               (finally (env/close! reopened)))))
      (finally (u/delete-files (:dir opts))))))

(deftest hard-cap-rejects-before-io-and-two-rotations-unblock
  (let [opts (assoc (options) :wal-retained-max-bytes 512 :wal-segment-max-bytes 180)
        environment (private/open! opts) c (env/collector environment)]
    (try
      (let [last-good (loop [n 1]
                        (let [result (try (write! environment n) (catch Throwable t t))]
                          (if (instance? Throwable result)
                            (do (is (= :txlog/retention-backpressure (:error (ex-data result))))
                                (is (= :not-committed (:outcome (ex-data result))))
                                (dec n))
                            (do (is (< n 100)) (recur (inc n))))))]
        (is (= last-good (value environment)))
        (is (batch/serving? c))
        (is (<= (reduce + (map #(.length ^java.io.File (:file %))
                              (wal/segment-files (recovery/wal-dir opts)))) 512))
        (snapshot! environment)
        (snapshot! environment)
        (is (= (inc last-good) (write! environment (inc last-good))))
        (is (zero? (:requests (batch/usage c)))))
      (finally (env/close! environment) (u/delete-files (:dir opts))))))

(deftest native-only-keeps-native-reopen-and-rejects-mixed-handles
  (let [opts (assoc (options) :wal? false) environment (private/open! opts)]
    (try
      (is (nil? (:wal-state (env/resources environment))))
      (is (= 4 (write! environment 4)))
      (is (zero? (batch/published-lsn (env/collector environment))))
      (is (thrown? clojure.lang.ExceptionInfo (private/open! (assoc opts :wal? true))))
      (is (= 1 (env/handles environment)))
      (env/close! environment)
      (is (not (.exists (io/file (recovery/wal-dir opts)))))
      (let [reopened (private/open! opts)]
        (try (is (= 4 (value reopened)))
             (finally (env/close! reopened))))
      (finally (u/delete-files (:dir opts))))))

(deftest scheduler-default-on-uses-explicit-worker-context
  (let [opts (dissoc (assoc (options) :snapshot-max-lsn-delta 1
                           :snapshot-interval-ms 200 :snapshot-defer-on-contention? false)
                     :snapshot-scheduler?)
        inherited (InheritableThreadLocal.) seen (promise)]
    (.set inherited :submitter)
    (try
      (with-redefs [recovery/phase!
                    (fn [event context]
                      (when (and (= event :before-copy) (pos? (:floor-lsn context)))
                        (deliver seen (.get inherited))))]
        (let [environment (private/open! opts)]
          (try
            (write! environment 1)
            (is (nil? (deref seen 5000 ::timeout)))
            (loop [remaining 5000]
              (when (and (zero? (:floor-lsn @(:snapshot-state (env/resources environment))))
                         (pos? remaining))
                (Thread/sleep 1)
                (recur (dec remaining))))
            (is (= 1 (:floor-lsn @(:snapshot-state (env/resources environment)))))
            (is (nil? @(:snapshot-error (env/resources environment))))
            (finally (env/close! environment)))))
      (finally (.remove inherited) (u/delete-files (:dir opts))))))
