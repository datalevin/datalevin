(ns datalevin.txlog.replay-batch-test
  (:require [clojure.test :refer [deftest is]]
            [datalevin.txlog :as wal]
            [datalevin.txlog.segment :as segment]
            [datalevin.util :as u])
  (:import [java.io Closeable IOException]
           [java.nio.channels FileChannel]))

(defn- with-runtime [opts f]
  (let [dir (u/tmp-dir (str "wal-replay-batch-" (random-uuid)))
        {:keys [state]}
        (wal/init-runtime-state
         (merge {:dir dir :wal? true :wal-shared? false
                 :wal-durability-profile :strict :wal-sync-mode :fdatasync
                 :wal-segment-prealloc? false} opts) nil)]
    (try (f state)
         (finally
           (doseq [key [:segment-channel :sync-lock-channel]]
             (when-let [^Closeable channel (some-> (get state key) deref)]
               (.close channel)))
           (u/delete-files dir)))))

(defn- observe-channel [^FileChannel channel write! forces]
  (proxy [FileChannel] []
    (write [buffers offset length] (write! channel buffers offset length))
    (position
      ([] (.position channel))
      ([position] (.position channel (long position))))
    (size [] (.size channel))
    (force [metadata?] (swap! forces inc) (.force channel metadata?))
    (implCloseChannel [] (.close channel))))

(defn- records [n]
  (mapv (fn [^long lsn]
          {:lsn lsn :tx-time (+ 123000 lsn) :ha-term (if (< lsn 3) 2 3)
           :rows [[:put "data" lsn (str lsn) :long :string]]})
        (range 1 (inc (long n)))))

(deftest replay-group-uses-gathered-io-and-one-sync
  (doseq [mode [:fdatasync :fsync]
          n [4 1100]]
    (with-runtime
      {:wal-sync-mode mode}
      (fn [state]
        (let [writes (atom 0) forces (atom 0) syncs (atom 0)
              channel (observe-channel
                       @(:segment-channel state)
                       (fn [^FileChannel ch buffers offset length]
                         (swap! writes inc)
                         (.write ch ^"[Ljava.nio.ByteBuffer;" buffers
                                 (int offset) (int length))) forces)
              records (records n)]
          ;; fsync accesses the native FileChannel descriptor directly.
          (when (= mode :fdatasync)
            (vreset! (:segment-channel state) channel))
          (let [result (wal/append-replay-batch!
                        state records {:before-sync! (fn [_ _] (swap! syncs inc))})
                scanned (:records (segment/scan-segment
                                   (wal/segment-path (:dir state) @(:segment-id state))))
                payloads (mapv #(wal/decode-commit-row-payload (:body %)) scanned)]
            (is (= n (count scanned) (:lsn result)))
            (is (= 1 @syncs))
            (when (= mode :fdatasync)
              (is (zero? @forces))
              (when (= n 4) (is (= 1 @writes))))
            (is (= (mapv :lsn records) (mapv :lsn payloads)))
            (is (= (mapv :tx-time records) (mapv :ts payloads)))
            (is (= (mapv :ha-term records) (mapv :ha-term payloads)))
            (is (= (:offset (peek scanned)) (:offset result)))
            (is (= n (:last-durable-lsn
                       (wal/sync-manager-state (:sync-manager state)))))
            (is (= 0 @(:meta-last-applied-lsn state)))))))))

(deftest partial-gather-failure-leaves-recovery-in-charge
  (with-runtime
    {}
    (fn [state]
      (let [writes (atom 0)
            error (IOException. "injected gathered write failure")
            channel (observe-channel
                     @(:segment-channel state)
                     (fn [^FileChannel ch buffers _ _]
                       (if (= 1 (swap! writes inc))
                         ;; One whole record and the next header reach disk.
                         (.write ch ^"[Ljava.nio.ByteBuffer;" buffers 0 3)
                         (throw error))) (atom 0))]
        (vreset! (:segment-channel state) channel)
        (is (thrown? IOException
                     (wal/append-replay-batch!
                      state (records 4)
                      {:mark-fatal! #(vreset! (:fatal-error %1) %2)})))
        (is (identical? error @(:fatal-error state)))
        (is (= 1 @(:next-lsn state)))
        (is (= 0 @(:segment-offset state)))
        (is (= 0 (:last-durable-lsn
                   (wal/sync-manager-state (:sync-manager state)))))
        (let [scan (segment/scan-segment
                    (wal/segment-path (:dir state) @(:segment-id state)))]
          (is (= 1 (count (:records scan))))
          (is (:partial-tail? scan)))))))
