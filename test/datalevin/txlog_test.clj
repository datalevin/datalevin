(ns datalevin.txlog-test
  (:require
   [clojure.test :refer [deftest is testing]]
   [clojure.java.io :as io]
   [datalevin.bits :as b]
   [datalevin.constants :as c]
   [datalevin.interface :as i]
   [datalevin.kv :as kv]
   [datalevin.lmdb :as l]
   [datalevin.txlog :as sut]
   [datalevin.util :as u])
  (:import
   [java.io RandomAccessFile]
   [java.nio ByteBuffer]
   [java.nio.file Files StandardOpenOption]
   [java.nio.charset StandardCharsets]
   [java.util UUID]))

(defmacro with-temp-dir
  [[sym] & body]
  `(let [~sym (u/tmp-dir (str "txlog-test-" (UUID/randomUUID)))]
     (u/create-dirs ~sym)
     (try
       ~@body
       (finally
         (u/delete-files ~sym)))))

(defn- ->bytes
  [^String s]
  (.getBytes s StandardCharsets/UTF_8))

(deftest classify-record-kind-test
  (testing "vector checkpoint records classify via vec-index/vec-meta DBIs"
    (is (= :vector-checkpoint
           (sut/classify-record-kind
            [[:put c/vec-index-dbi ["d" 0] (byte-array 0) :data :bytes]
             [:put c/vec-meta-dbi "d" {:current-snapshot-lsn 10}
              :string :data]]))))
  (testing "mixed or non-vector ops classify as user"
    (is (= :user
           (sut/classify-record-kind
            [[:put c/vec-meta-dbi "d" {:vec-replay-floor-lsn 5}
              :string :data]
             [:put "d/vec-refs" :ref 1 :data :id]]))))
  (testing "missing/invalid ops classify as unknown"
    (is (= :unknown (sut/classify-record-kind nil)))
    (is (= :unknown (sut/classify-record-kind [])))
    (is (= :unknown (sut/classify-record-kind {:ops []})))))

(deftest record-codec-roundtrip-test
  (let [body (->bytes "hello txlog")
        data (sut/encode-record body {:compressed? true})
        rec  (sut/decode-record-bytes data)]
    (is (= (alength ^bytes body) (:body-len rec)))
    (is (true? (:compressed? rec)))
    (is (= (seq body) (seq ^bytes (:body rec))))))

(deftest append-record-at-roundtrip-test
  (with-temp-dir [dir]
    (let [path (sut/segment-path dir 1)
          body (->bytes "hello append")]
      (with-open [^java.nio.channels.FileChannel ch (sut/open-segment-channel path)]
        (let [{:keys [offset size checksum]}
              (sut/append-record-at! ch 0 body {:compressed? true})
              {:keys [records partial-tail? valid-end]}
              (sut/scan-segment path)
              rec (first records)]
          (is (= 0 offset))
          (is (= (+ sut/record-header-size (alength ^bytes body)) size))
          (is (= valid-end size))
          (is (false? partial-tail?))
          (is (= checksum (:checksum rec)))
          (is (true? (:compressed? rec)))
          (is (= (seq body) (seq ^bytes (:body rec)))))))))

(deftest commit-payload-parallel-row-encoding-equivalence-test
  (let [rows (vec (for [i (range 128)]
                    [:put "dbi" i (str "v" i)]))
        lsn  42
        tx-time 99
        mode-var #'sut/use-parallel-row-encoding?
        original-mode (var-get mode-var)]
    (try
      (alter-var-root mode-var (constantly (fn [_] false)))
      (let [sequential (sut/encode-commit-row-payload lsn tx-time rows)]
        (alter-var-root mode-var (constantly (fn [_] true)))
        (let [parallel (sut/encode-commit-row-payload lsn tx-time rows)]
          (is (= (seq sequential) (seq parallel)))))
      (finally
        (alter-var-root mode-var (constantly original-mode))))))

(deftest commit-payload-lmdb-row-reuse-test
  (let [rows [(l/kv-tx :put "dbi" :k :v :keyword :keyword)
              (l/kv-tx :del "dbi" :k2 nil :keyword)
              (l/kv-tx :put-list "dbi" :lk [1 2] :keyword :long)]
        {:keys [body lmdb-rows]}
        (#'sut/encode-commit-row-payload+lmdb-rows 7 11 rows)
        decoded (:ops (sut/decode-commit-row-payload body))
        ^datalevin.lmdb.KVTxData put-row  (nth lmdb-rows 0)
        ^datalevin.lmdb.KVTxData del-row  (nth lmdb-rows 1)
        ^datalevin.lmdb.KVTxData list-row (nth lmdb-rows 2)]
    (is (= [[:put "dbi" :k :v :keyword :keyword]
            [:del "dbi" :k2 :keyword]
            [:put-list "dbi" :lk [1 2] :keyword :long]]
           decoded))
    (is (= 3 (count lmdb-rows)))

    (is (= :raw (.-kt put-row)))
    (is (= :raw (.-vt put-row)))
    (is (= :k (b/read-buffer (ByteBuffer/wrap ^bytes (.-k put-row)) :keyword)))
    (is (= :v (b/read-buffer (ByteBuffer/wrap ^bytes (.-v put-row)) :keyword)))

    (is (= :raw (.-kt del-row)))
    (is (= :k2 (b/read-buffer (ByteBuffer/wrap ^bytes (.-k del-row)) :keyword)))

    (is (= :raw (.-kt list-row)))
    (is (= :long (.-vt list-row)))
    (is (= :lk (b/read-buffer (ByteBuffer/wrap ^bytes (.-k list-row)) :keyword)))
    (is (= [1 2] (.-v list-row)))))

(deftest txlog-apply-after-append-retries-resized-test
  (let [state {:fatal-error (volatile! nil)}
        calls (atom 0)]
    (with-redefs [i/transact-kv
                  (fn [& _]
                    (if (= 1 (swap! calls inc))
                      (throw (ex-info "DB resized" {:resized true}))
                      :transacted))]
      (is (= :transacted
             (#'kv/apply-lmdb-after-txlog-append!
              :fake-lmdb state [[:put "dbi" :k :v]])))
      (is (= 2 @calls))
      (is (nil? @(:fatal-error state))))))

(deftest txlog-apply-after-append-failure-marks-fatal-test
  (let [state {:fatal-error (volatile! nil)}]
    (with-redefs [i/transact-kv
                  (fn [& _]
                    (throw (ex-info "apply failed" {})))]
      (is (thrown? clojure.lang.ExceptionInfo
                   (#'kv/apply-lmdb-after-txlog-append!
                    :fake-lmdb state [[:put "dbi" :k :v]])))
      (is (instance? Exception @(:fatal-error state)))
      (is (= "apply failed"
             (.getMessage ^Exception @(:fatal-error state)))))))

(deftest retention-state-empty-floors-test
  (let [state (sut/retention-state
               {:segments []
                :total-bytes 0
                :retention-bytes 0
                :retention-ms 0
                :active-segment-id 0
                :newest-segment-id 0
                :floors {}
                :explicit-gc? false})]
    (is (= sut/no-floor-lsn (:required-retained-floor-lsn state)))
    (is (empty? (:floor-limiters state)))
    (is (= [] (:segments state)))))

(deftest segment-summaries-cache-reuses-unchanged-segments-test
  (with-temp-dir [dir]
    (let [seg1 (sut/segment-path dir 1)
          seg2 (sut/segment-path dir 2)
          seg3 (sut/segment-path dir 3)
          append-lsns!
          (fn [path lsns]
            (with-open [^java.nio.channels.FileChannel ch
                        (sut/open-segment-channel path)]
              (doseq [lsn lsns]
                (sut/append-record! ch
                                    (sut/encode-commit-row-payload
                                     lsn
                                     (* 1000 lsn)
                                     [[:put "dbi" lsn lsn]])))))
          lsn-calls (atom 0)
          record->lsn
          (fn [record]
            (swap! lsn-calls inc)
            (some-> record
                    :body
                    sut/decode-commit-row-payload
                    :lsn
                    long))
          cache-v (volatile! {})]
      (append-lsns! seg1 [1 2])
      (append-lsns! seg2 [3 4])
      (append-lsns! seg3 [5 6])
      (sut/segment-summaries
       dir
       {:record->lsn record->lsn
        :cache-v cache-v
        :cache-key :test-lsn})
      (is (= 6 @lsn-calls))
      (reset! lsn-calls 0)
      (append-lsns! seg3 [7])
      (let [summary (sut/segment-summaries
                     dir
                     {:record->lsn record->lsn
                      :cache-v cache-v
                      :cache-key :test-lsn})]
        (is (= 2 @lsn-calls))
        (is (= 7 (-> summary :segments peek :max-lsn)))))))

(deftest segment-summaries-cache-fast-path-skips-metadata-stat-test
  (with-temp-dir [dir]
    (let [seg1 (sut/segment-path dir 1)
          seg2 (sut/segment-path dir 2)
          append-lsns!
          (fn [path lsns]
            (with-open [^java.nio.channels.FileChannel ch
                        (sut/open-segment-channel path)]
              (doseq [lsn lsns]
                (sut/append-record! ch
                                    (sut/encode-commit-row-payload
                                     lsn
                                     (* 1000 lsn)
                                     [[:put "dbi" lsn lsn]])))))
          lsn-calls (atom 0)
          record->lsn
          (fn [record]
            (swap! lsn-calls inc)
            (some-> record
                    :body
                    sut/decode-commit-row-payload
                    :lsn
                    long))
          cache-v (volatile! {})]
      (append-lsns! seg1 [1 2])
      (append-lsns! seg2 [3 4])
      (let [active-offset (long (.length (io/file seg2)))]
        (sut/segment-summaries
         dir
         {:record->lsn record->lsn
          :cache-v cache-v
          :cache-key :test-lsn
          :active-segment-id 2
          :active-segment-offset active-offset})
        (is (= 4 @lsn-calls))
        (reset! lsn-calls 0)

        ;; Change only metadata on a closed segment. Cache fast-path should
        ;; still reuse this entry and avoid rescanning.
        (let [f (io/file seg1)
              touched? (.setLastModified f (+ 1000 (System/currentTimeMillis)))]
          (is touched?)
          (let [summary (sut/segment-summaries
                         dir
                         {:record->lsn record->lsn
                          :cache-v cache-v
                          :cache-key :test-lsn
                          :active-segment-id 2
                          :active-segment-offset active-offset})]
            (is (= 0 @lsn-calls))
            (is (= 2 (count (:segments summary))))
            (is (= 4 (-> summary :segments peek :max-lsn)))))))))

(deftest txlog-records-tail-scan-test
  (with-temp-dir [dir]
    (let [seg1 (sut/segment-path dir 1)
          seg2 (sut/segment-path dir 2)
          seg3 (sut/segment-path dir 3)
          append-lsns!
          (fn [path lsns]
            (with-open [^java.nio.channels.FileChannel ch
                        (sut/open-segment-channel path)]
              (doseq [lsn lsns]
                (sut/append-record! ch
                                    (sut/encode-commit-row-payload
                                     lsn
                                     (* 1000 lsn)
                                     [[:put "dbi" lsn lsn]])))))]
      (append-lsns! seg1 [1 2])
      (append-lsns! seg2 [3 4])
      (append-lsns! seg3 [5 6])
      (with-open [raf (RandomAccessFile. seg1 "rw")]
        (.seek raf 0)
        (.write raf (int 0x00)))
      (testing "tail scan from high LSN avoids scanning corrupted old segments"
        (is (= [5 6]
               (mapv :lsn (kv/txlog-records {:dir dir} 5))))
        (is (= [3 4 5 6]
               (mapv :lsn (kv/txlog-records {:dir dir} 4)))))
      (testing "full scan still checks historical segments"
        (is (thrown? clojure.lang.ExceptionInfo
                     (kv/txlog-records {:dir dir} 0)))))))

(deftest txlog-records-cache-reuses-unchanged-segments-test
  (with-temp-dir [dir]
    (let [seg1 (sut/segment-path dir 1)
          seg2 (sut/segment-path dir 2)
          state {:dir dir
                 :txlog-records-cache (volatile! {})}
          append-lsns!
          (fn [path lsns]
            (with-open [^java.nio.channels.FileChannel ch
                        (sut/open-segment-channel path)]
              (doseq [lsn lsns]
                (sut/append-record! ch
                                    (sut/encode-commit-row-payload
                                     lsn
                                     (* 1000 lsn)
                                     [[:put "dbi" lsn lsn]])))))]
      (append-lsns! seg1 [1 2])
      (append-lsns! seg2 [3 4])
      (is (= [1 2 3 4]
             (mapv :lsn (kv/txlog-records state 0))))
      (let [cache1 @(:txlog-records-cache state)
            seg1-entry-1 (get cache1 1)
            seg2-entry-1 (get cache1 2)]
        (is (= 2 (count cache1)))

        (is (= [1 2 3 4]
               (mapv :lsn (kv/txlog-records state 0))))
        (let [cache2 @(:txlog-records-cache state)]
          (is (identical? seg1-entry-1 (get cache2 1)))
          (is (identical? seg2-entry-1 (get cache2 2))))

        (append-lsns! seg2 [5])
        (is (= [1 2 3 4 5]
               (mapv :lsn (kv/txlog-records state 0))))
        (let [cache3 @(:txlog-records-cache state)]
          (is (identical? seg1-entry-1 (get cache3 1)))
          (is (not (identical? seg2-entry-1 (get cache3 2))))
          (is (= 5 (-> cache3 (get 2) :records peek :lsn))))))))

(deftest segment-order-and-parse-test
  (with-temp-dir [dir]
    (doseq [f ["segment-0000000000000010.wal"
               "segment-0000000000000002.wal"
               "segment-0000000000000001.wal"
               "segment-0000000000000003.wal.tmp"
               "notes.txt"]]
      (spit (str dir u/+separator+ f) "x"))
    (let [ids (mapv :id (sut/segment-files dir))]
      (is (= [1 2 10] ids)))))

(deftest scan-and-truncate-partial-tail-test
  (with-temp-dir [dir]
    (let [^String path (sut/segment-path dir 1)]
      (with-open [^java.nio.channels.FileChannel ch (sut/open-segment-channel path)]
        (sut/append-record! ch (->bytes "a"))
        (sut/append-record! ch (->bytes "bb")))
      (Files/write (.toPath (io/file path))
                   (byte-array [(byte 1) (byte 2) (byte 3) (byte 4)])
                   (into-array StandardOpenOption [StandardOpenOption/APPEND]))
      (let [{:keys [records partial-tail? valid-end size]} (sut/scan-segment path)]
        (is (= 2 (count records)))
        (is partial-tail?)
        (is (< valid-end size)))
      (let [{:keys [records truncated? dropped-bytes old-size new-size
                    valid-end partial-tail? preallocated-tail?]}
            (sut/truncate-partial-tail! path)]
        (is (= 2 (count records)))
        (is truncated?)
        (is (pos? dropped-bytes))
        (is (= new-size valid-end))
        (is (= (- old-size new-size) dropped-bytes))
        (is (false? partial-tail?))
        (is (false? preallocated-tail?)))
      (let [{:keys [records partial-tail?]} (sut/scan-segment path)]
        (is (= 2 (count records)))
        (is (false? partial-tail?))))))

(deftest interior-corruption-fails-test
  (with-temp-dir [dir]
    (let [^String path (sut/segment-path dir 2)]
      (with-open [^java.nio.channels.FileChannel ch (sut/open-segment-channel path)]
        (sut/append-record! ch (->bytes "good-1"))
        (sut/append-record! ch (->bytes "good-2")))
      (with-open [raf (RandomAccessFile. path "rw")]
        (.seek raf sut/record-header-size)
        (.write raf (int 0x7f)))
      (testing "interior corruption is treated as hard corruption"
        (try
          (sut/scan-segment path)
          (is false "Expected corruption exception")
          (catch clojure.lang.ExceptionInfo e
            (is (= :txlog/corrupt (:type (ex-data e))))))))))

(deftest preallocate-and-activate-segment-test
  (with-temp-dir [dir]
    (let [tmp-path   (str dir u/+separator+ "segment-0000000000000009.wal.tmp")
          final-path (sut/segment-path dir 9)]
      (sut/prepare-segment! tmp-path 4096)
      (is (= 4096 (.length (io/file tmp-path))))
      (sut/activate-prepared-segment! tmp-path final-path)
      (is (not (.exists (io/file tmp-path))))
      (is (= 4096 (.length (io/file final-path)))))))

(deftest prepared-segment-list-test
  (with-temp-dir [dir]
    (doseq [f ["segment-0000000000000003.wal.tmp"
               "segment-0000000000000001.wal.tmp"
               "segment-0000000000000010.wal.tmp"
               "segment-0000000000000004.wal"
               "notes.txt"]]
      (spit (str dir u/+separator+ f) "x"))
    (is (= [1 3 10] (mapv :id (sut/prepared-segment-files dir))))))

(deftest preallocated-tail-scan-and-truncate-test
  (with-temp-dir [dir]
    (let [path (sut/segment-path dir 11)]
      (sut/prepare-segment! path 4096)
      (with-open [^java.nio.channels.FileChannel ch (sut/open-segment-channel path)]
        (let [{:keys [size]} (sut/append-record-at! ch 0 (->bytes "v1"))]
          (sut/append-record-at! ch size (->bytes "v2"))))
      (is (thrown? clojure.lang.ExceptionInfo (sut/scan-segment path)))
      (let [{:keys [records valid-end partial-tail? preallocated-tail? size]}
            (sut/scan-segment path {:allow-preallocated-tail? true})]
        (is (= 2 (count records)))
        (is partial-tail?)
        (is preallocated-tail?)
        (is (< valid-end size)))
      (let [{:keys [records truncated? new-size valid-end partial-tail?
                    preallocated-tail?]}
            (sut/truncate-partial-tail! path {:allow-preallocated-tail? true})]
        (is (= 2 (count records)))
        (is truncated?)
        (is (= new-size (.length (io/file path))))
        (is (= new-size valid-end))
        (is (false? partial-tail?))
        (is (false? preallocated-tail?)))
      (let [{:keys [records partial-tail?]}
            (sut/scan-segment path)]
        (is (= 2 (count records)))
        (is (false? partial-tail?))))))

(deftest next-segment-helpers-test
  (with-temp-dir [dir]
    (let [fallback-path (sut/activate-next-segment! dir 21)]
      (is (= fallback-path (sut/segment-path dir 21)))
      (is (= 0 (.length (io/file fallback-path)))))
    (sut/prepare-next-segment! dir 22 2048)
    (let [tmp-path   (sut/prepared-segment-path dir 22)
          final-path (sut/activate-next-segment! dir 22)]
      (is (not (.exists (io/file tmp-path))))
      (is (= 2048 (.length (io/file final-path)))))))

(deftest init-runtime-state-prepares-next-segment-test
  (with-temp-dir [dir]
    (doseq [mode [:native :mmap]]
      (let [root (str dir u/+separator+ (name mode))
            txlog-dir (str root u/+separator+ "txlog")
            {:keys [state]}
            (sut/init-runtime-state
             {:dir root
              :txn-log? true
              :txn-log-segment-prealloc? true
              :txn-log-segment-prealloc-mode mode
              :txn-log-segment-prealloc-bytes 512}
             {})]
        (try
          (is (= 1 @(:segment-id state)))
          (let [tmp-path (sut/prepared-segment-path txlog-dir 2)]
            (is (.exists (io/file tmp-path)))
            (is (= 512 (.length (io/file tmp-path)))))
          (when (= :mmap mode)
            (is (some? @(:segment-mmap state))))
          (finally
            (.close ^java.nio.channels.FileChannel @(:segment-channel state))))))))

(deftest append-durable-rolls-into-preallocated-segment-test
  (with-temp-dir [dir]
    (let [path1 (sut/segment-path dir 1)]
      (with-open [^java.nio.channels.FileChannel _ (sut/open-segment-channel path1)])
      (sut/prepare-next-segment! dir 2 2048)
      (let [state {:dir dir
                   :segment-id (volatile! 1)
                   :segment-created-ms (volatile! 0)
                   :segment-channel (volatile! (sut/open-segment-channel path1))
                   :segment-offset (volatile! 0)
                   :append-lock (Object.)
                   :next-lsn (volatile! 1)
                   :durability-profile :relaxed
                   :segment-max-bytes 0
                   :segment-max-ms 600000
                   :segment-prealloc? true
                   :segment-prealloc-mode :native
                   :segment-prealloc-bytes 2048
                   :sync-mode :fdatasync
                   :commit-wait-ms 100
                   :segment-roll-count (volatile! 0)
                   :segment-roll-duration-ms (volatile! 0)
                   :segment-prealloc-success-count (volatile! 0)
                   :segment-prealloc-failure-count (volatile! 0)
                   :append-near-roll-durations (volatile! [])
                   :append-p99-near-roll-ms (volatile! nil)
                   :sync-manager (sut/new-sync-manager
                                  {:group-commit 1
                                   :group-commit-ms 0
                                   :sync-adaptive? true
                                   :last-sync-ms 0})}]
        (try
          (let [res (sut/append-durable! state [[:put :k :v]] {})]
            (is (= 2 @(:segment-id state)))
            (is (= 2 (:segment-id res)))
            (is (= 0 (:offset res)))
            (is (pos? @(:segment-offset state)))
            (is (= @(:segment-offset state)
                   (+ (long (:offset res)) (long (:size res)))))
            (is (= 0 (.length (io/file path1))))
            (let [path2 (sut/segment-path dir 2)
                  {:keys [records partial-tail? preallocated-tail?
                          valid-end size]}
                  (sut/scan-segment path2 {:allow-preallocated-tail? true})]
              (is (= 1 (count records)))
              (is partial-tail?)
              (is preallocated-tail?)
              (is (< valid-end size)))
            (let [tmp3 (sut/prepared-segment-path dir 3)]
              (is (.exists (io/file tmp3)))
              (is (= 2048 (.length (io/file tmp3)))))
            (is (= 1 @(:segment-roll-count state)))
            (is (<= 0 @(:segment-roll-duration-ms state)))
            (is (<= 1 @(:segment-prealloc-success-count state)))
            (is (= 0 @(:segment-prealloc-failure-count state))))
          (finally
            (.close ^java.nio.channels.FileChannel @(:segment-channel state))))))))

(deftest append-durable-mmap-preallocated-segment-test
  (with-temp-dir [dir]
    (let [path1 (sut/segment-path dir 1)]
      (sut/prepare-segment! path1 2048)
      (let [ch (sut/open-segment-channel path1)
            mmap (.map ^java.nio.channels.FileChannel ch
                       java.nio.channels.FileChannel$MapMode/READ_WRITE
                       0 2048)
            state {:dir dir
                   :segment-id (volatile! 1)
                   :segment-created-ms (volatile! (System/currentTimeMillis))
                   :segment-channel (volatile! ch)
                   :segment-mmap (volatile! mmap)
                   :segment-offset (volatile! 0)
                   :append-lock (Object.)
                   :next-lsn (volatile! 1)
                   :durability-profile :relaxed
                   :segment-max-bytes 4096
                   :segment-max-ms 600000
                   :segment-prealloc? true
                   :segment-prealloc-mode :mmap
                   :segment-prealloc-bytes 2048
                   :sync-mode :none
                   :commit-wait-ms 100
                   :segment-roll-count (volatile! 0)
                   :segment-roll-duration-ms (volatile! 0)
                   :segment-prealloc-success-count (volatile! 0)
                   :segment-prealloc-failure-count (volatile! 0)
                   :append-near-roll-durations (volatile! [])
                   :append-p99-near-roll-ms (volatile! nil)
                   :sync-manager (sut/new-sync-manager
                                  {:group-commit 1000
                                   :group-commit-ms 100000
                                   :sync-adaptive? false
                                   :last-sync-ms 0})}]
        (try
          (let [res (sut/append-durable! state [[:put :k :v]] {})]
            (is (= 0 (:offset res)))
            (is (pos? (:size res)))
            (is (= (+ (long (:offset res)) (long (:size res)))
                   @(:segment-offset state)))
            (let [{:keys [records partial-tail? preallocated-tail?]}
                  (sut/scan-segment path1 {:allow-preallocated-tail? true})]
              (is (= 1 (count records)))
              (is partial-tail?)
              (is preallocated-tail?)))
          (finally
            (.close ^java.nio.channels.FileChannel ch)))))))

(deftest append-durable-near-roll-latency-metric-test
  (with-temp-dir [dir]
    (let [path1 (sut/segment-path dir 1)]
      (with-open [^java.nio.channels.FileChannel _ (sut/open-segment-channel path1)])
      (let [state {:dir dir
                   :segment-id (volatile! 1)
                   :segment-created-ms (volatile! (System/currentTimeMillis))
                   :segment-channel (volatile! (sut/open-segment-channel path1))
                   :segment-offset (volatile! 9)
                   :append-lock (Object.)
                   :next-lsn (volatile! 1)
                   :durability-profile :relaxed
                   :segment-max-bytes 10
                   :segment-max-ms 600000
                   :segment-prealloc? false
                   :segment-prealloc-mode :none
                   :segment-prealloc-bytes 0
                   :sync-mode :fdatasync
                   :commit-wait-ms 100
                   :segment-roll-count (volatile! 0)
                   :segment-roll-duration-ms (volatile! 0)
                   :segment-prealloc-success-count (volatile! 0)
                   :segment-prealloc-failure-count (volatile! 0)
                   :append-near-roll-durations (volatile! [])
                   :append-near-roll-sorted-durations (volatile! [])
                   :append-p99-near-roll-ms (volatile! nil)
                   :sync-manager (sut/new-sync-manager
                                  {:group-commit 1
                                   :group-commit-ms 0
                                   :sync-adaptive? true
                                   :last-sync-ms 0})}]
        (try
          (sut/append-durable! state [[:put :k :v]] {})
          (is (= 0 @(:segment-roll-count state)))
          (is (= 1 (count @(:append-near-roll-durations state))))
          (is (= 1 (count @(:append-near-roll-sorted-durations state))))
          (is (number? @(:append-p99-near-roll-ms state)))
          (is (<= 0 (long @(:append-p99-near-roll-ms state))))
          (finally
            (.close ^java.nio.channels.FileChannel @(:segment-channel state))))))))

(deftest append-near-roll-p99-sliding-window-test
  (let [max-n (var-get #'sut/append-near-roll-sample-max)
        state {:append-near-roll-durations (volatile! [])
               :append-near-roll-sorted-durations (volatile! [])
               :append-p99-near-roll-ms (volatile! nil)}]
    (doseq [n (range (inc max-n))]
      (#'sut/record-append-near-roll-ms! state n))
    (let [samples @(:append-near-roll-durations state)
          sorted  @(:append-near-roll-sorted-durations state)
          p99     @(:append-p99-near-roll-ms state)
          idx     (min (dec (count sorted))
                       (int (Math/floor (* 0.99 (dec (count sorted))))))
          expected (long (nth sorted idx))]
      (is (= max-n (count samples)))
      (is (= samples sorted))
      (is (= expected p99)))))

(deftest threadlocal-buffers-shrink-after-large-record-test
  (let [^ThreadLocal commit-tl (var-get #'sut/tl-commit-body-buffer)
        ^ThreadLocal bits-tl   (var-get #'sut/tl-bits-buffer)
        big-cap (* 8 1024 1024)]
    (.set commit-tl (ByteBuffer/allocate big-cap))
    (.set bits-tl (ByteBuffer/allocate big-cap))
    (sut/encode-commit-row-payload
     1
     1
     [[:put "dbi" :k :v]])
    (is (< (.capacity ^ByteBuffer (.get commit-tl)) big-cap))
    (is (< (.capacity ^ByteBuffer (.get bits-tl)) big-cap))))

(deftest append-durable-trailing-sync-batch-test
  (with-temp-dir [dir]
    (let [path1 (sut/segment-path dir 1)]
      (with-open [^java.nio.channels.FileChannel _ (sut/open-segment-channel path1)])
      (let [state {:dir dir
                   :segment-id (volatile! 1)
                   :segment-created-ms (volatile! (System/currentTimeMillis))
                   :segment-channel (volatile! (sut/open-segment-channel path1))
                   :segment-offset (volatile! 0)
                   :append-lock (Object.)
                   :next-lsn (volatile! 1)
                   :durability-profile :relaxed
                   :segment-max-bytes 1024
                   :segment-max-ms 600000
                   :segment-prealloc? false
                   :segment-prealloc-mode :none
                   :segment-prealloc-bytes 0
                   :sync-mode :none
                   :commit-wait-ms 100
                   :segment-roll-count (volatile! 0)
                   :segment-roll-duration-ms (volatile! 0)
                   :segment-prealloc-success-count (volatile! 0)
                   :segment-prealloc-failure-count (volatile! 0)
                   :append-near-roll-durations (volatile! [])
                   :append-p99-near-roll-ms (volatile! nil)
                   :sync-manager (sut/new-sync-manager
                                  {:group-commit 3
                                   :group-commit-ms 100000
                                   :sync-adaptive? false
                                   :last-sync-ms
                                   (System/currentTimeMillis)})}]
        (try
          (sut/append-durable! state [[:put "dbi" :k1 :v1]] {})
          (let [s1 (sut/sync-manager-state (:sync-manager state))]
            (is (= 1 (:last-appended-lsn s1)))
            (is (= 0 (:last-durable-lsn s1)))
            (is (= 1 (:pending-count s1)))
            (is (= 0 (:batched-sync-count s1))))
          (sut/append-durable! state [[:put "dbi" :k2 :v2]] {})
          (let [s2 (sut/sync-manager-state (:sync-manager state))]
            (is (= 2 (:last-appended-lsn s2)))
            (is (= 0 (:last-durable-lsn s2)))
            (is (= 2 (:pending-count s2)))
            (is (= 0 (:batched-sync-count s2))))
          (sut/append-durable! state [[:put "dbi" :k3 :v3]] {})
          (let [s3 (sut/sync-manager-state (:sync-manager state))]
            (is (= 3 (:last-appended-lsn s3)))
            (is (= 3 (:last-durable-lsn s3)))
            (is (= 0 (:pending-count s3)))
            (is (= 1 (:batched-sync-count s3)))
            (is (= 1 (get-in s3 [:sync-count-by-reason :batch-count]))))
          (finally
            (.close ^java.nio.channels.FileChannel @(:segment-channel state))))))))

(deftest append-durable-relaxed-group-commit-one-durable-per-append-test
  (with-temp-dir [dir]
    (let [path1 (sut/segment-path dir 1)]
      (with-open [^java.nio.channels.FileChannel _ (sut/open-segment-channel path1)])
      (let [state {:dir dir
                   :segment-id (volatile! 1)
                   :segment-created-ms (volatile! (System/currentTimeMillis))
                   :segment-channel (volatile! (sut/open-segment-channel path1))
                   :segment-offset (volatile! 0)
                   :append-lock (Object.)
                   :next-lsn (volatile! 1)
                   :durability-profile :relaxed
                   :segment-max-bytes 1024
                   :segment-max-ms 600000
                   :segment-prealloc? false
                   :segment-prealloc-mode :none
                   :segment-prealloc-bytes 0
                   :sync-mode :fdatasync
                   :commit-wait-ms 100
                   :segment-roll-count (volatile! 0)
                   :segment-roll-duration-ms (volatile! 0)
                   :segment-prealloc-success-count (volatile! 0)
                   :segment-prealloc-failure-count (volatile! 0)
                   :append-near-roll-durations (volatile! [])
                   :append-p99-near-roll-ms (volatile! nil)
                   :sync-manager (sut/new-sync-manager
                                  {:group-commit 1
                                   :group-commit-ms 100000
                                   :sync-adaptive? false
                                   :last-sync-ms
                                   (System/currentTimeMillis)})}]
        (try
          (let [r1 (sut/append-durable! state [[:put "dbi" :k1 :v1]] {})
                s1 (sut/sync-manager-state (:sync-manager state))]
            (is (true? (:synced? r1)))
            (is (= 1 (:last-appended-lsn s1)))
            (is (= 1 (:last-durable-lsn s1)))
            (is (= 0 (:pending-count s1)))
            (is (= 1 (get-in s1 [:sync-count-by-reason :batch-count]))))
          (let [r2 (sut/append-durable! state [[:put "dbi" :k2 :v2]] {})
                s2 (sut/sync-manager-state (:sync-manager state))]
            (is (true? (:synced? r2)))
            (is (= 2 (:last-appended-lsn s2)))
            (is (= 2 (:last-durable-lsn s2)))
            (is (= 0 (:pending-count s2)))
            (is (= 2 (get-in s2 [:sync-count-by-reason :batch-count]))))
          (finally
            (.close ^java.nio.channels.FileChannel @(:segment-channel state))))))))

(deftest append-durable-strict-single-writer-durable-per-append-test
  (with-temp-dir [dir]
    (let [path1 (sut/segment-path dir 1)]
      (with-open [^java.nio.channels.FileChannel _ (sut/open-segment-channel path1)])
      (let [state {:dir dir
                   :segment-id (volatile! 1)
                   :segment-created-ms (volatile! (System/currentTimeMillis))
                   :segment-channel (volatile! (sut/open-segment-channel path1))
                   :segment-offset (volatile! 0)
                   :append-lock (Object.)
                   :next-lsn (volatile! 1)
                   :durability-profile :strict
                   :segment-max-bytes 1024
                   :segment-max-ms 600000
                   :segment-prealloc? false
                   :segment-prealloc-mode :none
                   :segment-prealloc-bytes 0
                   :sync-mode :none
                   :commit-wait-ms 1000
                   :segment-roll-count (volatile! 0)
                   :segment-roll-duration-ms (volatile! 0)
                   :segment-prealloc-success-count (volatile! 0)
                   :segment-prealloc-failure-count (volatile! 0)
                   :append-near-roll-durations (volatile! [])
                   :append-p99-near-roll-ms (volatile! nil)
                   :sync-manager (sut/new-sync-manager
                                  {:group-commit 1000
                                   :group-commit-ms 100000
                                   :sync-adaptive? false
                                   :last-sync-ms
                                   (System/currentTimeMillis)})}]
        (try
          (let [r1 (sut/append-durable! state [[:put "dbi" :k1 :v1]] {})
                s1 (sut/sync-manager-state (:sync-manager state))]
            (is (true? (:synced? r1)))
            (is (= 1 (:last-appended-lsn s1)))
            (is (= 1 (:last-durable-lsn s1)))
            (is (= 0 (:pending-count s1)))
            (is (= 1 (:forced-sync-count s1))))
          (let [r2 (sut/append-durable! state [[:put "dbi" :k2 :v2]] {})
                s2 (sut/sync-manager-state (:sync-manager state))]
            (is (true? (:synced? r2)))
            (is (= 2 (:last-appended-lsn s2)))
            (is (= 2 (:last-durable-lsn s2)))
            (is (= 0 (:pending-count s2)))
            (is (= 2 (:forced-sync-count s2))))
          (finally
            (.close ^java.nio.channels.FileChannel @(:segment-channel state))))))))

(deftest sync-manager-trailing-target-batches-multiple-enqueued-lsns-test
  (let [mgr (sut/new-sync-manager {:group-commit 1000
                                   :group-commit-ms 100000
                                   :sync-adaptive? false
                                   :last-sync-ms
                                   (System/currentTimeMillis)})
        now (System/currentTimeMillis)]
    (is (nil? (sut/request-sync-on-append! mgr 1 now)))
    (is (= {:target-lsn 1 :reason :forced}
           (sut/begin-sync! mgr)))
    (doseq [lsn (range 2 9)]
      (is (nil? (sut/request-sync-on-append! mgr lsn now))))
    ;; A second leader is blocked while the first fsync is in-flight.
    (is (nil? (sut/begin-sync! mgr)))
    (sut/complete-sync-success! mgr 1 now :forced)
    ;; Trailing sync should pick up the queue tail in one shot.
    (is (= {:target-lsn 8 :reason :forced}
           (sut/begin-sync! mgr)))
    (sut/complete-sync-success! mgr 8 now :forced)
    (let [s (sut/sync-manager-state mgr)]
      (is (= 8 (:last-durable-lsn s)))
      (is (= 0 (:pending-count s)))
      (is (= 2 (:forced-sync-count s))))))

(deftest sync-manager-begin-sync-for-lsn-trailing-only-test
  (let [mgr (sut/new-sync-manager {:group-commit 1000
                                   :group-commit-ms 100000
                                   :sync-adaptive? false
                                   :last-sync-ms
                                   (System/currentTimeMillis)})
        now (System/currentTimeMillis)]
    (is (nil? (sut/request-sync-on-append! mgr 1 now)))
    (is (nil? (sut/begin-sync! mgr 2)))
    (is (= {:target-lsn 1 :reason :forced}
           (sut/begin-sync! mgr 1)))
    (is (nil? (sut/request-sync-on-append! mgr 2 now)))
    (is (nil? (sut/begin-sync! mgr 2)))
    (sut/complete-sync-success! mgr 1 now :forced)
    (is (nil? (sut/begin-sync! mgr 1)))
    (is (= {:target-lsn 2 :reason :forced}
           (sut/begin-sync! mgr 2)))))

(deftest segment-roll-predicate-test
  (is (false? (sut/should-roll-segment? 10 1000 1010
                                        {:segment-max-bytes 100
                                         :segment-max-ms 1000})))
  (is (true? (sut/should-roll-segment? 100 1000 1010
                                       {:segment-max-bytes 100
                                        :segment-max-ms 1000})))
  (is (true? (sut/should-roll-segment? 10 1000 3001
                                       {:segment-max-bytes 1000
                                        :segment-max-ms 2000}))))

(deftest segment-summaries-test
  (with-temp-dir [dir]
    (let [path1 (sut/segment-path dir 1)
          path2 (sut/segment-path dir 2)
          decode-lsn (fn [{:keys [body]}]
                       (long (aget ^bytes body 0)))
          marker-offset
          (with-open [^java.nio.channels.FileChannel ch1 (sut/open-segment-channel path1)
                      ^java.nio.channels.FileChannel ch2 (sut/open-segment-channel path2)]
            (sut/append-record! ch1 (byte-array [(byte 1)]))
            (let [{:keys [offset]} (sut/append-record! ch1 (byte-array [(byte 2)]))]
              (sut/append-record! ch2 (byte-array [(byte 3)]))
              offset))
          {:keys [segments marker-record min-retained-lsn newest-segment-id]}
          (sut/segment-summaries dir
                                {:record->lsn decode-lsn
                                 :marker-segment-id 1
                                 :marker-offset marker-offset
                                 :min-retained-fallback 999})]
      (is (= 2 (count segments)))
      (is (= 1 min-retained-lsn))
      (is (= 2 newest-segment-id))
      (is (= 2 (:lsn marker-record)))
      (is (= 1 (:segment-id marker-record)))))
  (with-temp-dir [dir]
    (is (= 77
           (:min-retained-lsn
            (sut/segment-summaries dir
                                   {:min-retained-fallback 77}))))))

(deftest meta-slot-codec-test
  (let [m {:revision           7
           :last-committed-lsn 100
           :last-durable-lsn   99
           :last-applied-lsn   98
           :segment-id         3
           :segment-offset     4096
           :updated-ms         12345}
        encoded (sut/encode-meta-slot m)
        decoded (sut/decode-meta-slot-bytes encoded)]
    (is (= 7 (:revision decoded)))
    (is (= 100 (:last-committed-lsn decoded)))
    (is (= 99 (:last-durable-lsn decoded)))
    (is (= 98 (:last-applied-lsn decoded)))
    (is (= 3 (:segment-id decoded)))
    (is (= 4096 (:segment-offset decoded)))
    (is (= 12345 (:updated-ms decoded)))))

(deftest meta-file-read-write-and-fallback-test
  (with-temp-dir [dir]
    (let [path (sut/meta-path dir)]
      (sut/write-meta-file! path {:last-committed-lsn 10
                                  :last-durable-lsn 9
                                  :last-applied-lsn 9
                                  :segment-id 1
                                  :segment-offset 100
                                  :updated-ms 1000})
      (sut/write-meta-file! path {:last-committed-lsn 20
                                  :last-durable-lsn 20
                                  :last-applied-lsn 19
                                  :segment-id 2
                                  :segment-offset 200
                                  :updated-ms 2000})
      (let [{:keys [current]} (sut/read-meta-file path)]
        (is (= 1 (:revision current)))
        (is (= 20 (:last-committed-lsn current))))
      ;; Corrupt slot-b payload. Reader should fall back to slot-a.
      (with-open [raf (RandomAccessFile. path "rw")]
        (.seek raf (+ sut/meta-slot-size 8))
        (.write raf (int 0x7f)))
      (let [{:keys [current slot-a slot-b]} (sut/read-meta-file path)]
        (is (some? slot-a))
        (is (nil? slot-b))
        (is (= 0 (:revision current)))
        (is (= 10 (:last-committed-lsn current)))))))

(deftest maybe-publish-meta-best-effort-threshold-test
  (with-temp-dir [dir]
    (let [path (sut/meta-path dir)
          mgr  (sut/new-sync-manager {:last-durable-lsn 2
                                      :last-appended-lsn 2
                                      :group-commit 100
                                      :group-commit-ms 100
                                      :last-sync-ms (System/currentTimeMillis)})
          state {:meta-path path
                 :sync-mode :none
                 :sync-manager mgr
                 :meta-publish-lock (Object.)
                 :meta-flush-max-txs 2
                 :meta-flush-max-ms 100000
                 :meta-last-flush-ms (volatile! (System/currentTimeMillis))
                 :meta-pending-txs (volatile! 0)}
          append-1 {:lsn 1 :segment-id 1 :offset 10}
          append-2 {:lsn 2 :segment-id 1 :offset 20}]
      (sut/maybe-publish-meta-best-effort! state append-1)
      (is (false? (.exists (io/file path))))
      (is (= 1 @(:meta-pending-txs state)))
      (sut/maybe-publish-meta-best-effort! state append-2)
      (is (.exists (io/file path)))
      (is (= 0 @(:meta-pending-txs state)))
      (let [{:keys [current]} (sut/read-meta-file path)]
        (is (= 2 (:last-committed-lsn current)))
        (is (= 2 (:last-durable-lsn current)))))))

(deftest sync-manager-request-and-complete-test
  (let [mgr (sut/new-sync-manager {:group-commit 1
                                   :group-commit-ms 100
                                   :sync-adaptive? true
                                   :last-sync-ms 0})]
    (is (= {:request? true :reason :batch-count}
           (sut/request-sync-on-append! mgr 1 10)))
    (is (nil? (sut/request-sync-on-append! mgr 2 11)))
    (is (= {:target-lsn 2 :reason :batch-count}
           (sut/begin-sync! mgr)))
    (sut/complete-sync-success! mgr 2 12)
    (let [state0 (sut/sync-manager-state mgr)]
      (is (= 2 (:last-durable-lsn state0)))
      (is (= 1 (:batched-sync-count state0)))
      (is (= 1 (get-in state0 [:sync-count-by-reason :batch-count]))))
    (sut/record-commit-wait-ms! mgr 7 :batch-count)
    (let [state1 (sut/sync-manager-state mgr)]
      (is (= 7.0 (:avg-commit-wait-ms state1)))
      (is (= 7.0 (get-in state1 [:avg-commit-wait-ms-by-mode :batched]))))
    (is (:durable? (sut/await-durable-lsn! mgr 2 100 12)))))

(deftest sync-manager-request-sync-now-test
  (let [mgr (sut/new-sync-manager {:group-commit 1000
                                   :group-commit-ms 100000
                                   :sync-adaptive? false
                                   :last-sync-ms 0})]
    (is (nil? (sut/request-sync-on-append! mgr 1 1)))
    (is (= {:request? true :reason :forced}
           (sut/request-sync-now! mgr)))
    (is (nil? (sut/request-sync-now! mgr)))
    (is (= {:target-lsn 1 :reason :forced}
           (sut/begin-sync! mgr)))))

(deftest sync-manager-batch-trigger-test
  (let [mgr (sut/new-sync-manager {:group-commit 3
                                   :group-commit-ms 100
                                   :sync-adaptive? false
                                   :last-sync-ms 0})]
    (is (nil? (sut/request-sync-on-append! mgr 1 1)))
    (is (nil? (sut/request-sync-on-append! mgr 2 2)))
    (is (= {:request? true :reason :batch-count}
           (sut/request-sync-on-append! mgr 3 3)))
    (is (= {:target-lsn 3 :reason :batch-count}
           (sut/begin-sync! mgr)))
    (sut/complete-sync-success! mgr 3 4)
    (let [state (sut/sync-manager-state mgr)]
      (is (= 1 (:batched-sync-count state)))
      (is (= 1 (get-in state [:sync-count-by-reason :batch-count])))))
  (let [mgr (sut/new-sync-manager {:group-commit 100
                                   :group-commit-ms 5
                                   :sync-adaptive? false
                                   :last-sync-ms 0})]
    (is (nil? (sut/request-sync-on-append! mgr 1 1)))
    (is (= {:request? true :reason :batch-time}
           (sut/request-sync-if-needed! mgr 10)))
    (is (= {:target-lsn 1 :reason :batch-time}
           (sut/begin-sync! mgr)))
    (sut/complete-sync-success! mgr 1 11)
    (sut/record-commit-wait-ms! mgr 12 :batch-time)
    (let [state (sut/sync-manager-state mgr)]
      (is (= 1 (:batched-sync-count state)))
      (is (= 12.0 (:avg-commit-wait-ms state)))
      (is (= 12.0 (get-in state [:avg-commit-wait-ms-by-mode :batched]))))))

(deftest sync-manager-n-or-t-only-test
  (let [mgr (sut/new-sync-manager {:group-commit 3
                                   :group-commit-ms 10
                                   :sync-adaptive? true
                                   :last-sync-ms 0})]
    (is (nil? (sut/request-sync-on-append! mgr 1 1)))
    (is (nil? (sut/request-sync-on-append! mgr 2 5)))
    ;; No adaptive special-case behavior: third append triggers by N only.
    (is (= {:request? true :reason :batch-count}
           (sut/request-sync-on-append! mgr 3 9)))
    (is (= {:target-lsn 3 :reason :batch-count}
           (sut/begin-sync! mgr)))
    (sut/complete-sync-success! mgr 3 10)
    (let [state (sut/sync-manager-state mgr)]
      (is (= 0 (:pending-count state)))
      (is (= 1 (:batched-sync-count state)))
      (is (= 1 (get-in state [:sync-count-by-reason :batch-count])))))
  (let [mgr (sut/new-sync-manager {:group-commit 100
                                   :group-commit-ms 10
                                   :sync-adaptive? true
                                   :last-sync-ms 0})]
    (is (nil? (sut/request-sync-on-append! mgr 1 1)))
    ;; Trigger by T only.
    (is (= {:request? true :reason :batch-time}
           (sut/request-sync-on-append! mgr 2 15)))
    (is (= {:target-lsn 2 :reason :batch-time}
           (sut/begin-sync! mgr)))
    (sut/complete-sync-success! mgr 2 16)
    (let [state (sut/sync-manager-state mgr)]
      (is (= 0 (:pending-count state)))
      (is (= 1 (:batched-sync-count state)))
      (is (= 1 (get-in state [:sync-count-by-reason :batch-time])))))
  (let [mgr (sut/new-sync-manager {:group-commit 100
                                   :group-commit-ms 10
                                   :sync-adaptive? false
                                   :last-sync-ms 0})]
    (is (nil? (sut/request-sync-on-append! mgr 1 1)))
    ;; adaptive knobs do not alter N/T behavior.
    (is (= {:request? true :reason :batch-time}
           (sut/request-sync-on-append! mgr 2 15)))
    (is (= {:target-lsn 2 :reason :batch-time}
           (sut/begin-sync! mgr)))
    (sut/complete-sync-success! mgr 2 16)
    (let [state (sut/sync-manager-state mgr)]
      (is (= 0 (:pending-count state)))
      (is (= 1 (:batched-sync-count state)))
      (is (= 1 (get-in state [:sync-count-by-reason :batch-time]))))))

(deftest sync-manager-trailing-request-survives-inflight-sync-test
  (let [mgr (sut/new-sync-manager {:group-commit 1000
                                   :group-commit-ms 1000
                                   :sync-adaptive? false
                                   :last-sync-ms 0})]
    (is (nil? (sut/request-sync-on-append! mgr 1 1)))
    (is (= {:target-lsn 1 :reason :forced}
           (sut/begin-sync! mgr)))
    (is (nil? (sut/request-sync-on-append! mgr 2 2)))
    (sut/complete-sync-success! mgr 1 3 :forced)
    (let [s (sut/sync-manager-state mgr)]
      (is (= 1 (:last-durable-lsn s)))
      (is (= 2 (:last-appended-lsn s))))
    (is (= {:target-lsn 2 :reason :forced}
           (sut/begin-sync! mgr)))))

(deftest sync-manager-begin-sync-clears-stale-request-test
  (let [mgr (sut/new-sync-manager {:last-durable-lsn 2
                                   :last-appended-lsn 2
                                   :group-commit 1000
                                   :group-commit-ms 1000
                                   :sync-adaptive? false
                                   :last-sync-ms 0})]
    (vreset! (:sync-requested? mgr) true)
    (vreset! (:sync-request-reason mgr) :batch-time)
    (is (nil? (sut/begin-sync! mgr)))
    (let [s (sut/sync-manager-state mgr)]
      (is (false? (:sync-requested? s)))
      (is (nil? (:sync-request-reason s))))))

(deftest sync-manager-timeout-marks-unhealthy-test
  (let [mgr (sut/new-sync-manager {:group-commit 100
                                   :group-commit-ms 100
                                   :sync-adaptive? false
                                   :last-sync-ms 0})]
    (sut/request-sync-on-append! mgr 1 1)
    (is (thrown-with-msg?
          clojure.lang.ExceptionInfo #"Timed out waiting for durable LSN"
          (sut/await-durable-lsn! mgr 1 0 1)))
    (is (false? (:healthy? (sut/sync-manager-state mgr))))
    (is (thrown-with-msg?
          clojure.lang.ExceptionInfo #"unhealthy"
          (sut/request-sync-on-append! mgr 2 2)))
    (is (true? (:healthy? (sut/reset-sync-health! mgr))))))
