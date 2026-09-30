(ns datalevin.txlog.write-pipeline-test
  (:require [clojure.test :refer [deftest is]]
            [datalevin.tx-group :as group]
            [datalevin.txlog :as wal]
            [datalevin.txlog.append :as append]
            [datalevin.txlog.codec :as codec]
            [datalevin.txlog.segment :as segment]
            [datalevin.tx-state.lifetime :as lifetime]
            [datalevin.util :as u])
  (:import [datalevin.lmdb DatomKVTxData]
           [java.io Closeable IOException]
           [java.util.concurrent CountDownLatch]
           [java.nio ByteBuffer]
           [java.nio.channels FileChannel]))

(defn- with-runtime [opts f]
  (let [dir (u/tmp-dir (str "wal-write-pipeline-" (random-uuid)))
        {:keys [state]}
        (wal/init-runtime-state
         (merge {:dir dir :wal? true :wal-shared? false
                 :wal-durability-profile :strict :wal-sync-mode :fsync
                 :wal-segment-prealloc? false :wal-commit-wait-ms 10000} opts)
         nil)]
    (try (f state)
         (finally
           (doseq [key [:segment-channel :sync-lock-channel]]
             (when-let [^Closeable channel (some-> (get state key) deref)]
               (.close channel)))
           (u/delete-files dir)))))

(defn- rows [n] [[:put "data" n (str n) :long :string]])
(defn- status [state] (wal/sync-manager-state (:sync-manager state)))
(defn- join [job] (deref job 10000 ::timeout))
(defn- caught [f] (try (f) (catch Throwable e e)))

(deftest grouped-payload-preserves-compact-and-ordinary-row-encoding
  (doseq [inputs [[(rows 1) [(DatomKVTxData. 9 (byte-array [1 2 3]) true false)] (rows 2)]
                  [[(DatomKVTxData. 9 (byte-array [1 2 3]) false false)] (rows 1)]]]
    (let [bodies (mapv #(wal/prepare-append-body % {:ha-term 7}) inputs)
          grouped (codec/combine-commit-row-payloads bodies)
          expected (wal/encode-commit-row-payload 0 0 (mapcat identity inputs) {:ha-term 7})]
      (is (java.util.Arrays/equals ^bytes expected ^bytes grouped))
      (is (= (count (mapcat identity inputs))
             (:op-count (codec/decode-commit-row-payload-header grouped))))
      (is (= 2 (aget ^bytes grouped 4))))))

(deftest mixed-terms-reject-the-group-before-append
  (with-runtime
    {}
    (fn [state]
      (let [bodies [(wal/prepare-append-body (rows 1) {:ha-term 7})
                    (wal/prepare-append-body (rows 2) {:ha-term 8})]]
        (is (= :txlog/mixed-terms
               (:type (ex-data (caught #(wal/append-prepared-batch-pending! state 1 bodies {}))))))
        (is (= 1 @(:next-lsn state)))
        (is (nil? @(:fatal-error state)))
        (is (empty? (:records (segment/scan-segment (wal/segment-path (:dir state) 1)))))
        (is (:synced? (wal/complete-append!
                       state (wal/append-batch-pending! state 1 [(rows 3) (rows 4)] {}) {})))))))

(deftest prepared-bodies-are-owned-and-stamped-only-at-insertion
  (with-runtime
    {}
    (fn [state]
      (let [inputs [(rows 1) (rows 2)]
            bodies (mapv #(wal/prepare-append-body % {:ha-term 7}) inputs)]
        ;; Reuse the encoder's scratch on this thread before another thread
        ;; stamps and appends the owned bodies.
        (wal/prepare-append-body
         [[:put "other" 9 (apply str (repeat 70000 "x")) :long :string]] {})
        (is (= [0 0] (mapv #(-> % wal/decode-commit-row-payload :lsn) bodies)))
        (is (= [0 0] (mapv #(-> % wal/decode-commit-row-payload :ts) bodies)))
        (is (= :txlog/stale-preparation
               (:type (ex-data (caught #(wal/append-prepared-batch-pending!
                                        state 2 bodies {}))))))
        (is (= [0 0] (mapv #(-> % wal/decode-commit-row-payload :lsn) bodies))
            "Rejected insertion leaves the prepared payload untouched")
        (let [start (System/currentTimeMillis)
              batch (join (future (wal/append-prepared-batch-pending! state 1 bodies {})))]
          (is (= 1 (append/last-lsn batch)))
          (is (:synced? (wal/complete-append! state batch {})))
          (let [records (:records (segment/scan-segment (wal/segment-path (:dir state) 1)))
                decoded (mapv #(wal/decode-commit-row-payload (:body %)) records)]
            (is (= [(vec (mapcat identity inputs))] (mapv :ops decoded)))
            (is (= [1] (mapv :lsn decoded)))
            (is (= [7] (mapv :ha-term decoded)))
            (is (every? #(<= start (:ts %) (System/currentTimeMillis)) decoded))
            (doseq [[record value] (map vector records decoded)]
              (is (java.util.Arrays/equals
                   ^bytes (:body record)
                   ^bytes (wal/encode-commit-row-payload
                           (:lsn value) (:ts value) (mapcat identity inputs) {:ha-term 7}))))))))))

(deftest group-completion-checks-the-shared-lsn-and-deadline
  (with-runtime
    {}
    (fn [state]
      (let [batch (wal/append-batch-pending! state 1 [(rows 1) (rows 2)] {})
            manager (:sync-manager state)
            deadline (append/deadline-ns batch)
            flushes (atom 0)
            hooks {:before-sync! (fn [_ _] (swap! flushes inc))}]
        ;; Both logical writes share this record's durability boundary.
        (.force ^FileChannel (append/channel batch) true)
        (wal/complete-sync-success! manager 1 (System/currentTimeMillis))
        (with-redefs [lifetime/nano-time (fn ^long [] (inc (long deadline)))]
          (is (true? (wal/complete-prefix! state batch 1 deadline hooks)))
          (is (wal/durable-append? state batch 1))
          (is (false? (wal/durable-append? state batch 2)))
          (let [error (caught #(wal/complete-prefix! state batch 2 deadline hooks))]
            (is (= :txlog/foreign-lsn (:type (ex-data error))))
            (is (= 2 (:lsn (ex-data error)))))
          (is (false? (:sync-in-progress? (status state)))))
        (is (zero? @flushes))
        (is (= :txlog/foreign-lsn
               (:type (ex-data (caught #(wal/complete-prefix! state batch 3 deadline hooks))))))
        (is (= :txlog/foreign-receipt
               (:type (ex-data (caught #(wal/complete-prefix!
                                        (assoc state :sync-manager {}) batch 1 deadline hooks))))))
        (is (:synced? (wal/complete-append! state batch hooks)))
        (is (= 0 @flushes))
        (is (= 1 (:last-durable-lsn (status state))))))))

(deftest durability-deadline-is-fixed-at-append-registration
  (with-runtime
    {:wal-commit-wait-ms 100}
    (fn [state]
      (let [clock (volatile! 0)]
        (with-redefs [lifetime/nano-time (fn ^long [] (long @clock))]
          (let [receipt (wal/append-pending! state (rows 1) {})]
            (vreset! clock (append/deadline-ns receipt))
            (is (= :txlog/commit-timeout
                   (:type (ex-data (caught #(wal/complete-append! state receipt {}))))))
            (is (false? (:sync-in-progress? (status state)))
                "Expired receipt must not strand sync ownership")
            (wal/force-sync! state {})
            (is (:synced? (wal/complete-append! state receipt {}))
                "Already established durability is still a known outcome")))))))

(deftest separate-receipts-share-one-flush
  (with-runtime
    {}
    (fn [state]
      (let [targets (atom [])
            receipts (mapv #(wal/append-pending! state (rows %) {}) (range 1 4))
            hooks {:before-sync!
                   (fn [_ round]
                     (is (not (Thread/holdsLock (:append-lock state))))
                     (swap! targets conj (:target-lsn round)))}]
        (is (= [1 2 3] (mapv append/last-lsn receipts)))
        (is (= {:last-appended-lsn 3 :last-durable-lsn 0
                :sync-in-progress? false}
               (select-keys (status state)
                            [:last-appended-lsn :last-durable-lsn :sync-in-progress?])))
        (doseq [receipt [(second receipts) (first receipts) (last receipts)]]
          (is (:synced? (wal/complete-append! state receipt hooks))))
        (is (= [3] @targets))
        (is (= 3 (:last-durable-lsn (status state))))
        (is (= 0 @(:meta-last-applied-lsn state)))
        (let [records (:records (segment/scan-segment
                                 (wal/segment-path (:dir state) 1)))]
          (is (= [1 2 3]
                 (mapv #(-> % :body wal/decode-commit-row-payload :lsn)
                       records))))))))

(deftest force-flushes-the-appended-prefix-without-waiting-for-preparation
  (with-runtime
    {}
    (fn [state]
      (let [a (wal/append-pending! state (rows 1) {})
            entered (promise) release (CountDownLatch. 1)
            targets (atom [])]
        (vreset! (:application-hooks state)
                 {:collect-before-sync!
                  (fn [_]
                    (deliver entered true)
                    (.await release))
                  :before-sync! (fn [_ round] (swap! targets conj (:target-lsn round)))})
        (let [job (future (caught #(wal/complete-append! state a {})))]
          (try
            (let [result (deref job 250 ::blocked)]
              (is (not= ::blocked result)
                  "An active collector body cannot hold an earlier prefix's force")
              (is (:synced? result))
              (is (false? (deref entered 0 false)))
              (is (= [1] @targets))
              (is (= 1 (:last-durable-lsn (status state))))
              (is (false? (:sync-in-progress? (status state))))
              (is (:healthy? (status state))))
            (finally (.countDown release) (join job))))))))

(deftest force-rechecks-durability-before-claiming-a-newer-round
  (with-runtime
    {}
    (fn [state]
      (let [a (wal/append-pending! state (rows 1) {})
            sampled (promise) release (promise)
            force-job (future
                        (caught #(wal/force-sync!
                                  state {:before-force-claim!
                                         (fn [_ target]
                                           (deliver sampled target)
                                           (assert (deref release 10000 false)))})))]
        (try
          (is (= 1 (deref sampled 10000 ::timeout)))
          (is (:synced? (wal/complete-append! state a {})))
          (let [b (wal/append-pending! state (rows 2) {})]
            (deliver release true)
            (is (:synced? (join force-job)))
            (is (false? (:sync-in-progress? (status state))))
            (is (:synced? (wal/complete-append! state b {})))
            (is (= 2 (:last-durable-lsn (status state)))))
          (finally (deliver release true) (join force-job)))))))

(deftest an-already-claimed-round-is-settled-even-for-a-durable-receipt
  (with-runtime
    {}
    (fn [state]
      (let [a (wal/append-pending! state (rows 1) {})]
        (wal/complete-append! state a {})
        (wal/append-pending! state (rows 2) {})
        (let [round (wal/begin-sync! (:sync-manager state))]
          (is (= 2 (:target-lsn round)))
          (#'wal/wait-strict-durable! state @(:segment-channel state)
                                      (:sync-manager state) 1 1000 {} round)
          (is (false? (:sync-in-progress? (status state))))
          (is (= 2 (:last-durable-lsn (status state)))))))))

(deftest collected-preparation-shares-one-record-and-weights-sync-counts
  (doseq [profile [:strict :extra :relaxed]]
    (with-runtime
      {:wal-durability-profile profile :wal-group-commit 10 :wal-group-commit-ms 0}
      (fn [state]
        (let [registered (atom nil)
              batch (wal/append-batch-pending!
                        state 1 [(rows 1) (rows 2) (rows 3)]
                        {:register-appends!
                         (fn [_ batch]
                           (is (= 0 (:last-appended-lsn (status state))))
                           (reset! registered batch))})]
          (is (identical? batch @registered))
          (is (= [1 1] [(append/first-lsn batch) (append/last-lsn batch)]))
          (is (= 3 (:unsynced-count (status state))))
          (doseq [lsn [1 1 1]]
            (is (= (not= profile :relaxed)
                   (wal/complete-prefix! state batch lsn (append/deadline-ns batch) {}))))
          (is (= (if (= profile :relaxed) 0 1) (:last-durable-lsn (status state))))
          (let [scan (segment/scan-segment (wal/segment-path (:dir state) 1))
                records (mapv #(wal/decode-commit-row-payload (:body %)) (:records scan))]
            (is (= [1] (mapv :lsn records)))
            (is (= [(vec (mapcat rows [1 2 3]))] (mapv :ops records)))))))))

(deftest relaxed-group-threshold-counts-logical-requests-once
  (with-runtime
    {:wal-durability-profile :relaxed :wal-group-commit 5 :wal-group-commit-ms 0}
    (fn [state]
      (let [a (wal/append-batch-pending! state 1 (mapv rows [1 2 3]) {})]
        (is (false? (:synced? (wal/complete-append! state a {}))))
        (is (= 3 (:unsynced-count (status state))))
        (let [b (wal/append-batch-pending! state 2 (mapv rows [4 5]) {})]
          (is (= 5 (:unsynced-count (status state))))
          (is (:synced? (wal/complete-append! state b {})))
          (is (= 2 (:last-durable-lsn (status state))))
          (is (zero? (:unsynced-count (status state)))))
        (let [c (wal/append-batch-pending! state 3 (mapv rows [6 7 8 9]) {})]
          (is (false? (:synced? (wal/complete-append! state c {}))))
          (is (= 4 (:unsynced-count (status state))))
          (is (= 2 (:last-durable-lsn (status state)))))))))

(deftest partial-prepared-batch-fences-the-original-tail
  (doseq [large? [false true]]
    (with-runtime
      {}
      (fn [state]
        (let [^FileChannel original @(:segment-channel state)
              batch (if large?
                      (mapv (fn [n] [[:put "data" n (apply str (repeat 70000 "x")) :long :string]])
                            (range 1 4))
                      [(rows 1) (rows 2) (rows 3)])
              first-size (+ 14 (alength ^bytes (wal/encode-commit-row-payload 1 0 (first batch))))
              writes (atom 0)
              error (IOException. "injected prepared batch failure")
              channel (proxy [FileChannel] []
                        (write
                          ([buffer position]
                           (if (= 1 (swap! writes inc))
                             (let [^ByteBuffer buffer buffer
                                   limit (.limit buffer)]
                               (.limit buffer (int (+ first-size 14)))
                               (try (.write original buffer (long position))
                                    (finally (.limit buffer limit))))
                             (throw error)))
                          ([buffers _ _]
                           (if (= 1 (swap! writes inc))
                             (.write original ^"[Ljava.nio.ByteBuffer;" buffers 0 1)
                             (throw error))))
                        (position
                          ([] (.position original))
                          ([n] (.position original (long n))))
                        (implCloseChannel [] (.close original)))]
          (vreset! (:segment-channel state) channel)
          (is (identical? error
                          (caught #(wal/append-batch-pending! state 1 batch {}))))
          (is (identical? error @(:fatal-error state)))
          (is (= 1 @(:next-lsn state)))
          (is (zero? (:last-appended-lsn (status state))))
          (is (= :txlog/fatal
                 (:type (ex-data (caught #(wal/append-batch-pending!
                                           state 1 [(rows 4)] {}))))))
          (let [scan (segment/scan-segment (wal/segment-path (:dir state) 1))]
            (is (empty? (:records scan))
                "A partial group cannot recover a subset of its transactions")
            (is (:partial-tail? scan))))))))

(deftest positioned-and-gathered-batches-retry-short-writes
  (doseq [large? [false true]]
    (with-runtime
      {}
      (fn [state]
        (let [^FileChannel original @(:segment-channel state)
              channel (proxy [FileChannel] []
                        (write
                          ([buffer position]
                           (let [^ByteBuffer buffer buffer
                                 limit (.limit buffer)]
                             (.limit buffer (int (min limit (+ 11 (.position buffer)))))
                             (try (.write original buffer (long position))
                                  (finally (.limit buffer limit)))))
                          ([buffers offset _]
                           (let [^"[Ljava.nio.ByteBuffer;" buffers buffers
                                 start (loop [n (int offset)]
                                         (if (.hasRemaining ^ByteBuffer (aget buffers n)) n
                                             (recur (inc n))))]
                             (.write original buffers start 1))))
                        (position
                          ([] (.position original))
                          ([n] (.position original (long n))))
                        (implCloseChannel [] (.close original)))
              bodies (mapv (fn [n] (ByteBuffer/wrap (byte-array (if large? 40000 30) (byte n))))
                           (range 1 4))
              prefix (segment/write-record-at! original 0 (byte-array 1))
              offset (:size prefix)
              results (segment/write-records-at! channel offset bodies)
              inputs [[(DatomKVTxData. 9 (byte-array [1 2 3]) true false)]
                      [[:put "data" 5 (apply str (repeat (if large? 70000 20) "x"))
                        :long :string]]
                      [[:del "data" 6 :long]]]
              prepared (mapv #(wal/prepare-append-body % {:ha-term 7}) inputs)
              group (codec/prepare-commit-row-group prepared)
              next-offset (+ (:offset (last results)) (:size (last results)))
              grouped (segment/write-prepared-record-at! channel next-offset group 8 12345)
              expected (wal/encode-commit-row-payload 8 12345 (mapcat identity inputs)
                                                       {:ha-term 7})
              records (:records (segment/scan-segment (wal/segment-path (:dir state) 1)))]
          (is (= 5 (count records)))
          (is (= (mapv :offset results) (mapv :offset (take 3 (rest records)))))
          (is (= [1 2 3] (mapv #(aget ^bytes (:body %) 0) (take 3 (rest records)))))
          (is (every? #(zero? (.remaining ^ByteBuffer %)) bodies))
          (is (= next-offset (:offset grouped) (:offset (last records))))
          (is (= (:checksum grouped)
                 (codec/current-record-checksum (alength ^bytes expected) false expected)))
          (is (java.util.Arrays/equals ^bytes expected ^bytes (:body (last records))))
          (is (= [0 0 0] (mapv #(-> % codec/decode-commit-row-payload-header :lsn) prepared)))
          (is (= [0 0 0] (mapv #(-> % codec/decode-commit-row-payload-header :ts) prepared))))))))

(deftest stopped-flush-allows-more-appends-without-overclaiming-durability
  (with-runtime
    {}
    (fn [state]
      (let [entered (promise) release (promise)
            targets (atom [])
            hooks {:before-sync!
                   (fn [_ round]
                     (is (not (Thread/holdsLock (:append-lock state))))
                     (swap! targets conj (:target-lsn round))
                     (when (= 1 (:target-lsn round))
                       (deliver entered true)
                       (assert (deref release 10000 false))))}
            first-receipt (wal/append-pending! state (rows 1) {})
            first-job (future (wal/complete-append! state first-receipt hooks))
            jobs (atom [first-job])]
        (try
          (is (deref entered 10000 false))
          (let [next-receipts (future
                                (mapv #(wal/append-pending! state (rows %) {}) [2 3]))
                receipts (join next-receipts)]
            (is (= [2 3] (mapv append/last-lsn receipts)))
            (is (= [3 0] ((juxt :last-appended-lsn :last-durable-lsn) (status state))))
            (doseq [receipt receipts]
              (swap! jobs conj (future (wal/complete-append! state receipt hooks))))
            (is (not-any? realized? @jobs))
            (deliver release true)
            (is (= [1 2 3] (mapv #(-> % join :lsn) @jobs)))
            (is (= [1 3] @targets)))
          (finally
            (deliver release true)
            (doseq [job @jobs] (join job))))))))

(deftest shared-collector-hands-off-before-wal-completion
  (with-runtime
    {}
    (fn [state]
      (let [g (group/create 8)
            entered (promise) release (promise) second-appended (promise)
            applied (atom [])
            runner
            (fn [execute]
              ;; Only reversible preparation belongs in execute. Appending is
              ;; outside its body-error retry boundary.
              (let [prepared (vec (execute nil))
                    receipts (mapv #(wal/append-pending! state (rows %) {}) prepared)]
                (when (= [2] prepared) (deliver second-appended true))
                (group/defer-completion execute
                                        (fn []
                                          (doseq [receipt receipts]
                                            (wal/complete-append!
                                             state receipt
                                             {:before-sync!
                                              (fn [_ round]
                                                (when (= 1 (:target-lsn round))
                                                  (deliver entered true)
                                                  (assert (deref release 10000 false))))}))
                                          (swap! applied conj prepared)
                                          (object-array prepared)))))
            first-job (future (group/submit! g runner (constantly 1)))
            jobs (atom [first-job])]
        (try
          (is (deref entered 10000 false))
          (swap! jobs conj (future (group/submit! g runner (constantly 2))))
          (is (deref second-appended 10000 false))
          (is (= 2 (:last-appended-lsn (status state))))
          (is (empty? @applied))
          (is (not-any? realized? @jobs))
          (deliver release true)
          (is (= [1 2] (mapv join @jobs)))
          (is (= #{[1] [2]} (set @applied)))
          (finally
            (deliver release true)
            (doseq [job @jobs] (join job))))))))

(deftest completed-old-receipt-cannot-flush-a-new-segment
  (with-runtime
    {:wal-segment-max-bytes 1}
    (fn [state]
      (let [old (wal/append-pending! state (rows 1) {})]
        ;; An unsettled append prevents rotation even though nobody owns sync.
        (wal/maybe-roll-segment! state (System/currentTimeMillis))
        (is (= 1 @(:segment-id state)))
        (wal/force-sync! state {})
        (let [new (wal/append-pending! state (rows 2) {})
              flushes (atom 0)
              hooks {:before-sync! (fn [_ _] (swap! flushes inc))}]
          (is (= 2 (append/segment-id new)))
          (is (false? (.isOpen ^FileChannel (append/channel old))))
          (is (:synced? (wal/complete-append! state old hooks)))
          (is (zero? @flushes))
          (is (= 1 (:last-durable-lsn (status state))))
          (is (:synced? (wal/complete-append! state new hooks)))
          (is (= 1 @flushes)))))))

(deftest concurrent-insertion-keeps-record-and-lsn-order
  (with-runtime
    {}
    (fn [state]
      (let [start (promise)
            jobs (mapv (fn [n] (future @start (wal/append-pending! state (rows n) {})))
                       (range 32))]
        (deliver start true)
        (let [receipts (mapv join jobs)
              scanned (:records (segment/scan-segment
                                 (wal/segment-path (:dir state) 1)))
              expected (vec (range 1 33))]
          (is (= expected (sort (map append/last-lsn receipts))))
          (is (= expected (mapv #(-> % :body wal/decode-commit-row-payload :lsn)
                                scanned)))
          (is (= 32 (:last-appended-lsn (status state))))
          (is (:synced? (wal/complete-append! state (first receipts) {})))
          (is (= 32 (:last-durable-lsn (status state)))))))))

(deftest relaxed-receipts-retain-their-submission-weight
  (with-runtime
    {:wal-durability-profile :relaxed :wal-group-commit 10 :wal-group-commit-ms 0}
    (fn [state]
      (let [first-receipt (wal/append-pending! state (rows 1) {:request-count 7})]
        (is (false? (:synced? (wal/complete-append! state first-receipt {}))))
        (is (= 7 (:unsynced-count (status state))))
        (is (= 1 (:pending-count (status state))))
        (let [second-receipt (wal/append-pending! state (rows 2) {:request-count 3})]
          (is (:synced? (wal/complete-append! state first-receipt {})))
          (is (:synced? (wal/complete-append! state second-receipt {})))
          (is (= [2 0] ((juxt :last-durable-lsn :pending-count) (status state)))))))))

(deftest failed-sync-does-not-allow-more-insertions
  (with-runtime
    {}
    (fn [state]
      (let [receipt (wal/append-pending! state (rows 1) {})
            error (IOException. "injected sync failure")]
        (is (identical? error
                        (caught #(wal/complete-append!
                                  state receipt {:before-sync! (fn [_ _] (throw error))}))))
        (is (= :txlog/unhealthy
               (:type (ex-data (caught #(wal/append-pending! state (rows 2) {}))))))
        (is (= 2 @(:next-lsn state)))
        (is (= 0 (:last-durable-lsn (status state))))))))

(deftest partial-append-poisons-the-tail-before-another-insertion
  (with-runtime
    {}
    (fn [state]
      (let [^FileChannel original @(:segment-channel state)
            calls (atom 0)
            error (IOException. "injected partial append")
            channel (proxy [FileChannel] []
                      (write [^ByteBuffer buffer position]
                        (if (= 1 (swap! calls inc))
                          (let [part (.duplicate buffer)]
                            (.limit part (+ (.position part) 3))
                            (let [n (.write original part (long position))]
                              (.position buffer (+ (.position buffer) n))
                              n))
                          (throw error)))
                      (implCloseChannel [] (.close original)))]
        (vreset! (:segment-channel state) channel)
        (is (identical? error (caught #(wal/append-pending! state (rows 1) {}))))
        (is (= :txlog/fatal
               (:type (ex-data (caught #(wal/append-pending! state (rows 2) {}))))))
        (is (= 1 @(:next-lsn state)))
        (is (= 0 (:last-appended-lsn (status state))))))))
