(ns datalevin.tx-group-batch-public-test
  (:require [clojure.java.io :as io]
            [clojure.test :refer [deftest is]]
            [datalevin.core :as d]
            [datalevin.binding.cpp.lifecycle :as lifecycle]
            [datalevin.interface :as i]
            [datalevin.lmdb :as l]
            [datalevin.tx-group.batch :as batch]
            [datalevin.tx-group.batch.env :as env]
            [datalevin.tx-group.batch.stage :as stage]
            [datalevin.tx-group.batch.recovery :as recovery]
            [datalevin.tx-group.phase :as phase]
            [datalevin.txlog.codec :as codec]
            [datalevin.txlog.segment :as segment]
            [datalevin.tx-state.protocol :as protocol]
            [datalevin.util :as u])
  (:import [java.util.concurrent CountDownLatch TimeUnit]))

(defn- options [wal?]
  {:write-mode :independent :wal? wal? :dbis {"data" {:validate-data? true}}
   :wal-sync-mode :fsync :wal-segment-prealloc? false :snapshot-scheduler? false})

(deftest public-typed-writes-and-native-transaction-reads
  (doseq [wal? [false true]]
    (let [dir (u/tmp-dir (str "public-batch-" (random-uuid)))
          db (d/open-kv dir (options wal?))]
      (try
        (d/open-dbi db "data")
        (is (= :transacted (d/transact-kv db "data" [[:put 1 {:n 2}]] :long :data)))
        (is (= {:n 2} (d/get-value db "data" 1 :long :data)))
        (is (= :done
               (d/with-transaction-kv [tx db {:context {:request :public}}]
                 (is (= {:request :public} (stage/tx-context (.-db ^datalevin.kv.KVLMDB tx))))
                 (is (= {:n 2} (d/get-value tx "data" 1 :long :data)))
                 (d/transact-kv tx "data" [[:put 2 {:n 3}]] :long :data)
                 (d/with-transaction-kv [nested tx]
                   (is (= {:n 3} (d/get-value nested "data" 2 :long :data))))
                 :done)))
        (is (= [[1 {:n 2}] [2 {:n 3}]]
               (vec (d/get-range db "data" [:all] :long :data))))
        (is (= 2 (d/range-count db "data" [:all] :long)))
        (is (= :transacted (d/transact-kv db "data" [[:del 1]] :long)))
        (is (= :transacted (d/transact-kv db [])))
        (is (nil? (d/get-value db "data" 1 :long :data)))
        (is (= wal? (:wal? (d/txlog-watermarks db))))
        (finally (d/close-kv db) (u/delete-files dir))))))

(deftest public-body-failure-rolls-back-once-and-next-write-succeeds
  (let [dir (u/tmp-dir (str "public-batch-abort-" (random-uuid)))
        db (d/open-kv dir (options true))
        calls (atom 0)]
    (try
      (is (thrown? clojure.lang.ExceptionInfo
                   (d/with-transaction-kv [tx db]
                     (swap! calls inc)
                     (d/transact-kv tx "data" [[:put 1 10]] :long :long)
                     (throw (ex-info "body failed" {})))))
      (is (= 1 @calls))
      (is (nil? (d/get-value db "data" 1 :long :long)))
      (is (= :transacted (d/update-kv db "data" 1 (fnil inc 0) :long :long)))
      (is (= 1 (d/get-value db "data" 1 :long :long)))
      (is (thrown? clojure.lang.ExceptionInfo
                   (d/with-transaction-kv [tx db]
                     (d/transact-kv tx "data" [[:put 1 20]] :long :long)
                     (d/abort-transact-kv tx))))
      (is (= 1 (d/get-value db "data" 1 :long :long)))
      (is (thrown? clojure.lang.ExceptionInfo
                   (d/with-transaction-kv [tx db]
                     (d/close-kv tx))))
      (is (not (d/closed-kv? db)))
      (finally (d/close-kv db) (u/delete-files dir)))))

(deftest public-aliases-share-runtime-and-reopen-restores-wal
  (let [dir (u/tmp-dir (str "public-batch-alias-" (random-uuid)))
        db (d/open-kv dir (options true))
        alias (d/open-kv (str dir "/."))]
    (try
      (is (identical? (i/kv-info db) (i/kv-info alias)))
      (is (= :independent (:write-mode (i/env-opts alias))))
      (is (not-any? fn? (vals (i/env-opts alias))))
      (is (thrown? clojure.lang.ExceptionInfo
                   (d/open-kv dir {:write-mode :compatibility})))
      (is (thrown? clojure.lang.ExceptionInfo
                   (l/open-kv dir {})))
      (is (thrown? clojure.lang.ExceptionInfo
                   (d/open-kv dir {:write-batch-size 1})))
      (d/close-kv db)
      (d/close-kv db)
      (is (d/closed-kv? db))
      (is (not (d/closed-kv? alias)))
      (is (thrown? clojure.lang.ExceptionInfo
                   (d/transact-kv db "data" [[:put 5 500]] :long :long)))
      (is (= :transacted (d/transact-kv alias "data" [[:put 5 50]] :long :long)))
      (d/close-kv alias)
      (let [reopened (d/open-kv dir)]
        (try
          (is (= 50 (d/get-value reopened "data" 5 :long :long)))
          (is (= 1 (:applied-lsn (d/txlog-watermarks reopened))))
          (is (map? (d/create-snapshot! reopened)))
          (is (= #{:current :previous} (set (map :slot (d/list-snapshots reopened)))))
          (finally (d/close-kv reopened))))
      (finally (d/close-kv db) (d/close-kv alias) (u/delete-files dir)))))

(deftest public-mixed-updates-have-no-lost-writes
  (doseq [profile [:strict :relaxed :extra]]
    (let [dir (u/tmp-dir (str "public-batch-mixed-" (random-uuid)))
          db (d/open-kv dir (assoc (options true) :wal-durability-profile profile
                                  :wal-group-commit 8 :wal-group-commit-ms 5))]
      (try
        (let [writers (doall (for [_ (range 8)]
                              (future
                                (dotimes [_ 20]
                                  (d/update-kv db "data" 1 (fnil inc 0) :long :long)
                                  (d/get-value db "data" 1 :long :long)))))]
          (doseq [writer writers] (is (not= ::timeout (deref writer 30000 ::timeout)))))
        (is (= 160 (d/get-value db "data" 1 :long :long)))
        (finally (d/close-kv db) (u/delete-files dir))))))

(deftest public-unsupported-operations-cannot-bypass-the-collector
  (let [dir (u/tmp-dir (str "public-batch-boundary-" (random-uuid)))
        db (d/open-kv dir (options true))]
    (try
      (doseq [operation [#(d/open-dbi db "undeclared")
                         #(d/drop-dbi db "data")
                         #(i/open-transact-kv db)
                         #(d/open-list-dbi db "list")
                         #(d/transact-kv db "data" [[:put 1 1 [:nooverwrite]]] :long :long)]]
        (is (thrown? clojure.lang.ExceptionInfo (operation))))
      (is (= ["data"] (vec (d/list-dbis db))))
      (is (= :transacted (d/transact-kv db "data" [[:put 1 1]] :long :long)))
      (finally (d/close-kv db) (u/delete-files dir)))))

(deftest public-invalid-options-fail-before-runtime-or-marker
  (doseq [invalid-options [{:wal-preparation-timeout-ms 0}
                   {:wal-pending-max-requests -1}
                   {:wal-rmw-max-bytes "large"}
                   {:dbis {"list" {:flags #{:create :integerkey}}}}]]
    (let [dir (u/tmp-dir (str "public-batch-invalid-" (random-uuid)))]
      (try
        (is (thrown? clojure.lang.ExceptionInfo
                     (d/open-kv dir (merge (options false) invalid-options))))
        (is (not (some #{dir} (env/active-environments))))
        (finally (when (.exists (io/file dir)) (u/delete-files dir)))))))

(deftest raw-opener-cannot-claim-independent-mode-with-a-compatibility-runtime
  (let [dir (u/tmp-dir (str "public-batch-raw-mode-" (random-uuid)))]
    (try
      (is (thrown? clojure.lang.ExceptionInfo
                   (l/open-kv dir {:write-mode :independent})))
      (is (nil? (protocol/read-write-protocol-marker dir)))
      (finally (when (.exists (io/file dir)) (u/delete-files dir))))))

(deftest public-option-validation-does-not-select-a-collector
  (let [dir (u/tmp-dir (str "public-compatible-options-" (random-uuid)))
        db (d/open-kv dir {:wal-pending-max-requests 4096
                          :wal-preparation-timeout-ms 1000})]
    (try
      (is (nil? (:independent-control @(i/kv-info db))))
      (is (nil? (protocol/read-write-protocol-marker dir)))
      (is (thrown? clojure.lang.ExceptionInfo
                   (protocol/acquire-write-protocol-lease! dir :kv-independent-v1 "foreign")))
      (is (nil? (protocol/read-write-protocol-marker dir)))
      (d/open-dbi db "dynamic")
      (is (= :transacted (d/transact-kv db "dynamic" [[:put 1 2]] :long :long)))
      (is (= 2 (d/get-value db "dynamic" 1 :long :long)))
      (finally (d/close-kv db) (u/delete-files dir)))))

(deftest public-body-budget-and-timeout-do-not-replay-the-body
  (let [dir (u/tmp-dir (str "public-batch-limits-" (random-uuid)))
        db (d/open-kv dir (assoc (options true) :wal-rmw-max-bytes 16384))
        calls (atom 0)]
    (try
      (is (thrown? clojure.lang.ExceptionInfo
                   (d/with-transaction-kv [tx db]
                     (swap! calls inc)
                     (d/transact-kv tx "data" [[:put 1 (byte-array 32768)]] :long :bytes))))
      (is (= 1 @calls))
      (is (nil? (d/get-value db "data" 1 :long :bytes)))
      (let [error (try
                    (d/with-transaction-kv [tx db {:timeout-ms 5}]
                      (swap! calls inc)
                      (d/get-value tx "data" 1 :long :long)
                      (Thread/sleep 50))
                    (catch clojure.lang.ExceptionInfo e e))]
        (is (= :transaction/timeout (:type (ex-data error)))))
      (is (= 2 @calls))
      (is (= :transacted (d/transact-kv db "data" [[:put 1 10]] :long :long)))
      (finally (d/close-kv db) (u/delete-files dir)))))

(deftest public-shutdown-drains-all-aliases-and-forces-relaxed-wal
  (let [dir (u/tmp-dir (str "public-batch-shutdown-" (random-uuid)))
        db (d/open-kv dir (assoc (options true) :wal-durability-profile :relaxed
                                :wal-group-commit 1000000 :wal-group-commit-ms 60000))
        alias (d/open-kv (str dir "/."))]
    (try
      (d/transact-kv db "data" [[:put 1 2]] :long :long)
      (lifecycle/run-shutdown-close! (i/env-dir db) db)
      (is (d/closed-kv? db))
      (is (d/closed-kv? alias))
      (is (= 1 (:durable-lsn (d/txlog-watermarks db))))
      (let [reopened (d/open-kv dir)]
        (try
          (is (= 2 (d/get-value reopened "data" 1 :long :long)))
          (finally (d/close-kv reopened))))
      (finally (d/close-kv db) (d/close-kv alias) (u/delete-files dir)))))

(deftest public-scheduled-snapshots-during-mixed-writes-reopen-correctly
  (let [dir (u/tmp-dir (str "public-batch-snapshots-" (random-uuid)))
        db (d/open-kv dir (assoc (options true) :snapshot-scheduler? true
                                :snapshot-interval-ms 200 :snapshot-max-age-ms 200
                                :snapshot-max-lsn-delta 1))]
    (try
      (let [writers (doall (for [_ (range 4)]
                            (future
                              (dotimes [_ 25]
                                (d/update-kv db "data" 1 (fnil inc 0) :long :long)
                                (d/get-value db "data" 1 :long :long)
                                (Thread/sleep 3)))))]
        (doseq [writer writers] (is (not= ::timeout (deref writer 10000 ::timeout)))))
      (let [deadline (+ (System/nanoTime) 2000000000)
            snapshot (loop []
                       (let [state (i/snapshot-scheduler-state db)]
                         (if (or (pos? (long (get-in state [:latest :floor-lsn] 0)))
                                 (> (System/nanoTime) deadline))
                           state
                           (do (Thread/sleep 10) (recur)))))]
        (is (true? (:running? snapshot)))
        (is (nil? (:last-error snapshot)))
        (is (pos? (long (get-in snapshot [:latest :floor-lsn] 0)))))
      (d/close-kv db)
      (let [reopened (d/open-kv dir)]
        (try
          (is (= 100 (d/get-value reopened "data" 1 :long :long)))
          (finally (d/close-kv reopened))))
      (finally (d/close-kv db) (u/delete-files dir)))))

(deftest public-list-writes-use-native-reads-and-recover-physical-effects
  (doseq [wal? [false true]]
    (let [dir (u/tmp-dir (str "public-batch-list-" (random-uuid)))
          opts (assoc (options wal?) :dbis {"list" {:flags #{:create :dupsort}
                                                   :validate-data? true}})
          db (d/open-kv dir opts)]
      (try
        (d/open-list-dbi db "list")
        (is (= :transacted
               (d/transact-kv db "list" [[:put-list 1 [10 20 30 20]]]
                              :long :long)))
        (is (= [10 20 30] (vec (d/get-list db "list" 1 :long :long))))
        (d/with-transaction-kv [tx db]
          (d/transact-kv tx "list" [[:del-list 1 [20 99]] [:put-list 1 [40]]]
                         :long :long)
          (is (= [10 30 40] (vec (d/get-list tx "list" 1 :long :long)))))
        (is (thrown? clojure.lang.ExceptionInfo
                     (d/with-transaction-kv [tx db]
                       (d/transact-kv tx "list" [[:del-list 1 [10]]
                                               [:put-list 1 [50]]]
                                      :long :long)
                       (throw (ex-info "abort list batch" {})))))
        (is (= [10 30 40] (vec (d/get-list db "list" 1 :long :long))))
        (d/transact-kv db "list" [[:del-list 1 [30 99]]] :long :long)
        (is (= [10 40] (vec (d/get-list db "list" 1 :long :long))))
        (d/close-kv db)
        (let [reopened (d/open-kv dir)]
          (try
            (is (= [10 40] (vec (d/get-list reopened "list" 1 :long :long))))
            (d/transact-kv reopened "list" [[:del 1]] :long)
            (is (empty? (d/get-list reopened "list" 1 :long :long)))
            (finally (d/close-kv reopened))))
        (finally (d/close-kv db) (u/delete-files dir))))))

(deftest public-clear-is-a-standalone-admin-transaction
  (doseq [wal? [false true]]
    (let [dir (u/tmp-dir (str "public-admin-clear-" (random-uuid)))
          db (d/open-kv dir (options wal?))]
      (try
        (d/transact-kv db "data" [[:put 1 10] [:put 2 20]] :long :long)
        (is (thrown? clojure.lang.ExceptionInfo
                     (d/with-transaction-kv [tx db] (d/clear-dbi tx "data"))))
        (is (= [[1 10] [2 20]] (vec (d/get-range db "data" [:all] :long :long))))
        (with-redefs [batch/submit! (fn [& _] (throw (ex-info "admin entered data collector" {})))]
          (is (nil? (d/clear-dbi db "data"))))
        (is (empty? (d/get-range db "data" [:all] :long :long)))
        (is (= ["data"] (vec (d/list-dbis db))))
        (is (thrown? clojure.lang.ExceptionInfo
                     (d/transact-kv db [(l/kv-tx :clear "data" nil nil :raw :raw)])))
        (d/transact-kv db "data" [[:put 4 40]] :long :long)
        (d/close-kv db)
        (let [reopened (d/open-kv dir)]
          (try
            (is (= [[4 40]] (vec (d/get-range reopened "data" [:all] :long :long))))
            (finally (d/close-kv reopened))))
        (finally (d/close-kv db) (u/delete-files dir))))))

(deftest public-list-budget-failure-and-empty-list-do-not-change-native-state
  (let [dir (u/tmp-dir (str "public-batch-list-budget-" (random-uuid)))
        db (d/open-kv dir (assoc (options true) :wal-rmw-max-bytes 16384
                                :dbis {"list" {:flags #{:create :dupsort}}}))]
    (try
      (is (= :transacted (d/put-list-items db "list" 1 [] :long :long)))
      (is (= :transacted (d/del-list-items db "list" 1 [] :long :long)))
      (d/put-list-items db "list" 1 [10 20] :long :long)
      (is (thrown? clojure.lang.ExceptionInfo
                   (d/with-transaction-kv [tx db]
                     (d/del-list-items tx "list" 1 [10] :long :long)
                     (d/put-list-items tx "list" 1 (range 1000) :long :long))))
      (is (= [10 20] (vec (d/get-list db "list" 1 :long :long))))
      (is (thrown? clojure.lang.ExceptionInfo
                   (d/put-list-items db "list" 1 [(byte-array 512)] :long :raw)))
      (d/del-list-items db "list" 1 [10] :long :long)
      (is (= [20] (vec (d/get-list db "list" 1 :long :long))))
      (finally (d/close-kv db) (u/delete-files dir)))))

(deftest public-admin-clear-waits-for-wal-policy-before-native-transaction
  (let [dir (u/tmp-dir (str "public-admin-clear-policy-" (random-uuid)))
        db (d/open-kv dir (options true))
        started (CountDownLatch. 1) release (CountDownLatch. 1)]
    (try
      (d/transact-kv db "data" [[:put 1 10]] :long :long)
      (with-redefs [segment/phase!
                    (fn [event _]
                      (when (= :force-started event)
                        (.countDown started)
                        (.await release 10 TimeUnit/SECONDS)))]
        (let [clear (future (d/clear-dbi db "data"))]
          (try
            (is (.await started 10 TimeUnit/SECONDS))
            (is (not (realized? clear)))
            (is (= 10 (d/get-value db "data" 1 :long :long)))
            (.countDown release)
            (is (nil? (deref clear 10000 ::timeout)))
            (is (nil? (d/get-value db "data" 1 :long :long)))
            (finally (.countDown release) (deref clear 10000 ::timeout)))))
      (finally (d/close-kv db) (u/delete-files dir)))))

(deftest public-admin-clear-follows-committed-data-before-result-publication
  (let [dir (u/tmp-dir (str "public-admin-clear-publication-" (random-uuid)))
        db (d/open-kv dir (options true))
        paused (CountDownLatch. 1) release (CountDownLatch. 1)
        uninstall (phase/observe!
                   (fn [event _]
                     (when (= event :joint-publication)
                       (.countDown paused)
                       (.await release 10 TimeUnit/SECONDS))))]
    (try
      (let [write (future (d/transact-kv db "data" [[:put 1 10]] :long :long))]
        (try
          (is (.await paused 10 TimeUnit/SECONDS))
          (is (nil? (d/clear-dbi db "data")))
          (is (= 2 (:applied-lsn (d/txlog-watermarks db))))
          (.countDown release)
          (is (= :transacted (deref write 10000 ::timeout)))
          (is (= 2 (:applied-lsn (d/txlog-watermarks db))))
          (is (nil? (d/get-value db "data" 1 :long :long)))
          (finally (.countDown release) (deref write 10000 ::timeout))))
      (finally (.countDown release) (uninstall) (d/close-kv db) (u/delete-files dir)))))

(deftest public-clear-and-list-recover-through-an-older-snapshot
  (let [dir (u/tmp-dir (str "public-batch-list-fallback-" (random-uuid)))
        db (d/open-kv dir (assoc (options true)
                                :dbis {"list" {:flags #{:create :dupsort}}}))]
    (try
      (d/put-list-items db "list" 1 [10 20 30] :long :long)
      (d/create-snapshot! db)
      (d/del-list-items db "list" 1 [20 99] :long :long)
      (d/clear-dbi db "list")
      (d/put-list-items db "list" 1 [40 50] :long :long)
      (d/del-list-items db "list" 1 [40] :long :long)
      (d/close-kv db)
      (spit (io/file (recovery/root {:dir dir}) "current" "snapshot.edn") "{:broken [")
      (let [reopened (d/open-kv dir)]
        (try
          (is (= [50] (vec (d/get-list reopened "list" 1 :long :long))))
          (finally (d/close-kv reopened))))
      (finally (d/close-kv db) (u/delete-files dir)))))

(deftest physical-duplicate-delete-and-clear-codec-roundtrip
  (let [key (byte-array [1 2]) value (byte-array [3 4])
        rows [(l/kv-tx :del-list "list" key [value] :raw :raw)
              (l/kv-tx :clear "list" nil nil :raw :raw)]
        body (codec/encode-commit-row-payload 7 0 rows)
        bounded (codec/decode-raw-commit-row-payload body 100000)
        generic (codec/decode-commit-row-payload body)]
    (is (= 7 (:lsn bounded)))
    (doseq [decoded [(:rows bounded) (:ops generic)]]
      (is (= [:del-list :clear] (mapv first decoded)))
      (is (= [1 2] (vec (nth (first decoded) 2))))
      (is (= [3 4] (vec (first (nth (first decoded) 3)))))
      (is (= [:clear "list" nil nil :raw :raw] (second decoded))))
    (is (thrown? clojure.lang.ExceptionInfo
                 (codec/decode-raw-commit-row-payload body 1)))))

(deftest public-fixed-encodings-and-raw-buffer-ownership
  (doseq [wal? [false true]]
    (let [dir (u/tmp-dir (str "public-fixed-encoding-" (random-uuid)))
          db (d/open-kv dir (assoc-in (options wal?) [:dbis "raw"] {}))]
      (try
        (doseq [[idx [type value]]
                (map-indexed vector [[:long -123] [:id 99]
                                     [:float (float 1.25)] [:double 2.5]
                                     [:boolean true]
                                     [:instant (java.util.Date. 123456)]
                                     [:uuid (java.util.UUID/randomUUID)]])]
          (d/transact-kv db "data" [[:put idx value]] :long type)
          (is (= value (d/get-value db "data" idx :long type))))
        (let [input (java.nio.ByteBuffer/wrap (byte-array [0 1 2 3]))]
          (.position input 1)
          (d/transact-kv db "raw" [[:put 100 input]] :long :raw)
          (is (= 1 (.position input)))
          (.put input 1 (byte 99))
          (is (= [1 2 3] (vec (d/get-value db "raw" 100 :long :raw)))))
        (finally (d/close-kv db) (u/delete-files dir))))))

(deftest public-mixed-physical-rows-preserve-order-and-recover-unicode-dbis
  (doseq [wal? [false true]]
    (let [dir (u/tmp-dir (str "public-mixed-preparation-" (random-uuid)))
          ordinary "資料😀"
          duplicates "列😀"
          db (d/open-kv dir (assoc (options wal?) :dbis
                                  {ordinary {:validate-data? true}
                                   duplicates {:flags #{:create :dupsort}
                                               :validate-data? true}}))]
      (try
        (is (= :transacted
               (d/transact-kv db [[:put ordinary 1 "old" :long :string]
                                  [:put-list duplicates "key" ["甲" "乙"] :string :string]
                                  [:del ordinary 1 :long]
                                  [:del-list duplicates "key" ["甲"] :string :string]
                                  [:put ordinary 1 "new" :long :string]
                                  [:put-list duplicates "empty" [] :string :string]])))
        (is (= "new" (d/get-value db ordinary 1 :long :string)))
        (is (= ["乙"] (vec (d/get-list db duplicates "key" :string :string))))
        (is (empty? (d/get-list db duplicates "empty" :string :string)))
        (d/close-kv db)
        (let [reopened (d/open-kv dir)]
          (try
            (is (= "new" (d/get-value reopened ordinary 1 :long :string)))
            (is (= ["乙"] (vec (d/get-list reopened duplicates "key" :string :string))))
            (finally (d/close-kv reopened))))
        (finally (d/close-kv db) (u/delete-files dir))))))
