(ns datalevin.wal-kv-test
  (:require
   [clojure.java.io :as io]
   [datalevin.lmdb :as l]
   [datalevin.bits :as b]
   [datalevin.overlay :as ol]
   [datalevin.wal :as wal]
   [datalevin.interface :as if]
   [datalevin.spill :as sp]
   [datalevin.util :as u]
   [datalevin.constants :as c]
   [datalevin.test.core :as tdc :refer [db-fixture]]
   [clojure.string :as s]
   [clojure.test :refer [deftest testing is use-fixtures]]
   [clojure.test.check.generators :as gen]
   [clojure.test.check.clojure-test :as test]
   [clojure.test.check.properties :as prop])
  (:import
   [java.io FileOutputStream]
   [java.util UUID]))

(use-fixtures :each db-fixture)

(defn- last-wal-id
  [lmdb]
  (:last-committed-wal-tx-id (if/kv-wal-watermarks lmdb)))

(defn- strictly-increasing?
  [xs]
  (every? true? (map < xs (rest xs))))

(defn- assert-kv-wal-post-commit-failpoint
  [step phase]
  (let [dir  (u/tmp-dir (str "kv-wal-fp-" (name step) "-" (name phase) "-"
                             (UUID/randomUUID)))
        lmdb (l/open-kv dir {:kv-wal? true})]
    (try
      (if/open-dbi lmdb "a")
      (let [base-id ^long (last-wal-id lmdb)
            ex      (try
                      (binding [c/*failpoint* {:step  step
                                               :phase phase
                                               :fn    #(throw (ex-info "fp-post" {}))}]
                        (if/transact-kv lmdb [[:put "a" 1 "x"]]))
                      nil
                      (catch Exception e e))]
        (is (instance? clojure.lang.ExceptionInfo ex))
        ;; WAL record is durable after step-3; recovery replays into overlay
        (is (= "x" (if/get-value lmdb "a" 1)))
        (is (= (u/long-inc base-id) (last-wal-id lmdb)))

        ;; After post-commit failure, WAL ids must continue from durable state.
        (if/transact-kv lmdb [[:put "a" 2 "y"]])
        (is (= (+ base-id 2) (last-wal-id lmdb)))
        (let [records (vec (wal/read-wal-records dir (dec base-id)
                                                 (+ base-id 2)))]
          (is (= #{base-id (inc base-id) (+ base-id 2)}
                 (set (map :wal/tx-id records))))))
      (finally
        (if/close-kv lmdb)
        (u/delete-files dir)))))

(defn- assert-kv-wal-crash-recovery-on-reopen
  [step phase durable?]
  (let [dir      (u/tmp-dir (str "kv-wal-crash-" (name step) "-" (name phase)
                                 "-" (UUID/randomUUID)))
        base-id  (volatile! 0)
        expected (if durable? "x" nil)]
    (try
      ;; Session 1: inject failpoint and simulate crash by forcing caller-visible
      ;; failure at a writer step.
      (let [lmdb (l/open-kv dir {:flags   (conj c/default-env-flags :nosync)
                                 :kv-wal? true})]
        (try
          (if/open-dbi lmdb "a")
          (let [start-id (last-wal-id lmdb)
                ex       (try
                           (binding [c/*failpoint* {:step  step
                                                    :phase phase
                                                    :fn    #(throw (ex-info "fp-crash" {}))}]
                             (if/transact-kv lmdb [[:put "a" 1 "x"]]))
                           nil
                           (catch Exception e e))]
            (vreset! base-id start-id)
            (is (instance? clojure.lang.ExceptionInfo ex))
            (is (= expected (if/get-value lmdb "a" 1))))
          (finally
            (if/close-kv lmdb))))

      ;; Session 2: reopen and verify persisted state is recovered.
      (let [lmdb (l/open-kv dir {:flags   (conj c/default-env-flags :nosync)
                                 :kv-wal? true})]
        (try
          (let [expected-id  (if durable? (u/long-inc @base-id) @base-id)
                recovered-id (last-wal-id lmdb)]
            (is (= expected-id recovered-id))
            (if/open-dbi lmdb "a")
            (is (= expected (if/get-value lmdb "a" 1)))
            (let [after-open-id (last-wal-id lmdb)]
              (is (>= after-open-id recovered-id))
              (if/transact-kv lmdb [[:put "a" 2 "y"]])
              (is (> (last-wal-id lmdb) after-open-id)))
            (is (= "y" (if/get-value lmdb "a" 2))))
          (finally
            (if/close-kv lmdb))))
      (finally
        (u/delete-files dir)))))

(defn- append-partial-record!
  [^java.io.File f record]
  ;; Write only a small prefix so the record header itself is incomplete.
  (let [n (max 1 (min 8 (dec (alength ^bytes record))))]
    (with-open [out (FileOutputStream. f true)]
      (.write out record 0 n))))

(defn- append-invalid-magic-tail!
  [^java.io.File f]
  (with-open [out (FileOutputStream. f true)]
    ;; `read-record` rejects this immediately with :wal/bad-magic.
    (.write out (byte-array [0 0 0 0]))))

(deftest kv-wal-enabled-basic-test
  (let [dir  (u/tmp-dir (str "kv-wal-default-" (UUID/randomUUID)))
        lmdb (l/open-kv dir {:kv-wal? true})]
    (try
      (if/open-dbi lmdb "a")
      (if/transact-kv lmdb [[:put "a" 1 "x"]])
      (let [{:keys [last-committed-wal-tx-id
                    last-indexed-wal-tx-id
                    last-committed-user-tx-id]}
            (if/kv-wal-watermarks lmdb)]
        (is (pos? last-committed-wal-tx-id))
        (is (= last-committed-wal-tx-id last-committed-user-tx-id))
        (is (<= 0 last-indexed-wal-tx-id last-committed-wal-tx-id)))
      (finally
        (if/close-kv lmdb)
        (u/delete-files dir)))))

(deftest kv-wal-append-test
  (let [dir  (u/tmp-dir (str "kv-wal-append-" (UUID/randomUUID)))
        lmdb (l/open-kv dir {:flags   (conj c/default-env-flags :nosync)
                             :kv-wal? true})]
    (try
      (if/open-dbi lmdb "a")
      (let [base-id ^long (last-wal-id lmdb)]
        (if/transact-kv lmdb [[:put "a" 1 "x"]
                              [:put "a" 2 "y"]])
        (let [wm      (if/kv-wal-watermarks lmdb)
              wal-id  (:last-committed-wal-tx-id wm)
              idx-id  (:last-indexed-wal-tx-id wm)
              user-id (:last-committed-user-tx-id wm)
              records (vec (wal/read-wal-records dir base-id wal-id))
              record  (last records)]
          (is (= (inc base-id) wal-id))
          (is (= 0 idx-id))
          (is (= wal-id user-id))
          (is (= wal-id (:wal/tx-id record)))
          (is (= 2 (count (:wal/ops record))))
          (is (= #{"a"} (set (map :dbi (:wal/ops record)))))
          (is (every? #(= :put (:op %)) (:wal/ops record))))

        (if/transact-kv lmdb [[:put "a" 3 "z"]])
        (is (= (+ base-id 2) (last-wal-id lmdb))))
      (finally
        (if/close-kv lmdb)
        (u/delete-files dir)))))

(deftest kv-wal-failpoint-step3-before-test
  (let [dir  (u/tmp-dir (str "kv-wal-fp-step3-before-" (UUID/randomUUID)))
        lmdb (l/open-kv dir {:flags   (conj c/default-env-flags :nosync)
                             :kv-wal? true})]
    (try
      (if/open-dbi lmdb "a")
      (let [base-id (last-wal-id lmdb)
            ex      (try
                      (binding [c/*failpoint* {:step  :step-3
                                               :phase :before
                                               :fn    #(throw (ex-info "fp-before" {}))}]
                        (if/transact-kv lmdb [[:put "a" 1 "x"]]))
                      nil
                      (catch Exception e e))]
        (is (instance? clojure.lang.ExceptionInfo ex))
        (is (nil? (if/get-value lmdb "a" 1)))
        (is (= base-id (last-wal-id lmdb))))
      (finally
        (if/close-kv lmdb)
        (u/delete-files dir)))))

(deftest kv-wal-failpoint-step3-after-test
  (let [dir  (u/tmp-dir (str "kv-wal-fp-step3-after-" (UUID/randomUUID)))
        lmdb (l/open-kv dir {:flags   (conj c/default-env-flags :nosync)
                             :kv-wal? true})]
    (try
      (if/open-dbi lmdb "a")
      (let [base-id ^long (last-wal-id lmdb)
            ex      (try
                      (binding [c/*failpoint* {:step  :step-3
                                               :phase :after
                                               :fn    #(throw (ex-info "fp-after" {}))}]
                        (if/transact-kv lmdb [[:put "a" 1 "x"]]))
                      nil
                      (catch Exception e e))]
        (is (instance? clojure.lang.ExceptionInfo ex))
        ;; Step-3 failpoint after WAL append — data is durable on disk and
        ;; recovery replays the WAL into the overlay, so it is visible.
        (is (= "x" (if/get-value lmdb "a" 1)))
        (is (= (inc base-id) (last-wal-id lmdb)))

        ;; After post-commit failure, subsequent tx should continue from next WAL id.
        (if/transact-kv lmdb [[:put "a" 2 "y"]])
        (is (= (+ base-id 2) (last-wal-id lmdb)))
        (let [records (vec (wal/read-wal-records dir (dec base-id)
                                                 (+ base-id 2)))]
          (is (= #{base-id (inc base-id) (+ base-id 2)}
                 (set (map :wal/tx-id records))))))
      (finally
        (if/close-kv lmdb)
        (u/delete-files dir)))))

(deftest kv-wal-failpoint-step4-after-test
  (assert-kv-wal-post-commit-failpoint :step-4 :after))

(deftest kv-wal-failpoint-step5-after-test
  (assert-kv-wal-post-commit-failpoint :step-5 :after))

(deftest kv-wal-failpoint-step6-after-test
  (assert-kv-wal-post-commit-failpoint :step-6 :after))

(deftest kv-wal-failpoint-step7-after-test
  (assert-kv-wal-post-commit-failpoint :step-7 :after))

(deftest kv-wal-failpoint-step8-after-nonfatal-test
  (let [dir  (u/tmp-dir (str "kv-wal-fp-step8-after-" (UUID/randomUUID)))
        lmdb (l/open-kv
               dir
               {:flags                  (conj c/default-env-flags :nosync)
                :kv-wal?                true
                :wal-meta-flush-max-txs 1})]
    (try
      (if/open-dbi lmdb "a")
      (let [base-id ^long (last-wal-id lmdb)]
        (is (= :transacted
               (binding [c/*failpoint* {:step  :step-8
                                        :phase :after
                                        :fn    #(throw (ex-info "fp-step8" {}))}]
                 (if/transact-kv lmdb [[:put "a" 1 "x"]]))))
        (is (= "x" (if/get-value lmdb "a" 1)))
        (is (= (inc base-id) (last-wal-id lmdb))))
      (finally
        (if/close-kv lmdb)
        (u/delete-files dir)))))

(deftest kv-wal-crash-recovery-reopen-matrix-test
  (doseq [[step phase durable?]
          [[:step-3 :before false]
           [:step-3 :after true]
           [:step-4 :before true]
           [:step-4 :after true]
           [:step-5 :before true]
           [:step-5 :after true]
           [:step-6 :before true]
           [:step-6 :after true]
           [:step-7 :before true]
           [:step-7 :after true]]]
    (testing (str "reopen recovery at " (name step) " " (name phase))
      (assert-kv-wal-crash-recovery-on-reopen step phase durable?))))

(deftest kv-wal-reopen-fails-on-overlay-recovery-error-test
  (let [dir  (u/tmp-dir (str "kv-wal-reopen-overlay-recover-fail-"
                             (UUID/randomUUID)))
        opts {:flags                  (conj c/default-env-flags :nosync)
              :kv-wal?                true
              :wal-meta-flush-max-txs 1
              :wal-meta-flush-max-ms  60000}]
    (try
      ;; Session 1: create a valid committed record, then advertise one more
      ;; committed WAL id and append an unreadable tail.
      (let [lmdb (l/open-kv dir opts)]
        (try
          (if/open-dbi lmdb "a")
          (if/transact-kv lmdb [[:put "a" 1 "x"]])
          (let [good-id    (last-wal-id lmdb)
                bad-id     (inc good-id)
                segment    (last (wal/segment-files dir))
                segment-id (or (some-> segment wal/parse-segment-id) 1)]
            (is segment)
            (append-invalid-magic-tail! segment)
            (wal/publish-wal-meta!
              dir
              {c/last-committed-wal-tx-id   bad-id
               c/last-indexed-wal-tx-id     0
               c/last-committed-user-tx-id  bad-id
               c/committed-last-modified-ms (System/currentTimeMillis)
               :wal/last-segment-id         segment-id
               :wal/enabled?                true}))
          (finally
            (if/close-kv lmdb))))

      ;; Session 2: reopen must fail; silent partial overlay recovery is unsafe.
      (let [opened (volatile! nil)]
        (binding [*out* (java.io.StringWriter.)
                  *err* (java.io.StringWriter.)]
          (is (thrown-with-msg? clojure.lang.ExceptionInfo
                                #"Fail to open database"
                                (vreset! opened (l/open-kv dir opts)))))
        (when @opened
          (if/close-kv @opened))
        (is (nil? @opened)))
      (finally
        (u/delete-files dir)))))

(deftest kv-wal-recover-from-truncated-tail-test
  (doseq [remove-meta? [false true]]
    (let [dir          (u/tmp-dir (str "kv-wal-crash-tail-"
                                       (if remove-meta? "scan" "meta") "-"
                                       (UUID/randomUUID)))
          committed-id (volatile! 0)]
      (try
        ;; Session 1: create two committed WAL records.
        (let [lmdb (l/open-kv dir {:flags   (conj c/default-env-flags :nosync)
                                   :kv-wal? true})]
          (try
            (if/open-dbi lmdb "a")
            (if/transact-kv lmdb [[:put "a" 1 "x"]])
            (if/transact-kv lmdb [[:put "a" 2 "y"]])
            (vreset! committed-id (last-wal-id lmdb))
            (finally
              (if/close-kv lmdb))))

        ;; Simulate crash during append of the next WAL record.
        (let [segment    (last (wal/segment-files dir))
              next-wal   (u/long-inc @committed-id)
              bad-record (wal/record-bytes
                           next-wal next-wal (System/currentTimeMillis)
                           [{:op  :put
                             :dbi "a"
                             :k   3
                             :v   "z"}])]
          (is segment)
          (append-partial-record! segment bad-record)
          (when remove-meta?
            (.delete (io/file (wal/wal-meta-path dir)))))

        ;; Session 2: reopen should ignore truncated tail and recover prefix.
        (let [lmdb (l/open-kv dir {:flags   (conj c/default-env-flags :nosync)
                                   :kv-wal? true})]
          (try
            (is (= @committed-id (last-wal-id lmdb)))
            (if/open-dbi lmdb "a")
            (is (= "x" (if/get-value lmdb "a" 1)))
            (is (= "y" (if/get-value lmdb "a" 2)))
            (is (nil? (if/get-value lmdb "a" 3)))
            (finally
              (if/close-kv lmdb))))
        (finally
          (u/delete-files dir))))))

(deftest kv-wal-replay-rejects-duplicate-or-backward-id-test
  (let [dir (u/tmp-dir (str "kv-wal-replay-order-" (UUID/randomUUID)))]
    (try
      ;; Session 1: write two committed WAL records.
      (let [lmdb (l/open-kv dir {:flags   (conj c/default-env-flags :nosync)
                                 :kv-wal? true})]
        (try
          (if/open-dbi lmdb "a")
          (if/transact-kv lmdb [[:put "a" 1 "x"]])
          (if/transact-kv lmdb [[:put "a" 2 "y"]])
          (finally
            (if/close-kv lmdb))))

      ;; Inject a duplicate/backward WAL id at tail: [1 2 1].
      (let [segment     (last (wal/segment-files dir))
            segment-id  (or (and segment (wal/parse-segment-id segment)) 1)
            duplicate-1 (wal/record-bytes
                          1 1 (System/currentTimeMillis)
                          [{:op  :put
                            :dbi "a"
                            :k   99
                            :v   "dup"}])]
        (is segment)
        (wal/append-record-bytes! dir segment-id duplicate-1 :none))

      ;; Session 2: replay must fail before applying duplicate id 1.
      (let [lmdb (l/open-kv dir {:flags   (conj c/default-env-flags :nosync)
                                 :kv-wal? true})]
        (try
          (if/open-dbi lmdb "a")
          (let [ex   (try
                       (if/flush-kv-indexer! lmdb)
                       nil
                       (catch Exception e e))
                data (when ex (ex-data ex))]
            (is (instance? clojure.lang.ExceptionInfo ex))
            (is (= :wal/out-of-order (:error data)))
            (is (= 1 (:wal-tx-id data)))
            (is (pos? (long (:applied-wal data))))
            (is (= (inc (long (:applied-wal data)))
                   (long (:expected-wal-tx-id data))))
            (is (< (long (:wal-tx-id data))
                   (long (:expected-wal-tx-id data)))))
          (finally
            (if/close-kv lmdb))))
      (finally
        (u/delete-files dir)))))

(deftest kv-wal-meta-publish-test
  (let [dir       (u/tmp-dir (str "kv-wal-meta-" (UUID/randomUUID)))
        lmdb      (l/open-kv
                    dir
                    {:flags                  (conj c/default-env-flags :nosync)
                     :kv-wal?                true
                     :wal-group-commit       1
                     :wal-meta-flush-max-txs 1
                     :wal-meta-flush-max-ms  60000})
        meta-path (str dir u/+separator+ c/wal-dir u/+separator+ c/wal-meta-file)]
    (try
      (if/open-dbi lmdb "a")
      (let [base-id ^long (last-wal-id lmdb)]
        (if/transact-kv lmdb [[:put "a" 1 "x"]])
        (is (.exists (io/file meta-path)))
        (let [meta (wal/read-wal-meta dir)]
          (is (= (inc base-id) (get meta c/last-committed-wal-tx-id)))
          (is (= 0 (get meta c/last-indexed-wal-tx-id)))
          (is (= (inc base-id) (get meta c/last-committed-user-tx-id)))
          (is (true? (get meta :wal/enabled?)))
          (is (pos? (long (or (get meta c/wal-meta-revision) 0))))
          (is (>= (long (or (get meta c/committed-last-modified-ms) -1)) 0))))
      (finally
        (if/close-kv lmdb)
        (u/delete-files dir)))))

(deftest kv-wal-meta-monotonic-publish-test
  (let [dir      (u/tmp-dir (str "kv-wal-meta-monotonic-" (UUID/randomUUID)))
        snapshot (fn [wal-id]
                   {c/last-committed-wal-tx-id   wal-id
                    c/last-indexed-wal-tx-id     wal-id
                    c/last-committed-user-tx-id  wal-id
                    c/committed-last-modified-ms (+ 1000 ^long wal-id)
                    :wal/last-segment-id         (quot ^long wal-id 10)
                    :wal/enabled?                true})]
    (try
      (wal/publish-wal-meta! dir (snapshot 100))
      ;; Late stale publisher must not move on-disk watermarks backward.
      (wal/publish-wal-meta! dir (snapshot 50))
      (let [meta (wal/read-wal-meta dir)]
        (is (= 100 (get meta c/last-committed-wal-tx-id)))
        (is (= 100 (get meta c/last-indexed-wal-tx-id)))
        (is (= 100 (get meta c/last-committed-user-tx-id)))
        (is (= 1100 (get meta c/committed-last-modified-ms)))
        (is (= 10 (get meta :wal/last-segment-id)))
        (is (= 2 (long (or (get meta c/wal-meta-revision) 0)))))
      (finally
        (u/delete-files dir)))))

(deftest kv-wal-meta-concurrent-publish-test
  (let [dir      (u/tmp-dir (str "kv-wal-meta-concurrent-" (UUID/randomUUID)))
        ids      (range 1 65)
        snapshot (fn [wal-id]
                   {c/last-committed-wal-tx-id   wal-id
                    c/last-indexed-wal-tx-id     wal-id
                    c/last-committed-user-tx-id  wal-id
                    c/committed-last-modified-ms (+ 1000 ^long wal-id)
                    :wal/last-segment-id         (quot ^long wal-id 10)
                    :wal/enabled?                true})]
    (try
      (->> ids
           (mapv #(future (wal/publish-wal-meta! dir (snapshot %))))
           (run! deref))
      (let [meta   (wal/read-wal-meta dir)
            max-id (apply max ids)]
        (is (= max-id (get meta c/last-committed-wal-tx-id)))
        (is (= max-id (get meta c/last-indexed-wal-tx-id)))
        (is (= max-id (get meta c/last-committed-user-tx-id)))
        (is (= (+ 1000 ^long max-id) (get meta c/committed-last-modified-ms)))
        (is (= (quot ^long max-id 10) (get meta :wal/last-segment-id)))
        (is (= (count ids) (long (or (get meta c/wal-meta-revision) 0)))))
      (finally
        (u/delete-files dir)))))

(deftest kv-wal-meta-flush-cadence-test
  (let [dir       (u/tmp-dir (str "kv-wal-meta-cadence-" (UUID/randomUUID)))
        lmdb      (l/open-kv
                    dir
                    {:flags                  (conj c/default-env-flags :nosync)
                     :kv-wal?                true
                     :wal-group-commit       1
                     :wal-meta-flush-max-txs 3
                     :wal-meta-flush-max-ms  60000})
        meta-path (str dir u/+separator+ c/wal-dir u/+separator+
                       c/wal-meta-file)]
    (try
      (if/open-dbi lmdb "a")
      (let [base-id ^long (last-wal-id lmdb)]
        (if/transact-kv lmdb [[:put "a" 1 "x"]])
        (is (not (.exists (io/file meta-path))))
        (if/transact-kv lmdb [[:put "a" 2 "y"]])
        (is (.exists (io/file meta-path)))
        (if/transact-kv lmdb [[:put "a" 3 "z"]])
        (let [meta (wal/read-wal-meta dir)]
          (is (= (+ base-id 2) (get meta c/last-committed-wal-tx-id)))
          (is (= 0 (get meta c/last-indexed-wal-tx-id)))
          (is (= (+ base-id 2) (get meta c/last-committed-user-tx-id)))))
      (finally
        (if/close-kv lmdb)
        (u/delete-files dir)))))

(deftest kv-wal-replay-test
  (let [dir  (u/tmp-dir (str "kv-wal-replay-" (UUID/randomUUID)))
        lmdb (l/open-kv dir {:flags   (conj c/default-env-flags :nosync)
                             :kv-wal? true})]
    (try
      (if/open-dbi lmdb "a")
      (if/transact-kv lmdb [[:put "a" 1 "x"]])
      (if/transact-kv lmdb [[:put "a" 2 "y"]
                            [:del "a" 1]])
      (let [committed-id (last-wal-id lmdb)]
        (is (= committed-id (:last-committed-wal-tx-id
                             (if/kv-wal-watermarks lmdb))))
        (binding [c/*trusted-apply* true
                  c/*bypass-wal*    true]
          (if/transact-kv lmdb c/kv-info
                          [[:put c/last-indexed-wal-tx-id 0]]
                          :data :data))
        (is (= 0 (if/get-value lmdb c/kv-info
                               c/last-indexed-wal-tx-id :data :data)))
        ;; Applied marker starts at 0; replay should apply WAL records.
        (is (nil? (if/get-value lmdb c/kv-info
                                c/applied-wal-tx-id :data :data)))

        (let [res (wal/replay-kv-wal! lmdb)]
          (is (= {:from 0 :to committed-id :applied committed-id} res)))
        (is (= committed-id (if/get-value lmdb c/kv-info
                                          c/last-indexed-wal-tx-id :data :data)))
        (is (= committed-id (if/get-value lmdb c/kv-info
                                          c/applied-wal-tx-id :data :data)))
        (is (= committed-id (:last-committed-wal-tx-id
                             (if/kv-wal-watermarks lmdb))))

        ;; Replay must not append additional WAL records.
        (is (= committed-id (count (wal/read-wal-records dir 0 committed-id))))
        (is (nil? (if/get-value lmdb "a" 1)))
        (is (= "y" (if/get-value lmdb "a" 2))))
      (finally
        (if/close-kv lmdb)
        (u/delete-files dir)))))

(deftest kv-wal-replay-list-ops-test
  (let [dir  (u/tmp-dir (str "kv-wal-replay-list-" (UUID/randomUUID)))
        lmdb (l/open-kv dir {:flags   (conj c/default-env-flags :nosync)
                             :kv-wal? true})]
    (try
      (if/open-list-dbi lmdb "list")
      (if/transact-kv lmdb [[:put-list "list" "a" [1 2 3] :string :long]])
      (if/transact-kv lmdb [[:del-list "list" "a" [1] :string :long]])
      (is (= [2 3] (if/get-list lmdb "list" "a" :string :long)))

      ;; Simulate base lagging behind committed WAL, then replay.
      ;; At this point, the base is cleared but the overlay still provides the data.
      (binding [c/*bypass-wal* true]
        (if/clear-dbi lmdb "list"))
      (is (= [2 3] (if/get-list lmdb "list" "a" :string :long)))
      ;; Reset watermarks to simulate base lagging behind WAL.
      ;; Use trusted apply to avoid emitting WAL records for kv-info setup.
      (binding [c/*trusted-apply* true
                c/*bypass-wal*    true]
        (if/transact-kv lmdb c/kv-info
                        [[:put c/last-indexed-wal-tx-id 0]
                         [:put c/applied-wal-tx-id 0]]
                        :data :data))

      (let [committed-id (last-wal-id lmdb)]
        (is (= {:from 0 :to committed-id :applied committed-id}
               (wal/replay-kv-wal! lmdb))))
      (is (= [2 3] (if/get-list lmdb "list" "a" :string :long)))
      (finally
        (if/close-kv lmdb)
        (u/delete-files dir)))))

(deftest kv-wal-reopen-overlay-rebuild-test
  (let [dir (u/tmp-dir (str "kv-wal-reopen-" (UUID/randomUUID)))]
    (try
      ;; Session 1: write data, do NOT flush, close.
      (let [lmdb (l/open-kv dir {:flags   (conj c/default-env-flags :nosync)
                                 :kv-wal? true})]
        (if/open-dbi lmdb "a")
        (if/open-list-dbi lmdb "l")
        (if/transact-kv lmdb [[:put "a" 1 "x"]
                              [:put "a" 2 "y"]
                              [:put-list "l" 10 [100 200 300] :long :long]])
        (if/transact-kv lmdb [[:del "a" 1]
                              [:put "a" 3 "z"]
                              [:del-list "l" 10 [200] :long :long]])
        ;; Verify data visible via overlay before close.
        (is (nil? (if/get-value lmdb "a" 1)))
        (is (= "y" (if/get-value lmdb "a" 2)))
        (is (= "z" (if/get-value lmdb "a" 3)))
        (is (= [100 300] (if/get-list lmdb "l" 10 :long :long)))
        ;; Watermarks: committed > indexed (un-flushed).
        (let [wm (if/kv-wal-watermarks lmdb)]
          (is (> (:last-committed-wal-tx-id wm)
                 (:last-indexed-wal-tx-id wm))))
        (if/close-kv lmdb))

      ;; Session 2: reopen — overlay should be rebuilt from WAL.
      (let [lmdb (l/open-kv dir {:flags   (conj c/default-env-flags :nosync)
                                 :kv-wal? true})]
        (if/open-dbi lmdb "a")
        (if/open-list-dbi lmdb "l")
        ;; Committed writes must be visible immediately after reopen.
        (is (nil? (if/get-value lmdb "a" 1)) "deleted key invisible after reopen")
        (is (= "y" (if/get-value lmdb "a" 2)) "put visible after reopen")
        (is (= "z" (if/get-value lmdb "a" 3)) "put visible after reopen")
        (is (= [100 300] (if/get-list lmdb "l" 10 :long :long))
            "list ops visible after reopen")
        ;; Range queries should also work.
        (is (= [[2 "y"] [3 "z"]]
               (vec (if/get-range lmdb "a" [:all] :data :data)))
            "get-range after reopen")
        (if/close-kv lmdb))
      (finally
        (u/delete-files dir)))))

(deftest kv-wal-reopen-overlay-rebuild-dupsort-put-test
  (let [dir (u/tmp-dir (str "kv-wal-reopen-dupsort-put-" (UUID/randomUUID)))]
    (try
      ;; Session 1: use :put (not :put-list) on dupsort DBI, clear base, close.
      (let [lmdb (l/open-kv dir {:flags   (conj c/default-env-flags :nosync)
                                 :kv-wal? true})]
        (if/open-list-dbi lmdb "l")
        (if/transact-kv lmdb [[:put "l" "k" 10 :string :long]
                              [:put "l" "k" 20 :string :long]])
        (binding [c/*bypass-wal* true]
          (if/clear-dbi lmdb "l"))
        ;; Visible via overlay in session 1.
        (is (= [10 20] (vec (if/get-list lmdb "l" "k" :string :long))))
        (if/close-kv lmdb))

      ;; Session 2: reopen — overlay recovery must preserve dupsort semantics.
      (let [lmdb (l/open-kv dir {:flags   (conj c/default-env-flags :nosync)
                                 :kv-wal? true})]
        (if/open-list-dbi lmdb "l")
        (is (= [10 20] (vec (if/get-list lmdb "l" "k" :string :long)))
            ":put WAL ops on dupsort DBI survive reopen before indexer catch-up")
        (is (= [["k" 10] ["k" 20]]
               (vec (if/get-range lmdb "l" [:all] :string :long))))
        (if/close-kv lmdb))
      (finally
        (u/delete-files dir)))))

(deftest kv-wal-tx-id-monotonic-across-reopen-test
  (let [dir (u/tmp-dir (str "kv-wal-id-monotonic-reopen-" (UUID/randomUUID)))]
    (try
      (let [lmdb (l/open-kv dir {:flags   (conj c/default-env-flags :nosync)
                                 :kv-wal? true})]
        (try
          (if/open-dbi lmdb "a")
          (if/transact-kv lmdb [[:put "a" 1 "x"]])
          (if/transact-kv lmdb [[:put "a" 2 "y"]])
          (if/transact-kv lmdb [[:put "a" 3 "z"]])
          (finally
            (if/close-kv lmdb))))

      (let [ids-session1 (mapv :wal/tx-id (wal/read-wal-records dir 0 Long/MAX_VALUE))
            lmdb         (l/open-kv dir {:flags   (conj c/default-env-flags :nosync)
                                         :kv-wal? true})]
        (try
          (if/open-dbi lmdb "a")
          (if/transact-kv lmdb [[:put "a" 4 "w"]])
          (let [ids-after-reopen (mapv :wal/tx-id
                                       (wal/read-wal-records
                                         dir 0 Long/MAX_VALUE))]
            (is (seq ids-session1))
            (is (seq ids-after-reopen))
            (is (strictly-increasing? ids-session1)
                (str "session1 WAL tx-ids must be strictly increasing: "
                     ids-session1))
            (is (strictly-increasing? ids-after-reopen)
                (str "WAL tx-ids must be strictly increasing after reopen: "
                     ids-after-reopen))
            (is (> (last ids-after-reopen) (last ids-session1))))
          (finally
            (if/close-kv lmdb))))
      (finally
        (u/delete-files dir)))))

(deftest kv-wal-dbi-guard-test
  (let [dir  (u/tmp-dir (str "kv-wal-guard-" (UUID/randomUUID)))
        lmdb (l/open-kv dir {:flags   (conj c/default-env-flags :nosync)
                             :kv-wal? true})]
    (try
      (let [ex (try
                 (if/transact-kv lmdb [[:put "nonexistent-dbi" 1 (byte-array [1]) :id :raw]])
                 nil
                 (catch Exception e e))]
        (is (instance? clojure.lang.ExceptionInfo ex))
        (is (s/includes? (ex-message ex) "not open")))
      (finally
        (if/close-kv lmdb)
        (u/delete-files dir)))))

(deftest kv-wal-replay-bounded-and-noop-test
  (let [dir   (u/tmp-dir (str "kv-wal-replay-bounded-" (UUID/randomUUID)))
        lmdb  (l/open-kv dir {:flags   (conj c/default-env-flags :nosync)
                              :kv-wal? true})
        dir2  (u/tmp-dir (str "kv-wal-replay-noop-" (UUID/randomUUID)))
        lmdb2 (l/open-kv dir2 {:flags (conj c/default-env-flags :nosync)})]
    (try
      (if/open-dbi lmdb "a")
      (let [base-id (last-wal-id lmdb)]
        (if/transact-kv lmdb [[:put "a" 1 "x"]])
        (if/transact-kv lmdb [[:put "a" 2 "y"]])
        (binding [c/*trusted-apply* true
                  c/*bypass-wal*    true]
          (if/transact-kv lmdb c/kv-info
                          [[:put c/last-indexed-wal-tx-id 0]
                           [:put c/applied-wal-tx-id 0]]
                          :data :data))

        (is (= {:from 0 :to base-id :applied base-id}
               (wal/replay-kv-wal! lmdb base-id)))
        (is (= base-id (if/get-value lmdb c/kv-info
                                     c/last-indexed-wal-tx-id :data :data)))
        (is (= base-id (if/get-value lmdb c/kv-info
                                     c/applied-wal-tx-id :data :data)))

        (is (= {:from base-id :to base-id :applied 0}
               (wal/replay-kv-wal! lmdb base-id)))

        (is (= {:from 0 :to 0 :applied 0}
               (wal/replay-kv-wal! lmdb2))))
      (finally
        (if/close-kv lmdb)
        (if/close-kv lmdb2)
        (u/delete-files dir)
        (u/delete-files dir2)))))

(deftest kv-wal-memory-pressure-flush-test
  (let [dir  (u/tmp-dir (str "kv-wal-mem-pressure-" (UUID/randomUUID)))
        lmdb (l/open-kv dir {:flags      (conj c/default-env-flags :nosync)
                             :kv-wal?    true
                             :spill-opts {:spill-threshold 1}})]
    (try
      (if/open-dbi lmdb "a")
      (let [base-id       ^long (last-wal-id lmdb)
            prev-pressure @sp/memory-pressure]
        (try
          (vreset! sp/memory-pressure 1)
          (if/open-transact-kv lmdb)
          (if/transact-kv lmdb [[:put "a" 1 "x"]])
          (is (= :committed (if/close-transact-kv lmdb)))
          (let [wm (if/kv-wal-watermarks lmdb)]
            (is (= (inc base-id) (:last-committed-wal-tx-id wm)))
            (is (= (inc base-id) (:last-indexed-wal-tx-id wm))))
          (finally
            (vreset! sp/memory-pressure prev-pressure))))
      (finally
        (if/close-kv lmdb)
        (u/delete-files dir)))))

(deftest kv-wal-replay-legacy-applied-marker-fallback-test
  (let [dir  (u/tmp-dir (str "kv-wal-replay-legacy-applied-" (UUID/randomUUID)))
        lmdb (l/open-kv dir {:flags   (conj c/default-env-flags :nosync)
                             :kv-wal? true})]
    (try
      (if/open-dbi lmdb "a")
      (if/transact-kv lmdb [[:put "a" 1 "x"]])
      (if/transact-kv lmdb [[:put "a" 2 "y"]])

      ;; Simulate a pre-separation marker store: legacy key exists, new key absent.
      ;; Use trusted apply to avoid emitting WAL records for kv-info setup.
      (binding [c/*trusted-apply* true
                c/*bypass-wal*    true]
        (if/transact-kv lmdb c/kv-info
                        [[:del c/applied-wal-tx-id]
                         [:put c/legacy-applied-tx-id 0]
                         [:put c/last-indexed-wal-tx-id 0]]
                        :data :data))

      (let [committed-id (last-wal-id lmdb)]
        (is (= {:from 0 :to committed-id :applied committed-id}
               (wal/replay-kv-wal! lmdb)))
        (is (= committed-id
               (if/get-value lmdb c/kv-info c/applied-wal-tx-id :data :data)))
        (is (= committed-id
               (if/get-value lmdb c/kv-info
                             c/last-indexed-wal-tx-id :data :data))))
      (is (= "x" (if/get-value lmdb "a" 1)))
      (is (= "y" (if/get-value lmdb "a" 2)))
      (finally
        (if/close-kv lmdb)
        (u/delete-files dir)))))

(deftest kv-wal-overlay-merge-and-prune-test
  (let [dir  (u/tmp-dir (str "kv-wal-overlay-merge-" (UUID/randomUUID)))
        lmdb (l/open-kv dir {:flags   (conj c/default-env-flags :nosync)
                             :kv-wal? true})]
    (try
      (if/open-dbi lmdb "a")
      (if/transact-kv lmdb [[:put "a" 1 "x"]
                            [:put "a" 2 "y"]
                            [:put "a" 3 "z"]])
      (if/transact-kv lmdb [[:del "a" 2]])

      ;; Simulate WAL tail visibility when base is stale: clear base only,
      ;; keep committed WAL overlay intact.
      (binding [c/*bypass-wal* true]
        (if/clear-dbi lmdb "a"))
      (is (= "x" (if/get-value lmdb "a" 1)))
      (is (nil? (if/get-value lmdb "a" 2)))
      (is (= "z" (if/get-value lmdb "a" 3)))
      (is (= [[1 "x"] [3 "z"]]
             (vec (if/get-range lmdb "a" [:all]))))
      (is (= [[3 "z"] [1 "x"]]
             (vec (if/get-range lmdb "a" [:all-back]))))
      (is (= [[1 "x"]]
             (vec (if/get-range lmdb "a" [:closed 1 2]))))
      (is (= ["x" "z"]
             (vec (if/get-range lmdb "a" [:all] :data :data true))))
      (is (= [1 "x"] (if/get-first lmdb "a" [:all] :data :data false)))
      (is (= true (if/get-first lmdb "a" [:all] :data :ignore true)))
      (is (= [[1 "x"] [3 "z"]]
             (vec (if/get-first-n lmdb "a" 5 [:all] :data :data false))))
      (is (= [1 3] (vec (if/key-range lmdb "a" [:all] :data))))
      (is (= 2 (if/key-range-count lmdb "a" [:all] :data)))
      (is (= 2 (if/range-count lmdb "a" [:all] :data)))
      (is (= [3 "z"]
             (if/get-some lmdb "a" (fn [k _] (= k 3))
                          [:all] :data :data false false)))
      (is (= [[1 "x"]]
             (vec (if/range-filter lmdb "a" (fn [k _] (= k 1))
                                   [:all] :data :data false false))))
      (is (= ["x!"]
             (vec (if/range-keep lmdb "a"
                                 (fn [k v] (when (= k 1) (str v "!")))
                                 [:all] :data :data false))))
      (is (= "x!"
             (if/range-some lmdb "a"
                            (fn [k v] (when (= k 1) (str v "!")))
                            [:all] :data :data false)))
      (is (= 1
             (if/range-filter-count lmdb "a" (fn [k _] (= k 1))
                                    [:all] :data :data false)))
      (let [visited  (volatile! [])
            kvisited (volatile! [])]
        (if/visit lmdb "a" (fn [k v] (vswap! visited conj [k v]))
                  [:all] :data :data false)
        (if/visit-key-range lmdb "a" (fn [k] (vswap! kvisited conj k))
                            [:all] :data false)
        (is (= [[1 "x"] [3 "z"]] @visited))
        (is (= [1 3] @kvisited)))
      (is (= 0 (if/get-rank lmdb "a" 1 :data)))
      (is (= 1 (if/get-rank lmdb "a" 3 :data)))
      (is (nil? (if/get-rank lmdb "a" 2 :data)))
      (is (= [1 "x"] (if/get-by-rank lmdb "a" 0 :data :data false)))
      (is (= [3 "z"] (if/get-by-rank lmdb "a" 1 :data :data false)))
      (is (nil? (if/get-by-rank lmdb "a" 2 :data :data false)))
      (is (= [[1 "x"] [3 "z"]]
             (vec (if/sample-kv lmdb "a" 2 :data :data false))))
      (is (= ["x" "z"]
             (vec (if/sample-kv lmdb "a" 2 :data :data true))))
      (is (= [[1 "x"] [3 "z"]]
             (->> (if/range-seq lmdb "a" [:all] :data :data false
                                {:batch-size 1})
                  seq
                  vec)))
      (let [sampled (volatile! [])]
        (if/visit-key-sample lmdb "a" (long-array [0 1]) 0 0
                             (fn [k] (vswap! sampled conj k))
                             [:all] :data false)
        (is (= [1 3] @sampled)))

      ;; Replay catches base up and prunes committed overlay <= indexed watermark.
      ;; Use trusted apply to avoid emitting WAL records for kv-info setup.
      (binding [c/*trusted-apply* true
                c/*bypass-wal*    true]
        (if/transact-kv lmdb c/kv-info
                        [[:put c/last-indexed-wal-tx-id 0]
                         [:put c/applied-wal-tx-id 0]]
                        :data :data))
      (let [committed-id (last-wal-id lmdb)]
        (is (= {:indexed-wal-tx-id   committed-id
                :committed-wal-tx-id committed-id
                :drained?            true}
               (if/flush-kv-indexer! lmdb))))

      ;; With pruned overlay, clearing base again leaves no values visible.
      (if/clear-dbi lmdb "a")
      (is (nil? (if/get-value lmdb "a" 1)))
      (is (empty? (if/get-range lmdb "a" [:all])))
      (finally
        (if/close-kv lmdb)
        (u/delete-files dir)))))

(deftest kv-wal-overlay-merge-and-prune-dupsort-test
  (let [dir  (u/tmp-dir (str "kv-wal-overlay-dupsort-" (UUID/randomUUID)))
        lmdb (l/open-kv dir {:flags   (conj c/default-env-flags :nosync)
                             :kv-wal? true})]
    (try
      (if/open-list-dbi lmdb "l")
      (if/put-list-items lmdb "l" "a" [1 2 3] :string :long)
      (if/put-list-items lmdb "l" "b" [4 5] :string :long)
      (if/del-list-items lmdb "l" "a" [2] :string :long)
      (if/put-list-items lmdb "l" "a" [6] :string :long)
      (if/del-list-items lmdb "l" "b" :string)
      (if/put-list-items lmdb "l" "c" [7] :string :long)

      ;; Simulate WAL tail visibility when base is stale: clear base only,
      ;; keep committed WAL overlay intact.
      (binding [c/*bypass-wal* true]
        (if/clear-dbi lmdb "l"))
      (is (= 1 (if/get-value lmdb "l" "a" :string :long)))
      (is (nil? (if/get-value lmdb "l" "b" :string :long)))
      (is (= [["a" 1] ["a" 3] ["a" 6] ["c" 7]]
             (vec (if/get-range lmdb "l" [:all] :string :long))))
      (is (= [["c" 7] ["a" 1] ["a" 3] ["a" 6]]
             (vec (if/get-range lmdb "l" [:all-back] :string :long))))
      (is (= [["a" 1] ["a" 3] ["a" 6]]
             (vec (if/get-range lmdb "l" [:closed "a" "b"] :string :long))))
      (is (= [["a" nil] ["a" nil] ["a" nil] ["c" nil]]
             (vec (if/get-range lmdb "l" [:all] :string :ignore false))))
      (is (= ["a" 1]
             (if/get-first lmdb "l" [:all] :string :long false)))
      (is (= [["a" 1] ["a" 3] ["a" 6]]
             (vec (if/get-first-n lmdb "l" 3 [:all] :string :long false))))
      (is (= ["a" "c"] (vec (if/key-range lmdb "l" [:all] :string))))
      (is (= 2 (if/key-range-count lmdb "l" [:all] :string)))
      (is (= 4 (if/range-count lmdb "l" [:all] :string)))
      (is (= ["c" 7]
             (if/get-some lmdb "l" (fn [_ v] (= v 7))
                          [:all] :string :long false false)))
      (is (= [["a" 6]]
             (vec (if/range-filter lmdb "l"
                                   (fn [k v] (and (= k "a") (= v 6)))
                                   [:all] :string :long false false))))
      (let [kvisited (volatile! [])]
        (if/visit-key-range lmdb "l" (fn [k] (vswap! kvisited conj k))
                            [:all] :string false)
        (is (= ["a" "c"] @kvisited)))
      (is (= [1 3 6] (vec (if/get-list lmdb "l" "a" :string :long))))
      (is (= [] (vec (if/get-list lmdb "l" "b" :string :long))))
      (is (= 3 (if/list-count lmdb "l" "a" :string)))
      (is (= 0 (if/list-count lmdb "l" "b" :string)))
      (is (if/in-list? lmdb "l" "a" 3 :string :long))
      (is (not (if/in-list? lmdb "l" "a" 2 :string :long)))
      (is (= 3 (b/read-buffer (if/near-list lmdb "l" "a" 2 :string :long)
                              :long)))
      (is (nil? (if/near-list lmdb "l" "b" 1 :string :long)))
      (is (= [["a" 1] ["a" 3] ["a" 6] ["c" 7]]
             (vec (if/list-range lmdb "l" [:all] :string [:all] :long))))
      (is (= [["a" 6] ["a" 3] ["a" 1] ["c" 7]]
             (vec (if/list-range lmdb "l" [:all] :string [:all-back] :long))))
      (is (= ["a" 1]
             (if/list-range-first lmdb "l" [:all] :string [:all] :long)))
      (is (= [["a" 1] ["a" 3]]
             (vec (if/list-range-first-n lmdb "l" 2 [:all] :string
                                         [:all] :long))))
      (is (= 4 (if/list-range-count lmdb "l" [:all] :string [:all] :long)))
      (is (= 4 (if/key-range-list-count lmdb "l" [:all] :string)))
      (is (= [["a" 6]]
             (vec (if/list-range-filter lmdb "l"
                                        (fn [k v] (and (= k "a") (= v 6)))
                                        [:all] :string [:all] :long false))))
      (is (= [6]
             (vec (if/list-range-keep lmdb "l"
                                      (fn [k v] (when (and (= k "a") (= v 6))
                                                  v))
                                      [:all] :string [:all] :long false))))
      (is (= 6
             (if/list-range-some lmdb "l"
                                 (fn [k v] (when (and (= k "a") (= v 6))
                                             v))
                                 [:all] :string [:all] :long false)))
      (is (= 1
             (if/list-range-filter-count lmdb "l"
                                         (fn [k v] (and (= k "a") (= v 6)))
                                         [:all] :string [:all] :long false)))
      (let [vals (volatile! [])]
        (if/visit-list lmdb "l" (fn [v] (vswap! vals conj v))
                       "a" :string :long false)
        (is (= [1 3 6] @vals)))
      (let [visited (volatile! [])]
        (if/visit-list-range lmdb "l" (fn [k v] (vswap! visited conj [k v]))
                             [:all] :string [:all] :long false)
        (is (= [["a" 1] ["a" 3] ["a" 6] ["c" 7]] @visited)))
      (let [visited (volatile! [])]
        (if/visit-list-key-range lmdb "l" (fn [k v] (vswap! visited conj [k v]))
                                 [:all] :string :long false)
        (is (= [["a" 1] ["a" 3] ["a" 6] ["c" 7]] @visited)))
      (let [sampled (volatile! [])]
        (if/visit-list-sample lmdb "l" (long-array [1 3]) 0 0
                              (fn [k v] (vswap! sampled conj [k v]))
                              [:all] :string :long false)
        (is (= [["a" 3] ["c" 7]] @sampled)))
      ;; Replay catches base up and prunes committed overlay <= indexed watermark.
      ;; Use trusted apply to avoid emitting WAL records for kv-info setup.
      (binding [c/*trusted-apply* true
                c/*bypass-wal*    true]
        (if/transact-kv lmdb c/kv-info
                        [[:put c/last-indexed-wal-tx-id 0]
                         [:put c/applied-wal-tx-id 0]]
                        :data :data))
      (let [committed-id (last-wal-id lmdb)]
        (is (= {:indexed-wal-tx-id   committed-id
                :committed-wal-tx-id committed-id
                :drained?            true}
               (if/flush-kv-indexer! lmdb))))

      ;; With pruned overlay, clearing base again leaves no values visible.
      (if/clear-dbi lmdb "l")
      (is (nil? (if/get-value lmdb "l" "a" :string :long)))
      (is (empty? (if/get-range lmdb "l" [:all] :string :long)))
      (finally
        (if/close-kv lmdb)
        (u/delete-files dir)))))

(deftest kv-wal-native-counts-do-not-materialize-ranges-test
  (let [dir  (u/tmp-dir (str "kv-wal-native-counts-" (UUID/randomUUID)))
        lmdb (l/open-kv dir {:flags   (conj c/default-env-flags :nosync)
                             :kv-wal? true})]
    (try
      (if/open-list-dbi lmdb "l")
      (if/put-list-items lmdb "l" "a" [1 2 3] :string :long)
      (if/put-list-items lmdb "l" "b" [4 5] :string :long)
      (if/del-list-items lmdb "l" "a" [2] :string :long)
      (if/put-list-items lmdb "l" "c" [6] :string :long)
      ;; Force reliance on WAL overlay state by clearing base only.
      (binding [c/*bypass-wal* true]
        (if/clear-dbi lmdb "l"))

      (with-redefs [if/key-range  (fn [& _]
                                    (throw (ex-info "unexpected key-range"
                                                    {})))
                    if/list-range (fn [& _]
                                    (throw (ex-info "unexpected list-range"
                                                    {})))]
        (is (= 3 (if/key-range-count lmdb "l" [:all] :string)))
        (is (= 5 (if/key-range-list-count lmdb "l" [:all] :string)))
        ;; list-range-count is approximate: value-range bounds are ignored.
        (is (= 5 (if/list-range-count lmdb "l" [:all] :string
                                      [:closed 1 5] :long)))
        (is (= 5 (if/list-range-count lmdb "l" [:all] :string
                                      [:greater-than 3] :long)))
        (is (= 4 (if/list-range-count lmdb "l" [:less-than "c"] :string
                                      [:closed 5 99] :long)))
        (is (= 3 (if/list-range-count lmdb "l" [:greater-than "a"] :string
                                      [:closed 5 99] :long))))
      (finally
        (if/close-kv lmdb)
        (u/delete-files dir)))))

(deftest kv-wal-watermarks-and-flush-indexer-test
  (let [dir   (u/tmp-dir (str "kv-wal-watermarks-" (UUID/randomUUID)))
        lmdb  (l/open-kv dir {:flags   (conj c/default-env-flags :nosync)
                              :kv-wal? true})
        dir2  (u/tmp-dir (str "kv-wal-watermarks-noop-" (UUID/randomUUID)))
        lmdb2 (l/open-kv dir2 {:flags (conj c/default-env-flags :nosync)})]
    (try
      (if/open-dbi lmdb "a")
      (let [base-id ^long (last-wal-id lmdb)]
        (if/transact-kv lmdb [[:put "a" 1 "x"]])
        (if/transact-kv lmdb [[:put "a" 2 "y"]])
        (let [committed-id (+ base-id 2)
              partial-id   (inc base-id)]
          (is (= {:last-committed-wal-tx-id  committed-id
                  :last-indexed-wal-tx-id    0
                  :last-committed-user-tx-id committed-id}
                 (wal/kv-wal-watermarks lmdb)))

          (binding [c/*trusted-apply* true
                    c/*bypass-wal*    true]
            (if/transact-kv lmdb c/kv-info
                            [[:put c/last-indexed-wal-tx-id 0]]
                            :data :data))

          (is (= {:indexed-wal-tx-id   partial-id
                  :committed-wal-tx-id committed-id
                  :drained?            false}
                 (wal/flush-kv-indexer! lmdb partial-id)))
          (is (= {:indexed-wal-tx-id   committed-id
                  :committed-wal-tx-id committed-id
                  :drained?            true}
                 (wal/flush-kv-indexer! lmdb)))
          (is (= {:last-committed-wal-tx-id  committed-id
                  :last-indexed-wal-tx-id    committed-id
                  :last-committed-user-tx-id committed-id}
                 (wal/kv-wal-watermarks lmdb)))))

      (is (= {:last-committed-wal-tx-id  0
              :last-indexed-wal-tx-id    0
              :last-committed-user-tx-id 0}
             (wal/kv-wal-watermarks lmdb2)))
      (is (= {:indexed-wal-tx-id   0
              :committed-wal-tx-id 0
              :drained?            true}
             (wal/flush-kv-indexer! lmdb2)))
      (finally
        (if/close-kv lmdb)
        (if/close-kv lmdb2)
        (u/delete-files dir)
        (u/delete-files dir2)))))

(deftest kv-wal-metrics-test
  (let [dir  (u/tmp-dir (str "kv-wal-metrics-" (UUID/randomUUID)))
        lmdb (l/open-kv dir {:flags   (conj c/default-env-flags :nosync)
                             :kv-wal? true})]
    (try
      (if/open-dbi lmdb "a")
      (if/transact-kv lmdb [[:put "a" 1 "x"]])
      (if/transact-kv lmdb [[:put "a" 2 "y"]])
      (let [metrics-before (wal/kv-wal-metrics lmdb)]
        (is (pos? (:indexer-lag metrics-before)))
        (is (pos? (:overlay-entries metrics-before)))
        (is (>= (:segment-count metrics-before) 1))
        (wal/flush-kv-indexer! lmdb)
        (let [metrics-after (wal/kv-wal-metrics lmdb)]
          (is (<= (:indexer-lag metrics-after) (:indexer-lag metrics-before)))
          (is (<= (:overlay-entries metrics-after) (:overlay-entries metrics-before)))
          (is (>= (:segment-count metrics-after) 1))))
      (finally
        (if/close-kv lmdb)
        (u/delete-files dir)))))

(deftest kv-wal-admin-test
  (let [dir  (u/tmp-dir (str "datalevin-kv-wal-admin-test-" (UUID/randomUUID)))
        lmdb (l/open-kv dir {:flags   (conj c/default-env-flags :nosync)
                             :kv-wal? true})]
    (try
      (if/open-dbi lmdb "misc")
      (let [base-id ^long (:last-committed-wal-tx-id
                           (if/kv-wal-watermarks lmdb))]
        (if/transact-kv lmdb [[:put "misc" 1 "x"]])
        (if/transact-kv lmdb [[:put "misc" 2 "y"]])
        (let [committed-id ^long (+ ^long base-id 2)
              partial-id   (inc ^long base-id)]
          (is (= {:last-committed-wal-tx-id  committed-id
                  :last-indexed-wal-tx-id    0
                  :last-committed-user-tx-id committed-id}
                 (if/kv-wal-watermarks lmdb)))

          (binding [c/*trusted-apply* true
                    c/*bypass-wal*    true]
            (if/transact-kv lmdb c/kv-info
                            [[:put c/last-indexed-wal-tx-id 0]]
                            :data :data))

          (is (= {:indexed-wal-tx-id   partial-id
                  :committed-wal-tx-id committed-id
                  :drained?            false}
                 (wal/flush-kv-indexer! lmdb partial-id)))
          (is (= {:indexed-wal-tx-id   committed-id
                  :committed-wal-tx-id committed-id
                  :drained?            true}
                 (wal/flush-kv-indexer! lmdb)))))
      (finally
        (if/close-kv lmdb)
        (u/delete-files dir)))))

;; ---------------------------------------------------------------------------
;; Concurrent read/write tests
;; ---------------------------------------------------------------------------

(deftest kv-wal-concurrent-read-during-writes-test
  (testing "Readers see a consistent snapshot while writers commit"
    (let [dir    (u/tmp-dir (str "kv-wal-conc-rw-" (UUID/randomUUID)))
          lmdb   (l/open-kv dir {:flags   (conj c/default-env-flags :nosync)
                                 :kv-wal? true})
          n-txs  100
          errors (atom [])
          stop?  (atom false)]
      (try
        (if/open-dbi lmdb "a")
        ;; Reader thread: continuously reads all keys and checks that
        ;; every key that exists has the value matching its key number.
        ;; A partially-visible transaction would show a key with the
        ;; wrong value or show key N without key N-1 (monotonicity).
        (let [reader
              (future
                (try
                  (loop [reads 0]
                    (when-not @stop?
                      (let [pairs (vec (if/get-range lmdb "a" [:all]
                                                     :long :long))]
                        ;; Every [k v] must satisfy v = k (our write pattern)
                        (doseq [[k v] pairs]
                          (when (not= k v)
                            (swap! errors conj
                                   {:type     :value-mismatch :k k :v v
                                    :read-num reads})))
                        ;; Keys must be a contiguous prefix 0..max
                        (when (seq pairs)
                          (let [ks (mapv first pairs)]
                            (when (not= ks (vec (range (count ks))))
                              (swap! errors conj
                                     {:type     :non-contiguous :keys ks
                                      :read-num reads})))))
                      (recur (inc reads))))
                  (catch Exception e
                    (swap! errors conj {:type :reader-exception
                                        :msg  (.getMessage e)}))))]
          ;; Writer: commit keys 0..n-txs-1, one per transaction
          (dotimes [i n-txs]
            (if/transact-kv lmdb [[:put "a" (long i) (long i) :long :long]]))
          (reset! stop? true)
          @reader
          ;; Final verification: all keys present
          (is (= (vec (map (fn [i] [i i]) (range n-txs)))
                 (vec (if/get-range lmdb "a" [:all] :long :long))))
          (is (empty? @errors)
              (str "Concurrent reader saw inconsistency: "
                   (first @errors))))
        (finally
          (if/close-kv lmdb)
          (u/delete-files dir))))))

(deftest kv-wal-concurrent-read-during-flush-test
  (testing "Reads remain consistent while flush+prune runs concurrently"
    (let [dir    (u/tmp-dir (str "kv-wal-conc-flush-" (UUID/randomUUID)))
          lmdb   (l/open-kv dir {:flags   (conj c/default-env-flags :nosync)
                                 :kv-wal? true})
          n-txs  50
          errors (atom [])
          stop?  (atom false)]
      (try
        (if/open-dbi lmdb "a")
        ;; Seed data into overlay
        (dotimes [i n-txs]
          (if/transact-kv lmdb [[:put "a" (long i) (long i) :long :long]]))
        ;; Reader: continuously verify all n-txs keys are visible
        (let [reader
              (future
                (try
                  (loop [reads 0]
                    (when-not @stop?
                      (let [cnt (if/range-count lmdb "a" [:all] :long)]
                        ;; Count must never drop below n-txs once all
                        ;; writes are committed (overlay or base).
                        (when (< ^long cnt n-txs)
                          (swap! errors conj
                                 {:type     :missing-keys :count    cnt
                                  :expected n-txs         :read-num reads})))
                      (recur (inc reads))))
                  (catch Exception e
                    (swap! errors conj {:type :reader-exception
                                        :msg  (.getMessage e)}))))]
          ;; Flush in the foreground — moves data from overlay to base
          (if/flush-kv-indexer! lmdb)
          ;; Write more data post-flush (goes into fresh overlay)
          (dotimes [i 20]
            (if/transact-kv lmdb [[:put "a" (long (+ n-txs i))
                                   (long (+ n-txs i)) :long :long]]))
          (reset! stop? true)
          @reader
          (is (= (+ n-txs 20)
                 (if/range-count lmdb "a" [:all] :long)))
          (is (empty? @errors)
              (str "Reader saw inconsistency during flush: "
                   (first @errors))))
        (finally
          (if/close-kv lmdb)
          (u/delete-files dir))))))

(deftest kv-wal-concurrent-multithread-writers-readers-test
  (testing "Multiple reader threads with sequential writers"
    (let [dir       (u/tmp-dir (str "kv-wal-conc-mt-" (UUID/randomUUID)))
          lmdb      (l/open-kv dir {:flags   (conj c/default-env-flags :nosync)
                                    :kv-wal? true})
          n-txs     80
          n-readers 4
          errors    (atom [])
          stop?     (atom false)]
      (try
        (if/open-dbi lmdb "a")
        ;; Launch multiple reader threads
        (let [readers
              (vec
                (for [rid (range n-readers)]
                  (future
                    (try
                      (loop []
                        (when-not @stop?
                          ;; Point reads: if a key exists, its value must match
                          (let [k (long (rand-int n-txs))]
                            (when-let [v (if/get-value lmdb "a" k :long :long)]
                              (when (not= k v)
                                (swap! errors conj
                                       {:type   :point-read-mismatch
                                        :reader rid :k k :v v}))))
                          ;; Range read: values must match keys
                          (doseq [[k v] (if/get-range lmdb "a" [:all]
                                                      :long :long)]
                            (when (not= k v)
                              (swap! errors conj
                                     {:type   :range-mismatch
                                      :reader rid :k k :v v})))
                          (recur)))
                      (catch Exception e
                        (swap! errors conj
                               {:type :reader-exception :reader rid
                                :msg  (.getMessage e)}))))))]
          ;; Writer: sequential transactions
          (dotimes [i n-txs]
            (if/transact-kv lmdb [[:put "a" (long i) (long i) :long :long]]))
          (reset! stop? true)
          (doseq [r readers] @r)
          (is (= (vec (map (fn [i] [i i]) (range n-txs)))
                 (vec (if/get-range lmdb "a" [:all] :long :long))))
          (is (empty? @errors)
              (str "Multi-reader saw inconsistency: "
                   (first @errors))))
        (finally
          (if/close-kv lmdb)
          (u/delete-files dir))))))

(deftest kv-wal-concurrent-list-read-during-writes-test
  (testing "Dupsort overlay reads consistent during concurrent writes"
    (let [dir    (u/tmp-dir (str "kv-wal-conc-list-" (UUID/randomUUID)))
          lmdb   (l/open-kv dir {:flags   (conj c/default-env-flags :nosync)
                                 :kv-wal? true})
          n-txs  50
          errors (atom [])
          stop?  (atom false)]
      (try
        (if/open-list-dbi lmdb "l")
        ;; Reader: continuously reads the list for key "k" and verifies
        ;; that the returned values are a sorted subset of 0..n-txs-1
        (let [reader
              (future
                (try
                  (loop [reads 0]
                    (when-not @stop?
                      (let [vals (into [] (if/get-list lmdb "l" "k"
                                                       :string :long))
                            cnt  (count vals)]
                        ;; Values must be monotonically non-decreasing
                        (when (> cnt 1)
                          (when-not (every? (fn [[a b]] (<= ^long a ^long b))
                                            (partition 2 1 vals))
                            (swap! errors conj
                                   {:type     :unsorted :vals vals
                                    :read-num reads})))
                        ;; Each value must be in range [0, n-txs)
                        (doseq [v vals]
                          (when-not (and (>= ^long v 0) (< ^long v n-txs))
                            (swap! errors conj
                                   {:type     :out-of-range :v v
                                    :read-num reads}))))
                      (recur (inc reads))))
                  (catch Exception e
                    (swap! errors conj {:type :reader-exception
                                        :msg  (.getMessage e)}))))]
          ;; Writer: add values one at a time to the list
          (dotimes [i n-txs]
            (if/transact-kv lmdb [[:put-list "l" "k" [(long i)]
                                   :string :long]]))
          (reset! stop? true)
          @reader
          ;; Final: all values present
          (is (= (vec (range n-txs))
                 (vec (if/get-list lmdb "l" "k" :string :long))))
          (is (empty? @errors)
              (str "List reader saw inconsistency: "
                   (first @errors))))
        (finally
          (if/close-kv lmdb)
          (u/delete-files dir))))))

;; ---------------------------------------------------------------------------
;; Generative / fuzz tests
;; ---------------------------------------------------------------------------

(def ^:private gen-typed-kv
  "Generator for [key value key-type value-type] tuples covering several
   WAL-serialisable types.  Each variant produces values that round-trip
   cleanly through both LMDB and the WAL codec."
  (gen/one-of
    [(gen/tuple gen/large-integer gen/large-integer
                (gen/return :long) (gen/return :long))
     (gen/tuple (gen/not-empty gen/string-alphanumeric)
                (gen/not-empty gen/string-alphanumeric)
                (gen/return :string) (gen/return :string))
     (gen/tuple gen/keyword-ns gen/large-integer
                (gen/return :keyword) (gen/return :long))
     (gen/tuple gen/large-integer gen/keyword-ns
                (gen/return :long) (gen/return :keyword))
     (gen/tuple gen/large-integer
                (gen/fmap double gen/large-integer)
                (gen/return :long) (gen/return :double))]))

(def ^:private gen-put-op
  "Generate a single :put op map."
  (gen/fmap (fn [[k v kt vt]]
              {:op :put :dbi "a" :k k :v v :kt kt :vt vt})
            gen-typed-kv))

(def ^:private gen-tx
  "Generate a transaction: a non-empty vector of :put ops."
  (gen/not-empty (gen/vector gen-put-op 1 6)))

(def ^:private gen-tx-sequence
  "Generate a sequence of 1-5 transactions."
  (gen/not-empty (gen/vector gen-tx 1 5)))

(defn- ops->kv-entries
  "Convert an op map to the vector form that transact-kv expects."
  [{:keys [op dbi k v kt vt]}]
  (case op
    :put [:put dbi k v kt vt]
    :del [:del dbi k kt]))

(defn- expected-state
  "Compute the expected key->value map from a sequence of transactions.
   Last-write-wins per [dbi kt k] triple."
  [txs]
  (reduce (fn [acc tx]
            (reduce (fn [m {:keys [op dbi k v kt vt]}]
                      (case op
                        :put (assoc m [dbi kt k] {:v v :vt vt})
                        :del (dissoc m [dbi kt k])))
                    acc tx))
          {} txs))

(test/defspec kv-wal-fuzz-write-read-test
  100
  (prop/for-all
    [txs gen-tx-sequence]
    (let [dir  (u/tmp-dir (str "kv-wal-fuzz-" (UUID/randomUUID)))
          lmdb (l/open-kv dir {:flags   (conj c/default-env-flags :nosync)
                               :kv-wal? true})]
      (try
        (if/open-dbi lmdb "a")
        (let [base-id  ^long (last-wal-id lmdb)
              _        (doseq [tx txs]
                         (if/transact-kv lmdb (mapv ops->kv-entries tx)))
              final-id ^long (last-wal-id lmdb)
              expected (expected-state txs)
              n-txs    (count txs)]

          ;; 1. WAL id advanced by exactly the number of transactions
          (is (= (+ base-id n-txs) final-id)
              "WAL tx-id should advance by number of transactions")

          ;; 2. Every expected value is readable from the KV store
          (let [vals-ok
                (every? (fn [[[dbi kt k] {:keys [v vt]}]]
                          (= v (if/get-value lmdb dbi k kt vt)))
                        expected)]
            (is vals-ok "All expected values should be readable"))

          ;; 3. WAL records round-trip: correct number of records and total ops
          (let [records   (vec (wal/read-wal-records dir base-id final-id))
                total-ops (reduce + (map (comp count :wal/ops) records))
                input-ops (reduce + (map count txs))]
            (is (= n-txs (count records))
                "WAL should contain one record per transaction")
            (is (= input-ops total-ops)
                "Total WAL ops should equal total input ops")
            (is (= (set (range (inc base-id) (inc final-id)))
                   (set (map :wal/tx-id records)))
                "WAL tx-ids should be contiguous"))

          ;; 4. Replay: reset indexed marker, replay WAL, verify values survive
          (binding [c/*trusted-apply* true
                    c/*bypass-wal*    true]
            (if/transact-kv lmdb c/kv-info
                            [[:put c/last-indexed-wal-tx-id 0]
                             [:put c/applied-wal-tx-id 0]]
                            :data :data))
          (let [replay-res (wal/replay-kv-wal! lmdb)]
            (is (= final-id (:applied replay-res))
                "Replay should apply up to the committed WAL id"))
          (let [vals-after-replay
                (every? (fn [[[dbi kt k] {:keys [v vt]}]]
                          (= v (if/get-value lmdb dbi k kt vt)))
                        expected)]
            (is vals-after-replay
                "All values should survive WAL replay")))
        (finally
          (if/close-kv lmdb)
          (u/delete-files dir))))))

(test/defspec kv-wal-fuzz-put-del-test
  100
  (prop/for-all
    [puts (gen/not-empty
            (gen/vector
              (gen/tuple gen/large-integer gen/string-alphanumeric)
              1 10))]
    (let [dir  (u/tmp-dir (str "kv-wal-fuzz-pd-" (UUID/randomUUID)))
          lmdb (l/open-kv dir {:flags   (conj c/default-env-flags :nosync)
                               :kv-wal? true})]
      (try
        (if/open-dbi lmdb "a")
        (let [base-id  (last-wal-id lmdb)
              ;; Deduplicate: last-write-wins per key
              put-map  (reduce (fn [m [k v]] (assoc m k v)) {} puts)
              ;; Transaction 1: put all keys
              _        (if/transact-kv lmdb
                         (mapv (fn [[k v]] [:put "a" k v :long :string]) puts))
              ;; Pick roughly half the unique keys to delete
              all-keys (keys put-map)
              to-del   (take (max 1 (quot (count all-keys) 2)) all-keys)
              _        (if/transact-kv lmdb
                         (mapv (fn [k] [:del "a" k :long]) to-del))
              del-set  (set to-del)
              alive    (remove (fn [[k _]] (del-set k)) put-map)
              final-id (last-wal-id lmdb)]

          ;; Deleted keys should be gone
          (is (every? (fn [k]
                        (nil? (if/get-value lmdb "a" k :long :string)))
                      to-del)
              "Deleted keys should return nil")

          ;; Surviving keys should still be readable
          (is (every? (fn [[k v]]
                        (= v (if/get-value lmdb "a" k :long :string)))
                      alive)
              "Non-deleted keys should be readable")

          ;; WAL should have exactly 2 records (one put-batch, one del-batch)
          (let [records (vec (wal/read-wal-records dir base-id final-id))]
            (is (= 2 (count records))
                "Two WAL records: puts then dels")
            (is (= (count puts)
                   (count (:wal/ops (first records))))
                "Put record should have one op per input put")
            (is (= (count to-del)
                   (count (:wal/ops (second records))))
                "Del record should have one op per deleted key")
            ;; Verify op types in WAL records
            (is (every? #(= :put (:op %)) (:wal/ops (first records))))
            (is (every? #(= :del (:op %)) (:wal/ops (second records))))))
        (finally
          (if/close-kv lmdb)
          (u/delete-files dir))))))

;; ---------------------------------------------------------------------------
;; Fuzz tests for wrapped KV iterators (overlay merge)
;; ---------------------------------------------------------------------------

(def ^:private gen-small-long
  "Small longs (0–30) for key/value generation.  Kept small to maximize
   collision probability between base and overlay."
  (gen/choose 0 30))

(def ^:private gen-large-long
  "Large longs (0–1000) to exercise broader key domains."
  (gen/choose 0 1000))

;; --- Non-dupsort helpers ---

(def ^:private gen-key-iter-op
  (gen/one-of
    [(gen/fmap (fn [[k v]] {:op :put :k k :v v})
               (gen/tuple gen-small-long gen-small-long))
     (gen/fmap (fn [k] {:op :del :k k})
               gen-small-long)]))

(def ^:private gen-key-iter-ops
  (gen/not-empty (gen/vector gen-key-iter-op 1 30)))

(def ^:private gen-key-iter-op-large
  (gen/one-of
    [(gen/fmap (fn [[k v]] {:op :put :k k :v v})
               (gen/tuple gen-large-long gen-large-long))
     (gen/fmap (fn [k] {:op :del :k k})
               gen-large-long)]))

(def ^:private gen-key-iter-ops-large
  (gen/not-empty (gen/vector gen-key-iter-op-large 20 200)))

(defn- apply-key-iter-ops
  "Apply put/del ops to a sorted-map reference model (key → value)."
  [m ops]
  (reduce (fn [acc {:keys [op k v]}]
            (case op
              :put (assoc acc k v)
              :del (dissoc acc k)))
          m ops))

(defn- kv-ops->txn [dbi ops]
  (mapv (fn [{:keys [op k v]}]
          (case op
            :put [:put dbi k v :long :long]
            :del [:del dbi k :long]))
        ops))

(defn- verify-key-iter
  "Verify key iteration against sorted-map reference."
  [lmdb dbi ref-map]
  ;; get-range :all
  (is (= (vec ref-map)
         (vec (if/get-range lmdb dbi [:all] :long :long)))
      "get-range :all")
  ;; get-range :all-back
  (is (= (if (empty? ref-map) [] (vec (rseq ref-map)))
         (vec (if/get-range lmdb dbi [:all-back] :long :long)))
      "get-range :all-back")
  ;; key-range
  (is (= (vec (keys ref-map))
         (vec (if/key-range lmdb dbi [:all] :long)))
      "key-range :all")
  ;; range-count
  (is (= (count ref-map)
         (if/range-count lmdb dbi [:all] :long))
      "range-count :all")
  ;; visit-key-range
  (let [visited (volatile! [])]
    (if/visit-key-range lmdb dbi (fn [k] (vswap! visited conj k))
                        [:all] :long false)
    (is (= (vec (keys ref-map)) @visited) "visit-key-range :all"))
  ;; visit (key+value)
  (let [visited (volatile! [])]
    (if/visit lmdb dbi (fn [k v] (vswap! visited conj [k v]))
              [:all] :long :long false)
    (is (= (vec ref-map) @visited) "visit :all"))
  ;; Parametric :closed range
  (when (>= (count ref-map) 2)
    (let [ks (vec (keys ref-map))
          lo (first ks)
          hi (last ks)]
      (is (= (vec (subseq ref-map >= lo <= hi))
             (vec (if/get-range lmdb dbi [:closed lo hi] :long :long)))
          "get-range :closed")
      (when (> (count ks) 2)
        (let [mid (nth ks (quot (count ks) 2))]
          (is (= (vec (subseq ref-map >= mid))
                 (vec (if/get-range lmdb dbi [:at-least mid] :long :long)))
              "get-range :at-least")
          (is (= (vec (subseq ref-map <= mid))
                 (vec (if/get-range lmdb dbi [:at-most mid] :long :long)))
              "get-range :at-most")
          (is (= (vec (subseq ref-map > mid))
                 (vec (if/get-range lmdb dbi [:greater-than mid] :long :long)))
              "get-range :greater-than"))))))

(test/defspec kv-wal-fuzz-key-iter-test
  50
  (prop/for-all
    [ops1 gen-key-iter-ops
     ops2 gen-key-iter-ops]
    (let [dir  (u/tmp-dir (str "kv-wal-fuzz-ki-" (UUID/randomUUID)))
          lmdb (l/open-kv dir {:flags   (conj c/default-env-flags :nosync)
                               :kv-wal? true})]
      (try
        (if/open-dbi lmdb "a")

        ;; Phase 1: overlay-only (base is empty)
        (if/transact-kv lmdb (kv-ops->txn "a" ops1))
        (let [ref1 (apply-key-iter-ops (sorted-map) ops1)]
          (verify-key-iter lmdb "a" ref1)

          ;; Phase 2: replay to base, prune overlay, add new overlay
          (binding [c/*trusted-apply* true
                    c/*bypass-wal*    true]
            (if/transact-kv lmdb c/kv-info
                            [[:put c/last-indexed-wal-tx-id 0]
                             [:put c/applied-wal-tx-id 0]]
                            :data :data))
          (if/flush-kv-indexer! lmdb)

          (if/transact-kv lmdb (kv-ops->txn "a" ops2))
          (let [ref2 (apply-key-iter-ops ref1 ops2)]
            (verify-key-iter lmdb "a" ref2)))
        true
        (finally
          (if/close-kv lmdb)
          (u/delete-files dir))))))

(test/defspec kv-wal-fuzz-key-iter-large-test
  80
  (prop/for-all
    [ops1 gen-key-iter-ops-large
     ops2 gen-key-iter-ops-large
     ops3 gen-key-iter-ops-large]
    (let [dir  (u/tmp-dir (str "kv-wal-fuzz-ki-large-" (UUID/randomUUID)))
          lmdb (l/open-kv dir {:flags   (conj c/default-env-flags :nosync)
                               :kv-wal? true})]
      (try
        (if/open-dbi lmdb "a")
        (if/transact-kv lmdb (kv-ops->txn "a" ops1))
        (let [ref1 (apply-key-iter-ops (sorted-map) ops1)]
          (verify-key-iter lmdb "a" ref1)
          (binding [c/*trusted-apply* true
                    c/*bypass-wal*    true]
            (if/transact-kv lmdb c/kv-info
                            [[:put c/last-indexed-wal-tx-id 0]
                             [:put c/applied-wal-tx-id 0]]
                            :data :data))
          (if/flush-kv-indexer! lmdb)
          (if/transact-kv lmdb (kv-ops->txn "a" ops2))
          (let [ref2 (apply-key-iter-ops ref1 ops2)]
            (verify-key-iter lmdb "a" ref2)
            (if/transact-kv lmdb (kv-ops->txn "a" ops3))
            (let [ref3 (apply-key-iter-ops ref2 ops3)]
              (verify-key-iter lmdb "a" ref3))))
        true
        (finally
          (if/close-kv lmdb)
          (u/delete-files dir))))))

;; --- Dupsort helpers ---

(def ^:private gen-list-iter-op
  (gen/frequency
    [[5 (gen/fmap (fn [[k vs]]
                    {:op :put-list :k k :vs (vec (distinct vs))})
                  (gen/tuple gen-small-long
                             (gen/not-empty (gen/vector gen-small-long 1 5))))]
     [3 (gen/fmap (fn [[k vs]]
                    {:op :del-list :k k :vs (vec (distinct vs))})
                  (gen/tuple gen-small-long
                             (gen/not-empty (gen/vector gen-small-long 1 3))))]
     [2 (gen/fmap (fn [k] {:op :wipe :k k})
                  gen-small-long)]]))

(def ^:private gen-list-iter-ops
  (gen/not-empty (gen/vector gen-list-iter-op 1 20)))

(def ^:private gen-list-iter-op-large
  (gen/frequency
    [[5 (gen/fmap (fn [[k vs]]
                    {:op :put-list :k k :vs (vec (distinct vs))})
                  (gen/tuple gen-large-long
                             (gen/not-empty (gen/vector gen-large-long 1 10))))]
     [3 (gen/fmap (fn [[k vs]]
                    {:op :del-list :k k :vs (vec (distinct vs))})
                  (gen/tuple gen-large-long
                             (gen/not-empty (gen/vector gen-large-long 1 6))))]
     [2 (gen/fmap (fn [k] {:op :wipe :k k})
                  gen-large-long)]]))

(def ^:private gen-list-iter-ops-large
  (gen/not-empty (gen/vector gen-list-iter-op-large 15 80)))

(defn- apply-list-iter-ops
  "Apply list ops to a sorted-map of key → sorted-set-of-values."
  [m ops]
  (reduce (fn [acc {:keys [op k vs]}]
            (case op
              :put-list (update acc k
                                (fn [s] (into (or s (sorted-set)) vs)))
              :del-list (if-let [s (get acc k)]
                          (let [new-s (reduce disj s vs)]
                            (if (empty? new-s) (dissoc acc k)
                                (assoc acc k new-s)))
                          acc)
              :wipe     (dissoc acc k)))
          m ops))

(defn- flatten-list-ref
  "Flatten sorted-map {k → sorted-set} to [[k v] …] pairs."
  ([m] (flatten-list-ref m false false))
  ([m key-back? val-back?]
   (vec (let [key-seq (if key-back?
                        (when (seq m) (rseq m))
                        (seq m))]
          (for [[k vs] key-seq
                v      (if val-back? (rseq vs) (seq vs))]
            [k v])))))

(defn- list-ops->txn [dbi ops]
  (mapv (fn [{:keys [op k vs]}]
          (case op
            :put-list [:put-list dbi k vs :long :long]
            :del-list [:del-list dbi k vs :long :long]
            :wipe     [:del dbi k :long]))
        ops))

(defn- verify-list-iter
  "Verify list iteration (wrap-list-iterable) against reference model."
  [lmdb dbi ref-map]
  ;; list-range [:all] [:all]
  (is (= (flatten-list-ref ref-map)
         (vec (if/list-range lmdb dbi [:all] :long [:all] :long)))
      "list-range :all :all")
  ;; list-range [:all-back] [:all]
  (is (= (flatten-list-ref ref-map true false)
         (vec (if/list-range lmdb dbi [:all-back] :long [:all] :long)))
      "list-range :all-back :all")
  ;; list-range [:all] [:all-back]
  (is (= (flatten-list-ref ref-map false true)
         (vec (if/list-range lmdb dbi [:all] :long [:all-back] :long)))
      "list-range :all :all-back")
  ;; list-range-count
  (is (= (reduce + 0 (map count (vals ref-map)))
         (if/list-range-count lmdb dbi [:all] :long [:all] :long))
      "list-range-count :all :all")
  ;; visit-list-range
  (let [visited (volatile! [])]
    (if/visit-list-range lmdb dbi (fn [k v] (vswap! visited conj [k v]))
                         [:all] :long [:all] :long false)
    (is (= (flatten-list-ref ref-map) @visited)
        "visit-list-range :all :all"))
  ;; Parametric value sub-range for a key that exists
  (when (seq ref-map)
    (let [[k vs]   (first ref-map)
          v-lo     (first vs)
          v-hi     (last vs)
          expected (vec (for [v (subseq vs >= v-lo <= v-hi)] [k v]))]
      (is (= expected
             (vec (if/list-range lmdb dbi [:closed k k] :long
                                 [:closed v-lo v-hi] :long)))
          "list-range :closed key :closed val"))
    ;; Single-bound value ranges for a key with >= 3 values
    (when-let [[k vs] (first (filter (fn [[_ s]] (>= (count s) 3)) ref-map))]
      (let [mid (nth (vec vs) (quot (count vs) 2))]
        (is (= (vec (for [v (subseq vs >= mid)] [k v]))
               (vec (if/list-range lmdb dbi [:closed k k] :long
                                   [:at-least mid] :long)))
            "list-range :at-least val")
        (is (= (vec (for [v (subseq vs <= mid)] [k v]))
               (vec (if/list-range lmdb dbi [:closed k k] :long
                                   [:at-most mid] :long)))
            "list-range :at-most val")
        (is (= (vec (for [v (subseq vs > mid)] [k v]))
               (vec (if/list-range lmdb dbi [:closed k k] :long
                                   [:greater-than mid] :long)))
            "list-range :greater-than val")
        (is (= (vec (for [v (subseq vs < mid)] [k v]))
               (vec (if/list-range lmdb dbi [:closed k k] :long
                                   [:less-than mid] :long)))
            "list-range :less-than val")
        ;; Back variants
        (is (= (vec (for [v (reverse (subseq vs >= mid))] [k v]))
               (vec (if/list-range lmdb dbi [:closed k k] :long
                                   [:at-least-back mid] :long)))
            "list-range :at-least-back val")
        (is (= (vec (for [v (reverse (subseq vs <= mid))] [k v]))
               (vec (if/list-range lmdb dbi [:closed k k] :long
                                   [:at-most-back mid] :long)))
            "list-range :at-most-back val")
        (is (= (vec (for [v (reverse (subseq vs > mid))] [k v]))
               (vec (if/list-range lmdb dbi [:closed k k] :long
                                   [:greater-than-back mid] :long)))
            "list-range :greater-than-back val")
        (is (= (vec (for [v (reverse (subseq vs < mid))] [k v]))
               (vec (if/list-range lmdb dbi [:closed k k] :long
                                   [:less-than-back mid] :long)))
            "list-range :less-than-back val")))))

(defn- verify-list-key-range-iter
  "Verify list-key-range iteration (wrap-list-key-range-full-val-iterable)."
  [lmdb dbi ref-map]
  ;; visit-list-key-range [:all]
  (let [visited (volatile! [])]
    (if/visit-list-key-range lmdb dbi (fn [k v] (vswap! visited conj [k v]))
                             [:all] :long :long false)
    (is (= (flatten-list-ref ref-map) @visited)
        "visit-list-key-range :all"))
  ;; visit-list-key-range [:all-back] — underlying iterator is forward-only
  (let [visited (volatile! [])]
    (if/visit-list-key-range lmdb dbi (fn [k v] (vswap! visited conj [k v]))
                             [:all-back] :long :long false)
    (is (= (flatten-list-ref ref-map) @visited)
        "visit-list-key-range :all-back"))
  ;; Parametric :closed key range
  (when (>= (count ref-map) 2)
    (let [lo      (first (keys ref-map))
          hi      (last (keys ref-map))
          sub     (into (sorted-map) (subseq ref-map >= lo <= hi))
          visited (volatile! [])]
      (if/visit-list-key-range lmdb dbi (fn [k v] (vswap! visited conj [k v]))
                               [:closed lo hi] :long :long false)
      (is (= (flatten-list-ref sub) @visited)
          "visit-list-key-range :closed"))))

(test/defspec kv-wal-fuzz-list-iter-test
  50
  (prop/for-all
    [ops1 gen-list-iter-ops
     ops2 gen-list-iter-ops]
    (let [dir  (u/tmp-dir (str "kv-wal-fuzz-li-" (UUID/randomUUID)))
          lmdb (l/open-kv dir {:flags   (conj c/default-env-flags :nosync)
                               :kv-wal? true})]
      (try
        (if/open-list-dbi lmdb "l")

        ;; Phase 1: overlay-only
        (if/transact-kv lmdb (list-ops->txn "l" ops1))
        (let [ref1 (apply-list-iter-ops (sorted-map) ops1)]
          (verify-list-iter lmdb "l" ref1)
          (verify-list-key-range-iter lmdb "l" ref1)

          ;; Phase 2: replay to base, prune overlay, add new overlay
          (binding [c/*trusted-apply* true
                    c/*bypass-wal*    true]
            (if/transact-kv lmdb c/kv-info
                            [[:put c/last-indexed-wal-tx-id 0]
                             [:put c/applied-wal-tx-id 0]]
                            :data :data))
          (if/flush-kv-indexer! lmdb)

          (if/transact-kv lmdb (list-ops->txn "l" ops2))
          (let [ref2 (apply-list-iter-ops ref1 ops2)]
            (verify-list-iter lmdb "l" ref2)
            (verify-list-key-range-iter lmdb "l" ref2)))
        true
        (finally
          (if/close-kv lmdb)
          (u/delete-files dir))))))

(test/defspec kv-wal-fuzz-list-iter-large-test
  80
  (prop/for-all
    [ops1 gen-list-iter-ops-large
     ops2 gen-list-iter-ops-large
     ops3 gen-list-iter-ops-large]
    (let [dir  (u/tmp-dir (str "kv-wal-fuzz-li-large-" (UUID/randomUUID)))
          lmdb (l/open-kv dir {:flags   (conj c/default-env-flags :nosync)
                               :kv-wal? true})]
      (try
        (if/open-list-dbi lmdb "l")
        (if/transact-kv lmdb (list-ops->txn "l" ops1))
        (let [ref1 (apply-list-iter-ops (sorted-map) ops1)]
          (verify-list-iter lmdb "l" ref1)
          (verify-list-key-range-iter lmdb "l" ref1)
          (binding [c/*trusted-apply* true
                    c/*bypass-wal*    true]
            (if/transact-kv lmdb c/kv-info
                            [[:put c/last-indexed-wal-tx-id 0]
                             [:put c/applied-wal-tx-id 0]]
                            :data :data))
          (if/flush-kv-indexer! lmdb)
          (if/transact-kv lmdb (list-ops->txn "l" ops2))
          (let [ref2 (apply-list-iter-ops ref1 ops2)]
            (verify-list-iter lmdb "l" ref2)
            (verify-list-key-range-iter lmdb "l" ref2)
            (if/transact-kv lmdb (list-ops->txn "l" ops3))
            (let [ref3 (apply-list-iter-ops ref2 ops3)]
              (verify-list-iter lmdb "l" ref3)
              (verify-list-key-range-iter lmdb "l" ref3))))
        (finally
          (if/close-kv lmdb)
          (u/delete-files dir))))))

;; ---------------------------------------------------------------------------
;; TX-Log API tests
;; ---------------------------------------------------------------------------

(deftest test-open-tx-log-basic
  (let [dir  (u/tmp-dir (str "kv-wal-txlog-basic-" (UUID/randomUUID)))
        lmdb (l/open-kv dir {:flags   (conj c/default-env-flags :nosync)
                             :kv-wal? true})]
    (try
      (if/open-dbi lmdb "a")
      (let [base-id ^long (last-wal-id lmdb)]
        (if/transact-kv lmdb [[:put "a" 1 "x"]])
        (if/transact-kv lmdb [[:put "a" 2 "y"]])
        (if/transact-kv lmdb [[:put "a" 3 "z"]])
        (let [records (vec (if/open-tx-log lmdb base-id))]
          (is (= 3 (count records)))
          (is (every? #(contains? % :wal/tx-id) records))
          (is (every? #(contains? % :wal/ops) records))
          (is (= (set (range (inc base-id) (+ base-id 4)))
                 (set (map :wal/tx-id records))))))
      (finally
        (if/close-kv lmdb)
        (u/delete-files dir)))))

(deftest test-open-tx-log-range
  (let [dir  (u/tmp-dir (str "kv-wal-txlog-range-" (UUID/randomUUID)))
        lmdb (l/open-kv dir {:flags   (conj c/default-env-flags :nosync)
                             :kv-wal? true})]
    (try
      (if/open-dbi lmdb "a")
      (let [base-id ^long (last-wal-id lmdb)]
        (if/transact-kv lmdb [[:put "a" 1 "x"]])
        (if/transact-kv lmdb [[:put "a" 2 "y"]])
        (if/transact-kv lmdb [[:put "a" 3 "z"]])
        ;; Read only the first two records
        (let [records (vec (if/open-tx-log lmdb base-id (+ base-id 2)))]
          (is (= 2 (count records)))
          (is (= #{(+ base-id 1) (+ base-id 2)}
                 (set (map :wal/tx-id records)))))
        ;; Read from middle
        (let [records (vec (if/open-tx-log lmdb (+ base-id 1)))]
          (is (= 2 (count records)))
          (is (= #{(+ base-id 2) (+ base-id 3)}
                 (set (map :wal/tx-id records))))))
      (finally
        (if/close-kv lmdb)
        (u/delete-files dir)))))

(deftest test-open-tx-log-empty
  (let [dir  (u/tmp-dir (str "kv-wal-txlog-empty-" (UUID/randomUUID)))
        lmdb (l/open-kv dir {:flags   (conj c/default-env-flags :nosync)
                             :kv-wal? true})]
    (try
      (if/open-dbi lmdb "a")
      (if/transact-kv lmdb [[:put "a" 1 "x"]])
      (let [committed (last-wal-id lmdb)
            records   (vec (if/open-tx-log lmdb committed))]
        (is (empty? records)))
      (finally
        (if/close-kv lmdb)
        (u/delete-files dir)))))

(deftest test-open-tx-log-wal-disabled
  (let [dir  (u/tmp-dir (str "kv-wal-txlog-disabled-" (UUID/randomUUID)))
        lmdb (l/open-kv dir {:flags   (conj c/default-env-flags :nosync)
                             :kv-wal? false})]
    (try
      (if/open-dbi lmdb "a")
      (if/transact-kv lmdb [[:put "a" 1 "x"]])
      (let [records (vec (if/open-tx-log lmdb 0))]
        (is (empty? records)))
      (finally
        (if/close-kv lmdb)
        (u/delete-files dir)))))

(deftest test-gc-wal-segments-basic
  (let [dir  (u/tmp-dir (str "kv-wal-gc-basic-" (UUID/randomUUID)))
        lmdb (l/open-kv dir {:flags                 (conj c/default-env-flags :nosync)
                             :kv-wal?               true
                             :wal-segment-max-bytes 1
                             :wal-retention-bytes   0
                             :wal-retention-ms      0})]
    (try
      (if/open-dbi lmdb "a")
      ;; Write enough txs to force segment rotation (max 1 byte per segment)
      (dotimes [i 5]
        (if/transact-kv lmdb [[:put "a" i (str "v" i)]]))
      ;; Should have multiple segment files
      (let [files-before (count (wal/segment-files dir))]
        (is (> files-before 1))
        ;; Advance indexed watermark via replay
        (binding [c/*trusted-apply* true
                  c/*bypass-wal*    true]
          (if/transact-kv lmdb c/kv-info
                          [[:put c/last-indexed-wal-tx-id 0]
                           [:put c/applied-wal-tx-id 0]]
                          :data :data))
        (if/flush-kv-indexer! lmdb)
        ;; Now GC should delete old segments
        (let [result (if/gc-wal-segments! lmdb)]
          (is (pos? (:deleted result)))
          (is (pos? (:retained result)))
          ;; Active segment should always remain
          (is (>= (count (wal/segment-files dir)) 1))))
      (finally
        (if/close-kv lmdb)
        (u/delete-files dir)))))

(deftest test-gc-wal-segments-delete-failure
  (let [dir     (u/tmp-dir (str "kv-wal-gc-delete-fail-" (UUID/randomUUID)))
        lmdb    (l/open-kv dir {:flags                 (conj c/default-env-flags :nosync)
                                :kv-wal?               true
                                :wal-segment-max-bytes 1
                                :wal-retention-bytes   0
                                :wal-retention-ms      0})
        wal-dir (io/file (wal/wal-dir-path dir))]
    (try
      (if/open-dbi lmdb "a")
      (dotimes [i 5]
        (if/transact-kv lmdb [[:put "a" i (str "v" i)]]))
      (binding [c/*trusted-apply* true
                c/*bypass-wal*    true]
        (if/transact-kv lmdb c/kv-info
                        [[:put c/last-indexed-wal-tx-id 0]
                         [:put c/applied-wal-tx-id 0]]
                        :data :data))
      (if/flush-kv-indexer! lmdb)
      (let [files-before (vec (wal/segment-files dir))]
        (is (> (count files-before) 1))
        (is (.setWritable wal-dir false false))
        (let [result (if/gc-wal-segments! lmdb)]
          (is (zero? (:deleted result)))
          (is (= (count files-before) (:retained result)))
          (is (= (count files-before)
                 (count (wal/segment-files dir))))))
      (finally
        (.setWritable wal-dir true false)
        (if/close-kv lmdb)
        (u/delete-files dir)))))

(deftest test-gc-wal-segments-retain
  (let [dir  (u/tmp-dir (str "kv-wal-gc-retain-" (UUID/randomUUID)))
        lmdb (l/open-kv dir {:flags                 (conj c/default-env-flags :nosync)
                             :kv-wal?               true
                             :wal-segment-max-bytes 1
                             :wal-retention-bytes   0
                             :wal-retention-ms      0})]
    (try
      (if/open-dbi lmdb "a")
      (dotimes [i 5]
        (if/transact-kv lmdb [[:put "a" i (str "v" i)]]))
      ;; Advance indexed watermark
      (binding [c/*trusted-apply* true
                c/*bypass-wal*    true]
        (if/transact-kv lmdb c/kv-info
                        [[:put c/last-indexed-wal-tx-id 0]
                         [:put c/applied-wal-tx-id 0]]
                        :data :data))
      (if/flush-kv-indexer! lmdb)
      ;; retain-wal-id = 0 should prevent GC from deleting anything
      ;; since gc watermark = min(indexed, retain) = 0
      (let [result (if/gc-wal-segments! lmdb 0)]
        (is (zero? (:deleted result))))
      (finally
        (if/close-kv lmdb)
        (u/delete-files dir)))))

(deftest test-gc-wal-segments-retention
  (let [dir  (u/tmp-dir (str "kv-wal-gc-retention-" (UUID/randomUUID)))
        lmdb (l/open-kv dir {:flags                 (conj c/default-env-flags :nosync)
                             :kv-wal?               true
                             :wal-segment-max-bytes 1
                             ;; Large retention — nothing should be GC'd
                             :wal-retention-bytes   (* 1024 1024 1024)
                             :wal-retention-ms      (* 7 24 60 60 1000)})]
    (try
      (if/open-dbi lmdb "a")
      (dotimes [i 5]
        (if/transact-kv lmdb [[:put "a" i (str "v" i)]]))
      (let [files-before (count (wal/segment-files dir))]
        (is (> files-before 1))
        ;; Advance indexed watermark
        (binding [c/*trusted-apply* true
                  c/*bypass-wal*    true]
          (if/transact-kv lmdb c/kv-info
                          [[:put c/last-indexed-wal-tx-id 0]
                           [:put c/applied-wal-tx-id 0]]
                          :data :data))
        (if/flush-kv-indexer! lmdb)
        ;; Even though segments are indexed, retention policy keeps them
        (let [result (if/gc-wal-segments! lmdb)]
          (is (zero? (:deleted result)))
          (is (= files-before (:retained result)))))
      (finally
        (if/close-kv lmdb)
        (u/delete-files dir)))))

(deftest test-gc-wal-segments-active-segment
  (let [dir  (u/tmp-dir (str "kv-wal-gc-active-" (UUID/randomUUID)))
        lmdb (l/open-kv dir {:flags   (conj c/default-env-flags :nosync)
                             :kv-wal? true})]
    (try
      (if/open-dbi lmdb "a")
      (if/transact-kv lmdb [[:put "a" 1 "x"]])
      ;; With only one segment, GC should never delete it
      (binding [c/*trusted-apply* true
                c/*bypass-wal*    true]
        (if/transact-kv lmdb c/kv-info
                        [[:put c/last-indexed-wal-tx-id 0]
                         [:put c/applied-wal-tx-id 0]]
                        :data :data))
      (if/flush-kv-indexer! lmdb)
      (let [result (if/gc-wal-segments! lmdb)]
        (is (zero? (:deleted result)))
        (is (= 1 (:retained result)))
        (is (= 1 (count (wal/segment-files dir)))))
      (finally
        (if/close-kv lmdb)
        (u/delete-files dir)))))
