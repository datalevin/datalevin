;; Copyright (c) Huahai Yang. All rights reserved.
;; Distributed under the Eclipse Public License 2.0.
(ns datalevin.tx-group-batch-rmw-test
  "Contract tests for ordered read-modify-write preparation.

  Preparation is driven on purpose-built sealed batches, so member order, what
  each body may observe, and what one rejection costs the batch are exact rather
  than timing-dependent. Staging mechanics live in
  `datalevin.tx-group-batch-stage-test`; the last test here is the one threaded
  case, which checks that the real executor runs preparation before dispatch."
  (:require [clojure.test :refer [deftest is testing]]
            [datalevin.tx-group.batch :as batch]
            [datalevin.tx-group.batch.charge :as charge]
            [datalevin.tx-group.batch.executor :as executor]
            [datalevin.tx-group.batch.rmw :as rmw]
            [datalevin.tx-group.batch.stage :as stage])
  (:import [java.nio ByteBuffer]
           [java.util.concurrent.atomic AtomicBoolean AtomicLong]
           [org.eclipse.collections.impl.list.mutable FastList]))

(def ^:private overrides
  {:wal-pending-max-requests 64
   :wal-pending-max-bytes 1048576
   :write-batch-size 8
   :write-batch-max-bytes 12288
   :wal-rmw-max-bytes 4096})

(defn- long-bytes ^bytes [v]
  (let [buffer (ByteBuffer/allocate 8)]
    (.putLong buffer (long v))
    (.array buffer)))

(defn- to-long ^long [^bytes bs]
  (if bs (.getLong (ByteBuffer/wrap bs)) -1))

(defn- base-of
  "A native base holding `k` -> `v` pairs, returning nil for anything else.

  A real base charges the reading descriptor before detaching its copy; these
  fixtures use small values and already return a fresh array, so they skip it."
  [pairs]
  (fn [_descriptor _dbi ^bytes key]
    (some (fn [[k v]] (when (java.util.Arrays/equals (long-bytes k) key)
                       (long-bytes v)))
          pairs)))

(defn- row-fn [dbi op key value]
  (if (= op :del)
    [:del dbi key nil :raw]
    [:put dbi key value :raw :raw]))

(defn- fold-row [row]
  [(nth row 1) (nth row 0) (nth row 2) (nth row 3)])

(defn- body-cost ^long [rows]
  (+ 128 (* 16 (count rows))))

(defn- encode-body ^bytes [rows _hooks]
  ;; A stand-in for the WAL encoder: one body per member, with a visible marker
  ;; per row so a test can tell whose rows reached it.
  (let [sb (StringBuilder.)]
    (doseq [row rows]
      (.append sb (str (nth row 0) "@" (to-long (nth row 2)) "=")))
    (.getBytes (.toString sb) "UTF-8")))

(defn- rmw-opts
  ([base] (rmw-opts base nil))
  ([base with-base]
   {:base base
    :with-base with-base
    :row-fn row-fn
    :fold-row fold-row
    :body-cost body-cost
    :encode-body encode-body}))

(defn- describe
  "One member's accepted rows, as `op@key` markers."
  [descriptor]
  (mapv #(str (nth % 0) "@" (to-long (nth % 2))) (:rows (batch/data descriptor))))

(defn- wrote-weight
  "The accepted write weight a preparation plan publishes."
  [plan]
  (long (:weight plan)))

(defn- body-descriptor
  "One sealed member whose ordered body runs during preparation."
  ([op] (body-descriptor op 1048576))
  ([op allowance]
   (batch/->Descriptor op
                       (volatile! nil)
                       nil
                       (long allowance)
                       Long/MAX_VALUE
                       (AtomicLong. charge/request-control-bundle)
                       (AtomicBoolean. true)
                       nil
                       (Thread/currentThread)
                       (AtomicBoolean. false)
                       (AtomicBoolean. false)
                       (AtomicBoolean. false)
                       (AtomicBoolean. false))))

(defn- blind-descriptor
  "One sealed member that prepared its own rows and WAL body.

  Fully prepaid before admission, exactly as a real blind request is: its
  charge starts at its allowance, so any further charge fails."
  [rows]
  (let [allowance (charge/blind-allowance {:declared-bytes 64
                                           :scratch-bytes 4096})]
    (batch/->Descriptor
     nil
     (delay {:rows rows :wal-body (encode-body rows {})})
     nil
     (long allowance)
     Long/MAX_VALUE
     (AtomicLong. (long allowance))
     (AtomicBoolean. true)
     nil
     (Thread/currentThread)
     (AtomicBoolean. false)
     (AtomicBoolean. false)
     (AtomicBoolean. false)
     (AtomicBoolean. false))))

(defn- sealed-batch
  "Seal `descriptors` in order, with a collector that only exists so preparation
  can read serving status."
  [descriptors]
  (let [n (count descriptors)
        collector (batch/create (fn [b] (object-array (batch/batch-count b)))
                                {:limits (charge/resolve-limits overrides)})]
    (batch/->Batch 1
                   (FastList. descriptors)
                   (object-array n)
                   (long n)
                   0                                       ; no cutoff
                   nil
                   (AtomicLong. 0)
                   (AtomicBoolean. false)
                   nil                                     ; wal status
                   collector
                   (long n))))

(defn- rows-of [k v] [(row-fn "data" :put (long-bytes k) (long-bytes v))])

;; ---------------------------------------------------------------------------
;; What each member may observe

(deftest bodies-see-their-predecessors-writes-and-never-a-later-members
  (let [seen (atom [])
        read (fn [tx k] (to-long (stage/tx-get tx "data" (long-bytes k))))
        write (fn [tx k v] (stage/tx-put! tx "data" (long-bytes k) (long-bytes v)))
        a (body-descriptor (fn [tx] (swap! seen conj [:a (read tx 3)])
                                      (write tx 1 10)))
        b (body-descriptor (fn [tx] (swap! seen conj [:b (read tx 1)])
                                      (write tx 2 20)))
        c (body-descriptor (fn [tx] (swap! seen conj [:c (read tx 2)])
                                      (write tx 3 30)))
        batch (sealed-batch [a b c])
        plan (rmw/prepare-batch! batch (rmw-opts (base-of [])))]
    (is (= 3 (wrote-weight plan)))
    (is (nil? (:values plan)))
    (testing "each body saw only what had been accepted before it"
      ;; The base is empty, so each read saw exactly its predecessor's write and
      ;; never its own or a later member's.
      (is (= [[:a -1] [:b 10] [:c 20]] @seen)))
    (testing "every member kept its own rows and body"
      (is (= 3 (batch/accepted-count batch)))
      (is (= [[":put@1"] [":put@2"] [":put@3"]] (mapv describe [a b c])))
      (is (every? some? (map #(:wal-body (batch/data %)) [a b c]))))))

(deftest publishing-a-blind-members-rows-is-visibility-only
  ;; Regression: blind requests are prepaid in full before admission, so folding
  ;; their rows into the accepted prefix must not charge them again. Their charge
  ;; is already at its allowance, so any extra charge would reject the request
  ;; and, outside a body's own rejection handling, cancel the whole batch.
  (let [blind (blind-descriptor (rows-of 1 42))
        before (batch/charged blind)
        reader (body-descriptor (fn [tx] (to-long (stage/tx-get tx "data"
                                                            (long-bytes 1)))))
        plan (rmw/prepare-batch! (sealed-batch [blind reader])
                                 (rmw-opts (base-of [])))]
    (is (= 1 (wrote-weight plan)))
    (is (= 42 (:result (batch/data reader))))
    (testing "the prepaid blind member kept its whole allowance for itself"
      (is (= before (batch/charged blind))))
    (testing "while its rows were still visible to the later body"
      (is (= [":put@1"] (describe blind))))))

(deftest a-retained-view-cannot-stage-after-its-own-result
  ;; Regression: a body that kept its view could stage into a request whose
  ;; result had already been published and whose reservation released, after a
  ;; successor had already accepted its own write of the same key.
  (let [retained (atom nil)
        first-d (body-descriptor
                 (fn [tx]
                   (stage/tx-put! tx "data" (long-bytes 1) (long-bytes 10))
                   (reset! retained tx)
                   :first))
        successor (body-descriptor
                   (fn [tx]
                     (stage/tx-put! tx "data" (long-bytes 1) (long-bytes 20))
                     :successor))
        batch (sealed-batch [first-d successor])
        plan (rmw/prepare-batch! batch (rmw-opts (base-of [])))
        error (try (stage/tx-put! @retained "data" (long-bytes 1) (long-bytes 99))
                   nil
                   (catch Throwable t t))]
    (testing "the retained view is refused once its body returned"
      (is (= :txlog/transaction-view-invalidated (:error (ex-data error)))))
    (testing "so the successor's accepted row is untouched"
      (is (= [":put@1"] (describe successor)))
      (is (= 2 (wrote-weight plan))))
    (testing "and the first request keeps exactly the row it published"
      (is (= [":put@1"] (describe first-d))
          "the late write must not appear in the frozen rows"))))

(deftest a-body-cannot-see-a-later-members-blind-rows
  (let [observed (atom ::unset)
        reader (body-descriptor (fn [tx] (reset! observed
                                                  (stage/tx-get tx "data"
                                                                (long-bytes 1)))
                                       :reader))
        blind (blind-descriptor (rows-of 1 42))
        batch (sealed-batch [reader blind])
        plan (rmw/prepare-batch! batch (rmw-opts (base-of [])))]
    (is (= 1 (wrote-weight plan)))
    (testing "the reader ran before its blind successor published its row"
      (is (nil? @observed)))
    (testing "the blind member kept its own rows and body"
      (is (= [":put@1"] (describe blind)))
      (is (some? (:wal-body (batch/data blind)))))))

(deftest a-body-reads-the-pinned-native-base
  (let [snapshots (atom 0)
        base (base-of [[1 5] [2 6]])
        reader (body-descriptor
                (fn [tx] [(to-long (stage/tx-get tx "data" (long-bytes 1)))
                          (to-long (stage/tx-get tx "data" (long-bytes 2)))
                          (to-long (stage/tx-get tx "data" (long-bytes 3)))]))
        plan (rmw/prepare-batch!
              (sealed-batch [reader])
              (rmw-opts base (fn [f] (swap! snapshots inc) (f base))))]
    (is (zero? (:weight plan)))
    (testing "committed state is visible and absent keys read as absent"
      (is (= [5 6 -1] (:result (batch/data reader)))))
    (testing "one native snapshot covered the whole preparation"
      (is (= 1 @snapshots)))))

;; ---------------------------------------------------------------------------
;; Request-local rejection

(defn- abort-body
  "A body that rejects only its own request."
  [_tx]
  (throw (batch/request-rejection "aborted by test"
                                  {:error :txlog/request-aborted
                                   :retryable? true})))

(deftest a-request-local-rejection-discards-only-its-own-write
  (let [first-d (body-descriptor (fn [tx] (stage/tx-put! tx "data" (long-bytes 1)
                                                        (long-bytes 1))))
        rejected (body-descriptor (fn [tx] (stage/tx-put! tx "data" (long-bytes 2)
                                                           (long-bytes 2))
                                            (abort-body tx)))
        successor (body-descriptor (fn [tx] (if (stage/tx-get tx "data" (long-bytes 2))
                                             :saw-rejected
                                             :clean)))
        batch (sealed-batch [first-d rejected successor])
        plan (rmw/prepare-batch! batch (rmw-opts (base-of [])))]
    (is (= 1 (wrote-weight plan)))
    (testing "the rejected member carries its own cause and no rows"
      (is (= :txlog/request-aborted
             (:error (ex-data (:rejection (batch/data rejected))))))
      (is (nil? (:rows (batch/data rejected)))))
    (testing "its staged row never became visible to the successor"
      (is (= :clean (:result (batch/data successor)))))
    (testing "and it is not counted as an accepted write"
      (is (= 1 (batch/accepted-count batch)))
      (is (= 1 (count (filter some? (map #(:wal-body (batch/data %))
                                         [first-d rejected successor]))))))))

(deftest a-wal-encode-rejection-does-not-leak-into-a-successor
  ;; Regression: staging was published before the WAL body's budget check, so a
  ;; member rejected during encoding was still visible to its successor.
  (let [first-d (body-descriptor
                 (fn [tx] (stage/tx-put! tx "data" (long-bytes 1) (long-bytes 111)))
                 2400)
        successor (body-descriptor
                   (fn [tx] (if (stage/tx-get tx "data" (long-bytes 1))
                              :saw-rejected
                              :clean)))
        batch (sealed-batch [first-d successor])
        plan (rmw/prepare-batch! batch (rmw-opts (base-of [])))]
    (is (= 0 (wrote-weight plan))
        "a member rejected during encoding is not an accepted write")
    (testing "its rejected write never became visible to the successor"
      (is (= :txlog/pending-budget-exceeded
             (:error (ex-data (:rejection (batch/data first-d))))))
      (is (= :clean (:result (batch/data successor)))))
    (is (nil? (:rows (batch/data first-d))))))

(deftest an-ordinary-body-exception-rejects-only-its-own-request
  ;; Regression: classification required the engine's `:not-committed` marker, so
  ;; an ordinary exception from a body fenced the whole collector.
  (let [first-d (body-descriptor (fn [tx] (stage/tx-put! tx "data" (long-bytes 1)
                                                        (long-bytes 1))))
        failed (body-descriptor (fn [tx]
                                  (stage/tx-put! tx "data" (long-bytes 2)
                                                 (long-bytes 2))
                                  (throw (IllegalArgumentException.
                                          "ordinary body bug"))))
        successor (body-descriptor (fn [tx] (if (stage/tx-get tx "data" (long-bytes 2))
                                             :saw-rejected
                                             :clean)))
        batch (sealed-batch [first-d failed successor])
        plan (rmw/prepare-batch! batch (rmw-opts (base-of [])))]
    (is (= 1 (wrote-weight plan)) "the batch kept its other accepted write")
    (testing "the failed member carries its own cause and no rows"
      (is (instance? IllegalArgumentException
                     (:rejection (batch/data failed))))
      (is (nil? (:rows (batch/data failed)))))
    (testing "its staging never became visible to the successor"
      (is (= :clean (:result (batch/data successor)))))))

(deftest an-escaping-interruption-skips-the-successor
  (let [failure (InterruptedException. "owner interrupted")
        ran? (atom false)
        interrupted (body-descriptor (fn [_] (throw failure)))
        successor (body-descriptor (fn [_] (reset! ran? true)))
        thrown (try
                 (rmw/prepare-batch! (sealed-batch [interrupted successor])
                                     (rmw-opts (base-of [])))
                 nil
                 (catch Throwable t t))]
    (is (identical? failure thrown))
    (is (false? @ran?))))

(deftest an-engine-failure-cancels-the-batch-instead-of-one-request
  (let [fenced (body-descriptor (fn [_tx]
                                   (throw (ex-info "fenced"
                                                   {:error :txlog/runtime-fenced
                                                    :outcome :not-committed}))))
        successor (body-descriptor (fn [tx] (stage/tx-put! tx "data" (long-bytes 1)
                                                           (long-bytes 1))
                                        :never-ran))
        batch (sealed-batch [fenced successor])
        thrown (try (rmw/prepare-batch! batch (rmw-opts (base-of []))) nil
                    (catch Throwable t t))]
    (testing "an engine failure escapes preparation instead of rejecting a member"
      (is (= :txlog/runtime-fenced (:error (ex-data thrown)))))
    (testing "so the later member never ran and never published rows"
      (is (nil? (:rows (batch/data successor)))))))

(deftest a-body-that-catches-its-own-rejection-cannot-stage-again
  (let [attempted (atom nil)
        ;; A body is charged as it allocates, so this allowance cannot cover even
        ;; the first staged write's control carriers.
        d (body-descriptor
           (fn [tx]
             (try (stage/tx-put! tx "data" (long-bytes 1) (long-bytes 1))
                  (catch Throwable _ nil))
             (reset! attempted
                     (try (stage/tx-put! tx "data" (long-bytes 2) (long-bytes 2))
                          :staged-anyway
                          (catch Throwable _ :refused))))
           charge/vector-wrapper)
        batch (sealed-batch [d])
        _ (rmw/prepare-batch! batch (rmw-opts (base-of [])))]
    (testing "the second staging attempt was refused after the first failed"
      (is (= :refused @attempted)))
    (testing "so a swallowed rejection cannot resurrect the request"
      (is (= :txlog/pending-budget-exceeded
             (:error (ex-data (:rejection (batch/data d))))))
      (is (empty? (describe d))))))

(deftest a-staged-nil-value-rejects-only-its-own-request
  (let [bad (body-descriptor (fn [tx] (stage/tx-put! tx "data" (long-bytes 1) nil)
                              :bad))
        good (body-descriptor (fn [tx] (stage/tx-put! tx "data" (long-bytes 2)
                                                         (long-bytes 2))
                               :good))
        plan (rmw/prepare-batch! (sealed-batch [bad good]) (rmw-opts (base-of [])))]
    (is (= 1 (wrote-weight plan)))
    (is (= :txlog/invalid-staged-write
           (:error (ex-data (:rejection (batch/data bad))))))
    (is (= [":put@2"] (describe good)))))

;; ---------------------------------------------------------------------------
;; Zero writes, schedule and compaction

(deftest a-batch-of-read-only-members-never-produces-writes
  (let [a (body-descriptor (fn [tx] (to-long (stage/tx-get tx "data" (long-bytes 1)))))
        b (body-descriptor (fn [_tx] :b))
        batch (sealed-batch [a b])
        plan (rmw/prepare-batch! batch (rmw-opts (base-of [[1 5]])))]
    (is (= 0 (:weight plan)))
    (is (some? (:values plan)))
    (is (= [5 :b] (vec (:values plan))))
    (testing "no member was accepted as a write"
      (is (zero? (batch/accepted-count batch)))
      (is (= {:result 5} (batch/data a)))
      (is (= {:result :b} (batch/data b))))
    (testing "and the schedule follows the accepted weight"
      (is (= :inline (batch/batch-schedule batch))))))

(deftest the-schedule-follows-the-accepted-write-weight
  (testing "one accepted write stays inline even in a larger batch"
    (let [blind (blind-descriptor (rows-of 1 1))
          rejected (body-descriptor (fn [_tx]
                                       (throw (batch/request-rejection "no"
                                                                       {:error :txlog/request-aborted
                                                                        :retryable? true}))))
          reader (body-descriptor (fn [_tx] :nothing))
          batch (sealed-batch [blind rejected reader])
          plan (rmw/prepare-batch! batch (rmw-opts (base-of [])))]
      (is (= 1 (wrote-weight plan)))
      (is (= :inline (batch/batch-schedule batch)))
      (is (= 1 (batch/accepted-count batch)))))
  (testing "two accepted writes select the parallel schedule"
    (let [a (blind-descriptor (rows-of 1 1))
          b (blind-descriptor (rows-of 2 2))
          reader (body-descriptor (fn [_tx] :nothing))
          batch (sealed-batch [a b reader])
          plan (rmw/prepare-batch! batch (rmw-opts (base-of [])))]
      (is (= 2 (wrote-weight plan)))
      (is (= :parallel (batch/batch-schedule batch)))
      (is (= 2 (batch/accepted-count batch))))))

;; ---------------------------------------------------------------------------
;; The executor runs preparation before dispatch

(defn- environment
  "A collector running the real two-branch executor with ordered preparation,
  recording what each branch received."
  [opts]
  (let [appended (atom [])
        applied (atom [])
        lsns (atom [])
        lsn (atom 0)
        wal (reify executor/IWalBranch
              (append-group! [_ b n]
                (swap! lsns conj n)
                (swap! appended conj
                       (mapv #(String. ^bytes % "UTF-8") (batch/wal-bodies b)))
                n)
              (complete-policy! [_ _ _] true))
        native (reify executor/INativeBranch
                 (apply-rows! [_ b before-commit]
                   (before-commit)
                   (swap! applied conj
                          (mapv #(str (nth % 0) "@" (to-long (nth % 2)))
                                (mapcat :rows
                                        (map #(batch/data (batch/batch-at b %))
                                             (range (batch/batch-count b))))))
                   (let [n (batch/batch-count b)
                         values (object-array n)]
                     (dotimes [i n]
                       (let [data (batch/data (batch/batch-at b i))]
                         (aset values i (if-let [rej (:rejection data)]
                                           (batch/rejected rej)
                                           (:result data)))))
                     values)))
        c (batch/create
           (executor/create wal native (fn [] (swap! lsn inc))
                            {:prepare-batch! #(rmw/prepare-batch! % opts)})
           {:limits (charge/resolve-limits overrides)})]
    {:collector c :appended appended :applied applied :lsns lsns}))

(deftest preparation-runs-before-dispatch-for-a-state-dependent-body
  (let [{:keys [collector appended applied lsns]} (environment
                                                    (rmw-opts (base-of [[1 41]])))
        value (batch/submit!
               collector
               {:op (fn [tx]
                      (let [seen (to-long (stage/tx-get tx "data" (long-bytes 1)))]
                        (stage/tx-put! tx "data" (long-bytes 2) (long-bytes (inc seen)))
                        seen))})]
    (testing "the body ran, read the pinned base and its caller got its result"
      (is (= 41 value)))
    (testing "one LSN, one WAL body and one native write, all from preparation"
      (is (= [1] @lsns))
      (is (= 1 (count @appended)))
      (is (= [":put@2="] (first @appended)))
      (is (= [[":put@2"]] @applied)))
    (testing "and the runtime is still serving with no charges retained"
      (is (batch/serving? collector))
      (is (zero? (:requests (batch/usage collector)))))))

(deftest a-read-only-body-skips-the-wal-and-the-native-writer
  (let [{:keys [collector appended applied lsns]} (environment
                                                    (rmw-opts (base-of [[1 7]])))]
    (testing "the caller still gets the value its own body computed"
      (is (= 7 (batch/submit! collector
                            {:op (fn [tx] (to-long (stage/tx-get tx "data"
                                                                 (long-bytes 1))))}))))
    (testing "no LSN was assigned and neither branch ran"
      (is (empty? @lsns))
      (is (empty? @appended))
      (is (empty? @applied)))
    (is (batch/serving? collector))))

(deftest owner-interruption-cancels-before-dispatch-and-restores-the-flag
  (let [{:keys [collector appended applied lsns]}
        (environment (rmw-opts (base-of [])))]
    (try
      (let [thrown (try
                     (batch/submit! collector
                                    {:op (fn [_]
                                           (throw (InterruptedException.
                                                    "owner interrupted")))})
                     nil
                     (catch Throwable t t))
            interrupted? (.isInterrupted (Thread/currentThread))]
        (is (= :txlog/write-interrupted (:error (ex-data thrown))))
        (is interrupted?)
        (is (empty? @lsns))
        (is (empty? @appended))
        (is (empty? @applied))
        (is (zero? (:requests (batch/usage collector))))
        (is (batch/serving? collector)))
      (finally
        (Thread/interrupted)))
    (is (= :after (batch/submit! collector {:op (fn [_] :after)})))))

(deftest infrastructure-failures-fence-before-dispatch
  (doseq [boundary [:base :row-fn :body-cost :encode-body]
          caught? (if (= boundary :base) [false true] [false])]
    (let [failure (java.io.IOException. (str "injected " boundary))
          opts (assoc (rmw-opts (base-of [])) boundary
                      (fn [& _] (throw failure)))
          {:keys [collector appended applied lsns]} (environment opts)
          thrown (try
                   (batch/submit!
                     collector
                     {:op (fn [tx]
                            (if caught?
                              (try (stage/tx-get tx "data" (long-bytes 1))
                                   (catch java.io.IOException _ nil))
                              (stage/tx-get tx "data" (long-bytes 1)))
                            (stage/tx-put! tx "data" (long-bytes 2)
                                           (long-bytes 2)))})
                   nil
                   (catch Throwable t t))]
      (is (some? thrown) (str boundary " caught=" caught?))
      (is (not (batch/serving? collector)))
      (is (empty? @lsns))
      (is (empty? @appended))
      (is (empty? @applied))
      (is (zero? (:requests (batch/usage collector))))
      (is (thrown? clojure.lang.ExceptionInfo
                   (batch/submit! collector {:op (fn [_] :must-not-run)}))))))

(deftest a-rejected-body-completes-without-reaching-either-branch
  (let [{:keys [collector appended applied lsns]} (environment (rmw-opts (base-of [])))
        thrown (try (batch/submit! collector {:op (fn [tx]
                                                     (stage/tx-put! tx "data"
                                                                   (long-bytes 1)
                                                                   (long-bytes 1))
                                                     (abort-body tx))})
                    nil
                    (catch Throwable t t))]
    (testing "only this request failed"
      (is (= :txlog/request-aborted (:error (ex-data thrown)))))
    (testing "its write never became part of a WAL group or the native writer"
      (is (empty? @lsns))
      (is (empty? @appended))
      (is (empty? @applied)))
    (testing "and the runtime kept serving"
      (is (batch/serving? collector))
      (is (= :after (batch/submit! collector
                                  {:op (fn [tx] (stage/tx-put! tx "data"
                                                              (long-bytes 2)
                                                              (long-bytes 2))
                                         :after)}))))))
