;; Copyright (c) Huahai Yang. All rights reserved.
;; Distributed under the Eclipse Public License 2.0.
(ns datalevin.tx-group-batch-stage-test
  "Mechanics of the ordered staging layer: read-your-writes, the three-layer read
  order, sticky request-local rejection, prefix publication and teardown.

  These run against a real descriptor so charging is exercised, but against no
  store, WAL or executor. Which members a body may see is a property of the
  ordered driver, so it belongs to `datalevin.tx-group-batch-rmw-test`."
  (:require [clojure.test :refer [deftest is testing]]
            [datalevin.tx-group.batch :as batch]
            [datalevin.tx-group.batch.charge :as charge]
            [datalevin.tx-group.batch.stage :as stage])
  (:import [java.util Arrays]
           [java.util.concurrent.atomic AtomicLong]))

(defn- long-bytes ^bytes [v]
  (let [buffer (java.nio.ByteBuffer/allocate 8)]
    (.putLong buffer (long v))
    (.array buffer)))

(defn- to-long [^bytes bs]
  (when bs
    (let [buffer (java.nio.ByteBuffer/wrap bs)]
      (.getLong buffer))))

(defn- descriptor-with-charge [allowance initial-charge]
  (-> (batch/->Descriptor nil nil nil (long allowance)
                          Long/MAX_VALUE
                          (AtomicLong. (long initial-charge))
                          (java.util.concurrent.atomic.AtomicBoolean. false)
                          nil nil
                          (java.util.concurrent.atomic.AtomicBoolean. false)
                          (java.util.concurrent.atomic.AtomicBoolean. false)
                          (java.util.concurrent.atomic.AtomicBoolean. false)
                          (java.util.concurrent.atomic.AtomicBoolean. false))))

(defn- descriptor-with-allowance [allowance]
  (descriptor-with-charge allowance charge/request-control-bundle))

(defn- prepaid-descriptor
  "A descriptor charged to its full allowance, as a blind request is before it is
  admitted. Any further charge fails."
  [allowance]
  (descriptor-with-charge allowance allowance))

(defn- other-descriptor [] (descriptor-with-allowance 1048576))

(defn- row-fn [dbi op key value]
  (if (= op :del)
    [:del dbi key nil :raw]
    [:put dbi key value :raw :raw]))

(defn- charged [d] (long (.get ^AtomicLong (.charged d))))

(deftest a-body-reads-its-own-staged-writes-and-nothing-else
  (let [d (descriptor-with-allowance 1048576)
        st (stage/create)
        view (stage/view st d (fn [_ _ _] (long-bytes 7)) nil)]
    (testing "an unstaged key falls through to the native base"
      (is (= 7 (to-long (stage/tx-get view "data" (long-bytes 1))))))
    (stage/tx-put! view "data" (long-bytes 1) (long-bytes 100))
    (testing "its own staged write wins over the base"
      (is (= 100 (to-long (stage/tx-get view "data" (long-bytes 1))))))
    (testing "another descriptor sees the base until this request is published"
      (let [other (other-descriptor)
            view2 (stage/view st other (fn [_ _ _] (long-bytes 7)) nil)]
        (is (= 7 (to-long (stage/tx-get view2 "data" (long-bytes 1)))))))))

(deftest freezing-publishes-rows-in-sorted-key-order
  (let [d (descriptor-with-allowance 1048576)
        st (stage/create)
        view (stage/view st d nil nil)]
    (doseq [v [3 1 2]] (stage/tx-put! view "data" (long-bytes v) (long-bytes v)))
    (let [rows (stage/freeze! st d row-fn)]
      (testing "one row per distinct staged key, in unsigned key order"
        (is (= [1 2 3]
               (mapv (fn [row] (to-long (nth row 2))) rows)))
        (is (every? (fn [row] (and (= :put (nth row 0))
                                   (= "data" (nth row 1))
                                   (= :raw (nth row 4))
                                   (= :raw (nth row 5))))
                    rows))
        (is (= [1 2 3] (mapv (fn [row] (to-long (nth row 3))) rows)))))))

(deftest repeated-writes-to-one-key-collapse-to-one-row
  (let [d (descriptor-with-allowance 1048576)
        st (stage/create)
        view (stage/view st d nil nil)]
    (stage/tx-put! view "data" (long-bytes 1) (long-bytes 1))
    (stage/tx-put! view "data" (long-bytes 1) (long-bytes 2))
    (stage/tx-put! view "data" (long-bytes 1) (long-bytes 3))
    (let [rows (stage/freeze! st d row-fn)]
      (is (= 1 (count rows)))
      (testing "the last staged value is the one that becomes the row"
        (is (= 3 (to-long (nth (first rows) 3))))))))

(deftest a-delete-tombstone-masks-the-accepted-prefix
  (let [writer (descriptor-with-allowance 1048576)
        deleter (descriptor-with-allowance 1048576)
        st (stage/create)]
    (stage/tx-put! (stage/view st writer nil nil) "data" (long-bytes 1) (long-bytes 42))
    (stage/freeze! st writer row-fn)
    (let [view (stage/view st deleter nil nil)]
      (is (= 42 (to-long (stage/tx-get view "data" (long-bytes 1)))))
      (stage/tx-del! view "data" (long-bytes 1))
      (testing "a staged delete reads as absent before it is published"
        (is (nil? (stage/tx-get view "data" (long-bytes 1)))))
      (testing "and becomes an explicit delete row"
        (let [rows (stage/freeze! st deleter row-fn)]
          (is (= 1 (count rows)))
          (is (= [:del "data" 1 :raw]
                 [(nth (first rows) 0) (nth (first rows) 1)
                  (to-long (nth (first rows) 2)) (nth (first rows) 4)])))))
    (testing "the prefix tombstone hides the earlier value from later requests"
      (let [reader (other-descriptor)]
        (is (nil? (stage/tx-get (stage/view st reader nil nil) "data" (long-bytes 1))))))))

(deftest a-rejected-request-garbage-collects-its-writes
  (let [d (descriptor-with-allowance 1048576)
        st (stage/create)
        view (stage/view st d nil nil)]
    (stage/tx-put! view "data" (long-bytes 1) (long-bytes 1))
    (stage/invalidate! view)
    (testing "invalidated staging never reaches the prefix"
      (is (= 0 (count (stage/freeze! st d row-fn))))
      (is (nil? (stage/tx-get (stage/view st d nil nil) "data" (long-bytes 1)))))
    (testing "and no later request can observe the discarded key"
      (let [reader (other-descriptor)
            view (stage/view st reader (fn [_ _ _] (long-bytes 9)) nil)]
        (is (= 9 (to-long (stage/tx-get view "data" (long-bytes 1)))))))))

(deftest an-over-budget-staged-write-rejects-the-request-and-poisons-it
  (let [d (descriptor-with-allowance charge/request-control-bundle)
        st (stage/create)
        view (stage/view st d nil nil)
        thrown (try
                 (stage/tx-put! view "data" (long-bytes 1) (long-bytes 1))
                 nil
                 (catch Throwable t t))]
    (testing "the write that does not fit is a request-local rejection"
      (is (some? thrown))
      (is (= :not-committed (:outcome (ex-data thrown)))))
    (testing "the request stays rejected even though the body swallowed the error"
      (is (stage/rejected? view))
      (is (identical? thrown (stage/rejection view)))
      (is (thrown? Throwable (stage/tx-put! view "data" (long-bytes 2) (long-bytes 2)))))
    (testing "the refused write charged this request nothing"
      (is (= charge/request-control-bundle (charged d))))))

(deftest aborting-is-sticky-and-local
  (let [d (descriptor-with-allowance 1048576)
        other (other-descriptor)
        st (stage/create)
        view (stage/view st d nil nil)]
    (stage/tx-put! view "data" (long-bytes 1) (long-bytes 1))
    (is (thrown? Throwable (stage/tx-abort! view)))
    (is (stage/rejected? view))
    (is (= :txlog/request-aborted (:error (ex-data (stage/rejection view)))))
    (let [again (try (stage/tx-abort! view) nil (catch Throwable t t))]
      (testing "aborting again reports the same sticky rejection"
        (is (some? again))
        (is (= :txlog/request-aborted (:error (ex-data again))))))
    (testing "another request's staging is untouched"
      (stage/tx-put! (stage/view st other nil nil) "data" (long-bytes 2) (long-bytes 2))
      (is (not (stage/rejected? (stage/view st other nil nil)))))))

(deftest a-view-is-unusable-once-its-body-has-returned
  ;; Regression: a body that kept its view could stage into a request whose
  ;; result was already published and whose reservation released, so nothing
  ;; owned those rows afterwards.
  (let [d (descriptor-with-allowance 1048576)
        st (stage/create)
        view (stage/view st d nil nil)
        thrown (try (stage/tx-put! view "data" (long-bytes 1) (long-bytes 99))
                    nil
                    (catch Throwable t t))]
    (testing "the body ran on this thread, so the view started usable"
      (is (nil? thrown)))
    (stage/invalidate! view)
    (testing "after its body returned, every operation is refused"
      ;; `tx-abort!` reports its own cause, because refusing it is a no-op either
      ;; way: the request is already unusable.
      (doseq [[attempt expected]
              [[#(stage/tx-put! % "data" (long-bytes 2) (long-bytes 2))
                :txlog/transaction-view-invalidated]
               [#(stage/tx-del! % "data" (long-bytes 2))
                :txlog/transaction-view-invalidated]
               [#(stage/tx-get % "data" (long-bytes 1))
                :txlog/transaction-view-invalidated]
               [#(stage/tx-abort! %) :txlog/request-aborted]]]
        (let [error (try (attempt view) nil (catch Throwable t t))]
          (is (some? error))
          (is (= expected (:error (ex-data error)))))))
    (testing "the refusal is this request's own sticky rejection"
      (is (stage/rejected? view))
      (is (= :txlog/transaction-view-invalidated
             (:error (ex-data (stage/rejection view))))))
    (testing "and none of those writes reached this request's staging"
      (is (empty? (stage/freeze! st d row-fn))))))

(deftest a-view-belongsto-the-thread-running-its-own-body
  ;; Regression: a view handed to another thread raced the stage's owner-confined
  ;; maps, which are not thread-safe.
  (let [d (descriptor-with-allowance 1048576)
        st (stage/create)
        view (stage/view st d nil nil)
        error (future
                (try (stage/tx-put! view "data" (long-bytes 1) (long-bytes 1))
                     nil
                     (catch Throwable t t)))
        thrown (deref error 5000 ::timeout)]
    (is (some? thrown))
    (is (= :txlog/transaction-view-foreign-thread (:error (ex-data thrown))))
    (testing "the foreign write is refused rather than staged"
      (is (stage/rejected? view))
      (is (empty? (stage/freeze! st d row-fn))))
    (testing "and the owning thread cannot use it afterwards either"
      (let [later (try (stage/tx-put! view "data" (long-bytes 1) (long-bytes 1))
                       nil
                       (catch Throwable t t))]
        (is (= :txlog/transaction-view-foreign-thread
               (:error (ex-data later))))))))

(deftest a-staged-nil-value-is-refused-instead-of-becoming-an-absent-key
  (let [d (descriptor-with-allowance 1048576)
        st (stage/create)
        view (stage/view st d nil nil)
        thrown (try (stage/tx-put! view "data" (long-bytes 1) nil) nil
                    (catch Throwable t t))]
    (is (= :txlog/invalid-staged-write (:error (ex-data thrown))))
    (is (= 0 (count (stage/freeze! st d row-fn))))))

(deftest folding-blind-rows-publishes-only-their-visibility
  ;; A blind request is fully prepaid before admission, so folding its rows must
  ;; charge nothing: its charge is already at its allowance.
  (let [blind (prepaid-descriptor 1048576)
        st (stage/create)
        before (charged blind)]
    (stage/fold-blind! st "data" :put (long-bytes 1) (long-bytes 5))
    (stage/fold-blind! st "data" :del (long-bytes 2) nil)
    (testing "folding a prepaid blind row charges that request nothing"
      (is (= before (charged blind))))
    (let [reader (other-descriptor)
          view (stage/view st reader (fn [_ _ _] (long-bytes 99)) nil)]
      (testing "folded puts and deletes become the accepted prefix"
        (is (= 5 (to-long (stage/tx-get view "data" (long-bytes 1)))))
        (is (nil? (stage/tx-get view "data" (long-bytes 2))))))
    (testing "and the prefix does not alias the caller's row arrays"
      (let [view (stage/view st (other-descriptor) (fn [_ _ _] nil) nil)
            staged (stage/tx-get view "data" (long-bytes 1))]
        (is (not (identical? staged (long-bytes 5))))
        (Arrays/fill ^bytes staged (byte 0))
        (let [view2 (stage/view st (other-descriptor) (fn [_ _ _] nil) nil)]
          (is (= 5 (to-long (stage/tx-get view2 "data" (long-bytes 1))))))))
    (testing "and a blind request's rows are never rebuilt from private staging"
      (is (= 0 (count (stage/freeze! st blind row-fn)))))))

(deftest a-check-failure-is-raised-at-every-transaction-operation
  (let [st (stage/create)
        d (other-descriptor)
        checks (atom 0)
        view (stage/view st d nil (fn [] (swap! checks inc)))]
    (is (nil? (stage/tx-get view "data" (long-bytes 1))))
    (stage/tx-put! view "data" (long-bytes 1) (long-bytes 1))
    (stage/tx-del! view "data" (long-bytes 2))
    (is (= 3 @checks))))

(deftest a-body-cannot-mutate-a-predecessors-accepted-bytes
  ;; Regression: tx-get returned the accepted prefix's byte array directly, so a
  ;; later body mutated a predecessor's native row while its encoded WAL still
  ;; held the original value.
  (let [writer (descriptor-with-allowance 1048576)
        reader (descriptor-with-allowance 1048576)
        st (stage/create)
        written (long-bytes 10)]
    (stage/tx-put! (stage/view st writer nil nil) "data" (long-bytes 1) written)
    (stage/freeze! st writer row-fn)
    (let [view (stage/view st reader nil nil)
          read (stage/tx-get view "data" (long-bytes 1))]
      (is (= 10 (to-long read)))
      (testing "the read is a detached copy of the accepted prefix"
        (is (not (identical? written read)))
        (Arrays/fill ^bytes read (byte 99))
        (is (= 10 (to-long (stage/tx-get (stage/view st reader nil nil)
                                         "data" (long-bytes 1)))))))))

(deftest mutating-a-staged-input-cannot-change-the-staged-value
  (let [d (descriptor-with-allowance 1048576)
        st (stage/create)
        view (stage/view st d nil nil)
        input (long-bytes 10)]
    (stage/tx-put! view "data" (long-bytes 1) input)
    (Arrays/fill ^bytes input (byte 99))
    (is (= 10 (to-long (stage/tx-get view "data" (long-bytes 1))))
        "the stage owns a frozen copy of a staged value")))

(deftest mutating-a-staged-key-cannot-change-the-staged-key
  (let [d (descriptor-with-allowance 1048576)
        st (stage/create)
        view (stage/view st d nil nil)
        k (long-bytes 1)]
    (stage/tx-put! view "data" k (long-bytes 10))
    (Arrays/fill ^bytes k (byte 99))
    (is (= 10 (to-long (stage/tx-get view "data" (long-bytes 1)))))
    (is (nil? (stage/tx-get view "data" (long-bytes 99))))))

(deftest a-body-cannot-mutate-the-native-base-bytes
  (let [d (descriptor-with-allowance 1048576)
        base-bytes (long-bytes 7)
        st (stage/create)
        view (stage/view st d (fn [_ _ _] base-bytes) nil)
        read (stage/tx-get view "data" (long-bytes 1))]
    (is (= 7 (to-long read)))
    (is (not (identical? base-bytes read)))
    (Arrays/fill ^bytes read (byte 99))
    (is (= 7 (to-long (stage/tx-get view "data" (long-bytes 1))))
        "the native base buffer is not exposed to the body")))