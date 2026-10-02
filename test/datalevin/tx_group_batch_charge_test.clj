(ns datalevin.tx-group-batch-charge-test
  (:require [clojure.test :refer [deftest is testing]]
            [datalevin.tx-group.batch.charge :as charge]))

(deftest align16-rounds-up-checked
  (testing "rounds up to the next multiple of 16 without touching exact fits"
    (is (= 0 (charge/align16 0)))
    (is (= 16 (charge/align16 1)))
    (is (= 16 (charge/align16 16)))
    (is (= 32 (charge/align16 17)))
    (is (= 2112 (charge/align16 2097))))
  (testing "rejects negative charges instead of wrapping"
    (is (thrown? clojure.lang.ExceptionInfo (charge/align16 -1)))
    (is (thrown? ArithmeticException (charge/align16 Long/MAX_VALUE)))))

(deftest array-charges-count-capacity-not-length
  (testing "an empty array still costs its header"
    (is (= 32 (charge/array-bytes 8 0)))
    (is (= 32 (charge/array-bytes 1 0))))
  (testing "element width and allocated capacity both count"
    (is (= 96 (charge/array-bytes 8 8)))
    (is (= 2096 (charge/array-bytes 8 258))))
  (testing "overflow is rejected before allocation"
    (is (thrown? ArithmeticException (charge/array-bytes Long/MAX_VALUE 4)))))

(deftest many-empty-rows-still-charge-their-separate-owned-representations
  ;; Payload-only or one-header-per-request accounting misses these costs.
  (let [n 256
        minimum (+ charge/request-control-bundle
                   (* n (+ charge/encoded-row-descriptor
                           (* 2 (charge/array-bytes 1 0))
                           (* 2 charge/buffer-wrapper)
                           charge/per-key-staging-state
                           charge/ordered-map-entry)))
        allowance (charge/blind-allowance {:declared-bytes 0 :row-capacity n})]
    (is (>= allowance minimum))))

(deftest owned-representation-charges
  (testing "a flat vector is its wrapper plus a reference array"
    (is (= 64 charge/vector-wrapper))
    (is (= 96 (charge/vector-bytes 0)))
    (is (= 160 (charge/vector-bytes 8))))
  (testing "an owned string is its box plus a char array"
    (is (= 128 (charge/string-bytes 16))))
  (testing "a carrier is its wrapper plus its reference array"
    (is (= 2160 (charge/carriers-bytes 258))))
  (testing "buffer wrappers are fixed"
    (is (= 256 charge/buffer-wrapper))
    (is (= 128 charge/encoded-row-descriptor))
    (is (= 128 charge/ordered-map-container))
    (is (= 128 charge/ordered-map-entry))
    (is (= 128 charge/per-key-staging-state))
    (is (= 1024 charge/request-control-bundle))))

(deftest shared-workspace-matches-the-contract
  (testing "F = 128 KiB + 256*Q + 3*V(N + 2) + 256*(N + 2)"
    ;; The contract's worked example: N = 256, Q = 4096 -> 1,252,176 bytes.
    (is (= 1252176 (charge/shared-reserved 256 4096)))))

(deftest default-limits-match-the-contract
  (let [limits (charge/resolve-limits nil)]
    (is (= 4096 (:max-requests limits)))
    (is (= 67108864 (:byte-budget limits)))
    (is (= 256 (:batch-limit limits)))
    (is (= 8388608 (:batch-max-bytes limits)))
    (is (= 1252176 (:shared-reserved limits)))
    (is (= 65856688 (:request-budget limits)))
    (testing "Q follows the environment request limit"
      (is (= (:max-requests limits) (:waiter-limit limits))))
    (testing "the batch byte cap is independent of the remaining global budget"
      (is (< (:batch-max-bytes limits) (:request-budget limits))))))

(deftest limits-accept-documented-overrides
  (let [limits (charge/resolve-limits {:wal-pending-max-requests 64
                                       :wal-pending-max-bytes 1048576
                                       :write-batch-size 8
                                       :write-batch-max-bytes 12288
                                       :wal-rmw-max-bytes 4096})]
    (is (= 64 (:max-requests limits)))
    (is (= 64 (:waiter-limit limits)))
    (is (= 8 (:batch-limit limits)))
    (is (= 12288 (:batch-max-bytes limits)))
    (is (= 4096 (:rmw-allowance-bytes limits)))))

(deftest limits-reject-incompatible-settings
  (testing "non-positive integers"
    (is (thrown? clojure.lang.ExceptionInfo
                 (charge/resolve-limits {:wal-pending-max-requests 0})))
    (is (thrown? clojure.lang.ExceptionInfo
                 (charge/resolve-limits {:wal-pending-max-bytes -1})))
    (is (thrown? clojure.lang.ExceptionInfo
                 (charge/resolve-limits {:write-batch-size 0})))
    (is (thrown? clojure.lang.ExceptionInfo
                 (charge/resolve-limits {:write-batch-max-bytes 0})))
    (is (thrown? clojure.lang.ExceptionInfo
                 (charge/resolve-limits {:wal-rmw-max-bytes 512}))))
  (testing "N cannot exceed the admitted request limit"
    (is (thrown? clojure.lang.ExceptionInfo
                 (charge/resolve-limits {:wal-pending-max-requests 4
                                         :write-batch-size 8
                                         :write-batch-max-bytes 1024
                                         :wal-rmw-max-bytes 1024}))))
  (testing "F must fit inside the byte budget"
    (is (thrown? clojure.lang.ExceptionInfo
                 (charge/resolve-limits {:wal-pending-max-bytes 65536}))))
  (testing "the batch byte cap cannot exceed the remaining admission budget"
    (is (thrown? clojure.lang.ExceptionInfo
                 (charge/resolve-limits {:wal-pending-max-bytes 1048576
                                         :write-batch-max-bytes 1048576
                                         :wal-rmw-max-bytes 1024}))))
  (testing "the batch byte cap is never silently replaced by global capacity"
    (let [limits (charge/resolve-limits {:wal-pending-max-bytes 8388608
                                         :wal-pending-max-requests 16
                                         :write-batch-size 4
                                         :write-batch-max-bytes 4096
                                         :wal-rmw-max-bytes 4096})]
      (is (= 4096 (:batch-max-bytes limits)))
      (is (> (:request-budget limits) (:batch-max-bytes limits))))))

(deftest nine-default-rmw-allowances-bind-the-byte-cap
  ;; The contract's worked selection example: nine ready requests with the
  ;; default 1 MiB allowance select eight and leave one queued, even though
  ;; N = 256 and global admission has ample capacity.
  (let [rmw (charge/rmw-allowance 1048576)
        cap 8388608]
    (is (= 8 (long (/ cap rmw))))
    (testing "an exact fit is allowed, so eight fit precisely"
      (is (= cap (* 8 rmw)))
      (is (<= (* 8 rmw) cap))
      (is (> (* 9 rmw) cap)))))

(deftest blind-allowance-covers-its-declared-layout
  (let [small (charge/blind-allowance {:declared-bytes 0 :row-capacity 1})
        large (charge/blind-allowance {:declared-bytes 65536 :row-capacity 64
                                       :scratch-bytes 4096 :result-capacity 32
                                       :duplicate-values 16})]
    (testing "every allowance at least covers its own control bundle"
      (is (>= small charge/request-control-bundle))
      (is (>= large charge/request-control-bundle)))
    (testing "charges grow monotonically with declared storage"
      (is (> large small))
      (is (> (charge/blind-allowance {:declared-bytes 4096 :row-capacity 1}) small))
      (is (> (charge/blind-allowance {:declared-bytes 0 :row-capacity 8}) small)))
    (testing "negative declarations are rejected"
      (is (thrown? clojure.lang.ExceptionInfo
                   (charge/blind-allowance {:declared-bytes -1})))))
  (testing "unchecked overflow is rejected rather than wrapped"
    (is (thrown? ArithmeticException
                 (charge/blind-allowance {:declared-bytes Long/MAX_VALUE
                                          :row-capacity 1024})))))
