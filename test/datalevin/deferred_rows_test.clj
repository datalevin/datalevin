(ns datalevin.deferred-rows-test
  (:require [clojure.test :refer [deftest is]])
  (:import [datalevin.utl DeferredRows RowRegions]
           [java.util.function Supplier]
           [java.util.concurrent CountDownLatch TimeUnit]))

(deftest frozen-row-size-does-not-encode-and-concurrent-readers-share-one-result
  (let [calls (atom 0)
        entered (CountDownLatch. 1)
        release (CountDownLatch. 1)
        rows (DeferredRows. 2
                           (reify Supplier
                             (get [_]
                               (swap! calls inc)
                               (.countDown entered)
                               (assert (.await release 5 TimeUnit/SECONDS))
                               [:first :second])))
        regions (RowRegions.)]
    (.append regions rows)
    (is (= 2 (.size regions)))
    (is (zero? @calls))
    (let [first-reader (future (.get rows 0))]
      (try
        (is (.await entered 5 TimeUnit/SECONDS))
        (let [second-reader (future (vec regions))]
          (.countDown release)
          (is (= :first (deref first-reader 5000 ::timeout)))
          (is (= [:first :second] (deref second-reader 5000 ::timeout)))
          (is (= [:first :second] (vec regions)))
          (is (= 1 @calls)))
        (finally (.countDown release))))))

(deftest failed-row-encoding-is-not-retried-by-the-other-branch
  (let [failure (ex-info "encoding failed" {})
        calls (atom 0)
        rows (DeferredRows. 1 (reify Supplier
                                (get [_] (swap! calls inc) (throw failure))))]
    (dotimes [_ 2]
      (is (identical? failure (try (.get rows 0) (catch Exception e e)))))
    (is (= 1 @calls))))
