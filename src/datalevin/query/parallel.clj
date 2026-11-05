(ns ^:no-doc datalevin.query.parallel
  "Execute independent read partitions and drain them before releasing a query."
  (:require [datalevin.binding.cpp.iter :as iter]
            [datalevin.client.request :as request]
            [datalevin.interface :as i]
            [datalevin.timeout :as timeout])
  (:import [datalevin.cpp Txn]
           [java.util ArrayList]
           [java.util.concurrent Callable ForkJoinPool Future]))

(def ^:dynamic *failure* nil)

(defn check!
  "Cooperatively stop sibling work after failure, cancellation, or timeout."
  []
  (when (or (.isInterrupted (Thread/currentThread))
            (and *failure* @*failure*))
    (throw (InterruptedException. "Parallel query interrupted")))
  (timeout/assert-time-left)
  (request/check! :request))

(defn snapshot-id
  ^long [rtx]
  (.id ^Txn (iter/rtx-txn rtx)))

(defn map-read
  "Run f once per input with a private thread reader matching snapshot-id.
  A nil result means a snapshot mismatch, allowing the caller to fall back.
  All submitted work finishes before this returns or throws; cancellation never
  releases a database while a sibling still owns a cursor."
  [lmdb snapshot-id f inputs]
  (let [failure (atom nil)
        bindings (assoc (get-thread-bindings) #'*failure* failure)
        run (fn [input]
              (with-bindings bindings
                (try
                  (check!)
                  (let [rtx (i/get-rtx lmdb)]
                    (try
                      (when (= (long snapshot-id) (datalevin.query.parallel/snapshot-id rtx))
                        (f input))
                      (finally (i/return-rtx lmdb rtx))))
                  (catch Throwable error
                    (compare-and-set! failure nil error)
                    nil))))
        ^ForkJoinPool pool (ForkJoinPool/commonPool)
        futures (ArrayList.)
        interrupted? (volatile! false)
        result (volatile! [])]
    (try
      (doseq [input (rest inputs)]
        (.add futures (.submit pool ^Callable (fn [] (run input)))))
      (vswap! result conj (run (first inputs)))
      (catch Throwable error (compare-and-set! failure nil error))
      (finally
        ;; Future.cancel can complete before its task releases native resources.
        ;; Signal through failure and join every submitted task instead.
        (doseq [^Future future futures]
          (loop []
            (let [finished?
                  (try
                    (vswap! result conj (.get future))
                    true
                    (catch InterruptedException error
                      (vreset! interrupted? true)
                      (compare-and-set! failure nil error)
                      false)
                    (catch Throwable error
                      (compare-and-set! failure nil error)
                      true))]
              (when-not finished? (recur)))))
        (when @interrupted? (.interrupt (Thread/currentThread)))))
    (if-let [error @failure] (throw error) @result)))
