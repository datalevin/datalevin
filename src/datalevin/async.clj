;;
;; Copyright (c) Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns ^:no-doc datalevin.async
  "Asynchronous work mechanism that does adaptive batch processing - the higher
  the load, the bigger the batch"
  (:require
   [taoensso.timbre :as log])
  (:import
   [java.util.concurrent.atomic AtomicBoolean]
   [java.util.concurrent Executors ExecutorService LinkedBlockingQueue
    ConcurrentLinkedQueue ConcurrentHashMap Callable TimeUnit
    ThreadPoolExecutor ArrayBlockingQueue ThreadPoolExecutor$CallerRunsPolicy
    Semaphore]
   [org.eclipse.collections.impl.list.mutable FastList]))

(defprotocol IAsyncWork
  "Work that wishes to be done asynchronously and auto-batched needs to
  implement this protocol"
  (work-key [_]
    "Return a keyword representing this type of work, workloads of the same
    type will be batched")
  (do-work [_]
    "Actually do the work, result will be in the future returned by exec.
    If there's an exception, it will be thrown when deref the future.")
  (combine [_]
    "Return a function that takes a collection of this work and combine them
     into one. Or return nil if there's no need to combine.")
  (callback [_]
    "Return a callback for when a work is done. This callback takes as
     input the result of do-work. This could be nil."))

(deftype WorkItem [work promise cb])

(deftype WorkQueue [^ConcurrentLinkedQueue items  ; [WorkItem ...]
                    fw ; first work
                    ^FastList stage ; for combining work
                    ])

(def ^:private async-backlog-factor
  64)

(def ^:private async-min-backlog
  256)

(defn- async-backlog-capacity
  ([] (async-backlog-capacity (.availableProcessors (Runtime/getRuntime))))
  ([^long threads]
   (long
     (Math/max (long async-min-backlog)
               (long (* (long async-backlog-factor) threads))))))

(defn- do-work*
  [work]
  (try [:ok (do-work work)]
       (catch Throwable e
         [:err e])))

(defn ^:no-doc new-backlog-semaphore
  []
  ;; Bound queued work so overloaded producers block instead of letting
  ;; promises/work items accumulate until the process runs out of heap.
  (Semaphore. (int (async-backlog-capacity)) true))

(defn- run-callback!
  [cb payload]
  (when cb
    (try
      (cb payload)
      (catch Throwable e
        (log/warn e "Async callback failed")))))

(defn- finalize-work-item!
  [^WorkItem item res ^Semaphore backlog]
  (try
    (when-let [p (.-promise item)]
      (deliver p res))
    (finally
      (.release backlog))))

(deftype AsyncResult [p]
  clojure.lang.IDeref
  (deref [_]
    (let [[status payload] @p]
      (if (identical? status :ok)
        payload
        (throw payload))))
  clojure.lang.IBlockingDeref
  (deref [_ timeout-ms timeout-val]
    (let [result (deref p timeout-ms ::timeout)]
      (if (identical? result ::timeout)
        timeout-val
        (let [[status payload] result]
          (if (identical? status :ok)
            payload
            (throw payload))))))
  clojure.lang.IPending
  (isRealized [_] (realized? p)))

(defn- individual-work
  [^ConcurrentLinkedQueue items ^Semaphore backlog]
  (loop []
    (when (.peek items)
      (let [^WorkItem item (.poll items)
            res            (do-work* (.-work item))
            [status payload] res
            cb             (.-cb item)]
        (finalize-work-item! item res backlog)
        (when (identical? status :ok)
          (run-callback! cb payload)))
      (recur))))

(defn- combined-work
  [cmb ^ConcurrentLinkedQueue items ^FastList stage ^Semaphore backlog]
  (.clear stage)
  (loop []
    (when (.peek items)
      (let [^WorkItem item (.poll items)]
        (.add stage item))
      (recur)))
  (let [res              (do-work* (cmb (mapv #(.-work ^WorkItem %) stage)))
        [status payload] res]
    (dotimes [i (.size stage)]
      (finalize-work-item! ^WorkItem (.get stage i) res backlog))
    (when (identical? status :ok)
      (dotimes [i (.size stage)]
        (run-callback! (.-cb ^WorkItem (.get stage i)) payload)))))

(defn- event-handler
  [^ConcurrentHashMap work-queues k ^Semaphore backlog]
  (let [^WorkQueue wq                (.get work-queues k)
        ^ConcurrentLinkedQueue items (.-items wq)
        first-work                   (.-fw wq)]
    (locking items
      (if-let [cmb (combine first-work)]
        (combined-work cmb items (.-stage wq) backlog)
        (individual-work items backlog)))))

(defn- new-workqueue
  [work]
  (let [cmb (combine work)]
    (assert (or (nil? cmb) (ifn? cmb)) "combine should be nil or a function")
    (->WorkQueue (ConcurrentLinkedQueue.) work (when cmb (FastList.)))))

(defn- enqueue-work!
  [^ConcurrentHashMap work-queues
   ^LinkedBlockingQueue event-queue
   ^Semaphore backlog
   work
   p
   cb]
  (let [k  (work-key work)]
    (assert (keyword? k) "work-key should return a keyword")
    (assert (or (nil? cb) (ifn? cb)) "callback should be nil or a function")
    (.acquire backlog)
    (try
      (.putIfAbsent work-queues k (new-workqueue work))
      (let [item                     (->WorkItem work p cb)
            ^WorkQueue wq            (.get work-queues k)
            ^ConcurrentLinkedQueue q (.-items wq)]
        (.offer q item)
        (.offer event-queue k))
      (catch Throwable e
        (.release backlog)
        (throw e)))))

(defprotocol IAsyncExecutor
  (start [_] "Start the async event loop")
  (stop [_] "Stop the async event loop")
  (running? [_] "Return true if AsyncExecutor is running")
  (exec [_ work] "Submit a work, get back a future"))

(deftype AsyncExecutor [^ExecutorService dispatcher
                        ^ExecutorService workers
                        ^LinkedBlockingQueue event-queue
                        ^ConcurrentHashMap work-queues ; work-key -> WorkQueue
                        ^AtomicBoolean running
                        ^Semaphore backlog]
  IAsyncExecutor
  (start [_]
    (letfn [(event-loop []
              (when (.get running)
                (let [k (.take event-queue)]
                  (when-not (.contains event-queue k) ; do nothing when busy
                    (when (.get running)
                      (.submit workers
                               ^Callable #(event-handler work-queues k backlog)))))
                (recur)))
            (init []
              (try (event-loop)
                   (catch Exception _
                     (when (.get running)
                       (.submit dispatcher ^Callable init)))))]
      (when-not (.get running)
        ;; Set running before scheduling init to avoid a startup race where
        ;; init checks running=false and exits before the loop starts.
        (.set running true)
        (.submit dispatcher ^Callable init))))
  (running? [_] (.get running))
  (stop [_]
    (.set running false)
    (.shutdownNow dispatcher)
    (.awaitTermination dispatcher 100 TimeUnit/MILLISECONDS)
    (.shutdownNow workers)
    (.awaitTermination workers 100 TimeUnit/MILLISECONDS))
  (exec [_ work]
    (let [p  (promise)
          cb (callback work)]
      (enqueue-work! work-queues event-queue backlog work p cb)
      (->AsyncResult p))))

(defn- async-worker-pool
  []
  (let [threads (.availableProcessors (Runtime/getRuntime))]
    (ThreadPoolExecutor.
      threads threads 0 TimeUnit/MILLISECONDS
      (ArrayBlockingQueue. (* 4 threads))
      (ThreadPoolExecutor$CallerRunsPolicy.))))

(defn- new-async-executor
  []
  (->AsyncExecutor (Executors/newSingleThreadExecutor)
                   (async-worker-pool)
                   (LinkedBlockingQueue.)
                   (ConcurrentHashMap.)
                   (AtomicBoolean. false)
                   (new-backlog-semaphore)))

(defonce executor-atom (atom nil))

(defn- new-executor
  []
  (let [new-e (new-async-executor)]
    (reset! executor-atom new-e)
    (start new-e)
    new-e))

(defn get-executor
  "access the async executor"
  []
  (locking executor-atom
    (let [e @executor-atom]
      (if (and e (running? e))
        e
        (new-executor)))))

(defn exec-noresult
  "Submit work without creating/returning a future. Useful when completion is
  tracked out-of-band."
  [executor work]
  (if (instance? AsyncExecutor executor)
    (let [^AsyncExecutor e executor
          cb             (callback work)]
      (enqueue-work! (.-work-queues e) (.-event-queue e) (.-backlog e) work nil cb)
      nil)
    (do
      (exec executor work)
      nil)))

(defn shutdown-executor []
  (locking executor-atom
    (when-let [e @executor-atom]
      (stop e)
      (reset! executor-atom nil))))
