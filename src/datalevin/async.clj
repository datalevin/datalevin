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
  (:import
   [java.util.concurrent.atomic AtomicBoolean]
   [java.util.concurrent Executors ExecutorService LinkedBlockingQueue
    ConcurrentLinkedQueue ConcurrentHashMap Callable TimeUnit
    ThreadPoolExecutor ArrayBlockingQueue ThreadPoolExecutor$CallerRunsPolicy]
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

(defn- do-work*
  [work]
  (try [:ok (do-work work)]
       (catch Exception e
         [:err e])))

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
  [^ConcurrentLinkedQueue items]
  (loop []
    (when (.peek items)
      (let [^WorkItem item (.poll items)
            res            (do-work* (.-work item))
            [status payload] res
            cb             (.-cb item)]
        (when (and cb (identical? status :ok))
          (cb payload))
        (when-let [p (.-promise item)]
          (deliver p res)))
      (recur))))

(defn- combined-work
  [cmb ^ConcurrentLinkedQueue items ^FastList stage]
  (.clear stage)
  (loop []
    (when (.peek items)
      (let [^WorkItem item (.poll items)]
        (.add stage item))
      (recur)))
  (let [res              (do-work* (cmb (mapv #(.-work ^WorkItem %) stage)))
        [status payload] res]
    (dotimes [i (.size stage)]
      (let [^WorkItem item (.get stage i)
            cb             (.-cb item)]
        (when (and cb (identical? status :ok))
          (cb payload))
        (when-let [p (.-promise item)]
          (deliver p res))))))

(defn- event-handler
  [^ConcurrentHashMap work-queues k]
  (let [^WorkQueue wq                (.get work-queues k)
        ^ConcurrentLinkedQueue items (.-items wq)
        first-work                   (.-fw wq)]
    (locking items
      (if-let [cmb (combine first-work)]
        (combined-work cmb items (.-stage wq))
        (individual-work items)))))

(defn- new-workqueue
  [work]
  (let [cmb (combine work)]
    (assert (or (nil? cmb) (ifn? cmb)) "combine should be nil or a function")
    (->WorkQueue (ConcurrentLinkedQueue.) work (when cmb (FastList.)))))

(defn- enqueue-work!
  [^ConcurrentHashMap work-queues
   ^LinkedBlockingQueue event-queue
   work
   p
   cb]
  (let [k  (work-key work)]
    (assert (keyword? k) "work-key should return a keyword")
    (assert (or (nil? cb) (ifn? cb)) "callback should be nil or a function")
    (.putIfAbsent work-queues k (new-workqueue work))
    (let [item                     (->WorkItem work p cb)
          ^WorkQueue wq            (.get work-queues k)
          ^ConcurrentLinkedQueue q (.-items wq)]
      (.offer q item)
      (.offer event-queue k))))

(defprotocol IAsyncExecutor
  (start [_] "Start the async event loop")
  (stop [_] "Stop the async event loop")
  (running? [_] "Return true if AsyncExecutor is running")
  (exec [_ work] "Submit a work, get back a future"))

(deftype AsyncExecutor [^ExecutorService dispatcher
                        ^ExecutorService workers
                        ^LinkedBlockingQueue event-queue
                        ^ConcurrentHashMap work-queues ; work-key -> WorkQueue
                        ^AtomicBoolean running]
  IAsyncExecutor
  (start [_]
    (letfn [(event-loop []
              (when (.get running)
                (let [k (.take event-queue)]
                  (when-not (.contains event-queue k) ; do nothing when busy
                    (when (.get running)
                      (.submit workers
                               ^Callable #(event-handler work-queues k)))))
                (recur)))
            (init []
              (try (event-loop)
                   (catch Exception _
                     (when (.get running)
                       (.submit dispatcher ^Callable init)))))]
      (when-not (.get running)
        (.submit dispatcher ^Callable init)
        (.set running true))))
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
      (enqueue-work! work-queues event-queue work p cb)
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
                   (AtomicBoolean. false)))

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
  (let [e @executor-atom]
    (if (and e (running? e))
      e
      (new-executor))))

(defn exec-noresult
  "Submit work without creating/returning a future. Useful when completion is
  tracked out-of-band."
  [executor work]
  (if (instance? AsyncExecutor executor)
    (let [^AsyncExecutor e executor
          cb             (callback work)]
      (enqueue-work! (.-work-queues e) (.-event-queue e) work nil cb)
      nil)
    (do
      (exec executor work)
      nil)))

(defn shutdown-executor [] (when-let [e @executor-atom] (stop e)))
