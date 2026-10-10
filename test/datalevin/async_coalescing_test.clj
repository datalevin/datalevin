(ns datalevin.async-coalescing-test
  (:require [clojure.test :refer [deftest is]]
            [datalevin.async :as a]
            [datalevin.constants :as c]
            [datalevin.conn :as conn])
  (:import [datalevin.async AsyncExecutor]
           [java.util.concurrent ConcurrentLinkedQueue Semaphore]
           [java.util.concurrent.atomic AtomicBoolean]
           [org.eclipse.collections.impl.list.mutable FastList]))

(deftype CoalescingWork [value weight limit window staged batches cb]
  a/IAsyncWork
  (work-key [_] ::coalescing)
  (do-work [_] [value])
  (combine [_]
    (fn [works]
      (let [values (mapv #(.-value ^CoalescingWork %) works)
            weight (reduce + (map #(.-weight ^CoalescingWork %) works))]
        (reify a/IAsyncWork
          (work-key [_] ::coalescing)
          (do-work [_]
            (swap! batches conj {:values values :weight weight})
            values)
          (combine [_] nil)
          (callback [_] nil)))))
  (callback [_] cb)
  a/IBoundedAsyncWork
  (batch-weight [_]
    (when staged (deliver staged (Thread/currentThread)))
    weight)
  (max-batch-weight [_] limit)
  a/ICoalescingAsyncWork
  (coalesce-window-ms [_] window))

(defn- new-executor []
  ((ns-resolve 'datalevin.async 'new-async-executor)))

(deftest datalog-enables-the-bounded-default-window
  (let [work (conn/->AsyncDLTx nil [] nil nil)]
    (is (= 10 (a/coalesce-window-ms work)))
    (is (= 100000 (a/max-batch-weight work)))
    (binding [c/*datalog-async-coalesce-ms* 0]
      (is (zero? (a/coalesce-window-ms work))))))

(deftest late-arrivals-fill-the-batch-and-realize-before-callbacks
  (let [executor (new-executor)
        staged (promise)
        batches (atom [])
        results (atom [])
        callbacks (atom [])
        results-ready (promise)
        callback-done (promise)
        cb (fn [value]
             @results-ready
             (swap! callbacks conj
                    {:value value :realized (mapv realized? @results)})
             (when (= 2 (count @callbacks)) (deliver callback-done true)))
        first-result (a/exec executor
                             (->CoalescingWork 1 1 2 5000 staged batches cb))]
    (try
      (swap! results conj first-result)
      (a/start executor)
      (is (instance? Thread (deref staged 1000 ::timeout)))
      (swap! results conj
             (a/exec executor (->CoalescingWork 2 1 2 5000 nil batches cb)))
      (deliver results-ready true)
      (is (= [[1 2] [1 2]] (mapv #(deref % 1000 ::timeout) @results)))
      (is (= true (deref callback-done 1000 ::timeout)))
      (is (= [{:values [1 2] :weight 2}] @batches))
      (is (every? #(every? true? (:realized %)) @callbacks))
      (finally
        (deliver results-ready true)
        (a/stop executor)))))

(deftest full-and-oversized-batches-run-without-waiting
  (let [executor (new-executor)
        batches (atom [])
        results (mapv #(a/exec executor
                              (->CoalescingWork % % 3 5000 nil batches nil))
                      [3 7])]
    (try
      (a/start executor)
      (is (= [[3] [7]] (mapv #(deref % 1000 ::timeout) results)))
      (is (= [3 7] (mapv :weight @batches)))
      (finally (a/stop executor)))))

(deftest sparse-and-residual-work-finishes-at-the-deadline
  (let [executor (new-executor)
        batches (atom [])
        results (mapv #(a/exec executor
                              (->CoalescingWork % 1 3 20 nil batches nil))
                      (range 4))]
    (try
      (a/start executor)
      (is (= [[0 1 2] [0 1 2] [0 1 2] [3]]
             (mapv #(deref % 1000 ::timeout) results)))
      (is (= [3 1] (mapv :weight @batches)))
      (finally (a/stop executor)))))

(deftest continuous-arrivals-do-not-extend-the-deadline
  (let [items (ConcurrentLinkedQueue.)
        stage (FastList.)
        staged (promise)
        done (promise)
        producing (AtomicBoolean. true)
        first-work (->CoalescingWork 0 1 Long/MAX_VALUE 100 staged (atom []) nil)
        stage! (ns-resolve 'datalevin.async 'stage-combined-work!)
        worker (Thread.
                 ^Runnable
                 #(try
                    (stage! items stage first-work)
                    (deliver done (.size stage))
                    (catch Throwable e (deliver done e))))
        producer (Thread.
                   ^Runnable
                   #(loop [n 1]
                      (when (.get producing)
                        (.offer items
                                (a/->WorkItem
                                  (->CoalescingWork n 1 Long/MAX_VALUE 100
                                                    nil nil nil)
                                  nil nil))
                        (Thread/sleep 1)
                        (recur (inc n)))))]
    (.offer items (a/->WorkItem first-work nil nil))
    (try
      (.start worker)
      (is (instance? Thread (deref staged 1000 ::timeout)))
      (.start producer)
      (let [result (deref done 1000 ::timeout)]
        (is (integer? result))
        (when (integer? result) (is (> result 1)))
        (is (.isAlive producer)))
      (finally
        (.set producing false)
        (.interrupt worker)
        (.join worker 1000)
        (.join producer 1000)))))

(deftest shutdown-interrupts-the-window-and-releases-admission
  (let [^AsyncExecutor executor (new-executor)
        ^Semaphore backlog (.-backlog executor)
        permits (.availablePermits backlog)
        staged (promise)
        callback-done (promise)
        result (a/exec executor
                       (->CoalescingWork 1 1 3 5000 staged (atom [])
                                         #(deliver callback-done %)))]
    (try
      (a/start executor)
      (is (instance? Thread (deref staged 1000 ::timeout)))
      (a/stop executor)
      (is (= [1] (deref result 1000 ::timeout)))
      (is (= [1] (deref callback-done 1000 ::timeout)))
      (is (= permits (.availablePermits backlog)))
      (finally (a/stop executor)))))

(deftest bounded-work-without-coalescing-remains-immediate
  (let [items (ConcurrentLinkedQueue.)
        stage (FastList.)
        work (reify a/IBoundedAsyncWork
               (batch-weight [_] 1)
               (max-batch-weight [_] 3))]
    (.offer items (a/->WorkItem work nil nil))
    ((ns-resolve 'datalevin.async 'stage-combined-work!) items stage work)
    (is (= 1 (.size stage)))
    (is (.isEmpty items))))
