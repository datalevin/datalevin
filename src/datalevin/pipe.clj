;;
;; Copyright (c) Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns ^:no-doc datalevin.pipe
  "Tuple pipes for query execution"
  (:refer-clojure :exclude [update assoc])
  (:require
   [datalevin.constants :as c]
   [datalevin.timeout :as timeout]
   [datalevin.util :as u])
  (:import
   [java.util List Collection HashMap]
   [java.util.concurrent LinkedBlockingQueue Semaphore TimeUnit]
   [org.eclipse.collections.impl.list.mutable FastList]))

(def ^:private ^ThreadLocal batch-buffer-tl
  (ThreadLocal.))

(defn batch-buffer
  "Returns a pre-allocated, thread-local FastList for batching.
   The buffer is cleared before returning. Caller should not hold
   references to it across batch operations."
  ^FastList []
  (let [^FastList buf (.get batch-buffer-tl)]
    (if buf
      (do (.clear buf) buf)
      (let [buf (FastList. (int c/query-pipe-batch-size))]
        (.set batch-buffer-tl buf)
        buf))))

(defn- enqueue
  [^LinkedBlockingQueue queue o]
  (try
    (.put queue o) ;; block when full
    true
    (catch InterruptedException e
      (.interrupt (Thread/currentThread))
      (u/raise "Interrupted while enqueuing to pipe" e {:object o}))))

(deftype TupleBatch [^List tuples ^long start ^long end])

(defn- acquire-permits
  [^Semaphore permits ^long n]
  (try
    (.acquire permits (int n))
    (catch InterruptedException e
      (.interrupt (Thread/currentThread))
      (u/raise "Interrupted while enqueuing to pipe" e {:tuple-count n}))))

(defn- enqueue-batches
  [^LinkedBlockingQueue queue ^Semaphore permits ^List tuples ^long batch-size]
  (let [n (.size tuples)]
    (loop [start 0]
      (when (< start n)
        (let [end (min n (+ start batch-size))
              cnt (- end start)]
          (acquire-permits permits cnt)
          (try
            (enqueue queue (TupleBatch. tuples start end))
            (catch Throwable e
              (.release permits (int cnt))
              (throw e)))
          (recur end))))))

(defn- release-batch
  [^Semaphore permits o]
  (when (instance? TupleBatch o)
    (let [^TupleBatch batch o]
      (.release permits (int (- (.-end batch) (.-start batch)))))))

(defprotocol IBatchedQueue
  (-flush [this])
  (-add [this tuple])
  (-add-all [this tuples])
  (-finish [this])
  (-produce [this])
  (-reset [this]))

(deftype BatchedQueue [^LinkedBlockingQueue queue
                       ^Semaphore permits
                       ^long batch-size
                       ^:unsynchronized-mutable ^FastList producer
                       ^:unsynchronized-mutable ^TupleBatch consumer
                       ^:unsynchronized-mutable ^long consumer-idx]
  IBatchedQueue
  (-flush [_]
    (timeout/assert-time-left)
    (when (pos? (.size producer))
      (enqueue-batches queue permits producer batch-size)
      (set! producer (FastList. (int batch-size)))))
  (-add [this tuple]
    (.add producer tuple)
    (when (>= (.size producer) batch-size)
      (-flush this))
    true)
  (-add-all [this tuples]
    (-flush this)
    (when (pos? (.size ^List tuples))
      (enqueue-batches queue permits tuples batch-size))
    true)
  (-finish [this]
    (-flush this)
    (enqueue queue :datalevin/end-scan))
  (-produce [_]
    (timeout/assert-time-left)
    (loop []
      (if consumer
        (let [i      consumer-idx
              end    (.-end consumer)
              tuple  (.get ^List (.-tuples consumer) i)
              next-i (inc i)]
          (if (= next-i end)
            (do (.release permits
                          (int (- end (.-start consumer))))
                (set! consumer nil)
                (set! consumer-idx 0))
            (set! consumer-idx next-i))
          tuple)
        (let [remaining (timeout/time-left)
              wait-ms   (if remaining
                          (max 1 (min (long c/query-pipe-timeout)
                                      (long remaining)))
                          (long c/query-pipe-timeout))
              o (.poll queue wait-ms
                       TimeUnit/MILLISECONDS)]
          (when (nil? o)
            (timeout/assert-time-left)
            (u/raise "Pipe take timed out waiting for producer"
                     {:timeout wait-ms}))
          (when-not (identical? :datalevin/end-scan o)
            (set! consumer o)
            (set! consumer-idx (.-start consumer))
            (recur))))))
  (-reset [_]
    (.clear producer)
    (when consumer
      (release-batch permits consumer)
      (set! consumer nil)
      (set! consumer-idx 0))
    (loop []
      (when-let [o (.poll queue)]
        (release-batch permits o)
        (recur)))))

(defn- batched-queue
  []
  (let [capacity   (long c/query-pipe-capacity)
        batch-size (Math/max 1 (Math/min capacity
                                         (long c/query-pipe-batch-size)))]
    (BatchedQueue. (LinkedBlockingQueue. capacity)
                   (Semaphore. (int capacity))
                   batch-size
                   (FastList. (int batch-size))
                   nil 0)))

(defprotocol ITuplePipe
  (pipe? [this] "test if implements this protocol")
  (finish [this] "send a sentinel to indicate end of this pipe")
  (produce [this]
    "take a tuple from the pipe, block if there is nothing to take (up to
     c/query-pipe-timeout), if encounter :datalevin/end-scan, return nil")
  (add-batch [this tuples]
    "Add a tuple batch without copying. The caller must not mutate it while the
     pipe is consuming it.")
  (drain-to [this sink] "pour all remaining content into sink")
  (reset [this] "reset the pipe for next round of operation")
  (total [this] "return the total number of tuples pass through the pipe"))

(extend-type Object
  ITuplePipe
  (pipe? [_] false)
  (add-batch [this tuples] (.addAll ^Collection this ^Collection tuples)))

(extend-type nil
  ITuplePipe
  (pipe? [_] false)
  (add-batch [_ _] false))

(deftype TuplePipe [^BatchedQueue state]
  ITuplePipe
  (pipe? [_] true)
  (finish [_] (-finish state))
  (produce [_] (-produce state))
  (add-batch [_ tuples] (-add-all state tuples))
  (drain-to [this sink]
    (loop [tuple (produce this)]
      (when tuple
        (.add ^Collection sink tuple)
        (recur (produce this)))))
  (reset [_] (-reset state))
  (total [_] 0)

  Collection
  (add [_ o] (-add state o))
  (addAll [_ l]
    (-add-all state (FastList. ^Collection l))))

(deftype CountedTuplePipe [^BatchedQueue state
                           ^:unsynchronized-mutable ^long total]
  ITuplePipe
  (pipe? [_] true)
  (finish [_] (-finish state))
  (produce [_]
    (let [o (-produce state)]
      (when o
        (set! total (u/long-inc total))
        o)))
  (add-batch [_ tuples] (-add-all state tuples))
  (drain-to [this sink]
    (loop [tuple (produce this)]
      (when tuple
        (.add ^Collection sink tuple)
        (recur (produce this)))))
  (reset [_] (-reset state))
  (total [_] total)

  Collection
  (add [_ o] (-add state o))
  (addAll [_ o]
    (-add-all state (FastList. ^Collection o))))

(defn tuple-pipe
  []
  (->TuplePipe (batched-queue)))

(defn counted-tuple-pipe
  []
  (->CountedTuplePipe (batched-queue) 0))

(deftype ListTuplePipe [^List tuples
                        ^:unsynchronized-mutable ^long i]
  ITuplePipe
  (pipe? [_] true)
  (finish [_] nil)
  (produce [_]
    (when (< i (.size tuples))
      (when (zero? (rem i (max 1 (long c/query-pipe-batch-size))))
        (timeout/assert-time-left))
      (let [tuple (.get tuples i)]
        (set! i (inc i))
        tuple)))
  (add-batch [_ _]
    (u/raise "Cannot add tuples to a list input pipe" {}))
  (drain-to [this sink]
    (loop [tuple (produce this)]
      (when tuple
        (.add ^Collection sink tuple)
        (recur (produce this)))))
  (reset [_]
    (set! i 0))
  (total [_] 0))

(defn list-tuple-pipe
  [tuples]
  (ListTuplePipe. tuples 0))

(defn remove-end-scan
  [tuples]
  (if (.isEmpty ^Collection tuples)
    tuples
    (let [size (.size ^List tuples)
          s-1  (dec size)
          l    (.get ^List tuples s-1)]
      (if (identical? :datalevin/end-scan l)
        (do (.remove ^List tuples s-1)
            (recur tuples))
        tuples))))

(deftype OrJoinTuplePipe [^List tuples
                          ^long bound-idx
                          ^HashMap or-by-bound
                          ^long free-var-idx
                          ^long tuple-len
                          ^:unsynchronized-mutable ^long i
                          ^:unsynchronized-mutable ^objects current
                          ^:unsynchronized-mutable ^List matches
                          ^:unsynchronized-mutable ^long j]
  ITuplePipe
  (pipe? [_] true)
  (finish [_] nil)
  (produce [_]
    (loop []
      (if (and matches (< j (.size ^List matches)))
        (let [^objects or-tuple (.get ^List matches j)
              fv                (aget or-tuple free-var-idx)
              ^objects joined   (object-array (inc tuple-len))]
          (System/arraycopy current 0 joined 0 tuple-len)
          (aset joined tuple-len fv)
          (set! j (inc j))
          joined)
        (when (< i (.size ^List tuples))
          (let [^objects in-tuple (.get ^List tuples i)
                bv                (aget in-tuple bound-idx)
                ^List m           (.get ^HashMap or-by-bound bv)]
            (set! i (inc i))
            (if (and m (pos? (.size m)))
              (do (set! current in-tuple)
                  (set! matches m)
                  (set! j 0)
                  (recur))
              (do (set! current nil)
                  (set! matches nil)
                  (set! j 0)
                  (recur))))))))
  (add-batch [_ _]
    (u/raise "Cannot add tuples to an or-join input pipe" {}))
  (drain-to [this sink]
    (loop [t (produce this)]
      (when t
        (.add ^Collection sink t)
        (recur (produce this)))))
  (reset [_]
    (set! i 0)
    (set! current nil)
    (set! matches nil)
    (set! j 0))
  (total [_] 0))

(defn or-join-tuple-pipe
  [tuples bound-idx or-by-bound free-var-idx tuple-len]
  (OrJoinTuplePipe. tuples bound-idx or-by-bound free-var-idx
                    tuple-len 0 nil nil 0))
