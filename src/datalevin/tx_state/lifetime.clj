;; Copyright (c) Huahai Yang. All rights reserved.
;; Distributed under the Eclipse Public License 2.0.
(ns ^:no-doc datalevin.tx-state.lifetime
  "Environment lifetime barrier. Fencing never frees native resources."
  (:import [java.lang AutoCloseable]
           [datalevin.utl NativeUsers]
           [java.util.concurrent.atomic AtomicBoolean]
           [java.util.concurrent.locks Condition ReentrantLock]))

(defn ^:redef nano-time ^long [] (System/nanoTime))

(defn teardown-owner? [lifetime]
  (identical? (Thread/currentThread) @(:teardown-thread lifetime)))

(defmacro with-lock [lock & body]
  `(let [^ReentrantLock lock# ~lock]
     (.lock lock#)
     (try ~@body (finally (.unlock lock#)))))

(defn deadline
  "An absolute monotonic deadline; nil means no deadline."
  ^long [timeout-ms]
  (if (nil? timeout-ms)
    Long/MAX_VALUE
    (let [nanos (Math/multiplyExact (max 0 (long timeout-ms)) (long 1000000))]
      (Math/addExact (nano-time) nanos))))

(defn create
  "Create a lifetime barrier before publishing the environment to callers."
  []
  (let [lock (ReentrantLock.)
        changed (.newCondition lock)]
    {:lock lock :changed changed
     :call-owner (ThreadLocal.) :teardown-thread (volatile! nil)
     :users (NativeUsers. lock changed) :state (volatile! :open)}))

(defn borrow!
  "Register native use with a reusable thread-local counter. Exactly one close
  per successful borrow is required, possibly on another thread. Registration
  and close coordinate under the lifecycle lock; steady-state reads do not."
  ^AutoCloseable [lifetime]
  (try (.borrow ^NativeUsers (:users lifetime))
       (catch IllegalStateException e
         (throw (ex-info "Native environment is fenced"
                         {:error :txlog/native-fenced :retryable? false} e)))))

(defn state
  "Return lifecycle diagnostics without acquiring any native resource."
  [lifetime]
  (with-lock (:lock lifetime)
    {:phase @(:state lifetime) :native-users (.countActive ^NativeUsers (:users lifetime))}))

(defn enter!
  "Register native use before touching the environment. Retain the returned
  lease through transaction/cursor cleanup, including blocked native calls."
  ^AutoCloseable [lifetime]
  (let [lease (borrow! lifetime)
        released (AtomicBoolean. false)]
    (reify AutoCloseable
      (close [_]
        (when (.compareAndSet released false true) (.close lease))))))

(defn fence!
  "Prevent new native users, preserving resources held by existing users."
  [lifetime]
  (with-lock (:lock lifetime)
    (when (= :open @(:state lifetime))
      (.fence ^NativeUsers (:users lifetime))
      (vreset! (:state lifetime) :fenced))
    (.signalAll ^Condition (:changed lifetime))))

(defmacro with-use
  "Retain one native/IO lease through a reentrant call chain. Ownership belongs
  to the guard and the current Java thread; it cannot propagate to futures."
  [guard & body]
  `(let [guard# ~guard
         ^ThreadLocal owner# (:call-owner guard#)]
     (if (or (.get owner#) (teardown-owner? guard#))
       (do ~@body)
       (with-open [~(with-meta 'lease# {:tag 'java.lang.AutoCloseable}) (enter! guard#)]
         (.set owner# true)
         (try ~@body (finally (.remove owner#)))))))

(defn close!
  "Fence, drain native users, then invoke teardown exactly once. A timeout or
  interruption leaves the runtime fenced without invoking teardown. The caller
  must retain its environment registry entry and protocol lease on failure.
  Teardown must free native resources before unregistering/releasing the lease."
  [lifetime timeout-ms teardown]
  (let [limit (deadline timeout-ms)
        run? (with-lock (:lock lifetime)
               (when (= :open @(:state lifetime))
                 (.fence ^NativeUsers (:users lifetime))
                 (vreset! (:state lifetime) :fenced))
               (loop []
                 (let [phase @(:state lifetime)
                       users (.countActive ^NativeUsers (:users lifetime))
                       remaining (- limit (nano-time))]
                   (cond
                     (= :closed phase) false
                     (= :teardown-failed phase)
                     (throw (ex-info "Native teardown failed; restart the process"
                                     {:error :txlog/native-teardown-failed
                                      :process-restart-required? true}))
                     (and (zero? users) (= :fenced phase))
                     (do (vreset! (:state lifetime) :closing) true)
                     (not (pos? remaining))
                     (throw (ex-info "Native users have not quiesced; environment remains fenced"
                                     {:error :txlog/native-not-quiescent
                                      :native-users users
                                      :process-restart-required? true}))
                     :else
                     (do
                       (try
                         (.awaitNanos ^Condition (:changed lifetime) remaining)
                         (catch InterruptedException e
                           (.interrupt (Thread/currentThread))
                           (throw (ex-info "Interrupted while draining native users"
                                           {:error :txlog/native-not-quiescent
                                            :native-users users
                                            :process-restart-required? true} e))))
                       (recur))))))]
    (when run?
      (try
        (vreset! (:teardown-thread lifetime) (Thread/currentThread))
        (try (teardown) (finally (vreset! (:teardown-thread lifetime) nil)))
        (with-lock (:lock lifetime)
          (vreset! (:state lifetime) :closed)
          (.signalAll ^Condition (:changed lifetime)))
        (catch Throwable e
          (with-lock (:lock lifetime)
            (vreset! (:state lifetime) :teardown-failed)
            (.signalAll ^Condition (:changed lifetime)))
          (throw e))))
    nil))
