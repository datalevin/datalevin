;; Copyright (c) Huahai Yang. All rights reserved.
;; Distributed under the Eclipse Public License 2.0.
(ns ^:no-doc datalevin.tx-state.lifetime
  "Environment lifetime barrier. Fencing never frees native resources."
  (:import [java.lang AutoCloseable]
           [datalevin.utl NativeUsers]
           [java.util.concurrent.atomic AtomicBoolean]
           [java.util.concurrent.locks Condition ReentrantLock]
           [clojure.lang Volatile]))

;; A deftype, not a map: the borrow boundary reads `users` and `teardown-thread`
;; on every eager read, and array-map lookups there are measurable. External
;; callers only use the functions below. Field hints do not propagate to
;; `.-field`, so each access binds an explicitly hinted local.
(deftype Lifetime [^ReentrantLock lock
                   ^Condition changed
                   ^ThreadLocal call-owner
                   ^Volatile teardown-thread
                   ^NativeUsers users
                   ^Volatile state])

(defn ^:redef nano-time ^long [] (System/nanoTime))

(defn teardown-owner? [^Lifetime lifetime]
  (let [^Volatile tt (.-teardown-thread lifetime)]
    (identical? (Thread/currentThread) @tt)))

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
    (Lifetime. lock changed (ThreadLocal.) (volatile! nil)
               (NativeUsers. lock changed) (volatile! :open))))

(defn instrument
  "Diagnostic seam: a lifetime over this runtime's state and teardown thread with
  a supplied lock, condition and native-user accounting, so a test can observe
  shared-lock acquisition on the borrow path."
  ^Lifetime [^Lifetime lifetime
             ^ReentrantLock lock
             ^Condition changed
             ^NativeUsers users]
  (Lifetime. lock changed (.-call-owner lifetime) (.-teardown-thread lifetime)
             users (.-state lifetime)))

(defn borrow!
  "Register native use with a reusable thread-local counter. Exactly one close
  per successful borrow is required, possibly on another thread. Registration
  and close coordinate under the lifecycle lock; steady-state reads do not."
  ^AutoCloseable [^Lifetime lifetime]
  (let [^NativeUsers users (.-users lifetime)]
    (try (.borrow users)
         (catch IllegalStateException e
           (throw (ex-info "Native environment is fenced"
                           {:error :txlog/native-fenced :retryable? false} e))))))

(defn state
  "Return lifecycle diagnostics without acquiring any native resource."
  [^Lifetime lifetime]
  (let [^ReentrantLock lock (.-lock lifetime)
        ^NativeUsers users (.-users lifetime)
        ^Volatile s (.-state lifetime)]
    (with-lock lock
      {:phase @s :native-users (.countActive users)})))

(defn enter!
  "Register native use before touching the environment. Retain the returned
  lease through transaction/cursor cleanup, including blocked native calls."
  ^AutoCloseable [^Lifetime lifetime]
  (let [lease (borrow! lifetime)
        released (AtomicBoolean. false)]
    (reify AutoCloseable
      (close [_]
        (when (.compareAndSet released false true) (.close lease))))))

(defn fence!
  "Prevent new native users, preserving resources held by existing users."
  [^Lifetime lifetime]
  (let [^ReentrantLock lock (.-lock lifetime)
        ^Condition changed (.-changed lifetime)
        ^NativeUsers users (.-users lifetime)
        ^Volatile s (.-state lifetime)]
    (with-lock lock
      (when (= :open @s)
        (.fence users)
        (vreset! s :fenced))
      (.signalAll changed))))

(defmacro with-use
  "Retain one native/IO lease through a reentrant call chain. Ownership belongs
  to the guard and the current Java thread; it cannot propagate to futures."
  [guard & body]
  `(let [^Lifetime guard# ~guard
         ^ThreadLocal owner# (.-call-owner guard#)]
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
  [^Lifetime lifetime timeout-ms teardown]
  (let [^ReentrantLock lock (.-lock lifetime)
        ^Condition changed (.-changed lifetime)
        ^NativeUsers users (.-users lifetime)
        ^Volatile s (.-state lifetime)
        ^Volatile tt (.-teardown-thread lifetime)
        limit (deadline timeout-ms)
        run? (with-lock lock
               (when (= :open @s)
                 (.fence users)
                 (vreset! s :fenced))
               (loop []
                 (let [phase @s
                       active (.countActive users)
                       remaining (- limit (nano-time))]
                   (cond
                     (= :closed phase) false
                     (= :teardown-failed phase)
                     (throw (ex-info "Native teardown failed; restart the process"
                                     {:error :txlog/native-teardown-failed
                                      :process-restart-required? true}))
                     (and (zero? active) (= :fenced phase))
                     (do (vreset! s :closing) true)
                     (not (pos? remaining))
                     (throw (ex-info "Native users have not quiesced; environment remains fenced"
                                     {:error :txlog/native-not-quiescent
                                      :native-users active
                                      :process-restart-required? true}))
                     :else
                     (do
                       (try
                         (.awaitNanos changed remaining)
                         (catch InterruptedException e
                           (.interrupt (Thread/currentThread))
                           (throw (ex-info "Interrupted while draining native users"
                                           {:error :txlog/native-not-quiescent
                                            :native-users active
                                            :process-restart-required? true} e))))
                       (recur))))))]
    (when run?
      (try
        (vreset! tt (Thread/currentThread))
        (try (teardown) (finally (vreset! tt nil)))
        (with-lock lock
          (vreset! s :closed)
          (.signalAll changed))
        (catch Throwable e
          (with-lock lock
            (vreset! s :teardown-failed)
            (.signalAll changed))
          (throw e))))
    nil))
