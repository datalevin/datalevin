;; Copyright (c) Huahai Yang. All rights reserved.
;; Distributed under the Eclipse Public License 2.0.
(ns ^:no-doc datalevin.tx-group
  "Collect synchronous writes before acquiring the native writer. A group is
  executed in one native transaction and acknowledged only after durable commit."
  (:refer-clojure :exclude [run!])
  (:import [java.util.concurrent ConcurrentLinkedQueue]
           [java.util.concurrent.locks ReentrantLock]
           [org.eclipse.collections.impl.list.mutable FastList]))

(def ^:dynamic *enabled?* true)

(deftype Request [op result])
(deftype Group [^ReentrantLock lock ^ConcurrentLinkedQueue queue ^long limit])

(defn create
  "Create one bounded-batch admission queue for a store and execution path."
  [limit]
  (Group. (ReentrantLock. true) (ConcurrentLinkedQueue.) (max 1 (long limit))))

(defn execute
  "Evaluate a group's requests against a private writing context. Tag only
  body failures as retryable: commit/flush failures must never retry the group."
  [^FastList requests context]
  (let [n (.size requests)
        results (object-array n)]
    (dotimes [idx n]
      (let [^Request request (.get requests idx)]
        (try
          (aset results idx ((.-op request) context))
          (catch Throwable t
            (throw (ex-info "Grouped transaction body failed"
                            (assoc (ex-data t) ::body-failure true) t))))))
    results))

(defn- complete!
  [^FastList requests results error]
  (dotimes [idx (.size requests)]
    (let [^Request request (.get requests idx)]
      (vreset! (.-result request)
               (if error [false error] [true (aget ^objects results idx)])))))

(defn- run!
  [run-group ^FastList requests]
  (try
    (complete! requests (run-group requests) nil)
    (catch Throwable t
      (if (::body-failure (ex-data t))
        (if (= 1 (.size requests))
          (complete! requests nil (ex-cause t))
          ;; The outer transaction has aborted. Isolate an invalid request so
          ;; it cannot fail unrelated callers. User update functions already
          ;; have the same side-effect-free requirement as map-resize retries.
          (dotimes [idx (.size requests)]
            (run! run-group (doto (FastList. 1) (.add (.get requests idx))))))
        (complete! requests nil t)))))

(defn submit!
  "Run op in a durable group, preserving the submitting thread's bindings.
  run-group owns native transaction lifetime and publishes state before return.
  No timer, worker handoff, or early acknowledgment is involved. The next caller
  holding the admission lock drains requests accumulated during the prior commit."
  [^Group group run-group op]
  (let [result (volatile! nil)
        request (Request. (bound-fn [context] (op context)) result)
        queue (.-queue group)
        lock (.-lock group)]
    (.add queue request)
    (.lock lock)
    (try
      (loop []
        (when-not @result
          (let [requests (FastList.)]
            (loop [n 0]
              (when (< n (.-limit group))
                (when-let [request (.poll queue)]
                  (.add requests request)
                  (recur (inc n)))))
            (run! run-group requests)
            (recur))))
      (finally (.unlock lock)))
    (let [[ok? value] @result]
      (if ok? value (throw ^Throwable value)))))
