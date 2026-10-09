;; Copyright (c) Huahai Yang. All rights reserved.
;; Distributed under the Eclipse Public License 2.0.
(ns ^:no-doc datalevin.tx-group.compat
  "Adapter for native-only Datalog and server writers. Adapter state
  (batched?, request count, confirmations) is thread-local instead of a dynamic
  var; arbitrary caller bindings are still captured here, lazily, only when a
  queued request may run on a foreign collector leader."
  (:require [datalevin.tx-group :as group]))

(def ^:dynamic *enabled?* true)

(def ^:private ^"[J" default-state (long-array [0 1]))
(def ^:private ^ThreadLocal state
  "Per-thread [batched? request-count]. ThreadLocals replace dynamic vars so a
  collected turn never captures or conveys caller bindings across threads."
  (ThreadLocal.))
(def ^:private ^ThreadLocal confirmations
  "Per-thread post-commit callback sink."
  (ThreadLocal.))

(def create group/create)
(def collect! group/collect!)

(defn batched?
  "Whether the current adapter turn is a collected batch."
  []
  (let [^"[J" s (or (.get state) default-state)]
    (not (zero? (aget s 0)))))

(defn request-count
  "Logical request count visible to the current operation."
  ^long []
  (let [^"[J" s (or (.get state) default-state)]
    (aget s 1)))

(defn- set-request-count! [n]
  (when-let [^"[J" s (.get state)]
    (aset s 1 (long n))))

(defn- with-state* [batched? ^long count f]
  (let [^"[J" previous (.get state)]
    (.set state (long-array [(if batched? 1 0) count]))
    (try (f) (finally (.set state previous)))))

(defn with-state
  "Run f with this thread's adapter state set to batched? and count. A submitter
  can present its own turn state without a dynamic binding."
  [batched? count f]
  (with-state* batched? (long count) f))

(defn confirm-after-commit! [f]
  (if-let [sink (.get confirmations)]
    (vswap! sink conj f)
    (f)))

(defn- capture-commit [f]
  (let [sink (volatile! nil)
        previous (.get confirmations)]
    (.set confirmations sink)
    (try
      (let [value (f)]
        (if-let [callbacks @sink]
          (group/committed value #(doseq [confirm (reverse callbacks)] (confirm)))
          value))
      (finally (.set confirmations previous)))))

(defn with-confirmation [f]
  (if (.get confirmations) (f) (group/await-commit (capture-commit f))))

(defn- run-local-op
  [op context submitted-batched? submitted-request-count]
  ;; The batch turn set adapter state; present the submitter's values for this
  ;; one operation without pushing a dynamic binding frame.
  (with-state* submitted-batched? submitted-request-count #(op context)))

(def ^:private specialization-context
  "Constant batch-specialization key. Adapter state is thread-local, so a
  collected request no longer carries a captured binding context."
  (Object.))

(defn submit!
  ([collector run-transaction op]
   (submit! collector run-transaction op 0))
  ([collector run-transaction op delay-nanos]
   (let [submitter (Thread/currentThread)
         submitted-batched? (batched?)
         submitted-request-count (request-count)
         data (or (::data (meta op)) (::group/data (meta op)))
         run (fn [execute]
               (with-state* true (group/request-count execute)
                 (fn []
                   (group/observe-collection! execute set-request-count!)
                   (capture-commit #(run-transaction execute)))))
         direct-body (fn [context]
                       ;; The batch runner rebinds adapter state; arbitrary
                       ;; caller bindings stay on the submitting thread.
                       (run-local-op op context submitted-batched?
                                     submitted-request-count))
         operation (with-meta direct-body
                     (assoc (meta op) ::group/data data
                            ::group/context specialization-context))
         queued-pair
         (fn []
           ;; Forced only after this caller loses the leadership race and the
           ;; request may run on another collector leader. Capture arbitrary
           ;; caller bindings on this thread before publishing, then restore
           ;; them only on a foreign thread. Adapter state (batched?/request
           ;; count) travels thread-locally, so a same-thread leader pays no
           ;; binding map.
           (let [bindings (get-thread-bindings)
                 foreign? #(not (identical? submitter (Thread/currentThread)))
                 operation (with-meta
                             (fn [context]
                               (if (foreign?)
                                 (with-bindings bindings (direct-body context))
                                 (direct-body context)))
                             (assoc (meta op) ::group/data data
                                    ::group/context specialization-context))
                 runner (with-meta
                          (fn [execute]
                            (if (foreign?)
                              (with-bindings bindings (run execute))
                              (run execute)))
                          {::group/context specialization-context})]
             [runner operation]))]
     (group/submit-adaptive! collector run operation queued-pair delay-nanos))))
