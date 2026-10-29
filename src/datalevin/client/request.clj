;; Copyright (c) Huahai Yang. All rights reserved.
;; Distributed under the Eclipse Public License 2.0.
(ns ^:no-doc datalevin.client.request
  "A caller-owned budget shared by transport, restoration, and HA attempts."
  (:require [datalevin.command :as cmd])
  (:import [java.io Closeable]
           [java.util Set]
           [java.util.concurrent.atomic AtomicBoolean AtomicReference]))

(def ^:dynamic *request* nil)
(def ^:dynamic *stage-deadline* nil)

(def ^:dynamic *transport-owners*
  "Enclosing recovery attempts that own transports until publication."
  nil)

(defn untrack-transport!
  "Forget a closed provisional socket or selector in each enclosing recovery."
  [transport]
  (doseq [{:keys [resources]} *transport-owners*]
    (.remove ^Set resources transport)))

(defn track-transport!
  "Own a provisional socket or selector before it can block. Registration and
  disconnect share the owner's lifecycle lock; transport closure runs outside it."
  [^Closeable transport]
  (try
    (doseq [{:keys [lock closed resources]} *transport-owners*]
      (locking lock
        (when (.get ^AtomicBoolean closed)
          (throw (ex-info "This client is closed"
                          {:error :client/closed
                           :termination-reason :client-disconnected})))
        (.add ^Set resources transport)))
    transport
    (catch Throwable t
      (try (.close transport)
           (catch Throwable cleanup-error
             (when-not (identical? t cleanup-error)
               (.addSuppressed t cleanup-error))))
      (untrack-transport! transport)
      (throw t))))

;; Keep the dynamic binding for conveyed callers, but avoid allocating a
;; Clojure MapEntry on every lookup while a synchronous request is running.
;; Scope exit clears values without removing/reallocating ThreadLocal entries.
;; Each entry holds the scope's budget, the last Clojure binding frame, and that
;; frame's budget. Explicit bindings override the local budget, including nil.
(defonce ^:private ^ThreadLocal request-local (ThreadLocal.))
(defonce ^:private ^ThreadLocal wire-local (ThreadLocal.))

(defrecord Budget [^long deadline ^long maximum ^long timeout-ms
                   ^AtomicBoolean closed ^AtomicBoolean ha?
                   ^AtomicReference request ^AtomicBoolean sent? ^AtomicBoolean uncertain?
                   ^AtomicBoolean tracked?])

(defn local-budget ^Budget []
  (when-let [^objects state (.get request-local)] (aget state 0)))

(defn set-local-budget! [value]
  (let [^objects state (or (.get request-local)
                          (when value
                            (let [state (object-array 3)]
                              (.set request-local state)
                              state)))]
    (when (and state (not (identical? value (aget state 0))))
      (aset state 0 value)
      (aset state 1 nil)
      (aset state 2 nil))))

(defn current ^Budget []
  (let [^objects state (.get request-local)]
    (if (and state (aget state 0))
      (let [frame (clojure.lang.Var/getThreadBindingFrame)]
        (if (identical? frame (aget state 1))
          (aget state 2)
          (let [value (if (.getThreadBinding #'*request*) *request* (aget state 0))]
            (aset state 1 frame)
            (aset state 2 value)
            value)))
      *request*)))
(defn wire-request [] (.get wire-local))
(defn set-wire-request! [value] (.set wire-local value))

(defmacro with-wire-request [message & body]
  `(let [previous# (wire-request)]
     (set-wire-request! ~message)
     (try ~@body (finally (set-wire-request! previous#)))))

(defn budget [timeout-ms ha-ms closed request]
  (let [now (System/nanoTime)
        deadline (+ now (* 1000000 (long timeout-ms)))]
    (->Budget deadline (+ deadline (* 1000000 (long ha-ms))) timeout-ms
              closed (AtomicBoolean. false) (AtomicReference. request)
              (AtomicBoolean. false) (AtomicBoolean. false) (AtomicBoolean. false))))

(defn- budget-deadline ^long [^Budget b]
  (let [outer (if (.get ^AtomicBoolean (.-ha? b)) (.-maximum b) (.-deadline b))]
    (if *stage-deadline* (min outer (long *stage-deadline*)) outer)))

(defn deadline ^long [] (budget-deadline (current)))

(defn closed? []
  (boolean (when-let [^Budget b (current)]
             (when-let [^AtomicBoolean closed (.-closed b)] (.get closed)))))

(defn expired? []
  (when-let [b (current)] (>= (System/nanoTime) (budget-deadline b))))

(defn interrupted?
  "Recognize cancellation even when an interruptible wait cleared the flag."
  [^Throwable error]
  (or (.isInterrupted (Thread/currentThread))
      (loop [^Throwable cause error]
        (when cause
          (if (instance? InterruptedException cause)
            true
            (recur (.getCause cause)))))))

(defn rethrow-interrupted!
  "Restore cancellation and prevent recovery from retrying a failed attempt."
  [^Throwable error]
  (when (interrupted? error)
    (.interrupt (Thread/currentThread))
    (throw error)))

(defn- check-budget! [^Budget b phase]
  (when b
    (when (.isInterrupted (Thread/currentThread))
      (throw (InterruptedException. "Request interrupted")))
    (when-let [^AtomicBoolean closed (.-closed b)]
      (when (.get closed)
        (throw (ex-info "This client is closed"
                       {:error :client/closed :termination-reason :client-disconnected}))))
    (when (>= (System/nanoTime) (budget-deadline b))
      (throw (ex-info "Timeout in making request"
                      {:error :socket/timeout :phase phase
                       :timeout-ms (.-timeout-ms b)
                       :termination-reason :deadline-exceeded})))))

(defn check! [phase] (check-budget! (current) phase))

(defn remaining-ms
  "Ceil a positive remaining interval; a zero selector timeout waits forever."
  ^long [fallback]
  (if-let [b (current)]
    (do (check-budget! b :wait)
        (max 1 (quot (+ 999999 (- (budget-deadline b) (System/nanoTime))) 1000000)))
    (long fallback)))

(defn enter-ha! []
  (when-let [^Budget b (current)] (.set ^AtomicBoolean (.-ha? b) true))
  (check! :ha-retry))

(defn await!
  "Wait on an already locked monitor, using the caller's remaining budget."
  [monitor phase]
  (check! phase)
  (let [ns-left (max 1 (- (deadline) (System/nanoTime)))]
    (.wait ^Object monitor (quot ns-left 1000000) (int (rem ns-left 1000000)))))

(defn- same-command? [request message]
  (and request (= (:type request) (:type message))
       (= (or (:db-name request) (nth (:args request) 0 nil))
          (or (:db-name message) (nth (:args message) 0 nil)))))

(defn register-request!
  "Capture metadata created inside the public boundary before its first send."
  [message]
  (when-let [^Budget b (current)]
    (let [^AtomicReference ref (.-request b)
          request (.get ref)]
      (when (and (same-command? request message) (nil? (:client-op-id request)))
        (.compareAndSet ref request message)))))

(defn- original? [^Budget b message]
  (when-let [request (.get ^AtomicReference (.-request b))]
    (and (= (:type request) (:type message))
         (= (:client-op-id request) (:client-op-id message))
         (= (or (:db-name request) (nth (:args request) 0 nil))
            (or (:db-name message) (nth (:args message) 0 nil))))))

(defn sending! []
  (when-let [^Budget b (current)]
    (let [message (wire-request)]
      (when (and (not (cmd/read-only? (:type message))) (original? b message))
        (.set ^AtomicBoolean (.-sent? b) true)))))

(defn track-exchange! [message]
  (when-let [^Budget b (current)]
    (when (and (not (cmd/read-only? (:type message))) (original? b message))
      (.set ^AtomicBoolean (.-tracked? b) true))))

(defn exchanged?
  "Whether the enclosing mutation reached a transport exchange."
  []
  (boolean (when-let [^Budget b (current)] (.get ^AtomicBoolean (.-tracked? b)))))

(defn failed-attempt! []
  (when-let [^Budget b (current)]
    (when (.getAndSet ^AtomicBoolean (.-sent? b) false)
      (.set ^AtomicBoolean (.-uncertain? b) true))))

(defn failed-exchange!
  "Alternate IClient/IConnection implementations cannot report byte-level
  progress. Conservatively retain uncertainty when their exchange fails."
  [request]
  (when-let [^Budget b (current)]
    (when (and (not (cmd/read-only? (:type request))) (original? b request)
               (not (.get ^AtomicBoolean (.-tracked? b))))
      (.set ^AtomicBoolean (.-uncertain? b) true)))
  (failed-attempt!))

(defn response! [request response]
  (when-let [^Budget b (current)]
    (when (and (not (cmd/read-only? (:type request))) (original? b request))
      (when-not (#{:copy-in-response :copy-out-response} (:type response))
        (.set ^AtomicBoolean (.-sent? b) false))
      (when (= :command-complete (:type response))
        (.set ^AtomicBoolean (.-uncertain? b) false)))))

(defn failure
  "Keep uncertainty across attempts, including when a later attempt never sent."
  [^Throwable error]
  (when (interrupted? error)
    (.interrupt (Thread/currentThread)))
  (let [^Budget b (current)]
    (if (and b (.get ^AtomicBoolean (.-uncertain? b)))
      (let [req (.get ^AtomicReference (.-request b))
            existing (:err-data (ex-data error))
            data (merge existing
                        (select-keys req [:client-op-id :client-op-hash
                                          :client-op-response-kind])
                        {:error :ha/write-indeterminate :indeterminate? true
                         :outcome :unknown :retryable? false :type (:type req)
                         :db-name (or (:db-name req) (first (:args req)))
                         :termination-reason
                         (cond (closed?) :client-disconnected
                               (expired?) :deadline-exceeded
                               (interrupted? error) :request-interrupted
                               :else (or (:termination-reason (ex-data error))
                                         :recovery-failed))})]
        (ex-info "Write outcome is unknown; a fresh invocation is not a safe retry"
                 (merge (ex-data error) data {:err-data data})
                 (if (= :ha/write-indeterminate (:error existing))
                   (or (ex-cause error) error)
                   error)))
      error)))

(defmacro with-local-budget [value & body]
  `(let [previous# (local-budget)]
     (set-local-budget! ~value)
     (try ~@body (finally (set-local-budget! previous#)))))

(defmacro with-budget [value & body]
  `(if-let [existing# (current)]
     (if (identical? existing# *request*)
       (with-local-budget existing# ~@body)
       ;; A local-only caller has no dynamic binding to convey to futures yet.
       (binding [*request* existing#]
         (with-local-budget existing# ~@body)))
     (binding [*request* ~value]
       (with-local-budget *request*
         (try ~@body
              (catch Throwable error#
                (failed-attempt!)
                (throw (failure error#))))))))

