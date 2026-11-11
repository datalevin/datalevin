(ns ^:no-doc datalevin.query.source.runtime
  "Query-scoped ownership and redacted calls across the trusted source boundary."
  (:require [datalevin.query.source :as source]
            [datalevin.query.source.control :as control]
            [datalevin.query.source.value :as value]
            [datalevin.timeout :as timeout])
  (:import [datalevin.utl QueryKey QueryKey$Budget QueryKey$Workspace]
           [java.util UUID]
           [java.util.concurrent.atomic AtomicReference]))

(def ^:dynamic *runtime* nil)
(def ^:dynamic ^:private *context-control* nil)
(def ^:dynamic ^:private *workspace-cleanups* {})

(def categories
  #{:query-source/version :query-source/unsupported :query-source/invalid
    :query-source/unauthorized :query-source/stale-definition :query-source/snapshot-lost
    :query-source/limit :query-source/cancelled :query-source/provider
    :query-source/incomplete :query/timeout})

(def ^:private phases
  #{:descriptor :inputs :clauses :projection :window :execution-mode :context
    :types :native-view :native-type :native-value :equality :acquisition :session
    :planning :open :fetch :batch :values :reserve :execution :metrics :close :syntax :explain})

(defn redacted-error [runtime phase error]
  ;; Only errors redacted by this invocation may retain diagnostic context.
  ;; Provider ex-data can contain arbitrary values even under structural keys.
  (if (contains? @(:redacted-errors runtime) error)
    error
    (let [data (ex-data error)
          error
          (ex-info "Query-source operation failed"
             (merge {:error (cond (instance? InterruptedException error) :query-source/cancelled
                                  (categories (:error data)) (:error data)
                                  :else :query-source/provider)
                     :phase (if (phases phase) phase :execution) :query-id (:query-id runtime)}
                    (:location runtime)
                    (when (= :query/timeout (:error data)) {:type :query/timeout})
                    (when (= :closed (:state data)) {:state :closed})
                    (when (and (contains? control/default-limits (:category data))
                               (integer? (:maximum data)) (integer? (:attempted data)))
                      (select-keys data [:category :maximum :attempted]))))]
      (swap! (:redacted-errors runtime) conj error)
      error)))

(defn active-error
  "Prefer a control failure over a new untrusted input/callback exception.
  Already redacted primary failures retain their original context."
  [runtime error]
  (if (contains? @(:redacted-errors runtime) error) error
      (try (source/check-active! (:control runtime)) error
           (catch Throwable active active))))

(declare charge-copy!)

(defn diagnostic-copy!
  "Own a public identifier without retaining provider annotations or backing."
  [runtime identifier]
  (charge-copy! runtime identifier)
  (value/diagnostic-copy identifier #(source/check-active! (:control runtime))))

(defn with-context
  "Locate failures using core query/plan context, never provider exception data."
  [runtime location f]
  (when (and (contains? location :region-id)
             (not (and (integer? (:region-id location)) (<= 0 (:region-id location) Long/MAX_VALUE))))
    (value/fail! :query-source/invalid :planning))
  (let [location (into {} (map (fn [[k v]]
                                [k (cond (= :clause-ids k) (with-meta v nil)
                                         (symbol? v) (with-meta v nil)
                                         (coll? v) (diagnostic-copy! runtime v)
                                         :else v)]))
                       (select-keys location [:source-symbol :region-id :clause-ids :alternative-id]))
        runtime (update runtime :location merge location)]
    (try (f runtime)
         (catch Throwable e
           (throw (redacted-error runtime (or (:phase (ex-data e)) :execution) e))))))

(defn invoke [runtime phase f]
  (let [result (try
                 (source/check-active! (:control runtime))
                 (try (f)
                      (catch Throwable e
                        (source/check-active! (:control runtime))
                        (throw e)))
                 (catch Throwable e
                   (throw (redacted-error runtime phase e))))]
    ;; A provider may successfully return a resource just as its deadline fires.
    ;; Callers first attach ownership, then check active before the next operation.
    result))

(declare with-runtime-context)

(deftype Cleanup [^AtomicReference action]
  clojure.lang.IFn
  (invoke [_]
    ;; Taking ownership clears both the resource callback and its runtime
    ;; context, even when close throws or reenters its own close function.
    (when-let [[runtime close] (.getAndSet action nil)]
      (try (if (identical? (:control runtime) *context-control*)
             (binding [*runtime* runtime] (close))
             (with-runtime-context runtime (fn [_] (close))))
           (catch Throwable e
             (throw (redacted-error runtime :close e)))))))

(defn own!
  "Attach one resource and return its idempotent close function. Completed
  cleanup entries release their captured resource and runtime references."
  [runtime close]
  (let [cleanup (Cleanup. (AtomicReference. [runtime close]))]
    (swap! (get *workspace-cleanups* (:cleanup runtime) (:cleanup runtime)) conj cleanup)
    (fn [] (cleanup))))

(defn with-workspace
  "Run synchronous work with an isolated cleanup stack on the same control.
  Consume or copy borrowed results before returning. Completed workspaces do
  not leave one cleanup entry per iteration in the parent invocation. Optional
  owners are captured operator runtimes whose new resources belong here."
  ([runtime f] (with-workspace runtime [] f))
  ([runtime owners f]
    (let [_ (source/check-active! (:control runtime))
          _ (when-not (and (vector? owners)
                           (every? #(and (identical? (:control runtime) (:control %))
                                         (= (:query-id runtime) (:query-id %))) owners))
              (value/fail! :query-source/invalid :execution))
          token (source/reserve! (:control runtime) {:retained-bytes (+ 1024 (* 128 (count owners)))})
          workspace (assoc runtime :cleanup (atom []))
          primary (volatile! nil)]
      (binding [*runtime* workspace
                *workspace-cleanups* (reduce (fn [targets owner]
                                               (assoc targets (:cleanup owner) (:cleanup workspace)))
                                             (assoc *workspace-cleanups* (:cleanup runtime) (:cleanup workspace)) owners)]
        (try
          (invoke workspace :execution #(f workspace))
          (catch Throwable e (vreset! primary e) (throw e))
          (finally
            (let [failure (volatile! nil)]
              (doseq [close (reverse @(:cleanup workspace))]
                (try (close)
                     (catch Throwable e
                       (let [error (redacted-error workspace :close e)]
                         (if-let [original (or @primary @failure)]
                           (.addSuppressed ^Throwable original error)
                           (vreset! failure error))))))
              (reset! (:cleanup workspace) [])
              (source/release! token)
              (when-let [error @failure]
                ;; A deadline first observed during cleanup precedes a close
                ;; failure, just as it does at the outer invocation boundary.
                (let [cancelled (try (source/check-active! (:control workspace)) nil
                                     (catch Throwable e
                                       (redacted-error workspace :execution e)))]
                  (throw (if cancelled (doto ^Throwable cancelled (.addSuppressed error)) error)))))))))))

(defn- charge-value! [runtime x size! overhead multiplier result?]
  (let [control (:control runtime)
        limits (control/limits control)
        categories (if result? [:retained-bytes :result-bytes] [:retained-bytes])
        ;; Preflight walks retain no values. Read the ledger once, then compare
        ;; incremental sizes with this headroom. The final atomic reservation
        ;; still checks concurrent reservations before any owned copy is made.
        usage (control/usage control)
        headroom (reduce min (map #(- (long (get limits %)) (long (get usage %))) categories))
        bytes (volatile! overhead)
        check-limit! (fn [usage category charge]
                       (let [maximum (long (get limits category))
                             attempted (value/add-charge (long (get usage category)) charge)]
                         (when-not (value/at-most? attempted maximum)
                           (value/fail! :query-source/limit :reserve
                                        {:category category :maximum maximum
                                         :attempted attempted}))))
        check! (fn []
                 (source/check-active! control)
                 (when-not (value/at-most? @bytes headroom)
                   (doseq [category categories] (check-limit! usage category @bytes))))
        observe! (fn [increment]
                   (when-not (zero? increment)
                     (vreset! bytes (value/add-charge (long @bytes)
                                      (if (= 1 multiplier) increment
                                        (value/multiply-charge multiplier increment)))))
                   (check!))]
    ;; Fixed overhead and row limits can reject without visiting any values.
    (check!)
    (when result? (check-limit! usage :result-rows 1))
    (size! x observe!)
    ;; Preflight does not mutate the shared ledger. A provider or nested query
    ;; can reserve concurrently; the final atomic reservation checks it again.
    (source/reserve! control (cond-> {:retained-bytes @bytes}
                              result? (assoc :result-rows 1 :result-bytes @bytes)))))

(defn charge!
  "Check size/cancellation incrementally, then reserve retained values,
  backing storage, and ledger/container overhead. Explicit overhead/multiplier
  account for row maps and blocking operators without an unbounded sizing walk."
  ([runtime x] (charge! runtime x 256 1))
  ([runtime x overhead multiplier]
   (charge-value! runtime x value/value-charge overhead multiplier false)))

(defn charge-metadata!
  "Validate and reserve an immutable envelope before structural validation.
  The caller releases transient envelopes once it no longer retains them."
  ([runtime x phase] (charge-metadata! runtime x phase 256))
  ([runtime x phase overhead]
   (charge-value! runtime x #(value/metadata-charge %1 phase %2) overhead 1 false)))

(defn charge-copy!
  "Reserve before owned-copy, which detaches vector views from their parents."
  [runtime x]
  (charge-value! runtime x value/copy-charge 256 1 false))

(defn charge-result!
  "Check all output limits while sizing, then atomically reserve one result row
  and its retained/result bytes. A quota overflow is never a semantic limit."
  [runtime row]
  (charge-value! runtime row value/value-charge 256 1 true))

(defn with-runtime-context
  "Bind an already owned runtime and its shared query-key budget. Nested query
  invocations inherit this control's limits and deadline. Restore the previous
  thread budget on every exit; the caller retains cleanup ownership."
  [runtime f]
  (let [ctl (:control runtime)
        remaining (control/remaining-time-ms ctl)
        local (when remaining
                (try
                  (Math/addExact (System/currentTimeMillis) (long remaining))
                  (catch ArithmeticException _ Long/MAX_VALUE)))
        deadlines (keep identity [timeout/*deadline* (:deadline runtime) local])
        deadline (when (seq deadlines) (reduce min deadlines))
        runtime (assoc runtime :deadline deadline)
        previous (QueryKey/setBudget
                   (reify QueryKey$Budget
                     (check [_] (source/check-active! ctl))
                     (reserve [_ bytes]
                       (let [token (source/reserve! ctl {:retained-bytes bytes})]
                         (reify QueryKey$Workspace
                           (close [_] (source/release! token)))))))]
    (binding [*runtime* runtime *context-control* ctl timeout/*deadline* deadline]
      (try (f runtime) (finally (QueryKey/setBudget previous))))))

(defn with-runtime [options f]
  (let [control (control/create (assoc options :parent (:control *runtime*)))
        runtime {:control control :query-id (UUID/randomUUID) :cleanup (atom [])
                 :redacted-errors (atom #{})}
        primary (atom nil)]
    (binding [*runtime* runtime]
      (try
        (let [result (with-runtime-context runtime f)]
          (source/check-active! control)
          result)
        (catch Throwable e
          (let [e (active-error runtime e)
                error (redacted-error runtime (or (:phase (ex-data e)) :execution) e)]
            (reset! primary error)
            (control/cancel! control (select-keys (ex-data error) [:error :phase]))
            (throw error)))
        (finally
          (let [cleanup-error (atom nil)]
            (doseq [close (reverse @(:cleanup runtime))]
              (try (close)
                   (catch Throwable e
                     (let [error (redacted-error runtime :close e)]
                       (if-let [original (or @primary @cleanup-error)]
                         (.addSuppressed ^Throwable original error)
                         (let [^Throwable cancelled
                               (try (source/check-active! control) nil
                                    (catch Throwable e
                                      (redacted-error runtime
                                        (or (:phase (ex-data e)) :execution) e)))
                               error (if cancelled
                                       (doto cancelled (.addSuppressed error)) error)]
                           ;; Cancellation can first become visible during
                           ;; cleanup. Its category still precedes cleanup errors.
                           (reset! cleanup-error error)
                           (control/cancel! control (select-keys (ex-data error) [:error :phase]))))))))
            (reset! (:cleanup runtime) [])
            (if (or @primary @cleanup-error)
              (control/close! control)
              (try (control/complete! control)
                   (catch Throwable e
                     (reset! cleanup-error
                             (redacted-error runtime (or (:phase (ex-data e)) :execution) e)))))
            (when @cleanup-error (throw @cleanup-error))))))))
