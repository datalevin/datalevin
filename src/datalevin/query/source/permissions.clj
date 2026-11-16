(ns ^:no-doc datalevin.query.source.permissions
  "Session-owned cursor permissions and actual backend request permits.

  Both use the invocation control's monitor and ledger. Replay and consistency
  grant no permissions. These helpers do not acquire sessions or open cursors."
  (:require [datalevin.query.source.control :as control]
            [datalevin.query.source.value :as value :refer [fail!]])
  (:import [datalevin.query.source.control Control]
           [java.io Closeable]
           [java.math BigInteger]
           [java.util LinkedList]))

(defn- ensure! [condition phase]
  (when-not condition (fail! :query-source/invalid phase)))

(defn- fields! [m required phase]
  (ensure! (and (value/plain-map? m) (every? #(contains? m %) required)
                (every? #(or (required %) (qualified-keyword? %)) (keys m))) phase))

(defn- positive-long? [x]
  (and (integer? x) (<= 1 x Long/MAX_VALUE)))

(defn- monitor [^Control ctl] (.-state ctl))

(defn- metadata! [ctl metadata overhead]
  ;; Admission precedes recursive validation/copying, including annotations and
  ;; backing views that will not be retained by the normalized declarations.
  (let [bytes (volatile! overhead)
        maximum (:retained-bytes (control/limits ctl))]
    (value/metadata-charge metadata :session
      (fn [increment]
        (control/check-active! ctl)
        (let [total (vswap! bytes #(value/add-charge (long %) increment))]
          (when-not (value/at-most? total maximum)
            (fail! :query-source/limit :reserve
                   {:category :retained-bytes :maximum maximum :attempted total})))))
    (control/reserve! ctl {:retained-bytes @bytes})))

(declare close-requests! release-request! close-consumption! release-open!)

(deftype RequestLimiter [ctl ^long maximum ^long requests ^BigInteger interval
                         ^LinkedList history state metadata]
  Closeable
  (close [this] (close-requests! this)))

(deftype RequestPermit [limiter request waiting closed]
  Closeable
  (close [this] (release-request! this)))

(deftype RequestStart [^BigInteger at reservation])

(defn request-constraints!
  "Validate already admitted immutable session request constraints. This pure
  check creates no limiter and does not charge an actual backend request."
  ([constraints] (request-constraints! constraints nil))
  ([constraints check!]
   (when check! (check!))
   (fields! constraints #{:max-concurrent-requests :rate-limit} :session)
   (ensure! (positive-long? (:max-concurrent-requests constraints)) :session)
   (when-some [rate (:rate-limit constraints)]
     (when check! (check!))
     (fields! rate #{:requests :per-ms} :session)
     (ensure! (and (positive-long? (:requests rate)) (positive-long? (:per-ms rate))) :session))
   constraints))

(defn request-limiter
  "Create one session-wide limiter, shared by every alias and consumption group.
  Setup, pages, retries, and splits each acquire a permit. Close with the session;
  applications remain responsible for backend limits shared across invocations."
  [ctl constraints]
  (let [metadata (metadata! ctl constraints 2048)]
    (try
      (request-constraints! constraints #(control/check-active! ctl))
      (let [maximum (:max-concurrent-requests constraints)
            rate (:rate-limit constraints)]
        (RequestLimiter. ctl (long maximum) (long (if rate (:requests rate) 0))
                         (when rate (.multiply (biginteger (:per-ms rate)) (biginteger 1000000)))
                         (LinkedList.) (volatile! {:active 0 :waiting 0 :closed? false}) metadata))
      (catch Throwable e (control/release! metadata) (throw e)))))

(defn- prune-requests! [^RequestLimiter limiter ^BigInteger now]
  (let [^LinkedList history (.-history limiter)]
    (loop []
      (when-let [^RequestStart start (.peekFirst history)]
        (when (not (pos? (long (.compareTo (.add (.-at start) (.-interval limiter)) now))))
          (.removeFirst history)
          (control/release! (.-reservation start))
          (recur))))))

(defn- wait-ms ^long [^RequestLimiter limiter ^BigInteger now]
  ;; Polling also observes interruption and closure even if a provider releases
  ;; concurrency using the ordinary ledger API. Cancellation/release notify the
  ;; monitor immediately; the bounded wait is a fallback, not a busy loop.
  (if-let [^RequestStart first-start (.peekFirst ^LinkedList (.-history limiter))]
    (let [remaining (.subtract (.add (.-at first-start) (.-interval limiter)) now)]
      (max 1 (.longValue (.min (biginteger 50)
                               (.divide (.add remaining (biginteger 999999)) (biginteger 1000000))))))
    50))

(defn- release-request-metadata! [^RequestLimiter limiter]
  (let [{:keys [active waiting closed?]} @(.-state limiter)]
    (when (and closed? (zero? (long active)) (zero? (long waiting)))
      (control/release! (.-metadata limiter)))))

(defn acquire-request!
  "Wait cancellably for a request start, charging actual query requests once.
  The returned Closeable owns live concurrency and must close in finally.
  Waiting neither charges an external request nor holds a concurrency slot."
  [^RequestLimiter limiter]
  (let [ctl (.-ctl limiter)
        lock (monitor ctl)
        waiting (control/reserve! ctl {:retained-bytes 1024})
        registered? (volatile! false)]
    (try
      (locking lock
        (control/check-active! ctl)
        (when (:closed? @(.-state limiter))
          (fail! :query-source/invalid :open {:state :closed}))
        (vswap! (.-state limiter) update :waiting inc)
        (vreset! registered? true)
        (loop []
          (control/check-active! ctl)
          (when (:closed? @(.-state limiter))
            (fail! :query-source/invalid :open {:state :closed}))
          (let [now (biginteger (System/nanoTime))
                limits (control/limits ctl)
                usage (control/usage ctl)]
            (when (>= (long (:external-requests usage)) (long (:external-requests limits)))
              (fail! :query-source/limit :reserve
                     {:category :external-requests :maximum (:external-requests limits)
                      :attempted (inc' (long (:external-requests usage)))}))
            (when (.-interval limiter) (prune-requests! limiter now))
            (if (and (< (long (:concurrent-requests usage)) (long (:concurrent-requests limits)))
                     (< (long (:active @(.-state limiter))) (.-maximum limiter))
                     (or (nil? (.-interval limiter))
                         (< (.size ^LinkedList (.-history limiter)) (.-requests limiter))))
              (let [history-charge (when (.-interval limiter)
                                     (control/reserve! ctl {:retained-bytes 512}))]
                (try
                  (let [request (control/reserve! ctl {:external-requests 1 :concurrent-requests 1})]
                    (when history-charge
                      (.addLast ^LinkedList (.-history limiter)
                                (RequestStart. (biginteger (System/nanoTime)) history-charge)))
                    (vswap! (.-state limiter) #(-> % (update :active inc) (update :waiting dec)))
                    (vreset! registered? false)
                    (RequestPermit. limiter request waiting (volatile! false)))
                  (catch Throwable e
                    (when history-charge (control/release! history-charge))
                    (throw e))))
              (do
                (try (.wait ^Object lock (wait-ms limiter now))
                     (catch InterruptedException _
                       (.interrupt (Thread/currentThread))
                       (control/check-active! ctl)))
                (recur))))))
      (catch Throwable e
        (locking lock
          (when @registered?
            (vswap! (.-state limiter) update :waiting dec))
          (control/release! waiting)
          (release-request-metadata! limiter))
        (throw e)))))

(defn release-request! [^RequestPermit permit]
  (let [^RequestLimiter limiter (.-limiter permit)]
    (locking (monitor (.-ctl limiter))
      (when-not @(.-closed permit)
        (vreset! (.-closed permit) true)
        (vswap! (.-state limiter) update :active dec)
        (control/release! (.-request permit))
        (control/release! (.-waiting permit))
        (release-request-metadata! limiter))))
  nil)

(defn close-requests! [^RequestLimiter limiter]
  (locking (monitor (.-ctl limiter))
    (when-not (:closed? @(.-state limiter))
      (vswap! (.-state limiter) assoc :closed? true)
      (doseq [^RequestStart start (.-history limiter)]
        (control/release! (.-reservation start)))
      (.clear ^LinkedList (.-history limiter))
      (release-request-metadata! limiter)
      (.notifyAll ^Object (monitor (.-ctl limiter)))))
  nil)

(deftype Consumption [ctl explicit? groups state metadata]
  Closeable
  (close [this] (close-consumption! this)))

(deftype OpenPermission [consumption group reservation closed]
  Closeable
  (close [this] (release-open! this)))

(defn consumption
  "Capture session-default or explicit independent-resource permissions.
  Default sessions expose no synthetic group ID. Replay is deliberately absent."
  [ctl info]
  (let [default? (contains? info :consumption)
        explicit? (contains? info :consumption-groups)
        declarations (if default? (:consumption info) (:consumption-groups info))
        metadata (metadata! ctl declarations 2048)]
    (try
      (ensure! (not= default? explicit?) :session)
      (if default?
        (fields! declarations #{:opens :max-open-cursors} :session)
        (ensure! (and (vector? declarations) (<= 2 (count declarations))) :session))
      (let [groups (if default? [declarations] declarations)
            accepted
            (reduce (fn [m group]
                      (fields! group (cond-> #{:opens :max-open-cursors} explicit? (conj :id)) :session)
                      (let [{:keys [id opens max-open-cursors]} group]
                        (ensure! (and (#{:once :many} opens) (positive-long? max-open-cursors)
                                      (or (= :many opens) (= 1 max-open-cursors))
                                      (or default? (keyword? id) (and (string? id) (seq id)))
                                      (not (contains? m id))) :session)
                        (assoc m id {:opens opens :max-open-cursors (long max-open-cursors)})))
                    {} groups)]
        (Consumption. ctl explicit? accepted
                      (volatile! {:closed? false :groups (zipmap (keys accepted) (repeat {:active 0 :attempts 0}))})
                      metadata))
      (catch Throwable e (control/release! metadata) (throw e)))))

(defn- open-group! [^Consumption permissions alternative request phase]
  (let [properties (:properties alternative)
        alternative-id (:consumption-group properties)
        request-id (:consumption-group request)]
    (if (.-explicit? permissions)
      (do (ensure! (and (contains? properties :consumption-group)
                        (contains? request :consumption-group)
                        (= alternative-id request-id)
                        (contains? (.-groups permissions) request-id)) phase)
          request-id)
      (do (ensure! (and (not (contains? properties :consumption-group))
                        (not (contains? request :consumption-group))) phase)
          nil))))

(defn planning-permission
  "Inspect the shared resource behind an alternative without reserving an open.
  The returned resource key is invocation-local, opaque, and never diagnostic
  metadata. Execution must still reserve immediately before its open attempt."
  [^Consumption permissions alternative]
  (let [ctl (.-ctl permissions)]
    (locking (monitor ctl)
      (control/check-active! ctl)
      (when (:closed? @(.-state permissions))
        (fail! :query-source/invalid :planning {:state :closed}))
      (let [properties (:properties alternative)
            request (select-keys properties [:consumption-group])
            group (open-group! permissions alternative request :planning)]
        {:resource [permissions group]
         :opens (get-in (.-groups permissions) [group :opens])
         :spent? (pos? (long (get-in @(.-state permissions) [:groups group :attempts])))}))))

(defn repeatable-default?
  "Whether a session permits repeated ordinary opens. This inspects ownership;
  it reserves nothing and does not turn replay or consistency into permission."
  [^Consumption permissions]
  (locking (monitor (.-ctl permissions))
    (control/check-active! (.-ctl permissions))
    (and (not (:closed? @(.-state permissions)))
         (not (.-explicit? permissions))
         (= :many (get-in (.-groups permissions) [nil :opens])))))

(defn reserve-open!
  "Reserve an attempt before calling the provider. Closing releases concurrency
  only: a failed or cancelled one-shot attempt remains spent. No replay property
  can replenish permissions. Validate the open's captured identity separately."
  [^Consumption permissions alternative request]
  (let [ctl (.-ctl permissions)]
    (locking (monitor ctl)
      (control/check-active! ctl)
      (when (:closed? @(.-state permissions))
        (fail! :query-source/invalid :open {:state :closed}))
      (let [group (open-group! permissions alternative request :open)
            {:keys [opens max-open-cursors]} (get (.-groups permissions) group)
            {:keys [active attempts]} (get-in @(.-state permissions) [:groups group])]
        (when (and (= :once opens) (pos? (long attempts)))
          (fail! :query-source/unsupported :open))
        (when (>= (long active) (long max-open-cursors))
          (fail! :query-source/limit :open))
        (let [reservation (control/reserve! ctl {:retained-bytes 512})]
          (vswap! (.-state permissions) update-in [:groups group]
                  #(-> % (update :active inc) (update :attempts inc')))
          (OpenPermission. permissions group reservation (volatile! false)))))))

(defn release-open! [^OpenPermission permission]
  (let [^Consumption permissions (.-consumption permission)]
    (locking (monitor (.-ctl permissions))
      (when-not @(.-closed permission)
        (vreset! (.-closed permission) true)
        (vswap! (.-state permissions) update-in [:groups (.-group permission) :active] dec)
        (control/release! (.-reservation permission))
        (when (and (:closed? @(.-state permissions))
                   (every? #(zero? (long (:active %))) (vals (:groups @(.-state permissions)))))
          (control/release! (.-metadata permissions))))))
  nil)

(defn close-consumption! [^Consumption permissions]
  (locking (monitor (.-ctl permissions))
    (when-not (:closed? @(.-state permissions))
      (vswap! (.-state permissions) assoc :closed? true)
      (when (every? #(zero? (long (:active %))) (vals (:groups @(.-state permissions))))
        (control/release! (.-metadata permissions)))))
  nil)
