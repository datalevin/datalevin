(ns ^:no-doc datalevin.query.source.control
  "Invocation-owned controls, cancellation deadlines, and shared resource ledger."
  (:require [datalevin.query.source.value :as value :refer [fail!]])
  (:import [java.math BigInteger]
           [java.util.concurrent CompletableFuture ScheduledThreadPoolExecutor
            ThreadFactory TimeUnit ScheduledFuture]))

(def default-limits
  {:batch-rows 4096 :batch-bytes 1048576 :binding-rows 256 :binding-bytes 2097152
   :in-flight-batches 2
   :retained-bytes 1073741824 :spill-bytes 1073741824 :result-rows 1000000
   :result-bytes 67108864 :external-requests 1024 :concurrent-requests 4})
(def ledger-keys
  (disj (set (keys default-limits)) :batch-rows :batch-bytes :binding-rows :binding-bytes))

(deftype Control [limits deadline state timer parent closed children])
(deftype Registration [control callback])
(deftype Reservation [control charges status])

(defonce ^:private scheduler
  (delay (doto (ScheduledThreadPoolExecutor.
                 1 (reify ThreadFactory
                     (newThread [_ task]
                       (doto (Thread. task "datalevin-source-deadline") (.setDaemon true)))))
           (.setRemoveOnCancelPolicy true))))

(declare cancel! check-active!)

(defn- remaining-nanos ^BigInteger [deadline]
  (.max BigInteger/ZERO
        (.subtract (biginteger deadline) (biginteger (System/nanoTime)))))

(defn- capped-long ^long [^BigInteger n]
  (.longValue (.min (biginteger Long/MAX_VALUE) n)))

(defn create
  ([] (create {}))
  ([{:keys [limits timeout-ms parent]}]
   (when parent (check-active! parent))
   (when-not (and (or (nil? limits) (map? limits))
                 (every? (set (keys default-limits)) (keys limits))
                 (every? (fn [[k v]]
                           (and (integer? v) (<= 0 v Long/MAX_VALUE)
                                (or (not (#{:batch-rows :batch-bytes :binding-rows :binding-bytes
                                            :in-flight-batches :concurrent-requests} k))
                                    (pos? (long v))))) limits)
                 (or (nil? timeout-ms) (and (integer? timeout-ms)
                                           (<= 0 timeout-ms Long/MAX_VALUE))))
     (fail! :query-source/invalid :context))
   (let [local-deadline (when timeout-ms
                          (.add (biginteger (System/nanoTime))
                                (.multiply (biginteger timeout-ms)
                                           (biginteger 1000000))))
         ;; Normal deadlines fit in a long. Preserve exact arithmetic for huge
         ;; timeouts, without allocating BigInteger comparisons at every value.
         local-deadline (if (and local-deadline (<= (.bitLength ^BigInteger local-deadline) 63))
                          (.longValue ^BigInteger local-deadline) local-deadline)
         deadlines (keep identity [local-deadline (when parent (.-deadline ^Control parent))])
         control (Control. (if parent (merge-with min (.-limits ^Control parent) limits)
                               (merge default-limits limits))
                           (when (seq deadlines) (apply min deadlines))
                           (if parent (.-state ^Control parent)
                               (atom {:usage (zipmap ledger-keys (repeat 0))
                                      :registrations #{} :reservations #{}}))
                           (atom nil) parent (atom false) (atom #{}))]
     ;; Register the child and its timer atomically with parent closure. A child
     ;; that loses this race must never leave a scheduled task or reservation.
     (locking (.-state control)
       (when parent (check-active! parent))
       (when-let [deadline (.-deadline control)]
         (reset! (.-timer control)
                 (.schedule ^ScheduledThreadPoolExecutor @scheduler
                            ^Runnable #(cancel! control {:error :query/timeout :phase :execution})
                            (capped-long (remaining-nanos deadline))
                            TimeUnit/NANOSECONDS)))
       (when parent (swap! (.-children ^Control parent) conj control)))
     control)))

(defn limits [^Control control] (.-limits control))
(defn usage [^Control control] (:usage @(.-state control)))
(defn cancelled? [^Control control] (boolean (:reason @(.-state control))))
(defn remaining-time-ms [^Control control]
  (when-let [deadline (.-deadline control)]
    (capped-long (.divide (remaining-nanos deadline) (biginteger 1000000)))))

(defn- dispatch! [^Registration registration reason]
  (CompletableFuture/runAsync
    ^Runnable (fn [] (try ((.-callback registration) reason)
                         (catch Throwable _ nil)))))

(defn- closed? [^Control control]
  (or @(.-closed control)
      (when-let [parent (.-parent control)] (closed? parent))))

(defn cancel! [^Control control reason]
  (let [state (.-state control)
        reason (select-keys reason [:error :phase])
        registrations
        (locking state
          (when-not (or (:reason @state) (:closed? @state) @(.-closed control))
            (let [registrations (:registrations @state)]
              (swap! state assoc :reason reason :registrations #{})
              (.notifyAll ^Object state)
              registrations)))]
    (doseq [registration registrations] (dispatch! registration reason)))
  nil)

(defn check-active! [^Control control]
  (let [state (.-state control)]
    (when (or (:closed? @state) (closed? control))
      (fail! :query-source/invalid :context {:state :closed}))
    (when (.isInterrupted (Thread/currentThread))
      (cancel! control {:error :query-source/cancelled :phase :execution}))
    (when (and (some? (.-deadline control))
               (value/at-most? (.-deadline control) (System/nanoTime)))
      (cancel! control {:error :query/timeout :phase :execution}))
    (when-let [reason (:reason @state)]
      (fail! (:error reason) (:phase reason))))
  nil)

(defn register-cancel! [^Control control callback]
  (when-not (fn? callback) (fail! :query-source/invalid :context))
  (let [state (.-state control)
        registration (Registration. control callback)
        reason (locking state
                 (when (or (:closed? @state) (closed? control))
                   (fail! :query-source/invalid :context {:state :closed}))
                 (if-let [reason (:reason @state)] reason
                     (do (swap! state update :registrations conj registration) nil)))]
    (when reason (dispatch! registration reason))
    registration))

(defn unregister-cancel! [^Registration registration]
  (let [state (.-state ^Control (.-control registration))]
    (locking state (swap! state update :registrations disj registration)))
  nil)

(defn reserve! [^Control control charges]
  (when-not (and (map? charges) (every? ledger-keys (keys charges))
                (every? #(and (integer? %) (<= 0 % Long/MAX_VALUE)) (vals charges)))
    (fail! :query-source/invalid :reserve))
  (let [state (.-state control)
        token (Reservation. control charges (atom :owned))]
    (locking state
      (check-active! control)
      (let [usage (merge-with +' (:usage @state) charges)]
        (doseq [[category attempted] usage]
          (when-not (value/at-most? attempted (long (get (.-limits control) category)))
            (fail! :query-source/limit :reserve
                   {:category category :maximum (get (.-limits control) category)
                    :attempted attempted})))
        (swap! state assoc :usage usage
               :reservations (conj (:reservations @state) token))))
    token))

(defn release! [^Reservation token]
  (let [state (.-state ^Control (.-control token))]
    (locking state
      (when-not (= :released @(.-status token))
        (reset! (.-status token) :released)
        (swap! state (fn [s]
                       (-> s
                           (update :usage #(merge-with - % (dissoc (.-charges token)
                                                                  :external-requests)))
                           (update :reservations disj token))))
        (.notifyAll ^Object state))))
  nil)

(defn accept!
  "Validate query ownership and transfer a live batch reservation exactly once."
  [^Control control token bytes]
  (let [state (.-state control)]
    (locking state
      (check-active! control)
      (when-not (and (instance? Reservation token)
                    (identical? control (.-control ^Reservation token))
                    (= :owned @(.-status ^Reservation token))
                    (value/at-most? bytes
                      (long (get (.-charges ^Reservation token) :retained-bytes 0)))
                    (>= (long (get (.-charges ^Reservation token) :in-flight-batches 0)) 1))
        (fail! :query-source/invalid :batch))
      (reset! (.-status ^Reservation token) :transferred)))
  nil)

(defn close!
  "Release this control and every descendant, including their deadline tasks."
  [^Control control]
  (let [state (.-state control)]
    (locking state
      (when-not @(.-closed control)
        (reset! (.-closed control) true)
        (when-let [timer @(.-timer control)] (.cancel ^ScheduledFuture timer false))
        (doseq [child @(.-children control)] (close! child))
        (doseq [^Reservation token (:reservations @state)
                :when (or (nil? (.-parent control)) (identical? control (.-control token)))]
          (release! token))
        (if-let [parent (.-parent control)]
          (do
            (swap! (.-children ^Control parent) disj control)
            (swap! state update :registrations
                   #(into #{} (remove (fn [^Registration r] (identical? control (.-control r)))) %)))
          (swap! state assoc :closed? true :registrations #{}))
        (.notifyAll ^Object state))))
  nil)

(defn complete!
  "Close a successful invocation atomically with its last cancellation check.
  A deadline/cancellation that wins the race fails the invocation; cleanup still
  releases its ledger and timers. Later cancellation cannot reopen completion."
  [^Control control]
  (locking (.-state control)
    (try (check-active! control)
         (finally (close! control)))))
