;; Copyright (c) Huahai Yang. All rights reserved.
;; Distributed under the Eclipse Public License 2.0.
(ns ^:no-doc datalevin.ha.publisher
  "Share lease renewal and committed-LSN publication within one HA runtime."
  (:require [datalevin.ha.control :as ctrl]
            [datalevin.util :refer [raise]])
  (:import [java.util.concurrent TimeUnit]))

(defn create
  "Create runtime-local publication coordination. No worker thread is needed."
  []
  (atom nil))

(defn- context
  [m]
  [(:ha-authority m) (:ha-renew-loop-running? m)
   (:ha-db-identity m) (:ha-node-id m) (:ha-leader-term m)
   (:ha-local-endpoint m) (:ha-membership-hash m)
   (:ha-lease-renew-ms m) (:ha-lease-timeout-ms m)])

(defn- remaining-ms
  ^long [^long deadline]
  (let [remaining (- deadline (System/nanoTime))]
    (when-not (pos? remaining)
      (raise "HA publication timed out"
             {:error :ha/control-timeout :where :lease-publication}))
    (max 1 (.toMillis TimeUnit/NANOSECONDS remaining))))

(defn- covers?
  [m outcome lsn]
  (let [{:keys [ok? lease]} (:result outcome)]
    (and ok?
         (= (:db-identity lease) (:ha-db-identity m))
         (= (:leader-node-id lease) (:ha-node-id m))
         (= (:term lease) (:ha-leader-term m))
         (>= (long (or (:leader-last-applied-lsn lease) 0)) (long lsn)))))

(defn- reusable?
  [m outcome lsn reuse-ms]
  (and outcome (pos? (long reuse-ms)) (covers? m outcome lsn)
       (let [{:keys [version authority-now-ms lease]} (:result outcome)
             age (- (System/nanoTime) (long (:local-start-nanos outcome)))
             lease-ms (when (integer? authority-now-ms)
                        (- (long (:lease-until-ms lease))
                           (long authority-now-ms)))]
         (and (integer? version)
              (>= (long version) (long (or (:ha-authority-version m) 0)))
              (some? lease-ms)
              (< age (.toNanos TimeUnit/MILLISECONDS
                               (min (long reuse-ms) (long lease-ms))))))))

(defn- submit
  [m lsn timeout-ms]
  (let [start-ms (System/currentTimeMillis)
        start-nanos (System/nanoTime)
        result (ctrl/renew-lease
                 (:ha-authority m)
                 {:db-identity (:ha-db-identity m)
                  :leader-node-id (:ha-node-id m)
                  :leader-endpoint (:ha-local-endpoint m)
                  :term (:ha-leader-term m)
                  :lease-renew-ms (:ha-lease-renew-ms m)
                  :lease-timeout-ms (:ha-lease-timeout-ms m)
                  :leader-last-applied-lsn lsn
                  :now-ms start-ms :timeout-ms timeout-ms})]
    ;; All waiters must derive the same lease deadline and observation age.
    ;; Using a later waiter's start time would extend the local lease proof.
    {:local-start-ms start-ms
     :local-start-nanos start-nanos
     :result (assoc result :observed-at-ms (System/currentTimeMillis))}))

(defn renew!
  "Publish at least lsn, sharing an in-flight command for the same owner/term.
  Higher LSNs coalesce into the next command. Each waiter keeps its own timeout;
  timing out does not cancel another caller's publication. Periodic callers may
  reuse a successful result younger than reuse-ms, measured from request start.
  current-state-fn, when provided, must return the live runtime without blocking.
  Return the result with its original local request timestamps."
  ([m lsn timeout-ms] (renew! m lsn timeout-ms nil))
  ([m lsn timeout-ms {:keys [reuse-ms current-state-fn] :or {reuse-ms 0}}]
   (if-let [publisher (:ha-renewal-publisher m)]
     (let [ctx (context m)
           caller-start (System/nanoTime)
           deadline (+ caller-start
                       (.toNanos TimeUnit/MILLISECONDS (long timeout-ms)))]
       (loop []
         (remaining-ms deadline)
         (let [{:keys [completed pending owner?]}
               (locking publisher
                 (let [current (if current-state-fn (current-state-fn) m)
                       _ (when (or (not= ctx (context current))
                                   (not= :leader (:ha-role current)))
                           (raise "HA publication context changed"
                                  {:error :ha/publication-context-changed}))
                       latest-lsn
                       (max (long lsn)
                            (long (or (:ha-leader-last-applied-lsn current) 0))
                            (long (or (get-in current [:ha-authority-lease
                                                       :leader-last-applied-lsn]) 0)))
                       state (if (= ctx (:context @publisher))
                               @publisher {:context ctx})
                       wanted (max latest-lsn (long (or (:wanted-lsn state) 0)))
                       state (assoc state :wanted-lsn wanted)]
                   (cond
                     (:pending state)
                     (do (reset! publisher state) {:pending (:pending state)})

                     (or (let [done (:completed state)]
                           (and done
                                (>= (long (:publication-start-nanos done)) caller-start)
                                (>= (long (:published-lsn done)) (long lsn))))
                         (reusable? current (:completed state) latest-lsn reuse-ms))
                     {:completed (:completed state)}

                     :else
                     (let [pending {:lsn wanted :result (promise)
                                    :started-nanos (System/nanoTime)}]
                       (reset! publisher (assoc state :pending pending))
                       {:pending pending :owner? true}))))]
           (if completed
             (if-let [error (:error completed)] (throw error) completed)
             (let [outcome
                   (if owner?
                     (let [outcome (assoc
                                     (try (submit m (:lsn pending) (remaining-ms deadline))
                                          (catch Throwable t {:error t}))
                                     :published-lsn (:lsn pending)
                                     :publication-start-nanos (:started-nanos pending))]
                       (locking publisher
                         (when (identical? pending (:pending @publisher))
                           (swap! publisher assoc :pending nil :completed outcome))
                         (deliver (:result pending) outcome))
                       outcome)
                     (deref (:result pending) (remaining-ms deadline) ::timeout))]
               (when (= ::timeout outcome)
                 (raise "HA publication timed out"
                        {:error :ha/control-timeout :where :lease-publication}))
               (if (or (<= (long lsn) (long (:lsn pending)))
                       (covers? m outcome lsn))
                 (if-let [error (:error outcome)] (throw error) outcome)
                 (recur)))))))
     ;; Standalone HA state users may not install server runtime coordination.
     (submit m lsn timeout-ms))))
