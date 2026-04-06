;;
;; Copyright (c) Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2.0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns ^:no-doc datalevin.server.ha
  "High Availability helpers and runtime state."
  (:require
   [clojure.string :as s]
   [datalevin.constants :as c]
   [datalevin.binding.cpp :as cpp]
   [datalevin.ha :as dha]
   [datalevin.ha.authority :as auth]
   [datalevin.ha.control :as ctrl]
   [datalevin.interface :as i]
   [datalevin.ha.replication :as drep]
   [datalevin.ha.util :as hu]
   [datalevin.txlog :as txlog]
   [datalevin.util :as u]
   [taoensso.timbre :as log])
  (:import
   [java.util.concurrent ConcurrentHashMap]
   [java.util.concurrent CountDownLatch ExecutorService Future FutureTask
    Semaphore TimeUnit]
   [java.util.concurrent.atomic AtomicBoolean]
   [datalevin.storage Store]))

(def missing-state-value
  (Object.))

(def ^:private identity-sensitive-state-keys
  #{:store
    :dt-db
    :wdt-db
    :wlmdb
    :runner
    :engine
    :index
    :ha-authority
    :ha-renew-loop-running?
    :ha-renew-loop-future
    :ha-renew-loop-stopped-latch
    :ha-follower-loop-running?
    :ha-follower-loop-future
    :ha-follower-loop-stopped-latch
    :lock
    :open-lock
    :ha-write-admission-lock
    :runtime-access-lock})

(defn- state-value-changed?
  [k expected-v next-v]
  (if (contains? identity-sensitive-state-keys k)
    (not (identical? expected-v next-v))
    (not (= expected-v next-v))))

(def ha-follower-local-side-effect-keys
  [:store
   :dt-db
   :engine
   :index
   :ha-local-last-applied-lsn])

(def ha-follower-side-effect-keys
  [:store
   :dt-db
   :engine
   :index
   :ha-local-last-applied-lsn
   :ha-local-persisted-lsn
   :ha-local-last-persisted-applied-ms
   :ha-follower-last-apply-readback
   :ha-follower-last-batch-records
   :ha-follower-batches-since-persist
   :ha-follower-next-lsn
   :ha-follower-last-batch-size
   :ha-follower-last-sync-ms
   :ha-follower-leader-endpoint
   :ha-follower-source-endpoint
   :ha-follower-source-order
   :ha-follower-last-bootstrap-ms
   :ha-follower-bootstrap-source-endpoint
   :ha-follower-bootstrap-snapshot-last-applied-lsn
   :ha-follower-sync-backoff-ms
   :ha-follower-next-sync-not-before-ms
   :ha-follower-degraded?
   :ha-follower-degraded-reason
   :ha-follower-degraded-details
   :ha-follower-degraded-since-ms
   :ha-follower-last-error
   :ha-follower-last-error-details
   :ha-follower-last-error-ms])

(defn state-patch
  [keys expected-state next-state]
  (let [patch (reduce
               (fn [acc k]
                 (let [expected-v (get expected-state k missing-state-value)
                       next-v     (get next-state k missing-state-value)]
                   (if (state-value-changed? k expected-v next-v)
                     (assoc acc k next-v)
                     acc)))
               {}
               keys)]
    (when (seq patch)
      patch)))

(defn ha-follower-local-side-effect-patch
  [expected-state next-state]
  (when (= :follower (:ha-role expected-state))
    (state-patch ha-follower-local-side-effect-keys
                 expected-state
                 next-state)))

(defn ha-follower-side-effect-patch
  [expected-state next-state]
  (when (= :follower (:ha-role expected-state))
    (state-patch ha-follower-side-effect-keys
                 expected-state
                 next-state)))

(def ha-renew-merge-excluded-keys
  (into #{:ha-leader-last-applied-lsn
          :ha-renew-loop-running?
          :ha-renew-loop-future
          :ha-follower-loop-running?
          :ha-follower-loop-future}
        ha-follower-side-effect-keys))

(defn ha-renew-state-patch
  [expected-state next-state]
  (let [patch-keys (->> (concat (keys expected-state)
                                (keys next-state))
                        distinct
                        (remove ha-renew-merge-excluded-keys))]
    (state-patch patch-keys
                 expected-state
                 next-state)))

(defn ha-authority-refresh-state-patch
  [expected-state next-state]
  (let [patch-keys (->> (concat (keys expected-state)
                                (keys next-state))
                        distinct)]
    (state-patch patch-keys
                 expected-state
                 next-state)))

(defn apply-state-patch
  [state patch]
  (reduce-kv
   (fn [acc k v]
     (if (identical? missing-state-value v)
       (dissoc acc k)
       (assoc acc k v)))
   state
   patch))

(defn same-ha-runtime-context?
  [current-state expected-state]
  (and (identical? (:ha-authority current-state)
                   (:ha-authority expected-state))
       (= (:ha-node-id current-state)
          (:ha-node-id expected-state))
       (= (:ha-db-identity current-state)
          (:ha-db-identity expected-state))
       (= (:ha-membership-hash current-state)
          (:ha-membership-hash expected-state))))

(defn same-ha-runtime-state?
  [current-state expected-state running-key]
  (and (identical? (get current-state running-key)
                   (get expected-state running-key))
       (same-ha-runtime-context? current-state expected-state)))

(defn merge-ha-follower-local-side-effect-patch
  [current-state expected-state patch]
  (if (and patch
           (same-ha-runtime-state?
            current-state
            expected-state
            :ha-follower-loop-running?))
    (apply-state-patch current-state patch)
    current-state))

(defn merge-ha-follower-side-effect-patch
  [current-state expected-state local-patch patch]
  (let [merged-state
        ;; `local-patch` never carries `:ha-role`, but merge it first so the
        ;; follower side-effect guard reads from the actual post-local-patch
        ;; state rather than the original input binding.
        (merge-ha-follower-local-side-effect-patch
         current-state
         expected-state
         local-patch)]
    (if (and patch
             (= :follower (:ha-role merged-state))
             (= :follower (:ha-role expected-state))
             (same-ha-runtime-state?
              merged-state
              expected-state
              :ha-follower-loop-running?))
      (apply-state-patch merged-state patch)
      merged-state)))

(defn merge-ha-renew-state-patch
  [current-state expected-state patch]
  (if (and patch
           (same-ha-runtime-state?
            current-state
            expected-state
            :ha-renew-loop-running?))
    (apply-state-patch current-state patch)
    current-state))

(defn merge-ha-renew-promotion-state-patch
  [current-state expected-state patch]
  (if (and patch
           ;; Promotion fallback must not let a stale renew loop publish a
           ;; leader transition after the renew loop has been restarted.
           (same-ha-runtime-state?
            current-state
            expected-state
            :ha-renew-loop-running?))
    (apply-state-patch current-state patch)
    current-state))

(defn merge-ha-authority-refresh-state-patch
  [current-state expected-state patch]
  (if (and patch
           (= :leader (:ha-role current-state))
           (= :leader (:ha-role expected-state))
           (satisfies? ctrl/ILeaseAuthority
                       (:ha-authority current-state))
           (same-ha-runtime-state?
            current-state
            expected-state
            :ha-renew-loop-running?))
    (apply-state-patch current-state patch)
    current-state))

(defn persist-ha-follower-side-effects!
  [expected-state next-state local-patch]
  ;; Follower replay now throttles `:ha/local-applied-lsn` persistence inside
  ;; the HA sync step and relies on txlog watermarks for crash recovery between
  ;; flushes. Keep the server-side publication hook free of extra per-batch KV
  ;; writes so catch-up can amortize those tiny updates.
  next-state)

(defn consensus-ha-opts
  [store]
  (dha/consensus-ha-opts store))

(def ha-runtime-option-keys
  [:ha-mode
   :db-identity
   :ha-node-id
   :ha-members
   :ha-lease-renew-ms
   :ha-lease-timeout-ms
   :ha-write-admission-lease-margin-ms
   :ha-promotion-base-delay-ms
   :ha-promotion-rank-delay-ms
   :ha-max-promotion-lag-lsn
   :ha-follower-max-batch-records
   :ha-follower-target-batch-bytes
   :ha-follower-persist-every-batches
   :ha-follower-persist-interval-ms
   :ha-client-credentials
   :ha-demotion-drain-ms
   :ha-fencing-hook
   :ha-clock-skew-budget-ms
   :ha-clock-skew-hook
   :ha-control-plane])

(def ha-runtime-option-key-set
  (set ha-runtime-option-keys))

(defn sanitize-ha-path-segment
  [x]
  (-> x
      str
      (s/replace #"[^A-Za-z0-9._-]" "_")))

(defn default-ha-control-raft-dir
  [root db-name ha-opts]
  (let [cp            (:ha-control-plane ha-opts)
        group-id      (sanitize-ha-path-segment (:group-id cp))
        local-peer-id (sanitize-ha-path-segment (:local-peer-id cp))
        db-segment    (u/hexify-string db-name)]
    (str root
         u/+separator+
         "ha-control"
         u/+separator+
         group-id
         u/+separator+
         local-peer-id
         u/+separator+
         db-segment)))

(defn with-default-ha-control-raft-dir
  [root db-name ha-opts]
  (if (and (= :sofa-jraft (get-in ha-opts [:ha-control-plane :backend]))
           (nil? (get-in ha-opts [:ha-control-plane :raft-dir])))
    (assoc-in ha-opts
              [:ha-control-plane :raft-dir]
              (default-ha-control-raft-dir root db-name ha-opts))
    ha-opts))

(defn start-ha-authority
  ([db-name ha-opts]
   (dha/start-ha-authority db-name ha-opts))
  ([db-name root ha-opts]
   (dha/start-ha-authority
    db-name
    (with-default-ha-control-raft-dir root db-name ha-opts))))

(defn stop-ha-authority
  [db-name m]
  (dha/stop-ha-authority db-name m))

(defn ha-loop-sleep-ms
  ([m]
   (ha-loop-sleep-ms m (System/currentTimeMillis)))
  ([m now-ms]
   (let [renew-ms  (long (max 100
                              (long (or (:ha-lease-renew-ms m)
                                        c/*ha-lease-renew-ms*))))
         role      (:ha-role m)
         deadlines (cond-> []
                     (and (= :candidate role)
                          (integer? (:ha-candidate-since-ms m))
                          (integer? (:ha-candidate-delay-ms m)))
                     (conj (+ (long (:ha-candidate-since-ms m))
                              (long (:ha-candidate-delay-ms m))))

                     (and (= :candidate role)
                          (integer? (:ha-candidate-pre-cas-wait-until-ms m)))
                     (conj (long (:ha-candidate-pre-cas-wait-until-ms m)))

                     (and (= :demoting role)
                          (or (integer? (:ha-demotion-drain-until-ms m))
                              (integer? (:ha-demoted-at-ms m))))
                     (conj (long (or (:ha-demotion-drain-until-ms m)
                                     (:ha-demoted-at-ms m)))))
         next-deadline (when (seq deadlines)
                         (long (reduce min (map long deadlines))))
         remaining-ms  (when (some? next-deadline)
                         (let [delta (- (long next-deadline)
                                        (long now-ms))]
                           (long (if (neg? delta) 0 delta))))]
     (if (some? remaining-ms)
       (cond
         (< (long remaining-ms) 1) 1
         (< (long remaining-ms) renew-ms) (long remaining-ms)
         :else renew-ms)
       renew-ms))))

(defn ha-follower-loop-sleep-ms
  ([m]
   (ha-follower-loop-sleep-ms m (System/currentTimeMillis)))
  ([m now-ms]
   (let [renew-ms (long (max 100
                             (long (or (:ha-lease-renew-ms m)
                                       c/*ha-lease-renew-ms*))))
         idle-ms  (long (min 250
                             (max 25 (quot renew-ms 4))))
         role     (:ha-role m)
         next-sync-not-before-ms
         (when (integer? (:ha-follower-next-sync-not-before-ms m))
           (long (:ha-follower-next-sync-not-before-ms m)))
         batch-size (long (or (:ha-follower-last-batch-size m) 0))]
     (cond
       (and (= :follower role)
            next-sync-not-before-ms)
       (let [remaining-ms (- (long next-sync-not-before-ms) (long now-ms))]
         (long (if (<= remaining-ms 0) 1 remaining-ms)))

       (and (= :follower role)
            (pos? batch-size))
       0

       :else
       idle-ms))))

(defn sleep-ha-loop!
  [^AtomicBoolean running? sleep-ms]
  (when (pos? (long sleep-ms))
    (try
      (Thread/sleep (long sleep-ms))
      (catch InterruptedException _
        (.set running? false)))))

(defn ha-loop-error-backoff!
  [^AtomicBoolean running?]
  (try
    (Thread/sleep 250)
    (catch InterruptedException _
      (.set running? false))))

(defn await-ha-loop-stop
  [loop-label db-name latch]
  (when (instance? CountDownLatch latch)
    (try
      (when-not (.await ^CountDownLatch latch 5 TimeUnit/SECONDS)
        (log/warn "Timed out waiting for HA loop to stop"
                  {:db-name db-name
                   :loop loop-label}))
      (catch InterruptedException _
        (.interrupt (Thread/currentThread))
        (log/warn "Interrupted while waiting for HA loop to stop"
                  {:db-name db-name
                   :loop loop-label})))))

(defn shared-store-lifecycle?
  [a b]
  (or (identical? a b)
      (and (instance? Store a)
           (instance? Store b)
           (identical? (.-lmdb ^Store a)
                       (.-lmdb ^Store b)))))

(defn ha-authority-running?
  [authority]
  (if authority
    (let [diagnostics (try
                        (ctrl/authority-diagnostics authority)
                        (catch Throwable _
                          nil))]
      (if (contains? diagnostics :running?)
        (true? (:running? diagnostics))
        true))
    false))

(defn ha-follower-apply-record-with-guard
  [deps server db-name expected-state record]
  (let [^Semaphore lock ((:get-lock-fn deps) server db-name)]
    (.acquire lock)
    (try
      (locking ((:db-write-admission-lock-fn deps) server db-name)
        (let [current-state (get ((:dbs-fn deps) server) db-name)]
          (if (and current-state
                   (= :follower (:ha-role current-state))
                   (same-ha-runtime-state?
                    current-state
                    expected-state
                    :ha-follower-loop-running?))
            (drep/apply-ha-follower-txlog-record! expected-state record)
            (u/raise "HA follower replay aborted because follower state changed"
                     {:error :ha/follower-stale-state
                      :db-name db-name
                      :record-lsn (:lsn record)
                      :state current-state
                      :current-role (:ha-role current-state)
                      :expected-role (:ha-role expected-state)}))))
      (finally
        (.release lock)))))

(defn with-ha-follower-replay-quiesced
  [deps server db-name f]
  (let [^Semaphore lock ((:get-lock-fn deps) server db-name)]
    (.acquire lock)
    (try
      (locking ((:db-write-admission-lock-fn deps) server db-name)
        (f))
      (finally
        (.release lock)))))

(defn- ha-renew-promotion-result?
  [expected-state next-state]
  (and (not (contains? #{:leader :demoting} (:ha-role expected-state)))
       (contains? #{:leader :demoting} (:ha-role next-state))
       (= (:ha-node-id next-state)
          (:ha-authority-owner-node-id next-state))))

(defn publish-ha-renew-state!
  [deps server db-name expected-state next-state ^AtomicBoolean running?]
  (let [renew-patch (ha-renew-state-patch expected-state next-state)
        promotion-result? (ha-renew-promotion-result? expected-state next-state)
        publish!
        (fn []
          (let [{:keys [updated? state]}
                ((:replace-db-state-if-current-fn deps)
                 server
                 db-name
                 expected-state
                 #(identical? running? (:ha-renew-loop-running? %))
                 next-state)
                state
                (if (and (not updated?) renew-patch)
                  (:state
                   ((:transform-db-state-when-fn deps)
                    server
                    db-name
                    #(identical? running? (:ha-renew-loop-running? %))
                    #(merge-ha-renew-state-patch
                      %
                      expected-state
                      renew-patch)))
                  state)
                state
                (if (and promotion-result?
                         renew-patch
                         (or (nil? state)
                             (not (contains? #{:leader :demoting}
                                             (:ha-role state)))))
                  (:state
                   ((:transform-db-state-when-fn deps)
                    server
                    db-name
                    #(same-ha-runtime-state? %
                                            expected-state
                                            :ha-renew-loop-running?)
                    #(merge-ha-renew-promotion-state-patch
                      %
                      expected-state
                      renew-patch)))
                  state)]
            (when (and promotion-result?
                       (or (nil? state)
                           (not (contains? #{:leader :demoting}
                                           (:ha-role state)))))
              (log/warn "HA renew promotion could not publish local leader state"
                        {:db-name db-name
                         :expected-role (:ha-role expected-state)
                         :next-role (:ha-role next-state)
                         :state-role (:ha-role state)}))
            state))]
    (if (and promotion-result?
             (= :follower (:ha-role expected-state)))
      ;; Followers and leaders share the same underlying store handle. Serialize
      ;; follower replay and follower->leader publication so replay cannot keep
      ;; mutating the store after local promotion publishes.
      (with-ha-follower-replay-quiesced deps server db-name publish!)
      (publish!))))

(defn run-ha-renew-loop
  [deps server db-name ^AtomicBoolean running? ^CountDownLatch stopped-latch]
  (try
    (loop []
      (when (and (.get running?)
                 (.get ^AtomicBoolean ((:running-fn deps) server)))
        (try
          (let [m (get ((:dbs-fn deps) server) db-name)]
            (if (or (nil? m)
                    (nil? (:ha-authority m))
                    (not (identical? running?
                                     (:ha-renew-loop-running? m))))
              (.set running? false)
              ;; Keep renew work outside `update-db` so HA probes and peer/server
              ;; operations do not block on control-plane I/O.
              (let [next-state (binding [drep/*ha-current-state-fn*
                                         #(get ((:dbs-fn deps) server) db-name)
                                         drep/*ha-with-local-store-read-fn*
                                         (fn [f]
                                           ((:with-db-runtime-store-read-access-fn deps)
                                            server
                                            db-name
                                            f))]
                                 ((:ha-renew-step-fn deps) db-name m))
                    state (publish-ha-renew-state!
                           deps server db-name m next-state running?)]
                (if (or (nil? state)
                        (nil? (:ha-authority state))
                        (not (identical? running?
                                         (:ha-renew-loop-running? state))))
                  (.set running? false)
                  ((:sleep-ha-loop-fn deps)
                   running?
                   ((:ha-loop-sleep-ms-fn deps) state))))))
          (catch Throwable t
            ((:log-ha-loop-crash!-fn deps)
             "HA renew loop crashed; retrying after backoff"
             db-name
             t)
            ((:ha-loop-error-backoff-fn deps) running?)))
        (recur)))
    (finally
      (.countDown stopped-latch))))

(defn run-ha-follower-sync-loop
  [deps server db-name ^AtomicBoolean running? ^CountDownLatch stopped-latch]
  (try
    (loop []
      (when (and (.get running?)
                 (.get ^AtomicBoolean ((:running-fn deps) server)))
        (try
          (let [m (get ((:dbs-fn deps) server) db-name)]
            (if (or (nil? m)
                    (nil? (:ha-authority m))
                    (not (identical? running?
                                     (:ha-follower-loop-running? m))))
              (.set running? false)
              ;; Follower replay can block on remote txlog fetch and local apply
              ;; work. Keep it off the authority renew path so lease reads and
              ;; promotions are not rate-limited by replication latency.
              (let [next-state (binding [drep/*ha-current-state-fn*
                                         #(get ((:dbs-fn deps) server) db-name)
                                         drep/*ha-follower-apply-record-fn*
                                         (fn [state record]
                                           (ha-follower-apply-record-with-guard
                                            deps
                                            server
                                            db-name
                                            state
                                            record))
                                         drep/*ha-with-local-store-swap-fn*
                                         (fn [f]
                                           ((:with-db-runtime-store-swap-fn deps)
                                            server
                                            db-name
                                            f))
                                         drep/*ha-with-local-store-read-fn*
                                         (fn [f]
                                           ((:with-db-runtime-store-read-access-fn deps)
                                            server
                                            db-name
                                            f))]
                                 ((:ha-follower-sync-step-fn deps) db-name m))
                    local-patch (ha-follower-local-side-effect-patch
                                 m next-state)
                    side-effect-patch (ha-follower-side-effect-patch
                                       m next-state)
                    _ ((:persist-ha-follower-side-effects!-fn deps)
                       m next-state local-patch)
                    {:keys [updated? state]}
                    ((:replace-db-state-if-current-fn deps)
                     server
                     db-name
                     m
                     #(identical? running? (:ha-follower-loop-running? %))
                     next-state)
                    state
                    (if (and (not updated?)
                             (or local-patch side-effect-patch))
                      (:state
                       ((:transform-db-state-when-fn deps)
                        server
                        db-name
                        #(identical? running? (:ha-follower-loop-running? %))
                        #(merge-ha-follower-side-effect-patch
                          %
                          m
                          local-patch
                          side-effect-patch)))
                      state)]
                (if (or (nil? state)
                        (nil? (:ha-authority state))
                        (not (identical? running?
                                         (:ha-follower-loop-running? state))))
                  (.set running? false)
                  ((:sleep-ha-loop-fn deps)
                   running?
                   ((:ha-follower-loop-sleep-ms-fn deps) state))))))
          (catch Throwable t
            ((:log-ha-loop-crash!-fn deps)
             "HA follower sync loop crashed; retrying after backoff"
             db-name
             t)
            ((:ha-loop-error-backoff-fn deps) running?)))
        (recur)))
    (finally
      (.countDown stopped-latch))))

(defn ensure-ha-renew-loop
  [deps server db-name]
  (let [new-running-v (volatile! nil)
        new-future-v  (volatile! nil)
        stopped-latch-v (volatile! nil)]
    ((:update-db-fn deps)
     server
     db-name
     (fn [m]
       (if (and m
                (:ha-authority m))
         (let [running?    (:ha-renew-loop-running? m)
               loop-future (:ha-renew-loop-future m)
               active?     (and (instance? AtomicBoolean running?)
                                (.get ^AtomicBoolean running?)
                                (instance? Future loop-future)
                                (not (.isDone ^Future loop-future)))]
           (if active?
             m
             (do
               (when (instance? AtomicBoolean running?)
                 (.set ^AtomicBoolean running? false))
               (let [new-running?  (AtomicBoolean. true)
                     stopped-latch (CountDownLatch. 1)
                     future-task
                     (FutureTask.
                      ^Runnable #(run-ha-renew-loop
                                   deps
                                   server
                                   db-name
                                   new-running?
                                   stopped-latch)
                      nil)]
                 (vreset! new-running-v new-running?)
                 (vreset! new-future-v future-task)
                 (vreset! stopped-latch-v stopped-latch)
                 (assoc m
                        :ha-renew-loop-running? new-running?
                        :ha-renew-loop-stopped-latch stopped-latch
                        :ha-renew-loop-future future-task)))))
         m)))
    (when-let [^FutureTask future @new-future-v]
      (try
        (.execute ^ExecutorService ((:work-executor-fn deps) server) future)
        (catch Throwable t
          (when-let [^AtomicBoolean running? @new-running-v]
            (.set running? false))
          (when-let [^CountDownLatch stopped-latch @stopped-latch-v]
            (.countDown stopped-latch))
          (.cancel future true)
          ((:update-db-fn deps)
           server
           db-name
           (fn [m]
             (if (and m
                      (identical? @new-running-v
                                  (:ha-renew-loop-running? m))
                      (identical? future
                                  (:ha-renew-loop-future m)))
               (assoc m
                      :ha-renew-loop-future nil
                      :ha-renew-loop-stopped-latch nil)
               m)))
          (throw t))))))

(defn ensure-ha-follower-sync-loop
  [deps server db-name]
  (let [new-running-v (volatile! nil)
        new-future-v  (volatile! nil)
        stopped-latch-v (volatile! nil)]
    ((:update-db-fn deps)
     server
     db-name
     (fn [m]
       (if (and m
                (:ha-authority m))
         (let [running?    (:ha-follower-loop-running? m)
               loop-future (:ha-follower-loop-future m)
               active?     (and (instance? AtomicBoolean running?)
                                (.get ^AtomicBoolean running?)
                                (instance? Future loop-future)
                                (not (.isDone ^Future loop-future)))]
           (if active?
             m
             (do
               (when (instance? AtomicBoolean running?)
                 (.set ^AtomicBoolean running? false))
               (let [new-running?  (AtomicBoolean. true)
                     stopped-latch (CountDownLatch. 1)
                     future-task
                     (FutureTask.
                      ^Runnable #(run-ha-follower-sync-loop
                                   deps
                                   server
                                   db-name
                                   new-running?
                                   stopped-latch)
                      nil)]
                 (vreset! new-running-v new-running?)
                 (vreset! new-future-v future-task)
                 (vreset! stopped-latch-v stopped-latch)
                 (assoc m
                        :ha-follower-loop-running? new-running?
                        :ha-follower-loop-stopped-latch stopped-latch
                        :ha-follower-loop-future future-task)))))
         m)))
    (when-let [^FutureTask future @new-future-v]
      (try
        (.execute ^ExecutorService ((:work-executor-fn deps) server) future)
        (catch Throwable t
          (when-let [^AtomicBoolean running? @new-running-v]
            (.set running? false))
          (when-let [^CountDownLatch stopped-latch @stopped-latch-v]
            (.countDown stopped-latch))
          (.cancel future true)
          ((:update-db-fn deps)
           server
           db-name
           (fn [m]
             (if (and m
                      (identical? @new-running-v
                                  (:ha-follower-loop-running? m))
                      (identical? future
                                  (:ha-follower-loop-future m)))
               (assoc m
                      :ha-follower-loop-future nil
                      :ha-follower-loop-stopped-latch nil)
               m)))
          (throw t))))))

(defn stop-ha-renew-loop
  [m]
  (when-let [^AtomicBoolean running? (:ha-renew-loop-running? m)]
    (.set running? false))
  (when-let [^Future future (:ha-renew-loop-future m)]
    (.cancel future true)))

(defn stop-ha-follower-sync-loop
  [m]
  (when-let [^AtomicBoolean running? (:ha-follower-loop-running? m)]
    (.set running? false))
  (when-let [^Future future (:ha-follower-loop-future m)]
    (.cancel future true)))

(defn current-ha-runtime-local-opts
  [deps m]
  (dha/merge-ha-runtime-local-opts
   (dha/effective-ha-runtime-local-opts m)
   (some-> ((:current-runtime-opts-fn deps) m)
           dha/select-ha-runtime-local-opts)))

(defn resolved-ha-runtime-opts
  ([deps root db-name store]
   (resolved-ha-runtime-opts deps root db-name store nil nil))
  ([deps root db-name store m]
   (resolved-ha-runtime-opts deps root db-name store m nil))
  ([deps root db-name store m explicit-ha-runtime-opts]
   (when-let [ha-opts ((:consensus-ha-opts-fn deps) store)]
     (let [current-ha-runtime-opts
           (dha/merge-ha-runtime-local-opts
            (current-ha-runtime-local-opts deps m)
            (dha/select-ha-runtime-local-opts explicit-ha-runtime-opts))
           merged-ha-opts
           (dha/merge-ha-runtime-local-opts ha-opts
                                            current-ha-runtime-opts)]
       (-> (with-default-ha-control-raft-dir root db-name merged-ha-opts)
           (select-keys ha-runtime-option-keys))))))

(defn stop-ha-runtime
  [deps db-name m]
  (let [runtime-local-opts
        (current-ha-runtime-local-opts deps m)]
    ((:stop-ha-renew-loop-fn deps) m)
    ((:stop-ha-follower-sync-loop-fn deps) m)
    ((:await-ha-loop-stop-fn deps) :renew db-name
     (:ha-renew-loop-stopped-latch m))
    ((:await-ha-loop-stop-fn deps) :follower db-name
     (:ha-follower-loop-stopped-latch m))
    (try
      (dha/persist-ha-runtime-local-applied-lsn! m)
      (catch Throwable t
        (log/warn t "Failed to persist HA local applied LSN during shutdown"
                  {:db-name db-name
                   :ha-node-id (:ha-node-id m)})))
    ((:stop-ha-authority-fn deps) db-name m)
    (cond-> (dissoc (dha/clear-ha-runtime-state m)
                    :ha-runtime-opts
                    :ha-renew-loop-future
                    :ha-renew-loop-stopped-latch
                    :ha-follower-loop-future
                    :ha-follower-loop-stopped-latch)
      (seq runtime-local-opts)
      (assoc :ha-runtime-local-opts runtime-local-opts))))

(defn- leader-authority-state?
  [m]
  (and (= :leader (:ha-role m))
       (satisfies? ctrl/ILeaseAuthority (:ha-authority m))))

(defn ha-write-admission-error
  [deps server message]
  (let [write?  (dha/ha-write-message? message)
        db-name (nth (:args message) 0 nil)
        m0      (when (and db-name (contains? ((:dbs-fn deps) server) db-name))
                  (if write?
                    ((:update-db-fn deps)
                     server db-name (:ensure-udf-readiness-state-fn deps))
                    (get ((:dbs-fn deps) server) db-name)))
        m       m0]
    (or (and write?
             db-name
             (not (contains? (:udf-admission-exempt-write-types deps)
                             (:type message)))
             ((:udf-write-admission-error-fn deps) db-name m))
        (dha/ha-write-admission-error ((:dbs-fn deps) server) message))))

(defn refresh-ha-write-commit-state!
  [deps server db-name]
  (let [m (get ((:dbs-fn deps) server) db-name)]
    (if-not (leader-authority-state? m)
      m
      (let [timeout-ms (hu/ha-request-timeout-ms
                        m
                        (or (get-in m [:ha-control-plane :operation-timeout-ms])
                            5000))
            next-state (dha/refresh-ha-authority-state db-name m timeout-ms)
            refresh-patch (ha-authority-refresh-state-patch
                           m
                           next-state)
            {:keys [updated? state]}
            ((:replace-db-state-if-current-fn deps)
             server
             db-name
             m
             leader-authority-state?
             next-state)]
        (if (or updated?
                (nil? refresh-patch))
          state
          (:state
           ((:transform-db-state-when-fn deps)
            server
            db-name
            #(and (leader-authority-state? %)
                  (same-ha-runtime-state?
                   %
                   m
                   :ha-renew-loop-running?))
            #(merge-ha-authority-refresh-state-patch
              %
              m
              refresh-patch))))))))

(defn ha-write-commit-admission!
  [deps server message]
  (let [db-name (nth (:args message) 0 nil)]
    (when db-name
      (refresh-ha-write-commit-state! deps server db-name)))
  (when-let [err (ha-write-admission-error deps server message)]
    (u/raise "HA write admission rejected" err)))

(defn ha-write-commit-check-fn
  [deps server message]
  (fn [_]
    (ha-write-commit-admission! deps server message)))

(defn- ha-write-commit-confirmation-error
  [db-name commit-lsn m result]
  (cond-> {:error :ha/write-indeterminate
           :indeterminate? true
           :reason :authority-confirmation-failed
           :db-name db-name
           :ha-role (:ha-role m)
           :ha-node-id (:ha-node-id m)
           :leader-last-applied-lsn (long (or commit-lsn 0))}
    (map? result)
    (assoc :authority-result
           (cond-> (select-keys result
                                [:ok? :reason :term :version
                                 :authority-now-ms])
             (map? (:lease result))
             (assoc :lease
                    (select-keys (:lease result)
                                 [:leader-node-id
                                  :leader-endpoint
                                  :term
                                  :lease-until-ms
                                  :leader-last-applied-lsn]))))))

(defn- publish-ha-write-commit-lsn!
  [deps server db-name txlog-lsn]
  (let [txlog-lsn (long (or txlog-lsn 0))]
    (when-not (pos? txlog-lsn)
      (u/raise "HA write commit confirmation failed"
               {:error :ha/write-indeterminate
                :indeterminate? true
                :reason :invalid-txlog-lsn
                :db-name db-name
                :leader-last-applied-lsn txlog-lsn}))
    (locking ((:db-write-admission-lock-fn deps) server db-name)
      (let [m (or (get ((:dbs-fn deps) server) db-name)
                  (u/raise "HA write commit confirmation failed"
                           {:error :ha/write-indeterminate
                            :indeterminate? true
                            :reason :missing-db-state
                            :db-name db-name
                            :leader-last-applied-lsn txlog-lsn}))]
        (when-not (leader-authority-state? m)
          (u/raise "HA write commit confirmation failed"
                   {:error :ha/write-indeterminate
                    :indeterminate? true
                    :reason :not-leader
                    :db-name db-name
                    :ha-role (:ha-role m)
                    :leader-last-applied-lsn txlog-lsn}))
        (let [local-start-ms    (System/currentTimeMillis)
              local-start-nanos (System/nanoTime)
              timeout-ms        (hu/ha-request-timeout-ms
                                 m
                                 (or (get-in m [:ha-control-plane
                                                :operation-timeout-ms])
                                     5000))
              leader-term       (:ha-leader-term m)
              commit-lsn        (long (max txlog-lsn
                                           (long (or (:ha-leader-last-applied-lsn
                                                      m)
                                                     0))
                                           (long (or (get-in m
                                                             [:ha-authority-lease
                                                              :leader-last-applied-lsn])
                                                     0))))
              result            (if (and (integer? leader-term)
                                         (pos? ^long leader-term))
                                  (ctrl/renew-lease
                                   (:ha-authority m)
                                   {:db-identity (:ha-db-identity m)
                                    :leader-node-id (:ha-node-id m)
                                    :leader-endpoint (:ha-local-endpoint m)
                                    :term leader-term
                                    :lease-renew-ms (:ha-lease-renew-ms m)
                                    :lease-timeout-ms (:ha-lease-timeout-ms m)
                                    :leader-last-applied-lsn commit-lsn
                                    :now-ms local-start-ms
                                    :timeout-ms timeout-ms})
                                  {:ok? false
                                   :reason :missing-leader-term})
              observation       (when (auth/control-result-authority-observation?
                                       result)
                                  (auth/control-result-authority-observation
                                   m
                                   local-start-ms
                                   local-start-nanos
                                   result))
              observed-at-ms    (System/currentTimeMillis)]
          (when observation
            ((:transform-db-state-when-fn deps)
             server
             db-name
             #(and (leader-authority-state? %)
                   (same-ha-runtime-state? %
                                           m
                                           :ha-renew-loop-running?))
             (fn [state]
               (-> state
                   (auth/apply-authority-observation observation observed-at-ms)
                   auth/apply-authority-read-success
                   (assoc :ha-leader-last-applied-lsn
                          (long (max commit-lsn
                                     (long (or (:ha-leader-last-applied-lsn
                                                state)
                                               0))
                                     (long (or (get-in state
                                                       [:ha-authority-lease
                                                        :leader-last-applied-lsn])
                                               0)))))))))
          (when-not (:ok? result)
            (u/raise "HA write commit confirmation failed"
                     (ha-write-commit-confirmation-error
                      db-name
                      commit-lsn
                      m
                      result))))))))

(defn ha-write-commit-publish-fn
  [deps server message]
  (fn [{:keys [txlog-lsn]}]
    (let [db-name (nth (:args message) 0 nil)]
      (when db-name
        (publish-ha-write-commit-lsn! deps server db-name txlog-lsn)))))

(defn with-ha-write-admission
  [deps server message f]
  (let [write?  (dha/ha-write-message? message)
        db-name (nth (:args message) 0 nil)
        ^ConcurrentHashMap dbs ((:dbs-fn deps) server)]
    (if (and write? db-name (.containsKey dbs db-name))
      ;; This request-time gate is a cached-state fast path only. The
      ;; authoritative HA check runs again at commit, so one-shot writes do
      ;; not need to serialize through the per-DB admission lock here.
      (if-let [err (ha-write-admission-error deps server message)]
        {:ok? false
         :error err}
        {:ok? true
         :result (f)})
      {:ok? true
       :result (f)})))

(defn cleanup-rejected-close-transact!
  [deps server {:keys [type args]}]
  (let [db-name (nth args 0 nil)
        dbs     ((:dbs-fn deps) server)
        runner  (and db-name (get-in dbs [db-name :runner]))]
    (when (and db-name runner (contains? #{:close-transact
                                          :close-transact-kv}
                                        type))
      (let [kv-store ((:get-kv-store-fn deps) server db-name)
            ^Semaphore lock (get-in dbs [db-name :lock])]
        (try
          (i/abort-transact-kv kv-store)
          (i/close-transact-kv kv-store)
          (catch Throwable t
            (let [details (cond-> {:db-name db-name
                                   :type type
                                   :error-class (.getName ^Class (class t))}
                            (some? (ex-message t))
                            (assoc :message (ex-message t)))]
              (log/warn
               "Failed to clean up rejected HA close-transact"
               details)
              (log/debug t
                         "Rejected HA close-transact cleanup stack trace"
                         {:db-name db-name
                          :type type})))
          (finally
            ((:halt-run-fn deps) runner)
            ((:update-db-fn deps) server db-name
             #(dissoc % :runner :wlmdb :wstore :wdt-db))
            (when lock
              (.release lock))))))))

(defn ensure-ha-runtime
  [deps root db-name m store explicit-ha-runtime-opts]
  (if-let [ha-opts (resolved-ha-runtime-opts
                    deps root db-name store m explicit-ha-runtime-opts)]
    (let [runtime-local-opts (dha/select-ha-runtime-local-opts ha-opts)]
      (if (and (= (:ha-runtime-opts m) ha-opts)
               (ha-authority-running? (:ha-authority m)))
        (assoc m :ha-runtime-local-opts runtime-local-opts)
        (-> (stop-ha-runtime deps db-name m)
            (merge ((:start-ha-authority-fn deps) db-name ha-opts))
            (assoc :ha-runtime-opts ha-opts
                   :ha-runtime-local-opts runtime-local-opts))))
    (dissoc (stop-ha-runtime deps db-name m)
            :ha-runtime-local-opts)))
