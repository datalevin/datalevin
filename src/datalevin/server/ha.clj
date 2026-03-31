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
  "HA runtime helpers extracted from datalevin.server."
  (:require
   [datalevin.binding.cpp :as cpp]
   [datalevin.ha :as dha]
   [datalevin.ha.control :as ctrl]
   [datalevin.interface :as i]
   [datalevin.ha.replication :as drep]
   [datalevin.ha.util :as hu]
   [datalevin.server.ha-runtime :as hrt]
   [datalevin.txlog :as txlog]
   [datalevin.util :as u]
   [taoensso.timbre :as log])
  (:import
   [java.util.concurrent CountDownLatch ExecutorService Future Semaphore]
   [java.util.concurrent.atomic AtomicBoolean]))

(defn ha-follower-apply-record-with-guard
  [deps server db-name expected-state record]
  (let [^Semaphore lock ((:get-lock-fn deps) server db-name)]
    (.acquire lock)
    (try
      (locking ((:db-write-admission-lock-fn deps) server db-name)
        (let [current-state (get ((:dbs-fn deps) server) db-name)]
          (if (and current-state
                   (= :follower (:ha-role current-state))
                   (hrt/same-ha-runtime-state?
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
  (let [renew-patch (hrt/ha-renew-state-patch expected-state next-state)
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
                    #(hrt/merge-ha-renew-state-patch
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
                    #(hrt/same-ha-runtime-state? %
                                                expected-state
                                                :ha-renew-loop-running?)
                    #(hrt/merge-ha-renew-promotion-state-patch
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
                    local-patch (hrt/ha-follower-local-side-effect-patch
                                 m next-state)
                    side-effect-patch (hrt/ha-follower-side-effect-patch
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
                        #(hrt/merge-ha-follower-side-effect-patch
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
  (let [new-running-v (volatile! nil)]
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
                     stopped-latch (CountDownLatch. 1)]
                 (vreset! new-running-v new-running?)
                 (assoc m
                        :ha-renew-loop-running? new-running?
                        :ha-renew-loop-stopped-latch stopped-latch
                        :ha-renew-loop-future nil)))))
         m)))
    (when-let [running? @new-running-v]
      (let [stopped-latch
            (get-in ((:dbs-fn deps) server)
                    [db-name :ha-renew-loop-stopped-latch])
            future (.submit ^ExecutorService
                            ((:work-executor-fn deps) server)
                            ^Runnable #(run-ha-renew-loop
                                         deps
                                         server
                                         db-name
                                         running?
                                         stopped-latch))]
        ((:update-db-fn deps)
         server
         db-name
         (fn [m]
           (if (and m
                    (identical? running?
                                (:ha-renew-loop-running? m)))
             (assoc m :ha-renew-loop-future future)
             m)))))))

(defn ensure-ha-follower-sync-loop
  [deps server db-name]
  (let [new-running-v (volatile! nil)]
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
                     stopped-latch (CountDownLatch. 1)]
                 (vreset! new-running-v new-running?)
                 (assoc m
                        :ha-follower-loop-running? new-running?
                        :ha-follower-loop-stopped-latch stopped-latch
                        :ha-follower-loop-future nil)))))
         m)))
    (when-let [running? @new-running-v]
      (let [stopped-latch
            (get-in ((:dbs-fn deps) server)
                    [db-name :ha-follower-loop-stopped-latch])
            future (.submit ^ExecutorService
                            ((:work-executor-fn deps) server)
                            ^Runnable #(run-ha-follower-sync-loop
                                         deps
                                         server
                                         db-name
                                         running?
                                         stopped-latch))]
        ((:update-db-fn deps)
         server
         db-name
         (fn [m]
           (if (and m
                    (identical? running?
                                (:ha-follower-loop-running? m)))
             (assoc m :ha-follower-loop-future future)
             m)))))))

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
  (hrt/current-ha-runtime-local-opts
   m
   (:current-runtime-opts-fn deps)))

(defn resolved-ha-runtime-opts
  ([deps root db-name store]
   (resolved-ha-runtime-opts deps root db-name store nil nil))
  ([deps root db-name store m]
   (resolved-ha-runtime-opts deps root db-name store m nil))
  ([deps root db-name store m explicit-ha-runtime-opts]
   (hrt/resolved-ha-runtime-opts
    root db-name store m explicit-ha-runtime-opts
    {:consensus-ha-opts-fn (:consensus-ha-opts-fn deps)
     :current-runtime-opts-fn (:current-runtime-opts-fn deps)})))

(defn stop-ha-runtime
  [deps db-name m]
  (hrt/stop-ha-runtime
   db-name
   m
   {:current-runtime-opts-fn (:current-runtime-opts-fn deps)
    :stop-ha-renew-loop-fn (:stop-ha-renew-loop-fn deps)
    :stop-ha-follower-sync-loop-fn (:stop-ha-follower-sync-loop-fn deps)
    :await-ha-loop-stop-fn (:await-ha-loop-stop-fn deps)
    :stop-ha-authority-fn (:stop-ha-authority-fn deps)}))

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
            refresh-patch (hrt/ha-authority-refresh-state-patch
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
                  (hrt/same-ha-runtime-state?
                   %
                   m
                   :ha-renew-loop-running?))
            #(hrt/merge-ha-authority-refresh-state-patch
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

(defn with-ha-write-admission
  [deps server message f]
  (let [write?  (dha/ha-write-message? message)
        db-name (nth (:args message) 0 nil)
        dbs     ((:dbs-fn deps) server)]
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
  (hrt/ensure-ha-runtime
   root
   db-name
   m
   store
   explicit-ha-runtime-opts
   {:resolved-ha-runtime-opts-fn
    (fn [root db-name store m explicit]
      (resolved-ha-runtime-opts deps root db-name store m explicit))
    :start-ha-authority-fn (:start-ha-authority-fn deps)
    :stop-ha-runtime-fn
    (fn [db-name m]
      (stop-ha-runtime deps db-name m))}))
