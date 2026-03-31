;;
;; Copyright (c) Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns ^:no-doc datalevin.ha
  "Consensus-lease HA runtime helpers shared by server runtime."
  (:require
   [clojure.string :as s]
   [datalevin.constants :as c]
   [datalevin.db :as db]
   [datalevin.ha.authority :as auth]
   [datalevin.ha.client-cache :as cache]
   [datalevin.ha.clock :as clock]
   [datalevin.ha.control :as ctrl]
   [datalevin.ha.lease :as lease]
   [datalevin.ha.promotion :as promo]
   [datalevin.ha.replication :as repl]
   [datalevin.ha.snapshot :as snap]
   [datalevin.ha.util :as hu]
   [datalevin.interface :as i]
   [datalevin.kv :as kv]
   [datalevin.remote :as r]
   [datalevin.storage :as st]
   [datalevin.util :as u]
   [datalevin.validate :as vld]
   [taoensso.timbre :as log])
  (:import
   [datalevin.db DB]
   [datalevin.interface IStore ILMDB]
   [datalevin.storage Store]
   [java.net ConnectException URI]
   [java.nio.channels ClosedChannelException]
   [java.util UUID]
   [java.util.concurrent Callable ExecutorCompletionService
    ExecutorService Executors ForkJoinPool Future ThreadFactory TimeUnit]
   [java.util.concurrent.atomic AtomicLong]))

(defn consensus-ha-opts
  [store]
  (when (instance? IStore store)
    (let [opts (i/opts store)]
      (when (= :consensus-lease (:ha-mode opts))
        opts))))

(def select-ha-runtime-local-opts hu/select-ha-runtime-local-opts)

(def merge-ha-runtime-local-opts hu/merge-ha-runtime-local-opts)

(def effective-ha-runtime-local-opts hu/effective-ha-runtime-local-opts)

(defn- local-ha-endpoint
  [ha-opts]
  (let [node-id (:ha-node-id ha-opts)]
    (:endpoint (first (filter #(= node-id (:node-id %))
                              (:ha-members ha-opts))))))

(def ^:private ordered-ha-members hu/ordered-ha-members)

(def ^:private ha-request-timeout-ms hu/ha-request-timeout-ms)

(def ^:private ha-lease-local-remaining-ms
  lease/ha-lease-local-remaining-ms)

(def ^:private ha-write-admission-lease-margin-ms
  lease/ha-write-admission-lease-margin-ms)

(def ^:private ha-write-admission-lease-margin-nanos
  lease/ha-write-admission-lease-margin-nanos)

(def ^:private ha-clock-skew-budget-ms
  lease/ha-clock-skew-budget-ms)

(def ^:private ha-lease-expired-for-promotion?
  lease/ha-lease-expired-for-promotion?)

(def ^:private ha-renew-timeout-ms
  lease/ha-renew-timeout-ms)

(def ^:private long-max2 hu/long-max2)

(def ^:private long-max3 hu/long-max3)

(def ^:private long-max4 hu/long-max4)

(def ^:private long-min2 hu/long-min2)

(def ^:private nonnegative-long-diff hu/nonnegative-long-diff)

(def ^:private ha-local-watermark-snapshot-key
  @#'repl/ha-local-watermark-snapshot-key)

(def ^:redef sync-ha-snapshot-install-target!
  snap/sync-ha-snapshot-install-target!)
(def ha-snapshot-install-marker-path
  snap/ha-snapshot-install-marker-path)
(def ^:private copy-dir-contents! snap/copy-dir-contents!)
(def ^:private move-path! snap/move-path!)
(def ^:private write-ha-snapshot-install-marker!
  snap/write-ha-snapshot-install-marker!)
(def ^:private delete-ha-snapshot-install-marker!
  snap/delete-ha-snapshot-install-marker!)
(def ^:private recover-ha-local-snapshot-install!
  snap/recover-ha-local-snapshot-install!)
(def recover-ha-local-store-dir-if-needed!
  snap/recover-ha-local-store-dir-if-needed!)
(def recover-ha-local-store-if-needed
  #'repl/recover-ha-local-store-if-needed)
(def ^:private close-ha-local-store! snap/close-ha-local-store!)
(def ^:private refresh-ha-local-dt-db snap/refresh-ha-local-dt-db)

(def ^:private closed-kv-store? #'repl/closed-kv-store?)
(def ^:private read-ha-local-persisted-lsn #'repl/read-ha-local-persisted-lsn)
(def ^:redef persist-ha-local-applied-lsn! #'repl/persist-ha-local-applied-lsn!)
(def ^:redef fresh-ha-local-watermark-snapshot
  #'repl/fresh-ha-local-watermark-snapshot)
(def ^:redef read-ha-snapshot-payload-lsn #'repl/read-ha-snapshot-payload-lsn)
(def ^:redef read-ha-local-last-applied-lsn #'repl/read-ha-local-last-applied-lsn)
(def ^:private read-ha-local-watermark-lsn #'repl/read-ha-local-watermark-lsn)
(def persist-ha-runtime-local-applied-lsn!
  #'repl/persist-ha-runtime-local-applied-lsn!)
(def ^:private ha-local-last-applied-lsn #'repl/ha-local-last-applied-lsn)
(def ^:private refresh-ha-local-watermarks #'repl/refresh-ha-local-watermarks)
(def ^:private raw-local-kv-store #'repl/raw-local-kv-store)
(def ^:private reopen-ha-local-store-if-needed
  #'repl/reopen-ha-local-store-if-needed)
(def ^:private ha-local-store-reopen-info #'repl/ha-local-store-reopen-info)
(def ^:redef reopen-ha-local-store-from-info
  #'repl/reopen-ha-local-store-from-info)
(def ^:private ha-promotion-lag-guard #'repl/ha-promotion-lag-guard)
(def ^:private fresh-ha-promotion-local-last-applied-lsn
  #'repl/fresh-ha-promotion-local-last-applied-lsn)
(def ^:private bootstrap-empty-lease? lease/bootstrap-empty-lease?)
(def ^:redef fetch-ha-endpoint-watermark-lsn
  #'repl/fetch-ha-endpoint-watermark-lsn)
(def ^:redef fetch-leader-watermark-lsn #'repl/fetch-leader-watermark-lsn)
(def ^:redef open-ha-snapshot-remote-store!
  #'repl/open-ha-snapshot-remote-store!)
(def ^:redef copy-ha-remote-store!
  #'repl/copy-ha-remote-store!)
(def ^:redef unpin-ha-remote-store-backup-floor!
  #'repl/unpin-ha-remote-store-backup-floor!)
(def ^:redef close-ha-snapshot-remote-store!
  #'repl/close-ha-snapshot-remote-store!)
(def ^:redef fetch-ha-leader-txlog-batch #'repl/fetch-ha-leader-txlog-batch)
(def ^:redef report-ha-replica-floor! #'repl/report-ha-replica-floor!)
(def ^:redef clear-ha-replica-floor! #'repl/clear-ha-replica-floor!)
(def ^:redef fetch-ha-endpoint-snapshot-copy!
  #'repl/fetch-ha-endpoint-snapshot-copy!)
(def ^:private highest-reachable-ha-member-watermark
  #'repl/highest-reachable-ha-member-watermark)
(def ^:private ha-member-watermarks #'repl/ha-member-watermarks)
(def ^:private normalize-leader-watermark-result
  #'repl/normalize-leader-watermark-result)
(def ^:private sync-ha-follower-state #'repl/sync-ha-follower-state)
(def ^:private new-ha-probe-executor #'repl/new-ha-probe-executor)
(def ^:private stop-ha-probe-executor! #'repl/stop-ha-probe-executor!)

(defn- demote-ha-leader
  [db-name m reason details now-ms]
  (promo/demote-ha-leader db-name m reason details now-ms))

(defn- ha-demotion-deadline-ms
  [m]
  (cond
    (integer? (:ha-demotion-drain-until-ms m))
    (long (:ha-demotion-drain-until-ms m))

    (integer? (:ha-demoted-at-ms m))
    (long (:ha-demoted-at-ms m))

    :else nil))

(defn- ha-demotion-draining?
  [m now-ms]
  (let [deadline-ms (ha-demotion-deadline-ms m)]
    (or (= :demoting (:ha-role m))
        (and (integer? deadline-ms)
             (< (long now-ms) (long deadline-ms))))))

(defn- maybe-finish-ha-demotion
  [m now-ms started-demoting?]
  (let [deadline-ms (ha-demotion-deadline-ms m)]
    (if (and started-demoting?
             (= :demoting (:ha-role m))
             (integer? deadline-ms)
             (>= (long now-ms) (long deadline-ms)))
      (assoc m :ha-role :follower)
      m)))

(defn- clear-ha-leader-fencing-state
  [m]
  (dissoc m
          :ha-leader-fencing-pending?
          :ha-leader-fencing-observed-lease
          :ha-leader-fencing-last-error))

(defn ^:redef ha-now-ms
  []
  (System/currentTimeMillis))

(defn ^:redef ha-now-nanos
  []
  (System/nanoTime))

(defn ^:redef maybe-wait-unreachable-leader-before-pre-cas!
  [m lease]
  (promo/maybe-wait-unreachable-leader-before-pre-cas! m lease))

(def ^:private authority-observation-from-state
  auth/authority-observation-from-state)

(def ^:private authority-lease-local-deadline-ms
  auth/authority-lease-local-deadline-ms)

(def ^:private authority-lease-local-deadline-nanos
  auth/authority-lease-local-deadline-nanos)

(def ^:private control-result-authority-observation?
  auth/control-result-authority-observation?)

(def ^:private control-result-authority-observation
  auth/control-result-authority-observation)

(def ^:private apply-authority-observation
  auth/apply-authority-observation)

(def ^:private authority-read-error
  auth/authority-read-error)

(def ^:private apply-authority-read-failure
  auth/apply-authority-read-failure)

(def ^:private apply-authority-read-success
  auth/apply-authority-read-success)

(def ^:private ha-authority-read-fresh?
  auth/ha-authority-read-fresh?)

(def ^:private ha-authority-read-failure-details
  auth/ha-authority-read-failure-details)

(defn ^:redef observe-authority-state
  ([m]
   (auth/observe-authority-state m))
  ([m timeout-ms]
   (auth/observe-authority-state m timeout-ms)))

(defn- run-command-with-timeout
  [cmd env timeout-ms]
  (try
    (let [process-builder (ProcessBuilder. ^java.util.List (mapv str cmd))
          env-map (.environment process-builder)
          _ (doseq [[k v] env]
              (.put env-map (str k) (str v)))
          _ (.redirectErrorStream process-builder true)
          process (.start process-builder)
          finished? (.waitFor process
                              (long timeout-ms)
                              TimeUnit/MILLISECONDS)]
      (if finished?
        (let [exit (.exitValue process)
              output (try
                       (slurp (.getInputStream process) :encoding "UTF-8")
                       (catch Exception _
                         ""))]
          {:ok? (zero? exit)
           :exit exit
           :output output})
        (do
          (.destroy process)
          (when-not (.waitFor process 200 TimeUnit/MILLISECONDS)
            (.destroyForcibly process))
          {:ok? false
           :reason :timeout
           :timeout-ms timeout-ms})))
    (catch Exception e
      {:ok? false
       :reason :exception
       :message (ex-message e)})))

(defn ^:redef run-ha-fencing-hook
  [db-name m observed-lease]
  (if (bootstrap-empty-lease? observed-lease)
    {:ok? true
     :skipped? true
     :reason :bootstrap-empty-lease}
    (let [{:keys [cmd timeout-ms retries retry-delay-ms]} (:ha-fencing-hook m)]
      (if-not (and (vector? cmd) (seq cmd))
        {:ok? false
         :reason :fencing-hook-unconfigured}
        (let [observed-term (lease/observed-term observed-lease)
              candidate-term (lease/next-term observed-lease)
              candidate-id (:ha-node-id m)
              fence-op-id (str db-name ":" observed-term ":" candidate-id)
              fence-shared-op-id (str db-name ":" observed-term)
              env {"DTLV_DB_NAME" db-name
                   "DTLV_OLD_LEADER_NODE_ID"
                   (str (or (:leader-node-id observed-lease) ""))
                   "DTLV_OLD_LEADER_ENDPOINT"
                   (str (or (:leader-endpoint observed-lease) ""))
                   "DTLV_NEW_LEADER_NODE_ID" (str candidate-id)
                   "DTLV_TERM_CANDIDATE" (str candidate-term)
                   "DTLV_TERM_OBSERVED" (str observed-term)
                   "DTLV_FENCE_OP_ID" fence-op-id
                   "DTLV_FENCE_SHARED_OP_ID" fence-shared-op-id}
              max-attempts (inc (long (or retries 0)))
              timeout-ms (long (or timeout-ms 3000))
              retry-delay-ms (long (or retry-delay-ms 1000))]
          (loop [attempt 1]
            (let [result (run-command-with-timeout cmd env timeout-ms)]
              (if (:ok? result)
                (assoc result
                       :attempt attempt
                       :fence-op-id fence-op-id
                       :fence-shared-op-id fence-shared-op-id)
                (if (< attempt max-attempts)
                  (do
                    (Thread/sleep retry-delay-ms)
                    (recur (u/long-inc attempt)))
                  (assoc result
                         :attempt attempt
                         :fence-op-id fence-op-id
                         :fence-shared-op-id fence-shared-op-id))))))))))

(defn- release-ha-leader-lease!
  [db-name m]
  (let [authority (:ha-authority m)
        db-identity (:ha-db-identity m)
        leader-node-id (:ha-node-id m)
        term (:ha-authority-term m)]
    (cond
      (not (satisfies? ctrl/ILeaseAuthority authority))
      {:ok? false
       :reason :authority-unavailable}

      (not (integer? leader-node-id))
      {:ok? false
       :reason :invalid-leader-node-id
       :leader-node-id leader-node-id}

      (not (integer? term))
      {:ok? false
       :reason :invalid-term
       :term term}

      :else
      (try
        (ctrl/release-lease authority
                            {:db-identity db-identity
                             :leader-node-id (long leader-node-id)
                             :term (long term)})
        (catch Exception e
          {:ok? false
           :reason :exception
           :message (ex-message e)
           :data (ex-data e)
           :db-name db-name
           :db-identity db-identity
           :leader-node-id leader-node-id
           :term term})))))

(defn- maybe-complete-ha-leader-fencing
  [m db-name]
  (let [observed-lease (:ha-leader-fencing-observed-lease m)]
    (if-not (and (= :leader (:ha-role m))
                 (true? (:ha-leader-fencing-pending? m)))
      m
      (let [fence-result (try
                           (run-ha-fencing-hook db-name m observed-lease)
                           (catch Exception e
                             {:ok? false
                              :reason :exception
                              :message (ex-message e)
                              :data (ex-data e)}))]
        (if (:ok? fence-result)
          (-> m
              clear-ha-leader-fencing-state
              (assoc :ha-leader-fencing-last-error nil))
          (do
            (log/warn "HA leader fencing incomplete"
                      {:db-name db-name
                       :ha-node-id (:ha-node-id m)
                       :ha-authority-term (:ha-authority-term m)
                       :fence-result fence-result})
            (let [lease-release (release-ha-leader-lease! db-name m)]
              (when-not (and (:ok? lease-release)
                             (true? (:released? lease-release)))
                (log/warn "HA leader fencing failure could not release authoritative lease"
                          {:db-name db-name
                           :ha-node-id (:ha-node-id m)
                           :ha-authority-term (:ha-authority-term m)
                           :lease-release lease-release}))
              (demote-ha-leader db-name m
                                :fencing-incomplete
                                {:fence-result fence-result
                                 :lease-release lease-release}
                                (ha-now-ms)))))))))

(def ^:private parse-ha-clock-skew-output
  clock/parse-ha-clock-skew-output)

(defn ^:redef run-ha-clock-skew-hook
  [db-name m]
  (clock/run-ha-clock-skew-hook db-name m))

(def ^:dynamic *ha-with-local-store-swap-fn*
  (fn [f]
    (f)))

(defn- with-ha-local-store-swap
  [f]
  (*ha-with-local-store-swap-fn* f))

(def ^:private ha-clock-skew-hook-configured?
  clock/ha-clock-skew-hook-configured?)

(def ^:private ha-clock-skew-check-fresh?
  clock/ha-clock-skew-check-fresh?)

(def ^:private ha-clock-skew-promotion-block-reason
  clock/ha-clock-skew-promotion-block-reason)

(def ^:private ha-clock-skew-promotion-failure-details
  clock/ha-clock-skew-promotion-failure-details)

(defn ^:redef ha-follower-sync-step
  [db-name m]
  (if-not (:ha-authority m)
    m
    (let [m0 (refresh-ha-local-watermarks m)]
      (cond-> (if (= :follower (:ha-role m0))
                (sync-ha-follower-state db-name m0 (ha-now-ms))
                m0)
        :always
        (dissoc ha-local-watermark-snapshot-key)))))

(defn- authority-deps
  []
  {:demote-ha-leader-fn demote-ha-leader
   :ha-now-ms-fn ha-now-ms
   :observe-authority-state-fn observe-authority-state})

(defn- clock-deps
  []
  {:demote-ha-leader-fn demote-ha-leader
   :ha-now-ms-fn ha-now-ms
   :run-ha-clock-skew-hook-fn run-ha-clock-skew-hook})

(defn- promotion-deps
  []
  {:ordered-ha-members-fn ordered-ha-members
   :ha-now-ms-fn ha-now-ms
   :ha-demotion-draining?-fn ha-demotion-draining?
   :maybe-wait-unreachable-leader-before-pre-cas-fn
   maybe-wait-unreachable-leader-before-pre-cas!
   :maybe-complete-ha-leader-fencing-fn maybe-complete-ha-leader-fencing
   :fetch-leader-watermark-lsn-fn fetch-leader-watermark-lsn
   :fresh-ha-promotion-local-last-applied-lsn-fn
   fresh-ha-promotion-local-last-applied-lsn
   :ha-member-watermarks-fn ha-member-watermarks
   :highest-reachable-ha-member-watermark-fn
   highest-reachable-ha-member-watermark
   :normalize-leader-watermark-result-fn normalize-leader-watermark-result
   :ha-promotion-lag-guard-fn ha-promotion-lag-guard
   :ha-local-last-applied-lsn-fn ha-local-last-applied-lsn
   :observe-authority-state-fn observe-authority-state})

(defn- refresh-ha-clock-skew-state
  [db-name m]
  (clock/refresh-ha-clock-skew-state (clock-deps) db-name m))

(defn- maybe-enter-ha-candidate
  [m now-ms]
  (promo/maybe-enter-ha-candidate (promotion-deps) m now-ms))

(defn- attempt-ha-candidate-promotion
  [db-name m now-ms]
  (promo/attempt-ha-candidate-promotion (promotion-deps) db-name m now-ms))

(defn- maybe-promote-ha-candidate
  [db-name m now-ms]
  (promo/maybe-promote-ha-candidate (promotion-deps) db-name m now-ms))

(defn- advance-ha-follower-or-candidate
  [db-name m]
  (promo/advance-ha-follower-or-candidate (promotion-deps) db-name m))

(defn refresh-ha-authority-state
  ([db-name m]
   (refresh-ha-authority-state db-name m nil))
  ([db-name m timeout-ms]
   (auth/refresh-ha-authority-state (authority-deps) db-name m timeout-ms)))

(defn- finish-ha-leader-renew
  [db-name m result local-start-ms local-start-nanos]
  (if (control-result-authority-observation? result)
    (let [observation
          (control-result-authority-observation
           m
           local-start-ms
           local-start-nanos
           result)
          {:keys [lease authority-membership-hash
                  db-identity-mismatch? membership-mismatch?]}
          observation
          now-ms (ha-now-ms)
          observed-m (-> m
                         (apply-authority-observation observation now-ms)
                         apply-authority-read-success)]
      (cond
        db-identity-mismatch?
        (demote-ha-leader db-name observed-m
                          :db-identity-mismatch
                          {:local-db-identity (:ha-db-identity m)
                           :authority-lease lease}
                          now-ms)

        membership-mismatch?
        (demote-ha-leader db-name observed-m
                          :membership-hash-mismatch
                          {:local-membership-hash
                           (:ha-membership-hash observed-m)
                           :authority-membership-hash
                           authority-membership-hash}
                          now-ms)

        (:ok? result)
        (maybe-complete-ha-leader-fencing observed-m db-name)

        :else
        (demote-ha-leader db-name observed-m
                          :renew-failed
                          {:reason (:reason result)}
                          now-ms)))
    (if (:ok? result)
      (let [{:keys [lease version authority-now-ms]} result
            observed-at-ms (ha-now-ms)]
        (-> m
            apply-authority-read-success
            (assoc :ha-authority-lease lease
                   :ha-authority-version version
                   :ha-authority-now-ms authority-now-ms
                   :ha-leader-term (:term lease)
                   :ha-lease-local-deadline-ms
                   (authority-lease-local-deadline-ms
                    lease authority-now-ms local-start-ms)
                   :ha-lease-local-deadline-nanos
                   (authority-lease-local-deadline-nanos
                    lease authority-now-ms local-start-nanos)
                   :ha-authority-owner-node-id (:leader-node-id lease)
                   :ha-authority-term (:term lease)
                   :ha-lease-until-ms (:lease-until-ms lease)
                   :ha-last-authority-refresh-ms observed-at-ms)
            (maybe-complete-ha-leader-fencing db-name)))
      (demote-ha-leader db-name m
                        :renew-failed
                        {:reason (:reason result)}
                        (ha-now-ms)))))

(defn- renew-ha-leader-state
  [db-name m]
  (if (not= :leader (:ha-role m))
    m
    (let [m (if (true? (:ha-leader-fencing-pending? m))
              (maybe-complete-ha-leader-fencing m db-name)
              m)]
      (if (not= :leader (:ha-role m))
        m
        (let [local-start-ms (ha-now-ms)
              local-start-nanos (ha-now-nanos)
              renew-timeout-ms (ha-renew-timeout-ms
                                m
                                local-start-ms
                                local-start-nanos)
              term (:ha-leader-term m)]
          (if-not (and (integer? term) (pos? ^long term))
            (demote-ha-leader db-name m :missing-leader-term nil local-start-ms)
            (let [result (ctrl/renew-lease
                          (:ha-authority m)
                          {:db-identity (:ha-db-identity m)
                           :leader-node-id (:ha-node-id m)
                           :leader-endpoint (:ha-local-endpoint m)
                           :term term
                           :lease-renew-ms (:ha-lease-renew-ms m)
                           :lease-timeout-ms (:ha-lease-timeout-ms m)
                           :leader-last-applied-lsn
                           (long (or (:ha-leader-last-applied-lsn m) 0))
                           :now-ms local-start-ms
                           :timeout-ms renew-timeout-ms})]
              (finish-ha-leader-renew
               db-name
               m
               result
               local-start-ms
               local-start-nanos))))))))

(defn- maybe-demote-on-refresh-timeout
  [db-name m now-ms]
  (let [timeout-ms (:ha-lease-timeout-ms m)
        last-ms (:ha-last-authority-refresh-ms m)]
    (if (and (= :leader (:ha-role m))
             (integer? timeout-ms)
             (pos? ^long timeout-ms)
             (integer? last-ms)
             (>= (- (long now-ms) (long last-ms))
                 (long timeout-ms)))
      (demote-ha-leader db-name m :authority-refresh-timeout nil now-ms)
      m)))

(defn ha-renew-step
  [db-name m]
  (if-not (:ha-authority m)
    m
    (let [started-demoting? (= :demoting (:ha-role m))
          m0 (refresh-ha-local-watermarks m)
          m1 (if (= :leader (:ha-role m0))
               (try
                 (renew-ha-leader-state db-name m0)
                 (catch Exception e
                   (demote-ha-leader db-name m0
                                     :renew-exception
                                     {:message (ex-message e)}
                                     (ha-now-ms))))
               (refresh-ha-authority-state db-name m0))
          m2 (refresh-ha-clock-skew-state db-name m1)
          m3 (advance-ha-follower-or-candidate db-name m2)
          end-now-ms (ha-now-ms)]
      (-> (maybe-demote-on-refresh-timeout db-name m3 end-now-ms)
          (maybe-finish-ha-demotion end-now-ms started-demoting?)
          (dissoc ha-local-watermark-snapshot-key)))))

(def ^:private ha-runtime-config-clear-keys
  [:ha-authority
   :ha-db-identity
   :ha-membership-hash
   :ha-authority-membership-hash
   :ha-db-identity-mismatch?
   :ha-membership-mismatch?
   :ha-members
   :ha-members-sorted
   :ha-node-id
   :ha-local-endpoint
   :ha-lease-renew-ms
   :ha-lease-timeout-ms
   :ha-write-admission-lease-margin-ms
   :ha-promotion-base-delay-ms
   :ha-promotion-rank-delay-ms
   :ha-max-promotion-lag-lsn
   :ha-demotion-drain-ms
   :ha-follower-max-batch-records
   :ha-follower-target-batch-bytes
   :ha-follower-persist-every-batches
   :ha-follower-persist-interval-ms
   :ha-fencing-hook
   :ha-clock-skew-budget-ms
   :ha-clock-skew-hook
   :ha-client-cache-state
   :ha-probe-executor])

(def ^:private ha-authority-observation-clear-keys
  [:ha-authority-lease
   :ha-authority-version
   :ha-authority-now-ms
   :ha-authority-owner-node-id
   :ha-authority-term
   :ha-lease-until-ms
   :ha-lease-local-deadline-ms
   :ha-lease-local-deadline-nanos
   :ha-last-authority-refresh-ms
   :ha-authority-read-ok?
   :ha-authority-read-error])

(def ^:private ha-clock-skew-clear-keys
  [:ha-clock-skew-paused?
   :ha-clock-skew-refresh-future
   :ha-clock-skew-refresh-pending?
   :ha-clock-skew-last-check-ms
   :ha-clock-skew-last-observed-ms
   :ha-clock-skew-last-result])

(def ^:private ha-leader-state-clear-keys
  [:ha-role
   :ha-leader-term
   :ha-leader-fencing-pending?
   :ha-leader-fencing-observed-lease
   :ha-leader-fencing-last-error
   :ha-demotion-drain-until-ms])

(def ^:private ha-follower-state-clear-keys
  [:ha-local-last-applied-lsn
   :ha-local-persisted-lsn
   :ha-local-last-persisted-applied-ms
   :ha-follower-last-applied-term
   :ha-follower-batches-since-persist
   :ha-follower-next-lsn
   :ha-follower-last-batch-size
   :ha-follower-last-batch-estimated-bytes
   :ha-follower-requested-batch-records
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

(def ^:private ha-promotion-state-clear-keys
  [:ha-promotion-last-failure
   :ha-promotion-failure-details
   :ha-candidate-since-ms
   :ha-candidate-delay-ms
   :ha-candidate-rank-index
   :ha-candidate-pre-cas-wait-until-ms
   :ha-candidate-pre-cas-observed-version
   :ha-promotion-wait-before-cas-ms
   :ha-rejoin-promotion-blocked?
   :ha-rejoin-promotion-blocked-until-ms
   :ha-rejoin-promotion-cleared-ms
   :ha-rejoin-started-at-ms])

(def ^:private ha-runtime-loop-clear-keys
  [:ha-renew-loop-running?
   :ha-follower-loop-running?])

(def ^:private ha-runtime-clear-keys
  (vec
   (concat
    [ha-local-watermark-snapshot-key]
    ha-runtime-config-clear-keys
    ha-authority-observation-clear-keys
    ha-clock-skew-clear-keys
    ha-leader-state-clear-keys
    ha-follower-state-clear-keys
    ha-promotion-state-clear-keys
    ha-runtime-loop-clear-keys)))

(defn clear-ha-runtime-state
  [m]
  (apply dissoc m ha-runtime-clear-keys))

(def ^:private ha-write-command-types
  #{:set-schema
    :swap-attr
    :del-attr
    :rename-attr
    :load-datoms
    :tx-data
    :tx-data+db-info
    :open-transact
    :close-transact
    :abort-transact
    :open-transact-kv
    :close-transact-kv
    :abort-transact-kv
    :transact-kv
    :open-dbi
    :clear-dbi
    :drop-dbi
    :set-env-flags
    :add-doc
    :remove-doc
    :clear-docs
    :add-vec
    :remove-vec
    :persist-vecs
    :clear-vecs
    :kv-re-index
    :datalog-re-index})

(defn ha-write-message?
  [{:keys [type]}]
  (boolean (ha-write-command-types type)))

(defn ha-write-admission-error
  [dbs {:keys [type args]}]
  (when (ha-write-message? {:type type})
    (let [db-name (nth args 0 nil)
          m (and db-name (get dbs db-name))]
      (when (and m (:ha-authority m))
        (let [now-ms (ha-now-ms)
              role (:ha-role m)
              local-node-id (:ha-node-id m)
              owner-node-id (:ha-authority-owner-node-id m)
              owner-endpoint (or (get-in m [:ha-authority-lease :leader-endpoint])
                                 (some->> (:ha-members m)
                                          (filter #(= owner-node-id
                                                      (:node-id %)))
                                          first
                                          :endpoint))
              ordered-endpoints
              (into []
                    (comp
                     (map :endpoint)
                     (remove nil?)
                     (remove s/blank?))
                    (ordered-ha-members m))
              retry-endpoints
              (->> (cond-> []
                     (and (string? owner-endpoint)
                          (not (s/blank? owner-endpoint)))
                     (conj owner-endpoint)
                     :always
                     (into ordered-endpoints))
                   distinct
                   vec)
              common-meta
              {:db-name db-name
               :ha-retry-endpoints retry-endpoints
               :ha-authoritative-leader-endpoint owner-endpoint
               :ha-authoritative-leader-node-id owner-node-id}
              authority-read-failure
              (when-not (ha-authority-read-fresh? m now-ms)
                (ha-authority-read-failure-details m now-ms))
              db-id-mismatch? (true? (:ha-db-identity-mismatch? m))
              membership-mismatch? (true? (:ha-membership-mismatch? m))
              lease-until-ms (:ha-lease-until-ms m)
              lease-local-deadline-ms (:ha-lease-local-deadline-ms m)
              lease-local-deadline-nanos (:ha-lease-local-deadline-nanos m)
              lease-admission-margin-ms
              (ha-write-admission-lease-margin-ms m)
              lease-admission-margin-nanos
              (ha-write-admission-lease-margin-nanos m)
              leader-term (:ha-leader-term m)
              authority-term (:ha-authority-term m)
              now-nanos (ha-now-nanos)
              leader-fencing-pending? (true? (:ha-leader-fencing-pending? m))
              leader-fencing-error (:ha-leader-fencing-last-error m)]
          (cond
            (= role :demoting)
            (merge common-meta
                   {:error :ha/write-rejected
                    :reason :demoting
                    :ha-role role
                    :retryable? true})

            (not= role :leader)
            (merge common-meta
                   {:error :ha/write-rejected
                    :reason :not-leader
                    :ha-role role
                    :retryable? true})

            (true? (:ha-clock-skew-paused? m))
            (merge common-meta
                   {:error :ha/write-rejected
                    :reason :clock-skew-paused
                    :ha-clock-skew-last-result
                    (:ha-clock-skew-last-result m)
                    :ha-clock-skew-last-check-ms
                    (:ha-clock-skew-last-check-ms m)
                    :ha-clock-skew-last-observed-ms
                    (:ha-clock-skew-last-observed-ms m)
                    :retryable? true})

            authority-read-failure
            (merge common-meta
                   authority-read-failure
                   {:error :ha/write-rejected
                    :reason (:reason authority-read-failure)
                    :ha-authority-read-error authority-read-failure
                    :retryable? true})

            leader-fencing-pending?
            (merge common-meta
                   {:error :ha/write-rejected
                    :reason :fencing-pending
                    :ha-fencing-error leader-fencing-error
                    :retryable? true})

            db-id-mismatch?
            (merge common-meta
                   {:error :ha/write-rejected
                    :reason :db-identity-mismatch
                    :retryable? false})

            membership-mismatch?
            (merge common-meta
                   {:error :ha/write-rejected
                    :reason :membership-hash-mismatch
                    :ha-membership-hash (:ha-membership-hash m)
                    :ha-authority-membership-hash
                    (:ha-authority-membership-hash m)
                    :retryable? false})

            (not= local-node-id owner-node-id)
            (merge common-meta
                   {:error :ha/write-rejected
                    :reason :owner-mismatch
                    :ha-node-id local-node-id
                    :ha-authority-owner-node-id owner-node-id
                    :retryable? true})

            ;; Admission keys off the translated authoritative lease expiry,
            ;; not renew cadence. A delayed renew loop does not extend this
            ;; deadline; stale control observations are handled separately by
            ;; `ha-authority-read-fresh?` above.
            (or (and (integer? lease-local-deadline-nanos)
                     (>= (+ (long now-nanos)
                            (long lease-admission-margin-nanos))
                         (long lease-local-deadline-nanos)))
                (and (not (integer? lease-local-deadline-nanos))
                     (integer? lease-local-deadline-ms)
                     (>= (+ (long now-ms)
                            (long lease-admission-margin-ms))
                         (long lease-local-deadline-ms)))
                (and (not (integer? lease-local-deadline-nanos))
                     (not (integer? lease-local-deadline-ms))
                     (or (nil? lease-until-ms)
                         (>= (+ (long now-ms)
                                (long lease-admission-margin-ms))
                             (long lease-until-ms)))))
            (merge common-meta
                   {:error :ha/write-rejected
                    :reason :lease-expired
                    :lease-until-ms lease-until-ms
                    :lease-local-deadline-ms lease-local-deadline-ms
                    :lease-local-deadline-nanos lease-local-deadline-nanos
                    :lease-admission-margin-ms lease-admission-margin-ms
                    :ha-authority-now-ms (:ha-authority-now-ms m)
                    :now-ms now-ms
                    :now-nanos now-nanos
                    :retryable? true})

            (or (not (integer? leader-term))
                (not (integer? authority-term))
                (> (long leader-term) (long authority-term)))
            (merge common-meta
                   {:error :ha/write-rejected
                    :reason :term-mismatch
                    :ha-leader-term leader-term
                    :ha-authority-term authority-term
                    :retryable? true})

            :else nil))))))

(defn- startup-read-ha-authority-state
  [db-name authority db-identity]
  (try
    (let [{:keys [lease version authority-now-ms]}
          (ctrl/read-state-for-startup authority db-identity)]
      {:ok? true
       :lease lease
       :version version
       :authority-now-ms authority-now-ms
       :error nil})
    (catch Exception e
      (log/warn e "HA startup read-lease failed; deferring to renew loop"
                {:db-name db-name})
      {:ok? false
       :lease nil
       :version nil
       :authority-now-ms nil
       :error {:reason :startup-authority-read-failed
               :message (ex-message e)
               :data (ex-data e)}})))

(defn start-ha-authority
  [db-name ha-opts]
  (let [validation-opts (cond-> ha-opts
                          (nil? (get-in ha-opts [:ha-control-plane :rpc-timeout-ms]))
                          (assoc-in [:ha-control-plane :rpc-timeout-ms] 2000)

                          (nil? (get-in ha-opts [:ha-control-plane :election-timeout-ms]))
                          (assoc-in [:ha-control-plane :election-timeout-ms]
                                    3000)

                          (nil? (get-in ha-opts [:ha-control-plane :operation-timeout-ms]))
                          (assoc-in [:ha-control-plane :operation-timeout-ms]
                                    5000))
        _ (vld/validate-ha-options validation-opts)
        db-identity (:db-identity ha-opts)
        node-id (:ha-node-id ha-opts)
        members (:ha-members ha-opts)
        ordered-members (some->> members (sort-by :node-id) vec)
        renew-ms (:ha-lease-renew-ms ha-opts)
        timeout-ms (:ha-lease-timeout-ms ha-opts)
        promotion-base-delay-ms
        (long (or (:ha-promotion-base-delay-ms ha-opts)
                  c/*ha-promotion-base-delay-ms*))
        promotion-rank-delay-ms
        (long (or (:ha-promotion-rank-delay-ms ha-opts)
                  c/*ha-promotion-rank-delay-ms*))
        max-promotion-lag-lsn
        (long (or (:ha-max-promotion-lag-lsn ha-opts)
                  c/*ha-max-promotion-lag-lsn*))
        demotion-drain-ms
        (long (or (:ha-demotion-drain-ms ha-opts)
                  c/*ha-demotion-drain-ms*))
        clock-skew-budget-ms
        (long (or (:ha-clock-skew-budget-ms ha-opts)
                  c/*ha-clock-skew-budget-ms*))
        follower-max-batch-records
        (long (or (:ha-follower-max-batch-records ha-opts)
                  c/*ha-follower-max-batch-records*))
        follower-target-batch-bytes
        (long (or (:ha-follower-target-batch-bytes ha-opts)
                  c/*ha-follower-target-batch-bytes*))
        follower-persist-every-batches
        (long-max2 1
                   (long (or (:ha-follower-persist-every-batches ha-opts)
                             c/*ha-follower-persist-every-batches*)))
        follower-persist-interval-ms
        (max 0
             (long (or (:ha-follower-persist-interval-ms ha-opts)
                       c/*ha-follower-persist-interval-ms*)))
        cp (assoc (:ha-control-plane ha-opts)
                  :clock-skew-budget-ms clock-skew-budget-ms)
        fencing-hook (:ha-fencing-hook ha-opts)
        clock-skew-hook (:ha-clock-skew-hook ha-opts)
        local-endpoint (local-ha-endpoint ha-opts)
        client-cache-state (cache/new-ha-client-cache-state db-name)
        probe-executor (new-ha-probe-executor db-name)
        authority (ctrl/new-authority cp)]
    (try
      (ctrl/start-authority! authority)
      (when (or (nil? db-identity) (s/blank? db-identity))
        (u/raise "HA db identity is missing for consensus mode"
                 {:error :ha/missing-db-identity
                  :db-name db-name}))
      (when (or (nil? local-endpoint) (s/blank? local-endpoint))
        (u/raise "HA local endpoint is missing for consensus mode"
                 {:error :ha/missing-local-endpoint
                  :db-name db-name
                  :ha-node-id node-id}))
      (let [local-start-ms (ha-now-ms)
            local-start-nanos (ha-now-nanos)
            derived-hash (vld/derive-ha-membership-hash ha-opts)
            init-result (ctrl/init-membership-hash! authority derived-hash)
            startup-read
            (startup-read-ha-authority-state db-name authority db-identity)
            observed-at-ms (ha-now-ms)
            {:keys [lease version authority-now-ms error]} startup-read
            _ (when (and lease (not= db-identity (:db-identity lease)))
                (u/raise "HA lease db identity mismatch at startup"
                         {:error :ha/db-identity-mismatch
                          :db-name db-name
                          :local-db-identity db-identity
                          :authority-lease lease}))
            lease-local-deadline-ms
            (authority-lease-local-deadline-ms
             lease authority-now-ms local-start-ms)
            lease-local-deadline-nanos
            (authority-lease-local-deadline-nanos
             lease authority-now-ms local-start-nanos)
            local-authority-owner? (and lease
                                        (= node-id (:leader-node-id lease))
                                        (not (ha-lease-expired-for-promotion?
                                              {:ha-clock-skew-budget-ms
                                               clock-skew-budget-ms}
                                              lease
                                              observed-at-ms))
                                        (= db-identity (:db-identity lease)))
            rejoin-promotion-blocked?
            (and lease
                 (integer? (:leader-node-id lease))
                 (not= node-id (:leader-node-id lease))
                 (not (ha-lease-expired-for-promotion?
                       {:ha-clock-skew-budget-ms clock-skew-budget-ms}
                       lease
                       observed-at-ms))
                 (= db-identity (:db-identity lease)))
            rejoin-promotion-blocked-until-ms
            (when rejoin-promotion-blocked?
              (long (max (+ (long observed-at-ms) (long renew-ms))
                         (+ (long (or (:lease-until-ms lease)
                                      observed-at-ms))
                            (long renew-ms)))))]
        (when-not (:ok? init-result)
          (u/raise "HA membership hash mismatch with authoritative control plane"
                   {:error :ha/membership-hash-mismatch
                    :db-name db-name
                    :derived-hash derived-hash
                    :authority init-result}))
        (when local-authority-owner?
          (log/info "HA startup resumed local authority ownership as leader"
                    {:db-name db-name
                     :ha-node-id node-id
                     :term (:term lease)
                     :lease-until-ms (:lease-until-ms lease)}))
        (cond-> {:ha-authority authority
                 :ha-db-identity db-identity
                 :ha-membership-hash derived-hash
                 :ha-authority-membership-hash (:membership-hash init-result)
                 :ha-db-identity-mismatch? false
                 :ha-membership-mismatch? false
                 :ha-client-cache-state client-cache-state
                 :ha-members members
                 :ha-members-sorted ordered-members
                 :ha-node-id node-id
                 :ha-local-endpoint local-endpoint
                 :ha-lease-renew-ms renew-ms
                 :ha-lease-timeout-ms timeout-ms
                 :ha-write-admission-lease-margin-ms
                 (long (or (:ha-write-admission-lease-margin-ms ha-opts)
                           c/*ha-write-admission-lease-margin-ms*
                           0))
                 :ha-promotion-base-delay-ms promotion-base-delay-ms
                 :ha-promotion-rank-delay-ms promotion-rank-delay-ms
                 :ha-max-promotion-lag-lsn max-promotion-lag-lsn
                 :ha-client-credentials (:ha-client-credentials ha-opts)
                 :ha-demotion-drain-ms demotion-drain-ms
                 :ha-follower-max-batch-records follower-max-batch-records
                 :ha-follower-target-batch-bytes follower-target-batch-bytes
                 :ha-follower-persist-every-batches
                 follower-persist-every-batches
                 :ha-follower-persist-interval-ms
                 follower-persist-interval-ms
                 :ha-fencing-hook fencing-hook
                 :ha-clock-skew-budget-ms clock-skew-budget-ms
                 :ha-clock-skew-hook clock-skew-hook
                 :ha-clock-skew-paused? false
                 :ha-clock-skew-refresh-future nil
                 :ha-clock-skew-refresh-pending? false
                 :ha-clock-skew-last-check-ms nil
                 :ha-clock-skew-last-observed-ms nil
                 :ha-clock-skew-last-result nil
                 :ha-authority-lease lease
                 :ha-authority-version version
                 :ha-authority-now-ms authority-now-ms
                 :ha-authority-owner-node-id (:leader-node-id lease)
                 :ha-authority-term (:term lease)
                 :ha-lease-until-ms (:lease-until-ms lease)
                 :ha-lease-local-deadline-ms lease-local-deadline-ms
                 :ha-lease-local-deadline-nanos lease-local-deadline-nanos
                 :ha-authority-read-ok? (:ok? startup-read)
                 :ha-authority-read-error error
                 :ha-last-authority-refresh-ms observed-at-ms
                 :ha-leader-fencing-pending? false
                 :ha-leader-fencing-last-error nil
                 :ha-probe-executor probe-executor
                 :ha-rejoin-promotion-blocked? rejoin-promotion-blocked?
                 :ha-rejoin-promotion-blocked-until-ms
                 rejoin-promotion-blocked-until-ms
                 :ha-rejoin-started-at-ms
                 (when rejoin-promotion-blocked? observed-at-ms)
                 :ha-role :follower
                 :ha-leader-term nil}
          local-authority-owner?
          (assoc :ha-role :leader
                 :ha-leader-term (:term lease))))
      (catch Exception e
        (try
          (ctrl/stop-authority! authority)
          (catch Exception stop-e
            (log/warn stop-e "Failed to stop HA authority after startup failure"
                      {:db-name db-name})))
        (cache/stop-ha-client-cache-state! db-name client-cache-state)
        (stop-ha-probe-executor! db-name probe-executor)
        (throw e)))))

(defn stop-ha-authority
  [db-name m]
  (try
    (when-let [authority (:ha-authority m)]
      (try
        (ctrl/stop-authority! authority)
        (catch Exception e
          (log/warn e "Failed to stop HA authority" {:db-name db-name}))))
    (finally
      (cache/stop-ha-client-cache-state! db-name (:ha-client-cache-state m))
      (stop-ha-probe-executor! db-name (:ha-probe-executor m)))))
