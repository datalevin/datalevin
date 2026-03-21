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
   [datalevin.ha.client-cache :as cache]
   [datalevin.ha.control :as ctrl]
   [datalevin.ha.lease :as lease]
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

(defn- ha-lease-local-remaining-ms
  [m now-ms now-nanos]
  (cond
    (integer? (:ha-lease-local-deadline-nanos m))
    (max 0
         (.toMillis TimeUnit/NANOSECONDS
                    (max 0
                         (- (long (:ha-lease-local-deadline-nanos m))
                            (long now-nanos)))))

    (integer? (:ha-lease-local-deadline-ms m))
    (max 0
         (- (long (:ha-lease-local-deadline-ms m))
            (long now-ms)))

    (integer? (:ha-lease-until-ms m))
    (max 0
         (- (long (:ha-lease-until-ms m))
            (long now-ms)))

    :else
    nil))

(defn- ha-write-admission-lease-margin-ms
  [m]
  (long (max 0
             (long (or (:ha-write-admission-lease-margin-ms m)
                       c/*ha-write-admission-lease-margin-ms*
                       0)))))

(defn- ha-write-admission-lease-margin-nanos
  [m]
  (.toNanos TimeUnit/MILLISECONDS
            (ha-write-admission-lease-margin-ms m)))

(defn- ha-clock-skew-budget-ms ^long
  [m]
  (long (max 0
             (long (or (:ha-clock-skew-budget-ms m)
                       c/*ha-clock-skew-budget-ms*
                       0)))))

(defn- ha-lease-expired-for-promotion?
  [m lease now-ms]
  (let [lease-until-ms (:lease-until-ms lease)]
    (or (nil? lease)
        (nil? lease-until-ms)
        (let [lease-until-ms (long lease-until-ms)
              skew-budget-ms (ha-clock-skew-budget-ms m)]
          (>= (long now-ms)
              (long (unchecked-add lease-until-ms skew-budget-ms)))))))

(defn- ha-renew-timeout-ms ^long
  [m now-ms now-nanos]
  (let [lease-timeout-ms   (long (or (:ha-lease-timeout-ms m)
                                     c/*ha-lease-timeout-ms*))
        request-timeout-ms (long (ha-request-timeout-ms m lease-timeout-ms))
        remaining-ms       (ha-lease-local-remaining-ms m now-ms now-nanos)
        timeout-ms         (long (if (integer? remaining-ms)
                                   (let [remaining-ms (long remaining-ms)]
                                     (if (< remaining-ms request-timeout-ms)
                                       remaining-ms
                                       request-timeout-ms))
                                   request-timeout-ms))]
    (if (< timeout-ms 1) 1 timeout-ms)))

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
  @#'repl/recover-ha-local-store-if-needed)
(def ^:private close-ha-local-store! snap/close-ha-local-store!)
(def ^:private refresh-ha-local-dt-db snap/refresh-ha-local-dt-db)

(def ^:private closed-kv-store? @#'repl/closed-kv-store?)
(def ^:private read-ha-local-persisted-lsn @#'repl/read-ha-local-persisted-lsn)
(def ^:redef persist-ha-local-applied-lsn! @#'repl/persist-ha-local-applied-lsn!)
(def ^:redef fresh-ha-local-watermark-snapshot
  @#'repl/fresh-ha-local-watermark-snapshot)
(def ^:redef read-ha-snapshot-payload-lsn @#'repl/read-ha-snapshot-payload-lsn)
(def ^:redef read-ha-local-last-applied-lsn @#'repl/read-ha-local-last-applied-lsn)
(def ^:private read-ha-local-watermark-lsn @#'repl/read-ha-local-watermark-lsn)
(def persist-ha-runtime-local-applied-lsn!
  @#'repl/persist-ha-runtime-local-applied-lsn!)
(def ^:private ha-local-last-applied-lsn @#'repl/ha-local-last-applied-lsn)
(def ^:private refresh-ha-local-watermarks @#'repl/refresh-ha-local-watermarks)
(def ^:private raw-local-kv-store @#'repl/raw-local-kv-store)
(def ^:private reopen-ha-local-store-if-needed
  @#'repl/reopen-ha-local-store-if-needed)
(def ^:private ha-local-store-reopen-info @#'repl/ha-local-store-reopen-info)
(def ^:redef reopen-ha-local-store-from-info
  @#'repl/reopen-ha-local-store-from-info)
(def ^:private ha-promotion-lag-guard @#'repl/ha-promotion-lag-guard)
(def ^:private fresh-ha-promotion-local-last-applied-lsn
  @#'repl/fresh-ha-promotion-local-last-applied-lsn)
(def ^:private bootstrap-empty-lease? lease/bootstrap-empty-lease?)
(def ^:redef fetch-ha-endpoint-watermark-lsn
  @#'repl/fetch-ha-endpoint-watermark-lsn)
(def ^:redef fetch-leader-watermark-lsn @#'repl/fetch-leader-watermark-lsn)
(def ^:private highest-reachable-ha-member-watermark
  @#'repl/highest-reachable-ha-member-watermark)
(def ^:private ha-member-watermarks @#'repl/ha-member-watermarks)
(def ^:private normalize-leader-watermark-result
  @#'repl/normalize-leader-watermark-result)
(def ^:private sync-ha-follower-state @#'repl/sync-ha-follower-state)
(def ^:private new-ha-probe-executor @#'repl/new-ha-probe-executor)
(def ^:private stop-ha-probe-executor! @#'repl/stop-ha-probe-executor!)

(defn- demote-ha-leader
  [db-name m reason details now-ms]
  (if (= :leader (:ha-role m))
    (let [drain-ms (long (or (:ha-demotion-drain-ms m)
                             c/*ha-demotion-drain-ms*))]
      (log/warn "Demoting HA leader for DB" db-name
                {:reason reason
                 :details details
                 :drain-ms drain-ms})
      (-> m
          (assoc :ha-role :demoting
                 :ha-demotion-reason reason
                 :ha-demotion-details details
                 :ha-demotion-drain-ms drain-ms
                 :ha-demoted-at-ms now-ms
                 :ha-demotion-drain-until-ms (+ (long now-ms) drain-ms))
          (dissoc :ha-leader-term
                  :ha-leader-last-applied-lsn
                  :ha-leader-fencing-pending?
                  :ha-leader-fencing-observed-lease
                  :ha-leader-fencing-last-error)))
    m))

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

(defn- clear-ha-candidate-state
  [m]
  (dissoc m
          :ha-candidate-since-ms
          :ha-candidate-delay-ms
          :ha-candidate-rank-index
          :ha-candidate-pre-cas-wait-until-ms
          :ha-candidate-pre-cas-observed-version
          :ha-promotion-wait-before-cas-ms))

(defn- clear-ha-leader-fencing-state
  [m]
  (dissoc m
          :ha-leader-fencing-pending?
          :ha-leader-fencing-observed-lease
          :ha-leader-fencing-last-error))

(defn- ha-promotion-rank-index
  [m]
  (let [node-id (:ha-node-id m)
        members (ordered-ha-members m)]
    (first
     (keep-indexed
      (fn [idx member]
        (when (= node-id (:node-id member))
          idx))
      members))))

(defn- ha-promotion-delay-ms
  [m]
  (let [base-ms (long (or (:ha-promotion-base-delay-ms m)
                          c/*ha-promotion-base-delay-ms*))
        rank-ms (long (or (:ha-promotion-rank-delay-ms m)
                          c/*ha-promotion-rank-delay-ms*))
        rank-idx (ha-promotion-rank-index m)]
    (when (integer? rank-idx)
      (+ base-ms (* (long rank-idx) rank-ms)))))

(defn ^:redef ha-now-ms
  []
  (System/currentTimeMillis))

(defn ^:redef ha-now-nanos
  []
  (System/nanoTime))

(defn ^:redef maybe-wait-unreachable-leader-before-pre-cas!
  [m lease]
  (let [renew-ms (long (or (:ha-lease-renew-ms m) c/*ha-lease-renew-ms*))
        lease-until-ms (long (or (:lease-until-ms lease) 0))
        wait-until-ms (+ lease-until-ms renew-ms)
        now-ms (System/currentTimeMillis)
        wait-ms (long (max 0 (- wait-until-ms now-ms)))]
    {:wait-ms wait-ms
     :wait-until-ms wait-until-ms}))

(defn- pre-cas-lag-input
  [db-name m lease now-ms]
  (let [authority-lsn (long (or (:leader-last-applied-lsn lease) 0))
        lease-expired? (ha-lease-expired-for-promotion? m lease now-ms)
        local-last-applied-lsn (fresh-ha-promotion-local-last-applied-lsn
                                m lease)
        member-watermarks
        (when lease-expired?
          (ha-member-watermarks db-name m [(:leader-endpoint lease)]))
        leader-watermark
        (if member-watermarks
          (normalize-leader-watermark-result
           lease
           (get member-watermarks
                (:leader-endpoint lease)
                {:reachable? false
                 :reason :missing-endpoint}))
          (fetch-leader-watermark-lsn db-name m lease))
        reachable? (true? (:reachable? leader-watermark))
        leader-lsn (if reachable?
                     (long (or (:last-applied-lsn leader-watermark)
                               authority-lsn))
                     0)
        reachable-member-watermark
        (when (or member-watermarks
                  (not reachable?)
                  (and lease-expired?
                       (< leader-lsn authority-lsn)))
          (highest-reachable-ha-member-watermark
           db-name
           m
           (or member-watermarks
               (cond-> {}
                 (string? (:leader-endpoint lease))
                 (assoc (:leader-endpoint lease) leader-watermark)))))
        reachable-member-lsn
        (when reachable-member-watermark
          (long (or (:last-applied-lsn reachable-member-watermark) 0)))
        max-local-member-lsn
        (long-max2 (or reachable-member-lsn 0)
                   local-last-applied-lsn)
        effective-lsn
        (cond
          (and lease-expired?
               reachable?
               (< leader-lsn authority-lsn))
          (long-max2 leader-lsn max-local-member-lsn)

          reachable?
          (long-max3 authority-lsn leader-lsn max-local-member-lsn)

          :else
          max-local-member-lsn)]
    {:effective-lease (assoc lease :leader-last-applied-lsn effective-lsn)
     :local-last-applied-lsn local-last-applied-lsn
     :lease-expired? lease-expired?
     :leader-endpoint-reachable? reachable?
     :authority-last-applied-lsn authority-lsn
     :leader-watermark-last-applied-lsn (when reachable? leader-lsn)
     :leader-watermark leader-watermark
     :reachable-member-last-applied-lsn
     reachable-member-lsn
     :reachable-member-watermark reachable-member-watermark}))

(defn- authority-observation-from-state
  [m]
  {:lease (:ha-authority-lease m)
   :version (:ha-authority-version m)
   :authority-now-ms (:ha-authority-now-ms m)
   :lease-local-deadline-ms (:ha-lease-local-deadline-ms m)
   :lease-local-deadline-nanos (:ha-lease-local-deadline-nanos m)
   :authority-membership-hash (:ha-authority-membership-hash m)
   :db-identity-mismatch? (true? (:ha-db-identity-mismatch? m))
   :membership-mismatch? (true? (:ha-membership-mismatch? m))
   :observed-at-ms (:ha-last-authority-refresh-ms m)})

(defn- authority-lease-local-deadline-ms
  [lease authority-now-ms local-start-ms]
  (when (and (integer? authority-now-ms)
             (integer? local-start-ms))
    (let [lease-until-ms (:lease-until-ms lease)]
      (when (integer? lease-until-ms)
        (+ (long local-start-ms)
           (max 0
                (- (long lease-until-ms)
                   (long authority-now-ms))))))))

(defn- authority-lease-local-deadline-nanos
  [lease authority-now-ms local-start-nanos]
  (when (and (integer? authority-now-ms)
             (integer? local-start-nanos))
    (let [lease-until-ms (:lease-until-ms lease)]
      (when (integer? lease-until-ms)
        (+ (long local-start-nanos)
           (.toNanos TimeUnit/MILLISECONDS
                     (max 0
                          (- (long lease-until-ms)
                             (long authority-now-ms)))))))))

(defn- control-result-authority-observation?
  [result]
  (and (map? result)
       (contains? result :version)
       (contains? result :authority-now-ms)))

(defn- control-result-authority-observation
  [m local-start-ms local-start-nanos
   {:keys [lease version authority-now-ms observed-at-ms] :as result}]
  (let [authority-membership-hash
        (if (contains? result :membership-hash)
          (:membership-hash result)
          (:ha-authority-membership-hash m))
        db-identity (:ha-db-identity m)
        db-identity-mismatch?
        (and lease (not= db-identity (:db-identity lease)))
        membership-mismatch?
        (and authority-membership-hash
             (not= authority-membership-hash (:ha-membership-hash m)))
        observed-at-ms (or observed-at-ms (ha-now-ms))]
    {:lease lease
     :version version
     :authority-now-ms authority-now-ms
     :lease-local-deadline-ms
     (authority-lease-local-deadline-ms
      lease authority-now-ms local-start-ms)
     :lease-local-deadline-nanos
     (authority-lease-local-deadline-nanos
      lease authority-now-ms local-start-nanos)
     :authority-membership-hash authority-membership-hash
     :db-identity-mismatch? db-identity-mismatch?
     :membership-mismatch? membership-mismatch?
     :observed-at-ms observed-at-ms}))

(defn ^:redef observe-authority-state
  [m]
  (let [authority (:ha-authority m)
        db-identity (:ha-db-identity m)
        local-start-ms (ha-now-ms)
        local-start-nanos (ha-now-nanos)
        result (ctrl/read-state authority db-identity)]
    (control-result-authority-observation
     m
     local-start-ms
     local-start-nanos
     result)))

(defn- apply-authority-observation
  [m {:keys [lease version authority-now-ms
             lease-local-deadline-ms lease-local-deadline-nanos
             authority-membership-hash
             db-identity-mismatch? membership-mismatch?
             observed-at-ms]}
   now-ms]
  (let [refresh-ms (or observed-at-ms now-ms)
        observed-term (:term lease)
        observed-owner-node-id (:leader-node-id lease)]
    (cond-> (assoc m
                   :ha-authority-lease lease
                   :ha-authority-version version
                   :ha-authority-now-ms authority-now-ms
                   :ha-lease-local-deadline-ms lease-local-deadline-ms
                   :ha-lease-local-deadline-nanos lease-local-deadline-nanos
                   :ha-authority-owner-node-id observed-owner-node-id
                   :ha-authority-term observed-term
                   :ha-lease-until-ms (:lease-until-ms lease)
                   :ha-authority-membership-hash authority-membership-hash
                   :ha-db-identity-mismatch? db-identity-mismatch?
                   :ha-membership-mismatch? membership-mismatch?
                   :ha-last-authority-refresh-ms refresh-ms)
      (and (= :leader (:ha-role m))
           (= (:ha-node-id m) observed-owner-node-id)
           (integer? observed-term)
           (or (not (integer? (:ha-leader-term m)))
               (<= (long (:ha-leader-term m))
                   (long observed-term))))
      (assoc :ha-leader-term (long observed-term)))))

(defn- authority-read-error
  [e]
  {:reason :authority-read-failed
   :message (ex-message e)
   :data (ex-data e)})

(defn- apply-authority-read-failure
  [m error]
  (assoc m
         :ha-authority-read-ok? false
         :ha-authority-read-error error))

(defn- apply-authority-read-success
  [m]
  (assoc m
         :ha-authority-read-ok? true
         :ha-authority-read-error nil))

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
            (assoc m
                   :ha-leader-fencing-pending? true
                   :ha-leader-fencing-last-error fence-result)))))))

(defn- parse-ha-clock-skew-output
  [output]
  (let [trimmed (some-> output s/trim)]
    (cond
      (s/blank? trimmed)
      {:ok? false
       :reason :invalid-output
       :message "Clock skew hook returned blank output"
       :output output}

      :else
      (try
        {:ok? true
         :clock-skew-ms (Math/abs (long (Long/parseLong trimmed)))}
        (catch NumberFormatException _
          {:ok? false
           :reason :invalid-output
           :message "Clock skew hook output must be an integer millisecond value"
           :output output})))))

(defn ^:redef run-ha-clock-skew-hook
  [db-name m]
  (let [budget-ms (long (or (:ha-clock-skew-budget-ms m)
                            c/*ha-clock-skew-budget-ms*))
        {:keys [cmd timeout-ms retries retry-delay-ms]} (:ha-clock-skew-hook m)]
    (if-not (and (vector? cmd) (seq cmd))
      {:ok? true
       :skipped? true
       :paused? false
       :reason :clock-skew-hook-unconfigured
       :budget-ms budget-ms}
      (let [env {"DTLV_DB_NAME" db-name
                 "DTLV_HA_NODE_ID" (str (or (:ha-node-id m) ""))
                 "DTLV_HA_ENDPOINT"
                 (str (or (:ha-local-endpoint m) ""))
                 "DTLV_CLOCK_SKEW_BUDGET_MS" (str budget-ms)}
            max-attempts (inc (long (or retries 0)))
            timeout-ms (long (or timeout-ms 3000))
            retry-delay-ms (long (or retry-delay-ms 1000))]
        (loop [attempt 1]
          (let [result (run-command-with-timeout cmd env timeout-ms)
                parsed (if (:ok? result)
                         (merge result
                                (parse-ha-clock-skew-output
                                 (:output result)))
                         result)
                paused? (if (:ok? parsed)
                          (> (long (or (:clock-skew-ms parsed) 0))
                             budget-ms)
                          true)]
            (if (:ok? parsed)
              (assoc parsed
                     :attempt attempt
                     :budget-ms budget-ms
                     :paused? paused?
                     :reason (if paused?
                               :clock-skew-budget-breached
                               :clock-skew-within-budget))
              (if (< attempt max-attempts)
                (do
                  (Thread/sleep retry-delay-ms)
                  (recur (u/long-inc attempt)))
                (assoc parsed
                       :attempt attempt
                       :budget-ms budget-ms
                       :paused? true)))))))))

(defn- refresh-ha-clock-skew-state
  [db-name m]
  (let [budget-ms (long (or (:ha-clock-skew-budget-ms m)
                            c/*ha-clock-skew-budget-ms*))
        role (:ha-role m)
        clock-skew-hook (:ha-clock-skew-hook m)
        hook-configured?
        (let [cmd (:cmd clock-skew-hook)]
          (and (vector? cmd) (seq cmd)))]
    (if (or (#{:follower :candidate} role)
            (and (= :leader role) hook-configured?))
      (let [result (try
                     (run-ha-clock-skew-hook db-name m)
                     (catch Exception e
                       {:ok? false
                        :paused? true
                        :reason :exception
                        :budget-ms budget-ms
                        :message (ex-message e)}))
            now-ms (System/currentTimeMillis)
            next-m (assoc m
                          :ha-clock-skew-budget-ms budget-ms
                          :ha-clock-skew-paused? (true? (:paused? result))
                          :ha-clock-skew-last-check-ms now-ms
                          :ha-clock-skew-last-observed-ms (:clock-skew-ms result)
                          :ha-clock-skew-last-result result)]
        (if (and (= :leader role)
                 (true? (:paused? result)))
          (demote-ha-leader db-name next-m
                            :clock-skew-paused
                            {:budget-ms budget-ms
                             :clock-skew-result result}
                            now-ms)
          next-m))
      (assoc m
             :ha-clock-skew-budget-ms budget-ms
             :ha-clock-skew-paused? false))))

(def ^:dynamic *ha-with-local-store-swap-fn*
  (fn [f]
    (f)))

(defn- with-ha-local-store-swap
  [f]
  (*ha-with-local-store-swap-fn* f))

(defn- ha-clock-skew-promotion-failure-details
  [m]
  (let [budget-ms (long (or (:ha-clock-skew-budget-ms m)
                            c/*ha-clock-skew-budget-ms*))
        result (:ha-clock-skew-last-result m)
        base {:budget-ms budget-ms
              :last-check-ms (:ha-clock-skew-last-check-ms m)
              :check result}]
    (if (= :clock-skew-budget-breached (:reason result))
      (assoc base
             :reason :clock-skew-budget-breached
             :clock-skew-ms (:clock-skew-ms result))
      (assoc base
             :reason :clock-skew-check-failed))))

(defn- ha-authority-read-fresh?
  [m now-ms]
  (let [ok? (true? (:ha-authority-read-ok? m))
        last-ms (:ha-last-authority-refresh-ms m)
        timeout-ms (long (or (:ha-lease-timeout-ms m)
                             c/*ha-lease-timeout-ms*))]
    (and ok?
         (or (not (integer? last-ms))
             (< (- (long now-ms) (long last-ms))
                timeout-ms)))))

(defn- current-authority-observation
  [m now-ms]
  (when (ha-authority-read-fresh? m now-ms)
    (authority-observation-from-state m)))

(defn- promotion-authority-observation
  [m now-ms]
  (or (current-authority-observation m now-ms)
      (observe-authority-state m)))

(defn- ha-authority-read-failure-details
  ([m]
   (ha-authority-read-failure-details m (ha-now-ms)))
  ([m now-ms]
   (or (when (and (true? (:ha-authority-read-ok? m))
                  (integer? (:ha-last-authority-refresh-ms m))
                  (not (ha-authority-read-fresh? m now-ms)))
         {:reason :authority-read-stale
          :last-authority-refresh-ms (:ha-last-authority-refresh-ms m)
          :timeout-ms (long (or (:ha-lease-timeout-ms m)
                                c/*ha-lease-timeout-ms*))})
       (:ha-authority-read-error m)
       {:reason :authority-read-failed})))

(defn- ha-rejoin-promotion-failure-details
  [m]
  {:reason :rejoin-in-progress
   :blocked-until-ms (:ha-rejoin-promotion-blocked-until-ms m)
   :authority-owner-node-id (:ha-authority-owner-node-id m)
   :local-last-applied-lsn (ha-local-last-applied-lsn m)
   :leader-last-applied-lsn
   (long (or (get-in m [:ha-authority-lease :leader-last-applied-lsn]) 0))
   :ha-follower-last-error (:ha-follower-last-error m)
   :ha-follower-degraded? (:ha-follower-degraded? m)})

(defn- clear-ha-rejoin-promotion-block
  [m]
  (-> m
      (assoc :ha-rejoin-promotion-blocked? false
             :ha-rejoin-promotion-blocked-until-ms nil
             :ha-rejoin-promotion-cleared-ms (System/currentTimeMillis))
      (dissoc :ha-rejoin-started-at-ms)))

(defn- maybe-clear-ha-rejoin-promotion-block
  [m now-ms]
  (if-not (:ha-rejoin-promotion-blocked? m)
    m
    (let [blocked-until-ms (long (or (:ha-rejoin-promotion-blocked-until-ms m) 0))
          owner-node-id (:ha-authority-owner-node-id m)
          local-node-id (:ha-node-id m)
          leader-lsn (long (or (get-in m [:ha-authority-lease
                                          :leader-last-applied-lsn])
                               0))
          local-lsn (ha-local-last-applied-lsn m)
          authority-fresh? (ha-authority-read-fresh? m now-ms)
          lag-lsn (nonnegative-long-diff leader-lsn local-lsn)
          lag-ok? (<= lag-lsn
                      (long (or (:ha-max-promotion-lag-lsn m) 0)))
          synced? (and authority-fresh?
                       (integer? owner-node-id)
                       (not= owner-node-id local-node-id)
                       (not (:ha-follower-degraded? m))
                       (nil? (:ha-follower-last-error m))
                       lag-ok?)]
      (cond
        synced?
        (clear-ha-rejoin-promotion-block m)

        (and (pos? blocked-until-ms)
             (>= (long now-ms) blocked-until-ms))
        (clear-ha-rejoin-promotion-block m)

        :else
        m))))

(defn- fail-ha-candidate
  [m reason details]
  (-> m
      clear-ha-candidate-state
      (assoc :ha-role :follower
             :ha-promotion-last-failure reason
             :ha-promotion-failure-details details)))

(defn- maybe-enter-ha-candidate
  [m now-ms]
  (cond
    (not (contains? #{:follower :candidate} (:ha-role m)))
    m

    (and (= :candidate (:ha-role m))
         (not (ha-lease-expired-for-promotion?
               m
               (:ha-authority-lease m)
               now-ms)))
    (-> m
        clear-ha-candidate-state
        (assoc :ha-role :follower))

    (not= :follower (:ha-role m))
    m

    (not (ha-lease-expired-for-promotion?
          m
          (:ha-authority-lease m)
          now-ms))
    m

    (or (true? (:ha-db-identity-mismatch? m))
        (true? (:ha-membership-mismatch? m))
        (not= (:ha-membership-hash m)
              (:ha-authority-membership-hash m)))
    m

    (true? (:ha-clock-skew-paused? m))
    (assoc m
           :ha-promotion-last-failure :clock-skew-paused
           :ha-promotion-failure-details
           (ha-clock-skew-promotion-failure-details m))

    (true? (:ha-follower-degraded? m))
    (assoc m
           :ha-promotion-last-failure :follower-degraded
           :ha-promotion-failure-details
           {:reason (:ha-follower-degraded-reason m)
            :details (:ha-follower-degraded-details m)})

    (true? (:ha-rejoin-promotion-blocked? m))
    (assoc m
           :ha-promotion-last-failure :rejoin-in-progress
           :ha-promotion-failure-details
           (ha-rejoin-promotion-failure-details m))

    (not (ha-authority-read-fresh? m now-ms))
    (assoc m
           :ha-promotion-last-failure :authority-read-failed
           :ha-promotion-failure-details
           (ha-authority-read-failure-details m now-ms))

    :else
    (if-let [delay-ms (ha-promotion-delay-ms m)]
      (assoc m :ha-role :candidate
             :ha-candidate-since-ms now-ms
             :ha-candidate-delay-ms delay-ms
             :ha-candidate-rank-index (ha-promotion-rank-index m))
      (fail-ha-candidate m :missing-promotion-rank
                         {:ha-node-id (:ha-node-id m)
                          :ha-members (:ha-members m)}))))

(defn- try-promote-with-cas
  [db-name m authority db-identity observed-lease version now-ms lag-check]
  (let [local-start-ms now-ms
        local-start-nanos (ha-now-nanos)
        acquire
        (ctrl/try-acquire-lease
         authority
         {:db-identity db-identity
          :leader-node-id (:ha-node-id m)
          :leader-endpoint (:ha-local-endpoint m)
          :lease-renew-ms (:ha-lease-renew-ms m)
          :lease-timeout-ms (:ha-lease-timeout-ms m)
          :leader-last-applied-lsn (:local-last-applied-lsn lag-check)
          :now-ms local-start-ms
          :observed-version version
          :observed-lease observed-lease})]
    (if (:ok? acquire)
      (let [{acquired-lease :lease
             acquired-version :version
             :keys [term authority-now-ms]} acquire
            observed-at-ms (ha-now-ms)]
        (-> m
            clear-ha-candidate-state
            (assoc :ha-role :leader
                   :ha-leader-term term
                   :ha-authority-lease acquired-lease
                   :ha-authority-version acquired-version
                   :ha-authority-now-ms authority-now-ms
                   :ha-lease-local-deadline-ms
                   (authority-lease-local-deadline-ms
                    acquired-lease authority-now-ms local-start-ms)
                   :ha-lease-local-deadline-nanos
                   (authority-lease-local-deadline-nanos
                    acquired-lease authority-now-ms local-start-nanos)
                   :ha-authority-owner-node-id (:leader-node-id acquired-lease)
                   :ha-authority-term (:term acquired-lease)
                   :ha-lease-until-ms (:lease-until-ms acquired-lease)
                   :ha-last-authority-refresh-ms observed-at-ms
                   :ha-db-identity-mismatch? false
                   :ha-membership-mismatch? false
                   :ha-promotion-last-failure nil
                   :ha-promotion-failure-details nil
                   :ha-leader-fencing-pending? true
                   :ha-leader-fencing-observed-lease observed-lease
                   :ha-leader-fencing-last-error nil)
            (maybe-complete-ha-leader-fencing db-name)))
      (fail-ha-candidate m :lease-cas-failed {:acquire acquire}))))

(defn- finalize-ha-candidate-promotion
  [db-name m authority db-identity lease version now-ms lag-input]
  (if-not (ha-lease-expired-for-promotion? m lease now-ms)
    (fail-ha-candidate m :lease-not-expired
                       {:lease lease})
    (let [lag-check
          (ha-promotion-lag-guard m
                                  (:effective-lease lag-input)
                                  (:local-last-applied-lsn lag-input))]
      (if-not (:ok? lag-check)
        (fail-ha-candidate m :lag-guard-failed
                           {:phase :pre-cas
                            :lag lag-check
                            :leader-lag-input lag-input})
        (try-promote-with-cas
         db-name
         m
         authority
         db-identity
         lease
         version
         now-ms
         lag-check)))))

(defn- maybe-promote-after-authority-observation
  [db-name m authority db-identity obs now-ms]
  (let [m1 (-> (apply-authority-observation m obs now-ms)
               (dissoc :ha-candidate-pre-cas-wait-until-ms
                       :ha-candidate-pre-cas-observed-version
                       :ha-promotion-wait-before-cas-ms))
        lease (:lease obs)
        version (:version obs)
        db-identity-mismatch? (:db-identity-mismatch? obs)
        membership-mismatch? (:membership-mismatch? obs)]
    (cond
      db-identity-mismatch?
      (fail-ha-candidate m1 :db-identity-mismatch
                         {:local-db-identity db-identity
                          :authority-lease lease})

      membership-mismatch?
      (fail-ha-candidate m1 :membership-hash-mismatch
                         {:ha-membership-hash (:ha-membership-hash m1)
                          :ha-authority-membership-hash
                          (:authority-membership-hash obs)})

      (not (ha-lease-expired-for-promotion? m1 lease now-ms))
      (fail-ha-candidate m1 :lease-not-expired
                         {:lease lease})

      :else
      (let [lag-input (pre-cas-lag-input db-name m1 lease now-ms)
            reachable? (:leader-endpoint-reachable? lag-input)]
        (if (or reachable? (bootstrap-empty-lease? lease))
          (finalize-ha-candidate-promotion
           db-name m1 authority db-identity lease version now-ms lag-input)
          (let [wait-info (maybe-wait-unreachable-leader-before-pre-cas!
                           m1 lease)
                wait-ms (long (or (:wait-ms wait-info)
                                  (:slept-ms wait-info)
                                  0))]
            (if (pos? wait-ms)
              (assoc m1
                     :ha-candidate-pre-cas-wait-until-ms
                     (:wait-until-ms wait-info)
                     :ha-candidate-pre-cas-observed-version version
                     :ha-promotion-wait-before-cas-ms wait-ms)
              (finalize-ha-candidate-promotion
               db-name m1 authority db-identity lease version now-ms lag-input))))))))

(def ^:private restart-ha-candidate-promotion
  ::restart-ha-candidate-promotion)

(defn- maybe-resume-ha-candidate-pre-cas-wait
  [db-name m now-ms]
  (when-let [wait-until-ms (:ha-candidate-pre-cas-wait-until-ms m)]
    (let [remaining-ms (long (max 0 (- (long wait-until-ms) (long now-ms))))]
      (if (pos? remaining-ms)
        (assoc m :ha-promotion-wait-before-cas-ms remaining-ms)
        (let [observed-version (:ha-candidate-pre-cas-observed-version m)
              current-obs (promotion-authority-observation m now-ms)
              m1 (dissoc m
                         :ha-candidate-pre-cas-wait-until-ms
                         :ha-candidate-pre-cas-observed-version
                         :ha-promotion-wait-before-cas-ms)
              now-ms-2 (System/currentTimeMillis)]
          (if (= observed-version (:version current-obs))
            (maybe-promote-after-authority-observation
             db-name
             m1
             (:ha-authority m1)
             (:ha-db-identity m1)
             current-obs
             now-ms-2)
            restart-ha-candidate-promotion))))))

(defn- attempt-ha-candidate-promotion
  [db-name m now-ms]
  (let [observed-lease (:ha-authority-lease m)]
    (cond
      (or (true? (:ha-db-identity-mismatch? m))
          (true? (:ha-membership-mismatch? m))
          (not= (:ha-membership-hash m)
                (:ha-authority-membership-hash m)))
      (fail-ha-candidate m :membership-hash-mismatch
                         {:ha-membership-hash (:ha-membership-hash m)
                          :ha-authority-membership-hash
                          (:ha-authority-membership-hash m)})

      (not (ha-lease-expired-for-promotion? m observed-lease now-ms))
      (fail-ha-candidate m :lease-not-expired
                         {:lease observed-lease})

      (true? (:ha-clock-skew-paused? m))
      (fail-ha-candidate m :clock-skew-paused
                         (ha-clock-skew-promotion-failure-details m))

      (false? (:ha-authority-read-ok? m))
      (fail-ha-candidate m :authority-read-failed
                         (ha-authority-read-failure-details m))

      :else
      (let [promotion-now-ms (System/currentTimeMillis)]
        (maybe-promote-after-authority-observation
         db-name
         m
         (:ha-authority m)
         (:ha-db-identity m)
         (promotion-authority-observation
          m
          promotion-now-ms)
         promotion-now-ms)))))

(defn- maybe-promote-ha-candidate
  [db-name m now-ms]
  (if (not= :candidate (:ha-role m))
    m
    (try
      (let [candidate-since-ms (long (or (:ha-candidate-since-ms m) now-ms))
            candidate-delay-ms (long (or (:ha-candidate-delay-ms m) 0))
            elapsed-ms (max 0 (- (long now-ms) candidate-since-ms))]
        (if (< elapsed-ms candidate-delay-ms)
          m
          (let [resume-result (maybe-resume-ha-candidate-pre-cas-wait
                               db-name m now-ms)]
            (cond
              (map? resume-result)
              resume-result

              (= restart-ha-candidate-promotion resume-result)
              (attempt-ha-candidate-promotion
               db-name
               (dissoc m
                       :ha-candidate-pre-cas-wait-until-ms
                       :ha-candidate-pre-cas-observed-version
                       :ha-promotion-wait-before-cas-ms)
               now-ms)

              :else
              (attempt-ha-candidate-promotion db-name m now-ms)))))
      (catch Exception e
        (fail-ha-candidate m :promotion-exception
                           {:message (ex-message e)})))))

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

(defn- advance-ha-follower-or-candidate
  [db-name m]
  (let [state-now-ms (ha-now-ms)]
    (if (or (ha-demotion-draining? m state-now-ms)
            (not (contains? #{:follower :candidate} (:ha-role m))))
      m
      (let [m1 (maybe-clear-ha-rejoin-promotion-block m state-now-ms)
            m2 (maybe-enter-ha-candidate m1 state-now-ms)]
        (maybe-promote-ha-candidate db-name m2 (ha-now-ms))))))

(defn refresh-ha-authority-state
  [db-name m]
  (if-not (:ha-authority m)
    m
    (try
      (let [db-identity (:ha-db-identity m)
            now-ms (ha-now-ms)
            observation (observe-authority-state m)
            {:keys [lease authority-membership-hash
                    db-identity-mismatch? membership-mismatch?]}
            observation
            m1 (-> m
                   (apply-authority-observation observation now-ms)
                   apply-authority-read-success)]
        (cond
          (and db-identity-mismatch? (= :leader (:ha-role m1)))
          (demote-ha-leader db-name m1
                            :db-identity-mismatch
                            {:local-db-identity db-identity
                             :authority-lease lease}
                            now-ms)

          (and membership-mismatch? (= :leader (:ha-role m1)))
          (demote-ha-leader db-name m1
                            :membership-hash-mismatch
                            {:local-membership-hash (:ha-membership-hash m1)
                             :authority-membership-hash
                             authority-membership-hash}
                            now-ms)

          :else
          m1))
      (catch Exception e
        (let [error (authority-read-error e)
              failed-m (apply-authority-read-failure m error)]
          (log/warn e "HA read-lease failed"
                    {:db-name db-name
                     :ha-role (:ha-role m)})
          failed-m)))))

(defn- renew-ha-leader-state
  [db-name m]
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
                       :leader-last-applied-lsn (long (or (:ha-leader-last-applied-lsn m) 0))
                       :now-ms local-start-ms
                       :timeout-ms renew-timeout-ms})]
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
                                 (apply-authority-observation observation
                                                              now-ms)
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
                                (ha-now-ms)))))))))

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
                          (= :in-memory
                             (get-in ha-opts [:ha-control-plane :backend]))
                          (assoc-in [:ha-control-plane :backend]
                                    :sofa-jraft)

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
