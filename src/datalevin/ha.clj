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
   [datalevin.client :as cl]
   [datalevin.constants :as c]
   [datalevin.ha.control :as ctrl]
   [datalevin.ha.lease :as lease]
   [datalevin.interface :as i]
   [datalevin.kv :as kv]
   [datalevin.util :as u]
   [datalevin.validate :as vld]
   [taoensso.timbre :as log])
  (:import
   [datalevin.interface IStore]
   [java.util.concurrent TimeUnit]))

(defn consensus-ha-opts
  [store]
  (when (instance? IStore store)
    (let [opts (i/opts store)]
      (when (= :consensus-lease (:ha-mode opts))
        opts))))

(defn- local-ha-endpoint
  [ha-opts]
  (let [node-id (:ha-node-id ha-opts)]
    (:endpoint (first (filter #(= node-id (:node-id %))
                              (:ha-members ha-opts))))))

(defn- demote-ha-leader
  [db-name m reason details now-ms]
  (if (= :leader (:ha-role m))
    (do
      (log/warn "Demoting HA leader for DB" db-name
                {:reason reason :details details})
      (-> m
          (assoc :ha-role :demoting
                 :ha-demotion-reason reason
                 :ha-demotion-details details
                 :ha-demoted-at-ms now-ms)
          (dissoc :ha-leader-term
                  :ha-leader-last-applied-lsn)))
    m))

(defn- maybe-finish-ha-demotion
  [m now-ms]
  (if (and (= :demoting (:ha-role m))
           (integer? (:ha-demoted-at-ms m))
           (> (long now-ms) (long (:ha-demoted-at-ms m))))
    (assoc m :ha-role :follower)
    m))

(defn- clear-ha-candidate-state
  [m]
  (dissoc m
          :ha-candidate-since-ms
          :ha-candidate-delay-ms
          :ha-candidate-rank-index))

(defn- ha-promotion-rank-index
  [m]
  (let [node-id  (:ha-node-id m)
        members  (sort-by :node-id (:ha-members m))]
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

(defn- local-kv-store
  [m]
  (let [store (:store m)]
    (cond
      (nil? store) nil
      :else (or (try
                  (.-lmdb store)
                  (catch Throwable _
                    nil))
                store))))

(defn ^:redef read-ha-local-last-applied-lsn
  [m]
  (try
    (if-let [kv-store (local-kv-store m)]
      (long (or (get (kv/txlog-watermarks kv-store) :last-applied-lsn) 0))
      0)
    (catch Exception e
      (log/warn e "Unable to read local txlog watermarks for HA lag guard"
                {:db-name (some-> (:store m) i/opts :db-name)})
      0)))

(defn- ha-local-last-applied-lsn
  [m]
  (if (integer? (:ha-local-last-applied-lsn m))
    (long (:ha-local-last-applied-lsn m))
    (read-ha-local-last-applied-lsn m)))

(defn- refresh-ha-local-watermarks
  [m]
  (if (local-kv-store m)
    (let [local-lsn (read-ha-local-last-applied-lsn m)]
      (cond-> (assoc m :ha-local-last-applied-lsn local-lsn)
        (= :leader (:ha-role m))
        (assoc :ha-leader-last-applied-lsn local-lsn)))
    m))

(defn- ha-promotion-lag-guard
  [m lease]
  (let [leader-last-applied-lsn (long (or (:leader-last-applied-lsn lease) 0))
        local-last-applied-lsn  (ha-local-last-applied-lsn m)
        lag-lsn                 (max 0
                                     (- leader-last-applied-lsn
                                        local-last-applied-lsn))
        max-lag-lsn             (long (or (:ha-max-promotion-lag-lsn m) 0))]
    {:ok? (<= lag-lsn max-lag-lsn)
     :leader-last-applied-lsn leader-last-applied-lsn
     :local-last-applied-lsn local-last-applied-lsn
     :lag-lsn lag-lsn
     :max-lag-lsn max-lag-lsn}))

(defn- bootstrap-empty-lease?
  [lease]
  (and (or (nil? lease) (nil? (:leader-node-id lease)))
       (zero? (lease/observed-term lease))))

(def ^:private endpoint-pattern
  #"^(.+):([0-9]+)$")

(defn- parse-endpoint
  [endpoint]
  (when-let [[_ host port-str] (and (string? endpoint)
                                    (re-matches endpoint-pattern endpoint))]
    (let [port (parse-long port-str)]
      (when (and (not (s/blank? host))
                 (integer? port)
                 (<= 1 (long port) 65535))
        {:host host :port (long port)}))))

(defn ^:redef fetch-leader-watermark-lsn
  [db-name m lease]
  (let [leader-endpoint (:leader-endpoint lease)
        authority-lsn   (long (or (:leader-last-applied-lsn lease) 0))]
    (cond
      (or (nil? leader-endpoint) (s/blank? leader-endpoint))
      {:reachable? false
       :reason :missing-leader-endpoint}

      (= leader-endpoint (:ha-local-endpoint m))
      {:reachable? true
       :last-applied-lsn (ha-local-last-applied-lsn m)
       :source :local}

      :else
      (if-let [{:keys [host port]} (parse-endpoint leader-endpoint)]
        (let [uri (str "dtlv://"
                       c/default-username
                       ":"
                       c/default-password
                       "@"
                       host
                       ":"
                       port)
              timeout-ms (long (max 500
                                    (min 5000
                                         (long (or (:ha-lease-renew-ms m)
                                                   c/*ha-lease-renew-ms*)))))
              client-opts {:pool-size 1
                           :time-out timeout-ms}]
          (try
            (let [client (cl/new-client uri client-opts)]
              (try
                (let [watermarks (cl/normal-request
                                   client
                                   :txlog-watermarks
                                   [db-name]
                                   false)]
                  {:reachable? true
                   :last-applied-lsn
                   (long (or (:last-applied-lsn watermarks) authority-lsn))
                   :source :remote})
                (finally
                  (cl/disconnect client))))
            (catch Exception e
              {:reachable? false
               :reason :leader-watermark-fetch-failed
               :message (ex-message e)})))
        {:reachable? false
         :reason :invalid-leader-endpoint
         :leader-endpoint leader-endpoint}))))

(def ^:private ha-follower-max-batch-records 256)
(def ^:private ha-follower-max-sync-backoff-ms 30000)

(defn- next-ha-follower-sync-backoff-ms
  [m]
  (let [renew-ms   (long (max 100
                              (long (or (:ha-lease-renew-ms m)
                                        c/*ha-lease-renew-ms*))))
        base-ms    (long (max 250 (quot renew-ms 2)))
        prev-ms    (long (or (:ha-follower-sync-backoff-ms m) 0))
        max-ms     (long (max ha-follower-max-sync-backoff-ms
                              (* 6 renew-ms)))
        candidate  (if (pos? prev-ms)
                     (* 2 prev-ms)
                     base-ms)]
    (long (min max-ms candidate))))

(defn- ha-follower-gap-error?
  [e]
  (contains? #{:ha/txlog-gap
               :ha/txlog-non-contiguous
               :ha/txlog-gap-unresolved}
             (:error (ex-data e))))

(defn- ha-leader-endpoint
  [m lease]
  (or (:leader-endpoint lease)
      (some->> (:ha-members m)
               (filter #(= (:leader-node-id lease) (:node-id %)))
               first
               :endpoint)))

(defn- ha-follower-source-endpoints
  [m lease]
  (let [local-endpoint  (:ha-local-endpoint m)
        leader-endpoint (ha-leader-endpoint m lease)
        ordered-members (sort-by :node-id (:ha-members m))
        fallback-endpoints
        (into []
              (comp
                (map :endpoint)
                (remove nil?)
                (remove s/blank?)
                (remove #(= % local-endpoint))
                (remove #(= % leader-endpoint)))
              ordered-members)]
    (->> (cond-> []
           (and (string? leader-endpoint)
                (not (s/blank? leader-endpoint))
                (not= leader-endpoint local-endpoint))
           (conj leader-endpoint)
           :always
           (into fallback-endpoints))
         distinct
         vec)))

(declare fetch-ha-leader-txlog-batch)
(declare assert-contiguous-lsn!)

(defn- fetch-ha-follower-records-with-gap-fallback
  [db-name m lease next-lsn upto-lsn]
  (let [sources (ha-follower-source-endpoints m lease)]
    (loop [remaining sources
           gap-errors []]
      (if-let [source-endpoint (first remaining)]
        (let [attempt
              (try
                (let [records (vec (or (fetch-ha-leader-txlog-batch
                                         db-name
                                         m
                                         source-endpoint
                                         next-lsn
                                         upto-lsn)
                                       []))]
                  (assert-contiguous-lsn! next-lsn records)
                  {:ok? true
                   :value {:source-endpoint source-endpoint
                           :records records
                           :source-order sources}})
                (catch Exception e
                  (if (ha-follower-gap-error? e)
                    {:ok? false
                     :gap-error {:source-endpoint source-endpoint
                                 :message (ex-message e)
                                 :data (ex-data e)}}
                    (throw e))))]
          (if (:ok? attempt)
            (:value attempt)
            (recur (rest remaining)
                   (conj gap-errors (:gap-error attempt)))))
        (u/raise "Follower txlog replay gap unresolved across deterministic sources"
                 {:error :ha/txlog-gap-unresolved
                  :expected-lsn next-lsn
                  :upto-lsn upto-lsn
                  :source-order sources
                  :gap-errors gap-errors})))))

(defn ^:redef fetch-ha-leader-txlog-batch
  [db-name m leader-endpoint from-lsn upto-lsn]
  (if-let [{:keys [host port]} (parse-endpoint leader-endpoint)]
    (let [uri (str "dtlv://"
                   c/default-username
                   ":"
                   c/default-password
                   "@"
                   host
                   ":"
                   port)
          timeout-ms (long (max 500
                                (min 10000
                                     (long (or (:ha-lease-renew-ms m)
                                               c/*ha-lease-renew-ms*)))))
          client-opts {:pool-size 1
                       :time-out timeout-ms}
          client (cl/new-client uri client-opts)]
      (try
        (cl/normal-request
          client
          :open-tx-log
          [db-name (long from-lsn) (long upto-lsn)]
          false)
        (finally
          (cl/disconnect client))))
    (u/raise "Invalid HA leader endpoint for txlog fetch"
             {:error :ha/follower-invalid-leader-endpoint
              :leader-endpoint leader-endpoint})))

(defn- assert-contiguous-lsn!
  [expected-from records]
  (let [expected-from (long expected-from)]
    (when (seq records)
      (let [first-lsn (long (:lsn (first records)))]
        (when (not= first-lsn expected-from)
          (u/raise "Follower txlog replay detected LSN gap"
                   {:error :ha/txlog-gap
                    :expected-lsn expected-from
                    :actual-lsn first-lsn})))
      (loop [prev expected-from
             rs (rest records)]
        (when-let [record (first rs)]
          (let [actual (long (:lsn record))
                want   (unchecked-inc (long prev))]
            (when (not= actual want)
              (u/raise "Follower txlog replay detected non-contiguous LSN"
                       {:error :ha/txlog-non-contiguous
                        :expected-lsn want
                        :actual-lsn actual}))
            (recur actual (rest rs))))))))

(defn ^:redef apply-ha-follower-txlog-record!
  [m record]
  (let [kv-store (local-kv-store m)
        ops      (:ops record)]
    (when-not kv-store
      (u/raise "Follower txlog replay requires a local KV store"
               {:error :ha/follower-missing-store}))
    (when-not (sequential? ops)
      (u/raise "Follower txlog replay record is missing :ops"
               {:error :ha/follower-invalid-record
                :record record}))
    (i/transact-kv kv-store ops)))

(defn ^:redef report-ha-replica-floor!
  [db-name m leader-endpoint applied-lsn]
  (if-let [{:keys [host port]} (parse-endpoint leader-endpoint)]
    (let [uri (str "dtlv://"
                   c/default-username
                   ":"
                   c/default-password
                   "@"
                   host
                   ":"
                   port)
          timeout-ms (long (max 500
                                (min 10000
                                     (long (or (:ha-lease-renew-ms m)
                                               c/*ha-lease-renew-ms*)))))
          client-opts {:pool-size 1
                       :time-out timeout-ms}
          client (cl/new-client uri client-opts)]
      (try
        (cl/normal-request
          client
          :txlog-update-replica-floor!
          [db-name (:ha-node-id m) (long applied-lsn)]
          false)
        (finally
          (cl/disconnect client))))
    (u/raise "Invalid HA leader endpoint for replica-floor update"
             {:error :ha/follower-invalid-leader-endpoint
              :leader-endpoint leader-endpoint
              :applied-lsn applied-lsn})))

(defn- sync-ha-follower-state
  [db-name m now-ms]
  (if (not= :follower (:ha-role m))
    m
    (let [lease          (:ha-authority-lease m)
          local-node-id  (:ha-node-id m)
          leader-node-id (:leader-node-id lease)]
      (cond
        (or (nil? lease) (nil? leader-node-id))
        m

        (= leader-node-id local-node-id)
        m

        (lease/lease-expired? lease now-ms)
        m

        (and (integer? (:ha-follower-next-sync-not-before-ms m))
             (< (long now-ms)
                (long (:ha-follower-next-sync-not-before-ms m))))
        m

        :else
        (let [leader-endpoint (ha-leader-endpoint m lease)]
          (if (or (nil? leader-endpoint) (s/blank? leader-endpoint))
            (assoc m
                   :ha-follower-last-error :missing-leader-endpoint
                   :ha-follower-last-error-details {:lease lease}
                   :ha-follower-last-error-ms now-ms)
            (try
              (let [next-lsn (long (max 1
                                        (or (:ha-follower-next-lsn m)
                                            (unchecked-inc
                                              (long (ha-local-last-applied-lsn m))))))
                    upto-lsn (long (+ next-lsn
                                      (dec (long ha-follower-max-batch-records))))
                    {:keys [records source-endpoint source-order]}
                    (fetch-ha-follower-records-with-gap-fallback
                      db-name m lease next-lsn upto-lsn)
                    _        (doseq [record records]
                               (apply-ha-follower-txlog-record! m record))
                    local-lsn-after (read-ha-local-last-applied-lsn m)
                    last-record-lsn (when-let [record (peek records)]
                                      (long (:lsn record)))
                    applied-lsn     (long (max local-lsn-after
                                               (or last-record-lsn
                                                   (dec next-lsn))))]
                (try
                  (report-ha-replica-floor!
                    db-name m leader-endpoint applied-lsn)
                  (catch Exception e
                    (log/warn e "HA follower failed to update leader replica floor"
                              {:db-name db-name
                               :ha-node-id local-node-id
                               :leader-endpoint leader-endpoint
                               :applied-lsn applied-lsn})))
                (assoc m
                       :ha-local-last-applied-lsn applied-lsn
                       :ha-follower-next-lsn (unchecked-inc applied-lsn)
                       :ha-follower-last-batch-size (count records)
                       :ha-follower-last-sync-ms now-ms
                       :ha-follower-leader-endpoint leader-endpoint
                       :ha-follower-source-endpoint source-endpoint
                       :ha-follower-source-order source-order
                       :ha-follower-sync-backoff-ms nil
                       :ha-follower-next-sync-not-before-ms nil
                       :ha-follower-degraded? nil
                       :ha-follower-degraded-reason nil
                       :ha-follower-degraded-details nil
                       :ha-follower-degraded-since-ms nil
                       :ha-follower-last-error nil
                       :ha-follower-last-error-details nil
                       :ha-follower-last-error-ms nil))
              (catch Exception e
                (let [gap?       (ha-follower-gap-error? e)
                       details    {:message (ex-message e)
                                   :data (ex-data e)
                                   :leader-endpoint leader-endpoint
                                   :source-order
                                   (ha-follower-source-endpoints m lease)}
                       backoff-ms (when-not gap?
                                    (next-ha-follower-sync-backoff-ms m))]
                  (cond-> (assoc m
                                 :ha-follower-last-error :sync-failed
                                 :ha-follower-last-error-details details
                                 :ha-follower-last-error-ms now-ms)
                    gap?
                    (assoc :ha-follower-degraded? true
                           :ha-follower-degraded-reason :wal-gap
                           :ha-follower-degraded-details details
                           :ha-follower-degraded-since-ms
                           (or (:ha-follower-degraded-since-ms m) now-ms)
                           :ha-follower-sync-backoff-ms nil
                           :ha-follower-next-sync-not-before-ms nil)

                    (not gap?)
                    (assoc :ha-follower-sync-backoff-ms backoff-ms
                           :ha-follower-next-sync-not-before-ms
                           (+ (long now-ms) (long backoff-ms)))))))))))))

(defn ^:redef maybe-wait-unreachable-leader-before-pre-cas!
  [m lease]
  (let [renew-ms      (long (or (:ha-lease-renew-ms m) c/*ha-lease-renew-ms*))
        lease-until-ms (long (or (:lease-until-ms lease) 0))
        wait-until-ms  (+ lease-until-ms renew-ms)
        now-ms         (System/currentTimeMillis)
        sleep-ms       (long (max 0 (- wait-until-ms now-ms)))]
    (when (pos? sleep-ms)
      (Thread/sleep sleep-ms))
    {:slept-ms sleep-ms
     :wait-until-ms wait-until-ms}))

(defn- pre-cas-lag-input
  [db-name m lease]
  (let [authority-lsn   (long (or (:leader-last-applied-lsn lease) 0))
        leader-watermark (fetch-leader-watermark-lsn db-name m lease)
        reachable?      (true? (:reachable? leader-watermark))
        leader-lsn      (if reachable?
                          (long (or (:last-applied-lsn leader-watermark)
                                    authority-lsn))
                          authority-lsn)
        effective-lsn   (if reachable?
                          (max authority-lsn leader-lsn)
                          authority-lsn)]
    {:effective-lease (assoc lease :leader-last-applied-lsn effective-lsn)
     :leader-endpoint-reachable? reachable?
     :authority-last-applied-lsn authority-lsn
     :leader-watermark-last-applied-lsn (when reachable? leader-lsn)
     :leader-watermark leader-watermark}))

(defn- observe-authority-state
  [m]
  (let [authority   (:ha-authority m)
        db-identity (:ha-db-identity m)
        {:keys [lease version]} (ctrl/read-lease authority db-identity)
        authority-membership-hash (ctrl/read-membership-hash authority)
        db-identity-mismatch?
        (and lease (not= db-identity (:db-identity lease)))
        membership-mismatch?
        (and authority-membership-hash
             (not= authority-membership-hash (:ha-membership-hash m)))]
    {:lease lease
     :version version
     :authority-membership-hash authority-membership-hash
     :db-identity-mismatch? db-identity-mismatch?
     :membership-mismatch? membership-mismatch?}))

(defn- apply-authority-observation
  [m {:keys [lease version authority-membership-hash
             db-identity-mismatch? membership-mismatch?]}
   now-ms]
  (-> m
      (assoc :ha-authority-lease lease
             :ha-authority-version version
             :ha-authority-owner-node-id (:leader-node-id lease)
             :ha-authority-term (:term lease)
             :ha-lease-until-ms (:lease-until-ms lease)
             :ha-authority-membership-hash authority-membership-hash
             :ha-db-identity-mismatch? db-identity-mismatch?
             :ha-membership-mismatch? membership-mismatch?
             :ha-last-authority-refresh-ms now-ms)))

(defn- run-command-with-timeout
  [cmd env timeout-ms]
  (try
    (let [process-builder (ProcessBuilder. ^java.util.List (mapv str cmd))
          env-map         (.environment process-builder)
          _               (doseq [[k v] env]
                            (.put env-map (str k) (str v)))
          _               (.redirectErrorStream process-builder true)
          process         (.start process-builder)
          finished?       (.waitFor process
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
        (let [observed-term  (lease/observed-term observed-lease)
              candidate-term (lease/next-term observed-lease)
              candidate-id   (:ha-node-id m)
              fence-op-id    (str db-name ":" observed-term ":" candidate-id)
              env            {"DTLV_DB_NAME" db-name
                              "DTLV_OLD_LEADER_NODE_ID"
                              (str (or (:leader-node-id observed-lease) ""))
                              "DTLV_OLD_LEADER_ENDPOINT"
                              (str (or (:leader-endpoint observed-lease) ""))
                              "DTLV_NEW_LEADER_NODE_ID" (str candidate-id)
                              "DTLV_TERM_CANDIDATE" (str candidate-term)
                              "DTLV_TERM_OBSERVED" (str observed-term)
                              "DTLV_FENCE_OP_ID" fence-op-id}
              max-attempts   (inc (long (or retries 0)))
              timeout-ms     (long (or timeout-ms 3000))
              retry-delay-ms (long (or retry-delay-ms 1000))]
          (loop [attempt 1]
            (let [result (run-command-with-timeout cmd env timeout-ms)]
              (if (:ok? result)
                (assoc result
                       :attempt attempt
                       :fence-op-id fence-op-id)
                (if (< attempt max-attempts)
                  (do
                    (Thread/sleep retry-delay-ms)
                    (recur (u/long-inc attempt)))
                  (assoc result
                         :attempt attempt
                         :fence-op-id fence-op-id))))))))))

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
         (not (lease/lease-expired? (:ha-authority-lease m) now-ms)))
    (-> m
        clear-ha-candidate-state
        (assoc :ha-role :follower))

    (not= :follower (:ha-role m))
    m

    (not (lease/lease-expired? (:ha-authority-lease m) now-ms))
    m

    (or (true? (:ha-db-identity-mismatch? m))
        (true? (:ha-membership-mismatch? m))
        (not= (:ha-membership-hash m)
              (:ha-authority-membership-hash m)))
    m

    (true? (:ha-follower-degraded? m))
    (assoc m
           :ha-promotion-last-failure :follower-degraded
           :ha-promotion-failure-details
           {:reason (:ha-follower-degraded-reason m)
            :details (:ha-follower-degraded-details m)})

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
  [m authority db-identity lease version now-ms lag-check]
  (let [acquire
        (ctrl/try-acquire-lease
          authority
          {:db-identity db-identity
           :leader-node-id (:ha-node-id m)
           :leader-endpoint (:ha-local-endpoint m)
           :lease-renew-ms (:ha-lease-renew-ms m)
           :lease-timeout-ms (:ha-lease-timeout-ms m)
           :leader-last-applied-lsn (:local-last-applied-lsn lag-check)
           :now-ms now-ms
           :observed-version version
           :observed-lease lease})]
    (if (:ok? acquire)
      (let [{:keys [lease version term]} acquire]
        (-> m
            clear-ha-candidate-state
            (assoc :ha-role :leader
                   :ha-leader-term term
                   :ha-authority-lease lease
                   :ha-authority-version version
                   :ha-authority-owner-node-id (:leader-node-id lease)
                   :ha-authority-term (:term lease)
                   :ha-lease-until-ms (:lease-until-ms lease)
                   :ha-last-authority-refresh-ms now-ms
                   :ha-db-identity-mismatch? false
                   :ha-membership-mismatch? false
                   :ha-promotion-last-failure nil
                   :ha-promotion-failure-details nil)))
      (fail-ha-candidate m :lease-cas-failed {:acquire acquire}))))

(defn- finalize-ha-candidate-promotion
  [db-name m authority db-identity lease version now-ms lag-input]
  (if-not (lease/lease-expired? lease now-ms)
    (fail-ha-candidate m :lease-not-expired
                       {:lease lease})
    (let [lag-check
          (ha-promotion-lag-guard m (:effective-lease lag-input))]
      (if-not (:ok? lag-check)
        (fail-ha-candidate m :lag-guard-failed
                           {:phase :pre-cas
                            :lag lag-check
                            :leader-lag-input lag-input})
        (try-promote-with-cas
          m
          authority
          db-identity
          lease
          version
          now-ms
          lag-check)))))

(defn- maybe-promote-after-authority-observation
  [db-name m authority db-identity obs now-ms]
  (let [m1 (apply-authority-observation m obs now-ms)
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

      (not (lease/lease-expired? lease now-ms))
      (fail-ha-candidate m1 :lease-not-expired
                         {:lease lease})

      :else
      (let [lag-input (pre-cas-lag-input db-name m1 lease)
            reachable? (:leader-endpoint-reachable? lag-input)]
        (if (or reachable? (bootstrap-empty-lease? lease))
          (finalize-ha-candidate-promotion
            db-name m1 authority db-identity lease version now-ms lag-input)
          (let [wait-info (maybe-wait-unreachable-leader-before-pre-cas!
                            m1 lease)
                obs-2 (observe-authority-state m1)
                now-ms-2 (System/currentTimeMillis)
                m2 (-> (apply-authority-observation m1 obs-2 now-ms-2)
                       (assoc :ha-promotion-wait-before-cas-ms
                              (:slept-ms wait-info)))
                lease-2 (:lease obs-2)
                version-2 (:version obs-2)]
            (cond
              (:db-identity-mismatch? obs-2)
              (fail-ha-candidate m2 :db-identity-mismatch
                                 {:local-db-identity db-identity
                                  :authority-lease lease-2})

              (:membership-mismatch? obs-2)
              (fail-ha-candidate m2 :membership-hash-mismatch
                                 {:ha-membership-hash (:ha-membership-hash m2)
                                  :ha-authority-membership-hash
                                  (:authority-membership-hash obs-2)})

              (not (lease/lease-expired? lease-2 now-ms-2))
              (fail-ha-candidate m2 :lease-not-expired
                                 {:lease lease-2})

              :else
              (finalize-ha-candidate-promotion
                db-name
                m2
                authority
                db-identity
                lease-2
                version-2
                now-ms-2
                (pre-cas-lag-input db-name m2 lease-2)))))))))

(defn- maybe-promote-ha-candidate
  [db-name m now-ms]
  (if (not= :candidate (:ha-role m))
    m
    (try
      (let [candidate-since-ms (long (or (:ha-candidate-since-ms m) now-ms))
            candidate-delay-ms (long (or (:ha-candidate-delay-ms m) 0))
            elapsed-ms         (max 0 (- (long now-ms) candidate-since-ms))]
        (if (< elapsed-ms candidate-delay-ms)
          m
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

              (not (lease/lease-expired? observed-lease now-ms))
              (fail-ha-candidate m :lease-not-expired
                                 {:lease observed-lease})

              :else
              (let [lag-check-1 (ha-promotion-lag-guard m observed-lease)]
                (if-not (:ok? lag-check-1)
                  (fail-ha-candidate m :lag-guard-failed
                                     {:phase :pre-fence
                                      :lag lag-check-1})
                  (let [fence-result
                        (run-ha-fencing-hook db-name m observed-lease)]
                    (if-not (:ok? fence-result)
                      (fail-ha-candidate m :fencing-failed fence-result)
                      (maybe-promote-after-authority-observation
                        db-name
                        m
                        (:ha-authority m)
                        (:ha-db-identity m)
                        (observe-authority-state m)
                        (System/currentTimeMillis))))))))))
      (catch Exception e
        (fail-ha-candidate m :promotion-exception
                           {:message (ex-message e)})))))

(defn- advance-ha-follower-or-candidate
  [db-name m now-ms]
  (if (contains? #{:follower :candidate} (:ha-role m))
    (let [m0 (sync-ha-follower-state db-name m now-ms)
          m1 (maybe-enter-ha-candidate m0 now-ms)]
      (maybe-promote-ha-candidate db-name m1 now-ms))
    m))

(defn- read-ha-authority-state
  [db-name m now-ms]
  (let [authority   (:ha-authority m)
        db-identity (:ha-db-identity m)
        {:keys [lease version]} (ctrl/read-lease authority db-identity)
        authority-membership-hash (ctrl/read-membership-hash authority)
        db-identity-mismatch?
        (and lease (not= db-identity (:db-identity lease)))
        membership-mismatch?
        (and authority-membership-hash
             (not= authority-membership-hash (:ha-membership-hash m)))
        m1 (-> m
               (assoc :ha-authority-lease lease
                      :ha-authority-version version
                      :ha-authority-owner-node-id (:leader-node-id lease)
                      :ha-authority-term (:term lease)
                      :ha-lease-until-ms (:lease-until-ms lease)
                      :ha-authority-membership-hash authority-membership-hash
                      :ha-db-identity-mismatch? db-identity-mismatch?
                      :ha-membership-mismatch? membership-mismatch?
                      :ha-last-authority-refresh-ms now-ms))]
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
                         :authority-membership-hash authority-membership-hash}
                        now-ms)

      :else
      m1)))

(defn- renew-ha-leader-state
  [db-name m now-ms]
  (if (not= :leader (:ha-role m))
    m
    (let [term (:ha-leader-term m)]
      (if-not (and (integer? term) (pos? ^long term))
        (demote-ha-leader db-name m :missing-leader-term nil now-ms)
        (let [result (ctrl/renew-lease
                       (:ha-authority m)
                       {:db-identity (:ha-db-identity m)
                        :leader-node-id (:ha-node-id m)
                        :leader-endpoint (:ha-local-endpoint m)
                        :term term
                        :lease-renew-ms (:ha-lease-renew-ms m)
                        :lease-timeout-ms (:ha-lease-timeout-ms m)
                        :leader-last-applied-lsn (long (or (:ha-leader-last-applied-lsn m) 0))
                        :now-ms now-ms})]
          (if (:ok? result)
            (let [{:keys [lease version]} result]
              (-> m
                  (assoc :ha-authority-lease lease
                         :ha-authority-version version
                         :ha-authority-owner-node-id (:leader-node-id lease)
                         :ha-authority-term (:term lease)
                         :ha-lease-until-ms (:lease-until-ms lease)
                         :ha-last-authority-refresh-ms now-ms)))
            (demote-ha-leader db-name m
                              :renew-failed
                              {:reason (:reason result)}
                              now-ms)))))))

(defn- maybe-demote-on-refresh-timeout
  [db-name m now-ms]
  (let [timeout-ms (:ha-lease-timeout-ms m)
        last-ms    (:ha-last-authority-refresh-ms m)]
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
    (let [now-ms (System/currentTimeMillis)
          m0 (refresh-ha-local-watermarks m)
          m1 (try
               (read-ha-authority-state db-name m0 now-ms)
               (catch Exception e
                 (log/warn e "HA read-lease failed"
                           {:db-name db-name})
                 m0))
          m2 (try
               (renew-ha-leader-state db-name m1 now-ms)
               (catch Exception e
                 (demote-ha-leader db-name m1
                                   :renew-exception
                                   {:message (ex-message e)}
                                   now-ms)))
          m3 (advance-ha-follower-or-candidate db-name m2 now-ms)]
      (-> (maybe-demote-on-refresh-timeout db-name m3 now-ms)
          (maybe-finish-ha-demotion now-ms)))))

(defn clear-ha-runtime-state
  [m]
  (dissoc m
          :ha-authority :ha-db-identity :ha-membership-hash
          :ha-authority-membership-hash
          :ha-db-identity-mismatch? :ha-membership-mismatch?
          :ha-members
          :ha-node-id :ha-local-endpoint
          :ha-lease-renew-ms :ha-lease-timeout-ms
          :ha-promotion-base-delay-ms :ha-promotion-rank-delay-ms
          :ha-max-promotion-lag-lsn :ha-fencing-hook
          :ha-authority-lease :ha-authority-version
          :ha-authority-owner-node-id :ha-authority-term
          :ha-lease-until-ms :ha-last-authority-refresh-ms
          :ha-role :ha-leader-term
          :ha-local-last-applied-lsn
          :ha-follower-next-lsn :ha-follower-last-batch-size
          :ha-follower-last-sync-ms :ha-follower-leader-endpoint
          :ha-follower-source-endpoint :ha-follower-source-order
          :ha-follower-sync-backoff-ms
          :ha-follower-next-sync-not-before-ms
          :ha-follower-degraded? :ha-follower-degraded-reason
          :ha-follower-degraded-details :ha-follower-degraded-since-ms
          :ha-follower-last-error :ha-follower-last-error-details
          :ha-follower-last-error-ms
          :ha-promotion-last-failure :ha-promotion-failure-details
          :ha-candidate-since-ms :ha-candidate-delay-ms
          :ha-candidate-rank-index
          :ha-renew-loop-running?))

(def ^:private ha-write-command-types
  #{:set-schema
    :swap-attr
    :del-attr
    :rename-attr
    :load-datoms
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

(defn ha-write-admission-error
  [dbs {:keys [type args]}]
  (when (ha-write-command-types type)
    (let [db-name (nth args 0 nil)
          m       (and db-name (get dbs db-name))]
      (when (and m (:ha-authority m))
        (let [now-ms          (System/currentTimeMillis)
              role            (:ha-role m)
              local-node-id   (:ha-node-id m)
              owner-node-id   (:ha-authority-owner-node-id m)
              owner-endpoint  (or (get-in m [:ha-authority-lease :leader-endpoint])
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
                    (sort-by :node-id (:ha-members m)))
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
              db-id-mismatch? (true? (:ha-db-identity-mismatch? m))
              membership-mismatch? (true? (:ha-membership-mismatch? m))
              lease-until-ms  (:ha-lease-until-ms m)
              leader-term     (:ha-leader-term m)
              authority-term  (:ha-authority-term m)]
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

            (or (nil? lease-until-ms)
                (>= (long now-ms) (long lease-until-ms)))
            (merge common-meta
                   {:error :ha/write-rejected
                    :reason :lease-expired
                    :lease-until-ms lease-until-ms
                    :now-ms now-ms
                    :retryable? true})

            (or (nil? leader-term)
                (nil? authority-term)
                (not= leader-term authority-term))
            (merge common-meta
                   {:error :ha/write-rejected
                    :reason :term-mismatch
                    :ha-leader-term leader-term
                    :ha-authority-term authority-term
                    :retryable? true})

            :else nil))))))

(defn start-ha-authority
  [db-name ha-opts]
  (let [cp        (:ha-control-plane ha-opts)
        db-identity (:db-identity ha-opts)
        node-id   (:ha-node-id ha-opts)
        members   (:ha-members ha-opts)
        renew-ms  (:ha-lease-renew-ms ha-opts)
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
        fencing-hook (:ha-fencing-hook ha-opts)
        local-endpoint (local-ha-endpoint ha-opts)
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
      (let [now-ms       (System/currentTimeMillis)
            derived-hash (vld/derive-ha-membership-hash ha-opts)
            init-result  (ctrl/init-membership-hash! authority derived-hash)
            {:keys [lease version]} (ctrl/read-lease authority db-identity)
            _ (when (and lease (not= db-identity (:db-identity lease)))
                (u/raise "HA lease db identity mismatch at startup"
                         {:error :ha/db-identity-mismatch
                          :db-name db-name
                          :local-db-identity db-identity
                          :authority-lease lease}))
            local-authority-owner? (and lease
                                      (= node-id (:leader-node-id lease))
                                      (not (lease/lease-expired? lease now-ms))
                                      (= db-identity (:db-identity lease)))]
        (when-not (:ok? init-result)
          (u/raise "HA membership hash mismatch with authoritative control plane"
                   {:error :ha/membership-hash-mismatch
                    :db-name db-name
                    :derived-hash derived-hash
                    :authority init-result}))
        (when local-authority-owner?
          (log/info "HA startup is rejoining as follower despite local authority ownership"
                    {:db-name db-name
                     :ha-node-id node-id
                     :term (:term lease)
                     :lease-until-ms (:lease-until-ms lease)}))
        {:ha-authority                 authority
         :ha-db-identity               db-identity
         :ha-membership-hash           derived-hash
         :ha-authority-membership-hash (:membership-hash init-result)
         :ha-db-identity-mismatch?     false
         :ha-membership-mismatch?      false
         :ha-members                   members
         :ha-node-id                   node-id
         :ha-local-endpoint            local-endpoint
         :ha-lease-renew-ms            renew-ms
         :ha-lease-timeout-ms          timeout-ms
         :ha-promotion-base-delay-ms   promotion-base-delay-ms
         :ha-promotion-rank-delay-ms   promotion-rank-delay-ms
         :ha-max-promotion-lag-lsn     max-promotion-lag-lsn
         :ha-fencing-hook              fencing-hook
         :ha-authority-lease           lease
         :ha-authority-version         version
         :ha-authority-owner-node-id   (:leader-node-id lease)
         :ha-authority-term            (:term lease)
         :ha-lease-until-ms            (:lease-until-ms lease)
         :ha-last-authority-refresh-ms now-ms
         :ha-role                      :follower
         :ha-leader-term               nil})
      (catch Exception e
        (try
          (ctrl/stop-authority! authority)
          (catch Exception stop-e
            (log/warn stop-e "Failed to stop HA authority after startup failure"
                      {:db-name db-name})))
        (throw e)))))

(defn stop-ha-authority
  [db-name m]
  (when-let [authority (:ha-authority m)]
    (try
      (ctrl/stop-authority! authority)
      (catch Exception e
        (log/warn e "Failed to stop HA authority" {:db-name db-name})))))
