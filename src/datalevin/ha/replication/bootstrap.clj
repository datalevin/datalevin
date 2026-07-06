;;
;; Copyright (c) Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2.0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns ^:no-doc datalevin.ha.replication.bootstrap
  "Snapshot bootstrap and install helpers for HA replication."
  (:require
   [clojure.string :as s]
   [datalevin.constants :as c]
   [datalevin.db :as db]
   [datalevin.ha.replication.store :as store]
   [datalevin.ha.snapshot :as snap]
   [datalevin.ha.util :as hu]
   [datalevin.interface :as i]
   [datalevin.kv :as kv]
   [datalevin.storage :as st]
   [datalevin.util :as u]
   [taoensso.timbre :as log])
  (:import
   [datalevin.interface IStore]
   [java.nio.channels ClosedByInterruptException]
   [java.util UUID]))

(def ^:private long-max2 hu/long-max2)
(def ^:private long-max4 hu/long-max4)
(def ^:private long-min2 hu/long-min2)

(defn- interrupted-message?
  [message]
  (and (string? message)
       (or (s/includes? message "java.lang.InterruptedException")
           (s/includes? message "java.nio.channels.ClosedByInterruptException"))))

(def ^:private sync-ha-snapshot-install-target!
  snap/sync-ha-snapshot-install-target!)
(def ^:private copy-dir-contents! snap/copy-dir-contents!)
(def ^:private move-path! snap/move-path!)
(def ^:private write-ha-snapshot-install-marker!
  snap/write-ha-snapshot-install-marker!)
(def ^:private delete-ha-snapshot-install-marker!
  snap/delete-ha-snapshot-install-marker!)
(def ^:private recover-ha-local-snapshot-install!
  snap/recover-ha-local-snapshot-install!)
(def ^:private close-ha-local-store! snap/close-ha-local-store!)
(def ^:private open-ha-store-dbis! snap/open-ha-store-dbis!)

(defn- interrupted-error?
  [e]
  (let [chain (take-while some? (iterate ex-cause e))]
    (boolean
     (some #(or (instance? InterruptedException %)
                (instance? ClosedByInterruptException %)
                (interrupted-message? (ex-message %)))
           chain))))

(defn normalize-ha-bootstrap-retry-state
  [candidate-m fallback-m reopen-info]
  (or (try
        (let [state (store/reopen-ha-local-store-if-needed candidate-m)]
          (when (store/local-kv-store state)
            state))
        (catch Throwable _
          nil))
      (try
        (let [state (store/reopen-ha-local-store-if-needed fallback-m)]
          (when (store/local-kv-store state)
            state))
        (catch Throwable _
          nil))
      (try
        ;; Snapshot install can leave only a reopen recipe behind after a
        ;; failed restore; use it before giving up on the next source.
        (let [state (store/with-ha-local-store-swap
                      #(store/reopen-ha-local-store-from-info
                        fallback-m
                        reopen-info))]
          (when (store/local-kv-store state)
            state))
        (catch Throwable _
          nil))
      candidate-m))

(defn- ha-lease-leader-endpoint
  [lease]
  (let [leader-endpoint (:leader-endpoint lease)]
    (when (and (string? leader-endpoint)
               (not (s/blank? leader-endpoint)))
      leader-endpoint)))

(defn- leader-snapshot-source-order
  [lease source-order]
  (let [source-order (vec source-order)]
    (if-let [leader-endpoint (ha-lease-leader-endpoint lease)]
      (vec (filter #(= leader-endpoint %) source-order))
      source-order)))

(defn- leader-snapshot-source-unavailable-error
  [lease source-order snapshot-source-order]
  (let [leader-endpoint (ha-lease-leader-endpoint lease)]
    (when (and (empty? snapshot-source-order)
               leader-endpoint)
      {:error :ha/follower-snapshot-leader-source-unavailable
       :message
       (str "HA follower snapshot bootstrap requires the current lease "
            "leader as the snapshot source")
       :data {:leader-endpoint leader-endpoint
              :source-order (vec source-order)}})))

(defn- authority-confirmed-lsn
  [lease]
  (long (max 0 (long (or (:leader-last-applied-lsn lease) 0)))))

(defn- cap-lsn-to-authority
  [lease lsn]
  (let [lsn (long (max 0 (long (or lsn 0))))
        authority-lsn (authority-confirmed-lsn lease)]
    (if (pos? (long authority-lsn))
      (long-min2 lsn authority-lsn)
      lsn)))

(defn- ha-local-contiguous-txlog-tail
  [kv-store from-lsn upto-lsn]
  (if (or (nil? kv-store)
          (> (long from-lsn) (long upto-lsn)))
    []
    (try
      (loop [expected (long from-lsn)
             remaining (seq (kv/open-tx-log-rows kv-store from-lsn upto-lsn))
             acc []]
        (if-let [record (first remaining)]
          (let [record-lsn (long (:lsn record))
                rows (or (:rows record) (:ops record))]
            (if (and (= record-lsn expected)
                     (sequential? rows))
              (recur (unchecked-inc expected)
                     (next remaining)
                     (conj acc record))
              acc))
          acc))
      (catch clojure.lang.ExceptionInfo e
        ;; Snapshot installs can reopen on a copied payload before the local
        ;; runtime txlog path is available again. Treat that as an unverified
        ;; tail and clamp back to the snapshot floor instead of aborting the
        ;; whole bootstrap.
        (if (= :txlog/not-enabled (:type (ex-data e)))
          []
          (throw e))))))

(defn- inspect-ha-local-bootstrap-tail
  ([m materialized-lsn]
   (inspect-ha-local-bootstrap-tail m materialized-lsn nil))
  ([m materialized-lsn trusted-max-lsn]
   (if-let [kv-store (store/local-kv-store m)]
     (let [persisted-floor-lsn
           (store/read-ha-local-persisted-lsn kv-store)
           payload-lsn
           (store/read-ha-snapshot-payload-lsn m)
           watermark-lsn
           (store/read-ha-local-watermark-lsn m)
           materialized-floor-lsn
           (long-max2 materialized-lsn payload-lsn)
           candidate-floor-lsn-raw
           (long-max4 materialized-floor-lsn
                      persisted-floor-lsn
                      watermark-lsn
                      payload-lsn)
           candidate-floor-lsn
           (long (if (integer? trusted-max-lsn)
                   (long-min2 candidate-floor-lsn-raw
                              (long trusted-max-lsn))
                   candidate-floor-lsn-raw))
           records
           (ha-local-contiguous-txlog-tail
            kv-store
            (unchecked-inc (long materialized-floor-lsn))
            candidate-floor-lsn)
           tail-last-lsn
           (long (or (some-> (peek records) :lsn long)
                     materialized-floor-lsn))
           tail-complete?
           (>= tail-last-lsn candidate-floor-lsn)
           verified-materialized-lsn
           (long-max4 materialized-floor-lsn
                      tail-last-lsn
                      payload-lsn
                      0)
           verified-floor-lsn
           (long (long-min2 candidate-floor-lsn
                             verified-materialized-lsn))]
       {:verified-floor-lsn verified-floor-lsn
        :materialized-floor-lsn materialized-floor-lsn
        :persisted-floor-lsn persisted-floor-lsn
        :payload-lsn payload-lsn
        :watermark-lsn watermark-lsn
        :candidate-floor-lsn-raw candidate-floor-lsn-raw
        :candidate-floor-lsn candidate-floor-lsn
        :trusted-max-lsn (some-> trusted-max-lsn long)
        :tail-last-lsn tail-last-lsn
        :tail-complete? tail-complete?
        :tail-record-count (count records)
        :tail-records records})
     {:verified-floor-lsn (long materialized-lsn)
      :materialized-floor-lsn (long materialized-lsn)
      :persisted-floor-lsn (long materialized-lsn)
      :candidate-floor-lsn-raw (long materialized-lsn)
      :candidate-floor-lsn (long materialized-lsn)
      :trusted-max-lsn (some-> trusted-max-lsn long)
      :tail-last-lsn (long materialized-lsn)
      :tail-complete? true
      :tail-record-count 0})))

(defn- persist-ha-local-bootstrap-floor!
  [m applied-lsn]
  (when-let [kv-store (store/raw-local-kv-store m)]
    (let [existing-payload-lsn (long (store/read-ha-local-payload-lsn kv-store))
          payload-lsn (long (max (long applied-lsn)
                                 existing-payload-lsn))]
      (i/transact-kv kv-store
                     c/kv-info
                     [[:put c/wal-local-payload-lsn payload-lsn]
                      [:put c/ha-local-applied-lsn (long applied-lsn)]]
                     :keyword
                     :data)))
  (long applied-lsn))

(defn- summarize-bootstrap-replay
  [replay]
  (dissoc replay :tail-records))

(defn reconcile-ha-installed-snapshot-state
  ([m materialized-lsn]
   (reconcile-ha-installed-snapshot-state m materialized-lsn nil nil))
  ([m materialized-lsn trusted-max-lsn]
   (reconcile-ha-installed-snapshot-state m materialized-lsn trusted-max-lsn nil))
  ([m materialized-lsn trusted-max-lsn apply-record-fn]
   (let [{:keys [materialized-floor-lsn
                 verified-floor-lsn
                 tail-records] :as replay}
         (binding [store/*ha-current-state-fn* (fn [] nil)]
           (inspect-ha-local-bootstrap-tail m materialized-lsn trusted-max-lsn))
         tail-records-to-apply
         (when (and (fn? apply-record-fn)
                    (> (long verified-floor-lsn)
                       (long materialized-floor-lsn)))
           (filterv (fn [record]
                      (let [lsn (long (:lsn record))]
                        (and (> lsn (long materialized-floor-lsn))
                             (<= lsn (long verified-floor-lsn)))))
                    tail-records))
         replayed-last-term
         (when (seq tail-records-to-apply)
           (some-> (peek tail-records-to-apply)
                   :ha-term
                   long))
         m (if (seq tail-records-to-apply)
             (let [next-m (reduce apply-record-fn m tail-records-to-apply)]
               (cond-> (assoc next-m
                               :ha-local-last-applied-lsn
                               (long verified-floor-lsn)
                               :ha-follower-next-lsn
                               (unchecked-inc (long verified-floor-lsn)))
                 replayed-last-term
                 (assoc :ha-follower-last-applied-term replayed-last-term)))
             m)
         reopen-info (store/ha-local-store-reopen-info m)
         clamped? (< (long verified-floor-lsn)
                     (long (:candidate-floor-lsn replay)))
         _ (when clamped?
             (persist-ha-local-bootstrap-floor! m verified-floor-lsn))
         next-m (if (and clamped? reopen-info)
                  (store/with-ha-local-store-swap
                    #(do
                       (close-ha-local-store! m)
                       (store/reopen-ha-local-store-from-info
                        m reopen-info)))
                  m)]
     (cond-> (assoc (summarize-bootstrap-replay replay)
                    :installed-lsn (long verified-floor-lsn)
                    :state next-m)
       replayed-last-term
       (assoc :replayed-last-term replayed-last-term)))))

(defn- reconcile-installed-snapshot-state
  [reconcile-fn state materialized-lsn install-target-lsn apply-record-fn]
  (if (fn? apply-record-fn)
    (reconcile-fn state materialized-lsn install-target-lsn apply-record-fn)
    (reconcile-fn state materialized-lsn install-target-lsn)))

(defn ha-snapshot-open-opts
  [m db-name db-identity]
  (let [store (:store m)
        base-opts (store/ha-local-store-open-opts m store)]
    (assoc (or base-opts {})
           ::st/raw-persist-open-opts? true
           :db-name db-name
           :db-identity db-identity)))

(defn validate-ha-snapshot-copy!
  [db-name m source-endpoint _snapshot-dir copy-meta required-lsn]
  (let [snapshot-db-name (:db-name copy-meta)
        snapshot-db-identity (:db-identity copy-meta)
        snapshot-last-lsn (:snapshot-last-applied-lsn copy-meta)
        payload-last-lsn (:payload-last-applied-lsn copy-meta)
        txlog-last-lsn (:txlog-last-applied-lsn copy-meta)]
    (when (not= db-name snapshot-db-name)
      (u/raise "HA snapshot copy DB name mismatch"
               {:error :ha/follower-snapshot-db-name-mismatch
                :db-name db-name
                :snapshot-db-name snapshot-db-name
                :source-endpoint source-endpoint}))
    (when (or (nil? snapshot-db-identity) (s/blank? snapshot-db-identity))
      (u/raise "HA snapshot copy is missing DB identity"
               {:error :ha/follower-snapshot-missing-db-identity
                :db-name db-name
                :source-endpoint source-endpoint}))
    (when (not= (:ha-db-identity m) snapshot-db-identity)
      (u/raise "HA snapshot copy DB identity mismatch"
               {:error :ha/follower-snapshot-db-identity-mismatch
                :db-name db-name
                :local-db-identity (:ha-db-identity m)
                :snapshot-db-identity snapshot-db-identity
                :source-endpoint source-endpoint}))
    (when-not (or (integer? snapshot-last-lsn)
                  (integer? payload-last-lsn)
                  (integer? txlog-last-lsn))
      (u/raise "HA snapshot copy is missing payload last applied LSN"
               {:error :ha/follower-snapshot-missing-last-applied-lsn
                :db-name db-name
                :source-endpoint source-endpoint
                :copy-meta copy-meta}))
    (let [snapshot-last-lsn (when (integer? snapshot-last-lsn)
                              (long snapshot-last-lsn))
          payload-last-lsn (when (integer? payload-last-lsn)
                             (long payload-last-lsn))
          txlog-last-lsn (when (integer? txlog-last-lsn)
                           (long txlog-last-lsn))
          ;; The snapshot LSN is a retention marker, not proof that all
          ;; datalog payload through that LSN is already materialized.
          materialized-last-lsn (long (or payload-last-lsn 0))
          install-last-lsn (long-max2 materialized-last-lsn
                                      (or txlog-last-lsn 0))]
      (when (< install-last-lsn (long required-lsn))
        (u/raise "HA snapshot copy payload is older than the required follower floor"
                 {:error :ha/follower-snapshot-too-stale
                  :db-name db-name
                  :required-lsn (long required-lsn)
                  :snapshot-last-applied-lsn snapshot-last-lsn
                  :payload-last-applied-lsn payload-last-lsn
                  :txlog-last-applied-lsn txlog-last-lsn
                  :source-endpoint source-endpoint}))
      {:db-name db-name
       :db-identity snapshot-db-identity
       :snapshot-last-applied-lsn (or snapshot-last-lsn 0)
       :payload-last-applied-lsn materialized-last-lsn
       :txlog-last-applied-lsn (or txlog-last-lsn 0)
       :install-last-applied-lsn install-last-lsn})))

(defn install-ha-local-snapshot!
  [m snapshot-dir]
  (let [store (:store m)]
    (if-not (instance? IStore store)
      {:ok? false
       :state m
       :error {:error :ha/follower-missing-store
               :message "HA follower snapshot install requires a local store"}}
      (let [env-dir (i/dir store)
            backup-dir (str env-dir ".ha-backup-" (UUID/randomUUID))
            stage-dir (str env-dir ".ha-install-" (UUID/randomUUID))
            install-marker {:backup-dir backup-dir
                            :stage-dir stage-dir
                            :db-name (some-> store i/db-name)
                            :created-at-ms (System/currentTimeMillis)}
            open-opts (ha-snapshot-open-opts
                       m
                       (:db-name (i/opts store))
                       (:ha-db-identity m))]
        ;; Validate that the copied snapshot is openable before swapping paths.
        (let [snapshot-store (st/open snapshot-dir nil open-opts)]
          (try
            (i/opts snapshot-store)
            (finally
              (i/close snapshot-store))))
        (store/with-ha-local-store-swap
          (fn []
            (try
              (close-ha-local-store! m)
              (st/retire-shared-local-store! env-dir)
              (when (u/file-exists backup-dir)
                (u/delete-files backup-dir))
              (when (u/file-exists stage-dir)
                (u/delete-files stage-dir))
              (write-ha-snapshot-install-marker!
               env-dir
               (assoc install-marker :stage :backup-moving))
              (move-path! env-dir backup-dir)
              (write-ha-snapshot-install-marker!
               env-dir
               (assoc install-marker :stage :backup-moved))
              (copy-dir-contents! snapshot-dir stage-dir)
              (sync-ha-snapshot-install-target! stage-dir)
              (write-ha-snapshot-install-marker!
               env-dir
               (assoc install-marker :stage :snapshot-staged))
              (move-path! stage-dir env-dir)
              (sync-ha-snapshot-install-target! env-dir)
              (let [new-store (open-ha-store-dbis!
                               (st/open env-dir nil open-opts))
                    new-db (db/new-db new-store)]
                (delete-ha-snapshot-install-marker! env-dir)
                (when (u/file-exists backup-dir)
                  (u/delete-files backup-dir))
                {:ok? true
                 :state (-> m
                            store/clear-ha-local-store-transient-state
                            (assoc :store new-store
                                   :dt-db new-db))})
              (catch Exception e
                (let [log-context {:db-name (some-> store i/db-name)
                                   :env-dir env-dir}]
                  (if (interrupted-error? e)
                    (do
                      (.interrupt (Thread/currentThread))
                      (log/debug
                        "HA follower snapshot install interrupted; local store will recover on reopen"
                        (assoc log-context
                               :install-message (ex-message e)
                               :install-data (ex-data e)))
                      {:ok? false
                       :state (-> m
                                  store/clear-ha-local-store-transient-state
                                  (assoc :store store
                                         :dt-db nil))
                       :error {:error :ha/follower-snapshot-install-interrupted
                               :message (ex-message e)
                               :data (ex-data e)}})
                    (try
                      (recover-ha-local-snapshot-install! env-dir)
                      (log/warn e
                                "HA follower snapshot install failed; restored local store"
                                log-context)
                      (let [restored-store (open-ha-store-dbis!
                                            (st/open env-dir nil open-opts))
                            restored-db (db/new-db restored-store)]
                        {:ok? false
                         :state (-> m
                                    store/clear-ha-local-store-transient-state
                                    (assoc :store restored-store
                                           :dt-db restored-db))
                         :error {:error :ha/follower-snapshot-install-failed
                                 :message (ex-message e)
                                 :data (ex-data e)}})
                      (catch Exception restore-e
                        (when (or (interrupted-error? e)
                                  (interrupted-error? restore-e))
                          (.interrupt (Thread/currentThread)))
                        (if (or (interrupted-error? e)
                                (interrupted-error? restore-e))
                          (log/debug
                            "HA follower snapshot install restore interrupted; local store will recover on reopen"
                            (merge log-context
                                   {:install-message (ex-message e)
                                    :install-data (ex-data e)
                                    :restore-message (ex-message restore-e)
                                    :restore-data (ex-data restore-e)}))
                          (log/error restore-e
                                     "HA follower snapshot install restore failed"
                                     (merge log-context
                                            {:install-message (ex-message e)
                                             :install-data (ex-data e)})))
                        {:ok? false
                         :state (-> m
                                    ;; Preserve the closed store handle so the next
                                    ;; renew step can recover and reopen from its
                                    ;; original env dir instead of getting stuck with
                                    ;; no local store at all.
                                    store/clear-ha-local-store-transient-state
                                    (assoc :store store
                                           :dt-db nil))
                         :error {:error :ha/follower-snapshot-install-restore-failed
                                 :message (ex-message e)
                                 :data (merge (or (ex-data e) {})
                                              {:restore-message (ex-message restore-e)
                                               :restore-data (ex-data restore-e)})}}))))))))))))

(defn bootstrap-ha-follower-from-snapshot*
  [{:keys [normalize-ha-bootstrap-retry-state
           ha-local-store-reopen-info
           fetch-ha-endpoint-snapshot-copy!
           validate-ha-snapshot-copy!
           install-ha-local-snapshot!
           explicit-raw-local-kv-store
           raw-local-kv-store
           read-ha-local-snapshot-current-lsn
           reconcile-ha-installed-snapshot-state
           persist-ha-local-applied-lsn!
           note-ha-bootstrap-installed-state
           apply-ha-follower-record!
           sync-ha-follower-batch]}
  db-name m lease source-order next-lsn now-ms]
  (let [required-lsn (cap-lsn-to-authority
                      lease
                      (long (max 0 (dec (long next-lsn)))))
        snapshot-source-order (leader-snapshot-source-order lease source-order)
        unavailable-error (leader-snapshot-source-unavailable-error
                           lease
                           source-order
                           snapshot-source-order)
        initial-errors (cond-> []
                         unavailable-error
                         (conj unavailable-error))]
    (loop [remaining snapshot-source-order
           current-m (normalize-ha-bootstrap-retry-state
                      m m (ha-local-store-reopen-info m))
           current-reopen-info (ha-local-store-reopen-info m)
           errors initial-errors]
      (if-let [source-endpoint (first remaining)]
        (let [current-m (normalize-ha-bootstrap-retry-state
                         current-m current-m current-reopen-info)
              current-reopen-info (or (ha-local-store-reopen-info current-m)
                                      current-reopen-info)
              snapshot-dir (u/tmp-dir (str "ha-snapshot-copy-"
                                           (UUID/randomUUID)))
              attempt
              (try
                (let [{:keys [copy-meta]}
                      (fetch-ha-endpoint-snapshot-copy!
                       db-name current-m source-endpoint snapshot-dir)
                      manifest
                      (validate-ha-snapshot-copy!
                       db-name current-m source-endpoint snapshot-dir
                       copy-meta required-lsn)
                      install-res
                      (install-ha-local-snapshot! current-m snapshot-dir)]
                  (if (:ok? install-res)
                    (let [installed-state (:state install-res)
                          installed-raw-local-kv-store
                          (or explicit-raw-local-kv-store raw-local-kv-store)
                          local-snapshot-lsn
                          (long (max 0
                                     (long (if-let [kv-store
                                                    (installed-raw-local-kv-store
                                                     installed-state)]
                                            (read-ha-local-snapshot-current-lsn
                                              kv-store)
                                             0))))
                          local-payload-lsn
                          (long (max 0
                                     (long (if-let [kv-store
                                                    (installed-raw-local-kv-store
                                                     installed-state)]
                                             (try
                                               (or (i/get-value
                                                    kv-store
                                                    c/kv-info
                                                    c/wal-local-payload-lsn
                                                    :keyword
                                                    :data)
                                                   0)
                                               (catch Throwable _
                                                 0))
                                             0))))
                          manifest-snapshot-lsn
                          (long (max 0
                                     (long (or (:snapshot-last-applied-lsn
                                                manifest)
                                               0))))
                          manifest-payload-lsn
                          (long (max 0
                                     (long (or (:payload-last-applied-lsn
                                                manifest)
                                               0))))
                          manifest-txlog-lsn
                          (long (max 0
                                     (long (or (:txlog-last-applied-lsn
                                                manifest)
                                               0))))
                          manifest-install-lsn
                          (long (max manifest-snapshot-lsn
                                     manifest-payload-lsn
                                     manifest-txlog-lsn
                                     (long (or (:install-last-applied-lsn
                                                manifest)
                                               0))))
                          materialized-floor-lsn
                          ;; The copied WAL tail can be ahead of the copied
                          ;; datalog payload. Snapshot markers are retention
                          ;; floors; only the local payload marker proves what
                          ;; is already queryable.
                          (long local-payload-lsn)
                          install-target-lsn
                          ;; The authority lease can lag a leader's copied
                          ;; snapshot metadata. Never clamp the installed floor
                          ;; below rows already materialized in the copied
                          ;; payload, or bootstrap will try to replay records
                          ;; that are already queryable in the snapshot.
                          (long-max2
                           materialized-floor-lsn
                           (cap-lsn-to-authority
                            lease
                            (long-max2 materialized-floor-lsn
                                       manifest-txlog-lsn)))
                          {:keys [state installed-lsn replayed-last-term]
                           :as replay}
                          (reconcile-installed-snapshot-state
                           reconcile-ha-installed-snapshot-state
                           installed-state
                           materialized-floor-lsn
                           install-target-lsn
                           apply-ha-follower-record!)
                          persisted-installed-lsn
                          (persist-ha-local-applied-lsn!
                           state
                           installed-lsn)
                          installed-state
                          (cond-> (note-ha-bootstrap-installed-state
                                   state
                                   installed-lsn
                                   source-endpoint
                                   installed-lsn
                                   now-ms
                                   persisted-installed-lsn)
                            replayed-last-term
                            (assoc :ha-follower-last-applied-term
                                   replayed-last-term))]
                      (if (< (long installed-lsn) (long required-lsn))
                        {:ok? false
                         :state installed-state
                         :error {:error :ha/follower-snapshot-installed-too-stale
                                 :message
                                 "HA snapshot copy installed below the required follower floor"
                                 :data {:required-lsn (long required-lsn)
                                        :snapshot-last-applied-lsn
                                        install-target-lsn
                                        :local-snapshot-last-applied-lsn
                                        local-snapshot-lsn
                                        :local-payload-last-applied-lsn
                                        local-payload-lsn
                                        :materialized-floor-lsn
                                        materialized-floor-lsn
                                        :manifest-install-last-applied-lsn
                                        manifest-install-lsn
                                        :installed-last-applied-lsn installed-lsn
                                        :resume-next-lsn
                                        (unchecked-inc (long installed-lsn))
                                        :manifest manifest
                                        :replay replay}}}
                        (try
                          (let [sync-res (sync-ha-follower-batch
                                          db-name installed-state lease
                                          (unchecked-inc (long installed-lsn))
                                          now-ms)
                                next-state (-> (:state sync-res)
                                               (assoc
                                                :ha-follower-last-bootstrap-ms
                                                now-ms
                                                :ha-follower-bootstrap-source-endpoint
                                                source-endpoint
                                                :ha-follower-bootstrap-snapshot-last-applied-lsn
                                                installed-lsn))]
                            {:ok? true
                             :state next-state})
                          (catch Exception e
                            {:ok? false
                             :state installed-state
                             :error {:error (or (:error (ex-data e))
                                                :ha/follower-snapshot-resume-failed)
                                     :message (ex-message e)
                                     :data (merge
                                            (or (ex-data e) {})
                                            {:snapshot-last-applied-lsn
                                             install-target-lsn
                                             :local-snapshot-last-applied-lsn
                                             local-snapshot-lsn
                                             :local-payload-last-applied-lsn
                                             local-payload-lsn
                                             :materialized-floor-lsn
                                             materialized-floor-lsn
                                             :manifest-install-last-applied-lsn
                                             manifest-install-lsn
                                             :installed-last-applied-lsn
                                             installed-lsn
                                             :resume-next-lsn
                                             (unchecked-inc
                                              (long installed-lsn))
                                             :manifest manifest
                                             :replay replay})}}))))
                    {:ok? false
                     :state (:state install-res)
                     :error (:error install-res)}))
                (catch Exception e
                  {:ok? false
                   :state current-m
                   :error {:error (or (:error (ex-data e))
                                      :ha/follower-snapshot-bootstrap-failed)
                           :message (ex-message e)
                           :data (ex-data e)}})
                (finally
                  (when (u/file-exists snapshot-dir)
                    (u/delete-files snapshot-dir))))]
          (if (:ok? attempt)
            attempt
            (let [next-m (normalize-ha-bootstrap-retry-state
                          (:state attempt)
                          current-m
                          current-reopen-info)
                  next-reopen-info (or (ha-local-store-reopen-info next-m)
                                       current-reopen-info)]
              (recur (rest remaining)
                     next-m
                     next-reopen-info
                     (conj errors
                           (assoc (:error attempt)
                                  :source-endpoint source-endpoint))))))
        {:ok? false
         :state current-m
         :source-order (vec snapshot-source-order)
         :candidate-source-order (vec source-order)
         :errors errors}))))
