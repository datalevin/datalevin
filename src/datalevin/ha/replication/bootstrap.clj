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
   [java.util UUID]))

(def ^:private long-max2 hu/long-max2)
(def ^:private long-max4 hu/long-max4)
(def ^:private long-min2 hu/long-min2)

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
  ([m snapshot-lsn]
   (inspect-ha-local-bootstrap-tail m snapshot-lsn nil))
  ([m snapshot-lsn trusted-max-lsn]
   (if-let [kv-store (store/local-kv-store m)]
     (let [persisted-floor-lsn
           (store/read-ha-local-persisted-lsn kv-store)
           candidate-floor-lsn-raw
           (long-max4 snapshot-lsn
                      persisted-floor-lsn
                      (store/read-ha-local-watermark-lsn m)
                      (store/read-ha-snapshot-payload-lsn m))
           candidate-floor-lsn
           (long (if (integer? trusted-max-lsn)
                   (long-min2 candidate-floor-lsn-raw
                              (long trusted-max-lsn))
                   candidate-floor-lsn-raw))
           records
           (ha-local-contiguous-txlog-tail
            kv-store
            (unchecked-inc (long snapshot-lsn))
            candidate-floor-lsn)
           verified-floor-lsn
           (long (if (or (= candidate-floor-lsn (long snapshot-lsn))
                         (seq records))
                   candidate-floor-lsn
                   snapshot-lsn))]
       {:verified-floor-lsn verified-floor-lsn
        :persisted-floor-lsn persisted-floor-lsn
        :candidate-floor-lsn-raw candidate-floor-lsn-raw
        :candidate-floor-lsn candidate-floor-lsn
        :trusted-max-lsn (some-> trusted-max-lsn long)
        :tail-record-count (count records)})
     {:verified-floor-lsn (long snapshot-lsn)
      :persisted-floor-lsn (long snapshot-lsn)
      :candidate-floor-lsn-raw (long snapshot-lsn)
      :candidate-floor-lsn (long snapshot-lsn)
      :trusted-max-lsn (some-> trusted-max-lsn long)
      :tail-record-count 0})))

(defn- persist-ha-local-bootstrap-floor!
  [m applied-lsn]
  (when-let [kv-store (store/raw-local-kv-store m)]
    (i/transact-kv kv-store
                   c/kv-info
                   [[:put c/wal-local-payload-lsn (long applied-lsn)]
                    [:put c/ha-local-applied-lsn (long applied-lsn)]]
                   :keyword
                   :data))
  (long applied-lsn))

(defn reconcile-ha-installed-snapshot-state
  ([m snapshot-lsn]
   (reconcile-ha-installed-snapshot-state m snapshot-lsn nil))
  ([m snapshot-lsn trusted-max-lsn]
   (let [{:keys [verified-floor-lsn] :as replay}
         (inspect-ha-local-bootstrap-tail m snapshot-lsn trusted-max-lsn)
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
     (assoc replay
            :installed-lsn (long verified-floor-lsn)
            :state next-m))))

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
        payload-last-lsn (:payload-last-applied-lsn copy-meta)]
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
                  (integer? payload-last-lsn))
      (u/raise "HA snapshot copy is missing payload last applied LSN"
               {:error :ha/follower-snapshot-missing-last-applied-lsn
                :db-name db-name
                :source-endpoint source-endpoint
                :copy-meta copy-meta}))
    (let [snapshot-last-lsn (when (integer? snapshot-last-lsn)
                              (long snapshot-last-lsn))
          payload-last-lsn (when (integer? payload-last-lsn)
                             (long payload-last-lsn))
          install-last-lsn (long-max2 (or payload-last-lsn 0)
                                      (or snapshot-last-lsn 0))]
      (when (< install-last-lsn (long required-lsn))
        (u/raise "HA snapshot copy payload is older than the required follower floor"
                 {:error :ha/follower-snapshot-too-stale
                  :db-name db-name
                  :required-lsn (long required-lsn)
                  :snapshot-last-applied-lsn snapshot-last-lsn
                  :payload-last-applied-lsn payload-last-lsn
                  :source-endpoint source-endpoint}))
      {:db-name db-name
       :db-identity snapshot-db-identity
       :snapshot-last-applied-lsn (or snapshot-last-lsn install-last-lsn)
       :payload-last-applied-lsn install-last-lsn})))

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
                      (log/error restore-e
                                 "HA follower snapshot install restore failed"
                                 (merge log-context
                                        {:install-message (ex-message e)
                                         :install-data (ex-data e)}))
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
                                             :restore-data (ex-data restore-e)})}})))))))))))

(defn bootstrap-ha-follower-from-snapshot*
  [{:keys [normalize-ha-bootstrap-retry-state
           ha-local-store-reopen-info
           fetch-ha-endpoint-snapshot-copy!
           validate-ha-snapshot-copy!
           install-ha-local-snapshot!
           raw-local-kv-store
           read-ha-local-snapshot-current-lsn
           reconcile-ha-installed-snapshot-state
           persist-ha-local-applied-lsn!
           note-ha-bootstrap-installed-state
           sync-ha-follower-batch]}
   db-name m lease source-order next-lsn now-ms]
  (let [required-lsn (long (max 0 (dec (long next-lsn))))]
    (loop [remaining source-order
           current-m (normalize-ha-bootstrap-retry-state
                      m m (ha-local-store-reopen-info m))
           current-reopen-info (ha-local-store-reopen-info m)
           errors []]
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
                          snapshot-lsn
                          (long (max 0
                                     (long (or (:snapshot-last-applied-lsn
                                                manifest)
                                               0))
                                     (long (if-let [kv-store
                                                    (raw-local-kv-store
                                                     installed-state)]
                                             (read-ha-local-snapshot-current-lsn
                                              kv-store)
                                             0))))
                          trusted-install-lsn
                          (long (max snapshot-lsn
                                     (long (or (:payload-last-applied-lsn
                                                manifest)
                                               0))))
                          {:keys [state installed-lsn]}
                          (reconcile-ha-installed-snapshot-state
                           installed-state
                           snapshot-lsn
                           trusted-install-lsn)
                          persisted-installed-lsn
                          (persist-ha-local-applied-lsn!
                           state
                           installed-lsn)
                          installed-state
                          (note-ha-bootstrap-installed-state
                           state
                           installed-lsn
                           source-endpoint
                           snapshot-lsn
                           now-ms
                           persisted-installed-lsn)]
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
                                              snapshot-lsn))]
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
                                           snapshot-lsn
                                           :installed-last-applied-lsn
                                           installed-lsn
                                           :resume-next-lsn
                                           (unchecked-inc
                                            (long installed-lsn))})}})))
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
         :source-order (vec source-order)
         :errors errors}))))
