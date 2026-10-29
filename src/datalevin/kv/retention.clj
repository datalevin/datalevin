;;
;; Copyright (c) Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns ^:no-doc datalevin.kv.retention
  "Txn-log segment retention and GC bookkeeping."
  (:require
   [datalevin.constants :as c]
   [datalevin.interface :as i]
   [datalevin.kv.txlog :as kvtx :refer [read-commit-marker-state
                                        txlog-backup-pin-floor-state
                                        txlog-config-enabled?
                                        txlog-replica-floor-state
                                        txlog-rollout-mode
                                        txlog-rollout-watermarks
                                        txlog-snapshot-floor-state
                                        txlog-vector-floor-state
                                        txlog-record-lsn
                                        txlog-watermarks
                                        txlog-write-path-enabled?
                                        with-runtime-txlog-state-guard]]
   [datalevin.txlog :as txlog]
   [datalevin.txlog.segment :as segment]
   [datalevin.util :as u])
  (:import [java.nio.file Files Paths NoSuchFileException]))

(defn- local-dir?
  [dir]
  (and (string? dir)
       (.isDirectory (java.io.File. ^String dir))))

(defn delete-txlog-segment!
  [{:keys [segment-id path bytes]}]
  (let [deleted? (try (Files/deleteIfExists (Paths/get ^String path (make-array String 0)))
                      (catch NoSuchFileException _ false))]
    {:segment-id segment-id :path path :bytes (long (or bytes 0))
     :deleted? deleted? :already-deleted? (not deleted?) :error nil}))

(defn txlog-retention-state-map
  [db operator-retain-floor-lsn explicit-gc?]
  (let [runtime-state (txlog/state db)
        watermarks (txlog-watermarks db)
        txlog? (:wal? watermarks)
        dir (:dir watermarks)]
    (when (and txlog? (local-dir? dir))
      (let [info (or (i/env-opts db) {})
            retention-bytes (long (or (:wal-retention-bytes info)
                                      c/*wal-retention-bytes*))
            retention-ms (long (or (:wal-retention-ms info)
                                   c/*wal-retention-ms*))
            marker-state (or (:commit-marker watermarks)
                             (read-commit-marker-state db))
            marker (:current marker-state)
            {:keys [segments marker-record min-retained-lsn total-bytes
                    newest-segment-id]}
            (txlog/segment-summaries
             dir
             {:allow-preallocated-tail? true
              :record->lsn txlog-record-lsn
              :cache-v (some-> runtime-state :segment-summaries-cache)
              :cache-key :txlog-record-lsn
              :active-segment-id
              (some-> runtime-state :segment-id deref long)
              :active-segment-offset
              (some-> runtime-state :segment-offset deref long)
              :marker-segment-id (some-> marker :txlog-segment-id long)
              :marker-offset (some-> marker :txlog-record-offset long)
              :min-retained-fallback (long (or (:next-lsn watermarks) 0))})
            has-records? (boolean (some (comp pos? :record-count) segments))
            {:keys [valid-marker applied-lsn]}
            (txlog/resolve-applied-lsn
             {:commit-marker? (:commit-marker? watermarks)
              :marker marker
              :marker-record marker-record
              :has-records? has-records?
              :meta-last-applied-lsn (:last-applied-lsn watermarks)
              :min-retained-lsn min-retained-lsn})
            snapshot-state (txlog-snapshot-floor-state db info applied-lsn)
            vector-state (txlog-vector-floor-state db info)
            replica-state (txlog-replica-floor-state db info)
            backup-state (txlog-backup-pin-floor-state db info)
            floors (txlog/retention-floors
                    {:snapshot-state snapshot-state
                     :vector-state vector-state
                     :replica-state replica-state
                     :backup-state backup-state
                     :operator-retain-floor-lsn
                     operator-retain-floor-lsn})
            active-segment-id (long (or (:segment-id watermarks) 0))]
        (txlog/retention-state-report
         {:dir dir
          :retention-bytes retention-bytes
          :retention-ms retention-ms
          :segments segments
          :total-bytes total-bytes
          :active-segment-id active-segment-id
          :newest-segment-id newest-segment-id
          :min-retained-lsn min-retained-lsn
          :applied-lsn applied-lsn
          :marker-state marker-state
          :valid-marker valid-marker
          :floors floors
          :floor-providers {:snapshot snapshot-state
                            :vector vector-state
                            :replica replica-state
                            :backup backup-state}
          :explicit-gc? explicit-gc?})))))

(defn- log-admin-metadata! [wrapped raw-db txs kt]
  ;; Catalog administration already owns its standalone native transaction.
  ;; Do not enqueue a data batch while the admin caller holds the writer lock.
  (if (:embedded? (:independent-control @(i/kv-info raw-db)))
    (kvtx/transact-with-txlog! raw-db (txlog/state raw-db) c/kv-info txs kt :data)
    (i/transact-kv wrapped c/kv-info txs kt :data)))

(defn txlog-log-dbi-registration!
  [wrapped raw-db dbi-name before after]
  (when (and (not= dbi-name c/kv-info)
             (not= before after)
             (txlog-write-path-enabled? raw-db))
    (log-admin-metadata! wrapped raw-db [[:put [:dbis dbi-name] after]] [:keyword :string])))

(defn txlog-log-dbi-drop!
  [wrapped raw-db dbi-name before]
  (when (and (not= dbi-name c/kv-info)
             (some? before)
             (txlog-write-path-enabled? raw-db))
    (log-admin-metadata! wrapped raw-db [[:del [:dbis dbi-name]]] [:keyword :string])))

(defn txlog-retention-state-local
  [db]
  (with-runtime-txlog-state-guard
    db
    (fn []
      (if-let [state (txlog-retention-state-map db nil false)]
        (dissoc state :gc-target-segments)
        (if (txlog-config-enabled? db)
          (let [rollout-mode (txlog-rollout-mode db)]
            {:wal? true
             :skipped? true
             :reason :rollback
             :watermarks (txlog-rollout-watermarks db rollout-mode)})
          {:wal? false})))))

(defn- serializable-data [value]
  (cond
    (instance? Throwable value) {:message (ex-message value)}
    (map? value) (into {} (map (fn [[k v]] [k (serializable-data v)])) value)
    (sequential? value) (mapv serializable-data value)
    (set? value) (into #{} (map serializable-data) value)
    :else value))

(defn- gc-error [code error data]
  {:error code :message (or (ex-message error) (name code))
   :data (serializable-data (merge (ex-data error) data))})

(defn- empty-gc-result [dry-run?]
  {:ok? false :dry-run? dry-run? :deleted-count 0 :deleted-bytes 0
   :deleted-segment-ids [] :deleted-segments [] :segment-results []
   :failed-count 0 :errors [] :durability {:status :not-needed}})

(defn- skipped-gc-result
  [db retain-floor-lsn dry-run?]
  (merge (empty-gc-result dry-run?)
         {:skipped? true :retain-floor-lsn retain-floor-lsn}
         (if (txlog-config-enabled? db)
           (if (= :rollback (txlog-rollout-mode db))
             {:reason :rollback :watermarks (txlog-rollout-watermarks db :rollback)}
             (throw (ex-info "WAL GC retention plan is unavailable" {})))
           {:reason :wal-disabled :watermarks {:wal? false}})))

(defn gc-txlog-segments-plan
  "Select all floor-safe segments, independent of retention age/byte caps.
  Applying a plan requires the maintenance lock and runtime state guard."
  ([db retain-floor-lsn]
   (gc-txlog-segments-plan db retain-floor-lsn txlog-retention-state-map))
  ([db retain-floor-lsn report]
   (when-let [before (report db retain-floor-lsn true)]
     {:before (dissoc before :gc-target-segments)
      :targets (vec (:gc-target-segments before))})))

(defn- sync-gc-directory! [path]
  (if (u/windows?)
    {:status :unsupported}
    (do (segment/force-parent-directory! path) {:status :synced})))

(defn- apply-gc-plan!
  [db retain-floor-lsn dry-run? {:keys [before targets]}
   {:keys [delete! sync-directory! report]}]
  (let [segment-results
        (if dry-run? []
            (mapv (fn [target]
                    (try (delete! target)
                         (catch Exception e
                           (merge (select-keys target [:segment-id :path :bytes])
                                  {:deleted? false :already-deleted? false
                                   :error (gc-error :gc/delete-failed e
                                                    (select-keys target [:segment-id :path]))}))))
                  targets))
        deleted (filterv :deleted? segment-results)
        deleted-bytes (reduce (fn [^long total item] (+ total (long (:bytes item)))) 0 deleted)
        errors (into [] (keep :error) segment-results)
        errors (try
                 (when (pos? deleted-bytes)
                   (when-let [state (txlog/state db)]
                     (txlog/note-gc-deleted-bytes! state deleted-bytes)))
                 errors
                 (catch Exception e (conj errors (gc-error :gc/accounting-failed e {}))))
        durability (if (empty? deleted) {:status :not-needed}
                       (try (sync-directory! (:path (first deleted)))
                            (catch Exception e
                              {:status :failed :error (gc-error :gc/directory-sync-failed e {})})))
        errors (cond-> errors
                 (= :unsupported (:status durability))
                 (conj {:error :gc/directory-sync-unsupported
                        :message "Directory sync is unsupported on this platform" :data {}})
                 (:error durability) (conj (:error durability)))
        final-report (try {:after (if-let [after (report db retain-floor-lsn false)]
                                    (dissoc after :gc-target-segments)
                                    (throw (ex-info "WAL GC final report is unavailable" {})))}
                          (catch Exception e {:after nil :error (gc-error :gc/report-failed e {})}))
        errors (cond-> errors (:error final-report) (conj (:error final-report)))]
    (cond-> (merge (empty-gc-result dry-run?)
                   {:ok? (boolean (and (empty? errors) (#{:synced :not-needed} (:status durability))))
                    :before before :after (:after final-report)
                    :operator-retain-floor-lsn (get-in before [:floors :operator-retain-floor-lsn])
                    :segment-results segment-results :errors errors
                    :failed-count (count (filter :error segment-results))
                    :deleted-count (count deleted) :deleted-bytes deleted-bytes
                    :deleted-segment-ids (mapv :segment-id deleted)
                    :deleted-segments (mapv #(select-keys % [:segment-id :path]) deleted)
                    :durability (dissoc durability :error)})
      dry-run? (assoc :targets targets :would-delete-count (count targets)
                      :would-delete-bytes (reduce (fn [^long n t] (+ n (long (:bytes t)))) 0 targets)))))

(defn gc-txlog-segments-local!
  "Run explicit GC under the same maintenance lock as snapshots. Dependencies
  are explicit so failure tests exercise real stores without rebinding Vars."
  ([db retain-floor-lsn]
   (if (map? retain-floor-lsn)
     (gc-txlog-segments-local! db (:retain-floor-lsn retain-floor-lsn) retain-floor-lsn {})
     (gc-txlog-segments-local! db retain-floor-lsn {} {})))
  ([db retain-floor-lsn opts] (gc-txlog-segments-local! db retain-floor-lsn opts {}))
  ([db retain-floor-lsn {:keys [dry-run?] :or {dry-run? false}} dependencies]
   (when-not (or (nil? dry-run?) (boolean? dry-run?))
     (throw (ex-info "GC dry-run flag must be boolean" {:error :gc/invalid-options})))
   (let [dry-run? (boolean dry-run?)
         dependencies (merge {:delete! delete-txlog-segment! :sync-directory! sync-gc-directory!
                              :report txlog-retention-state-map} dependencies)
         collect! (fn []
                    (try
                      (with-runtime-txlog-state-guard
                        db
                        (fn []
                          (if-let [plan (gc-txlog-segments-plan db retain-floor-lsn (:report dependencies))]
                            (apply-gc-plan! db retain-floor-lsn dry-run? plan dependencies)
                            (skipped-gc-result db retain-floor-lsn dry-run?))))
                      (catch Exception e
                        (assoc (empty-gc-result dry-run?) :before nil :after nil
                               :errors [(gc-error :gc/report-failed e {})]))))]
     (loop []
       (let [result
             (if-let [state (txlog/state db)]
               (or (try
                     (txlog/try-with-maintenance-lock
                       state
                       #(with-runtime-txlog-state-guard
                          db
                          (fn []
                            (if (identical? state (txlog/state db)) (collect!) ::retry))))
                     (catch Exception e
                       (assoc (empty-gc-result dry-run?)
                              :errors [(gc-error :gc/maintenance-lock-failed e {})])))
                   (assoc (empty-gc-result dry-run?) :skipped? true :reason :maintenance-busy
                          :errors [{:error :gc/maintenance-busy
                                    :message "WAL maintenance is busy" :data {}}]))
               (with-runtime-txlog-state-guard
                 db
                 #(if (txlog/state db) ::retry (collect!))))]
         (if (= ::retry result) (recur) result))))))
