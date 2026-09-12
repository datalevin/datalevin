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
   [clojure.java.io :as io]
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
   [datalevin.util :refer [raise]]))

(defn- local-dir?
  [dir]
  (and (string? dir)
       (.isDirectory (java.io.File. ^String dir))))

(defn delete-txlog-segment!
  [{:keys [segment-id path]}]
  (try
    (io/delete-file path)
    {:segment-id segment-id :path path}
    (catch Exception e
      (raise "Failed to delete txn-log segment"
             e
             {:type :txlog/gc-delete-failed
              :segment-id segment-id
              :path path}))))

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

(defn txlog-log-dbi-registration!
  [wrapped raw-db dbi-name before after]
  (when (and (not= dbi-name c/kv-info)
             (not= before after)
             (txlog-write-path-enabled? raw-db))
    (i/transact-kv wrapped
                   c/kv-info
                   [[:put [:dbis dbi-name] after]]
                   [:keyword :string]
                   :data)))

(defn txlog-log-dbi-drop!
  [wrapped raw-db dbi-name before]
  (when (and (not= dbi-name c/kv-info)
             (some? before)
             (txlog-write-path-enabled? raw-db))
    (i/transact-kv wrapped
                   c/kv-info
                   [[:del [:dbis dbi-name]]]
                   [:keyword :string])))

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

(defn gc-txlog-segments-local!
  [db retain-floor-lsn]
  (with-runtime-txlog-state-guard
    db
    (fn []
      (if-let [before (txlog-retention-state-map db retain-floor-lsn true)]
        (let [targets (:gc-target-segments before)
              deleted (mapv delete-txlog-segment! targets)
              deleted-bytes (reduce (fn [acc {:keys [bytes]}]
                                      (+ ^long acc ^long bytes))
                                    0 targets)
              _ (when-let [state (txlog/state db)]
                  (txlog/note-gc-deleted-bytes! state deleted-bytes))
              after (txlog-retention-state-map db retain-floor-lsn false)]
          {:ok? true
           :deleted-count (count deleted)
           :deleted-bytes deleted-bytes
           :deleted-segment-ids (mapv :segment-id targets)
           :deleted-segments deleted
           :operator-retain-floor-lsn
           (get-in before [:floors :operator-retain-floor-lsn])
           :before (dissoc before :gc-target-segments)
           :after (dissoc after :gc-target-segments)})
        (if (txlog-config-enabled? db)
          (let [rollout-mode (txlog-rollout-mode db)]
            {:ok? false
             :skipped? true
             :reason :rollback
             :retain-floor-lsn retain-floor-lsn
             :watermarks (txlog-rollout-watermarks db rollout-mode)})
          {:ok? false
           :skipped? true
           :reason :wal-disabled
           :retain-floor-lsn retain-floor-lsn
           :watermarks {:wal? false}})))))
