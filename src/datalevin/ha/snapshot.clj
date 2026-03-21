;;
;; Copyright (c) Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2.0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns ^:no-doc datalevin.ha.snapshot
  "Filesystem and local-store helpers shared by HA snapshot install paths."
  (:require
   [clojure.edn :as edn]
   [clojure.string :as s]
   [datalevin.db :as db]
   [datalevin.interface :as i]
   [datalevin.util :as u]
   [taoensso.timbre :as log])
  (:import
   [datalevin.io PosixFsync]
   [datalevin.interface IStore ILMDB]
   [datalevin.storage Store]
   [java.io File]
   [java.nio.channels FileChannel]
   [java.nio.file AtomicMoveNotSupportedException Files Paths
    StandardCopyOption StandardOpenOption]
   [java.util UUID]))

(def ^"[Ljava.nio.file.StandardOpenOption;"
  ha-sync-read-open-options
  (into-array StandardOpenOption [StandardOpenOption/READ]))

(defn ^:redef fsync-ha-snapshot-path!
  [path]
  (with-open [^FileChannel ch (FileChannel/open
                               (Paths/get path (into-array String []))
                               ha-sync-read-open-options)]
    (PosixFsync/fsync ch)))

(defn- fsync-ha-snapshot-parent-dir!
  [^String path]
  (when-let [^File parent (.getParentFile (File. path))]
    (when (.exists parent)
      (fsync-ha-snapshot-path! (.getPath parent)))))

(defn sync-ha-snapshot-dir-tree!
  [dir]
  (doseq [^File f (sort-by #(.getName ^File %)
                           (or (u/list-files dir) []))]
    (if (.isDirectory f)
      (sync-ha-snapshot-dir-tree! (.getPath f))
      (fsync-ha-snapshot-path! (.getPath f))))
  (fsync-ha-snapshot-path! dir))

(defn ^:redef sync-ha-snapshot-install-target!
  [^String env-dir]
  (when (u/file-exists env-dir)
    (sync-ha-snapshot-dir-tree! env-dir)
    (fsync-ha-snapshot-parent-dir! env-dir)))

(defn copy-dir-contents!
  [src-dir dest-dir]
  (u/create-dirs dest-dir)
  (doseq [^File f (or (u/list-files src-dir) [])]
    (let [dst (str dest-dir u/+separator+ (.getName f))]
      (if (.isDirectory f)
        (copy-dir-contents! (.getPath f) dst)
        (u/copy-file (.getPath f) dst)))))

(defn ^:redef atomic-move-replace-existing-paths!
  [src-path dst-path]
  (Files/move src-path dst-path
              (into-array java.nio.file.CopyOption
                          [StandardCopyOption/REPLACE_EXISTING
                           StandardCopyOption/ATOMIC_MOVE])))

(defn move-path!
  [src dst]
  (let [src-path (Paths/get src (into-array String []))
        dst-path (Paths/get dst (into-array String []))]
    (try
      (atomic-move-replace-existing-paths! src-path dst-path)
      (catch AtomicMoveNotSupportedException e
        (u/raise "HA snapshot install requires atomic path moves"
                 e
                 {:error :ha/follower-snapshot-atomic-move-unsupported
                  :src src
                  :dst dst})))))

(def ^:private ha-snapshot-install-marker-suffix
  ".ha-snapshot-install.edn")

(defn ha-snapshot-install-marker-path
  [env-dir]
  (str env-dir ha-snapshot-install-marker-suffix))

(defn read-ha-snapshot-install-marker
  [env-dir]
  (let [marker-path (ha-snapshot-install-marker-path env-dir)]
    (when (u/file-exists marker-path)
      (let [marker (try
                     (edn/read-string (slurp marker-path))
                     (catch Exception e
                       (u/raise "HA snapshot install marker is unreadable"
                                e
                                {:error :ha/follower-snapshot-install-marker-invalid
                                 :env-dir env-dir
                                 :marker-path marker-path})))
            backup-dir (:backup-dir marker)
            stage (:stage marker)
            stage-dir (:stage-dir marker)]
        (when-not (and (map? marker)
                       (string? backup-dir)
                       (not (s/blank? backup-dir))
                       (or (nil? stage-dir)
                           (and (string? stage-dir)
                                (not (s/blank? stage-dir))))
                       (keyword? stage))
          (u/raise "HA snapshot install marker is invalid"
                   {:error :ha/follower-snapshot-install-marker-invalid
                    :env-dir env-dir
                    :marker-path marker-path
                    :marker marker}))
        (assoc marker :marker-path marker-path)))))

(defn write-ha-snapshot-install-marker!
  [env-dir marker]
  (let [marker-path (ha-snapshot-install-marker-path env-dir)]
    (spit marker-path
          (str (pr-str marker) "\n"))
    (fsync-ha-snapshot-path! marker-path)
    (fsync-ha-snapshot-parent-dir! marker-path)))

(defn delete-ha-snapshot-install-marker!
  [env-dir]
  (let [marker-path (ha-snapshot-install-marker-path env-dir)]
    (when (u/file-exists marker-path)
      (u/delete-files marker-path)
      (fsync-ha-snapshot-parent-dir! marker-path))))

(defn- delete-ha-snapshot-install-stage-dir!
  [{:keys [stage-dir]}]
  (when (and (string? stage-dir)
             (not (s/blank? stage-dir))
             (u/file-exists stage-dir))
    (u/delete-files stage-dir)))

(defn- ha-snapshot-install-displaced-path
  [env-dir]
  (str env-dir ".ha-restore-displaced-" (UUID/randomUUID)))

(defn restore-ha-snapshot-install-backup!
  [env-dir backup-dir]
  (let [displaced-dir
        (when (u/file-exists env-dir)
          (let [displaced-dir (ha-snapshot-install-displaced-path env-dir)]
            (move-path! env-dir displaced-dir)
            displaced-dir))]
    (try
      (move-path! backup-dir env-dir)
      (when (and displaced-dir
                 (u/file-exists displaced-dir))
        (u/delete-files displaced-dir))
      (catch Exception e
        (when (and displaced-dir
                   (u/file-exists displaced-dir)
                   (not (u/file-exists env-dir))
                   (u/file-exists backup-dir))
          (try
            (move-path! displaced-dir env-dir)
            (catch Exception rollback-e
              (log/error rollback-e
                         "HA snapshot install restore failed to roll back displaced store"
                         {:env-dir env-dir
                          :backup-dir backup-dir
                          :displaced-dir displaced-dir}))))
        (throw e)))))

(defn- recover-ha-snapshot-install-from-backup!
  [env-dir {:keys [backup-dir stage] :as marker} log-message]
  (cond
    (u/file-exists backup-dir)
    (do
      (log/warn log-message
                {:env-dir env-dir
                 :backup-dir backup-dir
                 :stage stage})
      (delete-ha-snapshot-install-stage-dir! marker)
      (restore-ha-snapshot-install-backup! env-dir backup-dir)
      (delete-ha-snapshot-install-marker! env-dir)
      marker)

    (u/file-exists env-dir)
    (do
      (log/warn "Completing HA snapshot install recovery after backup was already restored"
                {:env-dir env-dir
                 :backup-dir backup-dir
                 :stage stage})
      (delete-ha-snapshot-install-stage-dir! marker)
      (delete-ha-snapshot-install-marker! env-dir)
      marker)

    :else
    (u/raise "HA snapshot install backup is missing during recovery"
             {:error :ha/follower-snapshot-install-recovery-failed
              :env-dir env-dir
              :backup-dir backup-dir
              :stage stage})))

(defn recover-ha-local-snapshot-install!
  [env-dir]
  (when-let [{:keys [backup-dir stage] :as marker}
             (read-ha-snapshot-install-marker env-dir)]
    (case stage
      :prepare
      (cond
        (u/file-exists backup-dir)
        (do
          (log/warn "Recovering HA snapshot install from staged backup"
                    {:env-dir env-dir
                     :backup-dir backup-dir
                     :stage stage})
          (delete-ha-snapshot-install-stage-dir! marker)
          (restore-ha-snapshot-install-backup! env-dir backup-dir)
          (delete-ha-snapshot-install-marker! env-dir)
          marker)

        (u/file-exists env-dir)
        (do
          (delete-ha-snapshot-install-stage-dir! marker)
          (delete-ha-snapshot-install-marker! env-dir)
          marker)

        :else
        (u/raise "HA snapshot install marker has no recoverable store"
                 {:error :ha/follower-snapshot-install-recovery-failed
                  :env-dir env-dir
                  :backup-dir backup-dir
                  :stage stage}))

      :backup-moving
      (cond
        (u/file-exists backup-dir)
        (do
          (log/warn "Recovering HA snapshot install while backup move was in progress"
                    {:env-dir env-dir
                     :backup-dir backup-dir
                     :stage stage})
          (delete-ha-snapshot-install-stage-dir! marker)
          (restore-ha-snapshot-install-backup! env-dir backup-dir)
          (delete-ha-snapshot-install-marker! env-dir)
          marker)

        (u/file-exists env-dir)
        (do
          (delete-ha-snapshot-install-stage-dir! marker)
          (delete-ha-snapshot-install-marker! env-dir)
          marker)

        :else
        (u/raise "HA snapshot install marker has no recoverable store"
                 {:error :ha/follower-snapshot-install-recovery-failed
                  :env-dir env-dir
                  :backup-dir backup-dir
                  :stage stage}))

      :backup-moved
      (recover-ha-snapshot-install-from-backup!
       env-dir
       marker
       "Recovering HA snapshot install after interrupted store swap")

      :snapshot-staged
      (recover-ha-snapshot-install-from-backup!
       env-dir
       marker
       "Recovering HA snapshot install after interrupted staged snapshot")

      (u/raise "HA snapshot install marker has an unsupported stage"
               {:error :ha/follower-snapshot-install-marker-invalid
                :env-dir env-dir
                :marker marker}))))

(defn recover-ha-local-store-dir-if-needed!
  [env-dir]
  (when (read-ha-snapshot-install-marker env-dir)
    (recover-ha-local-snapshot-install! env-dir)))

(defn- close-best-effort!
  [f x]
  (when x
    (try
      (f x)
      (catch Throwable _
        nil))))

(defn close-ha-local-store!
  [m]
  ;; HA store swaps must also retire write-side wrappers that share the same
  ;; LMDB env; leaving them live can poison subsequent reader creation after a
  ;; snapshot install or local reopen.
  (close-best-effort! db/close-db (:wdt-db m))
  (close-best-effort!
    (fn [store]
      (cond
        (instance? IStore store) (i/close store)
        (instance? ILMDB store) (i/close-kv store)
        :else nil))
    (:wstore m))
  (close-best-effort! i/close-kv (:wlmdb m))
  (close-best-effort! db/close-db (:dt-db m))
  (close-best-effort!
    (fn [store]
      (cond
        (instance? IStore store) (i/close store)
        (instance? ILMDB store) (i/close-kv store)
        :else nil))
    (:store m)))

(defn refresh-ha-local-dt-db
  [m]
  (let [store (:store m)]
    (if (instance? IStore store)
      (let [info {:max-eid (i/init-max-eid store)
                  :max-tx (long (i/max-tx store))
                  :last-modified (long (i/last-modified store))}]
        (assoc m :dt-db (db/new-db store info)))
      m)))

(defn open-ha-store-dbis!
  [store]
  (when-let [kv-store (cond
                        (instance? Store store)
                        (.-lmdb ^Store store)

                        (instance? ILMDB store)
                        store

                        :else nil)]
    (doseq [dbi-name (or (i/list-dbis kv-store) [])]
      (let [dbi-opts (try
                       (i/dbi-opts kv-store dbi-name)
                       (catch Exception _
                         nil))]
        (if (map? dbi-opts)
          (i/open-dbi kv-store dbi-name dbi-opts)
          (i/open-dbi kv-store dbi-name)))))
  store)

(defn ha-class-name
  [x]
  (some-> x class .getName))

(defn ha-retrieved-like?
  [x]
  (= "datalevin.bits.Retrieved" (ha-class-name x)))

(defn ha-reflect-field
  [x field-name]
  (when x
    (let [^Class cls (class x)
          field (.getDeclaredField cls field-name)]
      (.setAccessible field true)
      (.get field x))))

(defn ha-seq-like?
  [x]
  (or (sequential? x)
      (instance? java.util.List x)))
