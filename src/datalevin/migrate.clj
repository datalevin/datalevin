;;
;; Copyright (c) Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns ^:no-doc datalevin.migrate
  "Helpers to migrate databases by shelling out to released uberjars."
  (:require
   [clojure.java.io :as io]
   [clojure.java.shell :as sh]
   [clojure.edn :as edn]
   [taoensso.nippy :as nippy]
   [datalevin.datom :as dd]
   [datalevin.lmdb :as l]
   [datalevin.interface :as if]
   [datalevin.util :as u :refer [raise]])
  (:import
   [java.io BufferedInputStream DataInputStream File]
   [java.net URL]
   [java.nio.file AtomicMoveNotSupportedException Files Paths Path
    StandardCopyOption]
   [java.util UUID]))

(def java-opts
  ["--add-opens=java.base/java.nio=ALL-UNNAMED"
   "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED"])

(defn- jar-url
  [version]
  (format
    "https://github.com/juji-io/datalevin/releases/download/%s/datalevin-%s-standalone.jar"
    version version))

(defn ensure-jar
  [major minor patch]
  (let [version (str major "." minor "." patch)
        ^File jar-dir (io/file (u/tmp-dir "datalevin-migrate") "jars" version)
        jar     (io/file jar-dir (str "datalevin-" version "-standalone.jar"))]
    (u/create-dirs (.getPath jar-dir))
    (when-not (.exists jar)
      (try
        (with-open [in  (.openStream (URL. ^String (jar-url version)))
                    out (io/output-stream jar)]
          (io/copy in out))
        (catch Exception e
          (raise "Failed to download Datalevin uberjar: " (.getMessage e)
                 {:version version}))))
    (.getAbsolutePath jar)))

(defn run-cmd
  [cmd]
  (let [{:keys [exit err out]} (apply sh/sh cmd)]
    (when (pos? ^int exit)
      (raise "Failed to run command: " (or (not-empty err) (not-empty out))
             {:exit exit :cmd cmd}))
    out))

(defn- check-datalog
  [jar dir]
  (try
    (let [cmd  (-> ["java"]
                   (into java-opts)
                   (conj "-jar" jar "-d" dir "dump" "-l"))
          dbis (edn/read-string (run-cmd cmd))]
      (dbis "datalevin/eav"))
    (catch Exception e
      (raise "Unable to list dbis " dir {:dir dir :msg (.getMessage e)}))))

(defn- copy-export-script
  [^File tmp-root resource target]
  (let [script-resource (io/resource resource)
        script          (io/file tmp-root target)]
    (when-not script-resource
      (raise "Migration exporter resource is missing" {}))
    (with-open [in  (io/input-stream script-resource)
                out (io/output-stream script)]
      (io/copy in out))
    script))

(defn- start-export
  [jar dir ^File tmp-root resource target]
  (let [^File script (copy-export-script tmp-root resource target)
        cmd     (-> ["java"]
                    (into java-opts)
                    (conj "-cp" jar "clojure.main" (.getAbsolutePath script)
                          dir))
        process (.start (ProcessBuilder. ^java.util.List cmd))]
    {:cmd     cmd
     :process process
     :error   (future (slurp (io/reader (.getErrorStream process))))}))

(defn- start-datalog-export
  [jar dir tmp-root]
  (start-export jar dir tmp-root
                "datalevin/migration_export.clj" "export-datalog.clj"))

(defn- start-kv-export
  [jar dir tmp-root]
  (start-export jar dir tmp-root
                "datalevin/migration_kv_export.clj" "export-kv.clj"))

(defn- await-export
  [{:keys [cmd process error]}]
  (let [exit (.waitFor ^Process process)
        err  @error]
    (when (pos? exit)
      (raise "Migration export failed: " (not-empty err)
             {:exit exit :cmd cmd}))))

(defn- abort-export
  [{:keys [process error]}]
  (when (.isAlive ^Process process)
    (.destroyForcibly ^Process process))
  (.waitFor ^Process process)
  @error)

(defn- throw-export-error
  [e export]
  (let [err (abort-export export)]
    (if (seq err)
      (let [wrapped
            (ex-info (str "Migration exporter failed:\n" err)
                     (merge (ex-data e) {:export-error err}))]
        (.addSuppressed ^Throwable wrapped e)
        (throw wrapped))
      (throw e))))

(defn- path [s] (Paths/get s (make-array String 0)))

(defn- backup-path [dir] (str dir ".bak-" (System/currentTimeMillis)))

(defn- move-path
  [source target]
  (try
    (Files/move source target
                (into-array StandardCopyOption
                            [StandardCopyOption/ATOMIC_MOVE]))
    (catch AtomicMoveNotSupportedException _
      (Files/move source target (make-array StandardCopyOption 0)))))

(defn- backup-dir
  [dir]
  (let [src (.toAbsolutePath ^Path (path dir))
        dst (.toAbsolutePath ^Path (path (backup-path dir)))]
    (move-path src dst)
    (.toFile ^Path dst)))

(defn- restore-backup
  [^File backup dir]
  (when (.exists (io/file dir))
    (u/delete-files dir))
  (move-path (.toPath backup) (.toPath (io/file dir))))

(def empty-db (delay (requiring-resolve 'datalevin.db/empty-db)))
(def fill-db (delay (requiring-resolve 'datalevin.db/fill-db)))
(def close-db (delay (requiring-resolve 'datalevin.db/close-db)))
(def count-datoms (delay (requiring-resolve 'datalevin.db/-count)))

(def mixed-stream-format :datalevin/mixed-migration-v1)
(def kv-stream-format :datalevin/kv-migration-v1)

(defn- valid-kv-dbis?
  [dbis source-count]
  (and (vector? dbis)
       (integer? source-count)
       (not (neg? source-count))
       (every? (fn [{:keys [dbi entries opts]}]
                 (and (string? dbi)
                      (integer? entries)
                      (not (neg? entries))
                      (or (nil? opts) (map? opts))))
               dbis)
       (= (count dbis) (count (set (map :dbi dbis))))
       (= source-count (reduce + 0 (map :entries dbis)))))

(declare load-kv-sections)

(defn- load-datalog-stream
  [dir stream]
  (with-open [in (DataInputStream. (BufferedInputStream. stream))]
    (let [{:keys [format opts schema source-count kv-dbis kv-source-count]
           :as   header}
          (nippy/thaw-from-in! in)]
      (when-not (and (= mixed-stream-format format)
                     (map? opts)
                     (map? schema)
                     (integer? source-count)
                     (not (neg? source-count))
                     (valid-kv-dbis? kv-dbis kv-source-count))
        (raise "Invalid Datalog migration stream header" {:header header}))
      (let [db (volatile! (@empty-db dir schema opts))]
        (try
          (let [end-info (loop []
                           (let [batch (nippy/thaw-from-in! in)]
                             (if (map? batch)
                               batch
                               (do
                                 (vreset!
                                   db
                                   (@fill-db
                                     @db
                                     (map #(apply dd/datom %) batch)))
                                 (recur)))))]
            (when-not (= {:frame       :datalog-end
                          :datom-count source-count}
                         end-info)
              (raise "Invalid Datalog migration stream end"
                     {:frame end-info}))
            (let [loaded-count (@count-datoms @db [nil nil nil])
                  db-to-close  @db]
              (vreset! db nil)
              (@close-db db-to-close)
              (let [kv (l/open-kv dir)]
                (try
                  (merge
                    {:source-count source-count
                     :loaded-count loaded-count
                     :dump-count   (:datom-count end-info)}
                    (load-kv-sections kv in kv-dbis kv-source-count))
                  (finally
                    (if/close-kv kv))))))
          (finally
            (when-let [db @db]
              (@close-db db))))))))

(defn- load-kv-dbi
  [kv in {:keys [dbi entries opts] :as expected}]
  (let [start (nippy/thaw-from-in! in)]
    (when-not (= (assoc expected :frame :dbi) start)
      (raise "Invalid KV migration DBI header"
             {:expected expected :header start}))
    (if opts (if/open-dbi kv dbi opts) (if/open-dbi kv dbi))
    (if/clear-dbi kv dbi)
    (loop [loaded 0]
      (let [frame (nippy/thaw-from-in! in)]
        (cond
          (vector? frame)
          (do
            (if/transact-kv
              kv
              (map (fn [[k v]] (l/kv-tx :put dbi k v :raw :raw)) frame))
            (recur (+ loaded (count frame))))

          (= {:frame :dbi-end :dbi dbi :entry-count loaded} frame)
          (let [destination-count (if/entries kv dbi)]
            (when-not (= entries loaded destination-count)
              (raise "Migrated DBI entry count does not match source"
                     {:dbi               dbi
                      :source-count      entries
                      :dump-count        loaded
                      :destination-count destination-count}))
            destination-count)

          :else
          (raise "Invalid KV migration stream frame"
                 {:dbi dbi :frame frame}))))))

(defn- load-kv-sections
  [kv in dbis source-count]
  (let [loaded-count
        (reduce
          (fn [total expected]
            (+ total (load-kv-dbi kv in expected)))
          0
          dbis)
        end (nippy/thaw-from-in! in)]
    (when-not (= {:frame :end :entry-count source-count} end)
      (raise "Invalid KV migration stream end" {:frame end}))
    {:kv-source-count source-count
     :kv-dump-count   (:entry-count end)
     :kv-loaded-count loaded-count}))

(defn- load-kv-stream
  [dir stream]
  (with-open [in (DataInputStream. (BufferedInputStream. stream))]
    (let [{:keys [format opts dbis source-count] :as header}
          (nippy/thaw-from-in! in)]
      (when-not (and (= kv-stream-format format)
                     (map? opts)
                     (valid-kv-dbis? dbis source-count))
        (raise "Invalid KV migration stream header" {:header header}))
      (let [opts (cond-> (dissoc opts :dir :temp? :inmemory?
                                :max-val-size-changed?)
                   (:flags opts) (update :flags disj :inmemory))
            kv   (l/open-kv dir opts)]
        (try
          (let [{:keys [kv-source-count kv-dump-count kv-loaded-count]}
                (load-kv-sections kv in dbis source-count)]
            {:source-count kv-source-count
             :dump-count   kv-dump-count
             :loaded-count kv-loaded-count})
          (finally
            (if/close-kv kv)))))))

(defn- switch-databases
  [dir ^File staged]
  (let [^File backup (backup-dir dir)
        backup-path (.getAbsolutePath backup)]
    (try
      (move-path (.toPath staged) (.toPath (io/file dir)))
      (catch Throwable e
        (restore-backup backup dir)
        (throw e)))
    (println "Datalevin auto migration succeeded. Backup stored at"
             backup-path)
    backup-path))

(defn- perform-datalog-migration
  [jar dir ^File tmp-root]
  (let [^File staged (io/file (str dir ".migrating-" (UUID/randomUUID)))
        export       (start-datalog-export jar dir tmp-root)]
    (try
      (let [{:keys [source-count dump-count loaded-count
                    kv-source-count kv-dump-count kv-loaded-count]}
            (load-datalog-stream (.getAbsolutePath staged)
                                 (.getInputStream ^Process (:process export)))]
        (await-export export)
        (when (or (not= source-count dump-count loaded-count)
                  (not= kv-source-count kv-dump-count kv-loaded-count))
          (raise "Migrated mixed-store count does not match source"
                 {:source-count source-count
                  :dump-count   dump-count
                  :loaded-count loaded-count
                  :kv-source-count kv-source-count
                  :kv-dump-count   kv-dump-count
                  :kv-loaded-count kv-loaded-count
                  :dir          dir}))
        (switch-databases dir staged))
      (catch Throwable e
        (throw-export-error e export))
      (finally
        (when (.exists staged)
          (u/delete-files staged))))))

(defn- perform-kv-migration
  [jar dir ^File tmp-root]
  (let [^File staged (io/file (str dir ".migrating-" (UUID/randomUUID)))
        export       (start-kv-export jar dir tmp-root)]
    (try
      (let [{:keys [source-count dump-count loaded-count]}
            (load-kv-stream (.getAbsolutePath staged)
                            (.getInputStream ^Process (:process export)))]
        (await-export export)
        (when (not= source-count dump-count loaded-count)
          (raise "Migrated KV entry count does not match source"
                 {:source-count source-count
                  :dump-count   dump-count
                  :loaded-count loaded-count
                  :dir          dir}))
        (switch-databases dir staged))
      (catch Throwable e
        (throw-export-error e export))
      (finally
        (when (.exists staged)
          (u/delete-files staged))))))

(defn perform-migration
  [dir major minor patch]
  (let [dir       (str (.normalize (.toAbsolutePath ^Path (path dir))))
        jar       (ensure-jar major minor patch)
        datalog?  (check-datalog jar dir)
        ^File tmp-root (io/file (u/tmp-dir
                                 (str "datalevin-migrate-" (UUID/randomUUID))))]
    (try
      (u/create-dirs (.getPath tmp-root))
      (if datalog?
        (perform-datalog-migration jar dir tmp-root)
        (perform-kv-migration jar dir tmp-root))
      (finally
        (when (.exists tmp-root) (u/delete-files tmp-root))))))
