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
   [datalevin.datom :as dd]
   [datalevin.lmdb :as l]
   [datalevin.interface :as if]
   [datalevin.util :as u :refer [raise]])
  (:import
   [java.io File FileInputStream PushbackReader]
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

(defn- dump-db
  [jar dir dump-file datalog?]
  (let [cmd (-> ["java"]
                (into java-opts)
                (conj "-jar" jar "-d" dir "-f" dump-file)
                (cond-> datalog? (conj "-g")
                        (not datalog?) (conj "-a"))
                (conj "dump"))]
    (try
      (run-cmd cmd)
      (catch Throwable _
        (when (.exists (io/file dump-file))
          (u/delete-files dump-file))
        (raise "Unable to dump DB" dir {:dir dir :datalog? datalog?})))))

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

(def init-db (delay (resolve 'datalevin.db/init-db)))
(def close-db (delay (resolve 'datalevin.db/close-db)))

(defn- load-dump
  [dir dump-path datalog?]
  (let [f  (FileInputStream. ^String dump-path)
        in (java.io.PushbackReader. (java.io.InputStreamReader. f))]
    (if datalog?
      (with-open [^PushbackReader r in]
        (let [read-form     #(edn/read {:eof     ::EOF
                                        :readers *data-readers*} r)
              read-maps     #(let [m1 (read-form)]
                               (if (:db/ident m1)
                                 [nil m1]
                                 [m1 (read-form)]))
              [opts schema] (read-maps)
              datoms        (->> (repeatedly read-form)
                                 (take-while #(not= ::EOF %))
                                 (map #(apply dd/datom %)))
              db            (@init-db datoms dir schema opts)]
          (@close-db db)))
      (let [lmdb (l/open-kv dir)]
        (l/load-all lmdb in false)
        (if/close-kv lmdb)))
    (.close f)))

(defn perform-migration
  [dir major minor patch]
  (let [jar       (ensure-jar major minor patch)
        datalog?  (check-datalog jar dir)
        ^File tmp-root (io/file (u/tmp-dir
                                 (str "datalevin-migrate-" (UUID/randomUUID))))
        dump-file (io/file tmp-root "dump.nippy")
        dump-path (.getAbsolutePath dump-file)]
    (try
      (u/create-dirs (.getPath tmp-root))
      (dump-db jar dir dump-path datalog?)
      (let [^File backup (backup-dir dir)
            backup-path  (.getAbsolutePath backup)]
        (try
          (load-dump dir dump-path datalog?)
          (println "Datalevin auto migration succeeded. Backup stored at"
                   backup-path)
          backup-path
          (catch Throwable e
            (restore-backup backup dir)
            (throw e))))
      (finally
        (when (.exists dump-file) (u/delete-files dump-file))
        (when (.exists tmp-root) (u/delete-files tmp-root))))))
