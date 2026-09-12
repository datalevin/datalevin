;;
;; Copyright (c) Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns ^:no-doc datalevin.binding.cpp.lifecycle
  "LMDB environment lifecycle: local handle registry, shutdown hooks, and
  scheduled sync."
  (:require
   [clojure.java.io :as io]
   [datalevin.async :as a]
   [datalevin.constants :as c]
   [datalevin.interface :as i :refer [close-kv env-dir]]
   [datalevin.util :as u :refer [raise]])
  (:import
   [datalevin.async IAsyncWork]
   [datalevin.cpp Env]
   [java.io File]
   [java.util.concurrent ScheduledExecutorService ScheduledFuture TimeUnit]))

(def ^:private duplicate-local-open-msg
  "Please do not open multiple LMDB connections to the same DB
           in the same process. Instead, a LMDB connection should be held onto
           and managed like a stateful resource. Refer to the documentation of
           `datalevin.core/open-kv` for more details.")

(defn- sync-key* [dir] (->> dir hash (str "lmdb-sync-") keyword))

(def sync-key (memoize sync-key*))

(deftype AsyncSync [dir ^Env env]
  IAsyncWork
  (work-key [_] (sync-key dir))
  (do-work [_] (.sync env 1))
  (combine [_] first)
  (callback [_] nil))

(defn start-scheduled-sync
  [scheduled-sync dir ^Env env]
  (let [scheduler ^ScheduledExecutorService (u/get-scheduler)
        fut (.scheduleWithFixedDelay
             scheduler
             ^Runnable #(let [exe (a/get-executor)]
                          (when (a/running? exe)
                            (a/exec exe (AsyncSync. dir env))))
             ^long (rand-int c/lmdb-sync-interval)
             ^long c/lmdb-sync-interval
             TimeUnit/SECONDS)]
    (vreset! scheduled-sync fut)))

(defonce shutdown-hooks (atom {}))
(defonce ^:private shutdown-close-actions (atom {}))
(defonce ^:private active-local-kv-handles (atom #{}))
(defonce ^:private open-local-kv-handles (atom {}))

(defn- canonical-dir-key
  [^File dir-file]
  (.getCanonicalPath dir-file))

(defn local-kv-handle-key
  [^File dir-file flags]
  (when-not (some #{:inmemory} flags)
    (canonical-dir-key dir-file)))

(defn reserve-local-kv-handle!
  [^File dir-file flags]
  (when-let [dir-key (local-kv-handle-key dir-file flags)]
    (let [[before _]
          (swap-vals! active-local-kv-handles
                      #(if (contains? % dir-key) % (conj % dir-key)))]
      (when (contains? before dir-key)
        (raise duplicate-local-open-msg
               {:dir dir-key
                :type :lmdb/duplicate-open}))
      dir-key)))

(defn register-local-kv-handle!
  [dir-key lmdb]
  (when dir-key
    (swap! open-local-kv-handles assoc dir-key lmdb))
  lmdb)

(defn open-local-kv-handle
  [dir]
  (when-let [dir-key (some-> dir u/file canonical-dir-key)]
    (locking open-local-kv-handles
      (when-let [lmdb (get @open-local-kv-handles dir-key)]
        (if (i/closed-kv? lmdb)
          (do
            (swap! open-local-kv-handles dissoc dir-key)
            (swap! active-local-kv-handles disj dir-key)
            nil)
          lmdb)))))

(defn release-local-kv-handle!
  [dir-key]
  (when dir-key
    (swap! active-local-kv-handles disj dir-key)
    (swap! open-local-kv-handles dissoc dir-key))
  nil)

(defn register-shutdown-hook!
  [dir ^Thread hook]
  (.addShutdownHook (Runtime/getRuntime) hook)
  (swap! shutdown-hooks assoc dir hook)
  nil)

(defn register-shutdown-close!
  "Register a higher-level close action for an open LMDB environment.
   The raw JVM shutdown hook uses this action when present and otherwise
   falls back to closing LMDB directly."
  [lmdb close-fn]
  (swap! shutdown-close-actions assoc (env-dir lmdb) close-fn)
  nil)

(defn run-shutdown-close!
  [dir lmdb]
  (if-let [close-fn (get @shutdown-close-actions dir)]
    (close-fn)
    (close-kv lmdb)))

(defn unregister-shutdown-hook!
  [dir]
  (swap! shutdown-close-actions dissoc dir)
  (when-let [^Thread hook (get @shutdown-hooks dir)]
    (swap! shutdown-hooks dissoc dir)
    (try
      (.removeShutdownHook (Runtime/getRuntime) hook)
      (catch IllegalStateException _)
      (catch IllegalArgumentException _)
      (catch SecurityException _)))
  nil)

(defn stop-scheduled-sync
  [scheduled-sync]
  (when-let [fut @scheduled-sync]
    (.cancel ^ScheduledFuture fut true)
    (vreset! scheduled-sync nil)))

(defn copy-version-file
  [lmdb dest]
  (let [src (str (env-dir lmdb) u/+separator+ c/version-file-name)
        dst (str dest u/+separator+ c/version-file-name)]
    (u/copy-file src dst)))

(defn copy-compression-files
  [lmdb dest]
  (doseq [name [c/keycode-file-name c/valcode-file-name]
          :let [src (io/file (env-dir lmdb) name)]
          :when (.isFile src)]
    (u/copy-file (str src) (str (io/file dest name)))))
