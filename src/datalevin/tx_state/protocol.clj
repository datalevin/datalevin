;; Copyright (c) Huahai Yang. All rights reserved.
;; Distributed under the Eclipse Public License 2.0.
(ns ^:no-doc datalevin.tx-state.protocol
  "Lifetime write-protocol leases, separate from LMDB's lock.mdb and the WAL's
  short recovery/maintenance locks. No native open may precede this check."
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [datalevin.tx-state.lifetime :as lifetime])
  (:import [java.nio ByteBuffer]
           [java.nio.channels FileChannel FileLock OverlappingFileLockException]
           [java.nio.charset StandardCharsets]
           [java.nio.file Files OpenOption StandardOpenOption CopyOption
            StandardCopyOption]
           [java.util.concurrent ConcurrentHashMap]
           [java.util.concurrent.atomic AtomicBoolean AtomicInteger]))

(defonce ^:private ^ConcurrentHashMap holders (ConcurrentHashMap.))
(def ^:private modes #{:legacy-writer-v1 :kv-independent-v1})
(def ^:private open-options
  (into-array OpenOption [StandardOpenOption/CREATE StandardOpenOption/READ
                          StandardOpenOption/WRITE]))
(def ^:private read-options (into-array OpenOption [StandardOpenOption/READ]))

(def ^:dynamic *independent-native-open?*
  "Bound only by the independent runtime while opening recovery/native files."
  false)

(defn ^:redef phase!
  "Open/marker fault seam; disabled production calls allocate no event payload."
  [_event _context]
  nil)

(defn ^:redef read-write-protocol-marker [dir]
  (let [file (io/file dir "write-protocol.edn")]
    (phase! :before-marker-read file)
    (when (.exists file)
      (let [marker (edn/read-string {:readers {} :default (fn [tag _]
                                                       (throw (ex-info "Unknown protocol marker tag"
                                                                       {:tag tag})))}
                                    (slurp file))]
        (phase! :after-marker-read marker)
        marker))))

(defn ^:redef publish-write-protocol-marker!
  "Publish only under exclusive protocol ownership, before native open."
  [dir marker]
  (let [parent (.toPath (io/file dir))
        tmp (Files/createTempFile parent "write-protocol-" ".tmp"
                                  (make-array java.nio.file.attribute.FileAttribute 0))
        target (.toPath (io/file dir "write-protocol.edn"))]
    (try
      (phase! :before-marker-publish target)
      (with-open [ch (FileChannel/open tmp open-options)]
        (let [buffer (ByteBuffer/wrap (.getBytes (pr-str marker) StandardCharsets/UTF_8))]
          (while (.hasRemaining buffer) (.write ch buffer)))
        (.force ch true))
      (Files/move tmp target
                  (into-array CopyOption [StandardCopyOption/ATOMIC_MOVE
                                          StandardCopyOption/REPLACE_EXISTING]))
      (with-open [ch (FileChannel/open parent read-options)] (.force ch true))
      (phase! :after-marker-publish target)
      marker
      (finally (Files/deleteIfExists tmp)))))

(defn- mismatch! [dir expected actual]
  (throw (ex-info "Environment write protocol does not match; use quiescent recovery"
                  {:error :txlog/write-protocol-mismatch :dir dir
                   :expected expected :actual actual :retryable? false})))

(defn- validate-marker! [dir mode identity marker]
  (when-not (and (= 1 (:version marker)) (= mode (:mode marker))
                 (= identity (:db-identity marker)))
    (mismatch! dir {:version 1 :mode mode :db-identity identity} marker))
  marker)

(defn- lock! ^FileLock [^FileChannel channel dir shared?]
  (or (try (.tryLock channel 0 Long/MAX_VALUE (boolean shared?))
           (catch OverlappingFileLockException _ nil))
      (throw (ex-info "Environment write protocol is already in use"
                      {:error :txlog/write-protocol-in-use :dir dir
                       :retryable? false}))))

(defn ^:redef acquire-write-protocol-lease!
  "Acquire an exclusive independent-mode or shared compatibility-mode lease.
  Same-JVM handles share a canonical-path holder; mode/identity must match."
  ([dir mode identity] (acquire-write-protocol-lease! dir mode identity nil))
  ([dir mode identity options]
  (when-not (and (modes mode) (some? identity))
    (throw (ex-info "Write protocol requires a supported mode and database identity"
                    {:mode mode :db-identity identity})))
  (let [dir (.getCanonicalPath (io/file dir))]
    (phase! :before-protocol-lease dir)
    (locking holders
      (let [holder
            (if-let [holder (.get holders dir)]
              (do
                (validate-marker! dir mode identity (:marker holder))
                (when-let [guard @(:lifetime holder)]
                  (when-not (= :open (:phase (lifetime/state guard)))
                    (throw (ex-info "Fenced environment cannot be reopened"
                                    {:error :txlog/native-fenced :dir dir
                                     :process-restart-required? true}))))
                (.incrementAndGet ^AtomicInteger (:references holder))
                holder)
              (do
                (Files/createDirectories (.toPath (io/file dir))
                                         (make-array java.nio.file.attribute.FileAttribute 0))
                (let [channel (FileChannel/open
                               (.toPath (io/file dir "write-protocol.lock")) open-options)]
                  (try
                    (let [initialize? (nil? (read-write-protocol-marker dir))
                          shared? (= mode :legacy-writer-v1)
                          initial (lock! channel dir (and shared? (not initialize?)))
                          marker (or (read-write-protocol-marker dir)
                                     (publish-write-protocol-marker!
                                      dir (cond-> {:version 1 :mode mode :db-identity identity
                                                   :generation (str (random-uuid))}
                                            options (assoc :public-options options))))
                          _ (validate-marker! dir mode identity marker)
                          lock (if (and initialize? shared?)
                                 (do (.release initial) (lock! channel dir true))
                                 initial)
                          ;; Recheck after downgrading: no native open is allowed
                          ;; in the gap between exclusive and shared ownership.
                          _ (validate-marker! dir mode identity
                                              (read-write-protocol-marker dir))
                          holder {:dir dir :channel channel :lock lock :marker marker
                                  :references (AtomicInteger. 1) :lifetime (volatile! nil)}]
                      (.put holders dir holder)
                      holder)
                    (catch Throwable e (.close channel) (throw e))))))]
        {:holder holder :released? (AtomicBoolean. false)})))))

(defn assert-native-open!
  "Reject compatibility native opens of an independent environment before any
  native handle, migration or WAL runtime can be created."
  [dir]
  (when-not *independent-native-open?*
    (when (= :kv-independent-v1 (:mode (read-write-protocol-marker dir)))
      (mismatch! dir :legacy-writer-v1 :kv-independent-v1))))

(defn acquire-compat-open-lease!
  "Hold a shared protocol lock for a compatibility native environment. Do not
  publish a mode marker or change compatibility metadata. Check the marker
  under the lock, closing the race with independent runtime creation."
  [dir]
  (when-not *independent-native-open?*
    (let [directory (.toPath (io/file dir))
          _ (Files/createDirectories directory (make-array java.nio.file.attribute.FileAttribute 0))
          channel (FileChannel/open (.toPath (io/file dir "write-protocol.lock")) open-options)]
      (try
        (let [lock (lock! channel dir true)]
          (assert-native-open! dir)
          {:channel channel :lock lock :released? (AtomicBoolean. false)})
        (catch Throwable t (.close channel) (throw t))))))

(defn release-compat-open-lease!
  "Release only after native teardown, or when native creation never started."
  [lease]
  (when (and lease (.compareAndSet ^AtomicBoolean (:released? lease) false true))
    (try (.release ^FileLock (:lock lease))
         (finally (.close ^FileChannel (:channel lease))))))

(defn bind-lifetime!
  "Tie protocol ownership to native lifetime before publishing the environment."
  [lease guard]
  (locking holders
    (let [slot (:lifetime (:holder lease))]
      (when (and @slot (not (identical? @slot guard)))
        (throw (ex-info "Native lifetime already registered for this environment"
                        {:error :txlog/write-protocol-in-use})))
      (vreset! slot guard))))

(defn release!
  "Release only after native teardown, or before any native lifetime is bound.
  A fenced live environment deliberately retains both its registry and OS lock."
  [lease]
  (locking holders
    (let [{:keys [dir channel lock references lifetime]} (:holder lease)]
      (when-let [guard @lifetime]
        (when-not (= :closed (:phase (lifetime/state guard)))
          (throw (ex-info "Native environment has not been closed after quiescence"
                          {:error :txlog/native-not-quiescent :dir dir
                           :process-restart-required? true}))))
      (when (.compareAndSet ^AtomicBoolean (:released? lease) false true)
        (when (zero? (.decrementAndGet ^AtomicInteger references))
          (try (.release ^FileLock lock)
               (finally (.close ^FileChannel channel)))
          (.remove holders dir)))))
  nil)
