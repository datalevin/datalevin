;;
;; Copyright (c) Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns ^:no-doc datalevin.txlog.meta
  "Txn-log meta file and shared-watermark helpers."
  (:require
   [clojure.java.io :as io]
   [datalevin.buffer :as bf]
   [datalevin.txlog.codec :as codec]
   [datalevin.txlog.segment :as seg]
   [datalevin.util :as u])
  (:import
   [java.io File]
   [java.nio ByteBuffer]
   [java.nio.channels FileChannel]
   [java.nio.file StandardOpenOption]))

(def ^:private meta-file-name "meta")
(def ^:private meta-lock-file-name "meta.lock")
(def ^:private sync-lock-file-name "sync.lock")
(def ^:private recovery-lock-file-name "recovery.lock")
(def ^:private maintenance-lock-file-name "maintenance.lock")

(defn meta-path [dir] (str dir u/+separator+ meta-file-name))
(defn meta-lock-path [dir] (str dir u/+separator+ meta-lock-file-name))
(defn sync-lock-path [dir] (str dir u/+separator+ sync-lock-file-name))
(defn recovery-lock-path [dir] (str dir u/+separator+ recovery-lock-file-name))

(defn maintenance-lock-path
  [dir]
  (str dir u/+separator+ maintenance-lock-file-name))

(defn- newer-slot
  [slot-a slot-b]
  (cond
    (and slot-a slot-b)
    (if (>= ^long (:revision slot-a) ^long (:revision slot-b))
      slot-a
      slot-b)

    slot-a slot-a
    slot-b slot-b
    :else nil))

(defn- read-meta-slot-at
  [^FileChannel ch ^long offset]
  (let [size (.size ch)]
    (when (<= (+ offset codec/meta-slot-size) size)
      (let [^ByteBuffer slot-bf (bf/get-array-buffer codec/meta-slot-size)]
        (try
          (.clear slot-bf)
          (.limit slot-bf codec/meta-slot-size)
          (seg/read-fully-at! ch offset slot-bf)
          (.flip slot-bf)
          (let [slot-bytes (byte-array codec/meta-slot-size)]
            (.get slot-bf slot-bytes)
            (codec/decode-meta-slot-bytes slot-bytes))
          (catch Exception _
            nil)
          (finally
            (bf/return-array-buffer slot-bf)))))))

(defn read-meta-file
  [^String path]
  (when path
    (let [f (io/file path)]
      (when (.exists f)
        (with-open [^FileChannel ch (FileChannel/open
                                     (.toPath f)
                                     (into-array StandardOpenOption
                                                 [StandardOpenOption/READ]))]
          (let [slot-a (read-meta-slot-at ch 0)
                slot-b (read-meta-slot-at ch codec/meta-slot-size)
                current (newer-slot slot-a slot-b)]
            (when (or slot-a slot-b)
              {:slot-a slot-a
               :slot-b slot-b
               :current current})))))))

(defn write-meta-file!
  ([^String path meta]
   (write-meta-file! path meta {}))
  ([^String path meta {:keys [sync-mode]
                       :or {sync-mode :fdatasync}}]
   (let [revision (long
                   (if-some [rev (:revision meta)]
                     rev
                     (let [existing (read-meta-file path)
                           prev-revision
                           (long (or (get-in existing [:current :revision]) -1))]
                       (inc prev-revision))))
         slot-index (int (bit-and revision 0x1))
         slot-offset (long (* slot-index codec/meta-slot-size))
         payload (assoc meta :revision revision)
         slot-bytes (codec/encode-meta-slot payload)
         f (io/file path)]
     (when-let [^File parent (.getParentFile f)]
       (u/create-dirs (.getPath parent)))
     (with-open [^FileChannel ch (FileChannel/open
                                  (.toPath f)
                                  (into-array StandardOpenOption
                                              [StandardOpenOption/CREATE
                                               StandardOpenOption/READ
                                               StandardOpenOption/WRITE]))]
       (.position ch slot-offset)
       (seg/write-fully! ch (ByteBuffer/wrap slot-bytes))
       (seg/force-channel! ch sync-mode))
     (assoc payload :slot (if (zero? slot-index) :a :b)))))

(defn- base-meta-state
  [state]
  (let [sync-manager (:sync-manager state)
        last-applied-v (:meta-last-applied-lsn state)
        segment-id-v (:segment-id state)
        segment-offset-v (:segment-offset state)
        next-lsn-v (:next-lsn state)]
    {:revision (long (or (some-> (:meta-revision state) deref) -1))
     :last-committed-lsn (max 0 (dec (long (or (some-> next-lsn-v deref) 1))))
     :last-durable-lsn
     (long (or (some-> sync-manager :last-durable-lsn deref) 0))
     :last-applied-lsn (long (or (some-> last-applied-v deref) 0))
     :segment-id (long (or (some-> segment-id-v deref) 1))
     :segment-offset (long (or (some-> segment-offset-v deref) 0))
     :updated-ms (long (or (some-> sync-manager :last-sync-ms deref) 0))}))

(defn- current-meta-state
  [state]
  (merge (base-meta-state state)
         (or (:current (read-meta-file (:meta-path state))) {})))

(defn- next-meta-state
  [current f]
  (-> (f current)
      (assoc :revision (inc (long (or (:revision current) -1)))
             :updated-ms (System/currentTimeMillis))))

(defn- segment-created-ms-from-file
  [^String path now-ms]
  (let [modified (.lastModified (io/file path))]
    (if (pos? modified)
      (long modified)
      (long now-ms))))

(defn- ensure-runtime-segment-channel!
  [state ^long target-id ^String target-path]
  (let [append-lock (or (:append-lock state) state)
        segment-id-v (:segment-id state)
        segment-channel-v (:segment-channel state)]
    (locking append-lock
      (let [current-id (long (or (some-> segment-id-v deref) 0))
            current-ch (some-> segment-channel-v deref)]
        (if (and current-ch (= current-id target-id))
          current-ch
          (let [next-ch (seg/open-segment-channel target-path)]
            (try
              (when-let [old-ch @segment-channel-v]
                (when-not (identical? old-ch next-ch)
                  (try
                    (.close ^FileChannel old-ch)
                    (catch Exception _))))
              (vreset! segment-id-v target-id)
              (vreset! segment-channel-v next-ch)
              next-ch
              (catch Exception e
                (try
                  (.close ^FileChannel next-ch)
                  (catch Exception _))
                (throw e)))))))))

(defn- apply-shared-watermarks!
  [sync-manager committed-lsn durable-lsn updated-ms]
  (when sync-manager
    (let [committed* (max 0 (long (or committed-lsn 0)))
          durable* (max 0 (min committed* (long (or durable-lsn 0))))
          updated-ms* (long (or updated-ms 0))]
      (locking (:monitor sync-manager)
        (vreset! (:last-appended-lsn sync-manager) committed*)
        (vreset! (:last-durable-lsn sync-manager) durable*)
        (when (pos? updated-ms*)
          (vreset! (:last-sync-ms sync-manager)
                   (max updated-ms*
                        (long @(:last-sync-ms sync-manager)))))
        (vreset! (:unsynced-count sync-manager)
                 (max 0 (- committed* durable*)))
        (when (<= committed* durable*)
          (vreset! (:sync-requested? sync-manager) false)
          (vreset! (:sync-request-reason sync-manager) nil))
        (vreset! (:sync-in-progress? sync-manager) false)
        (vreset! (:healthy? sync-manager) true)
        (vreset! (:failure sync-manager) nil)
        (.notifyAll ^Object (:monitor sync-manager))))))

(defn refresh-shared-state!
  [state]
  (let [base-state (base-meta-state state)
        meta-file-state (or (:current (read-meta-file (:meta-path state))) {})
        meta-state (merge base-state meta-file-state)
        dir (:dir state)
        newest-segment-id (long (or (:id (peek (seg/segment-files dir))) 1))
        current-segment-id (long (or (:segment-id base-state) 1))
        current-offset (long (or (:segment-offset base-state) 0))
        current-revision (long (or (:revision base-state) -1))
        meta-revision (long (or (:revision meta-file-state) -1))
        ^FileChannel current-ch (some-> (:segment-channel state) deref)
        synced-runtime?
        (and current-ch
             (= newest-segment-id current-segment-id)
             (<= meta-revision current-revision)
             (try
               (= (long (.size current-ch)) current-offset)
               (catch Exception _
                 false)))]
    (if synced-runtime?
      base-state
      (let [meta-segment-id (long (or (:segment-id meta-state) 1))
            target-segment-id (long (max 1 meta-segment-id newest-segment-id))
            target-path (seg/activate-next-segment! dir target-segment-id)
            target-scan (seg/truncate-partial-tail!
                         target-path {:allow-preallocated-tail? true})
            scan-end-offset (long (seg/segment-end-offset target-scan))
            meta-segment-offset (long (or (:segment-offset meta-state) 0))
            ;; Meta can be ahead of the actual segment bytes after snapshot
            ;; restore or other recovery fallback paths. Never resume appends at
            ;; the higher meta offset or the next write will create a zero-filled
            ;; hole that later segment scans report as corruption.
            target-offset (long scan-end-offset)
            target-last-lsn (long (or (some-> (:records target-scan) peek :lsn) 0))
            committed-lsn (max (long (or (:last-committed-lsn meta-state) 0))
                               target-last-lsn)
            durable-lsn (min committed-lsn
                             (max 0 (long (or (:last-durable-lsn meta-state) 0))))
            applied-lsn (min committed-lsn
                             (max 0 (long (or (:last-applied-lsn meta-state) 0))))
            updated-ms (long (or (:updated-ms meta-state) 0))
            revision (long (or (:revision meta-state) -1))
            now-ms (System/currentTimeMillis)]
        (ensure-runtime-segment-channel! state target-segment-id target-path)
        (when-let [segment-offset-v (:segment-offset state)]
          (vreset! segment-offset-v target-offset))
        (when-let [segment-created-ms-v (:segment-created-ms state)]
          (vreset! segment-created-ms-v
                   (segment-created-ms-from-file target-path now-ms)))
        (when-let [next-lsn-v (:next-lsn state)]
          (vreset! next-lsn-v (inc committed-lsn)))
        (when-let [meta-revision-v (:meta-revision state)]
          (vreset! meta-revision-v revision))
        (when-let [last-applied-v (:meta-last-applied-lsn state)]
          (vreset! last-applied-v applied-lsn))
        (apply-shared-watermarks! (:sync-manager state)
                                  committed-lsn
                                  durable-lsn
                                  updated-ms)
        {:revision revision
         :last-committed-lsn committed-lsn
         :last-durable-lsn durable-lsn
         :last-applied-lsn applied-lsn
         :segment-id target-segment-id
         :segment-offset target-offset
         :updated-ms updated-ms}))))

(defn refresh-shared-watermarks!
  [state]
  (let [meta-state (current-meta-state state)
        committed-lsn (max 0 (long (or (:last-committed-lsn meta-state) 0)))
        durable-lsn (min committed-lsn
                         (max 0 (long (or (:last-durable-lsn meta-state) 0))))
        applied-lsn (min committed-lsn
                         (max 0 (long (or (:last-applied-lsn meta-state) 0))))
        updated-ms (long (or (:updated-ms meta-state) 0))
        revision (long (or (:revision meta-state) -1))]
    (when-let [meta-revision-v (:meta-revision state)]
      (vreset! meta-revision-v revision))
    (when-let [last-applied-v (:meta-last-applied-lsn state)]
      (vreset! last-applied-v applied-lsn))
    (apply-shared-watermarks! (:sync-manager state)
                              committed-lsn
                              durable-lsn
                              updated-ms)
    {:revision revision
     :last-committed-lsn committed-lsn
     :last-durable-lsn durable-lsn
     :last-applied-lsn applied-lsn
     :updated-ms updated-ms}))

(defn- update-shared-meta!
  [state f]
  (seg/with-file-lock
    (:meta-lock-path state)
    (fn []
      (let [current (current-meta-state state)
            next-state (next-meta-state current f)
            written (write-meta-file! (:meta-path state)
                                      next-state
                                      {:sync-mode :none})]
        (when-let [meta-revision-v (:meta-revision state)]
          (vreset! meta-revision-v (long (:revision written))))
        (when-let [last-applied-v (:meta-last-applied-lsn state)]
          (vreset! last-applied-v (long (:last-applied-lsn written))))
        (apply-shared-watermarks! (:sync-manager state)
                                  (:last-committed-lsn written)
                                  (:last-durable-lsn written)
                                  (:updated-ms written))
        written))))

(defn publish-meta-append!
  [state {:keys [lsn segment-id offset]}]
  (update-shared-meta!
   state
   (fn [current]
     (assoc current
            :last-committed-lsn (max (long (or (:last-committed-lsn current) 0))
                                     (long lsn))
            :segment-id (long segment-id)
            :segment-offset (long offset)))))

(defn publish-meta-commit!
  [state {:keys [lsn segment-id offset synced?]}]
  (update-shared-meta!
   state
   (fn [current]
     (cond-> (assoc current
                    :last-committed-lsn
                    (max (long (or (:last-committed-lsn current) 0))
                         (long lsn))
                    :last-applied-lsn
                    (max (long (or (:last-applied-lsn current) 0))
                         (long lsn))
                    :segment-id (long segment-id)
                    :segment-offset (long offset))
       synced?
       (assoc :last-durable-lsn
              (max (long (or (:last-durable-lsn current) 0))
                   (long lsn)))))))

(defn publish-meta-durable!
  [state target-lsn]
  (update-shared-meta!
   state
   (fn [current]
     (assoc current
            :last-durable-lsn
            (max (long (or (:last-durable-lsn current) 0))
                 (long target-lsn))))))

(defn try-with-maintenance-lock
  [state f]
  (when-let [lock-state (seg/try-acquire-file-lock! (:maintenance-lock-path state))]
    (try
      (f)
      (finally
        (seg/release-file-lock! lock-state)))))

(defn with-recovery-lock
  [state f]
  (seg/with-file-lock (:recovery-lock-path state) f))

(defn note-gc-deleted-bytes!
  [state deleted-bytes]
  (when-let [total-bytes-v (:retention-total-bytes state)]
    (let [remaining (- ^long @total-bytes-v ^long (max 0 (long deleted-bytes)))]
      (vreset! total-bytes-v (max 0 remaining)))))
