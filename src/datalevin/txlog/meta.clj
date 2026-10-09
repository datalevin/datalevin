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
  "Txn-log metadata publication helpers."
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
         f (io/file path)
         created? (not (.exists f))]
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
     (when created?
       (seg/force-parent-directory! path))
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

(defn- next-meta-state
  [current f]
  (-> (f current)
      (assoc :revision (inc (long (or (:revision current) -1)))
             :updated-ms (System/currentTimeMillis))))

(defn- update-meta!
  [state f]
  (locking (or (:append-lock state) state)
    (let [next-state (next-meta-state (base-meta-state state) f)
          written (write-meta-file! (:meta-path state) next-state {:sync-mode :none})]
      (when-let [revision (:meta-revision state)]
        (vreset! revision (long (:revision written))))
      (when-let [applied (:meta-last-applied-lsn state)]
        (vreset! applied (long (:last-applied-lsn written))))
      written)))

(defn publish-meta-append!
  [state {:keys [lsn segment-id offset]}]
  (update-meta!
   state
   (fn [current]
     (let [committed-lsn (max (long (or (:last-committed-lsn current) 0))
                              (long lsn))
           durable-lsn (max (long (or (:last-durable-lsn current) 0))
                            (long (or (some-> state
                                              :sync-manager
                                              :last-durable-lsn
                                              deref)
                                      0)))]
       (assoc current
              :last-committed-lsn committed-lsn
              :last-durable-lsn (min committed-lsn durable-lsn)
              :segment-id (long segment-id)
              :segment-offset (long offset))))))

(defn- commit-segment-end-offset
  [{:keys [offset size payload-bytes]}]
  (let [offset (long (or offset 0))]
    (cond
      (some? size)
      (+ offset (long size))

      (some? payload-bytes)
      (+ offset codec/record-header-size (long payload-bytes))

      :else
      offset)))

(defn publish-meta-commit!
  [state {:keys [lsn segment-id synced?] :as record}]
  (update-meta!
   state
   (fn [current]
     (let [segment-id (long segment-id)
           current-segment-id (long (or (:segment-id current) segment-id))
           current-segment-offset (long (or (:segment-offset current) 0))
           commit-end-offset (long (commit-segment-end-offset record))
           segment-offset (if (= current-segment-id segment-id)
                            (if (> current-segment-offset commit-end-offset)
                              current-segment-offset
                              commit-end-offset)
                            commit-end-offset)]
       (cond-> (assoc current
                      :last-committed-lsn
                      (max (long (or (:last-committed-lsn current) 0))
                           (long lsn))
                      :last-applied-lsn
                      (max (long (or (:last-applied-lsn current) 0))
                           (long lsn))
                      :segment-id segment-id
                      :segment-offset segment-offset)
       synced?
       (assoc :last-durable-lsn
              (max (long (or (:last-durable-lsn current) 0))
                   (long lsn))))))))

(defn publish-meta-durable!
  [state target-lsn]
  (update-meta!
   state
   (fn [current]
     (assoc current
            :last-durable-lsn
            (max (long (or (:last-durable-lsn current) 0))
                 (long target-lsn))))))

(defn publish-meta-current!
  [state]
  (update-meta! state identity))

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
