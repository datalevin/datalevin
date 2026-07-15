;;
;; Copyright (c) Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by the
;; terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns ^:no-doc datalevin.replica
  "Async read-replica helpers for a non-HA primary/replica topology."
  (:require
   [datalevin.client :as cl]
   [datalevin.constants :as c]
   [datalevin.interface :as i]
   [datalevin.kv :as kv]
   [datalevin.remote :as r]
   [datalevin.util :as u]
   [taoensso.timbre :as log])
  (:import
   [datalevin.remote KVStore]
   [java.net InetAddress]
   [java.nio.file Files LinkOption Paths StandardCopyOption]
   [java.util UUID]))

(def default-poll-ms 250)
(def default-report-ms 5000)
(def default-batch-records 256)
(def default-request-timeout-ms 60000)

(defn enabled?
  [opts]
  (true? (:replica/read-only? opts)))

(defn require-source!
  [opts]
  (or (:replica/source opts)
      (u/raise "Replica read-only mode requires :replica/source" {})))

(defn replica-id
  [opts]
  (or (:replica/id opts)
      (str (InetAddress/getLocalHost) ":" (UUID/randomUUID))))

(defn normalized-opts
  [opts]
  (when (enabled? opts)
    (let [batch-records (long (or (:replica/batch-records opts)
                                  default-batch-records))]
      (assoc opts
             :replica/read-only? true
             :replica/source (require-source! opts)
             :replica/id (replica-id opts)
             :replica/poll-ms (long (or (:replica/poll-ms opts)
                                        default-poll-ms))
             :replica/report-ms (long (or (:replica/report-ms opts)
                                          default-report-ms))
             :replica/batch-records
             (if (< batch-records 1) 1 batch-records)))))

(defn local-open-opts
  [opts]
  (if (enabled? opts)
    nil
    opts))

(defn data-file-path
  [dir]
  (Paths/get (str dir u/+separator+ c/data-file-name)
             (into-array String [])))

(defn local-data-exists?
  [dir]
  (Files/exists (data-file-path dir) (into-array LinkOption [])))

(defn source-client-opts
  [opts]
  (merge {:pool-size 1
          :time-out  (long (or (:replica/request-timeout-ms opts)
                               default-request-timeout-ms))}
         (:replica/client-opts opts)))

(defn open-source-kv
  [opts]
  (let [source (require-source! opts)
        client (cl/new-client source (source-client-opts opts))]
    {:client client
     :store  (r/open-kv client source nil)}))

(defn close-source-kv!
  [{:keys [client store]}]
  (try
    (when store
      (i/close-kv store))
    (catch Throwable e
      (log/debug e "Unable to close replica source store")))
  (try
    (when (and client (not (cl/disconnected? client)))
      (cl/disconnect client))
    (catch Throwable e
      (log/debug e "Unable to disconnect replica source client"))))

(defn source-watermarks
  [{:keys [store]}]
  (let [wm (kv/txlog-watermarks store)]
    (when-not (:wal? wm)
      (u/raise "Replica source does not have WAL enabled"
               {:type :replica/source-wal-disabled
                :watermarks wm}))
    wm))

(defn durable-lsn
  [watermarks]
  (long (or (:last-durable-lsn watermarks)
            (:last-applied-lsn watermarks)
            (:last-committed-lsn watermarks)
            0)))

(defn local-applied-lsn
  [store]
  (let [wm (kv/txlog-watermarks store)]
    (long (or (:last-applied-lsn wm)
              (:last-durable-lsn wm)
              (:last-committed-lsn wm)
              0))))

(defn bootstrap-applied-lsn
  [copy-meta]
  (let [candidate (some-> (or (:payload-last-applied-lsn copy-meta)
                              (:txlog-last-applied-lsn copy-meta)
                              (:snapshot-last-applied-lsn copy-meta)
                              (:last-applied-lsn copy-meta))
                          long)
        source-durable (some-> (:source-durable-lsn copy-meta) long)]
    (cond
      (and candidate source-durable)
      (if (< (long candidate) (long source-durable)) candidate source-durable)

      candidate candidate
      source-durable source-durable
      :else nil)))

(defn fetch-records
  [{^KVStore store :store} from-lsn upto-lsn]
  (vec (cl/normal-request (.-client store)
                          :open-tx-log-rows
                          [(.-db-name store) from-lsn upto-lsn]
                          false)))

(defn validate-contiguous-records!
  [records from-lsn upto-lsn]
  (let [expected (range (long from-lsn) (inc (long upto-lsn)))
        actual   (map :lsn records)]
    (when-not (= (seq expected) (seq actual))
      (u/raise "Replica source WAL has a gap"
               {:type :replica/source-wal-gap
                :from-lsn from-lsn
                :upto-lsn upto-lsn
                :actual-lsns (vec actual)}))))

(defn report-floor!
  [{:keys [store]} opts applied-lsn]
  (kv/txlog-update-replica-floor!
   store
   (:replica/id (normalized-opts opts))
   (long applied-lsn)))

(defn bootstrap-copy-if-needed!
  [db-name dir opts]
  (when-let [opts (normalized-opts opts)]
    (if (local-data-exists? dir)
      {:bootstrap? false
       :existing? true}
      (let [stage (str dir ".replica-bootstrap-" (UUID/randomUUID))]
        (try
          (when (Files/exists (Paths/get stage (into-array String []))
                              (into-array LinkOption []))
            (u/delete-files stage))
          (u/create-dirs stage)
          (let [source (open-source-kv opts)]
            (try
              (let [source-wm            (source-watermarks source)
                    source-durable-lsn   (durable-lsn source-wm)
                    source-committed-lsn (long (or (:last-committed-lsn
                                                    source-wm)
                                                   source-durable-lsn))
                    copy-meta            (i/copy (:store source) stage false)]
                (when-not (local-data-exists? stage)
                  (u/raise "Replica bootstrap copy did not produce an LMDB data file"
                           {:db-name db-name
                            :stage stage
                            :copy-meta copy-meta}))
                (when (Files/exists (Paths/get dir (into-array String []))
                                    (into-array LinkOption []))
                  (u/delete-files dir))
                (Files/move (Paths/get stage (into-array String []))
                            (Paths/get dir (into-array String []))
                            (into-array StandardCopyOption
                                        [StandardCopyOption/ATOMIC_MOVE]))
                (assoc copy-meta
                       :bootstrap? true
                       :source (:replica/source opts)
                       :source-durable-lsn source-durable-lsn
                       :source-committed-lsn source-committed-lsn))
              (finally
                (close-source-kv! source))))
          (catch Throwable e
            (try
              (when (Files/exists (Paths/get stage (into-array String []))
                                  (into-array LinkOption []))
                (u/delete-files stage))
              (catch Throwable cleanup-e
                (log/debug cleanup-e
                           "Unable to delete failed replica bootstrap directory"
                           {:path stage})))
            (throw e)))))))
