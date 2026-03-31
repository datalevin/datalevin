;;
;; Copyright (c) Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2.0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns ^:no-doc datalevin.server.copy
  "Copy helpers extracted from datalevin.server."
  (:require
   [datalevin.constants :as c]
   [datalevin.ha.replication :as drep]
   [datalevin.interface :as i]
   [datalevin.kv :as kv]
   [datalevin.protocol :as p]
   [datalevin.storage :as st]
   [datalevin.util :as u]
   [taoensso.timbre :as log])
  (:import
   [java.nio ByteBuffer]
   [java.nio.channels SelectionKey SocketChannel]
   [java.nio.file Files OpenOption]
   [java.security MessageDigest]
   [java.util.concurrent ConcurrentLinkedQueue]
   [datalevin.storage Store]
   [datalevin.interface ILMDB IStore]))

(defn cleanup-copy-tmp-dir*
  [tf]
  (u/delete-files tf))

(def ^:redef cleanup-copy-tmp-dir-fn*
  (atom cleanup-copy-tmp-dir*))

(defn cleanup-copy-tmp-dir!
  [tf]
  (@cleanup-copy-tmp-dir-fn* tf))

(def ^:redef server-copy-store!
  i/copy)

(def ^:redef open-server-copied-store!
  st/open)

(def ^:redef close-server-copied-store!
  i/close)

(def ^:redef unpin-server-copy-backup-floor!
  kv/txlog-unpin-backup-floor!)

(defn copy-in
  "Continuously read batched data from the client"
  [deps server ^SelectionKey skey]
  (let [state                      (.attachment skey)
        {:keys [read-bf write-bf wire-opts]} @state
        ^java.nio.channels.Selector selector (.selector skey)
        ^SocketChannel ch          (.channel skey)
        data                       (transient [])]
    ;; switch this channel to blocking mode for copy-in
    (.cancel skey)
    (.configureBlocking ch true)
    (try
      (p/write-message-blocking ch write-bf {:type :copy-in-response}
                                wire-opts)
      (.clear ^ByteBuffer read-bf)
      (loop [bf read-bf]
        (let [[msg bf'] (p/receive-ch ch bf wire-opts)]
          (when-not (identical? bf bf') (vswap! state assoc :read-bf bf'))
          (if (map? msg)
            (let [{:keys [type]} msg]
              (case type
                :copy-done :break
                :copy-fail (u/raise "Client error while loading data" {})
                (u/raise "Receive unexpected message while loading data"
                         {:msg msg})))
            (do
              (doseq [d msg] (conj! data d))
              (recur bf')))))
      (let [txs (persistent! data)]
        (log/debug "Copied in" (count txs) "data items")
        txs)
      (finally
        ;; switch back
        (.configureBlocking ch false)
        (.add ^ConcurrentLinkedQueue
              ((:register-queue-fn deps) server)
              [ch SelectionKey/OP_READ state])
        (.wakeup selector)))))

(defn copy-out
  "Continiously write data out to client in batches"
  ([deps ^SelectionKey skey data batch-size]
   (copy-out deps skey data batch-size nil nil))
  ([deps ^SelectionKey skey data batch-size copy-meta]
   (copy-out deps skey data batch-size copy-meta nil))
  ([deps ^SelectionKey skey data batch-size copy-meta response-meta]
   (let [state                          (.attachment skey)
         {:keys [^ByteBuffer write-bf wire-opts]} @state
         ^SocketChannel ch              (.channel skey)
         response                       (cond-> {:type :copy-out-response}
                                          copy-meta
                                          (assoc :copy-meta copy-meta)
                                          (seq response-meta)
                                          (merge response-meta))]
     (locking write-bf
       (p/write-message-blocking ch write-bf response wire-opts)
       (doseq [batch (partition batch-size batch-size nil data)]
         ((:write-message-fn deps) skey batch))
       (p/write-message-blocking ch write-bf {:type :copy-done}
                                 wire-opts))
     (log/debug "Copied out" (count data) "data items"))))

(defn copy-file-out
  "Stream a copied LMDB file to client as raw binary chunks with checksum."
  [deps ^SelectionKey skey path copy-meta]
  (let [chunk-bytes ^long c/+buffer-size+
        response    (cond-> {:type          :copy-out-response
                             :copy-format   :binary-chunks
                             :checksum-algo :sha-256
                             :chunk-bytes   chunk-bytes}
                      copy-meta
                      (assoc :copy-meta copy-meta))
        ^MessageDigest md (MessageDigest/getInstance "SHA-256")
        chunk             (byte-array chunk-bytes)]
    ((:write-message-fn deps) skey response)
    (with-open [in (Files/newInputStream path
                                         (into-array OpenOption []))]
      (loop [written-bytes 0
             chunk-count   0]
        (let [n (.read in chunk)]
          (if (neg? n)
            (let [checksum (u/hexify (.digest md))]
              ((:write-message-fn deps)
               skey
               {:type          :copy-done
                :copy-format   :binary-chunks
                :checksum-algo :sha-256
                :checksum      checksum
                :bytes         written-bytes
                :chunks        chunk-count})
              (log/debug "Copied out" written-bytes "bytes in" chunk-count
                         "chunks"))
            (let [^bytes out-chunk (if (= n chunk-bytes)
                                     chunk
                                     (let [tail (byte-array n)]
                                       (System/arraycopy chunk 0 tail 0 n)
                                       tail))]
              (.update md out-chunk 0 n)
              ((:write-message-fn deps) skey [out-chunk])
              (recur (+ written-bytes n) (inc chunk-count)))))))))

(defn copy-source-kv-store
  [store]
  (cond
    (instance? Store store) (.-lmdb ^Store store)
    (instance? ILMDB store) store
    :else nil))

(defn copy-response-meta
  [db-name store base-meta]
  (let [store-opts (when (instance? IStore store)
                     (i/opts store))
        kv-store   (copy-source-kv-store store)
        kv-opts    (when kv-store
                     (try
                       (i/env-opts kv-store)
                       (catch Exception _
                         nil)))
        stored-db-identity
        (when kv-store
          (try
            (i/get-value kv-store c/opts :db-identity :attr :data)
            (catch Exception _
              nil)))
        snapshot-lsn
        (when kv-store
          (try
            (long (or (i/get-value kv-store c/kv-info
                                   c/wal-snapshot-current-lsn
                                   :keyword :data)
                      0))
            (catch Exception _
              0)))
        payload-lsn
        (when kv-store
          (try
            (long (drep/read-ha-snapshot-payload-lsn {:store store}))
            (catch Exception _
              0)))
        db-identity (or (:db-identity store-opts)
                        (:db-identity kv-opts)
                        stored-db-identity)]
    (cond-> (assoc base-meta :db-name db-name)
      (some? db-identity)
      (assoc :db-identity db-identity)

      (some? snapshot-lsn)
      (assoc :snapshot-last-applied-lsn (long snapshot-lsn))

      (some? payload-lsn)
      (assoc :payload-last-applied-lsn (long payload-lsn)))))
