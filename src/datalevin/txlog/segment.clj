;;
;; Copyright (c) Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns ^:no-doc datalevin.txlog.segment
  "Txn-log segment and local file I/O helpers."
  (:require
   [clojure.java.io :as io]
   [datalevin.buffer :as bf]
   [datalevin.txlog.codec :as codec]
   [datalevin.util :as u :refer [raise]])
  (:import
   [datalevin.io PosixFsync]
   [java.io File]
   [java.nio ByteBuffer]
   [java.nio.channels FileChannel FileLock OverlappingFileLockException]
   [java.nio.file Files Path StandardCopyOption StandardOpenOption
    AtomicMoveNotSupportedException]
   [java.util Arrays]
   [java.util.concurrent.locks ReentrantLock]))

(def ^:private segment-pattern #"^segment-(\d{16})\.wal$")
(def ^:private prepared-segment-pattern #"^segment-(\d{16})\.wal\.tmp$")

(def ^:private ^"[Ljava.nio.file.StandardOpenOption;"
  open-segment-read-options
  (into-array StandardOpenOption [StandardOpenOption/READ]))

(def ^:private ^"[Ljava.nio.file.StandardOpenOption;"
  open-segment-create-read-write-options
  (into-array StandardOpenOption
              [StandardOpenOption/CREATE
               StandardOpenOption/READ
               StandardOpenOption/WRITE]))

(def ^:private ^"[Ljava.nio.file.StandardOpenOption;"
  open-lock-create-write-options
  (into-array StandardOpenOption
              [StandardOpenOption/CREATE
               StandardOpenOption/WRITE]))

(def ^:private sync-mode-values #{:fsync :fdatasync :extra :none})

(def ^:private ^ThreadLocal tl-record-header-buffer
  (ThreadLocal/withInitial
   (reify java.util.function.Supplier
     (get [_]
       (codec/big-endian-buffer!
        (ByteBuffer/allocate codec/record-header-size))))))

(def ^:private ^ThreadLocal tl-record-write-buffers
  (ThreadLocal/withInitial
   (reify java.util.function.Supplier
     (get [_]
       (make-array ByteBuffer 2)))))

(def ^:private scan-segment-concurrent-shrink-retries 4)

(defn segment-file-name [segment-id] (format "segment-%016d.wal" segment-id))

(defn segment-path
  [dir segment-id]
  (str dir u/+separator+ (segment-file-name segment-id)))

(defn prepared-segment-file-name
  [segment-id]
  (str (segment-file-name segment-id) ".tmp"))

(defn prepared-segment-path
  [dir segment-id]
  (str dir u/+separator+ (prepared-segment-file-name segment-id)))

(defn- open-lock-channel
  [^String path]
  (FileChannel/open
   (.toPath (io/file path))
   open-lock-create-write-options))

(defn release-file-lock!
  [{:keys [lock channel]}]
  (when lock
    (try
      (.release ^FileLock lock)
      (catch Exception _)))
  (when channel
    (try
      (.close ^FileChannel channel)
      (catch Exception _))))

(defn try-acquire-file-lock!
  [^String path]
  (let [^FileChannel ch (open-lock-channel path)]
    (try
      (if-let [lock (try
                      (.tryLock ch)
                      (catch OverlappingFileLockException _
                        nil))]
        {:channel ch
         :lock lock
         :path path}
        (do
          (.close ch)
          nil))
      (catch Exception e
        (try
          (.close ch)
          (catch Exception _))
        (throw e)))))

(defn with-file-lock
  [^String path f]
  (loop []
    (if-let [lock-state (try-acquire-file-lock! path)]
      (try
        (f)
        (finally
          (release-file-lock! lock-state)))
      (do
        (Thread/sleep 2)
        (recur)))))

(defn parse-segment-id
  [file-name]
  (when-let [[_ sid] (re-matches segment-pattern file-name)]
    (Long/parseLong sid)))

(defn parse-prepared-segment-id
  [file-name]
  (when-let [[_ sid] (re-matches prepared-segment-pattern file-name)]
    (Long/parseLong sid)))

(defn segment-files
  [dir]
  (->> (or (u/list-files dir) [])
       (keep (fn [^File f]
               (when-let [sid (parse-segment-id (.getName f))]
                 {:id sid :file f})))
       (sort-by :id)
       vec))

(defn prepared-segment-files
  [dir]
  (->> (or (u/list-files dir) [])
       (keep (fn [^File f]
               (when-let [sid (parse-prepared-segment-id (.getName f))]
                 {:id sid :file f})))
       (sort-by :id)
       vec))

(defn write-fully!
  [^FileChannel ch ^ByteBuffer bf]
  (while (.hasRemaining bf)
    (let [n (.write ch bf)]
      (when-not (pos? n)
        (raise "Unable to progress while writing txn-log data"
               {:remaining (.remaining bf)})))))

(defn- buffers-remaining?
  [^"[Ljava.nio.ByteBuffer;" bufs]
  (let [n (alength bufs)]
    (loop [i (int 0)]
      (cond
        (>= i n) false
        (.hasRemaining ^ByteBuffer (aget bufs i)) true
        :else (recur (unchecked-inc-int i))))))

(defn- total-buffers-remaining
  ^long [^"[Ljava.nio.ByteBuffer;" bufs]
  (reduce (fn [^long acc ^ByteBuffer bf]
            (+ acc (long (.remaining bf))))
          0
          bufs))

(defn- write-fully-buffers!
  [^FileChannel ch ^"[Ljava.nio.ByteBuffer;" bufs]
  (while (buffers-remaining? bufs)
    (let [n (.write ch bufs)]
      (when-not (pos? n)
        (raise "Unable to progress while writing txn-log data"
               {:remaining (total-buffers-remaining bufs)})))))

(defn- write-fully-record!
  [^FileChannel ch ^ByteBuffer header-bf ^bytes body]
  (let [^"[Ljava.nio.ByteBuffer;" bufs (.get tl-record-write-buffers)]
    (aset bufs 0 header-bf)
    (aset bufs 1 (ByteBuffer/wrap body))
    (try
      (write-fully-buffers! ch bufs)
      (finally
        (aset bufs 0 nil)
        (aset bufs 1 nil)))))

(defn force-channel!
  "Force channel to disk according to sync mode:
  - `:fsync` -> fsync (data + metadata)
  - `:fdatasync` -> fdatasync-like (macOS uses fsync path; others use force(false))
  - `:extra` -> extra durable full sync (e.g. F_FULLFSYNC on macOS)
  - `:none` -> no force"
  ([^FileChannel ch] (force-channel! ch :fdatasync))
  ([^FileChannel ch sync-mode]
   (when-not (sync-mode-values sync-mode)
     (raise "Unsupported txn-log sync mode"
            {:sync-mode sync-mode :allowed sync-mode-values}))
   (case sync-mode
     :none      nil
     :fdatasync (PosixFsync/fdatasync ch)
     :fsync     (PosixFsync/fsync ch)
     :extra     (PosixFsync/fullsync ch))
   sync-mode))

(defn read-fully-at!
  ^ByteBuffer [^FileChannel ch ^long pos ^ByteBuffer bf]
  (loop [p pos]
    (if (.hasRemaining bf)
      (let [n (.read ch bf p)]
        (when (neg? n)
          (raise "Unexpected EOF reading txn-log segment"
                 {:type :txlog/unexpected-eof
                  :position p
                  :remaining (.remaining bf)}))
        (when (zero? n)
          (raise "Unable to progress while reading txn-log segment"
                 {:type :txlog/read-stalled
                  :position p
                  :remaining (.remaining bf)}))
        (recur (+ p n)))
      bf)))

(defn- txlog-unexpected-eof?
  [^Throwable e]
  (loop [t e]
    (cond
      (nil? t) false
      (= :txlog/unexpected-eof (:type (ex-data t))) true
      :else (recur (.getCause t)))))

(defn read-fully-at
  [^FileChannel ch ^long pos ^long len]
  (let [out (byte-array (int len))
        bf  (ByteBuffer/wrap out)]
    (read-fully-at! ch pos bf)
    out))

(defn- buffer-all-zero?
  [^ByteBuffer bf]
  (let [start (.position bf)
        end   (.limit bf)]
    (loop [i start]
      (if (< i end)
        (if (zero? (int (.get bf i)))
          (recur (unchecked-inc-int i))
          false)
        true))))

(defn- tail-all-zero?
  ([^FileChannel ch ^long offset ^long size]
   (let [^ByteBuffer read-bf (bf/get-array-buffer 8192)]
     (try
       (tail-all-zero? ch offset size read-bf)
       (finally
         (bf/return-array-buffer read-bf)))))
  ([^FileChannel ch ^long offset ^long size ^ByteBuffer read-bf]
   (loop [pos offset]
     (if (>= pos size)
       true
       (let [len (int (min 8192 (- size pos)))]
         (.clear read-bf)
         (.limit read-bf len)
         (read-fully-at! ch pos read-bf)
         (.flip read-bf)
         (if (buffer-all-zero? read-bf)
           (recur (+ pos len))
           false))))))

(defn- scan-segment-once
  [^String path
   ^FileChannel ch
   size
   allow-preallocated-tail?
   collect-records?
   on-record]
  (let [size             (long size)
        ^ByteBuffer read-bf (codec/big-endian-buffer! (bf/get-array-buffer 8192))]
    (try
      (loop [offset  0
             records (when collect-records? [])]
        (let [remaining (- size offset)]
          (cond
            (zero? remaining)
            {:records       records
             :valid-end     offset
             :size          size
             :partial-tail? false}

            (< remaining codec/record-header-size)
            {:records       records
             :valid-end     offset
             :size          size
             :partial-tail? true}

            :else
            (let [_         (doto read-bf
                              (.clear)
                              (.limit codec/record-header-size))
                  _         (read-fully-at! ch offset read-bf)
                  _         (.flip read-bf)
                  header-map
                  (try
                    (codec/decode-header-buffer read-bf offset)
                    (catch Exception e
                      (if (and allow-preallocated-tail?
                               (tail-all-zero? ch offset size read-bf))
                        :txlog/preallocated-tail
                        (throw (ex-info "Txn-log segment corruption"
                                        {:type   :txlog/corrupt
                                         :path   path
                                         :offset offset}
                                        e)))))
                  body-len  (when (map? header-map)
                              (:body-len header-map))
                  total-len (when body-len
                              (+ ^long codec/record-header-size
                                 ^long body-len))]
              (cond
                (identical? header-map :txlog/preallocated-tail)
                {:records            records
                 :valid-end          offset
                 :size               size
                 :partial-tail?      true
                 :preallocated-tail? true}

                (> ^long total-len ^long remaining)
                {:records       records
                 :valid-end     offset
                 :size          size
                 :partial-tail? true}

                :else
                (let [record
                      (try
                        (let [{:keys [major flags body-len checksum]}
                              header-map
                              body (read-fully-at
                                    ch
                                    (+ offset codec/record-header-size)
                                    body-len)]
                          (when-not (= checksum
                                       (long (bit-and 0xffffffff
                                                      (long (codec/crc32c body)))))
                            (raise "Txn-log record checksum mismatch"
                                   {:offset   offset
                                    :expected checksum}))
                          {:major       major
                           :flags       flags
                           :compressed? (pos? (long (bit-and
                                                     (long flags)
                                                     codec/compressed-flag)))
                           :body-len    body-len
                           :checksum    checksum
                           :body        body})
                        (catch Exception e
                          (throw (ex-info "Txn-log segment corruption"
                                          {:type   :txlog/corrupt
                                           :path   path
                                           :offset offset}
                                          e))))
                      next-offset (long (+ offset (long total-len)))
                      record*     (assoc record
                                         :offset offset
                                         :next-offset next-offset)]
                  (when on-record
                    (on-record record*))
                  (recur next-offset
                         (if collect-records?
                           (conj records record*)
                           records))))))))
      (finally
        (bf/return-array-buffer read-bf)))))

(defn scan-segment
  "Scan a txn-log segment."
  ([^String path] (scan-segment path {}))
  ([^String path {:keys [allow-preallocated-tail? collect-records?
                         max-offset on-record]
                  :or   {allow-preallocated-tail? false
                         collect-records?         true}}]
   (let [f (io/file path)]
     (loop [attempt 0]
       (let [{:keys [value error retry?]}
             (with-open [^FileChannel ch (FileChannel/open
                                          (.toPath f)
                                          open-segment-read-options)]
               (let [file-size (.size ch)
                     size      (if (some? max-offset)
                                 (long (min ^long file-size
                                            (long (max 0
                                                       (long max-offset)))))
                                 (long file-size))]
                 (try
                   {:value (scan-segment-once
                            path ch size allow-preallocated-tail?
                            collect-records? on-record)}
                   (catch Exception e
                     (let [current-size (long (.length f))
                           size-limit   (long size)]
                       (if (and (neg? (Long/compare
                                       (long attempt)
                                       (long scan-segment-concurrent-shrink-retries)))
                                (txlog-unexpected-eof? e)
                                (neg? (Long/compare current-size
                                                    size-limit)))
                         {:retry? true}
                         {:error e}))))))]
         (cond
           retry? (recur (unchecked-inc (long attempt)))
           error (throw error)
           :else value))))))

(defn truncate-partial-tail!
  ([^String path] (truncate-partial-tail! path {}))
  ([^String path scan-opts]
   (let [{:keys [valid-end size partial-tail?] :as scan}
         (scan-segment path scan-opts)]
     (if partial-tail?
       (with-open [^FileChannel ch (FileChannel/open
                                    (.toPath (io/file path))
                                    (into-array StandardOpenOption
                                                [StandardOpenOption/WRITE]))]
         (.truncate ch (long valid-end))
         (assoc scan
                :size valid-end
                :valid-end valid-end
                :partial-tail? false
                :preallocated-tail? false
                :truncated? true
                :old-size size
                :new-size valid-end
                :dropped-bytes (long (- (long size)
                                        (long valid-end)))))
       (assoc scan
              :truncated? false
              :old-size size
              :new-size valid-end
              :dropped-bytes 0)))))

(defn segment-end-offset
  [scan]
  (long
   (if (:truncated? scan)
     (or (:new-size scan) 0)
     (or (:valid-end scan) 0))))

(defn open-segment-channel
  [^String path]
  (FileChannel/open
   (.toPath (io/file path))
   open-segment-create-read-write-options))

(defn append-record-at!
  ([^FileChannel ch ^long offset ^bytes body]
   (append-record-at! ch offset body {}))
  ([^FileChannel ch ^long offset ^bytes body
    {:keys [compressed?] :or {compressed? false}}]
   (let [body-len        (codec/checked-record-body-len body)
         checksum        (codec/record-body-checksum body)
         total-size      (+ codec/record-header-size (long body-len))
         ^ByteBuffer hdr (codec/write-record-header!
                          (.get tl-record-header-buffer)
                          body-len
                          (boolean compressed?)
                          checksum)]
     (.position ch offset)
     (if (pos? (long body-len))
       (write-fully-record! ch hdr body)
       (write-fully! ch hdr))
     {:offset offset :size total-size :checksum checksum})))

(defn append-record!
  ([^FileChannel ch ^bytes body]
   (append-record! ch body {}))
  ([^FileChannel ch ^bytes body opts]
   (let [offset (.size ch)]
     (append-record-at! ch offset body opts))))

(defn force-segment!
  [state ^FileChannel ch sync-mode]
  (force-channel! ch sync-mode))

(defn prepare-segment!
  "Create and preallocate a segment at `tmp-path`."
  [^String tmp-path ^long bytes]
  (let [p (.toPath (io/file tmp-path))]
    (with-open [^FileChannel ch
                (FileChannel/open
                 p
                 (into-array StandardOpenOption
                             [StandardOpenOption/CREATE
                              StandardOpenOption/TRUNCATE_EXISTING
                              StandardOpenOption/READ
                              StandardOpenOption/WRITE]))]
      (when (pos? bytes)
        (.position ch (dec bytes))
        (.write ch (ByteBuffer/wrap (byte-array [(byte 0x00)]))))
      (.force ch true))
    tmp-path))

(defn activate-prepared-segment!
  "Atomically (when possible) move `tmp-path` into final segment path."
  [^String tmp-path ^String final-path]
  (let [^Path src (.toPath (io/file tmp-path))
        ^Path dst (.toPath (io/file final-path))]
    (try
      (Files/move src dst
                  (into-array StandardCopyOption
                              [StandardCopyOption/REPLACE_EXISTING
                               StandardCopyOption/ATOMIC_MOVE]))
      (catch AtomicMoveNotSupportedException _
        (Files/move src dst
                    (into-array StandardCopyOption
                                [StandardCopyOption/REPLACE_EXISTING]))))
    final-path))

(defn prepare-next-segment!
  [^String dir ^long segment-id ^long bytes]
  (prepare-segment! (prepared-segment-path dir segment-id) bytes))

(defn activate-next-segment!
  "Activate preallocated next segment if present, else create empty segment."
  [^String dir ^long segment-id]
  (let [tmp-path   (prepared-segment-path dir segment-id)
        final-path (segment-path dir segment-id)
        tmp-file   (io/file tmp-path)
        final-file (io/file final-path)]
    (cond
      (.exists final-file) final-path
      (.exists tmp-file)   (activate-prepared-segment! tmp-path final-path)
      :else
      (do
        (with-open [^FileChannel _ (open-segment-channel final-path)])
        final-path))))

(defn preallocation-enabled-state?
  [state]
  (and (:segment-prealloc? state)
       (contains? #{:native} (:segment-prealloc-mode state))
       (pos? (long (or (:segment-prealloc-bytes state) 0)))))

(defn inc-volatile-long!
  [v]
  (when (some? v)
    (vswap! v u/long-inc)
    #_(vreset! v (u/long-inc @v))))

(defn add-volatile-long!
  [v delta]
  (when (some? v)
    (vreset! v (+ ^long @v ^long (long (or delta 0))))))

(defn ensure-next-segment-prepared!
  [state]
  (when (preallocation-enabled-state? state)
    (let [next-id    (inc ^long @(:segment-id state))
          dir        (:dir state)
          tmp-file   (io/file (prepared-segment-path dir next-id))
          final-file (io/file (segment-path dir next-id))]
      (cond
        (.exists final-file) false
        (.exists tmp-file)   true
        :else
        (try
          (prepare-next-segment! dir next-id
                                 (long (:segment-prealloc-bytes state)))
          (inc-volatile-long! (:segment-prealloc-success-count state))
          true
          (catch Exception _
            (inc-volatile-long! (:segment-prealloc-failure-count state))
            false))))))

(defn activated-segment-offset
  [^String path preserve-preallocated-tail?]
  (let [scan (scan-segment path {:allow-preallocated-tail? true
                                 :collect-records?         false})]
    (cond
      (and preserve-preallocated-tail?
           (:preallocated-tail? scan))
      (:valid-end scan)

      (:partial-tail? scan)
      (segment-end-offset
       (truncate-partial-tail! path {:allow-preallocated-tail? true}))

      :else
      (:valid-end scan))))
