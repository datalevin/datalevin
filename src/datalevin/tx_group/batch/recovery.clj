;; Copyright (c) Huahai Yang. All rights reserved.
;; Distributed under the Eclipse Public License 2.0.
(ns ^:no-doc datalevin.tx-group.batch.recovery
  "Private KV recovery: verified immutable snapshot, bounded streaming WAL
  replay, then native-file installation. The old overlay is never read."
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [datalevin.constants :as c]
            [datalevin.interface :as i]
            [datalevin.kv :as kv]
            [datalevin.lmdb :as l]
            [datalevin.tx-group.batch :as batch]
            [datalevin.tx-group.batch.charge :as charge]
            [datalevin.txlog :as wal]
            [datalevin.txlog.codec :as codec]
            [datalevin.txlog.segment :as segment]
            [datalevin.util :as u])
  (:import [java.io File FileInputStream FileOutputStream]
           [java.nio.charset StandardCharsets]
           [java.nio.channels FileChannel]
           [java.nio.file Files CopyOption StandardCopyOption StandardOpenOption]
           [java.util.zip CRC32C]))

(def ^:private manifest-name "snapshot.edn")
(def ^:private fixed-workspace (* 12 1024 1024))
(def ^:private copy-options
  (into-array CopyOption [StandardCopyOption/ATOMIC_MOVE
                          StandardCopyOption/REPLACE_EXISTING]))

(defn ^:redef phase!
  "Snapshot/restore fault and progress seam; disabled calls allocate nothing."
  [_event _context] nil)

(defn root [opts]
  (str (or (:snapshot-dir opts) (str (:dir opts) "/snapshots"))
       "/independent-v1"))

(defn wal-dir [opts]
  (or (:wal-dir opts) (str (:dir opts) "/txlog")))

(defn native-options
  "Use writemap/nosync by default, respecting an explicit native flag override.
  Recovery itself never recursively opens a WAL runtime or snapshot scheduler."
  [opts]
  (assoc opts :wal? false :snapshot-scheduler? false
         :flags (if (contains? opts :flags)
                  (:flags opts)
                  (if (:wal? opts)
                    (conj c/default-env-flags :writemap :nosync)
                    c/default-env-flags))))

(defn dbis [opts]
  ;; Private M0 has a fixed catalog. Additional DBIs are declared before the
  ;; durable C=0 baseline; catalog mutation is a later operation-coverage step.
  (get opts :dbis {"data" {}}))

(defn retention-limit ^long [opts]
  (let [value (get opts :wal-retained-max-bytes
                   (* 2 (long (get opts :wal-retention-bytes c/*wal-retention-bytes*))))]
    (when-not (pos-int? value)
      (throw (ex-info "WAL hard retention limit must be positive"
                      {:error :txlog/write-protocol-limits
                       :wal-retained-max-bytes value})))
    (long value)))

(defn validate-limits!
  "The portable replay proof must cover every batch admitted by this opener,
  including configured limits. Reject an unrecoverable configuration before open."
  [opts]
  (let [limits (charge/resolve-limits opts)
        required (+ fixed-workspace (* 5 (long (:batch-max-bytes limits))))
        retained (retention-limit opts)
        segment-size (long (wal/segment-max-bytes opts))]
    ;; Two snapshot anchors can pin two full segments. A lower hard limit can
    ;; stop writes before rollover and make reclamation impossible indefinitely.
    (when (< retained (* 2 segment-size))
      (throw (ex-info "Retention limit cannot support two snapshot anchors"
                      {:error :txlog/write-protocol-limits
                       :wal-retained-max-bytes retained
                       :min-retained-max-bytes (* 2 segment-size)})))
    (when (> required (long (:byte-budget limits)))
      (throw (ex-info "Batch cap cannot fit the portable recovery workspace"
                      {:error :txlog/write-protocol-limits
                       :required-recovery-bytes required
                       :wal-pending-max-bytes (:byte-budget limits)})))))

(defn check-capacity!
  "Conservative physical capacity check before either branch starts. WAL's
  retained-byte counter covers closed files and logical active bytes; reserve
  both active/next preallocated capacities and this entire record's growth.
  No filesystem scan or per-row allocation on the production hot path."
  [opts state sealed]
  (let [limit (retention-limit opts)
        slack (if (:segment-prealloc? state)
                (* 2 (long (:segment-prealloc-bytes state))) 0)
        retained (+ (long @(:retention-total-bytes state)) slack)
        max-growth (+ (long (get opts :write-batch-max-bytes
                                 (:batch-max-bytes charge/default-limits)))
                      (long codec/record-header-size))]
    ;; Far from pressure, the bounded batch allowance is already a conservative
    ;; upper bound. Traverse bodies only when that bound could cross the cap.
    (when (> (+ retained max-growth) limit)
      (let [bodies (batch/wal-bodies sealed)
            bytes (loop [idx 0 total (long codec/record-header-size)]
                (if (= idx (batch/accepted-count sealed))
                  total
                  (recur (inc idx)
                         (+ total (alength ^bytes (aget bodies idx))))))
            projected (+ retained bytes)]
        (when (> projected limit)
          (vreset! (:retention-backpressure-state state) bytes)
          (batch/cancel-before-dispatch!
           (ex-info "Required WAL history fills the hard retention limit"
                    {:error :txlog/retention-backpressure :type :txlog/retention-backpressure
                     :outcome :not-committed :retryable? (<= (+ slack bytes) limit)
                     :retained-max-bytes limit :projected-bytes projected})))))))

(defn- invalid! [reason data cause]
  (throw (ex-info "Required snapshot/WAL recovery history is invalid"
                  (merge {:type :txlog/recovery-history-invalid
                          :error :txlog/recovery-history-invalid
                          :phase :recovery :reason reason :retryable? false} data)
                  cause)))

(defn- crc ^long [^bytes bytes]
  (let [checksum (CRC32C.)]
    (.update checksum bytes 0 (alength bytes))
    (.getValue checksum)))

(defn- force-file! [^File file mode]
  (with-open [ch (FileChannel/open (.toPath file)
                                  (into-array StandardOpenOption
                                              [StandardOpenOption/WRITE]))]
    (segment/force-channel! ch mode)))

(defn- move! [^File source ^File target]
  (Files/move (.toPath source) (.toPath target) copy-options)
  (segment/force-parent-directory! (.getPath target)))

(defn- checksum-file
  "One fixed buffer; optional destination shares verification's streaming pass."
  [^File source ^File destination ^bytes buffer]
  (with-open [input (FileInputStream. source)
              ^java.io.OutputStream output
              (if destination (FileOutputStream. destination)
                  (java.io.OutputStream/nullOutputStream))]
    (let [checksum (CRC32C.)
          size (loop [total 0]
                 (let [_ (when (.isInterrupted (Thread/currentThread))
                           (throw (InterruptedException. "Recovery copy interrupted")))
                       n (.read input buffer)]
                   (if (neg? n)
                     total
                     (do
                       (.update checksum buffer 0 n)
                       (.write output buffer 0 n)
                       (recur (+ total n))))))]
      (.flush output)
      {:bytes size :crc32c (.getValue checksum)})))

(defn- read-edn! [text slot]
  (try (edn/read-string text)
       (catch RuntimeException t
         (invalid! :snapshot-invalid {:snapshot (.getPath ^File slot)} t))))

(defn- read-manifest [^File slot opts]
  (let [file (io/file slot manifest-name)]
    (when-not (and (.isFile file) (<= (.length file) (* 1024 1024)))
      (invalid! :snapshot-invalid {:snapshot (.getPath slot)} nil))
    (let [{:keys [payload crc32c]} (read-edn! (slurp file) slot)]
      (when-not (and (string? payload)
                     (= crc32c (crc (.getBytes ^String payload StandardCharsets/UTF_8))))
        (invalid! :snapshot-invalid {:snapshot (.getPath slot)} nil))
      (let [manifest (read-edn! payload slot)]
        (when-not (and (= 1 (:version manifest)) (true? (:complete? manifest))
                       (= (:db-identity opts) (:db-identity manifest))
                       (= (dbis opts) (:dbis manifest))
                       (nat-int? (:floor-lsn manifest))
                       (nat-int? (:durable-lsn manifest))
                       (<= (:floor-lsn manifest) (:durable-lsn manifest))
                       (pos-int? (:start-segment manifest))
                       (vector? (:files manifest))
                       (some #(= "data.mdb" (:name %)) (:files manifest))
                       (= (count (:files manifest))
                          (count (set (map :name (:files manifest))))))
          (invalid! :snapshot-invalid {:snapshot (.getPath slot)} nil))
        (doseq [{:keys [name bytes crc32c]} (:files manifest)]
          (when-not (and (string? name) (= name (.getName (io/file name)))
                         (not (#{"." ".." "lock.mdb" manifest-name} name))
                         (nat-int? bytes) (nat-int? crc32c)
                         (.isFile (io/file slot name)))
            (invalid! :snapshot-invalid {:snapshot (.getPath slot) :file name} nil)))
        manifest))))

(defn- publish-manifest! [^File dir manifest mode]
  (let [payload (pr-str manifest)
        file (io/file dir manifest-name)]
    (spit file (pr-str {:payload payload
                       :crc32c (crc (.getBytes payload StandardCharsets/UTF_8))}))
    (force-file! file mode)
    (segment/force-parent-directory! (.getPath file))))

(defn snapshot!
  "Copy without an activation barrier. Sample the active segment before joined
  P, then P before the MVCC copy; force the post-copy append prefix before
  publishing a verified current/previous anchor. Calls are serialized by the
  opener's snapshot monitor, outside execution ownership."
  [opts raw state collector]
  (let [directory (io/file (root opts))
        _ (.mkdirs directory)
        incoming (io/file directory (str "incoming-" (random-uuid)))
        ;; Sample bytes before P too. At most the one unjoined batch can lead
        ;; P, so the byte trigger can be late by at most one bounded batch.
        byte-floor (long @(:retention-total-bytes state))
        start-segment (long @(:segment-id state))
        floor (if collector (batch/published-lsn collector) 0)
        mode (let [m (:sync-mode state)] (if (= :none m) :fsync m))
        buffer (byte-array (* 1024 1024))]
    (.mkdirs incoming)
    (try
      (phase! :before-copy {:floor-lsn floor :start-segment start-segment})
      (i/copy raw (.getPath incoming) (get opts :snapshot-compact? true))
      (phase! :after-copy incoming)
      (let [target (dec (long @(:next-lsn state)))
            forced (wal/force-through! state target 0)
            _ (when-not (<= target (long (:last-durable-lsn forced)))
                (invalid! :snapshot-force-failed {:target-lsn target} nil))
            files (mapv (fn [^File file]
                          (force-file! file mode)
                          (assoc (checksum-file file nil buffer) :name (.getName file)))
                        (filter #(.isFile ^File %) (.listFiles incoming)))
            manifest {:version 1 :complete? true :db-identity (:db-identity opts)
                      :dbis (dbis opts) :floor-lsn floor :durable-lsn target
                      :retained-byte-floor byte-floor
                      :start-segment start-segment :created-ms (System/currentTimeMillis)
                      :files files}
            current (io/file directory "current")
            previous (io/file directory "previous")]
        (publish-manifest! incoming manifest mode)
        (phase! :before-snapshot-publish manifest)
        ;; At every crash point either current or previous is a complete old
        ;; anchor. Never mutate an installed snapshot's data or manifest.
        (when (.exists current)
          (when (.exists previous) (u/delete-files (.getPath previous)))
          (move! current previous))
        (move! incoming current)
        (phase! :snapshot-published manifest)
        manifest)
      (finally
        (when (.exists incoming) (u/delete-files (.getPath incoming)))))))

(defn collect-covered-segments!
  "Retain the complete tails required by both snapshot slots. Private M0 has
  no replica/backup/secondary-index pins; those environments remain compatibility.
  GC shares WAL ownership and never considers native applied progress."
  [opts state]
  (let [anchors (mapv #(read-manifest % opts)
                      (filter #(.isDirectory ^File %)
                              [(io/file (root opts) "current")
                               (io/file (root opts) "previous")]))
        start (when (seq anchors) (reduce min (map :start-segment anchors)))
        removed (volatile! 0)]
    (when start
      (wal/with-wal-owner!
       state 0
       (fn []
         (doseq [{:keys [id file]} (segment/segment-files (wal-dir opts))
                 :when (and (< (long id) (long start))
                            (< (long id) (long @(:segment-id state))))]
           (let [length (.length ^File file)]
             (Files/delete (.toPath ^File file))
             (vswap! removed + length)
             (vswap! (:retention-total-bytes state) - length)))
         (when-let [growth @(:retention-backpressure-state state)]
           (let [slack (if (:segment-prealloc? state)
                         (* 2 (long (:segment-prealloc-bytes state))) 0)]
             (when (<= (+ (long @(:retention-total-bytes state)) slack (long growth))
                       (retention-limit opts))
               (vreset! (:retention-backpressure-state state) nil))))
         (segment/force-parent-directory! (str (wal-dir opts) "/gc")))))
    @removed))

(defn- copy-snapshot! [opts ^File slot manifest ^File candidate]
  (let [buffer (byte-array (* 1024 1024))
        snapshot-bytes (reduce + (map :bytes (:files manifest)))
        tail-bytes (reduce + (map #(.length ^File (:file %))
                                 (segment/segment-files (wal-dir opts))))
        ;; Reserve room for the independent copy and native replay growth.
        required (+ snapshot-bytes (* 4 tail-bytes) (* 64 1024 1024))
        available (.getUsableSpace (Files/getFileStore (.toPath (io/file (:dir opts)))))]
    (when (< available required)
      (throw (ex-info "Insufficient space for independent recovery"
                      {:type :txlog/recovery-space-exhausted :phase :recovery
                       :required-bytes required :available-bytes available})))
    (.mkdirs candidate)
    (doseq [{:keys [name bytes crc32c]} (:files manifest)]
      (let [target (io/file candidate name)
            actual (checksum-file (io/file slot name) target buffer)]
        (when-not (= {:bytes bytes :crc32c crc32c} actual)
          (invalid! :snapshot-invalid {:snapshot (.getPath slot) :file name} nil))))
    (phase! :snapshot-restored {:snapshot (.getPath slot) :bytes snapshot-bytes})))

(defn- valid-replay-row?
  "Only the unconditional physical operations admitted by the private writer.
  Position/conditional flags cannot be replayed from a conservative floor."
  [catalog row]
  (let [op (nth row 0) name (nth row 1) key (nth row 2)
        options (get catalog name) value (when (= op :put) (nth row 3))]
    (and (some? options)
         (= (count row) (if (= op :put) 6 4))
         (bytes? key) (<= 1 (alength ^bytes key)
                           (long (get options :key-size c/+max-key-size+)))
         (or (= op :del)
             (and (= op :put) (bytes? value)
                  (or (not (some #{:dupsort} (:flags options)))
                      (<= 1 (alength ^bytes value) c/+max-key-size+)))))))

(defn- replay! [opts raw manifest]
  (let [limits (charge/resolve-limits opts)
        workspace (- (long (:byte-budget limits)) fixed-workspace)
        _ (when-not (pos? workspace)
            (invalid! :workspace-too-small {:byte-budget (:byte-budget limits)} nil))
        segments (filterv #(>= (long (:id %)) (long (:start-segment manifest)))
                          (segment/segment-files (wal-dir opts)))
        floor (long (:floor-lsn manifest))
        expected (volatile! (inc floor))
        replayed (volatile! 0)
        tail-bytes (volatile! 0)
        repairs (volatile! [])]
    (doseq [[idx {:keys [id file]}] (map-indexed vector segments)]
      (let [final? (= idx (dec (count segments)))
            scan (segment/scan-segment
                  (.getPath ^File file)
                  {:collect-records? false :allow-preallocated-tail? true
                   :max-record-bytes (:batch-max-bytes limits)
                   :on-record
                   (fn [record]
                     (let [^bytes body (:body record)
                           lsn (long (:lsn (codec/decode-commit-row-payload-header body)))]
                       (when-not (pos? lsn)
                         (invalid! :corrupt-record {:segment-id id :observed-lsn lsn} nil))
                       (when (> lsn floor)
                         (when-not (= lsn @expected)
                           (invalid! :gap {:snapshot-floor floor :segment-id id
                                           :expected-lsn @expected :observed-lsn lsn} nil))
                         (when (:compressed? record)
                           (invalid! :corrupt-record {:segment-id id :observed-lsn lsn} nil))
                         (let [rows (:rows (codec/decode-raw-commit-row-payload body workspace))]
                           (doseq [row rows]
                             (when-not (valid-replay-row? (dbis opts) row)
                               (invalid! :corrupt-record {:dbi (nth row 1)} nil)))
                           (i/transact-kv raw rows))
                         (vswap! expected inc)
                         (vswap! replayed inc)
                         (vswap! tail-bytes + (long codec/record-header-size)
                                 (long (:body-len record)))
                         (phase! :record-replayed {:lsn lsn :segment-id id}))))})]
        (when (:checksum-mismatch-tail? scan)
          ;; A complete checksum-invalid final frame is corruption, not an
          ;; incomplete frame we are permitted to discard.
          (invalid! :corrupt-record {:segment-id id :offset (:valid-end scan)} nil))
        (when (and (:partial-tail? scan) (not (:preallocated-tail? scan)))
          (when-not final?
            (invalid! :corrupt-record {:segment-id id :offset (:valid-end scan)} nil))
          (vswap! repairs conj [file (:valid-end scan)]))))
    (let [last-lsn (dec (long @expected))]
      (when (< last-lsn (long (:durable-lsn manifest)))
        (invalid! :gap {:snapshot-floor floor :expected-lsn (:durable-lsn manifest)
                        :observed-lsn last-lsn} nil))
      ;; Defer canonical tail repair until the complete required history has
      ;; validated and replay succeeded. A failed candidate cannot erase WAL.
      (doseq [[^File file end] @repairs]
        (with-open [ch (FileChannel/open (.toPath file)
                                        (into-array StandardOpenOption
                                                    [StandardOpenOption/WRITE]))]
          (.truncate ch (long end))))
      ;; Establish configured durability for recovered records even in relaxed
      ;; mode, before init-runtime-state initializes A=D and opens admission.
      (doseq [{:keys [file]} segments]
        (force-file! file (let [mode (wal/sync-mode opts)]
                           (if (= :none mode) :fsync mode))))
      (segment/force-parent-directory! (str (wal-dir opts) "/recovery"))
      {:floor-lsn floor :last-lsn last-lsn :records @replayed :tail-bytes @tail-bytes})))

(defn restore!
  "Restore on every runtime start, including clean close/reopen. Try current,
  then previous, preserving canonical snapshots/WAL on any invalid history.
  Return nil only for a truly fresh environment with no data or WAL."
  [opts]
  (let [directory (io/file (:dir opts))
        slots (filterv #(.isDirectory ^File %)
                       [(io/file (root opts) "current") (io/file (root opts) "previous")])]
    (if (empty? slots)
      (when (or (.exists (io/file directory "data.mdb"))
                (seq (segment/segment-files (wal-dir opts))))
        (invalid! :snapshot-missing {:dir (:dir opts)} nil))
      (loop [remaining slots failures []]
        (if-let [slot (first remaining)]
          (let [candidate (io/file directory (str ".restore-" (random-uuid)))
                attempt
                (try
                  (let [manifest (read-manifest slot opts)]
                    (copy-snapshot! opts slot manifest candidate)
                    (let [db (l/open-kv (.getPath candidate) (native-options opts))
                          raw (kv/raw-lmdb db)
                          result (try
                                   (doseq [[name dbi-opts] (dbis opts)]
                                     (i/open-dbi raw name dbi-opts))
                                   (let [result (replay! opts raw manifest)]
                                     (i/sync raw 1)
                                     result)
                                   (finally (i/close-kv db)))]
                      (phase! :before-install {:candidate candidate :recovery result})
                      ;; data.mdb is the atomic catalog/data unit. Side files
                      ;; precede it; another recovery always overwrites them
                      ;; from the selected snapshot before any native open.
                      (doseq [{:keys [name]} (:files manifest)
                              :when (not= name "data.mdb")]
                        (move! (io/file candidate name) (io/file directory name)))
                      (move! (io/file candidate "data.mdb") (io/file directory "data.mdb"))
                      (Files/deleteIfExists (.toPath (io/file directory "lock.mdb")))
                      (phase! :recovery-installed result)
                      {:result (assoc result :snapshot manifest)}))
                  (catch Throwable t {:error t})
                  (finally (when (.exists candidate) (u/delete-files (.getPath candidate)))))]
            (if-let [error (:error attempt)]
              ;; Operational/interruption errors cannot be disguised as a
              ;; corrupt-current fallback; leave serving closed and propagate.
              (if (#{:txlog/corrupt :txlog/recovery-history-invalid
                     :txlog/recovery-record-too-large} (:type (ex-data error)))
                (recur (next remaining) (conj failures error))
                (throw error))
              (:result attempt)))
          (invalid! (or (:reason (ex-data (last failures)))
                        (when (= :txlog/corrupt (:type (ex-data (last failures))))
                          :corrupt-record)
                        :scan-failed)
                    {:dir (:dir opts)
                                  :attempts (mapv #(ex-data %) failures)}
                    (last failures)))))))
