;; ;; Copyright (c) Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns ^:no-doc datalevin.binding.cpp
  "Native binding to LMDB using JavaCPP"
  (:refer-clojure :exclude [sync])
  (:require
   [clojure.java.io :as io]
   [clojure.string :as s]
   [datalevin.binding.cpp.buffer :as buffer]
   [datalevin.binding.cpp.iter :as iter]
   [datalevin.binding.cpp.lifecycle :as lifecycle]
   [datalevin.binding.cpp.open :as open]
   [datalevin.binding.cpp.write :as write]
   [datalevin.bits :as b]
   [datalevin.util :as u :refer [raise]]
   [datalevin.constants :as c]
   [datalevin.compress :as cp]
   [datalevin.buffer :as bf]
   [datalevin.migrate :as m]
   [datalevin.scan :as scan]
   [datalevin.interface :as i
    :refer [IList ILMDB IAdmin open-dbi close-kv env-dir close-vecs
            transact-kv stat key-compressor
            val-compressor set-max-val-size max-val-size
            set-key-compressor set-val-compressor]]
   [datalevin.lmdb :as l
    :refer [open-kv IBuffer IRange IRtx IWriting ICompress]])
  (:import
   [datalevin.dtlvnative DTLV DTLV$MDB_envinfo DTLV$MDB_stat]
   [datalevin.cpp BufVal Env Txn Dbi Cursor Stat Info Util Util$MapFullException]
   [datalevin.binding.cpp.buffer DBI]
   [datalevin.lmdb RangeContext]
   [datalevin.utl BitOps]
   [java.util.concurrent ConcurrentHashMap]
   [java.util.concurrent.atomic AtomicBoolean]
   [java.lang AutoCloseable]
   [java.io File]
   [java.util HashMap Arrays Collection List
    Map$Entry]
   [java.nio BufferOverflowException ByteBuffer]
   [org.bytedeco.javacpp LongPointer]
   [clojure.lang IObj]))

;; Compatibility aliases for callers that use `datalevin.binding.cpp`; the
;; implementations live in `datalevin.binding.cpp.lifecycle`.
(def open-local-kv-handle lifecycle/open-local-kv-handle)
(def register-shutdown-close! lifecycle/register-shutdown-close!)
(def shutdown-hooks lifecycle/shutdown-hooks)
(def add-only-datom-batch? write/add-only-datom-batch?)

(defn- version-file
  [^File dir]
  (io/file dir c/version-file-name))

(defn- write-version-file
  [^File dir version]
  (when (and version (not (s/blank? ^String version)))
    (spit (version-file dir) version)
    version))

(defn- read-version-file
  [^File dir]
  (try
    (let [^File f (version-file dir)]
      (when (.exists f)
        (some-> (slurp f) s/trim not-empty)))
    (catch Exception e
      (raise "Unable to read VERSION file"
             {:msg (.getMessage e)}))))

(def ^:dynamic *before-write-commit-fn*
  nil)

(def ^:dynamic *ave-multiple-write-observer*
  nil)

(def ^:dynamic *ave-multiple-buffer-allocation-observer*
  nil)

(def ^:dynamic *ave-multiple-write-flags-observer*
  nil)

;; The write and buffer-allocation observers are bound at the
;; `datalevin.binding.cpp` level for compatibility; the extracted namespaces
;; read them through these thunks.
(write/set-write-observer-readers!
 (fn [] *ave-multiple-write-observer*)
 (fn [] *ave-multiple-write-flags-observer*))

(buffer/set-buffer-allocation-observer-reader!
 (fn [] *ave-multiple-buffer-allocation-observer*))

(defn- run-before-write-commit!
  [context]
  (when-let [f *before-write-commit-fn*]
    (f context)))

(defprotocol ^:no-doc IListSeekBuffer
  (filter-list-id-int-prefix!
    [this cur in id-idx prefix out]
    "Filter sorted tuples by list values with an integer prefix. List keys are
    entity IDs; duplicate IDs are probed once and matching tuples are kept.")
  (list-avg-id?
    [this cur aid value value-type id]
    "Return true when an AVE list contains the exact entity ID.")
  (filter-list-avg-bound-id!
    [this cur in value-idx aid value-type bound-id out]
    "Filter tuples by an AVE key and a fixed entity ID, appending that ID to
    each matching tuple. Equal adjacent values are probed once.")
  (filter-list-avg-tuple-id!
    [this cur in value-idx entity-idx aid value-type out]
    "Filter tuples by AVE keys and entity IDs stored in tuple columns. Equal
    adjacent keys reuse their encoding and equal adjacent pairs are probed
    once."))

(deftype Rtx [^:unsynchronized-mutable lmdb
              ^Txn txn
              depth
              ^BufVal kp
              ^BufVal vp
              ^BufVal start-kp
              ^BufVal stop-kp
              ^BufVal start-vp
              ^BufVal stop-vp
              ^:unsynchronized-mutable ^ByteBuffer k-comp-bf
              ^:volatile-mutable ^ByteBuffer v-comp-bf
              aborted?
              ^AtomicBoolean closed?
              ^boolean owns-buffers?]

  ICompress
  (key-bf [_] (.clear k-comp-bf))
  (val-bf [_] (.clear v-comp-bf))

  iter/IRtxInternals
  (rtx-txn [_] txn)
  (rtx-key-buf [_] kp)
  (rtx-val-buf [_] vp)
  (rtx-start-key-buf [_] start-kp)
  (rtx-stop-key-buf [_] stop-kp)
  (rtx-start-val-buf [_] start-vp)
  (rtx-stop-val-buf [_] stop-vp)

  IListSeekBuffer
  (filter-list-id-int-prefix! [_ cur in id-idx prefix out]
    (let [^Cursor cur       cur
          ^List in         in
          ^Collection out  out
          id-idx           (int id-idx)
          prefix           (long prefix)
          nt               (.size in)
          key-compressor   (key-compressor lmdb)
          ^BufVal cur-val  (.val cur)]
      (buffer/put-bufval start-vp prefix :int (val-compressor lmdb) v-comp-bf)
      (loop [i           (long 0)
             last-id     (long 0)
             last-found? false
             have-last?  false]
        (when (< i nt)
          (let [^objects tuple (.get in (int i))
                id             (long (aget tuple id-idx))
                found?
                (if (and have-last? (== id last-id))
                  last-found?
                  (do
                    (buffer/put-id-bufval start-kp id key-compressor k-comp-bf)
                    (boolean
                      (and (.get cur ^BufVal start-kp ^BufVal start-vp
                                 DTLV/MDB_GET_BOTH_RANGE)
                           (== prefix (.getInt (.outBuf cur-val) 0))))))]
            (when found?
              (.add out tuple))
            (recur (u/long-inc i) (long id) found? true))))
      out))

  (list-avg-id? [_ cur aid value value-type id]
    (let [^Cursor cur cur]
      (buffer/put-bufval start-kp
                  (b/indexable nil (long aid) value value-type nil)
                  :avg (key-compressor lmdb) k-comp-bf)
      (buffer/put-id-bufval start-vp (long id) nil v-comp-bf)
      (boolean
        (.get cur ^BufVal start-kp ^BufVal start-vp DTLV/MDB_GET_BOTH))))

  (filter-list-avg-bound-id!
    [_ cur in value-idx aid value-type bound-id out]
    (let [^Cursor cur       cur
          ^List in         in
          ^Collection out  out
          value-idx        (int value-idx)
          aid              (long aid)
          bound-id         (long bound-id)
          nt               (.size in)
          key-compressor   (key-compressor lmdb)
          value-compressor nil]
      (buffer/put-id-bufval start-vp bound-id value-compressor v-comp-bf)
      (loop [i           (long 0)
             last-value  nil
             last-found? false
             have-last?  false]
        (when (< i nt)
          (let [^objects tuple (.get in (int i))
                value          (aget tuple value-idx)
                same-value?    (and have-last?
                                    (or (identical? value last-value)
                                        (= value last-value)))
                found?         (if same-value?
                                 last-found?
                                 (do
                                   (buffer/put-bufval
                                     start-kp
                                     (b/indexable nil aid value value-type nil)
                                     :avg key-compressor k-comp-bf)
                                   (boolean
                                     (.get cur
                                           ^BufVal start-kp
                                           ^BufVal start-vp
                                           DTLV/MDB_GET_BOTH))))]
            (when found?
              (let [tuple-size      (alength tuple)
                    ^objects joined (Arrays/copyOf
                                      tuple (int (inc tuple-size)))]
                (aset joined tuple-size (Long/valueOf bound-id))
                (.add out joined)))
            (recur (u/long-inc i) value found? true))))
      out))

  (filter-list-avg-tuple-id!
    [_ cur in value-idx entity-idx aid value-type out]
    (let [^Cursor cur       cur
          ^List in         in
          ^Collection out  out
          value-idx        (int value-idx)
          entity-idx       (int entity-idx)
          aid              (long aid)
          nt               (.size in)
          key-compressor   (key-compressor lmdb)
          value-compressor nil]
      (loop [i           (long 0)
             last-value  nil
             last-id     (long 0)
             last-found? false
             have-last?  false]
        (when (< i nt)
          (let [^objects tuple (.get in (int i))
                value          (aget tuple value-idx)
                id             (long (aget tuple entity-idx))
                same-value?    (and have-last?
                                    (or (identical? value last-value)
                                        (= value last-value)))
                same-id?       (and have-last? (== id last-id))
                same-probe?    (and same-value? same-id?)]
            (when-not same-value?
              (buffer/put-bufval start-kp
                          (b/indexable nil aid value value-type nil)
                          :avg key-compressor k-comp-bf))
            (when-not same-id?
              (buffer/put-id-bufval start-vp id value-compressor v-comp-bf))
            (let [found? (if same-probe?
                           last-found?
                           (boolean
                             (.get cur
                                   ^BufVal start-kp
                                   ^BufVal start-vp
                                   DTLV/MDB_GET_BOTH)))]
              (when found?
                (.add out tuple))
              (recur (u/long-inc i) value id found? true)))))
      out))

  IBuffer
  (put-key [_ x t]
    (try
      (buffer/put-bufval kp x t (key-compressor lmdb) k-comp-bf)
      (catch BufferOverflowException _
        (raise "Key cannot be larger than 511 bytes." {:input x}))
      (catch Exception e
        (raise "Error putting read-only transaction key buffer: "
               e {:value x :type t}))))
  (put-val [_ _ _]
    (raise "put-val not allowed for read only txn buffer" {}))

  IRange
  (range-info [_ range-type k1 k2 kt]
    (buffer/put-bufval start-kp k1 kt (key-compressor lmdb) k-comp-bf)
    (buffer/put-bufval stop-kp k2 kt (key-compressor lmdb) k-comp-bf)
    (l/range-table range-type start-kp stop-kp))

  (list-range-info [this k-range-type k1 k2 kt v-range-type v1 v2 vt]
    (buffer/list-range-info* (key-compressor lmdb) this k-range-type k1 k2 kt
                      v-range-type v1 v2 vt (val-compressor lmdb)))

  IRtx
  (read-only? [_] (.isReadOnly txn))
  (reset [this]
    (vswap! depth u/long-dec)
    (when (zero? ^long @depth)
      (.reset txn))
    this)

  (renew [this]
    (when (zero? ^long @depth)
      (.renew txn))
    (vswap! depth u/long-inc)
    this)

  AutoCloseable
  (close [_]
    (when (.compareAndSet closed? false true)
      (let [txn*       txn
            kp*        kp
            vp*        vp
            start-kp*  start-kp
            stop-kp*   stop-kp
            start-vp*  start-vp
            stop-vp*   stop-vp
            k-comp-bf* k-comp-bf
            v-comp-bf* v-comp-bf]
        (set! lmdb nil)
        (set! k-comp-bf nil)
        (set! v-comp-bf nil)
        (buffer/close-txn-quiet! txn*)
        (when owns-buffers?
          (buffer/close-bufval-quiet! kp*)
          (buffer/close-bufval-quiet! vp*)
          (buffer/close-bufval-quiet! start-kp*)
          (buffer/close-bufval-quiet! stop-kp*)
          (buffer/close-bufval-quiet! start-vp*)
          (buffer/close-bufval-quiet! stop-vp*)
          (buffer/clean-buffer-quiet! k-comp-bf*)
          (buffer/clean-buffer-quiet! v-comp-bf*))))
    nil))




(defn- list-count*
  [^Rtx rtx ^Cursor cur k kt]
  (.put-key rtx k kt)
  (iter/dtlv-c (DTLV/dtlv_list_val_count
           (.ptr cur) (.ptr ^BufVal (.-kp rtx)) (.ptr ^BufVal (.-vp rtx)))))

(defn- in-list?*
  [^DBI dbi ^Rtx rtx ^Cursor cur k kt v vt]
  (buffer/list-range-info* (.-key-codec dbi) rtx :at-least k nil kt :at-least v nil vt
                    (buffer/dbi-val-compressor dbi))
  (.get cur ^BufVal (.-start-kp rtx) ^BufVal (.-start-vp rtx)
        DTLV/MDB_GET_BOTH))

(defn- near-list*
  [^DBI dbi ^Rtx rtx ^Cursor cur k kt v vt]
  (let [value-compressor (buffer/dbi-val-compressor dbi)]
    (buffer/list-range-info* (.-key-codec dbi) rtx
                      :at-least k nil kt :at-least v nil vt value-compressor)
    (when (.get cur ^BufVal (.-start-kp rtx) ^BufVal (.-start-vp rtx)
                DTLV/MDB_GET_BOTH_RANGE)
      (iter/v-bf (.val cur) value-compressor rtx))))

(declare ->CppLMDB)

(defn- up-db-size [^Env env]
  (let [^Info info (Info/create env)]
    (.setMapSize env (* ^long c/+buffer-grow-factor+
                        (.me_mapsize ^DTLV$MDB_envinfo (.get info))))
    (.close info)))

(defn- close-rtx-quiet!
  [^Rtx rtx]
  (when rtx
    (try
      (.close ^AutoCloseable rtx)
      (catch Throwable _))))

(defn- discard-thread-reader!
  [^ThreadLocal tl-reader ^ConcurrentHashMap reader-registry
   ^Thread thread ^Rtx rtx]
  (.remove tl-reader)
  (.remove reader-registry thread rtx)
  (close-rtx-quiet! rtx)
  nil)

(defn- sweep-dead-reader-rtxs!
  [^ConcurrentHashMap reader-registry]
  (let [closed (volatile! 0)]
    (doseq [^Map$Entry entry (.entrySet reader-registry)]
      (let [^Thread thread (.getKey entry)
            ^Rtx rtx      (.getValue entry)]
        (when-not (.isAlive thread)
          (when (.remove reader-registry thread rtx)
            (close-rtx-quiet! rtx)
            (vswap! closed u/long-inc)))))
    @closed))

(defn- close-reader-rtxs!
  [^ConcurrentHashMap reader-registry]
  (doseq [^Map$Entry entry (.entrySet reader-registry)]
    (let [^Thread thread (.getKey entry)
          ^Rtx rtx      (.getValue entry)]
      (when (.remove reader-registry thread rtx)
        (close-rtx-quiet! rtx)))))

(defn- reusable-reader-rtx
  [this ^ThreadLocal tl-reader ^ConcurrentHashMap reader-registry]
  (let [thread (Thread/currentThread)]
    (when-not (.isVirtual thread)
      (when-let [^Rtx rtx (.get tl-reader)]
        (if (<= (long (max-val-size this))
                ^int (.capacity ^ByteBuffer (l/val-bf rtx)))
          (try
            (.renew rtx)
            (catch Exception _
              ;; Storage faults can leave a cached reader txn stale even though
              ;; the LMDB env itself remains valid. Drop the stale reader and let
              ;; the caller open a fresh one instead of surfacing a misleading
              ;; "multiple connections" error for subsequent reads.
              (discard-thread-reader! tl-reader reader-registry thread rtx)))
          (discard-thread-reader! tl-reader reader-registry thread rtx))))))

(defn- fresh-reader-rtx
  [this ^Env env ^ThreadLocal tl-reader ^ConcurrentHashMap reader-registry]
  (let [thread        (Thread/currentThread)
        max-val-size* (long (max-val-size this))
        rtx (Rtx. this
                  (Txn/createReadOnly env)
                  (volatile! 1)
                  (buffer/new-bufval c/+max-key-size+)
                  (buffer/new-bufval 0)
                  (buffer/new-bufval c/+max-key-size+)
                  (buffer/new-bufval c/+max-key-size+)
                  (buffer/new-bufval c/+max-key-size+)
                  (buffer/new-bufval c/+max-key-size+)
                  (bf/allocate-buffer c/+max-key-size+)
                  (bf/allocate-buffer max-val-size*)
                  (volatile! false)
                  (AtomicBoolean.)
                  true)]
    (.set tl-reader rtx)
    (when-not (.isVirtual thread)
      (when-let [^Rtx old (.put reader-registry thread rtx)]
        (when-not (identical? old rtx)
          (close-rtx-quiet! old))))
    rtx))

(defn- read-transaction-acquisition-error
  [dir ^Exception e]
  (ex-info
    (str "Failed to acquire LMDB read transaction: " (ex-message e))
    {:type      :lmdb/read-transaction
     :operation :acquire
     :dir       dir
     :cause     (ex-message e)}
    e))


(declare key-range-list-count-fast)

(deftype CppLMDB [^Env env
                  info
                  ^ThreadLocal tl-reader
                  ^ConcurrentHashMap reader-registry
                  ^HashMap dbis
                  scheduled-sync
                  ^BufVal kp-w
                  ^BufVal vp-w
                  ^BufVal start-kp-w
                  ^BufVal stop-kp-w
                  ^BufVal start-vp-w
                  ^BufVal stop-vp-w
                  ^ByteBuffer k-comp-bf-w
                  ^:volatile-mutable ^ByteBuffer v-comp-bf-w
                  write-txn
                  writing?
                  ^:volatile-mutable k-comp
                  ^:volatile-mutable v-comp
                  ^:unsynchronized-mutable meta]

  IWriting
  (writing? [_] writing?)

  (write-txn [_] write-txn)

  (mark-write [_]
    (->CppLMDB
      env info tl-reader reader-registry dbis scheduled-sync kp-w vp-w start-kp-w
      stop-kp-w start-vp-w stop-vp-w k-comp-bf-w v-comp-bf-w
      write-txn true k-comp v-comp meta))

  (reset-write
    [this]
    (.clear kp-w)
    (.clear vp-w)
    (.clear start-kp-w)
    (.clear stop-kp-w)
    (.clear start-vp-w)
    (.clear stop-vp-w)
    (.clear k-comp-bf-w)
    (when-some [^ByteBuffer bf v-comp-bf-w]
      (.clear bf))
    (vreset! write-txn (Rtx. this
                             (Txn/create env)
                             (volatile! 1)
                             kp-w
                             vp-w
                             start-kp-w
                             stop-kp-w
                             start-vp-w
                             stop-vp-w
                             k-comp-bf-w
                             v-comp-bf-w
                             (volatile! false)
                             (AtomicBoolean.)
                             false)))

  IObj
  (withMeta [this m] (set! meta m) this)
  (meta [_] meta)

  ILMDB
  (max-val-size [_] (or (:max-val-size @info) c/*init-val-size*))

  (set-max-val-size [_ size]
    (set! v-comp-bf-w (bf/allocate-buffer size))
    (vswap! info assoc :max-val-size size :max-val-size-changed? true))

  (close-kv [this]
    (let [dir         (env-dir this)
          dir-key     (lifecycle/local-kv-handle-key (u/file dir) (@info :flags))
          close-error (volatile! nil)]
      ;; The shutdown hook can race an explicit close. All marked-write views
      ;; share this lock, so guard the complete native teardown rather than only
      ;; the final env close.
      (locking write-txn
        (when-not (.isClosed env)
          (lifecycle/unregister-shutdown-hook! dir)
          (lifecycle/stop-scheduled-sync scheduled-sync)
          (close-reader-rtxs! reader-registry)
          (when-let [^Rtx wtxn @write-txn]
            (close-rtx-quiet! wtxn)
            (vreset! write-txn nil))
          (.remove tl-reader)
          (let [dir-prefix (str (.env-dir this) u/+separator+)
                indices    (->> @l/vector-indices
                                (keep (fn [[fname idx]]
                                        (when (and (string? fname)
                                                   (s/starts-with? fname
                                                                   dir-prefix))
                                          idx)))
                                vec)]
            ;; Close vector indices while LMDB env is still valid.
            (doseq [idx indices]
              (try
                (close-vecs idx)
                (catch Throwable e
                  (when-not @close-error
                    (vreset! close-error e))))))
          (doseq [^DBI db (.values dbis)]
            (try
              (buffer/close-dbi-quiet! db)
              (catch Throwable e
                (when-not @close-error
                  (vreset! close-error e)))))
          (.clear dbis)
          (buffer/close-bufval-quiet! kp-w)
          (buffer/close-bufval-quiet! vp-w)
          (buffer/close-bufval-quiet! start-kp-w)
          (buffer/close-bufval-quiet! stop-kp-w)
          (buffer/close-bufval-quiet! start-vp-w)
          (buffer/close-bufval-quiet! stop-vp-w)
          (buffer/clean-buffer-quiet! k-comp-bf-w)
          (buffer/clean-buffer-quiet! v-comp-bf-w)
          (try
            (.sync env 1)
            (catch Throwable e
              (when-not @close-error
                (vreset! close-error e))))
          (try
            (.close env)
            (catch Throwable e
              (when-not @close-error
                (vreset! close-error e))))
          (when (@info :temp?) (u/delete-files (@info :dir)))))
      (when (.isClosed env)
        (lifecycle/release-local-kv-handle! dir-key)
        (swap! l/lmdb-dirs disj dir)
        (when (and (not (@info :spill?)) (zero? (count @l/lmdb-dirs)))
          (l/shutdown-last-lmdb-executors!)))
      (when-let [e @close-error]
        (throw e))
      nil))

  (closed-kv? [_] (.isClosed env))

  (check-ready [this]
    (when (.closed-kv? this)
      (raise "LMDB env is closed." {:type :lmdb/closed})))

  (env-dir [_] (@info :dir))
  (kv-info [_] info)

  (env-opts [_] (dissoc @info :compression :dbis :custom-dbis :types :custom-types-revision :custom-value-id
                       :custom-type-cache :custom-payload-dbi-open? :runtime-opts))

  (dbi-opts [_ dbi-name] (get-in @info [:dbis dbi-name]))

  (key-compressor [_] k-comp)

  (set-key-compressor [_ c] (set! k-comp c))

  (val-compressor [_] v-comp)

  (set-val-compressor [_ c] (set! v-comp c))

  (open-dbi [this dbi-name]
    (.open-dbi this dbi-name nil))
  (open-dbi [this dbi-name
             {:keys [key-size val-size flags validate-data?]
              :or   {key-size       (or (get-in @info [:dbis dbi-name :key-size])
                                        c/+max-key-size+)
                     val-size       (or (get-in @info [:dbis dbi-name :val-size])
                                        c/*init-val-size*)
                     flags          (or (get-in @info [:dbis dbi-name :flags])
                                        c/default-dbi-flags)
                     validate-data? (or (get-in @info
                                                [:dbis dbi-name :validate-data?])
                                        false)} :as supplied}]
    (.check-ready this)
    (assert (< ^long key-size 512) "Key size cannot be greater than 511 bytes")
    (locking dbis
      (let [{info-dbis :dbis max-dbis :max-dbs} @info]
        ;; A known DBI can always be reopened: it does not consume another LMDB
        ;; named-database slot. This matters when an environment already has
        ;; max-dbs persisted DBIs and is opened by a fresh process.
        (if (or (contains? info-dbis dbi-name)
                (< (count info-dbis) ^long max-dbis))
          (let [existing-opts (get info-dbis dbi-name)
                _ (doseq [k [:key-type :value-type]
                          :when (and existing-opts (contains? supplied k)
                                     (not= (get existing-opts k) (get supplied k)))]
                    (raise "Cannot change a DBI's installed custom type"
                           {:error :custom-type/dbi-conflict :dbi dbi-name :option k}))
                opts     (merge {:key-size       key-size
                          :val-size       val-size
                          :flags          flags
                          :validate-data? validate-data?}
                                (select-keys existing-opts [:key-type :value-type])
                                (select-keys supplied [:key-type :value-type]))
                flags    (set flags)
                dupsort? (if (:dupsort flags) true false)
                dupfixed? (if (:dupfixed flags) true false)
                counted? (if (:counted flags) true false)
                _        (when (and v-comp dupsort? (not dupfixed?))
                           (raise "Value compression is not supported on ordered duplicate values"
                                  {:error :compression/ordered-duplicates
                                   :dbi dbi-name :val-compress :zstd}))
                raw?     (= dbi-name c/kv-info)
                kp       (buffer/new-bufval key-size)
                vp       (buffer/new-bufval val-size)
                kc       (bf/allocate-buffer key-size)
                vc       (bf/allocate-buffer val-size)
                dbi      (Dbi/create env dbi-name (buffer/kv-flags flags))
                db       (buffer/->DBI this (when-not raw? k-comp)
                               (when-not (or raw? dupfixed?) v-comp)
                               dbi (buffer/new-pools) kp vp kc vc
                               dupsort? dupfixed? counted?
                               ;; The custom KV adapter validates logical data;
                               ;; this DBI receives encoded references and bytes.
                               (and validate-data?
                                    (not (or (:key-type opts) (:value-type opts))))
                               nil nil)]
            (when (not= dbi-name c/kv-info)
              (when (not= existing-opts opts)
                (vswap! info assoc-in [:dbis dbi-name] opts)
                (transact-kv
                  this [(l/kv-tx :put c/kv-info [:dbis dbi-name] opts
                                 [:keyword :string])])))
            (when (or (:key-type opts) (:value-type opts))
              (vswap! info update :custom-dbis (fnil conj #{}) dbi-name))
            (.put dbis dbi-name db)
            db)
          (u/raise (str "Reached maximal number of DBI: " max-dbis) {})))))

  (get-dbi [this dbi-name]
    (.get-dbi this dbi-name true))
  (get-dbi [this dbi-name create?]
    (or (.get dbis dbi-name)
        (locking dbis
          (or (.get dbis dbi-name)
              ;; DBI metadata is durable, while an opened native DBI handle is
              ;; process-local. Lazily restore a known handle after a server or
              ;; environment reopen without creating an unknown database.
              (if (or create? (contains? (:dbis @info) dbi-name))
                (.open-dbi this dbi-name)
                (u/raise (str "DBI " dbi-name " is not open") {}))))))

  (clear-dbi [this dbi-name]
    (.check-ready this)
    (try
      (let [^Dbi dbi (.-db ^DBI (.get-dbi this dbi-name))
            ^Txn txn (Txn/create env)]
        (Util/checkRc (DTLV/mdb_drop (.get txn) (.get dbi) 0))
        (.commit txn))
      (catch Util$MapFullException _
        (let [^Info info (Info/create env)]
          (.setMapSize env (* ^long c/+buffer-grow-factor+
                              (.me_mapsize ^DTLV$MDB_envinfo (.get info))))
          (.close info))
        (.clear-dbi this dbi-name))
      (catch Exception e
        (raise "Fail to clear DBI: " dbi-name " " e {}))))

  (drop-dbi [this dbi-name]
    (.check-ready this)
    (try
      (let [^Dbi dbi (.-db ^DBI (.get-dbi this dbi-name))
            ^Txn txn (Txn/create env)]
        (Util/checkRc (DTLV/mdb_drop (.get txn) (.get dbi) 1))
        (.commit txn)
        (vswap! info update :dbis dissoc dbi-name)
        (vswap! info update :custom-dbis disj dbi-name)
        (transact-kv this c/kv-info
                     [[:del [:dbis dbi-name]]] [:keyword :string])
        (.remove dbis dbi-name)
        nil)
      (catch Exception e (raise "Fail to drop DBI: " dbi-name e {}))))

  (list-dbis [_] (keys (@info :dbis)))

  (copy [this dest]
    (.copy this dest false))
  (copy [this dest compact?]
    (if (-> dest u/file u/empty-dir?)
      (do (.copy env dest (if compact? true false))
          (lifecycle/copy-version-file this dest)
          (lifecycle/copy-compression-files this dest))
      (raise "Destination directory is not empty." {})))

  (get-rtx [this]
    (when-not (.closed-kv? this)
      (try
        (or (reusable-reader-rtx this tl-reader reader-registry)
            (do
              (sweep-dead-reader-rtxs! reader-registry)
              (fresh-reader-rtx this env tl-reader reader-registry)))
        (catch Exception e
          (throw (read-transaction-acquisition-error (env-dir this) e))))))

  (return-rtx [this rtx]
    (when-not (.closed-kv? this)
      (if (.isVirtual (Thread/currentThread))
        (try
          (close-rtx-quiet! rtx)
          (finally
            (.remove tl-reader)))
        (.reset ^Rtx rtx))))

  (stat [_]
    (try
      (let [stat ^Stat (Stat/create env)
            m    (buffer/stat-map stat)]
        (.close stat)
        m)
      (catch Exception e
        (raise "Fail to get statistics: " e {}))))
  (stat [this dbi-name]
    (if dbi-name
      (let [^Rtx rtx (.get-rtx this)]
        (try
          (let [^DBI dbi   (.get-dbi this dbi-name false)
                ^Dbi db    (.-db dbi)
                ^Txn txn   (.-txn rtx)
                ^Stat stat (Stat/create txn db)
                m          (buffer/stat-map stat)]
            (.close stat)
            m)
          (catch Exception e
            (raise "Fail to get statistics: " e {:dbi dbi-name}))
          (finally (.return-rtx this rtx))))
      (stat this)))

  (entries [this dbi-name]
    (let [^DBI dbi (.get-dbi this dbi-name)
          ^Rtx rtx (.get-rtx this)
          ^Dbi db  (.-db dbi)
          ^Txn txn (.-txn rtx)]
      (try
        (if (.-counted? dbi)
          (with-open [^LongPointer ptr (LongPointer. 1)]
            (DTLV/mdb_count_all (.get txn) (.get db) (int 0) ptr)
            (.get ptr))
          (let [^Stat stat (Stat/create txn db)
                entries    (.ms_entries ^DTLV$MDB_stat (.get stat))]
            (.close stat)
            entries))
        (catch Exception e
          (raise "Fail to get entries: " (ex-message e) {:dbi dbi-name}))
        (finally (.return-rtx this rtx)))))

  (open-transact-kv [this]
    (.check-ready this)
    (try
      (.reset-write this)
      (.mark-write this)
      (catch Exception e
        (raise "Fail to open read/write transaction in LMDB: " e {}))))

  (close-transact-kv [_]
    (if-let [^Rtx wtxn @write-txn]
      (when-let [^Txn txn (.-txn wtxn)]
        (let [aborted? @(.-aborted? wtxn)]
          (if aborted?
            (.close txn)
            (try
              (run-before-write-commit! {:operation :close-transact-kv})
              (.commit txn)
              (catch Util$MapFullException _
                (.close txn)
                (up-db-size env)
                (vreset! write-txn nil)
                (raise "DB resized" {:resized true}))
              (catch Exception e
                (.close txn)
                (vreset! write-txn nil)
                (if (= :ha/write-rejected (:error (ex-data e)))
                  (throw e)
                  (raise "Fail to commit read/write transaction in LMDB: "
                         e {})))))
          (vreset! write-txn nil)
          (.close txn)
          (if aborted? :aborted :committed)))
      (raise "Calling `close-transact-kv` without opening" {})))

  (abort-transact-kv [_]
    (when-let [^Rtx wtxn @write-txn]
      (vreset! (.-aborted? wtxn) true)
      (vreset! write-txn wtxn)
      nil))

  (transact-kv [this txs] (.transact-kv this nil txs))
  (transact-kv [this dbi-name txs]
    (.transact-kv this dbi-name txs :data :data))
  (transact-kv [this dbi-name txs k-type]
    (.transact-kv this dbi-name txs k-type :data))
  (transact-kv [this dbi-name txs k-type v-type]
    (let [^objects prepared-one-shot
          (let [tx-open? (some? @write-txn)]
            (when-not tx-open?
              (write/prepare-kvtx-ops txs dbis dbi-name k-type v-type)))]
      (letfn [(do-transact [prepared]
              (.check-ready this)
              (let [^Rtx rtx  @write-txn
                    one-shot? (nil? rtx)
                    ^DBI dbi  (when dbi-name
                                (or (.get dbis dbi-name)
                                    (raise dbi-name " is not open" {})))
                    ^Txn txn  (if one-shot?
                                (Txn/create env)
                                (.-txn rtx))]
                (try
                  (if prepared
                    (write/transact-prepared-ops* prepared dbis txn)
                    (if dbi
                      (write/transact1* txs dbi txn k-type v-type)
                      (write/transact* txs dbis txn)))
                  (when (:max-val-size-changed? @info)
                    (write/transact* [[:put c/kv-info :max-val-size (:max-val-size @info)]]
                               dbis txn)
                    (vswap! info assoc :max-val-size-changed? false))
                  (when one-shot?
                    (run-before-write-commit! {:operation :transact-kv
                                               :dbi-name dbi-name})
                    (.commit txn))
                  :transacted
                  (catch Util$MapFullException _
                    (.close txn)
                    (up-db-size env)
                    (if one-shot?
                      (.transact-kv this dbi-name txs k-type v-type)
                      (do (.reset-write this)
                          (raise "DB resized" {:resized true}))))
                  (catch Exception e
                    (when one-shot? (.close txn))
                    (if (or (= :ha/write-rejected (:error (ex-data e)))
                            (l/blind-unique-collision? e))
                      (throw e)
                      (raise "Fail to transact to LMDB: " e {}))))))]
        (if (Thread/holdsLock write-txn)
          (do-transact prepared-one-shot)
          (locking write-txn
            (do-transact prepared-one-shot))))))

  (set-env-flags [_ ks on-off] (.setFlags env (buffer/kv-flags ks) (if on-off 1 0)))

  (get-env-flags [_] (buffer/env-flag-keys (.getFlags env)))

  (sync [_] (.sync env 1))
  (sync [_ force] (.sync env force))

  (get-value [this dbi-name k]
    (.get-value this dbi-name k :data :data true))
  (get-value [this dbi-name k k-type]
    (.get-value this dbi-name k k-type :data true))
  (get-value [this dbi-name k k-type v-type]
    (.get-value this dbi-name k k-type v-type true))
  (get-value [this dbi-name k k-type v-type ignore-key?]
    (scan/get-value this dbi-name k k-type v-type ignore-key?))

  (get-rank [this dbi-name k]
    (.get-rank this dbi-name k :data))
  (get-rank [this dbi-name k k-type]
    (scan/get-rank this dbi-name k k-type))

  (get-by-rank [this dbi-name rank]
    (.get-by-rank this dbi-name rank :data :data true))
  (get-by-rank [this dbi-name rank k-type]
    (.get-by-rank this dbi-name rank k-type :data true))
  (get-by-rank [this dbi-name rank k-type v-type]
    (.get-by-rank this dbi-name rank k-type v-type true))
  (get-by-rank [this dbi-name rank k-type v-type ignore-key?]
    (scan/get-by-rank this dbi-name rank k-type v-type ignore-key?))

  (sample-kv [this dbi-name n]
    (.sample-kv this dbi-name n :data :data true))
  (sample-kv [this dbi-name n k-type]
    (.sample-kv this dbi-name n k-type :data true))
  (sample-kv [this dbi-name n k-type v-type]
    (.sample-kv this dbi-name n k-type v-type true))
  (sample-kv [this dbi-name n k-type v-type ignore-key?]
    (scan/sample-kv this dbi-name n k-type v-type ignore-key?))

  (get-first [this dbi-name k-range]
    (.get-first this dbi-name k-range :data :data false))
  (get-first [this dbi-name k-range k-type]
    (.get-first this dbi-name k-range k-type :data false))
  (get-first [this dbi-name k-range k-type v-type]
    (.get-first this dbi-name k-range k-type v-type false))
  (get-first [this dbi-name k-range k-type v-type ignore-key?]
    (scan/get-first this dbi-name k-range k-type v-type ignore-key?))

  (get-first-n [this dbi-name n k-range]
    (.get-first-n this dbi-name n k-range :data :data false))
  (get-first-n [this dbi-name n k-range k-type]
    (.get-first-n this dbi-name n k-range k-type :data false))
  (get-first-n [this dbi-name n k-range k-type v-type]
    (.get-first-n this dbi-name n k-range k-type v-type false))
  (get-first-n [this dbi-name n k-range k-type v-type ignore-key?]
    (scan/get-first-n this dbi-name n k-range k-type v-type ignore-key?))

  (get-range [this dbi-name k-range]
    (.get-range this dbi-name k-range :data :data false))
  (get-range [this dbi-name k-range k-type]
    (.get-range this dbi-name k-range k-type :data false))
  (get-range [this dbi-name k-range k-type v-type]
    (.get-range this dbi-name k-range k-type v-type false))
  (get-range [this dbi-name k-range k-type v-type ignore-key?]
    (scan/get-range this dbi-name k-range k-type v-type ignore-key?))

  (key-range [this dbi-name k-range]
    (.key-range this dbi-name k-range :data))
  (key-range [this dbi-name k-range k-type]
    (scan/key-range this dbi-name k-range k-type))

  (visit-key-range [this dbi-name visitor k-range]
    (.visit-key-range this dbi-name visitor k-range :data true))
  (visit-key-range [this dbi-name visitor k-range k-type]
    (.visit-key-range this dbi-name visitor k-range k-type true))
  (visit-key-range [this dbi-name visitor k-range k-type raw-pred?]
    (scan/visit-key-range this dbi-name visitor k-range k-type raw-pred?))

  (key-range-count [lmdb dbi-name k-range]
    (.key-range-count lmdb dbi-name k-range :data))
  (key-range-count [lmdb dbi-name [range-type k1 k2] k-type]
    (scan/scan lmdb dbi-name
      (let [^RangeContext ctx (buffer/key-range-info* (.-key-codec ^DBI dbi) rtx
                                              range-type k1 k2 k-type)
            forward?          (.-forward? ctx)
            lower             (if forward? (.-start-bf ctx) (.-stop-bf ctx))
            upper             (if forward? (.-stop-bf ctx) (.-start-bf ctx))
            include-lower?    (if forward? (.-include-start? ctx) (.-include-stop? ctx))
            include-upper?    (if forward? (.-include-stop? ctx) (.-include-start? ctx))
            flag              (BitOps/intOr
                                (if include-lower? (int DTLV/MDB_COUNT_LOWER_INCL) 0)
                                (if include-upper? (int DTLV/MDB_COUNT_UPPER_INCL) 0))]
        (with-open [total (LongPointer. 1)]
          (DTLV/mdb_range_count_keys
            (.get ^Txn (.-txn ^Rtx rtx)) (.get ^Dbi (.-db ^DBI dbi))
            (iter/dtlv-val lower) (iter/dtlv-val upper) flag total)
          (.get ^LongPointer total)))
      (raise "Fail to count key range: " e {:dbi dbi-name})))

  (range-seq [this dbi-name k-range]
    (.range-seq this dbi-name k-range :data :data false nil))
  (range-seq [this dbi-name k-range k-type]
    (.range-seq this dbi-name k-range k-type :data false nil))
  (range-seq [this dbi-name k-range k-type v-type]
    (.range-seq this dbi-name k-range k-type v-type false nil))
  (range-seq [this dbi-name k-range k-type v-type ignore-key?]
    (.range-seq this dbi-name k-range k-type v-type ignore-key? nil))
  (range-seq [this dbi-name k-range k-type v-type ignore-key? opts]
    (scan/range-seq this dbi-name k-range k-type v-type ignore-key? opts))

  (range-count [this dbi-name k-range]
    (.range-count this dbi-name k-range :data))
  (range-count [lmdb dbi-name k-range k-type]
    (let [dupsort? (.-dupsort? ^DBI (.get dbis dbi-name))]
      (if dupsort?
        (.list-range-count lmdb dbi-name k-range k-type)
        (.key-range-count lmdb dbi-name k-range k-type))))

  (get-some [this dbi-name pred k-range]
    (.get-some this dbi-name pred k-range :data :data false true))
  (get-some [this dbi-name pred k-range k-type]
    (.get-some this dbi-name pred k-range k-type :data false true))
  (get-some [this dbi-name pred k-range k-type v-type]
    (.get-some this dbi-name pred k-range k-type v-type false true))
  (get-some [this dbi-name pred k-range k-type v-type ignore-key?]
    (.get-some this dbi-name pred k-range k-type v-type ignore-key? true))
  (get-some [this dbi-name pred k-range k-type v-type ignore-key? raw-pred?]
    (scan/get-some this dbi-name pred k-range k-type v-type ignore-key?
                   raw-pred?))

  (range-filter [this dbi-name pred k-range]
    (.range-filter this dbi-name pred k-range :data :data false true))
  (range-filter [this dbi-name pred k-range k-type]
    (.range-filter this dbi-name pred k-range k-type :data false true))
  (range-filter [this dbi-name pred k-range k-type v-type]
    (.range-filter this dbi-name pred k-range k-type v-type false true))
  (range-filter [this dbi-name pred k-range k-type v-type ignore-key?]
    (.range-filter this dbi-name pred k-range k-type v-type ignore-key? true))
  (range-filter [this dbi-name pred k-range k-type v-type ignore-key? raw-pred?]
    (scan/range-filter this dbi-name pred k-range k-type v-type ignore-key?
                       raw-pred?))

  (range-keep [this dbi-name pred k-range]
    (.range-keep this dbi-name pred k-range :data :data true))
  (range-keep [this dbi-name pred k-range k-type]
    (.range-keep this dbi-name pred k-range k-type :data true))
  (range-keep [this dbi-name pred k-range k-type v-type]
    (.range-keep this dbi-name pred k-range k-type v-type true))
  (range-keep [this dbi-name pred k-range k-type v-type raw-pred?]
    (scan/range-keep this dbi-name pred k-range k-type v-type raw-pred?))

  (range-some [this dbi-name pred k-range]
    (.range-some this dbi-name pred k-range :data :data true))
  (range-some [this dbi-name pred k-range k-type]
    (.range-some this dbi-name pred k-range k-type :data true))
  (range-some [this dbi-name pred k-range k-type v-type]
    (.range-some this dbi-name pred k-range k-type v-type true))
  (range-some [this dbi-name pred k-range k-type v-type raw-pred?]
    (scan/range-some this dbi-name pred k-range k-type v-type raw-pred?))

  (range-filter-count [this dbi-name pred k-range]
    (.range-filter-count this dbi-name pred k-range :data :data true))
  (range-filter-count [this dbi-name pred k-range k-type]
    (.range-filter-count this dbi-name pred k-range k-type :data true))
  (range-filter-count [this dbi-name pred k-range k-type v-type]
    (.range-filter-count this dbi-name pred k-range k-type v-type true))
  (range-filter-count [this dbi-name pred k-range k-type v-type raw-pred?]
    (scan/range-filter-count this dbi-name pred k-range k-type v-type raw-pred?))

  (visit [this dbi-name visitor k-range]
    (.visit this dbi-name visitor k-range :data :data true))
  (visit [this dbi-name visitor k-range k-type]
    (.visit this dbi-name visitor k-range k-type :data true))
  (visit [this dbi-name visitor k-range k-type v-type]
    (.visit this dbi-name visitor k-range k-type v-type true))
  (visit [this dbi-name visitor k-range k-type v-type raw-pred?]
    (scan/visit this dbi-name visitor k-range k-type v-type raw-pred?))

  (visit-key-sample
    [db dbi-name indices visitor k-range k-type]
    (.visit-key-sample db dbi-name indices visitor k-range k-type true))
  (visit-key-sample
    [db dbi-name indices visitor k-range k-type raw-pred?]
    (scan/visit-key-sample db dbi-name indices visitor k-range k-type raw-pred?))

  (open-list-dbi [this dbi-name {:keys [key-size val-size flags]
                                 :or   {key-size c/+max-key-size+
                                        val-size c/+max-key-size+
                                        flags    c/default-dbi-flags} :as opts}]
    (.check-ready this)
    (assert (and (>= c/+max-key-size+ ^long key-size)
                 (>= c/+max-key-size+ ^long val-size))
            "Data size cannot be larger than 511 bytes")
    (.open-dbi this dbi-name
               (merge (select-keys opts [:key-type :value-type])
                      {:key-size key-size :val-size val-size
                       :flags    (conj flags :dupsort)})))
  (open-list-dbi [lmdb dbi-name]
    (.open-list-dbi lmdb dbi-name nil))

  IList
  (list-dbi? [this dbi-name]
    (get-in (.dbi-opts this dbi-name) [:flags :dupsort]))
  (put-list-items [this dbi-name k vs kt vt]
    (.transact-kv this [(l/kv-tx :put-list dbi-name k vs kt vt)]))

  (del-list-items [this dbi-name k kt]
    (.transact-kv this [(l/kv-tx :del dbi-name k kt)]))
  (del-list-items [this dbi-name k vs kt vt]
    (.transact-kv this [(l/kv-tx :del-list dbi-name k vs kt vt)]))

  (get-list [this dbi-name k kt vt]
    (scan/get-list this dbi-name k kt vt))

  (visit-list [this dbi-name visitor k kt]
    (.visit-list this dbi-name visitor k kt :data true))
  (visit-list [this dbi-name visitor k kt vt]
    (.visit-list this dbi-name visitor k kt vt true))
  (visit-list [this dbi-name visitor k kt vt raw-pred?]
    (scan/visit-list this dbi-name visitor k kt vt raw-pred?))

  (list-count [lmdb dbi-name k kt]
    (.check-ready lmdb)
    (if k
      (scan/scan lmdb dbi-name
        (list-count* rtx cur k kt)
        (raise "Fail to count list: " e {:dbi dbi-name :k k}))
      0))

  (near-list [lmdb dbi-name k v kt vt]
    (.check-ready lmdb)
    (scan/scan lmdb dbi-name
      (near-list* dbi rtx cur k kt v vt)
      (raise "Fail to get an item that is near in a list: "
             e {:dbi dbi-name :k k :v v})))

  (in-list? [lmdb dbi-name k v kt vt]
    (.check-ready lmdb)
    (if (and k v)
      (scan/scan lmdb dbi-name
        (in-list?* dbi rtx cur k kt v vt)
        (raise "Fail to test if an item is in list: "
               e {:dbi dbi-name :k k :v v}))
      false))

  (key-range-list-count [lmdb dbi-name k-range k-type]
    (key-range-list-count-fast lmdb dbi-name k-range k-type))

  (list-range [this dbi-name k-range kt v-range vt]
    (scan/list-range this dbi-name k-range kt v-range vt))

  (list-range-count [lmdb dbi-name k-range k-type]
    (key-range-list-count-fast lmdb dbi-name k-range k-type))

  (list-range-first [this dbi-name k-range kt v-range vt]
    (scan/list-range-first this dbi-name k-range kt v-range vt))

  (list-range-first-n [this dbi-name n k-range kt v-range vt]
    (scan/list-range-first-n this dbi-name n k-range kt v-range vt))

  (list-range-filter [this dbi-name pred k-range kt v-range vt]
    (.list-range-filter this dbi-name pred k-range kt v-range vt true))
  (list-range-filter [this dbi-name pred k-range kt v-range vt raw-pred?]
    (scan/list-range-filter this dbi-name pred k-range kt v-range vt raw-pred?))

  (list-range-keep [this dbi-name pred k-range kt v-range vt]
    (.list-range-keep this dbi-name pred k-range kt v-range vt true))
  (list-range-keep [this dbi-name pred k-range kt v-range vt raw-pred?]
    (scan/list-range-keep this dbi-name pred k-range kt v-range vt raw-pred?))

  (list-range-some [this list-name pred k-range k-type v-range v-type]
    (.list-range-some this list-name pred k-range k-type v-range v-type
                      true))
  (list-range-some [this dbi-name pred k-range kt v-range vt raw-pred?]
    (scan/list-range-some this dbi-name pred k-range kt v-range vt raw-pred?))

  (list-range-filter-count
    [this list-name pred k-range k-type v-range v-type]
    (.list-range-filter-count this list-name pred k-range k-type v-range
                              v-type true))
  (list-range-filter-count
    [this dbi-name pred k-range kt v-range vt raw-pred?]
    (scan/list-range-filter-count this dbi-name pred k-range kt v-range
                                  vt raw-pred?))

  (visit-list-range
    [this list-name visitor k-range k-type v-range v-type]
    (.visit-list-range this list-name visitor k-range k-type v-range
                       v-type true))
  (visit-list-range
    [this dbi-name visitor k-range kt v-range vt raw-pred?]
    (scan/visit-list-range this dbi-name visitor k-range kt v-range
                           vt raw-pred?))

  (visit-list-key-range
    [this dbi-name visitor k-range k-type v-type]
    (.visit-list-key-range this dbi-name visitor k-range k-type
                           v-type true))
  (visit-list-key-range
    [this dbi-name visitor k-range k-type v-type raw-pred?]
    (scan/visit-list-key-range this dbi-name visitor k-range k-type
                               v-type raw-pred?))

  (visit-list-sample
    [this list-name indices visitor k-range k-type v-type]
    (.visit-list-sample this list-name indices visitor k-range k-type v-type true))
  (visit-list-sample
    [this dbi-name indices visitor k-range kt vt raw-pred?]
    (scan/visit-list-sample this dbi-name indices visitor k-range kt vt raw-pred?))

  IAdmin
  (re-index [this opts] (l/re-index* this opts)))

(defn invalidate-thread-reader!
  [lmdb]
  (when (instance? CppLMDB lmdb)
    (let [^CppLMDB lmdb lmdb
          ^ThreadLocal tl-reader (.-tl-reader lmdb)
          ^ConcurrentHashMap reader-registry (.-reader-registry lmdb)
          thread (Thread/currentThread)]
      (when-let [^Rtx rtx (.get tl-reader)]
        (.remove reader-registry thread rtx)
        (close-rtx-quiet! rtx)
        (.remove tl-reader))))
  nil)

(defn- key-range-list-count-fast
  [lmdb dbi-name [range-type k1 k2] k-type]
  (scan/scan lmdb dbi-name
   (let [^RangeContext ctx (buffer/key-range-info* (.-key-codec ^DBI dbi) rtx
                                          range-type k1 k2 k-type)
         forward? (.-forward? ctx)
          ;; mdb_range_count_values expects (lower, upper) in ascending order.
          ;; For forward ranges: start-bf=lower, stop-bf=upper.
          ;; For backward ranges: start-bf=upper, stop-bf=lower, so swap.
         lower (if forward? (.-start-bf ctx) (.-stop-bf ctx))
         upper (if forward? (.-stop-bf ctx) (.-start-bf ctx))
         include-lower? (if forward? (.-include-start? ctx) (.-include-stop? ctx))
         include-upper? (if forward? (.-include-stop? ctx) (.-include-start? ctx))
         flag (BitOps/intOr
               (if include-lower? (int DTLV/MDB_COUNT_LOWER_INCL) 0)
               (if include-upper? (int DTLV/MDB_COUNT_UPPER_INCL) 0))]
     (with-open [ptr (LongPointer. 1)]
       (DTLV/mdb_range_count_values
        (.get ^Txn (.-txn ^Rtx rtx)) (.get ^Dbi (.-db ^DBI dbi))
        (iter/dtlv-val lower) (iter/dtlv-val upper)
        flag ptr)
       (.get ^LongPointer ptr)))
   (raise "Fail to count list in key range: " e {:dbi dbi-name})))


(defn- open-kv*
  [dir dir-file db-file {:keys [mapsize max-readers flags max-dbs temp?
                                key-compress val-compress]
                         :or {max-readers c/*max-readers*
                              max-dbs c/*max-dbs*
                              mapsize c/*init-db-size*
                              flags c/default-env-flags
                              temp? false}
                         :as opts}]
  (let [runtime-opts      (:runtime-opts opts)
        opts             (dissoc opts :runtime-opts :compression)
        opened           (volatile! nil)
        flags            (cond-> flags
                           temp? (conj :nosync))
        _ (when (and (or temp? (some #{:inmemory} flags))
                     (or key-compress val-compress))
            (raise "Compression requires a persistent environment"
                   {:error :compression/persistent-only}))
        ;; Reject a bad dictionary before creating data.mdb without a VERSION
        ;; file. An existing environment must first read its raw manifest.
        prepared (when (and (not temp?) (not (some #{:inmemory} flags))
                            (not (.exists ^File db-file)))
                   (cp/open-compression dir opts {}))
        local-handle-key (lifecycle/reserve-local-kv-handle! dir-file flags)]
    (try
      (let [inmemory? (some #{:inmemory} flags)
          ;; MDB_INMEMORY on Windows expects a simple env identifier instead of
          ;; a filesystem path (which may include ':' or '\').
          env-path (if (and inmemory? (u/windows?))
                     (str "datalevin-inmemory-" (java.util.UUID/randomUUID))
                     dir)
          mapsize (* (long (if (or inmemory? (not (.exists ^File db-file)))
                             mapsize
                             (c/pick-mapsize db-file)))
                     1024 1024)
          flags (cond-> flags
                  inmemory? (conj :nosync))
          ^Env env (Env/create env-path mapsize max-readers max-dbs
                               (buffer/kv-flags flags))
          info (cond-> (merge opts {:dir dir
                                    :flags flags
                                    :max-readers max-readers
                                    :max-dbs max-dbs
                                    :temp? temp?})
                 key-compress (assoc :key-compress key-compress)
                 val-compress (assoc :val-compress val-compress))
          ^CppLMDB lmdb (->CppLMDB env
                                   (volatile! info)
                                   (ThreadLocal.)
                                   (ConcurrentHashMap.)
                                   (HashMap.)
                                   (volatile! nil)
                                   (buffer/new-bufval c/+max-key-size+)
                                   (buffer/new-bufval 0)
                                   (buffer/new-bufval c/+max-key-size+)
                                   (buffer/new-bufval c/+max-key-size+)
                                   (buffer/new-bufval c/+max-key-size+)
                                   (buffer/new-bufval c/+max-key-size+)
                                   (bf/allocate-buffer c/+max-key-size+)
                                   nil
                                   (volatile! nil)
                                   false
                                   nil
                                   nil
                                   nil)]
        (vreset! opened lmdb)
        ;; Spill collections may outlive the application database until GC.
        ;; Their synchronous temporary stores must not keep global executors
        ;; alive after the last application database closes.
        (when-not (:spill? opts)
          (swap! l/lmdb-dirs conj dir))
        (open-dbi lmdb c/kv-info) ;; never compressed
        (cond
          inmemory? nil
          temp? (u/delete-on-exit dir-file)
          :else
          (let [loaded-info (when (pos? ^long (i/entries lmdb c/kv-info))
                              (open/load-info-from-kv lmdb))
                {:keys [manifest key-codec value-codec key-compress val-compress]}
                (or (when (empty? loaded-info) prepared)
                    (cp/open-compression dir opts loaded-info))
                merged-info (assoc (merge loaded-info info)
                                   :dbis (:dbis loaded-info)
                                   :compression manifest
                                   :key-compress key-compress
                                   :val-compress val-compress)
                merged-info (open/retain-wal-durability-profile!
                              lmdb loaded-info merged-info)]
            (if (empty? loaded-info)
              (vreset! (.-info lmdb) (open/init-info lmdb merged-info))
              (do
                (vreset! (.-info lmdb) merged-info)
                (when-not (:compression loaded-info)
                  (transact-kv lmdb [[:put c/kv-info :compression manifest]]))))
            (set-key-compressor lmdb key-codec)
            (set-val-compressor lmdb value-codec)
            (lifecycle/register-shutdown-hook!
              dir (Thread. #(lifecycle/run-shutdown-close! dir lmdb)))
            (lifecycle/start-scheduled-sync (.-scheduled-sync lmdb) dir env)))
        ;; Every environment needs the write transaction's value scratch buffer,
        ;; including in-memory and temporary stores that skip persisted metadata.
        (set-max-val-size lmdb (max-val-size lmdb))
        ;; Runtime state is installed after persistence/loading and before the
        ;; handle is published. It is shared by marked-write views and omitted
        ;; from env-opts and stored metadata.
        (vswap! (.-info lmdb) assoc :custom-type-cache (atom {})
                :runtime-opts runtime-opts)
        (lifecycle/register-local-kv-handle! local-handle-key (l/wrap-open-kv lmdb)))
      (catch Exception e
        (when-let [lmdb @opened]
          (try (close-kv lmdb) (catch Throwable _)))
        (lifecycle/release-local-kv-handle! local-handle-key)
        (raise "Fail to open database: " e {:dir dir})))))

(defmethod open-kv :cpp
  ([dir] (open-kv dir {}))
  ([dir opts]
   (let [migration-kv-types (:migration-kv-types opts)
         opts (c/canonicalize-wal-opts (dissoc opts :migration-kv-types))
         inmemory? (or (nil? dir)
                       (:inmemory? opts)
                       (some #{:inmemory} (:flags opts)))
         dir (or dir (str (u/tmp-dir) (java.util.UUID/randomUUID)))
         _ (assert (string? dir) "directory should be a string.")
         dir-file (u/file dir)
         db-file (io/file dir c/data-file-name)
         opts (if inmemory?
                (update opts :flags (fnil conj c/default-env-flags) :inmemory)
                opts)]
     (if inmemory?
       (open-kv* dir dir-file db-file opts)
       (let [exist-db? (.exists db-file)
             version (read-version-file dir-file)]
         (cond
           (not exist-db?)
           (let [lmdb (open-kv* dir dir-file db-file opts)]
             (write-version-file dir-file c/version)
             lmdb)
           version
           (do
             (when (not= version c/version)
               (if-let [{:keys [major minor patch] :as stored-version}
                        (c/parse-version version)]
                 (let [current-version (c/parse-version c/version)
                       order (long
                               (c/compare-storage-versions stored-version
                                                           current-version))]
                   (cond
                     (neg? order)
                     (do
                       (when c/require-migration?
                         (m/perform-migration dir major minor patch migration-kv-types))
                       (write-version-file dir-file c/version))

                     (pos? order)
                     (raise "Database was opened by a newer Datalevin version"
                            {:database-version version
                             :current-version  c/version
                             :dir              dir})

                     :else
                     (write-version-file dir-file c/version)))
                 (raise "Corrupt VERSION file" {:input version})))
             (open-kv* dir dir-file db-file opts))
           :else
           (raise "Database requires migration. Please follow instruction at https://github.com/datalevin/datalevin/blob/master/doc/upgrade.md"
                  {:dir dir})))))))
