;; ;; Copyright (c) Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns ^:no-doc datalevin.binding.cpp
  "Native binding using JavaCPP"
  (:refer-clojure :exclude [sync])
  (:require
   [clojure.java.io :as io]
   [clojure.string :as s]
   [datalevin.bits :as b]
   [datalevin.util :as u :refer [raise]]
   [datalevin.constants :as c]
   [datalevin.compress :as cp]
   [datalevin.buffer :as bf]
   [datalevin.async :as a]
   [datalevin.migrate :as m]
   [datalevin.validate :as vld]
   [datalevin.scan :as scan]
   [datalevin.interface :as i
    :refer [IList ILMDB IAdmin open-dbi close-kv env-dir close-vecs
            transact-kv get-range range-filter stat key-compressor
            val-compressor set-max-val-size max-val-size
            set-key-compressor set-val-compressor
            bf-compress bf-uncompress]]
   [datalevin.lmdb :as l
    :refer [IBuffer IRange IRtx IDB IKV IWriting ICompress
            IListRandKeyValIterable IListRandKeyValIterator]])
  (:import
   [datalevin.dtlvnative DTLV DTLV$MDB_envinfo DTLV$MDB_stat DTLV$dtlv_key_iter
    DTLV$dtlv_list_iter DTLV$dtlv_list_rank_sample_iter
    DTLV$dtlv_list_val_full_iter DTLV$MDB_val
    DTLV$dtlv_list_key_range_full_val_iter
    DTLV$dtlv_key_rank_sample_iter]
   [datalevin.cpp BufVal Env Txn Dbi Cursor Stat Info Util Util$MapFullException
    Util$DTLVException]
   [datalevin.lmdb RangeContext KVTxData]
   [datalevin.async IAsyncWork]
   [datalevin.utl BitOps]
   [java.util.concurrent TimeUnit ScheduledExecutorService
    ScheduledFuture]
   [java.lang AutoCloseable]
   [java.io File]
   [java.util Iterator HashMap ArrayDeque UUID]
   [java.util.function Supplier]
   [java.nio BufferOverflowException ByteBuffer]
   [org.bytedeco.javacpp SizeTPointer LongPointer]
   [clojure.lang IObj]))

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

(defprotocol IPool
  (pool-add [_ x])
  (pool-take [_]))

(deftype Pool [^ThreadLocal que]
  IPool
  (pool-add [_ x] (.add ^ArrayDeque (.get que) x))
  (pool-take [_] (.poll ^ArrayDeque (.get que))))

(defn- new-pools
  []
  (Pool. (ThreadLocal/withInitial
           (reify Supplier
             (get [_] (ArrayDeque.))))))

(defn- new-bufval [size] (BufVal. size))

(defn- flag-value
  "flag key to int value, cover all flags"
  [k]
  (case k
    :fixedmap   DTLV/MDB_FIXEDMAP
    :nosubdir   DTLV/MDB_NOSUBDIR
    :rdonly-env DTLV/MDB_RDONLY
    :writemap   DTLV/MDB_WRITEMAP
    :nometasync DTLV/MDB_NOMETASYNC
    :nosync     DTLV/MDB_NOSYNC
    :mapasync   DTLV/MDB_MAPASYNC
    :notls      DTLV/MDB_NOTLS
    :nolock     DTLV/MDB_NOLOCK
    :nordahead  DTLV/MDB_NORDAHEAD
    :nomeminit  DTLV/MDB_NOMEMINIT
    :inmemory   DTLV/MDB_INMEMORY

    :cp-compact DTLV/MDB_CP_COMPACT

    :reversekey         DTLV/MDB_REVERSEKEY
    :dupsort            DTLV/MDB_DUPSORT
    :integerkey         DTLV/MDB_INTEGERKEY
    :dupfixed           DTLV/MDB_DUPFIXED
    :integerdup         DTLV/MDB_INTEGERDUP
    :reversedup         DTLV/MDB_REVERSEDUP
    :create             DTLV/MDB_CREATE
    :prefix-compression DTLV/MDB_PREFIX_COMPRESSION
    :counted            DTLV/MDB_COUNTED

    :nooverwrite DTLV/MDB_NOOVERWRITE
    :nodupdata   DTLV/MDB_NODUPDATA
    :current     DTLV/MDB_CURRENT
    :reserve     DTLV/MDB_RESERVE
    :append      DTLV/MDB_APPEND
    :appenddup   DTLV/MDB_APPENDDUP
    :multiple    DTLV/MDB_MULTIPLE

    :rdonly-txn DTLV/MDB_RDONLY))

(defn- kv-flags
  [flags]
  (if (seq flags)
    (reduce (fn [r f] (bit-or ^int r ^int f))
            0 (mapv flag-value flags))
    (int 0)))

(defonce env-flag-map
  {0x01      :fixedmap
   0x4000    :nosubdir
   0x10000   :nosync
   0x20000   :rdonly-env
   0x40000   :nometasync
   0x80000   :writemap
   0x100000  :mapasync
   0x200000  :notls
   0x400000  :nolock
   0x800000  :nordahead
   0x1000000 :nomeminit
   DTLV/MDB_INMEMORY :inmemory})

(defn- env-flag-keys
  [v]
  (reduce-kv
    (fn [s i k]
      (if (not= 0 (bit-and ^int i ^int v))
        (conj s k)
        s))
    #{} env-flag-map))

(defn- put-bufval
  [^BufVal vp k kt compressor ^ByteBuffer cbf]
  (when-some [x k]
    (let [^ByteBuffer bf (.inBuf vp)]
      (.clear bf)
      (if compressor
        (do (b/put-buffer (.clear cbf) x kt)
            (bf-compress compressor (.flip cbf) bf))
        (b/put-buffer bf x kt))
      (.flip bf)
      (.reset vp))))

(deftype Rtx [lmdb
              ^Txn txn
              depth
              ^BufVal kp
              ^BufVal vp
              ^BufVal start-kp
              ^BufVal stop-kp
              ^BufVal start-vp
              ^BufVal stop-vp
              ^ByteBuffer k-comp-bf
              ^:volatile-mutable ^ByteBuffer v-comp-bf
              aborted?
              wal-ops
              kv-overlay-private]

  ICompress
  (key-bf [_] (.clear k-comp-bf))
  (val-bf [_] (.clear v-comp-bf))

  IBuffer
  (put-key [_ x t]
    (try
      (put-bufval kp x t (key-compressor lmdb) k-comp-bf)
      (catch BufferOverflowException _
        (raise "Key cannot be larger than 511 bytes." {:input x}))
      (catch Exception e
        (raise "Error putting read-only transaction key buffer: "
               e {:value x :type t}))))
  (put-val [_ _ _]
    (raise "put-val not allowed for read only txn buffer" {}))

  IRange
  (range-info [_ range-type k1 k2 kt]
    (put-bufval start-kp k1 kt (key-compressor lmdb) k-comp-bf)
    (put-bufval stop-kp k2 kt (key-compressor lmdb) k-comp-bf)
    (l/range-table range-type start-kp stop-kp))

  (list-range-info [_ k-range-type k1 k2 kt v-range-type v1 v2 vt]
    (put-bufval start-kp k1 kt (key-compressor lmdb) k-comp-bf)
    (put-bufval stop-kp k2 kt (key-compressor lmdb) k-comp-bf)
    (put-bufval start-vp v1 vt (val-compressor lmdb) v-comp-bf)
    (put-bufval stop-vp v2 vt (val-compressor lmdb) v-comp-bf)
    [(l/range-table k-range-type start-kp stop-kp)
     (l/range-table v-range-type start-vp stop-vp)])

  IRtx
  (read-only? [_] (.isReadOnly txn))
  (txn [_] txn)
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
  (private-overlay [this] (.-kv-overlay-private this))
  (kp [this] (.-kp this))
  (vp [this] (.-vp this)))

(defn- v-bf
  [^BufVal vp lmdb rtx]
  (let [bf (.outBuf vp)]
    (if-let [compressor (val-compressor lmdb)]
      (let [^ByteBuffer cbf (l/val-bf rtx)]
        (bf-uncompress compressor bf cbf)
        (.flip cbf))
      bf)))

(deftype KV [^BufVal kp ^BufVal vp lmdb rtx]
  IKV
  (k [_]
    (let [bf (.outBuf kp)]
      (if-let [compressor (key-compressor lmdb)]
        (let [^ByteBuffer cbf (l/key-bf rtx)]
          (bf-uncompress compressor bf cbf)
          (.flip cbf))
        bf)))

  (v [_] (v-bf vp lmdb rtx)))

(defn- stat-map [^Stat stat]
  (let [^DTLV$MDB_stat s (.get stat)]
    {:psize          (.ms_psize s)
     :depth          (.ms_depth s)
     :branch-pages   (.ms_branch_pages s)
     :leaf-pages     (.ms_leaf_pages s)
     :overflow-pages (.ms_overflow_pages s)
     :entries        (.ms_entries s)}))

(declare ->KeyIterable ->KeySampleIterable ->ListIterable
         ->ListFullValIterable ->ListSampleIterable
         ->ListKeyRangeFullValIterable
         maybe-flush-kv-indexer-on-pressure!)

(defn- val-size
  [x]
  (let [^long val-size (b/measure-size x)]
    (if (< Integer/MAX_VALUE val-size)
      (raise "Value size is too large" {:size val-size})
      (let [try-size (* ^long c/+buffer-grow-factor+ val-size)]
        (if (< Integer/MAX_VALUE try-size)
          val-size
          try-size)))))

(deftype DBI [lmdb
              ^Dbi db
              ^Pool curs
              ^BufVal kp
              ^:volatile-mutable ^BufVal vp
              ^ByteBuffer k-comp-bf
              ^:volatile-mutable ^ByteBuffer v-comp-bf
              ^boolean dupsort?
              ^boolean counted?
              ^boolean validate-data?]
  IBuffer
  (put-key [this x t]
    (try
      (put-bufval kp x t (key-compressor lmdb) k-comp-bf)
      (catch BufferOverflowException _
        (raise "Key cannot be larger than 511 bytes." {:input x}))
      (catch Exception e
        (raise "Error putting r/w key buffer of "
               (.dbi-name this)": " e {:value x :type t}))))
  (put-val [this x t]
    (try
      (put-bufval vp x t (val-compressor lmdb) v-comp-bf)
      (catch BufferOverflowException _
        (let [size (val-size x)]
          (set! vp (new-bufval size))
          (set! v-comp-bf (bf/allocate-buffer size))
          (set-max-val-size lmdb size)
          (put-bufval vp x t (val-compressor lmdb) v-comp-bf)))
      (catch Exception e
        (raise "Error putting r/w value buffer of "
               (.dbi-name this)": " e {:value x :type t}))))

  IDB
  (dbi [_] db)
  (dbi-name [_] (.getName db))
  (put [_ txn flags] (.put db txn kp vp (kv-flags flags)))
  (put [this txn] (.put this txn nil))
  (del [_ txn all?] (if all? (.del db txn kp nil) (.del db txn kp vp)))
  (del [this txn] (.del this txn true))
  (get-kv [_ rtx]
    (let [^BufVal kp (.-kp ^Rtx rtx)
          ^BufVal vp (.-vp ^Rtx rtx)
          rc         (DTLV/mdb_get (.get ^Txn (.-txn ^Rtx rtx))
                                   (.get db) (.ptr kp) (.ptr vp))]
      (Util/checkRc ^int rc)
      (when-not (= rc DTLV/MDB_NOTFOUND)
        (.outBuf vp))))
  (get-key-rank [_ rtx]
    (let [^BufVal kp      (.-kp ^Rtx rtx)
          ^LongPointer rp (LongPointer. 1)
          rc              (DTLV/mdb_get_key_rank
                            (.get ^Txn (.-txn ^Rtx rtx))
                            (.get db) (.ptr kp) nil rp)]
      (Util/checkRc ^int rc)
      (when-not (= rc DTLV/MDB_NOTFOUND)
        (.get rp))))
  (get-key-by-rank [_ rtx rank]
    (let [^BufVal kp (.-kp ^Rtx rtx)
          ^BufVal vp (.-vp ^Rtx rtx)
          rc         (DTLV/mdb_get_rank
                       (.get ^Txn (.-txn ^Rtx rtx))
                       (.get db) rank (.ptr kp) (.ptr vp))]
      (Util/checkRc ^int rc)
      (when-not (= rc DTLV/MDB_NOTFOUND)
        [(.outBuf kp) (.outBuf vp)])))
  (iterate-key [this rtx cur [range-type k1 k2] k-type]
    (let [ctx (l/range-info rtx range-type k1 k2 k-type)]
      (->KeyIterable lmdb this cur rtx ctx)))
  (iterate-key-sample [this rtx cur indices budget step [range-type k1 k2]
                       k-type]
    (let [ctx (l/range-info rtx range-type k1 k2 k-type)]
      (->KeySampleIterable lmdb this indices budget step cur rtx ctx)))
  (iterate-list [this rtx cur [k-range-type k1 k2] k-type
                 [v-range-type v1 v2] v-type]
    (let [ctx (l/list-range-info rtx k-range-type k1 k2 k-type
                                 v-range-type v1 v2 v-type)]
      (->ListIterable lmdb this cur rtx ctx)))
  (iterate-list-sample [this rtx cur indices budget step [k-range-type k1 k2]
                        k-type]
    (let [ctx (l/range-info rtx k-range-type k1 k2 k-type)]
      (->ListSampleIterable lmdb this indices budget step cur rtx ctx)))
  (iterate-list-val-full [this rtx cur]
    (->ListFullValIterable lmdb this cur rtx))
  (iterate-list-key-range-val-full [this rtx cur [range-type k1 k2]
                                    k-type]
    (let [ctx (l/range-info rtx range-type k1 k2 k-type)]
      (->ListKeyRangeFullValIterable lmdb this cur rtx ctx)))
  (iterate-kv [this rtx cur k-range k-type v-type]
    (if dupsort?
      (.iterate-list this rtx cur k-range k-type [:all] v-type)
      (.iterate-key this rtx cur k-range k-type)))
  (get-cursor [_ rtx]
    (let [^Rtx rtx rtx
          ^Txn txn (.-txn rtx)]
      (or (when (.isReadOnly txn)
            (when-let [^Cursor cur (pool-take curs)]
              (.renew cur txn)
              cur))
          ;; Cursor key/value buffers must stay isolated from the transaction
          ;; point-read buffers (kp/vp). Nested point-reads during scans
          ;; otherwise overwrite iterator state.
          (Cursor/create txn db
                         (new-bufval c/+max-key-size+)
                         (new-bufval (i/max-val-size lmdb))))))
  (cursor-count [_ cur] (.count ^Cursor cur))
  (close-cursor [_ cur] (.close ^Cursor cur))
  (return-cursor [_ cur] (pool-add curs cur)))

(defn- dtlv-bool [x] (if x DTLV/DTLV_TRUE DTLV/DTLV_FALSE))

(defn- dtlv-val ^DTLV$MDB_val [x] (when x (.ptr ^BufVal x)))

(defn- dtlv-rc [x]
  (condp = x
    DTLV/DTLV_TRUE  true
    DTLV/DTLV_FALSE false
    (u/raise "Native iterator returns error code" x {})))

(defn- dtlv-c [^long x]
  (if (< x 0)
    (u/raise "Native counter returns error code" x {})
    x))

(deftype KeyIterable [lmdb
                      ^DBI db
                      ^Cursor cur
                      ^Rtx rtx
                      ^RangeContext ctx]
  Iterable
  (iterator [_]
    (let [forward?       (dtlv-bool (.-forward? ctx))
          include-start? (dtlv-bool (.-include-start? ctx))
          include-stop?  (dtlv-bool (.-include-stop? ctx))
          sk             (dtlv-val (.-start-bf ctx))
          ek             (dtlv-val (.-stop-bf ctx))
          k              (.key cur)
          v              (.val cur)
          iter           (DTLV$dtlv_key_iter.)]
      (Util/checkRc
        (DTLV/dtlv_key_iter_create
          iter (.ptr cur) (.ptr k) (.ptr v)
          ^int forward? ^int include-start? ^int include-stop? sk ek))
      (reify
        Iterator
        (hasNext [_] (dtlv-rc (DTLV/dtlv_key_iter_has_next iter)))
        (next [_] (KV. k v lmdb rtx))

        AutoCloseable
        (close [_] (DTLV/dtlv_key_iter_destroy iter))))))

(deftype KeySampleIterable [lmdb
                            ^DBI db
                            ^longs indices
                            ^long budget
                            ^long step
                            ^Cursor cur
                            ^Rtx rtx
                            ^RangeContext ctx]
  Iterable
  (iterator [_]
    (let [sk      (dtlv-val (.-start-bf ctx))
          ek      (dtlv-val (.-stop-bf ctx))
          k       (.key cur)
          v       (.val cur)
          iter    (DTLV$dtlv_key_rank_sample_iter.)
          samples (alength indices)
          sizets  (SizeTPointer. samples)]
      (dotimes [i samples] (.put sizets i (aget indices i)))
      (Util/checkRc
        (DTLV/dtlv_key_rank_sample_iter_create
          ^DTLV$dtlv_key_rank_sample_iter iter
          sizets samples (.ptr cur) (.ptr k) (.ptr v) sk ek))
      (reify
        Iterator
        (hasNext [_]
          (dtlv-rc (DTLV/dtlv_key_rank_sample_iter_has_next iter)))
        (next [_] (KV. k v lmdb rtx))

        AutoCloseable
        (close [_]
          (DTLV/dtlv_key_rank_sample_iter_destroy iter))))))

(deftype ListIterable [lmdb
                       ^DBI db
                       ^Cursor cur
                       ^Rtx rtx
                       ctx]
  Iterable
  (iterator [_]
    (let [[^RangeContext kctx ^RangeContext vctx] ctx

          forward-key?       (dtlv-bool (.-forward? kctx))
          include-start-key? (dtlv-bool (.-include-start? kctx))
          include-stop-key?  (dtlv-bool (.-include-stop? kctx))
          sk                 (dtlv-val (.-start-bf kctx))
          ek                 (dtlv-val (.-stop-bf kctx))
          forward-val?       (dtlv-bool (.-forward? vctx))
          include-start-val? (dtlv-bool (.-include-start? vctx))
          include-stop-val?  (dtlv-bool (.-include-stop? vctx))
          sv                 (dtlv-val (.-start-bf vctx))
          ev                 (dtlv-val (.-stop-bf vctx))
          k                  (.key cur)
          v                  (.val cur)
          iter               (DTLV$dtlv_list_iter.)]
      (Util/checkRc
        (DTLV/dtlv_list_iter_create
          iter (.ptr cur) (.ptr k) (.ptr v)
          ^int forward-key? ^int include-start-key? ^int include-stop-key? sk ek
          ^int forward-val? ^int include-start-val? ^int include-stop-val?
          sv ev))
      (reify
        Iterator
        (hasNext [_] (dtlv-rc (DTLV/dtlv_list_iter_has_next iter)))
        (next [_] (KV. k v lmdb rtx))

        AutoCloseable
        (close [_] (DTLV/dtlv_list_iter_destroy iter))))))

(deftype ListSampleIterable [lmdb
                             ^DBI db
                             ^longs indices
                             ^long budget
                             ^long step
                             ^Cursor cur
                             ^Rtx rtx
                             ^RangeContext ctx]
  Iterable
  (iterator [_]
    (let [sk      (dtlv-val (.-start-bf ctx))
          ek      (dtlv-val (.-stop-bf ctx))
          k       (.key cur)
          v       (.val cur)
          iter    (DTLV$dtlv_list_rank_sample_iter.)
          samples (alength indices)
          sizets  (SizeTPointer. samples)]
      (dotimes [i samples] (.put sizets i (aget indices i)))
      (Util/checkRc
        (DTLV/dtlv_list_rank_sample_iter_create
          ^DTLV$dtlv_list_rank_sample_iter iter
          sizets samples (.ptr cur) (.ptr k) (.ptr v) sk ek))
      (reify
        Iterator
        (hasNext [_]
          (dtlv-rc (DTLV/dtlv_list_rank_sample_iter_has_next iter)))
        (next [_] (KV. k v lmdb rtx))

        AutoCloseable
        (close [_]
          (DTLV/dtlv_list_rank_sample_iter_destroy iter))))))

(deftype ListKeyRangeFullValIterable [lmdb
                                      ^DBI db
                                      ^Cursor cur
                                      ^Rtx rtx
                                      ^RangeContext ctx]
  Iterable
  (iterator [_]
    (let [include-start? (dtlv-bool (.-include-start? ctx))
          include-stop?  (dtlv-bool (.-include-stop? ctx))
          sk             (dtlv-val (.-start-bf ctx))
          ek             (dtlv-val (.-stop-bf ctx))
          k              (.key cur)
          v              (.val cur)
          iter           (DTLV$dtlv_list_key_range_full_val_iter.)]
      (Util/checkRc
        (DTLV/dtlv_list_key_range_full_val_iter_create
          iter (.ptr cur) (.ptr k) (.ptr v)
          ^int include-start? ^int include-stop? sk ek))
      (reify
        Iterator
        (hasNext [_]
          (dtlv-rc (DTLV/dtlv_list_key_range_full_val_iter_has_next iter)))
        (next [_] (KV. k v lmdb rtx))

        AutoCloseable
        (close [_] (DTLV/dtlv_list_key_range_full_val_iter_destroy iter))))))

(deftype ListFullValIterable [lmdb
                              ^DBI db
                              ^Cursor cur
                              ^Rtx rtx]
  IListRandKeyValIterable
  (val-iterator [_]
    (let [^BufVal k (.key cur)
          ^BufVal v (.val cur)
          iter      (DTLV$dtlv_list_val_full_iter.)]
      (Util/checkRc
        (DTLV/dtlv_list_val_full_iter_create iter (.ptr cur) (.ptr k) (.ptr v)))
      (reify
        IListRandKeyValIterator
        (seek-key [_ x t]
          (l/put-key rtx x t)
          (dtlv-rc
            (DTLV/dtlv_list_val_full_iter_seek iter (.ptr ^BufVal (.-kp rtx)))))
        (has-next-val [_]
          (dtlv-rc (DTLV/dtlv_list_val_full_iter_has_next iter)))
        (next-val [_] (v-bf v lmdb rtx))

        AutoCloseable
        (close [_] (DTLV/dtlv_list_val_full_iter_destroy iter))))))

(defn- put-tx
  [^DBI dbi txn ^KVTxData tx]
  (case (.-op tx)
    :put      (do (.put-key dbi (.-k tx) (.-kt tx))
                  (.put-val dbi (.-v tx) (.-vt tx))
                  (if-let [f (.-flags tx)]
                    (.put dbi txn f)
                    (.put dbi txn)))
    :del      (do (.put-key dbi (.-k tx) (.-kt tx))
                  (.del dbi txn))
    :put-list (let [vs (.-v tx)]
                (.put-key dbi (.-k tx) (.-kt tx))
                (doseq [v vs]
                  (.put-val dbi v (.-vt tx))
                  (.put dbi txn)))
    :del-list (let [vs (.-v tx)]
                (.put-key dbi (.-k tx) (.-kt tx))
                (doseq [v vs]
                  (.put-val dbi v (.-vt tx))
                  (.del dbi txn false)))))

(defn- transact1*
  [txs ^DBI dbi txn kt vt]
  (let [validate? (.-validate-data? dbi)]
    (doseq [t txs]
      (let [tx (l/->kv-tx-data t kt vt)]
        (vld/validate-kv-tx-data tx validate?)
        (put-tx dbi txn tx)))))

(defn- transact*
  [txs ^HashMap dbis txn]
  (doseq [t txs]
    (let [^KVTxData tx (l/->kv-tx-data t)
          dbi-name     (.-dbi-name tx)
          ^DBI dbi     (or (.get dbis dbi-name)
                           (raise dbi-name " is not open" {}))
          validate?    (.-validate-data? dbi)]
      (vld/validate-kv-tx-data tx validate?)
      (put-tx dbi txn tx))))

(defn- list-count*
  [^Rtx rtx ^Cursor cur k kt]
  (.put-key rtx k kt)
  (dtlv-c (DTLV/dtlv_list_val_count
            (.ptr cur) (.ptr ^BufVal (.-kp rtx)) (.ptr ^BufVal (.-vp rtx)))))

(defn- in-list?*
  [^Rtx rtx ^Cursor cur k kt v vt]
  (l/list-range-info rtx :at-least k nil kt :at-least v nil vt)
  (.get cur ^BufVal (.-start-kp rtx) ^BufVal (.-start-vp rtx)
        DTLV/MDB_GET_BOTH))

(defn- near-list*
  [^Rtx rtx ^Cursor cur k kt v vt]
  (l/list-range-info rtx :at-least k nil kt :at-least v nil vt)
  (when (.get cur ^BufVal (.-start-kp rtx) ^BufVal (.-start-vp rtx)
              DTLV/MDB_GET_BOTH_RANGE)
    (.outBuf (.val cur))))

(declare ->CppLMDB)

(defn- up-db-size [^Env env]
  (let [^Info info (Info/create env)]
    (.setMapSize env (* ^long c/+buffer-grow-factor+
                        (.me_mapsize ^DTLV$MDB_envinfo (.get info))))
    (.close info)))

(defn- sync-key* [dir] (->> dir hash (str "lmdb-sync-") keyword))

(def sync-key (memoize sync-key*))

(deftype AsyncSync [dir ^Env env]
  IAsyncWork
  (work-key [_] (sync-key dir))
  (do-work [_] (.sync env 1))
  (combine [_] first)
  (callback [_] nil))

(defn- start-scheduled-sync
  [scheduled-sync dir ^Env env]
  (let [scheduler ^ScheduledExecutorService (u/get-scheduler)
        fut       (.scheduleWithFixedDelay
                    scheduler
                    ^Runnable #(let [exe (a/get-executor)]
                                 (when (a/running? exe)
                                   (a/exec exe (AsyncSync. dir env))))
                    ^long (rand-int c/lmdb-sync-interval)
                    ^long c/lmdb-sync-interval
                    TimeUnit/SECONDS)]
    (vreset! scheduled-sync fut)))

(defn- stop-scheduled-sync
  [scheduled-sync]
  (when-let [fut @scheduled-sync]
    (.cancel ^ScheduledFuture fut true)
    (vreset! scheduled-sync nil)))

(defn- copy-version-file
  [lmdb dest]
  (let [src (str (env-dir lmdb) u/+separator+ c/version-file-name)
        dst (str dest u/+separator+ c/version-file-name)]
    (u/copy-file src dst)))

(defn- copy-keycode-file
  [lmdb dest]
  (let [src (str (env-dir lmdb) u/+separator+ c/keycode-file-name)
        dst (str dest u/+separator+ c/keycode-file-name)]
    (when (.exists (io/file src))
      (u/copy-file src dst))))

(declare key-range-list-count-fast)



(deftype CppLMDB [^Env env
                  info
                  ^ThreadLocal tl-reader
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
                  ^boolean kv-wal
                  ^:unsynchronized-mutable meta]

  IWriting
  (writing? [_] writing?)

  (write-txn [_] write-txn)

  (kv-info [_] info)

  (mark-write [_]
    (->CppLMDB
      env info tl-reader dbis scheduled-sync kp-w vp-w start-kp-w
      stop-kp-w start-vp-w stop-vp-w k-comp-bf-w v-comp-bf-w
      write-txn true k-comp v-comp kv-wal meta))

  (reset-write
    [this]
    (.clear kp-w)
    (.clear vp-w)
    (.clear start-kp-w)
    (.clear stop-kp-w)
    (.clear start-vp-w)
    (.clear stop-vp-w)
    (.clear k-comp-bf-w)
    (.clear v-comp-bf-w)
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
                             (volatile! [])
                             (volatile! nil))))

  IObj
  (withMeta [this m] (set! meta m) this)
  (meta [_] meta)

  ILMDB
  (max-val-size [_] (or (:max-val-size @info) c/*init-val-size*))

  (set-max-val-size [_ size]
    (set! v-comp-bf-w (bf/allocate-buffer size))
    (vswap! info assoc :max-val-size size :max-val-size-changed? true))

  (close-kv [this]
    (when-not (.isClosed env)
      (stop-scheduled-sync scheduled-sync)
      (let [dir-id (or (@info :dir-id) (env-dir this))]
        (swap! l/lmdb-dirs disj dir-id))
      (when (zero? (count @l/lmdb-dirs))
        (a/shutdown-executor)
        (u/shutdown-worker-thread-pool)
        (u/shutdown-scheduler))
      ;; Temp envs are ephemeral scratch spaces (e.g. WAL overlays). Forcing a
      ;; durability sync on close only adds latency with no correctness benefit.
      (when-not (or (@info :temp?) (@info :inmemory?))
        (.sync env 1))
      (.close env)
      (when-not (@info :inmemory?)
        (doseq [idx (keep @l/vector-indices (u/list-files (.env-dir this)))]
          (close-vecs idx)))
      (when (and (@info :temp?) (@info :dir))
        (u/delete-files (@info :dir)))
      nil))

  (closed-kv? [_] (.isClosed env))

  (check-ready [this] (assert (not (.closed-kv? this)) "LMDB env is closed."))

  (env-dir [_] (@info :dir))

  (env-opts [_] (dissoc @info :dbis))

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
                                        false)}}]
    (.check-ready this)
    (assert (< ^long key-size 512) "Key size cannot be greater than 511 bytes")
    (let [{info-dbis :dbis max-dbis :max-dbs} @info]
      (if (< (count info-dbis) ^long max-dbis)
        (let [opts     {:key-size       key-size
                        :val-size       val-size
                        :flags          flags
                        :validate-data? validate-data?}
              flags    (set flags)
              dupsort? (if (:dupsort flags) true false)
              counted? (if (:counted flags) true false)
              kp       (new-bufval key-size)
              vp       (new-bufval val-size)
              kc       (bf/allocate-buffer key-size)
              vc       (bf/allocate-buffer val-size)
              dbi      (Dbi/create env dbi-name (kv-flags flags))
              db       (DBI. this dbi (new-pools) kp vp kc vc
                             dupsort? counted? validate-data?)]

          (when (not= dbi-name c/kv-info)
            (vswap! info assoc-in [:dbis dbi-name] opts))
          (.put dbis dbi-name db)
          db)
        (u/raise (str "Reached maximal number of DBI: " max-dbis) {}))))

  (get-dbi [this dbi-name]
    (.get-dbi this dbi-name true))
  (get-dbi [this dbi-name create?]
    (or (.get dbis dbi-name)
        (if create?
          (.open-dbi this dbi-name)
          (u/raise (str "DBI " dbi-name " is not open") {}))))

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
        (.remove dbis dbi-name)
        nil)
      (catch Exception e (raise "Fail to drop DBI: " dbi-name e {}))))

  (list-dbis [_] (keys (@info :dbis)))

  (copy [this dest]
    (.copy this dest false))
  (copy [this dest compact?]
    (if (-> dest u/file u/empty-dir?)
      (do (.copy env dest (if compact? true false))
          (copy-version-file this dest)
          (copy-keycode-file this dest))
      (raise "Destination directory is not empty." {})))

  (get-rtx [this]
    (when-not (.closed-kv? this)
      (try
        (or (when-not (.isVirtual (Thread/currentThread))
              (when-let [^Rtx rtx (.get tl-reader)]
                (when (<= ^long (.max-val-size this)
                          ^int (.capacity ^ByteBuffer (l/val-bf rtx)))
                  (.renew rtx))))
            (let [rtx (Rtx. this
                            (Txn/createReadOnly env)
                            (volatile! 1)
                            (new-bufval c/+max-key-size+)
                            (new-bufval 0)
                            (new-bufval c/+max-key-size+)
                            (new-bufval c/+max-key-size+)
                            (new-bufval c/+max-key-size+)
                            (new-bufval c/+max-key-size+)
                            (bf/allocate-buffer c/+max-key-size+)
                            (bf/allocate-buffer (.max-val-size this))
                            (volatile! false)
                            (volatile! nil)
                            (volatile! nil))]
              (.set tl-reader rtx)
              rtx))
        (catch Exception e
          (raise "Please do not open multiple LMDB connections to the same DB
           in the same process. Instead, a LMDB connection should be held onto
           and managed like a stateful resource. Refer to the documentation of
           `datalevin.core/open-kv` for more details."
                 {:cause (.getMessage e)})))))

  (return-rtx [this rtx]
    (when-not  (.closed-kv? this)
      (if (.isVirtual (Thread/currentThread))
        (do (.abort ^Txn (.-txn ^Rtx rtx))
            (.remove tl-reader))
        (.reset ^Rtx rtx))))

  (stat [_]
    (try
      (let [stat ^Stat (Stat/create env)
            m    (stat-map stat)]
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
                m          (stat-map stat)]
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
          (let [
                ^Stat stat (Stat/create txn db)
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
          (try
            (if aborted?
              (.abort txn)
              (try
                (.commit txn)
                (catch Util$MapFullException _
                  (.close txn)
                  (up-db-size env)
                  (vreset! write-txn nil)
                  (raise "DB resized" {:resized true}))
                (catch Exception e
                  (.close txn)
                  (vreset! write-txn nil)
                  (raise "Fail to commit read/write transaction in LMDB: "
                         e {}))))
            (vreset! write-txn nil)
            (.close txn)
            (if aborted? :aborted :committed)
            (catch Exception e
              (when-let [^Txn t (.-txn wtxn)]
                (try
                  (.abort t)
                  (catch Exception _ nil))
                (try
                  (.close t)
                  (catch Exception _ nil)))
              (vreset! write-txn nil)
              (if-let [edata (ex-data e)]
                (raise "Fail to close read/write transaction in LMDB: "
                       e edata)
                (raise "Fail to close read/write transaction in LMDB: "
                       e {}))))))
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
    (locking write-txn
      (.check-ready this)
      (let [^Rtx rtx  @write-txn
            one-shot? (nil? rtx)
            ^DBI dbi  (when dbi-name
                        (or (.get dbis dbi-name)
                            (raise dbi-name " is not open" {})))]
        (let [^Txn txn (if one-shot?
                         (Txn/create env)
                         (.-txn rtx))]
          (try
            (if dbi
              (transact1* txs dbi txn k-type v-type)
              (transact* txs dbis txn))
            (when (:max-val-size-changed? @info)
              (transact* [[:put c/kv-info :max-val-size
                           (:max-val-size @info)]]
                         dbis txn)
              (vswap! info assoc :max-val-size-changed? false))
            (when one-shot?
              (.commit txn))
            :transacted
            (catch Util$MapFullException _
              (.close txn)
              (up-db-size env)
              (if one-shot?
                (.transact-kv this dbi-name txs k-type v-type)
                (do
                  (.reset-write this)
                  (raise "DB resized" {:resized true}))))
            (catch Exception e
              (when one-shot?
                (.close txn))
              (if-let [edata (ex-data e)]
                (raise "Fail to transact to LMDB: " e edata)
                (raise "Fail to transact to LMDB: " e {}))))))))

  (set-env-flags [_ ks on-off]
    (when (contains? ks :inmemory)
      (raise "inmemory flag cannot be set or cleared after open" {}))
    (.setFlags env (kv-flags ks) (if on-off 1 0)))

  (get-env-flags [_] (env-flag-keys (.getFlags env)))

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
    (scan/scan
      (let [^RangeContext ctx (l/range-info rtx range-type k1 k2 k-type)
            sk                (dtlv-val (.-start-bf ctx))
            ek                (dtlv-val (.-stop-bf ctx))
            _                 (when-not (l/dlmdb?)
                                (raise "Only dlmdb counted env is supported."
                                       {:dbi dbi-name}))
            flag              (BitOps/intOr
                                (if (.-include-start? ctx)
                                  (int DTLV/MDB_COUNT_LOWER_INCL) 0)
                                (if (.-include-stop? ctx)
                                  (int DTLV/MDB_COUNT_UPPER_INCL) 0))]
        (dtlv-c
          (with-open [total (LongPointer. 1)]
            (DTLV/mdb_range_count_keys
              (.get ^Txn (.-txn ^Rtx rtx)) (.get ^Dbi (.-db ^DBI dbi))
              sk ek flag total)
            (.get ^LongPointer total))))
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
        (.list-range-count lmdb dbi-name k-range k-type [:all] nil)
        (.key-range-count lmdb dbi-name k-range k-type))))

  (get-some [this dbi-name pred k-range]
    (.get-some this dbi-name pred k-range :data :data false true))
  (get-some [this dbi-name pred k-range k-type]
    (.get-some this dbi-name pred k-range k-type :data false true))
  (get-some [this dbi-name pred k-range k-type v-type]
    (.get-some this dbi-name pred k-range k-type v-type false true))
  (get-some [this dbi-name pred k-range k-type v-type ignore-key?]
    (.get-some this dbi-name pred k-range k-type v-type  ignore-key? true))
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
    (.range-filter this dbi-name pred k-range k-type v-type  ignore-key? true))
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
    (scan/range-filter-count this dbi-name pred k-range k-type
                             v-type raw-pred?))

  (visit [this dbi-name visitor k-range]
    (.visit this dbi-name visitor k-range :data :data true))
  (visit [this dbi-name visitor k-range k-type]
    (.visit this dbi-name visitor k-range k-type :data true))
  (visit [this dbi-name visitor k-range k-type v-type]
    (.visit this dbi-name visitor k-range k-type v-type true))
  (visit [this dbi-name visitor k-range k-type v-type raw-pred?]
    (scan/visit this dbi-name visitor k-range k-type v-type raw-pred?))

  (visit-key-sample
    [db dbi-name indices budget step visitor k-range k-type]
    (.visit-key-sample
      db dbi-name indices budget step visitor k-range k-type true))
  (visit-key-sample
    [db dbi-name indices budget step visitor k-range k-type raw-pred?]
    (scan/visit-key-sample
      db dbi-name indices budget step visitor k-range k-type raw-pred?))

  (open-list-dbi [this dbi-name {:keys [key-size val-size flags]
                                 :or   {key-size c/+max-key-size+
                                        val-size c/+max-key-size+
                                        flags    c/default-dbi-flags}}]
    (.check-ready this)
    (assert (and (>= c/+max-key-size+ ^long key-size)
                 (>= c/+max-key-size+ ^long val-size))
            "Data size cannot be larger than 511 bytes")
    (.open-dbi this dbi-name
               {:key-size key-size :val-size val-size
                :flags    (conj flags :dupsort)}))
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
    (do
      (.check-ready lmdb)
      (if k
        (scan/scan
          (list-count* rtx cur k kt)
          (raise "Fail to count list: " e {:dbi dbi-name :k k}))
        0)))

  (near-list [lmdb dbi-name k v kt vt]
    (do
      (.check-ready lmdb)
      (scan/scan
        (near-list* rtx cur k kt v vt)
        (raise "Fail to get an item that is near in a list: "
               e {:dbi dbi-name :k k :v v}))))

  (in-list? [lmdb dbi-name k v kt vt]
    (do
      (.check-ready lmdb)
      (if (and k v)
        (scan/scan
          (in-list?* rtx cur k kt v vt)
          (raise "Fail to test if an item is in list: "
                 e {:dbi dbi-name :k k :v v}))
        false)))

  (key-range-list-count [lmdb dbi-name k-range k-type]
    (when-not (l/dlmdb?)
      (raise "Only dlmdb counted env is supported." {:dbi dbi-name}))
    (key-range-list-count-fast lmdb dbi-name k-range k-type))

  (list-range [this dbi-name k-range kt v-range vt]
    (scan/list-range this dbi-name k-range kt v-range vt))

  (list-range-count [lmdb dbi-name k-range k-type _v-range _v-type]
    ;; Fast approximate count: value-range bounds are ignored.
    (when-not (l/dlmdb?)
      (raise "Only dlmdb counted env is supported." {:dbi dbi-name}))
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
    [this list-name indices budget step visitor k-range k-type v-type]
    (.visit-list-sample this list-name indices budget step visitor k-range
                        k-type v-type true))
  (visit-list-sample
    [this dbi-name indices budget step visitor k-range kt vt raw-pred?]
    (scan/visit-list-sample this dbi-name indices budget step visitor k-range
                            kt vt raw-pred?))

  IAdmin
  (re-index [this opts] (l/re-index* this opts)))

(defn- key-range-list-count-fast
  [lmdb dbi-name [range-type k1 k2] k-type]
  (scan/scan
    (let [^RangeContext ctx (l/range-info rtx range-type k1 k2 k-type)
          flag              (BitOps/intOr
                              (if (.-include-start? ctx)
                                (int DTLV/MDB_COUNT_LOWER_INCL) 0)
                              (if (.-include-stop? ctx)
                                (int DTLV/MDB_COUNT_UPPER_INCL) 0))]
      (with-open [ptr (LongPointer. 1)]
        (DTLV/mdb_range_count_values
          (.get ^Txn (.-txn ^Rtx rtx)) (.get ^Dbi (.-db ^DBI dbi))
          (dtlv-val (.-start-bf ctx)) (dtlv-val (.-stop-bf ctx))
          flag ptr)
        (.get ^LongPointer ptr)))
    (raise "Fail to count (fast) list in key range: " e {:dbi dbi-name})))

(defn- init-info
  ([^CppLMDB lmdb new-info]
   (init-info lmdb new-info false))
  ([^CppLMDB lmdb new-info bypass-wal?]
   (if bypass-wal?
     (binding [c/*bypass-wal* true]
       (transact-kv lmdb c/kv-info (map (fn [[k v]] [:put k v]) new-info)))
     (transact-kv lmdb c/kv-info (map (fn [[k v]] [:put k v]) new-info)))
   (let [dbis (into {}
                    (map (fn [[[_ dbi-name] opts]] [dbi-name opts]))
                    (get-range lmdb c/kv-info
                               [:closed
                                [:dbis :db.value/sysMin]
                                [:dbis :db.value/sysMax]]
                               [:keyword :string]))
         info (into {}
                    (range-filter lmdb c/kv-info
                                  (fn [kv]
                                    (let [b (b/read-buffer (l/k kv) :byte)]
                                      (not= b c/type-hete-tuple)))
                                  [:all]))]
     (vreset! (.-info lmdb) (assoc info :dbis dbis)))))

(defn- open-kv*
  [dir dir-file db-file {:keys [mapsize max-readers flags max-dbs temp?
                                kv-wal?
                                wal-meta-flush-max-txs
                                wal-meta-flush-max-ms
                                wal-group-commit
                                wal-group-commit-ms
                                wal-retention-bytes
                                wal-retention-ms
                                key-compress val-compress]
                         :or   {max-readers      c/*max-readers*
                                max-dbs          c/*max-dbs*
                                mapsize          c/*init-db-size*
                                flags            c/default-env-flags
                                temp?            false
                                wal-meta-flush-max-txs
                                c/*wal-meta-flush-max-txs*
                                wal-meta-flush-max-ms
                                c/*wal-meta-flush-max-ms*
                                wal-group-commit c/*wal-group-commit*
                                wal-group-commit-ms
                                c/*wal-group-commit-ms*
                                wal-retention-bytes
                                c/*wal-retention-bytes*
                                wal-retention-ms
                                c/*wal-retention-ms*
                                kv-wal?          c/*enable-kv-wal*}
                         :as   opts}]
  (try
    (let [inmemory?     (contains? flags :inmemory)
          dir-id        (when inmemory? (str "inmemory:" (UUID/randomUUID)))
          open-dir      (or dir dir-id)
          mapsize       (* (long (if (and db-file (.exists ^File db-file))
                                   (c/pick-mapsize db-file)
                                   mapsize))
                           1024 1024)
          flags         (if temp?
                          (conj flags :nosync)
                          flags)
          flags         (if inmemory?
                          (conj flags :nosync)
                          flags)
          kv-wal-enabled (if (or temp? inmemory?) false (boolean kv-wal?))
          ^Env env      (loop [attempt 0]
                          (let [result (try
                                         (Env/create open-dir mapsize max-readers
                                                     max-dbs (kv-flags flags))
                                         (catch Util$DTLVException e
                                           (if (and (< attempt 5)
                                                    (s/includes? (.getMessage e)
                                                                 "Resource temporarily unavailable"))
                                             ::retry
                                             (throw e))))]
                            (if (identical? result ::retry)
                              (do
                                (Thread/sleep (* 50 (inc attempt)))
                                (recur (inc attempt)))
                              result)))
          now-ms        (System/currentTimeMillis)
          info          (cond-> (merge opts
                                       {:flags        flags
                                        :max-readers  max-readers
                                        :max-dbs      max-dbs
                                        :temp?        temp?
                                        :inmemory?    inmemory?
                                        :kv-wal?      kv-wal-enabled
                                        :wal-meta-flush-max-txs
                                        wal-meta-flush-max-txs
                                        :wal-meta-flush-max-ms
                                        wal-meta-flush-max-ms
                                        :wal-group-commit-ms
                                        wal-group-commit-ms
                                        :wal-retention-bytes
                                        wal-retention-bytes
                                        :wal-retention-ms
                                        wal-retention-ms
                                        :wal-meta-pending-txs 0
                                        :wal-meta-last-flush-ms now-ms
                                        :wal-last-sync-ms now-ms
                                        :wal-indexer-last-flush-ms now-ms
                                        :wal-indexer-last-flush-duration-ms 0
                                        :kv-overlay-env nil
                                        :kv-overlay-private-env nil
                                        :kv-overlay-private-state nil
                                        :kv-overlay-private-entries 0})
                          (not inmemory?) (assoc :dir dir)
                          key-compress (assoc :key-compress key-compress)
                          val-compress (assoc :val-compress val-compress)
                          inmemory? (assoc :dir-id dir-id))
          ^CppLMDB lmdb (->CppLMDB env
                                   (volatile! info)
                                   (ThreadLocal.)
                                   (HashMap.)
                                   (volatile! nil)
                                   (new-bufval c/+max-key-size+)
                                   (new-bufval 0)
                                   (new-bufval c/+max-key-size+)
                                   (new-bufval c/+max-key-size+)
                                   (new-bufval c/+max-key-size+)
                                   (new-bufval c/+max-key-size+)
                                   (bf/allocate-buffer c/+max-key-size+)
                                   nil
                                   (volatile! nil)
                                   false
                                   nil
                                   nil
                                   kv-wal-enabled
                                   nil)]
      (swap! l/lmdb-dirs conj (or dir-id dir))
      (open-dbi lmdb c/kv-info) ;; never compressed
      (when (and kv-wal-enabled dir)
        (u/file (str dir u/+separator+ c/wal-dir)))
      (if temp?
        (when dir-file
          (u/delete-on-exit dir-file))
        (let [k-comp (when (and (not inmemory?) key-compress dir
                                (.exists (io/file dir c/keycode-file-name)))
                       (cp/load-key-compressor
                         (str dir u/+separator+ c/keycode-file-name)))
              v-comp (when (and (not inmemory?) val-compress dir
                                (.exists (io/file dir c/valcode-file-name)))
                       (cp/load-val-compressor
                         (str dir u/+separator+ c/valcode-file-name)))]
          (init-info lmdb (dissoc info
                                  :kv-wal?
                                  :kv-overlay-env
                                  :kv-overlay-private-env
                                  :kv-overlay-private-state))
          (vswap! (l/kv-info lmdb) assoc
                  :kv-wal? kv-wal-enabled
                  :kv-overlay-env nil
                  :kv-overlay-private-env nil
                  :kv-overlay-private-state nil)
          (set-max-val-size lmdb (max-val-size lmdb))
          (set-key-compressor lmdb k-comp)
          (set-val-compressor lmdb v-comp)
          (when-not inmemory?
            (.addShutdownHook (Runtime/getRuntime)
                              (Thread. #(close-kv lmdb))))
          (when (and (not inmemory?) dir)
            (start-scheduled-sync (.-scheduled-sync lmdb) dir env))))
      lmdb)
    (catch Exception e
      (raise "Fail to open database: " e {:dir dir}))))

(defn open-cpp-kv
  ([dir] (open-cpp-kv dir {}))
  ([dir opts]
   (let [flags     (or (:flags opts) c/default-env-flags)
         flags     (if (set? flags) flags (set flags))
         inmemory? (contains? flags :inmemory)
         opts      (assoc opts :flags flags)]
     (if inmemory?
       (open-kv* nil nil nil opts)
       (do
         (assert (string? dir) "directory should be a string.")
         (let [dir-file  (u/file dir)
               db-file   (io/file dir c/data-file-name)
               exist-db? (.exists db-file)
               version   (read-version-file dir-file)]
           (cond
             (not exist-db?)
             (let [lmdb (open-kv* dir dir-file db-file opts)]
               (write-version-file dir-file c/version)
               lmdb)
             version
             (do
               (when (not= version c/version)
                 (if-let [{:keys [major minor patch]} (c/parse-version version)]
                   (let [{cmajor :major cminor :minor}
                         (c/parse-version c/version)]
                     (when (and c/require-migration?
                                (or (< ^long major ^long cmajor)
                                    (< ^long minor ^long cminor)))
                       (m/perform-migration dir major minor patch))
                     (write-version-file dir-file c/version))
                   (raise "Corrupt VERSION file" {:input version})))
               (open-kv* dir dir-file db-file opts))
             :else
             (raise "Database requires migration. Please follow instruction at https://github.com/datalevin/datalevin/blob/master/doc/upgrade.md"
                    {:dir dir}))))))))
