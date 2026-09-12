;;
;; Copyright (c) Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns ^:no-doc datalevin.binding.cpp.buffer
  "Raw value buffers, resource pooling, and DBI handles."
  (:require
   [clojure.string :as s]
   [datalevin.binding.cpp.iter :as iter]
   [datalevin.bits :as b]
   [datalevin.buffer :as bf]
   [datalevin.constants :as c]
   [datalevin.interface :refer [bf-compress set-max-val-size]]
   [datalevin.lmdb :as l :refer [IBuffer IDB]]
   [datalevin.util :refer [raise]])
  (:import
   [datalevin.dtlvnative DTLV DTLV$MDB_val DTLV$MDB_stat]
   [datalevin.cpp BufVal Cursor Dbi Stat Txn UnsafeAccess Util]
   [java.nio BufferOverflowException ByteBuffer]
   [java.util ArrayDeque]
   [java.util.function Supplier]
   [org.bytedeco.javacpp LongPointer]))

(def ^:private ^:const ave-multiple-max-buffer-bytes 524288)

(defonce ^:private buffer-allocation-observer* (atom nil))

(defn set-buffer-allocation-observer-reader!
  "Register a thunk returning the current buffer-allocation observer."
  [f]
  (reset! buffer-allocation-observer* f))

(defn- buffer-allocation-observer
  []
  (when-let [f @buffer-allocation-observer*]
    (f)))

(defprotocol IPool
  (pool-add [_ x])
  (pool-take [_]))

(defprotocol ICloseableResource
  (close-resource! [_]))

(definterface ^:private IIdBuffer
  (^void putKeyId [^long id])
  (^void putValId [^long id]))

(definterface ^:private IWriteCursor
  (^datalevin.cpp.Cursor writeCursor [^datalevin.cpp.Txn txn]))

(definterface ^:private IMultipleBuffer
  (^datalevin.cpp.BufVal multipleValBuffer [^long size])
  (^datalevin.cpp.Cursor multipleWriteCursor [^datalevin.cpp.Txn txn]))

(deftype Pool [^ThreadLocal que]
  IPool
  (pool-add [_ x] (.add ^ArrayDeque (.get que) x))
  (pool-take [_] (.poll ^ArrayDeque (.get que))))

(defn new-pools
  []
  (Pool. (ThreadLocal/withInitial
          (reify Supplier
            (get [_] (ArrayDeque.))))))

(defn new-bufval [size] (BufVal. size))

(defn close-txn-quiet!
  [^Txn txn]
  (when txn
    (try
      (.close txn)
      (catch Exception _))))

(defn close-bufval-quiet!
  [^BufVal bufval]
  (when bufval
    (try
      (.close bufval)
      (catch Throwable _))))

(defn close-mdb-val-quiet!
  [^DTLV$MDB_val value]
  (when value
    (try
      (.close (.position value 0))
      (catch Throwable _))))

(defn clean-buffer-quiet!
  [^ByteBuffer buffer]
  (when buffer
    (try
      (UnsafeAccess/clean buffer)
      (catch Throwable _))))

(defn close-cursor-quiet!
  [^Cursor cur]
  (when cur
    (try
      (.close cur)
      (catch Throwable _))))

(defn- bufval-open?
  [^BufVal bufval]
  (try
    (some? (.ptr bufval))
    (catch Throwable _ false)))

(defn- cursor-open?
  [^Cursor cur]
  (and (bufval-open? (.key cur))
       (bufval-open? (.val cur))))

(defn- reusable-cursor
  [^Pool curs ^Txn txn]
  (loop []
    (when-let [^Cursor cur (pool-take curs)]
      (if-not (cursor-open? cur)
        (do
          (close-cursor-quiet! cur)
          (recur))
        (let [renewed (try
                        (.renew cur txn)
                        cur
                        (catch Throwable _
                          (close-cursor-quiet! cur)
                          nil))]
          (if renewed
            renewed
            (recur)))))))

(defn- flag-value
  "flag key to int value, cover all flags"
  [k]
  (case k
    :fixedmap DTLV/MDB_FIXEDMAP
    :nosubdir DTLV/MDB_NOSUBDIR
    :rdonly-env DTLV/MDB_RDONLY
    :writemap DTLV/MDB_WRITEMAP
    :nometasync DTLV/MDB_NOMETASYNC
    :nosync DTLV/MDB_NOSYNC
    :mapasync DTLV/MDB_MAPASYNC
    :notls DTLV/MDB_NOTLS
    :nolock DTLV/MDB_NOLOCK
    :nordahead DTLV/MDB_NORDAHEAD
    :nomeminit DTLV/MDB_NOMEMINIT
    :inmemory DTLV/MDB_INMEMORY

    :cp-compact DTLV/MDB_CP_COMPACT

    :reversekey DTLV/MDB_REVERSEKEY
    :dupsort DTLV/MDB_DUPSORT
    :integerkey DTLV/MDB_INTEGERKEY
    :dupfixed DTLV/MDB_DUPFIXED
    :integerdup DTLV/MDB_INTEGERDUP
    :reversedup DTLV/MDB_REVERSEDUP
    :create DTLV/MDB_CREATE
    :prefix-compression DTLV/MDB_PREFIX_COMPRESSION
    :counted DTLV/MDB_COUNTED

    :nooverwrite DTLV/MDB_NOOVERWRITE
    :nodupdata DTLV/MDB_NODUPDATA
    :current DTLV/MDB_CURRENT
    :reserve DTLV/MDB_RESERVE
    :append DTLV/MDB_APPEND
    :appenddup DTLV/MDB_APPENDDUP

    :rdonly-txn DTLV/MDB_RDONLY))

(defn kv-flags
  [flags]
  (if (seq flags)
    (reduce (fn [r f] (bit-or ^int r ^int f))
            0 (mapv flag-value flags))
    (int 0)))

(defonce env-flag-map
  {0x01 :fixedmap
   0x4000 :nosubdir
   0x10000 :nosync
   0x20000 :rdonly-env
   0x40000 :nometasync
   0x80000 :writemap
   0x100000 :mapasync
   0x200000 :notls
   0x400000 :nolock
   0x800000 :nordahead
   0x1000000 :nomeminit})

(defn env-flag-keys
  [v]
  (reduce-kv
   (fn [s i k]
     (if (not= 0 (bit-and ^int i ^int v))
       (conj s k)
       s))
   #{} env-flag-map))

(defn put-bufval
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

(defn put-id-bufval
  [^BufVal vp ^long id compressor ^ByteBuffer cbf]
  (let [^ByteBuffer bf (.inBuf vp)]
    (.clear bf)
    (if compressor
      (do (.clear cbf)
          (.putLong cbf id)
          (bf-compress compressor (.flip cbf) bf))
      (.putLong bf id))
    (.flip bf)
    (.reset vp)))

(defn key-range-info*
  [key-codec rtx range-type k1 k2 kt]
  (put-bufval (iter/rtx-start-key-buf rtx) k1 kt key-codec (l/key-bf rtx))
  (put-bufval (iter/rtx-stop-key-buf rtx) k2 kt key-codec (l/key-bf rtx))
  (l/range-table range-type (iter/rtx-start-key-buf rtx) (iter/rtx-stop-key-buf rtx)))

(defn list-range-info*
  [key-codec rtx k-range-type k1 k2 kt v-range-type v1 v2 vt
   value-compressor]
  (let [^BufVal start-kp      (iter/rtx-start-key-buf rtx)
        ^BufVal stop-kp       (iter/rtx-stop-key-buf rtx)
        ^BufVal start-vp      (iter/rtx-start-val-buf rtx)
        ^BufVal stop-vp       (iter/rtx-stop-val-buf rtx)
        ^ByteBuffer k-comp-bf (l/key-bf rtx)
        ^ByteBuffer v-comp-bf (l/val-bf rtx)]
    (put-bufval start-kp k1 kt key-codec k-comp-bf)
    (put-bufval stop-kp k2 kt key-codec k-comp-bf)
    (put-bufval start-vp v1 vt value-compressor v-comp-bf)
    (put-bufval stop-vp v2 vt value-compressor v-comp-bf)
    [(l/range-table k-range-type start-kp stop-kp)
     (l/range-table v-range-type start-vp stop-vp)]))

(defn stat-map [^Stat stat]
  (let [^DTLV$MDB_stat s (.get stat)]
    {:psize (.ms_psize s)
     :depth (.ms_depth s)
     :branch-pages (.ms_branch_pages s)
     :leaf-pages (.ms_leaf_pages s)
     :overflow-pages (.ms_overflow_pages s)
     :entries (.ms_entries s)}))

(defn val-size
  [x]
  (let [^long val-size (b/measure-size x)]
    (if (< Integer/MAX_VALUE val-size)
      (raise "Value size is too large" {:size val-size})
      (let [try-size (* ^long c/+buffer-grow-factor+ val-size)]
        (if (< Integer/MAX_VALUE try-size)
          val-size
          try-size)))))
(deftype DBI [lmdb
              key-codec
              value-codec
              ^Dbi db
              ^Pool curs
              ^BufVal kp
              ^:volatile-mutable ^BufVal vp
              ^ByteBuffer k-comp-bf
              ^:volatile-mutable ^ByteBuffer v-comp-bf
              ^boolean dupsort?
              ^boolean dupfixed?
              ^boolean counted?
              ^boolean validate-data?
              ^:volatile-mutable ^BufVal multiple-vp
              ^:volatile-mutable ^DTLV$MDB_val multiple-vals]
  iter/IKeyValCodec
  (dbi-key-codec [_] key-codec)
  (dbi-value-codec [_] value-codec)

  IMultipleBuffer
  (multipleValBuffer [_ size]
    (let [size             (long size)
          ^BufVal current  multiple-vp
          ^long current-capacity (if current
                                   (.capacity ^ByteBuffer (.inBuf current))
                                   0)]
      (when (or (neg? size)
                (> size (long ave-multiple-max-buffer-bytes)))
        (raise "Invalid AVE multiple-value buffer size" {:size size}))
      (when (< current-capacity size)
        (let [doubled-capacity (if (pos? current-capacity)
                                 (min (long ave-multiple-max-buffer-bytes)
                                      (* 2 current-capacity))
                                 size)
              new-capacity     (long (max size doubled-capacity))
              ^BufVal replacement (new-bufval new-capacity)]
          (set! multiple-vp replacement)
          (close-bufval-quiet! current)
          (when-let [observer (buffer-allocation-observer)]
            (observer new-capacity))))
      multiple-vp))
  (multipleWriteCursor [_ txn]
    (when-not multiple-vals
      (set! multiple-vals (DTLV$MDB_val. 2)))
    (Cursor/create txn db kp vp multiple-vals))

  IBuffer
  (put-key [this x t]
    (try
      (put-bufval kp x t key-codec k-comp-bf)
      (catch BufferOverflowException _
        (raise "Key cannot be larger than 511 bytes." {:input x}))
      (catch Exception e
        (raise "Error putting r/w key buffer of "
               (.dbi-name this) ": " e {:value x :type t}))))
  (put-val [this x t]
    (try
      (put-bufval vp x t value-codec v-comp-bf)
      (catch BufferOverflowException _
        (let [size          (val-size x)
              old-vp        vp
              old-v-comp-bf v-comp-bf]
          (set! vp (new-bufval size))
          (set! v-comp-bf (bf/allocate-buffer size))
          (close-bufval-quiet! old-vp)
          (clean-buffer-quiet! old-v-comp-bf)
          (set-max-val-size lmdb size)
          (put-bufval vp x t value-codec v-comp-bf)))
      (catch Exception e
        (raise "Error putting r/w value buffer of "
               (.dbi-name this) ": " e {:value x :type t}))))

  IIdBuffer
  (putKeyId [this id]
    (try
      (put-id-bufval kp id key-codec k-comp-bf)
      (catch BufferOverflowException _
        (raise "Key cannot be larger than 511 bytes." {:input id}))
      (catch Exception e
        (raise "Error putting r/w key buffer of "
               (.dbi-name this) ": " e {:value id :type :id}))))
  (putValId [this id]
    (try
      (put-id-bufval vp id value-codec v-comp-bf)
      (catch BufferOverflowException _
        ;; Preserve the generic buffer-growth behavior on the exceptional path.
        (.put-val this (Long/valueOf id) :id))
      (catch Exception e
        (raise "Error putting r/w value buffer of "
               (.dbi-name this) ": " e {:value id :type :id}))))

  IWriteCursor
  (writeCursor [_ txn]
    (Cursor/create txn db kp vp))

  IDB
  (dbi [_] db)
  (dbi-name [_] (.getName db))
  (put-read-key [_ rtx x t]
    (let [rtx rtx]
      (try
        (put-bufval (iter/rtx-key-buf rtx) x t key-codec (l/key-bf rtx))
        (catch BufferOverflowException _
          (raise "Key cannot be larger than 511 bytes after encoding."
                 {:input x})))))
  (put [_ txn flags] (.put db txn kp vp (kv-flags flags)))
  (put [this txn] (.put this txn nil))
  (del [_ txn all?] (if all? (.del db txn kp nil) (.del db txn kp vp)))
  (del [this txn] (.del this txn true))
  (get-kv [_ rtx]
    (let [^BufVal kp (iter/rtx-key-buf rtx)
          ^BufVal vp (iter/rtx-val-buf rtx)
          rc (DTLV/mdb_get (.get ^Txn (iter/rtx-txn rtx))
                           (.get db) (.ptr kp) (.ptr vp))]
      (Util/checkRc ^int rc)
      (when-not (= rc DTLV/MDB_NOTFOUND)
        (iter/v-bf vp value-codec rtx))))
  (get-key-rank [_ rtx]
    (let [^BufVal kp (iter/rtx-key-buf rtx)
          ^LongPointer rp (LongPointer. 1)
          rc (DTLV/mdb_get_key_rank (.get ^Txn (iter/rtx-txn rtx))
                                    (.get db) (.ptr kp) nil rp)]
      (Util/checkRc ^int rc)
      (when-not (= rc DTLV/MDB_NOTFOUND)
        (.get rp))))
  (get-key-by-rank [_ rtx rank]
    (let [^BufVal kp (iter/rtx-key-buf rtx)
          ^BufVal vp (iter/rtx-val-buf rtx)
          rc (DTLV/mdb_get_rank (.get ^Txn (iter/rtx-txn rtx))
                                (.get db) (long rank) (.ptr kp) (.ptr vp))]
      (Util/checkRc ^int rc)
      (when-not (= rc DTLV/MDB_NOTFOUND)
        [(l/k (iter/->KV kp vp key-codec value-codec rtx))
         (iter/v-bf vp value-codec rtx)])))
  (iterate-key [this rtx cur [range-type k1 k2] k-type]
    (let [ctx (key-range-info* key-codec rtx range-type k1 k2 k-type)]
      (iter/->KeyIterable lmdb this cur rtx ctx)))
  (iterate-key-sample [this rtx cur indices [range-type k1 k2] k-type]
    (let [ctx (key-range-info* key-codec rtx range-type k1 k2 k-type)]
      (iter/->KeySampleIterable lmdb this indices cur rtx ctx)))
  (iterate-list [this rtx cur [k-range-type k1 k2] k-type
                 [v-range-type v1 v2] v-type]
    (let [ctx (list-range-info*
                key-codec rtx k-range-type k1 k2 k-type v-range-type v1 v2 v-type
                value-codec)]
      (iter/->ListIterable lmdb this cur rtx ctx)))
  (iterate-list-sample [this rtx cur indices [k-range-type k1 k2] k-type]
    (let [ctx (key-range-info* key-codec rtx k-range-type k1 k2 k-type)]
      (iter/->ListSampleIterable lmdb this indices cur rtx ctx)))
  (iterate-list-key-range-val-full [this rtx cur [range-type k1 k2] k-type]
    (let [ctx (key-range-info* key-codec rtx range-type k1 k2 k-type)]
      (iter/->ListKeyRangeFullValIterable lmdb this cur rtx ctx)))
  (iterate-list-val-full [this rtx cur]
    (iter/->ListFullValIterable lmdb this cur rtx))
  (iterate-kv [this rtx cur k-range k-type v-type]
    (if dupsort?
      (let [range-type (first k-range)]
        (if (and (keyword? range-type)
                 (s/ends-with? (name range-type) "-back"))
          (.iterate-list this rtx cur k-range k-type [:all] v-type)
          (.iterate-list-key-range-val-full this rtx cur k-range k-type)))
      (.iterate-key this rtx cur k-range k-type)))
  (get-cursor [_ rtx]
    (let [rtx rtx
          ^Txn txn (iter/rtx-txn rtx)]
      (or (when (.isReadOnly txn)
            (reusable-cursor curs txn))
          (Cursor/create txn db (iter/rtx-key-buf rtx) (iter/rtx-val-buf rtx)))))
  (cursor-count [_ cur] (.count ^Cursor cur))
  (close-cursor [_ cur] (.close ^Cursor cur))
  (return-cursor [_ cur] (pool-add curs cur))

  ICloseableResource
  (close-resource! [_]
    (close-bufval-quiet! kp)
    (close-bufval-quiet! vp)
    (close-bufval-quiet! multiple-vp)
    (close-mdb-val-quiet! multiple-vals)
    (clean-buffer-quiet! k-comp-bf)
    (clean-buffer-quiet! v-comp-bf)
    (try
      (.close db)
      (catch Throwable _))
    nil))

(defn close-dbi-quiet!
  [^DBI dbi]
  (when dbi
    (try
      (close-resource! dbi)
      (catch Throwable _))))

(defn dbi-val-compressor
  [^DBI dbi]
  (.-value-codec dbi))


