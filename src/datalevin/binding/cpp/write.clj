;;
;; Copyright (c) Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns ^:no-doc datalevin.binding.cpp.write
  "Datom and KV write path for the native LMDB binding."
  (:require
   [datalevin.binding.cpp.buffer]
   [datalevin.bits :as b]
   [datalevin.constants :as c]
   [datalevin.lmdb :as l]
   [datalevin.util :refer [raise]]
   [datalevin.validate :as vld])
  (:import
   [datalevin.dtlvnative DTLV]
   [datalevin.cpp BufVal Cursor]
   [datalevin.binding.cpp.buffer DBI IMultipleBuffer IWriteCursor]
   [datalevin.lmdb DatomKVTxData KVTxData]
   [datalevin.utl BitOps]
   [java.nio ByteBuffer]
   [java.util Arrays Comparator HashMap List]
   [org.eclipse.collections.impl.list.mutable FastList]))

(def ^:private ^:const ave-multiple-min-items 2)
(def ^:private ^:const ave-multiple-max-items 65536)

(defonce ^:private write-observer-reader* (atom nil))
(defonce ^:private write-flags-observer-reader* (atom nil))

(defn set-write-observer-readers!
  "Register thunks returning the current write observers. The observers are
  bound at the `datalevin.binding.cpp` level for compatibility."
  [write-fn flags-fn]
  (reset! write-observer-reader* write-fn)
  (reset! write-flags-observer-reader* flags-fn))

(defn- write-observer
  []
  (when-let [f @write-observer-reader*]
    (f)))

(defn- write-flags-observer
  []
  (when-let [f @write-flags-observer-reader*]
    (f)))

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
    :del-list (let [vs         (.-v tx)
                    vt         (.-vt tx)
                    ^BufVal kp (.-kp dbi)]
                (.put-key dbi (.-k tx) (.-kt tx))
                (doseq [v vs]
                  (.put-val dbi v vt)
                  (.del dbi txn false)
                  ;; mdb_del may mutate the native key if value is missing
                  (.reset kp)))))

(defn- put-datom-tx
  [^DBI ave ^DBI eav txn ^DatomKVTxData tx]
  (let [e (.-e tx)
        avg (.-avg tx)]
    (if (.-added? tx)
      (do
        (.put-key ave avg :raw)
        (.putValId ave e)
        (.put ave txn)
        (.putKeyId eav e)
        (.put-val eav avg :raw)
        (.put eav txn))
      (do
        (let [^BufVal kp (.-kp ave)]
          (.put-key ave avg :raw)
          (.putValId ave e)
          (.del ave txn false)
          ;; mdb_del may mutate the native key if value is missing
          (.reset kp))
        (let [^BufVal kp (.-kp eav)]
          (.putKeyId eav e)
          (.put-val eav avg :raw)
          (.del eav txn false)
          (.reset kp))))))

(defn- put-ave-datom-tx
  [^DBI ave txn ^DatomKVTxData tx]
  (let [e (.-e tx)
        avg (.-avg tx)]
    (.put-key ave avg :raw)
    (.putValId ave e)
    (if (.-added? tx)
      (.put ave txn)
      (let [^BufVal kp (.-kp ave)]
        (.del ave txn false)
        (.reset kp)))))

(defn- put-eav-datom-tx
  [^DBI eav txn ^DatomKVTxData tx]
  (let [e (.-e tx)
        avg (.-avg tx)]
    (.putKeyId eav e)
    (.put-val eav avg :raw)
    (if (.-added? tx)
      (.put eav txn)
      (let [^BufVal kp (.-kp eav)]
        (.del eav txn false)
        (.reset kp)))))

(defn- put-eav-datom-append-tx
  [^DBI eav ^Cursor cur ^DatomKVTxData tx ^long flags]
  (let [e   (.-e tx)
        avg (.-avg tx)]
    (.putKeyId eav e)
    (.put-val eav avg :raw)
    (.put cur (int flags))))

(defn- last-id-key
  [^Cursor cur]
  (when (.seek cur DTLV/MDB_LAST)
    (long (b/read-buffer (.outBuf ^BufVal (.key cur)) :id))))

(defn- appendable-new-eav-batch?
  ^Boolean [^objects datoms ^long n ^Cursor cur ^Boolean add-only?]
  (and add-only?
       (pos? n)
       (let [first-e (.-e ^DatomKVTxData (aget datoms 0))
             last-e  (last-id-key cur)]
         (or (nil? last-e) (> first-e (long last-e))))))

(def ^:private datom-eav-comparator
  (reify Comparator
    (compare [_ x y]
      (let [^DatomKVTxData x x
            ^DatomKVTxData y y
            c (Long/compare (.-e x) (.-e y))]
        (if (zero? c)
          (BitOps/compareBytes (.-avg x) (.-avg y))
          c)))))

(def ^:private datom-ave-comparator
  (reify Comparator
    (compare [_ x y]
      (let [^DatomKVTxData x x
            ^DatomKVTxData y y
            c (BitOps/compareBytes (.-avg x) (.-avg y))]
        (if (zero? c)
          (Long/compare (.-e x) (.-e y))
          c)))))

(defn- ave-multiple-run-end
  ^long [^objects datoms ^long start ^long n]
  (let [^DatomKVTxData first-tx (aget datoms (int start))]
    (if (.-no-overwrite? first-tx)
      (unchecked-inc start)
      (let [^bytes avg (.-avg first-tx)]
        (loop [i (unchecked-inc start)]
          (if (< i n)
            (let [^DatomKVTxData tx (aget datoms (int i))]
              (if (and (not (.-no-overwrite? tx))
                       (zero? (BitOps/compareBytes avg (.-avg tx))))
                (recur (unchecked-inc i))
                i))
            i))))))

(defn- put-ave-multiple-chunk!
  [^DBI ave ^Cursor cur ^objects datoms start item-count flags ^BufVal values]
  (let [start          (long start)
        item-count     (long item-count)
        flags          (long flags)
        end            (+ start item-count)
        ^DatomKVTxData first-tx (aget datoms (int start))
        ^ByteBuffer bf (.inBuf values)]
    (.put-key ave (.-avg first-tx) :raw)
    (.clear bf)
    (loop [i start]
      (when (< i end)
        (.putLong bf (.-e ^DatomKVTxData (aget datoms (int i))))
        (recur (unchecked-inc i))))
    (.flip bf)
    (.reset values)
    (let [written (.putMultiple cur values (long c/+id-bytes+) item-count
                                (int flags))]
      (when-not (= item-count written)
        (raise "LMDB wrote only part of an AVE MDB_MULTIPLE batch"
               {:expected item-count :written written})))
    (when-let [observer (write-observer)]
      (observer item-count))
    (when-let [observer (write-flags-observer)]
      (observer item-count flags))))

(defn- put-ave-multiple-run!
  [^DBI ave ^Cursor cur ^objects datoms start end flags ^BufVal values]
  (let [start (long start)
        end   (long end)
        flags (long flags)]
    (loop [i start]
      (when (< i end)
        (let [item-count (long
                           (min (long ave-multiple-max-items) (- end i)))]
          (put-ave-multiple-chunk! ave cur datoms i item-count flags values)
          (recur (+ i item-count)))))))

(defn- put-ave-added-datom!
  [^DBI ave ^Cursor cur ^DatomKVTxData tx flags]
  (let [flags (long flags)
        flags (if (.-no-overwrite? tx)
                (bit-or flags DTLV/MDB_NOOVERWRITE)
                flags)]
    (.put-key ave (.-avg tx) :raw)
    (.putValId ave (.-e tx))
    (if (.-no-overwrite? tx)
      (when-not (.tryPut cur (int flags))
        (raise "Blind unique value already exists"
                   {:type l/blind-unique-collision-type}))
      (.put cur (int flags)))))

(defn- put-ave-added-segment!
  [^DBI ave ^Cursor cur ^objects datoms start end flags]
  (let [start      (long start)
        end        (long end)
        item-count (- end start)
        flags      (long flags)]
    (when (pos? item-count)
      (if (>= item-count (long ave-multiple-min-items))
        (let [buffer-size (* (long c/+id-bytes+)
                             (min (long ave-multiple-max-items) item-count))
              ^BufVal values (.multipleValBuffer
                               ^IMultipleBuffer ave buffer-size)]
          ;; AVE values are fixed-width entity IDs, so one sorted
          ;; (attribute, value) segment crosses JNI in one MDB_MULTIPLE call.
          (put-ave-multiple-run! ave cur datoms start end flags values))
        (loop [i start]
          (when (< i end)
            (put-ave-added-datom!
              ave cur ^DatomKVTxData (aget datoms (int i)) flags)
            (recur (unchecked-inc i))))))))

(defn- ave-last-existing-eid
  [^DBI ave ^Cursor cur ^DatomKVTxData first-tx]
  (.put-key ave (.-avg first-tx) :raw)
  (when (.seek cur DTLV/MDB_SET)
    (when-not (.seek cur DTLV/MDB_LAST_DUP)
      (raise "Unable to read the last AVE duplicate" {}))
    (.getLong ^ByteBuffer (.outBuf ^BufVal (.val cur)))))

(defn- ave-appenddup-start
  [^DBI ave ^Cursor cur ^objects datoms start end
   ^Boolean all-new-entities?]
  (let [start (long start)
        end   (long end)]
    (if all-new-entities?
      start
      (if-let [last-e (ave-last-existing-eid
                        ave cur ^DatomKVTxData (aget datoms (int start)))]
        (let [last-e (long last-e)]
          (loop [i start]
            (if (and (< i end)
                     (<= (.-e ^DatomKVTxData (aget datoms (int i))) last-e))
              (recur (unchecked-inc i))
              i)))
        start))))

(defn- put-ave-add-only-runs!
  [^DBI ave ^Cursor cur ^objects datoms n
   ^Boolean all-new-entities?]
  (let [n (long n)]
    (loop [i 0]
      (when (< i n)
        (let [^DatomKVTxData tx (aget datoms (int i))]
          (if (.-no-overwrite? tx)
            (do
              (put-ave-added-datom! ave cur tx (int 0))
              (recur (unchecked-inc i)))
            (let [end          (ave-multiple-run-end datoms i n)
                  item-count   (- end i)
                  append-start (if (or all-new-entities?
                                       (>= item-count
                                           (long ave-multiple-min-items)))
                                 (ave-appenddup-start
                                   ave cur datoms i end all-new-entities?)
                                 end)]
              (put-ave-added-segment! ave cur datoms i append-start (int 0))
              (put-ave-added-segment!
                ave cur datoms append-start end DTLV/MDB_APPENDDUP)
              (recur end))))))))

(defn add-only-datom-batch?
  ^Boolean [^java.util.List txs]
  (loop [i          0
         saw-datom? false]
    (if (< i (.size txs))
      (let [tx (.get txs i)]
        (if (instance? DatomKVTxData tx)
          (if (.-added? ^DatomKVTxData tx)
            (recur (unchecked-inc i) true)
            false)
          (recur (unchecked-inc i) saw-datom?)))
      saw-datom?)))

(defn transact1*
  [txs ^DBI dbi txn kt vt]
  (let [validate? (.-validate-data? dbi)]
    (doseq [t txs]
      (let [tx (l/->kv-tx-data t kt vt)]
        (vld/validate-kv-tx-data tx validate?)
        (put-tx dbi txn tx)))))

(defn- transact-datom-seq*
  [xs ^HashMap dbis txn]
  (loop [xs  (seq xs)
         ave nil
         eav nil]
    (when xs
      (let [t (first xs)]
        (if (instance? DatomKVTxData t)
          (let [^DBI ave* (or ave (.get dbis c/ave)
                              (raise c/ave " is not open" {}))
                ^DBI eav* (or eav (.get dbis c/eav)
                              (raise c/eav " is not open" {}))]
            (put-datom-tx ave* eav* txn t)
            (recur (next xs) ave* eav*))
          (let [^KVTxData tx (l/->kv-tx-data t)
                dbi-name (.-dbi-name tx)
                ^DBI dbi (or (.get dbis dbi-name)
                             (raise dbi-name " is not open" {}))
                validate? (.-validate-data? dbi)]
            (vld/validate-kv-tx-data tx validate?)
            (put-tx dbi txn tx)
            (recur (next xs) ave eav)))))))

(defn- datom-txs
  [^List txs]
  (let [n (.size txs)
        ^objects out (object-array n)]
    (loop [i 0
           j 0]
      (if (< i n)
        (let [tx (.get txs i)]
          (if (instance? DatomKVTxData tx)
            (do
              (aset out j tx)
              (recur (unchecked-inc i) (unchecked-inc j)))
            (recur (unchecked-inc i) j)))
        [out j]))))

(defn- transact-datom-index-passes*
  [^objects datoms n ^HashMap dbis txn ^Boolean add-only?]
  (let [^DBI ave (or (.get dbis c/ave) (raise c/ave " is not open" {}))
        ^DBI eav (or (.get dbis c/eav) (raise c/eav " is not open" {}))
        n         (int n)]
    (Arrays/sort datoms 0 n datom-eav-comparator)
    (let [all-new-entities?
          (with-open [^Cursor cur (.writeCursor ^IWriteCursor eav txn)]
            (let [append? (appendable-new-eav-batch?
                            datoms n cur add-only?)]
              (if append?
                (loop [i 0
                       previous-e Long/MIN_VALUE]
                  (when (< i n)
                    (let [^DatomKVTxData tx (aget datoms i)
                          e                 (.-e tx)]
                      (put-eav-datom-append-tx
                        eav cur tx
                        (if (= e previous-e)
                          DTLV/MDB_APPENDDUP
                          DTLV/MDB_APPEND))
                      (recur (unchecked-inc i) e))))
                (dotimes [i n]
                  (let [^DatomKVTxData tx (aget datoms i)]
                    (if (.-added? tx)
                      (do
                        (.putKeyId eav (.-e tx))
                        (.put-val eav (.-avg tx) :raw)
                        (.put cur (int 0)))
                      (put-eav-datom-tx eav txn tx)))))
              append?))]
      (Arrays/parallelSort datoms 0 n datom-ave-comparator)
      (with-open [^Cursor cur (if add-only?
                               (.multipleWriteCursor ^IMultipleBuffer ave txn)
                               (.writeCursor ^IWriteCursor ave txn))]
        (if add-only?
          (put-ave-add-only-runs!
            ave cur datoms n all-new-entities?)
          (dotimes [i n]
            (let [^DatomKVTxData tx (aget datoms i)]
              (if (.-added? tx)
                (put-ave-added-datom! ave cur tx (int 0))
                (put-ave-datom-tx ave txn tx)))))))))

(defn- transact-datom-list*
  [^java.util.List txs ^HashMap dbis txn]
  (let [add-only? (add-only-datom-batch? txs)]
    (if (or c/*ordered-datom-writes?* add-only?)
      (let [[^objects datoms n] (datom-txs txs)]
        (transact-datom-index-passes* datoms n dbis txn add-only?)
        (dotimes [i (.size txs)]
          (let [tx (.get txs i)]
            (when-not (instance? DatomKVTxData tx)
              (let [^KVTxData tx (l/->kv-tx-data tx)
                    dbi-name (.-dbi-name tx)
                    ^DBI dbi (or (.get dbis dbi-name)
                                 (raise dbi-name " is not open" {}))
                    validate? (.-validate-data? dbi)]
                (vld/validate-kv-tx-data tx validate?)
                (put-tx dbi txn tx))))))
      (let [n (.size txs)]
        (loop [i   0
               ave nil
               eav nil]
          (when (< i n)
            (let [t (.get txs i)]
              (if (instance? DatomKVTxData t)
                (let [^DBI ave* (or ave (.get dbis c/ave)
                                    (raise c/ave " is not open" {}))
                      ^DBI eav* (or eav (.get dbis c/eav)
                                    (raise c/eav " is not open" {}))]
                  (put-datom-tx ave* eav* txn t)
                  (recur (unchecked-inc i) ave* eav*))
                (let [^KVTxData tx (l/->kv-tx-data t)
                      dbi-name (.-dbi-name tx)
                      ^DBI dbi (or (.get dbis dbi-name)
                                   (raise dbi-name " is not open" {}))
                      validate? (.-validate-data? dbi)]
                  (vld/validate-kv-tx-data tx validate?)
                  (put-tx dbi txn tx)
                  (recur (unchecked-inc i) ave eav))))))))))

(defn transact*
  [txs ^HashMap dbis txn]
  (if (instance? java.util.List txs)
    (let [^java.util.List tx-list txs]
      (if (and (pos? (.size tx-list))
               (instance? DatomKVTxData (.get tx-list 0)))
        (transact-datom-list* tx-list dbis txn)
        (doseq [t tx-list]
          (let [^KVTxData tx (l/->kv-tx-data t)
                dbi-name (.-dbi-name tx)
                ^DBI dbi (or (.get dbis dbi-name)
                             (raise dbi-name " is not open" {}))
                validate? (.-validate-data? dbi)]
            (vld/validate-kv-tx-data tx validate?)
            (put-tx dbi txn tx)))))
    (let [xs (seq txs)]
      (if (instance? DatomKVTxData (first xs))
        (transact-datom-seq* xs dbis txn)
        (doseq [t xs]
          (let [^KVTxData tx (l/->kv-tx-data t)
                dbi-name (.-dbi-name tx)
                ^DBI dbi (or (.get dbis dbi-name)
                             (raise dbi-name " is not open" {}))
                validate? (.-validate-data? dbi)]
            (vld/validate-kv-tx-data tx validate?)
            (put-tx dbi txn tx)))))))

(defn- prepared-datom-txs
  [^objects ops]
  (let [n            (alength ops)
        ^objects out (object-array (quot n 2))]
    (loop [i          0
           j          0
           add-only?  true]
      (if (< i n)
        (let [tx (aget ops (unchecked-inc i))]
          (if (instance? DatomKVTxData tx)
            (do
              (aset out j tx)
              (recur (+ i 2) (unchecked-inc j)
                     (and add-only? (.-added? ^DatomKVTxData tx))))
            (recur (+ i 2) j add-only?)))
        [out j add-only?]))))

(defn- transact-prepared-datom-ops-scalar!*
  [^objects ops ^HashMap dbis txn]
  (let [n (alength ops)]
    (loop [i   0
           ave nil
           eav nil]
      (when (< i n)
        (let [^DBI dbi (aget ops i)
              tx       (aget ops (unchecked-inc i))]
          (if (instance? DatomKVTxData tx)
            (let [^DBI ave* (or ave (.get dbis c/ave)
                                (raise c/ave " is not open" {}))
                  ^DBI eav* (or eav (.get dbis c/eav)
                                (raise c/eav " is not open" {}))]
              (put-datom-tx ave* eav* txn tx)
              (recur (+ i 2) ave* eav*))
            (do
              (put-tx dbi txn ^KVTxData tx)
              (recur (+ i 2) ave eav))))))))

(defn- transact-prepared-datom-ops*
  [^objects ops ^HashMap dbis txn]
  (let [[^objects datoms n add-only?] (prepared-datom-txs ops)]
    (if (or c/*ordered-datom-writes?* add-only?)
      (do
        (transact-datom-index-passes* datoms n dbis txn add-only?)
        (loop [i 0]
          (when (< i (alength ops))
            (let [tx (aget ops (unchecked-inc i))]
              (when-not (instance? DatomKVTxData tx)
                (put-tx ^DBI (aget ops i) txn ^KVTxData tx))
              (recur (+ i 2))))))
      (transact-prepared-datom-ops-scalar!* ops dbis txn))))

(defn transact-prepared-ops*
  [^objects ops ^HashMap dbis txn]
  (let [n (alength ops)]
    (if (and (pos? n)
             (instance? DatomKVTxData (aget ops 1)))
      (transact-prepared-datom-ops* ops dbis txn)
      (loop [i 0]
        (when (< i n)
          (put-tx ^DBI (aget ops i) txn
                  ^KVTxData (aget ops (unchecked-inc i)))
          (recur (+ i 2)))))))

(defn prepare-kvtx-ops
  [txs ^HashMap dbis dbi-name kt vt]
  (if (and (instance? java.util.List txs)
           (instance? java.util.RandomAccess txs))
    (let [^java.util.List tx-list txs
          n                       (.size tx-list)
          ^objects out            (object-array (* 2 n))]
      (if dbi-name
        (let [^DBI dbi (or (.get dbis dbi-name)
                           (raise dbi-name " is not open" {}))
              validate? (.-validate-data? dbi)]
          (loop [i 0
                 j 0]
            (when (< i n)
              (let [^KVTxData tx (l/->kv-tx-data (.get tx-list i) kt vt)]
                (vld/validate-kv-tx-data tx validate?)
                (aset out j dbi)
                (aset out (unchecked-inc j) tx)
                (recur (unchecked-inc i) (+ j 2))))))
        (if (and (pos? n)
                 (instance? DatomKVTxData (.get tx-list 0)))
          (loop [i 0
                 j 0]
            (when (< i n)
              (let [t (.get tx-list i)]
                (if (instance? DatomKVTxData t)
                  (do
                    (aset out j nil)
                    (aset out (unchecked-inc j) t))
                  (let [^KVTxData tx (l/->kv-tx-data t)
                        dbi-name*     (.-dbi-name tx)
                        ^DBI dbi      (or (.get dbis dbi-name*)
                                          (raise dbi-name* " is not open" {}))
                        validate?     (.-validate-data? dbi)]
                    (vld/validate-kv-tx-data tx validate?)
                    (aset out j dbi)
                    (aset out (unchecked-inc j) tx)))
                (recur (unchecked-inc i) (+ j 2)))))
          (loop [i 0
                 j 0]
            (when (< i n)
              (let [^KVTxData tx (l/->kv-tx-data (.get tx-list i))
                    dbi-name*     (.-dbi-name tx)
                    ^DBI dbi      (or (.get dbis dbi-name*)
                                      (raise dbi-name* " is not open" {}))
                    validate?     (.-validate-data? dbi)]
                (vld/validate-kv-tx-data tx validate?)
                (aset out j dbi)
                (aset out (unchecked-inc j) tx)
                (recur (unchecked-inc i) (+ j 2)))))))
      out)
    (let [^FastList out (FastList.)]
      (if dbi-name
        (let [^DBI dbi (or (.get dbis dbi-name)
                           (raise dbi-name " is not open" {}))
              validate? (.-validate-data? dbi)]
          (doseq [t txs]
            (let [^KVTxData tx (l/->kv-tx-data t kt vt)]
              (vld/validate-kv-tx-data tx validate?)
              (.add out dbi)
              (.add out tx))))
        (let [xs (seq txs)]
          (if (instance? DatomKVTxData (first xs))
            (doseq [t xs]
              (if (instance? DatomKVTxData t)
                (do
                  (.add out nil)
                  (.add out t))
                (let [^KVTxData tx (l/->kv-tx-data t)
                      dbi-name*     (.-dbi-name tx)
                      ^DBI dbi      (or (.get dbis dbi-name*)
                                        (raise dbi-name* " is not open" {}))
                      validate?     (.-validate-data? dbi)]
                  (vld/validate-kv-tx-data tx validate?)
                  (.add out dbi)
                  (.add out tx))))
            (doseq [t xs]
              (let [^KVTxData tx (l/->kv-tx-data t)
                    dbi-name*     (.-dbi-name tx)
                    ^DBI dbi      (or (.get dbis dbi-name*)
                                      (raise dbi-name* " is not open" {}))
                    validate?     (.-validate-data? dbi)]
                (vld/validate-kv-tx-data tx validate?)
                (.add out dbi)
                (.add out tx))))))
      (.toArray out))))
