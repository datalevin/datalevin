;;
;; Copyright (c) Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns ^:no-doc datalevin.lmdb
  "Private API for local LMDB Key Value Store.
  Public API is in datalevin.interface"
  (:refer-clojure :exclude [load sync])
  (:require
   [clojure.edn :as edn]
   [clojure.string :as s]
   [clojure.pprint :as p]
   [taoensso.nippy :as nippy]
   [datalevin.async :as a]
   [datalevin.bits :as b]
   [datalevin.util :as u]
   [datalevin.constants :as c]
   [datalevin.interface
    :refer [close-kv list-dbis entries get-range open-dbi transact-kv clear-dbi
            env-dir copy open-transact-kv close-transact-kv stat]])
  (:import
   [datalevin.async IAsyncWork]
   [datalevin.cpp Util]
   [clojure.lang IPersistentVector]
   [java.io Writer PushbackReader FileOutputStream FileInputStream DataOutputStream
    DataInputStream IOException]
   [java.lang RuntimeException]))

(defprotocol IBuffer
  (put-key [this data k-type] "put data in key buffer")
  (put-val [this data v-type] "put data in val buffer"))

(defprotocol IRange
  (range-info [this range-type k1 k2 kt]
    "return key range information for kv iterators")
  (list-range-info [this k-range-type k1 k2 kt v-range-type b1 b2 vt]
    "return key value range information for list iterators"))

(defprotocol IRtx
  (reset [this] "reset a read only transaction")
  (renew [this] "renew a read only transaction")
  (read-only? [this] "is this a read only transaction"))

(defprotocol ICompress
  (key-bf [this] "return the working buffer for key compression")
  (val-bf [this] "return the working buffer for value compression"))

(defprotocol IDB
  (dbi [this] "Return the underlying dbi")
  (dbi-name [this] "Return string name of the dbi")
  (put [this txn] [this txn append?]
    "Put kv pair given in `put-key` and `put-val` of dbi")
  (del [this txn] [this txn all?]
    "Delete the key given in `put-key` of dbi")
  (get-kv [this rtx]
    "Get value of the key given in `put-key` of rtx, return a byte buffer")
  (get-key-rank [this rtx]
    "Get the rank (0-based position) of the key given in `put-key` of rtx.
     Returns nil if the key does not exist.")
  (get-key-by-rank [this rtx rank]
    "Get the key-value at the given rank. Returns [key-buffer value-buffer] or nil.")
  (iterate-kv [this rtx cur k-range k-type v-type]
    "Return an Iterable of key-values, given the key range")
  (iterate-key [this rtx cur k-range k-type]
    "Return an Iterable based on key range only")
  (iterate-key-sample [this rtx cur indices k-range k-type]
    "Return an Iterable of a sample of keys given key range, and an array
    of indices.")
  (iterate-list [this rtx cur k-range k-type v-range v-type]
    "Return an Iterable of key-values given key range and value range,
     applicable only to list dbi")
  (iterate-list-sample
    [this rtx cur indices k-range k-type]
    "Return an Iterable of a sample of key-values given key range,
     and an array of indices, applicable only to list dbi")
  (iterate-list-key-range-val-full [this crt cur k-range k-type]
    "Return an Iterable that walks all values of given key range forwardly,
     applicable only to list dbi")
  (iterate-list-val [this rtx cur v-range v-type]
    "Return a IListRandKeyValIterable given the value range,
     which allows randomly seek key and iterate its values forwardly,
     applicable only to list dbi")
  (iterate-list-val-full [this rtx cur]
    "Return a IListRandKeyValIterable,
     which allows randomly seek key and iterate all its values forwardly,
     applicable only to list dbi")
  (get-cursor [this rtx] "Get a reusable read-only cursor")
  (cursor-count [this cur] "get number of list items under the cursor")
  (close-cursor [this cur] "Close cursor")
  (return-cursor [this cur] "Return a read-only cursor after use"))

(defprotocol IKV
  (k [this] "Key of a key value pair")
  (v [this] "Value of a key value pair"))

(defprotocol IListRandKeyValIterable
  (val-iterator [this]
    "Return an IListRandKeyValIterator that can seek random key and iterate
     its values forwardly. Use with `with-open`."))

(defprotocol IListRandKeyValIterator
  (seek-key [this k-value k-type])
  (has-next-val [this])
  (next-val [this]))

(defprotocol IWriting
  "Used to mark the DB so that it should use the write-txn"
  (writing? [db] "return true if this db should use write-txn")
  (write-txn [db]
    "return deref'able object that is the write-txn or a mutex for locking")
  (mark-write [db] "return a new db what uses write-txn")
  (reset-write [db] "Reset buffers for writing"))

(deftype RangeContext [^boolean forward?
                       ^boolean include-start?
                       ^boolean include-stop?
                       start-bf
                       stop-bf])

(defn range-table
  "Provide context for range iterators"
  [range-type b1 b2]
  (case range-type
    :all               (RangeContext. true false false nil nil)
    :all-back          (RangeContext. false false false nil nil)
    :at-least          (RangeContext. true true false b1 nil)
    :at-most-back      (RangeContext. false true false b1 nil)
    :at-most           (RangeContext. true false true nil b1)
    :at-least-back     (RangeContext. false false true nil b1)
    :closed            (RangeContext. true true true b1 b2)
    :closed-back       (RangeContext. false true true b1 b2)
    :closed-open       (RangeContext. true true false b1 b2)
    :closed-open-back  (RangeContext. false true false b1 b2)
    :greater-than      (RangeContext. true false false b1 nil)
    :less-than-back    (RangeContext. false false false b1 nil)
    :less-than         (RangeContext. true false false nil b1)
    :greater-than-back (RangeContext. false false false nil b1)
    :open              (RangeContext. true false false b1 b2)
    :open-back         (RangeContext. false false false b1 b2)
    :open-closed       (RangeContext. true false true b1 b2)
    :open-closed-back  (RangeContext. false false true b1 b2)
    (u/raise "Unknown range type" range-type {})))

(defprotocol IKVTxable (kv-txable? [_]))

(extend-type Object IKVTxable (kv-txable? [_] false))
(extend-type nil IKVTxable (kv-txable? [_] false))

(deftype KVTxData [op
                   ^String dbi-name
                   k
                   v
                   kt
                   vt
                   flags]
  IKVTxable
  (kv-txable? [_] true))

(defmethod print-method KVTxData
  [^KVTxData d, ^Writer w]
  (.write w (pr-str [(.-op d) (.-dbi-name d) (.-k d) (.-v d) (.-kt d)
                     (.-vt d)])))

(defn kv-tx
  ([op dbi k]
   (KVTxData. op dbi k nil nil nil nil))
  ([op dbi k v]
   (if (= op :del)
     (KVTxData. op dbi k nil v nil nil)
     (KVTxData. op dbi k v nil nil nil)))
  ([op dbi k v kt]
   (KVTxData. op dbi k v kt nil nil))
  ([op dbi k v kt vt]
   (KVTxData. op dbi k v kt vt nil))
  ([op dbi k v kt vt f]
   (KVTxData. op dbi k v kt vt f)))

(defn ->kv-tx-data
  ([x]
   (if (kv-txable? x)
     x
     (if (vector? x)
       (let [tx  ^IPersistentVector x
             cnt (.length tx)
             op  (.nth tx 0)]
         (KVTxData. op
                    (.nth tx 1)
                    (.nth tx 2)
                    (when-not (= :del op) (.nth tx 3))
                    (if (= :del op)
                      (when (< 3 cnt) (.nth tx 3))
                      (when (< 4 cnt) (.nth tx 4)))
                    (when (< 5 cnt) (.nth tx 5))
                    (when (< 6 cnt) (.nth tx 6))))
       (u/raise "Invalid KV transaction data " x {}))))
  ([x kt vt]
   (if (vector? x)
     (let [tx  ^IPersistentVector x
           cnt (.length tx)]
       (KVTxData. (.nth tx 0)
                  nil
                  (.nth tx 1)
                  (when (< 2 cnt) (.nth tx 2))
                  kt
                  vt
                  (when (< 3 cnt) (.nth tx 3))))
     (u/raise "Invalid KV transaction data " x {}))))

(defn dump-dbis-list
  ([lmdb]
   (p/pprint (set (list-dbis lmdb))))
  ([lmdb data-output]
   (if data-output
     (nippy/freeze-to-out!
       data-output
       (set (list-dbis lmdb)))
     (dump-dbis-list lmdb))))

;; We have consolidated bindings to JavaCPP
(defn- pick-binding [] :cpp)

(defmulti open-kv (constantly (pick-binding)))

(defonce ^:private open-kv-wrapper (volatile! identity))

(defn set-open-kv-wrapper!
  [f]
  (vreset! open-kv-wrapper (or f identity))
  nil)

(defn wrap-open-kv
  [db]
  (@open-kv-wrapper db))

(defn- nippy-dbi [lmdb dbi]
  [{:dbi dbi :entries (entries lmdb dbi)}
   (for [[k v] (get-range lmdb dbi [:all] :raw :raw)]
     [(b/encode-base64 k) (b/encode-base64 v)])])

(defn dump-dbi
  ([lmdb dbi]
   (p/pprint {:dbi dbi :entries (entries lmdb dbi)})
   (doseq [[k v] (get-range lmdb dbi [:all] :raw :raw)]
     (p/pprint [(b/encode-base64 k) (b/encode-base64 v)])))
  ([lmdb dbi data-output]
   (if data-output
     (nippy/freeze-to-out! data-output (nippy-dbi lmdb dbi))
     (dump-dbi lmdb dbi))))

(defn dump-all
  ([lmdb]
   (dump-dbi lmdb c/kv-info)
   (doseq [dbi (set (list-dbis lmdb))] (dump-dbi lmdb dbi)))
  ([lmdb data-output]
   (if data-output
     (nippy/freeze-to-out!
       data-output
       (conj (for [dbi (set (list-dbis lmdb))] (nippy-dbi lmdb dbi))
             (nippy-dbi lmdb c/kv-info)))
     (dump-all lmdb))))

(defn- load-kv [dbi [k v]]
  (kv-tx :put dbi (b/decode-base64 k) (b/decode-base64 v) :raw :raw))

(defn load-dbi
  ([lmdb dbi in nippy?]
   (if nippy?
     (let [[_ kvs] (nippy/thaw-from-in! in)]
       (open-dbi lmdb dbi)
       (transact-kv lmdb (map #(load-kv dbi %) kvs)))
     (load-dbi lmdb dbi in)))
  ([lmdb dbi in]
   (try
     (with-open [^PushbackReader r in]
       (let [read-form         #(edn/read {:eof ::EOF} r)
             {:keys [entries]} (read-form)]
         (open-dbi lmdb dbi)
         (transact-kv lmdb (->> (repeatedly read-form)
                                (take-while #(not= ::EOF %))
                                (take entries)
                                (map #(load-kv dbi %))))))
     (catch IOException e
       (u/raise "IO error while loading raw data: " (ex-message e) {}))
     (catch RuntimeException e
       (u/raise "Parse error while loading raw data: " (ex-message e) {}))
     (catch Exception e
       (u/raise "Error loading raw data: " (ex-message e) {})))))

(defn load-all
  ([lmdb in nippy?]
   (if nippy?
     (doseq [[{:keys [dbi]} kvs] (nippy/thaw-from-in! in)]
       (open-dbi lmdb dbi)
       (transact-kv lmdb (map #(load-kv dbi %) kvs)))
     (load-all lmdb in)))
  ([lmdb in]
   (try
     (with-open [^PushbackReader r in]
       (let [read-form #(edn/read {:eof ::EOF} r)
             load-dbi  (fn [[ms vs]]
                         (doseq [{:keys [dbi]} (butlast ms)]
                           (open-dbi lmdb dbi))
                         (let [{:keys [dbi entries]} (last ms)]
                           (open-dbi lmdb dbi)
                           (->> vs
                                (take entries)
                                (map #(load-kv dbi %)))))]
         (transact-kv lmdb (->> (repeatedly read-form)
                                (take-while #(not= ::EOF %))
                                (partition-by map?)
                                (partition 2 2 nil)
                                (mapcat load-dbi)))))
     (catch IOException e
       (u/raise "IO error while loading raw data: " (ex-message e) {}))
     (catch RuntimeException e
       (u/raise "Parse error while loading raw data: " (ex-message e) {}))
     (catch Exception e
       (u/raise "Error loading raw data: " (ex-message e) {})))))

(defn clear [lmdb]
  (doseq [dbi (set (list-dbis lmdb)) ] (clear-dbi lmdb dbi)))

(defn dump
  [db ^String dumpfile]
  (let [d (DataOutputStream. (FileOutputStream. dumpfile))]
    (dump-all db d)
    (.flush d)
    (.close d)))

(defn- load
  [db ^String dumpfile]
  (let [f  (FileInputStream. dumpfile)
        in (DataInputStream. f)]
    (load-all db in true)
    (.close f)))

(defn re-index*
  [db opts]
  (try
    (let [bk    (when (:backup? opts)
                  (u/tmp-dir
                    (str "dtlv-re-index-" (System/currentTimeMillis))))
          d     (env-dir db)
          dfile (str d u/+separator+ "kv-dump")]
      (when bk (copy db bk true))
      (dump db dfile)
      (clear db)
      (close-kv db)
      (let [db (open-kv d (update opts :flags conj :nosync))]
        (load db dfile)
        (close-kv db))
      (open-kv d opts))
    (catch Exception e
      (u/raise "Unable to re-index" e {:dir (env-dir db)}))))

(defn resized? [e] (:resized (ex-data e)))

(defn data-size
  "data size in bytes, excluding kv-info DBI"
  [db]
  (* ^long (:psize (stat db))
     ^long (reduce
             (fn [^long pages dbi]
               (+ pages (let [m (stat db dbi)]
                          (+ ^long (:branch-pages m)
                             ^long (:leaf-pages m)
                             ^long (:overflow-pages m)))))
             0 (list-dbis db))))

(defmacro with-transaction-kv
  "Evaluate body within the context of a single new read/write transaction,
  ensuring atomicity of key-value operations. Works with synchronous `transact-kv`.

  `db` is a new identifier of the kv database with a new read/write transaction attached,
  and `orig-db` is the original kv database.

  `body` should refer to `db`.

  Example:

          (with-transaction-kv [kv lmdb]
            (let [^long now (get-value kv \"a\" :counter)]
              (transact-kv kv [[:put \"a\" :counter (inc now)]])
              (get-value kv \"a\" :counter)))"
  [[db orig-db] & body]
  `(locking (write-txn ~orig-db)
     (let [writing#   (writing? ~orig-db)
           condition# (fn [~'e] (and (resized? ~'e) (not writing#)))]
       (u/repeat-try-catch
           ~c/+in-tx-overflow-times+
           condition#
         (try
           (let [~db (if writing# ~orig-db (open-transact-kv ~orig-db))]
             (u/repeat-try-catch
                 ~c/+in-tx-overflow-times+
                 condition#
               ~@body))
           (finally (when-not writing# (close-transact-kv ~orig-db))))))))

;; for shutting down various executors when the last LMDB exits
(defonce lmdb-dirs (atom #{}))

;; for freeing in-memory vector index objects when an LMDB exits
(defonce vector-indices (atom {}))  ; fname -> VectorIndex

;; check if db is backed by DLMDB (rather than stock LMDB)
(defonce dlmdb? (memoize (fn [] (s/starts-with? (Util/version) "D"))))

(declare kv-tx-combine)

(defn- kv-work-key* [dir] (->> dir hash (str "kv-tx") keyword))

(def kv-work-key (memoize kv-work-key*))

(deftype ^:no-doc AsyncKVTx [lmdb dbi-name txs k-type v-type cb]
  IAsyncWork
  (work-key [_] (kv-work-key (env-dir lmdb)))
  (do-work [_] (transact-kv lmdb dbi-name txs k-type v-type))
  (combine [_] kv-tx-combine)
  (callback [_] cb))

(defn- kv-tx-combine
  [coll]
  (let [^AsyncKVTx fw (first coll)]
    (->AsyncKVTx (.-lmdb fw)
                 (.-dbi-name fw)
                 (into [] (comp (map #(.-txs ^AsyncKVTx %)) cat) coll)
                 (.-k-type fw)
                 (.-v-type fw)
                 (.-cb fw))))

(defn transact-kv-async
  ([this txs] (transact-kv-async this nil txs))
  ([this dbi-name txs]
   (transact-kv-async this dbi-name txs :data :data))
  ([this dbi-name txs k-type]
   (transact-kv-async this dbi-name txs k-type :data))
  ([this dbi-name txs k-type v-type]
   (transact-kv-async this dbi-name txs k-type v-type nil))
  ([this dbi-name txs k-type v-type callback]
   (a/exec (a/get-executor)
           (->AsyncKVTx this dbi-name txs k-type v-type callback))))
