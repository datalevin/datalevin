;;
;; Copyright (c) Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns ^:no-doc datalevin.binding.cpp.open
  "Reading and initializing kv-info metadata during environment open."
  (:require
   [datalevin.bits :as b]
   [datalevin.constants :as c]
   [datalevin.interface :as i :refer [get-range transact-kv]]
   [datalevin.lmdb :as l]
   [datalevin.util :refer [raise]]
   [datalevin.validate :as vld])
  (:import
   [datalevin.cpp BufVal Env Txn Util]
   [datalevin.dtlvnative DTLV]
   [java.nio ByteBuffer]
   [org.bytedeco.javacpp IntPointer]))

(defn- raw-header-type
  "Like `b/header->type`, but returns nil for unrecognized headers so the
   kv-info decoder can fall back to `:data`."
  [header]
  (try
    (b/header->type header)
    (catch Exception _ nil)))

(defn- decode-kv-info-buffer
  [^ByteBuffer bf fallback-type]
  (try
    (b/read-buffer (.rewind bf) :data)
    (catch Exception data-e
      (if fallback-type
        (b/read-buffer (.rewind bf) fallback-type)
        (throw data-e)))))

(defn- decode-kv-info-entry
  [kv]
  (let [^ByteBuffer kb (l/k kv)
        ^ByteBuffer vb (l/v kv)
        key-type       (b/read-buffer (.rewind kb) :byte)
        val-type       (b/read-buffer (.rewind vb) :byte)]
    (when (and (not= key-type c/type-hete-tuple)
               (not= val-type c/type-bytes))
      (let [k (decode-kv-info-buffer kb
                                     (when (= key-type c/type-keyword)
                                       :keyword))
            dbi-key? (and (vector? k)
                          (= 2 (count k))
                          (= :dbis (nth k 0)))]
        (when-not dbi-key?
          (try
            [k (decode-kv-info-buffer vb (raw-header-type val-type))]
            (catch Exception e
              (raise "Fail to decode kv-info entry"
                       e
                       {:key k
                        :key-type key-type
                        :raw-val-type val-type}))))))))

(defn persisted-wal?
  "Read WAL enablement before selecting the environment's immutable flags.
   The caller owns the probe environment and the local-open reservation."
  [^Env env]
  (with-open [^Txn txn (Txn/createReadOnly env)
              dbi (IntPointer. 1)
              key (BufVal. c/+max-key-size+)
              value (BufVal. 0)]
    (let [rc (DTLV/mdb_dbi_open (.get txn) c/kv-info 0 dbi)]
      (Util/checkRc rc)
      (when-not (= rc DTLV/MDB_NOTFOUND)
        (letfn [(lookup [key-type]
                  (let [bf (.inBuf key)]
                    (b/put-buffer (.clear bf) :wal? key-type)
                    (.flip bf)
                    (.reset key)
                    (let [rc (DTLV/mdb_get (.get txn) (.get dbi)
                                           (.ptr key) (.ptr value))]
                      (Util/checkRc rc)
                      (when-not (= rc DTLV/MDB_NOTFOUND)
                        (let [bf (.outBuf value)]
                          (decode-kv-info-buffer
                            bf (raw-header-type (.get bf 0))))))))]
          ;; Typed keyword rows sort after legacy :data keys in kv-info, so
          ;; prefer them just as load-info-from-kv does. Preserve false.
          (true? (if-some [wal? (lookup :keyword)] wal? (lookup :data))))))))

(defn load-info-from-kv
  [lmdb]
  (let [dbis (into {}
                   (map (fn [[[_ dbi-name] opts]] [dbi-name opts]))
                   (get-range lmdb c/kv-info
                              [:closed
                               [:dbis :db.value/sysMin]
                               [:dbis :db.value/sysMax]]
                              [:keyword :string]))
        types (into {}
                    (map (fn [[[_ type-name] definition]] [type-name definition]))
                    (get-range lmdb c/kv-info
                               [:closed [:types c/v0] [:types c/vmax]]
                               [:keyword :keyword]))
        info (into {}
                   (i/range-keep lmdb c/kv-info decode-kv-info-entry
                                 [:all] :raw :raw true))]
    (c/canonicalize-wal-opts
     (assoc info :dbis dbis :types types
            :custom-dbis (into #{} (keep (fn [[name opts]]
                                          (when (or (:key-type opts) (:value-type opts))
                                            name))) dbis)))))

(defn init-info
  [lmdb new-info]
  (transact-kv lmdb c/kv-info
               (keep (fn [[k v]] (when (some? v) [:put k v])) new-info))
  (merge new-info (load-info-from-kv lmdb)))

(defn retain-wal-durability-profile!
  [lmdb loaded-info info]
  (if-not (true? (:wal? info))
    info
    (let [profile (or (:wal-durability-profile info)
                      c/*wal-durability-profile*)]
      (vld/validate-option-mutation :wal-durability-profile profile)
      (when-not (= profile (:wal-durability-profile loaded-info))
        ;; Raw KV stores do not have the Datalog options DBI. Retain the
        ;; effective profile in kv-info before the WAL runtime starts so an
        ;; omitted profile on the next open cannot silently change durability.
        (transact-kv lmdb c/kv-info
                     [[:put :wal-durability-profile profile]]))
      (assoc info :wal-durability-profile profile))))
