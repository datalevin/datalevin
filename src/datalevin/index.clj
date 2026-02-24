;;
;; Copyright (c) Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns ^:no-doc datalevin.index
  "Shared index utility functions used by storage and prepare."
  (:require
   [datalevin.interface :refer [get-value]]
   [datalevin.bits :as b]
   [datalevin.datom :as d]
   [datalevin.constants :as c]
   [datalevin.util :as u])
  (:import
   [com.github.luben.zstd Zstd]
   [java.nio ByteBuffer]
   [datalevin.bits Retrieved]
   [datalevin.datom Datom]))

(defn value-type
  [props]
  (if-let [vt (:db/valueType props)]
    (if (identical? vt :db.type/tuple)
      (if-let [tts (props :db/tupleTypes)]
        tts
        (if-let [tt (props :db/tupleType)] [tt] :data))
      vt)
    :data))

(defn datom->indexable
  [schema ^Datom d high?]
  (let [e  (if-some [e (.-e d)] e (if high? c/emax c/e0))
        vm (if high? c/vmax c/v0)
        gm (if high? c/gmax c/g0)]
    (if-let [a (.-a d)]
      (if-let [p (schema a)]
        (if-some [v (.-v d)]
          (b/indexable e (p :db/aid) v (value-type p) gm)
          (b/indexable e (p :db/aid) vm (value-type p) gm))
        (b/indexable e c/a0 c/v0 nil gm))
      (let [am (if high? c/amax c/a0)]
        (if-some [v (.-v d)]
          (if (or (integer? v)
                  (identical? v :db.value/sysMax)
                  (identical? v :db.value/sysMin))
            (if e
              (b/indexable e am v :db.type/ref gm)
              (b/indexable (if high? c/emax c/e0) am v :db.type/ref gm))
            (u/raise
              "When v is known but a is unknown, v must be a :db.type/ref"
              {:v v}))
          (b/indexable e am vm :db.type/sysMin gm))))))

(defonce index->dbi {:eav c/eav :ave c/ave})

(defonce index->ktype {:eav :id :ave :avg})

(defonce index->vtype {:eav :avg :ave :id})

(def ^:private ^"[B" giant-zstd-magic
  (byte-array [(byte 0x44) (byte 0x4C) (byte 0x47) (byte 0x5A)]))

(def ^:private giant-zstd-version (byte 1))

(def ^:private ^:const giant-zstd-header-size 9)

(defn- giant-zstd-envelope?
  [^bytes bs]
  (and (<= giant-zstd-header-size (long (alength bs)))
       (= (aget bs 0) (aget giant-zstd-magic 0))
       (= (aget bs 1) (aget giant-zstd-magic 1))
       (= (aget bs 2) (aget giant-zstd-magic 2))
       (= (aget bs 3) (aget giant-zstd-magic 3))
       (= (aget bs 4) giant-zstd-version)))

(defn- maybe-compress-giant-datom-bytes
  ^bytes [^bytes raw]
  (let [threshold (long c/*giants-zstd-threshold*)]
    (when (<= threshold (long (alength raw)))
      (let [compressed (Zstd/compress raw (int c/*giants-zstd-level*))]
        (when (< (alength compressed) (alength raw))
          (let [compressed-len (alength compressed)
                out            (byte-array
                                 (unchecked-add-int
                                   (int giant-zstd-header-size)
                                   compressed-len))
                bb             (ByteBuffer/wrap out)]
            (.put bb ^bytes giant-zstd-magic)
            (.put bb ^byte giant-zstd-version)
            (.putInt bb (alength raw))
            (.put bb ^bytes compressed)
            out))))))

(defn encode-giant-datom
  "Encode a datom for `datalevin/giants`.
  Returns {:value x :vtype t} where t is :data or :raw."
  [^Datom datom]
  (let [raw (b/serialize datom)]
    (if-let [packed (maybe-compress-giant-datom-bytes raw)]
      {:value packed :vtype :raw}
      {:value datom :vtype :data})))

(defn decode-giant-datom
  "Decode `datalevin/giants` value bytes (supports both legacy :data encoding
  and zstd-compressed raw envelope)."
  [^bytes bs]
  (if (giant-zstd-envelope? bs)
    (let [bb         (ByteBuffer/wrap bs)
          _          (.position bb 5)
          raw-len    (.getInt bb)
          compressed (byte-array (.remaining bb))]
      (.get bb compressed)
      (b/deserialize (Zstd/decompress compressed (long raw-len))))
    (b/read-buffer (ByteBuffer/wrap bs) :data)))

(defn index->k
  [index schema ^Datom datom high?]
  (case index
    :eav (or (.-e datom) (if high? c/emax c/e0))
    :ave (datom->indexable schema datom high?)))

(defn index->v
  [index schema ^Datom datom high?]
  (case index
    :eav (datom->indexable schema datom high?)
    :ave (or (.-e datom) (if high? c/emax c/e0))))

(defn gt->datom
  [lmdb gt]
  (when-let [bs (get-value lmdb c/giants gt :id :raw)]
    (decode-giant-datom bs)))

(defn retrieved->v
  [lmdb ^Retrieved r]
  (let [g (.-g r)]
    (if (= g c/normal)
      (.-v r)
      (d/datom-v (gt->datom lmdb g)))))
