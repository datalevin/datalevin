;;
;; Copyright (c) Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns ^:no-doc datalevin.binding.cpp.iter
  "Key/value views and native LMDB iterators."
  (:require
   [datalevin.interface :refer [bf-uncompress]]
   [datalevin.lmdb :as l :refer [IKV IListRandKeyValIterable
                                 IListRandKeyValIterator]]
   [datalevin.util :as u])
  (:import
   [datalevin.dtlvnative DTLV DTLV$MDB_val DTLV$dtlv_key_iter
    DTLV$dtlv_key_rank_sample_iter DTLV$dtlv_list_iter
    DTLV$dtlv_list_key_range_full_val_iter DTLV$dtlv_list_rank_sample_iter
    DTLV$dtlv_list_val_full_iter]
   [datalevin.cpp BufVal Cursor Util]
   [datalevin.lmdb RangeContext]
   [java.lang AutoCloseable]
   [java.nio ByteBuffer]
   [java.util Iterator]
   [org.bytedeco.javacpp SizeTPointer]))

(defprotocol IKeyValCodec
  (dbi-key-codec [_] "Return the key codec of a DBI handle.")
  (dbi-value-codec [_] "Return the value codec of a DBI handle."))

(defprotocol IRtxInternals
  (rtx-txn [_] "Return the native transaction of a read transaction.")
  (rtx-key-buf [_] "Return the key buffer of a read transaction.")
  (rtx-val-buf [_] "Return the value buffer of a read transaction.")
  (rtx-start-key-buf [_] "Return the start key buffer.")
  (rtx-stop-key-buf [_] "Return the stop key buffer.")
  (rtx-start-val-buf [_] "Return the start value buffer.")
  (rtx-stop-val-buf [_] "Return the stop value buffer."))

(defn v-bf
  [^BufVal vp value-compressor rtx]
  (let [bf (.outBuf vp)]
    (if-let [compressor value-compressor]
      (let [^ByteBuffer cbf (l/val-bf rtx)]
        (bf-uncompress compressor bf cbf)
        (.flip cbf))
      bf)))

(deftype KV [^BufVal kp ^BufVal vp key-codec value-compressor rtx]
  IKV
  (k [_]
    (let [bf (.outBuf kp)]
      (if-let [compressor key-codec]
        (let [^ByteBuffer cbf (l/key-bf rtx)]
          (bf-uncompress compressor bf cbf)
          (.flip cbf))
        bf)))

  (v [_] (v-bf vp value-compressor rtx)))

(defn- dtlv-bool [x] (if x DTLV/DTLV_TRUE DTLV/DTLV_FALSE))

(defn dtlv-val ^DTLV$MDB_val [x] (when x (.ptr ^BufVal x)))

(defn- dtlv-rc [^long x]
  (cond
    (== x DTLV/DTLV_TRUE)  true
    (== x DTLV/DTLV_FALSE) false
    :else (u/raise "Native iterator returns error code" x {})))

(defn dtlv-c [^long x]
  (if (< x 0)
    (u/raise "Native counter returns error code" x {})
    x))

(deftype KeyIterable [lmdb
                      db
                      ^Cursor cur
                      rtx
                      ^RangeContext ctx]
  Iterable
  (iterator [_]
    (let [forward? (dtlv-bool (.-forward? ctx))
          include-start? (dtlv-bool (.-include-start? ctx))
          include-stop? (dtlv-bool (.-include-stop? ctx))
          sk (dtlv-val (.-start-bf ctx))
          ek (dtlv-val (.-stop-bf ctx))
          k (.key cur)
          v (.val cur)
          value-compressor (dbi-value-codec db)
          iter (DTLV$dtlv_key_iter.)]
      (Util/checkRc
       (DTLV/dtlv_key_iter_create
        iter (.ptr cur) (.ptr k) (.ptr v)
        ^int forward? ^int include-start? ^int include-stop? sk ek))
      (reify
        Iterator
        (hasNext [_] (dtlv-rc (DTLV/dtlv_key_iter_has_next iter)))
        (next [_] (->KV k v (dbi-key-codec db) value-compressor rtx))

        AutoCloseable
        (close [_] (DTLV/dtlv_key_iter_destroy iter))))))

(deftype KeySampleIterable [lmdb
                            db
                            ^longs indices
                            ^Cursor cur
                            rtx
                            ^RangeContext ctx]
  Iterable
  (iterator [_]
    (let [sk (dtlv-val (.-start-bf ctx))
          ek (dtlv-val (.-stop-bf ctx))
          k (.key cur)
          v (.val cur)
          value-compressor (dbi-value-codec db)
          iter (DTLV$dtlv_key_rank_sample_iter.)
          samples (alength indices)
          sizets (SizeTPointer. samples)]
      (dotimes [i samples] (.put sizets i (aget indices i)))
      (Util/checkRc
       (DTLV/dtlv_key_rank_sample_iter_create
        ^DTLV$dtlv_key_rank_sample_iter iter
        sizets samples (.ptr cur) (.ptr k) (.ptr v) sk ek))
      (reify
        Iterator
        (hasNext [_] (dtlv-rc (DTLV/dtlv_key_rank_sample_iter_has_next iter)))
        (next [_] (->KV k v (dbi-key-codec db) value-compressor rtx))

        AutoCloseable
        (close [_] (DTLV/dtlv_key_rank_sample_iter_destroy iter))))))

(deftype ListIterable [lmdb
                       db
                       ^Cursor cur
                       rtx
                       ctx]
  Iterable
  (iterator [_]
    (let [[^RangeContext kctx ^RangeContext vctx] ctx

          forward-key? (dtlv-bool (.-forward? kctx))
          include-start-key? (dtlv-bool (.-include-start? kctx))
          include-stop-key? (dtlv-bool (.-include-stop? kctx))
          sk (dtlv-val (.-start-bf kctx))
          ek (dtlv-val (.-stop-bf kctx))
          forward-val? (dtlv-bool (.-forward? vctx))
          include-start-val? (dtlv-bool (.-include-start? vctx))
          include-stop-val? (dtlv-bool (.-include-stop? vctx))
          sv (dtlv-val (.-start-bf vctx))
          ev (dtlv-val (.-stop-bf vctx))
          k (.key cur)
          v (.val cur)
          value-compressor (dbi-value-codec db)
          iter (DTLV$dtlv_list_iter.)]
      (Util/checkRc
       (DTLV/dtlv_list_iter_create
        iter (.ptr cur) (.ptr k) (.ptr v)
        ^int forward-key? ^int include-start-key? ^int include-stop-key? sk ek
        ^int forward-val? ^int include-start-val? ^int include-stop-val?
        sv ev))
      (reify
        Iterator
        (hasNext [_] (dtlv-rc (DTLV/dtlv_list_iter_has_next iter)))
        (next [_] (->KV k v (dbi-key-codec db) value-compressor rtx))

        AutoCloseable
        (close [_] (DTLV/dtlv_list_iter_destroy iter))))))

(deftype ListSampleIterable [lmdb
                             db
                             ^longs indices
                             ^Cursor cur
                             rtx
                             ^RangeContext ctx]
  Iterable
  (iterator [_]
    (let [sk      (dtlv-val (.-start-bf ctx))
          ek      (dtlv-val (.-stop-bf ctx))
          k       (.key cur)
          v       (.val cur)
          value-compressor (dbi-value-codec db)
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
        (hasNext [_] (dtlv-rc (DTLV/dtlv_list_rank_sample_iter_has_next iter)))
        (next [_] (->KV k v (dbi-key-codec db) value-compressor rtx))

        AutoCloseable
        (close [_] (DTLV/dtlv_list_rank_sample_iter_destroy iter))))))

(deftype ListKeyRangeFullValIterable [lmdb
                                      db
                                      ^Cursor cur
                                      rtx
                                      ^RangeContext ctx]
  Iterable
  (iterator [_]
    (let [include-start? (dtlv-bool (.-include-start? ctx))
          include-stop?  (dtlv-bool (.-include-stop? ctx))
          sk             (dtlv-val (.-start-bf ctx))
          ek             (dtlv-val (.-stop-bf ctx))
          k              (.key cur)
          v              (.val cur)
          value-compressor (dbi-value-codec db)
          iter           (DTLV$dtlv_list_key_range_full_val_iter.)]
      (Util/checkRc
       (DTLV/dtlv_list_key_range_full_val_iter_create
        iter (.ptr cur) (.ptr k) (.ptr v)
        ^int include-start? ^int include-stop? sk ek))
      (reify
        Iterator
        (hasNext [_]
          (dtlv-rc (DTLV/dtlv_list_key_range_full_val_iter_has_next iter)))
        (next [_] (->KV k v (dbi-key-codec db) value-compressor rtx))

        AutoCloseable
        (close [_] (DTLV/dtlv_list_key_range_full_val_iter_destroy iter))))))

(deftype ListFullValIterable [lmdb
                              db
                              ^Cursor cur
                              rtx]
  IListRandKeyValIterable
  (val-iterator [_]
    (let [^BufVal k (.key cur)
          ^BufVal v (.val cur)
          value-compressor (dbi-value-codec db)
          iter (DTLV$dtlv_list_val_full_iter.)]
      (Util/checkRc
       (DTLV/dtlv_list_val_full_iter_create iter (.ptr cur) (.ptr k) (.ptr v)))
      (reify
        IListRandKeyValIterator
        (seek-key [_ x t]
          (l/put-read-key db rtx x t)
          (dtlv-rc
           (DTLV/dtlv_list_val_full_iter_seek
            iter (.ptr ^BufVal (rtx-key-buf rtx)))))
        (has-next-val [_]
          (dtlv-rc (DTLV/dtlv_list_val_full_iter_has_next iter)))
        (next-val [_] (v-bf v value-compressor rtx))

        AutoCloseable
        (close [_] (DTLV/dtlv_list_val_full_iter_destroy iter))))))
