;;
;; Copyright (c) Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns ^:no-doc datalevin.sparselist
  "Sparse array list of integers"
  (:refer-clojure :exclude [get set remove])
  (:require
   [datalevin.ints :as i]
   [datalevin.interface :refer [compress uncompress]]
   [taoensso.nippy :as nippy])
  (:import
   [java.io Writer DataInput DataOutput]
   [java.nio ByteBuffer]
   [datalevin.utl GrowingIntArray]
   [org.roaringbitmap RoaringBitmap]))

(defprotocol ISparseIntArrayList
  (contains-index? [this index] "return true if containing index")
  (get [this index] "get item by index")
  (set [this index item] "set an item by index")
  (remove [this index] "remove an item by index")
  (size [this] "return the size")
  (select [this nth] "return the nth item")
  (serialize [this bf] "serialize to a bytebuffer")
  (deserialize [this bf] "serialize from a bytebuffer"))

(deftype SparseIntArrayList [^RoaringBitmap indices
                             ^GrowingIntArray items]
  ISparseIntArrayList
  (contains-index? [_ index]
    (.contains indices (int index)))

  (get [_ index]
    (when (.contains indices (int index))
      (.get items (dec (.rank indices index)))))

  (set [this index item]
    (let [index (int index)]
      (if (.contains indices index)
        (.set items (dec (.rank indices index)) item)
        (do (.add indices index)
            (.insert items (dec (.rank indices index)) item))))
    this)

  (remove [this index]
    (.remove items (dec (.rank indices index)))
    (.remove indices index)
    this)

  (size [_] (.getCardinality indices))

  (select [_ nth]
    (.get items nth))

  (serialize [_ bf]
    (i/put-ints bf (.toArray items))
    (.serialize indices ^ByteBuffer bf))

  (deserialize [_ bf]
    (.addAll items (i/get-ints bf))
    (.deserialize indices ^ByteBuffer bf))

  Object
  (equals [_ other]
    (and (instance? SparseIntArrayList other)
         (.equals indices (.-indices ^SparseIntArrayList other))
         (.equals items (.-items ^SparseIntArrayList other)))))

(nippy/extend-freeze
  RoaringBitmap :dtlv/bm
  [^RoaringBitmap x ^DataOutput out]
  (.serialize x out))

(nippy/extend-freeze
    GrowingIntArray :dtlv/gia
    [^GrowingIntArray x ^DataOutput out]
  (let [ar        (.toArray  x)
        osize     (alength ar)
        comp?     (< 3 osize)
        ^ints car (if comp?
                    (compress i/int-compressor ar)
                    ar)
        size      (alength car)]
    (.writeInt out (if comp? (- size) size))
    (dotimes [i size] (.writeInt out (aget car i)))))

(nippy/extend-freeze
  SparseIntArrayList :dtlv/sial
  [^SparseIntArrayList x ^DataOutput out]
  (nippy/freeze-to-out! out (.-items x))
  (nippy/freeze-to-out! out (.-indices x)))

(nippy/extend-thaw
  :dtlv/bm [^DataInput in]
  (doto (RoaringBitmap.) (.deserialize in)))

(nippy/extend-thaw
    :dtlv/gia [^DataInput in]
  (let [csize (.readInt in)
        comp? (neg? csize)
        size  (if comp? (- csize) csize)
        car   (int-array size)
        items (GrowingIntArray.)]
    (dotimes [i size] (aset car i (.readInt in)))
    (.addAll items
             (if comp?
               (uncompress i/int-compressor car)
               car))
    items))

(nippy/extend-thaw
  :dtlv/sial [^DataInput in]
  (let [items (nippy/thaw-from-in! in)]
    (->SparseIntArrayList (nippy/thaw-from-in! in) items)))

(defn sparse-arraylist
  ([]
   (->SparseIntArrayList (RoaringBitmap.) (GrowingIntArray.)))
  ([m]
   (let [ssl (sparse-arraylist)]
     (doseq [[k v] m] (set ssl k v))
     ssl))
  ([ks vs]
   (let [ssl (sparse-arraylist)]
     (loop [ks (seq ks)
            vs (seq vs)]
       (when (and ks vs)
         (set ssl (first ks) (first vs))
         (recur (next ks) (next vs))))
     ssl)) )

(defmethod print-method SparseIntArrayList
  [^SparseIntArrayList s ^Writer w]
  (.write w (str "#datalevin/SparseList "))
  (binding [*out* w]
    (pr (for [i (.-indices s)] [i (get s i)]))))
