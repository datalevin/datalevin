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
   [s-exp.hako.ext :as hext])
  (:import
   [com.s_exp.hako Reader]
   [java.io Writer DataInput DataOutput ByteArrayOutputStream DataOutputStream ByteArrayInputStream DataInputStream]
   [java.nio ByteBuffer]
   [datalevin.utl GrowingIntArray]
   [org.roaringbitmap ImmutableBitmapDataProvider RoaringBitmap]
   [org.roaringbitmap.buffer ImmutableRoaringBitmap]))

(defprotocol ISparseIntArrayList
  (contains-index? [this index] "return true if containing index")
  (get [this index] "get item by index")
  (set [this index item] "set an item by index")
  (remove [this index] "remove an item by index")
  (size [this] "return the size")
  (select [this nth] "return the nth item")
  (serialize [this bf] "serialize to a bytebuffer")
  (deserialize [this bf] "serialize from a bytebuffer"))

(deftype SparseIntArrayList [^ImmutableBitmapDataProvider indices
                             ^GrowingIntArray items]
  ISparseIntArrayList
  (contains-index? [_ index]
    (.contains indices (int index)))

  (get [_ index]
    (when (.contains indices (int index))
      (.get items (dec (.rank indices index)))))

  (set [this index item]
    (let [indices ^RoaringBitmap indices
          index   (int index)]
      (if (.contains indices index)
        (.set items (dec (.rank indices index)) item)
        (do (.add indices index)
            (.insert items (dec (.rank indices index)) item))))
    this)

  (remove [this index]
    (let [indices ^RoaringBitmap indices]
      (.remove items (dec (.rank indices index)))
      (.remove indices index))
    this)

  (size [_] (.getCardinality indices))

  (select [_ nth]
    (.get items nth))

  (serialize [_ bf]
    (i/put-ints bf (.toArray items))
    (.serialize indices ^ByteBuffer bf))

  (deserialize [_ bf]
    (.addAll items (i/get-ints bf))
    (.deserialize ^RoaringBitmap indices ^ByteBuffer bf))

  Object
  (equals [_ other]
    (and (instance? SparseIntArrayList other)
         (.equals indices (.-indices ^SparseIntArrayList other))
         (.equals items (.-items ^SparseIntArrayList other)))))

(hext/register-user-tag!
 0x10000005                             ; private range, subtag 5 = RoaringBitmap
 RoaringBitmap
 (fn write-bm [^com.s_exp.hako.Writer w ^RoaringBitmap x]
   (let [baos (ByteArrayOutputStream.)
         dos  (DataOutputStream. baos)]
     (.serialize x ^DataOutput dos)
     (.flush dos)
     (.writeBytes w (.toByteArray baos))))
 (fn read-bm [^Reader r]
   (let [tag (.getByte r)
         low (bit-and tag 0x0F)
         n   (.readTierPayload r (int low))
         arr (.getBytes r (int n))
         dis (DataInputStream. (ByteArrayInputStream. arr))]
     (doto (RoaringBitmap.) (.deserialize ^DataInput dis)))))

(hext/register-user-tag!
 0x10000006                             ; private range, subtag 6 = GrowingIntArray
 GrowingIntArray
 (fn write-gia [^com.s_exp.hako.Writer w ^GrowingIntArray x]
   (let [ar        (.toArray x)
         osize     (alength ar)
         comp?     (< 3 osize)
         ^ints car (if comp?
                     (compress i/int-compressor ar)
                     ar)
         size      (alength car)]
     (.putI32 w (if comp? (- size) size))
     (dotimes [i size] (.putI32 w (aget car i)))))
 (fn read-gia [^Reader r]
   (let [csize (.getI32 r)
         comp? (neg? csize)
         size  (if comp? (- csize) csize)
         car   (int-array size)
         items (GrowingIntArray.)]
     (dotimes [i size] (aset car i (.getI32 r)))
     (.addAll items
              (if comp?
                (uncompress i/int-compressor car)
                car))
     items)))

(hext/register-user-tag!
 0x10000007                             ; private range, subtag 7 = SparseIntArrayList
 SparseIntArrayList
 (fn write-sial [^com.s_exp.hako.Writer w ^SparseIntArrayList x]
   (.writeAny w (.-items x))
   (.writeAny w (.-indices x)))
 (fn read-sial [^Reader r]
   (let [items (.readAny r)
         indices (.readAny r)]
     (->SparseIntArrayList indices items))))

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
     ssl)))

(defn deserialize-off-heap
  "Deserialize a read-only sparse list with its bitmap backed by direct memory."
  ^SparseIntArrayList [^ByteBuffer bf]
  (let [items  (GrowingIntArray.)
        _      (.addAll items (i/get-ints bf))
        order  (.order bf)
        src    (doto (.slice bf) (.order order))
        direct (doto (ByteBuffer/allocateDirect (.remaining src))
                 (.order order))]
    (.put direct src)
    (.flip direct)
    (->SparseIntArrayList (ImmutableRoaringBitmap. direct) items)))

(defmethod print-method SparseIntArrayList
  [^SparseIntArrayList s ^Writer w]
  (.write w (str "#datalevin/SparseList "))
  (binding [*out* w]
    (pr (for [i (.-indices s)] [i (get s i)]))))
