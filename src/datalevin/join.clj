;;
;; Copyright (c) Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns ^:no-doc datalevin.join
  "Join algorithms"
  (:require
   [datalevin.relation :as r]
   [datalevin.db :as db]
   [datalevin.query-util :as qu]
   [datalevin.util :as u :refer [concatv]])
  (:import
   [datalevin.utl ArrayUtil LongPairObjectHashMap]
   [java.util List HashMap Collection Objects]
   [org.eclipse.collections.impl.list.mutable FastList]
   [org.eclipse.collections.impl.map.mutable.primitive LongObjectHashMap]))

;; hash join

;; Amortize the primitive map's three backing arrays. The pair-join kernel
;; crosses over the generic fixed-width key between 64 and 256 build rows.
(def ^:private ^:const long-pair-hash-min-tuples 256)

(definterface ^:private IPairLookup
  (^Object resetPair [^Object a ^Object b ^int h]))

(deftype ^:private PairKey [a b ^int h]
  Object
  (hashCode [_] h)
  (equals [_ that]
    (and (instance? PairKey that)
         (Objects/equals a (.-a ^PairKey that))
         (Objects/equals b (.-b ^PairKey that)))))

(deftype ^:private PairLookup [^:unsynchronized-mutable a
                               ^:unsynchronized-mutable b
                               ^:unsynchronized-mutable ^int h]
  IPairLookup
  (resetPair [this a' b' h']
    (set! a a')
    (set! b b')
    (set! h h')
    this)
  Object
  (hashCode [_] h)
  (equals [_ that]
    (and (instance? PairKey that)
         (Objects/equals a (.-a ^PairKey that))
         (Objects/equals b (.-b ^PairKey that)))))

(defn- resolve-eid
  [eid]
  (cond
    (number? eid)     eid
    (sequential? eid) (db/entid qu/*implicit-source* eid)
    :else             eid))

(defn getter-fn
  [attrs attr]
  (let [idx (attrs attr)]
    (if (contains? qu/*lookup-attrs* attr)
      (fn contained-int-getter-fn [^objects tuple]
        (resolve-eid (aget tuple idx)))
      (fn int-getter [^objects tuple] (aget tuple idx)))))

(defn- tuple-key-fns
  [attrs common-attrs]
  (let [n (count common-attrs)]
    (cond
      (== n 1)
      (let [getter (getter-fn attrs (first common-attrs))]
        [getter getter])

      (== n 2)
      (let [attr-a   (first common-attrs)
            attr-b   (second common-attrs)
            getter-a (getter-fn attrs attr-a)
            getter-b (getter-fn attrs attr-b)]
        [(fn build-pair-key [^objects tuple]
           (let [a (getter-a tuple)
                 b (getter-b tuple)]
             (PairKey. a b (ArrayUtil/hashObjectPair a b))))
         (let [lookup (PairLookup. nil nil (int 0))]
           (fn lookup-pair-key [^objects tuple]
             (let [a (getter-a tuple)
                   b (getter-b tuple)]
               (.resetPair ^IPairLookup lookup a b
                           (ArrayUtil/hashObjectPair a b)))))])

      :else
      (let [^ints idxs        (int-array n)
            ^booleans resolve (boolean-array n)
            resolve?          (boolean
                                (some #(contains? qu/*lookup-attrs* %)
                                      common-attrs))]
        (loop [i 0, common-attrs (seq common-attrs)]
          (when common-attrs
            (let [attr (first common-attrs)
                  r?   (contains? qu/*lookup-attrs* attr)]
              (aset idxs i (int (attrs attr)))
              (when r? (aset resolve i true))
              (recur (unchecked-inc i) (next common-attrs)))))
        (if resolve?
          [(fn build-resolved-tuple-key [^objects tuple]
             (let [^objects arr (object-array n)]
               (dotimes [i n]
                 (let [v (aget tuple (aget idxs i))]
                   (aset arr i (if (aget resolve i) (resolve-eid v) v))))
               (r/wrap-array arr)))
           (let [^objects scratch (object-array n)
                 lookup           (r/array-lookup)]
             (fn lookup-resolved-tuple-key [^objects tuple]
               (dotimes [i n]
                 (let [v (aget tuple (aget idxs i))]
                   (aset scratch i (if (aget resolve i) (resolve-eid v) v))))
               (r/reset-array-lookup! lookup scratch)))]
          [(fn build-tuple-key [^objects tuple]
             (let [^objects arr (object-array n)]
               (dotimes [i n]
                 (aset arr i (aget tuple (aget idxs i))))
               (r/wrap-array arr)))
           (let [^objects scratch (object-array n)
                 lookup           (r/array-lookup)]
             (fn lookup-tuple-key [^objects tuple]
               (dotimes [i n]
                 (aset scratch i (aget tuple (aget idxs i))))
               (r/reset-array-lookup! lookup scratch)))])))))

(defn tuple-key-fn
  [attrs common-attrs]
  (first (tuple-key-fns attrs common-attrs)))

(defn hash-tuples
  [key-fn ^List tuples]
  (let [size         (if tuples (.size tuples) 0)
        capacity     (min Integer/MAX_VALUE
                          (inc (quot (* (long size) 4) 3)))
        ^HashMap res (HashMap. (int capacity))]
    (when tuples
      (dotimes [i size]
        (let [x (.get tuples i)
              k (key-fn x)]
          (if-let [bucket (.get res k)]
            (if (u/array? bucket)
              (.put res k (doto (FastList. 2)
                            (.add bucket)
                            (.add x)))
              (.add ^List bucket x))
            (.put res k x)))))
    res))

(defn- hash-long-tuples
  [key-fn ^List tuples]
  (let [size                    (.size tuples)
        ^LongObjectHashMap res  (LongObjectHashMap. size)]
    (loop [i 0]
      (if (< i size)
        (let [x (.get tuples i)
              k (key-fn x)]
          (if (instance? Long k)
            (let [k      (.longValue ^Long k)
                  bucket (.get res k)]
              (if bucket
                (if (u/array? bucket)
                  (.put res k (doto (FastList. 2)
                                (.add bucket)
                                (.add x)))
                  (.add ^List bucket x))
                (.put res k x))
              (recur (unchecked-inc i)))
            nil))
        res))))

(defn- long-bucket
  [^LongObjectHashMap hash k]
  (when (instance? Long k)
    (.get hash (.longValue ^Long k))))

(defn- long-pair-indexes
  [attrs common-attrs]
  (when (and (== 2 (count common-attrs))
             (not-any? #(contains? qu/*lookup-attrs* %) common-attrs))
    (int-array [(int (attrs (first common-attrs)))
                (int (attrs (second common-attrs)))])))

(defn- long-pair-tuple?
  [^ints idxs ^objects tuple]
  (and (instance? Long (aget tuple (aget idxs 0)))
       (instance? Long (aget tuple (aget idxs 1)))))

(defn- sampled-long-pair-tuples?
  [^ints idxs ^List tuples]
  (let [size (.size tuples)]
    (or (zero? size)
        (and (long-pair-tuple? idxs (.get tuples 0))
             (long-pair-tuple? idxs (.get tuples (quot size 2)))
             (long-pair-tuple? idxs (.get tuples (unchecked-dec size)))))))

(defn- hash-long-pair-tuples
  [^ints idxs ^List tuples]
  (let [size (.size tuples)]
    (when (and (<= long-pair-hash-min-tuples size)
               (sampled-long-pair-tuples? idxs tuples))
      (let [^LongPairObjectHashMap res (LongPairObjectHashMap. size)
            idx-a                      (aget idxs 0)
            idx-b                      (aget idxs 1)]
        (loop [i 0]
          (if (< i size)
            (let [^objects x (.get tuples i)
                  a          (aget x idx-a)
                  b          (aget x idx-b)]
              (if (and (instance? Long a) (instance? Long b))
                (let [a (.longValue ^Long a)
                      b (.longValue ^Long b)]
                  (.add res a b x)
                  (recur (unchecked-inc i)))
                nil))
            res))))))

(defn- long-pair-bucket
  [^LongPairObjectHashMap hash ^ints idxs ^objects tuple]
  (let [a (aget tuple (aget idxs 0))
        b (aget tuple (aget idxs 1))]
    (when (and (instance? Long a) (instance? Long b))
      (let [a (long (.longValue ^Long a))
            b (long (.longValue ^Long b))]
        (.get hash a b)))))

(defn- attr-keys
  "attrs are map, preserve order by val"
  [attrs]
  (->> attrs (sort-by val) (mapv key)))

(defn- diff-keys
  "return (- vec2 vec1) elements"
  [vec1 vec2]
  (persistent!
    (reduce
      (fn [d e2]
        (if (some (fn [e1] (= e1 e2)) vec1)
          d
          (conj! d e2)))
      (transient []) vec2)))

(defn hash-join
  [rel1 rel2]
  (let [tuples1      ^List (:tuples rel1)
        tuples2      ^List (:tuples rel2)
        attrs1       (:attrs rel1)
        attrs2       (:attrs rel2)
        common-attrs (qu/intersect-keys attrs1 attrs2)
        keep-attrs1  (attr-keys attrs1)
        keep-attrs2  (diff-keys keep-attrs1 (attr-keys attrs2))
        keep-idxs1   (int-array (sort (vals attrs1)))
        keep-idxs2   (int-array (->Eduction (map attrs2) keep-attrs2))
        pair-idxs1   (long-pair-indexes attrs1 common-attrs)
        pair-idxs2   (long-pair-indexes attrs2 common-attrs)
        [key-fn1 lookup-key-fn1] (tuple-key-fns attrs1 common-attrs)
        [key-fn2 lookup-key-fn2] (tuple-key-fns attrs2 common-attrs)
        attrs        (zipmap (concatv keep-attrs1 keep-attrs2) (range))]
    (if (or (nil? tuples1) (nil? tuples2))
      (r/relation! attrs (FastList.))
      (if (< (.size tuples1) (.size tuples2))
        (r/relation!
          attrs
          (let [acc                         (FastList.)
                ^LongPairObjectHashMap pair-hash
                (when pair-idxs1
                  (hash-long-pair-tuples pair-idxs1 tuples1))
                ^LongObjectHashMap lhash (when (== 1 (count common-attrs))
                                           (hash-long-tuples key-fn1 tuples1))
                ^HashMap hash            (when-not (or pair-hash lhash)
                                           (hash-tuples key-fn1 tuples1))]
            (dotimes [i (.size tuples2)]
              (let [^objects tuple2 (.get tuples2 i)
                    k               (when-not pair-hash
                                      (lookup-key-fn2 tuple2))]
                (when-some [bucket (cond
                                     pair-hash
                                     (long-pair-bucket pair-hash pair-idxs2
                                                       tuple2)

                                     lhash (long-bucket lhash k)
                                     :else (.get hash k))]
                  (if (u/array? bucket)
                    (.add acc (r/join-tuples bucket keep-idxs1
                                             tuple2 keep-idxs2))
                    (let [^List tuples1 bucket]
                      (dotimes [j (.size tuples1)]
                        (.add acc (r/join-tuples (.get tuples1 j) keep-idxs1
                                                 tuple2 keep-idxs2))))))))
            acc))
        (r/relation!
          attrs
          (let [acc                         (FastList.)
                ^LongPairObjectHashMap pair-hash
                (when pair-idxs2
                  (hash-long-pair-tuples pair-idxs2 tuples2))
                ^LongObjectHashMap lhash (when (== 1 (count common-attrs))
                                           (hash-long-tuples key-fn2 tuples2))
                ^HashMap hash            (when-not (or pair-hash lhash)
                                           (hash-tuples key-fn2 tuples2))]
            (dotimes [i (.size tuples1)]
              (let [^objects tuple1 (.get tuples1 i)
                    k               (when-not pair-hash
                                      (lookup-key-fn1 tuple1))]
                (when-some [bucket (cond
                                     pair-hash
                                     (long-pair-bucket pair-hash pair-idxs1
                                                       tuple1)

                                     lhash (long-bucket lhash k)
                                     :else (.get hash k))]
                  (if (u/array? bucket)
                    (.add acc (r/join-tuples tuple1 keep-idxs1
                                             bucket keep-idxs2))
                    (let [^List tuples2 bucket]
                      (dotimes [j (.size tuples2)]
                        (.add acc (r/join-tuples tuple1 keep-idxs1
                                                 (.get tuples2 j)
                                                 keep-idxs2))))))))
            acc))))))

(defn hash-join-into
  [rel1 rel2 sink]
  (let [tuples1      ^List (:tuples rel1)
        tuples2      ^List (:tuples rel2)
        attrs1       (:attrs rel1)
        attrs2       (:attrs rel2)
        common-attrs (qu/intersect-keys attrs1 attrs2)
        keep-attrs1  (attr-keys attrs1)
        keep-attrs2  (diff-keys keep-attrs1 (attr-keys attrs2))
        keep-idxs1   (int-array (sort (vals attrs1)))
        keep-idxs2   (int-array (->Eduction (map attrs2) keep-attrs2))
        pair-idxs1   (long-pair-indexes attrs1 common-attrs)
        pair-idxs2   (long-pair-indexes attrs2 common-attrs)
        [key-fn1 lookup-key-fn1] (tuple-key-fns attrs1 common-attrs)
        [key-fn2 lookup-key-fn2] (tuple-key-fns attrs2 common-attrs)]
    (when (and tuples1 tuples2)
      (if (< (.size tuples1) (.size tuples2))
        (let [^LongPairObjectHashMap pair-hash
              (when pair-idxs1
                (hash-long-pair-tuples pair-idxs1 tuples1))
              ^LongObjectHashMap lhash (when (== 1 (count common-attrs))
                                         (hash-long-tuples key-fn1 tuples1))
              ^HashMap hash            (when-not (or pair-hash lhash)
                                         (hash-tuples key-fn1 tuples1))]
          (dotimes [i (.size tuples2)]
            (let [^objects tuple2 (.get tuples2 i)
                  k               (when-not pair-hash
                                    (lookup-key-fn2 tuple2))]
              (when-some [bucket (cond
                                   pair-hash
                                   (long-pair-bucket pair-hash pair-idxs2
                                                     tuple2)

                                   lhash (long-bucket lhash k)
                                   :else (.get hash k))]
                (if (u/array? bucket)
                  (.add ^Collection sink
                        (r/join-tuples bucket keep-idxs1 tuple2 keep-idxs2))
                  (let [^List tuples1 bucket]
                    (dotimes [j (.size tuples1)]
                      (.add ^Collection sink
                            (r/join-tuples (.get tuples1 j) keep-idxs1
                                           tuple2 keep-idxs2)))))))))
        (let [^LongPairObjectHashMap pair-hash
              (when pair-idxs2
                (hash-long-pair-tuples pair-idxs2 tuples2))
              ^LongObjectHashMap lhash (when (== 1 (count common-attrs))
                                         (hash-long-tuples key-fn2 tuples2))
              ^HashMap hash            (when-not (or pair-hash lhash)
                                         (hash-tuples key-fn2 tuples2))]
          (dotimes [i (.size tuples1)]
            (let [^objects tuple1 (.get tuples1 i)
                  k               (when-not pair-hash
                                    (lookup-key-fn1 tuple1))]
              (when-some [bucket (cond
                                   pair-hash
                                   (long-pair-bucket pair-hash pair-idxs1
                                                     tuple1)

                                   lhash (long-bucket lhash k)
                                   :else (.get hash k))]
                (if (u/array? bucket)
                  (.add ^Collection sink
                        (r/join-tuples tuple1 keep-idxs1 bucket keep-idxs2))
                  (let [^List tuples2 bucket]
                    (dotimes [j (.size tuples2)]
                      (.add ^Collection sink
                            (r/join-tuples tuple1 keep-idxs1
                                           (.get tuples2 j)
                                           keep-idxs2)))))))))))
    sink))

(defn subtract-rel
  [a b]
  (let [{attrs-a :attrs, tuples-a :tuples} a
        {attrs-b :attrs, tuples-b :tuples} b

        attrs    (qu/intersect-keys attrs-a attrs-b)
        key-fn-b (tuple-key-fn attrs-b attrs)
        hash     ^HashMap (hash-tuples key-fn-b tuples-b)
        key-fn-a (second (tuple-key-fns attrs-a attrs))]
    (assoc a :tuples (let [res (FastList.)]
                       (dotimes [i (.size ^List tuples-a)]
                         (let [t (.get ^List tuples-a i)]
                           (when (nil? (.get hash (key-fn-a t)))
                             (.add res t))))
                       res))))
