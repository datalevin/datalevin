;;
;; Copyright (c) Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns ^:no-doc datalevin.relation
  "Functions for relational algebra"
  (:require
   [clojure.pprint :as pp]
   [datalevin.util :as u :refer [raise]]
   [datalevin.timeout :as timeout])
  (:import
   [java.util List Arrays HashSet]
   [java.io Writer]
   [org.eclipse.collections.impl.list.mutable FastList]))

(deftype ArrayWrapper [^objects a ^int h]
  Object
  (hashCode [_] h)
  (equals [_ that]
    (and (instance? ArrayWrapper that)
         (Arrays/equals a ^objects (.-a ^ArrayWrapper that)))))

(defn wrap-array [^objects a] (ArrayWrapper. a (Arrays/hashCode a)))

;; attrs: {?e 0, ?v 1}
;; tuples is a list of objects: [ objects ... ]
(defrecord Relation [attrs tuples])

(defmethod print-method Relation [^Relation r, ^Writer w]
  (binding [*out* w]
    (let [{:keys [attrs tuples]} r]
      (pp/pprint {:attrs attrs :tuples (mapv vec tuples)}))))

(defn relation!
  [attrs tuples]
  (timeout/assert-time-left)
  (Relation. attrs tuples))

;; Relation algebra

(defn join-tuples
  ([^objects t1 ^objects t2]
   (let [l1  (alength t1)
         l2  (alength t2)
         res (object-array (+ l1 l2))]
     (System/arraycopy t1 0 res 0 l1)
     (System/arraycopy t2 0 res l1 l2)
     res))
  ([^objects t1 ^objects idxs1 ^objects t2 ^objects idxs2]
   (let [l1  (alength idxs1)
         l2  (alength idxs2)
         res (object-array (+ l1 l2))]
     (dotimes [i l1] (aset res i (aget t1 (aget idxs1 i))))
     (dotimes [i l2] (aset res (+ l1 i) (aget t2 (aget idxs2 i))))
     res)))

(defn conj-tuple
  [^objects tuple item]
  (let [len (alength tuple)
        res (object-array (inc len))]
    (System/arraycopy tuple 0 res 0 len)
    (aset res len item)
    res))

(defn same-keys?
  [a b]
  (and (= (count a) (count b))
       (every? #(contains? b %) (keys a))
       (every? #(contains? a %) (keys b))))

(defn- attrs-tuple-length
  ^long [attrs]
  (loop [entries (seq attrs)
         max-idx (long -1)]
    (if entries
      (let [[_ idx] (first entries)
            idx     (long idx)]
        (recur (next entries)
               (if (> idx max-idx) idx max-idx)))
      (unchecked-inc max-idx))))

(defn- sum-rel*
  [attrs-a ^List tuples-a attrs-b ^List tuples-b]
  (let [n            (count attrs-b)
        ^ints idxs-b (int-array n)
        ^ints idxs-a (int-array n)
        _            (loop [i 0, entries (seq attrs-b)]
                       (when entries
                         (let [[sym idx-b] (first entries)]
                           (aset idxs-b i (int idx-b))
                           (aset idxs-a i (int (attrs-a sym)))
                           (recur (unchecked-inc i) (next entries)))))
        tlen         (attrs-tuple-length attrs-a)
        size-a       (.size tuples-a)
        size-b       (.size tuples-b)]
    (if (< 0 size-b)
      (relation!
        attrs-a
        (let [res (FastList. (+ size-a size-b))]
          (.addAll res tuples-a)
          (dotimes [i size-b]
            (let [^objects tuple-b (.get tuples-b i)
                  ^objects tuple   (object-array tlen)]
              (dotimes [j n]
                (aset tuple (aget idxs-a j) (aget tuple-b (aget idxs-b j))))
              (.add res tuple)))
          res))
      (relation! attrs-a tuples-a))))

(defn sum-rel
  ([] (relation! {} (FastList.)))
  ([a] a)
  ([a b]
   (let [{attrs-a :attrs, tuples-a :tuples} a
         {attrs-b :attrs, tuples-b :tuples} b]

     (cond
       (= attrs-a attrs-b)
       (relation! attrs-a (let [size-a (.size ^List tuples-a)
                                size-b (.size ^List tuples-b)]
                            (doto (FastList. (+ size-a size-b))
                              (.addAll tuples-a)
                              (.addAll tuples-b))))

       (empty? tuples-a) b
       (empty? tuples-b) a

       (not (same-keys? attrs-a attrs-b))
       (raise
         "Can’t sum relations with different attrs: " attrs-a " and " attrs-b
         {:error :query/where})

       (every? number? (vals attrs-a))
       (sum-rel* attrs-a tuples-a attrs-b tuples-b)

       :else
       (let [number-attrs (zipmap (keys attrs-a) (range))
             size-a       (.size ^List tuples-a)
             size-b       (.size ^List tuples-b)]
         (-> (sum-rel* number-attrs (FastList. (+ size-a size-b))
                       attrs-a tuples-a)
             (sum-rel b)))))))

(defn dedupe-rel
  [rel]
  (let [tuples ^List (:tuples rel)]
    (if (or (nil? tuples) (zero? (.size tuples)))
      rel
      (assoc rel :tuples
             (let [tset (HashSet.)
                   new  (FastList.)]
               (dotimes [i (.size tuples)]
                 (let [t  (.get tuples i)
                       tw (wrap-array t)]
                   (when-not (.contains tset tw)
                     (.add new t)
                     (.add tset tw))))
               new)))))

(defn sum-rel-dedupe
  ([] (relation! {} (FastList.)))
  ([a] a)
  ([a b]
   (let [attrs-a  (:attrs a)
         attrs-b  (:attrs b)
         tuples-a (:tuples a)
         tuples-b (:tuples b)]
     (cond
       (or (nil? tuples-a) (zero? (.size ^List tuples-a))) b
       (or (nil? tuples-b) (zero? (.size ^List tuples-b))) a

       (= attrs-a attrs-b)
       (relation!
         attrs-a
         (let [^List tuples-a tuples-a
               ^List tuples-b tuples-b
               la             (.size tuples-a)
               lb             (.size tuples-b)
               res            (FastList. (+ la lb))
               seen           (HashSet.)]
           (dotimes [i la]
             (let [t  (.get tuples-a i)
                   tw (wrap-array t)]
               (when-not (.contains seen tw)
                 (.add seen tw)
                 (.add res t))))
           (dotimes [i lb]
             (let [t  (.get tuples-b i)
                   tw (wrap-array t)]
               (when-not (.contains seen tw)
                 (.add seen tw)
                 (.add res t))))
           res))

       (not (same-keys? attrs-a attrs-b))
       (raise
         "Can’t sum relations with different attrs: " attrs-a " and " attrs-b
         {:error :query/where})

       (every? number? (vals attrs-a))
       (let [n              (count attrs-b)
             ^ints idxs-b   (int-array n)
             ^ints idxs-a   (int-array n)
             _              (loop [i 0, entries (seq attrs-b)]
                             (when entries
                                (let [[sym idx-b] (first entries)]
                                  (aset idxs-b i (int idx-b))
                                  (aset idxs-a i (int (attrs-a sym)))
                                  (recur (unchecked-inc i) (next entries)))))
             tlen           (attrs-tuple-length attrs-a)
             ^List tuples-a tuples-a
             ^List tuples-b tuples-b
             la             (.size tuples-a)
             lb             (.size tuples-b)
             res            (FastList. (+ la lb))
             seen           (HashSet. (int (+ la lb)))]
         (dotimes [i la]
           (let [t  (.get tuples-a i)
                 tw (wrap-array t)]
             (when-not (.contains seen tw)
               (.add seen tw)
               (.add res t))))
         (dotimes [i lb]
           (let [^objects tuple-b (.get tuples-b i)
                 ^objects tuple   (object-array tlen)]
             (dotimes [j n]
               (aset tuple (aget idxs-a j) (aget tuple-b (aget idxs-b j))))
             (let [tw (wrap-array tuple)]
               (when-not (.contains seen tw)
                 (.add seen tw)
                 (.add res tuple)))))
         (relation! attrs-a res))

       :else
       (dedupe-rel (sum-rel a b))))))

(defn prod-tuples
  ([] (doto (FastList.) (.add (object-array []))))
  ([tuples] tuples)
  ([^List tuples1 ^List tuples2]
   (let [l1  (.size tuples1)
         l2  (.size tuples2)
         acc (FastList. (* l1 l2))]
     (dotimes [i l1]
       (dotimes [j l2]
         (.add acc (join-tuples (.get tuples1 i) (.get tuples2 j)))))
     acc)))

(defn prod-rel
  ([] (relation! {} (doto (FastList.) (.add (make-array Object 0)))))
  ([rel1] rel1)
  ([rel1 rel2]
   (let [attrs1 (keys (:attrs rel1))
         attrs2 (keys (:attrs rel2))
         idxs1  (to-array (->Eduction (map (:attrs rel1)) attrs1))
         idxs2  (to-array (->Eduction (map (:attrs rel2)) attrs2))]
     (relation!
       (zipmap (u/concatv attrs1 attrs2) (range))
       (let [tuples1 ^List (:tuples rel1)
             tuples2 ^List (:tuples rel2)
             l1      (.size tuples1)
             l2      (.size tuples2)
             acc     (FastList. (* l1 l2))]
         (dotimes [i l1]
           (dotimes [j l2]
             (.add acc (join-tuples (.get tuples1 i) idxs1
                                    (.get tuples2 j) idxs2))))
         acc)))))

(defn vertical-tuples [coll] (u/map-fl #(object-array [%]) coll))

(defn single-tuples [tuple] (doto (FastList.) (.add tuple)))

(defn many-tuples [values] (transduce (map vertical-tuples) prod-tuples values))

(defn difference
  "Returns r1 - r2. Assumes r1 and r2 have same attrs."
  [r1 r2]
  (let [^List t1 (:tuples r1)
        ^List t2 (:tuples r2)]
    (if (.isEmpty t2)
      r1
      (assoc r1 :tuples (let [s2         (HashSet.)
                              new-tuples (FastList.)]
                          (dotimes [i (.size t2)]
                            (.add s2 (wrap-array (.get t2 i))))
                          (dotimes [i (.size t1)]
                            (let [tuple (.get t1 i)]
                              (when-not (.contains s2 (wrap-array tuple))
                                (.add new-tuples tuple))))
                          new-tuples)))))

(defn difference-with-seen!
  "Returns tuples from r1 not already in seen-set. Mutates seen-set by adding
   new tuples. More efficient than difference for iterative algorithms."
  [r1 ^HashSet seen-set]
  (let [^List t1 (:tuples r1)]
    (if (or (nil? t1) (.isEmpty t1))
      r1
      (assoc r1 :tuples (let [new-tuples (FastList.)]
                          (dotimes [i (.size t1)]
                            (let [tuple (.get t1 i)
                                  tw    (wrap-array tuple)]
                              (when (.add seen-set tw)
                                (.add new-tuples tuple))))
                          new-tuples)))))

(defn add-to-seen!
  "Add all tuples from relation to seen-set. Returns the seen-set."
  [rel ^HashSet seen-set]
  (let [^List tuples (:tuples rel)]
    (when (and tuples (pos? (.size tuples)))
      (dotimes [i (.size tuples)]
        (.add seen-set (wrap-array (.get tuples i)))))
    seen-set))

(defn select-tuples
  [pred ^List tuples]
  (let [res (FastList.)]
    (dotimes [i (.size tuples)]
      (let [t (.get tuples i)]
        (when (pred t)
          (.add res t))))
    res))

(defn rel-not-empty
  [rel]
  (let [tuples (:tuples rel)]
    (and tuples (< 0 (.size ^List tuples)))))

(defn rel-empty
  [rel]
  (let [tuples (:tuples rel)]
    (or (nil? tuples) (zero? (.size ^List tuples)))))
