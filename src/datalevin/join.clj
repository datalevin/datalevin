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
   [java.util List HashMap Collection]
   [org.eclipse.collections.impl.list.mutable FastList]))

;; hash join

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
    (if (== n 1)
      (let [getter (getter-fn attrs (first common-attrs))]
        [getter getter])
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
  (let [^HashMap res (HashMap.)]
    (when tuples
      (dotimes [i (.size tuples)]
        (let [x (.get tuples i)
              k (key-fn x)]
          (if-let [^List l (.get res k)]
            (.add l x)
            (let [^List nl (FastList.)]
              (.add nl x)
              (.put res k nl))))))
    res))

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
        [key-fn1 lookup-key-fn1] (tuple-key-fns attrs1 common-attrs)
        [key-fn2 lookup-key-fn2] (tuple-key-fns attrs2 common-attrs)
        attrs        (zipmap (concatv keep-attrs1 keep-attrs2) (range))]
    (if (or (nil? tuples1) (nil? tuples2))
      (r/relation! attrs (FastList.))
      (if (< (.size tuples1) (.size tuples2))
        (r/relation!
          attrs
          (let [acc           (FastList.)
                ^HashMap hash (hash-tuples key-fn1 tuples1)]
            (dotimes [i (.size tuples2)]
              (let [^objects tuple2 (.get tuples2 i)]
                (when-some [^List tuples1 (.get hash (lookup-key-fn2 tuple2))]
                  (dotimes [j (.size tuples1)]
                    (.add acc (r/join-tuples (.get tuples1 j) keep-idxs1
                                             tuple2 keep-idxs2))))))
            acc))
        (r/relation!
          attrs
          (let [acc           (FastList.)
                ^HashMap hash (hash-tuples key-fn2 tuples2)]
            (dotimes [i (.size tuples1)]
              (let [^objects tuple1 (.get tuples1 i)]
                (when-some [^List tuples2 (.get hash (lookup-key-fn1 tuple1))]
                  (dotimes [j (.size tuples2)]
                    (.add acc (r/join-tuples tuple1 keep-idxs1
                                             (.get tuples2 j) keep-idxs2))))))
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
        [key-fn1 lookup-key-fn1] (tuple-key-fns attrs1 common-attrs)
        [key-fn2 lookup-key-fn2] (tuple-key-fns attrs2 common-attrs)]
    (when (and tuples1 tuples2)
      (if (< (.size tuples1) (.size tuples2))
        (let [^HashMap hash (hash-tuples key-fn1 tuples1)]
          (dotimes [i (.size tuples2)]
            (let [^objects tuple2 (.get tuples2 i)]
              (when-some [^List tuples1 (.get hash (lookup-key-fn2 tuple2))]
                (dotimes [j (.size tuples1)]
                  (.add ^Collection sink
                        (r/join-tuples (.get tuples1 j) keep-idxs1
                                       tuple2 keep-idxs2)))))))
        (let [^HashMap hash (hash-tuples key-fn2 tuples2)]
          (dotimes [i (.size tuples1)]
            (let [^objects tuple1 (.get tuples1 i)]
              (when-some [^List tuples2 (.get hash (lookup-key-fn1 tuple1))]
                (dotimes [j (.size tuples2)]
                  (.add ^Collection sink
                        (r/join-tuples tuple1 keep-idxs1
                                       (.get tuples2 j) keep-idxs2)))))))))
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
