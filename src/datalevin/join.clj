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
   [java.util Collection HashMap HashSet List]
   [org.eclipse.collections.impl.list.mutable FastList]
   [org.eclipse.collections.impl.map.mutable.primitive LongObjectHashMap]))

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
          (let [acc                     (FastList.)
                ^LongObjectHashMap lhash (when (== 1 (count common-attrs))
                                           (hash-long-tuples key-fn1 tuples1))
                ^HashMap hash            (when-not lhash
                                           (hash-tuples key-fn1 tuples1))]
            (dotimes [i (.size tuples2)]
              (let [^objects tuple2 (.get tuples2 i)
                    k               (lookup-key-fn2 tuple2)]
                (when-some [bucket (if lhash
                                     (long-bucket lhash k)
                                     (.get hash k))]
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
          (let [acc                     (FastList.)
                ^LongObjectHashMap lhash (when (== 1 (count common-attrs))
                                           (hash-long-tuples key-fn2 tuples2))
                ^HashMap hash            (when-not lhash
                                           (hash-tuples key-fn2 tuples2))]
            (dotimes [i (.size tuples1)]
              (let [^objects tuple1 (.get tuples1 i)
                    k               (lookup-key-fn1 tuple1)]
                (when-some [bucket (if lhash
                                     (long-bucket lhash k)
                                     (.get hash k))]
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
        [key-fn1 lookup-key-fn1] (tuple-key-fns attrs1 common-attrs)
        [key-fn2 lookup-key-fn2] (tuple-key-fns attrs2 common-attrs)]
    (when (and tuples1 tuples2)
      (if (< (.size tuples1) (.size tuples2))
        (let [^LongObjectHashMap lhash (when (== 1 (count common-attrs))
                                         (hash-long-tuples key-fn1 tuples1))
              ^HashMap hash            (when-not lhash
                                         (hash-tuples key-fn1 tuples1))]
          (dotimes [i (.size tuples2)]
            (let [^objects tuple2 (.get tuples2 i)
                  k               (lookup-key-fn2 tuple2)]
              (when-some [bucket (if lhash
                                   (long-bucket lhash k)
                                   (.get hash k))]
                (if (u/array? bucket)
                  (.add ^Collection sink
                        (r/join-tuples bucket keep-idxs1 tuple2 keep-idxs2))
                  (let [^List tuples1 bucket]
                    (dotimes [j (.size tuples1)]
                      (.add ^Collection sink
                            (r/join-tuples (.get tuples1 j) keep-idxs1
                                           tuple2 keep-idxs2)))))))))
        (let [^LongObjectHashMap lhash (when (== 1 (count common-attrs))
                                         (hash-long-tuples key-fn2 tuples2))
              ^HashMap hash            (when-not lhash
                                         (hash-tuples key-fn2 tuples2))]
          (dotimes [i (.size tuples1)]
            (let [^objects tuple1 (.get tuples1 i)
                  k               (lookup-key-fn1 tuple1)]
              (when-some [bucket (if lhash
                                   (long-bucket lhash k)
                                   (.get hash k))]
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

(defn- projection-key-fn
  [attrs vars]
  (case (count vars)
    0 (let [singleton (Object.)] (fn [_] singleton))
    1 (let [idx (int (attrs (first vars)))]
        (fn [^objects tuple] (aget tuple idx)))
    (let [^ints idxs (int-array (map attrs vars))
          width      (alength idxs)]
      (fn [^objects tuple]
        (let [key (object-array width)]
          (dotimes [i width]
            (aset key i (aget tuple (aget idxs i))))
          (r/wrap-array key))))))

(defn hash-join-project-distinct
  "Hash-join two relations while retaining only distinct `vars`, without
   accumulating the complete proof relation. If projected columns split across
   the two inputs, stop probing an output group once it has reached the other
   input's complete projected domain."
  [rel1 rel2 vars]
  (let [^List tuples1 (:tuples rel1)
        ^List tuples2 (:tuples rel2)
        attrs1        (:attrs rel1)
        attrs2        (:attrs rel2)
        vars          (vec (distinct vars))
        available     (into (set (keys attrs1)) (keys attrs2))
        missing       (into [] (remove available) vars)
        _             (when (seq missing)
                        (u/raise "Cannot project missing joined attributes"
                                 {:missing   missing
                                  :available available}))
        output-attrs  (zipmap vars (range))
        output        (FastList.)]
    (if (or (nil? tuples1) (nil? tuples2)
            (zero? (.size tuples1)) (zero? (.size tuples2)))
      (r/relation! output-attrs output)
      (let [common-attrs (qu/intersect-keys attrs1 attrs2)
            hash-first?  (< (.size tuples1) (.size tuples2))
            ^List hash-input-tuples (if hash-first? tuples1 tuples2)
            ^List scan-tuples (if hash-first? tuples2 tuples1)
            hash-attrs    (if hash-first? attrs1 attrs2)
            scan-attrs    (if hash-first? attrs2 attrs1)
            [hash-key-fn _]
            (tuple-key-fns hash-attrs common-attrs)
            [_ scan-key-fn]
            (tuple-key-fns scan-attrs common-attrs)
            ^LongObjectHashMap lhash
            (when (== 1 (count common-attrs))
              (hash-long-tuples hash-key-fn hash-input-tuples))
            ^HashMap hash (when-not lhash
                            (hash-tuples hash-key-fn hash-input-tuples))
            value-vars    (filterv #(and (contains? hash-attrs %)
                                         (not (contains? scan-attrs %)))
                                   vars)
            anchor-vars   (filterv #(contains? scan-attrs %) vars)
            composition?  (and (seq value-vars)
                                (not-any? #(contains? qu/*lookup-attrs* %)
                                          vars)
                                (= (count vars)
                                   (+ (count anchor-vars)
                                      (count value-vars))))
            output-plan   (mapv (fn [var]
                                  (if-let [idx (get attrs1 var)]
                                    [0 (int idx)]
                                    [1 (int (attrs2 var))]))
                                vars)
            output-width  (count output-plan)
            output-scratch (object-array output-width)
            emit!
            (fn [^objects tuple1 ^objects tuple2]
              (dotimes [i output-width]
                (let [[side idx] (nth output-plan i)]
                  (aset output-scratch i
                        (aget (if (zero? ^long side) tuple1 tuple2)
                              ^long idx))))
              (.add output (aclone output-scratch)))]
        (if composition?
          (let [value-key     (projection-key-fn hash-attrs value-vars)
                anchor-key    (projection-key-fn scan-attrs anchor-vars)
                value-domain  (HashSet.)
                groups        (HashMap.)]
            (dotimes [i (.size hash-input-tuples)]
              (.add value-domain (value-key (.get hash-input-tuples i))))
            (let [domain-size (.size value-domain)]
              (dotimes [i (.size scan-tuples)]
                (let [^objects scan-tuple (.get scan-tuples i)
                      anchor               (anchor-key scan-tuple)
                      ^HashSet known       (.get groups anchor)]
                  (when-not (and known (= (.size known) domain-size))
                    (let [join-key (scan-key-fn scan-tuple)]
                      (when-some [bucket (if lhash
                                           (long-bucket lhash join-key)
                                           (.get hash join-key))]
                        (let [^HashSet known
                              (or known
                                  (let [values (HashSet.)]
                                    (.put groups anchor values)
                                    values))
                              accept!
                              (fn [^objects hash-tuple]
                                (when (.add known (value-key hash-tuple))
                                  (if hash-first?
                                    (emit! hash-tuple scan-tuple)
                                    (emit! scan-tuple hash-tuple))))]
                          (if (u/array? bucket)
                            (accept! bucket)
                            (let [^List bucket bucket]
                              (loop [j 0]
                                (when (and (< j (.size bucket))
                                           (< (.size known) domain-size))
                                  (accept! (.get bucket j))
                                  (recur (unchecked-inc-int j))))))))))))))
          (let [seen   (HashSet.)
                lookup (r/array-lookup)]
            (dotimes [i (.size scan-tuples)]
              (let [^objects scan-tuple (.get scan-tuples i)
                    join-key            (scan-key-fn scan-tuple)]
                (when-some [bucket (if lhash
                                     (long-bucket lhash join-key)
                                     (.get hash join-key))]
                  (let [accept!
                        (fn [^objects hash-tuple]
                          (let [tuple1 (if hash-first?
                                         hash-tuple scan-tuple)
                                tuple2 (if hash-first?
                                         scan-tuple hash-tuple)]
                            (dotimes [j output-width]
                              (let [[side idx] (nth output-plan j)]
                                (aset output-scratch j
                                      (aget ^objects
                                            (if (zero? ^long side)
                                              tuple1 tuple2)
                                            ^long idx))))
                            (r/reset-array-lookup! lookup output-scratch)
                            (when-not (.contains seen lookup)
                              (let [projected (aclone output-scratch)]
                                (.add seen (r/wrap-array projected))
                                (.add output projected)))))]
                    (if (u/array? bucket)
                      (accept! bucket)
                      (let [^List bucket bucket]
                        (dotimes [j (.size bucket)]
                          (accept! (.get bucket j)))))))))))
        (r/relation! output-attrs output)))))

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
