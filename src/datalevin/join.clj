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
   [java.util BitSet Collection HashMap HashSet List Map$Entry]
   [org.eclipse.collections.impl.list.mutable FastList]
   [org.eclipse.collections.impl.map.mutable.primitive LongObjectHashMap]
   [org.roaringbitmap PeekableIntIterator RoaringBitmap]))

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

(def ^:private ^:const dense-composition-min-candidates 65536)
(def ^:private ^:const dense-composition-min-fanout 4)
(def ^:private ^:const dense-composition-max-bitset-bytes
  (* 64 1024 1024))
(def ^:private ^:const dense-composition-sample-size 512)
(def ^:private ^:const dense-composition-min-sample-repeats 16)
(def ^:private ^:const dense-composition-sample-threshold 4096)
(def ^:private ^:const dense-composition-roaring-min-domain 4096)

(defn- composition-bitmap
  [roaring? ^long domain-size]
  (if roaring?
    (RoaringBitmap.)
    (BitSet. (int domain-size))))

(defn- composition-bitmap-add!
  [roaring? bitmap ^long ordinal]
  (if roaring?
    (.add ^RoaringBitmap bitmap (int ordinal))
    (.set ^BitSet bitmap (int ordinal))))

(defn- composition-bitmap-cardinality
  ^long [roaring? bitmap]
  (if roaring?
    (.getCardinality ^RoaringBitmap bitmap)
    (.cardinality ^BitSet bitmap)))

(defn- composition-bitmap-or!
  [roaring? target source]
  (if roaring?
    (.or ^RoaringBitmap target ^RoaringBitmap source)
    (.or ^BitSet target ^BitSet source)))

(defn- clone-composition-bitmap
  [roaring? bitmap]
  (if roaring?
    (.clone ^RoaringBitmap bitmap)
    (.clone ^BitSet bitmap)))

(defn- sampled-key-reuse?
  [^List tuples key-fn]
  (let [n             (min (.size tuples) dense-composition-sample-size)
        ^HashSet seen (HashSet. n)]
    (loop [i 0, repeated 0]
      (cond
        (>= repeated dense-composition-min-sample-repeats) true
        (= i n) false
        :else
        (recur (unchecked-inc-int i)
               (if (.add seen (key-fn (.get tuples i)))
                 repeated
                 (unchecked-inc repeated)))))))

(defn ^:no-doc dense-binary-composition
  "Use bitset unions for a dense two-relation composition. Returns nil when
   the shape or measured relation density does not justify the representation."
  [rel1 rel2 vars]
  (let [^List tuples1 (:tuples rel1)
        ^List tuples2 (:tuples rel2)
        attrs1        (:attrs rel1)
        attrs2        (:attrs rel2)
        vars          (vec vars)
        common-attrs  (qu/intersect-keys attrs1 attrs2)
        vars1         (filterv #(and (contains? attrs1 %)
                                     (not (contains? attrs2 %)))
                               vars)
        vars2         (filterv #(and (contains? attrs2 %)
                                     (not (contains? attrs1 %)))
                               vars)]
    (when (and (= 2 (count vars))
               (seq common-attrs)
               (= 1 (count vars1))
               (= 1 (count vars2))
               (not-any? #(contains? qu/*lookup-attrs* %) vars))
      (let [hash-first?  (< (.size tuples1) (.size tuples2))
            ^List hash-tuples (if hash-first? tuples1 tuples2)
            ^List scan-tuples (if hash-first? tuples2 tuples1)
            hash-attrs   (if hash-first? attrs1 attrs2)
            scan-attrs   (if hash-first? attrs2 attrs1)
            value-var    (if hash-first? (first vars1) (first vars2))
            anchor-var   (if hash-first? (first vars2) (first vars1))
            value-idx    (int (hash-attrs value-var))
            anchor-idx   (int (scan-attrs anchor-var))
            [hash-key-fn _] (tuple-key-fns hash-attrs common-attrs)
            [scan-build-key-fn scan-key-fn]
            (tuple-key-fns scan-attrs common-attrs)
            input-size     (long (+ (long (.size hash-tuples))
                                    (.size scan-tuples)))
            ^HashMap value-ordinals (HashMap.)
            ^FastList ordinal-values (FastList.)
            ^HashMap join-counts (HashMap.)]
        ;; Avoid an exact costing pass when a bounded sample finds no evidence
        ;; that either side reuses join keys enough for dense composition.
        (when (or (< input-size dense-composition-sample-threshold)
                  (sampled-key-reuse? hash-tuples hash-key-fn)
                  (sampled-key-reuse? scan-tuples scan-build-key-fn))
          ;; Establish a compact value domain and exact hash-side key counts.
          (dotimes [i (.size hash-tuples)]
            (let [^objects tuple (.get hash-tuples i)
                  value          (aget tuple value-idx)]
              (when-not (.containsKey value-ordinals value)
                (.put value-ordinals value (.size ordinal-values))
                (.add ordinal-values value))
              (let [join-key (hash-key-fn tuple)
                    n        (.get join-counts join-key)]
                (.put join-counts join-key
                      (unchecked-inc (long (or n 0)))))))
          ;; Count the other output domain and exact number of proof pairs.
          (let [^HashSet anchors (HashSet.)
                candidate-pairs
                (long
                  (loop [i 0, total 0]
                    (if (< i (.size scan-tuples))
                      (let [^objects tuple (.get scan-tuples i)
                            _              (.add anchors
                                                 (aget tuple anchor-idx))
                            n              (.get join-counts
                                                 (scan-key-fn tuple))
                            n              (long (or n 0))
                            total          (if (> total (- Long/MAX_VALUE n))
                                             Long/MAX_VALUE
                                             (unchecked-add total n))]
                        (recur (unchecked-inc-int i) total))
                      total)))
                domain-size  (.size ordinal-values)
                anchor-count (.size anchors)
                bitset-words (long (quot (+ (long domain-size) 63) 64))
                bitset-bytes (long (* bitset-words 8
                                      (+ (long anchor-count)
                                         (.size join-counts))))
                roaring?     (> domain-size
                                dense-composition-roaring-min-domain)]
            (when (and (pos? domain-size)
                       (pos? anchor-count)
                       (>= candidate-pairs dense-composition-min-candidates)
                       (>= candidate-pairs
                           (* dense-composition-min-fanout input-size))
                       (<= bitset-bytes dense-composition-max-bitset-bytes))
              (let [^HashMap adjacency (HashMap.)]
                ;; Each join key maps to the distinct projected values
                ;; reachable on the hash side.
                (dotimes [i (.size hash-tuples)]
                  (let [^objects tuple (.get hash-tuples i)
                        join-key       (hash-key-fn tuple)
                        values         (or (.get adjacency join-key)
                                           (let [values
                                                 (composition-bitmap
                                                   roaring? domain-size)]
                                             (.put adjacency join-key values)
                                             values))
                        ^Number ordinal
                        (.get value-ordinals (aget tuple value-idx))]
                    (composition-bitmap-add!
                      roaring? values (.intValue ordinal))))
                ;; Composition becomes an OR of complete value sets instead of
                ;; one hash-set insertion for every proof pair.
                (let [^HashMap groups (HashMap.)
                      ^longs total-output (long-array 1)]
                  (dotimes [i (.size scan-tuples)]
                    (let [^objects tuple (.get scan-tuples i)
                          values         (.get adjacency (scan-key-fn tuple))]
                      (when values
                        (let [anchor        (aget tuple anchor-idx)
                              known         (.get groups anchor)]
                          (if known
                            (when (< (composition-bitmap-cardinality
                                      roaring? known)
                                     domain-size)
                              (let [before (composition-bitmap-cardinality
                                             roaring? known)]
                                (composition-bitmap-or!
                                  roaring? known values)
                                (aset total-output 0
                                      (unchecked-add
                                        (aget total-output 0)
                                        (long
                                          (- (composition-bitmap-cardinality
                                               roaring? known)
                                             before))))))
                            (let [copy (clone-composition-bitmap
                                         roaring? values)]
                              (.put groups anchor copy)
                              (aset total-output 0
                                    (unchecked-add
                                      (aget total-output 0)
                                      (composition-bitmap-cardinality
                                        roaring? copy)))))))))
                  (let [anchor-pos (int (.indexOf ^List vars anchor-var))
                        value-pos  (int (.indexOf ^List vars value-var))
                        capacity   (int (Math/min (aget total-output 0)
                                                  (long 1000000)))
                        output     (FastList. capacity)]
                    (doseq [^Map$Entry entry (.entrySet groups)]
                      (let [anchor (.getKey entry)
                            known  (.getValue entry)
                            emit!  (fn [^long ordinal]
                                     (let [tuple (object-array 2)]
                                       (aset tuple anchor-pos anchor)
                                       (aset tuple value-pos
                                             (.get ordinal-values ordinal))
                                       (.add output tuple)))]
                        (if roaring?
                          (let [^PeekableIntIterator iter
                                (.getIntIterator ^RoaringBitmap known)]
                            (while (.hasNext iter)
                              (emit! (.next iter))))
                          (loop [ordinal (.nextSetBit ^BitSet known 0)]
                            (when-not (neg? ordinal)
                              (emit! ordinal)
                              (recur (.nextSetBit
                                       ^BitSet known
                                       (unchecked-inc-int ordinal))))))))
                    (with-meta
                      (r/relation! (zipmap vars (range)) output)
                      {::dense-composition
                       {:candidate-pairs candidate-pairs
                        :input-tuples   input-size
                        :output-tuples  (aget total-output 0)
                        :anchor-count   anchor-count
                        :domain-size    domain-size
                        :bitmap         (if roaring? :roaring :dense)
                        :memory-limit   dense-composition-max-bitset-bytes
                        :memory-upper-bound bitset-bytes}})))))))))))

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
      (if-some [composed (dense-binary-composition rel1 rel2 vars)]
        composed
        (let [common-attrs   (qu/intersect-keys attrs1 attrs2)
              hash-first?    (< (.size tuples1) (.size tuples2))
              ^List hash-input-tuples (if hash-first? tuples1 tuples2)
              ^List scan-tuples (if hash-first? tuples2 tuples1)
              hash-attrs     (if hash-first? attrs1 attrs2)
              scan-attrs     (if hash-first? attrs2 attrs1)
              [hash-key-fn _] (tuple-key-fns hash-attrs common-attrs)
              [_ scan-key-fn] (tuple-key-fns scan-attrs common-attrs)
              ^LongObjectHashMap lhash
              (when (== 1 (count common-attrs))
                (hash-long-tuples hash-key-fn hash-input-tuples))
              ^HashMap hash   (when-not lhash
                                (hash-tuples hash-key-fn hash-input-tuples))
              value-vars      (filterv #(and (contains? hash-attrs %)
                                              (not (contains? scan-attrs %)))
                                       vars)
              anchor-vars     (filterv #(contains? scan-attrs %) vars)
              composition?    (and (seq value-vars)
                                    (not-any?
                                      #(contains? qu/*lookup-attrs* %) vars)
                                    (= (count vars)
                                       (+ (count anchor-vars)
                                          (count value-vars))))
              output-plan     (mapv (fn [var]
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
          (r/relation! output-attrs output))))))

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
