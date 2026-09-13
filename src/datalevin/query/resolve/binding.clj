;;
;; Copyright (c) Huahai Yang, Nikita Prokopov. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns ^:no-doc datalevin.query.resolve.binding
  "Tuple conversion and compiled products for input and function bindings."
  (:refer-clojure :exclude [assoc])
  (:require
   [datalevin.inline :refer [assoc]]
   [datalevin.parser :as dp]
   [datalevin.relation :as r]
   [datalevin.util :as u :refer [raise]])
  (:import
   [java.util List]
   [datalevin.relation Relation]
   [org.eclipse.collections.impl.list.mutable FastList]))

(defn empty-rel
  ^Relation [binding]
  (let [vars (->> (dp/collect-vars-distinct binding)
                  (map :symbol))]
    (r/relation! (zipmap vars (range)) (FastList.))))

(defn tuple-list->rel
  [binding ^List tuples
   {:keys [attrs source-attrs needed source-width output-width]}]
  (let [size         (.size tuples)
        source-width (long source-width)
        output-width (long output-width)]
    (if (zero? size)
      (empty-rel binding)
      (let [t0 (.get tuples 0)]
        (if (u/array? t0)
          (let [^objects t0 t0]
            (when (< (alength t0) source-width)
              (raise "Not enough elements in a collection " tuples
                     " to bind tuple " (dp/source binding)
                     {:error   :query/binding
                      :value   tuples
                      :binding (dp/source binding)}))
            (r/relation! source-attrs tuples))
          (let [^ints src-idxs
                (or needed (int-array (range source-width)))
                res (FastList. size)]
            (dotimes [i size]
              (let [row (.get tuples i)]
                (when-not (u/seqable? row)
                  (raise "Cannot bind value " row " to tuple "
                         (dp/source (:binding binding))
                         {:error   :query/binding
                          :value   row
                          :binding (dp/source (:binding binding))}))
                (when (< (count row) source-width)
                  (raise "Not enough elements in a collection " row
                         " to bind tuple "
                         (dp/source (:binding binding))
                         {:error   :query/binding
                          :value   row
                          :binding (dp/source (:binding binding))}))
                (let [tuple (object-array output-width)]
                  (dotimes [j output-width]
                    (aset tuple j (nth row (aget src-idxs j))))
                  (.add res tuple))))
            (r/relation! attrs res)))))))

(defn attach-needed-meta
  "Attach :tuple-needed metadata to the last argument or append a metadata map.
   Returns the modified args vector."
  [args ^ints needed]
  (let [v        (vec args)
        n        (count v)
        last-arg (when (pos? n) (peek v))
        meta-map (with-meta {} {:tuple-needed needed})]
    (cond
      (zero? n)
      [meta-map]

      (nil? last-arg)
      (assoc v (dec n) meta-map)

      (instance? clojure.lang.IObj last-arg)
      (assoc v (dec n) (with-meta last-arg {:tuple-needed needed}))

      :else
      (conj v meta-map))))

(defn bind-scalar-tuples
  [production out-var tuple-fn]
  (let [attrs        (:attrs production)
        attr-keys    (vec (keys attrs))
        n            (count attr-keys)
        ^ints idxs   (int-array (map attrs attr-keys))
        ^List tuples (:tuples production)
        size         (.size tuples)
        res          (FastList. size)]
    (dotimes [i size]
      (let [^objects tuple (.get tuples i)
            val            (tuple-fn tuple)]
        (when-not (nil? val)
          (let [^objects out (object-array (unchecked-inc n))]
            (dotimes [j n]
              (aset out j (aget tuple (aget idxs j))))
            (aset out n val)
            (.add res out)))))
    (r/relation! (zipmap (conj attr-keys out-var) (range)) res)))

(defn compile-tuple-product
  [left-attrs right-attrs]
  (let [left-vars        (vec (keys left-attrs))
        right-vars       (vec (keys right-attrs))
        common-vars      (into [] (filter #(contains? right-attrs %))
                               left-vars)
        new-right-vars   (into [] (remove #(contains? left-attrs %))
                               right-vars)]
    (object-array
      [right-attrs
       (zipmap (u/concatv left-vars new-right-vars) (range))
       (int-array (map left-attrs left-vars))
       (int-array (map right-attrs new-right-vars))
       (int-array (map left-attrs common-vars))
       (int-array (map right-attrs common-vars))])))

(defn- compile-flat-tuple-product
  [left-attrs {:keys [cols source-attrs source-width]}]
  (let [left-vars       (vec (keys left-attrs))
        common-vars     (into [] (filter #(contains? left-attrs %)) cols)
        new-right-vars  (into [] (remove #(contains? left-attrs %)) cols)]
    (object-array
      [(zipmap (u/concatv left-vars new-right-vars) (range))
       (int-array (map left-attrs left-vars))
       (int-array (map left-attrs common-vars))
       (int-array (map source-attrs common-vars))
       (int-array (map source-attrs new-right-vars))
       (long source-width)])))

(defn- flat-tuple-product-match?
  [^objects left-tuple value ^objects product-plan]
  (let [^ints left-idxs   (aget product-plan 2)
        ^ints source-idxs (aget product-plan 3)
        n                 (alength left-idxs)]
    (loop [i 0]
      (or (== i n)
          (and (= (aget left-tuple (aget left-idxs i))
                  (nth value (aget source-idxs i)))
               (recur (unchecked-inc-int i)))))))

(defn- append-flat-tuple-product!
  [^List res ^objects left-tuple value ^objects product-plan]
  (when (flat-tuple-product-match? left-tuple value product-plan)
    (let [^ints left-idxs   (aget product-plan 1)
          ^ints source-idxs (aget product-plan 4)
          left-size         (alength left-idxs)
          source-size       (alength source-idxs)
          ^objects out      (object-array (+ left-size source-size))]
      (dotimes [i left-size]
        (aset out i (aget left-tuple (aget left-idxs i))))
      (dotimes [i source-size]
        (aset out (+ left-size i) (nth value (aget source-idxs i))))
      (.add res out))))

(defn bind-flat-tuple-tuples
  "Bind a flat tuple-valued function directly into each production tuple.
  This avoids constructing and hash-joining a pair of temporary relations for
  every function result. Duplicate or nested tuple bindings keep using the
  general binding path because `tuple-binding-projection` rejects them."
  [production binding projection tuple-fn]
  (let [^List tuples (:tuples production)
        size         (.size tuples)
        res          (FastList. size)
        product-plan (compile-flat-tuple-product
                       (:attrs production) projection)
        source-width (long (aget ^objects product-plan 5))]
    (dotimes [i size]
      (let [^objects tuple (.get tuples i)
            value          (tuple-fn tuple)]
        (when-not (nil? value)
          (when-not (u/seqable? value)
            (raise "Cannot bind value " value " to tuple "
                   (dp/source binding)
                   {:error :query/binding, :value value,
                    :binding (dp/source binding)}))
          (when (< (count value) source-width)
            (raise "Not enough elements in a collection " value
                   " to bind tuple " (dp/source binding)
                   {:error :query/binding, :value value,
                    :binding (dp/source binding)}))
          (append-flat-tuple-product! res tuple value product-plan))))
    (r/relation! (aget ^objects product-plan 0) res)))

(defn- tuple-product-match?
  [^objects left-tuple ^objects right-tuple ^objects product-plan]
  (let [^ints left-idxs  (aget product-plan 4)
        ^ints right-idxs (aget product-plan 5)
        n                (alength left-idxs)]
    (loop [i 0]
      (or (== i n)
          (and (= (aget left-tuple (aget left-idxs i))
                  (aget right-tuple (aget right-idxs i)))
               (recur (unchecked-inc-int i)))))))

(defn append-tuple-product!
  [^List res ^objects left-tuple bound-rel ^objects product-plan]
  (let [^List right-tuples (:tuples bound-rel)
        ^ints left-idxs    (aget product-plan 2)
        ^ints right-idxs   (aget product-plan 3)
        size               (.size right-tuples)]
    (dotimes [i size]
      (let [^objects right-tuple (.get right-tuples i)]
        (when (tuple-product-match? left-tuple right-tuple product-plan)
          (.add res (r/join-tuples left-tuple left-idxs
                                   right-tuple right-idxs)))))))
