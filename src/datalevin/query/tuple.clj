;;
;; Copyright (c) Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns ^:no-doc datalevin.query.tuple
  "Projection and emission of tuples produced by query access methods."
  (:require
   [datalevin.datom :as dd]
   [datalevin.index :as idx]
   [datalevin.parser])
  (:import
   [datalevin.parser BindIgnore BindScalar BindTuple]))

(defrecord TupleProjection
    [cols attrs source-attrs needed source-width output-width])

(defn tuple-binding-projection
  "Compile a flat tuple binding into the physical and compact schemas used by
  tuple-producing query functions. Returns nil for nested bindings or duplicate
  variables, which require the general relation-binding path."
  [^BindTuple binding]
  (loop [source-i    0
         output-i    0
         bindings    (:bindings binding)
         cols        []
         attrs       {}
         source-attrs {}]
    (if-some [b (first bindings)]
      (cond
        (instance? BindScalar b)
        (let [sym (get-in b [:variable :symbol])]
          (when-not (contains? attrs sym)
            (recur (unchecked-inc-int source-i)
                   (unchecked-inc-int output-i)
                   (next bindings)
                   (conj cols sym)
                   (assoc attrs sym output-i)
                   (assoc source-attrs sym source-i))))

        (instance? BindIgnore b)
        (recur (unchecked-inc-int source-i)
               output-i
               (next bindings)
               cols
               attrs
               source-attrs)

        :else nil)
      (let [needed (when (< output-i source-i)
                     (int-array (map source-attrs cols)))]
        (->TupleProjection cols attrs source-attrs needed
                           source-i output-i)))))

(defn needed-indices
  "Return the physical tuple indices retained by a tuple binding, or nil when
  every column is retained."
  ^ints [^BindTuple binding]
  (:needed (tuple-binding-projection binding)))

(defn giant-doc-ref?
  [doc-ref]
  (and (vector? doc-ref)
       (identical? :g (first doc-ref))))

(defn rich-giant-doc-ref?
  [doc-ref]
  (and (giant-doc-ref? doc-ref)
       (< 3 (count doc-ref))))

(defn doc-ref->eav
  [lmdb aid->attr doc-ref]
  (if (and (vector? doc-ref)
           (= :g (first doc-ref)))
    (let [d (idx/gt->datom lmdb (second doc-ref))]
      [(dd/datom-e d) (dd/datom-a d) (dd/datom-v d)])
    [(nth doc-ref 0)
     (aid->attr (nth doc-ref 1))
     (peek doc-ref)]))

(defn make-datom-emitter
  "Build an emitter for access-method document references. The optional
  `needed` array contains source tuple indices, allowing ignored bindings to be
  omitted without decoding unused giant values."
  [lmdb aid->attr ^ints needed]
  (if needed
    (let [n            (alength needed)
          needs-value? (loop [i 0]
                         (cond
                           (== i n) false
                           (== 2 (aget needed i)) true
                           :else (recur (inc i))))
          needs-ref?   (loop [i 0]
                         (cond
                           (== i n) false
                           (< (aget needed i) 2) true
                           :else (recur (inc i))))]
      (letfn [(emit [doc-ref value-provided? value]
                (let [giant?      (giant-doc-ref? doc-ref)
                      rich-giant? (rich-giant-doc-ref? doc-ref)
                      datom        (when (and
                                           giant?
                                           (or
                                             (and
                                               needs-ref?
                                               (not rich-giant?))
                                             (and
                                               needs-value?
                                               (not value-provided?))))
                                     (idx/gt->datom lmdb (second doc-ref)))
                      ^objects arr (object-array n)]
                  (dotimes [j n]
                    (aset arr j (case (aget needed j)
                                  0 (cond
                                      rich-giant? (nth doc-ref 2)
                                      giant?      (dd/datom-e datom)
                                      :else       (nth doc-ref 0))
                                  1 (cond
                                      rich-giant? (aid->attr (nth doc-ref 3))
                                      giant?      (dd/datom-a datom)
                                      :else       (aid->attr (nth doc-ref 1)))
                                  2 (if value-provided?
                                      value
                                      (if giant?
                                        (dd/datom-v datom)
                                        (peek doc-ref))))))
                  arr))]
        (fn
          ([doc-ref] (emit doc-ref false nil))
          ([doc-ref value] (emit doc-ref true value)))))
    (fn
      ([doc-ref]
       (object-array (doc-ref->eav lmdb aid->attr doc-ref)))
      ([doc-ref value]
       (if (rich-giant-doc-ref? doc-ref)
         (object-array [(nth doc-ref 2)
                        (aid->attr (nth doc-ref 3))
                        value])
         (object-array (doc-ref->eav lmdb aid->attr doc-ref)))))))

(defn make-fulltext-emitter
  "Build an emitter for full-text results, preserving optional score, text, or
  offsets columns while compacting ignored reference columns."
  [lmdb aid->attr display ^ints needed]
  (if needed
    (let [n          (alength needed)
          ref-n      (loop [i 0, cnt 0]
                       (if (< i n)
                         (recur (inc i)
                                (if (< (aget needed i) 3)
                                  (inc cnt)
                                  cnt))
                         cnt))
          ref-needed (int-array ref-n)]
      (loop [j 0, ref-j 0]
        (when (< j n)
          (let [idx (aget needed j)]
            (if (< idx 3)
              (do
                (aset-int ref-needed ref-j idx)
                (recur (inc j) (inc ref-j)))
              (recur (inc j) ref-j)))))
      (let [refs-only? (= :refs display)
            emit-ref   (when (pos? (long ref-n))
                         (make-datom-emitter lmdb aid->attr ref-needed))]
        (fn [result]
          (let [doc-ref        (if refs-only? result (nth result 0))
                ^objects tuple (when emit-ref (emit-ref doc-ref))
                ^objects arr   (object-array n)]
            (loop [j 0, ref-j 0]
              (when (< j n)
                (let [idx (aget needed j)]
                  (if (< idx 3)
                    (do
                      (aset arr j (aget tuple ref-j))
                      (recur (inc j) (inc ref-j)))
                    (do
                      (aset arr j
                            (when-not refs-only?
                              (nth result (- idx 2) nil)))
                      (recur (inc j) ref-j))))))
            arr))))
    (let [refs-only? (= :refs display)
          extra-n    (case display
                       :texts+offsets 2
                       :refs         0
                       1)
          emit-ref   (make-datom-emitter lmdb aid->attr nil)]
      (fn [result]
        (let [doc-ref        (if refs-only? result (nth result 0))
              ^objects tuple (emit-ref doc-ref)
              ^objects arr   (object-array (+ 3 extra-n))]
          (dotimes [i 3]
            (aset arr i (aget tuple i)))
          (dotimes [i extra-n]
            (aset arr (+ 3 i) (nth result (inc i))))
          arr)))))

(defn make-vector-emitter
  "Build an emitter for vector-search results, preserving the optional
  distance column while compacting ignored reference columns."
  [lmdb aid->attr display ^ints needed]
  (if (= :refs+dists display)
    (if needed
      (let [n          (alength needed)
            ref-n      (loop [j 0, cnt 0]
                         (if (< j n)
                           (recur (inc j)
                                  (if (< (aget needed j) 3)
                                    (inc cnt)
                                    cnt))
                           cnt))
            ref-needed (int-array ref-n)]
        (loop [j 0, ref-j 0]
          (when (< j n)
            (let [idx (aget needed j)]
              (if (< idx 3)
                (do
                  (aset-int ref-needed ref-j idx)
                  (recur (inc j) (inc ref-j)))
                (recur (inc j) ref-j)))))
        (let [emit-ref (when (pos? (long ref-n))
                         (make-datom-emitter lmdb aid->attr ref-needed))]
          (fn [result]
            (let [doc-ref        (nth result 0)
                  dist           (nth result 1)
                  ^objects tuple (when emit-ref (emit-ref doc-ref))
                  ^objects arr   (object-array n)]
              (loop [j 0, ref-j 0]
                (when (< j n)
                  (if (== 3 (aget needed j))
                    (do
                      (aset arr j dist)
                      (recur (inc j) ref-j))
                    (do
                      (aset arr j (aget tuple ref-j))
                      (recur (inc j) (inc ref-j))))))
              arr))))
      (let [emit-ref (make-datom-emitter lmdb aid->attr nil)]
        (fn [result]
          (let [^objects tuple (emit-ref (nth result 0))]
            (object-array [(aget tuple 0)
                           (aget tuple 1)
                           (aget tuple 2)
                           (nth result 1)])))))
    (make-datom-emitter lmdb aid->attr needed)))
