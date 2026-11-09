;;
;; Copyright (c) Nikita Prokopov, Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns ^:no-doc datalevin.datom
  "Datom object"
  (:require
   [taoensso.nippy :as nippy]
   [datalevin.constants :refer [tx0]]
   [datalevin.util :refer [array? combine-hashes combine-cmp defcomp]])
  (:import
   [datalevin.utl BitOps QueryKey]
   [java.util Arrays]
   [java.io DataInput DataOutput]))

(declare hash-datom equiv-datom seq-datom nth-datom assoc-datom
         empty-datom val-at-datom)

(defprotocol IDatom
  (datom-tx [this])
  (datom-added [this]))

;; Giant preparation shares the metadata slot, preserving the ordinary datom
;; layout. IMeta exposes only the caller's metadata.
(deftype ^:private StorageMeta [metadata reference])

(defprotocol ^:no-doc IStorageReference
  (storage-reference [this])
  (set-storage-reference! [this reference]))

(deftype Datom [^long e a v ^long tx
                ^:unsynchronized-mutable ^int _hash
                ^:unsynchronized-mutable _meta]
  IDatom
  (datom-tx [d] (if (pos? tx) tx (- tx)))
  (datom-added [d] (pos? tx))

  IStorageReference
  (storage-reference [_]
    (when (instance? StorageMeta _meta) (.-reference ^StorageMeta _meta)))
  (set-storage-reference! [this reference]
    (let [metadata (if (instance? StorageMeta _meta)
                     (.-metadata ^StorageMeta _meta) _meta)]
      (set! _meta (if reference (StorageMeta. metadata reference) metadata)))
    this)

  Object
  (hashCode [d]
    (if (zero? _hash)
      (let [h (int (hash-datom d))]
        (set! _hash h)
        h)
      _hash))
  (toString [d] (pr-str d))

  clojure.lang.IHashEq
  (hasheq [d] (.hashCode d))

  clojure.lang.IMeta
  (meta [_]
    (if (instance? StorageMeta _meta) (.-metadata ^StorageMeta _meta) _meta))

  clojure.lang.IObj
  (withMeta [this meta]
    (Datom. e a v tx _hash
            (if (instance? StorageMeta _meta)
              (StorageMeta. meta (.-reference ^StorageMeta _meta)) meta)))

  clojure.lang.Seqable
  (seq [d] (seq-datom d))

  clojure.lang.IPersistentCollection
  (equiv [d o] (and (instance? Datom o) (equiv-datom d o)))
  (empty [d] (empty-datom))
  (count [d] 5)
  (cons [d [k v]] (assoc-datom d k v))

  clojure.lang.Indexed
  (nth [this i] (nth-datom this i))
  (nth [this i not-found] (nth-datom this i not-found))

  clojure.lang.ILookup
  (valAt [d k] (val-at-datom d k nil))
  (valAt [d k nf] (val-at-datom d k nf))

  clojure.lang.Associative
  (entryAt [d k] (some->> (val-at-datom d k nil) (clojure.lang.MapEntry k)))
  (containsKey [_ k] (#{:e :a :v :tx :added} k))
  (assoc [d k v] (assoc-datom d k v)))

(defn ^Datom datom
  ([e a v] (Datom. e a v tx0 0 nil))
  ([e a v tx] (Datom. e a v tx 0 nil))
  ([e a v tx added]
   (Datom. e a v (if added tx (- ^long tx)) 0 nil)))

(defn delete
  "create a datom that means deleting it"
  [^Datom d]
  (datom (.-e d) (.-a d) (.-v d) (.-tx d) false))

(defn datom? [x] (instance? Datom x))

(defn value-equal?
  "Compare byte arrays by content, including in ordinary collections.
   Other datom values retain Clojure equality."
  [expected actual]
  (QueryKey/equiv expected actual))

(defn- hash-datom [^Datom d]
  (let [v (.-v d)]
    (-> (hash (.-e d))
        (combine-hashes (hash (.-a d)))
        (combine-hashes (QueryKey/equivHash v)))))

(defn- equiv-datom [^Datom d ^Datom o]
  (and (== (.-e d) (.-e o))
       (= (.-a d) (.-a o))
       (value-equal? (.-v d) (.-v o))))

(defn- seq-datom [^Datom d] (list [:e (.-e d)] [:a (.-a d)] [:v (.-v d)]))

(defn- val-at-datom
  [^Datom d k not-found]
  (cond
    (keyword? k)
    (case k
      :e     (.-e d)
      :a     (.-a d)
      :v     (.-v d)
      :tx    (datom-tx d)
      :added (datom-added d)
      not-found)

    (string? k)
    (case k
      "e"     (.-e d)
      "a"     (.-a d)
      "v"     (.-v d)
      "tx"    (datom-tx d)
      "added" (datom-added d)
      not-found)

    :else
    not-found))

(defn- nth-datom
  ([^Datom d ^long i]
   (case i
     0 (.-e d)
     1 (.-a d)
     2 (.-v d)
     3 (datom-tx d)
     4 (datom-added d)
     (throw (IndexOutOfBoundsException.))))
  ([^Datom d ^long i not-found]
   (case i
     0 (.-e d)
     1 (.-a d)
     2 (.-v d)
     3 (datom-tx d)
     4 (datom-added d)
     not-found)))

(defn- ^Datom empty-datom [] (datom 0 nil nil 0 0))

(defn- ^Datom assoc-datom [^Datom d k v]
  (let [m  (meta d)
        d' (case k
             :e     (datom v (.-a d) (.-v d) (datom-tx d) (datom-added d))
             :a     (datom (.-e d) v (.-v d) (datom-tx d) (datom-added d))
             :v     (datom (.-e d) (.-a d) v (datom-tx d) (datom-added d))
             :tx    (datom (.-e d) (.-a d) (.-v d) v (datom-added d))
             :added (datom (.-e d) (.-a d) (.-v d) (datom-tx d) v)
             (throw (IllegalArgumentException.
                      (str "invalid key for #datalevin/Datom: " k))))]
    (if m (with-meta d' m) d')))

;; printing and reading

(defn datom-from-reader ^Datom [vec] (apply datom vec))

(defmethod print-method Datom [^Datom d, ^java.io.Writer w]
  (.write w (str "#datalevin/Datom "))
  (binding [*out* w]
    (pr [(.-e d) (.-a d) (.-v d)])))

;; ----------------------------------------------------------------------------
;; datom cmp macros/funcs
;;

(defn nil-check-cmp-fn [cmp-fn]
  (fn nil-check-cmp [o1 o2]
    (if (nil? o1) 0
        (if (nil? o2) 0
            (cmp-fn o1 o2)))))

(declare compare-collision-values)

(defn- compare-collision-seqs
  [xs ys]
  (let [n (Long/compare (count xs) (count ys))]
    (if (zero? n)
      (if (and (indexed? xs) (indexed? ys))
        (let [size (count xs)]
          (loop [i 0]
            (if (< i size)
              (let [n (long (compare-collision-values (nth xs i) (nth ys i)))]
                (if (zero? n) (recur (inc i)) n))
              0)))
        (loop [xs (seq xs) ys (seq ys)]
          (if xs
            (let [n (long (compare-collision-values (first xs) (first ys)))]
              (if (zero? n) (recur (next xs) (next ys)) n))
            0)))
      n)))

(defn- collision-kind
  [x]
  (cond
    (record? x) (class x)
    (map? x) :map
    (set? x) :set
    (sequential? x) :sequential
    (number? x) :number
    (inst? x) :instant
    :else (class x)))

(defn- numeric-kind
  [x]
  (cond (integer? x) :integer
        (ratio? x) :ratio
        (decimal? x) :decimal
        (float? x) :floating
        :else (class x)))

(defn- compare-collision-values
  "Order collection values structurally, independent of iteration order.
   Equivalent collection implementations and numeric classes share a kind."
  [a b]
  (cond
    (identical? a b)
    0

    (and (bytes? a) (bytes? b))
    (BitOps/compareBytes a b)

    (and (sequential? a) (sequential? b))
    (compare-collision-seqs a b)

    (= a b)
    0

    :else
    (let [ak (collision-kind a)
          bk (collision-kind b)
          kind-order (long (if (identical? ak bk) 0 (compare (str ak) (str bk))))
          n (long
              (if-not (zero? kind-order)
                kind-order
                (cond
                  (map? a)
                  ;; Content-equal array keys can occur more than once in a
                  ;; map. Order complete entries to break those key ties.
                  (compare-collision-seqs (sort compare-collision-values a)
                                          (sort compare-collision-values b))

                  (set? a)
                  (compare-collision-seqs (sort compare-collision-values a)
                                          (sort compare-collision-values b))

                  :else
                  (let [n (long (try (compare a b) (catch ClassCastException _ 0)))
                        n (long (if (and (zero? n) (number? a))
                                  (compare (str (numeric-kind a))
                                           (str (numeric-kind b))) n))]
                    (if (zero? n) (Long/compare (hash a) (hash b)) n)))))]
      ;; Structural equality can now hold for collections whose array leaves
      ;; differ by identity. Only ambiguous opaque leaves must be rejected.
      (if (or (not (zero? n)) (map? a) (set? a))
        n
        ;; An opaque value needs its own ordering contract. Reject an
        ;; ambiguous comparison rather than silently dropping a fact.
        (throw (ex-info "Cannot order unequal values with colliding hashes"
                        {:error :datom/ambiguous-order :left a :right b}))))))

(defn compare-with-type
  ([a b] (compare-with-type a b false))
  ([a b order-opaque?]
   (try
     (cond
       ;; Comparable vectors and non-Comparable sequences must use the same
       ;; ordering, including when they occur inside another collection.
       (and (coll? a) (coll? b))
       (compare-collision-values a b)

       (and (array? a) (identical? (class a) (class b)))
       ;; Pending transaction indexes must distinguish vector contents even
       ;; when every value has the same array class and serialized prefix.
       (case (.getName (class a))
         "[B" (BitOps/compareBytes a b)
         "[S" (Arrays/compare ^shorts a ^shorts b)
         "[I" (Arrays/compare ^ints a ^ints b)
         "[J" (Arrays/compare ^longs a ^longs b)
         "[F" (Arrays/compare ^floats a ^floats b)
         "[D" (Arrays/compare ^doubles a ^doubles b)
         "[C" (Arrays/compare ^chars a ^chars b)
         "[Z" (Arrays/compare ^booleans a ^booleans b)
         (compare (vec a) (vec b)))
       :else (compare a b))
     (catch ClassCastException _
       (if order-opaque?
         ;; Pending indexes need distinct entries, including for regex values.
         ;; Reuse nested-value ordering and reject ambiguous hash ties.
         (compare-collision-values a b)
         ;; Query grouping also handles custom values with their own equality
         ;; and ordering contract; retain its coarse type fallback.
         (compare (str (collision-kind a)) (str (collision-kind b))))))))

(defn compare-index-values
  "Keep numeric equality families and unequal opaque values distinct in pending
   indexes. Query predicates still use compare-with-type's coarse fallback."
  [a b]
  (if (and (not (identical? (class a) (class b)))
           (number? a) (number? b))
    (let [ak (numeric-kind a)
          bk (numeric-kind b)]
      (if (identical? ak bk)
        (compare a b)
        (compare (str ak) (str bk))))
    (compare-with-type a b true)))

(def nil-cmp (nil-check-cmp-fn compare))
(def nil-cmp-type (nil-check-cmp-fn compare-index-values))

(defmacro long-compare [x y] `(Long/compare ^long ~x ^long ~y))

(defcomp cmp-datoms-eavt [^Datom d1, ^Datom d2]
  (combine-cmp
    (long-compare (.-e d1) (.-e d2))
    (nil-cmp (.-a d1) (.-a d2))
    (nil-cmp-type (.-v d1) (.-v d2))
    (long-compare (datom-tx d1) (datom-tx d2))))

(defcomp cmp-datoms-avet [^Datom d1, ^Datom d2]
  (combine-cmp
    (nil-cmp (.-a d1) (.-a d2))
    (nil-cmp-type (.-v d1) (.-v d2))
    (long-compare (.-e d1) (.-e d2))
    (long-compare (datom-tx d1) (datom-tx d2))))

(defn datom-e [^Datom d] (.-e d))

(defn datom-a [^Datom d] (.-a d))

(defn datom-v [^Datom d] (.-v d))

(defn datom-eav [^Datom d] [(.-e d) (.-a d) (.-v d)])

(def datom->vec-xf (map datom-eav))

(nippy/extend-freeze Datom :datalevin/datom
    [^Datom x ^DataOutput out]
  (.writeLong out (.-e x))
  (nippy/freeze-to-out! out (.-a x))
  (nippy/freeze-to-out! out (.-v x))
  (when-let [tx (.-tx x)]
    (nippy/freeze-to-out! out tx)))

(nippy/extend-thaw :datalevin/datom
                   [^DataInput in]
  (let [vs [(.readLong in)
            (nippy/thaw-from-in! in)
            (nippy/thaw-from-in! in)]
        tx (nippy/thaw-from-in! in)]
                     (datom-from-reader (if tx
                                          (conj vs tx)
                                          vs))))
