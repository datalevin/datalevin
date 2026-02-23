;;
;; Copyright (c) Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns ^:no-doc datalevin.scan
  "Index scan routines that use iterators"
  (:require
   [datalevin.bits :as b]
   [datalevin.spill :as sp]
   [datalevin.util :as u :refer [raise]]
   [datalevin.lmdb :as l]
   [datalevin.interface :as i])
  (:import
   [datalevin.spill SpillableVector]
   [clojure.lang Seqable IReduceInit]
   [java.nio ByteBuffer]
   [java.util Iterator]
   [java.lang AutoCloseable Iterable]))

(defn get-value
  [lmdb dbi-name k k-type v-type ignore-key?]
  (i/check-ready lmdb)
  (let [dbi (i/get-dbi lmdb dbi-name false)
        rtx (if (l/writing? lmdb)
              @(l/write-txn lmdb)
              (i/get-rtx lmdb))]
    (try
      (l/put-key rtx k k-type)
      (when-let [^ByteBuffer bb (l/get-kv dbi rtx)]
        (if ignore-key?
          (b/read-buffer bb v-type)
          [(b/expected-return k k-type) (b/read-buffer bb v-type)]))
      (catch Throwable e
        (raise "Fail to get-value: " e
               {:dbi dbi-name :k k :k-type k-type :v-type v-type}))
      (finally
        (when-not (l/writing? lmdb)
          (i/return-rtx lmdb rtx))))))

(defn get-rank
  [lmdb dbi-name k k-type]
  (i/check-ready lmdb)
  (let [dbi (i/get-dbi lmdb dbi-name false)
        rtx (if (l/writing? lmdb)
              @(l/write-txn lmdb)
              (i/get-rtx lmdb))]
    (try
      (l/put-key rtx k k-type)
      (l/get-key-rank dbi rtx)
      (catch Throwable e
        (raise "Fail to get-rank: " e
               {:dbi dbi-name :k k :k-type k-type}))
      (finally
        (when-not (l/writing? lmdb)
          (i/return-rtx lmdb rtx))))))

(defn get-by-rank
  [lmdb dbi-name rank k-type v-type ignore-key?]
  (i/check-ready lmdb)
  (let [dbi (i/get-dbi lmdb dbi-name false)
        rtx (if (l/writing? lmdb)
              @(l/write-txn lmdb)
              (i/get-rtx lmdb))]
    (try
      (when-let [[kb vb] (l/get-key-by-rank dbi rtx rank)]
        (if ignore-key?
          (b/read-buffer vb v-type)
          [(b/read-buffer kb k-type) (b/read-buffer vb v-type)]))
      (catch Throwable e
        (raise "Fail to get-by-rank: " e
               {:dbi dbi-name :rank rank :k-type k-type :v-type v-type}))
      (finally
        (when-not (l/writing? lmdb)
          (i/return-rtx lmdb rtx))))))

(defmacro scan
  ([call error]
   `(scan ~call ~error false))
  ([call error keep-rtx?]
   `(do
      (i/check-ready ~'lmdb)
      (let [~'dbi (i/get-dbi ~'lmdb ~'dbi-name false)
            ~'rtx (if (l/writing? ~'lmdb)
                    @(l/write-txn ~'lmdb)
                    (i/get-rtx ~'lmdb))
            ~'cur (l/get-cursor ~'dbi ~'rtx)]
        (try
          ~call
          (catch Throwable ~'e
            ~error)
          (finally
            (if (l/read-only? ~'rtx)
              (l/return-cursor ~'dbi ~'cur)
              (l/close-cursor ~'dbi ~'cur))
            (when-not (or (l/writing? ~'lmdb) ~keep-rtx?)
              (i/return-rtx ~'lmdb ~'rtx))))))))

(defn sample-kv
  [lmdb dbi-name n k-type v-type ignore-key?]
  (let [total     (i/entries lmdb dbi-name)
        indices   (u/reservoir-sampling total n)
        list-dbi? (i/list-dbi? lmdb dbi-name)
        sample
        (fn [^Iterator iter]
          (let [^SpillableVector holder
                (sp/new-spillable-vector nil (:spill-opts (i/env-opts lmdb)))]
            (loop []
              (if (.hasNext iter)
                (let [kv (.next iter)
                      v  (b/read-buffer (l/v kv) v-type)]
                  (.cons holder (if ignore-key?
                                  v
                                  [(b/read-buffer (l/k kv) k-type) v]))
                  (recur))
                holder))))]
    (when indices
      (if list-dbi?
        (scan
          (with-open [^AutoCloseable iter
                      (.iterator ^Iterable
                                 (l/iterate-list-sample
                                   dbi rtx cur indices [:all] k-type))]
            (sample iter))
          (raise "Fail to sample-kv: " e
                 {:dbi dbi-name :n n :k-type k-type :v-type v-type}))
        (scan
          (with-open [^AutoCloseable iter
                      (.iterator ^Iterable
                                 (l/iterate-key-sample
                                   dbi rtx cur indices [:all] k-type))]
            (sample iter))
          (raise "Fail to sample-kv: " e
                 {:dbi dbi-name :n n :k-type k-type :v-type v-type}))))))

(defn get-first
  [lmdb dbi-name k-range k-type v-type ignore-key?]
  (scan
    (with-open [^AutoCloseable iter
                (.iterator
                  ^Iterable (l/iterate-kv dbi rtx cur k-range k-type v-type))]
      (when (.hasNext ^Iterator iter)
        (let [kv (.next ^Iterator iter)
              v  (when (not= v-type :ignore)
                   (b/read-buffer (l/v kv) v-type))]
          (if ignore-key?
            (if v v true)
            [(b/read-buffer (l/k kv) k-type) v]))))
    (raise "Fail to get-first: " e
           {:dbi    dbi-name :k-range k-range
            :k-type k-type   :v-type  v-type})))

(defn get-first-n
  [lmdb dbi-name n k-range k-type v-type ignore-key?]
  (scan
    (with-open [^AutoCloseable iter
                (.iterator
                  ^Iterable (l/iterate-kv dbi rtx cur k-range k-type v-type))]
      (let [^SpillableVector holder
            (sp/new-spillable-vector nil (:spill-opts (i/env-opts lmdb)))]
        (loop [i 0]
          (if (and (< i ^long n) (.hasNext ^Iterator iter))
            (let [kv (.next ^Iterator iter)
                  v  (when (not= v-type :ignore)
                       (b/read-buffer (l/v kv) v-type))]
              (.cons holder (if ignore-key?
                              v
                              [(b/read-buffer (l/k kv) k-type) v]))
              (recur (inc i)))
            holder))))
    (raise "Fail to get-first-n: " e
           {:dbi    dbi-name :k-range k-range
            :k-type k-type   :v-type  v-type})))

(defn get-range
  [lmdb dbi-name k-range k-type v-type ignore-key?]
  (assert (not (and (= v-type :ignore) ignore-key?))
          "Cannot ignore both key and value")
  (scan
    (with-open [^AutoCloseable iter
                (.iterator
                  ^Iterable (l/iterate-kv dbi rtx cur k-range k-type v-type))]
      (let [^SpillableVector holder
            (sp/new-spillable-vector nil (:spill-opts (i/env-opts lmdb)))]
        (loop []
          (if (.hasNext ^Iterator iter)
            (let [kv (.next ^Iterator iter)
                  v  (when (not= v-type :ignore)
                       (b/read-buffer (l/v kv) v-type))]
              (.cons holder (if ignore-key?
                              v
                              [(b/read-buffer (l/k kv) k-type) v]))
              (recur))
            holder))))
    (raise "Fail to get-range: " e
           {:dbi    dbi-name :k-range k-range
            :k-type k-type   :v-type  v-type})))

(defn- range-seq*
  [lmdb dbi rtx cur k-range k-type v-type ignore-key?
   {:keys [batch-size] :or {batch-size 100}}]
  (assert (not (and (= v-type :ignore) ignore-key?))
          "Cannot ignore both key and value")
  (let [iter  (.iterator
                ^Iterable (l/iterate-kv dbi rtx cur k-range k-type v-type))
        item  (fn [kv]
                (let [v (when (not= v-type :ignore)
                          (b/read-buffer (l/v kv) v-type))]
                  (if ignore-key?
                    (if v v true)
                    [(b/read-buffer (l/k kv) k-type) v])))
        fetch (fn [^long k]
                (let [holder (transient [])]
                  (loop [i 0]
                    (if (.hasNext ^Iterator iter)
                      (let [kv (.next ^Iterator iter)]
                        (conj! holder (item kv))
                        (if (< i k)
                          (recur (inc i))
                          {:batch  (persistent! holder)
                           :next-k k}))
                      {:batch  (persistent! holder)
                       :next-k nil}))))]
    (reify
      Seqable
      (seq [_]
        (u/lazy-concat
          ((fn next [ret]
             (when (clojure.core/seq (:batch ret))
               (cons (:batch ret)
                     (when-some [k (:next-k ret)]
                       (lazy-seq (next (fetch k)))))))
           (fetch batch-size))))

      IReduceInit
      (reduce [_ rf init]
        (loop [acc init
               ret (fetch batch-size)]
          (if (clojure.core/seq (:batch ret))
            (let [acc (rf acc (:batch ret))]
              (if (reduced? acc)
                @acc
                (if-some [k (:next-k ret)]
                  (recur acc (fetch k))
                  acc)))
            acc)))

      AutoCloseable
      (close [_]
        (.close ^AutoCloseable iter)
        (i/return-rtx lmdb rtx))

      Object
      (toString [this] (str (apply list this))))))

(defn range-seq
  ([lmdb dbi-name k-range k-type v-type ignore-key?]
   (range-seq lmdb dbi-name k-range k-type v-type ignore-key? nil))
  ([lmdb dbi-name k-range k-type v-type ignore-key? opts]
   (scan
     (range-seq* lmdb dbi rtx cur k-range k-type v-type ignore-key? opts)
     (raise "Fail in range-seq: " e
            {:dbi    dbi-name :k-range k-range
             :k-type k-type   :v-type  v-type})
     true)))

(defn key-range
  [lmdb dbi-name k-range k-type]
  (scan
    (with-open [^AutoCloseable iter
                (.iterator
                  ^Iterable (l/iterate-key dbi rtx cur k-range k-type))]
      (let [^SpillableVector holder
            (sp/new-spillable-vector nil (:spill-opts (i/env-opts lmdb)))]
        (loop []
          (if (.hasNext ^Iterator iter)
            (let [kv (.next ^Iterator iter)]
              (.cons holder (b/read-buffer (l/k kv) k-type))
              (recur))
            holder))))
    (raise "Fail to get key-range: " e
           {:dbi dbi-name :k-range k-range :k-type k-type })))

(defn visit-key-range
  [lmdb dbi-name visitor k-range k-type raw-pred?]
  (scan
    (with-open [^AutoCloseable iter
                (.iterator ^Iterable (l/iterate-key dbi rtx cur k-range k-type))]
      (loop []
        (when (.hasNext ^Iterator iter)
          (let [k   (l/k (.next ^Iterator iter))
                res (if raw-pred?
                      (visitor k)
                      (visitor (b/read-buffer k k-type)))]
            (when-not (identical? res :datalevin/terminate-visit)
              (recur))))))
    (raise "Fail to visit key range: " e
           {:dbi dbi-name :k-range k-range :k-type k-type })))

(defn get-some
  [lmdb dbi-name pred k-range k-type v-type ignore-key? raw-pred?]
  (assert (not (and (= v-type :ignore) ignore-key?))
          "Cannot ignore both key and value")
  (scan
    (with-open [^AutoCloseable iter
                (.iterator
                  ^Iterable (l/iterate-kv dbi rtx cur k-range k-type v-type))]
      (loop []
        (when (.hasNext ^Iterator iter)
          (let [kv             (.next ^Iterator iter)
                ^ByteBuffer kb (l/k kv)
                ^ByteBuffer vb (l/v kv)]
            (if raw-pred?
              (if (pred kv)
                (let [v (when (not= v-type :ignore)
                          (b/read-buffer (.rewind vb) v-type))]
                  (if ignore-key?
                    v [(b/read-buffer (.rewind kb) k-type) v]))
                (recur))
              (let [rk (b/read-buffer kb k-type)
                    rv (b/read-buffer vb v-type)]
                (if (pred rk rv)
                  (let [v (when (not= v-type :ignore) rv)]
                    (if ignore-key? v [rk v]))
                  (recur))))))))
    (raise "Fail to get-some: " e
           {:dbi    dbi-name :k-range k-range
            :k-type k-type   :v-type  v-type})))

(defn range-filter
  [lmdb dbi-name pred k-range k-type v-type ignore-key? raw-pred?]
  (assert (not (and (= v-type :ignore) ignore-key?))
          "Cannot ignore both key and value")
  (scan
    (let [^SpillableVector holder
          (sp/new-spillable-vector nil (:spill-opts (i/env-opts lmdb)))]
      (with-open [^AutoCloseable iter
                  (.iterator
                    ^Iterable (l/iterate-kv dbi rtx cur k-range k-type v-type))]
        (loop []
          (if (.hasNext ^Iterator iter)
            (let [kv             (.next ^Iterator iter)
                  ^ByteBuffer kb (l/k kv)
                  ^ByteBuffer vb (l/v kv)]
              (if raw-pred?
                (if (pred kv)
                  (let [v (when (not= v-type :ignore)
                            (b/read-buffer (.rewind vb) v-type))]
                    (.cons holder (if ignore-key?
                                    v
                                    [(b/read-buffer (.rewind kb) k-type) v]))
                    (recur))
                  (recur))
                (let [rk (b/read-buffer kb k-type)
                      rv (b/read-buffer vb v-type)]
                  (if (pred rk rv)
                    (let [v (when (not= v-type :ignore) rv)]
                      (.cons holder (if ignore-key? v [rk v]))
                      (recur))
                    (recur)))))
            holder))))
    (raise "Fail to range-filter: " e
           {:dbi    dbi-name :k-range k-range
            :k-type k-type   :v-type  v-type})))

(defn- range-keep*
  [^Iterable iterable ^SpillableVector holder pred k-type v-type raw-pred?]
  (with-open [^AutoCloseable iter (.iterator iterable)]
    (loop []
      (if (.hasNext ^Iterator iter)
        (let [kv (.next ^Iterator iter)]
          (if raw-pred?
            (do (when-let [res (pred kv)] (.cons holder res))
                (recur))
            (let [^ByteBuffer kb (l/k kv)
                  ^ByteBuffer vb (l/v kv)
                  rk             (b/read-buffer kb k-type)
                  rv             (b/read-buffer vb v-type)]
              (when-let [res (pred rk rv)] (.cons holder res))
              (recur))))
        holder))))

(defn range-keep
  [lmdb dbi-name pred k-range k-type v-type raw-pred?]
  (scan
    (let [holder (sp/new-spillable-vector nil (:spill-opts (i/env-opts lmdb)))

          iterable (l/iterate-kv dbi rtx cur k-range k-type v-type)]
      (range-keep* iterable holder pred k-type v-type raw-pred?))
    (raise "Fail to range-keep: " e
           {:dbi    dbi-name :k-range k-range
            :k-type k-type   :v-type  v-type})))

(defn- range-some*
  [iterable pred k-type v-type raw-pred?]
  (with-open [^AutoCloseable iter (.iterator ^Iterable iterable)]
    (loop []
      (when (.hasNext ^Iterator iter)
        (let [kv (.next ^Iterator iter)]
          (if raw-pred?
            (or (pred kv)
                (recur))
            (or (pred (b/read-buffer (l/k kv) k-type)
                      (b/read-buffer (l/v kv) v-type))
                (recur))))))))

(defn range-some
  [lmdb dbi-name pred k-range k-type v-type raw-pred?]
  (scan
    (let [iterable (l/iterate-kv dbi rtx cur k-range k-type v-type)]
      (range-some* iterable pred k-type v-type raw-pred?))
    (raise "Fail to find some in range: " e
           {:dbi dbi-name :key-range k-range})))

(defn- filter-count*
  [iterable pred raw-pred? k-type v-type]
  (with-open [^AutoCloseable iter (.iterator ^Iterable iterable)]
    (loop [c 0]
      (if (.hasNext ^Iterator iter)
        (let [kv (.next ^Iterator iter)]
          (if raw-pred?
            (if (pred kv)
              (recur (unchecked-inc c))
              (recur c))
            (if (pred (b/read-buffer (l/k kv) k-type)
                      (when v-type (b/read-buffer (l/v kv) v-type)))
              (recur (unchecked-inc c))
              (recur c))))
        c))))

(defn range-filter-count
  [lmdb dbi-name pred k-range k-type v-type raw-pred?]
  (scan
    (let [iterable (l/iterate-kv dbi rtx cur k-range k-type v-type)]
      (filter-count* iterable pred raw-pred? k-type v-type))
    (raise "Fail to range-filter-count: " e
           {:dbi dbi-name :k-range k-range :k-type k-type :v-type v-type})))

(defn- visit*
  [iterable visitor raw-pred? k-type v-type]
  (with-open [^AutoCloseable iter (.iterator ^Iterable iterable)]
    (loop []
      (when (.hasNext ^Iterator iter)
        (let [kv  (.next ^Iterator iter)
              res (if raw-pred?
                    (visitor kv)
                    (visitor (b/read-buffer (l/k kv) k-type)
                             (b/read-buffer (l/v kv) v-type)))]
          (when-not (identical? res :datalevin/terminate-visit)
            (recur)))))))

(defn visit
  [lmdb dbi-name visitor k-range k-type v-type raw-pred?]
  (scan
    (let [iterable (l/iterate-kv dbi rtx cur k-range k-type v-type)]
      (visit* iterable visitor raw-pred? k-type v-type))
    (raise "Fail to visit: " e
           {:dbi dbi-name :k-range k-range :k-type k-type :v-type v-type})))

(defn list-range
  [lmdb dbi-name k-range k-type v-range v-type]
  (scan
    (let [^SpillableVector holder
          (sp/new-spillable-vector nil (:spill-opts (i/env-opts lmdb)))]
      (with-open [^AutoCloseable iter
                  (.iterator
                    ^Iterable (l/iterate-list dbi rtx cur k-range k-type
                                              v-range v-type))]
        (loop []
          (if (.hasNext ^Iterator iter)
            (let [kv (.next ^Iterator iter)]
              (.cons holder [(b/read-buffer (l/k kv) k-type)
                             (b/read-buffer (l/v kv) v-type)])
              (recur))
            holder))))
    (raise "Fail to get list range: " e
           {:dbi dbi-name :key-range k-range :val-range v-range})) )

(defn list-range-first
  [lmdb dbi-name k-range k-type v-range v-type]
  (scan
    (with-open [^AutoCloseable iter
                (.iterator ^Iterable (l/iterate-list dbi rtx cur k-range k-type
                                                     v-range v-type))]
      (when (.hasNext ^Iterator iter)
        (let [kv (.next ^Iterator iter)]
          [(b/read-buffer (l/k kv) k-type)
           (b/read-buffer (l/v kv) v-type)])))
    (raise "Fail to get first of list range: " e
           {:dbi dbi-name :key-range k-range :val-range v-range})) )

(defn list-range-first-n
  [lmdb dbi-name n k-range k-type v-range v-type]
  (scan
    (let [^SpillableVector holder
          (sp/new-spillable-vector nil (:spill-opts (i/env-opts lmdb)))]
      (with-open [^AutoCloseable iter
                  (.iterator
                    ^Iterable (l/iterate-list dbi rtx cur k-range k-type
                                              v-range v-type))]
        (loop [i 0]
          (if (and (< i ^long n) (.hasNext ^Iterator iter))
            (let [kv (.next ^Iterator iter)]
              (.cons holder [(b/read-buffer (l/k kv) k-type)
                             (b/read-buffer (l/v kv) v-type)])
              (recur (inc i)))
            holder))))
    (raise "Fail to get first n of list range: " e
           {:dbi dbi-name :key-range k-range :val-range v-range})) )

(defn list-range-filter
  [lmdb dbi-name pred k-range k-type v-range v-type raw-pred?]
  (scan
    (let [^SpillableVector holder
          (sp/new-spillable-vector nil (:spill-opts (i/env-opts lmdb)))]
      (with-open [^AutoCloseable iter
                  (.iterator
                    ^Iterable (l/iterate-list dbi rtx cur k-range k-type
                                              v-range v-type))]
        (loop []
          (if (.hasNext ^Iterator iter)
            (let [kv             (.next ^Iterator iter)
                  ^ByteBuffer kb (l/k kv)
                  ^ByteBuffer vb (l/v kv)]
              (if raw-pred?
                (if (pred kv)
                  (do (.cons holder [(b/read-buffer (.rewind kb) k-type)
                                     (b/read-buffer (.rewind vb) v-type)])
                      (recur))
                  (recur))
                (let [rk (b/read-buffer kb k-type)
                      rv (b/read-buffer vb v-type)]
                  (if (pred rk rv)
                    (do (.cons holder [rk rv])
                        (recur))
                    (recur)))))
            holder))))
    (raise "Fail to filter list range: " e
           {:dbi dbi-name :key-range k-range :val-range v-range})) )

(defn list-range-keep
  [lmdb dbi-name pred k-range k-type v-range v-type raw-pred?]
  (scan
    (let [^SpillableVector holder
          (sp/new-spillable-vector nil (:spill-opts (i/env-opts lmdb)))
          iterable (l/iterate-list dbi rtx cur k-range k-type
                                   v-range v-type)]
      (range-keep* iterable holder pred k-type v-type raw-pred?))
    (raise "Fail to keep list range: " e
           {:dbi dbi-name :key-range k-range :val-range v-range})))

(defn- range-some*
  [iterable pred k-type v-type raw-pred?]
  (with-open [^AutoCloseable iter (.iterator ^Iterable iterable)]
    (loop []
      (when (.hasNext ^Iterator iter)
        (let [kv (.next ^Iterator iter)]
          (if raw-pred?
            (or (pred kv) (recur))
            (or (pred (b/read-buffer (l/k kv) k-type)
                      (b/read-buffer (l/v kv) v-type))
                (recur))))))))

(defn list-range-some
  [lmdb dbi-name pred k-range k-type v-range v-type raw-pred?]
  (scan
    (let [iterable (l/iterate-list dbi rtx cur k-range k-type
                                   v-range v-type)]
      (range-some* iterable pred k-type v-type raw-pred?))
    (raise "Fail to find some in list range: " e
           {:dbi dbi-name :key-range k-range :val-range v-range})))

(defn list-range-filter-count
  [lmdb dbi-name pred k-range k-type v-range v-type raw-pred?]
  (scan
    (let [iterable (l/iterate-list dbi rtx cur k-range k-type v-range v-type)]
      (filter-count* iterable pred raw-pred? k-type v-type))
    (raise "Fail to count filtered list range: " e
           {:dbi dbi-name :key-range k-range :val-range v-range})))

(defn visit-list-range
  [lmdb dbi-name visitor k-range k-type v-range v-type raw-pred?]
  (scan
    (let [iterable (l/iterate-list dbi rtx cur k-range k-type
                                   v-range v-type)]
      (visit* iterable visitor raw-pred? k-type v-type))
    (raise "Fail to visit list range: " e
           {:dbi dbi-name :key-range k-range :val-range v-range})))

(defn visit-list-key-range
  [lmdb dbi-name visitor k-range k-type v-type raw-pred?]
  (scan
    (let [iterable (l/iterate-list-key-range-val-full
                     dbi rtx cur k-range k-type)]
      (visit* iterable visitor raw-pred? k-type v-type))
    (raise "Fail to visit list key range: " e
           {:dbi dbi-name :key-range k-range})))

(defn visit-list-sample
  [lmdb dbi-name indices visitor k-range k-type v-type raw-pred?]
  (scan
    (let [iterable (l/iterate-list-sample dbi rtx cur indices k-range k-type)]
      (visit* iterable visitor raw-pred? k-type v-type))
    (raise "Fail to visit list sample: " e
           {:dbi dbi-name :key-range k-range})))

(defn visit-key-sample
  [lmdb dbi-name indices visitor k-range k-type raw-pred?]
  (scan
    (let [iterable (l/iterate-key-sample dbi rtx cur indices k-range k-type)]
      (visit* iterable visitor raw-pred? k-type nil))
    (raise "Fail to visit key sample: " e
           {:dbi dbi-name :key-range k-range})))

(defn operate-list-val-range
  [lmdb dbi-name operator v-range v-type]
  (scan
    (let [iterable (l/iterate-list-val dbi rtx cur v-range v-type)]
      (operator iterable))
    (raise "Fail to operate list val range: " e
           {:dbi dbi-name :val-range v-range})))

(defn visit-list*
  [iter visitor k kt vt raw-pred?]
  (loop [next? (l/seek-key iter k kt)]
    (when next?
      (let [vb (l/next-val iter)]
        (if raw-pred?
          (visitor vb)
          (visitor (b/read-buffer vb vt))))
      (recur (l/has-next-val iter)))))

(defn visit-list
  [lmdb dbi-name visitor k kt vt raw-pred?]
  (when k
    (scan
      (with-open [^AutoCloseable iter
                  (l/val-iterator (l/iterate-list-val-full dbi rtx cur))]
        (visit-list* iter visitor k kt vt raw-pred?))
      (raise "Fail to visit list: " e {:dbi dbi-name :k k}))))

(defn get-list*
  [lmdb iter k kt vt]
  (let [^SpillableVector holder
        (sp/new-spillable-vector nil (:spill-opts (i/env-opts lmdb)))]
    (loop [next? (l/seek-key iter k kt)]
      (when next?
        (.cons holder (b/read-buffer (l/next-val iter) vt))
        (recur (l/has-next-val iter))))
    holder))

(defn get-list
  [lmdb dbi-name k kt vt]
  (when k
    (scan
      (with-open [^AutoCloseable iter
                  (l/val-iterator (l/iterate-list-val-full dbi rtx cur))]
        (get-list* lmdb iter k kt vt))
      (raise "Fail to get a list: " e {:dbi dbi-name :key k}))))
