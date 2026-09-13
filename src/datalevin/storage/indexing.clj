;;
;; Copyright (c) Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns ^:no-doc datalevin.storage.indexing
  "Batched fulltext, vector, embedding, and document index updates."
  (:require
   [datalevin.idoc :as idoc]
   [datalevin.interface :refer [add-doc remove-doc add-vec remove-vec]]
   [datalevin.search :as s]
   [datalevin.vector :as v])
  (:import
   [java.util HashMap IdentityHashMap]
   [org.eclipse.collections.impl.list.mutable FastList]))

(defn- op-ref
  "Extract the document reference from a fulltext or idoc index operation."
  [op]
  (let [kind (nth op 0)
        d    (nth op 1)]
    (case kind
      ;; Keep e and aid in giant refs so projected reads need not load the value.
      (:g :r) [:g (nth d 2) (nth d 0) (nth d 1)]
      (:a :d) d)))

(defn- apply-fulltext-op!
  [search-engines res]
  (let [op (peek res)
        d  (nth op 1)
        ref (op-ref op)]
    (doseq [domain (nth res 0)
            :let   [engine (search-engines domain)]]
      (case (nth op 0)
        (:a :g) (add-doc engine ref (peek d) false)
        (:d :r) (remove-doc engine ref)))))

(defn- fulltext-entry
  [ref text]
  (let [entry (object-array 2)]
    (aset entry 0 ref)
    (aset entry 1 text)
    entry))

(defn- fulltext-op-entry
  [kind ref text]
  (let [entry (object-array 3)]
    (aset entry 0 kind)
    (aset entry 1 ref)
    (aset entry 2 text)
    entry))

(def ^:private ^:const max-fulltext-batch-size 1024)

(defn- add-fulltext-batches!
  [engine ^FastList entries]
  (let [n (long (.size entries))]
    (loop [start (long 0)]
      (when (< start n)
        (let [end (long (min n (+ start max-fulltext-batch-size)))]
          (s/add-docs engine (.subList entries (int start) (int end)))
          (recur end))))))

(defn- transact-fulltext-batches!
  [engine ^FastList entries]
  (let [n (long (.size entries))]
    (loop [start (long 0)]
      (when (< start n)
        (let [end (long (min n (+ start max-fulltext-batch-size)))]
          (s/transact-docs engine (.subList entries (int start) (int end)))
          (recur end))))))

(defn fulltext-index
  [search-engines ft-ds]
  (let [^FastList ft-ds ft-ds
        n               (.size ft-ds)]
    (if (= n 1)
      (apply-fulltext-op! search-engines (.get ft-ds 0))
      (let [add-only? (loop [idx 0]
                        (if (< idx n)
                          (let [op   (peek (.get ft-ds idx))
                                kind (nth op 0)]
                            (if (or (identical? kind :a)
                                    (identical? kind :g))
                              (recur (unchecked-inc-int idx))
                              false))
                          true))]
        (if add-only?
          (let [batches (IdentityHashMap.)]
            (doseq [res    ft-ds
                    :let   [op   (peek res)
                            d    (nth op 1)]
                    domain (nth res 0)
                    :let   [engine (search-engines domain)
                            ^FastList entries
                            (or (.get batches engine)
                                (let [entries (FastList.)]
                                  (.put batches engine entries)
                                  entries))]]
              (.add entries
                    (fulltext-entry
                      (op-ref op)
                      (peek d))))
            (doseq [[engine entries] batches]
              (add-fulltext-batches! engine entries)))
          (let [batches (IdentityHashMap.)]
            (doseq [res    ft-ds
                    :let   [op   (peek res)
                            d    (nth op 1)
                            kind (nth op 0)]
                    domain (nth res 0)
                    :let   [engine (search-engines domain)
                            ^FastList entries
                            (or (.get batches engine)
                                (let [entries (FastList.)]
                                  (.put batches engine entries)
                                  entries))]]
              (.add entries
                    (case kind
                      :a (fulltext-op-entry :add d (peek d))
                      :d (fulltext-op-entry :delete d nil)
                      :g (fulltext-op-entry :add (op-ref op)
                                            (peek d))
                      :r (fulltext-op-entry :delete (op-ref op)
                                            nil))))
            (doseq [[engine entries] batches]
              (transact-fulltext-batches! engine entries))))))))

(defn- apply-vector-op!
  [vector-indices res]
  (let [op (peek res)
        d  (nth op 1)]
    (doseq [domain (nth res 0)
            :let   [index (vector-indices domain)]]
      (case (nth op 0)
        :a (add-vec index d (peek d))
        :d (remove-vec index d)
        :g (add-vec index [:g (nth d 2) (nth d 0) (nth d 1)] (peek d))
        :r (remove-vec index [:g (nth d 2) (nth d 0) (nth d 1)])))))

(defn- vector-entry
  [ref value]
  (let [entry (object-array 2)]
    (aset entry 0 ref)
    (aset entry 1 value)
    entry))

(defn vector-index
  [vector-indices vi-ds]
  (let [^FastList vi-ds vi-ds
        n               (.size vi-ds)]
    (if (= n 1)
      (apply-vector-op! vector-indices (.get vi-ds 0))
      (let [add-only? (loop [idx 0]
                        (if (< idx n)
                          (let [op   (peek (.get vi-ds idx))
                                kind (nth op 0)]
                            (if (or (identical? kind :a)
                                    (identical? kind :g))
                              (recur (unchecked-inc-int idx))
                              false))
                          true))]
        (if add-only?
          (let [batches (IdentityHashMap.)]
            (doseq [res    vi-ds
                    :let   [op   (peek res)
                            d    (nth op 1)
                            kind (nth op 0)]
                    domain (nth res 0)
                    :let   [index   (vector-indices domain)
                            ^FastList entries
                            (or (.get batches index)
                                (let [entries (FastList.)]
                                  (.put batches index entries)
                                  entries))]]
              (.add entries (vector-entry
                             (if (identical? kind :g)
                               [:g (nth d 2) (nth d 0) (nth d 1)]
                               d)
                             (peek d))))
            (doseq [[index entries] batches]
              (v/add-vecs index entries)))
          (doseq [res vi-ds]
            (apply-vector-op! vector-indices res)))))))

(defn embedding-index
  [embedding-indices em-ds]
  (doseq [res em-ds
          :let [[domain op] res
                index       (embedding-indices domain)]]
    (case (nth op 0)
      :a (let [[doc-ref vec-data] (nth op 1)]
           (add-vec index doc-ref vec-data))
      :d (remove-vec index (nth op 1)))))

(defn- plan-idoc-update!
  [index txs state-actions pending-paths pending-doc-ids old-op new-op]
  (let [old-d   (nth old-op 1)
        new-d   (nth new-op 1)
        old-ref (op-ref old-op)
        new-ref (op-ref new-op)
        old-doc (peek old-d)
        new-doc (peek new-d)
        patch   (some-> (meta new-op) :idoc/patch)
        res     (if patch
                  (idoc/patch-doc-plan!
                    index txs state-actions
                    pending-paths pending-doc-ids
                    old-ref old-doc new-ref new-doc patch)
                  (idoc/update-doc-plan!
                    index txs state-actions
                    pending-paths pending-doc-ids
                    old-ref old-doc new-ref new-doc))]
    (when (= res :doc-missing)
      (idoc/remove-doc-plan! index txs state-actions
                             pending-paths pending-doc-ids
                             old-ref old-doc)
      (idoc/add-doc-plan! index txs state-actions
                          pending-paths pending-doc-ids
                          new-ref new-doc false))))

(defn- fast-idoc-update!
  [idoc-indices ^FastList id-ds txs state-actions]
  (when (= 2 (.size id-ds))
    (let [res0    (.get id-ds 0)
          res1    (.get id-ds 1)
          domain0 (nth res0 0)
          domain1 (nth res1 0)
          op0     (peek res0)
          op1     (peek res1)
          kind0   (nth op0 0)
          kind1   (nth op1 0)
          [old-op new-op]
          (cond
            (and (or (identical? kind0 :d) (identical? kind0 :r))
                 (or (identical? kind1 :a) (identical? kind1 :g)))
            [op0 op1]

            (and (or (identical? kind1 :d) (identical? kind1 :r))
                 (or (identical? kind0 :a) (identical? kind0 :g)))
            [op1 op0])]
      (when (and old-op
                 (= domain0 domain1)
                 (let [old-d (nth old-op 1)
                       new-d (nth new-op 1)]
                   (and (= (nth old-d 0) (nth new-d 0))
                        (= (nth old-d 1) (nth new-d 1)))))
        (let [index (idoc-indices domain0)]
          (plan-idoc-update! index txs state-actions
                              (HashMap.) (HashMap.) old-op new-op))
        true))))

(defn idoc-index
  [idoc-indices id-ds txs]
  (let [state-actions (FastList.)]
    (if (fast-idoc-update! idoc-indices id-ds txs state-actions)
      state-actions
      (let [updates (volatile! {})
            path-plans (IdentityHashMap.)
            doc-plans  (IdentityHashMap.)
            path-plan  (fn [index]
                         (or (.get path-plans index)
                             (let [m (HashMap.)]
                               (.put path-plans index m)
                               m)))
            doc-plan   (fn [index]
                         (or (.get doc-plans index)
                             (let [m (HashMap.)]
                               (.put doc-plans index m)
                               m)))]
        (doseq [res  id-ds
                :let [op     (peek res)
                      d      (nth op 1)
                      domain (nth res 0)
                      kind   (nth op 0)]]
          (case kind
            (:a :g)
            (let [k [(nth d 0) (nth d 1)]]
              (vswap! updates update-in [domain k :a] (fnil conj []) op))
            (:d :r)
            (let [k [(nth d 0) (nth d 1)]]
              (vswap! updates update-in [domain k :d] (fnil conj []) op))))
        (doseq [[domain domain-ops] @updates
                :let         [index (idoc-indices domain)
                              pending-paths (path-plan index)
                              pending-doc-ids (doc-plan index)]]
          (doseq [[_ {:keys [a d]}] domain-ops
                  :let              [na (count a)
                                     nd (count d)]]
            (cond
              (and (= 1 na) (= 1 nd))
              (plan-idoc-update! index txs state-actions
                                  pending-paths pending-doc-ids
                                  (first d) (first a))

              (and (= 1 na) (zero? nd))
              (let [op  (first a)
                    od  (nth op 1)]
                (idoc/add-doc-plan! index txs state-actions
                                    pending-paths pending-doc-ids
                                    (op-ref op) (peek od) false))

              (and (zero? na) (= 1 nd))
              (let [op  (first d)
                    od  (nth op 1)]
                (idoc/remove-doc-plan! index txs state-actions
                                       pending-paths pending-doc-ids
                                       (op-ref op) (peek od)))

              :else
              (let [adds (mapv (fn [op]
                                 (let [d (nth op 1)]
                                   [(op-ref op) (peek d)]))
                               a)
                    rems (mapv (fn [op]
                                 (let [d (nth op 1)]
                                   [(op-ref op) (peek d)]))
                               d)]
                (idoc/add-docs-plan! index txs state-actions
                                     pending-paths pending-doc-ids adds false)
                (idoc/remove-docs-plan! index txs state-actions
                                        pending-paths pending-doc-ids rems)))))
        state-actions))))

(defn remove-fulltext-doc-idempotently!
  [engine ref]
  (try
    (remove-doc engine ref)
    (catch clojure.lang.ExceptionInfo e
      (when-not (= "Document does not exist." (ex-message e))
        (throw e)))))
