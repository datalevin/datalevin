;;
;; Copyright (c) Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns ^:no-doc datalevin.query.cache
  "Query parsing and result cache."
  (:require
   [datalevin.constants :as c]
   [datalevin.db :as db]
   [datalevin.interface :refer [db-name dir]]
   [datalevin.lmdb :as l]
   [datalevin.parser :as dp]
   [datalevin.query.execute :as qexec]
   [datalevin.query.resolve :as qresolve])
  (:import
   [datalevin.db DB]
   [datalevin.parser Constant Function Pattern]
   [datalevin.storage Store]
   [datalevin.utl LRUCache]))

(def ^:dynamic *query-cache* (LRUCache. c/query-result-cache-size))

(def ^:dynamic *cache?* true)

(defn- run-query
  [parsed-q inputs]
  (qexec/q* parsed-q inputs))

(defn- cache-enabled? []
  (boolean *cache?*))

(defn parsed-q
  [q]
  (or (.get ^LRUCache *query-cache* q)
      (let [res (dp/parse-query q)]
        (.put ^LRUCache *query-cache* q res)
        res)))

(defn- resolve-qualified-fns
  "Convert qualified fns to fn-objects so that function implementation
  changes invalidate query result cache."
  [qualified-fns]
  (into #{} (map #(some-> % resolve deref)) qualified-fns))

(defn- query-uses-udf?
  [parsed-q]
  (boolean
    (some #(= 'udf (some-> ^Function % :fn :symbol))
          (dp/collect #(instance? Function %) (:qwhere parsed-q)))))

(defn- udf-cache-token
  [inputs]
  (mapv #(when (db/-searchable? %) (db/udf-cache-token %)) inputs))

(defn- query-cache-deps
  "Extract conservative dependencies for query-result cache invalidation.

  If dependency analysis is uncertain, return {:all? true} so the entry is
  invalidated on any transaction."
  [parsed-q]
  (letfn [(keyword-constant [term]
            (when (instance? Constant term)
              (let [v (:value ^Constant term)]
                (when (keyword? v) v))))
          (merge-deps [x y]
            (if (or (:all? x) (:all? y))
              {:all? true}
              {:all? false
               :attrs (into (:attrs x #{}) (:attrs y #{}))}))
          (pattern-deps [parsed-q]
            (let [patterns (dp/collect #(instance? Pattern %) (:qwhere parsed-q))]
              (loop [ps      patterns
                     attrs   (transient #{})
                     all?    false]
                (cond
                  all?
                  {:all? true}

                  (empty? ps)
                  {:all? false :attrs (persistent! attrs)}

                  :else
                  (let [^Pattern p (first ps)
                        attr-term  (nth (:pattern p) 1 nil)]
                    (cond
                      (instance? Constant attr-term)
                      (recur (rest ps) (conj! attrs (:value ^Constant attr-term))
                             false)

                      :else
                      (recur nil attrs true)))))))
          (tuple-fn-deps [parsed-q]
            (let [fns (dp/collect #(instance? Function %) (:qwhere parsed-q))]
              (reduce
                (fn [acc ^Function f]
                  (let [fname (some-> (:fn f) :symbol)
                        args  (:args f)]
                    (if (contains? qresolve/tuple-producing-fns fname)
                      (if-let [a (keyword-constant (nth args 1 nil))]
                        (merge-deps acc {:all? false :attrs #{a}})
                        {:all? true})
                      acc)))
                {:all? false :attrs #{}}
                fns)))]
    (let [find-elements (dp/find-elements (:qfind parsed-q))]
      (if (some dp/pull? find-elements)
        {:all? true}
        (merge-deps (pattern-deps parsed-q) (tuple-fn-deps parsed-q))))))

(defn- store-write-context-token
  [store]
  (if (instance? Store store)
    (let [lmdb      (.-lmdb ^Store store)
          tx-holder (l/write-txn lmdb)
          tx        @tx-holder
          writing?  (l/writing? lmdb)]
      (when tx
        [(if writing? :write :read)
         (System/identityHashCode tx-holder)
         (System/identityHashCode tx)]))
    (let [tx-holder (l/write-txn store)]
      (when (l/writing? store)
        [(System/identityHashCode tx-holder) 0]))))

(defn- cache-input-token
  [input]
  (if (db/-searchable? input)
    (let [store (.-store ^DB input)]
      [:db-input
       (db-name store)
       (dir store)
       (store-write-context-token store)])
    input))

(defn q-result
  [parsed-q inputs]
  (if (cache-enabled?)
    (if-let [store (some #(when (db/-searchable? %) (.-store ^DB %)) inputs)]
      (let [parsed-q' (-> (update parsed-q :qwhere-qualified-fns
                                  resolve-qualified-fns)
                          (dissoc :limit :offset))
            deps      (query-cache-deps parsed-q')
            udf-token (when (query-uses-udf? parsed-q')
                        (udf-cache-token inputs))
            k         [:query-result deps parsed-q' udf-token
                       (mapv cache-input-token inputs)]]
        (if-let [cached (db/cache-get store k)]
          cached
          (let [res (run-query parsed-q inputs)]
            (db/cache-put store k res)
            res)))
      (run-query parsed-q inputs))
    (run-query parsed-q inputs)))
