;;
;; Copyright (c) Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns ^:no-doc datalevin.storage.tuples
  "Schema-derived composite tuple datoms for prepared bulk loads."
  (:require [datalevin.constants :as c]
            [datalevin.datom :as d]
            [datalevin.interface :as i]
            [datalevin.storage.schema :as schema])
  (:import [datalevin.datom Datom]
           [java.util Objects]
           [org.eclipse.collections.impl.list.mutable FastList]))

(defn prepare-bulk-datoms
  "Reconcile affected composites against a batch's final source values.
  Store reads must use the same native write transaction as the eventual load.
  Explicit composite datoms from exports are derived again from their sources,
  so input order and batch boundaries cannot leave stale tuple values."
  [store datoms]
  (let [store-schema (i/schema store)
        rschema (i/rschema store)
        tuple-attrs (:db/tupleAttrs rschema)
        sources (schema/attr-tuples store-schema rschema)
        ^FastList result (FastList.)
        [values affected]
        (reduce
          (fn [[values affected] ^Datom datom]
            (let [e (.-e datom)
                  a (.-a datom)
                  v (.-v datom)
                  tuples (get sources a)
                  composite? (contains? tuple-attrs a)]
              (when-not composite? (.add result datom))
              [(if tuples
                 (let [key [e a]]
                   (if (d/datom-added datom)
                     (assoc values key v)
                     (let [current (if-some [entry (find values key)]
                                     (val entry)
                                     (i/ea-first-v store e a))]
                       (assoc values key
                              (when-not (Objects/deepEquals current v)
                                current)))))
                 values)
               (cond-> (reduce #(conj %1 [e %2]) affected (keys tuples))
                 composite? (conj [e a]))]))
          [{} #{}] datoms)]
    (doseq [[e a] affected]
      (let [value (mapv (fn [source]
                          (if-some [entry (find values [e source])]
                            (val entry)
                            (i/ea-first-v store e source)))
                        (:db/tupleAttrs (store-schema a)))
            value (when-not (every? nil? value) value)
            current (i/slice store :eav
                             (d/datom e a c/v0) (d/datom e a c/vmax))]
        (doseq [^Datom datom current
                :when (not= (.-v datom) value)]
          (.add result (d/delete datom)))
        (when (and value (not-any? #(= (:v %) value) current))
          (.add result (d/datom e a value)))))
    result))
