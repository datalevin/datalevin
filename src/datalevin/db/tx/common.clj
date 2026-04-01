;;
;; Copyright (c) Nikita Prokopov, Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns ^:no-doc datalevin.db.tx.common
  "Shared transaction helpers."
  (:require
   [datalevin.constants :refer [e0 tx0 emax txmax]]
   [datalevin.datom :refer [datom]]
   [datalevin.interface :refer [av-first-e rschema]]
   [datalevin.validate :as vld])
  (:import
   [java.util SortedSet]
   [org.eclipse.collections.impl.set.sorted.mutable TreeSortedSet]))

(defn- sf [^SortedSet s] (when-not (.isEmpty s) (.first s)))

(defn attrs-by
  [db property]
  ((rschema (:store db)) property))

(defn is-attr?
  ^Boolean [db attr property]
  (contains? (attrs-by db property) attr))

(defn multival?
  ^Boolean [db attr]
  (is-attr? db attr :db.cardinality/many))

(defn multi-value?
  ^Boolean [db attr value]
  (and
    (is-attr? db attr :db.cardinality/many)
    (or
      (and (some? value) (.isArray (class value)))
      (and (coll? value) (not (map? value))))))

(defn ref?
  ^Boolean [db attr]
  (is-attr? db attr :db.type/ref))

(defn component?
  ^Boolean [db attr]
  (is-attr? db attr :db/isComponent))

(defn tuple-attr?
  ^Boolean [db attr]
  (is-attr? db attr :db/tupleAttrs))

(defn tuple-type?
  ^Boolean [db attr]
  (is-attr? db attr :db/tupleType))

(defn tuple-types?
  ^Boolean [db attr]
  (is-attr? db attr :db/tupleTypes))

(defn tuple-source?
  ^Boolean [db attr]
  (is-attr? db attr :db/attrTuples))

(declare entid-strict)

(defn entid
  [db eid]
  (cond
    (and (integer? eid) (not (neg? ^long eid)))
    eid

    (sequential? eid)
    (let [[attr value] eid]
      (cond
        (not= (count eid) 2)
        (vld/validate-lookup-ref-shape eid)

        (not (is-attr? db attr :db/unique))
        (vld/validate-lookup-ref-unique false eid)

        (nil? value)
        nil

        :else
        (or (:e (sf (.subSet ^TreeSortedSet (:avet db)
                             (datom e0 attr value tx0)
                             (datom emax attr value txmax))))
            (av-first-e (:store db) attr value))))

    (keyword? eid)
    (or (:e (sf (.subSet ^TreeSortedSet (:avet db)
                         (datom e0 :db/ident eid tx0)
                         (datom emax :db/ident eid txmax))))
        (av-first-e (:store db) :db/ident eid))

    :else
    (vld/validate-entity-id-syntax eid)))

(defn entid-strict
  [db eid]
  (let [result (entid db eid)]
    (vld/validate-entity-id-exists result eid)
    result))

(defn entid-some
  [db eid]
  (when eid
    (entid-strict db eid)))

(defn reverse-ref?
  ^Boolean [attr]
  (if (keyword? attr)
    (= \_ (nth (name attr) 0))
    (do (vld/validate-reverse-ref-attr attr)
        false)))

(defn reverse-ref
  [attr]
  (if (reverse-ref? attr)
    (keyword (namespace attr) (subs (name attr) 1))
    (keyword (namespace attr) (str "_" (name attr)))))

(defn udf-registry
  [x]
  (get (:runtime-opts (meta x)) :udf-registry))
