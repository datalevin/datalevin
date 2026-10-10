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
   [datalevin.constants :refer [e0 tx0 emax txmax v0 vmax]]
   [datalevin.datom :as d :refer [datom]]
   [datalevin.interface :as i :refer [rschema]]
   [datalevin.validate :as vld])
  (:import
   [datalevin.datom Datom]
   [java.util HashMap SortedSet]
   [org.eclipse.collections.impl.map.mutable.primitive LongObjectHashMap]
   [org.eclipse.collections.impl.set.sorted.mutable TreeSortedSet]))

(def ^:dynamic *batch-prepare* nil)

(defprotocol BatchPreparation
  (flush-preparation! [context])
  (restore-preparation! [context db]))

(defprotocol ScalarDatomBuffer
  "Thread-confined batch scratch space. A failed lease uses request-local storage;
  callers must clear the array and release it without publishing it."
  (acquire-scalar-datom-buffer! [context capacity])
  (release-scalar-datom-buffer! [context buffer]))

(defprotocol ScalarPendingIndex
  (pending-scalar-index [context])
  (pending-unique-index [context])
  (retain-pending-index? [context])
  (prepare-pending-index! [context db])
  (materialize-scalar-pending! [context db])
  (discard-scalar-pending! [context]))

(defn scalar-pending-index
  "Return the owner's E/A index while preparation contains only scalar writes."
  []
  (let [context *batch-prepare*]
    (when (instance? datalevin.db.tx.common.ScalarPendingIndex context)
      (pending-scalar-index context))))

(declare ref?)

(defn flush-batch-prepare!
  "Make previously frozen writes visible to ordinary native reads in user code."
  []
  (when-let [context *batch-prepare*]
    (if (satisfies? BatchPreparation context)
      (flush-preparation! context)
      (when-let [flush! (:flush! context)] (flush!)))))

(defn stage-batch-datom!
  "Update the resolver's existing indexes with the latest pending E/A/V state."
  [db ^Datom datom]
  (if-let [^LongObjectHashMap pending (scalar-pending-index)]
    ;; Scalar preparation has cardinality-one E/A keys. The latest datom,
    ;; including a retraction, replaces its entire pending attribute state.
    (let [e (.-e datom)
          ^HashMap attrs (or (.get pending e)
                             (let [attrs (HashMap. 4)]
                               (.put pending e attrs)
                               attrs))]
      (.put attrs (.-a datom) datom)
      (when (:db/unique ((i/schema (:store db)) (.-a datom)))
        (.put ^HashMap (pending-unique-index *batch-prepare*)
              [(.-a datom) (.-v datom)] datom)))
    (let [^TreeSortedSet eavt (:eavt db)
          ^TreeSortedSet avet (:avet db)
          ^SortedSet cached (.subSet eavt
                                    (d/datom (.-e datom) (.-a datom) (.-v datom) tx0)
                                    (d/datom (.-e datom) (.-a datom) (.-v datom) txmax))]
      (while (not (.isEmpty cached))
        (let [old (.first cached)] (.remove eavt old) (.remove avet old)))
      ;; Keep the retraction bit: it masks an older native value until the
      ;; frozen writes have been applied. One latest datom per E/A/V is enough.
      (.add eavt datom)
      (when-not (contains? (:db/noindex (rschema (:store db))) (.-a datom))
        (.add avet datom)))))

(defn- first-added-datom
  [^SortedSet datoms]
  (let [iterator (.iterator datoms)]
    (loop []
      (when (.hasNext iterator)
        (let [^Datom datom (.next iterator)]
          (if (d/datom-added datom) datom (recur)))))))

(defn visible-stored
  "Let the resolver overlay's latest datom replace an older native value."
  [db datoms]
  (if *batch-prepare*
    (remove (fn [^Datom d]
              (not (.isEmpty (.subSet ^TreeSortedSet (:eavt db)
                                     (datom (.-e d) (.-a d) (.-v d) tx0)
                                     (datom (.-e d) (.-a d) (.-v d) txmax))))) datoms)
    datoms))

(defn cached-av-first-e
  "Resolve an identity in the transaction index, skipping retracted values."
  [db a v]
  (let [^SortedSet cached (.subSet ^TreeSortedSet (:avet db)
                                  (datom e0 a v tx0) (datom emax a v txmax))]
    (if *batch-prepare*
      (:e (first-added-datom cached))
      (when-not (.isEmpty cached) (:e (.first cached))))))

(defn ea-first-datom
  "Resolve the first current entity/attribute datom during write preparation."
  [db e a]
  (if *batch-prepare*
    (let [^SortedSet cached (.subSet ^TreeSortedSet (:eavt db)
                                    (datom e a nil tx0) (datom e a nil txmax))]
      (if (.isEmpty cached)
        (i/ea-first-datom (:store db) e a)
        (or (first-added-datom cached)
            (first (visible-stored
                     db (i/slice (:store db) :eav
                                 (datom e a v0) (datom e a vmax)))))))
    (i/ea-first-datom (:store db) e a)))

(defn ea-first-v
  "Resolve the current entity/attribute value during write preparation."
  [db e a]
  (if-let [^LongObjectHashMap pending (scalar-pending-index)]
    (if-let [^Datom datom (some-> ^HashMap (.get pending (long e)) (.get a))]
      (when (d/datom-added datom) (.-v datom))
      (i/ea-first-v (:store db) e a))
    (if *batch-prepare*
      (let [^SortedSet cached (.subSet ^TreeSortedSet (:eavt db)
                                      (datom e a nil tx0) (datom e a nil txmax))]
        (if (.isEmpty cached)
          (i/ea-first-v (:store db) e a)
          (if-let [^Datom pending (first-added-datom cached)]
            (.-v pending)
            (:v (first (visible-stored
                         db (i/slice (:store db) :eav
                                     (datom e a v0) (datom e a vmax))))))))
      (i/ea-first-v (:store db) e a))))

(defn av-first-e
  "Resolve the current owner of an indexed value during write preparation."
  [db a v]
  (if-let [^HashMap pending (when (scalar-pending-index)
                             (pending-unique-index *batch-prepare*))]
    (if-let [^Datom datom (.get pending [a v])]
      (when (d/datom-added datom) (.-e datom))
      (i/av-first-e (:store db) a v))
    (if *batch-prepare*
      (or (cached-av-first-e db a v)
          (:e (first (visible-stored db (i/av-datoms (:store db) a v)))))
      (i/av-first-e (:store db) a v))))

(defn ea-datoms
  "Resolve current attribute datoms for CAS and retraction preparation."
  [db e a]
  (let [stored (i/slice (:store db) :eav (datom e a v0) (datom e a vmax))]
    (if *batch-prepare*
      (vec (concat (filter d/datom-added
                           (.subSet ^TreeSortedSet (:eavt db)
                                    (datom e a nil tx0) (datom e a nil txmax)))
                   (visible-stored db stored)))
      stored)))

(defn e-datoms
  "Resolve current entity datoms for retraction preparation."
  [db e]
  (let [stored (i/e-datoms (:store db) e)]
    (if *batch-prepare*
      (vec (concat (filter d/datom-added
                           (.subSet ^TreeSortedSet (:eavt db)
                                    (datom e nil nil tx0) (datom e nil nil txmax)))
                   (visible-stored db stored)))
      stored)))

(defn v-datoms
  "Resolve current incoming references for entity retraction preparation."
  [db e]
  (let [stored (i/v-datoms (:store db) e)]
    (if *batch-prepare*
      (vec (concat (filter #(and (d/datom-added %)
                                (ref? db (:a %)) (= e (:v %))) (:eavt db))
                   (visible-stored db stored)))
      stored)))

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

(deftype LookupRefCache [store owner ^HashMap entries])

(def ^:dynamic *lookup-ref-cache* nil)

(defmacro with-lookup-ref-cache
  "Reuse persisted identity reads within one transaction attempt. Always check
  the changing AVET overlay first; retries and nested transactions get a fresh
  cache, and conveyed bindings must not share it with another thread."
  [db & body]
  `(binding [*lookup-ref-cache*
             (LookupRefCache. (:store ~db) (Thread/currentThread) (HashMap.))]
     ~@body))

(defn- immutable-lookup-value?
  [value]
  (or (string? value) (keyword? value) (symbol? value) (number? value)
      (boolean? value) (uuid? value)
      (and (vector? value) (every? immutable-lookup-value? value))))

(defn- stored-entid
  [db attr value]
  (let [store (:store db)
        ^LookupRefCache cache *lookup-ref-cache*]
    (if (and (nil? *batch-prepare*) cache (identical? store (.-store cache))
             (identical? (Thread/currentThread) (.-owner cache))
             ;; Dates, byte arrays and custom values may change in user code.
             (immutable-lookup-value? value))
      (let [^HashMap entries (.-entries cache)
            key [attr value]
            cached (.getOrDefault entries key ::uncached)]
        (if-not (identical? cached ::uncached)
          cached
          (let [eid (av-first-e db attr value)]
            (.put entries key eid)
            eid)))
      (av-first-e db attr value))))

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
        (or (cached-av-first-e db attr value)
            (stored-entid db attr value))))

    (keyword? eid)
    (or (cached-av-first-e db :db/ident eid)
        (stored-entid db :db/ident eid))

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
