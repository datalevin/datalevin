;; Copyright (c) Huahai Yang. All rights reserved.
;; Distributed under the Eclipse Public License 2.0.
(ns ^:no-doc datalevin.db.result-cache
  "Keep mutable read results owned by the cache, separately from callers."
  (:require [datalevin.datom :as d]
            [datalevin.remote-cache :as remote-cache]
            [datalevin.util :as u])
  (:import [datalevin.datom Datom]
           [java.lang.reflect Array]
           [java.util Date]
           [org.eclipse.collections.impl.list.mutable FastList]))

(def ^:private unsupported (Object.))

(declare copy-value)

(defn- copy-items
  [xs depth]
  (reduce (fn [out x]
            (let [copy (copy-value x (inc (long depth)))]
              (if (identical? unsupported copy)
                (reduced unsupported)
                (conj out copy))))
          [] xs))

(defn- copy-container
  [x depth]
  (let [items (copy-items (if (map? x) (mapcat identity x) x) depth)]
    (cond
      (identical? unsupported items) unsupported
      (every? true? (map identical? (if (map? x) (mapcat identity x) x) items)) x
      (map? x) (into (empty x) (map vec) (partition 2 items))
      (vector? x) items
      (set? x) (into (empty x) items)
      (list? x) (apply list items)
      :else (into (empty x) items))))

(defn- copy-array
  [x depth]
  (let [component (.getComponentType ^Class (class x))
        n (Array/getLength x)]
    (if (.isPrimitive component)
      (let [copy (Array/newInstance component (int n))]
        (System/arraycopy x 0 copy 0 n)
        copy)
      (let [items (copy-items x depth)]
        (if (or (identical? unsupported items)
                (not-every? #(or (nil? %) (.isInstance component %)) items))
          unsupported
          (into-array component items))))))

(defn- copy-value
  [x ^long depth]
  (cond
    ;; Cyclic mutable containers and excessive nesting bypass retention.
    (> depth 128) unsupported
    (remote-cache/immutable-data? x)
    x
    :else
    (let [copy (cond
                 (instance? Datom x)
                 (let [^Datom x x
                       v (copy-value (.-v x) (inc depth))]
                   (cond
                     (identical? unsupported v) unsupported
                     (identical? v (.-v x)) x
                     :else (d/datom (.-e x) (.-a x) v (.-tx x))))

                 (bytes? x) (aclone ^bytes x)
                 (u/array? x) (copy-array x depth)
                 (= Date (class x)) (Date. (.getTime ^Date x))
                 (remote-cache/immutable-collection? x) (copy-container x depth)
                 (seq? x)
                 (let [items (copy-items x depth)]
                   (if (identical? unsupported items)
                     unsupported
                     (apply list items)))
                 (instance? FastList x)
                 (let [items (copy-items x depth)]
                   (if (identical? unsupported items)
                     unsupported
                     (FastList. ^java.util.Collection items)))
                 (symbol? x) x
                 :else unsupported)]
      (if (or (identical? unsupported copy) (nil? (meta x)))
        copy
        (let [metadata (copy-value (meta x) (inc depth))]
          (cond
            (identical? unsupported metadata) unsupported
            (identical? metadata (meta copy)) copy
            :else (with-meta copy metadata)))))))

(deftype Entry [value copy?])

(defn snapshot
  "Detach supported mutable values before publication. Unknown values bypass
  retention rather than imposing serialization requirements on local reads."
  [result]
  (let [copy (copy-value result 0)]
    (when-not (identical? unsupported copy)
      (Entry. copy (and (not (identical? copy result))
                       (not (remote-cache/immutable-data? copy)))))))

(defn restore
  "Return an owned result from a cache entry. Immutable results stay shared."
  [^Entry entry]
  (if (.-copy? entry)
    (copy-value (.-value entry) 0)
    (.-value entry)))
