;;
;; Copyright (c) Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns ^:no-doc datalevin.storage.schema
  "Schema patches, tuple type inference, attribute IDs, and rename validation."
  (:refer-clojure :exclude [update assoc])
  (:require
   [datalevin.constants :as c]
   [datalevin.custom-datalog :as cd]
   [datalevin.datom :as d]
   [datalevin.inline :refer [update assoc]]
   [datalevin.interface :refer [transact-kv get-range populated?]]
   [datalevin.lmdb :as lmdb]
   [datalevin.util :as u :refer [conjs raise]]
   [datalevin.validate :as vld]))

(defn- patch-attr-schema
  [old-props property-patch]
  (reduce-kv
    (fn [props k v]
      (cond
        ;; Attribute IDs are allocated and owned by storage. Ignoring incoming
        ;; IDs makes the result of `schema` safe to reuse as schema input.
        (identical? k :db/aid) props

        ;; A schema property is removed only when explicitly retracted.
        (identical? v :db/retract) (dissoc props k)

        :else (assoc props k v)))
    (or old-props {})
    property-patch))

(defn- apply-schema-patch
  [old-schema schema-update]
  (reduce-kv
    (fn [updates attr property-patch]
      (assoc updates attr
             (patch-attr-schema (old-schema attr) property-patch)))
    {}
    (or schema-update {})))

(defn- infer-tuple-attr-types
  "Add typed tuple encoding to composite attributes whose schema only declares
  :db/tupleAttrs. Component types come from the source attributes. Attributes
  with untyped or non-scalar sources retain the legacy :data encoding until
  those source types are declared."
  [old-schema schema-update]
  (let [full-schema (merge old-schema schema-update)]
    (reduce-kv
      (fn [updates attr props]
        (if (and (sequential? (:db/tupleAttrs props))
                 (not (contains? props :db/tupleType))
                 (not (contains? props :db/tupleTypes)))
          (let [types (mapv #(get-in full-schema [% :db/valueType])
                            (:db/tupleAttrs props))]
            (if (and (< 1 (count types))
                     (every? c/tuple-value-types types))
              (assoc updates attr
                     (assoc props
                            :db/valueType :db.type/tuple
                            :db/tupleTypes types))
              updates))
          updates))
      (or schema-update {})
      full-schema)))

(defn prepare-schema-update
  [old-schema schema-update]
  (vld/validate-schema-update schema-update)
  (infer-tuple-attr-types
    old-schema (apply-schema-patch old-schema schema-update)))

(defn- attr->properties [k v]
  (case v
    :db.unique/identity  [:db/unique :db.unique/identity]
    :db.unique/value     [:db/unique :db.unique/value]
    :db.cardinality/many [:db.cardinality/many]
    (case k
      :db/tupleAttrs [:db.type/tuple :db/tupleAttrs]
      :db/tupleType  [:db.type/tuple :db/tupleType]
      :db/tupleTypes [:db.type/tuple :db/tupleTypes]
      (cond
        (and (identical? :db/valueType k)
             (identical? :db.type/ref v)) [:db.type/ref]
        (and (identical? :db/isComponent k)
             (true? v))                   [:db/isComponent]
        (and (identical? :db.attr/preds k)
             (some? v))                   [:db.attr/preds]
        :else                             []))))

(defn attr-tuples
  "e.g. :reg/semester => #{:reg/semester+course+student ...}"
  [schema rschema]
  (reduce
    (fn [m tuple-attr] ;; e.g. :reg/semester+course+student
      (u/reduce-indexed
        (fn [m src-attr idx] ;; e.g. :reg/semester
          (update m src-attr assoc tuple-attr idx))
        m ((schema tuple-attr) :db/tupleAttrs)))
    {} (rschema :db/tupleAttrs)))

(defn schema->rschema
  ":db/unique           => #{attr ...}
   :db.unique/identity  => #{attr ...}
   :db.unique/value     => #{attr ...}
   :db.cardinality/many => #{attr ...}
   :db.type/ref         => #{attr ...}
   :db/isComponent      => #{attr ...}
   :db.attr/preds       => #{attr ...}
   :db.type/tuple       => #{attr ...}
   :db/tupleAttr        => #{attr ...}
   :db/tupleType        => #{attr ...}
   :db/tupleTypes       => #{attr ...}
   :db/attrTuples       => {attr => {tuple-attr => idx}}"
  [schema]
  (let [rschema (reduce-kv
                  (fn [rschema attr attr-schema]
                    (reduce-kv
                      (fn [rschema key value]
                        (reduce
                          (fn [rschema prop]
                            (update rschema prop conjs attr))
                          rschema (attr->properties key value)))
                      rschema attr-schema))
                  {} schema)]
    (assoc rschema :db/attrTuples (attr-tuples schema rschema))))

(defn transact-schema
  [lmdb schema]
  (transact-kv
    lmdb
    (conj (for [[attr props] schema]
            (lmdb/kv-tx :put c/schema attr props :attr :data))
          (lmdb/kv-tx :put c/meta :last-modified
                      (System/currentTimeMillis) :attr :long))))

(defn load-schema
  [lmdb]
  (into {} (get-range lmdb c/schema [:all] :attr :data)))

(defn init-max-aid
  [schema]
  (inc ^long (apply max (map :db/aid (vals schema)))))

(defn update-schema
  [old schema]
  (let [^long init-aid (init-max-aid old)
        i              (volatile! 0)]
    (into {}
          (map (fn [[attr props]]
                 (if-let [old-props (old attr)]
                   [attr (assoc props :db/aid (old-props :db/aid))]
                   (let [res [attr (assoc props :db/aid (+ init-aid ^long @i))]]
                     (vswap! i u/long-inc)
                     res))))
          schema)))

(defn- effective-schema-update
  [old schema]
  (into {}
        (map (fn [[attr props]]
               [attr (if-let [old-props (old attr)]
                       (assoc props :db/aid (old-props :db/aid))
                       props)]))
        schema))

(defn schema-update-required?
  [old schema]
  (boolean
    (some (fn [[attr props]]
            (not= (old attr) props))
          (effective-schema-update old schema))))

(defn init-schema
  [lmdb schema]
  (let [now     (load-schema lmdb)
        missing (reduce-kv
                  (fn [acc attr props]
                    (if (contains? now attr) acc (assoc acc attr props)))
                  {} c/implicit-schema)]
    (cond
      (empty? now)
      (transact-schema lmdb c/implicit-schema)

      (seq missing)
      (transact-schema lmdb (update-schema now missing))))
  (when schema
    (let [old-schema   (load-schema lmdb)
          schema       (prepare-schema-update old-schema schema)
          full-schema  (merge old-schema schema)]
      (vld/validate-schema full-schema)
      (cd/validate-schema! lmdb full-schema)
      (cd/initialize! lmdb full-schema)
      (when (schema-update-required? old-schema schema)
        (transact-schema lmdb (update-schema old-schema schema)))))
  (cd/initialize! lmdb (load-schema lmdb)))

(defn init-attrs [schema]
  (into {} (map (fn [[k v]] [(v :db/aid) k])) schema))

(defn validate-schema-operations
  [schema-update del-attrs rename-map]
  (vld/validate-schema-update schema-update)
  (when-not (or (nil? del-attrs)
                (set? del-attrs)
                (sequential? del-attrs))
    (raise "Schema attributes to delete must be a set or sequence"
             {:error :schema/validation
              :value del-attrs}))
  (doseq [attr del-attrs]
    (when-not (keyword? attr)
      (raise "Schema attribute to delete must be a keyword"
               {:error     :schema/validation
                :attribute attr})))
  (when-not (or (nil? rename-map) (map? rename-map))
    (raise "Schema attribute renames must be a map"
             {:error :schema/validation
              :value rename-map}))
  (doseq [[old new] rename-map]
    (when-not (and (keyword? old) (keyword? new))
      (raise "Schema rename attributes must be keywords"
               {:error     :schema/validation
                :attribute old
                :target    new}))))

(defn normalize-schema-renames
  [rename-map]
  (let [renames (into {} (remove (fn [[old new]] (= old new))) rename-map)
        targets (vec (vals renames))]
    (when-not (= (count targets) (count (set targets)))
      (raise "Schema rename targets must be unique"
               {:error      :schema/rename-conflict
                :rename-map rename-map}))
    (let [sources (set (keys renames))
          overlap (set (filter sources targets))]
      (when (seq overlap)
        (raise "Schema rename chains and cycles are not supported"
                 {:error      :schema/rename-conflict
                  :attributes overlap
                  :rename-map rename-map})))
    renames))

(defn schema-rename-plans
  [current-schema schema-update renames]
  (reduce-kv
    (fn [plans old new]
      (let [old?       (contains? current-schema old)
            new?       (contains? current-schema new)
            patch-old? (contains? schema-update old)]
        (cond
          (and old? new?)
          (raise "Cannot rename attribute: target already exists"
                   {:error     :schema/rename-conflict
                    :attribute old
                    :target    new})

          old?
          (conj plans {:old old :new new :canonical old :pending? true})

          new?
          (conj plans {:old old :new new :canonical new :pending? false})

          patch-old?
          (conj plans {:old old :new new :canonical old :pending? true})

          :else
          (raise "Cannot rename missing attribute"
                   {:error     :schema/missing-attribute
                    :attribute old
                    :target    new}))))
    [] renames))

(defn resolve-renamed-schema-patches
  [schema-update rename-plans]
  (let [aliases
        (reduce
          (fn [m {:keys [old new canonical]}]
            (-> m (assoc old canonical) (assoc new canonical)))
          {} rename-plans)]
    (reduce-kv
      (fn [resolved attr property-patch]
        (let [canonical (get aliases attr attr)]
          (when (contains? resolved canonical)
            (raise "Schema patches resolve to the same renamed attribute"
                     {:error     :schema/rename-conflict
                      :attribute canonical}))
          (assoc resolved canonical property-patch)))
      {} schema-update)))

(defn populated-attr?
  [store attr]
  (populated? store :ave
              (d/datom c/e0 attr c/v0)
              (d/datom c/emax attr c/vmax)))
