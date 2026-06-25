;;
;; Copyright (c) Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns ^:no-doc datalevin.datafy
  "Implement Clojure Datafiable protocol for entities"
  (:require [clojure.core.protocols :as cp]
            [clojure.set :as set]
            [datalevin.pull-api :as dp]
            [datalevin.db :as db]
            [datalevin.entity :as e]))

(declare datafy-entity-seq)

(defn- attr-types [db-val]
  (let [rschema          (db/-rschema db-val)
        ref-attrs        (set (rschema :db.type/ref))
        many-attrs       (set (rschema :db.cardinality/many))
        component-attrs  (set (rschema :db/isComponent))
        ref-many-rattrs  (into #{}
                                (map db/reverse-ref)
                                (set/difference ref-attrs component-attrs))
        component-rattrs (into #{} (map db/reverse-ref) component-attrs)]
    {:ref-attrs        ref-attrs
     :many-attrs       many-attrs
     :ref-many-rattrs  ref-many-rattrs
     :component-rattrs component-rattrs}))

(declare normalize-pulled-entity)

(defn- many-values [v]
  (if (and (coll? v) (not (map? v))) v [v]))

(defn- normalize-ref-value [types v]
  (if (map? v) (normalize-pulled-entity types v) v))

(defn- normalize-pulled-value
  [{:keys [ref-attrs many-attrs ref-many-rattrs component-rattrs] :as types} k v]
  (cond
    (nil? v) nil

    (or (and (many-attrs k) (ref-attrs k))
        (ref-many-rattrs k))
    (into #{} (map #(normalize-ref-value types %)) (many-values v))

    (many-attrs k)
    (set (many-values v))

    (or (ref-attrs k) (component-rattrs k))
    (normalize-ref-value types v)

    :else v))

(defn- normalize-pulled-entity [types pulled-entity]
  (persistent!
    (reduce-kv
      (fn [m k v]
        (assoc! m k (normalize-pulled-value types k v)))
      (transient {})
      pulled-entity)))

(defn- navize-pulled-entity [db-val pulled-entity]
  (let [{:keys [ref-attrs many-attrs ref-many-rattrs component-rattrs]
         :as   types} (attr-types db-val)
        pulled-entity (normalize-pulled-entity types pulled-entity)]
    (with-meta pulled-entity
      {`cp/nav (fn [_coll k v]
                 (cond
                   (or (and (many-attrs k) (ref-attrs k))
                       (ref-many-rattrs k))
                   (datafy-entity-seq db-val v)
                   (component-rattrs k)
                   (e/entity db-val (:db/id v))
                   (ref-attrs k)
                   (e/entity db-val (:db/id v))
                   :else v))})))

(defn- navize-pulled-entity-seq [db-val entities]
  (with-meta entities
    {`cp/nav (fn [_coll _k v]
               (e/entity db-val (:db/id v)))}))

(defn- datafy-entity-seq [db-val entities]
  (with-meta entities
    {`cp/datafy (fn [entities] (navize-pulled-entity-seq db-val entities))}))

(extend-protocol cp/Datafiable
  datalevin.entity.Entity
  (datafy [this]
    (let [db           (.-db this)
          ref-attrs    ((db/-rschema db) :db.type/ref )
          ref-rattrs   (set (map db/reverse-ref ref-attrs))
          pull-pattern (into ["*"] ref-rattrs)]
      (navize-pulled-entity db (dp/pull db pull-pattern (:db/id this))))))
