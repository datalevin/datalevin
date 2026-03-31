;;
;; Copyright (c) Nikita Prokopov, Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns ^:no-doc datalevin.db.tx.prepare
  "Transaction preparation."
  (:require
   [datalevin.constants :refer [e0 tx0 emax txmax]]
   [datalevin.interface :refer [av-first-e ea-first-v opts]]
   [datalevin.prepare :as coreprep]
   [datalevin.datom :as d :refer [datom?]]
   [datalevin.udf :as udf]
   [datalevin.util :as u :refer [cond+]]
   [datalevin.validate :as vld])
  (:import
   [java.io Writer]
   [java.util SortedSet]
   [org.eclipse.collections.impl.set.sorted.mutable TreeSortedSet]))

(defn- sf [^SortedSet s] (when-not (.isEmpty s) (.first s)))

(def *last-auto-tempid
  (volatile! 0))

(deftype AutoTempid [id]
  Object
  (toString [d] (str "#datalevin/AutoTempid [" id "]")))

(defmethod print-method AutoTempid [^AutoTempid id, ^Writer w]
  (.write w (str "#datalevin/AutoTempid "))
  (binding [*out* w]
    (pr [(.-id id)])))

(defn auto-tempid [] (AutoTempid. (vswap! *last-auto-tempid u/long-inc)))

(defn auto-tempid? ^Boolean [x] (instance? AutoTempid x))

(defn tempid?
  ^Boolean [x]
  (or (and (number? x) (neg? ^long x))
      (string? x)
      (auto-tempid? x)))

(declare assoc-auto-tempid)

(defn assoc-auto-tempids
  [deps db entities]
  (mapv #(assoc-auto-tempid deps db %) entities))

(defn assoc-auto-tempid
  [deps db entity]
  (cond+
    (map? entity)
    (persistent!
      (reduce-kv
        (fn [entity a v]
          (cond
            (and ((:ref?-fn deps) db a)
                 ((:multi-value?-fn deps) db a v))
            (assoc! entity a (assoc-auto-tempids deps db v))

            ((:ref?-fn deps) db a)
            (assoc! entity a (assoc-auto-tempid deps db v))

            (and ((:reverse-ref?-fn deps) a) (sequential? v))
            (assoc! entity a (assoc-auto-tempids deps db v))

            ((:reverse-ref?-fn deps) a)
            (assoc! entity a (assoc-auto-tempid deps db v))

            :else
            (assoc! entity a v)))
        (transient {})
        (if (contains? entity :db/id)
          entity
          (assoc entity :db/id (auto-tempid)))))

    (not (sequential? entity))
    entity

    :let [[op e a v] entity]

    (and (= :db/add op) ((:ref?-fn deps) db a))
    (if ((:multi-value?-fn deps) db a v)
      [op e a (assoc-auto-tempids deps db v)]
      [op e a (assoc-auto-tempid deps db v)])

    :else
    entity))

(defn resolve-upserts
  "Returns [entity' upserts]. Upsert attributes that resolve to existing entities
   are removed from entity, rest are kept in entity for insertion. No validation
   is performed."
  [deps db entity]
  (if-some [idents (not-empty ((:attrs-by-fn deps) db :db.unique/identity))]
    (let [store   (:store db)
          resolve (fn [a v]
                    (cond
                      (not ((:ref?-fn deps) db a))
                      (or (:e (sf (.subSet ^TreeSortedSet (:avet db)
                                           (d/datom e0 a v tx0)
                                           (d/datom emax a v txmax))))
                          (av-first-e store a v))

                      (not (tempid? v))
                      (let [rv ((:entid-fn deps) db v)]
                        (or (:e (sf (.subSet ^TreeSortedSet (:avet db)
                                             (d/datom e0 a rv tx0)
                                             (d/datom emax a rv txmax))))
                            (av-first-e store a rv)))))
          split   (fn [a vs]
                    (reduce
                      (fn [acc v]
                        (if-some [e (resolve a v)]
                          (update acc 1 assoc v e)
                          (update acc 0 conj v)))
                      [[] {}] vs))]
      (reduce-kv
        (fn [[entity' upserts] a v]
          (vld/validate-attr a entity)
          (vld/validate-val v entity)
          (cond
            (not (contains? idents a))
            [(assoc entity' a v) upserts]

            ((:multi-value?-fn deps) db a v)
            (let [[insert upsert] (split a v)]
              [(cond-> entity'
                 (seq insert) (assoc a insert))
               (cond-> upserts
                 (seq upsert) (assoc a upsert))])

            :else
            (let [v' (if ((:ref?-fn deps) db a)
                       v
                       (coreprep/correct-value store a v))]
              (if-some [e (resolve a v')]
                [entity' (assoc upserts a {v e})]
                [(assoc entity' a v) upserts]))))
        [{} {}]
        entity))
    [entity nil]))

(defn validate-upserts
  [entity upserts]
  (vld/validate-upserts entity upserts tempid?))

(def builtin-fn?
  #{:db.fn/call
    :db.fn/cas
    :db/cas
    :db.fn/patchIdoc
    :db/add
    :db/retract
    :db.fn/retractAttribute
    :db.fn/retractEntity
    :db/retractEntity})

(defn- lookup-installed-callable
  [deps db target]
  (when-not (udf/descriptor? target)
    (when-some [eid ((:entid-fn deps) db target)]
      (let [store    (:store db)
            fun      (ea-first-v store eid :db/fn)
            udf-desc (ea-first-v store eid :db/udf)
            ident    (ea-first-v store eid :db/ident)]
        (when (and fun udf-desc)
          (u/raise "Installed callable entity cannot have both :db/fn and :db/udf: "
                   target
                   {:error     :transact/syntax
                    :target    target
                    :entity-id eid}))
        {:eid eid :ident ident :fn fun :udf udf-desc}))))

(defn- lookup-tx-fn-value
  [_db store ident]
  (ea-first-v store ident :db/fn))

(defn- tx-udf-context
  [db]
  {:db        db
   :kind      :tx-fn
   :embedded? true
   :store     (:store db)})

(defn installed-udf-descriptor
  ([deps db target]
   (installed-udf-descriptor deps db nil target))
  ([deps db allowed target]
   (when-some [{:keys [ident udf]} (lookup-installed-callable deps db target)]
     (when udf
       (let [descriptor (udf/descriptor udf)]
         (vld/validate-installed-udf-ident ident descriptor
                                           [:db/udf target])
         (if allowed
           (udf/ensure-kind descriptor allowed)
           descriptor))))))

(defn- wrap-tx-udf
  [deps db descriptor]
  (let [registry   ((:udf-registry-fn deps) db)
        descriptor (udf/ensure-kind descriptor :tx-fn)]
    (fn [db* & args]
      (let [callable (udf/materialize registry (tx-udf-context db*)
                                      descriptor)]
        (apply callable db* args)))))

(defn- resolve-installed-tx-callable
  [deps db target entity]
  (when-some [{:keys [udf] :as installed}
              (lookup-installed-callable deps db target)]
    (let [fun (:fn installed)]
      (cond
        fun
        (do
          (vld/validate-custom-tx-fn-value fun target entity)
          fun)

        udf
        (wrap-tx-udf deps db
                     (installed-udf-descriptor deps db :tx-fn target))

        :else
        nil))))

(defn- resolve-tx-callable
  [deps db target]
  (cond
    (fn? target)
    target

    (udf/descriptor? target)
    (wrap-tx-udf deps db target)

    :else
    (or (resolve-installed-tx-callable deps db target [:db.fn/call target])
        (if ((:entid-fn deps) db target)
          (do
            (vld/validate-custom-tx-fn-value nil target [:db.fn/call target])
            nil)
          (wrap-tx-udf deps db
                       (udf/descriptor-or-registered
                         ((:udf-registry-fn deps) db) :tx-fn target))))))

(defn handle-fn-call
  [deps db entity]
  (let [[_ target & args] entity
        f                 (resolve-tx-callable deps db target)]
    (apply f db args)))

(defn handle-custom-tx-fn
  [deps db store entity entities]
  (let [op    (first entity)
        ident (or (:e (sf (.subSet
                            ^TreeSortedSet (:avet db)
                            (d/datom e0 op nil tx0)
                            (d/datom emax op nil txmax))))
                  ((:entid-fn deps) db op))]
    (vld/validate-custom-tx-fn-entity ident op entity)
    (let [fun  (or (resolve-installed-tx-callable deps db op entity)
                   (lookup-tx-fn-value db store ident))
          args (next entity)]
      (vld/validate-custom-tx-fn-value fun op entity)
      (concat (apply fun db args) entities))))

(defn maybe-wrap-multival
  [deps db a vs]
  (cond
    (not (or ((:reverse-ref?-fn deps) a)
             ((:multival?-fn deps) db a)))
    [vs]

    (not (and (coll? vs) (not (map? vs))))
    [vs]

    (and (= (count vs) 2)
         ((:is-attr?-fn deps) db (first vs) :db.unique/identity))
    [vs]

    :else
    vs))

(defn explode
  [deps db entity]
  (let [eid  (:db/id entity)
        a+vs (into []
                   cat
                   (reduce
                     (fn [acc [a vs]]
                       (update acc
                               (if ((:tuple-attr?-fn deps) db a) 1 0)
                               conj
                               [a vs]))
                     [[] []]
                     entity))]
    (for [[a vs] a+vs
          :when  (not (identical? a :db/id))
          :let   [reverse?   ((:reverse-ref?-fn deps) a)
                  straight-a (if reverse?
                               ((:reverse-ref-fn deps) a)
                               a)
                  _          (when reverse?
                               (vld/validate-reverse-ref-type
                                 ((:ref?-fn deps) db straight-a)
                                 a
                                 eid
                                 vs))]
          v      (maybe-wrap-multival deps db a vs)]
      (if (and ((:ref?-fn deps) db straight-a) (map? v))
        (assoc v ((:reverse-ref-fn deps) a) eid)
        (if reverse?
          [:db/add v straight-a eid]
          [:db/add eid straight-a v])))))

(defn expand-transactable-entity
  [deps entity]
  (if ((:de-entity?-fn deps) entity)
    ((:de-entity->txs-fn deps) entity)
    [entity]))

(defn update-entity-time
  [entity tx-time]
  (cond
    (map? entity)
    [(assoc entity :db/updated-at tx-time)]

    (sequential? entity)
    (let [[op e _ _] entity]
      (if (or (identical? op :db/retractEntity)
              (identical? op :db.fn/retractEntity))
        [entity]
        [entity [:db/add e :db/updated-at tx-time]]))

    (datom? entity)
    (let [e (d/datom-e entity)]
      [entity [:db/add e :db/updated-at tx-time]])

    (nil? entity)
    []

    :else
    (vld/validate-tx-entity-type entity)))

(defn prepare-entities
  [deps db entities tx-time]
  (let [aat #(assoc-auto-tempid deps db %)
        uet #(update-entity-time % tx-time)]
    (sequence
      (if (:auto-entity-time? (opts (:store db)))
        (comp (mapcat #(expand-transactable-entity deps %))
              (map aat)
              (mapcat uet))
        (comp (mapcat #(expand-transactable-entity deps %))
              (map aat)))
      entities)))
