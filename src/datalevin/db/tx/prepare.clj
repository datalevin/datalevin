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
   [datalevin.db.tx.common :as txcommon]
   [datalevin.interface :refer [ea-first-v]]
   [datalevin.prepare :as coreprep]
   [datalevin.datom :as d :refer [datom?]]
   [datalevin.udf :as udf]
   [datalevin.tx-group.failure :as failure]
   [datalevin.util :as u :refer [ cond+ raise]]
   [datalevin.validate :as vld])
  (:import
   [java.io Writer]
   [java.util ArrayList SortedSet]
   [org.eclipse.collections.impl.set.sorted.mutable TreeSortedSet]))

(defn- sf [^SortedSet s] (some #(when (d/datom-added %) %) s))

(def *last-auto-tempid
  (volatile! 0))

(declare resolve-attr-pred-udf)

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
  ([db entities]
   (assoc-auto-tempids db entities (coreprep/make-prepare-ctx db)))
  ([db entities ctx]
   (mapv #(assoc-auto-tempid db % ctx) entities)))

(defn assoc-auto-tempid
  ([db entity]
   (assoc-auto-tempid db entity (coreprep/make-prepare-ctx db)))
  ([db entity ctx]
   (let [refs (:db.type/ref (:rschema ctx))
         many (:db.cardinality/many (:rschema ctx))]
     (cond+
       (map? entity)
       (let [entity (if (contains? entity :db/id)
                      entity
                      (assoc entity :db/id (auto-tempid)))]
         ;; Scalar fields already have their final shape. Only rebuild entries
         ;; whose reference values can contain entities needing an implicit ID.
         (reduce-kv
           (fn [entity a v]
             (cond
               (contains? refs a)
               (assoc entity a (if (txcommon/multi-value? (contains? many a) v)
                                 (assoc-auto-tempids db v ctx)
                                 (assoc-auto-tempid db v ctx)))

               (txcommon/reverse-ref? a)
               (assoc entity a (if (sequential? v)
                                 (assoc-auto-tempids db v ctx)
                                 (assoc-auto-tempid db v ctx)))

               :else entity))
           entity entity))

       (not (sequential? entity))
       entity

       :let [[op e a v] entity]

       (and (= :db/add op) (contains? refs a))
       (if (txcommon/multi-value? (contains? many a) v)
         [op e a (assoc-auto-tempids db v ctx)]
         [op e a (assoc-auto-tempid db v ctx)])

       :else
       entity))))

(deftype IdentityResolution [attr value stored-eid])
(deftype ResolvedIdentityAdd [e resolution])

(defn identity-owner
  "Overlay current pending writes on a plan's native identity resolution."
  [db ^IdentityResolution resolution]
  (txcommon/av-first-e db (.-attr resolution) (.-value resolution)
                      (.-stored-eid resolution)))

(defn resolve-upserts
  "Returns [entity' upserts]. Upsert attributes that resolve to existing entities
   are removed from entity, rest are kept in entity for insertion. Performs the
   validations needed for removed unique identity values."
  ([db entity]
   (resolve-upserts db entity (coreprep/make-prepare-ctx db)))
  ([db entity ctx]
   (if-some [idents (not-empty (:db.unique/identity (:rschema ctx)))]
     (let [schema  (:schema ctx)
           options (:opts ctx)
           upsert-value
           (fn [a v]
             (if (contains? (:db.type/ref (:rschema ctx)) a)
               (when-not (or (tempid? v) (map? v))
                 (txcommon/entid db v))
               (coreprep/correct-value-with-props options (schema a) a v)))
           validate-upsert-preds
           (fn [a v]
             (when (:db.attr/preds (schema a)) (txcommon/flush-batch-prepare!))
             (vld/validate-attr-preds
               (:db/id entity) a v (schema a)
               #(resolve-attr-pred-udf db %)))
           ;; Multi-value inputs reach resolution one scalar at a time.
           resolve (fn [a v at]
                     (vld/validate-val v at)
                     (when-some [v' (upsert-value a v)]
                       (when-some [e (or (txcommon/cached-av-first-e db a v')
                                        (txcommon/av-first-e db a v'))]
                         [e v'])))
           split   (fn [a vs]
                     (reduce
                       (fn [acc v]
                         (if-some [[e v'] (resolve a v entity)]
                           (do
                             (validate-upsert-preds a v')
                             (update acc 1 assoc v e))
                           (update acc 0 conj v)))
                       [[] {}] vs))]
       (reduce-kv
         (fn [[entity' upserts] a v]
           (vld/validate-attr a entity)
           (cond
             (not (contains? idents a))
             (do
               (vld/validate-val v entity)
               [(assoc entity' a v) upserts])

             (txcommon/multi-value? (contains? (:db.cardinality/many (:rschema ctx)) a) v)
             (let [[insert upsert] (split a v)]
               [(cond-> entity'
                  (seq insert) (assoc a insert))
                (cond-> upserts
                  (seq upsert) (assoc a upsert))])

             :else
             (if-some [[e v'] (resolve a v entity)]
               (do
                 (validate-upsert-preds a v')
                 [entity' (assoc upserts a {v e})])
               [(assoc entity' a v) upserts])))
         [{} {}]
         entity))
     [entity nil])))

(defn resolve-identity-map
  "Resolve a flat map whose plan has validated its single scalar identity.
  Preserve map syntax checks and overlay the pending owner at execution time."
  [db entity ^IdentityResolution resolution]
  (reduce-kv (fn [_ a v]
               (vld/validate-attr a entity)
               (vld/validate-val v entity))
             nil entity)
  (let [owner (identity-owner db resolution)]
    [(if owner (dissoc entity (.-attr resolution)) entity) owner]))

(defn validate-upserts
  [entity upserts]
  (vld/validate-upserts entity upserts tempid?))

(def builtin-fn?
  #{:db.fn/call
    :db.fn/cas
    :db/cas
    :db.fn/patchIdoc
    :db/ensure
    :db/add
    :db/retract
    :db.fn/retractAttribute
    :db.fn/retractEntity
    :db/retractEntity})

(defn- lookup-installed-callable
  [db target]
  (when-not (udf/descriptor? target)
    (when-some [eid (txcommon/entid db target)]
      (let [store    (:store db)
            fun      (ea-first-v store eid :db/fn)
            udf-desc (ea-first-v store eid :db/udf)
            ident    (ea-first-v store eid :db/ident)]
        (when (and fun udf-desc)
          (raise "Installed callable entity cannot have both :db/fn and :db/udf: "
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
  ([db target]
   (installed-udf-descriptor db nil target))
  ([db allowed target]
   (when-some [{:keys [ident udf]} (lookup-installed-callable db target)]
     (when udf
       (let [descriptor (udf/descriptor udf)]
         (vld/validate-installed-udf-ident ident descriptor
                                           [:db/udf target])
         (if allowed
           (udf/ensure-kind descriptor allowed)
           descriptor))))))

(defn- attr-pred-udf-context
  [db]
  {:db        db
   :kind      :predicate
   :embedded? true
   :store     (:store db)
   :use       :db.attr/preds})

(defn resolve-attr-pred-udf
  [db pred]
  (let [registry   (txcommon/udf-registry db)
        descriptor (or (installed-udf-descriptor db :predicate pred)
                       (udf/descriptor-or-registered
                         registry :predicate pred))]
    (udf/materialize registry (attr-pred-udf-context db) descriptor)))

(defn- wrap-tx-udf
  [db descriptor]
  (let [registry   (txcommon/udf-registry db)
        descriptor (udf/ensure-kind descriptor :tx-fn)]
    (fn [db* & args]
      (let [callable (udf/materialize registry (tx-udf-context db*)
                                      descriptor)]
        (apply callable db* args)))))

(defn- resolve-installed-tx-callable
  [db target entity]
  (when-some [{:keys [udf] :as installed}
              (lookup-installed-callable db target)]
    (let [fun (:fn installed)]
      (cond
        fun
        (do
          (vld/validate-custom-tx-fn-value fun target entity)
          fun)

        udf
        (wrap-tx-udf db
                     (installed-udf-descriptor db :tx-fn target))

        :else
        nil))))

(defn- resolve-tx-callable
  [db target]
  (cond
    (fn? target)
    target

    (udf/descriptor? target)
    (wrap-tx-udf db target)

    :else
    (or (resolve-installed-tx-callable db target [:db.fn/call target])
        (if (txcommon/entid db target)
          (do
            (vld/validate-custom-tx-fn-value nil target [:db.fn/call target])
            nil)
          (wrap-tx-udf db
                       (udf/descriptor-or-registered
                         (txcommon/udf-registry db) :tx-fn target))))))

(defn handle-fn-call
  [db entity]
  (txcommon/flush-batch-prepare!)
  (let [[_ target & args] entity
        f                 (resolve-tx-callable db target)]
    (failure/with-user-body txcommon/*batch-prepare* (apply f db args))))

(defn handle-custom-tx-fn
  ([db store entity]
   (txcommon/flush-batch-prepare!)
   (let [op    (first entity)
         ident (or (:e (sf (.subSet
                             ^TreeSortedSet (:avet db)
                             (d/datom e0 op nil tx0)
                             (d/datom emax op nil txmax))))
                   (txcommon/entid db op))]
     (vld/validate-custom-tx-fn-entity ident op entity)
     (let [fun  (or (resolve-installed-tx-callable db op entity)
                    (lookup-tx-fn-value db store ident))
           args (next entity)]
       (vld/validate-custom-tx-fn-value fun op entity)
       (failure/with-user-body txcommon/*batch-prepare* (apply fun db args)))))
  ([db store entity entities]
   (concat (handle-custom-tx-fn db store entity) entities)))

(defn- expand-multival?
  [ctx a vs]
  (and (or (txcommon/reverse-ref? a)
           (contains? (:db.cardinality/many (:rschema ctx)) a))
       (coll? vs)
       (not (map? vs))
       (not (and (= (count vs) 2)
                 (contains? (:db.unique/identity (:rschema ctx)) (first vs))))))

(defn maybe-wrap-multival
  [db a vs]
  (if (expand-multival? (coreprep/make-prepare-ctx db) a vs) vs [vs]))

(defn contains-nested-map?
  "Whether explode will emit a nested entity map, without building operations.
   Opaque map values and maps inside lookup references remain ordinary values."
  ([db entity] (contains-nested-map? db entity nil))
  ([db entity resolution]
   (contains-nested-map? db entity resolution (coreprep/make-prepare-ctx db)))
  ([_db entity ^IdentityResolution resolution ctx]
   (reduce-kv
     (fn [_ a vs]
       (if (and (not (identical? a :db/id))
                (not (and resolution (= a (.-attr resolution))))
                ;; Most values cannot emit entity maps; avoid schema work for
                ;; those before considering reference and cardinality rules.
                (or (map? vs)
                    (and (coll? vs)
                         (reduce (fn [_ value]
                                   (if (map? value) (reduced true) false))
                                 false vs)))
                (let [straight-a (if (txcommon/reverse-ref? a)
                                   (txcommon/reverse-ref a) a)]
                  (and (contains? (:db.type/ref (:rschema ctx)) straight-a)
                       (or (map? vs) (expand-multival? ctx a vs)))))
         (reduced true)
         false))
     false entity)))

(defn- add-exploded-value!
  [^ArrayList txs ref? eid a reverse? straight-a v]
  (.add txs
        (if (and ref? (map? v))
          (assoc v (txcommon/reverse-ref a) eid)
          (if reverse?
            [:db/add v straight-a eid]
            [:db/add eid straight-a v]))))

(defn explode
  ([db entity] (explode db entity nil))
  ([db entity resolution]
   (explode db entity resolution (coreprep/make-prepare-ctx db)))
  ([_db entity ^IdentityResolution resolution ctx]
   (let [eid        (:db/id entity)
         txs        (ArrayList. (count entity))
         tuple-txs  (ArrayList.)]
     (reduce-kv
       (fn [_ a vs]
         (when-not (identical? a :db/id)
           (let [reverse?   (txcommon/reverse-ref? a)
                 straight-a (if reverse?
                              (txcommon/reverse-ref a)
                              a)
                 ref? (contains? (:db.type/ref (:rschema ctx)) straight-a)
                 ^ArrayList target (if (contains? (:db/tupleAttrs (:rschema ctx)) a)
                                     tuple-txs
                                     txs)]
             (when reverse?
               (vld/validate-reverse-ref-type
                 ref? a eid vs))
             (cond
               (and resolution (= a (.-attr resolution)))
               (.add target (ResolvedIdentityAdd. eid resolution))

               (expand-multival? ctx a vs)
               (doseq [v vs]
                 (add-exploded-value! target ref? eid a reverse? straight-a v))

               :else
               (add-exploded-value! target ref? eid a reverse? straight-a vs))))
         nil)
       nil
       entity)
     (.addAll txs tuple-txs)
     txs)))

(def de-entity? (delay (resolve 'datalevin.entity/entity?)))
(def de-entity->txs (delay (resolve 'datalevin.entity/->txs)))

(defn expand-transactable-entity
  [entity]
  (if (@de-entity? entity)
    (@de-entity->txs entity)
    [entity]))

(defn update-entity-time
  [entity tx-time]
  (cond
    (map? entity)
    [(assoc entity :db/updated-at tx-time)]

    (sequential? entity)
    (let [[op e _ _] entity]
      (if (or (identical? op :db/retractEntity)
              (identical? op :db.fn/retractEntity)
              (identical? op :db/ensure)
              (identical? op :db.fn/call)
              (and (keyword? op) (not (builtin-fn? op))))
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
  ([db entities tx-time]
   (prepare-entities db entities tx-time (coreprep/make-prepare-ctx db)))
  ([db entities tx-time ctx]
   (let [aat #(assoc-auto-tempid db % ctx)
         uet #(update-entity-time % tx-time)]
     (sequence
       (if (:auto-entity-time? (:opts ctx))
         (comp (mapcat expand-transactable-entity)
               (map aat)
               (mapcat uet))
         (comp (mapcat expand-transactable-entity)
               (map aat)))
       entities))))

(defn prepare-tx-fn-result
  ([db entities tx-time]
   (prepare-tx-fn-result db entities tx-time (coreprep/make-prepare-ctx db)))
  ([db entities tx-time ctx]
   (if (:auto-entity-time? (:opts ctx))
     (prepare-entities db entities tx-time ctx)
     entities)))
