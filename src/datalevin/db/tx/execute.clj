;;
;; Copyright (c) Nikita Prokopov, Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns ^:no-doc datalevin.db.tx.execute
  "Transaction execution loop."
  (:require
   [datalevin.constants :as c :refer [tx0 txmax]]
   [datalevin.datom :as d :refer [datom datom-added datom?]]
   [datalevin.db.tx.common :as txcommon]
   [datalevin.db.tx.prepare :as txprep]
   [datalevin.idoc :as idoc]
   [datalevin.index :as idx]
   [datalevin.interface :refer [fetch init-max-eid opts schema]]
   [datalevin.prepare :as coreprep]
   [datalevin.storage :as s]
   [datalevin.udf :as udf]
   [datalevin.util :as u :refer [ conjv conjs concatv cond+ raise]]
   [datalevin.validate :as vld])
  (:import
   [datalevin.datom Datom]
   [datalevin.storage Store]
   [java.util ArrayList Collection List SortedSet]
   [org.eclipse.collections.impl.set.sorted.mutable TreeSortedSet]))

(deftype ^:private TxStep [report entities])
(deftype ^:private TxRetry [report])

(deftype ^:private ResolvedTuple [^long e a value before])

(defn- cached-ea-first-datom
  [db ^long e a]
  (if txcommon/*batch-prepare*
    (first (filter datom-added
                   (.subSet ^TreeSortedSet (:eavt db)
                            (datom e a nil tx0) (datom e a nil txmax))))
    (let [^SortedSet tail (.tailSet ^TreeSortedSet (:eavt db)
                                    (datom e a nil tx0))]
      (when-not (.isEmpty tail)
        (let [^Datom d (.first tail)]
          (when (and (== e (.-e d)) (= a (.-a d))) d))))))

(defn- cached-eav-first-datom
  [db ^long e a v]
  (if txcommon/*batch-prepare*
    (first (filter datom-added
                   (.subSet ^TreeSortedSet (:eavt db)
                            (datom e a v tx0) (datom e a v txmax))))
    (let [^SortedSet tail (.tailSet ^TreeSortedSet (:eavt db)
                                    (datom e a v tx0))]
      (when-not (.isEmpty tail)
        (let [^Datom d (.first tail)]
          (when (and (== e (.-e d))
                     (= a (.-a d))
                     (= v (.-v d)))
            d))))))

(defn- clear-tx-cache
  [db]
  (when-not txcommon/*batch-prepare*
    (.clear ^TreeSortedSet (:eavt db))
    (.clear ^TreeSortedSet (:avet db)))
  db)

(defn- validate-datom
  [db ^Datom datom]
  (when (and (datom-added datom)
             (txcommon/is-attr? db (.-a datom) :db/unique))
    (let [a (.-a datom)
          v (.-v datom)]
      (vld/validate-datom-unique
        true
        datom
        (fn []
          (or (when (txcommon/cached-av-first-e db a v) true)
              (txcommon/av-first-e db a v)))))))

(defn- current-tx
  {:inline (fn [report] `(-> ~report :db-before :max-tx long inc))}
  ^long [report]
  (-> report :db-before :max-tx long inc))

(defn- next-eid
  {:inline (fn [db] `(inc (long (:max-eid ~db))))}
  ^long [db]
  (inc (long (:max-eid db))))

(defn- tx-id? ^Boolean [e] (identical? :db/current-tx e))

(defn- new-eid? [db ^long eid] (> eid ^long (:max-eid db)))

(defn- advance-max-eid [db eid]
  (cond-> db
    (new-eid? db eid)
    (assoc :max-eid eid)))

(defn- append-tx-datom!
  [report datom]
  (.add ^ArrayList (:tx-data report) datom)
  report)

(defn- allocate-eid
  ([_ report eid]
   (update report :db-after advance-max-eid eid))
  ([tx-time report e eid]
   (let [db   (:db-after report)
         new? (new-eid? db eid)
         db-opts (opts (:store db))]
     (cond-> report
       (tx-id? e)
       (->
         (update :tempids assoc e eid)
         (update ::reverse-tempids update eid conjs e))

       (txprep/tempid? e)
       (->
         (update :tempids assoc e eid)
         (update ::reverse-tempids update eid conjs e))

       (and new? (not (txprep/tempid? e)))
       (update :tempids assoc eid eid)

       (and new? (db-opts :auto-entity-time?))
       (append-tx-datom! (d/datom eid :db/created-at tx-time))

       true
       (update :db-after advance-max-eid eid)))))

(defn- with-datom
  [db ^Datom datom]
  (validate-datom db datom)
  (if txcommon/*batch-prepare*
    (do (txcommon/stage-batch-datom! db datom)
        (if (datom-added datom) (advance-max-eid db (.-e datom)) db))
    (if (datom-added datom)
      (do
        (.add ^TreeSortedSet (:eavt db) datom)
        (when-not (txcommon/is-attr? db (.-a datom) :db/noindex)
          (.add ^TreeSortedSet (:avet db) datom))
        (advance-max-eid db (.-e datom)))
      (if (.isEmpty
            (.subSet ^TreeSortedSet (:eavt db)
                     (d/datom (.-e datom) (.-a datom) (.-v datom) tx0)
                     (d/datom (.-e datom) (.-a datom) (.-v datom) txmax)))
        db
        (do
          (.remove ^TreeSortedSet (:eavt db) datom)
          (when-not (txcommon/is-attr? db (.-a datom) :db/noindex)
            (.remove ^TreeSortedSet (:avet db) datom))
          db)))))

(declare effective-attr-value)

(defn- queue-tuple
  [queue tuple idx report db e _ v]
  (let [^ResolvedTuple queued (get queue tuple)
        before (if queued
                 (.-before queued)
                 (effective-attr-value report db e tuple))
        value (or (when queued (.-value queued))
                  before
                  (vec (repeat (-> (schema (:store db))
                                   (get tuple)
                                   :db/tupleAttrs
                                   count)
                               nil)))]
    (assoc queue tuple (ResolvedTuple. e tuple (assoc value idx v) before))))

(defn- queue-tuples
  [queue tuples report db e a v]
  (reduce-kv
    (fn [queue tuple idx]
      (queue-tuple queue tuple idx report db e a v))
    queue
    tuples))

(defn- track-retraction
  [report datom]
  (when-let [retracted (and (not (datom-added datom)) (::tx-retracted report))]
    (conj! retracted datom))
  report)

(defn- transact-report
  [report ^Datom datom]
  (let [db      (:db-after report)
        a       (.-a datom)
        db'     (with-datom db datom)
        report' (-> report
                    (cond-> (not (identical? db db')) (assoc :db-after db'))
                    (append-tx-datom! datom)
                    (track-retraction datom))]
    (if (txcommon/tuple-source? db a)
      (let [e      (.-e datom)
            v      (if (datom-added datom) (.-v datom) nil)
            queue  (or (-> report' ::queued-tuples (get e)) {})
            tuples (get (txcommon/attrs-by db :db/attrTuples) a)
            queue' (queue-tuples queue tuples report' db e a v)]
        (update report' ::queued-tuples assoc e queue'))
      report')))

(defn- transact-same-value
  [report datom]
  ;; Build membership on the first redundant operation, then maintain it as
  ;; retractions are emitted. Transactions without redundant values avoid it.
  ;; Keep even re-added values; Datom equivalence ignores tx, added? and metadata.
  (let [report (if (::tx-retracted report)
                 report
                 (assoc report ::tx-retracted
                        (reduce (fn [retracted d]
                                  (if (datom-added d)
                                    retracted
                                    (conj! retracted d)))
                                (transient #{}) (:tx-data report))))]
    (if (contains? (::tx-retracted report) datom)
      (transact-report report datom)
      (update report ::tx-redundant conjv datom))))

(defn- apply-add
  "Apply a normalized value against its known previous value. Ordinary input,
  CAS and derived tuples share datom publication and constraint maintenance."
  [report ^Datom new-datom old-value]
  (cond
    (nil? old-value)
    (transact-report report new-datom)

    (d/value-equal? old-value (.-v new-datom))
    (transact-same-value report new-datom)

    :else
    (-> report
        (transact-report (datom (.-e new-datom) (.-a new-datom) old-value
                               (d/datom-tx new-datom) false))
        (transact-report new-datom))))

(defn- pending-attr-state
  [report e a]
  (let [^List datoms (:tx-data report)]
    (loop [i (dec (.size datoms))]
      (if (neg? i)
        ::missing-attr-state
        (let [^Datom d (.get datoms i)]
          (if (and (= e (.-e d)) (= a (.-a d)))
            (if (datom-added d) (.-v d) ::retracted-attr-state)
            (recur (dec i))))))))

(defn- effective-attr-value
  [report db e a]
  (if (> (long e) ^long (::max-eid-before report))
    ;; New entities have no stored values to mask. The existing EAV overlay
    ;; already reflects their additions and retractions; avoid scanning all
    ;; preceding transaction datoms each time a new tuple is queued.
    (when-let [^Datom d (cached-ea-first-datom db e a)] (.-v d))
    (let [pending (pending-attr-state report e a)]
      (cond
        (identical? pending ::missing-attr-state)
        (txcommon/ea-first-v db e a)

        (identical? pending ::retracted-attr-state)
        nil

        :else
        pending))))

(defn- validate-installed-callable-write
  [report db e a v ent]
  (case a
    :db/udf
    (let [descriptor (udf/descriptor v)
          ident      (effective-attr-value report db e :db/ident)]
      (vld/validate-installed-udf-ident ident descriptor ent)
      (when (some? (effective-attr-value report db e :db/fn))
        (raise "Installed callable entity cannot have both :db/fn and :db/udf at "
                 ent
                 {:error   :transact/syntax
                  :tx-data ent
                  :db/id   e})))

    :db/fn
    (when (some? (effective-attr-value report db e :db/udf))
      (raise "Installed callable entity cannot have both :db/fn and :db/udf at "
               ent
               {:error   :transact/syntax
                :tx-data ent
                :db/id   e}))

    :db/ident
    (when-some [descriptor (effective-attr-value report db e :db/udf)]
      (vld/validate-installed-udf-ident v (udf/descriptor descriptor) ent))

    nil))

(defn- transact-resolved-add
  "Publish a normalized datom. IDs, value, transaction and metadata are already
  carried by the datom; old-value reads do not repeat input resolution."
  [report ^Datom new-datom props]
  (let [e (.-e new-datom)
        a (.-a new-datom)
        value (.-v new-datom)
        db (:db-after report)
        store (:store db)
        report (if (or props (contains? (::new-attributes report) a))
                 report
                 (update report ::new-attributes u/conjv a))
        ^Datom old-datom
        (if (identical? (:db/cardinality props) :db.cardinality/many)
          (or (cached-eav-first-datom db e a value)
              (when (<= (long e) ^long (::max-eid-before report))
                (first (txcommon/visible-stored db (fetch store (datom e a value))))))
          (or (cached-ea-first-datom db e a)
              ;; Capture the store boundary before allocation, including writes
              ;; made by other connections since db-before was obtained.
              (when (<= (long e) ^long (::max-eid-before report))
                (txcommon/ea-first-datom db e a))))]
    (apply-add report new-datom (when old-datom (.-v old-datom)))))

(defn- transact-add
  ([report [_ _ a _ _ :as ent]]
   (let [store (:store (:db-after report))
         props ((schema store) a)]
     (transact-add report ent props (opts store))))
  ([report [_ e a v tx :as ent] props store-opts]
   (vld/validate-attr a ent)
   (vld/validate-val v ent)
   (let [db (:db-after report)
         e (long (txcommon/entid-strict db e))
         _ (validate-installed-callable-write report db e a v ent)
         v (if (identical? (:db/valueType props) :db.type/ref)
             (txcommon/entid-strict db v) v)
         value (coreprep/correct-value-with-props store-opts props a v)]
     (when (:db.attr/preds props)
       (txcommon/flush-batch-prepare!)
       (vld/validate-attr-preds
         e a value props #(txprep/resolve-attr-pred-udf db %)))
     (let [meta* (meta ent)
           new-datom (cond-> (datom e a value (or tx (current-tx report)))
                       meta* (with-meta meta*))]
       (transact-resolved-add report new-datom props)))))

(defn- transact-retract-datom [report ^Datom d]
  (let [tx (current-tx report)]
    (transact-report report (datom (.-e d) (.-a d) (.-v d) tx false))))

(defn- transact-cas-cardinality-one
  [report db store entity e a nv ^Datom old-datom]
  (let [tx        (current-tx report)
        _         (validate-installed-callable-write report db e a nv entity)
        props     ((schema store) a)
        nv'       (coreprep/correct-value-with-props
                    (opts store) props a nv)
        _         (when (:db.attr/preds props)
                    (txcommon/flush-batch-prepare!)
                    (vld/validate-attr-preds
                      e a nv' props #(txprep/resolve-attr-pred-udf db %)))
        meta*     (meta entity)
        new-datom (cond-> (datom e a nv' tx)
                    meta* (with-meta meta*))]
    (apply-add report new-datom (when old-datom (.-v old-datom)))))

(defn- retract-components
  ([db datoms]
   (retract-components db #{} datoms))
  ([db components datoms]
   (reduce (fn [components ^Datom d]
             (if (txcommon/component? db (.-a d))
               (conj components [:db.fn/retractEntity (.-v d)])
               components))
           components
           datoms)))

(defn- dissoc-present
  [m k]
  (if (contains? m k) (dissoc m k) m))

(defn- remove-auto-tempids
  [tempids]
  (if (reduce-kv (fn [_ k _]
                   (if (txprep/auto-tempid? k) (reduced true) false))
                 false
                 tempids)
    (u/removem txprep/auto-tempid? tempids)
    tempids))

(defn- contains-map-entity?
  ^Boolean [^List entities]
  (loop [i 0]
    (cond
      (== i (.size entities)) false
      (map? (.get entities i)) true
      :else (recur (unchecked-inc i)))))

(defn- check-value-tempids [report]
  (let [tx-data (:tx-data report)]
    (if-let [tempids (::value-tempids report)]
      (let [all-tempids (transient tempids)
            reduce-fn   (fn [tempids datom]
                          (if (datom-added datom)
                            (dissoc! tempids (:e datom))
                            tempids))
            unused      (reduce reduce-fn all-tempids tx-data)
            unused      (reduce reduce-fn unused (::tx-redundant report))]
        (vld/validate-value-tempids (vals (persistent! unused)))
        (-> report
            (dissoc-present ::value-tempids)
            (dissoc-present ::tx-redundant)))
      (-> report
          (dissoc-present ::value-tempids)
          (dissoc-present ::tx-redundant)))))

(defn- retry-with-tempid
  [initial-report report _es tempid upserted-eid _tx-time]
  (let [eid (get (::upserted-tempids initial-report) tempid)]
    (vld/validate-upsert-retry-conflict eid tempid upserted-eid)
    ;; Retain stable mappings and tempids that own this in-flight target.
    (let [target-tempids (get (::reverse-tempids report) upserted-eid)
          tempids'       (-> (:tempids initial-report)
                             (merge (select-keys (:tempids report)
                                                 target-tempids))
                             (assoc tempid upserted-eid))
          report'        (-> initial-report
                             (assoc :tempids tempids')
                             (update ::upserted-tempids assoc
                                     tempid upserted-eid))]
      (when-let [context txcommon/*batch-prepare*]
        (if (satisfies? txcommon/BatchPreparation context)
          (txcommon/restore-preparation! context (:db-after initial-report))
          (when-let [restore! (:restore! context)] (restore!))))
      (TxRetry. report'))))

(defn- flush-tuples
  [report schema]
  (let [queued (::queued-tuples report)
        ;; Identity tuples can restart resolution; predicates can execute user
        ;; code. Preserve ordered general dispatch for that whole flush.
        resolved? (every? (fn [[_ tuples]]
                            (every? (fn [a]
                                      (let [props (schema a)]
                                        (and (not= :db.unique/identity (:db/unique props))
                                             (not (:db.attr/preds props)))))
                                    (keys tuples)))
                          queued)]
    (persistent!
      (reduce-kv
        (fn [entities eid tuples+values]
          (reduce-kv
            (fn [entities tuple ^ResolvedTuple change]
              (let [queued-value (.-value change)
                    value (when-not (every? nil? queued-value) queued-value)
                    current (.-before change)]
                (if (= value current)
                  entities
                  (conj! entities
                    (if resolved?
                      (if (nil? value)
                        (ResolvedTuple. eid tuple nil current)
                        change)
                      (if (nil? value)
                        ^::internal [:db/retract eid tuple current]
                        ^::internal [:db/add eid tuple value]))))))
            entities tuples+values))
        (transient []) queued))))

(defn- transact-resolved-tuple
  [report schema store-opts ^ResolvedTuple change]
  (let [e (.-e change) a (.-a change) value (.-value change)
        tx (current-tx report)]
    (if (nil? value)
      (transact-report report (datom e a (.-before change) tx false))
      (let [value (coreprep/correct-value-with-props store-opts (schema a) a value)
            new-datom (with-meta (datom e a value tx) {::internal true})]
        (apply-add report new-datom (.-before change))))))

(defn- finalize-report
  [report]
  (let [new-attrs (::new-attributes report)
        report    (check-value-tempids report)
        report    (-> report
                      ;; No attempt-local mutable collection escapes in the
                      ;; published report or in a simulated transaction view.
                      (assoc :tx-data (vec (:tx-data report)))
                      (dissoc-present ::upserted-tempids)
                      (dissoc-present ::reverse-tempids)
                      (dissoc-present ::max-eid-before)
                      (dissoc-present ::tx-retracted)
                      (dissoc-present ::new-attributes))
        tx-id     (current-tx report)
        report    (update report :tempids
                          #(assoc (remove-auto-tempids %)
                                  :db/current-tx tx-id))
        report    (update report :db-after update :max-tx u/long-inc)]
    (cond-> report
      (seq new-attrs) (assoc :new-attributes new-attrs))))

(defn- handle-cas
  [report db store entity entities]
  (let [[_ e a ov nv] entity
        e             (txcommon/entid-strict db e)
        _             (vld/validate-attr a entity)
        ov            (if (txcommon/ref? db a)
                        (txcommon/entid-strict db ov)
                        ov)
        nv            (if (txcommon/ref? db a)
                        (txcommon/entid-strict db nv)
                        nv)
        _             (vld/validate-val nv entity)
        multival?     (txcommon/multival? db a)
        datoms        (if txcommon/*batch-prepare*
                        (txcommon/ea-datoms db e a)
                        (concatv
                          (.subSet ^TreeSortedSet (:eavt db)
                                   (datom e a nil tx0) (datom e a nil txmax))
                          (txcommon/ea-datoms db e a)))]
    (vld/validate-cas-value multival? e a ov nv datoms)
    [(if multival?
       (transact-add report [:db/add e a nv])
       (transact-cas-cardinality-one
         report db store entity e a nv (first datoms)))
     entities]))

(defn- handle-patch-idoc
  [report db store schema entity entities]
  (let [argc (count entity)]
    (vld/validate-patch-idoc-arity argc entity (first entity))
    (let [[_ e a x y] entity
          [old-v ops] (if (= argc 5) [x y] [nil x])
          e           (txcommon/entid-strict db e)
          _           (vld/validate-attr a entity)
          props       (schema a)
          _           (vld/validate-patch-idoc-type
                        (idx/value-type props) a)
          many?       (txcommon/multival? db a)
          _           (vld/validate-patch-idoc-cardinality
                        many? old-v a)
          ops         (or ops [])]
      (if (empty? ops)
        [report entities]
        (if many?
          (let [old-v'    (coreprep/correct-value-with-props
                            (opts store) props a old-v)
                old-datom (or (cached-eav-first-datom db e a old-v')
                              (first (txcommon/visible-stored
                                       db (fetch (:store db) (datom e a old-v')))))]
            (vld/validate-patch-idoc-old-value old-datom old-v a)
            (let [old-doc             (.-v ^Datom old-datom)
                  {:keys [doc paths]} (idoc/apply-patch old-doc ops)]
              (if (= old-doc doc)
                [report entities]
                (let [ent (with-meta [:db/add e a doc]
                            {:idoc/patch {:paths paths}})]
                  [(transact-add report ent)
                   (cons [:db/retract e a old-doc] entities)]))))
          (let [old-datom           (or (cached-ea-first-datom db e a)
                                        (txcommon/ea-first-datom db e a))
                old-doc             (when old-datom (.-v ^Datom old-datom))
                {:keys [doc paths]} (idoc/apply-patch (or old-doc {}) ops)
                ent                 (with-meta [:db/add e a doc]
                                      {:idoc/patch {:paths paths}})]
            [(transact-add report ent) entities]))))))

(defn- handle-ensure
  [report entity entities]
  (vld/validate-ensure-arity (count entity) entity)
  (let [[_ pred & args] entity]
    (vld/validate-ensure-predicate pred entity)
    [(update report ::ensures conjv [pred args entity]) entities]))

(defn- handle-ref-tempid-value
  [report db tempids entity entities es tx-time]
  (let [[op e a v] entity]
    (if-some [resolved (get tempids v)]
      [(update report ::value-tempids assoc resolved v)
       (cons [op e a resolved] entities)]
      (let [resolved (next-eid db)]
        [(-> (allocate-eid tx-time report v resolved)
             (update ::value-tempids assoc resolved v))
         es]))))

(defn- handle-tempid-entity
  [initial-report report db _store tempids unique-identity? entity entities
   initial-es tx-time]
  (let [[op e a v] entity
        upserted-eid  (when unique-identity?
                        (or (txcommon/cached-av-first-e db a v)
                            (txcommon/av-first-e db a v)))
        allocated-eid (get tempids e)]
    (if (and upserted-eid allocated-eid (not= upserted-eid allocated-eid))
      (retry-with-tempid initial-report report initial-es e upserted-eid
                         tx-time)
      (let [eid (or upserted-eid allocated-eid (next-eid db))]
        [(if allocated-eid
           report
           (allocate-eid tx-time report e eid))
         (cons [op eid a v] entities)]))))

(defn- handle-reverse-tempid-upsert
  [initial-report report entity entities initial-es tx-time upserted-eid]
  (let [[_ e _ _] entity
        tempids (get (::reverse-tempids report) e)
        tempid  (u/find #(not (contains? (::upserted-tempids report) %))
                        tempids)]
    (vld/validate-upsert-conflict tempid e upserted-eid entity)
    (retry-with-tempid initial-report report initial-es tempid
                       upserted-eid tx-time)))

(defn- handle-tuple-attr
  [db _store schema entity report entities]
  (let [[_ e a v] entity
        tuple-attrs (get-in schema [a :db/tupleAttrs])]
    (vld/validate-tuple-direct-write
      (and
        (every? some? v)
        (= (count tuple-attrs) (count v))
        (every?
          (fn [[tuple-attr tuple-value]]
            (let [db-value
                  (or (:v (cached-ea-first-datom db e tuple-attr))
                      (txcommon/ea-first-v db e tuple-attr))]
              (= tuple-value db-value)))
          (mapv vector tuple-attrs v)))
      entity)
    [report entities]))

(defn- handle-tuple-ref-tempids
  [report db tempids entity entities vs tx-time]
  (let [[op e a _] entity
        [report' v']
        (loop [report' report
               vs      vs
               v'      []]
          (if-let [[[tuple-type v] & vs] vs]
            (if (and (identical? tuple-type :db.type/ref)
                     (txprep/tempid? v))
              (if-some [resolved (get tempids v)]
                (recur report' vs (conj v' resolved))
                (let [resolved (next-eid db)
                      report'  (-> (allocate-eid tx-time report' v resolved)
                                   (update ::value-tempids assoc resolved v))]
                  (recur report' vs (conj v' resolved))))
              (recur report' vs (conj v' v)))
            [report' v']))]
    [report' (cons [op e a v'] entities)]))

(defn- handle-tuple-ref-idents
  [db entity vs]
  (let [[op e a _] entity
        v' (mapv (fn [[tuple-type v]]
                   (if (and (identical? tuple-type :db.type/ref)
                            (keyword? v))
                     (txcommon/entid-strict db v)
                     v))
                 vs)]
    [op e a v']))

(defn- handle-retract-value
  [report db store entity entities]
  (let [[_ e a v] entity]
    (if-some [e (txcommon/entid db e)]
      (let [v (if (txcommon/ref? db a)
                (txcommon/entid-strict db v)
                v)]
        (vld/validate-attr a entity)
        (vld/validate-val v entity)
        (if-some [old-datom (or (cached-eav-first-datom db e a v)
                                (first (txcommon/visible-stored db (fetch store (datom e a v)))))]
          [(transact-retract-datom report old-datom) entities]
          [report entities]))
      [report entities])))

(defn- handle-retract-attribute
  [report db _store entity entities]
  (let [[_ e a _] entity]
    (if-some [e (txcommon/entid db e)]
      (let [_       (vld/validate-attr a entity)
            stored  (txcommon/ea-datoms db e a)
            cached  (when-not txcommon/*batch-prepare*
                      (vec (.subSet ^TreeSortedSet (:eavt db)
                                    (datom e a nil tx0) (datom e a nil txmax))))
            report' (reduce transact-retract-datom report stored)
            report' (reduce transact-retract-datom report' cached)
            components (retract-components db stored)
            components (retract-components db components cached)]
        [report' (if (seq components)
                   (concat components entities)
                   entities)])
      [report entities])))

(defn- handle-retract-entity
  [report db _store entity entities]
  (let [[_ e _ _] entity]
    (if-some [e (txcommon/entid db e)]
      (let [stored-e-datoms (vec (txcommon/e-datoms db e))
            cached-e-datoms (when-not txcommon/*batch-prepare*
                              (vec (.subSet ^TreeSortedSet (:eavt db)
                                            (datom e nil nil tx0) (datom e nil nil txmax))))
            v-datoms        (vec (txcommon/v-datoms db e))
            report'         (reduce transact-retract-datom
                                    report stored-e-datoms)
            report'         (reduce transact-retract-datom
                                    report' cached-e-datoms)
            report'         (reduce transact-retract-datom report' v-datoms)
            components      (retract-components db stored-e-datoms)
            components      (retract-components db components cached-e-datoms)]
        [report' (if (seq components)
                   (concat components entities)
                   entities)])
      [report entities])))

(defn- handle-map-entity
  [initial-report report db entity entities initial-es tx-time]
  (let [old-eid (:db/id entity)]
    (vld/validate-installed-callable-entity entity)
    (cond+
      (tx-id? old-eid)
      (let [id (current-tx report)]
        [(allocate-eid tx-time report old-eid id)
         (cons (assoc entity :db/id id) entities)])

      (sequential? old-eid)
      (let [id (txcommon/entid-strict db old-eid)]
        [report (cons (assoc entity :db/id id) entities)])

      :let [[entity' upserts] (txprep/resolve-upserts db entity)
            upserted-eid      (txprep/validate-upserts entity' upserts)]

      (some? upserted-eid)
      (let [tempids (:tempids report)]
        (if (and (txprep/tempid? old-eid)
                 (contains? tempids old-eid)
                 (not= upserted-eid (get tempids old-eid)))
          (retry-with-tempid initial-report report initial-es old-eid
                             upserted-eid tx-time)
          [(-> (allocate-eid tx-time report old-eid upserted-eid)
               (update ::tx-redundant conjv
                       (datom upserted-eid nil nil tx0)))
           (concat (txprep/explode db
                                   (assoc entity' :db/id upserted-eid))
                   entities)]))

      (txprep/tempid? old-eid)
      (let [allocated-eid (get (:tempids report) old-eid)
            eid           (or allocated-eid (next-eid db))
            txs           (txprep/explode db (assoc entity :db/id eid))]
        (cond
          (.isEmpty ^List txs)
          [report entities]

          (contains-map-entity? txs)
          [report (concat (txprep/explode db entity) entities)]

          :else
          [(if allocated-eid
             report
             (allocate-eid tx-time report old-eid eid))
           (concat txs entities)]))

      (or (number? old-eid)
          (nil? old-eid)
          (string? old-eid)
          (txprep/auto-tempid? old-eid))
      [report (concat (txprep/explode db entity) entities)]

      :else
      (vld/validate-map-entity-id-syntax old-eid))))

(defn- regular-attr?
  "An attribute that needs no identity, reference or tuple input resolution.
  Composite source attributes still use the shared tuple maintenance below."
  [props]
  (and (not (identical? (:db/valueType props) :db.type/ref))
       (not (identical? (:db/unique props) :db.unique/identity))
       (not (contains? props :db/tupleAttrs))
       (not (contains? props :db/tupleType))
       (not (contains? props :db/tupleTypes))))

(defn- regular-map?
  [schema entity]
  (let [eid (:db/id entity)]
    (and (or (txprep/tempid? eid)
             (and (integer? eid) (not (neg? ^long eid))))
         (reduce-kv
           (fn [_ a v]
             (if (identical? a :db/id)
               true
               (let [props (schema a)]
                 (if (and (keyword? a)
                          (not= "db" (namespace a))
                          (not (txcommon/reverse-ref? a))
                          (or (regular-attr? props)
                              ;; Resolved reference values need no map expansion.
                              ;; Keep scalar operation dispatch on its existing
                              ;; path; unresolved/identity/tuple inputs still
                              ;; require the general resolver.
                              (and (identical? (:db/valueType props) :db.type/ref)
                                   (integer? v) (not (neg? ^long v))
                                   (not (identical? (:db/unique props) :db.unique/identity))
                                   (not (contains? props :db/tupleAttrs))
                                   (not (contains? props :db/tupleType))
                                   (not (contains? props :db/tupleTypes))))
                          (not (identical? (:db/cardinality props)
                                           :db.cardinality/many))
                          (not (:db.attr/preds props)))
                   true
                   (reduced false)))))
           true entity))))

(defn- transact-regular-map
  "Feed a flat map directly to the shared add operation. Input forms requiring
  expansion or upsert retries remain in handle-map-entity; both paths use the
  same validation, pending indexes, retractions, and composite maintenance."
  [report schema store-opts entity tx-time]
  (if (= 1 (count entity))
    report
    (let [old-eid (:db/id entity)
          temporary? (txprep/tempid? old-eid)
          allocated (when temporary? (get (:tempids report) old-eid))
          eid (if temporary? (or allocated (next-eid (:db-after report))) old-eid)
          report (if (and temporary? (nil? allocated))
                   (allocate-eid tx-time report old-eid eid)
                   report)]
      (reduce-kv
        (fn [report a v]
          (if (identical? a :db/id)
            report
            (let [props (schema a)]
              ;; The map gate checked attribute syntax and resolved references.
              ;; Materialize an operation only for a value error's context.
              (when (nil? v) (vld/validate-val v [:db/add eid a v]))
              (transact-resolved-add
                report
                (datom eid a (coreprep/correct-value-with-props store-opts props a v)
                       (current-tx report))
                props))))
        report entity))))

(defn- handle-sequential-entity
  [initial-report report db store schema store-opts tempids entity entities
   initial-es tx-time]
  (let [[op e a v]     entity
        props          (schema a)
        ref?           (identical? (:db/valueType props) :db.type/ref)
        unique-identity? (identical? (:db/unique props)
                                     :db.unique/identity)
        tuple-attr?    (contains? props :db/tupleAttrs)
        tuple-type?    (contains? props :db/tupleType)
        tuple-types?   (contains? props :db/tupleTypes)]
    (cond+
      (identical? op :db.fn/call)
      [report (concat
                (txprep/prepare-tx-fn-result
                  db (txprep/handle-fn-call db entity) tx-time)
                entities)]

      (identical? op :db/ensure)
      (handle-ensure report entity entities)

      (and (keyword? op) (not (txprep/builtin-fn? op)))
      [report (concat
                (txprep/prepare-tx-fn-result
                  db (txprep/handle-custom-tx-fn db store entity) tx-time)
                entities)]

      (and (txprep/tempid? e) (not (identical? op :db/add)))
      (vld/validate-tempid-op true op entity)

      (or (identical? op :db.fn/cas) (identical? op :db/cas))
      (handle-cas report db store entity entities)

      (identical? op :db.fn/patchIdoc)
      (handle-patch-idoc report db store schema entity entities)

      (tx-id? e)
      [(allocate-eid tx-time report e (current-tx report))
       (cons [op (current-tx report) a v] entities)]

      (and ref? (tx-id? v))
      [(allocate-eid tx-time report v (current-tx report))
       (cons [op e a (current-tx report)] entities)]

      (and ref? (txprep/tempid? v))
      (handle-ref-tempid-value report db tempids entity entities
                               (cons entity entities) tx-time)

      (and ref? (sequential? v))
      (let [resolved (txcommon/entid-strict db v)]
        [report (cons [op e a resolved] entities)])

      (txprep/tempid? e)
      (handle-tempid-entity initial-report report db store tempids
                            unique-identity? entity entities initial-es
                            tx-time)

      :let [upserted-eid (when (and unique-identity?
                                    (contains? (::reverse-tempids report) e)
                                    e)
                           (or (txcommon/cached-av-first-e db a v)
                               (txcommon/av-first-e db a v)))]

      (and upserted-eid (not= e upserted-eid))
      (handle-reverse-tempid-upsert initial-report report entity entities
                                    initial-es tx-time upserted-eid)

      (and tuple-attr? (not (::internal (meta entity))))
      (handle-tuple-attr db store schema entity report entities)

      :let [tuple-types (when (and (or tuple-type? tuple-types?)
                                   (not (::internal (meta entity))))
                           (or (:db/tupleTypes props)
                               (repeat (:db/tupleType props))))
            vs (when tuple-types
                 (partition 2 (interleave tuple-types v)))]

      (some #(and (identical? (first %) :db.type/ref)
                  (txprep/tempid? (second %))) vs)
      (handle-tuple-ref-tempids report db tempids entity entities vs tx-time)

      (some #(and (identical? (first %) :db.type/ref)
                  (keyword? (second %))) vs)
      [report (cons (handle-tuple-ref-idents db entity vs) entities)]

      (identical? op :db/add)
      (TxStep. (transact-add report entity props store-opts) entities)

      (and (identical? op :db/retract) (some? v))
      (handle-retract-value report db store entity entities)

      (or (identical? op :db.fn/retractAttribute)
          (identical? op :db/retract))
      (handle-retract-attribute report db store entity entities)

      (or (identical? op :db.fn/retractEntity)
          (identical? op :db/retractEntity))
      (handle-retract-entity report db store entity entities)

      :else
      (vld/validate-tx-op op entity))))

(defn- execute-tx-loop*
  [initial-report initial-es tx-time]
  (let [db              (:db-before initial-report)
        store           (:store db)
        ;; The owning writer supplies its current maximum. Preserve the
        ;; attempt boundary on the initial report so upsert retries reuse it.
        max-eid-before  (max (long (:max-eid db))
                             (long (or (::max-eid-before initial-report)
                                       (init-max-eid store))))
        initial-report' (-> initial-report
                            (dissoc-present ::tx-retracted)
                            ;; Each retry owns a fresh append buffer. Complex
                            ;; handlers and regular adds share it, and only the
                            ;; finalized report exposes an immutable vector.
                            (assoc :tx-data
                                   (doto (ArrayList. 32)
                                     (.addAll ^Collection (:tx-data initial-report))))
                            (assoc ::max-eid-before max-eid-before)
                            (update :db-after
                                    #(-> % clear-tx-cache
                                         (advance-max-eid max-eid-before))))
        tuples?         (seq (txcommon/attrs-by db :db.type/tuple))
        schema          (schema store)
        store-opts      (opts store)]
    (loop [report    initial-report'
           es        (when-not tuples? initial-es)
           remaining (when tuples? (seq initial-es))]
      (cond+
        (empty? es)
        ;; Finish all expansions of one input before deriving its tuples.
        ;; Empty queues need neither synthetic operations nor a handler result.
        (cond
          (::queued-tuples report)
          (recur (dissoc report ::queued-tuples)
                 (seq (flush-tuples report schema)) remaining)

          (seq remaining)
          (recur report (list (first remaining)) (next remaining))

          :else
          (finalize-report report))

        :let [[entity & entities] es]

        :let [db      (:db-after report)
              tempids (:tempids report)]

        (instance? ResolvedTuple entity)
        (recur (transact-resolved-tuple report schema store-opts entity)
               entities remaining)

        (map? entity)
        (if (regular-map? schema entity)
          (recur (transact-regular-map report schema store-opts entity tx-time)
                 entities remaining)
          (let [result (handle-map-entity initial-report report db entity
                                          entities initial-es tx-time)]
            (if (or (instance? TxRetry result) (map? result))
              result
              (let [[r' es'] result]
                (recur r' es' remaining)))))

        (sequential? entity)
        (let [[op e a] entity
              props (schema a)]
          (if (and (identical? op :db/add)
                   (integer? e) (not (neg? ^long e))
                   (regular-attr? props))
            (recur (transact-add report entity props store-opts) entities remaining)
            (let [result (handle-sequential-entity
                           initial-report report db store schema store-opts
                           tempids entity entities initial-es tx-time)]
              (cond
                (instance? TxRetry result)
                result

                (instance? TxStep result)
                (recur (.-report ^TxStep result) (.-entities ^TxStep result)
                       remaining)

                (map? result)
                result

                :else
                (let [[r' es'] result]
                  (recur r' es' remaining))))))

        (datom? entity)
        (let [[e a v tx added] entity]
          (if added
            (recur (transact-add report [:db/add e a v tx]) entities remaining)
            (recur report (cons [:db/retract e a v] entities) remaining)))

        (nil? entity)
        (recur report entities remaining)

        :else
        (vld/validate-tx-entity-type entity)))))

(defn- flat-upsert-tempids
  "Resolve independent flat identity maps before allocating forward refs.
  Restrict this to distinct explicit tempids and immutable scalar identities;
  ordered mutations, nested entities and user predicates use ordinary retries."
  [db entities]
  ;; :db/ident is implicit, but db-namespaced keys cannot enter this flat
  ;; path. Only identities that an eligible map can name justify the walk.
  (when (some #(not= "db" (namespace %))
              (txcommon/attrs-by db :db.unique/identity))
    (let [store (:store db)
          schema (schema store)
          identity? #(identical? :db.unique/identity (:db/unique (schema %)))
          scalar-types #{nil :db.type/string :db.type/long :db.type/keyword
                         :db.type/symbol :db.type/uuid :db.type/instant
                         :db.type/boolean :db.type/float :db.type/double
                         :db.type/bigint :db.type/bigdec :db.type/bytes}]
      (when (and (next entities)
                 (not (:auto-entity-time? (opts store)))
                 (empty? (txcommon/attrs-by db :db.type/tuple))
                 (every? (fn [entity]
                           (and (map? entity)
                                (txprep/tempid? (:db/id entity))
                                (not-any? coll? (vals entity))
                                (every? #(and (keyword? %)
                                              (or (= :db/id %)
                                                  (not= "db" (namespace %))))
                                        (keys entity))
                                (every? #(nil? (:db.attr/preds (schema %))) (keys entity))
                                (<= (count (filter identity? (keys entity))) 1)
                                (every? (fn [[a v]]
                                          (or (not (identity? a))
                                              (let [props (schema a)]
                                                (and (some? v) (not (coll? v))
                                                     (contains? scalar-types (:db/valueType props))
                                                     (not= :db.cardinality/many (:db/cardinality props))
                                                     (nil? (:db.attr/preds props))))))
                                        entity)))
                         entities)
                 (= (count entities) (count (set (map :db/id entities)))))
        (reduce (fn [tempids entity]
                  (if-let [a (first (filter identity? (keys entity)))]
                    (let [v (coreprep/correct-value-with-props
                              (opts store) (schema a) a (get entity a))]
                      (if-some [eid (txcommon/av-first-e db a v)]
                        (assoc tempids (:db/id entity) eid)
                        tempids))
                    tempids))
                {} entities)))))

(defn execute-tx-loop
  [initial-report initial-es tx-time]
  (let [upserts (flat-upsert-tempids (:db-before initial-report) initial-es)
        initial-report (if (seq upserts)
                         (-> initial-report
                             (update :tempids merge upserts)
                             (update ::upserted-tempids merge upserts))
                         initial-report)]
    (loop [initial-report initial-report]
      (let [result (txcommon/with-lookup-ref-cache (:db-before initial-report)
                     (execute-tx-loop* initial-report initial-es tx-time))]
        (if (instance? TxRetry result)
          (recur (.-report ^TxRetry result))
          result)))))

(defn local-transact-tx-data
  [initial-report initial-es tx-time]
  (if c/*use-prepare-path*
    (let [db    (:db-before initial-report)
          store (:store db)
          lmdb  (when (instance? Store store)
                  (.-lmdb ^Store store))
          ctx   (coreprep/make-prepare-ctx db lmdb)
          ptx   (coreprep/prepare-tx
                   ctx initial-es tx-time
                   (fn [es t]
                     (execute-tx-loop initial-report es t)))]
      (cond-> (assoc (dissoc-present initial-report ::max-eid-before)
                     :db-after (:db-after ptx)
                     :tx-data (:tx-data ptx)
                     :tempids (:tempids ptx))
        (seq (:new-attributes ptx))
        (assoc :new-attributes (:new-attributes ptx))

        (seq (:ensures ptx))
        (assoc ::ensures (:ensures ptx))))
    (execute-tx-loop initial-report initial-es tx-time)))
