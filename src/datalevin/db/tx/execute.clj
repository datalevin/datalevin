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
   [datalevin.constants :as c :refer [e0 tx0 emax txmax v0 vmax]]
   [datalevin.datom :as d :refer [datom datom-added datom?]]
   [datalevin.db.tx.common :as txcommon]
   [datalevin.db.tx.prepare :as txprep]
   [datalevin.idoc :as idoc]
   [datalevin.index :as idx]
   [datalevin.interface :refer [av-first-e ea-first-v ea-first-datom
                                av-first-datom fetch slice e-datoms
                                v-datoms opts schema]]
   [datalevin.prepare :as coreprep]
   [datalevin.storage :as s]
   [datalevin.udf :as udf]
   [datalevin.util :as u :refer [conjv conjs concatv cond+]]
   [datalevin.validate :as vld])
  (:import
   [datalevin.datom Datom]
   [datalevin.storage Store]
   [java.util SortedSet]
   [org.eclipse.collections.impl.set.sorted.mutable TreeSortedSet]))

(defn- sf [^SortedSet s] (when-not (.isEmpty s) (.first s)))

(defn- clear-tx-cache
  [db]
  (let [clear #(.clear ^TreeSortedSet %)]
    (clear (:eavt db))
    (clear (:avet db))
    db))

(defn- validate-datom
  [db ^Datom datom]
  (let [a       (.-a datom)
        v       (.-v datom)
        unique? (txcommon/is-attr? db a :db/unique)
        found?  (fn []
                  (or (not (.isEmpty
                             (.subSet ^TreeSortedSet (:avet db)
                                      (d/datom e0 a v tx0)
                                      (d/datom emax a v txmax))))
                      (av-first-e (:store db) a v)))]
    (vld/validate-datom-unique unique? datom found?)))

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
       (update :tx-data conj (d/datom eid :db/created-at tx-time))

       true
       (update :db-after advance-max-eid eid)))))

(defn- with-datom
  [db datom]
  (let [v            (validate-datom db datom)
        ^Datom datom (coreprep/correct-datom* datom v)
        add          #(do (.add ^TreeSortedSet % datom) %)
        del          #(do (.remove ^TreeSortedSet % datom) %)]
    (if (datom-added datom)
      (-> db
          (update :eavt add)
          (update :avet add)
          (advance-max-eid (.-e datom)))
      (if (.isEmpty
            (.subSet ^TreeSortedSet (:eavt db)
                     (d/datom (.-e datom) (.-a datom) (.-v datom) tx0)
                     (d/datom (.-e datom) (.-a datom) (.-v datom) txmax)))
        db
        (-> db
            (update :eavt del)
            (update :avet del))))))

(defn- queue-tuple
  [queue tuple idx db e _ v]
  (let [tuple-value  (or (get queue tuple)
                         (:v (sf
                               (.subSet ^TreeSortedSet (:eavt db)
                                        (d/datom e tuple nil tx0)
                                        (d/datom e tuple nil txmax))))
                         (ea-first-v (:store db) e tuple)
                         (vec (repeat (-> (schema (:store db))
                                          (get tuple)
                                          :db/tupleAttrs
                                          count)
                                      nil)))
        tuple-value' (assoc tuple-value idx v)]
    (assoc queue tuple tuple-value')))

(defn- queue-tuples
  [queue tuples db e a v]
  (reduce-kv
    (fn [queue tuple idx]
      (queue-tuple queue tuple idx db e a v))
    queue
    tuples))

(defn- transact-report
  [report datom]
  (let [db      (:db-after report)
        a       (:a datom)
        report' (-> report
                    (assoc :db-after (with-datom db datom))
                    (update :tx-data conj datom))]
    (if (txcommon/tuple-source? db a)
      (let [e      (:e datom)
            v      (if (datom-added datom) (:v datom) nil)
            queue  (or (-> report' ::queued-tuples (get e)) {})
            tuples (get (txcommon/attrs-by db :db/attrTuples) a)
            queue' (queue-tuples queue tuples db e a v)]
        (update report' ::queued-tuples assoc e queue'))
      report')))

(defn- pending-attr-state
  [report e a]
  (loop [xs (seq (rseq (vec (:tx-data report))))]
    (if-some [^Datom d (first xs)]
      (if (and (= e (.-e d)) (= a (.-a d)))
        (if (datom-added d) (.-v d) ::retracted-attr-state)
        (recur (next xs)))
      ::missing-attr-state)))

(defn- effective-attr-value
  [report db e a]
  (let [pending (pending-attr-state report e a)]
    (cond
      (identical? pending ::missing-attr-state)
      (ea-first-v (:store db) e a)

      (identical? pending ::retracted-attr-state)
      nil

      :else
      pending)))

(defn- validate-installed-callable-write
  [report db e a v ent]
  (case a
    :db/udf
    (let [descriptor (udf/descriptor v)
          ident      (effective-attr-value report db e :db/ident)]
      (vld/validate-installed-udf-ident ident descriptor ent)
      (when (some? (effective-attr-value report db e :db/fn))
        (u/raise "Installed callable entity cannot have both :db/fn and :db/udf at "
                 ent
                 {:error   :transact/syntax
                  :tx-data ent
                  :db/id   e})))

    :db/fn
    (when (some? (effective-attr-value report db e :db/udf))
      (u/raise "Installed callable entity cannot have both :db/fn and :db/udf at "
               ent
               {:error   :transact/syntax
                :tx-data ent
                :db/id   e}))

    :db/ident
    (when-some [descriptor (effective-attr-value report db e :db/udf)]
      (vld/validate-installed-udf-ident v (udf/descriptor descriptor) ent))

    nil))

(defn- transact-add
  [report [_ e a v tx :as ent]]
  (vld/validate-attr a ent)
  (vld/validate-val v ent)
  (let [tx        (or tx (current-tx report))
        db        (:db-after report)
        store     (:store db)
        report    (if (or (contains? (::new-attributes report) a)
                          ((schema store) a))
                    report
                    (update report ::new-attributes u/conjv a))
        e         (txcommon/entid-strict db e)
        _         (validate-installed-callable-write report db e a v ent)
        v         (if (txcommon/ref? db a)
                    (txcommon/entid-strict db v)
                    v)
        v'        (coreprep/correct-value store a v)
        meta*     (meta ent)
        new-datom (cond-> (datom e a v' tx)
                    meta* (with-meta meta*))
        multival? (txcommon/multival? db a)
        ^Datom old-datom
        (if multival?
          (or (sf (.subSet ^TreeSortedSet (:eavt db)
                           (datom e a v' tx0)
                           (datom e a v' txmax)))
              (first (fetch (:store db) (datom e a v'))))
          (or (sf (.subSet ^TreeSortedSet (:eavt db)
                           (datom e a nil tx0)
                           (datom e a nil txmax)))
              (ea-first-datom (:store db) e a)))]
    (cond
      (nil? old-datom)
      (transact-report report new-datom)

      (= (.-v old-datom) v')
      (if (some #(and (not (datom-added %)) (= % new-datom))
                (:tx-data report))
        (transact-report report new-datom)
        (update report ::tx-redundant conjv new-datom))

      :else
      (let [report' (transact-report report
                                     (datom e a (.-v old-datom) tx false))]
        (transact-report report' new-datom)))))

(defn- transact-retract-datom [report ^Datom d]
  (let [tx (current-tx report)]
    (transact-report report (datom (.-e d) (.-a d) (.-v d) tx false))))

(defn- retract-components [db datoms]
  (into #{} (comp
              (filter (fn [^Datom d] (txcommon/component? db (.-a d))))
              (map (fn [^Datom d] [:db.fn/retractEntity (.-v d)]))) datoms))

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
            (dissoc ::value-tempids ::tx-redundant)
            (assoc :tx-data tx-data)))
      (-> report
          (dissoc ::value-tempids ::tx-redundant)
          (assoc :tx-data tx-data)))))

(declare local-transact-tx-data)

(defn- retry-with-tempid
  [initial-report report es tempid upserted-eid tx-time]
  (let [eid (get (::upserted-tempids initial-report) tempid)]
    (vld/validate-upsert-retry-conflict eid tempid upserted-eid)
    (let [tempids' (-> (:tempids report)
                       (assoc tempid upserted-eid))
          report'  (-> initial-report
                       (assoc :tempids tempids')
                       (update ::upserted-tempids assoc tempid upserted-eid))]
      (local-transact-tx-data report' es tx-time))))

(defn- flush-tuples [report]
  (let [db    (:db-after report)
        store (:store db)]
    (reduce-kv
      (fn [entities eid tuples+values]
        (persistent!
          (reduce-kv
            (fn [entities tuple value]
              (let [value   (if (every? nil? value) nil value)
                    current (or (:v (sf (.subSet ^TreeSortedSet (:eavt db)
                                                 (d/datom eid tuple nil tx0)
                                                 (d/datom eid tuple nil txmax))))
                                (ea-first-v store eid tuple))]
                (cond
                  (= value current) entities
                  (nil? value)
                  (conj! entities ^::internal [:db/retract eid tuple current])
                  :else
                  (conj! entities ^::internal [:db/add eid tuple value]))))
            (transient entities) tuples+values)))
      [] (::queued-tuples report))))

(defn- finalize-report
  [report]
  (let [new-attrs (::new-attributes report)]
    (cond-> (-> report
                check-value-tempids
                (dissoc ::upserted-tempids)
                (dissoc ::reverse-tempids)
                (dissoc ::new-attributes)
                (update :tempids #(u/removem txprep/auto-tempid? %))
                (update :tempids assoc :db/current-tx (current-tx report))
                (update :db-after update :max-tx u/long-inc))
      (seq new-attrs) (assoc :new-attributes new-attrs))))

(defn- handle-flush-tuples
  [report entities]
  (if (contains? report ::queued-tuples)
    [(dissoc report ::queued-tuples)
     (concat (flush-tuples report) entities)]
    [report entities]))

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
        datoms        (concat
                        (.subSet ^TreeSortedSet (:eavt db)
                                 (datom e a nil tx0)
                                 (datom e a nil txmax))
                        (slice (:store db) :eav
                               (datom e a c/v0)
                               (datom e a c/vmax)))]
    (vld/validate-cas-value (txcommon/multival? db a) e a ov nv datoms)
    [(transact-add report [:db/add e a nv]) entities]))

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
          (let [old-v'    (coreprep/correct-value store a old-v)
                old-datom (or (sf (.subSet ^TreeSortedSet (:eavt db)
                                           (datom e a old-v' tx0)
                                           (datom e a old-v' txmax)))
                              (first (fetch (:store db) (datom e a old-v'))))]
            (vld/validate-patch-idoc-old-value old-datom old-v a)
            (let [old-doc             (.-v ^Datom old-datom)
                  {:keys [doc paths]} (idoc/apply-patch old-doc ops)]
              (if (= old-doc doc)
                [report entities]
                (let [ent (with-meta [:db/add e a doc]
                            {:idoc/patch {:paths paths}})]
                  [(transact-add report ent)
                   (cons [:db/retract e a old-doc] entities)]))))
          (let [old-datom           (or (sf (.subSet ^TreeSortedSet (:eavt db)
                                                      (datom e a nil tx0)
                                                      (datom e a nil txmax)))
                                        (ea-first-datom store e a))
                old-doc             (when old-datom (.-v ^Datom old-datom))
                {:keys [doc paths]} (idoc/apply-patch (or old-doc {}) ops)
                ent                 (with-meta [:db/add e a doc]
                                      {:idoc/patch {:paths paths}})]
            [(transact-add report ent) entities]))))))

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
  [initial-report report db store tempids entity entities initial-es tx-time]
  (let [[op e a v] entity
        upserted-eid  (when (txcommon/is-attr? db a :db.unique/identity)
                        (or (:e (sf (.subSet
                                      ^TreeSortedSet (:avet db)
                                      (d/datom e0 a v tx0)
                                      (d/datom emax a v txmax))))
                            (av-first-e store a v)))
        allocated-eid (get tempids e)]
    (if (and upserted-eid allocated-eid (not= upserted-eid allocated-eid))
      (retry-with-tempid initial-report report initial-es e upserted-eid
                         tx-time)
      (let [eid (or upserted-eid allocated-eid (next-eid db))]
        [(allocate-eid tx-time report e eid)
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
  [db store schema entity report entities]
  (let [[_ e a v] entity
        tuple-attrs (get-in schema [a :db/tupleAttrs])]
    (vld/validate-tuple-direct-write
      (and
        (every? some? v)
        (= (count tuple-attrs) (count v))
        (every?
          (fn [[tuple-attr tuple-value]]
            (let [db-value
                  (or (:v (sf
                            (.subSet
                              ^TreeSortedSet (:eavt db)
                              (d/datom e tuple-attr nil tx0)
                              (d/datom e tuple-attr nil txmax))))
                      (ea-first-v store e tuple-attr))]
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
        (if-some [old-datom (or (sf (.subSet
                                      ^TreeSortedSet (:eavt db)
                                      (datom e a v tx0)
                                      (datom e a v txmax)))
                                (first (fetch (:store db) (datom e a v))))]
          [(transact-retract-datom report old-datom) entities]
          [report entities]))
      [report entities])))

(defn- handle-retract-attribute
  [report db store entity entities]
  (let [[_ e a _] entity]
    (if-some [e (txcommon/entid db e)]
      (let [_      (vld/validate-attr a entity)
            datoms (concatv
                     (slice (:store db) :eav
                            (datom e a c/v0)
                            (datom e a c/vmax))
                     (.subSet ^TreeSortedSet (:eavt db)
                              (datom e a nil tx0)
                              (datom e a nil txmax)))]
        [(reduce transact-retract-datom report datoms)
         (concat (retract-components db datoms) entities)])
      [report entities])))

(defn- handle-retract-entity
  [report db store entity entities]
  (let [[_ e _ _] entity]
    (if-some [e (txcommon/entid db e)]
      (let [e-datoms (concatv
                       (e-datoms (:store db) e)
                       (.subSet ^TreeSortedSet (:eavt db)
                                (datom e nil nil tx0)
                                (datom e nil nil txmax)))
            v-datoms (v-datoms (:store db) e)]
        [(reduce transact-retract-datom
                 report
                 (concat e-datoms v-datoms))
         (concat (retract-components db e-datoms) entities)])
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

      (or (number? old-eid)
          (nil? old-eid)
          (string? old-eid)
          (txprep/auto-tempid? old-eid))
      [report (concat (txprep/explode db entity) entities)]

      :else
      (vld/validate-map-entity-id-syntax old-eid))))

(defn- handle-sequential-entity
  [initial-report report db store schema tempids entity entities initial-es
   tx-time]
  (let [[op e a v] entity]
    (cond+
      (identical? op :db.fn/call)
      [report (concat (txprep/handle-fn-call db entity) entities)]

      (and (keyword? op) (not (txprep/builtin-fn? op)))
      [report (txprep/handle-custom-tx-fn db store entity entities)]

      (and (txprep/tempid? e) (not (identical? op :db/add)))
      (vld/validate-tempid-op true op entity)

      (or (identical? op :db.fn/cas) (identical? op :db/cas))
      (handle-cas report db store entity entities)

      (identical? op :db.fn/patchIdoc)
      (handle-patch-idoc report db store schema entity entities)

      (tx-id? e)
      [(allocate-eid tx-time report e (current-tx report))
       (cons [op (current-tx report) a v] entities)]

      (and (txcommon/ref? db a) (tx-id? v))
      [(allocate-eid tx-time report v (current-tx report))
       (cons [op e a (current-tx report)] entities)]

      (and (txcommon/ref? db a) (txprep/tempid? v))
      (handle-ref-tempid-value report db tempids entity entities
                               (cons entity entities) tx-time)

      (and (txcommon/ref? db a) (sequential? v))
      (let [resolved (txcommon/entid-strict db v)]
        [report (cons [op e a resolved] entities)])

      (txprep/tempid? e)
      (handle-tempid-entity initial-report report db store tempids entity
                            entities initial-es tx-time)

      :let [upserted-eid (when (and (txcommon/is-attr? db a :db.unique/identity)
                                    (contains? (::reverse-tempids report) e)
                                    e)
                           (av-first-e store a v))]

      (and upserted-eid (not= e upserted-eid))
      (handle-reverse-tempid-upsert initial-report report entity entities
                                    initial-es tx-time upserted-eid)

      (and (txcommon/tuple-attr? db a) (not (::internal (meta entity))))
      (handle-tuple-attr db store schema entity report entities)

      :let [tuple-types (when (and (or (txcommon/tuple-type? db a)
                                       (txcommon/tuple-types? db a))
                                   (not (::internal (meta entity))))
                           (or (get-in schema [a :db/tupleTypes])
                               (repeat (get-in schema [a :db/tupleType]))))
            vs (when tuple-types
                 (partition 2 (interleave tuple-types v)))]

      (some #(and (identical? (first %) :db.type/ref)
                  (txprep/tempid? (second %))) vs)
      (handle-tuple-ref-tempids report db tempids entity entities vs tx-time)

      (some #(and (identical? (first %) :db.type/ref)
                  (keyword? (second %))) vs)
      [report (cons (handle-tuple-ref-idents db entity vs) entities)]

      (identical? op :db/add)
      [(transact-add report entity) entities]

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

(defn execute-tx-loop
  [initial-report initial-es tx-time]
  (let [initial-report' (update initial-report :db-after clear-tx-cache)
        db              (:db-before initial-report)
        initial-es'     (if (seq (txcommon/attrs-by db :db.type/tuple))
                          (sequence
                            (mapcat vector)
                            initial-es (repeat ::flush-tuples))
                          initial-es)
        store           (:store db)
        schema          (schema store)]
    (loop [report initial-report'
           es     initial-es']
      (cond+
        (empty? es)
        (finalize-report report)

        :let [[entity & entities] es]

        (identical? ::flush-tuples entity)
        (let [[r' es'] (handle-flush-tuples report entities)]
          (recur r' es'))

        :let [db      (:db-after report)
              tempids (:tempids report)]

        (map? entity)
        (let [result (handle-map-entity initial-report report db entity
                                        entities initial-es tx-time)]
          (if (map? result)
            result
            (let [[r' es'] result]
              (recur r' es'))))

        (sequential? entity)
        (let [result (handle-sequential-entity
                       initial-report report db store schema tempids
                       entity entities initial-es tx-time)]
          (if (map? result)
            result
            (let [[r' es'] result]
              (recur r' es'))))

        (datom? entity)
        (let [[e a v tx added] entity]
          (if added
            (recur (transact-add report [:db/add e a v tx]) entities)
            (recur report (cons [:db/retract e a v] entities))))

        (nil? entity)
        (recur report entities)

        :else
        (vld/validate-tx-entity-type entity)))))

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
      (cond-> (assoc initial-report
                     :db-after (:db-after ptx)
                     :tx-data (:tx-data ptx)
                     :tempids (:tempids ptx))
        (seq (:new-attributes ptx))
        (assoc :new-attributes (:new-attributes ptx))))
    (execute-tx-loop initial-report initial-es tx-time)))
