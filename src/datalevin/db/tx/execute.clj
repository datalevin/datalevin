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
   [datalevin.interface :as i :refer [fetch init-max-eid opts]]
   [datalevin.prepare :as coreprep]
   [datalevin.udf :as udf]
   [datalevin.util :as u :refer [ conjv conjs concatv cond+ raise]]
   [datalevin.validate :as vld])
  (:import
   [datalevin.datom Datom]
   [datalevin.db.tx.prepare IdentityResolution ResolvedIdentityAdd]
   [java.util ArrayList Collection HashMap List SortedSet]
   [org.eclipse.collections.impl.set.sorted.mutable TreeSortedSet]))

(deftype ^:private TxStep [report entities])
(deftype ^:private TxRetry [report])

(deftype ^:private ResolvedTuple [^long e a value before])
(deftype ^:private ResolvedIdentityMap [entity resolution])

(def ^:private resolution-value-classes
  ;; No host conversion, predicates or nested calls may change the native
  ;; transaction while a flat plan carries its stored identity results.
  #{String Long Integer Short Byte Double Float Boolean
    clojure.lang.Keyword clojure.lang.Symbol clojure.lang.BigInt
    clojure.lang.Ratio java.math.BigInteger java.math.BigDecimal
    java.util.UUID java.util.Date java.sql.Date java.sql.Timestamp
    java.time.Instant (class (byte-array 0))})

(def ^:private resolution-scalar-types
  #{nil :db.type/string :db.type/long :db.type/keyword :db.type/symbol
    :db.type/uuid :db.type/instant :db.type/boolean :db.type/float
    :db.type/double :db.type/bigint :db.type/bigdec :db.type/bytes})


(defn- refresh-context
  [report context]
  (when-let [ctx @context]
    (let [store (:store (:db-after report))]
      (when-not (and (identical? (:schema ctx) (i/schema store))
                     (identical? (:opts ctx) (opts store)))
        (vreset! context (coreprep/make-prepare-ctx (:db-after report))))))
  report)

(defn- after-value-callbacks
  "Ordinary plans exclude callbacks. Other additions refresh schema decisions
  after predicates or host coercion before publishing their datoms."
  [report props value context]
  (if (or (::reference-free? report)
          (and (not (:db.attr/preds props))
               (or (contains? resolution-scalar-types (:db/valueType props))
                   (identical? :db.type/ref (:db/valueType props)))
               (contains? resolution-value-classes (class value))))
    report
    (refresh-context report context)))

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
  [db ^Datom datom resolution props]
  (when (and (datom-added datom) (:db/unique props))
    (let [a (.-a datom)
          v (.-v datom)]
      (vld/validate-datom-unique
        true
        datom
        (fn []
          (if resolution
            (txprep/identity-owner db resolution)
            (or (when (txcommon/cached-av-first-e db a v) true)
                (txcommon/av-first-e db a v))))))))

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
  [report ^Datom datom]
  (.add ^ArrayList (:tx-data report) datom)
  (when-let [pending (::pending-attr-index report)]
    (when-let [^HashMap index @pending]
      (.put index [(.-e datom) (.-a datom)] datom)))
  report)

(defn- allocate-eid
  ([_ report eid _context]
   (update report :db-after advance-max-eid eid))
  ([tx-time report e eid context]
   (let [db   (:db-after report)
         new? (new-eid? db eid)
         db-opts (:opts @context)]
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
  [db ^Datom datom resolution props]
  (validate-datom db datom resolution props)
  (if txcommon/*batch-prepare*
    (do (txcommon/stage-batch-datom! db datom props)
        (if (datom-added datom) (advance-max-eid db (.-e datom)) db))
    (if (datom-added datom)
      (do
        (.add ^TreeSortedSet (:eavt db) datom)
        (when-not (:db/noindex props)
          (.add ^TreeSortedSet (:avet db) datom))
        (advance-max-eid db (.-e datom)))
      (if (.isEmpty
            (.subSet ^TreeSortedSet (:eavt db)
                     (d/datom (.-e datom) (.-a datom) (.-v datom) tx0)
                     (d/datom (.-e datom) (.-a datom) (.-v datom) txmax)))
        db
        (do
          (.remove ^TreeSortedSet (:eavt db) datom)
          (when-not (:db/noindex props)
            (.remove ^TreeSortedSet (:avet db) datom))
          db)))))

(declare effective-attr-value effective-attr-preparation)

(defn- queue-tuple
  [queue tuple idx report db e _ v context]
  (let [^ResolvedTuple queued (get queue tuple)
        before (if queued
                 (.-before queued)
                 (effective-attr-preparation report db e tuple))
        value (or (when queued (.-value queued))
                  (idx/logical-value before)
                  (vec (repeat (-> @context :schema
                                   (get tuple)
                                   :db/tupleAttrs
                                   count)
                               nil)))]
    (assoc queue tuple (ResolvedTuple. e tuple (assoc value idx v) before))))

(defn- queue-tuples
  [queue tuples report db e a v context]
  (reduce-kv
    (fn [queue tuple idx]
      (queue-tuple queue tuple idx report db e a v context))
    queue
    tuples))

(defn- track-retraction
  [report datom]
  (when-let [retracted (and (not (datom-added datom)) (::tx-retracted report))]
    (conj! retracted datom))
  report)

(defn- transact-report
  ([report datom context] (transact-report report datom nil context))
  ([report ^Datom datom resolution context]
   (transact-report report datom resolution
                    (get (:schema @context) (.-a datom)) context))
  ([report ^Datom datom resolution props context]
   (let [db      (:db-after report)
         rschema (:rschema @context)
         a       (.-a datom)
         db'     (with-datom db datom resolution props)
         report' (-> report
                     (cond-> (not (identical? db db')) (assoc :db-after db'))
                     (append-tx-datom! datom)
                     (track-retraction datom))]
     (if-let [tuples (when-let [sources (:db/attrTuples rschema)]
                       (get sources a))]
       (let [e      (.-e datom)
             v      (if (datom-added datom) (.-v datom) nil)
             queue  (or (-> report' ::queued-tuples (get e)) {})
             queue' (queue-tuples queue tuples report' db e a v context)]
         (update report' ::queued-tuples assoc e queue'))
       report'))))

(defn- transact-same-value
  ([report datom resolution]
   (transact-same-value report datom resolution (volatile! nil)))
  ([report ^Datom datom resolution context]
   (transact-same-value report datom resolution
                        (get (:schema @context) (.-a datom)) context))
  ([report datom resolution props context]
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
       (transact-report report datom resolution props context)
       ;; Only reference tempid validation consumes redundant additions. The
       ;; ordinary-addition plan proves that no later input can introduce one.
       (if (::reference-free? report)
         report
         (update report ::tx-redundant conjv datom))))))

(defn- apply-add
  "Apply a normalized value against its known previous value. Ordinary input,
  CAS and derived tuples share datom publication and constraint maintenance."
  ([report ^Datom new-datom old-value context]
   (apply-add report new-datom old-value nil
              (get (:schema @context) (.-a new-datom)) context))
  ([report ^Datom new-datom old-value resolution props context]
   (cond
     (nil? old-value)
     (transact-report report new-datom resolution props context)

     (d/value-equal? (idx/logical-value old-value) (.-v new-datom))
     (transact-same-value report new-datom resolution props context)

     :else
     (-> report
         (transact-report (idx/retraction (.-e new-datom) (.-a new-datom) old-value
                                         (d/datom-tx new-datom)) nil props context)
         (transact-report new-datom resolution props context)))))

(defn- pending-attr-state
  [report e a]
  (if-let [pending (::pending-attr-index report)]
    (let [^HashMap index
          (or @pending
              ;; Seed the complete prefix once. A miss then proves that this
              ;; request has no pending E/A state; it must not rescan history.
              (let [index (HashMap.)]
                (doseq [^Datom datom (:tx-data report)]
                  (.put index [(.-e datom) (.-a datom)] datom))
                (vreset! pending index)
                index))]
      (if-let [^Datom datom (.get index [(long e) a])]
        (if (datom-added datom) (.-v datom) ::retracted-attr-state)
        ::missing-attr-state))
    (let [^List datoms (:tx-data report)]
      (loop [i (dec (.size datoms))]
        (if (neg? i)
          ::missing-attr-state
          (let [^Datom d (.get datoms i)]
            (if (and (= e (.-e d)) (= a (.-a d)))
              (if (datom-added d) (.-v d) ::retracted-attr-state)
              (recur (dec i)))))))))

(defn- effective-attr-preparation
  [report db e a]
  (if (> (long e) ^long (::max-eid-before report))
    ;; New entities have no stored values to mask. The existing EAV overlay
    ;; already reflects their additions and retractions; avoid scanning all
    ;; preceding transaction datoms each time a new tuple is queued.
    (when-let [^Datom d (cached-ea-first-datom db e a)] (.-v d))
    (let [pending (pending-attr-state report e a)]
      (cond
        (identical? pending ::missing-attr-state)
        (txcommon/preparation-first-v db e a)

        (identical? pending ::retracted-attr-state)
        nil

        :else
        pending))))

(defn- effective-attr-value
  [report db e a]
  (idx/logical-value (effective-attr-preparation report db e a)))

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
  ([report new-datom props context] (transact-resolved-add report new-datom props nil context))
  ([report ^Datom new-datom props resolution context]
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
     (apply-add report new-datom (idx/datom-prepared-value old-datom) resolution props context))))

(defn- transact-add
  ([report [_ _ a _ _ :as ent] context]
   (let [ctx @context]
     (transact-add report ent (get (:schema ctx) a) (:opts ctx) context)))
  ([report [_ e a v tx :as ent] props store-opts context]
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
     (let [before @context
           report (after-value-callbacks report props v context)
           props (if (identical? before @context)
                   props (get (:schema @context) a))
           meta* (meta ent)
           new-datom (cond-> (datom e a value (or tx (current-tx report)))
                       meta* (with-meta meta*))]
       (transact-resolved-add report new-datom props context)))))

(defn- transact-retract-datom [report ^Datom d context]
  (let [tx (current-tx report)]
    (transact-report report
                     (idx/retraction (.-e d) (.-a d)
                                     (idx/datom-prepared-value d) tx) context)))

(defn- transact-known-old-add
  "Validate and apply an addition whose previous cardinality-one value is
  already known. CAS and document patches retain metadata and predicates."
  [report db _store entity e a nv ^Datom old-datom context]
  (vld/validate-val nv entity)
  (let [tx        (current-tx report)
        _         (validate-installed-callable-write report db e a nv entity)
        props     (get (:schema @context) a)
        nv'       (coreprep/correct-value-with-props
                    (:opts @context) props a nv)
        _         (when (:db.attr/preds props)
                    (txcommon/flush-batch-prepare!)
                    (vld/validate-attr-preds
                      e a nv' props #(txprep/resolve-attr-pred-udf db %)))
        report    (after-value-callbacks report props nv context)
        meta*     (meta entity)
        new-datom (cond-> (datom e a nv' tx)
                    meta* (with-meta meta*))]
    (apply-add report new-datom (idx/datom-prepared-value old-datom) context)))

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
                (if (= value (idx/logical-value current))
                  entities
                  (conj! entities
                    (if resolved?
                      (if (nil? value)
                        (ResolvedTuple. eid tuple nil current)
                        change)
                      (if (nil? value)
                        ^::internal [:db/retract eid tuple (idx/logical-value current)]
                        ^::internal [:db/add eid tuple value]))))))
            entities tuples+values))
        (transient []) queued))))

(defn- transact-resolved-tuple
  [report schema store-opts ^ResolvedTuple change context]
  (let [e (.-e change) a (.-a change) value (.-value change)
        tx (current-tx report)]
    (if (nil? value)
      (transact-report report (idx/retraction e a (.-before change) tx) context)
      (let [value (coreprep/correct-value-with-props store-opts (schema a) a value)
            new-datom (with-meta (datom e a value tx) {::internal true})]
        (apply-add report new-datom (.-before change) context)))))

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
                      (dissoc-present ::reference-free?)
                      (dissoc-present ::pending-attr-index)
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
  [report db store entity entities context]
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
       (transact-add report [:db/add e a nv] context)
       (transact-known-old-add
         report db store entity e a nv (first datoms) context))
     entities]))

(defn- handle-patch-idoc
  [report db store schema entity entities context]
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
                  [(transact-add report ent context)
                   (cons [:db/retract e a old-doc] entities)]))))
          (let [old-datom           (or (cached-ea-first-datom db e a)
                                        (txcommon/ea-first-datom db e a))
                old-doc             (when old-datom (.-v ^Datom old-datom))
                {:keys [doc paths]} (idoc/apply-patch (or old-doc {}) ops)
                ent                 (with-meta [:db/add e a doc]
                                      {:idoc/patch {:paths paths}})]
            [(transact-known-old-add report db store ent e a doc old-datom context)
             entities]))))))

(defn- handle-ensure
  [report entity entities]
  (vld/validate-ensure-arity (count entity) entity)
  (let [[_ pred & args] entity]
    (vld/validate-ensure-predicate pred entity)
    [(update report ::ensures conjv [pred args entity]) entities]))

(defn- handle-ref-tempid-value
  [report db tempids entity entities es tx-time context]
  (let [[op e a v] entity]
    (if-some [resolved (get tempids v)]
      [(update report ::value-tempids assoc resolved v)
       (cons [op e a resolved] entities)]
      (let [resolved (next-eid db)]
        [(-> (allocate-eid tx-time report v resolved context)
             (update ::value-tempids assoc resolved v))
         es]))))

(defn- handle-tempid-entity
  [initial-report report db _store tempids unique-identity? entity entities
   initial-es tx-time context]
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
           (allocate-eid tx-time report e eid context))
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
  [report _db _tempids entity entities vs tx-time context]
  (let [[op e a _] entity
        [report' v']
        (loop [report' report
               vs      vs
               v'      []]
          (if-let [[[tuple-type v] & vs] vs]
            (if (and (identical? tuple-type :db.type/ref)
                     (txprep/tempid? v))
              ;; Each component must see allocations from earlier components.
              (if-some [resolved (get (:tempids report') v)]
                (recur report' vs (conj v' resolved))
                (let [resolved (next-eid (:db-after report'))
                      report'  (-> (allocate-eid tx-time report' v resolved context)
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
  [report db store entity entities context]
  (let [[_ e a v] entity]
    (if-some [e (txcommon/entid db e)]
      (let [v (if (txcommon/ref? db a)
                (txcommon/entid-strict db v)
                v)]
        (vld/validate-attr a entity)
        (vld/validate-val v entity)
        (if-some [old-datom (or (cached-eav-first-datom db e a v)
                                (first (txcommon/visible-stored db (fetch store (datom e a v)))))]
          [(transact-retract-datom report old-datom context) entities]
          [report entities]))
      [report entities])))

(defn- handle-retract-attribute
  [report db _store entity entities context]
  (let [[_ e a _] entity]
    (if-some [e (txcommon/entid db e)]
      (let [_       (vld/validate-attr a entity)
            stored  (txcommon/ea-datoms db e a)
            cached  (when-not txcommon/*batch-prepare*
                      (vec (.subSet ^TreeSortedSet (:eavt db)
                                    (datom e a nil tx0) (datom e a nil txmax))))
            report' (reduce #(transact-retract-datom %1 %2 context) report stored)
            report' (reduce #(transact-retract-datom %1 %2 context) report' cached)
            components (retract-components db stored)
            components (retract-components db components cached)]
        [report' (if (seq components)
                   (concat components entities)
                   entities)])
      [report entities])))

(defn- handle-retract-entity
  [report db _store entity entities context]
  (let [[_ e _ _] entity]
    (if-some [e (txcommon/entid db e)]
      (let [stored-e-datoms (vec (txcommon/e-datoms db e))
            cached-e-datoms (when-not txcommon/*batch-prepare*
                              (vec (.subSet ^TreeSortedSet (:eavt db)
                                            (datom e nil nil tx0) (datom e nil nil txmax))))
            v-datoms        (vec (txcommon/v-datoms db e))
            report'         (reduce #(transact-retract-datom %1 %2 context) report stored-e-datoms)
            report'         (reduce #(transact-retract-datom %1 %2 context) report' cached-e-datoms)
            report'         (reduce #(transact-retract-datom %1 %2 context) report' v-datoms)
            components      (retract-components db stored-e-datoms)
            components      (retract-components db components cached-e-datoms)]
        [report' (if (seq components)
                   (concat components entities)
                   entities)])
      [report entities])))

(defn- handle-map-entity
  ([initial-report report db entity entities initial-es tx-time resolution]
   (handle-map-entity initial-report report db entity entities initial-es tx-time
                      resolution (volatile! (coreprep/make-prepare-ctx db))))
  ([initial-report report db entity entities initial-es tx-time resolution context]
   (let [old-eid (:db/id entity)
         ctx (or @context (coreprep/make-prepare-ctx db))]
     (vld/validate-installed-callable-entity entity)
     (cond+
       (tx-id? old-eid)
       (let [id (current-tx report)]
         [(allocate-eid tx-time report old-eid id context)
          (cons (assoc entity :db/id id) entities)])

       (sequential? old-eid)
       (let [id (txcommon/entid-strict db old-eid)]
         [report (cons (assoc entity :db/id id) entities)])

       :let [[entity' resolution-result]
             (if resolution
               (txprep/resolve-identity-map db entity resolution)
               (txprep/resolve-upserts db entity ctx))
             upserted-eid (if resolution resolution-result
                             (txprep/validate-upserts entity' resolution-result))
             report (if resolution report (refresh-context report context))
             ctx (or @context ctx)]

       (some? upserted-eid)
       (let [tempids (:tempids report)]
         (if (and (txprep/tempid? old-eid)
                  (contains? tempids old-eid)
                  (not= upserted-eid (get tempids old-eid)))
           (retry-with-tempid initial-report report initial-es old-eid
                              upserted-eid tx-time)
           [(-> (allocate-eid tx-time report old-eid upserted-eid context)
                (update ::tx-redundant conjv
                        (datom upserted-eid nil nil tx0)))
            (concat (txprep/explode db
                                    (assoc entity' :db/id upserted-eid) resolution ctx)
                    entities)]))

       (txprep/tempid? old-eid)
       (if (txprep/contains-nested-map? db entity resolution ctx)
         [report (concat (txprep/explode db entity nil ctx) entities)]
         (let [allocated-eid (get (:tempids report) old-eid)
               eid           (or allocated-eid (next-eid db))
               txs           (txprep/explode db (assoc entity :db/id eid) resolution ctx)]
           (if (.isEmpty ^List txs)
             [report entities]
             [(if allocated-eid
                report
                (allocate-eid tx-time report old-eid eid context))
              (concat txs entities)])))

       (or (number? old-eid)
           (nil? old-eid)
           (string? old-eid)
           (txprep/auto-tempid? old-eid))
       [report (concat (txprep/explode db entity nil ctx) entities)]

       :else
       (vld/validate-map-entity-id-syntax old-eid)))))

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
  [report entity tx-time context]
  (if (= 1 (count entity))
    report
    (let [old-eid (:db/id entity)
          temporary? (txprep/tempid? old-eid)
          allocated (when temporary? (get (:tempids report) old-eid))
          eid (if temporary? (or allocated (next-eid (:db-after report))) old-eid)
          report (if (and temporary? (nil? allocated))
                   (allocate-eid tx-time report old-eid eid context)
                   report)]
      (reduce-kv
        (fn [report a v]
          (if (identical? a :db/id)
            report
            (let [ctx @context
                  props (get (:schema ctx) a)]
              ;; The map gate checked attribute syntax and resolved references.
              ;; Materialize an operation only for a value error's context.
              (when (nil? v) (vld/validate-val v [:db/add eid a v]))
              (let [value (coreprep/correct-value-with-props (:opts ctx) props a v)
                    before @context
                    report (after-value-callbacks report props v context)]
                (transact-resolved-add
                  report (datom eid a value (current-tx report))
                  (if (identical? before @context)
                    props (get (:schema @context) a)) context)))))
        report entity))))

(defn- handle-sequential-entity
  [initial-report report db store schema store-opts tempids entity entities
   initial-es tx-time context]
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
      (let [result (txprep/handle-fn-call db entity)
            report (refresh-context report context)]
        [report (concat (txprep/prepare-tx-fn-result
                          db result tx-time @context) entities)])

      (identical? op :db/ensure)
      (handle-ensure report entity entities)

      (and (keyword? op) (not (txprep/builtin-fn? op)))
      (let [result (txprep/handle-custom-tx-fn db store entity)
            report (refresh-context report context)]
        [report (concat (txprep/prepare-tx-fn-result
                          db result tx-time @context) entities)])

      (and (txprep/tempid? e) (not (identical? op :db/add)))
      (vld/validate-tempid-op true op entity)

      (or (identical? op :db.fn/cas) (identical? op :db/cas))
      (handle-cas report db store entity entities context)

      (identical? op :db.fn/patchIdoc)
      (handle-patch-idoc report db store schema entity entities context)

      (tx-id? e)
      [(allocate-eid tx-time report e (current-tx report) context)
       (cons [op (current-tx report) a v] entities)]

      (and ref? (tx-id? v))
      [(allocate-eid tx-time report v (current-tx report) context)
       (cons [op e a (current-tx report)] entities)]

      (and ref? (txprep/tempid? v))
      (handle-ref-tempid-value report db tempids entity entities
                               (cons entity entities) tx-time context)

      (and ref? (sequential? v))
      (let [resolved (txcommon/entid-strict db v)]
        [report (cons [op e a resolved] entities)])

      (txprep/tempid? e)
      (handle-tempid-entity initial-report report db store tempids
                            unique-identity? entity entities initial-es
                            tx-time context)

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
      (handle-tuple-ref-tempids report db tempids entity entities vs tx-time context)

      (some #(and (identical? (first %) :db.type/ref)
                  (keyword? (second %))) vs)
      [report (cons (handle-tuple-ref-idents db entity vs) entities)]

      (identical? op :db/add)
      (TxStep. (transact-add report entity props store-opts context) entities)

      (and (identical? op :db/retract) (some? v))
      (handle-retract-value report db store entity entities context)

      (or (identical? op :db.fn/retractAttribute)
          (identical? op :db/retract))
      (handle-retract-attribute report db store entity entities context)

      (or (identical? op :db.fn/retractEntity)
          (identical? op :db/retractEntity))
      (handle-retract-entity report db store entity entities context)

      :else
      (vld/validate-tx-op op entity))))

(defn- transact-resolved-identity
  [initial-report report ^ResolvedIdentityAdd entity entities initial-es tx-time context]
  (let [e (.-e entity)
        ^IdentityResolution resolution (.-resolution entity)
        a (.-attr resolution)
        v (.-value resolution)
        db (:db-after report)
        owner (when (contains? (::reverse-tempids report) e)
                (txprep/identity-owner db resolution))]
    (if (and owner (not= e owner))
      (handle-reverse-tempid-upsert initial-report report [:db/add e a v]
                                    entities initial-es tx-time owner)
      (transact-resolved-add report (datom e a v (current-tx report))
                             (get (:schema @context) a) resolution context))))

(defn- execute-tx-loop*
  [initial-report initial-es tx-time ordinary? context]
  (let [ctx             @context
        db              (:db-before initial-report)
        store           (:store db)
        ;; The owning writer supplies its current maximum. Preserve the
        ;; attempt boundary on the initial report so upsert retries reuse it.
        max-eid-before  (max (long (:max-eid db))
                             (long (or (::max-eid-before initial-report)
                                       (init-max-eid store))))
        tuples?         (seq (:db.type/tuple (:rschema ctx)))
        initial-report' (-> initial-report
                            (dissoc-present ::tx-retracted)
                            (dissoc-present ::pending-attr-index)
                            ;; Each retry owns a fresh append buffer. Complex
                            ;; handlers and regular adds share it, and only the
                            ;; finalized report exposes an immutable vector.
                            (assoc :tx-data
                                   (doto (ArrayList. 32)
                                     (.addAll ^Collection (:tx-data initial-report))))
                            (assoc ::max-eid-before max-eid-before)
                            (cond-> tuples?
                              (assoc ::pending-attr-index (volatile! nil)))
                            (cond-> ordinary? (assoc ::reference-free? true))
                            (update :db-after
                                    #(-> % clear-tx-cache
                                         (advance-max-eid max-eid-before))))]
    (loop [report    initial-report'
           es        (when-not tuples? initial-es)
           remaining (when tuples? (seq initial-es))]
      (let [ctx @context
            schema (:schema ctx)
            store-opts (:opts ctx)]
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
          (recur (transact-resolved-tuple report schema store-opts entity context)
                 entities remaining)

          (map? entity)
          (if (or ordinary? (regular-map? schema entity))
            (recur (transact-regular-map report entity tx-time context)
                   entities remaining)
            (let [result (handle-map-entity initial-report report db entity
                                            entities initial-es tx-time nil context)]
              (if (or (instance? TxRetry result) (map? result))
                result
                (let [[r' es'] result]
                  (recur r' es' remaining)))))

          (sequential? entity)
          (let [[op e a] entity
                props (schema a)]
            (if (or ordinary?
                    (and (identical? op :db/add)
                         (integer? e) (not (neg? ^long e))
                         (regular-attr? props)))
              (recur (transact-add report entity props store-opts context) entities remaining)
              (let [result (handle-sequential-entity
                             initial-report report db store schema store-opts
                             tempids entity entities initial-es tx-time context)]
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
              (recur (transact-add report [:db/add e a v tx] context) entities remaining)
              (recur report (cons [:db/retract e a v] entities) remaining)))

          (nil? entity)
          (recur report entities remaining)

          (instance? ResolvedIdentityMap entity)
          (let [result (handle-map-entity
                         initial-report report db (.-entity ^ResolvedIdentityMap entity)
                         entities initial-es tx-time
                         (.-resolution ^ResolvedIdentityMap entity) context)]
            (if (or (instance? TxRetry result) (map? result))
              result
              (let [[r' es'] result]
                (recur r' es' remaining))))

          (instance? ResolvedIdentityAdd entity)
          (let [result (transact-resolved-identity initial-report report entity
                                                  entities initial-es tx-time context)]
            (if (instance? TxRetry result)
              result
              (recur result entities remaining)))

          :else
          (vld/validate-tx-entity-type entity))))))

(defn- ordinary-value?
  [schema a value]
  (let [props (schema a)]
    (and (keyword? a)
         (not= "db" (namespace a))
         (not (txcommon/reverse-ref? a))
         (not (identical? (:db/unique props) :db.unique/identity))
         (contains? resolution-scalar-types (:db/valueType props))
         (not (:db.attr/preds props))
         (contains? resolution-value-classes (class value)))))

(defn- ordinary-addition?
  [schema entity]
  (and
    (nil? (meta entity))
    (cond
      (#{clojure.lang.PersistentArrayMap clojure.lang.PersistentHashMap} (class entity))
      (let [e (:db/id entity)]
        (and (or (txprep/auto-tempid? e)
                 (contains? resolution-value-classes (class e)))
             (or (txprep/tempid? e) (and (integer? e) (not (neg? ^long e))))
             (reduce-kv
               (fn [_ a v]
                 (if (or (= a :db/id)
                         (and (ordinary-value? schema a v)
                              (not= :db.cardinality/many (:db/cardinality (schema a)))))
                   true (reduced false)))
               true entity)))

      (identical? clojure.lang.PersistentVector (class entity))
      (let [[op e a v tx] entity]
        (and (= :db/add op)
             (<= 4 (count entity) 5)
             (integer? e) (not (neg? ^long e))
             (or (nil? tx) (integer? tx))
             (ordinary-value? schema a v)))

      :else false)))

(defn- plan-ordinary-additions
  "Prove that flat scalar additions cannot introduce reference tempids.
  An entirely repeated input also needs only its first ordinary addition:
  there are no intervening replacements, retractions or user callbacks."
  ([db entities collapse?]
   (plan-ordinary-additions db entities collapse? (coreprep/make-prepare-ctx db)))
  ([_db entities collapse? ctx]
   (when (and (seq entities)
              (not (:auto-entity-time? (:opts ctx)))
              (empty? (:db.type/tuple (:rschema ctx))))
     (let [schema (:schema ctx)
           entity (first entities)]
       (when (ordinary-addition? schema entity)
         (loop [es (next entities) repeated? collapse?]
           (if (seq es)
             (let [next-entity (first es)]
               (when (or (identical? entity next-entity)
                         (ordinary-addition? schema next-entity))
                 (recur (next es) (and repeated? (= entity next-entity)))))
             (if (and repeated? (next entities)) [entity] entities))))))))

(defn- plan-flat-upserts
  "Resolve independent flat identity maps before allocating forward refs.
  Carry normalized identities and native owners only when the entire request
  excludes host callbacks. Ordered mutations, nested entities and predicates
  use ordinary resolution and retries."
  [db entities ctx]
  ;; :db/ident is implicit, but db-namespaced keys cannot enter this flat
  ;; path. Only identities that an eligible map can name justify the walk.
  (when (some #(not= "db" (namespace %))
              (:db.unique/identity (:rschema ctx)))
    (let [store (:store db)
          schema (:schema ctx)
          identity? #(identical? :db.unique/identity (:db/unique (schema %)))
          scalar-types resolution-scalar-types]
      (when (and (seq entities)
                 (not (:auto-entity-time? (:opts ctx)))
                 (empty? (:db.type/tuple (:rschema ctx)))
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
                 (some #(some identity? (keys %)) entities)
                 (= (count entities) (count (set (map :db/id entities)))))
        (let [safe? (every? (fn [entity]
                              (and (#{clojure.lang.PersistentArrayMap
                                      clojure.lang.PersistentHashMap} (class entity))
                                   (every? (fn [[a v]]
                                             (if (= :db/id a)
                                               (or (txprep/auto-tempid? v)
                                                   (contains? resolution-value-classes (class v)))
                                               (and (or (contains? scalar-types
                                                                   (:db/valueType (schema a)))
                                                        (= :db.type/ref (:db/valueType (schema a))))
                                                    (or (nil? v)
                                                        (contains? resolution-value-classes (class v))))))
                                           entity)))
                            entities)]
          (when (or safe? (next entities))
            (loop [es (seq entities) tempids {} planned (transient [])]
              (if-let [entity (first es)]
                (if-let [a (first (filter identity? (keys entity)))]
                  (let [v (coreprep/correct-value-with-props
                            (if safe? (:opts ctx) (opts store)) (schema a) a (get entity a))
                        stored (when safe? (i/av-first-e store a v))
                        resolution (when safe? (IdentityResolution. a v stored))
                        eid (if resolution
                              ;; Ordinary attempts clear these indexes on entry;
                              ;; only an active batch owns a pending prefix.
                              (if txcommon/*batch-prepare*
                                (txprep/identity-owner db resolution) stored)
                              (txcommon/av-first-e db a v))]
                    (recur (next es) (cond-> tempids eid (assoc (:db/id entity) eid))
                           (conj! planned (if resolution
                                            (ResolvedIdentityMap. entity resolution)
                                            entity))))
                  (recur (next es) tempids (conj! planned entity)))
                {:tempids tempids :entities (persistent! planned) :stable-schema? safe?}))))))))

(defn execute-tx-loop
  ([initial-report initial-es tx-time]
   (execute-tx-loop initial-report initial-es tx-time
                    (coreprep/make-prepare-ctx (:db-before initial-report))))
  ([initial-report initial-es tx-time ctx]
   (let [db (:db-before initial-report)
         ordinary (when-not (::value-tempids initial-report)
                    (plan-ordinary-additions db initial-es
                                             (empty? (:tx-data initial-report)) ctx))
         plan (when-not ordinary (plan-flat-upserts db initial-es ctx))
         ctx (if (and plan (not (:stable-schema? plan)))
               (coreprep/make-prepare-ctx db) ctx)
         initial-es (or ordinary (:entities plan) initial-es)
         upserts (:tempids plan)
         initial-report (if (seq upserts)
                          (-> initial-report
                              (update :tempids merge upserts)
                              (update ::upserted-tempids merge upserts))
                          initial-report)]
     ;; All operations share one attempt-local holder. Only user callbacks can
     ;; replace its schema snapshot; it never becomes part of the report.
     (loop [initial-report initial-report ctx ctx]
       (let [result (txcommon/with-lookup-ref-cache (:db-before initial-report)
                      (execute-tx-loop* initial-report initial-es tx-time
                                        (boolean ordinary) (volatile! ctx)))]
         (if (instance? TxRetry result)
           (recur (.-report ^TxRetry result)
                  (coreprep/make-prepare-ctx (:db-before initial-report)))
           result))))))

(defn local-transact-tx-data
  ([initial-report initial-es tx-time]
   (local-transact-tx-data initial-report initial-es tx-time
                           (coreprep/make-prepare-ctx (:db-before initial-report))))
  ([initial-report initial-es tx-time ctx]
   (if c/*use-prepare-path*
     (let [ptx (coreprep/prepare-tx
                 ctx initial-es tx-time
                 (fn [ctx es t] (execute-tx-loop initial-report es t ctx)))]
       (cond-> (assoc (dissoc-present initial-report ::max-eid-before)
                      :db-after (:db-after ptx)
                      :tx-data (:tx-data ptx)
                      :tempids (:tempids ptx))
         (seq (:new-attributes ptx))
         (assoc :new-attributes (:new-attributes ptx))

         (seq (:ensures ptx))
         (assoc ::ensures (:ensures ptx))))
     (execute-tx-loop initial-report initial-es tx-time ctx))))
