;;
;; Copyright (c) Nikita Prokopov, Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns ^:no-doc datalevin.db.tx.bind
  "Operand binding for compiler-selected scalar, blind and patch fast paths.
  Binding preserves correction and entity-resolution checkpoints; it emits no datoms."
  (:require [datalevin.datom :refer [datom?]]
            [datalevin.db.tx.compile :as compile]
            [datalevin.db.tx.common :as txcommon]
            [datalevin.db.tx.plan :as plan]
            [datalevin.db.tx.prepare :as txprep]
            [datalevin.prepare :as coreprep]
            [datalevin.validate :as vld])
  (:import [datalevin.db.tx.plan AttrInfo BlindEntity]
           [org.eclipse.collections.impl.list.mutable FastList]
           [java.util Collection HashMap HashSet]))

(defn scalar-update-entity-id
  "Resolve an existing entity id for a scalar update, or nil to fall back.
  Numeric ids use the in-memory maximum; lookup references and idents read db."
  ^Long [db e]
  (let [eid (cond
              (integer? e)
              (when (pos? (long e)) (long e))

              (and (sequential? e) (= 2 (count e)))
              (try (txcommon/entid db e) (catch Throwable _ nil))

              (keyword? e)
              (try (txcommon/entid db e) (catch Throwable _ nil))

              :else nil)]
    (when (and eid (pos? (long eid)) (<= (long eid) (long (:max-eid db))))
      (long eid))))

(defn- resolve-scalar-update-reference!
  "Reuse an entity resolution within one read-only preparation request."
  [db ^HashMap entities reference]
  (or (.get entities reference)
      (when-let [eid (scalar-update-entity-id db reference)]
        (.put entities reference eid)
        eid)))

(defn- resolve-scalar-update-references
  "Resolve distinct references in order, discarding results after this request."
  [db references]
  (let [entities (HashMap.)]
    (when (reduce (fn [_ reference]
                    (if (resolve-scalar-update-reference! db entities reference)
                      true
                      (reduced false)))
                  true references)
      entities)))

(defn resolve-scalar-update-candidates!
  "Resolve eager candidates before correction, retaining the alias/E-A check."
  [db ^FastList candidates]
  (if (= 4 (.size candidates))
    ;; Singleton requests already satisfy distinctness.
    (when-let [eid (scalar-update-entity-id db (.get candidates 0))]
      (.set candidates 0 eid)
      true)
    (when-let [entities (resolve-scalar-update-references
                         db (map #(.get candidates %) (range 0 (.size candidates) 4)))]
      (let [seen (HashSet.)]
        (loop [i 0]
          (if (< i (.size candidates))
            (let [eid (get entities (.get candidates i))
                  attr (.get candidates (inc i))]
              (when (.add seen [eid attr])
                (.set candidates i eid)
                (recur (+ i 4))))
            true))))))

(defn resolve-scalar-update-entries
  "Bind all scalar entity operands before old-value reads or datom emission.
  Return resolved entries or nil for missing/out-of-bound owners or E/A aliases.
  Resolution scratch is request-owned; bound operands and read layouts remain
  in the same compiler plan."
  [db entries]
  (txcommon/with-lookup-ref-cache db
    (if (= 1 (count entries))
      (let [[e] (first entries)]
        (when-let [eid (scalar-update-entity-id db e)]
          [(assoc (first entries) 0 eid)]))
      (let [entities (HashMap.)
            seen (HashSet.)]
        (reduce (fn [resolved [e attr :as entry]]
                  (if-let [eid (resolve-scalar-update-reference! db entities e)]
                    ;; Distinct references can alias the same entity.
                    ;; Repeated E/A updates need general ordering.
                    (if (.add seen [eid attr])
                      (conj resolved (assoc entry 0 eid))
                      (reduced nil))
                    (reduced nil)))
                [] entries)))))

(defn- append-current-scalar-candidate!
  "Keep runtime entity checks around shared scalar attribute/distinctness rules."
  [db infos store-schema tuple-source-attrs candidates seen e attr value]
  (when-let [^AttrInfo info (compile/scalar-attribute-info
                             store-schema tuple-source-attrs attr value false infos)]
    (when-let [eid (if (or (keyword? e) (and (sequential? e) (= 2 (count e))))
                    e (scalar-update-entity-id db e))]
      (compile/append-scalar-candidate!
        candidates seen eid attr value (.-value-type info)))))

(defn prepare-scalar-operands
  "Bind forms over an associative view and captured context.
  Eager aliases resolve before correction; deferred aliases retain their inputs.
  Corrections occur once in order, with errors propagated at the original stage.
  Candidate scratch is private; return operands only, without headers or reads
  of old values. Validate input in this pass; caller owns lookup-cache lifetime."
  [db ctx initial-es defer-entity-resolution?]
  (let [store-schema (:schema ctx) store-opts (:opts ctx)
        validate? (:validate-data? store-opts)
        tuple-source-attrs (:db/attrTuples (:rschema ctx))
        infos (HashMap.)]
    (when-not (:auto-entity-time? store-opts)
      (let [max-eid (:max-eid db)
            ^FastList candidates
            (FastList. (int (* 4 (if (and (counted? initial-es)
                                         (instance? Collection initial-es))
                                  (long (.size ^Collection initial-es)) 0))))]
        (loop [es (seq initial-es) seen nil resolve-entities? false]
          (if es
            (let [entity (first es)]
              (cond
                (and (map? entity) (nil? (meta entity)) (> (count entity) 1)
                     (plan/resolved-positive-ref? (:db/id entity)))
                (let [remaining (next es)
                      e (:db/id entity)
                      seen (or seen
                               (when (or remaining (> (count entity) 2))
                                 (HashSet.)))]
                  (when (and (<= (long e) (long max-eid))
                             (reduce-kv
                               (fn [_ attr value]
                                 (if (identical? attr :db/id)
                                   true
                                   (if (and (keyword? attr) (pos? (count (name attr)))
                                            (not (txcommon/reverse-ref? attr))
                                            (append-current-scalar-candidate!
                                              db infos store-schema tuple-source-attrs candidates
                                              seen e attr value))
                                     true (reduced false))))
                               true entity))
                    (recur remaining seen resolve-entities?)))
                (and (sequential? entity) (nil? (meta entity))
                     (= 4 (count entity)) (identical? :db/add (nth entity 0)))
                (let [remaining (next es)
                      [_ e attr value] entity]
                  (when-let [eid (append-current-scalar-candidate!
                                   db infos store-schema tuple-source-attrs candidates seen e attr value)]
                    (recur remaining
                           (or seen
                               (when remaining
                                 (doto (HashSet.) (.add [eid attr]))))
                           (or resolve-entities? (not (integer? eid))))))
                :else nil))
            (when (and (not (.isEmpty candidates))
                       (or defer-entity-resolution? (not resolve-entities?)
                           (resolve-scalar-update-candidates! db candidates)))
              ;; Mutate only owned scratch; a late failure publishes nothing.
              ;; Legacy correction may return nil, unlike compiler readiness.
              (loop [i 0]
                (when (< i (.size candidates))
                  (.set candidates (+ i 2)
                        (coreprep/correct-scalar-value
                          validate? (.get candidates (+ i 3)) (.get candidates (+ i 2))))
                  (recur (+ i 4))))
              (plan/scalar-candidate-entries candidates))))))))

;; Identity probes in the blind path share the LMDB write transaction. Large
;; batches enable them from two entities onward so ingestion avoids the old
;; discontinuity. A single entity is routed to the identity-upsert
;; specialization when it names a unique-identity attribute, on both the WAL
;; and non-WAL direct paths, so simple upserts avoid the ordinary resolver.
;; Value-unique inserts reject duplicates instead of upserting, so they can use
;; the blind path at any batch size.
(def ^:private ^:const ^long blind-unique-write-threshold 2)

(defn- blind-write-multiple-values?
  [many? unique-identity-attrs value]
  (and many?
       (coll? value)
       (not (map? value))
       (not (and (= (count value) 2)
                 (contains? unique-identity-attrs (first value))))))

(defn- append-blind-value!
  ^Boolean [^FastList attrs ^FastList unique-avs
            seen-avs set-capacity fuse-unique-inserts?
            store-opts ^AttrInfo info attr entity value]
  (if (or (map? value)
          (datom? value)
          (and (coll? value) (not (bytes? value))))
    false
    (let [props   (.-props info)
          value'  (if (compile/blind-value-ready? info value)
                    value
                    (do
                      (vld/validate-val value entity)
                      (if (.-scalar-correction? info)
                        (coreprep/correct-scalar-value
                          (:validate-data? store-opts) (.-value-type info) value)
                        (coreprep/correct-value-with-props
                          store-opts props attr value))))]
      (compile/append-blind-canonical-value!
        attrs unique-avs seen-avs set-capacity fuse-unique-inserts? info attr value'))))

(defn- prepare-blind-write-entity
  [store-opts unique-identity-attrs ^FastList unique-avs seen-avs set-capacity
   has-many? fuse-unique-inserts? attr-infos entity]
  (let [old-eid   (:db/id entity)
        ;; An implicit entity does not need a transaction-scoped tempid: the
        ;; blind stamper assigns its final EID directly and never exposes an
        ;; implicit mapping in the report. Retain explicit tempids so repeated
        ;; user IDs still fall back to the resolver and merge correctly.
        entity-id old-eid]
    (when (if (nil? old-eid)
            (not (contains? entity :db/id))
            (txprep/tempid? old-eid))
      (let [^FastList attrs (FastList. (int (* 2 (count entity))))
            supported?
            (reduce-kv
              (fn [_ attr raw-value]
                (if (identical? attr :db/id)
                  true
                  ;; Upfront preflight supplies a map; bulk preparation uses
                  ;; a value-aware lookup without adding per-entity state.
                  (if-some [^AttrInfo info (if (instance? java.util.HashMap attr-infos)
                                           (.get ^java.util.HashMap attr-infos attr)
                                           (attr-infos attr raw-value))]
                    (let [props (.-props info)
                          many? (identical? :db.cardinality/many (:db/cardinality props))
                          _ (when (and (not (.-fuse-blind? info)) fuse-unique-inserts?)
                              (vreset! fuse-unique-inserts? false))
                          supported-values?
                          (if (blind-write-multiple-values?
                                many? unique-identity-attrs raw-value)
                            (let [vt (.-value-type info)
                                  ^java.util.Set seen-values (compile/blind-value-set vt)
                                  values (seq raw-value)]
                              (when values (vreset! has-many? true))
                              (loop [values values]
                                ;; A nil member must reach value validation.
                                (if values
                                  (if (append-blind-value!
                                        attrs unique-avs seen-avs set-capacity
                                        fuse-unique-inserts? store-opts info attr entity
                                        (first values))
                                    (if (.add seen-values (.get attrs (dec (.size attrs))))
                                      (recur (next values))
                                      (if (:db/unique props)
                                        false
                                        (do
                                          ;; Keep the first normalized occurrence.
                                          ;; Unique AV collection remains untouched.
                                          (.remove attrs (int (dec (.size attrs))))
                                          (.remove attrs (int (dec (.size attrs))))
                                          (recur (next values)))))
                                    false)
                                  true)))
                            (do
                              (when many? (vreset! has-many? true))
                              (append-blind-value!
                                attrs unique-avs seen-avs set-capacity
                                fuse-unique-inserts? store-opts info attr entity
                                raw-value)))]
                      (if-not supported-values?
                        (reduced false)
                        true))
                    (reduced false))))
              true
              entity)]
        (when supported?
          (BlindEntity. entity-id attrs))))))

(defn- blind-attributes-supported?
  "Preflight attributes and retain unique/many flags without correcting values."
  [entities check-attr]
  (reduce (fn [kinds entity]
            (if-some [kinds (reduce-kv check-attr kinds entity)]
              kinds
              (reduced nil)))
          0 entities))

(defn blind-entity-kinds
  "Retain the classified implicit/tempid proof, or establish it before metadata
  capture. Zero proves implicit IDs; false declines. Runtime numeric/tempid
  callbacks and input demand stay outside compiler probing."
  [initial-es entity-kinds]
  (if (some? entity-kinds)
    entity-kinds
    (reduce (fn [kinds entity]
              (if (map? entity)
                (let [e (:db/id entity)]
                  (cond
                    (nil? e) (if (contains? entity :db/id) (reduced false) kinds)
                    (txprep/tempid? e) 1
                    :else (reduced false)))
                (reduced false)))
            0 initial-es)))

(defn prepare-blind-operands
  "Bind new maps over captured metadata and a prior ID-shape proof.
  Preserve demand, correction, deduplication and uniqueness order. Return the
  existing operand/flag components, without a header, persistent IDs or store
  reads. Caller owns initial ID admission, snapshot capture and provenance."
  [ctx initial-es allow-small-unique? check-unique-avs? entity-kinds]
  (let [store-schema (:schema ctx)
        store-opts (:opts ctx)
        store-rschema (:rschema ctx)
        tuple-source-attrs (:db/attrTuples store-rschema)
        unique-identity-attrs (:db.unique/identity store-rschema)
        auto-entity-time? (boolean (:auto-entity-time? store-opts))
        allow-unique?     (or allow-small-unique?
                              (>= (long (bounded-count
                                         blind-unique-write-threshold
                                         initial-es))
                                  blind-unique-write-threshold))
        initial-count     (int
                            (if (instance? java.util.Collection initial-es)
                              (.size ^java.util.Collection initial-es)
                              0))
        ;; Amortize the lazy preflight in bulk requests. Small requests
        ;; retain their upfront attribute check and direct metadata lookup.
        incremental?      (if (instance? java.util.Collection initial-es)
                            (>= initial-count 32)
                            (>= (long (bounded-count 32 initial-es)) 32))
        attr-infos        (java.util.HashMap.)
        check-attr        (fn [kinds attr value]
                            (if (identical? attr :db/id)
                              kinds
                              (let [^AttrInfo info
                                    (compile/blind-attr-info!
                                      attr-infos store-schema tuple-source-attrs
                                      allow-unique? attr)]
                                (if (and info (.-blind-kinds info)
                                         (or (not (identical? (.-value-type info) :db.type/ref))
                                             (if (blind-write-multiple-values?
                                                   (identical?
                                                     :db.cardinality/many
                                                     (:db/cardinality (.-props info)))
                                                   unique-identity-attrs value)
                                               (every? plan/resolved-positive-ref? value)
                                               (plan/resolved-positive-ref? value))))
                                  (bit-or (long kinds) (long (.-blind-kinds info)))
                                  (reduced nil)))))
        attribute-kinds   (when-not incremental?
                            (blind-attributes-supported? initial-es check-attr))]
    (when (or incremental? (some? attribute-kinds))
      (let [;; Entity preparation receives an Object; box once per batch.
            set-capacity      (Integer/valueOf
                                (int (max 1 (inc (quot (* 4 (long initial-count))
                                                      3)))))
            ;; Small requests already proved which workspace is needed.
            ;; Incremental bulk preflight retains its existing shared state.
            ^FastList unique-avs (when (or incremental?
                                          (not (zero? (bit-and (long attribute-kinds) 1))))
                                   (FastList.))
            seen-avs          (when (and unique-avs check-unique-avs?)
                                (volatile! nil))
            seen-tempids      (when (and (pos? (long entity-kinds)) (not (== initial-count 1)))
                                (java.util.HashSet. (int set-capacity)))
            has-many?         (when (or incremental?
                                       (not (zero? (bit-and (long attribute-kinds) 2))))
                                (volatile! false))
            fuse-unique-inserts? (when unique-avs (volatile! true))
            ;; Canonical scalar values are inert. Check and prepare those
            ;; together; complete the attribute check before any coercion
            ;; or collection handling that could invoke user code or fail.
            preflight         (when incremental?
                                (delay (blind-attributes-supported?
                                         initial-es check-attr)))
            attr-lookup       (when incremental?
                                (fn [attr value]
                                  ;; A failed preflight stops preparation.
                                  ;; Once it succeeds, use the populated map.
                                  (if (realized? preflight)
                                    (.get attr-infos attr)
                                    (let [^AttrInfo info
                                          (compile/blind-attr-info!
                                            attr-infos store-schema
                                            tuple-source-attrs allow-unique? attr)]
                                      (when (and info (.-blind-kinds info)
                                                 (or (compile/blind-value-ready? info value)
                                                     @preflight))
                                        info)))))
            entity-attr-infos (or attr-lookup attr-infos)
            entities
            (reduce
              (fn [^FastList acc entity]
                (if-let [^BlindEntity prepared
                         (prepare-blind-write-entity
                           store-opts unique-identity-attrs unique-avs
                           seen-avs set-capacity has-many?
                           fuse-unique-inserts? entity-attr-infos entity)]
                  (if (and (.-tempid prepared) seen-tempids
                           (not (.add seen-tempids (.-tempid prepared))))
                    (reduced nil)
                    (if (or (not (.isEmpty ^FastList (.-attrs prepared)))
                            auto-entity-time?)
                      (doto acc (.add prepared))
                      acc))
                  (reduced nil)))
              (FastList. initial-count)
              initial-es)
            has-unique?       (boolean (and unique-avs (not (.isEmpty unique-avs))))
            fuse-unique-inserts? (and has-unique? @fuse-unique-inserts?)
            identity-upsert-av
            (when (and has-unique? entities
                       (not auto-entity-time?)
                       (= 1 (.size ^FastList entities)))
              (compile/blind-identity-upsert-av
                store-schema (.get ^FastList entities 0)))]
        (when (and entities (not (.isEmpty ^FastList entities))
                   ;; Fused writes enforce uniqueness natively. Other writes
                   ;; deduplicate only the AVs collected during preparation.
                   (or check-unique-avs? fuse-unique-inserts? (not has-unique?)
                       (= (.size unique-avs)
                          (.size (java.util.HashSet. ^java.util.Collection unique-avs)))))
          {:entries entities
           :auto-entity-time? auto-entity-time?
           :unique-avs unique-avs
           :has-unique? has-unique?
           :has-many? (boolean (and has-many? @has-many?))
           :fuse-unique-inserts? fuse-unique-inserts?
           :identity-upsert-av identity-upsert-av})))))

(defn prepare-patch-operands
  "Bind an admitted patch at its established caller checkpoint. The compiler
  owns the resulting template; document reads remain current-writer work."
  [ctx initial-es]
  (let [store-opts (:opts ctx)
        forms      (seq initial-es)
        entity     (when (and forms (nil? (next forms))) (first forms))]
    (when (and (sequential? entity)
               (= 4 (count entity))
               (not (:auto-entity-time? store-opts)))
      (let [[op e attr patch-ops] entity
            info (compile/patch-attribute-info ctx attr patch-ops)]
        (when (and (identical? op :db.fn/patchIdoc)
                   (integer? e)
                   (<= 1 e Long/MAX_VALUE)
                   info)
          (plan/patch-plan ctx entity (long e) attr patch-ops))))))

