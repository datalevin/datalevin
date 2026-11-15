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
  (:import [datalevin.db.tx.plan AttrInfo BlindTemplate]
           [org.eclipse.collections.impl.list.mutable FastList]
           [java.util HashMap HashSet]))

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

(defn prepare-scalar-operands
  "Use the compiler's source/correction operands at the caller checkpoint.
  Eager aliases resolve before correction; deferred aliases retain their inputs.
  Runtime errors and lazy demand keep their established order."
  [db ctx forms defer-entity-resolution?]
  (when-not (:auto-entity-time? (:opts ctx))
    (let [infos (HashMap.) resolve-entities? (volatile! false)
          candidates
          (compile/collect-scalar-candidates
            forms map?
            #(when (plan/resolved-positive-ref? %) %)
            #(<= (long %) (long (:max-eid db)))
            (fn [candidates seen e attr value _]
              (when-let [^AttrInfo info (compile/scalar-attribute-info
                                         (:schema ctx) (:db/attrTuples (:rschema ctx))
                                         attr value false infos)]
                (when-let [eid (if (or (keyword? e) (and (sequential? e) (= 2 (count e))))
                                e (scalar-update-entity-id db e))]
                  (when-not (integer? eid) (vreset! resolve-entities? true))
                  (compile/append-scalar-candidate!
                    candidates seen eid attr value (or (.-value-type info) :data))))))]
      (when (and candidates
                 (or defer-entity-resolution? (not @resolve-entities?)
                     (resolve-scalar-update-candidates! db candidates)))
        (compile/correct-scalar-candidates!
          candidates #(coreprep/correct-scalar-value (:validate-data? (:opts ctx)) %1 %2))
        (plan/scalar-candidate-entries candidates)))))

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

(defn bind-blind-operands
  "Bind a compiler template at the caller's correction checkpoint. All operand
  layout, deduplication and uniqueness proofs belong to shared lowering."
  [^BlindTemplate template check-unique-avs?]
  (let [ctx (.-ctx template)]
    (compile/bind-blind-template
      template check-unique-avs?
      (fn [source]
        (let [e (:db/id source)]
          (if (nil? e) (not (contains? source :db/id)) (txprep/tempid? e))))
      (fn [^AttrInfo info attr source _ value]
        (if (or (map? value) (datom? value) (and (coll? value) (not (bytes? value))))
          compile/operand-decline
          (if (compile/blind-value-ready? info value)
            value
            (do (vld/validate-val value source)
                (if (.-scalar-correction? info)
                  (coreprep/correct-scalar-value (:validate-data? (:opts ctx)) (.-value-type info) value)
                  (coreprep/correct-value-with-props (:opts ctx) (.-props info) attr value)))))))))

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

