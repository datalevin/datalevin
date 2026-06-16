;;
;; Copyright (c) Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns ^:no-doc datalevin.prepare
  "Prepare phase of transaction processing."
  (:require
   [datalevin.interface :as i
    :refer [schema opts]]
   [datalevin.index :as idx]
   [datalevin.datom :as d]
   [datalevin.util :as u]
   [datalevin.bits :as b]
   [datalevin.validate :as vld])
  (:import
   [datalevin.datom Datom]
   [java.util Date]))

;; ---- Records ----

(defrecord PrepareCtx
  [schema        ; map: attr keyword -> props map
   rschema       ; reverse schema (property -> attrs)
   opts          ; store options map (:closed-schema?, :auto-entity-time?, etc.)
   store         ; the Store (for point lookups during resolve phase)
   db            ; DB snapshot (db-before, for index lookups)
   lmdb          ; the LMDB instance (for index lookups)
   attrs         ; aid -> attr mapping
   attr-cache])  ; precomputed per-attr flags cache (ref?, multival?, tuple?, etc.)

(defrecord PreparedTx
    [tx-data        ; vector of resolved Datom objects (canonical order)
     db-after       ; DB snapshot after applying all datoms (with updated indexes)
     tempids        ; map: tempid -> resolved eid
     new-attributes ; vector of attrs added in this tx
     tx-redundant   ; vector of redundant (no-op) datoms
     side-index-ops ; map: {:ft [...] :vec [...] :idoc [...] :gt [...]}
     touch-summary  ; set of touched attr keywords (for cache invalidation)
     stats])        ; optional: per-stage timing/counters map

;; ---- Constructor ----

(defn make-prepare-ctx
  "Build a PrepareCtx from a DB snapshot."
  [db lmdb]
  (let [store (:store db)]
    (->PrepareCtx (schema store)
                  (i/rschema store)
                  (opts store)
                  store
                  db
                  lmdb
                  (i/attrs store)
                  nil)))

;; ---- Stage boundary function shells ----
;; Each stage accepts a context map and returns it unchanged (pass-through).
;; Stages will be filled in subsequent steps.

(defn normalize
  "Expand/normalize entities and tx forms.
   Currently a pass-through shell."
  [ctx entities]
  entities)

(defn resolve-ids
  "Resolve tempids/upserts/refs to fixed point.
   Currently a pass-through shell."
  [ctx entities]
  entities)

(defn apply-op-semantics
  "Apply :db/add, retract, CAS, patchIdoc, etc.
   Currently a pass-through shell."
  [ctx entities]
  entities)

(defn plan-delta
  "Plan datom delta with cardinality/uniqueness rules.
   Currently a pass-through shell."
  [ctx entities]
  entities)

(defn build-side-index-ops
  "Build side-index overlay deltas, allocate IDs.
   Currently a pass-through shell."
  [ctx entities]
  entities)

(defn finalize
  "Check-value-tempids, deterministic ordering, produce PreparedTx.
   Currently a pass-through shell."
  [ctx entities]
  entities)

;; ---- Top-level entry point ----

(defn prepare-tx
  "Run the full prepare pipeline. Returns a PreparedTx.
   execute-fn: (fn [entities tx-time] -> report) runs the transaction loop."
  [ctx entities tx-time execute-fn]
  (let [report (execute-fn entities tx-time)]
    (->PreparedTx
      (:tx-data report)
      (:db-after report)
      (:tempids report)
      (:new-attributes report)
      nil   ; tx-redundant not needed externally
      nil   ; side-index-ops (future)
      nil   ; touch-summary (future)
      nil)))

;; ---- Type coercion and value correction ----

(defn coerce-inst
  "Coerce a value to java.util.Date."
  [v]
  (cond
    (instance? java.time.Instant v) (Date. (inst-ms v))
    (inst? v)    v ; java.sql.Date and java.sql.Timestamp are java.util.Date
    (integer? v) (Date. (long v))
    :else        (u/raise "Expect java.util.Date" {:input v})))

(defn coerce-uuid
  "Coerce a value to java.util.UUID."
  [v]
  (cond
    (uuid? v)   v
    (string? v) (if-let [u (parse-uuid v)]
                  u
                  (u/raise "Unable to parse string to UUID" {:input v}))
    :else       (u/raise "Expect java.util.UUID" {:input v})))

(defn type-coercion
  "Coerce a value to the appropriate type based on value type."
  [vt v]
  (case vt
    :db.type/string              (str v)
    :db.type/bigint              (biginteger v)
    :db.type/bigdec              (bigdec v)
    (:db.type/long :db.type/ref) (long v)
    :db.type/float               (float v)
    :db.type/double              (double v)
    (:db.type/bytes :bytes)      (if (bytes? v) v (byte-array v))
    (:db.type/keyword :keyword)  (keyword v)
    (:db.type/symbol :symbol)    (symbol v)
    (:db.type/boolean :boolean)  (boolean v)
    (:db.type/instant :instant)  (coerce-inst v)
    (:db.type/uuid :uuid)        (coerce-uuid v)
    :db.type/tuple               (vec v)
    v))

(defn correct-datom*
  "Reconstruct datom with corrected value."
  [^Datom datom v]
  (d/datom (.-e datom) (.-a datom) v
           (d/datom-tx datom) (d/datom-added datom)))

(declare correct-value)

(defn correct-datom
  "Correct value in datom via type validation and coercion."
  [store ^Datom datom]
  (correct-datom* datom (correct-value store (.-a datom) (.-v datom))))

(defn correct-value
  "Validate type and coerce value for an attribute."
  [store a v]
  (let [props ((schema store) a)
        vt    (idx/value-type props)]
    (if (identical? vt :db.type/idoc)
      ((requiring-resolve 'datalevin.idoc/parse-value) a props (opts store) v)
      (type-coercion (vld/validate-type store a v) v))))
