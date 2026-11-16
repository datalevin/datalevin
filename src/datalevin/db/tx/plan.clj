;; Copyright (c) Huahai Yang. All rights reserved.
;; Distributed under the Eclipse Public License 2.0.
(ns ^:no-doc datalevin.db.tx.plan
  "Owned transaction operands and schema-independent literal helpers."
  (:require [datalevin.datom :as datom])
  (:import [java.util HashMap]
           [org.eclipse.collections.impl.list.mutable FastList]))

(defrecord TxPlan [ctx strategy entries entity-reads])

(deftype TxParam [key])

(defrecord WriteOp [ctx source expression params kind entity fields tx annotations])

(defrecord WriteTemplate [ctx operations params batch? strategy explicit? metadata])

(deftype PreparedTxInput [forms])

(defn request-forms
  "Materialize a prepared request only when ordinary preparation is required.
  The delayed result is a vector, preserving materialized-input semantics."
  [input]
  (if (instance? PreparedTxInput input)
    (force (.-forms ^PreparedTxInput input)) input))

(deftype ScalarEntityRead [^objects names ^longs aids])

(deftype ScalarSequence [entries ^ints slots ^long slot-count])

(defn scalar-sequence
  "Assign each E/A one ordered execution slot. Entity operands must already
  have their final identity when different aliases can name the same entity."
  [entries]
  (let [positions (HashMap.) slots (int-array (count entries))]
    (dotimes [i (count entries)]
      (let [entry (nth entries i) key [(nth entry 0) (nth entry 1)]
            slot (or (.get positions key)
                     (let [slot (.size positions)] (.put positions key slot) slot))]
        (aset-int slots i (int slot))))
    (ScalarSequence. entries slots (.size positions))))

(deftype BlindEntity [tempid ^FastList attrs])

;; Templates cannot enter the executor as TxPlans. Their triples still contain
;; raw values and correction instructions; only lowering produces BlindEntity.
(deftype BlindSource [source ^FastList attrs])

(deftype BlindTemplate
  [ctx ^FastList entries infos ^boolean identity? ^boolean fuse?])

(deftype AttrInfo
  ;; One immutable logical binding; schema props remain the authoritative payload.
  ;; Eligibility is compiler-owned. Physical storage tables copy encoding fields.
  [props value-type storage-type tuples
   ^boolean ordinary? ^boolean insert? ^boolean update? ^boolean typed?
   blind-kinds ^boolean fuse-blind? ^boolean textual? ^boolean scalar-correction?
   ordered?])

(defn with-ordered-proof
  "Complete an immutable binding during ordered selection. Nil is unchecked;
  Boolean results belong only to this compiler attempt and captured context."
  [^AttrInfo info eligible?]
  (AttrInfo. (.-props info) (.-value-type info) (.-storage-type info) (.-tuples info)
             (.-ordinary? info) (.-insert? info) (.-update? info) (.-typed? info)
             (.-blind-kinds info) (.-fuse-blind? info) (.-textual? info)
             (.-scalar-correction? info) (boolean eligible?)))

(deftype MixedInsert [tempid ^objects attrs])

(deftype MixedUpdate [^long e attr value])

(deftype MixedOperands
  [entries infos ^long required-eid ^boolean prepare-reader? ^boolean storage?])

(defn patch-plan
  "Own one normalized patch entry; source is diagnostic context only."
  [ctx source e attr operations]
  (->TxPlan ctx :patch [[e attr operations source]] nil))

(defn unique-av-key
  "Canonicalize unique keys with the pending indexes' scalar/tuple equivalence.
  Instants use storage milliseconds and decimals ignore scale; operands remain."
  [[attr value :as av]]
  (let [key (datom/storage-value-key value)]
    (if (identical? key value) av [attr key])))

(defn scalar-candidate-entries
  "Project owned E/A/value/type scratch into scalar operands. No correction,
  hashing, entity binding or input traversal; values retain their identity."
  [^FastList candidates]
  (loop [i 0 entries (transient [])]
    (if (< i (.size candidates))
      (let [e (.get candidates i) attr (.get candidates (+ i 1))
            value (.get candidates (+ i 2))
            entry [e attr value]]
        (recur (+ i 4) (conj! entries entry)))
      (persistent! entries))))

(defn resolved-positive-ref?
  "Recognize positive literal IDs within the signed long range."
  [value]
  (and (integer? value)
       (try (pos? (long value))
            (catch IllegalArgumentException _ false))))

(defn reverse-ref-attr?
  "Recognize a reverse-reference keyword. Attribute validation stays in runtime."
  [attr]
  (= \_ (nth (name attr) 0)))
