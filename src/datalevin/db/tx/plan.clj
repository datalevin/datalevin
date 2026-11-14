;; Copyright (c) Huahai Yang. All rights reserved.
;; Distributed under the Eclipse Public License 2.0.
(ns ^:no-doc datalevin.db.tx.plan
  "Owned transaction operands and schema-independent literal helpers."
  (:import [org.eclipse.collections.impl.list.mutable FastList]))

(defrecord TxPlan [ctx strategy entries entity-reads])

(deftype ScalarEntityRead [^objects names ^longs aids])

(deftype BlindEntity [tempid ^FastList attrs])

(deftype AttrInfo
  ;; One immutable logical binding; schema props remain the authoritative payload.
  ;; Eligibility is compiler-owned. Physical storage tables copy encoding fields.
  [props value-type storage-type tuples
   ^boolean ordinary? ^boolean insert? ^boolean update? ^boolean typed?
   blind-kinds ^boolean fuse-blind? ^boolean textual? ^boolean scalar-correction?])

(deftype MixedInsert [tempid ^objects attrs])

(deftype MixedUpdate [^long e attr value])

(deftype MixedOperands
  [entries infos ^long required-eid ^boolean prepare-reader? ^boolean storage?])

(defn patch-plan
  "Own one normalized patch entry; source is diagnostic context only."
  [ctx source e attr operations]
  (->TxPlan ctx :patch [[e attr operations source]] nil))

(defn scalar-candidate-entries
  "Project owned E/A/value/type scratch into scalar operands. No correction,
  hashing, entity binding or input traversal; values retain their identity."
  [^FastList candidates]
  (loop [i 0 entries (transient [])]
    (if (< i (.size candidates))
      (recur (+ i 4)
             (conj! entries [(.get candidates i) (.get candidates (+ i 1))
                            (.get candidates (+ i 2))]))
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
