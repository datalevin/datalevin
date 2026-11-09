;; Copyright (c) Huahai Yang. All rights reserved.
;; Distributed under the Eclipse Public License 2.0.
(ns ^:no-doc datalevin.db.tx.plan
  "Owned transaction operands and schema-independent literal helpers.")

(defrecord TxPlan [ctx strategy entries entity-reads])

(deftype ScalarEntityRead [^objects names ^longs aids])

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
