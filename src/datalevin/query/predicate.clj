;;
;; Copyright (c) Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns ^:no-doc datalevin.query.predicate
  "Forkable predicates for concurrent query scans.")

(def ^:private fork-factory-key ::fork-factory)

(defn forkable-predicate
  "Attach a factory that creates an independent predicate instance."
  ([factory]
   (forkable-predicate (factory) factory))
  ([pred factory]
   (with-meta pred (assoc (meta pred) fork-factory-key factory))))

(defn shareable-predicate
  "Mark an immutable predicate as safe to share between workers."
  [pred]
  (forkable-predicate pred (fn [] pred)))

(defn forkable-predicate?
  [pred]
  (or (nil? pred)
      (some? (get (meta pred) fork-factory-key))))

(defn fork-predicate
  [pred]
  (if-let [factory (and pred (get (meta pred) fork-factory-key))]
    (factory)
    pred))

(defn fork-predicates
  ^objects [^objects preds]
  (let [n   (alength preds)
        out (object-array n)]
    (dotimes [i n]
      (aset out i (fork-predicate (aget preds i))))
    out))

(defn combine-predicates
  "Combine predicates while preserving their fork factories when possible."
  [old-pred new-pred or?]
  (cond
    (nil? new-pred) old-pred
    (nil? old-pred) new-pred
    :else
    (let [combine (if or?
                    (fn [p q]
                      (fn [x] (or (p x) (q x))))
                    (fn [p q]
                      (fn [x] (and (p x) (q x)))))
          pred    (combine old-pred new-pred)]
      (if (and (forkable-predicate? old-pred)
               (forkable-predicate? new-pred))
        (forkable-predicate
          pred
          (fn []
            (combine (fork-predicate old-pred)
                     (fork-predicate new-pred))))
        pred))))
