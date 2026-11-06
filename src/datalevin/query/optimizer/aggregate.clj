;;
;; Copyright (c) Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns ^:no-doc datalevin.query.optimizer.aggregate
  "Prove when collection already supplies distinct aggregate inputs."
  (:require [clojure.set :as set]
            [datalevin.db :as db]
            [datalevin.parser :as dp])
  (:import [datalevin.parser Pattern PlainSymbol Variable]))

(def ^:private exact-count-types
  ;; Query tuple keys and count-distinct use different equality for decimal
  ;; scale, signed zero and some generic values. Only admit uniform native
  ;; domains where both equality relations agree.
  #{:db.type/long :db.type/ref :db.type/string :db.type/keyword
    :db.type/symbol :db.type/uuid :db.type/boolean :db.type/instant})

(defn distinct-count-plan
  "Find built-in counts whose group and value cover the complete find/with
  identity. Defer native type checks until the current sources are available;
  prepared queries may run against a different schema on their next call."
  [parsed-q]
  (let [elements (dp/find-elements (:qfind parsed-q))]
    (when (and (some #(= 'count-distinct (get-in % [:fn :symbol])) elements)
               (empty? (:qwith parsed-q))
               (every? #(or (instance? Variable %) (dp/aggregate? %)) elements))
      (let [groups (into #{} (keep #(when (instance? Variable %) (:symbol %)))
                         elements)
            inputs (set (dp/find-vars (:qfind parsed-q)))
            patterns (filterv #(instance? Pattern %) (:qwhere parsed-q))]
        (into []
              (keep-indexed
                (fn [index element]
                  (when (and (dp/aggregate? element)
                             (instance? PlainSymbol (:fn element))
                             (= 'count-distinct (get-in element [:fn :symbol]))
                             (= 1 (count (:args element)))
                             (instance? Variable (first (:args element))))
                    (let [variable (:symbol (first (:args element)))]
                      (when (set/subset? inputs (conj groups variable))
                        {:index index :variable variable :patterns patterns})))))
              elements)))))

(defn- native-value-domain?
  [sources variable patterns]
  (some
    (fn [{:keys [source pattern]}]
      (let [[_ attr value] pattern
            database (get sources (or (:symbol source) '$))]
        (and (= variable (:symbol value))
             (db/db? database)
             (contains? exact-count-types
                        (get-in (db/-schema database)
                                [(:value attr) :db/valueType])))))
    patterns))

(defn simplify-distinct-counts
  "Select ordinary count over the existing distinct find projection. Retain
  element positions and all other aggregate contracts, including HAVING."
  [elements candidates sources]
  (when-let [selected
             (seq (filter #(native-value-domain? sources (:variable %) (:patterns %))
                          candidates))]
    {:elements (reduce (fn [elements {:keys [index]}]
                         (update-in elements [index :fn] assoc :symbol 'count))
                       (vec elements) selected)
     :rewrites (mapv #(-> (select-keys % [:index :variable])
                         (assoc :from 'count-distinct :to 'count
                                :proof :distinct-find-input))
                     selected)}))
