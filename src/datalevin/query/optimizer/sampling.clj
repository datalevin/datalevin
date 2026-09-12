;;
;; Copyright (c) Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns ^:no-doc datalevin.query.optimizer.sampling
  "Sampling helpers"
  (:require
   [datalevin.constants :as c]
   [datalevin.parser :as dp]
   [datalevin.query.access :as qaccess]
   [datalevin.query.plan :as qplan]
   [datalevin.query.optimizer.estimates
    :refer [estimate-link-cost estimate-round materialized-output-cost
            relation-size]])
  (:import
   [datalevin.parser Function Variable Predicate]))

(defn -sample [step db source]
  (qplan/step-sample step db source))

(defn sample-relation-projection-count
  [find-vars {:keys [attrs tuples] :as relation}]
  (let [projected (filterv #(contains? attrs %) find-vars)]
    (cond
      (zero? (relation-size relation)) 0
      (empty? projected)              1
      :else
      (count
        (into #{}
              (map (fn [^objects tuple]
                     (mapv #(aget tuple (long (attrs %))) projected)))
              tuples)))))

(defn sampled-access-output
  [parsed-q relations]
  (let [find-vars (dp/find-vars (:qfind parsed-q))]
    (reduce
      (fn [n relation]
        (estimate-round
          (* (double n)
             (double
               (sample-relation-projection-count find-vars relation)))))
      1 relations)))

(defn sample-context-size
  [relations]
  (reduce
    (fn [n relation]
      (estimate-round
        (* (double n) (double (relation-size relation)))))
    1 relations))

(defn access-sample-relation
  [context access-var]
  (some #(when (contains? (:attrs %) access-var) %)
        (:rels context)))

(defn sample-safe-residual?
  [clause]
  ;; Query-input functions can be effectful and have no estimator contract.
  ;; Account for their evaluation cost, but leave their selectivity unknown
  ;; until adaptive execution observes it.
  (not (and (or (instance? Predicate clause)
                (instance? Function clause))
            (instance? Variable (:fn clause)))))

(defn sample-stage-output-cap
  ^long [^double remaining-budget ^long output-width]
  (if-not (pos? remaining-budget)
    0
    (let [per-output (+ (double c/magic-cost-hash-join-output-tuple)
                        (* (double c/magic-cost-hash-join-output-cell)
                           (double output-width)))]
      (long
        (min (double Long/MAX_VALUE)
             (Math/floor (/ remaining-budget per-output)))))))

(defn estimated-access-sample-stage-cost
  ^double [^long input-size ^long output-size ^long output-width]
  (+ (double (estimate-link-cost input-size output-size))
     (materialized-output-cost output-size output-width)))

(defn estimated-late-input-size
  ^long
  [{:keys [plan rels result-set]} ^long plan-size]
  (cond
    (= result-set #{}) 0
    (seq plan)          plan-size
    :else               (long (sample-context-size rels))))

(defn sampled-access-cardinality
  "Estimate complete result cardinality from real planning samples. Heuristic
   access estimates are deliberately excluded: only an observed residual yield
   may increase the conventional plan's late-clause cardinality."
  [access-plans]
  (reduce
    (fn [best {:keys [correlated? estimate]}]
      (let [sample-rows (long (or (:sample-rows estimate) 0))
            sample-output (long (or (:sample-output estimate) 0))
            output-rows (qaccess/estimate-output-rows estimate)
            yield       (:yield estimate)]
        (if (and (not correlated?)
                 (= :sampled (:confidence estimate))
                 (pos? sample-rows)
                 (pos? sample-output)
                 (number? yield)
                 (pos? output-rows))
          (let [rows (estimate-round (* (double output-rows)
                                        (double yield)))
                candidate {:rows          rows
                           :sample-rows   sample-rows
                           :sample-output sample-output
                           :range-rows    (qaccess/estimate-range-rows estimate)
                           :yield         (double yield)
                           :confidence    :sampled}]
            (if (> rows (long (or (:rows best) 0))) candidate best))
          best)))
    nil access-plans))
