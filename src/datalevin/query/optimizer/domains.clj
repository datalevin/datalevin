;;
;; Copyright (c) Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns ^:no-doc datalevin.query.optimizer.domains
  "Bounded, query-local dimension domains for selective planning. Original
  patterns remain in the query: inferred membership is only a necessary filter."
  (:require
   [datalevin.constants :as c]
   [datalevin.db :as db]
   [datalevin.parser :as dp]
   [datalevin.query-util :as qu]
   [datalevin.query.optimizer.estimates
    :refer [estimate-link-cost materialized-output-cost]]
   [datalevin.query.optimizer.plan-cost :refer [final-plan-cost]]
   [datalevin.query.optimizer.rewrite
    :refer [clause-source-symbol pattern-form]]
   [datalevin.query.plan :as qplan])
  (:import
   [datalevin.db DB]
   [datalevin.parser Pattern]
   [datalevin.storage Store]
   [java.nio.charset StandardCharsets]))

(def ^:private ^:const max-domain-size 128)
(def ^:private ^:const max-producer-rows 512)
(def ^:private ^:const max-path-length 4)
(def ^:private ^:const max-paths 64)
(def ^:private ^:const min-consumer-rows 4096)

(defn- index-value?
  "Limit literal domains to scalar keys whose bounded lookup does not fall
  back to a full giant-key collision scan."
  [x]
  (or (int? x) (boolean? x)
      (and (or (string? x) (keyword? x))
           (<= (alength (.getBytes ^String (str x) StandardCharsets/UTF_8))
               128))))

(defn- patterns
  [{:keys [parsed-q sources]}]
  (into []
        (keep
          (fn [[parsed form]]
            (when (and (instance? Pattern parsed) (vector? form))
              (let [src (clause-source-symbol (:source parsed))
                    source (get sources src)
                    [e a v :as pattern] (pattern-form form)
                    schema (when (and (instance? DB source)
                                      (instance? Store (:store source))
                                      (not (db/pending-tx-cache? source)))
                             (get (db/-schema source) a))]
                (when (and (= 3 (count pattern))
                           (qu/binding-var? e) (keyword? a)
                           (:db/aid schema) (not (:db/noindex schema))
                           (#{:db.type/long :db.type/ref :db.type/string
                              :db.type/keyword :db.type/boolean}
                             (:db/valueType schema))
                           (or (qu/binding-var? v) (index-value? v)))
                  {:source source :src src :entity e :attr a :value v
                   :value-type (:db/valueType schema)
                   :unique? (boolean (:db/unique schema))
                   :many? (= :db.cardinality/many (:db/cardinality schema))
                   :form form})))))
        (map vector (:qwhere parsed-q) (:qorig-where parsed-q))))

(defn- equality-classes
  "Normalize proof variables, without renaming query outputs or scoped forms."
  [patterns]
  (reduce
    (fn [classes group]
      (let [vars (mapv :value (filter #(qu/binding-var? (:value %)) group))
            members (into #{} (mapcat #(get classes % #{%})) vars)]
        (reduce #(assoc %1 %2 members) classes members)))
    {} (vals (group-by (juxt :src :entity :attr)
                       (remove :many? patterns)))))

(defn- representative [classes v]
  (if-let [members (classes v)] (first (sort members)) v))

(defn- paths-from
  [patterns classes start]
  (let [canon #(representative classes %)
        edges (mapcat
                (fn [{:keys [source entity attr value unique?] :as pattern}]
                  (when (qu/binding-var? value)
                    (cond->
                        [(assoc pattern :from (canon entity) :to (canon value)
                                :direction :eav)]
                      ;; A short path through a fact table is not a small
                      ;; dimension producer. Reject it before reading values.
                      ;; Counts only gate admission; bounded reads still prove
                      ;; completeness if statistics underestimate the fanout.
                      (or unique?
                          (<= (long (db/-count source [nil attr nil]))
                              max-producer-rows))
                      (conj (assoc pattern :from (canon value) :to (canon entity)
                                   :direction :ave)))))
                patterns)
        edges (group-by :from edges)
        start (canon start)]
    (loop [pending [[start []]] seen #{start} result []]
      (if (or (empty? pending) (<= max-paths (count result)))
        result
        (let [[v path] (first pending)
              next-edges (when (< (count path) max-path-length)
                           (->> (get edges v)
                                (remove #(seen (:to %)))
                                (group-by :to) vals (map first)
                                (sort-by :to)
                                (take (- max-paths (count result)))))
              next-paths (mapv #(vector (:to %) (conj path %)) next-edges)]
          (recur (into (subvec pending 1) next-paths)
                 (into seen (map first next-paths))
                 (into result next-paths)))))))

(defn- first-bound-index [trace var]
  (first (keep-indexed
           (fn [i node]
             (when (some? (qplan/find-index var
                                           (some-> node :steps last :cols))) i))
           trace)))

(defn candidates
  "Find short dimension paths whose key is available before a large expansion
  in the baseline plan. This discovery performs no domain materialization."
  [context]
  (let [patterns (patterns context)]
    (->> (for [[src ps] (group-by :src patterns)
               :let [classes (equality-classes ps)]
               trace (get-in context [:plan src])
               anchor ps
               :when (not (qu/binding-var? (:value anchor)))
               :let [anchor-idx (first-bound-index trace (:entity anchor))]
               :when (and anchor-idx (< 1 anchor-idx))
               [key path] (paths-from ps classes (:entity anchor))
               target ps
               :when (and (qu/binding-var? (:value target))
                          (= key (representative classes (:value target)))
                          (= (:value-type target)
                             (if (= :ave (:direction (peek path)))
                               :db.type/long (:value-type (peek path)))))
               :let [idx (first-bound-index trace (:value target))]
               :when (and idx (< (inc idx) anchor-idx)
                          (= idx (first-bound-index trace (:entity target))))
               :let [rows (long (or (:size (nth trace idx)) 0))
                     before (final-plan-cost (subvec (vec trace) 0 (inc idx)))
                     downstream (- (double (final-plan-cost
                                             (subvec (vec trace) 0 anchor-idx)))
                                   (double before))
                     expansion (reduce max 0 (map #(long (or (:size %) 0))
                                                  (subvec (vec trace)
                                                          (inc idx) anchor-idx)))]
               :when (and (<= min-consumer-rows rows)
                          (< rows expansion)
                          (< (* rows (double c/magic-cost-pred) 4.0)
                             downstream))]
           {:anchor anchor :target target :path path :input-rows rows
            :downstream-cost downstream
            :equality-vars (vec (sort (get classes (:value target)
                                          #{(:value target)})))})
         ;; Prefer a short proof over exploring a longer route through facts.
         (sort-by (juxt #(count (:path %)) #(- (:downstream-cost %)))))))

(defn- bounded-values
  [source direction attr inputs budget]
  (loop [inputs (seq inputs) values #{} rows 0 probes 0]
    (if (seq inputs)
      (let [input (first inputs)]
        (when (and (< rows budget) (index-value? input)
                   (or (= direction :ave)
                       (and (int? input) (not (neg? input)))))
          (let [limit (inc (min max-domain-size (- budget rows)))
                datoms (if (= direction :ave)
                         (db/-datoms source :ave attr input nil limit)
                         (db/-datoms source :eav input attr nil limit))
                n (count datoms)
                values (into values (map (if (= direction :ave) :e :v)) datoms)]
            (when (and (< n limit) (<= (count values) max-domain-size)
                       (every? index-value? values))
              (recur (next inputs) values (+ rows n) (inc probes))))))
      {:values (vec (sort values)) :rows rows :probes probes})))

(defn materialize
  "Produce a complete necessary domain within a fixed lookup/row budget.
  An overflowing prefix is discarded, never used as a membership restriction."
  [{:keys [anchor path] :as candidate}]
  (let [{:keys [source attr value]} anchor]
    (when-let [seed (bounded-values source :ave attr [value] max-producer-rows)]
      (loop [values (:values seed) steps path rows (:rows seed)
             cost (+ (double (estimate-link-cost (:probes seed) (:rows seed)))
                     (materialized-output-cost (:rows seed) 1))
             stages [(assoc (dissoc seed :values) :pattern (:form anchor))]]
        (if-let [{:keys [direction attr form]} (first steps)]
          (when-let [result (bounded-values source direction attr values
                                            (- max-producer-rows rows))]
            (recur (:values result) (next steps) (+ rows (:rows result))
                   (+ cost (double (estimate-link-cost
                                     (:probes result) (:rows result)))
                      (materialized-output-cost (:rows result) 1))
                   (conj stages (assoc (dissoc result :values) :pattern form))))
          (assoc candidate :values values :producer-cost cost
                 :producer-rows rows :stages stages))))))

(defn profitable?
  "Use existing attribute counts to price membership and downstream savings.
  Counts select an alternative; only complete index reads establish its domain."
  [{:keys [target values producer-cost downstream-cost input-rows]}]
  (let [{:keys [source attr]} target
        total (long (db/-count source [nil attr nil]))
        matched (reduce (fn [n value]
                          (+ n (long (db/-count source [nil attr value]))))
                        0 values)
        fraction (if (pos? total) (min 1.0 (/ (double matched) total)) 1.0)
        saving (* downstream-cost (- 1.0 fraction))
        cost (+ producer-cost (* input-rows (double c/magic-cost-pred)))]
    (< (* 2.0 cost) saving)))

(defn restrict-context
  [context {:keys [target values]}]
  (let [form [(list 'in (:value target) values)]]
    (-> context
        (update-in [:parsed-q :qorig-where] conj form)
        (update-in [:parsed-q :qwhere] into (dp/parse-where [form])))))
