;;
;; Copyright (c) Huahai Yang, Nikita Prokopov. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns ^:no-doc datalevin.query.optimizer.estimates
  "Auto-split from datalevin.query-optimizer."
  (:require
   [clojure.set :as set]
   [clojure.core.reducers :as rd]
   [clojure.walk :as w]
   [datalevin.constants :as c]
   [datalevin.datom :as dd]
   [datalevin.db :as db]
   [datalevin.interface :refer [av-size populated?]]
   [datalevin.join :as j]
   [datalevin.lmdb :as l]
   [datalevin.parser :as dp]
   [datalevin.query.optimizer.graph :as qog]
   [datalevin.pipe :as p]
   [datalevin.query.access :as qaccess]
   [datalevin.query.optimizer.range :as qor]
   [datalevin.query.predicate :as qpred]
   [datalevin.query.plan :as qplan]
   [datalevin.query.resolve :as qresolve]
   [datalevin.query-util :as qu]
   [datalevin.relation :as r]
   [datalevin.rules :as rules]
   [datalevin.util :as u :refer [cond+ raise concatv map+]])
  (:import
   [java.util HashMap HashSet IdentityHashMap List]
   [java.util.concurrent ConcurrentHashMap]
   [datalevin.db DB]
   [datalevin.storage Store]
   [datalevin.utl DPKey LRUCache]
   [datalevin.parser And BindColl BindScalar BindTuple Constant
    DefaultSrc Function Or Variable Pattern Predicate Not RuleExpr]
   [org.eclipse.collections.impl.list.mutable FastList]))

(declare estimate-hash-join-cost estimate-link-cost)

(def ^:const ^long selective-rule-anchor-min-plan-size
  1000000)

(def ^:const ^long selective-rule-anchor-max-inline-size
  1000000)

(def ^:const ^long selective-rule-anchor-max-rule-size
  1000000)

(def ^:const ^long selective-rule-anchor-max-output-size
  2000000)

(defn variable-ref-count
  ^long [form sym]
  (let [n (volatile! 0)]
    (w/postwalk
      (fn [e]
        (when (and (instance? Variable e)
                   (= sym (:symbol e)))
          (vswap! n (fn [^long x] (unchecked-inc x))))
        e)
      form)
    (long @n)))

(defn relation-size
  ^long [rel]
  (if-some [^List tuples (:tuples rel)]
    (.size tuples)
    0))

(defn materialized-output-cost
  ^double [^long tuple-count ^long width]
  (* (double tuple-count)
     (+ (double c/magic-cost-hash-join-output-tuple)
        (* (double c/magic-cost-hash-join-output-cell)
           (double width)))))

(defn- collapse-rels-with-cost
  [rels new-rel]
  (loop [rels          rels
         new-rel       new-rel
         new-rel-attrs (:attrs new-rel)
         cost          0.0
         acc           (transient [])]
    (if-some [rel (first rels)]
      (if (not-empty (qu/intersect-keys new-rel-attrs (:attrs rel)))
        (let [joined    (j/hash-join rel new-rel)
              join-cost (estimate-hash-join-cost
                          (relation-size rel)
                          (relation-size new-rel)
                          (relation-size joined)
                          (count (:attrs joined)))]
          (recur (next rels) joined (:attrs joined)
                 (+ cost (double join-cost)) acc))
        (recur (next rels) new-rel new-rel-attrs cost (conj! acc rel)))
      [(persistent! (conj! acc new-rel)) cost])))

(defn materialize-pattern-with-cost
  [context source pattern ^long probe-count]
  (let [rel             (qresolve/lookup-pattern
                          (assoc context :rels-bound-cache (volatile! {}))
                          source pattern)
        tuple-count     (relation-size rel)
        lookup-cost     (estimate-link-cost probe-count tuple-count)
        output-cost     (materialized-output-cost
                          tuple-count (count (:attrs rel)))
        [rels join-cost] (collapse-rels-with-cost (:rels context) rel)]
    {:context (assoc context :rels rels)
     :cost    (+ (double lookup-cost) (double output-cost) (double join-cost))
     :stage   {:pattern      pattern
               :probes       probe-count
               :lookup-rows  tuple-count
               :lookup-cost  lookup-cost
               :output-cost  output-cost
               :join-cost    join-cost}}))

(defn- projected-bound-lookup-cost
  ^double [source attr ^long probe-count ^long output-count]
  (if (= 1 probe-count)
    (double (estimate-link-cost probe-count output-count))
    (let [scan-count (long (db/-count source [nil attr nil]))
          multi-cost (+ (* (double probe-count)
                           (double c/magic-cost-link-probe))
                        (* (double output-count)
                           (double c/magic-cost-link-retrieval))
                        (* (+ (double probe-count) (double output-count))
                           (double c/magic-cost-hash-join)))
          full-cost  (+ (* (double scan-count)
                           (double c/magic-cost-init-scan-e))
                        (* (+ (double probe-count) (double scan-count))
                           (double c/magic-cost-hash-join)))]
      (min multi-cost full-cost))))

(defn projected-pattern-materialization-cost
  ^double [source attr ^long probe-count ^long output-count]
  (+ (projected-bound-lookup-cost
       source attr probe-count output-count)
     ;; At least the newly produced entity/value column must be allocated.
     (materialized-output-cost output-count 1)))

(defn cap-output-count
  ^long [^long count ^long cap]
  (min count (unchecked-inc cap)))

(defn estimate-round ^long [x]
  (let [v (Math/ceil (double x))]
    (if (>= v (double Long/MAX_VALUE))
      Long/MAX_VALUE
      (long v))))

(def verified-non-empty-size
  (inc (long c/init-exec-size-threshold)))

(defn over-count-cap
  ^long [^long cap]
  (if (= cap Long/MAX_VALUE) Long/MAX_VALUE (unchecked-inc cap)))

(defn capped-tuple-count-sum
  ^long [^List tuples count-tuple ^long cap]
  (loop [i     0
         total 0]
    (if (< i (.size tuples))
      (let [n (max 0 (long (count-tuple (.get tuples i))))]
        (if (< (- cap total) n)
          (over-count-cap cap)
          (recur (unchecked-inc-int i) (+ total n))))
      total)))

(defn count-only-output-cap
  ^long [^double remaining-budget ^long input-size]
  (let [probe-cost (* (double input-size)
                      (double c/magic-cost-link-probe))
        available  (- remaining-budget probe-cost)]
    (if-not (pos? available)
      0
      (long
        (min (double (dec Long/MAX_VALUE))
             (Math/floor
               (/ available (double c/magic-cost-link-retrieval))))))))

(defn access-variable-domain-estimates
  [step join-candidates ^long range-rows]
  (let [access-var (first (:cols step))]
    (reduce
      (fn [domains {:keys [entity-var rows]}]
        (let [rows (long rows)]
          ;; The access variable already has the source range as its domain.
          ;; A residual attribute count is selectivity evidence, not a smaller
          ;; replacement domain for that same variable.
          (if (and entity-var
                   (not= access-var entity-var)
                   (pos? rows)
                   (< rows Long/MAX_VALUE))
            (update domains entity-var
                    (fn [current]
                      (if current (min (long current) rows) rows)))
            domains)))
      (if access-var {access-var range-rows} {})
      join-candidates)))

(defn scaled-row-estimate
  ^long [^long input-size ^double ratio]
  (if (or (zero? input-size) (not (pos? ratio)))
    0
    (long
      (min (double Long/MAX_VALUE)
           (Math/ceil (* (double input-size) ratio))))))

(defn estimate-row-evaluation-cost
  ^long
  [^long input-size]
  (estimate-round
    (* (double input-size) (double c/magic-cost-pred))))

(defn n-costly-preds
  [attrs-v]
  (reduce
    (fn [^long c [_ {:keys [pred range-pred?]}]]
      (if (and pred (not range-pred?)) (inc c) c))
    0 attrs-v))

(defn final-plan-size ^long
  [plan-trace]
  (if-let [{:keys [steps size]} (last plan-trace)]
    (long (or size
              (some-> steps first :mcount)
              1))
    0))

(defn estimated-plan-size
  [{:keys [plan result-set]}]
  (if (= result-set #{})
    0
    (reduce
      (fn [size [_src components]]
        (let [component-size
              (reduce
                (fn [size plan-trace]
                  (let [n (final-plan-size plan-trace)]
                    (if (zero? (long size))
                      n
                      (estimate-round (* (double size) n)))))
                0 components)]
          (if (zero? (long size))
            component-size
            (estimate-round (* (double size) (long component-size))))))
      0 plan)))

(defn top-k-enforcer-cost
  [^long rows demand]
  (if (and (pos? rows) (seq (:ordering demand)))
    (let [required-count (long (or (:required-count demand) rows))
          required (max 1 required-count)
          retained (long (max 2 (min rows required)))]
      (* (double rows)
         (/ (Math/log (double retained)) (Math/log 2.0))))
    0.0))

(defn selective-anchor-cost
  ^double [^long fanout]
  (+ (double (estimate-link-cost 1 fanout))
     (materialized-output-cost fanout 1)))

(defn reset-cost-planning
  [context]
  (-> context
      (assoc :opt-clauses nil
             :late-clauses nil
             :optimizable-or-joins nil
             :graph nil
             :plan nil
             :result-set nil)
      (dissoc :optimizable-not-joins
              :deferred-base-samples
              :attribute-group-planning
              ::selective-preplanned?)))

(defn conventional-access-cost
  [access-plans]
  (reduce
    +
    0.0
    (vals
      (reduce
        (fn [costs {:keys [expr estimate]}]
          (if-some [cost (:conventional-cost estimate)]
            (let [logical-key (or (:covered-originals expr)
                                  (:covers expr))]
              (update costs logical-key
                      (fn [previous]
                        (if (some? previous)
                          (min (double previous) (double cost))
                          (double cost)))))
            costs))
        {}
        access-plans))))

(defn estimated-fragment-join-cost
  [joins operators initial-size]
  (first
    (reduce
      (fn [[cost size] [join operator]]
        (let [join-size     (max 0 (long (or (get-in join [:estimate :rows])
                                             size)))
              operator-cost (case (:type operator)
                              :hash-join
                              (estimate-hash-join-cost size join-size)

                              :index-join
                              (estimate-link-cost size join-size)

                              0.0)]
          [(+ (double cost) (double operator-cost)) join-size]))
      [0.0 (max 0 (long initial-size))]
      (map vector joins operators))))

(defn recount-node
  [{:keys [bound free] :as node}]
  (let [clauses (concat (map-indexed (fn [i clause] [:bound i clause]) bound)
                        (map-indexed (fn [i clause] [:free i clause]) free))]
    (if-let [[k i clause]
             (when (seq clauses)
               (apply min-key (fn [[_ _ clause]]
                                (long (or (:count clause) Long/MAX_VALUE)))
                      clauses))]
      (assoc node :mpath [k i]
                  :mcount (long (or (:count clause) Long/MAX_VALUE)))
      (assoc node :mpath nil :mcount Long/MAX_VALUE))))

(defn count-init-follows
  [^DB db tuples attr index]
  (let [store (.-store db)]
    (rd/fold
      +
      (rd/map #(av-size store attr (aget ^objects % index))
              (p/remove-end-scan tuples)))))

(defn- count-init-follows-summary
  [^DB db tuples attr index]
  (let [store    (.-store db)
        ^List ts (p/remove-end-scan tuples)
        n        (.size ts)]
    (loop [i   0
           sum 0.0]
      (if (< i n)
        (let [^objects t (.get ts i)]
          (recur (u/long-inc i)
                 (+ sum
                    (double (av-size store attr (aget t index))))))
        {:n n
         :sum sum}))))

(defn count-init-follows-summary-cached
  "Cache count and sum by sample identity, without hashing the sample list."
  [^DB db ^IdentityHashMap cache tuples attr index]
  (let [^HashMap entries (or (.get cache tuples)
                             (let [m (HashMap.)]
                               (.put cache tuples m)
                               m))
        k                [::link-follow-summary attr index]]
    (if (.containsKey entries k)
      (.get entries k)
      (let [summary (count-init-follows-summary db tuples attr index)]
        (.put entries k summary)
        summary))))

(defn count-or-join-follows
  "Count the linked output of an or-join sample."
  [db sources rules ^IdentityHashMap build-cache tuples
   {:keys [clause bound-var free-vars tgt-attr]} bound-idx]
  (qresolve/or-join-count-built
    db tuples bound-idx tgt-attr
    (qresolve/or-join-build-cached build-cache sources rules tuples clause
                                   bound-var bound-idx free-vars)))

(defn estimate-link-cost
  [^long outer-size ^long result-size]
  (estimate-round
    (+ (* outer-size ^double c/magic-cost-link-probe)
       (* result-size ^double c/magic-cost-link-retrieval))))

(defn estimate-hash-join-cost
  "Price a hash join by its dominating input or output work. The legacy input
  coefficient is a monolithic operator estimate, so adding ordinary output
  work would double count it. A many-to-many join can emit enough tuples that
  allocation and column copies dominate that estimate, however."
  ([^long left-size ^long right-size]
   (estimate-round (* ^double c/magic-cost-hash-join
                      (+ left-size right-size))))
  ([^long left-size ^long right-size ^long result-size ^long output-width]
   (let [input-cost  (* ^double c/magic-cost-hash-join
                        (+ left-size right-size))
         output-cost (* result-size
                        (+ ^double c/magic-cost-hash-join-output-tuple
                           (* ^double c/magic-cost-hash-join-output-cell
                              output-width)))]
     (estimate-round
       (max input-cost output-cost)))))

(defn incoming-link-counts
  [nodes]
  (persistent!
    (reduce-kv
      (fn [counts _ {:keys [links]}]
        (reduce (fn [counts {:keys [tgt]}]
                  (assoc! counts tgt
                          (inc (long (get counts tgt 0)))))
                counts links))
      (transient {}) nodes)))
