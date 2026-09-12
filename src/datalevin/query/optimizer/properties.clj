;;
;; Copyright (c) Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns ^:no-doc datalevin.query.optimizer.properties
  "Physical properties and selection of conventional and access
  alternatives in the property memo."
  (:require
   [clojure.set :as set]
   [datalevin.constants :as c]
   [datalevin.query-util :as qu]
   [datalevin.query.access :as qaccess]
   [datalevin.query.optimizer.access-plan
    :refer [access-join-from-bound]]
   [datalevin.query.optimizer.estimates
    :refer [conventional-access-cost estimate-hash-join-cost
            estimate-link-cost estimate-round estimated-fragment-join-cost
            estimated-plan-size top-k-enforcer-cost]]
   [datalevin.query.optimizer.plan-cost
    :refer [estimated-late-cost estimated-plan-cost]]
   [datalevin.query.plan :as qplan]))

(defn- logical-plan-key
  [graph]
  (into #{}
        (mapcat (fn [[src nodes]]
                  (map #(vector src %) (keys nodes))))
        graph))

(defn- conventional-alternative
  [context logical-key demand access-plans]
  (let [plan-size  (estimated-plan-size context)
        base-cost  (estimated-plan-cost context)
        late       (estimated-late-cost context plan-size access-plans)
        size        (:output-size late)
        late-cost  (:cost late)
        access-cost (conventional-access-cost access-plans)
        effective-late-cost (max (double late-cost)
                                 (double access-cost))
        properties (qplan/->PhysicalProperties
                     (:ordering demand) false true :exact
                     #{:complete :top-k-enforced})
        enforcer-cost (top-k-enforcer-cost size demand)
        cost          (+ (double base-cost)
                         effective-late-cost
                         (double enforcer-cost))
        plan          (qplan/->ConventionalRootPlan
                        context properties cost size)]
    (assoc
      (qplan/->PlanAlternative
        :conventional logical-key properties plan
        cost size nil)
      :cost-breakdown {:base        base-cost
                       :late        late-cost
                       :access-expression access-cost
                       :effective-late effective-late-cost
                       :late-stages (:stages late)
                       :enforcer    enforcer-cost})))

(defn- root-access-properties
  [fragment-properties demand]
  (let [ordered? (seq (:ordering demand))]
    (qplan/->PhysicalProperties
      (when ordered? (:ordering demand))
      false
      true
      (:quality fragment-properties)
      (cond-> (set/difference (:capabilities fragment-properties)
                              qaccess/top-k-proof-capabilities)
        true     (conj :complete)
        ordered? (conj :top-k-enforced)))))

(defn- fragment-output-cols
  [step joins]
  (reduce
    (fn [cols join]
      (into cols (remove (set cols)) (:produces-cols join)))
    (vec (:cols step))
    joins))

(defn- access-alternative
  [logical-key fallback access-plan fragment]
  (let [{:keys [step demand work estimate
                correlated? outer-query outer-estimate]} access-plan
        fragment-plan (:plan fragment)
        fragment-properties (:properties fragment)
        joins       (:joins fragment-plan)
        operators   (:operators fragment-plan)
        fragment-cols (fragment-output-cols step joins)
        access-plan (assoc access-plan
                           :joins joins
                           :operators operators
                           :fragment-cols fragment-cols
                           :fragment-properties fragment-properties
                           :fragment-cost (:cost fragment)
                           :fragment-size (:size fragment))
        adaptive-top-k?
        (and (not correlated?)
             (qaccess/adaptive-top-k-properties?
               (:ordering fragment-properties)
               (:capabilities fragment-properties)
               demand))
        adaptive-limit?
        (and (not correlated?)
             (qaccess/adaptive-limit-properties?
               (:capabilities fragment-properties) demand))
        adaptive? (or adaptive-top-k? adaptive-limit?)
        per-open-cost
        (double
          (if adaptive?
            (:cost estimate)
            (or (:upper-cost estimate) (:cost estimate))))
        per-open-rows
        (long
          (or (if adaptive?
                (:rows estimate)
                (qaccess/estimate-output-rows estimate))
              0))
        outer-rows (long (if correlated?
                           (or (:rows outer-estimate) 1)
                           1))
        modeled-joins? (some #(= :indexed-join (:operation %))
                             (:join-stages estimate))
        all-joins      (or (:joins access-plan) [])
        candidate-rows per-open-rows
        unmodeled-join-cost
        (if modeled-joins?
          0.0
          (estimated-fragment-join-cost
            all-joins
            (repeat {:type :index-join})
            candidate-rows))
        selected-index-cost
        (estimated-fragment-join-cost
          joins
          (repeat {:type :index-join})
          candidate-rows)
        selected-physical-cost
        (estimated-fragment-join-cost joins operators candidate-rows)
        physical-adjustment (- (double selected-physical-cost)
                               (double selected-index-cost))
        access-cost
        (+ (double (if correlated? (or (:cost outer-estimate) 0.0) 0.0))
           (* (double outer-rows)
              (+ per-open-cost
                 (double unmodeled-join-cost)
                 physical-adjustment)))
        rows      (long (* outer-rows per-open-rows))
        properties (root-access-properties fragment-properties demand)
        estimated-size
        (estimate-round
          (* (double rows)
             (double (or (:yield estimate) 1.0))))
        size (max 0 (long estimated-size))
        enforcer-cost (top-k-enforcer-cost size demand)
        cost          (+ (double access-cost) (double enforcer-cost))
        plan (assoc
               (qplan/->AccessRootPlan
                 (cond
                   correlated?     :correlated-complete
                   adaptive-top-k? :adaptive-top-k
                   adaptive-limit? :adaptive-limit
                   :else           :complete)
                 step nil demand work properties
                 cost size fallback)
               :access-plan access-plan
               :outer-query outer-query
               :logical-key logical-key
               :joins joins
               :operators operators
               :fragment-cols fragment-cols
               :fragment-properties fragment-properties)]
    (assoc
      (qplan/->PlanAlternative
        :access logical-key properties plan cost size nil)
      :cost-breakdown
      {:point       (:point-cost estimate)
       :upper-bound (:upper-cost estimate)
       :required-count (:required-count demand)
       :outer       (when correlated? outer-estimate)
       :access      access-cost
       :fragment    (:cost fragment)
       :unmodeled-joins unmodeled-join-cost
       :physical-adjustment physical-adjustment
       :enforcer    enforcer-cost
       :selected    cost
       :stages      (:join-stages estimate)})))

(defn- quality-satisfies?
  [provided required]
  (or (nil? required)
      (= provided required)
      (and (= required :approximate) (= provided :exact))))

(defn- ordering-satisfies?
  [provided required]
  (let [provided (qu/ordering-terms provided)
        required (qu/ordering-terms required)]
    (or (empty? required)
        (and (<= (count required) (count provided))
             (= required (subvec provided 0 (count required)))))))

(defn properties-satisfy?
  "Return true when provided physical properties are a superset of required
   properties for one logical subset."
  [provided required]
  (and (ordering-satisfies? (:ordering provided) (:ordering required))
       (or (not (:resumable? required)) (:resumable? provided))
       (or (not (:complete? required)) (:complete? provided))
       (quality-satisfies? (:quality provided) (:quality required))
       (set/subset? (:capabilities required) (:capabilities provided))))

(defn- alternative-dominates?
  [left right]
  (and (<= (double (:cost left)) (double (:cost right)))
       (<= (long (:size left)) (long (:size right)))
       (properties-satisfy? (:properties left) (:properties right))))

(defn retain-property-alternative
  "Retain a bounded Pareto frontier for alternatives implementing one logical
   subset. Cheaper alternatives with a property superset dominate."
  [alternatives candidate]
  (if (some #(alternative-dominates? % candidate) alternatives)
    (vec alternatives)
    (conj
      (into []
            (remove #(alternative-dominates? candidate %))
            alternatives)
      candidate)))

(defn propagate-physical-properties
  "Transfer physical properties through an access-aware physical operator."
  [properties {:keys [type preserves-outer-order? ordering]}]
  (case type
    :filter properties

    :index-join
    (if preserves-outer-order?
      properties
      (assoc properties
             :ordering nil
             :resumable? false
             :capabilities
             (set/difference (:capabilities properties)
                             qaccess/top-k-proof-capabilities)))

    :hash-join
    (assoc properties
           :ordering nil
           :resumable? false
           :capabilities
           (set/difference (:capabilities properties)
                           qaccess/top-k-proof-capabilities))

    :sort
    (assoc properties
           :ordering ordering
           :resumable? false
           :capabilities
           (-> (:capabilities properties)
               (set/difference qaccess/top-k-proof-capabilities)
               (conj :top-k-enforced)))

    properties))

(defn alternative-satisfies?
  [{:keys [properties]} demand]
  (and (quality-satisfies? (:quality properties) (:quality demand))
       (or (:complete? properties)
           (and (= (:ordering properties) (:ordering demand))
                (set/subset? qaccess/top-k-proof-capabilities
                             (:capabilities properties))))))

(defn- choose-alternative
  [alternatives demand]
  (first
    (sort-by
      (juxt :cost #(if (= :conventional (:kind %)) 0 1))
      (filter #(alternative-satisfies? % demand) alternatives))))

(defn- access-fragment-properties
  [path]
  (qplan/->PhysicalProperties
    (:ordering path)
    (contains? (:capabilities path) :resumable)
    (contains? (:capabilities path) :complete)
    (:quality path)
    (:capabilities path)))

(defn- add-subset-alternative
  [subsets alternative]
  (update subsets (:logical-key alternative)
          #(retain-property-alternative (or % []) alternative)))

(defn- access-subset-alternatives
  [{:keys [access-id expr path demand estimate step joins join-candidates]}]
  (let [joins      (vec (or join-candidates joins))
        properties (access-fragment-properties path)
        source-key (set (:covers expr))
        adaptive?  (qaccess/adaptive-demand? path demand)
        estimated-source-size
        (long
          (if adaptive?
            (or (:rows estimate) 0)
            (qaccess/estimate-output-rows estimate)))
        source-size (max 0 estimated-source-size)
        source-scan-rows
        (long
          (if adaptive?
            (or (:remaining-point-scan-rows estimate)
                (:point-scan-rows estimate)
                source-size)
            (qaccess/estimate-scan-rows estimate)))
        source-cost
        (double
          (if (pos? source-scan-rows)
            (+ (double (or (:startup estimate) 0.0))
               (* (double source-scan-rows)
                  (double (or (:per-row estimate) 0.0))))
            0.0))
        source
        (qplan/->PlanAlternative
          :access-fragment source-key properties
          {:access-id access-id :source step :joins [] :operators []}
          source-cost source-size nil)]
    (loop [queue      [#{}]
           memo       {#{} [source]}
           expansions 0]
      (if (or (empty? queue)
              (>= (long expansions) (long c/plan-search-max)))
        (mapcat val memo)
        (let [used       (first queue)
              queue      (subvec queue 1)
              frontier   (get memo used)
              bound      (into (set (:cols step))
                               (mapcat :vars)
                               (map joins used))
              eligible
              (keep-indexed
                (fn [i candidate]
                  (when-not (contains? used i)
                    (when-let [join
                               (access-join-from-bound bound candidate)]
                      [i join])))
                joins)
              [memo queue]
              (reduce
                (fn [[memo queue] [i {:keys [estimate] :as join}]]
                  (let [used'       (conj used i)
                        logical-key
                        (into source-key
                              (map (comp :clause joins))
                              used')
                        candidates
                        (mapcat
                          (fn [{:keys [properties plan cost size]}]
                            (let [estimated-join-size
                                  (long (or (:rows estimate) size))
                                  join-size (max 0 estimated-join-size)
                                  index-op
                                  {:type :index-join
                                   :preserves-outer-order? true}
                                  hash-op {:type :hash-join}
                                  alternative
                                  (fn [operator operator-cost]
                                    (qplan/->PlanAlternative
                                      :access-fragment logical-key
                                      (propagate-physical-properties
                                        properties operator)
                                      (-> plan
                                          (update :joins conj join)
                                          (update :operators conj operator))
                                      (+ (double cost)
                                         (double operator-cost))
                                      join-size nil))]
                              [(alternative
                                 index-op
                                 (estimate-link-cost size join-size))
                               (alternative
                                 hash-op
                                 (estimate-hash-join-cost size join-size))]))
                          frontier)
                        previous (get memo used' [])
                        retained
                        (reduce retain-property-alternative
                                previous candidates)]
                    (if (= previous retained)
                      [memo queue]
                      [(assoc memo used' retained)
                       (conj queue used')])))
                [memo queue] eligible)]
          (recur queue memo (unchecked-inc expansions)))))))

(defn- access-subset-memo
  [access-plans]
  (reduce
    (fn [subsets alternative]
      (add-subset-alternative subsets alternative))
    {}
    (mapcat access-subset-alternatives access-plans)))

(defn- executable-access-fragments
  [subsets {:keys [access-id] :as access-plan}]
  (let [source-key (set (get-in access-plan [:expr :covers]))]
    (->> subsets
         (mapcat val)
         (filter #(and (= access-id (get-in % [:plan :access-id]))
                       (set/subset? source-key (:logical-key %))))
         ;; Prefer a more complete physical fragment when costs and properties
         ;; tie, while still retaining executable roots for smaller subsets.
         (sort-by #(count (:logical-key %)) >)
         vec)))

(defn build-property-memo
  "For access queries, retain conventional and physical access alternatives
   under one logical root until the query's ordering/quality demand is applied."
  [{:keys [access-plans access-demand graph] :as context}]
  (if (seq access-plans)
    (let [logical-key  (logical-plan-key graph)
          demand       (or access-demand (:demand (first access-plans)))
          executable   (into []
                             (comp
                               (filter #(and (:step %)
                                             (not (:unavailable? %))))
                               (map-indexed #(assoc %2 :access-id %1)))
                             access-plans)
          conventional (conventional-alternative
                         context logical-key demand access-plans)
          subsets      (access-subset-memo executable)
          alternatives
          (into [conventional]
                (mapcat
                  (fn [access-plan]
                    (map #(access-alternative
                            logical-key (:plan conventional) access-plan %)
                         (executable-access-fragments subsets access-plan))))
                executable)
          selected     (choose-alternative alternatives demand)]
      (assoc context
             :access-plans executable
             :property-memo
             (qplan/->PropertyMemo
               logical-key demand alternatives selected subsets)))
    context))

(defn selected-alternative
  [context]
  (get-in context [:property-memo :selected]))

(defn property-memo-summary
  [{:keys [logical-key demand alternatives selected subsets]}]
  (let [summary (fn [{:keys [kind properties cost size cost-breakdown plan]}]
                  {:kind       kind
                   :properties properties
                   :fragment-properties (:fragment-properties plan)
                   :cost       cost
                   :size       size
                   :mode       (:mode plan)
                   :operators  (:operators plan)
                   :fragment-cols (:fragment-cols plan)
                   :cost-breakdown cost-breakdown})]
    {:logical-key  logical-key
     :demand       demand
     :alternatives (mapv summary alternatives)
     :selected     (some-> selected summary)
     :subsets
     (into {}
           (map (fn [[logical-key alternatives]]
                  [logical-key (mapv summary alternatives)]))
           subsets)}))
