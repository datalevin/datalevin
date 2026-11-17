(ns datalevin.query.source-key-qualification-test
  (:require [clojure.test :refer [deftest is]]
            [datalevin.core :as d]
            [datalevin.query.source :as source]
            [datalevin.query.source.join-cost :as cost]
            [datalevin.query.source-union-selection-test :as fixture]))

(def query
  '{:find [?e ?v ?endpoint] :in [$seed $a $b]
    :where [(or-join [?e ?v ?endpoint]
              (and [$a ?e :row/value ?v] [(ground 0) ?endpoint])
              (and [$b ?e :row/value ?v] [(ground 1) ?endpoint]))
            [$seed ?e :row/value "anchor"]]})

(defn- seed-provider [config]
  (let [p (fixture/provider [[1 "anchor"]] config)
        delegate (:source p)]
    (assoc p :source
      (reify source/IQuerySource
        (-source-info [_] (source/-source-info delegate))
        (-open-session [_ request] (source/-open-session delegate request))
        (-plan-source [_ session request]
          (update (source/-plan-source delegate session request) :alternatives
            #(mapv (fn [alternative]
                     ;; Keep the independent constant lookup cheaper in rows,
                     ;; reproducing the unpriced shared-key join in CH5.
                     (if (some? (get-in alternative [:estimate :rows-per-binding]))
                       (assoc-in alternative [:estimate :rows-per-binding] 2)
                       alternative)) %)))))))

(defn- providers [seed-config branch-config]
  [(seed-provider seed-config)
   (fixture/provider [[1 "v1"] [2 "v2"]] branch-config)
   (fixture/provider [[1 "v1"] [2 "v2"]] branch-config)])

(defn- explain [q ps]
  (apply d/explain {:run? true :intermediate-counts? true} q (mapv :source ps)))

(defn- root [result]
  (first (filter #(zero? (:region-id %)) (:regions result))))

(defn- closed! [ps]
  (doseq [p ps]
    (is (= 1 (count (fixture/events p :plan)))))
  (doseq [p ps]
    (is (= (count (fixture/events p :acquire))
           (count (fixture/events p :close-session))))))

(deftest an-unpriced-flat-union-baseline-can-recover-from-a-small-key-lookup
  (let [ps (providers {} {})
        result (explain query ps)
        region (root result)]
    (is (= 2 (:result-rows result)))
    (is (true? (get-in region [:union-selection :baseline-unpriced?])))
    (is (number? (get-in region [:union-selection :selected-cost :estimated-ns])))
    (is (some seq (vals (get-in region [:union-selection :bindings]))))
    (closed! ps)))

(deftest recovery-keeps-an-opaque-mandatory-qualification-in-the-original-order
  (let [q (-> query
              (assoc :find '[?e ?v ?endpoint ?qual]
                     :in '[$seed $a $b $left $right])
              (update :where #(into [(first %)
                                    '(or-join [?qual]
                                       (and [$left ?l :row/value ?qual] [(= ?qual "allowed")])
                                       (and [$right ?r :row/value ?qual] [(= ?qual "also-allowed")]))]
                                   (rest %))))
        ps (into (providers {} {}) [(fixture/provider [[10 "blocked"]] {})
                                   (fixture/provider [[11 "blocked"]] {})])
        result (explain q ps)]
    (is (= 0 (:result-rows result)))
    (is (nil? (:union-selection (root result))))
    (is (empty? (fixture/events (first ps) :open)))
    (closed! ps)))

(deftest incomplete-estimates-and-one-shot-branches-retain-their-fallback
  (doseq [[seed-config branch-config] [[{:known? false} {}] [{} {:opens :once}]]]
    (let [ps (providers seed-config branch-config)
          result (explain query ps)]
      (is (= 2 (:result-rows result)))
      (is (nil? (:union-selection (root result))))
      (closed! ps))))

(defn- choice [id requires outputs rows]
  (let [columns #(mapv (fn [v] {:variable v :type :string :nullable? false}) %)]
    {:kind :access :value {:id id :rows rows} :covers #{id}
     :requires (columns requires) :columns (columns outputs) :after #{} :resources #{}}))

(deftest a-small-bound-key-check-precedes-an-expanding-edge-despite-fanout-above-one
  (let [seed (choice :seed [] '[?e] 1)
        expand (choice :expand '[?e] '[?e ?k] 100)
        check (choice :check '[?e] '[?e] 2)
        payload (choice :payload '[?e] '[?e ?value] 2)
        result (cost/select {:inputs [] :required-work #{:seed :expand :check :payload}
                             :required-vars #{'?k '?value}
                             :choices (into [seed expand check payload]
                                            (repeat 30 (assoc seed :unavailable? true)))
                             :check! (fn []) :reserve! (fn [_]) :max-states 4096} :rows)
        selected (mapv (comp :id :value) (:choices result))]
    (is (= :complete (:status result)))
    (is (= [:seed :check :expand :payload] selected))
    (is (= 400.0 (:tuples (cost/price (:choices result) [] :rows))))))
