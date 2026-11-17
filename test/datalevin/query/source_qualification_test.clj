(ns datalevin.query.source-qualification-test
  (:require [clojure.test :refer [deftest is]]
            [datalevin.core :as d]
            [datalevin.query.access.dependency :as dependency]
            [datalevin.query.source.join-cost :as cost]
            [datalevin.query.source-union-selection-test :as fixture]))

(defn- column [v] {:variable v :type :string :nullable? false})

(defn- choice [id requires columns rows]
  {:kind :access :value {:id id :rows rows} :covers #{id}
   :requires (mapv column requires) :columns (mapv column columns)
   :after #{} :resources #{}})

(deftest alternative-probes-do-not-consume-admitted-state-budget
  (let [a (choice :a [] ['?a] 10)
        b (choice :b [] ['?b] 1)
        bound-a (choice :a ['?b] ['?a] 0.1)
        unavailable (repeat 300 (assoc a :unavailable? true))
        checks (volatile! 0)
        result (cost/select {:inputs [] :required-work #{:a :b} :required-vars #{'?a '?b}
                             :choices (vec (concat [a] unavailable [b bound-a]))
                             :check! #(vswap! checks inc) :reserve! (fn [_]) :max-states 64}
                            :rows)]
    (is (= :complete (:status result)))
    (is (= [b bound-a] (:choices result)))
    (is (:cost-changed? result))
    (is (< (get-in result [:cost-search :states]) 64))
    (is (> @checks 300))
    (is (< (get-in result [:join-cost :peak-rows]) 10))))

(deftest admitted-state-accounting-keeps-cancellation-checks
  (let [seen (volatile! 0) checks (volatile! 0)
        a (choice :a [] ['?a] 1)
        b (choice :b ['?a] ['?b] 1)
        result (dependency/schedule {:inputs [] :required-work #{:a :b} :required-vars #{'?b}
                                     :choices [b a] :max-states 3
                                     :reserve! (fn [_]) :check! #(vswap! checks inc)
                                     :visit! #(vswap! seen inc)})]
    (is (= :complete (:status result)))
    (is (= [a b] (:choices result)))
    (is (= 3 @seen))
    (is (> @checks @seen)))
  (let [failure (ex-info "Cancelled" {:error :query-source/cancelled})]
    (is (identical? failure
          (try (cost/select {:inputs [] :required-work #{} :required-vars #{} :choices []
                             :check! #(throw failure) :reserve! (fn [_]) :max-states 1} :rows)
               (catch Exception e e))))))

(defn- risk-graph []
  (let [choices [(choice :country [] '[?country] 10)
                 (choice :broad '[?country] '[?country ?g] 1000000)
                 (choice :bridge '[?country] '[?country ?drug] 1)
                 (choice :drug '[?drug] '[?drug] 1)
                 (choice :qual '[?drug] '[?drug ?payload] 0.1)
                 (choice :leaf '[?drug] '[?drug ?e] 1)
                 (choice :drug [] '[?drug] 2)
                 (choice :bridge '[?drug] '[?country ?drug] 100)
                 (choice :country '[?country] '[?country] 1)]]
    {:inputs [] :required-work #{:country :broad :bridge :drug :qual :leaf}
     :required-vars #{'?g '?e '?payload}
     ;; A realistic alternative count admits the regular search allowance.
     :choices (into choices (repeat 25 (assoc (first choices) :unavailable? true)))
     :check! (fn []) :reserve! (fn [_]) :max-states 4096}))

(deftest over-capacity-estimates-can-prefer-a-cheap-mandatory-qualification
  (let [g (risk-graph)
        ordinary (cost/select g :rows)
        qualified (cost/select (assoc g :intermediate-row-limit 1000) :rows)]
    (is (= :complete (:status qualified)))
    (is (= :country (get-in ordinary [:choices 0 :value :id])))
    (is (= :drug (get-in qualified [:choices 0 :value :id])))
    (is (> (get-in qualified [:join-cost :work-rows])
           (get-in ordinary [:join-cost :work-rows])))
    (is (< (get-in qualified [:cost-search :qualification-prefix :peak-rows]) 1000))
    (is (= (:required-work g) (into #{} (mapcat :covers) (:choices qualified))))))

(deftest plans-within-estimated-capacity-keep-full-cost-ranking
  (let [result (cost/select (assoc (risk-graph) :intermediate-row-limit 20000000) :rows)]
    (is (= :country (get-in result [:choices 0 :value :id])))
    (is (< (get-in result [:join-cost :peak-rows]) 20000000))
    (is (nil? (get-in result [:cost-search :qualification-prefix])))))

(deftest qualification-search-does-not-cross-an-opaque-cost-fence
  (let [fence {:kind :core :value {:cost-barrier? true} :covers #{:opaque}
               :requires [] :columns [] :after #{:country} :resources #{}}
        g (-> (risk-graph)
              (assoc :intermediate-row-limit 1000)
              (update :required-work conj :opaque)
              (update :choices #(into [fence] %)))
        result (cost/select g :rows)]
    (is (= :complete (:status result)))
    (is (= [:country :opaque] (mapv #(first (:covers %)) (take 2 (:choices result)))))
    (is (nil? (:join-cost result)))
    (is (= (:required-work g) (into #{} (mapcat :covers) (:choices result))))))

(deftest qualification-preserves-nonempty-results-union-witnesses-and-multiplicity
  (let [q '{:find [?patient ?drug ?endpoint ?g ?name]
            :in [$clinical $a $b $iupac $generic $country $broad]
            :query-source {:limits {:retained-bytes 67108864}}
            :where [[$clinical ?patient :row/value ?drug]
                    (or-join [?compound ?drug ?endpoint]
                      (and [$a ?compound :row/value ?drug] [(ground 0) ?endpoint])
                      (and [$b ?compound :row/value ?drug] [(ground 1) ?endpoint]))
                    [$iupac ?compound :row/value ?name]
                    [$generic ?bank :row/value ?drug]
                    [$country ?patient :row/value ?country]
                    [$broad ?g :row/value ?country]]}
        broad (mapv #(vector % "US") (range 20))
        rows [[[1 "alpha"] [2 "alpha"]]
              [[10 "alpha"]] [[10 "alpha"]]
              [[10 "iupac-1"] [10 "iupac-2"]]
              [[100 "alpha"]]
              [[1 "US"] [2 "US"]] broad]
        ps (mapv fixture/provider rows
                 [{} {} {} {:estimated-rows 0} {} {} {:estimated-rows 1000000000}])
        inputs (mapv :source ps)
        expected (set (for [patient [1 2] endpoint [0 1] g (range 20) name ["iupac-1" "iupac-2"]]
                        [patient "alpha" endpoint g name]))]
    ;; The estimated-zero leaf has two real values: neither may be skipped.
    (is (= expected (set (apply d/q q inputs))))
    (is (= 160 (apply d/q (assoc q :find '[(count ?g) .]
                                 :with '[?patient ?endpoint ?name]) inputs)))
    (doseq [p ps]
      (is (= (count (fixture/events p :acquire))
             (count (fixture/events p :close-session)))))))
