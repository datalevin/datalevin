(ns datalevin.query.source-smoke-test
  (:require [clojure.test :refer [deftest is]]
            [datalevin.core :as d]
            [datalevin.query.source :as source]
            [datalevin.query.source.fake :as fake]))

(def ^:private row-info (fake/row-info))

(defn- events [provider] (mapv first @(:events provider)))
(defn- pair-source [info rows]
  (fake/source info (fake/row-facts rows) {:mode :scan}))

(def pairs-query '[:find ?id ?name :in $s :where [$s ?id :row/value ?name]])

(deftest query-entry-and-fresh-invocations
  (let [rows [[1 "Ada"] [2 nil] [3 "Ada"] [3 "Ada"]]
        s (pair-source row-info rows)]
    (is (= #{[1 "Ada"] [3 "Ada"]} (d/q pairs-query s)))
    (is (= [1 3] (sort (d/q '[:find [?id ...] :in $s :where [$s ?id :row/value _]] s))))
    (is (= 2 (d/q '[:find (count ?id) . :in $s :where [$s ?id :row/value _]] s)))
    (is (= 1 (d/q '[:find (count ?name) . :in $s :where [$s _ :row/value ?name]] s)))
    (is (= 2 (d/q '[:find (count ?name) . :with ?id :in $s :where [$s ?id :row/value ?name]] s)))
    (is (= #{} (d/q '[:find ?id ?name :in $s ?id :where [$s ?id :row/value ?name]] s 2)))
    (fake/replace-facts! s (fake/row-facts [[4 "fresh"]]))
    (is (= #{[4 "fresh"]} (d/q pairs-query s)))
    (is (= 7 (count (filter #{:acquire} (events s)))))))

(deftest aliases-share-a-view
  (let [s (pair-source row-info [[1 "a"] [2 "b"]])
        query '[:find ?id ?name :in $a $b :where [$a ?id :row/value ?name] [$b ?id :row/value ?name]]]
    (is (= #{[1 "a"] [2 "b"]} (d/q query s s)))
    (is (= 1 (count (filter #{:acquire} (events s)))))
    (is (= 2 (count (filter #{:plan} (events s)))))
    (is (= [:close-cursor :close-session] (take-last 2 (events s))))))

(deftest obsolete-positional-descriptors-reject-before-acquisition
  (let [events (atom [])
        provider (reify source/IQuerySource
                   (-source-info [_] (assoc row-info :spi-versions #{1}))
                   (-open-session [_ _] (swap! events conj :acquire))
                   (-plan-source [_ _ _] (swap! events conj :plan)))]
    (is (= :query-source/version
           (try (d/q pairs-query provider) nil
                (catch clojure.lang.ExceptionInfo e (:error (ex-data e))))))
    (is (empty? @events))))

(deftest nested-source-expressions-retain-binding-semantics
  (let [s (pair-source row-info [[1 "Ada"] [2 "Grace"] [3 "Lin"]])]
    (is (= #{[1 true] [2 false] [3 true]}
           (d/q '[:find ?id ?match :in $s :where [$s ?id :row/value ?name]
                  [(or (= ?name "Ada") (= ?name "Lin")) ?match]] s)))
    (is (= #{[2 12] [3 15]}
           (d/q '[:find ?id ?total :in $s :where [$s ?id :row/value ?name]
                  [(or (= ?id 2) (= ?id 3))]
                  [(* (+ ?id 2) 3) ?total]] s)))))

(deftest grouped-query-keeps-witnesses-and-joins-composite-keys
  (let [s (pair-source row-info [[1 "A"] [2 "B"]])
        lines [[10 :part 1] [10 :supplier "A"] [10 :quantity 3]
               [11 :part 1] [11 :supplier "A"] [11 :quantity 3]
               [12 :part 2] [12 :supplier "A"] [12 :quantity 8]
               [13 :part 1] [13 :supplier "B"] [13 :quantity 9]]
        query '[:find ?id ?name ?total :in $s $lines :where
                [$s ?id :row/value ?name]
                [(q [:find ?p ?s (sum ?n) :with ?id :in $facts :where
                     [$facts ?id :part ?p] [$facts ?id :supplier ?s]
                     [$facts ?id :quantity ?n]] $lines) [[?id ?name ?total]]]
                :query-source {:limits {:result-rows 1}}]]
    (is (= #{[1 "A" 6]} (d/q query s lines)))
    (let [only-total (into '[:find ?total] (drop-while #(not= :in %) query))]
      (is (= #{[6]} (d/q only-total s lines))))
    (is (= :query-source/limit
           (try (d/q (assoc-in query [(dec (count query)) :limits :result-rows] 0) s lines) nil
                (catch clojure.lang.ExceptionInfo e (:error (ex-data e))))))
    (is (= :close-session (last (events s))))
    (is (= #{} (d/q query s [])))))

(deftest grouped-query-shares-the-source-session
  (let [s (pair-source row-info [[1 "a"] [2 "a"] [3 "b"]])
        q '[:find ?id ?n :in $s :where
            [(q [:find ?v (count ?e) :where [?e :row/value ?v]] $s) [[?v ?n]]]
            [$s ?id :row/value ?v]]]
    (d/explain {:run? false} q s)
    (is (= 1 (count (filter #{:acquire} (events s)))))
    (is (empty? (filter #{:open} (events s))))
    (is (= #{[1 2] [2 2] [3 1]} (d/q q s)))
    (is (= 2 (count (filter #{:close-session} (events s)))))))

(defn- priced-facts [info facts scan-rows lookup-rows]
  (let [s (fake/source info facts {:mode :both})]
    (reify source/IQuerySource
      (-source-info [_] (source/-source-info s))
      (-open-session [_ request] (source/-open-session s request))
      (-plan-source [_ session request]
        (update (source/-plan-source s session request) :alternatives
          (fn [alternatives]
            (mapv (fn [alt]
                    (let [bound? (seq (:requires alt))]
                      (update alt :estimate assoc
                        (if bound? :rows-per-binding :rows) (if bound? lookup-rows scan-rows)
                        :confidence :high :provenance {:kind :statistics :id nil
                                                       :observed-at-ms (System/currentTimeMillis)
                                                       :valid-for-ms nil}))) alternatives)))))))

(deftest grouped-lookup-follows-cost-and-preserves-witnesses
  (let [q '[:find ?key ?n :in $parts $lines :where
            [$parts ?key :row/value "selected"]
            [(q [:find (count ?p) ?p :with ?w :in $facts :where
                 [$facts ?w :line/part ?p]] $lines) [[?n ?key]]]]
        parts (priced-facts row-info [[1 :row/value "selected"] [2 :row/value "other"]] 1 1)
        info (fake/info {:line/part {:entity-type :long :value-type :long}})
        facts [[10 :line/part 1] [11 :line/part 1] [12 :line/part 2]]
        group-step (fn [plan] (some #(when (= :grouped-query (:kind %)) %)
                                    (get-in plan [:regions 0 :operators])))]
    (doseq [[scan lookup keys] [[10000 2 '[?key]] [3 10000 []]]]
      (let [lines (priced-facts info facts scan lookup)
            plan (d/explain {:run? true} q parts lines)]
        (is (= #{[1 2]} (d/q q parts lines)))
        (is (= keys (vec (:binding-inputs (group-step plan)))))
        (is (= (if (seq keys) 2 3)
               (get-in plan [:regions 1 :operators 0 :actual :output-rows])))))
    (let [lines (priced-facts info facts 10000 2)
          weak-view (conj q :query-source {:consistency :statement})]
      (is (nil? (get-in (d/explain {:run? false} weak-view parts lines)
                        [:regions 0 :grouped-selection])))
      (is (= #{[1 2]} (d/q weak-view parts lines)))
      (let [q '[:find ?key ?n :in $parts $lines :where
                [$parts ?n :row/value "selected"]
                [(q [:find (count ?w) ?p :in $s :where [$s ?w :line/part ?p]]
                    $lines) [[?n ?key]]]]]
        (is (nil? (get-in (d/explain {:run? false} q parts lines) [:regions 0 :grouped-selection])))
        (is (= #{[2 1]} (d/q q parts lines)))))))

(deftest grouped-lookup-keeps-failing-aggregates-observable
  (let [parts (priced-facts row-info [[1 :row/value "selected"]] 1 1)
        q '[:find ?part ?mean :in $parts $lines :where
            [$parts ?part :row/value "selected"]
            [(q [:find ?k (avg ?x) :in $s :where [$s ?k :row/value ?x]]
                $lines) [[?part ?mean]]]]]
    (doseq [[type facts] [[:bigdec [[1 1M] [2 1M] [2 2M] [2 4M]]]
                          [:long [[1 1] [2 2] [2 Long/MAX_VALUE]]]]]
      (let [lines (priced-facts (fake/row-info type) (fake/row-facts facts) 10000 2)]
        (binding [*math-context* nil]
          (is (nil? (get-in (d/explain {:run? false} q parts lines) [:regions 0 :grouped-selection])))
          (is (= :query-source/provider
                 (try (d/q q parts lines) nil
                      (catch clojure.lang.ExceptionInfo e (:error (ex-data e)))))))))))

(deftest grouped-query-rejects-correlated-inputs-and-tighter-inner-limits
  (let [s (pair-source row-info [[1 "a"]])]
    (doseq [q ['[:find ?v ?n :in $s :where [$s ?id :row/value ?v]
                 [(q [:find ?x (count ?e) :in $ ?x :where [?e :row/value ?x]]
                     $s ?v) [[?v ?n]]]]
               '[:find ?v ?n :in $s :where
                 [(q [:find ?x (count ?e) :where [?e :row/value ?x]
                      :query-source {:limits {:retained-bytes 32}}] $s) [[?v ?n]]]]]]
      (is (= :query-source/unsupported
             (try (d/q q s) nil
                  (catch clojure.lang.ExceptionInfo e (:error (ex-data e)))))))
    (is (empty? (filter #{:acquire} (events s))))))
