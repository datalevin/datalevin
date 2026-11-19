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
