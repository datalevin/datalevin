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
