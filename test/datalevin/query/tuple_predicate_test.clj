(ns datalevin.query.tuple-predicate-test
  (:require [clojure.string :as str]
            [clojure.test :refer [deftest is testing]]
            [datalevin.constants :as c]
            [datalevin.core :as d]
            [datalevin.query :as query]))

(def shipping-query
  '[:find ?mode (sum ?high) (sum ?low)
    :with ?line
    :where
    [?line :line/order ?key]
    [?line :line/mode ?mode]
    [(or (= ?mode "MAIL") (= ?mode "SHIP"))]
    [?line :line/commit ?commit]
    [?line :line/receipt ?receipt]
    [(< ?commit ?receipt)]
    [?line :line/ship ?ship]
    [(< ?ship ?commit)]
    [(>= ?receipt 10)]
    [(< ?receipt 20)]
    [?order :order/key ?key]
    [?order :order/priority ?priority]
    [(= ?priority "HIGH") ?hi]
    (or-join [?hi ?high ?low]
      (and [(= ?hi true)] [(ground 1) ?high] [(ground 0) ?low])
      (and [(= ?hi false)] [(ground 0) ?high] [(ground 1) ?low]))])

(deftest tuple-comparisons-filter-before-joins-in-every-partition
  (with-open [conn (d/create-conn
                    nil
                    {:line/order {:db/valueType :db.type/long}
                     :line/mode {:db/valueType :db.type/string}
                     :line/commit {:db/valueType :db.type/long}
                     :line/receipt {:db/valueType :db.type/long}
                     :line/ship {:db/valueType :db.type/long}
                     :order/key {:db/valueType :db.type/long
                                 :db/unique :db.unique/identity}
                     :order/priority {:db/valueType :db.type/string}}
                    {:background-sampling? false})]
    (d/transact! conn
      (concat
        (for [i (range 10)]
          {:order/key i :order/priority (if (even? i) "HIGH" "LOW")})
        (for [i (range 240)]
          {:line/order (mod i 10)
           :line/mode (nth ["MAIL" "SHIP" "RAIL"] (mod i 3))
           :line/receipt 15 :line/commit (if (zero? (mod i 4)) 16 12)
           :line/ship (if (zero? (mod i 5)) 13 10)})))
    (let [db @conn
          facts (mapv (fn [datom] [(:e datom) (:a datom) (:v datom)])
                      (d/datoms db :eav))
          expected (set (d/q shipping-query facts))]
      (binding [query/*cache?* false
                c/init-exec-size-threshold 5
                c/query-partition-min-input-size 5
                c/query-partition-target-size 10
                c/query-partition-min-step-count 3]
        (testing "sequential and partitioned execution retain all witnesses"
          (is (= expected (binding [c/query-partitioned-execution? false]
                            (set (d/q shipping-query db)))))
          (dotimes [_ 3]
            (is (= expected (binding [c/query-partitioned-execution? true]
                              (set (d/q shipping-query db)))))))
        (let [explain (binding [c/query-partitioned-execution? true]
                        (d/explain {:run? true} shipping-query db))
              plans (-> explain :plan vals first first)
              line-plan (first plans)
              order-plan (last plans)]
          (testing "the order join receives only rows passing both comparisons"
            (is (< (:actual-size line-plan) 160))
            (is (= (:actual-size line-plan) (:actual-size order-plan)))
            (is (= 2 (count (filter #(str/starts-with? % "Filter by (<")
                                    (:steps line-plan))))))
          (when (< 1 (.availableProcessors (Runtime/getRuntime)))
            (testing "comparisons remain inside the parallel scan and join segment"
              (is (some (fn [{:keys [partitioned-step-types]}]
                          (and (= 2 (count (filter #{:tuple-predicate}
                                                  partitioned-step-types)))
                               (some #{:link} partitioned-step-types)))
                        (:partitioned-execution explain))))))))))
