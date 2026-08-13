(ns datalevin.query-optimizer-test
  (:require
   [clojure.test :refer [deftest is testing]]
   [datalevin.constants :as c]
   [datalevin.core :as d]
   [datalevin.db :as db]
   [datalevin.util :as u])
  (:import
   [java.util UUID]))

(def late-expansion-query
  '[:find ?id ?score
    :where
    [?start :start/id 1]
    (or-join [?start ?item]
      (and [?link :link/from ?start]
           [?link :link/to ?item])
      (and [?link1 :link/from ?start]
           [?link1 :link/to ?mid]
           [?link2 :link/from ?mid]
           [?link2 :link/to ?item]))
    [?item :item/id ?id]
    [?item :item/score ?score]
    [(< ?score 3000)]
    :order-by [?score :desc ?id :asc]
    :limit 5])

(deftest hash-join-output-materialization-cost-test
  (let [estimate-cost @(ns-resolve 'datalevin.query-optimizer
                                   'estimate-hash-join-cost)]
    (binding [c/magic-cost-hash-join              10.0
              c/magic-cost-hash-join-output-tuple 4.0
              c/magic-cost-hash-join-output-cell  1.0]
      (testing "the compatibility arity prices hash inputs"
        (is (= 300 (estimate-cost 10 20))))
      (testing "ordinary output is covered by the input work"
        (is (= 300 (estimate-cost 10 20 30 3))))
      (testing "materialization dominates when fanout is high enough"
        (is (= 350 (estimate-cost 10 20 50 3)))
        (is (= 1050 (estimate-cost 10 20 150 3)))))))

(deftest sampled-late-expansion-cost-test
  (let [dir    (u/tmp-dir (str "late-expansion-cost-" (UUID/randomUUID)))
        schema {:start/id   {:db/valueType :db.type/long
                             :db/unique    :db.unique/identity}
                :item/id    {:db/valueType :db.type/long
                             :db/unique    :db.unique/identity}
                :item/score {:db/valueType :db.type/long}
                :link/from  {:db/valueType :db.type/ref}
                :link/to    {:db/valueType :db.type/ref}}
        conn   (d/get-conn dir schema)]
    (try
      (let [start     1
            item-base 10000
            link-base 100000
            item-count 2000
            tx-data
            (into [{:db/id start :start/id 1}]
                  (mapcat
                    (fn [^long i]
                      (let [item (+ item-base i)]
                        [{:db/id item :item/id i :item/score i}
                         {:db/id (+ link-base i)
                          :link/from start
                          :link/to item}]))
                    (range item-count)))]
        (d/transact! conn tx-data)
        ;; Ordered access deliberately excludes an unmerged transaction cache.
        ;; The transaction is durable here, so use the persisted-index view.
        (let [db-value     (db/-clear-tx-cache (d/db conn))
              explain      (d/explain {:run? false}
                                      late-expansion-query db-value)
              conventional (some #(when (= :conventional (:kind %)) %)
                                 (:physical-plan-alternatives explain))
              expansion    (some #(when (= :sampled-late-expansion
                                           (:operation %))
                                     %)
                                 (get-in conventional
                                         [:cost-breakdown :late-stages]))]
          (testing "sampled expansion corrects conventional cardinality"
            (is (= :sampled (:confidence expansion)))
            (is (= item-count (:output expansion)))
            (is (= item-count (:size conventional))))
          (testing "the ordered top-k alternative is selected"
            (is (= :access
                   (get-in explain [:selected-plan-alternative :kind]))))
          (testing "the selected plan preserves query results"
            (is (= [[1999 1999]
                    [1998 1998]
                    [1997 1997]
                    [1996 1996]
                    [1995 1995]]
                   (d/q late-expansion-query db-value))))))
      (finally
        (d/close conn)
        (u/delete-files dir)))))
