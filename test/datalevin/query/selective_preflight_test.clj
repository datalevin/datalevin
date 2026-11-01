(ns datalevin.query.selective-preflight-test
  (:require [clojure.test :refer [deftest is testing]]
            [datalevin.constants :as c]
            [datalevin.core :as d]
            [datalevin.query :as q])
  (:import [datalevin.utl LRUCache]))

(def returned-items
  '[:find ?name (sum ?amount)
    :with ?line
    :where
    [?customer :customer/key ?person]
    [?customer :customer/name ?name]
    [?order :order/customer ?person]
    [?order :order/key ?key]
    [?order :order/date ?date]
    [(>= ?date 10)] [(< ?date 14)]
    [?line :line/order ?key]
    [?line :line/flag "R"]
    [?line :line/amount ?amount]])

(deftest oversized-speculative-seed-is-rejected-before-materialization
  (with-open [conn (d/create-conn nil
                    {:customer/key {:db/valueType :db.type/long}
                     :customer/name {:db/valueType :db.type/string}
                     :order/key {:db/valueType :db.type/long}
                     :order/date {:db/valueType :db.type/long}
                     :order/customer {:db/valueType :db.type/long}
                     :line/order {:db/valueType :db.type/long}
                     :line/flag {:db/valueType :db.type/string}
                     :line/amount {:db/valueType :db.type/long}}
                    {:background-sampling? false})]
    (d/transact! conn
      (concat
        (for [i (range 40)] {:customer/key i :customer/name (str "C" i)})
        (for [i (range 2000)]
          {:order/key i :order/date (mod i 100) :order/customer (mod i 40)})
        (for [i (range 2000) j (range 4)]
          {:line/order i :line/flag (if (zero? j) "R" "N") :line/amount 10})))
    (binding [q/*cache?* false
              q/*plan-cache* (LRUCache. 64)
              c/init-exec-size-threshold 10
              c/magic-cost-hash-join 60.0]
      (let [facts (mapv (fn [datom] [(:e datom) (:a datom) (:v datom)])
                        (d/datoms @conn :eav))
            expected (set (d/q returned-items facts))
            cache-keys (volatile! nil)]
        (testing "cold and cached plans avoid allocating the rejected seed"
          (dotimes [_ 2]
            (let [explain (d/explain {:run? true} returned-items @conn)
                  decision (some #(when (= '[?line :line/flag "R"] (:pattern %)) %)
                                 (:pre-materialization-decisions explain))]
              (is (= expected (set (:result explain))))
              (is (= :planner-value-lookup (:strategy decision)))
              (is (= :seed-propagation-preflight (get-in decision [:guardrail :phase])))
              (is (true? (get-in decision [:guardrail :lower-bound?])))
              (is (zero? (:materialization-cost decision)))
              (is (empty? (:materialization-stages decision)))
              (let [keys (.keys ^LRUCache q/*plan-cache*)]
                (if @cache-keys
                  (is (= @cache-keys keys))
                  (do (is (seq keys)) (vreset! cache-keys keys)))))))
        (testing "a transaction refreshes the count used by the guardrail"
          (d/transact! conn [{:line/order 10 :line/flag "R" :line/amount 20}])
          (let [explain (d/explain {:run? true} returned-items @conn)
                decision (some #(when (= '[?line :line/flag "R"] (:pattern %)) %)
                               (:pre-materialization-decisions explain))]
            (is (= 2001 (:fanout decision)))
            (is (not= expected (set (:result explain))))
            (is (empty? (:materialization-stages decision)))))))))
