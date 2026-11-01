(ns datalevin.query.existence-placement-test
  (:require [clojure.test :refer [deftest is testing]]
            [datalevin.core :as d]
            [datalevin.query :as q]
            [datalevin.query.optimizer.existence-placement :as placement])
  (:import [datalevin.utl LRUCache]))

(def prefix
  '[:find ?person ?name
    :where [?root :root/id 1]
    [?edge :edge/root ?root] [?edge :edge/person ?person]
    [?person :person/keep true] [?person :person/name ?name]])

(def cheap-query (conj prefix '(not-join [?person] [?person :person/blocked true])))
(def expensive-query
  (conj prefix '(not-join [?person] [?block :block/person ?person]
                         [?block :block/score ?score] [(> ?score 0)])))

(defn- fixture [f]
  (with-open [conn (d/create-conn nil
                    {:root/id {:db/valueType :db.type/long}
                     :edge/root {:db/valueType :db.type/ref}
                     :edge/person {:db/valueType :db.type/ref}
                     :person/keep {:db/valueType :db.type/boolean}
                     :person/name {:db/valueType :db.type/string}
                     :person/blocked {:db/valueType :db.type/boolean}
                     :block/person {:db/valueType :db.type/ref}
                     :block/score {:db/valueType :db.type/long}}
                    {:background-sampling? false})]
    (d/transact! conn
      (concat [{:db/id 1 :root/id 1}]
              (for [i (range 4000)]
                {:db/id (+ 1000 i) :person/name (str "P" i)
                 :person/keep (even? i) :person/blocked (not (zero? (mod i 20)))})
              (for [i (range 400)] {:edge/root 1 :edge/person (+ 1000 i)})
              (for [i (range 400) _ (range 12)]
                {:block/person (+ 1000 i) :block/score (if (zero? (mod i 5)) 0 1)})))
    (binding [q/*cache?* false q/*plan-cache* (LRUCache. 128)] (f conn))))

(deftest placement-charges-for-existence-work-and-avoided-lookups
  (fixture
    (fn [conn]
      (doseq [[query expected-site] [[cheap-query :early] [expensive-query :late]]]
        (let [database @conn
              early (binding [placement/*strategy* :early]
                      (d/explain {:run? true} query database))
              late (binding [placement/*strategy* :late]
                     (d/explain {:run? true} query database))
              selected (d/explain {:run? true} query database)
              decision (first (:existence-placement selected))
              alternatives (:alternatives decision)
              expected (if (= expected-site :early) (first alternatives) (last alternatives))]
          (is (= (set (:result early)) (set (:result late)) (set (:result selected))))
          (is (= :cost (:strategy decision)) (pr-str decision))
          (is (= (:site expected) (:site decision)) (pr-str decision))
          (is (<= (or (:sample-work decision) (inc placement/*max-rows*)) placement/*max-rows*))
          (is (every? #(and (pos? (:existence-cost %)) (pos? (:sample-input %))) alternatives))
          (testing "cache hits reuse only scalar estimates"
            (let [cached (first (:existence-placement (d/explain {:run? true} query database)))]
              (is (:cache-hit? cached))
              (is (= (:alternatives decision) (:alternatives cached)))
              (is (not-any? #(and (map? %) (contains? % :tuples))
                            (tree-seq coll? seq cached))))))))))

(deftest uncertain-estimates-and-transactions-keep-execution-exact
  (fixture
    (fn [conn]
      (let [expected (set (d/q expensive-query @conn))
            limited (binding [placement/*max-rows* 1]
                      (d/explain {:run? true} expensive-query @conn))]
        (is (= expected (set (:result limited))))
        (is (= :late (get-in limited [:existence-placement 0 :strategy])))
        (is (= :sample-budget (get-in limited [:existence-placement 0 :reason]))))
      (d/explain {:run? true} cheap-query @conn)
      (d/transact! conn [{:db/id 1000 :person/blocked true}])
      (let [selected (d/explain {:run? true} cheap-query @conn)
            late (binding [placement/*strategy* :late] (d/q cheap-query @conn))]
        (is (= (set late) (set (:result selected))))
        (is (not (get-in selected [:existence-placement 0 :cache-hit?])))))))

(deftest closing-the-store-evicts-placement-estimates
  (let [cache (volatile! nil)]
    (fixture
      (fn [conn]
        (d/explain {:run? true} cheap-query @conn)
        (vreset! cache q/*plan-cache*)
        (is (not (.isEmpty ^LRUCache @cache)))))
    (is (.isEmpty ^LRUCache @cache))))
