(ns datalevin.entity-range-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [datalevin.core :as d]
            [datalevin.server :as server]
            [datalevin.test.core :refer [allocate-port db-fixture]]
            [datalevin.util :as u]))

(use-fixtures :each db-fixture)

(def ^:private range-query
  '[:find ?e ?name ?value :in $ ?start ?end
    :where [(entity-range $ ?start ?end) [?e ...]]
           [?e :name ?name] [?e :value ?value]
    :order-by ?e])

(defn- check-range! [conn]
  (d/transact! conn [{:db/id 0 :name "zero" :value 0}
                    {:db/id 2 :name "two" :value 20}
                    {:db/id 7 :name "seven" :value 70}])
  (doseq [[bounds expected] [[[0 8] [0 2 7]] [[0 2] [0]] [[2 7] [2]]
                             [[3 7] []] [[7 8] [7]] [[8 10] []]
                             [[2 2] []] [[7 2] []]
                             [[0 Long/MAX_VALUE] [0 2 7]]]]
    (is (= expected (apply d/entity-range @conn bounds)) (pr-str bounds)))
  (doseq [bounds [[-1 8] [0 -1] [nil 8] [0 "8"] [0 8.5]
                 [0 (inc (bigint Long/MAX_VALUE))]]]
    (is (thrown-with-msg? Exception #"bounds must be nonnegative 64-bit integers"
                         (apply d/entity-range @conn bounds))))
  (let [reader (d/prepare-q @conn range-query)
        ids (d/prepare-q @conn
                        '[:find [?e ...] :in $ ?start ?end
                          :where [(entity-range $ ?start ?end) [?e ...]]])]
    (is (= [[0 "zero" 0] [2 "two" 20] [7 "seven" 70]] (reader [0 8])))
    (is (= [[2 "two" 20]] (reader [2 7])))
    (is (empty? (reader [3 7])))
    (is (= [0 2 7] (sort (ids [0 10]))))
    (testing "a query with only entity-range must invalidate for any attribute"
      (d/transact! conn [{:db/id 9 :data {:new true}}])
      (is (= [0 2 7 9] (sort (ids [0 10]))))
      (d/transact! conn [[:db/retractEntity 9]])
      (is (= [0 2 7] (sort (ids [0 10])))))
    (d/transact! conn [[:db/add 2 :value 21]])
    (is (= [[2 "two" 21]] (reader [2 7])))
    (d/with-transaction [tx conn]
      (let [tx-reader (d/prepare-q @tx range-query)]
        (d/transact! tx [{:db/id 3 :name "three" :value 30}
                        [:db/retractEntity 2]])
        (is (= [0 3 7] (d/entity-range @tx 0 8)))
        (is (= [[3 "three" 30]] (tx-reader [2 7])))
        (d/abort-transact tx)))
    (is (= [0 2 7] (d/entity-range @conn 0 8)))
    (is (= [[2 "two" 21]] (reader [2 7]))))
    (testing "simulated views include new entities and remove fully retracted ones"
      (let [pending (:db-after (d/tx-data->simulated-report
                                @conn [[:db/retractEntity 0]
                                       [:db/retract 2 :name "two"]
                                       {:db/id 3 :name "three"}
                                       {:db/id 8 :name "outside"}]))]
        (is (= [2 3 7] (d/entity-range pending 0 8)))
        (is (= [3] (d/entity-range pending 3 7)))
        (is (= [0 2 7] (d/entity-range @conn 0 8))))))

(deftest entity-range-and-prepared-projection
  (let [root (u/tmp-dir (str "entity-range-" (random-uuid)))
        port (allocate-port)
        srv (server/create {:root (str root "/server") :port port})]
    (try
      (server/start srv)
      (doseq [remote? [false true] cache-limit [0 512]]
        (testing (str "remote=" remote? ", cache=" cache-limit)
          (let [name (str "range-" cache-limit)
                path (if remote?
                       (str "dtlv://datalevin:datalevin@localhost:" port "/" name)
                       (str root "/" name))
                conn (d/create-conn path
                                    {:name {:db/valueType :db.type/string}
                                     :value {:db/valueType :db.type/long} :data {}}
                                    {:cache-limit cache-limit
                                     :client-opts {:pool-size 1}})]
            (try (check-range! conn) (finally (d/close conn))))))
      (finally (server/stop srv) (u/delete-files root)))))
