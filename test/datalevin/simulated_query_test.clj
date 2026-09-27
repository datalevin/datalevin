(ns datalevin.simulated-query-test
  (:require [clojure.test :refer [deftest is testing]]
            [datalevin.core :as d]))

(deftest simulated-query-reads-and-cache-isolation
  (doseq [cache-limit [0 128]]
    (let [conn (d/create-conn
                 nil {:key {:db/valueType :db.type/long
                            :db/unique :db.unique/identity}
                      :name {:db/valueType :db.type/string}
                      :score {:db/valueType :db.type/long}
                      :friend {:db/valueType :db.type/ref}
                      :tag {:db/cardinality :db.cardinality/many}}
                 {:cache-limit cache-limit})]
      (try
        (d/transact! conn [{:db/id 1 :key 1 :name "before" :score 10
                           :friend 2 :tag #{"old" "kept"}}
                          {:db/id 2 :key 2 :name "kept" :score 20}
                          {:db/id 3 :key 3 :name "removed" :score 50}])
        (let [simulated (:db-after
                          (d/tx-data->simulated-report
                            @conn [[:db/add 1 :name "simulated"]
                                   [:db/add 1 :score 40]
                                   [:db/retract 1 :tag "old"]
                                   [:db/add 1 :tag "new"]
                                   [:db/retractEntity 3]
                                   {:db/id 4 :key 4 :name "added" :score 30}]))
              cases
              [['[:find ?name . :where [1 :name ?name]] [] "simulated"]
               ['[:find ?e ?name :where [?e :name ?name]] []
                #{[1 "simulated"] [2 "kept"] [4 "added"]}]
               ['[:find ?e . :where [?e :name "simulated"]] [] 1]
               ['[:find ?e :where [?e :name "before"]] [] #{}]
               ['[:find ?e ?name :in $ [?e ...] :where [?e :name ?name]]
                [[1 2 3 4]] #{[1 "simulated"] [2 "kept"] [4 "added"]}]
               ['[:find ?name . :in $ ?key
                  :where [?e :key ?key] [?e :name ?name]] [1] "simulated"]
               ['[:find ?score :in $ ?start ?limit ?offset
                  :where [?e :score ?score] [(>= ?score ?start)]
                  :order-by ?score :limit ?limit :offset ?offset]
                [0 2 1] [[30] [40]]]
               ['[:find ?name ?friend :where [?e :name ?name]
                  [?e :friend ?f] [?f :name ?friend]] [] #{["simulated" "kept"]}]
               ['[:find (count ?e) . :where [?e :name _]] [] 3]
               ['[:find ?tag :where [1 :tag ?tag]] [] #{["kept"] ["new"]}]
               ['[:find ?name . :where [(get-else $ 1 :name "missing") ?name]]
                [] "simulated"]
               ['[:find ?e :where [?e :name _] (not [?e :name "kept"])]
                [] #{[1] [4]}]
               ['[:find ?e :where (or [?e :name "simulated"] [?e :name "added"])]
                [] #{[1] [4]}]
               ['[:find ?name :in $ % :where (named ?e ?name)]
                ['[[(named ?e ?name) [?e :name ?name]]]]
                #{["simulated"] ["kept"] ["added"]}]]]
          (doseq [[query inputs expected] cases]
            (testing (str "cache=" cache-limit " query=" query)
              (let [committed (apply d/q query @conn inputs)
                    read (d/prepare-q simulated query)]
                (is (= expected (apply d/q query simulated inputs)))
                (is (= expected (d/execute-prepared read inputs)))
                (is (= committed (apply d/q query @conn inputs))))))
          (is (= #{["before" "simulated"]}
                 (d/q '[:find ?before ?after :in $before $after
                        :where [$before 1 :name ?before] [$after 1 :name ?after]]
                      @conn simulated)))
          (is (= #{[1 "simulated"] [2 "kept"] [4 "added"]}
                 (:result (d/explain {:run? true}
                                    '[:find ?e ?name :where [?e :name ?name]]
                                    simulated))))
          (is (seq (:late-clauses
                     (d/explain {:run? false}
                                '[:find ?e ?name :where [?e :name ?name]]
                                simulated))))
          (is (= {:name "before"} (d/pull @conn [:name] 1)))
          (is (= {:name "simulated"} (d/pull simulated [:name] 1))))
        (finally (d/close conn))))))
