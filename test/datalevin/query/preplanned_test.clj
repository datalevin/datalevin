(ns datalevin.query.preplanned-test
  (:require [clojure.test :refer [deftest is testing]]
            [datalevin.core :as d]
            [datalevin.parser :as parser]
            [datalevin.query.execute :as execute]
            [datalevin.query.optimizer.estimates :as estimates]
            [datalevin.query.optimizer.selective :as selective]
            [datalevin.query.plan :as plan]
            [datalevin.query.resolve :as resolve]))

(deftest selective-costing-plan-is-reused-and-invalidated
  (with-open [conn (d/create-conn nil
                    {:item/state {:db/valueType :db.type/keyword}
                     :item/name {:db/valueType :db.type/string}}
                    {:background-sampling? false})]
    (d/transact! conn [{:item/state :ready :item/name "Alice"}
                       {:item/state :ready :item/name "Amy"}
                       {:item/state :pending :item/name "Bob"}])
    (let [query '[:find ?name
                  :where [?e :item/state :ready]
                  [?e :item/name ?name]
                  [(like ?name "A%")]]
          context (-> (plan/make-context (parser/parse-query query) false)
                      (resolve/resolve-ins [@conn]))
          prepared (selective/materialize-selective-value-lookups context)
          planned (#'execute/planning prepared)]
      (testing "execution retains the graph and plan already built for costing"
        (is (some? (:graph prepared)))
        (is (some? (:plan prepared)))
        (is (identical? (:graph prepared) (:graph planned)))
        (is (= (:plan prepared) (:plan planned)))
        (is (= #{["Alice"] ["Amy"]} (d/q query @conn))))
      (testing "resetting a speculative context permits a new plan"
        (let [reset (estimates/reset-cost-planning prepared)
              replanned (#'execute/planning reset)]
          (is (nil? (:graph reset)))
          (is (nil? (:plan reset)))
          (is (some? (:graph replanned)))
          (is (some? (:plan replanned)))
          (is (not (identical? (:graph prepared) (:graph replanned)))))))))
