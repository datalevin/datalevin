(ns datalevin.query.fused-aggregate-test
  (:require [clojure.test :refer [deftest is testing]]
            [datalevin.core :as d]
            [datalevin.query :as query]
            [datalevin.query.execute.fused-aggregate :as fused]
            [datalevin.storage.scan :as scan])
  (:import [datalevin.storage Store]))

(def schema {:g {:db/valueType :db.type/string}
             :v {:db/valueType :db.type/long}
             :price {:db/valueType :db.type/double}})

(defn- explain [database q & inputs]
  (binding [query/*cache?* false fused/*min-input* 0 fused/*min-scan-fraction* 0]
    (assoc (apply d/explain {:run? true} q database inputs)
           :result (apply d/q q database inputs))))

(deftest grouped-scan-fuses-expressions-and-keeps-duplicate-entities
  (with-open [^java.io.Closeable conn
              (d/create-conn nil schema {:background-sampling? false})]
    (d/transact! conn [{:db/id 1 :g "a" :v 10 :price 1.5}
                       {:db/id 2 :g "a" :v 10 :price 1.5}
                       {:db/id 3 :g "b" :v 20 :price 2.5}
                       {:db/id 4 :g "incomplete"}])
    (let [q '[:find ?g (sum ?x) (avg ?x) (count ?e)
              :order-by [0 :asc]
              :where [?e :g ?g] [?e :v ?v] [(* ?v 2) ?x]]
          actual (explain @conn q)
          reference (binding [fused/*enabled?* false] (explain @conn q))]
      (is (= [["a" 40 20.0 2] ["b" 40 40.0 1]] (:result actual)))
      (is (= (:result reference) (:result actual)))
      (is (= :fused-aggregate (get-in actual [:executed-plan-alternative :kind])))
      (is (= 0 (get-in actual [:fused-aggregation :retained-input-tuples])))
      (is (= 3 (get-in actual [:fused-aggregation :input-tuples])))
      (is (= 2 (get-in actual [:fused-aggregation :group-count]))))
    (let [q '[:find ?g (sum ?x) :with ?e
              :in $ ?factor
              :having [(> (sum ?x) 30)]
              :order-by [1 :desc] :limit 1
              :where [?e :g ?g] [?e :price ?v] [(* ?v ?factor) ?x]]
          actual (explain @conn q 20)]
      (is (= [["a" 60.0]] (:result actual)))
      (is (some? (:fused-aggregation actual))))))

(deftest unsupported-shapes-retain-set-semantics
  (with-open [^java.io.Closeable conn
              (d/create-conn nil schema {:background-sampling? false})]
    (d/transact! conn [{:db/id 1 :g "a" :v 10} {:db/id 2 :g "a" :v 10}])
    (doseq [[q expected]
            [['[:find ?g (sum ?v) :where [?e :g ?g] [?e :v ?v]] #{["a" 10]}]
             ['[:find ?g (min ?v) (count ?e) :where [?e :g ?g] [?e :v ?v]] #{["a" 10 2]}]
             ['[:find ?g (sum ?x) (count ?e)
                :where [?e :g ?g] [?e :v ?v] [(identity ?v) ?x]] #{["a" 20 2]}]]]
      (let [actual (explain @conn q)]
        (is (= expected (set (:result actual))))
        (is (nil? (:fused-aggregation actual)))))
    (testing "schema changes are checked rather than reusing a stale fused layout"
      (d/update-schema conn {:v {:db/cardinality :db.cardinality/many}})
      (d/transact! conn [[:db/add 1 :v 11]])
      (let [actual (explain @conn '[:find ?g (sum ?v) :with ?e
                                   :where [?e :g ?g] [?e :v ?v]])]
        (is (= #{["a" 31]} (set (:result actual))))
        (is (nil? (:fused-aggregation actual)))))))

(deftest streaming-errors-close-the-native-read
  (with-open [^java.io.Closeable conn
              (d/create-conn nil schema {:background-sampling? false})]
    (d/transact! conn [{:db/id 1 :g "a" :v 10} {:db/id 2 :g "b" :v 20}])
    (let [schema (d/schema conn)
          fields (sort-by #(get-in schema [% :db/aid]) [:g :v])
          ids (int-array (map #(get-in schema [% :db/aid]) fields))
          seen (atom 0)
          store (:store @conn)]
      (is (thrown-with-msg? clojure.lang.ExceptionInfo #"stop"
            (scan/fold-entity-rows
              (.-lmdb ^Store store) ids (int-array [1 2]) (object-array 3)
              (fn [_] (swap! seen inc) (throw (ex-info "stop" {:test true})))
              (fn []))))
      (is (= 1 @seen))
      (d/transact! conn [[:db/add 1 :v 11]])
      (is (= #{["a" 11 1] ["b" 20 1]}
             (set (:result (explain @conn '[:find ?g (sum ?v) (count ?e)
                                           :where [?e :g ?g] [?e :v ?v]])))))
      (is (thrown? InterruptedException
            (try
              (.interrupt (Thread/currentThread))
              (explain @conn '[:find ?g (sum ?v) (count ?e)
                               :where [?e :g ?g] [?e :v ?v]])
              (finally (Thread/interrupted))))))))
