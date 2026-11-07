(ns datalevin.tuple-pending-test
  (:require [clojure.test :refer [deftest is testing]]
            [datalevin.constants :as c]
            [datalevin.core :as d]
            [datalevin.util :as u]))

(def ^:private schema
  {:parent {:db/valueType :db.type/ref}
   :part {:db/valueType :db.type/long}
   :key {:db/tupleAttrs [:parent :part] :db/unique :db.unique/value}
   :label {:db/valueType :db.type/string}})

(defn- tuple-changes [report]
  (->> (:tx-data report)
       (filter #(= :key (:a %)))
       (mapv (juxt :e :v :added))))

(deftest tuple-flushes-preserve-map-and-function-expansion-boundaries
  (doseq [prepare? [false true]]
    (let [dir (u/tmp-dir (str "tuple-boundaries-" (random-uuid)))
          conn (d/create-conn dir
                 (assoc schema
                        :key {:db/tupleAttrs [:part :label]
                              :db/unique :db.unique/value}
                        :identity {:db/unique :db.unique/identity})
                 {:wal? true :snapshot-scheduler? false})]
      (try
        (binding [c/*use-prepare-path* prepare?]
          (d/transact! conn [{:db/id 1 :part 1 :label "a"}
                            {:db/id 2 :part 2 :label "a"}
                            {:db/id 3 :identity "parent"}])
          ;; The lookup ref forces map expansion. Flushing between its source
          ;; operations would incorrectly conflict with entity 2's [2 "a"].
          (let [report (d/transact! conn
                         [(array-map :db/id 1 :part 2 :label "b"
                                     :parent [:identity "parent"])])]
            (is (= [[1 [1 "a"] false] [1 [2 "b"] true]]
                   (tuple-changes report))))
          ;; A function's expanded operations share one flush, including
          ;; several changes to the same source and a net-zero change.
          (let [report (d/transact! conn
                         [[:db.fn/call
                           (fn [_] [[:db/add 1 :part 3]
                                    [:db/add 1 :part 4]])]])]
            (is (= [[1 [2 "b"] false] [1 [4 "b"] true]]
                   (tuple-changes report))))
          (let [report (d/transact! conn
                         [[:db.fn/call
                           (fn [_] [[:db/add 1 :part 5]
                                    [:db/add 1 :part 4]])]])]
            (is (empty? (tuple-changes report))))
          (is (= [4 "b"] (:key (d/entity @conn 1))))
          (is (= [2 "a"] (:key (d/entity @conn 2)))))
        (finally (d/close conn) (u/delete-files dir))))))

(deftest tuple-before-values-follow-retractions-and-repeated-new-entities
  (doseq [prepare? [false true]]
    (let [dir (u/tmp-dir (str "tuple-before-" (random-uuid)))
          conn (d/create-conn dir schema {:wal? true :snapshot-scheduler? false})]
      (try
        (binding [c/*use-prepare-path* prepare?]
          (d/transact! conn [{:db/id 1 :label "parent"}])
          (let [report (d/transact! conn
                         [{:db/id 10 :parent 1 :part 1}
                          {:db/id 10 :part 2}
                          [:db/retract 10 :parent 1]
                          [:db/retract 10 :part 2]
                          {:db/id 10 :part 3}])]
            (is (= [[10 [1 1] true]
                    [10 [1 1] false] [10 [1 2] true]
                    [10 [1 2] false] [10 [nil 2] true]
                    [10 [nil 2] false]
                    [10 [nil 3] true]]
                   (tuple-changes report))))
          (let [report (d/transact! conn
                         [[:db/retract 10 :part 3]
                          {:db/id 10 :parent 1 :part 4}])]
            (is (= [[10 [nil 3] false] [10 [1 4] true]]
                   (tuple-changes report))))
          (is (= [1 4] (:key (d/entity @conn 10)))))
        (finally (d/close conn) (u/delete-files dir))))))

(deftest new-entity-tuples-follow-the-pending-index
  (doseq [[wal? prepare?] [[false false] [true false] [true true]]]
    (testing (str "WAL " wal? ", preparation " prepare?)
      (let [dir (u/tmp-dir (str "tuple-pending-" (random-uuid)))
            conn (d/create-conn dir schema
                   {:wal? wal? :wal-durability-profile :strict
                    :snapshot-scheduler? false})]
        (try
          (binding [c/*use-prepare-path* prepare?]
            (d/transact! conn [{:db/id 1 :label "parent"}])
            (let [report
                  (d/transact! conn
                    [{:db/id 10 :parent 1 :part 7}
                     {:db/id 11 :parent 1 :part 11}
                     {:db/id 10 :part 8}
                     [:db/retract 10 :parent 1]
                     [:db/retract 10 :part 8]
                     ;; Exercise the native preparation flush before returning
                     ;; further changes to the same newly allocated entity.
                     [:db.fn/call (fn [_] [{:db/id 10 :part 9}])]
                     {:db/id 10 :parent 1}
                     {:db/id "line" :parent 1}
                     {:db/id "line" :part 12}
                     {:db/id "line" :part 13}])
                  line (get (:tempids report) "line")]
              (is (= #{[10 [1 9]] [11 [1 11]] [line [1 13]]}
                     (set (map (juxt :e :v) (d/datoms @conn :ave :key)))))
              (is (= [nil 9]
                     (->> (:tx-data report)
                          (filter #(and (= 10 (:e %)) (= :key (:a %))
                                        (:added %) (= [nil 9] (:v %))))
                          first :v))))
            ;; A later request in an explicit transaction has a new allocation
            ;; boundary and must still see the first request's tuple values.
            (d/with-transaction [tx conn]
              (d/transact! tx [{:db/id "outer" :parent 1 :part 20}])
              (let [eid (d/entid @tx [:key [1 20]])]
                (d/transact! tx [{:db/id eid :part 21}])
                (is (= eid (d/entid @tx [:key [1 21]])))))
            (is (nil? (d/entid @conn [:key [1 20]]))))
          (finally (d/close conn) (u/delete-files dir)))))))
