(ns datalevin.conn.scalar-refresh-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [datalevin.conn :as conn]
            [datalevin.constants :as c]
            [datalevin.core :as d]
            [datalevin.test.core :refer [db-fixture]]
            [datalevin.tx-group.batch.embedded :as embedded]
            [datalevin.tx-group.compat :as group]
            [datalevin.util :as u]))

(use-fixtures :each db-fixture)

(def ^:private schema {:value {:db/valueType :db.type/long}})

(defn- native-tx [connection]
  (d/get-value (d/datalog-kv connection) c/meta :max-tx :attr :long))

(deftest scalar-reports-follow-the-current-writer-across-connections
  (doseq [wal? [false true] collected? [false true] shape [:map :vector]]
    (testing (str [wal? collected? shape])
      (binding [embedded/*defer-attachment?* (not collected?)
                group/*enabled?* collected?]
        (let [dir (u/tmp-dir (str "scalar-refresh-" (random-uuid)))
              options {:wal? wal? :wal-segment-prealloc? false
                       :background-sampling? false :snapshot-scheduler? false}
              committed-tx (atom nil)]
          (try
            (with-open [seed (d/create-conn dir schema options)]
              (d/transact! seed [{:db/id 1 :value 0}]))
            (with-open [left (d/create-conn dir nil options)
                        right (d/create-conn dir nil options)]
              (let [start (long (native-tx left))
                    reports
                    (mapv
                      (fn [[connection value no-op?]]
                        (let [before @connection
                              native-before (long (native-tx connection))
                              expected (inc native-before)
                              paths (atom [])
                              forms (if (= shape :map)
                                      [{:db/id 1 :value value}]
                                      [[:db/add 1 :value value]])
                              report (binding [conn/*local-wal-tx-path-observer*
                                               #(swap! paths conj %)]
                                       (d/transact! connection forms {:value value}))]
                          (is (= [:scalar-update] @paths))
                          (when-not collected?
                            (is (= native-before (:max-tx (:db-before report)))))
                          (is (= expected (get (:tempids report) :db/current-tx)))
                          (is (= expected (:max-tx (:db-after report))))
                          (is (= expected (:max-tx @connection)))
                          (is (= expected (native-tx connection)))
                          (is (every? #(= expected (:tx %)) (:tx-data report)))
                          (is (= (if no-op? 0 2) (count (:tx-data report))))
                          (is (= {:value value} (:tx-meta report)))
                          (is (= value (:value (d/entity @connection 1))))
                          (when (and (not collected?) (= value 4) (not no-op?))
                            (is (< (:max-tx before) native-before)))
                          report))
                      [[left 1 false] [right 2 false] [right 3 false]
                       [left 4 false] [left 4 true] [right 5 false]])]
                (is (= (vec (range (inc start) (+ start 7)))
                       (mapv #(get (:tempids %) :db/current-tx) reports)))
                (reset! committed-tx (native-tx right))))
            (with-open [reopened (d/create-conn dir nil options)]
              (is (= @committed-tx (:max-tx @reopened)))
              (is (= @committed-tx (native-tx reopened)))
              (is (= 5 (:value (d/entity @reopened 1)))))
            (finally (u/delete-files dir))))))))

(deftest scalar-refresh-preserves-the-next-blind-inserts-entity-floor
  (doseq [wal? [false true]]
    (binding [embedded/*defer-attachment?* true group/*enabled?* false]
      (let [dir (u/tmp-dir (str "scalar-entity-floor-" (random-uuid)))
            options {:wal? wal? :wal-segment-prealloc? false
                     :background-sampling? false :snapshot-scheduler? false}]
        (try
          (with-open [seed (d/create-conn dir schema options)]
            (d/transact! seed [{:db/id 1 :value 0}]))
          (with-open [peer (d/create-conn dir nil options)
                      stale (d/create-conn dir nil options)]
            (d/transact! peer [{:db/id 2 :value 22} {:db/id 100 :value 100}])
            (is (= 1 (:max-eid @stale)))
            (let [report (d/transact! stale [[:db/add 1 :value 1]])]
              (is (= 100 (:max-eid (:db-before report))))
              (is (= 100 (:max-eid (:db-after report)))))
            (let [report (d/transact! stale [{:db/id "new" :value 2}])]
              (is (= 101 (get (:tempids report) "new")))
              (is (= 22 (:value (d/entity @stale 2))))
              (is (= 100 (:value (d/entity @stale 100))))
              (is (= 2 (:value (d/entity @stale 101))))))
          (finally (u/delete-files dir)))))))
