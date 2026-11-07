(ns datalevin.db.explicit-preparation-test
  (:require
   [clojure.test :refer [deftest is testing use-fixtures]]
   [clojure.test.check :as tc]
   [clojure.test.check.generators :as gen]
   [clojure.test.check.properties :as prop]
   [datalevin.constants :as c]
   [datalevin.core :as d]
   [datalevin.datom :as datom]
   [datalevin.db :as db]
   [datalevin.lmdb :as l]
   [datalevin.test.core :refer [db-fixture]]
   [datalevin.tx-group.phase :as phase]))

(use-fixtures :each db-fixture)

(def ^:private schema
  {:value {:db/valueType :db.type/long}
   :name {:db/valueType :db.type/string :db/unique :db.unique/identity}
   :tags {:db/valueType :db.type/string :db/cardinality :db.cardinality/many}})

(def ^:private opts
  {:wal? true :background-sampling? false :snapshot-scheduler? false})

(deftest inner-requests-share-current-values-and-native-read-boundaries
  (with-open [conn (d/create-conn nil schema opts)]
    (d/transact! conn [{:db/id 1 :value 0}])
    (let [callbacks (atom 0)]
      (d/with-transaction [tx conn]
        (let [first-report (d/transact! tx [[:db/add 1 :value 1]])
              second-report (d/transact! tx [[:db/add 1 :value 2]])]
          (is (= [[1 false] [2 true]]
                 (mapv (juxt :v datom/datom-added) (:tx-data second-report))))
          (is (= [[0 false] [1 true]]
                 (mapv (juxt :v datom/datom-added) (:tx-data first-report)))))
        (d/transact! tx [[:db.fn/call
                         (fn [db]
                           (swap! callbacks inc)
                           (is (= 2 (:value (d/entity db 1))))
                           [[:db/cas 1 :value 2 3]])]])
        (is (= 3 (:value (d/entity @tx 1))))
        (d/transact! tx [{:value 10}])
        (d/transact! tx [{:value 20}])
        (is (= 3 (d/entries (d/datalog-kv tx) c/eav)))
        (is (= #{[3] [10] [20]} (d/q '[:find ?v :where [?e :value ?v]] @tx)))
        (is (= 0 (:value (d/entity @conn 1))))
        (d/transact! tx [[:db/add 1 :value 4]])
        (is (= {:db/id 1 :value 4} (d/pull @tx '[*] 1))))
      (is (= 1 @callbacks))
      (is (= 4 (:value (d/entity @conn 1)))))))

(deftest explicit-preparation-composes-with-simulation-and-nested-bodies
  (with-open [conn (d/create-conn nil schema opts)]
    (d/transact! conn [{:db/id 1 :value 0 :name "one"}])
    (d/open-dbi (d/datalog-kv conn) "aux")
    (d/with-transaction [tx conn]
      (d/transact! tx [[:db/add 1 :value 1]])
      (d/transact! tx [[:db/retract 1 :name "one"]])
      (d/transact! tx [{:name "one" :value 5}])
      (d/transact! tx [[:db/add [:name "one"] :tags "a"]])
      (d/transact! tx [[:db/add [:name "one"] :tags "b"]])
      (d/transact! tx [[:db/retract [:name "one"] :tags "a"]])
      (is (= #{"b"} (:tags (d/entity @tx [:name "one"]))))
      (let [simulated (d/with (db/transfer @tx (:store @tx))
                              [[:db/add 1 :value 99]] nil true)]
        (is (= 99 (:value (d/entity (:db-after simulated) 1))))
        (is (= 1 (:value (d/entity @tx 1)))))
      (d/transact! tx [[:db/add 1 :value 2]])
      (d/with-transaction [nested tx]
        (is (= 2 (:value (d/entity @nested 1))))
        (d/transact! nested [[:db/cas 1 :value 2 3]]))
      (d/transact! tx [[:db/add 1 :value 4]])
      (d/transact-kv (d/datalog-kv tx) "aux" [[:put :marker :written]])
      (is (= :written (d/get-value (d/datalog-kv tx) "aux" :marker)))
      (is (= 4 (:value (d/entity @tx 1)))))
    (is (= 4 (:value (d/entity @conn 1))))
    (is (= 5 (:value (d/entity @conn [:name "one"]))))))

(deftest explicit-preparation-preserves-abort-and-listener-publication
  (doseq [outcome [:commit :abort :exception]]
    (testing (name outcome)
      (with-open [conn (d/create-conn nil schema opts)]
        (d/transact! conn [{:db/id 1 :value 0}])
        (let [notifications (atom [])
              bodies (atom 0)
              run! (fn []
                     (d/with-transaction [tx conn]
                       (swap! bodies inc)
                       (d/transact! tx [[:db/add 1 :value 1]])
                       (d/transact! tx [[:db/add 1 :value 2]])
                       (is (empty? @notifications))
                       (is (= 2 (:value (d/entity @tx 1))))
                       (case outcome
                         :abort (d/abort-transact tx)
                         :exception (throw (ex-info "reject explicit body" {}))
                         nil)))]
          (d/listen! conn ::listener #(swap! notifications conj %))
          (if (= :exception outcome)
            (is (thrown-with-msg? Exception #"reject explicit body" (run!)))
            (run!))
          (is (= 1 @bodies))
          (is (= (if (= :commit outcome) 2 0) (:value (d/entity @conn 1))))
          (is (= (if (= :commit outcome) 2 0) (count @notifications)))
          (d/transact! conn [[:db/add 1 :value 3]])
          (is (= 3 (:value (d/entity @conn 1)))))))))

(deftest caught-inner-validation-failures-discard-only-their-preparation
  (with-open [conn (d/create-conn nil schema opts)]
    (d/transact! conn [{:db/id 1 :name "one" :value 0}
                       {:db/id 2 :name "two" :value 20}])
    (d/with-transaction [tx conn]
      (d/transact! tx [[:db/add 1 :value 1]])
      (d/transact! tx [[:db/add 1 :value 2]])
      (is (thrown? Exception
                   (d/transact! tx [[:db/add 1 :value 99]
                                    [:db/cas 2 :value 999 21]])))
      (d/transact! tx [[:db/cas 1 :value 2 3]])
      (is (= 3 (:value (d/entity @tx 1))))
      (is (= 20 (:value (d/entity @tx 2))))
      (is (= 0 (:value (d/entity @conn 1)))))
    (is (= 3 (:value (d/entity @conn 1))))))

(deftest explicit-preparation-retains-timeout-and-owner-checks
  (with-open [conn (d/create-conn nil schema opts)]
    (d/transact! conn [{:db/id 1 :value 0}])
    (is (thrown? Exception
                 (d/with-transaction [tx conn {:timeout-ms 25}]
                   (d/transact! tx [[:db/add 1 :value 1]])
                   (Thread/sleep 100))))
    (is (= 0 (:value (d/entity @conn 1))))
    (d/with-transaction [tx conn]
      (d/transact! tx [[:db/add 1 :value 2]])
      (let [error @(future
                     (try (d/transact! tx [[:db/add 1 :value 99]])
                          nil
                          (catch Exception e e)))]
        (is (= :txlog/transaction-view-invalidated (:error (ex-data error)))))
      (is (= 2 (:value (d/entity @tx 1)))))
    (let [timeout (l/explicit-transaction-timeout)]
      (try
        (l/set-explicit-transaction-timeout! 10000)
        (d/with-transaction [tx conn]
          (d/transact! tx [[:db/add 1 :value 3]])
          (d/transact! tx [[:db/cas 1 :value 3 4]])
          (is (= 4 (:value (d/entity @tx 1)))))
        (finally (l/set-explicit-transaction-timeout! timeout))))))

(deftest pending-explicit-writes-survive-map-growth-without-replaying-the-body
  (with-open [conn (d/create-conn nil schema (assoc opts :kv-opts {:mapsize 1}))]
    (let [bodies (atom 0)
          resizes (atom 0)
          unobserve (phase/observe!
                      (fn [event _]
                        (when (= :native-resized event) (swap! resizes inc))))]
      (try
        (d/with-transaction [tx conn]
          (swap! bodies inc)
          (d/transact! tx (mapv #(hash-map :value %) (range 15000)))
          (d/transact! tx (mapv #(hash-map :value %) (range 15000 30000)))
          (is (= 30000 (d/entries (d/datalog-kv tx) c/eav)))
          (d/transact! tx [[:db/add 1 :value -1]]))
        (is (= 1 @bodies))
        (is (pos? @resizes))
        (is (= -1 (:value (d/entity @conn 1))))
        (is (= 30000 (d/entries (d/datalog-kv conn) c/eav)))
        (finally (unobserve))))))

(deftest inner-request-sequences-match-a-register-model
  (with-open [conn (d/create-conn nil schema opts)]
    (d/transact! conn [{:db/id 1 :value 0}])
    (let [result
          (tc/quick-check
            50
            (prop/for-all [steps (gen/vector
                                  (gen/tuple (gen/elements [:add :retract :cas])
                                             (gen/choose -10 10) gen/boolean)
                                  1 40)]
              (d/with-transaction [tx conn]
                (let [matches?
                      (loop [remaining (seq steps) value 0]
                        (if-let [[op next-value read?] (first remaining)]
                          (let [after (case op :retract nil next-value)
                                request (case op
                                          :add [:db/add 1 :value next-value]
                                          :retract [:db/retract 1 :value]
                                          :cas [:db/cas 1 :value value next-value])]
                            (d/transact! tx [request])
                            (if (and read? (not= after (:value (d/entity @tx 1))))
                              false
                              (recur (next remaining) after)))
                          (= value (:value (d/entity @tx 1)))))]
                  (d/abort-transact tx)
                  matches?)))
            :seed 42)]
      (is (:pass? result) (pr-str result))
      (is (= 0 (:value (d/entity @conn 1)))))))
