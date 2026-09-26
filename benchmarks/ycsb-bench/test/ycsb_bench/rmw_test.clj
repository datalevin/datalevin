(ns ycsb-bench.rmw-test
  (:require [clojure.test :refer [deftest is testing]]
            [datalevin.core :as d]
            [ycsb-bench.runner :as runner]
            [ycsb-bench.sql :as sql]
            [ycsb-bench.store :as store]
            [ycsb-bench.workload :as w])
  (:import [java.sql Connection]
           [java.util Random]))

(def options
  (runner/options {:workload :f :records 8 :ops 100 :warmup 20
                   :threads 2 :pool-size 2 :distribution :uniform
                   :field-count 3 :field-length 4 :timeout-ms 5000
                   :server-mode :in-process :datalog-handles :independent}))

(def conditions
  (cond-> [[:datalevin :kv :embedded] [:datalevin :datalog :embedded]
           [:datalevin :kv :remote] [:datalevin :datalog :remote]
           [:sqlite :kv :embedded] [:sqlite :datalog :embedded]]
    (System/getenv "YCSB_PG_URL")
    (into [[:postgres :kv :remote] [:postgres :datalog :remote]])))

(deftest upstream-read-update-order-and-independent-value
  (doseq [old-value ["aaaa" "yyyy"]]
    (let [events (atom [])
          key (w/application-key 0)
          rng (proxy [Random] [] (nextInt [_bound] 1))
          db (reify store/Records
               (read-record [_ id]
                 (swap! events conj [:read id])
                 [old-value old-value old-value])
               (update-field! [_ id field value]
                 (swap! events conj [:update id field value])))]
      (with-redefs [w/choose-key (fn [& _] 0)
                    w/random-value (fn [_ length]
                                     (swap! events conj [:generate length]) "zzzz")]
        (#'runner/execute! db (w/keyspace 8) rng nil options :rmw))
      (is (= [[:generate 4] [:read key] [:update key 1 "zzzz"]] @events)
          "Generate an independent value, then read and update the same key"))))

(deftest read-failure-invalidates-rmw-before-update
  (let [updated? (atom false)
        failure (ex-info "Read failed" {})
        db (reify store/Records
             (read-record [_ _] (throw failure))
             (update-field! [_ _ _ _] (reset! updated? true)))]
    (is (identical? failure (try (#'runner/execute! db (w/keyspace 8) (Random. 17)
                                                  nil options :rmw)
                                (catch Exception e e))))
    (is (false? @updated?))))

(deftest f-counts-the-pair-as-one-logical-operation
  (let [reads (atom 0)
        updates (atom 0)
        db (reify store/Records
             (read-record [_ _] (swap! reads inc) ["aaaa" "bbbb" "cccc"])
             (update-field! [_ _ _ _] (swap! updates inc)))
        result (runner/run-phase! db (w/keyspace 8) nil options :measured 100)]
    (is (= 100 (:operations result) @reads))
    (is (pos? @updates))
    (is (= @updates (get-in result [:by-operation :rmw :count])))))

(deftest another-write-can-commit-between-the-read-and-update
  (doseq [[system api mode] conditions]
    (testing (str [system api mode])
      ((if (= system :datalevin) store/with-store sql/with-store)
       (assoc options :system system :api api :mode mode)
       (fn [group]
         (let [db (store/for-worker group 0)
               other (store/for-worker group 1)
               opts (assoc options :system system)
               key (w/application-key 0)
               events (atom [])
               wrapper (reify store/Records
                         (read-record [_ id]
                           (let [value (store/read-record db id)]
                             (swap! events conj [:read value])
                             ;; This write must not wait for an enclosing F
                             ;; transaction or get overwritten by the final update.
                             (store/update-field! other id 2 "peer")
                             (swap! events conj :other-committed)
                             value))
                         (update-field! [_ id field value]
                           (swap! events conj [:update field value])
                           (store/update-field! db id field value)))
               rng (proxy [Random] [] (nextInt [_bound] 1))]
           (store/put-records! db [[key ["aaaa" "bbbb" "cccc"]]])
           (with-redefs [w/choose-key (fn [& _] 0)
                         w/random-value (fn [& _] "zzzz")]
             (#'runner/execute! wrapper (w/keyspace 1) rng nil opts :rmw))
           (is (= [[:read ["aaaa" "bbbb" "cccc"]] :other-committed [:update 1 "zzzz"]]
                  @events))
           (is (= ["aaaa" "zzzz" "peer"] (store/read-record db key)))
           (is (= {:rmw-execution :client-read-update :atomic-rmw? false}
                  (select-keys (store/storage-info db) [:rmw-execution :atomic-rmw?])))
           (when (and (= system :datalevin) (= api :datalog))
             (is (empty? (d/q '[:find ?e :where [?e :db/fn]] @(:conn db)))
                 "Opening the benchmark does not install a transaction function"))
           (when (#{:sqlite :postgres} system)
             (is (.getAutoCommit ^Connection (:connection db))))
           {}))))))

(deftest f-load-warmup-and-measurement-use-upstream-semantics
  (doseq [[system api mode] conditions]
    (let [result (runner/run-case! (assoc options :system system :api api :mode mode
                                                :value-audit? true))]
      (is (= :ycsb-read-update-v1 (get-in result [:configuration :workload-model])))
      (is (false? (get-in result [:configuration :atomic-rmw?])))
      (is (= :client-read-update (get-in result [:storage :rmw-execution])))
      (is (= :passed (get-in result [:validation :status])))
      (is (= :passed (get-in result [:validation :value-checks :status])))
      (is (= 100 (get-in result [:validation :value-checks :point-reads])))
      (is (= 20 (get-in result [:warmup :validation :value-checks :point-reads])))
      (is (= 8 (get-in result [:validation :records])))
      (is (= 100 (get-in result [:measured :operations])))
      (is (pos? (get-in result [:measured :by-operation :rmw :count]))))))
