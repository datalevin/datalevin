(ns datalevin.jepsen.smoke-test
  (:require
   [clojure.test :refer [deftest is testing]]
   [datalevin.core :as d]
   [datalevin.jepsen.core :as core]
   [datalevin.jepsen.local :as local]
   [datalevin.jepsen.nemesis :as nemesis]
   [datalevin.jepsen.workload.append :as append]
   [datalevin.jepsen.workload.append-cas :as append-cas]
   [datalevin.jepsen.workload.bank :as bank]
   [datalevin.jepsen.workload.grant :as grant]
   [datalevin.jepsen.workload.internal :as internal]
   [jepsen.client :as client]
   [jepsen.db :as jdb]
   [jepsen.nemesis :as jn]
   [datalevin.util :as u])
  (:import
   [java.util UUID]))

(deftest append-workload-smoke-test
  (let [workload (append/workload {:key-count 4
                                   :min-txn-length 2
                                   :max-txn-length 3
                                   :max-writes-per-key 8})]
    (is (some? (:client workload)))
    (is (some? (:generator workload)))
    (is (some? (:checker workload)))
    (is (= append/schema (:schema workload)))))

(deftest append-cas-workload-smoke-test
  (let [workload (append-cas/workload {:key-count 4
                                       :min-txn-length 2
                                       :max-txn-length 3
                                       :max-writes-per-key 8})]
    (is (some? (:client workload)))
    (is (some? (:generator workload)))
    (is (some? (:checker workload)))
    (is (= append-cas/schema (:schema workload)))))

(deftest grant-workload-smoke-test
  (let [workload (grant/workload {:key-count 4})]
    (is (some? (:client workload)))
    (is (some? (:generator workload)))
    (is (some? (:checker workload)))
    (is (some? (:final-generator workload)))
    (is (= grant/schema (:schema workload)))))

(deftest bank-workload-smoke-test
  (let [workload (bank/workload {:key-count 4
                                 :account-balance 100
                                 :max-transfer 5})]
    (is (some? (:client workload)))
    (is (some? (:generator workload)))
    (is (some? (:checker workload)))
    (is (some? (:final-generator workload)))
    (is (= bank/schema (:schema workload)))))

(deftest bank-workload-rejects-too-few-accounts-test
  (is (thrown-with-msg?
       clojure.lang.ExceptionInfo
       #"requires at least 2 accounts"
       (bank/workload {:key-count 1
                       :account-balance 100
                       :max-transfer 5}))))

(deftest internal-workload-smoke-test
  (let [workload (internal/workload {})]
    (is (some? (:client workload)))
    (is (some? (:generator workload)))
    (is (some? (:checker workload)))
    (is (= internal/schema (:schema workload)))))

(deftest datalevin-test-smoke-test
  (let [test-map (core/datalevin-test {:db-name "smoke"
                                       :control-backend :in-memory
                                       :workload :append
                                       :rate 10
                                       :time-limit 5
                                       :key-count 4
                                       :min-txn-length 2
                                       :max-txn-length 3
                                       :max-writes-per-key 8
                                       :nodes ["n1" "n2" "n3"]
                                       :nemesis []})]
    (is (= ["n1" "n2" "n3"] (:nodes test-map)))
    (is (= :in-memory (:control-backend test-map)))
    (is (some? (:db test-map)))
    (is (some? (:client test-map)))
    (is (some? (:generator test-map)))
    (is (some? (:checker test-map)))))

(deftest datalevin-append-cas-test-smoke-test
  (let [test-map (core/datalevin-test {:db-name "smoke"
                                       :control-backend :in-memory
                                       :workload :append-cas
                                       :rate 10
                                       :time-limit 5
                                       :key-count 4
                                       :min-txn-length 2
                                       :max-txn-length 3
                                       :max-writes-per-key 8
                                       :nodes ["n1" "n2" "n3"]
                                       :nemesis []})]
    (is (= ["n1" "n2" "n3"] (:nodes test-map)))
    (is (= :in-memory (:control-backend test-map)))
    (is (some? (:db test-map)))
    (is (some? (:client test-map)))
    (is (some? (:generator test-map)))
    (is (some? (:checker test-map)))))

(deftest datalevin-grant-test-smoke-test
  (let [test-map (core/datalevin-test {:db-name "smoke"
                                       :control-backend :in-memory
                                       :workload :grant
                                       :rate 10
                                       :time-limit 5
                                       :key-count 4
                                       :nodes ["n1" "n2" "n3"]
                                       :nemesis []})]
    (is (= ["n1" "n2" "n3"] (:nodes test-map)))
    (is (= :in-memory (:control-backend test-map)))
    (is (some? (:db test-map)))
    (is (some? (:client test-map)))
    (is (some? (:generator test-map)))
    (is (some? (:checker test-map)))))

(deftest datalevin-bank-test-smoke-test
  (let [test-map (core/datalevin-test {:db-name "smoke"
                                       :control-backend :in-memory
                                       :workload :bank
                                       :rate 10
                                       :time-limit 5
                                       :key-count 4
                                       :account-balance 100
                                       :max-transfer 5
                                       :nodes ["n1" "n2" "n3"]
                                       :nemesis []})]
    (is (= ["n1" "n2" "n3"] (:nodes test-map)))
    (is (= :in-memory (:control-backend test-map)))
    (is (some? (:db test-map)))
    (is (some? (:client test-map)))
    (is (some? (:generator test-map)))
    (is (some? (:checker test-map)))))

(deftest datalevin-internal-test-smoke-test
  (let [test-map (core/datalevin-test {:db-name "smoke"
                                       :control-backend :in-memory
                                       :workload :internal
                                       :rate 10
                                       :time-limit 5
                                       :nodes ["n1" "n2" "n3"]
                                       :nemesis []})]
    (is (= ["n1" "n2" "n3"] (:nodes test-map)))
    (is (= :in-memory (:control-backend test-map)))
    (is (some? (:db test-map)))
    (is (some? (:client test-map)))
    (is (some? (:generator test-map)))
    (is (some? (:checker test-map)))))

(deftest bank-client-transfer-smoke-test
  (let [cluster-id (str (UUID/randomUUID))
        test-map   {:db-name "bank-smoke"
                    :control-backend :in-memory
                    :nodes ["n1" "n2" "n3"]
                    :verbose false
                    :datalevin/cluster-id cluster-id}
        db         (local/db cluster-id)
        client     (bank/->Client nil 4 100)]
    (try
      (doseq [node (:nodes test-map)]
        (jdb/setup! db test-map node))
      (let [opened  (client/open! client test-map "n1")
            _       (client/setup! opened test-map)
            write   (client/invoke! opened
                                    test-map
                                    {:type :invoke
                                     :f :transfer
                                     :value {:from 0 :to 1 :amount 5}})
            read-op (client/invoke! opened
                                    test-map
                                    {:type :invoke
                                     :f :read-all})
            totals  (:value read-op)]
        (is (= :ok (:type write)))
        (is (= :ok (:type read-op)))
        (is (= 4 (count totals)))
        (is (= 400 (reduce + 0 totals))))
      (finally
        (doseq [node (:nodes test-map)]
          (jdb/teardown! db test-map node))))))

(deftest nemesis-spec-smoke-test
  (is (= [:leader-failover]
         (core/parse-nemesis-spec "failover")))
  (is (= [:quorum-loss]
         (core/parse-nemesis-spec "quorum")))
  (is (= [:quorum-loss]
         (core/parse-nemesis-spec "quorum-loss")))
  (is (= [:clock-skew-pause]
         (core/parse-nemesis-spec "clock-skew")))
  (is (= [:clock-skew-pause]
         (core/parse-nemesis-spec "clock-skew-pause")))
  (is (= [:leader-failover]
         (core/parse-nemesis-spec "leader-failover")))
  (let [{:keys [nemesis generator final-generator]}
        (nemesis/nemesis-package {:faults [:leader-failover]})]
    (is (some? nemesis))
    (is (some? generator))
    (is (some? final-generator)))
  (let [{:keys [nemesis generator final-generator]}
        (nemesis/nemesis-package {:faults [:quorum-loss]})]
    (is (some? nemesis))
    (is (some? generator))
    (is (some? final-generator)))
  (let [{:keys [nemesis generator final-generator]}
        (nemesis/nemesis-package {:faults [:clock-skew-pause]})]
    (is (some? nemesis))
    (is (some? generator))
    (is (some? final-generator))))

(deftest datalevin-test-with-failover-smoke-test
  (let [test-map (core/datalevin-test {:db-name "smoke"
                                       :control-backend :sofa-jraft
                                       :workload :append
                                       :rate 10
                                       :time-limit 5
                                       :key-count 4
                                       :min-txn-length 1
                                       :max-txn-length 1
                                       :max-writes-per-key 8
                                       :nodes ["n1" "n2" "n3"]
                                       :nemesis [:leader-failover]})]
    (is (= [:leader-failover] (:datalevin/nemesis-faults test-map)))
    (is (some? (:nemesis test-map)))
    (is (some? (:generator test-map)))))

(deftest datalevin-test-with-quorum-loss-smoke-test
  (let [test-map (core/datalevin-test {:db-name "smoke"
                                       :control-backend :sofa-jraft
                                       :workload :append
                                       :rate 10
                                       :time-limit 5
                                       :key-count 4
                                       :min-txn-length 1
                                       :max-txn-length 1
                                       :max-writes-per-key 8
                                       :nodes ["n1" "n2" "n3"]
                                       :nemesis [:quorum-loss]})]
    (is (= [:quorum-loss] (:datalevin/nemesis-faults test-map)))
    (is (some? (:nemesis test-map)))
    (is (some? (:generator test-map)))))

(deftest datalevin-test-with-clock-skew-smoke-test
  (let [test-map (core/datalevin-test {:db-name "smoke"
                                       :control-backend :sofa-jraft
                                       :workload :append
                                       :rate 10
                                       :time-limit 5
                                       :key-count 4
                                       :min-txn-length 1
                                       :max-txn-length 1
                                       :max-writes-per-key 8
                                       :nodes ["n1" "n2" "n3"]
                                       :nemesis [:clock-skew-pause]})]
    (is (= [:clock-skew-pause] (:datalevin/nemesis-faults test-map)))
    (is (some? (:nemesis test-map)))
    (is (some? (:generator test-map)))))

(deftest nemesis-kill-leader-preserves-info-when-no-replacement-yet-test
  (let [nemesis-obj (#'nemesis/leader-failover-nemesis)
        op          {:type :info :process :nemesis :f :kill-leader}]
    (with-redefs [local/wait-for-single-leader! (fn
                                                  ([_] {:leader "n1"})
                                                  ([_ _timeout-ms]
                                                   {:leader "n1"}))
                  local/stop-node!             (fn [_cluster-id _logical-node]
                                                 true)
                  local/maybe-wait-for-single-leader
                  (fn
                    ([_cluster-id] nil)
                    ([_cluster-id _timeout-ms] nil))]
      (is (= {:type :info
              :process :nemesis
              :f :kill-leader
              :value {:stopped "n1"
                      :leader nil
                      :status :leader-unavailable}}
             (jn/invoke! nemesis-obj
                         {:datalevin/cluster-id "smoke"}
                         op))))))

(deftest datalevin-test-rejects-in-memory-failover-smoke-test
  (testing "HA disruption nemeses need persisted JRaft membership"
    (is (thrown-with-msg?
         clojure.lang.ExceptionInfo
         #"HA disruption nemeses currently require --control-backend sofa-jraft"
         (core/datalevin-test {:db-name "smoke"
                               :control-backend :in-memory
                               :workload :append
                               :rate 10
                               :time-limit 5
                               :key-count 4
                               :min-txn-length 1
                               :max-txn-length 1
                               :max-writes-per-key 8
                               :nodes ["n1" "n2" "n3"]
                               :nemesis [:leader-failover]})))
    (is (thrown-with-msg?
         clojure.lang.ExceptionInfo
         #"HA disruption nemeses currently require --control-backend sofa-jraft"
         (core/datalevin-test {:db-name "smoke"
                               :control-backend :in-memory
                               :workload :append
                               :rate 10
                               :time-limit 5
                               :key-count 4
                               :min-txn-length 1
                               :max-txn-length 1
                               :max-writes-per-key 8
                               :nodes ["n1" "n2" "n3"]
                               :nemesis [:quorum-loss]})))
    (is (thrown-with-msg?
         clojure.lang.ExceptionInfo
         #"HA disruption nemeses currently require --control-backend sofa-jraft"
         (core/datalevin-test {:db-name "smoke"
                               :control-backend :in-memory
                               :workload :append
                               :rate 10
                               :time-limit 5
                               :key-count 4
                               :min-txn-length 1
                               :max-txn-length 1
                               :max-writes-per-key 8
                               :nodes ["n1" "n2" "n3"]
                               :nemesis [:clock-skew-pause]})))))

(deftest execute-mixed-transaction-smoke-test
  (let [dir  (str (System/getProperty "java.io.tmpdir")
                  "/datalevin-jepsen-mixed-"
                  (UUID/randomUUID))
        conn (d/create-conn dir append/schema)]
    (try
      (d/transact! conn [{:append/key 1 :append/value 10}])
      (is (= [[:r 1 [10]]
              [:append 1 11]
              [:r 1 [10 11]]]
             (#'append/execute-txn! conn
              [[:r 1 nil]
               [:append 1 11]
               [:r 1 nil]])))
      (is (= [10 11]
             (#'append/read-list @conn 1)))
      (finally
        (d/close conn)
        (u/delete-files dir)))))

(deftest execute-append-cas-transaction-smoke-test
  (let [dir  (str (System/getProperty "java.io.tmpdir")
                  "/datalevin-jepsen-append-cas-"
                  (UUID/randomUUID))
        conn (d/create-conn dir append-cas/schema)]
    (try
      (d/transact! conn [{:append.meta/key 1 :append.meta/version 0}
                         {:append/key 1 :append/value 10}])
      (is (= [[:r 1 [10]]
              [:append 1 11]
              [:r 1 [10 11]]]
             (#'append-cas/execute-txn! conn
              [[:r 1 nil]
               [:append 1 11]
               [:r 1 nil]])))
      (is (= [10 11]
             (#'append-cas/read-list @conn 1)))
      (is (= 1
             (#'append-cas/current-version @conn 1)))
      (finally
        (d/close conn)
        (u/delete-files dir)))))

(deftest execute-append-cas-initializes-missing-meta-smoke-test
  (let [dir  (str (System/getProperty "java.io.tmpdir")
                  "/datalevin-jepsen-append-cas-missing-meta-"
                  (UUID/randomUUID))
        conn (d/create-conn dir append-cas/schema)]
    (try
      (is (= [[:append 1 11]]
             (#'append-cas/execute-txn! conn
              [[:append 1 11]])))
      (is (= [11]
             (#'append-cas/read-list @conn 1)))
      (is (= 1
             (#'append-cas/current-version @conn 1)))
      (finally
        (d/close conn)
        (u/delete-files dir)))))

(deftest execute-grant-transaction-function-smoke-test
  (let [dir  (str (System/getProperty "java.io.tmpdir")
                  "/datalevin-jepsen-grant-"
                  (UUID/randomUUID))
        conn (d/create-conn dir grant/schema)]
    (try
      (#'grant/ensure-tx-fns! conn)
      (let [created (#'grant/execute-op! conn {:f :create
                                               :value {:grant-id 1
                                                       :amount 100}})]
        (is (= 1 (:grant-id created)))
        (is (= :pending (:status created)))
        (is (= 100 (:amount created)))
        (is (integer? (:requested-at created)))
        (is (nil? (:approved-at created)))
        (is (nil? (:denied-at created))))
      (let [approved (#'grant/execute-op! conn {:f :approve
                                                :value {:grant-id 1}})
            denied   (#'grant/execute-op! conn {:f :deny
                                                :value {:grant-id 1}})
            all      (#'grant/execute-op! conn {:f :read-all})]
        (is (= :approved (:status approved)))
        (is (= approved denied))
        (is (= [approved] all)))
      (finally
        (d/close conn)
        (u/delete-files dir)))))

(deftest execute-bank-transfer-smoke-test
  (let [dir  (str (System/getProperty "java.io.tmpdir")
                  "/datalevin-jepsen-bank-"
                  (UUID/randomUUID))
        conn (d/create-conn dir bank/schema)]
    (try
      (#'bank/ensure-tx-fns! conn)
      (#'bank/ensure-accounts! conn 4 100)
      (let [transfer (#'bank/execute-op! conn
                                         4
                                         {:f :transfer
                                          :value {:from 0
                                                  :to 1
                                                  :amount 5}})
            balances (#'bank/execute-op! conn 4 {:f :read-all})]
        (is (= {:from 0
                :to 1
                :amount 5
                :applied? true
                :from-balance 95
                :to-balance 105}
               transfer))
        (is (= [95 105 100 100]
               balances)))
      (finally
        (d/close conn)
        (u/delete-files dir)))))

(deftest execute-internal-transaction-cases-smoke-test
  (let [dir  (str (System/getProperty "java.io.tmpdir")
                  "/datalevin-jepsen-internal-"
                  (UUID/randomUUID))
        conn (d/create-conn dir internal/schema)]
    (try
      (doseq [op [{:f :lookup-ref-same :internal/case-id 1}
                  {:f :tx-fn-after-add :internal/case-id 2}
                  {:f :tx-fn-twice :internal/case-id 3}
                  {:f :cas-chain :internal/case-id 4}
                  {:f :retract-add :internal/case-id 5}
                  {:f :tempid-ref :internal/case-id 6}]]
        (let [actual (try
                       {:type  :ok
                        :value (#'internal/execute-op! conn op)}
                       (catch Throwable e
                         {:type  :fail
                          :error (#'internal/op-error e)}))]
          (is (= (#'internal/expected-outcome op)
                 actual))))
      (finally
        (d/close conn)
        (u/delete-files dir)))))
