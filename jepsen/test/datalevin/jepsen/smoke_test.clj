(ns datalevin.jepsen.smoke-test
  (:require
   [clojure.test :refer [deftest is testing]]
   [datalevin.client :as cl]
   [datalevin.core :as d]
   [datalevin.jepsen.core :as core]
   [datalevin.jepsen.local :as local]
   [datalevin.jepsen.nemesis :as nemesis]
   [datalevin.jepsen.workload.append :as append]
   [datalevin.jepsen.workload.append-cas :as append-cas]
   [datalevin.jepsen.workload.bank :as bank]
   [datalevin.jepsen.workload.fencing :as fencing]
   [datalevin.jepsen.workload.giant-values :as giant-values]
   [datalevin.jepsen.workload.grant :as grant]
   [datalevin.jepsen.workload.identity-upsert :as identity-upsert]
   [datalevin.jepsen.workload.index-consistency :as index-consistency]
   [datalevin.jepsen.workload.internal :as internal]
   [datalevin.jepsen.workload.rejoin-bootstrap :as rejoin-bootstrap]
   [datalevin.jepsen.workload.register :as register]
   [datalevin.jepsen.workload.tx-fn-register :as tx-fn-register]
   [jepsen.checker :as checker]
   [jepsen.client :as client]
   [jepsen.db :as jdb]
   [jepsen.history :as history]
   [jepsen.net.proto :as net.proto]
   [jepsen.nemesis :as jn]
   [datalevin.util :as u])
  (:import
   [java.util UUID]))

(defn- wait-for-leader-append-write!
  [test key value timeout-ms]
  (let [deadline (+ (System/currentTimeMillis) (long timeout-ms))]
    (loop [last-error nil]
      (let [result (try
                     (local/with-leader-conn
                       test
                       append/schema
                       (fn [conn]
                         (d/transact! conn [{:append/key key
                                             :append/value value}])
                         :committed))
                     (catch Throwable e
                       e))]
        (cond
          (= :committed result)
          :committed

          (and (instance? Throwable result)
               (< (System/currentTimeMillis) deadline)
               (or (local/transport-failure? result)
                   (local/expected-disruption-write-failure? test result)))
          (do
            (Thread/sleep 250)
            (recur result))

          (instance? Throwable result)
          (throw result)

          :else
          (throw (ex-info "Timed out waiting for append write"
                          {:test (:db-name test)
                           :key key
                           :value value
                           :timeout-ms timeout-ms
                           :last-error (some-> last-error ex-message)})))))))

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

(deftest giant-values-workload-smoke-test
  (let [workload (giant-values/workload {:key-count 4
                                         :nodes ["n1" "n2" "n3"]})]
    (is (some? (:client workload)))
    (is (some? (:generator workload)))
    (is (some? (:checker workload)))
    (is (= giant-values/schema (:schema workload)))))

(deftest bank-workload-rejects-too-few-accounts-test
  (is (thrown-with-msg?
       clojure.lang.ExceptionInfo
       #"requires at least 2 accounts"
       (bank/workload {:key-count 1
                       :account-balance 100
                       :max-transfer 5}))))

(deftest fencing-workload-smoke-test
  (let [workload (fencing/workload {})]
    (is (some? (:client workload)))
    (is (some? (:generator workload)))
    (is (some? (:checker workload)))
    (is (some? (:final-generator workload)))
    (is (= fencing/schema (:schema workload)))))

(deftest internal-workload-smoke-test
  (let [workload (internal/workload {})]
    (is (some? (:client workload)))
    (is (some? (:generator workload)))
    (is (some? (:checker workload)))
    (is (= internal/schema (:schema workload)))))

(deftest local-port-reservation-uses-dedicated-server-range-test
  (let [ports (#'local/reserve-ports 6)]
    (is (= 6 (count ports)))
    (is (= 6 (count (set ports))))
    (is (every? #(<= 19001 % 31999) ports))
    (is (= ports
           (vec (range (first ports)
                       (+ (first ports) (count ports))))))))

(deftest random-graph-cut-covers-live-nodes-test
  (let [cluster-id    (str (UUID/randomUUID))
        clusters-atom @#'local/clusters
        snapshot      @clusters-atom]
    (try
      (swap! clusters-atom assoc cluster-id {:live-nodes #{"n1" "n2" "n3"}})
      (let [{:keys [groups pair-cuts grudge dropped-links]}
            (local/random-graph-cut cluster-id)]
        (is (seq groups))
        (is (<= 2 (count groups)))
        (is (every? seq groups))
        (is (= #{"n1" "n2" "n3"}
               (set (mapcat identity groups))))
        (is (seq pair-cuts))
        (is (seq grudge))
        (is (= dropped-links
               (#'local/grudge->dropped-links grudge))))
      (finally
        (reset! clusters-atom snapshot)))))

(deftest random-degraded-network-shape-is-heterogeneous-test
  (let [cluster-id    (str (UUID/randomUUID))
        clusters-atom @#'local/clusters
        snapshot      @clusters-atom]
    (try
      (swap! clusters-atom assoc cluster-id {:live-nodes #{"n1" "n2" "n3"}})
      (let [{:keys [kind nodes link-profiles profile-summary]}
            (local/random-degraded-network-shape cluster-id)]
        (is (= :heterogeneous kind))
        (is (= #{"n1" "n2" "n3"} (set nodes)))
        (is (= 6 (count link-profiles)))
        (is (> (:distinct-profile-count profile-summary) 1))
        (is (<= (get-in profile-summary [:delay-ms :min])
                (get-in profile-summary [:delay-ms :max])))
        (is (pos? (get-in profile-summary [:drop-probability :max]))))
      (finally
        (reset! clusters-atom snapshot)))))

(deftest local-storage-stall-hook-blocks-until-healed-test
  (let [cluster-id    (str (UUID/randomUUID))
        db-identity   (str "db-" (UUID/randomUUID))
        clusters-atom @#'local/clusters
        snapshot      @clusters-atom]
    (try
      (swap! clusters-atom assoc
             cluster-id
             {:db-identity db-identity
              :node-by-id {1 "n1"}
              :storage-faults {"n1" {:mode :stall
                                     :stages #{:txlog-sync}}}})
      (let [fut (future
                  (local/maybe-apply-storage-fault!
                   {:db-identity db-identity
                    :ha-node-id 1
                    :stage :txlog-sync})
                  :released)]
        (Thread/sleep 200)
        (is (not (realized? fut)))
        (swap! clusters-atom update-in [cluster-id :storage-faults] dissoc "n1")
        (is (= :released (deref fut 2000 ::timeout))))
      (finally
        (reset! clusters-atom snapshot)))))

(deftest local-storage-disk-full-hook-throws-test
  (let [cluster-id    (str (UUID/randomUUID))
        db-identity   (str "db-" (UUID/randomUUID))
        clusters-atom @#'local/clusters
        snapshot      @clusters-atom]
    (try
      (swap! clusters-atom assoc
             cluster-id
             {:db-identity db-identity
              :node-by-id {1 "n1"}
              :storage-faults {"n1" {:mode :disk-full
                                     :stages #{:txlog-append}}}})
      (is (thrown-with-msg?
           clojure.lang.ExceptionInfo
           #"No space left on device"
           (local/maybe-apply-storage-fault!
            {:db-identity db-identity
             :ha-node-id 1
             :stage :txlog-append})))
      (finally
        (reset! clusters-atom snapshot)))))

(deftest expected-disruption-write-failure-includes-storage-faults-test
  (is (true? (local/expected-disruption-write-failure?
              {:datalevin/nemesis-faults [:leader-io-stall]}
              "Timeout in making request")))
  (is (true? (local/expected-disruption-write-failure?
              {:datalevin/nemesis-faults [:leader-disk-full]}
              "Request to Datalevin server failed: \"No space left on device\""))))

(deftest identity-upsert-workload-smoke-test
  (let [workload (identity-upsert/workload {})]
    (is (some? (:client workload)))
    (is (some? (:generator workload)))
    (is (some? (:checker workload)))
    (is (= identity-upsert/schema (:schema workload)))))

(deftest index-consistency-workload-smoke-test
  (let [workload (index-consistency/workload {})]
    (is (some? (:client workload)))
    (is (some? (:generator workload)))
    (is (some? (:checker workload)))
    (is (= index-consistency/schema (:schema workload)))))

(deftest append-checker-all-disruption-write-loss-is-valid-test
  (let [checker (:checker (append/workload {:key-count 4
                                            :max-writes-per-key 8}))
        fail-op {:type :fail
                 :f :txn
                 :value [[:append 1 1]]
                 :error "Timed out waiting for single leader"}
        pause-result
        (checker/check checker
                       {:name "smoke"
                        :start-time "20260308T000000.000-0800"
                        :datalevin/nemesis-faults [:leader-pause]}
                       (history/history [fail-op])
                       nil)
        node-pause-result
        (checker/check checker
                       {:name "smoke"
                        :start-time "20260308T000000.000-0800"
                        :datalevin/nemesis-faults [:node-pause]}
                       (history/history [fail-op])
                       nil)
        multi-node-pause-result
        (checker/check checker
                       {:name "smoke"
                        :start-time "20260308T000000.000-0800"
                        :datalevin/nemesis-faults [:multi-node-pause]}
                       (history/history [fail-op])
                       nil)
        asymmetric-result
        (checker/check checker
                       {:name "smoke"
                        :start-time "20260308T000000.000-0800"
                        :datalevin/nemesis-faults [:asymmetric-partition]}
                       (history/history [fail-op])
                       nil)
        degraded-result
        (checker/check checker
                       {:name "smoke"
                        :start-time "20260308T000000.000-0800"
                        :datalevin/nemesis-faults [:degraded-network]}
                       (history/history [fail-op])
                       nil)
        normal-result
        (checker/check checker
                       {:name "smoke"
                        :start-time "20260308T000000.000-0800"
                        :datalevin/nemesis-faults []}
                       (history/history [fail-op])
                       nil)]
    (is (true? (:valid? pause-result)))
    (is (= :disruption-only-empty-graph
           (:adjusted-valid? pause-result)))
    (is (= 1 (:disruption-failure-count pause-result)))
    (is (true? (:valid? node-pause-result)))
    (is (= 1 (:disruption-failure-count node-pause-result)))
    (is (true? (:valid? multi-node-pause-result)))
    (is (= 1 (:disruption-failure-count multi-node-pause-result)))
    (is (true? (:valid? asymmetric-result)))
    (is (= 1 (:disruption-failure-count asymmetric-result)))
    (is (true? (:valid? degraded-result)))
    (is (= 1 (:disruption-failure-count degraded-result)))
    (is (not (true? (:valid? normal-result))))))

(deftest append-cas-checker-all-disruption-write-loss-is-valid-test
  (let [checker (:checker (append-cas/workload {:key-count 4
                                                :max-writes-per-key 8}))
        fail-op {:type :fail
                 :f :txn
                 :value [[:append 1 1]]
                 :error "Timed out waiting for single leader"}
        pause-result
        (checker/check checker
                       {:name "smoke"
                        :start-time "20260308T000000.000-0800"
                        :datalevin/nemesis-faults [:leader-pause]}
                       (history/history [fail-op])
                       nil)
        normal-result
        (checker/check checker
                       {:name "smoke"
                        :start-time "20260308T000000.000-0800"
                        :datalevin/nemesis-faults []}
                       (history/history [fail-op])
                       nil)]
    (is (true? (:valid? pause-result)))
    (is (= :disruption-only-empty-graph
           (:adjusted-valid? pause-result)))
    (is (= 1 (:disruption-failure-count pause-result)))
    (is (not (true? (:valid? normal-result))))))

(deftest internal-checker-tolerates-leader-disruption-write-loss-test
  (let [checker (:checker (internal/workload {}))
        ok-op    {:type :ok
                  :f :lookup-ref-same
                  :internal/case-id 1
                  :value (#'internal/expected-states
                          {:f :lookup-ref-same
                           :internal/case-id 1})}
        fail-op  {:type :fail
                  :f :tempid-ref
                  :internal/case-id 2
                  :error "Request to Datalevin server failed: \"HA write admission rejected\""}
        partition-result
        (checker/check checker
                       {:datalevin/nemesis-faults [:leader-partition]}
                       [ok-op fail-op]
                       nil)
        pause-result
        (checker/check checker
                       {:datalevin/nemesis-faults [:leader-pause]}
                       [ok-op fail-op]
                       nil)
        node-pause-result
        (checker/check checker
                       {:datalevin/nemesis-faults [:node-pause]}
                       [ok-op fail-op]
                       nil)
        multi-node-pause-result
        (checker/check checker
                       {:datalevin/nemesis-faults [:multi-node-pause]}
                       [ok-op fail-op]
                       nil)
        normal-result
        (checker/check checker
                       {:datalevin/nemesis-faults []}
                       [ok-op fail-op]
                       nil)]
    (is (true? (:valid? partition-result)))
    (is (= 1 (:disruption-failure-count partition-result)))
    (is (true? (:valid? pause-result)))
    (is (= 1 (:disruption-failure-count pause-result)))
    (is (true? (:valid? node-pause-result)))
    (is (= 1 (:disruption-failure-count node-pause-result)))
    (is (true? (:valid? multi-node-pause-result)))
    (is (= 1 (:disruption-failure-count multi-node-pause-result)))
    (is (false? (:valid? normal-result)))))

(deftest internal-checker-all-disruption-write-loss-is-valid-test
  (let [checker (:checker (internal/workload {}))
        fail-op  {:type :fail
                  :f :lookup-ref-same
                  :internal/case-id 1
                  :error "Timed out waiting for single leader"}
        pause-result
        (checker/check checker
                       {:datalevin/nemesis-faults [:leader-pause]}
                       [fail-op]
                       nil)
        normal-result
        (checker/check checker
                       {:datalevin/nemesis-faults []}
                       [fail-op]
                       nil)]
    (is (true? (:valid? pause-result)))
    (is (= 1 (:disruption-failure-count pause-result)))
    (is (false? (:valid? normal-result)))))

(deftest identity-upsert-checker-tolerates-leader-disruption-write-loss-test
  (let [checker (:checker (identity-upsert/workload {}))
        ok-op    {:type :ok
                  :f :upsert-same-tempid
                  :identity/case-id 1
                  :value (#'identity-upsert/expected-states
                          {:f :upsert-same-tempid
                           :identity/case-id 1})}
        fail-op  {:type :fail
                  :f :lookup-ref-cas
                  :identity/case-id 2
                  :error "Request to Datalevin server failed: \"HA write admission rejected\""}
        partition-result
        (checker/check checker
                       {:datalevin/nemesis-faults [:leader-partition]}
                       [ok-op fail-op]
                       nil)
        pause-result
        (checker/check checker
                       {:datalevin/nemesis-faults [:leader-pause]}
                       [ok-op fail-op]
                       nil)
        normal-result
        (checker/check checker
                       {:datalevin/nemesis-faults []}
                       [ok-op fail-op]
                       nil)]
    (is (true? (:valid? partition-result)))
    (is (= 1 (:disruption-failure-count partition-result)))
    (is (true? (:valid? pause-result)))
    (is (= 1 (:disruption-failure-count pause-result)))
    (is (false? (:valid? normal-result)))))

(deftest identity-upsert-checker-all-disruption-write-loss-is-valid-test
  (let [checker (:checker (identity-upsert/workload {}))
        fail-op  {:type :fail
                  :f :lookup-ref-intermediate
                  :identity/case-id 1
                  :error "Timed out waiting for single leader"}
        pause-result
        (checker/check checker
                       {:datalevin/nemesis-faults [:leader-pause]}
                       [fail-op]
                       nil)
        normal-result
        (checker/check checker
                       {:datalevin/nemesis-faults []}
                       [fail-op]
                       nil)]
    (is (true? (:valid? pause-result)))
    (is (= 1 (:disruption-failure-count pause-result)))
    (is (false? (:valid? normal-result)))))

(deftest index-consistency-checker-tolerates-leader-disruption-write-loss-test
  (let [checker (:checker (index-consistency/workload {}))
        ok-op    {:type :ok
                  :f :ref-create
                  :index/case-id 1
                  :value (#'index-consistency/expected-states
                          {:f :ref-create
                           :index/case-id 1})}
        fail-op  {:type :fail
                  :f :tag-swap
                  :index/case-id 2
                  :error "Timeout in making request"}
        partition-result
        (checker/check checker
                       {:datalevin/nemesis-faults [:leader-partition]}
                       [ok-op fail-op]
                       nil)
        pause-result
        (checker/check checker
                       {:datalevin/nemesis-faults [:leader-pause]}
                       [ok-op fail-op]
                       nil)
        normal-result
        (checker/check checker
                       {:datalevin/nemesis-faults []}
                       [ok-op fail-op]
                       nil)]
    (is (true? (:valid? partition-result)))
    (is (= 1 (:disruption-failure-count partition-result)))
    (is (true? (:valid? pause-result)))
    (is (= 1 (:disruption-failure-count pause-result)))
    (is (false? (:valid? normal-result)))))

(deftest index-consistency-checker-all-disruption-write-loss-is-valid-test
  (let [checker (:checker (index-consistency/workload {}))
        fail-op  {:type :fail
                  :f :ref-retarget
                  :index/case-id 1
                  :error "Timed out waiting for single leader"}
        pause-result
        (checker/check checker
                       {:datalevin/nemesis-faults [:leader-pause]}
                       [fail-op]
                       nil)
        normal-result
        (checker/check checker
                       {:datalevin/nemesis-faults []}
                       [fail-op]
                       nil)]
    (is (true? (:valid? pause-result)))
    (is (= 1 (:disruption-failure-count pause-result)))
    (is (false? (:valid? normal-result)))))

(deftest index-consistency-ref-retarget-snapshot-smoke-test
  (let [dir  (u/tmp-dir (str "jepsen-index-consistency-" (UUID/randomUUID)))
        conn (d/create-conn dir index-consistency/schema)]
    (try
      (let [snapshots (#'datalevin.jepsen.workload.index-consistency/execute-op!
                        conn
                        {:type :invoke
                         :f :ref-retarget
                         :index/case-id 1})]
        (is (= 2 (count snapshots)))
        (is (= :present (get-in snapshots [0 :root :entity :status])))
        (is (= #{} (get-in snapshots [0 :datoms :child-b])))
        (is (= "index-00000001-child-b"
               (get-in snapshots [1 :root :entity :ref-key])))
        (is (= #{[:index/case 1]
                 [:index/key "index-00000001-child-b"]
                 [:index/name "child-b-index-00000001"]}
               (get-in snapshots [1 :datoms :child-b]))))
      (finally
        (d/close conn)
        (u/delete-files dir)))))

(deftest register-workload-smoke-test
  (let [workload (register/workload {:key-count 4
                                     :nodes ["n1" "n2" "n3"]})]
    (is (some? (:client workload)))
    (is (some? (:generator workload)))
    (is (some? (:checker workload)))
    (is (= register/schema (:schema workload)))))

(deftest tx-fn-register-workload-smoke-test
  (let [workload (tx-fn-register/workload {:key-count 4
                                           :nodes ["n1" "n2" "n3"]})]
    (is (some? (:client workload)))
    (is (some? (:generator workload)))
    (is (some? (:checker workload)))
    (is (= tx-fn-register/schema (:schema workload)))))

(deftest rejoin-bootstrap-workload-smoke-test
  (let [workload (rejoin-bootstrap/workload {:key-count 4
                                             :nodes ["n1" "n2" "n3"]})]
    (is (some? (:client workload)))
    (is (some? (:generator workload)))
    (is (some? (:checker workload)))
    (is (some? (:final-generator workload)))
    (is (map? (:datalevin/cluster-opts workload)))
    (is (= rejoin-bootstrap/schema (:schema workload)))))

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

(deftest datalevin-test-orders-nemesis-final-before-workload-final-test
  (let [timed-gen          ::timed
        workload-final-gen {:type :invoke :f :converge}
        nemesis-final-gen  {:type :info :f :heal-partition}
        phases             (#'core/compose-generator-phases
                            timed-gen
                            workload-final-gen
                            nemesis-final-gen)]
    (is (= timed-gen (nth phases 0)))
    (is (= nemesis-final-gen (:gen (nth phases 1))))
    (is (= workload-final-gen (:gen (nth phases 2))))))

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

(deftest datalevin-giant-values-test-smoke-test
  (let [test-map (core/datalevin-test {:db-name "smoke"
                                       :control-backend :in-memory
                                       :workload :giant-values
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

(deftest datalevin-fencing-test-smoke-test
  (let [test-map (core/datalevin-test {:db-name "smoke"
                                       :control-backend :sofa-jraft
                                       :workload :fencing
                                       :rate 10
                                       :time-limit 5
                                       :nodes ["n1" "n2" "n3"]
                                       :nemesis [:leader-failover]})]
    (is (= ["n1" "n2" "n3"] (:nodes test-map)))
    (is (= :sofa-jraft (:control-backend test-map)))
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

(deftest datalevin-identity-upsert-test-smoke-test
  (let [test-map (core/datalevin-test {:db-name "smoke"
                                       :control-backend :in-memory
                                       :workload :identity-upsert
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

(deftest datalevin-index-consistency-test-smoke-test
  (let [test-map (core/datalevin-test {:db-name "smoke"
                                       :control-backend :in-memory
                                       :workload :index-consistency
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

(deftest datalevin-register-test-smoke-test
  (let [test-map (core/datalevin-test {:db-name "smoke"
                                       :control-backend :in-memory
                                       :workload :register
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

(deftest datalevin-tx-fn-register-test-smoke-test
  (let [test-map (core/datalevin-test {:db-name "smoke"
                                       :control-backend :in-memory
                                       :workload :tx-fn-register
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

(deftest datalevin-rejoin-bootstrap-test-smoke-test
  (let [test-map (core/datalevin-test {:db-name "smoke"
                                       :control-backend :sofa-jraft
                                       :workload :rejoin-bootstrap
                                       :rate 10
                                       :time-limit 5
                                       :key-count 4
                                       :nodes ["n1" "n2" "n3"]
                                       :nemesis [:follower-rejoin]})]
    (is (= ["n1" "n2" "n3"] (:nodes test-map)))
    (is (= :sofa-jraft (:control-backend test-map)))
    (is (= [:follower-rejoin] (:datalevin/nemesis-faults test-map)))
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

(deftest register-client-smoke-test
  (let [cluster-id (str (UUID/randomUUID))
        test-map   {:db-name "register-smoke"
                    :control-backend :in-memory
                    :nodes ["n1" "n2" "n3"]
                    :key-count 4
                    :verbose false
                    :datalevin/cluster-id cluster-id}
        db         (local/db cluster-id)
        client     (register/->Client nil 4)]
    (try
      (doseq [node (:nodes test-map)]
        (jdb/setup! db test-map node))
      (let [opened    (client/open! client test-map "n1")
            _         (client/setup! opened test-map)
            read-op   (client/invoke! opened
                                      test-map
                                      {:type :invoke
                                       :f :read
                                       :value (clojure.lang.MapEntry. 0 nil)})
            write-op  (client/invoke! opened
                                      test-map
                                      {:type :invoke
                                       :f :write
                                       :value (clojure.lang.MapEntry. 0 3)})
            cas-op    (client/invoke! opened
                                      test-map
                                      {:type :invoke
                                       :f :cas
                                       :value (clojure.lang.MapEntry. 0 [3 4])})
            final-op  (client/invoke! opened
                                      test-map
                                      {:type :invoke
                                       :f :read
                                       :value (clojure.lang.MapEntry. 0 nil)})]
        (is (= :ok (:type read-op)))
        (is (= (clojure.lang.MapEntry. 0 0) (:value read-op)))
        (is (= :ok (:type write-op)))
        (is (= (clojure.lang.MapEntry. 0 3) (:value write-op)))
        (is (= :ok (:type cas-op)))
        (is (= (clojure.lang.MapEntry. 0 [3 4]) (:value cas-op)))
        (is (= :ok (:type final-op)))
        (is (= (clojure.lang.MapEntry. 0 4) (:value final-op))))
      (finally
        (doseq [node (:nodes test-map)]
          (jdb/teardown! db test-map node))))))

(deftest giant-values-client-smoke-test
  (let [cluster-id (str (UUID/randomUUID))
        test-map   {:db-name "giant-values-smoke"
                    :control-backend :in-memory
                    :nodes ["n1" "n2" "n3"]
                    :key-count 4
                    :verbose false
                    :datalevin/cluster-id cluster-id}
        db         (local/db cluster-id)
        client     (giant-values/->Client nil 4 12000)]
    (try
      (doseq [node (:nodes test-map)]
        (jdb/setup! db test-map node))
      (let [opened   (client/open! client test-map "n1")
            _        (client/setup! opened test-map)
            read-op  (client/invoke! opened
                                     test-map
                                     {:type :invoke
                                      :f :read
                                      :value (clojure.lang.MapEntry. 0 nil)})
            write-op (client/invoke! opened
                                     test-map
                                     {:type :invoke
                                      :f :write
                                      :value (clojure.lang.MapEntry. 0 7)})
            cas-op   (client/invoke! opened
                                     test-map
                                     {:type :invoke
                                      :f :cas
                                      :value (clojure.lang.MapEntry. 0 [7 9])})
            final-op (client/invoke! opened
                                     test-map
                                     {:type :invoke
                                      :f :read
                                      :value (clojure.lang.MapEntry. 0 nil)})]
        (is (= :ok (:type read-op)))
        (is (= (clojure.lang.MapEntry. 0 0) (:value read-op)))
        (is (true? (:giant/payload-valid? read-op)))
        (is (= :ok (:type write-op)))
        (is (= (clojure.lang.MapEntry. 0 7) (:value write-op)))
        (is (true? (:giant/payload-valid? write-op)))
        (is (= :ok (:type cas-op)))
        (is (= (clojure.lang.MapEntry. 0 [7 9]) (:value cas-op)))
        (is (true? (:giant/payload-valid? cas-op)))
        (is (= :ok (:type final-op)))
        (is (= (clojure.lang.MapEntry. 0 9) (:value final-op)))
        (is (true? (:giant/payload-valid? final-op))))
      (finally
        (doseq [node (:nodes test-map)]
          (jdb/teardown! db test-map node))))))

(deftest tx-fn-register-client-smoke-test
  (let [cluster-id (str (UUID/randomUUID))
        test-map   {:db-name "tx-fn-register-smoke"
                    :control-backend :in-memory
                    :nodes ["n1" "n2" "n3"]
                    :key-count 4
                    :verbose false
                    :datalevin/cluster-id cluster-id}
        db         (local/db cluster-id)
        client     (tx-fn-register/->Client nil 4 12000)]
    (try
      (doseq [node (:nodes test-map)]
        (jdb/setup! db test-map node))
      (let [opened   (client/open! client test-map "n1")
            _        (client/setup! opened test-map)
            read-op  (client/invoke! opened
                                     test-map
                                     {:type :invoke
                                      :f :read
                                      :value (clojure.lang.MapEntry. 0 nil)})
            write-op (client/invoke! opened
                                     test-map
                                     {:type :invoke
                                      :f :write
                                      :value (clojure.lang.MapEntry. 0 7)})
            cas-op   (client/invoke! opened
                                     test-map
                                     {:type :invoke
                                      :f :cas
                                      :value (clojure.lang.MapEntry. 0 [7 9])})
            final-op (client/invoke! opened
                                     test-map
                                     {:type :invoke
                                      :f :read
                                      :value (clojure.lang.MapEntry. 0 nil)})]
        (is (= :ok (:type read-op)))
        (is (= (clojure.lang.MapEntry. 0 0) (:value read-op)))
        (is (true? (:txreg/payload-valid? read-op)))
        (is (= :ok (:type write-op)))
        (is (= (clojure.lang.MapEntry. 0 7) (:value write-op)))
        (is (true? (:txreg/payload-valid? write-op)))
        (is (= :ok (:type cas-op)))
        (is (= (clojure.lang.MapEntry. 0 [7 9]) (:value cas-op)))
        (is (true? (:txreg/payload-valid? cas-op)))
        (is (= :ok (:type final-op)))
        (is (= (clojure.lang.MapEntry. 0 9) (:value final-op)))
        (is (true? (:txreg/payload-valid? final-op))))
      (finally
        (doseq [node (:nodes test-map)]
          (jdb/teardown! db test-map node))))))

(deftest rejoin-bootstrap-client-converges-follower-smoke-test
  (let [cluster-id (str (UUID/randomUUID))
        workload   (rejoin-bootstrap/workload {:key-count 4
                                               :nodes ["n1" "n2" "n3"]})
        test-map   {:db-name "rejoin-smoke"
                    :control-backend :sofa-jraft
                    :nodes ["n1" "n2" "n3"]
                    :key-count 4
                    :verbose false
                    :datalevin/cluster-id cluster-id
                    :datalevin/nemesis-faults []
                    :datalevin/cluster-opts (:datalevin/cluster-opts workload)}
        db         (local/db cluster-id)
        client     (:client workload)]
    (try
      (doseq [node (:nodes test-map)]
        (jdb/setup! db test-map node))
      (let [opened      (client/open! client test-map "n1")
            _           (client/setup! opened test-map)
            _           (client/invoke! opened
                                        test-map
                                        {:type :invoke
                                         :f :write
                                         :value (clojure.lang.MapEntry. 0 1)})
            _           (client/invoke! opened
                                        test-map
                                        {:type :invoke
                                         :f :write
                                         :value (clojure.lang.MapEntry. 1 2)})
            leader      (:leader (local/wait-for-single-leader! cluster-id))
            stopped-node (->> (get-in (local/cluster-state cluster-id)
                                      [:live-nodes])
                              sort
                              (remove #{leader})
                              first)
            _           (is (string? stopped-node))
            _           (local/stop-node! cluster-id stopped-node)
            _           (client/invoke! opened
                                        test-map
                                        {:type :invoke
                                         :f :write
                                         :value (clojure.lang.MapEntry. 0 3)})
            _           (client/invoke! opened
                                        test-map
                                        {:type :invoke
                                         :f :cas
                                         :value (clojure.lang.MapEntry. 1 [2 4])})
            converge-op (client/invoke! opened
                                        test-map
                                        {:type :invoke
                                         :f :converge})]
        (is (= :ok (:type converge-op))
            (pr-str converge-op))
        (is (true? (get-in converge-op [:value :caught-up?])))
        (is (= [stopped-node] (get-in converge-op [:value :restarted-nodes])))
        (is (= stopped-node
               (get-in converge-op [:value :wal-gap :target-node])))
        (is (integer? (get-in converge-op
                              [:value
                               :bootstrap-state
                               :ha-follower-last-bootstrap-ms])))
        (is (pos? (long (or (get-in converge-op
                                    [:value
                                     :bootstrap-state
                                     :ha-follower-bootstrap-snapshot-last-applied-lsn])
                           0))))
        (is (every? (fn [[_ gc-result]]
                      (> (long (or (get-in gc-result [:after :min-retained-lsn])
                                   0))
                         (long (or (get-in converge-op
                                           [:value :wal-gap :follower-next-lsn])
                                   0))))
                    (get-in converge-op [:value :wal-gap :gc-results])))
        (is (= [3 4 0 0] (get-in converge-op [:value :expected])))
        (is (= {"n1" [3 4 0 0]
                "n2" [3 4 0 0]
                "n3" [3 4 0 0]}
               (into {}
                     (map (fn [[logical-node {:keys [values]}]]
                            [logical-node values]))
                     (get-in converge-op [:value :nodes])))))
      (finally
        (doseq [node (:nodes test-map)]
          (jdb/teardown! db test-map node))))))

(deftest rejoin-bootstrap-checker-ignores-invoke-and-requires-lsn-catch-up-test
  (let [checker (:checker (rejoin-bootstrap/workload {:key-count 2}))
        good-snapshot {:caught-up? true
                       :lsn-caught-up? true
                       :expected [1 2]
                       :nodes {"n1" {:ready? true
                                     :values [1 2]
                                     :node-diagnostics {}}
                               "n2" {:ready? true
                                     :values [1 2]
                                     :node-diagnostics {}}}}
        lagging-snapshot (assoc good-snapshot
                                :lsn-caught-up? false
                                :lsn-snapshot {"n1" 12 "n2" 11}
                                :lsn-error {:message "lagging"})
        ok-result (checker/check checker
                                 nil
                                 [{:type :invoke :f :converge}
                                  {:type :ok :f :converge :value good-snapshot}]
                                 nil)
        lagging-result (checker/check checker
                                      nil
                                      [{:type :ok :f :converge
                                        :value lagging-snapshot}]
                                      nil)]
    (is (true? (:valid? ok-result)))
    (is (= 1 (:converge-count ok-result)))
    (is (zero? (:failure-count ok-result)))
    (is (false? (:valid? lagging-result)))
    (is (= 1 (:lsn-not-caught-up-count lagging-result)))))

(deftest fencing-client-smoke-test
  (let [cluster-id (str (UUID/randomUUID))
        test-map   {:db-name "fencing-smoke"
                    :control-backend :sofa-jraft
                    :nodes ["n1" "n2" "n3"]
                    :verbose false
                    :datalevin/cluster-id cluster-id
                    :datalevin/nemesis-faults []}
        db         (local/db cluster-id)
        client     (fencing/->Client nil)]
    (try
      (doseq [node (:nodes test-map)]
        (jdb/setup! db test-map node))
      (let [opened   (client/open! client test-map "n1")
            _        (client/setup! opened test-map)
            probe-op (client/invoke! opened
                                     test-map
                                     {:type :invoke
                                      :f :probe})]
        (is (= :ok (:type probe-op)))
        (is (map? (:value probe-op)))
        (is (= 3 (count (get-in probe-op [:value :nodes]))))
        (is (<= (count (filter (fn [[_ {:keys [status]}]]
                                 (= :admitted status))
                               (get-in probe-op [:value :nodes])))
                1)))
      (finally
        (doseq [node (:nodes test-map)]
          (jdb/teardown! db test-map node))))))

(deftest fencing-probe-timeout-smoke-test
  (with-redefs-fn {#'fencing/node-probe-timeout-ms 5
                   #'fencing/probe-node*         (fn [_cluster-id _db-name _logical-node]
                                                    (Thread/sleep 100)
                                                    {:status :admitted})}
    (fn []
      (let [result (#'fencing/probe-node! "smoke-cluster"
                                          "smoke-db"
                                          "n1")]
        (is (= {:status :unreachable
                :message "Timeout in making request"
                :timeout-ms 5}
               result))))))

(deftest leader-partition-nemesis-smoke-test
  (let [cluster-id  (str (UUID/randomUUID))
        test-map    {:db-name "partition-smoke"
                     :schema append/schema
                     :control-backend :sofa-jraft
                     :nodes ["n1" "n2" "n3"]
                     :net (local/net cluster-id)
                     :verbose false
                     :datalevin/cluster-id cluster-id
                     :datalevin/nemesis-faults [:leader-partition]}
        db          (local/db cluster-id)
        nemesis-obj (#'nemesis/leader-failover-nemesis)
        part-op     {:type :info
                     :process :nemesis
                     :f :partition-leader}
        heal-op     {:type :info
                     :process :nemesis
                     :f :heal-partition}]
    (try
      (doseq [node (:nodes test-map)]
        (jdb/setup! db test-map node))
      (let [{leader-before :leader}
            (local/wait-for-authority-leader! cluster-id)
            partitioned-op
            (jn/invoke! nemesis-obj test-map part-op)
            leader-after
            (get-in partitioned-op [:value :leader])
            healed-op
            (do
              (is (seq (local/network-grudge cluster-id)))
              (jn/invoke! nemesis-obj test-map heal-op))]
        (is (= leader-before
               (get-in partitioned-op [:value :partitioned])))
        (is (seq (get-in partitioned-op [:value :grudge])))
        (is (not= :leader-unchanged
                  (get-in partitioned-op [:value :status]))
            (pr-str partitioned-op))
        (is (not= leader-before leader-after)
            (pr-str partitioned-op))
        (is (= leader-before
               (get-in healed-op [:value :healed])))
        (is (empty? (local/network-grudge cluster-id)))
        (local/with-leader-conn
          test-map
          append/schema
          (fn [conn]
            (d/transact! conn [{:append/key 0
                                :append/value 1}])))
        (is (= :committed
               (wait-for-leader-append-write! test-map 0 2 20000))))
      (finally
        (doseq [node (:nodes test-map)]
          (jdb/teardown! db test-map node))))))

(deftest asymmetric-partition-nemesis-smoke-test
  (let [cluster-id  (str (UUID/randomUUID))
        test-map    {:db-name "asymmetric-smoke"
                     :schema append/schema
                     :control-backend :sofa-jraft
                     :nodes ["n1" "n2" "n3"]
                     :net (local/net cluster-id)
                     :verbose false
                     :datalevin/cluster-id cluster-id
                     :datalevin/nemesis-faults [:asymmetric-partition]}
        db          (local/db cluster-id)
        nemesis-obj (#'nemesis/leader-failover-nemesis)
        part-op     {:type :info
                     :process :nemesis
                     :f :partition-asymmetric}
        heal-op     {:type :info
                     :process :nemesis
                     :f :heal-asymmetric}]
    (try
      (doseq [node (:nodes test-map)]
        (jdb/setup! db test-map node))
      (let [partitioned-op
            (jn/invoke! nemesis-obj test-map part-op)
            healed-op
            (do
              (is (seq (local/network-grudge cluster-id)))
              (jn/invoke! nemesis-obj test-map heal-op))]
        (is (seq (get-in partitioned-op [:value :grudge])))
        (is (seq (get-in partitioned-op [:value :groups])))
        (is (every? seq (get-in partitioned-op [:value :groups])))
        (is (= #{"n1" "n2" "n3"}
               (set (mapcat identity
                            (get-in partitioned-op [:value :groups])))))
        (is (seq (get-in partitioned-op [:value :pair-cuts])))
        (is (seq (get-in partitioned-op [:value :dropped-links])))
        (is (empty? (local/network-grudge cluster-id)))
        (local/with-leader-conn
          test-map
          append/schema
          (fn [conn]
            (d/transact! conn [{:append/key 0
                                :append/value 1}])))
        (let [leader-final (:leader (local/wait-for-single-leader! cluster-id))
              target-lsn (local/effective-local-lsn cluster-id leader-final)
              lsn-snapshot
              (local/wait-for-live-nodes-at-least-lsn! cluster-id
                                                       target-lsn
                                                       20000)]
          (is (= #{"n1" "n2" "n3"}
                 (set (keys lsn-snapshot))))
          (is (every? #(>= (long %)
                           (long target-lsn))
                      (vals lsn-snapshot))))
        (is (= (get-in partitioned-op [:value :grudge])
               (get-in healed-op [:value :grudge]))))
      (finally
        (doseq [node (:nodes test-map)]
          (jdb/teardown! db test-map node))))))

(deftest degraded-network-nemesis-smoke-test
  (let [cluster-id  (str (UUID/randomUUID))
        test-map    {:db-name "degraded-smoke"
                     :schema append/schema
                     :control-backend :sofa-jraft
                     :nodes ["n1" "n2" "n3"]
                     :net (local/net cluster-id)
                     :verbose false
                     :datalevin/cluster-id cluster-id
                     :datalevin/nemesis-faults [:degraded-network]}
        db          (local/db cluster-id)
        nemesis-obj (#'nemesis/leader-failover-nemesis)
        degrade-op  {:type :info
                     :process :nemesis
                     :f :degrade-network}
        restore-op  {:type :info
                     :process :nemesis
                     :f :restore-network}]
    (try
      (doseq [node (:nodes test-map)]
        (jdb/setup! db test-map node))
      (let [degraded-op
            (jn/invoke! nemesis-obj test-map degrade-op)
            behavior (local/network-behavior cluster-id)
            link-behaviors (local/network-link-behaviors cluster-id)
            restored-op
            (do
              (is (seq link-behaviors))
              (jn/invoke! nemesis-obj test-map restore-op))]
        (is (= behavior
               (get-in degraded-op [:value :behavior])))
        (is (= (:link-profiles behavior)
               link-behaviors))
        (is (> (get-in behavior [:profile-summary :distinct-profile-count]) 1))
        (is (<= (get-in behavior [:profile-summary :delay-ms :min])
                (get-in behavior [:profile-summary :delay-ms :max])))
        (is (pos? (get-in behavior [:profile-summary :drop-probability :max])))
        (is (empty? (local/network-link-behaviors cluster-id)))
        (is (nil? (local/network-behavior cluster-id)))
        (local/with-leader-conn
          test-map
          append/schema
          (fn [conn]
            (d/transact! conn [{:append/key 0
                                :append/value 1}])))
        (let [leader-final (:leader (local/wait-for-single-leader! cluster-id))
              target-lsn (local/effective-local-lsn cluster-id leader-final)
              lsn-snapshot
              (local/wait-for-live-nodes-at-least-lsn! cluster-id
                                                       target-lsn
                                                       20000)]
          (is (= #{"n1" "n2" "n3"}
                 (set (keys lsn-snapshot))))
          (is (every? #(>= (long %)
                           (long target-lsn))
                      (vals lsn-snapshot))))
        (is (= (get-in degraded-op [:value :behavior])
               (get-in restored-op [:value :behavior]))))
      (finally
        (doseq [node (:nodes test-map)]
          (jdb/teardown! db test-map node))))))

(deftest leader-io-stall-nemesis-smoke-test
  (let [cluster-id  (str (UUID/randomUUID))
        test-map    {:db-name "io-stall-smoke"
                     :schema append/schema
                     :control-backend :sofa-jraft
                     :nodes ["n1" "n2" "n3"]
                     :verbose false
                     :datalevin/cluster-id cluster-id
                     :datalevin/nemesis-faults [:leader-io-stall]}
        db          (local/db cluster-id)
        nemesis-obj (#'nemesis/leader-failover-nemesis)
        wedge-op    {:type :info
                     :process :nemesis
                     :f :wedge-leader-storage
                     :value {:mode :stall}}
        heal-op     {:type :info
                     :process :nemesis
                     :f :heal-storage}]
    (try
      (doseq [node (:nodes test-map)]
        (jdb/setup! db test-map node))
      (let [wedge-res (jn/invoke! nemesis-obj test-map wedge-op)
            leader    (get-in wedge-res [:value :wedged])
            tx-fut    (future
                        (local/with-leader-conn
                          test-map
                          append/schema
                          (fn [conn]
                            (d/transact! conn [{:append/key 0
                                                :append/value 1}])
                            :committed)))]
        (is (= :stall (get-in wedge-res [:value :fault :mode])))
        (is (= :stall
               (get-in (local/storage-fault cluster-id leader)
                       [:mode])))
        (Thread/sleep 200)
        (is (not (realized? tx-fut)))
        (let [heal-res (jn/invoke! nemesis-obj test-map heal-op)]
          (is (= :stall (get-in heal-res [:value :fault :mode]))))
        (is (= :committed (deref tx-fut 5000 ::timeout)))
        (is (nil? (local/storage-fault cluster-id leader)))
        (let [leader-final (:leader (local/wait-for-single-leader! cluster-id))
              target-lsn (local/effective-local-lsn cluster-id leader-final)
              lsn-snapshot
              (local/wait-for-live-nodes-at-least-lsn! cluster-id
                                                       target-lsn
                                                       20000)]
          (is (= #{"n1" "n2" "n3"}
                 (set (keys lsn-snapshot))))
          (is (every? #(>= (long %)
                           (long target-lsn))
                      (vals lsn-snapshot)))))
      (finally
        (doseq [node (:nodes test-map)]
          (jdb/teardown! db test-map node))))))

(deftest leader-disk-full-nemesis-smoke-test
  (let [cluster-id  (str (UUID/randomUUID))
        test-map    {:db-name "disk-full-smoke"
                     :schema append/schema
                     :control-backend :sofa-jraft
                     :nodes ["n1" "n2" "n3"]
                     :verbose false
                     :datalevin/cluster-id cluster-id
                     :datalevin/nemesis-faults [:leader-disk-full]}
        db          (local/db cluster-id)
        nemesis-obj (#'nemesis/leader-failover-nemesis)
        wedge-op    {:type :info
                     :process :nemesis
                     :f :wedge-leader-storage
                     :value {:mode :disk-full}}
        heal-op     {:type :info
                     :process :nemesis
                     :f :heal-storage}]
    (try
      (doseq [node (:nodes test-map)]
        (jdb/setup! db test-map node))
      (let [wedge-res (jn/invoke! nemesis-obj test-map wedge-op)
            leader    (get-in wedge-res [:value :wedged])
            tx-result (try
                        (local/with-leader-conn
                          test-map
                          append/schema
                          (fn [conn]
                            (d/transact! conn [{:append/key 0
                                                :append/value 1}])
                            :committed))
                        (catch Throwable e
                          e))]
        (is (= :disk-full (get-in wedge-res [:value :fault :mode])))
        (is (= :disk-full
               (get-in (local/storage-fault cluster-id leader)
                       [:mode])))
        (if (instance? Throwable tx-result)
          (is (true? (local/expected-disruption-write-failure?
                      test-map
                      tx-result)))
          (do
            (is (= :committed tx-result))
            (let [replacement (local/wait-for-single-leader! cluster-id
                                                             10000)]
              (is (not= leader
                        (:leader replacement))))))
        (let [heal-res (jn/invoke! nemesis-obj test-map heal-op)]
          (is (= :disk-full (get-in heal-res [:value :fault :mode]))))
        (local/with-leader-conn
          test-map
          append/schema
          (fn [conn]
            (d/transact! conn [{:append/key 0
                                :append/value 2}])))
        (is (nil? (local/storage-fault cluster-id leader)))
        (let [leader-final (:leader (local/wait-for-single-leader! cluster-id))
              target-lsn (local/effective-local-lsn cluster-id leader-final)
              lsn-snapshot
              (local/wait-for-live-nodes-at-least-lsn! cluster-id
                                                       target-lsn
                                                       20000)]
          (is (= #{"n1" "n2" "n3"}
                 (set (keys lsn-snapshot))))
          (is (every? #(>= (long %)
                           (long target-lsn))
                      (vals lsn-snapshot)))))
      (finally
        (doseq [node (:nodes test-map)]
          (jdb/teardown! db test-map node))))))

(deftest leader-pause-nemesis-smoke-test
  (let [cluster-id  (str (UUID/randomUUID))
        test-map    {:db-name "pause-smoke"
                     :schema append/schema
                     :control-backend :sofa-jraft
                     :nodes ["n1" "n2" "n3"]
                     :verbose false
                     :datalevin/cluster-id cluster-id
                     :datalevin/nemesis-faults [:leader-pause]}
        db          (local/db cluster-id)
        nemesis-obj (#'nemesis/leader-failover-nemesis)
        pause-op    {:type :info
                     :process :nemesis
                     :f :pause-leader}
        resume-op   {:type :info
                     :process :nemesis
                     :f :resume-node}]
    (try
      (doseq [node (:nodes test-map)]
        (jdb/setup! db test-map node))
      (let [{leader-before :leader}
            (local/wait-for-single-leader! cluster-id)
            paused-op
            (jn/invoke! nemesis-obj test-map pause-op)
            leader-after
            (get-in paused-op [:value :leader])
            resumed-op
            (do
              (is (contains? (get-in (local/cluster-state cluster-id)
                                     [:paused-nodes])
                             leader-before))
              (is (some? (local/paused-node-info cluster-id leader-before)))
              (jn/invoke! nemesis-obj test-map resume-op))]
        (is (= leader-before
               (get-in paused-op [:value :paused])))
        (is (contains? #{nil :leader-unavailable}
                       (get-in paused-op [:value :status]))
            (pr-str paused-op))
        (when leader-after
          (is (not= leader-before leader-after)
              (pr-str paused-op)))
        (is (= leader-before
               (get-in resumed-op [:value :resumed])))
        (is (empty? (get-in (local/cluster-state cluster-id) [:paused-nodes])))
        (is (= :committed
               (wait-for-leader-append-write! test-map 0 1 20000))))
      (finally
        (doseq [node (:nodes test-map)]
          (jdb/teardown! db test-map node))))))

(deftest node-pause-nemesis-smoke-test
  (let [cluster-id  (str (UUID/randomUUID))
        test-map    {:db-name "node-pause-smoke"
                     :schema append/schema
                     :control-backend :sofa-jraft
                     :nodes ["n1" "n2" "n3"]
                     :verbose false
                     :datalevin/cluster-id cluster-id
                     :datalevin/nemesis-faults [:node-pause]}
        db          (local/db cluster-id)
        nemesis-obj (#'nemesis/leader-failover-nemesis)
        resume-op   {:type :info
                     :process :nemesis
                     :f :resume-node}]
    (try
      (doseq [node (:nodes test-map)]
        (jdb/setup! db test-map node))
      (let [{leader-before :leader}
            (local/wait-for-single-leader! cluster-id)
            follower-before
            (->> (:nodes test-map)
                 sort
                 (remove #{leader-before})
                 first)
            pause-op   {:type :info
                        :process :nemesis
                        :f :pause-node
                        :value {:node follower-before}}
            paused-op  (jn/invoke! nemesis-obj test-map pause-op)]
        (is (= follower-before
               (get-in paused-op [:value :paused])))
        (is (= leader-before
               (get-in paused-op [:value :leader]))
            (pr-str paused-op))
        (is (contains? (get-in (local/cluster-state cluster-id) [:paused-nodes])
                       follower-before))
        (is (some? (local/paused-node-info cluster-id follower-before)))
        (let [resumed-op (jn/invoke! nemesis-obj test-map resume-op)]
          (is (= follower-before
                 (get-in resumed-op [:value :resumed])))
          (is (empty? (get-in (local/cluster-state cluster-id)
                              [:paused-nodes]))))
        (is (= :committed
               (wait-for-leader-append-write! test-map 0 1 20000))))
      (finally
        (doseq [node (:nodes test-map)]
          (jdb/teardown! db test-map node))))))

(deftest multi-node-pause-nemesis-smoke-test
  (let [cluster-id  (str (UUID/randomUUID))
        test-map    {:db-name "multi-node-pause-smoke"
                     :schema append/schema
                     :control-backend :sofa-jraft
                     :nodes ["n1" "n2" "n3"]
                     :verbose false
                     :datalevin/cluster-id cluster-id
                     :datalevin/nemesis-faults [:multi-node-pause]}
        db          (local/db cluster-id)
        nemesis-obj (#'nemesis/leader-failover-nemesis)
        resume-op   {:type :info
                     :process :nemesis
                     :f :resume-nodes}]
    (try
      (doseq [node (:nodes test-map)]
        (jdb/setup! db test-map node))
      (let [{leader-before :leader}
            (local/wait-for-single-leader! cluster-id)
            paused-targets
            (vec (cons leader-before
                       (take 1
                             (remove #{leader-before}
                                     (sort (:nodes test-map))))))
            pause-op   {:type :info
                        :process :nemesis
                        :f :pause-nodes
                        :value {:nodes paused-targets}}
            paused-op  (jn/invoke! nemesis-obj test-map pause-op)]
        (is (= (set paused-targets)
               (set (get-in paused-op [:value :paused-nodes]))))
        (is (= :leader-unavailable
               (get-in paused-op [:value :status]))
            (pr-str paused-op))
        (is (nil? (get-in paused-op [:value :leader])))
        (is (= (set paused-targets)
               (get-in (local/cluster-state cluster-id) [:paused-nodes])))
        (is (every? #(some? (local/paused-node-info cluster-id %))
                    paused-targets))
        (let [resumed-op (jn/invoke! nemesis-obj test-map resume-op)]
          (is (= (set paused-targets)
                 (set (get-in resumed-op [:value :resumed-nodes]))))
          (is (empty? (get-in (local/cluster-state cluster-id)
                              [:paused-nodes]))))
        (let [{leader-after :leader}
              (local/wait-for-single-leader! cluster-id 60000)]
          (is (contains? (set (:nodes test-map))
                         leader-after))))
      (finally
        (doseq [node (:nodes test-map)]
          (jdb/teardown! db test-map node))))))

(deftest nemesis-spec-smoke-test
  (is (= [:leader-failover]
         (core/parse-nemesis-spec "failover")))
  (is (= [:leader-pause]
         (core/parse-nemesis-spec "pause")))
  (is (= [:leader-pause]
         (core/parse-nemesis-spec "leader-pause")))
  (is (= [:node-pause]
         (core/parse-nemesis-spec "pause-any")))
  (is (= [:node-pause]
         (core/parse-nemesis-spec "node-pause")))
  (is (= [:multi-node-pause]
         (core/parse-nemesis-spec "pause-multi")))
  (is (= [:multi-node-pause]
         (core/parse-nemesis-spec "multi-node-pause")))
  (is (= [:leader-partition]
         (core/parse-nemesis-spec "partition")))
  (is (= [:leader-partition]
         (core/parse-nemesis-spec "leader-partition")))
  (is (= [:asymmetric-partition]
         (core/parse-nemesis-spec "asymmetric")))
  (is (= [:asymmetric-partition]
         (core/parse-nemesis-spec "asymmetric-partition")))
  (is (= [:degraded-network]
         (core/parse-nemesis-spec "degraded")))
  (is (= [:degraded-network]
         (core/parse-nemesis-spec "degraded-network")))
  (is (= [:leader-io-stall]
         (core/parse-nemesis-spec "io-stall")))
  (is (= [:leader-io-stall]
         (core/parse-nemesis-spec "leader-io-stall")))
  (is (= [:leader-disk-full]
         (core/parse-nemesis-spec "disk-full")))
  (is (= [:leader-disk-full]
         (core/parse-nemesis-spec "leader-disk-full")))
  (is (= [:follower-rejoin]
         (core/parse-nemesis-spec "rejoin")))
  (is (= [:follower-rejoin]
         (core/parse-nemesis-spec "follower-rejoin")))
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
        (nemesis/nemesis-package {:faults [:leader-pause]})]
    (is (some? nemesis))
    (is (some? generator))
    (is (some? final-generator)))
  (let [{:keys [nemesis generator final-generator]}
        (nemesis/nemesis-package {:faults [:node-pause]})]
    (is (some? nemesis))
    (is (some? generator))
    (is (some? final-generator)))
  (let [{:keys [nemesis generator final-generator]}
        (nemesis/nemesis-package {:faults [:multi-node-pause]})]
    (is (some? nemesis))
    (is (some? generator))
    (is (some? final-generator)))
  (let [{:keys [nemesis generator final-generator]}
        (nemesis/nemesis-package {:faults [:leader-partition]})]
    (is (some? nemesis))
    (is (some? generator))
    (is (some? final-generator)))
  (let [{:keys [nemesis generator final-generator]}
        (nemesis/nemesis-package {:faults [:asymmetric-partition]})]
    (is (some? nemesis))
    (is (some? generator))
    (is (some? final-generator)))
  (let [{:keys [nemesis generator final-generator]}
        (nemesis/nemesis-package {:faults [:degraded-network]})]
    (is (some? nemesis))
    (is (some? generator))
    (is (some? final-generator)))
  (let [{:keys [nemesis generator final-generator]}
        (nemesis/nemesis-package {:faults [:leader-io-stall]})]
    (is (some? nemesis))
    (is (some? generator))
    (is (some? final-generator)))
  (let [{:keys [nemesis generator final-generator]}
        (nemesis/nemesis-package {:faults [:leader-disk-full]})]
    (is (some? nemesis))
    (is (some? generator))
    (is (some? final-generator)))
  (let [{:keys [nemesis generator final-generator]}
        (nemesis/nemesis-package {:faults [:follower-rejoin]})]
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

(deftest datalevin-test-with-pause-smoke-test
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
                                       :nemesis [:leader-pause]})]
    (is (= [:leader-pause] (:datalevin/nemesis-faults test-map)))
    (is (some? (:nemesis test-map)))
    (is (some? (:generator test-map)))))

(deftest datalevin-test-with-node-pause-smoke-test
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
                                       :nemesis [:node-pause]})]
    (is (= [:node-pause] (:datalevin/nemesis-faults test-map)))
    (is (some? (:nemesis test-map)))
    (is (some? (:generator test-map)))))

(deftest datalevin-test-with-multi-node-pause-smoke-test
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
                                       :nemesis [:multi-node-pause]})]
    (is (= [:multi-node-pause] (:datalevin/nemesis-faults test-map)))
    (is (some? (:nemesis test-map)))
    (is (some? (:generator test-map)))))

(deftest datalevin-test-with-partition-smoke-test
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
                                       :nemesis [:leader-partition]})]
    (is (= [:leader-partition] (:datalevin/nemesis-faults test-map)))
    (is (some? (:net test-map)))
    (is (some? (:nemesis test-map)))
    (is (some? (:generator test-map)))))

(deftest datalevin-test-with-asymmetric-partition-smoke-test
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
                                       :nemesis [:asymmetric-partition]})]
    (is (= [:asymmetric-partition] (:datalevin/nemesis-faults test-map)))
    (is (some? (:net test-map)))
    (is (some? (:nemesis test-map)))
    (is (some? (:generator test-map)))))

(deftest datalevin-test-with-degraded-network-smoke-test
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
                                       :nemesis [:degraded-network]})]
    (is (= [:degraded-network] (:datalevin/nemesis-faults test-map)))
    (is (some? (:net test-map)))
    (is (some? (:nemesis test-map)))
    (is (some? (:generator test-map)))))

(deftest datalevin-test-with-io-stall-smoke-test
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
                                       :nemesis [:leader-io-stall]})]
    (is (= [:leader-io-stall] (:datalevin/nemesis-faults test-map)))
    (is (some? (:nemesis test-map)))
    (is (some? (:generator test-map)))))

(deftest datalevin-test-with-disk-full-smoke-test
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
                                       :nemesis [:leader-disk-full]})]
    (is (= [:leader-disk-full] (:datalevin/nemesis-faults test-map)))
    (is (some? (:nemesis test-map)))
    (is (some? (:generator test-map)))))

(deftest datalevin-test-with-follower-rejoin-smoke-test
  (let [test-map (core/datalevin-test {:db-name "smoke"
                                       :control-backend :sofa-jraft
                                       :workload :rejoin-bootstrap
                                       :rate 10
                                       :time-limit 5
                                       :key-count 4
                                       :nodes ["n1" "n2" "n3"]
                                       :nemesis [:follower-rejoin]})]
    (is (= [:follower-rejoin] (:datalevin/nemesis-faults test-map)))
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

(deftest nemesis-partition-net-uses-jepsen-net-test
  (let [dropped      (atom nil)
        healed?      (atom false)
        expected-net (sorted-map "n1" ["n2" "n3"]
                                 "n2" ["n1"]
                                 "n3" ["n1"])
        net          (reify
                       net.proto/Net
                       (drop! [_ _test _src _dest]
                         (throw (UnsupportedOperationException.
                                 "drop! not used in this test")))
                       (heal! [_ _test]
                         (reset! healed? true)
                         true)
                       (slow! [_ _test]
                         (throw (UnsupportedOperationException.
                                 "slow! not used in this test")))
                       (slow! [_ _test _opts]
                         (throw (UnsupportedOperationException.
                                 "slow! not used in this test")))
                       (flaky! [_ _test]
                         (throw (UnsupportedOperationException.
                                 "flaky! not used in this test")))
                       (fast! [_ _test]
                         (reset! healed? true)
                         true)
                       (shape! [_ _test _nodes _behavior]
                         (throw (UnsupportedOperationException.
                                 "shape! not used in this test")))
                       net.proto/PartitionAll
                       (drop-all! [_ _test grudge]
                         (reset! dropped grudge)
                         {:grudge grudge}))]
    (#'datalevin.jepsen.nemesis/partition-net!
     {:datalevin/cluster-id "smoke"
      :net net}
     "smoke"
     expected-net)
    (#'datalevin.jepsen.nemesis/heal-net!
     {:datalevin/cluster-id "smoke"
      :net net}
     "smoke")
    (is (= expected-net @dropped))
    (is (true? @healed?))))

(deftest nemesis-degraded-net-uses-jepsen-net-test
  (let [shaped       (atom nil)
        restored?    (atom false)
        expected-nodes ["n1" "n2" "n3"]
        expected-behavior {:delay-ms 10
                           :jitter-ms 20
                           :drop-probability 0.1}
        net          (reify
                       net.proto/Net
                       (drop! [_ _test _src _dest]
                         (throw (UnsupportedOperationException.
                                 "drop! not used in this test")))
                       (heal! [_ _test]
                         (reset! restored? true)
                         true)
                       (slow! [_ _test]
                         (throw (UnsupportedOperationException.
                                 "slow! not used in this test")))
                       (slow! [_ _test _opts]
                         (throw (UnsupportedOperationException.
                                 "slow! not used in this test")))
                       (flaky! [_ _test]
                         (throw (UnsupportedOperationException.
                                 "flaky! not used in this test")))
                       (fast! [_ _test]
                         (reset! restored? true)
                         true)
                       (shape! [_ _test nodes behavior]
                         (reset! shaped {:nodes nodes
                                         :behavior behavior})
                         {:nodes nodes
                          :behavior behavior})
                       net.proto/PartitionAll
                       (drop-all! [_ _test _grudge]
                         (throw (UnsupportedOperationException.
                                 "drop-all! not used in this test"))))]
    (#'datalevin.jepsen.nemesis/shape-net!
     {:datalevin/cluster-id "smoke"
      :net net}
     "smoke"
     expected-nodes
     expected-behavior)
    (#'datalevin.jepsen.nemesis/fast-net!
     {:datalevin/cluster-id "smoke"
      :net net}
     "smoke")
    (is (= {:nodes expected-nodes
            :behavior expected-behavior}
           @shaped))
    (is (true? @restored?))))

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

(deftest nemesis-pause-leader-preserves-info-when-no-replacement-yet-test
  (let [nemesis-obj (#'nemesis/leader-failover-nemesis)
        op          {:type :info :process :nemesis :f :pause-leader}]
    (with-redefs [local/wait-for-single-leader! (fn
                                                  ([_] {:leader "n1"})
                                                  ([_ _timeout-ms]
                                                   {:leader "n1"}))
                  local/pause-node!            (fn [_cluster-id _logical-node]
                                                 true)
                  local/maybe-wait-for-single-leader
                  (fn
                    ([_cluster-id] nil)
                    ([_cluster-id _timeout-ms] nil))]
      (is (= {:type :info
              :process :nemesis
              :f :pause-leader
              :value {:paused "n1"
                      :leader nil
                      :status :leader-unavailable}}
             (jn/invoke! nemesis-obj
                         {:datalevin/cluster-id "smoke"}
                         op))))))

(deftest leader-conn-bootstrap-timeout-smoke-test
  (let [ex (try
             (with-redefs [d/create-conn (fn [_uri _schema _opts]
                                           (Thread/sleep 100)
                                           :late-conn)]
               (#'local/create-conn-with-timeout!
                "dtlv://datalevin:datalevin@127.0.0.1/smoke"
                {}
                5))
             nil
             (catch Exception e
               e))]
    (is (instance? clojure.lang.ExceptionInfo ex))
    (is (= "Timeout in making request" (ex-message ex)))
    (is (= :open-conn (:phase (ex-data ex))))))

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
                               :nemesis [:node-pause]})))
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
                               :nemesis [:multi-node-pause]})))
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
                               :nemesis [:leader-pause]})))
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
                               :nemesis [:leader-partition]})))
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
                               :nemesis [:follower-rejoin]})))
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

(deftest execute-giant-values-smoke-test
  (let [dir  (str (System/getProperty "java.io.tmpdir")
                  "/datalevin-jepsen-giant-values-"
                  (UUID/randomUUID))
        conn (d/create-conn dir giant-values/schema)]
    (try
      (#'giant-values/ensure-giants! conn 2 12000)
      (let [read-before (#'giant-values/execute-op! conn
                                                    12000
                                                    {:f :read
                                                     :value (clojure.lang.MapEntry. 0 nil)})
            write-op    (#'giant-values/execute-op! conn
                                                    12000
                                                    {:f :write
                                                     :value (clojure.lang.MapEntry. 0 5)})
            cas-op      (#'giant-values/execute-op! conn
                                                    12000
                                                    {:f :cas
                                                     :value (clojure.lang.MapEntry. 0 [5 8])})]
        (is (= (clojure.lang.MapEntry. 0 0) (:tuple read-before)))
        (is (true? (:payload-valid? read-before)))
        (is (= 12000 (:payload-bytes read-before)))
        (is (= (clojure.lang.MapEntry. 0 5) (:tuple write-op)))
        (is (true? (:payload-valid? write-op)))
        (is (= (clojure.lang.MapEntry. 0 [5 8]) (:tuple cas-op)))
        (is (true? (:payload-valid? cas-op))))
      (finally
        (d/close conn)
        (u/delete-files dir)))))

(deftest execute-tx-fn-register-smoke-test
  (let [dir  (str (System/getProperty "java.io.tmpdir")
                  "/datalevin-jepsen-tx-fn-register-"
                  (UUID/randomUUID))
        conn (d/create-conn dir tx-fn-register/schema)]
    (try
      (#'tx-fn-register/ensure-tx-fns! conn)
      (#'tx-fn-register/ensure-registers! conn 2 12000)
      (let [read-before (#'tx-fn-register/execute-op! conn
                                                      12000
                                                      {:f :read
                                                       :value (clojure.lang.MapEntry. 0 nil)})
            write-op    (#'tx-fn-register/execute-op! conn
                                                      12000
                                                      {:f :write
                                                       :value (clojure.lang.MapEntry. 0 5)})
            cas-op      (#'tx-fn-register/execute-op! conn
                                                      12000
                                                      {:f :cas
                                                       :value (clojure.lang.MapEntry. 0 [5 8])})
            bad-cas     (#'tx-fn-register/execute-op! conn
                                                      12000
                                                      {:f :cas
                                                       :value (clojure.lang.MapEntry. 0 [5 9])})]
        (is (= (clojure.lang.MapEntry. 0 0) (:tuple read-before)))
        (is (true? (:payload-valid? read-before)))
        (is (= (clojure.lang.MapEntry. 0 5) (:tuple write-op)))
        (is (true? (:payload-valid? write-op)))
        (is (= (clojure.lang.MapEntry. 0 [5 8]) (:tuple cas-op)))
        (is (true? (:payload-valid? cas-op)))
        (is (= ::tx-fn-register/cas-failed bad-cas)))
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
