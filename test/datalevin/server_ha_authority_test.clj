(ns datalevin.server-ha-authority-test
  (:require
   [clojure.java.io :as io]
   [clojure.string :as s]
   [clojure.test :refer [deftest is testing use-fixtures]]
   [datalevin.client :as cl]
   [datalevin.constants :as c]
   [datalevin.core :as d]
   [datalevin.datom :as dd]
   [datalevin.db :as db]
   [datalevin.ha :as dha]
   [datalevin.ha.control :as ha]
   [datalevin.interpret :as interp]
   [datalevin.interface :as i]
   [datalevin.kv :as kv]
   [datalevin.server :as srv]
   [datalevin.server-ha-test-support :refer :all]
   [datalevin.storage :as st]
   [datalevin.udf :as udf]
   [datalevin.util :as u]
   [datalevin.validate :as vld]
   [taoensso.timbre :as log])
  (:import
   [datalevin.db DB]
   [datalevin.storage Store]
   [datalevin.server Server]
   [java.net ConnectException]
   [java.nio.channels ClosedChannelException]
   [java.util UUID]
   [java.util.concurrent ConcurrentHashMap ConcurrentLinkedQueue]
   [java.util.concurrent.atomic AtomicBoolean]))

(use-fixtures :once quiet-server-ha-logs-fixture)

(deftest start-ha-authority-initializes-membership-hash-test
  (let [opts (valid-ha-opts)
        expected (vld/derive-ha-membership-hash opts)
        runtime (#'srv/start-ha-authority "orders" opts)
        authority (:ha-authority runtime)]
    (is (= expected (:ha-membership-hash runtime)))
    (is (= expected (ha/read-membership-hash authority)))
    (#'srv/stop-ha-authority "orders" runtime)))

(deftest stop-ha-authority-stops-lifecycle-test
  (let [opts (valid-ha-opts)
        runtime (#'srv/start-ha-authority "orders" opts)
        authority (:ha-authority runtime)]
    (#'srv/stop-ha-authority "orders" runtime)
    (is (thrown? clojure.lang.ExceptionInfo
                 (ha/read-membership-hash authority)))))

(deftest start-ha-authority-supports-sofa-jraft-adapter-test
  (let [opts (-> (valid-ha-opts)
                 (assoc-in [:ha-control-plane :backend] :sofa-jraft)
                      ;; Single-voter smoke config for local JRaft startup.
                 (assoc-in [:ha-control-plane :voters]
                           [{:peer-id "10.0.0.12:7801"
                             :ha-node-id 2
                             :promotable? true}]))
        runtime (#'srv/start-ha-authority "orders" opts)
        authority (:ha-authority runtime)]
    (is (some? authority))
    (is (string? (ha/read-membership-hash authority)))
    (#'srv/stop-ha-authority "orders" runtime)))

(deftest start-ha-authority-derives-raft-dir-from-server-root-test
  (let [root "/var/lib/datalevin-test"
        opts (-> (valid-ha-opts "ha/prod")
                 (assoc-in [:ha-control-plane :backend] :sofa-jraft))
        resolved (#'srv/with-default-ha-control-raft-dir root "orders" opts)]
    (is (= (str root
                u/+separator+
                "ha-control"
                u/+separator+
                "ha_prod"
                u/+separator+
                "10.0.0.12_7801"
                u/+separator+
                (u/hexify-string "orders"))
           (get-in resolved [:ha-control-plane :raft-dir])))))

(deftest start-ha-authority-keeps-explicit-raft-dir-test
  (let [root "/var/lib/datalevin-test"
        explicit "/srv/dtlv/raft/custom-dir"
        opts (-> (valid-ha-opts "ha/prod")
                 (assoc-in [:ha-control-plane :backend] :sofa-jraft)
                 (assoc-in [:ha-control-plane :raft-dir] explicit))
        resolved (#'srv/with-default-ha-control-raft-dir root "orders" opts)]
    (is (= explicit
           (get-in resolved [:ha-control-plane :raft-dir])))))

(deftest create-does-not-auto-reopen-consensus-ha-db-from-persisted-sessions-test
  (let [port       (random-port-candidate)
        root       (u/tmp-dir (str "server-ha-reopen-" (UUID/randomUUID)))
        group-id   (str "server-ha-reopen-" (UUID/randomUUID))
        local-peer "127.0.0.1:17801"
        peer-2     "127.0.0.1:17802"
        peer-3     "127.0.0.1:17803"
        db-name    "orders"
        endpoint-1 (str "127.0.0.1:" port)
        endpoint-2 "127.0.0.1:28899"
        endpoint-3 "127.0.0.1:28900"
        client-id  (UUID/randomUUID)
        ha-opts    {:wal? true
                    :ha-mode :consensus-lease
                    :db-identity (str "db-" (UUID/randomUUID))
                    :ha-node-id 1
                    :ha-lease-renew-ms 1000
                    :ha-lease-timeout-ms 3000
                    :ha-promotion-base-delay-ms 100
                    :ha-promotion-rank-delay-ms 200
                    :ha-max-promotion-lag-lsn 0
                    :ha-clock-skew-budget-ms 1000
                    :ha-fencing-hook {:cmd ["/bin/sh" "-c" "exit 0"]
                                      :timeout-ms 1000
                                      :retries 0
                                      :retry-delay-ms 0}
                    :ha-members [{:node-id 1 :endpoint endpoint-1}
                                 {:node-id 2 :endpoint endpoint-2}
                                 {:node-id 3 :endpoint endpoint-3}]
                    :ha-control-plane
                    {:backend :sofa-jraft
                     :group-id group-id
                     :local-peer-id local-peer
                     :rpc-timeout-ms 5000
                     :election-timeout-ms 5000
                     :operation-timeout-ms 30000
                     :voters [{:peer-id local-peer
                               :ha-node-id 1
                               :promotable? true}
                              {:peer-id peer-2
                               :ha-node-id 2
                               :promotable? true}
                              {:peer-id peer-3
                               :ha-node-id 3
                               :promotable? true}]}}]
    (try
      (let [server-1 (binding [c/*db-background-sampling?* false]
                       (srv/create {:port port :root root}))]
        (try
          (let [store (st/open (#'srv/db-dir root db-name)
                               e2e-ha-schema
                               ha-opts)]
            (try
              (#'srv/add-client server-1 "127.0.0.1" client-id c/default-username)
              (#'srv/update-client server-1
                                   client-id
                                   #(-> %
                                        (update :stores assoc db-name
                                                {:datalog? true
                                                 :dbis #{}})
                                        (update :dt-dbs conj db-name)))
              (is (contains? (set (keys (.-clients ^Server server-1))) client-id))
              (finally
                (i/close store))))
          (srv/stop server-1)
          (let [server-2 (binding [c/*db-background-sampling?* false]
                           (srv/create {:port port :root root}))]
            (try
              (is (nil? (get (.-dbs ^Server server-2) db-name)))
              (is (contains? (set (keys (.-clients ^Server server-2))) client-id))
              (finally
                (srv/stop server-2))))
          (finally
            (when (.get ^AtomicBoolean (.-running ^Server server-1))
              (srv/stop server-1)))))
      (finally
        (safe-delete-dir! root)))))

(deftest start-ha-authority-fails-closed-on-membership-mismatch-test
  (let [group-id (str "ha-test-" (UUID/randomUUID))
        opts-a (valid-ha-opts group-id)
        opts-b (assoc-in (valid-ha-opts group-id)
                         [:ha-members 2 :endpoint]
                         "10.0.0.99:8898")
        runtime-a (#'srv/start-ha-authority "orders" opts-a)]
    (try
      (is (thrown-with-msg?
           clojure.lang.ExceptionInfo
           #"membership hash mismatch"
           (#'srv/start-ha-authority "orders" opts-b)))
      (is (string? (ha/read-membership-hash (:ha-authority runtime-a))))
      (finally
        (#'srv/stop-ha-authority "orders" runtime-a)))))

(deftest startup-read-ha-authority-state-timeout-falls-back-test
  (let [authority (reify ha/ILeaseAuthority
                    (start-authority! [this] this)
                    (stop-authority! [this] this)
                    (read-lease [_ _]
                      (throw (ex-info "warming up"
                                      {:error :ha/control-timeout})))
                    (try-acquire-lease [_ _]
                      (throw (ex-info "unsupported" {})))
                    (renew-lease [_ _]
                      (throw (ex-info "unsupported" {})))
                    (read-membership-hash [_]
                      "unused")
                    (init-membership-hash! [_ membership-hash]
                      {:ok? true
                       :membership-hash membership-hash})
                    (read-voters [_] [])
                    (replace-voters! [_ _]
                      (throw (ex-info "unsupported" {}))))
        result (#'dha/startup-read-ha-authority-state
                "orders" authority "db-a")]
    (is (false? (:ok? result)))
    (is (nil? (:lease result)))
    (is (= :startup-authority-read-failed
           (get-in result [:error :reason])))
    (is (= :ha/control-timeout
           (get-in result [:error :data :error])))))

(deftest start-ha-authority-rejoins-local-authority-owner-as-follower-test
  (let [group-id (str "ha-test-" (UUID/randomUUID))
        opts (valid-ha-opts group-id)
        runtime-a (#'srv/start-ha-authority "orders" opts)
        runtime-b (atom nil)
        now-ms (System/currentTimeMillis)]
    (try
      (let [acquire (ha/try-acquire-lease
                     (:ha-authority runtime-a)
                     {:db-identity (:ha-db-identity runtime-a)
                      :leader-node-id (:ha-node-id runtime-a)
                      :leader-endpoint (:ha-local-endpoint runtime-a)
                      :lease-renew-ms (:ha-lease-renew-ms runtime-a)
                      :lease-timeout-ms (:ha-lease-timeout-ms runtime-a)
                      :leader-last-applied-lsn 1
                      :now-ms now-ms
                      :observed-version 0
                      :observed-lease nil})]
        (is (:ok? acquire))
        (#'srv/stop-ha-authority "orders" runtime-a)
        (reset! runtime-b (#'srv/start-ha-authority "orders" opts))
        (is (= :follower (:ha-role @runtime-b)))
        (is (nil? (:ha-leader-term @runtime-b)))
        (is (= (:ha-node-id opts)
               (:ha-authority-owner-node-id @runtime-b)))
        (is (= (:term acquire)
               (:ha-authority-term @runtime-b))))
      (finally
        (when-let [runtime-b' @runtime-b]
          (#'srv/stop-ha-authority "orders" runtime-b'))
        (#'srv/stop-ha-authority "orders" runtime-a)))))

(deftest ha-renew-step-keeps-leader-on-success-test
  (let [opts (valid-ha-opts)
        runtime (#'srv/start-ha-authority "orders" opts)
        now-ms (System/currentTimeMillis)]
    (try
      (let [acquire (ha/try-acquire-lease
                     (:ha-authority runtime)
                     {:db-identity (:ha-db-identity runtime)
                      :leader-node-id (:ha-node-id runtime)
                      :leader-endpoint (:ha-local-endpoint runtime)
                      :lease-renew-ms (:ha-lease-renew-ms runtime)
                      :lease-timeout-ms (:ha-lease-timeout-ms runtime)
                      :leader-last-applied-lsn 1
                      :now-ms now-ms
                      :observed-version 0
                      :observed-lease nil})
            leader-runtime (-> runtime
                               (assoc :ha-role :leader
                                      :ha-leader-term (:term acquire)
                                      :ha-last-authority-refresh-ms now-ms))
            next-state (#'srv/ha-renew-step "orders" leader-runtime)]
        (is (:ok? acquire))
        (is (= :leader (:ha-role next-state)))
        (is (= (:term acquire) (:ha-authority-term next-state))))
      (finally
        (#'srv/stop-ha-authority "orders" runtime)))))

(deftest renew-ha-leader-state-uses-current-clock-for-renew-request-test
  (let [authority (ha/new-in-memory-authority
                   {:group-id (str "ha-renew-now-" (UUID/randomUUID))})
        _ (ha/start-authority! authority)
        acquire (ha/try-acquire-lease
                 authority
                 {:db-identity "db-a"
                  :leader-node-id 2
                  :leader-endpoint "10.0.0.12:8898"
                  :lease-renew-ms 500
                  :lease-timeout-ms 1000
                  :leader-last-applied-lsn 7
                  :now-ms 1000
                  :observed-version 0
                  :observed-lease nil})
        leader-state {:ha-authority authority
                      :ha-db-identity "db-a"
                      :ha-role :leader
                      :ha-node-id 2
                      :ha-local-endpoint "10.0.0.12:8898"
                      :ha-lease-renew-ms 500
                      :ha-lease-timeout-ms 1000
                      :ha-leader-term (:term acquire)
                      :ha-leader-last-applied-lsn 7
                      :ha-last-authority-refresh-ms 0}]
    (try
      (let [next-state (with-redefs-fn
                         {#'dha/ha-now-ms (fn [] 1500)}
                         (fn []
                           (#'dha/renew-ha-leader-state
                            "orders" leader-state)))]
        (is (= 1500 (:ha-last-authority-refresh-ms next-state)))
        (is (= 2500 (:ha-lease-until-ms next-state))))
      (finally
        (ha/stop-authority! authority)))))

(deftest ha-renew-step-demotes-on-renew-term-mismatch-test
  (let [opts (valid-ha-opts)
        runtime (#'srv/start-ha-authority "orders" opts)
        now-ms (System/currentTimeMillis)]
    (try
      (let [acquire (ha/try-acquire-lease
                     (:ha-authority runtime)
                     {:db-identity (:ha-db-identity runtime)
                      :leader-node-id (:ha-node-id runtime)
                      :leader-endpoint (:ha-local-endpoint runtime)
                      :lease-renew-ms (:ha-lease-renew-ms runtime)
                      :lease-timeout-ms (:ha-lease-timeout-ms runtime)
                      :leader-last-applied-lsn 1
                      :now-ms now-ms
                      :observed-version 0
                      :observed-lease nil})
            leader-runtime (-> runtime
                               (assoc :ha-role :leader
                                      :ha-leader-term (unchecked-inc (long (:term acquire)))
                                      :ha-last-authority-refresh-ms now-ms))
            demoting-state (#'srv/ha-renew-step "orders" leader-runtime)
            follower-state (#'srv/ha-renew-step "orders"
                                                (assoc demoting-state
                                                       :ha-demoted-at-ms
                                                       (dec (System/currentTimeMillis))))]
        (is (:ok? acquire))
        (is (= :demoting (:ha-role demoting-state)))
        (is (= :renew-failed (:ha-demotion-reason demoting-state)))
        (is (nil? (:ha-leader-term demoting-state)))
        (is (= :follower (:ha-role follower-state))))
      (finally
        (#'srv/stop-ha-authority "orders" runtime)))))

(deftest ha-renew-step-refreshes-time-after-follower-sync-before-candidate-entry-test
  (let [tick (atom 0)
        follower-runtime {:ha-role :follower
                          :ha-authority-lease {:lease-until-ms 1500
                                               :leader-node-id 2}
                          :ha-authority-read-ok? true
                          :ha-membership-hash "hash-a"
                          :ha-authority-membership-hash "hash-a"
                          :ha-node-id 2
                          :ha-members [{:node-id 1 :endpoint "10.0.0.11:8898"}
                                       {:node-id 2 :endpoint "10.0.0.12:8898"}]
                          :ha-promotion-base-delay-ms 10000
                          :ha-promotion-rank-delay-ms 0}]
    (with-redefs-fn
      {#'dha/ha-now-ms
       (fn []
         (swap! tick + 1000))}
      (fn []
        (let [next-state (#'dha/advance-ha-follower-or-candidate
                          "orders" follower-runtime)]
          (is (= :candidate (:ha-role next-state)))
          (is (= 2000 (:ha-candidate-since-ms next-state)))
          (is (= 10000 (:ha-candidate-delay-ms next-state))))))))

(deftest ha-renew-step-demotes-on-membership-hash-mismatch-test
  (let [opts (valid-ha-opts)
        runtime (#'srv/start-ha-authority "orders" opts)
        now-ms (System/currentTimeMillis)]
    (try
      (let [acquire (ha/try-acquire-lease
                     (:ha-authority runtime)
                     {:db-identity (:ha-db-identity runtime)
                      :leader-node-id (:ha-node-id runtime)
                      :leader-endpoint (:ha-local-endpoint runtime)
                      :lease-renew-ms (:ha-lease-renew-ms runtime)
                      :lease-timeout-ms (:ha-lease-timeout-ms runtime)
                      :leader-last-applied-lsn 1
                      :now-ms now-ms
                      :observed-version 0
                      :observed-lease nil})
            leader-runtime (-> runtime
                               (assoc :ha-role :leader
                                      :ha-membership-hash "bogus-hash"
                                      :ha-leader-term (:term acquire)
                                      :ha-last-authority-refresh-ms now-ms))
            next-state (#'srv/ha-renew-step "orders" leader-runtime)]
        (is (:ok? acquire))
        (is (= :demoting (:ha-role next-state)))
        (is (= :membership-hash-mismatch (:ha-demotion-reason next-state)))
        (is (true? (:ha-membership-mismatch? next-state))))
      (finally
        (#'srv/stop-ha-authority "orders" runtime)))))

(deftest ha-renew-step-promotes-follower-from-empty-lease-test
  (let [opts (valid-ha-opts)
        runtime (#'srv/start-ha-authority "orders" opts)]
    (try
      (let [follower-runtime (-> runtime
                                 (assoc :ha-role :follower
                                        :ha-promotion-base-delay-ms 0
                                        :ha-promotion-rank-delay-ms 0
                                        :ha-local-last-applied-lsn 0))
            next-state (with-redefs-fn
                         {#'dha/fetch-ha-endpoint-watermark-lsn
                          (fn [_ _ _]
                            {:reachable? false
                             :reason :missing-endpoint})
                          #'dha/run-ha-fencing-hook
                          (fn [_ _ _]
                            {:ok? true})}
                         (fn []
                           (#'srv/ha-renew-step "orders" follower-runtime)))]
        (is (= :leader (:ha-role next-state)))
        (is (= 1 (:ha-leader-term next-state))))
      (finally
        (#'srv/stop-ha-authority "orders" runtime)))))

(deftest ha-renew-step-promotes-from-empty-lease-using-local-watermark-test
  (let [opts (valid-ha-opts)
        runtime (#'srv/start-ha-authority "orders" opts)]
    (try
      (let [follower-runtime (-> runtime
                                 (assoc :ha-role :follower
                                        :ha-promotion-base-delay-ms 0
                                        :ha-promotion-rank-delay-ms 0
                                        :ha-max-promotion-lag-lsn 0
                                        :ha-local-last-applied-lsn 0))
            next-state (with-redefs-fn
                         {#'dha/read-ha-local-watermark-lsn
                          (fn [_]
                            3)
                          #'dha/fetch-ha-endpoint-watermark-lsn
                          (fn [_ _ endpoint]
                            (case endpoint
                              "10.0.0.12:8898"
                              {:reachable? true
                               :last-applied-lsn 0
                               :source :remote}

                              "10.0.0.13:8898"
                              {:reachable? true
                               :last-applied-lsn 0
                               :source :remote}

                              {:reachable? false
                               :reason :missing-endpoint}))
                          #'dha/run-ha-fencing-hook
                          (fn [_ _ _]
                            {:ok? true})}
                         (fn []
                           (#'srv/ha-renew-step "orders" follower-runtime)))]
        (is (= :leader (:ha-role next-state)))
        (is (= 1 (:ha-leader-term next-state)))
        (is (= 3
               (get-in next-state
                       [:ha-authority-lease :leader-last-applied-lsn]))))
      (finally
        (#'srv/stop-ha-authority "orders" runtime)))))

(deftest ha-renew-step-candidate-lag-guard-blocks-promotion-test
  (let [opts (valid-ha-opts)
        runtime (#'srv/start-ha-authority "orders" opts)]
    (try
      (let [authority (:ha-authority runtime)
            _ (ha/try-acquire-lease
               authority
               {:db-identity (:ha-db-identity runtime)
                :leader-node-id 1
                :leader-endpoint "10.0.0.11:8898"
                :lease-renew-ms (:ha-lease-renew-ms runtime)
                :lease-timeout-ms (:ha-lease-timeout-ms runtime)
                :leader-last-applied-lsn 10
                :now-ms 1000
                :observed-version 0
                :observed-lease nil})
            follower-runtime (-> runtime
                                 (assoc :ha-role :follower
                                        :ha-promotion-base-delay-ms 0
                                        :ha-promotion-rank-delay-ms 0
                                        :ha-max-promotion-lag-lsn 0
                                        :ha-local-last-applied-lsn 0))
            next-state (with-redefs-fn {#'dha/run-ha-fencing-hook
                                        (fn [_ _ _]
                                          (throw (ex-info "must-not-call-fencing" {})))
                                        #'dha/fetch-ha-endpoint-watermark-lsn
                                        (fn [_ _ endpoint]
                                          (case endpoint
                                            "10.0.0.11:8898"
                                            {:reachable? true
                                             :last-applied-lsn 10
                                             :source :remote}

                                            "10.0.0.13:8898"
                                            {:reachable? true
                                             :last-applied-lsn 0
                                             :source :remote}

                                            {:reachable? false
                                             :reason :missing-endpoint}))}
                         (fn []
                           (#'srv/ha-renew-step "orders" follower-runtime)))]
        (is (= :follower (:ha-role next-state)))
        (is (= :lag-guard-failed (:ha-promotion-last-failure next-state))))
      (finally
        (#'srv/stop-ha-authority "orders" runtime)))))

(deftest ha-renew-step-candidate-fencing-failure-blocks-promotion-test
  (let [opts (assoc (valid-ha-opts)
                    :ha-fencing-hook {:cmd ["/bin/sh" "-c" "exit 7"]
                                      :timeout-ms 1000
                                      :retries 0
                                      :retry-delay-ms 0})
        runtime (#'srv/start-ha-authority "orders" opts)]
    (try
      (let [authority (:ha-authority runtime)
            _ (ha/try-acquire-lease
               authority
               {:db-identity (:ha-db-identity runtime)
                :leader-node-id 1
                :leader-endpoint "10.0.0.11:8898"
                :lease-renew-ms (:ha-lease-renew-ms runtime)
                :lease-timeout-ms (:ha-lease-timeout-ms runtime)
                :leader-last-applied-lsn 0
                :now-ms 1000
                :observed-version 0
                :observed-lease nil})
            follower-runtime (-> runtime
                                 (assoc :ha-role :follower
                                        :ha-promotion-base-delay-ms 0
                                        :ha-promotion-rank-delay-ms 0
                                        :ha-max-promotion-lag-lsn 0
                                        :ha-local-last-applied-lsn 0))
            next-state (with-redefs-fn
                         {#'dha/fetch-ha-endpoint-watermark-lsn
                          (fn [_ _ endpoint]
                            (case endpoint
                              "10.0.0.11:8898"
                              {:reachable? true
                               :last-applied-lsn 0
                               :source :remote}

                              "10.0.0.13:8898"
                              {:reachable? true
                               :last-applied-lsn 0
                               :source :remote}

                              {:reachable? false
                               :reason :missing-endpoint}))}
                         (fn []
                           (#'srv/ha-renew-step "orders" follower-runtime)))]
        (is (= :follower (:ha-role next-state)))
        (is (= :fencing-failed (:ha-promotion-last-failure next-state))))
      (finally
        (#'srv/stop-ha-authority "orders" runtime)))))

(deftest ha-run-fencing-hook-retries-reuse-fence-op-id-test
  (let [dir (u/tmp-dir (str "ha-fence-hook-" (UUID/randomUUID)))
        log-file (str dir u/+separator+ "fence.log")
        cmd ["/bin/sh" "-c"
             (str "printf '%s\\n' "
                  "\"$DTLV_FENCE_OP_ID,$DTLV_TERM_CANDIDATE,$DTLV_TERM_OBSERVED\" "
                  ">> \"$1\"; "
                  "n=$(wc -l < \"$1\"); "
                  "if [ \"$n\" -lt 3 ]; then exit 7; else exit 0; fi")
             "fence-hook"
             log-file]]
    (try
      (u/create-dirs dir)
      (let [result (#'dha/run-ha-fencing-hook
                    "orders"
                    {:ha-node-id 2
                     :ha-fencing-hook {:cmd cmd
                                       :timeout-ms 1000
                                       :retries 2
                                       :retry-delay-ms 0}}
                    {:leader-node-id 1
                     :leader-endpoint "10.0.0.11:8898"
                     :term 4})
            calls (->> (slurp log-file)
                       s/split-lines)]
        (is (:ok? result))
        (is (= 3 (:attempt result)))
        (is (= 1
               (count (distinct (map #(first (s/split % #","))
                                     calls)))))
        (is (= #{"5"}
               (set (map #(second (s/split % #",")) calls))))
        (is (= #{"4"}
               (set (map #(nth (s/split % #",") 2) calls)))))
      (finally
        (try
          (u/delete-files dir)
          (catch Exception _))))))

(deftest ha-renew-step-follower-control-read-failure-blocks-promotion-test
  (let [authority (reify ha/ILeaseAuthority
                    (start-authority! [this] this)
                    (stop-authority! [this] this)
                    (read-lease [_ _]
                      (throw (ex-info "quorum lost"
                                      {:error :ha/control-timeout})))
                    (try-acquire-lease [_ _]
                      (throw (ex-info "must-not-acquire" {})))
                    (renew-lease [_ _]
                      (throw (ex-info "must-not-renew" {})))
                    (read-membership-hash [_]
                      (throw (ex-info "must-not-read-membership" {})))
                    (init-membership-hash! [_ _]
                      (throw (ex-info "unsupported" {})))
                    (read-voters [_] [])
                    (replace-voters! [_ _]
                      (throw (ex-info "unsupported" {}))))
        follower-runtime
        {:ha-authority authority
         :ha-db-identity "db-a"
         :ha-role :follower
         :ha-node-id 2
         :ha-local-endpoint "10.0.0.12:8898"
         :ha-members [{:node-id 1 :endpoint "10.0.0.11:8898"}
                      {:node-id 2 :endpoint "10.0.0.12:8898"}]
         :ha-membership-hash "hash-a"
         :ha-authority-membership-hash "hash-a"
         :ha-lease-renew-ms 5000
         :ha-lease-timeout-ms 15000
         :ha-promotion-base-delay-ms 0
         :ha-promotion-rank-delay-ms 0
         :ha-max-promotion-lag-lsn 0
         :ha-local-last-applied-lsn 0}
        next-state (with-redefs-fn
                     {#'dha/run-ha-fencing-hook
                      (fn [_ _ _]
                        (throw (ex-info "must-not-call-fencing" {})))}
                     (fn []
                       (#'srv/ha-renew-step "orders" follower-runtime)))]
    (is (= :follower (:ha-role next-state)))
    (is (false? (:ha-authority-read-ok? next-state)))
    (is (= :authority-read-failed
           (:ha-promotion-last-failure next-state)))
    (is (= :authority-read-failed
           (get-in next-state
                   [:ha-promotion-failure-details :reason])))
    (is (= :ha/control-timeout
           (get-in next-state
                   [:ha-promotion-failure-details :data :error])))))

(deftest ha-renew-step-candidate-clock-skew-pause-resumes-after-recovery-test
  (let [opts (assoc (valid-ha-opts)
                    :ha-clock-skew-hook {:cmd ["/bin/sh" "-c" "printf 0"]
                                         :timeout-ms 1000
                                         :retries 0
                                         :retry-delay-ms 0})
        runtime (#'srv/start-ha-authority "orders" opts)
        hook-results (atom [{:ok? true
                             :paused? true
                             :reason :clock-skew-budget-breached
                             :budget-ms 100
                             :clock-skew-ms 125}
                            {:ok? true
                             :paused? false
                             :reason :clock-skew-within-budget
                             :budget-ms 100
                             :clock-skew-ms 25}])]
    (try
      (let [follower-runtime (-> runtime
                                 (assoc :ha-role :follower
                                        :ha-promotion-base-delay-ms 0
                                        :ha-promotion-rank-delay-ms 0
                                        :ha-max-promotion-lag-lsn 0
                                        :ha-local-last-applied-lsn 0))
            [paused-state resumed-state]
            (with-redefs-fn
              {#'dha/run-ha-clock-skew-hook
               (fn [_ _]
                 (let [result (first @hook-results)]
                   (swap! hook-results #(vec (rest %)))
                   result))
               #'dha/fetch-ha-endpoint-watermark-lsn
               (fn [_ _ endpoint]
                 (case endpoint
                   "10.0.0.12:8898"
                   {:reachable? true
                    :last-applied-lsn 0
                    :source :remote}

                   "10.0.0.13:8898"
                   {:reachable? true
                    :last-applied-lsn 0
                    :source :remote}

                   {:reachable? false
                    :reason :missing-endpoint}))
               #'dha/run-ha-fencing-hook
               (fn [_ _ _]
                 {:ok? true})}
              (fn []
                (let [state-1 (#'srv/ha-renew-step "orders" follower-runtime)
                      state-2 (#'srv/ha-renew-step "orders" state-1)]
                  [state-1 state-2])))]
        (is (= :follower (:ha-role paused-state)))
        (is (true? (:ha-clock-skew-paused? paused-state)))
        (is (= :clock-skew-paused
               (:ha-promotion-last-failure paused-state)))
        (is (= :clock-skew-budget-breached
               (get-in paused-state
                       [:ha-promotion-failure-details :reason])))
        (is (= 125
               (get-in paused-state
                       [:ha-promotion-failure-details :clock-skew-ms])))
        (is (= :leader (:ha-role resumed-state)))
        (is (false? (:ha-clock-skew-paused? resumed-state)))
        (is (= 25 (:ha-clock-skew-last-observed-ms resumed-state))))
      (finally
        (#'srv/stop-ha-authority "orders" runtime)))))

(deftest ha-renew-step-candidate-lag-input-uses-reachable-watermark-test
  (let [opts (valid-ha-opts)
        runtime (#'srv/start-ha-authority "orders" opts)]
    (try
      (let [authority (:ha-authority runtime)
            _ (ha/try-acquire-lease
               authority
               {:db-identity (:ha-db-identity runtime)
                :leader-node-id 1
                :leader-endpoint "10.0.0.11:8898"
                :lease-renew-ms (:ha-lease-renew-ms runtime)
                :lease-timeout-ms (:ha-lease-timeout-ms runtime)
                :leader-last-applied-lsn 5
                :now-ms 1000
                :observed-version 0
                :observed-lease nil})
            follower-runtime (-> runtime
                                 (assoc :ha-role :follower
                                        :ha-promotion-base-delay-ms 0
                                        :ha-promotion-rank-delay-ms 0
                                        :ha-max-promotion-lag-lsn 0
                                        :ha-local-last-applied-lsn 8))
            next-state (with-redefs-fn {#'dha/run-ha-fencing-hook
                                        (fn [_ _ _] {:ok? true})
                                        #'dha/fetch-leader-watermark-lsn
                                        (fn [_ _ _]
                                          {:reachable? true
                                           :last-applied-lsn 10
                                           :source :test})}
                         (fn []
                           (#'srv/ha-renew-step "orders" follower-runtime)))]
        (is (= :follower (:ha-role next-state)))
        (is (= :lag-guard-failed (:ha-promotion-last-failure next-state)))
        (is (= :pre-fence
               (get-in next-state [:ha-promotion-failure-details :phase])))
        (is (= 10
               (get-in next-state
                       [:ha-promotion-failure-details
                        :leader-lag-input
                        :leader-watermark-last-applied-lsn]))))
      (finally
        (#'srv/stop-ha-authority "orders" runtime)))))

(deftest ha-renew-step-candidate-expired-reachable-leader-behind-uses-best-reachable-watermark-test
  (let [opts (valid-ha-opts)
        runtime (#'srv/start-ha-authority "orders" opts)]
    (try
      (let [authority (:ha-authority runtime)
            _ (ha/try-acquire-lease
               authority
               {:db-identity (:ha-db-identity runtime)
                :leader-node-id 1
                :leader-endpoint "10.0.0.11:8898"
                :lease-renew-ms (:ha-lease-renew-ms runtime)
                :lease-timeout-ms (:ha-lease-timeout-ms runtime)
                :leader-last-applied-lsn 17
                :now-ms 1000
                :observed-version 0
                :observed-lease nil})
            follower-runtime (-> runtime
                                 (assoc :ha-role :follower
                                        :ha-promotion-base-delay-ms 0
                                        :ha-promotion-rank-delay-ms 0
                                        :ha-max-promotion-lag-lsn 0
                                        :ha-local-last-applied-lsn 10))
            next-state (with-redefs-fn
                         {#'dha/run-ha-fencing-hook
                          (fn [_ _ _] {:ok? true})
                          #'dha/fetch-ha-endpoint-watermark-lsn
                          (fn [_ _ endpoint]
                            (case endpoint
                              "10.0.0.11:8898"
                              {:reachable? true
                               :last-applied-lsn 4
                               :source :remote}

                              "10.0.0.12:8898"
                              {:reachable? true
                               :last-applied-lsn 10
                               :source :local}

                              "10.0.0.13:8898"
                              {:reachable? true
                               :last-applied-lsn 10
                               :source :remote}

                              {:reachable? false
                               :reason :missing-endpoint}))}
                         (fn []
                           (#'srv/ha-renew-step "orders" follower-runtime)))]
        (is (= :leader (:ha-role next-state)))
        (is (= 2 (:ha-authority-owner-node-id next-state)))
        (is (= 10
               (get-in next-state
                       [:ha-authority-lease :leader-last-applied-lsn])))
        (is (= 10
               (get-in next-state
                       [:ha-local-last-applied-lsn])))
        (is (nil? (:ha-promotion-last-failure next-state))))
      (finally
        (#'srv/stop-ha-authority "orders" runtime)))))

(deftest ha-renew-step-candidate-unreachable-leader-waits-before-cas-test
  (let [opts (valid-ha-opts)
        runtime (#'srv/start-ha-authority "orders" opts)
        wait-calls (atom 0)]
    (try
      (let [authority (:ha-authority runtime)
            _ (ha/try-acquire-lease
               authority
               {:db-identity (:ha-db-identity runtime)
                :leader-node-id 1
                :leader-endpoint "10.0.0.11:8898"
                :lease-renew-ms (:ha-lease-renew-ms runtime)
                :lease-timeout-ms (:ha-lease-timeout-ms runtime)
                :leader-last-applied-lsn 0
                :now-ms 1000
                :observed-version 0
                :observed-lease nil})
            follower-runtime (-> runtime
                                 (assoc :ha-role :follower
                                        :ha-promotion-base-delay-ms 0
                                        :ha-promotion-rank-delay-ms 0
                                        :ha-max-promotion-lag-lsn 0
                                        :ha-local-last-applied-lsn 0))
            next-state (with-redefs-fn {#'dha/run-ha-fencing-hook
                                        (fn [_ _ _] {:ok? true})
                                        #'dha/fetch-leader-watermark-lsn
                                        (fn [_ _ _]
                                          {:reachable? false
                                           :reason :test-unreachable})
                                        #'dha/fetch-ha-endpoint-watermark-lsn
                                        (fn [_ _ endpoint]
                                          (case endpoint
                                            "10.0.0.12:8898"
                                            {:reachable? true
                                             :last-applied-lsn 0
                                             :source :local}

                                            "10.0.0.13:8898"
                                            {:reachable? true
                                             :last-applied-lsn 0
                                             :source :remote}

                                            {:reachable? false
                                             :reason :test-unreachable}))
                                        #'dha/maybe-wait-unreachable-leader-before-pre-cas!
                                        (fn [_ _]
                                          (swap! wait-calls inc)
                                          {:slept-ms 0
                                           :wait-until-ms 0})}
                         (fn []
                           (#'srv/ha-renew-step "orders" follower-runtime)))]
        (is (= 1 @wait-calls))
        (is (= :leader (:ha-role next-state)))
        (is (nil? (:ha-promotion-wait-before-cas-ms next-state))))
      (finally
        (#'srv/stop-ha-authority "orders" runtime)))))

(deftest ha-renew-step-candidate-unreachable-leader-resumes-after-wait-without-refencing-test
  (let [opts (valid-ha-opts)
        runtime (#'srv/start-ha-authority "orders" opts)
        fence-calls (atom 0)
        wait-calls (atom 0)]
    (try
      (let [authority (:ha-authority runtime)
            _ (ha/try-acquire-lease
               authority
               {:db-identity (:ha-db-identity runtime)
                :leader-node-id 1
                :leader-endpoint "10.0.0.11:8898"
                :lease-renew-ms (:ha-lease-renew-ms runtime)
                :lease-timeout-ms (:ha-lease-timeout-ms runtime)
                :leader-last-applied-lsn 0
                :now-ms 1000
                :observed-version 0
                :observed-lease nil})
            follower-runtime (-> runtime
                                 (assoc :ha-role :follower
                                        :ha-promotion-base-delay-ms 0
                                        :ha-promotion-rank-delay-ms 0
                                        :ha-max-promotion-lag-lsn 0
                                        :ha-local-last-applied-lsn 0))
            wait-until-ms (+ (System/currentTimeMillis) 200)
            promote-step
            (fn [state]
              (#'srv/ha-renew-step "orders" state))
            next-state
            (with-redefs-fn
              {#'dha/run-ha-fencing-hook
               (fn [_ _ _]
                 (swap! fence-calls inc)
                 {:ok? true})
               #'dha/fetch-leader-watermark-lsn
               (fn [_ _ _]
                 {:reachable? false
                  :reason :test-unreachable})
               #'dha/fetch-ha-endpoint-watermark-lsn
               (fn [_ _ endpoint]
                 (case endpoint
                   "10.0.0.12:8898"
                   {:reachable? true
                    :last-applied-lsn 0
                    :source :local}

                   "10.0.0.13:8898"
                   {:reachable? true
                    :last-applied-lsn 0
                    :source :remote}

                   {:reachable? false
                    :reason :test-unreachable}))
               #'dha/maybe-wait-unreachable-leader-before-pre-cas!
               (fn [_ _]
                 (swap! wait-calls inc)
                 (if (= 1 @wait-calls)
                   {:wait-ms 200
                    :wait-until-ms wait-until-ms}
                   {:wait-ms 0
                    :wait-until-ms wait-until-ms}))}
              (fn []
                (let [waiting-state (promote-step follower-runtime)]
                  (Thread/sleep 225)
                  [waiting-state
                   (promote-step waiting-state)])))]
        (let [[waiting-state promoted-state] next-state]
          (is (= 1 @fence-calls))
          (is (= 2 @wait-calls))
          (is (= :candidate (:ha-role waiting-state)))
          (is (= wait-until-ms
                 (:ha-candidate-pre-cas-wait-until-ms waiting-state)))
          (is (= 200 (:ha-promotion-wait-before-cas-ms waiting-state)))
          (is (= :leader (:ha-role promoted-state)))
          (is (nil? (:ha-candidate-pre-cas-wait-until-ms promoted-state)))
          (is (nil? (:ha-promotion-wait-before-cas-ms promoted-state)))))
      (finally
        (#'srv/stop-ha-authority "orders" runtime)))))

(deftest ha-renew-step-candidate-unreachable-leader-uses-reachable-member-watermark-test
  (let [opts (valid-ha-opts)
        runtime (#'srv/start-ha-authority "orders" opts)]
    (try
      (let [authority (:ha-authority runtime)
            _ (ha/try-acquire-lease
               authority
               {:db-identity (:ha-db-identity runtime)
                :leader-node-id 1
                :leader-endpoint "10.0.0.11:8898"
                :lease-renew-ms (:ha-lease-renew-ms runtime)
                :lease-timeout-ms (:ha-lease-timeout-ms runtime)
                :leader-last-applied-lsn 17
                :now-ms 1000
                :observed-version 0
                :observed-lease nil})
            follower-runtime (-> runtime
                                 (assoc :ha-role :follower
                                        :ha-promotion-base-delay-ms 0
                                        :ha-promotion-rank-delay-ms 0
                                        :ha-max-promotion-lag-lsn 0
                                        :ha-local-last-applied-lsn 10))
            next-state (with-redefs-fn
                         {#'dha/run-ha-fencing-hook
                          (fn [_ _ _] {:ok? true})
                          #'dha/maybe-wait-unreachable-leader-before-pre-cas!
                          (fn [_ _]
                            {:slept-ms 0
                             :wait-until-ms 0})
                          #'dha/fetch-ha-endpoint-watermark-lsn
                          (fn [_ _ endpoint]
                            (case endpoint
                              "10.0.0.11:8898"
                              {:reachable? false
                               :reason :endpoint-watermark-fetch-failed}

                              "10.0.0.12:8898"
                              {:reachable? true
                               :last-applied-lsn 10
                               :source :local}

                              "10.0.0.13:8898"
                              {:reachable? true
                               :last-applied-lsn 10
                               :source :remote}

                              {:reachable? false
                               :reason :missing-endpoint}))}
                         (fn []
                           (#'srv/ha-renew-step "orders" follower-runtime)))]
        (is (= :leader (:ha-role next-state)))
        (is (= 2 (:ha-authority-owner-node-id next-state)))
        (is (= 10
               (get-in next-state
                       [:ha-authority-lease :leader-last-applied-lsn])))
        (is (nil? (:ha-promotion-last-failure next-state))))
      (finally
        (#'srv/stop-ha-authority "orders" runtime)))))

(deftest ha-loop-sleep-ms-respects-candidate-and-follower-deadlines-test
  (let [now-ms 1000]
    (is (= 300
           (#'srv/ha-loop-sleep-ms
            {:ha-role :candidate
             :ha-lease-renew-ms 5000
             :ha-candidate-since-ms now-ms
             :ha-candidate-delay-ms 300}
            now-ms)))
    (is (= 250
           (#'srv/ha-loop-sleep-ms
            {:ha-role :follower
             :ha-lease-renew-ms 5000
             :ha-follower-next-sync-not-before-ms (+ now-ms 250)}
            now-ms)))
    (is (= 250
           (#'srv/ha-loop-sleep-ms
            {:ha-role :candidate
             :ha-lease-renew-ms 5000
             :ha-candidate-pre-cas-wait-until-ms (+ now-ms 250)}
            now-ms)))
    (is (= 5000
           (#'srv/ha-loop-sleep-ms
            {:ha-role :leader
             :ha-lease-renew-ms 5000}
            now-ms)))))

(deftest ha-maybe-enter-candidate-blocked-when-follower-degraded-test
  (let [now-ms (System/currentTimeMillis)
        m {:ha-role :follower
           :ha-follower-degraded? true
           :ha-follower-degraded-reason :wal-gap
           :ha-follower-degraded-details {:error :ha/txlog-gap}
           :ha-node-id 2
           :ha-members [{:node-id 1 :endpoint "10.0.0.11:8898"}
                        {:node-id 2 :endpoint "10.0.0.12:8898"}]
           :ha-promotion-base-delay-ms 0
           :ha-promotion-rank-delay-ms 0
           :ha-membership-hash "hash-a"
           :ha-authority-membership-hash "hash-a"
           :ha-authority-lease {:lease-until-ms (dec now-ms)}}
        next-m (#'dha/maybe-enter-ha-candidate m now-ms)]
    (is (= :follower (:ha-role next-m)))
    (is (= :follower-degraded (:ha-promotion-last-failure next-m)))
    (is (= :wal-gap (get-in next-m [:ha-promotion-failure-details :reason])))))

(deftest ha-maybe-enter-candidate-blocked-when-clock-skew-paused-test
  (let [now-ms (System/currentTimeMillis)
        m {:ha-role :follower
           :ha-clock-skew-paused? true
           :ha-clock-skew-budget-ms 100
           :ha-clock-skew-last-check-ms now-ms
           :ha-clock-skew-last-result {:ok? true
                                       :paused? true
                                       :reason :clock-skew-budget-breached
                                       :budget-ms 100
                                       :clock-skew-ms 125}
           :ha-node-id 2
           :ha-members [{:node-id 1 :endpoint "10.0.0.11:8898"}
                        {:node-id 2 :endpoint "10.0.0.12:8898"}]
           :ha-promotion-base-delay-ms 0
           :ha-promotion-rank-delay-ms 0
           :ha-membership-hash "hash-a"
           :ha-authority-membership-hash "hash-a"
           :ha-authority-lease {:lease-until-ms (dec now-ms)}}
        next-m (#'dha/maybe-enter-ha-candidate m now-ms)]
    (is (= :follower (:ha-role next-m)))
    (is (= :clock-skew-paused (:ha-promotion-last-failure next-m)))
    (is (= :clock-skew-budget-breached
           (get-in next-m [:ha-promotion-failure-details :reason])))
    (is (= 125
           (get-in next-m [:ha-promotion-failure-details :clock-skew-ms])))))

(deftest ha-maybe-enter-candidate-blocked-when-authority-read-failed-test
  (let [now-ms (System/currentTimeMillis)
        m {:ha-role :follower
           :ha-authority-read-ok? false
           :ha-authority-read-error {:reason :authority-read-failed
                                     :data {:error :ha/control-timeout}}
           :ha-node-id 2
           :ha-members [{:node-id 1 :endpoint "10.0.0.11:8898"}
                        {:node-id 2 :endpoint "10.0.0.12:8898"}]
           :ha-promotion-base-delay-ms 0
           :ha-promotion-rank-delay-ms 0
           :ha-membership-hash "hash-a"
           :ha-authority-membership-hash "hash-a"
           :ha-authority-lease {:lease-until-ms (dec now-ms)}}
        next-m (#'dha/maybe-enter-ha-candidate m now-ms)]
    (is (= :follower (:ha-role next-m)))
    (is (= :authority-read-failed (:ha-promotion-last-failure next-m)))
    (is (= :authority-read-failed
           (get-in next-m [:ha-promotion-failure-details :reason])))
    (is (= :ha/control-timeout
           (get-in next-m [:ha-promotion-failure-details :data :error])))))

(deftest ha-maybe-enter-candidate-blocked-when-rejoin-in-progress-test
  (let [now-ms (System/currentTimeMillis)
        m {:ha-role :follower
           :ha-node-id 2
           :ha-authority-owner-node-id 1
           :ha-local-last-applied-lsn 3
           :ha-rejoin-promotion-blocked? true
           :ha-rejoin-promotion-blocked-until-ms (+ now-ms 5000)
           :ha-members [{:node-id 1 :endpoint "10.0.0.11:8898"}
                        {:node-id 2 :endpoint "10.0.0.12:8898"}]
           :ha-promotion-base-delay-ms 0
           :ha-promotion-rank-delay-ms 0
           :ha-max-promotion-lag-lsn 0
           :ha-membership-hash "hash-a"
           :ha-authority-membership-hash "hash-a"
           :ha-authority-read-ok? true
           :ha-authority-lease {:lease-until-ms (dec now-ms)
                                :leader-node-id 1
                                :leader-last-applied-lsn 5}}
        next-m (#'dha/maybe-enter-ha-candidate m now-ms)]
    (is (= :follower (:ha-role next-m)))
    (is (= :rejoin-in-progress (:ha-promotion-last-failure next-m)))
    (is (= 1
           (get-in next-m
                   [:ha-promotion-failure-details :authority-owner-node-id])))))

(deftest maybe-clear-ha-rejoin-promotion-block-after-catch-up-test
  (let [now-ms (System/currentTimeMillis)
        m {:ha-role :follower
           :ha-node-id 2
           :ha-authority-owner-node-id 1
           :ha-local-last-applied-lsn 5
           :ha-rejoin-promotion-blocked? true
           :ha-rejoin-promotion-blocked-until-ms (+ now-ms 5000)
           :ha-max-promotion-lag-lsn 0
           :ha-authority-read-ok? true
           :ha-authority-lease {:leader-node-id 1
                                :leader-last-applied-lsn 5}}
        next-m (#'dha/maybe-clear-ha-rejoin-promotion-block m now-ms)]
    (is (false? (:ha-rejoin-promotion-blocked? next-m)))
    (is (nil? (:ha-rejoin-promotion-blocked-until-ms next-m)))
    (is (integer? (:ha-rejoin-promotion-cleared-ms next-m)))))
