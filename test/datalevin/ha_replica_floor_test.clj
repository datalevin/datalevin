(ns datalevin.ha-replica-floor-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [datalevin.core :as d]
            [datalevin.ha :as ha]
            [datalevin.ha.replication :as repl]
            [datalevin.kv :as kv]
            [datalevin.server.ha :as server-ha]
            [datalevin.test.core :refer [db-fixture]]
            [datalevin.util :as u])
  (:import [java.nio.channels ClosedChannelException]))

(use-fixtures :each db-fixture)

(defn- with-reports [f]
  (let [now (atom 0)
        reports (atom [])
        clears (atom [])
        response (atom {:ok? true :ttl-ms 30000})]
    (with-redefs [repl/ha-now-nanos #(long (* 1000000 @now))
                  repl/report-ha-replica-floor!
                  (fn [_ _ endpoint lsn]
                    (swap! reports conj [endpoint lsn @now])
                    (let [result @response]
                      (if (fn? result) (result) result)))
                  repl/clear-ha-replica-floor!
                  (fn [_ _ endpoint]
                    (swap! clears conj endpoint)
                    {:ok? true})]
      (f {:reports reports :clears clears :response response :now now
          :report! (fn report!
                     ([m lsn at-ms] (report! m "leader" 1 lsn at-ms))
                     ([m endpoint term lsn at-ms]
                      (reset! now at-ms)
                      (#'repl/maybe-report-ha-replica-floor
                       "db" m endpoint term lsn)))}))))

(deftest progress-is-coalesced-and-idle-heartbeats-continue
  (with-reports
    (fn [{:keys [report! reports]}]
      (let [initial (report! {:ha-role :follower} 0 0)
            busy (reduce #(report! %1 %2 %2) initial (range 1 1000))
            progressed (report! busy 1000 1000)
            idle (report! progressed 1000 10999)
            heartbeat (report! idle 1000 11000)]
        (is (= initial busy))
        (is (= progressed idle))
        (is (= [["leader" 0 0] ["leader" 1000 1000]
                ["leader" 1000 11000]] @reports))
        (is (= 1000 (get-in heartbeat
                            [:ha-follower-replica-floor-report :applied-lsn])))))))

(deftest heartbeat-and-progress-respect-the-leaders-ttl
  (with-reports
    (fn [{:keys [report! reports response]}]
      (reset! response {:ok? true :ttl-ms 300})
      (let [initial (report! {} 0 0)
            skipped (report! initial 1 99)
            advanced (report! skipped 1 100)
            idle (report! advanced 1 199)]
        (is (= initial skipped))
        (is (= advanced idle))
        (report! idle 1 200)
        (is (= [["leader" 0 0] ["leader" 1 100] ["leader" 1 200]]
               @reports))))))

(deftest empty-batch-polls-retain-report-state
  (with-reports
    (fn [{:keys [reports now]}]
      (with-redefs-fn
        {#'repl/fetch-ha-follower-records-with-gap-fallback
         (fn [& _] {:records [] :source-endpoint "leader"
                    :source-order ["leader"]})
         #'repl/read-ha-local-last-applied-lsn (constantly 14)}
        (fn []
          (let [lease {:leader-endpoint "leader" :term 1}
                initial {:ha-node-id 2 :ha-local-last-applied-lsn 14}
                poll (fn [m at-ms]
                       (reset! now at-ms)
                       (:state (#'repl/sync-ha-follower-batch
                                 "db" m lease 15 at-ms)))
                first-poll (poll initial 0)
                idle (reduce poll first-poll (range 250 10000 250))]
            (is (= [["leader" 14 0]] @reports))
            (is (= 15 (:ha-follower-next-lsn idle)))
            (is (= (:ha-follower-replica-floor-report first-poll)
                   (:ha-follower-replica-floor-report idle)))
            (poll idle 10000)
            (is (= [["leader" 14 0] ["leader" 14 10000]] @reports))))))))

(deftest nonexpiring-floors-and-older-leaders
  (with-reports
    (fn [{:keys [report! reports response]}]
      (testing "a zero TTL still gets periodic heartbeats"
        (reset! response {:ok? true :ttl-ms 0})
        (let [initial (report! {} 0 0)]
          (is (= initial (report! initial 0 9999)))
          (report! initial 0 10000)
          (is (= 2 (count @reports)))))
      (testing "an unknown TTL preserves reporting on every poll"
        (reset! reports [])
        (reset! response {:ok? true})
        (let [initial (report! {} 0 0)]
          (report! initial 0 1)
          (is (= 2 (count @reports))))))))

(deftest leader-changes-and-local-resets-report-immediately
  (with-reports
    (fn [{:keys [report! reports response clears]}]
      (let [initial (report! {} 10 0)
            new-leader (report! initial "other-leader" 2 11 1)
            new-term (report! new-leader "other-leader" 3 11 2)]
        (reset! response
                (fn []
                  (if (= 4 (count @reports))
                    (throw (ex-info "Replica floor LSN cannot move backward"
                                    {:type :txlog/invalid-floor-provider-state
                                     :old-lsn 11 :new-lsn 5}))
                    {:ok? true :ttl-ms 30000})))
        (let [reset-state (report! new-term "other-leader" 3 5 3)]
          (is (= [["leader" 10 0] ["other-leader" 11 1]
                  ["other-leader" 11 2] ["other-leader" 5 3]
                  ["other-leader" 5 3]] @reports))
          (is (= ["other-leader"] @clears))
          (is (= 5 (get-in reset-state
                           [:ha-follower-replica-floor-report :applied-lsn]))))))))

(deftest skipped-and-failed-reports-remain-eligible-for-retry
  (with-reports
    (fn [{:keys [report! reports response]}]
      (let [initial (report! {} 0 0)]
        (reset! response {:ok? false :skipped? true})
        (is (= initial (report! initial 1 1000)))
        (reset! response #(throw (ClosedChannelException.)))
        (is (= initial (report! initial 1 1001)))
        (reset! response {:ok? true :ttl-ms 30000})
        (let [retried (report! initial 1 1002)]
          (is (= 4 (count @reports)))
          (is (= 1002000000 (get-in retried
                                   [:ha-follower-replica-floor-report
                                    :reported-at-nanos]))))))))

(deftest report-tracking-survives-follower-publication-and-runtime-clear
  (with-reports
    (fn [{:keys [report!]}]
      (let [initial {:ha-role :follower}
            reported (report! initial 10 0)
            patch (server-ha/ha-follower-side-effect-patch initial reported)]
        (is (= reported (server-ha/apply-state-patch initial patch)))
        (is (nil? (server-ha/ha-renew-state-patch initial reported)))
        (is (not (contains? (ha/clear-ha-runtime-state reported)
                            :ha-follower-replica-floor-report)))))))

(deftest replica-floor-update-advertises-retention-ttl
  (let [dir (u/tmp-dir (str "replica-floor-ttl-" (random-uuid)))]
    (try
      (let [db (d/open-kv dir {:wal? true :wal-replica-floor-ttl-ms 600
                                :snapshot-bootstrap-force? false
                                :snapshot-scheduler? false})]
        (try
          (let [result (kv/txlog-update-replica-floor! db 2 0)]
            (is (true? (:ok? result)))
            (is (= 600 (:ttl-ms result)))
            (is (= 0 (:applied-lsn result))))
          (finally (d/close-kv db))))
      (finally (u/delete-files dir)))))
