(ns datalevin.ha.control-alignment-test
  (:require
   [clojure.test :refer [deftest is testing]]
   [datalevin.ha.control :as ctrl]
   [datalevin.server.ha :as ha])
  (:import
   [com.alipay.sofa.jraft Node Status]
   [com.alipay.sofa.jraft.entity PeerId]
   [java.util.concurrent CountDownLatch]
   [java.util.concurrent.atomic AtomicBoolean]))

(defn- peer [id]
  (doto (PeerId.) (.parse (str "127.0.0.1:" (+ 7000 (long id))))))

(defn- authority
  ([] (authority {}))
  ([{:keys [owner lease-until-ms live-peers current-peers leader? outcome]
     :or {owner 2 lease-until-ms 20000 live-peers [1 2 3]
          current-peers [1 2 3] leader? true outcome (Status/OK)}}]
   (let [calls (atom [])
         node (reify Node
                (isLeader [_] leader?)
                (listPeers [_] (mapv peer current-peers))
                (listAlivePeers [_] (mapv peer live-peers))
                (transferLeadershipTo [_ target]
                  (swap! calls conj (str target))
                  (if (instance? Exception outcome)
                    (throw outcome)
                    outcome)))]
     [(ctrl/map->SofaJraftLeaseAuthority
        {:group-id "alignment"
         :local-peer-id (str (peer 1))
         :voters (mapv (fn [id] {:peer-id (str (peer id))
                                 :ha-node-id id :promotable? true}) [1 2 3])
         :election-timeout-ms 1000
         :clock-skew-budget-ms 100
         :fsm-state (atom {:leases {"db" {:lease {:leader-node-id owner
                                                  :term 7
                                                  :lease-until-ms lease-until-ms}}}})
         :node-v (volatile! node)
         :running-v (volatile! true)
         :leadership-transfer-state (atom nil)})
      calls])))

(deftest aligns-with-the-committed-data-owner
  (let [[a calls] (authority)
        before @(:fsm-state a)]
    (with-redefs [ctrl/control-now-ms (constantly 1000)]
      (is (true? (:accepted? (ctrl/align-leadership! a))))
      (is (= [(str (peer 2))] @calls))
      (is (= before @(:fsm-state a)) "Alignment does not change the data lease")
      (is (nil? (ctrl/align-leadership! a)))
      (is (= 1 (count @calls))))))

(deftest alignment-skips-unsuitable-targets
  (doseq [[label opts]
          [[:already-aligned {:owner 1}]
           [:expired {:lease-until-ms 999}]
           [:expiring {:lease-until-ms 3100}]
           [:unmapped {:owner 4}]
           [:unreachable {:live-peers [1 3]}]
           [:removed-voter {:current-peers [1 3]}]
           [:raft-follower {:leader? false}]]]
    (testing (name label)
      (let [[a calls] (authority opts)]
        (with-redefs [ctrl/control-now-ms (constantly 1000)]
          (is (nil? (ctrl/align-leadership! a)))
          (is (empty? @calls))))))
  (let [[a calls] (authority)]
    (with-redefs [ctrl/control-now-ms (constantly 1000)]
      (is (nil? (ctrl/align-leadership!
                  (update-in a [:voters 1] assoc :promotable? false))))
      (swap! (:fsm-state a) assoc-in [:leases "other" :lease]
             {:leader-node-id 3 :lease-until-ms 20000})
      (is (nil? (ctrl/align-leadership! a)))
      (swap! (:fsm-state a) assoc :leases {})
      (is (nil? (ctrl/align-leadership! a)))
      (vreset! (:running-v a) false)
      (is (nil? (ctrl/align-leadership! a)))
      (is (empty? @calls)))))

(deftest alignment-failure-is-best-effort-and-rate-limited
  (doseq [outcome [(Status. 16 "busy") (ex-info "node stopped" {})]]
    (let [[a calls] (authority {:outcome outcome})]
      (with-redefs [ctrl/control-now-ms (constantly 1000)]
        (is (false? (:accepted? (ctrl/align-leadership! a))))
        (is (= [(str (peer 2))] @calls)))
      ;; Moving the wall clock does not bypass the retry delay.
      (with-redefs [ctrl/control-now-ms (constantly -10000)]
        (is (nil? (ctrl/align-leadership! a)))
        (is (= 1 (count @calls)))))))

(deftest renew-loop-aligns-after-publishing-state
  (doseq [pause [{} {:ha-clock-skew-paused? true}
                 {:ha-membership-mismatch? true}
                 {:ha-db-identity-mismatch? true}]]
    (let [[a calls] (authority)
          running? (AtomicBoolean. true)
          stopped (CountDownLatch. 1)
          state (merge {:ha-authority a :ha-role :follower
                        :ha-renew-loop-running? running?} pause)
          events (atom [])
          deps {:running-fn (constantly running?)
                :dbs-fn (constantly {"db" state})
                :ha-renew-step-fn (fn [_ m] (swap! events conj :renew) m)
                :replace-db-state-if-current-fn
                (fn [_ _ _ _ next-state]
                  (is (empty? @calls))
                  (swap! events conj :publish)
                  {:updated? true :state next-state})
                :ha-loop-sleep-ms-fn (constantly 1000)
                :sleep-ha-loop-fn (fn [_ _]
                                   (swap! events conj :sleep)
                                   (.set running? false))
                :log-ha-loop-crash!-fn (fn [_ _ t] (throw t))}]
      (with-redefs [ctrl/control-now-ms (constantly 1000)]
        (ha/run-ha-renew-loop deps nil "db" running? stopped))
      (is (= [:renew :publish :sleep] @events))
      (is (= (if (empty? pause) [(str (peer 2))] []) @calls))
      (is (zero? (.getCount stopped))))))
