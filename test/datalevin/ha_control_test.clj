(ns datalevin.ha-control-test
  (:require
   [clojure.test :refer [deftest is testing]]
   [datalevin.ha.control :as ctrl]
   [datalevin.util :as u])
  (:import
   [com.alipay.sofa.jraft Node]
   [java.io File]
   [java.net ServerSocket]
   [java.util UUID]))

(defn- started-authority
  [group-id]
  (doto (ctrl/new-in-memory-authority {:group-id group-id})
    (ctrl/start-authority!)))

(defn- unique-group-id
  []
  (str "ha-test-" (UUID/randomUUID)))

(defn- reserve-port
  []
  (with-open [s (ServerSocket. 0)]
    (.getLocalPort s)))

(defn- wait-until
  [f timeout-ms]
  (let [deadline (+ (System/currentTimeMillis) timeout-ms)]
    (loop []
      (if (try
            (boolean (f))
            (catch Exception _ false))
        true
        (if (< (System/currentTimeMillis) deadline)
          (do (Thread/sleep 25)
              (recur))
          false)))))

(defn- started-sofa-authority
  ([group-id local-peer-id voters]
   (started-sofa-authority group-id local-peer-id voters {}))
  ([group-id local-peer-id voters opts]
   (doto (ctrl/new-sofa-jraft-authority
          (merge {:group-id group-id
                  :local-peer-id local-peer-id
                  :voters voters
                  :rpc-timeout-ms 600
                  :election-timeout-ms 1200
                  :operation-timeout-ms 12000}
                 opts))
     (ctrl/start-authority!))))

(defn- authority-leader?
  [authority]
  (when-let [^Node node @(:node-v authority)]
    (.isLeader node)))

(def ^:private db-identity "7a9f1f8d-cf5a-4fd6-a5a0-6db4a74a6f6f")

(deftest canonical-key-derivation-test
  (is (= "/ha-prod/ha/v2/db/7a9f1f8d-cf5a-4fd6-a5a0-6db4a74a6f6f/lease"
         (ctrl/lease-key "ha-prod" db-identity)))
  (is (= "/ha-prod/ha/v2/membership-hash"
         (ctrl/membership-hash-key "ha-prod"))))

(deftest in-memory-authority-cas-and-renew-test
  (let [authority (started-authority (unique-group-id))
        req       {:db-identity db-identity
                   :leader-node-id 2
                   :leader-endpoint "10.0.0.12:8898"
                   :lease-renew-ms 5000
                   :lease-timeout-ms 15000
                   :leader-last-applied-lsn 42
                   :now-ms 1000
                   :observed-version 0
                   :observed-lease nil}]
    (testing "single winner for same observed version"
      (let [winner (ctrl/try-acquire-lease authority req)
            loser  (ctrl/try-acquire-lease authority req)]
        (is (:ok? winner))
        (is (= 1 (:term winner)))
        (is (= :cas-mismatch (:reason loser)))))

    (testing "renew is owner+term guarded and keeps term stable"
      (let [{:keys [lease version]} (ctrl/read-lease authority db-identity)
            renew-ok (ctrl/renew-lease authority
                                        {:db-identity db-identity
                                         :leader-node-id 2
                                         :leader-endpoint "10.0.0.12:8898"
                                         :term (:term lease)
                                         :lease-renew-ms 5000
                                         :lease-timeout-ms 15000
                                         :leader-last-applied-lsn 43
                                         :now-ms 2000})
            renew-bad (ctrl/renew-lease authority
                                         {:db-identity db-identity
                                          :leader-node-id 2
                                          :leader-endpoint "10.0.0.12:8898"
                                          :term (unchecked-inc (long (:term lease)))
                                          :lease-renew-ms 5000
                                          :lease-timeout-ms 15000
                                          :leader-last-applied-lsn 44
                                          :now-ms 3000})]
        (is (= 1 (:term renew-ok)))
        (is (= (unchecked-inc (long version)) (:version renew-ok)))
        (is (= :term-mismatch (:reason renew-bad)))))

    (testing "acquisition requires expiry and then increments term"
      (let [{:keys [lease version]} (ctrl/read-lease authority db-identity)
            early (ctrl/try-acquire-lease authority
                                          {:db-identity db-identity
                                           :leader-node-id 3
                                           :leader-endpoint "10.0.0.13:8898"
                                           :lease-renew-ms 5000
                                           :lease-timeout-ms 15000
                                           :leader-last-applied-lsn 45
                                           :now-ms 16000
                                           :observed-version version
                                           :observed-lease lease})
            late  (ctrl/try-acquire-lease authority
                                          {:db-identity db-identity
                                           :leader-node-id 3
                                           :leader-endpoint "10.0.0.13:8898"
                                           :lease-renew-ms 5000
                                           :lease-timeout-ms 15000
                                           :leader-last-applied-lsn 46
                                           :now-ms 18000
                                           :observed-version version
                                           :observed-lease lease})]
        (is (= :lease-not-expired (:reason early)))
        (is (:ok? late))
        (is (= 2 (:term late)))))))

(deftest membership-hash-cas-init-test
  (let [authority (started-authority (unique-group-id))
        first-init (ctrl/init-membership-hash! authority "abc123")
        second-init (ctrl/init-membership-hash! authority "abc123")
        mismatch (ctrl/init-membership-hash! authority "def456")]
    (is (:ok? first-init))
    (is (:initialized? first-init))
    (is (:ok? second-init))
    (is (false? (:initialized? second-init)))
    (is (= :membership-hash-mismatch (:reason mismatch)))))

(deftest in-memory-authority-replace-voters-test
  (let [group-id     (unique-group-id)
        initial      [{:peer-id "127.0.0.1:7801"}
                      {:peer-id "127.0.0.1:7802"}
                      {:peer-id "127.0.0.1:7803"}]
        replacement  [{:peer-id "127.0.0.1:8801"}
                      {:peer-id "127.0.0.1:8802"}
                      {:peer-id "127.0.0.1:8803"}]
        authority    (doto (ctrl/new-in-memory-authority
                            {:group-id group-id
                             :voters initial})
                       (ctrl/start-authority!))]
    (is (= ["127.0.0.1:7801" "127.0.0.1:7802" "127.0.0.1:7803"]
           (ctrl/read-voters authority)))
    (let [replaced (ctrl/replace-voters! authority replacement)]
      (is (:ok? replaced))
      (is (= ["127.0.0.1:8801" "127.0.0.1:8802" "127.0.0.1:8803"]
             (:voters replaced)))
      (is (= ["127.0.0.1:8801" "127.0.0.1:8802" "127.0.0.1:8803"]
             (ctrl/read-voters authority))))))

(deftest in-memory-authority-shares-state-by-group-id-test
  (let [group-id   (unique-group-id)
        authority-a (started-authority group-id)
        authority-b (started-authority group-id)
        win         (ctrl/try-acquire-lease authority-a
                                            {:db-identity db-identity
                                             :leader-node-id 2
                                             :leader-endpoint "10.0.0.12:8898"
                                             :lease-renew-ms 5000
                                             :lease-timeout-ms 15000
                                             :leader-last-applied-lsn 42
                                             :now-ms 1000
                                             :observed-version 0
                                             :observed-lease nil})
        seen        (ctrl/read-lease authority-b db-identity)]
    (is (:ok? win))
    (is (= (:term win) (get-in seen [:lease :term])))
    (is (= (:version win) (:version seen)))))

(deftest membership-hash-is-shared-by-group-id-test
  (let [group-id    (unique-group-id)
        authority-a (started-authority group-id)
        authority-b (started-authority group-id)
        init-a      (ctrl/init-membership-hash! authority-a "abc123")
        mismatch-b  (ctrl/init-membership-hash! authority-b "def456")]
    (is (:ok? init-a))
    (is (= :membership-hash-mismatch (:reason mismatch-b)))))

(deftest sofa-jraft-authority-replicates-and-fails-over-test
  (let [group-id (unique-group-id)
        peers    (repeatedly 3 #(str "127.0.0.1:" (reserve-port)))
        voters   (mapv (fn [idx peer-id]
                         {:peer-id peer-id
                          :ha-node-id (inc idx)
                          :promotable? true})
                       (range 3) peers)
        authorities (mapv #(started-sofa-authority group-id % voters) peers)
        authority-a (nth authorities 0)
        authority-b (nth authorities 1)
        authority-c (nth authorities 2)]
    (try
      (testing "membership hash is replicated to all peers"
        (is (:ok? (ctrl/init-membership-hash! authority-a "abc123")))
        (let [again (ctrl/init-membership-hash! authority-b "abc123")]
          (is (:ok? again))
          (is (false? (:initialized? again)))))

      (testing "lease writes replicate and survive raft-leader loss"
        (let [initial (ctrl/try-acquire-lease
                       authority-c
                       {:db-identity db-identity
                        :leader-node-id 3
                        :leader-endpoint "10.0.0.13:8898"
                        :lease-renew-ms 1000
                        :lease-timeout-ms 2000
                        :leader-last-applied-lsn 7
                        :now-ms 1000
                        :observed-version 0
                        :observed-lease nil})]
          (is (:ok? initial))

          (let [raft-leader (first (filter authority-leader? authorities))]
            (is (some? raft-leader))
            (ctrl/stop-authority! raft-leader)
            (is (wait-until #(some authority-leader?
                                   [authority-a authority-b authority-c])
                            10000))
            (let [live-authorities (remove #(identical? raft-leader %)
                                           authorities)
                  candidate       (first live-authorities)
                  reacquired      (ctrl/try-acquire-lease
                                   candidate
                                   {:db-identity db-identity
                                    :leader-node-id 2
                                    :leader-endpoint "10.0.0.12:8898"
                                    :lease-renew-ms 1000
                                    :lease-timeout-ms 2000
                                    :leader-last-applied-lsn 8
                                    :now-ms 5000
                                    :observed-version (:version initial)
                                    :observed-lease (:lease initial)})]
              (is (:ok? reacquired))
              (is (= 2 (:term reacquired)))))))
      (finally
        (doseq [a authorities]
          (try
            (ctrl/stop-authority! a)
            (catch Exception _)))))))

(deftest sofa-jraft-linearizable-read-requires-quorum-test
  (let [group-id (unique-group-id)
        peers    (repeatedly 3 #(str "127.0.0.1:" (reserve-port)))
        voters   (mapv (fn [idx peer-id]
                         {:peer-id peer-id
                          :ha-node-id (inc idx)
                          :promotable? true})
                       (range 3) peers)
        authorities (mapv #(started-sofa-authority
                            group-id
                            %
                            voters
                            {:operation-timeout-ms 1200})
                          peers)]
    (try
      (is (wait-until #(some authority-leader? authorities) 10000))
      (is (:ok? (ctrl/init-membership-hash! (first authorities) "abc123")))
      (let [raft-leader (first (filter authority-leader? authorities))
            quorum-loss-peers (remove #(identical? raft-leader %) authorities)]
        (is (some? raft-leader))
        (doseq [a quorum-loss-peers]
          (ctrl/stop-authority! a))
        (let [ex (try
                   (ctrl/read-membership-hash raft-leader)
                   nil
                   (catch clojure.lang.ExceptionInfo e
                     e))]
          (is (instance? clojure.lang.ExceptionInfo ex))
          (is (contains? #{:ha/control-timeout
                           :ha/control-read-failed
                           :ha/control-node-unavailable}
                         (:error (ex-data ex))))))
      (finally
        (doseq [a authorities]
          (try
            (ctrl/stop-authority! a)
            (catch Exception _)))))))

(deftest sofa-jraft-witness-voter-topology-retains-quorum-test
  (let [group-id (unique-group-id)
        peers    (repeatedly 3 #(str "127.0.0.1:" (reserve-port)))
        authority-a-peer (nth peers 0)
        authority-b-peer (nth peers 1)
        witness-peer     (nth peers 2)
        voters   [{:peer-id authority-a-peer
                   :ha-node-id 1
                   :promotable? true}
                  {:peer-id authority-b-peer
                   :ha-node-id 2
                   :promotable? true}
                  {:peer-id witness-peer
                   :promotable? false}]
        authorities [ (started-sofa-authority group-id authority-a-peer voters)
                      (started-sofa-authority group-id authority-b-peer voters)
                      (started-sofa-authority group-id witness-peer voters)]
        authority-a (nth authorities 0)
        authority-b (nth authorities 1)
        witness     (nth authorities 2)]
    (try
      (is (wait-until #(some authority-leader? authorities) 10000))
      (is (:ok? (ctrl/init-membership-hash! authority-a "abc123")))
      (let [acquire (ctrl/try-acquire-lease
                      authority-a
                      {:db-identity db-identity
                       :leader-node-id 1
                       :leader-endpoint "10.0.0.11:8898"
                       :lease-renew-ms 1000
                       :lease-timeout-ms 3000
                       :leader-last-applied-lsn 7
                       :now-ms 1000
                       :observed-version 0
                       :observed-lease nil})]
        (is (:ok? acquire))
        (ctrl/stop-authority! authority-b)
        (is (wait-until #(some authority-leader? [authority-a witness]) 10000))
        (is (wait-until
              #(try
                 (:ok? (ctrl/renew-lease
                         authority-a
                         {:db-identity db-identity
                          :leader-node-id 1
                          :leader-endpoint "10.0.0.11:8898"
                          :term (:term (:lease acquire))
                          :lease-renew-ms 1000
                          :lease-timeout-ms 3000
                          :leader-last-applied-lsn 8
                          :now-ms 1500}))
                 (catch Exception _ false))
              10000)))
      (finally
        (doseq [a authorities]
          (try
            (ctrl/stop-authority! a)
            (catch Exception _)))))))

(deftest sofa-jraft-replace-voters-forwards-through-leader-test
  (let [group-id (unique-group-id)
        peers    (repeatedly 3 #(str "127.0.0.1:" (reserve-port)))
        voters   (mapv (fn [idx peer-id]
                         {:peer-id peer-id
                          :ha-node-id (inc idx)
                          :promotable? true})
                       (range 3) peers)
        authorities (mapv #(started-sofa-authority group-id % voters) peers)
        expected-voters (vec (sort peers))]
    (try
      (is (wait-until #(some authority-leader? authorities) 10000))
      (let [follower (first (remove authority-leader? authorities))
            replaced (ctrl/replace-voters! follower (vec (reverse voters)))]
        (is (some? follower))
        (is (:ok? replaced))
        (is (= expected-voters (:voters replaced))))
      (finally
        (doseq [a authorities]
          (try
            (ctrl/stop-authority! a)
            (catch Exception _)))))))

(deftest sofa-jraft-authority-recovers-state-after-restart-test
  (let [group-id (unique-group-id)
        peer     (str "127.0.0.1:" (reserve-port))
        voters   [{:peer-id peer
                   :ha-node-id 1
                   :promotable? true}]
        raft-dir (u/tmp-dir (str "datalevin-ha-control-restart/"
                                 (UUID/randomUUID)))
        authority-opts {:raft-dir raft-dir
                        :operation-timeout-ms 8000}]
    (u/create-dirs raft-dir)
    (let [authority-1 (started-sofa-authority
                       group-id peer voters authority-opts)]
      (try
        (is (wait-until #(authority-leader? authority-1) 10000))
        (is (:ok? (ctrl/init-membership-hash! authority-1 "abc123")))
        (let [acquire (ctrl/try-acquire-lease
                       authority-1
                       {:db-identity db-identity
                        :leader-node-id 1
                        :leader-endpoint "10.0.0.11:8898"
                        :lease-renew-ms 1000
                        :lease-timeout-ms 3000
                        :leader-last-applied-lsn 9
                        :now-ms 1000
                        :observed-version 0
                        :observed-lease nil})]
          (is (:ok? acquire))
          (ctrl/stop-authority! authority-1)
          (let [^File log-dir (File. ^String raft-dir "log")]
            (is (.isFile (File. log-dir "data.mdb"))))
          (let [authority-2 (started-sofa-authority
                             group-id peer voters authority-opts)]
            (try
              (is (wait-until #(authority-leader? authority-2) 10000))
              (is (= "abc123" (ctrl/read-membership-hash authority-2)))
              (let [{:keys [lease version]} (ctrl/read-lease authority-2 db-identity)]
                (is (= 1 (:leader-node-id lease)))
                (is (= 1 (:term lease)))
                (is (pos? (long version))))
              (is (= [peer] (ctrl/read-voters authority-2)))
              (finally
                (try
                  (ctrl/stop-authority! authority-2)
                  (catch Exception _))))))
        (finally
          (try
            (ctrl/stop-authority! authority-1)
            (catch Exception _))
          (u/delete-files raft-dir))))))
