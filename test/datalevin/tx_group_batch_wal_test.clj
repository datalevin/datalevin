(ns datalevin.tx-group-batch-wal-test
  "Contract tests for the WAL-only branch adapter.

  Injected append/complete fns stand in for `txlog`, so body gathering, dispatch
  order and append-token hand-off are checked without a WAL runtime or disk."
  (:require [clojure.test :refer [deftest is testing]]
            [datalevin.tx-group.batch :as batch]
            [datalevin.tx-group.batch.charge :as charge]
            [datalevin.tx-group.batch.executor :as executor]
            [datalevin.tx-group.batch.wal :as wal]
            [datalevin.tx-group.phase :as phase])
  (:import [java.util.concurrent CountDownLatch TimeUnit]))

(def ^:private limits
  {:wal-pending-max-requests 64
   :wal-pending-max-bytes 1048576
   :write-batch-size 8
   :write-batch-max-bytes 12288
   :wal-rmw-max-bytes 4096})

(defn- native-branch
  "Echo each request's `:result`; block the first batch so followers accumulate."
  [^CountDownLatch entered ^CountDownLatch release]
  (let [first? (atom true)]
    (reify executor/INativeBranch
      (apply-rows! [_ batch before-commit]
        (before-commit)
        (when (compare-and-set! first? true false)
          (.countDown entered)
          (.await release 5 TimeUnit/SECONDS))
        (let [n (batch/batch-count batch)
              values (object-array n)]
          (dotimes [i n]
            (aset values i (:result (batch/data (batch/batch-at batch i)))))
          values)))))

(deftest wal-adapter-gathers-sealed-bodies-in-dispatch-order
  (let [appends (atom [])
        completes (atom [])
        wal-branch (wal/branch
                    :runtime
                    {:append-fn (fn [_state lsn bodies deadline b]
                                  (is (= (batch/batch-cutoff b) deadline))
                                  (swap! appends conj [lsn (vec bodies)])
                                  :token)
                     :complete-fn (fn [_state token deadline]
                                    (swap! completes conj [token deadline])
                                    true)})
        entered (CountDownLatch. 1)
        release (CountDownLatch. 1)
        lsn (atom 0)
        c (batch/create
           (executor/create wal-branch (native-branch entered release)
                            #(swap! lsn inc)
                            {:schedule-fn (constantly :parallel)})
           {:limits (charge/resolve-limits limits)})
        leader (future (batch/submit! c {:allowance 1024
                                         :data {:wal-body :body-a :result :a}}))]
    (try
      (is (.await entered 5 TimeUnit/SECONDS))
      (let [b (future (batch/submit! c {:allowance 1024
                                        :data {:wal-body :body-b :result :b}}))
            d (future (batch/submit! c {:allowance 1024
                                        :data {:wal-body :body-c :result :c}}))]
        (.countDown release)
        (is (= :a (deref leader 5000 ::timeout)))
        (is (= :b (deref b 5000 ::timeout)))
        (is (= :c (deref d 5000 ::timeout))))
      (finally
        (.countDown release)))
    (testing "one group record per sealed batch, bodies in dispatch order"
      (is (= [[1 [:body-a]] [2 [:body-b :body-c]]] @appends)))
    (testing "each append token completes its own batch's policy"
      (is (= 2 (count @completes)))
      (is (every? #(= :token (first %)) @completes)))))

(deftest wal-adapter-delegates-maintenance-to-txlog
  (let [deadline (atom 7)
        serviced (atom nil)
        wal-branch (wal/branch
                    :runtime
                    {:maintenance-deadline-fn (fn [_state] @deadline)
                     :service-maintenance-fn (fn [_state deadline-ns]
                                               (reset! serviced deadline-ns)
                                               :ok)})]
    (is (= 7 (executor/maintenance-deadline-ns wal-branch)))
    (is (= :ok (executor/service-maintenance! wal-branch 5)))
    (is (= 5 @serviced))))

(deftest wal-adapter-uses-the-sealed-array-after-ordered-preparation
  (let [sealed (atom nil)
        uninstall (phase/observe!
                   (fn [event b]
                     (when (= :batch-sealed event)
                       (let [bodies (batch/wal-bodies b)]
                         (is (= [:before] (vec bodies)))
                         (reset! sealed bodies)))))
        wal-branch (wal/branch
                    :runtime
                    {:append-fn (fn [_ _ bodies _ _]
                                  (is (identical? @sealed bodies)
                                      "dispatch does not rebuild the body carrier")
                                  (is (= [:after] (vec bodies)))
                                  :token)
                     :complete-fn (fn [_ _ _] true)})
        native (reify executor/INativeBranch
                 (apply-rows! [_ _ gate] (gate) (object-array [:ok])))
        c (batch/create
           (executor/create
            wal-branch native (constantly 1)
            {:schedule-fn (constantly :parallel)
             :prepare-batch! (fn [b]
                               (let [d (batch/batch-at b 0)]
                                 (batch/set-data! d {:wal-body :after}))
                               nil)})
           {:limits (charge/resolve-limits limits)})]
    (try
      (is (= :ok (batch/submit! c {:allowance 1024 :data {:wal-body :before}})))
      (is (= "[Ljava.lang.Object;" (.getName (class @sealed))))
      (finally (uninstall)))))
