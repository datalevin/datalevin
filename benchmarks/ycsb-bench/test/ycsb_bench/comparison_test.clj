(ns ycsb-bench.comparison-test
  (:require [clojure.test :refer [deftest is]]
            [clojure.java.io :as io]
            [datalevin.client :as client]
            [datalevin.core :as d]
            [datalevin.util :as u]
            [ycsb-bench.core :as core]
            [ycsb-bench.core-test :as shared]
            [ycsb-bench.runner :as runner]
            [ycsb-bench.server :as server]
            [ycsb-bench.store :as store]
            [ycsb-bench.workload :as w])
  (:import [datalevin.remote DatalogStore]
           [java.lang ProcessHandle]
           [java.nio.file Files]
           [java.nio.file.attribute FileAttribute]
           [java.util.concurrent Callable Executors Future TimeUnit]))

(deftest comparison-options-and-order-test
  (doseq [bad [{:repetitions 0} {:client-counts []} {:client-counts [1 1]}
               {:client-counts [1 0]} {:measurement-ms 0} {:warmup-ms -1}
               {:measurement-ms 100 :phase-timeout-ms 100}
               {:server-workers 0} {:server-mode :thread} {:datalog-handles :global}
               {:system :datalevin :api :datalog :mode :remote
                :datalog-handles :independent :threads 3 :pool-size 1}]]
    (is (thrown? clojure.lang.ExceptionInfo (runner/options bad)) (pr-str bad)))
  (let [cases (runner/cases (runner/options {:system :all :api :datalog :mode :remote
                                            :workload :a :client-counts [1 8]
                                            :datalog-handles :both :repetitions 3}))
        trials (partition 6 cases)]
    (is (= 18 (count cases)))
    (is (= [1 2 3] (mapv #(-> % first :trial) trials)))
    (is (= (map #(dissoc % :trial) (first trials))
           (reverse (map #(dissoc % :trial) (second trials)))))
    (doseq [opts cases]
      (is (= (:threads opts) (:pool-size opts)))
      (is (= 16 (:worker-threads (server/server-options "/tmp/unused" opts)))))
    (is (= 6 (count (filter #(= :postgres (:system %)) cases))))))

(deftest trial-summary-test
  (let [results (for [mode [:shared :independent], rate (if (= mode :shared) [10 20 90] [3 5])]
                  {:configuration {:datalog-handles mode}
                   :measured {:ops-per-second rate}})
        summaries (into {} (map (juxt #(get-in % [:configuration :datalog-handles]) identity)
                                (core/summarize-trials results)))]
    (is (= {:median 20 :min 10 :max 90} (:ops-per-second (:shared summaries))))
    (is (= {:median 4.0 :min 3 :max 5} (:ops-per-second (:independent summaries))))))

(deftest wal-mismatch-rejects-case-test
  (doseq [actual [{:wal? false}
                  {:wal? true :write-path-enabled? false :durability-profile :strict}
                  {:wal? true :write-path-enabled? true :durability-profile :relaxed}]]
    (let [db (reify store/Records (storage-info [_] actual))]
      (is (thrown-with-msg? clojure.lang.ExceptionInfo #"WAL settings"
                           (#'store/verify-wal! db :strict))))))

(deftest worker-handle-assignment-test
  (let [counts (repeatedly 3 #(atom 0))
        handles (mapv (fn [counter]
                        (reify store/Records
                          (read-record [_ _] (swap! counter inc) ["aaaa" "bbbb" "cccc"])))
                      counts)
        group (store/->StoreGroup handles {})
        opts (assoc shared/small-options :workload :c :distribution :uniform)]
    (runner/run-phase! group (w/keyspace 4) nil opts :measured 11)
    (is (= [4 4 3] (mapv deref counts)))
    (doseq [counter counts] (reset! counter 0))
    (let [result (runner/run-phase! group (w/keyspace 4) nil
                                    (assoc opts :measurement-ms 100) :measured 1)]
      (is (>= (:seconds result) 0.1))
      (is (= (reduce + (map deref counts)) (:operations result)))
      (is (= (:operations result) (get-in result [:by-operation :read :count])))
      (is (every? pos? (map deref counts))))))

(deftest timed-inserts-and-cdf-growth-test
  (let [cdf (atom (w/zipf-cdf 4))
        grown (w/grow-cdf! cdf 19)]
    (is (= (vec (w/zipf-cdf 19)) (vec (take 19 grown)))))
  ;; The operation bounds are deliberately tiny. Timed inserts must grow
  ;; past them and still validate the complete committed keyspace.
  (let [result (runner/run-case!
                 (assoc shared/small-options :system :sqlite :api :datalog :mode :embedded
                        :workload :d :records 2 :warmup 0 :ops 1
                        :warmup-ms 100 :measurement-ms 200))
        inserts (+ (get-in result [:warmup :by-operation :insert :count] 0)
                   (get-in result [:measured :by-operation :insert :count] 0))]
    (is (>= (get-in result [:warmup :seconds]) 0.1))
    (is (>= (get-in result [:measured :seconds]) 0.2))
    (is (> inserts 2))
    (is (= (+ 2 inserts) (get-in result [:validation :records])))))

(deftest separate-process-independent-datalog-test
  (let [pid (atom nil)
        handles (atom [])
        opts (assoc shared/small-options :api :datalog :mode :remote
                    :server-mode :process :server-heap-mb 512 :datalog-handles :independent)]
    (store/with-store
      opts
      (fn [db]
        (let [info (store/storage-info db)
              stores (mapv #(store/for-worker db %) (range 3))
              client-ids (mapv #(client/get-id (.-client ^DatalogStore (d/datalog-kv (:conn %)))) stores)
              executor (Executors/newFixedThreadPool 3)]
          (reset! handles stores)
          (reset! pid (get-in info [:server :pid]))
          (is (not= (.pid (ProcessHandle/current)) @pid))
          (is (= :separate-process (get-in info [:server :placement])))
          (is (= :connection-thread (get-in info [:server :configuration :request-execution])))
          (is (= :connection-thread (get-in info [:server :configuration :transaction-execution])))
          (is (not-any? #{:routing :transactions} (get-in info [:server :execution-keys])))
          (doseq [[path origin] (get-in info [:server :source-resources])]
            (is (= (str (io/resource path)) origin) path))
          (is (= {:handles :independent :handle-count 3 :read-connections 3
                  :dedicated-transaction-connections 0} (:client-topology info)))
          (is (= 3 (count (distinct client-ids))))
          (is (= {:wal? true :write-path-enabled? true :durability-profile :strict}
                 (select-keys info [:wal? :write-path-enabled? :durability-profile])))
          (store/put-records! db [[0 ["aaaa" "bbbb" "cccc"]]])
          (try
            (let [tasks (.invokeAll executor
                                   (mapv (fn [handle]
                                           ^Callable (fn [] (dotimes [_ 20] (store/modify-field! handle 0 0))))
                                         stores))]
              (doseq [^Future task tasks] (.get task)))
            (doseq [handle stores]
              (is (= ["iaaa" "bbbb" "cccc"] (store/read-record handle 0))
                  "Independent clients must not lose atomic updates"))
            (finally
              (.shutdownNow executor)
              (.awaitTermination executor 30 TimeUnit/SECONDS))))
        {}))
    (is (every? #(d/closed? (:conn %)) @handles))
    (is (not (.isPresent (ProcessHandle/of @pid))))))

(deftest server-startup-timeout-cleans-child-test
  (let [root (str (Files/createTempDirectory "ycsb-process-test-" (make-array FileAttribute 0)))
        stopped (atom nil)
        stop! @#'server/stop-process!]
    (try
      (with-redefs-fn
        {#'server/stop-process! (fn [process] (reset! stopped process) (stop! process))}
        #(is (thrown? clojure.lang.ExceptionInfo
                      (server/start! root (assoc server/defaults :server-startup-timeout-ms 1
                                                :server-heap-mb 512)))))
      (is (some? @stopped))
      (is (false? (.isAlive ^Process @stopped)))
      (finally (u/delete-files root)))))
