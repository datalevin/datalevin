(ns ycsb-bench.core-test
  (:require [clojure.test :refer [deftest is testing]]
            [datalevin.core :as d]
            [datalevin.pull-api :as pull]
            [datalevin-bench.host :as host]
            [ycsb-bench.core :as core]
            [ycsb-bench.runner :as runner]
            [ycsb-bench.sql :as sql]
            [ycsb-bench.store :as store]
            [ycsb-bench.workload :as w])
  (:import [java.util Random]
           [java.util.concurrent Callable ExecutionException Executors Future TimeUnit]))

(def small-options
  (runner/options {:records 4 :ops 11 :warmup 7 :threads 3
                   :server-mode :in-process
                   :field-count 3 :field-length 4 :scan-length 3}))

(deftest benchmark-resumes-media-test
  (doseq [fail? [false true]]
    (testing (if fail? "case failure" "successful comparison")
      (let [events (atom [])
            paused [123 456]
            failure (ex-info "Injected benchmark failure" {})
            opts (assoc small-options :system :all :api :datalog
                        :mode :remote :workload :c)
            result {:measured {:ops-per-second 1.0 :latency-us {:p99 1.0}}
                    :validation {:records 4}}]
        (with-redefs [sql/check-postgres! (fn [_] (swap! events conj :preflight))
                      host/pause! (fn [] (swap! events conj :pause) paused)
                      host/resume! (fn [pids] (swap! events conj [:resume pids]))
                      runner/run-case!
                      (fn [{:keys [system]}]
                        (swap! events conj [:run system])
                        (when (and fail? (= system :postgres)) (throw failure))
                        result)]
          (binding [*out* (java.io.StringWriter.)]
            (if fail?
              (is (identical? failure
                              (try (core/run-benchmark opts)
                                   (catch Exception e e))))
              (is (= [result result] (:results (core/run-benchmark opts)))))))
        (is (= [:preflight :pause [:run :datalevin] [:run :postgres]
                [:resume paused]]
               @events))))))

(deftest deterministic-records-test
  (let [values (w/initial-values 17 4 small-options)]
    (is (= values (w/initial-values 17 4 small-options)))
    (is (not= values (w/initial-values 18 4 small-options)))
    (is (= [4 4 4] (mapv count values)))
    (is (every? #(re-matches #"[a-z]+" %) values))
    (is (= "aaaa" (w/modified-value "zaaa")))))

(deftest committed-keyspace-test
  (let [space (w/keyspace 3)
        first-id (w/reserve-key! space)
        second-id (w/reserve-key! space)]
    (is (= [3 4] [first-id second-id]))
    (w/acknowledge-key! space second-id)
    (is (= 3 (:visible @space)))
    (w/acknowledge-key! space first-id)
    (is (= {:next 5 :visible 5 :pending #{}} @space))))

(deftest request-generators-test
  (let [cdf (w/zipf-cdf 100)]
    (doseq [distribution [:uniform :zipfian :latest]
            n [1 7 100]]
      (let [rng (Random. 17)
            keys (repeatedly 2000 #(w/choose-key rng distribution cdf n))]
        (is (every? #(<= 0 % (dec n)) keys))))
    (let [rng (Random. 17)
          keys (repeatedly 10000 #(w/choose-key rng :zipfian cdf 100))]
      (is (> (count (filter #(< % 10) keys))
             (* 5 (count (filter #(>= % 90) keys)))))))
  (doseq [[workload {:keys [mix]}] w/workloads]
    (let [rng (Random. 42)
          counts (frequencies (repeatedly 10000 #(w/choose-operation rng mix)))]
      (is (= (set (map first mix)) (set (keys counts))) (str workload))
      (doseq [[operation weight] mix]
        (is (< (abs (- (get counts operation) (* 100 weight))) 250))))))

(deftest malformed-operation-mix-test
  (let [rng (proxy [Random] [] (nextInt [_bound] 99))]
    (is (= :update (w/choose-operation rng [[:read 50] [:update 50]])))
    (doseq [[mix remaining-draw] [[nil 99]
                                 [[] 99]
                                 [[[:read]] 99]
                                 [[[:read nil]] 99]
                                 [[[:read 50] [:update 49]] 0]]]
      (let [failure (try (w/choose-operation rng mix)
                         (catch Exception e e))]
        (is (instance? clojure.lang.ExceptionInfo failure))
        (is (= {:mix mix :remaining-draw remaining-draw} (ex-data failure)))
        (when (instance? clojure.lang.ExceptionInfo failure)
          (is (re-find #"Invalid operation mix" (ex-message failure))))))))

(deftest invalid-options-test
  (doseq [bad [{:ops 0} {:warmup -1} {:threads 0} {:pool-size -1}
               {:records nil} {:field-length 0} {:seed 1.5}
               {:distribution :random} {:durability :off}
               {:api :sql} {:records Integer/MAX_VALUE} {:sql-indexes :primary}]]
    (is (thrown? clojure.lang.ExceptionInfo (runner/options bad)) (str bad)))
  (is (= 3 (:pool-size (runner/options {:threads 3}))))
  (is (= 0 (:warmup (runner/options {:warmup 0})))))

(deftest latency-summary-test
  (is (= {:mean 2.5 :p50 2.0 :p95 4.0 :p99 4.0 :max 4.0}
         (runner/latency-summary! (long-array [4000 1000 3000 2000]))))
  (is (nil? (runner/latency-summary! (long-array 0)))))

(deftest record-byte-length-validation-test
  (doseq [value ["aaaa" "a\u0000\u007fz"]]
    (is (nil? (runner/validate-record! [value "bbbb" "cccc"] small-options))))
  ;; Each string occupies four UTF-16 code units, just like a valid field.
  (doseq [value ["éaaa" "aaaé" "ab😀" (str "aaa" (char 0xd800))]]
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"Missing or malformed record"
                         (runner/validate-record! [value "bbbb" "cccc"] small-options)))))

(defn- concurrent-modifications! [db]
  (let [executor (Executors/newFixedThreadPool 2)]
    (try
      (let [tasks (.invokeAll executor
                              (vec (repeat 2
                                           ^Callable
                                           (fn []
                                             (dotimes [_ 10]
                                               (store/modify-field! db 0 0))))))]
        (doseq [^Future task tasks] (.get task)))
      (finally
        (.shutdownNow executor)
        (.awaitTermination executor 30 TimeUnit/SECONDS)))))

(defn check-adapter! [db]
  (let [values ["aaaa" "bbbb" "cccc"]]
    (store/put-records! db (mapv #(vector % values) [2 0 1]))
    (is (= 3 (store/record-count db)))
    (is (= values (store/read-record db 1)))
    (store/update-field! db 0 1 "zzzz")
    (is (= ["aaaa" "zzzz" "cccc"] (store/read-record db 0)))
    (concurrent-modifications! db)
    (is (= ["uaaa" "zzzz" "cccc"] (store/read-record db 0))
        "Atomic RMW must not lose concurrent modifications")
    (is (= [[0 ["uaaa" "zzzz" "cccc"]]] (store/scan-records db 0 1)))
    (is (empty? (store/scan-records db 0 0)))
    (is (= [[1 values]] (store/scan-records db 1 1)))
    (is (= [[1 values] [2 values]] (store/scan-records db 1 10)))
    (store/put-records! db [[3 values]])
    (is (= 4 (store/record-count db)))
    (is (= [[2 values] [3 values]] (store/scan-records db 2 2)))
    (store/update-field! db 1 2 "zzzz")
    (is (= [[1 ["aaaa" "bbbb" "zzzz"]]] (store/scan-records db 1 1)))
    (is (empty? (store/scan-records db 4 1)))
    ;; Callers continue checking this fixture after the shared adapter checks.
    (store/update-field! db 1 2 "cccc"))
  {})

(defn- check-kv-record-layout! [db]
  (let [handle (:handle (store/for-worker db 0))]
    (is (= {:layout :record-value :key-type :id :value-type :data}
           (select-keys (store/storage-info db) [:layout :key-type :value-type])))
    (is (= 4 (d/entries handle "records")))
    (is (= [0 1 2 3] (mapv first (d/get-range handle "records" [:all] :id :data))))
    (is (= ["uaaa" "zzzz" "cccc"] (d/get-value handle "records" 0 :id :data)))
    (store/put-records! db [[7 ["same" "same" "same"]]])
    (is (= [[7 ["same" "same" "same"]]] (store/scan-records db 4 10)))
    (let [executor (Executors/newFixedThreadPool 2)]
      (try
        (let [tasks (.invokeAll
                      executor
                      (mapv (fn [field]
                              ^Callable
                              (fn []
                                (dotimes [step 20]
                                  (store/update-field! db 7 field
                                                       (format "%04d" (+ (* field 100) step))))))
                            [0 1]))]
          (doseq [^Future task tasks] (.get task)))
        (finally
          (.shutdownNow executor)
          (.awaitTermination executor 30 TimeUnit/SECONDS))))
    (is (= ["0019" "0119" "same"] (store/read-record db 7))
        "Replacing a record must preserve concurrent writes to other fields")
    (is (= ["0019" "0119" "same"] (d/get-value handle "records" 7 :id :data)))
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"Missing KV record"
                         (store/update-field! db 99 0 "oops")))
    (is (= 5 (store/record-count db) (d/entries handle "records")))))

(deftest adapter-semantics-test
  (doseq [api [:kv :datalog], mode [:embedded :remote]]
    (testing (str api " " mode)
      (store/with-store
        (assoc small-options :api api :mode mode)
        (fn [db]
          (is (= (if (= mode :remote) :server-function :transaction-function)
                 (:rmw-execution (store/storage-info db))))
          (when (= api :datalog)
            (is (= 0 (:cache-limit (store/storage-info db))))
            (is (= :slice (:scan-api (store/storage-info db))))
            (is (= :db/id (:record-key (store/storage-info db)))))
          (check-adapter! db)
          (when (= api :kv)
            (check-kv-record-layout! db))
          (when (= api :datalog)
            (let [conn (:conn (store/for-worker db 0))]
              (is (= {:db/id 0 :ycsb/field0 "uaaa" :ycsb/field1 "zzzz"
                      :ycsb/field2 "cccc"}
                     (d/pull @conn '[*] 0))
                  "Record keys are explicit entity IDs, including zero")
              (is (= #{[0] [1] [2] [3]}
                     (d/q '[:find ?e :where [?e :ycsb/field0]] @conn)))
              (is (not (contains? (d/schema conn) :ycsb/id)))
              (store/put-records! db [[7 ["dddd" "eeee" "ffff"]]])
              (is (= [[7 ["dddd" "eeee" "ffff"]]] (store/scan-records db 4 10)))
              (is (empty? (store/scan-records db 4 3))
                  "The upper entity bound is exclusive, including across gaps")
              (is (= [[7 ["dddd" "eeee" "ffff"]]] (store/scan-records db 7 1)))
              (is (= 5 (store/record-count db)))))
          {})))))

(deftest rmw-reader-reuse-test
  (doseq [mode [:embedded :remote]]
    (testing (str mode)
      (store/with-store
        (assoc small-options :api :datalog :mode mode)
        (fn [db]
          (let [record (store/for-worker db 0)
                attributes (:attributes record)
                prepare d/prepare-pull
                parse pull/parse-opts
                preparations (atom 0)
                parses (atom 0)]
            (store/put-records! db [[1 ["aaaa" "bbbb" "cccc"]]])
            (store/modify-field! db 1 0)
            (with-redefs [d/prepare-pull
                          (fn [& args]
                            (swap! preparations inc)
                            (apply prepare args))
                          pull/parse-opts
                          (fn [view pattern opts]
                            (when (= pattern attributes) (swap! parses inc))
                            (parse view pattern opts))]
              (dotimes [_ 5] (store/modify-field! db 1 0)))
            (is (zero? @preparations)
                "Successive transactions reuse the preparation wrapper")
            (is (zero? @parses)
                "Successive transaction views retain the parsed RMW pattern")
            (is (= ["gaaa" "bbbb" "cccc"] (store/read-record db 1))
                "Every reused read sees the current transaction's value"))
          {})))))

(deftest rmw-reader-reuse-after-rollback-test
  (store/with-store
    (assoc small-options :api :datalog :mode :embedded)
    (fn [db]
      (let [{:keys [conn attributes]} (store/for-worker db 0)
            reader (atom nil)]
        (store/put-records! db [[1 ["aaaa" "bbbb" "cccc"]]])
        (d/with-transaction [tx conn]
          (reset! reader (w/rmw-reader @tx attributes))
          (d/transact! tx [[:db/add 1 :ycsb/field0 "xxxx"]])
          (is (= "xxxx" (:ycsb/field0 (d/execute-prepared @reader @tx 1))))
          (d/abort-transact tx))
        (d/with-transaction [tx conn]
          (is (identical? @reader (w/rmw-reader @tx attributes)))
          (is (= "aaaa" (:ycsb/field0 (d/execute-prepared @reader @tx 1))))
          (d/transact! tx [[:db/add 1 :ycsb/field0 "yyyy"]])
          (is (= "yyyy" (:ycsb/field0 (d/execute-prepared @reader @tx 1)))))
        (is (= ["yyyy" "bbbb" "cccc"] (store/read-record db 1))))
      {})))

(deftest datalog-scan-field-order-test
  (doseq [mode [:embedded :remote]]
    (testing (str mode)
      (store/with-store
        (assoc small-options :api :datalog :mode mode :field-count 12)
        (fn [db]
          (let [rows (mapv (fn [id]
                             [id (mapv #(format "%02d%02d" id %) (range 12))])
                           [0 2 3 7])]
            (store/put-records! db rows)
            (doseq [[id values] rows]
              (is (= values (store/read-record db id))))
            (is (= (subvec rows 1 3) (store/scan-records db 1 3)))
            (is (= rows (store/scan-records db 0 8)))
            (store/update-field! db 2 10 "edit")
            (is (= (assoc (second (nth rows 1)) 10 "edit")
                   (store/read-record db 2)))
            (is (= [[2 (assoc (second (nth rows 1)) 10 "edit")]]
                   (store/scan-records db 2 1))))
          {})))))

(deftest comparison-selection-test
  (let [opts (runner/options {:system :all :api :datalog :workload :all})
        cases (runner/cases opts)]
    (is (= 24 (count cases)))
    (is (= #{[:datalevin :embedded] [:sqlite :embedded]
             [:datalevin :remote] [:postgres :remote]}
           (set (map (juxt :system :mode) cases))))
    (is (= 48 (count (runner/cases (assoc opts :api :all)))))
    (is (= 6 (count (runner/cases (assoc opts :system :postgres))))))
  (is (thrown? clojure.lang.ExceptionInfo
               (runner/cases (runner/options {:system :postgres :mode :embedded})))))

(deftest warmup-and-measurement-test
  (let [result (runner/run-case! (assoc small-options :api :kv :mode :embedded
                                       :workload :c :distribution :uniform))]
    (is (= 7 (get-in result [:warmup :operations])))
    (is (= 11 (get-in result [:measured :operations])))
    (is (= 11 (get-in result [:measured :by-operation :read :count])))
    (is (= {:status :passed :records 4 :all-records-checked? true} (:validation result)))
    (is (pos? (get-in result [:measured :ops-per-second])))))

(deftest worker-failure-invalidates-run-test
  (let [db (reify store/Records
             (read-record [_ _] (throw (ex-info "Injected read failure" {}))))]
    (is (thrown? ExecutionException
                 (runner/run-phase! db (w/keyspace 4) nil
                                    (assoc small-options :workload :c :distribution :uniform)
                                    :measured 11)))))

(deftest phase-timeout-test
  (let [db (reify store/Records
             (read-record [_ _] (Thread/sleep 10000)))]
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"phase timed out"
                         (runner/run-phase! db (w/keyspace 4) nil
                                            (assoc small-options :workload :c
                                                   :distribution :uniform
                                                   :phase-timeout-ms 100)
                                            :measured 11)))))
