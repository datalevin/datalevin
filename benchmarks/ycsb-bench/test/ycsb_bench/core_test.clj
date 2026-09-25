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
    (is (every? #(re-matches #"[a-z]+" %) values))))

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
            keys (repeatedly 2000 #(w/choose-key rng distribution
                                                 (if (= distribution :zipfian) n cdf) n))]
        (is (every? #(<= 0 % (dec n)) keys))))
    (let [rng (Random. 17)
          keys (repeatedly 100000 #(w/choose-key rng :zipfian 100000 100000))]
      (is (< (count (filter #(< % 1000) keys)) 5000)
          "The lowest 1% of numeric IDs must not receive most Zipfian requests")))
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
               {:api :sql} {:records Integer/MAX_VALUE} {:sql-indexes :primary}
               {:value-audit? :yes}]]
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

(deftest character-validation-after-timing-test
  (doseq [operation [:read :scan]
          timed? [false true]
          [expected values] [[:valid ["aaaa"]] [:characters ["éaaa"]]
                             [:shape nil] [:shape [42]] [:shape ["aaa"]]
                             [:shape ["aaaa" "bbbb"]]]]
    (testing (str operation " duration-based=" timed? " values=" (pr-str values))
      (let [completed (atom [])
            run-phase runner/run-phase!
            db (reify store/Records
                 (put-records! [_ _] nil)
                 (read-record [_ _] values)
                 (scan-records [_ start _n] [[start values]])
                 (record-count [_] 1)
                 (storage-info [_] {}))
            opts (cond-> {:api :kv :mode :embedded :workload :c
                          :distribution :uniform :records 1 :warmup 2 :ops 3
                          :threads 1 :field-count 1 :field-length 4 :scan-length 1}
                   timed? (assoc :warmup-ms 10 :measurement-ms 10))
            result (with-redefs [store/with-stores (fn [_ f] (f (fn [callback] (callback db))))
                                 w/choose-operation (fn [_ _] operation)
                                 runner/run-phase!
                                 (fn [db space cdf opts phase n]
                                   (let [result (run-phase db space cdf opts phase n)]
                                     (swap! completed conj phase)
                                     result))]
                     (try (runner/run-case! opts) (catch Exception e e)))]
        (is (= (case expected :shape [] :characters [:warmup] [:warmup :measured]) @completed))
        (case expected
          :valid
          (is (= {:status :passed :records 1 :all-records-checked? true
                  :scope :structure :value-checks :not-performed
                  :character-checks :post-measurement}
                 (:validation result)))
          :characters
          (do (is (instance? clojure.lang.ExceptionInfo result))
              (is (= "Missing or malformed record" (ex-message result))))
          :shape
          (do (is (instance? ExecutionException result))
              (is (= "Missing or malformed record" (ex-message (ex-cause result))))))))))

(defn check-adapter! [db]
  (let [values ["aaaa" "bbbb" "cccc"]]
    (store/put-records! db (mapv #(vector % values) ["user2" "user0" "user1"]))
    (is (= 3 (store/record-count db)))
    (is (= values (store/read-record db "user1")))
    (store/update-field! db "user0" 1 "zzzz")
    (is (= ["aaaa" "zzzz" "cccc"] (store/read-record db "user0")))
    (store/update-field! db "user0" 0 "uaaa")
    (is (= ["uaaa" "zzzz" "cccc"] (store/read-record db "user0"))
        "Updating a field preserves the other fields")
    (is (= [["user0" ["uaaa" "zzzz" "cccc"]]] (store/scan-records db "user0" 1)))
    (is (empty? (store/scan-records db "user0" 0)))
    (is (= [["user1" values]] (store/scan-records db "user1" 1)))
    (is (= [["user1" values] ["user2" values]] (store/scan-records db "user1" 3)))
    (store/put-records! db [["user3" values]])
    (is (= 4 (store/record-count db)))
    (is (= [["user2" values] ["user3" values]] (store/scan-records db "user2" 2)))
    (store/update-field! db "user1" 2 "zzzz")
    (is (= [["user1" ["aaaa" "bbbb" "zzzz"]]] (store/scan-records db "user1" 1)))
    (is (empty? (store/scan-records db "user4" 1)))
    ;; Callers continue checking this fixture after the shared adapter checks.
    (store/update-field! db "user1" 2 "cccc"))
  {})

(defn- check-kv-record-layout! [db]
  (let [handle (:handle (store/for-worker db 0))]
    (is (= {:layout :record-value :key-type :string :value-type :data}
           (select-keys (store/storage-info db) [:layout :key-type :value-type])))
    (is (= 4 (d/entries handle "records")))
    (is (= ["user0" "user1" "user2" "user3"]
           (mapv first (d/get-range handle "records" [:all] :string :data))))
    (is (= ["uaaa" "zzzz" "cccc"] (d/get-value handle "records" "user0" :string :data)))
    (store/put-records! db [["user7" ["same" "same" "same"]]])
    (is (= [["user7" ["same" "same" "same"]]] (store/scan-records db "user4" 3)))
    (let [executor (Executors/newFixedThreadPool 2)]
      (try
        (let [tasks (.invokeAll
                      executor
                      (mapv (fn [field]
                              ^Callable
                              (fn []
                                (dotimes [step 20]
                                  (store/update-field! db "user7" field
                                                       (format "%04d" (+ (* field 100) step))))))
                            [0 1]))]
          (doseq [^Future task tasks] (.get task)))
        (finally
          (.shutdownNow executor)
          (.awaitTermination executor 30 TimeUnit/SECONDS))))
    (is (= ["0019" "0119" "same"] (store/read-record db "user7"))
        "Replacing a record must preserve concurrent writes to other fields")
    (is (= ["0019" "0119" "same"] (d/get-value handle "records" "user7" :string :data)))
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"Missing KV record"
                         (store/update-field! db "user99" 0 "oops")))
    (is (= 5 (store/record-count db) (d/entries handle "records")))))

(deftest adapter-semantics-test
  (doseq [api [:kv :datalog], mode [:embedded :remote]]
    (testing (str api " " mode)
      (store/with-store
        (assoc small-options :api api :mode mode)
        (fn [db]
          (is (every? (:env-flags (store/storage-info db)) [:writemap :nosync])
              "The standard WAL adapter must inherit the effective native defaults")
          (is (= :client-read-update (:rmw-execution (store/storage-info db))))
          (is (false? (:atomic-rmw? (store/storage-info db))))
          (when (= api :datalog)
            (is (= 0 (:cache-limit (store/storage-info db))))
            (is (= :prepare-q (:scan-api (store/storage-info db))))
            (is (= :attribute-value-range (:scan-selection (store/storage-info db))))
            (is (= :ycsb/key (:record-key (store/storage-info db)))))
          (check-adapter! db)
          (when (= api :kv)
            (check-kv-record-layout! db))
          (when (= api :datalog)
            (let [conn (:conn (store/for-worker db 0))]
              (is (= {:ycsb/key "user0" :ycsb/field0 "uaaa" :ycsb/field1 "zzzz"
                      :ycsb/field2 "cccc"}
                     (dissoc (d/pull @conn '[*] [:ycsb/key "user0"]) :db/id))
                  "The application key is independent of the internal entity ID")
              (is (= #{["user0"] ["user1"] ["user2"] ["user3"]}
                     (d/q '[:find ?key :where [?e :ycsb/key ?key]] @conn)))
              (is (not (contains? (d/schema conn) :ycsb/id)))
              (doseq [attr (:attributes (store/for-worker db 0))]
                (is (true? (get-in (d/schema conn) [attr :db/noindex]))))
              (is (not (get-in (d/schema conn) [:ycsb/key :db/noindex])))
              (is (= #{:ycsb/key} (set (map :a (d/datoms @conn :ave))))
                  "Only application keys belong in AVE after inserts and updates")
              (store/put-records! db [["user7" ["dddd" "eeee" "ffff"]]])
              (is (= [["user7" ["dddd" "eeee" "ffff"]]] (store/scan-records db "user4" 3))
                  "The page counts existing records across key gaps")
              (is (= [["user7" ["dddd" "eeee" "ffff"]]] (store/scan-records db "user7" 1)))
              (is (= 5 (store/record-count db)))))
          {})))))

(deftest read-update-preparation-reuse-test
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
            (store/put-records! db [["user1" ["aaaa" "bbbb" "cccc"]]])
            (store/read-record db "user1")
            (with-redefs [d/prepare-pull
                          (fn [& args]
                            (swap! preparations inc)
                            (apply prepare args))
                          pull/parse-opts
                          (fn [view pattern opts]
                            (when (= pattern attributes) (swap! parses inc))
                            (parse view pattern opts))]
              (dotimes [i 5]
                (store/read-record db "user1")
                (store/update-field! db "user1" 0 (format "%04d" i))))
            (is (zero? @preparations)
                "Successive reads reuse the preparation wrapper")
            (is (zero? @parses)
                "Reads after writes retain the parsed pull pattern")
            (is (= ["0004" "bbbb" "cccc"] (store/read-record db "user1"))
                "Every reused read sees the latest committed value"))
          {})))))

(deftest datalog-scan-field-order-test
  (doseq [mode [:embedded :remote]]
    (testing (str mode)
      (store/with-store
        (assoc small-options :api :datalog :mode mode :field-count 12 :scan-length 8)
        (fn [db]
          (let [rows (mapv (fn [id]
                             [(str "user" id) (mapv #(format "%02d%02d" id %) (range 12))])
                           [0 2 3 7])]
            (store/put-records! db rows)
            (doseq [[id values] rows]
              (is (= values (store/read-record db id))))
            (is (= (subvec rows 1 4) (store/scan-records db "user1" 3)))
            (is (= rows (store/scan-records db "user0" 8)))
            (store/update-field! db "user2" 10 "edit")
            (is (= (assoc (second (nth rows 1)) 10 "edit")
                   (store/read-record db "user2")))
            (is (= [["user2" (assoc (second (nth rows 1)) 10 "edit")]]
                   (store/scan-records db "user2" 1))))
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
    (is (= {:status :passed :records 4 :all-records-checked? true
            :scope :structure :value-checks :not-performed
            :character-checks :post-measurement}
           (:validation result)))
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
