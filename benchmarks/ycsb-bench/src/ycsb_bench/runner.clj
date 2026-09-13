(ns ycsb-bench.runner
  "Closed-loop concurrent execution, latency measurements and validation."
  (:require [ycsb-bench.store :as store]
            [ycsb-bench.sql :as sql]
            [ycsb-bench.workload :as w])
  (:import [java.util Arrays Random]
           [java.util.concurrent Callable CountDownLatch ExecutorCompletionService
            ExecutorService Executors Future TimeUnit]
           [java.util.concurrent.atomic AtomicBoolean]))

(set! *warn-on-reflection* true)

(def defaults
  {:system :datalevin :api :all :mode :all :workload :a :records 10000 :ops 10000 :warmup 1000
   :threads 1 :pool-size nil :seed 17 :field-count 10 :field-length 100
   :scan-length 100 :batch-size 100 :distribution nil :durability :strict
   :timeout-ms 60000 :phase-timeout-ms 600000 :keep-db? false})

(defn options
  "Normalize and validate options before allocating resources."
  [provided]
  (let [opts (merge defaults provided)
        opts (update opts :pool-size #(or % (:threads opts)))]
    (doseq [[k allowed] {:system #{:all :datalevin :sqlite :postgres}
                         :api #{:all :kv :datalog}
                         :mode #{:all :embedded :remote}
                         :workload (conj (set (keys w/workloads)) :all)
                         :distribution #{nil :uniform :zipfian :latest}
                         :durability #{:strict :relaxed}}]
      (when-not (contains? allowed (get opts k))
        (throw (ex-info (str "Invalid " (name k)) {:value (get opts k)}))))
    (doseq [k [:records :ops :threads :pool-size :field-count :field-length
               :scan-length :batch-size :timeout-ms :phase-timeout-ms]]
      (when-not (and (integer? (get opts k)) (<= 1 (get opts k) Integer/MAX_VALUE))
        (throw (ex-info (str (name k) " must be a positive 32-bit integer") {}))))
    (when-not (and (integer? (:warmup opts)) (<= 0 (:warmup opts) Integer/MAX_VALUE))
      (throw (ex-info "warmup must be a nonnegative 32-bit integer" {})))
    (when-not (and (integer? (:seed opts)) (<= Long/MIN_VALUE (:seed opts) Long/MAX_VALUE))
      (throw (ex-info "seed must be a 64-bit integer" {})))
    (when (> (+ (:records opts) (:ops opts) (:warmup opts)) Integer/MAX_VALUE)
      (throw (ex-info "records + ops + warmup must fit a 32-bit integer" {})))
    opts))

(defn cases
  "Pair embedded Datalog with SQLite and remote Datalog with PostgreSQL.
  SQL-only selections retain just their compatible API/mode combination."
  [opts]
  (let [expand (fn [k values] (if (= :all (get opts k)) values [(get opts k)]))
        selected
        (vec (for [api (expand :api [:kv :datalog])
                   mode (expand :mode [:embedded :remote])
                   workload (expand :workload [:a :b :c :d :e :f])
                   system (cond-> [:datalevin]
                            (= api :datalog) (conj (if (= mode :embedded) :sqlite :postgres)))
                   :when (or (= :all (:system opts)) (= system (:system opts)))]
               (assoc opts :system system :api api :mode mode :workload workload)))]
    (when (empty? selected)
      (throw (ex-info "No compatible cases: SQLite needs embedded Datalog; PostgreSQL needs remote Datalog"
                      (select-keys opts [:system :api :mode]))))
    selected))

(defn- ascii-field? [value ^long field-length]
  (and (string? value)
       (= (.length ^String value) field-length)
       (loop [i 0]
         (if (= i field-length)
           true
           (and (< (int (.charAt ^String value (int i))) 128)
                (recur (inc i)))))))

(defn validate-record!
  "Validate field count and fixed ASCII byte lengths."
  [values {:keys [field-count field-length]}]
  (when-not (and (= (count values) field-count)
                 (every? #(ascii-field? % field-length) values))
    (throw (ex-info "Missing or malformed record" {:field-count (count values)}))))

(defn- execute!
  [db space ^Random rng cdf {:keys [field-count field-length scan-length
                                   distribution] :as opts} operation]
  (if (= operation :insert)
    (let [id (w/reserve-key! space)]
      (store/put-records! db [[id (w/record-values rng opts)]])
      (w/acknowledge-key! space id))
    (let [visible (:visible @space)
          id      (w/choose-key rng distribution cdf visible)]
      (case operation
        :read (validate-record! (store/read-record db id) opts)
        :update (store/update-field! db id (.nextInt rng field-count)
                                      (w/random-value rng field-length))
        :rmw (store/modify-field! db id (.nextInt rng field-count))
        :scan (let [n (min (inc (.nextInt rng scan-length)) (- (long visible) id))
                    rows (store/scan-records db id n)]
                (when-not (= (mapv first rows) (vec (range id (+ id n))))
                  (throw (ex-info "Scan returned incorrect IDs" {:start id :count n})))
                (doseq [[_ values] rows] (validate-record! values opts)))))))

(defn latency-summary!
  "Exact nearest-rank percentiles in microseconds; sorts the supplied array in place."
  [^longs samples]
  (let [n (alength samples)]
    (when (pos? n)
      (Arrays/sort samples)
      (let [percentile (fn [p]
                         (/ (double (aget samples (dec (int (Math/ceil (* (double p) n))))))
                            1000.0))]
        {:mean (/ (areduce samples i total 0.0 (+ total (double (aget samples i))))
                   n 1000.0)
         :p50 (percentile 0.50) :p95 (percentile 0.95) :p99 (percentile 0.99)
         :max (/ (double (aget samples (dec n))) 1000.0)}))))

(defn- summarize [^longs latencies ^bytes codes elapsed-ns]
  (let [n (alength latencies)
        counts (long-array (count w/operations))]
    (dotimes [i n]
      (let [code (aget codes i)]
        (aset-long counts code (inc (aget counts code)))))
    {:operations n
     :seconds (/ (double elapsed-ns) 1e9)
     :ops-per-second (if (zero? n) 0.0 (/ (* n 1e9) (double elapsed-ns)))
     :by-operation
     (into {}
           (keep-indexed
             (fn [code operation]
               (let [cnt (aget counts code)]
                 (when (pos? cnt)
                   (let [samples (long-array cnt)]
                     (loop [i 0, j 0]
                       (when (< i n)
                         (if (= code (aget codes i))
                           (do (aset-long samples j (aget latencies i))
                               (recur (inc i) (inc j)))
                           (recur (inc i) j))))
                     [operation {:count cnt :latency-us (latency-summary! samples)}]))))
             w/operations))
     :latency-us (latency-summary! latencies)}))

(defn- stop-workers!
  "Join every worker before the owner can close the database. Interruptions
  of the coordinator must not bypass this wait or mask the original failure."
  [^ExecutorService executor tasks]
  (let [interrupted? (volatile! (Thread/interrupted))]
    (try
      (doseq [^Future task tasks] (.cancel task true))
      (.shutdownNow executor)
      (loop []
        (when-not (try (.awaitTermination executor 1 TimeUnit/SECONDS)
                       (catch InterruptedException _
                         (vreset! interrupted? true)
                         false))
          (recur)))
      (finally
        (when @interrupted? (.interrupt (Thread/currentThread)))))))

(defn run-phase!
  "Workers share a start gate. Each operation has a disjoint sample slot.
  A failed worker or phase timeout invalidates the entire run."
  [db space cdf opts phase n]
  (if (zero? (long n))
    (summarize (long-array 0) (byte-array 0) 0)
    (let [workers (min (long (:threads opts)) (long n))
          executor (Executors/newFixedThreadPool workers)
          completion (ExecutorCompletionService. executor)
          ready (CountDownLatch. workers)
          start (CountDownLatch. 1)
          latencies (long-array n)
          codes (byte-array n)
          stopping? (AtomicBoolean. false)
          timeout (long (:phase-timeout-ms opts))
          mix (get-in w/workloads [(:workload opts) :mix])
          futures (atom [])]
      (try
        (dotimes [worker workers]
          (swap! futures conj
                 (.submit completion
                          ^Callable
                          (fn []
                            (let [seed (unchecked-add (long (:seed opts))
                                                      (+ (* 1000003 (inc worker))
                                                         (if (= phase :warmup) 0 999983)))
                                  rng (Random. seed)]
                              (.countDown ready)
                              (.await start)
                              (loop [i worker]
                                (when (< i (long n))
                                  (when (or (.get stopping?)
                                            (.isInterrupted (Thread/currentThread)))
                                    (throw (InterruptedException. "Benchmark cancelled")))
                                  (let [t0 (System/nanoTime)
                                        operation (w/choose-operation rng mix)]
                                    (execute! db space rng cdf opts operation)
                                    (aset-long latencies i (- (System/nanoTime) t0))
                                    (aset-byte codes i (byte (w/operation-code operation))))
                                  (recur (+ i workers)))))))))
        (when-not (.await ready timeout TimeUnit/MILLISECONDS)
          (throw (ex-info "Benchmark workers did not start" {:phase phase})))
        (let [t0 (System/nanoTime)
              deadline (+ t0 (* timeout 1000000))]
          (.countDown start)
          (dotimes [_ workers]
            (if-let [^Future done (.poll completion
                                        (max 0 (- deadline (System/nanoTime)))
                                        TimeUnit/NANOSECONDS)]
              (.get done)
              (throw (ex-info "Benchmark phase timed out" {:phase phase :timeout-ms timeout}))))
          (summarize latencies codes (- (System/nanoTime) t0)))
        (finally
          (.set stopping? true)
          (stop-workers! executor @futures))))))

(defn- load! [db {:keys [records batch-size seed] :as opts}]
  (let [t0 (System/nanoTime)]
    (doseq [ids (partition-all batch-size (range records))]
      (store/put-records! db (mapv (fn [id] [id (w/initial-values seed id opts)]) ids)))
    (let [elapsed (- (System/nanoTime) t0)]
      {:records records :seconds (/ elapsed 1e9)
       :records-per-second (/ (* (double records) 1e9) elapsed)})))

(defn- validate-database! [db expected opts]
  (let [actual (store/record-count db)]
    (when-not (= actual expected)
      (throw (ex-info "Incorrect final record count" {:expected expected :actual actual})))
    (doseq [start (range 0 expected 1000)]
      (let [n (min 1000 (- (long expected) (long start)))
            rows (store/scan-records db start n)]
        (when-not (= (mapv first rows) (vec (range start (+ start n))))
          (throw (ex-info "Missing or unordered records" {:start start :count n})))
        (doseq [[_ values] rows] (validate-record! values opts))))
    {:status :passed :records actual :all-records-checked? true}))

(defn run-case! [provided]
  (let [opts (options provided)
        opts (update opts :distribution #(or % (get-in w/workloads [(:workload opts) :distribution])))
        _ (when (some #{:all} ((juxt :system :api :mode :workload) opts))
            (throw (ex-info "run-case! requires a single system, API, mode and workload" {})))
        _ (cases opts)
        capacity (+ (:records opts) (:warmup opts) (:ops opts))
        cdf (when-not (= :uniform (:distribution opts)) (w/zipf-cdf capacity))]
    ((if (= :datalevin (:system opts)) store/with-store sql/with-store)
      opts
      (fn [db]
        (let [load-result (load! db opts)
              space (w/keyspace (:records opts))
              warmup (run-phase! db space cdf opts :warmup (:warmup opts))
              measured (run-phase! db space cdf opts :measured (:ops opts))
              expected (+ (:records opts)
                          (get-in warmup [:by-operation :insert :count] 0)
                          (get-in measured [:by-operation :insert :count] 0))]
          (when-not (= expected (:visible @space) (:next @space))
            (throw (ex-info "Unacknowledged insert IDs" {:keyspace @space})))
          {:configuration (dissoc opts :pg-url :pg-user)
           :storage (store/storage-info db)
           :load load-result :warmup warmup :measured measured
           :validation (validate-database! db expected opts)})))))
