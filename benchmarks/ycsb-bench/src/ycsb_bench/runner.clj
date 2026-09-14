(ns ycsb-bench.runner
  "Closed-loop concurrent execution, latency measurements and validation."
  (:require [ycsb-bench.store :as store]
            [ycsb-bench.server :as server]
            [ycsb-bench.sql :as sql]
            [ycsb-bench.workload :as w])
  (:import [java.util Arrays Random]
           [java.util.concurrent Callable CountDownLatch ExecutorCompletionService
            ExecutorService Executors Future TimeUnit]
           [java.util.concurrent.atomic AtomicBoolean]))

(set! *warn-on-reflection* true)

(def defaults
  (merge server/defaults
         {:system :datalevin :api :all :mode :all :workload :a :records 10000 :ops 10000 :warmup 1000
          :threads 1 :pool-size nil :seed 17 :field-count 10 :field-length 100
          :scan-length 100 :batch-size 100 :distribution nil :durability :strict
          :timeout-ms 60000 :phase-timeout-ms 600000 :keep-db? false
          :datalog-handles :shared :client-counts nil :repetitions 1
          :warmup-ms nil :measurement-ms nil}))

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
                         :durability #{:strict :relaxed}
                         :datalog-handles #{:shared :independent :both}
                         :server-mode #{:process :in-process}}]
      (when-not (contains? allowed (get opts k))
        (throw (ex-info (str "Invalid " (name k)) {:value (get opts k)}))))
    (doseq [k [:records :ops :threads :pool-size :field-count :field-length
               :scan-length :batch-size :timeout-ms :phase-timeout-ms :repetitions
               :server-heap-mb :server-workers :server-queue-size
               :server-transaction-threads :server-background-threads
               :server-startup-timeout-ms]]
      (when-not (and (integer? (get opts k)) (<= 1 (get opts k) Integer/MAX_VALUE))
        (throw (ex-info (str (name k) " must be a positive 32-bit integer") {}))))
    (when-not (and (integer? (:warmup opts)) (<= 0 (:warmup opts) Integer/MAX_VALUE))
      (throw (ex-info "warmup must be a nonnegative 32-bit integer" {})))
    (doseq [k [:warmup-ms :measurement-ms :server-transaction-lock-timeout-ms]
            :let [value (get opts k)] :when (some? value)]
      (when-not (and (integer? value)
                    (<= (if (= k :measurement-ms) 1 0) value Integer/MAX_VALUE))
        (throw (ex-info (str "Invalid " (name k)) {:value value}))))
    (doseq [k [:warmup-ms :measurement-ms] :when (some? (get opts k))]
      (when (>= (long (get opts k)) (long (:phase-timeout-ms opts)))
        (throw (ex-info "Phase timeout must exceed the requested duration" {:phase k}))))
    (when-let [counts (:client-counts opts)]
      (when-not (and (sequential? counts) (seq counts)
                    (= (count counts) (count (distinct counts)))
                    (every? #(and (integer? %) (<= 1 % Integer/MAX_VALUE)) counts))
        (throw (ex-info "client-counts must contain distinct positive integers" {}))))
    (when (and (= :datalevin (:system opts)) (= :datalog (:api opts))
               (= :remote (:mode opts)) (= :independent (:datalog-handles opts))
               (not= (:pool-size opts) (:threads opts)))
      (throw (ex-info "Independent Datalog handles require one connection per worker: pool-size must equal threads" {})))
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
        (vec (for [clients (or (:client-counts opts) [nil])
                   api (expand :api [:kv :datalog])
                   mode (expand :mode [:embedded :remote])
                   workload (expand :workload [:a :b :c :d :e :f])
                   system (cond-> [:datalevin]
                            (= api :datalog) (conj (if (= mode :embedded) :sqlite :postgres)))
                   :when (or (= :all (:system opts)) (= system (:system opts)))
                   handles (if (and (= system :datalevin) (= api :datalog) (= mode :remote))
                             (if (= :both (:datalog-handles opts))
                               [:shared :independent] [(:datalog-handles opts)])
                             [:shared])]
               (cond-> (assoc opts :system system :api api :mode mode :workload workload
                                   :datalog-handles handles :client-counts nil :repetitions 1)
                 clients (assoc :threads clients :pool-size clients))))]
    (when (empty? selected)
      (throw (ex-info "No compatible cases: SQLite needs embedded Datalog; PostgreSQL needs remote Datalog"
                      (select-keys opts [:system :api :mode]))))
    (vec (mapcat (fn [trial]
                   (map #(assoc % :trial trial)
                        (if (odd? trial) selected (reverse selected))))
                 (range 1 (inc (long (:repetitions opts))))))))

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
          cdf     (if (instance? clojure.lang.IAtom cdf) (w/grow-cdf! cdf visible) cdf)
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

(defn- check-stopping! [^AtomicBoolean stopping?]
  (when (or (.get stopping?) (.isInterrupted (Thread/currentThread)))
    (throw (InterruptedException. "Benchmark cancelled"))))

(defn- timed-worker!
  [db space cdf opts ^Random rng mix deadline stopping?]
  ;; Primitive chunks retain every sample without boxing each latency or
  ;; requiring an estimate of how many operations fit in the time window.
  (loop [chunks [], latencies (long-array 8192), codes (byte-array 8192), i 0]
    (let [t0 (System/nanoTime)]
      (if (and (or (pos? i) (seq chunks)) (>= t0 (long deadline)))
        (cond-> chunks (pos? i) (conj [latencies codes i]))
        (do
          (check-stopping! stopping?)
          (let [operation (w/choose-operation rng mix)]
            (execute! db space rng cdf opts operation)
            (aset-long latencies i (- (System/nanoTime) t0))
            (aset-byte codes i (byte (w/operation-code operation))))
          (if (= (inc i) 8192)
            (recur (conj chunks [latencies codes 8192])
                   (long-array 8192) (byte-array 8192) 0)
            (recur chunks latencies codes (inc i))))))))

(defn- combine-samples [worker-results]
  (let [chunks (mapcat identity worker-results)
        n (reduce + 0 (map #(nth % 2) chunks))]
    (when (> n Integer/MAX_VALUE)
      (throw (ex-info "Too many latency samples; shorten the measurement" {:operations n})))
    (let [latencies (long-array n), codes (byte-array n)]
      (reduce (fn [offset [samples operations size]]
                (System/arraycopy samples 0 latencies offset size)
                (System/arraycopy operations 0 codes offset size)
                (+ (long offset) (long size)))
              0 chunks)
      [latencies codes])))

(defn run-phase!
  "Workers share a start gate. Counts or a duration bound each phase.
  A failed worker or phase timeout invalidates the entire run."
  [db space cdf opts phase n]
  (let [duration-ms (get opts (if (= phase :warmup) :warmup-ms :measurement-ms))
        timed? (some? duration-ms)]
    (if (if timed? (zero? (long duration-ms)) (zero? (long n)))
      (summarize (long-array 0) (byte-array 0) 0)
      (let [workers (if timed? (long (:threads opts)) (min (long (:threads opts)) (long n)))
            handles (mapv #(store/for-worker db %) (range workers))
            executor (Executors/newFixedThreadPool workers)
            completion (ExecutorCompletionService. executor)
            ready (CountDownLatch. workers)
            start (CountDownLatch. 1)
            latencies (when-not timed? (long-array n))
            codes (when-not timed? (byte-array n))
            end-ns (long-array 1)
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
                                    db (nth handles worker)
                                    rng (Random. seed)]
                                (.countDown ready)
                                (.await start)
                                (if timed?
                                  (timed-worker! db space cdf opts rng mix (aget end-ns 0) stopping?)
                                  (loop [i worker]
                                    (when (< i (long n))
                                      (check-stopping! stopping?)
                                      (let [t0 (System/nanoTime)
                                            operation (w/choose-operation rng mix)]
                                        (execute! db space rng cdf opts operation)
                                        (aset-long latencies i (- (System/nanoTime) t0))
                                        (aset-byte codes i (byte (w/operation-code operation))))
                                      (recur (+ i workers))))))))))
          (when-not (.await ready timeout TimeUnit/MILLISECONDS)
            (throw (ex-info "Benchmark workers did not start" {:phase phase})))
          (let [t0 (System/nanoTime)
                deadline (+ t0 (* timeout 1000000))
                _ (when timed? (aset-long end-ns 0 (+ t0 (* (long duration-ms) 1000000))))
                _ (.countDown start)
                results
                (mapv (fn [_]
                        (if-let [^Future done (.poll completion
                                                    (max 0 (- deadline (System/nanoTime)))
                                                    TimeUnit/NANOSECONDS)]
                          (.get done)
                          (throw (ex-info "Benchmark phase timed out"
                                          {:phase phase :timeout-ms timeout}))))
                      (range workers))
                elapsed (- (System/nanoTime) t0)
                [latencies codes] (if timed? (combine-samples results) [latencies codes])]
            (summarize latencies codes elapsed))
          (finally
            (.set stopping? true)
            (stop-workers! executor @futures)))))))

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
        _ (when (and (= :datalevin (:system opts)) (= :datalog (:api opts))
                     (= :remote (:mode opts)) (= :both (:datalog-handles opts)))
            (throw (ex-info "run-case! requires a single Datalog handle mode" {})))
        _ (cases opts)
        capacity (+ (:records opts) (:warmup opts) (:ops opts))
        cdf (when-not (= :uniform (:distribution opts))
              (let [table (w/zipf-cdf capacity)]
                (if (or (:warmup-ms opts) (:measurement-ms opts)) (atom table) table)))]
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
          {:configuration (cond-> (dissoc opts :pg-url :pg-user :client-counts :repetitions)
                            (not (and (= :datalevin (:system opts))
                                      (= :datalog (:api opts)) (= :remote (:mode opts))))
                            (dissoc :datalog-handles))
           :storage (store/storage-info db)
           :load load-result :warmup warmup :measured measured
           :validation (validate-database! db expected opts)})))))
