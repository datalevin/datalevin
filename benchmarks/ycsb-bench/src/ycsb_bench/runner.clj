(ns ycsb-bench.runner
  "Closed-loop concurrent execution, latency measurements and validation."
  (:require [ycsb-bench.store :as store]
            [ycsb-bench.audit :as audit]
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
          :scan-length 100 :batch-size 1 :distribution nil :zipfian-keyspace nil :durability :strict
          :timeout-ms 60000 :phase-timeout-ms 600000 :keep-db? false
          :datalog-handles :independent :client-counts nil :repetitions 1
          :warmup-ms nil :measurement-ms nil :value-audit? false}))

(defn options
  "Normalize and validate options before allocating resources."
  [provided]
  (when (contains? provided :sql-indexes)
    (throw (ex-info "sql-indexes is no longer supported; payload fields are unindexed for every API" {})))
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
               :server-heap-mb
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
    ;; Check expanded selections before starting any case. Client-count sweeps
    ;; override both counts with the same value when the cases are expanded.
    (when (and (#{:datalevin :all} (:system opts)) (#{:datalog :all} (:api opts))
               (#{:remote :all} (:mode opts)) (#{:independent :both} (:datalog-handles opts))
               (nil? (:client-counts opts))
               (not= (:pool-size opts) (:threads opts)))
      (throw (ex-info "Independent Datalog handles require one connection per worker: pool-size must equal threads" {})))
    (when-not (and (integer? (:seed opts)) (<= Long/MIN_VALUE (:seed opts) Long/MAX_VALUE))
      (throw (ex-info "seed must be a 64-bit integer" {})))
    (when-not (boolean? (:value-audit? opts))
      (throw (ex-info "value-audit? must be boolean" {})))
    (when (> (+ (:records opts) (max (:ops opts) (:warmup opts))) Integer/MAX_VALUE)
      (throw (ex-info "records + the larger phase operation count must fit a 32-bit integer" {})))
    (when-some [n (:zipfian-keyspace opts)]
      (when-not (and (integer? n) (<= (:records opts) n Integer/MAX_VALUE))
        (throw (ex-info "zipfian-keyspace must be at least records and fit a 32-bit integer" {}))))
    ;; A duration supplies no operation count from which to predict inserts.
    ;; Check every selected workload before opening any comparison resources.
    (when (and (nil? (:zipfian-keyspace opts))
               (or (some? (:measurement-ms opts)) (pos? (or (:warmup-ms opts) 0))))
      (doseq [workload (if (= :all (:workload opts)) (keys w/workloads) [(:workload opts)])
              :let [spec (w/workloads workload)]
              :when (and (= :zipfian (or (:distribution opts) (:distribution spec)))
                         (some (fn [[op weight]] (and (= :insert op) (pos? weight)))
                               (:mix spec)))]
        (throw (ex-info "Timed workloads with Zipfian inserts require an explicit --zipfian-keyspace"
                        {:workload workload}))))
    opts))

(defn cases
  "Pair each API with SQLite embedded or PostgreSQL remote. All stores index
  the application key and leave payload fields unindexed."
  [opts]
  (let [expand (fn [k values] (if (= :all (get opts k)) values [(get opts k)]))
        selected
        (vec (for [clients (or (:client-counts opts) [nil])
                   api (expand :api [:kv :datalog])
                   mode (expand :mode [:embedded :remote])
                   workload (expand :workload [:a :b :c :d :e :f])
                   system [:datalevin (if (= mode :embedded) :sqlite :postgres)]
                   :when (or (= :all (:system opts)) (= system (:system opts)))
                   handles (if (and (= system :datalevin) (= api :datalog) (= mode :remote))
                             (if (= :both (:datalog-handles opts))
                               [:shared :independent] [(:datalog-handles opts)])
                             [:shared])]
               (cond-> (assoc opts :system system :api api :mode mode :workload workload
                                   :datalog-handles handles :client-counts nil :repetitions 1)
                 clients (assoc :threads clients :pool-size clients))))]
    (when (empty? selected)
      (throw (ex-info "No compatible cases: SQLite needs embedded mode; PostgreSQL needs remote mode"
                      (select-keys opts [:system :api :mode]))))
    (vec (mapcat (fn [trial]
                   (map #(assoc % :trial trial)
                        (if (odd? trial) selected (reverse selected))))
                 (range 1 (inc (long (:repetitions opts))))))))

(defn- field-shape? [value ^long field-length]
  (and (string? value)
       (= (.length ^String value) field-length)))

(defn- ascii-field? [value ^long field-length]
  (and (field-shape? value field-length)
       (loop [i 0]
         (if (= i field-length)
           true
           (and (< (int (.charAt ^String value (int i))) 128)
                (recur (inc i)))))))

(defn validate-record!
  "Validate field count and fixed ASCII byte lengths outside measured phases."
  [values {:keys [field-count field-length]}]
  (when-not (and (= (count values) field-count)
                 (every? #(ascii-field? % field-length) values))
    (throw (ex-info "Missing or malformed record" {:field-count (count values)}))))

(defn- validate-record-shape!
  "Check materialized fields without walking their characters in timed reads."
  [values {:keys [field-count field-length]}]
  (when-not (and (= (count values) field-count)
                 (every? #(field-shape? % field-length) values))
    (throw (ex-info "Missing or malformed record" {:field-count (count values)}))))

(defn- validate-key-page!
  "Timed checks for a page starting at an existing key. Exact membership is
  checked against sorted generated keys after writers have stopped."
  [rows start n opts]
  (let [keys (mapv first rows)]
    (when-not (and (<= 1 (count rows) n)
                   (= start (first keys))
                   (every? string? keys)
                   (every? neg? (map compare keys (next keys))))
      (throw (ex-info "Incorrect application-key page"
                      {:start start :limit n :keys keys})))
    (doseq [[_ values] rows] (validate-record-shape! values opts))))

(defn- execute!
  [db space ^Random rng cdf {:keys [field-count field-length scan-length
                                   distribution] :as opts} operation]
  (if (= operation :insert)
    (let [id (w/reserve-key! space)]
      (store/put-records! db [[(w/application-key id) (w/record-values rng opts)]])
      (w/acknowledge-key! space id))
    (let [visible (:visible @space)
          cdf     (if (instance? clojure.lang.IAtom cdf) (w/grow-cdf! cdf visible) cdf)
          ordinal (w/choose-key rng distribution cdf visible)
          id      (w/application-key ordinal)]
      (case operation
        :read (validate-record-shape! (store/read-record db id) opts)
        :update (store/update-field! db id (.nextInt rng field-count)
                                      (w/random-value rng field-length))
        :rmw (let [field (.nextInt rng field-count)
                   value (w/random-value rng field-length)]
               ;; CoreWorkload generates an independent replacement, then calls
               ;; read and update separately. There is no encompassing transaction.
               (validate-record-shape! (store/read-record db id) opts)
               (store/update-field! db id field value))
        :scan (let [n (inc (.nextInt rng scan-length))
                    rows (store/scan-records db id n)]
                ;; Inserts can appear anywhere in string-key order, including
                ;; between two records that were visible at request generation.
                (validate-key-page! rows id n opts))))))

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
      (store/put-records! db (mapv (fn [id] [(w/application-key id) (w/initial-values seed id opts)]) ids)))
    (let [elapsed (- (System/nanoTime) t0)]
      {:records records :seconds (/ elapsed 1e9)
       :records-per-second (/ (* (double records) 1e9) elapsed)})))

(defn- validate-database!
  ([db expected opts] (validate-database! db expected opts nil))
  ([db expected opts value-index]
   (let [actual (store/record-count db)
         check! (fn [id values]
                  (validate-record! values opts)
                  (when value-index
                    (audit/check-values! value-index id values Long/MAX_VALUE Long/MAX_VALUE)))]
     (when-not (= actual expected)
       (throw (ex-info "Incorrect final record count" {:expected expected :actual actual})))
     (if (= :e (:workload opts))
       (doseq [keys (partition-all (:scan-length opts)
                                   (sort (map w/application-key (range expected))))]
         (let [rows (store/scan-records db (first keys) (count keys))]
           (when-not (= (vec keys) (mapv first rows))
             (throw (ex-info "Missing or unordered application keys"
                             {:expected (vec keys) :actual (mapv first rows)})))
           (doseq [[id values] rows] (check! id values))))
       ;; Point validation runs after measurement and uses the same string keys
       ;; as loading and timed requests.
       (doseq [ordinal (range expected)]
         (let [id (w/application-key ordinal)]
           (check! id (store/read-record db id)))))
     {:status :passed :records actual :all-records-checked? true
      :scope (if value-index :structure-and-observed-values :structure)
      :value-checks (if value-index :passed :not-performed)
      :character-checks :post-measurement})))

(defn- phase-duration [opts phase]
  (get opts (if (= phase :warmup) :warmup-ms :measurement-ms)))

(defn- request-state [opts phase n]
  (case (:distribution opts)
    :uniform nil
    :zipfian (:zipfian-keyspace opts)
    :latest (if (some? (phase-duration opts phase))
              ;; Operation counts are overridden by the timer. Grow only as
              ;; committed inserts extend the visible prefix.
              (atom (w/zipf-cdf (:records opts)))
              (w/zipf-cdf (+ (:records opts) n)))))

(defn- predicted-keyspace [opts n]
  (let [insert-percent (get (into {} (get-in w/workloads [(:workload opts) :mix])) :insert 0)]
    ;; Each phase has its own dataset. Only that phase's inserts need headroom.
    (inc (+ (:records opts) (quot (* n insert-percent 2) 100)))))

(defn- run-dataset!
  "Load and execute one phase on a fresh store, with its own sampler and audit."
  [db opts phase n]
  (let [load-result (load! db opts)
        starting-records (store/record-count db)
        _ (when-not (= (:records opts) starting-records)
            (throw (ex-info "Incorrect starting record count"
                            {:phase phase :expected (:records opts) :actual starting-records})))
        space (w/keyspace (:records opts))
        cdf (request-state opts phase n)
        history (when (:value-audit? opts) (audit/history))
        phase-db (if history (audit/->AuditedRecords db history) db)
        result (run-phase! phase-db space cdf opts phase n)
        expected (+ (:records opts) (get-in result [:by-operation :insert :count] 0))
        events (when history (audit/events history))
        value-index (when history (audit/prepare events opts w/application-key))
        value-checks (when history
                       (audit/check-observations! value-index events #(validate-record! % opts)))]
    (when-not (= expected (:visible @space) (:next @space))
      (throw (ex-info "Unacknowledged insert IDs" {:phase phase :keyspace @space})))
    {:storage (store/storage-info db)
     :load load-result
     :result (cond-> (assoc result :starting-records starting-records)
               (= :zipfian (:distribution opts)) (assoc :zipfian-keyspace (:zipfian-keyspace opts)))
     :validation (cond-> (validate-database! db expected opts value-index)
                   history (assoc :value-checks (assoc value-checks :final-records expected))
                   (= phase :warmup) (assoc :character-checks :post-warmup)
                   (and history (= phase :warmup))
                   (assoc-in [:value-checks :comparisons] :post-warmup))}))

(defn run-case! [provided]
  (let [opts (options provided)
        opts (cond-> (assoc opts :workload-model (w/workload-model (:workload opts)))
               (= :f (:workload opts)) (assoc :rmw-execution :client-read-update
                                             :atomic-rmw? false))
        opts (update opts :distribution #(or % (get-in w/workloads [(:workload opts) :distribution])))
        opts (assoc opts :key-generator :ycsb-fnv64-decimal
                         :payload-indexes :none
                         :insert-order :hashed
                         :warmup-isolation :separate-database
                         :request-generator (if (= :zipfian (:distribution opts))
                                              :ycsb-scrambled-zipfian :committed-prefix))
        opts (if (= :zipfian (:distribution opts))
               (assoc opts :zipfian-keyspace
                      (or (:zipfian-keyspace opts)
                          (predicted-keyspace opts (:ops opts))))
               (dissoc opts :zipfian-keyspace))
        _ (when (some #{:all} ((juxt :system :api :mode :workload) opts))
            (throw (ex-info "run-case! requires a single system, API, mode and workload" {})))
        _ (when (and (= :datalevin (:system opts)) (= :datalog (:api opts))
                     (= :remote (:mode opts)) (= :both (:datalog-handles opts)))
            (throw (ex-info "run-case! requires a single Datalog handle mode" {})))
        opts (cond-> opts
               (not= :datalevin (:system opts))
               (assoc :batch-size 1 :sql-binding-model :upstream-jdbc-v1))
        _ (cases opts)
        warmup-opts (cond-> opts
                      (= :zipfian (:distribution opts))
                      (assoc :zipfian-keyspace (or (:zipfian-keyspace provided)
                                                  (predicted-keyspace opts (:warmup opts)))))
        warmup? (pos? (long (or (phase-duration opts :warmup) (:warmup opts))))]
    ((if (= :datalevin (:system opts)) store/with-stores sql/with-stores)
      opts
      (fn [with-fresh-store]
        (let [warmup (when warmup?
                       (with-fresh-store #(run-dataset! % warmup-opts :warmup (:warmup opts))))
              measured (with-fresh-store #(run-dataset! % opts :measured (:ops opts)))]
          (-> measured
              (dissoc :result)
              (assoc :configuration
                     (cond-> (dissoc opts :pg-url :pg-user :client-counts :repetitions)
                       (#{:sqlite :postgres} (:system opts))
                       (dissoc :pool-size)
                       (not (and (= :datalevin (:system opts))
                                 (= :datalog (:api opts)) (= :remote (:mode opts))))
                       (dissoc :datalog-handles))
                     :warmup (if warmup
                               (merge (:result warmup) (dissoc warmup :result))
                               (assoc (summarize (long-array 0) (byte-array 0) 0) :skipped? true))
                     :measured (:result measured))))))))
