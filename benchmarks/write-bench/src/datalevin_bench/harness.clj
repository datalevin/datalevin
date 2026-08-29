(ns datalevin-bench.harness
  "Completion-safe measurement support for the write benchmark."
  (:import
   [java.util Arrays Locale]
   [java.util.concurrent CountDownLatch ExecutionException Executors Semaphore
    TimeUnit]
   [java.util.concurrent.atomic AtomicIntegerArray AtomicLong AtomicReference]
   [org.eclipse.collections.impl.list.mutable FastList]))

(defn- positive-long!
  [label value]
  (when-not (and (integer? value) (pos? (long value)))
    (throw (ex-info (str label " must be a positive integer")
                    {:option label :value value})))
  (long value))

(defn- non-negative-long!
  [label value]
  (when-not (and (integer? value) (not (neg? (long value))))
    (throw (ex-info (str label " must be a non-negative integer")
                    {:option label :value value})))
  (long value))

(defn- positive-int!
  [label value]
  (let [value (positive-long! label value)]
    (when (> value Integer/MAX_VALUE)
      (throw (ex-info (str label " exceeds the supported integer range")
                      {:option label
                       :value value
                       :maximum Integer/MAX_VALUE})))
    value))

(defn- ceil-div
  ^long [^long numerator ^long denominator]
  (quot (+ numerator denominator -1) denominator))

(defn- set-max!
  [^AtomicLong target ^long value]
  (loop []
    (let [current (.get target)]
      (when (and (> value current)
                 (not (.compareAndSet target current value)))
        (recur)))))

(defn- format-number
  [format-string value]
  (String/format Locale/ROOT format-string (object-array [value])))

(defn- percentile-ns
  ^long [^longs sorted-values ^double percentile]
  (let [n   (alength sorted-values)
        idx (-> (* percentile n)
                Math/ceil
                long
                dec
                (max 0)
                (min (dec n)))]
    (aget sorted-values idx)))

(defn- latency-summary
  [^longs values]
  (let [n (alength values)]
    (when (zero? n)
      (throw (ex-info "Cannot summarize an empty latency sample" {})))
    (let [sorted (aclone values)
          sum    (loop [i 0
                        total 0.0]
                   (if (< i n)
                     (recur (inc i) (+ total (double (aget values i))))
                     total))]
      (Arrays/sort sorted)
      {:mean-ms (/ sum n 1000000.0)
       :p50-ms  (/ (double (percentile-ns sorted 0.50)) 1000000.0)
       :p95-ms  (/ (double (percentile-ns sorted 0.95)) 1000000.0)
       :p99-ms  (/ (double (percentile-ns sorted 0.99)) 1000000.0)})))

(defn- build-batch
  ^FastList [^long batch-size add-fn]
  (let [^FastList txs (FastList. (int batch-size))]
    (dotimes [_ (int batch-size)]
      (add-fn txs))
    txs))

(defn- print-progress!
  [^AtomicLong next-report
   ^long report-every
   ^long completed
   ^long total-writes]
  (when (pos? report-every)
    (loop []
      (let [threshold (.get next-report)]
        (when (and (<= threshold total-writes)
                   (>= completed threshold))
          (if (.compareAndSet next-report
                              threshold
                              (+ threshold report-every))
            (do
              (binding [*out* *err*]
                (println "Completed" (min completed total-writes)
                         "of" total-writes "writes"))
              (recur))
            (recur)))))))

(defn run-benchmark!
  "Run a saturated write workload and return final metrics.

  `tx-fn` receives `[txs completion-callback]`. The callback must be invoked
  exactly once with either the successful result or a Throwable. `add-fn`
  appends one transaction item to the supplied FastList."
  [{:keys [total-writes batch-size threads async? in-flight
           completion-timeout-ms report-every tx-fn add-fn]
    :or   {threads               1
           async?               false
           in-flight            1000
           completion-timeout-ms 600000
           report-every         10000}}]
  (let [total-writes          (positive-long! :total-writes total-writes)
        batch-size            (positive-int! :batch-size batch-size)
        threads               (positive-int! :threads threads)
        in-flight             (positive-int! :in-flight in-flight)
        completion-timeout-ms (positive-long! :completion-timeout-ms
                                               completion-timeout-ms)
        report-every          (non-negative-long! :report-every report-every)
        _                     (when-not (ifn? tx-fn)
                                (throw (ex-info ":tx-fn must be callable"
                                                {:option :tx-fn
                                                 :value tx-fn})))
        _                     (when-not (ifn? add-fn)
                                (throw (ex-info ":add-fn must be callable"
                                                {:option :add-fn
                                                 :value add-fn})))
        request-count         (ceil-div total-writes batch-size)
        _                     (when (> request-count Integer/MAX_VALUE)
                                (throw
                                  (ex-info "Too many benchmark requests"
                                           {:request-count request-count
                                            :maximum Integer/MAX_VALUE})))
        request-count         (int request-count)
        completion-latencies  (long-array request-count)
        call-latencies        (long-array request-count)
        completed-once        (AtomicIntegerArray. request-count)
        completed-writes      (AtomicLong. 0)
        next-report           (AtomicLong. (if (pos? report-every)
                                             report-every
                                             Long/MAX_VALUE))
        last-completion       (AtomicLong. 0)
        first-error           (AtomicReference. nil)
        remaining             (CountDownLatch. request-count)
        permits               (when async? (Semaphore. (int in-flight) true))
        next-request          (AtomicLong. 0)
        start-time            (System/nanoTime)
        complete!             (fn [^long idx
                                  ^long writes
                                  submitted-at
                                  payload]
                                (if (.compareAndSet completed-once
                                                    (int idx) 0 1)
                                  (let [now        (System/nanoTime)
                                        submitted? (some? submitted-at)]
                                    (aset-long completion-latencies
                                               (int idx)
                                               (if submitted?
                                                 (- now (long submitted-at))
                                                 0))
                                    (set-max! last-completion now)
                                    (try
                                      (if (instance? Throwable payload)
                                        (.compareAndSet first-error nil payload)
                                        (let [completed
                                              (.addAndGet completed-writes writes)]
                                          (print-progress! next-report
                                                           report-every
                                                           completed
                                                           total-writes)))
                                      (catch Throwable t
                                        (.compareAndSet first-error nil t))
                                      (finally
                                        (.countDown remaining)
                                        (when (and permits submitted?)
                                          (.release ^Semaphore permits)))))
                                  (.compareAndSet
                                    first-error
                                    nil
                                    (ex-info
                                      "Completion callback invoked more than once"
                                      {:request-index idx}))))
        fail-request!         (fn [^long idx
                                  ^long writes
                                  release-permit?
                                  ^Throwable error]
                                ;; Set the error independently of complete! so a
                                ;; malformed tx-fn that calls back successfully
                                ;; and then throws still invalidates the run.
                                (.compareAndSet first-error nil error)
                                (complete! idx writes
                                           (when release-permit?
                                             (System/nanoTime))
                                           error))
        submit-request!       (fn [^long idx
                                  ^long writes
                                  release-permit?]
                                (if-let [error (.get first-error)]
                                  (complete! idx writes
                                             (when release-permit?
                                               (System/nanoTime))
                                             error)
                                  (try
                                    (let [txs          (build-batch writes add-fn)
                                          submitted-at (System/nanoTime)
                                          callback     #(complete! idx
                                                                   writes
                                                                   submitted-at
                                                                   %)
                                          call-start   (System/nanoTime)]
                                      (try
                                        (tx-fn txs callback)
                                        (finally
                                          (aset-long call-latencies
                                                     (int idx)
                                                     (- (System/nanoTime)
                                                        call-start)))))
                                    (catch Throwable t
                                      (fail-request! idx writes
                                                     release-permit? t)))))
        worker                (fn []
                                (loop []
                                  (let [idx (.getAndIncrement next-request)]
                                    (when (< idx request-count)
                                      (let [offset       (* idx batch-size)
                                            writes       (min batch-size
                                                              (- total-writes
                                                                 offset))]
                                        (if-let [error (.get first-error)]
                                          (complete! idx writes
                                                     nil error)
                                          (if permits
                                            (try
                                              (if (.tryAcquire
                                                    permits
                                                    completion-timeout-ms
                                                    TimeUnit/MILLISECONDS)
                                                (try
                                                  (submit-request! idx writes true)
                                                  (catch Throwable t
                                                    (fail-request! idx writes
                                                                   true t)))
                                                (fail-request!
                                                  idx writes false
                                                  (ex-info
                                                    "Timed out waiting for an async benchmark permit"
                                                    {:in-flight in-flight
                                                     :timeout-ms
                                                     completion-timeout-ms
                                                     :completed-writes
                                                     (.get completed-writes)})))
                                              (catch Throwable t
                                                (fail-request! idx writes false t)))
                                            (submit-request! idx writes false)))
                                      (recur))))))]
    (if (= threads 1)
      (worker)
      (let [pool (Executors/newFixedThreadPool (int threads))]
        (try
          (let [workers (doall
                          (repeatedly (int threads)
                                      #(.submit pool ^Runnable worker)))]
            (doseq [future workers]
              (try
                (.get future)
                (catch ExecutionException e
                  (throw (.getCause e))))))
          (finally
            (.shutdown pool)
            (when-not (.awaitTermination pool 1 TimeUnit/MINUTES)
              (.shutdownNow pool))))))
    (when-not (.await remaining completion-timeout-ms TimeUnit/MILLISECONDS)
      (let [data  {:pending-requests (.getCount remaining)
                   :completed-writes (.get completed-writes)
                   :total-writes total-writes
                   :timeout-ms completion-timeout-ms}
            error (.get first-error)]
        (throw
          (if (instance? Throwable error)
            (ex-info "Timed out waiting for benchmark writes to complete"
                     data ^Throwable error)
            (ex-info "Timed out waiting for benchmark writes to complete"
                     data)))))
    (when-let [error (.get first-error)]
      (throw (ex-info "Benchmark write failed" {:type :benchmark/write-failed}
                      ^Throwable error)))
    (let [end-time       (.get last-completion)
          elapsed-ns    (max 1 (- end-time start-time))
          elapsed-sec   (/ (double elapsed-ns) 1000000000.0)
          written       (.get completed-writes)]
      (when-not (= total-writes written)
        (throw (ex-info "Benchmark completed an unexpected number of writes"
                        {:expected total-writes :actual written})))
      {:writes                 written
       :requests               request-count
       :elapsed-seconds        elapsed-sec
       :throughput-writes-sec  (/ written elapsed-sec)
       :call-latency           (latency-summary call-latencies)
       :completion-latency     (latency-summary completion-latencies)})))

(def csv-header
  "Phase,Writes,Requests,Time (seconds),Throughput (writes/second),Call Mean (milliseconds),Completion Mean (milliseconds),Completion P50 (milliseconds),Completion P95 (milliseconds),Completion P99 (milliseconds)")

(defn result->csv-row
  [phase {:keys [writes requests elapsed-seconds throughput-writes-sec
                 call-latency completion-latency]}]
  (str (name phase)
       "," writes
       "," requests
       "," (format-number "%.2f" elapsed-seconds)
       "," (format-number "%.2f" throughput-writes-sec)
       "," (format-number "%.4f" (:mean-ms call-latency))
       "," (format-number "%.4f" (:mean-ms completion-latency))
       "," (format-number "%.4f" (:p50-ms completion-latency))
       "," (format-number "%.4f" (:p95-ms completion-latency))
       "," (format-number "%.4f" (:p99-ms completion-latency))))

(defn print-result!
  ([phase result] (print-result! phase result true))
  ([phase result header?]
   (when header? (println csv-header))
   (println (result->csv-row phase result))))
