(ns datalevin-bench.core
  "Max write throughput benchmark"
  (:require
   [datalevin.async :as a]
   [datalevin.core :as d]
   [datalevin.constants :as c]
   [clojure.string :as s]
   [next.jdbc :as jdbc]
   [next.jdbc.sql :as sql])
  (:import
   [datalevin.io PosixFsync]
   [java.util Random]
   [java.util.concurrent Semaphore Executors TimeUnit]
   [java.util.concurrent.atomic AtomicLong AtomicReference]
   [java.util.function Supplier]
   [org.eclipse.collections.impl.list.mutable FastList]))

;; for kv
(def max-write-dbi "test")

;; limit the number of threads in flight
(def in-flight 1000)

;; total number of writes for a task
(def total 1000000)

;; integer key range
(def keyspace (* 2 total))

;; measure every this number of writes, also when to deref futures for async
(def report 10000)

(defn print-header []
  (println
    "Number of Writes,Time (seconds),Throughput (writes/second),Call Latency (milliseconds),Commit Latency (milliseconds)"))

(defn- ns->seconds
  ^double [^long ns]
  (/ (double ns) 1000000000.0))

(defn print-row
  [written inserted write-time sync-count sync-time prev-time start-time]
  (let [duration-ns (- (long @sync-time) (long start-time))
        duration-sec (ns->seconds duration-ns)
        call-latency-ms (double
                          (/ (double @write-time)
                             (double @sync-count)
                             1000000.0))
        commit-latency-ms (double
                            (/ (double (- (long @sync-time)
                                          (long @prev-time)))
                               (double @sync-count)
                               1000000.0))]
    (println
      (str
        written
        ","
        (format "%.2f" duration-sec)
        ","
        (format "%.2f" (double (/ @inserted duration-sec)))
        ","
        (format "%.2f" call-latency-ms)
        ","
        (format "%.2f" commit-latency-ms)))))

(defn- build-batch-txs
  ^FastList [^long batch-size add-fn]
  (when add-fn
    (let [^FastList txs (FastList. (int batch-size))]
      (dotimes [_ (int batch-size)]
        (add-fn txs))
      txs)))

(defn max-write-bench
  [batch-size tx-fn add-fn async?]
  (print-header)
  (let [sem        (Semaphore. (* in-flight batch-size))
        write-time (volatile! 0)
        sync-count (volatile! 0)
        inserted   (volatile! 0)
        start-time (System/nanoTime)
        prev-time  (volatile! start-time)
        sync-time  (volatile! start-time)
        measure    (fn [_]
                     (.release sem batch-size)
                     (vreset! sync-time (System/nanoTime))
                     (vswap! sync-count inc)
                     (vswap! inserted + batch-size))]
    (loop [counter 0
           fut     nil]
      (let [written (* counter batch-size)]
        (if (< written total)
          (do
            (.acquire sem batch-size)
            (when (and (= 0 (mod written report))
                       (not= 0 counter)
                       (not= 0 @sync-count))
              (when async? @fut)
              (print-row (* counter batch-size) inserted write-time sync-count
                         sync-time prev-time start-time)
              (vreset! write-time 0)
              (vreset! prev-time @sync-time)
              (vreset! sync-count 0))
            (let [txs    (build-batch-txs batch-size add-fn)
                  before (System/nanoTime)
                  fut    (tx-fn txs measure)]
              (vswap! write-time + (- (System/nanoTime) before))
              (recur (inc counter) fut)))
          (do
            (when async? @fut)
            (print-row written inserted write-time sync-count
                       sync-time prev-time start-time)))))))

(defn max-write-bench-sync-mt
  [batch-size tx-fn add-fn threads async?]
  (print-header)
  (let [batch-size   (long batch-size)
        threads      (long threads)
        sem          (when async?
                       (Semaphore. (int (* in-flight batch-size))))
        latest-fut   (when async? (AtomicReference. nil))
        write-time   (volatile! 0)
        sync-count   (volatile! 0)
        inserted     (volatile! 0)
        timeout-count (volatile! 0)
        start-time   (System/nanoTime)
        prev-time    (volatile! start-time)
        sync-time    (volatile! start-time)
        next-report  (volatile! report)
        metrics-lock (Object.)
        counter      (AtomicLong. 0)
        measure      (fn [_]
                       (when sem
                         (.release ^Semaphore sem (int batch-size)))
                       (locking metrics-lock
                         (vreset! sync-time (System/nanoTime))
                         (vswap! sync-count inc)
                         (vswap! inserted + batch-size)
                         (when (and (>= @inserted @next-report)
                                    (pos? @sync-count))
                           (print-row @inserted inserted write-time sync-count
                                      sync-time prev-time start-time)
                           (vreset! write-time 0)
                           (vreset! prev-time @sync-time)
                           (vreset! sync-count 0)
                           (vreset! next-report (+ @next-report report)))))
        worker       (fn []
                       (loop []
                         (let [idx (.getAndIncrement counter)
                               written (* idx batch-size)]
                           (when (< written total)
                             (when sem
                               (.acquire ^Semaphore sem (int batch-size)))
                             (let [txs    (build-batch-txs batch-size add-fn)
                                   before (System/nanoTime)]
                               (try
                                 (let [fut (tx-fn txs measure)]
                                   (when latest-fut
                                     (loop []
                                       (let [cur (.get ^AtomicReference latest-fut)]
                                         (if (or (nil? cur)
                                                 (> ^long idx ^long (:idx cur)))
                                           (when-not (.compareAndSet
                                                       ^AtomicReference latest-fut
                                                       cur
                                                       {:idx idx :fut fut})
                                             (recur))
                                           nil))))
                                   (locking metrics-lock
                                     (vswap! write-time
                                             +
                                             (- (System/nanoTime)
                                                before))))
                                 (catch Throwable t
                                   (when sem
                                     (.release ^Semaphore sem (int batch-size)))
                                   (if (= :txlog/commit-timeout
                                          (:type (ex-data t)))
                                     ;; Transient strict-mode timeout: store
                                     ;; is still healthy, just skip this write
                                     (locking metrics-lock
                                       (vswap! timeout-count inc)
                                       (vswap! write-time
                                               +
                                               (- (System/nanoTime) before)))
                                     (throw t)))))
                             (recur)))))]
    (if (= threads 1)
      (worker)
      (let [pool (Executors/newFixedThreadPool (int threads))]
        (try
          (let [workers (doall
                          (repeatedly (int threads)
                                      #(.submit pool ^Runnable worker)))]
            (doseq [w workers] (.get w)))
          (finally
            (.shutdown pool)
            (.awaitTermination pool 1 TimeUnit/MINUTES)))))
    (when latest-fut
      (when-let [fut (:fut (.get ^AtomicReference latest-fut))]
        @fut))
    (when (pos? @sync-count)
      (print-row @inserted inserted write-time sync-count
                 sync-time prev-time start-time))
    (when (pos? @timeout-count)
      (binding [*out* *err*]
        (println "Commit timeouts (strict mode contention):" @timeout-count)))))

(def id (AtomicLong. 0))

(defn- effective-durability-profile
  [wal? durability-profile]
  (when wal?
    (or durability-profile :strict)))

(defn- run-dir
  ([base-dir f batch]
   (run-dir base-dir f batch 1 nil))
  ([base-dir f batch threads]
   (run-dir base-dir f batch threads nil))
  ([base-dir f batch threads durability-profile]
   (let [suffix (if (> (long threads) 1)
                  (str "-t" threads)
                  "")
         profile-suffix (if durability-profile
                          (str "-" (name durability-profile))
                          "")
         name   (str f "-" batch suffix profile-suffix)]
    (if (and (string? base-dir) (not (s/blank? base-dir)))
      (str (if (s/ends-with? base-dir "/")
             base-dir
             (str base-dir "/"))
           name)
      name))))

(defn- wal-task? [nf] (s/ends-with? nf "-wal"))

(defn- base-task
  [nf]
  (if (s/ends-with? nf "-wal")
    (subs nf 0 (- (count nf) 4))
    nf))

(def ^:private valid-durability-profiles #{:strict :extra :relaxed})

(defn- validate-durability-profile!
  [durability-profile]
  (when (and durability-profile
             (not (valid-durability-profiles durability-profile)))
    (throw (ex-info
             ":durability-profile must be one of :strict, :extra, or :relaxed"
             {:durability-profile durability-profile}))))

(defn- sql-sync-mode-for
  [durability-profile]
  (case durability-profile
    :relaxed "NORMAL"
    :extra   "EXTRA"
    "FULL"))

(defn- ensure-native-wal-sync-path!
  [nf wal? kv? dl?]
  (when (and wal? (or kv? dl?) (not (PosixFsync/available)))
    (throw
      (ex-info
        (str "Native WAL fsync path unavailable for " nf
             "; falling back to FileChannel.force(false) would skew this benchmark. "
             "Run with --add-opens=java.base/sun.nio.ch=ALL-UNNAMED "
             "(for example, use the :write/:mixed aliases).")
        {:task nf
         :wal? wal?
         :posix-fsync-available? false}))))

(defn write
  [{:keys [base-dir batch f threads durability-profile]
    :or   {threads 1}}]
  (let [nf       (name f)
        base-nf  (base-task nf)
        wal?     (wal-task? nf)
        threads  (long threads)
        effective-profile (effective-durability-profile wal? durability-profile)
        _        (when (not (pos? threads))
                   (throw (ex-info ":threads must be a positive integer"
                                   {:threads threads})))
        _        (validate-durability-profile! durability-profile)
        sql-journal-mode (if wal? "WAL" "DELETE")
        sql-sync-mode (sql-sync-mode-for effective-profile)
        dir      (run-dir base-dir f batch threads effective-profile)
        kv?      (s/starts-with? base-nf "kv")
        dl?      (s/starts-with? base-nf "dl")
        sql?     (s/starts-with? base-nf "sql")
        _        (ensure-native-wal-sync-path! nf wal? kv? dl?)
        async?   (s/ends-with? base-nf "async")
        _        (when (and (string? base-dir) (not (s/blank? base-dir)))
                   (.mkdirs (java.io.File. base-dir)))
        kvdb     (when kv?
                   (doto (d/open-kv dir
                                    (cond-> (assoc {:mapsize 60000
                                                    :flags   (-> c/default-env-flags
                                                                 ;; (conj :writemap)
                                                                 ;; (conj :mapasync)
                                                                 ;; (conj :nosync)
                                                                 ;; (conj :nometasync)
                                                                 )}
                                                   :wal? wal?)
                                      (and wal? effective-profile)
                                      (assoc :wal-durability-profile
                                             effective-profile)))
                     (d/open-dbi max-write-dbi)))
        kv-async (fn [txs measure]
                   (d/transact-kv-async kvdb max-write-dbi txs
                                        :id :string measure))
        kv-sync  (fn [txs measure]
                   (measure (d/transact-kv kvdb max-write-dbi txs
                                           :id :string)))
        kv-add   (fn [^FastList txs]
                   (.add txs [:put (.addAndGet id 2) (str (random-uuid))]))
        conn     (when dl?
                   (d/get-conn
                     dir
                     {:k {:db/valueType :db.type/long}
                      :v {:db/valueType :db.type/string}}
                     (cond-> (assoc {:kv-opts {:mapsize 60000
                                               :flags   (-> c/default-env-flags
                                                            ;; (conj :writemap)
                                                            ;; (conj :mapasync)
                                                            ;; (conj :nosync)
                                                            ;; (conj :nometasync)
                                                            )}}
                                    :wal? wal?)
                       (and wal? effective-profile)
                       (assoc :wal-durability-profile
                              effective-profile))))
        dl-async (fn [txs measure] (d/transact-async conn txs nil measure))
        dl-sync  (fn [txs measure] (measure (d/transact! conn txs nil)))
        dl-add   (fn [^FastList txs]
                   (.add txs {:k (.addAndGet id 2) :v (str (random-uuid))}))
        sql-dir   (when sql?
                    (run-dir base-dir
                             (if wal? "sqlite-wal" "sqlite")
                             batch
                             threads
                             effective-profile))
        sql-spec  (when sql?
                    {:dbtype "sqlite" :dbname sql-dir})
        sql-conn  (when (and sql? (= threads 1))
                    (let [conn (jdbc/get-connection sql-spec)]
                      (jdbc/execute!
                        conn
                        [(str "PRAGMA journal_mode=" sql-journal-mode ";")])
                      (jdbc/execute!
                        conn
                        [(str "PRAGMA synchronous=" sql-sync-mode ";")])
                      (jdbc/execute! conn ["CREATE TABLE IF NOT EXISTS my_table (
                      k INTEGER PRIMARY KEY, v TEXT)"])
                      conn))
        sql-conns (atom [])
        sql-tl    (when (and sql? (> threads 1))
                    (with-open [conn (jdbc/get-connection sql-spec)]
                      (jdbc/execute!
                        conn
                        [(str "PRAGMA journal_mode=" sql-journal-mode ";")])
                      (jdbc/execute!
                        conn
                        [(str "PRAGMA synchronous=" sql-sync-mode ";")])
                      (jdbc/execute! conn ["CREATE TABLE IF NOT EXISTS my_table (
                      k INTEGER PRIMARY KEY, v TEXT)"]))
                    (let [mt-spec {:jdbcUrl (str "jdbc:sqlite:" sql-dir
                                                 "?busy_timeout=10000")}]
                      (ThreadLocal/withInitial
                        (reify Supplier
                          (get [_]
                            (let [conn (jdbc/get-connection mt-spec)]
                              (jdbc/execute!
                                conn
                                [(str "PRAGMA journal_mode="
                                      sql-journal-mode
                                      ";")])
                              (jdbc/execute!
                                conn
                                [(str "PRAGMA synchronous=" sql-sync-mode ";")])
                              (swap! sql-conns conj conn)
                              conn))))))
        sql-tx    (if sql-tl
                    (fn [txs measure]
                      (measure (sql/insert-multi!
                                 (.get ^ThreadLocal sql-tl) :my_table txs)))
                    (fn [txs measure]
                      (measure (sql/insert-multi! sql-conn :my_table txs))))
        sql-add  (fn [^FastList txs]
                   (.add txs {:k (.addAndGet id 2) :v (str (random-uuid))}))
        tx-fn    (case base-nf
                   "kv-async" kv-async
                   "kv-sync"  kv-sync
                   "dl-async" dl-async
                   "dl-sync"  dl-sync
                   "sql-tx"   sql-tx
                   (throw (ex-info "Unsupported write benchmark task"
                                   {:f f :base-task base-nf})))
        add-fn   (cond
                   kv?  kv-add
                   dl?  dl-add
                   sql? sql-add)]
    (cond
      (= threads 1)
      (max-write-bench batch tx-fn add-fn async?)

      (and dl? (not wal?))
      (throw
        (ex-info
          "Multi-thread write benchmark for Datalog transact! requires WAL mode (use dl-sync-wal)."
          {:threads threads :f f}))

      :else
      (max-write-bench-sync-mt batch tx-fn add-fn threads async?))
    (when kvdb
      (let [written (d/entries kvdb max-write-dbi)]
        (when-not (= written total) (println "Write only" written)))
      (d/close-kv kvdb))
    (when conn
      (let [db              (d/db conn)
            user-datoms     (+ (d/count-datoms db nil :k nil)
                               (d/count-datoms db nil :v nil))
            expected-datoms (* 2 total)]
        (when-not (= user-datoms expected-datoms)
          (println "Write only" user-datoms)))
      (d/close conn))
    (when (or sql-conn sql-tl)
      (let [verify-conn (or sql-conn (jdbc/get-connection sql-spec))
            written     (-> (jdbc/execute! verify-conn
                                           ["SELECT count(1) FROM my_table"])
                            ffirst
                            val)]
        (when-not (= written total) (println "Write only" written))
        (when sql-conn (.close sql-conn))
        (when sql-tl
          (.close verify-conn)
          (doseq [c @sql-conns] (.close c))))
      (a/shutdown-executor))))

(def random (Random.))

(defn random-int [] (.nextInt random keyspace))

(defn mixed
  [{:keys [dir f durability-profile]}]
  (let [nf       (name f)
        base-nf  (base-task nf)
        wal?     (wal-task? nf)
        sql-journal-mode (if wal? "WAL" "DELETE")
        sql-sync-mode    (sql-sync-mode-for durability-profile)
        _        (validate-durability-profile! durability-profile)
        kv?      (s/starts-with? base-nf "kv")
        dl?      (s/starts-with? base-nf "dl")
        sql?     (s/starts-with? base-nf "sql")
        _        (ensure-native-wal-sync-path! nf wal? kv? dl?)
        kvdb     (when kv?
                   (doto (d/open-kv dir
                                    (cond-> (assoc {:mapsize 60000
                                                    :flags   (-> c/default-env-flags
                                                                 ;; (conj :writemap)
                                                                 ;; (conj :mapasync)
                                                                 ;; (conj :nosync)
                                                                 ;; (conj :nometasync)
                                                                 )}
                                                   :wal? wal?)
                                      (and wal? durability-profile)
                                      (assoc :wal-durability-profile
                                             durability-profile)))
                     (d/open-dbi max-write-dbi)))
        kv-async (fn [txs measure]
                   (d/get-value kvdb max-write-dbi (random-int) :id :string)
                   (d/transact-kv-async kvdb max-write-dbi txs
                                        :id :string measure))
        kv-sync  (fn [txs measure]
                   (d/get-value kvdb max-write-dbi (random-int) :id :string)
                   (measure (d/transact-kv kvdb max-write-dbi txs
                                           :id :string)))
        kv-add   (fn [^FastList txs]
                   (.add txs [:put (random-int) (str (random-uuid))]))
        conn     (when dl?
                   (d/get-conn
                     dir
                     {:k {:db/valueType :db.type/long}
                      :v {:db/valueType :db.type/string}}
                     (cond-> (assoc {:kv-opts {:mapsize 60000
                                               :flags   (-> c/default-env-flags
                                                            ;; (conj :writemap)
                                                            ;; (conj :mapasync)
                                                            ;; (conj :nosync)
                                                            ;; (conj :nometasync)
                                                            )}}
                                    :wal? wal?)
                       (and wal? durability-profile)
                       (assoc :wal-durability-profile durability-profile))))
        query    '[:find (pull ?e [:v])
                   :in $ ?k
                   :where [?e :k ?k]]
        dl-async (fn [txs measure]
                   (d/q query (d/db conn) (random-int))
                   (d/transact-async conn txs nil measure))
        dl-sync  (fn [txs measure]
                   (d/q query (d/db conn) (random-int))
                   (measure (d/transact! conn txs nil)))
        dl-add   (fn [^FastList txs]
                   (.add txs {:k (random-int) :v (str (random-uuid))}))
        sql-conn (when sql?
                   (let [conn (jdbc/get-connection {:dbtype "sqlite"
                                                    :dbname dir})]
                     (jdbc/execute!
                       conn
                       [(str "PRAGMA journal_mode=" sql-journal-mode ";")])
                     (jdbc/execute!
                       conn
                       [(str "PRAGMA synchronous=" sql-sync-mode ";")])
                     (jdbc/execute! conn
                                    ["CREATE TABLE IF NOT EXISTS my_table (
                     k INTEGER PRIMARY KEY, v TEXT)"])
                     conn))
        tx       "INSERT OR REPLACE INTO my_table (k, v) values (?, ?)"
        sql-tx   (fn [txs measure]
                   (jdbc/execute-one! sql-conn
                                      ["SELECT v FROM my_table WHERE k = ?"
                                       (random-int)])
                   (let [vs (first txs)]
                     (measure (jdbc/execute! sql-conn [tx (first vs) (peek vs)]))))
        sql-add  (fn [^FastList txs]
                   (.add txs [(random-int) (str (random-uuid))]))
        tx-fn    (case base-nf
                   "kv-async" kv-async
                   "kv-sync"  kv-sync
                   "dl-async" dl-async
                   "dl-sync"  dl-sync
                   "sql-tx"   sql-tx
                   (throw (ex-info "Unsupported mixed benchmark task"
                                   {:f f :base-task base-nf})))
        add-fn   (cond
                   kv?  kv-add
                   dl?  dl-add
                   sql? sql-add)]
    (max-write-bench 1 tx-fn add-fn false)
    (when kvdb (d/close-kv kvdb))
    (when conn (d/close conn))
    (when sql-conn (.close sql-conn))
    (a/shutdown-executor)))

(defn dl-init
  [{:keys [dir]}]
  (let [es      (range 1 (inc total))
        datoms1 (mapv (fn [e k] (d/datom e :k k))
                      es (repeatedly total random-int))
        datoms2 (mapv (fn [e v] (d/datom e :v v))
                      es (repeatedly total #(str (random-uuid))))
        start   (System/currentTimeMillis)
        db      (-> (d/init-db datoms1 dir {:k {:db/valueType :db.type/long}
                                            :v {:db/valueType :db.type/string}}
                               {:kv-opts {:mapsize 60000}})
                    (d/fill-db datoms2))]
    (println "took" (- (System/currentTimeMillis) start) "milliseconds")
    (d/close-db db)))
