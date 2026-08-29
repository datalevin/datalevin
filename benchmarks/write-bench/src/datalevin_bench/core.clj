(ns datalevin-bench.core
  "Write-throughput benchmark for Datalevin and SQLite."
  (:require
   [clojure.java.io :as io]
   [clojure.pprint :as pprint]
   [clojure.string :as s]
   [datalevin-bench.harness :as h]
   [datalevin.constants :as c]
   [datalevin.core :as d]
   [next.jdbc :as jdbc])
  (:import
   [datalevin.cpp Util]
   [datalevin.io PosixFsync]
   [java.lang.management ManagementFactory]
   [java.sql Connection PreparedStatement]
   [java.time Instant]
   [java.util BitSet Random UUID]
   [java.util.concurrent.atomic AtomicLong]
   [java.util.function Supplier]
   [org.eclipse.collections.impl.list.mutable FastList]))

(def max-write-dbi "test")
(def total 1000000)
(def report 10000)
(def in-flight 1000)
(def in-flight-writes 100000)
(def completion-timeout-ms 600000)
(def default-seed 42)

(def ^:private person-schema
  {:person/id         {:db/valueType :db.type/string
                       :db/unique    :db.unique/identity}
   :person/first-name {:db/valueType :db.type/string}
   :person/last-name  {:db/valueType :db.type/string}
   :person/age        {:db/valueType :db.type/long}})

(def ^:private sqlite-person-table-ddl
  (str "CREATE TABLE person ("
       "person_id TEXT PRIMARY KEY NOT NULL, "
       "first_name TEXT NOT NULL, "
       "last_name TEXT NOT NULL, "
       "age INTEGER NOT NULL)"))

(def ^:private sqlite-person-insert-sql
  (str "INSERT INTO person "
       "(person_id, first_name, last_name, age) "
       "VALUES (?, ?, ?, ?)"))

(deftype ^:private SQLiteBatchWriter
  [^Connection conn ^PreparedStatement statement])

(def ^:private workload-description
  {:name :small-person-record
   :identity {:field :person/id :type :string :format :uuid}
   :fields [[:person/first-name :string]
            [:person/last-name :string]
            [:person/age :long]]})

(def ^:private first-names
  ["Avery" "Camila" "Charlotte" "Daniel" "Elijah" "Emma" "Ethan" "Grace"
   "Henry" "Isabella" "Jack" "James" "Layla" "Leo" "Lucas" "Maya"
   "Mia" "Noah" "Nora" "Oliver" "Olivia" "Owen" "Priya" "Ravi"
   "Sofia" "Sophia" "Theodore" "Violet" "William" "Xavier" "Yara" "Zoe"])

(def ^:private last-names
  ["Adams" "Ali" "Anderson" "Baker" "Brown" "Campbell" "Carter" "Chen"
   "Clark" "Davis" "Evans" "Garcia" "Green" "Hall" "Harris" "Hernandez"
   "Hill" "Jackson" "Johnson" "Jones" "Khan" "Kim" "Lee" "Lewis"
   "Martin" "Martinez" "Miller" "Moore" "Murphy" "Nguyen" "Patel" "Perez"
   "Ramirez" "Reed" "Roberts" "Robinson" "Rodriguez" "Sanchez" "Scott"
   "Singh" "Smith" "Taylor" "Thomas" "Thompson" "Turner" "Walker" "Wang"
   "White" "Williams" "Wilson" "Wong" "Wood" "Wright" "Young"])

(def ^:private valid-base-tasks
  #{"kv-async" "kv-sync" "dl-async" "dl-sync" "sql-tx"})

(def ^:private valid-durability-profiles #{:strict :extra :relaxed})

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

(defn- effective-in-flight-limit
  [batch request-limit write-limit]
  (int (min (long request-limit)
            (max 1 (quot (long write-limit) (long batch))))))

(defn- positive-int!
  [label value]
  (let [value (positive-long! label value)]
    (when (> value Integer/MAX_VALUE)
      (throw (ex-info (str label " exceeds the supported integer range")
                      {:option label
                       :value value
                       :maximum Integer/MAX_VALUE})))
    value))

(defn- integer-long!
  [label value]
  (when-not (integer? value)
    (throw (ex-info (str label " must be an integer")
                    {:option label :value value})))
  (try
    (long value)
    (catch ArithmeticException cause
      (throw (ex-info (str label " exceeds the supported long range")
                      {:option label :value value}
                      cause)))))

(defn- required-path!
  [label value]
  (when-not (and (string? value) (not (s/blank? value)))
    (throw (ex-info (str label " must be a non-blank path")
                    {:option label :value value})))
  value)

(defn- wal-task?
  [task-name]
  (s/ends-with? task-name "-wal"))

(defn- base-task
  [task-name]
  (if (wal-task? task-name)
    (subs task-name 0 (- (count task-name) 4))
    task-name))

(defn- task-info
  [f]
  (when (nil? f)
    (throw (ex-info ":f is required" {:option :f})))
  (let [task-name (name f)
        base-name (base-task task-name)
        wal?      (wal-task? task-name)]
    (when-not (valid-base-tasks base-name)
      (throw (ex-info "Unsupported write benchmark task"
                      {:f f
                       :allowed (sort
                                  (mapcat #(vector % (str % "-wal"))
                                          valid-base-tasks))})))
    {:name    task-name
     :base    base-name
     :wal?    wal?
     :async?  (s/ends-with? base-name "async")
     :kind    (cond
                (s/starts-with? base-name "kv-") :kv
                (s/starts-with? base-name "dl-") :datalog
                :else :sqlite)}))

(defn- validate-durability-profile!
  [wal? durability-profile]
  (when (and durability-profile
             (not (valid-durability-profiles durability-profile)))
    (throw (ex-info
             ":durability-profile must be one of :strict, :extra, or :relaxed"
             {:durability-profile durability-profile})))
  (when (and durability-profile (not wal?))
    (throw (ex-info ":durability-profile is only valid for WAL tasks"
                    {:durability-profile durability-profile :wal? false}))))

(defn- effective-durability-profile
  [wal? durability-profile]
  (when wal? (or durability-profile :relaxed)))

(defn- ensure-native-wal-sync-path!
  [{:keys [name wal? kind]}]
  (when (and wal?
             (#{:kv :datalog} kind)
             (not (PosixFsync/available)))
    (throw
      (ex-info
        (str "Native WAL fsync path unavailable for " name
             "; falling back to FileChannel.force(false) would skew this benchmark. "
             "Use the benchmark aliases so sun.nio.ch is opened to Datalevin.")
        {:task name :wal? true :posix-fsync-available? false}))))

(defn- validate-common!
  [{:keys [batch threads total-writes report-every in-flight-limit
           in-flight-write-limit
           completion-timeout]} info durability-profile]
  (positive-int! :batch batch)
  (positive-int! :threads threads)
  (positive-long! :total total-writes)
  (non-negative-long! :report report-every)
  (positive-int! :in-flight in-flight-limit)
  (positive-long! :in-flight-writes in-flight-write-limit)
  (positive-long! :completion-timeout-ms completion-timeout)
  (validate-durability-profile! (:wal? info) durability-profile)
  (when (and (> (long threads) 1)
             (= :datalog (:kind info))
             (not (:wal? info)))
    (throw
      (ex-info "Multi-thread Datalog writes require WAL mode"
               {:threads threads :f (:name info)})))
  (ensure-native-wal-sync-path! info))

(defn- run-dir
  [base-dir f batch threads durability-profile]
  (let [thread-suffix  (if (> (long threads) 1) (str "-t" threads) "")
        profile-suffix (if durability-profile
                         (str "-" (name durability-profile))
                         "")
        dir-name       (str (name f) "-" batch
                            thread-suffix profile-suffix)]
    (if (and (string? base-dir) (not (s/blank? base-dir)))
      (.getPath (io/file base-dir dir-name))
      dir-name)))

(defn- benchmark-target
  [base-dir {:keys [name kind wal?]} batch threads durability-profile]
  (run-dir base-dir
           (if (= kind :sqlite)
             (if wal? "sqlite-wal" "sqlite")
             name)
           batch
           threads
           durability-profile))

(defn- ensure-parent-directory!
  [path]
  (when-let [parent (.getParentFile (io/file path))]
    (when-not (or (.isDirectory parent) (.mkdirs parent))
      (throw (ex-info "Unable to create benchmark parent directory"
                      {:path (.getPath parent)})))))

(defn- sqlite-sidecar-paths
  [path]
  [path (str path "-journal") (str path "-wal") (str path "-shm")])

(defn- ensure-fresh-target!
  [path sqlite?]
  (let [paths (if sqlite? (sqlite-sidecar-paths path) [path])]
    (when-let [existing (some #(when (.exists (io/file %)) %) paths)]
      (throw
        (ex-info "Benchmark target already exists; use a fresh directory"
                 {:target path :existing existing}))))
  (ensure-parent-directory! path))

(defn- ensure-existing-target!
  [path]
  (when-not (.exists (io/file path))
    (throw (ex-info "Mixed benchmark target does not exist"
                    {:target path}))))

(defn- mix64
  ^long [^long value]
  (let [value (unchecked-multiply
                (bit-xor value (unsigned-bit-shift-right value 30))
                -4658895280553007687)
        value (unchecked-multiply
                (bit-xor value (unsigned-bit-shift-right value 27))
                -7723592293110705685)]
    (bit-xor value (unsigned-bit-shift-right value 31))))

(defn- deterministic-uuid
  [^long seed ^long sequence-number]
  (let [x (unchecked-add seed sequence-number)
        y (unchecked-add seed
                         (unchecked-multiply sequence-number
                                             -7046029254386353131))]
    (str (UUID. (mix64 x) (mix64 y)))))

(defn- person-id
  [^long seed ^long key-slot]
  (deterministic-uuid seed key-slot))

(defn- person-record
  [^long seed ^long key-slot ^long record-version]
  (let [name-hash (mix64
                    (unchecked-add
                      (unchecked-add seed key-slot)
                      (unchecked-multiply record-version
                                          -7046029254386353131)))
        age-hash  (mix64 (unchecked-add name-hash 7640891576956012809))]
    {:person-id (person-id seed key-slot)
     :first-name (nth first-names
                      (int (mod name-hash (count first-names))))
     :last-name (nth last-names
                     (int (mod age-hash (count last-names))))
     :age (long (+ 18 (mod (unsigned-bit-shift-right age-hash 8) 73)))}))

(defn- pure-person-generator
  [seed]
  (let [sequence-number (AtomicLong. 0)]
    (fn []
      (let [sequence-number (.incrementAndGet sequence-number)]
        (person-record (long seed)
                       (unchecked-multiply 2 sequence-number)
                       sequence-number)))))

(defn- person-update-generator
  [seed]
  (let [record-version (AtomicLong. 0)]
    (fn [key-slot]
      (person-record (long seed) (long key-slot)
                     (.incrementAndGet record-version)))))

(defn- person-value
  [{:keys [first-name last-name age]}]
  {:first-name first-name
   :last-name  last-name
   :age        age})

(defn- datalog-person
  [{:keys [person-id first-name last-name age]}]
  {:person/id         person-id
   :person/first-name first-name
   :person/last-name  last-name
   :person/age        age})

(defn- sqlite-person
  [{:keys [person-id first-name last-name age]}]
  {:person_id person-id
   :first_name first-name
   :last_name last-name
   :age age})

(defn- store-opts
  [wal? durability-profile]
  (cond-> {:mapsize 60000
           :flags   c/default-env-flags
           :wal?    wal?}
    (and wal? durability-profile)
    (assoc :wal-durability-profile durability-profile)))

(defn- datalog-opts
  [wal? durability-profile]
  (cond-> {:kv-opts {:mapsize 60000
                     :flags   c/default-env-flags}
           :wal?    wal?}
    (and wal? durability-profile)
    (assoc :wal-durability-profile durability-profile)))

(defn- sql-sync-mode-for
  [durability-profile]
  (case durability-profile
    :relaxed "NORMAL"
    :extra   "EXTRA"
    "FULL"))

(def ^:private sql-sync-values
  {"OFF" 0 "NORMAL" 1 "FULL" 2 "EXTRA" 3})

(defn- result-value
  [row]
  (when (map? row) (first (vals row))))

(defn- sqlite-pragma
  [conn pragma]
  (result-value (jdbc/execute-one! conn [(str "PRAGMA " pragma ";")])))

(defn- configure-sqlite!
  [conn journal-mode sync-mode durability-profile]
  (let [actual-journal (-> (jdbc/execute-one!
                             conn
                             [(str "PRAGMA journal_mode=" journal-mode ";")])
                           result-value
                           str
                           s/lower-case)
        expected-journal (s/lower-case journal-mode)]
    (when-not (= expected-journal actual-journal)
      (throw (ex-info "SQLite did not enter the requested journal mode"
                      {:expected expected-journal :actual actual-journal}))))
  (jdbc/execute! conn [(str "PRAGMA synchronous=" sync-mode ";")])
  (let [fullsync? (= :extra durability-profile)]
    (jdbc/execute! conn [(str "PRAGMA fullfsync="
                              (if fullsync? "ON" "OFF") ";")])
    (jdbc/execute! conn [(str "PRAGMA checkpoint_fullfsync="
                              (if fullsync? "ON" "OFF") ";")])
    (let [actual-sync           (long (sqlite-pragma conn "synchronous"))
          expected-sync         (long (sql-sync-values sync-mode))
          actual-fullsync       (long (sqlite-pragma conn "fullfsync"))
          actual-checkpoint-sync
          (long (sqlite-pragma conn "checkpoint_fullfsync"))]
      (when-not (= expected-sync actual-sync)
        (throw (ex-info "SQLite did not enter the requested synchronous mode"
                        {:expected expected-sync :actual actual-sync})))
      (when-not (= (if fullsync? 1 0) actual-fullsync)
        (throw (ex-info "SQLite did not enter the requested fullfsync mode"
                        {:expected fullsync? :actual actual-fullsync})))
      (when-not (= (if fullsync? 1 0) actual-checkpoint-sync)
        (throw
          (ex-info "SQLite did not enter the requested checkpoint fullsync mode"
                   {:expected fullsync?
                    :actual actual-checkpoint-sync})))
      {:journal-mode         (s/lower-case journal-mode)
       :synchronous          (s/lower-case sync-mode)
       :fullfsync            fullsync?
       :checkpoint-fullfsync fullsync?
       :busy-timeout-ms      (long (sqlite-pragma conn "busy_timeout"))})))

(defn- configure-sqlite-busy-timeout!
  [conn timeout-ms]
  (jdbc/execute! conn [(str "PRAGMA busy_timeout=" (long timeout-ms) ";")])
  (let [actual (long (sqlite-pragma conn "busy_timeout"))]
    (when-not (= (long timeout-ms) actual)
      (throw (ex-info "SQLite did not enter the requested busy timeout"
                      {:expected timeout-ms :actual actual})))
    actual))

(defn- sqlite-version
  [conn]
  (str (result-value
         (jdbc/execute-one! conn ["SELECT sqlite_version() AS version"]))))

(defn- open-sqlite-connection
  [spec journal-mode sync-mode durability-profile]
  (let [conn (jdbc/get-connection spec)]
    (try
      {:conn   conn
       :config (configure-sqlite! conn journal-mode sync-mode
                                  durability-profile)}
      (catch Throwable t
        (.close conn)
        (throw t)))))

(defn- sqlite-count
  [conn]
  (long (result-value
          (jdbc/execute-one! conn ["SELECT count(1) AS n FROM person"]))))

(defn- create-sqlite-person-table!
  [conn]
  (jdbc/execute! conn [sqlite-person-table-ddl]))

(defn- open-sqlite-batch-writer
  ^SQLiteBatchWriter [^Connection conn]
  (.setAutoCommit conn false)
  (SQLiteBatchWriter. conn (.prepareStatement conn sqlite-person-insert-sql)))

(defn- close-sqlite-batch-writer!
  [^SQLiteBatchWriter writer]
  (let [^PreparedStatement statement (.-statement writer)
        ^Connection conn             (.-conn writer)]
    (try
      (.close statement)
      (finally
        (.close conn)))))

(defn- rollback-sqlite-batch!
  [^Connection conn ^Throwable primary]
  (try
    (.rollback conn)
    (catch Throwable rollback-error
      (.addSuppressed primary rollback-error))))

(defn- transact-sqlite-person-batch!
  [^SQLiteBatchWriter writer ^FastList txs]
  (let [^Connection conn             (.-conn writer)
        ^PreparedStatement statement (.-statement writer)]
    (try
      (.clearBatch statement)
      (dotimes [i (.size txs)]
        (let [person (.get txs i)]
          (.setString statement 1 ^String (:person_id person))
          (.setString statement 2 ^String (:first_name person))
          (.setString statement 3 ^String (:last_name person))
          (.setLong statement 4 (long (:age person)))
          (.addBatch statement)))
      (let [result (.executeBatch statement)]
        (.commit conn)
        result)
      (catch Throwable t
        (rollback-sqlite-batch! conn t)
        (try
          (.clearBatch statement)
          (catch Throwable clear-error
            (.addSuppressed t clear-error)))
        (throw t)))))

(defn- sqlite-batch-configuration
  [config ^SQLiteBatchWriter writer]
  (assoc config
         :auto-commit (.getAutoCommit ^Connection (.-conn writer))
         :insert-mode :prepared-jdbc-batch))

(defn- verify-count!
  [label expected actual]
  (when-not (= (long expected) (long actual))
    (throw (ex-info "Benchmark verification failed"
                    {:store label :expected expected :actual actual}))))

(def ^:private datalog-id-count-query
  '[:find (count ?e) .
    :where [?e :person/id _]])

(def ^:private datalog-first-name-count-query
  '[:find (count ?e) .
    :where [?e :person/first-name _]])

(def ^:private datalog-last-name-count-query
  '[:find (count ?e) .
    :where [?e :person/last-name _]])

(def ^:private datalog-age-count-query
  '[:find (count ?e) .
    :where [?e :person/age _]])

(defn- verify-datalog-counts!
  [db expected label]
  ;; Use exact query scans here. Native key-range counts are intended to be
  ;; exact, but a large AVE range can straddle an internal page boundary and
  ;; misattribute entries between adjacent attributes. Verification is outside
  ;; the timed interval, so favor the independent exact scans.
  (verify-count! [label :person/id]
                 expected (d/q datalog-id-count-query db))
  (verify-count! [label :person/first-name]
                 expected (d/q datalog-first-name-count-query db))
  (verify-count! [label :person/last-name]
                 expected (d/q datalog-last-name-count-query db))
  (verify-count! [label :person/age]
                 expected (d/q datalog-age-count-query db)))

(defn- open-write-context
  [{:keys [kind wal?] :as info}
   target threads durability-profile seed]
  (let [next-person (pure-person-generator seed)]
    (case kind
      :kv
      (let [db (d/open-kv target (store-opts wal? durability-profile))]
        (try
          (d/open-dbi db max-write-dbi)
          {:tx-fn   (if (:async? info)
                      (fn [txs callback]
                        (d/transact-kv-async db max-write-dbi txs
                                             :string :data callback))
                      (fn [txs callback]
                        (callback
                          (d/transact-kv db max-write-dbi txs
                                         :string :data))))
           :add-fn  (fn [^FastList txs]
                      (let [{:keys [person-id] :as person} (next-person)]
                        (.add txs [:put person-id (person-value person)])))
           :verify! (fn [expected]
                      (verify-count! :kv expected
                                     (d/entries db max-write-dbi)))
           :close!  #(d/close-kv db)
           :storage {:engine :datalevin-kv
                     :native-version (Util/version)}}
          (catch Throwable t
            (d/close-kv db)
            (throw t))))

      :datalog
      (let [conn (d/get-conn
                   target
                   person-schema
                   (datalog-opts wal? durability-profile))]
        (try
          {:tx-fn   (if (:async? info)
                      (fn [txs callback]
                        (d/transact-async conn txs nil callback))
                      (fn [txs callback]
                        (callback (d/transact! conn txs nil))))
           :add-fn  (fn [^FastList txs]
                      (.add txs (datalog-person (next-person))))
           :verify! (fn [expected]
                      (verify-datalog-counts! (d/db conn) expected :datalog))
           :close!  #(d/close conn)
           :storage {:engine :datalevin-datalog
                     :native-version (Util/version)}}
          (catch Throwable t
            (d/close conn)
            (throw t))))

      :sqlite
      (let [journal-mode (if wal? "WAL" "DELETE")
            sync-mode    (sql-sync-mode-for durability-profile)
            spec         {:dbtype "sqlite" :dbname target}]
        (if (= threads 1)
          (let [{:keys [conn config]}
                (open-sqlite-connection spec journal-mode sync-mode
                                        durability-profile)]
            (try
              (create-sqlite-person-table! conn)
              (let [sqlite-ver (sqlite-version conn)
                    driver-ver (.getDriverVersion (.getMetaData conn))
                    writer     (open-sqlite-batch-writer conn)
                    config     (sqlite-batch-configuration config writer)]
                {:tx-fn   (fn [txs callback]
                            (callback
                              (transact-sqlite-person-batch! writer txs)))
                 :add-fn  (fn [^FastList txs]
                            (.add txs (sqlite-person (next-person))))
                 :verify! #(verify-count! :sqlite % (sqlite-count conn))
                 :close!  #(close-sqlite-batch-writer! writer)
                 :storage {:engine :sqlite
                           :sqlite-version sqlite-ver
                           :jdbc-driver-version driver-ver
                           :configuration config}})
              (catch Throwable t
                (.close conn)
                (throw t))))
          (let [initial (open-sqlite-connection spec journal-mode sync-mode
                                                durability-profile)
                opened  (atom [(:conn initial)])]
            (try
              (create-sqlite-person-table! (:conn initial))
              (let [busy-ms    (configure-sqlite-busy-timeout!
                                 (:conn initial) 10000)
                    base-config (assoc (:config initial)
                                       :busy-timeout-ms busy-ms)
                    sqlite-ver (sqlite-version (:conn initial))
                    driver-ver (.getDriverVersion
                                 (.getMetaData (:conn initial)))
                    mt-spec    {:jdbcUrl (str "jdbc:sqlite:" target
                                              "?busy_timeout=10000")}
                    _          (dotimes [_ (dec (int threads))]
                                 (let [conn (:conn
                                              (open-sqlite-connection
                                                mt-spec journal-mode sync-mode
                                                durability-profile))]
                                   ;; Add before configuring so the failure
                                   ;; path closes this connection too.
                                   (swap! opened conj conn)
                                   (configure-sqlite-busy-timeout!
                                     conn 10000)))
                    conns      @opened
                    writers    (mapv open-sqlite-batch-writer conns)
                    config     (sqlite-batch-configuration
                                 base-config (first writers))
                    next-writer (AtomicLong. 0)
                    thread-writer
                    (ThreadLocal/withInitial
                      (reify Supplier
                        (get [_]
                          (let [idx (.getAndIncrement next-writer)]
                            (when (>= idx (count writers))
                              (throw
                                (ex-info "More SQLite worker threads than connections"
                                         {:threads threads
                                          :connection-index idx})))
                            (nth writers (int idx))))))]
                {:tx-fn   (fn [txs callback]
                            (callback
                              (transact-sqlite-person-batch!
                                (.get ^ThreadLocal thread-writer) txs)))
                 :add-fn  (fn [^FastList txs]
                            (.add txs (sqlite-person (next-person))))
                 :verify! (fn [expected]
                            (verify-count! :sqlite expected
                                           (sqlite-count (first conns))))
                 :close!  #(doseq [^SQLiteBatchWriter writer writers]
                             (close-sqlite-batch-writer! writer))
                 :storage {:engine :sqlite
                           :sqlite-version sqlite-ver
                           :jdbc-driver-version driver-ver
                           :configuration config}})
              (catch Throwable t
                (doseq [^java.sql.Connection conn @opened]
                  (try
                    (when-not (.isClosed conn) (.close conn))
                    (catch Throwable _)))
                (throw t)))))))))

(defn- run-write-pass
  [{:keys [base-dir batch f threads durability-profile total report
           in-flight in-flight-writes completion-timeout-ms seed]
    :or   {threads               1
           total                 1000000
           report                10000
           in-flight             1000
           in-flight-writes      100000
           completion-timeout-ms 600000
           seed                  42}}]
  (let [base-dir           (when (some? base-dir)
                             (required-path! :base-dir base-dir))
        info               (task-info f)
        batch              (positive-int! :batch batch)
        threads            (positive-int! :threads threads)
        total-writes       (positive-long! :total total)
        report-every       (non-negative-long! :report report)
        in-flight-limit    (positive-int! :in-flight in-flight)
        in-flight-write-limit
        (positive-long! :in-flight-writes in-flight-writes)
        effective-in-flight
        (effective-in-flight-limit batch
                                   in-flight-limit
                                   in-flight-write-limit)
        seed               (integer-long! :seed seed)
        completion-timeout (positive-long! :completion-timeout-ms
                                           completion-timeout-ms)
        effective-profile  (effective-durability-profile
                             (:wal? info) durability-profile)
        config             {:batch batch
                            :threads threads
                            :total-writes total-writes
                            :report-every report-every
                            :in-flight-limit in-flight-limit
                            :in-flight-write-limit in-flight-write-limit
                            :completion-timeout completion-timeout}
        _                  (validate-common! config info durability-profile)
        target             (benchmark-target base-dir info batch threads
                                             effective-profile)
        _                  (ensure-fresh-target!
                             target (= :sqlite (:kind info)))
        context            (open-write-context info target threads
                                               effective-profile seed)
        started-at         (str (Instant/now))]
    (try
      (let [metrics (h/run-benchmark!
                      {:total-writes total-writes
                       :batch-size batch
                       :threads threads
                       :async? (:async? info)
                       :in-flight effective-in-flight
                       :completion-timeout-ms completion-timeout
                       :report-every report-every
                       :tx-fn (:tx-fn context)
                       :add-fn (:add-fn context)})]
        ((:verify! context) total-writes)
        (assoc metrics
               :started-at started-at
               :finished-at (str (Instant/now))
               :target target
               :task (:name info)
               :batch batch
               :threads threads
               :async? (:async? info)
               :durability-profile effective-profile
               :seed seed
               :in-flight (when (:async? info) effective-in-flight)
               :in-flight-request-limit
               (when (:async? info) in-flight-limit)
               :in-flight-write-limit
               (when (:async? info) in-flight-write-limit)
               :completion-timeout-ms completion-timeout
               :report-every report-every
               :workload workload-description
               :storage (:storage context)))
      (finally
        ((:close! context))))))

(declare environment-metadata)

(defn- result-manifest-file
  [base-dir]
  (when (some? base-dir)
    (io/file (required-path! :base-dir base-dir) "results.edn")))

(defn- write-result-manifest!
  [result-file measurement]
  (let [manifest {:protocol {:warmup-passes 0
                             :measurement-passes 1
                             :fresh-database-per-pass true
                             :measurement-writes (:writes measurement)}
                  :environment (environment-metadata)
                  :measurement measurement}]
    (with-open [writer (io/writer result-file)]
      (binding [*out* writer]
        (pprint/pprint manifest)))))

(defn write
  "Run one measured pure-write pass against a fresh target. When `:base-dir`
  is supplied, persist the result, environment, and one-pass protocol in
  `results.edn` below that directory."
  [opts]
  (let [result-file (result-manifest-file (:base-dir opts))]
    (when (and result-file (.exists result-file))
      (throw (ex-info "Benchmark result manifest already exists"
                      {:path (.getPath result-file)})))
    (let [measurement (run-write-pass opts)]
      (when result-file
        (write-result-manifest! result-file measurement))
      (h/print-result! :measurement measurement)
      (when result-file
        (binding [*out* *err*]
          (println "Wrote benchmark manifest to" (.getPath result-file))))))
  nil)

(defn- environment-metadata
  []
  {:os-name              (System/getProperty "os.name")
   :os-version           (System/getProperty "os.version")
   :architecture         (System/getProperty "os.arch")
   :available-processors (.availableProcessors (Runtime/getRuntime))
   :java-version         (System/getProperty "java.version")
   :java-vm-name         (System/getProperty "java.vm.name")
   :jvm-arguments        (vec
                           (.getInputArguments
                             (ManagementFactory/getRuntimeMXBean)))
   :clojure-version      (clojure-version)
   :datalevin-version    c/version
   :native-version       (Util/version)})

(defn suite
  "Compatibility alias for the one-pass `write` protocol. The legacy
  `:measurement-writes` option is accepted as an alias for `:total`; warmup
  passes are deliberately unsupported for this write benchmark."
  [{:keys [base-dir warmup-writes measurement-writes total] :as opts}]
  (required-path! :base-dir base-dir)
  (when (some? warmup-writes)
    (throw
      (ex-info "Warmup passes are not part of the write benchmark protocol"
               {:warmup-writes warmup-writes})))
  (when (and (some? total)
             (some? measurement-writes)
             (not= total measurement-writes))
    (throw
      (ex-info ":total and :measurement-writes disagree"
               {:total total :measurement-writes measurement-writes})))
  (write
    (cond-> (dissoc opts :warmup-writes :measurement-writes)
      (some? measurement-writes) (assoc :total measurement-writes))))

(defn- initial-keys
  [^long initial-total]
  (let [keys (BitSet. (inc (int (* 2 initial-total))))]
    (loop [key 2]
      (when (<= key (* 2 initial-total))
        (.set keys (int key))
        (recur (+ key 2))))
    keys))

(defn- random-key
  ^long [^Random random ^long keyspace]
  (inc (.nextInt random (int keyspace))))

(defn- open-mixed-context
  [{:keys [kind wal? async?]}
   target durability-profile initial-total seed]
  (let [keyspace     (* 2 (long initial-total))
        _            (when (> keyspace Integer/MAX_VALUE)
                       (throw (ex-info "Mixed keyspace exceeds Random.nextInt limit"
                                       {:keyspace keyspace})))
        expected     (initial-keys initial-total)
        read-random  (Random. (long seed))
        write-random (Random. (unchecked-add (long seed) 1))
        next-person  (person-update-generator seed)
        read-slot    #(random-key read-random keyspace)
        write-slot   #(random-key write-random keyspace)
        read-id      #(person-id (long seed) (read-slot))]
    (case kind
      :kv
      (let [db (d/open-kv target (store-opts wal? durability-profile))]
        (try
          (d/open-dbi db max-write-dbi)
          (verify-count! :kv-initial initial-total
                         (d/entries db max-write-dbi))
          {:tx-fn   (if async?
                      (fn [txs callback]
                        (d/get-value db max-write-dbi (read-id) :string :data)
                        @(d/transact-kv-async db max-write-dbi txs
                                              :string :data callback))
                      (fn [txs callback]
                        (d/get-value db max-write-dbi (read-id) :string :data)
                        (callback
                          (d/transact-kv db max-write-dbi txs
                                         :string :data))))
           :add-fn  (fn [^FastList txs]
                      (let [slot                       (write-slot)
                            {:keys [person-id] :as person}
                            (next-person slot)]
                        (.set expected (int slot))
                        (.add txs [:put person-id (person-value person)])))
           :verify! #(verify-count! :kv-mixed (.cardinality expected)
                                    (d/entries db max-write-dbi))
           :close!  #(d/close-kv db)
           :storage {:engine :datalevin-kv
                     :native-version (Util/version)}}
          (catch Throwable t
            (d/close-kv db)
            (throw t))))

      :datalog
      (let [conn  (d/get-conn
                    target
                    person-schema
                    (datalog-opts wal? durability-profile))
            query '[:find (pull ?e [:person/first-name
                                    :person/last-name
                                    :person/age])
                    :in $ ?person-id
                    :where [?e :person/id ?person-id]]]
        (try
          (verify-datalog-counts! (d/db conn) initial-total :datalog-initial)
          {:tx-fn   (if async?
                      (fn [txs callback]
                        (d/q query (d/db conn) (read-id))
                        @(d/transact-async conn txs nil callback))
                      (fn [txs callback]
                        (d/q query (d/db conn) (read-id))
                        (callback (d/transact! conn txs nil))))
           :add-fn  (fn [^FastList txs]
                      (let [slot (write-slot)]
                        (.set expected (int slot))
                        (.add txs (datalog-person (next-person slot)))))
           :verify! (fn []
                      (let [expected-count (.cardinality expected)
                            db             (d/db conn)]
                        (verify-datalog-counts! db expected-count
                                                :datalog-mixed)))
           :close!  #(d/close conn)
           :storage {:engine :datalevin-datalog
                     :native-version (Util/version)}}
          (catch Throwable t
            (d/close conn)
            (throw t))))

      :sqlite
      (let [journal-mode (if wal? "WAL" "DELETE")
            sync-mode    (sql-sync-mode-for durability-profile)
            spec         {:dbtype "sqlite" :dbname target}
            {:keys [conn config]}
            (open-sqlite-connection spec journal-mode sync-mode
                                    durability-profile)
            upsert-sql   (str "INSERT INTO person "
                              "(person_id, first_name, last_name, age) "
                              "VALUES (?, ?, ?, ?) "
                              "ON CONFLICT(person_id) DO UPDATE SET "
                              "first_name=excluded.first_name, "
                              "last_name=excluded.last_name, "
                              "age=excluded.age")]
        (try
          (verify-count! :sqlite-initial initial-total (sqlite-count conn))
          {:tx-fn   (fn [txs callback]
                      (jdbc/execute-one!
                        conn [(str "SELECT first_name, last_name, age "
                                   "FROM person WHERE person_id = ?")
                              (read-id)])
                      (let [values (first txs)]
                        (callback
                          (jdbc/execute! conn (into [upsert-sql] values)))))
           :add-fn  (fn [^FastList txs]
                      (let [slot (write-slot)
                            {:keys [person-id first-name last-name age]}
                            (next-person slot)]
                        (.set expected (int slot))
                        (.add txs [person-id first-name last-name age])))
           :verify! #(verify-count! :sqlite-mixed (.cardinality expected)
                                    (sqlite-count conn))
           :close!  #(.close conn)
           :storage {:engine :sqlite
                     :sqlite-version (sqlite-version conn)
                     :jdbc-driver-version
                     (.getDriverVersion (.getMetaData conn))
                     :configuration config}}
          (catch Throwable t
            (.close conn)
            (throw t)))))))

(defn mixed
  "Run a closed-loop mixed workload against a database produced by `write`.
  Every read/write pair completes before the next pair starts, including when
  an async API is selected, so all stores provide the same read-your-writes
  semantics."
  [{:keys [dir f durability-profile total initial-total report
           completion-timeout-ms seed]
    :or   {total                 1000000
           initial-total         1000000
           report                10000
           completion-timeout-ms 600000
           seed                  42}}]
  (let [dir                (required-path! :dir dir)
        info               (task-info f)
        total-operations   (positive-long! :total total)
        initial-total      (positive-long! :initial-total initial-total)
        seed               (integer-long! :seed seed)
        report-every       (non-negative-long! :report report)
        completion-timeout (positive-long! :completion-timeout-ms
                                           completion-timeout-ms)
        _                  (validate-durability-profile!
                             (:wal? info) durability-profile)
        effective-profile  (effective-durability-profile
                             (:wal? info) durability-profile)
        _                  (ensure-native-wal-sync-path! info)
        _                  (ensure-existing-target! dir)
        context            (open-mixed-context info dir effective-profile
                                               initial-total seed)
        started-at         (str (Instant/now))]
    (try
      (let [metrics (h/run-benchmark!
                      {:total-writes total-operations
                       :batch-size 1
                       :threads 1
                       ;; Async APIs are deliberately awaited inside tx-fn.
                       :async? false
                       :completion-timeout-ms completion-timeout
                       :report-every report-every
                       :tx-fn (:tx-fn context)
                       :add-fn (:add-fn context)})]
        ((:verify! context))
        (h/print-result!
          :measurement
          (assoc metrics
                 :started-at started-at
                 :finished-at (str (Instant/now))
                 :target dir
                 :task (:name info)
                 :batch 1
                 :threads 1
                 :async? (:async? info)
                 :closed-loop? true
                 :durability-profile effective-profile
                 :seed seed
                 :workload workload-description
                 :storage (:storage context))))
      (finally
        ((:close! context))))
    nil))

(defn dl-init
  [{:keys [dir total seed]
    :or   {total 1000000 seed 42}}]
  (let [dir       (required-path! :dir dir)
        total     (positive-long! :total total)
        seed      (integer-long! :seed seed)
        _         (ensure-fresh-target! dir false)
        es        (range 1 (inc total))
        id-datoms (mapv (fn [e]
                          (d/datom e :person/id
                                   (person-id seed (* 2 (long e)))))
                        es)
        start     (System/nanoTime)
        db        (d/init-db id-datoms dir person-schema
                             {:kv-opts {:mapsize 60000}})]
    (try
      (let [field-datoms
            (mapcat
              (fn [e]
                (let [{:keys [first-name last-name age]}
                      (person-record seed (* 2 (long e)) (long e))]
                  [(d/datom e :person/first-name first-name)
                   (d/datom e :person/last-name last-name)
                   (d/datom e :person/age age)]))
              es)
            filled-db (d/fill-db db field-datoms)]
        (verify-datalog-counts! filled-db total :datalog-init)
        (println "Loaded" total "entities in"
                 (/ (double (- (System/nanoTime) start)) 1000000000.0)
                 "seconds"))
      (finally
        (d/close-db db)))
    nil))
