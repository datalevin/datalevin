;; Copyright (c) Huahai Yang. All rights reserved.
;; Distributed under the Eclipse Public License 2.0.
(ns ^:no-doc datalevin.admin
  "Synchronous administration of the contacted server endpoint."
  (:refer-clojure :exclude [run!])
  (:require [clojure.string :as str]
            [datalevin.client :as client]
            [datalevin.client.copy :as file-copy]
            [datalevin.util :as u])
  (:import [java.io File IOException InterruptedIOException]
           [java.nio.channels ClosedByInterruptException]
           [java.nio.file Files Path Paths LinkOption CopyOption FileSystemException
            DirectoryNotEmptyException NoSuchFileException]
           [java.nio.file.attribute FileAttribute PosixFilePermissions]
           [java.util.concurrent Callable Executors ExecutorService Future ThreadFactory TimeUnit]
           [java.util.concurrent.atomic AtomicBoolean]))

(def cli-options
  [[nil "--uri-env VAR" "Environment variable containing the server URI"
    :default "DATALEVIN_URI"
    :validate [#(not (str/blank? %)) "Must name an environment variable"]]
   [nil "--timeout-ms MS" "Timeout per authentication or request, not the whole sweep"
    :default 60000 :parse-fn #(Long/parseLong %)
    :validate [#(<= 1 (long %) Integer/MAX_VALUE) "Must be a positive 32-bit integer"]]
   [nil "--database NAME" "Named database; repeat to select more databases"
    :assoc-fn (fn [options key value]
                (update options key (fnil conj []) value))
    :validate [#(not (str/blank? %)) "Must name a database"]]
   [nil "--all-databases" "Select the contacted node's complete database catalog"]
   [nil "--status-concurrency N" "Concurrent status reads (default 1; maximum 16)"
    :parse-fn #(Long/parseLong %)
    :validate [#(<= 1 (long %) 16) "Must be between 1 and 16"]]
   [nil "--dry-run" "Plan WAL GC without deleting segments (wal-gc only)"]])

(def help
  "Command admin - administer databases on the contacted server.

Usage: dtlv admin [options] status (--all-databases | --database NAME ...)
       dtlv admin [options] wal-gc (--all-databases | --database NAME ...) [--dry-run]
       dtlv admin [options] backup (--all-databases | --database NAME ...) DEST

Options:
  --uri-env VAR       Server URI environment variable (default DATALEVIN_URI)
  --timeout-ms MS     Per-request timeout in milliseconds (default 60000)
  --database NAME     Select a database; repeatable, normalized and deduplicated
  --all-databases     Select the contacted node's catalog, including closed DBs
  --status-concurrency N  Concurrent status reads, 1–16 (default 1; status only)
  --dry-run          Plan WAL GC without unlinking files (wal-gc only)
  -h, --help          Show this help

Status does not open closed databases. Named status requires database view
permission. WAL GC requires database alter permission. Global enumeration also
requires database create and server view. WAL GC opens closed targets and leaves
them open, including in dry runs; it never closes other clients' databases.
Explicit GC deletes every floor-safe segment, ignoring retention age/byte caps.
Partial failures retain native results and exit 1. Directory sync may fail or be
unsupported (including Windows) after deletion; such a result also exits 1.
Results are one EDN map per target. Diagnostics go to stderr.
Exit codes: 0 success; 1 target failure; 2 invalid invocation or discovery failure.
WAL-disabled and rollback GC skips exit 0. Backup creates a new local DEST with
one independently usable database directory per target. The parent must exist.
Do not place DEST under a live server's data root. Failed transfers remain in
DEST/.incomplete. Backup needs view permission, plus alter when flushing indexes.
Budget a server temp copy plus a destination copy; writers resume after capture.

Example:
  dtlv admin --uri-env DATALEVIN_URI status --database orders --database users")

(defn validate-options
  [options arguments]
  (let [names (->> (:database options)
                   (map #(some-> % u/lisp-case))
                   distinct
                   vec)
        concurrency (get options :status-concurrency 1)
        error (cond
                (not (or (and (= 1 (count arguments)) (#{"status" "wal-gc"} (first arguments)))
                         (and (= 2 (count arguments)) (= "backup" (first arguments))
                              (not (str/blank? (second arguments))))))
                "Expected admin status, wal-gc, or backup DEST"
                (and (:dry-run options) (not= "wal-gc" (first arguments)))
                "--dry-run is only supported by wal-gc"
                (not (and (integer? concurrency) (<= 1 concurrency 16)))
                "--status-concurrency must be between 1 and 16"
                (and (some? (:status-concurrency options)) (not= "status" (first arguments)))
                "--status-concurrency is only supported by status"
                (:all options) "--all selects sub-databases; use --all-databases for admin"
                (= (boolean (:all-databases options)) (boolean (seq names)))
                "Select exactly one of --all-databases or --database NAME"
                (some str/blank? names) "Database names must not be blank")]
    (if error
      {:exit-message (str error "\n\n" help) :exit-code 2}
      {:command "admin" :options (assoc options :database names)
       :arguments arguments})))

(def ^:private server-path-keys
  #{"dir" "root" "path" "file" "filename" "wal-dir" "snapshot-dir"
    "segment-path" "segment-file" "temp-dir" "tmp-dir" "copy-dir"})

(def ^:private server-path-pattern
  (let [prefix "(?:(?<![\\w:/])(?:/[\\w.]|//)|(?<![\\w/])[A-Za-z]:[\\\\/]|\\\\\\\\)"]
    ;; Spaces and punctuation can belong to filenames. Redact through the last
    ;; matching quote, or through the end of the line for unquoted diagnostics.
    (re-pattern (str "(?:([\"'])" prefix "[^\\r\\n]*\\1|" prefix "[^\\r\\n]*)"))))

(defn- safe-string
  [uri value]
  (-> (if (str/blank? uri) value (str/replace value uri "[redacted-uri]"))
      (str/replace #"(?i)([a-z][a-z0-9+.-]*://)[^\s/@]+@" "$1[redacted]@")
      (str/replace server-path-pattern
                   (fn [[_ quote]] (str quote "[redacted-path]" quote)))))

(defn sanitize
  "Keep EDN results while removing server-local paths and URI credentials."
  [uri value]
  (cond
    (string? value) (safe-string uri value)
    (instance? Throwable value) {:message (safe-string uri (or (ex-message value) "Error"))
                                 :data (sanitize uri (ex-data value))}
    (or (instance? File value) (instance? Path value)) "[redacted-path]"
    (map? value) (into {}
                      (keep (fn [[key item]]
                              (when-not (and (or (keyword? key) (string? key))
                                             (contains? server-path-keys (name key)))
                                [(sanitize uri key) (sanitize uri item)])))
                      value)
    (set? value) (into #{} (map #(sanitize uri %)) value)
    (sequential? value) (mapv #(sanitize uri %) value)
    (or (nil? value) (boolean? value) (number? value) (keyword? value)
        (symbol? value) (char? value) (uuid? value) (instance? java.util.Date value)) value
    :else (safe-string uri (str value))))

(defn- print-result
  [result]
  (binding [*print-length* nil *print-level* nil *print-meta* false
            *print-readably* true *print-dup* false]
    (prn result)))

(def ^:private catalog-query
  '[:find ?name ?type ?id
    :where [?id :database/name ?name] [?id :database/type ?type]])

(defn- all-targets
  [request connection runtime?]
  (let [response (request connection :list-databases [{:admin? true :runtime? runtime?}])]
    (if (and (map? response) (vector? (:admin-statuses response)))
      (let [rows (:admin-statuses response)]
        (when-not (and (every? #(and (string? (:db-name %))
                                     (integer? (:db-id %))
                                     (#{:datalog :key-value} (:db-type %))
                                     (or (not runtime?)
                                         (and (contains? % :result) (not (contains? % :error)))
                                         (and (map? (:error %)) (not (contains? % :result)))))
                               rows)
                       (= (count rows) (count (distinct (map :db-name rows)))))
          (throw (ex-info "Invalid admin catalog" {:error :admin/invalid-result})))
        (mapv (fn [row]
                (cond-> (select-keys row [:db-name :db-id :db-type])
                  runtime? (assoc ::status-outcome (select-keys row [:result :error]))))
              (sort-by :db-name rows)))
      ;; Older servers ignore list-databases options and return names. Keep
      ;; identity checks on that path, without fetching unused open state.
      (do
        (when-not (and (sequential? response) (every? string? response))
          (throw (ex-info "Invalid database catalog" {:error :admin/invalid-result})))
        (let [catalog (into {} (map (fn [[name type id]]
                                      [name {:db-name name :db-type type :db-id id}]))
                            (request connection :query-system [catalog-query []]))]
          (mapv #(get catalog % {:db-name % :db-id nil}) (sort response)))))))

(defn- error-code [data]
  (or (:error data) (get-in data [:err-data :error])))

(defn- terminal-error?
  [error]
  (let [causes (take-while some? (iterate ex-cause error))
        local-io? (some (fn [cause]
                          (let [data (ex-data cause)]
                            (or (instance? FileSystemException cause)
                                (:copy/local-io? data)
                                (get-in data [:err-data :copy/local-io?])
                                (= :copy/local-io (error-code data)))))
                        causes)]
    (boolean
      (some (fn [cause]
              (let [data (ex-data cause)
                    code (error-code data)]
                (or (and (instance? IOException cause) (not local-io?))
                    (instance? InterruptedException cause)
                    (instance? InterruptedIOException cause)
                    (instance? ClosedByInterruptException cause)
                    (#{:admin/unsupported-server :socket/timeout :client/closed} code)
                    (:termination-reason data)
                    (and (not local-io?)
                         (re-find #"(?i)authentication fail" (or (ex-message cause) ""))))))
            causes))))

(defn- validate-status
  [{:keys [db-name db-id db-type] :as target} result]
  (when-not (and (map? result) (set? (:admin-capabilities result)))
    (throw (ex-info "Server does not support admin status"
                    {:error :admin/unsupported-server})))
  (when-not (and (= db-name (:db-name result))
                 (integer? (:db-id result))
                 (#{:datalog :key-value} (:db-type result))
                 (or (nil? db-id) (= db-id (:db-id result)))
                 (or (nil? db-type) (= db-type (:db-type result))))
    (throw (ex-info "Database identity changed during discovery"
                    {:error :admin/identity-mismatch :expected target
                     :actual (select-keys result [:db-name :db-type :db-id])})))
  result)

(defn- read-status
  ([request connection target] (read-status request connection target true))
  ([request connection {:keys [db-name] :as target} runtime?]
   (when (and (contains? target :db-id) (nil? (:db-id target)))
     (throw (ex-info "Database disappeared during discovery"
                     {:error :admin/database-not-found :db-name db-name})))
   (validate-status
     target
     (try
       (request connection :admin-status [db-name {:runtime? runtime?}])
       (catch Exception e
         (if (str/includes? (or (:server-message (ex-data e)) (ex-message e) "")
                            "Unknown message type :admin-status")
           (throw (ex-info "Server does not support admin status"
                           {:error :admin/unsupported-server} e))
           (throw e)))))))

(defn- status-outcome
  [request connection target]
  (try
    (if-let [batch-outcome (::status-outcome target)]
      (if (:error batch-outcome)
        batch-outcome
        {:result (validate-status target (:result batch-outcome))})
      {:result (read-status request connection target)})
    (catch Exception e
      {:error {:message (ex-message e) :data (ex-data e)}
       :stop? (terminal-error? e)})))

(defn- emit-status
  [emit target outcome]
  (emit (merge {:db-name (:db-name target) :operation :status}
               (dissoc outcome :stop?))))

(defn- sweep
  [request connection targets emit]
  (loop [targets (seq targets) failed? false]
    (if-let [target (first targets)]
      (let [outcome (status-outcome request connection target)]
        (emit-status emit target outcome)
        (if (:stop? outcome)
          1
          (recur (next targets) (or failed? (contains? outcome :error)))))
      (if failed? 1 0))))

(defn- stop-status-workers!
  [^ExecutorService executor]
  (.shutdownNow executor)
  (let [interrupted? (volatile! (Thread/interrupted))
        deadline (+ (System/nanoTime) 1000000000)]
    (try
      (loop []
        (when (= ::interrupted
                 (try
                   (.awaitTermination executor (max 0 (- deadline (System/nanoTime)))
                                      TimeUnit/NANOSECONDS)
                   (catch InterruptedException _
                     (vreset! interrupted? true)
                     ::interrupted)))
          (recur)))
      (finally (when @interrupted? (.interrupt (Thread/currentThread)))))))

(defn- parallel-status-sweep
  [request connection targets concurrency emit]
  (let [executor (Executors/newFixedThreadPool
                   concurrency
                   (reify ThreadFactory
                     (newThread [_ runnable]
                       (doto (Thread. ^Runnable runnable "datalevin-admin-status")
                         (.setDaemon true)))))
        stopped? (AtomicBoolean. false)
        submit (fn [target]
                 (.submit executor
                   ^Callable
                   (bound-fn []
                     (when-not (.get stopped?)
                       (let [outcome (status-outcome request connection target)]
                         (when (:stop? outcome) (.set stopped? true))
                         outcome)))))]
    (try
      ;; Keep at most N futures, including completed results waiting for an
      ;; earlier target. This bounds both request pressure and buffered output.
      (loop [remaining (drop concurrency targets)
             pending (into clojure.lang.PersistentQueue/EMPTY
                           (map (fn [target] [target (submit target)]))
                           (take concurrency targets))
             failed? false]
        (if-let [[target ^Future task] (peek pending)]
          (let [outcome (.get task)]
            (when outcome (emit-status emit target outcome))
            (if (:stop? outcome)
              1
              (let [pending (pop pending)]
                (if (and (seq remaining) (not (.get stopped?)))
                  (recur (next remaining)
                         (conj pending [(first remaining) (submit (first remaining))])
                         (or failed? (contains? outcome :error)))
                  (recur nil pending (or failed? (contains? outcome :error)))))))
          (if (or failed? (.get stopped?)) 1 0)))
      (catch InterruptedException e
        (.interrupt (Thread/currentThread))
        (throw e))
      (finally
        (.set stopped? true)
        (stop-status-workers! executor)))))

(defn- require-gc-support!
  [discovery dry-run?]
  (let [required (cond-> #{:guarded-maintenance} dry-run? (conj :wal-gc-dry-run))]
    (when-not (every? #(contains? (:admin-capabilities discovery) %) required)
      (throw (ex-info "Server does not support guarded WAL GC"
                      {:error :admin/unsupported-server :required required})))))

(defn- gc-success? [result]
  (or (true? (:ok? result))
      (and (false? (:ok? result)) (#{:wal-disabled :rollback} (:reason result)))))

(defn- gc-sweep
  [request maintenance connection targets options emit]
  (loop [targets (seq targets) failed? false attempted? false]
    (if-let [target (first targets)]
      (let [sent? (volatile! false)
            outcome
            (try
              (let [discovery (read-status request connection target false)
                    _ (require-gc-support! discovery (:dry-run options))
                    result (maintenance connection
                                        (select-keys discovery [:db-name :db-id :db-type])
                                        {:dry-run? (boolean (:dry-run options))})]
                (vreset! sent? true)
                (when-not (and (map? result) (boolean? (:ok? result)))
                  (throw (ex-info "Invalid WAL GC result" {:error :admin/invalid-result})))
                {:result result})
              (catch Exception e
                (when (:admin/maintenance-sent? (ex-data e)) (vreset! sent? true))
                {:error {:message (ex-message e) :data (ex-data e)}
                 :stop? (terminal-error? e)
                 :unsupported? (= :admin/unsupported-server (error-code (ex-data e)))}))
            attempted? (or attempted? @sent?)]
        (emit (merge {:db-name (:db-name target) :operation :wal-gc}
                     (select-keys outcome [:result :error])))
        (if (:stop? outcome)
          (if (and (:unsupported? outcome) (not attempted?)) 2 1)
          (recur (next targets)
                 (or failed? (:error outcome) (not (gc-success? (:result outcome))))
                 attempted?)))
      (if failed? 1 0))))

(def ^:private no-follow (into-array LinkOption [LinkOption/NOFOLLOW_LINKS]))

;; Windows also reserves aliases with extensions and superscript ¹/²/³ digits.
;; https://learn.microsoft.com/en-us/windows/win32/fileio/naming-a-file
(def ^:private windows-device-name
  #"(?i)(?:con|prn|aux|nul|conin\$|conout\$|(?:com|lpt)[1-9¹²³])(?:\..*)?")

(defn- private-directory!
  [^Path path]
  (file-copy/with-local-io
    (try
      (Files/createDirectory path
        (into-array FileAttribute [(PosixFilePermissions/asFileAttribute
                                    (PosixFilePermissions/fromString "rwx------"))]))
      (catch UnsupportedOperationException _
        (Files/createDirectory path (make-array FileAttribute 0))))))

(defn- remove-empty-directory! [^Path path]
  (file-copy/with-local-io
    (try
      (when (Files/isDirectory path no-follow)
        (Files/delete path))
      (catch DirectoryNotEmptyException _)
      (catch NoSuchFileException _))))

(defn- cleanup-backup-destination! [^Path destination]
  (remove-empty-directory! (.resolve destination ".incomplete"))
  (remove-empty-directory! destination))

(defn- backup-destination!
  [create-directory! destination targets]
  (let [names (mapv :db-name targets)]
    (when (or (some #(or (str/blank? %) (#{"." ".."} %)
                        (.equalsIgnoreCase ".incomplete" %)
                        (re-matches windows-device-name %)
                        (re-find #"[\\/\u0000<>:\"|?*]" %)
                        (re-find #"[. ]$" %)) names)
              (not= (count names) (count (distinct (map str/lower-case names)))))
      (throw (ex-info "Unsafe or colliding backup database names"
                      {:error :admin/invalid-destination}))))
  (let [path (.normalize (.toAbsolutePath (Paths/get destination (make-array String 0))))
        parent (.getParent path)]
    (when-not (and parent (Files/isDirectory parent (make-array LinkOption 0))
                   (Files/isWritable parent))
      (throw (ex-info "Backup parent directory must exist and be writable"
                      {:error :admin/invalid-destination})))
    (let [path (.resolve (.toRealPath parent (make-array LinkOption 0)) (.getFileName path))]
      (create-directory! path)
      (try
        (create-directory! (.resolve path ".incomplete"))
        path
        (catch Throwable failure
          (try (cleanup-backup-destination! path)
               (catch Throwable cleanup
                 (when-not (identical? failure cleanup) (.addSuppressed failure cleanup))))
          (throw failure))))))

(defn- backup-sweep
  [request backup create-directory! connection targets ^Path destination emit]
  (loop [targets (seq targets) failed? false attempted? false]
    (if-let [target (first targets)]
      (let [sent? (volatile! false)
            outcome
            (try
              (let [discovery (read-status request connection target false)
                    required #{:guarded-maintenance :standalone-copy}]
                (when-not (every? (:admin-capabilities discovery) required)
                  (throw (ex-info "Server does not support standalone backup"
                                  {:error :admin/unsupported-server :required required})))
                (let [name (:db-name discovery)
                      stage (.resolve (.resolve destination ".incomplete") ^String name)
                      final (.resolve destination ^String name)]
                  (create-directory! stage)
                  (let [result (backup connection
                                       (select-keys discovery [:db-name :db-id :db-type])
                                       (.resolve stage "data.mdb"))]
                    (vreset! sent? true)
                    (when-not (and (:verified? result) (= :binary-chunks (:copy-format result)))
                      (throw (ex-info "Backup requires a verified binary transfer"
                                      {:error :copy/missing-integrity})))
                    (when (or (Files/exists final no-follow)
                              (not (Files/isDirectory stage no-follow))
                              (not (Files/isRegularFile (.resolve stage "data.mdb") no-follow))
                              (not (Files/isRegularFile (.resolve stage "VERSION") no-follow)))
                      (throw (ex-info "Backup destination changed during transfer"
                                      {:error :admin/invalid-destination})))
                    (file-copy/with-local-io (Files/move stage final (make-array CopyOption 0)))
                    {:result (dissoc result :type :result)})))
              (catch Exception e
                (when (:admin/maintenance-sent? (ex-data e)) (vreset! sent? true))
                {:error {:message (ex-message e) :data (ex-data e)}
                 :stop? (terminal-error? e)
                 :unsupported? (= :admin/unsupported-server (error-code (ex-data e)))}))
            attempted? (or attempted? @sent?)]
        (emit (merge {:db-name (:db-name target) :operation :backup}
                     (select-keys outcome [:result :error])))
        (if (:stop? outcome)
          (if (and (:unsupported? outcome) (not attempted?)) 2 1)
          (recur (next targets) (or failed? (contains? outcome :error)) attempted?)))
      (if failed? 1 0))))

(defn run!
  "Execute an admin sweep, returning an exit code. Dependencies are injectable
  for transport, filesystem, output and environment tests; normal calls use the real client."
  ([options arguments] (run! options arguments {}))
  ([options arguments dependencies]
   (let [{:keys [getenv connect disconnect request maintenance backup create-directory! emit diagnostic]}
         (merge {:getenv u/env :connect client/new-client
                 :disconnect client/close-client :request client/endpoint-request
                 :maintenance client/admin-gc!
                 :backup client/admin-backup!
                 :create-directory! private-directory!
                 :emit print-result :diagnostic #(binding [*out* *err*] (println %))}
                dependencies)
         validation (validate-options options arguments)]
     (if-let [message (:exit-message validation)]
       (do (diagnostic message) 2)
       (let [options (:options validation)
             uri (getenv (or (:uri-env options) "DATALEVIN_URI"))]
         (if (str/blank? uri)
           (do (diagnostic "The server URI environment variable is missing or blank") 2)
           (let [connection (try
                              (connect uri
                                {:pool-size (if (= "status" (first arguments))
                                              (min (or (:status-concurrency options) 1)
                                                   (if (:all-databases options)
                                                     16
                                                     (count (:database options))))
                                              1)
                                 :time-out (or (:timeout-ms options) 60000)})
                              (catch Exception _ nil))]
             (if-not connection
               (do (diagnostic "Unable to connect or authenticate to the server") 2)
               (let [started? (volatile! false)]
                 (try
                   (let [targets (if (:all-databases options)
                                   (all-targets request connection (= "status" (first arguments)))
                                   (mapv #(hash-map :db-name %) (:database options)))
                         destination (when (and (= "backup" (first arguments)) (seq targets))
                                       (backup-destination! create-directory! (second arguments) targets))]
                     (vreset! started? true)
                     (case (first arguments)
                       "wal-gc" (gc-sweep request maintenance connection targets options #(emit (sanitize uri %)))
                       "backup" (if destination
                                  (try
                                    (backup-sweep request backup create-directory! connection targets destination
                                                  #(emit (sanitize uri %)))
                                    (finally (cleanup-backup-destination! destination)))
                                  0)
                       (let [concurrency (min (or (:status-concurrency options) 1) (count targets))
                             emit #(emit (sanitize uri %))]
                         (if (and (> concurrency 1) (not-every? ::status-outcome targets))
                           (parallel-status-sweep request connection targets concurrency emit)
                           (sweep request connection targets emit)))))
                   (catch Exception e
                     (diagnostic (str "Admin command failed: "
                                      (safe-string uri (or (ex-message e) "Error"))))
                     (if @started? 1 2))
                   (finally
                     (try (disconnect connection)
                          (catch Exception _
                            (diagnostic "Unable to close the admin client session"))))))))))))))
