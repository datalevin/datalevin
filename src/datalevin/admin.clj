;; Copyright (c) Huahai Yang. All rights reserved.
;; Distributed under the Eclipse Public License 2.0.
(ns ^:no-doc datalevin.admin
  "Synchronous administration of the contacted server endpoint."
  (:refer-clojure :exclude [run!])
  (:require [clojure.string :as str]
            [datalevin.client :as client]
            [datalevin.util :as u])
  (:import [java.io File IOException]
           [java.nio.file Path]))

(def cli-options
  [[nil "--uri-env VAR" "Environment variable containing the server URI"
    :default "DATALEVIN_URI"
    :validate [#(not (str/blank? %)) "Must name an environment variable"]]
   [nil "--timeout-ms MS" "Timeout per authentication or request, not the whole sweep"
    :default 60000 :parse-fn #(Long/parseLong %)
    :validate [#(<= 1 (long %) Integer/MAX_VALUE) "Must be a positive 32-bit integer"]]
   [nil "--database NAME" "Named database; repeat to select more databases"
    :assoc-fn (fn [options key value]
                (update options key (fnil conj []) (u/lisp-case value)))
    :validate [#(not (str/blank? %)) "Must name a database"]]
   [nil "--all-databases" "Select the contacted node's complete database catalog"]
   [nil "--dry-run" "Plan WAL GC without deleting segments (wal-gc only)"]])

(def help
  "Command admin - administer databases on the contacted server.

Usage: dtlv admin [options] status (--all-databases | --database NAME ...)
       dtlv admin [options] wal-gc (--all-databases | --database NAME ...) [--dry-run]

Options:
  --uri-env VAR       Server URI environment variable (default DATALEVIN_URI)
  --timeout-ms MS     Per-request timeout in milliseconds (default 60000)
  --database NAME     Select a database; repeatable, normalized and deduplicated
  --all-databases     Select the contacted node's catalog, including closed DBs
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
WAL-disabled and rollback GC skips exit 0. Standalone backup is not yet available.

Example:
  dtlv admin --uri-env DATALEVIN_URI status --database orders --database users")

(defn validate-options
  [options arguments]
  (let [names (vec (distinct (:database options)))
        error (cond
                (not (and (= 1 (count arguments)) (#{"status" "wal-gc"} (first arguments))))
                "Expected the admin status or wal-gc subcommand"
                (and (:dry-run options) (not= "wal-gc" (first arguments)))
                "--dry-run is only supported by wal-gc"
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

(defn- safe-string
  [uri value]
  (-> (if (str/blank? uri) value (str/replace value uri "[redacted-uri]"))
      (str/replace #"(?i)([a-z][a-z0-9+.-]*://)[^\s/@]+@" "$1[redacted]@")
      (str/replace #"(?<![\w:/])(?:/[\w.][^\s\"',;)}\]]*|[A-Za-z]:[\\/][^\s\"',;)}\]]*)"
                   "[redacted-path]")))

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
  [request connection]
  (let [names (request connection :list-databases [])
        open-names (set (request connection :list-databases-in-use []))
        catalog (into {} (map (fn [[name type id]]
                               [name {:db-name name :db-type type :db-id id}]))
                      (request connection :query-system [catalog-query []]))]
    (mapv (fn [name]
            (assoc (get catalog name {:db-name name :db-id nil})
                   :open? (contains? open-names name)))
          (sort names))))

(defn- terminal-error?
  [error]
  (let [causes (take-while some? (iterate ex-cause error))]
    (boolean
      (some (fn [cause]
              (let [data (ex-data cause)
                    code (or (:error data) (get-in data [:err-data :error]))]
                (or (instance? IOException cause)
                    (instance? InterruptedException cause)
                    (#{:admin/unsupported-server :socket/timeout :client/closed} code)
                    (:termination-reason data)
                    (re-find #"(?i)authentication fail" (or (ex-message cause) "")))))
            causes))))

(defn- read-status
  ([request connection target] (read-status request connection target true))
  ([request connection {:keys [db-name db-id db-type] :as target} runtime?]
  (when (and (contains? target :db-id) (nil? db-id))
    (throw (ex-info "Database disappeared during discovery"
                    {:error :admin/database-not-found :db-name db-name})))
  (let [result (try
                 (request connection :admin-status [db-name {:runtime? runtime?}])
                 (catch Exception e
                   (if (str/includes? (or (:server-message (ex-data e)) (ex-message e) "")
                                      "Unknown message type :admin-status")
                     (throw (ex-info "Server does not support admin status"
                                     {:error :admin/unsupported-server} e))
                     (throw e))))]
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
    result)))

(defn- sweep
  [request connection targets emit]
  (loop [targets (seq targets) failed? false]
    (if-let [target (first targets)]
      (let [outcome (try
                      {:result (read-status request connection target)}
                      (catch Exception e
                        {:error {:message (ex-message e) :data (ex-data e)}
                         :stop? (terminal-error? e)}))]
        (emit (merge {:db-name (:db-name target) :operation :status}
                     (dissoc outcome :stop?)))
        (if (:stop? outcome)
          1
          (recur (next targets) (or failed? (contains? outcome :error)))))
      (if failed? 1 0))))

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
                 :unsupported? (= :admin/unsupported-server (:error (ex-data e)))}))
            attempted? (or attempted? @sent?)]
        (emit (merge {:db-name (:db-name target) :operation :wal-gc}
                     (select-keys outcome [:result :error])))
        (if (:stop? outcome)
          (if (and (:unsupported? outcome) (not attempted?)) 2 1)
          (recur (next targets)
                 (or failed? (:error outcome) (not (gc-success? (:result outcome))))
                 attempted?)))
      (if failed? 1 0))))

(defn run!
  "Execute an admin sweep, returning an exit code. Dependencies are injectable
  for transport, output and environment tests; normal calls use the real client."
  ([options arguments] (run! options arguments {}))
  ([options arguments dependencies]
   (let [{:keys [getenv connect disconnect request maintenance emit diagnostic]}
         (merge {:getenv u/env :connect client/new-client
                 :disconnect client/close-client :request client/endpoint-request
                 :maintenance client/admin-gc!
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
                              (connect uri {:pool-size 1 :time-out (or (:timeout-ms options) 60000)})
                              (catch Exception _ nil))]
             (if-not connection
               (do (diagnostic "Unable to connect or authenticate to the server") 2)
               (let [started? (volatile! false)]
                 (try
                   (let [targets (if (:all-databases options)
                                   (all-targets request connection)
                                   (mapv #(hash-map :db-name %) (:database options)))]
                     (vreset! started? true)
                     (if (= "wal-gc" (first arguments))
                       (gc-sweep request maintenance connection targets options #(emit (sanitize uri %)))
                       (sweep request connection targets #(emit (sanitize uri %)))))
                   (catch Exception e
                     (diagnostic (str "Admin status failed: "
                                      (safe-string uri (or (ex-message e) "Error"))))
                     (if @started? 1 2))
                   (finally
                     (try (disconnect connection)
                          (catch Exception _
                            (diagnostic "Unable to close the admin client session"))))))))))))))
