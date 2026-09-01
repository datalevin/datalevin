(ns datalevin-bench.core-test
  (:require
   [clojure.edn :as edn]
   [clojure.java.io :as io]
   [clojure.string :as str]
   [clojure.test :refer [deftest is testing]]
   [datalevin-bench.core :as bench]
   [datalevin-bench.harness :as harness]
   [datalevin.core :as d]
   [datalevin.util :as u]
   [next.jdbc :as jdbc]
   [next.jdbc.result-set :as rs])
  (:import
   [java.io File StringWriter]
   [java.nio.file Files]
   [java.nio.file.attribute FileAttribute]
   [java.util UUID]
   [org.eclipse.collections.impl.list.mutable FastList]))

(defn- invoke-private
  [name & args]
  (apply (ns-resolve 'datalevin-bench.core name) args))

(defn- thrown-info
  [f]
  (try
    (f)
    nil
    (catch clojure.lang.ExceptionInfo e e)))

(defn- with-temp-root
  [prefix f]
  (let [^File root (.toFile
                     (Files/createTempDirectory
                       prefix (make-array FileAttribute 0)))]
    (try
      (f (.getPath root))
      (finally
        (when (.exists root)
          (u/delete-files root))))))

(defn- capture-output
  [f]
  (let [out    (StringWriter.)
        err    (StringWriter.)
        result (binding [*out* out
                         *err* err]
                 (f))]
    {:result result :out (str out) :err (str err)}))

(defn- run-pass
  [base-dir task & [overrides]]
  (invoke-private
    'run-write-pass
    (merge {:base-dir base-dir
            :batch 3
            :f task
            :total 7
            :report 0
            :in-flight 2
            :completion-timeout-ms 5000
            :seed 17}
           overrides)))

(deftest task-classification-covers-every-supported-spelling
  (doseq [[base kind async?]
          [["kv-sync" :kv false]
           ["kv-async" :kv true]
           ["dl-sync" :datalog false]
           ["dl-async" :datalog true]
           ["sql-tx" :sqlite false]]
          wal? [false true]]
    (let [task (str base (when wal? "-wal"))
          info (invoke-private 'task-info task)]
      (is (= task (:name info)))
      (is (= base (:base info)))
      (is (= kind (:kind info)))
      (is (= async? (:async? info)))
      (is (= wal? (:wal? info)))))
  (is (= "dl-sync" (:name (invoke-private 'task-info 'dl-sync))))
  (is (= "dl-sync" (:name (invoke-private 'task-info :dl-sync)))))

(deftest unsupported-tasks-fail-with-the-allowed-surface
  (is (= ":f is required"
         (ex-message (thrown-info #(invoke-private 'task-info nil)))))
  (let [error (thrown-info #(invoke-private 'task-info 'unknown))]
    (is (= "Unsupported write benchmark task" (ex-message error)))
    (is (= 10 (count (:allowed (ex-data error)))))
    (is (some #{"dl-async-wal"} (:allowed (ex-data error))))))

(deftest async-in-flight-window-caps-requests-and-write-volume
  (let [effective (ns-resolve 'datalevin-bench.core
                              'effective-in-flight-limit)]
    (is (= 1000 (effective 1 1000 100000)))
    (is (= 1000 (effective 10 1000 100000)))
    (is (= 1000 (effective 100 1000 100000)))
    (is (= 100 (effective 1000 1000 100000)))
    (is (= 7 (effective 1000 7 100000)))
    (is (= 1 (effective 200000 1000 100000)))))

(deftest durability-profiles-are-context-sensitive
  (doseq [profile [:strict :relaxed :extra]]
    (is (nil? (invoke-private 'validate-durability-profile! true profile)))
    (is (= profile
           (invoke-private 'effective-durability-profile true profile))))
  (is (= :strict (invoke-private 'effective-durability-profile true nil)))
  (is (nil? (invoke-private 'effective-durability-profile false nil)))
  (is (= ":durability-profile is only valid for WAL tasks"
         (ex-message
           (thrown-info
             #(invoke-private 'validate-durability-profile! false :strict)))))
  (is (str/includes?
        (ex-message
          (thrown-info
            #(invoke-private 'validate-durability-profile! true :unknown)))
        "must be one of")))

(deftest kv-non-durable-profiles-are-restricted-to-non-wal-sync
  (let [kv-sync     (invoke-private 'task-info 'kv-sync)
        invalid-tasks (mapv #(invoke-private 'task-info %)
                            ['kv-async 'kv-sync-wal 'dl-sync])]
    (doseq [profile [:nometasync :nosync :writemap-mapasync]]
      (is (nil? (invoke-private 'validate-kv-non-durable-profile!
                                kv-sync profile))))
    (is (str/includes?
          (ex-message
            (thrown-info
              #(invoke-private 'validate-kv-non-durable-profile!
                               kv-sync :unknown)))
          "must be one of"))
    (doseq [task invalid-tasks]
      (is (= ":kv-non-durable-profile is only valid for non-WAL kv-sync"
             (ex-message
               (thrown-info
                 #(invoke-private 'validate-kv-non-durable-profile!
                                  task :nosync))))))))

(deftest storage-options-preserve-the-selected-durability
  (let [kv-default (invoke-private 'store-opts false nil)
        kv-wal     (invoke-private 'store-opts true :strict)
        dl-default (invoke-private 'datalog-opts false nil)
        dl-wal     (invoke-private 'datalog-opts true :relaxed)]
    (is (false? (:wal? kv-default)))
    (is (not (contains? kv-default :wal-durability-profile)))
    (is (= :strict (:wal-durability-profile kv-wal)))
    (is (false? (:wal? dl-default)))
    (is (not (contains? dl-default :wal-durability-profile)))
    (is (= :relaxed (:wal-durability-profile dl-wal)))
    (is (= (:flags kv-default) (get-in dl-default [:kv-opts :flags]))))
  (is (= #{:nordahead :notls :nometasync}
         (:flags (invoke-private 'store-opts false nil :nometasync))))
  (is (= #{:nordahead :notls :nosync}
         (:flags (invoke-private 'store-opts false nil :nosync))))
  (is (= #{:nordahead :notls :writemap :mapasync}
         (:flags
           (invoke-private 'store-opts false nil :writemap-mapasync))))
  (is (= "FULL" (invoke-private 'sql-sync-mode-for nil)))
  (is (= "FULL" (invoke-private 'sql-sync-mode-for :strict)))
  (is (= "NORMAL" (invoke-private 'sql-sync-mode-for :relaxed)))
  (is (= "EXTRA" (invoke-private 'sql-sync-mode-for :extra))))

(deftest benchmark-targets-encode-relevant-run-settings
  (let [root       (.getPath (io/file "root"))
        dl-info    (invoke-private 'task-info 'dl-async-wal)
        sqlite-info (invoke-private 'task-info 'sql-tx-wal)]
    (is (= (.getPath (io/file root "dl-async-wal-10-t4-strict"))
           (invoke-private 'benchmark-target root dl-info 10 4 :strict)))
    (is (= (.getPath (io/file root "sqlite-wal-100-relaxed"))
           (invoke-private 'benchmark-target
                           root sqlite-info 100 1 :relaxed)))
    (is (= "kv-sync-1"
           (invoke-private 'benchmark-target
                           nil (invoke-private 'task-info 'kv-sync)
                           1 1 nil)))
    (is (= (.getPath (io/file root "kv-sync-100-nosync"))
           (invoke-private 'benchmark-target
                           root (invoke-private 'task-info 'kv-sync)
                           100 1 nil :nosync)))))

(deftest deterministic-person-records-have-a-stable-shape
  (let [first-id    (invoke-private 'deterministic-uuid 42 1)
        second-id   (invoke-private 'person-id 42 2)
        generator-a (invoke-private 'pure-person-generator 42)
        generator-b (invoke-private 'pure-person-generator 42)
        person-a    (generator-a)
        person-b    (generator-b)]
    (is (= "4f0a61d9-c798-d8ca-bdd7-32262feb6e95" first-id))
    (is (= "fb2bf499-6809-baf7-28ef-e333b266f103" second-id))
    (is (= person-a person-b))
    (is (= second-id (:person-id person-a)))
    (is (= #{:person-id :first-name :last-name :age} (set (keys person-a))))
    (is (string? (:first-name person-a)))
    (is (string? (:last-name person-a)))
    (is (<= 18 (:age person-a) 90))
    (is (not= person-a (generator-a)))
    (is (not= second-id (invoke-private 'person-id 43 2)))
    (is (= second-id (str (UUID/fromString second-id))))))

(deftest mixed-datalog-read-uses-a-relation-find-spec
  (let [query (var-get
                (ns-resolve 'datalevin-bench.core
                            'datalog-mixed-person-query))]
    (is (= '[:find ?first-name ?last-name ?age]
           (subvec query 0 4)))
    (is (= :in (nth query 4)))))

(deftest fresh-target-validation-covers-files-directories-and-sidecars
  (with-temp-root
    "write-bench-target-"
    (fn [root]
      (let [target (str (io/file root "nested" "db"))]
        (is (nil? (invoke-private 'ensure-fresh-target! target false)))
        (is (.isDirectory (.getParentFile (io/file target))))
        (is (not (.exists (io/file target))))
        (spit target "occupied")
        (is (= "Benchmark target already exists; use a fresh directory"
               (ex-message
                 (thrown-info
                   #(invoke-private 'ensure-fresh-target! target false))))))
      (let [target  (str (io/file root "sqlite.db"))
            sidecar (str target "-wal")]
        (spit sidecar "occupied")
        (let [error (thrown-info
                      #(invoke-private 'ensure-fresh-target! target true))]
          (is (= sidecar (:existing (ex-data error))))
          (is (= target (:target (ex-data error)))))
        (is (= "Mixed benchmark target does not exist"
               (ex-message
                 (thrown-info
                   #(invoke-private 'ensure-existing-target! target)))))
        (spit target "present")
        (is (nil? (invoke-private 'ensure-existing-target! target)))))))

(deftest sqlite-pragmas-are-applied-and-reported
  (with-open [conn (jdbc/get-connection
                     {:dbtype "sqlite" :dbname ":memory:"})]
    (let [strict (invoke-private 'configure-sqlite!
                                 conn "MEMORY" "FULL" :strict)]
      (is (= "memory" (:journal-mode strict)))
      (is (= "full" (:synchronous strict)))
      (is (false? (:fullfsync strict)))
      (is (false? (:checkpoint-fullfsync strict)))
      (is (integer? (:busy-timeout-ms strict))))
    (let [extra (invoke-private 'configure-sqlite!
                                conn "MEMORY" "EXTRA" :extra)]
      (is (= "extra" (:synchronous extra)))
      (is (true? (:fullfsync extra)))
      (is (true? (:checkpoint-fullfsync extra))))
    (let [relaxed (invoke-private 'configure-sqlite!
                                  conn "MEMORY" "NORMAL" :relaxed)]
      (is (= "normal" (:synchronous relaxed)))
      (is (false? (:fullfsync relaxed)))
      (is (false? (:checkpoint-fullfsync relaxed))))
    (is (= 10000
           (invoke-private 'configure-sqlite-busy-timeout! conn 10000)))))

(deftest sqlite-person-identity-has-a-separate-auto-assigned-rowid
  (with-open [conn (jdbc/get-connection
                     {:dbtype "sqlite" :dbname ":memory:"})]
    (invoke-private 'create-sqlite-person-table! conn)
    (jdbc/execute! conn
                   ["INSERT INTO person
                     (person_id, first_name, last_name, age)
                     VALUES (?, ?, ?, ?)"
                    "person-1" "Ada" "Lovelace" 36])
    (is (= 1
           (invoke-private
             'result-value
             (jdbc/execute-one! conn
                                ["SELECT rowid FROM person
                                  WHERE person_id = ?" "person-1"]))))
    (is (= "integer"
           (invoke-private
             'result-value
             (jdbc/execute-one! conn
                                ["SELECT typeof(age) FROM person
                                  WHERE person_id = ?" "person-1"]))))
    (let [indexes (jdbc/execute! conn ["PRAGMA index_list('person')"])]
      (is (some #{"sqlite_autoindex_person_1"} (mapcat vals indexes))))))

(deftest sqlite-adapter-maintains-each-value-lookup-index
  (with-temp-root
    "write-bench-sqlite-indexed-"
    (fn [root]
      (let [result   (run-pass root 'sql-tx)
            expected [{:name "person_first_name_idx" :column "first_name"}
                      {:name "person_last_name_idx" :column "last_name"}
                      {:name "person_age_idx" :column "age"}]]
        (is (= expected
               (get-in result [:storage :configuration :secondary-indexes])))
        (with-open [conn (jdbc/get-connection
                           {:dbtype "sqlite" :dbname (:target result)})]
          (let [indexes (jdbc/execute!
                          conn ["PRAGMA index_list('person')"]
                          {:builder-fn rs/as-unqualified-lower-maps})]
            (is (= (set (map :name expected))
                   (set (keep :name
                              (filter #(= "c" (:origin %)) indexes))))))
          (doseq [{:keys [name column]} expected]
            (let [columns (jdbc/execute!
                            conn [(str "PRAGMA index_info('" name "')")]
                            {:builder-fn rs/as-unqualified-lower-maps})]
              (is (= [column] (mapv :name columns))))))))))

(deftest sqlite-prepared-batches-are-atomic-and-reusable
  (let [conn (jdbc/get-connection {:dbtype "sqlite" :dbname ":memory:"})]
    (invoke-private 'create-sqlite-person-table! conn)
    (let [writer (invoke-private 'open-sqlite-batch-writer conn)
          person (fn [id]
                   {:person_id id
                    :first_name "Ada"
                    :last_name "Lovelace"
                    :age 36})]
      (try
        (is (false? (.getAutoCommit conn)))
        (let [result (invoke-private
                       'transact-sqlite-person-batch!
                       writer
                       (doto (FastList.)
                         (.add (person "person-1"))
                         (.add (person "person-2"))))]
          (is (= 2 (alength ^ints result)))
          (is (= 2 (invoke-private 'sqlite-count conn))))

        (testing "a failed batch rolls back rows executed before the failure"
          (is (thrown?
                Throwable
                (invoke-private
                  'transact-sqlite-person-batch!
                  writer
                  (doto (FastList.)
                    (.add (person "person-3"))
                    (.add (person "person-1"))))))
          (is (= 2 (invoke-private 'sqlite-count conn))))

        (testing "the prepared statement remains reusable after rollback"
          (invoke-private
            'transact-sqlite-person-batch!
            writer
            (doto (FastList.) (.add (person "person-4"))))
          (is (= 3 (invoke-private 'sqlite-count conn))))
        (finally
          (invoke-private 'close-sqlite-batch-writer! writer))))))

(deftest sqlite-mixed-statements-are-prepared-once-and-reusable
  (let [conn (jdbc/get-connection {:dbtype "sqlite" :dbname ":memory:"})]
    (invoke-private 'create-sqlite-person-table! conn)
    (let [writer (invoke-private 'open-sqlite-mixed-writer conn)]
      (try
        (is (true? (.getAutoCommit conn)))
        (is (= 1
               (invoke-private
                 'upsert-sqlite-person! writer
                 ["person-1" "Ada" "Lovelace" 36])))
        (is (= {:first_name "Ada" :last_name "Lovelace" :age 36}
               (invoke-private 'read-sqlite-person! writer "person-1")))
        (is (= 1
               (invoke-private
                 'upsert-sqlite-person! writer
                 ["person-1" "Grace" "Hopper" 85])))
        (is (= {:first_name "Grace" :last_name "Hopper" :age 85}
               (invoke-private 'read-sqlite-person! writer "person-1")))
        (is (nil?
              (invoke-private 'read-sqlite-person! writer "missing")))
        (is (= 1 (invoke-private 'sqlite-count conn)))
        (finally
          (invoke-private 'close-sqlite-mixed-writer! writer))))))

(deftest sqlite-journal-mismatches-are-fatal
  (with-open [conn (jdbc/get-connection
                     {:dbtype "sqlite" :dbname ":memory:"})]
    (is (= "SQLite did not enter the requested journal mode"
           (ex-message
             (thrown-info
               #(invoke-private 'configure-sqlite!
                                conn "WAL" "FULL" :strict)))))))

(deftest invalid-write-options-fail-before-creating-a-target
  (with-temp-root
    "write-bench-invalid-"
    (fn [root]
      (let [valid {:base-dir root
                   :batch 1
                   :f 'kv-sync
                   :total 1
                   :report 0}
            invalid-options
            [(dissoc valid :f)
             (assoc valid :f 'unknown)
             (assoc valid :batch 0)
             (assoc valid :threads 0)
             (assoc valid :total 0)
             (assoc valid :report -1)
             (assoc valid :in-flight 0)
             (assoc valid :in-flight-writes 0)
             (assoc valid :completion-timeout-ms 0)
             (assoc valid :seed 1.5)
             (assoc valid :durability-profile :unknown)
             (assoc valid :durability-profile :strict)
             (assoc valid :kv-non-durable-profile :unknown)
             (assoc valid :f 'kv-async
                    :kv-non-durable-profile :nosync)
             (assoc valid :f 'kv-sync-wal
                    :kv-non-durable-profile :nosync)
             (assoc valid :f 'dl-sync
                    :kv-non-durable-profile :nosync)
             (assoc valid :f 'dl-sync :threads 2)]]
        (doseq [opts invalid-options]
          (is (instance? clojure.lang.ExceptionInfo
                         (thrown-info
                           #(invoke-private 'run-write-pass opts))))
          (is (empty? (seq (.listFiles (io/file root))))))))))

(deftest default-storage-adapters-complete-and-verify
  (with-temp-root
    "write-bench-default-"
    (fn [root]
      (doseq [[task engine async?]
              [['kv-sync :datalevin-kv false]
               ['kv-async :datalevin-kv true]
               ['dl-sync :datalevin-datalog false]
               ['dl-async :datalevin-datalog true]
               ['sql-tx :sqlite false]]]
        (testing (name task)
          (let [result (run-pass root task)]
            (is (= 7 (:writes result)))
            (is (= 3 (:requests result)))
            (is (= (name task) (:task result)))
            (is (= async? (:async? result)))
            (if async?
              (do
                (is (= 2 (:in-flight result)))
                (is (= 2 (:in-flight-request-limit result)))
                (is (= 100000 (:in-flight-write-limit result))))
              (do
                (is (nil? (:in-flight result)))
                (is (nil? (:in-flight-request-limit result)))
                (is (nil? (:in-flight-write-limit result)))))
            (is (nil? (:durability-profile result)))
            (is (= engine (get-in result [:storage :engine])))
            (when (= :sqlite engine)
              (is (false? (get-in result
                                  [:storage :configuration :auto-commit])))
              (is (= :prepared-jdbc-batch
                     (get-in result
                             [:storage :configuration :insert-mode])))
              (is (= 3
                     (count
                       (get-in result
                               [:storage :configuration
                                :secondary-indexes])))))
            (is (.exists (io/file (:target result))))))))))

(deftest non-durable-kv-sync-adapters-record-effective-env-flags
  (with-temp-root
    "write-bench-kv-non-durable-"
    (fn [root]
      (doseq [[profile expected]
              [[:nometasync #{:nordahead :notls :nometasync}]
               [:nosync #{:nordahead :notls :nosync}]
               [:writemap-mapasync
                #{:nordahead :notls :writemap :mapasync}]]]
        (testing (name profile)
          (let [result (run-pass root 'kv-sync
                                 {:kv-non-durable-profile profile})]
            (is (= 7 (:writes result)))
            (is (= profile (:kv-non-durable-profile result)))
            (is (nil? (:durability-profile result)))
            (is (false? (get-in result [:storage :configuration :wal?])))
            (is (= expected
                   (set (get-in result
                                [:storage :configuration :env-flags]))))
            (is (= profile
                   (get-in result
                           [:storage :configuration
                            :kv-non-durable-profile])))))))))

(deftest every-sync-adapter-persists-the-same-person-record
  (with-temp-root
    "write-bench-person-"
    (fn [root]
      (let [seed     17
            expected (invoke-private 'person-record seed 2 1)
            person-id (:person-id expected)]
        (doseq [task ['kv-sync 'dl-sync 'sql-tx]]
          (let [result (run-pass root task {:batch 2 :total 3 :seed seed})]
            (case task
              kv-sync
              (let [db (d/open-kv (:target result))]
                (try
                  (d/open-dbi db bench/max-write-dbi)
                  (is (= (invoke-private 'person-value expected)
                         (d/get-value db bench/max-write-dbi person-id
                                      :string :data)))
                  (finally
                    (d/close-kv db))))

              dl-sync
              (let [conn  (d/get-conn (:target result))
                    query '[:find (pull ?e [:person/id
                                            :person/first-name
                                            :person/last-name
                                            :person/age]) .
                            :in $ ?person-id
                            :where [?e :person/id ?person-id]]]
                (try
                  (is (= (invoke-private 'datalog-person expected)
                         (d/q query (d/db conn) person-id)))
                  (finally
                    (d/close conn))))

              sql-tx
              (with-open [conn (jdbc/get-connection
                                 {:dbtype "sqlite"
                                  :dbname (:target result)})]
                (is (= (invoke-private 'sqlite-person expected)
                       (jdbc/execute-one!
                         conn
                         ["SELECT person_id, first_name, last_name, age
                           FROM person WHERE person_id = ?" person-id]
                         {:builder-fn rs/as-unqualified-lower-maps})))))))))))

(deftest datalog-verification-uses-exact-queries
  (with-temp-root
    "write-bench-exact-datalog-count-"
    (fn [root]
      (with-redefs [d/count-datoms
                    (fn [& _]
                      (throw
                        (ex-info "range counting must not verify a benchmark"
                                 {})))]
        (let [initial (run-pass (str (io/file root "pure")) 'dl-sync
                                {:batch 4 :total 8})]
          (is (= 8 (:writes initial)))
          (is (nil?
                (:result
                  (capture-output
                    #(bench/mixed
                       {:dir (:target initial)
                        :f 'dl-sync
                        :total 4
                        :initial-total 8
                        :report 0
                        :completion-timeout-ms 5000
                        :seed 17})))))
          (is (str/includes?
                (:out
                  (capture-output
                    #(bench/dl-init
                       {:dir (str (io/file root "bulk"))
                        :total 8
                        :seed 17})))
                "Loaded 8 entities")))))))

(deftest strict-wal-storage-adapters-complete-and-verify
  (with-temp-root
    "write-bench-wal-"
    (fn [root]
      (doseq [[task engine async?]
              [['kv-sync-wal :datalevin-kv false]
               ['kv-async-wal :datalevin-kv true]
               ['dl-sync-wal :datalevin-datalog false]
               ['dl-async-wal :datalevin-datalog true]
               ['sql-tx-wal :sqlite false]]]
        (testing (name task)
          (let [result (run-pass root task {:durability-profile :strict})]
            (is (= 7 (:writes result)))
            (is (= 3 (:requests result)))
            (is (= :strict (:durability-profile result)))
            (is (= async? (:async? result)))
            (is (= engine (get-in result [:storage :engine])))
            (when (= :sqlite engine)
              (is (= "wal" (get-in result
                                    [:storage :configuration :journal-mode])))
              (is (= "full" (get-in result
                                     [:storage :configuration :synchronous])))
              (let [indexes (get-in result
                                    [:storage :configuration
                                     :secondary-indexes])]
                (is (= 3 (count indexes)))))))))))

(deftest default-wal-storage-adapters-use-strict-profile
  (with-temp-root
    "write-bench-default-wal-"
    (fn [root]
      (doseq [[task engine async?]
              [['kv-sync-wal :datalevin-kv false]
               ['kv-async-wal :datalevin-kv true]
               ['dl-sync-wal :datalevin-datalog false]
               ['dl-async-wal :datalevin-datalog true]
               ['sql-tx-wal :sqlite false]]]
        (testing (name task)
          (let [result (run-pass root task)]
            (is (= 7 (:writes result)))
            (is (= 3 (:requests result)))
            (is (= :strict (:durability-profile result)))
            (is (= async? (:async? result)))
            (is (= engine (get-in result [:storage :engine])))
            (when (= :sqlite engine)
              (is (= "wal" (get-in result
                                    [:storage :configuration :journal-mode])))
              (is (= "full" (get-in result
                                     [:storage :configuration :synchronous])))
              (let [indexes (get-in result
                                    [:storage :configuration
                                     :secondary-indexes])]
                (is (= 3 (count indexes)))))))))))

(deftest sqlite-multi-caller-run-uses-uniform-busy-timeouts
  (with-temp-root
    "write-bench-sqlite-mt-"
    (fn [root]
      (let [result (run-pass root 'sql-tx-wal {:threads 2})]
        (is (= 7 (:writes result)))
        (is (= 2 (:threads result)))
        (is (= 10000
               (get-in result [:storage :configuration :busy-timeout-ms])))
        (is (= "wal"
               (get-in result [:storage :configuration :journal-mode])))
        (is (= 3
               (count (get-in result
                              [:storage :configuration
                               :secondary-indexes]))))))))

(deftest completed-targets-cannot-be-silently-reused
  (with-temp-root
    "write-bench-reuse-"
    (fn [root]
      (let [opts {:base-dir root
                  :batch 2
                  :f 'kv-sync
                  :total 3
                  :report 0}]
        (is (= 3 (:writes (invoke-private 'run-write-pass opts))))
        (is (= "Benchmark target already exists; use a fresh directory"
               (ex-message
                 (thrown-info
                   #(invoke-private 'run-write-pass opts)))))))))

(deftest suite-enforces-the-one-pass-protocol-before-writing
  (with-temp-root
    "write-bench-suite-invalid-"
    (fn [root]
      (let [base {:base-dir root :batch 1 :f 'kv-sync :report 0}]
        (is (= "Warmup passes are not part of the write benchmark protocol"
               (ex-message
                 (thrown-info
                   #(bench/suite
                      (assoc base
                             :warmup-writes 1))))))
        (is (= ":total and :measurement-writes disagree"
               (ex-message
                 (thrown-info
                   #(bench/suite
                      (assoc base :total 1 :measurement-writes 2))))))
        (is (empty? (seq (.listFiles (io/file root))))))))
  (is (= ":base-dir must be a non-blank path"
         (ex-message
           (thrown-info
             #(bench/suite {:base-dir "" :batch 1 :f 'kv-sync}))))))

(deftest write-persists-an-auditable-one-pass-manifest
  (with-temp-root
    "write-bench-measurement-"
    (fn [root]
      (let [{:keys [result out err]}
            (capture-output
              #(bench/write {:base-dir root
                             :batch 2
                             :f 'kv-sync
                             :total 3
                             :report 0
                             :seed 17}))
            manifest-file (io/file root "results.edn")
            manifest      (edn/read-string (slurp manifest-file))
            output-lines  (str/split-lines out)]
        (is (nil? result))
        (is (= 2 (count output-lines)))
        (is (= harness/csv-header (first output-lines)))
        (is (str/starts-with? (second output-lines) "measurement,3,2,"))
        (is (str/includes? err "Wrote benchmark manifest"))
        (is (= {:warmup-passes 0
                :measurement-passes 1
                :fresh-database-per-pass true
                :measurement-writes 3}
               (:protocol manifest)))
        (is (= 3 (get-in manifest [:measurement :writes])))
        (is (= :small-person-record
               (get-in manifest [:measurement :workload :name])))
        (is (= :string
               (get-in manifest
                       [:measurement :workload :identity :type])))
        (is (.exists (io/file (get-in manifest [:measurement :target]))))
        (is (not (contains? manifest :warmup)))
        (is (seq (get-in manifest [:environment :jvm-arguments])))
        (is (= "Benchmark result manifest already exists"
               (ex-message
                 (thrown-info
                   #(bench/write {:base-dir root
                                  :batch 2
                                  :f 'kv-sync
                                  :total 3
                                  :report 0})))))))))

(deftest suite-is-a-one-pass-measurement-alias
  (with-temp-root
    "write-bench-suite-alias-"
    (fn [root]
      (let [output (capture-output
                     #(bench/suite {:base-dir root
                                    :batch 2
                                    :f 'kv-sync
                                    :measurement-writes 3
                                    :report 0}))
            manifest (edn/read-string
                       (slurp (io/file root "results.edn")))]
        (is (nil? (:result output)))
        (is (str/includes? (:out output) "measurement,3,2,"))
        (is (= 0 (get-in manifest [:protocol :warmup-passes])))
        (is (= 3 (get-in manifest [:measurement :writes])))))))

(deftest mixed-mode-runs-the-real-async-and-sql-adapters
  (with-temp-root
    "write-bench-mixed-"
    (fn [root]
      (doseq [task ['kv-async 'dl-async 'sql-tx]]
        (testing (name task)
          (let [initial (run-pass root task {:batch 4 :total 8})
                output  (capture-output
                          #(bench/mixed
                             {:dir (:target initial)
                              :f task
                              :total 4
                              :initial-total 8
                              :report 0
                              :completion-timeout-ms 5000
                              :seed 17}))]
            (is (nil? (:result output)))
            (is (= 2 (count (str/split-lines (:out output)))))
            (is (str/includes? (:out output) "measurement,4,4,"))))))))

(deftest bulk-initialization-verifies-and-persists-both-attributes
  (with-temp-root
    "write-bench-init-"
    (fn [root]
      (let [target (str (io/file root "db"))
            output (capture-output
                     #(bench/dl-init {:dir target :total 5 :seed 17}))]
        (is (str/includes? (:out output) "Loaded 5 entities"))
        (let [conn (d/get-conn target)]
          (try
            (is (= 5 (d/count-datoms @conn nil :person/id nil)))
            (is (= 5 (d/count-datoms @conn nil :person/first-name nil)))
            (is (= 5 (d/count-datoms @conn nil :person/last-name nil)))
            (is (= 5 (d/count-datoms @conn nil :person/age nil)))
            (finally
              (d/close conn))))
        (is (= "Benchmark target already exists; use a fresh directory"
               (ex-message
                 (thrown-info
                   #(bench/dl-init {:dir target :total 5 :seed 17})))))))))

(deftest environment-metadata-identifies-the-runtime
  (let [metadata (invoke-private 'environment-metadata)]
    (is (string? (:os-name metadata)))
    (is (string? (:architecture metadata)))
    (is (pos? (:available-processors metadata)))
    (is (string? (:java-version metadata)))
    (is (string? (:clojure-version metadata)))
    (is (string? (:datalevin-version metadata)))
    (is (string? (:native-version metadata)))
    (is (vector? (:jvm-arguments metadata)))))
