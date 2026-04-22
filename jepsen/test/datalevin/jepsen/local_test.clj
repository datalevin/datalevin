(ns datalevin.jepsen.local-test
  (:require
   [clojure.edn :as edn]
   [clojure.java.io :as io]
   [clojure.test :refer [deftest is]]
   [datalevin.core :as d]
   [datalevin.jepsen.local :as local]
   [datalevin.kv :as kv]
   [datalevin.server :as srv]
   [datalevin.util :as u])
  (:import
   [datalevin.db DB]
   [datalevin.server Server]
   [java.util UUID]
   [java.util.concurrent ConcurrentHashMap ConcurrentLinkedQueue TimeUnit]
   [java.util.concurrent.atomic AtomicBoolean]))

(defn- current-java-bin
  []
  (.getPath (io/file (System/getProperty "java.home") "bin" "java")))

(defn- last-nonblank-line
  [s]
  (some->> (clojure.string/split-lines (or s ""))
           reverse
           (some (fn [line]
                   (let [trimmed (clojure.string/trim line)]
                     (when-not (clojure.string/blank? trimmed)
                       trimmed))))))

(def ^:private wal-child-ready-timeout-ms 15000)
(def ^:private wal-child-process-timeout-ms 30000)

(defn- process-output
  [^Process process]
  (try
    (slurp (.getInputStream process) :encoding "UTF-8")
    (catch Exception _
      "")))

(defn- child-process-result
  [^Process process timeout-ms]
  (let [finished? (.waitFor process
                            (long timeout-ms)
                            TimeUnit/MILLISECONDS)]
    (if finished?
      (let [exit   (.exitValue process)
            output (process-output process)
            result (try
                     (some-> output last-nonblank-line edn/read-string)
                     (catch Exception _
                       nil))]
        {:ok? (zero? exit)
         :exit exit
         :output output
         :result result})
      (do
        (.destroy process)
        (when-not (.waitFor process 200 TimeUnit/MILLISECONDS)
          (.destroyForcibly process))
        {:ok? false
         :reason :timeout
         :timeout-ms timeout-ms
         :output (process-output process)}))))

(defn- start-clojure-child!
  [form]
  (let [cmd [(current-java-bin)
             "-cp"
             (System/getProperty "java.class.path")
             "clojure.main"
             "-e"
             form]
        process-builder (ProcessBuilder. ^java.util.List (mapv str cmd))]
    (.directory process-builder (io/file (System/getProperty "user.dir")))
    (.redirectErrorStream process-builder true)
    (.start process-builder)))

(defn- ha-test-server
  [dbs]
  (srv/->Server (AtomicBoolean. true)
                0
                ""
                0
                nil
                nil
                (ConcurrentLinkedQueue.)
                nil
                nil
                nil
                (ConcurrentHashMap.)
                (doto (ConcurrentHashMap.)
                  (.putAll dbs))))

(defn- assert-local-query-refreshes-ha-read-view!
  [db-state]
  (let [dir        (u/tmp-dir (str "jepsen-local-query-refresh-"
                                   (UUID/randomUUID)))
        db-name    "jepsen-local-query-refresh"
        query      '[:find ?v .
                     :where
                     [?e :register/key 0]
                     [?e :register/value ?v]]
        conn       (d/create-conn dir {:register/key {:db/valueType :db.type/long
                                                      :db/unique :db.unique/identity}
                                       :register/value {:db/valueType :db.type/long}})
        _          (d/transact! conn [{:register/key 0 :register/value 0}])
        stale-db   @conn
        store      (.-store ^DB stale-db)
        server     (ha-test-server {db-name (merge {:store store
                                                    :dt-db stale-db}
                                                   db-state)})
        cluster-id (keyword (str "local-query-refresh-" (UUID/randomUUID)))
        clusters*  @#'local/clusters]
    (swap! clusters* assoc cluster-id {:db-name db-name
                                       :servers {"n1" server}})
    (try
      (is (= 0 (local/local-query cluster-id "n1" query)))
      (d/transact! conn [{:register/key 0 :register/value 1000}])
      (is (= 1000 (local/local-query cluster-id "n1" query)))
      (finally
        (swap! clusters* dissoc cluster-id)
        (d/close conn)
        (u/delete-files dir)))))

(defn- wal-child-overlap-form
  [dir opts ready-path release-path]
  (let [form
        `(do
           (require '[clojure.java.io :as io]
                    '[datalevin.core :as d]
                    '[datalevin.kv :as kv])
           (let [db# (d/open-kv ~dir ~opts)]
             (try
               (d/open-dbi db# "a")
               (spit ~ready-path "ready")
               (loop [elapsed# 0]
                 (cond
                   (.exists (io/file ~release-path))
                   nil

                   (>= elapsed# 5000)
                   (throw (ex-info "timed out waiting for release"
                                   {:elapsed-ms elapsed#}))

                   :else
                   (do
                     (Thread/sleep 25)
                     (recur (+ elapsed# 25)))))
               (d/transact-kv db# [[:put "a" :k2 :v2]])
               (println (pr-str {:status :ok
                                 :lsns (mapv :lsn (kv/open-tx-log db# 1))
                                 :applied-lsn
                                 (get-in (kv/read-commit-marker db#)
                                         [:current :applied-lsn])}))
               (finally
                 (d/close-kv db#)))))]
    (pr-str form)))

(defn- wait-for-file
  [path timeout-ms]
  (let [deadline (+ (System/currentTimeMillis) (long timeout-ms))]
    (loop []
      (cond
        (u/file-exists path)
        true

        (< (System/currentTimeMillis) deadline)
        (do
          (Thread/sleep 25)
          (recur))

        :else
        false))))

(deftest wal-multi-process-writable-overlap-test
  (let [dir          (u/tmp-dir (str "jepsen-wal-multi-process-"
                                     (UUID/randomUUID)))
        ready-path   (str dir u/+separator+ "child.ready")
        release-path (str dir u/+separator+ "child.release")
        opts         {:wal? true
                      :wal-commit-marker? true
                      :snapshot-bootstrap-force? false
                      :wal-durability-profile :strict}]
    (try
      (let [db1 (d/open-kv dir opts)]
        (try
          (d/open-dbi db1 "a")
          (let [child (start-clojure-child!
                       (wal-child-overlap-form dir
                                               opts
                                               ready-path
                                               release-path))]
            (try
              (is (wait-for-file ready-path wal-child-ready-timeout-ms))
              (is (= :transacted
                     (d/transact-kv db1 [[:put "a" :k1 :v1]])))
              (spit release-path "go")
              (let [{:keys [ok? result output]}
                    (child-process-result child wal-child-process-timeout-ms)]
                (is ok? output)
                (is (= {:status :ok
                        :lsns [1 2]
                        :applied-lsn 2}
                       result))
                (is (= [1 2]
                       (mapv :lsn (kv/open-tx-log db1 1))))
                (is (= :v1
                       (d/get-value db1 "a" :k1)))
                (is (= :v2
                       (d/get-value db1 "a" :k2)))
                (is (:ok? (kv/verify-commit-marker! db1))))
              (finally
                (when-not (u/file-exists release-path)
                  (spit release-path "go"))
                (child-process-result child 1000))))
          (finally
            (d/close-kv db1))))
      (let [db2 (d/open-kv dir opts)]
        (try
          (d/open-dbi db2 "a")
          (is (= :v1
                 (d/get-value db2 "a" :k1)))
          (is (= :v2
                 (d/get-value db2 "a" :k2)))
          (finally
            (d/close-kv db2))))
      (finally
        (u/delete-files dir)))))

(deftest expected-disruption-write-failure-matches-transport-errors-test
  (let [active-test {:datalevin/nemesis-faults [:clock-skew-pause
                                                :leader-failover]}
        inactive-test {:datalevin/nemesis-faults []}
        transport-error "Unable to connect to server: Connection refused"
        control-timeout "Request to Datalevin server failed: \"HA control command timed out\""
        commit-confirmation-failure
        "Request to Datalevin server failed: \"HA write commit confirmation failed\""]
    (is (true? (boolean
                 (local/expected-disruption-write-failure?
                   active-test
                   transport-error))))
    (is (true? (boolean
                 (local/expected-disruption-write-failure?
                   active-test
                   {:error transport-error}))))
    (is (false? (boolean
                  (local/expected-disruption-write-failure?
                    inactive-test
                    transport-error))))
    (is (true? (boolean
                 (local/expected-disruption-write-failure?
                   active-test
                   control-timeout))))
    (is (true? (boolean
                 (local/expected-disruption-write-failure?
                   active-test
                   {:error commit-confirmation-failure}))))
    (is (false? (boolean
                  (local/expected-disruption-write-failure?
                    inactive-test
                    control-timeout))))))

(deftest local-query-uses-server-ha-read-view-test
  (assert-local-query-refreshes-ha-read-view!
   {:ha-role :follower
    :ha-authority (Object.)}))

(deftest local-query-uses-server-ha-read-view-without-authority-test
  (assert-local-query-refreshes-ha-read-view!
   {:ha-role :follower}))

(deftest local-query-uses-server-ha-read-view-for-leader-test
  (assert-local-query-refreshes-ha-read-view!
   {:ha-role :leader
    :ha-authority (Object.)}))

(deftest local-query-returns-unavailable-for-stopped-node-test
  (let [cluster-id (keyword (str "local-query-stopped-" (UUID/randomUUID)))
        query      '[:find ?e .
                     :where
                     [?e :db/ident ?ident]]
        clusters*  @#'local/clusters]
    (swap! clusters* assoc cluster-id {:db-name "jepsen-local-query-stopped"
                                       :servers {"n1" nil}})
    (try
      (is (= ::local/unavailable
             (local/local-query cluster-id "n1" query)))
      (finally
        (swap! clusters* dissoc cluster-id)))))
