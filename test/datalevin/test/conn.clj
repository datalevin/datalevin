(ns datalevin.test.conn
  (:require
   [clojure.edn :as edn]
   [clojure.java.io :as io]
   [clojure.string :as str]
   [datalevin.test.core :as tdc :refer [db-fixture]]
   [clojure.test :refer [deftest testing is use-fixtures]]
   [datalevin.binding.cpp :as cpp]
   [datalevin.conn :as dc]
   [datalevin.core :as d]
   [datalevin.db :as db]
   [datalevin.constants :as c]
   [datalevin.kv :as kv]
   [datalevin.util :as u])
  (:import [java.util Date UUID]
           [java.util.concurrent TimeUnit]))

(use-fixtures :each db-fixture)

(deftest test-close
  (let [dir  (u/tmp-dir (str "test-" (UUID/randomUUID)))
        conn (d/create-conn dir)]
    (is (not (d/closed? conn)))
    (d/close conn)
    (is (d/closed? conn))
    (is (nil? @conn))
    (u/delete-files dir)))

(deftest test-update-schema
  (let [dir1  (u/tmp-dir (str "test-" (UUID/randomUUID)))
        dir2  (u/tmp-dir (str "test-" (UUID/randomUUID)))
        conn1 (d/create-conn dir1)
        aid0   (count c/implicit-schema)
        s     {:a/b {:db/valueType :db.type/string}}
        s1    {:c/d {:db/valueType :db.type/string}}
        txs   [{:c/d "cd" :db/id -1}
               {:a/b "ab" :db/id -2}]
        conn2 (d/create-conn dir2 s)]
    (is (= (d/schema conn2) (d/update-schema conn1 s)))
    (d/update-schema conn1 s1)
    (is (= (d/schema conn1) (-> (merge c/implicit-schema s s1)
                                (assoc-in [:a/b :db/aid] aid0)
                                (assoc-in [:c/d :db/aid] (inc aid0)))))
    (d/transact! conn1 txs)
    (is (= 2 (count (d/datoms @conn1 :eav))))

    (is (thrown-with-msg? Exception #"Cannot delete attribute"
                          (d/update-schema conn1 {} #{:c/d})))

    (d/transact! conn1 [[:db/retractEntity 1]])
    (is (= (d/schema conn2)
           (d/update-schema conn1 {} #{:c/d})
           (d/schema conn1)))

    (d/update-schema conn1 nil nil {:a/b :e/f})
    (is (= (d/schema conn1) (assoc c/implicit-schema :e/f
                                   {:db/valueType :db.type/string
                                    :db/aid       aid0})))

    (d/close conn1)
    (d/close conn2)
    (u/delete-files dir1)
    (u/delete-files dir2)))

(deftest test-update-schema-1
  (let [dir  (u/tmp-dir (str "test-" (UUID/randomUUID)))
        conn (d/create-conn dir)
        aid0 (count c/implicit-schema)]
    (d/update-schema conn {:things {}})
    (is (= (d/schema conn) (-> c/implicit-schema
                               (assoc-in [:things :db/aid] aid0))))
    (d/update-schema conn {:stuff {}})
    (is (= (d/schema conn) (-> c/implicit-schema
                               (assoc-in [:things :db/aid] aid0)
                               (assoc-in [:stuff :db/aid] (inc aid0)))))
    (d/update-schema conn {} [:things])
    (is (= (d/schema conn) (-> c/implicit-schema
                               (assoc-in [:stuff :db/aid] (inc aid0)))))
    (d/update-schema conn {:things {}})
    (is (= (d/schema conn) (-> c/implicit-schema
                               (assoc-in [:stuff :db/aid] (inc aid0))
                               (assoc-in [:things :db/aid] (+ aid0 2)))))
    (d/close conn)
    (u/delete-files dir)))

(deftest test-update-schema-ensure-no-duplicate-aids
  (let [dir  (u/tmp-dir (str "test-" (UUID/randomUUID)))
        conn (d/create-conn dir)]
    (d/update-schema conn {:up/a {}})
    (d/transact! conn [{:foo 1}])
    (let [aids (map :db/aid (vals (d/schema conn)))]
      (is (= (count aids) (count (set aids))))
      (d/close conn)
      (u/delete-files dir))))

(deftest test-update-schema-validates-new-attrs
  (let [dir  (u/tmp-dir (str "test-" (UUID/randomUUID)))
        conn (d/create-conn dir)]
    ;; :db/isComponent true requires :db/valueType :db.type/ref
    (is (thrown-with-msg?
          Exception #"isComponent.*should also have.*ref"
          (d/update-schema conn {:bad/attr {:db/isComponent true
                                            :db/valueType   :db.type/string}})))
    ;; invalid :db/valueType
    (is (thrown-with-msg?
          Exception #"Bad attribute specification"
          (d/update-schema conn {:bad/attr {:db/valueType :db.type/bogus}})))
    ;; invalid :db/cardinality
    (is (thrown-with-msg?
          Exception #"Bad attribute specification"
          (d/update-schema conn {:bad/attr {:db/cardinality :db.cardinality/bogus}})))
    ;; valid schema still works
    (d/update-schema conn {:good/attr {:db/valueType :db.type/string}})
    (is (:good/attr (d/schema conn)))
    (d/close conn)
    (u/delete-files dir)))

(deftest test-ways-to-create-conn-1
  (let [dir  (u/tmp-dir (str "test-" (UUID/randomUUID)))
        conn (d/create-conn dir)]
    (is (= #{} (set (d/datoms @conn :eav))))
    (is (= c/implicit-schema (db/-schema @conn)))
    (d/close conn)
    (u/delete-files dir)))

(deftest test-ways-to-create-conn-2
  (let [schema { :aka { :db/cardinality :db.cardinality/many
                        :db/aid         (count c/implicit-schema)}}
        dir    (u/tmp-dir (str "test-" (UUID/randomUUID)))
        conn   (d/create-conn dir schema)]
    (is (= #{} (set (d/datoms @conn :eav))))
    (is (= (db/-schema @conn) (merge schema c/implicit-schema)))
    (d/close conn)
    (u/delete-files dir)))

(deftest test-ways-to-create-conn-3
  (let [datoms #{(d/datom 1 :age  17)
                 (d/datom 1 :name "Ivan")}
        dir    (u/tmp-dir (str "test-" (UUID/randomUUID)))
        conn   (d/conn-from-datoms datoms dir)]
    (is (= datoms (set (d/datoms @conn :eav))))
    (is (= (d/schema conn) (db/-schema @conn)))
    (d/close conn)
    (u/delete-files dir))

  (let [schema { :aka { :db/cardinality :db.cardinality/many
                        :db/aid         (count c/implicit-schema)}}
        datoms #{(d/datom 1 :age  17)
                 (d/datom 1 :name "Ivan")}
        dir    (u/tmp-dir (str "test-" (UUID/randomUUID)))
        conn   (d/conn-from-datoms datoms dir schema)]
    (is (= datoms (set (d/datoms @conn :eav))))
    (is (= (d/schema conn) (db/-schema @conn)))
    (d/close conn)
    (u/delete-files dir))

  (let [datoms #{(d/datom 1 :age  17)
                 (d/datom 1 :name "Ivan")}
        dir    (u/tmp-dir (str "test-" (UUID/randomUUID)))
        conn   (d/conn-from-db (d/init-db datoms dir))]
    (is (thrown-with-msg? Exception
                          #"init-db expects list of Datoms, got "
                          (d/init-db [[:add -1 :name "Ivan"]
                                      {:add -1 :age 35}])))
    (is (= datoms (set (d/datoms @conn :eav))))
    (is (= (d/schema conn) (db/-schema @conn)))
    (d/close conn)
    (u/delete-files dir))

  (let [schema { :aka { :db/cardinality :db.cardinality/many
                        :db/aid         (count c/implicit-schema)}}
        datoms #{(d/datom 1 :age  17)
                 (d/datom 1 :name "Ivan")
                 (d/datom 1 :aka "danger")
                 (d/datom 1 :aka "fun")}
        dir    (u/tmp-dir (str "test-" (UUID/randomUUID)))
        conn   (d/conn-from-db (-> (d/empty-db dir schema)
                                   (d/fill-db datoms)))]
    (is (= datoms (set (d/datoms @conn :eav))))
    (is (= (d/schema conn) (db/-schema @conn)))
    (d/close conn)
    (u/delete-files dir)))

(deftest test-recreate-conn
  (let [schema {:name          {:db/valueType :db.type/string}
                :dt/updated-at {:db/valueType :db.type/instant}}
        dir    (u/tmp-dir (str "recreate-conn-test-" (UUID/randomUUID)))
        conn   (d/create-conn dir schema)]
    (d/transact! conn [{:db/id         -1
                        :name          "Namebo"
                        :dt/updated-at (Date.)}])
    (d/close conn)

    (let [conn2 (d/create-conn dir schema)]
      (d/transact! conn2 [{:db/id         -2
                           :name          "Another name"
                           :dt/updated-at (Date.)}])
      (is (= 4 (count (d/datoms @conn2 :eav))))
      (d/close conn2))
    (u/delete-files dir)))

(deftest test-open-kv-enables-virtual-thread-safe-reader-slots
  (let [dir (u/tmp-dir (str "open-kv-flags-test-" (UUID/randomUUID)))]
    (try
      (let [db (d/open-kv dir)]
        (try
          (is (= c/default-env-flags (d/get-env-flags db)))
          (finally
            (d/close-kv db))))
      (finally
        (u/delete-files dir)))))

(deftest test-get-conn
  (let [schema {:name          {:db/valueType :db.type/string}
                :dt/updated-at {:db/valueType :db.type/instant}}
        dir    (u/tmp-dir (str "get-conn-test-" (UUID/randomUUID)))
        conn   (d/get-conn dir schema)]
    (d/transact! conn [{:db/id         -1
                        :name          "Namebo"
                        :dt/updated-at (Date.)}])
    (d/close conn)

    (let [conn2 (d/get-conn dir schema)]
      (d/transact! conn2 [{:db/id         -2
                           :name          "Another name"
                           :dt/updated-at (Date.)}])
      (is (= 4 (count (d/datoms @conn2 :eav))))
      (d/close conn2))
    (u/delete-files dir)))

(deftest test-with-conn
  (let [dir (u/tmp-dir (str "with-conn-test-" (UUID/randomUUID)))]
    (d/with-conn [conn dir]
      (d/transact! conn [{:db/id      -1
                          :name       "something"
                          :updated-at (Date.)}])
      (is (= 2 (count (d/datoms @conn :eav)))))
    (u/delete-files dir)))

(deftest test-relaxed-transact-uses-queued-path
  (let [conn  (d/create-conn nil
                             {:k {:db/valueType :db.type/long}}
                             {:wal? true
                              :wal-durability-profile :relaxed
                              :kv-opts {:inmemory? true}})
        paths (atom [])]
    (try
      (binding [dc/*txlog-sync-path-observer*
                (fn [path] (swap! paths conj path))]
        (dotimes [i 32]
          (d/transact! conn [{:db/id i :k i}])))
      (is (= 32 (count @paths)))
      (is (every? #{:queued-relaxed} @paths))
      (finally
        (dc/shutdown-transact-async-executor!)
        (d/close conn)))))

(deftest test-strict-transact-prefers-direct-path-when-idle
  (let [conn  (d/create-conn nil
                             {:k {:db/valueType :db.type/long}}
                             {:wal? true
                              :wal-durability-profile :strict
                              :kv-opts {:inmemory? true}})
        paths (atom [])]
    (try
      (binding [dc/*txlog-sync-path-observer*
                (fn [path] (swap! paths conj path))]
        (dotimes [i 16]
          (d/transact! conn [{:db/id i :k i}])))
      (is (= 16 (count @paths)))
      (is (every? #{:direct-wal-idle-strict} @paths))
      (finally
        (dc/shutdown-transact-async-executor!)
        (d/close conn)))))

(deftest test-extra-transact-prefers-direct-path-when-idle
  (let [conn  (d/create-conn nil
                             {:k {:db/valueType :db.type/long}}
                             {:wal? true
                              :wal-durability-profile :extra
                              :kv-opts {:inmemory? true}})
        paths (atom [])]
    (try
      (binding [dc/*txlog-sync-path-observer*
                (fn [path] (swap! paths conj path))]
        (dotimes [i 16]
          (d/transact! conn [{:db/id i :k i}])))
      (is (= 16 (count @paths)))
      (is (every? #{:direct-wal-idle-extra} @paths))
      (finally
        (dc/shutdown-transact-async-executor!)
        (d/close conn)))))

(deftest test-strict-transact-async-no-stall
  (let [n    256
        conn (d/create-conn nil
                            {:k {:db/valueType :db.type/long}}
                            {:wal? true
                             :wal-durability-profile :strict
                             :kv-opts {:inmemory? true}})]
    (try
      (let [futs    (doall
                      (for [i (range n)]
                        (d/transact-async conn [{:db/id i :k i}])))
            results (doall (map #(deref % 10000 ::timeout) futs))]
        (is (not-any? #{::timeout} results))
        (is (= n
               (d/q '[:find (count ?e) .
                      :where [?e :k]]
                    (d/db conn)))))
      (finally
        (dc/shutdown-transact-async-executor!)
        (d/close conn)))))

(deftest test-transact-async-callback-exception-does-not-stall-result
  (let [conn (d/create-conn nil
                            {:k {:db/valueType :db.type/long}}
                            {:wal? true
                             :wal-durability-profile :strict
                             :kv-opts {:inmemory? true}})
        fut  (d/transact-async conn
                               [{:db/id 1 :k 1}]
                               nil
                               (fn [_]
                                 (throw (ex-info "callback failed" {}))))]
    (try
      (is (not= ::timeout (deref fut 2000 ::timeout)))
      (is (= 1
             (d/q '[:find (count ?e) .
                    :where [?e :k]]
                  (d/db conn))))
      (finally
        (dc/shutdown-transact-async-executor!)
        (d/close conn)))))

(defn- wait-until
  [pred timeout-ms]
  (let [^long timeout-ms timeout-ms]
    (loop [elapsed 0]
      (cond
        (pred) true
        (>= ^long elapsed timeout-ms) false
        :else
        (do
          (Thread/sleep 25)
          (recur (+ elapsed 25)))))))

(defn- current-java-bin
  []
  (str (System/getProperty "java.home")
       u/+separator+
       "bin"
       u/+separator+
       "java"))

(defn- last-nonblank-line
  [s]
  (some->> (str/split-lines (or s ""))
           reverse
           (remove str/blank?)
           first))

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
      (let [exit (.exitValue process)
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

(defn- wal-child-write-form
  [dir opts txs]
  (let [db-sym (gensym "db")
        form
        `(do
           (require '[datalevin.core :as d]
                    '[datalevin.kv :as kv])
           (let [~db-sym (d/open-kv ~dir ~opts)]
             (try
               (d/open-dbi ~db-sym "a")
               ~@(map (fn [tx]
                        `(d/transact-kv ~db-sym [~tx]))
                      txs)
               (println (pr-str {:status :ok
                                 :lsns (mapv :lsn (kv/open-tx-log ~db-sym 1))
                                 :applied-lsn
                                 (get-in (kv/read-commit-marker ~db-sym)
                                         [:current :applied-lsn])}))
               (finally
                 (d/close-kv ~db-sym)))))]
    (pr-str form)))

(defn- wal-child-failing-write-form
  [dir opts tx]
  (let [form
        `(do
           (require '[datalevin.binding.cpp :as cpp]
                    '[datalevin.core :as d])
           (let [db# (d/open-kv ~dir ~opts)]
             (try
               (d/open-dbi db# "a")
               (println
                (pr-str
                 (try
                   (binding [cpp/*before-write-commit-fn*
                             (fn [ctx#]
                               (when (= (:operation ctx#) :close-transact-kv)
                                 (throw (ex-info "forced commit failure"
                                                 {:type :forced-commit-failure}))))]
                     (d/transact-kv db# [~tx])
                     {:status :unexpected-success})
                   (catch Exception e#
                     {:status :expected-failure
                      :message (.getMessage e#)}))))
               (finally
                 (d/close-kv db#)))))]
    (pr-str form)))

(deftest test-strict-with-transaction-transact-uses-direct-path
  (let [conn  (d/create-conn nil
                             {:k {:db/valueType :db.type/long}}
                             {:wal? true
                              :wal-durability-profile :strict
                              :kv-opts {:inmemory? true}})
        paths (atom [])]
    (try
      (binding [dc/*txlog-sync-path-observer*
                (fn [path] (swap! paths conj path))]
        (d/with-transaction [cn conn]
          (dotimes [i 8]
            (d/transact! cn [{:db/id (- (inc i)) :k i}]))))
      (is (= 8 (count @paths)))
      (is (every? #{:direct-no-wal} @paths))
      (is (= 8
             (d/q '[:find (count ?e) .
                    :where [?e :k]]
                  (d/db conn))))
      (finally
        (d/close conn)))))

(deftest test-wal-allows-multiple-writable-processes
  (let [dir          (u/tmp-dir (str "wal-multi-process-test-" (UUID/randomUUID)))
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
                       (wal-child-overlap-form dir opts ready-path release-path))]
            (try
              (is (wait-until #(u/file-exists ready-path)
                              wal-child-ready-timeout-ms))
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
                (is (= 2
                       (get-in (kv/read-commit-marker db1) [:current :applied-lsn])))
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
      (let [db3 (d/open-kv dir opts)]
        (try
          (d/open-dbi db3 "a")
          (is (not (d/closed-kv? db3)))
          (is (= :v1
                 (d/get-value db3 "a" :k1)))
          (is (= :v2
                 (d/get-value db3 "a" :k2)))
          (finally
            (d/close-kv db3))))
      (finally
        (u/delete-files dir)))))

(deftest test-wal-parent-process-appends-after-child-process-advances-wal
  (let [dir  (u/tmp-dir (str "wal-parent-after-child-test-" (UUID/randomUUID)))
        opts {:wal? true
              :wal-commit-marker? true
              :snapshot-bootstrap-force? false
              :wal-durability-profile :strict}]
    (try
      (let [db (d/open-kv dir opts)]
        (try
          (d/open-dbi db "a")
          (is (= :transacted
                 (d/transact-kv db [[:put "a" :k1 :v1]])))
          (let [{:keys [ok? result output]}
                (child-process-result
                 (start-clojure-child!
                  (wal-child-write-form dir
                                        opts
                                        [[:put "a" :k2 :v2]
                                         [:put "a" :k3 :v3]]))
                 wal-child-process-timeout-ms)]
            (is ok? output)
            (is (= {:status :ok
                    :lsns [1 2 3]
                    :applied-lsn 3}
                   result)))
          (is (= :transacted
                 (d/transact-kv db [[:put "a" :k4 :v4]])))
          (is (= [1 2 3 4]
                 (mapv :lsn (kv/open-tx-log db 1))))
          (is (= 4
                 (get-in (kv/read-commit-marker db) [:current :applied-lsn])))
          (is (= 4
                 (:last-applied-lsn (kv/txlog-watermarks db))))
          (is (= :v1
                 (d/get-value db "a" :k1)))
          (is (= :v2
                 (d/get-value db "a" :k2)))
          (is (= :v3
                 (d/get-value db "a" :k3)))
          (is (= :v4
                 (d/get-value db "a" :k4)))
          (is (:ok? (kv/verify-commit-marker! db)))
          (finally
            (d/close-kv db))))
      (let [db (d/open-kv dir opts)]
        (try
          (d/open-dbi db "a")
          (is (= [1 2 3 4]
                 (mapv :lsn (kv/open-tx-log db 1))))
          (is (= :v4
                 (d/get-value db "a" :k4)))
          (finally
            (d/close-kv db))))
      (finally
        (u/delete-files dir)))))

(deftest test-wal-recovers-child-process-commit-failure-before-lmdb-commit
  (let [dir  (u/tmp-dir (str "wal-child-commit-failure-test-"
                             (UUID/randomUUID)))
        opts {:wal? true
              :wal-commit-marker? true
              :snapshot-bootstrap-force? false
              :wal-durability-profile :strict}]
    (try
      (let [db (d/open-kv dir opts)]
        (try
          (d/open-dbi db "a")
          (is (= :transacted
                 (d/transact-kv db [[:put "a" :k1 :v1]])))
          (finally
            (d/close-kv db))))
      (let [{:keys [ok? result output]}
            (child-process-result
             (start-clojure-child!
              (wal-child-failing-write-form dir
                                            opts
                                            [:put "a" :k2 :v2]))
             wal-child-process-timeout-ms)]
        (is ok? output)
        (is (= :expected-failure (:status result)))
        (is (re-find #"forced commit failure" (:message result))))
      (let [db (d/open-kv dir opts)]
        (try
          (d/open-dbi db "a")
          (is (= :v1
                 (d/get-value db "a" :k1)))
          (is (= :v2
                 (d/get-value db "a" :k2)))
          (is (= [1 2]
                 (mapv :lsn (kv/open-tx-log db 1))))
          (is (= 2
                 (get-in (kv/read-commit-marker db) [:current :applied-lsn])))
          (is (= 2
                 (:last-applied-lsn (kv/txlog-watermarks db))))
          (is (:ok? (kv/verify-commit-marker! db)))
          (is (= :transacted
                 (d/transact-kv db [[:put "a" :k3 :v3]])))
          (is (= [1 2 3]
                 (mapv :lsn (kv/open-tx-log db 1))))
          (is (= :v3
                 (d/get-value db "a" :k3)))
          (finally
            (d/close-kv db))))
      (finally
        (u/delete-files dir)))))

(deftest test-wal-recovers-after-commit-failure-before-lmdb-commit
  (let [dir  (u/tmp-dir (str "wal-cross-process-recovery-test-"
                             (UUID/randomUUID)))
        opts {:wal? true
              :wal-commit-marker? true
              :snapshot-bootstrap-force? false
              :wal-durability-profile :strict}]
    (try
      (let [db (d/open-kv dir opts)]
        (try
          (d/open-dbi db "a")
          (is (thrown-with-msg?
               Exception
               #"forced commit failure"
               (binding [cpp/*before-write-commit-fn*
                         (fn [ctx]
                           (when (= (:operation ctx) :close-transact-kv)
                             (throw (ex-info "forced commit failure"
                                             {:type ::forced-commit-failure}))))]
                 (d/transact-kv db [[:put "a" :k :v]]))))
          (finally
            (d/close-kv db))))
      (let [db (d/open-kv dir opts)]
        (try
          (d/open-dbi db "a")
          (is (= :v
                 (d/get-value db "a" :k)))
          (is (= [1]
                 (mapv :lsn (kv/open-tx-log db 1))))
          (is (= 1
                 (get-in (kv/read-commit-marker db) [:current :applied-lsn])))
          (is (= 1
                 (:last-applied-lsn (kv/txlog-watermarks db))))
          (is (:ok? (kv/verify-commit-marker! db)))
          (finally
            (d/close-kv db))))
      (finally
        (u/delete-files dir)))))

(deftest test-wal-open-failure-still-allows-reopen
  (let [dir                    (u/tmp-dir (str "wal-process-lock-failure-test-"
                                               (UUID/randomUUID)))
        bootstrap-disabled-opts {:wal? true
                                 :snapshot-bootstrap-force? false}
        bootstrap-opts          {:wal? true
                                 :snapshot-bootstrap-force? true}]
    (try
      (let [db (d/open-kv dir bootstrap-disabled-opts)]
        (try
          (d/open-dbi db "bootstrap")
          (d/transact-kv db [[:put "bootstrap" :k :v]])
          (finally
            (d/close-kv db))))
      (let [{:keys [db error]}
            (binding [kv/*wal-snapshot-copy-failpoint*
                      (fn [_]
                        (throw (ex-info "forced snapshot bootstrap failure"
                                        {:type ::snapshot-bootstrap-failed})))]
              (try
                {:db (d/open-kv dir bootstrap-opts)}
                (catch Exception e
                  {:error e})))]
        (when db
          (d/close-kv db))
        (is (instance? clojure.lang.ExceptionInfo error))
        (is (re-find #"forced snapshot bootstrap failure"
                     (.getMessage ^Exception error))))
      (let [db (d/open-kv dir bootstrap-opts)]
        (try
          (is (not (d/closed-kv? db)))
          (finally
            (d/close-kv db))))
      (finally
        (u/delete-files dir)))))

(deftest test-wal-one-shot-write-uses-explicit-lmdb-write-transaction
  (let [dir  (u/tmp-dir (str "wal-one-shot-write-test-" (UUID/randomUUID)))
        ops* (atom [])]
    (try
      (let [db (d/open-kv dir {:wal? true})]
        (try
          (d/open-dbi db "a")
          (binding [cpp/*before-write-commit-fn*
                    (fn [{:keys [operation]}]
                      (swap! ops* conj operation))]
            (is (= :transacted
                   (d/transact-kv db [[:put "a" :k :v]]))))
          (is (= [:close-transact-kv] @ops*))
          (is (= :v
                 (d/get-value db "a" :k)))
          (finally
            (d/close-kv db))))
      (finally
        (u/delete-files dir)))))
