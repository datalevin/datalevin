(ns datalevin.test.conn
  (:require
   [clojure.string :as str]
   [datalevin.test.core :as tdc :refer [db-fixture]]
   [clojure.test :refer [deftest testing is use-fixtures]]
   [datalevin.binding.cpp :as cpp]
   [datalevin.conn :as dc]
   [datalevin.core :as d]
   [datalevin.db :as db]
   [datalevin.interface :as i]
   [datalevin.constants :as c]
   [datalevin.kv :as kv]
   [datalevin.txlog :as txlog]
   [datalevin.util :as u])
  (:import [java.util Date UUID]))

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

(deftest test-open-kv-rejects-second-local-handle-in-process
  (let [dir (u/tmp-dir (str "open-kv-duplicate-handle-test-"
                            (UUID/randomUUID)))]
    (try
      (let [db1 (d/open-kv dir)]
        (try
          (d/open-dbi db1 "a")
          (d/transact-kv db1 [[:put "a" :k :v]])
          (let [result (try
                         (let [db2 (d/open-kv dir)]
                           (try
                             {:status :opened}
                             (finally
                               (d/close-kv db2))))
                         (catch Exception e
                           {:status  :error
                            :message (ex-message e)}))]
            (is (= :error (:status result)) result)
            (is (re-find #"Please do not open multiple LMDB connections"
                         (:message result)))
            (is (= :v
                   (d/get-value db1 "a" :k))))
          (finally
            (d/close-kv db1))))
      (let [db2 (d/open-kv dir)]
        (try
          (d/open-dbi db2 "a")
          (is (= :v
                 (d/get-value db2 "a" :k)))
          (finally
            (d/close-kv db2))))
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

(deftest test-wal-rejects-commit-before-txlog-append
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
          (is (nil? (d/get-value db "a" :k)))
          (is (= []
                 (mapv :lsn (kv/open-tx-log db 1))))
          (is (nil? (get-in (kv/read-commit-marker db) [:current :applied-lsn])))
          (is (= 0
                 (:last-applied-lsn (kv/txlog-watermarks db))))
          (is (:ok? (kv/verify-commit-marker! db)))
          (finally
            (d/close-kv db))))
      (finally
        (u/delete-files dir)))))

(deftest test-wal-explicit-write-transaction-rejects-commit-before-txlog-append
  (let [dir  (u/tmp-dir (str "wal-explicit-write-transaction-test-"
                             (UUID/randomUUID)))
        opts {:wal? true
              :wal-commit-marker? true
              :snapshot-bootstrap-force? false
              :wal-durability-profile :strict}]
    (try
      (let [db (d/open-kv dir opts)]
        (try
          (d/open-dbi db "a")
          (let [wdb (i/open-transact-kv db)]
            (is (= :transacted
                   (d/transact-kv wdb [[:put "a" :k :v]])))
            (is (thrown-with-msg?
                 Exception
                 #"forced commit failure"
                 (binding [cpp/*before-write-commit-fn*
                           (fn [ctx]
                             (when (= (:operation ctx) :close-transact-kv)
                               (throw (ex-info "forced commit failure"
                                               {:type ::forced-commit-failure}))))]
                   (i/close-transact-kv db)))))
          (is (nil? (d/get-value db "a" :k)))
          (is (= []
                 (mapv :lsn (kv/open-tx-log db 1))))
          (is (= :transacted
                 (d/transact-kv db [[:put "a" :k2 :v2]])))
          (is (= :v2
                 (d/get-value db "a" :k2)))
          (is (= [1]
                 (mapv :lsn (kv/open-tx-log db 1))))
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

(deftest test-wal-open-fallback-restores-snapshot-with-clean-txlog-runtime
  (let [dir      (u/tmp-dir (str "wal-snapshot-fallback-test-"
                                 (UUID/randomUUID)))
        txlog-dir (str dir u/+separator+ "txlog")
        opts     {:wal? true
                  :snapshot-bootstrap-force? false}]
    (try
      (let [db (d/open-kv dir opts)]
        (try
          (d/open-dbi db "a")
          (is (= :transacted
                 (d/transact-kv db [[:put "a" :k1 :v1]])))
          (d/create-snapshot! db)
          (is (seq (d/list-snapshots db)))
          (is (= :transacted
                 (d/transact-kv db [[:put "a" :k2 :v2]])))
          (finally
            (d/close-kv db))))
      (let [segment-path (->> (or (u/list-files txlog-dir) [])
                              (map #(.getPath ^java.io.File %))
                              (filter #(str/ends-with? % ".wal"))
                              sort
                              first)]
        (is (string? segment-path))
        (with-open [raf (java.io.RandomAccessFile. ^String segment-path "rw")]
          (.seek raf 0)
          (.write raf (byte-array [0 0 0 0]))))
      (let [db (d/open-kv dir opts)]
        (try
          (d/open-dbi db "a")
          (is (= :v1
                 (d/get-value db "a" :k1)))
          (is (nil? (d/get-value db "a" :k2)))
          (is (= :transacted
                 (d/transact-kv db [[:put "a" :k3 :v3]])))
          (is (= :v3
                 (d/get-value db "a" :k3)))
          (finally
            (d/close-kv db))))
      (finally
        (u/delete-files dir)))))

(deftest test-wal-refresh-shared-state-clamps-stale-meta-segment-offset
  (let [dir       (u/tmp-dir (str "wal-stale-meta-offset-test-"
                                  (UUID/randomUUID)))
        txlog-dir (str dir u/+separator+ "txlog")
        opts      {:wal? true}
        segment-path
        (fn []
          (->> (or (u/list-files txlog-dir) [])
               (map #(.getPath ^java.io.File %))
               (filter #(str/ends-with? % ".wal"))
               sort
               last))]
    (try
      (let [db (d/open-kv dir opts)]
        (try
          (d/open-dbi db "a")
          (is (= :transacted
                 (d/transact-kv db [[:put "a" :k1 :v1]])))
          (finally
            (d/close-kv db))))
      (let [path (segment-path)]
        (is (string? path))
        ;; Leave the WAL meta file intact but wipe the active segment bytes to
        ;; simulate recovery paths where metadata survives while the segment
        ;; tail is rebuilt from scratch.
        (with-open [raf (java.io.RandomAccessFile. ^String path "rw")]
          (.setLength raf 0)))
      (let [db (d/open-kv dir opts)]
        (try
          (d/open-dbi db "a")
          (is (= :v1
                 (d/get-value db "a" :k1)))
          (is (= :transacted
                 (d/transact-kv db [[:put "a" :k2 :v2]])))
          (finally
            (d/close-kv db))))
      (let [path (segment-path)]
        (with-open [raf (java.io.RandomAccessFile. ^String path "r")]
          (let [magic (byte-array 4)]
            (is (= 4 (.read raf magic)))
            (is (= [0x44 0x4c 0x57 0x4c]
                   (mapv (fn [b] (bit-and 0xff (int b))) magic))))))
      (let [db (d/open-kv dir opts)]
        (try
          (d/open-dbi db "a")
          (is (= :v2
                 (d/get-value db "a" :k2)))
          (finally
            (d/close-kv db))))
      (finally
        (u/delete-files dir)))))

(deftest test-wal-commit-meta-segment-offset-matches-segment-end
  (let [dir       (u/tmp-dir (str "wal-commit-meta-offset-test-"
                                  (UUID/randomUUID)))
        txlog-dir (str dir u/+separator+ "txlog")
        opts      {:wal? true}]
    (try
      (let [db (d/open-kv dir opts)]
        (try
          (d/open-dbi db "a")
          (is (= :transacted
                 (d/transact-kv db [[:put "a" :k1 :v1]])))
          (finally
            (d/close-kv db))))
      (let [{:keys [file]} (last (txlog/segment-files txlog-dir))
            meta-state (get-in (txlog/read-meta-file (txlog/meta-path txlog-dir))
                               [:current])
            scan (txlog/scan-segment (.getPath ^java.io.File file)
                                     {:allow-preallocated-tail? true})
            end-offset (txlog/segment-end-offset scan)]
        (is (map? meta-state))
        (is (= (long end-offset)
               (long (:segment-offset meta-state)))))
      (finally
        (u/delete-files dir)))))

(deftest test-wal-replay-aligns-runtime-cursor-to-persisted-payload-floor
  (let [dir  (u/tmp-dir (str "wal-replay-align-floor-test-"
                             (UUID/randomUUID)))
        opts {:wal? true}]
    (try
      (let [db (d/open-kv dir opts)]
        (try
          (d/open-dbi db "a")
          ;; Build a local WAL tail that ends at LSN 13, then simulate a
          ;; snapshot-installed follower whose persisted payload floor has
          ;; already advanced to LSN 14 before the runtime txlog cursor is
          ;; realigned.
          (dotimes [i 13]
            (is (= :transacted
                   (d/transact-kv db [[:put "a" i i]]))))
          (let [state (txlog/state db)]
            (is (some? state))
            (is (= 14 (long @(:next-lsn state))))
            (is (= :transacted
                   (i/transact-kv db
                                  c/kv-info
                                  [[:put c/wal-local-payload-lsn 14]]
                                  :keyword
                                  :data)))
            (let [res (kv/mirror-replayed-txlog-record!
                       db
                       {:lsn 15
                        :ha-term 7
                        :rows [[:put "a" :replayed :ok]]})]
              (is (= 15 (long (:lsn res))))
              (is (not (:skipped? res)))
              (is (= 16 (long @(:next-lsn state))))
              (is (= :ok
                     (d/get-value db "a" :replayed)))))
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
