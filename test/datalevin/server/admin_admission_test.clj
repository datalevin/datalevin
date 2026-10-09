(ns datalevin.server.admin-admission-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [datalevin.bits :as b]
            [datalevin.client :as client]
            [datalevin.core :as d]
            [datalevin.interpret :refer [inter-fn]]
            [datalevin.server :as server]
            [datalevin.test.core :refer [allocate-port db-fixture]]
            [datalevin.util :as u])
  (:import [datalevin.remote KVStore DatalogStore]
           [datalevin.db DB]
           [java.util.concurrent Semaphore]))

(use-fixtures :once db-fixture)

(defn- busy-error [f]
  (try (f) nil
       (catch clojure.lang.ExceptionInfo e
         (let [data (ex-data e)]
           (:error (or (:err-data data) data))))))

(deftest schema-and-administration-share-the-database-writer-slot
  (let [root (u/tmp-dir (str "admin-admission-" (random-uuid)))
        port (allocate-port)
        srv (server/create {:root root :port port :transaction-lock-timeout-ms 10})
        base (str "dtlv://datalevin:datalevin@localhost:" port "/")]
    (try
      (server/start srv)
      (let [kv (d/open-kv (str base "kv") {:wal? true :snapshot-scheduler? false})
            conn (d/create-conn (str base "dl") {:value {}} {:background-sampling? false})
            kv-client (.-client ^KVStore kv)
            dl-client (.-client ^DatalogStore (.-store ^DB @conn))]
        (try
          (d/open-dbi kv "data")
          (d/transact-kv kv "data" [[:put 1 :before]])
          (doseq [[db-name remote-client commands]
                  [["kv" kv-client
                    [[:open-dbi ["new"]]
                     [:clear-dbi ["data"]]
                     [:drop-dbi ["data"]]
                     [:set-env-flags [[:nosync] false]]
                     [:sync [true]]
                     [:force-txlog-sync! []]
                     [:force-lmdb-sync! []]
                     [:create-snapshot! []]
                     [:gc-txlog-segments! []]
                     [:txlog-update-snapshot-floor! [0]]
                     [:txlog-clear-snapshot-floor! []]
                     [:txlog-pin-backup-floor! ["test" 0]]
                     [:txlog-unpin-backup-floor! ["test"]]
                     [:kv-re-index [{}]]
                     [:search-re-index [{}]]
                     [:vec-re-index [{}]]]]
                   ["dl" dl-client
                    [[:set-schema [{:new {}}]]
                     [:index-attr [:value]]
                     [:swap-attr [:value (b/serialize (inter-fn [props] props))]]
                     [:del-attr [:value]]
                     [:rename-attr [:value :renamed]]
                     [:analyze [:value]]
                     [:assoc-opt [:validate-data? true]]
                     [:assoc-opts [{:validate-data? true}]]
                     [:datalog-re-index [nil {}]]
                     [:ha-update-membership! [{:ha-members ["localhost:1234"]}]]]]]]
            (let [^Semaphore slot (#'server/get-lock srv db-name)]
              (.acquire slot)
              (try
                (doseq [[operation args] commands]
                  (testing (name operation)
                    (is (= :server/busy
                           (busy-error
                             #(client/normal-request remote-client operation
                                                     (into [db-name] args)))))))
                (finally (.release slot)))
              (is (= 1 (.availablePermits slot)))))
          (is (= :before (d/get-value kv "data" 1)))
          (is (= #{"data"} (set (d/list-dbis kv))))
          (is (contains? (d/schema conn) :value))
          (is (not (contains? (d/schema conn) :new)))
          (testing "released slots allow administration again"
            (d/open-dbi kv "new")
            (d/clear-dbi kv "data")
            (d/drop-dbi kv "new")
            (d/update-schema conn {:new {}})
            (is (nil? (d/get-value kv "data" 1)))
            (is (contains? (d/schema conn) :new)))
          (testing "transaction-bound schema changes reuse their existing slot"
            (d/with-transaction [tx conn]
              (d/update-schema tx {:transaction-only {}})
              (is (contains? (d/schema tx) :transaction-only)))
            (is (contains? (d/schema conn) :transaction-only)))
          (finally (d/close-kv kv) (d/close conn))))
      (finally (server/stop srv) (u/delete-files root)))))
