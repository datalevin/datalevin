(ns datalevin.test.ha-replication
  (:require
   [clojure.test :refer [deftest is]]
   [datalevin.constants :as c]
   [datalevin.ha.client-cache :as cache]
   [datalevin.ha.replication :as repl]
   [datalevin.ha.replication.bootstrap :as boot]
   [datalevin.interface :as i]))

(deftest txlog-not-enabled-source-enters-gap-bootstrap-path-test
  (let [m {:ha-local-endpoint "n1"
           :ha-authority-version 1
           :ha-members [{:node-id 1 :endpoint "n1"}
                        {:node-id 2 :endpoint "n2"}]}
        lease {:leader-node-id 2
               :leader-endpoint "n2"
               :leader-last-applied-lsn 10
               :term 1}
        fetches (atom [])]
    (with-redefs [repl/fetch-leader-watermark-lsn
                  (fn [_db-name _m lease]
                    {:reachable? true
                     :last-applied-lsn (:leader-last-applied-lsn lease)
                     :txlog-last-applied-lsn (:leader-last-applied-lsn lease)})
                  repl/fetch-ha-leader-txlog-batch
                  (fn [_db-name _m endpoint _from-lsn _upto-lsn]
                    (swap! fetches conj endpoint)
                    (throw (ex-info "Txn-log is not enabled for this LMDB"
                                    {:type :txlog/not-enabled})))]
      (let [err (try
                  (#'repl/fetch-ha-follower-records-with-gap-fallback
                   "db" m lease 5 5)
                  nil
                  (catch clojure.lang.ExceptionInfo e
                    e))
            data (ex-data err)]
        (is (= ["n2"] @fetches))
        (is (= :ha/txlog-gap-unresolved (:error data)))
        (is (= :txlog/not-enabled
               (get-in data [:gap-errors 0 :data :type])))))))

(deftest ha-internal-kv-source-open-enables-wal-test
  (is (= {:wal? true}
         (#'cache/ha-kv-open-opts))))

(deftest ha-snapshot-copy-source-open-enables-wal-test
  (is (= {:wal? true
          :client-opts {:pool-size 1
                        :time-out 2500}}
         (#'repl/ha-snapshot-remote-open-opts 2500))))

(deftest snapshot-bootstrap-recognizes-wrapped-interrupt-test
  (is (true?
       (#'boot/interrupted-error?
        (ex-info
         (str "Fail to open database: #error {\n"
              " :via [{:type java.nio.channels.ClosedByInterruptException}]}")
         {})))))

(deftest snapshot-bootstrap-keeps-copied-payload-floor-above-stale-lease-test
  (let [reconcile-calls (atom [])
        resume-next-lsns (atom [])
        lease {:leader-node-id 1
               :leader-endpoint "leader"
               :leader-last-applied-lsn 11
               :term 1}
        manifest {:db-name "db"
                  :db-identity "dbid"
                  :snapshot-last-applied-lsn 14
                  :payload-last-applied-lsn 14
                  :txlog-last-applied-lsn 14
                  :install-last-applied-lsn 14}]
    (with-redefs [i/get-value (fn [_kv-store _dbi key & _opts]
                                (when (= c/wal-local-payload-lsn key)
                                  14))]
      (let [result
            (boot/bootstrap-ha-follower-from-snapshot*
             {:normalize-ha-bootstrap-retry-state
              (fn [m _fallback-m _reopen-info] m)
              :ha-local-store-reopen-info (constantly nil)
              :fetch-ha-endpoint-snapshot-copy!
              (fn [_db-name _m source-endpoint _snapshot-dir]
                (is (= "leader" source-endpoint))
                {:copy-meta manifest})
              :validate-ha-snapshot-copy!
              (fn [_db-name _m _source-endpoint _snapshot-dir copy-meta
                   required-lsn]
                (is (= 11 required-lsn))
                (is (= manifest copy-meta))
                manifest)
              :install-ha-local-snapshot!
              (fn [m _snapshot-dir]
                {:ok? true
                 :state (assoc m :installed? true)})
              :explicit-raw-local-kv-store (constantly ::kv-store)
              :read-ha-local-snapshot-current-lsn (constantly 14)
              :reconcile-ha-installed-snapshot-state
              (fn [state materialized-lsn install-target-lsn _apply-record-fn]
                (swap! reconcile-calls conj
                       {:materialized-lsn materialized-lsn
                        :install-target-lsn install-target-lsn})
                {:state state
                 :installed-lsn install-target-lsn})
              :persist-ha-local-applied-lsn!
              (fn [_state installed-lsn] installed-lsn)
              :note-ha-bootstrap-installed-state
              (fn [state installed-lsn source-endpoint snapshot-lsn now-ms
                   persisted-installed-lsn]
                (assoc state
                       :installed-lsn installed-lsn
                       :source-endpoint source-endpoint
                       :snapshot-lsn snapshot-lsn
                       :now-ms now-ms
                       :persisted-installed-lsn persisted-installed-lsn))
              :apply-ha-follower-record! (fn [state _record] state)
              :sync-ha-follower-batch
              (fn [_db-name state _lease next-lsn _now-ms]
                (swap! resume-next-lsns conj next-lsn)
                {:state (assoc state :resume-next-lsn next-lsn)})}
             "db"
             {:ha-db-identity "dbid"}
             lease
             ["leader"]
             15
             1000)]
        (is (true? (:ok? result)))
        (is (= [{:materialized-lsn 14
                 :install-target-lsn 14}]
               @reconcile-calls))
        (is (= [15] @resume-next-lsns))))))
