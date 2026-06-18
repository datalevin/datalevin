(ns datalevin.test.ha-replication
  (:require
   [clojure.test :refer [deftest is]]
   [datalevin.ha.replication :as repl]))

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
