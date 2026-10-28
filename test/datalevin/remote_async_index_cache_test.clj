(ns datalevin.remote-async-index-cache-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [datalevin.async :as async]
            [datalevin.constants :as c]
            [datalevin.core :as d]
            [datalevin.interface :as i]
            [datalevin.server :as server]
            [datalevin.storage :as storage]
            [datalevin.test.core :refer [db-fixture server-port]]
            [datalevin.util :as u])
  (:import [datalevin.remote DatalogStore]))

(use-fixtures :each db-fixture)

(def ^:private query
  '[:find ?e :in $ ?query
    :where [(idoc-match $ :doc ?query) [[?e _ _]]]])

(def ^:private scalar-query
  '[:find ?e . :in $ ?query
    :where [(idoc-match $ :doc ?query) [[?e _ _]]]])

(deftest remote-idoc-queries-observe-completed-index-jobs
  (let [root (u/tmp-dir (str "remote-async-index-cache-" (random-uuid)))
        srv (server/create {:root root :port 0})]
    (try
      (server/start srv)
      (doseq [interval [0 60000]]
        (binding [c/*remote-db-last-modified-check-interval-ms* interval]
          (let [db-name (str "docs-" (random-uuid))
                uri (str "dtlv://datalevin:datalevin@localhost:"
                         (server-port srv) "/" db-name)]
            (with-open [^java.io.Closeable conn
                        (d/create-conn uri {:doc {:db/valueType :db.type/idoc}}
                                       {:background-sampling? false
                                        :idoc-opts {:indexing-mode :async}})]
              (let [local-store (#'server/get-store srv db-name false)
                    ^DatalogStore remote-store (:store @conn)
                    queries (mapv (fn [[query pending completed]]
                                    [query pending completed (d/prepare-q @conn query)])
                                  [[query #{} #{[1]}] [scalar-query nil 1]])
                    entered (promise)
                    release (promise)
                    executor (async/get-executor)
                    worker-key (async/work-key
                                 (storage/->SecondaryIndexWork local-store executor))
                    ;; Work sharing a key executes serially. Hold only this DB's
                    ;; index queue until both client query paths observe the lag.
                    blocker (async/exec executor
                                        (reify async/IAsyncWork
                                          (work-key [_] worker-key)
                                          (do-work [_] (deliver entered true) @release)
                                          (combine [_] nil)
                                          (callback [_] nil)))]
                (try
                  (is (true? (deref entered 5000 ::timeout)))
                  (d/transact! conn [{:db/id 1 :doc {:status "ready"}}])
                  (let [before (select-keys (i/db-info remote-store)
                                           [:last-modified :max-tx :schema-revision])
                        view @conn]
                    (testing "pending index results are not retained by the client"
                      (is (= 1 (:pending-count (storage/secondary-index-status local-store))))
                      (doseq [[query pending _ prepared] queries]
                        (dotimes [_ 2]
                          (is (= pending (d/q query view {:status "ready"})))
                          (is (= pending (prepared [{:status "ready"}]))))))
                    (deliver release true)
                    (is (true? (deref blocker 5000 ::timeout)))
                    (is (:caught-up? (storage/wait-for-secondary-index
                                      local-store {:tx (:max-tx before)
                                                   :timeout-ms 5000 :poll-ms 10})))
                    (is (= before (select-keys (i/db-info remote-store)
                                              [:last-modified :max-tx :schema-revision])))
                    (doseq [[query _ completed prepared] queries]
                      (is (= completed (i/q remote-store query
                                            [:remote-db-placeholder {:status "ready"}])))
                      (dotimes [_ 2]
                        (is (= completed (d/q query view {:status "ready"})))
                        (is (= completed (prepared [{:status "ready"}]))))))
                  (finally
                    (deliver release true)
                    (deref blocker 5000 nil))))))))
      (finally
        (server/stop srv)
        (u/delete-files root)))))
