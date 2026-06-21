(ns datalevin.test.server
  (:require
   [clojure.test :refer [deftest is]]
   [datalevin.constants :as c]
   [datalevin.server :as srv]
   [datalevin.test.core :as tc]
   [datalevin.util :as u])
  (:import
   [datalevin.server Server]
   [java.util UUID]
   [java.util.concurrent ThreadPoolExecutor]))

(deftest server-worker-executor-is-bounded-test
  (let [port   (tc/allocate-port)
        dir    (u/tmp-dir (str "server-worker-executor-test-"
                               (UUID/randomUUID)))
        ^Server server (binding [c/*db-background-sampling?* false]
                         (srv/create {:port              port
                                      :root              dir
                                      :worker-threads    3
                                      :worker-queue-size 5}))]
    (try
      (let [^ThreadPoolExecutor executor (.-work-executor server)]
        (is (instance? ThreadPoolExecutor executor))
        (is (= 3 (.getCorePoolSize executor)))
        (is (= 3 (.getMaximumPoolSize executor)))
        (is (= 5 (.remainingCapacity (.getQueue executor)))))
      (finally
        (srv/stop server)
        (u/delete-files dir)))))
