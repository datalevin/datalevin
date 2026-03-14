(ns datalevin.jepsen.remote-node-test
  (:require
   [clojure.java.io :as io]
   [clojure.test :refer [deftest is]]
   [datalevin.jepsen.remote-node :as remote-node]))

(defn- temp-dir-path
  []
  (.toString
   (java.nio.file.Files/createTempDirectory
    "dtlv-remote-node-test-"
    (make-array java.nio.file.attribute.FileAttribute 0))))

(defn- delete-tree!
  [path]
  (when path
    (let [root (io/file path)]
      (when (.exists root)
        (doseq [file (reverse (file-seq root))]
          (io/delete-file file true))))))

(defn- remote-config
  [root]
  {:nodes [{:logical-node "n1"
            :node-id 1
            :endpoint "127.0.0.1:8891"
            :peer-id "127.0.0.1:9891"
            :root root}]
   :db-name "remote-node-test"
   :workload :append
   :group-id "remote-node-test-group"
   :db-identity "remote-node-test-db"})

(deftest wait-for-open-prereqs-skips-data-endpoints-test
  (let [node     {:logical-node "n1"}
        topology {:data-nodes [{:logical-node "n1"
                                :endpoint "127.0.0.1:1"}
                               {:logical-node "n2"
                                :endpoint "127.0.0.1:2"}]
                  :control-only-nodes []}]
    (is (= node
           (#'remote-node/wait-for-open-prereqs! node topology 1)))))

(deftest stop-node-preserves-failed-state-when-pid-missing-test
  (let [root      (temp-dir-path)
        config    (remote-config root)
        node      {:logical-node "n1"
                   :root root}
        state-path (#'remote-node/state-file node)]
    (try
      (io/make-parents state-path)
      (spit state-path (pr-str {:status :failed
                                :node "n1"
                                :error-message "boom"}))
      (with-out-str
        (#'remote-node/stop-node! config "n1"))
      (is (.exists (io/file state-path)))
      (finally
        (delete-tree! root)))))

(deftest stop-node-deletes-nonfailed-state-when-pid-missing-test
  (let [root      (temp-dir-path)
        config    (remote-config root)
        node      {:logical-node "n1"
                   :root root}
        state-path (#'remote-node/state-file node)]
    (try
      (io/make-parents state-path)
      (spit state-path (pr-str {:status :running
                                :node "n1"}))
      (with-out-str
        (#'remote-node/stop-node! config "n1"))
      (is (not (.exists (io/file state-path))))
      (finally
        (delete-tree! root)))))
