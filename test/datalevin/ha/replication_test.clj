(ns datalevin.ha.replication-test
  (:require
   [clojure.test :refer [deftest is]]
   [datalevin.ha.replication :as repl])
  (:import
   [java.util.concurrent Executors]))

(deftest ha-parallel-mapv-clamps-overflowing-probe-deadlines
  (let [executor (Executors/newFixedThreadPool 2)
        timeout-ms (inc (quot Long/MAX_VALUE 1000000))]
    (try
      (with-redefs [repl/ha-now-nanos (constantly 100)
                    repl/ha-probe-round-timeout-ms (constantly timeout-ms)]
        (is (= [2 3]
               (#'datalevin.ha.replication/ha-parallel-mapv
                {:ha-probe-executor executor}
                (fn [x]
                  (Thread/sleep 20)
                  (inc x))
                [1 2]
                (constantly ::timeout)))))
      (finally
        (.shutdownNow executor)))))
