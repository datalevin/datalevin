(ns datalevin.admin-test
  (:require [clojure.test :refer [deftest is]]
            [datalevin.command :as command]
            [datalevin.main :as main]))

(deftest admin-command-line-smoke
  (let [parsed (main/validate-args ["admin" "status" "--database" "Orders_DB"
                                   "--database" "users" "--database" "orders-db"
                                   "--timeout-ms" "1250" "--status-concurrency" "4"])]
    (is (= "admin" (:command parsed)))
    (is (= ["orders-db" "users"] (get-in parsed [:options :database])))
    (is (= 1250 (get-in parsed [:options :timeout-ms])))
    (is (= 4 (get-in parsed [:options :status-concurrency]))))
  (is (= 2 (:exit-code (main/validate-args ["admin" "status"]))))
  (is (:ok? (main/validate-args ["admin" "--help"])))
  (is (command/read-only? :admin-status))
  (is (not (command/ha-write? :admin-status))))
