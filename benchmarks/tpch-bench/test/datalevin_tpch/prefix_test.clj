(ns datalevin-tpch.prefix-test
  (:require [clojure.test :refer [deftest is]]
            [datalevin.core :as d]
            [datalevin-tpch.queries :as q]
            [datalevin-tpch.queries-test :as qt]
            [datalevin-tpch.verify :as v]))

(deftest q22-short-phone-numbers-match-sql
  (let [customers [[1 "" 1.0e6] [2 "1" 1.0e6] [3 "99-0" 1.0e6]
                   [4 "13-0" 100.0] [5 "13-1" 200.0] [6 "17-0" 300.0]
                   [7 "13-2" 400.0] [8 "13-3" 400.0] [9 "17-1" -100.0]
                   [10 "31-0" 0.0] [11 "18-0" 270.0]]
        expected
        (#'qt/check-fixture 22
          [["customer" ["c_custkey INTEGER" "c_phone TEXT" "c_acctbal REAL"]
            [:customer/custkey :customer/phone :customer/acctbal] customers]
           ["orders" ["o_custkey INTEGER"] [:orders/custkey] [[8] [8]]]])]
    (is (= [["13" 1 400.0] ["17" 1 300.0]] expected))
    (with-open [conn (d/create-conn nil
                      {:customer/custkey {:db/valueType :db.type/long}
                       :customer/phone {:db/valueType :db.type/string}
                       :customer/acctbal {:db/valueType :db.type/double}
                       :orders/customer {:db/valueType :db.type/ref}})]
      (d/transact! conn
        (concat (map (fn [[k phone balance]]
                       {:db/id k :customer/custkey k :customer/phone phone
                        :customer/acctbal balance}) customers)
                [{:orders/customer 8} {:orders/customer 8}]))
      (is (v/results-match? 22 expected (d/q q/q-22 @conn))))))
