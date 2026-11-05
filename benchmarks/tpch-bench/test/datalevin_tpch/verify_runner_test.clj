(ns datalevin-tpch.verify-runner-test
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.test :refer [deftest is]]
            [datalevin.core :as d]
            [datalevin.util :as u]
            [datalevin-tpch.common :as c]
            [datalevin-tpch.datalevin :as dt]
            [datalevin-tpch.sqlite :as sq]
            [datalevin-tpch.verify :as v])
  (:import [java.sql DriverManager]
           [java.util UUID]))

(defn- fixture-value [attribute type]
  (case attribute
    :lineitem/shipdate "1994-05-01"
    :lineitem/quantity 10.0
    :lineitem/discount 0.06
    :lineitem/extendedprice 100.0
    (case type :long 1 :double 1.0 :string "fixture")))

(defn- load-fixture! [dir sqlite]
  (let [conn (d/get-conn dir dt/schema)]
    (try
      (with-open [sql (DriverManager/getConnection (str "jdbc:sqlite:" sqlite))]
        (#'sq/exec-sql-file! sql (io/file (c/data-dir) "schema-sqlite.sql"))
        (doseq [[entity table] (map-indexed vector c/table-order)
                :let [attributes (c/attrs table)
                      types (:types (c/table-specs table))
                      values (mapv fixture-value attributes types)]]
          (d/transact! conn
            [(into {:db/id (inc entity)}
                   (map (fn [a value]
                          (if-let [[ref target] (dt/references a)]
                            [ref (inc (.indexOf ^java.util.List c/table-order target))]
                            [a value])) attributes values))])
          (with-open [statement (.prepareStatement sql
                                  (str "INSERT INTO " table " VALUES ("
                                       (str/join "," (repeat (count values) "?")) ")"))]
            (doseq [[i value] (map-indexed vector values)] (.setObject statement (inc i) value))
            (.executeUpdate statement))))
      (finally (d/close conn)))))

(deftest verification-reports-real-results-and-rejects-incompatible-stores
  (let [root (str "tmp/verify-" (UUID/randomUUID))
        dir (str root "/db") sqlite (str root "/sqlite.db") out (str root "/report.edn")]
    (.mkdirs (io/file root))
    (try
      (load-fixture! dir sqlite)
      (let [report (v/run-verification {:dir dir :sqlite sqlite :queries [6] :out out :timeout-ms 10000})]
        (is (:ok? report))
        (is (= :complete (:status report)))
        (is (= 1 (:passed report)))
        (is (= {"region" 1 "nation" 1 "part" 1 "supplier" 1 "partsupp" 1
                "customer" 1 "orders" 1 "lineitem" 1} (:table-counts report)))
        (is (= {:query 6 :ok? true :reference-rows 1 :expected-rows 1 :actual-rows 1}
               (select-keys (first (:results report)) [:query :ok? :reference-rows :expected-rows :actual-rows])))
        (is (= report (edn/read-string (slurp out)))))
      (with-open [conn (d/create-conn dir)]
        ;; All row counts still match, but the supplier points at an order.
        (d/transact! conn [[:db/add 4 :supplier/nation 7]]))
      (is (thrown-with-msg? clojure.lang.ExceptionInfo #"reference integrity"
                           (v/run-verification {:dir dir :sqlite sqlite :queries [6] :out out})))
      (with-open [conn (d/create-conn dir)]
        (d/transact! conn [[:db/add 4 :supplier/nation 2]]))
      (with-open [conn (DriverManager/getConnection (str "jdbc:sqlite:" sqlite))
                  stmt (.createStatement conn)]
        (.executeUpdate stmt "DELETE FROM supplier"))
      (is (thrown-with-msg? clojure.lang.ExceptionInfo #"table row counts do not match"
                           (v/run-verification {:dir dir :sqlite sqlite :queries [6] :out out})))
      (is (= :preflight (:status (edn/read-string (slurp out))))
          "a failed preflight cannot leave a previous successful report in place")
      (is (thrown-with-msg? clojure.lang.ExceptionInfo #"Missing datalevin database"
                           (v/run-verification {:dir (str root "/missing") :sqlite sqlite :queries [6]})))
      (is (not (.exists (io/file root "missing"))) "verification cannot silently create an empty store")
      (finally (u/delete-files root)))))
