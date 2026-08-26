(ns ldbc-snb-bench.neo4j-test
  (:require
   [clojure.string :as str]
   [clojure.test :refer [deftest is]]
   [ldbc-snb-bench.neo4j :as neo4j])
  (:import
   [java.time LocalDate]
   [java.util Date]))

(deftest complete-cypher-suite-test
  (let [queries (neo4j/load-cypher-queries "neo4j/queries.cypher")]
    (is (= (set (concat (map #(str "IC" %) (range 1 15))
                            (map #(str "IS" %) (range 1 8))))
           (set (keys queries))))
    (is (every? #(not (re-find #";\s*$" %)) (vals queries)))))

(deftest embedded-version-test
  (is (= (str/trim (slurp "neo4j/version.txt"))
         neo4j/neo4j-version)))

(deftest logical-schema-only-test
  (let [schema (slurp "neo4j/schema.cypher")]
    (is (= 8 (count (re-seq #"(?m)^CREATE CONSTRAINT " schema))))
    (is (not (re-find #"(?m)^CREATE INDEX " schema)))))

(deftest embedded-value-conversion-test
  (let [date (Date. 0)]
    (is (= {"personId" 100
            "countryXName" "Germany"
            "maxDate" "1970-01-01T00:00:00Z"}
           (neo4j/neo4j-parameters
             {:person-id 100
              :country-x-name "Germany"
              :max-date date})))
    (is (= date (neo4j/result-value (LocalDate/of 1970 1 1))))))

(deftest embedded-options-test
  (let [{:keys [connection benchmark]}
        (neo4j/parse-args ["--home" "/tmp/neo4j-home"
                           "--page-cache" "2g"
                           "--database" "neo4j"
                           "IS1"])]
    (is (= "/tmp/neo4j-home" (:home-path connection)))
    (is (= (* 2 1024 1024 1024) (:page-cache-bytes connection)))
    (is (= "neo4j" (:database connection)))
    (is (= ["IS1"] (:query-names benchmark)))
    (is (not (re-find #"Bolt|--address|--password" (neo4j/usage))))))
