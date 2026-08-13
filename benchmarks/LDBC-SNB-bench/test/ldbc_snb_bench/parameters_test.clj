(ns ldbc-snb-bench.parameters-test
  (:require
   [clojure.test :refer [deftest is]]
   [ldbc-snb-bench.parameters :as parameters])
  (:import
   [java.nio.file Files]
   [java.nio.file.attribute FileAttribute]
   [java.util Date]))

(defn- temp-file
  [name content]
  (let [dir  (.toFile (Files/createTempDirectory
                        "ldbc-params-" (make-array FileAttribute 0)))
        file (java.io.File. dir name)]
    (.deleteOnExit dir)
    (.deleteOnExit file)
    (spit file content)
    file))

(deftest normalize-parameter-map-test
  (let [params (parameters/normalize-parameter-map
                 :ic3
                 {"personId" "100"
                  "startDate" "1325376000000"
                  "durationDays" "365"
                  "countryXName" "Germany"
                  "countryYName" "France"})]
    (is (= 100 (:person-id params)))
    (is (= 365 (:duration-days params)))
    (is (= 1325376000000 (.getTime ^Date (:start-date params))))
    (is (= "Germany" (:country-x-name params)))))

(deftest official-substitution-file-test
  (let [file (temp-file
               "interactive_3_param.txt"
               (str "personId|startDate|durationDays|countryXName|countryYName\n"
                    "100|1325376000000|365|Germany|France\n"
                    "200|2012-01-01T00:00:00Z|30|Spain|China\n"))
        rows (parameters/read-official-file :ic3 file)]
    (is (= 2 (count rows)))
    (is (= {:person-id 100
            :start-date (Date. 1325376000000)
            :duration-days 365
            :country-x-name "Germany"
            :country-y-name "France"}
           (first rows)))
    (is (= 200 (:person-id (second rows))))))

(deftest official-header-validation-test
  (let [file (temp-file "interactive_1_param.txt"
                        "firstName|personId\nJohn|100\n")]
    (is (thrown-with-msg?
          clojure.lang.ExceptionInfo
          #"Unexpected header"
          (parameters/read-official-file :ic1 file)))))

(deftest edn-parameter-suite-test
  (let [file (temp-file
               "parameters.edn"
               (str "{:ic1 [{:person-id 100 :first-name \"John\"} "
                    "{:person-id \"200\" :first-name \"Jane\"}], "
                    ":is4 {:message-id 42}}"))
        suite (parameters/read-edn-suite file)]
    (is (= :edn (:kind suite)))
    (is (= 2 (count (get-in suite [:parameters :ic1]))))
    (is (= 200 (get-in suite [:parameters :ic1 1 :person-id])))
    (is (= [{:message-id 42}] (get-in suite [:parameters :is4])))))

(deftest missing-parameter-validation-test
  (is (thrown-with-msg?
        clojure.lang.ExceptionInfo
        #"Missing parameters"
        (parameters/normalize-parameter-map :ic1 {:person-id 100}))))
