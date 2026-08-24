(ns datalevin.test.csv
  (:require
   [clojure.test :refer [deftest is]]
   [datalevin.csv :as csv])
  (:import
   [java.io StringReader]))

(defn- read-rows
  ([s]
   (read-rows s \, \"))
  ([s separator quote]
   (vec (csv/read-csv s :separator separator :quote quote))))

(deftest read-csv-test
  (is (= [["a" "b,c" "d\"e"]
          ["line 1\nline 2" "" "tail"]]
         (read-rows "a,\"b,c\",\"d\\\"e\"\r\n\"line 1\nline 2\",,tail\n")))
  (is (= [["a" "b"] ["c" "d"]]
         (read-rows "a|b\r\nc|d" \| \")))
  (is (= [["a" ""]] (read-rows "a,")))
  (is (= [[""]] (read-rows "\n")))
  (is (= [] (read-rows "")))
  (is (every? vector? (read-rows "a,b\nc,d\n")))
  (is (= [["a" "b"]]
         (vec (csv/read-csv (StringReader. "a,b\n"))))))

(deftest read-csv-backslash-before-closing-quote-test
  (is (= [["1" "value\\" "tail"]
          ["2" "next" "row"]]
         (read-rows "1,\"value\\\\\",tail\n2,next,row\n"))))

(deftest read-csv-is-lazy-test
  (let [reads  (atom 0)
        reader (proxy [java.io.Reader] []
                 (read [buffer offset length]
                   (swap! reads inc)
                   -1)
                 (close []))
        rows   (csv/read-csv reader)]
    (is (zero? @reads))
    (is (empty? rows))
    (is (= 1 @reads))))
