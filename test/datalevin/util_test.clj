(ns datalevin.util-test
  (:require
   [clojure.test :refer [deftest is]]
   [datalevin.util :as u]))

(deftest lisp-case-preserves-name-normalization-test
  (doseq [[input expected]
          [["db" "db"] ["db123" "db123"] ["123-db" "123-db"]
           ["a-b-c" "a-b-c"] ["SimpleName" "simple-name"]
           ["HTTPServer" "http-server"] ["simple_name" "simple-name"]
           ["--simple--name--" "simple-name"] ["aB" "a-b"]
           ["User42" "user42"] ["UPPER_CASE" "upper-case"]
           ["résumé" "r-sum"] ["" ""] [" -_ / " ""]]]
    (is (= expected (u/lisp-case input)) input)))
