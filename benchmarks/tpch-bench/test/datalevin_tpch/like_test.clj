(ns datalevin-tpch.like-test
  (:require [clojure.test :refer [deftest is]]
            [datalevin.built-ins :as b])
  (:import [java.sql DriverManager]
           [java.util Random]))

(deftest native-like-agrees-with-case-sensitive-sql
  (with-open [conn (DriverManager/getConnection "jdbc:sqlite::memory:")
              setup (.createStatement conn)
              query (.prepareStatement conn
                       "SELECT ? LIKE ? ESCAPE ?, ? NOT LIKE ? ESCAPE ?")]
    (.execute setup "PRAGMA case_sensitive_like=ON")
    (let [random (Random. 72931)
          literals ["a" "b" "é" "猫" "😀" "\n" "!" "%" "_" "Ā" "\uFFFF"]
          generated
          (repeatedly 1500
            (fn []
              (let [escape (nth [\! \| \Ā \猫] (.nextInt random 4))
                    literal #(nth literals (.nextInt random (count literals)))
                    input (apply str (repeatedly (.nextInt random 9) literal))
                    pattern (apply str
                              (repeatedly (.nextInt random 7)
                                #(case (.nextInt random 4)
                                   0 "%"
                                   1 "_"
                                   (let [s (literal)]
                                     (if (or (= s (str escape)) (#{"%" "_"} s))
                                       (str escape s) s)))))]
                [input pattern escape])))
          examples [["" "%%" \!] ["a" "%_" \!] ["aaa" "%_" \!]
                    ["é" "_" \!] ["😀" "_" \!] ["😀" "__" \!]
                    ["aaab" "%aab" \!] ["aabaab" "%aab%ab" \!]
                    ["USA:1 August 2007" "USA:% 200%" \!]
                    ["|title|" "%||%||%" \|]
                    ["book |No. 1|" "%book%||" \|]
                    ["special\nrequests" "%special%requests%" \!]
                    ["Special requests" "%special%requests%" \!]
                    ["a%b" "a猫%b" \猫]
                    ["%" "%%" \%] ["_" "__" \_]]]
      (doseq [[input pattern escape] (concat examples generated)]
        (doseq [[i value] (map-indexed vector
                           [input pattern (str escape) input pattern (str escape)])]
          (.setString query (int (inc i)) value))
        (with-open [result (.executeQuery query)]
          (is (.next result))
          (is (= (.getBoolean result 1) (b/like input pattern {:escape escape}))
              (pr-str [input pattern escape]))
          (is (= (.getBoolean result 2) (b/not-like input pattern {:escape escape}))
              (pr-str [input pattern escape])))))))
