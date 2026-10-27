(ns datalevin.like-test
  (:require [clojure.test :refer [deftest is]]
            [datalevin.built-ins :as b])
  (:import [datalevin.utl LikeFSM]
           [java.nio.charset StandardCharsets]))

(deftest wildcard-overlaps-empty-input-and-unicode-codepoints
  (doseq [[input pattern expected]
          [["" "" true] ["" "%%" true] ["" "%%%" true]
           ["" "%_" false] ["a" "%_" true] ["aaa" "%_" true]
           ["é" "_" true] ["猫" "_" true] ["😀" "_" true]
           ["😀" "__" false] ["a\nb" "a_b" true]
           ["aaab" "%aab" true] ["aabaab" "%aab%ab" true]
           ["aXbYc" "a%b%c" true] ["aXbYd" "a%b%c" false]
           ["%" "!%" true] ["_" "!_" true] ["!" "!!" true]
           ["" "!" true] ["ab" "ab!" true]]]
    (is (= expected (b/like input pattern)) (pr-str [input pattern]))
    (is (= (not expected) (b/not-like input pattern))
        (pr-str [input pattern])))
  (is (b/like "a%b" "a猫%b" {:escape \猫}))
  (is (b/like "a_b" "aĀ_b" {:escape \Ā}))
  (is (b/like "Ā" "ĀĀ" {:escape \Ā}))
  (doseq [pattern ["x!a" "!猫" "%!x" "x|a" "x猫a"]
          :let [escape (case pattern "x|a" \| "x猫a" \猫 \!)]
          f [b/like b/not-like]]
    (is (thrown-with-msg? IllegalStateException #"Can only escape"
                         (f "" pattern {:escape escape})))))

(deftest java-byte-entry-points-retain-utf8-semantics
  (doseq [[input pattern] [["😀" "_"] ["é%" "_!%"]
                           ["abc" "a%"] ["" "%%"]]]
    (let [matcher (LikeFSM. (.getBytes ^String pattern StandardCharsets/UTF_8))]
      (is (.match matcher (.getBytes ^String input StandardCharsets/UTF_8)))
      (is (.match matcher ^String input)))))
