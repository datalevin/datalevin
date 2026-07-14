(ns datalevin.test.relation
  (:require
   [clojure.test :refer [deftest is]]
   [datalevin.relation :as r])
  (:import
   [datalevin.utl ArrayUtil]
   [java.util HashSet]))

(deftest array-wrapper-hash-test
  (let [a  (r/wrap-array (object-array [0 31]))
        a' (r/wrap-array (object-array [0 31]))
        b  (r/wrap-array (object-array [1 0]))]
    (is (= a a'))
    (is (= (.hashCode a) (.hashCode a')))
    ;; These dense numeric pairs collide under java.util.Arrays/hashCode.
    (is (not= (.hashCode a) (.hashCode b)))))

(deftest precomputed-array-hash-test
  (let [tuple   (object-array [1 2])
        wrapped (r/wrap-array tuple)
        h       (.hashCode wrapped)
        hashed  (r/wrap-array-with-hash tuple h)
        lookup  (r/array-lookup)
        seen    (doto (HashSet.) (.add wrapped))]
    (is (= wrapped hashed))
    (is (= h (.hashCode hashed)))
    (is (.contains seen
                   (r/reset-array-lookup-with-hash! lookup tuple h)))))

(deftest fill-rule-output-hash-test
  (let [output   (object-array 2)
        h        (ArrayUtil/fillRuleOutputAndHash
                   output
                   (long-array [0 1])
                   (int-array [1 0])
                   (object-array [10 20])
                   (int-array [0 1])
                   30
                   nil)]
    (is (= [20 30] (vec output)))
    (is (= h (ArrayUtil/hashObjectArray output)))))
