(ns datalevin.ha-test
  (:require
   [clojure.test :refer [deftest is]]
   [datalevin.ha.util :as hu]))

(deftest saturated-long-add-clamps-positive-overflow
  (is (= Long/MAX_VALUE
         (hu/saturated-long-add (- Long/MAX_VALUE 5) 10))))
