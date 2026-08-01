;;
;; Copyright (c) Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns datalevin.test.util
  (:require
   [clojure.test :refer [deftest is testing]]
   [datalevin.util :as u]))

(deftest deterministic-reservoir-sampling
  (let [sample (fn [seed]
                 (binding [u/*reservoir-sampling-seed* seed]
                   (vec (u/reservoir-sampling 10000 128))))
        first-sample  (sample 20260727)
        second-sample (sample 20260728)]
    (testing "a seed exactly replays a sample"
      (is (= first-sample (sample 20260727))))
    (testing "different seeds identify different samples"
      (is (not= first-sample second-sample)))
    (testing "sample invariants are preserved"
      (is (= 128 (count first-sample)))
      (is (= first-sample (sort first-sample)))
      (is (= 128 (count (distinct first-sample))))
      (is (every? #(< -1 % 10000) first-sample)))))

(deftest deterministic-reservoir-sampling-boundaries
  (binding [u/*reservoir-sampling-seed* 42]
    (is (= (vec (range 4))
           (vec (u/reservoir-sampling 4 4))))
    (is (nil? (u/reservoir-sampling 3 4)))))
