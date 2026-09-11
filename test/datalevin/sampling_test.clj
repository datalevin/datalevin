(ns datalevin.sampling-test
  (:require [clojure.test :refer [deftest is testing]]
            [datalevin.util :as u]))

(defn- sample [m n seed]
  (binding [u/*reservoir-sampling-seed* seed]
    (some-> (u/reservoir-sampling m n) vec)))

(deftest sampling-boundaries-and-replay
  (is (= [] (sample 0 0 1)))
  (is (= [] (sample Long/MAX_VALUE 0 1)))
  (is (= [0 1 2 3] (sample 4 4 1)))
  (is (nil? (sample 3 4 1)))
  (is (thrown? IllegalArgumentException (sample -1 0 1)))
  (is (thrown? IllegalArgumentException (sample 1 -1 1)))
  (doseq [[m n] [[10000 128] [Long/MAX_VALUE 128] [1000 999]]]
    (let [xs (sample m n 20260910)]
      (is (= xs (sample m n 20260910)))
      (is (not= xs (sample m n 20260911)))
      (is (= n (count xs) (count (distinct xs))))
      (is (= xs (sort xs)))
      (is (every? #(<= 0 % (dec m)) xs)))))

(deftest sampling-does-not-retain-the-initial-reservoir
  (let [m 31615420 n 163840
        xs (sample m n 92026)
        early (count (take-while #(< % n) xs))]
    ;; Uniform expectation is 849; the old fixed skip probability gave 60,529.
    (is (< 600 early 1100))))

(deftest sampling-distribution
  (testing "every part of a population is represented across fixed seeds"
    (let [samples (mapcat #(sample 10000 100 %) (range 300))
          bins (frequencies (map #(quot % 1000) samples))]
      (doseq [bin (range 10)]
        (is (< 2700 (get bins bin 0) 3300)))))
  (testing "all two-element subsets have comparable probability"
    (let [counts (frequencies (map #(sample 5 2 %) (range 5000)))]
      (is (= 10 (count counts)))
      (doseq [n (vals counts)] (is (< 380 n 620))))))
