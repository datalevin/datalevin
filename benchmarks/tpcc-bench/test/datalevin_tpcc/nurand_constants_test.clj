(ns datalevin-tpcc.nurand-constants-test
  (:require
   [clojure.test :refer [deftest is testing]]
   [datalevin-tpcc.generate :as g])
  (:import [java.util Random]))

(def ^:private valid-deltas (disj (set (range 65 120)) 96 112))

(deftest surname-constant-boundaries-and-excluded-differences
  (doseq [c-load [0 255]
          delta [0 64 65 96 112 119 120 255]]
    (testing (str "C-Load=" c-load " delta=" delta)
      (let [candidate (if (zero? c-load) delta (- 255 delta))
            fallback (if (zero? c-load) 65 190)
            draws (atom [candidate fallback])
            r (proxy [Random] []
                (nextInt [bound]
                  (is (= 256 bound))
                  (let [value (first @draws)]
                    (swap! draws subvec 1)
                    value)))]
        (is (= (if (valid-deltas delta) candidate fallback)
               (#'g/run-c-last r c-load)))))))

(deftest every-population-constant-has-valid-run-constants
  (doseq [c-load (range 256)
          seed (range 20)]
    (let [c-run (#'g/run-c-last (Random. seed) c-load)]
      (is (and (<= 0 c-run 255)
               (valid-deltas (Math/abs (long (- c-load c-run)))))
          (str "C-Load=" c-load " seed=" seed)))))

(deftest run-constants-are-reproducible-with-independent-load-seeds
  (is (= 186 (g/load-c-last 42)))
  (doseq [seed [0 1 42 104729 123456789]
          load-seed [0 42 104729 123456789]]
    (let [{:keys [c-last c-cust c-item] :as constants}
          (g/run-constants seed load-seed)
          delta (Math/abs (long (- (g/load-c-last load-seed) c-last)))
          r (Random. seed)]
      (is (valid-deltas delta))
      (is (= constants (g/run-constants seed load-seed)))
      (is (= [(.nextInt r 8192) (.nextInt r 1024)] [c-item c-cust])
          "customer-ID and item constants keep their existing seeded draws"))))
