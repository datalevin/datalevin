(ns datalevin-tpcc.payment-input-test
  (:require
   [clojure.test :refer [deftest is testing]]
   [datalevin-tpcc.datalevin :as datalevin]
   [datalevin-tpcc.generate :as g]
   [datalevin-tpcc.postgres :as postgres]
   [datalevin-tpcc.sqlite :as sqlite])
  (:import [java.util Random]))

(def ^:private drivers
  [#'datalevin/gen-input #'sqlite/gen-input #'postgres/gen-input])

(deftest payment-name-lookup-draws
  ;; Exercise every possible lookup draw through each real input generator.
  ;; TPC-C 2.5.1.2 requires 60 surname draws and 40 customer-ID draws.
  (doseq [generate drivers]
    (testing (str generate)
      (let [draws (atom 0)
            inputs
            (mapv (fn [draw]
                    (let [r (proxy [Random] []
                              (nextInt [bound]
                                (if (= bound 100)
                                  (do (swap! draws inc) (dec draw))
                                  0)))]
                      (generate r {:warehouses 1 :c-cust 0 :c-last 0} :payment)))
                  (range 1 101))]
        (is (= 100 @draws) "one lookup-method draw per Payment")
        (is (= (vec (concat (repeat 60 true) (repeat 40 false)))
               (mapv :by-name? inputs)))
        (is (every? (set g/last-names) (map :last-name (take 60 inputs))))
        (is (every? nil? (map :last-name (drop 60 inputs))))))))

(deftest payment-input-distribution-matches-across-drivers
  (doseq [warehouses [1 3]]
    (let [opts {:warehouses warehouses :c-cust 127 :c-last 37}
          samples (mapv (fn [generate]
                          (let [r (Random. 42)]
                            (vec (repeatedly 10000 #(generate r opts :payment)))))
                        drivers)]
      (is (apply = samples) "the same seed produces identical Payments on all backends")
      (doseq [[generate inputs] (map vector drivers samples)]
        (testing (str generate " warehouses=" warehouses)
          (is (<= 5800 (count (filter :by-name? inputs)) 6200)
              "about 60% of Payments use the surname index")
          (is (every? #(= (:by-name? %) (some? (:last-name %))) inputs)))))))
