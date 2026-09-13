(ns datalevin-tpcc.new-order-input-test
  (:require
   [clojure.test :refer [deftest is testing]]
   [datalevin-tpcc.common :as c]
   [datalevin-tpcc.datalevin :as datalevin]
   [datalevin-tpcc.generate :as g]
   [datalevin-tpcc.postgres :as postgres]
   [datalevin-tpcc.sqlite :as sqlite])
  (:import [java.util Random]))

(def ^:private drivers
  [#'datalevin/gen-input #'sqlite/gen-input #'postgres/gen-input])

(defn- invalid-line-indexes [lines]
  (keep-indexed (fn [i {:keys [i-id]}]
                  (when (> i-id c/item-count) i))
                lines))

(deftest new-order-rollback-draw-and-final-item
  (doseq [n [5 10 15]]
    (testing (str n " lines")
      (let [rollback-draws (atom 0)
            orders
            (mapv (fn [rbk]
                    (let [r (proxy [Random] []
                              (nextInt [bound]
                                (case (long bound)
                                  11 (- n 5)
                                  100 (do (swap! rollback-draws inc) (dec rbk))
                                  0)))]
                      (g/new-order-lines r 1 (constantly 123))))
                  (range 1 101))]
        (is (every? #(= n (count %)) orders))
        (is (= 100 @rollback-draws) "exactly one rollback draw per order")
        (is (= [(dec n)] (vec (invalid-line-indexes (first orders)))))
        (is (every? #(empty? (invalid-line-indexes %)) (rest orders)))
        (is (every? #(= 123 (:i-id %)) (butlast (first orders))))))))

(deftest new-order-workload-distribution
  (doseq [generate drivers]
    (testing (str generate)
      (let [r       (Random. 42)
            opts    {:warehouses 1 :c-item 0 :c-cust 0 :c-last 0}
            orders  (vec (repeatedly 10000 #(generate r opts :new-order)))
            counts  (mapv #(count (:ol %)) orders)
            invalid (filterv #(seq (invalid-line-indexes (:ol %))) orders)]
        (is (= (set (range 5 16)) (set counts)))
        (is (<= 9.85 (/ (reduce + counts) 10000.0) 10.15))
        (is (<= 70 (count invalid) 130) "about 1% of orders request rollback")
        (is (every? (fn [{:keys [ol]}]
                       (= [(dec (count ol))] (vec (invalid-line-indexes ol))))
                    invalid)
            "rollback orders contain exactly one invalid item, at the end")
        (is (every? (fn [{:keys [ol]}]
                       (and (every? #(<= 1 (:i-id %) c/item-count) (butlast ol))
                            (<= 1 (:i-id (peek ol)) (inc c/item-count))
                            (every? #(<= 1 (:qty %) 10) ol)))
                    orders))))))

(deftest new-order-inputs-match-across-drivers
  (doseq [warehouses [1 3]]
    (let [opts {:warehouses warehouses :c-item 751 :c-cust 127 :c-last 37}
          samples (mapv (fn [generate]
                          (let [r (Random. 42)]
                            (vec (repeatedly 1000 #(generate r opts :new-order)))))
                        drivers)]
      (is (apply = samples) "the same seed produces the same order stream"))))
