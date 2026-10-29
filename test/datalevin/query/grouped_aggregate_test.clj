(ns datalevin.query.grouped-aggregate-test
  (:require [clojure.test :refer [deftest is]]
            [datalevin.built-ins :as built-ins]
            [datalevin.parser :as parser]
            [datalevin.query.aggregate :as aggregate]
            [datalevin.query.resolve.binding :as binding]
            [datalevin.relation :as relation])
  (:import [datalevin.utl GroupedAggregate]
           [org.eclipse.collections.impl.list.mutable FastList]))

(deftest grouped-arithmetic-matches-the-existing-builtins
  (doseq [values [[1 1 2] [1/2 1/3 1/7] [1.00M 2.0M 3.00M]
                  [1M 2 (float 3.5) 4.5]
                  [1.0 2.0 3.0 4M 5.0 6.0 7/2 8.0]
                  [1.0e16 1.0 -1.0e16 0.5]
                  (vec (concat (repeat 40 1.5) [2.0M] (repeat 40 0.5) [1/3]))
                  [Long/MAX_VALUE 1N]
                  [(bigint (.shiftLeft java.math.BigInteger/ONE 2000)) 2N]]
          context [nil java.math.MathContext/DECIMAL64]]
    (binding [*math-context* context]
      (let [acc (GroupedAggregate. (int-array [0 1 1 1])
                                  (int-array [GroupedAggregate/KEY GroupedAggregate/SUM
                                              GroupedAggregate/AVG GroupedAggregate/COUNT]))
            evaluate (fn [f] (try (f) (catch ArithmeticException _ :arithmetic-error)))
            expected (evaluate #(vector :group (built-ins/sum values)
                                        (built-ins/avg values) (count values)))
            actual (evaluate #(do (doseq [value values] (.add acc (object-array [:group value])))
                                  (vec (first (.results acc)))))]
        (is (= expected actual)))))
  (let [acc (GroupedAggregate. (int-array [0]) (int-array [GroupedAggregate/SUM]))]
    (is (empty? (.results acc)))
    (.add acc (object-array [Long/MAX_VALUE]))
    (is (thrown? ArithmeticException (.add acc (object-array [1]))))))

(deftest groups-preserve-query-key-equality
  (doseq [composite? [false true]]
    (let [acc (GroupedAggregate.
                (int-array (if composite? [0 1 2] [0 2]))
                (int-array (if composite?
                             [GroupedAggregate/KEY GroupedAggregate/KEY GroupedAggregate/SUM]
                             [GroupedAggregate/KEY GroupedAggregate/SUM])))]
      (doseq [row [[(byte-array [1 2]) :group 10]
                   [(byte-array [1 2]) :group 20]
                   [(int -1) :group 3] [(long -1) :group 4]
                   [1.0M :group 5] [1.00M :group 6]]]
        (.add acc (object-array row)))
      (is (= 4 (.groupCount acc)))
      (is (= #{5 6 7 30} (into #{} (map #(last (vec %))) (.results acc)))))))

(deftest scalar-bindings-preserve-projections-and-unique-keys
  (doseq [[attrs tuple] [[(array-map '?e 0 '?v 1) [1 10]]
                         [(array-map '?e 1 '?v 0) [10 1]]
                         [(array-map '?e 0 '?v 2) [1 :hidden 10]]]]
    (let [source (relation/with-unique-key
                   (relation/relation! attrs (FastList. [(object-array tuple)])) ['?e])
          result (binding/bind-scalar-tuples source '?x (constantly 20))
          row (vec (first (:tuples result)))]
      (is (= [1 10 20] row))
      (is (relation/unique-on? result ['?e]))
      (is (not (relation/unique-on? result ['?v '?x]))))))

(deftest rebinding-a-hidden-column-does-not-restore-its-old-key
  (let [source (relation/with-unique-key
                 (relation/relation! {'?e 0 '?v 1}
                   (FastList. [(object-array [1 10]) (object-array [2 10])])) ['?e])
        projected (assoc source :attrs {'?v 1})
        result (binding/bind-scalar-tuples projected '?e (constantly 100))]
    (is (= [[10 100] [10 100]] (mapv vec (:tuples result))))
    (is (not (relation/unique-on? result ['?e])))))

(deftest compiled-having-preserves-argument-and-nil-semantics
  (let [rows [[:a -1 1 0] [:b 0 2 1] [:c 10 2 11] [:d 20 3 21]
              [:e nil 0 nil]]
        query '[:find ?g (sum ?v) (count ?e) (+ (sum ?v) 1)
                :where [?e :group ?g] [?e :value ?v]]]
    (doseq [[having expected]
            [['[[(pos? (sum ?v))]] [:c :d]]
             ['[[(> (sum ?v) 10)]] [:d]]
             ['[[(< 10 (sum ?v))]] [:d]]
             ['[[(<= 0 (sum ?v) 10)]] [:b :c]]
             ['[[(pos? (sum ?v))] [(> (count ?e) 2)]] [:d]]
             ['[[(> (+ (sum ?v) 1) 11)]] [:d]]
             ['[[(= false false)]] [:a :b :c :d :e]]
             ['[[(= nil nil)]] []]]]
      (let [parsed (parser/parse-query (into query (cons :having having)))
            elements (parser/find-elements (:qfind parsed))
            keep? (aggregate/compile-having (:qhaving parsed) elements)]
        (doseq [values [rows (mapv object-array rows)]]
          (is (= expected (mapv first (filter keep? values)))))))
    (is (nil? (aggregate/compile-having [] [])))))

(deftest primitive-double-sums-preserve-order-and-ieee-values
  (doseq [values [[0.0 -0.0] [-0.0 -0.0] [Double/MAX_VALUE Double/MAX_VALUE]
                  [Double/POSITIVE_INFINITY Double/NEGATIVE_INFINITY]
                  [Double/NaN 1.0] [1.0e16 1.0 -1.0e16 0.5]]]
    (let [acc (GroupedAggregate. (int-array [0]) (int-array [GroupedAggregate/SUM]))
          row (object-array 1)
          values (concat values (repeat 64 0.0))]
      (doseq [value values] (aset row 0 value) (.add acc row))
      (is (= (Double/doubleToLongBits (double (built-ins/sum values)))
             (Double/doubleToLongBits (double (aget ^objects (first (.results acc)) 0))))))))
