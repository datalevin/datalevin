(ns datalevin.leftist-heap-test
  (:require [clojure.test :refer [deftest is]])
  (:import [datalevin.utl LeftistHeap]
           [java.util Random TreeSet]))

(defn- heap ^LeftistHeap []
  (proxy [LeftistHeap] []
    (lessThan [a b] (< (long a) (long b)))))

(deftest merge-consumes-donor-index
  (let [left (heap) right (heap)]
    (.insert left 4)
    (.insert right 1)
    (.insert right 3)
    (.merge left right)
    (is (nil? (.findNode right 1)))
    (is (nil? (.findNode right 3)))
    (.insert right 2)
    (.deleteMin right)
    (.deleteElement left 3)
    (is (= 1 (.findMin left)))
    (.deleteMin left)
    (is (= 4 (.findMin left)))
    (.deleteMin left)
    (is (nil? (.findNode left 4)))))

(deftest mixed-heap-operations
  (let [rng (Random. 20260910)
        heaps (vec (repeatedly 4 heap))
        models (vec (repeatedly 4 #(TreeSet.)))]
    (dotimes [step 4000]
      (let [idx (.nextInt rng 4)
            ^LeftistHeap h (heaps idx)
            ^TreeSet model (models idx)]
        (case (.nextInt rng 4)
          0 (do (.insert h step) (.add model step))
          1 (when-not (.isEmpty model)
              (.deleteMin h)
              (.pollFirst model))
          2 (when-not (.isEmpty model)
              (let [value (nth (vec model) (.nextInt rng (.size model)))]
                (.deleteElement h value)
                (.remove model value)))
          3 (let [other (mod (inc idx) 4)
                  ^LeftistHeap donor (heaps other)
                  ^TreeSet donor-model (models other)]
              (.merge h donor)
              (.addAll model donor-model)
              (doseq [value donor-model] (is (nil? (.findNode donor value))))
              (.clear donor-model)))
        (when-not (.isEmpty model)
          (is (= (.first model) (.findMin h))))))
    (doseq [[^LeftistHeap h ^TreeSet model] (map vector heaps models)]
      (doseq [value model]
        (is (= value (.findMin h)))
        (.deleteMin h)))))
