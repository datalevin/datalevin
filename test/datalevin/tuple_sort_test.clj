(ns datalevin.tuple-sort-test
  (:require [clojure.test :refer [deftest is testing]]
            [datalevin.storage.scan :as scan])
  (:import [java.util ArrayList Collections LinkedList Random]
           [org.eclipse.collections.impl.list.mutable FastList]))

(deftest entity-ordering-is-stable-for-dense-and-sparse-keys
  (doseq [[label keys]
          [[:empty []]
           [:small [3 1 3 2 1]]
           [:dense (mapv #(long (mod % 503)) (range 9000))]
           [:negative (mapv #(- (long (mod % 503)) 1000) (range 9000))]
           [:sparse (mapv #(* 1000000 (long %)) (range 9000))]
           [:extremes (vec (take 9000 (cycle [Long/MAX_VALUE 0 Long/MIN_VALUE -1 1])))]]
          make-list [#(FastList. ^java.util.Collection %)
                     #(ArrayList. ^java.util.Collection %)
                     #(LinkedList. ^java.util.Collection %)]]
    (testing (str label)
      (let [rows (mapv (fn [id key] (object-array [id key])) (range) keys)
            shuffled (ArrayList. ^java.util.Collection rows)]
        (Collections/shuffle shuffled (Random. 42))
        (let [expected (vec (sort-by #(long (aget ^objects % 1)) shuffled))
              actual (make-list shuffled)]
          (is (identical? actual (scan/sort-tuples-by-eid actual 1)))
          (is (= (mapv #(aget ^objects % 0) expected)
                 (mapv #(aget ^objects % 0) actual)))
          (is (every? true? (map identical? expected actual))))))))

(deftest ordered-and-equal-keys-retain-their-tuples
  (doseq [keys [(range 5000) (repeat 5000 -5) (range -5000 0)]]
    (let [rows (mapv #(object-array [%]) keys)
          actual (FastList. ^java.util.Collection rows)]
      (scan/sort-tuples-by-eid actual 0)
      (is (every? true? (map identical? rows actual))))))
