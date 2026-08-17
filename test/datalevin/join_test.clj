;;
;; Copyright (c) Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns datalevin.join-test
  (:require
   [clojure.test :refer [deftest is testing]]
   [datalevin.join :as j]
   [datalevin.relation :as r])
  (:import
   [datalevin.utl LongPairObjectHashMap]
   [java.util List]
   [org.eclipse.collections.impl.list.mutable FastList]))

(defn- tuples
  [& rows]
  (FastList. ^java.util.Collection (mapv object-array rows)))

(defn- row-set
  [relation]
  (set (map vec (:tuples relation))))

(deftest two-long-key-hash-join-test
  (testing "the smaller right relation is hashed, including duplicate keys"
    (let [left  (r/relation! {'?person 0 '?forum 1 '?left 2}
                             (tuples [1 10 :a]
                                     [1 10 :b]
                                     [2 20 :c]
                                     [3 30 :d]))
          right (r/relation! {'?person 0 '?forum 1 '?right 2}
                             (tuples [1 10 :x]
                                     [1 10 :y]
                                     [2 99 :z]))
          expected #{[1 10 :a :x]
                     [1 10 :a :y]
                     [1 10 :b :x]
                     [1 10 :b :y]}]
      (is (= expected (row-set (j/hash-join left right))))
      (let [sink (FastList.)]
        (is (identical? sink (j/hash-join-into left right sink)))
        (is (= expected (set (map vec sink)))))))

  (testing "the smaller left relation is hashed"
    (let [left  (r/relation! {'?person 0 '?forum 1 '?left 2}
                             (tuples [4 40 :a]
                                     [4 40 :b]))
          right (r/relation! {'?person 0 '?forum 1 '?right 2}
                             (tuples [4 40 :x]
                                     [4 41 :y]
                                     [5 40 :z]))]
      (is (= #{[4 40 :a :x] [4 40 :b :x]}
             (row-set (j/hash-join left right)))))))

(deftest two-value-key-fallback-test
  (testing "a late non-Long build key falls back without losing prior rows"
    (let [left  (r/relation! {'?id 0 '?kind 1 '?left 2}
                             (tuples [1 10 :longs]
                                     ["1" 10 :string]
                                     [1 :ten :keyword]))
          right (r/relation! {'?id 0 '?kind 1 '?right 2}
                             (tuples [1 10 :a]
                                     ["1" 10 :b]
                                     [1 :ten :c]
                                     [2 20 :d]))]
      (is (= #{[1 10 :longs :a]
               ["1" 10 :string :b]
               [1 :ten :keyword :c]}
             (row-set (j/hash-join left right))))))

  (testing "the fixed-width key keeps Java numeric equality semantics"
    (let [left  (r/relation! {'?id 0 '?kind 1}
                             (tuples [(int 1) 10]))
          right (r/relation! {'?id 0 '?kind 1 '?right 2}
                             (tuples [(long 1) 10 :long]))]
      (is (empty? (:tuples (j/hash-join left right)))))))

(deftest large-two-long-key-hash-join-test
  (let [left-rows  (mapv (fn [i]
                           (let [i (long i)]
                             [i (* 17 i) (keyword (str "l" i))]))
                         (range 300))
        right-rows (mapv (fn [i]
                           (let [i (long i)]
                             [i (* 17 i) (keyword (str "r" i))]))
                         (range 300))
        left       (r/relation! {'?a 0 '?b 1 '?left 2}
                                (FastList. ^java.util.Collection
                                           (mapv object-array left-rows)))
        right      (r/relation! {'?a 0 '?b 1 '?right 2}
                                (FastList. ^java.util.Collection
                                           (mapv object-array right-rows)))
        expected   (set (map (fn [[a b l] [_ _ r]] [a b l r])
                             left-rows right-rows))]
    (is (= expected (row-set (j/hash-join left right))))))

(deftest wider-composite-key-fallback-test
  (let [left  (r/relation! {'?a 0 '?b 1 '?c 2 '?left 3}
                           (tuples [1 2 3 :match]
                                   [1 2 4 :miss]))
        right (r/relation! {'?a 0 '?b 1 '?c 2 '?right 3}
                           (tuples [1 2 3 :right]))]
    (is (= #{[1 2 3 :match :right]}
           (row-set (j/hash-join left right))))))

(deftest primitive-pair-map-growth-test
  (let [m    (LongPairObjectHashMap. 1)
        rows (mapv (fn [i] (object-array [i])) (range 1000))]
    (dotimes [i 1000]
      (let [j (long (- 1000 (long i)))]
        (.add m (long i) j ^objects (rows i))))
    (is (loop [i 0]
          (if (< i 1000)
            (let [j (long (- 1000 (long i)))]
              (if (identical? (rows i) (.get m (long i) j))
                (recur (unchecked-inc i))
                false))
            true)))
    (is (nil? (.get m 500 501)))
    (.add m 500 500 (object-array [:duplicate]))
    (let [bucket (.get m 500 500)]
      (is (instance? List bucket))
      (is (= [[500] [:duplicate]] (mapv vec bucket))))))
