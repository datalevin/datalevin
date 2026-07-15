(ns datalevin.test.join
  (:require
   [clojure.test :refer [deftest is]]
   [datalevin.join :as j]
   [datalevin.relation :as r])
  (:import
   [org.eclipse.collections.impl.list.mutable FastList]))

(defn- tuples
  [& rows]
  (FastList. ^java.util.Collection (mapv object-array rows)))

(defn- row-set
  [relation]
  (set (map vec (:tuples relation))))

(deftest hash-join-long-key
  (let [left  (r/relation! {'?id 0 '?left 1}
                           (tuples [1 :a] [2 :b] [2 :c]))
        right (r/relation! {'?id 0 '?right 1}
                           (tuples [2 :x] [3 :y]))]
    (is (= #{[2 :b :x] [2 :c :x]}
           (row-set (j/hash-join left right))))
    (let [sink (FastList.)]
      (j/hash-join-into left right sink)
      (is (= #{[2 :b :x] [2 :c :x]} (set (map vec sink)))))))

(deftest hash-join-falls-back-for-mixed-keys
  (let [left  (r/relation! {'?id 0 '?left 1}
                           (tuples [1 :number] ["1" :string]))
        right (r/relation! {'?id 0 '?right 1}
                           (tuples [1 :a] ["1" :b] [2 :c]))]
    (is (= #{[1 :number :a] ["1" :string :b]}
           (row-set (j/hash-join left right))))))

(deftest hash-join-composite-key
  (let [left  (r/relation! {'?id 0 '?kind 1 '?left 2}
                           (tuples [1 :a :left-a] [1 :b :left-b]))
        right (r/relation! {'?id 0 '?kind 1 '?right 2}
                           (tuples [1 :b :right-b] [2 :a :right-a]))]
    (is (= #{[1 :b :left-b :right-b]}
           (row-set (j/hash-join left right))))))
