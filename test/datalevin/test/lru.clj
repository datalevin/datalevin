(ns datalevin.test.lru
  (:require
   [clojure.test :as t :refer [is are deftest]]
   [datalevin.util :as u])
  (:import [datalevin.utl LRUCache]))

(deftest test-put
  (let [tgt (System/currentTimeMillis)
        l   (LRUCache. 2 tgt)]
    (is (= (.target l) tgt))
    (is (nil? (.get l :a)))
    (.put l :a 1)
    (is (= (.get l :a) 1))
    (.put l :b 2)
    (is (= (.get l :a) 1))
    (is (= (.get l :b) 2))
    (.put l :c 3)
    (is (nil? (.get l :a))) ;; :a get evicted on third insert
    (is (= (.get l :b) 2))
    (is (= (.get l :c) 3))
    (.put l :b 4)
    (is (= (.get l :b) 4))
    (is (= (.get l :c) 3))
    (.put l :d 5)
    (is (= (.get l :c) 3))
    (is (= (.get l :d) 5))
    (is (nil? (.get l :b))) ;; :b get evicted because :c is accessed more recently
    ))

(deftest test-remove
  (let [l (LRUCache. 2)]
    (is (nil? (.get l :a)))
    (.put l :a 1)
    (is (= (.get l :a) 1))
    (.put l :b 2)
    (is (= (.get l :a) 1))
    (is (= (.get l :b) 2))
    (.remove l :b)
    (is (nil? (.get l :b)))
    (is (= (.get l :a) 1))
    (.put l :b 4)
    (is (= (.get l :b) 4))
    (is (= (.get l :a) 1))
    (.put l :d 5)
    (is (nil? (.get l :b)));; :b get evicted because :a is accessed more recently
    (is (= (.get l :d) 5))))

(deftest test-keys-and-target
  (let [l (LRUCache. 4 1)]
    (is (.isEmpty l))
    (.put l :a 1)
    (.put l :b 2)
    (is (not (.isEmpty l)))
    (is (= #{:a :b} (set (.keys l))))
    (.clear l)
    (is (.isEmpty l))
    (.setTarget l 42)
    (is (= 42 (.target l)))))

(deftest stale-generation-cannot-publish
  (let [l          (LRUCache. 4 1)
        generation (.generation l)]
    (is (.putIfGeneration l :before :old generation))
    (.beginInvalidation l 2)
    (.remove l :before)
    (is (not (.putIfGeneration l :stale :old generation)))
    (is (nil? (.get l :stale)))
    (is (.putIfGeneration l :current :new (.generation l)))
    (is (= :new (.get l :current)))))
