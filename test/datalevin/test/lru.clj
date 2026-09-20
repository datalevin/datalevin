(ns datalevin.test.lru
  (:require
   [clojure.test :as t :refer [is deftest]])
  (:import [datalevin.utl LRUCache]
           [java.util.function Function]))

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

(deftest disabled-reader-cannot-publish-after-enable
  (let [l (LRUCache. 4 1)]
    (.put l :unaffected :keep)
    (.disable l)
    ;; Invalidation can precede the native transaction's commit. A reader
    ;; starting here still sees the old data despite capturing this generation.
    (.beginInvalidation l 2)
    (let [generation (.generation l)]
      (is (not (.putIfGeneration l :stale :old generation)))
      (.enable l)
      (is (not (.putIfGeneration l :stale :old generation)))
      (is (nil? (.get l :stale))))
    (is (= :keep (.get l :unaffected)))
    (let [generation (.generation l)]
      ;; Enabling an already enabled cache must not reject current readers.
      (.enable l)
      (is (.putIfGeneration l :current :new generation)))
    (is (= :new (.get l :current)))))

(deftest dependency-index-follows-lru-lifecycle
  (let [classified (atom [])
        l (LRUCache. 2 1
                     (reify Function
                       (apply [_ k]
                         (swap! classified conj k)
                         ;; Repeated dependencies must be safe on removal.
                         [(first k) :shared :shared])))
        candidates #(set (.candidateKeys l %))]
    (.put l [:a 1] :one)
    (.put l [:b 2] :two)
    (.put l [:a 1] :replacement)
    (is (empty? @classified))
    (is (= #{[:a 1]} (candidates [:a])))
    (is (= #{[:a 1] [:b 2]} (set @classified)))
    (is (= 2 (count @classified)))
    (is (= #{[:a 1] [:b 2]} (candidates [:shared :a])))
    ;; Candidate lookup must not promote :b ahead of :a in the LRU.
    (candidates [:b])
    (.put l [:c 3] :three)
    (is (nil? (.get l [:b 2])))
    (is (empty? (candidates [:b])))
    (is (= #{[:a 1] [:c 3]} (candidates [:shared])))
    (is (= :replacement (.remove l [:a 1])))
    (is (empty? (candidates [:a])))
    (let [generation (.generation l)]
      (.beginInvalidation l 2)
      (is (not (.putIfGeneration l [:stale 4] :old generation)))
      (is (empty? (candidates [:stale])))
      (.disable l)
      (.put l [:disabled 5] :no)
      (is (not (.putIfGeneration l [:disabled 6] :no (.generation l))))
      (is (empty? (candidates [:disabled])))
      (.enable l)
      (is (.putIfGeneration l [:fresh 7] nil (.generation l)))
      (is (= #{[:fresh 7]} (candidates [:fresh]))))
    (.clear l)
    (is (empty? (candidates [:shared])))
    (doseq [field-name ["keyDependencies" "dependencyKeys"]]
      (let [field (.getDeclaredField LRUCache field-name)]
        (.setAccessible field true)
        (is (empty? (.get field l)))))
    (.put l [:a 1] :new)
    (is (= #{[:a 1]} (candidates [:a])))))

(deftest dependency-index-with-no-retained-entries
  (let [l (LRUCache. 0 0 (reify Function (apply [_ k] [k k])))]
    (.put l :a 1)
    (is (.isEmpty l))
    (is (empty? (.candidateKeys l [:a]))))
  (let [l (LRUCache. 2)]
    (.put l :unindexed 1)
    (is (= #{:unindexed} (set (.candidateKeys l [:anything]))))))

(deftest dependency-index-stays-bounded-after-eviction
  (doseq [capacity [0 8]]
    (let [l (LRUCache. capacity 0 (reify Function (apply [_ k] [k])))]
      (dotimes [k 1000] (.put l k k))
      (.candidateKeys l [999])
      (dotimes [k 1000] (.put l (+ k 1000) k))
      (doseq [field-name ["keyDependencies" "dependencyKeys"]]
        (let [field (.getDeclaredField LRUCache field-name)]
          (.setAccessible field true)
          (is (= capacity (count (.get field l)))))))))
