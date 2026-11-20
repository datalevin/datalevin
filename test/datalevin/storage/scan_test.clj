(ns datalevin.storage.scan-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [datalevin.core :as d]
            [datalevin.db :as db]
            [datalevin.storage.scan :as scan]
            [datalevin.test.core :refer [db-fixture]])
  (:import [java.util Collection Collections Random]
           [java.util.concurrent CyclicBarrier TimeUnit]
           [org.eclipse.collections.impl.list.mutable FastList]))

(use-fixtures :each db-fixture)

(deftest scan-sorts-preserve-cached-initial-tuples
  (with-open [conn (d/create-conn nil
                                 {:name {:db/valueType :db.type/string}
                                  :code {:db/valueType :db.type/long}}
                                 {:background-sampling? false :cache-limit 16})]
    (d/transact! conn [{:db/id 30 :name "b" :code 20}
                       {:db/id 10 :name "c" :code 30}
                       {:db/id 20 :name "a" :code 10}])
    (let [view @conn
          store (:store view)
          ;; The range order differs from both entity and string sort order.
          ranges (mapv (fn [v] [[:closed v] [:closed v]]) ["b" "c" "a"])
          read-tuples #(db/-init-tuples-list view :name ranges nil true)
          original [[30 "b"] [10 "c"] [20 "a"]]
          cache-key (db/index-cache-key store [:init-tuples :name ranges nil true])]
      (is (= original (mapv vec (read-tuples))))
      (is (some? (db/cache-get store cache-key)))
      (doseq [[label scan expected]
              [["EAV scan by entity ID"
                #(db/-eav-scan-v-list view % 0 [[:code {:skip? false}]])
                [[10 "c" 30] [20 "a" 10] [30 "b" 20]]]
               ["AVE scan by string value"
                #(db/-val-eq-scan-e-list view % 1 :name)
                [[20 "a" 20] [30 "b" 30] [10 "c" 10]]]
               ["AVE scan by long value"
                #(db/-val-eq-scan-e-list view % 0 :code)
                [[10 "c" 20] [20 "a" 30] [30 "b" 10]]]]]
        (testing label
          ;; Hold a cache hit across another reader's sort to reproduce shared
          ;; list mutation without relying on the timing of parallel sorts.
          (let [other-reader (read-tuples)
                result (scan (read-tuples))]
            (is (= expected (mapv vec result)))
            (is (= original (mapv vec other-reader)) "another reader's input")
            (is (= original (mapv vec (read-tuples))) "a subsequent cache hit")))))))

(deftest scan-sorts-return-stable-shallow-copies
  (let [rows (mapv object-array [[30 "b"] [10 "c"] [10 "b"] [20 "a"]])
        input (FastList. ^Collection rows)]
    (doseq [[label sorter order]
            [["entity ID" #(scan/sort-tuples-by-eid % 0) [1 2 3 0]]
             ["long value" #(scan/sort-tuples-by-val % 0 :db.type/long) [1 2 3 0]]
             ["ref value" #(scan/sort-tuples-by-val % 0 :db.type/ref) [1 2 3 0]]
             ["string value" #(scan/sort-tuples-by-val % 1 :db.type/string) [3 0 2 1]]]]
      (testing label
        (let [result (sorter input)]
          (is (not (identical? input result)))
          ;; Row arrays compare by identity, including within equal-key groups.
          (is (= (mapv rows order) (vec result)))
          (is (= rows (vec input))))))))

(deftest parallel-scan-sorts-own-their-lists
  (let [rows (mapv (fn [i] (object-array [i (- 5000 i) (str i)])) (range 5000))
        input (FastList. ^Collection rows)
        barrier (CyclicBarrier. 3)]
    (Collections/shuffle input (Random. 42))
    (let [original (vec input)
          workers (mapv (fn [sorter]
                          (future
                            (.await barrier 10 TimeUnit/SECONDS)
                            (sorter input)))
                        [#(scan/sort-tuples-by-eid % 0)
                         #(scan/sort-tuples-by-val % 1 :db.type/long)
                         #(scan/sort-tuples-by-val % 2 :db.type/string)])]
      (doseq [[column worker] (map-indexed vector workers)]
        (let [result (deref worker 10000 nil)
              expected (sort-by #(aget ^objects % (int column)) original)]
          (is (and (= (count rows) (count result))
                   (every? true? (map identical? expected result)))
              (str "sorted column " column))))
      (is (= original (vec input))))))
