(ns datalevin.byte-key-sort-test
  (:require [clojure.test :refer [deftest is use-fixtures]]
            [datalevin.conn :as conn]
            [datalevin.core :as d]
            [datalevin.test.core :refer [db-fixture]]
            [datalevin.util :as u])
  (:import [datalevin.utl ByteKeySort]
           [datalevin.tx_group Group]
           [java.util Arrays Comparator Random]
           [java.util.concurrent ConcurrentLinkedQueue]
           [java.util.concurrent.locks ReentrantLock]))

(use-fixtures :each db-fixture)

(deftest byte-key-sort-matches-stable-unsigned-comparison
  (let [random (Random. 42)]
    (dotimes [trial 50]
      (let [n (if (< trial 5) trial (.nextInt random 20000))
            ^objects keys (make-array (Class/forName "[B") (+ n 2))
            ^objects rows (object-array (concat (range n) [:tail :tail]))]
        (dotimes [i n]
          (let [^bytes key (byte-array (.nextInt random 64))]
            (.nextBytes random key)
            (when (zero? (mod trial 3))
              (Arrays/fill key 0 (int (min 8 (alength key))) (byte 0)))
            (aset keys i (if (and (pos? i) (zero? (mod i 7)))
                           (aget keys (dec i)) key))))
        (let [^objects original (aclone keys)
              ^objects expected (aclone rows)
              comparator (reify Comparator
                           (compare [_ a b]
                             (Arrays/compareUnsigned
                               ^bytes (aget original (int a))
                               ^bytes (aget original (int b)))))]
          (Arrays/sort expected 0 (int n) comparator)
          (ByteKeySort/sort rows keys (int n))
          (is (= (vec expected) (vec rows)) (str "trial " trial))
          (is (every? #(identical? (aget keys %)
                                  (aget original (int (aget rows %))))
                      (range n))))))))

(defn- queued-requests! [connection ops]
  (let [^Group group (#'conn/embedded-write-group connection)
        ^ReentrantLock lock (.-lock group)
        pending (atom [])]
    (.lock lock)
    (try
      (doseq [op ops]
        (swap! pending conj (future (try (op) (catch Throwable t t))))
        (let [deadline (+ (System/nanoTime) 10000000000)]
          (loop []
            (cond
              (= (count @pending) (.size ^ConcurrentLinkedQueue (.-queue group)))
              true
              (> (System/nanoTime) deadline)
              (is false "requests did not reach the owned queue")
              :else (do (Thread/sleep 1) (recur))))))
      (finally (.unlock lock)))
    (mapv #(deref % 10000 ::timeout) @pending)))

(deftest large-fused-sort-preserves-reservation-rollback
  (let [dir (u/tmp-dir (str "byte-sort-rollback-" (random-uuid)))
        connection (d/create-conn
                     dir {:key {:db/valueType :db.type/long
                                :db/unique :db.unique/value}
                          :value {:db/valueType :db.type/long}
                          :label {:db/valueType :db.type/string}}
                     {:wal? false :write-batch-size 8})]
    (try
      (d/transact! connection [{:key 4000 :value 0 :label "existing"}])
      (let [entities (conj (mapv #(hash-map :key % :value (mod % 7) :label "fresh")
                                (range 3000))
                           {:key 4000 :value 1 :label "collision"})
            [prefix failed suffix]
            (queued-requests!
              connection
              [#(d/transact! connection [{:key -1 :value 2 :label "prefix"}])
               #(d/transact! connection entities)
               #(d/transact! connection [{:key 0 :value 3 :label "suffix"}])])]
        (is (map? prefix))
        (is (instance? Throwable failed))
        (is (map? suffix))
        (is (= #{[-1 2 "prefix"] [0 3 "suffix"] [4000 0 "existing"]}
               (d/q '[:find ?k ?v ?l :where [?e :key ?k] [?e :value ?v]
                      [?e :label ?l]] @connection)))
        (is (= 3 (count (d/datoms @connection :ave :key)))))
      (finally
        (d/close connection)
        (u/delete-files dir)))))
