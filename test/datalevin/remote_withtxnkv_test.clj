(ns datalevin.remote-withtxnkv-test
  (:require
   [datalevin.core :as d]
   [datalevin.util :as u]
   [datalevin.test.core :as tdc :refer [server-fixture]]
   [clojure.test :as t :refer [is deftest testing use-fixtures]]))

(use-fixtures :each server-fixture)

(deftest with-transaction-kv-test
  (let [dir  "dtlv://datalevin:datalevin@localhost/remote-with-tx"
        lmdb (d/open-kv dir)]
    (d/open-dbi lmdb "a")

    (testing "new value is invisible to outside readers"
      (d/with-transaction-kv [db lmdb]
        (is (nil? (d/get-value db "a" 1 :data :data false)))
        (d/transact-kv db [[:put "a" 1 2]])
        (is (= [1 2] (d/get-value db "a" 1 :data :data false)))
        (is (nil? (d/get-value lmdb "a" 1 :data :data false))))
      (is (= [1 2] (d/get-value lmdb "a" 1 :data :data false))))

    (testing "abort"
      (d/with-transaction-kv [db lmdb]
        (d/transact-kv db [[:put "a" 1 3]])
        (is (= [1 3] (d/get-value db "a" 1 :data :data false)))
        (d/abort-transact-kv db))
      (is (= [1 2] (d/get-value lmdb "a" 1 :data :data false))))

    (d/close-kv lmdb)))

(deftest concurrent-with-transaction-kv-test
  (let [dir  "dtlv://datalevin:datalevin@localhost/remote-with-tx"
        lmdb (d/open-kv dir)]
    (d/open-dbi lmdb "a")

    (d/transact-kv lmdb [[:put "a" :counter 0]])

    (testing "concurrent writes from same client do not overwrite each other"
      (let [count-f
            #(d/with-transaction-kv [db lmdb]
               (let [^long now (d/get-value db "a" :counter)]
                 (d/transact-kv db [[:put "a" :counter (inc now)]])
                 (d/get-value db "a" :counter)))]
        (is (= (set [1 2 3])
               (set (pcalls count-f count-f count-f))))
        (is (= 3 (d/get-value lmdb "a" :counter)))))

    (testing "concurrent writes from diff clients do not overwrite each other"
      (let [count-f
            #(d/with-transaction-kv [db (d/open-kv
                                          dir {:client-opts {:pool-size 1}})]
               (let [^long now (d/get-value db "a" :counter)]
                 (d/transact-kv db [[:put "a" :counter (inc now)]])
                 (d/get-value db "a" :counter)))
            read-f (fn []
                     (Thread/sleep (long (rand-int 1000)))
                     (d/get-value lmdb "a" :counter))]
        (is (#{(set [4 5 6]) (set [3 4 5 6])}
              (set (pcalls count-f read-f  read-f
                           count-f count-f read-f))))
        (is (= 6 (d/get-value lmdb "a" :counter)))))

    (d/close-kv lmdb)))

(deftest with-txn-map-resize-test
  (let [dir  "dtlv://datalevin:datalevin@localhost/remote-with-tx"
        lmdb (d/open-kv dir {:mapsize 1 :kv-wal? false})
        data {:description "this is going to be bigger than 1MB"
              :numbers     (range 1000000)}]
    (d/open-dbi lmdb "a")

    (d/with-transaction-kv [db lmdb]
      (d/transact-kv db [[:put "a" 0 :prior]])
      (is (= :prior (d/get-value db "a" 0)))
      (d/transact-kv db [[:put "a" 1 data]])
      (is (= data (d/get-value db "a" 1))))

    (is (= :prior (d/get-value lmdb "a" 0)))
    (is (= data (d/get-value lmdb "a" 1)))

    (d/close-kv lmdb)))
