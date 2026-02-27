(ns datalevin.lmdb-test
  (:require
   [datalevin.lmdb :as l]
   [datalevin.bits :as b]
   [datalevin.interpret :as i]
   [datalevin.kv :as kv]
   [datalevin.interface :as if]
   [datalevin.txlog :as txlog]
   [datalevin.util :as u]
   [datalevin.core :as dc]
   [datalevin.constants :as c]
   [datalevin.datom :as d]
   [datalevin.test.core :as tdc :refer [db-fixture]]
   [clojure.java.io :as io]
   [clojure.string :as s]
   [clojure.test :refer [deftest testing is use-fixtures]]
   [clojure.test.check.generators :as gen]
   [clojure.test.check.clojure-test :as test]
   [clojure.test.check.properties :as prop])
  (:import
   [java.util UUID Arrays]
   [java.lang Long]
   [datalevin.db DB]
   [datalevin.storage Store]
   [datalevin.lmdb IListRandKeyValIterable IListRandKeyValIterator]))

(use-fixtures :each db-fixture)

(deftest basic-ops-test
  (let [dir  (u/tmp-dir (str "lmdb-test-" (UUID/randomUUID)))
        lmdb (l/open-kv dir {:spill-opts {:spill-threshold 50}})]

    (if/set-env-flags lmdb #{:nosync} true)
    (is (= (if/get-env-flags lmdb) (conj c/default-env-flags :nosync)))

    (is (= 50 (-> lmdb if/env-opts :spill-opts :spill-threshold)))

    (if/open-dbi lmdb "a")
    (is (not (if/list-dbi? lmdb "a")))

    (if/open-dbi lmdb "b")
    (if/open-dbi lmdb "c" {:key-size (inc Long/BYTES) :val-size (inc Long/BYTES)})
    (if/open-dbi lmdb "d")

    (testing "dbis"
      (is (= #{"a" "b" "c" "d"} (set (if/list-dbis lmdb))))
      (is (= (inc Long/BYTES) (:key-size (if/dbi-opts lmdb "c")))))

    (testing "transacting nil will throw"
      (is (thrown? Exception (if/transact-kv lmdb [[:put "a" nil 1]])))
      (is (thrown? Exception (if/transact-kv lmdb [[:put "a" 1 nil]]))))

    (testing "transact-kv"
      (if/transact-kv lmdb
        [[:put "a" 1 2]
         [:put "a" 'a 1]
         [:put "a" 5 {}]
         [:put "a" :annunaki/enki true :attr :data]
         [:put "a" :datalevin ["hello" "world"]]
         [:put "a" 42 (d/datom 1 :a/b {:id 4}) :long :datom]
         [:put "b" 2 3]
         [:put "b" (byte 0x01) #{1 2} :byte :data]
         [:put "b" (byte-array [0x41 0x42]) :bk :bytes :data]
         [:put "b" [-1 -235254457N] 5]
         [:put "b" :a 4]
         [:put "b" :bv (byte-array [0x41 0x42 0x43]) :data :bytes]
         [:put "b" 1 :long :long :data]
         [:put "b" :long 1 :data :long]
         [:put "b" 2 3 :long :long]
         [:put "b" "ok" 42 :string :long]
         [:put "d" 3.14 :pi :double :keyword]
         [:put "d" #inst "1969-01-01" "nice year" :instant :string]
         [:put "d" [-1 0 1 2 3 4] 1 [:long]]
         [:put "d" [:a :b :c :d] [1 2 3] [:keyword] [:long]]
         [:put "d" [-1 "heterogeneous" :datalevin/tuple] 2
          [:long :string :keyword]]
         [:put "d"  [:ok -0.687 "nice"] [2 4]
          [:keyword :double :string] [:long]]]))

    (if/sync lmdb)

    (testing "entries"
      (is (= 5 (:entries (if/stat lmdb))))
      (is (= 6 (:entries (if/stat lmdb "a"))))
      (is (= 6 (if/entries lmdb "a")))
      (is (= 10 (if/entries lmdb "b"))))

    (testing "get-value"
      (is (= 1 (if/get-value lmdb "d" [-1 0 1 2 3 4] [:long])))
      (is (= [1 2 3] (if/get-value lmdb "d" [:a :b :c :d] [:keyword] [:long])))
      (is (= 2 (if/get-value lmdb "d" [-1 "heterogeneous" :datalevin/tuple]
                             [:long :string :keyword])))
      (is (= [2 4] (if/get-value lmdb "d" [:ok -0.687 "nice"]
                                 [:keyword :double :string] [:long])))
      (is (= 2 (if/get-value lmdb "a" 1)))
      (is (= [1 2] (if/get-value lmdb "a" 1 :data :data false)))
      (is (= true (if/get-value lmdb "a" :annunaki/enki :attr :data)))
      (is (= (d/datom 1 :a/b {:id 4}) (if/get-value lmdb "a" 42 :long :datom)))
      (is (nil? (if/get-value lmdb "a" 2)))
      (is (nil? (if/get-value lmdb "b" 1)))
      (is (= 5 (if/get-value lmdb "b" [-1 -235254457N])))
      (is (= 1 (if/get-value lmdb "a" 'a)))
      (is (= {} (if/get-value lmdb "a" 5)))
      (is (= ["hello" "world"] (if/get-value lmdb "a" :datalevin)))
      (is (= 3 (if/get-value lmdb "b" 2)))
      (is (= 4 (if/get-value lmdb "b" :a)))
      (is (= #{1 2} (if/get-value lmdb "b" (byte 0x01) :byte)))
      (is (= :bk (if/get-value lmdb "b" (byte-array [0x41 0x42]) :bytes)))
      (is (Arrays/equals ^bytes (byte-array [0x41 0x42 0x43])
                         ^bytes (if/get-value lmdb "b" :bv :data :bytes)))
      (is (= :long (if/get-value lmdb "b" 1 :long :data)))
      (is (= 1 (if/get-value lmdb "b" :long :data :long)))
      (is (= 3 (if/get-value lmdb "b" 2 :long :long)))
      (is (= 42 (if/get-value lmdb "b" "ok" :string :long)))
      (is (= :pi (if/get-value lmdb "d" 3.14 :double :keyword)))
      (is (= "nice year"
             (if/get-value lmdb "d" #inst "1969-01-01" :instant :string))))

    (testing "get-rank"
      (if/open-dbi lmdb "rank-test")
      (if/transact-kv lmdb [[:put "rank-test" 10 "ten" :long :string]
                            [:put "rank-test" 20 "twenty" :long :string]
                            [:put "rank-test" 30 "thirty" :long :string]
                            [:put "rank-test" 40 "forty" :long :string]
                            [:put "rank-test" 50 "fifty" :long :string]])
      (is (= 0 (if/get-rank lmdb "rank-test" 10 :long)))
      (is (= 1 (if/get-rank lmdb "rank-test" 20 :long)))
      (is (= 2 (if/get-rank lmdb "rank-test" 30 :long)))
      (is (= 3 (if/get-rank lmdb "rank-test" 40 :long)))
      (is (= 4 (if/get-rank lmdb "rank-test" 50 :long)))
      (is (nil? (if/get-rank lmdb "rank-test" 99 :long))))

    (testing "get-by-rank"
      (is (= "ten" (if/get-by-rank lmdb "rank-test" 0 :long :string)))
      (is (= "twenty" (if/get-by-rank lmdb "rank-test" 1 :long :string)))
      (is (= "thirty" (if/get-by-rank lmdb "rank-test" 2 :long :string)))
      (is (= "forty" (if/get-by-rank lmdb "rank-test" 3 :long :string)))
      (is (= "fifty" (if/get-by-rank lmdb "rank-test" 4 :long :string)))
      (is (= [10 "ten"] (if/get-by-rank lmdb "rank-test" 0 :long :string false)))
      (is (= [50 "fifty"] (if/get-by-rank lmdb "rank-test" 4 :long :string false)))
      (is (nil? (if/get-by-rank lmdb "rank-test" 99 :long :string))))

    (testing "sample-kv"
      (let [samples (if/sample-kv lmdb "rank-test" 3 :long :string)]
        (is (= 3 (count samples)))
        (is (every? #{"ten" "twenty" "thirty" "forty" "fifty"} samples)))
      (let [samples (if/sample-kv lmdb "rank-test" 3 :long :string false)]
        (is (= 3 (count samples)))
        (is (every? (fn [[k v]]
                      (and (#{10 20 30 40 50} k)
                           (#{"ten" "twenty" "thirty" "forty" "fifty"} v)))
                    samples)))
      (let [all-samples (if/sample-kv lmdb "rank-test" 5 :long :string)]
        (is (= 5 (count all-samples)))
        (is (= #{"ten" "twenty" "thirty" "forty" "fifty"} (set all-samples))))
      (is (nil? (if/sample-kv lmdb "rank-test" 10 :long :string))))

    (testing "get-first and get-first-n"
      (is (= [1 2] (if/get-first lmdb "a" [:closed 1 10] :data)))
      (is (= [[1 2] [5 {}]] (if/get-first-n lmdb "a" 2 [:closed 1 10] :data)))
      (is (= [[1 2] [5 {}]] (if/get-first-n lmdb "a" 3 [:closed 1 10] :data))))

    (testing "delete"
      (if/transact-kv lmdb [[:del "a" 1]
                            [:del "a" :non-exist]
                            [:del "a" "random things that do not exist"]])
      (is (nil? (if/get-value lmdb "a" 1))))

    (testing "entries-again"
      (is (= 5 (if/entries lmdb "a")))
      (is (= 10 (if/entries lmdb "b"))))

    (testing "non-existent dbi"
      (is (thrown? Exception (if/get-value lmdb "z" 1))))

    (testing "handle val overflow automatically"
      (if/transact-kv lmdb [[:put "c" 1 (range 100000)]])
      (is (= (range 100000) (if/get-value lmdb "c" 1))))

    (testing "key overflow throws"
      (is (thrown? Exception
                   (if/transact-kv lmdb [[:put "a" (range 1000) 1]]))))

    (testing "close then re-open, clear and drop"
      (let [dir (if/env-dir lmdb)]
        (if/close-kv lmdb)
        (is (if/closed-kv? lmdb))
        (let [lmdb  (l/open-kv dir
                               {:flags (conj c/default-env-flags :nosync)})
              dbi-a (if/open-dbi lmdb "a")]
          (is (= "a" (l/dbi-name dbi-a)))
          (is (= ["hello" "world"] (if/get-value lmdb "a" :datalevin)))
          (if/clear-dbi lmdb "a")
          (is (nil? (if/get-value lmdb "a" :datalevin)))
          (if/drop-dbi lmdb "a")
          (is (thrown? Exception (if/get-value lmdb "a" 1)))
          (if/close-kv lmdb))))
    (u/delete-files dir)))

(deftest async-basic-ops-test
  (let [dir  (u/tmp-dir (str "async-lmdb-test-" (UUID/randomUUID)))
        lmdb (l/open-kv dir {:spill-opts {:spill-threshold 50}})]

    (if/open-dbi lmdb "a")
    (if/open-dbi lmdb "b")
    (if/open-dbi lmdb "c" {:key-size (inc Long/BYTES) :val-size (inc Long/BYTES)})
    (if/open-dbi lmdb "d")

    (testing "transacting nil will throw"
      (is (thrown? Exception @(dc/transact-kv-async lmdb [[:put "a" nil 1]])))
      (is (thrown? Exception @(dc/transact-kv-async lmdb [[:put "a" 1 nil]]))))

    (testing "transact-kv-async"
      @(dc/transact-kv-async lmdb
                             [[:put "a" 1 2]
                              [:put "a" 'a 1]
                              [:put "a" 5 {}]
                              [:put "a" :annunaki/enki true :attr :data]
                              [:put "a" :datalevin ["hello" "world"]]
                              [:put "a" 42 (d/datom 1 :a/b {:id 4}) :long :datom]
                              [:put "b" 2 3]
                              [:put "b" (byte 0x01) #{1 2} :byte :data]
                              [:put "b" (byte-array [0x41 0x42]) :bk :bytes :data]
                              [:put "b" [-1 -235254457N] 5]
                              [:put "b" :a 4]
                              [:put "b" :bv (byte-array [0x41 0x42 0x43]) :data :bytes]
                              [:put "b" 1 :long :long :data]
                              [:put "b" :long 1 :data :long]
                              [:put "b" 2 3 :long :long]
                              [:put "b" "ok" 42 :string :long]
                              [:put "d" 3.14 :pi :double :keyword]
                              [:put "d" #inst "1969-01-01" "nice year" :instant :string]
                              [:put "d" [-1 0 1 2 3 4] 1 [:long]]
                              [:put "d" [:a :b :c :d] [1 2 3] [:keyword] [:long]]
                              [:put "d" [-1 "heterogeneous" :datalevin/tuple] 2
                               [:long :string :keyword]]
                              [:put "d"  [:ok -0.687 "nice"] [2 4]
                               [:keyword :double :string] [:long]]]))

    (testing "entries"
      (is (= 5 (:entries (if/stat lmdb))))
      (is (= 6 (:entries (if/stat lmdb "a"))))
      (is (= 6 (if/entries lmdb "a")))
      (is (= 10 (if/entries lmdb "b"))))

    (testing "get-value"
      (is (= 1 (if/get-value lmdb "d" [-1 0 1 2 3 4] [:long])))
      (is (= [1 2 3] (if/get-value lmdb "d" [:a :b :c :d] [:keyword] [:long])))
      (is (= 2 (if/get-value lmdb "d" [-1 "heterogeneous" :datalevin/tuple]
                             [:long :string :keyword])))
      (is (= [2 4] (if/get-value lmdb "d" [:ok -0.687 "nice"]
                                 [:keyword :double :string] [:long])))
      (is (= 2 (if/get-value lmdb "a" 1)))
      (is (= [1 2] (if/get-value lmdb "a" 1 :data :data false)))
      (is (= true (if/get-value lmdb "a" :annunaki/enki :attr :data)))
      (is (= (d/datom 1 :a/b {:id 4}) (if/get-value lmdb "a" 42 :long :datom)))
      (is (nil? (if/get-value lmdb "a" 2)))
      (is (nil? (if/get-value lmdb "b" 1)))
      (is (= 5 (if/get-value lmdb "b" [-1 -235254457N])))
      (is (= 1 (if/get-value lmdb "a" 'a)))
      (is (= {} (if/get-value lmdb "a" 5)))
      (is (= ["hello" "world"] (if/get-value lmdb "a" :datalevin)))
      (is (= 3 (if/get-value lmdb "b" 2)))
      (is (= 4 (if/get-value lmdb "b" :a)))
      (is (= #{1 2} (if/get-value lmdb "b" (byte 0x01) :byte)))
      (is (= :bk (if/get-value lmdb "b" (byte-array [0x41 0x42]) :bytes)))
      (is (Arrays/equals ^bytes (byte-array [0x41 0x42 0x43])
                         ^bytes (if/get-value lmdb "b" :bv :data :bytes)))
      (is (= :long (if/get-value lmdb "b" 1 :long :data)))
      (is (= 1 (if/get-value lmdb "b" :long :data :long)))
      (is (= 3 (if/get-value lmdb "b" 2 :long :long)))
      (is (= 42 (if/get-value lmdb "b" "ok" :string :long)))
      (is (= :pi (if/get-value lmdb "d" 3.14 :double :keyword)))
      (is (= "nice year"
             (if/get-value lmdb "d" #inst "1969-01-01" :instant :string))))

    (testing "get-first and get-first-n"
      (is (= [1 2] (if/get-first lmdb "a" [:closed 1 10] :data)))
      (is (= [[1 2] [5 {}]] (if/get-first-n lmdb "a" 2 [:closed 1 10] :data)))
      (is (= [[1 2] [5 {}]] (if/get-first-n lmdb "a" 3 [:closed 1 10] :data))))

    (testing "delete"
      @(dc/transact-kv-async lmdb [[:del "a" 1]
                                   [:del "a" :non-exist]
                                   [:del "a" "random things that do not exist"]])
      (is (nil? (if/get-value lmdb "a" 1))))

    (testing "entries-again"
      (is (= 5 (if/entries lmdb "a")))
      (is (= 10 (if/entries lmdb "b"))))

    (testing "non-existent dbi"
      (is (thrown? Exception (if/get-value lmdb "z" 1))))

    (testing "handle val overflow automatically"
      @(dc/transact-kv-async lmdb [[:put "c" 1 (range 100000)]])
      (is (= (range 100000) (if/get-value lmdb "c" 1))))

    (testing "key overflow throws"
      (is (thrown? Exception
                   @(dc/transact-kv-async lmdb [[:put "a" (range 1000) 1]]))))

    (when-not (u/windows?) (u/delete-files dir))))

(deftest transact-arity-test
  (let [dir  (u/tmp-dir (str "lmdb-tx-arity-test-" (UUID/randomUUID)))
        lmdb (l/open-kv dir {:flags (conj c/default-env-flags :nosync)})]
    (if/open-dbi lmdb "a")
    (if/transact-kv lmdb "a" [[:put :a 1]
                              [:put :b 2]])
    (is (= 1 (if/get-value lmdb "a" :a)))
    (is (= 2 (if/get-value lmdb "a" :b)))

    (if/open-dbi lmdb "b")
    (if/transact-kv lmdb "b" [[:put "b" 1]
                              [:put "a" 2]] :string)
    (is (= [["a" 2] ["b" 1]]
           (if/get-range lmdb "b" [:all] :string)))
    (if/close-kv lmdb)
    (u/delete-files dir)))

(deftest reentry-test
  (let [dir  (u/tmp-dir (str "lmdb-test-" (UUID/randomUUID)))
        lmdb (l/open-kv dir {:flags (conj c/default-env-flags :nosync)})]
    (if/open-dbi lmdb "a")
    (if/transact-kv lmdb [[:put "a" :old 1]])
    (is (= 1 (if/get-value lmdb "a" :old)))
    (let [res (future
                (let [lmdb2 (l/open-kv
                              dir
                              {:flags (conj c/default-env-flags :nosync)})]
                  (if/open-dbi lmdb2 "a")
                  (is (= 1 (if/get-value lmdb2 "a" :old)))
                  (if/transact-kv lmdb2 [[:put "a" :something 1]])
                  (is (= 1 (if/get-value lmdb2 "a" :something)))
                  (is (= 1 (if/get-value lmdb "a" :something)))
                  ;; should not close this
                  ;; https://github.com/juji-io/datalevin/issues/7
                  (if/close-kv lmdb2)
                  1))]
      (is (= 1 @res)))
    (is (thrown-with-msg? Exception #"multiple LMDB"
                          (if/get-value lmdb "a" :something)))
    (if/close-kv lmdb)
    (u/delete-files dir)))

(deftest multi-threads-get-value-test
  (let [dir  (u/tmp-dir (str "lmdb-test-" (UUID/randomUUID)))
        lmdb (l/open-kv dir {:flags (conj c/default-env-flags :nosync)})]
    (if/open-dbi lmdb "a")
    (let [ks  (shuffle (range 0 100000))
          vs  (map inc ks)
          txs (map (fn [k v] [:put "a" k v :long :long]) ks vs)]
      (if/transact-kv lmdb txs)
      (is (= 100000 (if/entries lmdb "a")))
      (is (= vs (pmap #(if/get-value lmdb "a" % :long :long) ks))))
    (if/close-kv lmdb)
    (u/delete-files dir)))

(deftest multi-threads-put-test
  (let [dir  (u/tmp-dir (str "lmdb-test-" (UUID/randomUUID)))
        lmdb (l/open-kv dir {:flags (conj c/default-env-flags :nosync)})]
    (if/open-dbi lmdb "a")
    (let [ks  (shuffle (range 0 10000))
          vs  (map inc ks)
          txs (map (fn [k v] [:put "a" k v :long :long]) ks vs)]
      (dorun (pmap #(if/transact-kv lmdb [%]) txs))
      (is (= 10000 (if/entries lmdb "a")))
      (is (= vs (map #(if/get-value lmdb "a" % :long :long) ks))))
    (if/close-kv lmdb)
    (u/delete-files dir)))

;; generative tests

(test/defspec datom-ops-generative-test
  100
  (prop/for-all
    [k gen/large-integer
     e gen/large-integer
     a gen/keyword-ns
     v gen/any-equatable]
    (let [dir    (u/tmp-dir (str "lmdb-test-" (UUID/randomUUID)))
          lmdb   (l/open-kv dir {:flags (conj c/default-env-flags :nosync)})
          _      (if/open-dbi lmdb "a")
          d      (d/datom e a v e)
          _      (if/transact-kv lmdb [[:put "a" k d :long :datom]])
          put-ok (= d (if/get-value lmdb "a" k :long :datom))
          _      (if/transact-kv lmdb [[:del "a" k :long]])
          del-ok (nil? (if/get-value lmdb "a" k :long))]
      (if/close-kv lmdb)
      (u/delete-files dir)
      (is (and put-ok del-ok)))))

(defn- data-size-less-than?
  [^long limit data]
  (< (alength ^bytes (b/serialize data)) limit))

(test/defspec data-ops-generative-test
  100
  (prop/for-all
    [k (gen/such-that (partial data-size-less-than? c/+max-key-size+)
                      gen/any-equatable)
     v (gen/such-that (partial data-size-less-than? c/*init-val-size*)
                      gen/any-equatable)]
    (let [dir    (u/tmp-dir (str "data-test-" (UUID/randomUUID)))
          lmdb   (l/open-kv dir {:flags (conj c/default-env-flags :nosync)})
          _      (if/open-dbi lmdb "a")
          _      (if/transact-kv lmdb [[:put "a" k v]])
          put-ok (= v (if/get-value lmdb "a" k))
          _      (if/transact-kv lmdb [[:del "a" k]])
          del-ok (nil? (if/get-value lmdb "a" k))]
      (if/close-kv lmdb)
      (u/delete-files dir)
      (is (and put-ok del-ok)))))

(test/defspec bytes-ops-generative-test
  100
  (prop/for-all
    [^bytes k (gen/not-empty gen/bytes)
     ^bytes v (gen/not-empty gen/bytes)]
    (let [dir    (u/tmp-dir (str "lmdb-test-" (UUID/randomUUID)))
          lmdb   (l/open-kv dir {:flags (conj c/default-env-flags :nosync)})
          _      (if/open-dbi lmdb "a")
          _      (if/transact-kv lmdb [[:put "a" k v :bytes :bytes]])
          put-ok (Arrays/equals v
                                ^bytes
                                (if/get-value
                                    lmdb "a" k :bytes :bytes))
          _      (if/transact-kv lmdb [[:del "a" k :bytes]])
          del-ok (nil? (if/get-value lmdb "a" k :bytes))]
      (if/close-kv lmdb)
      (u/delete-files dir)
      (is (and put-ok del-ok)))))

(test/defspec long-ops-generative-test
  100
  (prop/for-all [^long k gen/large-integer
                 ^long v gen/large-integer]
                (let [dir    (u/tmp-dir (str "lmdb-test-" (UUID/randomUUID)))
                      lmdb   (l/open-kv
                               dir
                               {:flags (conj c/default-env-flags :nosync)})
                      _      (if/open-dbi lmdb "a")
                      _      (if/transact-kv lmdb [[:put "a" k v :long :long]])
                      put-ok (= v ^long (if/get-value lmdb "a" k :long :long))
                      _      (if/transact-kv lmdb [[:del "a" k :long]])
                      del-ok (nil? (if/get-value lmdb "a" k)) ]
                  (if/close-kv lmdb)
                  (u/delete-files dir)
                  (is (and put-ok del-ok)))))

(deftest list-basic-ops-test
  (let [dir     (u/tmp-dir (str "list-test-" (UUID/randomUUID)))
        lmdb    (l/open-kv dir {:flags (conj c/default-env-flags :nosync)})
        sum     (volatile! 0)
        visitor (i/inter-fn
                  [vb]
                  (let [^long v (b/read-buffer vb :long)]
                    (vswap! sum #(+ ^long %1 ^long %2) v)))
        joins   (volatile! "")
        kvisit  (i/inter-fn
                  [bf]
                  (let [k (b/read-buffer bf :string)]
                    (vswap! joins #(s/join " " [% k]))))
        values  (volatile! [])
        op-gen  (i/inter-fn
                  [k kt]
                  (i/inter-fn
                    [^IListRandKeyValIterable iterable]
                    (let [^IListRandKeyValIterator iter
                          (l/val-iterator iterable)]
                      (loop [next? (l/seek-key iter k kt)]
                        (when next?
                          (vswap! values conj
                                  (b/read-buffer (l/next-val iter) :long))
                          (recur (l/has-next-val iter)))))))]
    (if/open-list-dbi lmdb "list" #_{:flags #{:create :counted :dupsort}})
    (is (if/list-dbi? lmdb "list"))

    (if/put-list-items lmdb "list" "a" [1 2 3 4] :string :long)
    (if/put-list-items lmdb "list" "b" [5 6 7] :string :long)
    (if/put-list-items lmdb "list" "c" [3 6 9] :string :long)

    (is (= (if/entries lmdb "list") 10))

    (is (= (if/key-range-count lmdb "list" [:all]) 3))

    (if/visit-key-range lmdb "list" kvisit [:all] :string)
    (is (= "a b c" (s/trim @joins)))

    (is (= (if/key-range-list-count lmdb "list" [:all] :string) 10))
    (is (= (if/key-range-list-count lmdb "list" [:greater-than "a"] :string)
           6))
    (is (= (if/key-range-list-count lmdb "list" [:closed "A" "d"] :string) 10))
    (is (= (if/key-range-list-count lmdb "list" [:closed "a" "e"] :string) 10))
    (is (= (if/key-range-list-count lmdb "list" [:less-than "c"] :string)
           7))

    (is (= (if/key-range lmdb "list" [:all] :string) ["a" "b" "c"]))
    (is (= (if/key-range-count lmdb "list" [:greater-than "b"] :string) 1))
    (is (= (if/key-range lmdb "list" [:less-than "b"] :string) ["a"]))

    (is (= [["a" 1] ["a" 2] ["a" 3] ["a" 4] ["b" 5] ["b" 6] ["b" 7]
            ["c" 3] ["c" 6] ["c" 9]]
           (if/get-range lmdb "list" [:all] :string :long)))
    (is (= [["a" 1] ["a" 2] ["a" 3] ["a" 4] ["b" 5] ["b" 6] ["b" 7]]
           (if/get-range lmdb "list" [:closed "a" "b"] :string :long)))
    (is (= [["b" 5] ["b" 6] ["b" 7]]
           (if/get-range lmdb "list" [:closed "b" "b"] :string :long)))
    (is (= [["b" 5] ["b" 6] ["b" 7]]
           (if/get-range lmdb "list" [:open-closed "a" "b"] :string :long)))
    (is (= [["c" 3] ["c" 6] ["c" 9] ["b" 5] ["b" 6] ["b" 7]
            ["a" 1] ["a" 2] ["a" 3] ["a" 4]]
           (if/get-range lmdb "list" [:all-back] :string :long)))

    (is (= ["a" 1]
           (if/get-first lmdb "list" [:closed "a" "a"] :string :long)))
    (is (= [["a" 1] ["a" 2]]
           (if/get-first-n lmdb "list" 2 [:closed "a" "c"] :string :long)))
    (is (= [["a" 1] ["a" 2]]
           (if/list-range-first-n lmdb "list" 2 [:closed "a" "c"] :string
                                  [:closed 1 5] :long)))

    (is (= [3 6 9]
           (if/get-list lmdb "list" "c" :string :long)))

    (is (= [["a" 1] ["a" 2] ["a" 3] ["a" 4] ["b" 5] ["b" 6] ["b" 7]
            ["c" 3] ["c" 6] ["c" 9]]
           (if/list-range lmdb "list" [:all] :string [:all] :long)))
    (is (= [["a" 2] ["a" 3] ["a" 4] ["c" 3]]
           (if/list-range lmdb "list" [:closed "a" "c"] :string
                          [:closed 2 4] :long)))
    (is (= [["c" 9] ["c" 6] ["c" 3] ["b" 7] ["b" 6] ["b" 5]
            ["a" 4] ["a" 3] ["a" 2] ["a" 1]]
           (if/list-range lmdb "list" [:all-back] :string [:all-back] :long)))
    (is (= [["c" 3]]
           (if/list-range lmdb "list" [:at-least "b"] :string
                          [:at-most-back 4] :long)))

    (is (= [["b" 5]]
           (if/list-range lmdb "list" [:open "a" "c"] :string
                          [:less-than 6] :long)))

    (is (= (if/list-count lmdb "list" "a" :string) 4))
    (is (= (if/list-count lmdb "list" "b" :string) 3))

    (is (not (if/in-list? lmdb "list" "a" 7 :string :long)))
    (is (if/in-list? lmdb "list" "b" 7 :string :long))

    (is (nil? (if/near-list lmdb "list" "a" 7 :string :long)))
    (is (= 5 (b/read-buffer
               (if/near-list lmdb "list" "b" 4 :string :long) :long)))

    (is (= (if/get-list lmdb "list" "a" :string :long) [1 2 3 4]))
    (is (= (if/get-list lmdb "list" "a" :string :long) [1 2 3 4]))

    (if/visit-list lmdb "list" visitor "a" :string)
    (is (= @sum 10))

    (if/del-list-items lmdb "list" "a" :string)

    (is (= (if/list-count lmdb "list" "a" :string) 0))
    (is (not (if/in-list? lmdb "list" "a" 1 :string :long)))
    (is (empty? (if/get-list lmdb "list" "a" :string :long)))

    (if/put-list-items lmdb "list" "b" [1 2 3 4] :string :long)

    (is (= [1 2 3 4 5 6 7]
           (if/get-list lmdb "list" "b" :string :long)))
    (is (= (if/list-count lmdb "list" "b" :string) 7))
    (is (if/in-list? lmdb "list" "b" 1 :string :long))

    (if/del-list-items lmdb "list" "b" [1 2] :string :long)

    (is (= (if/list-count lmdb "list" "b" :string) 5))
    (is (not (if/in-list? lmdb "list" "b" 1 :string :long)))
    (is (= [3 4 5 6 7]
           (if/get-list lmdb "list" "b" :string :long)))

    (if/close-kv lmdb)
    (u/delete-files dir)))

(deftest list-string-test
  (let [dir   (u/tmp-dir (str "string-list-test-" (UUID/randomUUID)))
        lmdb  (l/open-kv dir {:flags (conj c/default-env-flags :nosync)})
        pred  (i/inter-fn [kv]
                (let [^String v (b/read-buffer (l/v kv) :string)]
                  (< (count v) 5)))
        pred1 (i/inter-fn [kv]
                (let [^String v (b/read-buffer (l/v kv) :string)]
                  (when (< (count v) 5) v)))]
    (if/open-list-dbi lmdb "str")
    (is (if/list-dbi? lmdb "str"))

    (if/put-list-items lmdb "str" "a" ["abc" "hi" "defg" ] :string :string)
    (if/put-list-items lmdb "str" "b" ["hello" "world" "nice"] :string :string)

    (is (= [["a" "abc"] ["a" "defg"] ["a" "hi"]
            ["b" "hello"] ["b" "nice"] ["b" "world"]]
           (if/get-range lmdb "str" [:all] :string :string)))
    (is (= [["a" "abc"] ["a" "defg"] ["a" "hi"]
            ["b" "hello"] ["b" "nice"] ["b" "world"]]
           (if/get-range lmdb "str" [:closed "a" "b"] :string :string)))
    (is (= [["b" "hello"] ["b" "nice"] ["b" "world"]]
           (if/get-range lmdb "str" [:closed "b" "b"] :string :string)))
    (is (= [["b" "hello"] ["b" "nice"] ["b" "world"]]
           (if/get-range lmdb "str" [:open-closed "a" "b"] :string :string)))

    (is (= [["b" "nice"]]
           (if/list-range-filter lmdb "str" pred [:greater-than "a"] :string
                                 [:all] :string)))

    (is (= ["nice"]
           (if/list-range-keep lmdb "str" pred1 [:greater-than "a"] :string
                               [:all] :string)))
    (is (= "nice"
           (if/list-range-some lmdb "str" pred1 [:greater-than "a"] :string
                               [:all] :string)))

    (is (= ["a" "abc"]
           (if/get-first lmdb "str" [:closed "a" "a"] :string :string)))

    (is (= (if/list-count lmdb "str" "a" :string) 3))
    (is (= (if/list-count lmdb "str" "b" :string) 3))

    (is (not (if/in-list? lmdb "str" "a" "hello" :string :string)))
    (is (if/in-list? lmdb "str" "b" "hello" :string :string))

    (is (= (if/get-list lmdb "str" "a" :string :string)
           ["abc" "defg" "hi"]))

    (if/del-list-items lmdb "str" "a" :string)

    (is (= (if/list-count lmdb "str" "a" :string) 0))
    (is (not (if/in-list? lmdb "str" "a" "hi" :string :string)))
    (is (empty? (if/get-list lmdb "str" "a" :string :string)))

    (if/put-list-items lmdb "str" "b" ["good" "peace"] :string :string)

    (is (= (if/list-count lmdb "str" "b" :string) 5))
    (is (if/in-list? lmdb "str" "b" "good" :string :string))

    (if/del-list-items lmdb "str" "b" ["hello" "world"] :string :string)

    (is (= (if/list-count lmdb "str" "b" :string) 3))
    (is (not (if/in-list? lmdb "str" "b" "world" :string :string)))

    (if/close-kv lmdb)
    (u/delete-files dir)))

(deftest validate-data-test
  (let [dir  (u/tmp-dir (str "valid-data-" (UUID/randomUUID)))
        lmdb (l/open-kv dir {:flags (conj c/default-env-flags :nosync)})]
    (if/open-dbi lmdb "a" {:validate-data? true})
    (is (if/transact-kv lmdb [[:put "a" (byte-array [1 3]) 2 :bytes :id]]))
    (is (if/transact-kv lmdb [[:put "a" 1 -0.1 :id :double]]))
    (is (thrown-with-msg? Exception #"Invalid data"
                          (if/transact-kv lmdb [[:put "a" "a" 2 :bytes]])))
    (is (thrown-with-msg? Exception #"Invalid data"
                          (if/transact-kv lmdb [[:put "a" 1 2 :string]])))
    (is (thrown-with-msg? Exception #"Invalid data"
                          (if/transact-kv lmdb [[:put "a" 1 "b" :long :long]])))
    (is (thrown-with-msg? Exception #"Invalid data"
                          (if/transact-kv lmdb [[:put "a" 1 1 :float]])))
    (is (thrown-with-msg? Exception #"Invalid data"
                          (if/transact-kv lmdb [[:put "a" 1000 1 :byte]])))
    (is (thrown-with-msg? Exception #"Invalid data"
                          (if/transact-kv lmdb [[:put "a" 1 1 :bytes]])))
    (is (thrown-with-msg? Exception #"Invalid data"
                          (if/transact-kv lmdb [[:put "a" "b" 1 :keyword]])))
    (is (thrown-with-msg? Exception #"Invalid data"
                          (if/transact-kv lmdb [[:put "a" "b" 1 :symbol]])))
    (is (thrown-with-msg? Exception #"Invalid data"
                          (if/transact-kv lmdb [[:put "a" "b" 1 :boolean]])))
    (is (thrown-with-msg? Exception #"Invalid data"
                          (if/transact-kv lmdb [[:put "a" "b" 1 :instant]])))
    (is (thrown-with-msg? Exception #"Invalid data"
                          (if/transact-kv lmdb [[:put "a" "b" 1 :uuid]])))
    (if/close-kv lmdb)
    (u/delete-files dir)))

(deftest read-during-transaction-test
  (let [dir   (u/tmp-dir (str "lmdb-ctx-test-" (UUID/randomUUID)))
        lmdb  (l/open-kv dir {:flags (conj c/default-env-flags :nosync)})
        lmdb1 (l/mark-write lmdb)]
    (if/open-dbi lmdb "a")
    (if/open-dbi lmdb "d")

    (if/open-transact-kv lmdb)

    (testing "get-value"
      (is (nil? (if/get-value lmdb1 "a" 1 :data :data false)))
      (if/transact-kv lmdb
        [[:put "a" 1 2]
         [:put "a" 'a 1]
         [:put "a" 5 {}]
         [:put "a" :annunaki/enki true :attr :data]
         [:put "a" :datalevin ["hello" "world"]]
         [:put "a" 42 (d/datom 1 :a/b {:id 4}) :long :datom]])

      (is (= [1 2] (if/get-value lmdb1 "a" 1 :data :data false)))
      ;; non-writing txn will still read pre-transaction values
      (is (nil? (if/get-value lmdb "a" 1 :data :data false)))

      (is (nil? (if/get-value lmdb1 "d" #inst "1969-01-01" :instant :string
                              true)))
      (if/transact-kv lmdb
        [[:put "d" 3.14 :pi :double :keyword]
         [:put "d" #inst "1969-01-01" "nice year" :instant :string]])
      (is (= "nice year"
             (if/get-value lmdb1 "d" #inst "1969-01-01" :instant :string
                           true)))
      (is (nil? (if/get-value lmdb "d" #inst "1969-01-01" :instant :string
                              true))))

    (if/close-transact-kv lmdb)

    (testing "entries after transaction"
      (is (= 6 (if/entries lmdb "a")))
      (is (= 2 (if/entries lmdb "d"))))

    (if/close-kv lmdb)
    (u/delete-files dir)))

(deftest with-txn-map-resize-test
  (let [dir  (u/tmp-dir (str "map-size-" (UUID/randomUUID)))
        lmdb (l/open-kv dir {:mapsize 1
                             :flags   (conj c/default-env-flags :nosync)})
        data {:description "this is going to be bigger than 10 MB"
              :numbers     (range 10000000)}]
    (if/open-dbi lmdb "a")

    (l/with-transaction-kv [db lmdb]
      (if/transact-kv db [[:put "a" 0 :prior]])
      (is (= :prior (if/get-value db "a" 0)))
      (if/transact-kv db [[:put "a" 1 data]])
      (is (= data (if/get-value db "a" 1))))

    (is (= :prior (if/get-value lmdb "a" 0)))
    (is (= data (if/get-value lmdb "a" 1)))

    (if/close-kv lmdb)
    (u/delete-files dir)))

(defn- read-txlog-records
  [dir]
  (let [txlog-dir (str dir u/+separator+ "txlog")]
    (->> (txlog/segment-files txlog-dir)
         (mapcat (fn [{:keys [id file]}]
                   (let [{:keys [records]}
                         (txlog/scan-segment (.getPath file))]
                     (mapv (fn [record]
                             (let [{:keys [lsn ops]}
                                   (txlog/decode-commit-row-payload
                                    ^bytes (:body record))]
                               {:lsn lsn
                                :ops ops
                                :segment-id id
                                :offset (:offset record)
                                :checksum (:checksum record)}))
                           records))))
         vec)))

(defn- list-put-record
  [records vec-val]
  (first
   (filter
    (fn [{:keys [ops]}]
      (some (fn [row]
              (and (vector? row)
                   (= :put-list (nth row 0 nil))
                   (= "list" (nth row 1 nil))
                   (= :k (nth row 2 nil))
                   (= [vec-val] (nth row 3 nil))))
            ops))
    records)))

(defn- wait-until
  [pred timeout-ms]
  (let [deadline (+ (System/currentTimeMillis) timeout-ms)]
    (loop []
      (if (pred)
        true
        (if (< (System/currentTimeMillis) deadline)
          (do (Thread/sleep 25)
              (recur))
          false)))))

(defn- offpeak-window-away-from-now
  []
  (let [now      (java.time.LocalTime/now)
        minute   (+ (* 60 (.getHour ^java.time.LocalTime now))
                    (.getMinute ^java.time.LocalTime now))
        day-mins (* 24 60)
        start    (mod (+ minute 720) day-mins)
        end      (mod (+ start 60) day-mins)]
    [{:start start :end end}]))

(deftest txn-log-enables-nosync-flag-test
  (let [dir (u/tmp-dir (str "txlog-nosync-" (UUID/randomUUID)))]
    (try
      (let [lmdb (l/open-kv dir {:wal? true})]
        (is (contains? (if/get-env-flags lmdb) :nosync))
        (if/close-kv lmdb))
      (finally
        (u/delete-files dir)))))

(deftest wal-alias-enables-nosync-flag-test
  (let [dir (u/tmp-dir (str "wal-nosync-" (UUID/randomUUID)))]
    (try
      (let [lmdb       (l/open-kv dir {:wal? true})
            watermarks (if/txlog-watermarks lmdb)]
        (is (contains? (if/get-env-flags lmdb) :nosync))
        (is (:wal? watermarks))
        (if/close-kv lmdb))
      (finally
        (u/delete-files dir)))))

(deftest top-level-txn-log-propagates-to-kv-test
  (let [dir (u/tmp-dir (str "txlog-top-level-propagation-" (UUID/randomUUID)))
        schema {:k {:db/valueType :db.type/long}
                :v {:db/valueType :db.type/string}}]
    (try
      (let [conn       (dc/get-conn dir schema
                                    {:wal? true
                                     :kv-opts  {:mapsize 64}})
            ^DB db     @conn
            ^Store s   (.-store db)
            lmdb       (.-lmdb s)
            watermarks (if/txlog-watermarks lmdb)]
        (is (:wal? watermarks))
        (is (contains? (if/get-env-flags lmdb) :nosync))
        (dc/close conn))
      (finally
        (u/delete-files dir)))))

(deftest top-level-wal-propagates-to-kv-test
  (let [dir (u/tmp-dir (str "wal-top-level-propagation-" (UUID/randomUUID)))
        schema {:k {:db/valueType :db.type/long}
                :v {:db/valueType :db.type/string}}]
    (try
      (let [conn (dc/create-conn dir schema
                                 {:wal? true
                                  :wal-durability-profile :strict
                                  :kv-opts {:mapsize 64}})]
        (try
          (let [^DB db     @conn
                ^Store s   (.-store db)
                lmdb       (.-lmdb s)
                env-opts   (if/env-opts lmdb)
                watermarks (if/txlog-watermarks lmdb)]
            (is (:wal? watermarks))
            (is (contains? (if/get-env-flags lmdb) :nosync))
            (is (true? (:wal? env-opts)))
            (is (= :strict (:wal-durability-profile env-opts))))
          (finally
            (dc/close conn))))
      (finally
        (try
          (u/delete-files dir)
          (catch Exception _))))))

(deftest datalog-defaults-to-strict-wal-test
  (let [dir (u/tmp-dir (str "datalog-default-wal-" (UUID/randomUUID)))
        schema {:k {:db/valueType :db.type/long}
                :v {:db/valueType :db.type/string}}]
    (try
      (let [conn (dc/create-conn dir schema {:kv-opts {:mapsize 64}})]
        (try
          (let [^DB db     @conn
                ^Store s   (.-store db)
                lmdb       (.-lmdb s)
                env-opts   (if/env-opts lmdb)
                watermarks (if/txlog-watermarks lmdb)]
            (is (true? (:wal? env-opts)))
            (is (= :strict (:wal-durability-profile env-opts)))
            (is (:wal? watermarks))
            (is (= :strict (:durability-profile watermarks))))
          (finally
            (dc/close conn))))
      (finally
        (try
          (u/delete-files dir)
          (catch Exception _))))))

(deftest txn-log-replay-from-commit-marker-test
  (let [dir (u/tmp-dir (str "txlog-replay-" (UUID/randomUUID)))]
    (try
      (let [lmdb (l/open-kv dir {:flags    (conj c/default-env-flags :nosync)
                                 :wal? true})]
        (if/open-list-dbi lmdb "list")
        (if/transact-kv lmdb [[:put-list "list" :k [1] :data :long]])
        (if/transact-kv lmdb [[:put-list "list" :k [2] :data :long]])
        (is (= [1 2] (if/get-list lmdb "list" :k :data :long)))
        (if/close-kv lmdb))

      (let [records (read-txlog-records dir)
            r1      (or (list-put-record records 1)
                        (throw (ex-info "Missing txn-log record for value 1" {})))
            lmdb    (l/open-kv dir {:flags (conj c/default-env-flags :nosync)})
            slot-a  (txlog/encode-commit-marker-slot
                     {:revision            100
                      :applied-lsn         (:lsn r1)
                      :txlog-segment-id    (:segment-id r1)
                      :txlog-record-offset (:offset r1)
                      :txlog-record-crc    (:checksum r1)})
            slot-b  (txlog/encode-commit-marker-slot
                     {:revision            101
                      :applied-lsn         (:lsn r1)
                      :txlog-segment-id    (:segment-id r1)
                      :txlog-record-offset (:offset r1)
                      :txlog-record-crc    (:checksum r1)})]
        ;; Simulate pre-replay LMDB state at marker LSN.
        (if/open-list-dbi lmdb "list")
        (if/transact-kv lmdb [[:del-list "list" :k [2] :data :long]])
        (is (= [1] (if/get-list lmdb "list" :k :data :long)))
        (if/transact-kv lmdb
                        [[:put c/kv-info c/wal-marker-a slot-a :keyword :bytes]
                         [:put c/kv-info c/wal-marker-b slot-b :keyword :bytes]])
        (if/close-kv lmdb))

      (let [lmdb (l/open-kv dir {:flags    (conj c/default-env-flags :nosync)
                                 :wal? true})]
        (if/open-list-dbi lmdb "list")
        (is (= [1 2] (if/get-list lmdb "list" :k :data :long)))
        (if/close-kv lmdb))
      (finally
        (u/delete-files dir)))))

(deftest txn-log-snapshot-bootstrap-test
  (let [dir (u/tmp-dir (str "txlog-bootstrap-" (UUID/randomUUID)))]
    (try
      (let [lmdb (l/open-kv dir {:flags    (conj c/default-env-flags :nosync)
                                 :wal? true})]
        ;; Fresh LMDB may not have all copy prerequisites until first write.
        (is (empty? (if/list-snapshots lmdb)))
        (if/open-list-dbi lmdb "list")
        (if/transact-kv lmdb [[:put-list "list" :k [1] :data :long]])
        (if/close-kv lmdb))
      ;; Re-open should complete bootstrap to steady-state (current + previous).
      (let [lmdb (l/open-kv dir {:flags    (conj c/default-env-flags :nosync)
                                 :wal? true})]
        (let [snapshots (if/list-snapshots lmdb)]
          (is (= 2 (count snapshots)))
          (is (= :current (:slot (first snapshots))))
          (is (= :previous (:slot (second snapshots)))))
        (if/close-kv lmdb))
      (finally
        (u/delete-files dir)))))

(deftest txn-log-snapshot-bootstrap-disabled-test
  (let [dir (u/tmp-dir (str "txlog-bootstrap-off-" (UUID/randomUUID)))]
    (try
      (let [lmdb (l/open-kv dir {:flags                    (conj c/default-env-flags
                                                                 :nosync)
                                 :wal?                true
                                 :snapshot-bootstrap-force? false})]
        (is (empty? (if/list-snapshots lmdb)))
        (is (:ok? (if/create-snapshot! lmdb)))
        (is (= 1 (count (if/list-snapshots lmdb))))
        (if/close-kv lmdb))
      (finally
        (u/delete-files dir)))))

(deftest txn-log-snapshot-backup-pin-lifecycle-test
  (let [dir (u/tmp-dir (str "txlog-snapshot-pin-" (UUID/randomUUID)))]
    (try
      (let [lmdb (l/open-kv dir {:flags    (conj c/default-env-flags :nosync)
                                 :wal? true})]
        (if/open-list-dbi lmdb "list")
        (if/transact-kv lmdb [[:put-list "list" :k [1] :data :long]])
        (let [seen-pin-id (atom nil)
              seen-floor (atom nil)
              res (binding [kv/*wal-snapshot-copy-failpoint*
                            (fn [{:keys [lmdb pin-id pin-floor-lsn
                                         pin-expires-ms]}]
                              (reset! seen-pin-id pin-id)
                              (reset! seen-floor pin-floor-lsn)
                              (let [pins (if/get-value lmdb c/kv-info
                                                       c/wal-backup-pins
                                                       :keyword :data)]
                                (is (map? pins))
                                (is (= pin-floor-lsn
                                       (get-in pins [pin-id :floor-lsn])))
                                (is (= pin-expires-ms
                                       (get-in pins [pin-id :expires-ms])))))]
                    (if/create-snapshot! lmdb))
              pins-after (if/get-value lmdb c/kv-info c/wal-backup-pins
                                       :keyword :data)]
          (is (:ok? res))
          (is (string? @seen-pin-id))
          (is (= @seen-pin-id (get-in res [:backup-pin :pin-id])))
          (is (= @seen-floor (get-in res [:backup-pin :floor-lsn])))
          (is (or (nil? pins-after)
                  (not (contains? pins-after @seen-pin-id)))))
        (if/close-kv lmdb))
      (finally
        (u/delete-files dir)))))

(deftest txn-log-snapshot-backup-pin-unpinned-on-failure-test
  (let [dir (u/tmp-dir (str "txlog-snapshot-pin-fail-" (UUID/randomUUID)))]
    (try
      (let [lmdb (l/open-kv dir {:flags    (conj c/default-env-flags :nosync)
                                 :wal? true})]
        (if/open-list-dbi lmdb "list")
        (if/transact-kv lmdb [[:put-list "list" :k [1] :data :long]])
        (let [seen-pin-id (atom nil)]
          (is (thrown-with-msg?
               clojure.lang.ExceptionInfo #"snapshot-copy-failpoint"
               (binding [kv/*wal-snapshot-copy-failpoint*
                         (fn [{:keys [lmdb pin-id]}]
                           (reset! seen-pin-id pin-id)
                           (let [pins (if/get-value lmdb c/kv-info
                                                    c/wal-backup-pins
                                                    :keyword :data)]
                             (is (contains? pins pin-id)))
                           (throw (ex-info "snapshot-copy-failpoint" {})))]
                 (if/create-snapshot! lmdb))))
          (let [pins-after (if/get-value lmdb c/kv-info c/wal-backup-pins
                                         :keyword :data)]
            (is (string? @seen-pin-id))
            (is (or (nil? pins-after)
                    (not (contains? pins-after @seen-pin-id))))))
        (if/close-kv lmdb))
      (finally
        (u/delete-files dir)))))

(deftest txn-log-copy-backup-pin-lifecycle-test
  (let [dir (u/tmp-dir (str "txlog-copy-pin-" (UUID/randomUUID)))
        dest-root (u/tmp-dir (str "txlog-copy-pin-dest-" (UUID/randomUUID)))
        dest (str dest-root u/+separator+ "copy")]
    (try
      (let [lmdb (l/open-kv dir {:flags    (conj c/default-env-flags :nosync)
                                 :wal? true})]
        (if/open-dbi lmdb "a")
        (if/transact-kv lmdb [[:put "a" :k :v]])
        (let [seen-pin-id (atom nil)
              seen-floor (atom nil)]
          (let [copy-res
                (binding [kv/*wal-copy-backup-pin-failpoint*
                          (fn [{:keys [lmdb pin-id pin-floor-lsn
                                       pin-expires-ms compact?]}]
                            (reset! seen-pin-id pin-id)
                            (reset! seen-floor pin-floor-lsn)
                            (is (true? compact?))
                            (let [pins (if/get-value lmdb c/kv-info
                                                     c/wal-backup-pins
                                                     :keyword :data)]
                              (is (map? pins))
                              (is (= pin-floor-lsn
                                     (get-in pins [pin-id :floor-lsn])))
                              (is (= pin-expires-ms
                                     (get-in pins [pin-id :expires-ms])))))]
                  (if/copy lmdb dest true))]
            (is (map? copy-res))
            (is (number? (:started-ms copy-res)))
            (is (number? (:completed-ms copy-res)))
            (is (number? (:duration-ms copy-res)))
            (is (<= (:started-ms copy-res) (:completed-ms copy-res)))
            (is (true? (:compact? copy-res)))
            (is (= @seen-pin-id (get-in copy-res [:backup-pin :pin-id])))
            (is (= @seen-floor (get-in copy-res [:backup-pin :floor-lsn])))
            (is (number? (get-in copy-res [:backup-pin :expires-ms]))))
          (let [pins-after (if/get-value lmdb c/kv-info c/wal-backup-pins
                                         :keyword :data)]
            (is (string? @seen-pin-id))
            (is (number? @seen-floor))
            (is (u/file-exists (str dest u/+separator+ c/data-file-name)))
            (is (or (nil? pins-after)
                    (not (contains? pins-after @seen-pin-id))))))
        (if/close-kv lmdb))
      (let [copy-lmdb (l/open-kv dest)]
        (if/open-dbi copy-lmdb "a")
        (is (= :v (if/get-value copy-lmdb "a" :k)))
        (if/close-kv copy-lmdb))
      (finally
        (u/delete-files dir)
        (try
          (u/delete-files dest-root)
          (catch Exception _))))))

(deftest txn-log-copy-backup-pin-unpinned-on-failure-test
  (let [dir (u/tmp-dir (str "txlog-copy-pin-fail-" (UUID/randomUUID)))
        dest-root (u/tmp-dir (str "txlog-copy-pin-fail-dest-" (UUID/randomUUID)))
        dest (str dest-root u/+separator+ "copy")]
    (try
      (let [lmdb (l/open-kv dir {:flags    (conj c/default-env-flags :nosync)
                                 :wal? true})]
        (if/open-dbi lmdb "a")
        (if/transact-kv lmdb [[:put "a" :k :v]])
        (let [seen-pin-id (atom nil)]
          (is (thrown-with-msg?
               clojure.lang.ExceptionInfo #"copy-backup-pin-failpoint"
               (binding [kv/*wal-copy-backup-pin-failpoint*
                         (fn [{:keys [lmdb pin-id]}]
                           (reset! seen-pin-id pin-id)
                           (let [pins (if/get-value lmdb c/kv-info
                                                    c/wal-backup-pins
                                                    :keyword :data)]
                             (is (contains? pins pin-id)))
                           (throw (ex-info "copy-backup-pin-failpoint" {})))]
                 (if/copy lmdb dest false))))
          (let [pins-after (if/get-value lmdb c/kv-info c/wal-backup-pins
                                         :keyword :data)]
            (is (string? @seen-pin-id))
            (is (or (nil? pins-after)
                    (not (contains? pins-after @seen-pin-id))))))
        (if/close-kv lmdb))
      (finally
        (u/delete-files dir)
        (try
          (u/delete-files dest-root)
          (catch Exception _))))))

(deftest txn-log-snapshot-scheduler-auto-test
  (let [dir (u/tmp-dir (str "txlog-snapshot-scheduler-" (UUID/randomUUID)))]
    (try
      (let [lmdb (l/open-kv dir {:flags               (conj c/default-env-flags
                                                             :nosync)
                                 :wal?            true
                                 :snapshot-scheduler? true
                                 :snapshot-interval-ms 100
                                 :snapshot-max-lsn-delta 1})]
        (if/open-list-dbi lmdb "list")
        (if/transact-kv lmdb [[:put-list "list" :k [1] :data :long]])
        (if/transact-kv lmdb [[:put-list "list" :k [2] :data :long]])
        (is (wait-until #(u/file-exists (str dir u/+separator+ c/version-file-name))
                        2000))
        ;; Force two scheduler steps to cover bootstrap (current, then previous).
        (is (some? (#'kv/maybe-run-snapshot-scheduler! lmdb)))
        (is (some? (#'kv/maybe-run-snapshot-scheduler! lmdb)))
        (is (= 2 (count (if/list-snapshots lmdb))))
        (let [state (if/snapshot-scheduler-state lmdb)]
          (is (:enabled? state))
          (is (= :auto (:mode state)))
          (is (boolean? (:running? state)))
          (is (number? (:last-success-ms state)))
          (is (contains? #{:bootstrap :interval :lsn-delta
                           :gc-safety
                           :max-age :log-bytes-delta}
                         (:last-trigger state))))
        (if/close-kv lmdb))
      (finally
        (u/delete-files dir)))))

(deftest txn-log-snapshot-scheduler-log-bytes-trigger-test
  (let [dir (u/tmp-dir (str "txlog-snapshot-log-bytes-" (UUID/randomUUID)))]
    (try
      (let [lmdb (l/open-kv dir {:flags                       (conj
                                                                c/default-env-flags
                                                                :nosync)
                                 :wal?                    true
                                 :snapshot-scheduler?         true
                                 :snapshot-interval-ms        600000
                                 :snapshot-max-lsn-delta      1000000
                                 :snapshot-max-log-bytes-delta 1
                                 :snapshot-max-age-ms         600000})]
        (if/open-list-dbi lmdb "list")
        (if/transact-kv lmdb [[:put-list "list" :k [1] :data :long]])
        ;; Bootstrap to steady-state snapshots.
        (is (some? (#'kv/maybe-run-snapshot-scheduler! lmdb)))
        (is (some? (#'kv/maybe-run-snapshot-scheduler! lmdb)))
        ;; Add more txlog bytes without tripping other triggers.
        (if/transact-kv lmdb [[:put-list "list" :k [2] :data :long]])
        (let [res   (#'kv/maybe-run-snapshot-scheduler! lmdb)
              state (if/snapshot-scheduler-state lmdb)]
          (is (:ok? res))
          (is (= :log-bytes-delta (:trigger res)))
          (is (= :log-bytes-delta (:last-trigger state)))
          (is (<= 1 (or (get-in state
                                 [:last-trigger-details
                                  :txlog-bytes-since-snapshot])
                        0))))
        (if/close-kv lmdb))
      (finally
        (u/delete-files dir)))))

(deftest txn-log-snapshot-scheduler-max-age-trigger-test
  (let [dir (u/tmp-dir (str "txlog-snapshot-max-age-" (UUID/randomUUID)))]
    (try
      (let [lmdb (l/open-kv dir {:flags                       (conj
                                                                c/default-env-flags
                                                                :nosync)
                                 :wal?                    true
                                 :snapshot-scheduler?         true
                                 :snapshot-interval-ms        600000
                                 :snapshot-max-lsn-delta      1000000
                                 :snapshot-max-log-bytes-delta 9223372036854775807
                                 :snapshot-max-age-ms         1})]
        (if/open-list-dbi lmdb "list")
        (if/transact-kv lmdb [[:put-list "list" :k [1] :data :long]])
        ;; Bootstrap to steady-state snapshots.
        (is (some? (#'kv/maybe-run-snapshot-scheduler! lmdb)))
        (is (some? (#'kv/maybe-run-snapshot-scheduler! lmdb)))
        (Thread/sleep 5)
        (let [res   (#'kv/maybe-run-snapshot-scheduler! lmdb)
              state (if/snapshot-scheduler-state lmdb)]
          (is (:ok? res))
          (is (= :max-age (:trigger res)))
          (is (= :max-age (:last-trigger state)))
          (is (<= 1 (or (get-in state
                                 [:last-trigger-details
                                  :snapshot-age-ms])
                        0))))
        (if/close-kv lmdb))
      (finally
        (u/delete-files dir)))))

(deftest txn-log-snapshot-scheduler-gc-safety-trigger-test
  (let [dir (u/tmp-dir (str "txlog-snapshot-gc-safety-" (UUID/randomUUID)))]
    (try
      (let [lmdb (l/open-kv dir {:flags                       (conj
                                                                c/default-env-flags
                                                                :nosync)
                                 :wal?                    true
                                 :wal-retention-bytes     1
                                 :snapshot-scheduler?         true
                                 :snapshot-interval-ms        600000
                                 :snapshot-max-lsn-delta      1000000
                                 :snapshot-max-log-bytes-delta 9223372036854775807
                                 :snapshot-max-age-ms         600000})]
        (if/open-list-dbi lmdb "list")
        (if/transact-kv lmdb [[:put-list "list" :k [1] :data :long]])
        ;; Bootstrap to steady-state snapshots.
        (is (some? (#'kv/maybe-run-snapshot-scheduler! lmdb)))
        (is (some? (#'kv/maybe-run-snapshot-scheduler! lmdb)))
        (let [res   (#'kv/maybe-run-snapshot-scheduler! lmdb)
              state (if/snapshot-scheduler-state lmdb)]
          (is (:ok? res))
          (is (= :gc-safety (:trigger res)))
          (is (= :gc-safety (:last-trigger state)))
          (is (true? (get-in state [:last-trigger-details
                                    :pressure
                                    :degraded?])))
          (is (some #{:snapshot-floor-lsn}
                    (get-in state [:last-trigger-details
                                   :floor-limiters]))))
        (if/close-kv lmdb))
      (finally
        (u/delete-files dir)))))

(deftest txn-log-snapshot-scheduler-offpeak-deferral-test
  (let [dir             (u/tmp-dir (str "txlog-snapshot-offpeak-deferral-"
                                        (UUID/randomUUID)))
        offpeak-windows (offpeak-window-away-from-now)]
    (try
      (let [lmdb (l/open-kv dir {:flags                        (conj
                                                                 c/default-env-flags
                                                                 :nosync)
                                 :wal?                     true
                                 :snapshot-scheduler?          true
                                 :snapshot-interval-ms         600000
                                 :snapshot-max-lsn-delta       1000000
                                 :snapshot-max-log-bytes-delta 1
                                 :snapshot-max-age-ms          600000
                                 :snapshot-defer-on-contention? false
                                 :snapshot-defer-backoff-min-ms 5
                                 :snapshot-defer-backoff-max-ms 5
                                 :snapshot-offpeak-windows     offpeak-windows})]
        (if/open-list-dbi lmdb "list")
        (if/transact-kv lmdb [[:put-list "list" :k [1] :data :long]])
        ;; Prepare steady-state snapshot slots so non-bootstrap triggers apply.
        (is (:ok? (if/create-snapshot! lmdb)))
        (is (:ok? (if/create-snapshot! lmdb)))
        ;; Add txlog bytes without tripping max-age/interval/lsn-delta.
        (if/transact-kv lmdb [[:put-list "list" :k [2] :data :long]])
        (let [res   (#'kv/maybe-run-snapshot-scheduler! lmdb)
              state (if/snapshot-scheduler-state lmdb)]
          (is (true? (:deferred? res)))
          (is (= :log-bytes-delta (:trigger res)))
          (is (= :offpeak-window (get-in res [:defer :reason])))
          (is (= :offpeak-window (:last-defer-reason state)))
          (is (= :log-bytes-delta (:last-defer-trigger state)))
          (is (false? (:in-offpeak-window? state)))
          (is (= 5 (:defer-backoff-ms state)))
          (is (number? (:next-eligible-ms state)))
          (is (number? (:defer-since-ms state)))
          (is (<= 0 (or (:current-defer-duration-ms state) -1)))
          (is (<= 1 (or (get-in state [:defer-count :offpeak-window]) 0)))
          (is (<= 1 (or (get-in state [:snapshot-defer-count
                                       :offpeak-window])
                        0)))
          (is (<= -1 (or (:snapshot-defer-duration-ms state) -1))))
        (if/close-kv lmdb))
      (finally
        (u/delete-files dir)))))

(deftest txn-log-snapshot-scheduler-contention-deferral-test
  (let [dir (u/tmp-dir (str "txlog-snapshot-contention-deferral-"
                            (UUID/randomUUID)))]
    (try
      (let [lmdb (l/open-kv dir {:flags                        (conj
                                                                 c/default-env-flags
                                                                 :nosync)
                                 :wal?                     true
                                 :snapshot-scheduler?          true
                                 :snapshot-interval-ms         600000
                                 :snapshot-max-lsn-delta       1000000
                                 :snapshot-max-log-bytes-delta 1
                                 :snapshot-max-age-ms          600000
                                 :snapshot-contention-thresholds
                                 {:commit-wait-p99-ms 1
                                  :queue-depth 1000000
                                  :fsync-p99-ms 1}
                                 :snapshot-contention-sample-max-age-ms
                                 1
                                 :snapshot-defer-backoff-min-ms 1
                                 :snapshot-defer-backoff-max-ms 1})]
        (if/open-list-dbi lmdb "list")
        (if/transact-kv lmdb [[:put-list "list" :k [1] :data :long]])
        ;; Prepare steady-state snapshot slots so non-bootstrap triggers apply.
        (is (:ok? (if/create-snapshot! lmdb)))
        (is (:ok? (if/create-snapshot! lmdb)))
        ;; Add txlog bytes without tripping max-age/interval/lsn-delta.
        (if/transact-kv lmdb [[:put-list "list" :k [2] :data :long]])
        ;; Inject recent contention samples to trigger load-shed deferral.
        (let [sync-manager (:sync-manager (txlog/state lmdb))]
          (txlog/record-commit-wait-ms! sync-manager 10)
          (txlog/record-fsync-ms! sync-manager 10))
        (let [res   (#'kv/maybe-run-snapshot-scheduler! lmdb)
              state (if/snapshot-scheduler-state lmdb)]
          (is (true? (:deferred? res)))
          (is (= :log-bytes-delta (:trigger res)))
          (is (= :contention (get-in res [:defer :reason])))
          (is (true? (get-in res [:defer :hits :commit-wait-p99-ms])))
          (is (true? (get-in res [:defer :hits :fsync-p99-ms])))
          (is (= :contention (:last-defer-reason state)))
          (is (= :log-bytes-delta (:last-defer-trigger state)))
          (is (= 1 (:defer-backoff-ms state)))
          (is (number? (:next-eligible-ms state)))
          (is (<= 1 (or (get-in state [:defer-count :contention]) 0)))
          (is (<= 1 (or (get-in state [:snapshot-defer-count :contention])
                        0))))
        ;; Allow contention samples to expire and confirm scheduler proceeds.
        (Thread/sleep 5)
        (let [res   (#'kv/maybe-run-snapshot-scheduler! lmdb)
              state (if/snapshot-scheduler-state lmdb)]
          (is (:ok? res))
          (is (= :log-bytes-delta (:trigger res)))
          (is (<= 1 (or (:run-count state) 0)))
          (is (nil? (:defer-since-ms state)))
          (is (number? (:last-run-duration-ms state)))
          (is (<= 0 (or (:defer-duration-ms state) -1)))
          (is (<= 0 (or (:last-defer-duration-ms state) -1))))
        (if/close-kv lmdb))
      (finally
        (u/delete-files dir)))))

(deftest txn-log-snapshot-scheduler-max-age-override-deferral-test
  (let [dir             (u/tmp-dir (str "txlog-snapshot-max-age-override-"
                                        (UUID/randomUUID)))
        offpeak-windows (offpeak-window-away-from-now)]
    (try
      (let [lmdb (l/open-kv dir {:flags                        (conj
                                                                 c/default-env-flags
                                                                 :nosync)
                                 :wal?                     true
                                 :snapshot-scheduler?          true
                                 :snapshot-interval-ms         600000
                                 :snapshot-max-lsn-delta       1000000
                                 :snapshot-max-log-bytes-delta 9223372036854775807
                                 :snapshot-max-age-ms          1
                                 :snapshot-offpeak-windows     offpeak-windows
                                 :snapshot-defer-on-contention? true
                                 :snapshot-contention-thresholds
                                 {:commit-wait-p99-ms 1
                                  :queue-depth 1000000
                                  :fsync-p99-ms 1}
                                 :snapshot-contention-sample-max-age-ms
                                 60000})]
        (if/open-list-dbi lmdb "list")
        (if/transact-kv lmdb [[:put-list "list" :k [1] :data :long]])
        ;; Prepare steady-state snapshot slots.
        (is (:ok? (if/create-snapshot! lmdb)))
        (is (:ok? (if/create-snapshot! lmdb)))
        ;; Inject contention and offpeak mismatch; max-age must still force run.
        (let [sync-manager (:sync-manager (txlog/state lmdb))]
          (txlog/record-commit-wait-ms! sync-manager 10)
          (txlog/record-fsync-ms! sync-manager 10))
        (Thread/sleep 5)
        (let [res   (#'kv/maybe-run-snapshot-scheduler! lmdb)
              state (if/snapshot-scheduler-state lmdb)]
          (is (:ok? res))
          (is (= :max-age (:trigger res)))
          (is (not (:deferred? res)))
          (is (= :max-age (:last-trigger state)))
          (is (<= 1 (or (:max-age-breach-count state) 0)))
          (is (<= 1 (or (:snapshot-max-age-breach-count state) 0)))
          (is (number? (:last-max-age-breach-ms state)))
          (is (<= 1 (or (:run-count state) 0)))
          (is (number? (:last-run-duration-ms state)))
          (is (<= (or (:last-run-duration-ms state) 0)
                  (or (:run-duration-ms state) 0)))
          (is (<= (or (:last-run-duration-ms state) 0)
                  (or (:snapshot-run-duration-ms state) 0))))
        (if/close-kv lmdb))
      (finally
        (u/delete-files dir)))))

(deftest txn-log-operational-apis-test
  (let [dir (u/tmp-dir (str "txlog-api-" (UUID/randomUUID)))]
    (try
      (let [lmdb (l/open-kv dir {:flags    (conj c/default-env-flags :nosync)
                                 :wal? true})]
        (if/open-list-dbi lmdb "list")
        (if/transact-kv lmdb [[:put-list "list" :k [1] :data :long]])
        (if/transact-kv lmdb [[:put-list "list" :k [2] :data :long]])
        (let [watermarks  (if/txlog-watermarks lmdb)
              committed   (:last-committed-lsn watermarks)
              applied     (:last-applied-lsn watermarks)
              marker      (if/read-commit-marker lmdb)
              verified    (if/verify-commit-marker! lmdb)
              tx-records  (if/open-tx-log lmdb 1 committed)
              force-res   (if/force-txlog-sync! lmdb)
              lmdb-force  (if/force-lmdb-sync! lmdb)
              forced-watermarks (:watermarks lmdb-force)
              snapshot1   (if/create-snapshot! lmdb)
              _           (if/transact-kv lmdb [[:put-list "list" :k [3]
                                                 :data :long]])
              snapshot2   (if/create-snapshot! lmdb)
              snapshots   (if/list-snapshots lmdb)
              sched-state (if/snapshot-scheduler-state lmdb)]
          (is (:wal? watermarks))
          (is (<= 2 committed))
          (is (= committed applied))
          (is (= applied (get-in marker [:current :applied-lsn])))
          (is (:ok? verified))
          (is (some? (:valid-marker verified)))
          (is (some? (list-put-record tx-records 1)))
          (is (some? (list-put-record tx-records 2)))
          (is (every? #(contains? % :tx-kind) tx-records))
          (is (every? #{:user :vector-checkpoint :unknown}
                      (map :tx-kind tx-records)))
          (is (some #(= :user (:tx-kind %)) tx-records))
          (is (<= 0 (or (:batched-sync-count watermarks) -1)))
          (is (contains? watermarks :avg-commit-wait-ms))
          (is (map? (:avg-commit-wait-ms-by-mode watermarks)))
          (is (<= 0 (or (:segment-roll-count watermarks) -1)))
          (is (<= 0 (or (:segment-roll-duration-ms watermarks) -1)))
          (is (<= 0 (or (:segment-prealloc-success-count watermarks) -1)))
          (is (<= 0 (or (:segment-prealloc-failure-count watermarks) -1)))
          (is (contains? watermarks :append-p99-near-roll-ms))
          (is (<= 0 (or (:vec-checkpoint-count watermarks) -1)))
          (is (<= 0 (or (:vec-checkpoint-duration-ms watermarks) -1)))
          (is (<= 0 (or (:vec-checkpoint-bytes watermarks) -1)))
          (is (<= 0 (or (:vec-checkpoint-failure-count watermarks) -1)))
          (is (<= 0 (or (:vec-replay-lag-lsn watermarks) -1)))
          (is (map? (:vec-checkpoint-domains watermarks)))
          (is (:synced? force-res))
          (is (<= (:target-lsn force-res)
                  (:last-durable-lsn force-res)))
          (is (:synced? lmdb-force))
          (is (= committed
                 (get-in lmdb-force [:watermarks :last-applied-lsn])))
          (is (number? (:last-checkpoint-ms forced-watermarks)))
          (is (<= 0 (or (:checkpoint-staleness-ms forced-watermarks) -1)))
          (is (number? (:checkpoint-stale-threshold-ms forced-watermarks)))
          (is (boolean? (:checkpoint-stale? forced-watermarks)))
          (is (<= 0 (or (:durable-applied-lag-lsn forced-watermarks) -1)))
          (is (number? (:durable-applied-lag-threshold-lsn forced-watermarks)))
          (is (boolean? (:durable-applied-lag-alert? forced-watermarks)))
          (is (<= 1 (or (:lmdb-sync-count forced-watermarks) 0)))
          (is (:ok? snapshot1))
          (is (:ok? snapshot2))
          (is (= 2 (count snapshots)))
          (is (= :current (:slot (first snapshots))))
          (is (= :previous (:slot (second snapshots))))
          (is (u/file-exists (get-in snapshot2 [:snapshot :path])))
          (is (<= (get-in snapshot1 [:snapshot :applied-lsn])
                  (get-in snapshot2 [:snapshot :applied-lsn])))
          (is (= 2 (:snapshot-count sched-state)))
          (is (false? (:enabled? sched-state)))
          (is (some? (:latest-snapshot sched-state)))
          (is (boolean? (:snapshot-age-alert? sched-state)))
          (is (<= 0 (or (:snapshot-failure-count sched-state) -1)))
          (is (<= 0 (or (:snapshot-consecutive-failure-count sched-state) -1)))
          (is (boolean? (:snapshot-build-failure-alert? sched-state))))
        (if/close-kv lmdb))
      (finally
        (u/delete-files dir)))))

(deftest txn-log-rollback-switch-test
  (let [dir (u/tmp-dir (str "txlog-rollback-switch-" (UUID/randomUUID)))]
    (try
      (let [lmdb (l/open-kv dir {:flags                (conj
                                                         c/default-env-flags
                                                         :nosync)
                                 :wal?            true
                                 :wal-rollout-mode :rollback})]
        (if/open-dbi lmdb "a")
        (if/transact-kv lmdb [[:put "a" :k :v]])
        (is (= :v (if/get-value lmdb "a" :k)))
        (is (false? (u/file-exists (str dir u/+separator+ "txlog"))))
        (let [watermarks (if/txlog-watermarks lmdb)
              txlog-force (if/force-txlog-sync! lmdb)
              lmdb-force (if/force-lmdb-sync! lmdb)
              snapshot (if/create-snapshot! lmdb)
              verified (if/verify-commit-marker! lmdb)
              retention (if/txlog-retention-state lmdb)
              gc-res (if/gc-txlog-segments! lmdb)]
          (is (:wal? watermarks))
          (is (= :rollback (:rollout-mode watermarks)))
          (is (false? (:write-path-enabled? watermarks)))
          (is (true? (:rollback? watermarks)))
          (is (false? (:synced? txlog-force)))
          (is (:skipped? txlog-force))
          (is (= :rollback (:reason txlog-force)))
          (is (:synced? lmdb-force))
          (is (= :rollback
                 (get-in lmdb-force [:watermarks :rollout-mode])))
          (is (false? (:ok? snapshot)))
          (is (:skipped? snapshot))
          (is (= :rollback (:reason snapshot)))
          (is (false? (:ok? verified)))
          (is (:skipped? verified))
          (is (= :rollback (:reason verified)))
          (is (= :rollback (get-in verified [:watermarks :rollout-mode])))
          (is (:wal? retention))
          (is (:skipped? retention))
          (is (= :rollback (:reason retention)))
          (is (= :rollback (get-in retention [:watermarks :rollout-mode])))
          (is (false? (:ok? gc-res)))
          (is (:skipped? gc-res))
          (is (= :rollback (:reason gc-res)))
          (is (= :rollback (get-in gc-res [:watermarks :rollout-mode]))))
        (if/close-kv lmdb))
      (finally
        (u/delete-files dir)))))

(deftest txn-log-rollout-option-conflict-test
  (let [dir (u/tmp-dir (str "txlog-rollout-conflict-" (UUID/randomUUID)))]
    (try
      (is (thrown-with-msg? Exception #"Conflicting txn-log rollout options"
            (l/open-kv dir {:flags                (conj
                                                   c/default-env-flags
                                                   :nosync)
                            :wal?            true
                            :wal-rollout-mode :active
                            :wal-rollback?   true})))
      (finally
        (u/delete-files dir)))))

(defn- setup-txlog-snapshot-recovery-fixture!
  ([dir]
   (setup-txlog-snapshot-recovery-fixture! dir false))
  ([dir corrupt-marker?]
  (let [lmdb (l/open-kv dir {:flags    (conj c/default-env-flags :nosync)
                             :wal? true})]
    (try
      (if/open-list-dbi lmdb "list")
      (if/transact-kv lmdb [[:put-list "list" :k [1] :data :long]])
      (if/transact-kv lmdb [[:put-list "list" :k [2] :data :long]])
      (if/create-snapshot! lmdb)
      (if/transact-kv lmdb [[:put-list "list" :k [3] :data :long]])
      (let [snapshot2 (if/create-snapshot! lmdb)
            current-path (get-in snapshot2 [:snapshot :path])
            previous-path (get-in snapshot2 [:snapshots 1 :path])]
        (if/transact-kv lmdb [[:put-list "list" :k [4] :data :long]])
        (when corrupt-marker?
          (let [slot-a (txlog/encode-commit-marker-slot
                        {:revision            1000
                         :applied-lsn         999999
                         :txlog-segment-id    9999
                         :txlog-record-offset 0
                         :txlog-record-crc    0})
                slot-b (txlog/encode-commit-marker-slot
                        {:revision            1001
                         :applied-lsn         999999
                         :txlog-segment-id    9999
                         :txlog-record-offset 0
                         :txlog-record-crc    0})]
            (if/transact-kv lmdb c/kv-info
                            [[:put c/wal-marker-a slot-a]
                             [:put c/wal-marker-b slot-b]]
                            :keyword :bytes)))
        {:current-path current-path
         :previous-path previous-path})
      (finally
        (if/close-kv lmdb))))))

(deftest txn-log-snapshot-recovery-current-fallback-test
  (let [dir (u/tmp-dir (str "txlog-snapshot-current-" (UUID/randomUUID)))]
    (try
      (setup-txlog-snapshot-recovery-fixture! dir true)
      (let [lmdb (l/open-kv dir {:flags    (conj c/default-env-flags :nosync)
                                 :wal? true})]
        (try
          (if/open-list-dbi lmdb "list")
          (is (= [1 2 3 4] (if/get-list lmdb "list" :k :data :long)))
          (is (= :snapshot-current
                 (get (if/env-opts lmdb) :txlog-recovery-source)))
          (is (= :current
                 (get (if/env-opts lmdb) :txlog-recovery-snapshot-slot)))
          (finally
            (if/close-kv lmdb))))
      (finally
        (u/delete-files dir)))))

(deftest txn-log-snapshot-recovery-previous-fallback-test
  (let [dir (u/tmp-dir (str "txlog-snapshot-previous-" (UUID/randomUUID)))]
    (try
      (let [{:keys [current-path previous-path]}
            (setup-txlog-snapshot-recovery-fixture! dir true)]
        (is (u/file-exists (str previous-path u/+separator+ c/data-file-name)))
        (u/delete-files (str current-path u/+separator+ c/data-file-name))
        (let [lmdb (l/open-kv dir {:flags    (conj c/default-env-flags :nosync)
                                   :wal? true})]
          (try
            (if/open-list-dbi lmdb "list")
            (is (= [1 2 3 4] (if/get-list lmdb "list" :k :data :long)))
            (is (= :snapshot-previous
                   (get (if/env-opts lmdb) :txlog-recovery-source)))
            (is (= :previous
                   (get (if/env-opts lmdb) :txlog-recovery-snapshot-slot)))
            (finally
              (if/close-kv lmdb)))))
      (finally
        (u/delete-files dir)))))

(deftest txn-log-retention-gc-test
  (let [dir       (u/tmp-dir (str "txlog-retention-" (UUID/randomUUID)))
        txlog-dir (str dir u/+separator+ "txlog")]
    (try
      (let [lmdb (l/open-kv dir {:flags                     (conj c/default-env-flags
                                                                  :nosync)
                                 :wal?                  true
                                 :wal-group-commit      1
                                 :wal-segment-max-bytes 64
                                 :wal-segment-max-ms    600000})]
        (if/open-list-dbi lmdb "list")
        (doseq [v (range 1 13)]
          (if/transact-kv lmdb [[:put-list "list" :k [v] :data :long]]))
        (let [before-files   (txlog/segment-files txlog-dir)
              before-count   (count before-files)
              retention0     (if/txlog-retention-state lmdb)
              pinned-gc      (if/gc-txlog-segments! lmdb 1)
              unpinned-gc    (if/gc-txlog-segments! lmdb)
              after-files    (txlog/segment-files txlog-dir)
              after-count    (count after-files)
              retention1     (if/txlog-retention-state lmdb)
              marker-verified (if/verify-commit-marker! lmdb)]
          (is (> before-count 1))
          (is (pos? (:safety-deletable-segment-count retention0)))
          (is (zero? (:deleted-count pinned-gc)))
          (is (pos? (:deleted-count unpinned-gc)))
          (is (= 1 after-count))
          (is (:ok? marker-verified))
          (is (<= (:min-retained-lsn retention1)
                  (inc (:applied-lsn retention1)))))
        (if/close-kv lmdb))
      (finally
        (u/delete-files dir)))))

(deftest txn-log-retention-floor-providers-test
  (let [dir       (u/tmp-dir (str "txlog-floor-providers-" (UUID/randomUUID)))
        stale-ms  (- (System/currentTimeMillis) 60000)
        now-ms    (System/currentTimeMillis)]
    (try
      (let [lmdb (l/open-kv dir {:flags    (conj c/default-env-flags :nosync)
                                 :wal? true
                                 :wal-replica-floor-ttl-ms 30000})]
        (if/open-dbi lmdb "a")
        (if/open-dbi lmdb c/vec-meta-dbi)
        (if/transact-kv lmdb [[:put "a" :k :v]])
        (if/transact-kv lmdb c/vec-meta-dbi
                        [[:put "d-prev" {:previous-snapshot-lsn 3}]
                         [:put "d-cur" {:current-snapshot-lsn 10}]
                         [:put "d-replay" {:vec-replay-floor-lsn 5}]]
                        :string :data)
        (if/transact-kv lmdb c/kv-info
                        [[:put c/wal-snapshot-current-lsn 12]
                         [:put c/wal-snapshot-previous-lsn 8]
                         [:put c/wal-replica-floors
                          {:replica-a {:applied-lsn 6 :updated-ms now-ms}
                           :replica-b {:applied-lsn 2 :updated-ms stale-ms}}]
                         [:put c/wal-backup-pins
                          {:backup-a {:floor-lsn 7}
                           :backup-b {:floor-lsn 1 :expires-ms stale-ms}}]]
                        :keyword :data)
        (let [state      (if/txlog-retention-state lmdb)
              floors     (:floors state)
              providers  (:floor-providers state)]
          (is (= 9 (:snapshot-floor-lsn floors)))
          (is (= 4 (:vector-global-floor-lsn floors)))
          (is (= 6 (:replica-floor-lsn floors)))
          (is (= 7 (:backup-pin-floor-lsn floors)))
          (is (= 4 (:required-retained-floor-lsn state)))
          (is (= [:vector-global-floor-lsn] (:floor-limiters state)))
          (is (= 1 (get-in providers [:replica :active-count])))
          (is (= 1 (get-in providers [:replica :stale-count])))
          (is (= 1 (get-in providers [:backup :active-count])))
          (is (= 1 (get-in providers [:backup :expired-count]))))
        (if/close-kv lmdb))
      (finally
        (u/delete-files dir)))))

(deftest txn-log-retention-degraded-write-backpressure-test
  (let [dir (u/tmp-dir (str "txlog-retention-backpressure-"
                            (UUID/randomUUID)))]
    (try
      (let [lmdb (l/open-kv dir {:flags                    (conj
                                                            c/default-env-flags
                                                            :nosync)
                                 :wal?                 true
                                 :wal-retention-bytes  1
                                 :wal-retention-ms     600000
                                 :wal-segment-max-ms   600000
                                 :wal-segment-max-bytes 1024})]
        (if/open-dbi lmdb "a")
        (if/txlog-update-snapshot-floor! lmdb 0)
        (if/transact-kv lmdb [[:put "a" :k1 :v1]])
        (Thread/sleep 1100)
        (let [ex (try
                   (if/transact-kv lmdb [[:put "a" :k2 :v2]])
                   nil
                   (catch clojure.lang.ExceptionInfo e
                     e))
              retention (if/txlog-retention-state lmdb)]
          (is (nil? ex))
          (is (true? (get-in retention [:pressure :degraded?])))
          (is (= :v1 (if/get-value lmdb "a" :k1)))
          (is (= :v2 (if/get-value lmdb "a" :k2))))
        (if/close-kv lmdb))
      (finally
        (u/delete-files dir)))))

(deftest txn-log-retention-pin-backpressure-grace-test
  (let [dir (u/tmp-dir (str "txlog-retention-pin-backpressure-"
                            (UUID/randomUUID)))]
    (try
      (let [lmdb (l/open-kv dir {:flags    (conj c/default-env-flags :nosync)
                                 :wal? true
                                 :wal-retention-bytes 1
                                 :wal-retention-ms 600000
                                 :wal-retention-pin-backpressure-threshold-ms
                                 100
                                 :wal-segment-max-ms 600000
                                 :wal-segment-max-bytes 1024})]
        (if/open-dbi lmdb "a")
        (if/txlog-update-snapshot-floor! lmdb 1000)
        (if/txlog-update-replica-floor! lmdb :replica-a 0)
        (if/transact-kv lmdb [[:put "a" :k1 :v1]])
        (Thread/sleep 1100)
        (if/transact-kv lmdb [[:put "a" :k2 :v2]])
        (if/transact-kv lmdb [[:put "a" :k3 :v3]])
        (let [retention (if/txlog-retention-state lmdb)]
          (is (true? (get-in retention [:pressure :degraded?])))
          (is (some #{:replica-floor-lsn} (:floor-limiters retention))))
        (Thread/sleep 1100)
        (let [ex (try
                   (if/transact-kv lmdb [[:put "a" :k4 :v4]])
                   nil
                   (catch clojure.lang.ExceptionInfo e
                     e))
              retention (if/txlog-retention-state lmdb)]
          (is (nil? ex))
          (is (true? (get-in retention [:pressure :degraded?])))
          (is (some #{:replica-floor-lsn} (:floor-limiters retention)))
          (is (= :v1 (if/get-value lmdb "a" :k1)))
          (is (= :v2 (if/get-value lmdb "a" :k2)))
          (is (= :v3 (if/get-value lmdb "a" :k3)))
          (is (= :v4 (if/get-value lmdb "a" :k4))))
        (if/close-kv lmdb))
      (finally
        (u/delete-files dir)))))

(deftest txn-log-floor-provider-api-test
  (let [dir (u/tmp-dir (str "txlog-floor-api-" (UUID/randomUUID)))]
    (try
      (let [lmdb (l/open-kv dir {:flags    (conj c/default-env-flags :nosync)
                                 :wal? true})]
        (if/open-dbi lmdb "a")
        (if/transact-kv lmdb [[:put "a" :k :v]])
        (let [s1 (if/txlog-update-snapshot-floor! lmdb 5)
              s2 (if/txlog-update-snapshot-floor! lmdb 9)
              _  (is (thrown? clojure.lang.ExceptionInfo
                              (if/txlog-update-snapshot-floor! lmdb 8)))
              s3 (if/txlog-update-snapshot-floor! lmdb 12 10)
              _  (is (thrown? clojure.lang.ExceptionInfo
                              (if/txlog-update-snapshot-floor! lmdb 11 13)))
              s4 (if/txlog-clear-snapshot-floor! lmdb)
              r1 (if/txlog-update-replica-floor! lmdb :r1 5)
              r2 (if/txlog-update-replica-floor! lmdb :r1 8)
              _  (is (thrown? clojure.lang.ExceptionInfo
                              (if/txlog-update-replica-floor! lmdb :r1 7)))
              r3 (if/txlog-clear-replica-floor! lmdb :r1)
              b1 (if/txlog-pin-backup-floor! lmdb :b1 9)
              b2 (if/txlog-pin-backup-floor! lmdb :b2 4
                                             (+ (System/currentTimeMillis) 60000))
              b3 (if/txlog-unpin-backup-floor! lmdb :b1)
              snapshot-current
              (if/get-value lmdb c/kv-info c/wal-snapshot-current-lsn
                            :keyword :data)
              snapshot-previous
              (if/get-value lmdb c/kv-info c/wal-snapshot-previous-lsn
                            :keyword :data)
              floors-map (if/get-value lmdb c/kv-info c/wal-replica-floors
                                       :keyword :data)
              pins-map   (if/get-value lmdb c/kv-info c/wal-backup-pins
                                       :keyword :data)]
          (is (= 5 (:snapshot-current-lsn s1)))
          (is (nil? (:snapshot-previous-lsn s1)))
          (is (= 9 (:snapshot-current-lsn s2)))
          (is (= 5 (:snapshot-previous-lsn s2)))
          (is (= 12 (:snapshot-current-lsn s3)))
          (is (= 10 (:snapshot-previous-lsn s3)))
          (is (:cleared? s4))
          (is (= 5 (:applied-lsn r1)))
          (is (= 8 (:applied-lsn r2)))
          (is (:removed? r3))
          (is (= 9 (:floor-lsn b1)))
          (is (= 4 (:floor-lsn b2)))
          (is (:removed? b3))
          (is (nil? snapshot-current))
          (is (nil? snapshot-previous))
          (is (nil? floors-map))
          (is (= #{:b2} (set (keys pins-map)))))
        (if/close-kv lmdb))
      (finally
        (u/delete-files dir)))))

(deftest open-again-resized-test
  (let [dir  (u/tmp-dir (str "again-resize-" (UUID/randomUUID)))
        lmdb (l/open-kv dir {:mapsize 1
                             :flags   (conj c/default-env-flags :nosync)})
        data {:description "this is normal data"}]

    (if/open-dbi lmdb "a")
    (if/transact-kv lmdb [[:put "a" 0 data]])
    (is (= data (if/get-value lmdb "a" 0)))
    (if/close-kv lmdb)

    (let [lmdb1 (l/open-kv dir {:mapsize 10})]
      (if/open-dbi lmdb1 "a")
      (is (= data (if/get-value lmdb1 "a" 0)))
      (if/transact-kv lmdb1 [[:put "a" 1000 "good"]])
      (is (= "good" (if/get-value lmdb1 "a" 1000)))
      (if/close-kv lmdb1))
    (u/delete-files dir)))

(deftest with-transaction-kv-test
  (let [dir  (u/tmp-dir (str "with-tx-kv-test-" (UUID/randomUUID)))
        lmdb (l/open-kv dir {:flags (conj c/default-env-flags :nosync)})]
    (if/open-dbi lmdb "a")

    (testing "new value is invisible to outside readers"
      (dc/with-transaction-kv [db lmdb]
        (is (nil? (if/get-value db "a" 1 :data :data false)))
        (if/transact-kv db [[:put "a" 1 2]
                            [:put "a" :counter 0]])
        (is (= [1 2] (if/get-value db "a" 1 :data :data false)))
        (is (nil? (if/get-value lmdb "a" 1 :data :data false))))
      (is (= [1 2] (if/get-value lmdb "a" 1 :data :data false))))

    (testing "abort"
      (dc/with-transaction-kv [db lmdb]
        (if/transact-kv db [[:put "a" 1 3]])
        (is (= [1 3] (if/get-value db "a" 1 :data :data false)))
        (if/abort-transact-kv db))
      (is (= [1 2] (if/get-value lmdb "a" 1 :data :data false))))

    (testing "concurrent writes do not overwrite each other"
      (let [count-f
            #(dc/with-transaction-kv [db lmdb]
               (let [^long now (if/get-value db "a" :counter)]
                 (if/transact-kv db [[:put "a" :counter (inc now)]])
                 (if/get-value db "a" :counter)))]
        (is (= (set [1 2 3])
               (set (pcalls count-f count-f count-f))))))

    (testing "nested concurrent writes"
      (let [count-f
            #(dc/with-transaction-kv [db lmdb]
               (let [^long now (if/get-value db "a" :counter)]
                 (dc/with-transaction-kv [db' db]
                   (if/transact-kv db' [[:put "a" :counter (inc now)]]))
                 (if/get-value db "a" :counter)))]
        (is (= (set [4 5 6])
               (set (pcalls count-f count-f count-f))))))

    (if/close-kv lmdb)
    (u/delete-files dir)))

(deftest tuple-range-query-test
  (let [dir  (u/tmp-dir (str "tuple-range-" (UUID/randomUUID)))
        lmdb (l/open-kv dir {:flags (conj c/default-env-flags :nosync)})]
    (if/open-dbi lmdb "t")
    (if/open-dbi lmdb "u")

    (if/transact-kv lmdb "t" [[:put [:a "a"] 1]
                              [:put [:a "b"] 2]]
                    [:keyword :string])
    (is (= [[[:a "a"] 1] [[:a "b"] 2]]
           (if/get-range lmdb "t" [:closed [:a "a"] [:a "z"]]
                         [:keyword :string])))
    (is (= [[[:a "a"] 1] [[:a "b"] 2]]
           (if/get-range lmdb "t"
                         [:closed [:a :db.value/sysMin] [:a :db.value/sysMax]]
                         [:keyword :string])))
    (is (= [[[:a "a"] 1]]
           (if/get-range lmdb "t"
                         [:closed [:db.value/sysMin :db.value/sysMin]
                          [:a "a"]]
                         [:keyword :string])))

    (if/transact-kv lmdb "u" [[:put [:a 1] 1] [:put [:a 2] 2] [:put [:b 2] 3]]
                    [:keyword :long])
    (is (= [[[:a 1] 1] [[:a 2] 2] [[:b 2] 3]]
           (if/get-range lmdb "u"
                         [:closed [:a :db.value/sysMin]
                          [:db.value/sysMax :db.value/sysMax]]
                         [:keyword :long])))
    (is (= [[[:b 2] 3]]
           (if/get-range lmdb "u"
                         [:closed [:b :db.value/sysMin] [:b :db.value/sysMax]]
                         [:keyword :long])))
    (if/close-kv lmdb)
    (u/delete-files dir)))

(deftest int-int-range-query-test
  (let [dir  (u/tmp-dir (str "int-int-range-" (UUID/randomUUID)))
        lmdb (l/open-kv dir {:flags (conj c/default-env-flags :nosync)})]
    (if/open-dbi lmdb "t")
    (if/transact-kv lmdb "t" [[:put [2 4] 4]
                              [:put [1 2] 2]
                              [:put [3 2] 5]
                              [:put [5 2] 6]
                              [:put [1 3] 3]
                              [:put [1 1] 1]
                              ]
                    :int-int :id)
    (is (= [[[2 4] 4] [[3 2] 5]]
           (if/get-range lmdb "t" [:closed [2 2] [3 3]] :int-int :id)))
    (is (= [[[3 2] 5] [[5 2] 6]]
           (if/get-range lmdb "t"
                         [:closed-open [3 0] [5 Integer/MAX_VALUE]]
                         :int-int :id)))
    (is (= [[[3 2] 5]]
           (if/get-range lmdb "t"
                         [:closed [3 0] [4 0]] :int-int :id)))
    (is (= [] (if/get-range lmdb "t"
                            [:closed [13 0] [14 0]] :int-int :id)))
    (if/close-kv lmdb)
    (u/delete-files dir)))

(deftest inmemory-test
  (testing "nil dir implies in-memory"
    (let [lmdb (l/open-kv nil)]
      (if/open-dbi lmdb "a")
      (if/transact-kv lmdb [[:put "a" 1 "hello" :long :string]
                            [:put "a" 2 "world" :long :string]])
      (is (= "hello" (if/get-value lmdb "a" 1 :long :string)))
      (is (= "world" (if/get-value lmdb "a" 2 :long :string)))
      (is (= [[1 "hello"] [2 "world"]]
             (if/get-range lmdb "a" [:all] :long :string)))
      (is (not (.exists (io/file (if/env-dir lmdb) c/data-file-name))))
      (if/close-kv lmdb)))

  (testing "explicit :inmemory? opt"
    (let [dir  (u/tmp-dir (str "inmemory-" (UUID/randomUUID)))
          lmdb (l/open-kv dir {:inmemory? true})]
      (if/open-dbi lmdb "a")
      (if/transact-kv lmdb [[:put "a" :x 42]])
      (is (= 42 (if/get-value lmdb "a" :x)))
      (is (not (.exists (io/file dir c/data-file-name))))
      (if/close-kv lmdb)
      (u/delete-files dir)))

  (testing "data does not persist across opens"
    (let [dir  (u/tmp-dir (str "inmemory-persist-" (UUID/randomUUID)))
          lmdb (l/open-kv dir {:inmemory? true})]
      (if/open-dbi lmdb "a")
      (if/transact-kv lmdb [[:put "a" :k :v]])
      (is (= :v (if/get-value lmdb "a" :k)))
      (if/close-kv lmdb)
      (let [lmdb2 (l/open-kv dir {:inmemory? true})]
        (if/open-dbi lmdb2 "a")
        (is (nil? (if/get-value lmdb2 "a" :k)))
        (if/close-kv lmdb2))
      (u/delete-files dir))))
