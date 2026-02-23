(ns datalevin.lmdb-test
  (:require
   [clojure.java.io :as io]
   [datalevin.lmdb :as l]
   [datalevin.bits :as b]
   [datalevin.interpret :as i]
   [datalevin.interface :as if]
   [datalevin.util :as u]
   [datalevin.core :as dc]
   [datalevin.constants :as c]
   [datalevin.datom :as d]
   [datalevin.test.core :as tdc :refer [db-fixture]]
   [clojure.string :as s]
   [clojure.test :refer [deftest testing is use-fixtures]]
   [clojure.test.check.generators :as gen]
   [clojure.test.check.clojure-test :as test]
   [clojure.test.check.properties :as prop])
  (:import
   [java.util UUID Arrays]
   [java.lang Long]))

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

(deftest inmemory-ops-test
  (let [flags (conj c/default-env-flags :inmemory)
        lmdb  (l/open-kv nil {:flags flags})]
    (try
      (if/open-dbi lmdb "a")
      (if/transact-kv lmdb [[:put "a" 1 "one"]
                            [:put "a" 2 "two"]])
      (is (= "one" (if/get-value lmdb "a" 1)))
      (is (contains? (if/get-env-flags lmdb) :inmemory))
      (is (contains? (if/get-env-flags lmdb) :nosync))
      (is (false? (:kv-wal? (if/env-opts lmdb))))
      (is (nil? (if/env-dir lmdb)))
      (is (nil? (if/sync lmdb)))
      (let [lmdb-other (l/open-kv nil {:flags flags})]
        (try
          (if/open-dbi lmdb-other "a")
          (is (nil? (if/get-value lmdb-other "a" 1)))
          (finally
            (if/close-kv lmdb-other))))
      (let [readers (doall (repeatedly 4 #(future (if/get-value lmdb "a" 2))))]
        (is (every? #(= "two" @%) readers)))
      (let [reader (future (dotimes [_ 100] (if/get-value lmdb "a" 1)) true)]
        (if/transact-kv lmdb [[:put "a" 1 "one-updated"]])
        (is @reader)
        (is (= "one-updated" (if/get-value lmdb "a" 1))))
      (if/close-kv lmdb)
      (let [lmdb2 (l/open-kv nil {:flags flags})]
        (if/open-dbi lmdb2 "a")
        (is (nil? (if/get-value lmdb2 "a" 1)))
        (if/close-kv lmdb2))
      (finally
        (when-not (if/closed-kv? lmdb)
          (if/close-kv lmdb))))))

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
        lmdb (l/open-kv dir {:flags   (conj c/default-env-flags :nosync)
                             :kv-wal? false})]
    (if/open-dbi lmdb "a")
    (if/transact-kv lmdb [[:put "a" :old 1]])
    (is (= 1 (if/get-value lmdb "a" :old)))
    (let [res (future
                (let [lmdb2 (l/open-kv
                              dir
                              {:flags   (conj c/default-env-flags :nosync)
                               :kv-wal? false})]
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
                    (vswap! joins #(s/join " " [% k]))))]
    (if/open-list-dbi lmdb "list" #_{:flags #{:create :counted :dupsort}})
    (is (if/list-dbi? lmdb "list"))

    (if/put-list-items lmdb "list" "a" [1 2 3 4] :string :long)
    (if/put-list-items lmdb "list" "b" [5 6 7] :string :long)
    (if/put-list-items lmdb "list" "c" [3 6 9] :string :long)

    (is (= (if/entries lmdb "list") 10))

    (is (= (if/key-range-count lmdb "list" [:all]) 3))
    (is (= (if/key-range-count lmdb "list" [:all] :string) 3))

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

    ;; list-range-count is approximate: value-range bounds are ignored.
    (is (= 10 (if/list-range-count lmdb "list" [:all] :string
                                   [:closed 2 4] :long)))
    (is (= 7 (if/list-range-count lmdb "list" [:less-than "c"] :string
                                  [:closed 2 4] :long)))
    (is (= 6 (if/list-range-count lmdb "list" [:greater-than "a"] :string
                                  [:closed 2 4] :long)))

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
                             :kv-wal? false
                             :flags   (conj c/default-env-flags :nosync)})
        data {:description "this is going to be bigger than 1 MB"
              :numbers     (range 500000)}]
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

(deftest open-again-resized-test
  (let [dir  (u/tmp-dir (str "again-resize-" (UUID/randomUUID)))
        lmdb (l/open-kv dir {:mapsize 1
                             :kv-wal? false
                             :flags   (conj c/default-env-flags :nosync)})
        data {:description "this is normal data"}]

    (if/open-dbi lmdb "a")
    (if/transact-kv lmdb [[:put "a" 0 data]])
    (is (= data (if/get-value lmdb "a" 0)))
    (if/close-kv lmdb)

    (let [lmdb1 (l/open-kv dir {:mapsize 10 :kv-wal? false})]
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
