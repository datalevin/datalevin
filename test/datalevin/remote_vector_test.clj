(ns datalevin.remote-vector-test
  (:require
   [datalevin.core :as d]
   [datalevin.lmdb :as l]
   [datalevin.interface :as i]
   [datalevin.util :as u]
   [datalevin.constants :as c]
   [datalevin.test.core :refer [server-fixture]]
   [clojure.test :refer [is deftest use-fixtures]])
  (:import
   [java.util Random]))

(use-fixtures :each server-fixture)

(defn- rand-float-vec
  [^long n]
  (let [random (Random.)
        v      (float-array n)]
    (dotimes [i n] (aset v i (.nextFloat ^Random random)))
    v))

(deftest basic-ops-test
  (when-not (u/windows?)
    (let [dir   "dtlv://datalevin:datalevin@localhost/remote-vector-test"
          lmdb  (d/open-kv dir)
          n     200
          v1    (rand-float-vec n)
          v2    (rand-float-vec n)
          index (d/new-vector-index lmdb {:dimensions n})
          info  (d/vector-index-info index)]
      (is (= (info :size) 0))
      (is (= (info :capacity) 0))
      (is (<= 0 (info :memory)))
      (is (string? (info :hardware)))
      (is (= (info :dimensions) n))
      (is (= (info :metric-type) c/default-metric-type))
      (is (= (info :quantization) c/default-quantization))
      (is (= (info :connectivity) c/default-connectivity))
      (is (= (info :expansion-add) c/default-expansion-add))
      (is (= (info :expansion-search) c/default-expansion-search))

      (d/add-vec index :ok v1)
      (let [info1 (d/vector-index-info index)]
        (is (= (info1 :size) 1))
        (is (<= 1 (info1 :capacity)))
        (is (<= 1 (info1 :memory))))
      (d/force-vec-checkpoint! index)
      (let [cp-state (d/vector-checkpoint-state index)]
        (is (pos? (:chunk-count cp-state)))
        (is (pos? (:total-bytes cp-state)))
        (is (<= 1 (:vec-checkpoint-count cp-state)))
        (is (<= 0 (:vec-checkpoint-duration-ms cp-state)))
        (is (pos? (:vec-checkpoint-bytes cp-state)))
        (is (= 0 (:vec-checkpoint-failure-count cp-state)))
        (is (nil? (:vec-replay-lag-lsn cp-state))))
      (is (= [:ok] (d/search-vec index v1)))
      (is (= [[:ok 0.0]] (d/search-vec index v1 {:display :refs+dists})))

      (d/close-vector-index index)
      (d/close-vector-index index) ;; close should be idempotent

      (let [index1 (d/new-vector-index lmdb {:dimensions n})
            info1  (d/vector-index-info index1)]
        (is (= (info1 :size) 1))
        (is (= [:ok] (d/search-vec index1 v1)))
        (is (= [[:ok 0.0]] (d/search-vec index1 v1 {:display :refs+dists})))

        (d/add-vec index1 :nice v2)
        (let [info2 (d/vector-index-info index1)]
          (is (= 2 (info2 :size))))
        (is (= [:nice] (d/search-vec index1 v2 {:top 1})))
        (is (= [[:nice 0.0]] (d/search-vec index1 v2 {:top 1 :display :refs+dists})))
        (is (= [:nice :ok] (d/search-vec index1 v2)))

        (d/remove-vec index1 :ok)
        (is (= 1 (:size (d/vector-index-info index1))))
        (is (= [:nice] (d/search-vec index1 v2)))
        (is (= [[:nice 0.0]] (d/search-vec index1 v2 {:display :refs+dists})))

        (d/close-vector-index index1))

      (let [index2 (d/new-vector-index lmdb {:dimensions n})]
        (is (= 1 (:size (d/vector-index-info index2))))
        (is (= [:nice] (d/search-vec index2 v2)))
        (is (= [[:nice 0.0]] (d/search-vec index2 v2 {:display :refs+dists})))
        (d/clear-vector-index index2))

      (let [index3 (d/new-vector-index lmdb {:dimensions n})]
        (is (= 0 (:size (d/vector-index-info index3))))
        (d/close-vector-index index3))

      (d/close-kv lmdb))))

(deftest re-index-test
  (when-not (u/windows?)
    (let [dir   "dtlv://datalevin:datalevin@localhost/remote-vector-test"
          lmdb  (d/open-kv dir)
          n     800
          v1    (rand-float-vec n)
          v2    (rand-float-vec n)
          v3    (rand-float-vec n)
          index (d/new-vector-index lmdb {:dimensions n})
          info  (d/vector-index-info index)]
      (d/add-vec index 1 v1)
      (d/add-vec index 2 v2)
      (d/add-vec index 3 v3)
      (let [new-index (i/re-index index {:dimensions   n
                                         :connectivity 32
                                         :metric-type  :cosine})
            new-info  (d/vector-index-info new-index)]
        (is (= (new-info :size) 3))
        (is (<= 3 (new-info :capacity)))
        (is (<= 0 (new-info :memory)))
        (is (= (info :hardware) (new-info :hardware)))
        (is (= (info :filename) (new-info :filename)))
        (is (= (info :dimensions) (new-info :dimensions)))
        (is (= (new-info :metric-type) :cosine))
        (is (= (info :quantization) (new-info :quantization)))
        (is (= (new-info :connectivity) 32))
        (is (= (info :expansion-add) (new-info :expansion-add)))
        (is (= (info :expansion-search) (new-info :expansion-search)))

        (is (= [1] (d/search-vec new-index v1 {:top 1})))
        (is (= [2] (d/search-vec new-index v2 {:top 1})))
        (is (= [3] (d/search-vec new-index v3 {:top 1})))
        (d/close-vector-index new-index))

      (d/close-kv lmdb))))
