(ns datalevin.storage-test
  (:require
   [datalevin.storage :as sut]
   [datalevin.util :as u]
   [datalevin.constants :as c]
   [datalevin.interface :as if]
   [datalevin.datom :as d]
   [datalevin.pipe :as p]
   [datalevin.test.core :as tdc :refer [db-fixture]]
   [clojure.test.check.generators :as gen]
   [clojure.test.check.clojure-test :as test]
   [clojure.test.check.properties :as prop]
   [clojure.test :refer [deftest use-fixtures is are]]
   [clojure.walk :as w]
   [clojure.string :as s])
  (:import
   [java.util UUID Collection]
   [java.util.concurrent LinkedBlockingQueue ConcurrentHashMap]
   [org.eclipse.collections.impl.list.mutable FastList]
   [datalevin.storage Store]
   [datalevin.datom Datom]))

(use-fixtures :each db-fixture)

(deftest basic-ops-test
  (let [dir   (u/tmp-dir (str "storage-test-" (UUID/randomUUID)))
        store (sut/open
                dir {}
                {:kv-opts {:flags (conj c/default-env-flags :nosync)}})
        pipe  (LinkedBlockingQueue.)
        res   (FastList.)]
    (is (= c/g0 (if/max-gt store)))
    (is (= 3 (if/max-aid store)))
    (is (= (merge c/entity-time-schema c/implicit-schema)
           (if/schema store)))
    (is (= c/e0 (if/init-max-eid store)))
    (is (= c/tx0 (if/max-tx store)))
    (is (if/analyze store nil))
    (let [a   :a/b
          v   (UUID/randomUUID)
          d   (d/datom c/e0 a v)
          s   (assoc (if/schema store) a {:db/aid 3})
          b   :b/c
          p1  {:db/valueType :db.type/uuid}
          v1  (UUID/randomUUID)
          d0  (d/datom c/e0 a v1)
          d1  (d/datom c/e0 b v1)
          s1  (assoc s b (merge p1 {:db/aid 4}))
          c   :c/d
          p2  {:db/valueType :db.type/ref}
          v2  (long (rand c/emax))
          d2  (d/datom c/e0 c v2)
          s2  (assoc s1 c (merge p2 {:db/aid 5}))
          dir (if/env-dir (.-lmdb ^Store store))
          t1  (if/last-modified store)]
      (is (= d0 (assoc d :v v1)))
      (is (= d0 (conj d [:v v1])))
      (is (= d0 (w/postwalk #(if (d/datom? %) (assoc % :v v1) %) d)))
      (if/load-datoms store [d])
      (is (= (inc c/tx0) (if/max-tx store)))
      (is (<= t1 (if/last-modified store)))
      (is (= s (if/schema store)))
      (is (= 1 (if/datom-count store :eav)))
      (is (= 1 (if/datom-count store :ave)))
      (is (= d (if/ea-first-datom store c/e0 a)))
      (is (= c/e0 (if/av-first-e store a v)))
      (is (= v (if/ea-first-v store c/e0 a)))
      (is (= [d] (if/av-datoms store a v)))
      (is (= [d] (if/fetch store d)))
      (is (= [d] (if/slice store :eav d d)))
      (is (if/populated? store :eav d d))
      (is (= 1 (if/size store :eav d d)))
      (is (= 1 (if/e-size store c/e0)))
      (is (= 1 (if/a-size store a)))
      (is (= 1 (if/av-size store a v)))
      (is (= 1 (if/av-range-size store a v v)))
      (is (= d (if/head store :eav d d)))
      (is (= d (if/tail store :eav d d)))
      (if/swap-attr store b merge p1)
      (if/load-datoms store [d1])
      (is (= (+ 2 c/tx0) (if/max-tx store)))
      (is (= s1 (if/schema store)))
      (is (= 2 (if/datom-count store :eav)))
      (is (= 2 (if/datom-count store :ave)))
      (is (= [] (if/slice store :eav d (d/datom c/e0 :non-exist v1))))
      ;; size is approximate: counts all datoms for keys in key range, ignoring v-range
      (is (= 2 (if/size store :eav d (d/datom c/e0 :non-exist v1))))
      (is (nil? (if/populated? store :eav d (d/datom c/e0 :non-exist v1))))
      (is (= d (if/head store :eav d d1)))
      (is (= d1 (if/tail store :eav d1 d)))
      (is (= 2 (if/size store :eav d d1)))
      (is (= 2 (if/e-size store c/e0)))
      (is (= 1 (if/a-size store b)))
      (is (= [d d1] (if/slice store :eav d d1)))
      (is (= [d d1] (if/slice store :ave d d1)))
      (is (= [d1 d] (if/rslice store :eav d1 d)))
      (is (= [d d1] (if/slice store :eav
                              (d/datom c/e0 a nil)
                              (d/datom c/e0 nil nil))))
      (is (= [d1 d] (if/rslice store :eav
                               (d/datom c/e0 b nil)
                               (d/datom c/e0 nil nil))))
      (is (= 1 (if/size-filter store :eav
                               (fn [^Datom d] (= v (.-v d)))
                               (d/datom c/e0 nil nil)
                               (d/datom c/e0 nil nil))))
      (is (= d (if/head-filter store :eav
                               (fn [^Datom d] (when (= v (.-v d)) d))
                               (d/datom c/e0 nil nil)
                               (d/datom c/e0 nil nil))))
      (is (= d (if/tail-filter store :eav
                               (fn [^Datom d]
                                 (when (= v (.-v d)) d))
                               (d/datom c/e0 nil nil)
                               (d/datom c/e0 nil nil))))
      (is (= [d] (if/slice-filter store :eav
                                  (fn [^Datom d]
                                    (when (= v (.-v d)) d))
                                  (d/datom c/e0 nil nil)
                                  (d/datom c/e0 nil nil))))
      (if/ave-tuples store pipe a
                     [[[:closed c/v0] [:closed c/vmax]]]
                     (constantly true))
      (.drainTo pipe res)
      (is (= [c/e0] (map #(aget ^objects % 0) res)))
      (.clear res)
      (if/ave-tuples store pipe b [[[:closed v1] [:closed v1]]])
      (.drainTo pipe res)
      (is (= [c/e0] (map #(aget ^objects % 0) res)))
      (.clear res)
      (is (= [d1 d] (if/rslice store :ave d1 d)))
      (is (= [d d1] (if/slice store :ave
                              (d/datom c/e0 a nil)
                              (d/datom c/e0 nil nil))))
      (is (= [d1 d] (if/rslice store :ave
                               (d/datom c/e0 b nil)
                               (d/datom c/e0 nil nil))))
      (is (= [d] (if/slice-filter store :ave
                                  (fn [^Datom d]
                                    (when (= v (.-v d)) d))
                                  (d/datom c/e0 nil nil)
                                  (d/datom c/e0 nil nil))))
      (if/swap-attr store c merge p2)
      (if/load-datoms store [d2])
      (is (= [d d1 d2] (if/e-datoms store c/e0)))
      (is (= d (if/e-first-datom store c/e0)))
      (is (= d (if/ea-first-datom store c/e0 a)))
      (is (= d1 (if/ea-first-datom store c/e0 b)))
      (is (= d2 (if/ea-first-datom store c/e0 c)))
      (is (nil? (if/ea-first-datom store c/e0 :not-exist)))
      (is (= (+ 3 c/tx0) (if/max-tx store)))
      (is (= s2 (if/schema store)))
      (is (= 3 (if/datom-count store c/eav)))
      (is (= 3 (if/datom-count store c/ave)))
      (is (= 3 (if/e-size store c/e0)))
      (is (= 1 (if/a-size store c)))
      (is (= 1 (if/v-size store v2)))
      (if/load-datoms store [(d/delete d)])
      (is (= (+ 4 c/tx0) (if/max-tx store)))
      (is (= 2 (if/datom-count store c/eav)))
      (is (= 2 (if/datom-count store c/ave)))
      (if/close store)
      (is (if/closed? store))
      (let [store (sut/open dir {}
                            {:kv-opts {:flags (conj c/default-env-flags :nosync)}})]
        (is (= (+ 4 c/tx0) (if/max-tx store)))
        (is (= [d1] (if/slice store :eav d1 d1)))
        (if/load-datoms store [(d/delete d1)])
        (is (= (+ 5 c/tx0) (if/max-tx store)))
        (is (= 1 (if/datom-count store c/eav)))
        (if/load-datoms store [d d1])
        (is (= (+ 6 c/tx0) (if/max-tx store)))
        (is (= 3 (if/datom-count store c/eav)))
        (if/close store))
      (let [d     :d/e
            p3    {:db/valueType :db.type/long}
            s3    (assoc s2 d (merge p3 {:db/aid 6}))
            s4    (assoc s3 :f/g {:db/aid 7 :db/valueType :db.type/string})
            store (sut/open dir {d p3}
                            {:kv-opts {:flags (conj c/default-env-flags :nosync)}})]
        (is (= (+ 6 c/tx0) (if/max-tx store)))
        (is (= s3 (if/schema store)))
        (if/set-schema store {:f/g {:db/valueType :db.type/string}})
        (is (= s4 (if/schema store)))
        (if/close store)))
    (u/delete-files dir)))

(deftest schema-test
  (let [s     {:a {:db/valueType :db.type/string}
               :b {:db/valueType :db.type/long}}
        dir   (u/tmp-dir (str "datalevin-schema-test-" (UUID/randomUUID)))
        store (sut/open
                dir s
                {:kv-opts {:flags (conj c/default-env-flags :nosync)}})
        s1    (if/schema store)]
    (if/close store)
    (is (if/closed? store))
    (let [store (sut/open dir s)]
      (is (= s1 (if/schema store)))
      (if/close store))
    (u/delete-files dir)))

(deftest update-schema-migrate-untyped-values-test
  (let [dir   (u/tmp-dir (str "datalevin-schema-migrate-test-" (UUID/randomUUID)))
        store (sut/open
                dir nil
                {:validate-data? true
                 :kv-opts        {:flags (conj c/default-env-flags :nosync)}})]
    (try
      (if/load-datoms store [(d/datom 1 :age (int 42))
                             (d/datom 2 :age (int 7))])
      (is (nil? (get-in (if/schema store) [:age :db/valueType])))
      (if/set-schema store {:age {:db/valueType :db.type/long}})
      (is (= :db.type/long (get-in (if/schema store) [:age :db/valueType])))
      (is (= 42 (if/ea-first-v store 1 :age)))
      (is (= 7 (if/ea-first-v store 2 :age)))
      (finally
        (if/close store)
        (u/delete-files dir)))))

(deftest update-schema-migrate-untyped-values-failure-test
  (let [dir   (u/tmp-dir (str "datalevin-schema-migrate-fail-test-" (UUID/randomUUID)))
        store (sut/open
                dir nil
                {:validate-data? true
                 :kv-opts        {:flags (conj c/default-env-flags :nosync)}})]
    (try
      (if/load-datoms store [(d/datom 1 :age (int 1))
                             (d/datom 2 :age "not-a-number")])
      (is (thrown-with-msg?
            Exception
            #"Cannot migrate attribute values to new type"
            (if/set-schema store {:age {:db/valueType :db.type/long}})))
      (is (nil? (get-in (if/schema store) [:age :db/valueType])))
      (is (= 1 (if/ea-first-v store 1 :age)))
      (is (= "not-a-number" (if/ea-first-v store 2 :age)))
      (finally
        (if/close store)
        (u/delete-files dir)))))

(deftest giants-string-test
  (let [schema {:a {:db/valueType :db.type/string}}
        dir    (u/tmp-dir (str "datalevin-giants-str-test-" (UUID/randomUUID)))
        store  (sut/open
                 dir schema
                 {:kv-opts {:flags (conj c/default-env-flags :nosync)}})
        v      (apply str (repeat 100 (UUID/randomUUID)))
        d      (d/datom c/e0 :a v)]
    (if/load-datoms store [d])
    (is (= [d] (if/fetch store d)))
    (is (= [d] (if/slice store :eav
                         (d/datom c/e0 :a c/v0)
                         (d/datom c/e0 :a c/vmax))))
    (if/close store)
    (u/delete-files dir)))

(deftest giants-data-test
  (let [dir   (u/tmp-dir (str "datalevin-giants-data-test-" (UUID/randomUUID)))
        store (sut/open
                dir nil
                {:kv-opts {:flags (conj c/default-env-flags :nosync)}})
        v     (apply str (repeat 100 (UUID/randomUUID)))
        d     (d/datom c/e0 :a v)
        d1    (d/datom (inc c/e0) :b v)]
    (if/load-datoms store [d])
    (is (= [d] (if/fetch store d)))
    (is (= [d] (if/slice store :eav
                         (d/datom c/e0 :a c/v0)
                         (d/datom c/e0 :a c/vmax))))
    (if/close store)
    (let [store' (sut/open dir nil
                           {:kv-opts {:flags (conj c/default-env-flags :nosync)}})]
      (is (if/populated? store' :eav
                         (d/datom c/e0 :a c/v0)
                         (d/datom c/e0 :a c/vmax)))
      (is (= [d] (if/fetch store' d)))
      (is (= [d] (if/slice store' :eav
                           (d/datom c/e0 :a c/v0)
                           (d/datom c/e0 :a c/vmax))))
      (if/load-datoms store' [d1])
      (is (= 1 (if/init-max-eid store')))
      (is (= [d1] (if/fetch store' d1)))
      (if/close store'))
    (u/delete-files dir)))

(deftest giants-zstd-compression-test
  (let [dir   (u/tmp-dir (str "datalevin-giants-zstd-test-" (UUID/randomUUID)))
        store (sut/open
                dir nil
                {:kv-opts {:flags (conj c/default-env-flags :nosync)}})
        v     (apply str (repeat 12000 "giant-value-"))
        d     (d/datom c/e0 :a v)]
    (try
      (if/load-datoms store [d])
      (is (= [d] (if/fetch store d)))
      (let [[gt raw] (if/get-first (.-lmdb ^Store store)
                                   c/giants [:all] :id :raw)
            sig      (mapv #(bit-and (int %) 0xFF) (take 4 raw))]
        (is (= c/g0 gt))
        (is (= [0x44 0x4C 0x47 0x5A] sig)))
      (finally
        (if/close store)
        (u/delete-files dir)))))

(deftest normal-data-test
  (let [dir   (u/tmp-dir (str "datalevin-normal-data-test-" (UUID/randomUUID)))
        store (sut/open
                dir nil
                {:kv-opts {:flags (conj c/default-env-flags :nosync)}})
        v     (UUID/randomUUID)
        d     (d/datom c/e0 :a v)
        d1    (d/datom (inc c/e0) :b v)]
    (if/load-datoms store [d])
    (is (= [d] (if/fetch store d)))
    (is (= [d] (if/slice store :eav
                         (d/datom c/e0 :a c/v0)
                         (d/datom c/e0 :a c/vmax))))
    (if/close store)

    (let [store' (sut/open dir nil
                           {:kv-opts {:flags (conj c/default-env-flags :nosync)}})]
      (is (if/populated? store' :eav
                         (d/datom c/e0 :a c/v0)
                         (d/datom c/e0 :a c/vmax)))
      (is (= [d] (if/fetch store' d)))
      (is (= [d] (if/slice store' :eav
                           (d/datom c/e0 :a c/v0)
                           (d/datom c/e0 :a c/vmax))))
      (if/load-datoms store' [d1])
      (is (= 1 (if/init-max-eid store')))
      (is (= [d1] (if/fetch store' d1)))
      (if/close store'))
    (u/delete-files dir)))

(deftest false-value-test
  (let [d     (d/datom c/e0 :a false)
        dir   (u/tmp-dir (str "storage-test-" (UUID/randomUUID)))
        store (sut/open dir nil
                        {:kv-opts {:flags (conj c/default-env-flags :nosync)}})]
    (if/load-datoms store [d])
    (is (= [d] (if/fetch store d)))
    (if/close store)
    (u/delete-files dir)))

(test/defspec random-data-test
  100
  (prop/for-all
    [v gen/any-printable-equatable
     a gen/keyword-ns
     e (gen/large-integer* {:min 0})]
    (let [d     (d/datom e a v)
          dir   (u/tmp-dir (str "storage-test-" (UUID/randomUUID)))
          store (sut/open dir {}
                          {:kv-opts
                           {:flags (conj c/default-env-flags :nosync)}})
          _     (if/load-datoms store [d])
          r     (if/fetch store d)]
      (if/close store)
      (u/delete-files dir)
      (is (= [d] r)))))

(deftest ave-tuples-test
  (let [d0    (d/datom 0 :a "0a0")
        d1    (d/datom 0 :a "0a1")
        d2    (d/datom 0 :a "0a2")
        d3    (d/datom 0 :b 7)
        d4    (d/datom 8 :a "8a")
        d5    (d/datom 8 :b 11)
        d6    (d/datom 10 :a "10a")
        d7    (d/datom 10 :b 4)
        d8    (d/datom 10 :b 15)
        d9    (d/datom 10 :b 20)
        d10   (d/datom 15 :a "15a")
        d11   (d/datom 15 :b 1)
        d12   (d/datom 20 :b 2)
        d13   (d/datom 20 :b 4)
        d14   (d/datom 20 :b 7)
        dir   (u/tmp-dir (str "storage-test-" (UUID/randomUUID)))
        store (sut/open dir
                        {:a {:db/valueType   :db.type/string
                             :db/cardinality :db.cardinality/many}
                         :b {:db/cardinality :db.cardinality/many
                             :db/valueType   :db.type/long}}
                        {:kv-opts
                         {:flags (conj c/default-env-flags :nosync)}})
        pipe  (LinkedBlockingQueue.)
        res   (FastList.)
        ]
    (if/load-datoms store [d0 d1 d2 d3 d4 d5 d6 d7 d8 d9 d10 d11 d12 d13 d14])
    (is (= 6 (if/cardinality store :a)))
    (is (= 6 (if/a-size store :a)))

    (is (= 7 (if/cardinality store :b)))
    (is (= 9 (if/a-size store :b)))

    (is (= 1 (if/av-size store :a "8a")))
    (is (= 2 (if/av-size store :b 7)))

    (is (= 5 (if/av-range-size store :b 7 20)))
    (is (= 7 (if/av-range-size store :b 4 20)))

    (are [a range pred get-v? result]
        (do (.clear res)
            (if/ave-tuples store pipe a range pred get-v?)
            (.drainTo pipe res)
            (is (= result (set (mapv vec res)))))

      :b [[[:closed 11] [:closed c/vmax]]] nil false
      (set [[8] [10]])

      :b [[[:closed 11] [:closed c/vmax]]] nil true
      (set [[8 11] [10 15] [10 20]])

      :b [[[:closed c/v0] [:closed 11]]] nil false
      (set [[15] [20] [10] [0] [8]])

      :b [[[:closed c/v0] [:closed 11]]] odd? false
      (set [[15] [0] [8] [20]])

      :b [[[:closed 11] [:open 20]]] nil false
      (set [[8] [10]])

      :b [[[:open 11] [:closed c/vmax]]] nil false
      (set [[10]])

      :b [[[:closed c/v0] [:open 11]]] nil false
      (set [[15] [20] [10] [0]])

      :b [[[:closed c/v0] [:open 11]]] odd? true
      (set [[15 1] [0 7] [20 7]])

      :b [[[:open 2] [:oen 11]]] nil false
      (set [[10] [0] [20]])

      :b [[[:open 2] [:closed 11]]] nil false
      (set [[10] [0] [8] [20]])

      :a [[[:closed "8"] [:closed c/vmax]]] nil false
      (set [[8]])

      :a [[[:closed c/v0] [:closed "10a"]]] nil false
      (set [[0] [10]])

      :a [[[:closed "10a"][:open "15a"]]] nil false
      (set [[10]])

      :a [[[:open "10a"] [:closed c/vmax]]] nil false
      (set [[15] [8]])

      :a [[[:closed c/v0] [:open "10a"]]] nil false
      (set [[0]])

      :a [[[:open "0a0"] [:open "15a"]]] nil false
      (set [[0] [10]])

      :a [[[:open "0a0"] [:closed "15a"]]] nil false
      (set [[0] [10] [15]]))
    (if/close store)
    (u/delete-files dir)
    ))

(deftest eav-scan-v-test
  (let [d0      (d/datom 0 :a 10)
        d1      (d/datom 5 :a 1)
        d2      (d/datom 5 :b "5b")
        d3      (d/datom 8 :a 7)
        d4      (d/datom 8 :b "8b")
        d5      (d/datom 10 :b "10b")
        d6      (d/datom 10 :c :c10)
        d7      (d/datom 10 :d "Tom")
        d8      (d/datom 10 :d "Jerry")
        d9      (d/datom 10 :d "Mick")
        d10     (d/datom 10 :e "nice")
        d11     (d/datom 10 :e "good")
        d12     (d/datom 12 :f 2.2)
        d13     (d/datom 5 :b "13b")
        tuples0 [(object-array [0]) (object-array [5]) (object-array [8])]
        tuples1 [(object-array [:none 0])
                 (object-array [:nada 8])
                 (object-array [:zero 10])]
        tuples2 [(object-array [8]) (object-array [5]) (object-array [8])]
        tuples3 [(object-array [10]) (object-array [5]) (object-array [10])]
        tuples4 [(object-array [10]) (object-array [0])]
        dir     (u/tmp-dir (str "storage-test-" (UUID/randomUUID)))
        store   (sut/open dir
                          {:a {}
                           :b {:db/valueType   :db.type/string
                               :db/cardinality :db.cardinality/many}
                           :c {:db/valueType :db.type/keyword}
                           :d {:db/cardinality :db.cardinality/many
                               :db/valueType   :db.type/string}
                           :e {:db/cardinality :db.cardinality/many
                               :db/valueType   :db.type/string}}
                          {:kv-opts
                           {:flags (conj c/default-env-flags :nosync)}})
        in      (p/tuple-pipe)
        out     (FastList.)]
    (if/load-datoms store [d0 d1 d2 d3 d4 d5 d6 d7 d8 d9 d10 d11 d12 d13])

    (are [tuples eid-idx attrs-v result]
        (do (.clear out)
            (p/reset in)
            (.addAll ^Collection in tuples)
            (p/finish in)
            (if/eav-scan-v store in out eid-idx attrs-v)
            (is (= (set result) (set (mapv vec out)))))

      tuples0 0 [[:b {:pred nil :skip? false}]]
      [[5 "5b"] [5 "13b"] [8 "8b"]]

      tuples0 0 [[:b {:pred nil :skip? false}] [:b {:pred nil :skip? false}]]
      [[5 "5b" "5b"] [5 "13b" "5b"] [5 "13b" "13b"] [5 "5b" "13b"] [8 "8b" "8b"]]

      tuples0 0 [[:b {:pred #(s/starts-with? % "1") :skip? false}]
                 [:b {:pred nil :skip? false}]]
      [ [5 "13b" "5b"] [5 "13b" "13b"]]

      tuples0 0 [[:b {:pred nil :skip? false}] [:b {:pred nil :skip? true}]]
      [[5 "5b"] [5 "13b"] [8 "8b"]]

      tuples0 0 [[:a {:pred #(< ^long % 5) :skip? false}]]
      [[5 1]]

      tuples0 0 [[:a {:pred #(< ^long % 5) :skip? true}]]
      [[5]]

      tuples0 0 [[:a {:pred (constantly true) :skip? false}]
                 [:b {:pred (constantly true) :skip? true}]]
      [[5 1] [8 7]]

      tuples0 0 [[:a {:pred (constantly true) :skip? false}]
                 [:b {:pred #(s/starts-with? % "8") :skip? true}]]
      [[8 7]]

      tuples0 0 [[:a {:pred (constantly true) :skip? true}]
                 [:b {:pred (constantly true) :skip? true}]]
      [[5] [8]]

      tuples0 0 [[:a {:pred (constantly true) :skip? false}]
                 [:b {:pred (constantly true) :skip? false}]]
      [[5 1 "5b"] [5 1 "13b"] [8 7 "8b"]]

      tuples0 0 [[:a {:skip? false}] [:b {:skip? false}]]
      [[5 1 "5b"] [5 1 "13b"] [8 7 "8b"]]

      tuples0 0 [[:a {:skip? false}] [:b {:skip? true}]]
      [[5 1] [8 7]]

      tuples0 0 [[:a {:pred odd? :skip? false}]
                 [:b {:pred #(s/ends-with? % "b") :skip? false}]]
      [[5 1 "5b"] [5 1 "13b"] [8 7 "8b"]]

      tuples0 0 [[:a {:pred odd? :skip? false}] [:b {:skip? false}]]
      [[5 1 "5b"] [5 1 "13b"] [8 7 "8b"]]

      tuples0 0 [[:a {:pred odd? :skip? false}]
                 [:b {:pred (constantly true) :skip? false}]]
      [[5 1 "5b"] [5 1 "13b"] [8 7 "8b"]]

      tuples0 0 [[:a {:pred even? :skip? false}]
                 [:b {:pred (constantly true) :skip? false}]]
      []

      tuples0 0 [[:a {:pred even? :skip? false}]]
      [[0 10]]

      tuples1 1 [[:a {:pred even? :skip? false}]]
      [[:none 0 10]]

      tuples1 1 [[:a {:skip? false}]]
      [[:none 0 10] [:nada 8 7]]

      tuples1 1 [[:b {:skip? false}] [:c {:skip? false}]]
      [[:zero 10 "10b" :c10]]

      tuples1 1 [[:d {:skip? false}]]
      [[:zero 10 "Jerry"] [:zero 10 "Mick"] [:zero 10 "Tom"]]

      tuples1 1 [[:d {:skip? true}]]
      [[:zero 10]]

      tuples1 1 [[:d {:pred #(< (count %) 4) :skip? false}]]
      [[:zero 10 "Tom"]]

      tuples1 1 [[:c {:skip? false}]
                 [:d {:pred #(< (count %) 4) :skip? false}]]
      [[:zero 10 :c10 "Tom"]]

      tuples1 1 [[:c {:skip? true}]
                 [:d {:pred #(< (count %) 4) :skip? false}]]
      [[:zero 10 "Tom"]]

      tuples2 0 [[:a {:skip? false}] [:b {:skip? false}]]
      [[8 7 "8b"] [5 1 "5b"] [5 1 "13b"]]

      tuples3 0 [[:c {:skip? false}] [:d {:skip? false}]]
      [[10 :c10 "Jerry"] [10 :c10 "Mick"] [10 :c10 "Tom"]
       [10 :c10 "Jerry"] [10 :c10 "Mick"] [10 :c10 "Tom"]]

      tuples4 0 [[:d {:skip? false}] [:e {:skip? false}]]
      [[10 "Jerry" "good"] [10 "Jerry" "nice"]
       [10 "Mick" "good"] [10 "Mick" "nice"]
       [10 "Tom" "good"] [10 "Tom" "nice"]]

      tuples4 0 [[:d {:skip? true}] [:e {:skip? false}]]
      [[10 "good"] [10 "nice"]]

      tuples4 0 [[:d {:skip? false}] [:e {:skip? true}]]
      [[10 "Jerry"] [10 "Mick"] [10 "Tom"] ]

      tuples4 0 [[:d {:skip? true}] [:e {:skip? true}]]
      [[10]])
    (if/close store)
    (u/delete-files dir)))

(deftest val-eq-scan-e-test
  (let [d0      (d/datom 0 :b "GPT")
        d1      (d/datom 5 :a 1)
        d2      (d/datom 5 :b "AI")
        d3      (d/datom 8 :a 7)
        d4      (d/datom 8 :b "AGI")
        d5      (d/datom 8 :a 2)
        d6      (d/datom 8 :a 1)
        d7      (d/datom 9 :b "AI")
        d8      (d/datom 10 :b "AI")
        tuples0 [(object-array [1]) (object-array [2])]
        tuples1 [(object-array [:none "GPT"])
                 (object-array [:zero "AI"])]
        tuples2 [(object-array [1]) (object-array [2]) (object-array [1])]
        dir     (u/tmp-dir (str "storage-test-" (UUID/randomUUID)))
        store   (sut/open dir
                          {:a {:db/valueType   :db.type/ref
                               :db/cardinality :db.cardinality/many}
                           :b {:db/valueType :db.type/string}}
                          {:kv-opts
                           {:flags (conj c/default-env-flags :nosync)}})
        in      (p/tuple-pipe)
        out     (FastList.)]
    (if/load-datoms store [d0 d1 d2 d3 d4 d5 d6 d7 d8])

    (are [tuples veid-idx attr result]
        (do (.clear out)
            (p/reset in)
            (.addAll ^Collection in tuples)
            (p/finish in)
            (if/val-eq-scan-e store in out veid-idx attr)
            (is (= result (mapv vec out))))

      tuples0 0 :c
      []

      tuples0 0 :a
      [[1 5] [1 8] [2 8]]

      tuples1 1 :b
      [[:none "GPT" 0] [:zero "AI" 5] [:zero "AI" 9] [:zero "AI" 10]]

      tuples2 0 :a
      [[1 5] [1 8] [2 8][1 5] [1 8]]
      )
    (if/close store)
    (u/delete-files dir)))

(deftest search-tuples-test
  (let [d0    (d/datom 0 :a "0a0")
        d1    (d/datom 0 :a "0a1")
        d2    (d/datom 0 :a "0a2")
        d3    (d/datom 0 :b 7)
        d4    (d/datom 8 :a "8a")
        d5    (d/datom 8 :b 11)
        d6    (d/datom 10 :a "10a")
        d7    (d/datom 10 :b 4)
        d8    (d/datom 10 :b 15)
        d9    (d/datom 10 :b 20)
        d10   (d/datom 15 :a "15a")
        d11   (d/datom 15 :b 1)
        d12   (d/datom 20 :b 2)
        d13   (d/datom 20 :b 4)
        d14   (d/datom 20 :b 7)
        dir   (u/tmp-dir (str "storage-test-" (UUID/randomUUID)))
        store (sut/open dir
                        {:a {:db/valueType   :db.type/string
                             :db/cardinality :db.cardinality/many}
                         :b {:db/cardinality :db.cardinality/many
                             :db/valueType   :db.type/long}}
                        {:kv-opts
                         {:flags (conj c/default-env-flags :nosync)}})]
    (if/load-datoms store [d0 d1 d2 d3 d4 d5 d6 d7 d8 d9 d10 d11 d12 d13 d14])
    (is (= (map vec (sut/all-tuples store))
           [[0 :a "0a0"]
            [0 :a "0a1"]
            [0 :a "0a2"]
            [0 :b 7]
            [8 :a "8a"]
            [8 :b 11]
            [10 :a "10a"]
            [10 :b 4]
            [10 :b 15]
            [10 :b 20]
            [15 :a "15a"]
            [15 :b 1]
            [20 :b 2]
            [20 :b 4]
            [20 :b 7]]))
    (is (= (map vec (sut/ea-tuples store 8 :b)) [[11]]))
    (is (= (map vec (sut/ea-tuples store 10 :b)) [[4] [15] [20]]))
    (is (= (map vec (sut/ev-tuples store 0 7)) [[:b]]))
    (is (= (map vec (sut/ev-tuples store 15 "15a")) [[:a]]))
    (is (= (map vec (sut/e-tuples store 8)) [[:a "8a"] [:b 11]]))
    (is (= (map vec (sut/e-tuples store 20)) [[:b 2] [:b 4] [:b 7]]))
    (is (= (map vec (sut/av-tuples store :b 7)) [[0] [20]]))
    (is (= (map vec (sut/av-tuples store :a "0a1")) [[0]]))
    (is (= (set (map vec (sut/a-tuples store :b)))
           (set [[0 7] [8 11] [10 4] [10 15] [10 20]
                 [15 1] [20 2] [20 4] [20 7]])))
    (is (= (map vec (sut/v-tuples store "0a1")) [[0 :a]]))
    (is (= (map vec (sut/v-tuples store 7)) [[0 :b] [20 :b]]))

    (if/close store)
    (u/delete-files dir)))

(deftest sampling-test
  (let [dir     (u/tmp-dir (str "sampling-test-" (UUID/randomUUID)))
        store   (sut/open dir
                          {:a {:db/valueType :db.type/long}
                           :b {:db/valueType :db.type/long}}
                          {:kv-opts {:flags (conj c/default-env-flags :nosync)}})
        aid-a   (-> (if/schema store) :a :db/aid)
        size-a  5000
        csize-a 1000
        aid-b   (-> (if/schema store) :b :db/aid)
        size-b  10
        csize-b 5
        counts  #(.get ^ConcurrentHashMap (.-counts ^Store store) %)]
    (is (nil? (counts aid-a)))
    (if/load-datoms store (mapv #(d/datom % :a %) (range 1 (inc size-a))))
    (is (nil? (counts aid-a)))
    (is (= size-a (if/a-size store :a)))
    (let [sample (mapv #(aget ^objects % 0) (sut/e-sample* store :a aid-a))
          ratio  (sut/default-ratio* store :a aid-a)]
      (is (= 1.0 ^double ratio))
      (is (= size-a (counts aid-a)))
      (is (= c/init-exec-size-threshold (count sample)))
      (if/load-datoms store (mapv #(d/datom % :a %)
                                  (range (inc size-a) (+ size-a (inc csize-a)))))
      (is (= (+ size-a csize-a) (if/a-size store :a)))
      (is (= size-a (counts aid-a)))
      (let [new-sample (mapv #(aget ^objects % 0) (sut/e-sample* store :a aid-a))]
        (is (= (+ size-a csize-a) (counts aid-a)))
        (is (not= sample new-sample))
        (is (<= (apply max sample) (apply max new-sample)))))

    (.remove ^ConcurrentHashMap (.-counts ^Store store) aid-a)
    (is (nil? (counts aid-b)))
    (if/load-datoms store (mapv #(d/datom % :b %) (range 1 (inc size-b))))
    (is (nil? (counts aid-b)))
    (let [sample (mapv #(aget ^objects % 0) (sut/e-sample* store :b aid-b))]
      (is (= size-b (counts aid-b)))
      (is (= size-b (count sample)))
      (is (= sample (range 1 (inc size-b))))
      (if/load-datoms store (mapv #(d/datom % :b %)
                                  (range (inc size-b) (+ size-b (inc csize-b)))))
      (is (= size-b (counts aid-b)))
      (is (= (+ size-b csize-b) (if/a-size store :b)))
      (let [new-sample (mapv #(aget ^objects % 0) (sut/e-sample* store :b aid-b))]
        (is (= (+ size-b csize-b) (counts aid-b)))
        (is (= (concat sample (range (inc size-b) (+ size-b (inc csize-b))))
               new-sample))))
    (if/close store)
    (u/delete-files dir)))
