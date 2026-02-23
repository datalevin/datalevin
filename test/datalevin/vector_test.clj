(ns datalevin.vector-test
  (:require
   [datalevin.vector :as sut]
   [datalevin.interface :as if]
   [datalevin.util :as u]
   [datalevin.lmdb :as l]
   [datalevin.core :as d]
   [datalevin.constants :as c]
   [datalevin.test.core :refer [db-fixture]]
   [clojure.test :refer [deftest is use-fixtures]])
  (:import
   [datalevin.vector VectorIndex]
   [java.util UUID Random]))

(use-fixtures :each db-fixture)

(defn- rand-float-vec
  [^long n]
  (let [random (Random.)
        v      (float-array n)]
    (dotimes [i n] (aset v i (.nextFloat ^Random random)))
    v))

(deftest basic-ops-test
  (when-not (u/windows?)
    (let [dir   (u/tmp-dir (str "test-" (UUID/randomUUID)))
          lmdb  (d/open-kv dir)
          n     200
          v1    (rand-float-vec n)
          v2    (rand-float-vec n)
          index ^VectorIndex (sut/new-vector-index lmdb {:dimensions n})
          info  (if/vecs-info index)]
      (is (= (info :size) 0))
      (is (= (info :capacity) 0))
      (is (<= 0 (info :memory)))
      (is (string? (info :hardware)))
      (is (= (info :filename) (sut/index-fname lmdb c/default-domain)))
      (is (= (info :dimensions) n))
      (is (= (info :metric-type) c/default-metric-type))
      (is (= (info :quantization) c/default-quantization))
      (is (= (info :connectivity) c/default-connectivity))
      (is (= (info :expansion-add) c/default-expansion-add))
      (is (= (info :expansion-search) c/default-expansion-search))

      (if/add-vec index :ok v1)
      (let [info1 (if/vecs-info index)]
        (is (= (info1 :size) 1))
        (is (<= 1 (info1 :capacity)))
        (is (<= 1 (info1 :memory))))
      (is (if/vec-indexed? index :ok))
      (is (= [(vec v1)] (mapv vec (if/get-vec index :ok))))
      (is (= [:ok] (if/search-vec index v1)))
      (is (= [[:ok 0.0]] (if/search-vec index v1 {:display :refs+dists})))

      (if/close-vecs index)
      (if/close-vecs index) ;; close should be idempotent
      (is (not (u/file-exists (sut/index-fname lmdb c/default-domain))))

      (let [index1 ^VectorIndex (sut/new-vector-index lmdb {:dimensions n})
            info1  (if/vecs-info index1)]
        (is (= (info1 :size) 1))
        (is (if/vec-indexed? index1 :ok))
        (is (= [(vec v1)] (mapv vec (if/get-vec index1 :ok))))
        (is (= [:ok] (if/search-vec index1 v1)))
        (is (= [[:ok 0.0]] (if/search-vec index1 v1 {:display :refs+dists})))

        (if/add-vec index1 :nice v2)
        (let [info2 (if/vecs-info index1)]
          (is (= 2 (info2 :size))))
        (is (if/vec-indexed? index1 :nice))
        (is (= [(vec v1)] (mapv vec (if/get-vec index1 :ok))))
        (is (= [(vec v2)] (mapv vec (if/get-vec index1 :nice))))
        (is (= [:nice] (if/search-vec index1 v2 {:top 1})))
        (is (= [[:nice 0.0]] (if/search-vec index1 v2 {:top 1 :display :refs+dists})))
        (is (= [:nice :ok] (if/search-vec index1 v2)))

        (if/remove-vec index1 :ok)
        (is (= 1 (:size (if/vecs-info index1))))
        (is (not (if/vec-indexed? index1 :ok)))

        (is (= [(vec v2)] (mapv vec (if/get-vec index1 :nice))))
        (is (= [:nice] (if/search-vec index1 v2)))
        (is (= [[:nice 0.0]] (if/search-vec index1 v2 {:display :refs+dists})))

        (if/close-vecs index1))

      (let [index2 ^VectorIndex (sut/new-vector-index lmdb {:dimensions n})]
        (is (= 1 (:size (if/vecs-info index2))))
        (is (if/vec-indexed? index2 :nice))
        (is (= [(vec v2)] (mapv vec (if/get-vec index2 :nice))))
        (is (= [:nice] (if/search-vec index2 v2)))
        (is (= [[:nice 0.0]] (if/search-vec index2 v2 {:display :refs+dists})))
        (if/clear-vecs index2))
      (is (not (u/file-exists (sut/index-fname lmdb c/default-domain))))

      (let [index3 ^VectorIndex (sut/new-vector-index lmdb {:dimensions n})]
        (is (= 0 (:size (if/vecs-info index3))))
        (is (= [] (mapv vec (if/get-vec index3 :ok))))
        (is (= [] (mapv vec (if/get-vec index3 :nice))))
        (if/close-vecs index3))

      (d/close-kv lmdb)
      (u/delete-files dir))))

(deftest re-index-test
  (when-not (u/windows?)
    (let [dir   (u/tmp-dir (str "test-" (UUID/randomUUID)))
          lmdb  (d/open-kv dir)
          n     800
          v1    (rand-float-vec n)
          v2    (rand-float-vec n)
          v3    (rand-float-vec n)
          index ^VectorIndex (sut/new-vector-index lmdb {:dimensions n})
          info  (if/vecs-info index)]
      (if/add-vec index 1 v1)
      (if/add-vec index 2 v2)
      (if/add-vec index 3 v3)
      (let [new-index (if/re-index index {:dimensions   n
                                          :connectivity 32
                                          :metric-type  :cosine})
            new-info  (if/vecs-info new-index)]
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

        (is (if/vec-indexed? new-index 1))
        (is (if/vec-indexed? new-index 2))
        (is (if/vec-indexed? new-index 3))
        (is (= [(vec v1)] (mapv vec (if/get-vec new-index 1))))
        (is (= [(vec v2)] (mapv vec (if/get-vec new-index 2))))
        (is (= [(vec v3)] (mapv vec (if/get-vec new-index 3))))
        (is (= [1] (if/search-vec new-index v1 {:top 1})))
        (is (= [2] (if/search-vec new-index v2 {:top 1})))
        (is (= [3] (if/search-vec new-index v3 {:top 1})))
        (d/close-vector-index new-index))

      (d/close-kv lmdb)
      (u/delete-files dir))))

(def vec-data (->> (d/read-csv (slurp "test/data/word2vec.csv"))
                   (drop 1)
                   (reduce (fn [m [w & vs]]
                             (assoc m w (mapv Float/parseFloat vs)))
                           {})))

(def dims 300)

(deftest word2vec-test
  (when-not (u/windows?)
    (let [dir   (u/tmp-dir (str "test-" (UUID/randomUUID)))
          lmdb  (d/open-kv dir)
          index ^VectorIndex (sut/new-vector-index lmdb {:dimensions dims})]
      (doseq [[w vs] vec-data] (d/add-vec index w vs))
      (let [info (d/vector-index-info index)]
        (is (= (info :size) 277))
        (is (= (info :dimensions) dims)))
      (is (= ["king"] (d/search-vec index (vec-data "king") {:top 1})))
      (is (= ["king" "queen"] (d/search-vec index (vec-data "king") {:top 2})))

      (is (= ["man" "woman"] (d/search-vec index (vec-data "man") {:top 2})))
      (is (= ["cat" "feline" "animal"]
             (d/search-vec index (vec-data "cat") {:top 3})))
      (is (= ["physics" "science" "chemistry"]
             (d/search-vec index (vec-data "physics") {:top 3})))
      (d/close-vector-index index)
      (d/close-kv lmdb)
      (u/delete-files dir))))

(deftest vec-neighbors-fns-test
  (let [dir  (u/tmp-dir (str "vec-fns-" (UUID/randomUUID)))
        conn (d/create-conn
               dir {:chunk/id        {:db/valueType :db.type/string
                                      :db/unique    :db.unique/identity}
                    :chunk/embedding {:db/valueType :db.type/vec}}
               {:vector-opts {:dimensions  dims
                              :metric-type :cosine}})]
    (d/transact! conn [{:chunk/id        "cat"
                        :chunk/embedding (vec-data "cat")}
                       {:chunk/id        "rooster"
                        :chunk/embedding (vec-data "rooster")}
                       {:chunk/id        "jaguar"
                        :chunk/embedding (vec-data "jaguar")}
                       {:chunk/id        "animal"
                        :chunk/embedding (vec-data "animal")}
                       {:chunk/id        "physics"
                        :chunk/embedding (vec-data "physics")}
                       {:chunk/id        "chemistry"
                        :chunk/embedding (vec-data "chemistry")}
                       {:chunk/id        "history"
                        :chunk/embedding (vec-data "history")}])
    (is (= (set (d/q '[:find [?i ...]
                       :in $ ?q
                       :where
                       [(vec-neighbors $ :chunk/embedding ?q {:top 4}) [[?e _ _]]]
                       [?e :chunk/id ?i]]
                     (d/db conn) (vec-data "cat")))
           #{"cat" "jaguar" "animal" "rooster"}))
    (is (= (set (d/q '[:find [?i ...]
                       :in $ ?q
                       :where
                       [(vec-neighbors $ :chunk/embedding ?q) [[?e]]]
                       [?e :chunk/id ?i]]
                     (d/db conn) (vec-data "cat")))
           #{"cat" "jaguar" "animal" "rooster" "physics"
             "chemistry" "history"}))
    (is (= "cat" (d/q '[:find ?i .
                        :in $ ?q
                        :where
                        [(vec-neighbors $ ?q {:domains ["chunk_embedding"]
                                              :top     1})
                         [[?e]]]
                        [?e :chunk/id ?i]]
                      (d/db conn) (vec-data "cat"))))
    (is (number? (d/q '[:find ?dist .
                        :in $ ?q
                        :where
                        [(vec-neighbors $ :chunk/embedding ?q
                                        {:top 1 :display :refs+dists})
                         [[_ _ _ ?dist]]]]
                      (d/db conn) (vec-data "cat"))))
    (d/close conn)
    (u/delete-files dir)))
