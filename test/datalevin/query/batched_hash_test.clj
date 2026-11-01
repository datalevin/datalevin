(ns datalevin.query.batched-hash-test
  (:require [clojure.test :refer [deftest is testing]]
            [datalevin.constants :as c]
            [datalevin.core :as d]
            [datalevin.pipe :as pipe]
            [datalevin.query.plan :as plan])
  (:import [org.eclipse.collections.impl.list.mutable FastList]))

(def schema
  {:target/key {:db/valueType :db.type/long}
   :target/other {:db/valueType :db.type/long}
   :target/keep {:db/valueType :db.type/boolean}})

(defn tuples [rows]
  (let [out (FastList.)]
    (doseq [row rows] (.add out (object-array row)))
    out))

(defn hash-step [n]
  (let [init-cols ['?e #{:target/key '?k}]
        cols (conj init-cols #{:target/other '?s})]
    (plan/map->HashJoinStep
      {:link {:type :val-eq :var '?k :tgt '?e}
       :in-cols ['?input '?k '?s] :in-size 10 :tgt-size n
       :tgt-steps [(plan/map->InitStep
                     {:attr :target/key :vars ['?e '?k] :cols init-cols :mcount n})
                   (plan/map->MergeScanStep
                     {:index 0 :cols cols
                      :attrs-v [[:target/other {:skip? false}]
                                [:target/keep {:skip? true :pred true?}]]})]})))

(defn run-join [database step rows probes? pipeline?]
  (binding [c/hash-join-batched-probes? probes?
            c/hash-join-min-input-size 40
            plan/*explain* (volatile! {})]
    (let [input (tuples rows)
          result (if pipeline?
                   (let [out (FastList.)]
                     (plan/step-execute-pipe step database
                                             (pipe/list-tuple-pipe input) out)
                     out)
                   (plan/step-execute step database input))]
      {:rows (frequencies (map vec result))
       :decision (first (:batched-hash-targets @plan/*explain*))})))

(defn compare-paths [database step rows expected-used?]
  (doseq [pipeline? [false true]]
    (let [full (run-join database step rows false pipeline?)
          auto (run-join database step rows true pipeline?)]
      (is (= (:rows full) (:rows auto)))
      (is (= expected-used? (get-in auto [:decision :used?])))))

(deftest sparse-batched-hash-test
  (with-open [conn (d/create-conn nil schema {:background-sampling? false})]
    (d/transact! conn
                 (concat
                   (for [k (range 256) s (range 2)]
                     {:target/key k :target/other s :target/keep (zero? s)})
                   (repeat 3 {:target/key 1 :target/other 0 :target/keep true})
                   (for [k [Long/MIN_VALUE -1 Long/MAX_VALUE]]
                     {:target/key k :target/other 0 :target/keep true})))
    (let [step (hash-step 518)]
      (testing "all keys, target predicates, duplicate input and target rows"
        (let [rows [[10 (int 1) 0] [10 (int 1) 0] [11 1 1] [12 2 0]]
              result (run-join @conn step rows true false)]
          (is (= 9 (reduce + (vals (:rows result)))))
          (is (= 7 (get-in result [:decision :candidate-rows])))
          (is (= 5 (get-in result [:decision :target-rows])))
          (compare-paths @conn step rows true)))
      (testing "the entire signed long range is retained"
        (compare-paths @conn step
                       [[1 Long/MIN_VALUE 0] [2 -1 0] [3 Long/MAX_VALUE 0]] true))
      (testing "empty input does not read a target"
        (compare-paths @conn step [] nil))
      (testing "unsupported numeric representations use normal hash semantics"
        (doseq [key [nil 1.0 1N (biginteger 1)]]
          (compare-paths @conn step [[1 key 0]] false)))
      (testing "dense domains and bounded domains fall back"
        (compare-paths @conn step (mapv #(vector % % 0) (range 256)) false)
        (binding [c/hash-join-max-probe-keys 1]
          (compare-paths @conn step [[1 1 0] [2 2 0]] false)))
      (testing "unsupported target forms keep the full target"
        (binding [c/hash-join-min-input-size 40]
          (doseq [s [(assoc step :in-cols ['?input '?k '?unrelated])
                     (assoc step :tgt-size 10)
                     (assoc-in step [:tgt-steps 0 :pred] pos?)
                     (assoc-in step [:tgt-steps 0 :range] [[[:closed 1] [:closed 3]]])
                     (assoc-in step [:tgt-steps 0 :val] 1)
                     (assoc-in step [:tgt-steps 0 :know-e?] true)
                     (assoc-in step [:tgt-steps 0 :result] (tuples [[1 1]]))
                     (assoc-in step [:tgt-steps 1 :result] (tuples [[1 1 0]]))]]
            (is (nil? (plan/batched-hash-target @conn s))))))))))

(deftest skewed-batched-hash-fallback-test
  (with-open [conn (d/create-conn nil schema {:background-sampling? false})]
    (d/transact! conn (for [s (range 256)]
                       {:target/key 1 :target/other s :target/keep true}))
    (let [step (hash-step 256) rows [[7 1 0]]
          result (run-join @conn step rows true false)]
      (is (= :dense-domain (get-in result [:decision :reason])))
      (is (= 256 (get-in result [:decision :candidate-rows])))
      (compare-paths @conn step rows false))))

(deftest many-valued-batched-hash-test
  (with-open [conn (d/create-conn nil
                                  (assoc-in schema [:target/key :db/cardinality]
                                            :db.cardinality/many)
                                  {:background-sampling? false})]
    (d/transact! conn (for [k (range 128)]
                       {:target/key [(* 2 k) (inc (* 2 k))]
                        :target/other 0 :target/keep true}))
    (compare-paths @conn (hash-step 256) [[1 0 0] [2 1 0] [2 1 0]] true)))

(deftest indexed-key-types-test
  (doseq [[props key used?]
          [[{:db/valueType :db.type/ref} 1 true]
           [{:db/valueType :db.type/long :db/noindex true} 1 nil]
           [{:db/valueType :db.type/string} "1" nil]]]
    (with-open [conn (d/create-conn nil (assoc schema :target/key props)
                                    {:background-sampling? false})]
      (d/transact! conn
                   (for [k (range 1 257)]
                     {:db/id k
                      :target/key (if (string? key) (str k) k)
                      :target/other 0 :target/keep true}))
      (compare-paths @conn (hash-step 256) [[7 key 0]] used?))))

(deftest simulated-transaction-batched-hash-test
  (with-open [conn (d/create-conn nil schema {:background-sampling? false})]
    (d/transact! conn (for [k (range 256)]
                       {:target/key k :target/other 0 :target/keep true}))
    (let [database (d/db-with @conn [{:target/key 1000 :target/other 0
                                     :target/keep true}])
          step (hash-step 257)
          rows [[7 1000 0] [8 1 0]]]
      (is (= 2 (count (:rows (run-join database step rows true false)))))
      (compare-paths database step rows true))))
