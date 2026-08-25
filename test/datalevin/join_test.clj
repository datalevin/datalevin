;;
;; Copyright (c) Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you agree to be bound by its terms.
;; You must not remove this notice.
;;

(ns datalevin.join-test
  (:require
   [clojure.test :refer [deftest is testing]]
   [datalevin.join :as j]
   [datalevin.query-util :as qu]
   [datalevin.relation :as r])
  (:import
   [java.util Random]
   [org.eclipse.collections.impl.list.mutable FastList]))

(defn- relation
  [attrs rows]
  (let [tuples (FastList.)]
    (.addAll ^FastList tuples ^java.util.Collection (mapv object-array rows))
    (r/relation! attrs tuples)))

(defn- row-set
  [rel]
  (into #{} (map vec) (:tuples rel)))

(defn- expected-projected-join
  [left right vars]
  (-> (j/hash-join left right)
      (r/project-distinct vars)
      (row-set)))

(deftest hash-join-project-distinct-test
  (let [left  (relation {'?x 0 '?z 1}
                        [[1 10] [1 11] [2 10] [2 10]])
        right (relation {'?z 0 '?y 1}
                        [[10 :a] [10 :b] [11 :a] [11 :a]])]
    (testing "relation composition is exact in either input orientation"
      (doseq [[a b] [[left right] [right left]]]
        (is (= (expected-projected-join a b ['?x '?y])
               (row-set (j/hash-join-project-distinct a b ['?x '?y]))))))

    (testing "one-sided projections remain distinct"
      (doseq [vars [['?x] ['?y] ['?z] ['?x '?z '?y]]]
        (is (= (expected-projected-join left right vars)
               (row-set (j/hash-join-project-distinct left right vars))))))

    (testing "logical lookup-ref equality uses the conservative distinct path"
      (binding [qu/*lookup-attrs* #{'?x}]
        (is (= (expected-projected-join left right ['?x '?y])
               (row-set
                 (j/hash-join-project-distinct left right ['?x '?y]))))))

    (testing "cartesian products and empty inputs retain normal join semantics"
      (let [xs    (relation {'?x 0} [[1] [2]])
            ys    (relation {'?y 0} [[:a] [:b]])
            empty (relation {'?x 0} [])]
        (is (= (expected-projected-join xs ys ['?x '?y])
               (row-set (j/hash-join-project-distinct xs ys ['?x '?y]))))
        (is (= #{}
               (row-set (j/hash-join-project-distinct empty ys
                                                      ['?x '?y]))))))))

(deftest randomized-hash-join-project-distinct-test
  (let [rng  (Random. 739391)
        vars [['?x '?y] ['?x] ['?y] ['?z] ['?k] ['?x '?z '?y]]]
    (dotimes [_ 100]
      (let [left-count  (.nextInt rng 40)
            right-count (.nextInt rng 40)
            left        (relation
                          {'?x 0 '?z 1 '?k 2}
                          (repeatedly left-count
                                      #(vector (.nextInt rng 8)
                                               (.nextInt rng 5)
                                               (.nextInt rng 3))))
            right       (relation
                          {'?z 0 '?k 1 '?y 2}
                          (repeatedly right-count
                                      #(vector (.nextInt rng 5)
                                               (.nextInt rng 3)
                                               (.nextInt rng 8))))]
        (doseq [projected vars]
          (is (= (expected-projected-join left right projected)
                 (row-set
                   (j/hash-join-project-distinct left right projected)))))))))

(deftest dense-binary-composition-test
  (let [n     64
        left  (relation {'?x 0 '?z 1}
                        (for [x (range n), z (range n)] [x z]))
        right (relation {'?z 0 '?y 1}
                        (for [z (range n), y (range n)] [z y]))
        selected (#'j/dense-binary-composition left right ['?x '?y])
        actual   (j/hash-join-project-distinct left right ['?x '?y])]
    (is (some? selected))
    (is (= (* n n) (.size ^java.util.List (:tuples selected))))
    (is (= (set (for [x (range n), y (range n)] [x y]))
           (row-set actual)))
    (is (= (row-set actual)
           (row-set
             (j/hash-join-project-distinct right left ['?x '?y]))))
    (is (nil? (#'j/dense-binary-composition
                (relation {'?x 0 '?z 1} [[1 10] [2 20]])
                (relation {'?z 0 '?y 1} [[10 :a] [20 :b]])
                ['?x '?y])))))

(deftest dense-binary-composition-resolved-lookup-projection-test
  (let [n     64
        left  (relation {'?x 0 '?z 1}
                        (for [x (range n), z (range n)] [x z]))
        right (relation {'?z 0 '?y 1}
                        (for [z (range n), y (range n)] [z y]))]
    (testing "resolved entity ids can use the dense path"
      (binding [qu/*lookup-attrs* #{'?x '?z '?y}]
        (let [composed (j/dense-binary-composition left right ['?x '?y])]
          (is (some? composed))
          (is (= (expected-projected-join left right ['?x '?y])
                 (row-set composed))))))

    (testing "actual lookup refs preserve the conservative result path"
      (let [lookup-left
            (relation {'?x 0 '?z 1}
                      (for [x (range n), z (range n)]
                        [[:entity/id x] z]))]
        (binding [qu/*lookup-attrs* #{'?x '?z '?y}]
          (is (nil? (j/dense-binary-composition
                      lookup-left right ['?x '?y])))
          (is (= (expected-projected-join lookup-left right ['?x '?y])
                 (row-set
                   (j/hash-join-project-distinct
                     lookup-left right ['?x '?y])))))))))

(deftest roaring-binary-composition-test
  (let [domain   5000
        ;; Keep the projected value side slightly smaller so it becomes the
        ;; hash side and exercises the large-domain representation. Duplicate
        ;; proof rows are legal input to the physical operator and should not
        ;; affect its distinct result.
        left     (relation {'?x 0 '?z 1}
                           (repeat (inc domain) [0 1]))
        right    (relation {'?z 0 '?y 1}
                           (map (fn [y] [1 y]) (range domain)))
        composed (j/dense-binary-composition left right ['?x '?y])
        stats    (::j/dense-composition (meta composed))]
    (is (= :roaring (:bitmap stats)))
    (is (= (* (inc domain) domain) (:candidate-pairs stats)))
    (is (= domain (.size ^java.util.List (:tuples composed))))
    (is (= #{[0 0] [0 (dec domain)]}
           (set (filter #(#{0 (dec domain)} (second %))
                        (row-set composed)))))))
