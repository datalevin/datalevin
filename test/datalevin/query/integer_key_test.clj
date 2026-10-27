(ns datalevin.query.integer-key-test
  (:require [clojure.test :refer [deftest is]]
            [datalevin.join :as join]
            [datalevin.query.source.value :as value]
            [datalevin.relation :as relation])
  (:import [datalevin.utl QueryKey QueryKey$KeyMap QueryKey$KeySet]
           [org.eclipse.collections.impl.list.mutable FastList]))

(deftest boxed-integers-share-long-type-and-key-identity
  (doseq [n [-128 -1 0 1 127]
          convert [byte short int long]
          :let [x (convert n)
                y (long n)
                keys (QueryKey$KeySet.)
                index (QueryKey$KeyMap.)]]
    (is (value/matches-type? :long x))
    (is (= :long (value/inferred-type x)))
    (is (value/matches-type? {:kind :tuple :elements [:long :long]} [x y]))
    (is (QueryKey/equals x y))
    (is (QueryKey/equals y x))
    (is (= (QueryKey/hash x) (QueryKey/hash y)))
    (is (QueryKey/equals [x] [y]))
    (is (= (QueryKey/hash [x]) (QueryKey/hash [y])))
    (.add keys x)
    (.put index y :found)
    (is (.contains keys y))
    (is (= :found (.get index x)))
    (is (not (.add keys y)))))

(deftest long-types-retain-their-integral-range-and-representation
  (doseq [x [(int Integer/MIN_VALUE) (int Integer/MAX_VALUE)
             Long/MIN_VALUE Long/MAX_VALUE]]
    (is (value/matches-type? :long x))
    (is (identical? x (value/owned-copy x))))
  (doseq [x [1N (biginteger 1) 1.0 (float 1) 1M 1/2 "1" nil]]
    (is (not (value/matches-type? :long x))))
  (doseq [x [1N 1.0 1M]]
    (is (not (QueryKey/equals (int 1) x))))
  (is (not (QueryKey/equals (int 1) (long 2)))))

(deftest primitive-single-key-joins-match-every-boxed-integer-width
  (doseq [convert [byte short int long]
          n [-128 -1 0 1 127]
          [left-size right-size] [[1 2] [2 1]]
          :let [left (relation/relation! {'?id 0 '?left 1}
                       (FastList. (mapv object-array
                                    (take left-size [[(convert n) :a] [1000 :extra]]))))
                right (relation/relation! {'?id 0 '?right 1}
                        (FastList. (mapv object-array
                                     (take right-size [[(long n) :b] [2000 :extra]]))))
                result (join/hash-join left right)
                attrs (:attrs result)]]
    (is (= #{[n :a :b]}
           (into #{} (map (fn [^objects row]
                            (mapv #(aget row (int (attrs %))) ['?id '?left '?right])))
                 (:tuples result))))))
