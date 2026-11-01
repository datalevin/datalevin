(ns datalevin.query.prefix-range-test
  (:require [clojure.test :refer [deftest is testing]]
            [datalevin.core :as d]))

(defn- prefix-query [length predicate]
  {:find '[?s]
   :where ['[?e :s ?s]
           [(list 'subs '?s 0 (list 'min length '(count ?s))) '?p]
           [predicate]]})

(defn- input-range [explain]
  (some #(when (and (map? %) (= :s (:attr %))) (:range %))
        (tree-seq coll? seq (:query-graph explain))))

(defn- check-query [conn datoms query]
  (let [explain (d/explain {:run? true} query @conn)]
    (is (= (set (d/q query datoms)) (set (:result explain))) (pr-str query))
    explain))

(deftest total-prefix-membership-restricts-native-scans
  (let [values ["" "1" "13" "13-12" "17-44" "18-55" "29-99"]
        datoms (mapv (fn [n s] [(inc n) :s s]) (range) values)
        query (prefix-query 2 '(or (= ?p "13") (= "17" ?p)))]
    (with-open [conn (d/create-conn nil {:s {:db/valueType :db.type/string}})]
      (d/transact! conn (map (fn [[e a s]] [:db/add e a s]) datoms))
      (let [explain (check-query conn datoms query)]
        (is (= [[[:closed "13"] [:open "14"]]
                [[:closed "17"] [:open "18"]]]
               (input-range explain)))
        (is (= (set (rest (:where query))) (set (:late-clauses explain)))))
      (doseq [[predicate expected-range]
              [['(in ?p ["13" "17"])
                [[[:closed "13"] [:open "14"]]
                 [[:closed "17"] [:open "18"]]]]
               ['(= ?p "1") [[[:closed "1"] [:closed "1"]]]]
               ['(= ?p "") [[[:closed ""] [:closed ""]]]]
               ['(= ?p "130") :empty-range]
               ['(and (= ?p "13") (= ?p "17")) :empty-range]
               ['(or (and (= ?p "13") (= ?s "13-12"))
                     (and (= ?p "17") (= ?s "17-44")))
                [[[:closed "13"] [:open "14"]]
                 [[:closed "17"] [:open "18"]]]]
               ['(or (= ?p "13") (= ?s "29-99")) nil]
               ['(not (= ?p "13")) nil]]]
        (is (= expected-range
               (input-range (check-query conn datoms (prefix-query 2 predicate))))))
      (testing "input bounds and inferred prefix bounds intersect"
        (let [query (update query :where conj '[(< ?s "16")])]
          (is (= [[[:closed "13"] [:open "14"]]]
                 (input-range (check-query conn datoms query))))))
      (testing "an impossible output stays empty alongside an input bound"
        (let [query (update (prefix-query 2 '(= ?p "130"))
                            :where conj '[(< ?s "16")])]
          (is (empty? (:result (check-query conn datoms query))))))
      (testing "zero-length extraction is unrestricted"
        (is (nil? (input-range
                    (check-query conn datoms (prefix-query 0 '(= ?p ""))))))))))

(deftest partial-subs-retains-short-string-errors
  (let [query '[:find ?s :where [?e :s ?s]
                [(subs ?s 0 2) ?p] [(= ?p "13")]]]
    (with-open [conn (d/create-conn nil {:s {:db/valueType :db.type/string}})]
      (d/transact! conn [{:s "1"} {:s "13-0"}])
      (is (nil? (input-range (d/explain {:run? false} query @conn))))
      (is (thrown? StringIndexOutOfBoundsException (d/q query @conn)))
      (is (thrown? StringIndexOutOfBoundsException
                   (d/q query [[1 :s "1"] [2 :s "13-0"]]))))))

(deftest inference-requires-string-storage
  (with-open [conn (d/create-conn nil)]
    (d/transact! conn [{:s "1"} {:s "13-0"}])
    (is (nil? (input-range
                (check-query conn [[1 :s "1"] [2 :s "13-0"]]
                             (prefix-query 2 '(= ?p "13"))))))))

(deftest unicode-and-giant-prefixes-preserve-expression-semantics
  (let [giant (apply str (repeat 600 "x"))
        prefixes ["a" "é" "猫" "😀" "\uD7FF" "\uE000" "\uFFFF"
                  (String. (Character/toChars 0x10FFFF)) giant]
        values (vec (concat ["" "1"] (mapcat #(vector % (str % "tail")) prefixes)))
        datoms (mapv (fn [n s] [(inc n) :s s]) (range) values)]
    (with-open [conn (d/create-conn nil {:s {:db/valueType :db.type/string}})]
      (d/transact! conn (map (fn [[e a s]] [:db/add e a s]) datoms))
      (doseq [prefix prefixes]
        (let [query (prefix-query (count prefix) (list '= '?p prefix))
              explain (check-query conn datoms query)]
          (if (= prefix giant)
            (is (nil? (input-range explain)))
            (is (some? (input-range explain))))))
      (testing "a split surrogate cannot be used as a UTF-8 index bound"
        (let [prefix (subs "😀" 0 1)
              explain (check-query conn datoms
                                   (prefix-query 1 (list '= '?p prefix)))]
          (is (= #{["😀"] ["😀tail"]} (set (:result explain))))
          (is (nil? (input-range explain)))))
      (testing "unions of BMP and supplementary prefixes retain aggregate counts"
        (doseq [n [1 2 3]
                choices [["a" "é" "猫" "\uFFFF"] ["\uFFFF" "😀"]
                         ["\uD7FF" "\uE000" "😀"]]]
          (check-query conn datoms
                       (assoc (prefix-query n (list 'in '?p choices))
                              :find '[(count ?e)])))))))
