(ns datalevin.interpret-quotation-test
  (:require [clojure.edn :as edn]
            [clojure.test :refer [deftest is testing]]
            [datalevin.interpret :as i]
            [sci.core :as sci]))

(defn- exec-value [code]
  (edn/read-string (with-out-str (i/exec-code code))))

(defn- repl-value [code]
  (let [ctx (sci/init i/sci-opts)]
    (i/eval-fn ctx (sci/parse-next ctx (sci/reader code)))))

(def ^:private evaluators
  [[:exec exec-value] [:repl repl-value]])

(deftest quoted-forms-remain-data
  (doseq [[label evaluate] evaluators]
    (testing (name label)
      (doseq [value ['(max ?a)
                     '[:find (max ?a) (distinct ?b) (sum ?c)]
                     '{:find [(max ?a)] :where [[?e :a ?a]]}]]
        (is (= value (evaluate (str "'" (pr-str value)))))
        (is (= value (evaluate (str "(quote " (pr-str value) ")")))))
      (is (= '[(max ?a) {:query [:find (distinct ?a)]}]
             (evaluate "['(max ?a) {:query '[:find (distinct ?a)]}]"))))))

(deftest quoted-query-aggregates-use-datalog-functions
  (doseq [[label evaluate] evaluators]
    (testing (name label)
      (doseq [[query expected]
              [['[:find (max ?a) . :where [_ :a ?a]] 9]
               ['[:find (min ?a) . :where [_ :a ?a]] 3]
               ['[:find (distinct ?a) . :where [_ :a ?a]] #{3 9}]
               ['[:find (max 2 ?a) . :where [_ :a ?a]] [3 9]]
               ['[:find (sum ?a) . :where [_ :a ?a]] 12]]]
        (is (= expected
               (evaluate (str "(q '" (pr-str query)
                              " [[1 :a 3] [2 :a 9]])")))
            (pr-str query)))
      (is (contains? #{3 9}
                     (evaluate "(q '[:find (rand ?a) . :where [_ :a ?a]]
                                   [[1 :a 3] [2 :a 9]])"))))))

(deftest quoted-where-functions-compare-strings
  (doseq [[label evaluate] evaluators]
    (testing (name label)
      (is (= "y"
             (evaluate "(q '[:find ?m . :where [_ :a ?a] [_ :b ?b]
                             [(max ?a ?b) ?m]]
                           [[1 :a \"x\"] [2 :b \"y\"]])")))
      (is (= "x"
             (evaluate "(q '[:find ?m . :where [_ :a ?a] [_ :b ?b]
                             [(min ?a ?b) ?m]]
                           [[1 :a \"x\"] [2 :b \"y\"]])"))))))

(deftest executable-forms-still-resolve-functions
  (doseq [[label evaluate] evaluators]
    (testing (name label)
      (is (= 9 (evaluate "(let [value (inc 2)] (max value 9))")))
      (is (true? (evaluate "(datom? (datom 1 :a (+ 1 2)))")))
      (is (nil? (evaluate "(Thread/sleep 0)")))
      (doseq [value [nil true false 42 "text"]]
        (is (= value (evaluate (pr-str value))))))))
