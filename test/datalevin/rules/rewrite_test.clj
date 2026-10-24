(ns datalevin.rules.rewrite-test
  (:require [clojure.test :refer [deftest is]]
            [datalevin.core :as d]
            [datalevin.rules :as rules]))

(deftest expansion-reuses-call-local-stratification-state
  (doseq [[program where]
          [['[[(large ?e) [?e :value ?v] [(>= ?v 60)]]]
            '[(large ?e)]]
           ['[[(shift [?x] ?y) [(inc ?x) ?y]]]
            '[(shift 1 ?y)]]
           ['[[(reach ?x ?y) [?x :edge ?y]]
              [(reach ?x ?y) [?x :edge ?z] (reach ?z ?y)]]
            '[(reach ?x ?y)]]]]
    (let [grouped (rules/parse-rules program)
          deps (rules/dependency-graph grouped)
          strata (rules/dependency-sccs deps)
          rewritten (rules/rewrite {:rules grouped :rules-deps deps
                                    :parsed-q {:qorig-where where}})]
      (is (identical? deps (:rules-deps rewritten)))
      (is (identical? strata (rules/dependency-sccs (:rules-deps rewritten))))
      (is (seq (get-in rewritten [:parsed-q :qwhere]))))))

(deftest expansion-builds-missing-dependency-state
  (let [grouped (rules/parse-rules '[[(shift [?x] ?y) [(inc ?x) ?y]]])
        where '[(shift 1 ?y)]
        rewritten (rules/rewrite {:rules grouped :parsed-q {:qorig-where where}})]
    (is (= {'shift #{}} (:rules-deps rewritten)))
    (is (= #{#{'shift}} (set (rules/dependency-sccs (:rules-deps rewritten)))))
    (is (= where (get-in rewritten [:parsed-q :qorig-where])))))

(deftest expanded-call-local-variables-do-not-capture-each-other
  (let [program '[[(f ?in ?out) [(inc ?in) ?local] [(inc ?local) ?out]]
                  [(g ?in ?out) (f ?in ?middle) (f ?middle ?out)]]]
    (is (= #{[999 3 4]}
           (d/q '[:find ?local ?a ?b :in % ?local
                  :where (f 1 ?a) (f 2 ?b)] program 999)))
    (is (= #{[999 5 6]}
           (d/q '[:find ?local ?a ?b :in % ?local
                  :where (g 1 ?a) (g 2 ?b)] program 999)))))
