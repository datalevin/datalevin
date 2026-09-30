(ns datalevin.rules.magic-test
  (:require [clojure.test :refer [deftest is testing]]
            [datalevin.rules.magic :as magic]))

(deftest stable-bindings-do-not-need-magic-propagation
  (let [rules '{path [[(path ?type ?from ?to) (edge ?type ?from ?to)]
                     [(path ?type ?from ?to)
                      (edge ?type ?from ?next)
                      (path ?type ?next ?to)]]}]
    (testing "the ordinary evaluator already seeds stable type/target arguments"
      (is (not (magic/magic-effective? rules 'path [0] #{'path})))
      (is (not (magic/magic-effective? rules 'path [0 2] #{'path}))))
    (testing "a changing source binding still benefits from propagation"
      (is (magic/magic-effective? rules 'path [1] #{'path})))
    (testing "stable arguments do not hide a changing binding"
      (is (magic/magic-effective? rules 'path [0 1] #{'path})))))

(deftest mutual-recursion-retains-magic-propagation
  (let [rules '{even-path [[(even-path ?from ?to) (edge ?from ?to)]
                          [(even-path ?from ?to)
                           (edge ?from ?next) (odd-path ?next ?to)]]
                odd-path [[(odd-path ?from ?to)
                           (edge ?from ?next) (even-path ?next ?to)]]}]
    (is (magic/magic-effective? rules 'even-path [0] #{'even-path 'odd-path}))))
