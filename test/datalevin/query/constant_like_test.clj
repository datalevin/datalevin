(ns datalevin.query.constant-like-test
  (:require [clojure.test :refer [deftest is testing]]
            [datalevin.built-ins :as b]
            [datalevin.core :as d]
            [datalevin.query.resolve :as resolve])
  (:import [java.util HashMap]
           [datalevin.utl LRUCache]))

(deftest constant-matchers-survive-cache-eviction-without-row-lookups
  (doseq [[f ^LRUCache matcher-cache expected]
          [[b/like b/like-cache true] [b/not-like b/not-like-cache false]]]
    (.clear matcher-cache)
    (let [call (resolve/prepare-call f (object-array [nil "a%" nil]) #{0})]
      (try
        (is (= expected (call (object-array ["abc" "a%" nil]))))
        (is (not (.isEmpty matcher-cache)))
        (.clear matcher-cache)
        (is (= expected (call (object-array ["abc" "a%" nil]))))
        (is (.isEmpty matcher-cache))
        (finally (.clear matcher-cache))))))

(deftest dynamic-patterns-and-options-remain-dynamic
  (is (= #{[1] [2]}
         (d/q '[:find ?id :in [[?id ?text ?pattern ?opts]]
                :where [(like ?text ?pattern ?opts)]]
              [[1 "a_foo" "a|_%" {:escape \|}]
               [2 "b-foo" "b%" nil]
               [3 "a_foo" "a|_%" {:escape \!}]])))
  (is (= #{[1] [2]}
         (d/q '[:find ?id :in [[?id ?text ?opts]]
                :where [(like ?text "a|_%" ?opts)]]
              [[1 "a_foo" {:escape \|}]
               [2 "a|xfoo" {:escape \!}]
               [3 "a_foo" {:escape \!}]])))
  (let [opts (doto (HashMap.) (.put :escape \|))
        args (object-array ["a_foo" "a|_%" opts])
        call (resolve/prepare-call b/like args #{0})]
    (is (true? (call args)))
    (.put opts :escape \!)
    (is (false? (call args)))))

(deftest literal-matching-retains-unicode-escape-and-error-behavior
  (doseq [[text pattern opts expected]
          [["😀" "_" nil true] ["a\nb" "a%b" nil true]
           ["a%b" "a|%b" {:escape \|} true]
           ["a_b" "a猫_b" {:escape \猫} true]
           ["abc" "a!" nil false] ["a" "a!" nil true]]]
    (doseq [[f result] [[b/like expected] [b/not-like (not expected)]]]
      (let [args (object-array [text pattern opts])
            call (resolve/prepare-call f args #{0})]
        (is (= result (call args))))))
  (testing "unreached generic predicates do not compile invalid patterns"
    (let [args (object-array ["abc" "bad!x"])
          call (resolve/prepare-call b/like args #{0})]
      (is (thrown-with-msg? IllegalStateException #"Can only escape" (call args)))))
  (is (= #{} (d/q '[:find ?text :in [?text ...]
                    :where [(like ?text "bad!x")]] [])))
  (testing "safe queries still use the registered built-in"
    (binding [resolve/*resolver-mode* :server-safe]
      (is (= #{["abc"]}
             (d/q '[:find ?text :in [?text ...] :where [(like ?text "a%")]]
                  ["abc" "xyz"]))))))
