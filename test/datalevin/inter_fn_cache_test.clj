(ns datalevin.inter-fn-cache-test
  (:require [clojure.test :refer [deftest is]]
            [datalevin.bits :as b]
            [datalevin.interpret :as inter]))

(deftest cached-functions-rebind-captured-values
  (let [cache (inter/inter-fn-cache)
        make-fn (fn [n] (inter/inter-fn [x] (+ x n)))
        first-bytes (b/serialize (make-fn 10))
        second-bytes (b/serialize (make-fn 20))]
    (binding [inter/*inter-fn-cache* cache]
      (let [f (b/deserialize first-bytes)
            g (b/deserialize second-bytes)]
        (is (= 11 (f 1)))
        (is (= 21 (g 1)))
        (is (= 12 (f 2)))
        (is (= 22 ((b/deserialize (b/serialize g)) 2)))
        (is (= 1 (.size cache)) "captured data is not part of the compiled template")))))

(deftest cached-functions-do-not-share-mutable-captures
  (let [cache (inter/inter-fn-cache)
        source (fn [] (list 'let ['state (list 'quote (atom 0))]
                            '(fn [] (swap! state inc))))]
    (binding [inter/*inter-fn-cache* cache]
      (let [f (inter/compile-inter-fn-source (source))
            g (inter/compile-inter-fn-source (source))]
        (is (= 1 (f)))
        (is (= 2 (f)))
        (is (= 1 (g)))
        (is (= 3 (f)))
        (is (= 1 (.size cache)))))))

(deftest cached-functions-follow-host-var-replacement-and-visibility
  (let [ns-sym 'app.inter-fn-cache-test
        host-ns (create-ns ns-sym)
        source '(fn [x] (app.inter-fn-cache-test/helper x))]
    (try
      (let [v (intern host-ns 'helper inc)]
        (binding [inter/*inter-fn-cache* (inter/inter-fn-cache)]
          (is (= 2 ((inter/compile-inter-fn-source source) 1)))
          (with-redefs-fn {v dec}
            #(is (= 0 ((inter/compile-inter-fn-source source) 1))))
          (ns-unmap host-ns 'helper)
          (let [replacement (intern host-ns 'helper #(* 2 %))]
            (is (= 6 ((inter/compile-inter-fn-source source) 3)))
            (alter-meta! replacement assoc :private true)
            (is (thrown-with-msg? Exception #"Cannot resolve host var"
                                 (inter/compile-inter-fn-source source))))))
      (finally (remove-ns ns-sym)))))

(deftest compiler-cache-is-bounded-and-still-validates-code
  (binding [inter/*inter-fn-cache* (inter/inter-fn-cache)]
    (dotimes [n 80]
      (is (= n ((inter/compile-inter-fn-source (list 'fn [] n))))))
    (is (= 64 (.size inter/*inter-fn-cache*)))
    (is (thrown-with-msg? Exception #"Disallowed inter-fn symbol"
                         (inter/compile-inter-fn-source '(fn [] (slurp "secret")))))))
