(ns datalevin.remote-cache-mutability-test
  (:require [clojure.test :refer [deftest is testing]]
            [datalevin.remote-cache :as cache]
            [datalevin.spill :as spill]))

(deftest mutable-collections-bypass-read-cache-retention
  (let [opts {:spill-threshold 100}
        values [(spill/new-spillable-set #{[1]} opts)
                (spill/new-spillable-vector [[1]] opts)
                (spill/new-spillable-map {:value [1]} opts)
                (seq (spill/new-spillable-vector [[1]] opts))
                (rseq (spill/new-spillable-vector [[1]] opts))
                (seq (object-array [[1]]))]]
    (doseq [value values
            wrap [identity vector #(hash-map :value %) #(hash-map % :value)]
            placement [:key :result]]
      (testing (str (class value) " inside " (class (wrap value)) " as " placement)
        (let [cache (cache/read-cache 4 60000)
              reads (atom 0)
              key (if (= placement :key) (wrap value) :key)
              result (if (= placement :result) (wrap value) :value)
              read #(cache/read-result cache key 0 (constantly 1)
                                       (fn [] (swap! reads inc) result))]
          (dotimes [_ 2] (read))
          (is (= 2 @reads)))))))

(deftest persistent-collections-still-retain-results
  (doseq [value [nil false {:value #{[1]}}
                 (sorted-map :value [1]) (sorted-set [1])
                 '([1]) '() (subvec [0 1 2] 1)
                 (conj clojure.lang.PersistentQueue/EMPTY [1])]]
    (let [cache (cache/read-cache 4 60000)
          reads (atom 0)
          read #(cache/read-result cache :key 0 (constantly 1)
                                   (fn [] (swap! reads inc) value))]
      (is (= value (read) (read)))
      (is (= 1 @reads)))))
