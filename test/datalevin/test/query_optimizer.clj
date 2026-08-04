(ns datalevin.test.query-optimizer
  (:require
   [clojure.test :refer [deftest is testing]]
   [datalevin.constants :as c]
   [datalevin.query-optimizer :as qo]))

(deftest physical-cost-model-is-additive
  (let [scan-cost  (deref #'qo/estimate-scan-v-cost)
        link-cost  (deref #'qo/estimate-link-cost)
        hash-cost  (deref #'qo/estimate-hash-join-cost)
        step       {:attrs-v [[:a {:pred identity :fidx nil}]
                              [:b {:pred nil :fidx 0}]]
                    :vars    ['?a '?b]}]
    (testing "scan work adds operator costs instead of multiplying factors"
      (is (= 4840.0
             (binding [c/optimizer-cost-model :legacy]
               (scan-cost step 10))))
      (is (= 294
             (binding [c/optimizer-cost-model :physical]
               (scan-cost step 10)))))
    (testing "sparse merge probes include their larger index footprint"
      (let [dense  (binding [c/optimizer-cost-model :physical]
                     (scan-cost step 10 10 10 10))
            sparse (binding [c/optimizer-cost-model :physical]
                     (scan-cost step 10 10 10 1000))]
        (is (= 294 dense))
        (is (< dense sparse))))
    (testing "physical hash work includes inputs and materialized output"
      (is (= 199
             (binding [c/optimizer-cost-model :physical
                       c/physical-cost-hash-row 5.0
                       c/physical-cost-hash-output-cell 1.0]
               (hash-cost 10 20 7 3 false))))
      (is (= 109
             (binding [c/optimizer-cost-model :physical
                       c/physical-cost-sip-hash-row 2.0
                       c/physical-cost-hash-output-cell 1.0]
               (hash-cost 10 20 7 3 true)))))
    (testing "physical indexed links have independently calibrated prices"
      (is (= 240
             (binding [c/optimizer-cost-model :physical
                       c/physical-cost-link-probe 5.0
                       c/physical-cost-link-retrieval 6.0]
               (link-cost 10 4 30)))))))

(deftest sampled-distinct-counts-drive-physical-probes
  (let [sampled-distinct (deref #'qo/sampled-distinct-size)
        selected-distinct (deref #'qo/estimate-selected-distinct-size)
        probes          (deref #'qo/estimated-link-probes)
        tuples          (java.util.ArrayList.)]
    (doseq [v [1 1 1 2 3]]
      (.add tuples (object-array [v])))
    (testing "sample repeats keep the NDV estimate below row cardinality"
      (is (= 5 (sampled-distinct tuples 0 10 false)))
      (is (= 3 (sampled-distinct tuples 0 10 true))))
    (testing "selection accounts for repeated values surviving together"
      (is (= 10 (selected-distinct 100 100 10)))
      (is (= 7 (selected-distinct 10 100 10)))
      (is (= 10 (selected-distinct 10 100 200))))
    (testing "an indexed link probes estimated distinct keys, not every row"
      (is (= 7 (probes {:size 100 :distinct-sizes [7]} 0)))
      (is (= 100 (probes {:size 100} 0))))))

(deftest distinct-size-propagation-is-physical-only
  (let [join-distinct (deref #'qo/join-distinct-sizes)
        make-plan     (deref #'qo/make-plan)
        left  {:size 100 :distinct-sizes [20]
               :steps [{:cols ['?x]}]}
        right {:size 50 :distinct-sizes [10]
               :steps [{:cols ['?x]}]}
        distincts (join-distinct left right ['?x] 25)]
    (is (= [10] distincts))
    (is (nil? (:distinct-sizes
                (binding [c/optimizer-cost-model :legacy]
                  (make-plan [] 0 25 0 distincts)))))
    (is (= [10]
           (:distinct-sizes
             (binding [c/optimizer-cost-model :physical]
               (make-plan [] 0 25 0 distincts)))))))

(deftest repeated-link-rows-do-not-become-repeated-storage-probes
  (let [target-distinct
        (deref #'qo/estimate-repeated-link-distinct-size)]
    (is (= 200 (target-distinct 1000 100 20 10000)))
    (is (= 150 (target-distinct 1000 100 20 150)))
    (is (= 1000 (target-distinct 1000 0 20 10000)))))

(deftest material-cardinality-oracle-distinguishes-link-input
  (let [request (deref #'qo/link-input-oracle-request)
        plan    {:steps [{:out '#{?movie ?keyword}}]}
        link    {:type :_ref
                 :tgt  '?cast
                 :attr :cast-info/movie
                 :var  '?movie}]
    (is (= {:kind     :link-input
            :entities '#{?movie ?keyword}
            :link-e   '?movie
            :target   '?cast
            :type     :_ref
            :attr     :cast-info/movie
            :var      '?movie
            :attrs    nil}
           (request plan '?movie link)))))

(deftest physical-plan-cost-follows-pipeline-phases
  (let [initial (deref #'qo/with-initial-stream-cost)
        append  (deref #'qo/with-appended-stream-cost)
        barrier (deref #'qo/with-appended-barrier-cost)]
    (binding [c/optimizer-cost-model :physical
              c/physical-cost-pipeline-parallelism 10]
      (let [p1 (initial {:cost 40} [10 30])
            p2 (append {:cost 110} p1 [20 50])
            p3 (barrier {:cost 120} p2 10 10)
            p4 (append {:cost 145} p3 [25])]
        (is (= [34 40] [(:cost p1) (:work-cost p1)]))
        (is (= [61 110] [(:cost p2) (:work-cost p2)]))
        (is (= [72 120] [(:cost p3) (:work-cost p3)]))
        (is (= [100 145] [(:cost p4) (:work-cost p4)]))))))

(deftest directional-filter-sample-bounds-independence
  (let [ratio (deref #'qo/conservative-filter-ratio)]
    (testing "positive source-target correlation overrides independence"
      (is (= 0.7 (ratio 0.1 1000 700))))
    (testing "a zero-hit sample retains the catalog lower bound"
      (is (= 0.1 (ratio 0.1 1000 0))))
    (testing "missing samples retain the historical neutral selectivity"
      (is (= 1.0 (ratio 0.1 0 0))))))

(deftest linked-filter-selectivity-uses-the-linked-population
  (let [scale (deref #'qo/scale-linked-target-size)]
    (is (= 100 (scale 1000 10000 100000)))
    (is (= 1000 (scale 1000 10000 0)))
    (is (= 1 (scale 1 1 100000)))))

(deftest base-sampling-retains-the-physical-denominator
  (let [scan-size (deref #'qo/estimate-scan-v-size)
        tuples    (fn [n]
                    (let [result (java.util.ArrayList.)]
                      (dotimes [i n]
                        (.add result (object-array [i])))
                      result))]
    (testing "fully executed bases use their exact retained row count"
      (is (= 2 (scan-size 100 [{:mcount 100}
                               {:result (tuples 2)}])))
      (is (= 1 (scan-size 50 [{:mcount 100}
                              {:result (tuples 2)}]))))
    (testing "sample selectivity includes the initial predicate"
      (binding [c/base-estimate-prior-size 100
                c/optimizer-fallback-selectivity 0.1]
        (is (= 119 (scan-size 10000 [{:mcount 10000 :sample (tuples 3)}
                                     {:sample (tuples 3)}])))
        (is (= 91 (scan-size 10000 [{:mcount 10000 :sample (tuples 0)}
                                    {:sample (tuples 0)}])))))
    (testing "sample selectivity scales a linked input, not the base population"
      (binding [c/base-estimate-prior-size 0]
        (is (= 30 (scan-size 10000 [{:mcount 100000 :sample (tuples 3)}
                                    {:sample (tuples 3)}])))
        (is (= 3 (scan-size 1000 [{:mcount 100000 :sample (tuples 3)}
                                  {:sample (tuples 3)}])))))))

(deftest sip-cost-charges-only-materialized-target
  (let [target-size (deref #'qo/estimate-sip-target-size)
        base-cost   (deref #'qo/estimate-sip-base-cost)
        source      {:steps [{:mcount 1000}]}
        target      {:steps [{:mcount 500} {}]
                     :size  500
                     :cost  2000}]
    (testing "input rows conservatively bound distinct SIP keys"
      (is (= 50 (target-size source target 100)))
      (is (= 500 (target-size source target 2000)))
      (is (= 500 (target-size nil target 100))))
    (testing "the target scan remains charged while rejected output does not"
      (is (= 200
             (binding [c/optimizer-cost-model :physical]
               (base-cost target 50))))
      (is (= 2000
             (binding [c/optimizer-cost-model :legacy]
               (base-cost target 50)))))))

(deftest unknown-cost-model-is-rejected
  (is (thrown-with-msg?
        clojure.lang.ExceptionInfo #"Unknown optimizer cost model"
        (binding [c/optimizer-cost-model :unknown]
          ((deref #'qo/estimate-scan-v-cost)
           {:attrs-v [] :vars []} 1)))))
