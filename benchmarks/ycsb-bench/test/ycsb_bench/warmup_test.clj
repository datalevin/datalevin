(ns ycsb-bench.warmup-test
  (:require [clojure.test :refer [deftest is testing]]
            [ycsb-bench.core :as core]
            [ycsb-bench.runner :as runner]
            [ycsb-bench.store :as store]
            [ycsb-bench.workload :as w]))

(def options
  (runner/options {:system :datalevin :api :kv :mode :embedded :workload :d
                   :records 8 :ops 20 :warmup 20 :threads 1 :pool-size 1
                   :field-count 2 :field-length 4 :scan-length 4
                   :server-mode :in-process :value-audit? true}))

(defn- initial-records [opts]
  (into (sorted-map)
        (map (fn [ordinal]
               [(w/application-key ordinal)
                (w/initial-values (:seed opts) ordinal opts)]))
        (range (:records opts))))

(defn- memory-store [calls]
  (let [data (atom (sorted-map))]
    (reify store/Records
      (put-records! [_ records]
        (swap! calls conj [:insert records])
        (swap! data into records))
      (read-record [_ id]
        (swap! calls conj [:read id])
        (get @data id))
      (update-field! [_ id field value]
        (swap! calls conj [:update id field value])
        (swap! data update id assoc field value))
      (scan-records [_ start n]
        (swap! calls conj [:scan start n])
        (vec (take n (subseq @data >= start))))
      (record-count [_] (count @data))
      (storage-info [_] {})
      (close-store! [_]))))

(deftest timed-zipfian-inserts-require-explicit-keyspace
  (doseq [selection [{:workload :e} {:workload :all}
                     {:workload :d :distribution :zipfian}]
          timer [{:measurement-ms 10} {:warmup-ms 10 :warmup 0}]]
    (let [opts (merge selection timer)]
      (is (thrown-with-msg? clojure.lang.ExceptionInfo #"require an explicit --zipfian-keyspace"
                           (runner/options opts)))
      (is (= 20000 (:zipfian-keyspace
                     (runner/options (assoc opts :zipfian-keyspace 20000)))))))
  (doseq [n [false 9999 "20000"]]
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"zipfian-keyspace must be at least records"
                         (runner/options {:workload :e :measurement-ms 10
                                          :zipfian-keyspace n}))))
  ;; No keyspace estimate is needed when there are no timed inserts using
  ;; scrambled Zipfian selection. A zero-duration warmup is skipped.
  (doseq [opts [{:workload :e} {:workload :e :warmup-ms 0}
                {:workload :a :measurement-ms 10}
                {:workload :b :measurement-ms 10}
                {:workload :c :measurement-ms 10}
                {:workload :f :measurement-ms 10}
                {:workload :d :warmup-ms 10 :measurement-ms 10}
                {:workload :e :distribution :uniform :measurement-ms 10}
                {:workload :all :distribution :latest :measurement-ms 10}]]
    (is (nil? (:zipfian-keyspace (runner/options opts))) (pr-str opts))))

(deftest explicit-timed-keyspace-ignores-overridden-counts
  (doseq [workload [:d :e]]
    (let [run-phase runner/run-phase!
          runs
          (mapv
            (fn [n]
              (let [logs (atom [])
                    result
                    (with-redefs [store/with-stores
                                  (fn [_ f]
                                    (f (fn [callback]
                                         (let [calls (atom [])]
                                           (swap! logs conj calls)
                                           (callback (memory-store calls))))))
                                  runner/run-phase!
                                  (fn [db space cdf opts phase _]
                                    ;; Run a fixed prefix so elapsed time cannot
                                    ;; change the trace being compared. Sampler
                                    ;; setup and option resolution remain real.
                                    (is (= 32 cdf (:zipfian-keyspace opts)))
                                    (run-phase db space cdf
                                               (dissoc opts :warmup-ms :measurement-ms)
                                               phase 200))]
                      (runner/run-case!
                        (assoc options :workload workload :distribution :zipfian
                               :warmup n :ops n :warmup-ms 10 :measurement-ms 10
                               :zipfian-keyspace 32)))]
                (is (= 32 (get-in result [:configuration :zipfian-keyspace])
                       (get-in result [:warmup :zipfian-keyspace])
                       (get-in result [:measured :zipfian-keyspace])))
                (is (= :passed (get-in result [:validation :value-checks :status])))
                (mapv deref @logs)))
            [1 100000])]
      (is (apply = runs) (str workload " keys, values and operations")))))

(deftest counted-zipfian-keyspaces-retain-per-phase-predictions
  (with-redefs [store/with-stores
                (fn [_ f] (f (fn [callback] (callback (memory-store (atom []))))))]
    (let [result (runner/run-case! (assoc options :workload :e :warmup 20 :ops 200))]
      (is (= 11 (get-in result [:warmup :zipfian-keyspace])))
      (is (= 29 (get-in result [:configuration :zipfian-keyspace])
             (get-in result [:measured :zipfian-keyspace])))
      (is (= :passed (get-in result [:validation :value-checks :status]))))))

(deftest warmup-size-and-duration-do-not-change-measured-workload
  ;; With one worker the complete measured request stream is reproducible.
  ;; Cover growing latest samplers, scrambled Zipfian rejection, and updates.
  (doseq [workload [:a :d :e :f]]
    (let [runs
          (for [warmup-opts [{:warmup 0} {:warmup 2} {:warmup 200}
                             {:warmup 0 :warmup-ms 5} {:warmup 200 :warmup-ms 0}]]
            (let [logs (atom [])
                  result
                  (with-redefs [store/with-stores
                                (fn [_ f]
                                  (f (fn [callback]
                                       (let [calls (atom [])]
                                         (swap! logs conj calls)
                                         (callback (memory-store calls))))))]
                    (runner/run-case! (cond-> (merge options {:workload workload} warmup-opts)
                                        (= workload :e) (assoc :zipfian-keyspace 32))))]
              (is (= (if (pos? (or (:warmup-ms warmup-opts) (:warmup warmup-opts))) 2 1)
                     (count @logs)))
              (is (= 8 (get-in result [:measured :starting-records])))
              (is (= :passed (get-in result [:validation :value-checks :status])))
              {:trace @(peek @logs)
               :mix (get-in result [:measured :by-operation])
               :keyspace (get-in result [:configuration :zipfian-keyspace])}))]
      (is (apply = (map :trace runs)) (str workload " measured keys and values"))
      (is (apply = (map #(update-vals (:mix %) :count) runs)))
      (is (apply = (map :keyspace runs))))))

(deftest warmup-mutations-never-reach-real-measured-stores
  (doseq [[system api mode]
          (cond-> [[:datalevin :kv :embedded] [:datalevin :datalog :embedded]
                    [:datalevin :kv :remote] [:datalevin :datalog :remote]
                    [:sqlite :kv :embedded] [:sqlite :datalog :embedded]]
            (System/getenv "YCSB_PG_URL")
            (into [[:postgres :kv :remote] [:postgres :datalog :remote]]))
          [workload operation] [[:d :insert] [:e :insert] [:f :rmw]]]
    (testing (str [system api mode workload])
      (let [run-phase runner/run-phase!
            starting-data (atom {})
            opts (cond-> (assoc options :system system :api api :mode mode :workload workload
                                :threads 2 :pool-size 2 :datalog-handles :independent
                                :value-audit? false :warmup-ms 5 :ops 4)
                   (= workload :e) (assoc :zipfian-keyspace 32))
            initial (initial-records opts)
            result
            (with-redefs [runner/run-phase!
                          (fn [db space cdf opts phase n]
                            (is (= (:records opts) (:next @space) (:visible @space)))
                            (swap! starting-data assoc phase
                                   (into (sorted-map)
                                         (map (fn [id] [id (store/read-record db id)]))
                                         (keys initial)))
                            ;; Force mutations even on a slow host where a timed
                            ;; warmup may complete only one operation per worker.
                            (with-redefs [w/choose-operation
                                          (fn [& _]
                                            (if (= phase :warmup) operation
                                                (if (= workload :e) :scan :read)))]
                              (run-phase db space cdf opts phase n)))]
              (runner/run-case! opts))]
        (is (= {:warmup initial :measured initial} @starting-data))
        (is (= 8 (get-in result [:measured :starting-records])))
        (is (= 8 (get-in result [:validation :records])))
        (is (pos? (get-in result [:warmup :by-operation operation :count])))
        (when (= operation :insert)
          (is (< 8 (get-in result [:warmup :validation :records]))))
        (when (= mode :remote)
          (is (= (get-in result [:warmup :storage :server :pid])
                 (get-in result [:storage :server :pid]))))))))

(deftest measurement-sampler-does-not-inherit-warmup-capacity
  (let [opts (assoc options :distribution :latest :warmup-ms 5000)
        measured (#'runner/request-state opts :measured 20)
        warmup (#'runner/request-state opts :warmup 100)]
    (is (not (instance? clojure.lang.IAtom measured)))
    (is (= 28 (alength ^doubles measured)))
    (is (instance? clojure.lang.IAtom warmup))
    (is (= 8 (alength ^doubles @warmup))
        "Timed phases start with the loaded prefix, ignoring their operation count")
    (w/grow-cdf! warmup 500)
    (is (= 28 (alength ^doubles measured)))
    (is (= (vec (w/zipf-cdf 28)) (vec measured)))))

(deftest summaries-separate-old-warmup-behavior
  (let [results (for [isolation [nil :separate-database]]
                  {:configuration (cond-> {} isolation (assoc :warmup-isolation isolation))
                   :measured {:ops-per-second 1.0}})]
    (is (= 2 (count (core/summarize-trials results))))))
