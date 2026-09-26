(ns ycsb-bench.audit-test
  (:require [clojure.test :refer [deftest is testing]]
            [ycsb-bench.audit :as audit]
            [ycsb-bench.core :as core]
            [ycsb-bench.runner :as runner]
            [ycsb-bench.store :as store]
            [ycsb-bench.workload :as w]))

(def options
  (runner/options {:api :kv :mode :embedded :workload :c :distribution :uniform
                   :records 4 :warmup 2 :ops 5 :threads 1
                   :field-count 2 :field-length 4 :scan-length 4 :value-audit? true}))

(defn- fake-store [fault]
  (let [data (atom (sorted-map))
        loaded? (atom false)
        reads (atom 0)]
    (reify store/Records
      (put-records! [_ records]
        (let [corrupt? (and @loaded? (= fault :corrupt-insert))]
          (swap! data into (if corrupt?
                            (mapv (fn [[id values]] [id (assoc values 0 "oops")]) records)
                            records))
          (reset! loaded? true)))
      (read-record [_ id]
        (let [n (swap! reads inc)]
          ;; Only corrupt the first worker read; all final reads are correct.
          (if (and (= n 1) (= fault :wrong-record))
            (get @data (first (remove #{id} (keys @data))))
            (get @data id))))
      (update-field! [_ id field value]
        (when-not (= fault :drop-update)
          (swap! data update id
                 #(cond-> (assoc % field value)
                    (= fault :clobber-field) (assoc (- 1 field) "oops")))))
      (scan-records [_ start n]
        (let [rows (vec (take n (subseq @data >= start)))]
          (if (= fault :wrong-scan-values)
            (mapv (fn [[id _]] [id ["oops" "oops"]]) rows)
            rows)))
      (record-count [_] (count @data))
      (storage-info [_] {})
      (close-store! [_]))))

(deftest structural-reports-do-not-claim-value-correctness
  (doseq [[fault workload op] [[:wrong-record :c :read]
                              [:drop-update :a :update]
                              [:drop-update :f :rmw]
                              [:wrong-scan-values :e :scan]]]
    (with-redefs [store/with-stores (fn [_ f] (f (fn [callback] (callback (fake-store fault)))))
                  w/choose-operation (fn [& _] op)]
      (let [validation (:validation (runner/run-case!
                                     (assoc options :workload workload :value-audit? false)))]
        (is (= :passed (:status validation)))
        (is (= :structure (:scope validation)))
        (is (= :not-performed (:value-checks validation)))))))

(deftest audits-reject-shape-correct-corruption-after-each-phase
  (doseq [fault-phase [:warmup :measured]
          [fault workload op] [[:wrong-record :c :read]
                              [:drop-update :a :update]
                              [:drop-update :f :rmw]
                              [:clobber-field :a :update]
                              [:wrong-scan-values :e :scan]
                              [:corrupt-insert :d :insert]
                              [:corrupt-insert :e :insert]]]
    (testing (str [fault workload])
      (let [completed (atom [])
            opened (atom 0)
            run-phase runner/run-phase!]
        (with-redefs [store/with-stores
                      (fn [_ f]
                        (f (fn [callback]
                             (callback (fake-store
                                         (when (= (swap! opened inc)
                                                   (if (= fault-phase :warmup) 1 2))
                                           fault))))))
                      w/choose-operation (fn [& _] op)
                      runner/run-phase!
                      (fn [db space cdf opts phase n]
                        (let [result (run-phase db space cdf opts phase n)]
                          (swap! completed conj phase)
                          result))]
          (is (thrown-with-msg? clojure.lang.ExceptionInfo #"write history"
                               (runner/run-case! (assoc options :workload workload)))))
        (is (= (if (= fault-phase :warmup) [:warmup] [:warmup :measured]) @completed))))))

(deftest audit-retains-warmup-and-timed-phase-history
  (doseq [workload [:a :b :c :d :e :f]
          timed? [false true]]
    (with-redefs [store/with-stores (fn [_ f] (f (fn [callback] (callback (fake-store nil)))))]
      (let [result (runner/run-case!
                     (cond-> (assoc options :workload workload :threads 4)
                       timed? (assoc :warmup-ms 5 :measurement-ms 10)))]
        (doseq [phase [:warmup :measured]]
          (let [counts (update-vals (get-in result [phase :by-operation]) :count)
                validation (if (= phase :warmup) (get-in result [:warmup :validation]) (:validation result))
                checks (:value-checks validation)]
            (is (= :structure-and-observed-values (:scope validation)))
            (is (= :passed (:status checks)))
            (is (= (if (= phase :warmup) :post-warmup :post-measurement)
                   (:comparisons checks) (:character-checks validation)))
            (is (= (+ (get counts :read 0) (get counts :rmw 0)) (:point-reads checks)))
            (is (= (+ (get counts :insert 0) (get counts :update 0) (get counts :rmw 0))
                   (:write-calls checks)))
            (is (= (:records validation) (:final-records checks)))))))))

(defn- update-event [start end field value]
  {:op :update :id 0 :field field :value value :start start :end end})

(defn- accepted? [index values start end]
  (try (audit/check-values! index 0 values start end) true
       (catch clojure.lang.ExceptionInfo _ false)))

(deftest overlapping-writes-do-not-use-response-order
  (let [opts (assoc options :records 1)
        initial (w/initial-values (:seed opts) 0 opts)
        ;; The first call can commit last, even though its response comes first.
        events [(update-event 10 30 0 "one1") (update-event 20 40 0 "two2")]
        index (audit/prepare events opts identity)]
    (is (accepted? index initial 0 5))
    (is (not (accepted? index (assoc initial 0 "one1") 0 5))
        "A read cannot observe a future write")
    (is (accepted? index initial 15 25))
    (is (accepted? index (assoc initial 0 "two2") 15 25))
    (doseq [value ["one1" "two2"]]
      (is (accepted? index (assoc initial 0 value) Long/MAX_VALUE Long/MAX_VALUE)))
    (is (not (accepted? index initial Long/MAX_VALUE Long/MAX_VALUE)))
    (let [later (audit/prepare (conj events (update-event 50 60 0 "last")) opts identity)]
      (is (accepted? later (assoc initial 0 "last") 61 70))
      (is (not (accepted? later (assoc initial 0 "one1") 61 70)))
      (is (not (accepted? later (assoc initial 0 "two2") 61 70))))
    (let [independent (audit/prepare (conj events (update-event 50 60 1 "peer")) opts identity)]
      (doseq [value ["one1" "two2"]]
        (is (accepted? independent [value "peer"] 61 70))
        (is (not (accepted? independent (assoc initial 0 value) 61 70)))))
    (let [equal-times (audit/prepare [(update-event 10 20 0 "one1")
                                      (update-event 20 30 0 "two2")] opts identity)]
      (doseq [value ["one1" "two2"]]
        (is (accepted? equal-times (assoc initial 0 value) 31 40))))))

(deftest a-long-overlap-does-not-resurrect-an-ordered-write
  (let [opts (assoc options :records 1)
        initial (w/initial-values (:seed opts) 0 opts)
        index (audit/prepare [(update-event 10 100 0 "long")
                               (update-event 20 30 0 "old1")
                               (update-event 40 50 0 "new2")] opts identity)]
    (is (not (accepted? index (assoc initial 0 "old1") 110 120)))
    (doseq [value ["long" "new2"]]
      (is (accepted? index (assoc initial 0 value) 110 120)))))

(deftest observations-and-final-state-have-distinct-coverage
  (let [opts (assoc options :records 1)
        initial (w/initial-values (:seed opts) 0 opts)
        events [(update-event 10 20 0 "lost") (update-event 40 50 0 "kept")]
        index (audit/prepare events opts identity)]
    (is (accepted? index (assoc initial 0 "kept") 60 70)
        "Final state cannot prove that a subsequently overwritten write was applied")
    (is (not (accepted? index initial 25 30))
        "A read after the dropped write and before the next write exposes the loss")
    (is (accepted? index (assoc initial 0 "lost") 25 30))))

(deftest audit-keeps-worker-handles-and-thread-logs
  (let [opts (assoc options :records 1 :threads 3 :ops 7)
        values (w/initial-values (:seed opts) 0 opts)
        calls (mapv (fn [_] (atom 0)) (range 3))
        handles (mapv (fn [counter]
                        (reify store/Records
                          (read-record [_ _] (swap! counter inc) values))) calls)
        history (audit/history)
        group (store/->StoreGroup handles {})
        db (audit/->AuditedRecords group history)]
    (runner/run-phase! db (w/keyspace 1) nil opts :measured 7)
    (is (= [3 2 2] (mapv deref calls)))
    (is (= 3 (count (:logs history))))
    (is (= 7 (count (audit/events history))))))

(deftest audit-timings-are-grouped-separately
  (let [results (for [audit? [false true]]
                  {:configuration {:value-audit? audit?}
                   :measured {:ops-per-second 1.0}})]
    (is (= 2 (count (core/summarize-trials results))))))
