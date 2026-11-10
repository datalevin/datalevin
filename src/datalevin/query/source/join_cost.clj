(ns ^:no-doc datalevin.query.source.join-cost
  "Bounded lookahead over complete typed plans. No source or row access."
  (:require [clojure.set :as set]
            [datalevin.query.access.dependency :as dependency]))

(defn price
  "Estimate returned-row work along a complete path. Counts are advisory and
  floor the propagated tuple count at one; a zero never proves emptiness. A second independent
  scan sharing keys has unknown join selectivity and declines this model."
  [selected inputs row-count]
  (reduce
    (fn [{:keys [known tuples work-rows peak-rows] :as state} {:keys [kind value requires columns]}]
      (when state
        (if (= kind :core)
          (when (set/subset? (set (map :variable columns)) known) state)
          (when-let [n (row-count value)]
            (let [output (set (map :variable columns))
                  dependent? (seq requires)
                  shared? (seq (set/intersection known output))
                  n (if (zero? n) 1.0 (double n))]
              (when (or dependent? (not shared?))
                (let [returned (max 1.0 (min Double/MAX_VALUE (* tuples n)))]
                  {:known (set/union known output) :tuples returned
                   :work-rows (min Double/MAX_VALUE (+ work-rows tuples returned))
                   :peak-rows (max peak-rows returned)})))))))
    {:known (set (map :variable inputs)) :tuples 1.0 :work-rows 0.0 :peak-rows 1.0} selected))

(defn select
  "Keep the original feasible plan, then compare complete alternatives with a
  different first access. All attempts share the remaining 4096-state allowance
  and a 20 ms optional CPU budget. Unknown estimates retain the original plan."
  [arguments row-count]
  (let [checks (volatile! 0) check! (:check! arguments)
        counted! #(do (vswap! checks inc) (check!))
        baseline (dependency/schedule (assoc arguments :check! counted!))
        initial (when (= :complete (:status baseline)) (price (:choices baseline) (:inputs arguments) row-count))
        started (System/nanoTime) maximum (:max-states arguments)]
    (if-not initial baseline
      (let [choices (:choices arguments)
            seed (into {} (map (juxt :variable identity)) (:inputs arguments))
            roots (->> choices
                       (filter #(and (= :access (:kind %))
                                     (every? (fn [c] (dependency/supplies? (seed (:variable c)) c)) (:requires %))))
                       (sort-by #(if (:filtered? %) 0 1))
                       ;; Probe distinct starting work, not repeated scan/lookup
                       ;; variants for the same patterns. Keys contain only core IDs.
                       (reduce (fn [{:keys [seen roots] :as state} choice]
                                 (let [source-work (into #{} (map #(if (vector? %) (first %) %)) (:covers choice))]
                                   (if (seen source-work) state
                                     {:seen (conj seen source-work) :roots (conj roots choice)})))
                               {:seen #{} :roots []})
                       :roots (take 16))
            early (vec (take-while #(= :core (:kind %)) choices))
            row-key (juxt :work-rows :peak-rows)
            _ (when-not (pos? maximum) (throw (IllegalArgumentException. "invalid search bound")))]
        (loop [pending (seq roots) best baseline best-price initial evaluated 1 starts []]
          (if (or (nil? pending) (>= @checks maximum) (>= (- (System/nanoTime) started) 20000000))
            (assoc best :cost-changed? (not (and (= (count (:choices best)) (count (:choices baseline)))
                                                       (every? true? (map identical? (:choices best) (:choices baseline)))))
                        :join-cost (select-keys best-price [:work-rows :peak-rows])
                        :cost-search {:evaluated evaluated :states @checks :starts starts
                                      :elapsed-ns (- (System/nanoTime) started)})
            (let [root (first pending)
                  remaining (remove #(identical? root %) (drop (count early) choices))
                  ;; Keep optional lookahead connected: bind-dependent accesses
                  ;; precede additional independent scans once their inputs exist.
                  ordered (into early (cons root (concat (filter #(seq (:requires %)) remaining)
                                                          (remove #(seq (:requires %)) remaining))))
                  attempt (try
                            (dependency/schedule
                              (assoc arguments :choices ordered :max-states (max 1 (- maximum @checks))
                                     :check! #(do (check!)
                                                  (when (>= (vswap! checks inc) maximum)
                                                    (throw (ex-info "Cost search budget" {::budget true})))
                                                  (when (>= (- (System/nanoTime) started) 20000000)
                                                    (throw (ex-info "Cost search budget" {::budget true}))))))
                            (catch clojure.lang.ExceptionInfo e (when-not (::budget (ex-data e)) (throw e))))
                  priced (when (= :complete (:status attempt)) (price (:choices attempt) (:inputs arguments) row-count))
                  better? (and priced (neg? (compare (row-key priced) (row-key best-price))))]
              (recur (next pending) (if better? attempt best) (if better? priced best-price) (inc evaluated)
                     (conj starts (merge {:covers (:covers root) :status (:status attempt)}
                                         (select-keys priced [:work-rows :peak-rows])))))))))))
