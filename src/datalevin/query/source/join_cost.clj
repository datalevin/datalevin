(ns ^:no-doc datalevin.query.source.join-cost
  "Bounded lookahead over complete typed plans. No source or row access."
  (:require [clojure.set :as set]
            [datalevin.query.access.dependency :as dependency]))

(defn finite-cost
  "Saturate nonnegative advisory work at a finite double. Never wrap costs."
  [n]
  (let [n (double n)] (if (Double/isNaN n) Double/MAX_VALUE (min Double/MAX_VALUE n))))

(defn access-time
  "Price an admitted estimate over actual logical binding batches. Backend
  splitting is already included in per-binding hints, not extra cursor opens."
  [estimate tuples batch dependent?]
  (let [start (:startup-ns estimate) run (get estimate (if dependent? :per-binding-ns :run-ns))]
    (when (and (number? start) (number? run) (pos? batch))
      (let [opens (if dependent? (Math/ceil (/ (double tuples) (double batch))) 1.0)
            inputs (if dependent? (double tuples) 1.0)
            fixed (:requests-per-open estimate) per-binding (:requests-per-binding estimate)]
        {:access-ns (finite-cost (+ (* opens (finite-cost start)) (* inputs (finite-cost run))))
         :opens opens
         :requests (when (and (number? fixed) (or (not dependent?) (number? per-binding)))
                     (finite-cost (+ (* opens (finite-cost fixed))
                                     (if dependent? (* inputs (finite-cost per-binding)) 0.0))))}))))

(defn price
  "Estimate returned-row work along a complete path. Counts are advisory and
  floor the propagated tuple count at one; a zero never proves emptiness. A second independent
  scan sharing keys has unknown join selectivity and declines this model."
  ([selected inputs row-count] (price selected inputs row-count nil))
  ([selected inputs row-count model]
  (reduce
    (fn [{:keys [known tuples work-rows peak-rows access-ns local-ns estimated-opens estimated-requests] :as state}
         {:keys [kind value requires columns]}]
      (when state
        (if (= kind :core)
          (when (set/subset? (set (map :variable columns)) known)
            (let [local (finite-cost (+ (double local-ns)
                                        (* (double tuples) (+ (double (get model :local-row-ns 0))
                                                               (* (double (get model :local-column-ns 0)) (count requires))))))]
              (assoc state :local-ns local :estimated-ns (when (some? access-ns) (finite-cost (+ (double access-ns) local))))))
          (when-let [n (row-count value)]
            (let [output (set (map :variable columns))
                  dependent? (seq requires)
                  shared? (seq (set/intersection known output))
                  n (if (zero? (compare n 0)) 1.0 (double n))]
              (when (or dependent? (not shared?))
                (let [returned (max 1.0 (min Double/MAX_VALUE (* (double tuples) n)))
                      priced (when-let [f (:access-cost model)] (f value tuples))
                      access-ns (when (and (some? access-ns) (some? (:access-ns priced)))
                                  (finite-cost (+ (double access-ns) (double (:access-ns priced)))))
                      local (finite-cost (+ (double local-ns)
                                             (* (finite-cost (+ (double tuples) returned)) (double (get model :local-row-ns 0)))
                                             (* (double (get model :local-column-ns 0))
                                                (finite-cost (+ (* (double tuples) (count requires)) (* returned (count columns)))))))]
                  {:known (set/union known output) :tuples returned
                   :work-rows (min Double/MAX_VALUE (+ (double work-rows) (double tuples) returned))
                   :peak-rows (max (double peak-rows) returned)
                   :access-ns access-ns :local-ns local
                   :estimated-ns (when (some? access-ns) (finite-cost (+ (double access-ns) local)))
                   :estimated-opens (when (and (some? estimated-opens) (some? (:opens priced)))
                                      (finite-cost (+ (double estimated-opens) (double (:opens priced)))))
                   :estimated-requests (when (and (some? estimated-requests) (some? (:requests priced)))
                                         (finite-cost (+ (double estimated-requests) (double (:requests priced)))))})))))))
    {:known (set (map :variable inputs)) :tuples 1.0 :work-rows 0.0 :peak-rows 1.0
     :access-ns (when model 0.0) :local-ns 0.0 :estimated-ns (when model 0.0)
     :estimated-opens (when model 0.0) :estimated-requests (when model 0.0)} selected)))

(defn select
  "Keep the original feasible plan, then compare complete alternatives with a
  different first access. All attempts share the remaining 4096-state allowance
  and a 20 ms optional CPU budget. Small plans use at most 128 additional
  states and 1 ms. Unknown estimates retain the original plan."
  ([arguments row-count] (select arguments row-count nil))
  ([arguments row-count model]
  (let [checks (volatile! 0) check! (:check! arguments)
        counted! #(do (vreset! checks (inc (long @checks))) (check!))
        baseline (dependency/schedule (assoc arguments :check! counted!))
        initial (when (= :complete (:status baseline)) (price (:choices baseline) (:inputs arguments) row-count model))
        started (System/nanoTime) maximum (long (:max-states arguments))]
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
            ;; The baseline already evaluated this starting work. Repeating
            ;; its scan/lookup variants cannot discover a different driver.
            first-access (first (filter #(= :access (:kind %)) (:choices baseline)))
            work-key #(into #{} (map (fn [id] (if (vector? id) (first id) id))) (:covers %))
            roots (remove #(= (work-key first-access) (work-key %)) roots)
            small? (and (<= (count (:required-work arguments)) 8) (<= (count choices) 32))
            budget-ns (if small? 1000000 20000000)
            maximum (if small? (min maximum (+ (long @checks) 128)) maximum)
            early (vec (take-while #(= :core (:kind %)) choices))
            timed? (some? (:estimated-ns initial))
            row-key (if timed? (juxt :estimated-ns :work-rows :peak-rows) (juxt :work-rows :peak-rows))
            _ (when-not (pos? maximum) (throw (IllegalArgumentException. "invalid search bound")))]
        (loop [pending (seq roots) best baseline best-price initial evaluated 1 starts []]
          (if (or (nil? pending) (>= (long @checks) maximum) (>= (- (System/nanoTime) started) budget-ns))
            (assoc best :cost-changed? (not (and (= (count (:choices best)) (count (:choices baseline)))
                                                       (every? true? (map identical? (:choices best) (:choices baseline)))))
                        :join-cost (select-keys best-price [:work-rows :peak-rows :access-ns :local-ns :estimated-ns
                                                           :estimated-opens :estimated-requests])
                        :cost-search {:model (if timed? :elapsed :cardinality)
                                      :evaluated evaluated :states @checks :starts starts
                                      :budget-ns budget-ns :max-states maximum
                                      :elapsed-ns (- (System/nanoTime) started)})
            (let [root (first pending)
                  remaining (remove #(identical? root %) (drop (count early) choices))
                  ;; Keep optional lookahead connected: bind-dependent accesses
                  ;; precede additional independent scans once their inputs exist.
                  ordered (into early (cons root (concat (filter #(seq (:requires %)) remaining)
                                                          (remove #(seq (:requires %)) remaining))))
                  attempt (try
                            (dependency/schedule
                              (assoc arguments :choices ordered :max-states (max 1 (- maximum (long @checks)))
                                     :check! #(do (check!)
                                                  (let [n (inc (long @checks))]
                                                    (vreset! checks n)
                                                    (when (>= n maximum)
                                                      (throw (ex-info "Cost search budget" {::budget true}))))
                                                  (when (>= (- (System/nanoTime) started) budget-ns)
                                                    (throw (ex-info "Cost search budget" {::budget true}))))))
                            (catch clojure.lang.ExceptionInfo e (when-not (::budget (ex-data e)) (throw e))))
                  priced (when (= :complete (:status attempt)) (price (:choices attempt) (:inputs arguments) row-count model))
                  better? (and priced (or (not timed?) (some? (:estimated-ns priced)))
                               (neg? (compare (row-key priced) (row-key best-price))))]
              (recur (next pending) (if better? attempt best) (if better? priced best-price) (inc evaluated)
                     (conj starts (merge {:covers (:covers root) :status (:status attempt)}
                                         (select-keys priced [:work-rows :peak-rows :access-ns :local-ns :estimated-ns
                                                             :estimated-opens :estimated-requests]))))))))))))
