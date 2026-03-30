(ns datalevin.jepsen.workload.util
  (:require
   [datalevin.jepsen.local :as local]
   [jepsen.checker :as checker]))

(defn terminal-op?
  [op]
  (contains? #{:ok :fail :info} (:type op)))

(defn expected-disruption-failures
  [test history pred]
  (->> history
       (filter pred)
       (filter terminal-op?)
       (filter (fn [{:keys [error]}]
                 (local/expected-disruption-write-failure? test error)))
       vec))

(defn disruption-failure-samples
  [ops sample-keys]
  (vec (take 10
             (map #(select-keys % sample-keys) ops))))

(defn read-only-micro-op-txn?
  [op]
  (and (= :ok (:type op))
       (sequential? (:value op))
       (seq (:value op))
       (every? (fn [micro-op]
                 (= :r (first micro-op)))
               (:value op))))

(defn append-only-micro-op-txn?
  [op]
  (and (= :ok (:type op))
       (sequential? (:value op))
       (seq (:value op))
       (every? (fn [micro-op]
                 (= :append (first micro-op)))
               (:value op))))

(defn append-graph-ignorable-micro-op-txn?
  [op]
  (or (read-only-micro-op-txn? op)
      (append-only-micro-op-txn? op)))

(defn wrap-empty-graph-checker
  ([base-checker pred sample-keys]
   (wrap-empty-graph-checker base-checker
                             pred
                             sample-keys
                             (constantly false)))
  ([base-checker pred sample-keys ignorable-terminal?]
   (reify checker/Checker
     (check [_ test history opts]
       (let [result               (checker/check base-checker test history opts)
             disruption-failures  (expected-disruption-failures test history pred)
             terminal             (->> history
                                       (filter pred)
                                       (filter terminal-op?))
             checked-terminal     (remove (fn [op]
                                            (or ((set disruption-failures) op)
                                                (ignorable-terminal? op)))
                                          terminal)
             empty-graph?         (true? (get-in result
                                                 [:anomalies
                                                  :empty-transaction-graph]))
             only-ignorable?      (and (= :unknown (:valid? result))
                                       empty-graph?
                                       (pos? (count terminal))
                                       (empty? checked-terminal))
             only-disruption?     (and (= :unknown (:valid? result))
                                       empty-graph?
                                       (pos? (count disruption-failures))
                                       (empty? checked-terminal))
             failure-summary      {:disruption-failure-count
                                   (count disruption-failures)
                                   :disruption-failure-samples
                                   (disruption-failure-samples
                                     disruption-failures
                                     sample-keys)}]
         (cond-> (merge result failure-summary)
           only-ignorable?
           (assoc :valid? true
                  :base-valid? (:valid? result)
                  :adjusted-valid? :ignorable-empty-graph)

           only-disruption?
           (assoc :valid? true
                  :base-valid? (:valid? result)
                  :adjusted-valid? :disruption-only-empty-graph)))))))
