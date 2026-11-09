(ns ^:no-doc datalevin.query.source.estimation
  "Cost-aware, bounded provider estimate acquisition before pure access planning."
  (:require [datalevin.query.source :as source]
            [datalevin.query.source.control :as control]
            [datalevin.query.source.runtime :as rt]
            [datalevin.query.source.value :as value :refer [fail!]]))

(defn- finite-nonnegative? [n]
  (and (number? n) (Double/isFinite (double n)) (not (neg? n))))

(defn work-budget
  "Spend at most one eighth of the cheapest independent access's estimated
  fetch work. This is a conservative opportunity bound, not expected savings."
  [infos]
  (when (and (seq infos) (every? some? infos))
    (let [known (filter #(some? (:rows %)) infos)]
      (when (seq known)
        (let [best (first (sort-by :rows known))]
          (when (and (pos? (:rows best)) (some? (:request-ns best)))
            (long (min Long/MAX_VALUE
                       (* 0.125 (:request-ns best)
                          (Math/ceil (/ (double (:rows best)) (:batch-rows best))))))))))))

(defn request-budget [info remaining-ns remaining-requests]
  (when (and (pos? (:probe-requests info)) (some? (:estimated-ns info))
             (pos? (:estimated-ns info)) (pos? remaining-ns) (pos? remaining-requests))
    (let [per-request (/ (double (:estimated-ns info)) (:probe-requests info))
          n (long (min (:probe-requests info) remaining-requests
                       (Math/floor (/ (double remaining-ns) per-request))))]
      (when (pos? n) {:max-requests n :max-elapsed-ns remaining-ns}))))

(defn- quote! [runtime provider session request]
  (when (satisfies? source/IQuerySourceEstimation provider)
    (when-let [info (rt/invoke runtime :planning #(source/-estimation-info provider session request))]
      (when-not (and (value/plain-map? info)
                     (= #{:rows :batch-rows :request-ns :probe-requests :estimated-ns} (set (keys info)))
                     (or (nil? (:rows info)) (finite-nonnegative? (:rows info)))
                     (integer? (:batch-rows info)) (<= 1 (:batch-rows info) Long/MAX_VALUE)
                     (integer? (:probe-requests info)) (<= 0 (:probe-requests info) (:max-alternatives request))
                     (every? #(or (nil? %) (finite-nonnegative? %)) [(:request-ns info) (:estimated-ns info)]))
        (fail! :query-source/invalid :planning))
      (let [token (rt/charge-copy! runtime info)]
        (rt/own! runtime #(control/release! token))
        (value/owned-copy info)))))

(defn- driver-groups [requests]
  (into #{} (mapcat (fn [request]
                     (for [{:keys [id kind terms]} (:clauses request) :when (= :pattern kind)]
                       [(:source-symbol request) (if (= :ignore (ffirst terms)) [:occurrence id] (first terms))])))
        requests))

(defn prepare!
  "Price optional probes using acquired-session observations. The configured
  time/request caps are shared across regions. Soft time caps are checked between
  calls; the invocation deadline bounds an individual slow provider request."
  [runtime regions by-alias planning]
  (let [ctl (:control runtime) started (System/nanoTime)
        maximum (long (min Long/MAX_VALUE (*' 1000000 (get planning :estimate-budget-ms 100))))
        maximum (if-let [remaining (source/remaining-time-ms ctl)]
                  (long (min maximum (*' 1000000 remaining))) maximum)
        left (volatile! (get planning :estimate-max-requests 8))]
    (into {}
      (map
        (fn [{:keys [region requests inputs]}]
          (control/check-active! ctl)
          (let [begin (System/nanoTime) spent #(- (System/nanoTime) started)
                base {:requests 0 :elapsed-ns 0 :estimated-provider-ns 0 :budget-ns 0}
                report
                (cond
                  (or (zero? maximum) (zero? (get planning :estimate-max-requests 8)))
                  (assoc base :decision :disabled)
                  (zero? @left) (assoc base :decision :skipped :reason :request-budget)
                  (or (seq inputs) (< (count (driver-groups requests)) 2))
                  (assoc base :decision :skipped :reason :no-independent-choice)
                  (>= (spent) maximum) (assoc base :decision :skipped :reason :time-budget)
                  :else
                  (let [quotes
                        (mapv (fn [request]
                                (let [group (by-alias (:source-symbol request))
                                      entry (some #(when (= (:source-symbol request) (:symbol %)) %) (:entries group))]
                                  {:provider (:source entry) :session (:session group) :request request
                                   :info (when (< (spent) maximum)
                                           (quote! runtime (:source entry) (:session group) request))})) requests)
                        allowance (work-budget (mapv :info quotes))
                        baseline (when (every? #(some? (:info %)) quotes)
                                   (some->> quotes (keep #(get-in % [:info :rows])) seq (apply min)))
                        candidates (filter #(and (some? (get-in % [:info :rows])) baseline
                                                 (> (get-in % [:info :rows]) baseline)
                                                 (pos? (get-in % [:info :probe-requests] 0))) quotes)
                        candidates (sort-by #(or (get-in % [:info :estimated-ns]) Double/POSITIVE_INFINITY) candidates)]
                    (loop [pending (seq candidates)
                           result (assoc base :budget-ns (or allowance 0) :decision :skipped
                                              :reason (cond (nil? allowance) :unknown-cost
                                                            (empty? candidates) :no-useful-probe
                                                            :else :cost-exceeds-budget))]
                      (if-let [{:keys [provider session request info]} (first pending)]
                        (let [remaining (long (max 0 (min (- maximum (spent))
                                                         (- (or allowance 0) (- (System/nanoTime) begin)))))
                              budget (request-budget info remaining @left)]
                          (if budget
                            (let [before (:external-requests (control/usage ctl))
                                  _ (rt/invoke runtime :acquisition
                                      #(source/-prepare-estimates provider session request budget))
                                  n (- (:external-requests (control/usage ctl)) before)]
                              (when (> n (:max-requests budget)) (fail! :query-source/invalid :acquisition))
                              (vswap! left - n)
                              (recur (next pending)
                                     (-> result (cond-> (pos? n) (assoc :decision :budgeted :reason nil)
                                                         (and (zero? n) (= :skipped (:decision result)))
                                                         (assoc :reason :provider-declined))
                                         (update :requests + n)
                                         (update :estimated-provider-ns +
                                                 (long (* n (/ (double (:estimated-ns info)) (:probe-requests info))))))))
                            (recur (next pending) result)))
                        result))))]
            [(:region-id region) (assoc report :elapsed-ns (- (System/nanoTime) begin))])) regions))))
