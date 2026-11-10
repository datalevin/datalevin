(ns ^:no-doc datalevin.query.source.estimation
  "Cost-aware, bounded provider estimate acquisition before pure access planning."
  (:require [datalevin.query.source :as source]
            [datalevin.query.source.control :as control]
            [datalevin.query.source.runtime :as rt]
            [datalevin.query.source.value :as value :refer [fail!]]))

(defn- finite-nonnegative? [n]
  (and (number? n) (Double/isFinite (double n)) (not (neg? (compare n 0)))))

(defn work-budget
  "Spend at most one eighth of the cheapest independent access's estimated
  fetch work among substantial relations when base sizes are supplied. Tiny
  dimensions cannot price the whole join. This is opportunity, not savings."
  [infos]
  (when (and (seq infos) (every? some? infos))
    (let [known (filter #(some? (:rows %)) infos)
          substantial (remove #(and (some? (:relation-rows %))
                                    (value/at-most? (:relation-rows %) (long (:batch-rows %)))) known)
          ;; A tiny dimension cannot price an entire join's opportunity.
          ;; A selective lookup into a large relation still constrains it.
          known (if (seq substantial) substantial known)]
      (when (seq known)
        (let [best (first (sort-by :rows known))]
          (when (and (pos? (compare (:rows best) 0)) (some? (:request-ns best)))
            (let [budget (* 0.125 (double (:request-ns best))
                            (Math/ceil (/ (double (:rows best)) (double (:batch-rows best)))))]
              (if (>= budget (double Long/MAX_VALUE)) Long/MAX_VALUE (long budget)))))))))

(defn request-budget [info ^long remaining-ns ^long remaining-requests]
  (let [probes (long (:probe-requests info))
        estimated (:estimated-ns info)]
    (when (and (pos? probes) (some? estimated)
               (pos? (compare estimated 0)) (pos? remaining-ns) (pos? remaining-requests))
      (let [per-request (/ (double estimated) (double probes))
            limit (min probes remaining-requests)
            affordable (Math/floor (/ (double remaining-ns) per-request))
            n (if (>= affordable (double limit)) limit (long affordable))]
        (when (pos? n) {:max-requests n :max-elapsed-ns remaining-ns})))))

(defn- millis->nanos ^long [^long millis]
  (if (> millis (quot Long/MAX_VALUE 1000000))
    Long/MAX_VALUE
    (* 1000000 millis)))

(defn- quote! [runtime provider session request]
  (when (satisfies? source/IQuerySourceEstimation provider)
    (when-let [info (rt/invoke runtime :planning #(source/-estimation-info provider session request))]
      (when-not (and (value/plain-map? info)
                     (every? #(contains? info %) [:rows :batch-rows :request-ns :probe-requests :estimated-ns])
                     (every? #{:rows :relation-rows :batch-rows :request-ns :probe-requests :estimated-ns} (keys info))
                     (or (nil? (:relation-rows info)) (finite-nonnegative? (:relation-rows info)))
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
        maximum (millis->nanos (get planning :estimate-budget-ms 100))
        maximum (if-let [remaining (source/remaining-time-ms ctl)]
                  (min maximum (millis->nanos remaining)) maximum)
        max-requests (long (get planning :estimate-max-requests 8))
        left (volatile! max-requests)]
    (into {}
      (map
        (fn [{:keys [region requests inputs]}]
          (control/check-active! ctl)
          (let [begin (System/nanoTime)
                base {:requests 0 :elapsed-ns 0 :estimated-provider-ns 0 :budget-ns 0}
                report
                (cond
                  (or (zero? maximum) (zero? max-requests))
                  (assoc base :decision :disabled)
                  (zero? (long @left)) (assoc base :decision :skipped :reason :request-budget)
                  (or (seq inputs) (< (count (driver-groups requests)) 2))
                  (assoc base :decision :skipped :reason :no-independent-choice)
                  (>= (- (System/nanoTime) started) maximum) (assoc base :decision :skipped :reason :time-budget)
                  :else
                  (let [quotes
                        (mapv (fn [request]
                                (let [group (by-alias (:source-symbol request))
                                      entry (some #(when (= (:source-symbol request) (:symbol %)) %) (:entries group))]
                                  {:provider (:source entry) :session (:session group) :request request
                                   :info (when (< (- (System/nanoTime) started) maximum)
                                           (quote! runtime (:source entry) (:session group) request))})) requests)
                        allowance (work-budget (mapv :info quotes))
                        baseline (when (every? #(some? (:info %)) quotes)
                                   (some->> quotes (keep #(get-in % [:info :rows])) seq (apply min)))
                        candidates (filter #(and (some? (get-in % [:info :rows])) baseline
                                                 (pos? (compare (get-in % [:info :rows]) baseline))
                                                 (pos? (long (get-in % [:info :probe-requests] 0)))) quotes)
                        candidates (sort-by #(or (get-in % [:info :estimated-ns]) Double/POSITIVE_INFINITY) candidates)]
                    (loop [pending (seq candidates)
                           result (assoc base :budget-ns (or allowance 0) :decision :skipped
                                              :reason (cond (nil? allowance) :unknown-cost
                                                            (empty? candidates) :no-useful-probe
                                                            :else :cost-exceeds-budget))]
                      (if-let [{:keys [provider session request info]} (first pending)]
                        (let [remaining (max 0 (min (- maximum (- (System/nanoTime) started))
                                                   (- (long (or allowance 0)) (- (System/nanoTime) begin))))
                              budget (request-budget info remaining (long @left))]
                          (if budget
                            (let [before (long (:external-requests (control/usage ctl)))
                                  _ (rt/invoke runtime :acquisition
                                      #(source/-prepare-estimates provider session request budget))
                                  n (- (long (:external-requests (control/usage ctl))) before)]
                              (when (> n (long (:max-requests budget))) (fail! :query-source/invalid :acquisition))
                              (vreset! left (- (long @left) n))
                              (recur (next pending)
                                     (-> result (cond-> (pos? n) (assoc :decision :budgeted :reason nil)
                                                         (and (zero? n) (= :skipped (:decision result)))
                                                         (assoc :reason :provider-declined))
                                         (update :requests + n)
                                         (update :estimated-provider-ns +
                                                 (long (* n (/ (double (:estimated-ns info)) (double (:probe-requests info)))))))))
                            (recur (next pending) result)))
                        result))))]
            [(:region-id region) (assoc report :elapsed-ns (- (System/nanoTime) begin))])) regions))))
