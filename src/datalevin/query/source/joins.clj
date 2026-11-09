(ns ^:no-doc datalevin.query.source.joins
  "Pure, bounded join alternatives across aliases already acquired together."
  (:require [clojure.set :as set]
            [datalevin.query.source :as source]
            [datalevin.query.source.control :as control]
            [datalevin.query.source.contract :as contract]
            [datalevin.query.source.envelope :as envelope]
            [datalevin.query.source.runtime :as rt]
            [datalevin.query.source.value :refer [fail!]]))

(defn- merged-request [requests group maximum]
  (let [leader (first requests)
        clauses (mapcat (fn [request]
                          (map #(cond-> % (= :pattern (:kind %))
                                  (assoc :source-symbol (:source-symbol request))) (:clauses request))) requests)
        by-id (group-by :id clauses)
        _ (when-not (every? (fn [items] (or (= 1 (count items))
                                       (and (every? #(= :predicate (:kind %)) items)
                                            (apply = (map #(select-keys % [:operator :semantics-version]) items))))) (vals by-id)) (fail! :query-source/invalid :planning))]
    (assoc leader :max-alternatives maximum
           :datalevin.query.source/region-sources
           (mapv (fn [request]
                   (let [entry (some #(when (= (:symbol %) (:source-symbol request)) %) (:entries group))]
                     (select-keys entry [:symbol :descriptor]))) requests)
           :clauses (mapv #(first (by-id %)) (sort (keys by-id)))
           :bindings (apply merge (map :bindings requests))
           :required-vars (apply set/union (map :required-vars requests))
           :available-inputs (vec (vals (into (sorted-map)
                                            (map (juxt :variable identity))
                                            (mapcat :available-inputs requests)))))))

(defn plan!
  "Borrow only core-captured descriptors, requests and sessions. Joined
  alternatives share the leader's existing candidate quota and lifecycle."
  [runtime {:keys [requests region operators]} by-alias regular]
  (let [ctl (:control runtime)
        groups (vals (into (array-map) (map (fn [request]
                                             (let [group (by-alias (:source-symbol request))]
                                               [(first (:aliases group)) group]))) requests))]
    (into []
      (mapcat
        (fn [group]
          (let [members (filterv #(some #{(:source-symbol %)} (:aliases group)) requests)
                leader (first members)
                entry (some #(when (= (:symbol %) (:source-symbol leader)) %) (:entries group))
                provider (:source entry)
                existing (some #(when (identical? leader (:request %)) (:response %)) regular)
                maximum (- (:max-alternatives leader)
                           (if existing (count (envelope/alternatives runtime existing)) 0))]
            (if (and (empty? (:path region)) (<= 2 (count members) 32) (pos? maximum)
                     (satisfies? source/IQuerySourceJoinPlanning provider))
              (let [scratch (control/reserve! ctl {:retained-bytes (+ 131072 (* 2048 (reduce + (map #(count (:clauses %)) members))))})]
                (try
                  (let [request (assoc (merged-request members group maximum)
                                       :required-vars
                                       (set/union (set (:required-vars region)) (:input-vars region)
                                                  (into #{} (map :variable) (mapcat #(concat (:requires %) (:columns %)) operators))
                                                  (into #{} (mapcat (fn [request]
                                                                     (mapcat contract/variables
                                                                             (filter #(= :pattern (:kind %)) (:clauses request)))))
                                                        (remove #(some #{(:source-symbol %)} (:aliases group)) requests))))
                        token (rt/charge-copy! runtime (dissoc request :control))]
                    (rt/own! runtime #(control/release! token))
                    (envelope/request! runtime request)
                    [{:request request :group group
                      :response (rt/with-context runtime {:source-symbol (:source-symbol leader) :region-id (:region-id leader)}
                                  (fn [runtime]
                                    (envelope/capture-response! runtime request
                                      (rt/invoke runtime :planning #(source/-plan-joins provider (:session group) request))
                                      (:consumption group))))}])
                  (finally (control/release! scratch)))) []))) groups))))
