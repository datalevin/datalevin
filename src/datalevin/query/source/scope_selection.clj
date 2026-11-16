(ns ^:no-doc datalevin.query.source.scope-selection
  "Select caller binding interfaces for scoped calls with the shared typed
  scheduler. Possible caller outputs never become unproved execution seeds."
  (:require [datalevin.query.source.access :as access]
            [datalevin.query.source.control :as control]
            [datalevin.query.source.runtime :as rt]
            [datalevin.query.source.value :refer [fail!]]))

(def ^:private max-variants 4096)

(defn needed? [work]
  (boolean (some (fn [w] (some #(get-in % [:step :scope-bindings :optional?]) (:operators w))) work)))

(defn- feasible [f]
  (try (f)
       (catch clojure.lang.ExceptionInfo e
         (when-not (and (= :query-source/unsupported (:error (ex-data e)))
                        (= :planning (:phase (ex-data e))))
           (throw e)))))

(defn select!
  "Compile feasible scoped interfaces before choosing the enclosing schedule.
  Every chosen child receives exactly the inputs its parent exports. Binding
  choices share the scheduler's dependency proof and the invocation's ledger."
  [runtime work planning]
  (let [by-id (into {} (map #(vector (get-in % [:region :region-id]) %)) work)
        root (some #(when (empty? (get-in % [:region :path])) %) work)
        variants (volatile! 0)
        check! #(control/check-active! (:control runtime))
        admit! (fn [width]
                 (check!)
                 (when (>= (long @variants) max-variants) (fail! :query-source/limit :planning))
                 (vswap! variants inc)
                 (let [token (control/reserve! (:control runtime)
                               {:retained-bytes (+ 2048 (* 256 width))})]
                   (rt/own! runtime #(control/release! token))))]
    (letfn [(keys! [columns]
              (reduce (fn [sets column]
                        (into sets (map (fn [keys]
                                          (admit! (inc (count keys)))
                                          (conj keys column))) sets))
                [[]] columns))
            (region! [id inputs]
              (check!)
              (let [w (by-id id)
                    operators
                    (mapv
                      (fn [operator]
                        (if-let [{:keys [columns build]} (get-in operator [:step :scope-bindings])]
                          (let [initial (build inputs [])
                                required (set (map :variable (get-in initial [:boundary :inputs])))
                                columns (remove #(contains? required (:variable %)) columns)
                                choices
                                (into []
                                  (keep (fn [keys]
                                          (feasible
                                            (fn []
                                              (admit! (count keys))
                                              (let [{:keys [boundary step]} (if (empty? keys) initial (build inputs keys))
                                                    children (mapv #(region! (:region-id %) (:inputs %))
                                                               (remove :empty? (:branches boundary)))
                                                    choice (access/core-operator! runtime id (:id operator)
                                                             (:inputs boundary)
                                                             (if (= :negation (:kind boundary)) [] (:columns boundary))
                                                             (:after operator) step)
                                                    size (reduce + 0 (map count children))
                                                    token (control/reserve! (:control runtime)
                                                            {:retained-bytes (+ 1024 (* 64 size))})]
                                                (rt/own! runtime #(control/release! token))
                                                (assoc choice :scope-plans (vec (mapcat identity children))))))))
                                  (keys! columns))]
                            (when (empty? choices) (fail! :query-source/unsupported :planning))
                            (assoc (first choices) :scope-alternatives choices))
                          operator)) (:operators w))
                    schedule (assoc (access/schedule! runtime id inputs (:requests w) (:candidates w) operators
                                      (assoc planning :required-vars (get-in w [:region :required-vars])))
                               :input-columns inputs)]
                (into [schedule]
                  (mapcat #(get-in % [:value :scope-plans])) (:operators schedule))))]
      (let [selected (if root (into {} (map (juxt :region-id identity))
                               (region! (get-in root [:region :region-id]) (:inputs root))) {})]
        (mapv (fn [w]
                (let [id (get-in w [:region :region-id])]
                  (or (get selected id)
                      {:region-id id :policy :eav-structural-v1 :plans [] :operators []
                       :input-columns [] :planning-time-ms 0.0}))) work)))))
