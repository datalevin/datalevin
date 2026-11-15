(ns ^:no-doc datalevin.query.source.union-selection
  "Bounded selection of independent and bound flat-union interfaces.
  Reuses captured branch accesses; no provider callbacks, rows or new executor."
  (:require [datalevin.query.source.access :as access]
            [datalevin.query.source.control :as control]
            [datalevin.query.source.runtime :as rt]))

(defn- schedule! [runtime work inputs operators planning]
  (assoc (access/schedule! runtime (get-in work [:region :region-id]) inputs
           (:requests work) (:candidates work) operators
           (assoc planning :required-vars (get-in work [:region :required-vars])))
    :input-columns inputs))

(defn- branch-model [runtime schedule planning]
  (when (= 1 (count (:plans schedule)))
    (access/repeatable-estimate runtime (first (:plans schedule)) planning)))

(defn- key-sets [columns]
  (mapv (fn [mask] (into [] (keep-indexed #(when (bit-test mask %1) %2)) columns))
        (range 1 (bit-shift-left 1 (count columns)))))

(defn- optional-schedule! [schedule]
  (try (schedule)
       (catch clojure.lang.ExceptionInfo e
         ;; A bounded optional dependency search may decline. Invalid envelopes,
         ;; limits and cancellation are never converted into an optimizer choice.
         (when-not (and (= :query-source/unsupported (:error (ex-data e)))
                        (= :planning (:phase (ex-data e))))
           (throw e)))))

(defn- variants! [runtime operator by-id base planning allowance]
  (let [{:keys [columns branches build]} (:conditional-union (:step operator))
        token (control/reserve! (:control runtime)
                {:retained-bytes (+ 16384 (* 4096 (count branches) (bit-shift-left 1 (count columns))))})
        _ (rt/own! runtime #(control/release! token))
        independent (mapv base branches)
        scan (mapv #(branch-model runtime % planning) independent)]
    (when (every? #(and % (not (:dependent? %))) scan)
      (let [bound
            (into []
              (keep
                (fn [keys]
                  (control/check-active! (:control runtime))
                  (when (<= (count branches) @allowance)
                    (vswap! allowance - (count branches))
                    (let [interface (build keys)
                          schedules
                          (optional-schedule!
                            (fn []
                            (mapv (fn [{:keys [region-id inputs]}]
                                    (let [work (by-id region-id)]
                                      (schedule! runtime work inputs (:operators work) planning))) (:branches interface))))
                          models (when schedules (mapv #(branch-model runtime % planning) schedules))]
                      (when (and (seq models) (every? #(and % (:dependent? %)) models))
                        (let [variant (access/core-operator! runtime (:region-id operator) (:id operator)
                                        (:inputs interface) (:columns operator) (:after operator) (:step interface))]
                          {:key (set (map :variable (:inputs interface)))
                           :models models :operator variant :schedules schedules}))))))
              (key-sets columns))
            lookups (into {} (map (juxt :key :models)) bound)]
        (when (seq bound)
          (let [hint {:scan scan :lookups lookups}
                independent (assoc operator :union-estimate hint :scope-plans independent :binding-inputs [])
                choices (into [independent]
                          (map (fn [{:keys [operator schedules]}]
                                 (assoc operator :union-estimate hint :scope-plans schedules
                                   :binding-inputs (mapv :variable (:requires operator))))) bound)]
            (assoc independent :union-alternatives choices)))))))

(defn- cost-key [a b]
  (if (and (number? (:estimated-ns a)) (number? (:estimated-ns b)))
    (juxt :estimated-ns :work-rows :peak-rows)
    (juxt :work-rows :peak-rows)))

(defn- select-schedules!
  "Keep the original complete schedule as fallback. Optional branch interfaces
  share captured plans and a 128-branch planning allowance. Only homogeneous,
  repeatable flat unions supplied by the compiler enter this comparison."
  [runtime work planning]
  (let [base (into {} (map (fn [w]
                            [(get-in w [:region :region-id])
                             (schedule! runtime w (:inputs w) (:operators w) planning)])) work)
        root (some #(when (empty? (get-in % [:region :path])) %) work)
        operators (:operators root)
        conditional (filterv #(get-in % [:step :conditional-union]) operators)
        ;; Leave room for final validation/executable assembly under small caps.
        remaining (- (:retained-bytes (control/limits (:control runtime)))
                     (:retained-bytes (control/usage (:control runtime))))]
    (if (or (empty? conditional) (< remaining 16777216))
      (mapv #(base (get-in % [:region :region-id])) work)
      (let [by-id (into {} (map #(vector (get-in % [:region :region-id]) %)) work)
            allowance (volatile! 128)
            changed (into {} (keep (fn [op]
                                    (when-let [v (variants! runtime op by-id base planning allowance)]
                                      [(:id op) v]))) conditional)
            root-id (get-in root [:region :region-id])
            decorated (mapv #(get changed (:id %) %) operators)
            original (update (base root-id) :operators
                       (fn [ops]
                         (mapv (fn [choice]
                                 (if (= :core (:kind choice))
                                   (if-let [op (get changed (get-in choice [:value :id]))]
                                     (assoc choice :value (first (:union-alternatives op))) choice)
                                   choice)) ops)))
            original-cost (when (seq changed) (access/price-schedule runtime original planning))
            candidate (when original-cost
                        (optional-schedule! #(schedule! runtime root (:inputs root) decorated planning)))
            candidate-cost (:join-cost candidate)
            rank (cost-key original-cost candidate-cost)
            chosen (if (and candidate-cost (neg? (compare (rank candidate-cost) (rank original-cost)))) candidate original)
            cost (if (identical? chosen candidate) candidate-cost original-cost)
            selected (reduce (fn [schedules {:keys [kind value]}]
                               (if (and (= :core kind) (:scope-plans value))
                                 (reduce #(assoc %1 (:region-id %2) %2) schedules (:scope-plans value))
                                 schedules)) base (:operators chosen))
            chosen (cond-> chosen
                     cost (assoc :union-selection
                            {:alternatives (reduce + 0 (map #(count (:union-alternatives %)) (vals changed)))
                             :original-cost (select-keys original-cost [:work-rows :peak-rows :estimated-ns])
                             :selected-cost (select-keys cost [:work-rows :peak-rows :estimated-ns])
                             :bindings (into {} (keep (fn [{:keys [kind value]}]
                                                       (when (and (= :core kind) (:union-estimate value))
                                                         [(:id value) (:binding-inputs value)]))) (:operators chosen))}))
            selected (assoc selected root-id chosen)]
        ;; Every selected region still appears exactly once in the original order.
        (mapv #(selected (get-in % [:region :region-id])) work)))))

(defn select!
  "Select region schedules and account for all optional comparison work.
  Branch interfaces are planned once per candidate key, never during execution."
  [runtime work planning]
  (let [started (System/nanoTime)
        schedules (select-schedules! runtime work planning)
        elapsed (/ (double (- (System/nanoTime) started)) 1000000.0)
        accounted (reduce + 0.0 (map :planning-time-ms schedules))
        extra (max 0.0 (- elapsed accounted))
        root (some #(when (empty? (get-in % [:region :path])) (get-in % [:region :region-id])) work)]
    (mapv (fn [schedule]
            (if (= root (:region-id schedule))
              (cond-> (update schedule :planning-time-ms + extra)
                (:union-selection schedule) (assoc-in [:union-selection :comparison-time-ms] extra))
              schedule)) schedules)))
