(ns ^:no-doc datalevin.query.source.union-selection
  "Bounded selection of independent and bound flat-union interfaces.
  Reuses captured branch accesses; no provider callbacks, rows or new executor."
  (:require [clojure.set :as set]
            [datalevin.query.access.dependency :as dependency]
            [datalevin.query.source.access :as access]
            [datalevin.query.source.control :as control]
            [datalevin.query.source.key-demand :as key-demand]
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
                  (when (<= (count branches) (long @allowance))
                    (vreset! allowance (- (long @allowance) (count branches)))
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

(defn- modeled-operator? [operator]
  (or (:union-estimate operator)
      (= :at-most-one (get-in operator [:step :scalar-estimate]))))

(defn- prefix-operators
  "Fence opaque operators behind the entire compared mandatory prefix. A
  dependency cycle declines this optional comparison in the normal scheduler.
  The suffix remains executable and keeps all outputs and qualifications."
  [work operators]
  (let [known? modeled-operator?
        region-id (get-in work [:region :region-id])
        required (into (into #{} (map #(vector :core region-id (:id %))) (filter known? operators))
                       (mapcat (fn [request]
                                 (for [clause (:clauses request) :when (= :pattern (:kind clause))]
                                   [(:source-symbol request) region-id (:id clause)]))) (:requests work))]
    (mapv #(if (known? %) %
             (assoc % :cost-barrier? true :after (into (:after %) required))) operators)))

(defn- prefix-schedule!
  "Preserve the baseline access choices and their order. Move only opaque
  operators to the suffix, and validate every typed dependency and work fence
  in that exact order. Never search for a different independent baseline."
  [runtime schedule operators planning]
  (let [token (control/reserve! (:control runtime)
                {:retained-bytes (+ 2048 (* 1024 (count (:operators schedule))))})
        _ (rt/own! runtime #(control/release! token))
        by-id (into {} (map (juxt :id identity)) operators)
        choices (mapv (fn [{:keys [kind value] :as choice}]
                        (if (= :core kind)
                          (assoc choice :value (dissoc (by-id (:id value)) :union-alternatives)) choice))
                      (:operators schedule))
        barrier? #(get-in % [:value :cost-barrier?])
        ordered (into (filterv (complement barrier?) choices) (filter barrier?) choices)
        valid (reduce
                (fn [{:keys [known done] :as state} {:keys [kind value]}]
                  (control/check-active! (:control runtime))
                  (when state
                    (let [o (if (= :access kind) (get-in value [:path :options]) value)
                          covers (if (= :access kind) (:covers (:expr value))
                                   #{[:core (:region-id schedule) (:id value)]})]
                      (when (and (set/subset? (or (:after o) #{}) done)
                                 (every? #(dependency/supplies? (known (:variable %)) %) (:requires o)))
                        (when-let [columns (dependency/extend-columns known (:columns o))]
                          {:known columns :done (into done covers)})))))
                {:known (into {} (map (juxt :variable identity)) (:input-columns schedule)) :done #{}} ordered)]
    (when valid
      (let [prefix (assoc schedule :operators ordered :cost-search nil)]
        (when-let [cost (access/price-schedule runtime prefix planning)]
          (assoc prefix :join-cost cost))))))

(defn- select-schedules!
  "Keep the original complete schedule as fallback. Optional branch interfaces
  share captured plans and a 128-branch planning allowance. Only homogeneous,
  repeatable flat unions supplied by the compiler enter this comparison."
  [runtime work planning context]
  (let [base (into {} (map (fn [w]
                            [(get-in w [:region :region-id])
                             (schedule! runtime w (:inputs w) (:operators w) planning)])) work)
        root (some #(when (empty? (get-in % [:region :path])) %) work)
        operators (:operators root)
        conditional (filterv #(get-in % [:step :conditional-union]) operators)
        ;; Leave room for final validation/executable assembly under small caps.
        remaining (- (long (:retained-bytes (control/limits (:control runtime))))
                     (long (:retained-bytes (control/usage (:control runtime)))))]
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
            whole-cost (when (seq changed) (access/price-schedule runtime original planning))
            comparison-operators (if whole-cost decorated (prefix-operators root decorated))
            prefix (when (and (seq changed) (nil? whole-cost))
                     (prefix-schedule! runtime original
                       (mapv (fn [op]
                               (if-let [independent (first (:union-alternatives op))]
                                 independent op)) comparison-operators) planning))
            original-cost (or whole-cost (:join-cost prefix))
            original (if original-cost (or prefix original) original)
            ;; Do not move an opaque mandatory qualification merely to
            ;; make costing possible. Recover only a fully modeled flat body.
            recover? (and (nil? original-cost) (seq changed)
                          (every? modeled-operator? decorated))
            candidate (when (or original-cost recover?)
                        (optional-schedule!
                          #(schedule! runtime root (:inputs root) comparison-operators
                             (cond-> planning recover? (assoc :seed-unpriced-unions? true)))))
            candidate-cost (:join-cost candidate)
            rank (cost-key original-cost candidate-cost)
            chosen (if (and candidate-cost
                            (or (nil? original-cost)
                                (neg? (compare (rank candidate-cost) (rank original-cost))))) candidate original)
            cost (if (identical? chosen candidate) candidate-cost original-cost)
            selected (reduce (fn [schedules {:keys [kind value]}]
                               (if (and (= :core kind) (:scope-plans value))
                                 (reduce #(assoc %1 (:region-id %2) %2) schedules (:scope-plans value))
                                 schedules)) base (:operators chosen))
            chosen (cond-> chosen
                     cost (assoc :union-selection
                            {:alternatives (reduce + 0 (map #(count (:union-alternatives %)) (vals changed)))
                             :original-cost (select-keys original-cost [:work-rows :peak-rows :estimated-ns])
                             :baseline-unpriced? (nil? original-cost)
                             :selected-cost (select-keys cost [:work-rows :peak-rows :estimated-ns])
                             :candidate-cost (select-keys candidate-cost [:work-rows :peak-rows :estimated-ns])
                             :candidate-search (:cost-search candidate)
                             :cost-scope (get cost :cost-scope :complete)
                             :bindings (into {} (keep (fn [{:keys [kind value]}]
                                                       (when (and (= :core kind) (:union-estimate value))
                                                         [(:id value) (:binding-inputs value)]))) (:operators chosen))}))
            selected (assoc selected root-id chosen)
            schedules (mapv #(selected (get-in % [:region :region-id])) work)
            demand (when context (key-demand/select! runtime work schedules changed planning context))]
        ;; Every selected region still appears exactly once in the original order.
        (mapv #(cond-> % (and demand (= root-id (:region-id %))) (assoc :key-demand demand)) schedules)))))

(defn select!
  "Select region schedules and account for all optional comparison work.
  Branch interfaces are planned once per candidate key, never during execution."
  ([runtime work planning] (select! runtime work planning nil))
  ([runtime work planning context]
  (let [started (System/nanoTime)
        schedules (select-schedules! runtime work planning context)
        elapsed (/ (double (- (System/nanoTime) started)) 1000000.0)
        accounted (double (reduce + 0.0 (map :planning-time-ms schedules)))
        extra (max 0.0 (- elapsed accounted))
        root (some #(when (empty? (get-in % [:region :path])) (get-in % [:region :region-id])) work)]
    (mapv (fn [schedule]
            (if (= root (:region-id schedule))
              (cond-> (update schedule :planning-time-ms + extra)
                (:union-selection schedule) (assoc-in [:union-selection :comparison-time-ms] extra))
              schedule)) schedules))))
