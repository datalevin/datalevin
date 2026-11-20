(ns ^:no-doc datalevin.query.source.join-cost
  "Bounded lookahead over typed plans and fenced prefixes. No source or row access."
  (:require [clojure.set :as set]
            [datalevin.query.access.dependency :as dependency]))

(defn finite-cost
  "Saturate nonnegative advisory work at a finite double. Never wrap costs."
  ^double [n]
  (let [n (double n)] (if (Double/isNaN n) Double/MAX_VALUE (min Double/MAX_VALUE n))))

(defn access-time
  "Price an admitted estimate over actual logical binding batches. Backend
  splitting is already included in per-binding hints, not extra cursor opens."
  [estimate tuples batch dependent?]
  (let [start (:startup-ns estimate) run (get estimate (if dependent? :per-binding-ns :run-ns))]
    (when (and (number? start) (number? run) (pos? (long (compare batch 0))))
      (let [opens (if dependent? (Math/ceil (/ (double tuples) (double batch))) 1.0)
            inputs (if dependent? (double tuples) 1.0)
            fixed (:requests-per-open estimate) per-binding (:requests-per-binding estimate)]
        {:access-ns (finite-cost (+ (* opens (finite-cost start)) (* inputs (finite-cost run))))
         :opens opens
         :requests (when (and (number? fixed) (or (not dependent?) (number? per-binding)))
                     (finite-cost (+ (* opens (finite-cost fixed))
                                     (if dependent? (* inputs (finite-cost per-binding)) 0.0))))}))))

(defn- union-price
  [state {:keys [value requires columns]} model]
  (let [hint (:union-estimate value)
        known (:known state)
        shared (set/intersection known (set (map :variable columns)))
        key (if (seq requires) (set (map :variable requires))
              (first (sort-by (comp - count)
                       (filter #(set/subset? % shared) (keys (:lookups hint))))))
        fanout (if (seq shared) (get-in hint [:lookups key]) (:scan hint))
        branches (if (seq requires) (get-in hint [:lookups key]) (:scan hint))]
    (when (and (seq fanout) (seq branches))
      (let [rows #(finite-cost (reduce + 0.0 (map (fn [b] (if (zero? (compare (:rows b) 0)) 1.0
                                                          (double (:rows b)))) %)))
            tuples (double (:tuples state))
            matches (max 1.0 (finite-cost (* tuples (double (rows fanout)))))
            read (if (seq requires) matches (double (rows branches)))
            ;; A grouped lookup consumes every witness but emits at most one
            ;; group per outer key. Estimate independent groups from fanout.
            returned (cond
                       (not (:grouped? hint)) matches
                       (seq shared) tuples
                       :else (let [rows-per-group (double (rows (first (vals (:lookups hint)))))]
                               (finite-cost (* tuples (max 1.0 (/ read rows-per-group))))))
            priced (when model (mapv #(access-time (:estimate %) (if (seq requires) tuples 1.0)
                                                    (:batch %) (:dependent? %)) branches))
            sum-cost (fn [key]
                       (when (and (some? (get state key)) (every? #(number? (get % key)) priced))
                         (finite-cost (+ (double (get state key)) (double (reduce + 0.0 (map key priced)))))))
            access (when priced (sum-cost :access-ns))
            work (finite-cost (+ tuples read returned))
            local (finite-cost (+ (double (:local-ns state))
                                   (* work (double (get model :local-row-ns 0)))
                                   (* (finite-cost (+ (* tuples (count requires)) (* returned (count columns))))
                                      (double (get model :local-column-ns 0)))))
            opens (when (and priced (some? (:estimated-opens state)) (every? #(number? (:opens %)) priced))
                    (finite-cost (+ (double (:estimated-opens state)) (double (reduce + 0.0 (map :opens priced))))))
            requests (when (and priced (some? (:estimated-requests state)) (every? #(number? (:requests %)) priced))
                       (finite-cost (+ (double (:estimated-requests state)) (double (reduce + 0.0 (map :requests priced))))))]
        {:known (into known (map :variable columns)) :tuples returned
         :work-rows (finite-cost (+ (double (:work-rows state)) work))
         :peak-rows (max (double (:peak-rows state)) read returned)
         :access-ns access :local-ns local :estimated-opens opens :estimated-requests requests
         :estimated-ns (when (some? access) (finite-cost (+ (double access) local)))}))))

(defn price
  "Estimate returned-row work along a complete path or explicitly fenced prefix. Counts are advisory and
  floor the propagated tuple count at one; a zero never proves emptiness. A second independent
  scan sharing keys has unknown join selectivity and declines this model."
  ([selected inputs row-count] (price selected inputs row-count nil))
  ([selected inputs row-count model]
  (reduce
    (fn [{:keys [known tuples work-rows peak-rows access-ns local-ns estimated-opens estimated-requests] :as state}
         {:keys [kind value requires columns] :as choice}]
      (when state
        (cond
          ;; Only the selector may mark an opaque suffix. Its dependencies
          ;; require all compared work to finish first; never price its fanout.
          (:cost-barrier? value) (assoc state :cost-scope :mandatory-prefix)
          (= :mandatory-prefix (:cost-scope state)) nil
          (= kind :core)
          (if (:union-estimate value)
            (union-price state choice model)
            (when (or (= :at-most-one (get-in value [:step :scalar-estimate]))
                      (set/subset? (set (map :variable columns)) known))
            (let [local (finite-cost (+ (double local-ns)
                                        (* (double tuples) (+ (double (get model :local-row-ns 0))
                                                               (* (double (get model :local-column-ns 0)) (count requires))))))]
              (assoc state :known (into known (map :variable columns))
                           :local-ns local :estimated-ns (when (some? access-ns) (finite-cost (+ (double access-ns) local)))))))
          :else
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

(defn- root-work [choice]
  (if (= :core (:kind choice)) (:covers choice)
    (into #{} (map #(if (vector? %) (first %) %)) (:covers choice))))

(defn- qualification-model
  "Recognize cheap mandatory leaf reads and checks of already supplied keys.
  A key check can retain repeated witnesses without introducing new key values.
  Counts are advisory: never infer uniqueness, emptiness or selectivity."
  [choices row-count]
  (let [mandatory (filter #(or (= :access (:kind %)) (:union-estimate (:value %))) choices)
        uses (reduce (fn [m choice]
                       (let [work (root-work choice)]
                         (if (= 1 (count work))
                           (reduce #(update %1 (:variable %2) (fnil into #{}) work)
                                   m (:columns choice)) m))) {} mandatory)
        joins (into #{} (keep (fn [[v work]] (when (< 1 (count work)) v))) uses)
        qualify? (fn [{:keys [kind value requires columns]}]
                   (and (= :access kind) (seq requires)
                        (every? #(contains? uses (:variable %)) columns)
                        (set/subset? (set/intersection joins (set (map :variable columns)))
                                     (set (map :variable requires)))
                        (let [rows (row-count value)]
                          (and (number? rows) (<= 0 rows 8)
                               (or (not (pos? (long (compare rows 1))))
                                   ;; Preserve bag witnesses even though this
                                   ;; read can only constrain existing keys.
                                   (set/subset? (set (map :variable columns))
                                                (set (map :variable requires))))))))
        leaves (filter qualify? mandatory)
        components
        (into #{}
          (keep (fn [[_ work]]
                  (when (<= 3 (count work) 8)
                    (let [variables (into #{} (mapcat #(map :variable (:columns %)))
                                          (filter #(seq (set/intersection work (root-work %))) mandatory))
                          attached (filter #(some (comp variables :variable) (:requires %)) leaves)
                          component (into work (mapcat root-work) attached)]
                      (when (and (seq attached) (<= (count component) 8)) component))))) uses)]
    {:qualify? qualify? :components components}))

(defn- qualification-prefix
  "Price completion of a small mandatory neighborhood. This is an advisory
  alternative driver only when full-plan estimates exceed tuple capacity."
  [selected inputs row-count model components]
  (loop [remaining (seq selected) done #{} prefix []]
    (when-let [choice (first remaining)]
      (when-not (get-in choice [:value :cost-barrier?])
        (let [done (into done (root-work choice)) prefix (conj prefix choice)]
          (if-let [component (some #(when (set/subset? % done) %) components)]
            (when-let [cost (price prefix inputs row-count model)]
              (assoc (select-keys cost [:estimated-ns :work-rows :peak-rows]) :work component))
            (recur (next remaining) done prefix)))))))

(defn select
  "Keep the original feasible plan, then compare complete alternatives with a
  different first access. Account admitted search states separately from
  cancellation checks, and share the remaining allowance across starting work.
  All attempts share 4096 states and a 20 ms optional CPU budget. Small plans
  use at most 128 additional states and 1 ms. Unknown estimates retain the
  original plan."
  ([arguments row-count] (select arguments row-count nil))
  ([arguments row-count model]
  (let [checks (volatile! 0) check! (:check! arguments)
        visit! #(vreset! checks (inc (long @checks)))
        baseline (dependency/schedule (assoc arguments :visit! visit!))
        initial (when (= :complete (:status baseline)) (price (:choices baseline) (:inputs arguments) row-count model))
        started (System/nanoTime) maximum (long (:max-states arguments))]
    (if-not initial baseline
      (let [choices (:choices arguments)
            qualification (qualification-model choices row-count)
            qualification? (:qualify? qualification)
            qualifications? (boolean (seq (:components qualification)))
            seed (into {} (map (juxt :variable identity)) (:inputs arguments))
            roots (->> choices
                       (filter #(and (or (= :access (:kind %)) (:union-estimate (:value %)))
                                     (every? (fn [c] (dependency/supplies? (seed (:variable c)) c)) (:requires %))))
                       (sort-by #(if (:filtered? %) 0 1))
                       ;; Probe distinct starting work, not repeated scan/lookup
                       ;; variants for the same patterns. Keys contain only core IDs.
                       (reduce (fn [{:keys [seen roots] :as state} choice]
                                 (let [source-work (root-work choice)]
                                   (if (seen source-work) state
                                     {:seen (conj seen source-work) :roots (conj roots choice)})))
                               {:seen #{} :roots []})
                       :roots (take 16))
            ;; The baseline already evaluated this starting work. Repeating
            ;; its scan/lookup variants cannot discover a different driver.
            first-access (first (filter #(or (= :access (:kind %)) (:union-estimate (:value %))) (:choices baseline)))
            work-key root-work
            roots (if qualifications? roots
                      (remove #(= (work-key first-access) (work-key %)) roots))
            ;; Several conditional unions have mutually dependent interfaces.
            ;; A short clause list can still need the regular lookahead bound.
            small? (and (<= (count (:required-work arguments)) 8) (<= (count choices) 32)
                        (<= (count (filter :union-estimate (map :value (:choices baseline)))) 2))
            budget-ns (if small? 1000000 20000000)
            maximum (if small? (min maximum (+ (long @checks) 128)) maximum)
            early (vec (take-while #(and (= :core (:kind %)) (not (:union-estimate (:value %)))) choices))
            timed? (some? (:estimated-ns initial))
            row-key (if timed? (juxt :estimated-ns :work-rows :peak-rows) (juxt :work-rows :peak-rows))
            row-limit (:intermediate-row-limit arguments)
            risky? #(and row-limit (> (double (:peak-rows %)) (double row-limit)))
            prefix #(qualification-prefix (:choices %) (:inputs arguments) row-count model (:components qualification))
            initial-prefix (when (risky? initial) (prefix baseline))
            _ (when-not (pos? maximum) (throw (IllegalArgumentException. "invalid search bound")))]
        (loop [pending (seq roots) best baseline best-price initial best-prefix initial-prefix evaluated 1 starts []]
          (if (or (nil? pending) (>= (long @checks) maximum) (>= (- (System/nanoTime) started) budget-ns))
            (assoc best :cost-changed? (not (and (= (count (:choices best)) (count (:choices baseline)))
                                                       (every? true? (map identical? (:choices best) (:choices baseline)))))
                        :join-cost (select-keys best-price [:work-rows :peak-rows :access-ns :local-ns :estimated-ns
                                                           :estimated-opens :estimated-requests :cost-scope])
                        :cost-search {:model (if timed? :elapsed :cardinality)
                                      :evaluated evaluated :states @checks :starts starts
                                      :budget-ns budget-ns :max-states maximum
                                      :qualification-prefix best-prefix
                                      :elapsed-ns (- (System/nanoTime) started)})
            (let [root (first pending)
                  allowance (max 1 (quot (- maximum (long @checks)) (count pending)))
                  remaining (remove #(identical? root %) (drop (count early) choices))
                  ;; Qualify an already bound entity before following another
                  ;; expanding edge. The typed scheduler still supplies every
                  ;; input and preserves all work, fences and resource rules.
                  ordered (into early
                            (cons root
                              (concat (filter qualification? remaining)
                                      (filter #(and (seq (:requires %)) (not (qualification? %))) remaining)
                                      (remove #(seq (:requires %)) remaining))))
                  attempt (try
                            (dependency/schedule
                              (assoc arguments :choices ordered :max-states allowance :visit! visit!
                                     :check! #(do (check!)
                                                  (when (>= (- (System/nanoTime) started) budget-ns)
                                                    (throw (ex-info "Cost search budget" {::budget true}))))))
                            (catch clojure.lang.ExceptionInfo e (when-not (::budget (ex-data e)) (throw e))))
                  priced (when (= :complete (:status attempt)) (price (:choices attempt) (:inputs arguments) row-count model))
                  candidate-prefix (when (and priced (risky? priced)) (prefix attempt))
                  prefix-comparison (when (and priced (risky? best-price) (risky? priced)
                                               candidate-prefix best-prefix
                                               (or (not timed?) (and (:estimated-ns candidate-prefix) (:estimated-ns best-prefix))))
                                      (compare (row-key candidate-prefix) (row-key best-prefix)))
                  better? (and priced (or (not timed?) (some? (:estimated-ns priced)))
                               (cond
                                 (and (risky? best-price) (not (risky? priced))) true
                                 (and (not (risky? best-price)) (risky? priced)) false
                                 (and prefix-comparison (not (zero? (long prefix-comparison)))) (neg? (long prefix-comparison))
                                 :else (neg? (compare (row-key priced) (row-key best-price)))))]
              (recur (next pending) (if better? attempt best) (if better? priced best-price)
                     (if better? candidate-prefix best-prefix) (inc evaluated)
                     (conj starts (merge {:covers (:covers root) :status (:status attempt)}
                                         (select-keys priced [:work-rows :peak-rows :access-ns :local-ns :estimated-ns
                                                             :estimated-opens :estimated-requests]))))))))))))
