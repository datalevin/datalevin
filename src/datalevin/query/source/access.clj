(ns ^:no-doc datalevin.query.source.access
  "Structural EAV access plans for the shared access abstraction. This bridge
  consumes captured, validated envelopes; it does not negotiate or acquire a
  source, call a provider, sample rows, or install a query execution path."
  (:require [clojure.set :as set]
            [datalevin.parser]
            [datalevin.query.access :as access]
            [datalevin.query.access.dependency :as dependency]
            [datalevin.query.plan :as plan]
            [datalevin.query.source.contract :as contract]
            [datalevin.query.source.control :as control]
            [datalevin.query.source.permissions :as permissions]
            [datalevin.query.source.runtime :as rt]
            [datalevin.query.source.value :as value :refer [fail!]]))

(def ^:private kind-rank {:entity-lookup 0 :value-lookup 1 :attribute-enumeration 2})
(def ^:private search-bound 4096)

(defn- ensure! [ok]
  (when-not ok (fail! :query-source/invalid :planning)))

(defn- nonnegative-id? [x]
  (and (integer? x) (<= 0 x Long/MAX_VALUE)))

(defn- variable? [x]
  (and (symbol? x) (.startsWith (name x) "?")))

(defn- source-symbol? [x]
  (and (symbol? x) (.startsWith (name x) "$")))

(defn- alternative-id? [x]
  (or (keyword? x) (and (string? x) (seq x))))

(defn- columns! [columns check!]
  (ensure! (vector? columns))
  (when (seq columns) (contract/binding-columns! columns check!))
  columns)

(defn- column-map [columns]
  (into {} (map (juxt :variable identity)) columns))

(def ^:private supplies? dependency/supplies?)

(defn- text-compare
  "Compare Unicode code points, including supplementary characters. Java's
  UTF-16 string ordering is different for some valid identifiers."
  [^String a ^String b check!]
  (loop [i 0 j 0 n 0]
    (when (zero? (bit-and n 255)) (check!))
    (cond
      (= i (.length a)) (if (= j (.length b)) 0 -1)
      (= j (.length b)) 1
      :else (let [x (.codePointAt a i) y (.codePointAt b j)]
              (if (= x y)
                (recur (+ i (Character/charCount x)) (+ j (Character/charCount y)) (inc n))
                (compare x y))))))

(defn- key-tag [x]
  (cond (nil? x) 0 (integer? x) 1 (symbol? x) 2 (keyword? x) 3
        (string? x) 4 (vector? x) 5
        :else (fail! :query-source/invalid :planning)))

(defn compare-keys
  "Compare admitted structural identifiers in canonical Unicode/numeric order."
  ^long [a b check!]
  (let [tag (key-tag a) other (key-tag b)]
    (if (not= tag other) (compare tag other)
        (case tag
          0 0
          1 (compare a b)
          2 (text-compare (str a) (str b) check!)
          3 (let [n (compare-keys (namespace a) (namespace b) check!)]
              (if (zero? n) (text-compare (name a) (name b) check!) n))
          4 (text-compare a b check!)
          5 (loop [i 0]
              (check!)
              (cond (= i (count a)) (if (= i (count b)) 0 -1)
                    (= i (count b)) 1
                    :else (let [c (compare-keys (nth a i) (nth b i) check!)]
                            (if (zero? c) (recur (inc i)) c))))))))

(defn- canonical-key [options]
  [(:source-symbol options) (:region-id options)
   (get-in options [:access :anchor-clause])
   (vec (sort (:covers options))) (get-in options [:access :kind])
   (mapv :variable (:requires options)) (:alternative-id options)])

(defn- request-metadata [request]
  (select-keys request [:source-symbol :region-id :scope-id :clauses :bindings
                        :available-inputs :required-vars :demand :max-alternatives]))

(defn- request! [request check!]
  (let [{:keys [source-symbol region-id scope-id clauses bindings available-inputs
                required-vars max-alternatives demand]} request]
    (ensure! (and (source-symbol? source-symbol) (nonnegative-id? region-id)
                  (string? scope-id) (seq scope-id) (vector? clauses)
                  (value/plain-map? bindings) (every? variable? (keys bindings))
                  (set? required-vars) (every? variable? required-vars)
                  (integer? max-alternatives) (<= 1 max-alternatives Long/MAX_VALUE)
                  (= {:quality :exact :ordering [] :offset 0 :limit nil} demand)))
    (columns! available-inputs check!)
    (ensure! (= (count clauses) (count (set (map :id clauses)))))
    (doseq [clause clauses]
      (check!)
      (ensure! (and (value/plain-map? clause) (nonnegative-id? (:id clause))
                    (#{:pattern :predicate} (:kind clause))))
      (when (= :pattern (:kind clause))
        (ensure! (and (= 3 (count (:terms clause)))
                      (= :const (first (second (:terms clause))))
                      (keyword? (second (second (:terms clause))))))))
    request))

(defn candidate!
  "Bridge one validated alternative into an AccessPlan. Request values and the
  opaque handle must already be captured/owned under the invocation's quotas.
  Validate structural boundaries again here before forming dependency edges.
  Advisory estimates are kept separate from native cardinalities and costs."
  [runtime request alternative consumption implementation]
  (let [ctl (:control runtime) check! #(control/check-active! ctl)
        metadata (rt/charge-metadata! runtime (dissoc alternative :plan) :planning 2048)]
    (try
      (request! request check!)
      (let [{:keys [id covers prefilters requires columns properties scope-id completion]} alternative
            {:keys [source-symbol region-id clauses bindings required-vars available-inputs]} request
            by-id (into {} (map (juxt :id identity)) clauses)
            patterns (into #{} (comp (filter #(= :pattern (:kind %))) (map :id)) clauses)
            _ (ensure! (and (set? covers) (set? prefilters)))
            covered-patterns (set/intersection patterns covers)
            access (:access alternative)
            anchor (:anchor-clause access)
            anchor-clause (get by-id anchor)
            required (column-map (columns! requires check!))
            output (column-map (columns! columns check!))
            available (column-map available-inputs)
            provided (into (into (set (keys bindings)) (keys required)) (keys output))
            covered-vars (into #{} (mapcat #(contract/variables (get by-id %))) covers)]
        (ensure! (and (alternative-id? id) (some? (:plan alternative))
                      (= (:scope-id request) scope-id) (= :all-bindings completion)
                      (set? covers) (seq covered-patterns) (set? prefilters)
                      (set/subset? covers (set (keys by-id)))
                      (set/subset? prefilters (set (keys by-id)))
                      (empty? (set/intersection covers prefilters))
                      (every? #(= :predicate (:kind (by-id %))) prefilters)
                      (value/plain-map? access) (every? #(contains? access %) [:kind :anchor-clause])
                      (every? #(or (#{:kind :anchor-clause} %) (qualified-keyword? %)) (keys access))
                      (contains? kind-rank (:kind access)) (contains? covered-patterns anchor)
                      (#{:identical :unstable} (:replay properties))
                      (= [] (:ordering properties)) (= :none (:blocking-state properties))
                      (satisfies? access/IAccessMethod implementation)))
        (doseq [[variable column] required]
          (check!)
          (ensure! (supplies? (get available variable) column)))
        (ensure! (set/subset? (set/intersection required-vars covered-vars) provided))
        (doseq [id (set/union covers prefilters)
                :let [clause (by-id id)] :when (= :predicate (:kind clause))]
          (ensure! (set/subset? (contract/variables clause) provided)))
        (let [position (case (:kind access) :entity-lookup 0 :value-lookup 2 nil)]
          (when position
            (let [[tag x] (nth (:terms anchor-clause) position)]
              (ensure! (or (= :const tag)
                            (and (= :var tag) (or (contains? bindings x) (contains? required x))))))))
        ;; These are structural identifiers/columns only. Do not retain provider
        ;; annotations, labels, handles, estimates, or request values in a key.
        (let [options {:source-symbol source-symbol :region-id region-id :alternative-id id
                       :access (select-keys access [:kind :anchor-clause]) :covers covered-patterns
                       :requires (mapv #(select-keys % [:variable :type :nullable?]) requires)
                       :columns (mapv #(select-keys % [:variable :type :nullable?]) columns)
                       :scope-id scope-id}
              token (rt/charge-metadata! runtime options :planning
                                         (+ 2048 (* 64 (+ (count requires) (count covered-patterns)))))]
          (try
            (let [options (value/diagnostic-copy options check!)
                  options (assoc options :canonical-key (canonical-key options)
                                         :permission (permissions/planning-permission consumption alternative)
                                         :consumption consumption
                                         :capture {:request request :alternative alternative})
                  required-vars (mapv :variable (:requires options))
                  output-vars (mapv :variable (:columns options))
                  expr (access/->AccessExpr :query-source
                         (into #{} (map #(vector (:source-symbol options) region-id %)) covered-patterns)
                         (set required-vars) (set/difference (set output-vars) (set required-vars))
                         (set/intersection (set output-vars) (set required-vars)) output-vars (:source-symbol options))
                  path (access/->AccessPath :query-source implementation (:kind access) [] #{:complete}
                                           options (access/access-policy :none :execution))
                  plan (assoc (access/->AccessPlan expr path (access/source-bounds)
                                                  (access/access-work) nil)
                              :advisory-estimate (:estimate alternative)
                              ::reservation token)]
              (rt/own! runtime #(control/release! token))
              plan)
            (catch Throwable e (control/release! token) (throw e)))))
      (finally (control/release! metadata)))))

(defn- options [plan] (get-in plan [:path :options]))

(defn release-discarded!
  "Release the bridge reservation after whole-query selection has discarded
  a captured candidate. Envelope ownership is released separately."
  [plan]
  (control/release! (::reservation plan)))
(defn- work-keys [plan]
  (:covers (:expr plan)))

(defn- first-work-key [plan]
  (let [{:keys [source-symbol region-id covers]} (options plan)]
    [source-symbol region-id (reduce min covers)]))

(defrecord ^:private CoreOperator [owner region-id id requires columns after step])

(defn core-operator!
  "Capture the typed interface of a shared step or tuple-layout factory. Only
  its explicit input/output variables enter the dependency graph; hidden
  native, rule or union body bindings stay private. after contains local work
  IDs that must finish before this step (for example a residual's boundary).
  This function neither samples nor executes the supplied step."
  [runtime region-id id requires columns after step]
  (let [ctl (:control runtime) check! #(control/check-active! ctl)
        token (rt/charge-metadata! runtime [region-id id requires columns after] :planning)]
    (try
      (ensure! (and (nonnegative-id? region-id) (nonnegative-id? id)
                    (set? after) (or (satisfies? plan/IStep step)
                                    (satisfies? plan/IStepFactory step))))
      (columns! requires check!)
      (columns! columns check!)
      (ensure! (and (= (set (map :variable requires)) (:in step))
                    (= (mapv :variable columns) (:cols step))))
      (let [clean #(mapv (fn [column] (select-keys column [:variable :type :nullable?])) %)
            captured (value/diagnostic-copy [(clean requires) (clean columns) after] check!)
            operator (->CoreOperator {:control ctl :query-id (:query-id runtime)} region-id id
                                    (nth captured 0) (nth captured 1) (nth captured 2) step)]
        (rt/own! runtime #(control/release! token))
        operator)
      (catch Throwable e (control/release! token) (throw e)))))

(defn- core-operator? [runtime region-id operator]
  (and (instance? CoreOperator operator) (= region-id (:region-id operator))
       (identical? (:control runtime) (get-in operator [:owner :control]))
       (= (:query-id runtime) (get-in operator [:owner :query-id]))))

(defn available-inputs!
  "Derive potential request exports from captured local pattern schemas and
  existing shared operators. These are possible supplies, never actual seeds.
  The dependency scheduler must still prove their ordering. Only variables
  visible in this region can enter a provider request."
  [runtime region input-columns operators]
  (let [ctl (:control runtime) check! #(control/check-active! ctl)
        owner (:owner region)
        token (rt/charge-metadata! runtime input-columns :planning)
        workspace (volatile! nil)]
    (try
      (ensure! (and (identical? ctl (:control owner)) (= (:query-id runtime) (:query-id owner))
                    (not @(:closed? owner)) (vector? operators)
                    (every? #(core-operator? runtime (:region-id region) %) operators)))
      (columns! input-columns check!)
      (ensure! (set/subset? (set (map :variable input-columns)) (:input-vars region)))
      (vreset! workspace
        (control/reserve! ctl
          {:retained-bytes
           (value/add-charge 1024
             (value/multiply-charge 512
               (reduce (fn [^long n item] (+ n (count (:columns item))))
                       (+ (count input-columns) (count operators))
                       (concat (:patterns region) operators))))}))
      ;; Mapped facts are optimistic potential producers in this necessary
      ;; preflight. They do not become execution seeds. Even with all of those
      ;; variables, an unseeded local dependency cannot be repaired by a provider
      ;; response, so reject it before acquiring any provider session. This is
      ;; only a variable-availability proof: private outputs need not share a
      ;; type or be exported. The typed scheduler checks actual interfaces.
      (loop [known (into (set (map :variable input-columns))
                        (mapcat #(map :variable (:columns %))) (:patterns region))
             pending operators]
        (check!)
        (when (seq pending)
          (if-let [operator (some (fn [operator]
                                    (check!)
                                    (when (every? #(contains? known (:variable %)) (:requires operator)) operator)) pending)]
            (recur (into known (map :variable (:columns operator)))
                   (filterv #(not= (:id operator) (:id %)) pending))
            (fail! :query-source/unsupported :planning))))
      (let [possible (concat input-columns (mapcat :columns (:patterns region)) (mapcat :columns operators))
            known (reduce (fn [known column]
                            (check!)
                            (if (contains? (:available-vars region) (:variable column))
                              (or (dependency/extend-columns known
                                    [(select-keys column [:variable :type :nullable?])])
                                  (fail! :query-source/unsupported :planning))
                              known)) {} possible)
            columns (->> (vals known)
                         (sort #(compare-keys (:variable %1) (:variable %2) check!)) vec)
            output-token (rt/charge-copy! runtime columns)]
        (try
          (let [output (value/diagnostic-copy columns check!)]
            (rt/own! runtime #(control/release! output-token))
            output)
          (catch Throwable e (control/release! output-token) (throw e))))
      (finally (control/release! token)
               (when-let [token @workspace] (control/release! token))))))

(defn schedule!
  "Validate complete region-local coverage and schedule typed AccessPlans.
  Inputs are actual seeds from the enclosing engine operator. Optional captured
  core operators contribute typed outputs only after their dependencies run.
  Never infer an export from another region or inspect outer rows. Regions with
  fused candidates use :eav-fusion-v1: lookup priority, then wider coverage for
  the same first work item, then canonical identity. Single-pattern regions
  retain :eav-structural-v1. Feasibility
  search shares the invocation ledger and the existing 4096-state bound.
  Caller-owned candidate/request reservations must live through execution."
  ([runtime region-id input-columns requests plans]
   (schedule! runtime region-id input-columns requests plans []))
  ([runtime region-id input-columns requests plans operators]
   (let [started (System/nanoTime)
         ctl (:control runtime) check! #(control/check-active! ctl)
         charges (volatile! []) output-token (volatile! nil)
         own-work! (fn [bytes]
                     (let [token (control/reserve! ctl {:retained-bytes bytes})]
                       (vswap! charges conj token)))]
     (try
       (ensure! (and (nonnegative-id? region-id) (vector? requests) (vector? plans)
                     (vector? operators) (every? #(core-operator? runtime region-id %) operators)
                     (= (count operators) (count (set (map :id operators))))))
       (vswap! charges conj (rt/charge-metadata! runtime input-columns :planning))
       (columns! input-columns check!)
       (own-work! (+ 2048 (* 1024 (count plans)) (* 512 (count requests)) (* 1024 (count operators))))
       (doseq [request requests]
         (vswap! charges conj (rt/charge-copy! runtime (request-metadata request)))
         (request! request check!)
         (ensure! (= region-id (:region-id request))))
       (let [request-map (into {} (map (juxt :source-symbol identity)) requests)
             _ (ensure! (= (count requests) (count request-map)))
             required (into #{} (mapcat (fn [request]
                                         (for [clause (:clauses request) :when (= :pattern (:kind clause))]
                                           [(:source-symbol request) region-id (:id clause)]))) requests)
             live-vars (into #{} (mapcat :required-vars) requests)
             _ (doseq [plan plans]
                 (check!)
                 (let [o (options plan) request (get request-map (:source-symbol o))]
                   (ensure! (and request (= region-id (:region-id o))
                                 (identical? request (get-in o [:capture :request]))
                                 (set/subset? (into #{} (map #(vector (:source-symbol o) region-id %)) (:covers o))
                                              required)))))
             _ (doseq [[source-symbol candidates] (group-by #(get-in % [:expr :source]) plans)]
                 (ensure! (<= (count candidates) (long (:max-alternatives (request-map source-symbol)))))
                 (ensure! (= (count candidates)
                             (count (set (map #(get-in % [:path :options :alternative-id]) candidates))))))
             candidates (->> plans
                             (map (fn [plan]
                                    (let [o (options plan)]
                                      (assoc-in plan [:path :options :permission]
                                                (permissions/planning-permission
                                                  (:consumption o) (get-in o [:capture :alternative]))))))
                             (sort (fn [a b]
                                     (check!)
                                     (let [work (compare-keys (first-work-key a) (first-work-key b) check!)]
                                       (if (zero? work)
                                         (let [kind (compare (kind-rank (get-in a [:path :options :access :kind]))
                                                             (kind-rank (get-in b [:path :options :access :kind])))
                                               width (compare (count (work-keys b)) (count (work-keys a)))]
                                           (cond
                                             (not (zero? kind)) kind
                                             (not (zero? width)) width
                                             :else (compare-keys (:canonical-key (options a))
                                                                 (:canonical-key (options b)) check!))) work))))
                             vec)
             local-work (into #{} (map #(vector :core region-id (:id %))) operators)
             required (set/union required local-work)
             _ (doseq [operator operators]
                 (ensure! (set/subset? (:after operator) required)))
             seeded (set (map :variable input-columns))
             early? (fn [operator]
                      (let [keys (:binding-inputs (:step operator))
                            outputs (set (map :variable (:columns operator)))]
                        (or (empty? keys) (:binding-bound? (:step operator))
                            (seq (set/intersection keys seeded))
                            ;; Keep producers ahead of scans when they can
                            ;; enable a provider's preferred dependent path.
                            (and (not (:binding-enumerate? (:step operator)))
                                 (some (fn [plan]
                                         (some #(contains? outputs (:variable %)) (:requires (options plan))))
                                       candidates)))))
             ;; Optional native keys only affect preference. Propagate the
             ;; preference through native producers, so a reference read can
             ;; bind a later payload read. The typed scheduler still proves
             ;; every actual dependency and never treats these keys as seeds.
             preferred (into seeded (mapcat #(map :variable (:columns %))) (filter early? operators))
             ranks (loop [known (into preferred
                                      (mapcat #(when (empty? (:requires (options %)))
                                                 (map :variable (:columns (options %))))) candidates)
                          pending (vec (remove early? operators)) rank 1 result {}]
                     (check!)
                     (let [ready (filterv #(do (check!)
                                              (seq (set/intersection known (:binding-inputs (:step %))))) pending)
                           ready-ids (into #{} (map :id) ready)]
                       (if (empty? ready) result
                         (recur (into known (mapcat #(map :variable (:columns %))) ready)
                                (filterv #(do (check!) (not (contains? ready-ids (:id %)))) pending)
                                (inc rank) (into result (map #(vector (:id %) rank)) ready)))))
             deferred? #(contains? ranks (:id %))
             core-choice (fn [operator]
                                  {:kind :core :value operator :covers #{[:core region-id (:id operator)]}
                                   :requires (:requires operator) :columns (:columns operator)
                                   :after (:after operator) :resources #{}})
             ordered-core (sort-by :id operators)
             choices (into (mapv core-choice (remove deferred? ordered-core))
                           (concat (map (fn [plan]
                                  (let [o (options plan) p (:permission o)]
                                    {:kind :access :value plan :covers (work-keys plan)
                                     :requires (:requires o) :columns (:columns o) :after #{}
                                     :resources (if (= :once (:opens p)) #{(:resource p)} #{})
                                     :unavailable? (and (= :once (:opens p)) (:spent? p))})) candidates)
                                   (map core-choice (sort-by #(get ranks (:id %)) (filter deferred? ordered-core)))))
             result (dependency/schedule {:inputs input-columns :required-work required
                                          :required-vars live-vars :choices choices
                                          :check! check! :reserve! own-work! :max-states search-bound})]
         (when-not (= :complete (:status result)) (fail! :query-source/unsupported :planning))
         ;; Native/core steps remain private and keep their shared implementation.
         ;; Only selected external AccessPlans enter the provider explanation.
         (let [selected (:choices result)
               plan-count (count (filter #(= :access (:kind %)) selected))]
           (vreset! output-token
             (control/reserve! ctl {:retained-bytes (+ 1024 (* 512 plan-count) (* 1024 (count operators)))}))
           (let [result {:region-id region-id
                         :policy (if (some #(> (count (work-keys %)) 1) candidates)
                                   :eav-fusion-v1 :eav-structural-v1)
                         :selection :structural
                         :estimated-access-ns nil :fallback-reasons []
                         :plans (into [] (comp (filter #(= :access (:kind %))) (map :value)) selected)
                         :operators (mapv #(select-keys % [:kind :value]) selected)
                         :planning-time-ms (/ (double (- (System/nanoTime) started)) 1000000.0)}]
             (let [token @output-token] (rt/own! runtime #(control/release! token)))
             (vreset! output-token nil)
             result)))
       (finally
         (when-let [token @output-token] (control/release! token))
         (doseq [token @charges] (control/release! token)))))))

(defn validate-query!
  "Audit all selected regions together before execution. Region-local planning
  cannot grant a one-shot resource another open in a different region. This
  guard admits no implicit spool, replay, or duplicate region/work occurrence."
  [runtime schedules]
  (let [ctl (:control runtime)]
    (control/check-active! ctl)
    (ensure! (vector? schedules))
    (let [n (reduce (fn [n schedule]
                      (reduce (fn [n plan] (value/add-charge n (count (work-keys plan))))
                              n (:plans schedule))) 0 schedules)
          token (control/reserve! ctl
                  {:retained-bytes
                   (value/add-charge (+ 1024 (* 256 (count schedules)))
                     (value/multiply-charge 512 n))})]
      (try
        (ensure! (= (count schedules) (count (set (map :region-id schedules)))))
        (loop [remaining (seq schedules) used #{} work #{}]
          (when-let [schedule (first remaining)]
            (control/check-active! ctl)
            (ensure! (#{:eav-structural-v1 :eav-fusion-v1} (:policy schedule)))
            (let [[used work]
                  (reduce (fn [[used work] plan]
                            (control/check-active! ctl)
                            (let [o (options plan)
                                  permission (permissions/planning-permission
                                               (:consumption o) (get-in o [:capture :alternative]))
                                  keys (work-keys plan)]
                              (ensure! (and (= (:region-id schedule) (:region-id o))
                                            (seq keys) (empty? (set/intersection work keys))))
                              (when (and (= :once (:opens permission))
                                         (or (:spent? permission) (contains? used (:resource permission))))
                                (fail! :query-source/unsupported :planning))
                              [(cond-> used (= :once (:opens permission)) (conj (:resource permission)))
                               (into work keys)])) [used work] (:plans schedule))]
              (recur (next remaining) used work))))
        nil
        (finally (control/release! token))))))

(defn explain!
  "Copy the structural decision without opaque handles, bindings, annotations,
  session resource keys, or provider labels. Estimates remain advisory."
  [runtime schedule]
  (let [plans (:plans schedule)
        ctl (:control runtime)
        token (control/reserve! ctl {:retained-bytes (+ 1024 (* 1024 (count plans)))})]
    (try
      (let [steps (mapv
                    (fn [plan]
                      (control/check-active! ctl)
                      (let [o (options plan)
                            alternative (get-in o [:capture :alternative])
                            estimate (:advisory-estimate plan)
                            numeric (if (seq (:requires o))
                                      [:startup-ns :per-binding-ns :rows-per-binding :row-bytes
                                       :requests-per-open :requests-per-binding :transfer-bytes-per-binding
                                       :peak-retained-bytes]
                                      [:startup-ns :run-ns :rows :row-bytes :requests-per-open
                                       :transfer-bytes :peak-retained-bytes])]
                        (rt/diagnostic-copy! runtime
                          (cond-> {:source (:source-symbol o) :region-id (:region-id o)
                           :alternative-id (:alternative-id o) :covers (:covers o) :access (:access o)
                           :prefilters (:prefilters alternative)
                           :requires (mapv :variable (:requires o))
                           :columns (mapv :variable (:columns o))
                           :replay (get-in alternative [:properties :replay])
                           :blocking-state (get-in alternative [:properties :blocking-state])
                           :batch-limits (select-keys (control/limits ctl) [:batch-rows :batch-bytes])
                           :constraints (select-keys (:constraints alternative)
                                                     [:max-binding-rows :max-binding-bytes :max-request-binding-rows])
                           :estimate (assoc (select-keys estimate (conj numeric :confidence))
                                            :provenance (select-keys (:provenance estimate)
                                                                     [:kind :id :observed-at-ms :valid-for-ms]))}
                            (contains? (:properties alternative) :consumption-group)
                            (assoc :consumption-group (get-in alternative [:properties :consumption-group]))
                            (seq (:requires o))
                            (assoc :binding-limits
                                   {:rows (min (long (:binding-rows (control/limits ctl)))
                                               (long (get-in alternative [:constraints :max-binding-rows])))
                                    :bytes (min (long (:binding-bytes (control/limits ctl)))
                                                (long (get-in alternative [:constraints :max-binding-bytes])))}))))) plans)
            result (assoc (select-keys schedule [:region-id :policy :selection
                                                 :estimated-access-ns :fallback-reasons :planning-time-ms]) :steps steps)]
        (rt/own! runtime #(control/release! token))
        result)
      (catch Throwable e (control/release! token) (throw e)))))
