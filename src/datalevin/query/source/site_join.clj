(ns ^:no-doc datalevin.query.source.site-join
  "Choose a direction and a placement for a single equality between two sites."
  (:require [clojure.set :as set]
            [datalevin.parser :as parser]
            [datalevin.query.source.contract :as contract]
            [datalevin.query.source.control :as control]
            [datalevin.query.source.diagnostics :as diagnostics]
            [datalevin.query.source.extrema :as extrema]
            [datalevin.query.source.region :as region]
            [datalevin.query.source.relational :as relational]
            [datalevin.query.source.runtime :as rt]
            [datalevin.query.source.session :as session]
            [datalevin.relation :as relation])
  (:import [datalevin.parser Constant FindRel Function Variable]
           [org.eclipse.collections.impl.list.mutable FastList]))

(defn- charge! [runtime size]
  (let [token (control/reserve! (:control runtime) {:retained-bytes size})]
    (rt/own! runtime #(control/release! token))))

(defn- calculation [node]
  (when (instance? Function node)
    (let [op ({'+ :+ '- :- '* :*} (get-in node [:fn :symbol]))
          v (get-in node [:binding :variable :symbol])
          args (mapv (fn [a] (cond (instance? Variable a) [:var (:symbol a)]
                                   (and (instance? Constant a) (int? (:value a))) [:const (:value a)])) (:args node))]
      (when (and op v (= 2 (count args)) (every? some? args))
        {:variable v :operator op :args args}))))

(defn- part [group patterns predicates calcs]
  (let [aliases (set (:aliases group)) ps (filterv #(aliases (:source-symbol %)) patterns)
        types (contract/variable-types (:entries group) ps)
        vars (set (keys types))
        derived (reduce (fn [xs calc]
                          (if (every? (fn [[tag x]] (or (= tag :const) ((into vars (map :variable xs)) x))) (:args calc))
                            (conj xs calc) xs)) [] calcs)]
    (when (and (seq ps) (every? #(= 1 (count %)) (vals types)))
      {:group group :vars (into vars (map :variable derived)) :patterns ps :calculations derived
       :types (into (update-vals types first) (map #(vector (:variable %) :bigdec)) derived)
       :filters (filterv #(set/subset? (contract/variables %) vars) predicates)})))

(defn- connected-patterns? [patterns]
  (let [domains (map contract/variables patterns)]
    (loop [known (first domains) remaining (next domains)]
      (or (empty? remaining)
          (let [linked (filter #(seq (set/intersection known %)) remaining)]
            (and (seq linked)
                 (recur (reduce set/union known linked) (remove (set linked) remaining))))))))

(defn- layout [compiled groups native]
  (let [parsed (:parsed compiled) root (first (:regions compiled))
        ps (mapv #(assoc (:clause %) :source-symbol (:source-symbol %)) (:patterns root))
        nodes (mapv :node (:core-clauses root))
        preds (region/predicate-clauses root (into #{} (mapcat contract/variables) ps))
        calcs (into [] (keep calculation) nodes)
        parts (mapv #(part % ps preds calcs) groups)
        [a b] parts boundary (set/intersection (:vars a) (:vars b)) k (first boundary)
        find (mapv relational/projection (parser/find-elements (:qfind parsed)))
        vars (set (map :variable find)) with (set (map :symbol (:qwith parsed)))
        aggregates (filter #(= :aggregate (:kind %)) find) agg (first aggregates)
        sum? (= :sum (:operator agg))
        grouped (into #{} (comp (filter #(= :variable (:kind %))) (map :variable)) find)
        owner (some #(when (contains? (:vars %) (:variable agg)) %) parts)
        native? (seq native)
        key-type (get-in a [:types k])
        reference? (and (= :custom (:kind key-type)) (= [:long] (get-in key-type [:parameters :key-types]))
                        (every? #(true? (get-in % [:descriptor :datalevin.query.source/integer-reference-key?]))
                                (mapcat :entries groups)))
        native-owner (when native?
                       (some (fn [p] (when (and (set/subset? vars (:vars p))
                                                (every? (set (get-in p [:group :aliases])) (map :source (vals native)))) p)) parts))
        all (set/union (:vars a) (:vars b))]
    (when (and (every? some? parts) (= 1 (count boundary))
               (or (#{:long :string} key-type) (and native? reference?)) (= ((:types a) k) ((:types b) k))
               (= (+ (count preds) (count calcs)) (count nodes))
               (= (set preds) (into #{} (mapcat :filters) parts))
               (= (count calcs) (count (set (map :variable calcs))))
               (empty? (set/intersection (set (map :variable calcs)) (into #{} (mapcat contract/variables) ps)))
               (= (set calcs) (into #{} (mapcat :calculations) parts))
               (every? some? find) (set/subset? (into vars with) all)
               (if native?
                 (and native-owner (= vars (set (keys native))) (empty? with)
                      (every? #(connected-patterns? (:patterns %)) parts)
                      (= (count find) (count aggregates)) (every? #(#{:min :max} (:operator %)) find)
                      (not (vars k)) (empty? calcs)
                      (every? #(not ((contract/variables %) k)) preds)
                      (some #(and (not= % native-owner) (< 1 (count (get-in % [:group :entries])))
                                  (seq (:filters %))) parts))
                 (or (empty? aggregates)
                   (and (= 1 (count aggregates))
                        (or (= :count-distinct (:operator agg))
                            (and sum? (not (grouped (:variable agg))) (grouped k) (not= k (:variable agg))
                                 (= :bigdec (get-in owner [:types (:variable agg)]))
                                 (set/subset? with (:vars owner))
                                 (empty? (set/intersection grouped (disj (:vars owner) k))))))))
               (or sum? (empty? calcs)))
      (let [wanted (conj (if sum? vars (set/union vars with)) k)]
        {:key k :type (if reference? :long key-type) :sum? sum? :native? (boolean native?)
         :driver-index (when native? (if (= a native-owner) 1 0))
         :next-id (inc (long (reduce max -1 (map :id (concat ps preds)))))
         :parts (mapv (fn [p]
                        (let [exports (vec (sort (set/intersection wanted (:vars p))))
                              reduction? (and sum? (= p owner))
                              elements (mapv (fn [v] (if (and reduction? (= v (:variable agg))) agg
                                                       {:variable v :kind :variable})) exports)]
                          (assoc p :reference-key (when reference? k) :exports exports :find elements :with (if reduction? (vec (sort with)) [])
                                 :existence? (and (= [k] exports) (not reduction?))))) parts)}))))

(defn- plan! [runtime p k id domain]
  (let [group (:group p) entries (:entries group)
        columns (mapv (fn [{:keys [kind variable]}]
                        {:variable variable :type (cond (= (:reference-key p) variable) :long (= :aggregate kind) :data :else ((:types p) variable))
                         :nullable? false}) (:find p))
        parsed (parser/map->Query {:qfind (parser/->FindRel (mapv parser/->Variable (:exports p)))})
        pred {:id id :kind :predicate :operator :in :semantics-version 1 :args [[:var k] [:const domain]]}
        request (merge (:request group)
                  {:mode :relational :semantics-version 1 :bindings {} :find (:find p) :with (:with p)
                   :columns columns :sources (mapv #(select-keys % [:symbol :descriptor]) entries)
                   :clauses (cond-> (into (:patterns p) (:filters p)) domain (conj pred))}
                  (when domain {:datalevin.query.source/fanout-key k})
                  (when (:reference-key p) {:datalevin.query.source/integer-reference-key k})
                  (when (seq (:calculations p)) {:datalevin.query.source/decimal-expressions (:calculations p)})
                  (when (and domain (:existence? p)) {:datalevin.query.source/existence-key k}))]
    (relational/local-plan! runtime group (:source (first entries)) request parsed)))

(defn transfer-cost
  "Price a placement using cardinality or projected serial exchanges, returned
  bytes, uploaded bytes and local work. One open has three fixed exchanges."
  [estimate rows opens input-bytes planning]
  (if (= :cardinality (:cost-model planning)) rows
    (let [rows (double rows)
          rtt (double (get planning :link-rtt-ns (:request-ns estimate)))
          pages (Math/ceil (/ rows (double (:fetch-rows estimate))))
          bytes (+ (double input-bytes) (* rows (double (:row-bytes estimate))))]
      (+ (* rtt (+ (* 3.0 (double opens)) pages))
         (* (double (get planning :transfer-ns-per-byte 1)) bytes)
         (* rows (double (get planning :local-row-ns 1000)))))))

(defn- batch-cap ^long [p type]
  (if (= type :string) 256
    (long (reduce min 8192 (map #(get-in % [:descriptor :datalevin.query.source/integer-domain-limit])
                               (get-in p [:group :entries]))))))

(defn- probe-cost [base sample key-count sample-count cap bytes planning]
  (when (and sample (pos? (compare key-count 0)))
    (let [opens (Math/ceil (/ (double key-count) (double cap)))
          rows (* (double (:rows sample)) (/ (double key-count) (double sample-count)))]
      (transfer-cost (assoc base :row-bytes (:row-bytes sample)) (min rows (double (:rows base))) opens bytes planning))))

(defn- driver-index [parts type plain-hints bound-hints bound planning]
  (let [score (fn [i]
                (let [j (- 1 (long i)) driver (plain-hints i) target (plain-hints j)
                      keys (:rows driver) cap (batch-cap (parts j) type)
                      bulk (double (transfer-cost target (:rows target) 1 0 planning))
                      key-bytes (if (= type :long) 8.0 (double (:row-bytes driver)))
                      probe (probe-cost target (bound-hints j) keys 1 cap (* (double keys) key-bytes) planning)]
                  (+ (double (transfer-cost driver keys 1 0 planning))
                     (double (if probe (min bulk (double probe)) bulk)))))]
    (cond
      (every? some? plain-hints)
      (first (sort-by (fn [i] [(score i) (:rows (plain-hints i)) (:existence? (parts i)) i]) [0 1]))
      ;; With incomplete estimates, place the keyed existence check or
      ;; per-key subtotal on the receiving side if it admits restrictions.
      (and (bound 1) (or (:existence? (parts 1)) (some #(= :aggregate (:kind %)) (:find (parts 1))))) 0
      (and (bound 0) (or (:existence? (parts 0)) (some #(= :aggregate (:kind %)) (:find (parts 0))))) 1
      (nil? (bound 1)) 1
      :else 0)))

(defn- records! [runtime group plan accept]
  (relational/rows! runtime (assoc plan :group group :retain-row? accept)))

(defn- tuple-relation [runtime cols rows]
  (charge! runtime (+ 256 (* (count rows) (+ 64 (* 8 (count cols))))))
  (let [tuples (FastList. (count rows))]
    (doseq [r rows]
      (control/check-active! (:control runtime))
      (.add tuples (object-array r)))
    (relation/relation! (zipmap cols (range)) tuples)))

(defn- restricted-rows! [runtime target key id chunks first-plan read!]
  (into [] (mapcat (fn [chunk initial?]
                     (control/check-active! (:control runtime))
                     (let [plan (if initial? first-plan (plan! runtime target key id (vec chunk)))]
                       (if plan (read! plan nil)
                         ;; A provider can decline a subsequent membership predicate.
                         (let [domain (set chunk)
                               pos (.indexOf ^java.util.List (:exports target) key)]
                           (read! (:plain target) #(contains? domain (nth % pos)))))))
                   chunks (cons true (repeat false)))))

(defn fanout-range
  "Scale per-key counts (including zeroes) with a finite-population standard
  error. The interval is an uncertainty indicator, not an execution bound."
  [population sample-size total squares]
  (let [population (double population)
        n (double sample-size) mean (/ (double total) n)
        variance (double (if (> n 1) (/ (max 0.0 (- (double squares) (* n mean mean))) (dec n)) 0.0))
        error (* 2.0 (Math/sqrt (* (/ variance n) (max 0.0 (- 1.0 (/ n population))))))]
    {:rows (* population mean) :low (* population (max 0.0 (- mean error)))
     :high (* population (+ mean error))}))

(defn receiver-route
  "Compare source-local planner costs and transfer nanoseconds. Without a
  calibration, conflicting rankings leave the existing route in place."
  [bound-link bulk-link bound-work bulk-work calibration]
  (let [bound-link (double bound-link) bulk-link (double bulk-link)
        bound-work (double bound-work) bulk-work (double bulk-work)]
    (cond
      calibration (let [scale (double calibration)]
                    (if (< (+ bound-link (* scale bound-work)) (+ bulk-link (* scale bulk-work)))
                      :bound :independent))
      (and (< bound-link bulk-link) (<= bound-work bulk-work)) :bound
      (and (>= bound-link bulk-link) (>= bound-work bulk-work)) :independent)))

(defn- receiver-choice [target domain first-batch keys small sample estimate planning]
  (let [{:keys [hint session]} target
        n (count domain) width (count first-batch) measured (count keys) opens (Math/ceil (/ (double n) width))
        bytes (reduce (fn [total k] (+ (long total) (long (if (string? k) (+ 8 (* 3 (count k))) 8)))) 0 domain)
        bulk-link (transfer-cost hint (:rows hint) 1 0 planning)
        bound-link (probe-cost hint (or sample (:per-key target)) n (if sample measured 1) width bytes planning)
        previous (if (or (nil? bound-link) (neg? (compare bound-link bulk-link))) :bound :independent)
        compared? (and (not= :cardinality (:cost-model planning)) (:source-cost sample) (:source-cost hint))
        startup (double (get sample :startup-cost 0.0))
        fixed (* opens startup)
        bound-work (when compared? (+ fixed (* (/ (double n) measured) (- (double (:source-cost sample)) startup))))
        bulk-work (:source-cost hint)
        calibration (:source-ns-per-cost-unit planning)
        choice (when compared? (receiver-route bound-link bulk-link bound-work bulk-work calibration))
        ;; Ask the receiver only when its two cost components disagree, or a
        ;; reduction hides intermediate rows. Charge the probe to this decision.
        refine? (and compared?
                     (or (nil? choice) (pos? (compare (:join-rows sample) (:rows sample)))
                         (and (> n width) (next (get-in target [:group :entries])))))
        stages (when (and small refine?) (estimate session small true))
        fanout (when (seq stages)
                 (mapv (fn [[stage total squares label]]
                         (merge (fanout-range n (count keys) total squares) label {:stage stage})) (sort-by first stages)))
        ;; Use sampled witnesses to reprice the variable component. Fixed
        ;; source work and all network opens are paid even for a zero-match key.
        projected (first fanout)
        expected (when compared? (* (/ (double n) measured) (double (:join-rows sample))))
        costs (when (and projected (pos? (double expected)))
                (let [variable (max 0.0 (- (double bound-work) fixed))
                      ;; A precise final join count can mask highly skewed
                      ;; first lookups. Widen the decision interval by the
                      ;; relative sampling error of each direct lookup stage.
                      direct (filter #(pos? (double (:rows %))) (rest fanout))
                      low (* (/ (double (:low projected)) (double expected))
                             (double (reduce min 1.0 (map #(/ (double (:low %)) (double (:rows %))) direct))))
                      high (* (/ (double (:high projected)) (double expected))
                              (double (reduce max 1.0 (map #(/ (double (:high %)) (double (:rows %))) direct))))]
                  [(+ fixed (* variable low)) (+ fixed (* variable high))]))
        choices (when costs (mapv #(receiver-route bound-link bulk-link % bulk-work calibration) costs))
        decided (when-not (and projected (< measured n) (zero? (double (:rows projected))))
                  (if (seq choices) (when (apply = choices) (first choices)) choice))]
    (cond-> {:mode (or decided (if (:ordinary-fallback? target) :ordinary previous))
             :independent-cost bulk-link :bound-cost bound-link
             :transfer-choice previous}
      compared? (assoc :independent-source-cost bulk-work :bound-source-cost bound-work)
      (seq fanout) (assoc :sample-keys (count keys) :fanout fanout :bound-source-cost-range costs))))

(defn- target-rows! [runtime target key type id domain planning estimate]
  (let [{:keys [plain hint session]} target
        read! #(records! runtime session %1 %2)
        cap (batch-cap target type)
        ;; A deterministic hash order spreads the first physical batch and its
        ;; optional probe over the actual domain. Every key still runs once.
        chunks (partition-all cap (sort-by (juxt hash identity) domain))
        first-batch (some-> (first chunks) vec)
        probe (when (and first-batch (:bound target)) (plan! runtime target key id first-batch))
        keys (when (and probe hint) (vec (take 256 first-batch)))
        small (when keys (if (= (count first-batch) (count keys)) probe
                           (plan! runtime target key id keys)))
        sample (when small (estimate session small))
        decision (cond (empty? domain) {:mode :empty}
                       (nil? probe) {:mode :independent}
                       (nil? hint) {:mode (if (:ordinary-fallback? target) :ordinary :bound)}
                       :else (receiver-choice target domain first-batch keys small sample estimate planning))
        pos (.indexOf ^java.util.List (:exports target) key)]
    (assoc decision :rows (case (:mode decision)
                            :ordinary nil
                            :empty []
                            :bound (restricted-rows! runtime target key id chunks probe read!)
                            :independent (read! plain #(contains? domain (nth % pos))))
           :distinct-keys (count domain) :cost-model (:cost-model planning)
           :batch-count (if (= :bound (:mode decision)) (long (Math/ceil (/ (double (count domain)) cap))) 0))))

(defn- prefer-ordinary-start? [driver target type planning]
  ;; A multi-relation producer has no proven upper bound on distinct keys.
  ;; If the full filtered receiver costs less to transfer than even one full
  ;; producer batch, preserve the planner's ability to start at that filter.
  (let [a (:hint driver) b (:hint target)]
    (and a b (seq (:filters target)) (next (get-in target [:group :entries]))
         (neg? (compare (transfer-cost b (:rows b) 1 0 planning)
                        (transfer-cost a (batch-cap target type) 1 0 planning))))))

(defn- execute-join! [runtime {:keys [key type next-id sum? native? ordering]} parsed driver target planning estimate join! finish!]
  (let [driver-hint (:hint driver) target-hint (:hint target)
        counter (volatile! 0)
        ;; A selective receiver can be the better starting site. Abandon this
        ;; speculative direction if reading just the driver already costs more
        ;; transfer than fetching the receiver's estimated complete result.
        abandon? (when (and native? driver-hint target-hint (seq (:filters target)))
                   (let [bulk (transfer-cost target-hint (:rows target-hint) 1 0 planning)]
                     (fn [observed]
                       (when (pos? (compare (transfer-cost driver-hint observed 1 0 planning) bulk))
                         (vreset! counter observed) true))))
        row-releases (when native? (volatile! []))
        rows (relational/rows! runtime (assoc (:plain driver) :group (:session driver)
                                             :abandon? abandon? :row-releases row-releases))
        target-result (if (nil? rows)
                        {:mode :ordinary :reason :driver-transfer :driver-keys-at-least @counter}
                        (let [release (charge! runtime (+ 256 (* 128 (count rows))))
                              _ (when row-releases (vswap! row-releases conj release))
                              pos (.indexOf ^java.util.List (:exports driver) key)
                              domain (into #{} (map (fn [row]
                                                      (control/check-active! (:control runtime))
                                                      (nth row pos))) rows)]
                          (target-rows! runtime target key type next-id domain planning estimate)))
        _ (when (and row-releases (= :ordinary (:mode target-result)))
            (doseq [release @row-releases] (release)))
        rel (when-not (= :ordinary (:mode target-result))
              (let [receiver (tuple-relation runtime (:exports target) (:rows target-result))]
                (if native? receiver (join! (tuple-relation runtime (:exports driver) rows) receiver))))
        ;; The aggregate is already final for this visible equality key.
        query (if sum?
                (assoc parsed :qwith [] :qfind
                       (parser/->FindRel (mapv #(parser/->Variable (:variable (relational/projection %)))
                                               (parser/find-elements (:qfind parsed)))))
                parsed)]
    (cond-> (dissoc target-result :rows)
      rel (assoc :result (if native? (finish! query rel ordering) (finish! query rel))))))

(defn- describe! [runtime parts driver target result]
  (diagnostics/sources! runtime (session/explain! runtime (mapv :session parts)))
  (diagnostics/region! runtime
    {:program-id (diagnostics/program! runtime) :region-id 0 :policy :costed-site-join :selection :planner
     :placement (merge {:driver (get-in driver [:group :aliases 0])
                        :sites (mapv #(get-in % [:group :aliases 0]) parts)
                        :estimates (mapv :hint parts) :per-key-estimates (mapv :per-key parts)}
                       (if result (dissoc result :result) {:mode :after-driver}))
     :steps (mapv (fn [part id]
                    {:source-symbol (get-in part [:group :aliases 0]) :alternative-id id
                     :covers (set (map :id (get-in part [:plain :request :clauses])))
                     :access {:kind :relational} :columns (get-in part [:plain :request :columns])})
                  (cond (= :filtered-receiver-transfer (:reason result)) []
                        (= :ordinary (:mode result)) [driver]
                        :else [driver target])
                  ["site-driver" (case (:mode result)
                                   :independent "independent-site" :empty "empty-driver"
                                   (if (:existence? target) "key-existence" "costed-key-restriction"))])
     :operators []}))

(defn try-plan! [runtime {:keys [compiled prepared] :as compilation} input execute? join! finish! fallback!]
  (let [groups (:groups prepared) parsed (:parsed compiled) root (first (:regions compiled))
        entries (mapcat :entries groups)
        planning (:planning prepared)]
    (when (and (or (empty? (:native-extrema prepared))
                   (and (= :elapsed (:cost-model planning)) (pos? (long (:estimate-max-requests planning)))
                        (pos? (long (:estimate-budget-ms planning)))))
               (= 2 (count groups)) (<= 2 (count entries) 12)
               (= (count entries) (count (:entries input)))
               (every? #(pos-int? (get-in % [:descriptor :datalevin.query.source/integer-domain-limit])) entries)
               (= 1 (count (:regions compiled))) (empty? (:sites compiled)) (empty? (:rules compiled))
               (empty? (:columns input)) (= 1 (count (get-in input [:relation :tuples])))
               (<= 1 (count (:patterns root)) 32) (<= (count (:qwhere parsed)) 48)
               (instance? FindRel (:qfind parsed)) (empty? (:qhaving parsed)) (nil? *math-context*)
               (empty? (:ignore-nil-extrema prepared)))
      (when-let [{:keys [key type parts next-id] :as shape} (layout compiled groups (:native-extrema prepared))]
        (charge! runtime (+ 8192 (* 2048 (count (:qwhere parsed)))))
        (let [plain (mapv #(plan! runtime % key next-id nil) parts)
              example [(if (= :long type) 0 "")]
              bound (mapv #(plan! runtime % key next-id example) parts)]
          (when (and (every? some? plain) (some some? bound))
            (let [sessions (session/acquire! runtime prepared)
                  ordering (when (:native? shape) (extrema/prepare! runtime compilation sessions))
                  estimate (relational/estimator runtime planning)
                  estimates (mapv estimate sessions plain)
                  per-key (if (:native? shape) [nil nil] (mapv estimate sessions bound))
                  i (or (:driver-index shape) (driver-index parts type estimates per-key bound planning))
                  sites (mapv (fn [p s a b h k] (assoc p :session s :plain a :bound b :hint h :per-key k))
                              parts sessions plain bound estimates per-key)
                  driver (sites i) target (assoc (sites (- 1 (long i))) :ordinary-fallback? (:native? shape))
                  skip-driver? (and (:native? shape) (prefer-ordinary-start? driver target type planning))
                  result (cond skip-driver? {:mode :ordinary :reason :filtered-receiver-transfer}
                               execute? (execute-join! runtime (assoc shape :ordering ordering) parsed
                                                      driver target planning estimate join! finish!))]
              (when (:diagnostics runtime) (describe! runtime sites driver target result))
              (if (= :ordinary (:mode result))
                {:result (fallback! sessions (estimate))}
                (or result {:result nil})))))))))
