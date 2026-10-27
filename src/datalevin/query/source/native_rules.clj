(ns ^:no-doc datalevin.query.source.native-rules
  "Compile stratified native rule bodies with the existing region compiler.
  Rule references read completed invocation-owned relations; the shared
  fixpoint kernel supplies iteration, ownership and set semantics."
  (:require [clojure.set :as set]
            [datalevin.parser :as parser]
            [datalevin.query.source.access :as access]
            [datalevin.query.source.collection :as collection]
            [datalevin.query.source.control :as control]
            [datalevin.query.source.recursion :as recursion]
            [datalevin.query.source.runtime :as rt]
            [datalevin.query.source.value :as value :refer [fail!]]
            [datalevin.relation :as relation]
            [datalevin.rules.relation :as rule-relation])
  (:import [datalevin.parser And Constant DefaultSrc Function Not Or Pattern Predicate RuleExpr Variable]
           [java.util AbstractList HashSet List]
           [org.eclipse.collections.impl.list.mutable FastList]))

(def ^:dynamic ^:private *iteration* nil)
(def ^:private max-stratum-depth 32)
(def ^:private max-adornments 4096)

(defn source-symbol [node inherited]
  (let [source (:source node)]
    (if (or (nil? source) (instance? DefaultSrc source)) inherited (:symbol source))))

(defn- head-vars [branch]
  (mapv :symbol (concat (get-in branch [:vars :required]) (get-in branch [:vars :free]))))

(defn- variables [node]
  (vec (distinct (keep #(when (instance? Variable %) (:symbol %)) (:args node)))))

(defn- retain! [runtime token]
  (rt/own! runtime #(control/release! token)))

(defn- containers! [runtime ^long rows ^long width]
  (when (> rows Integer/MAX_VALUE) (fail! :query-source/limit :reserve))
  (retain! runtime
    (control/reserve! (:control runtime)
      {:retained-bytes (value/add-charge (+ 2048 (* 128 width))
                        (value/multiply-charge rows (+ 512 (* 64 width))))})))

(defn- checked [check! rel]
  (let [^List tuples (:tuples rel)]
    (assoc rel :tuples (proxy [AbstractList] []
                         (size [] (check!) (.size tuples))
                         (get [i] (check!) (.get tuples i))))))

(defn- requirements [node positions]
  (mapv (fn [arg]
          (when-not (instance? Variable arg) (fail! :query-source/unsupported :planning))
          {:variable (:symbol arg) :type :data :nullable? true})
        (distinct (remove #(instance? Constant %) (map #(nth (:args node) %) positions)))))

(defn reference-spec
  "Emit a relation-reference interface only in an explicit body compiler context."
  [compiled node source]
  (let [key [source (get-in node [:name :symbol])]]
    (when-let [{:keys [head-vars input-positions observable-positions]} (get (:rule-relations compiled) key)]
      (when-not (= (count head-vars) (count (:args node))) (fail! :query-source/invalid :planning))
      {:requires (requirements node input-positions)
       :columns (mapv #(hash-map :variable % :type :data :nullable? true) (variables node))
       :rule-reference key :head-vars head-vars :input-positions input-positions
       :observable-positions observable-positions})))

(defn select-inputs
  "Select an already compiled adornment from the step's actual input columns.
  Required bindings always seed the body; optional observable bindings seed it
  only when supplied. Wildcards never become demands."
  [spec node input-cols]
  (let [available (set input-cols)]
    (vec (into (into (sorted-set) (:input-positions spec))
           (filter (fn [i]
                     (let [arg (nth (:args node) i)]
                       (or (instance? Constant arg)
                           (and (instance? Variable arg) (contains? available (:symbol arg)))))))
           (:observable-positions spec)))))

(defn reference!
  "Borrow the current complete relation on the serial iteration's owning thread."
  [runtime key node input-cols tuples]
  (control/check-active! (:control runtime))
  (when-not (and (identical? (:control runtime) (:control *iteration*))
                 (= (:query-id runtime) (:query-id *iteration*))
                 (or (contains? (:tables *iteration*) key) (:complete! *iteration*)))
    (fail! :query-source/invalid :execution))
  (if (contains? (:tables *iteration*) key)
    (do (when-let [request! (:request! *iteration*)] (request! key node input-cols tuples))
        (get (:tables *iteration*) key))
    ((:complete! *iteration*) key node input-cols tuples)))

(defn call!
  "Apply constants, repeated arguments and wildcards to a complete head set.
  Shared matching preserves predicate equivalence; shared distinct projection
  retains rule-call set semantics after ignored head positions disappear."
  [runtime check! node rel input-positions]
  (check!)
  (containers! runtime 0 (count (:args node)))
  (let [required (mapv #(vector % (nth (:args node) %)) input-positions)
        positions (reduce (fn [positions [i arg]]
                            (if (and (instance? Variable arg) (not (contains? positions (:symbol arg))))
                              (assoc positions (:symbol arg) i) positions)) {} required)
        key-equalities (into [] (keep (fn [[i arg]]
                                       (when (instance? Variable arg)
                                         (let [first-position (get positions (:symbol arg))]
                                           (when (not= i first-position) [first-position i]))))) required)
        ;; Predicate-equivalent arguments can belong to different demand keys.
        ;; Constrain every required position before projecting away duplicates.
        key-values (into {} (keep (fn [[i arg]] (when (instance? Constant arg) [i (:value arg)]))) required)
        mapped (collection/match! runtime (:args node) (:tuples rel) check!
                 {:positions positions :key-values key-values :key-equalities key-equalities})
        vars (variables node)]
    (containers! runtime (.size ^List (:tuples mapped)) (count vars))
    (let [view (checked check! mapped)
          result (relation/project-distinct view vars)]
      (check!)
      (if (identical? (:tuples view) (:tuples result)) (assoc result :tuples (:tuples mapped)) result))))

(defn- graph!
  [runtime entries rules root source]
  (containers! runtime (+ (count entries) (count rules)) 2)
  (let [check! #(control/check-active! (:control runtime))
        entries (into {} (map (juxt :symbol identity)) entries)
        rules (into {} (map #(vector (get-in % [:name :symbol]) %)) rules)
        graph (atom {})]
    (letfn [(visit-node! [node inherited depth calls negatives negative?]
              (check!)
              (when (> (long depth) 128) (fail! :query-source/limit :planning))
              (containers! runtime 0 1)
              (let [source (source-symbol node inherited)]
                (cond
                  (instance? Not node)
                  (doseq [clause (:clauses node)]
                    (visit-node! clause source (inc (long depth)) calls negatives (not negative?)))
                  (instance? Pattern node)
                  (when-not (and (contains? entries source) (:native (get entries source)))
                    (fail! :query-source/unsupported :planning))
                  (instance? RuleExpr node)
                  (let [key [source (get-in node [:name :symbol])]]
                    (vswap! calls conj key)
                    (when negative? (vswap! negatives conj key))
                    (visit-rule! node source (inc (long depth))))
                  (or (instance? Or node) (instance? And node))
                  (doseq [clause (:clauses node)]
                    (visit-node! clause source (inc (long depth)) calls negatives negative?)))))
            (visit-rule! [node source depth]
              (check!)
              (when (> (long depth) 128) (fail! :query-source/limit :planning))
              (let [name (get-in node [:name :symbol]) key [source name]
                    rule (get rules name) head (head-vars (first (:branches rule)))]
                (when-not (and rule (= (count head) (count (:args node))))
                  (fail! :query-source/invalid :planning))
                (when-not (contains? @graph key)
                  (containers! runtime (count (:branches rule)) (count head))
                  (doseq [branch (:branches rule)]
                    (when (not= (count (head-vars branch)) (count (set (head-vars branch))))
                      (fail! :query-source/unsupported :planning)))
                  (swap! graph assoc key {:head-vars head})
                  (let [calls (volatile! #{}) negatives (volatile! #{})]
                    (doseq [branch (:branches rule) clause (:clauses branch)]
                      (visit-node! clause source (inc (long depth)) calls negatives false))
                    (swap! graph assoc key {:head-vars head :source source
                                            :required-count (count (get-in (first (:branches rule)) [:vars :required]))
                                            :branches (:branches rule) :dependencies @calls
                                            :negative-dependencies @negatives})))))]
      (visit-rule! root source 0)
      @graph)))

(defn- input-positions!
  "Track caller-observable head positions through public scope interfaces.
  Bound adornments preserve those inputs; enumeration obtains them locally."
  [runtime ids graph]
  (let [check! #(control/check-active! (:control runtime))
        observed (volatile! #{}) edges (volatile! [])
        edge! (fn [from to]
                (containers! runtime 1 2)
                (vswap! edges conj [from to]))]
    (letfn [(visit! [node inherited local path depth]
              (check!)
              (when (> (long depth) 128) (fail! :query-source/limit :planning))
              (containers! runtime 0 2)
              (let [source (source-symbol node inherited)]
                (cond
                  (instance? Function node)
                  (doseq [arg (:args node) :when (instance? Variable arg)]
                    (containers! runtime 1 1)
                    (vswap! observed conj (local (:symbol arg))))
                  (instance? RuleExpr node)
                  (doseq [[i arg] (map-indexed vector (:args node)) :when (instance? Variable arg)]
                    (edge! [[source (get-in node [:name :symbol])] :head i] (local (:symbol arg))))
                  (or (instance? Or node) (instance? Not node))
                  (let [exports (if (instance? Not node) (:vars node)
                                    (concat (get-in node [:rule-vars :required]) (get-in node [:rule-vars :free])))
                        exports (set (map :symbol exports))]
                    (doseq [[i clause] (map-indexed vector (:clauses node))]
                      (let [child (conj path i)]
                        (visit! clause source #(if (exports %) (local %) [child %]) child (inc (long depth))))))
                  (instance? And node)
                  (doseq [[i clause] (map-indexed vector (:clauses node))]
                    (visit! clause source local (conj path i) (inc (long depth)))))))]
      (doseq [id ids [j branch] (map-indexed vector (:branches (get graph id)))]
        (let [local #(vector id j %)]
          (doseq [[i v] (map-indexed vector (head-vars branch))] (edge! (local v) [id :head i]))
          (doseq [[i clause] (map-indexed vector (:clauses branch))]
            (visit! clause (:source (get graph id)) local [id j i] 0))))
      (loop [known @observed]
        (check!)
        (let [next (reduce (fn [known [from to]]
                             (check!)
                             (if (contains? known from) (conj known to) known)) known @edges)]
          (if (= known next)
            (into {} (map (fn [id]
                            [id (into [] (filter #(or (< (long %) (long (:required-count (get graph id))))
                                                     (contains? known [id :head %])))
                                  (range (count (:head-vars (get graph id)))))])) ids)
            (recur next)))))))

(defn- binding-inputs!
  "Infer a conjunction's binding interface without executing its clauses.
  Private union variables cannot become inputs of the enclosing scope."
  [runtime clauses exports initial positions source depth pattern-inputs]
  (when (> (long depth) 128) (fail! :query-source/limit :planning))
  (containers! runtime (+ 2 (count clauses)) (+ 2 (count exports)))
  (let [check! #(control/check-active! (:control runtime))
        vars (fn [args] (into #{} (keep #(when (instance? Variable %) (:symbol %))) args))
        operations
        (mapv (fn [node]
                (check!)
                (containers! runtime 0 (count (or (:args node) (:pattern node) (:vars node))))
                (let [source (source-symbol node source)]
                  (cond
                    (instance? Pattern node)
                    {:requires (pattern-inputs node source) :columns (vars (:pattern node))}
                    (instance? RuleExpr node)
                    {:requires (vars (map #(nth (:args node) %) (get positions [source (get-in node [:name :symbol])])))
                     :columns (vars (:args node))}
                    (instance? Function node)
                    {:requires (vars (:args node)) :columns #{(get-in node [:binding :variable :symbol])}}
                    (instance? Predicate node) {:requires (vars (:args node)) :columns #{}}
                    (instance? Or node)
                    (let [required (vars (get-in node [:rule-vars :required]))
                          public (vars (concat (get-in node [:rule-vars :required]) (get-in node [:rule-vars :free])))
                          inputs (reduce (fn [inputs clause]
                                           (set/union inputs
                                             (binding-inputs! runtime (if (instance? And clause) (:clauses clause) [clause])
                                               public required positions source (inc (long depth)) pattern-inputs))) required (:clauses node))]
                      {:requires inputs :columns public})
                    (instance? Not node) {:negative (vars (:vars node)) :columns #{}}
                    :else {:requires #{} :columns #{}}))) clauses)
        available (into exports (mapcat :columns) operations)
        operations (mapv #(if (:negative %) (assoc % :requires (set/intersection available (:negative %))) %) operations)]
    (loop [inputs initial]
      (check!)
      (let [[known pending]
            (loop [known inputs pending operations]
              (check!)
              (if-let [ready (some #(when (set/subset? (:requires %) known) %) pending)]
                (recur (into known (:columns ready)) (filterv #(not (identical? ready %)) pending))
                [known pending]))
            missing (set/difference (if (seq pending) (into #{} (mapcat :requires) pending) exports) known)
            roots (set/difference missing (into #{} (mapcat :columns) pending))
            next (into inputs (set/intersection exports (if (seq roots) roots missing)))]
        (cond (not= inputs next) (recur next)
              (or (seq pending) (seq missing)) (fail! :query-source/unsupported :planning)
              :else inputs)))))

(defn- dependency-inputs!
  "Forward missing head bindings through calls and public union interfaces.
  Positive local producers can satisfy a callee input without a caller seed."
  [runtime ids graph initial pattern-inputs]
  (containers! runtime (reduce + 0 (map #(count (:head-vars %)) (vals graph))) 2)
  (loop [positions initial]
    (control/check-active! (:control runtime))
    (let [next
          (reduce (fn [positions id]
                    (reduce (fn [positions branch]
                              (rt/with-workspace runtime
                                (fn [workspace]
                                  (let [head (head-vars branch)
                                        inputs (binding-inputs! workspace (:clauses branch) (set head)
                                                 (into #{} (map #(nth head %)) (get positions id))
                                                 positions (:source (get graph id)) 0 pattern-inputs)]
                                    (assoc positions id (into [] (keep-indexed #(when (inputs %2) %1)) head))))))
                      positions (:branches (get graph id)))) positions ids)]
      (if (= positions next) positions (recur next)))))

(defn- adornments!
  "Enumerate legal input interfaces with admission before every expansion.
  Distinct adornments never share demand or answer relations."
  [runtime ids minimum observed]
  (let [n (volatile! 0)]
    (into {} (map (fn [id]
                    (let [base (get minimum id)
                          optional (remove (set base) (get observed id))
                          admit! (fn [positions]
                                   (control/check-active! (:control runtime))
                                   (when (>= (long @n) (long max-adornments)) (fail! :query-source/limit :planning))
                                   (containers! runtime 1 (count positions))
                                   (vswap! n (fn [n] (inc (long n))))
                                   positions)]
                      [id (reduce (fn [variants i]
                                    (into variants (map #(admit! (vec (sort (conj % i))))) variants))
                            [(admit! base)] optional)]))) ids)))

(defn- strata!
  "Solve caller >= callee (+ 1 for a negative dependency). A finite graph
  needs at most N rounds; further growth proves a cycle through negation."
  [runtime ids graph]
  (containers! runtime (* 2 (count ids)) 2)
  (let [check! #(control/check-active! (:control runtime))]
    (loop [levels (zipmap ids (repeat 0)) round 0]
      (check!)
      (let [next (reduce (fn [levels id]
                           (reduce (fn [levels dependency]
                                     (check!)
                                     (update levels id max
                                       (+ (long (get levels dependency))
                                          (if (contains? (:negative-dependencies (get graph id)) dependency) 1 0))))
                             levels (:dependencies (get graph id)))) levels ids)]
        (cond (= levels next)
              (do
                ;; Lower strata complete synchronously inside compiled steps.
                ;; Bound that descent before allocating body programs or using
                ;; the JVM stack; clause nesting has its separate logical cap.
                (when (some #(> (long %) (long max-stratum-depth)) (vals levels)) (fail! :query-source/limit :planning))
                levels)
              (>= round (count ids)) (fail! :query-source/unsupported :planning)
              :else (recur next (inc round)))))))

(defn- program-clauses [program]
  (mapcat (fn [r]
            (mapcat (fn [clause]
                      (let [spec (get (:core-specs program) (:path clause))]
                        (cons (assoc clause :default-source (:default-source r) :rule-spec spec)
                              (when (:negation? spec) (program-clauses (:program spec))))))
              (:core-clauses r))) (get-in program [:compiled :regions])))

(defn- analyze-heads!
  "Conservative producer and comparison propagation.
  Required inputs are producers; comparison proofs also follow negative facts
  and copied values without claiming that those facts produce outer values."
  [runtime ids headers programs]
  (let [check! #(control/check-active! (:control runtime))
        seeds (volatile! {}) inputs (volatile! {}) constraints (volatile! {})
        edges (volatile! []) constraint-edges (volatile! [])
        union-column (fn [a b]
                       (cond (nil? a) b (nil? b) a
                             :else {:type (if (= (:type a) (:type b)) (:type a) :data)
                                    :nullable? (or (:nullable? a) (:nullable? b))}))
        seed! (fn [target v column]
                (containers! runtime 1 2)
                (vswap! target update v union-column (select-keys column [:type :nullable?])))
        edge! (fn [from to direct?]
                (containers! runtime 1 2)
                (vswap! edges conj [from to])
                (if direct?
                  (vswap! constraint-edges conj [from to])
                  ;; A comparison on a copied value also constrains its input.
                  (vswap! constraint-edges conj [to from])))
        constrain-negative!
        (fn constrain! [program local prefix]
          (doseq [r (get-in program [:compiled :regions]) {:keys [node path]} (:core-clauses r)]
            (check!)
            (let [spec (get (:core-specs program) path)]
              (cond
                (instance? Pattern node)
                (doseq [column (:columns spec)] (seed! constraints (local (:variable column)) column))
                (instance? RuleExpr node)
                (doseq [[i arg] (map-indexed vector (:args node))
                        :when (and (some? (:selected-input-positions spec)) (instance? Variable arg))]
                  (containers! runtime 1 2)
                  (let [edge [[(conj (:rule-reference spec) (:selected-input-positions spec)) :head i] (local (:symbol arg))]]
                    (vswap! constraint-edges conj edge)))
                (instance? Function node)
                (let [arg (first (:args node)) output (get-in node [:binding :variable :symbol])]
                  (cond (instance? Variable arg)
                        (do (containers! runtime 1 2)
                            (vswap! constraint-edges conj [(local output) (local (:symbol arg))]))
                        (instance? Constant arg)
                        (seed! constraints (local output) {:type (value/inferred-type (:value arg))
                                                          :nullable? (nil? (:value arg))})))
                (instance? Not node)
                (let [inputs (set (map :variable (:requires spec))) child (conj prefix path)]
                  (constrain! (:program spec) #(if (inputs %) (local %) [child %]) child))))))
        propagate (fn [types edges]
                    (loop [types types]
                      (check!)
                      (let [next (reduce (fn [types [from to]]
                                           (check!)
                                           (if-let [column (get types from)]
                                             (update types to union-column column) types)) types edges)]
                        (if (= types next) types (recur next)))))
        columns (fn [types]
                  (into {} (map (fn [id]
                                  [id (mapv (fn [i v] (some-> (get types [id :head i]) (assoc :variable v)))
                                            (range) (:head-vars (get headers id)))])) ids))]
    (doseq [id ids [branch-index {:keys [head-vars program]}] (map-indexed vector (get programs id))]
      (let [local #(vector id branch-index %)]
        (doseq [v (map #(nth head-vars %) (:input-positions (get headers id)))]
          (seed! inputs (local v) {:type :data :nullable? true}))
        (doseq [r (get-in program [:compiled :regions]) {:keys [node path]} (:core-clauses r)]
          (check!)
          (cond
            (instance? Pattern node)
            (doseq [column (:columns (get (:core-specs program) path))]
              (seed! seeds (local (:variable column)) column)
              (seed! constraints (local (:variable column)) column))
            (instance? RuleExpr node)
            (let [spec (get (:core-specs program) path)
                  key (conj (:rule-reference spec) (:selected-input-positions spec))]
              (doseq [[i arg] (map-indexed vector (:args node))
                      :when (and (some? (:selected-input-positions spec)) (instance? Variable arg))]
                (edge! [key :head i] (local (:symbol arg)) true)))
            (instance? Not node)
            (let [spec (get (:core-specs program) path) inputs (set (map :variable (:requires spec)))
                  prefix [id branch-index :negative path]]
              ;; Negative bodies constrain existing values; their facts and
              ;; outputs must never become positive head-type producers.
              (constrain-negative! (:program spec) #(if (inputs %) (local %) [prefix %]) prefix))
            (instance? Function node)
            (let [arg (first (:args node)) output (get-in node [:binding :variable :symbol])]
              (cond
                (instance? Constant arg)
                (let [column {:type (value/inferred-type (:value arg)) :nullable? (nil? (:value arg))}]
                  (seed! seeds (local output) column)
                  (seed! constraints (local output) column))
                (instance? Variable arg)
                (edge! (local (:symbol arg)) (local output) false)))))
        (doseq [[i v] (map-indexed vector head-vars)] (edge! (local v) [id :head i] true))))
    {:producers (columns (propagate @seeds @edges))
     :comparisons (columns (propagate @constraints @constraint-edges))
     :columns (columns (propagate (merge-with union-column @seeds @inputs) @edges))}))

(defn- literal-key-safe?
  [x check!]
  (check!)
  (cond
    (number? x) false
    (satisfies? value/IQuerySourceValue x) false
    (map? x) (every? (fn [[k v]] (and (literal-key-safe? k check!) (literal-key-safe? v check!))) x)
    (or (vector? x) (list? x) (set? x)) (every? #(literal-key-safe? % check!) x)
    :else true))

(defn- check-literal-seeds! [runtime headers producers node source]
  (let [check! #(control/check-active! (:control runtime))
        id [source (get-in node [:name :symbol])]]
    (doseq [i (:input-positions (get headers id)) :let [arg (nth (:args node) i)]
            :when (instance? Constant arg)]
      (let [x (:value arg) column (get-in producers [id i])]
        (value/execution-value! x check!)
        (when-not (or (nil? column) (literal-key-safe? x check!)
                      (and (= :long (:type column)) (int? x)))
          (fail! :query-source/unsupported :planning))))))

(defn- requests!
  "Own distinct correlated demand tuples before a body workspace closes.
  The solver copies these into its demand predicates; this staging owner is
  released immediately when solving ends, including on partial failure."
  [runtime headers]
  (let [ctl (:control runtime) check! #(control/check-active! ctl)
        tokens (volatile! []) tables (volatile! {}) closed? (atom false)
        hold! #(do (vswap! tokens conj %) %)
        close (fn []
                (when (compare-and-set! closed? false true)
                  (doseq [{:keys [^List rows ^HashSet seen]} (vals @tables)] (.clear rows) (.clear seen))
                  (vreset! tables {})
                  (doseq [token @tokens] (control/release! token))
                  (vreset! tokens [])))]
    (try
      (hold! (control/reserve! ctl
               {:retained-bytes
                (value/add-charge (+ 4096 (* 4096 (count headers)))
                  (value/multiply-charge 1024
                    (reduce (fn [^long n header] (+ n (count (:input-positions header))))
                            0 (vals headers))))}))
      (vreset! tables (into {} (map (fn [[id header]]
                                     [id {:rows (FastList.) :seen (HashSet.)
                                          :positions (:input-positions header)
                                          :variables (mapv #(nth (:head-vars header) %) (:input-positions header))}])) headers))
      {:close close
       :relation! (fn [id]
                    (check!)
                    (when @closed? (fail! :query-source/invalid :execution))
                    (let [{:keys [variables rows]} (get @tables id)]
                      (relation/relation! (zipmap variables (range)) rows)))
       :request!
       (fn [id node input-cols ^List tuples]
         (check!)
         (when @closed? (fail! :query-source/invalid :execution))
         (let [{:keys [^List rows ^HashSet seen variables positions]} (get @tables id)
               width (count variables)
               workspace (control/reserve! ctl {:retained-bytes (+ 1024 (* 128 (+ width (count input-cols))))})]
           (try
             (let [attrs (zipmap input-cols (range)) args (map #(nth (:args node) %) positions)
                   scratch (object-array width) lookup (relation/array-lookup)]
               (dotimes [i (.size tuples)]
                 (check!)
                 (let [^objects tuple (.get tuples i)]
                   (doseq [[j arg] (map-indexed vector args)]
                     (aset scratch j (if (instance? Constant arg) (:value arg)
                                         (let [index (get attrs (:symbol arg))]
                                           (when-not (and (integer? index) (< -1 index (alength tuple)))
                                             (fail! :query-source/invalid :execution))
                                           (aget tuple (int index))))))
                   (doseq [x scratch] (value/execution-value! x check!))
                   (when-not (.contains seen (relation/reset-array-lookup! lookup scratch))
                     (when (= Integer/MAX_VALUE (.size rows)) (fail! :query-source/limit :reserve))
                     (hold! (control/reserve! ctl {:retained-bytes (+ 512 (* 64 width))}))
                     (let [row (vec scratch) _ (hold! (rt/charge-copy! runtime row))
                           owned (object-array (value/owned-copy row check!))]
                       (.add seen (relation/wrap-array owned))
                       (.add rows owned))))))
             (finally (control/release! workspace)))))}
      (catch Throwable e (close) (throw e)))))

(defn- solve-seeded!
  [runtime headers ids empty-tables dependencies owners run-rule! root node input-cols tuples complete!]
  (let [pending (requests! runtime headers)]
    (try
      (let [demand-id #(vector ::demand %)
            demand-ids (mapv demand-id ids) all-ids (into ids demand-ids)
            ;; A body can discover new demands without producing any answers.
            ;; Drain all requests after each active round, including that case.
            all-dependencies (set all-ids)
            demand-rules (zipmap demand-ids ids)
            schemas (into empty-tables
                          (map (fn [id]
                                 [(demand-id id) (relation/relation!
                                                   (zipmap (map #(nth (:head-vars (get headers id)) %) (:input-positions (get headers id)))
                                                           (range)) (FastList.))])) ids)
            dependencies (into (into {} (map (fn [[id deps]] [id (conj deps (demand-id id))])) dependencies)
                               (map #(vector % all-dependencies)) demand-ids)]
        ((:request! pending) root node input-cols tuples)
        (let [handle (recursion/evaluate! runtime schemas all-ids dependencies
                       (fn [workspace totals _ id]
                         (if-let [rule (get demand-rules id)]
                           ((:relation! pending) rule)
                           (let [seeds (get totals (demand-id id))]
                             (if (.isEmpty ^List (:tuples seeds)) (get empty-tables id)
                                 (run-rule! workspace totals id seeds (:request! pending) complete!)))))
                       {:owners owners
                        :initialize! (fn [_ id]
                                       (if-let [rule (get demand-rules id)] ((:relation! pending) rule)
                                           (get empty-tables id)))})]
          (get ((:relations handle)) root)))
      (finally ((:close pending))))))

(defn prepare!
  "Compile stratified native bodies and their bounded input adornments before
  provider acquisition. Actual step inputs select a complete correlated demand
  interface; unbound observable heads use independently compiled enumeration."
  [runtime entries rules node source pattern-inputs compile-body!]
  (let [check! #(control/check-active! (:control runtime))
        logical-graph (graph! runtime entries rules node source)
        logical-ids (vec (sort #(access/compare-keys %1 %2 check!) (keys logical-graph)))
        logical-levels (strata! runtime logical-ids logical-graph)
        observed (input-positions! runtime logical-ids logical-graph)
        minimum (dependency-inputs! runtime logical-ids logical-graph
                  (into {} (map (fn [[id rule]] [id (vec (range (:required-count rule)))])) logical-graph)
                  pattern-inputs)
        variants (adornments! runtime logical-ids minimum observed)
        reference-headers
        (into {} (map (fn [[id rule]]
                        [id (assoc (select-keys rule [:head-vars :required-count])
                              :input-positions (get minimum id) :observable-positions (get observed id))])) logical-graph)
        ids (into [] (mapcat (fn [id] (map #(conj id %) (get variants id)))) logical-ids)
        variant-ids (group-by #(subvec % 0 2) ids)
        graph (into {} (map #(vector % (get logical-graph (subvec % 0 2)))) ids)
        levels (into {} (map #(vector % (get logical-levels (subvec % 0 2)))) ids)
        headers (into {} (map (fn [id]
                               [id (assoc (select-keys (get graph id) [:head-vars :required-count])
                                     :input-positions (nth id 2))])) ids)
        heads (into {} (map (fn [[id header]] [id (:head-vars header)])) headers)
        root [source (get-in node [:name :symbol])]
        rules-by-name (into {} (map #(vector (get-in % [:name :symbol]) %)) rules)
        empty-tables (into {} (map (fn [[id head]] [id (relation/relation! (zipmap head (range)) (FastList.))])) heads)
        programs
        (into {} (map (fn [id]
                        (let [rule (get logical-graph (subvec id 0 2))
                              body-ids (vec (sort #(access/compare-keys %1 %2 check!) (:dependencies rule)))
                              body-headers (select-keys reference-headers body-ids)
                              body-rules (mapv rules-by-name (distinct (map second body-ids)))]
                          [id (mapv (fn [branch]
                                      (check!)
                                      (let [head (head-vars branch)
                                            columns (mapv #(hash-map :variable % :type :data :nullable? true)
                                                          (map #(nth head %) (nth id 2)))
                                            query (parser/map->Query
                                                    {:qfind (parser/->FindRel (mapv parser/->Variable head))
                                                     :qin [] :qwhere (:clauses branch)})]
                                        {:head-vars head :inputs (mapv :variable columns)
                                         :program (compile-body! query
                                                    {:rule-relations body-headers :default-source (:source rule)} body-rules columns)}))
                                    (:branches rule))]))) ids)
        owners (vec (mapcat (fn [id] (map #(get-in % [:program :runtime]) (get programs id))) ids))
        analysis (analyze-heads! runtime ids headers programs)
        union-column (fn [a b]
                       (cond (nil? a) b (nil? b) a
                             :else {:type (if (= (:type a) (:type b)) (:type a) :data)
                                    :nullable? (or (:nullable? a) (:nullable? b))}))
        logical-columns (fn [columns]
                          (into {} (map (fn [id]
                                          [id (reduce (fn [a variant]
                                                        (mapv union-column a (get columns variant)))
                                                (vec (repeat (count (:head-vars (get logical-graph id))) nil))
                                                (get variant-ids id))])) logical-ids))
        head-columns (logical-columns (:columns analysis))
        comparisons (logical-columns (:comparisons analysis))
        literal-headers (into {} (map (fn [[id header]]
                                       [id (assoc header :input-positions
                                             (vec (sort (set/union (set (:input-positions header))
                                                          (set (:observable-positions header))))))])) reference-headers)
        _ (check-literal-seeds! runtime literal-headers comparisons node source)
        _ (doseq [id ids {:keys [program]} (get programs id)
                  {:keys [node default-source]} (program-clauses program)
                  :when (instance? RuleExpr node)]
            (check-literal-seeds! runtime literal-headers comparisons node (source-symbol node default-source)))
        call-columns
        (reduce (fn [columns [arg column]]
                  (if (instance? Variable arg)
                    (let [v (:symbol arg) before (get columns v)
                          column (or column {:type :data :nullable? true})]
                      (assoc columns v {:variable v :type (if (or (nil? before) (= (:type before) (:type column)))
                                                           (:type column) :data)
                                        :nullable? (or (:nullable? before) (:nullable? column))})) columns))
                {} (map vector (:args node) (get head-columns root)))
        dependencies
        (into {} (map (fn [id]
                        [id (into #{} (for [{:keys [program]} (get programs id)
                                           {:keys [node rule-spec]} (program-clauses program)
                                           :when (and (instance? RuleExpr node) (some? (:selected-input-positions rule-spec)))]
                                       (do (containers! runtime 1 2)
                                           (conj (:rule-reference rule-spec) (:selected-input-positions rule-spec)))))])) ids)
        run-rule!
        (fn [workspace tables id seeds request! complete!]
          (binding [*iteration* {:control (:control runtime) :query-id (:query-id runtime)
                                :tables tables :request! request! :complete! complete!}]
            (reduce
              (fn [acc {:keys [head-vars inputs program]}]
                (check!)
                (let [input (when seeds (assoc seeds :attrs (zipmap inputs (range))))
                      body ((:run! program) input) width (count head-vars)]
                  (containers! workspace (.size ^List (:tuples body)) width)
                  (let [projected (rule-relation/project-rule-result (checked check! body) head-vars)
                        projected (rule-relation/rename-rel-attrs projected (get heads id))
                        n (+' (.size ^List (:tuples acc)) (.size ^List (:tuples projected)))]
                    (containers! workspace n width)
                    (relation/sum-rel acc projected))))
              (get empty-tables id) (get programs id))))
        seeded? (some #(seq (:input-positions %)) (vals headers))
        stratified? (some #(seq (:negative-dependencies %)) (vals graph))
        strata (when stratified?
                 (into {} (map (fn [[level members]]
                                 (let [members (vec members) included (set members)]
                                   [level {:ids members :headers (select-keys headers members)
                                           :empty (select-keys empty-tables members)
                                           :dependencies (into {} (map (fn [id]
                                                                        [id (into #{} (filter included) (get dependencies id))])) members)
                                           :owners (vec (mapcat (fn [id] (map #(get-in % [:program :runtime]) (get programs id))) members))}]))
                       (group-by levels ids))))]
    {:requires (requirements node (get minimum root))
     :input-positions (get minimum root) :observable-positions (get observed root)
     :columns (mapv call-columns (variables node)) :native-rule-program true
     :solve! (fn [positions input-cols tuples]
               (check!)
               (let [root (conj root positions)]
                 (if stratified?
                   (letfn [(solve! [root node input-cols tuples]
                             (check!)
                             (let [level (get levels root) stratum (get strata level)]
                               (solve-seeded! runtime (:headers stratum) (:ids stratum) (:empty stratum)
                                 (:dependencies stratum) (:owners stratum) run-rule! root node input-cols tuples
                                 (fn [key node input-cols tuples]
                                   (when-not (< (long (get levels key)) (long level)) (fail! :query-source/invalid :execution))
                                   (solve! key node input-cols tuples)))))]
                     (solve! root node input-cols tuples))
                   (if seeded?
                     (solve-seeded! runtime headers ids empty-tables dependencies owners run-rule! root node input-cols tuples nil)
                     (let [handle (recursion/evaluate! runtime empty-tables ids dependencies
                                    (fn [workspace totals _ id] (run-rule! workspace totals id nil nil nil))
                                    {:owners owners :initialize! (fn [workspace id] (run-rule! workspace empty-tables id nil nil nil))})]
                       (get ((:relations handle)) root))))))}))
