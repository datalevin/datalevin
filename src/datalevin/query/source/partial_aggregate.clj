(ns ^:no-doc datalevin.query.source.partial-aggregate
  "Distribute a grouped decimal SUM over explicitly disjoint union witnesses."
  (:require [clojure.set :as set]
            [datalevin.parser :as parser]
            [datalevin.query.source.contract :as contract]
            [datalevin.query.source.control :as control]
            [datalevin.query.source.diagnostics :as diagnostics]
            [datalevin.query.source.region :as region]
            [datalevin.query.source.relational :as relational]
            [datalevin.query.source.runtime :as rt]
            [datalevin.query.source.session :as session]
            [datalevin.relation :as relation])
  (:import [datalevin.parser Constant Function Predicate]
           [org.eclipse.collections.impl.list.mutable FastList]))

(defn- branch-plan! [runtime compiled groups site branch elements witnesses]
  (let [r (nth (:regions compiled) branch)
        core (mapv :node (:core-clauses r))
        functions (filter #(instance? Function %) core)
        tag (first functions) arg (first (:args tag)) v (get-in tag [:binding :variable :symbol])
        patterns (mapv #(assoc (:clause %) :source-symbol (:source-symbol %)) (:patterns r))
        aliases (set (map :source-symbol patterns)) group (groups (:source-symbol (first patterns)))
        entries (filterv #(aliases (:symbol %)) (:entries group))
        variables (into #{} (mapcat contract/variables) patterns)
        filters (region/predicate-clauses r variables)
        types (when (= (count entries) (count aliases)) (contract/variable-types entries patterns))
        wanted (set (map :variable elements))]
    (when (and group (= aliases (set (map :symbol entries)))
               (every? #(= 1 (count %)) (vals types))
               (= 1 (count functions)) (= 'ground (get-in tag [:fn :symbol]))
               (= 1 (count (:args tag))) (instance? Constant arg)
               (int? (:value arg)) (witnesses v) (not (variables v))
               (= (count core) (inc (count filters))) (every? #(or (= tag %) (instance? Predicate %)) core)
               (set/subset? (into witnesses wanted) (:exports site))
               (set/subset? (into (disj witnesses v) wanted) variables)
               (every? (fn [{:keys [variable kind]}]
                         (if (= kind :aggregate) (= #{:bigdec} (types variable))
                           (#{#{:long} #{:string}} (types variable)))) elements))
      (let [columns (mapv (fn [{:keys [kind variable]}]
                            {:variable variable :type (if (= kind :aggregate) :data (first (types variable)))
                             :nullable? false}) elements)
            request (merge (:request group)
                      {:mode :relational :semantics-version 1 :bindings {} :columns columns :find elements
                       :sources (mapv #(select-keys % [:descriptor :symbol]) entries)
                       :clauses (into patterns filters) :with (vec (sort (disj witnesses v)))})
            parsed (parser/map->Query {:qfind (parser/->FindRel (mapv #(parser/->Variable (:variable %)) elements))})]
        (when-let [plan (relational/local-plan! runtime group (:source (first entries)) request parsed)]
          (assoc plan :tag-variable v :tag-value (:value arg)))))))

(defn try-plan!
  [runtime {:keys [compiled prepared]} input execute? finish!]
  (let [parsed (:parsed compiled) [site] (:sites compiled)
        elements (mapv relational/projection (parser/find-elements (:qfind parsed)))
        witnesses (set (map :symbol (:qwith parsed))) groups (:groups prepared)]
    (when (and (< 1 (count groups)) (= 1 (count (:sites compiled))) (= :union (:kind site))
               (= 0 (:parent-region site)) (empty? (:required-inputs site))
               (= 1 (count (:qwhere parsed))) (<= 2 (count (:branches site)) 8)
               (empty? (:rules compiled)) (empty? (:columns input)) (nil? *math-context*)
               (= 1 (count (get-in input [:relation :tuples]))) (seq witnesses)
               (empty? (:native-extrema prepared)) (empty? (:ignore-nil-extrema prepared))
               (empty? (:qhaving parsed)) (every? some? elements)
               (= 1 (count (filter #(= :sum (:operator %)) elements)))
               (every? #(or (= :variable (:kind %)) (= :sum (:operator %))) elements)
               (some #(= :variable (:kind %)) elements)
               (= (count elements) (count (set (map :variable elements))))
               (every? #(<= (count (:patterns %)) 48) (:regions compiled)))
      (let [reservation (control/reserve! (:control runtime)
                          {:retained-bytes (+ 8192 (* 2048 (reduce + (map #(count (:patterns %)) (:regions compiled)))))})
            _ (rt/own! runtime #(control/release! reservation))
            prepared-groups (session/groups-by-alias groups)
            plans (mapv #(branch-plan! runtime compiled prepared-groups site % elements witnesses) (:branches site))
            tag (:tag-variable (first plans))]
        (when (and (every? some? plans) (every? #(= tag (:tag-variable %)) plans)
                   (= (count plans) (count (set (map :tag-value plans)))))
          (let [sessions (session/acquire! runtime prepared) acquired (session/groups-by-alias sessions)
                result (when execute?
                         (let [rows (FastList.)
                               vars (conj (mapv :variable elements) tag)]
                           (doseq [plan plans
                                   :let [alias (get-in plan [:request :sources 0 :symbol])
                                         values (relational/rows! runtime (assoc plan :group (acquired alias)))
                                         token (control/reserve! (:control runtime)
                                                 {:retained-bytes (* (count values) (+ 128 (* 16 (count vars))))})
                                         _ (rt/own! runtime #(control/release! token))]
                                   row values]
                             (control/check-active! (:control runtime))
                             (.add rows (object-array (conj row (:tag-value plan)))))
                           ;; Equal subtotals on two channels must both enter the final sum.
                           (finish! (assoc parsed :qwith [(parser/->Variable tag)])
                                    (relation/relation! (zipmap vars (range)) rows))))]
            (when (:diagnostics runtime)
              (diagnostics/sources! runtime (session/explain! runtime sessions))
              (diagnostics/region! runtime
                {:program-id (diagnostics/program! runtime) :region-id 0 :policy :distributed-sum
                 :selection :planner :steps (mapv (fn [plan]
                    {:source-symbol (get-in plan [:request :sources 0 :symbol]) :alternative-id "grouped-sum"
                     :covers (set (map :id (get-in plan [:request :clauses]))) :access {:kind :relational}
                     :columns (get-in plan [:request :columns])}) plans) :operators []}))
            {:result result}))))))
