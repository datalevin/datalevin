(ns ^:no-doc datalevin.query.source.semijoin
  "Place independent dimension restrictions at a common fact source."
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
  (:import [datalevin.parser Pattern Predicate]
           [org.eclipse.collections.impl.list.mutable FastList]))

(defn- variables [patterns]
  (into #{} (mapcat contract/variables) patterns))

(defn- charge! [runtime n]
  (let [token (control/reserve! (:control runtime) {:retained-bytes n})]
    (rt/own! runtime #(control/release! token))))

(defn- connected? [patterns]
  (loop [known (variables (take 1 patterns)) remaining (next patterns)]
    (if (empty? remaining) true
      (let [{linked true rest false} (group-by #(boolean (some known (contract/variables %))) remaining)]
        (and (seq linked) (recur (into known (variables linked)) rest))))))

(defn- component [entries by-source predicates]
  (let [patterns (into [] (mapcat #(by-source (:symbol %))) entries)
        vars (variables patterns)
        filters (filterv #(set/subset? (contract/variables %) vars) predicates)]
    {:entries entries :patterns patterns :vars vars :filters filters}))

(defn- shape [entries groups root terminal]
  (let [patterns (mapv #(assoc (:clause %) :source-symbol (:source-symbol %)) (:patterns root))
        by-source (group-by :source-symbol patterns)
        predicates (region/predicate-clauses root (variables patterns))
        types (contract/variable-types entries patterns)]
    (when (= (count predicates) (count (:core-clauses root)))
      (some (fn [group]
              (let [fact (component (:entries group) by-source predicates)
                    leaves (mapv #(component [%] by-source predicates)
                                 (remove (set (:entries fact)) entries))
                    shared (mapv #(set/intersection (:vars fact) (:vars %)) leaves)
                    keys (mapv first shared)]
                (when (and (seq (:patterns fact)) (connected? (:patterns fact)) (<= 2 (count leaves) 4)
                           (every? #(= 1 (count %)) shared) (= (count keys) (count (set keys)))
                           (every? #(= #{:long} (types %)) keys)
                           (every? #(= 1 (count %)) (vals types))
                           (every? #(and (seq (:patterns %)) (connected? (:patterns %))
                                         (or (seq (:filters %))
                                             (some (fn [p] (= :const (first (last (:terms p))))) (:patterns %)))) leaves)
                           ;; Distinct required keys do not imply independent predicates.
                           ;; A variable shared by two dimensions makes their tuples correlated.
                           (= (reduce + (map #(count (:vars %)) leaves)) (count (apply set/union (map :vars leaves))))
                           (= (set predicates) (into #{} (mapcat :filters) (cons fact leaves)))
                           (set/subset? terminal (set (clojure.core/keys types))))
                  {:fact fact :leaves leaves :keys keys :types (update-vals types first)
                   :terminal terminal :next-id (inc (reduce max -1 (map :id (concat patterns predicates))))}))) groups))))

(defn- local-plan! [runtime groups shape part filters]
  (let [entries (:entries part) leader (first entries) group (groups (:symbol leader))
        vars (vec (sort (set/intersection (:vars part) (into (:terminal shape) (:keys shape)))))
        parsed (parser/map->Query {:qfind (parser/->FindRel (mapv parser/->Variable vars)) :qwith []})
        request (merge (:request group)
                  {:mode :relational :semantics-version 1 :bindings {} :with []
                   :find (mapv #(hash-map :kind :variable :variable %) vars)
                   :columns (mapv #(hash-map :variable % :type ((:types shape) %) :nullable? false) vars)
                   :sources (mapv #(select-keys % [:symbol :descriptor]) entries)
                   :clauses (into (into (:patterns part) (:filters part)) filters)})]
    (relational/local-plan! runtime group (:source leader) request parsed)))

(defn- membership [id variable keys]
  {:id id :kind :predicate :operator :in :semantics-version 1 :args [[:var variable] [:const keys]]})

(defn- rows! [runtime groups plan]
  (let [alias (get-in plan [:request :sources 0 :symbol])
        vars (mapv :variable (get-in plan [:request :columns]))
        rows (relational/rows! runtime (assoc plan :group (groups alias)))
        _ (charge! runtime (+ 256 (* (count rows) (+ 48 (* 8 (count vars))))))
        tuples (FastList. (count rows))]
    (doseq [row rows]
      (control/check-active! (:control runtime))
      (.add tuples (object-array row)))
    (relation/relation! (zipmap vars (range)) tuples)))

(defn- domain [runtime rel key cap]
  (let [i (int ((:attrs rel) key))]
    (reduce (fn [ks ^objects row]
              (control/check-active! (:control runtime))
              (let [ks (conj ks (aget row i))]
                (if (> (count ks) cap) (reduced nil) ks))) #{} (:tuples rel))))

(defn try-plan!
  "Negotiate leaf projections and the fact access before any reads. Actual
  independent key sets become optional predicates on the pinned fact source.
  Complete joins still restore every projected dimension value and witness."
  [runtime {:keys [entries compiled prepared]} input execute? join! finish!]
  (let [groups (:groups prepared) parsed (:parsed compiled) root (first (:regions compiled))
        caps (map #(get-in % [:descriptor :datalevin.query.source/integer-domain-limit]) entries)]
    (when (and (<= 2 (count groups) 5) (<= 3 (count entries) 8)
               (every? #(and (int? %) (pos? %)) caps)
               (empty? (:columns input)) (= 1 (count (get-in input [:relation :tuples])))
               (= 1 (count (:regions compiled))) (empty? (:rules compiled)) (empty? (:sites compiled))
               (empty? (:native-extrema prepared)) (empty? (:ignore-nil-extrema prepared))
               (nil? *math-context*) (empty? (:qhaving parsed))
               (every? #(or (instance? Pattern %) (instance? Predicate %)) (:qwhere parsed))
               (every? relational/projection (parser/find-elements (:qfind parsed)))
               (<= (count (:qwhere parsed)) 48))
      (when-let [shape (shape entries groups root (into (set (parser/find-vars (:qfind parsed))) (map :symbol (:qwith parsed))))]
        (charge! runtime (+ 8192 (* 2048 (count (:qwhere parsed)))))
        (let [prepared-groups (session/groups-by-alias groups)
              plan #(local-plan! runtime prepared-groups shape %1 %2)
              leaves (mapv #(plan % []) (:leaves shape))
              plain (plan (:fact shape) [])
              check (mapv #(membership %1 %2 [0]) (iterate inc (:next-id shape)) (:keys shape))]
          (when (and (every? some? leaves) plain (plan (:fact shape) check))
            (let [sessions (session/acquire! runtime prepared) acquired (session/groups-by-alias sessions)
                  cap (min 8192 (apply min caps))
                  applied (volatile! nil)
                  result (when execute?
                           (let [dims (mapv #(rows! runtime acquired %) leaves)
                                 filters (into [] (keep (fn [[rel key id]]
                                                          (charge! runtime (+ 128 (* 64 (min (inc cap) (count (:tuples rel))))))
                                                          (when-let [ks (domain runtime rel key cap)]
                                                            (membership id key (vec (sort ks))))))
                                               (map vector dims (:keys shape) (iterate inc (:next-id shape))))
                                 target (or (when (seq filters) (plan (:fact shape) filters)) plain)
                                 _ (vreset! applied (if (identical? target plain) [] (mapv #(second (first (:args %))) filters)))
                                 fact (rows! runtime acquired target)]
                             (finish! parsed (reduce join! fact dims))))]
              (when (:diagnostics runtime)
                (diagnostics/sources! runtime (session/explain! runtime sessions))
                (diagnostics/region! runtime
                  {:program-id (diagnostics/program! runtime) :region-id 0 :policy :semijoin-reduction
                   :selection :planner :placement {:eligible-keys (:keys shape) :applied-keys @applied} :steps
                   (mapv (fn [p id]
                           {:source-symbol (get-in p [:request :sources 0 :symbol]) :alternative-id id
                            :covers (set (map :id (get-in p [:request :clauses])))
                            :access {:kind :relational} :columns (get-in p [:request :columns])})
                         (conj leaves plain) (concat (repeat (count leaves) "dimension-keys") [(if (and execute? (empty? @applied)) "unrestricted-fact" "fact-key-sets")]))
                   :operators []}))
              {:result result})))))))
