(ns ^:no-doc datalevin.query.source.key-demand
  "Bounded mandatory-key progress for small unordered DISTINCT windows.
  Plans use captured accesses and sessions; qualification keeps every scope."
  (:refer-clojure :exclude [run!])
  (:require [clojure.set :as set]
            [datalevin.parser :as parser]
            [datalevin.query.source.access :as access]
            [datalevin.query.source.control :as control]
            [datalevin.query.source.permissions :as permissions]
            [datalevin.query.source.relational :as relational]
            [datalevin.query.source.result-window :as window]
            [datalevin.query.source.runtime :as rt]
            [datalevin.relation :as relation])
  (:import [datalevin.parser Constant FindRel Function Variable]
           [java.util List]
           [org.eclipse.collections.impl.list.mutable FastList]))

(defn- eligible? [compiled root changed]
  (let [parsed (:parsed compiled) limit (:qlimit parsed)]
    (and (instance? FindRel (:qfind parsed))
         (every? #(instance? Variable %) (parser/find-elements (:qfind parsed)))
         (seq (parser/find-elements (:qfind parsed)))
         (= (count (parser/find-elements (:qfind parsed))) (count (set (parser/find-vars (:qfind parsed)))))
         (empty? (:qwith parsed)) (empty? (:qhaving parsed)) (empty? (:qorder parsed))
         (nil? (:qreturn-map parsed)) (= 0 (or (:qoffset parsed) 0))
         (integer? limit) (<= 1 limit 256) (empty? (:rules compiled))
         (empty? (:patterns (:region root))) (empty? (:inputs root))
         (<= 2 (count changed) 8)
         ;; This first policy excludes native/rule/custom scalar work. Constant
         ;; bindings stay in qualification; omitting them from a producer can
         ;; only enlarge its candidate domain, never invent a qualified row.
         (every? (fn [{:keys [core-clauses]}]
                   (every? (fn [{:keys [node]}]
                             (and (instance? Function node)
                                  (#{'ground 'identity} (get-in node [:fn :symbol]))
                                  (= 1 (count (:args node)))
                                  (instance? Constant (first (:args node))))) core-clauses))
                 (:regions compiled)))))

(defn- repeatable? [schedules]
  (every? #(= :identical (get-in % [:path :options :capture :alternative :properties :replay]))
          (mapcat :plans schedules)))

(defn- window-plan! [runtime producer key column size]
  (let [{:keys [group patterns]} producer
        parsed (parser/map->Query {:qfind (parser/->FindRel [(parser/->Variable key)])
                                  :qwith [] :qlimit size :qoffset 0})
        request (merge (:request group)
                  {:mode :relational-window :semantics-version 1
                   :sources (mapv #(select-keys % [:symbol :descriptor]) (:entries group))
                   :clauses (mapv #(assoc (:clause %) :source-symbol (:source-symbol %)) patterns)
                   :bindings {} :find [{:kind :variable :variable key}] :with []
                   :columns [(assoc column :variable '?key_demand_result)]
                   :window {:limit size :offset 0 :order []}})]
    (relational/window-plan! runtime group request parsed)))

(defn- optional-schedule [f]
  (try (f)
       (catch clojure.lang.ExceptionInfo e
         (when-not (and (= :query-source/unsupported (:error (ex-data e)))
                        (= :planning (:phase (ex-data e))))
           (throw e)))))

(defn select!
  "Compare a parameterized schedule with the ordinary complete fallback.
  Selection neither reads keys nor reacquires descriptors/sessions."
  [runtime work normal changed planning {:keys [compiled groups]}]
  (let [root (some #(when (empty? (get-in % [:region :path])) %) work)]
    (when (and compiled (eligible? compiled root changed)
               (= (count changed) (count (filter #(get-in % [:step :conditional-union]) (:operators root))))
               (every? #(and (= :snapshot (:actual-consistency %))
                             (permissions/repeatable-default? (:consumption %))) groups)
               (repeatable? normal))
      (let [operators (sort-by :id (vals changed))
            keys (apply set/intersection (map #(set (map :variable (get-in % [:step :conditional-union :columns]))) operators))
            by-id (into {} (map #(vector (get-in % [:region :region-id]) %)) work)
            by-alias (into {} (mapcat (fn [g] (map #(vector % g) (:aliases g)))) groups)
            size (max (long (:qlimit (:parsed compiled)))
                      (min 256 (long (:binding-rows (control/limits (:control runtime))))))]
        (some
          (fn [key]
            (let [column (some #(when (= key (:variable %)) %) (get-in (first operators) [:step :conditional-union :columns]))
                  variants (mapv (fn [op]
                                   (some #(when (= [key] (:binding-inputs %)) %) (:union-alternatives op))) operators)]
              (when (every? some? variants)
                (let [chosen (into {} (map (juxt :id identity)) variants)
                      root-id (get-in root [:region :region-id])
                      bound-root (optional-schedule
                                   #(assoc (access/schedule! runtime root-id [column] (:requests root) (:candidates root)
                                             (mapv (fn [op] (get chosen (:id op) op)) (:operators root))
                                             (assoc planning :required-vars (get-in root [:region :required-vars])))
                                      :input-columns [column]))
                      schedules (when bound-root
                                  (let [selected (reduce (fn [m op]
                                                           (reduce #(assoc %1 (:region-id %2) %2) m (:scope-plans op)))
                                                         (into {} (map (juxt :region-id identity)) normal) variants)
                                        selected (assoc selected root-id bound-root)]
                                    (mapv #(selected (:region-id %)) normal)))]
                  (when (and schedules (repeatable? schedules))
                    (some
                      (fn [op]
                        (let [ids (get-in op [:step :conditional-union :branches])
                              producers (mapv (fn [id estimate]
                                                (let [patterns (get-in (by-id id) [:region :patterns])]
                                                  {:region-id id :group (by-alias (:source-symbol (first patterns)))
                                                   :patterns patterns :scan-rows (:rows estimate)}))
                                              ids (get-in op [:union-estimate :scan]))
                              producers (vec (sort-by :scan-rows producers))
                              plans (mapv #(window-plan! runtime % key column size) producers)]
                          (when (every? some? plans)
                            (let [token (control/reserve! (:control runtime) {:retained-bytes 4096})]
                              (rt/own! runtime #(control/release! token))
                              {:key key :column column :size size :limit (:qlimit (:parsed compiled))
                               :schedules schedules :producers (mapv #(assoc %1 :initial %2) producers plans)
                               :stats (atom {:windows 0 :qualified-keys 0 :fallback? false})}))))
                      (sort-by #(apply min (map :rows (get-in % [:union-estimate :scan]))) operators)))))))
          (sort keys))))))

(defn explanation [demand]
  (when demand
    (merge {:key (:key demand) :initial-keys (:size demand)
            :producers (mapv #(select-keys % [:region-id :scan-rows]) (:producers demand))}
           @(:stats demand))))

(defn run!
  "Read exact key windows, fully qualify each binding batch, and retain only
  final distinct rows. All work shares pinned sessions, permits and one ledger."
  [runtime demand parsed qualify! fallback!]
  (let [ctl (:control runtime) check! #(control/check-active! ctl)
        key (:key demand) vars (vec (parser/find-vars (:qfind parsed)))
        collector (window/collector! runtime parsed)
        sufficient? #(>= (count ((:rows collector))) (long (:limit demand)))
        initial-requests (long (:external-requests (control/usage ctl)))
        request-budget (min 2048 (max 8 (quot (long (:external-requests (control/limits ctl))) 8)))
        spent? #(or (>= (long (:windows @(:stats demand))) 8)
                    (>= (- (long (:external-requests (control/usage ctl))) initial-requests) request-budget))
        outcome
        (loop [size (long (:size demand)) pending (:producers demand)]
          (check!)
          (if (empty? pending) :complete
            (let [round
                  (reduce
                    (fn [remaining producer]
                      (check!)
                      (cond (sufficient?) (reduced :complete)
                            (spent?) (do (swap! (:stats demand) assoc :fallback-reason :probe-budget)
                                         (reduced :fallback))
                            :else
                        (let [status
                              (rt/with-workspace runtime
                                (fn [workspace]
                                  (if-let [plan (if (= size (:size demand)) (:initial producer)
                                                 (window-plan! workspace producer key (:column demand) size))]
                                    (let [rows (relational/rows! workspace plan)]
                                      (swap! (:stats demand) update :windows inc)
                                      (loop [batches (seq (partition-all (:size demand) rows))]
                                        (when (and batches (not (sufficient?)))
                                          (rt/with-workspace runtime
                                            (fn [batch-runtime]
                                              (let [batch (first batches)
                                                    token (control/reserve! ctl {:retained-bytes (+ 1024 (* 256 (count batch)))})
                                                    _ (rt/own! batch-runtime #(control/release! token))
                                                    input (FastList.)]
                                                (doseq [row batch] (.add input (object-array row)))
                                                (swap! (:stats demand) update :qualified-keys + (count batch))
                                                (let [result (qualify! (relation/relation! {key 0} input))
                                                      attrs (:attrs result) ^List tuples (:tuples result)]
                                                  (loop [i 0]
                                                    (when (and (< i (.size tuples)) (not (sufficient?)))
                                                      (check!)
                                                      (let [^objects tuple (.get tuples i)]
                                                        ((:add! collector) (mapv #(aget tuple (int (get attrs %))) vars)))
                                                      (recur (inc i))))))))
                                          (recur (next batches))))
                                      (if (< (count rows) size) :exhausted :more))
                                    :declined)))]
                          (cond (= status :declined) (do (swap! (:stats demand) assoc :fallback-reason :window-declined)
                                                        (reduced :fallback))
                                (= status :more) (conj remaining producer)
                                :else remaining)))) [] pending)]
              (cond (keyword? round) round
                    (sufficient?) :complete
                    (> size (quot Long/MAX_VALUE 2)) :fallback
                    :else (recur (* 2 size) round)))))]
    (if (= outcome :fallback)
      (do (swap! (:stats demand) assoc :fallback? true) (fallback!))
      (let [rows ((:rows collector)) token (control/reserve! ctl {:retained-bytes (+ 1024 (* 256 (count rows)))})
            _ (rt/own! runtime #(control/release! token)) tuples (FastList.)]
        (doseq [row rows] (.add tuples (object-array row)))
        (relation/relation! (zipmap vars (range)) tuples)))))
