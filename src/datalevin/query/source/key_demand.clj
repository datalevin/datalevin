(ns ^:no-doc datalevin.query.source.key-demand
  "Bounded mandatory-input progress for small unordered DISTINCT windows.
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

(defn- eligible? [compiled root]
  (let [parsed (:parsed compiled) limit (:qlimit parsed)]
    (and (instance? FindRel (:qfind parsed))
         (every? #(instance? Variable %) (parser/find-elements (:qfind parsed)))
         (seq (parser/find-elements (:qfind parsed)))
         (= (count (parser/find-elements (:qfind parsed))) (count (set (parser/find-vars (:qfind parsed)))))
         (empty? (:qwith parsed)) (empty? (:qhaving parsed)) (empty? (:qorder parsed))
         (nil? (:qreturn-map parsed)) (= 0 (or (:qoffset parsed) 0))
         (integer? limit) (<= 1 limit 512) (empty? (:rules compiled))
         (empty? (:inputs root))
         ;; The scalar work must distribute over batches of outer rows.
         (every? (fn [{:keys [core-clauses]}]
                   (every? (fn [{:keys [node]}]
                             (let [f (get-in node [:fn :symbol]) arg (first (:args node))]
                               (and (instance? Function node) (= 1 (count (:args node)))
                                    (#{'ground 'identity} f)
                                    (or (instance? Constant arg)
                                        (and (= 'identity f) (instance? Variable arg)))))) core-clauses))
                 (:regions compiled)))))

(defn- repeatable? [schedules]
  (every? #(= :identical (get-in % [:path :options :capture :alternative :properties :replay]))
          (mapcat :plans schedules)))

(defn- window-plan! [runtime {:keys [group clauses]} columns size]
  (let [vars (mapv :variable columns)
        parsed (parser/map->Query {:qfind (parser/->FindRel (mapv parser/->Variable vars))
                                  :qwith [] :qlimit size :qoffset 0})
        request (merge (:request group)
                  {:mode :relational-window :semantics-version 1
                   :sources (mapv #(select-keys % [:symbol :descriptor]) (:entries group))
                   :clauses clauses :bindings {}
                   :find (mapv (fn [v] {:kind :variable :variable v}) vars) :with []
                   :columns (mapv (fn [i column]
                                    (assoc column :variable (symbol (str "?key_demand_result_" i))))
                                  (range) columns)
                   :window {:limit size :offset 0 :order []}})]
    (relational/window-plan! runtime group request parsed)))

(defn- demand! [runtime columns size limit schedules producers plans]
  (let [token (control/reserve! (:control runtime) {:retained-bytes 4096})]
    (rt/own! runtime #(control/release! token))
    {:columns columns :size size :limit limit :schedules schedules
     :producers (mapv #(assoc %1 :initial %2) producers plans)
     :stats (atom {:windows 0 :qualified-keys 0 :fallback? false})}))

(defn- optional-schedule [f]
  (try (f)
       (catch clojure.lang.ExceptionInfo e
         (when-not (and (= :query-source/unsupported (:error (ex-data e)))
                        (= :planning (:phase (ex-data e))))
           (throw e)))))

(defn- union-demand! [runtime work normal root changed planning groups limit]
  (when (and (<= (long limit) 256) (empty? (get-in root [:region :patterns]))
             (<= 2 (count changed) 8)
             (= (count changed) (count (filter #(get-in % [:step :conditional-union]) (:operators root)))))
    (let [operators (sort-by :id (vals changed))
          keys (apply set/intersection (map #(set (map :variable (get-in % [:step :conditional-union :columns]))) operators))
          by-id (into {} (map #(vector (get-in % [:region :region-id]) %)) work)
          by-alias (into {} (mapcat (fn [g] (map #(vector % g) (:aliases g)))) groups)
          size (max (long limit)
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
                                                 :clauses (mapv #(assoc (:clause %) :source-symbol (:source-symbol %)) patterns)
                                                 :scan-rows (:rows estimate)}))
                                            ids (get-in op [:union-estimate :scan]))
                            producers (vec (sort-by :scan-rows producers))
                            plans (mapv #(window-plan! runtime % [column] size) producers)]
                        (when (every? some? plans)
                          (demand! runtime [column] size limit schedules producers plans))))
                    (sort-by #(apply min (map :rows (get-in % [:union-estimate :scan]))) operators)))))))
        (sort keys)))))

(defn- access-demand! [runtime normal root groups compiled planning limit]
  (let [root-id (get-in root [:region :region-id])
        schedule (some #(when (= root-id (:region-id %)) %) normal)
        [head & tail] (:operators schedule)
        anchor (:value head)
        {:keys [source-symbol columns requires capture]} (get-in anchor [:path :options])
        {:keys [request alternative]} capture
        group (some #(when (some #{source-symbol} (:aliases %)) %) groups)
        bound-unions (filter #(seq (get-in % [:value :binding-inputs])) tail)]
    ;; Keep the original schedule after its first access. Every remaining
    ;; provider access must consume bindings and every scope must be a bound
    ;; flat positive union. The complete anchor tuple fixes all downstream
    ;; correlations, even when the unions need different keys.
    (when (and (= :access (:kind head)) (seq columns) (empty? requires) group
               (empty? (:bindings request)) (empty? (:prefilters alternative))
               (seq bound-unions) (= (count bound-unions) (count (:sites compiled)))
               (every? #(seq (get-in % [:path :options :requires]))
                       (concat (rest (:plans schedule))
                               (mapcat :plans (remove #(= root-id (:region-id %)) normal)))))
      (let [size (min 256 (long (:binding-rows (control/limits (:control runtime)))))
            scan-rows (:rows (access/repeatable-estimate runtime anchor planning))
            clauses (into [] (comp (filter #(contains? (:covers alternative) (:id %)))
                                   (map #(assoc % :source-symbol source-symbol))) (:clauses request))]
        (when (and scan-rows (< size (double scan-rows))
                   (seq clauses) (every? #(= :pattern (:kind %)) clauses))
          (let [producer {:region-id root-id :group group :clauses clauses :scan-rows scan-rows}]
            (when-let [plan (window-plan! runtime producer columns size)]
              (let [suffix (-> schedule
                               (assoc :input-columns columns :operators (vec tail)
                                      :plans (subvec (:plans schedule) 1))
                               (dissoc :join-cost :estimated-access-ns :cost-search :union-selection))]
                (demand! runtime columns size limit
                         (mapv #(if (= root-id (:region-id %)) suffix %) normal)
                         [producer] [plan])))))))))

(defn select!
  "Select bounded input windows with a complete captured fallback schedule."
  [runtime work normal changed planning {:keys [compiled groups]}]
  (let [root (some #(when (empty? (get-in % [:region :path])) %) work)
        limit (get-in compiled [:parsed :qlimit])]
    (when (and compiled (eligible? compiled root)
               (every? #(and (= :snapshot (:actual-consistency %))
                             (permissions/repeatable-default? (:consumption %))) groups)
               (repeatable? normal))
      (or (union-demand! runtime work normal root changed planning groups limit)
          (access-demand! runtime normal root groups compiled planning limit)))))

(defn explanation [demand]
  (when demand
    (let [vars (mapv :variable (:columns demand))]
      (merge (if (= 1 (count vars)) {:key (first vars)} {:keys vars})
             {:initial-keys (:size demand)
              :producers (mapv #(select-keys % [:region-id :scan-rows]) (:producers demand))}
             @(:stats demand)))))

(defn run!
  "Read exact key windows, fully qualify each binding batch, and retain only
  final distinct rows. All work shares pinned sessions, permits and one ledger."
  [runtime demand parsed qualify! fallback!]
  (let [ctl (:control runtime) check! #(control/check-active! ctl)
        columns (:columns demand) input-attrs (zipmap (map :variable columns) (range))
        vars (vec (parser/find-vars (:qfind parsed)))
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
                                                 (window-plan! workspace producer columns size))]
                                    (let [rows (relational/rows! workspace plan)]
                                      (swap! (:stats demand) update :windows inc)
                                      (loop [batches (seq (partition-all (:size demand) rows))]
                                        (when (and batches (not (sufficient?)))
                                          (rt/with-workspace runtime
                                            (fn [batch-runtime]
                                              (let [batch (first batches)
                                                    token (control/reserve! ctl
                                                            {:retained-bytes (+ 1024 (* (+ 256 (* 16 (count columns)))
                                                                                       (count batch)))})
                                                    _ (rt/own! batch-runtime #(control/release! token))
                                                    input (FastList.)]
                                                (doseq [row batch] (.add input (object-array row)))
                                                (swap! (:stats demand) update :qualified-keys + (count batch))
                                                (let [result (qualify! (relation/relation! input-attrs input))
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
