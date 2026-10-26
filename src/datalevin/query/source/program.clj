(ns ^:no-doc datalevin.query.source.program
  "Compile captured v2 regions into shared, materialized plan steps. Scoped
  calls execute complete child regions once and join their projected results
  back to the original outer witnesses. No alternate Datalog interpreter."
  (:refer-clojure :exclude [run!])
  (:require [clojure.set :as set]
            [datalevin.db :as db]
            [datalevin.join :as join]
            [datalevin.parser :as parser]
            [datalevin.query.access :as query-access]
            [datalevin.query.access.dependency :as dependency]
            [datalevin.query.execute :as execute]
            [datalevin.query.execute.result :as result]
            [datalevin.query.plan :as plan]
            [datalevin.query.resolve :as resolve]
            [datalevin.query.source.access :as access]
            [datalevin.query.source.arithmetic :as arithmetic]
            [datalevin.query.source.collection :as collection]
            [datalevin.query.source.control :as control]
            [datalevin.query.source.context :as context]
            [datalevin.query.source.diagnostics :as diagnostics]
            [datalevin.query.source.input-program :as input-program]
            [datalevin.query.source.inputs :as inputs]
            [datalevin.query.source.native :as native]
            [datalevin.query.source.native-plan :as native-plan]
            [datalevin.query.source.native-rules :as native-rules]
            [datalevin.query.source.planning :as planning]
            [datalevin.query.source.region :as region]
            [datalevin.query.source.runtime :as rt]
            [datalevin.query.source.scope :as scope]
            [datalevin.query.source.session :as session]
            [datalevin.query.source.value :as value :refer [fail!]]
            [datalevin.relation :as relation]
            [datalevin.spill :as spill]
            [datalevin.timeout :as timeout])
  (:import [datalevin.parser Constant DefaultSrc FindColl FindRel FindScalar FindTuple Function Not Pattern Predicate RuleExpr SrcVar Variable]
           [datalevin.utl QueryKey QueryKey$KeySet]
           [java.util AbstractList List]
           [org.eclipse.collections.impl.list.mutable FastList]))

(defn- ensure! [ok] (when-not ok (fail! :query-source/invalid :execution)))
(defn- unsupported! [] (fail! :query-source/unsupported :planning))
(defn- fail-program! [runtime compilation error]
  (let [error (rt/redacted-error runtime (or (:phase (ex-data error)) :execution) error)]
    (control/cancel! (:control runtime) (select-keys (ex-data error) [:error :phase]))
    (try ((:close compilation))
         (catch Throwable secondary (.addSuppressed ^Throwable error secondary)))
    (throw error)))
(defn- columns-map [columns] (into {} (map (juxt :variable identity)) columns))
(defn- child-id [branch] (if (map? branch) (:region-id branch) branch))
(defn- canonical [variables check!]
  (vec (sort #(access/compare-keys %1 %2 check!) variables)))
(defn- column [v t nullable?] {:variable v :type t :nullable? nullable?})

(defn- merge-column [a b]
  (cond (nil? a) b
        (= (:type a) (:type b)) (update b :nullable? #(and % (:nullable? a)))
        (= :data (:type a)) b
        (= :data (:type b)) a
        :else (column (:variable b) :data (and (:nullable? a) (:nullable? b)))))

(defn- add-columns [schema columns]
  (reduce #(update %1 (:variable %2) merge-column %2) schema columns))

(defn- reserve! [runtime ^long rows bytes]
  (let [ctl (:control runtime) maximum (long (:retained-bytes (control/limits ctl)))
        attempted (value/add-charge (long (:retained-bytes (control/usage ctl))) bytes)]
    (control/check-active! ctl)
    (when (or (> rows Integer/MAX_VALUE) (not (value/at-most? attempted maximum)))
      (fail! :query-source/limit :reserve
             {:category :retained-bytes :maximum maximum :attempted attempted}))
    (control/reserve! ctl {:retained-bytes bytes})))

(defn- retain! [runtime token]
  (rt/own! runtime #(control/release! token)))

(defn- containers! [runtime ^long rows ^long width]
  (reserve! runtime rows
    (value/add-charge (+ 2048 (* 128 width))
      (value/multiply-charge rows (+ 192 (* 32 width))))))

(defn- checked-list [check! ^List tuples]
  (proxy [AbstractList] []
    (size [] (check!) (.size tuples))
    (get [i] (check!) (.get tuples i))))

(defn- joined! [runtime check! left right]
  (join/hash-join-bounded left right
    {:check! check! :reserve! #(reserve! runtime %1 %2)
     :retain! #(retain! runtime %) :release! control/release!}))

(defrecord ^:private StepFactory [in cols build]
  plan/IStepFactory
  (-instantiate-step [_ input-cols] (build input-cols)))

(defrecord ^:private MaterializedStep [in cols execute]
  plan/IStep
  (-type [_] :scoped)
  (-execute [_ _ tuples] (execute tuples))
  (-execute-pipe [_ _ _ _] (unsupported!))
  (-sample [_ _ _] (unsupported!))
  (-explain [_ _] "Shared scoped operation"))

(defn- collection-factory [runtime check! {:keys [columns relation] :as spec}]
  (let [variables (mapv :variable columns)]
    (->StepFactory #{} variables
      (fn [input-cols]
        (->MaterializedStep (set input-cols) (into input-cols (remove (set input-cols)) variables)
          (fn [tuples]
            (check!)
            (collection/check! runtime (:database spec))
            (:tuples (joined! runtime check!
                       (relation/relation! (zipmap input-cols (range)) tuples) relation))))))))

(defn- rule-factory [runtime check! node spec]
  (let [variables (mapv :variable (:columns spec))]
    (->StepFactory (set (map :variable (:requires spec))) variables
      (fn [input-cols]
        (let [_ (retain! runtime (containers! runtime 0 (+ (count input-cols) (count (:args node)))))
              positions (native-rules/select-inputs spec node input-cols)
              _ (reset! (:selected-inputs spec) positions)
              output-cols (into input-cols (remove (set input-cols)) variables)]
          (->MaterializedStep (set input-cols) output-cols
            (fn [^List tuples]
              (check!)
              (if (.isEmpty tuples) (FastList.)
                  (let [table (if-let [id (:rule-reference spec)]
                                (native-rules/reference! runtime (conj id positions) node input-cols tuples)
                                ((:solve! spec) positions input-cols tuples))
                        result (native-rules/call! runtime check! node table positions)
                        result (joined! runtime check!
                                 (relation/relation! (zipmap input-cols (range)) tuples) result)]
                    (ensure! (= output-cols (mapv key (sort-by val (:attrs result)))))
                    (:tuples result))))))))))

(defn- negation-factory [runtime check! {:keys [requires program]}]
  (let [variables (mapv :variable requires)]
    (->StepFactory (set variables) []
      (fn [input-cols]
        (->MaterializedStep (set input-cols) input-cols
          (fn [^List tuples]
            (check!)
            (if (.isEmpty tuples) (FastList.)
                (let [output-token (volatile! nil)]
                  (try
                    (let [result
                          (rt/with-workspace runtime [(:runtime program)]
                            (fn [workspace]
                              (retain! workspace
                                (reserve! workspace (.size tuples)
                                  (value/add-charge (+ 2048 (* 128 (count variables)))
                                    (value/multiply-charge (.size tuples)
                                      (+ 512 (* 64 (count variables)))))))
                              (let [outer (relation/relation! (zipmap input-cols (range)) tuples)
                                    input (relation/project-distinct (assoc outer :tuples (checked-list check! tuples)) variables)
                                    body ((:run! program) input)
                                    ;; A not-join local may have the same spelling
                                    ;; as an outer column. Only its declared join
                                    ;; variables participate in subtraction.
                                    bad (assoc body :attrs (select-keys (:attrs body) variables))]
                                (ensure! (or (.isEmpty ^List (:tuples body))
                                             (= (set variables) (set (keys (:attrs bad))))))
                                (join/subtract-rel-bounded outer bad
                                  {:check! check! :reserve! #(reserve! workspace %1 %2)
                                   ;; Transfer the output lease after the child
                                   ;; workspace releases its temporary resources.
                                   :retain! #(vreset! output-token %) :release! control/release!}))))]
                      (retain! runtime @output-token)
                      (:tuples result))
                    (catch Throwable e
                      (when-let [token @output-token] (control/release! token))
                      (throw e)))))))))))

(defn- pure-spec [node schema check!]
  (let [args (:args node)
        variables (into #{} (keep #(when (instance? Variable %) (:symbol %))) args)
        ;; A local predicate consumes bindings, including nulls, before a fact
        ;; can narrow their schema. Possible fact outputs are not its inputs.
        inputs (mapv #(column % :data true) (canonical variables check!))]
    (cond
      (instance? Predicate node)
      (do (when-not (and (#{'= 'not= '< '<= '> '>= 'nil? 'some?} (get-in node [:fn :symbol]))
                        (every? #(or (instance? Variable %) (instance? Constant %)) args))
            (unsupported!))
          {:requires inputs
           :columns (if (and (= 'some? (get-in node [:fn :symbol])) (= 1 (count args))
                             (instance? Variable (first args)))
                      [(column (:symbol (first args)) :data false)] [])})
      (instance? Function node)
      (let [f (get-in node [:fn :symbol]) arg (first args)
            variable (get-in node [:binding :variable :symbol])]
        (when-not (and (#{'identity 'ground} f) (= 1 (count args)) variable
                      (or (instance? Variable arg) (instance? Constant arg))) (unsupported!))
        {:requires (if (instance? Variable arg)
                     [(assoc (or (schema (:symbol arg)) (column (:symbol arg) :data true)) :nullable? true)] inputs)
         :columns [(if (instance? Constant arg)
                     (column variable (value/inferred-type (:value arg)) false)
                     (assoc (or (schema (:symbol arg)) (column variable :data true))
                            :variable variable :nullable? false))]})
      :else (unsupported!))))

(defn- pure-factory [runtime check! node spec literal-output]
  (let [exports (mapv :variable (:columns spec))]
    (->StepFactory (set (map :variable (:requires spec))) exports
      (fn [input-cols]
        (let [output-cols (into input-cols (remove (set input-cols)) exports)
              attrs (zipmap input-cols (range))
              form (parser/source node)
              temporary (when literal-output
                          (loop []
                            (check!)
                            (let [v (gensym "?literal_result__")]
                              (if (contains? attrs v) (recur) v))))
              forms (if temporary
                      ;; Keep function nil filtering and predicate equality in
                      ;; the existing resolver. The temporary never crosses
                      ;; this scalar step's public column layout.
                      [(assoc form 1 temporary) [(list '= literal-output temporary)]]
                      [form])]
          (->MaterializedStep (set input-cols) output-cols
            (fn [tuples]
              (check!)
              (let [^List tuples tuples
                    token (containers! runtime (if temporary (*' 2 (.size tuples)) (.size tuples))
                                       (+ (count output-cols) (if temporary 1 0)))
                    success? (volatile! false)]
                (try
                  ;; These admitted scalar operators cannot decode a source,
                  ;; expand a collection, or allocate arithmetic workspaces.
                  (let [context {:rels [(relation/relation! attrs (checked-list check! tuples))]
                                 :sources {} :rules {}}
                        resolved (reduce resolve/resolve-clause context forms)
                        result (reduce #(joined! runtime check! %1 %2) (:rels resolved))
                        ^List rows (:tuples result)
                        output (FastList. (.size rows))]
                    (dotimes [i (.size rows)]
                      (check!)
                      (let [^objects row (.get rows i)]
                        (.add output (object-array (mapv #(aget row (int (get (:attrs result) %))) output-cols)))))
                    (retain! runtime token)
                    (vreset! success? true)
                    output)
                  (finally (when-not @success? (control/release! token))))))))))))

(defn- branch-input! [runtime check! boundary branch input-cols ^List tuples]
  (let [columns (:inputs branch) width (count columns)
        token (containers! runtime (.size tuples) width)
        success? (volatile! false)]
    (try
      (let [seen (QueryKey$KeySet.) output (FastList.) attrs (zipmap input-cols (range))]
        (dotimes [i (.size tuples)]
          (check!)
          (let [^objects outer (.get tuples i)
                routed? (every? (fn [[v x]]
                                  (or (not (contains? attrs v))
                                      (QueryKey/equals x (aget outer (int (attrs v)))))) (:restore branch))]
            (when routed?
              (let [values
                    (mapv (fn [{:keys [variable type nullable?]}]
                            (check!)
                            (let [[tag actual] (get (:arguments branch) variable)
                                  source (if (= :rule (:kind boundary)) actual variable)
                                  x (if (contains? (:bindings branch) variable)
                                      (get (:bindings branch) variable)
                                      (do (ensure! (and (or (not= :rule (:kind boundary)) (= :var tag))
                                                       (contains? attrs source)))
                                          (aget outer (int (attrs source)))))]
                              (ensure! (if (nil? x) nullable? (value/matches-type? type x check!))) x)) columns)]
                (when (.add seen values) (.add output (object-array values)))))))
        (retain! runtime token)
        (vreset! success? true)
        (relation/relation! (zipmap (map :variable columns) (range)) output))
      (finally (when-not @success? (control/release! token))))))

(defn- scope-factory [runtime check! boundary run-region]
  (->StepFactory (set (map :variable (:inputs boundary))) (mapv :variable (:columns boundary))
    (fn [input-cols]
      (let [output-cols (into input-cols (remove (set input-cols)) (map :variable (:columns boundary)))]
        (->MaterializedStep (set input-cols) output-cols
          (fn [tuples]
            (check!)
            (scope/check! runtime boundary)
            (let [results (mapv (fn [branch]
                                  (let [input (branch-input! runtime check! boundary branch input-cols tuples)]
                                    {:region-id (:region-id branch)
                                     :relation (run-region (:region-id branch) input)}))
                                (filterv (complement :empty?) (:branches boundary)))
                  result (scope/finish! runtime boundary results)
                  outer (relation/relation! (zipmap input-cols (range)) tuples)
                  result (joined! runtime check! outer result)]
              (ensure! (= output-cols (mapv key (sort-by val (:attrs result)))))
              (:tuples result))))))))

(defn- schemas [compiled native-specs root-columns check!]
  (reduce
    (fn [schemas r]
      (let [id (:region-id r)
            initial (if (= id (:root-region compiled)) (columns-map root-columns)
                        (into {} (map #(vector % (column % :data true))) (:input-vars r)))
            fact-columns (concat (mapcat :columns (:patterns r))
                                 (mapcat #(get-in native-specs [(:path %) :columns]) (:core-clauses r)))
            produced (set (map :variable fact-columns))
            initial (add-columns initial fact-columns)
            ;; A known constraint does not change the type of a body producer.
            ;; Its compatibility is checked at the captured scope boundary.
            initial (reduce-kv (fn [schema v x]
                                 (if (contains? produced v) schema
                                     (assoc schema v (column v (value/inferred-type x) (nil? x)))))
                               initial (:bindings r))
            initial
            (reduce
              (fn [schema site]
                (let [branches
                      (mapv (fn [branch]
                              (let [body (get schemas (child-id branch))]
                                (if (= :rule (:kind site))
                                  (reduce (fn [m [v [tag actual]]]
                                            (if (= :var tag)
                                              (update m actual merge-column
                                                      (assoc (or (body v) (column v :data true)) :variable actual)) m))
                                          {} (:arguments branch))
                                  (add-columns body
                                    (map (fn [[v _]] (column v :keyword false))
                                         (get-in compiled [:regions (child-id branch) :patterns 0 :restore]))))))
                            (filter #(get-in compiled [:regions (child-id %) :live?]) (:branches site)))
                      outputs
                      (mapv (fn [v]
                              (let [cs (mapv #(or (get % v) (get schema v) (column v :data true)) branches)]
                                (column v (if (and (seq cs) (apply = (map :type cs))) (:type (first cs)) :data)
                                        (boolean (some :nullable? cs))))) (canonical (:exports site) check!))]
                  (add-columns schema outputs))) initial
              (filter #(= id (:parent-region %)) (:sites compiled)))
            [final _ constraints]
            (reduce (fn [[schema produced constraints] {:keys [node path]}]
                      (check!)
                      (let [columns (:columns (if (or (instance? Pattern node) (instance? RuleExpr node) (instance? Not node)) (get native-specs path)
                                                  (pure-spec node schema check!)))]
                        [(reduce (fn [schema column]
                                   (let [v (:variable column)]
                                     (cond (contains? (:literal-vars r) v) schema
                                           (and (contains? (:bindings r) v) (not (contains? produced v)))
                                           (assoc schema v column)
                                           :else (add-columns schema [column])))) schema columns)
                         (into produced (map :variable) columns)
                         (into constraints (filter #(contains? (:literal-vars r) (:variable %))) columns)]))
                    [initial produced (vec (mapcat #(get-in native-specs [(:path %) :constant-constraints])
                                                  (:core-clauses r)))] (:core-clauses r))]
        (assoc schemas id (with-meta final {:constant-constraints constraints})))) {} (reverse (:regions compiled))))

(defn- branch-output [id schema]
  {:region-id id :columns (vec (vals schema))
   :constant-constraints (:constant-constraints (meta schema))})

(defn- core-seeds!
  "Prove caller columns independently of a scoped child's outputs. Ready
  native/local producers and scalar steps can run before the scoped call;
  possible mapped outputs and sibling scope exports cannot seed themselves."
  [runtime check! compiled id native-specs inputs]
  (if-not (some #(and (= id (:parent-region %)) (seq (:branches %))) (:sites compiled)) inputs
    (let [region (nth (:regions compiled) id)
          nodes (:core-clauses region) width (count (:available-vars region))]
      ;; The returned column vector survives the temporary proof workspace.
      ;; Keep its container lease with the compilation until cleanup.
      (retain! runtime (containers! runtime 1 width))
      (rt/with-workspace runtime
        (fn [workspace]
          ;; Bound all retained proof maps/queues before iterative propagation.
          (retain! workspace (containers! workspace (inc (count nodes)) (+ width (count nodes))))
          (loop [known (columns-map inputs) pending nodes]
            (check!)
            (let [[columns pending progressed?]
                  (reduce
                    (fn [[known pending progressed?] {:keys [node path] :as entry}]
                      (check!)
                      (let [spec (if (or (instance? Pattern node) (instance? RuleExpr node) (instance? Not node))
                                   (get native-specs path) (pure-spec node known check!))
                            extended (when (and spec (every? #(dependency/supplies? (known (:variable %)) %) (:requires spec)))
                                       (dependency/extend-columns known (:columns spec)))]
                        (if extended [extended pending true]
                            [known (conj pending entry) progressed?])))
                    [known [] false] pending)]
              (if (and progressed? (seq pending)) (recur columns pending)
                  (mapv columns (canonical (keys columns) check!))))))))))

(defn- scope-required-vars [site region-inputs]
  (if (= :rule (:kind site))
    (into #{} (mapcat (fn [branch]
                       (keep (fn [v]
                               (let [[tag name] (get (:arguments branch) v)]
                                 (when (= :var tag) name)))
                         (set/union (:required-inputs branch) (get region-inputs (child-id branch))))))
      (:branches site))
    (into (set (:required-inputs site)) (mapcat #(get region-inputs (child-id %))) (:branches site))))

(defn- region-inputs!
  "Carry body dependencies through scoped calls. Local producers can satisfy
  them; a possible caller binding becomes required only when the body needs it."
  [runtime check! compiled native-specs all-schemas]
  (reduce
    (fn [inputs r]
      (check!)
      (assoc inputs (:region-id r)
        (rt/with-workspace runtime
          (fn [workspace]
            (let [sites (filter #(= (:region-id r) (:parent-region %)) (:sites compiled))
                  _ (retain! workspace (containers! workspace
                                         (+ (count sites) (count (:core-clauses r)) (count (:patterns r)))
                                         (count (:available-vars r))))
                  operations
                  (into (mapv (fn [{:keys [node path]}]
                                (check!)
                                (let [spec (if (or (instance? Pattern node) (instance? RuleExpr node) (instance? Not node))
                                             (get native-specs path)
                                             (pure-spec node (get all-schemas (:region-id r)) check!))]
                                  {:requires (set (map :variable (:requires spec)))
                                   :columns (set (map :variable (:columns spec)))})) (:core-clauses r))
                    (concat (map #(hash-map :requires #{} :columns (set (map :variable (:columns %)))) (:patterns r))
                            (map #(hash-map :requires (scope-required-vars % inputs) :columns (:exports %)) sites)))]
              (loop [required #{}]
                (check!)
                (let [[known pending]
                      (loop [known (into required (keys (:bindings r))) pending operations]
                        (check!)
                        (if-let [ready (some #(when (set/subset? (:requires %) known) %) pending)]
                          (recur (into known (:columns ready)) (filterv #(not (identical? ready %)) pending))
                          [known pending]))
                      missing (set/difference (into #{} (mapcat :requires) pending) known)
                      roots (set/difference missing (into #{} (mapcat :columns) pending))
                      next (into required (set/intersection (:input-vars r) (if (seq roots) roots missing)))]
                  (if (= required next) required (recur next)))))))))
    {} (reverse (:regions compiled))))

(defn- with-region-inputs! [runtime check! compiled native-specs all-schemas f]
  ;; Only scoped calls consume these dependencies. Their map is planning
  ;; scratch: no inferred input map survives in an executable operator.
  (if-not (some #(seq (:branches %)) (:sites compiled))
    (f {})
    (let [token (containers! runtime (count (:regions compiled))
                  (reduce + 0 (map #(count (:input-vars %)) (:regions compiled))))]
      (try (f (region-inputs! runtime check! compiled native-specs all-schemas))
           (finally (control/release! token))))))

(defn- scope-parent-columns [site schema known-inputs child-schemas region-inputs check!]
  (let [actual (columns-map known-inputs)
        required (scope-required-vars site region-inputs)
        needed
        (if (= :rule (:kind site))
          (set/union required (set/intersection (:exports site) (set (keys actual))))
          (set/union required
                     (set/intersection (:exports site) (set (map :variable known-inputs)))
                     (into #{} (mapcat #(set/difference (:exports site) (set (keys (get child-schemas (child-id %))))))
                           (:branches site))))]
    ;; Required inputs may wait for a preceding fact or explicit null filter.
    ;; An optional caller seed cannot require its own future body output type.
    (mapv #(or (when (contains? required %) (schema %)) (actual %) (schema %) (column % :data true))
      (canonical needed check!))))

(defrecord ^:private Program [runtime compilation planned root input regions state runner])

(defn- operator-description [compiled native-specs region-id paths index {:keys [kind value]} inputs outputs]
  (let [path (get paths (:id value))
        node (:node (some #(when (= path (:path %)) %) (get-in compiled [:regions region-id :core-clauses])))
        site (some #(when (= path (:path %)) %) (:sites compiled))
        options (get-in value [:path :options])]
    (cond-> {:operator-id index :kind (if (= :access kind) :access
                                       (cond (:collection? (get native-specs path)) :collection
                                             (instance? Pattern node) :native
                                             (instance? RuleExpr node) :rule
                                             (instance? Not node) :negation
                                             (instance? Function node) :function
                                             (instance? Predicate node) :predicate
                                             :else (:kind site)))
             :inputs inputs :outputs outputs}
      (= :core kind) (assoc :core-id (:id value) :path path)
      (= :access kind) (assoc :source (:source-symbol options) :alternative-id (:alternative-id options)
                             :covers (:covers options))
      (or (instance? Function node) (instance? Predicate node)) (assoc :function (get-in node [:fn :symbol])))))

(defn- terminal-query! [compiled]
  (let [parsed (:parsed compiled) constants (get-in compiled [:regions (:root-region compiled) :bindings])]
    (when (or (seq (:qorder parsed)) (seq (:qhaving parsed))) (unsupported!))
    (doseq [element (parser/find-elements (:qfind parsed))]
      (when-not (or (instance? Variable element)
                    (and (parser/aggregate? element)
                         (#{'count 'sum 'avg 'min 'max} (get-in element [:fn :symbol]))
                         (= 1 (count (:args element))) (instance? Variable (first (:args element)))))
        (unsupported!)))
    (when (and (not (instance? FindRel (:qfind parsed))) (or (:qlimit parsed) (:qoffset parsed)))
      (unsupported!))
    (reduce (fn [parsed key]
              (let [x (get parsed key) x (if (symbol? x) (get constants x ::missing) x)]
                (ensure! (or (nil? x) (= -1 x)
                             (and (integer? x) (<= (if (= key :qlimit) 1 0) x Long/MAX_VALUE))))
                (assoc parsed key x))) parsed [:qlimit :qoffset])))

(defn- input! [runtime check! compiled input-columns input-tuples]
  (let [root (:root-region compiled) constants (get-in compiled [:regions root :bindings])
        supplied (columns-map input-columns)
        columns (into input-columns
                  (map (fn [v] (column v (value/inferred-type (get constants v)) (nil? (get constants v)))))
                  (canonical (set/difference (set (keys constants)) (set (keys supplied))) check!))
        _ (ensure! (set/subset? (set (map :variable columns))
                               (get-in compiled [:regions root :input-vars])))
        tuples (FastList.)
        source (if (nil? input-tuples) (do (ensure! (empty? input-columns)) [[]]) input-tuples)
        input-vars (mapv :variable input-columns) read-item! (inputs/item-reader source)]
    (loop []
      (check!)
      (when-let [[tuple] (read-item!)]
        (retain! runtime (containers! runtime 1 (count columns)))
        (let [row (inputs/tuple-row (:control runtime) input-vars tuple)
              values (mapv (fn [{:keys [variable type nullable?]}]
                             (let [x (if (contains? row variable) (get row variable) (get constants variable))]
                               (ensure! (if (nil? x) nullable? (value/matches-type? type x check!))) x)) columns)]
          (retain! runtime (rt/charge-copy! runtime values))
          (.add tuples (object-array (value/owned-copy values check!))))
        (recur)))
    {:columns columns :relation (relation/relation! (zipmap (map :variable columns) (range)) tuples)}))

(defn- boundaries! [runtime check! compiled native-specs root-columns]
  (let [all-schemas (schemas compiled native-specs root-columns check!)
        boundaries (volatile! [])]
    (with-region-inputs! runtime check! compiled native-specs all-schemas
      (fn [region-inputs]
        (letfn [(region! [id inputs]
                  (check!)
                  (let [schema (add-columns (get all-schemas id) inputs)
                        seeds (core-seeds! runtime check! compiled id native-specs inputs)]
                    (doseq [[index site] (map-indexed vector (:sites compiled))
                            :when (and (= id (:parent-region site)) (seq (:branches site)))]
                      (let [parent (scope-parent-columns site schema seeds all-schemas region-inputs check!)
                            outputs (mapv (fn [b] (branch-output (child-id b) (get all-schemas (child-id b)))) (:branches site))
                            boundary (scope/interfaces! runtime compiled index parent outputs)]
                        (vswap! boundaries conj boundary)
                        (doseq [branch (:branches boundary) :when (not (:empty? branch))]
                          (region! (:region-id branch) (:inputs branch)))))))]
          (when (get-in compiled [:regions (:root-region compiled) :live?])
            (region! (:root-region compiled) root-columns))
          @boundaries)))))

(defn- prune-empty-collections! [runtime compilation specs]
  ;; Unlike a provider estimate, a complete owned local match is an exact
  ;; no-fact proof for every possible upstream binding. Propagate it before
  ;; typed dependency selection, including through native rule/union scopes.
  (let [compiled (:compiled compilation)
        empty-ids (into #{}
                    (keep (fn [r]
                            (when (some (fn [{:keys [path]}]
                                          (let [spec (get specs path)]
                                            (and (:collection? spec)
                                                 (.isEmpty ^List (get-in spec [:relation :tuples])))))
                                        (:core-clauses r)) (:region-id r)))) (:regions compiled))]
    (if (empty? empty-ids) compilation
        (do
          (retain! runtime (containers! runtime (count (:regions compiled)) 4))
          (planning/refine! runtime
            (assoc compilation :compiled
                   (update compiled :regions
                     #(mapv (fn [r] (cond-> r (contains? empty-ids (:region-id r)) (assoc :empty? true))) %)))
            [])))))

(defn- compile-program!
  "Compile captured regions and real typed input tuples into executable shared
  steps. This is the revised internal compiler, not a caller-supplied interface
  fixture. Scalar predicates/functions and positive rule/union/attribute sites
  use existing engine operators. Native negation compiles child bodies and
  uses shared bounded subtraction. Unsupported core operators reject before
  acquisition. Local collections use controlled shared pattern emission.
  Native initializations use the existing planner and the explicit controlled
  storage boundary within the caller's pinned views."
  [runtime compilation input-columns input-tuples snapshots prepared-input]
  (ensure! (and (identical? (:control runtime) (get-in compilation [:runtime :control]))
                (= (:query-id runtime) (get-in compilation [:runtime :query-id]))))
  (let [runtime (:runtime compilation) compiled (:compiled compilation)
        root (:root-region compiled) runner (atom nil) state (atom :compiling)
        program-id (diagnostics/program! runtime)
        check! #(do (control/check-active! (:control runtime))
                    (ensure! (not= :closed @state)))
        run-region (fn [id input] (check!) (ensure! @runner) (@runner id input))
        _ (rt/own! runtime #(do (reset! state :closed) (reset! runner nil)))
        _ (terminal-query! compiled)
        entries (into {} (map (juxt :symbol identity)) (:entries compilation))
        compile-body!
        (fn [parsed context body-rules bindings columns]
          (let [body (planning/capture! runtime (filterv :native (:entries compilation)) parsed body-rules
                       bindings (set (map :variable columns)) nil context)
                program (compile-program! runtime body columns (when (seq columns) []) snapshots nil)]
            {:runtime (:runtime program)
             :compiled (get-in program [:compilation :compiled]) :core-specs (:core-specs program)
             :run! (fn [seeds]
                     (check!)
                     (ensure! (= :ready @(:state program)))
                     (if (get (:regions program) (:root program))
                       (@(:runner program) (:root program) (or seeds (:input program)))
                       (relation/relation! {} (FastList.))))}))
        native-specs
        (into {} (for [r (:regions compiled) {:keys [node path literal-terms]} (:core-clauses r)
                       :when (or (instance? Pattern node) (instance? RuleExpr node) (instance? Not node))
                       :let [source (:source node)
                             source (if (or (nil? source) (instance? DefaultSrc source))
                                      (:default-source r) (:symbol source))
                             database (:native (get entries source))]]
                   [path (assoc (cond
                                  (instance? Not node)
                                  (let [variables (canonical (::region/negation-inputs (meta node)) check!)
                                        columns (mapv #(column % :data true) variables)
                                        parsed (parser/map->Query
                                                 {:qfind (parser/->FindRel (mapv parser/->Variable variables))
                                                  :qin [] :qwhere (:clauses node)})
                                        context {:default-source source :rule-relations (or (:rule-relations compiled) {})
                                                 :literal-vars (::region/negation-literals (meta node))}]
                                    {:requires columns :columns [] :negation? true
                                     :program (compile-body! parsed context (:rules compiled)
                                                (select-keys (:bindings r) variables) columns)})
                                  (instance? RuleExpr node)
                                  (or (native-rules/reference-spec compiled node source)
                                      (native-rules/prepare! runtime (:entries compilation) (:rules compiled) node source
                                        (fn [pattern source]
                                          (let [database (:native (get entries source))]
                                            (if (collection/owned? database) #{}
                                              (into #{} (map :variable)
                                                (:requires (native-plan/spec! runtime snapshots database pattern []))))))
                                        (fn [parsed context body-rules columns]
                                          (compile-body! parsed context body-rules {} columns))))
                                  :else
                                  (if (collection/owned? database)
                                    (collection/spec! runtime database node)
                                    (native-plan/spec! runtime snapshots database node literal-terms)))
                                :database database :source-symbol source :region-id (:region-id r)
                                :selected-inputs (when (instance? RuleExpr node) (atom nil)))]))
        compilation (prune-empty-collections! runtime compilation native-specs)
        compiled (:compiled compilation)
        input (or prepared-input (input! runtime check! compiled input-columns input-tuples))
        ;; An ordinary invocation's complete owned input is an exact empty
        ;; proof, even when an empty column has no narrow type. Body templates
        ;; use placeholder rows during compilation and receive real seeds later.
        compilation (if (and prepared-input (.isEmpty ^List (get-in input [:relation :tuples])))
                      (planning/refine! runtime
                        (update-in compilation [:compiled :regions root] assoc :empty? true) [])
                      compilation)
        compiled (:compiled compilation)
        root-columns (:columns input)
        proofs (boundaries! runtime check! compiled native-specs root-columns)
        compilation (planning/refine! runtime compilation proofs)
        _ (doseq [proof proofs] ((:close proof)))
        tables (volatile! nil)
        planned
          (planning/plan! runtime compilation
            (fn [runtime compiled]
                (let [all-schemas (schemas compiled native-specs root-columns check!)
                      interfaces (atom {})]
                  (with-region-inputs! runtime check! compiled native-specs all-schemas
                    (fn [region-inputs]
                      (letfn [(region! [id inputs]
                                (check!)
                                (let [r (nth (:regions compiled) id)
                                      schema (add-columns (get all-schemas id) inputs)
                                      seeds (core-seeds! runtime check! compiled id native-specs inputs)
                                      core (volatile! {}) operators (volatile! [])
                                      add! (fn [path requires columns step]
                                             (let [op-id (count @operators)
                                                   op (access/core-operator! runtime id op-id requires columns #{} step)]
                                               (vswap! core assoc path op-id) (vswap! operators conj op)))]
                                  (doseq [{:keys [node path]} (:core-clauses r)]
                                    (let [native? (or (instance? Pattern node) (instance? RuleExpr node) (instance? Not node))
                                          spec (if native? (get native-specs path) (pure-spec node schema check!))
                                          output (get-in node [:binding :variable :symbol])
                                          literal-output (when (contains? (:literal-vars r) output) output)
                                          spec (if literal-output
                                                 (-> spec
                                                     (update :requires #(conj (filterv (fn [c] (not= literal-output (:variable c))) %)
                                                                               (column literal-output :data true)))
                                                     (assoc :columns [])) spec)
                                          factory (cond (:negation? spec) (negation-factory runtime check! spec)
                                                        (:collection? spec) (collection-factory runtime check! spec)
                                                        (or (:rule-reference spec) (:native-rule-program spec))
                                                        (rule-factory runtime check! node spec)
                                                        native? (native-plan/factory! runtime snapshots (:database spec) node spec
                                                                  (:core-clauses r) (:bindings r) check! #(joined! runtime check! %1 %2))
                                                        :else (pure-factory runtime check! node spec literal-output))]
                                      (add! path (:requires spec) (:columns spec) factory)))
                                  (doseq [[index site] (map-indexed vector (:sites compiled))
                                          :when (= id (:parent-region site))]
                                    (let [parent (scope-parent-columns site schema seeds all-schemas region-inputs check!)
                                          branch-outputs
                                          (mapv (fn [b]
                                                  (let [id (child-id b)]
                                                    (branch-output id (get all-schemas id)))) (:branches site))
                                          boundary (scope/interfaces! runtime compiled index parent branch-outputs)]
                                      (doseq [branch (:branches boundary) :when (not (:empty? branch))]
                                        (region! (:region-id branch) (:inputs branch)))
                                      (add! (:path site) (:inputs boundary) (:columns boundary)
                                            (scope-factory runtime check! boundary run-region))))
                                  (swap! interfaces assoc id {:inputs inputs :operators @operators :core @core})))]
                        (when (:live? (nth (:regions compiled) root)) (region! root root-columns))
                        (vreset! tables @interfaces)
                        @interfaces))))))
          schedules (planning/schedules runtime planned)
          _ (when (:diagnostics runtime)
              (let [groups (:groups planned)
                    descriptions (session/explain! runtime groups)
                    entries (into {} (map (juxt :symbol identity)) (:entries compilation))]
                (diagnostics/sources! runtime
                  (mapv (fn [description]
                          (let [descriptor (:descriptor (entries (:source description)))]
                            (assoc description :definition-id (:definition-id descriptor)
                              :definition-fingerprint (:definition-fingerprint descriptor)
                              :scope-id (get-in descriptor [:scope :id])))) descriptions))))
          executable
          (into {}
                (map (fn [schedule]
                       (let [id (:region-id schedule) inputs (get-in @tables [id :inputs])
                             paths (when (:diagnostics runtime) (into {} (map (fn [[path id]] [id path])) (get-in @tables [id :core])))
                             [cols steps descriptions]
                             (reduce (fn [[cols steps descriptions] [index {:keys [kind value] :as operator}]]
                                       (let [step (if (= :access kind)
                                                    (plan/access-step (:expr value) (:path value)
                                                      (query-access/complete-demand :exact nil)
                                                      (:bounds value) (:work value) cols)
                                                    (let [step (:step value)]
                                                      (if (satisfies? plan/IStepFactory step)
                                                        (plan/-instantiate-step step cols) step)))]
                                         [(:cols step) (conj steps step)
                                          (cond-> descriptions (:diagnostics runtime)
                                            (conj (operator-description compiled native-specs id paths index operator cols (:cols step))))]))
                                     [(mapv :variable inputs) [] []] (map-indexed vector (:operators schedule)))
                             _ (when (:diagnostics runtime)
                                 (diagnostics/region! runtime (assoc (access/explain! runtime schedule)
                                                               :program-id program-id :operators descriptions)))]
                         [id {:inputs (mapv :variable inputs) :cols cols :steps steps}]))) schedules)]
      (reset! runner
        (fn [id input]
          (check!)
          (let [{:keys [inputs cols steps]} (get executable id)]
            (ensure! (and inputs (= inputs (mapv key (sort-by val (:attrs input))))))
            (let [tuples (loop [tuples (:tuples input) index 0]
                           (check!)
                           (if (= index (count steps)) tuples
                               (let [started (when (get-in runtime [:diagnostics :intermediate?]) (System/nanoTime))
                                     result (plan/-execute (nth steps index) nil tuples)]
                                 (check!)
                                 (when started
                                   (diagnostics/operator! runtime program-id id index
                                     (.size ^List tuples) (.size ^List result) (- (System/nanoTime) (long started))))
                                 (recur result (inc index)))))]
              (relation/relation! (zipmap cols (range)) tuples)))))
      (reset! state :ready)
      (assoc (->Program runtime compilation planned root (:relation input) executable state runner)
             :program-id program-id
             :core-specs (into {} (map (fn [[path spec]]
                                        [path (if-let [selected (:selected-inputs spec)]
                                                (-> spec (dissoc :selected-inputs)
                                                    (assoc :selected-input-positions @selected)) spec)])) native-specs))))

(defn compile!
  "Compile a captured region graph and typed input rows into shared steps.
  All failed construction, including final step assembly, unwinds immediately."
  ([runtime compilation input-columns input-tuples]
   (compile! runtime compilation input-columns input-tuples {}))
  ([runtime compilation input-columns input-tuples snapshots]
   (ensure! (and (identical? (:control runtime) (get-in compilation [:runtime :control]))
                 (= (:query-id runtime) (get-in compilation [:runtime :query-id]))))
   (try (compile-program! runtime compilation input-columns input-tuples snapshots nil)
        (catch Throwable e (fail-program! (:runtime compilation) compilation e)))))

(defn execute!
  "Execute a compiled region graph once, with no replay of its external paths.
  Return an owned internal Relation; public result shaping runs before cleanup."
  [runtime program]
  (ensure! (and (instance? Program program)
                (identical? (:control runtime) (get-in program [:runtime :control]))
                (= (:query-id runtime) (get-in program [:runtime :query-id]))
                (= :planned @(get-in program [:compilation :state]))
                (compare-and-set! (:state program) :ready :running)))
  (control/check-active! (:control runtime))
  (try
    (if (get (:regions program) (:root program))
      (@(:runner program) (:root program) (:input program))
      (relation/relation! {} (FastList.)))
    (catch Throwable e (fail-program! (:runtime program) (:compilation program) e))
    (finally (compare-and-set! (:state program) :running :finished))))

(defn result!
  "Complete a compiled query through the shared collector, aggregates, windows
  and return-shape processor. Realize all lazy work and own public values before
  the invocation closes. No external rows or session plans enter native caches."
  [runtime program]
  (ensure! (and (instance? Program program)
                (identical? (:control runtime) (get-in program [:runtime :control]))
                (= (:query-id runtime) (get-in program [:runtime :query-id]))))
  (try
   (let [rel (execute! runtime program) runtime (:runtime program)
        parsed (terminal-query! (get-in program [:compilation :compiled]))
        find (:qfind parsed)
        all-vars (into (vec (parser/find-vars find)) (map :symbol) (:qwith parsed))
        check! #(control/check-active! (:control runtime))
        ^List tuples (:tuples rel) n (.size tuples) width (count all-vars)
        ;; Projection, set indexes, group keys/lists, return maps and window
        ;; vectors are all sized before invoking their shared constructors.
        token (reserve! runtime n
                (value/add-charge (+ 8192 (* 512 width))
                  (value/multiply-charge n (+ 4096 (* 128 width)))))
        _ (retain! runtime token)
        collected (spill/new-spillable-set nil {:spill-threshold Long/MAX_VALUE :initial-capacity n})
        context {:rels [(assoc rel :tuples (checked-list check! tuples))] :sources {} :rules {}}
        _ (doseq [^objects tuple (result/-collect context all-vars)]
            (check!) (.cons ^clojure.lang.IPersistentCollection collected (vec tuple)))
        context (assoc context :result-set collected :terminal-collected-symbols all-vars)
        scalar-row? (volatile! false)
        post-process
        (fn [find return-map tuples]
          (check!)
          (when (instance? FindScalar find) (vreset! scalar-row? (boolean (seq tuples))))
          (cond
            (and return-map (instance? FindRel find))
            (result/tuples->return-map return-map tuples {:spill-threshold Long/MAX_VALUE})
            (and return-map (instance? FindTuple find))
            (first (result/tuples->return-map return-map [(first tuples)] {:spill-threshold Long/MAX_VALUE}))
            :else (execute/-post-process find return-map tuples)))
        process (execute/result-processor parsed
                  {:check! check! :wrap-aggregate #(arithmetic/wrap-aggregate runtime %1 %2)
                   :post-process post-process})
        raw (process parsed context)
        own-row! (fn [row]
                   (check!)
                   (retain! runtime (rt/charge-result! runtime row))
                   (retain! runtime (rt/charge-copy! runtime row))
                   (value/owned-copy row check!))]
    (check!)
    (cond
      (instance? FindScalar find) (if @scalar-row? (first (own-row! [raw])) nil)
      (instance? FindTuple find) (when (some? raw) (own-row! raw))
      (instance? FindColl find) (mapv #(first (own-row! [%])) raw)
      (set? raw) (into (spill/new-spillable-set nil {:spill-threshold Long/MAX_VALUE :initial-capacity n})
                      (map own-row!) raw)
      :else (mapv own-row! raw)))
   (catch Throwable e (fail-program! (:runtime program) (:compilation program) e))))

(defn run!
  "Compile and finish a captured query inside native read views pinned for the
  entire invocation. No borrowed native context may escape this callback."
  [runtime compilation input-columns input-tuples]
  (ensure! (and (identical? (:control runtime) (get-in compilation [:runtime :control]))
                (= (:query-id runtime) (get-in compilation [:runtime :query-id]))))
  (try
    (native/with-views (filter db/db? (keep :native (:entries compilation)))
      (fn [snapshots]
        (result! runtime (compile! runtime compilation input-columns input-tuples snapshots))))
    (catch Throwable e (fail-program! (:runtime compilation) compilation e))))

(defn- invoke-query!
  [runtime query arguments options explain-options]
  (let [child (assoc runtime :cleanup (atom [])) primary (volatile! nil)
        active (volatile! child)
        close (rt/own! runtime
                #(let [held (reverse @(:cleanup child)) error (volatile! nil)]
                   (reset! (:cleanup child) [])
                   (doseq [f held]
                     (try (f) (catch Throwable e
                                (if-let [first-error @error]
                                  (.addSuppressed ^Throwable first-error e)
                                  (vreset! error e)))))
                   (when-let [e @error] (throw e))))]
    (try
      (let [{:keys [runtime options deadline]} (context/open! child query options)]
        (vreset! active runtime)
        (binding [timeout/*deadline* deadline]
          (rt/with-runtime-context runtime
            (fn [child]
              (let [check! #(control/check-active! (:control child))
                    _ (retain! child (rt/charge-metadata! child options :context))
                    _ (retain! child (rt/charge-copy! child options))
                    options (value/owned-copy options check!)
                    _ (retain! child (rt/charge-copy! child query))
                    parsed (input-program/parse! parser/parse-query (value/owned-copy query check!))
                    _ (session/validate-options! child options
                        (into #{} (keep #(when (instance? SrcVar (:variable %)) (get-in % [:variable :symbol]))) (:qin parsed)))
                    _ (when (some? explain-options)
                        (retain! child (rt/charge-metadata! child explain-options :explain))
                        (when-not (value/plain-map? explain-options) (fail! :query-source/invalid :explain))
                        (when-not (every? #{:run? :intermediate-counts?} (keys explain-options))
                          (fail! :query-source/unsupported :explain))
                        (when-not (every? boolean? (vals explain-options)) (fail! :query-source/invalid :explain)))
                    execute? (or (nil? explain-options) (get explain-options :run? false))
                    child (if (nil? explain-options) child
                              (assoc child :diagnostics (diagnostics/start! child (get explain-options :intermediate-counts? true))))]
                ;; Nested native and source queries see this invocation's
                ;; control, including the diagnostic owner, during callbacks.
                (binding [rt/*runtime* child]
                  (let [input (input-program/prepare! child parsed arguments #(joined! child check! %1 %2))
                        compilation (planning/capture! child (:entries input) (:parsed input) (:rules input)
                                      (:bindings input) (set (map :variable (:columns input))) options)]
                    (native/with-views (filter db/db? (keep :native (:entries compilation)))
                      (fn [snapshots]
                        (let [program (compile-program! child compilation nil nil snapshots (select-keys input [:columns :relation]))
                              result (when execute? (result! child program))]
                          (if (nil? explain-options) result
                              (diagnostics/finish! child execute? (:result-rows (control/usage (:control child)))
                                {:storage :memory :spill-policy :reject :replay-spools :none
                                 :empty-input? (.isEmpty ^List (get-in program [:input :tuples]))
                                 :join-indexes :bounded-hash :distinct :find-and-with
                                 :grouping (boolean (some parser/aggregate? (parser/find-elements (:qfind parsed))))
                                 :result :retained}))))))))))))
      (catch Throwable e
        (let [e (rt/active-error @active e)
              error (rt/redacted-error @active (or (:phase (ex-data e)) :inputs) e)]
          (vreset! primary error)
          (control/cancel! (:control child) (select-keys (ex-data error) [:error :phase]))
          (throw error)))
      (finally
        (try (close)
             (catch Throwable e
               (if-let [error @primary]
                 (.addSuppressed ^Throwable error e)
                 (let [error (rt/redacted-error child :close e)
                       cancelled (try (control/check-active! (:control child)) nil
                                      (catch Throwable active
                                        (rt/redacted-error child (or (:phase (ex-data active)) :execution) active)))]
                   (throw (if cancelled (doto ^Throwable cancelled (.addSuppressed error)) error))))))))))

(defn query!
  "Execute ordinary query syntax and :in arguments through the v2
  compiler. Inputs are owned before descriptor callbacks, and all preparation,
  compilation and session resources close before the owned result returns.
  The public driver supplies invocation options and deadlines."
  ([runtime query arguments] (query! runtime query arguments nil))
  ([runtime query arguments options] (invoke-query! runtime query arguments options nil)))

(defn explain!
  "Compile a v2 query and return an owned, redacted explanation after cleanup.
  Plan-only mode acquires authorized sessions and selects complete accesses
  without opening execution cursors. Executed mode keeps metrics separate from
  optional intermediate counts; public explain uses this same boundary."
  ([runtime query arguments explain-options] (explain! runtime query arguments explain-options nil))
  ([runtime query arguments explain-options options]
   (invoke-query! runtime query arguments options (if (nil? explain-options) {} explain-options))))
