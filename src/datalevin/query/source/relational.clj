(ns ^:no-doc datalevin.query.source.relational
  "Optional whole-conjunction delegation with ordinary owned cursor batches."
  (:refer-clojure :exclude [run!])
  (:require [clojure.set :as set]
            [datalevin.parser :as parser]
            [datalevin.query.execute :as execute]
            [datalevin.query.source :as source]
            [datalevin.query.source.batch :as batch]
            [datalevin.query.source.contract :as contract]
            [datalevin.query.source.control :as control]
            [datalevin.query.source.diagnostics :as diagnostics]
            [datalevin.query.source.extrema :as extrema]
            [datalevin.query.source.permissions :as permissions]
            [datalevin.query.source.region :as region]
            [datalevin.query.source.result-window :as window]
            [datalevin.query.source.runtime :as rt]
            [datalevin.query.source.session :as session]
            [datalevin.query.source.value :as value :refer [fail!]]
            [datalevin.spill :as spill])
  (:import [datalevin.parser Aggregate FindColl FindRel FindScalar FindTuple Pattern Predicate Variable]
           [java.io Closeable]
           [java.util List]))

(defn- ensure! [ok] (when-not ok (fail! :query-source/invalid :planning)))
(defn- retain! [runtime token] (rt/own! runtime #(control/release! token)))

(defn projection [element]
  (cond
    (instance? Variable element) {:kind :variable :variable (:symbol element)}
    (and (instance? Aggregate element) (= 1 (count (:args element)))
         (instance? Variable (first (:args element)))
         (#{'count 'count-distinct 'sum 'min 'max} (get-in element [:fn :symbol])))
    {:kind :aggregate :operator (keyword (name (get-in element [:fn :symbol])))
     :variable (:symbol (first (:args element)))}))

(defn- whole-predicates
  "Flatten a completely represented scalar conjunction for SQL. Chained long
  comparisons normalize only here; the ordinary scoped compiler is unchanged."
  [root variables types]
  (let [base (inc (long (reduce max -1 (map (comp :id :clause) (:patterns root)))))
        binary (into {} (map (juxt :id identity)) (region/predicate-clauses root variables))
        clauses (map-indexed
                  (fn [i {:keys [node]}]
                    (if-let [predicate (get binary (+ base (long i)))] [predicate]
                      (let [args (:args node)
                            operator ({'< :< '<= :<= '> :> '>= :>=} (get-in node [:fn :symbol]))
                            operand (fn [arg]
                                      (cond (instance? Variable arg)
                                            (when (and (variables (:symbol arg)) (= :long (types (:symbol arg))))
                                              (if (contains? (:bindings root) (:symbol arg))
                                                [:const (get-in root [:bindings (:symbol arg)])] [:var (:symbol arg)]))
                                            (and (instance? datalevin.parser.Constant arg) (int? (:value arg)))
                                            [:const (:value arg)]))
                            terms (when (and (instance? Predicate node) operator (<= 3 (count args) 32))
                                    (mapv operand args))]
                        (when (and (seq terms) (every? some? terms))
                          (mapv (fn [pair] {:kind :predicate :operator operator :semantics-version 1 :args (vec pair)})
                                (partition 2 1 terms)))))) (:core-clauses root))]
    (when (every? some? clauses) (vec (mapcat identity clauses)))))

(defn- absence-clause [compiled sources patterns site]
  (when (= :negation (:kind site))
    (let [child (get-in compiled [:regions (first (:branches site))])
          [pattern] (:patterns child)
          {:keys [terms] :as clause} (:clause pattern)
          [entity attribute val] terms
          source (:source-symbol pattern)]
      (when (and (= 1 (count (:patterns child))) (empty? (:core-clauses child))
                 (= :const (first attribute))
                 (= :var (first entity)) (contains? (:exports site) (second entity))
                 (or (= :ignore (first val))
                     (and (= :var (first val)) (not (contains? (:exports site) (second val)))))
                 (true? (:datalevin.query.source/absence-relational? (sources source)))
                 ;; A reference alone does not establish the FROM row exists.
                 (some #(and (= source (:source-symbol %)) (= entity (first (:terms %)))) patterns))
        (assoc clause :kind :absence :source-symbol source :terms [entity attribute [:ignore]])))))

(defn- optional-projections
  "Recognize a complete bound fact-or-absence union. Keep every other scoped
  operator in core; a nullable column is never an independent row witness."
  [compiled sources patterns]
  (let [sites (:sites compiled) by-parent (group-by :parent-region sites)
        regions (:regions compiled) root (:root-region compiled)]
    (into []
      (keep (fn [site]
              (when (and (= :union (:kind site)) (= root (:parent-region site))
                         (= 2 (count (:branches site))))
                (let [branches (mapv #(nth regions %) (:branches site))
                      present (first (filter #(= 1 (count (:patterns %))) branches))
                      absent (first (remove #(identical? present %) branches))
                      pattern (first (:patterns present))
                      [entity attribute val] (get-in pattern [:clause :terms])
                      [etag e] entity [vtag v] val source (:source-symbol pattern)
                      negative (first (get by-parent (:region-id absent)))
                      body (get regions (first (:branches negative)))
                      probe (first (:patterns body))
                      ground (get-in absent [:core-clauses 0 :node])]
                  (when (and (= :var etag) (= :var vtag) (not= e v)
                             (= :const (first attribute))
                             (= #{e v} (:exports site)) (= #{e} (:required-inputs site))
                             (empty? (:core-clauses present))
                             (empty? (get by-parent (:region-id present)))
                             (empty? (:patterns absent)) (= 1 (count (:core-clauses absent)))
                             (instance? datalevin.parser.Function ground)
                             (= 'ground (get-in ground [:fn :symbol])) (= 1 (count (:args ground)))
                             (instance? datalevin.parser.Constant (first (:args ground)))
                             (= [nil] (:value (first (:args ground))))
                             (instance? datalevin.parser.BindTuple (:binding ground))
                             (= 1 (count (get-in ground [:binding :bindings])))
                             (= v (get-in ground [:binding :bindings 0 :variable :symbol]))
                             (= 1 (count (get by-parent (:region-id absent))))
                             (= :negation (:kind negative)) (= #{e} (:exports negative))
                             (= 1 (count (:patterns body))) (empty? (:core-clauses body))
                             (empty? (get by-parent (:region-id body)))
                             (= source (:source-symbol probe))
                             (= [entity attribute [:ignore]] (get-in probe [:clause :terms]))
                             (true? (:datalevin.query.source/nullable-projections-relational? (sources source)))
                             (= :one (get-in (sources source) [:logical-schema :attributes (second attribute) :cardinality]))
                             (not-any? #(contains? (contract/variables %) v) patterns)
                             (some #(and (= source (:source-symbol %)) (= entity (first (:terms %)))) patterns))
                    {:clause (assoc (:clause pattern) :kind :optional :source-symbol source)
                     :sites #{(:path site) (:path negative)} :variable v}))))) sites)))

(defn- request [compilation input]
  (let [{:keys [entries compiled prepared]} compilation
        parsed (:parsed compiled) [root] (:regions compiled)
        elements (mapv projection (parser/find-elements (:qfind parsed)))
        sources (into {} (map (juxt :symbol :descriptor)) entries)
        native-extrema (:native-extrema prepared)
        sites (:sites compiled)
        patterns (mapv #(assoc (:clause %) :source-symbol (:source-symbol %)) (:patterns root))
        optional (optional-projections compiled sources patterns)
        consumed (into #{} (mapcat :sites) optional)
        sites (filterv #(not (contains? consumed (:path %))) sites)
        optionals (mapv :clause optional)
        nullable (into #{} (map :variable) optional)
        ignore-nil (:ignore-nil-extrema prepared)
        unions (filterv #(= :union (:kind %)) sites)
        negatives (filterv #(= :negation (:kind %)) sites)
        absences (mapv #(absence-clause compiled sources patterns %) negatives)
        pure-unions? (and (seq unions)
                          (every? #(= 1 (count (:exports %))) unions)
                          (= (count unions) (count (:source-prefilters root)))
                          (every? #(contains? (:datalevin.query.source/predicate-prefilters (:descriptor %)) :like-any) entries))
        flat? (or (and (= 1 (count (:regions compiled))) (empty? sites))
                  (and (every? #(= (:region-id root) (:parent-region %)) sites)
                       (= (count sites) (+ (count unions) (count negatives)))
                       (or (empty? unions) pure-unions?)
                       (every? some? absences)))
        variables (into #{} (mapcat contract/variables) (into patterns optionals))
        groups (set (map #(vector (:source-symbol %) (first (:terms %))) patterns))
        types (reduce (fn [out {:keys [source-symbol terms] :as clause}]
                        (reduce (fn [out [term column]]
                                  (if (= :var (first term))
                                    (update out (second term) #(if (or (nil? %) (= % (:type column)))
                                                               (:type column) ::mixed)) out))
                                out (map vector terms (contract/pattern-columns (sources source-symbol) clause))))
                      {} (into patterns optionals))]
    (when (and (= 1 (count (:groups prepared))) (every? :descriptor entries)
               flat?
               (or (empty? native-extrema)
                   (and (empty? (:qorder parsed)) (nil? (:qlimit parsed)) (nil? (:qoffset parsed))
                        (every? #(true? (:datalevin.query.source/native-extrema-relational? (:descriptor %))) entries)))
               (set/subset? ignore-nil (into #{} (keep #(when (#{:min :max} (:operator %)) (:variable %))) elements))
               (every? #(or (not (nullable (:variable %))) (= :variable (:kind %))
                            (and (#{:min :max} (:operator %)) (contains? ignore-nil (:variable %)))) elements)
               (empty? (:rules compiled)) (empty? (:qhaving parsed))
               (not (and (:qreturn-map parsed) (seq (:qorder parsed))))
               (nil? *math-context*)
               (every? #(or (instance? Pattern %) (instance? Predicate %)
                            (and (or pure-unions? (seq optional)) (instance? datalevin.parser.Or %))
                            (and (seq absences) (instance? datalevin.parser.Not %))) (:qwhere parsed))
               (= 1 (.size ^List (get-in input [:relation :tuples])))
               (set/subset? (set (map :variable (:columns input))) (set (keys (:bindings input))))
               (seq patterns) (every? some? elements)
               (every? variables (concat (map :variable elements) (map :symbol (:qwith parsed))))
               (not-any? #{::mixed} (vals types))
               (or (> (count groups) 1) (seq absences) (seq optionals) (some #(= :aggregate (:kind %)) elements)
                   (window/specification parsed)))
      ;; Every scalar operator must be represented. A function, ternary
      ;; predicate, scoped body, or unknown operator keeps the core path.
      (when-let [predicates (whole-predicates root variables types)]
        ;; SQL NULL comparisons need separate semantics from local predicates.
        (when-not (some #(seq (set/intersection nullable (contract/variables %)))
                        (into predicates (when pure-unions? (:source-prefilters root))))
          (let [predicates (mapv (fn [i predicate] (assoc predicate :id i))
                                (range (inc (long (reduce max -1 (map :id patterns)))) Long/MAX_VALUE)
                                (into (into (into predicates absences) optionals) (when pure-unions? (:source-prefilters root))))]
            (merge (:request (first (:groups prepared)))
                   (when (seq native-extrema) {:datalevin.query.source/native-extrema native-extrema})
                   (when (seq ignore-nil) {:datalevin.query.source/ignore-nil-extrema ignore-nil})
                   {:mode :relational :semantics-version 1
                    :sources (mapv #(select-keys % [:symbol :descriptor]) entries)
                    :clauses (into patterns predicates)
                    :bindings (:bindings input)
                    :find elements :with (mapv :symbol (:qwith parsed))
                    :columns (mapv (fn [i {:keys [kind operator variable]}]
                                     {:variable (symbol (str "?relational_result_" i))
                                      :type (if (= kind :variable) (types variable)
                                                (if (#{:count :count-distinct} operator) :long :data))
                                      :nullable? (boolean (or (#{:min :max} operator)
                                                              (and (= kind :variable) (nullable variable))))}) (range) elements)})))))))

(defn plan!
  "Try a pure, owned relational plan before ordinary acquisition. Declining
  incurs no reads and leaves the existing fact-access compiler untouched."
  [runtime compilation input]
  (let [provider (get-in compilation [:entries 0 :source])]
    (when (or (satisfies? source/IRelationalQuerySource provider)
              (satisfies? source/IRelationalWindowSource provider))
      (when-let [request (request compilation input)]
        (let [request-token (rt/charge-copy! runtime (dissoc request :control))
              _ (retain! runtime request-token)
              window (window/specification (get-in compilation [:compiled :parsed]))
              _ (when window (retain! runtime (rt/charge-copy! runtime window)))
              bounded (when (and window (satisfies? source/IRelationalWindowSource provider))
                        (rt/invoke runtime :planning
                          #(source/-plan-relational-window provider (assoc request :mode :relational-window :window window))))
              request (if bounded (assoc request :mode :relational-window :window window) request)
              response (or bounded (when (satisfies? source/IRelationalQuerySource provider)
                                     (rt/invoke runtime :planning #(source/-plan-relational provider request))))]
          (when-not response (control/release! request-token))
          (when response
          (ensure! (and (value/plain-map? response) (= #{:plan :columns} (set (keys response)))
                        (value/plain-map? (:plan response)) (vector? (:columns response))
                        (= (:columns request) (:columns response))))
          (let [borrowed (control/reserve! (:control runtime) {:retained-bytes (value/value-charge response)})
                response (try
                           (retain! runtime (rt/charge-copy! runtime response))
                           (value/owned-copy response #(control/check-active! (:control runtime)))
                           (finally (control/release! borrowed)))
                groups (session/acquire! runtime (:prepared compilation))
                group (first groups)
                _ (when (seq (get-in compilation [:prepared :native-extrema]))
                    (extrema/prepare! runtime compilation groups))]
            (ensure! (satisfies? source/IRelationalQuerySession (:session group)))
            (when (:diagnostics runtime)
              (diagnostics/sources! runtime
                (mapv (fn [description]
                        (let [descriptor (:descriptor (first (filter #(= (:symbol %) (:source description))
                                                                     (:entries compilation))))]
                          (assoc description :definition-id (:definition-id descriptor)
                                             :definition-fingerprint (:definition-fingerprint descriptor)
                                             :scope-id (get-in descriptor [:scope :id]))))
                      (session/explain! runtime groups)))
              (diagnostics/region! runtime
                {:program-id 0 :region-id 0 :policy :relational-v1 :selection :provider
                 :steps [{:source-symbol (get-in compilation [:entries 0 :symbol])
                          :alternative-id "relational" :covers (set (map :id (:clauses request)))
                          :access {:kind :relational} :columns (:columns request)}]
                 :operators [] :result-window (when bounded window)}))
            {:group group :response response :request request
             :parsed (cond-> (get-in input [:parsed]) bounded (assoc :qoffset 0))})))))))

(defn- result-query [parsed columns]
  (let [elements (mapv #(parser/->Variable (:variable %)) columns)
        positions (reduce (fn [m [i v]] (if (contains? m v) m (assoc m v i)))
                          {} (map-indexed vector (parser/find-vars (:qfind parsed))))
        find (:qfind parsed)
        find (if (or (instance? FindRel find) (instance? FindTuple find))
               (assoc find :elements elements) (assoc find :element (first elements)))]
    (assoc parsed :qfind find :qwith nil :qhaving nil
                  :qorder (when (:qorder parsed)
                            (mapv #(if (symbol? %) (positions %) %) (:qorder parsed))))))

(defn- shape-result!
  "Process collected rows and charge and detach each public result. Staging
  reservations belong to the caller that collected the rows."
  [runtime query vars collected]
  (let [check! #(control/check-active! (:control runtime))
        raw ((execute/result-processor query {:check! check!}) query {:result-set collected :terminal-collected-symbols vars})
        own! (fn [row]
               (check!) (retain! runtime (rt/charge-result! runtime row))
               (retain! runtime (rt/charge-copy! runtime row)) (value/owned-copy row check!))
        find (:qfind query)]
    (check!)
    (cond
      (instance? FindScalar find) (when (seq collected) (first (own! [raw])))
      (instance? FindTuple find) (when (some? raw) (own! raw))
      (instance? FindColl find) (mapv #(first (own! [%])) raw)
      (set? raw) (into (spill/new-spillable-set nil {:spill-threshold Long/MAX_VALUE}) (map own!) raw)
      :else (mapv own! raw))))

(defn finish!
  "Shape already collected relational rows. Aggregated rows use synthetic find
  variables; non-aggregate fragments retain the original find/with projection."
  [runtime parsed columns collected aggregated?]
  (retain! runtime (control/reserve! (:control runtime)
                     {:retained-bytes (+ 8192 (* 512 (count columns))
                                         (* (count collected) (+ 4096 (* 128 (count columns)))))}))
  (let [query (if aggregated? (result-query parsed columns) parsed)
        vars (if aggregated? (mapv :variable columns)
                 (into (vec (parser/find-vars (:qfind parsed))) (map :symbol) (:qwith parsed)))]
    (shape-result! runtime query vars collected)))

(defn run!
  "Consume an exact relational cursor, enforce batch/result quotas, and use
  the shared result processor for public shapes, maps, and result windows."
  [runtime {:keys [group response request parsed]}]
  (let [ctl (:control runtime) check! #(control/check-active! ctl)
        columns (:columns response) vars (mapv :variable columns)
        descriptor (get-in request [:sources 0 :descriptor])
        open (merge (:context group)
                    {:source-symbol (get-in request [:sources 0 :symbol]) :region-id 0
                     :scope-id (get-in descriptor [:scope :id]) :alternative-id "relational"
                     :definition-fingerprint (:definition-fingerprint descriptor)
                     :plan (:plan response) :columns columns})
        alternative {:id "relational" :scope-id (:scope-id open)
                     :properties {:replay :identical :ordering [] :blocking-state :none}}
        trace (diagnostics/cursor! runtime open false)
        permission (permissions/reserve-open! (:consumption group) alternative open)
        close-permission (rt/own! runtime #(.close ^Closeable permission))
        primary (volatile! nil) close-cursor (volatile! nil)]
    (try
      (let [cursor (rt/invoke runtime :open #(source/-open-relational-cursor (:session group) open))]
        (ensure! (satisfies? source/IQuerySourceCursor cursor))
        (vreset! close-cursor (rt/own! runtime #(source/-close-cursor! cursor)))
        (let [bounded (window/collector! runtime (result-query parsed columns))
              collected (when-not bounded (spill/new-spillable-set nil {:spill-threshold Long/MAX_VALUE}))
              seen (volatile! 0)
              fetch {:control ctl :max-rows (get-in request [:limits :batch-rows])
                     :max-bytes (get-in request [:limits :batch-bytes])}]
          (retain! runtime (control/reserve! ctl {:retained-bytes 4096}))
          (loop []
            (let [batch (batch/accept! runtime columns false fetch
                          (rt/invoke runtime :fetch #(source/-next-batch! cursor fetch)) identity)]
              (try
                (doseq [row (:rows batch)]
                  (check!)
                  (when (= :relational-window (:mode request))
                    (let [n (inc (long @seen))]
                      (vreset! seen n)
                      (when (> n (long (get-in request [:window :limit])))
                        (fail! :query-source/invalid :fetch))))
                  (if bounded ((:add! bounded) row)
                    (do (retain! runtime (rt/charge-copy! runtime row))
                        (retain! runtime (control/reserve! ctl {:retained-bytes 128}))
                        (.cons ^clojure.lang.IPersistentCollection collected (value/owned-copy row check!)))))
                (diagnostics/batch! runtime trace (:row-count batch) (:byte-count batch))
                (finally (control/release! (:reservation batch))))
              (when-not (:exhausted? batch) (recur))))
          (when (:diagnostics runtime)
            (diagnostics/metrics! runtime trace (rt/invoke runtime :metrics #(source/-cursor-metrics cursor))))
          (let [query (result-query parsed columns)
                collected (if bounded ((:rows bounded)) collected)]
            (shape-result! runtime query vars collected))))
      (catch Throwable e (vreset! primary e) (throw e))
      (finally
        (try
          (when-let [close @close-cursor] (close))
          (catch Throwable e (if-let [error @primary] (.addSuppressed ^Throwable error e) (throw e)))
          (finally (close-permission)))))))
