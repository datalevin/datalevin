;; Copyright (c) Huahai Yang. All rights reserved.
;; Distributed under the Eclipse Public License 2.0.
(ns ^:no-doc datalevin.db.tx.template
  "Immutable parameterized write programs and pure, execution-local binding."
  (:require [datalevin.db.tx.compile :as compile]
            [datalevin.db.tx.plan :as plan])
  (:import [datalevin.db.tx.plan AttrInfo BlindSource MixedInsert ScalarSequence TxParam WriteOp]
           [java.util ArrayList HashMap HashSet]
           [org.eclipse.collections.impl.list.mutable FastList]))

(defn parameter [key]
  (when-not (keyword? key)
    (throw (ex-info "Transaction parameter names must be keywords"
                    {:error :prepared/parameter-name})))
  (TxParam. key))

(defn compatible?
  [captured current]
  (and (identical? (:schema captured) (:schema current))
       (identical? (:opts captured) (:opts current))))

(defn- plain-map? [value]
  (or (identical? clojure.lang.PersistentArrayMap (class value))
      (identical? clojure.lang.PersistentHashMap (class value))))

(defn- literal-vector? [value]
  (or (identical? clojure.lang.PersistentVector (class value))
      (identical? clojure.lang.APersistentVector$SubVector (class value))))

(defn- expression
  "Compile input substitution once. Literal objects are returned unchanged;
  only containers containing parameter markers are rebuilt during binding."
  [value]
  (cond
    (instance? TxParam value)
    (let [key (.-key ^TxParam value)] {:run #(get % key) :params [key]})

    (or (identical? clojure.lang.PersistentHashSet (class value))
        (identical? clojure.lang.PersistentTreeSet (class value)))
    (let [children (mapv expression value)]
      (when (some #(seq (:params %)) children)
        (throw (ex-info "Bind a parameterized set as one parameter value"
                        {:error :prepared/parameter-container})))
      {:run (constantly value) :params []})

    (or (plain-map? value) (literal-vector? value))
    (let [map? (plain-map? value)
          _ (when (and map? (not-every? compile/prepared-literal? (keys value)))
              (throw (ex-info "Prepared definition map keys must be literal scalars"
                              {:error :prepared/definition-key})))
          children (if map?
                     (into {} (map (fn [[k v]] [k (expression v)])) value)
                     (mapv expression value))
          params (vec (distinct (mapcat :params (if map? (vals children) children))))]
      {:params params :children children
       :run (if (empty? params)
              (constantly value)
              (let [metadata (meta value)]
                (fn [input]
                  (with-meta
                    (cond
                      map? (reduce-kv (fn [out k child] (assoc out k ((:run child) input))) (empty value) children)
                      :else (mapv #((:run %) input) children))
                    metadata))))})

    :else {:run (constantly value) :params []}))

(defn prepare-operation
  "Compile an operation's constants and schema metadata. Compatible operations
  are shared by identity across templates. Parameter values are never retained."
  [ctx source]
  (if (and (instance? WriteOp source) (compatible? (:ctx source) ctx))
    source
    (let [source (if (instance? WriteOp source) (:source source) source)
          _ (when-not (or (plain-map? source) (literal-vector? source))
              (throw (ex-info "A prepared transaction operation must be a map or vector"
                              {:error :prepared/operation-shape})))
          compiled (expression source)
          children (:children compiled)
          map? (plain-map? source)
          add? (and (vector? source) (<= 4 (count source) 5) (identical? :db/add (nth source 0)))
          cas? (and (vector? source) (= 5 (count source)) (#{:db.fn/cas :db/cas} (nth source 0)))
          retract? (and (vector? source) (<= 3 (count source) 4) (identical? :db/retract (nth source 0)))
          clear? (and (vector? source) (= 3 (count source)) (identical? :db.fn/retractAttribute (nth source 0)))
          ordered? (or cas? retract? clear?)
          kind (cond
                 map? (if (or (not (contains? source :db/id))
                              (instance? TxParam (:db/id source))
                              (compile/prepared-tempid-key (:db/id source)))
                        :blind
                        (if (or (compile/prepared-positive-id? (:db/id source))
                                (vector? (:db/id source))) :scalar :dynamic))
                 add? :scalar
                 ordered? :scalar-program
                 :else :dynamic)
          infos (HashMap.)
          fields (cond
                   map? (mapv (fn [[attr _]]
                                [attr (:run (get children attr))
                                 (compile/prepared-write-attribute ctx infos attr kind)])
                              (remove #(identical? :db/id (key %)) source))
                   add? [[(nth source 2) (:run (nth children 3))
                          (compile/prepared-write-attribute ctx infos (nth source 2) kind)]]
                   ordered? [[(nth source 2) (if cas? (:run (nth children 4)) (constantly nil))
                              (let [^AttrInfo info (compile/prepared-write-attribute ctx infos (nth source 2) :scalar-program)]
                                (when (and info (.-typed? info)) info))]]
                   :else [])
          mixed-form? (and (nil? (meta source))
                           (or (and map? (> (count source) (if (contains? source :db/id) 1 0)))
                               (and add? (= 4 (count source)))))
          mixed-fields (when mixed-form?
                         (mapv (fn [[a value _]]
                                 [a value (compile/prepared-write-attribute ctx infos a :mixed)]) fields))
          mixed? (and mixed-form? (every? #(some? (nth % 2)) mixed-fields))
          program-fields (if (or map? add?)
                           (mapv (fn [[a value _]]
                                   [a value (compile/prepared-write-attribute ctx infos a :scalar-program)]) fields)
                           fields)
          references? (some (fn [[_ _ ^AttrInfo info]]
                              (and info (identical? :db.type/ref (.-value-type info)))) program-fields)
          program? (and (or map? add? ordered?) (seq program-fields)
                        (compile/prepared-write-metadata? (meta source))
                        ;; Reference-add preprocessing can discard stamps and
                        ;; annotations. Keep that checkpoint on the raw route.
                        (or (not references?) (not add?)
                            (and (= 4 (count source)) (nil? (meta source))))
                        (every? (fn [[_ _ ^AttrInfo info]] (and info (.-typed? info))) program-fields))
          kind (if (and (not= :dynamic kind)
                        (every? #(some? (nth % 2)) fields)
                        (compile/prepared-write-metadata? (meta source))) kind :dynamic)]
      (assoc (plan/->WriteOp ctx source (:run compiled) (:params compiled) kind
                            (if map? (or (:run (get children :db/id)) (constantly nil))
                                (when (or add? ordered?) (:run (nth children 1))))
                            fields (when (and add? (= 5 (count source))) (:run (nth children 4)))
                            (when (or add? cas?) (meta source)))
             :opcode (cond cas? :cas retract? :retract clear? :clear :else :add)
             :match (when (or cas? retract?) (or (:run (nth children 3 nil)) (constantly nil)))
             :program? (boolean program?) :program-fields program-fields
             :references? (boolean references?)
             :mixed? (boolean mixed?) :mixed-fields mixed-fields :mixed-infos (into {} infos)))))

(defn- fixed-scalar-layout [operations infos batch?]
  (when (and (not batch?)
             (every? (fn [op]
                       (let [source (:source op)]
                         (compile/prepared-positive-id?
                           (if (map? source) (:db/id source) (nth source 1 nil))))) operations))
    (let [entries (into [] (mapcat (fn [op]
                                    (let [source (:source op)
                                          e (long (if (map? source) (:db/id source) (nth source 1)))]
                                      (map (fn [[a]] [e a nil]) (:fields op))))) operations)
          sequence (plan/scalar-sequence entries)]
      {:sequence-layout sequence
       :dependent? (< (.-slot-count ^ScalarSequence sequence) (count entries))
       :entity-reads (compile/scalar-update-entity-reads
                       #(.-props ^AttrInfo (get infos %)) entries)})))

(defn prepare-template
  "Compose immutable operations. A batch repeats this program for every input
  row; its length is independent of the number of execution rows."
  [ctx operations {:keys [batch?]}]
  (when-not (and (literal-vector? operations) (seq operations))
    (throw (ex-info "A prepared transaction requires a nonempty operation vector"
                    {:error :prepared/program-shape})))
  (let [metadata (meta operations)
        operations (mapv #(prepare-operation ctx %) operations)
        kinds (set (map :kind operations))
        mixed? (and (nil? metadata) (every? :mixed? operations)
                    (or (some (fn [{:keys [source]}]
                                (and (map? source)
                                     (or (not (contains? source :db/id))
                                         (instance? TxParam (:db/id source))
                                         (compile/prepared-tempid-key (:db/id source))))) operations)
                        (some (fn [op] (some #(identical? :db.type/tuple (.-value-type ^AttrInfo (nth % 2)))
                                             (:mixed-fields op))) operations)))
        extended? (and mixed?
                       (some (fn [op] (some #(#{:db.type/ref :db.type/tuple}
                                               (.-value-type ^AttrInfo (nth % 2)))
                                            (:mixed-fields op))) operations))
        strategy (cond
                   (and (nil? metadata) (contains? kinds :scalar-program)
                        (every? :program? operations))
                   :scalar-program
                   (and (nil? metadata) (not mixed?) (some :references? operations)
                        (every? :program? operations)) :scalar-program
                   (and (= 1 (count kinds)) (nil? metadata)
                        (not (contains? kinds :dynamic)) (not extended?)) (first kinds)
                   mixed? :mixed
                   :else :dynamic)
        infos (into {} (comp (mapcat (if (= :scalar-program strategy) :program-fields :fields))
                             (keep (fn [[a _ info]] (when info [a info])))) operations)
        fixed (when (#{:scalar :scalar-program} strategy) (fixed-scalar-layout operations infos batch?))]
    (assoc (plan/->WriteTemplate ctx operations (vec (distinct (mapcat :params operations)))
                                (boolean batch?) strategy
                                (boolean (some #(or (:tx %) (:annotations %)) operations)) metadata)
           :infos infos :fixed-layout fixed
           :reference-attrs (when (= :scalar-program strategy)
                              (into #{} (keep (fn [[a ^AttrInfo info]]
                                               (when (identical? :db.type/ref (.-value-type info)) a))) infos))
           :mixed-infos (when mixed? (apply merge (map :mixed-infos operations)))
           :single-attribute? (= 1 (count infos))
           :identity? (boolean (some #(identical? :db.unique/identity (:db/unique (.-props ^AttrInfo %))) (vals infos)))
           :fuse? (every? #(.-fuse-blind? ^AttrInfo %) (vals infos)))))

(defn input-rows
  "Validate only parameter structure. Values, including lazy/host objects,
  remain untouched until the normal admission and correction checkpoint."
  [template input]
  (let [rows (if (:batch? template) input [input])]
    (when-not (vector? rows)
      (throw (ex-info "Prepared transaction batches require a vector of parameter maps"
                      {:error :prepared/input-shape})))
    (doseq [row rows]
      (when-not (plain-map? row)
        (throw (ex-info "Prepared transaction inputs require plain parameter maps"
                        {:error :prepared/input-shape})))
      (doseq [key (:params template)]
        (when-not (contains? row key)
          (throw (ex-info "Missing prepared transaction parameter"
                          {:error :prepared/missing-parameter :parameter key})))))
    rows))

(defn materialize
  "Instantiate original forms only for ordinary execution or a writer rejection."
  [template rows]
  (with-meta
    (into [] (mapcat (fn [row] (map #((:expression %) row) (:operations template)))) rows)
    (:metadata template)))

(defn request-input [template rows]
  (plan/->PreparedTxInput (delay (materialize template rows))))

(defn- bindings
  [template rows append!]
  (reduce (fn [_ row]
            (if (reduce (fn [_ op] (if (append! op row) true (reduced false)))
                        true (:operations template))
              true (reduced false)))
          true rows))

(defn- bind-scalar
  [template rows]
  (let [ctx (:ctx template) candidates (FastList.)
        fixed (:fixed-layout template) seen (when-not fixed (HashMap.))
        explicit? (:explicit? template) dependent? (volatile! (boolean (:dependent? fixed)))
        valid? (and (not (:auto-entity-time? (:opts ctx)))
                    (bindings template rows
                      (fn [op row]
                        (let [e ((:entity op) row) tx (when-let [f (:tx op)] (f row))]
                          (and (or (nil? tx) (compile/prepared-positive-id? tx))
                               (reduce (fn [_ [attr value info]]
                                         (if-let [result (compile/append-prepared-scalar!
                                                           ctx info candidates seen e attr (value row))]
                                           (do (when (= :datalevin.db.tx.compile/dependent-scalar result)
                                                 (vreset! dependent? true)) true)
                                           (reduced false)))
                                       true (:fields op)))))))]
    (when (and valid? (pos? (.size candidates))
               (compile/correct-prepared-scalars! ctx candidates))
      (let [offset (volatile! 0)
            entries (persistent!
                      (reduce (fn [out row]
                                (reduce (fn [out op]
                                          (let [tx (when-let [f (:tx op)] (some-> (f row) long))
                                                annotations (:annotations op)]
                                            (reduce (fn [out _]
                                                      (let [i @offset
                                                            e (.get candidates i) a (.get candidates (+ i 1))
                                                            v (.get candidates (+ i 2))]
                                                        (vswap! offset + 4)
                                                        (conj! out (if explicit?
                                                                     (cond-> [e a v tx] annotations (conj annotations))
                                                                     [e a v]))))
                                                    out (:fields op))))
                                        out (:operations template)))
                              (transient []) rows))
            aliases? (some #(not (instance? Long (nth % 0))) entries)
            infos (:infos template)
            sequence (when (and @dependent? (not aliases?))
                       (if fixed
                         (let [^ScalarSequence layout (:sequence-layout fixed)]
                           (ScalarSequence. entries (.-slots layout) (.-slot-count layout)))
                         (plan/scalar-sequence entries)))
            plan (plan/->TxPlan ctx (cond sequence :scalar-sequence aliases? :scalar-aliases :else :scalar)
                                (or sequence entries)
                                (when-not aliases?
                                  (when-not (or sequence (:single-attribute? template))
                                    (if fixed (:entity-reads fixed)
                                        (compile/scalar-update-entity-reads
                                          #(.-props ^AttrInfo (get infos %)) entries)))))]
        (when-not (and @dependent?
                       (or aliases?
                           (some (fn [op] (some #(not (.-typed? ^AttrInfo (nth % 2))) (:fields op)))
                                 (:operations template))))
          (cond-> plan
            explicit? (assoc :explicit-tx? true)
            (:single-attribute? template) (assoc :single-attribute? true)))))))

(defn- bind-blind
  [template rows]
  (let [ctx (:ctx template) sources (FastList.) seen (HashSet.)
        infos (HashMap. ^java.util.Map (:infos template))]
    (when (bindings template rows
            (fn [op row]
              (let [e ((:entity op) row)
                    explicit? (contains? (:source op) :db/id)
                    attrs (FastList.)]
                (and (or (not explicit?)
                         (when-let [key (compile/prepared-tempid-key e)] (.add seen key)))
                     (reduce (fn [_ [a value ^AttrInfo info]]
                               (if (compile/append-prepared-blind! ctx attrs info a (value row))
                                 true (reduced false)))
                             true (:fields op))
                     (do (.add sources (BlindSource. (if explicit? {:db/id e} {}) attrs)) true)))))
      (compile/finish-prepared-blind ctx sources infos (:identity? template) (:fuse? template)))))

(defn- bind-mixed
  [template rows]
  (let [ctx (:ctx template) infos (HashMap. ^java.util.Map (:mixed-infos template))
        entries (ArrayList.) tempids (HashSet.) updates (HashSet.)
        corrections? (volatile! false) inserts? (volatile! false)
        required-eid (volatile! 0) typed? (volatile! true)]
    (when (and (not (:auto-entity-time? (:opts ctx)))
               (bindings template rows
                 (fn [op row]
                   (let [e ((:entity op) row) fields (:mixed-fields op)]
                     (if (compile/prepared-positive-id? e)
                       (let [e (long e)]
                         (when (reduce (fn [_ [a value _]]
                                         (if-let [^AttrInfo info (compile/append-mixed-update!
                                                                  ctx infos updates entries e a (value row) corrections?)]
                                           (do (when-not (.-typed? info) (vreset! typed? false)) true)
                                           (reduced false)))
                                       true fields)
                           (vreset! required-eid (max (long @required-eid) e)) true))
                       (when (and (map? (:source op))
                                  (or (not (contains? (:source op) :db/id))
                                      (when-let [key (compile/prepared-tempid-key e)] (.add tempids key))))
                         (let [attrs (object-array (* 3 (count fields)))]
                           (dotimes [i (count fields)]
                             (let [[a value info] (nth fields i) offset (* 3 i)]
                               (aset attrs offset a) (aset attrs (inc offset) (value row))
                               (aset attrs (+ offset 2) info)))
                           (when (compile/mixed-insert-values-ready? ctx attrs tempids corrections?)
                             (.add entries (MixedInsert. e attrs))
                             (vreset! inserts? true) true))))))))
      (compile/finish-mixed-plan ctx infos updates entries corrections? @inserts? @required-eid @typed?))))

(defn- bind-scalar-program
  "Bind ordered scalar operations without creating source forms. Match values
  remain uncorrected; only fully proven addition operands enter correction."
  [template rows]
  (let [ctx (:ctx template) candidates (FastList.) entries (volatile! (transient []))
        aliases? (volatile! false) resolve? (volatile! false)]
    (when (and (not (:auto-entity-time? (:opts ctx)))
               (bindings template rows
                 (fn [op row]
                   (let [raw-e ((:entity op) row) e (compile/prepared-program-entity ctx raw-e)
                         opcode (:opcode op)
                         raw-match (when-let [f (:match op)] (f row))
                         tx (when-let [f (:tx op)] (f row))]
                     (and e (not (and (map? (:source op)) (keyword? raw-e)))
                          (or (nil? tx) (compile/prepared-positive-id? tx))
                          (reduce (fn [_ [attr value info]]
                                    (let [offset (.size candidates)
                                          reference? (identical? :db.type/ref (.-value-type ^AttrInfo info))
                                          raw-value (when (#{:add :cas} opcode) (value row))
                                          match (if reference? (when (some? raw-match) (compile/prepared-program-reference ctx raw-match)) raw-match)
                                          new-value (when reference? (compile/prepared-program-reference ctx raw-value))]
                                      (if (and (if reference?
                                                 (if (= :cas opcode) (some? match)
                                                     (or (nil? raw-match) (some? match)))
                                                 (compile/prepared-scalar-match? ctx info match (= :cas opcode)))
                                               (or (#{:retract :clear} opcode)
                                                   (if reference? new-value
                                                       (compile/append-prepared-scalar!
                                                         ctx info candidates nil e attr raw-value))))
                                        (do
                                          (when-not (instance? Long e)
                                            (vreset! aliases? true) (vreset! resolve? true))
                                          (when (and reference? (some #(and (some? %) (not (instance? Long %))) [match new-value]))
                                            (vreset! resolve? true))
                                          (vswap! entries conj! [e attr opcode match
                                                                (if reference? new-value offset)
                                                                (some-> tx long) (:annotations op)
                                                                (if reference? new-value raw-value)]) true)
                                        (reduced false))))
                                  true (:program-fields op))))))
               (compile/correct-prepared-scalars! ctx candidates))
      (let [reference-attrs (:reference-attrs template)
            entries (mapv (fn [[e a opcode match operand tx annotations raw-value]]
                            [e a opcode match
                             (when-not (#{:retract :clear} opcode)
                               (if (contains? reference-attrs a) operand (.get candidates (+ operand 2))))
                             tx annotations raw-value]) (persistent! @entries))
            ^ScalarSequence fixed (get-in template [:fixed-layout :sequence-layout])
            operands (cond
                       @aliases? (ScalarSequence. entries nil 0)
                       fixed (ScalarSequence. entries (.-slots fixed) (.-slot-count fixed))
                       :else (plan/scalar-sequence entries))]
        (cond-> (plan/->TxPlan ctx :scalar-program operands nil)
          @resolve? (assoc :resolve-ids? true :deferred-entity-resolution? @aliases?
                           :reference-attrs reference-attrs))))))

(defn bind-template
  "Create an execution-owned plan using compiled metadata and fresh operands.
  Declines precede effectful correction; the original materialized program is
  retained for the ordinary admission/retry path."
  [template rows]
  (case (:strategy template)
    :scalar (bind-scalar template rows)
    :blind (bind-blind template rows)
    :mixed (bind-mixed template rows)
    :scalar-program (bind-scalar-program template rows)
    nil))
