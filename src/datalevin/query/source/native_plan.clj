(ns ^:no-doc datalevin.query.source.native-plan
  "Controlled execution of existing native attribute plans inside a v2
  region. The native planner and storage scan driver remain authoritative."
  (:require [datalevin.query.optimizer.plan-build :as plan-build]
            [datalevin.query.optimizer.range :as query-range]
            [datalevin.query.plan :as plan]
            [datalevin.query.resolve :as resolve]
            [datalevin.query.source.control :as control]
            [datalevin.query.source.native :as native]
            [datalevin.query.source.runtime :as rt]
            [datalevin.query.source.value :as value :refer [fail!]]
            [datalevin.relation :as relation]
            [datalevin.storage.scan :as scans])
  (:import [datalevin.db DB]
           [datalevin.parser Constant Placeholder Predicate Variable]
           [datalevin.query.plan InitStep LinkStep MergeScanStep]
           [datalevin.utl QueryKey QueryKey$KeySet]
           [java.util List]
           [org.eclipse.collections.impl.list.mutable FastList]))

(defn- unsupported! [] (fail! :query-source/unsupported :planning))
(defn- ensure! [ok] (when-not ok (fail! :query-source/invalid :native-view)))

(defn- private-variable [prefix variables check!]
  (loop []
    (check!)
    (let [v (gensym prefix)]
      (if (contains? variables v) (recur) v))))

(defn- constant-entity [x]
  (when-not (or (nil? x) (integer? x)
                (and (not= :data (value/inferred-type x))
                     (value/disjoint-constant? :long x (fn [])))) (unsupported!))
  (when (and (integer? x) (<= Long/MIN_VALUE x Long/MAX_VALUE)) (long x)))

(defn spec!
  "Emit the precise interface of a native fixed-attribute access. No
  index is scanned and no payload is decoded while deriving this interface."
  [runtime snapshots database node literal-terms]
  (when-not (instance? DB database) (unsupported!))
  (native/validate! database)
  (let [snapshot (get snapshots (.-store ^DB database))
        _ (native/check-view! (:control runtime) snapshot)
        [entity attribute v :as pattern] (:pattern node)]
    (when-not (and (= 3 (count pattern))
                   (or (instance? Variable entity) (instance? Constant entity) (instance? Placeholder entity))
                   (instance? Constant attribute) (keyword? (:value attribute))
                   (or (instance? Variable v) (instance? Constant v) (instance? Placeholder v))) (unsupported!))
    (let [attribute (:value attribute)
          _ (when (instance? Constant entity) (constant-entity (:value entity)))
          unindexed? (get-in snapshot [:schema attribute :db/noindex])
          _ (when (and unindexed? (instance? Placeholder entity)) (unsupported!))
          type (when-not (instance? Placeholder v) (native/logical-type! snapshot attribute))]
      ;; A value bound is not an access path without AVE. Require an actual
      ;; entity supplier so scheduling cannot start with an unindexed scan.
      {:requires (if (and unindexed? (instance? Variable entity))
                   [{:variable (:symbol entity) :type :data :nullable? true}]
                   [])
       :value-type type
       :constant-constraints (mapv (fn [[variable i]]
                                     {:variable variable :type (case i 0 :long 1 :keyword 2 type) :nullable? false})
                                   literal-terms)
       :columns (cond-> []
                  (instance? Variable entity) (conj {:variable (:symbol entity) :type :long :nullable? false})
                  (and (instance? Variable v) (not= (:symbol entity) (:symbol v)))
                  (conj {:variable (:symbol v) :type type :nullable? false}))})))

(defn- bound-value [props x literal?]
  (let [type (:db/valueType props)
        logical (if (= type :db.type/ref) :long (some-> type name keyword))]
    (when (and (#{:db.type/boolean :db.type/long :db.type/ref :db.type/float :db.type/double
                  :db.type/uuid :db.type/instant :db.type/string :db.type/keyword :db.type/symbol
                  :db.type/bytes} type)
               (some? x)
               (not (and (or (instance? Float x) (instance? Double x)) (Double/isNaN (double x)))))
      (cond (and literal? (#{:db.type/long :db.type/ref} type)
                 (integer? x) (<= Long/MIN_VALUE x Long/MAX_VALUE)) (long x)
            (value/matches-type? logical x)
            (if (and (#{:db.type/float :db.type/double} type) (zero? (double x)))
              (if (= type :db.type/float) (float 0.0) 0.0) x)))))

(defn- predicate-constraints [node core-clauses props bindings check!]
  (let [v (:symbol (nth (:pattern node) 2))]
    (if-not v []
      (into []
        (keep (fn [{:keys [node]}]
                (check!)
                (let [operator (get-in node [:fn :symbol]) [a b :as args] (:args node)
                      fixed? #(or (instance? Constant %)
                                  (and (instance? Variable %) (contains? bindings (:symbol %))))
                      first? (and (instance? Variable a) (= v (:symbol a)) (fixed? b))
                      last? (and (fixed? a) (instance? Variable b) (= v (:symbol b)))
                      term (when (or first? last?) (if first? b a))
                      bound (if (instance? Constant term) (:value term) (get bindings (:symbol term)))
                      physical (when (#{'= '< '<= '> '>=} operator) (bound-value props bound (= '= operator)))]
                  (when (and (instance? Predicate node) (= 2 (count args)) (or first? last?)
                             ;; Bare symbols can resolve to host values in the
                             ;; shared scalar resolver; they are not fixed data.
                             (not (symbol? bound))
                             (some? physical)
                             (or (= '= operator)
                                 (#{:db.type/long :db.type/ref :db.type/float :db.type/double :db.type/instant}
                                   (:db/valueType props))))
                    {:variables (cond-> [v] (instance? Variable term) (conj (:symbol term)))
                     :bound physical :equality? (= '= operator)
                     :range (first (query-range/constant-predicate-ranges operator v physical first?))
                     :test (query-range/activate-var-pred
                             {:make-call resolve/make-call :resolve-pred resolve/resolve-pred}
                             v (list operator (if first? v bound) (if first? bound v)))})))) core-clauses))))

(defn- controlled! [runtime check! snapshot database step keys mode v value-type constraints]
  (check!)
  (native/check-view! (:control runtime) snapshot)
  ;; Admission covers the output list, visitor closures, and native range/key
  ;; scratch before creating a scan. Each row/value has its own prior admission.
  (let [tokens (volatile! [(control/reserve! (:control runtime) {:retained-bytes 8192})])
        own! #(do (vswap! tokens conj %) %)
        _ (rt/own! runtime #(let [held @tokens]
                             (vreset! tokens [])
                             (doseq [token held] (control/release! token))))
        attribute (if (instance? MergeScanStep step) (ffirst (:attrs-v step)) (:attr step))
        props (get-in snapshot [:schema attribute])
        visit (when (or (instance? LinkStep step)
                        (and (instance? InitStep step) (< 1 (count (:vars step))))
                        (and (instance? MergeScanStep step) (seq (:vars step))))
                (native/value-visitor (:control runtime) snapshot attribute))
        ;; The resolver checks host Vars before tuple bindings. A provider
        ;; planning callback may install one after emission, so check here.
        constraints (filterv #(not-any? (fn [v] (ns-resolve *ns* v)) (:variables %)) constraints)
        literal? (instance? Constant v)
        runtime-value? (#{:value :entity-value} mode)
        policy {:schema (:schema snapshot)
                :check! #(do (check!) (native/check-view! (:control runtime) snapshot))
                :unsupported! unsupported!
                :admit-row! #(own! (control/reserve! (:control runtime) {:retained-bytes 1024}))
                :with-key!
                (fn [key scan!]
                  (check!)
                  (let [x (cond literal? (:value v)
                                runtime-value? (aget ^objects key (if (= mode :value) 0 1)))
                        fixed? (or literal? runtime-value?)
                        physical (when fixed? (bound-value props x literal?))
                        equality (if (some? physical)
                                   {:bound physical :range [[:closed physical] [:closed physical]]}
                                   (first (filter :equality? constraints)))
                        bounds (if equality [(:bound equality)] (mapv :bound constraints))
                        scratch (when (seq bounds)
                                  (rt/charge! runtime bounds (+ 4096 (* 2048 (count bounds))) 8))]
                    (try
                      (let [ranges (if equality [(:range equality)]
                                       (when (seq constraints)
                                         (apply query-range/intersect-ranges (mapv (comp vector :range) constraints))))
                            empty? (or (and fixed? (nil? x)) (and (seq bounds) (empty? ranges))
                                       (and literal? (value/disjoint-constant? value-type x check!))
                                       (and literal? (#{:db.type/long :db.type/ref} (:db/valueType props))
                                            (integer? x) (not (<= Long/MIN_VALUE x Long/MAX_VALUE))))
                            index-range (when (and (:db/aid props) (seq ranges))
                                          ;; Closed candidate endpoints retain complete buckets;
                                          ;; the original shared predicates decide strictness.
                                          [(scans/ave-key-range (:db/aid props) (:db/valueType props)
                                                                (mapv #(assoc % 0 :closed) (first ranges))) :avg])]
                        (scan! {:empty? empty? :index-range index-range
                                :visit-value!
                                (fn [raw eid emit]
                                  (visit raw eid
                                    (fn [value]
                                      (when (and (or (not fixed?)
                                                     (if literal? (QueryKey/equiv x value) (QueryKey/equals x value)))
                                                 (every? (fn [constraint] (check!) ((:test constraint) value)) constraints))
                                        (own! (rt/charge-copy! runtime value))
                                        (emit (value/owned-copy value check!))))))}))
                      (finally (when scratch (control/release! scratch))))))}]
    (plan/-execute-controlled step database keys policy)))

(defn- binding-keys! [runtime check! entity v mode input-cols ^List tuples]
  (let [entity? (not= mode :value) value? (#{:value :entity-value} mode)
        fixed? (instance? Constant entity)
        n (long (if (and fixed? (not value?)) (min 1 (.size tuples)) (.size tuples)))
        token (control/reserve! (:control runtime) {:retained-bytes (+ 2048 (* 512 n))})
        _ (rt/own! runtime #(control/release! token))
        seen (QueryKey$KeySet.) keys (FastList.)
        e-index (.indexOf ^List input-cols (:symbol entity)) v-index (.indexOf ^List input-cols (:symbol v))]
    (dotimes [i n]
      (check!)
      (let [e (when entity? (if fixed? (constant-entity (:value entity)) (aget ^objects (.get tuples i) e-index)))
            value (when value? (aget ^objects (.get tuples i) v-index))
            key (cond-> [] entity? (conj e) value? (conj value))]
        ;; Native IDs are Long keys. Other bound representations cannot join
        ;; these facts under shared key equality; a literal uses constant equality.
        (when (and (or (not entity?) (instance? Long e)) (or (not value?) (some? value)) (.add seen key))
          (.add keys (object-array key)))))
    keys))

(defrecord ^:private NativeStep [in cols run]
  plan/IStep
  (-type [_] :native-controlled)
  (-execute [_ _ tuples] (run tuples))
  (-execute-pipe [_ _ _ _] (unsupported!))
  (-sample [_ _ _] (unsupported!))
  (-explain [_ _] "Controlled native attribute access"))

(defrecord ^:private NativeFactory [in cols build]
  plan/IStepFactory
  (-instantiate-step [_ input-cols] (build input-cols)))

(defn factory!
  "Compile a single native pattern with the existing native planner. Native
  InitSteps and entity-bound MergeScanSteps use controlled storage. Other plan
  shapes reject before provider acquisition until their admission paths exist."
  [runtime snapshots database node {:keys [requires columns value-type source-symbol region-id]} core-clauses bindings check! join!]
  (let [snapshot (get snapshots (.-store ^DB database))
        _ (native/check-view! (:control runtime) snapshot)
        token (rt/charge! runtime (mapv :variable columns) (+ 16384 (* 1024 (count core-clauses))) 1)
        _ (rt/own! runtime #(control/release! token))
        [e attribute v] (:pattern node)
        repeated? (and (instance? Variable e) (instance? Variable v) (= (:symbol e) (:symbol v)))
        variables (mapv :variable columns)
        entity (if (instance? Variable e) (:symbol e)
                   (private-variable "?native_entity__" (set variables) check!))
        value-var (when-not (instance? Placeholder v)
                    (if (and (instance? Variable v) (not repeated?)) (:symbol v)
                        (private-variable "?native_value__" (conj (set variables) entity) check!)))
        ;; Reuse the native planner's pure single-pattern emitter. Mutable
        ;; Store metadata, global plan caches and installed access methods
        ;; cannot change this plan after its schema was captured in the view.
        init (plan-build/initialization-step entity (:value attribute) value-var)
        lookup (plan-build/entity-lookup-step entity (:value attribute) value-var)
        value-lookup (when value-var (plan-build/value-lookup-step entity (:value attribute) value-var))
        constraints (predicate-constraints node core-clauses (get-in snapshot [:schema (:value attribute)]) bindings check!)]
    (assoc (->NativeFactory (set (map :variable requires)) variables
      (fn [input-cols]
        (let [lookup? (or (instance? Constant e) (and (instance? Variable e) (some #{entity} input-cols)))
              value-bound? (and (instance? Variable v) (not repeated?) (some #{value-var} input-cols))
              mode (cond (and lookup? value-bound?) :entity-value lookup? :entity value-bound? :value)
              step (case mode :value value-lookup (:entity :entity-value) lookup init)
              output-cols (into input-cols (remove (set input-cols)) variables)
              attrs (select-keys (plan/cols->attrs (:cols step)) variables)]
          (->NativeStep (set input-cols) output-cols
            (fn [^List tuples]
              (rt/with-context runtime (cond-> {} source-symbol (assoc :source-symbol source-symbol)
                                             (some? region-id) (assoc :region-id region-id))
                (fn [runtime]
                  (check!)
                  (native/check-view! (:control runtime) snapshot)
                  (let [keys (when mode (binding-keys! runtime check! e v mode input-cols tuples))
                        scanned (controlled! runtime check! snapshot database step keys mode v value-type constraints)
                        rows (if repeated?
                               (let [result (FastList.)
                                     attrs (plan/cols->attrs (:cols step))
                                     ei (int (get attrs entity)) vi (int (get attrs value-var))]
                                 (doseq [^objects row scanned]
                                   (check!)
                                   (when (QueryKey/equals (aget row ei) (aget row vi))
                                     (.add result row)))
                                 result) scanned)
                        right (relation/relation! attrs rows)
                        left (relation/relation! (zipmap input-cols (range)) tuples)
                        result (join! left right)]
                    (ensure! (= output-cols (mapv key (sort-by val (:attrs result)))))
                    (:tuples result)))))))))
      ;; An independent mapped producer can bound this read before decoding.
      ;; These preferences never supply bindings; spec! requires the entity
      ;; independently for unindexed attributes.
      :binding-bound? (or (instance? Constant e) (instance? Constant v) (seq constraints))
      :binding-enumerate? (get-in snapshot [:schema (:value attribute) :db/noindex])
      :binding-inputs (when (get-in snapshot [:schema (:value attribute)])
                        (cond-> #{}
                          (instance? Variable e) (conj (:symbol e))
                          (instance? Variable v) (conj (:symbol v)))))))
