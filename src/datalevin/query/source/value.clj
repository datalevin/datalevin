(ns datalevin.query.source.value
  "Owned logical values and explicit custom-value validation/accounting hooks."
  (:import [clojure.lang APersistentVector$SubVector BigInt Ratio RT Sorted]
           [datalevin.utl QueryKey]
           [java.math BigDecimal BigInteger]
           [java.time Duration Instant Period]
           [java.util Date UUID]))

(def scalar-types
  #{:boolean :long :bigint :double :float :bigdec :string :keyword :symbol
    :uuid :instant :bytes :data})

(defprotocol IQuerySourceValue
  "Opt-in contract for immutable custom values. This does not register a storage
  codec or change equality. Implementations must include all retained backing
  storage in their conservative charge and perform no I/O in either method."
  (-logical-type [value] "Return this value's complete custom type descriptor.")
  (-retained-bytes [value] "Return a conservative positive integer heap charge."))

(declare matches-type?)

(defn inferred-type
  "Describe a captured value without changing its representation. Collections
  and nil use :data; a custom value retains its complete declared identity."
  [x]
  (cond (nil? x) :data
        (satisfies? IQuerySourceValue x) (-logical-type x)
        :else (or (some #(when (matches-type? % x) %) (disj scalar-types :data)) :data)))

(defn fail!
  ([category phase] (fail! category phase {}))
  ([category phase context]
   ;; All callers supply structural context, never provider values or causes.
   (throw (ex-info "Query-source contract violation"
                   (merge {:error category :phase phase} context)))))

(defn plain-map? [x] (and (map? x) (not (record? x))))

(def ^:private maximum-depth 128)

(defn- check-depth! [depth phase]
  (when (> (long depth) (long maximum-depth)) (fail! :query-source/limit phase)))

(defn- custom-comparator? [x]
  (and (sorted? x) (not (identical? RT/DEFAULT_COMPARATOR (.comparator ^Sorted x)))))

(defn- owned-copy* [x check! depth]
  (when check! (check!))
  (check-depth! depth :values)
  (if (satisfies? IQuerySourceValue x)
    x
    (let [_ (when (custom-comparator? x) (fail! :query-source/unsupported :values))
          copy-child #(owned-copy* % check! (inc (long depth)))
          copy (cond
                 (bytes? x) (aclone ^bytes x)
                 (= Date (class x)) (Date. (.getTime ^Date x))
                 (vector? x) (mapv copy-child x)
                 (plain-map? x) (into (if (sorted? x) (empty x) {})
                                      (map (fn [[k v]] [(copy-child k) (copy-child v)])) x)
                 (set? x) (into (if (sorted? x) (empty x) #{}) (map copy-child) x)
                 (list? x) (apply list (map copy-child x))
                 :else x)]
      (when check! (check!))
      (if-let [metadata (when (instance? clojure.lang.IObj copy) (meta x))]
        (with-meta copy (copy-child metadata)) copy))))

(defn owned-copy
  "Detach mutable leaves in supported values, including metadata. Custom values
  follow their immutable opt-in contract and are retained as supplied. The
  caller must reserve the value's charge before copying retained query state.
  An internal check callback can stop copying on cancellation."
  ([x] (owned-copy x nil))
  ([x check!] (owned-copy* x check! 0)))

(defn- diagnostic-copy* [x check! depth]
  (check!)
  (check-depth! depth :values)
  (let [copy #(diagnostic-copy* % check! (inc (long depth)))]
    (cond
      (symbol? x) (with-meta x nil)
      (plain-map? x) (into {} (map (fn [[k v]] [(copy k) (copy v)])) x)
      (vector? x) (mapv copy x)
      (set? x) (into #{} (map copy) x)
      (list? x) (apply list (map copy x))
      :else x)))

(defn diagnostic-copy
  "Copy a validated non-secret portable identifier for diagnostics. Preserve
  its value, detach collection backing, and omit Clojure metadata at every level.
  The caller must reserve copy space and supply its cancellation check."
  [x check!]
  (diagnostic-copy* x check! 0))

(defn- add-charges
  "Add exact charge values. A pair of longs stays on the primitive path;
  larger integer charges use BigInt arithmetic without boxed math."
  ([a b]
   (if (and (instance? Long a) (instance? Long b))
     (+' (long a) (long b))
     (.add ^BigInt (bigint a) ^BigInt (bigint b))))
  ([a b c] (add-charges (add-charges a b) c))
  ([a b c d] (add-charges (add-charges (add-charges a b) c) d)))

(defn add-charge
  "Add fixed overhead with primitive dispatch for long charges and exact overflow."
  [^long overhead charge]
  (if (instance? Long charge)
    (+' overhead (long charge))
    (add-charges overhead charge)))

(defn multiply-charge
  "Scale a charge without truncating an arbitrarily large retained value."
  [^long factor charge]
  (if (instance? Long charge)
    (*' factor (long charge))
    (.multiply (BigInt/valueOf factor) ^BigInt (bigint charge))))

(defn at-most?
  "Compare against a long bound, preserving exact charges and deadlines beyond it."
  [n ^long maximum]
  (if (instance? Long n)
    (<= (long n) maximum)
    (and (number? n)
         ;; Numbers.compare treats NaN as equal to every number, unlike <=.
         (not (and (or (instance? Double n) (instance? Float n))
                   (Double/isNaN (double n))))
         (not (pos? (compare n maximum))))))

(defn- metadata-node? [x]
  (cond
    (or (nil? x) (boolean? x) (string? x) (keyword? x) (symbol? x)
        (uuid? x) (integer? x) (decimal? x) (char? x) (ratio? x)) true
    (or (double? x) (float? x)) (Double/isFinite (double x))
    (custom-comparator? x) false
    :else (or (plain-map? x) (vector? x) (set? x) (list? x))))

(defn- portable*? [x check! depth]
  (when check! (check!))
  (check-depth! depth :values)
  (let [child? #(portable*? % check! (inc (long depth)))]
    (and (metadata-node? x)
         (cond
           (plain-map? x) (every? (fn [[k v]] (and (child? k) (child? v))) x)
           (or (vector? x) (set? x) (list? x)) (every? child? x)
           :else true)
         (if-let [metadata (meta x)] (child? metadata) true))))

(defn portable?
  "Metadata, including Clojure metadata, must be immutable EDN data.
  Mutable dates/arrays are row values, not descriptor or plan metadata.
  An internal check callback observes cancellation during validation."
  ([x] (portable? x nil))
  ([x check!] (portable*? x check! 0)))

(declare value-charge*)

(defn- charge-part [charge observe!]
  (when observe! (observe! charge))
  charge)

(defn- collection-overhead [x observe! backing? metadata-phase depth]
  (add-charges
    (charge-part (+ 64 (* 48 (count x))) observe!)
    (if-let [m (meta x)] (value-charge* m observe! backing? metadata-phase (inc (long depth))) 0)
    ;; A view pins the entire parent, including elements outside its range
    ;; and the parent's metadata. An owned copy detaches that parent.
    (if (and backing? (instance? APersistentVector$SubVector x))
      (value-charge* (.-v ^APersistentVector$SubVector x) observe! true metadata-phase (inc (long depth))) 0)))

(defn- value-charge* [x observe! backing? metadata-phase depth]
  (when observe! (observe! 0))
  (check-depth! depth (or metadata-phase :values))
  (when (and metadata-phase (not (metadata-node? x)))
    (fail! :query-source/invalid metadata-phase))
  (cond
    (satisfies? IQuerySourceValue x)
    (let [n (-retained-bytes x)]
      (when-not (and (integer? n) (<= 64 n Long/MAX_VALUE))
        (fail! :query-source/invalid :values))
      (charge-part n observe!))
    (nil? x) (charge-part 8 observe!)
    (or (boolean? x) (instance? Long x) (instance? Integer x)
        (instance? Byte x) (instance? Short x) (char? x)
        (instance? Double x) (instance? Float x)) (charge-part 32 observe!)
    (string? x) (charge-part (+ 64 (* 2 (count x))) observe!)
    (or (keyword? x) (symbol? x))
    (add-charges
      (charge-part 96 observe!)
      (value-charge* (namespace x) observe! backing? metadata-phase depth)
      (value-charge* (name x) observe! backing? metadata-phase depth)
      (if-let [m (meta x)] (value-charge* m observe! backing? metadata-phase (inc (long depth))) 0))
    (instance? UUID x) (charge-part 48 observe!)
    (= Date (class x)) (charge-part 64 observe!)
    (or (instance? Instant x) (instance? Duration x) (instance? Period x))
    (charge-part 64 observe!)
    (bytes? x) (charge-part (+' 32 (alength ^bytes x)) observe!)
    (instance? BigInteger x)
    (charge-part (+' 64 (quot (+ 7 (.bitLength ^BigInteger x)) 8)) observe!)
    (instance? clojure.lang.BigInt x)
    (add-charges (charge-part 32 observe!)
                 (value-charge* (biginteger x) observe! backing? metadata-phase depth))
    (instance? BigDecimal x)
    (add-charges (charge-part 64 observe!)
                 (value-charge* (.unscaledValue ^BigDecimal x) observe! backing? metadata-phase depth))
    (ratio? x) (add-charges (charge-part 64 observe!)
                            (value-charge* (.numerator ^Ratio x) observe! backing? metadata-phase depth)
                            (value-charge* (.denominator ^Ratio x) observe! backing? metadata-phase depth))
    (custom-comparator? x) (fail! :query-source/unsupported :values)
    (plain-map? x)
    (add-charges
      (charge-part (+ 96 (* 64 (count x))) observe!)
      (reduce (fn [n [k v]] (add-charges n (value-charge* k observe! backing? metadata-phase (inc (long depth)))
                                          (value-charge* v observe! backing? metadata-phase (inc (long depth))))) 0 x)
      (if-let [m (meta x)] (value-charge* m observe! backing? metadata-phase (inc (long depth))) 0))
    (or (vector? x) (set? x) (list? x))
    (add-charges
      (collection-overhead x observe! backing? metadata-phase depth)
      (reduce (fn [n child] (add-charges n (value-charge* child observe! backing? metadata-phase (inc (long depth))))) 0 x))
    :else (fail! :query-source/unsupported :values)))

(defn value-charge
  "Conservative retained size, including metadata and vector backing storage.
  Each occurrence is charged; shared backing may be counted more than once.
  The internal observer receives incremental charges before child traversal
  and may throw on cancellation or a byte limit."
  ([x] (value-charge* x nil true nil 0))
  ([x observe!] (value-charge* x observe! true nil 0)))

(defn metadata-charge
  "Validate immutable metadata while charging its retained storage. Reject
  custom values without invoking their hooks. Observe limits/cancellation
  before descending into children, including metadata and vector parents."
  [x phase observe!]
  (value-charge* x observe! true phase 0))

(defn vector-parent
  "Return the retained parent of a vector view, or nil for an owned vector."
  [x]
  (when (instance? APersistentVector$SubVector x)
    (.-v ^APersistentVector$SubVector x)))

(defn copy-charge
  "Charge the detached result of owned-copy without allocating that copy.
  Hidden vector elements are omitted; immutable custom values keep their full
  declared retained charge because owned-copy preserves them."
  ([x] (value-charge* x nil false nil 0))
  ([x observe!] (value-charge* x observe! false nil 0)))

(defn- matches-type*?
  ([t x charged? check!] (matches-type*? t x charged? check! 0))
  ([t x charged? check! depth]
    (when check! (check!))
    (check-depth! depth :types)
    (if (map? t)
      (case (:kind t)
        :tuple (and (vector? x) (= (count x) (count (:elements t)))
                    (every? true? (map #(matches-type*? %1 %2 charged? check! (inc (long depth))) (:elements t) x)))
        :custom (and (satisfies? IQuerySourceValue x) (= t (-logical-type x)))
        false)
      (case t
        :boolean (boolean? x)
        :long (int? x)
        :bigint (or (instance? BigInteger x) (instance? clojure.lang.BigInt x))
        :double (instance? Double x)
        :float (instance? Float x)
        :bigdec (instance? BigDecimal x)
        :string (string? x)
        :keyword (keyword? x)
        :symbol (symbol? x)
        :uuid (instance? UUID x)
        :instant (= Date (class x))
        :bytes (bytes? x)
        :data (or charged?
                  (try (copy-charge x (when check! (fn [_] (check!)))) true
                       (catch clojure.lang.ExceptionInfo e
                         ;; Resource failure must not become a type mismatch.
                         (when check! (check!))
                         (when (#{:query-source/limit :query-source/cancelled :query/timeout} (:error (ex-data e)))
                           (throw e))
                         false)))
        false))))

(defn matches-type?
  "Check a logical type, optionally observing cancellation inside nested data
  and tuple components. The callback also runs after custom type validation."
  ([t x] (matches-type? t x nil))
  ([t x check!]
   (let [matches? (matches-type*? t x false check!)]
     (when check! (check!))
     matches?)))

(defn disjoint-constant?
  "Prove that an owned constant cannot equal any non-null value of a logical
  type. A representation mismatch alone is insufficient for numeric values,
  sequential tuples or custom equality. Unknown overlap is never an empty
  proof; callers must preserve ordinary predicate/constant equality."
  [type x check!]
  (check!)
  (or (nil? x)
      (and (not (matches-type? type x check!))
           (not (or (satisfies? IQuerySourceValue x)
                    (= :custom (:kind type))
                    (and (number? x) (#{:long :bigint :float :double :bigdec} type))
                    (and (= :tuple (:kind type)) (sequential? x)))))))

(defn- execution-value* [x check! depth]
  (when check! (check!))
  (check-depth! depth :values)
  (cond
    (plain-map? x) (doseq [[k v] x] (execution-value* k check! (inc (long depth))) (execution-value* v check! (inc (long depth))))
    (or (vector? x) (set? x) (list? x)) (run! #(execution-value* % check! (inc (long depth))) x)))

(defn execution-value!
  ([x] (execution-value! x nil))
  ([x check!] (execution-value* x check! 0)))

(defn rows!
  "Validate logical rows before copying or sizing their retained representation."
  [columns rows]
  (when-not (and (vector? rows)
                (every? #(and (vector? %) (= (count columns) (count %))) rows))
    (fail! :query-source/invalid :batch))
  (doseq [row rows, [column x] (map vector columns row)]
    (copy-charge x)
    (when-not (if (nil? x) (:nullable? column) (matches-type*? (:type column) x true nil))
      (fail! :query-source/invalid :batch)))
  rows)

(defn batch-charge-bytes
  ([columns rows] (batch-charge-bytes columns rows nil))
  ([columns rows observe!]
   (when-not (vector? rows) (fail! :query-source/invalid :batch))
   ;; Ordinary empty batches transfer no row state. Empty views or metadata
   ;; can still pin data, and need a charge and a reservation of their own.
   (if (and (empty? rows) (nil? (meta rows))
            (not (instance? APersistentVector$SubVector rows))) 0
       (add-charges
         (charge-part 64 observe!)
         (value-charge columns observe!)
         (collection-overhead rows observe! true nil 0)
         (reduce
           (fn [n row]
             (when-not (and (vector? row) (= (count columns) (count row)))
               (fail! :query-source/invalid :batch))
             (add-charges
               n
               (collection-overhead row observe! true nil 0)
               (reduce
                 (fn [n i]
                   (let [x (nth row i)
                         bytes (value-charge x observe!)
                         column (nth columns i)]
                     ;; The value walk already validated nested :data.
                     (when-not (if (nil? x) (:nullable? column)
                                   (matches-type*? (:type column) x true nil))
                       (fail! :query-source/invalid :batch))
                     (add-charges n bytes)))
                 0 (range (count row)))))
           0 rows)))))

(defn- binding-id? [x]
  (and (integer? x) (<= 0 x Long/MAX_VALUE)))

(defn dependent-batch-charge-bytes
  "Charge the dependent batch shape, including IDs on an empty final batch.
  The input-free helper above deliberately never allocates or charges IDs."
  ([columns rows binding-ids]
   (dependent-batch-charge-bytes columns rows binding-ids nil))
  ([columns rows binding-ids observe!]
   (when-not (and (vector? rows) (vector? binding-ids)
                  (= (count rows) (count binding-ids)))
     (fail! :query-source/invalid :batch))
   (let [bytes (add-charges (batch-charge-bytes columns rows observe!)
                            (charge-part 64 observe!)
                            (value-charge binding-ids observe!))]
     (doseq [id binding-ids]
       (when observe! (observe! 0))
       (when-not (binding-id? id) (fail! :query-source/invalid :batch)))
     bytes)))

(defn binding-charge-bytes
  "Charge a nonempty dependent open after its required columns are validated.
  IDs and typed keys must be unique; metadata and backing views are included.
  The observer can reject quota excess before descending into retained values."
  ([requires bindings] (binding-charge-bytes requires bindings nil))
  ([requires bindings observe!]
   (when-not (and (vector? requires) (seq requires) (vector? bindings) (seq bindings))
     (fail! :query-source/invalid :batch))
   (let [bytes (add-charges (charge-part 64 observe!)
                            (value-charge requires observe!)
                            (value-charge bindings observe!))]
     (dotimes [i (count bindings)]
       (when observe! (observe! 0))
       (let [{:keys [binding-id values] :as binding} (nth bindings i)]
         (when-not (and (plain-map? binding) (contains? binding :binding-id) (contains? binding :values)
                        (every? #(or (#{:binding-id :values} %) (qualified-keyword? %)) (keys binding))
                        (binding-id? binding-id) (vector? values) (= (count requires) (count values)))
           (fail! :query-source/invalid :batch))
         (doseq [[column x] (map vector requires values)]
           (when-not (if (nil? x) (:nullable? column) (matches-type*? (:type column) x true nil))
             (fail! :query-source/invalid :batch)))
         ;; Batches are already capped before execution. Pairwise validation
         ;; avoids allocating an uncharged secondary dedup index while sizing.
         (dotimes [j i]
           (when observe! (observe! 0))
           (let [previous (nth bindings j)]
             (when (or (= binding-id (:binding-id previous))
                       (QueryKey/equals values (:values previous)))
               (fail! :query-source/invalid :batch))))))
     bytes)))

(defn ^:no-doc owned-bindings-header-charge
  "Fixed charge for core-generated binding vectors without metadata/backing.
  This is a sizing component, not a legal empty dependent input envelope."
  [requires observe!]
  (add-charges (charge-part 64 observe!)
               (value-charge requires observe!)
               (collection-overhead [] observe! true nil 0)))

(defn ^:no-doc owned-binding-element-charge
  "Incremental vector slot and binding charge. The collector has already
  validated/owned this binding and established typed key/ID uniqueness."
  [binding observe!]
  (add-charges (charge-part 48 observe!) (value-charge binding observe!)))
