(ns datalevin.query.source.conformance
  "Reusable embedded execution checks. Adapters supply constructors, never
  engine dependencies or an alternate evaluator. Not part of the runtime SPI."
  (:require [clojure.test :refer [is testing]]
            [datalevin.core :as d]
            [datalevin.parser :as parser]
            [datalevin.query :as query]
            [datalevin.query.source :as spi]
            [datalevin.query.source.control :as control]
            [datalevin.query.source.fake :as fixture]
            [datalevin.query.source.value :as value]
            [datalevin.util :as u])
  (:import [datalevin.parser FindScalar FindTuple Pattern SrcVar]
           [datalevin.utl QueryKey QueryKey$KeyMap]
           [java.time Duration Instant]
           [java.util Collection Date HashSet Objects Random UUID]
           [java.util.concurrent CountDownLatch TimeUnit]))

(defn- descriptor [contract]
  (fixture/info (:attributes contract) {:compute-context-id :conformance
                                        :definition-fingerprint "conformance:v2"}))

(defn- row-descriptor [type]
  (assoc (descriptor {:kind :eav :attributes {:row/value {:entity-type :long :value-type type}}})
         :fixture/row-mapping true))

(defn source-constructor
  "Apply an explicit test row mapping before handing EAV facts to an adapter."
  [constructor]
  (fn
    ([info rows] (constructor info (if (:fixture/row-mapping info) (fixture/row-facts rows) rows)))
    ([info rows options] (constructor info (if (:fixture/row-mapping info) (fixture/row-facts rows) rows) options))))

(defn replace-rows [replace-facts! provider rows]
  (replace-facts! provider (if (:fixture/row-mapping (spi/-source-info provider)) (fixture/row-facts rows) rows)))

(defn- error-data [f]
  (try (f) nil (catch clojure.lang.ExceptionInfo e (ex-data e))))

(defn- bag [rows]
  ;; Preserve multiplicities, decimal scale, signed zero and byte content. An
  ;; ordinary Clojure set/frequency map would erase some of these distinctions.
  (let [result (QueryKey$KeyMap.)]
    (doseq [row rows]
      (.put result row (inc (long (or (.get result row) 0)))))
    result))

(defn- same-result? [q expected actual]
  (let [shape (:qfind (parser/parse-query q))]
    (if (or (instance? FindScalar shape) (instance? FindTuple shape))
      (QueryKey/equals expected actual)
      (.equals ^QueryKey$KeyMap (bag expected) (bag actual)))))

(defn mapped-scan-semantics!
  "Shared scan gate for providers with their own accepted nominal identities.
  Supply a reusable source and complete eager EAV facts for exactly its view;
  attribute roles are :identity, :value, :many, and optional :reference. This
  exercises only input-free scans, with bindings/residuals/joins in core. It
  does not certify dependent access paths or provider-specific acquisition."
  [source facts {:keys [identity value many reference]}]
  (let [value-fact (first (filter #(= value (second %)) facts))
        entity (first value-fact) sample (nth value-fact 2)
        cases
        [[{:find '[?e ?v] :in '[$s] :where [['$s '?e value '?v]]} []]
         [{:find '[[?v ...]] :in '[$s] :where [['$s '_ value '?v]]} []]
         [{:find '[(count ?e) .] :in '[$s] :where [['$s '?e value '_]]} []]
         [{:find '[?id ?v] :in '[$s] :where [['$s '?e identity '?id] ['$s '?e value '?v]]} []]
         [{:find '[?v ?m] :in '[$s] :where [['$s '?e value '?v] ['$s '?e many '?m]]} []]
         [{:find '[(count ?e) .] :with '[?m] :in '[$s]
           :where [['$s '?e value '_] ['$s '?e many '?m]]} []]
         [{:find '[?e] :in '[$s ?v] :where [['$s '?e value '?v]]} [sample]]
         [{:find '[?v] :in '[$s ?e] :where [['$s '?e value '?v]]} [entity]]
         [{:find '[?e ?v] :in '[$s [?v ...]] :where [['$s '?e value '?v]]} [[sample sample]]]
         [{:find '[?v] :in '[$s]
           :where [(list 'or ['$s '_ value '?v] ['$s '_ many '?v])]} []]
         [{:find '[?v ?m] :in '[$a $b]
           :where [['$a '?e value '?v] ['$b '?e many '?m]]} ::aliases]]]
    (is (some? value-fact) "The scan fixture must include a value fact")
    (doseq [[query inputs] cases]
      (testing (str "mapped scan: " (pr-str query))
        (is (let [expected (if (= ::aliases inputs) (d/q query facts facts) (apply d/q query facts inputs))
                  actual (if (= ::aliases inputs) (d/q query source source) (apply d/q query source inputs))]
              (same-result? query expected actual)))))
    (when reference
      (let [query {:find '[?e ?ref] :in '[$s] :where [['$s '?e reference '?ref]]}]
        (is (same-result? query (d/q query facts) (d/q query source)))))
    (let [query {:find '[?e ?m] :in '[$s] :where [['$s '?e many '?m]]
                 :query-source {:limits {:batch-rows 1}}}
          explanation (d/explain {:run? true} query source)]
      (is (= (count (d/q query facts)) (:result-rows explanation)))
      (is (seq (:cursors explanation)))
      (is (every? #(= :attribute-enumeration (get-in % [:access :kind]))
                  (mapcat :selected (:sources explanation)))))))

(defn mapped-lookup-semantics!
  "Shared entity-lookup gate using the provider's own nominal types. Supply
  complete eager EAV facts and :identity, :value, :many, optional :reference
  attribute roles. Every read has an entity seed; no scan is required. This
  checks typed dependent association, multiplicity, joins, aliases and bounds
  through ordinary d/q. Provider-specific acquisition/fault gates are separate."
  [source facts {:keys [identity value many reference]}]
  (let [entities (vec (distinct (map first (filter #(= identity (second %)) facts))))
        witnesses (into [] (mapcat (fn [e] [[e :a] [e :b]])) entities)
        cases [{:find '[?e ?v] :in '[$s [?e ...]] :where [['$s '?e value '?v]]}
               {:find '[?e ?key] :in '[$s [?e ...]] :where [['$s '?e identity '?key]]}
               {:find '[?e ?v ?m] :in '[$s [?e ...]]
                :where [['$s '?e value '?v] ['$s '?e many '?m]]}
               {:find '[(count ?e) .] :with '[?m] :in '[$s [?e ...]]
                :where [['$s '?e value '_] ['$s '?e many '?m]]}]
        run! (fn [query & inputs]
               (let [expected (apply d/q query facts inputs)
                     actual (apply d/q (assoc query :query-source
                                             {:consistency :best-effort
                                              :limits {:binding-rows 1 :batch-rows 1}}) source inputs)]
                 (is (same-result? query expected actual) (pr-str query))))]
    (is (seq entities) "The lookup fixture must include identity facts")
    (doseq [query cases] (run! query entities))
    (run! {:find '[(count ?e) .] :with '[?witness] :in '[$s [[?e ?witness]]]
           :where [['$s '?e value '_]]} witnesses)
    (when reference
      (run! {:find '[?e ?ref] :in '[$s [?e ...]] :where [['$s '?e reference '?ref]]} entities))
    (doseq [entity entities]
      (run! {:find '[?v] :in '[$s ?e] :where [['$s '?e value '?v]]} entity))
    (let [query {:find '[?e ?v ?m] :in '[$a $b [?e ...]]
                 :where [['$a '?e value '?v] ['$b '?e many '?m]]}
          expected (d/q query facts facts entities)
          actual (d/q (assoc query :query-source {:consistency :best-effort}) source source entities)]
      (is (same-result? query expected actual)))))

(defrecord ObservedSession [delegate events hook request envelopes]
  spi/IQuerySourceSession
  (-session-info [_]
    ((get envelopes :session-info identity) (spi/-session-info delegate)))
  (-open-cursor [_ request]
    (hook :open request)
    (let [cursor (spi/-open-cursor delegate ((get envelopes :cursor-request identity) request))
          wrapped
          (reify spi/IQuerySourceCursor
            (-next-batch! [_ batch-request]
              (hook :fetch batch-request)
              (let [batch (spi/-next-batch! cursor batch-request)]
                (swap! events conj [:batch (:row-count batch) (:byte-count batch)])
                (hook :fetched batch-request)
                batch))
            (-cursor-metrics [_]
              ((get envelopes :metrics identity) (spi/-cursor-metrics cursor)))
            (-close-cursor! [_]
              (hook :close-cursor request)
              (swap! events conj [:close-cursor])
              (spi/-close-cursor! cursor)
              (hook :closed-cursor request)))]
      (swap! events conj [:open-cursor])
      (hook :opened request)
      wrapped))
  (-cancel-session! [_ reason]
    (hook :cancel-session reason)
    (spi/-cancel-session! delegate reason))
  (-close-session! [_]
    (hook :close-session request)
    (swap! events conj [:close-session])
    (spi/-close-session! delegate)
    (hook :closed-session request)))

(defn- observe
  ([source] (observe source (fn [_ _])))
  ([source hook] (observe source hook {}))
  ([source hook envelopes]
   (let [events (atom []) controls (atom [])]
     {:events events :controls controls
      :source
      (reify spi/IQuerySource
        (-source-info [_] ((get envelopes :descriptor identity) (spi/-source-info source)))
        (-open-session [_ request]
          (swap! controls conj (:control request))
          (hook :acquisition request)
          (let [session (spi/-open-session source request)]
            (swap! events conj [:open-session])
            (let [session (->ObservedSession session events hook request envelopes)]
              (hook :acquired request)
              session)))
        (-plan-source [_ session request]
          (swap! events conj [:plan (:source-symbol request)])
          (hook :planning request)
          (let [provider-request ((get envelopes :planning-request identity) request)
                response ((get envelopes :planning identity)
                          (spi/-plan-source source (:delegate session) provider-request))]
            (hook :planned {:request request :provider-request provider-request
                            :response response})
            response)))})))

(defn- closed! [{:keys [events controls]}]
  (let [counts (frequencies (map first @events))]
    (is (= (get counts :open-session 0) (get counts :close-session 0)))
    (is (= (get counts :open-cursor 0) (get counts :close-cursor 0))))
  (doseq [ctl @controls]
    (is (= :closed (:state (error-data #(spi/check-active! ctl)))))
    (is (every? zero? (vals (dissoc (control/usage ctl) :external-requests))))))

(defn- eager-arguments [q arguments]
  (let [parsed (parser/parse-query q)
        aliases (into #{} (keep (fn [p]
                                 (when (= :row/value (:value (get (:pattern p) 1)))
                                   (or (get-in p [:source :symbol]) '$))))
                      (parser/collect #(instance? Pattern %) (:qwhere parsed)))]
    (mapv (fn [binding input]
            (if (and (instance? SrcVar (:variable binding))
                     (contains? aliases (get-in binding [:variable :symbol]))
                     (vector? input) (every? #(and (vector? %) (= 2 (count %))) input))
              (fixture/row-facts input) input)) (:qin parsed) arguments)))

(defn- compare! [q eager-inputs source-inputs observed]
  (try
    (binding [query/*cache?* false]
      (is (same-result? q (apply d/q q (eager-arguments q eager-inputs)) (apply d/q q source-inputs))
          (pr-str q)))
    (finally (run! closed! observed))))

(def ^:private families
  {:boolean [nil false true]
   :long [nil -1 0 1 2 Long/MIN_VALUE Long/MAX_VALUE]
   :bigint [nil -1N 0N 1N (biginteger 1) 18446744073709551616N]
   :string [nil "" "a" "雪"]
   :keyword [nil :a :test/a :test/b :测试/值]
   :symbol [nil 'a 'test/a 'test/b (with-meta 'test/a {:note ["metadata"]})]
   :uuid [nil (UUID. 0 0) (UUID. 0 1) (UUID. -1 -1)]
   :instant [nil (Date. -1) (Date. 0) (Date. 1) (Date. Long/MAX_VALUE)]
   :bigdec [nil 1.0M 1.00M -2M 3M]
   :double [nil 0.0 -0.0 1.0 ##Inf ##NaN]
   :float [nil (float 0) (float -0.0) (float 1) Float/POSITIVE_INFINITY Float/NaN]
   :data [nil [1 nil] (list 1 nil) {:nested [1 "x"]} #{:a :b}
          (Date. 42) (Instant/ofEpochSecond -1 123) (Duration/ofSeconds -2 3)]
   :bytes [nil (byte-array []) (byte-array [0 31]) (byte-array [1 0])
           (byte-array [0 31])]})

(defn- generated-rows [seed values]
  (if (zero? (long seed)) []
      (let [random (Random. (long seed))
            rows (mapv (fn [i] [(long (inc (long i)))
                                (value/owned-copy (nth values (.nextInt random (count values))))])
                       (range 9))]
        (into rows (value/owned-copy (subvec rows 0 3))))))

(def ^:private tuple-queries
  ['{:find [?id ?v] :in [$s] :where [[$s ?id :row/value ?v]]}
   '{:find [?v] :in [$s] :where [[$s _ :row/value ?v]]}
   '{:find [[?v ...]] :in [$s] :where [[$s _ :row/value ?v]]}
   '{:find [?v] :keys [value] :in [$s] :where [[$s _ :row/value ?v]]}
   '{:find [?v] :with [?id] :in [$s] :where [[$s ?id :row/value ?v]]}
   '{:find [?v (count ?id)] :in [$s] :where [[$s ?id :row/value ?v]]}
   '{:find [(count ?v) .] :in [$s] :where [[$s _ :row/value ?v]]}
   '{:find [(count ?v) .] :with [?id] :in [$s] :where [[$s ?id :row/value ?v]]}
   '{:find [?id] :in [$s] :where [[$s ?id :row/value ?v] [(not= ?v nil)]]}
   '{:find [?a ?b] :in [$s] :where [[$s ?a :row/value ?v] [$s ?b :row/value ?v] [(< ?a ?b)]]}])

(defn- repeated-variable-columns! [make-source]
  ;; Nullable positional data remains ordinary Datalog input. An independent
  ;; mapped fact exercises these joins through the public v2 compiler.
  (doseq [width [2 3], mask (range (bit-shift-left 1 (long width))), batch-size [1 4]]
    (let [nullable (mapv #(bit-test mask %) (range width))
          rows (into [(vec (repeat width 1)) (vec (repeat width 2)) (vec (repeat width 1))]
                     (concat (for [i (range width) :when (nullable i)]
                               (assoc (vec (repeat width 1)) i nil))
                             (when (every? true? nullable) [(vec (repeat width nil))])))
          anchor [[1 :row/value "anchor"]]
          provider (make-source (row-descriptor :string) [[1 "anchor"]])
          pattern (into ['$rows] (repeat width '?v))
          q {:find '[?v] :in '[$rows $anchor] :where [pattern '[$anchor 1 :row/value "anchor"]]
             :query-source {:limits {:batch-rows batch-size}}}]
      (doseq [query [q (assoc q :find '[(count ?v) .])]]
        (let [observed (observe provider)]
          (compare! query [rows anchor] [rows (:source observed)] [observed]))))))

(defn tuple-semantics!
  "Compare mapped EAV row values with eager d/q using deterministic seeds.
  Failure contexts name the seed, logical type and batch size."
  [make-source]
  (is (= value/scalar-types (set (keys families))))
  (repeated-variable-columns! make-source)
  (doseq [[type values] families, seed [0 1 42], batch-size [1 4]]
    (testing (str "type=" type " seed=" seed " batch=" batch-size)
      (let [rows (generated-rows seed values)
            source (make-source (row-descriptor type) rows)
            options {:limits {:batch-rows batch-size :spill-bytes 0}}]
        (doseq [q tuple-queries]
          (let [observed (observe source)]
            (compare! (assoc q :query-source options) [rows] [(:source observed)] [observed])))
        (doseq [[binding input] [['?v (second values)] ['[?v] [(second values)]]
                                 ['[?v ...] [(second values) (second values) nil]]
                                 ['[[?v]] [[(second values)] [nil] [(second values)]]]]]
          (let [observed (observe source)
                q {:find '[?id] :in ['$s binding] :where '[[$s ?id :row/value ?v]] :query-source options}]
            (compare! q [rows (value/owned-copy input)]
                      [(:source observed) (value/owned-copy input)] [observed])))
        (let [observed (observe source)
              q '{:find [?a ?b] :in [$a $b]
                  :where [[$a ?a :row/value ?v] [$b ?b :row/value ?v] [(< ?a ?b)]]}]
          (compare! (assoc q :query-source options) [rows (value/owned-copy rows)]
                    [(:source observed) (:source observed)] [observed])
          (is (= 1 (count (filter #(= :open-session (first %)) @(:events observed)))))
          (is (= {'$a 1 '$b 1} (frequencies (keep #(when (= :plan (first %)) (second %)) @(:events observed))))))))))

(deftype NominalRef [type key]
  value/IQuerySourceValue
  (-logical-type [_] type)
  (-retained-bytes [_] (+ 128 (long (value/value-charge type)) (long (value/value-charge key))))
  Object
  ;; Descriptor equality is Clojure equality; keys retain Java key equality.
  (hashCode [_] (unchecked-int (bit-xor (long (hash type)) (Objects/hashCode key))))
  (equals [_ other]
    (and (instance? NominalRef other)
         (= type (.-type ^NominalRef other))
         (Objects/equals key (.-key ^NominalRef other)))))

(defn- reference-type [domain epoch key-type]
  {:kind :custom :name :conformance/entity-ref :version 1 :fingerprint "conformance-ref:v1"
   :parameters {:domain domain :identity-epoch epoch :key-type key-type}})

(defn- reference-type-variants [type]
  [[:same type true]
   [:map-order (into (array-map) (reverse (seq type))) true]
   [:metadata (with-meta type {:conformance/note "annotation"}) true]
   [:name (assoc type :name :conformance/another-ref) false]
   [:version (update type :version inc) false]
   [:fingerprint (assoc type :fingerprint "conformance-ref:v2") false]
   [:namespaced-extra (assoc type :conformance/extra {:meaning "v2"}) false]
   [:namespaced-extra-value (assoc type :conformance/extra {:meaning "v3"}) false]
   [:parameter-extra (assoc-in type [:parameters :refinement] :v2) false]
   [:domain (assoc-in type [:parameters :domain] :suppliers) false]
   [:epoch (update-in type [:parameters :identity-epoch] inc) false]])

(defn- reference-identity! [left right same?]
  (is (= same? (.equals ^Object left right)))
  (is (= same? (.equals ^Object right left)))
  (is (= same? (= left right)))
  (when same?
    (is (= (.hashCode ^Object left) (.hashCode ^Object right)))
    (is (= (hash left) (hash right))))
  ;; Distinct identities may collide; only equal identities must hash alike.
  (let [^Collection refs [left right]]
    (is (= (if same? 1 2) (count (HashSet. refs)))))
  (is (= (if same? 1 2) (count (set [left right])))))

(defn- nominal-descriptor-validation! [make-source]
  (let [type (reference-type :customers 0 :long)
        ref (NominalRef. type 7)
        info (descriptor {:kind :eav :attributes {:customer/id {:entity-type type :value-type :long}}})
        valid (make-source info [[ref :customer/id 7]])]
    (doseq [[label invalid] [[:kind (assoc type :kind :entity-ref)]
                             [:name (assoc type :name :entity-ref)]
                             [:version (assoc type :version 0)]
                             [:fingerprint (assoc type :fingerprint "")]
                             [:unknown-field (assoc type :extra :v2)]]]
      (testing (str "invalid nominal descriptor " label)
        (let [provider (reify spi/IQuerySource
                         (-source-info [_] (assoc-in info [:pattern-contract :attributes :customer/id :entity-type]
                                                    invalid))
                         (-open-session [_ request] (spi/-open-session valid request))
                         (-plan-source [_ session request] (spi/-plan-source valid session request)))
              observed (observe provider)]
          (is (= :query-source/invalid
                 (:error (error-data #(d/q '[:find ?ref :in $s :where [$s ?ref :customer/id _]]
                                          (:source observed))))))
          (is (empty? @(:controls observed)))
          (closed! observed))))
    (let [refs (mapv #(NominalRef. (second %) 7) (reference-type-variants type))
          observed (observe (make-source (row-descriptor :data) (mapv vector (range) refs)))]
      ;; Three equivalent descriptors plus eight distinct nominal variants.
      ;; Grouping must preserve changed extra values as well as their presence.
      (is (= 9 (count (HashSet. ^Collection refs))))
      (is (= 9 (d/q '[:find (count ?ref) . :in $s :where [$s _ :row/value ?ref]] (:source observed))))
      (is (= {1 8, 3 1}
             (frequencies
               (map second
                    (d/q '[:find ?ref (count ?id) :in $s :where [$s ?id :row/value ?ref]] (:source observed))))))
      (closed! observed))
    ;; Descriptor equality is Clojure map equality, including inside extras;
    ;; hashing must agree even when equal metadata has different JVM hashes.
    (let [a (NominalRef. (assoc type :conformance/extra {:n 1.0M}) 7)
          b (NominalRef. (assoc type :conformance/extra {:n 1.00M}) 7)
          rows [[1 a] [2 b]]
          provider (make-source (row-descriptor :data) rows)
          q '[:find (count ?ref) . :in $s :where [$s _ :row/value ?ref]]
          observed (observe provider)]
      (reference-identity! a b true)
      (is (= 1 (d/q q (:source observed))))
      (closed! observed))))

(defn- nominal-fixture [composite?]
  (let [key-type (if composite? {:kind :tuple :elements [:string :long]} :long)
        key (fn [id] (if composite? ["tenant" id] id))
        order-type (reference-type :orders 0 key-type)
        customer-type (reference-type :customers 0 key-type)
        order #(NominalRef. order-type (key %))
        customer #(NominalRef. customer-type (key %))
        info (descriptor {:kind :eav :attributes
                          {:order/id {:entity-type order-type :value-type key-type}
                           :order/customer {:entity-type order-type :value-type customer-type}
                           :order/amount {:entity-type order-type :value-type :long}
                           :customer/id {:entity-type customer-type :value-type key-type}
                           :customer/name {:entity-type customer-type :value-type :string}
                           :customer/friend {:entity-type customer-type :value-type customer-type}}})
        rows [[(order 1) :order/id (key 1)] [(order 2) :order/id (key 2)]
              [(order 1) :order/customer (customer 7)] [(order 2) :order/customer (customer 7)]
              [(order 3) :order/customer (customer 8)] [(order 4) :order/customer nil]
              [(order 1) :order/amount 10] [(order 1) :order/amount 10] [(order 2) :order/amount 20]
              [(order 3) :order/amount nil]
              [(customer 7) :customer/id (key 7)] [(customer 8) :customer/id (key 8)]
              [(customer 7) :customer/name "Ada"] [(customer 8) :customer/name nil]
              [(customer 7) :customer/friend (customer 7)] [(customer 8) :customer/friend (customer 7)]
              [nil :customer/name "missing entity"]]]
    {:info info :rows rows :key key :key-type key-type :order order :customer customer
     :eager (filterv #(and (some? (first %)) (some? (nth % 2))) rows)}))

(defn- nominal-eav! [make-source]
  (doseq [composite? [false true], batch-size [1 4]]
    (testing (str "nominal references, composite=" composite? ", batch=" batch-size)
      (let [{:keys [info rows eager order customer key key-type]} (nominal-fixture composite?)
            provider (make-source info rows)
            options {:limits {:batch-rows batch-size}}
            queries ['{:find [?order ?name] :in [$s]
                       :where [[$s ?order :order/customer ?customer] [$s ?customer :customer/name ?name]]}
                     '{:find [?name] :with [?order] :in [$s]
                       :where [[$s ?order :order/customer ?customer] [$s ?customer :customer/name ?name]]}
                     '{:find [?name (count ?order) (sum ?amount)] :in [$s]
                       :where [[$s ?order :order/customer ?customer] [$s ?customer :customer/name ?name]
                               [$s ?order :order/amount ?amount]]}
                     '{:find [?order] :in [$s] :where [[$s ?order :order/amount _]]}
                     '{:find [?order] :in [$s] :where [[$s ?order :order/customer _]]}
                     '{:find [?customer] :in [$s] :where [[$s ?customer :customer/friend ?customer]]}
                     '{:find [?order ?name] :in [$a $b]
                       :where [[$a ?order :order/customer ?customer] [$b ?customer :customer/name ?name]]}]]
        (doseq [q queries]
          (let [observed (observe provider)
                aliases (count (:in q))]
            (compare! (assoc q :query-source options) (repeat aliases eager)
                      (repeat aliases (:source observed)) [observed])))
        (doseq [[binding input] [['?customer (customer 7)]
                                 ['[?customer] [(customer 7)]]
                                 ['[?customer ...] [(customer 7) (customer 7) (customer 8)]]
                                 ['[[?customer ?tag]] [[(customer 7) :a] [(customer 7) :b]]]]]
          (let [observed (observe provider)
                q {:find '[?order] :in ['$s binding]
                   :where '[[$s ?order :order/customer ?customer]] :query-source options}]
            (compare! q [eager input] [(:source observed) input] [observed])))
        (let [observed (observe provider)
              q {:find '[?order] :in '[$s] :where [['$s '?order :order/customer (customer 7)]]
                 :query-source options}]
          (compare! q [eager] [(:source observed)] [observed]))
        (doseq [input [(order 7) (key 7) (NominalRef. (reference-type :customers 1 key-type) (key 7))]]
          (let [observed (observe provider)
                q '[:find ?order :in $s ?customer :where [$s ?order :order/customer ?customer]]]
            (if (and composite? (= input (key 7)))
              (do
                (is (= #{} (d/q q (:source observed) input)))
                (is (= #{} (d/q q eager input))))
              (do
                (is (= :query-source/unsupported (:error (error-data #(d/q q (:source observed) input)))))
                (is (empty? @(:controls observed)))))
            (closed! observed)))
        (doseq [[label type same?] (reference-type-variants (reference-type :customers 0 key-type))]
          (testing (str "nominal type field " label ", composite=" composite?)
            (let [ref (NominalRef. type (key 7))
                  other-info (descriptor {:kind :eav :attributes
                                          {:customer/id {:entity-type type :value-type key-type}}})
                  other-rows [[ref :customer/id (key 7)]]
                  a (observe provider) b (observe (make-source other-info other-rows))
                  natural '[:find ?left ?right :in $a $b
                            :where [$a ?left :customer/id ?key] [$b ?right :customer/id ?key]]
                  nominal '[:find ?ref :in $a $b
                            :where [$a ?ref :customer/id _] [$b ?ref :customer/id _]]]
              (compare! natural [eager other-rows] [(:source a) (:source b)] [a b])
              (reference-identity! (customer 7) ref same?)
              (if same?
                (compare! nominal [eager other-rows] [(:source a) (:source b)] [a b])
                (let [opens (mapv #(count @(:controls %)) [a b])]
                  (is (= :query-source/unsupported
                         (:error (error-data #(d/q nominal (:source a) (:source b))))))
                  (is (= opens (mapv #(count @(:controls %)) [a b])))
                  (run! closed! [a b])))
              (let [bound (observe provider)
                    q '[:find ?order :in $s ?customer :where [$s ?order :order/customer ?customer]]]
                (if same?
                  (compare! q [eager ref] [(:source bound) ref] [bound])
                  (do
                    (is (= :query-source/unsupported
                           (:error (error-data #(d/q q (:source bound) ref)))))
                    (is (empty? @(:controls bound)))
                    (closed! bound)))))))))))

(defn eav-semantics! [make-source]
  (nominal-descriptor-validation! make-source)
  (nominal-eav! make-source)
  (let [info (descriptor {:kind :eav :attributes
                           {:item/key {:entity-type :long :value-type :long}
                            :item/value {:entity-type :long :value-type :string}}})
        facts [[1 :item/key 1] [2 :item/key 2] [3 :item/key 3]
               [1 :item/value "a"] [1 :item/value "a"] [2 :item/value nil]
               [3 :item/value "a"] [nil :item/value "missing entity"]]
        eager (filterv #(and (some? (first %)) (some? (nth % 2))) facts)
        source (make-source info facts)]
    (doseq [q ['{:find [?e] :in [$s] :where [[$s ?e :item/value _]]}
               '{:find [?v] :with [?e] :in [$s] :where [[$s ?e :item/value ?v]]}
               '{:find [?e ?key] :in [$s] :where [[$s ?e :item/key ?key] [$s ?e :item/value _]]}
               '{:find [?a ?b] :in [$s]
                 :where [[$s ?a :item/value ?v] [$s ?b :item/value ?v] [(< ?a ?b)]]}
               '{:find [(count ?e) .] :in [$s] :where [[$s ?e :item/value _]]}]]
      (let [observed (observe source)]
        (compare! (assoc q :query-source {:limits {:batch-rows 1}})
                  [eager] [(:source observed)] [observed])))))

(defn- native-nominal! [make-source]
  (doseq [composite? [false true]]
    (let [{:keys [info rows eager key customer]} (nominal-fixture composite?)
          dir (u/tmp-dir (str "source-nominal-conformance-" (random-uuid)))
          conn (d/create-conn dir
                 {:native/key (if composite?
                                {:db/valueType :db.type/tuple
                                 :db/tupleTypes [:db.type/string :db.type/long]}
                                {:db/valueType :db.type/long})}
                 {:wal? false :background-sampling? false})
          provider (make-source info rows)]
      (try
        (d/transact! conn [{:db/id 1 :native/key (key 7)} {:db/id 2 :native/key (key 9)}])
        (doseq [batch-size [1 4]]
          (let [observed (observe provider)
                q {:find '[?native ?ref ?name] :in '[$ $s]
                   :where '[[?native :native/key ?key] [$s ?ref :customer/id ?key]
                            [$s ?ref :customer/name ?name]]
                   :query-source {:limits {:batch-rows batch-size}}}]
            (compare! q [@conn eager] [@conn (:source observed)] [observed])
            (is (= #{[1 (customer 7) "Ada"]} (d/q q @conn (:source observed))))
            (closed! observed)))
        (let [observed (observe provider)]
          (is (= :query-source/unsupported
                 (:error (error-data #(d/q '[:find ?ref :in $ $s
                                             :where [?ref :native/key _] [$s ?ref :customer/name _]]
                                           @conn (:source observed))))))
          (is (empty? @(:controls observed)))
          (closed! observed))
        (finally (d/close conn) (u/delete-files dir))))))

(defn native-semantics! [make-source]
  (native-nominal! make-source)
  (let [dir (u/tmp-dir (str "source-conformance-" (random-uuid)))
        conn (d/create-conn dir {:native/id {:db/valueType :db.type/long}
                                :native/value {} :native/private {:db/noindex true}}
                            {:wal? false :background-sampling? false})]
    (try
      (let [rows [[1 {:v (byte-array [1])}] [2 {:v (byte-array [2])}]
                  [3 {:v (byte-array [1])}] [4 nil]]
            source (make-source (row-descriptor :data) (value/owned-copy rows))]
        (d/transact! conn
          (vec (concat (for [[id _] rows] [:db/add id :native/id id])
                       (for [[id v] rows :when (some? v) a [:native/value :native/private]]
                         [:db/add id a v]))))
        (doseq [q ['{:find [?e ?v] :in [$ $s] :where [[$s ?id :row/value ?v] [?e :native/id ?id]]}
                   '{:find [?id] :in [$ $s] :where [[$s ?id :row/value ?v] [?id :native/value ?v]]}
                   '{:find [?id] :in [$ $s] :where [[$s ?id :row/value ?v] [?id :native/private ?v]]}
                   '{:find [?a ?b] :in [$ $s] :where [[?a :native/value ?v] [$s ?b :row/value ?v]]}
                   '{:find [?v (count ?id)] :in [$ $s] :where [[$s ?id :row/value ?v] [?id :native/id _]]}]]
          (let [observed (observe source)]
            (compare! q [@conn (value/owned-copy rows)] [@conn (:source observed)] [observed]))))
      (finally (d/close conn) (u/delete-files dir)))))

(defn- acquisition-boundary! [make-source replace-rows!]
  ;; More than one lazy-sequence chunk: small source groups can accidentally
  ;; acquire every view through lookahead even without an explicit boundary.
  (let [n 40
        symbols (mapv #(symbol (str "$s" %)) (range n))
        q {:find '[?id ?v] :in symbols
           :where (mapv #(vector % '?id :row/value '?v) symbols)}]
    (testing "all views are pinned before the first planning callback"
      (let [providers (mapv (fn [_] (make-source (row-descriptor :string) [[1 "old"]])) symbols)
            acquired (atom 0)
            at-plan (atom nil)
            hook (fn [phase _]
                   (case phase
                     :acquisition (swap! acquired inc)
                     :planning (when (compare-and-set! at-plan nil @acquired)
                                 (doseq [provider providers]
                                   (replace-rows! provider [[2 "new"]])))
                     nil))
            observed (mapv #(observe % hook) providers)]
        (try
          (is (= #{[1 "old"]} (apply d/q q (mapv :source observed))))
          (is (= n @at-plan))
          (is (= #{[2 "new"]} (apply d/q q (mapv :source observed))))
          (finally (run! closed! observed)))))
    (testing "a late acquisition failure prevents every planning callback"
      (let [attempts (atom 0)
            hook (fn [phase _]
                   (when (and (= :acquisition phase) (= n (swap! attempts inc)))
                     (throw (ex-info "secret acquisition failure" {:secret true}))))
            observed (mapv (fn [_] (observe (make-source (row-descriptor :string) [[1 "old"]])
                                           hook)) symbols)]
        (try
          (is (= :query-source/provider
                 (:error (error-data #(apply d/q q (mapv :source observed))))))
          (is (= n @attempts))
          (is (not-any? #(#{:plan :open-cursor} (first %))
                        (mapcat #(deref (:events %)) observed)))
          (finally (run! closed! observed)))))))

(defn- concurrent-views! [make-source replace-rows!]
  (doseq [phase [:planning :fetch], cancel? [false true]]
    (testing (str "independent invocations at " phase ", cancel first=" cancel?)
      (let [provider (make-source (row-descriptor :string) [[1 "old"]])
            q '[:find ?id ?v :in $a $b :where [$a ?id :row/value ?v] [$b ?id :row/value ?v]]
            pending (mapv
                      (fn [_]
                        (let [entered (promise) release (CountDownLatch. 1)
                              first? (atom true)]
                          {:entered entered :release release
                           :observed (observe provider
                                       (fn [at request]
                                         (when (and (= at phase) (compare-and-set! first? true false))
                                           (deliver entered request)
                                           (when-not (.await release 10 TimeUnit/SECONDS)
                                             (throw (ex-info "conformance wait expired" {}))))))}))
                      (range 2))
            start (fn [{:keys [observed]}]
                    (future
                      (try {:result (d/q q (:source observed) (:source observed))}
                           (catch clojure.lang.ExceptionInfo e {:error (ex-data e)}))))
            [a b] pending
            workers (atom [])]
        (try
          (let [first-query (start a)
                _ (swap! workers conj first-query)
                first-request (deref (:entered a) 5000 ::timeout)]
            (is (map? first-request))
            (when (map? first-request)
              (replace-rows! provider [[2 "new"]])
              (let [second-query (start b)
                    _ (swap! workers conj second-query)
                    second-request (deref (:entered b) 5000 ::timeout)]
                (is (map? second-request))
                (when (map? second-request)
                  (is (not (identical? (:control first-request) (:control second-request))))
                  (when cancel?
                    (control/cancel! (:control first-request)
                                     {:error :query-source/cancelled :phase :execution}))
                  (.countDown ^CountDownLatch (:release a))
                  (let [result (deref first-query 5000 ::timeout)]
                    (if cancel?
                      (is (= :query-source/cancelled (get-in result [:error :error])))
                      (is (= {:result #{[1 "old"]}} result))))
                  (closed! (:observed a))
                  (is (not (realized? second-query)))
                  (is (nil? (spi/check-active! (:control second-request))))
                  (.countDown ^CountDownLatch (:release b))
                  (is (= {:result #{[2 "new"]}} (deref second-query 5000 ::timeout)))
                  (closed! (:observed b))))))
          (finally
            (doseq [{:keys [release]} pending] (.countDown ^CountDownLatch release))
            (doseq [worker @workers] (future-cancel worker))))))))

(defn- blocking-cancellation! [make-source]
  (doseq [phase [:acquisition :acquired :open :opened :fetch :fetched
                :close-cursor :close-session]
          cause [:cancel :deadline]
          aliases? [false true]
          mode (if (#{:acquisition :acquired :close-session} phase)
                 [:query :explain :plan] [:query :explain])]
    (testing (str "blocked " phase ", " cause ", aliases=" aliases? ", " mode)
      (let [entered (promise)
            callback (promise)
            released (CountDownLatch. 1)
            visits (atom 0)
            signal! (fn [reason]
                      (deliver callback {:thread (Thread/currentThread) :reason reason})
                      (.countDown released))
            hook (fn [at request]
                   (when (= :cancel-session at) (signal! request))
                   (when (and (= phase at)
                              (= (if (#{:fetch :fetched} phase) 2 1) (swap! visits inc)))
                     ;; Acquisition has no session callback yet. All later
                     ;; operations must be released by core's session callback.
                     ;; After-return hooks deliberately hand the acquired
                     ;; resource to core even when cancellation won the race.
                     (let [ctl (:control request)
                           registration (when (#{:acquisition :acquired} phase)
                                          (spi/register-cancel! ctl signal!))]
                       (try
                         (deliver entered {:control ctl :thread (Thread/currentThread)})
                         (when-not (.await released 10 TimeUnit/SECONDS)
                           (throw (ex-info "conformance wait expired" {})))
                         (finally
                           (when registration (spi/unregister-cancel! registration)))))))
            rows [[1 "one"] [2 "two"] [3 "three"]]
            provider (make-source (row-descriptor :string) rows)
            observed (observe provider hook)
            q (cond-> '{:find [?id ?v] :in [$a] :where [[$a ?id :row/value ?v]]
                        :query-source {:limits {:batch-rows 1 :in-flight-batches 1}}}
                aliases? (assoc :in '[$a $b] :where '[[$a ?id :row/value ?v] [$b ?id :row/value ?v]]))
            inputs (repeat (if aliases? 2 1) (:source observed))
            worker (future
                     (try
                       {:result (case mode
                                  :query (apply d/q q inputs)
                                  :explain (apply d/explain {:run? true} q inputs)
                                  :plan (apply d/explain {:run? false} q inputs))}
                       (catch clojure.lang.ExceptionInfo e {:error (ex-data e)})))
            nested (atom nil)]
        (try
          (let [{:keys [control thread] :as waiting} (deref entered 5000 ::timeout)]
            (is (map? waiting))
            (when control
              (is (not (realized? worker)))
              (if (= :cancel cause)
                (control/cancel! control {:error :query-source/cancelled :phase :execution})
                ;; Start a real nested deadline only after the operation is
                ;; blocked, avoiding compilation/scheduling-dependent timeouts.
                (reset! nested (control/create {:parent control :timeout-ms 25})))
              (let [outcome (deref worker 5000 ::timeout)
                    category (if (= :cancel cause) :query-source/cancelled :query/timeout)
                    signalled (deref callback 1000 ::timeout)]
                (is (= category (get-in outcome [:error :error])))
                (is (not (contains? outcome :result)))
                (is (= category (get-in signalled [:reason :error])))
                (is (not (identical? thread (:thread signalled))))
                (when (#{:acquisition :acquired :open :opened :fetch :fetched} phase)
                  (is (= '$a (get-in outcome [:error :source-symbol])))
                  (is (= (if (#{:acquisition :acquired} phase) nil 0) (get-in outcome [:error :region-id]))))
                (when (#{:fetch :fetched} phase)
                  (is (some #(= [:batch 1] (take 2 %)) @(:events observed))))
                (let [events (mapv first @(:events observed))]
                  (is (= (if (= :acquisition phase) 0 1)
                         (count (filter #{:open-session} events))))
                  (when (some #{:open-cursor} events)
                    (is (< (.lastIndexOf events :close-cursor)
                           (.lastIndexOf events :close-session)))))
                (closed! observed)
                ;; A cancelled session must not poison the source object or a
                ;; later invocation's captured view and reservations.
                (let [fresh (observe provider)]
                  (compare! q (repeat (count inputs) rows)
                            (repeat (count inputs) (:source fresh)) [fresh])))))
          (finally
            (.countDown released)
            (when-let [ctl @nested] (control/close! ctl))
            (future-cancel worker)))))))

(defn- cursor-request-contract! [make-source]
  (doseq [aliases? [false true], mode [:query :explain :plan]]
    (testing (str "cursor request envelope, aliases=" aliases? ", " mode)
      (let [info (row-descriptor :string)
            acquired (atom nil)
            plans (atom {})
            opened (atom [])
            common [:spi-version :features :query-id :authorization :consistency :control :limits]
            observed
            (observe (make-source info [[1 "a"] [2 "b"] [3 "b"]])
              (fn [phase request]
                (case phase
                  :acquisition (reset! acquired request)
                  :planned (swap! plans assoc (get-in request [:request :source-symbol]) request)
                  :open
                  (let [{:keys [alternative-id plan source-symbol definition-fingerprint]} request
                        planned (get @plans source-symbol)
                        selected (some #(when (= alternative-id (:id %)) %)
                                       (get-in planned [:response :alternatives]))]
                    (swap! opened conj request)
                    (is (= (into (set common) [:alternative-id :plan :source-symbol :definition-fingerprint :scope-id :region-id])
                           (set (keys request))))
                    (is (= (select-keys @acquired common) (select-keys request common)))
                    (is (identical? (:control @acquired) (:control request)))
                    (is (= (:definition-fingerprint info) definition-fingerprint))
                    (is (some? selected))
                    (is (identical? (:plan selected) plan))
                    (is (= {'?name "b"} (get-in planned [:request :bindings]))))
                  nil)))
            q (cond-> '{:find [?id] :in [$a ?name]
                        :where [[$a ?id :row/value ?name] [(not= ?id 1)]]
                        :query-source {:authorization {:principal-id :cursor-test :policy-token "policy"}
                                       :limits {:batch-rows 1 :in-flight-batches 1}}}
                aliases? (assoc :in '[$a $b ?name]
                               :where '[[$a ?id :row/value ?name] [$b ?id :row/value ?name] [(not= ?id 1)]]))
            inputs (conj (vec (repeat (if aliases? 2 1) (:source observed))) "b")]
        (try
          (case mode
            :query (is (= #{[2] [3]} (apply d/q q inputs)))
            :explain (is (= 2 (:result-rows (apply d/explain {:run? true} q inputs))))
            :plan (is (false? (:executed? (apply d/explain {:run? false} q inputs)))))
          (is (= (if aliases? #{'$a '$b} #{'$a}) (set (keys @plans))))
          (is (= (if (= :plan mode) 0 (if aliases? 2 1)) (count @opened)))
          (finally (closed! observed)))))))

(defn explanation!
  "Keep advisory estimates, consistency and actual results separate."
  [make-source]
  (let [secret "private explanation annotation"
        unknown (fixture/unknown-estimate false)
        numeric (remove #{:confidence :provenance} (keys unknown))
        known (merge (zipmap numeric (repeat 20))
                     {:confidence :high :provenance {:kind :heuristic :id nil
                                                    :observed-at-ms nil :valid-for-ms nil}})
        zero-estimate (merge known (zipmap numeric (repeat 0)))
        q '{:find [?id] :in [$a] :where [[$a ?id :row/value ?value] [$a ?id :row/value ?other]]}
        estimates (fn [estimate response]
                    (update response :alternatives
                      (fn [alternatives]
                        (mapv #(assoc % :estimate estimate :explain {:label secret}) alternatives))))
        make #(make-source (row-descriptor :string) [[1 "a"] [2 "b"] [3 "c"]])]
    (doseq [estimate [unknown (assoc unknown :rows 200) zero-estimate known]
            aliases? [false true], run? [false true]]
      (let [observed (observe (make) (fn [_ _]) {:planning (partial estimates estimate)})
            query (cond-> q aliases? (assoc :in '[$a $b]
                                           :where (conj (:where q) '[$b ?id :row/value ?value])))
            result (apply d/explain {:run? run?} query (repeat (if aliases? 2 1) (:source observed)))
            selected (mapcat :steps (:regions result))]
        (is (= (if aliases? #{'$a '$b} #{'$a}) (set (map :source (:sources result)))))
        (is (= (if aliases? 3 2) (count selected)))
        (is (every? #(= estimate (:estimate %)) selected))
        (is (every? #(= :snapshot (:consistency %)) (:sources result)))
        (if run?
          (do (is (= 3 (:result-rows result)))
              (is (every? #(= 3 (get-in % [:metrics :rows])) (:cursors result))))
          (do (is (= [] (:cursors result)))
              (is (not (contains? result :result-rows)))))
        (is (not (.contains (pr-str result) secret)))
        (closed! observed)))
    (doseq [limits [{:result-rows 1} {:batch-bytes 1}]]
      (let [observed (observe (make) (fn [_ _]) {:planning (partial estimates zero-estimate)})
            error (error-data #(d/explain {:run? true} (assoc q :query-source {:limits limits}) (:source observed)))
            [category maximum] (first limits)]
        (is (= :query-source/limit (:error error)))
        (is (= category (:category error)))
        (is (= maximum (:maximum error)))
        (is (< maximum (:attempted error)))
        (closed! observed)))
    (doseq [estimate [(assoc known :rows -1) (assoc known :row-bytes ##NaN)
                     (assoc known :confidence :invalid) (dissoc known :run-ns)]
            run? [false true]]
      (let [observed (observe (make) (fn [_ _]) {:planning (partial estimates estimate)})
            error (error-data #(d/explain {:run? run?} q (:source observed)))]
        (is (= :query-source/invalid (:error error)))
        (is (not-any? #(= :open-cursor (first %)) @(:events observed)))
        (closed! observed)))
    (doseq [requested [:snapshot :statement :best-effort]]
      (let [observed (observe (make) (fn [_ _]) {:session-info #(assoc % :consistency :snapshot)})
            result (d/explain {} (assoc q :query-source {:consistency requested}) (:source observed))]
        (is (= requested (get-in result [:sources 0 :requested-consistency])))
        (is (= :snapshot (get-in result [:sources 0 :consistency])))
        (closed! observed)))
    (let [provider (make)
          observed (observe provider (fn [phase _]
                                       (when (= :fetch phase)
                                         (throw (ex-info secret {:error :query-source/snapshot-lost})))))
          error (error-data #(d/explain {:run? true} q (:source observed)))]
      (is (= :query-source/snapshot-lost (:error error)))
      (is (= :fetch (:phase error)))
      (is (= '$a (:source-symbol error)))
      (is (= 0 (:region-id error)))
      (is (not (.contains (pr-str error) secret)))
      (closed! observed)
      (let [fresh (observe provider)]
        (is (= 3 (:result-rows (d/explain {:run? true} q (:source fresh)))))
        (closed! fresh)))))

(defn coverage-explanation!
  "Core predicates remain visible and execute before grouping/windows."
  [make-source]
  (let [secret "private residual binding"
        rows [[1 5] [2 20] [3 30] [4 40] [4 40]]
        ceilings [[1 0] [2 25] [3 0] [4 35]]
        q '{:find [?id] :in [$a $b ?minimum ?secret]
            :where [[$a ?id :row/value ?score] [$a ?id :row/value ?again] [$b ?id :row/value ?ceiling]
                    [(> ?score ?ceiling)] [(>= ?score ?minimum)] [(not= ?secret "excluded residual value")]]}
        conn (d/create-conn nil {:threshold {:db/valueType :db.type/long}} {:kv-opts {:inmemory? true}})]
    (try
      (d/transact! conn (mapv (fn [[id ceiling]] [:db/add id :threshold ceiling]) ceilings))
      (doseq [native? [false true], shape [:rows :aggregate :window], run? [false true]]
        (let [planned (atom [])
              hook (fn [phase payload] (when (= :planned phase) (swap! planned conj (:request payload))))
              a (observe (make-source (row-descriptor :long) rows) hook)
              b (when-not native? (observe (make-source (row-descriptor :long) ceilings) hook))
              query (cond-> q
                      native? (assoc-in [:where 2] '[$b ?id :threshold ?ceiling])
                      (= :aggregate shape) (assoc :find '[(count ?id) .])
                      (= :window shape) (assoc :limit 1 :offset 1))
              inputs [(:source a) (if native? @conn (:source b)) 10 secret]
              result (apply d/explain {:run? run?} query inputs)
              steps (mapcat :steps (:regions result))
              operators (mapcat :operators (:regions result))]
          (is (= (if native? #{'$a} #{'$a '$b}) (set (map :source (:sources result)))))
          (is (= #{0 1} (reduce into #{} (map :covers (filter #(= '$a (:source %)) steps)))))
          (is (= 3 (count (filter #(= :predicate (:kind %)) operators))))
          (when native? (is (some #(= :native (:kind %)) operators)))
          ;; Private bindings and cross-source predicates never enter a source's region request.
          (is (every? #(not-any? (fn [c] (= 3 (:id c))) (:clauses %)) @planned))
          (when run? (is (= (if (= :rows shape) 2 1) (:result-rows result))))
          (is (not (.contains (pr-str result) secret)))
          (is (not (.contains (pr-str result) "excluded residual value")))
          (let [actual (apply d/q query inputs)
                expected (apply d/q query (eager-arguments query [rows (if native? @conn ceilings) 10 secret]))]
            (case shape
              :rows (is (= #{[3] [4]} expected actual))
              :aggregate (is (= 2 expected actual))
              :window (do (is (= 1 (count expected) (count actual)))
                          (is (every? #{[3] [4]} actual)))))
          (run! closed! (cond-> [a] b (conj b)))))
      (finally (d/close conn)))))

(defrecord MetadataSession [provider delegate version info-reads]
  spi/IQuerySourceSession
  (-session-info [_]
    (swap! info-reads inc)
    (with-meta (assoc (spi/-session-info delegate) :data-version version
                     :test/private "private version annotation")
      {:private "private version annotation"}))
  (-open-cursor [_ request] (spi/-open-cursor delegate request))
  (-cancel-session! [_ reason] (spi/-cancel-session! delegate reason))
  (-close-session! [_] (spi/-close-session! delegate)))

(defn metadata-explanation!
  "Pin public definition/data identities across aliases and fragments while
  the provider publishes new definitions or rows for later invocations."
  [make-source]
  (let [private {:private "private version annotation"}
        definition-id (with-meta [:orders (with-meta 'catalog private)] private)
        token (fn [position]
                (with-meta
                  {:provider/epoch 7 :positions (with-meta [position] private)
                   :shards (with-meta #{(with-meta 'east private)} private)
                   :history (with-meta (list position) private)} private))
        rows [[1 "private row payload"] [2 "private row payload"]]]
    (doseq [aliases? [false true], run? [false true]
            phase (if run? [:acquired :planning :fetch] [:acquired :planning])]
      (testing (str "metadata, aliases=" aliases? ", run=" run? ", refresh=" phase)
        (let [base (assoc (row-descriptor :string) :definition-id definition-id)
              snapshots
              (mapv (fn [[fingerprint data-version rows revision counter]]
                      (let [info (with-meta
                                   (assoc base :definition-fingerprint fingerprint
                                          :test/schema-revision revision :test/update-counter counter
                                          :test/private "private version annotation") private)]
                        {:info info :version data-version :row-count (count rows)
                         :provider (make-source info rows)}))
                    [["metadata:v1" (token 10) (subvec rows 0 1) 17 1]
                     ["metadata:v1" (token 20) rows 17 1] ; row-only refresh
                     ["metadata:v2" (token 20) rows 18 2] ; definition refresh
                     ["metadata:v2" (token 20) rows 99 3]]) ; counter-only refresh
              current (atom 0) expected (atom nil) advanced? (atom false)
              descriptor-reads (atom 0) info-reads (atom 0)
              provider
              (reify spi/IQuerySource
                (-source-info [_]
                  (swap! descriptor-reads inc)
                  (:info (snapshots @current)))
                (-open-session [_ request]
                  (let [{:keys [info provider version]} (snapshots @current)]
                    (when-not (every? #(= info %) (:source-descriptors request))
                      (value/fail! :query-source/stale-definition :acquisition))
                    (->MetadataSession provider (spi/-open-session provider request) version info-reads)))
                (-plan-source [_ session request]
                  ;; Plan against the acquired provider even after publication
                  ;; replaces the source's current metadata/rows.
                  (spi/-plan-source (:provider session) (:delegate session) request)))
              observed
              (observe provider
                (fn [at request]
                  (case at
                    :planning (is (= (:info @expected) (:source-descriptor request)))
                    :open (is (= (get-in @expected [:info :definition-fingerprint])
                                 (:definition-fingerprint request)))
                    nil)
                  (when (and (= at phase) (compare-and-set! advanced? false true))
                    (swap! current #(min 3 (inc (long %)))))))
              q (cond-> '{:find [?id ?name] :in [$a]
                          :where [[$a ?id :row/value ?name] [$a ?id :row/value ?other]]}
                  aliases? (assoc :in '[$a $b]
                                  :where '[[$a ?id :row/value ?name] [$a ?id :row/value ?other] [$b ?id :row/value ?name]]))
              inputs (repeat (if aliases? 2 1) (:source observed))
              explanations
              (mapv
                (fn [n]
                  (let [snapshot (snapshots n)]
                    (reset! expected snapshot)
                    (reset! advanced? false)
                    (let [result (apply d/explain {:run? run?} q inputs)]
                      (is (= (inc (long n)) @descriptor-reads @info-reads))
                      (is (= (min 3 (inc (long n))) @current))
                      (is (= (if aliases? 2 1) (count (:sources result))))
                      (doseq [s (:sources result)]
                        (is (= definition-id (:definition-id s)))
                        (is (= (get-in snapshot [:info :definition-fingerprint]) (:definition-fingerprint s)))
                        (is (= (:version snapshot) (:data-version s)))
                        (is (not-any? #(contains? s %)
                              [:schema-revision :definition-version :schema-fingerprint
                               :session-id :authorization-token :test/schema-revision
                               :test/update-counter :test/private])))
                      (when run? (is (= (:row-count snapshot) (:result-rows result))))
                      (is (not (.contains (binding [*print-meta* true] (pr-str result)) "private")))
                      (closed! observed)
                      result))) (range 4))]
          ;; Old explanations retain the old view after all later refreshes.
          (is (= ["metadata:v1" "metadata:v1" "metadata:v2" "metadata:v2"]
                 (mapv #(get-in % [:sources 0 :definition-fingerprint]) explanations)))
          (is (= [(token 10) (token 20) (token 20) (token 20)]
                 (mapv #(get-in % [:sources 0 :data-version]) explanations)))
          (when-not run?
            (is (not-any? #(= :open-cursor (first %)) @(:events observed)))))))
    (doseq [run? [false true]]
      (let [observed (observe (make-source (row-descriptor :string) rows))
            result (d/explain {:run? run?} '[:find ?id :in $s :where [$s ?id :row/value _]] (:source observed))]
        (is (contains? (first (:sources result)) :data-version))
        (is (uuid? (get-in result [:sources 0 :data-version])))
        (closed! observed)))
    (let [version (with-meta {:offsets (vec (range 1024))} private)
          usage (atom nil)
          observed (observe (make-source (row-descriptor :string) rows)
                     (fn [phase request]
                       (when (= phase :planning) (reset! usage (control/usage (:control request)))))
                     {:session-info #(assoc % :data-version version)})
          result (d/explain {:run? false} '[:find ?id :in $s :where [$s ?id :row/value _]] (:source observed))]
      (is (= version (get-in result [:sources 0 :data-version])))
      ;; The session envelope has been released by planning, but the captured
      ;; display value must still have a reservation of its own.
      (is (<= (value/value-charge version) (:retained-bytes @usage)))
      (is (not (.contains (binding [*print-meta* true] (pr-str result)) "private")))
      (closed! observed))
    (let [observed (observe (make-source (row-descriptor :string) rows) (fn [_ _])
                           {:session-info #(assoc % :data-version (vec (range 100000)))})
          error (error-data #(d/explain {:run? false}
                               '{:find [?id] :in [$s] :where [[$s ?id :row/value _]]
                                 :query-source {:limits {:retained-bytes 524288}}}
                               (:source observed)))]
      (is (= :query-source/limit (:error error)))
      (is (= :retained-bytes (:category error)))
      (is (= 524288 (:maximum error)))
      (is (= 1 (count @(:controls observed))))
      (is (not-any? #(#{:plan :open-cursor} (first %)) @(:events observed)))
      (closed! observed))))

(defn execution-explanation!
  "Reconcile v2 cursor metrics with adapter events and shared operator counts."
  [make-source]
  (doseq [scenario [:single :fragments :alias :cross :input :aggregate :empty-input :empty-source :limit]
          run? [false true], intermediate? [false true], batch-size [1 4]]
    (let [rows (if (= :empty-source scenario) [] [[1 "a"] [2 "b"] [3 "b"]])
          observed (observe (make-source (row-descriptor :string) rows))
          provider (:source observed)
          base '{:find [?id] :in [$a] :where [[$a ?id :row/value ?value]]}
          [query inputs]
          (case scenario
            :fragments [(assoc base :where '[[$a ?id :row/value ?value] [$a ?id :row/value ?other]]) [provider]]
            :alias [(assoc base :in '[$a $b] :where '[[$a ?id :row/value ?value] [$b ?id :row/value ?other]]) [provider provider]]
            :cross [(assoc base :in '[$a $b] :where '[[$a ?id :row/value ?value] [$b ?other :row/value _]]) [provider provider]]
            (:input :empty-input) [(assoc base :in '[$a [[?id]]]) [provider (if (= :input scenario) [[1] [2] [3]] [])]]
            :aggregate [(assoc base :find '[?value (count ?id)]) [provider]]
            :limit [(assoc base :limit 1) [provider]]
            [base [provider]])
          query (assoc query :query-source {:limits {:batch-rows batch-size :spill-bytes 0}})
          result (apply d/explain {:run? run? :intermediate-counts? intermediate?} query inputs)
          operators (mapcat :operators (:regions result))
          selected (set (map (juxt :source :region-id :alternative-id) (mapcat :steps (:regions result))))
          batches (filter #(= :batch (first %)) @(:events observed))]
      (is (= :memory (get-in result [:operators :storage])))
      (is (= :reject (get-in result [:operators :spill-policy])))
      (is (= :none (get-in result [:operators :replay-spools])))
      (is (= (= :aggregate scenario) (get-in result [:operators :grouping])))
      (if run?
        (do
          (is (= (case scenario (:empty-input :empty-source) 0 :limit 1 :aggregate 2 3) (:result-rows result)))
          (is (= (count batches) (reduce + 0 (map #(get-in % [:metrics :batches]) (:cursors result)))))
          (is (= (reduce + 0 (map second batches)) (reduce + 0 (map #(get-in % [:metrics :rows]) (:cursors result)))))
          (is (= (reduce + 0 (map #(nth % 2) batches)) (reduce + 0 (map #(get-in % [:metrics :charged-bytes]) (:cursors result)))))
          (is (every? #(selected ((juxt :source-symbol :region-id :alternative-id) %)) (:cursors result)))
          (is (every? #(zero? (get-in % [:metrics :external-requests])) (:cursors result)))
          (is (every? #(= intermediate? (contains? % :actual)) (:cursors result)))
          (when (= :empty-input scenario) (is (empty? (:cursors result))))
          (when (= :limit scenario) (is (= 3 (get-in result [:cursors 0 :metrics :rows]))))
          (when-not intermediate? (is (not-any? :actual operators))))
        (do (is (not (contains? result :result-rows)))
            (is (= [] (:cursors result)))
            (is (not-any? :actual operators))))
      (closed! observed))))

(defn diagnostic-boundary!
  "Keep private provider annotations and error text out of all diagnostics;
  retain safe IDs, unknown metrics, limit details, and primary failures."
  [make-source]
  (let [secret "private SQL credential binding and row payload"
        annotate #(with-meta (assoc % :test/private secret) {:private secret})
        ids (atom {})
        public-id (fn [id]
                    (let [public (str "public:" id)] (swap! ids assoc public id) public))
        envelopes {:descriptor #(with-meta % {:private secret}) :session-info annotate
                   :planning (fn [response]
                               (annotate (update response :alternatives
                                           #(mapv (fn [a] (-> a annotate
                                                              (update :plan (fn [plan] {:delegate plan :private secret}))
                                                              (assoc :id (public-id (:id a))
                                                                     :explain {:label secret}))) %))))
                   :cursor-request #(-> % (update :alternative-id @ids) (update :plan :delegate))
                   :metrics #(annotate (assoc % :read-rows nil :external-requests nil))}
        source-symbol (with-meta '$a {:private secret})
        q {:find '[?id] :in [source-symbol] :where [[source-symbol '?id :row/value '?value]]}
        make #(make-source (annotate (row-descriptor :string)) [[1 secret] [2 "two"] [3 "three"]])
        visible? #(.contains (binding [*print-meta* true] (pr-str %)) secret)]
    (doseq [run? [false true]]
      (let [observed (observe (make) (fn [_ _]) envelopes)
            result (d/explain {:run? run?} q (:source observed))]
        (is (not (visible? result)))
        (is (every? #(.startsWith ^String (:alternative-id %) "public:")
                    (mapcat :steps (:regions result))))
        (when run?
          (is (= 3 (:result-rows result)))
          (is (nil? (get-in result [:cursors 0 :metrics :read-rows])))
          (is (nil? (get-in result [:cursors 0 :metrics :external-requests]))))
        (closed! observed)))
    (doseq [run? [false true], phase (if run? [:planning :fetch] [:planning])]
      (let [observed (observe (make)
                       (fn [at _]
                         (when (or (= at phase) (#{:closed-cursor :closed-session} at))
                           (throw (ex-info secret (annotate {:phase secret :alternative-id secret})
                                           (Exception. secret)))))
                       envelopes)
            error (try (d/explain {:run? run?} q (:source observed)) nil
                       (catch clojure.lang.ExceptionInfo e e))]
        (is (= {:error :query-source/provider :phase phase :source-symbol '$a :region-id 0}
               (select-keys (ex-data error) [:error :phase :source-symbol :region-id])))
        (when error
          (is (= (if (= :fetch phase) 2 1) (count (.getSuppressed ^Throwable error))))
          (is (not (visible? (Throwable->map error))))
          (doseq [e (cons error (.getSuppressed ^Throwable error))]
            (is (nil? (.getCause ^Throwable e)))
            (is (not (visible? (Throwable->map e))))))
        (closed! observed)))
    (doseq [category [:batch-bytes :retained-bytes :result-rows :result-bytes :external-requests]
            close-fault? [false true]]
      (let [limits (case category
                     :retained-bytes {:retained-bytes 262144}
                     :external-requests {:external-requests 1}
                     {category 1})
            observed
            (observe (make)
              (fn [phase payload]
                (when (and (= :retained-bytes category) (= :fetched phase))
                  ;; Real batch acquisition succeeds; core's row/join state
                  ;; encounters the retained limit on its next reservation.
                  (let [ctl (:control payload)
                        available (- (:retained-bytes (control/limits ctl))
                                     (:retained-bytes (control/usage ctl)))]
                    (spi/reserve! ctl {:retained-bytes available})))
                (when (and (= :external-requests category) (= :fetch phase))
                  ;; Request budget is cumulative, even after release.
                  (spi/release! (spi/reserve! (:control payload) {:external-requests 1})))
                (when (and close-fault? (#{:closed-cursor :closed-session} phase))
                  (throw (ex-info secret {:private secret :source-symbol secret}))))
              envelopes)
            query (assoc q :query-source {:limits (assoc limits :batch-rows 1)})
            error (try (d/explain {:run? true} query (:source observed)) nil
                       (catch clojure.lang.ExceptionInfo e e))
            data (ex-data error)
            terminal? (#{:result-rows :result-bytes} category)
            close-first? (and terminal? close-fault?)]
        ;; Cursor exhaustion/close precedes terminal result admission. A close
        ;; failure is consequently primary if the terminal cap is not reached.
        (if close-first?
          (is (= :query-source/provider (:error data)))
          (do
            (is (= :query-source/limit (:error data)))
            (is (= category (:category data)))
            (is (= (get limits category) (:maximum data)))
            (is (< (:maximum data) (:attempted data)))))
        (when-not terminal?
          (is (= '$a (:source-symbol data)))
          (is (= 0 (:region-id data)))
          (is (.startsWith ^String (:alternative-id data) "public:")))
        (when error
          (is (nil? (.getCause ^Throwable error)))
          (is (= (if close-fault? (if close-first? 1 2) 0) (count (.getSuppressed ^Throwable error))))
          (doseq [e (cons error (.getSuppressed ^Throwable error))]
            (is (not (visible? (Throwable->map e))))
            (is (nil? (.getCause ^Throwable e)))))
        (closed! observed)))
    (doseq [metrics [(fn [_] {:private secret})
                    #(assoc % :rows -1) #(assoc % :read-bytes secret)]]
      (let [observed (observe (make) (fn [_ _]) (assoc envelopes :metrics metrics))
            error (error-data #(d/explain {:run? true} q (:source observed)))]
        (is (= {:error :query-source/invalid :phase :metrics :source-symbol '$a :region-id 0}
               (select-keys error [:error :phase :source-symbol :region-id])))
        (is (not (visible? error)))
        (closed! observed)))))

(defn lifecycle!
  "Exercise public SPI lifetime boundaries without inspecting adapter internals."
  [make-source replace-rows!]
  (explanation! make-source)
  (coverage-explanation! make-source)
  (metadata-explanation! make-source)
  (execution-explanation! make-source)
  (diagnostic-boundary! make-source)
  (cursor-request-contract! make-source)
  (acquisition-boundary! make-source replace-rows!)
  (concurrent-views! make-source replace-rows!)
  (blocking-cancellation! make-source)
  (let [info (row-descriptor :string)
        rows [[1 "old"] [2 "two"] [3 "three"]]
        source (make-source info rows)
        changed? (atom false)
        observed (observe source (fn [phase _]
                                   (when (and (= phase :planning)
                                              (compare-and-set! changed? false true))
                                     (replace-rows! source [[4 "new"]]))))
        q '{:find [?id ?v] :in [$a $b] :where [[$a ?id :row/value ?v] [$b ?id :row/value ?v]]}]
    (compare! q [rows rows] [(:source observed) (:source observed)] [observed])
    (compare! q [[[4 "new"]] [[4 "new"]]] [(:source observed) (:source observed)] [observed]))
  (doseq [phase [:acquisition :planning :open :fetch]]
    (testing (str "failure at " phase)
      (let [observed (observe (make-source (row-descriptor :string) [[1 "x"]])
                             (fn [at _] (when (= at phase)
                                          (throw (ex-info "secret provider detail" {:secret true})))))
            error (error-data #(d/q '[:find ?id :in $s :where [$s ?id :row/value _]] (:source observed)))]
        (is (= :query-source/provider (:error error)))
        (is (= '$s (:source-symbol error)))
        (is (not (contains? error :secret)))
        (closed! observed))))
  (let [observed (observe (make-source (row-descriptor :string) [[1 "x"]]))]
    (d/explain {:run? false} '[:find ?id :in $s :where [$s ?id :row/value _]] (:source observed))
    (is (= 1 (count (filter #(= :plan (first %)) @(:events observed)))))
    (is (not-any? #(= :open-cursor (first %)) @(:events observed)))
    (closed! observed))
  (let [observed (observe (make-source (row-descriptor :string) [[1 "x"]])
                         (fn [phase request]
                           (when (= :fetch phase)
                             (control/cancel! (:control request)
                                              {:error :query-source/cancelled
                                               :phase :execution}))))]
    (is (= :query-source/cancelled
           (:error (error-data #(d/q '[:find ?id :in $s :where [$s ?id :row/value _]]
                                     (:source observed))))))
    (closed! observed))
  (doseq [[options category] [[{:limits {:result-rows 1}} :query-source/limit]
                              [{:limits {:batch-bytes 1}} :query-source/limit]]]
    (let [observed (observe (make-source (row-descriptor :string) [[1 "x"] [2 "y"]]))
          q {:find '[?id] :in '[$s] :where '[[$s ?id :row/value _]] :query-source options}]
      (is (= category (:error (error-data #(d/q q (:source observed))))))
      (closed! observed)))
  (let [observed (observe (make-source (row-descriptor :string)
                                       (mapv #(vector % "x") (range 20))))
        q '{:find [?id] :in [$s] :where [[$s ?id :row/value _]] :limit 1
            :query-source {:limits {:batch-rows 1 :in-flight-batches 1}}}]
    (is (= 1 (count (d/q q (:source observed)))))
    (is (= 20 (reduce + (map second (filter #(= :batch (first %)) @(:events observed))))))
    (closed! observed)))

(defn key-semantics!
  "Pin expected key behavior independently of the eager comparison oracle."
  [make-source]
  (let [dir (u/tmp-dir (str "source-key-conformance-" (random-uuid)))
        ;; Native storage canonicalizes floating zero. Native expectations use
        ;; the stored values, while eager/source fixtures retain both signs.
        conn (d/create-conn dir {:native/value {}
                                :native/zero {:db/valueType :db.type/double}}
                            {:wal? false :background-sampling? false})
        rows [[1 :native/value 1.0M] [2 :native/value 1.00M]
              [3 :native/zero 0.0] [4 :native/zero -0.0]
              [5 :native/value (long 1)] [6 :native/value (int 1)]]
        info (descriptor {:kind :eav :attributes {:native/value {:entity-type :long :value-type :data}
                                                        :native/zero {:entity-type :long :value-type :double}}})
        provider (make-source info rows)]
    (try
      (d/transact! conn (mapv (fn [[id a v]] [:db/add id a v]) rows))
      (let [stored (mapv (juxt :e :a :v) (d/datoms @conn :eav))]
        (doseq [[id attr v] rows
                [binding input] [['?v v] ['[?v] [v]]
                                 ['[?v ...] [v]] ['[[?v]] [[v]]]
                                 ['[?v ...] [v v]] ['[[?v]] [[v] [v]]]]]
          (testing (str "exact input " id " " binding " " (pr-str input))
            (let [observed (observe provider)
                  q {:find '[?id] :in ['$s binding]
                     :where [['$s '?id attr '?v]]}
                  native-result
                  (set (for [[e a value] stored
                              :when (and (= a attr) (QueryKey/equals v value))]
                         [e]))
                  expected (if (#{5 6} id) #{[5] [6]} #{[id]})]
              (doseq [[label source expected]
                      [[:eager rows expected]
                       [:native @conn native-result]
                       [:source (:source observed) expected]]]
                (testing (name label)
                  (is (= expected (d/q q source input)))))
              ;; The same native input join through source-backed execution.
              (let [mixed (-> q
                              (update :in conj '$anchor)
                              (update :where conj '[$anchor 1 _ _]))]
                (is (= native-result (d/q mixed @conn input (:source observed)))))
              (closed! observed))))
        (doseq [shape [{:keys '[value]} {:strs '[value]} {:syms '[value]}]]
          (let [q (merge '{:find [?v] :in [$s] :where [[$s _ _ ?v]]} shape)
                observed (observe provider)]
            (doseq [[label source] [[:eager rows] [:native @conn]
                                    [:source (:source observed)]]]
              (testing (name label)
                (is (= (if (= label :native) 4 5) (count (d/q q source))))))
            (closed! observed)))
        (doseq [input [[[3 -0.0 :wrong] [4 -0.0 :wrong-too]]
                       [[3 0.0 :a] [3 -0.0 :wrong] [4 0.0 :b]]]]
          (let [observed (observe provider)
                q '[:find ?tag :in $s [[?e ?v ?tag]]
                    :where [$s ?e :native/zero ?v]]
                expected (set (for [[e v tag] input, [id a value] stored
                                    :when (and (= e id) (= a :native/zero)
                                               (QueryKey/equals v value))]
                                [tag]))]
            (is (= expected (d/q q stored input) (d/q q @conn input)))
            (let [mixed {:find '[?tag] :in '[$s [[?e ?v ?tag]] $anchor]
                         :where '[[$s ?e :native/zero ?v] [$anchor 1 _ _]]}]
              (is (= expected (d/q mixed @conn input (:source observed)))))
            (closed! observed))))
      (finally (d/close conn) (u/delete-files dir))))
  (let [rows [[1 1] [1 2] [2 2] [3 4]]
        info (row-descriptor :long)
        observed (observe (make-source info rows))]
    (doseq [source [(fixture/row-facts rows) (:source observed)]]
      (is (= #{[1] [2]} (d/q '[:find ?v :in $s :where [$s ?v :row/value ?v]] source))))
    (closed! observed)))
