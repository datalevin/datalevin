(ns ^:no-doc datalevin.query.source.fragment
  "Owned boundaries between a native prefix and one parameterized SQL fragment."
  (:require [clojure.set :as set]
            [datalevin.parser :as parser]
            [datalevin.query.source :as source]
            [datalevin.query.source.batch :as batch]
            [datalevin.query.source.contract :as contract]
            [datalevin.query.source.control :as control]
            [datalevin.query.source.diagnostics :as diagnostics]
            [datalevin.query.source.permissions :as permissions]
            [datalevin.query.source.region :as region]
            [datalevin.query.source.relational :as relational]
            [datalevin.query.source.result-window :as window]
            [datalevin.query.source.runtime :as rt]
            [datalevin.query.source.session :as session]
            [datalevin.query.source.value :as value :refer [fail!]]
            [datalevin.spill :as spill])
  (:import [datalevin.parser Constant Pattern Predicate Variable]
           [datalevin.utl QueryKey$KeyMap QueryKey$KeySet]
           [java.io Closeable]))

(defn- ensure! [ok] (when-not ok (fail! :query-source/invalid :planning)))
(defn- retain! [runtime x]
  (let [token (rt/charge-copy! runtime x)]
    (rt/own! runtime #(control/release! token)))
  x)
(defn- reserve! [runtime bytes]
  (let [token (control/reserve! (:control runtime) {:retained-bytes bytes})]
    (rt/own! runtime #(control/release! token))))
(defn- vars [node] (into #{} (map :symbol) (parser/collect-vars-distinct node)))
(defn- fresh [taken stem]
  (first (remove taken (map #(symbol (str "?fragment_" stem "_" %)) (range)))))

(defn- shape [compilation input]
  (let [{:keys [entries compiled prepared]} compilation
        parsed (:parsed compiled) [root] (:regions compiled)
        external (filterv :descriptor entries) native (filterv :native entries)
        patterns (mapv #(assoc (:clause %) :source-symbol (:source-symbol %)) (:patterns root))
        descriptors (into {} (map (juxt :symbol :descriptor)) external)
        native-nodes (filterv #(instance? Pattern %) (map :node (:core-clauses root)))
        native-vars (set/union (set (map :variable (:columns input))) (into #{} (mapcat vars) native-nodes))
        remote-vars (into #{} (mapcat contract/variables) patterns)
        elements (mapv relational/projection (parser/find-elements (:qfind parsed)))
        types (reduce (fn [out {:keys [source-symbol terms] :as clause}]
                        (reduce (fn [out [term c]]
                                  (if (= :var (first term))
                                    (update out (second term) #(if (or (nil? %) (= % (:type c))) (:type c) ::mixed)) out))
                                out (map vector terms (contract/pattern-columns (descriptors source-symbol) clause)))) {} patterns)
        exported (into {} (map (juxt :id identity)) (region/predicate-clauses root remote-vars))
        base (inc (reduce max -1 (map :id patterns)))
        clauses (reduce (fn [out [i {:keys [node]}]]
                          (cond
                            (nil? out) nil
                            (instance? Pattern node) (update out :native conj node)
                            (and (instance? Predicate node) (get exported (+ base i)))
                            (update out :remote conj (get exported (+ base i)))
                            (and (instance? Predicate node) (set/subset? (vars node) native-vars)
                                 (#{'= 'not= '< '<= '> '>= 'nil? 'some?} (get-in node [:fn :symbol]))
                                 (every? #(or (instance? Variable %) (instance? Constant %)) (:args node)))
                            (update out :native conj node)
                            :else nil)) {:native [] :remote patterns} (map-indexed vector (:core-clauses root)))
        aggregate? (boolean (some #(= :aggregate (:kind %)) elements))
        with (mapv :symbol (:qwith parsed))
        native-find (vec (distinct (keep #(when (and (= :variable (:kind %)) (native-vars (:variable %))) (:variable %)) elements)))
        native-with (filterv #(and (native-vars %) (not (remote-vars %))) with)
        boundary (vec (sort (set/difference (set/intersection native-vars remote-vars) (set (keys (:bindings input))))))
        prefix-vars (vec (distinct (concat native-find (filter native-vars with) boundary)))
        taken (set/union native-vars remote-vars)
        group-var (fresh taken "group") witness-var (fresh (conj taken group-var) "witness")
        remote-find (if aggregate?
                      (vec (remove #(and (= :variable (:kind %)) (native-vars (:variable %))) elements))
                      (mapv #(hash-map :kind :variable :variable %)
                            (distinct (remove native-vars (concat (map :variable elements) with)))))
        remote-find (into [{:kind :variable :variable group-var}] (distinct remote-find))
        inputs (into (mapv #(hash-map :variable % :type (types %) :nullable? false) boundary)
                     (cond-> [{:variable group-var :type :long :nullable? false}]
                       (and aggregate? (seq native-with)) (conj {:variable witness-var :type :long :nullable? false})))
        columns (mapv (fn [i element]
                        {:variable (fresh taken (str "column_" i))
                         :type (if (= :variable (:kind element)) (if (= group-var (:variable element)) :long (types (:variable element)))
                                   (if (#{:count :count-distinct} (:operator element)) :long :data))
                         :nullable? (boolean (#{:min :max} (:operator element)))}) (range) remote-find)]
    (when (and (= 1 (count (:groups prepared))) (seq external) (seq native) (seq native-nodes)
               (every? #(satisfies? source/IRelationalFragmentSource (:source %)) external)
               (= 1 (count (:regions compiled))) (empty? (:sites compiled)) (empty? (:rules compiled))
               (not (or (seq (:qhaving parsed)) (seq (:qorder parsed)))) (nil? *math-context*)
               (every? #(or (instance? Pattern %) (instance? Predicate %)) (:qwhere parsed))
               clauses (seq boundary) (seq prefix-vars) (seq patterns) (every? some? elements)
               (every? (set/union native-vars remote-vars) (concat (map :variable elements) with))
               (every? #(or (not= :aggregate (:kind %)) (remote-vars (:variable %))) elements)
               (not-any? #{::mixed} (vals types)))
      {:native-entries native :prefix-vars prefix-vars :native-find native-find :native-with native-with
       :native-vars native-vars :elements elements :aggregate? aggregate? :group-var group-var :witness-var witness-var
       :prefix-query (parser/map->Query {:qfind (parser/->FindRel (mapv parser/->Variable prefix-vars))
                                        :qin [] :qwhere (:native clauses)})
       :provider (:source (first external))
       :request (merge (:request (first (:groups prepared)))
                  {:mode :relational-fragment :semantics-version 1
                   :sources (mapv #(select-keys % [:symbol :descriptor]) external)
                   :clauses (:remote clauses) :bindings (select-keys (:bindings input) remote-vars)
                   :inputs inputs :find remote-find :columns columns
                   :with (if aggregate? (cond-> (filterv remote-vars with)
                                         (seq native-with) (conj witness-var)) [])})})))

(defn- plan! [runtime compilation input]
  (when (and (= 1 (count (get-in compilation [:prepared :groups])))
             (some :native (:entries compilation))
             (every? #(satisfies? source/IRelationalFragmentSource (:source %)) (filter :descriptor (:entries compilation))))
   (let [compiled (:compiled compilation)
        n (+ (count (get-in compiled [:regions 0 :patterns])) (count (get-in compiled [:regions 0 :core-clauses]))
             (count (parser/find-elements (get-in compiled [:parsed :qfind]))) (count (get-in compiled [:parsed :qwith])))
        workspace (control/reserve! (:control runtime) {:retained-bytes (+ 8192 (* 8192 n))})]
    (try
      (when-let [shape (shape compilation input)]
        (let [token (rt/charge-copy! runtime (dissoc (:request shape) :control))]
          (try
            (if-let [response (rt/invoke runtime :planning
                                #(source/-plan-relational-fragment (:provider shape) (:request shape)))]
              (do
                (ensure! (and (value/plain-map? response) (= #{:plan :columns :constraints} (set (keys response)))
                              (value/plain-map? (:plan response)) (= (:columns (:request shape)) (:columns response))
                              (= #{:max-binding-rows :max-binding-bytes} (set (keys (:constraints response))))
                              (every? #(and (integer? %) (<= 1 % Long/MAX_VALUE)) (vals (:constraints response)))))
                (retain! runtime response)
                (rt/own! runtime #(control/release! token))
                (assoc shape :response (value/owned-copy response)))
              (do (control/release! token) nil))
            (catch Throwable e (control/release! token) (throw e)))))
      (finally (control/release! workspace))))))

(defn- prepare-groups! [runtime shape relation]
  (let [{:keys [prefix-vars native-find native-with aggregate? request]} shape
        indices (mapv (:attrs relation) prefix-vars)
        groups (QueryKey$KeyMap.) order (atom [])]
    (doseq [^objects tuple (:tuples relation)]
      (control/check-active! (:control runtime))
      (reserve! runtime 2048)
      (let [values (mapv #(aget tuple (int %)) indices)
            _ (retain! runtime values)
            row (zipmap prefix-vars (value/owned-copy values))
            key (if aggregate? (mapv row native-find) values)]
        (when (every? (fn [{:keys [variable type]}]
                        (value/matches-type? type (row variable)))
                      (take (- (count (:inputs request)) (if (and aggregate? (seq native-with)) 2 1)) (:inputs request)))
          (let [group (or (.get groups key)
                          (let [g {:id (long (count @order)) :host row :seen (QueryKey$KeySet.)
                                   :witnesses (QueryKey$KeyMap.) :bindings (atom [])}]
                            (.put groups key g) (swap! order conj g) g))
                witnesses (:witnesses group) witness (mapv row native-with)
                witness-id (or (.get ^QueryKey$KeyMap witnesses witness)
                               (let [id (long (.size ^QueryKey$KeyMap witnesses))]
                                 (.put ^QueryKey$KeyMap witnesses witness id) id))
                values (mapv (fn [{:keys [variable]}]
                               (cond (= variable (:group-var shape)) (:id group)
                                     (= variable (:witness-var shape)) witness-id
                                     :else (row variable))) (:inputs request))]
            (when (.add ^QueryKey$KeySet (:seen group) values)
              (retain! runtime values)
              (swap! (:bindings group) conj values))))))
    @order))

(defn- batches [shape groups]
  (let [request (:request shape) columns (:inputs request)
        cap (min 256 (get-in request [:limits :binding-rows]) (get-in shape [:response :constraints :max-binding-rows]))
        byte-cap (min (get-in request [:limits :binding-bytes]) (get-in shape [:response :constraints :max-binding-bytes]))
        pack #(mapv (fn [i values] {:binding-id (long i) :values values}) (range) %)
        fits? #(and (<= (count %) cap) (<= (source/binding-charge-bytes columns (pack %)) byte-cap))]
    (loop [groups groups pending [] out []]
      (control/check-active! (:control request))
      (if-let [group (first groups)]
        (let [rows @(:bindings group) combined (into pending rows)]
          (cond (not (fits? rows)) nil
                (fits? combined) (recur (next groups) combined out)
                :else (recur groups [] (conj out (pack pending)))))
        (cond-> out (seq pending) (conj (pack pending)))))))

(defn- describe! [runtime compilation shape groups]
  (when (:diagnostics runtime)
    (let [descriptors (into {} (map (juxt :symbol :descriptor)) (:entries compilation))]
      (diagnostics/sources! runtime
        (mapv #(assoc % :definition-id (:definition-id (descriptors (:source %)))
                       :definition-fingerprint (:definition-fingerprint (descriptors (:source %)))
                       :scope-id (get-in (descriptors (:source %)) [:scope :id])) (session/explain! runtime groups)))
      (diagnostics/region! runtime
        {:program-id (diagnostics/program! runtime) :region-id 0 :policy :relational-fragment-v1 :selection :provider
         :conditional-inputs true
         :steps [{:source-symbol (get-in shape [:request :sources 0 :symbol]) :alternative-id "relational-fragment"
                  :covers (set (map :id (get-in shape [:request :clauses]))) :access {:kind :relational-fragment}
                  :columns (get-in shape [:request :columns])}] :operators []}))))

(defn- close-after! [primary close]
  (try (close)
       (catch Throwable e
         (if primary (when-not (identical? primary e) (.addSuppressed ^Throwable primary e)) (throw e)))))

(defn- consume! [runtime shape group bindings emit!]
  (let [request (:request shape) response (:response shape) descriptor (get-in request [:sources 0 :descriptor])
        open (merge (:context group)
               {:source-symbol (get-in request [:sources 0 :symbol]) :region-id 0
                :scope-id (get-in descriptor [:scope :id]) :definition-fingerprint (:definition-fingerprint descriptor)
                :alternative-id "relational-fragment" :plan (:plan response) :columns (:columns response) :bindings bindings})
        permission (permissions/reserve-open! (:consumption group)
                     {:id "relational-fragment" :scope-id (:scope-id open) :properties {:replay :identical :ordering []}} open)
        close-permission (rt/own! runtime #(.close ^Closeable permission))
        trace (diagnostics/cursor! runtime open true)
        group-index (.indexOf ^java.util.List (mapv :variable (:inputs request)) (:group-var shape))
        allowed (set (map #(nth (:values %) group-index) bindings))
        primary (volatile! nil)]
    (try
      (let [cursor (rt/invoke runtime :open #(source/-open-relational-fragment-cursor (:session group) open))]
        (ensure! (satisfies? source/IQuerySourceCursor cursor))
        (let [close (rt/own! runtime #(source/-close-cursor! cursor))
              fetch {:control (:control runtime) :max-rows (get-in request [:limits :batch-rows])
                     :max-bytes (get-in request [:limits :batch-bytes])}]
          (try
            (loop []
              (let [b (batch/accept! runtime (:columns response) false fetch
                        (rt/invoke runtime :fetch #(source/-next-batch! cursor fetch)) identity)]
                (try
                  (doseq [row (:rows b)]
                    (control/check-active! (:control runtime))
                    (ensure! (contains? allowed (first row)))
                    (emit! row))
                  (diagnostics/batch! runtime trace (:row-count b) (:byte-count b))
                  (finally (control/release! (:reservation b))))
                (when-not (:exhausted? b) (recur))))
            (when (:diagnostics runtime)
              (diagnostics/metrics! runtime trace (rt/invoke runtime :metrics #(source/-cursor-metrics cursor))))
            (catch Throwable e (vreset! primary e) (throw e))
            (finally (close-after! @primary close)))))
      (catch Throwable e (vreset! primary e) (throw e))
      (finally (close-after! @primary close-permission)))))

(defn- close-scope! [runtime]
  (let [actions (reverse (first (swap-vals! (:cleanup runtime) (constantly []))))
        error (volatile! nil)]
    (doseq [action actions]
      (try (action) (catch Throwable e (if-let [first-error @error] (.addSuppressed ^Throwable first-error e)
                                             (vreset! error e)))))
    (when-let [e @error] (throw e))))

(defn try!
  "Compile the native prefix through the shared compiler. Return nil before
  acquisition on decline or an oversized complete aggregate group; never split
  such groups or merge partial aggregates. Pins are owned by the caller."
  [runtime compilation input execute? compile-prefix]
  (when-let [shape (plan! runtime compilation input)]
    (let [prefix-runtime (assoc runtime :cleanup (atom []))
          close (rt/own! runtime #(close-scope! prefix-runtime)) primary (volatile! nil)]
      (try
        (let [relation (compile-prefix prefix-runtime shape execute?)
              groups (when execute? (prepare-groups! prefix-runtime shape relation))
              batches (when execute? (batches shape groups))]
          (when (or (not execute?) (some? batches))
            (let [sessions (session/acquire! runtime (:prepared compilation)) session (first sessions)]
              (ensure! (satisfies? source/IRelationalFragmentSession (:session session)))
              (describe! runtime compilation shape sessions)
              (if-not execute? {:result nil :fragment? true}
                (let [bounded (window/collector! runtime (get-in compilation [:compiled :parsed]))
                      collected (when-not bounded (spill/new-spillable-set nil {:spill-threshold Long/MAX_VALUE}))
                      elements (:elements shape)
                      remote-positions (into {} (map-indexed (fn [i e] [(if (= :variable (:kind e)) (:variable e) e) i]))
                                             (get-in shape [:request :find]))
                      all-vars (into (vec (parser/find-vars (:qfind (get-in compilation [:compiled :parsed]))))
                                     (map :symbol) (get-in compilation [:compiled :parsed :qwith]))
                      emit! (fn [row]
                              (let [id (first row)]
                                (ensure! (and (integer? id) (<= 0 id) (< id (count groups))))
                                (let [host (:host (nth groups (int id)))
                                      output (if (:aggregate? shape)
                                               (mapv (fn [e] (if (and (= :variable (:kind e)) ((:native-vars shape) (:variable e)))
                                                               (get host (:variable e)) (nth row (remote-positions (if (= :variable (:kind e)) (:variable e) e))))) elements)
                                               (mapv #(if ((:native-vars shape) %) (get host %) (nth row (remote-positions %))) all-vars))]
                                  (if bounded ((:add! bounded) output)
                                    (do (retain! runtime output) (reserve! runtime 192)
                                        (.cons ^clojure.lang.IPersistentCollection collected (value/owned-copy output)))))))]
                  (doseq [bindings batches] (consume! runtime shape session bindings emit!))
                  {:fragment? true
                   :result (relational/finish! runtime (get-in compilation [:compiled :parsed])
                             (mapv #(hash-map :variable (symbol (str "?fragment_final_" %))) (range (count elements)))
                             (if bounded ((:rows bounded)) collected) (:aggregate? shape))})))))
        (catch Throwable e (vreset! primary e) (throw e))
        (finally (close-after! @primary close))))))
