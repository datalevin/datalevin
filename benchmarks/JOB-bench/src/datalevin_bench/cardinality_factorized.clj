(ns datalevin-bench.cardinality-factorized
  "Datalevin-native exact cardinalities by factor-graph message passing.

  JOB's connected entity-star subsets form acyclic bipartite factor graphs.
  Each entity star is therefore streamed as a local factor, while exact
  multiplicities are passed between stars as unary maps keyed by their shared
  query variables.  This avoids materializing either the complete join or a
  multi-key copy of a large base relation."
  (:require
   [clojure.java.io :as io]
   [clojure.set :as set]
   [clojure.walk :as walk]
   [datalevin-bench.cardinality-oracle :as oracle]
   [datalevin.built-ins :as built-ins]
   [datalevin.core :as d]
   [datalevin.db :as db]
   [datalevin.parser :as dp]
   [datalevin.query :as q]
   [datalevin.query-optimizer :as qo]
   [datalevin.query.execute :as qexec]
   [datalevin.query.plan :as qplan]
   [datalevin.query.resolve :as qresolve]
   [datalevin.query-util :as qu]
   [datalevin.relation :as relation]
   [datalevin.timeout :as timeout]
   [datalevin.util :as u])
  (:import
   [datalevin.utl LRUCache]
   [java.util HashMap Map Map$Entry]
   [java.util.concurrent ConcurrentHashMap ExecutorService Executors]))

(def ^:private stream-batch-size 16384)

(defn- pattern-vars
  [[entity _attr value]]
  (cond-> #{}
    (qu/binding-var? entity) (conj entity)
    (qu/binding-var? value)  (conj value)))

(defn- choose-predicate-owner
  [factor-vars factors predicate]
  (let [vars       (oracle/clause-vars predicate)
        candidates (filterv #(set/subset? vars (factor-vars %)) factors)]
    (cond
      (= 1 (count candidates)) (first candidates)
      (and (empty? vars) (seq factors)) (first factors)
      :else
      (throw
        (ex-info "Factorized oracle requires each predicate to be local to one entity star"
                 {:predicate predicate
                  :variables vars
                  :candidate-factors candidates})))))

(defn factor-problem
  "Build the acyclic factor graph for an explicit logical clause set."
  [analysis entities clauses]
  (let [entities       (set entities)
        factors        (vec (sort-by pr-str entities))
        patterns       (filterv oracle/pattern-clause? clauses)
        predicates     (filterv (complement oracle/pattern-clause?) clauses)
        patterns-by-f  (group-by first patterns)
        factor-vars    (into {}
                             (map (fn [factor]
                                    [factor
                                     (into #{} (mapcat pattern-vars)
                                           (patterns-by-f factor))]))
                             factors)
        predicate-owner
        (into {}
              (map (fn [predicate]
                     [predicate
                      (choose-predicate-owner factor-vars factors predicate)]))
              predicates)
        local-clauses
        (into {}
              (map
                (fn [factor]
                  [factor
                   (filterv
                     (fn [clause]
                       (if (oracle/pattern-clause? clause)
                         (= factor (first clause))
                         (= factor (predicate-owner clause))))
                     clauses)]))
              factors)
        owners
        (reduce
          (fn [result [factor factor-patterns]]
            (reduce
              (fn [result var]
                (update result var (fnil conj #{}) factor))
              result (into #{} (mapcat pattern-vars) factor-patterns)))
          {} patterns-by-f)
        shared-vars    (into #{}
                             (keep (fn [[var var-owners]]
                                     (when (< 1 (count var-owners)) var)))
                             owners)
        factor->vars   (into {}
                             (map (fn [factor]
                                    [factor
                                     (set/intersection shared-vars
                                                       (factor-vars factor))]))
                             factors)
        var->factors   (into {}
                             (map (fn [var]
                                    [var (set/intersection entities
                                                           (owners var))]))
                             shared-vars)
        edge-count     (reduce + (map count (vals factor->vars)))
        node-count     (+ (count factors) (count shared-vars))]
    (when-not (= (set (keys patterns-by-f)) entities)
      (throw (ex-info "Every factorized entity must have a pattern clause"
                      {:entities entities
                       :pattern-entities (set (keys patterns-by-f))})))
    (when (and (< 1 node-count)
               (not= edge-count (dec node-count)))
      (throw
        (ex-info "Factorized exact counter requires an acyclic connected factor graph"
                 {:entities entities
                  :shared-vars shared-vars
                  :factor->vars factor->vars
                  :edges edge-count
                  :nodes node-count})))
    {:analysis       analysis
     :entities       entities
     :factors        factors
     :shared-vars    shared-vars
     :local-clauses  local-clauses
     :factor-vars    factor-vars
     :factor->vars   factor->vars
     :var->factors   var->factors}))

(defn subset-problem
  [analysis entities]
  (let [{:keys [clauses]} (oracle/subset-clauses analysis entities)]
    (factor-problem analysis entities clauses)))

(defn link-input-problem
  [analysis {:keys [entities target] :as request}]
  (let [{:keys [clauses]} (oracle/subset-clauses analysis entities)
        link-clause       (oracle/link-input-clause analysis request)]
    (factor-problem analysis (conj entities target)
                    (conj clauses link-clause))))

(defn- local-query
  [{:keys [analysis local-clauses factor-vars]} factor timeout-ms]
  (let [replacements (:entity-query-vars analysis)
        clauses      (walk/postwalk-replace replacements
                                            (local-clauses factor))
        original     (vec
                       (distinct
                         (concat
                           (when (qu/binding-var? factor) [factor])
                           (sort-by pr-str (factor-vars factor)))))
        original     (if (some #{factor} original)
                       original
                       (into [factor] original))
        symbols      (mapv #(get replacements % %) original)
        form         (vec (concat [:find] symbols [:where] clauses))]
    {:form     (cond-> (dp/query->map form)
                 timeout-ms (assoc :timeout timeout-ms))
     :original original
     :symbols  symbols
     :index    (zipmap original (range))}))

(defn- component-plans
  [{:keys [plan sources]}]
  (vec
    (for [[src components] plan
          plans components]
      [(sources src) (vec (mapcat :steps plans))])))

(defn- reduce-local-query
  "Stream the distinct assignments of one entity-local query. `rf` receives
  the accumulator and a result tuple ordered as `symbols`."
  [db {:keys [form symbols]} plan-cache seed rf init]
  (let [parsed-q (dp/parse-query form)]
    (binding [timeout/*deadline*            (timeout/to-deadline
                                              (:qtimeout parsed-q))
              built-ins/*udf-db*            db
              qo/*cardinality-oracle*       nil
              qplan/*explain*               nil
              q/*cache?*                    false
              q/*plan-cache*                plan-cache
              u/*reservoir-sampling-seed*   (long seed)]
      (let [{:keys [late-clauses rels result-set sources] :as context}
            (qexec/plan-context* parsed-q [db])
            plans (component-plans context)]
        (cond
          (= result-set #{}) init

          (seq rels)
          (throw (ex-info "Local factor query unexpectedly produced input relations"
                          {:relation-count (count rels) :query form}))

          (not= 1 (count plans))
          (throw (ex-info "Local factor query must have one connected plan"
                          {:component-count (count plans) :query form}))

          :else
          (let [[source steps] (first plans)
                attrs          (qplan/step-attrs steps)]
            (qplan/reduce-step-batches
              source steps stream-batch-size
              (fn [acc tuples]
                (let [resolved
                      (if (seq late-clauses)
                        (binding [qu/*implicit-source* (get sources '$)]
                          (reduce qresolve/resolve-clause
                                  (assoc context
                                         :rels [(relation/relation! attrs
                                                                    tuples)])
                                  late-clauses))
                        (assoc context
                               :rels [(relation/relation! attrs tuples)]))]
                  (reduce rf acc (qexec/-collect resolved symbols))))
              init)))))))

(defn- multiply-exact
  ^long [^long left ^long right]
  (Math/multiplyExact left right))

(defn- add-exact
  ^long [^long left ^long right]
  (Math/addExact left right))

(defn- map-add!
  [^HashMap result key ^long weight]
  (when (pos? weight)
    (if-some [old (.get result key)]
      (.put result key (add-exact (long old) weight))
      (.put result key weight)))
  result)

(defn- pointwise-product
  [messages]
  (let [messages (vec (remove nil? messages))]
    (cond
      (empty? messages) nil
      (= 1 (count messages)) (first messages)
      :else
      (let [ordered (vec (sort-by #(.size ^Map %) messages))
            ^Map smallest (first ordered)
            others (next ordered)
            result (HashMap. (max 16 (* 2 (.size smallest))))]
        (doseq [^Map$Entry entry (.entrySet smallest)]
          (let [key (.getKey entry)]
            (loop [weight (long (.getValue entry))
                   maps   others]
              (if-let [^Map message (first maps)]
                (when-some [value (.get message key)]
                  (recur (multiply-exact weight (long value)) (next maps)))
                (map-add! result key weight)))))
        result))))

(defn- sum-pointwise-product
  ^long [messages]
  (let [ordered      (vec (sort-by #(.size ^Map %) messages))
        ^Map smallest (first ordered)
        others       (next ordered)]
    (reduce
      (fn [^long total ^Map$Entry entry]
        (let [key (.getKey entry)]
          (loop [weight (long (.getValue entry))
                 maps   others]
            (if-let [^Map message (first maps)]
              (if-some [value (.get message key)]
                (recur (multiply-exact weight (long value)) (next maps))
                total)
              (add-exact total weight)))))
      0
      (.entrySet smallest))))

(defn- factor-signature
  [{:keys [local-clauses]} factor]
  [factor (local-clauses factor)])

(declare factor-subtree-key variable-subtree-key)

(defn- factor-subtree-key
  [{:keys [factor->vars] :as problem} factor parent-var]
  [:factor
   (factor-signature problem factor)
   parent-var
   (mapv #(variable-subtree-key problem % factor)
         (sort-by pr-str (disj (factor->vars factor) parent-var)))])

(defn- variable-subtree-key
  [{:keys [var->factors] :as problem} var parent-factor]
  [:variable
   var
   (mapv #(factor-subtree-key problem % var)
         (sort-by pr-str (disj (var->factors var) parent-factor)))])

(defn make-backend
  ([db analysis]
   (make-backend db analysis {}))
  ([db analysis {:keys [message-cache-max-entries
                        message-cache-max-message-entries]
                 :or   {message-cache-max-entries 1000000
                        message-cache-max-message-entries 250000}}]
   {:db                  db
    :analysis            analysis
    :message-cache       (ConcurrentHashMap.)
    :message-cache-size  (atom 0)
    :message-cache-max-entries message-cache-max-entries
    :message-cache-max-message-entries message-cache-max-message-entries
    :plan-cache          (LRUCache. 256)
    :stats               (atom {:factor-scans 0
                                :assignments 0
                                :point-count-probes 0
                                :message-cache-hits 0
                                :message-cache-misses 0
                                :message-cache-skips 0
                                :largest-message 0})}))

(defn backend-stats
  [backend]
  (assoc @(:stats backend)
         :cached-messages (.size ^ConcurrentHashMap (:message-cache backend))
         :cached-message-entries @(:message-cache-size backend)))

(defn- cache-message!
  [{:keys [message-cache message-cache-size message-cache-max-entries
           message-cache-max-message-entries stats]}
   key ^Map message]
  (let [size (.size message)]
    (if (or (> size (long message-cache-max-message-entries))
            (> (+ (long @message-cache-size) size)
               (long message-cache-max-entries)))
      (swap! stats update :message-cache-skips inc)
      (let [value    (delay message)
            existing (.putIfAbsent ^ConcurrentHashMap message-cache key value)]
        (when-not existing
          (swap! message-cache-size + size)))))
  message)

(declare factor-message variable-message)

(defn- stream-factor
  [{:keys [db plan-cache stats]} problem factor parent-var incoming timeout-ms]
  (let [{:keys [index] :as query} (local-query problem factor timeout-ms)
        parent-idx (when parent-var (index parent-var))
        incoming   (mapv (fn [[var message]] [(index var) message]) incoming)
        result     (when parent-var (HashMap.))
        total      (volatile! 0)
        assignments (volatile! 0)
        seed       (bit-and Long/MAX_VALUE
                            (hash [(:form query) parent-var]))]
    (when (and parent-var (nil? parent-idx))
      (throw (ex-info "Parent variable is absent from local factor query"
                      {:factor factor :parent-var parent-var :query query})))
    (reduce-local-query
      db query plan-cache seed
      (fn [state tuple]
        (vswap! assignments inc)
        (loop [weight 1
               inputs incoming]
          (if-let [[idx ^Map message] (first inputs)]
            (if-some [value (.get message (nth tuple idx))]
              (recur (multiply-exact weight (long value)) (next inputs))
              state)
            (do
              (if parent-var
                (map-add! result (nth tuple parent-idx) weight)
                (vswap! total add-exact weight))
              state))))
      nil)
    (swap! stats
           (fn [s]
             (-> s
                 (update :factor-scans inc)
                 (update :assignments + @assignments)
                 (update :largest-message max (if result (.size result) 0)))))
    (if parent-var result @total)))

(defn- variable-message
  [backend {:keys [var->factors] :as problem} var parent-factor timeout-ms]
  (pointwise-product
    (mapv #(factor-message backend problem % var timeout-ms)
          (sort-by pr-str (disj (var->factors var) parent-factor)))))

(defn- factor-message
  [{:keys [message-cache stats] :as backend}
   {:keys [factor->vars] :as problem} factor parent-var timeout-ms]
  (let [key      (factor-subtree-key problem factor parent-var)
        existing (.get ^ConcurrentHashMap message-cache key)]
    (if existing
      (do
        (swap! stats update :message-cache-hits inc)
        @existing)
      (do
        (swap! stats update :message-cache-misses inc)
        (let [incoming
              (into {}
                    (map (fn [var]
                           [var (variable-message backend problem var factor
                                                  timeout-ms)]))
                    (sort-by pr-str
                             (disj (factor->vars factor) parent-var)))
              message
              (stream-factor backend problem factor parent-var incoming
                             timeout-ms)]
          (cache-message! backend key message))))))

(defn count-problem
  [backend {:keys [factors shared-vars var->factors] :as problem} timeout-ms]
  (if (seq shared-vars)
    (let [root
          (first
            (sort-by
              (fn [var]
                [(- (count (var->factors var))) (pr-str var)])
              shared-vars))
          messages
          (mapv #(factor-message backend problem % root timeout-ms)
                (sort-by pr-str (var->factors root)))]
      (sum-pointwise-product messages))
    (long (stream-factor backend problem (first factors) nil {} timeout-ms))))

(defn factorized-subset-count
  [backend entities timeout-ms]
  (count-problem backend (subset-problem (:analysis backend) entities)
                 timeout-ms))

(defn factorized-link-input-count
  [backend request timeout-ms]
  (let [{:keys [target]} request
        {:keys [factor->vars local-clauses] :as problem}
        (link-input-problem (:analysis backend) request)
        target-vars (factor->vars target)
        target-clauses (local-clauses target)]
    (if (and (= 1 (count target-vars))
             (= 1 (count target-clauses)))
      (let [link-var       (first target-vars)
            [_ attr _]     (first target-clauses)
            ^Map prefix    (variable-message backend problem link-var target
                                             timeout-ms)
            probes         (.size prefix)]
        (swap! (:stats backend) update :point-count-probes + probes)
        (binding [timeout/*deadline* (timeout/to-deadline timeout-ms)]
          (let [^longs ticks (long-array 1)]
            (reduce
              (fn [^long total ^Map$Entry entry]
                (let [i (aget ticks 0)]
                  (when (zero? (bit-and 4095 i))
                    (timeout/assert-time-left))
                  (aset ticks 0 (unchecked-inc i)))
                (let [value  (.getKey entry)
                      n      (long (db/-count (:db backend)
                                              [nil attr value]))
                      weight (long (.getValue entry))]
                  (add-exact total (multiply-exact weight n))))
              0
              (.entrySet prefix)))))
      (count-problem backend problem timeout-ms))))

(defn- checked-counter
  [expected kind query-name f]
  (fn [& args]
    (let [key    (case kind
                   :subset (set (nth args 2))
                   :material (oracle/material-request-key (nth args 2)))
          actual (apply f args)]
      (when (and expected (contains? expected key)
                 (not= (long (expected key)) (long actual)))
        (throw (ex-info "Datalevin factorized count disagrees with checkpoint"
                        {:query query-name
                         :kind kind
                         :key key
                         :expected (expected key)
                         :actual actual})))
      actual)))

(defn run
  [{:keys [db-path output-dir expected-dir queries timeout-ms parallelism
           material-cardinalities? message-cache-max-entries
           message-cache-max-message-entries]
    :or   {db-path "db"
           output-dir "results/cidr-exact-cardinalities-factorized"
           timeout-ms 600000
           parallelism 1
           material-cardinalities? false}}]
  (when-not (and (integer? parallelism) (pos? parallelism))
    (throw (ex-info "Factorized-oracle parallelism must be a positive integer"
                    {:parallelism parallelism})))
  (let [conn     (d/get-conn db-path)
        database (d/db conn)
        executor (Executors/newFixedThreadPool (int parallelism))]
    (try
      (let [query-symbols (oracle/selected-query-symbols queries)
            shared-file  (io/file output-dir
                                  (if material-cardinalities?
                                    "shared.material.edn"
                                    "shared.edn"))
            shared-counts (atom (oracle/read-shared-checkpoint shared-file))]
        (io/make-parents shared-file)
        (with-open [shared-writer (io/writer shared-file :append true)]
          (doseq [query-sym query-symbols]
            (let [name       (oracle/query-name query-sym)
                  analysis   (oracle/query-analysis
                               (oracle/query-value query-sym))
                  backend    (make-backend
                               database analysis
                               (cond-> {}
                                 message-cache-max-entries
                                 (assoc :message-cache-max-entries
                                        message-cache-max-entries)
                                 message-cache-max-message-entries
                                 (assoc :message-cache-max-message-entries
                                        message-cache-max-message-entries)))
                  logical-file (io/file output-dir (str name ".edn"))
                  expected-logical
                  (when expected-dir
                    (oracle/read-checkpoint
                      (io/file expected-dir (str name ".edn"))))
                  expected-material
                  (when expected-dir
                    (oracle/read-material-checkpoint
                      (io/file expected-dir (str name ".material.edn"))))
                  count-subset
                  (checked-counter
                    expected-logical :subset name
                    (fn [_db _analysis entities per-query-timeout-ms _known]
                      (factorized-subset-count backend entities
                                               per-query-timeout-ms)))
                  count-material
                  (checked-counter
                    expected-material :material name
                    (fn [_db _analysis request per-query-timeout-ms _known]
                      (factorized-link-input-count backend request
                                                  per-query-timeout-ms)))]
              (if material-cardinalities?
                (oracle/precompute-material-query!
                  database query-sym
                  {:logical-file logical-file
                   :output-file (io/file output-dir
                                         (str name ".material.edn"))
                   :timeout-ms timeout-ms
                   :shared-counts shared-counts
                   :shared-writer shared-writer
                   :executor executor
                   :count-material count-material
                   :count-method :datalevin-factorized
                   :trusted-shared-only? true})
                (oracle/precompute-query!
                  database query-sym
                  {:output-file logical-file
                   :timeout-ms timeout-ms
                   :shared-counts shared-counts
                   :shared-writer shared-writer
                   :executor executor
                   :count-subset count-subset
                   :count-method :datalevin-factorized}))
              (println name "factorized stats" (backend-stats backend))))))
      (finally
        (.shutdownNow ^ExecutorService executor)
        (d/close conn)))))
