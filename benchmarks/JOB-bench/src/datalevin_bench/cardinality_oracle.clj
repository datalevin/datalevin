(ns datalevin-bench.cardinality-oracle
  "Exact logical cardinalities for fixed-search JOB optimizer experiments."
  (:require
   [clojure.edn :as edn]
   [clojure.java.io :as io]
   [clojure.set :as set]
   [clojure.walk :as walk]
   [datalevin-bench.core :as job]
   [datalevin.core :as d]
   [datalevin.parser :as dp]
   [datalevin.query :as q]
   [datalevin.query-optimizer :as qo]
   [datalevin.query.plan :as qplan]
   [datalevin.query-util :as qu]
   [datalevin.util :as u])
  (:import
   [datalevin.utl LRUCache]
   [java.io Writer]
   [java.nio.charset StandardCharsets]
   [java.security MessageDigest]
   [java.util ArrayDeque Base64]
   [java.util.concurrent Callable ExecutorService Executors Future]))

(defn query-value
  [query-sym]
  (-> (ns-resolve 'datalevin-bench.core query-sym) var-get))

(defn query-name
  [query-sym]
  (subs (name query-sym) 2))

(defn query-where
  [query]
  (let [query (vec query)
        i     (.indexOf query :where)]
    (when (neg? i)
      (throw (ex-info "Query has no :where clause" {:query query})))
    (subvec query (inc i))))

(defn pattern-clause?
  [clause]
  (and (vector? clause)
       (= 3 (count clause))
       (or (symbol? (nth clause 0)) (integer? (nth clause 0)))
       (keyword? (nth clause 1))))

(defn clause-vars
  [clause]
  (into #{} (filter qu/binding-var?) (qu/collect-vars clause)))

(defn query-analysis
  [query]
  (let [parsed-q   (dp/parse-query query)
        replacements (qo/unused-var-replacements parsed-q)
        where      (mapv #(walk/postwalk-replace replacements %)
                         (query-where query))
        patterns   (filterv pattern-clause? where)
        predicates (filterv (complement pattern-clause?) where)
        by-entity  (group-by first patterns)
        entities   (set (keys by-entity))
        entity-query-vars
        (into {}
              (map (fn [entity]
                     [entity
                      (if (qu/placeholder? entity)
                        (symbol (str "?oracle__" (subs (name entity) 1)))
                        entity)]))
              entities)]
    {:query      query
     :where      where
     :patterns   patterns
     :predicates predicates
     :by-entity  by-entity
     :entities   entities
     :entity-query-vars entity-query-vars}))

(defn subset-clauses
  [{:keys [where]} entities]
  (let [patterns   (filterv #(contains? entities (first %)) where)
        bound-vars (into #{} (mapcat clause-vars) patterns)]
    {:clauses
     (filterv
       (fn [clause]
         (if (pattern-clause? clause)
           (contains? entities (first clause))
           (set/subset? (clause-vars clause) bound-vars)))
       where)
     :bound-vars bound-vars}))

(defn clauses-count-form
  "Count the multiplicity of `root` after applying an explicit logical clause
  set. `entities` is used only to replace optimizer placeholder entities with
  legal query variables."
  [analysis entities clauses bound-vars root]
  (let [entity-query-vars (:entity-query-vars analysis)
        replacements (select-keys entity-query-vars entities)
        clauses     (mapv #(walk/postwalk-replace replacements %) clauses)
        root-var    (entity-query-vars root)
        count-vars  (into bound-vars (vals replacements))
        with-vars   (-> count-vars (disj root-var) (->> (sort-by pr-str)))]
    (vec
      (concat [:find (list 'count root-var) '.]
              (when (seq with-vars)
                (into [:with] with-vars))
              [:where]
              clauses))))

(defn subset-count-form
  [analysis entities]
  (let [{:keys [clauses bound-vars]} (subset-clauses analysis entities)]
    (clauses-count-form analysis entities clauses bound-vars
                        (first (sort-by pr-str entities)))))

(defn link-input-clause
  "Return the single target pattern executed by an indexed link before the
  target star's remaining clauses are merged and filtered."
  [{:keys [patterns]} {:keys [link-e target type attr var attrs] :as request}]
  (let [target-attr (case type
                      :_ref attr
                      :val-eq (get attrs target)
                      nil)
        target-val  (case type
                      :_ref link-e
                      :val-eq var
                      nil)
        matches     (filterv (fn [[entity pattern-attr value]]
                               (and (= target entity)
                                    (= target-attr pattern-attr)
                                    (= target-val value)))
                             patterns)]
    (when-not (= 1 (count matches))
      (throw (ex-info "Cannot identify one indexed-link target clause"
                      {:request request :matches matches})))
    (first matches)))

(defn link-input-count-form
  "Build an exact count for rows emitted by a reverse/value-equality link,
  before any other clauses in the newly joined target star are applied."
  [analysis {:keys [entities target] :as request}]
  (let [{:keys [clauses bound-vars]} (subset-clauses analysis entities)
        link-clause (link-input-clause analysis request)
        all-entities (conj entities target)]
    (clauses-count-form analysis all-entities
                        (conj clauses link-clause)
                        (into bound-vars (clause-vars link-clause))
                        target)))

(defn subset-count-query
  [analysis entities timeout-ms]
  (cond-> (dp/query->map (subset-count-form analysis entities))
    timeout-ms (assoc :timeout timeout-ms)))

(defn- count-form-key
  [form]
  (let [bytes  (.getBytes (pr-str form) StandardCharsets/UTF_8)
        digest (.digest (MessageDigest/getInstance "SHA-256") bytes)]
    (.encodeToString (.withoutPadding (Base64/getUrlEncoder)) digest)))

(defn subset-query-key
  [analysis entities]
  (count-form-key (subset-count-form analysis entities)))

(defn exact-subset-count
  ([db analysis entities timeout-ms]
   (exact-subset-count db analysis entities timeout-ms nil))
  ([db analysis entities timeout-ms known-counts]
   (let [query  (subset-count-query analysis entities timeout-ms)
         oracle
         (when known-counts
           (fn [{:keys [kind entities]}]
             (if (and (= :subset kind) (contains? known-counts entities))
               (known-counts entities)
               qo/cardinality-oracle-fallback)))]
     (binding [qo/*cardinality-oracle* oracle
               qplan/*explain*         nil
               q/*cache?*              false
               q/*plan-cache*          (LRUCache. 16)
               u/*reservoir-sampling-seed*
               (long (bit-and Long/MAX_VALUE (hash [(:query analysis)
                                                     entities])))]
       (long (q/count-plan query db))))))

(defn exact-link-input-count
  [db analysis request timeout-ms known-counts]
  (let [query (cond-> (dp/query->map (link-input-count-form analysis request))
                timeout-ms (assoc :timeout timeout-ms))
        oracle
        (when known-counts
          (fn [{:keys [kind entities]}]
            (if (and (= :subset kind) (contains? known-counts entities))
              (known-counts entities)
              qo/cardinality-oracle-fallback)))]
    (binding [qo/*cardinality-oracle* oracle
              qplan/*explain*         nil
              q/*cache?*              false
              q/*plan-cache*          (LRUCache. 16)
              u/*reservoir-sampling-seed*
              (long (bit-and Long/MAX_VALUE (hash [(:query analysis)
                                                    request])))]
      (long (q/count-plan query db)))))

(defn query-graph
  [db query]
  (:query-graph
    (binding [qo/*cardinality-oracle* nil
              q/*cache?*              false
              q/*plan-cache*          (LRUCache. 16)]
      (d/explain {:run? false} query db))))

(defn graph-components
  [query-graph]
  (for [[_ nodes] query-graph
        :let [adjacency
              (into {}
                    (map (fn [[entity node]]
                           [entity (into #{} (map :tgt) (:links node))]))
                    nodes)
              remaining (volatile! (set (keys nodes)))]
        component
        (loop [components []]
          (if-let [root (first @remaining)]
            (let [component
                  (loop [frontier [root]
                         seen     #{}]
                    (if-let [entity (peek frontier)]
                      (if (contains? seen entity)
                        (recur (pop frontier) seen)
                        (recur (into (pop frontier) (adjacency entity))
                               (conj seen entity)))
                      seen))]
              (vswap! remaining set/difference component)
              (recur (conj components {:entities component
                                       :adjacency adjacency})))
            components))]
    component))

(defn connected-subsets
  [{:keys [entities adjacency]}]
  (let [nodes    (vec (sort-by pr-str entities))
        n        (count nodes)
        ids      (zipmap nodes (range))
        masks    (long-array n)
        seen     (boolean-array (bit-shift-left 1 n))
        queue    (ArrayDeque.)
        all-mask (dec (bit-shift-left 1 n))]
    (dotimes [i n]
      (let [entity (nodes i)]
        (aset-long
          masks i
          (reduce (fn [mask neighbor]
                    (if-let [j (ids neighbor)]
                      (bit-set mask j)
                      mask))
                  0 (adjacency entity)))))
    (dotimes [i n]
      (let [mask (bit-shift-left 1 i)]
        (aset-boolean seen mask true)
        (.add queue mask)))
    (loop [result (transient [])]
      (if (.isEmpty queue)
        (persistent! result)
        (let [mask     (long (.remove queue))
              subset  (persistent!
                        (loop [i 0 subset (transient #{})]
                          (if (< i n)
                            (recur (inc i)
                                   (if (bit-test mask i)
                                     (conj! subset (nodes i))
                                     subset))
                            subset)))
              frontier
              (loop [i 0 neighbors 0]
                (if (< i n)
                  (recur (inc i)
                         (if (bit-test mask i)
                           (bit-or neighbors (aget masks i))
                           neighbors))
                  (bit-and all-mask (bit-and-not neighbors mask))))]
          (loop [bits frontier]
            (when-not (zero? bits)
              (let [bit      (Long/lowestOneBit bits)
                    new-mask (bit-or mask bit)]
                (when-not (aget seen new-mask)
                  (aset-boolean seen new-mask true)
                  (.add queue new-mask))
                (recur (bit-xor bits bit)))))
          (recur (conj! result subset)))))))

(defn all-connected-subsets
  [query-graph]
  (into [] (mapcat connected-subsets) (graph-components query-graph)))

(defn read-checkpoint
  [file]
  (if (.exists (io/file file))
    (with-open [reader (io/reader file)]
      (into {}
            (keep (fn [line]
                    (when-not (empty? line)
                      (let [{:keys [entities cardinality]} (edn/read-string line)]
                        [(set entities) cardinality]))))
            (line-seq reader)))
    {}))

(def material-request-fields
  [:kind :entities :link-e :target :type :attr :var :attrs])

(defn material-query-key
  "Hash the exact count form rather than the optimizer request. Different DP
  transitions often ask for the same physical row count; sharing by form
  avoids executing those equivalent counts repeatedly."
  [analysis request]
  (count-form-key (link-input-count-form analysis request)))

(defn material-request-key
  [request]
  (-> (select-keys request material-request-fields)
      (update :entities set)))

(defn read-material-checkpoint
  [file]
  (if (.exists (io/file file))
    (with-open [reader (io/reader file)]
      (into {}
            (keep (fn [line]
                    (when-not (empty? line)
                      (let [{:keys [cardinality] :as row}
                            (edn/read-string line)]
                        [(material-request-key row) cardinality]))))
            (line-seq reader)))
    {}))

(defn append-material-checkpoint!
  ([writer query-name request cardinality elapsed-ms]
   (append-material-checkpoint! writer query-name request cardinality
                                elapsed-ms :executed))
  ([^Writer writer query-name request cardinality elapsed-ms method]
   (.write writer
           (str (pr-str (merge (material-request-key request)
                               {:query query-name
                                :cardinality cardinality
                                :elapsed-ms elapsed-ms
                                :method method}))
                "\n"))
   (.flush writer)))

(defn append-material-shared-checkpoint!
  [^Writer writer key query-name request cardinality]
  (.write writer
          (str (pr-str {:key key
                        :query query-name
                        :request (material-request-key request)
                        :cardinality cardinality})
               "\n"))
  (.flush writer))

(defn append-checkpoint!
  ([writer query-name entities cardinality elapsed-ms]
   (append-checkpoint! writer query-name entities cardinality elapsed-ms
                       :executed))
  ([^Writer writer query-name entities cardinality elapsed-ms method]
   (.write writer
           (str (pr-str {:query query-name
                         :entities (vec (sort-by pr-str entities))
                         :cardinality cardinality
                         :elapsed-ms elapsed-ms
                         :method method})
                "\n"))
   (.flush writer)))

(defn read-shared-checkpoint
  [file]
  (if (.exists (io/file file))
    (with-open [reader (io/reader file)]
      (into {}
            (keep (fn [line]
                    (when-not (empty? line)
                      (let [{:keys [key cardinality]} (edn/read-string line)]
                        [key cardinality]))))
            (line-seq reader)))
    {}))

(defn append-shared-checkpoint!
  [^Writer writer key query-name entities cardinality]
  (.write writer
          (str (pr-str {:key key
                        :query query-name
                        :entities (vec (sort-by pr-str entities))
                        :cardinality cardinality})
               "\n"))
  (.flush writer))

(defn- submit-count
  [^ExecutorService executor count-subset db analysis entities timeout-ms
   known-counts]
  (.submit
    executor
    ^Callable
    (bound-fn []
      (let [start       (System/nanoTime)
            cardinality (count-subset db analysis entities timeout-ms
                                      known-counts)]
        {:cardinality cardinality
         :elapsed-ms (double (/ (- (System/nanoTime) start) 1000000.0))}))))

(defn material-requests
  "Plan one query with exact logical counts and every material count currently
  known, returning all indexed-link input requests encountered by the DP
  search. Missing material counts deliberately use the production fallback so
  collection can continue; callers replan after filling them until closure."
  [db query logical-counts material-counts]
  (let [requests (atom #{})
        cardinality
        (fn [{:keys [kind entities] :as request}]
          (case kind
            :subset
            (if (contains? logical-counts entities)
              (get logical-counts entities)
              (throw
                (ex-info "Exact logical cardinality checkpoint is missing"
                         {:request request})))

            :link-input
            (let [request (material-request-key request)]
              (swap! requests conj request)
              (get material-counts request qo/cardinality-oracle-fallback))

            (throw (ex-info "Unsupported cardinality request"
                            {:request request}))))]
    (binding [qo/*cardinality-oracle* cardinality
              qplan/*explain*         nil
              q/*cache?*              false
              q/*plan-cache*          (LRUCache. 16)
              u/*reservoir-sampling-seed*
              (long (bit-and Long/MAX_VALUE (hash query)))]
      (d/explain {:run? false} query db))
    @requests))

(defn- submit-material-count
  [^ExecutorService executor count-material db analysis request timeout-ms
   logical-counts]
  (.submit
    executor
    ^Callable
    (bound-fn []
      (let [start (System/nanoTime)
            cardinality
            (count-material db analysis request timeout-ms logical-counts)]
        {:cardinality cardinality
         :elapsed-ms (double (/ (- (System/nanoTime) start) 1000000.0))}))))

(defn- register-material-shared!
  [shared-counts shared-writer key query-name request cardinality]
  (if (contains? @shared-counts key)
    (when-not (= (long (get @shared-counts key)) (long cardinality))
      (throw
        (ex-info "Equivalent material count forms disagree"
                 {:key key
                  :query query-name
                  :request request
                  :shared-cardinality (get @shared-counts key)
                  :cardinality cardinality})))
    (do
      (swap! shared-counts assoc key cardinality)
      (append-material-shared-checkpoint!
        shared-writer key query-name request cardinality))))

(defn seed-material-shared!
  "Add completed per-query material counts to the cross-query form cache."
  [shared-counts shared-writer query-name analysis material-counts]
  (doseq [[request cardinality] material-counts]
    (register-material-shared!
      shared-counts shared-writer (material-query-key analysis request)
      query-name request cardinality)))

(defn precompute-material-query!
  "Fill every indexed-link input count requested while planning one query with
  exact logical cardinalities. Results are checkpointed after each completed
  form and planning repeats until no request falls back."
  [db query-sym {:keys [logical-file output-file timeout-ms executor
                        shared-counts shared-writer count-material count-method
                        trusted-shared-only?]
                 :or   {timeout-ms 300000
                        count-material exact-link-input-count
                        count-method :executed
                        trusted-shared-only? false}}]
  (let [query          (query-value query-sym)
        name           (query-name query-sym)
        analysis       (query-analysis query)
        logical-counts (read-checkpoint logical-file)
        checkpointed   (read-material-checkpoint output-file)
        existing
        (if trusted-shared-only?
          (into {}
                (filter
                  (fn [[request cardinality]]
                    (let [key (material-query-key analysis request)]
                      (and (contains? @shared-counts key)
                           (= (long cardinality)
                              (long (get @shared-counts key)))))))
                checkpointed)
          checkpointed)
        output         (io/file output-file)]
    (when (empty? logical-counts)
      (throw (ex-info "Exact logical-cardinality checkpoint is missing"
                      {:query name :file (str logical-file)})))
    (io/make-parents output)
    (when-not trusted-shared-only?
      (seed-material-shared! shared-counts shared-writer name analysis existing))
    (with-open [writer (io/writer output :append true)]
      (loop [counts existing
             pass   1]
        (when (> pass 20)
          (throw (ex-info "Material-cardinality collection did not converge"
                          {:query name :passes (dec pass)})))
        (let [requested (material-requests db query logical-counts counts)
              missing   (remove #(contains? counts %) requested)]
          (if (empty? missing)
            (do
              (println name "material complete:" (count requested)
                       "requests," (count counts) "checkpointed")
              counts)
            (let [groups
                  (->> missing
                       (group-by #(material-query-key analysis %))
                       (sort-by key)
                       vec)
                  jobs
                  (mapv
                    (fn [[key requests]]
                      (if (contains? @shared-counts key)
                        {:key key
                         :requests requests
                         :cached? true
                         :method :shared
                         :result {:cardinality (get @shared-counts key)
                                  :elapsed-ms 0.0}}
                        {:key key
                         :requests requests
                         :cached? false
                         :method count-method
                         :result (submit-material-count
                                   executor count-material db analysis
                                   (first requests) timeout-ms
                                   logical-counts)}))
                    groups)]
              (println name "material pass" pass ":" (count missing)
                       "missing requests," (count groups) "unique forms")
              (recur
                (reduce
                  (fn [counts [i {:keys [key requests cached? method result]}]]
                    (let [{:keys [cardinality elapsed-ms]}
                          (if cached?
                            result
                            (.get ^Future result))]
                      (register-material-shared!
                        shared-counts shared-writer key name (first requests)
                        cardinality)
                      (when (or (= i (dec (count jobs)))
                                (zero? (mod (inc i) 100)))
                        (println name "material forms" (inc i) "/"
                                 (count jobs) "=" cardinality
                                 (clojure.core/name method)
                                 (format "%.1f ms" elapsed-ms)))
                      (reduce
                        (fn [counts request]
                          (append-material-checkpoint!
                            writer name request cardinality elapsed-ms method)
                          (assoc counts request cardinality))
                        counts requests)))
                  counts
                  (map-indexed vector jobs))
                (inc pass)))))))))

(defn- minimal-zero-subsets
  [counts]
  (reduce
    (fn [minimal [entities cardinality]]
      (if (or (not (zero? (long cardinality)))
              (some #(set/subset? % entities) minimal))
        minimal
        (conj minimal entities)))
    []
    (sort-by (comp count key) counts)))

(defn precompute-query!
  [db query-sym {:keys [output-file timeout-ms shared-counts shared-writer
                        executor count-subset count-method]
                 :or   {timeout-ms 300000
                        count-subset exact-subset-count
                        count-method :executed}}]
  (let [query      (query-value query-sym)
        name       (query-name query-sym)
        analysis   (query-analysis query)
        subsets    (->> (all-connected-subsets (query-graph db query))
                        (sort-by (juxt count #(mapv pr-str
                                                   (sort-by pr-str %)))))
        existing   (read-checkpoint output-file)
        output     (io/file output-file)]
    (io/make-parents output)
    (when shared-counts
      (doseq [[entities cardinality] existing]
        (let [key (subset-query-key analysis entities)]
          (when-not (contains? @shared-counts key)
            (swap! shared-counts assoc key cardinality)
            (append-shared-checkpoint! shared-writer key name entities
                                       cardinality)))))
    (with-open [writer (io/writer output :append true)]
      (reduce
        (fn [counts group]
          (let [missing (remove #(contains? counts (second %)) group)
                zero-subsets (minimal-zero-subsets counts)
                jobs
                (mapv
                  (fn [[i entities]]
                    (let [key     (subset-query-key analysis entities)
                          shared? (and shared-counts
                                       (contains? @shared-counts key))
                          inferred-zero?
                          (and (not shared?)
                               (some #(set/subset? % entities)
                                     zero-subsets))]
                      {:i        i
                       :entities entities
                       :key      key
                       :shared?  shared?
                       :inferred-zero? inferred-zero?
                       :result
                       (cond
                         shared?
                         {:cardinality (get @shared-counts key)
                          :elapsed-ms 0.0}

                         inferred-zero?
                         {:cardinality 0 :elapsed-ms 0.0}

                         :else
                         (submit-count executor count-subset db analysis
                                       entities timeout-ms counts))}))
                  missing)]
            (reduce
              (fn [counts {:keys [i entities key shared? inferred-zero?
                                  result]}]
                (let [{:keys [cardinality elapsed-ms]}
                      (if (or shared? inferred-zero?)
                        result
                        (.get ^Future result))]
                  (append-checkpoint! writer name entities cardinality
                                      elapsed-ms
                                      (cond
                                        shared? :shared
                                        inferred-zero? :zero-superset
                                        :else count-method))
                  (when (and shared-counts (not shared?))
                    (swap! shared-counts assoc key cardinality)
                    (append-shared-checkpoint! shared-writer key name entities
                                               cardinality))
                  (when (or (= i (dec (count subsets)))
                            (zero? (mod (inc i) 100)))
                    (println name (inc i) "/" (count subsets)
                             (vec (sort-by pr-str entities)) "=" cardinality
                             (cond
                               shared? "shared"
                               inferred-zero? "zero-superset"
                               :else
                               (str (clojure.core/name count-method) " "
                                    (format "%.1f ms" elapsed-ms)))))
                  (assoc counts entities cardinality)))
              counts jobs)))
        existing
        (partition-by (comp count second) (map-indexed vector subsets))))))

(defn selected-query-symbols
  [requested]
  (let [available (into {} (map (juxt query-name identity)) job/queries)]
    (if (seq requested)
      (mapv #(or (available (str %))
                 (throw (ex-info "Unknown JOB query" {:query %})))
            requested)
      job/queries)))

(defn run
  [{:keys [db-path output-dir queries timeout-ms parallelism
           material-cardinalities?]
    :or   {db-path "db"
           output-dir "results/cidr-exact-cardinalities"
           timeout-ms 300000
           parallelism 2
           material-cardinalities? false}}]
  (when-not (and (integer? parallelism) (pos? parallelism))
    (throw (ex-info "Oracle parallelism must be a positive integer"
                    {:parallelism parallelism})))
  (let [conn     (d/get-conn db-path)
        executor (Executors/newFixedThreadPool (int parallelism))]
    (try
      (let [query-symbols (selected-query-symbols queries)
            shared-file   (io/file output-dir
                                   (if material-cardinalities?
                                     "shared.material.edn"
                                     "shared.edn"))
            shared-counts (atom (read-shared-checkpoint shared-file))]
        (io/make-parents shared-file)
        (with-open [shared-writer (io/writer shared-file :append true)]
          ;; Seed all completed material files before starting new work, so an
          ;; early query can reuse an equivalent form already collected for a
          ;; later query.
          (when material-cardinalities?
            (doseq [query-sym query-symbols]
              (let [name     (query-name query-sym)
                    analysis (query-analysis (query-value query-sym))]
                (seed-material-shared!
                  shared-counts shared-writer name analysis
                  (read-material-checkpoint
                    (io/file output-dir (str name ".material.edn")))))))
          (doseq [query-sym query-symbols]
            (let [name (query-name query-sym)]
              (if material-cardinalities?
                (precompute-material-query!
                  (d/db conn) query-sym
                  {:logical-file (io/file output-dir (str name ".edn"))
                   :output-file (io/file output-dir
                                         (str name ".material.edn"))
                   :timeout-ms timeout-ms
                   :shared-counts shared-counts
                   :shared-writer shared-writer
                   :executor executor})
                (precompute-query!
                  (d/db conn) query-sym
                  {:output-file (str (io/file output-dir
                                              (str name ".edn")))
                   :timeout-ms timeout-ms
                   :shared-counts shared-counts
                   :shared-writer shared-writer
                   :executor executor}))))))
      (finally
        (.shutdownNow ^ExecutorService executor)
        (d/close conn)))))
