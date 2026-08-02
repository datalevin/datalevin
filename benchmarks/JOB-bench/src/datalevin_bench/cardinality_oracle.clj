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

(defn subset-count-form
  [analysis entities]
  (let [{:keys [clauses bound-vars]} (subset-clauses analysis entities)
        entity-query-vars (:entity-query-vars analysis)
        replacements (select-keys entity-query-vars entities)
        clauses     (mapv #(walk/postwalk-replace replacements %) clauses)
        root        (first (sort-by pr-str entities))
        root-var    (entity-query-vars root)
        count-vars  (into bound-vars (vals replacements))
        with-vars   (-> count-vars (disj root-var) (->> (sort-by pr-str)))]
    (vec
      (concat [:find (list 'count root-var) '.]
              (when (seq with-vars)
                (into [:with] with-vars))
              [:where]
              clauses))))

(defn subset-count-query
  [analysis entities timeout-ms]
  (cond-> (dp/query->map (subset-count-form analysis entities))
    timeout-ms (assoc :timeout timeout-ms)))

(defn subset-query-key
  [analysis entities]
  (let [bytes  (.getBytes (pr-str (subset-count-form analysis entities))
                          StandardCharsets/UTF_8)
        digest (.digest (MessageDigest/getInstance "SHA-256") bytes)]
    (.encodeToString (.withoutPadding (Base64/getUrlEncoder)) digest)))

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
  [{:keys [db-path output-dir queries timeout-ms parallelism]
    :or   {db-path "db"
           output-dir "results/cidr-exact-cardinalities"
           timeout-ms 300000
           parallelism 2}}]
  (when-not (and (integer? parallelism) (pos? parallelism))
    (throw (ex-info "Oracle parallelism must be a positive integer"
                    {:parallelism parallelism})))
  (let [conn     (d/get-conn db-path)
        executor (Executors/newFixedThreadPool (int parallelism))]
    (try
      (let [shared-file   (io/file output-dir "shared.edn")
            shared-counts (atom (read-shared-checkpoint shared-file))]
        (io/make-parents shared-file)
        (with-open [shared-writer (io/writer shared-file :append true)]
          (doseq [query-sym (selected-query-symbols queries)]
            (precompute-query!
              (d/db conn) query-sym
              {:output-file (str (io/file output-dir
                                          (str (query-name query-sym) ".edn")))
               :timeout-ms timeout-ms
               :shared-counts shared-counts
               :shared-writer shared-writer
               :executor executor}))))
      (finally
        (.shutdownNow ^ExecutorService executor)
        (d/close conn)))))
