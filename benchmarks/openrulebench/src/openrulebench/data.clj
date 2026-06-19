(ns openrulebench.data
  "Data generation and loading for OpenRuleBench.
   Implements standard OpenRuleBench benchmark instances:
   - TC (Transitive Closure): Random graphs with 50K-1M edges
   - SG (Same Generation): Same random graphs
   - Join1: 5-way join benchmark
   - DBLP: Real-world publication data
   - LUBM: Semantic university benchmark"
  (:import [java.util Random]))

;; =============================================================================
;; Random Graph Generation (OpenRuleBench style)
;; =============================================================================

(defn generate-random-graph
  "Generate a random directed graph with n nodes and m edges.
   Returns seq of [from to] pairs.
   Options:
     :seed    - Random seed for reproducibility (default: 42)
     :acyclic? - If true, ensure from < to (creates DAG, default: false)"
  [n m & {:keys [seed acyclic?] :or {seed 42 acyclic? false}}]
  (let [rng (Random. seed)
        gen-edge (if acyclic?
                   ;; For acyclic: from < to
                   (fn [] (let [a (.nextInt rng n)
                                b (.nextInt rng n)]
                            (if (< a b) [a b] [b a])))
                   ;; For cyclic: any edge
                   (fn [] [(.nextInt rng n) (.nextInt rng n)]))]
    ;; Generate m unique edges
    (loop [edges #{}]
      (if (>= (count edges) m)
        (vec edges)
        (let [e (gen-edge)]
          (if (and (not= (first e) (second e)) ;; no self-loops
                   (not (edges e)))
            (recur (conj edges e))
            (recur edges)))))))

;; =============================================================================
;; OpenRuleBench TC/SG Instances
;; Sizes match OpenRuleBench paper: 50K, 125K, 250K, 500K, 1M edges
;; =============================================================================

(def tc-instances
  "TC/SG benchmark instances (OpenRuleBench standard sizes)."
  {:small   {:nodes 1000  :edges 50000}
   :medium  {:nodes 1000  :edges 125000}
   :large   {:nodes 2000  :edges 250000}
   :xlarge  {:nodes 2000  :edges 500000}
   :xxlarge {:nodes 2000  :edges 1000000}})

(def sg-instances
  "SG uses same instances as TC."
  tc-instances)

(defn generate-tc-instance
  "Generate TC instance data. Returns seq of [from to] edges.
   Instance can be :small, :medium, :large, :xlarge, :xxlarge
   or a map with :nodes and :edges keys."
  ([instance-key]
   (generate-tc-instance instance-key {}))
  ([instance-key opts]
   (let [{:keys [nodes edges]} (if (map? instance-key)
                                 instance-key
                                 (get tc-instances instance-key))
         {:keys [acyclic?] :or {acyclic? false}} opts]
     (when (nil? nodes)
       (throw (ex-info (str "Unknown instance: " instance-key)
                       {:instance instance-key :available (keys tc-instances)})))
     (generate-random-graph nodes edges :acyclic? acyclic?))))

(defn generate-sg-instance
  "Generate SG instance data. Same as TC instances."
  ([instance-key]
   (generate-tc-instance instance-key))
  ([instance-key opts]
   (generate-tc-instance instance-key opts)))

;; =============================================================================
;; Join1 Data Generation
;; =============================================================================

(def join1-instances
  "JOIN1 benchmark instances.
   Each instance has 5 relations: d1, d2, c2, c3, c4"
  {:small  {:tuples 10000}
   :medium {:tuples 50000}
   :large  {:tuples 250000}})

(defn generate-join1-relation
  "Generate a join1 relation with n tuples.
   Each tuple is [a b] where a,b are random integers."
  [n domain seed]
  (let [rng (Random. seed)]
    (vec (repeatedly n (fn [] [(.nextInt rng domain) (.nextInt rng domain)])))))

(defn generate-join1-instance
  "Generate all JOIN1 relations for an instance.
   Returns map {:d1 [...] :d2 [...] :c2 [...] :c3 [...] :c4 [...]}
   Domain size scales with tuple count to maintain join selectivity."
  [instance-key]
  (let [{:keys [tuples]} (if (map? instance-key)
                           instance-key
                           (get join1-instances instance-key))
        domain (int (Math/sqrt tuples))]
    (when (nil? tuples)
      (throw (ex-info (str "Unknown instance: " instance-key)
                      {:instance instance-key :available (keys join1-instances)})))
    {:d1 (generate-join1-relation tuples domain 1)
     :d2 (generate-join1-relation tuples domain 2)
     :c2 (generate-join1-relation tuples domain 3)
     :c3 (generate-join1-relation tuples domain 4)
     :c4 (generate-join1-relation tuples domain 5)}))

;; =============================================================================
;; Datalevin Data Conversion
;; =============================================================================

(defn edges->datoms
  "Convert edge pairs to Datalevin datoms for TC benchmark."
  [edges]
  (mapv (fn [[from to]]
          {:db/id from :edge to})
        edges))

(defn edges->parent-datoms
  "Convert parent-child edges to Datalevin datoms for SG benchmark.
   Edge [parent, child] becomes {:db/id child :parent parent}
   So [child :parent parent] in EAV - child has parent."
  [edges]
  (mapv (fn [[parent child]]
          {:db/id child :parent parent})
        edges))

(defn join1->datoms
  "Convert JOIN1 relations to Datalevin datoms.
   Each relation becomes [from :rel to] triples using ref attributes."
  [{:keys [d1 d2 c2 c3 c4]}]
  (vec (concat
         (map (fn [[a b]] {:db/id a :d1 b}) d1)
         (map (fn [[a b]] {:db/id a :d2 b}) d2)
         (map (fn [[a b]] {:db/id a :c2 b}) c2)
         (map (fn [[a b]] {:db/id a :c3 b}) c3)
         (map (fn [[a b]] {:db/id a :c4 b}) c4))))

(comment
  ;; Generate TC instance
  (def edges (generate-tc-instance :small))
  (count edges) ;; => 50000
  (take 5 edges)

  ;; Generate acyclic instance
  (def acyclic-edges (generate-tc-instance :small {:acyclic? true}))
  (every? (fn [[a b]] (<= a b)) acyclic-edges) ;; => true

  ;; Generate Join1 instance
  (def j1 (generate-join1-instance :small))
  (count (:d1 j1)) ;; => 10000

  ;; List available instances
  (keys tc-instances)  ;; => (:small :medium :large :xlarge :xxlarge)
  (keys join1-instances) ;; => (:small :medium :large)
  )
