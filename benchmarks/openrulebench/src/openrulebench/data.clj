(ns openrulebench.data
  "Data generation and loading for OpenRuleBench.
   Implements standard OpenRuleBench benchmark instances:
   - TC (Transitive Closure): Random graphs with 50K-1M edges
   - SG (Same Generation): Random par/sib relations
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
;; OpenRuleBench TC Instances
;; Standard sizes match OpenRuleBench paper: 50K, 125K, 250K, 500K, 1M edges.
;; :tiny is a non-standard development scale for local comparisons.
;; =============================================================================

(def tc-instances
  "TC benchmark instances."
  {:tiny    {:nodes 100   :edges 1000}
   :small   {:nodes 1000  :edges 50000}
   :medium  {:nodes 1000  :edges 125000}
   :large   {:nodes 2000  :edges 250000}
   :xlarge  {:nodes 2000  :edges 500000}
   :xxlarge {:nodes 2000  :edges 1000000}})

(defn generate-tc-instance
  "Generate TC instance data. Returns seq of [from to] edges.
   Instance can be :tiny, :small, :medium, :large, :xlarge, :xxlarge
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

;; =============================================================================
;; OpenRuleBench SG Instances
;; OpenRuleBench SG uses two base relations, par and sib. The paper reports
;; 6K and 24K SG data sizes; :tiny and :large are development extensions.
;; =============================================================================

(def sg-instances
  "SG benchmark instances. Counts are total par+sib facts."
  {:tiny  {:nodes 100  :par-facts 500   :sib-facts 500}
   :small {:nodes 1000 :par-facts 3000  :sib-facts 3000}
   :medium {:nodes 1000 :par-facts 12000 :sib-facts 12000}
   :large {:nodes 1000 :par-facts 24000 :sib-facts 24000}})

(defn generate-sg-instance
  "Generate SG instance data as {:par [...], :sib [...]}.
   Instance can be :tiny, :small, :medium, :large, or a map with :nodes,
   :par-facts, and :sib-facts keys."
  ([instance-key]
   (generate-sg-instance instance-key {}))
  ([instance-key opts]
   (let [{:keys [nodes par-facts sib-facts]} (if (map? instance-key)
                                               instance-key
                                               (get sg-instances instance-key))
         {:keys [acyclic?] :or {acyclic? false}} opts]
     (when (nil? nodes)
       (throw (ex-info (str "Unknown SG instance: " instance-key)
                       {:instance instance-key :available (keys sg-instances)})))
     {:par (generate-random-graph nodes par-facts
                                  :seed 42
                                  :acyclic? acyclic?)
      :sib (generate-random-graph nodes sib-facts
                                  :seed 43)})))

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

(defn sg->datoms
  "Convert SG par/sib pairs to Datalevin datoms."
  [{:keys [par sib]}]
  (vec
    (concat
      (map (fn [[a b]] {:db/id a :par b}) par)
      (map (fn [[a b]] {:db/id a :sib b}) sib))))

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
  (keys tc-instances)  ;; => (:tiny :small :medium :large :xlarge :xxlarge)
  (keys sg-instances)  ;; => (:tiny :small :medium :large)
  (keys join1-instances) ;; => (:small :medium :large)
  )
