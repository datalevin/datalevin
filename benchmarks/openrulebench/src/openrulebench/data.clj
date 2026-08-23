(ns openrulebench.data
  "Deterministic set-valued data generators for the portable OpenRuleBench
   tasks. Each sample is generated in Clojure and handed unchanged to its
   backend."
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
  (let [capacity (if acyclic?
                   (quot (* (long n) (dec (long n))) 2)
                   (* (long n) (dec (long n))))
        _ (when (or (neg? m) (> m capacity))
            (throw (ex-info "Requested graph does not fit its node domain"
                            {:nodes n :edges m :capacity capacity
                             :acyclic? acyclic?})))
        rng (Random. seed)
        gen-edge (if acyclic?
                   ;; For acyclic: from < to
                   (fn [] (let [a (.nextInt rng n)
                                b (.nextInt rng n)]
                            (if (< a b) [a b] [b a])))
                   ;; For cyclic: any edge
                   (fn [] [(.nextInt rng n) (.nextInt rng n)]))]
    ;; A sorted vector makes the generated input stable across runtimes, not
    ;; merely stable within one Clojure hash-set implementation.
    (loop [edges #{}]
      (if (>= (count edges) m)
        (vec (sort edges))
        (let [e (gen-edge)]
          (if (and (not= (first e) (second e)) ;; no self-loops
                   (not (edges e)))
            (recur (conj edges e))
            (recur edges)))))))

;; =============================================================================
;; OpenRuleBench TC Instances
;; The paper reports 50K and 500K facts, each in cyclic and acyclic forms.
;; Legacy aliases remain available for old local commands, but only :50k and
;; :500k are part of the publication task matrix.
;; =============================================================================

(def tc-instances
  "TC benchmark instances."
  {:tiny    {:nodes 100  :edges 1000}
   :50k     {:nodes 1000 :edges 50000}
   :500k    {:nodes 2000 :edges 500000}
   ;; Legacy development aliases.
   :small   {:nodes 1000 :edges 50000}
   :medium  {:nodes 1000 :edges 125000}
   :large   {:nodes 2000 :edges 250000}
   :xlarge  {:nodes 2000 :edges 500000}
   :xxlarge {:nodes 2000 :edges 1000000}})

(defn generate-tc-instance
  "Generate TC instance data. Returns seq of [from to] edges.
   Canonical instances are :tiny, :50k, and :500k. Legacy development aliases
   and maps with :nodes/:edges are also accepted."
  ([instance-key]
   (generate-tc-instance instance-key {}))
  ([instance-key opts]
   (let [{:keys [nodes edges]} (if (map? instance-key)
                                 instance-key
                                 (get tc-instances instance-key))
         {:keys [acyclic? seed] :or {acyclic? false seed 42}} opts]
     (when (nil? nodes)
       (throw (ex-info (str "Unknown instance: " instance-key)
                       {:instance instance-key :available (keys tc-instances)})))
     (generate-random-graph nodes edges :seed seed :acyclic? acyclic?))))

;; =============================================================================
;; OpenRuleBench SG Instances
;; OpenRuleBench SG uses two base relations, par and sib. The paper reports
;; 6K and 24K SG data sizes, each in cyclic and acyclic forms. Cyclicity is a
;; property of par, the relation traversed by the recursive rule.
;; =============================================================================

(def sg-instances
  "SG benchmark instances. Counts are total par+sib facts."
  {:tiny   {:nodes 100  :par-facts 500   :sib-facts 500}
   :6k     {:nodes 1000 :par-facts 3000  :sib-facts 3000}
   :24k    {:nodes 1000 :par-facts 12000 :sib-facts 12000}
   ;; Legacy development aliases.
   :small  {:nodes 1000 :par-facts 3000  :sib-facts 3000}
   :medium {:nodes 1000 :par-facts 12000 :sib-facts 12000}
   :large  {:nodes 1000 :par-facts 24000 :sib-facts 24000}})

(defn generate-sg-instance
  "Generate SG instance data as {:par [...], :sib [...]}.
   Canonical instances are :tiny, :6k, and :24k. Legacy development aliases
   and maps with :nodes/:par-facts/:sib-facts are also accepted."
  ([instance-key]
   (generate-sg-instance instance-key {}))
  ([instance-key opts]
   (let [{:keys [nodes par-facts sib-facts]} (if (map? instance-key)
                                               instance-key
                                               (get sg-instances instance-key))
         {:keys [acyclic? seed] :or {acyclic? false seed 42}} opts]
     (when (nil? nodes)
       (throw (ex-info (str "Unknown SG instance: " instance-key)
                       {:instance instance-key :available (keys sg-instances)})))
     {:par (generate-random-graph nodes par-facts
                                  :seed seed
                                  :acyclic? acyclic?)
      :sib (generate-random-graph nodes sib-facts
                                  :seed (inc (long seed)))})))

;; =============================================================================
;; Join1 Data Generation
;; =============================================================================

(def join1-instances
  "JOIN1 benchmark instances.
   Each instance has 5 relations: d1, d2, c2, c3, c4"
  {:tiny   {:tuples 100 :domain 100}
   :50k    {:tuples 50000 :domain 1000}
   :250k   {:tuples 250000 :domain 1000}
   ;; Legacy development aliases.
   :small  {:tuples 10000 :domain 1000}
   :medium {:tuples 50000 :domain 1000}
   :large  {:tuples 250000 :domain 1000}})

(defn generate-join1-relation
  "Generate exactly n unique tuples in a deterministic order."
  [n domain seed]
  (let [capacity (* (long domain) domain)
        _ (when (> n capacity)
            (throw (ex-info "JOIN1 relation does not fit its domain"
                            {:tuples n :domain domain :capacity capacity})))
        rng (Random. seed)]
    (loop [tuples #{}]
      (if (= (count tuples) n)
        (vec (sort tuples))
        (recur (conj tuples [(.nextInt rng domain)
                             (.nextInt rng domain)]))))))

(defn generate-join1-instance
  "Generate all JOIN1 relations for an instance.
   Returns map {:d1 [...] :d2 [...] :c2 [...] :c3 [...] :c4 [...]}. A custom
   instance map must contain :tuples and :domain."
  [instance-key]
  (let [{:keys [tuples domain]} (if (map? instance-key)
                                  instance-key
                                  (get join1-instances instance-key))]
    (when (or (nil? tuples) (nil? domain))
      (throw (ex-info (str "Unknown instance: " instance-key)
                      {:instance instance-key :available (keys join1-instances)})))
    {:d1 (generate-join1-relation tuples domain 1)
     :d2 (generate-join1-relation tuples domain 2)
     :c2 (generate-join1-relation tuples domain 3)
     :c3 (generate-join1-relation tuples domain 4)
     :c4 (generate-join1-relation tuples domain 5)}))

(defn join1-domain
  "Return the node-domain size used by a JOIN1 instance."
  [instance-key]
  (let [{:keys [domain]} (if (map? instance-key)
                           instance-key
                           (get join1-instances instance-key))]
    (when-not domain
      (throw (ex-info (str "Unknown JOIN1 instance: " instance-key)
                      {:instance instance-key
                       :available (keys join1-instances)})))
    domain))

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
  (keys tc-instances)
  (keys sg-instances)
  (keys join1-instances)
  )
