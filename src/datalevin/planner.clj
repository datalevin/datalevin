;;
;; Copyright (c) Huahai Yang, Nikita Prokopov. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns ^:no-doc datalevin.planner
  "Datalog query planner and optimizer"
  (:refer-clojure :exclude [update assoc])
  (:require
   [clojure.set :as set]
   [clojure.string :as str]
   [clojure.walk :as w]
   [clojure.core.reducers :as rd]
   [datalevin.db :as db]
   [datalevin.lmdb :as l]
   [datalevin.query-util :as qu :refer [-type -execute -sample]]
   [datalevin.storage :as s]
   [datalevin.built-ins :as built-ins]
   [datalevin.util :as u :refer [cond+ raise conjv concatv map+]]
   [datalevin.inline :refer [update assoc]]
   [datalevin.pipe :as p]
   [datalevin.parser :as dp]
   [datalevin.constants :as c]
   [datalevin.bits :as b]
   [datalevin.interface :refer [av-size]])
  (:import
   [datalevin.query Plan Node Link OrJoinLink Clause InitStep MergeScanStep
    LinkStep HashJoinStep OrJoinStep]
   [datalevin.utl LikeFSM LRUCache]
   [datalevin.db DB]
   [datalevin.storage Store]
   [datalevin.parser And BindColl  BindScalar BindTuple Constant
    DefaultSrc Function Or Variable Pattern Predicate Not RuleExpr]
   [java.util Arrays List]
   [java.util.concurrent ConcurrentHashMap]
   [org.eclipse.collections.impl.list.mutable FastList]))

(declare estimate-hash-join-cost)

(defn resolve-pattern-lookup-refs [source pattern]
  (if (db/-searchable? source)
    (let [[e a v] pattern
          e'      (if (or (qu/lookup-ref? e) (keyword? e))
                    (db/entid-strict source e)
                    e)
          v'      (if (and v
                           (keyword? a)
                           (db/ref? source a)
                           (or (qu/lookup-ref? v) (keyword? v)))
                    (db/entid-strict source v)
                    v)]
      (subvec [e' a v'] 0 (count pattern)))
    pattern))

(defn- dot-form [f] (when (and (symbol? f) (str/starts-with? (name f) ".")) f))

(defn- dot-call
  [^String fname ^objects args]
  (clojure.lang.Reflector/invokeInstanceMethod
    (aget args 0) fname (Arrays/copyOfRange args 1 (alength args))))

(defn- opt-apply
  [f args]
  (if (u/array? args)
    (let [args ^objects args
          len  (alength args)]
      (case len
        0 (f)
        1 (f (aget args 0))
        2 (f (aget args 0) (aget args 1))
        3 (f (aget args 0) (aget args 1) (aget args 2))
        4 (f (aget args 0) (aget args 1) (aget args 2) (aget args 3))
        5 (f (aget args 0) (aget args 1) (aget args 2) (aget args 3)
             (aget args 4))
        6 (f (aget args 0) (aget args 1) (aget args 2) (aget args 3)
             (aget args 4) (aget args 5))
        7 (f (aget args 0) (aget args 1) (aget args 2) (aget args 3)
             (aget args 4) (aget args 5) (aget args 6))
        (apply f args)))
    (apply f args)))

(defn- make-call
  [f]
  (if (dot-form f)
    (let [fname (subs (name f) 1)] #(dot-call fname %))
    #(opt-apply f %)))

(defn- resolve-sym
  [sym]
  (when (symbol? sym)
    (when-let [v (or (resolve sym)
                     (when (find-ns 'pod.huahaiy.datalevin)
                       (ns-resolve 'pod.huahaiy.datalevin sym)))]
      @v)))

(defn- resolve-pred-simple
  "Resolve a predicate function without query context.
   Used for pushdown predicates where context is not available."
  [f]
  (if (fn? f)
    f
    (or (get built-ins/query-fns f)
        (dot-form f)
        (resolve-sym f)
        (raise "Unknown function or predicate \u2018" f
               {:error :query/where :var f}))))

;; optimizer

(defn or-join-var?
  [clause s]
  (and (list? clause)
       (= 'or-join (first clause))
       (some #(= % s) (tree-seq sequential? seq (second clause)))))

(defn plugin-inputs*
  [parsed-q inputs]
  (let [qins    (:qin parsed-q)
        finds   (tree-seq sequential? seq (:qorig-find parsed-q))
        owheres (:qorig-where parsed-q)
        to-rm   (keep-indexed
                  (fn [i qin]
                    (let [v (:variable qin)
                          s (:symbol v)
                          val (nth inputs i)]
                      (when (and (instance? BindScalar qin)
                                 (instance? Variable v)
                                 ;; keep sequential inputs as variables so
                                 ;; function calls don't eagerly evaluate them
                                 (not (sequential? val))
                                 (not (some #(= s %) finds))
                                 (not (some #(or-join-var? % s) owheres)))
                        [i s])))
                  qins)
        rm-idxs (into #{} (map first) to-rm)
        smap    (reduce (fn [m [i s]] (assoc m s (nth inputs i))) {} to-rm)]
    [(assoc parsed-q
            :qwhere (reduce-kv
                      (fn [ws s v]
                        (w/postwalk
                          (fn [e]
                            (if (and (instance? Variable e)
                                     (= s (:symbol e)))
                              (Constant. v)
                              e))
                          ws))
                      (:qwhere parsed-q) smap)
            :qorig-where (w/postwalk-replace smap owheres)
            :qin (u/remove-idxs rm-idxs qins))
     (u/remove-idxs rm-idxs inputs)]))

(defn plugin-inputs
  "optimization that plugs simple value inputs into where clauses"
  [parsed-q inputs]
  (let [ins (:qin parsed-q)
        cb  (count ins)
        cv  (count inputs)]
    (cond
      (< cb cv) (raise "Extra inputs passed, expected: "
                       (mapv #(:source (meta %)) ins) ", got: " cv
                       {:error :query/inputs :expected ins :got inputs})
      (> cb cv) (raise "Too few inputs passed, expected: "
                       (mapv #(:source (meta %)) ins) ", got: " cv
                       {:error :query/inputs :expected ins :got inputs})
      :else     (plugin-inputs* parsed-q inputs))))

(defn var-symbol
  [v]
  (when (instance? Variable v)
    (:symbol v)))

(defn collect-var-usage
  [qwhere]
  (let [counts    (volatile! {})
        kinds     (volatile! {})
        protected (volatile! #{})]
    (letfn [(note-var! [sym kind]
              (when (qu/binding-var? sym)
                (vswap! counts update sym (fnil inc 0))
                (vswap! kinds update sym (fnil conj #{}) kind)))
            (protect-var! [sym]
              (when (qu/free-var? sym)
                (vswap! protected conj sym)))
            (note-var [v kind]
              (when-let [sym (var-symbol v)]
                (note-var! sym kind)))
            (protect-var [v]
              (when-let [sym (var-symbol v)]
                (protect-var! sym)))
            (protect-vars-in-form [form]
              (doseq [sym (qu/collect-vars form)]
                (protect-var! sym)))
            (protect-arg-vars [arg]
              (when (instance? Constant arg)
                (protect-vars-in-form (:value arg))))
            (walk-binding [binding]
              (cond
                (instance? BindScalar binding)
                (note-var (:variable binding) :binding)

                (instance? BindTuple binding)
                (doseq [b (:bindings binding)]
                  (walk-binding b))

                (instance? BindColl binding)
                (walk-binding (:binding binding))

                :else nil))
            (walk-clause [clause]
              (cond
                (instance? Pattern clause)
                (doseq [el (:pattern clause)]
                  (note-var el :pattern))

                (instance? Function clause)
                (do
                  (protect-var (:fn clause))
                  (doseq [arg (:args clause)]
                    (protect-var arg)
                    (protect-arg-vars arg))
                  (walk-binding (:binding clause)))

                (instance? Predicate clause)
                (do
                  (protect-var (:fn clause))
                  (doseq [arg (:args clause)]
                    (protect-var arg)
                    (protect-arg-vars arg)))

                (instance? RuleExpr clause)
                (doseq [arg (:args clause)]
                  (protect-var arg))

                (instance? And clause)
                (doseq [c (:clauses clause)]
                  (walk-clause c))

                (instance? Or clause)
                (doseq [c (:clauses clause)]
                  (protect-vars-in-form c)
                  (walk-clause c))

                (instance? Not clause)
                (doseq [c (:clauses clause)]
                  (protect-vars-in-form c)
                  (walk-clause c))

                :else nil))]
      (doseq [c qwhere] (walk-clause c))
      {:counts @counts :kinds @kinds :protected @protected})))

(defn unused-var-replacements
  [parsed-q]
  (let [find-vars (set (dp/find-vars (:qfind parsed-q)))
        with-vars (set (map :symbol (or (:qwith parsed-q) [])))
        in-vars   (set (map :symbol (dp/collect-vars-distinct (:qin parsed-q))))
        used      (set/union find-vars with-vars in-vars)
        {:keys [counts kinds protected]}
        (collect-var-usage (:qwhere parsed-q))]
    (into {}
          (keep (fn [[sym n]]
                  (when (and (= 1 n)
                             (not (contains? used sym))
                             (not (contains? protected sym)))
                    (let [kind (get kinds sym)]
                      [sym (if (contains? kind :binding)
                             '_
                             (qu/placeholder-sym sym))]))))
          counts)))

(defn replace-unused-vars-form
  [form replacements]
  (letfn [(walk [form]
            (cond
              (qu/quoted-form? form) form
              (symbol? form)         (get replacements form form)
              (map? form)            (into (empty form)
                                           (map (fn [[k v]]
                                                  [(walk k) (walk v)]))
                                           form)
              (seq? form)            (apply list (map walk form))
              (coll? form)           (into (empty form) (map walk) form)
              :else                  form))]
    (walk form)))

(defn rewrite-unused-vars
  [{:keys [parsed-q] :as context}]
  (let [replacements (unused-var-replacements parsed-q)]
    (if (empty? replacements)
      context
      (let [qorig-where  (mapv #(replace-unused-vars-form % replacements)
                               (:qorig-where parsed-q))
            qwhere       (dp/parse-where qorig-where)]
        (assoc context :parsed-q
               (assoc parsed-q :qorig-where qorig-where :qwhere qwhere))))))

(defn optimizable?
  "only optimize attribute-known patterns referring to Datalevin source"
  [sources resolved clause]
  (when (instance? Pattern clause)
    (let [{:keys [pattern]} clause]
      (when (and (instance? Constant (second pattern))
                 (not-any? resolved (map :symbol pattern)))
        (if-let [s (get-in clause [:source :symbol])]
          (when-let [src (get sources s)] (db/-searchable? src))
          (when-let [src (get sources '$)] (db/-searchable? src)))))))

(defn parsed-rule-expr? [clause] (instance? datalevin.parser.RuleExpr clause))

(defn rule-clause?
  "Check if clause is a rule call"
  [rules clause]
  (or (parsed-rule-expr? clause)
      (when (and (sequential? clause) (not (vector? clause)))
        (let [head (qu/clause-head clause)]
          (and (symbol? head)
               (not (qu/free-var? head))
               (not (qu/rule-head head))
               (contains? rules head))))))

(defn get-rule-args
  [clause]
  (if (parsed-rule-expr? clause)
    (mapv #(when (instance? Variable %) (:symbol %)) (:args clause))
    (qu/clause-args clause)))

(defn or-join-clause?
  [clause]
  (or (instance? Or clause)
      (and (sequential? clause)
           (not (vector? clause))
           (= 'or-join (first clause)))))

(defn get-or-join-vars
  [clause]
  (cond
    (instance? Or clause)
    (let [rule-vars (:rule-vars clause)]
      (when rule-vars
        (into []
              (comp (map :symbol) (filter qu/free-var?))
              (concat (:required rule-vars) (:free rule-vars)))))

    (or-join-clause? clause)
    (let [[_ vars & _] clause]
      (if (vector? (first vars))
        (into (vec (first vars)) (rest vars))
        (vec vars)))))

(defn get-or-join-branches
  [clause]
  (cond
    (instance? Or clause)
    (:clauses clause)

    (or-join-clause? clause)
    (let [[_ vars & branches] clause]
      (if (and (sequential? vars) (vector? (first vars)))
        branches
        branches))))

(defn infer-branch-source
  "Infer the source for an or-join branch.
   A branch can be:
   - A single pattern: [?e :attr ?v] or [$src ?e :attr ?v]
   - An (and ...) clause containing patterns
   Returns the source symbol (e.g., '$, '$src) or nil if ambiguous/unknown."
  [branch]
  (cond
    (instance? Pattern branch)
    (let [src (:source branch)]
      (if (instance? DefaultSrc src) '$ (:symbol src)))

    (instance? And branch)
    (when-let [first-clause (first (:clauses branch))]
      (infer-branch-source first-clause))

    (vector? branch)
    (if (qu/source? (first branch))
      (first branch)
      '$)

    (and (sequential? branch) (= 'and (first branch)))
    (when-let [first-clause (second branch)]
      (infer-branch-source first-clause))

    :else nil))

(defn single-source-or-join?
  [sources clause]
  (let [branches       (get-or-join-branches clause)
        branch-sources (keep infer-branch-source branches)]
    (and (seq branch-sources)
         (= (count branch-sources) (count branches))
         (apply = branch-sources)
         (contains? sources (first branch-sources)))))

(defn or-join-branch-valid?
  "Check if an or-join branch is valid for optimization.
   A branch is valid if it contains patterns that can be executed."
  [branch]
  (cond
    (instance? Pattern branch)
    true

    (instance? And branch)
    (every? #(or (instance? Pattern %)
                 (instance? Predicate %)
                 (instance? Function %))
            (:clauses branch))

    ;; Raw pattern vector
    (vector? branch)
    true

    ;; Raw (and ...) form
    (and (sequential? branch) (= 'and (first branch)))
    true

    ;; Other forms (not, not-join, nested or-join) - not valid for optimization
    :else false))

(defn or-join-optimizable?
  "Check if an or-join can be optimized as a link step in the query graph.
   An or-join is optimizable when:
   1. It's an or-join clause (not plain or)
   2. Has exactly ONE bound join var (from input or patterns)
   3. Has at least one unbound join var (to be derived)
   4. Bound var is not rule-derived (would create dependency issue)
   5. Bound var is a graph node (pattern entity var)
   6. All branches reference same source
   7. All branches are valid patterns

   Returns the clause if optimizable, nil otherwise."
  [sources resolved clause pattern-entity-vars rule-derived-vars]
  (when (or-join-clause? clause)
    (let [vars          (get-or-join-vars clause)
          ;; A variable is considered "will be bound" if it's:
          ;; 1. Already bound by input relations (in resolved), OR
          ;; 2. A pattern entity var (will be bound when patterns execute)
          will-be-bound (set/union resolved pattern-entity-vars)
          bound-vars    (filterv will-be-bound vars)
          free-vars     (filterv (complement will-be-bound) vars)]
      (when (and (= 1 (count bound-vars))
                 (seq free-vars)
                 (not (contains? (or rule-derived-vars #{})
                                 (first bound-vars)))
                 (contains? pattern-entity-vars (first bound-vars))
                 (single-source-or-join? sources clause)
                 (every? or-join-branch-valid? (get-or-join-branches clause)))
        clause))))

(defn find-rule-derived-vars
  "Find entity variables that should be derived from rules/or-join rather than
   scanned. A variable is rule-derived if:
   1. It appears in a rule call or or-join alongside an already-bound variable
   2. The patterns using it as entity have no value constraints

   This detects cases where a rule/or-join connects a bound var to an unbound
   var. "
  [clauses rules input-bound-vars optimizable-or-joins]
  (let [patterns             (filter #(instance? Pattern %) clauses)
        constrained-entities
        (into #{}
              (comp
                (filter
                  (fn [p]
                    (let [pat (:pattern p)]
                      (and (>= (count pat) 3)
                           (let [v (nth pat 2)]
                             (or (instance? Constant v)
                                 (and (instance? Variable v)
                                      (input-bound-vars (:symbol v)))))))))
                (map #(get-in % [:pattern 0 :symbol]))
                (filter qu/binding-var?))
              patterns)
        ref-connected
        (reduce
          (fn [m p]
            (let [pat   (:pattern p)
                  e-var (get-in pat [0 :symbol])
                  v-var (when (<= 3 (count pat))
                          (let [v (nth pat 2)]
                            (when (instance? Variable v) (:symbol v))))]
              (if (and (qu/binding-var? e-var) (qu/binding-var? v-var))
                (-> m
                    (update e-var (fnil conj #{}) v-var)
                    (update v-var (fnil conj #{}) e-var))
                m)))
          {} patterns)
        reachable
        (loop [frontier constrained-entities
               visited  #{}]
          (if (empty? frontier)
            visited
            (let [next-frontier (into #{}
                                      (comp
                                        (mapcat ref-connected)
                                        (remove visited))
                                      frontier)]
              (recur next-frontier (into visited frontier)))))
        all-pattern-vars
        (into #{}
              (comp
                (mapcat (fn [p]
                          (let [pat   (:pattern p)
                                e-var (get-in pat [0 :symbol])
                                v-var (when (>= (count pat) 3)
                                        (let [v (nth pat 2)]
                                          (when (instance? Variable v)
                                            (:symbol v))))]
                            (cond-> []
                              (qu/binding-var? e-var) (conj e-var)
                              (qu/binding-var? v-var) (conj v-var)))))
                (filter qu/binding-var?))
              patterns)
        all-entity-vars      (into #{}
                                   (comp
                                     (map #(get-in % [:pattern 0 :symbol]))
                                     (filter qu/binding-var?))
                                   patterns)
        unreachable-entities (set/difference all-entity-vars reachable)
        unreachable-vars     (set/difference all-pattern-vars reachable)
        directly-rule-derived
        (reduce
          (fn [derived clause]
            (if (or (parsed-rule-expr? clause) (rule-clause? rules clause))
              (let [args                (get-rule-args clause)
                    rule-vars           (filterv qu/binding-var? args)
                    has-reachable?      (some reachable rule-vars)
                    unreachable-in-rule (filter unreachable-entities rule-vars)]
                (if (and has-reachable? (seq unreachable-in-rule))
                  (into derived unreachable-in-rule)
                  derived))
              derived))
          #{} clauses)
        optimizable-set      (set optimizable-or-joins)
        or-join-derived
        (reduce
          (fn [derived clause]
            (if (or-join-clause? clause)
              (if (optimizable-set clause)
                derived
                (let [or-vars           (get-or-join-vars clause)
                      has-reachable?    (some reachable or-vars)
                      unreachable-in-or (filter unreachable-vars or-vars)]
                  (if (and has-reachable? (seq unreachable-in-or))
                    (into derived unreachable-in-or)
                    derived)))
              derived))
          directly-rule-derived clauses)
        indirectly-derived
        (loop [frontier or-join-derived
               derived  or-join-derived]
          (if (empty? frontier)
            derived
            (let [newly-derived (into #{}
                                      (comp
                                        (mapcat ref-connected)
                                        (filter unreachable-entities)
                                        (remove derived))
                                      frontier)]
              (recur newly-derived (into derived newly-derived)))))
        rule-only-indirectly
        (loop [frontier directly-rule-derived
               derived  directly-rule-derived]
          (if (empty? frontier)
            derived
            (let [newly-derived (into #{}
                                      (comp
                                        (mapcat ref-connected)
                                        (filter unreachable-entities)
                                        (remove derived))
                                      frontier)]
              (recur newly-derived (into derived newly-derived)))))]
    {:rule-derived (when (seq rule-only-indirectly) rule-only-indirectly)
     :all-derived  (when (seq indirectly-derived) indirectly-derived)}))

(defn depends-on-rule-output?
  "Check if a pattern's entity variable should be derived from a rule."
  [clause rule-derived-vars]
  (when (and rule-derived-vars (instance? Pattern clause))
    (let [e-var (get-in clause [:pattern 0 :symbol])]
      (and (qu/binding-var? e-var)
           (contains? rule-derived-vars e-var)))))

(defn split-clauses
  "Split clauses into two parts: opt-clauses (to be optimized) and late-clauses.
   Also identifies optimizable or-join clauses for graph integration."
  [{:keys [sources parsed-q rels rules] :as context}]
  (let [resolved    (reduce (fn [rs {:keys [attrs]}]
                              (set/union rs (set (keys attrs))))
                            #{} rels)
        ;; Variables bound by input relations
        input-bound (reduce (fn [s {:keys [attrs]}]
                              (into s (keys attrs)))
                            #{} rels)
        qwhere      (:qwhere parsed-q)
        clauses     (:qorig-where parsed-q)

        ;; Collect entity vars from patterns (these will be graph nodes)
        pattern-entity-vars (into #{}
                                  (comp
                                    (filter #(instance? Pattern %))
                                    (map #(get-in % [:pattern 0 :symbol]))
                                    (filter qu/binding-var?))
                                  qwhere)

        ;; Find optimizable or-joins
        opt-or-joins
        (filterv #(or-join-optimizable? sources resolved % pattern-entity-vars
                                        nil)
                 qwhere)

        ;; Find variables derived from rules
        ;; Use all-derived which includes both rule-derived and or-join-derived
        rule-derived
        (:all-derived
         (find-rule-derived-vars qwhere rules input-bound opt-or-joins))

        opt-or-join-set (set opt-or-joins)

        ptn-idxs
        (set (u/idxs-of
               (fn [clause]
                 (and (optimizable? sources resolved clause)
                      (not (depends-on-rule-output? clause rule-derived))))
               qwhere))

        or-join-idxs
        (set (u/idxs-of (fn [clause] (opt-or-join-set clause)) qwhere))]
    (assoc context
           :opt-clauses (u/keep-idxs ptn-idxs clauses)
           :optimizable-or-joins (u/keep-idxs or-join-idxs clauses)
           :late-clauses (u/remove-idxs (set/union ptn-idxs or-join-idxs)
                                        clauses))))

(defn make-node
  [[e patterns]]
  [e (reduce (fn [m pattern]
               (let [attr   (second pattern)
                     clause (Clause. attr nil nil nil nil nil)]
                 (if-let [v (qu/get-v pattern)]
                   (if (qu/free-var? v)
                     (update m :free conjv (assoc clause :var v))
                     (update m :bound conjv (assoc clause :val v)))
                   (update m :free conjv clause))))
             (Node. nil nil nil nil nil) patterns)])

(defn link-refs
  [graph]
  (let [es (set (keys graph))]
    (reduce-kv
      (fn [g e {:keys [free]}]
        (reduce
          (fn [g {:keys [attr var]}]
            (if (es var)
              (-> g
                  (update-in [e :links] conjv (Link. :ref var nil nil attr))
                  (update-in [var :links] conjv (Link. :_ref e nil nil attr)))
              g))
          g free))
      graph graph)))

(defn link-eqs
  [graph]
  (reduce-kv
    (fn [g v lst]
      (if (< 1 (count lst))
        (reduce
          (fn [g [[e1 k1] [e2 k2]]]
            (let [attrs {e1 k1 e2 k2}]
              (-> g
                  (update-in
                    [e1 :links] conjv (Link. :val-eq e2 v attrs nil))
                  (update-in
                    [e2 :links] conjv (Link. :val-eq e1 v attrs nil)))))
          g (u/combinations lst 2))
        g))
    graph (reduce-kv (fn [m e {:keys [free]}]
                       (reduce (fn [m {:keys [attr var]}]
                                 (if var (update m var conjv [e attr]) m))
                               m free))
                     {} graph)))

(defn make-nodes
  [[src patterns]]
  [src (let [patterns' (mapv qu/replace-blanks patterns)
             graph     (into {} (map make-node) (group-by first patterns'))]
         (if (< 1 (count graph))
           (-> graph link-refs link-eqs)
           graph))])

(defn extract-or-join-info
  "Extract bound/free variable information from an or-join clause.
   Currently, we only support single bound var. "
  [will-be-bound clause]
  (let [vars       (get-or-join-vars clause)
        bound-vars (filterv will-be-bound vars)
        free-vars  (filterv (complement will-be-bound) vars)]
    {:bound-var (first bound-vars)
     :free-vars free-vars
     :clause    clause}))

(defn find-nodes-using-vars
  "Find entity nodes that have patterns using any of the given vars in value
  position."
  [graph vars]
  (let [var-set (set vars)]
    (into []
          (comp
            (filter (fn [[_e node]]
                      (some (fn [{:keys [var]}]
                              (contains? var-set var))
                            (:free node))))
            (map first))
          graph)))

(defn link-or-joins
  "Add or-join links to the graph.
   Creates links from bound variable node to entity nodes that USE the free
   variables.
   Returns {:graph updated-graph :unlinked or-joins-that-couldnt-link}"
  [graph will-be-bound or-join-clauses source]
  (reduce
    (fn [{:keys [graph unlinked]} clause]
      (let [{:keys [bound-var free-vars]}
            (extract-or-join-info will-be-bound clause)
            target-entities (find-nodes-using-vars graph free-vars)]

        (if (and bound-var (seq target-entities) (contains? graph bound-var))
          {:graph
           (reduce
             (fn [g tgt-entity]
               (let [tgt-node (get graph tgt-entity)
                     tgt-attr (some (fn [fv]
                                      (some (fn [{:keys [attr var]}]
                                              (when (= var fv) attr))
                                            (:free tgt-node)))
                                    free-vars)]
                 (update-in g [bound-var :links] conjv
                            (OrJoinLink.
                              :or-join tgt-entity clause bound-var free-vars
                              tgt-attr source))))
             graph
             target-entities)
           :unlinked unlinked}
          {:graph    graph
           :unlinked (conj unlinked clause)})))
    {:graph graph :unlinked []} or-join-clauses))

(defn resolve-lookup-refs
  [sources [src patterns]]
  [src (mapv #(resolve-pattern-lookup-refs (sources src) %) patterns)])

(defn remove-src
  [[src patterns]]
  [src (mapv #(if (= (first %) src) (vec (rest %)) %) patterns)])

(defn get-src [[f & _]] (if (qu/source? f) f '$))

(defn get-or-join-source
  [clause]
  (or (infer-branch-source (first (get-or-join-branches clause))) '$))

(defn init-graph
  "Build one graph per Datalevin db from pattern clauses."
  [context]
  (let [opt-clauses          (:opt-clauses context)
        optimizable-or-joins (:optimizable-or-joins context)
        sources              (:sources context)
        rels                 (:rels context)

        ;; Get input-bound vars
        input-bound         (reduce (fn [s {:keys [attrs]}]
                                      (into s (keys attrs)))
                                    #{} rels)
        ;; Get pattern entity vars that will be bound
        ;; opt-clauses contains original vector patterns, not Pattern records
        pattern-entity-vars (into #{}
                                  (comp (filter vector?)
                                     (map first)  ;; entity position
                                     (filter qu/binding-var?))
                                  opt-clauses)
        will-be-bound       (into input-bound pattern-entity-vars)

        ;; Build base graph from patterns (grouped by source)
        base-graphs (into {}
                          (comp
                            (map remove-src)
                            (map #(resolve-lookup-refs sources %))
                            (map make-nodes))
                          (group-by get-src opt-clauses))

        ;; Add or-join links to the graph
        {:keys [graph unlinked]}
        (reduce
          (fn [{:keys [graph unlinked]} clause]
            (let [src (get-or-join-source clause)]
              (if-let [nodes (get graph src)]
                (let [result (link-or-joins nodes will-be-bound [clause] src)]

                  {:graph    (assoc graph src (:graph result))
                   :unlinked (into unlinked (:unlinked result))})
                ;; Source not in graph, can't link
                {:graph graph :unlinked (conj unlinked clause)})))
          {:graph base-graphs :unlinked []}
          optimizable-or-joins)]
    (-> context
        (assoc :graph graph)
        ;; Move unlinked or-joins to late-clauses
        (update :late-clauses into unlinked)
        ;; Clear optimizable-or-joins since they're now in the graph
        (assoc :optimizable-or-joins []))))

(defn pushdownable
  "predicates that can be pushed down involve only one free variable"
  [where gseq]
  (when (instance? Predicate where)
    (let [{:keys [args]} where
          syms           (qu/collect-vars args)]
      (when (= (count syms) 1)
        (let [s (first syms)]
          (some #(when (= s (:var %)) s) gseq))))))

(defn range-compare
  ([r1 r2]
   (range-compare r1 r2 true))
  ([[p i] [q j] from?]
   (case i
     :db.value/sysMin -1
     :db.value/sysMax 1
     (case j
       :db.value/sysMax -1
       :db.value/sysMin 1
       (let [res (compare i j)]
         (if (zero? res)
           (if from?
             (cond
               (identical? p q)       0
               (identical? p :closed) -1
               :else                  1)
             (cond
               (identical? p q)     0
               (identical? p :open) -1
               :else                1))
           res))))))

(def range-compare-to #(range-compare %1 %2 false))

(defn combine-ranges*
  [ranges]
  (let [orig-from (sort range-compare (map first ranges))]
    (loop [intervals (transient [])
           from      (rest orig-from)
           to        (sort range-compare-to (map peek ranges))
           thread    (transient [(first orig-from)])]
      (if (seq to)
        (let [fc (first from)
              tc (first to)]
          (if (= (count from) (count to))
            (recur (conj! intervals (persistent! thread)) (rest from) to
                   (transient [fc]))
            (if fc
              (if (< ^long (range-compare fc tc) 0)
                (recur intervals (rest from) to (conj! thread fc))
                (recur intervals from (rest to) (conj! thread tc)))
              (recur intervals from (rest to) (conj! thread tc)))))
        (mapv (fn [t] [(first t) (peek t)])
              (persistent! (conj! intervals (persistent! thread))))))))

(defn combine-ranges
  [ranges]
  (reduce
    (fn [vs [[cl l] [cr r] :as n]]
      (let [[[pcl pl] [pcr pr]] (peek vs)]
        (if (and (= pr l) (not (= pcr cl :open)))
          (conj (pop vs) [[pcl pl] [cr r]])
          (conj vs n))))
    [] (combine-ranges* ranges)))

(defn flip [c] (if (identical? c :open) :closed :open))

(defn flip-ranges
  ([ranges] (flip-ranges ranges c/v0 c/vmax))
  ([ranges v0 vmax]
   (let [vs (reduce
              (fn [vs [[cl l] [cr r]]]
                (-> vs
                    (assoc-in [(dec (count vs)) (count (peek vs))]
                              [(if (= l v0) cl (flip cl)) l])
                    (conj [[(if (= r vmax) cr (flip cr)) r]])))
              [[[:closed v0]]] ranges)]
     (assoc-in vs [(dec (count vs)) (count (peek vs))]
               [:closed vmax]))))

(defn intersect-ranges
  [& ranges]
  (let [n         (count ranges)
        ranges    (apply concatv ranges)
        orig-from (sort range-compare (map first ranges))
        res
        (loop [res  []
               from (rest orig-from)
               fp   (first orig-from)
               to   (sort range-compare-to (map peek ranges))
               i    1
               j    0]
          (let [tc (first to)]
            (if (seq from)
              (let [fc (first from)]
                (if (<= 0 ^long (range-compare fc tc))
                  (if (= i (+ j n))
                    (recur (conj res [fp tc]) (rest from) fc
                           (drop n to) (inc i) i)
                    (recur res (rest from) fc to (inc i) j))
                  (recur res (rest from) fc to (inc i) j)))
              (if (and (<= ^long (range-compare fp tc) 0) (= i (+ j n)))
                (conj res [fp tc])
                res))))]
    (when (seq res) res)))

(defn add-range [m & rs]
  (let [old-range (:range m)]
    (assoc m :range (if old-range
                      (if-let [new-range (intersect-ranges old-range rs)]
                        new-range
                        :empty-range)
                      (combine-ranges rs)))))

(defn prefix-max-string
  [^String prefix]
  (let [n (alength (.getBytes prefix))]
    (if (< n c/+val-bytes-wo-hdr+)
      (let [l  (- c/+val-bytes-wo-hdr+ n)
            ba (byte-array l)]
        (Arrays/fill ba (unchecked-byte 0xFF))
        (str prefix (String. ba)))
      prefix)))

(def ^:const wildm (int \%))
(def ^:const wilds (int \_))
(def ^:const max-string (b/text-ba->str c/max-bytes))

(defn like-convert-range
  "turn wildcard-free prefix into range"
  [m ^String pattern not?]
  (let [wm-s (.indexOf pattern wildm)
        ws-s (.indexOf pattern wilds)]
    (cond
      (or (zero? wm-s) (zero? ws-s)) m
      ;; not-like w/ a wildcard-free pattern
      (== wm-s ws-s -1)
      (add-range m [[:closed ""] [:open pattern]]
                 [[:open pattern] [:closed max-string]])
      :else
      (let [min-s    (min wm-s ws-s)
            end      (if (== min-s -1) (max wm-s ws-s) min-s)
            prefix-s (subs pattern 0 end)
            prefix-e (prefix-max-string prefix-s)
            range    [[:closed prefix-s] [:closed prefix-e]]]
        (if not?
          (apply add-range m (flip-ranges [range] "" max-string))
          (add-range m range))))))

(defn like-pattern-as-string
  "Used for plain text matching, e.g. as bounded val or range, not as FSM"
  [^String pattern escape]
  (let [esc (str (or escape \!))]
    (-> pattern
        (str/replace (str esc esc) esc)
        (str/replace (str esc "%") "%")
        (str/replace (str esc "_") "_"))))

(defn wildcard-free-like-pattern
  [^String pattern {:keys [escape]}]
  (LikeFSM/isValid (.getBytes pattern) (or escape \!))
  (let [pstring (like-pattern-as-string pattern escape)]
    (when (and (not (str/includes? pstring "%"))
               (not (str/includes? pstring "_")))
      pstring)))

(defn activate-var-pred
  [var clause]
  (when clause
    (if (fn? clause)
      clause
      (let [[f & args] clause
            idxs       (u/idxs-of #(= var %) args) ;; may appear more than once
            ni         (count idxs)
            idxs-arr   (int-array idxs)
            args-arr   (object-array args)
            call       (make-call (resolve-pred-simple f))]
        (fn var-pred [x]
          (dotimes [i ni] (aset args-arr (aget idxs-arr i) x))
          (call args-arr))))))

(defn add-pred
  ([old-pred new-pred]
   (add-pred old-pred new-pred false))
  ([old-pred new-pred or?]
   (if new-pred
     (if old-pred
       (if or?
         (fn [x] (or (old-pred x) (new-pred x)))
         (fn [x] (and (old-pred x) (new-pred x))))
       new-pred)
     old-pred)))

(defn optimize-like
  [m pred [_ ^String pattern {:keys [escape]}] v not?]
  (let [pstring (like-pattern-as-string pattern escape)
        m'      (update m :pred add-pred (activate-var-pred v pred))]
    (like-convert-range m' pstring not?)))

(defn inequality->range
  [m f args v]
  (let [args (vec args)
        ac-1 (dec (count args))
        i    ^long (u/index-of #(= % v) args)
        fa   (first args)
        pa   (peek args)]
    (case f
      <  (cond
           (zero? i)  (add-range m [[:closed c/v0] [:open pa]])
           (= i ac-1) (add-range m [[:open fa] [:closed c/vmax]])
           :else      (add-range m [[:open fa] [:open pa]]))
      <= (cond
           (zero? i)  (add-range m [[:closed c/v0] [:closed pa]])
           (= i ac-1) (add-range m [[:closed fa] [:closed c/vmax]])
           :else      (add-range m [[:closed fa] [:closed pa]]))
      >  (cond
           (zero? i)  (add-range m [[:open pa] [:closed c/vmax]])
           (= i ac-1) (add-range m [[:closed c/v0] [:open fa]])
           :else      (add-range m [[:open pa] [:open fa]]))
      >= (cond
           (zero? i)  (add-range m [[:closed pa] [:closed c/vmax]])
           (= i ac-1) (add-range m [[:closed c/v0] [:closed fa]])
           :else      (add-range m [[:closed pa] [:closed fa]])))))

(defn range->inequality
  [v [[so sc :as s] [eo ec :as e]]]
  (cond
    (= s [:closed c/v0])
    (if (identical? eo :open) (list '< v ec) (list '<= v ec))
    (= e [:closed c/vmax])
    (if (identical? so :open) (list '< sc v) (list '<= sc v))
    :else
    (if (identical? so :open) (list '< sc v ec) (list '<= sc v ec))))

(defn equality->range
  [m args]
  (let [c (some #(when-not (qu/free-var? %) %) args)]
    (add-range m [[:closed c] [:closed c]])))

(defn in-convert-range
  [m [_ coll] not?]
  (assert (and (coll? coll) (not (map? coll)))
          "function `in` expects a collection")
  (apply add-range m
         (let [ranges (map (fn [v] [[:closed v] [:closed v]]) (sort coll))]
           (if not? (flip-ranges ranges) ranges))))

(defn nested-pred
  [f args v]
  (let [len      (count args)
        fn-arr   (object-array len)
        args-arr (object-array args)
        call     (make-call (resolve-pred-simple f))]
    (dotimes [i len]
      (let [arg (aget args-arr i)]
        (when (list? arg)
          (aset fn-arr i (if (some #(list? %) arg)
                           (nested-pred (first arg) (rest arg) v)
                           (activate-var-pred v arg))))))
    (fn [x]
      (dotimes [i len]
        (when-some [f (aget fn-arr i)]
          (aset args-arr i (f x))))
      (call args-arr))))

(defn split-and-clauses
  "If pred is an (and ...) form where every arg is a predicate list,
  return a flat seq of those predicate lists; otherwise nil."
  [pred]
  (when (and (list? pred) (= 'and (first pred)))
    (let [args (rest pred)]
      (when (every? list? args)
        (mapcat (fn [arg]
                  (or (split-and-clauses arg) [arg]))
                args)))))

(defn add-pred-clause
  [graph clause v]
  (let [pred       (first clause)
        and-clauses (split-and-clauses pred)
        preds      (or and-clauses [pred])
        apply-pred (fn [m pred]
                     (let [[f & args] pred]
                       (if (some list? pred)
                         (update m :pred add-pred (nested-pred f args v))
                         (case f
                           (< <= > >=) (inequality->range m f args v)
                           =           (equality->range m args)
                           like        (optimize-like m pred args v false)
                           not-like    (optimize-like m pred args v true)
                           in          (in-convert-range m args false)
                           not-in      (in-convert-range m args true)
                           (update m :pred add-pred (activate-var-pred v pred))))))]
    (w/postwalk
      (fn [m]
        (if (= (:var m) v)
          (reduce apply-pred m preds)
          m))
      graph)))

(defn free->bound
  "cases where free var can be rewritten as bound:
    * like pattern is free of wildcards"
  [graph clause v]
  (w/postwalk
    (fn [m]
      (if-let [free (:free m)]
        (if-let [[new k]
                 (u/some-indexed
                   (fn [{:keys [var] :as old}]
                     (when (= v var)
                       (let [[f & args] (first clause)]
                         (when (= f 'like)
                           (let [[_ pattern opts] args]
                             (when-let [ps (wildcard-free-like-pattern
                                             pattern opts)]
                               (-> old
                                   (dissoc :var)
                                   (assoc :val ps))))))))
                   free)]
          (-> m
              (update :bound conjv new)
              (update :free u/vec-remove k))
          m)
        m))
    graph))

(defn pushdown-predicates
  "optimization that pushes predicates down to value scans"
  [{:keys [parsed-q graph] :as context}]
  (let [gseq (tree-seq coll? seq graph)]
    (u/reduce-indexed
      (fn [c where i]
        (if-let [v (pushdownable where gseq)]
          (let [clause (nth (:qorig-where parsed-q) i)]
            (-> c
                (update :late-clauses #(remove #{clause} %))
                (update :opt-clauses conj clause)
                (update :graph #(free->bound % clause v))
                (update :graph #(add-pred-clause % clause v))))
          c))
      context (:qwhere parsed-q))))

(defn estimate-round [x]
  (let [v (Math/ceil (double x))]
    (if (>= v (double Long/MAX_VALUE))
      Long/MAX_VALUE
      (long v))))

(defn attr-var [{:keys [var]}] (or var '_))

(defn build-graph
  "Split clauses, turn the group of clauses to be optimized into a query
  graph that looks like this:
  {$
    {?e  {:links [{:type :ref :tgt ?e0 :attr :friend}
                  {:type :val-eq :tgt ?e1 :var ?a :attrs {?e :age ?e1 :age}}]
          :mpath [:bound 0]
          :bound [{:attr :name :val \"Tom\" :count 5}]
          :free  [{:attr :age :var ?a :range [[:less-than 18]] :count 1089}
                  {:attr :school :var ?s :count 108 :pred [(.startsWith ?s \"New\")]}
                  {:attr :friend :var ?e0 :count 2500}]}
    ?e0 {:links [{:type :_ref :tgt ?e :attr :friend}]
         :mpath [:free 0]
         :free  [{:attr :age :var ?a :count 10890}]}
    ...
    }}

  Remaining clauses are in :late-clauses. "
  [context]
  (-> context
      split-clauses
      init-graph
      pushdown-predicates))

(defn nillify [v] (if (or (identical? v c/v0) (identical? v c/vmax)) nil v))

(defn range->start-end [[[_ lv] [_ hv]]] [(nillify lv) (nillify hv)])

(defn range-count
  [db attr ranges]
  (if (identical? ranges :empty-range)
    0
    (reduce
      (fn ^long [^long sum range]
        (let [[lv hv] (range->start-end range)]
          (unchecked-add sum ^long (db/-index-range-size db attr lv hv))))
      0 ranges)))

(defn count-node-datoms
  [^DB db {:keys [free bound] :as node}]
  (let [store (.-store db)]
    (reduce
      (fn [{:keys [mcount] :as node} [k i {:keys [attr val range]}]]
        (let [^long c (cond
                        (some? val) (av-size store attr val)
                        range       (range-count db attr range)
                        :else       (db/-count db [nil attr nil]))]
          (cond
            (zero? c)          (reduced (assoc node :mcount 0))
            (< c ^long mcount) (-> node
                                   (assoc-in [k i :count] c)
                                   (assoc :mcount c :mpath [k i]))
            :else              (assoc-in node [k i :count] c))))
      (assoc node :mcount Long/MAX_VALUE)
      (let [flat (fn [k m] (map-indexed (fn [i clause] [k i clause]) m))]
        (concat (flat :bound bound) (flat :free free))))))

(defn count-known-e-datoms
  [db e {:keys [free] :as node}]
  (u/reduce-indexed
    (fn [{:keys [mcount] :as node} {:keys [attr]} i]
      (let [^long c (db/-count db [e attr nil])]
        (cond
          (zero? c)          (reduced (assoc node :mcount 0))
          (< c ^long mcount) (-> node
                                 (assoc-in [:free i :count] c)
                                 (assoc :mcount c :mpath [:free i]))
          :else              (assoc-in node [:free i :count] c))))
    (assoc node :mcount Long/MAX_VALUE) free))

(defn count-datoms
  [db e node]
  (unreduced (if (int? e)
               (count-known-e-datoms db e node)
               (count-node-datoms db node))))

(defn add-back-range
  [v {:keys [pred range]}]
  (if range
    (reduce
      (fn [p r]
        (if r
          (add-pred p (activate-var-pred v (range->inequality v r)) true)
          p))
      pred range)
    pred))

(defn attrs-vec
  [attrs preds skips fidxs]
  (mapv (fn [a p f]
          [a (cond-> {:pred p :skip? false :fidx nil}
               (skips a) (assoc :skip? true)
               f         (assoc :fidx f :skip? true))])
        attrs preds fidxs))

(defn aid [db] #(((db/-schema db) %) :db/aid))

(defn init-steps
  [db e node single?]
  (let [{:keys [bound free mpath mcount]}            node
        {:keys [attr var val range pred] :as clause} (get-in node mpath)

        know-e? (int? e)
        no-var? (or (not var) (qu/placeholder? var))

        init (cond-> (InitStep. attr nil nil nil [e] nil [e] nil nil nil nil
                                (:count clause) nil nil)
               var     (assoc :pred pred
                              :vars (cond-> [e]
                                      (not no-var?) (conj var))
                              :range range)
               val     (assoc :val val)
               know-e? (assoc :know-e? true)
               true    (#(let [vars (:vars %)]
                           (assoc % :cols (if (= 1 (count vars))
                                            [e]
                                            [e #{attr var}])
                                  :strata [(set vars)]
                                  :seen-or-joins #{})))

               (not single?)
               (#(if (< ^long c/init-exec-size-threshold ^long mcount)
                   (assoc % :sample (-sample % db nil))
                   (assoc % :result (-execute % db nil)))))]
    (cond-> [init]
      (< 1 (+ (count bound) (count free)))
      (conj
        (let [[k i]   mpath
              bound1  (mapv (fn [{:keys [val] :as b}]
                              (-> b
                                  (update :pred add-pred #(= val %))
                                  (assoc :var (gensym "?bound"))))
                            (if (= k :bound) (u/vec-remove bound i) bound))
              all     (->> (concatv bound1
                                    (if (= k :free) (u/vec-remove free i) free))
                           (sort-by (fn [{:keys [attr]}] ((aid db) attr))))
              attrs   (mapv :attr all)
              vars    (mapv attr-var all)
              skips   (cond-> (set (sequence
                                     (comp (map (fn [a v]
                                               (when (or (= v '_)
                                                         (qu/placeholder? v))
                                                 a)))
                                        (remove nil?))
                                     attrs vars))
                        no-var? (conj attr))
              preds   (mapv add-back-range vars all)
              attrs-v (attrs-vec attrs preds skips (repeat nil))
              cols    (into (:cols init)
                            (sequence
                              (comp (map (fn [a v] (when-not (skips a) #{a v})))
                                 (remove nil?))
                              attrs vars))
              strata  (conj (:strata init) (set vars))
              ires    (:result init)
              isp     (:sample init)
              step    (MergeScanStep. 0 attrs-v vars [e] [e] cols strata
                                      #{} nil nil)]
          (cond-> step
            ires (assoc :result (-execute step db ires))
            isp  (assoc :sample (-sample step db isp))))))))

(defn n-items
  [attrs-v k]
  (reduce
    (fn [^long c [_ m]] (if (m k) (inc c) c))
    0 attrs-v))

(defn estimate-scan-v-size
  [^long e-size steps]
  (cond+
    (= (count steps) 1) e-size ; no merge step

    :let [{:keys [know-e?] res1 :result sp1 :sample} (first steps)
          {:keys [attrs-v result sample]} (peek steps)]

    know-e? (count attrs-v)

    :else
    (estimate-round
      (* e-size (double
                  (cond
                    result (let [s (.size ^List result)]
                             (if (< 0 s)
                               (/ s (.size ^List res1))
                               c/magic-scan-ratio))
                    sample (let [s (.size ^List sample)]
                             (if (< 0 s)
                               (/ s (.size ^List sp1))
                               c/magic-scan-ratio))))))))

(defn factor
  [magic ^long n]
  (if (zero? n) 1 ^long (estimate-round (* ^double magic n))))

(defn estimate-scan-v-cost
  [{:keys [attrs-v vars]} ^long size]
  (* size
     ^double c/magic-cost-merge-scan-v
     ^long (factor c/magic-cost-var (count vars))
     ^long (factor c/magic-cost-pred (n-items attrs-v :pred))
     ^long (factor c/magic-cost-fidx (n-items attrs-v :fidx))))

(defn estimate-base-cost
  [{:keys [mcount]} steps]
  (let [{:keys [pred]} (first steps)
        init-cost      (estimate-round
                         (cond-> (* ^double c/magic-cost-init-scan-e
                                    ^long mcount)
                           pred (* ^double c/magic-cost-pred)))]
    (if (< 1 (count steps))
      (+ ^long init-cost ^long (estimate-scan-v-cost (peek steps) mcount))
      init-cost)))

(defn base-plan
  ([db nodes e]
   (base-plan db nodes e false))
  ([db nodes e single?]
   (let [node   (get nodes e)
         mcount (:mcount node)]
     (when-not (zero? ^long mcount)
       (let [isteps (init-steps db e node single?)]
         (if single?
           (Plan. isteps nil nil 0)
           (Plan. isteps
                  (estimate-base-cost node isteps)
                  (estimate-scan-v-size mcount isteps)
                  0)))))))

(defn writing? [db] (l/writing? (.-lmdb ^Store (.-store ^DB db))))

(defn update-nodes
  [db nodes]
  (if (= (count nodes) 1)
    (let [[e node] (first nodes)] {e (count-datoms db e node)})
    (let [f (fn [e] [e (count-datoms db e (get nodes e))])]
      (into {} (if (writing? db)
                 (map f (keys nodes))
                 (map+ f (keys nodes)))))))

(defn build-base-plans
  [db nodes component]
  (let [f (fn [e] [[e] (base-plan db nodes e)])]
    (into {} (if (writing? db)
               (map f component)
               (map+ f component)))))

(defn find-index
  [a-or-v cols]
  (when a-or-v
    (u/index-of (fn [x] (if (set? x) (x a-or-v) (= x a-or-v))) cols)))

(defn merge-scan-step
  [db last-step index new-key new-steps]
  (let [in       (:out last-step)
        out      (if (set? in) (set new-key) new-key)
        lcols    (:cols last-step)
        lstrata  (:strata last-step)
        ncols    (:cols (peek new-steps))
        [s1 s2]  new-steps
        val1     (:val s1)
        [_ v1]   (:vars s1)
        a1       (:attr s1)
        ip       (cond-> (add-back-range v1 s1)
                   (some? val1) (add-pred #(= % val1)))
        attrs-v2 (:attrs-v s2)
        get-a    (fn [coll] (some #(when (keyword? %) %) coll))
        [attrs-v vars cols]
        (reduce
          (fn [[attrs-v vars cols] col]
            (let [v (some #(when (symbol? %) %) col)]
              (if (and ip (= v v1))
                [attrs-v vars cols]
                (let [a (get-a col)
                      p (some #(when (= a (first %)) (:pred (peek %))) attrs-v2)]
                  (if-let [f (find-index v lcols)]
                    [(conj attrs-v [a {:pred p :skip? true :fidx f}]) vars cols]
                    [(conj attrs-v [a {:pred  p
                                       :skip? (if (some #(when (= a (first %))
                                                           (:skip? (peek %)))
                                                        attrs-v2)
                                                true false)
                                       :fidx  nil}])
                     (conj vars v) (conj cols col)])))))
          (if (or ip (nil? v1))
            [[[a1 {:pred  ip
                   :skip? (if (and v1 (find-index v1 ncols)) false true)
                   :fidx  nil}]]
             (if v1 [v1] [])
             (if v1 [#{a1 v1}] [])]
            [[] [] []])
          (rest ncols))
        fcols    (into lcols (sort-by (comp (aid db) get-a) cols))
        strata   (conj lstrata (set vars))
        lseen    (:seen-or-joins last-step)]
    (MergeScanStep. index attrs-v vars in out fcols strata lseen nil nil)))

(defn index-by-link
  [cols link-e link]
  (case (:type link)
    :ref     (or (find-index (:tgt link) cols)
                 (find-index (:attr link) cols))
    :_ref    (find-index link-e cols)
    :val-eq  (or (find-index (:var link) cols)
                 (find-index ((:attrs link) link-e) cols))
    ;; For or-join, return index where tgt will be after step adds free-vars
    ;; and tgt
    :or-join (+ (count cols) (count (:free-vars link)))))

(defn enrich-cols
  [cols index attr]
  (let [pa (cols index)]
    (mapv (fn [e] (if (and (= e pa) (set? e)) (conj e attr) e)) cols)))

(defn col-var
  [col]
  (if (set? col)
    (some #(when (symbol? %) %) col)
    col))

(defn col-attrs
  [col]
  (if (set? col)
    (into #{} (filter keyword?) col)
    #{}))

(defn merge-join-cols
  "Merge input and target cols for hash join output, preserving input order.
   Returns [merged-cols new-vars]."
  [in-cols tgt-cols]
  (let [in-vars    (mapv col-var in-cols)
        tgt-vars   (mapv col-var tgt-cols)
        tgt-map    (zipmap tgt-vars tgt-cols)
        in-var-set (set in-vars)
        merged-in  (mapv (fn [col v]
                           (if-let [tcol (tgt-map v)]
                             (let [attrs (set/union (col-attrs col)
                                                    (col-attrs tcol))]
                               (if (seq attrs)
                                 (conj attrs v)
                                 v))
                             col))
                         in-cols in-vars)
        new-cols   (reduce (fn [acc [v col]]
                             (if (in-var-set v) acc (conj acc col)))
                           [] (map vector tgt-vars tgt-cols))
        new-vars   (set/difference (set tgt-vars) in-var-set)]
    [(into merged-in new-cols) new-vars]))

(defn link-step
  [type last-step index attr tgt new-key]
  (let [in      (:out last-step)
        out     (if (set? in) (set new-key) new-key)
        lcols   (:cols last-step)
        lstrata (:strata last-step)
        lseen   (:seen-or-joins last-step)
        fidx    (find-index tgt lcols)
        cols    (cond-> (enrich-cols lcols index attr)
                  (nil? fidx) (conj tgt))]
    [(LinkStep. type index attr tgt fidx in out cols (conj lstrata #{tgt}) lseen)
     (or fidx (dec (count cols)))]))

(defn rev-ref-plan
  [db last-step index {:keys [type attr tgt]} new-key new-steps]
  (let [[step n-index] (link-step type last-step index attr tgt new-key)]
    (if (= 1 (count new-steps))
      [step]
      [step (merge-scan-step db step n-index new-key new-steps)])))

(defn val-eq-plan
  [db last-step index {:keys [type attrs tgt]} new-key new-steps]
  (let [attr           (attrs tgt)
        [step n-index] (link-step type last-step index attr tgt new-key)]
    (if (= 1 (count new-steps))
      [step]
      [step (merge-scan-step db step n-index new-key new-steps)])))

(defn hash-join-plan
  [_db {:keys [steps cost size]} link-e link new-key
   new-base-plan result-size]
  (let [last-step       (peek steps)
        in              (:out last-step)
        out             (if (set? in) (set new-key) new-key)
        lcols           (:cols last-step)
        lstrata         (:strata last-step)
        lseen           (:seen-or-joins last-step)
        tgt-steps       (:steps new-base-plan)
        in-size         (or size 0)
        tgt-size        (or (:size new-base-plan) 0)
        tgt-cols        (:cols (peek tgt-steps))
        [cols new-vars] (merge-join-cols lcols tgt-cols)
        step            (HashJoinStep. link link-e in out lcols cols
                                       (conj lstrata new-vars) lseen tgt-steps
                                       in-size tgt-size)
        base-cost       (or (:cost new-base-plan) 0)
        join-cost       (estimate-hash-join-cost in-size tgt-size)]
    (Plan. [step]
           (+ ^long cost ^long base-cost ^long join-cost)
           result-size
           (- ^long (find-index link-e (:strata last-step))))))

(defn or-join-plan*
  [db sources rules last-step
   {:keys [clause bound-var free-vars tgt tgt-attr]} new-key new-base]
  (let [in        (:out last-step)
        out       (if (set? in) (set new-key) new-key)
        lcols     (:cols last-step)
        lstrata   (:strata last-step)
        lseen     (:seen-or-joins last-step)
        bound-idx (find-index bound-var lcols)
        or-cols   (-> lcols (into free-vars) (conj tgt))
        or-seen   (conj lseen clause)
        or-step   (OrJoinStep. clause
                               bound-var
                               bound-idx
                               free-vars
                               tgt
                               tgt-attr
                               sources
                               rules
                               in out or-cols
                               (conj lstrata #{tgt})
                               or-seen)
        tgt-idx   (dec (count or-cols))]
    (if new-base
      (let [new-steps (:steps new-base)]
        [or-step (merge-scan-step db or-step tgt-idx new-key new-steps)])
      [or-step])))

(defn count-init-follows
  [^DB db tuples attr index]
  (let [store (.-store db)]
    (rd/fold
      +
      (rd/map #(av-size store attr (aget ^objects % index))
              (p/remove-end-scan tuples)))))

(defn count-init-follows-stats
  [^DB db tuples attr index]
  (let [store    (.-store db)
        ^List ts (p/remove-end-scan tuples)
        n        (.size ts)]
    (loop [i     0
           sum   0.0
           sumsq 0.0
           mx    0.0]
      (if (< i n)
        (let [^objects t (.get ts i)
              f          (double (av-size store attr (aget t index)))]
          (recur (u/long-inc i)
                 (+ sum f)
                 (+ sumsq (* f f))
                 (if (> f mx) f mx)))
        {:n       n
         :sum     sum
         :sumsq   sumsq
         :max-val mx}))))

(defn link-ratio-key
  [link-e {:keys [type attr attrs tgt]}]
  (case type
    :val-eq [type (attrs link-e) (attrs tgt)]
    :_ref   [type attr]
    [type attr]))

(defn estimate-link-size
  [db link-e {:keys [type attr attrs tgt var]} ^ConcurrentHashMap ratios
   prev-size prev-plan index]
  (let [prev-steps              (:steps prev-plan)
        attr                    (or attr (attrs tgt))
        ratio-key               (link-ratio-key link-e {:type  type
                                                        :attr  attr
                                                        :var   var
                                                        :attrs attrs
                                                        :tgt   tgt})
        {:keys [result sample]} (peek prev-steps)
        ^long ssize             (if sample (.size ^List sample) 0)
        ^long rsize             (if result (.size ^List result) 0)]
    (estimate-round
      (cond
        (< 0 ssize)
        (let [{:keys [^long n ^double sum ^double sumsq ^double max-val]}
              (count-init-follows-stats db sample attr index)

              mean       (if (pos? n) (/ sum (double n)) 0.0)
              variance   (if (pos? n)
                           (max 0.0 (- (/ sumsq (double n)) (* mean mean)))
                           0.0)
              cv2        (if (pos? mean) (/ variance (* mean mean)) 0.0)
              base-ratio (double (db/-default-ratio db attr))
              k-eff      (* (double c/link-estimate-prior-size)
                            (+ 1.0 (* (double c/link-estimate-var-alpha) cv2)))
              blended    (if (pos? (+ (double n) k-eff))
                           (/ (+ sum (* k-eff base-ratio))
                              (+ (double n) k-eff))
                           base-ratio)
              ub         (if (pos? n)
                           (min (+ mean (/ (- max-val mean)
                                           (Math/sqrt (double n))))
                                (* mean (double c/link-estimate-max-multi)))
                           base-ratio)
              ratio      (max base-ratio blended ub (double c/magic-link-ratio))]
          (.put ratios ratio-key ratio)
          (* (double prev-size) ratio))

        (< 0 rsize)
        (let [^long size (count-init-follows db result attr index)
              ratio      (/ size rsize)]
          (.put ratios ratio-key ratio)
          size)

        (.containsKey ratios ratio-key)
        (* ^long prev-size ^double (.get ratios ratio-key))

        :else
        (let [ratio (db/-default-ratio db attr)]
          (.put ratios ratio-key ratio)
          (* ^long prev-size ^double ratio))))))

(defn count-or-join-follows
  "Execute or-join on input tuples and count output size."
  [or-join-exec-fn db sources rules tuples
   {:keys [clause bound-var free-vars tgt-attr]} bound-idx]
  (let [result (or-join-exec-fn db sources rules tuples clause bound-var
                                bound-idx free-vars tgt-attr)]
    (.size ^List result)))

(defn estimate-or-join-size
  [or-join-exec-fn db sources rules ^ConcurrentHashMap ratios prev-plan link]
  (let [prev-size               (:size prev-plan)
        prev-steps              (:steps prev-plan)
        last-step               (peek prev-steps)
        bound-idx               (find-index (:bound-var link) (:cols last-step))
        ratio-key               [:or-join (:bound-var link) (:tgt link)]
        {:keys [result sample]} last-step
        ^long ssize             (if sample (.size ^List sample) 0)
        ^long rsize             (if result (.size ^List result) 0)]
    (estimate-round
      (cond
        (< 0 ssize)
        (let [size  (count-or-join-follows or-join-exec-fn db sources rules
                                           sample link bound-idx)
              ratio (max (double (/ ^long size ssize))
                         ^double c/magic-or-join-ratio)]
          (.put ratios ratio-key ratio)
          (* ^long prev-size ratio))

        (< 0 rsize)
        (let [^long size (count-or-join-follows or-join-exec-fn db sources
                                                rules result link bound-idx)
              ratio      (/ size rsize)]
          (.put ratios ratio-key ratio)
          size)

        (.containsKey ratios ratio-key)
        (* ^long prev-size ^double (.get ratios ratio-key))

        :else
        (do (.put ratios ratio-key c/magic-or-join-ratio)
            (* ^long prev-size ^double c/magic-or-join-ratio))))))

(defn estimate-join-size
  [or-join-exec-fn db sources rules link-e link ratios prev-plan index
   new-base-plan]
  (let [prev-size (:size prev-plan)
        steps     (:steps new-base-plan)]
    (case (:type link)
      :ref     [nil (estimate-scan-v-size prev-size steps)]
      :or-join (let [or-size (estimate-or-join-size or-join-exec-fn db sources
                                                    rules ratios prev-plan link)]
                 ;; or-join doesn't have new-base-plan steps to merge
                 [or-size or-size])
      ;; :_ref and :val-eq
      (let [e-size (estimate-link-size db link-e link ratios prev-size
                                       prev-plan index)]
        [e-size (estimate-scan-v-size e-size steps)]))))

(defn estimate-link-cost
  [^long outer-size ^long result-size]
  (estimate-round
    (+ (* outer-size ^double c/magic-cost-link-probe)
       (* result-size ^double c/magic-cost-link-retrieval))))

(defn estimate-hash-join-cost
  [^long left-size ^long right-size]
  (estimate-round (* ^double c/magic-cost-hash-join
                     (+ left-size right-size))))

(defn estimate-e-plan-cost
  [prev-size e-size cur-steps]
  (let [step1 (first cur-steps)]
    (if (= 1 (count cur-steps))
      (if (identical? (-type step1) :merge)
        (estimate-scan-v-cost step1 prev-size)
        (estimate-link-cost prev-size e-size))
      (+ ^long (estimate-link-cost prev-size e-size)
         ^long (estimate-scan-v-cost (peek cur-steps) e-size)))))

(defn e-plan
  [db {:keys [steps cost size]} index link-e link new-key new-base-plan e-size
   result-size]
  (let [new-steps (:steps new-base-plan)
        last-step (peek steps)
        cur-steps
        (case (:type link)
          :ref    [(merge-scan-step db last-step index new-key new-steps)]
          :_ref   (rev-ref-plan db last-step index link new-key new-steps)
          :val-eq (val-eq-plan db last-step index link new-key new-steps))]
    (Plan. cur-steps
           (+ ^long cost ^long (estimate-e-plan-cost size e-size cur-steps))
           result-size
           (- ^long (find-index link-e (:strata last-step))))))

(defn compare-plans
  "Compare two plans. Prefer lower cost, then lower size as tiebreaker."
  [p1 p2]
  (let [c1 ^long (:cost p1)
        c2 ^long (:cost p2)]
    (if (= c1 c2)
      (if (< ^long (:size p2) ^long (:size p1)) p2 p1)
      (if (< ^long c2 ^long c1) p2 p1))))

(defn or-join-plan
  [or-join-exec-fn base-plans new-e db sources rules ratios prev-plan link
   last-step new-key link-e]
  (let [new-base  (base-plans [new-e])
        or-size   (estimate-or-join-size or-join-exec-fn db sources rules
                                         ratios prev-plan link)
        cur-steps (or-join-plan* db sources rules last-step link new-key
                                 new-base)
        or-cost   (estimate-e-plan-cost (:size prev-plan) or-size cur-steps)]
    (Plan. cur-steps
           (+ ^long (:cost prev-plan) ^long or-cost)
           or-size
           (- ^long (find-index link-e (:strata last-step))))))

(defn binary-plan*
  [or-join-exec-fn db sources rules base-plans ratios prev-plan link-e new-e
   link new-key]
  (let [last-step (peek (:steps prev-plan))
        index     (index-by-link (:cols last-step) link-e link)
        link-type (:type link)]
    (if (identical? :or-join link-type)
      (or-join-plan or-join-exec-fn base-plans new-e db sources rules ratios
                    prev-plan link last-step new-key link-e)
      (let [new-base (base-plans [new-e])
            [e-size result-size]
            (estimate-join-size or-join-exec-fn db sources rules link-e link
                                ratios prev-plan index new-base)]
        (if (and (#{:_ref :val-eq} link-type) new-base)
          (let [link-plan (e-plan db prev-plan index link-e link new-key
                                  new-base e-size result-size)]
            (if (< ^long (:size prev-plan) ^long c/hash-join-min-input-size)
              link-plan
              (let [hash-plan (hash-join-plan db prev-plan link-e link new-key
                                              new-base result-size)]
                (compare-plans link-plan hash-plan))))
          (e-plan db prev-plan index link-e link new-key new-base e-size
                  result-size))))))

(defn binary-plan
  [or-join-exec-fn db sources rules nodes base-plans ratios prev-plan link-e
   new-e new-key]
  (let [last-step     (peek (:steps prev-plan))
        seen-or-joins (or (:seen-or-joins last-step) #{})
        links         (get-in nodes [link-e :links])
        filtered-links
        (into []
              (comp
                (filter #(= new-e (:tgt %)))
                (filter #(or (not= :or-join (:type %))
                             (not (contains? seen-or-joins (:clause %))))))
              links)
        candidates
        (mapv #(binary-plan* or-join-exec-fn db sources rules base-plans ratios
                             prev-plan link-e new-e % new-key)
              filtered-links)]
    (when (seq candidates)
      (apply u/min-key-comp (juxt :recency :cost :size) candidates))))

(defn plans
  [or-join-exec-fn db sources rules nodes pairs base-plans prev-plans ratios]
  (apply
    u/merge-with compare-plans
    (mapv
      (fn [[prev-key prev-plan]]
        (let [prev-key-set (set prev-key)]
          (persistent!
            (reduce
              (fn [t [link-e new-e]]
                (if (and (prev-key-set link-e) (not (prev-key-set new-e)))
                  (let [new-key  (conj prev-key new-e)
                        cur-plan (t new-key)
                        new-plan
                        (binary-plan or-join-exec-fn db sources rules nodes
                                     base-plans ratios prev-plan link-e new-e
                                     new-key)]
                    (if new-plan
                      (if (or (nil? cur-plan)
                              (identical? new-plan
                                          (compare-plans cur-plan new-plan)))
                        (assoc! t new-key new-plan)
                        t)
                      t))
                  t))
              (transient {}) pairs))))
      prev-plans)))

(defn connected-pairs
  "Get all connected pairs in a component.
   All links (including or-join) use :tgt pointing to entity nodes."
  [nodes component]
  (let [pairs (volatile! #{})]
    (doseq [e    component
            link (get-in nodes [e :links])]
      (vswap! pairs conj [e (:tgt link)]))
    @pairs))

(defn shrink-space
  [plans]
  (persistent!
    (reduce-kv
      (fn [m k ps]
        (assoc! m k (-> (peek (apply min-key (fn [p] (:cost (peek p))) ps))
                        (update :steps (fn [ss]
                                         (if (= 1 (count ss))
                                           [(update (first ss) :out set)]
                                           [(first ss)
                                            (update (peek ss) :out set)]))))))
      (transient {}) (group-by (fn [p] (set (nth p 0))) plans))))

(defn trace-steps
  [^List tables ^long n-1]
  (let [final-plans (vals (.get tables n-1))]
    (reduce
      (fn [plans i]
        (cons ((.get tables i) (:in (first (:steps (first plans))))) plans))
      [(apply min-key :cost final-plans)]
      (range (dec n-1) -1 -1))))

(defn plan-component
  [or-join-exec-fn db sources rules nodes component]
  (let [n (count component)]
    (if (= n 1)
      [(base-plan db nodes (first component) true)]
      (let [base-plans (build-base-plans db nodes component)]
        (if (some nil? (vals base-plans))
          [nil]
          (let [pairs  (connected-pairs nodes component)
                tables (FastList. n)
                ratios (ConcurrentHashMap.)
                n-1    (dec n)
                pn     ^long (min (long c/plan-search-max)
                                  (long (u/n-permutations n 2)))]
            (.add tables base-plans)
            (dotimes [i n-1]
              (let [plans (plans or-join-exec-fn db sources rules nodes pairs
                                 base-plans (.get tables i) ratios)]
                (if (< pn (count plans))
                  (.add tables (shrink-space plans))
                  (.add tables plans))))
            (trace-steps tables n-1)))))))

(defn dfs
  [graph start]
  (loop [stack [start] visited #{}]
    (if (empty? stack)
      visited
      (let [v     (peek stack)
            stack (pop stack)]
        (if (visited v)
          (recur stack visited)
          (let [links     (:links (graph v))
                neighbors (map :tgt links)]
            (recur (into stack neighbors) (conj visited v))))))))

(defn connected-components
  [graph]
  (loop [vertices (keys graph) components []]
    (if (empty? vertices)
      components
      (let [component (dfs graph (first vertices))]
        (recur (remove component vertices)
               (conj components component))))))

(defn build-plan*
  [or-join-exec-fn db sources rules nodes]
  (let [cc (connected-components nodes)]
    (if (= 1 (count cc))
      [(plan-component or-join-exec-fn db sources rules nodes (first cc))]
      (map+ #(plan-component or-join-exec-fn db sources rules nodes %) cc))))

(defn strip-step-result
  [step]
  (let [step (if (instance? HashJoinStep step)
               (update step :tgt-steps (fn [steps]
                                         (mapv strip-step-result steps)))
               step)]
    (assoc step :result nil :sample nil)))

(defn strip-result
  [plans]
  (mapv (fn [plan-vec]
          (mapv #(update % :steps (fn [steps]
                                    (mapv strip-step-result steps)))
                plan-vec))
        plans))

(defn build-plan
  "Generate a query plan that looks like this:

  [{:op :init :attr :name :val \"Tom\" :out #{?e} :vars [?e]
    :cols [?e]}
   {:op :merge-scan  :attrs [:age :friend] :preds [(< ?a 20) nil]
    :vars [?a ?f] :in #{?e} :index 0 :out #{?e} :cols [?e :age :friend]}
   {:op :link :attr :friend :var ?e1 :in #{?e} :index 2
    :out #{?e ?e1} :cols [?e :age :friend ?e1]}
   {:op :merge-scan :attrs [:name] :preds [nil] :vars [?n] :index 3
    :in #{?e ?e1} :out #{?e ?e1} :cols [?e :age :friend ?e1 :name]}]

  :op here means step type.
  :result-set will be #{} if there is any clause that matches nothing."
  [or-join-exec-fn {:keys [graph sources rules] :as context}]
  (if graph
    (unreduced
      (reduce-kv
        (fn [c src nodes]
          (let [^DB db (sources src)
                k      [(.-store db) nodes]]
            (if-let [cached (.get ^LRUCache qu/*plan-cache* k)]
              (assoc-in c [:plan src] cached)
              (let [nodes (update-nodes db nodes)
                    plans (if (< 1 (count nodes))
                            (build-plan* or-join-exec-fn db sources rules nodes)
                            [[(base-plan db nodes (ffirst nodes) true)]])]
                (if (some #(some nil? %) plans)
                  (reduced (assoc c :result-set #{}))
                  (do (.put ^LRUCache qu/*plan-cache* k (strip-result plans))
                      (assoc-in c [:plan src] plans)))))))
        context graph))
    context))

(defn plan-explain
  []
  (when qu/*explain*
    (let [{:keys [^long parsing-time ^long building-time]} @qu/*explain*]
      (vswap! qu/*explain* assoc :planning-time
              (- ^long (System/nanoTime)
                 (+ ^long qu/*start-time* parsing-time building-time))))))

(defn build-explain
  []
  (when qu/*explain*
    (let [{:keys [^long parsing-time]} @qu/*explain*]
      (vswap! qu/*explain* assoc :building-time
              (- ^long (System/nanoTime)
                 (+ ^long qu/*start-time* parsing-time))))))

;; TODO improve plan cache
(defn planning
  [or-join-exec-fn context]
  (-> context
      build-graph
      ((fn [c] (build-explain) c))
      ((fn [c] (build-plan or-join-exec-fn c)))))
