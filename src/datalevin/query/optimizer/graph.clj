;;
;; Copyright (c) Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns ^:no-doc datalevin.query.optimizer.graph
  "Query graph building helpers."
  (:require
   [clojure.set :as set]
   [datalevin.db :as db]
   [datalevin.query.optimizer.range :as qor]
   [datalevin.query-util :as qu]
   [datalevin.util :as u :refer [raise]])
  (:import
   [datalevin.parser And Constant DefaultSrc Function Not Or Pattern Predicate
    RuleExpr Variable]))

(defn- dep
  [deps k]
  (or (get deps k)
      (raise "Missing query optimizer dependency " k {:dependency k})))

(defn- resolve-pattern-lookup-refs
  [deps source pattern]
  ((dep deps :resolve-pattern-lookup-refs) source pattern))

(defn- make-call
  [deps f]
  ((dep deps :make-call) f))

(defn- resolve-pred
  [deps f context]
  ((dep deps :resolve-pred) f context))

(defn- map->Clause
  [deps m]
  ((dep deps :map->Clause) m))

(defn- map->Node
  [deps m]
  ((dep deps :map->Node) m))

(defn- mk-link
  [deps type tgt var attrs attr]
  ((dep deps :link) type tgt var attrs attr))

(defn- mk-or-join-link
  [deps type tgt clause bound-var free-vars tgt-attr source]
  ((dep deps :or-join-link)
   type tgt clause bound-var free-vars tgt-attr source))

(defn- not-join-clause?
  [clause]
  (and (sequential? clause)
       (not (vector? clause))
       (= 'not-join
          (if (qu/source? (first clause))
            (second clause)
            (first clause)))))

(defn- get-not-join-source
  [clause]
  (if (qu/source? (first clause)) (first clause) '$))

(defn- clause-source-symbol
  [source]
  (if (instance? DefaultSrc source) '$ (:symbol source)))

(defn- not-join-optimizable?
  "Conservative check for planner-handled not-join."
  [sources parsed-clause orig-clause]
  (when (and (instance? Not parsed-clause) (not-join-clause? orig-clause))
    (let [vars             (into []
                                 (comp (map :symbol) (filter qu/binding-var?))
                                 (:vars parsed-clause))
          clauses          (:clauses parsed-clause)
          src              (get-not-join-source orig-clause)
          pattern-only?    (every? #(instance? Pattern %) clauses)
          clause-sources   (into #{} (map #(clause-source-symbol (:source %)))
                                 clauses)
          body-vars        (qu/collect-vars clauses)
          all-vars-used?   (set/subset? (set vars) body-vars)
          searchable-src?  (when-let [db (get sources src)]
                             (db/-searchable? db))]
      (when (and searchable-src?
                 (seq vars)
                 pattern-only?
                 (= 1 (count clause-sources))
                 (= src (first clause-sources))
                 all-vars-used?)
        orig-clause))))

(defn- optimizable?
  [sources resolved clause]
  (when (instance? Pattern clause)
    (let [{:keys [pattern]} clause]
      (when (and (instance? Constant (second pattern))
                 (not-any? resolved (map :symbol pattern)))
        (if-let [s (get-in clause [:source :symbol])]
          (when-let [src (get sources s)] (db/-searchable? src))
          (when-let [src (get sources '$)] (db/-searchable? src)))))))

(defn- parsed-rule-expr?
  [clause]
  (instance? RuleExpr clause))

(defn- rule-clause?
  [rules clause]
  (or (parsed-rule-expr? clause)
      (when (and (sequential? clause) (not (vector? clause)))
        (let [head (qu/clause-head clause)]
          (and (symbol? head)
               (not (qu/free-var? head))
               (not (qu/rule-head head))
               (contains? rules head))))))

(defn- get-rule-args
  [clause]
  (if (parsed-rule-expr? clause)
    (mapv #(when (instance? Variable %) (:symbol %)) (:args clause))
    (qu/clause-args clause)))

(defn- or-join-clause?
  [clause]
  (or (instance? Or clause)
      (and (sequential? clause)
           (not (vector? clause))
           (= 'or-join (first clause)))))

(defn- get-or-join-vars
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

(defn- get-or-join-branches
  [clause]
  (cond
    (instance? Or clause)
    (:clauses clause)

    (or-join-clause? clause)
    (let [[_ vars & branches] clause]
      (if (and (sequential? vars) (vector? (first vars)))
        branches
        branches))))

(defn- infer-branch-source
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

(defn- single-source-or-join?
  [sources clause]
  (let [branches       (get-or-join-branches clause)
        branch-sources (keep infer-branch-source branches)]
    (and (seq branch-sources)
         (= (count branch-sources) (count branches))
         (apply = branch-sources)
         (contains? sources (first branch-sources)))))

(defn- or-join-branch-valid?
  [branch]
  (cond
    (instance? Pattern branch)
    true

    (instance? And branch)
    (every? #(or (instance? Pattern %)
                 (instance? Predicate %)
                 (instance? Function %))
            (:clauses branch))

    (vector? branch)
    true

    (and (sequential? branch) (= 'and (first branch)))
    true

    :else false))

(defn- or-join-optimizable?
  [sources resolved clause pattern-entity-vars rule-derived-vars]
  (when (or-join-clause? clause)
    (let [vars          (get-or-join-vars clause)
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

(defn- find-rule-derived-vars
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

(defn- depends-on-rule-output?
  [clause rule-derived-vars]
  (when (and rule-derived-vars (instance? Pattern clause))
    (let [e-var (get-in clause [:pattern 0 :symbol])]
      (and (qu/binding-var? e-var)
           (contains? rule-derived-vars e-var)))))

(defn- split-clauses
  [{:keys [sources parsed-q rels rules] :as context}]
  (let [resolved    (reduce (fn [rs {:keys [attrs]}]
                              (set/union rs (set (keys attrs))))
                            #{} rels)
        input-bound (reduce (fn [s {:keys [attrs]}]
                              (into s (keys attrs)))
                            #{} rels)
        qwhere      (vec (:qwhere parsed-q))
        clauses     (:qorig-where parsed-q)
        pattern-entity-vars
        (into #{}
              (comp
               (filter #(instance? Pattern %))
               (map #(get-in % [:pattern 0 :symbol]))
               (filter qu/binding-var?))
              qwhere)
        opt-or-joins
        (filterv #(or-join-optimizable? sources resolved % pattern-entity-vars
                                        nil)
                 qwhere)
        not-join-idxs
        (set
         (keep-indexed
          (fn [i clause]
            (let [orig (nth clauses i)]
              (when (not-join-optimizable? sources clause orig) i)))
          qwhere))
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
           :optimizable-not-joins (u/keep-idxs not-join-idxs clauses)
           :late-clauses (u/remove-idxs (set/union ptn-idxs
                                                   or-join-idxs
                                                   not-join-idxs)
                                        clauses))))

(defn- make-node
  [deps [e patterns]]
  [e (reduce (fn [m pattern]
               (let [attr   (second pattern)
                     clause (map->Clause deps {:attr attr})]
                 (if-some [v (qu/get-v pattern)]
                   (if (qu/free-var? v)
                     (update m :free u/conjv (assoc clause :var v))
                     (update m :bound u/conjv (assoc clause :val v)))
                   (update m :free u/conjv clause))))
             (map->Node deps {}) patterns)])

(defn- link-refs
  [deps graph]
  (let [es (set (keys graph))]
    (reduce-kv
     (fn [g e {:keys [free]}]
       (reduce
        (fn [g {:keys [attr var]}]
          (if (es var)
            (-> g
                (update-in [e :links] u/conjv
                           (mk-link deps :ref var nil nil attr))
                (update-in [var :links] u/conjv
                           (mk-link deps :_ref e nil nil attr)))
            g))
        g free))
     graph graph)))

(defn- link-eqs
  [deps graph]
  (reduce-kv
   (fn [g v lst]
     (if (< 1 (count lst))
       (reduce
        (fn [g [[e1 k1] [e2 k2]]]
          (let [attrs {e1 k1 e2 k2}]
            (-> g
                (update-in [e1 :links] u/conjv
                           (mk-link deps :val-eq e2 v attrs nil))
                (update-in [e2 :links] u/conjv
                           (mk-link deps :val-eq e1 v attrs nil)))))
        g (u/combinations lst 2))
       g))
   graph (reduce-kv (fn [m e {:keys [free]}]
                      (reduce (fn [m {:keys [attr var]}]
                                (if var (update m var u/conjv [e attr]) m))
                              m free))
                    {} graph)))

(defn- make-nodes
  [deps [src patterns]]
  [src (let [patterns' (mapv qu/replace-blanks patterns)
             graph     (into {} (map #(make-node deps %))
                             (group-by first patterns'))]
         (if (< 1 (count graph))
           (->> graph
                (link-refs deps)
                (link-eqs deps))
           graph))])

(defn- extract-or-join-info
  [will-be-bound clause]
  (let [vars       (get-or-join-vars clause)
        bound-vars (filterv will-be-bound vars)
        free-vars  (filterv (complement will-be-bound) vars)]
    {:bound-var (first bound-vars)
     :free-vars free-vars
     :clause    clause}))

(defn- find-nodes-using-vars
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

(defn connected-pairs
  "Get all connected pairs in a component.
   All links (including or-join) use :tgt pointing to entity nodes."
  [nodes component]
  (let [pairs (volatile! #{})]
    (doseq [e    component
            link (get-in nodes [e :links])]
      (vswap! pairs conj [e (:tgt link)]))
    @pairs))

(defn- dfs
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

(defn- link-or-joins
  [deps graph will-be-bound or-join-clauses source]
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
               (update-in g [bound-var :links] u/conjv
                          (mk-or-join-link deps :or-join tgt-entity clause
                                           bound-var free-vars tgt-attr
                                           source))))
           graph target-entities)
          :unlinked unlinked}
         {:graph    graph
          :unlinked (conj unlinked clause)})))
   {:graph graph :unlinked []} or-join-clauses))

(defn- resolve-lookup-refs
  [deps sources [src patterns]]
  [src (mapv #(resolve-pattern-lookup-refs deps (sources src) %) patterns)])

(defn- remove-src
  [[src patterns]]
  [src (mapv #(if (= (first %) src) (vec (rest %)) %) patterns)])

(defn- get-src
  [[f & _]]
  (if (qu/source? f) f '$))

(defn- get-or-join-source
  [clause]
  (or (infer-branch-source (first (get-or-join-branches clause))) '$))

(defn- init-graph
  [deps context]
  (let [opt-clauses          (:opt-clauses context)
        optimizable-or-joins (:optimizable-or-joins context)
        sources              (:sources context)
        rels                 (:rels context)
        input-bound          (reduce (fn [s {:keys [attrs]}]
                                       (into s (keys attrs)))
                                     #{} rels)
        pattern-entity-vars  (into #{}
                                   (comp
                                    (filter vector?)
                                    (map first)
                                    (filter qu/binding-var?))
                                   opt-clauses)
        will-be-bound        (into input-bound pattern-entity-vars)
        base-graphs          (into {}
                                   (comp
                                    (map remove-src)
                                    (map #(resolve-lookup-refs deps sources %))
                                    (map #(make-nodes deps %)))
                                   (group-by get-src opt-clauses))
        {:keys [graph unlinked]}
        (reduce
         (fn [{:keys [graph unlinked]} clause]
           (let [src (get-or-join-source clause)]
             (if-let [nodes (get graph src)]
               (let [result (link-or-joins deps nodes will-be-bound [clause] src)]
                 {:graph    (assoc graph src (:graph result))
                  :unlinked (into unlinked (:unlinked result))})
               {:graph graph :unlinked (conj unlinked clause)})))
         {:graph base-graphs :unlinked []}
         optimizable-or-joins)]
    (-> context
        (assoc :graph graph)
        (update :late-clauses into unlinked)
        (assoc :optimizable-or-joins []))))

(defn build-graph
  [deps context]
  (let [helpers {:make-call    (fn [f] (make-call deps f))
                 :resolve-pred (fn [f ctx] (resolve-pred deps f ctx))}]
    (-> context
        split-clauses
        ((fn [ctx] (init-graph deps ctx)))
        ((fn [ctx] (qor/pushdown-predicates ctx helpers))))))
