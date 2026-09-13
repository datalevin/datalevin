;;
;; Copyright (c) Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns ^:no-doc datalevin.query.execute.group-reduction
  "Planning and execution of terminal keyed group reductions."
  (:refer-clojure :exclude [update assoc])
  (:require
   [datalevin.inline
    :refer [update assoc]]
   [datalevin.join :as j]
   [datalevin.parser :as dp]
   [datalevin.query-util :as qu]
   [datalevin.query.aggregate :as qagg]
   [datalevin.query.execute.late
    :refer [late-clause-deps late-or-join-branch-selector late-or-join-clause?
            or-join-vars planned-bound-vars]]
   [datalevin.query.resolve :as qresolve]
   [datalevin.relation :as r]
   [datalevin.util :as u])
  (:import
   [java.util HashSet List]
   [datalevin.parser FindRel Variable]
   [org.eclipse.collections.impl.list.mutable FastList]))

(defn- direct-sum-spec
  [{:keys [qfind qwith]}]
  (let [elements   (dp/find-elements qfind)
        aggregates (filterv dp/aggregate? elements)
        groups     (filterv (complement dp/aggregate?) elements)]
    (when (and (instance? FindRel qfind)
               (= 1 (count aggregates))
               (every? #(instance? Variable %) groups))
      (let [aggregate (first aggregates)
            args      (:args aggregate)
            value     (last args)
            group-vars (mapv :symbol groups)
            with-vars  (mapv :symbol qwith)]
        (when (and (= 'sum (get-in aggregate [:fn :symbol]))
                   (= 1 (count args))
                   (instance? Variable value)
                   (= (count group-vars) (count (distinct group-vars)))
                   (= (count with-vars) (count (distinct with-vars))))
          {:group-vars group-vars
           :with-vars  with-vars
           :value-var  (:symbol value)})))))

(defn- constant-output-domain
  "Return a finite, proven constant domain for `var` in `form`, or nil when
  the form may produce another value. Conjunction intersects known constraints;
  disjunction requires a proof for every branch."
  [form var]
  (cond
    (and (vector? form) (= 2 (count form)))
    (let [[call binding] form]
      (when (and (= var binding)
                 (sequential? call)
                 (= 2 (count call))
                 (= 'ground (first call))
                 (empty? (qu/collect-vars (second call))))
        #{(second call)}))

    (sequential? form)
    (let [op       (first form)
          branches (cond
                     (u/sym-name-eqs op "and") (next form)
                     (u/sym-name-eqs op "or") (next form)
                     (u/sym-name-eqs op "or-join") (nnext form))]
      (cond
        (u/sym-name-eqs op "and")
        (when-let [domains (not-empty
                             (into [] (keep #(constant-output-domain % var))
                                   branches))]
          (reduce u/intersection domains))

        (or (u/sym-name-eqs op "or")
            (u/sym-name-eqs op "or-join"))
        (let [domains (mapv #(constant-output-domain % var) branches)]
          (when (every? some? domains)
            (reduce into #{} domains)))))

    :else nil))

(defn- pairwise-disjoint-domains?
  [domains]
  (loop [seen #{}
         domains domains]
    (if-let [domain (first domains)]
      (when (empty? (u/intersection seen domain))
        (recur (into seen domain) (next domains)))
      true)))

(defn- disjoint-producer-proof
  [branches identity-vars]
  (some
    (fn [var]
      (let [domains (mapv #(constant-output-domain % var) branches)]
        (when (and (every? some? domains)
                   (pairwise-disjoint-domains? domains))
          {:var var :domains domains})))
    identity-vars))

(defn plan-keyed-group-reduction
  "Recognize an exact terminal reduction frontier. The first physical
  implementation handles direct sum over a simple terminal or-join whose
  non-correlation outputs are precisely the aggregate identity."
  [{:keys [parsed-q late-clauses] :as context}]
  (let [candidate (last late-clauses)
        sum-spec  (when qagg/*keyed-group-reduction?*
                    (direct-sum-spec parsed-q))
        vars-form (when (late-or-join-clause? candidate)
                    (or-join-vars candidate))]
    (if (and sum-spec
             (vector? vars-form)
             (u/sym-name-eqs (first candidate) "or-join")
             (not (sequential? (first vars-form)))
             (every? qu/binding-var? vars-form))
      (let [bound-before
            (reduce
              (fn [bound clause]
                (into bound (:provides (late-clause-deps clause))))
              (planned-bound-vars context)
              (butlast late-clauses))
            declared       (vec vars-form)
            declared-set   (set declared)
            outer-key-vars (filterv bound-before declared)
            {:keys [group-vars with-vars value-var]} sum-spec
            payload-vars   (conj (vec with-vars) value-var)
            expected-vars  (into (set outer-key-vars) payload-vars)
            producer-proof (disjoint-producer-proof
                             (nnext candidate) payload-vars)]
        (if (and (seq outer-key-vars)
                 (every? bound-before group-vars)
                 (not (bound-before value-var))
                 (every? #(not (bound-before %)) with-vars)
                 (= declared-set expected-vars))
          (assoc context :keyed-group-reduction
                 (merge sum-spec
                        {:mode               :distinct-sum
                         :clause             candidate
                         :outer-key-vars     outer-key-vars
                         :producer-disjoint? (boolean producer-proof)
                         :proof
                         {:terminal-late-clause true
                          :group-vars-bound-before-frontier true
                          :payload-exclusive-to-fragment true
                          :identity-vars (vec payload-vars)
                          :producer-disjoint producer-proof}}))
          (dissoc context :keyed-group-reduction)))
      (dissoc context :keyed-group-reduction))))

(defn- keyed-group-reduction-profitable?
  [^long input-count ^long outer-count]
  (and (<= (long qagg/*keyed-group-reduction-min-input*) input-count)
       (<= (* (double qagg/*keyed-group-reduction-min-ratio*)
              (double (max 1 outer-count)))
           (double input-count))))

(defn- branch-producer-row-count
  ^long [branch-rels]
  (reduce
    (fn [^long n rel]
      (if-some [tuples (:tuples rel)]
        (unchecked-add n (long (.size ^List tuples)))
        n))
    0 branch-rels))

(defn- constant-ground-binding
  [clause]
  (when (and (vector? clause) (= 2 (count clause)))
    (let [[call binding] clause]
      (when (and (qu/binding-var? binding)
                 (sequential? call)
                 (= 2 (count call))
                 (= 'ground (first call))
                 (empty? (qu/collect-vars (second call))))
        [binding (second call)]))))

(defn- constant-branch-bindings
  [branch]
  (let [clauses (if (and (sequential? branch)
                         (u/sym-name-eqs (first branch) "and"))
                  (vec (next branch))
                  [branch])
        bindings (mapv constant-ground-binding clauses)]
    (when (and (seq clauses) (every? some? bindings))
      (reduce
        (fn [constants [var value]]
          (if (and (contains? constants var)
                   (not= (get constants var) value))
            (reduced nil)
            (assoc constants var value)))
        {}
        bindings))))

(defn- simple-or-join-form
  [clause]
  (when (and (sequential? clause)
             (not (vector? clause))
             (not (qu/source? (first clause)))
             (u/sym-name-eqs (first clause) "or-join")
             (vector? (second clause))
             (not (sequential? (first (second clause)))))
    {:vars     (second clause)
     :branches (vec (nnext clause))}))

(defn- nested-constant-stream-spec
  [branch value-var]
  (when (and (sequential? branch)
             (u/sym-name-eqs (first branch) "and"))
    (let [clauses (vec (next branch))
          prefix  (pop clauses)
          nested  (peek clauses)]
      (when-let [{:keys [vars branches]} (and (seq prefix)
                                             (simple-or-join-form nested))]
        (let [classified
              (mapv (fn [inner]
                      {:branch    inner
                       :constants (constant-branch-bindings inner)})
                    branches)
              constants (filterv #(contains? (:constants %) value-var)
                                 classified)
              residual  (filterv #(not (contains? (:constants %) value-var))
                                 classified)]
          (when (seq constants)
            {:prefix            prefix
             :inner-vars        vars
             :constant-branches constants
             :residual-branches residual}))))))

(defn- terminal-virtual-stream-spec
  [branches value-var]
  (let [candidates
        (into []
              (keep-indexed
                (fn [idx branch]
                  (when-let [spec (nested-constant-stream-spec branch value-var)]
                    (assoc spec :idx idx))))
              branches)]
    (when (= 1 (count candidates))
      (first candidates))))

(defn- producer-compatible?
  [producer required-vars]
  (let [attrs     (get-in producer [:rel :attrs])
        constants (:constants producer)]
    (and
      ;; A ground binding for an already-bound variable is a unification
      ;; constraint, not a replacement column. Leave those branches on the
      ;; ordinary resolver path instead of silently overriding the value.
      (not-any? #(contains? attrs %) (keys constants))
      (every? #(or (contains? attrs %) (contains? constants %))
              required-vars))))

(defn- relation-producer
  [rel domain source]
  {:rel       rel
   :constants {}
   :distinct? true
   :domain    domain
   :source    source})

(defn- virtual-producer
  [rel constants domain source]
  {:rel       rel
   :constants constants
   :distinct? false
   :domain    domain
   :source    source})

(defn- joined-projected-relation
  [context rel vars]
  (-> (reduce j/hash-join
              (qresolve/collapse-rels (:rels context) rel))
      (r/project-distinct vars)))

(defn- terminal-virtual-producers
  [context vars branches plan]
  (when-let [{:keys [idx prefix inner-vars constant-branches
                     residual-branches]}
             (and qagg/*keyed-group-virtual-producers?*
                  (terminal-virtual-stream-spec branches (:value-var plan)))]
    (let [vars             (vec vars)
          var-set          (set vars)
          _                (qresolve/check-free-subset
                             (qresolve/bound-vars context) var-set branches)
          join-context     (qresolve/limit-context context var-set)
          prefix-context   (qresolve/resolve-branch-clauses join-context prefix)
          prefix-rel       (reduce j/hash-join (:rels prefix-context))
          required-vars    (into (vec (:outer-key-vars plan))
                                 (conj (vec (:with-vars plan))
                                       (:value-var plan)))
          prefix-producers
          (mapv
            (fn [{:keys [branch constants]}]
              (virtual-producer
                prefix-rel constants
                (constant-output-domain branch (:value-var plan))
                :nested-constant))
            constant-branches)
          residual-forms   (mapv :branch residual-branches)
          residual-rels
          (when (seq residual-forms)
            (qresolve/resolve-or-join-branch-relations
              prefix-context inner-vars residual-forms))
          residual-producers
          (mapv (fn [rel {:keys [branch]}]
                  (relation-producer
                    (joined-projected-relation prefix-context rel vars)
                    (constant-output-domain branch (:value-var plan))
                    :nested-residual))
                residual-rels residual-branches)
          outer-rel        (reduce j/hash-join (:rels context))
          other-producers
          (into []
                (mapcat
                  (fn [[branch-idx branch]]
                    (when (not= branch-idx idx)
                      (if-let [constants (constant-branch-bindings branch)]
                        [(virtual-producer
                           outer-rel constants
                           (constant-output-domain branch (:value-var plan))
                           :outer-constant)]
                        (mapv #(relation-producer
                                 %
                                 (constant-output-domain
                                   branch (:value-var plan))
                                 :outer-branch)
                              (qresolve/resolve-or-join-branch-relations
                                context vars [branch]))))))
                (map-indexed vector branches))
          producers        (into [] cat [other-producers prefix-producers
                                         residual-producers])]
      (when (and (seq producers)
                 (every? #(producer-compatible? % required-vars) producers))
        (let [domains (mapv :domain producers)]
          {:outer-rel outer-rel
           :producers producers
           :producer-disjoint?
           (boolean
             (and (every? some? domains)
                  (pairwise-disjoint-domains? domains)))
           :domains domains
           :virtual-producer-count
           (count (filter #(seq (:constants %)) producers))})))))

(defn- materialize-virtual-producer
  [producer vars]
  (let [{:keys [rel constants distinct?]} producer]
    (if (and distinct? (empty? constants))
      rel
      (let [attrs        (:attrs rel)
            ^List tuples (:tuples rel)
            n            (.size tuples)
            output       (FastList. n)
            seen         (HashSet. n)
            plan         (mapv (fn [var]
                                 (if (contains? constants var)
                                   [-1 true (get constants var)]
                                   [(long (attrs var)) false nil]))
                               vars)]
        (dotimes [i n]
          (let [^objects tuple (.get tuples i)
                projected (object-array (count vars))]
            (dotimes [j (count plan)]
              (let [[idx constant? constant] (nth plan j)]
                (aset projected j
                      (if constant? constant (aget tuple (int idx))))))
            (when (.add seen (r/wrap-array projected))
              (.add output projected))))
        (r/relation! (zipmap vars (range)) output)))))

(defn- virtual-producer-fallback
  [context clause plan producers producer-row-count outer-count reason]
  (let [vars      (vec (second clause))
        branch-rels (mapv #(materialize-virtual-producer % vars) producers)
        union-rel (qresolve/union-or-join-branch-relations branch-rels)]
    (-> context
        (update :rels qresolve/collapse-rels union-rel)
        (assoc :keyed-group-reduction
               (merge plan
                      {:executed?              false
                       :direct-feed?           false
                       :virtual-producer-feed? false
                       :union-materialized?    true
                       :producer-row-count     producer-row-count
                       :input-count            (.size ^List (:tuples union-rel))
                       :outer-count            outer-count
                       :reason                 reason})))))

(defn- resolve-terminal-virtual-group-reduction
  [context clause]
  (let [[_ vars & branches] clause
        plan (:keyed-group-reduction context)]
    (when-let [{:keys [outer-rel producers producer-disjoint? domains
                       virtual-producer-count]}
               (terminal-virtual-producers context vars branches plan)]
      (let [outer-count        (.size ^List (:tuples outer-rel))
            producer-row-count (branch-producer-row-count
                                 (mapv :rel producers))
            stream-proof       (cond->
                                 (-> (:proof plan)
                                     (dissoc :producer-disjoint)
                                     (assoc :stream-producer-domains domains))
                                 producer-disjoint?
                                 (assoc :producer-disjoint
                                        {:var     (:value-var plan)
                                         :domains domains}))
            stream-plan        (assoc plan
                                      :producer-disjoint?
                                      producer-disjoint?
                                      :proof stream-proof)]
        (if (keyed-group-reduction-profitable? producer-row-count outer-count)
          (let [{:keys [relation] :as reduction}
                (qagg/keyed-sum-producers outer-rel producers stream-plan)
                distinct-count (:identity-count reduction)]
            (if (keyed-group-reduction-profitable? distinct-count outer-count)
              (assoc context
                     :rels [relation]
                     :keyed-group-reduction
                     (merge stream-plan
                            (dissoc reduction :relation)
                            {:executed?              true
                             :direct-feed?           true
                             :virtual-producer-feed? true
                             :union-materialized?    false
                             :producer-row-count     producer-row-count
                             :virtual-producer-count virtual-producer-count}))
              (virtual-producer-fallback
                context clause stream-plan producers producer-row-count
                outer-count :below-distinct-runtime-cost-threshold)))
          (virtual-producer-fallback
            context clause stream-plan producers producer-row-count outer-count
            :below-runtime-cost-threshold))))))

(defn- resolve-terminal-materialized-group-reduction
  [context clause]
  (let [[_ vars & branches] clause
        branch-rels        (qresolve/resolve-or-join-branch-relations
                             context vars branches)
        outer-rel          (reduce j/hash-join (:rels context))
        outer-count        (.size ^List (:tuples outer-rel))
        producer-row-count (branch-producer-row-count branch-rels)
        plan                (:keyed-group-reduction context)
        pre-profitable?
        (keyed-group-reduction-profitable? producer-row-count outer-count)]
    (if pre-profitable?
      (let [{:keys [relation] :as reduction}
            (qagg/keyed-sum-relations outer-rel branch-rels plan)
            distinct-count (:identity-count reduction)]
        (if (keyed-group-reduction-profitable? distinct-count outer-count)
          (assoc context
                 :rels [relation]
                 :keyed-group-reduction
                 (merge plan
                        (dissoc reduction :relation)
                        {:executed?          true
                         :direct-feed?       true
                         :union-materialized? false
                         :producer-row-count producer-row-count}))
          (let [union-rel (qresolve/union-or-join-branch-relations branch-rels)]
            (-> context
                (update :rels qresolve/collapse-rels union-rel)
                (assoc :keyed-group-reduction
                       (merge plan
                              {:executed?          false
                               :direct-feed?       false
                               :union-materialized? true
                               :producer-row-count producer-row-count
                               :input-count
                               (.size ^List (:tuples union-rel))
                               :outer-count        outer-count
                               :reason
                               :below-distinct-runtime-cost-threshold}))))))
      (let [union-rel (qresolve/union-or-join-branch-relations branch-rels)]
        (-> context
            (update :rels qresolve/collapse-rels union-rel)
            (assoc :keyed-group-reduction
                   (merge plan
                          {:executed?          false
                           :direct-feed?       false
                           :union-materialized? true
                           :producer-row-count producer-row-count
                           :input-count        (.size ^List (:tuples union-rel))
                           :outer-count        outer-count
                           :reason             :below-runtime-cost-threshold})))))))

(defn resolve-terminal-keyed-group-reduction
  [context clause]
  (binding [qresolve/*or-join-branch-selector*
            (late-or-join-branch-selector clause)]
    (or (resolve-terminal-virtual-group-reduction context clause)
        (resolve-terminal-materialized-group-reduction context clause))))
