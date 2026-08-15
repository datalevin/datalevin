;;
;; Copyright (c) Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns ^:no-doc datalevin.query.execute
  "Query execution."
  (:refer-clojure :exclude [update assoc])
  (:require
   [clojure.walk :as w]
   [datalevin.built-ins :as built-ins]
   [datalevin.constants :as c]
   [datalevin.datom :as dd]
   [datalevin.db :as db]
   [datalevin.index :as idx]
   [datalevin.inline :refer [update assoc]]
   [datalevin.join :as j]
   [datalevin.parser :as dp]
   [datalevin.pull-api :as dpa]
   [datalevin.query.aggregate :as qagg]
   [datalevin.query.access :as qaccess]
   [datalevin.query.access.ave :as qave]
   [datalevin.query.access.function :as qfunction]
   [datalevin.query.access.fulltext :as qfulltext]
   [datalevin.query.access.idoc :as qidoc]
   [datalevin.query.access.vector :as qvector]
   [datalevin.query.optimizer.range :as qor]
   [datalevin.query-optimizer :as qo]
   [datalevin.query.plan :as qplan]
   [datalevin.query.resolve :as qresolve]
   [datalevin.query-util :as qu]
   [datalevin.relation :as r]
   [datalevin.rules :as rules]
   [datalevin.spill :as sp]
   [datalevin.timeout :as timeout]
   [datalevin.util :as u :refer [cond+ concatv map+]])
  (:import
   [java.util Comparator PriorityQueue]
   [datalevin.parser Constant FindColl FindRel FindScalar FindTuple Pattern
    Variable]
   [org.eclipse.collections.impl.list.mutable FastList]))

(def ^:private plugin-inputs qo/plugin-inputs)

(def ^:private function-access-method
  (qfunction/access-method {:fulltext qfulltext/access-method
                            :idoc     qidoc/access-method
                            :vector   qvector/access-method}))

(def ^:dynamic *access-methods*
  "Physical access methods considered during query planning."
  [qave/access-method function-access-method])

(def ^:private materialize-input-bound-patterns
  qo/materialize-input-bound-patterns)

(def ^:private materialize-selective-value-lookups
  qo/materialize-selective-value-lookups)

(def ^:private push-down-equality-disjunctions
  qo/push-down-equality-disjunctions)

(def ^:private rewrite-unused-vars qo/rewrite-unused-vars)

(declare sort-planned-late-clauses access-batch-query access-outer-query)

(defn- adaptive-limit-query?
  [parsed-q]
  (let [find          (:qfind parsed-q)
        find-elements (dp/find-elements find)]
    (and (instance? FindRel find)
         (nil? (:qwith parsed-q))
         (empty? (:qhaving parsed-q))
         (nil? (:qreturn-map parsed-q))
         (not-any? #(or (dp/aggregate? %) (dp/find-expr? %)
                        (dp/pull? %))
                   find-elements))))

(defn- root-access-demand
  [parsed-q]
  (let [ordering     (:qorder parsed-q)
        query-limit  (:qlimit parsed-q)
        finite-limit (when (and (some? query-limit)
                                (not= -1 query-limit))
                       query-limit)
        required-vars (set (dp/find-vars (:qfind parsed-q)))]
    (cond
      (and (seq ordering)
           (some? finite-limit)
           (pos? (long finite-limit)))
      (assoc
        (qaccess/top-k-demand ordering (:qoffset parsed-q) finite-limit)
        :required-vars required-vars)

      (and (empty? ordering)
           (some? finite-limit)
           (pos? (long finite-limit))
           (adaptive-limit-query? parsed-q))
      (qaccess/limit-demand
        (:qoffset parsed-q) finite-limit :exact required-vars)

      :else
      (qaccess/complete-demand
        ordering (:qoffset parsed-q) finite-limit :exact required-vars))))

(defn- discover-access-plans
  [parsed-q inputs]
  (let [root-demand  (root-access-demand parsed-q)
        input-values (delay (qaccess/scalar-input-values parsed-q inputs))
        planning-context
        {:parsed-q    parsed-q
         :inputs      inputs
         :input-values input-values
         :demand      root-demand}
        plans (mapv (fn [{:keys [expr path bounds work] :as plan}]
                      (let [bounds (or bounds (qaccess/source-bounds))
                            access-source
                            (or (:access-source plan)
                                (qaccess/resolve-source
                                  @input-values (:source expr))
                                (get-in path [:options :db]))]
                        (cond->
                          (assoc plan
                                 :demand root-demand
                                 :bounds bounds
                                 :access-source access-source
                                 :query parsed-q
                                 :inputs inputs)
                        (empty? (:requires expr))
                        (assoc :step
                               (qplan/access-step
                                 expr path root-demand bounds work []
                                 access-source)))))
                    (qaccess/access-plans
                      *access-methods* planning-context))]
    plans))

(defn- prepare-access-plans
  [plans]
  (if (or (empty? plans) (:prepared? (first plans)))
    plans
    (let [{:keys [query inputs]} (first plans)]
      (mapv
        (fn [plan]
          (if (:unavailable? plan)
            (assoc plan :prepared? true)
            (cond->
                (assoc plan :prepared? true)
              (:correlated? plan)
              (assoc :outer-query (access-outer-query query plan)))))
        (qo/plan-access-joins query inputs plans)))))

(defn- attach-access-plans
  [context plans]
  (assoc context
         :access-plans plans
         :access-demand (some-> plans first :demand)
         :preferred-access-plan (qaccess/best-plan plans)))

(defn- build-explain
  []
  (when qplan/*explain*
    (let [{:keys [^long parsing-time]} @qplan/*explain*]
      (vswap! qplan/*explain* assoc :building-time
              (- ^long (System/nanoTime)
                 (+ ^long qplan/*start-time* parsing-time))))))

(defn- planning
  [context]
  (-> (if (:datalevin.query-optimizer/selective-preplanned? context)
        (dissoc context
                :datalevin.query-optimizer/selective-preplanned?)
        (-> context
            qo/build-graph
            ((fn [c] (build-explain) c))
            qo/build-plan
            qo/plan-not-joins))
      sort-planned-late-clauses
      ((fn [context]
         (if (seq (:access-plans context))
           (let [plans (prepare-access-plans (:access-plans context))]
             (-> context
                 (attach-access-plans plans)
                 (qo/build-property-memo)))
           context)))))

(defn execute-plan
  [{:keys [plan sources] :as context}]
  (if (= 1 (transduce (map (fn [[_ components]] (count components))) + plan))
    (update context :rels qresolve/collapse-rels
            (let [[src components] (first plan)
                  all-steps        (vec (mapcat :steps (first components)))]
              (qplan/execute-steps context (sources src) all-steps)))
    (reduce
      (fn [c r] (update c :rels qresolve/collapse-rels r))
      context (->> plan
                   (mapcat (fn [[src components]]
                             (let [db (sources src)]
                               (for [plans components]
                                 [db (mapcat :steps plans)]))))
                   (map+ #(apply qplan/execute-steps context %))
                   (sort-by #(count (:tuples %)))))))

(defn- strip-clause-source
  [clause]
  (if (and (sequential? clause) (qu/source? (first clause)))
    (next clause)
    clause))

(defn- clause-vars
  [form]
  (into #{} (filter qu/binding-var?) (qu/collect-vars form)))

(defn- call-vars
  [[f & args]]
  (cond-> (qu/collect-fn-arg-vars args)
    (qu/binding-var? f) (conj f)))

(defn- late-clause-deps
  [clause]
  (let [clause (strip-clause-source clause)
        head   (when (sequential? clause) (first clause))]
    (cond
      (and (sequential? clause) (sequential? head))
      {:requires (call-vars head)
       :provides (clause-vars (second clause))}

      (= 'not head)
      {:requires-any (clause-vars (next clause))}

      (= 'not-join head)
      {:requires (clause-vars (second clause))}

      (= 'or-join head)
      (let [vars-form (second clause)
            req-form  (when (and (sequential? vars-form)
                                 (sequential? (first vars-form)))
                        (first vars-form))]
        {:requires (clause-vars req-form)
         :provides (clause-vars vars-form)})

      :else
      {:provides (clause-vars clause)})))

(defn- late-clause-ready?
  [bound {:keys [requires requires-any]}]
  (and (every? bound requires)
       (or (empty? requires-any)
           (some bound requires-any))))

(defn- sort-late-clauses
  [initial-bound clauses]
  (let [entries (mapv #(assoc (late-clause-deps %) :clause %) clauses)]
    (loop [bound initial-bound
           todo  entries
           acc   []]
      (if (empty? todo)
        (mapv :clause acc)
        (if-let [idx (first (keep-indexed
                              (fn [i entry]
                                (when (late-clause-ready? bound entry) i))
                              todo))]
          (let [entry (nth todo idx)]
            (recur (into bound (:provides entry))
                   (u/vec-remove todo idx)
                   (conj acc entry)))
          (mapv :clause (into acc todo)))))))

(defn- planned-step-vars
  [step]
  (clause-vars (:cols step)))

(defn- planned-bound-vars
  [{:keys [plan] :as context}]
  (reduce
    into
    (qresolve/bound-vars context)
    (for [[_src components] plan
          plans components
          :let [step (some-> plans last :steps last)]
          :when step]
      (planned-step-vars step))))

(defn- sort-planned-late-clauses
  [{:keys [late-clauses] :as context}]
  (cond-> context
    (seq late-clauses)
    (assoc :late-clauses
           (sort-late-clauses (planned-bound-vars context) late-clauses))))

(defn- context-bound-values
  [context sym]
  (when-let [{:keys [attrs tuples]}
             (some #(when (contains? (:attrs %) sym) %) (:rels context))]
    (let [idx (long (attrs sym))]
      (into #{}
            (map (fn [^objects tuple] (aget tuple idx)))
            tuples))))

(defn- data-pattern-parts
  [clause default-source]
  (when (vector? clause)
    (let [source? (qu/source? (first clause))
          pattern (if source? (subvec clause 1) clause)]
      (when (and (= 3 (count pattern))
                 (qu/binding-var? (first pattern))
                 (keyword? (second pattern))
                 (qu/binding-var? (nth pattern 2)))
        {:source (if source? (first clause) default-source)
         :entity (first pattern)
         :attr   (second pattern)
         :value  (nth pattern 2)}))))

(defn- constant-ground-binding?
  [clause]
  (when (and (vector? clause) (= 2 (count clause)))
    (let [[call binding] clause]
      (and (sequential? call)
           (u/sym-name-eqs (first call) "ground")
           (empty? (qu/collect-vars call))
           (qu/binding-var? binding)))))

(defn- branch-clauses
  [branch]
  (if (and (sequential? branch)
           (u/sym-name-eqs (first branch) "and"))
    (vec (next branch))
    [branch]))

(defn- branch-indexed-producer
  [branch default-source]
  (let [clauses   (branch-clauses branch)
        producers (keep-indexed
                    (fn [idx clause]
                      (when-let [producer
                                 (data-pattern-parts clause default-source)]
                        (assoc producer :idx idx)))
                    clauses)]
    (when (= 1 (count producers))
      (let [{:keys [idx] :as producer} (first producers)]
        (when (every? constant-ground-binding?
                      (u/remove-idxs #{idx} clauses))
          (dissoc producer :idx))))))

(defn- indexed-or-union
  [context clause entity]
  (when (and (sequential? clause) (not (vector? clause)))
    (let [source? (qu/source? (first clause))
          body    (if source? (next clause) clause)
          source  (if source? (first clause) '$)]
      (when (and (u/sym-name-eqs (first body) "or-join")
                 (vector? (second body))
                 (not (vector? (first (second body))))
                 (<= 2 (count (nnext body))))
        (let [vars      (set (second body))
              bound     (qresolve/bound-vars context)
              producers (mapv #(branch-indexed-producer % source)
                              (nnext body))]
          (when (and (every? some? producers)
                     (contains? vars entity)
                     (every? #(= entity (:entity %)) producers)
                     (every? #(contains? vars (:value %)) producers)
                     (apply = (map (juxt :source :attr) producers))
                     (every? #(= 1 (count (context-bound-values
                                           context (:value %))))
                             producers)
                     (every? #(= 1 (count (context-bound-values context %)))
                             (filter bound vars)))
            {:clause    clause
             :entity    entity
             :source    (:source (first producers))
             :attr      (:attr (first producers))
             :producers producers}))))))

(defn- saturating-add
  ^long [^long x ^long y]
  (if (> y (- Long/MAX_VALUE x))
    Long/MAX_VALUE
    (+ x y)))

(defn- indexed-fanout
  ^long [source attr values]
  (reduce
    (fn [^long total value]
      (let [pattern (qresolve/resolve-pattern-lookup-refs
                      source [nil attr value])]
        (saturating-add total (long (db/-count source pattern)))))
    0
    values))

(defn- indexed-producer-cost
  ^double [^long probe-count ^long output-count]
  (+ (* (double probe-count) (double c/magic-cost-link-probe))
     (* (double output-count) (double c/magic-cost-link-retrieval))
     (* (+ (double probe-count) (double output-count))
        (double c/magic-cost-hash-join))))

(defn- or-join-vars
  [clause]
  (let [body (if (qu/source? (first clause)) (next clause) clause)]
    (second body)))

(defn- isolated-union-switch-cost
  ^double [context clause ^long fanout ^double producer-cost]
  (let [vars      (or-join-vars clause)
        bound     (qresolve/bound-vars context)
        new-width (count (remove bound vars))
        projection-cost
        (* (double fanout)
           (+ (double c/magic-cost-hash-join-output-tuple)
              (* (double new-width)
                 (double c/magic-cost-hash-join-output-cell))))
        follow-up-cost (indexed-producer-cost fanout fanout)]
    (+ producer-cost projection-cost follow-up-cost)))

(def ^:private no-range-bound (Object.))

(defn- scalar-range-bound
  [context term]
  (cond
    (qu/binding-var? term)
    (let [values (context-bound-values context term)]
      (if (= 1 (count values)) (first values) no-range-bound))

    (and (some? term)
         (not (coll? term))
         (empty? (qu/collect-vars term)))
    term

    :else
    no-range-bound))

(defn- range-boundary
  [context value clause]
  (when (and (vector? clause) (= 1 (count clause)))
    (let [pred (first clause)]
      (when (and (sequential? pred) (= 3 (count pred)))
        (let [[f left right] pred
              value-left?      (= value left)
              value-right?     (= value right)]
          (when (not= value-left? value-right?)
            (let [bound (scalar-range-bound
                          context (if value-left? right left))]
              (when-not (identical? bound no-range-bound)
                (cond
                  (u/sym-name-eqs f "<")
                  {:side     (if value-left? :upper :lower)
                   :endpoint [:open bound]}

                  (u/sym-name-eqs f "<=")
                  {:side     (if value-left? :upper :lower)
                   :endpoint [:closed bound]}

                  (u/sym-name-eqs f ">")
                  {:side     (if value-left? :lower :upper)
                   :endpoint [:open bound]}

                  (u/sym-name-eqs f ">=")
                  {:side     (if value-left? :lower :upper)
                   :endpoint [:closed bound]})))))))))

(defn- query-result-var?
  [context sym]
  (let [{:keys [qfind qwith qhaving]} (:parsed-q context)]
    (or (some #{sym} (when qfind (dp/find-vars qfind)))
        (some #(= sym (:symbol %))
              (concat qwith (dp/collect-vars-distinct qhaving))))))

(defn- indexed-range-candidate
  [context pending current-entity idx clause]
  (when-let [{:keys [source entity attr value] :as producer}
             (data-pattern-parts clause '$)]
    (let [bound      (qresolve/bound-vars context)
          source-db  (get (:sources context) source)
          schema      (when source-db (db/-schema source-db))
          attr-schema (get schema attr)]
      (when (and (= entity current-entity)
                 source-db
                 (db/-searchable? source-db)
                 attr-schema
                 (not (contains? bound value))
                 (qor/exact-inequality-range?
                   (idx/value-type attr-schema))
                 (not (identical? :db.cardinality/many
                                  (:db/cardinality attr-schema)))
                 (not (query-result-var? context value)))
        (let [uses       (keep-indexed
                           (fn [i candidate]
                             (when (contains? (qu/collect-vars candidate) value)
                               i))
                           pending)
              pred-idxs  (remove #{idx} uses)
              boundaries (mapv
                           (fn [i]
                             (when-let [boundary
                                        (range-boundary context value
                                                        (nth pending i))]
                               (assoc boundary :idx i)))
                           pred-idxs)
              lower      (filterv #(= :lower (:side %)) boundaries)
              upper      (filterv #(= :upper (:side %)) boundaries)]
          (when (and (= 3 (count uses))
                     (= 2 (count pred-idxs))
                     (every? some? boundaries)
                     (= 1 (count lower))
                     (= 1 (count upper)))
            (let [predicate-idxs (sort (map :idx boundaries))
                  selected-idxs  (into [idx] predicate-idxs)]
              (assoc producer
                     :kind              :indexed-range
                     :source-db         source-db
                     :pattern-clause    clause
                     :predicate-clauses (mapv #(nth pending %)
                                               predicate-idxs)
                     :selected-idxs     selected-idxs
                     :selected-clauses  (mapv #(nth pending %)
                                               selected-idxs)
                     :range             [(:endpoint (first lower))
                                         (:endpoint (first upper))]))))))))

(defn- indexed-range-fanout
  [source attr [[lower-kind lower] [upper-kind upper]]]
  (let [^long comparison (dd/compare-with-type lower upper)]
    (if (or (pos? comparison)
            (and (zero? comparison)
                 (or (identical? lower-kind :open)
                     (identical? upper-kind :open))))
      0
      (let [inclusive (long (db/-index-range-size source attr lower upper))
            lower-n   (if (identical? lower-kind :open)
                        (indexed-fanout source attr [lower])
                        0)
            upper-n   (if (identical? upper-kind :open)
                        (indexed-fanout source attr [upper])
                        0)]
        (max 0 (- inclusive lower-n upper-n))))))

(defn- isolated-range-switch-cost
  ^double [^long fanout ^double producer-cost]
  (let [materialization-cost
        (* (double fanout)
           (+ (double c/magic-cost-hash-join-output-tuple)
              (double c/magic-cost-hash-join-output-cell)))
        follow-up-cost (indexed-producer-cost fanout fanout)]
    (+ producer-cost materialization-cost follow-up-cost)))

(defn- bound-pattern-producer
  [context clause]
  (when-let [{:keys [source entity value] :as producer}
             (data-pattern-parts clause '$)]
    (let [bound  (qresolve/bound-vars context)
          source (get (:sources context) source)
          values (context-bound-values context value)]
      (when (and source
                 (db/-searchable? source)
                 (not (contains? bound entity))
                 (contains? bound value)
                 (some? values))
        (assoc producer :source-db source :values values)))))

(defn- union-producer-estimate
  [context {:keys [source attr producers] :as union}]
  (when-let [source-db (get (:sources context) source)]
    (when (db/-searchable? source-db)
      (let [{:keys [probes output]}
            (reduce
              (fn [{:keys [^long probes ^long output]} producer]
                (let [values (context-bound-values context (:value producer))]
                  {:probes (saturating-add probes (long (count values)))
                   :output (saturating-add
                             output
                             (indexed-fanout source-db attr values))}))
              {:probes 0 :output 0}
              producers)]
        (assoc union
               :probe-count probes
               :fanout output
               :cost (indexed-producer-cost probes output))))))

(defn- indexed-union-alternatives
  [context pending entity]
  (keep-indexed
    (fn [idx candidate]
      (when (pos? (long idx))
        (when-let [union (indexed-or-union context candidate entity)]
          (when-let [estimate (union-producer-estimate context union)]
            (assoc estimate
                   :kind             :indexed-union
                   :selected-idxs    [idx]
                   :selected-clauses [candidate]
                   :producer-cost    (:cost estimate)
                   :switch-cost
                   (isolated-union-switch-cost
                     context candidate (:fanout estimate)
                     (double (:cost estimate))))))))
    pending))

(defn- indexed-range-alternatives
  [context pending entity]
  (keep-indexed
    (fn [idx candidate]
      (when (pos? (long idx))
        (when-let [{:keys [source-db attr range] :as producer}
                   (indexed-range-candidate
                     context pending entity idx candidate)]
          (let [fanout (long (indexed-range-fanout source-db attr range))
                cost   (* (double fanout)
                          (double c/magic-cost-init-scan-e))]
            (assoc producer
                   :fanout       fanout
                   :producer-cost cost
                   :switch-cost  (isolated-range-switch-cost fanout cost))))))
    pending))

(defn- late-producer-decision
  [clause entity values pattern-fanout pattern-cost best]
  (let [switch-cost (double (:switch-cost best))
        switch?     (< switch-cost (double pattern-cost))
        strategy    (if switch?
                      (case (:kind best)
                        :indexed-union :indexed-union-first
                        :indexed-range :indexed-range-first)
                      :bound-pattern-first)
        common      {:strategy       strategy
                     :entity         entity
                     :bound-pattern  clause
                     :pattern-probes (count values)
                     :pattern-fanout (long pattern-fanout)
                     :pattern-cost   (double pattern-cost)}]
    (case (:kind best)
      :indexed-union
      (merge common
             {:indexed-union     (:clause best)
              :union-probes      (:probe-count best)
              :union-fanout      (:fanout best)
              :union-cost        (:producer-cost best)
              :union-switch-cost switch-cost})

      :indexed-range
      (merge common
             {:range-pattern     (:pattern-clause best)
              :range-predicates  (:predicate-clauses best)
              :indexed-range     (:range best)
              :range-fanout      (:fanout best)
              :range-cost        (:producer-cost best)
              :range-switch-cost switch-cost}))))

(defn- cheaper-late-producer
  ([context pending]
   (cheaper-late-producer context pending #{:indexed-union :indexed-range}))
  ([context pending kinds]
   (let [clause (first pending)]
     (when-let [{:keys [entity attr source-db values]}
                (bound-pattern-producer context clause)]
       (let [alternatives
             (concat
               (when (contains? kinds :indexed-union)
                 (indexed-union-alternatives context pending entity))
               (when (contains? kinds :indexed-range)
                 (indexed-range-alternatives context pending entity)))]
         ;; Do no index counting on the common path where no compatible
         ;; producer is waiting. Ordinary JOB late clauses therefore pay no
         ;; scheduling overhead merely because their value is already bound.
         (when (seq alternatives)
           (let [pattern-fanout (indexed-fanout source-db attr values)
                 pattern-cost   (indexed-producer-cost (count values)
                                                       pattern-fanout)
                 best           (apply min-key :switch-cost alternatives)
                 decision       (late-producer-decision
                                  clause entity values pattern-fanout
                                  pattern-cost best)
                 switch?        (not= :bound-pattern-first
                                      (:strategy decision))]
             {:idxs     (if switch? (:selected-idxs best) [0])
              :producer best
              :decision decision})))))))

(defn- isolated-union-context
  [context clause]
  (let [vars       (or-join-vars clause)
        bound      (qresolve/bound-vars context)
        bound-vars (filterv bound vars)
        new-vars   (filterv (complement bound) vars)
        seed-rels
        (keep
          (fn [rel]
            (let [rel-vars (filterv #(contains? (:attrs rel) %) bound-vars)]
              (when (seq rel-vars)
                (r/project-distinct rel rel-vars))))
          (:rels context))
        seed-rel   (if (< 1 (count seed-rels))
                     (reduce j/hash-join seed-rels)
                     (first seed-rels))
        resolved   (qresolve/resolve-clause
                     (assoc context :rels [seed-rel]) clause)
        union-rel  (if (< 1 (count (:rels resolved)))
                     (reduce j/hash-join (:rels resolved))
                     (first (:rels resolved)))
        projected  (r/project-distinct union-rel new-vars)]
    ;; The bound seed variables have already constrained the union. Keeping
    ;; them on the new relation would eagerly cross-product the union with an
    ;; unrelated outer relation that happens to carry the same singleton.
    (update context :rels conj projected)))

(defn- isolated-range-context
  [context {:keys [entity attr range source-db]}]
  (let [tuples (db/-init-tuples-list source-db attr [range] nil false)]
    (update context :rels conj (r/relation! {entity 0} tuples))))

(defn- resolve-late-clauses
  [context clauses]
  (loop [context  context
         pending  (vec clauses)
         executed []]
    (if (empty? pending)
      (assoc context :late-clauses executed)
      (let [clause           (first pending)
            choice           (cheaper-late-producer context pending)
            selected-idxs    (or (:idxs choice) [0])
            selected-clauses (if (and choice
                                      (not= :bound-pattern-first
                                            (get-in choice
                                                    [:decision :strategy])))
                               (get-in choice [:producer :selected-clauses])
                               [clause])
            strategy         (get-in choice [:decision :strategy])]
        (when-let [decision (:decision choice)]
          (when qplan/*explain*
            (vswap! qplan/*explain* update :late-clause-decisions
                    (fnil conj []) decision)))
        (recur (case strategy
                 :indexed-union-first
                 (isolated-union-context
                   context (get-in choice [:producer :clause]))

                 :indexed-range-first
                 (isolated-range-context context (:producer choice))

                 (qresolve/resolve-clause context clause))
               (u/remove-idxs (set selected-idxs) pending)
               (into executed selected-clauses))))))

(defn- plan-explain
  []
  (when qplan/*explain*
    (let [{:keys [^long parsing-time ^long building-time]} @qplan/*explain*]
      (vswap! qplan/*explain* assoc :planning-time
              (- ^long (System/nanoTime)
                 (+ ^long qplan/*start-time* parsing-time building-time))))))

(defn -q
  [context run?]
  (binding [qu/*implicit-source* (get (:sources context) '$)]
    (let [{:keys [result-set] :as context} (planning context)]
      (if (= result-set #{})
        (do (plan-explain) context)
        (as-> context c
          (do (plan-explain) c)
          (if run? (execute-plan c) c)
          (if run?
            (resolve-late-clauses c (:late-clauses c))
            c))))))

(defn -collect-tuples
  [acc rel ^ints dst-idxs ^ints src-idxs]
  (let [n (alength src-idxs)]
    (->Eduction
      (comp
        (map (fn [^objects t1]
               (->Eduction
                 (map (fn [t2]
                        (let [res (aclone t1)]
                          (if (u/array? t2)
                            (dotimes [i n]
                              (aset res (aget dst-idxs i)
                                    (aget ^objects t2 (aget src-idxs i))))
                            (dotimes [i n]
                              (aset res (aget dst-idxs i)
                                    (get t2 (aget src-idxs i)))))
                          res)))
                 (:tuples rel))))
        cat)
      acc)))

(defn -collect
  ([context symbols]
   (let [rels (:rels context)]
     (-collect [(make-array Object (count symbols))] rels symbols)))
  ([acc rels symbols]
   (cond+
     :let [rel (first rels)]

     (nil? rel) acc

     (empty? (:tuples rel)) []

     :let [keep-attrs (select-keys (:attrs rel) symbols)]

     (empty? keep-attrs) (recur acc (next rels) symbols)

     :let [copy-pairs (keep-indexed
                        (fn [dst-idx sym]
                          (when-some [src-idx (get keep-attrs sym)]
                            [dst-idx src-idx]))
                        symbols)
           dst-idxs  (int-array (map first copy-pairs))
           src-idxs  (int-array (map second copy-pairs))]

     :else
     (recur (-collect-tuples acc rel dst-idxs src-idxs)
            (next rels) symbols))))

(defn collect
  [{:keys [result-set] :as context} symbols]
  (if (= result-set #{})
    context
    (assoc context :result-set (into (sp/new-spillable-set) (map vec)
                                     (-collect context symbols)))))

(defn- typed-aget [a i]
  (aget ^objects a ^Long i))

(defn- tuple-get [tuple]
  (if (u/array? tuple) typed-aget get))

(defn tuples->return-map
  [return-map tuples]
  (if (seq tuples)
    (let [symbols (:symbols return-map)
          idxs    (range 0 (count symbols))
          get-i   (tuple-get (first tuples))]
      (persistent!
        (reduce
          (fn [coll tuple]
            (conj! coll
                   (persistent!
                     (reduce
                       (fn [m i] (assoc! m (nth symbols i) (get-i tuple i)))
                       (transient {}) idxs))))
          (transient #{}) tuples)))
    #{}))

(defprotocol IPostProcess
  (-post-process [find return-map tuples]))

(extend-protocol IPostProcess
  FindRel
  (-post-process [_ return-map tuples]
    (if (nil? return-map)
      tuples
      (tuples->return-map return-map tuples)))

  FindColl
  (-post-process [_ _ tuples]
    (into [] (map first) tuples))

  FindScalar
  (-post-process [_ _ tuples]
    (ffirst tuples))

  FindTuple
  (-post-process [_ return-map tuples]
    (if (some? return-map)
      (first (tuples->return-map return-map [(first tuples)]))
      (first tuples))))

(defn- pull
  [find-elements context resultset]
  (let [resolved (for [find find-elements]
                   (when (dp/pull? find)
                     (let [db      (qagg/-context-resolve (:source find) context)
                           pattern (qagg/-context-resolve (:pattern find) context)]
                       (dpa/parse-opts db pattern))))]
    (for [tuple resultset]
      (mapv
        (fn [parsed-opts el]
          (if parsed-opts (dpa/pull-impl parsed-opts el) el))
        resolved
        tuple))))

(defn- redundant-pattern-group-strategy
  [sources patterns]
  (let [{:keys [source pattern]} (first patterns)
        attr-term               (nth pattern 1 nil)
        attr                    (when (instance? Constant attr-term)
                                  (:value attr-term))
        source                  (get sources (or (:symbol source) '$))]
    (when source
      (if (and (keyword? attr) (db/-searchable? source))
        (if (identical? :db.cardinality/many
                        (get-in (db/-schema source) [attr :db/cardinality]))
          :materialize
          :elide)
        :materialize))))

(defn- pattern-value
  [pattern]
  (nth (:pattern pattern) 2 nil))

(defn- constant-value-pattern
  [patterns]
  (some #(when (instance? Constant (pattern-value %)) %) patterns))

(defn- remove-pattern-clause
  [context pattern]
  (let [qwhere (get-in context [:parsed-q :qwhere])]
    (if-some [idx (u/index-of #(= pattern %) qwhere)]
      (-> context
          (update-in [:parsed-q :qwhere] #(u/remove-idxs #{idx} %))
          (update-in [:parsed-q :qorig-where] #(u/remove-idxs #{idx} %)))
      context)))

(defn- elide-cardinality-one-patterns
  [context patterns constant-pattern]
  (let [v (:value ^Constant (pattern-value constant-pattern))]
    (reduce
      (fn [context pattern]
        (let [value (pattern-value pattern)]
          (cond
            (instance? Variable value)
            (-> context
                (remove-pattern-clause pattern)
                (update :rels conj
                        (r/relation! {(:symbol ^Variable value) 0}
                                     (doto (FastList.)
                                       (.add (object-array [v]))))))

            (not (instance? Constant value))
            (remove-pattern-clause context pattern)

            :else
            context)))
      context patterns)))

(defn- materialize-repeated-patterns
  [context patterns]
  (let [patterns (sort-by #(if (instance? Constant (pattern-value %)) 0 1)
                          patterns)
        context  (binding [qu/*implicit-source* (get (:sources context) '$)]
                   (reduce (fn [context pattern]
                             (qresolve/resolve-clause context
                                                      (dp/source pattern)))
                           context patterns))]
    (reduce remove-pattern-clause context patterns)))

(defn resolve-redudants
  "Resolve repeated source/entity/attribute patterns containing a constant.
  Cardinality-one variable values are determined by the constant and can be
  elided. Cardinality-many and schema-unknown groups are materialized with the
  constant pattern first so every matching variable value is retained."
  [{:keys [parsed-q sources] :as context}]
  (let [{:keys [qwhere]} parsed-q
        redundant-groups
        (into []
              (->> qwhere
                   (eduction (filter #(instance? Pattern %)))
                   (group-by (fn [{:keys [source pattern]}]
                               [source (first pattern) (second pattern)]))
                   (eduction (filter
                               #(let [ps (val %)]
                                  (and (< 1 (count ps))
                                       (constant-value-pattern ps)
                                       (redundant-pattern-group-strategy
                                         sources ps)))))))]
    (reduce
      (fn [c [_ patterns]]
        (let [constant-pattern (constant-value-pattern patterns)]
          (case (redundant-pattern-group-strategy sources patterns)
            :elide
            (elide-cardinality-one-patterns c patterns constant-pattern)

            :materialize
            (materialize-repeated-patterns c patterns)

            c)))
      context
      redundant-groups)))

(defn result-explain
  ([context result]
   (result-explain context)
   (when qplan/*explain* (vswap! qplan/*explain* assoc :result result)))
  ([{:keys [graph result-set plan opt-clauses late-clauses run?
            access-plans preferred-access-plan property-memo]
     :as context}]
   (when qplan/*explain*
     (let [{:keys [^long planning-time ^long parsing-time ^long building-time]}
           @qplan/*explain*
           memo-summary (some-> property-memo qo/property-memo-summary)
           conventional-cost
           (some #(when (= :conventional (:kind %)) (:cost %))
                 (:alternatives memo-summary))
           et  (double (/ (- ^long (System/nanoTime)
                             (+ ^long qplan/*start-time* planning-time
                                parsing-time building-time))
                          1000000))
           bt  (double (/ building-time 1000000))
           plt (double (/ planning-time 1000000))
           pat (double (/ parsing-time 1000000))
           ppt (double (/ (+ parsing-time building-time planning-time)
                          1000000))]
       (vswap! qplan/*explain* assoc
               :actual-result-size (count result-set)
               :parsing-time (format "%.3f" pat)
               :building-time (format "%.3f" bt)
               :planning-time (format "%.3f" plt)
               :prepare-time (format "%.3f" ppt)
               :execution-time (format "%.3f" et)
               :opt-clauses opt-clauses
               :query-graph (w/postwalk
                              (fn [e]
                                (if (map? e)
                                  (apply dissoc e
                                         (for [[k v] e
                                               :when (nil? v)] k))
                                  e)) graph)
               :plan (w/postwalk
                       (fn [e]
                         (if (qplan/plan? e)
                           (let [{:keys [steps] :as plan} e]
                             (cond->
                                 (assoc plan :steps
                                        (mapv #(qplan/step-explain % context) steps))
                               (and run? qplan/*intermediate-counts?*)
                               (assoc :actual-size
                                      (get-in @(:intermediates context)
                                               [(:out (last steps))
                                                :tuples-count]))))
                           e)) plan)
               :late-clauses late-clauses
               :access-plans (mapv qaccess/plan-summary access-plans)
               :preferred-access-plan
               (some-> preferred-access-plan qaccess/plan-summary)
               :conventional-plan-cost conventional-cost
               :access-path-selected?
               (= :access (get-in memo-summary [:selected :kind]))
               :physical-plan-alternatives (:alternatives memo-summary)
               :physical-plan-subsets (:subsets memo-summary)
               :selected-plan-alternative (:selected memo-summary)
               :recommended-plan-alternative (:selected memo-summary)
               :executed-plan-alternative
               {:kind (if run? :conventional :not-run)})))))

(defn- order-comps
  [tg find-vars order]
  (let [pairs     (vec (partition-all 2 order))
        n         (count pairs)
        idxs      (long-array n)
        ascending (boolean-array n)]
    (dotimes [i n]
      (let [[v direction] (pairs i)]
        (aset idxs i (long (if (integer? v)
                             v
                             (u/index-of #(= v %) find-vars))))
        (aset ascending i (identical? direction :asc))))
    (reify Comparator
      (compare [_ t1 t2]
        (loop [i 0]
          (if (< i n)
            (let [idx (aget idxs i)
                  res (if (aget ascending i)
                        (compare (tg t1 idx) (tg t2 idx))
                        (compare (tg t2 idx) (tg t1 idx)))]
              (if (zero? res)
                (recur (unchecked-inc-int i))
                res))
            0))))))

(defn- finite-limit?
  [limit]
  (and (some? limit) (not= -1 limit)))

(defn- result-window
  [result limit offset]
  (let [offset (long (or offset 0))]
    (if (or (pos? offset) (finite-limit? limit))
      (into []
            (cond-> (drop offset)
              (finite-limit? limit) (comp (take limit)))
            result)
      result)))

(defn- top-k-result
  [^Comparator cmp result limit offset]
  (let [offset     (long (or offset 0))
        window-end (+ offset (long limit))]
    (if (or (zero? window-end) (> window-end Integer/MAX_VALUE))
      (if (zero? window-end)
        []
        (result-window (sort cmp result) limit offset))
      (let [worst-first
            (reify Comparator
              (compare [_ a b] (.compare cmp b a)))
            ^PriorityQueue heap
            (PriorityQueue. (int (max 1 window-end)) worst-first)]
        (doseq [tuple result]
          (if (< (.size heap) window-end)
            (.add heap tuple)
            (when (neg? (.compare cmp tuple (.peek heap)))
              (.poll heap)
              (.add heap tuple))))
        (result-window (sort cmp (seq heap)) limit offset)))))

(defn- order-result
  [find-vars result order limit offset]
  (if (seq result)
    (let [cmp (order-comps (tuple-get (first result)) find-vars order)]
      (if (finite-limit? limit)
        (top-k-result cmp result limit offset)
        (result-window (sort cmp result) limit offset)))
    result))

(def ^:private ^:const ^long top-k-max-candidate-batches 32)

(defn- access-outer-query
  [parsed-q {:keys [outer-cols outer-joins]}]
  (let [where      (mapv :clause outer-joins)
        orig-where (mapv :orig-clause outer-joins)]
    (assoc parsed-q
           :qfind       (dp/parse-find outer-cols)
           :qorig-find  outer-cols
           :qwith       nil
           :qreturn-map nil
           :qwhere      where
           :qorig-where orig-where
           :qhaving     nil
           :qorder      nil
           :qlimit      nil
           :qoffset     nil)))

(defn- access-batch-query
  [parsed-q {:keys [expr joins outer-joins step fragment-cols]} adaptive?]
  (let [covered-clauses   (or (:covers expr) #{})
        covered-originals (or (:covered-originals expr) #{})
        fragment-clauses  (into #{} (map :clause) joins)
        fragment-originals
        (into #{}
              (map #(nth (:qorig-where parsed-q) (:clause-idx %)))
              joins)
        outer-clauses     (into #{} (map :clause) outer-joins)
        outer-originals   (into #{} (map :orig-clause) outer-joins)
        cols              (or fragment-cols (:cols step))]
    (cond->
        (-> parsed-q
            (update :qin conj (dp/parse-binding [cols]))
            (update :qwhere
                    #(into []
                           (remove
                             (some-fn covered-clauses fragment-clauses
                                      outer-clauses))
                           %))
            (update :qorig-where
                    #(into []
                           (remove
                             (some-fn covered-originals fragment-originals
                                      outer-originals))
                           %)))
      adaptive? (assoc :qorder nil :qlimit nil :qoffset nil))))

(defn- access-fragment-pattern
  [parsed-q clause-idx]
  (let [pattern (nth (:qorig-where parsed-q) clause-idx)]
    (if (and (vector? pattern) (qu/source? (first pattern)))
      (subvec pattern 1)
      pattern)))

(defn- project-access-relation
  [relation cols]
  (let [attrs       (:attrs relation)
        ^java.util.List tuples (:tuples relation)
        n           (.size tuples)
        output-attrs (qplan/cols->attrs cols)]
    ;; Some joins report only the looked-up side's schema when their result is
    ;; empty. Preserve the fragment's declared schema without trying to project
    ;; columns from an empty intermediate.
    (if (zero? n)
      (r/relation! output-attrs (FastList.))
      (let [indices    (mapv attrs cols)
            missing    (into []
                             (keep-indexed
                               (fn [i idx]
                                 (when (nil? idx) (nth cols i))))
                             indices)
            _          (when (seq missing)
                         (throw
                           (ex-info "Access fragment lost projected columns"
                                    {:missing missing
                                     :available (keys attrs)
                                     :projected cols})))
            projected  (FastList. n)
            ^ints idxs (int-array indices)
            width      (alength idxs)]
        (dotimes [i n]
          (let [^objects tuple (.get tuples i)
                ^objects output (object-array width)]
            (dotimes [j width]
              (aset output j (aget tuple (aget idxs j))))
            (.add projected output)))
        (r/relation! output-attrs projected)))))

(defn- execute-index-fragment-join
  [parsed-q source-db relation join output-cols]
  (let [pattern (access-fragment-pattern parsed-q (:clause-idx join))
        ^java.util.List tuples (:tuples relation)
        joined (FastList.)]
    (dotimes [i (.size tuples)]
      (let [tuple   (.get tuples i)
            one     (r/relation! (:attrs relation)
                                 (doto (FastList.) (.add tuple)))
            context (assoc (qplan/make-context parsed-q false)
                           :rels [one])
            lookup  (qresolve/lookup-pattern context source-db pattern)]
        ;; A bound index lookup can legitimately miss for an individual
        ;; access tuple. `lookup-pattern` represents that as a relation with a
        ;; nil tuple list; it is an empty join result, not an input to
        ;; `prod-rel`.
        (when (some? (:tuples lookup))
          (let [result (reduce r/prod-rel
                               (qresolve/collapse-rels [one] lookup))
                projected (project-access-relation result output-cols)]
            (.addAll joined ^java.util.Collection (:tuples projected))))))
    (r/relation! (qplan/cols->attrs output-cols) joined)))

(defn- execute-hash-fragment-join
  [parsed-q source-db relation join output-cols]
  (let [pattern (access-fragment-pattern parsed-q (:clause-idx join))
        context (assoc (qplan/make-context parsed-q false)
                       :rels [relation])
        lookup  (qresolve/lookup-pattern context source-db pattern)]
    (if (some? (:tuples lookup))
      (let [result (reduce r/prod-rel
                           (qresolve/collapse-rels [relation] lookup))]
        (project-access-relation result output-cols))
      (r/relation! (qplan/cols->attrs output-cols) (FastList.)))))

(defn- execute-access-fragment
  [parsed-q source-db {:keys [step joins operators]} tuples]
  (if (seq joins)
    (:tuples
      (first
        (reduce
          (fn [[relation cols] [join operator]]
            (let [output-cols
                  (into cols (remove (set cols)) (:produces-cols join))
                  relation
                  (case (:type operator)
                    :index-join
                    (execute-index-fragment-join
                      parsed-q source-db relation join output-cols)

                    :hash-join
                    (execute-hash-fragment-join
                      parsed-q source-db relation join output-cols)

                    relation)]
              [relation output-cols]))
          [(r/relation! (qplan/cols->attrs (:cols step)) tuples)
           (vec (:cols step))]
          (map vector joins operators))))
    tuples))

(defn- past-top-k-boundary?
  [path demand find-vars order rows frontier window-end]
  (let [window-end (long window-end)]
    (when (and frontier (<= window-end (long (count rows))))
      (let [cmp          (order-comps (tuple-get (first rows)) find-vars order)
            cutoff-row   (nth (sort cmp rows) (unchecked-dec window-end))
            order-var    (first order)
            order-idx    (u/index-of #(= order-var %) find-vars)
            cutoff-value (nth cutoff-row order-idx)]
        (qaccess/frontier-satisfies?
          path demand frontier
          {:row           cutoff-row
           :find-vars     find-vars
           :ordering      order
           :primary-value cutoff-value})))))

(declare execute-query execute-planned-query)

(defn- access-source-db
  [step inputs]
  (or (:access-source step)
      (first (filter db/db? inputs))))

(defn- top-k-pushdown-query
  [parsed-q inputs
   {:keys [source residual-query demand work fallback access-plan]}]
  (let [path        (:path source)
        source-db   (access-source-db source inputs)
        batch-query residual-query
        find-vars   (dp/find-vars (:qfind parsed-q))
        order       (:ordering demand)
        limit       (:limit demand)
        offset      (:offset demand)
        window-end  (:required-count demand)
        work-budget (:max-candidates work)
        budgeted?   (some? work-budget)
        sample-batch (:sample-batch source)
        sample-tuples (:tuples sample-batch)
        sample-count (long (if sample-tuples
                             (.size ^java.util.List sample-tuples)
                             0))
        sample-rows
        (if (pos? sample-count)
          (into #{}
                (execute-query batch-query
                               (conj
                                 (vec inputs)
                                 (execute-access-fragment
                                   parsed-q source-db access-plan
                                   sample-tuples))))
          #{})
        sample-done?
        (or (:exhausted? sample-batch)
            (past-top-k-boundary?
              path demand find-vars order sample-rows
              (:frontier sample-batch) window-end))
        attempt-work
        (cond-> work
          (and budgeted? (pos? (long work-budget)))
          (assoc :batch-size
                 (min (long (or (:batch-size work) work-budget))
                      (long work-budget)))

          sample-batch
          (assoc :resume (:frontier sample-batch)
                 :emitted sample-count))
        fallback-query #(execute-planned-query (:context fallback) inputs)]
    (cond
      sample-done?
      (order-result find-vars sample-rows order limit offset)

      (and budgeted?
           (or (not (pos? (long work-budget)))
               (>= sample-count (long work-budget))))
      (fallback-query)

      :else
      (let [cursor (qaccess/open-access
                     path demand (:bounds source) attempt-work source-db nil)]
        (try
          (loop [rows sample-rows
                 batches (if sample-batch 1 0)
                 scanned sample-count]
            (if (or (and (not budgeted?)
                         (>= (long batches) top-k-max-candidate-batches))
                    (and budgeted?
                         (>= (long scanned) (long work-budget))))
              (fallback-query)
              (let [{:keys [tuples frontier exhausted?] :as batch}
                    (qaccess/next-batch cursor)]
                (if (zero? (.size ^java.util.List tuples))
                  (if exhausted?
                    (order-result find-vars rows order limit offset)
                    (fallback-query))
                  (let [batch-result
                        (execute-query batch-query
                                       (conj
                                         (vec inputs)
                                         (execute-access-fragment
                                           parsed-q source-db access-plan
                                           tuples)))
                        rows (into rows batch-result)]
                    (if (or exhausted?
                            (past-top-k-boundary?
                              path demand find-vars order rows
                              frontier window-end))
                      (order-result find-vars rows order limit offset)
                      (recur rows
                             (unchecked-inc-int batches)
                             (+ (long scanned)
                                (qaccess/batch-work batch)))))))))
          (finally
            (qaccess/close-cursor cursor)))))))

(defn- limit-pushdown-query
  [parsed-q inputs
   {:keys [source residual-query demand work fallback access-plan]}]
  (let [path          (:path source)
        source-db     (access-source-db source inputs)
        batch-query   residual-query
        limit         (:limit demand)
        offset        (:offset demand)
        window-end    (long (:required-count demand))
        work-budget   (:max-candidates work)
        budgeted?     (some? work-budget)
        sample-batch  (:sample-batch source)
        sample-tuples (:tuples sample-batch)
        sample-count  (long (if sample-tuples
                              (.size ^java.util.List sample-tuples)
                              0))
        sample-work   (if sample-batch
                        (qaccess/batch-work sample-batch)
                        0)
        sample-rows
        (if (pos? sample-count)
          (into #{}
                (execute-query batch-query
                               (conj
                                 (vec inputs)
                                 (execute-access-fragment
                                   parsed-q source-db access-plan
                                   sample-tuples))))
          #{})
        sample-done?
        (or (:exhausted? sample-batch)
            (<= window-end (long (count sample-rows))))
        attempt-work
        (cond-> work
          (and budgeted? (pos? (long work-budget)))
          (assoc :batch-size
                 (min (long (or (:batch-size work) work-budget))
                      (long work-budget)))

          sample-batch
          (assoc :resume (:frontier sample-batch)
                 :emitted sample-work))
        fallback-query #(execute-planned-query (:context fallback) inputs)
        finish         #(result-window % limit offset)]
    (cond
      (zero? window-end)
      []

      sample-done?
      (finish sample-rows)

      (and budgeted?
           (or (not (pos? (long work-budget)))
               (>= sample-work (long work-budget))))
      (fallback-query)

      :else
      (let [cursor (qaccess/open-access
                     path demand (:bounds source) attempt-work source-db nil)]
        (try
          (loop [rows    sample-rows
                 batches (if sample-batch 1 0)
                 scanned sample-work]
            (if (or (and (not budgeted?)
                         (>= (long batches) top-k-max-candidate-batches))
                    (and budgeted?
                         (>= (long scanned) (long work-budget))))
              (fallback-query)
              (let [{:keys [tuples exhausted?] :as batch}
                    (qaccess/next-batch cursor)
                    batch-work (qaccess/batch-work batch)
                    scanned    (+ (long scanned) batch-work)]
                (if (zero? (.size ^java.util.List tuples))
                  (cond
                    exhausted?
                    (finish rows)

                    (zero? batch-work)
                    (fallback-query)

                    :else
                    (recur rows (unchecked-inc-int batches) scanned))
                  (let [batch-result
                        (execute-query batch-query
                                       (conj
                                         (vec inputs)
                                         (execute-access-fragment
                                           parsed-q source-db access-plan
                                           tuples)))
                        rows (into rows batch-result)]
                    (if (or exhausted?
                            (<= window-end (long (count rows))))
                      (finish rows)
                      (recur rows
                             (unchecked-inc-int batches)
                             scanned)))))))
          (finally
            (qaccess/close-cursor cursor)))))))

(defn- finish-query
  [parsed-q context]
  (let [find          (:qfind parsed-q)
        find-elements (dp/find-elements find)
        result-arity  (count find-elements)
        with          (:qwith parsed-q)
        having        (:qhaving parsed-q)
        find-vars     (dp/find-vars find)
        all-vars      (concatv find-vars (map :symbol with))
        context       (collect context all-vars)
        result
        (cond->> (:result-set context)
          with (mapv #(subvec % 0 result-arity))

          (some #(or (dp/aggregate? %) (dp/find-expr? %)) find-elements)
          (qagg/aggregate find-elements context)

          (seq having)
          (qagg/apply-having having find-elements)

          (some dp/pull? find-elements)
          (pull find-elements context)

          true
          (-post-process find (:qreturn-map parsed-q)))]
    (result-explain context result)
    (if (instance? FindRel find)
      (if-let [order (:qorder parsed-q)]
        (order-result find-vars result order
                      (:qlimit parsed-q) (:qoffset parsed-q))
        (result-window result (:qlimit parsed-q) (:qoffset parsed-q)))
      result)))

(defn- run-planned-context
  [{:keys [result-set late-clauses sources] :as context}]
  (binding [qu/*implicit-source* (get sources '$)]
    (if (= result-set #{})
      context
      (let [context (execute-plan context)]
        (resolve-late-clauses context late-clauses)))))

(defn- execute-planned-query
  [context inputs]
  (let [parsed-q (:parsed-q context)
        udf-db   (first (filter db/-searchable? inputs))]
    (binding [built-ins/*udf-db* udf-db]
      (finish-query parsed-q (run-planned-context context)))))

(defn- execute-query
  ([parsed-q inputs]
   (execute-query parsed-q inputs []))
  ([parsed-q inputs access-plans]
   (let [[parsed-q inputs] (plugin-inputs parsed-q inputs)
         udf-db            (first (filter db/-searchable? inputs))
         context
         (binding [built-ins/*udf-db* udf-db]
           (-> (qplan/make-context parsed-q true)
               (attach-access-plans access-plans)
               (qresolve/resolve-ins inputs)
               (materialize-input-bound-patterns)
               (resolve-redudants)
               (rules/rewrite)
               (push-down-equality-disjunctions)
               (rewrite-unused-vars)
               (materialize-selective-value-lookups)
               (-q true)))]
     (binding [built-ins/*udf-db* udf-db]
       (finish-query parsed-q context)))))

(defn- access-query-plan
  [parsed-q inputs access-plans]
  (let [[parsed-q inputs] (plugin-inputs parsed-q inputs)
        udf-db            (first (filter db/-searchable? inputs))]
    (binding [built-ins/*udf-db* udf-db]
      (-> (qplan/make-context parsed-q false)
          (attach-access-plans access-plans)
          (qresolve/resolve-ins inputs)
          (materialize-input-bound-patterns)
          (resolve-redudants)
          (rules/rewrite)
          (push-down-equality-disjunctions)
          (rewrite-unused-vars)
          (materialize-selective-value-lookups)
          (-q false)))))

(defmulti ^:private execute-alternative
  (fn [_parsed-q _inputs _access-plans alternative]
    (:kind alternative)))

(defmethod execute-alternative :access
  [parsed-q inputs _access-plans alternative]
  (let [{:keys [mode source outer-query access-plan] :as plan}
        (:plan alternative)
        adaptive?     (#{:adaptive-top-k :adaptive-limit} mode)
        residual-query (access-batch-query parsed-q access-plan adaptive?)
        outer-query   (or outer-query
                          (when (:correlated? access-plan)
                            (access-outer-query parsed-q access-plan)))
        plan          (assoc plan
                             :residual-query residual-query
                             :outer-query outer-query)]
    (case mode
      :adaptive-top-k
      (top-k-pushdown-query parsed-q inputs plan)

      :adaptive-limit
      (limit-pushdown-query parsed-q inputs plan)

      :correlated-complete
      (let [source-db (access-source-db source inputs)
            outer     (execute-query outer-query inputs)
            tuples    (->> (qplan/step-execute source source-db outer)
                           (execute-access-fragment
                             parsed-q source-db access-plan))]
        (execute-query residual-query (conj (vec inputs) tuples)))

      :complete
      (let [source-db (access-source-db source inputs)
            tuples    (->> (qplan/step-execute source source-db nil)
                           (execute-access-fragment
                             parsed-q source-db access-plan))]
        (execute-query residual-query (conj (vec inputs) tuples)))

      (execute-query parsed-q inputs))))

(defmethod execute-alternative :conventional
  [_parsed-q inputs _access-plans alternative]
  (execute-planned-query (get-in alternative [:plan :context]) inputs))

(defmethod execute-alternative :default
  [parsed-q inputs access-plans _alternative]
  (execute-query parsed-q inputs access-plans))

(defn q*
  [parsed-q inputs]
  (binding [timeout/*deadline* (timeout/effective-deadline
                                 (:qtimeout parsed-q))]
    (let [plans (discover-access-plans parsed-q inputs)]
      (cond
        qplan/*explain*
        (execute-query parsed-q inputs plans)

        (seq plans)
        (let [planned-context (access-query-plan parsed-q inputs plans)]
          (execute-alternative
            parsed-q inputs (:access-plans planned-context)
            (qo/selected-alternative planned-context)))

        :else
        (execute-query parsed-q inputs plans)))))

(defn mark-parsing-finished!
  []
  (when qplan/*explain*
    (vswap! qplan/*explain* assoc :parsing-time
            (- (System/nanoTime) ^long qplan/*start-time*))))

(defn plan-context*
  [parsed-q inputs]
  (binding [timeout/*deadline* (timeout/effective-deadline
                                 (:qtimeout parsed-q))]
    (let [plans             (discover-access-plans parsed-q inputs)
          [parsed-q inputs] (plugin-inputs parsed-q inputs)]
      (-> (qplan/make-context parsed-q false)
          (attach-access-plans plans)
          (qresolve/resolve-ins inputs)
          (materialize-input-bound-patterns)
          (resolve-redudants)
          (rules/rewrite)
          (push-down-equality-disjunctions)
          (rewrite-unused-vars)
          (materialize-selective-value-lookups)
          (-q false)))))

(defn plan*
  [parsed-q inputs]
  (result-explain (plan-context* parsed-q inputs)))

(defn- relation-product-count
  [rels]
  (reduce
    (fn [^long total rel]
      (Math/multiplyExact total
                          (long (.size ^java.util.List (:tuples rel)))))
    1 rels))

(defn count-plan*
  "Count tuples produced by the optimized where-clause plan without retaining
  its final output. Late clauses are applied to bounded batches so generated
  cardinality probes do not retain the complete intermediate relation."
  [parsed-q inputs]
  (binding [timeout/*deadline* (timeout/effective-deadline
                                 (:qtimeout parsed-q))]
    (let [udf-db (first (filter db/-searchable? inputs))]
      (binding [built-ins/*udf-db* udf-db]
        (let [{:keys [plan sources late-clauses rels result-set] :as context}
              (plan-context* parsed-q inputs)
              component-plans
              (vec
                (for [[src components] plan
                      plans components]
                  [(sources src) (vec (mapcat :steps plans))]))]
          (cond
            (= result-set #{}) 0

            (seq rels)
            (throw
              (ex-info "Streaming plan count does not support input relations"
                       {:relation-count (count rels)}))

            (seq late-clauses)
            (do
              (when-not (= 1 (count component-plans))
                (throw
                  (ex-info
                    "Streaming late-clause count requires one connected plan"
                    {:component-count (count component-plans)
                     :late-clauses late-clauses})))
              (let [[source steps] (first component-plans)
                    attrs          (qplan/step-attrs steps)]
                (qplan/reduce-step-batches
                  source steps 16384
                  (fn [^long total tuples]
                    (let [resolved
                          (binding [qu/*implicit-source* (get sources '$)]
                            (reduce
                              qresolve/resolve-clause
                              (assoc context
                                     :rels [(r/relation! attrs tuples)])
                              late-clauses))]
                      (Math/addExact total
                                     (long (relation-product-count
                                             (:rels resolved))))))
                  0)))

            :else
            (reduce
              (fn [^long total ^long component-count]
                (Math/multiplyExact total component-count))
              1
              (for [[source steps] component-plans]
                (qplan/count-steps source steps)))))))))
