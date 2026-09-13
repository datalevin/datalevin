;;
;; Copyright (c) Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns ^:no-doc datalevin.query.execute.late
  "Late-clause ordering, branch estimates, and indexed producer selection."
  (:refer-clojure :exclude [update assoc])
  (:require
   [datalevin.constants :as c]
   [datalevin.datom :as dd]
   [datalevin.db :as db]
   [datalevin.index :as idx]
   [datalevin.inline :refer [update assoc]]
   [datalevin.join :as j]
   [datalevin.parser :as dp]
   [datalevin.query-util :as qu]
   [datalevin.query.optimizer.range :as qor]
   [datalevin.query.plan :as qplan]
   [datalevin.query.resolve :as qresolve]
   [datalevin.relation :as r]
   [datalevin.util :as u])
  (:import
   [java.util List]))

(defn strip-clause-source
  [clause]
  (if (and (sequential? clause) (qu/source? (first clause)))
    (next clause)
    clause))

(defn clause-vars
  [form]
  (into #{} (filter qu/binding-var?) (qu/collect-vars form)))

(defn late-clause-deps
  [clause]
  (let [clause (strip-clause-source clause)
        head   (when (sequential? clause) (first clause))]
    (cond
      (and (sequential? clause) (sequential? head))
      {:requires (qu/call-vars head)
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

(defn- late-rule-call?
  [rules clause]
  (let [clause (strip-clause-source clause)
        head   (when (sequential? clause) (first clause))]
    (and (symbol? head) (contains? rules head))))

(defn- bound-rule-arg-count
  [bound clause]
  (count
    (filter
      (fn [arg]
        (if (qu/binding-var? arg)
          (contains? bound arg)
          (not (qu/placeholder? arg))))
      (qu/clause-args clause))))

(defn- best-ready-late-clause
  "Retain dependency order generally. If the first ready clause is a rule,
   prefer another ready rule with more bound arguments so predicate evaluation
   starts from the more selective side of a rule DAG."
  [bound rules todo ready]
  (let [first-ready (first ready)
        first-entry (nth todo first-ready)]
    (if (late-rule-call? rules (:clause first-entry))
      (reduce
        (fn [best idx]
          (let [entry (nth todo idx)]
            (if (and (late-rule-call? rules (:clause entry))
                     (> (long (bound-rule-arg-count bound (:clause entry)))
                        (long
                          (bound-rule-arg-count
                            bound (:clause (nth todo best))))))
              idx
              best)))
        first-ready (next ready))
      first-ready)))

(defn sort-late-clauses
  [initial-bound rules clauses]
  (let [entries (mapv #(assoc (late-clause-deps %) :clause %) clauses)]
    (loop [bound initial-bound
           todo  entries
           acc   []]
      (if (empty? todo)
        (mapv :clause acc)
        (if-let [ready (seq (keep-indexed
                              (fn [i entry]
                                (when (late-clause-ready? bound entry) i))
                              todo))]
          (let [idx   (best-ready-late-clause bound rules todo ready)
                entry (nth todo idx)]
            (recur (into bound (:provides entry))
                   (u/vec-remove todo idx)
                   (conj acc entry)))
          (mapv :clause (into acc todo)))))))

(defn- planned-step-vars
  [step]
  (clause-vars (:cols step)))

(defn planned-bound-vars
  [{:keys [plan] :as context}]
  (reduce
    into
    (qresolve/bound-vars context)
    (for [[_src components] plan
          plans components
          :let [step (some-> plans last :steps last)]
          :when step]
      (planned-step-vars step))))

(defn sort-planned-late-clauses
  [{:keys [late-clauses rules] :as context}]
  (cond-> context
    (seq late-clauses)
    (assoc :late-clauses
           (sort-late-clauses (planned-bound-vars context) rules
                              late-clauses))))

(defn- context-bound-values
  [context sym]
  (when-let [{:keys [attrs tuples]}
             (some #(when (contains? (:attrs %) sym) %) (:rels context))]
    (let [idx (long (attrs sym))]
      (into #{}
            (map (fn [^objects tuple] (aget tuple idx)))
            tuples))))

(defn data-pattern-parts
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

(def ^:const ^:private ^long late-or-join-branch-probe-limit 100000)

(def ^:const ^:private ^long late-or-join-exact-count-limit 8)

(defn- late-branch-pattern-parts
  [clause default-source]
  (when (vector? clause)
    (let [source? (qu/source? (first clause))
          pattern (if source? (subvec clause 1) clause)]
      (when (and (= 3 (count pattern))
                 (keyword? (second pattern)))
        {:source (if source? (first clause) default-source)
         :entity (first pattern)
         :attr   (second pattern)
         :value  (nth pattern 2)}))))

(defn- late-bound-term
  [context term]
  (cond
    (qu/binding-var? term)
    (when-let [{:keys [attrs tuples]}
               (some #(when (contains? (:attrs %) term) %) (:rels context))]
      (let [n (.size ^List tuples)]
        (if (<= n late-or-join-exact-count-limit)
          (let [values (into #{}
                             (map (fn [^objects tuple]
                                    (aget tuple (long (attrs term)))))
                             tuples)]
            {:probes (count values) :values values})
          ;; A large relation's row count is a cheap conservative proxy for
          ;; distinct bound values. Avoid materializing a set merely to choose
          ;; the next branch pattern; the lookup itself will deduplicate it.
          {:probes n})))

    (or (nil? term) (= '_ term) (qu/placeholder? term)) nil
    :else {:probes 1 :values #{term}}))

(defn- late-indexed-side-fanout
  [source attr side {:keys [probes values]}]
  (let [probes (long probes)]
    (when (<= probes late-or-join-branch-probe-limit)
      (try
        (if (some? values)
          {:fanout
           (reduce
             (fn [^long total value]
               (let [pattern (if (identical? side :entity)
                               [value attr nil]
                               [nil attr value])
                     resolved (qresolve/resolve-pattern-lookup-refs source
                                                                    pattern)]
                 (saturating-add total (long (db/-count source resolved)))))
             0 values)
           :confidence :counted}
          (let [scan-count (long (db/-count source [nil attr nil]))
                estimated (long
                            (Math/ceil
                              (* (double probes)
                                 (double c/magic-link-ratio))))]
            {:fanout (min scan-count estimated)
             :confidence :heuristic}))
        ;; Planning must not surface lookup-ref or storage errors from a clause
        ;; that normal source-order execution may never reach.
        (catch Exception _ nil)))))

(defn- cached-late-side-estimate
  [cache source-sym source attr side {:keys [probes values]}]
  (let [key [source-sym attr side probes values]
        m   @cache]
    (if (contains? m key)
      (get m key)
      (let [{:keys [fanout confidence]}
            (late-indexed-side-fanout source attr side
                                      {:probes probes :values values})
            result (when (some? fanout)
                     {:side       side
                      :probes     probes
                      :fanout     (long fanout)
                      :confidence confidence
                      :cost       (indexed-producer-cost probes
                                                         (long fanout))})]
        (vswap! cache assoc key result)
        result))))

(defn- cheaper-estimate
  [best candidate]
  (if (< (double (or (:rank-cost candidate) (:cost candidate)))
         (double (or (:rank-cost best) (:cost best))))
    candidate
    best))

(defn- comparable-estimates
  [estimates]
  (let [all-counted? (every? #(identical? :counted (:confidence %))
                             estimates)]
    (mapv (fn [{:keys [probes cost] :as estimate}]
            (assoc estimate :rank-cost
                   (if all-counted?
                     cost
                     (let [fanout (long
                                    (Math/ceil
                                      (* (double probes)
                                         (double c/magic-link-ratio))))]
                       (indexed-producer-cost probes fanout)))))
          estimates)))

(defn- late-branch-pattern-estimate
  [context cache idx clause]
  (when-let [{:keys [source entity attr value]}
             (late-branch-pattern-parts clause '$)]
    (let [source-db (get (:sources context) source)]
      (when (and source-db
                 (db/-searchable? source-db)
                 (not (and (qu/binding-var? entity)
                           (= entity value))))
        (let [entity-term (late-bound-term context entity)
              value-term  (late-bound-term context value)
              estimates
              (cond-> []
                (some? entity-term)
                (conj (cached-late-side-estimate
                        cache source source-db attr :entity entity-term))

                (some? value-term)
                (conj (cached-late-side-estimate
                        cache source source-db attr :value value-term)))
              estimates (->> estimates (filterv some?) comparable-estimates)]
          (when (seq estimates)
            (assoc (reduce cheaper-estimate estimates)
                   :idx idx :clause clause)))))))

(defn- ready-pattern-run
  [pending ready first-ready]
  (let [ready (set ready)]
    (loop [idx (long first-ready)
           result []]
      (if (and (< idx (long (count pending)))
               (contains? ready idx)
               (late-branch-pattern-parts (nth pending idx) '$))
        (recur (u/long-inc idx) (conj result idx))
        result))))

(defn- explain-late-or-join-choice
  [or-clause pending first-ready selected estimates]
  (when qplan/*explain*
    (vswap! qplan/*explain* update :late-or-join-branch-decisions
            (fnil conj [])
            {:or-join        or-clause
             :from-index     first-ready
             :selected-index (:idx selected)
             :from-clause    (nth pending first-ready)
             :selected-clause (:clause selected)
             :alternatives
             (mapv #(select-keys % [:idx :clause :side :probes :fanout
                                    :confidence :cost :rank-cost])
                   estimates)})))

(defn late-or-join-branch-selector
  [or-clause]
  (let [cache (volatile! {})]
    (fn [context pending ready]
      (let [first-ready (first ready)
            run         (ready-pattern-run pending ready first-ready)
            estimates   (->> run
                             (keep #(late-branch-pattern-estimate
                                      context cache % (nth pending %)))
                             vec
                             comparable-estimates)]
        (if (and (seq estimates)
                 (= first-ready (:idx (first estimates))))
          (let [selected (reduce cheaper-estimate estimates)]
            (when (not= first-ready (:idx selected))
              (explain-late-or-join-choice or-clause pending first-ready
                                           selected estimates))
            (:idx selected))
          first-ready)))))

(defn late-or-join-clause?
  [clause]
  (when (and (sequential? clause) (not (vector? clause)))
    (let [body (if (qu/source? (first clause)) (next clause) clause)]
      (u/sym-name-eqs (first body) "or-join"))))

(defn or-join-vars
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

(defn cheaper-late-producer
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

(defn isolated-union-context
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

(defn isolated-range-context
  [context {:keys [entity attr range source-db]}]
  (let [tuples (db/-init-tuples-list source-db attr [range] nil false)]
    (update context :rels conj (r/relation! {entity 0} tuples))))
