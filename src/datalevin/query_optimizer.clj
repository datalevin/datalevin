;;
;; Copyright (c) Huahai Yang, Nikita Prokopov. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns ^:no-doc datalevin.query-optimizer
  "Optimizer helpers extracted from datalevin.query."
  (:require
   [clojure.set :as set]
   [clojure.core.reducers :as rd]
   [clojure.walk :as w]
   [datalevin.constants :as c]
   [datalevin.datom :as dd]
   [datalevin.db :as db]
   [datalevin.interface :refer [av-size populated?]]
   [datalevin.join :as j]
   [datalevin.lmdb :as l]
   [datalevin.parser :as dp]
   [datalevin.query.optimizer.graph :as qog]
   [datalevin.pipe :as p]
   [datalevin.query.access :as qaccess]
   [datalevin.query.optimizer.range :as qor]
   [datalevin.query.plan :as qplan]
   [datalevin.query.resolve :as qresolve]
   [datalevin.query-util :as qu]
   [datalevin.relation :as r]
   [datalevin.util :as u :refer [cond+ raise conjv concatv map+]])
  (:import
   [java.util HashMap HashSet IdentityHashMap List]
   [java.util.concurrent ConcurrentHashMap]
   [datalevin.db DB]
   [datalevin.storage Store]
   [datalevin.utl DPKey LRUCache]
   [datalevin.parser And BindColl BindIgnore BindScalar BindTuple Constant
    DefaultSrc Function Or Variable Pattern Predicate Not RuleExpr]
   [org.eclipse.collections.impl.list.mutable FastList]))

(def ^:dynamic *plan-cache* (LRUCache. c/query-result-cache-size))

(def ^:private ^:const ^long collection-input-plugin-threshold
  128)

(def ^:private ^:const ^long collection-input-materialize-threshold
  100000)

(def ^:private ^:const ^double selective-anchor-dominance-margin
  2.0)

(defn- -sample [step db source]
  (qplan/step-sample step db source))

(defn- -execute [step db source]
  (qplan/step-execute step db source))

(defn- -type [step]
  (qplan/step-type step))

(defn- map->init-step [m]
  (qplan/map->InitStep m))

(defn- mk-merge-scan-step
  [index attrs-v vars in out cols strata seen-or-joins result sample]
  (qplan/->MergeScanStep index attrs-v vars in out cols strata seen-or-joins
                         result sample))

(defn- mk-link-step [type index attr var fidx in out cols strata seen-or-joins]
  (qplan/->LinkStep type index attr var fidx in out cols strata
                    seen-or-joins))

(defn- mk-hash-join-step
  [link link-e in out in-cols cols strata seen-or-joins tgt-steps in-size tgt-size]
  (qplan/->HashJoinStep link link-e in out in-cols cols strata seen-or-joins
                        tgt-steps in-size tgt-size))

(defn- mk-semi-join-step
  [in out in-cols cols strata seen-or-joins join-steps]
  (qplan/->SemiJoinStep in out in-cols cols strata seen-or-joins join-steps))

(defn- mk-or-join-step
  [clause bound-var bound-idx free-vars tgt tgt-attr sources rules in out cols strata seen-or-joins]
  (qplan/->OrJoinStep clause bound-var bound-idx free-vars tgt tgt-attr
                      sources rules in out cols strata seen-or-joins))

(defn- mk-not-join-step
  [clause vars sources rules in out cols strata seen-or-joins]
  (qplan/->NotJoinStep clause vars sources rules in out cols strata
                       seen-or-joins))

(defn- make-plan [steps cost size recency]
  (qplan/->Plan steps cost size recency))

(defn- plan-cache ^LRUCache []
  *plan-cache*)

(declare access-clause-deps access-clause-ready?
         build-plan estimate-hash-join-cost estimate-link-cost plan-not-joins)

;; optimizer

(defn- or-join-var?
  [clause s]
  (and (list? clause)
       (= 'or-join (first clause))
       (some #(= % s) (tree-seq sequential? seq (second clause)))))

(defn- find-var-symbols
  [parsed-q]
  (set (dp/find-vars (:qfind parsed-q))))

(defn- with-var-symbols
  [parsed-q]
  (set (map :symbol (:qwith parsed-q))))

(defn- scalar-coll-bind-var
  [qin]
  (when (instance? BindColl qin)
    (let [binding (:binding qin)]
      (when (instance? BindScalar binding)
        (:variable binding)))))

(defn- small-distinct-coll-values
  [value]
  (when (u/seqable? value)
    (let [values (vec (take (inc collection-input-plugin-threshold) value))]
      (when (and (seq values)
                 (<= (long (count values)) collection-input-plugin-threshold)
                 (apply distinct? values))
        values))))

(defn- variable-ref-count
  ^long [form sym]
  (let [n (volatile! 0)]
    (w/postwalk
      (fn [e]
        (when (and (instance? Variable e)
                   (= sym (:symbol e)))
          (vswap! n (fn [^long x] (unchecked-inc x))))
        e)
      form)
    (long @n)))

(defn- pattern-value-form-idx
  ^long [form]
  (+ (if (and (seq form) (qu/source? (first form))) 1 0) 2))

(defn- pattern-form
  [form]
  (if (and (seq form) (qu/source? (first form)))
    (subvec form 1)
    form))

(defn- value-pattern-for-var?
  [parsed-clause orig-clause sym]
  (and (instance? Pattern parsed-clause)
       (vector? orig-clause)
       (let [pattern (:pattern parsed-clause)
             idx     (pattern-value-form-idx orig-clause)]
         (and (<= 3 (count pattern))
              (< idx (count orig-clause))
              (= sym (nth orig-clause idx))
              (let [a (nth pattern 1)
                    v (nth pattern 2)]
                (and (instance? Constant a)
                     (keyword? (:value a))
                     (instance? Variable v)
                     (= sym (:symbol v))))))))

(defn- collection-input-candidate
  [parsed-q inputs input-idx qin]
  (when-let [v (scalar-coll-bind-var qin)]
    (let [sym (:symbol v)]
      (when-let [values (small-distinct-coll-values (nth inputs input-idx))]
        (when (and (= 1 (count values))
                   (not (contains? (find-var-symbols parsed-q) sym))
                   (not (contains? (with-var-symbols parsed-q) sym))
                   (not (some #(or-join-var? % sym) (:qorig-where parsed-q)))
                   (= 1 (variable-ref-count (:qwhere parsed-q) sym)))
          (let [matches (keep-indexed
                          (fn [clause-idx [parsed-clause orig-clause]]
                            (when (value-pattern-for-var?
                                    parsed-clause orig-clause sym)
                              clause-idx))
                          (map vector (:qwhere parsed-q)
                               (:qorig-where parsed-q)))]
            (when (= 1 (count matches))
              {:input-idx  input-idx
               :clause-idx (first matches)
               :values     values})))))))

(defn- expand-collection-input-pattern
  [orig-clause values]
  (let [idx      (pattern-value-form-idx orig-clause)
        branches (map #(assoc orig-clause idx %) values)]
    (if (= 1 (count branches))
      (first branches)
      (apply list 'or branches))))

(defn- not-join-clause?
  [clause]
  (and (sequential? clause)
       (not (vector? clause))
       (= 'not-join
          (if (qu/source? (first clause))
            (second clause)
            (first clause)))))

(defn- get-not-join-vars
  [clause]
  (let [clause (if (qu/source? (first clause)) (next clause) clause)
        [_ vars & _] clause]
    (into [] (filter qu/binding-var?) vars)))

(defn- get-not-join-source
  [clause]
  (if (qu/source? (first clause)) (first clause) '$))

(defn- clause-source-symbol
  [source]
  (if (instance? DefaultSrc source) '$ (:symbol source)))

(defn- not-join-optimizable?
  "Conservative check for planner-handled not-join.
   Easy cases only: explicit not-join form, non-empty join vars, pattern-only
   body, single source, and all join vars used in the body."
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

(defn- plugin-scalar-inputs
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

(defn- plugin-collection-inputs
  [parsed-q inputs]
  (loop [parsed-q parsed-q
         inputs   inputs]
    (if-let [{:keys [^long input-idx ^long clause-idx values]}
             (first (keep-indexed
                      #(collection-input-candidate parsed-q inputs %1 %2)
                      (:qin parsed-q)))]
      (let [qorig-where (assoc (:qorig-where parsed-q) clause-idx
                               (expand-collection-input-pattern
                                 (nth (:qorig-where parsed-q) clause-idx)
                                 values))]
        (recur (assoc parsed-q
                      :qorig-where qorig-where
                      :qwhere (dp/parse-where qorig-where)
                      :qin (u/remove-idxs #{input-idx} (:qin parsed-q)))
               (u/remove-idxs #{input-idx} inputs)))
      [parsed-q inputs])))

(defn- plugin-inputs*
  [parsed-q inputs]
  (let [[parsed-q inputs] (plugin-scalar-inputs parsed-q inputs)]
    (plugin-collection-inputs parsed-q inputs)))

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

(defn- pattern-var-symbol
  [x]
  (when (instance? Variable x)
    (:symbol x)))

(defn- form-var-symbols
  [form]
  (let [vars (volatile! #{})]
    (w/postwalk
      (fn [x]
        (cond
          (instance? Variable x) (vswap! vars conj (:symbol x))
          (qu/binding-var? x)     (vswap! vars conj x))
        x)
      form)
    @vars))

(defn- or-join-form-parts
  [form]
  (when (and (sequential? form) (not (vector? form)))
    (let [source? (qu/source? (first form))
          body    (if source? (next form) form)]
      (when (and (u/sym-name-eqs (first body) "or-join")
                 (vector? (second body))
                 (every? qu/binding-var? (second body)))
        {:source   (when source? (first form))
         :vars     (second body)
         :branches (nnext body)}))))

(defn- equality-target
  [value-sym clause]
  (when (and (vector? clause) (= 1 (count clause)))
    (let [call (first clause)]
      (when (and (sequential? call)
                 (= 3 (count call))
                 (u/sym-name-eqs (first call) "="))
        (let [[_ left right] call]
          (cond
            (and (= value-sym left)
                 (qu/binding-var? right)
                 (not= value-sym right))
            right

            (and (= value-sym right)
                 (qu/binding-var? left)
                 (not= value-sym left))
            left))))))

(defn- rewrite-equality-branch
  [pattern ^long value-idx value-sym branch]
  (let [and?    (and (sequential? branch)
                     (u/sym-name-eqs (first branch) "and"))
        clauses (if and? (vec (next branch)) [branch])
        matches (keep-indexed
                  (fn [idx clause]
                    (when-let [target (equality-target value-sym clause)]
                      [idx target]))
                  clauses)]
    (when (= 1 (count matches))
      (let [[idx target] (first matches)
            remaining    (u/remove-idxs #{idx} clauses)]
        (when (not-any? #(contains? (qu/collect-vars %) value-sym) remaining)
          (let [rewritten (assoc clauses idx (assoc pattern value-idx target))]
            {:target target
             :branch (if and?
                       (apply list 'and rewritten)
                       (first rewritten))}))))))

(defn- rel-bound-var?
  [context sym]
  (some #(contains? (:attrs %) sym) (:rels context)))

(defn- constant-constrained-pattern-var?
  [parsed-clause sym]
  (when (instance? Pattern parsed-clause)
    (let [pattern (:pattern parsed-clause)
          e       (first pattern)
          v       (nth pattern 2 nil)]
      (or (and (= sym (pattern-var-symbol e))
               (instance? Constant v))
          (and (= sym (pattern-var-symbol v))
               (instance? Constant e))))))

(defn- selectively-bound-var?
  [context sym excluded-idxs]
  (or (rel-bound-var? context sym)
      (some true?
            (keep-indexed
              (fn [idx clause]
                (when-not (contains? excluded-idxs idx)
                  (constant-constrained-pattern-var? clause sym)))
              (get-in context [:parsed-q :qwhere])))))

(defn- protected-query-vars
  [parsed-q]
  (set/union
    (find-var-symbols parsed-q)
    (with-var-symbols parsed-q)
    (form-var-symbols (:qin parsed-q))
    (form-var-symbols (:qhaving parsed-q))
    (form-var-symbols (:qorder parsed-q))))

(defn- rewrite-equality-or-join
  [context pattern-idx orig-pattern e-sym value-sym value-idx or-idx or-form]
  (when (not= pattern-idx or-idx)
    (when-let [{:keys [source vars branches]} (or-join-form-parts or-form)]
      (when (and (<= 2 (count branches))
                 (some #{value-sym} vars)
                 (not (some #{e-sym} vars)))
        (let [rewrites (mapv #(rewrite-equality-branch
                                orig-pattern value-idx value-sym %)
                             branches)]
          (when (every? some? rewrites)
            (let [targets       (into #{} (map :target) rewrites)
                  excluded-idxs #{pattern-idx or-idx}
                  other-clauses (u/remove-idxs
                                  excluded-idxs
                                  (get-in context [:parsed-q :qorig-where]))]
              (when (and (every? (set vars) targets)
                         (not-any? #(contains? (qu/collect-vars %) value-sym)
                                   other-clauses)
                         (every? #(selectively-bound-var?
                                    context % excluded-idxs)
                                 targets)
                         (not (selectively-bound-var?
                                context e-sym excluded-idxs)))
                (let [new-vars     (mapv #(if (= value-sym %) e-sym %) vars)
                      new-branches (mapv :branch rewrites)]
                  (if source
                    (apply list source 'or-join new-vars new-branches)
                    (apply list 'or-join new-vars new-branches)))))))))))

(defn- equality-pushdown-candidate
  [{:keys [parsed-q sources] :as context} ^long pattern-idx]
  (let [parsed-pattern (nth (:qwhere parsed-q) pattern-idx)
        orig-pattern   (nth (:qorig-where parsed-q) pattern-idx)]
    (when (and (instance? Pattern parsed-pattern)
               (vector? orig-pattern)
               (= 3 (count (:pattern parsed-pattern))))
      (let [pattern    (:pattern parsed-pattern)
            e-sym      (pattern-var-symbol (first pattern))
            attr       (second pattern)
            value-sym  (pattern-var-symbol (nth pattern 2))
            value-idx  (pattern-value-form-idx orig-pattern)
            source     (get sources
                            (clause-source-symbol (:source parsed-pattern)))]
        (when (and e-sym value-sym (not= e-sym value-sym)
                   (instance? Constant attr)
                   (keyword? (:value attr))
                   source
                   (db/-searchable? source)
                   (not (contains? (protected-query-vars parsed-q) value-sym)))
          (some
            (fn [[or-idx or-form]]
              (when-let [or-clause
                         (rewrite-equality-or-join
                           context pattern-idx orig-pattern e-sym value-sym
                           value-idx or-idx or-form)]
                {:pattern-idx pattern-idx
                 :or-idx      or-idx
                 :or-clause   or-clause}))
            (map-indexed vector (:qorig-where parsed-q))))))))

(defn push-down-equality-disjunctions
  "Push a filter-only pattern into simple equality or-join branches. This lets
   runtime lookup costing use a small set of bound AV values instead of first
   materializing an entire attribute relation. The rewrite is deliberately
   limited to selectively bound branch targets and unanchored entity vars."
  [{:keys [parsed-q] :as context}]
  (loop [parsed-q parsed-q]
    (let [context (assoc context :parsed-q parsed-q)]
      (if-let [{:keys [^long pattern-idx ^long or-idx or-clause]}
               (first (keep-indexed
                        (fn [idx _]
                          (equality-pushdown-candidate context idx))
                        (:qwhere parsed-q)))]
        (let [qorig-where (u/remove-idxs
                            #{pattern-idx}
                            (assoc (:qorig-where parsed-q)
                                   or-idx or-clause))]
          (recur (assoc parsed-q
                        :qorig-where qorig-where
                        :qwhere (dp/parse-where qorig-where))))
        (assoc context :parsed-q parsed-q)))))

(defn- rel-for-var
  [context sym]
  (when (qu/binding-var? sym)
    (some #(when (contains? (:attrs %) sym) %)
          (:rels context))))

(defn- relation-size
  ^long [rel]
  (if-some [^List tuples (:tuples rel)]
    (.size tuples)
    0))

(defn- small-bound-relation?
  [context sym]
  (when-let [rel (rel-for-var context sym)]
    (let [n (relation-size rel)]
      (and (pos? n)
           (<= n collection-input-materialize-threshold)))))

(defn- bound-relation?
  [context sym]
  (when-let [rel (rel-for-var context sym)]
    (pos? (relation-size rel))))

(defn- relation-has-unresolved-ref?
  [context source attr entity? sym]
  (when (and (qu/binding-var? sym)
             (or entity? (db/ref? source attr)))
    (when-let [rel (rel-for-var context sym)]
      (let [^List tuples (:tuples rel)
            idx          (int ((:attrs rel) sym))]
        (when tuples
          (loop [i 0]
            (when (< i (.size tuples))
              (let [^objects tuple (.get tuples i)
                    value          (aget tuple idx)]
                (if (or (qu/lookup-ref? value) (keyword? value))
                  true
                  (recur (unchecked-inc-int i)))))))))))

(defn- unique-attr?
  [source attr]
  (contains? #{:db.unique/identity :db.unique/value}
             (get-in (db/-schema source) [attr :db/unique])))

(defn- bound-unique-entity-var?
  [parsed-q sym]
  (or (contains? (find-var-symbols parsed-q) sym)
      (contains? (with-var-symbols parsed-q) sym)
      (some #(or-join-var? % sym) (:qorig-where parsed-q))))

(defn- protected-unique-constant-patterns
  [context]
  (let [{:keys [parsed-q sources]} context]
    (keep-indexed
      (fn [clause-idx [parsed-clause orig-clause]]
        (when (and (instance? Pattern parsed-clause)
                   (vector? orig-clause))
          (let [pattern (:pattern parsed-clause)
                e-sym   (pattern-var-symbol (first pattern))
                attr    (second pattern)
                value   (when (<= 3 (count pattern)) (nth pattern 2))]
            (when (and e-sym
                       (instance? Constant attr)
                       (keyword? (:value attr))
                       (instance? Constant value)
                       (bound-unique-entity-var? parsed-q e-sym))
              (when-let [source (get sources
                                     (clause-source-symbol
                                       (:source parsed-clause)))]
                (when (and (db/-searchable? source)
                           (unique-attr? source (:value attr)))
                  {:clause-idx clause-idx
                   :source     source
                   :pattern    (pattern-form orig-clause)
                   :entity-sym e-sym}))))))
      (map vector (:qwhere parsed-q)
           (:qorig-where parsed-q)))))

(defn- connected-vars
  [clause-vars seed]
  (loop [connected #{seed}]
    (let [expanded
          (reduce
            (fn [connected vars]
              (if (seq (set/intersection connected vars))
                (set/union connected vars)
                connected))
            connected clause-vars)]
      (if (= connected expanded)
        connected
        (recur expanded)))))

(defn- connected-unique-anchors
  [parsed-q candidates]
  (let [clause-vars (mapv qu/collect-vars (:qorig-where parsed-q))
        anchor-vars (into #{} (map :entity-sym) candidates)]
    (filterv
      (fn [{:keys [entity-sym]}]
        (<= 2 (count (set/intersection
                       anchor-vars
                       (connected-vars clause-vars entity-sym)))))
      candidates)))

(defn- materialize-protected-unique-anchors
  [context]
  (let [candidates (->> (protected-unique-constant-patterns context)
                        vec
                        (connected-unique-anchors (:parsed-q context)))]
    ;; A single unique literal is already an ideal selective planner root.
    ;; Eager binding is useful when multiple protected roots must constrain the
    ;; same join component, as in two-endpoint path queries.
    (if (empty? candidates)
      context
      (let [context (reduce
                      (fn [context {:keys [source pattern]}]
                        (let [rel (qresolve/lookup-pattern
                                    (assoc context :rels-bound-cache
                                           (volatile! {}))
                                    source pattern)]
                          (update context :rels qresolve/collapse-rels rel)))
                      context candidates)
            idxs    (into #{} (map :clause-idx) candidates)]
        (-> context
            (update-in [:parsed-q :qwhere] #(u/remove-idxs idxs %))
            (update-in [:parsed-q :qorig-where]
                       #(u/remove-idxs idxs %)))))))

(defn- materializable-bound-pattern
  [context bound?]
  (let [{:keys [parsed-q sources]} context
        qwhere (:qwhere parsed-q)]
    (first
      (keep-indexed
        (fn [clause-idx [parsed-clause orig-clause]]
          (when (and (instance? Pattern parsed-clause)
                     (vector? orig-clause))
            (let [pattern (:pattern parsed-clause)
                  e-sym   (pattern-var-symbol (first pattern))
                  v-sym   (when (<= 3 (count pattern))
                            (pattern-var-symbol (nth pattern 2)))
                  attr    (second pattern)
                  e-bound? (bound? context e-sym)
                  v-bound? (bound? context v-sym)]
              (when (and (instance? Constant attr)
                         (keyword? (:value attr)))
                (when-let [source (get sources
                                       (clause-source-symbol
                                         (:source parsed-clause)))]
                  (let [attr-value (:value attr)
                        input-bound?
                        (and (or e-bound? v-bound?)
                             (not (and e-bound? v-bound?)))
                        unresolved-ref?
                        (or (and e-bound?
                                 (relation-has-unresolved-ref?
                                   context source attr-value true e-sym))
                            (and v-bound?
                                 (relation-has-unresolved-ref?
                                   context source attr-value false v-sym)))]
                    (when (and (db/-searchable? source)
                               input-bound?
                               (not unresolved-ref?))
                      {:clause-idx clause-idx
                       :source     source
                       :pattern    (pattern-form orig-clause)})))))))
        (map vector qwhere
             (:qorig-where parsed-q))))))

(defn- materializable-input-bound-pattern
  [context]
  (materializable-bound-pattern context small-bound-relation?))

(defn- cost-materializable-bound-pattern
  [context]
  (materializable-bound-pattern context bound-relation?))

(defn materialize-input-bound-patterns
  "Materialize patterns constrained by small input-bound relations before
   planning. When a query has multiple unique constant anchors whose entity
   variables must remain bound, seed all anchors first so they constrain the
   same join component instead of leaving one as a late filter."
  [context]
  (loop [context (materialize-protected-unique-anchors context)]
    (if-let [{:keys [^long clause-idx source pattern]}
             (materializable-input-bound-pattern context)]
      (let [rel (qresolve/lookup-pattern
                  (assoc context :rels-bound-cache (volatile! {}))
                  source pattern)]
        (recur (-> context
                   (update :rels qresolve/collapse-rels rel)
                   (update-in [:parsed-q :qwhere]
                              #(u/remove-idxs #{clause-idx} %))
                   (update-in [:parsed-q :qorig-where]
                              #(u/remove-idxs #{clause-idx} %)))))
      context)))

(defn- materialized-output-cost
  ^double [^long tuple-count ^long width]
  (* (double tuple-count)
     (+ (double c/magic-cost-hash-join-output-tuple)
        (* (double c/magic-cost-hash-join-output-cell)
           (double width)))))

(defn- collapse-rels-with-cost
  [rels new-rel]
  (loop [rels          rels
         new-rel       new-rel
         new-rel-attrs (:attrs new-rel)
         cost          0.0
         acc           (transient [])]
    (if-some [rel (first rels)]
      (if (not-empty (qu/intersect-keys new-rel-attrs (:attrs rel)))
        (let [joined    (j/hash-join rel new-rel)
              join-cost (estimate-hash-join-cost
                          (relation-size rel)
                          (relation-size new-rel)
                          (relation-size joined)
                          (count (:attrs joined)))]
          (recur (next rels) joined (:attrs joined)
                 (+ cost (double join-cost)) acc))
        (recur (next rels) new-rel new-rel-attrs cost (conj! acc rel)))
      [(persistent! (conj! acc new-rel)) cost])))

(defn- bound-pattern-probe-count
  ^long [context pattern]
  (let [e-rel (rel-for-var context (first pattern))
        v-rel (rel-for-var context (nth pattern 2 nil))
        n     (long (cond
                      e-rel (relation-size e-rel)
                      v-rel (relation-size v-rel)
                      :else 1))]
    (max 1 n)))

(defn- relation-distinct-values
  [rel sym]
  (let [idx    (long ((:attrs rel) sym))
        tuples ^List (:tuples rel)
        values (HashSet.)]
    (dotimes [i (.size tuples)]
      (.add values (aget ^objects (.get tuples i) idx)))
    values))

(defn- resolved-bound-entity
  [source entity]
  (cond
    (integer? entity) entity
    (or (qu/lookup-ref? entity) (keyword? entity)) (db/entid source entity)
    :else nil))

(defn- resolved-bound-value
  [source attr value]
  (if (and (db/ref? source attr)
           (or (qu/lookup-ref? value) (keyword? value)))
    (db/entid source value)
    value))

(defn- capped-count-sum
  ^long [values count-value ^long cap]
  (let [value-count (.size ^HashSet values)
        sample-size (long (min value-count
                               collection-input-plugin-threshold))
        sampled     (take sample-size values)
        sample-sum
        (long
          (unreduced
            (reduce
              (fn [^long total value]
                (let [n     (long (count-value value))
                      total (if (< (- cap total) n)
                              (unchecked-inc cap)
                              (+ total n))]
                  (if (< cap total) (reduced total) total)))
              0 sampled)))]
    (cond
      (< cap sample-sum) (unchecked-inc cap)
      (= sample-size value-count) sample-sum
      (zero? sample-size) 0
      :else
      (long
        (min (unchecked-inc cap)
             (Math/ceil
               (* (double sample-sum)
                  (/ (double value-count) (double sample-size)))))))))

(defn- bounded-pattern-output-count
  ^long [context source pattern ^long cap]
  (let [pattern (qresolve/resolve-pattern-lookup-refs source pattern)
        [e attr v] pattern
        e-rel   (rel-for-var context e)
        v-rel   (rel-for-var context v)]
    (cond
      e-rel
      (let [entities (relation-distinct-values e-rel e)
            entity-count (.size ^HashSet entities)
            value-var? (qu/binding-var? v)
            existence? (or (= v '_) (qu/placeholder? v))
            cardinality-many?
            (= :db.cardinality/many
               (get-in (db/-schema source) [attr :db/cardinality]))]
        ;; A cardinality-one attribute, an existence lookup, or a concrete
        ;; value can emit at most one row per bound entity. Apart from being
        ;; cheaper, this avoids treating stale per-entity count metadata as an
        ;; expansion signal.
        (if (or existence? (not value-var?) (not cardinality-many?))
          (long entity-count)
          (capped-count-sum
            entities
            (fn [entity]
              (if-let [entity (resolved-bound-entity source entity)]
                (db/-count source [entity attr nil])
                0))
            cap)))

      v-rel
      (let [values (relation-distinct-values v-rel v)]
        (capped-count-sum
          values
          (fn [value]
            (let [value (resolved-bound-value source attr value)]
              (if (qu/binding-var? e)
                (av-size (.-store ^DB source) attr value)
                (if-let [entity (resolved-bound-entity source e)]
                  (db/-count source [entity attr value])
                  0))))
          cap))

      :else 0)))

(defn- materialize-pattern-with-cost
  [context source pattern ^long probe-count]
  (let [rel             (qresolve/lookup-pattern
                          (assoc context :rels-bound-cache (volatile! {}))
                          source pattern)
        tuple-count     (relation-size rel)
        lookup-cost     (estimate-link-cost probe-count tuple-count)
        output-cost     (materialized-output-cost
                          tuple-count (count (:attrs rel)))
        [rels join-cost] (collapse-rels-with-cost (:rels context) rel)]
    {:context (assoc context :rels rels)
     :cost    (+ (double lookup-cost) (double output-cost) (double join-cost))
     :stage   {:pattern      pattern
               :probes       probe-count
               :lookup-rows  tuple-count
               :lookup-cost  lookup-cost
               :output-cost  output-cost
               :join-cost    join-cost}}))

(defn- remove-materialized-clause
  [context ^long clause-idx]
  (-> context
      (update-in [:parsed-q :qwhere]
                 #(u/remove-idxs #{clause-idx} %))
      (update-in [:parsed-q :qorig-where]
                 #(u/remove-idxs #{clause-idx} %))))

(defn- projected-bound-lookup-cost
  ^double [source attr ^long probe-count ^long output-count]
  (if (= 1 probe-count)
    (double (estimate-link-cost probe-count output-count))
    (let [scan-count (long (db/-count source [nil attr nil]))
          multi-cost (+ (* (double probe-count)
                           (double c/magic-cost-link-probe))
                        (* (double output-count)
                           (double c/magic-cost-link-retrieval))
                        (* (+ (double probe-count) (double output-count))
                           (double c/magic-cost-hash-join)))
          full-cost  (+ (* (double scan-count)
                           (double c/magic-cost-init-scan-e))
                        (* (+ (double probe-count) (double scan-count))
                           (double c/magic-cost-hash-join)))]
      (min multi-cost full-cost))))

(defn- projected-pattern-materialization-cost
  ^double [source attr ^long probe-count ^long output-count]
  (+ (projected-bound-lookup-cost
       source attr probe-count output-count)
     ;; At least the newly produced entity/value column must be allocated.
     (materialized-output-cost output-count 1)))

(defn- affordable-output-cap
  "Return the largest one-column output that could fit in the remaining cost
   budget. This cap only bounds cardinality probing; the complete lookup and
   join estimate below makes the actual decision."
  ^long [^double budget]
  (let [per-row (+ (double c/magic-cost-hash-join-output-tuple)
                   (double c/magic-cost-hash-join-output-cell))]
    (if (or (not (pos? budget)) (not (pos? per-row)))
      0
      (long (min (double (dec Long/MAX_VALUE))
                 (Math/floor (/ budget per-row)))))))

(defn- abstract-bound-groups
  [context]
  (into []
        (keep (fn [rel]
                (let [rows (relation-size rel)]
                  (when (and (pos? rows) (seq (:attrs rel)))
                    {:vars   (set (keys (:attrs rel)))
                     :rows   rows
                     :exact? true}))))
        (:rels context)))

(defn- abstract-group-index
  [groups sym]
  (when (qu/binding-var? sym)
    (first
      (keep-indexed
        (fn [idx group]
          (when (contains? (:vars group) sym) idx))
        groups))))

(defn- abstract-materializable-bound-pattern
  [{:keys [parsed-q sources]} groups consumed]
  (first
    (keep-indexed
      (fn [clause-idx [parsed-clause orig-clause]]
        (when (and (not (contains? consumed clause-idx))
                   (instance? Pattern parsed-clause)
                   (vector? orig-clause))
          (let [parsed-pattern (:pattern parsed-clause)
                e-sym          (pattern-var-symbol (first parsed-pattern))
                v-sym          (when (<= 3 (count parsed-pattern))
                                 (pattern-var-symbol
                                   (nth parsed-pattern 2)))
                attr           (second parsed-pattern)
                e-group        (abstract-group-index groups e-sym)
                v-group        (abstract-group-index groups v-sym)]
            (when (and (instance? Constant attr)
                       (keyword? (:value attr))
                       (not= (some? e-group) (some? v-group)))
              (when-let [source (get sources
                                     (clause-source-symbol
                                       (:source parsed-clause)))]
                (when (db/-searchable? source)
                  {:clause-idx clause-idx
                   :source     source
                   :pattern    (qresolve/resolve-pattern-lookup-refs
                                 source (pattern-form orig-clause))
                   :group-idx  (long (or e-group v-group))
                   :entity-bound? (some? e-group)}))))))
      (map vector (:qwhere parsed-q)
           (:qorig-where parsed-q)))))

(defn- cap-output-count
  ^long [^long count ^long cap]
  (min count (unchecked-inc cap)))

(defn- abstract-pattern-output-count
  [context source pattern group entity-bound? cap]
  (let [cap (long cap)]
    (if (:exact? group)
      (bounded-pattern-output-count context source pattern cap)
      (let [[_ attr value] pattern
            input-rows (long (:rows group))
            scan-rows  (long (db/-count source [nil attr nil]))
            cardinality-many?
            (= :db.cardinality/many
               (get-in (db/-schema source) [attr :db/cardinality]))
            value-var? (qu/binding-var? value)
            existence? (or (= value '_) (qu/placeholder? value))
            estimated
            (if (and entity-bound? (or existence?
                                       (not value-var?)
                                       (not cardinality-many?)))
              (min input-rows scan-rows)
              (long
                (min (double scan-rows)
                     (Math/ceil (* (double input-rows)
                                   (double c/magic-link-ratio))))))]
        (cap-output-count estimated cap)))))

(defn- project-bound-patterns-with-cost
  "Project a complete propagation chain without reading its output tuples.
   This prevents a cheap first lookup from triggering an expensive speculative
   stage when a later, already-predictable stage exhausts the cost budget."
  [context ^double budget]
  (loop [groups   (abstract-bound-groups context)
         consumed #{}
         cost     0.0
         stages   []]
    (if-let [{:keys [^long clause-idx source pattern ^long group-idx
                     entity-bound?]}
             (abstract-materializable-bound-pattern
               context groups consumed)]
      (let [group          (nth groups group-idx)
            probe-count    (long (:rows group))
            remaining-cost (max 0.0 (- budget cost))
            output-count   (long
                             (abstract-pattern-output-count
                               context source pattern group entity-bound?
                               (affordable-output-cap remaining-cost)))
            projected-cost (projected-pattern-materialization-cost
                             source (second pattern)
                             probe-count output-count)
            new-cost       (+ cost projected-cost)
            stage          {:pattern pattern
                            :probes probe-count
                            :projected-rows output-count
                            :projected-cost projected-cost}]
        (if (<= budget new-cost)
          {:eligible? false
           :cost cost
           :stages (conj stages stage)
           :guardrail {:phase :propagation-preflight
                       :pattern pattern
                       :projected-rows output-count
                       :projected-cost projected-cost
                       :accumulated-cost cost
                       :budget budget}}
          (let [pattern-vars (into #{}
                                   (filter qu/binding-var?)
                                   [(first pattern) (nth pattern 2 nil)])
                next-group  {:vars   (set/union (:vars group) pattern-vars)
                             :rows   output-count
                             :exact? false}]
            (recur (assoc groups group-idx next-group)
                   (conj consumed clause-idx)
                   new-cost
                   (conj stages stage)))))
      {:eligible? true :cost cost :stages stages})))

(defn- materialize-bound-patterns-with-cost
  [context ^double budget]
  (loop [context context
         cost    0.0
         stages  []]
    (if-let [{:keys [^long clause-idx source pattern]}
             (cost-materializable-bound-pattern context)]
      (let [remaining-cost (max 0.0 (- budget cost))
            output-count (bounded-pattern-output-count
                           context source pattern
                           (affordable-output-cap remaining-cost))
            probe-count (bound-pattern-probe-count context pattern)
            projected-cost (projected-pattern-materialization-cost
                             source (second pattern)
                             probe-count output-count)]
        (if (<= budget (+ cost projected-cost))
          {:context context
           :cost cost
           :stages stages
           :eligible? false
           :guardrail {:pattern pattern
                       :projected-rows output-count
                       :projected-cost projected-cost
                       :accumulated-cost cost
                       :budget budget}}
          (let [result   (materialize-pattern-with-cost
                           context source pattern probe-count)
                charged-cost (max projected-cost (double (:cost result)))
                new-cost (+ cost charged-cost)]
            (if (<= budget new-cost)
              {:context (:context result)
               :cost new-cost
               :stages (conj stages (:stage result))
               :eligible? false
               :guardrail {:pattern pattern
                           :actual-cost new-cost
                           :budget budget}}
              (recur (remove-materialized-clause
                       (:context result) clause-idx)
                     new-cost
                     (conj stages (:stage result)))))))
      {:context context :cost cost :stages stages :eligible? true})))

(defn- var-symbol
  [v]
  (when (instance? Variable v)
    (:symbol v)))

(defn- collect-var-usage
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
  ([parsed-q]
   (unused-var-replacements parsed-q nil))
  ([parsed-q bound-vars]
  (let [find-vars (set (dp/find-vars (:qfind parsed-q)))
        with-vars (set (map :symbol (or (:qwith parsed-q) [])))
        in-vars   (set (map :symbol (dp/collect-vars-distinct (:qin parsed-q))))
        used      (set/union find-vars with-vars in-vars (set bound-vars))
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
          counts))))

(defn- replace-unused-vars-form
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
  (let [rel-vars     (mapcat (comp keys :attrs) (:rels context))
        replacements (unused-var-replacements parsed-q rel-vars)]
    (if (empty? replacements)
      context
      (let [qorig-where  (mapv #(replace-unused-vars-form % replacements)
                               (:qorig-where parsed-q))
            qwhere       (dp/parse-where qorig-where)]
        (assoc context :parsed-q
               (assoc parsed-q :qorig-where qorig-where :qwhere qwhere))))))

(def combine-ranges qor/combine-ranges)
(def flip-ranges qor/flip-ranges)
(def intersect-ranges qor/intersect-ranges)
(def ^:private add-pred qor/add-pred)
(def ^:private range->inequality qor/range->inequality)

(defn- activate-var-pred
  [var clause]
  (qor/activate-var-pred {:make-call qresolve/make-call
                          :resolve-pred qresolve/resolve-pred}
                         var clause))

(defn build-graph
  [context]
  (qog/build-graph
    {:resolve-pattern-lookup-refs qresolve/resolve-pattern-lookup-refs
     :make-call qresolve/make-call
     :resolve-pred qresolve/resolve-pred
     :map->Clause qresolve/map->Clause
     :map->Node qplan/map->Node
     :link qplan/->Link
     :or-join-link qresolve/->OrJoinLink}
    context))

(defn- estimate-round ^long [x]
  (let [v (Math/ceil (double x))]
    (if (>= v (double Long/MAX_VALUE))
      Long/MAX_VALUE
      (long v))))

(defn- attr-var [{:keys [var]}] (or var '_))

(defn- nillify [v] (if (or (identical? v c/v0) (identical? v c/vmax)) nil v))

(defn- range->start-end [[[_ lv] [_ hv]]] [(nillify lv) (nillify hv)])

(defn- range-count
  [db attr ranges ^long cap]
  (if (identical? ranges :empty-range)
    0
    (unreduced
      (reduce
        (fn [^long sum range]
          (let [s (+ sum (let [[lv hv] (range->start-end range)]
                           ^long (db/-index-range-size db attr lv hv)))]
            (if (< s cap) s (reduced cap))))
        0 ranges))))

(def ^:private verified-non-empty-size
  (inc (long c/init-exec-size-threshold)))

(defn- zero-count-clause-size
  "Fast clause counts come from the counted-index metadata, which has been
  observed to report 0 for an attribute that still holds datoms (issue #371).
  A zero count is a correctness decision, not an estimate -- it short-circuits
  the whole query to an empty result -- so it must be confirmed against the
  actual index before being trusted. Returns 0 only when the clause is truly
  empty; otherwise returns a conservative non-empty size that keeps planning on
  the sampled path."
  ^long [^DB db e {:keys [attr val range]}]
  (let [store (.-store db)]
    (if (and (some-> ((db/-schema db) attr) :db/aid)
             (cond
               (int? e)
               (populated? store :eav (dd/datom e attr c/v0)
                           (dd/datom e attr c/vmax))

               (some? val)
               (populated? store :ave (dd/datom c/e0 attr val)
                           (dd/datom c/emax attr val))

               range
               (when-not (identical? range :empty-range)
                 (some (fn [r]
                         (let [[lv hv] (range->start-end r)]
                           (populated? store :ave
                                       (dd/datom c/e0 attr lv)
                                       (dd/datom c/emax attr hv))))
                       range))

               :else
               (populated? store :ave (dd/datom c/e0 attr nil)
                           (dd/datom c/emax attr nil))))
      verified-non-empty-size
      0)))

(defn ^:redef fast-clause-count
  "Fast datom count for a clause, backed by the counted-index metadata.
  May be inaccurate; a zero result must be verified by
  `zero-count-clause-size` before it short-circuits planning (issue #371).
  ^:redef so tests can simulate counted-index metadata drift."
  ^long [^DB db e {:keys [attr val range]} ^long mcount]
  (let [store (.-store db)]
    (cond
      (int? e)    (db/-count db [e attr nil] mcount)
      (some? val) (av-size store attr val)
      range       (range-count db attr range mcount)
      :else       (db/-count db [nil attr nil] mcount))))

(defn- adjusted-scan-ratio
  ^double
  [^long input-size ^long output-size]
  (if (and (pos? input-size) (pos? output-size))
    (/ (double output-size) input-size)
    (double c/magic-scan-ratio)))

(defn- access-join-candidate
  [db access-source covered clause-idx clause]
  (when (and (not (contains? covered clause))
             (instance? Pattern clause)
             (= (or (qaccess/source-symbol access-source) '$)
                (clause-source-symbol (:source ^Pattern clause))))
    (let [pattern (:pattern ^Pattern clause)]
      (when (= 3 (count pattern))
        (let [e    (nth pattern 0)
              a    (nth pattern 1)
              v    (nth pattern 2)
              attr (when (instance? Constant a) (:value ^Constant a))
              evar (when (instance? Variable e) (:symbol ^Variable e))
              vvar (when (instance? Variable v) (:symbol ^Variable v))]
          (when (and evar (keyword? attr))
            (let [val  (when (instance? Constant v) (:value ^Constant v))
                  rows (fast-clause-count db nil {:attr attr :val val}
                                          Long/MAX_VALUE)]
              {:clause-idx clause-idx
               :clause     clause
               :attr       attr
               :entity-var evar
               :value-var  vvar
               :cols       (cond-> [evar] vvar (conj vvar))
               :vars       (cond-> #{evar} vvar (conj vvar))
               :rows       rows})))))))

(defn- eligible-access-join
  [bound {:keys [entity-var value-var] :as join}]
  (let [entity-bound? (contains? bound entity-var)
        value-bound?  (and value-var (contains? bound value-var))]
    (when (or entity-bound? value-bound?)
      join)))

(defn- access-join-candidates
  [db parsed-q {:keys [expr]}]
  (into []
        (keep-indexed
          #(access-join-candidate
             db (:source expr) (:covers expr) %1 %2))
        (:qwhere parsed-q)))

(defn- access-join-from-bound
  [bound {:keys [cols vars rows] :as candidate}]
  (when-let [join (eligible-access-join bound candidate)]
    (-> join
        (dissoc :entity-var :value-var :vars :rows)
        (assoc :requires (set/intersection bound vars)
               :produces (set/difference vars bound)
               :produces-cols (into [] (remove bound) cols)
               :estimate {:rows rows
                          :confidence :medium}))))

(defn- order-access-joins
  [db parsed-q {:keys [expr] :as plan}]
  (let [initial-bound (into (:requires expr)
                            (filter qu/binding-var?)
                            (:cols expr))
        candidates    (access-join-candidates db parsed-q plan)]
    (loop [bound     initial-bound
           candidates candidates
           joins      []]
      (let [eligible (keep #(eligible-access-join bound %) candidates)]
        (if (seq eligible)
          (let [{:keys [clause-idx vars] :as chosen}
                (apply min-key :rows eligible)
                join (access-join-from-bound bound chosen)]
            (recur (into bound vars)
                   (filterv #(not= clause-idx (:clause-idx %)) candidates)
                   (conj joins join)))
          joins)))))

(defn- input-bound-vars
  [parsed-q inputs]
  (qresolve/bound-vars
    (qresolve/resolve-ins (qplan/make-context parsed-q false) inputs)))

(defn- order-correlated-bindings
  [db parsed-q inputs {:keys [expr]}]
  (let [required   (set (:requires expr))
        input-bound (set (input-bound-vars parsed-q inputs))
        candidates
        (into []
              (keep-indexed
                (fn [i clause]
                  (let [orig-clause (nth (:qorig-where parsed-q) i)]
                    (when-not
                      (or (contains? (:covers expr) clause)
                          (contains? (:covered-originals expr) orig-clause))
                      (let [pattern
                            (access-join-candidate
                              db (:source expr) #{} i clause)
                            deps (access-clause-deps orig-clause)
                            vars (set/union
                                   (set (:requires deps))
                                   (set (:requires-any deps))
                                   (set (:provides deps)))]
                        (assoc deps
                               :clause-idx i
                               :clause clause
                               :orig-clause orig-clause
                               :vars vars
                               :rows (long
                                       (or (:rows pattern)
                                           Long/MAX_VALUE))))))))
              (:qwhere parsed-q))
        needed
        (loop [needed required]
          (let [needed'
                (reduce
                  (fn [needed {:keys [requires provides]}]
                    (if (seq (set/intersection needed (set provides)))
                      (into needed requires)
                      needed))
                  needed candidates)]
            (if (= needed needed')
              needed
              (recur needed'))))]
    (loop [bound      input-bound
           candidates candidates
           outer      []]
      (if (qaccess/access-ready? expr bound)
        {:outer-joins outer
         :outer-cols  (vec
                        (sort-by str
                                 (set/union required
                                            (into #{} (mapcat :vars) outer))))}
        (let [eligible
              (filter
                (fn [{:keys [provides] :as candidate}]
                  (and (access-clause-ready? bound candidate)
                       (seq (set/intersection
                              (set provides)
                              (set/difference needed bound)))))
                candidates)]
          (when (seq eligible)
            (let [chosen (apply min-key :rows eligible)]
              (recur (into bound (:provides chosen))
                     (filterv #(not= (:clause-idx chosen)
                                     (:clause-idx %))
                              candidates)
                     (conj outer chosen)))))))))

(defn schedule-correlated-access
  "Schedule a correlated access source after a bounded outer subset has
   produced every variable in AccessExpr.requires. Returns nil when no safe
   outer subset can provide the requirements."
  [db parsed-q inputs
   {:keys [expr path demand bounds work access-source] :as plan}]
  (when (satisfies? qaccess/ICorrelatedAccessMethod
                    (:implementation path))
    (when-let [{:keys [outer-cols] :as schedule}
               (order-correlated-bindings db parsed-q inputs plan)]
      (let [finite-rows (keep (fn [{:keys [rows]}]
                                (when (< (long rows) Long/MAX_VALUE)
                                  (long rows)))
                              (:outer-joins schedule))
            minimum    (long (or (when (seq finite-rows)
                                   (apply min finite-rows))
                                 1))
            outer-rows (max 1 minimum)
            outer-cost (* (double outer-rows)
                          (double (max 1
                                       (count (:outer-joins schedule)))))]
        (merge plan schedule
               {:correlated? true
                :outer-estimate {:rows outer-rows
                                 :cost outer-cost
                                 :confidence :medium}
                :step (qplan/access-step
                        expr path demand bounds work outer-cols
                        access-source)})))))

(defn- access-join-pattern
  [parsed-q clause-idx]
  (let [pattern (nth (:qorig-where parsed-q) clause-idx)]
    (if (and (vector? pattern) (qu/source? (first pattern)))
      (subvec pattern 1)
      pattern)))

(defn- sample-relation-projection-count
  [find-vars {:keys [attrs tuples] :as relation}]
  (let [projected (filterv #(contains? attrs %) find-vars)]
    (cond
      (zero? (relation-size relation)) 0
      (empty? projected)              1
      :else
      (count
        (into #{}
              (map (fn [^objects tuple]
                     (mapv #(aget tuple (long (attrs %))) projected)))
              tuples)))))

(defn- sampled-access-output
  [parsed-q relations]
  (let [find-vars (dp/find-vars (:qfind parsed-q))]
    (reduce
      (fn [n relation]
        (estimate-round
          (* (double n)
             (double
               (sample-relation-projection-count find-vars relation)))))
      1 relations)))

(defn- sample-context-size
  [relations]
  (reduce
    (fn [n relation]
      (estimate-round
        (* (double n) (double (relation-size relation)))))
    1 relations))

(defn- access-sample-relation
  [context access-var]
  (some #(when (contains? (:attrs %) access-var) %)
        (:rels context)))

(defn- access-clause-vars
  [form]
  (into #{} (filter qu/binding-var?) (qu/collect-vars form)))

(defn- access-clause-deps
  [clause]
  (let [clause (if (and (sequential? clause)
                        (qu/source? (first clause)))
                 (next clause)
                 clause)
        head   (when (sequential? clause) (first clause))]
    (cond
      (and (sequential? head) (= 1 (count clause)))
      {:requires (access-clause-vars head)}

      (and (sequential? head) (= 2 (count clause)))
      {:requires (access-clause-vars head)
       :provides (access-clause-vars (second clause))}

      (= 'not head)
      {:requires-any (access-clause-vars (next clause))}

      (= 'not-join head)
      {:requires (access-clause-vars (second clause))}

      (= 'or-join head)
      (let [vars-form (second clause)
            req-form  (when (and (sequential? vars-form)
                                 (sequential? (first vars-form)))
                        (first vars-form))]
        {:requires (access-clause-vars req-form)
         :provides (access-clause-vars vars-form)})

      :else
      {:provides (access-clause-vars clause)})))

(defn- access-clause-ready?
  [bound {:keys [requires requires-any]}]
  (and (every? bound requires)
       (or (empty? requires-any)
           (some bound requires-any))))

(defn- order-access-residuals
  [bound entries]
  (let [entries (mapv #(merge % (access-clause-deps (:orig-clause %)))
                      entries)]
    (loop [bound bound
           todo  entries
           acc   []]
      (if (empty? todo)
        acc
        (if-let [idx (first
                       (keep-indexed
                         (fn [i entry]
                           (when (access-clause-ready? bound entry) i))
                         todo))]
          (let [entry (nth todo idx)]
            (recur (into bound (:provides entry))
                   (u/vec-remove todo idx)
                   (conj acc entry)))
          ;; Preserve query order for clauses whose dependencies require
          ;; runtime rule/or semantics that this lightweight sorter cannot
          ;; prove. Resolution will still enforce binding correctness.
          (into acc todo))))))

(defn- residual-operation
  [clause]
  (cond
    (instance? Pattern clause)   :indexed-join
    (instance? Predicate clause) :predicate
    (instance? Function clause)  :function
    (instance? Not clause)       :not
    (instance? Or clause)        :or
    (instance? RuleExpr clause)  :rule
    :else                        :residual))

(defn- sample-safe-residual?
  [clause]
  ;; Query-input functions can be effectful and have no estimator contract.
  ;; Account for their evaluation cost, but leave their selectivity unknown
  ;; until adaptive execution observes it.
  (not (and (or (instance? Predicate clause)
                (instance? Function clause))
            (instance? Variable (:fn clause)))))

(defn- access-residual-entries
  [parsed-q step joins]
  (let [covered-clauses (or (get-in step [:expr :covers]) #{})
        covered-originals
        (or (get-in step [:expr :covered-originals]) #{})
        joined (into #{} (map :clause-idx) joins)]
    (into []
          (keep-indexed
            (fn [i [clause orig-clause]]
              (when-not (or (joined i)
                            (contains? covered-clauses clause)
                            (contains? covered-originals orig-clause))
                {:clause-idx i
                 :clause clause
                 :orig-clause orig-clause})))
          (map vector (:qwhere parsed-q) (:qorig-where parsed-q)))))

(defn- sample-access-residuals
  [parsed-q context step joins stages]
  (let [bound   (qresolve/bound-vars context)
        entries (order-access-residuals
                  bound (access-residual-entries parsed-q step joins))]
    (reduce
      (fn [[context stages] {:keys [clause-idx clause orig-clause]}]
        (let [before   (sample-context-size (:rels context))
              safe?    (sample-safe-residual? clause)
              context' (if safe?
                         (qresolve/resolve-clause context orig-clause)
                         context)
              after    (sample-context-size (:rels context'))]
          [context'
           (conj stages
                 {:clause-idx clause-idx
                  :operation  (if safe?
                                (residual-operation clause)
                                :opaque-predicate)
                  :modeled?   safe?
                  :input      before
                  :output     after})]))
      [context stages]
      entries)))

(defn- sample-access-joins
  [db parsed-q inputs step planned-joins sample-size]
  (if (pos? (long sample-size))
    (let [sample-work (assoc (:work step)
                             :sample-size sample-size
                             :batch-size sample-size)
          sample-step (assoc step :work sample-work)
          sample-batch  (qplan/access-sample-batch sample-step db)
          ^List sample  (:tuples sample-batch)
          initial       (r/relation! (qplan/cols->attrs (:cols step)) sample)
          context       (-> (qplan/make-context parsed-q false)
                            (qresolve/resolve-ins inputs)
                            (update :rels qresolve/collapse-rels initial))
          access-var    (first (:cols step))]
      (loop [context context
             joins   planned-joins
             stages  []]
        (let [relation (access-sample-relation context access-var)
              before   (relation-size relation)]
          (if (or (zero? before) (empty? joins))
            (let [[context stages]
                  (sample-access-residuals
                    parsed-q context step planned-joins stages)]
              {:sample        sample
               :sample-batch  sample-batch
               :sample-rows   (.size sample)
               :sample-output (sampled-access-output parsed-q (:rels context))
               :stages        stages})
            (let [{:keys [clause-idx attr]} (first joins)
                  pattern  (access-join-pattern parsed-q clause-idx)
                  new-rel  (qresolve/lookup-pattern
                             (assoc context :rels-bound-cache (volatile! {}))
                             db pattern)
                  rels     (qresolve/collapse-rels (:rels context) new-rel)
                  relation (access-sample-relation
                             (assoc context :rels rels) access-var)
                  after    (relation-size relation)]
              (recur (assoc context :rels rels)
                     (next joins)
                     (conj stages
                           {:clause-idx clause-idx
                            :attr       attr
                            :operation  :indexed-join
                            :input      before
                            :output     after})))))))
    {:sample        (FastList.)
     :sample-batch  nil
     :sample-rows   0
     :sample-output 0
     :stages        []}))

(defn- adjusted-access-yield
  ^double
  [^long sample-rows ^long sample-output]
  (if (zero? sample-rows)
    0.0
    (adjusted-scan-ratio sample-rows sample-output)))

(defn- unsampled-access-yield
  ^double
  [parsed-q step joins ^long range-rows]
  (let [^double join-yield
        (reduce
          (fn [^double yield join]
            (let [rows  (long (or (get-in join [:estimate :rows]) 0))
                  ratio (if (pos? range-rows)
                          (min 1.0
                               (adjusted-scan-ratio range-rows rows))
                          0.0)]
              (* yield ratio)))
          1.0 joins)
        residual-count
        (count (access-residual-entries parsed-q step joins))]
    (* join-yield
       (Math/pow (double c/magic-scan-ratio)
                 (double residual-count)))))

(defn- estimate-row-evaluation-cost
  ^long
  [^long input-size]
  (estimate-round
    (* (double input-size) (double c/magic-cost-pred))))

(defn- access-candidate-budget
  ^long
  [^long required-count ^long range-rows ^double yield]
  (if (or (zero? range-rows) (zero? yield))
    0
    (min range-rows
         (max 1 (long
                  (estimate-round (/ (double required-count) yield)))))))

(defn- proportional-scan-rows
  ^long
  [^long candidate-rows ^long range-rows ^long scan-rows]
  (cond
    (or (zero? candidate-rows) (zero? range-rows) (zero? scan-rows)) 0
    (<= range-rows candidate-rows) scan-rows
    :else
    (min scan-rows
         (max 1
              (long
                (Math/ceil
                  (* (double scan-rows)
                     (/ (double candidate-rows)
                        (double range-rows)))))))))

(defn- adjusted-access-cost
  [estimate scan-rows input-rows stages]
  (let [base-cost (if (pos? (long scan-rows))
                    (+ (double (:startup estimate))
                       (* (double scan-rows)
                          (double (:per-row estimate))))
                    0.0)]
    (first
      (reduce
        (fn [[cost input-size] {:keys [operation input output]}]
          (let [ratio       (adjusted-scan-ratio (long input) (long output))
                output-size (estimate-round (* (double input-size) ratio))
                stage-cost  (if (#{:predicate :function :opaque-predicate}
                                  operation)
                              (estimate-row-evaluation-cost input-size)
                              (estimate-link-cost input-size output-size))]
            [(+ (double cost)
                (double stage-cost))
             output-size]))
        [base-cost input-rows]
        stages))))

(defn- adjust-access-plan
  [db parsed-q inputs {:keys [step path demand work estimate] :as plan}]
  (if step
    (let [range-rows      (qaccess/estimate-range-rows estimate)
          scan-rows       (qaccess/estimate-scan-rows estimate)
          output-rows     (qaccess/estimate-output-rows estimate)
          adaptive?       (qaccess/adaptive-demand? path demand)
          sample?         (and (qaccess/planning-sample? path)
                               (pos? range-rows))
          sample-size     (if sample?
                            (long (min range-rows
                                       (long c/init-exec-size-threshold)))
                            0)
          {:keys [sample sample-batch sample-rows sample-output stages]}
          (if sample?
            (sample-access-joins
              db parsed-q inputs step (:joins plan) sample-size)
            {:sample        (FastList.)
             :sample-batch  nil
             :sample-rows   0
             :sample-output 0
             :stages        []})
          sample-rows     (long sample-rows)
          sample-output   (long sample-output)
          yield           (if (pos? sample-rows)
                            (adjusted-access-yield sample-rows sample-output)
                            (double
                              (if (contains? estimate :yield)
                                (:yield estimate)
                                (unsampled-access-yield
                                  parsed-q step (:joins plan) range-rows))))
          heuristic-yield?
          (and (zero? sample-rows)
               (not (contains? estimate :yield))
               (< yield 1.0))
          complete?       (nil? (:required-count demand))
          initial-candidate-budget
          (if complete?
            range-rows
            (access-candidate-budget (long (:required-count demand))
                                     range-rows yield))
          candidate-budget
          (long
            (if (and adaptive? heuristic-yield?)
              (min range-rows
                   (* 2 (long initial-candidate-budget)))
              initial-candidate-budget))
          point-output-rows
          (long (if adaptive?
                  (min output-rows candidate-budget)
                  output-rows))
          point-scan-rows
          (long (if adaptive?
                  (proportional-scan-rows
                    candidate-budget range-rows scan-rows)
                  scan-rows))
          reusable?       (and sample?
                               (qaccess/reusable-sample? path)
                               adaptive?
                               (some? sample-batch))
          reused-rows     (long (if reusable? sample-rows 0))
          candidate-remaining (- candidate-budget reused-rows)
          remaining-candidates
          (if (pos? candidate-remaining) candidate-remaining 0)
          range-remaining (- range-rows reused-rows)
          remaining-range
          (if (pos? range-remaining) range-remaining 0)
          point-scan-remaining (- point-scan-rows reused-rows)
          remaining-point-scan
          (if (pos? point-scan-remaining) point-scan-remaining 0)
          scan-remaining (- scan-rows reused-rows)
          remaining-scan
          (if (pos? scan-remaining) scan-remaining 0)
          point-cost      (adjusted-access-cost
                            estimate remaining-point-scan
                            point-output-rows stages)
          upper-cost      (adjusted-access-cost
                            estimate remaining-scan output-rows stages)
          selection-cost  (if (or (not adaptive?)
                                  (and (pos? sample-rows)
                                       (zero? sample-output)
                                       (< sample-rows range-rows)))
                            upper-cost
                            point-cost)
          estimate        (assoc estimate
                                 :rows point-output-rows
                                 :range-rows range-rows
                                 :scan-rows scan-rows
                                 :output-rows output-rows
                                 :point-scan-rows point-scan-rows
                                 :point-output-rows point-output-rows
                                 :cost selection-cost
                                 :point-cost point-cost
                                 :upper-cost upper-cost
                                 :confidence (if sample?
                                               :sampled
                                               (or (:confidence estimate) :low))
                                 :sample-rows sample-rows
                                 :sample-output sample-output
                                 :reused-candidates reused-rows
                                 :remaining-candidates remaining-candidates
                                 :remaining-range remaining-range
                                 :remaining-scan-rows remaining-scan
                                 :remaining-point-scan-rows
                                 remaining-point-scan
                                 :yield yield
                                 :join-stages stages)
          work
          (cond->
              (assoc work :max-candidates candidate-budget)
            (and adaptive?
                 (pos? (long initial-candidate-budget)))
            (assoc :batch-size
                   (min
                     (long (or (:batch-size work)
                               initial-candidate-budget))
                     (long initial-candidate-budget))))]
      (assoc plan
             :work work
             :estimate estimate
             :step (assoc step
                          :work work
                          :sample sample
                          :sample-batch (when reusable? sample-batch))
             :sample-batch (when reusable? sample-batch)))
    plan))

(defn plan-access-joins
  "Add a greedy order for indexed pattern joins reachable from each access
   plan's initially produced variables. This is called only when access plans
   exist; ordinary query planning stays on the existing path."
  [parsed-q inputs plans]
  (let [input-db (first (filter db/db? inputs))]
    (mapv (fn [plan]
            (if-let [plan-db (or (:access-source plan)
                                 (get-in plan [:path :options :db])
                                 input-db)]
              (if-let [plan
                       (or (when (:step plan) plan)
                           (schedule-correlated-access
                             plan-db parsed-q inputs plan))]
                (let [plan (assoc
                             plan
                             :joins
                             (order-access-joins plan-db parsed-q plan)
                             :join-candidates
                             (access-join-candidates plan-db parsed-q plan))]
                  (if (:correlated? plan)
                    plan
                    (adjust-access-plan plan-db parsed-q inputs plan)))
                (assoc plan :unavailable? true))
              plan))
          plans)))

(defn- count-node-datoms
  [^DB db {:keys [free bound] :as node}]
  (reduce
    (fn [{:keys [mcount] :as node} [k i clause]]
      (let [c (fast-clause-count db nil clause (long mcount))
            c (if (zero? c) (zero-count-clause-size db nil clause) c)]
        (cond
          (zero? c)          (reduced (assoc node :mcount 0))
          (< c ^long mcount) (-> node
                                 (assoc-in [k i :count] c)
                                 (assoc :mcount c :mpath [k i]))
          :else              (assoc-in node [k i :count] c))))
    (assoc node :mcount Long/MAX_VALUE)
    (let [flat (fn [k m] (map-indexed (fn [i clause] [k i clause]) m))]
      (concat (flat :bound bound) (flat :free free)))))

(defn- count-known-e-datoms
  [db e {:keys [free] :as node}]
  (u/reduce-indexed
    (fn [{:keys [mcount] :as node} {:keys [attr]} i]
      (let [c (fast-clause-count db e {:attr attr} (long mcount))
            c (if (zero? c)
                (zero-count-clause-size db e {:attr attr})
                c)]
        (cond
          (zero? c)          (reduced (assoc node :mcount 0))
          (< c ^long mcount) (-> node
                                 (assoc-in [:free i :count] c)
                                 (assoc :mcount c :mpath [:free i]))
          :else              (assoc-in node [:free i :count] c))))
    (assoc node :mcount Long/MAX_VALUE) free))

(defn- count-datoms
  [db e node]
  (unreduced (if (int? e)
               (count-known-e-datoms db e node)
               (count-node-datoms db node))))

(defn- add-back-range
  [v {:keys [pred range]}]
  (if range
    (let [range-pred
          (reduce
            (fn [p r]
              (if r
                (add-pred p (activate-var-pred v (range->inequality v r)) true)
                p))
            nil range)]
      (add-pred pred range-pred))
    pred))

(defn- simple-range-pred?
  "Whether add-back-range produces only one index-derived range predicate.
  Keep residual predicates and disjoint ranges on the ordinary predicate cost
  path: both can do materially more work per scanned entity."
  [{:keys [pred range]}]
  (and (nil? pred) (vector? range) (= 1 (count range))))

(defn- merge-pred-options
  [v clause]
  (let [pred (add-back-range v clause)]
    (cond-> {:pred pred}
      (and pred (simple-range-pred? clause)) (assoc :range-pred? true))))

(defn- attrs-vec
  [attrs pred-options skips fidxs]
  (mapv (fn [a options f]
          [a (cond-> (assoc options :skip? false :fidx nil)
               (skips a) (assoc :skip? true)
               f         (assoc :fidx f :skip? true))])
        attrs pred-options fidxs))

(defn- aid [db] #(((db/-schema db) %) :db/aid))

(defn- init-steps
  [db e node single?]
  (let [{:keys [bound free mpath mcount]}            node
        {:keys [attr var val range pred] :as clause} (get-in node mpath)

        know-e? (int? e)
        no-var? (or (not var) (qu/placeholder? var))

        init (cond-> (map->init-step
                       {:attr attr :vars [e] :out [e]
                        :mcount (:count clause)})
               var     (assoc :pred pred
                              :vars (cond-> [e]
                                      (not no-var?) (conj var))
                              :range range)
               (some? val) (assoc :val val)
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
              pred-options (mapv merge-pred-options vars all)
              attrs-v      (attrs-vec attrs pred-options skips (repeat nil))
              cols         (into (:cols init)
                                 (sequence
                                   (comp
                                     (map (fn [a v]
                                            (when-not (skips a) #{a v})))
                                     (remove nil?))
                                   attrs vars))
              strata       (conj (:strata init) (set vars))
              ires         (:result init)
              isp          (:sample init)
              step         (mk-merge-scan-step
                             0 attrs-v vars [e] [e] cols strata #{} nil nil)]
          (cond-> step
            ires (assoc :result (-execute step db ires))
            isp  (assoc :sample (-sample step db isp))))))))

(defn- n-items
  [attrs-v k]
  (reduce
    (fn [^long c [_ m]] (if (m k) (inc c) c))
    0 attrs-v))

(defn- n-costly-preds
  [attrs-v]
  (reduce
    (fn [^long c [_ {:keys [pred range-pred?]}]]
      (if (and pred (not range-pred?)) (inc c) c))
    0 attrs-v))

(defn- estimate-scan-v-size
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
                    result (adjusted-scan-ratio
                             (.size ^List res1)
                             (.size ^List result))
                    sample (adjusted-scan-ratio
                             (.size ^List sp1)
                             (.size ^List sample))
                    :else c/magic-scan-ratio))))))

(defn- factor
  [magic ^long n]
  (if (zero? n) 1 ^long (estimate-round (* ^double magic n))))

(defn- estimate-scan-v-cost
  [{:keys [attrs-v vars]} ^long size]
  (* size
     ^double c/magic-cost-merge-scan-v
     ^long (factor c/magic-cost-var (count vars))
     ^long (factor c/magic-cost-pred (n-costly-preds attrs-v))
     ^long (factor c/magic-cost-fidx (n-items attrs-v :fidx))))

(defn- estimate-base-cost
  [{:keys [mcount]} steps]
  (let [{:keys [pred]} (first steps)
        init-cost      (estimate-round
                         (cond-> (* ^double c/magic-cost-init-scan-e
                                    ^long mcount)
                           pred (* ^double c/magic-cost-pred)))]
    (if (< 1 (count steps))
      (+ ^long init-cost ^long (estimate-scan-v-cost (peek steps) mcount))
      init-cost)))

(defn- final-plan-cost ^double
  [plan-trace]
  (if-let [{:keys [steps cost]} (last plan-trace)]
    (if (some? cost)
      (double cost)
      (if (seq steps)
        (double
          (estimate-base-cost
            {:mcount (long (or (:mcount (first steps)) 1))}
            steps))
        0.0))
    0.0))

(defn estimated-plan-cost
  "Return the existing planner's estimated cost for all final component plans."
  [{:keys [plan result-set]}]
  (if (= result-set #{})
    0.0
    (reduce
      (fn [cost [_src components]]
        (+ (double cost)
           (double
             (reduce
               (fn [cost plan-trace]
                 (+ (double cost) (final-plan-cost plan-trace)))
               0.0 components))))
      0.0 plan)))

(defn- final-plan-size ^long
  [plan-trace]
  (if-let [{:keys [steps size]} (last plan-trace)]
    (long (or size
              (some-> steps first :mcount)
              1))
    0))

(defn- estimated-plan-size
  [{:keys [plan result-set]}]
  (if (= result-set #{})
    0
    (reduce
      (fn [size [_src components]]
        (let [component-size
              (reduce
                (fn [size plan-trace]
                  (let [n (final-plan-size plan-trace)]
                    (if (zero? (long size))
                      n
                      (estimate-round (* (double size) n)))))
                0 components)]
          (if (zero? (long size))
            component-size
            (estimate-round (* (double size) (long component-size))))))
      0 plan)))

(defn- late-operation
  [clause]
  (let [parsed (if (or (instance? Predicate clause)
                       (instance? Function clause)
                       (instance? Pattern clause)
                       (instance? Not clause)
                       (instance? Or clause)
                       (instance? RuleExpr clause))
                 clause
                 (dp/parse-clause clause))]
    (cond
      (instance? Pattern parsed)   :indexed-join
      (instance? Predicate parsed) :predicate
      (instance? Function parsed)  :function
      (instance? Not parsed)       :not
      (instance? Or parsed)        :or
      (instance? RuleExpr parsed)  :rule
      :else                        :residual)))

(defn- late-row-operation?
  [operation]
  (#{:predicate :function} operation))

(defn- late-expansion-operation?
  [operation]
  (#{:indexed-join :or :rule} operation))

(defn- estimated-late-input-size
  ^long
  [{:keys [plan rels result-set]} ^long plan-size]
  (cond
    (= result-set #{}) 0
    (seq plan)          plan-size
    :else               (long (sample-context-size rels))))

(defn- sampled-access-cardinality
  "Estimate complete result cardinality from real planning samples. Heuristic
   access estimates are deliberately excluded: only an observed residual yield
   may increase the conventional plan's late-clause cardinality."
  [access-plans]
  (reduce
    (fn [best {:keys [correlated? estimate]}]
      (let [sample-rows (long (or (:sample-rows estimate) 0))
            sample-output (long (or (:sample-output estimate) 0))
            output-rows (qaccess/estimate-output-rows estimate)
            yield       (:yield estimate)]
        (if (and (not correlated?)
                 (= :sampled (:confidence estimate))
                 (pos? sample-rows)
                 (pos? sample-output)
                 (number? yield)
                 (pos? output-rows))
          (let [rows (estimate-round (* (double output-rows)
                                        (double yield)))
                candidate {:rows          rows
                           :sample-rows   sample-rows
                           :sample-output sample-output
                           :range-rows    (qaccess/estimate-range-rows estimate)
                           :yield         (double yield)
                           :confidence    :sampled}]
            (if (> rows (long (or (:rows best) 0))) candidate best))
          best)))
    nil access-plans))

(defn- estimated-late-cost
  [context ^long plan-size access-plans]
  (let [input-size  (estimated-late-input-size context plan-size)
        operations  (mapv late-operation (:late-clauses context))
        expansion?  (some late-expansion-operation? operations)
        sampled     (when expansion?
                      (sampled-access-cardinality access-plans))
        output-size (max input-size (long (or (:rows sampled) 0)))
        expanded?   (< input-size output-size)
        expansion-cost
        (if expanded?
          (estimate-link-cost input-size output-size)
          0)
        initial
        {:cost expansion-cost
         :output-size output-size
         :stages
         (cond-> []
           expanded?
           (conj (assoc sampled
                        :operation :sampled-late-expansion
                        :input input-size
                        :output output-size
                        :cost expansion-cost)))}]
    (reduce
      (fn [{:keys [cost stages] :as estimate} [clause operation]]
        (if (late-row-operation? operation)
          (let [stage-cost (estimate-row-evaluation-cost output-size)]
            {:cost   (+ (double cost) (double stage-cost))
             :output-size output-size
             :stages (conj stages
                           {:operation operation
                            :input     output-size
                            :cost      stage-cost
                            :clause    clause})})
          estimate))
      initial
      (map vector (:late-clauses context) operations))))

(defn- logical-plan-key
  [graph]
  (into #{}
        (mapcat (fn [[src nodes]]
                  (map #(vector src %) (keys nodes))))
        graph))

(defn- top-k-enforcer-cost
  [^long rows demand]
  (if (and (pos? rows) (seq (:ordering demand)))
    (let [required-count (long (or (:required-count demand) rows))
          required (max 1 required-count)
          retained (long (max 2 (min rows required)))]
      (* (double rows)
         (/ (Math/log (double retained)) (Math/log 2.0))))
    0.0))

(defn- selective-value-candidates
  [context]
  (let [{:keys [parsed-q sources]} context]
    (into []
          (keep-indexed
            (fn [clause-idx [parsed-clause orig-clause]]
              (when (and (instance? Pattern parsed-clause)
                         (vector? orig-clause)
                         (= 3 (count (:pattern parsed-clause))))
                (let [parsed-pattern (:pattern parsed-clause)
                      e-sym          (pattern-var-symbol
                                       (first parsed-pattern))
                      attr           (second parsed-pattern)
                      value          (nth parsed-pattern 2)]
                  (when (and e-sym
                             (not (rel-bound-var? context e-sym))
                             (< 1 (variable-ref-count
                                    (:qwhere parsed-q) e-sym))
                             (instance? Constant attr)
                             (keyword? (:value attr))
                             (instance? Constant value))
                    (let [source-sym (clause-source-symbol
                                       (:source parsed-clause))]
                      (when-let [source (get sources source-sym)]
                        (when (db/-searchable? source)
                          (let [pattern (qresolve/resolve-pattern-lookup-refs
                                          source
                                          (pattern-form orig-clause))
                                attr    (second pattern)
                                value   (nth pattern 2)
                                fanout  (av-size (.-store ^DB source)
                                                 attr value)]
                            {:key         [source-sym pattern]
                             :clause-idx  clause-idx
                             :source-sym  source-sym
                             :source      source
                             :pattern     pattern
                             :entity-sym  e-sym
                             :attr        attr
                             :value       value
                             :fanout      (long fanout)})))))))))
          (map vector (:qwhere parsed-q)
               (:qorig-where parsed-q)))))

(defn- plan-binds-var?
  [plan sym]
  (when-let [cols (some-> plan :steps last :cols)]
    (some? (qplan/find-index sym cols))))

(defn- plan-entry-cost
  ^double [plan]
  (final-plan-cost [plan]))

(defn- delayed-anchor-cost
  ^double [planned source-sym entity-sym]
  (reduce
    (fn [^double best trace]
      (if-let [idx' (first (keep-indexed
                             (fn [idx plan]
                               (when (plan-binds-var? plan entity-sym) idx))
                             trace))]
        (let [idx     (long idx')
              current (double (plan-entry-cost (nth trace idx)))
              prior   (double (if (zero? idx)
                                0.0
                                (plan-entry-cost
                                  (nth trace (dec (long idx))))))]
          (max best (max 0.0 (- current prior))))
        best))
    0.0
    (get-in planned [:plan source-sym])))

(defn- selective-anchor-cost
  ^double [^long fanout]
  (+ (double (estimate-link-cost 1 fanout))
     (materialized-output-cost fanout 1)))

(defn- planned-context-for-cost
  [context]
  (let [context (build-graph context)]
    ;; The first graph construction ends the normal building phase. Candidate
    ;; materialization and any residual replanning belong to planning time.
    (when (and qplan/*explain*
               (not (contains? @qplan/*explain* :building-time)))
      (let [parsing-time (long (or (:parsing-time @qplan/*explain*) 0))]
        (vswap! qplan/*explain* assoc :building-time
                (- (System/nanoTime)
                   (+ (long qplan/*start-time*) parsing-time)))))
    (-> context build-plan plan-not-joins)))

(defn- estimated-context-work
  ^double [planned]
  (if (= #{} (:result-set planned))
    0.0
    (let [plan-size (estimated-plan-size planned)
          late      (estimated-late-cost
                      planned plan-size (:access-plans planned))]
      (+ (double (estimated-plan-cost planned))
         (double (:cost late))))))

(defn- costed-selective-value-candidates
  [planned ^double base-cost candidates]
  (into []
        (comp
          (map (fn [{:keys [source-sym entity-sym fanout] :as candidate}]
                 (let [delayed-cost (delayed-anchor-cost
                                      planned source-sym entity-sym)
                       anchor-cost  (selective-anchor-cost fanout)]
                   (assoc candidate
                          :delayed-cost delayed-cost
                          :anchor-cost anchor-cost
                          :remaining-cost (max 0.0
                                               (- base-cost delayed-cost))
                          :saving (- delayed-cost anchor-cost)))))
          ;; This is the cheap decision boundary. Only a value lookup cheaper
          ;; than a delayed step which dominates the rest of the plan is
          ;; allowed to perform speculative materialization. The margin keeps
          ;; ordinary graph plans from paying execution work merely to reject
          ;; a close alternative.
          (filter
            (fn [candidate]
              (and (pos? (double (:saving candidate)))
                   (< (* selective-anchor-dominance-margin
                         (double (:remaining-cost candidate)))
                      (double (:delayed-cost candidate)))))))
        candidates))

(defn- try-selective-value-candidate
  [context {:keys [^long clause-idx source pattern fanout anchor-cost]
            :as candidate}
   ^double budget]
  (if (<= budget (double anchor-cost))
    (assoc candidate
           :context context
           :eligible? false
           :guardrail {:pattern pattern
                       :projected-rows fanout
                       :projected-cost anchor-cost
                       :accumulated-cost 0.0
                       :budget budget}
           :materialization-cost 0.0
           :materialization-stages [])
    (let [seed         (materialize-pattern-with-cost context source pattern 1)
          seed-context (remove-materialized-clause
                         (:context seed) clause-idx)
          budget       (max 0.0 (- budget (double (:cost seed))))
          preflight    (project-bound-patterns-with-cost
                         seed-context budget)]
      (if-not (:eligible? preflight)
        (assoc candidate
               :context context
               :eligible? false
               :guardrail (:guardrail preflight)
               :materialization-cost (double (:cost seed))
               :materialization-stages [(:stage seed)])
        (let [propagated (materialize-bound-patterns-with-cost
                           seed-context budget)]
          (assoc candidate
                 :context              (:context propagated)
                 :eligible?            (:eligible? propagated)
                 :guardrail            (:guardrail propagated)
                 :materialization-cost (+ (double (:cost seed))
                                          (double (:cost propagated)))
                 :materialization-stages
                 (into [(:stage seed)] (:stages propagated))))))))

(defn- record-selective-value-decision!
  [decision]
  (when qplan/*explain*
    (vswap! qplan/*explain* update :pre-materialization-decisions
            (fnil conj []) decision)))

(defn materialize-selective-value-lookups
  "Cost non-unique constant AVE lookups as possible pre-planning entity
   relations. A candidate first has to beat the normal plan step that would
   introduce its entity. The fully propagated relation and residual plan must
   then beat the unchanged plan before the rewrite is accepted."
  [context]
  (loop [context  context
         rejected #{}
         preplanned nil]
    (let [raw-candidates (remove #(contains? rejected (:key %))
                                 (selective-value-candidates context))]
      (if (empty? raw-candidates)
        (cond-> (or preplanned context)
          preplanned (assoc ::selective-preplanned? true))
        (let [planned        (or preplanned
                                 (planned-context-for-cost context))
              base-cost      (estimated-context-work planned)
              candidates     (costed-selective-value-candidates
                               planned base-cost raw-candidates)]
          (if (empty? candidates)
            (assoc planned ::selective-preplanned? true)
            (let [candidate     (apply max-key :saving candidates)
                  trial         (try-selective-value-candidate
                                  context candidate
                                  (min (double base-cost)
                                       (double (:delayed-cost candidate))))
                  trial-planned (when (:eligible? trial)
                                  (planned-context-for-cost (:context trial)))
                  residual-cost (when trial-planned
                                  (estimated-context-work trial-planned))
                  trial-cost    (when residual-cost
                                  (+ (double (:materialization-cost trial))
                                     (double residual-cost)))
                  selected?     (and trial-cost
                                     (< (double trial-cost)
                                        (double base-cost)))
                  decision      {:strategy
                                 (if selected?
                                   :pre-materialized-value-lookup
                                   :planner-value-lookup)
                                 :pattern  (:pattern candidate)
                                 :entity   (:entity-sym candidate)
                                 :fanout   (:fanout candidate)
                                 :lookup-cost (:anchor-cost candidate)
                                 :delayed-cost (:delayed-cost candidate)
                                 :materialization-cost
                                 (:materialization-cost trial)
                                 :residual-cost residual-cost
                                 :guardrail (:guardrail trial)
                                 :baseline-cost base-cost
                                 :candidate-cost trial-cost}]
              (record-selective-value-decision! decision)
              (if selected?
                (recur (:context trial) #{} trial-planned)
                (recur context (conj rejected (:key candidate))
                       planned)))))))))

(defn- conventional-access-cost
  [access-plans]
  (reduce
    +
    0.0
    (vals
      (reduce
        (fn [costs {:keys [expr estimate]}]
          (if-some [cost (:conventional-cost estimate)]
            (let [logical-key (or (:covered-originals expr)
                                  (:covers expr))]
              (update costs logical-key
                      (fn [previous]
                        (if (some? previous)
                          (min (double previous) (double cost))
                          (double cost)))))
            costs))
        {}
        access-plans))))

(defn- conventional-alternative
  [context logical-key demand access-plans]
  (let [plan-size  (estimated-plan-size context)
        base-cost  (estimated-plan-cost context)
        late       (estimated-late-cost context plan-size access-plans)
        size        (:output-size late)
        late-cost  (:cost late)
        access-cost (conventional-access-cost access-plans)
        effective-late-cost (max (double late-cost)
                                 (double access-cost))
        properties (qplan/->PhysicalProperties
                     (:ordering demand) false true :exact
                     #{:complete :top-k-enforced})
        enforcer-cost (top-k-enforcer-cost size demand)
        cost          (+ (double base-cost)
                         effective-late-cost
                         (double enforcer-cost))
        plan          (qplan/->ConventionalRootPlan
                        context properties cost size)]
    (assoc
      (qplan/->PlanAlternative
        :conventional logical-key properties plan
        cost size nil)
      :cost-breakdown {:base        base-cost
                       :late        late-cost
                       :access-expression access-cost
                       :effective-late effective-late-cost
                       :late-stages (:stages late)
                       :enforcer    enforcer-cost})))

(defn- root-access-properties
  [fragment-properties demand]
  (let [ordered? (seq (:ordering demand))]
    (qplan/->PhysicalProperties
      (when ordered? (:ordering demand))
      false
      true
      (:quality fragment-properties)
      (cond-> (set/difference (:capabilities fragment-properties)
                              qaccess/top-k-proof-capabilities)
        true     (conj :complete)
        ordered? (conj :top-k-enforced)))))

(defn- fragment-output-cols
  [step joins]
  (reduce
    (fn [cols join]
      (into cols (remove (set cols)) (:produces-cols join)))
    (vec (:cols step))
    joins))

(defn- estimated-fragment-join-cost
  [joins operators initial-size]
  (first
    (reduce
      (fn [[cost size] [join operator]]
        (let [join-size     (max 0 (long (or (get-in join [:estimate :rows])
                                             size)))
              operator-cost (case (:type operator)
                              :hash-join
                              (estimate-hash-join-cost size join-size)

                              :index-join
                              (estimate-link-cost size join-size)

                              0.0)]
          [(+ (double cost) (double operator-cost)) join-size]))
      [0.0 (max 0 (long initial-size))]
      (map vector joins operators))))

(defn- access-alternative
  [logical-key fallback access-plan fragment]
  (let [{:keys [step demand work estimate
                correlated? outer-query outer-estimate]} access-plan
        fragment-plan (:plan fragment)
        fragment-properties (:properties fragment)
        joins       (:joins fragment-plan)
        operators   (:operators fragment-plan)
        fragment-cols (fragment-output-cols step joins)
        access-plan (assoc access-plan
                           :joins joins
                           :operators operators
                           :fragment-cols fragment-cols
                           :fragment-properties fragment-properties
                           :fragment-cost (:cost fragment)
                           :fragment-size (:size fragment))
        adaptive-top-k?
        (and (not correlated?)
             (qaccess/adaptive-top-k-properties?
               (:ordering fragment-properties)
               (:capabilities fragment-properties)
               demand))
        adaptive-limit?
        (and (not correlated?)
             (qaccess/adaptive-limit-properties?
               (:capabilities fragment-properties) demand))
        adaptive? (or adaptive-top-k? adaptive-limit?)
        per-open-cost
        (double
          (if adaptive?
            (:cost estimate)
            (or (:upper-cost estimate) (:cost estimate))))
        per-open-rows
        (long
          (or (if adaptive?
                (:rows estimate)
                (qaccess/estimate-output-rows estimate))
              0))
        outer-rows (long (if correlated?
                           (or (:rows outer-estimate) 1)
                           1))
        modeled-joins? (some #(= :indexed-join (:operation %))
                             (:join-stages estimate))
        all-joins      (or (:joins access-plan) [])
        candidate-rows per-open-rows
        unmodeled-join-cost
        (if modeled-joins?
          0.0
          (estimated-fragment-join-cost
            all-joins
            (repeat {:type :index-join})
            candidate-rows))
        selected-index-cost
        (estimated-fragment-join-cost
          joins
          (repeat {:type :index-join})
          candidate-rows)
        selected-physical-cost
        (estimated-fragment-join-cost joins operators candidate-rows)
        physical-adjustment (- (double selected-physical-cost)
                               (double selected-index-cost))
        access-cost
        (+ (double (if correlated? (or (:cost outer-estimate) 0.0) 0.0))
           (* (double outer-rows)
              (+ per-open-cost
                 (double unmodeled-join-cost)
                 physical-adjustment)))
        rows      (long (* outer-rows per-open-rows))
        properties (root-access-properties fragment-properties demand)
        estimated-size
        (estimate-round
          (* (double rows)
             (double (or (:yield estimate) 1.0))))
        size (max 0 (long estimated-size))
        enforcer-cost (top-k-enforcer-cost size demand)
        cost          (+ (double access-cost) (double enforcer-cost))
        plan (assoc
               (qplan/->AccessRootPlan
                 (cond
                   correlated?     :correlated-complete
                   adaptive-top-k? :adaptive-top-k
                   adaptive-limit? :adaptive-limit
                   :else           :complete)
                 step nil demand work properties
                 cost size fallback)
               :access-plan access-plan
               :outer-query outer-query
               :logical-key logical-key
               :joins joins
               :operators operators
               :fragment-cols fragment-cols
               :fragment-properties fragment-properties)]
    (assoc
      (qplan/->PlanAlternative
        :access logical-key properties plan cost size nil)
      :cost-breakdown
      {:point       (:point-cost estimate)
       :upper-bound (:upper-cost estimate)
       :required-count (:required-count demand)
       :outer       (when correlated? outer-estimate)
       :access      access-cost
       :fragment    (:cost fragment)
       :unmodeled-joins unmodeled-join-cost
       :physical-adjustment physical-adjustment
       :enforcer    enforcer-cost
       :selected    cost
       :stages      (:join-stages estimate)})))

(defn- quality-satisfies?
  [provided required]
  (or (nil? required)
      (= provided required)
      (and (= required :approximate) (= provided :exact))))

(defn- ordering-terms
  [ordering]
  (if (every? sequential? ordering)
    (vec ordering)
    (mapv vec (partition-all 2 ordering))))

(defn- ordering-satisfies?
  [provided required]
  (let [provided (ordering-terms provided)
        required (ordering-terms required)]
    (or (empty? required)
        (and (<= (count required) (count provided))
             (= required (subvec provided 0 (count required)))))))

(defn properties-satisfy?
  "Return true when provided physical properties are a superset of required
   properties for one logical subset."
  [provided required]
  (and (ordering-satisfies? (:ordering provided) (:ordering required))
       (or (not (:resumable? required)) (:resumable? provided))
       (or (not (:complete? required)) (:complete? provided))
       (quality-satisfies? (:quality provided) (:quality required))
       (set/subset? (:capabilities required) (:capabilities provided))))

(defn- alternative-dominates?
  [left right]
  (and (<= (double (:cost left)) (double (:cost right)))
       (<= (long (:size left)) (long (:size right)))
       (properties-satisfy? (:properties left) (:properties right))))

(defn retain-property-alternative
  "Retain a bounded Pareto frontier for alternatives implementing one logical
   subset. Cheaper alternatives with a property superset dominate."
  [alternatives candidate]
  (if (some #(alternative-dominates? % candidate) alternatives)
    (vec alternatives)
    (conj
      (into []
            (remove #(alternative-dominates? candidate %))
            alternatives)
      candidate)))

(defn propagate-physical-properties
  "Transfer physical properties through an access-aware physical operator."
  [properties {:keys [type preserves-outer-order? ordering]}]
  (case type
    :filter properties

    :index-join
    (if preserves-outer-order?
      properties
      (assoc properties
             :ordering nil
             :resumable? false
             :capabilities
             (set/difference (:capabilities properties)
                             qaccess/top-k-proof-capabilities)))

    :hash-join
    (assoc properties
           :ordering nil
           :resumable? false
           :capabilities
           (set/difference (:capabilities properties)
                           qaccess/top-k-proof-capabilities))

    :sort
    (assoc properties
           :ordering ordering
           :resumable? false
           :capabilities
           (-> (:capabilities properties)
               (set/difference qaccess/top-k-proof-capabilities)
               (conj :top-k-enforced)))

    properties))

(defn- alternative-satisfies?
  [{:keys [properties]} demand]
  (and (quality-satisfies? (:quality properties) (:quality demand))
       (or (:complete? properties)
           (and (= (:ordering properties) (:ordering demand))
                (set/subset? qaccess/top-k-proof-capabilities
                             (:capabilities properties))))))

(defn- choose-alternative
  [alternatives demand]
  (first
    (sort-by
      (juxt :cost #(if (= :conventional (:kind %)) 0 1))
      (filter #(alternative-satisfies? % demand) alternatives))))

(defn- access-fragment-properties
  [path]
  (qplan/->PhysicalProperties
    (:ordering path)
    (contains? (:capabilities path) :resumable)
    (contains? (:capabilities path) :complete)
    (:quality path)
    (:capabilities path)))

(defn- add-subset-alternative
  [subsets alternative]
  (update subsets (:logical-key alternative)
          #(retain-property-alternative (or % []) alternative)))

(defn- access-subset-alternatives
  [{:keys [access-id expr path demand estimate step joins join-candidates]}]
  (let [joins      (vec (or join-candidates joins))
        properties (access-fragment-properties path)
        source-key (set (:covers expr))
        adaptive?  (qaccess/adaptive-demand? path demand)
        estimated-source-size
        (long
          (if adaptive?
            (or (:rows estimate) 0)
            (qaccess/estimate-output-rows estimate)))
        source-size (max 0 estimated-source-size)
        source-scan-rows
        (long
          (if adaptive?
            (or (:remaining-point-scan-rows estimate)
                (:point-scan-rows estimate)
                source-size)
            (qaccess/estimate-scan-rows estimate)))
        source-cost
        (double
          (if (pos? source-scan-rows)
            (+ (double (or (:startup estimate) 0.0))
               (* (double source-scan-rows)
                  (double (or (:per-row estimate) 0.0))))
            0.0))
        source
        (qplan/->PlanAlternative
          :access-fragment source-key properties
          {:access-id access-id :source step :joins [] :operators []}
          source-cost source-size nil)]
    (loop [queue      [#{}]
           memo       {#{} [source]}
           expansions 0]
      (if (or (empty? queue)
              (>= (long expansions) (long c/plan-search-max)))
        (mapcat val memo)
        (let [used       (first queue)
              queue      (subvec queue 1)
              frontier   (get memo used)
              bound      (into (set (:cols step))
                               (mapcat :vars)
                               (map joins used))
              eligible
              (keep-indexed
                (fn [i candidate]
                  (when-not (contains? used i)
                    (when-let [join
                               (access-join-from-bound bound candidate)]
                      [i join])))
                joins)
              [memo queue]
              (reduce
                (fn [[memo queue] [i {:keys [estimate] :as join}]]
                  (let [used'       (conj used i)
                        logical-key
                        (into source-key
                              (map (comp :clause joins))
                              used')
                        candidates
                        (mapcat
                          (fn [{:keys [properties plan cost size]}]
                            (let [estimated-join-size
                                  (long (or (:rows estimate) size))
                                  join-size (max 0 estimated-join-size)
                                  index-op
                                  {:type :index-join
                                   :preserves-outer-order? true}
                                  hash-op {:type :hash-join}
                                  alternative
                                  (fn [operator operator-cost]
                                    (qplan/->PlanAlternative
                                      :access-fragment logical-key
                                      (propagate-physical-properties
                                        properties operator)
                                      (-> plan
                                          (update :joins conj join)
                                          (update :operators conj operator))
                                      (+ (double cost)
                                         (double operator-cost))
                                      join-size nil))]
                              [(alternative
                                 index-op
                                 (estimate-link-cost size join-size))
                               (alternative
                                 hash-op
                                 (estimate-hash-join-cost size join-size))]))
                          frontier)
                        previous (get memo used' [])
                        retained
                        (reduce retain-property-alternative
                                previous candidates)]
                    (if (= previous retained)
                      [memo queue]
                      [(assoc memo used' retained)
                       (conj queue used')])))
                [memo queue] eligible)]
          (recur queue memo (unchecked-inc expansions)))))))

(defn- access-subset-memo
  [access-plans]
  (reduce
    (fn [subsets alternative]
      (add-subset-alternative subsets alternative))
    {}
    (mapcat access-subset-alternatives access-plans)))

(defn- executable-access-fragments
  [subsets {:keys [access-id] :as access-plan}]
  (let [source-key (set (get-in access-plan [:expr :covers]))]
    (->> subsets
         (mapcat val)
         (filter #(and (= access-id (get-in % [:plan :access-id]))
                       (set/subset? source-key (:logical-key %))))
         ;; Prefer a more complete physical fragment when costs and properties
         ;; tie, while still retaining executable roots for smaller subsets.
         (sort-by #(count (:logical-key %)) >)
         vec)))

(defn build-property-memo
  "For access queries, retain conventional and physical access alternatives
   under one logical root until the query's ordering/quality demand is applied."
  [{:keys [access-plans access-demand graph] :as context}]
  (if (seq access-plans)
    (let [logical-key  (logical-plan-key graph)
          demand       (or access-demand (:demand (first access-plans)))
          executable   (into []
                             (comp
                               (filter #(and (:step %)
                                             (not (:unavailable? %))))
                               (map-indexed #(assoc %2 :access-id %1)))
                             access-plans)
          conventional (conventional-alternative
                         context logical-key demand access-plans)
          subsets      (access-subset-memo executable)
          alternatives
          (into [conventional]
                (mapcat
                  (fn [access-plan]
                    (map #(access-alternative
                            logical-key (:plan conventional) access-plan %)
                         (executable-access-fragments subsets access-plan))))
                executable)
          selected     (choose-alternative alternatives demand)]
      (assoc context
             :access-plans executable
             :property-memo
             (qplan/->PropertyMemo
               logical-key demand alternatives selected subsets)))
    context))

(defn selected-access-plan
  [context]
  (let [selected (get-in context [:property-memo :selected])]
    (when (= :access (:kind selected))
      (get-in selected [:plan :access-plan]))))

(defn selected-alternative
  [context]
  (get-in context [:property-memo :selected]))

(defn property-memo-summary
  [{:keys [logical-key demand alternatives selected subsets]}]
  (let [summary (fn [{:keys [kind properties cost size cost-breakdown plan]}]
                  {:kind       kind
                   :properties properties
                   :fragment-properties (:fragment-properties plan)
                   :cost       cost
                   :size       size
                   :mode       (:mode plan)
                   :operators  (:operators plan)
                   :fragment-cols (:fragment-cols plan)
                   :cost-breakdown cost-breakdown})]
    {:logical-key  logical-key
     :demand       demand
     :alternatives (mapv summary alternatives)
     :selected     (some-> selected summary)
     :subsets
     (into {}
           (map (fn [[logical-key alternatives]]
                  [logical-key (mapv summary alternatives)]))
           subsets)}))

(defn- base-plan
  ([db nodes e]
   (base-plan db nodes e false))
  ([db nodes e single?]
   (let [node   (get nodes e)
         mcount (:mcount node)]
     (when-not (zero? ^long mcount)
       (let [isteps (init-steps db e node single?)]
         (if single?
           (make-plan isteps nil nil 0)
           (make-plan isteps
                      (estimate-base-cost node isteps)
                      (estimate-scan-v-size mcount isteps)
                      0)))))))

(defn writing? [db] (l/writing? (.-lmdb ^Store (.-store ^DB db))))

(defn- update-nodes
  [db nodes]
  (if (= (count nodes) 1)
    (let [[e node] (first nodes)] {e (count-datoms db e node)})
    (let [f (bound-fn [e] [e (count-datoms db e (get nodes e))])]
      (into {} (if (writing? db)
                 (map f (keys nodes))
                 (map+ f (keys nodes)))))))

(defn- build-base-plans
  [db nodes component]
  (let [f (bound-fn [e] [[e] (base-plan db nodes e)])]
    (into {} (if (writing? db)
               (map f component)
               (map+ f component)))))

(def find-index qplan/find-index)

(defn- merge-scan-step
  [db last-step index new-key new-steps]
  (let [in         (:out last-step)
        out        (if (set? in) (set new-key) new-key)
        lcols      (:cols last-step)
        lstrata    (:strata last-step)
        ncols      (:cols (peek new-steps))
        [s1 s2]    new-steps
        val1       (:val s1)
        [_ v1]     (:vars s1)
        a1         (:attr s1)
        ip-options (merge-pred-options v1 s1)
        ip         (cond-> (:pred ip-options)
                     (some? val1) (add-pred #(= % val1)))
        ip-options (cond-> (assoc ip-options :pred ip)
                     (some? val1) (dissoc :range-pred?))
        attrs-v2   (:attrs-v s2)
        get-a      (fn [coll] (some #(when (keyword? %) %) coll))
        [attrs-v vars cols]
        (reduce
          (fn [[attrs-v vars cols] col]
            (let [v (some #(when (symbol? %) %) col)]
              (if (and ip (= v v1))
                [attrs-v vars cols]
                (let [a (get-a col)
                      options (or (some (fn [[attr options]]
                                          (when (and (= a attr)
                                                     (:pred options))
                                            (select-keys options
                                                         [:pred
                                                          :range-pred?])))
                                        attrs-v2)
                                  {:pred nil})
                      skip? (boolean
                              (some (fn [[attr options]]
                                      (when (= a attr) (:skip? options)))
                                    attrs-v2))]
                  (if-let [f (find-index v lcols)]
                    [(conj attrs-v
                           [a (assoc options :skip? true :fidx f)])
                     vars cols]
                    [(conj attrs-v
                           [a (assoc options
                                     :skip? skip?
                                     :fidx nil)])
                     (conj vars v) (conj cols col)])))))
          (if (or ip (nil? v1))
            [[[a1 (assoc ip-options
                         :skip? (not (and v1 (find-index v1 ncols)))
                         :fidx nil)]]
             (if v1 [v1] [])
             (if v1 [#{a1 v1}] [])]
            [[] [] []])
          (rest ncols))
        fcols    (into lcols (sort-by (comp (aid db) get-a) cols))
        strata   (conj lstrata (set vars))
        lseen    (:seen-or-joins last-step)]
    (mk-merge-scan-step index attrs-v vars in out fcols strata lseen nil nil)))

(defn- index-by-link
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

(defn- enrich-cols
  [cols index attr]
  (let [pa (cols index)]
    (mapv (fn [e] (if (and (= e pa) (set? e)) (conj e attr) e)) cols)))

(defn- col-var
  [col]
  (if (set? col)
    (some #(when (symbol? %) %) col)
    col))

(defn- merge-join-cols
  "Merge input and target cols for hash join output, preserving input order.
   Returns [merged-cols new-vars]."
  [in-cols tgt-cols]
  (let [^HashMap tgt-map    (HashMap.)
        ^HashSet in-var-set (HashSet.)]
    (doseq [col tgt-cols]
      (.put tgt-map (col-var col) col))
    (let [merged-in
          (mapv (fn [col]
                  (let [v (col-var col)]
                    (.add in-var-set v)
                    (if (.containsKey tgt-map v)
                      (let [tcol (.get tgt-map v)]
                        (cond
                          (set? col)  (if (set? tcol) (into col tcol) col)
                          (set? tcol) tcol
                          :else       v))
                      col)))
                in-cols)
          [new-cols new-vars]
          (loop [i        0
                 new-cols (transient [])
                 new-vars (transient #{})]
            (if (< i (count tgt-cols))
              (let [col (nth tgt-cols i)
                    v   (col-var col)]
                (if (.contains in-var-set v)
                  (recur (u/long-inc i) new-cols new-vars)
                  (recur (u/long-inc i) (conj! new-cols col)
                         (conj! new-vars v))))
              [(persistent! new-cols) (persistent! new-vars)]))]
      [(into merged-in new-cols) new-vars])))

(defn- required-new-join-var?
  [required-vars in-cols tgt-cols]
  (let [^HashSet in-vars (HashSet.)]
    (doseq [col in-cols]
      (.add in-vars (col-var col)))
    (boolean
      (some (fn [col]
              (let [v (col-var col)]
                (and (required-vars v) (not (.contains in-vars v)))))
            tgt-cols))))

(defn- link-step
  [type last-step index attr tgt new-key]
  (let [in      (:out last-step)
        out     (if (set? in) (set new-key) new-key)
        lcols   (:cols last-step)
        lstrata (:strata last-step)
        lseen   (:seen-or-joins last-step)
        fidx    (find-index tgt lcols)
        cols    (cond-> (enrich-cols lcols index attr)
                  (nil? fidx) (conj tgt))]
    [(mk-link-step type index attr tgt fidx in out cols (conj lstrata #{tgt}) lseen)
     (or fidx (dec (count cols)))]))

(defn- rev-ref-plan
  [db last-step index {:keys [type attr tgt]} new-key new-steps]
  (let [[step n-index] (link-step type last-step index attr tgt new-key)]
    (if (= 1 (count new-steps))
      [step]
      [step (merge-scan-step db step n-index new-key new-steps)])))

(defn- val-eq-plan
  [db last-step index {:keys [type attrs tgt]} new-key new-steps]
  (let [attr           (attrs tgt)
        [step n-index] (link-step type last-step index attr tgt new-key)]
    (if (= 1 (count new-steps))
      [step]
      [step (merge-scan-step db step n-index new-key new-steps)])))

(defn- hash-join-plan
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
        step            (mk-hash-join-step link link-e in out lcols cols
                                           (conj lstrata new-vars) lseen
                                           tgt-steps in-size tgt-size)
        base-cost       (or (:cost new-base-plan) 0)
        join-cost       (estimate-hash-join-cost
                          in-size tgt-size result-size (count cols))]
    (make-plan [step]
               (+ ^long cost ^long base-cost ^long join-cost)
               result-size
               (- ^long (find-index link-e (:strata last-step))))))

(defn- semi-join-eligible?
  [nodes incoming-link-counts required-vars prev-plan link-e new-e link
   new-base-plan]
  (when (and new-base-plan
             (#{:ref :_ref :val-eq} (:type link))
             (= 1 (count (get-in nodes [new-e :links])))
             (= link-e (get-in nodes [new-e :links 0 :tgt]))
             (= 1 (long (get incoming-link-counts new-e 0))))
    (let [in-cols  (:cols (peek (:steps prev-plan)))
          tgt-cols (:cols (peek (:steps new-base-plan)))]
      (not (required-new-join-var? required-vars in-cols tgt-cols)))))

(defn- or-join-plan*
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
        or-step   (mk-or-join-step clause
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

(defn- count-init-follows
  [^DB db tuples attr index]
  (let [store (.-store db)]
    (rd/fold
      +
      (rd/map #(av-size store attr (aget ^objects % index))
              (p/remove-end-scan tuples)))))

(defn- count-init-follows-summary
  [^DB db tuples attr index]
  (let [store    (.-store db)
        ^List ts (p/remove-end-scan tuples)
        n        (.size ts)]
    (loop [i   0
           sum 0.0]
      (if (< i n)
        (let [^objects t (.get ts i)]
          (recur (u/long-inc i)
                 (+ sum
                    (double (av-size store attr (aget t index))))))
        {:n n
         :sum sum}))))

(defn- count-init-follows-summary-cached
  "Cache count and sum by sample identity, without hashing the sample list."
  [^DB db ^IdentityHashMap cache tuples attr index]
  (let [^HashMap entries (or (.get cache tuples)
                             (let [m (HashMap.)]
                               (.put cache tuples m)
                               m))
        k                [::link-follow-summary attr index]]
    (if (.containsKey entries k)
      (.get entries k)
      (let [summary (count-init-follows-summary db tuples attr index)]
        (.put entries k summary)
        summary))))

(defn- link-ratio-key
  [link-e {:keys [type attr attrs tgt]}]
  (case type
    :val-eq [type (attrs link-e) (attrs tgt)]
    :_ref   [type attr]
    [type attr]))

(defn- estimate-link-size
  [db link-e {:keys [type attr attrs tgt var]} ^ConcurrentHashMap ratios
   ^IdentityHashMap build-cache prev-size prev-plan index]
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
        (let [{:keys [^long n ^double sum]}
              (count-init-follows-summary-cached
                db build-cache sample attr index)
              mean       (if (pos? n) (/ sum (double n)) 0.0)
              base-ratio (double (db/-default-ratio db attr))
              ratio      (max mean base-ratio
                              (double c/magic-link-ratio))]
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

(defn- count-or-join-follows
  "Count the linked output of an or-join sample."
  [db sources rules ^IdentityHashMap build-cache tuples
   {:keys [clause bound-var free-vars tgt-attr]} bound-idx]
  (qresolve/or-join-count-built
    db tuples bound-idx tgt-attr
    (qresolve/or-join-build-cached build-cache sources rules tuples clause
                                   bound-var bound-idx free-vars)))

(defn- estimate-or-join-size
  [db sources rules ^ConcurrentHashMap ratios
   ^IdentityHashMap build-cache prev-plan link]
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
        (let [^long size (count-or-join-follows db sources rules build-cache
                                                sample link bound-idx)
              ratio      (max (double (/ size ssize))
                              ^double c/magic-or-join-ratio)]
          (.put ratios ratio-key ratio)
          (* ^long prev-size ratio))

        (< 0 rsize)
        (let [^long size (count-or-join-follows db sources rules build-cache
                                                result link bound-idx)
              ratio      (/ size rsize)]
          (.put ratios ratio-key ratio)
          size)

        (.containsKey ratios ratio-key)
        (* ^long prev-size ^double (.get ratios ratio-key))

        :else
        (do (.put ratios ratio-key c/magic-or-join-ratio)
            (* ^long prev-size ^double c/magic-or-join-ratio))))))

(defn- estimate-join-size
  [db sources rules link-e link ratios build-cache prev-plan index
   new-base-plan]
  (let [prev-size (:size prev-plan)
        steps     (:steps new-base-plan)]
    (case (:type link)
      :ref     [nil (estimate-scan-v-size prev-size steps)]
      :or-join (let [or-size (estimate-or-join-size db sources rules ratios
                                                    build-cache prev-plan
                                                    link)]
                 ;; or-join doesn't have new-base-plan steps to merge
                 [or-size or-size])
      ;; :_ref and :val-eq
      (let [e-size (estimate-link-size db link-e link ratios build-cache
                                       prev-size prev-plan index)]
        [e-size (estimate-scan-v-size e-size steps)]))))

(defn- estimate-link-cost
  [^long outer-size ^long result-size]
  (estimate-round
    (+ (* outer-size ^double c/magic-cost-link-probe)
       (* result-size ^double c/magic-cost-link-retrieval))))

(defn- estimate-hash-join-cost
  "Price a hash join by its dominating input or output work. The legacy input
  coefficient is a monolithic operator estimate, so adding ordinary output
  work would double count it. A many-to-many join can emit enough tuples that
  allocation and column copies dominate that estimate, however."
  ([^long left-size ^long right-size]
   (estimate-round (* ^double c/magic-cost-hash-join
                      (+ left-size right-size))))
  ([^long left-size ^long right-size ^long result-size ^long output-width]
   (let [input-cost  (* ^double c/magic-cost-hash-join
                        (+ left-size right-size))
         output-cost (* result-size
                        (+ ^double c/magic-cost-hash-join-output-tuple
                           (* ^double c/magic-cost-hash-join-output-cell
                              output-width)))]
     (estimate-round
       (max input-cost output-cost)))))

(defn- estimate-e-plan-cost
  [prev-size e-size cur-steps]
  (let [step1 (first cur-steps)]
    (if (= 1 (count cur-steps))
      (if (identical? (-type step1) :merge)
        (estimate-scan-v-cost step1 prev-size)
        (estimate-link-cost prev-size e-size))
      (+ ^long (estimate-link-cost prev-size e-size)
         ^long (estimate-scan-v-cost (peek cur-steps) e-size)))))

(defn- e-plan
  [db {:keys [steps cost size]} index link-e link new-key new-base-plan e-size
   result-size]
  (let [new-steps (:steps new-base-plan)
        last-step (peek steps)
        cur-steps
        (case (:type link)
          :ref    [(merge-scan-step db last-step index new-key new-steps)]
          :_ref   (rev-ref-plan db last-step index link new-key new-steps)
          :val-eq (val-eq-plan db last-step index link new-key new-steps))]
    (make-plan cur-steps
               (+ ^long cost ^long (estimate-e-plan-cost size e-size cur-steps))
               result-size
               (- ^long (find-index link-e (:strata last-step))))))

(defn- index-semi-join-plan
  [db prev-plan link-e link new-key new-base-plan e-size result-size]
  (let [last-step (peek (:steps prev-plan))
        index     (index-by-link (:cols last-step) link-e link)
        in-size   (:size prev-plan)]
    (when (< ^long in-size ^long result-size)
      (let [join-plan (e-plan db prev-plan index link-e link new-key
                              new-base-plan e-size result-size)
            in        (:out last-step)
            out       (if (set? in) (set new-key) new-key)
            cols      (:cols last-step)
            step      (mk-semi-join-step in out cols cols
                                         (:strata last-step)
                                         (:seen-or-joins last-step)
                                         (:steps join-plan))]
        (make-plan [step]
                   (:cost join-plan)
                   in-size
                   (:recency join-plan))))))

(defn- compare-plans
  "Compare two plans. Prefer lower cost, then lower size as tiebreaker."
  [p1 p2]
  (let [c1 ^long (:cost p1)
        c2 ^long (:cost p2)]
    (if (= c1 c2)
      (if (< ^long (:size p2) ^long (:size p1)) p2 p1)
      (if (< ^long c2 ^long c1) p2 p1))))

(defn- or-join-plan
  [base-plans new-e db sources rules ratios build-cache prev-plan link
   last-step new-key link-e]
  (let [new-base  (base-plans [new-e])
        or-size   (estimate-or-join-size db sources rules ratios build-cache
                                         prev-plan link)
        cur-steps (or-join-plan* db sources rules last-step link new-key
                                 new-base)
        or-cost   (estimate-e-plan-cost (:size prev-plan) or-size cur-steps)]
    (make-plan cur-steps
               (+ ^long (:cost prev-plan) ^long or-cost)
               or-size
               (- ^long (find-index link-e (:strata last-step))))))

(defn- binary-plan*
  [db sources rules base-plans ratios build-cache prev-plan link-e new-e link
   new-key semi-join?]
  (let [last-step (peek (:steps prev-plan))
        index     (index-by-link (:cols last-step) link-e link)
        link-type (:type link)]
    (if (identical? :or-join link-type)
      (or-join-plan base-plans new-e db sources rules ratios build-cache
                    prev-plan link last-step new-key link-e)
      (let [new-base (base-plans [new-e])
            [e-size result-size]
            (estimate-join-size db sources rules link-e link ratios build-cache
                                prev-plan index new-base)
            link-plan  (e-plan db prev-plan index link-e link new-key
                               new-base e-size result-size)
            regular    (if (and (#{:_ref :val-eq} link-type)
                                new-base
                                (<= ^long c/hash-join-min-input-size
                                    ^long (:size prev-plan)))
                         (compare-plans
                           link-plan
                           (hash-join-plan db prev-plan link-e link new-key
                                           new-base result-size))
                         link-plan)
            index-semi (when semi-join?
                         (index-semi-join-plan
                           db prev-plan link-e link new-key new-base e-size
                           result-size))]
        (reduce compare-plans regular
                (cond-> []
                  index-semi (conj index-semi)))))))

(defn- binary-plan
  [db sources rules nodes incoming-link-counts required-vars base-plans ratios
   build-cache prev-plan link-e new-e new-key]
  (let [last-step     (peek (:steps prev-plan))
        seen-or-joins (or (:seen-or-joins last-step) #{})
        links         (get-in nodes [link-e :links])
        candidate-key (juxt :recency :cost :size)]
    (reduce
      (fn [best link]
        (if (and (= new-e (:tgt link))
                 (or (not= :or-join (:type link))
                     (not (contains? seen-or-joins (:clause link)))))
          (let [new-base  (base-plans [new-e])
                semi-join?
                (semi-join-eligible? nodes incoming-link-counts required-vars
                                     prev-plan link-e new-e link new-base)
                candidate (binary-plan* db sources rules base-plans ratios
                                        build-cache prev-plan link-e new-e link
                                        new-key semi-join?)]
            (if best
              (u/min-key-comp candidate-key best candidate)
              candidate))
          best))
      nil links)))

(defn- plans
  [db sources rules nodes node-ids incoming-link-counts required-vars pairs
   base-plans prev-plans ratios build-cache]
  (persistent!
    (reduce
      (fn [plans [prev-key prev-plan]]
        (let [prev-key-set (when-not node-ids (set prev-key))]
          (reduce
            (fn [plans [link-e new-e link-id new-id]]
              (if (if node-ids
                    (and (.contains ^DPKey prev-key (int link-id))
                         (not (.contains ^DPKey prev-key (int new-id))))
                    (and (prev-key-set link-e) (not (prev-key-set new-e))))
                (let [plan-key  (if node-ids
                                  (.append ^DPKey prev-key (int new-id))
                                  (conj prev-key new-e))
                      new-key   (if node-ids
                                  (conj (:out (peek (:steps prev-plan))) new-e)
                                  plan-key)
                      cur-plan  (plans plan-key)
                      new-plan
                      (binary-plan db sources rules nodes incoming-link-counts
                                   required-vars base-plans ratios build-cache
                                   prev-plan link-e new-e new-key)]
                  (if (and new-plan
                           (or (nil? cur-plan)
                               (identical?
                                 new-plan
                                 (compare-plans cur-plan new-plan))))
                    (assoc! plans plan-key new-plan)
                    plans))
                plans))
            plans pairs)))
      (transient {}) prev-plans)))

(def ^:private connected-pairs qog/connected-pairs)

(defn- dp-node-ids
  [component]
  (when (<= (count component) Long/SIZE)
    (into {} (map-indexed (fn [i e] [e i])) component)))

(defn- dp-key
  [node-ids entities ordered?]
  (if ordered?
    (reduce (fn [key e]
              (let [node (int (node-ids e))]
                (if key
                  (.append ^DPKey key node)
                  (DPKey/ordered node))))
            nil entities)
    (DPKey/canonical
      (reduce (fn [^long members e]
                (bit-set members (long (node-ids e))))
              0 entities))))

(defn- dp-initial-plans
  [base-plans node-ids]
  (persistent!
    (reduce-kv (fn [plans [e] plan]
                 (assoc! plans (DPKey/ordered (int (node-ids e))) plan))
               (transient {}) base-plans)))

(defn- dp-pairs
  [pairs node-ids]
  (mapv (fn [[link-e new-e]]
          [link-e new-e (node-ids link-e) (node-ids new-e)])
        pairs))

(defn- incoming-link-counts
  [nodes]
  (persistent!
    (reduce-kv
      (fn [counts _ {:keys [links]}]
        (reduce (fn [counts {:keys [tgt]}]
                  (assoc! counts tgt
                          (inc (long (get counts tgt 0)))))
                counts links))
      (transient {}) nodes)))

(defn- shrink-space
  [plans node-ids]
  (persistent!
    (reduce-kv
      (fn [m k ps]
        (assoc! m (if node-ids (DPKey/canonical (long k)) k)
                (-> (peek (apply min-key (fn [p] (:cost (peek p))) ps))
                    (update :steps (fn [ss]
                                     (if (= 1 (count ss))
                                       [(update (first ss) :out set)]
                                       [(first ss)
                                        (update (peek ss) :out set)]))))))
      (transient {})
      (group-by (fn [p]
                  (if node-ids
                    (.members ^DPKey (nth p 0))
                    (set (nth p 0))))
                plans))))

(defn- dp-table-key
  [table node-ids entities]
  (if node-ids
    (dp-key node-ids entities (.isOrdered ^DPKey (ffirst table)))
    entities))

(defn- trace-steps
  [^List tables ^long n-1 node-ids]
  (let [final-plans (vals (.get tables n-1))]
    (reduce
      (fn [plans i]
        (let [table (.get tables i)
              in    (:in (first (:steps (first plans))))]
          (cons (table (dp-table-key table node-ids in)) plans)))
      [(apply min-key :cost final-plans)]
      (range (dec n-1) -1 -1))))

(defn- plan-component
  [db sources rules nodes incoming-link-counts required-vars component]
  (let [n (count component)]
    (if (= n 1)
      [(base-plan db nodes (first component) true)]
      (let [base-plans (build-base-plans db nodes component)
            node-ids   (dp-node-ids component)]
        (if (some nil? (vals base-plans))
          [nil]
          (let [raw-pairs     (connected-pairs nodes component)
                pairs         (if node-ids
                                (dp-pairs raw-pairs node-ids)
                                raw-pairs)
                initial-plans (if node-ids
                                (dp-initial-plans base-plans node-ids)
                                base-plans)
                tables        (FastList. n)
                ratios        (ConcurrentHashMap.)
                build-cache   (IdentityHashMap.)
                n-1           (dec n)
                pn            ^long (min (long c/plan-search-max)
                                         (long (u/n-permutations n 2)))]
            (.add tables initial-plans)
            (dotimes [i n-1]
              (let [plans (plans db sources rules nodes node-ids
                                 incoming-link-counts required-vars pairs
                                 base-plans (.get tables i) ratios build-cache)]
                (if (< pn (count plans))
                  (.add tables (shrink-space plans node-ids))
                  (.add tables plans))))
            (trace-steps tables n-1 node-ids)))))))

(def ^:private connected-components qog/connected-components)

(defn- build-plan*
  [db sources rules nodes required-vars]
  (let [cc              (connected-components nodes)
        incoming-counts (incoming-link-counts nodes)]
    (if (= 1 (count cc))
      [(plan-component db sources rules nodes incoming-counts required-vars
                       (first cc))]
      (map+ (bound-fn [component]
              (plan-component db sources rules nodes incoming-counts
                              required-vars component))
            cc))))

(defn- required-plan-vars
  [{:keys [parsed-q rels late-clauses optimizable-not-joins graph]} src]
  (set/union
    (set (dp/find-vars (:qfind parsed-q)))
    (set (map :symbol (:qwith parsed-q)))
    (into #{} (mapcat (comp keys :attrs)) rels)
    (qu/collect-vars late-clauses)
    (qu/collect-vars optimizable-not-joins)
    (reduce-kv (fn [vars other-src nodes]
                 (if (= src other-src)
                   vars
                   (into vars (qu/collect-vars nodes))))
               #{} graph)))

(defn- strip-step-result
  [step]
  (let [step (cond-> step
               (contains? step :tgt-steps)
               (update :tgt-steps (fn [steps]
                                    (mapv strip-step-result steps)))

               (contains? step :join-steps)
               (update :join-steps (fn [steps]
                                     (mapv strip-step-result steps))))]
    (assoc step :result nil :sample nil)))

(defn- strip-result
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
  [{:keys [graph sources rules] :as context}]
  (if graph
    (unreduced
      (reduce-kv
        (fn [c src nodes]
          (let [^DB db        (sources src)
                required-vars (required-plan-vars context src)
                k             [(.-store db) nodes required-vars]]
            (if-let [cached (.get ^LRUCache (plan-cache) k)]
              (assoc-in c [:plan src] cached)
              (let [nodes (update-nodes db nodes)
                    plans (if (< 1 (count nodes))
                            (build-plan* db sources rules nodes required-vars)
                            [[(base-plan db nodes (ffirst nodes) true)]])]
                (if (some #(some nil? %) plans)
                  (reduced (assoc c :result-set #{}))
                  (do (.put ^LRUCache (plan-cache) k (strip-result plans))
                      (assoc-in c [:plan src] plans)))))))
        context graph))
    context))

(defn- component-binds-vars?
  [plans vars]
  (when-let [step (some-> plans last :steps last)]
    (let [cols (:cols step)]
      (every? #(some? (find-index % cols)) vars))))

(defn- add-not-join-step
  [plans clause sources rules]
  (let [plans     (vec plans)
        plan-idx  (dec (count plans))
        last-plan (plans plan-idx)
        last-step (some-> last-plan :steps peek)
        vars      (get-not-join-vars clause)
        nstep     (mk-not-join-step clause vars sources rules
                                    (:out last-step) (:out last-step)
                                    (:cols last-step) (:strata last-step)
                                    (:seen-or-joins last-step))]
    (assoc plans plan-idx (update last-plan :steps conj nstep))))

(defn plan-not-joins
  "Attach optimizable not-join clauses to source plans when all join vars are
   bound by a single component. Unlinked clauses remain in :late-clauses."
  [{:keys [plan sources rules optimizable-not-joins] :as context}]
  (if (seq optimizable-not-joins)
    (let [plan'                (into {} (map (fn [[src comps]]
                                               [src (mapv vec comps)]))
                                    plan)
          [planned unlinked]
          (reduce
            (fn [[p u] clause]
              (let [src        (get-not-join-source clause)
                    vars       (get-not-join-vars clause)
                    components (get p src)]
                (if (and (seq vars) (seq components))
                  (let [idxs (keep-indexed
                               (fn [i comp]
                                 (when (component-binds-vars? comp vars) i))
                               components)]
                    (if (= 1 (count idxs))
                      (let [idx (first idxs)]
                        [(assoc-in p [src idx]
                                   (add-not-join-step
                                     (nth components idx) clause sources rules))
                         u])
                      [p (conj u clause)]))
                  [p (conj u clause)])))
            [plan' []]
            optimizable-not-joins)]
      (-> context
          (assoc :plan planned)
          (update :late-clauses into unlinked)
          (assoc :optimizable-not-joins [])))
    context))
