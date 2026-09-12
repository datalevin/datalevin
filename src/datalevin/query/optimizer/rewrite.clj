;;
;; Copyright (c) Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns ^:no-doc datalevin.query.optimizer.rewrite
  "Input substitution, equality disjunctions, unused-variable rewrites,
  and query graph construction."
  (:require
   [clojure.set :as set]
   [clojure.walk :as w]
   [datalevin.db :as db]
   [datalevin.parser :as dp]
   [datalevin.query-util :as qu]
   [datalevin.query.optimizer.estimates
    :refer [variable-ref-count]]
   [datalevin.query.optimizer.graph :as qog]
   [datalevin.query.optimizer.range :as qor]
   [datalevin.query.plan :as qplan]
   [datalevin.query.resolve :as qresolve]
   [datalevin.util :as u
    :refer [raise]])
  (:import
   [datalevin.parser And BindColl BindScalar BindTuple Constant DefaultSrc
    Function Or Variable Pattern Predicate Not RuleExpr]))

(def ^:const ^long collection-input-plugin-threshold
  128)

(defn or-join-var?
  [clause s]
  (and (list? clause)
       (= 'or-join (first clause))
       (some #(= % s) (tree-seq sequential? seq (second clause)))))

(defn find-var-symbols
  [parsed-q]
  (set (dp/find-vars (:qfind parsed-q))))

(defn with-var-symbols
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

(defn- pattern-value-form-idx
  ^long [form]
  (+ (if (and (seq form) (qu/source? (first form))) 1 0) 2))

(defn pattern-form
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

(defn get-not-join-vars
  [clause]
  (let [clause (if (qu/source? (first clause)) (next clause) clause)
        [_ vars & _] clause]
    (into [] (filter qu/binding-var?) vars)))

(defn get-not-join-source
  [clause]
  (if (qu/source? (first clause)) (first clause) '$))

(defn clause-source-symbol
  [source]
  (if (instance? DefaultSrc source) '$ (:symbol source)))

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

(defn pattern-var-symbol
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

(defn rel-bound-var?
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
