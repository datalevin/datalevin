;;
;; Copyright (c) Huahai Yang, Nikita Prokopov. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns ^:no-doc datalevin.query.resolve
  "Clause resolution, function calls, recursive branches, and indexed
  specializations."
  (:refer-clojure :exclude [update assoc])
  (:require
   [clojure.set :as set]
   [clojure.string :as str]
   [datalevin.built-ins :as built-ins]
   [datalevin.db :as db]
   [datalevin.inline :refer [update assoc]]
   [datalevin.join :as j]
   [datalevin.parser :as dp]
   [datalevin.pipe :as p]
   [datalevin.query-util :as qu]
   [datalevin.query.resolve.binding :as binding
    :refer [append-tuple-product! attach-needed-meta bind-flat-tuple-tuples
            bind-scalar-tuples compile-tuple-product tuple-list->rel]]
   [datalevin.query.resolve.context :as context
    :refer [add-resolved-relation project-visible-distinct rel-prod-by-attrs
            rel-with-attr]]
   [datalevin.query.resolve.domain :as domain
    :refer [bound-value-expansion contiguous-entity-presence-clauses
            singleton-domain-candidate-clause? singleton-domain-plan]]
   [datalevin.query.resolve.pattern :as pattern
    :refer [filter-bound-entity-presence lookup-pattern-domain-filtered-entity
            ;; Resolved here by query-not-test in the sibling test project.
            #_{:clj-kondo/ignore [:unused-referred-var]}
            lookup-pattern-multi-entity
            #_{:clj-kondo/ignore [:unused-referred-var]}
            merged-eav-lookup-threshold
            #_{:clj-kondo/ignore [:unused-referred-var]}
            multi-lookup-cheaper?
            #_{:clj-kondo/ignore [:unused-referred-var]}
            resolve-entity-pairs]]
   [datalevin.query.tuple :as qtuple]
   [datalevin.relation :as r]
   [datalevin.rules :as rules]
   [datalevin.util :as u :refer [raise concatv]])
  (:import
   [java.util HashMap HashSet IdentityHashMap List]
   [datalevin.parser BindColl BindIgnore BindScalar BindTuple RulesVar SrcVar]
   [datalevin.relation Relation]
   [org.eclipse.collections.impl.list.mutable FastList]))

(declare resolve-clause)

;; Retain the existing entry points for query callers.
(def ^Relation empty-rel binding/empty-rel)
(def substitute-constant pattern/substitute-constant)
(def substitute-constants pattern/substitute-constants)
(def resolve-pattern-lookup-refs pattern/resolve-pattern-lookup-refs)
(def lookup-pattern-db pattern/lookup-pattern-db)
(def matches-pattern? pattern/matches-pattern?)
(def lookup-pattern-coll pattern/lookup-pattern-coll)
(def lookup-pattern pattern/lookup-pattern)
(def collapse-rels context/collapse-rels)
(def context-resolve-val context/context-resolve-val)
(def dynamic-lookup-attrs pattern/dynamic-lookup-attrs)
(def limit-rel context/limit-rel)
(def limit-context context/limit-context)
(def bound-vars context/bound-vars)
(def check-bound context/check-bound)
(def check-free-same context/check-free-same)
(def check-free-subset context/check-free-subset)
(def single context/single)
(def singleton-domain-candidate-values domain/singleton-domain-candidate-values)
(def singleton-domain-planning-input? domain/singleton-domain-planning-input?)

(defrecord OrJoinLink [type tgt clause bound-var free-vars tgt-attr source])

(defrecord Clause [attr val var range count pred])

(def ^:dynamic *resolver-mode*
  "Controls host function resolution for query predicates/functions.

  :embedded preserves same-process Datalevin behavior and may resolve host vars.
  :server-safe is for client/server queries and only allows built-in query
  functions, sandboxed inter-fn values, query context values that are not used
  as call targets, and UDFs reached through the built-in udf function."
  :embedded)

(def ^:dynamic *or-join-branch-selector*
  "Optional selector used by late execution to choose among ready clauses in
  a direct `and` branch of an or-join. The function receives the current
  context, pending clauses, and ready clause indices, and returns one of those
  indices. Normal resolution retains dependency-ordered source order."
  nil)

(def ^:dynamic *not-join-prefix-specialization?*
  "Whether a correlated not-join may evaluate a reusable indexed prefix on
  distinct anchor keys before joining the remaining correlation keys back in."
  true)

(def ^:dynamic *bound-value-presence-fusion?*
  "Whether a bound-value entity expansion may apply contiguous wildcard EAV
  filters to its compact lookup relation before joining the outer payload."
  true)

(def ^:dynamic *singleton-domain-scan?*
  "Whether two indexed patterns sharing a free value may use the small value
  domain owned by a singleton entity to constrain the other pattern's scan."
  true)

(def ^:private ^:const ^long not-join-prefix-min-reuse 16)

(defn server-safe-resolver?
  []
  (= :server-safe *resolver-mode*))

(defn solve-rule
  [context clause]
  (let [[rule-name & args] clause]
    (rules/solve-stratified context rule-name args resolve-clause)))

;; binding

(defprotocol IBinding
  ^Relation (in->rel [binding value]))

(def tuple-producing-fns
  "Set of function symbols that produce tuples and can benefit from
  knowing which indices are needed."
  #{'fulltext 'idoc-match 'vec-neighbors 'embedding-neighbors})

(extend-protocol IBinding
  BindIgnore
  (in->rel [_ _]
    (r/prod-rel))

  BindScalar
  (in->rel [binding value]
    (r/relation! {(get-in binding [:variable :symbol]) 0}
                 (doto (FastList.) (.add (into-array Object [value])))))

  BindColl
  (in->rel [binding coll]
    (cond
      (instance? Relation coll) coll

      (not (u/seqable? coll))
      (raise "Cannot bind value " coll " to collection " (dp/source binding)
             {:error :query/binding, :value coll, :binding (dp/source binding)})

      (empty? coll)
      (empty-rel binding)

      (instance? BindScalar (:binding binding))
      (r/relation! {(get-in binding [:binding :variable :symbol]) 0}
                   (r/vertical-tuples coll))

      (and (instance? java.util.List coll)
           (instance? BindTuple (:binding binding)))
      (if-let [projection
               (qtuple/tuple-binding-projection (:binding binding))]
        (tuple-list->rel binding coll projection)
        (transduce (map #(in->rel (:binding binding) %)) r/sum-rel coll))

      :else
      (transduce (map #(in->rel (:binding binding) %)) r/sum-rel coll)))

  BindTuple
  (in->rel [binding coll]
    (cond
      (not (u/seqable? coll))
      (raise "Cannot bind value " coll " to tuple " (dp/source binding)
             {:error :query/binding, :value coll, :binding (dp/source binding)})

      (< (count coll) (count (:bindings binding)))
      (raise "Not enough elements in a collection " coll " to bind tuple "
             (dp/source binding)
             {:error :query/binding, :value coll, :binding (dp/source binding)})

      :else
      (reduce j/hash-join
              (map #(in->rel %1 %2) (:bindings binding) coll)))))

(defn resolve-ins
  [context values]
  (loop [context  context
         bindings (seq (get-in context [:parsed-q :qin]))
         values   (seq values)]
    (if-some [binding (first bindings)]
      (let [value (when values (first values))
            context
            (cond
              (and (instance? BindScalar binding)
                   (instance? SrcVar (:variable binding)))
              (update context :sources assoc (get-in binding [:variable :symbol])
                      value)

              (and (instance? BindScalar binding)
                   (instance? RulesVar (:variable binding)))
              (let [parsed (rules/parse-rules value)]
                (assoc context
                       :rules parsed
                       :rules-deps (rules/dependency-graph parsed)))

              :else
              (update context :rels conj (in->rel binding value)))]
        (recur context
               (next bindings)
               (when values (next values))))
      context)))

(defn dot-form [f]
  (when (and (symbol? f) (str/starts-with? (name f) "."))
    f))

(defn- dot-call
  [fname ^objects args]
  (let [obj (aget args 0)
        oc  (.getClass ^Object obj)
        as  (rest args)
        res (if (zero? (count as))
              (. (.getDeclaredMethod oc fname nil) (invoke obj nil))
              (. (.getDeclaredMethod
                   oc fname
                   (into-array Class (map #(.getClass ^Object %) as)))
                 (invoke obj (into-array Object as))))]
    (when (not= res false) res)))

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

(defn make-call
  [f]
  (if (dot-form f)
    (let [fname (subs (name f) 1)] #(dot-call fname %))
    #(opt-apply f %)))

(defn resolve-sym
  [sym]
  (when (symbol? sym)
    (when-let [v (or (resolve sym)
                     (when (find-ns 'pod.huahaiy.datalevin)
                       (ns-resolve 'pod.huahaiy.datalevin sym)))]
      @v)))

(defonce pod-fns (atom {}))

(defn- disallowed-server-query-function!
  [f]
  (raise "Server query cannot call unregistered function or predicate '" f
         {:error :query/where :var f :resolver-mode *resolver-mode*}))

(defn- resolve-built-in-query-fn
  [f]
  (get built-ins/query-fns f))

(defn- inter-fn?
  [f]
  (= (:type (meta f)) :datalevin/inter-fn))

(defn- validate-server-safe-apply!
  [f args]
  (when (and (server-safe-resolver?) (= 'apply f))
    (let [target (first args)]
      (when-not (and (symbol? target)
                     (not= 'apply target)
                     (contains? built-ins/query-fns target))
        (disallowed-server-query-function! target)))))

(defn resolve-pred
  [f context]
  (let [fun (cond
              (inter-fn? f)
              f

              (fn? f)
              (if (server-safe-resolver?)
                (disallowed-server-query-function! f)
                f)

              (resolve-built-in-query-fn f)
              (resolve-built-in-query-fn f)

              (and (not (server-safe-resolver?))
                   (context-resolve-val context f))
              (context-resolve-val context f)

              (and (server-safe-resolver?)
                   (rel-with-attr context f))
              (disallowed-server-query-function! f)

              (and (server-safe-resolver?)
                   (or (qualified-symbol? f) (dot-form f)))
              (disallowed-server-query-function! f)

              (and (not (server-safe-resolver?))
                   (dot-form f))
              (dot-form f)

              (and (not (server-safe-resolver?))
                   (resolve-sym f))
              (resolve-sym f)

              :else
              (raise "Unknown function or predicate '" f
                     {:error :query/where :var f}))]
    (if-let [s (:pod.huahaiy.datalevin/inter-fn fun)]
      (@pod-fns s)
      fun)))

(defn -call-fn
  [context rel f args]
  (validate-server-safe-apply! f args)
  (let [sources              (:sources context)
        attrs                (:attrs rel)
        len                  (count args)
        ^objects static-args (make-array Object len)
        ^objects tuples-args (make-array Object len)
        call                 (make-call (resolve-pred f context))]
    (dotimes [i len]
      (let [arg (nth args i)]
        (cond
          (symbol? arg)
          (if-some [source (get sources arg)]
            (aset static-args i source)
            (if-some [fn-val (or (resolve-built-in-query-fn arg)
                                 (when-not (server-safe-resolver?)
                                   (resolve-sym arg)))]
              (aset static-args i fn-val)
              (if (contains? attrs arg)
                (aset tuples-args i (get attrs arg))
                (when (server-safe-resolver?)
                  (disallowed-server-query-function! arg)))))

          (list? arg)
          (aset tuples-args i (-call-fn context rel (first arg) (rest arg)))

          :else
          (aset static-args i arg))))
    (let [tuple-bindings
          (into []
                (keep-indexed
                  (fn [i tuple-arg]
                    (when (and (some? tuple-arg) (not (fn? tuple-arg)))
                      [i tuple-arg])))
                tuples-args)
          nested-bindings
          (into []
                (keep-indexed
                  (fn [i tuple-arg]
                    (when (fn? tuple-arg) [i tuple-arg])))
                tuples-args)
          ^ints tuple-positions (int-array (map first tuple-bindings))
          ^ints tuple-indexes   (int-array (map (comp int second)
                                                tuple-bindings))
          ^ints nested-positions (int-array (map first nested-bindings))
          ^objects nested-fns    (object-array (map second nested-bindings))
          tuple-count            (alength tuple-positions)
          nested-count           (alength nested-positions)]
      (fn [^objects tuple]
        (dotimes [i tuple-count]
          (aset static-args (aget tuple-positions i)
                (aget tuple (aget tuple-indexes i))))
        (dotimes [i nested-count]
          (aset static-args (aget nested-positions i)
                ((aget nested-fns i) tuple)))
        (call static-args)))))

(defn filter-by-pred
  [context clause]
  (let [[[f & args]]         clause
        attrs                (qu/collect-fn-arg-vars args)
        [context production] (rel-prod-by-attrs context attrs)
        new-rel              (let [tuple-pred (-call-fn context production f args)]
                               (update production :tuples
                                       #(r/select-tuples tuple-pred %)))]
    (update context :rels conj new-rel)))

(defn- bind-coll-tuples
  [production binding projection needed tuple-fn]
  (let [^List tuples (:tuples production)
        size         (.size tuples)
        initial-res  (FastList. size)]
    (loop [i 0, ^objects product-plan nil, ^List res initial-res]
      (if (< i size)
        (let [tuple ^objects (.get tuples i)
              val   (tuple-fn tuple)]
          (if (nil? val)
            (recur (unchecked-inc-int i) product-plan res)
            (let [bound-rel
                  (cond
                    needed
                    (r/relation! (:attrs projection) val)

                    (and projection (instance? java.util.List val))
                    (tuple-list->rel binding val projection)

                    :else
                    (in->rel binding val))
                  bound-attrs (:attrs bound-rel)]
              (if (or (nil? product-plan)
                      (= (aget product-plan 0) bound-attrs))
                (let [plan (or product-plan
                               (compile-tuple-product (:attrs production)
                                                      bound-attrs))]
                  (append-tuple-product! res tuple bound-rel plan)
                  (recur (unchecked-inc-int i) plan res))
                (let [joined (j/hash-join
                               (r/relation! (:attrs production)
                                            (r/single-tuples tuple))
                               bound-rel)
                      merged (r/sum-rel
                               (r/relation! (aget product-plan 1) res)
                               joined)]
                  (recur (unchecked-inc-int i)
                         product-plan (:tuples merged)))))))
        (if product-plan
          (r/relation! (aget product-plan 1) res)
          (j/hash-join production (empty-rel binding)))))))

(defn bind-by-fn
  [context clause]
  (let [[[f & args] out]     clause
        binding              (dp/parse-binding out)
        flat-tuple-projection
        (when (instance? BindTuple binding)
          (qtuple/tuple-binding-projection binding))
        coll-tuple-bind?     (and (instance? BindColl binding)
                                  (instance? BindTuple (:binding binding)))
        projection           (when coll-tuple-bind?
                               (qtuple/tuple-binding-projection
                                 (:binding binding)))
        needed               (when (and projection
                                        (contains? tuple-producing-fns f))
                               (:needed projection))
        args'                (if needed
                               (attach-needed-meta args needed)
                               args)
        attrs                (qu/collect-fn-arg-vars args)
        [context production] (rel-prod-by-attrs context attrs)
        out-var              (when (instance? BindScalar binding)
                               (get-in binding [:variable :symbol]))
        out-idx              (when out-var (get (:attrs production) out-var))
        new-rel
        (if out-idx
          (let [tuple-fn (-call-fn context production f args')]
            (clojure.core/update
              production :tuples
              #(r/select-tuples
                 (fn [^objects tuple]
                   (let [val (tuple-fn tuple)]
                     (and (not (nil? val))
                          (= (aget tuple (int out-idx)) val))))
                 %)))
          (let [tuple-fn (-call-fn context production f args')]
            (if (instance? BindScalar binding)
              (bind-scalar-tuples production out-var tuple-fn)
              (if flat-tuple-projection
                (bind-flat-tuple-tuples
                  production binding flat-tuple-projection tuple-fn)
                (bind-coll-tuples
                  production binding projection needed tuple-fn)))))]
    (update context :rels collapse-rels new-rel)))

(defn- indexed-data-pattern?
  [clause]
  (and (vector? clause)
       (= 3 (count clause))
       (keyword? (second clause))))

(defn- not-join-prefix-plan
  "Find a conservative decorrelation boundary for a not-join. The reusable
  prefix must consist of connected indexed patterns, start from a strict
  subset of the correlation variables, produce another correlation variable,
  and leave a residual clause to evaluate after the outer keys are rejoined."
  [vars clauses]
  (when (and *not-join-prefix-specialization?*
             (< 1 (count vars))
             (indexed-data-pattern? (first clauses)))
    (let [var-set     (set vars)
          first-vars  (qu/collect-vars (first clauses))
          anchor-vars (into [] (filter first-vars) vars)
          anchor-set  (set anchor-vars)]
      (when (and (seq anchor-vars)
                 (< (count anchor-vars) (count vars)))
        (let [[prefix residual known]
              (loop [known   anchor-set
                     pending clauses
                     prefix  []]
                (if-let [clause (first pending)]
                  (let [pattern-vars (qu/collect-vars clause)]
                    (if (and (indexed-data-pattern? clause)
                             (seq (set/intersection known pattern-vars)))
                      (recur (into known pattern-vars)
                             (next pending)
                             (conj prefix clause))
                      [prefix pending known]))
                  [prefix nil known]))
              produced (set/difference (set/intersection var-set known)
                                       anchor-set)]
          (when (and (< 1 (count prefix))
                     (seq residual)
                     (seq produced))
            {:anchor-vars anchor-vars
             :prefix     prefix
             :residual   residual}))))))

(defn- resolve-not-join-negation
  [context outer-rel vars clauses]
  (let [join-context     (assoc context :rels
                                [(r/project-distinct outer-rel vars)])
        negation-context (-> (reduce resolve-clause join-context clauses)
                             (limit-context (set vars)))]
    (-> (reduce j/hash-join (:rels negation-context))
        (r/project-distinct vars))))

(defn- resolve-not-join-prefix
  [context outer-rel vars clauses]
  (when-let [{:keys [anchor-vars prefix residual]}
             (when (and qu/*implicit-source*
                        (nil? (:delta-bound-values context))
                        (db/-searchable? qu/*implicit-source*))
               (not-join-prefix-plan vars clauses))]
    (let [outer-keys  (r/project-distinct outer-rel vars)
          anchor-keys (r/project-distinct outer-keys anchor-vars)
          outer-size  (.size ^List (:tuples outer-keys))
          anchor-size (.size ^List (:tuples anchor-keys))]
      (when (and (pos? anchor-size)
                 (<= (* (long anchor-size) not-join-prefix-min-reuse)
                     (long outer-size)))
        (let [prefix-context (reduce resolve-clause
                                     (assoc context :rels [anchor-keys])
                                     prefix)
              prefix-rel     (reduce j/hash-join (:rels prefix-context))
              lookup-attrs   (into (or qu/*lookup-attrs* #{})
                                   (mapcat #(dynamic-lookup-attrs
                                              qu/*implicit-source* %))
                                   prefix)
              joined-rel     (binding [qu/*lookup-attrs* lookup-attrs]
                               (j/hash-join outer-keys prefix-rel))
              residual-ctx   (reduce resolve-clause
                                     (assoc context :rels [joined-rel])
                                     residual)]
          (-> residual-ctx
              (limit-context (set vars))
              :rels
              (#(reduce j/hash-join %))
              (r/project-distinct vars)))))))

(defn looks-like?
  [pattern form]
  (cond
    (= '_ pattern)    true
    (= '[*] pattern)  (sequential? form)
    (symbol? pattern) (= form pattern)

    (sequential? pattern)
    (if (= (last pattern) '*)
      (and (sequential? form)
           (every? (fn [[pattern-el form-el]] (looks-like? pattern-el form-el))
                   (mapv vector (butlast pattern) form)))
      (and (sequential? form)
           (= (count form) (count pattern))
           (every? (fn [[pattern-el form-el]] (looks-like? pattern-el form-el))
                   (mapv vector pattern form))))
    :else
    (pattern form)))

(defn- clause-vars [clause]
  (into #{} (filter qu/binding-var?) (nfirst clause)))

(defn- clause-binding-requirements
  [clause]
  (let [clause (if (and (sequential? clause)
                        (qu/source? (first clause)))
                 (next clause)
                 clause)
        head   (when (sequential? clause) (first clause))]
    (cond
      ;; Predicate and function expressions need their call arguments. The
      ;; function binding, if present, is an output and is not required.
      (and (vector? clause) (sequential? head))
      {:required (qu/call-vars head)}

      ;; Plain not joins on whichever variables it shares with the surrounding
      ;; context. Its existing validation requires at least one such variable.
      (= 'not head)
      {:required-any (qu/collect-vars (next clause))}

      (= 'not-join head)
      {:required (qu/collect-vars (second clause))}

      (= 'or-join head)
      (let [vars-form (second clause)
            req-form  (when (and (sequential? vars-form)
                                 (sequential? (first vars-form)))
                        (first vars-form))]
        {:required (qu/collect-vars req-form)})

      :else {})))

(defn- clause-bindings-ready?
  [bound clause]
  (let [{:keys [required required-any]}
        (clause-binding-requirements clause)]
    (and (set/subset? required bound)
         (or (empty? required-any)
             (some bound required-any)))))

(defn- resolve-singleton-domain-plan
  [context {:keys [domain consumer]}]
  (let [{domain-source :source
         domain-pattern :pattern
         owner          :entity
         value          :value
         owner-rel      :entity-rel} domain
        {consumer-source :source
         consumer-pattern :pattern
         entity            :entity
         entity-values     :entity-values} consumer
        owner-rel      (r/project-distinct owner-rel [owner])
        domain-context (binding [qu/*implicit-source* domain-source]
                         (resolve-clause (assoc context :rels [owner-rel])
                                         domain-pattern))
        domain-rel     (-> (reduce j/hash-join (:rels domain-context))
                           (r/project-distinct [owner value]))
        domain-values  (qu/relation-distinct-values domain-rel value)
        matched-rel    (r/relation!
                         {entity 0, value 1}
                         (if (zero? (.size ^HashSet domain-values))
                           (FastList.)
                           (lookup-pattern-domain-filtered-entity
                             consumer-source consumer-pattern
                             entity-values domain-values)))
        lookup-attrs   (into (or qu/*lookup-attrs* #{})
                             (concat
                               (dynamic-lookup-attrs domain-source
                                                     domain-pattern)
                               (dynamic-lookup-attrs consumer-source
                                                     consumer-pattern)))
        pair-rel       (binding [qu/*lookup-attrs* lookup-attrs]
                         (j/hash-join domain-rel matched-rel))
        context        (binding [qu/*lookup-attrs* lookup-attrs]
                         (assoc context
                                :rels (collapse-rels (:rels context) pair-rel)
                                :rels-bound-cache (volatile! {})))]
    {:context      context
     :idxs         (vec (sort [(:idx domain) (:idx consumer)]))
     :domain-size  (.size ^HashSet domain-values)
     :matched-size (.size ^List (:tuples matched-rel))}))

(defn resolve-singleton-domain-scan
  "Resolve two indexed patterns sharing a free value as a runtime-domain scan
  when one pattern is owned by a singleton entity and produces a small domain.
  The value remains in the result relation, so this is an exact join rewrite,
  not existential join elimination. Returns the context and consumed indices."
  ([context pending ^long selected-idx]
   (resolve-singleton-domain-scan
     context pending selected-idx
     (singleton-domain-candidate-values context pending)))
  ([context pending ^long selected-idx candidate-values]
   (when (and *singleton-domain-scan?*
              (not-any? r/rel-empty (:rels context))
              (singleton-domain-candidate-clause?
                context candidate-values (nth pending selected-idx)))
     (when-let [plan (singleton-domain-plan context pending selected-idx)]
       (resolve-singleton-domain-plan context plan)))))

(defn resolve-bound-value-presence-prefix
  "Resolve a bound-value entity lookup and its contiguous wildcard EAV
  filters as one compact producer. Returns the new context and consumed clause
  indices, or nil when the selected clause is not an eligible expansion."
  [context pending ^long producer-idx]
  (when (and *bound-value-presence-fusion?*
             (not-any? r/rel-empty (:rels context)))
    (let [context   (assoc context :rels-bound-cache (volatile! {}))
          expansion (bound-value-expansion context
                                           (nth pending producer-idx))
          presence  (when expansion
                      (contiguous-entity-presence-clauses
                        context expansion pending producer-idx))]
      (when (seq presence)
        (let [{:keys [source pattern]} expansion
              pattern      (resolve-pattern-lookup-refs source pattern)
              producer-rel (lookup-pattern context source pattern)
              isolated     (binding [qu/*implicit-source* source]
                             (reduce resolve-clause
                                     (assoc context :rels [producer-rel])
                                     (map :clause presence)))
              filtered-rel (reduce j/hash-join (:rels isolated))
              context      (binding [qu/*lookup-attrs*
                                     (dynamic-lookup-attrs source pattern)]
                             (assoc context :rels
                                    (collapse-rels (:rels context)
                                                   filtered-rel)))
              end-idx      (+ producer-idx (count presence))]
          {:context context
           :idxs    (vec (range producer-idx (u/long-inc end-idx)))})))))

(defn- resolve-clauses
  "Resolve conjunction clauses in dependency order, retaining source order
  among clauses whose input bindings are already available."
  ([context clauses]
   (resolve-clauses context clauses nil))
  ([context clauses selector]
   (loop [context          context
          pending          (vec clauses)
          candidate-values nil
          candidates-ready? false]
     (if (empty? pending)
       context
       (let [plan-candidates?
             (and *singleton-domain-scan?*
                  (not candidates-ready?)
                  (< 1 (count pending))
                  (singleton-domain-planning-input? context))
             candidate-values
             (if plan-candidates?
               (singleton-domain-candidate-values context clauses)
               candidate-values)
             candidates-ready? (or candidates-ready? plan-candidates?)
             bound (bound-vars context)
               ready (into []
                           (keep-indexed
                             (fn [i clause]
                               (when (clause-bindings-ready? bound clause) i)))
                           pending)
               selected (when (and selector (seq ready))
                          (selector context pending ready))
               idx (if (some #{selected} ready) selected (first ready))]
           (if (some? idx)
             (if-let [{next-context :context consumed :idxs}
                      (or (resolve-bound-value-presence-prefix
                            context pending idx)
                          (when (seq candidate-values)
                            (resolve-singleton-domain-scan
                              context pending idx candidate-values)))]
               (recur next-context
                      (u/remove-idxs (set consumed) pending)
                      candidate-values candidates-ready?)
               (recur (resolve-clause context (nth pending idx))
                      (u/vec-remove pending idx)
                      candidate-values candidates-ready?))
             ;; Preserve the resolver's detailed insufficient-binding error
             ;; when the conjunction has no clause capable of making progress.
             (reduce resolve-clause context pending)))))))

(defn resolve-branch-clauses
  "Resolve a conjunction using the active late or-join branch selector."
  [context clauses]
  (resolve-clauses context clauses *or-join-branch-selector*))

(defn resolve-or-join-branch-relations
  "Resolve each branch of a simple or-join to a projected, branch-distinct
  relation without materializing the union between branches."
  [context vars branches]
  (let [vars         (into #{} (filter qu/binding-var?) vars)
        _            (check-free-subset (bound-vars context) vars branches)
        join-context (limit-context context vars)]
    (into []
          (comp (map (fn [branch]
                       (-> (if (and *or-join-branch-selector*
                                    (sequential? branch)
                                    (= 'and (first branch)))
                             (resolve-branch-clauses join-context (next branch))
                             (resolve-clause join-context branch))
                           (limit-context vars))))
                (map #(let [rels (:rels %)]
                        (if (seq rels)
                          (-> (reduce j/hash-join rels)
                              project-visible-distinct)
                          []))))
          branches)))

(defn union-or-join-branch-relations
  "Materialize the exact union of already projected or-join branches."
  [branch-rels]
  (transduce identity r/sum-rel-dedupe branch-rels))

(defn resolve-or-join-relation
  "Resolve the branches of a simple or-join to their exact projected union.
  The returned relation has not yet been joined back into the outer context,
  which lets terminal consumers reduce it at the correlation boundary."
  [context vars branches]
  (union-or-join-branch-relations
    (resolve-or-join-branch-relations context vars branches)))

(defn -resolve-clause
  ([context clause]
   (-resolve-clause context clause clause))
  ([context clause orig-clause]
   (condp looks-like? clause
     [[symbol? '*]]
     (do
       (check-bound (bound-vars context) (clause-vars clause) clause)
       (filter-by-pred context clause))

     [[fn? '*]]
     (do
       (check-bound (bound-vars context) (clause-vars clause) clause)
       (filter-by-pred context clause))

     [[symbol? '*] '_]
     (do
       (check-bound (bound-vars context) (clause-vars clause) clause)
       (bind-by-fn context clause))

     [[fn? '*] '_]
     (do
       (check-bound (bound-vars context) (clause-vars clause) clause)
       (bind-by-fn context clause))

     [qu/source? '*]
     (let [[source-sym & rest] clause]
       (binding [qu/*implicit-source* (get (:sources context) source-sym)]
         (-resolve-clause context rest clause)))

     '[or *]
     (let [[_ & branches] clause
           _              (check-free-same (bound-vars context) branches clause)
           contexts       (map #(resolve-clause context %) branches)]
       (assoc (first contexts) :rels [(transduce
                                        (map #(-> (reduce j/hash-join (:rels %))
                                                  project-visible-distinct))
                                        r/sum-rel-dedupe
                                        contexts)]))

     '[or-join [[*] *] *]
     (let [[_ [req-vars & vars] & branches] clause
           req-vars                         (into #{} (filter qu/binding-var?)
                                                     req-vars)
           bound                            (bound-vars context)]
       (check-bound bound req-vars orig-clause)
       (check-free-subset bound vars branches)
       (recur context (list* 'or-join (concatv req-vars vars) branches) clause))

     '[or-join [*] *]
     (let [[_ vars & branches] clause
           union-rel           (resolve-or-join-relation context vars branches)]
       (update context :rels collapse-rels
               union-rel))

     '[and *]
     (let [[_ & clauses] clause]
       (resolve-clauses context clauses))

     '[not *]
     (let [[_ & clauses] clause
           bound         (bound-vars context)
           negation-vars (qu/collect-vars clauses)
           _             (when (empty? (u/intersection bound negation-vars))
                           (raise "Insufficient bindings: none of "
                                  negation-vars " is bound in " orig-clause
                                  {:error :query/where :form orig-clause}))
           context1      (assoc context :rels
                                [(reduce j/hash-join (:rels context))])]
       (assoc context1 :rels
              [(j/subtract-rel
                 (single (:rels context1))
                 (reduce j/hash-join
                         (:rels (reduce resolve-clause context1 clauses))))]))

     '[not-join [*] *]
     (let [[_ vars & clauses] clause
           vars               (into []
                                    (comp (filter qu/binding-var?) (distinct))
                                    vars)
           var-set            (set vars)
           bound              (bound-vars context)
           _                  (check-bound bound var-set orig-clause)
           context1           (assoc context :rels
                                     [(reduce j/hash-join (:rels context))])
           outer-rel          (single (:rels context1))
           neg-rel            (or (resolve-not-join-prefix
                                    context1 outer-rel vars clauses)
                                  (resolve-not-join-negation
                                    context1 outer-rel vars clauses))]
       (assoc context1 :rels
              [(j/subtract-rel
                 outer-rel
                 neg-rel)]))

     '[*]
     (let [source   qu/*implicit-source*
           pattern' (resolve-pattern-lookup-refs source clause)]
       (if-let [filtered-context
                (when (satisfies? db/ITuples source)
                  (filter-bound-entity-presence context source pattern'))]
         filtered-context
         (let [relation (lookup-pattern context source pattern')]
           (binding [qu/*lookup-attrs* (if (db/-searchable? source)
                                         (dynamic-lookup-attrs source pattern')
                                         qu/*lookup-attrs*)]
             (add-resolved-relation context relation))))))))

(defn resolve-clause
  [context clause]
  (let [context (assoc context :rels-bound-cache (volatile! {}))]
    (if (some r/rel-empty (:rels context))
      (assoc context :rels
             [(r/relation!
                (zipmap (mapcat #(keys (:attrs %)) (:rels context)) (range))
                (FastList.))])
      (if (qu/rule? context clause)
        (if (qu/source? (first clause))
          (binding [qu/*implicit-source* (get (:sources context) (first clause))]
            (resolve-clause context (next clause)))
          (update context :rels collapse-rels (solve-rule context clause)))
        (-resolve-clause context clause)))))

(defn or-join-build
  ([sources rules ^List tuples clause bound-var bound-idx free-vars]
   (or-join-build sources rules tuples clause bound-var bound-idx free-vars
                  nil))
  ([sources rules ^List tuples clause bound-var bound-idx free-vars
    capture-domain]
   (when (pos? (.size tuples))
     (let [bound-rel      (r/relation!
                            {bound-var 0}
                            (let [seen (HashSet.)
                                  res  (FastList.)]
                              (dotimes [i (.size tuples)]
                                (let [v (aget ^objects (.get tuples i)
                                              bound-idx)]
                                  (when (.add seen v)
                                    (.add res (object-array [v])))))
                              res))
           or-context     {:sources sources
                           :rules   rules
                           :rels    [bound-rel]}
           result-context (binding [qu/*implicit-source* (get sources '$)]
                            (resolve-clause or-context clause))
           result-rels    (:rels result-context)]
       (when (seq result-rels)
         (let [or-result-rel       (if (< 1 (count result-rels))
                                     (reduce j/hash-join result-rels)
                                     (first result-rels))
               or-attrs            (:attrs or-result-rel)
               or-tuples           ^List (:tuples or-result-rel)
               free-var            (first free-vars)
               free-var-idx        (or-attrs free-var)
               bound-var-idx-in-or (or-attrs bound-var)
               or-by-bound
               (let [m (HashMap.)]
                 (try
                   (dotimes [i (.size or-tuples)]
                     (let [^objects t (.get or-tuples i)
                           bv         (aget t bound-var-idx-in-or)
                           fv         (aget t free-var-idx)]
                       (when capture-domain
                         (capture-domain fv))
                       (.putIfAbsent m bv (FastList.))
                       (.add ^List (.get m bv) t)))
                   (finally
                     (when capture-domain
                       (capture-domain))))
                 m)]
           {:or-by-bound   or-by-bound
            :free-var-idx free-var-idx
            :tuple-len    (alength ^objects (.get tuples 0))}))))))

(defn or-join-build-cached
  "Reuse an or-join build for the same input list and immutable link shape."
  [^IdentityHashMap cache sources rules ^List tuples clause bound-var bound-idx
   free-vars]
  (let [^HashMap builds (or (.get cache tuples)
                            (let [m (HashMap.)]
                              (.put cache tuples m)
                              m))
        build-key       [clause bound-var bound-idx free-vars]]
    (if (.containsKey builds build-key)
      (.get builds build-key)
      (let [built (or-join-build sources rules tuples clause bound-var
                                 bound-idx free-vars)]
        (.put builds build-key built)
        built))))

(defn or-join-execute-link
  ([db sources rules ^List tuples clause bound-var bound-idx free-vars
    tgt-attr]
   (or-join-execute-link db sources rules tuples clause bound-var bound-idx
                         free-vars tgt-attr nil))
  ([db sources rules ^List tuples clause bound-var bound-idx free-vars tgt-attr
    capture-domain]
   (if-let [{:keys [or-by-bound free-var-idx tuple-len]}
            (or-join-build sources rules tuples clause bound-var bound-idx
                           free-vars capture-domain)]
     (let [size   (.size tuples)
           joined (FastList. size)]
       (dotimes [i size]
         (let [^objects in-tuple (.get tuples i)
               bv                (aget in-tuple bound-idx)]
           (when-let [^List or-matches (.get ^HashMap or-by-bound bv)]
             (dotimes [j (.size or-matches)]
               (let [^objects or-tuple (.get or-matches j)
                     fv                (aget or-tuple free-var-idx)
                     joined-tuple      (object-array (inc ^long tuple-len))]
                 (System/arraycopy in-tuple 0 joined-tuple 0 tuple-len)
                 (aset joined-tuple tuple-len fv)
                 (.add joined joined-tuple))))))
       (if (zero? (.size joined))
         (FastList.)
         (db/-val-eq-scan-e-list db joined tuple-len tgt-attr)))
     (FastList.))))

(defn or-join-count-built
  "Count target tuples from an existing or-join build."
  [db ^List tuples bound-idx tgt-attr built]
  (if-let [{:keys [or-by-bound free-var-idx]} built]
    (let [size    (.size tuples)
          fanouts (HashMap.)]
      (loop [i     (long 0)
             total (long 0)]
        (if (< i size)
          (let [^objects in-tuple (.get tuples i)
                bv                (aget in-tuple bound-idx)
                ^List matches     (.get ^HashMap or-by-bound bv)
                total
                (if matches
                  (loop [j     (long 0)
                         total (long total)]
                    (if (< j (.size matches))
                      (let [^objects match (.get matches j)
                            fv             (aget match free-var-idx)
                            cached         (.get fanouts fv)
                            fanout         (if cached
                                             (long cached)
                                             (let [n (long
                                                       (db/-count
                                                         db [nil tgt-attr fv]
                                                         Long/MAX_VALUE))]
                                               (.put fanouts fv n)
                                               n))]
                        (recur (unchecked-inc j)
                               (unchecked-add total fanout)))
                      total))
                  total)]
            (recur (unchecked-inc i) (long total)))
          total)))
    0))

(defn or-join-count-link
  "Count linked or-join tuples without materializing the final target tuples."
  [db sources rules ^List tuples clause bound-var bound-idx free-vars tgt-attr]
  (or-join-count-built
    db tuples bound-idx tgt-attr
    (or-join-build sources rules tuples clause bound-var bound-idx free-vars)))

(defn or-join-execute-link-into
  ([db sources rules ^List tuples clause bound-var bound-idx free-vars tgt-attr
    sink]
   (or-join-execute-link-into db sources rules tuples clause bound-var
                              bound-idx free-vars tgt-attr sink nil))
  ([db sources rules ^List tuples clause bound-var bound-idx free-vars tgt-attr
    sink capture-domain]
   (when-let [{:keys [or-by-bound free-var-idx tuple-len]}
              (or-join-build sources rules tuples clause bound-var bound-idx
                             free-vars capture-domain)]
     (when-not (.isEmpty ^HashMap or-by-bound)
       (let [pipe (p/or-join-tuple-pipe tuples bound-idx or-by-bound
                                        free-var-idx tuple-len)]
         (db/-val-eq-scan-e db pipe sink tuple-len tgt-attr))))
   sink))
