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
  "Clause resolution."
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
   [datalevin.relation :as r]
   [datalevin.rules :as rules]
   [datalevin.util :as u :refer [raise concatv]])
  (:import
   [java.util HashMap HashSet List]
   [datalevin.parser BindColl BindIgnore BindScalar BindTuple RulesVar SrcVar]
   [datalevin.relation Relation]
   [org.eclipse.collections.impl.list.mutable FastList]))

(defrecord OrJoinLink [type tgt clause bound-var free-vars tgt-attr source])

(defrecord Clause [attr val var range count pred])

(declare resolve-clause)

(defn solve-rule
  [context clause]
  (let [[rule-name & args] clause]
    (rules/solve-stratified context rule-name args resolve-clause)))

;; binding

(defn empty-rel
  ^Relation [binding]
  (let [vars (->> (dp/collect-vars-distinct binding)
                  (map :symbol))]
    (r/relation! (zipmap vars (range)) (FastList.))))

(defprotocol IBinding
  ^Relation (in->rel [binding value]))

(defn- bindtuple-attrs
  "Build attr -> index map for a tuple binding. Returns nil when binding
  contains unsupported elements or duplicate variables."
  [^BindTuple binding]
  (loop [i 0, bs (:bindings binding), attrs {}]
    (if (seq bs)
      (let [b (first bs)]
        (cond
          (instance? BindScalar b)
          (let [sym (get-in b [:variable :symbol])]
            (if (contains? attrs sym)
              nil
              (recur (inc i) (next bs) (assoc attrs sym i))))

          (instance? BindIgnore b)
          (recur (inc i) (next bs) attrs)

          :else nil))
      attrs)))

(defn- tuple-needed-indices
  "Returns an int array of indices that are needed (not BindIgnore) from a
   BindTuple. Returns nil if all indices are needed."
  [^BindTuple binding]
  (let [bs     (:bindings binding)
        n      (count bs)
        needed (int-array (keep-indexed
                            (fn [i b] (when-not (instance? BindIgnore b) i))
                            bs))]
    (when (< (alength needed) n)
      needed)))

(defn- compact-bindtuple-attrs
  "Build attr -> compact index map for a tuple binding when using needed indices.
   Maps each non-ignored variable to its position in the compact tuple."
  [^BindTuple binding]
  (loop [i 0, compact-i 0, bs (:bindings binding), attrs {}]
    (if (seq bs)
      (let [b (first bs)]
        (cond
          (instance? BindScalar b)
          (let [sym (get-in b [:variable :symbol])]
            (if (contains? attrs sym)
              nil
              (recur (inc i) (inc compact-i) (next bs)
                     (assoc attrs sym compact-i))))

          (instance? BindIgnore b)
          (recur (inc i) compact-i (next bs) attrs)

          :else nil))
      attrs)))

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

      (and (instance? java.util.List coll)
           (instance? BindTuple (:binding binding)))
      (if-let [attrs (bindtuple-attrs (:binding binding))]
        (let [^List tuples coll
              size        (.size tuples)]
          (if (zero? size)
            (empty-rel binding)
            (let [t0 (.get tuples 0)]
              (if (u/array? t0)
                (let [^objects t0 t0
                      needed     (count (:bindings (:binding binding)))]
                  (when (< (alength t0) needed)
                    (raise "Not enough elements in a collection " coll
                           " to bind tuple " (dp/source binding)
                           {:error   :query/binding
                            :value   coll
                            :binding (dp/source binding)}))
                  (r/relation! attrs tuples))
                (transduce (map #(in->rel (:binding binding) %))
                           r/sum-rel coll)))))
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
      (reduce r/prod-rel (map #(in->rel %1 %2) (:bindings binding) coll)))))

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

(defn- rel-with-attr [context sym]
  (some #(when ((:attrs %) sym) %) (:rels context)))

(defn substitute-constant [context pattern-el]
  (when (qu/binding-var? pattern-el)
    (when-some [rel (rel-with-attr context pattern-el)]
      (let [tuples (:tuples rel)]
        (when-some [tuple (first tuples)]
          (when (nil? (fnext tuples))
            (let [idx ((:attrs rel) pattern-el)]
              (if (u/array? tuple)
                (aget ^objects tuple idx)
                (get tuple idx)))))))))

(defn substitute-constants [context pattern]
  (mapv (fn [pattern-el]
          (if (qu/binding-var? pattern-el)
            (let [substituted (substitute-constant context pattern-el)]
              (if (nil? substituted) pattern-el substituted))
            pattern-el))
        pattern))

(defn- compute-rels-bound-values
  "Compute bound values for a variable from context relations."
  [context var]
  (when-some [rel (rel-with-attr context var)]
    (let [^List tuples (:tuples rel)
          n            (.size tuples)]
      (when (> n 1)
        (let [idx ((:attrs rel) var)
              res (HashSet.)]
          (dotimes [i n]
            (.add res (aget ^objects (.get tuples i) idx)))
          res)))))

(defn- bound-values
  "Extract unique values for a variable from context relations.
   Returns nil if not bound, or a set of values if bound to multiple values.
   Uses :rels-bound-cache volatile for lazy caching within a clause resolution."
  [context var]
  (when (qu/binding-var? var)
    (if-some [cache (:rels-bound-cache context)]
      (let [cached @cache]
        (if (contains? cached var)
          (get cached var)
          (let [result (or (compute-rels-bound-values context var)
                           (get (:delta-bound-values context) var))]
            (vswap! cache assoc var result)
            result)))
      (or (compute-rels-bound-values context var)
          (get (:delta-bound-values context) var)))))

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

(defn- resolve-entity-pairs
  [db entity-values]
  (keep (fn [e]
          (cond
            (integer? e)
            (when-not (neg? (long e))
              [e e])

            (or (qu/lookup-ref? e) (keyword? e))
            (when-let [eid (db/entid db e)]
              [e eid])

            :else
            nil))
        entity-values))

(def ^:const ^:private ^long multi-lookup-threshold 100000)

(defn- lookup-pattern-multi-entity
  "Perform multiple point lookups for bound entity values.
   More efficient than full table scan when entity is bound to multiple values.
   Returns tuples in format matching what full scan would return."
  [db pattern entity-pairs v-is-var?]
  (let [[_ a v] pattern
        a'      (if (keyword? a) a nil)
        v'      (if (or (qu/free-var? v) (= v '_)) nil v)
        acc     (FastList.)]
    (doseq [[e eid] entity-pairs]
      (let [tuples (db/-search-tuples db [eid a' v'])]
        (when tuples
          (let [^List ts tuples
                n        (.size ts)]
            (if v-is-var?
              (dotimes [i n]
                (let [^objects t (.get ts i)
                      result     (object-array 2)]
                  (aset result 0 e)
                  (aset result 1 (aget t 0))
                  (.add acc result)))
              (when (pos? n)
                (.add acc (object-array [e]))))))))
    acc))

(defn- lookup-pattern-multi-value
  "Perform multiple AV lookups for bound value variable.
   More efficient than full table scan when value is bound to multiple values.
   Returns tuples in format [e v] or [e] depending on pattern."
  [db pattern value-set e-is-var?]
  (let [[_ a _]   pattern
        ref-attr? (and (keyword? a) (db/ref? db a))
        acc       (FastList.)]
    (doseq [v value-set]
      (when-some [v' (if (and ref-attr?
                              (or (qu/lookup-ref? v) (keyword? v)))
                       (db/entid db v)
                       v)]
        (let [tuples (db/-search-tuples db [nil a v'])]
          (when tuples
            (let [^List ts tuples
                  n        (.size ts)]
              (if e-is-var?
                (dotimes [i n]
                  (let [^objects t (.get ts i)
                        result     (object-array 2)]
                    (aset result 0 (aget t 0))
                    (aset result 1 v)
                    (.add acc result)))
                (dotimes [_ n]
                  (.add acc (object-array [v])))))))))
    acc))

(defn lookup-pattern-db
  [context db pattern]
  (let [[e a v]           pattern
        entity-values     (when (and (qu/binding-var? e) (keyword? a))
                            (bound-values context e))
        use-entity-multi? (and entity-values
                               (<= (.size ^HashSet entity-values)
                                   multi-lookup-threshold))
        value-values      (when (and (not use-entity-multi?)
                                     (qu/binding-var? e)
                                     (qu/binding-var? v)
                                     (keyword? a))
                            (bound-values context v))
        use-value-multi?  (and value-values
                               (<= (.size ^HashSet value-values)
                                   multi-lookup-threshold))]
    (cond
      use-entity-multi?
      (let [resolved-pattern (resolve-pattern-lookup-refs db pattern)
            entity-pairs     (resolve-entity-pairs db entity-values)
            v-resolved       (nth resolved-pattern 2 nil)
            v-is-var?        (and (or (nil? v-resolved)
                                      (qu/free-var? v-resolved)
                                      (= v-resolved '_))
                                  (not (qu/placeholder? v-resolved)))
            attrs            (if (and (qu/binding-var? v) (not= v e))
                               {e 0, v 1}
                               {e 0})]
        (r/relation! attrs
                     (lookup-pattern-multi-entity db resolved-pattern
                                                  entity-pairs v-is-var?)))

      use-value-multi?
      (let [e-is-var? (qu/binding-var? e)
            attrs     (if e-is-var?
                        {e 0, v 1}
                        {v 0})]
        (r/relation! attrs
                     (lookup-pattern-multi-value db pattern value-values
                                                 e-is-var?)))

      :else
      (let [search-pattern (->> pattern
                                (substitute-constants context)
                                (resolve-pattern-lookup-refs db)
                                (mapv #(if (or (qu/free-var? %) (= % '_))
                                         nil
                                         %)))]
        (r/relation! (let [idxs (volatile! {})
                           i    (volatile! 0)]
                       (mapv (fn [p sp]
                               (when (nil? sp)
                                 (when (qu/binding-var? p)
                                   (vswap! idxs assoc p @i))
                                 (vswap! i u/long-inc)))
                             pattern search-pattern)
                       @idxs)
                     (db/-search-tuples db search-pattern))))))

(defn matches-pattern?
  [pattern tuple]
  (loop [tuple   tuple
         pattern pattern]
    (if (and tuple pattern)
      (let [t (first tuple)
            p (first pattern)]
        (if (or (= p '_) (qu/free-var? p) (= t p))
          (recur (next tuple) (next pattern))
          false))
      true)))

(defn lookup-pattern-coll
  [coll pattern]
  (r/relation! (into {}
                     (filter (fn [[s _]] (qu/binding-var? s)))
                     (map vector pattern (range)))
               (u/map-fl to-array
                         (filterv #(matches-pattern? pattern %) coll))))

(defn lookup-pattern
  [context source pattern]
  (if (db/-searchable? source)
    (lookup-pattern-db context source pattern)
    (lookup-pattern-coll source pattern)))

(defn collapse-rels
  [rels new-rel]
  (persistent!
    (loop [rels          rels
           new-rel       new-rel
           new-rel-attrs (:attrs new-rel)
           acc           (transient [])]
      (if-some [rel (first rels)]
        (if (not-empty (qu/intersect-keys new-rel-attrs (:attrs rel)))
          (let [joined (j/hash-join rel new-rel)]
            (recur (next rels) joined (:attrs joined) acc))
          (recur (next rels) new-rel new-rel-attrs (conj! acc rel)))
        (conj! acc new-rel)))))

(defn context-resolve-val
  [context sym]
  (when-some [rel (rel-with-attr context sym)]
    (when-some [^objects tuple (.get ^List (:tuples rel) 0)]
      (aget tuple ((:attrs rel) sym)))))

(defn- rel-contains-attrs?
  [rel attrs]
  (let [rel-attrs (:attrs rel)]
    (some #(rel-attrs %) attrs)))

(defn- rel-prod-by-attrs
  [context attrs]
  (let [rels       (into #{}
                         (filter #(rel-contains-attrs? % attrs))
                         (:rels context))
        production (reduce r/prod-rel rels)]
    [(update context :rels #(remove rels %)) production]))

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

(defn resolve-pred
  [f context]
  (let [fun (if (fn? f)
              f
              (or (get built-ins/query-fns f)
                  (context-resolve-val context f)
                  (dot-form f)
                  (resolve-sym f)
                  (when (nil? (rel-with-attr context f))
                    (raise "Unknown function or predicate '" f
                           {:error :query/where :var f}))))]
    (if-let [s (:pod.huahaiy.datalevin/inter-fn fun)]
      (@pod-fns s)
      fun)))

(defn -call-fn
  [context rel f args]
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
            (if-some [fn-val (or (get built-ins/query-fns arg)
                                 (resolve-sym arg))]
              (aset static-args i fn-val)
              (aset tuples-args i (get attrs arg))))

          (list? arg)
          (aset tuples-args i (-call-fn context rel (first arg) (rest arg)))

          :else
          (aset static-args i arg))))
    (fn [^objects tuple]
      (dotimes [i len]
        (when-some [tuple-arg (aget tuples-args i)]
          (aset static-args i (if (fn? tuple-arg)
                                (tuple-arg tuple)
                                (aget tuple tuple-arg)))))
      (call static-args))))

(defn filter-by-pred
  [context clause]
  (let [[[f & args]]         clause
        attrs                (qu/collect-vars args)
        [context production] (rel-prod-by-attrs context attrs)
        new-rel              (let [tuple-pred (-call-fn context production f args)]
                               (update production :tuples
                                       #(r/select-tuples tuple-pred %)))]
    (update context :rels conj new-rel)))

(defn- attach-needed-meta
  "Attach :tuple-needed metadata to the last argument or append a metadata map.
   Returns the modified args vector."
  [args ^ints needed]
  (let [v        (vec args)
        n        (count v)
        last-arg (when (pos? n) (peek v))
        meta-map (with-meta {} {:tuple-needed needed})]
    (cond
      (zero? n)
      [meta-map]

      (nil? last-arg)
      (assoc v (dec n) meta-map)

      (instance? clojure.lang.IObj last-arg)
      (assoc v (dec n) (with-meta last-arg {:tuple-needed needed}))

      :else
      (conj v meta-map))))

(defn bind-by-fn
  [context clause]
  (let [[[f & args] out]     clause
        binding              (dp/parse-binding out)
        tuple-bind?          (and (instance? BindColl binding)
                                  (instance? BindTuple (:binding binding)))
        needed               (when (and tuple-bind?
                                        (contains? tuple-producing-fns f))
                               (tuple-needed-indices (:binding binding)))
        args'                (if needed
                               (attach-needed-meta args needed)
                               args)
        attrs                (qu/collect-vars args)
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
          (let [tuple-fn (-call-fn context production f args')
                rels     (for [tuple (:tuples production)
                               :let  [val (tuple-fn tuple)]
                               :when (not (nil? val))]
                           (if needed
                             (r/prod-rel
                               (r/relation! (:attrs production)
                                            (doto (FastList.) (.add tuple)))
                               (r/relation!
                                 (compact-bindtuple-attrs (:binding binding))
                                 val))
                             (r/prod-rel
                               (r/relation! (:attrs production)
                                            (doto (FastList.) (.add tuple)))
                               (in->rel binding val))))]
            (if (empty? rels)
              (r/prod-rel production (empty-rel binding))
              (reduce r/sum-rel rels))))]
    (update context :rels collapse-rels new-rel)))

(defn dynamic-lookup-attrs
  [source pattern]
  (let [[e a v] pattern]
    (cond-> #{}
      (qu/binding-var? e)   (conj e)
      (and (qu/binding-var? v)
           (not (qu/binding-var? a))
           (db/ref? source a)) (conj v))))

(defn limit-rel
  [rel vars]
  (when-some [attrs (not-empty (select-keys (:attrs rel) vars))]
    (assoc rel :attrs attrs)))

(defn limit-context
  [context vars]
  (assoc context :rels (keep #(limit-rel % vars) (:rels context))))

(defn bound-vars
  [context]
  (into #{} (mapcat #(keys (:attrs %))) (:rels context)))

(defn check-bound
  [bound vars form]
  (when-not (set/subset? vars bound)
    (let [missing (set/difference vars bound)]
      (raise "Insufficient bindings: " missing " not bound in " form
             {:error :query/where :form form :vars missing}))))

(defn check-free-same
  [bound branches form]
  (let [free (mapv #(set/difference (qu/collect-vars %) bound) branches)]
    (when-not (apply = free)
      (raise "All clauses in 'or' must use same set of free vars, had " free
             " in " form
             {:error :query/where :form form :vars free}))))

(defn check-free-subset
  [bound vars branches]
  (let [free (into #{} (remove bound) vars)]
    (doseq [branch branches]
      (when-some [missing (not-empty
                            (set/difference free (qu/collect-vars branch)))]
        (raise "All clauses in 'or' must use same set of free vars, had "
               missing " not bound in " branch
               {:error :query/where :form branch :vars missing})))))

(defn single
  [coll]
  (assert (nil? (next coll)) "Expected single element")
  (first coll))

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
                                        (map #(reduce j/hash-join (:rels %)))
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
           vars                (into #{} (filter qu/binding-var?) vars)
           _                   (check-free-subset (bound-vars context) vars
                                                  branches)
           join-context        (limit-context context vars)]
       (update context :rels collapse-rels
               (transduce (comp (map (fn [branch]
                                       (-> join-context
                                           (resolve-clause branch)
                                           (limit-context vars))))
                                (map #(let [rels (:rels %)]
                                        (if (seq rels)
                                          (reduce j/hash-join rels)
                                          []))))
                          r/sum-rel-dedupe branches)))

     '[and *]
     (let [[_ & clauses] clause]
       (reduce resolve-clause context clauses))

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
           vars               (into #{} (filter qu/binding-var?) vars)
           bound              (bound-vars context)
           _                  (check-bound bound vars orig-clause)
           context1           (assoc context :rels
                                     [(reduce j/hash-join (:rels context))])
           join-context       (limit-context context1 vars)
           negation-context   (-> (reduce resolve-clause join-context clauses)
                                  (limit-context vars))
           neg-rel            (reduce j/hash-join (:rels negation-context))]
       (assoc context1 :rels
              [(j/subtract-rel
                 (single (:rels context1))
                 neg-rel)]))

     '[*]
     (let [source   qu/*implicit-source*
           pattern' (resolve-pattern-lookup-refs source clause)
           relation (lookup-pattern context source pattern')]
       (binding [qu/*lookup-attrs* (if (db/-searchable? source)
                                     (dynamic-lookup-attrs source pattern')
                                     qu/*lookup-attrs*)]
         (update context :rels collapse-rels relation))))))

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
  [sources rules ^List tuples clause bound-var bound-idx free-vars]
  (when (pos? (.size tuples))
    (let [bound-vals     (let [s (HashSet.)]
                           (dotimes [i (.size tuples)]
                             (.add s (aget ^objects (.get tuples i) bound-idx)))
                           (vec s))
          bound-rel      (r/relation! {bound-var 0}
                                      (let [fl (FastList.)]
                                        (doseq [v bound-vals]
                                          (.add fl (object-array [v])))
                                        fl))
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
                (dotimes [i (.size or-tuples)]
                  (let [^objects t (.get or-tuples i)
                        bv         (aget t bound-var-idx-in-or)]
                    (.putIfAbsent m bv (FastList.))
                    (.add ^List (.get m bv) t)))
                m)]
          {:or-by-bound  or-by-bound
           :free-var-idx free-var-idx
           :tuple-len    (alength ^objects (.get tuples 0))})))))

(defn or-join-execute-link
  [db sources rules ^List tuples clause bound-var bound-idx free-vars tgt-attr]
  (if-let [{:keys [or-by-bound free-var-idx tuple-len]}
           (or-join-build sources rules tuples clause bound-var bound-idx
                          free-vars)]
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
    (FastList.)))

(defn or-join-execute-link-into
  [db sources rules ^List tuples clause bound-var bound-idx free-vars tgt-attr
   sink]
  (when-let [{:keys [or-by-bound free-var-idx tuple-len]}
             (or-join-build sources rules tuples clause bound-var bound-idx
                            free-vars)]
    (when-not (.isEmpty ^HashMap or-by-bound)
      (let [pipe (p/or-join-tuple-pipe tuples bound-idx or-by-bound free-var-idx
                                       tuple-len)]
        (db/-val-eq-scan-e db pipe sink tuple-len tgt-attr))))
  sink)
