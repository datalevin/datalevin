;;
;; Copyright (c) Huahai Yang, Nikita Prokopov. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns ^:no-doc datalevin.query.aggregate
  "Aggregate results"
  (:require
   [datalevin.built-ins :as built-ins]
   [datalevin.parser :as dp]
   [datalevin.query.resolve :as qresolve]
   [datalevin.relation :as r]
   [datalevin.timeout :as timeout]
   [datalevin.util :as u :refer [raise]])
  (:import
   [datalevin.parser Constant PlainSymbol SrcVar Variable]
   [datalevin.utl GroupedAggregate NumberOps]
   [java.util Map Set List]
   [datalevin.utl QueryKey$KeyMap QueryKey$KeySet]
   [org.eclipse.collections.impl.list.mutable FastList]))

(def ^:dynamic *keyed-group-reduction?*
  "Whether a terminal high-fanout fragment may feed an exact keyed aggregate
  reducer instead of materializing its payload join and final grouping input."
  true)

(def ^:dynamic *keyed-group-reduction-min-input*
  "Minimum terminal-fragment cardinality before keyed reduction is selected."
  4096)

(def ^:dynamic *keyed-group-reduction-min-ratio*
  "Minimum fragment rows per outer row before keyed reduction is selected."
  4.0)

(def ^:dynamic *keyed-group-virtual-producers?*
  "Whether terminal nested unions may expose constant payload columns to the
  keyed reducer without first materializing those columns as relations."
  true)

(defprotocol IKeyedReductionSink
  (-accept-keyed! [sink group-key identity-key value])
  (-fork-keyed [sink])
  (-merge-keyed! [sink other])
  (-keyed-stats [sink])
  (-keyed-relation [sink]))

(defn- add-sum-values
  [a b]
  (NumberOps/add a b))

(defn- sums-relation
  [group-vars value-var ^Map sums]
  (let [vars   (conj group-vars value-var)
        attrs  (zipmap vars (range))
        tuples (FastList. (.size sums))]
    (doseq [[group-key value] sums]
      (let [tuple (object-array (count vars))]
        (dotimes [i (count group-vars)]
          (aset tuple i (nth group-key i)))
        (aset tuple (count group-vars) value)
        (.add tuples tuple)))
    (r/relation! attrs tuples)))

(defn- mapped-sums-relation
  [group-vars value-var ^Map sums ^Map outer-groups]
  (let [vars   (conj group-vars value-var)
        attrs  (zipmap vars (range))
        tuples (FastList. (.size sums))]
    (doseq [[outer-key value] sums]
      (let [group-key (first (.get outer-groups outer-key))
            tuple     (object-array (count vars))]
        (dotimes [i (count group-vars)]
          (aset tuple i (nth group-key i)))
        (aset tuple (count group-vars) value)
        (.add tuples tuple)))
    (r/relation! attrs tuples)))

(deftype DistinctSumSink [group-vars value-var
                          ^Map sums ^Map identities]
  IKeyedReductionSink
  (-accept-keyed! [_ group-key identity-key value]
    ;; Current Datalog aggregation first deduplicates the complete find/with
    ;; identity, then removes :with columns and aggregates. Keep the logical
    ;; result group separate from the identity suffix so different physical
    ;; outer keys mapping to the same projected group still deduplicate.
    (let [key [group-key identity-key]]
      (when-not (.containsKey identities key)
        (.put identities key value)
        (.put sums group-key
              (if (.containsKey sums group-key)
                (add-sum-values (.get sums group-key) value)
                value))
        true)))
  (-fork-keyed [_]
    (DistinctSumSink. group-vars value-var (QueryKey$KeyMap.) (QueryKey$KeyMap.)))
  (-merge-keyed! [this other]
    (doseq [[key value] (.-identities ^DistinctSumSink other)]
      (-accept-keyed! this (nth key 0) (nth key 1) value))
    this)
  (-keyed-stats [_]
    {:identity-count (.size identities)
     :group-count    (.size sums)})
  (-keyed-relation [_]
    (sums-relation group-vars value-var sums)))

(defn distinct-sum-sink
  "Construct a forkable reducer that implements the query engine's exact
  set-before-sum semantics. `identity-key` supplied to the sink must contain
  the aggregate input and every :with value."
  [group-vars value-var]
  (DistinctSumSink. (vec group-vars) value-var (QueryKey$KeyMap.) (QueryKey$KeyMap.)))

(defn- tuple-values
  [^objects tuple ^ints idxs]
  (let [n (alength idxs)]
    (loop [i      (long 0)
           values (transient [])]
      (if (== i n)
        (persistent! values)
        (recur (unchecked-inc i)
               (conj! values (aget tuple (aget idxs (int i)))))))))

(defn- planned-values
  [^objects tuple plan]
  (persistent!
    (reduce
      (fn [values [idx constant? constant]]
        (conj! values (if constant?
                        constant
                        (aget tuple (int idx)))))
      (transient [])
      plan)))

(defn- planned-array!
  ^objects [^objects tuple plan ^objects scratch]
  (loop [i    (long 0)
         plan (seq plan)]
    (if-let [[idx constant? constant] (first plan)]
      (do
        (aset scratch (int i)
              (if constant? constant (aget tuple (int idx))))
        (recur (u/long-inc i) (next plan)))
      scratch)))

(defn- value-plan
  [attrs constants var]
  (if (contains? constants var)
    [-1 true (get constants var)]
    [(long (attrs var)) false nil]))

(defn- outer-group-state
  [outer-rel outer-key-vars group-vars]
  (let [outer-attrs       (:attrs outer-rel)
        ^ints outer-key-i (int-array (map outer-attrs outer-key-vars))
        ^ints group-i     (int-array (map outer-attrs group-vars))
        outer-groups      (QueryKey$KeyMap.)
        group-outers      (QueryKey$KeyMap.)
        injective?        (volatile! true)
        ^List outer-ts    (:tuples outer-rel)]
    (dotimes [i (.size outer-ts)]
      (let [^objects tuple (.get outer-ts i)
            outer-key     (tuple-values tuple outer-key-i)
            group-key     (tuple-values tuple group-i)
            groups        (or (.get outer-groups outer-key)
                              (let [groups (QueryKey$KeySet.)]
                                (.put outer-groups outer-key groups)
                                groups))]
        (when (.add ^Set groups group-key)
          (when (< 1 (.size ^Set groups))
            (vreset! injective? false))
          (if (.containsKey group-outers group-key)
            (when (not= outer-key (.get group-outers group-key))
              (vreset! injective? false))
            (.put group-outers group-key outer-key)))))
    {:outer-groups outer-groups
     :injective?   @injective?
     :outer-count  (.size outer-ts)}))

(defn- producer-plan
  [producer outer-key-vars with-vars value-var]
  (let [{:keys [rel constants distinct?]}
        (if (contains? producer :rel)
          (merge {:constants {} :distinct? false} producer)
          {:rel producer :constants {} :distinct? true})
        attrs         (:attrs rel)
        identity-vars (into (vec outer-key-vars)
                            (conj (vec with-vars) value-var))
        identity-idxs (mapv attrs identity-vars)
        identity-plan (mapv #(value-plan attrs constants %)
                            (conj (vec with-vars) value-var))
        physical-plan (mapv #(value-plan attrs constants %) identity-vars)
        [value-i value-constant? constant-value]
        (value-plan attrs constants value-var)]
    {:rel                   rel
     :distinct?             distinct?
     :outer-key-i           (int-array (map attrs outer-key-vars))
     :identity-plan         identity-plan
     :physical-plan         physical-plan
     :physical-identity-direct?
     (and (empty? constants)
          (= (count attrs) (count identity-vars))
          (= identity-idxs (vec (range (count identity-vars)))))
     :value-i               (int value-i)
     :value-constant?       value-constant?
     :constant-value        constant-value}))

(defn- physical-identity-tuple
  ^objects [^objects tuple plan direct? ^objects scratch]
  (if direct?
    tuple
    (planned-array! tuple plan scratch)))

(defn- producer-value
  [^objects tuple ^long value-i value-constant? constant-value]
  (if value-constant? constant-value (aget tuple (int value-i))))

(defn- sum-injective-producers
  [producer-plans ^Map outer-groups group-vars value-var
   input-count producer-disjoint?]
  (let [cross-producer? (< 1 (count producer-plans))
        need-seen?      (and cross-producer? (not producer-disjoint?))
        ^Set seen   (when need-seen?
                          (QueryKey$KeySet. (int (min (long Integer/MAX_VALUE)
                                              (long input-count)))))
        seen-lookup     (when need-seen? (r/array-lookup))
        sums            (QueryKey$KeyMap.)
        ^longs accepted (long-array 1)]
    (doseq [{:keys [rel distinct? outer-key-i physical-plan
                    physical-identity-direct? value-i value-constant?
                    constant-value]}
            producer-plans]
      (let [^List tuples (:tuples rel)
            ^ints outer-key-i outer-key-i
            producer-seen? (and (not need-seen?) (not distinct?))
            ^Set producer-seen
            (when producer-seen?
              (QueryKey$KeySet. (int (min (long Integer/MAX_VALUE)
                                  (long (.size tuples))))))
            producer-lookup (when producer-seen? (r/array-lookup))
            identity-seen   (or seen producer-seen)
            identity-lookup (or seen-lookup producer-lookup)
            check-identity? (boolean identity-seen)
            scratch (when (and check-identity?
                               (not physical-identity-direct?))
                      (object-array (count physical-plan)))]
        (dotimes [i (.size tuples)]
          (let [^objects tuple (.get tuples i)
                identity
                (when check-identity?
                  (physical-identity-tuple tuple physical-plan
                                           physical-identity-direct? scratch))
                unique?
                (or (not check-identity?)
                    (let [lookup (r/reset-array-lookup! identity-lookup
                                                        identity)]
                      (when-not (.contains identity-seen lookup)
                        (.add identity-seen
                              (r/wrap-array
                                (if physical-identity-direct?
                                  identity
                                  (aclone ^objects identity))))
                        true)))]
            (when unique?
              (let [outer-key (tuple-values tuple outer-key-i)
                    groups    (.get outer-groups outer-key)]
                (when groups
                  (let [value (producer-value tuple value-i value-constant?
                                              constant-value)]
                    (aset accepted 0 (unchecked-inc (aget accepted 0)))
                    ;; Aggregate on the narrower physical key and map it to
                    ;; the logical group only once when emitting each group.
                    (.put sums outer-key
                          (if (.containsKey sums outer-key)
                            (add-sum-values (.get sums outer-key) value)
                            value))))))))))
    {:relation       (mapped-sums-relation
                       group-vars value-var sums outer-groups)
     :identity-count (aget accepted 0)
     :group-count    (.size sums)
     :identity-mode  (cond
                       (not cross-producer?) :producer-unique
                       producer-disjoint?    :producer-disjoint
                       :else                 :cross-producer-seen-set)}))

(defn- sum-noninjective-producers
  [producer-plans ^Map outer-groups group-vars value-var]
  (let [sink (distinct-sum-sink group-vars value-var)]
    (doseq [{:keys [rel outer-key-i identity-plan value-i value-constant?
                    constant-value]} producer-plans]
      (let [^List tuples (:tuples rel)
            ^ints outer-key-i outer-key-i]
        (dotimes [i (.size tuples)]
          (let [^objects tuple (.get tuples i)
                outer-key     (tuple-values tuple outer-key-i)
                groups        (.get outer-groups outer-key)]
            (when groups
              (let [identity-key (planned-values tuple identity-plan)
                    value        (producer-value tuple value-i value-constant?
                                                 constant-value)]
                (doseq [group-key groups]
                  (-accept-keyed! sink group-key identity-key value))))))))
    (merge {:relation      (-keyed-relation sink)
            :identity-mode :seen-set}
           (-keyed-stats sink))))

(defn keyed-sum-producers
  "Feed terminal producer relations into an exact keyed sum. A producer map
  may provide virtual constant columns with `:constants`; when `:distinct?` is
  false, identities are deduplicated in the reducer rather than by first
  materializing a projected relation."
  [outer-rel producers
   {:keys [outer-key-vars group-vars with-vars value-var
           producer-disjoint?]}]
  (let [producers (into []
                        (filter #(some? (:tuples (or (:rel %) %))))
                        producers)
        producer-plans (mapv #(producer-plan % outer-key-vars with-vars
                                             value-var)
                             producers)
        input-count (reduce
                      (fn [^long n {:keys [rel]}]
                        (unchecked-add n
                                       (long (.size ^List (:tuples rel)))))
                      0 producer-plans)
        {:keys [outer-groups injective? outer-count]}
        (outer-group-state outer-rel outer-key-vars group-vars)
        reduction
        (if injective?
          (sum-injective-producers producer-plans outer-groups group-vars
                                   value-var input-count producer-disjoint?)
          (sum-noninjective-producers producer-plans outer-groups group-vars
                                      value-var))]
    (merge {:input-count    input-count
            :outer-count    outer-count
            :producer-count (count producers)}
           reduction)))

(defn keyed-sum-relations
  "Feed projected, branch-distinct terminal producers directly into an exact
  keyed sum. Cross-producer duplicates are removed in the sink, so the caller
  need not materialize their union."
  [outer-rel fragment-rels plan]
  (keyed-sum-producers outer-rel fragment-rels plan))

(defn keyed-sum-relation
  "Reduce one materialized, already-distinct terminal fragment."
  [outer-rel fragment-rel plan]
  (keyed-sum-relations outer-rel [fragment-rel] plan))

(defprotocol IContextResolve
  (-context-resolve [var context]))

(extend-protocol IContextResolve
  Variable
  (-context-resolve [var context]
    (qresolve/context-resolve-val context (.-symbol var)))
  SrcVar
  (-context-resolve [var context]
    (get-in context [:sources (.-symbol var)]))
  PlainSymbol
  (-context-resolve [var _]
    (let [sym (.-symbol var)]
      (or (get built-ins/aggregates sym)
          (when-not (qresolve/server-safe-resolver?)
            (qresolve/resolve-sym sym))
          (when (qresolve/server-safe-resolver?)
            (raise
              "Server query cannot call unregistered aggregate function '" sym
              {:error :query/where
               :var sym
               :resolver-mode qresolve/*resolver-mode*})))))
  Constant
  (-context-resolve [var _]
    (.-value var)))

(defn- resolve-aggregate-fn
  [element context]
  (let [fn-expr (:fn element)
        f       (-context-resolve fn-expr context)]
    (when (and (qresolve/server-safe-resolver?)
               (instance? Variable fn-expr))
      (let [sym (.-symbol ^Variable fn-expr)]
        (raise
          "Server query cannot call unregistered aggregate function '" sym
          {:error :query/where
           :var sym
           :resolver-mode qresolve/*resolver-mode*})))
    f))

(defn- compile-aggregate
  [element context]
  [(resolve-aggregate-fn element context)
   (mapv #(-context-resolve % context) (butlast (:args element)))])

(defn- compile-aggregates
  [find-elements context]
  (into {}
        (map (fn [element]
               [element (compile-aggregate element context)]))
        (dp/collect dp/aggregate? find-elements)))

(def ^:dynamic ^:no-doc *unique-group-aggregation?* true)

(defn aggregate-relation
  "Reduce a relation directly when a retained unique key proves the complete
  find/with input distinct. Return nil for unsupported aggregate expressions;
  custom functions and controlled callers keep their collection contracts."
  [find-elements context rel symbols keep-group?]
  (when (and *unique-group-aggregation?* (r/unique-on? rel symbols))
    (let [attrs (:attrs rel)
          plans (mapv
                  (fn [element]
                    (cond
                      (instance? Variable element)
                      [(attrs (:symbol element)) GroupedAggregate/KEY]

                      (and (dp/aggregate? element)
                           (= 1 (count (:args element)))
                           (instance? Variable (first (:args element))))
                      (let [f (resolve-aggregate-fn element context)
                            op (cond
                                 (identical? f (built-ins/aggregates 'sum)) GroupedAggregate/SUM
                                 (identical? f (built-ins/aggregates 'avg)) GroupedAggregate/AVG
                                 (identical? f (built-ins/aggregates 'count)) GroupedAggregate/COUNT)]
                        (when op [(attrs (:symbol (first (:args element)))) op]))))
                  find-elements)]
      (when (and (every? #(and % (some? (first %))) plans)
                 (every? #(contains? attrs %) symbols))
        (let [accumulator (GroupedAggregate. (int-array (map first plans))
                                              (int-array (map second plans)))
              ^List tuples (:tuples rel)]
          (dotimes [i (.size tuples)]
            (when (zero? (bit-and i 4095)) (timeout/assert-time-left))
            (.add accumulator (.get tuples i)))
          ;; Reject groups before allocating their public result vectors.
          {:rows (if keep-group?
                   (sequence (comp (filter keep-group?) (map vec))
                             (.results accumulator))
                   (map vec (.results accumulator)))
           :input-tuples (.size tuples)
           :group-count (.groupCount accumulator)})))))

(defn- checked-map
  "Keep checks between individual operations, including over chunked inputs."
  [f check! coll]
  (lazy-seq
    (when-let [items (seq coll)]
      (check!)
      (cons (f (first items)) (checked-map f check! (rest items))))))

(deftype ^:private ProjectedAggregateColumn [tuples ^long idx]
  clojure.lang.IReduceInit
  (reduce [_ rf init]
    (reduce (fn [acc tuple] (rf acc (nth tuple idx))) init tuples))
  clojure.lang.Counted
  (count [_] (count tuples)))

(defn- compute-aggregate
  "Compute an aggregate over tuples at the given tuple index."
  [element aggregate-plans tuples tuple-idx]
  (let [[f args check!] (aggregate-plans element)
        project #(nth % tuple-idx)]
    (if (and (nil? check!) (empty? args)
             (or (identical? f (built-ins/aggregates 'sum))
                 (identical? f (built-ins/aggregates 'avg))
                 (identical? f (built-ins/aggregates 'count))))
      ;; Keep the exact built-in arithmetic (including decimal/rational avg),
      ;; while reducing and counting a projection without a lazy value sequence.
      ;; Custom/wrapped functions keep their collection contract below.
      (f (ProjectedAggregateColumn. tuples tuple-idx))
      (let [vals (if check! (checked-map project check! tuples)
                     (map project tuples))]
        (apply f (conj args vals))))))

(defn- eval-find-expr
  "Evaluate a FindExpr by computing its inner aggregates and applying the
  operator."
  [expr context tuples var->idx aggregate-plans]
  (let [op   (get built-ins/query-fns (:symbol (:fn expr)))
        args (mapv (fn [arg]
                     (cond
                       (dp/aggregate? arg)
                       (let [var-sym (-> arg :args last :symbol)
                             idx     (get var->idx var-sym)]
                         (compute-aggregate arg aggregate-plans tuples idx))

                       (dp/find-expr? arg)
                       (eval-find-expr arg context tuples var->idx
                                       aggregate-plans)

                       :else
                       (-context-resolve arg context)))
                   (:args expr))]
    (apply op args)))

(defn- build-var->idx
  "Build a mapping from variable symbols to tuple indices."
  [find-elements]
  (loop [elements find-elements
         idx      0
         result   {}]
    (if (empty? elements)
      result
      (let [elem (first elements)
            vars (dp/-find-vars elem)]
        (recur (rest elements)
               (+ idx (count vars))
               (into result
                     (map vector vars (range idx (+ idx (count vars))))))))))

(defn -aggregate
  ([find-elements context tuples]
   (-aggregate find-elements context tuples
               (build-var->idx find-elements)
               (compile-aggregates find-elements context)))
  ([find-elements context tuples var->idx]
   (-aggregate find-elements context tuples var->idx
               (compile-aggregates find-elements context)))
  ([find-elements context tuples var->idx aggregate-plans]
   (let [first-tuple (first tuples)]
     (loop [elements  find-elements
            tuple-idx 0
            result    []]
       (if (empty? elements)
         result
         (let [elem     (first elements)
               num-vars (count (dp/-find-vars elem))]
           (cond
             (dp/find-expr? elem)
             (recur (rest elements)
                    (+ tuple-idx num-vars)
                    (conj result (eval-find-expr elem context tuples var->idx
                                                 aggregate-plans)))

             (dp/aggregate? elem)
             (recur (rest elements)
                    (inc tuple-idx)
                    (conj result
                          (compute-aggregate elem aggregate-plans tuples
                                             tuple-idx)))

             :else
             (recur (rest elements)
                    (inc tuple-idx)
                    (conj result (nth first-tuple tuple-idx))))))))))

(defn- groupable-elem?
  "Check if an element should be used for grouping
  (not an aggregate or find-expr)."
  [elem]
  (not (or (dp/aggregate? elem) (dp/find-expr? elem))))

(defn- group-tuples
  [resultset ^ints group-idxs]
  (let [n (alength group-idxs)]
    (cond
      (zero? n)
      (when (seq resultset)
        (list resultset))

      (= 1 n)
      (let [^Map groups (QueryKey$KeyMap.)
            idx             (aget group-idxs 0)]
        (doseq [tuple resultset]
          (let [key (nth tuple idx)]
            (if-let [^FastList bucket (.get groups key)]
              (.add bucket tuple)
              (.put groups key (doto (FastList.) (.add tuple))))))
        (.values groups))

      :else
      (let [^Map groups (QueryKey$KeyMap.)
            ^objects scratch (object-array n)
            lookup           (r/array-lookup)]
        (doseq [tuple resultset]
          (dotimes [i n]
            (aset scratch i (nth tuple (aget group-idxs i))))
          (if-let [^FastList bucket
                   (.get groups (r/reset-array-lookup! lookup scratch))]
            (.add bucket tuple)
            (.put groups (r/wrap-array (aclone scratch))
                  (doto (FastList.) (.add tuple)))))
        (.values groups)))))

(defn aggregate
  "Group and reduce results using native aggregate semantics. An optional
  invocation check runs during grouping, between groups, and while each
  aggregate projects its input values. Separate aggregates check independently
  even when they traverse the same group. An optional wrapper receives each
  aggregate expression and its resolved function to provide bounded execution."
  ([find-elements context resultset]
   (aggregate find-elements context resultset nil))
  ([find-elements context resultset check!]
   (aggregate find-elements context resultset check! nil))
  ([find-elements context resultset check! wrap-aggregate]
   (let [^ints group-idxs (int-array (u/idxs-of groupable-elem? find-elements))
         var->idx         (build-var->idx find-elements)
         aggregate-plans  (compile-aggregates find-elements context)
         aggregate-plans  (if wrap-aggregate
                            (into {} (map (fn [[element [f args]]]
                                            [element [(wrap-aggregate element f) args]]))
                                  aggregate-plans)
                            aggregate-plans)
         aggregate-plans  (if check! (update-vals aggregate-plans #(conj % check!))
                             aggregate-plans)
         resultset        (if check! (checked-map identity check! resultset) resultset)
         groups           (group-tuples resultset group-idxs)
         reduce-group     #(-aggregate find-elements context % var->idx aggregate-plans)]
     (if check! (checked-map reduce-group check! groups)
         (map reduce-group groups)))))

(defn- find-aggregate-idx
  "Find the index of an aggregate in find-elements by matching structure."
  [aggregate find-elements]
  (let [agg-var (-> aggregate :args last :symbol)]
    (loop [elems find-elements
           idx   0]
      (when (seq elems)
        (let [elem (first elems)]
          (cond
            (and (dp/aggregate? elem)
                 (= (-> elem :fn :symbol) (-> aggregate :fn :symbol))
                 (= (-> elem :args last :symbol) agg-var))
            idx

            :else
            (recur (rest elems) (inc idx))))))))

(defn- compile-having-arg
  "Resolve an aggregate column once, before visiting the result groups."
  [arg find-elements]
  (cond
    (dp/aggregate? arg)
    (if-some [idx (find-aggregate-idx arg find-elements)]
      #(nth % idx)
      (constantly nil))

    (dp/find-expr? arg)
    (if-some [idx (u/index-of #(and (dp/find-expr? %)
                                   (= (:fn %) (:fn arg)))
                             find-elements)]
      #(nth % idx)
      (constantly nil))

    (instance? Constant arg)
    (constantly (:value arg))

    :else
    (constantly arg)))

(defn- compile-having-pred
  [pred find-elements]
  (let [pred-fn (get built-ins/query-fns (-> pred :fn :symbol))
        readers (mapv #(compile-having-arg % find-elements) (:args pred))]
    (if-not pred-fn
      (constantly nil)
      (case (count readers)
        0 (fn [_] (pred-fn))
        1 (let [read-arg (first readers)]
            (fn [row]
              (when-some [arg (read-arg row)] (pred-fn arg))))
        2 (let [[read-left read-right] readers]
            (fn [row]
              (let [left (read-left row)
                    right (read-right row)]
                (when (and (some? left) (some? right))
                  (pred-fn left right)))))
        (fn [row]
          (let [args (mapv #(% row) readers)]
            (when (every? some? args) (apply pred-fn args))))))))

(defn compile-having
  "Compile HAVING once for vector or native array rows. Nil means no filter."
  [having find-elements]
  (when (seq having)
    (let [predicates (mapv #(compile-having-pred % find-elements) having)]
      (if (= 1 (count predicates))
        (first predicates)
        (fn [row] (every? #(% row) predicates))))))

(defn apply-having
  "Filter aggregated results by having predicates."
  [having find-elements results]
  (if-let [keep-group? (compile-having having find-elements)]
    (filter keep-group? results)
    results))
