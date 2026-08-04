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
   [datalevin.db :as db]
   [datalevin.inline :refer [update assoc]]
   [datalevin.parser :as dp]
   [datalevin.pull-api :as dpa]
   [datalevin.query.aggregate :as qagg]
   [datalevin.query.access :as qaccess]
   [datalevin.query.access.ave :as qave]
   [datalevin.query.access.function :as qfunction]
   [datalevin.query.access.idoc :as qidoc]
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
  (qfunction/access-method {:idoc qidoc/access-method}))

(def ^:dynamic *access-methods*
  "Physical access methods considered during query planning."
  [qave/access-method function-access-method])

(def ^:private materialize-input-bound-patterns
  qo/materialize-input-bound-patterns)

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
  (-> context
      qo/build-graph
      ((fn [c] (build-explain) c))
      qo/build-plan
      qo/plan-not-joins
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
            (reduce qresolve/resolve-clause c (:late-clauses c))
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

(defn resolve-redudants
  "handle pathological cases of variable is already bound in where clauses"
  [{:keys [parsed-q] :as context}]
  (let [{:keys [qwhere]} parsed-q
        get-v            #(nth (:pattern %) 2)
        const-v          (fn [patterns]
                           (some #(let [v (get-v %)]
                                    (when (instance? Constant v) (:value v)))
                                 patterns))
        redundant-groups
        (into []
              (->> qwhere
                   (eduction (filter #(instance? Pattern %)))
                   (group-by (fn [{:keys [source pattern]}]
                               [source (first pattern) (second pattern)]))
                   (eduction (filter
                               #(let [ps (val %)]
                                  (and (< 1 (count ps)) (const-v ps)))))))]
    (reduce
      (fn [c [_ patterns]]
        (let [v (const-v patterns)]
          (reduce
            (fn [c pattern]
              (let [origs (get-in c [:parsed-q :qorig-where])
                    idx   (u/index-of #(= pattern %) origs)]
                (-> c
                    (update-in [:parsed-q :qwhere] #(remove #{pattern} %))
                    (update-in [:parsed-q :qorig-where]
                               #(u/remove-idxs #{idx} %))
                    (update :rels conj
                            (r/relation! {(:symbol (get-v pattern)) 0}
                                         (doto (FastList.)
                                           (.add (object-array [v]))))))))
            c (eduction (filter #(instance? Variable (get-v %))) patterns))))
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
            lookup  (qresolve/lookup-pattern context source-db pattern)
            result  (reduce r/prod-rel
                            (qresolve/collapse-rels [one] lookup))
            projected (project-access-relation result output-cols)]
        (.addAll joined ^java.util.Collection (:tuples projected))))
    (r/relation! (qplan/cols->attrs output-cols) joined)))

(defn- execute-hash-fragment-join
  [parsed-q source-db relation join output-cols]
  (let [pattern (access-fragment-pattern parsed-q (:clause-idx join))
        context (assoc (qplan/make-context parsed-q false)
                       :rels [relation])
        lookup  (qresolve/lookup-pattern context source-db pattern)
        result  (reduce r/prod-rel
                        (qresolve/collapse-rels [relation] lookup))]
    (project-access-relation result output-cols)))

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
        (reduce qresolve/resolve-clause context late-clauses)))))

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
               (rewrite-unused-vars)
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
          (rewrite-unused-vars)
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
          (rewrite-unused-vars)
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
