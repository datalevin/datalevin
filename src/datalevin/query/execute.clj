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
   [datalevin.datom Datom]
   [datalevin.parser BindScalar Constant DefaultSrc FindColl FindRel FindScalar
    FindTuple Pattern Predicate Variable]
   [org.eclipse.collections.impl.list.mutable FastList]))

(def ^:private plugin-inputs qo/plugin-inputs)

(def ^:private materialize-input-bound-patterns
  qo/materialize-input-bound-patterns)

(def ^:private rewrite-unused-vars qo/rewrite-unused-vars)

(declare sort-planned-late-clauses)

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
      sort-planned-late-clauses))

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
  ([{:keys [graph result-set plan opt-clauses late-clauses run?] :as context}]
   (when qplan/*explain*
     (let [{:keys [^long planning-time ^long parsing-time ^long building-time]}
           @qplan/*explain*
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
               :late-clauses late-clauses)))))

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

(def ^:private ^:const ^long top-k-candidate-batch-size 1024)
(def ^:private ^:const ^long top-k-max-candidate-batches 32)

(defn- first-order-key
  [order]
  (when-let [order-var (first order)]
    (when (symbol? order-var)
      [order-var (if (keyword? (second order)) (second order) :asc)])))

(defn- ranked-pattern
  [parsed-q order-var]
  (first
    (keep-indexed
      (fn [i clause]
        (when (and (instance? Pattern clause)
                   (instance? DefaultSrc (:source ^Pattern clause)))
          (let [pattern (:pattern ^Pattern clause)]
            (when (and (= 3 (count pattern))
                       (instance? Variable (nth pattern 0))
                       (instance? Constant (nth pattern 1))
                       (keyword? (:value ^Constant (nth pattern 1)))
                       (instance? Variable (nth pattern 2))
                       (= order-var (:symbol ^Variable (nth pattern 2))))
              {:clause-idx i
               :entity-var (:symbol ^Variable (nth pattern 0))
               :order-var  order-var
               :attr       (:value ^Constant (nth pattern 1))}))))
      (:qwhere parsed-q))))

(defn- input-values
  [parsed-q inputs]
  (into {}
        (keep (fn [[binding value]]
                (when (instance? BindScalar binding)
                  [(get-in binding [:variable :symbol]) value])))
        (map vector (:qin parsed-q) inputs)))

(defn- term-value
  [values term]
  (cond
    (instance? Constant term) (:value ^Constant term)
    (instance? Variable term) (get values (:symbol ^Variable term) ::none)
    :else                     ::none))

(defn- ordered-range-start
  [parsed-q inputs order-var direction]
  (let [values (input-values parsed-q inputs)]
    (some
      (fn [clause]
        (when (instance? Predicate clause)
          (let [op   (get-in clause [:fn :symbol])
                args (:args ^Predicate clause)
                lhs  (first args)
                rhs  (second args)
                lvar (when (instance? Variable lhs) (:symbol ^Variable lhs))
                rvar (when (instance? Variable rhs) (:symbol ^Variable rhs))]
            (cond
              (and (identical? direction :desc)
                   (= lvar order-var) (#{'< '<=} op))
              (let [v (term-value values rhs)] (when-not (= ::none v) v))

              (and (identical? direction :desc)
                   (= rvar order-var) (#{'> '>=} op))
              (let [v (term-value values lhs)] (when-not (= ::none v) v))

              (and (identical? direction :asc)
                   (= lvar order-var) (#{'> '>=} op))
              (let [v (term-value values rhs)] (when-not (= ::none v) v))

              (and (identical? direction :asc)
                   (= rvar order-var) (#{'< '<=} op))
              (let [v (term-value values lhs)] (when-not (= ::none v) v))

              :else nil))))
      (:qwhere parsed-q))))

(defn- top-k-pushdown-spec
  [parsed-q inputs]
  (let [find          (:qfind parsed-q)
        find-elements (dp/find-elements find)
        limit         (:qlimit parsed-q)
        dbs           (filterv db/db? inputs)]
    (when (and (instance? FindRel find)
               (finite-limit? limit)
               (pos? (long limit))
               (seq (:qorder parsed-q))
               (nil? (:qwith parsed-q))
               (empty? (:qhaving parsed-q))
               (nil? (:qreturn-map parsed-q))
               (not-any? #(or (dp/aggregate? %) (dp/find-expr? %)
                              (dp/pull? %))
                         find-elements)
               (= 1 (count dbs))
               (not (db/pending-tx-cache? (first dbs))))
      (when-let [[order-var direction] (first-order-key (:qorder parsed-q))]
        (when-let [ranked (ranked-pattern parsed-q order-var)]
          (when-some [start-value (ordered-range-start parsed-q inputs order-var
                                                       direction)]
            (assoc ranked
                   :db          (first dbs)
                   :direction   direction
                   :start-value start-value)))))))

(defn- ranked-datom-page
  [db attr direction start-value cursor]
  (let [reverse?   (identical? direction :desc)
        cursor-v   (:value cursor)
        cursor-n   (long (or (:ties cursor) 0))
        requested  (+ top-k-candidate-batch-size cursor-n)
        ^java.util.List found
        (if reverse?
          (db/-rseek-datoms db :ave attr (or cursor-v start-value) nil requested)
          (db/-seek-datoms db :ave attr (or cursor-v start-value) nil requested))
        size       (.size found)
        start      (long
                     (loop [i 0]
                       (if (and cursor-v (< i size)
                                (= cursor-v (.-v ^Datom (.get found i))))
                         (recur (unchecked-inc-int i))
                         i)))
        boundary   (when (< start size) (.-v ^Datom (.get found (dec size))))
        boundary-datoms
        ;; AVE stores entities as duplicate values. Expand the boundary key so
        ;; secondary order terms cannot omit a primary-key tie.
        (when boundary (db/-datoms db :ave attr boundary nil))
        boundary-count (long (count boundary-datoms))
        tuples     (FastList. (int (+ (- size start) boundary-count)))]
    (loop [i start]
      (when (< i size)
        (let [^Datom datom (.get found i)]
          (when-not (= boundary (.-v datom))
            (.add tuples (object-array [(.-e datom) (.-v datom)])))
          (recur (unchecked-inc-int i)))))
    (doseq [^Datom datom boundary-datoms]
      (.add tuples (object-array [(.-e datom) (.-v datom)])))
    {:tuples tuples
     :cursor (when boundary {:value boundary :ties boundary-count})
     :done?  (< size requested)}))

(defn- ranked-batch-query
  [parsed-q clause-idx entity-var order-var]
  (-> parsed-q
      (update :qin conj (dp/parse-binding [[entity-var order-var]]))
      (update :qwhere #(u/remove-idxs #{clause-idx} %))
      (update :qorig-where #(u/remove-idxs #{clause-idx} %))
      (assoc :qorder nil :qlimit nil :qoffset nil)))

(defn- past-top-k-boundary?
  [find-vars order direction rows cursor window-end]
  (let [window-end (long window-end)]
    (when (and cursor (<= window-end (long (count rows))))
      (let [cmp          (order-comps (tuple-get (first rows)) find-vars order)
            cutoff-row   (nth (sort cmp rows) (unchecked-dec window-end))
            order-var    (first order)
            order-idx    (u/index-of #(= order-var %) find-vars)
            cutoff-value (nth cutoff-row order-idx)
            c             (compare (:value cursor) cutoff-value)]
        (if (identical? direction :desc) (neg? c) (pos? c))))))

(declare execute-query)

(defn- top-k-pushdown-query
  [parsed-q inputs {:keys [db attr direction start-value clause-idx entity-var
                           order-var]}]
  (let [batch-query (ranked-batch-query parsed-q clause-idx entity-var order-var)
        find-vars   (dp/find-vars (:qfind parsed-q))
        order       (:qorder parsed-q)
        limit       (:qlimit parsed-q)
        offset      (:qoffset parsed-q)
        window-end  (+ (long (or offset 0)) (long limit))]
    (loop [cursor nil
           rows   #{}
           batches 0]
      (if (>= (long batches) top-k-max-candidate-batches)
        (execute-query parsed-q inputs)
        (let [{:keys [tuples done?] next-cursor :cursor}
              (ranked-datom-page db attr direction start-value cursor)]
          (if (zero? (.size ^java.util.List tuples))
            (order-result find-vars rows order limit offset)
            (let [batch-result (execute-query batch-query (conj (vec inputs)
                                                                 tuples))
                  rows         (into rows batch-result)]
              (if (or done?
                      (past-top-k-boundary? find-vars order direction rows
                                            next-cursor window-end))
                (order-result find-vars rows order limit offset)
                (recur next-cursor rows (unchecked-inc-int batches))))))))))

(defn- execute-query
  [parsed-q inputs]
  (let [find          (:qfind parsed-q)
        find-elements (dp/find-elements find)
        result-arity  (count find-elements)
        with          (:qwith parsed-q)
        having        (:qhaving parsed-q)
        find-vars     (dp/find-vars find)
        all-vars      (concatv find-vars (map :symbol with))
        [parsed-q inputs] (plugin-inputs parsed-q inputs)
        udf-db        (first (filter db/-searchable? inputs))
        context
        (binding [built-ins/*udf-db* udf-db]
          (-> (qplan/make-context parsed-q true)
              (qresolve/resolve-ins inputs)
              (materialize-input-bound-patterns)
              (resolve-redudants)
              (rules/rewrite)
              (rewrite-unused-vars)
              (-q true)
              (collect all-vars)))
        result
        (binding [built-ins/*udf-db* udf-db]
          (cond->> (:result-set context)
            with (mapv #(subvec % 0 result-arity))

            (some #(or (dp/aggregate? %) (dp/find-expr? %)) find-elements)
            (qagg/aggregate find-elements context)

            (seq having)
            (qagg/apply-having having find-elements)

            (some dp/pull? find-elements)
            (pull find-elements context)

            true
            (-post-process find (:qreturn-map parsed-q))))]
    (result-explain context result)
    (if (instance? FindRel find)
      (if-let [order (:qorder parsed-q)]
        (order-result find-vars result order
                      (:qlimit parsed-q) (:qoffset parsed-q))
        (result-window result (:qlimit parsed-q) (:qoffset parsed-q)))
      result)))

(defn q*
  [parsed-q inputs]
  (binding [timeout/*deadline* (timeout/effective-deadline
                                 (:qtimeout parsed-q))]
    (if-let [spec (and (nil? qplan/*explain*)
                       (top-k-pushdown-spec parsed-q inputs))]
      (top-k-pushdown-query parsed-q inputs spec)
      (execute-query parsed-q inputs))))

(defn mark-parsing-finished!
  []
  (when qplan/*explain*
    (vswap! qplan/*explain* assoc :parsing-time
            (- (System/nanoTime) ^long qplan/*start-time*))))

(defn plan-context*
  [parsed-q inputs]
  (binding [timeout/*deadline* (timeout/effective-deadline
                                 (:qtimeout parsed-q))]
    (let [[parsed-q inputs] (plugin-inputs parsed-q inputs)]
      (-> (qplan/make-context parsed-q false)
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
