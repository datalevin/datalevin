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
  "Query execution orchestration, access paths, and terminal collection."
  (:refer-clojure :exclude [update assoc])
  (:require
   [datalevin.built-ins :as built-ins]
   [datalevin.db :as db]
   [datalevin.inline :refer [update assoc]]
   [datalevin.parser :as dp]
   [datalevin.query-optimizer :as qo]
   [datalevin.query-util :as qu]
   [datalevin.query.access :as qaccess]
   [datalevin.query.access.ave :as qave]
   [datalevin.query.access.fulltext :as qfulltext]
   [datalevin.query.access.function :as qfunction]
   [datalevin.query.access.idoc :as qidoc]
   [datalevin.query.access.vector :as qvector]
   [datalevin.query.aggregate :as qagg]
   [datalevin.query.execute.enrichment :refer [plan-post-top-k-enrichment]]
   [datalevin.query.execute.group-reduction
    :refer [plan-keyed-group-reduction resolve-terminal-keyed-group-reduction]]
   [datalevin.query.execute.late
    :refer [cheaper-late-producer isolated-range-context
            isolated-union-context late-or-join-branch-selector
            late-or-join-clause? sort-planned-late-clauses
            ;; Resolved here by query-resolve-test in the sibling test project.
            #_{:clj-kondo/ignore [:unused-referred-var]}
            sort-late-clauses]]
   [datalevin.query.execute.point-lookup :as point-lookup
    :refer [execute-point-lookup-projection
            explain-point-lookup-projection-plan! point-lookup-projection-db
            point-lookup-projection-key point-lookup-projection-shape]]
   [datalevin.query.execute.result :as result
    :refer [*deferred-result-explain* adaptive-limit-query?
            indexed-unique-result-set order-comps order-result pull
            query-result-size result-window spillable-result-set tuple-get
            ;; Resolved here by query-resolve-test in the sibling test project.
            #_{:clj-kondo/ignore [:unused-referred-var]}
            tuple->persistent-vector]]
   [datalevin.query.execute.rewrite :as rewrite]
   [datalevin.query.plan :as qplan]
   [datalevin.query.resolve :as qresolve]
   [datalevin.relation :as r]
   [datalevin.rules :as rules]
   [datalevin.timeout :as timeout]
   [datalevin.util :as u :refer [concatv map+ raise]])
  (:import
   [java.util Collection HashSet List]
   [datalevin.parser FindColl FindRel FindScalar FindTuple Variable]
   [org.eclipse.collections.impl.list.mutable FastList]))

(declare access-batch-query access-outer-query
         execute-query execute-planned-query)

;; Retain the existing entry points for query callers.
(def prepare-query point-lookup/prepare-query)
(def -collect-tuples result/-collect-tuples)
(def -collect result/-collect)
(def collect result/collect)
(def tuples->return-map result/tuples->return-map)
(def resolve-redudants rewrite/resolve-redudants)
(def result-explain result/result-explain)

(def ^:private plugin-inputs qo/plugin-inputs)

(def ^:private function-access-method
  (qfunction/access-method {:fulltext qfulltext/access-method
                            :idoc     qidoc/access-method
                            :vector   qvector/access-method}))

(def ^:dynamic *access-methods*
  "Physical access methods considered during query planning."
  [qave/access-method function-access-method])

(def ^:dynamic ^:private *access-execution* nil)

(def ^:dynamic ^:no-doc *terminal-result-collection?* true)

(def ^:private materialize-input-bound-patterns
  qo/materialize-input-bound-patterns)

(def ^:private materialize-selective-value-lookups
  qo/materialize-selective-value-lookups)

(def ^:private materialize-selective-rule-anchors
  qo/materialize-selective-rule-anchors)

(def ^:private push-down-equality-disjunctions
  qo/push-down-equality-disjunctions)

(def ^:private rewrite-unused-vars qo/rewrite-unused-vars)

(definterface IProjectedDistinctSink
  (^Object finishResult []))

(deftype ProjectedDistinctSink [^ints idxs
                                ^:unsynchronized-mutable ^HashSet seen
                                ^:unsynchronized-mutable ^FastList projected
                                ^:unsynchronized-mutable ^objects scratch
                                ^:unsynchronized-mutable lookup
                                ^:unsynchronized-mutable ^boolean distinct-batch
                                ^:unsynchronized-mutable ^boolean proven-unique
                                ^longs input-count]
  IProjectedDistinctSink
  (finishResult [_]
    (let [^FastList tuples projected
          result-set       (if proven-unique
                             (indexed-unique-result-set tuples)
                             (spillable-result-set tuples))]
      (set! seen nil)
      (set! scratch nil)
      (set! lookup nil)
      (.clear tuples)
      (set! projected nil)
      result-set))

  qplan/ITerminalDistinctSink
  (-add-distinct-batch! [_ tuples]
    (let [^List tuples tuples
          n            (.size tuples)
          previous     (aget input-count 0)]
      (aset-long input-count 0
                 (unchecked-add previous (long n)))
      (when (or distinct-batch (pos? previous))
        ;; Each batch is internally distinct, but separate batches are not
        ;; guaranteed disjoint. The spillable fallback performs the union.
        (set! proven-unique (boolean false)))
      (set! distinct-batch (boolean true))
      (if (zero? (.size projected))
        (if (instance? FastList tuples)
          (set! projected tuples)
          (.addAll projected tuples))
        (.addAll projected tuples))
      true))

  Collection
  (add [_ tuple]
    (let [^objects tuple tuple
          width          (alength idxs)]
      (when distinct-batch
        (set! proven-unique (boolean false)))
      (aset-long input-count 0 (unchecked-inc (aget input-count 0)))
      (if (= 1 width)
        (let [value (aget tuple (aget idxs 0))]
          (when (.add seen value)
            (.add projected (object-array [value]))))
        (do
          (dotimes [i width]
            (aset scratch i (aget tuple (aget idxs i))))
          (r/reset-array-lookup! lookup scratch)
          (when-not (.contains seen lookup)
            (let [key (aclone scratch)]
              (.add seen (r/wrap-array key))
              (.add projected key)))))
      true))
  (addAll [this tuples]
    (if (instance? List tuples)
      (let [^List tuples tuples]
        (dotimes [i (.size tuples)]
          (.add ^Collection this (.get tuples i))))
      (doseq [tuple tuples]
        (.add ^Collection this tuple)))
    true)
  (size [_]
    (int (min Integer/MAX_VALUE (aget input-count 0)))))

(defn- projected-distinct-sink
  [attrs symbols]
  (let [input-count (long-array 1)
        idxs        (int-array (map attrs symbols))
        width       (alength idxs)]
    {:sink        (ProjectedDistinctSink.
                    idxs (HashSet.) (FastList.)
                    (when (< 1 width) (object-array width))
                    (when (< 1 width) (r/array-lookup))
                    (boolean false) (boolean true) input-count)
     :input-count input-count}))

(defn- terminal-collection-symbols
  [{:keys [parsed-q rels late-clauses result-set]
    :as context}]
  (let [find-elements (dp/find-elements (:qfind parsed-q))]
    (when (and *terminal-result-collection?*
               (nil? result-set)
               (empty? rels)
               (empty? late-clauses)
               (nil? (:post-top-k-enrichment context))
               (adaptive-limit-query? parsed-q)
               (seq find-elements)
               (every? #(instance? Variable %) find-elements))
      (mapv :symbol find-elements))))

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
  [context plans]
  (if (or (empty? plans) (:prepared? (first plans)))
    plans
    (let [{:keys [query inputs]} (first plans)
          sample-cost-budget (qo/access-sample-cost-budget context)]
      (mapv
        (fn [plan]
          (if (:unavailable? plan)
            (assoc plan :prepared? true)
            (cond->
                (assoc plan :prepared? true)
              (:correlated? plan)
              (assoc :outer-query (access-outer-query query plan)))))
        (qo/plan-access-joins query inputs plans sample-cost-budget)))))

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
      plan-keyed-group-reduction
      plan-post-top-k-enrichment
      ((fn [context]
         (if (seq (:access-plans context))
           (let [plans (prepare-access-plans context
                                             (:access-plans context))]
             (-> context
                 (attach-access-plans plans)
                 (qo/build-property-memo)))
           context)))))

(defn execute-plan
  [{:keys [plan sources] :as context}]
  (if (= 1 (transduce (map (fn [[_ components]] (count components))) + plan))
    (let [[src components] (first plan)
          db               (sources src)
          all-steps        (vec (mapcat :steps (first components)))
          execute-rel      #(update context :rels qresolve/collapse-rels
                                    (qplan/execute-steps context db all-steps))]
      (if-let [symbols (terminal-collection-symbols context)]
        (let [attrs (qplan/step-attrs all-steps)]
          (if (every? #(contains? attrs %) symbols)
            (let [{:keys [sink input-count]}
                  (projected-distinct-sink attrs symbols)]
              (if-let [physical-operator
                       (qplan/execute-steps-into
                         context db all-steps sink symbols)]
                (let [result-set
                      (.finishResult ^IProjectedDistinctSink sink)
                      candidate-pairs (:candidate-pairs physical-operator)
                      collection
                      (cond->
                        {:mode          :projected-distinct
                         :symbols       symbols
                         :input-tuples  (long
                                          (or candidate-pairs
                                              (aget ^longs input-count 0)))
                         :result-tuples (count result-set)}
                        (= :dense-eav-composition
                           (:operator physical-operator))
                        (assoc :physical-operator physical-operator))]
                  (assoc context
                         :result-set result-set
                         :terminal-collected-symbols symbols
                         :terminal-result-collection collection))
                (execute-rel)))
            (execute-rel)))
        (execute-rel)))
    (reduce
      (fn [c r] (update c :rels qresolve/collapse-rels r))
      context (->> plan
                   (mapcat (fn [[src components]]
                             (let [db (sources src)]
                               (for [plans components]
                                 [db (mapcat :steps plans)]))))
                   (map+ #(apply qplan/execute-steps context %))
                   (sort-by #(count (:tuples %)))))))

(defn- resolve-late-clause
  [context clause]
  (cond
    (= clause (get-in context [:keyed-group-reduction :clause]))
    (resolve-terminal-keyed-group-reduction context clause)

    (late-or-join-clause? clause)
    (binding [qresolve/*or-join-branch-selector*
              (late-or-join-branch-selector clause)]
      (qresolve/resolve-clause context clause))

    :else
    (qresolve/resolve-clause context clause)))

(defn- resolve-late-clauses
  [context clauses]
  (loop [context           context
         pending           (vec clauses)
         executed          []
         candidate-values  nil
         candidates-ready? false]
    (if (empty? pending)
      (assoc context :late-clauses executed)
      (let [plan-candidates?
            (and qresolve/*singleton-domain-scan?*
                 (not candidates-ready?)
                 (< 1 (count pending))
                 (qresolve/singleton-domain-planning-input? context))
            candidate-values
            (if plan-candidates?
              (qresolve/singleton-domain-candidate-values context clauses)
              candidate-values)
            candidates-ready? (or candidates-ready? plan-candidates?)
            clause           (first pending)
              choice           (cheaper-late-producer context pending)
              selected-idxs    (or (:idxs choice) [0])
              strategy         (get-in choice [:decision :strategy])
              specialization   (when (and (= [0] selected-idxs)
                                          (or (nil? strategy)
                                              (= :bound-pattern-first strategy)))
                                 (or
                                   (qresolve/resolve-bound-value-presence-prefix
                                     context pending 0)
                                   (when (seq candidate-values)
                                     (qresolve/resolve-singleton-domain-scan
                                       context pending 0 candidate-values))))
              consumed-idxs    (or (:idxs specialization) selected-idxs)
              selected-clauses (cond
                                 specialization
                                 (mapv pending consumed-idxs)

                                 (and choice
                                      (not= :bound-pattern-first strategy))
                                 (get-in choice [:producer :selected-clauses])

                                 :else
                                 [clause])]
          (when-let [decision (:decision choice)]
            (when qplan/*explain*
              (vswap! qplan/*explain* update :late-clause-decisions
                      (fnil conj []) decision)))
          (recur (if specialization
                   (:context specialization)
                   (case strategy
                     :indexed-union-first
                     (isolated-union-context
                       context (get-in choice [:producer :clause]))

                     :indexed-range-first
                     (isolated-range-context context (:producer choice))

                     (resolve-late-clause context clause)))
                 (u/remove-idxs (set consumed-idxs) pending)
                 (into executed selected-clauses)
                 candidate-values candidates-ready?)))))

(defn- resolve-pre-top-k-late-clauses
  [{:keys [late-clauses post-top-k-enrichment] :as context}]
  (if-let [deferred (seq (:clauses post-top-k-enrichment))]
    (let [deferred (set deferred)
          resolved (resolve-late-clauses
                     context (into [] (remove deferred) late-clauses))]
      (assoc resolved :all-late-clauses late-clauses))
    (resolve-late-clauses context late-clauses)))

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
            (resolve-pre-top-k-late-clauses c)
            c))))))

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
                         (raise "Access fragment lost projected columns"
                                    {:missing missing
                                     :available (keys attrs)
                                     :projected cols}))
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

(def ^:private access-subquery-explain-keys
  [:actual-result-size :execution-time :plan :late-clauses
   :late-clause-decisions :late-or-join-branch-decisions
   :post-top-k-enrichment :reusable-sip-domains])

(defn- record-access-execution!
  [key value]
  (when *access-execution*
    (vswap! *access-execution* update key (fnil conj []) value)))

(defn- execute-access-subquery
  [phase execute]
  (if *access-execution*
    (let [explain (volatile! {:parsing-time  0
                              :building-time 0
                              :planning-time 0})
          result
          (binding [qplan/*explain*    explain
                    qplan/*start-time* (System/nanoTime)]
            (execute))]
      (record-access-execution!
        :subqueries
        (assoc (select-keys @explain access-subquery-explain-keys)
               :phase phase))
      result)
    (execute)))

(defn- execute-access-batch
  [parsed-q batch-query inputs source-db access-plan tuples batch]
  (let [fragment-tuples (execute-access-fragment
                          parsed-q source-db access-plan tuples)
        result          (execute-access-subquery
                          :residual
                          #(execute-query
                             batch-query
                             (conj (vec inputs) fragment-tuples)))]
    (record-access-execution!
      :batches
      (assoc batch
             :candidate-count (.size ^java.util.List tuples)
             :candidate-work (long (or (:candidate-work batch)
                                       (.size ^java.util.List tuples)))
             :fragment-result-count
             (.size ^java.util.List fragment-tuples)
             :residual-result-count (query-result-size batch-query result)))
    result))

(defn- execute-access-fallback
  [fallback inputs reason]
  (when *access-execution*
    (vswap! *access-execution* assoc :fallback
            {:kind :conventional :reason reason}))
  (execute-access-subquery
    :fallback
    #(execute-planned-query
       (assoc (:context fallback) :run? true) inputs)))

(defn- access-source-db
  [step inputs]
  (or (:access-source step)
      (first (filter db/db? inputs))))

(defn- execute-complete-access-source
  [source source-db input]
  (if *access-execution*
    (let [candidate-work (volatile! 0)
          tuples
          (binding [qplan/*access-batch-observer*
                    #(vswap! candidate-work
                             (fn [^long total]
                               (unchecked-add
                                 total (qaccess/batch-work %))))]
            (qplan/step-execute source source-db input))]
      [tuples @candidate-work])
    [(qplan/step-execute source source-db input) nil]))

(defn- pushdown-query
  "Shared batch-scanning pipeline for adaptive top-k and limit pushdown.

  `strategy` supplies the mode-specific pieces:
    `:scanned-by`    `:tuples` or `:work`, the sample scan accounting unit
    `:done?`         `(fn [rows frontier window-end] ...)`
    `:finish`        `(fn [rows] ...)`
    `:retry-empty?`  continue past empty non-exhausted batches with work
    `:empty-window?` short-circuit to an empty result when window-end is 0"
  [parsed-q inputs
   {:keys [source residual-query demand work fallback access-plan]}
   {:keys [scanned-by done? finish retry-empty? empty-window?]}]
  (let [path           (:path source)
        source-db      (access-source-db source inputs)
        batch-query    residual-query
        window-end     (long (:required-count demand))
        work-budget    (:max-candidates work)
        budgeted?      (some? work-budget)
        sample-batch   (:sample-batch source)
        sample-tuples  (:tuples sample-batch)
        sample-count   (long (if sample-tuples
                               (.size ^java.util.List sample-tuples)
                               0))
        sample-work    (long (if sample-batch
                               (qaccess/batch-work sample-batch)
                               0))
        sample-scanned (case scanned-by
                         :tuples sample-count
                         :work   sample-work)
        sample-rows
        (if (pos? sample-count)
          (into #{}
                (execute-access-batch
                  parsed-q batch-query inputs source-db access-plan
                  sample-tuples
                  {:source         :planning-sample
                   :candidate-work sample-work
                   :exhausted?     (:exhausted? sample-batch)}))
          #{})
        sample-done?   (or (:exhausted? sample-batch)
                           (done? sample-rows (:frontier sample-batch)
                                  window-end))
        attempt-work
        (cond-> work
          (and budgeted? (pos? (long work-budget)))
          (assoc :batch-size
                 (min (long (or (:batch-size work) work-budget))
                      (long work-budget)))

          sample-batch
          (assoc :resume  (:frontier sample-batch)
                 :emitted sample-scanned))
        fallback-query #(execute-access-fallback fallback inputs %)]
    (cond
      (and empty-window? (zero? window-end))
      []

      sample-done?
      (finish sample-rows)

      (and budgeted?
           (or (not (pos? (long work-budget)))
               (>= sample-scanned (long work-budget))))
      (fallback-query :candidate-budget)

      :else
      (let [cursor (qaccess/open-access
                     path demand (:bounds source) attempt-work source-db nil)]
        (try
          (loop [rows    sample-rows
                 batches (if sample-batch 1 0)
                 scanned sample-scanned]
            (if (or (and (not budgeted?)
                         (>= (long batches) top-k-max-candidate-batches))
                    (and budgeted?
                         (>= (long scanned) (long work-budget))))
              (fallback-query (if budgeted?
                                :candidate-budget
                                :batch-limit))
              (let [{:keys [tuples frontier exhausted?] :as batch}
                    (qaccess/next-batch cursor)
                    batch-work (long (qaccess/batch-work batch))
                    scanned    (+ (long scanned) batch-work)]
                (if (zero? (.size ^java.util.List tuples))
                  (if exhausted?
                    (finish rows)
                    (if (and retry-empty? (pos? batch-work))
                      (recur rows (unchecked-inc-int batches) scanned)
                      (fallback-query :empty-batch)))
                  (let [batch-result
                        (execute-access-batch
                          parsed-q batch-query inputs source-db access-plan
                          tuples
                          {:source         :access-cursor
                           :candidate-work batch-work
                           :exhausted?     exhausted?})
                        rows (into rows batch-result)]
                    (if (or exhausted?
                            (done? rows frontier window-end))
                      (finish rows)
                      (recur rows
                             (unchecked-inc-int batches)
                             scanned)))))))
          (finally
            (qaccess/close-cursor cursor)))))))

(defn- top-k-pushdown-query
  [parsed-q inputs plan]
  (let [find-vars (dp/find-vars (:qfind parsed-q))
        demand    (:demand plan)
        order     (:ordering demand)
        limit     (:limit demand)
        offset    (:offset demand)
        path      (:path (:source plan))]
    (pushdown-query
      parsed-q inputs plan
      {:scanned-by    :tuples
       :done?         (fn [rows frontier window-end]
                        (past-top-k-boundary?
                          path demand find-vars order rows frontier window-end))
       :finish        (fn [rows]
                        (order-result find-vars rows order limit offset))
       :retry-empty?  false
       :empty-window? false})))

(defn- limit-pushdown-query
  [parsed-q inputs plan]
  (let [demand (:demand plan)
        limit  (:limit demand)
        offset (:offset demand)]
    (pushdown-query
      parsed-q inputs plan
      {:scanned-by    :work
       :done?         (fn [rows _frontier window-end]
                        (<= (long window-end) (long (count rows))))
       :finish        (fn [rows] (result-window rows limit offset))
       :retry-empty?  true
       :empty-window? true})))

(defn- candidate-relation
  [candidate-vars rows]
  (let [tuples (FastList. (count rows))]
    (doseq [row rows]
      (.add tuples (object-array row)))
    (r/relation! (zipmap candidate-vars (range)) tuples)))

(defn- apply-post-top-k-enrichment
  [parsed-q context]
  (if-let [{:keys [clauses candidate-vars ordering offset limit] :as plan}
           (:post-top-k-enrichment context)]
    (let [candidate-context (collect context candidate-vars)
          candidates        (:result-set candidate-context)
          selected          (order-result candidate-vars candidates ordering
                                          limit offset)
          seeded            (assoc candidate-context
                                   :rels [(candidate-relation
                                           candidate-vars selected)]
                                   :result-set nil
                                   :late-clauses [])
          resolved          (binding [qu/*implicit-source*
                                      (get (:sources context) '$)]
                              (resolve-late-clauses seeded clauses))
          diagnostic        (assoc plan
                                   :candidate-count (count candidates)
                                   :selected-count (count selected))]
      [(assoc parsed-q :qoffset 0)
       (assoc resolved
              :late-clauses (or (:all-late-clauses context)
                                (:late-clauses context))
              :post-top-k-enrichment diagnostic)])
    [parsed-q context]))

(defn- finish-query
  [parsed-q context]
  (let [[parsed-q context] (apply-post-top-k-enrichment parsed-q context)
        find          (:qfind parsed-q)
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
  [{:keys [result-set sources] :as context}]
  (binding [qu/*implicit-source* (get sources '$)]
    (if (= result-set #{})
      context
      (let [context (execute-plan context)]
        (resolve-pre-top-k-late-clauses context)))))

(defn- execute-planned-query
  [context inputs]
  (let [parsed-q (:parsed-q context)
        udf-db   (first (filter db/-searchable? inputs))]
    (binding [built-ins/*udf-db* udf-db]
      (finish-query parsed-q (run-planned-context context)))))

(defn- prepare-context
  "Run the shared context preparation pipeline. `execute?` selects execution
   vs planning mode for `make-context` and `-q`."
  [parsed-q inputs access-plans execute?]
  (-> (qplan/make-context parsed-q execute?)
      (attach-access-plans access-plans)
      (qresolve/resolve-ins inputs)
      (materialize-input-bound-patterns)
      (resolve-redudants)
      (rules/rewrite)
      (push-down-equality-disjunctions)
      (rewrite-unused-vars)
      (materialize-selective-value-lookups)
      (materialize-selective-rule-anchors)
      (-q execute?)))

(defn- execute-query
  ([parsed-q inputs]
   (execute-query parsed-q inputs []))
  ([parsed-q inputs access-plans]
   (let [[parsed-q inputs] (plugin-inputs parsed-q inputs)
         udf-db            (first (filter db/-searchable? inputs))
         context
         (binding [built-ins/*udf-db* udf-db]
           (prepare-context parsed-q inputs access-plans true))]
     (binding [built-ins/*udf-db* udf-db]
       (finish-query parsed-q context)))))

(defn- access-query-plan
  [parsed-q inputs access-plans]
  (let [[parsed-q inputs] (plugin-inputs parsed-q inputs)
        udf-db            (first (filter db/-searchable? inputs))]
    (binding [built-ins/*udf-db* udf-db]
      (prepare-context parsed-q inputs access-plans false))))

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
            outer     (execute-access-subquery
                        :outer #(execute-query outer-query inputs))
            [tuples candidate-work]
            (execute-complete-access-source source source-db outer)]
        (execute-access-batch
          parsed-q residual-query inputs source-db access-plan tuples
          {:source         :correlated-access
           :candidate-work candidate-work
           :exhausted?     true}))

      :complete
      (let [source-db (access-source-db source inputs)
            [tuples candidate-work]
            (execute-complete-access-source source source-db nil)]
        (execute-access-batch
          parsed-q residual-query inputs source-db access-plan tuples
          {:source         :complete-access
           :candidate-work candidate-work
           :exhausted?     true}))

      (do
        (when *access-execution*
          (vswap! *access-execution* assoc :fallback
                  {:kind :conventional :reason :unsupported-access-mode}))
        (execute-access-subquery
          :fallback #(execute-query parsed-q inputs))))))

(defmethod execute-alternative :conventional
  [_parsed-q inputs _access-plans alternative]
  (execute-planned-query
    (assoc (get-in alternative [:plan :context]) :run? true) inputs))

(defmethod execute-alternative :default
  [parsed-q inputs access-plans _alternative]
  (execute-query parsed-q inputs access-plans))

(defn- access-execution-plan
  [context alternative execution actual-size]
  (let [{:keys [mode source access-plan operators fragment-cols]}
        (:plan alternative)
        batches (:batches execution)
        sum-batches
        (fn [key]
          (reduce + 0 (map #(long (or (get % key) 0)) batches)))]
    (cond->
        {:kind                  :access
         :mode                  mode
         :steps                 [(qplan/step-explain source context)]
         :access-plan           (qaccess/plan-summary access-plan)
         :operators             operators
         :fragment-cols         fragment-cols
         :cost                  (:cost alternative)
         :size                  (:size alternative)
         :actual-size           actual-size
         :candidate-count       (sum-batches :candidate-count)
         :candidate-work        (sum-batches :candidate-work)
         :fragment-result-count (sum-batches :fragment-result-count)
         :residual-result-count (sum-batches :residual-result-count)
         :batches               (vec batches)
         :subqueries            (vec (:subqueries execution))}
      (:fallback execution) (assoc :fallback (:fallback execution)))))

(defn q*
  [parsed-q inputs]
  (binding [timeout/*deadline* (timeout/effective-deadline
                                 (:qtimeout parsed-q))]
    (let [shape    (or (get parsed-q point-lookup-projection-key)
                       (point-lookup-projection-shape parsed-q))
          database (when shape (point-lookup-projection-db shape inputs))]
      (if database
        (execute-point-lookup-projection parsed-q shape database (second inputs))
        (let [plans (discover-access-plans parsed-q inputs)]
          (if (seq plans)
            (let [planned-context (access-query-plan parsed-q inputs plans)
                  alternative     (qo/selected-alternative planned-context)]
              (if qplan/*explain*
                (if (= :access (:kind alternative))
                  (let [execution   (volatile! {:batches [] :subqueries []})
                        result
                        (binding [qplan/*explain*    nil
                                  *access-execution* execution]
                          (execute-alternative
                            parsed-q inputs (:access-plans planned-context)
                            alternative))
                        actual-size (query-result-size parsed-q result)
                        explained-context
                        (assoc planned-context
                               :run? true
                               :explain-actual-result-size actual-size
                               :access-path-execution
                               (access-execution-plan
                                 planned-context alternative @execution
                                 actual-size))]
                    (result-explain explained-context result)
                    result)
                  (let [deferred         (volatile! nil)
                        result
                        (binding [*deferred-result-explain* deferred]
                          (execute-alternative
                            parsed-q inputs (:access-plans planned-context)
                            alternative))
                        executed-context (:context @deferred)]
                    (result-explain
                      (assoc executed-context
                             :property-memo (:property-memo planned-context))
                      result)
                    result))
                (execute-alternative
                  parsed-q inputs (:access-plans planned-context)
                  alternative)))
            (execute-query parsed-q inputs plans)))))))

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
      (prepare-context parsed-q inputs plans false))))

(defn plan*
  [parsed-q inputs]
  (let [shape    (or (get parsed-q point-lookup-projection-key)
                     (point-lookup-projection-shape parsed-q))
        database (when shape (point-lookup-projection-db shape inputs))]
    (if database
      (explain-point-lookup-projection-plan! shape)
      (result-explain (plan-context* parsed-q inputs)))))

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
            (raise "Streaming plan count does not support input relations"
                       {:relation-count (count rels)})

            (seq late-clauses)
            (do
              (when-not (= 1 (count component-plans))
                (raise
                    "Streaming late-clause count requires one connected plan"
                    {:component-count (count component-plans)
                     :late-clauses late-clauses}))
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
