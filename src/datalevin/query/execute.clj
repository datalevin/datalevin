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
   [datalevin.bits :as b]
   [datalevin.built-ins :as built-ins]
   [datalevin.constants :as c]
   [datalevin.db :as db]
   [datalevin.inline :refer [update assoc]]
   [datalevin.join :as j]
   [datalevin.parser :as dp]
   [datalevin.pipe :as p]
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
   [datalevin.util :as u :refer [cond+ raise concatv map+]])
  (:import
   [java.util Collection Comparator List]
   [datalevin.parser Constant FindColl FindRel FindScalar FindTuple Function
    Pattern Variable]
   [org.eclipse.collections.impl.list.mutable FastList]))

(def ^:private plugin-inputs qo/plugin-inputs)

(def ^:private rewrite-unused-vars qo/rewrite-unused-vars)

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
      qo/plan-not-joins))

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
          (if run? (reduce qresolve/resolve-clause c (:late-clauses c)) c))))))

(defn -collect-tuples
  [acc rel ^long len copy-map]
  (->Eduction
    (comp
      (map (fn [^objects t1]
             (->Eduction
               (map (fn [t2]
                      (let [res (aclone t1)]
                        (if (u/array? t2)
                          (dotimes [i len]
                            (when-some [idx (aget ^objects copy-map i)]
                              (aset res i (aget ^objects t2 idx))))
                          (dotimes [i len]
                            (when-some [idx (aget ^objects copy-map i)]
                              (aset res i (get t2 idx)))))
                        res)))
               (:tuples rel))))
      cat)
    acc))

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

     :let [copy-map (to-array (map #(get keep-attrs %) symbols))
           len      (count symbols)]

     :else
     (recur (-collect-tuples acc rel len copy-map) (next rels) symbols))))

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
                               run? (assoc :actual-size
                                           (get-in @(:intermediates context)
                                                   [(:out (last steps))
                                                    :tuples-count]))))
                           e)) plan)
               :late-clauses late-clauses)))))

(defn- order-comp
  [tg idx di]
  (if (identical? di :asc)
    (fn [t1 t2] (compare (tg t1 idx) (tg t2 idx)))
    (fn [t1 t2] (compare (tg t2 idx) (tg t1 idx)))))

(defn- order-comps
  [tg find-vars order]
  (let [pairs (partition-all 2 order)
        idxs  (mapv (fn [v]
                      (if (integer? v)
                        v
                        (u/index-of #(= v %) find-vars)))
                    (into [] (map first) pairs))
        comps (reverse (mapv #(order-comp tg %1 %2) idxs
                             (into [] (map second) pairs)))]
    (reify Comparator
      (compare [_ t1 t2]
        (loop [comps comps res (num 0)]
          (if (not-empty comps)
            (recur (next comps) (let [r ((first comps) t1 t2)]
                                  (if (= 0 r) res r)))
            res))))))

(defn- order-result
  [find-vars result order]
  (if (seq result)
    (sort (order-comps (tuple-get (first result)) find-vars order) result)
    result))

(defn q*
  [parsed-q inputs]
  (binding [timeout/*deadline* (timeout/to-deadline (:qtimeout parsed-q))]
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
      (if-let [order (:qorder parsed-q)]
        (if (instance? FindRel find)
          (order-result find-vars result order)
          result)
        result))))

(defn mark-parsing-finished!
  []
  (when qplan/*explain*
    (vswap! qplan/*explain* assoc :parsing-time
            (- (System/nanoTime) ^long qplan/*start-time*))))

(defn plan*
  [parsed-q inputs]
  (binding [timeout/*deadline* (timeout/to-deadline (:qtimeout parsed-q))]
    (let [[parsed-q inputs] (plugin-inputs parsed-q inputs)]
      (-> (qplan/make-context parsed-q false)
          (qresolve/resolve-ins inputs)
          (resolve-redudants)
          (rules/rewrite)
          (rewrite-unused-vars)
          (-q false)
          (result-explain)))))
