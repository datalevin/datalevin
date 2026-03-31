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
   [datalevin.query.resolve :as qresolve]
   [datalevin.query-util :as qu]
   [datalevin.relation :as r]
   [datalevin.rules :as rules]
   [datalevin.spill :as sp]
   [datalevin.timeout :as timeout]
   [datalevin.util :as u :refer [cond+ raise concatv map+]])
  (:import
   [java.util Collection Comparator List]
   [java.util.concurrent Callable ExecutorService Executors Future]
   [datalevin.parser Constant FindColl FindRel FindScalar FindTuple Function
    Pattern Variable]
   [datalevin.utl LRUCache]
   [org.eclipse.collections.impl.list.mutable FastList]))

(def ^:dynamic *plan-cache* (LRUCache. c/query-result-cache-size))

(def ^:dynamic *explain* nil)

(def ^:dynamic *cache?* true)

(def ^:dynamic *start-time* nil)

(def ^:dynamic *execute-deps* nil)

(def ^:private plugin-inputs qo/plugin-inputs)

(def ^:private rewrite-unused-vars qo/rewrite-unused-vars)

(def intersect-ranges qo/intersect-ranges)

(defn- dep [k]
  (or (get *execute-deps* k)
      (raise "Missing query execute dependency " k {:dependency k})))

(defn- step-execute [step db source]
  ((dep :step-execute) step db source))

(defn- step-execute-pipe [step db source sink]
  ((dep :step-execute-pipe) step db source sink))

(defn- step-explain [step context]
  ((dep :step-explain) step context))

(defn- planning [context]
  ((dep :planning) context))

(defn- make-context [parsed-q run?]
  ((dep :make-context) parsed-q run?))

(defn- plan? [x]
  ((dep :plan?) x))

(defn- parsed-q [q]
  ((dep :parsed-q) q))

(defn- q-result [parsed-q inputs]
  ((dep :q-result) parsed-q inputs))

(defn- save-intermediates
  [context steps ^objects sinks ^List tuples]
  (when-let [res (:intermediates context)]
    (vswap! res merge
            (u/reduce-indexed
              (fn [m step i]
                (assoc m (:out step)
                       {:tuples-count
                        (let [sink (aget sinks i)]
                          (if (p/pipe? sink)
                            (p/total sink)
                            (.size ^Collection (p/remove-end-scan sink))))}))
              {(:out (peek steps)) {:tuples-count (.size tuples)}}
              (butlast steps)))))

(defn cols->attrs
  [cols]
  (u/reduce-indexed
    (fn [m col i]
      (let [v (if (set? col)
                (some #(when (symbol? %) %) col)
                col)]
        (assoc m v i)))
    {} cols))

(declare execute-steps hash-join-execute-into)

(defn hash-join-execute
  [db in-cols tgt-steps ^List tuples]
  (let [out (FastList. (.size tuples))]
    (hash-join-execute-into db in-cols tgt-steps tuples out)
    out))

(defn hash-join-execute-into
  ([db in-cols tgt-steps tuples sink]
   (when (and tuples (pos? (.size ^List tuples)))
     (let [tgt-rel (execute-steps nil db tgt-steps)]
       (hash-join-execute-into in-cols tgt-rel tuples sink))))
  ([in-cols tgt-rel tuples sink]
   (when (and tuples (pos? (.size ^List tuples)))
     (let [in-rel (r/relation! (cols->attrs in-cols) tuples)]
       (j/hash-join-into in-rel tgt-rel sink)))))

(defn- build-sip-bitmap
  "Build a 64-bit bitmap from the values at col-idx in input tuples"
  [^List input ^long col-idx]
  (let [bm (b/bitmap64)]
    (dotimes [i (.size input)]
      (let [tuple ^objects (.get input i)
            v     (aget tuple col-idx)]
        (when (integer? v)
          (b/bitmap64-add bm (long v)))))
    bm))

(defn- values->ranges
  "Convert a collection of values to single-value ranges"
  [values]
  (mapv (fn [v] [[:closed v] [:closed v]]) values))

(defn- compose-pred
  "Compose a new predicate with an existing one"
  [existing-pred new-pred]
  (if existing-pred
    (fn [v] (and (existing-pred v) (new-pred v)))
    new-pred))

(defn- find-attr-in-attrs-v
  "Find the index of attr in attrs-v and return [index opts]"
  [attrs-v attr]
  (reduce-kv
    (fn [_ i [a opts]]
      (when (= a attr)
        (reduced [i opts])))
    nil attrs-v))

(defn- modify-init-step-for-sip
  "Modify InitStep with SIP optimization - either ranges or bitmap pred"
  [init-step bm]
  (let [cardinality (b/bitmap64-cardinality bm)]
    (if (<= cardinality ^long c/sip-range-threshold)
      (let [values     (b/bitmap64->longs bm)
            new-ranges (values->ranges values)
            old-range  (:range init-step)]
        (assoc init-step :range (if old-range
                                  (intersect-ranges old-range new-ranges)
                                  new-ranges)))
      (let [min-v     (b/bitmap64-min bm)
            max-v     (b/bitmap64-max bm)
            new-range [[[:closed min-v] [:closed max-v]]]
            old-range (:range init-step)
            bm-pred   (fn [v] (b/bitmap64-contains? bm v))
            old-pred  (:pred init-step)]
        (assoc init-step
               :range (if old-range
                        (intersect-ranges old-range new-range)
                        new-range)
               :pred (compose-pred old-pred bm-pred))))))

(defn- modify-merge-scan-step-for-sip
  "Modify MergeScanStep attrs-v to add bitmap predicate for join attr"
  [merge-step bm join-attr]
  (let [attrs-v (:attrs-v merge-step)
        bm-pred (fn [v] (b/bitmap64-contains? bm v))
        new-attrs-v
        (mapv (fn [[a opts :as entry]]
                (if (= a join-attr)
                  [a (update opts :pred #(compose-pred % bm-pred))]
                  entry))
              attrs-v)]
    (assoc merge-step :attrs-v new-attrs-v)))

(defn- apply-sip-to-tgt-steps
  "Apply SIP optimization to target steps for :_ref link type"
  [tgt-steps bm join-attr]
  (let [init-step (first tgt-steps)
        init-attr (:attr init-step)]
    (if (= init-attr join-attr)
      (assoc (vec tgt-steps) 0
             (modify-init-step-for-sip init-step bm))
      (if (< 1 (count tgt-steps))
        (let [merge-step (second tgt-steps)
              attrs-v    (:attrs-v merge-step)]
          (if (find-attr-in-attrs-v attrs-v join-attr)
            (assoc (vec tgt-steps) 1
                   (modify-merge-scan-step-for-sip merge-step bm join-attr))
            tgt-steps))
        tgt-steps))))

(defn sip-execute-pipe
  "Execute hash join with SIP (Sideways Information Passing) optimization.
   Called when SIP is determined to be beneficial."
  [db link link-e in-cols tgt-steps ^FastList input sink]
  (let [join-attr   (:attr link)
        col-idx     (qo/find-index link-e in-cols)
        bm          (build-sip-bitmap input col-idx)
        cardinality (b/bitmap64-cardinality bm)]
    (when (pos? cardinality)
      (let [modified-tgt-steps (apply-sip-to-tgt-steps tgt-steps bm join-attr)
            tgt-rel            (execute-steps nil db modified-tgt-steps)]
        (hash-join-execute-into in-cols tgt-rel input sink)))))

(defn sip-hash-join-execute
  "Execute hash join with SIP optimization (for -execute path)"
  [db link link-e in-cols tgt-steps input]
  (when (and input (pos? (.size ^List input)))
    (let [join-attr   (:attr link)
          col-idx     (qo/find-index link-e in-cols)
          bm          (build-sip-bitmap input col-idx)
          cardinality (b/bitmap64-cardinality bm)]
      (if (pos? cardinality)
        (let [modified-tgt-steps (apply-sip-to-tgt-steps tgt-steps bm join-attr)
              tgt-rel            (execute-steps nil db modified-tgt-steps)
              out                (FastList. cardinality)]
          (hash-join-execute-into in-cols tgt-rel input out)
          out)
        (FastList.)))))

(def pipe-thread-pool (Executors/newCachedThreadPool))

(defn- pipelining
  [context db attrs steps n]
  (let [n-1    (dec ^long n)
        tuples (FastList. (int c/init-exec-size-threshold))
        pipes  (object-array (repeatedly n-1 #(if *explain*
                                                (p/counted-tuple-pipe)
                                                (p/tuple-pipe))))
        work   (fn [step ^long i]
                 (if (zero? i)
                   (step-execute-pipe step db nil (aget pipes 0))
                   (let [src (aget pipes (dec i))]
                     (if (= i n-1)
                       (step-execute-pipe step db src tuples)
                       (step-execute-pipe step db src (aget pipes i))))))
        finish #(when (not= % n-1) (p/finish (aget pipes %)))]
    (if (qo/writing? db)
      (dotimes [i n]
        (let [step (nth steps i)]
          (try
            (work step i)
            (finally (finish i)))))
      (let [bindings (get-thread-bindings)
            tasks    (mapv (fn [step i]
                             ^Callable
                             #(with-bindings bindings
                                (try
                                  (work step i)
                                  (catch Throwable e
                                    (raise "Error in executing step" i e
                                           {:step step}))
                                  (finally
                                    (finish i)))))
                           steps (range))]
        (doseq [^Future f (.invokeAll ^ExecutorService pipe-thread-pool tasks)]
          (.get f))))
    (p/remove-end-scan tuples)
    (save-intermediates context steps pipes tuples)
    (r/relation! attrs tuples)))

(defn execute-steps
  "execute all steps of a component's plan to obtain a relation"
  [context db steps]
  (let [steps (vec steps)
        n     (count steps)
        attrs (cols->attrs (:cols (peek steps)))]
    (condp = n
      1 (let [tuples (step-execute (first steps) db nil)]
          (save-intermediates context steps nil tuples)
          (r/relation! attrs tuples))
      2 (let [src    (step-execute (first steps) db nil)
              tuples (step-execute (peek steps) db src)]
          (save-intermediates context steps (object-array [src]) tuples)
          (r/relation! attrs tuples))
      (pipelining context db attrs steps n))))

(defn execute-plan
  [{:keys [plan sources] :as context}]
  (if (= 1 (transduce (map (fn [[_ components]] (count components))) + plan))
    (update context :rels qresolve/collapse-rels
            (let [[src components] (first plan)
                  all-steps        (vec (mapcat :steps (first components)))]
              (execute-steps context (sources src) all-steps)))
    (reduce
      (fn [c r] (update c :rels qresolve/collapse-rels r))
      context (->> plan
                   (mapcat (fn [[src components]]
                             (let [db (sources src)]
                               (for [plans components]
                                 [db (mapcat :steps plans)]))))
                   (map+ #(apply execute-steps context %))
                   (sort-by #(count (:tuples %)))))))

(defn- plan-explain
  []
  (when *explain*
    (let [{:keys [^long parsing-time ^long building-time]} @*explain*]
      (vswap! *explain* assoc :planning-time
              (- ^long (System/nanoTime)
                 (+ ^long *start-time* parsing-time building-time))))))

(defn- build-explain
  []
  (when *explain*
    (let [{:keys [^long parsing-time]} @*explain*]
      (vswap! *explain* assoc :building-time
              (- ^long (System/nanoTime)
                 (+ ^long *start-time* parsing-time))))))

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
   (when *explain* (vswap! *explain* assoc :result result)))
  ([{:keys [graph result-set plan opt-clauses late-clauses run?] :as context}]
   (when *explain*
     (let [{:keys [^long planning-time ^long parsing-time ^long building-time]}
           @*explain*
           et  (double (/ (- ^long (System/nanoTime)
                             (+ ^long *start-time* planning-time
                                parsing-time building-time))
                          1000000))
           bt  (double (/ building-time 1000000))
           plt (double (/ planning-time 1000000))
           pat (double (/ parsing-time 1000000))
           ppt (double (/ (+ parsing-time building-time planning-time)
                          1000000))]
       (vswap! *explain* assoc
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
                         (if (plan? e)
                           (let [{:keys [steps] :as plan} e]
                             (cond->
                                 (assoc plan :steps
                                        (mapv #(step-explain % context) steps))
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
            (-> (make-context parsed-q true)
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

(defn- parse-explain
  []
  (when *explain*
    (vswap! *explain* assoc :parsing-time
            (- (System/nanoTime) ^long *start-time*))))

(defn perform
  [q & inputs]
  (let [parsed-q (parsed-q q)
        _        (parse-explain)
        result   (q-result parsed-q inputs)]
    (if (instance? FindRel (:qfind parsed-q))
      (let [limit  (:qlimit parsed-q)
            offset (:qoffset parsed-q)]
        (->> result
             (#(if offset (drop offset %) %))
             (#(if (or (nil? limit) (= limit -1)) % (take limit %)))))
      result)))

(defn plan-only
  [q & inputs]
  (let [parsed-q (parsed-q q)]
    (parse-explain)
    (binding [timeout/*deadline* (timeout/to-deadline (:qtimeout parsed-q))]
      (let [[parsed-q inputs] (plugin-inputs parsed-q inputs)]
        (-> (make-context parsed-q false)
            (qresolve/resolve-ins inputs)
            (resolve-redudants)
            (rules/rewrite)
            (rewrite-unused-vars)
            (-q false)
            (result-explain))))))

(defn explain*
  [{:keys [run?] :or {run? false}} & args]
  (binding [*explain*    (volatile! {})
            *cache?*     false
            *start-time* (System/nanoTime)]
    (if run?
      (do (apply perform args) @*explain*)
      (do (apply plan-only args)
          (dissoc @*explain* :actual-result-size :execution-time)))))
