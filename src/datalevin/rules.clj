;;
;; Copyright (c) Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns ^:no-doc datalevin.rules
  "Rule parsing, stratification, recursive evaluation, and optimization dispatch."
  (:require
   [clojure.edn :as edn]
   [clojure.set :as set]
   [clojure.walk :as walk]
   [datalevin.constants :as c]
   [datalevin.db :as db]
   [datalevin.join :as j]
   [datalevin.parser :as dp]
   [datalevin.query-util :as qu]
   [datalevin.relation :as r]
   [datalevin.rules.clause
    :refer [bound-arg-indices clause-bound-vars clause-required-vars
            context-bound-vars ensure-src ensure-src-where external-rule-heads
            recursive-branch? rename-rule rule-args rule-call? rule-head
            source stable-head-idxs]]
   [datalevin.rules.eav :refer [eval-linear-eav-branch-with-dedup]]
   [datalevin.rules.eav-specialize
    :refer [add-bound-transitive-tuple!
            bound-transitive-full-scan-min-observations
            bound-transitive-full-scan-min-pending
            bound-transitive-full-scan-work-ratio build-long-eav-adjacency
            eval-bound-linear-eav eval-bound-synchronized-eav
            eval-full-linear-eav eval-full-synchronized-eav
            eval-full-transitive-eav linear-eav-rule-path
            singleton-bound-argument singleton-bound-binary-result
            synchronized-eav-rule-plan transitive-eav-rule-plan
            transitive-output-saturated?
            ;; Resolved here by tests in the sibling test project.
            #_{:clj-kondo/ignore [:unused-referred-var]}
            dense-synchronized-domain?]]
   [datalevin.rules.magic
    :refer [binding-pattern bound-indices magic-effective? magic-head?
            magic-name magic-rewrite-program magic-rules-size magic-seed-rel
            positive-recursive?]]
   [datalevin.rules.order
    :refer [reorder-clauses
            ;; Resolved here by tests in the sibling test project.
            #_{:clj-kondo/ignore [:unused-referred-var]}
            cached-rule-rel-size
            #_{:clj-kondo/ignore [:unused-referred-var]}
            estimate-clause-size]]
   [datalevin.rules.relation
    :refer [empty-rel-for-rule extract-delta-bound-values map-rule-result
            project-rule-result project-rule-result-distinct rename-rel-attrs
            unique-seeds]]
   [datalevin.util :as u :refer [raise concatv cond+]])
  (:import
   [datalevin.datom Datom]
   [datalevin.db DB]
   [org.eclipse.collections.impl.list.mutable FastList]
   [org.eclipse.collections.impl.list.mutable.primitive LongArrayList]
   [org.eclipse.collections.impl.map.mutable.primitive LongObjectHashMap]
   [org.eclipse.collections.impl.set.mutable.primitive LongHashSet]
   [java.util List HashSet]))

(declare solve-stratified solve-stratified* recursive? expand-clauses)

(defn parse-rules
  [rules]
  (let [rules (if (string? rules) (edn/read-string rules) rules)]
    (dp/parse-rules rules) ;; validation
    (group-by ffirst rules)))

(defprotocol INode
  (lowlink [this])
  (index [this])
  (on-stack [this])
  (set-lowlink [this l])
  (set-index [this i])
  (set-on-stack [this s]))

(deftype Node [value
               ^:unsynchronized-mutable index
               ^:unsynchronized-mutable lowlink
               ^:unsynchronized-mutable on-stack]
  INode
  (lowlink [_] lowlink)
  (index [_] index)
  (on-stack [_] on-stack)
  (set-lowlink [_ l] (set! lowlink l))
  (set-index [_ i] (set! index i))
  (set-on-stack [_ s] (set! on-stack s)))

(defn- init-nodes
  [graph]
  (let [vs      (keys graph)
        v->node (zipmap vs (map #(Node. % nil nil false) vs))]
    (reduce-kv (fn [m v node] (assoc m node (map v->node (graph v))))
               {} v->node)))

(defn tarjans-scc
  "returns SCCs in reverse topological order"
  [graph]
  (let [nodes (init-nodes graph)
        cur   (volatile! 0)
        stack (volatile! '())
        sccs  (volatile! '())
        connect
        (fn connect [^Node node]
          (set-index node @cur)
          (set-lowlink node @cur)
          (vswap! cur u/long-inc)
          (vswap! stack conj node)
          (set-on-stack node true)
          (doseq [^Node tgt (nodes node)]
            (if (nil? (index tgt))
              (do (connect tgt)
                  (set-lowlink node (min ^long (lowlink node)
                                         ^long (lowlink tgt))))
              (when (on-stack tgt)
                (set-lowlink node (min ^long (lowlink node)
                                       ^long (index tgt))))))
          (when (= (lowlink node) (index node))
            (let [w   (volatile! nil)
                  scc (volatile! #{})]
              (while (not= @w node)
                (let [^Node n (peek @stack)]
                  (vswap! stack pop)
                  (set-on-stack n false)
                  (vswap! scc conj (.-value n))
                  (vreset! w n)))
              (vswap! sccs conj @scc))))]
    (doseq [node (keys nodes)]
      (when (nil? (index node)) (connect node)))
    @sccs))

(defn dependency-graph
  [rules]
  (let [graph
        (reduce-kv
          (fn [g head branches]
            (reduce
              (fn [g branch]
                (let [[_ & clauses] branch]
                  (reduce
                    (fn [g clause]
                      (if (sequential? clause)
                        (let [sym (first clause)]
                          (if (rules sym)
                            (update g head (fnil conj #{}) sym)
                            g))
                        g))
                    g clauses)))
              (update g head #(or % #{})) ;; Ensure node exists
              branches))
          {} rules)]
    ;; Preserve stratification metadata on the graph for reuse
    ;; (SCC + temporal cache).
    (with-meta graph {:sccs               (delay (tarjans-scc graph))
                      :temporal-idx-cache (volatile! {})})))

(defn dependency-sccs
  [deps]
  (if-let [sccs (:sccs (meta deps))]
    (if (delay? sccs) @sccs sccs)
    (tarjans-scc deps)))

;; SNE

(defn- solve-rule-call-relation
  [context clause resolve-fn]
  (let [src  (source clause)
        head (rule-head clause)
        args (rule-args clause)]
    (if src
      (binding [qu/*implicit-source* (get (:sources context) src)]
        (solve-stratified context head args resolve-fn))
      (solve-stratified context head args resolve-fn))))

(defn- attach-singleton-head-seeds
  "Reattach head bindings that a bound lookup proved and elided from its body
   relation. Only a singleton seed is safe to reattach without a correlation
   key; multi-valued seeds must remain represented in the resolved body."
  [rel context head-vars]
  (reduce
    (fn [rel head-var]
      (if (contains? (:attrs rel) head-var)
        rel
        (if-let [seed
                 (some
                   (fn [candidate]
                     (let [^List tuples (:tuples candidate)]
                       (when (and (contains? (:attrs candidate) head-var)
                                  tuples
                                  (= 1 (.size tuples)))
                         (r/project-distinct candidate [head-var]))))
                   (:rels context))]
          (j/hash-join rel seed)
          rel)))
    rel head-vars))

(defn- eval-rule-branch
  "Evaluate a rule branch as a set-valued head relation. When the last body
   clause is itself a rule call, stream its join with the already evaluated
   prefix into the distinct head projection."
  [context clauses head-vars resolve-fn]
  (let [clauses  (vec clauses)
        terminal (peek clauses)]
    (if (and terminal (rule-call? context terminal))
      (let [prefix-context (reduce resolve-fn context (pop clauses))
            prefix-rels    (:rels prefix-context)]
        (if (some r/rel-empty prefix-rels)
          (r/relation! (zipmap head-vars (range)) (FastList.))
          (let [terminal-rel (solve-rule-call-relation prefix-context terminal
                                                       resolve-fn)]
            (if (seq prefix-rels)
              (j/hash-join-project-distinct
                (attach-singleton-head-seeds
                  (reduce j/hash-join prefix-rels) context head-vars)
                terminal-rel head-vars)
              (project-rule-result-distinct
                (attach-singleton-head-seeds terminal-rel context head-vars)
                head-vars)))))
      (let [prefix-context   (reduce resolve-fn context (pop clauses))
            body-res-context (resolve-fn
                               (assoc
                                 prefix-context
                                 :datalevin.rules/distinct-projection-vars
                                 head-vars)
                               terminal)
            body-rel         (-> (reduce j/hash-join (:rels body-res-context))
                                 (attach-singleton-head-seeds
                                   context head-vars))]
        (project-rule-result-distinct body-rel head-vars)))))

(defn- eval-rule-body
  [context rule-name rule-branches resolve-fn]
  (let [context (assoc context :current-rule rule-name)]
    (reduce
      (fn [rel branch]
        (let [delta-bounds   (extract-delta-bound-values context branch)
              branch-context (cond-> context
                               delta-bounds
                               (assoc :delta-bound-values delta-bounds))
              [[_ & args] & clauses] branch
              ordered-clauses        (reorder-clauses clauses context)
              projected-rel          (eval-rule-branch branch-context
                                                       ordered-clauses args
                                                       resolve-fn)]
          (r/sum-rel-dedupe rel projected-rel)))
      (empty-rel-for-rule rule-name (:rules context))
      rule-branches)))

(defn- eval-rule-body-with-dedup
  [context rule-name rule-branches resolve-fn ^HashSet seen-set]
  (let [context (assoc context
                       :current-rule rule-name
                       :single-recursive-branch? (= 1 (count rule-branches)))]
    (reduce
      (fn [rel branch]
        (if-let [fast-rel (eval-linear-eav-branch-with-dedup
                            context branch seen-set)]
          (r/sum-rel rel fast-rel)
          (let [delta-bounds   (extract-delta-bound-values context branch)
                branch-context (cond-> context
                                 delta-bounds
                                 (assoc :delta-bound-values delta-bounds))
                [[_ & args] & clauses] branch
                ordered-clauses        (reorder-clauses clauses context)
                body-res-context       (reduce resolve-fn branch-context
                                               ordered-clauses)
                body-rel               (reduce j/hash-join
                                           (:rels body-res-context))
                projected-rel          (project-rule-result body-rel args)
                deduped-rel            (r/difference-with-seen! projected-rel
                                                                seen-set)]
            (r/sum-rel rel deduped-rel))))
      (empty-rel-for-rule rule-name (:rules context))
      rule-branches)))

(defn- branch-requires?
  [branch var context]
  (loop [clauses (rest branch) bound #{}]
    (if (empty? clauses)
      false
      (let [clause       (first clauses)
            required     (clause-required-vars clause context)
            next-clauses (rest clauses)]
        (cond
          (and (seq? clause) (= 'and (first clause)))
          (recur (concatv (rest clause) next-clauses) bound)

          (and (some #{var} required) (not (bound var)))
          true

          :else
          (recur next-clauses
                 (into bound (clause-bound-vars clause context))))))))

(defn- required-seeds
  [branches head-vars context]
  (into #{}
        (keep-indexed
          (fn [idx var]
            (when (some #(branch-requires? % var context) branches)
              idx)))
        head-vars))

(def ^:dynamic *temporal-elimination* false)

(def ^:dynamic *auto-optimize-temporal* true)

(def ^:dynamic *keep-temporal-intermediates* false)

(def ^:dynamic *magic-rewrite?* true)

(def ^:dynamic *bound-transitive-eav?* true)

(def ^:dynamic *bound-transitive-full-scan?* true)

(def ^:dynamic *bound-transitive-saturation?* true)

(def ^:dynamic *full-transitive-eav?* true)

(def ^:dynamic *bound-linear-eav?* true)

(def ^:dynamic *full-linear-eav?* true)

(def ^:dynamic *bound-synchronized-eav?* true)

(def ^:dynamic *full-synchronized-eav?* true)

;; magic set rewrite

(defn- bound-linear-eav-plan
  [context rule-name args]
  (when (and *bound-linear-eav?*
             (= 2 (count args))
             (nil? (get-in context [:rule-rels rule-name])))
    (let [pattern    (binding-pattern args (context-bound-vars context))
          bound-idxs (vec (bound-indices pattern))]
      (when (= 1 (count bound-idxs))
        (let [bound-idx (long (first bound-idxs))]
          (when-let [links (linear-eav-rule-path context rule-name)]
            (let [database (get-in context [:sources '$])]
              (when (and database
                         (db/-searchable? database)
                         (every?
                           #(identical? :db.type/ref
                                        (get-in (db/-schema database)
                                                [(:attr %) :db/valueType]))
                           links))
                (when-let [bound
                           (singleton-bound-argument
                             context (nth args bound-idx))]
                  (when-let [traversal-value
                             (db/entid database (:value bound))]
                    (assoc
                      {:db database
                       :head-vars
                       (vec (rest (ffirst
                                    (get-in context [:rules rule-name]))))
                       :links links}
                      :bound-idx bound-idx
                      :bound-value (:value bound)
                      :pending-tx? (db/pending-tx-cache? database)
                      :traversal-value traversal-value)))))))))))

(defn- bound-transitive-eav-plan
  [context rule-name args]
  (when (and *bound-transitive-eav?*
             (= 2 (count args))
             (nil? (get-in context [:rule-rels rule-name])))
    (let [pattern    (binding-pattern args (context-bound-vars context))
          bound-idxs (vec (bound-indices pattern))]
      (when (= 1 (count bound-idxs))
        (let [bound-idx (long (first bound-idxs))]
          (when-let [{:keys [attr] :as plan}
                     (transitive-eav-rule-plan context rule-name)]
            (let [database (get-in context [:sources '$])]
              (when (and database
                         (db/-searchable? database)
                         (identical? :db.type/ref
                                     (get-in (db/-schema database)
                                             [attr :db/valueType])))
                (when-let [bound (singleton-bound-argument
                                   context (nth args bound-idx))]
                  (when-let [traversal-value
                             (db/entid database (:value bound))]
                    (assoc plan
                           :db database
                           :bound-idx bound-idx
                           :bound-value (:value bound)
                           :pending-tx? (db/pending-tx-cache? database)
                           :traversal-value traversal-value)))))))))))

(defn- dense-transitive-frontier?
  [^long pending ^long observations ^long observed-edges ^long scan-count]
  (and *bound-transitive-full-scan?*
       (>= observations bound-transitive-full-scan-min-observations)
       (>= pending bound-transitive-full-scan-min-pending)
       (pos? observed-edges)
       (pos? scan-count)
       (>= (* (double pending)
              (/ (double observed-edges) (double observations)))
           (* bound-transitive-full-scan-work-ratio
              (double scan-count)))))

(defn- eval-bound-transitive-eav
  [{:keys [^DB db attr ^long entity-pos
           ^long bound-idx traversal-value pending-tx?]}
   args]
  (let [bound-side     (if (= bound-idx entity-pos) :e :v)
        traversal-node (long traversal-value)
        queue          (LongArrayList.)
        seen           (LongHashSet.)
        cycle?         (boolean-array 1)
        full-scan?     (and *bound-transitive-full-scan?*
                            (db/local-ref-attr-adjacency? db))
        output-domain-size
        (when (and *bound-transitive-saturation?*
                   (not pending-tx?)
                   (= bound-side :e))
          ;; In the forward EAV direction every possible answer is a distinct
          ;; value of this attribute. Once all such values have been reached,
          ;; expanding the remaining queue cannot add an answer.
          (long (db/-cardinality db attr)))
        tuples         (FastList.)]
    (.add seen traversal-node)
    (.add queue traversal-node)
    (if pending-tx?
      ;; Transaction overlays are small and must use transaction-aware datom
      ;; probes, so they deliberately stay on the indexed path.
      (loop [cursor 0]
        (when (< cursor (.size queue))
          (let [node    (.get queue (int cursor))
                pattern (if (= bound-side :e)
                          [node attr nil]
                          [nil attr node])
                datoms  ^List (db/-search db pattern)]
            (when datoms
              (dotimes [i (.size datoms)]
                (let [datom    ^Datom (.get datoms i)
                      neighbor (long (if (= bound-side :e)
                                       (.-v datom)
                                       (.-e datom)))]
                  (if (== neighbor traversal-node)
                    (when-not (aget cycle? 0)
                      (aset cycle? 0 true)
                      (add-bound-transitive-tuple! tuples neighbor))
                    (when (.add seen neighbor)
                      (.add queue neighbor)
                      (add-bound-transitive-tuple! tuples neighbor))))))
            (recur (inc cursor)))))
      (let [switch-cursor
            ;; Indexed probes are optimal while the reachable subgraph is
            ;; small. Once the observed fanout and the already-known pending
            ;; frontier predict work comparable to a full attribute scan,
            ;; return the first unexpanded queue position.
            (loop [cursor        0
                   observed-edges 0
                   scan-count    -1]
              (if (or (>= cursor (.size queue))
                      (transitive-output-saturated?
                        tuples output-domain-size))
                -1
                (let [node      (.get queue (int cursor))
                      pattern   (if (= bound-side :e)
                                  [node attr nil]
                                  [nil attr node])
                      neighbors ^List (db/-search-tuples db pattern)
                      n         (long (if neighbors (.size neighbors) 0))]
                  (when neighbors
                    (dotimes [i n]
                      (let [neighbor
                            (long (aget ^objects (.get neighbors i) 0))]
                        (if (== neighbor traversal-node)
                          (when-not (aget cycle? 0)
                            (aset cycle? 0 true)
                            (add-bound-transitive-tuple! tuples neighbor))
                          (when (.add seen neighbor)
                            (.add queue neighbor)
                            (add-bound-transitive-tuple! tuples neighbor))))))
                  (let [next-cursor   (inc cursor)
                        observations  next-cursor
                        observed      (+ observed-edges n)
                        pending       (- (.size queue) next-cursor)
                        eligible?     (and full-scan?
                                           (>= observations
                                               bound-transitive-full-scan-min-observations)
                                           (>= pending
                                               bound-transitive-full-scan-min-pending))
                        scan-count'   (if (and eligible? (neg? scan-count))
                                        (long (db/-count db [nil attr nil]))
                                        scan-count)]
                    (cond
                      (transitive-output-saturated?
                        tuples output-domain-size)
                      -1

                      (and eligible?
                           (dense-transitive-frontier?
                             pending observations observed scan-count'))
                      next-cursor

                      :else
                      (recur next-cursor observed scan-count'))))))]
        (when-not (neg? (long switch-cursor))
          (let [adjacency ^LongObjectHashMap
                (build-long-eav-adjacency db attr bound-side)]
            (loop [cursor (long switch-cursor)]
              (when (and (< cursor (.size queue))
                         (not (transitive-output-saturated?
                                tuples output-domain-size)))
                (let [node   (.get queue (int cursor))
                      values ^LongArrayList (.get adjacency node)]
                  (when values
                    (dotimes [i (.size values)]
                      (let [neighbor (.get values i)]
                        (if (== neighbor traversal-node)
                          (when-not (aget cycle? 0)
                            (aset cycle? 0 true)
                            (add-bound-transitive-tuple! tuples neighbor))
                          (when (.add seen neighbor)
                            (.add queue neighbor)
                            (add-bound-transitive-tuple! tuples neighbor))))))
                  (recur (inc cursor)))))))))
    (singleton-bound-binary-result tuples args bound-idx)))

(defn- full-synchronized-eav-plan
  [context rule-name args]
  (when (and *full-synchronized-eav?*
             (= 2 (count args))
             (nil? (get-in context [:rule-rels rule-name]))
             (every? #{:f}
                     (binding-pattern args (context-bound-vars context))))
    (when-let [{:keys [base links] :as plan}
               (synchronized-eav-rule-plan context rule-name)]
      (let [database (get-in context [:sources '$])
            attrs    (into [(:attr base)] (map :attr) links)]
        (when (and database
                   (db/-searchable? database)
                   (every?
                     #(identical? :db.type/ref
                                  (get-in (db/-schema database)
                                          [% :db/valueType]))
                     attrs))
          (assoc plan :db database))))))

(defn- full-transitive-eav-plan
  [context rule-name args]
  (when (and *full-transitive-eav?*
             (= 2 (count args))
             (nil? (get-in context [:rule-rels rule-name]))
             (every? #{:f}
                     (binding-pattern args (context-bound-vars context))))
    (when-let [{:keys [attr] :as plan}
               (transitive-eav-rule-plan context rule-name)]
      (let [database (get-in context [:sources '$])]
        (when (and database
                   (db/-searchable? database)
                   (identical? :db.type/ref
                               (get-in (db/-schema database)
                                       [attr :db/valueType])))
          (assoc plan :db database))))))

(defn- full-linear-eav-plan
  [context rule-name args]
  (when (and *full-linear-eav?*
             (= 2 (count args))
             (nil? (get-in context [:rule-rels rule-name]))
             (every? #{:f}
                     (binding-pattern args (context-bound-vars context))))
    (when-let [links (linear-eav-rule-path context rule-name)]
      (let [database (get-in context [:sources '$])]
        (when (and (< 1 (count links))
                   database
                   (db/-searchable? database)
                   (db/local-ref-attr-adjacency? database)
                   (not (db/pending-tx-cache? database))
                   (every?
                     #(identical? :db.type/ref
                                  (get-in (db/-schema database)
                                          [(:attr %) :db/valueType]))
                     links))
          {:db        database
           :head-vars (vec (rest (ffirst
                                   (get-in context [:rules rule-name]))))
           :links     links})))))

(defn- bound-synchronized-eav-plan
  [context rule-name args]
  (when (and *bound-synchronized-eav?*
             (= 2 (count args))
             (nil? (get-in context [:rule-rels rule-name])))
    (let [pattern    (binding-pattern args (context-bound-vars context))
          bound-idxs (vec (bound-indices pattern))]
      (when (= 1 (count bound-idxs))
        (let [bound-idx (long (first bound-idxs))]
          (when-let [{:keys [base links] :as plan}
                     (synchronized-eav-rule-plan context rule-name)]
            (let [database (get-in context [:sources '$])
                  attrs    (into [(:attr base)] (map :attr) links)]
              (when (and database
                         (db/-searchable? database)
                         (every?
                           #(identical? :db.type/ref
                                        (get-in (db/-schema database)
                                                [% :db/valueType]))
                           attrs))
                (when-let [bound (singleton-bound-argument
                                   context (nth args bound-idx))]
                  (when-let [traversal-value
                             (db/entid database (:value bound))]
                    (assoc plan
                           :db database
                           :bound-idx bound-idx
                           :bound-value (:value bound)
                           :traversal-value traversal-value)))))))))))

(defn- variable-dependency
  "look for [(inc ?x) ?y], [(+ ?x 1) ?y], ..."
  [clauses]
  (reduce
    (fn [deps clause]
      (if (vector? clause)
        (let [[call out-var] clause]
          (if (seq? call)
            (let [[f v c] call]
              (cond
                (= 'inc f) (assoc-in deps [out-var v] 1)
                (= '+ f)   (cond
                             (number? c) (assoc-in deps [out-var v] c)
                             (number? v) (assoc-in deps [out-var c] v)
                             :else       deps)))
            deps))
        deps))
    {} clauses))

(defn- extract-attr-dependencies
  [rule-name branches scc-rules]
  (let [edges (volatile! [])]
    (doseq [branch branches]
      (let [[head & clauses] branch
            head-vars        (rest head)
            var-origins      (volatile! {})]

        (doseq [clause clauses]
          (when (sequential? clause)
            (let [c-head (rule-head clause)]
              (when (scc-rules c-head)
                (let [args (rest clause)]
                  (doseq [[idx arg] (map-indexed vector args)]
                    (when (symbol? arg)
                      (vswap! var-origins
                              update arg (fnil conj #{}) [c-head idx]))))))))

        (let [var-deps (variable-dependency clauses)]
          (doseq [[h-idx h-var] (map-indexed vector head-vars)]
            (if-let [origins (@var-origins h-var)]
              (doseq [origin origins]
                (vswap! edges conj [origin [rule-name h-idx] 0]))

              (doseq [[src-var offset] (get var-deps h-var)]
                (when-let [origins (@var-origins src-var)]
                  (doseq [origin origins]
                    (vswap! edges conj
                            [origin [rule-name h-idx] offset])))))))))
    @edges))

(defn- build-attributes-graph
  [scc rules]
  (let [all-edges (mapcat
                    (fn [rname]
                      (extract-attr-dependencies rname (rules rname)
                                                 (set scc)))
                    scc)
        nodes     (into #{}
                        (mapcat (fn [[src tgt _]] [src tgt]))
                        all-edges)]
    (reduce
      (fn [g [src tgt weight]]
        (update g src (fnil conj []) {:target tgt :weight weight}))
      (zipmap nodes (repeat []))
      all-edges)))

(defn- find-temporal-candidates
  [ag]
  (let [simple-graph (reduce-kv (fn [m k v]
                                  (assoc m k (map :target v)))
                                {} ag)]
    (filterv (fn [comp]
               (let [comp-set (set comp)]
                 (some (fn [u]
                         (some (fn [edge]
                                 (and (comp-set (:target edge))
                                      (pos? ^long (:weight edge))))
                               (ag u)))
                       comp)))
             (tarjans-scc simple-graph))))

(defn- build-apdg
  [scc rules temporal-comp ag]
  (let [comp-set   (set temporal-comp)
        scc-set    (set scc)
        ;; Precompute max weights for edges within the temporal component keyed
        ;; by rule heads, so clause scanning is O(1) lookups.
        weight-map (reduce-kv
                     (fn [m src edges]
                       (if (comp-set src)
                         (reduce (fn [m {:keys [target weight]}]
                                   (if (comp-set target)
                                     (update m [(first src) (first target)]
                                             (fnil max 0) weight)
                                     m))
                                 m edges)
                         m))
                     {} ag)]
    (reduce
      (fn [graph r-head]
        (reduce
          (fn [g clause]
            (if (sequential? clause)
              (let [c-head (rule-head clause)]
                (if (scc-set c-head)
                  (update g c-head (fnil conj [])
                          {:target r-head
                           :weight (weight-map [c-head r-head] 0)})
                  g))
              g))
          graph (mapcat rest (rules r-head))))
      {} scc)))

(defn- check-positive-cycles
  [graph nodes]
  (let [zero-graph
        (merge (zipmap nodes (repeat []))
               (reduce-kv
                 (fn [m k edges]
                   (let [zeros (filterv #(zero? ^long (:weight %)) edges)]
                     (if (seq zeros)
                       (assoc m k (mapv :target zeros))
                       m)))
                 {} graph))]
    (every? (fn [comp]
              (let [size (count comp)]
                (cond
                  (> size 1) false
                  (= size 1) (let [node      (first comp)
                                   neighbors (zero-graph node)]
                               (not (some #{node} neighbors)))
                  :else      true)))
            (tarjans-scc zero-graph))))

(defn- temporal-index
  ([scc rules] (temporal-index scc rules nil))
  ([scc rules cache]
   (let [cache-map (when cache @cache)]
     (if (and cache-map (cache-map scc))
       (cache-map scc)
       (let [ag         (build-attributes-graph scc rules)
             candidates (find-temporal-candidates ag)
             res
             (loop [cands candidates]
               (when (seq cands)
                 (let [cand   (first cands)
                       apdg   (build-apdg scc rules cand ag)
                       valid? (check-positive-cycles apdg scc)]
                   (if valid?
                     (let [rname (first scc)
                           match (some #(when (= (first %) rname) %) cand)]
                       (if match
                         (second match)
                         (recur (rest cands))))
                     (recur (rest cands))))))]
         (when cache (vswap! cache assoc scc res))
         res)))))

(defn recursive-stratum?
  [stratum deps rule-name]
  (or (< 1 (count stratum))
      ((deps rule-name) rule-name)))

(defn- context-stratum
  "Return the rules, dependency graph, containing SCC (stratum), and whether
   the stratum is recursive for `rule-name` in `context`."
  [context rule-name]
  (let [rules   (:rules context)
        deps    (or (:rules-deps context) (dependency-graph rules))
        sccs    (dependency-sccs deps)
        stratum (some #(when (% rule-name) %) sccs)]
    {:rules      rules
     :deps       deps
     :stratum    stratum
     :recursive? (when stratum
                   (recursive-stratum? stratum deps rule-name))}))

(defn- rename-stratum-rules
  [rules stratum]
  (reduce
    (fn [m rname]
      (assoc m rname (rename-rule (rules rname))))
    {} stratum))

(defn- precompute-stratum-rels
  [context precompute-context deps full-renamed-rules external-heads
   resolve-clause-fn]
  (reduce
    (fn [m rname]
      (let [branches (full-renamed-rules rname)]
        (cond
          (nil? branches)                        m
          (contains? (:rule-rels context) rname) m
          (recursive? deps rname)                m
          :else
          (let [head-vars (rest (ffirst branches))]
            (assoc m rname
                   (solve-stratified* precompute-context
                                      rname
                                      head-vars
                                      resolve-clause-fn))))))
    {} external-heads))

(defn- build-stratum-seeds
  [context args entry-renamed-branches entry-renamed-head stratum-set
   stratum-recursive?]
  (let [required-indices (required-seeds entry-renamed-branches
                                         entry-renamed-head context)
        bound-indices    (bound-arg-indices args context)
        ;; When recursion preserves certain head vars unchanged, we can
        ;; safely seed on those bound args (lightweight magic-set style)
        stable-indices   (stable-head-idxs entry-renamed-branches
                                           stratum-set)
        magic-indices    (set/intersection bound-indices stable-indices)
        constant-indices (into #{}
                               (keep-indexed
                                 (fn [idx arg]
                                   (when (not (qu/free-var? arg))
                                     idx)))
                               args)
        stable-const-indices (set/intersection stable-indices
                                               constant-indices)
        ;; Seeding recursive strata with constant arguments can drop
        ;; necessary intermediate bindings (e.g. recursive calls that
        ;; introduce new head values), so only seed on required vars
        ;; when recursion is involved. Constants are also safe if the
        ;; corresponding head var is stable through recursion.
        seed-indices (sort
                       (if stratum-recursive?
                         (set/union required-indices magic-indices
                                    stable-const-indices)
                         (set/union required-indices constant-indices
                                    bound-indices)))
        ;; warm start: only when a single outer relation already covers
        ;; all head vars
        warm-start
        (when (and stratum-recursive? (every? qu/free-var? args))
          (when-let [rel (some #(when (every? (set entry-renamed-head)
                                              (keys (:attrs %)))
                                  %)
                               (:rels context))]
            (project-rule-result rel entry-renamed-head)))
        seed-rels
        (reduce
          (fn [rels idx]
            (let [hv  (nth entry-renamed-head idx)
                  arg (nth args idx)]
              (if (qu/free-var? arg)
                ;; Bind to Outer Context Var
                ;; Filter and extract index in single pass to avoid
                ;; repeated (:attrs rel) lookups
                (let [outer-with-idx (into []
                                           (keep (fn [rel]
                                                   (when-let [idx ((:attrs rel)
                                                                   arg)]
                                                     [rel idx])))
                                           (:rels context))]
                  (if (seq outer-with-idx)
                    (into
                      rels
                      (map (fn [[rel idx]]
                             ;; View: Rename attr
                             (r/relation! {hv 0}
                                          (unique-seeds rel idx))))
                      outer-with-idx)
                    rels))
                ;; Bind to Constant
                (conj rels
                      (r/relation! {hv 0}
                                   (doto (FastList.)
                                     (.add (object-array [arg]))))))))
          [] seed-indices)]
    {:seed-rels  seed-rels
     :warm-start warm-start}))

(defn- partition-stratum-branches
  [renamed-rules-map stratum stratum-set full-renamed-rules]
  (let [base-branches-map
        (reduce
          (fn [m rname]
            (let [branches (renamed-rules-map rname)
                  base     (filterv
                             #(not (recursive-branch? % stratum-set))
                             branches)]
              (if (seq base) (assoc m rname base) m)))
          {} stratum)
        rec-branches-map
        (reduce
          (fn [m rname]
            (let [branches (renamed-rules-map rname)
                  rec      (filterv
                             #(recursive-branch? % stratum-set)
                             branches)]
              (if (seq rec) (assoc m rname rec) m)))
          {} stratum)
        ;; Track recursive dependencies so we can skip work when no
        ;; relevant deltas arrived in an iteration.
        stratum-deps
        (reduce
          (fn [m rname]
            (assoc m rname (reduce
                             (fn [acc branch]
                               (reduce
                                 (fn [acc clause]
                                   (if (sequential? clause)
                                     (let [head (rule-head clause)]
                                       (if (stratum-set head)
                                         (conj acc head)
                                         acc))
                                     acc))
                                 acc (rest branch)))
                             #{} (renamed-rules-map rname))))
          {} stratum)
        empty-stratum-rels
        (zipmap stratum
                (mapv #(empty-rel-for-rule % full-renamed-rules)
                      stratum))]
    {:base-branches-map  base-branches-map
     :rec-branches-map   rec-branches-map
     :stratum-deps       stratum-deps
     :empty-stratum-rels empty-stratum-rels}))

(defn- eval-stratum-base-cases
  [context rule-name stratum base-branches-map clean-context
   full-renamed-rules seed-rels base-rule-rels warm-start magic-seeds
   stratum-set resolve-clause-fn]
  (let [start-totals
        (reduce
          (fn [acc rname]
            (let [branches (base-branches-map rname)]
              (assoc acc rname (if branches
                                 (eval-rule-body
                                   (assoc clean-context
                                          :rules full-renamed-rules
                                          :rels seed-rels
                                          :rule-totals base-rule-rels)
                                   rname branches resolve-clause-fn)
                                 (empty-rel-for-rule
                                   rname full-renamed-rules)))))
          {} stratum)
        start-totals (if warm-start
                       (update start-totals rule-name
                               #(r/sum-rel warm-start %))
                       start-totals)]
    (if magic-seeds
      (reduce
        (fn [acc [rname seed-rel]]
          (if (and seed-rel (stratum-set rname))
            (let [head-vars (rest (ffirst (full-renamed-rules rname)))
                  seed-rel  (rename-rel-attrs seed-rel head-vars)]
              (update acc rname r/sum-rel seed-rel))
            acc))
        start-totals magic-seeds)
      start-totals)))

(defn- run-stratum-fixpoint
  [start-totals stratum-recursive? stratum clean-context full-renamed-rules
   seed-rels base-rule-rels empty-stratum-rels rec-branches-map stratum-deps
   temporal-elim? magic-threshold resolve-clause-fn]
  (if-not stratum-recursive?
    start-totals
    ;; Maintain seen-sets for deduplication across iterations.
    ;; This is critical for cyclic graphs where the same tuple can be
    ;; reached through paths of different lengths.
    (let [seen-sets
          (reduce
            (fn [m rname]
              (let [init-rel  (start-totals rname)
                    init-size (if-let [^List ts (:tuples init-rel)]
                                (.size ts)
                                0)
                    capacity  (max 16 (* 4 ^long init-size))
                    seen      (HashSet. (int capacity))]
                (r/add-to-seen! init-rel seen)
                (assoc m rname seen)))
            {} stratum)]
      (loop [totals      start-totals
             deltas      start-totals
             has-deltas? (some r/rel-not-empty (vals start-totals))
             iter        0]
        (if (not has-deltas?)
          totals
          (let [iter-context
                (assoc clean-context
                       :rules full-renamed-rules
                       :rels seed-rels
                       :rule-rels (merge base-rule-rels
                                         empty-stratum-rels
                                         deltas)
                       ;; Only use base-rule-rels for size estimation
                       ;; (pre-computed non-recursive rules), not the
                       ;; stratum's iterative totals which can affect
                       ;; clause ordering in ways that break correctness
                       :rule-totals base-rule-rels)

                ;; Fused tuple production with deduplication.
                eval-one
                (fn [rname]
                  (let [branches (rec-branches-map rname)
                        deps     (stratum-deps rname)
                        dep-delta?
                        (some (fn [dep]
                                (let [rel (deltas dep)]
                                  (and rel (r/rel-not-empty rel))))
                              deps)]
                    (when (and branches dep-delta?)
                      (let [deduped (eval-rule-body-with-dedup
                                      iter-context rname branches
                                      resolve-clause-fn
                                      (seen-sets rname))]
                        (when (r/rel-not-empty deduped)
                          [rname deduped])))))

                new-deltas
                (if (> (count stratum) 1)
                  (into {} (keep identity) (pmap eval-one stratum))
                  (if-let [result (eval-one (first stratum))]
                    {(first result) (second result)}
                    {}))

                new-totals
                (if (and temporal-elim?
                         (not *keep-temporal-intermediates*))
                  (if (some r/rel-not-empty (vals new-deltas))
                    new-deltas
                    totals)
                  (reduce
                    (fn [acc rname]
                      (let [diff (new-deltas rname)]
                        (if diff
                          (update acc rname r/sum-rel diff)
                          acc)))
                    totals stratum))

                ;; Check for magic explosion: if magic rules have grown
                ;; beyond threshold, abort and fall back to non-magic
                _ (when magic-threshold
                    (let [cur-size (magic-rules-size new-totals)]
                      (when (> cur-size ^long magic-threshold)
                        (raise "Magic explosion"
                                        {:type         ::magic-explosion
                                         :current-size cur-size
                                         :threshold    magic-threshold}))))]
            (recur new-totals new-deltas
                   (seq new-deltas) (inc iter))))))))

(defn- solve-stratified*
  [context rule-name args resolve-clause-fn]
  (let [{:keys [rules deps] :as info}
        (context-stratum context rule-name)
        cached-rel (get-in context [:rule-rels rule-name])
        head-vars  (rest (ffirst (rules rule-name)))]
    (if cached-rel
      (map-rule-result cached-rel head-vars args)
      (let [stratum            (:stratum info)
            stratum-recursive? (:recursive? info)]
        (if-not stratum
          (raise "Rule not found in strata" {:rule rule-name})
          (let [stratum-set    (set stratum)
                temporal-cache (:temporal-idx-cache (meta deps))
                ;; 1. Rename rules in stratum to avoid collision & freshen vars
                renamed-rules-map (rename-stratum-rules rules stratum)
                full-renamed-rules (merge rules renamed-rules-map)
                rules-context      (assoc context :rules full-renamed-rules)
                stratum-branches   (mapcat identity (vals renamed-rules-map))
                external-heads     (external-rule-heads stratum-branches
                                                        rules-context
                                                        stratum-set)
                ;; Check if any args are bound (either constants or vars with
                ;; values). If so, skip precomputation - filtering will be more
                ;; efficient.
                has-bound-args?    (some (fn [arg]
                                           (or (not (qu/free-var? arg))
                                               (some #(contains? (:attrs %) arg)
                                                     (:rels context))))
                                         args)
                precompute?        (and stratum-recursive?
                                        (get context :precompute-rule-rels? true)
                                        (seq external-heads)
                                        (not has-bound-args?))
                precompute-context
                (when precompute?
                  (-> context
                      (assoc :rules full-renamed-rules
                             :rules-deps deps
                             :rels []
                             :precompute-rule-rels? false)
                      (dissoc :magic-seeds)))
                precomputed-rels
                (when precompute?
                  (precompute-stratum-rels context precompute-context deps
                                           full-renamed-rules external-heads
                                           resolve-clause-fn))
                base-rule-rels     (merge (:rule-rels context) precomputed-rels)

                ;; Detection of Temporal Elimination
                temporal-idx   (when stratum-recursive?
                                 (temporal-index stratum renamed-rules-map
                                                 temporal-cache))
                temporal-elim? (or *temporal-elimination*
                                   (and *auto-optimize-temporal*
                                        temporal-idx))

                ;; 2. Determine Required Seeds
                entry-renamed-branches (renamed-rules-map rule-name)
                entry-renamed-head     (rest (ffirst entry-renamed-branches))

                {:keys [seed-rels warm-start]}
                (build-stratum-seeds context args entry-renamed-branches
                                     entry-renamed-head stratum-set
                                     stratum-recursive?)

                ;; 3. Build Seed Relations (only for required vars or constants)
                clean-context
                (assoc (select-keys context
                                    [:sources :rule-rels :rules-deps
                                     :magic-seeds])
                       :rules-deps deps
                       :rule-rels base-rule-rels
                       :linear-eav-cache (atom {})
                       :linear-eav-keyed-seen-cache (atom {}))

                ;; Split branches into base (non-recursive) and recursive
                {:keys [base-branches-map rec-branches-map stratum-deps
                        empty-stratum-rels]}
                (partition-stratum-branches renamed-rules-map stratum
                                            stratum-set full-renamed-rules)

                ;; 4. Evaluate Base Cases
                magic-seeds     (:magic-seeds context)
                start-totals    (eval-stratum-base-cases
                                  context rule-name stratum base-branches-map
                                  clean-context full-renamed-rules seed-rels
                                  base-rule-rels warm-start magic-seeds
                                  stratum-set resolve-clause-fn)

                ;; Track initial magic seed size for explosion detection
                init-magic-size (if magic-seeds
                                  (reduce-kv
                                    (fn [^long acc _ rel]
                                      (let [^List ts (:tuples rel)]
                                        (if ts (+ acc (.size ts)) acc)))
                                    0 magic-seeds)
                                  0)

                magic-threshold (when (pos? ^long init-magic-size)
                                  (* ^long init-magic-size
                                     ^long c/magic-explosion-factor))

                final-totals    (run-stratum-fixpoint
                                  start-totals stratum-recursive? stratum
                                  clean-context full-renamed-rules seed-rels
                                  base-rule-rels empty-stratum-rels
                                  rec-branches-map stratum-deps temporal-elim?
                                  magic-threshold resolve-clause-fn)]
            (map-rule-result (final-totals rule-name)
                             entry-renamed-head args)))))))

(def ^:private no-specialized-rule-result (Object.))

(defn- specialized-rule-result
  [context rule-name args]
  (if-let [plan (bound-transitive-eav-plan context rule-name args)]
    (eval-bound-transitive-eav plan args)
    (if-let [plan (bound-synchronized-eav-plan context rule-name args)]
      (eval-bound-synchronized-eav plan args)
      (if-let [plan (bound-linear-eav-plan context rule-name args)]
        (eval-bound-linear-eav plan args)
        (if-let [plan (full-linear-eav-plan context rule-name args)]
          (or (eval-full-linear-eav plan args)
              no-specialized-rule-result)
          (if-let [plan (full-transitive-eav-plan context rule-name args)]
            (or (eval-full-transitive-eav plan args)
                no-specialized-rule-result)
            (if-let [plan (full-synchronized-eav-plan context rule-name args)]
              (or (eval-full-synchronized-eav plan args)
                  no-specialized-rule-result)
              no-specialized-rule-result)))))))

(defn solve-stratified
  [context rule-name args resolve-clause-fn]
  (let [specialized (specialized-rule-result context rule-name args)]
    (if-not (identical? specialized no-specialized-rule-result)
      specialized
        (let [{:keys [rules stratum recursive?]}
              (context-stratum context rule-name)
            bound-vars    (context-bound-vars context)
            base-pattern  (binding-pattern args bound-vars)
            base-bound?   (some #{:b} base-pattern)
            stratum-set    (when stratum (set stratum))
            branches       (when stratum (rules rule-name))
            head-vars      (when branches (rest (ffirst branches)))
            required-idxs (if (and recursive? branches)
                            (required-seeds branches head-vars context)
                            #{})
            stable-idxs   (if (and recursive? branches)
                            (stable-head-idxs branches stratum-set)
                            #{})
            bound-idxs    (set (bound-indices base-pattern))
            magic-idxs    (set/intersection bound-idxs stable-idxs)
            seedable-idxs (set/union required-idxs magic-idxs)
            ;; Avoid over-adornment for recursive rules when extra bound args
            ;; would explode magic seeds; only keep seedable bound indices.
            magic-pattern (if (and recursive? base-bound? (seq stable-idxs))
                            (mapv (fn [idx p]
                                    (if (and (= p :b)
                                             (contains? seedable-idxs idx))
                                      :b
                                      :f))
                                  (range (count base-pattern))
                                  base-pattern)
                            base-pattern)
            has-bound?    (some #{:b} magic-pattern)]
        (if (and *magic-rewrite?*
                 has-bound?
                 recursive?
                 (positive-recursive? rules stratum)
                 (not (magic-head? rule-name))
                 (magic-effective? rules rule-name
                                   (bound-indices magic-pattern) stratum-set))
          ;; Try magic evaluation; fall back to non-magic if explosion detected
          (let [{:keys [rules magic-heads goal]}
                (magic-rewrite-program rules rule-name magic-pattern
                                       stratum-set)
                magic-goal    (magic-name goal)
                b-idxs        (vec (bound-indices magic-pattern))
                bound-args    (mapv #(nth args %) b-idxs)
                head-vars     (get magic-heads magic-goal)
                seed-rel      (magic-seed-rel context head-vars bound-args)
                magic-context (-> context
                                  (assoc :rules rules
                                         :rules-deps (dependency-graph rules)
                                         :magic-seeds {magic-goal seed-rel}))]
            (try
              (binding [*magic-rewrite?* false]
                (solve-stratified* magic-context goal args resolve-clause-fn))
              (catch clojure.lang.ExceptionInfo e
                (if (= ::magic-explosion (:type (ex-data e)))
                  ;; Magic caused explosion, retry without magic
                  (binding [*magic-rewrite?* false]
                    (solve-stratified* context rule-name args
                                       resolve-clause-fn))
                  ;; Re-throw other exceptions
                  (throw e)))))
          (binding [*magic-rewrite?* false]
            (solve-stratified* context rule-name args
                               resolve-clause-fn)))))))

;; rewrite

(defn- recursive?
  [deps rule-name]
  (let [sccs    (dependency-sccs deps)
        scc-map (into {} (mapcat (fn [scc] (map #(vector % scc) scc))) sccs)]
    (recursive-stratum? (scc-map rule-name) deps rule-name)))

(defn- rule-needs-set-boundary?
  "A nested rule needs its predicate boundary when evaluating a branch can
   project away bindings, or when branches can derive the same head tuple.
   Inlining through that boundary preserves answers but retains every proof,
   which can multiply dramatically in a downstream join."
  [rules head]
  (let [branches (get rules head)]
    (or (< 1 (count branches))
        (some
          (fn [[head-clause & body-clauses]]
            (let [head-vars (into #{} (filter qu/free-var?)
                                  (rule-args head-clause))
                  body-vars (into
                              #{}
                              (mapcat #(u/walk-collect % qu/free-var?))
                              body-clauses)]
              (not (set/subset? body-vars head-vars))))
          branches))))

(defn- rule-depends-on-derived-relation?
  [context rules head]
  (boolean
    (some
      (fn [branch]
        (some #(rule-call? context %) (rest branch)))
      (get rules head))))

(defn- expand-rule
  [context to-rm rules deps src head args]
  (let [branches (rules head)
        expanded
        (mapv
          (fn [branch]
            (let [[head-clause & body-clauses] branch

                  head-vars  (rest head-clause)
                  mapping    (zipmap head-vars args)
                  body-vars  (into
                               #{}
                               (mapcat #(u/walk-collect % qu/free-var?))
                               body-clauses)
                  local-vars (set/difference body-vars (set head-vars))
                  local-map  (zipmap local-vars
                                     (mapv #(gensym (name %)) local-vars))
                  full-map   (merge mapping local-map)
                  new-body   (mapv #(ensure-src-where src %)
                                   (walk/postwalk-replace
                                     full-map body-clauses))]
              (expand-clauses context to-rm rules deps new-body true)))
          branches)]
    (if (= 1 (count expanded))
      (first expanded)
      (let [join-vars (filterv qu/free-var? args)]
        [(apply list 'or-join join-vars
                (mapv (fn [ex]
                        (if (and (seq ex) (nil? (next ex)))
                          (first ex)
                          (cons 'and ex)))
                      expanded))]))))

(defn expand-nonrecursive-rule-call-for-planning
  "Expand one non-recursive rule call for cardinality planning. The expansion
   deliberately crosses the outer rule's set boundary and must therefore
   never be executed as a replacement for the rule relation. Nested rule
   boundaries remain intact."
  [{:keys [rules rules-deps] :as context} clause]
  (when (and (seq rules) (rule-call? context clause))
    (let [deps (or rules-deps (dependency-graph rules))
          src  (source clause)
          head (rule-head clause)
          args (rule-args clause)]
      (when-not (recursive? deps head)
        (expand-rule context (volatile! #{}) rules deps src head args)))))

(defn- expand-clauses
  "Expand non-recursive rules, preserving set-valued boundaries below the
   outermost expansion when a rule can have multiple proofs per head tuple."
  [context to-rm rules deps clauses nested-rule?]
  (into
    []
    (mapcat
      (fn [clause]
        (cond+
          :let [src  (source clause)
                head (rule-head clause)
                args (rule-args clause)]

          ;; Non-recursive rule call
          (and (rule-call? context clause)
               (not (recursive? deps head))
               (not (and (rule-needs-set-boundary? rules head)
                         (or nested-rule?
                             (rule-depends-on-derived-relation?
                               context rules head)))))
          (do (vswap! to-rm conj head)
              (expand-rule context to-rm rules deps src head args))

          ;; (or ...)
          (and (sequential? head) (= 'or (first head)))
          (let [[_ & branches] head]
            [(ensure-src
               src
               (cons
                 'or
                 (mapv
                   (fn [branch]
                     (let [expanded
                           (expand-clauses context to-rm rules deps [branch]
                                           nested-rule?)]
                       (if (and (seq expanded) (nil? (next expanded)))
                         (first expanded)
                         (cons 'and expanded))))
                   branches)))])

          ;; (and ...)
          (and (sequential? head) (= 'and (first head)))
          (let [[_ & sub-clauses] head]
            [(ensure-src
               src
               (cons 'and
                     (expand-clauses context to-rm rules deps sub-clauses
                                     nested-rule?)))])

          ;; (not ...)
          (and (sequential? head) (= 'not (first head)))
          (let [[_ & sub-clauses] head]
            [(ensure-src
               src
               (cons 'not
                     (expand-clauses context to-rm rules deps sub-clauses
                                     nested-rule?)))])

          ;; not-join
          (= 'not-join head)
          (let [[vars & sub-clauses] args]
            [(ensure-src
               src
               (apply list 'not-join (filterv qu/free-var? vars)
                      (expand-clauses context to-rm rules deps sub-clauses
                                      nested-rule?)))])

          ;; (not-join ...)
          (and (sequential? head) (= 'not-join (first head)))
          (let [[_ vars & sub-clauses] head]
            [(ensure-src
               src
               (apply list 'not-join (filterv qu/free-var? vars)
                      (expand-clauses context to-rm rules deps sub-clauses
                                      nested-rule?)))])

          ;; (or-join ...)
          (and (sequential? head) (= 'or-join (first head)))
          (let [[_ vars & branches] head]
            [(ensure-src
               src
               (apply
                 list 'or-join vars
                 (mapv
                   (fn [branch]
                     (let [expanded
                           (expand-clauses context to-rm rules deps [branch]
                                           nested-rule?)]
                       (if (and (seq expanded) (nil? (next expanded)))
                         (first expanded)
                         (cons 'and expanded))))
                   branches)))])

          :else [clause])))
    clauses))

(defn rewrite
  "optimization that pulls out non-recursive rules"
  [{:keys [rules rules-deps] :as context}]
  (if (empty? rules)
    context
    (let [to-remove (volatile! #{})
          old-where (get-in context [:parsed-q :qorig-where])
          new-where (expand-clauses
                      context to-remove rules rules-deps old-where false)]
      (-> context
          (assoc :rules-deps (dependency-graph rules))
          (assoc-in [:parsed-q :qorig-where] new-where)
          (assoc-in [:parsed-q :qwhere] (dp/parse-where new-where))))))
