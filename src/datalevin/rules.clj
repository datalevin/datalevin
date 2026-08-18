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
  "Rules evaluation engine"
  (:require
   [clojure.set :as set]
   [clojure.edn :as edn]
   [clojure.walk :as walk]
   [clojure.string :as str]
   [datalevin.parser :as dp]
   [datalevin.db :as db]
   [datalevin.query-util :as qu]
   [datalevin.join :as j]
   [datalevin.constants :as c]
   [datalevin.util :as u :refer [raise concatv cond+ map+]]
   [datalevin.relation :as r])
  (:import
   [datalevin.utl ArrayUtil]
   [datalevin.db DB]
   [datalevin.relation Relation]
   [org.eclipse.collections.impl.list.mutable FastList]
   [java.util List HashMap HashSet]))

(defn parse-rules
  [rules]
  (let [rules (if (string? rules) (edn/read-string rules) rules)]
    (dp/parse-rules rules) ;; validation
    (group-by ffirst rules)))

(defn- rule-head
  [clause]
  (if (qu/source? (first clause))
    (second clause)
    (first clause)))

(defn- rule-args
  [clause]
  (if (qu/source? (first clause))
    (nnext clause)
    (rest clause)))

(defn- source [clause] (let [src (first clause)] (when (qu/source? src) src)))

(defn- ensure-src
  [src clause]
  (if (nil? src)
    clause
    (if (vector? clause)
      (into [src] clause)
      (cons src clause))))

(defn- sourceable-clause?
  "Return true if a clause can be prefixed with a source.
   Predicate and function clauses should not be source-prefixed since
   the parser does not treat them as source-aware."
  [clause]
  (not (or (dp/parse-pred clause)
           (dp/parse-fn clause))))

(defn- ensure-src-where
  [src clause]
  (if (and src (sourceable-clause? clause))
    (ensure-src src clause)
    clause))

;; stratification

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

(defn- empty-rel-for-rule
  "Create an empty relation with attributes matching the rule head args"
  [rule-name rules]
  (let [head-vars (rest (ffirst (get rules rule-name)))]
    (r/relation! (zipmap head-vars (range)) (FastList.))))

(defn- project-rule-result
  "Project body-rel to head-vars"
  [body-rel head-vars]
  (if (seq head-vars)
    (let [tuples      ^List (:tuples body-rel)
          m           (.size tuples)
          final-attrs (zipmap head-vars (range))]
      (if (zero? m)
        (r/relation! final-attrs (FastList.))
        (let [attrs      (:attrs body-rel)
              ^ints idxs (int-array (map attrs head-vars))
              n          (alength idxs)]
          (r/relation!
            final-attrs
            (let [res (FastList. m)]
              (dotimes [i m]
                (let [from ^objects (.get tuples i)
                      to   (object-array n)]
                  (dotimes [j n]
                    (aset to j (aget from (aget idxs j))))
                  (.add res to)))
              res)))))
    body-rel))

(defn- head-indices
  [attrs rule-head-vars]
  (int-array
    (mapv (fn [v]
            (if-let [i (attrs v)]
              i
              (raise "Missing var in rule-rel attrs"
                     {:var v :attrs attrs :head rule-head-vars})))
          rule-head-vars)))

(defn- const-indices
  [^ints head-idxs call-args]
  (let [checks (into []
                     (keep-indexed
                       (fn [idx arg]
                         (when-not (qu/free-var? arg)
                           [(aget head-idxs idx) arg])))
                     call-args)
        n      (count checks)
        idxs   (int-array n)
        values (object-array n)]
    (dotimes [i n]
      (let [[idx value] (checks i)]
        (aset idxs i (int idx))
        (aset values i value)))
    [idxs values]))

(defn- const-check
  [^ints idxs ^objects values ^objects tuple]
  (let [n (alength idxs)]
    (loop [i 0]
      (if (< i n)
        (if (= (aget tuple (aget idxs i)) (aget values i))
          (recur (unchecked-inc-int i))
          false)
        true))))

(defn- var-positions
  [call-args]
  (u/reduce-indexed
    (fn [m arg idx]
      (if (qu/free-var? arg)
        (update m arg (fnil conj []) idx)
        m))
    {} call-args))

(defn- equality-indices
  [^ints head-idxs var->positions]
  (let [pairs (into []
                    (mapcat
                      (fn [[_ positions]]
                        (when (< 1 (count positions))
                          (let [first-idx (aget head-idxs (first positions))]
                            (map (fn [position]
                                   [first-idx (aget head-idxs position)])
                                 (rest positions))))))
                    var->positions)
        n     (count pairs)
        left  (int-array n)
        right (int-array n)]
    (dotimes [i n]
      (let [[left-idx right-idx] (pairs i)]
        (aset left i (int left-idx))
        (aset right i (int right-idx))))
    [left right]))

(defn- unique-vars
  [call-args]
  (into []
        (comp (filter qu/free-var? )
           (distinct))
        call-args))

(defn- projection-indices
  [var->positions ^ints head-idxs unique-call-vars]
  (int-array (mapv #(aget head-idxs (first (var->positions %)))
                   unique-call-vars)))

(defn- equality-check
  [^ints left ^ints right ^objects tuple]
  (let [n (alength left)]
    (loop [i 0]
      (if (< i n)
        (if (= (aget tuple (aget left i)) (aget tuple (aget right i)))
          (recur (unchecked-inc-int i))
          false)
        true))))

(defn- identity-indices?
  [^ints idxs]
  (let [n (alength idxs)]
    (loop [i 0]
      (if (< i n)
        (and (= i (aget idxs i))
             (recur (unchecked-inc-int i)))
        true))))

(defn- map-rule-result
  "transforms evaluated rule-rel into a result rel tailored to the rule call"
  [rule-rel rule-head-vars call-args]
  (let [attrs            (:attrs rule-rel)
        ^List tuples     (:tuples rule-rel)
        ^ints head-idxs  (head-indices attrs rule-head-vars)
        [^ints const-idxs ^objects const-values]
        (const-indices head-idxs call-args)
        var->positions   (var-positions call-args)
        [^ints equality-left ^ints equality-right]
        (equality-indices head-idxs var->positions)
        unique-call-vars (unique-vars call-args)
        ^ints projection-idxs
        (projection-indices var->positions head-idxs unique-call-vars)
        n                (alength projection-idxs)
        result-attrs     (zipmap unique-call-vars (range))]
    (if (and (= n (count attrs))
             (zero? (alength const-idxs))
             (zero? (alength equality-left))
             (identity-indices? projection-idxs))
      (r/relation! result-attrs tuples)
      (r/relation!
        result-attrs
        (let [size (.size tuples)
              acc  (FastList. size)]
          (dotimes [i size]
            (let [^objects tuple (.get tuples i)]
              (when (and (const-check const-idxs const-values tuple)
                         (equality-check equality-left equality-right tuple))
                (let [to (object-array n)]
                  (dotimes [j n]
                    (aset to j (aget tuple (aget projection-idxs j))))
                  (.add acc to)))))
          acc)))))

(defn- rename-rule
  [branches]
  (let [mapping (volatile! {})]
    (walk/postwalk
      (fn [x]
        (if (qu/free-var? x)
          (if-let [n (@mapping x)]
            n
            (let [n (gensym (name x))]
              (vswap! mapping assoc x n)
              n))
          x))
      branches)))

(defn- rule-call?
  [context clause]
  (when (sequential? clause)
    (let [head (rule-head clause)]
      (and (symbol? head)
           (not (qu/free-var? head))
           (not (qu/rule-head head))
           ((:rules context) head)))))

(defn- clause-required-vars
  "Vars that must be bound before evaluating a clause."
  [clause context]
  (cond
    ;; or-join: only the binding vector vars that appear in input positions
    ;; are required, not all internal vars
    (and (sequential? clause)
         (not (vector? clause))
         (= 'or-join (first clause)))
    (let [[_ binding-vars & branches] clause]
      ;; For or-join, we need the first binding var(s) that will be used
      ;; to filter/seed the branches. Analyze which vars appear in E position
      ;; (required) vs V position (produced) in the branches.
      (into #{}
            (filter
              (fn [v]
                (and (qu/free-var? v)
                     ;; Check if this var appears in E position in any branch
                     (some (fn [branch]
                             (let [clauses (if (and (seq? branch)
                                                    (= 'and (first branch)))
                                             (rest branch)
                                             [branch])]
                               (some (fn [c] (and (vector? c) (= v (first c))))
                                     clauses)))
                           branches))))
            binding-vars))

    ;; predicate style list, but not a rule call
    (and (sequential? clause)
         (not (vector? clause))
         (not (rule-call? context clause)))
    (into #{} (filter qu/free-var?) (flatten clause))

    ;; function binding clause: [(f ?in ...) ?out]
    (and (vector? clause) (sequential? (first clause)))
    (into #{} (filter qu/free-var?) (rest (first clause)))

    :else #{}))

(defn- clause-bound-vars
  "Vars that become bound after a clause is evaluated."
  [clause context]
  (cond
    ;; or-join: only the binding vector vars become bound, not internal vars
    (and (sequential? clause)
         (not (vector? clause))
         (= 'or-join (first clause)))
    (let [[_ binding-vars & _] clause]
      (into #{} (filter qu/free-var?) binding-vars))

    (and (sequential? clause)
         (not (vector? clause)))
    (if (rule-call? context clause)
      (into #{} (filter qu/free-var?) (rest clause))
      (into #{} (filter qu/free-var?) clause))

    (vector? clause)
    (into #{} (filter qu/free-var?) (flatten clause))

    :else #{}))

(defn- clause-free-vars
  [clause]
  (if (and (sequential? clause)
           (not (vector? clause))
           (= 'or-join (first clause)))
    ;; For or-join, only consider binding vector vars for reordering purposes
    (let [[_ binding-vars & _] clause]
      (into #{} (filter qu/free-var?) binding-vars))
    (into #{} (filter qu/free-var?) (flatten clause))))

(defn- context-bound-vars
  [context]
  (into #{} (mapcat #(keys (:attrs %))) (:rels context)))

(defn- values-for-var
  [context v]
  (let [values (HashSet.)]
    (doseq [rel (:rels context)
            :let [idx ((:attrs rel) v)]
            :when (some? idx)]
      (let [tuples ^List (:tuples rel)]
        (dotimes [i (.size tuples)]
          (.add values (aget ^objects (.get tuples i) ^long idx)))))
    (vec values)))

(defn- clause-source
  [clause context]
  (let [src-sym (if (qu/source? (first clause))
                  (first clause)
                  '$)]
    (get-in context [:sources src-sym])))

(defn- clause-pattern
  [clause]
  (if (qu/source? (first clause))
    (subvec clause 1)
    clause))

(defn- clause->pattern
  [clause]
  (mapv #(if (or (qu/free-var? %) (= % '_)) nil %)
        (clause-pattern clause)))

(defn- rel-size
  [rel]
  (let [^List tuples (:tuples rel)]
    (if tuples (.size tuples) 0)))

(defn- cached-rule-rel-size
  "Get a stable size for a rule call. Uses either :rule-totals, which contains
   pre-computed results for non-recursive external rules, or an explicit magic
   seed. Does NOT fall back to :rule-rels, which may contain deltas during
   iteration."
  [context clause]
  (when (rule-call? context clause)
    (when-let [rel (or (get-in context [:rule-totals (rule-head clause)])
                       (get-in context [:magic-seeds (rule-head clause)]))]
      (rel-size rel))))

(defn- unbound-eav-pattern?
  [clause bound]
  (let [[e _ v] (clause-pattern clause)]
    (and (or (qu/free-var? e) (= e '_))
         (or (qu/free-var? v) (= v '_))
         (not (contains? bound e))
         (not (contains? bound v)))))

(defn- scale-estimate
  [^long n ^long factor]
  (if (or (= n Long/MAX_VALUE) (= factor 1))
    n
    (let [limit (quot Long/MAX_VALUE factor)]
      (if (> n limit)
        Long/MAX_VALUE
        (unchecked-multiply n factor)))))

(def ^:private ^:const rule-bound-count-limit
  "Maximum materialized values probed when costing a bound rule pattern."
  128)

(defn- saturating-count-add
  ^long [^long x ^long y]
  (if (> y (- Long/MAX_VALUE x))
    Long/MAX_VALUE
    (+ x y)))

(defn- pattern-count
  ^long [^DB source pattern]
  (long (or (db/-count source pattern) Long/MAX_VALUE)))

(defn- bounded-pattern-count-sum
  [^DB source values pattern-fn]
  (when (<= (long (count values)) (long rule-bound-count-limit))
    (reduce
      (fn [^long total value]
        (saturating-count-add total
                              (pattern-count source (pattern-fn value))))
      0 values)))

(defn- context-values-for-term
  [context term]
  (when (and (qu/binding-var? term)
             (contains? (context-bound-vars context) term))
    (values-for-var context term)))

(defn- bound-pattern-size
  "Estimate an EAV clause from small value domains already materialized in
   the rule context. Returns nil when the domains are too large to probe."
  [^DB source clause context]
  (let [[e a v]   (clause-pattern clause)
        a          (if (or (qu/free-var? a) (= a '_)) nil a)
        e-values   (context-values-for-term context e)
        v-values   (context-values-for-term context v)
        fixed-e    (when-not (or (qu/free-var? e) (= e '_)) e)
        fixed-v    (when-not (or (qu/free-var? v) (= v '_)) v)
        from-e     (when (some? e-values)
                     (bounded-pattern-count-sum
                       source e-values #(vector % a fixed-v)))
        from-v     (when (some? v-values)
                     (bounded-pattern-count-sum
                       source v-values #(vector fixed-e a %)))]
    (cond
      (and (some? from-e) (some? from-v))
      (min (long from-e) (long from-v))
      (some? from-e) from-e
      (some? from-v) from-v)))

(defn- estimate-clause-size
  [clause context bound]
  (cond
    (and (vector? clause)
         (dp/parse-pattern clause)
         (not (dp/parse-pred clause))
         (not (dp/parse-fn clause)))
    (if-let [^DB db (clause-source clause context)]
      (try
        (let [n (or (bound-pattern-size db clause context)
                    (pattern-count db (clause->pattern clause)))]
          (if (unbound-eav-pattern? clause bound)
            (scale-estimate n c/rule-unbound-pattern-penalty)
            n))
        (catch Exception _ Long/MAX_VALUE))
      Long/MAX_VALUE)

    (sequential? clause)
    (if (rule-call? context clause)
      (or (cached-rule-rel-size context clause) Long/MAX_VALUE)
      0)

    :else Long/MAX_VALUE))

(defn- reorder-clauses
  [clauses context]
  (let [bound (volatile! (context-bound-vars context))]
    (loop [remaining clauses
           ordered   []]
      (if (empty? remaining)
        ordered
        (let [candidates-indices
              (keep-indexed
                (fn [idx clause]
                  (when (set/subset?
                          (clause-required-vars clause context) @bound)
                    idx))
                remaining)

              candidates-indices
              (if (seq candidates-indices)
                candidates-indices
                (range (count remaining)))

              scored-candidates
              (mapv
                (fn [idx]
                  (let [clause (nth remaining idx)]
                    [idx
                     [(- (count (set/intersection
                                  (clause-free-vars clause) @bound)))
                      (estimate-clause-size clause context @bound)]]))
                candidates-indices)

              ^long best-idx (ffirst (sort-by second scored-candidates))

              best-clause (nth remaining best-idx)]

          (vswap! bound set/union (clause-bound-vars best-clause context))
          (recur (concatv (take best-idx remaining)
                          (drop (inc best-idx) remaining))
                 (conj ordered best-clause)))))))

(defn- delta-values-for-column
  "Return distinct values in a delta column, or nil when the cap is exceeded."
  [^List tuples ^long idx ^long limit]
  (let [res (HashSet.)
        n   (.size tuples)]
    (loop [i 0]
      (if (< i n)
        (do
          (.add res (aget ^objects (.get tuples i) idx))
          (if (> (.size res) limit)
            nil
            (recur (unchecked-inc i))))
        res))))

(defn- intersect-hash-sets
  [^HashSet a ^HashSet b]
  (let [a-size (.size a)
        b-size (.size b)
        small  (if (< a-size b-size) a b)
        large  (if (< a-size b-size) b a)
        res    (HashSet. (min a-size b-size))]
    (doseq [v small]
      (when (.contains large v)
        (.add res v)))
    res))

(defn- merge-delta-bound-set
  [acc var ^HashSet values]
  (if-let [^HashSet existing (get acc var)]
    (assoc acc var (intersect-hash-sets existing values))
    (assoc acc var values)))

(defn- extract-delta-bound-values
  "Pre-extract bounded delta values for vars used as recursive call args.
   The bound variables must be derived from the branch's rule-call arguments,
   not from the caller rule's head vars."
  [context branch]
  (let [limit (long c/rule-delta-index-threshold)
        res
        (reduce
          (fn [acc clause]
            (if (and (sequential? clause) (rule-call? context clause))
              (let [rname     (rule-head clause)
                    rel       (get (:rule-rels context) rname)
                    branches  (get (:rules context) rname)
                    head-vars (when branches (rest (ffirst branches)))
                    call-args (rule-args clause)
                    ^List tuples (:tuples rel)]
                (if (and tuples (pos? (.size tuples)) (seq head-vars))
                  (reduce
                    (fn [acc [head-var arg]]
                      (if (qu/binding-var? arg)
                        (if-let [idx ((:attrs rel) head-var)]
                          (if-let [values (delta-values-for-column
                                            tuples (long idx) limit)]
                            (merge-delta-bound-set acc arg values)
                            acc)
                          acc)
                        acc))
                    acc (map vector head-vars call-args))
                  acc))
              acc))
          {} (rest branch))]
    (when (seq res) res)))

(defn- distinct-vars?
  [vars]
  (= (count vars) (count (distinct vars))))

(defn- simple-eav-clause?
  [clause]
  (and (vector? clause)
       (= 3 (count clause))
       (keyword? (second clause))
       (qu/binding-var? (first clause))
       (qu/binding-var? (nth clause 2))))

(defn- eav-link-plan
  [head-pos call-pos clause]
  (let [[e attr v] clause
        e-head     (head-pos e)
        v-head     (head-pos v)
        e-call     (call-pos e)
        v-call     (call-pos v)]
    (cond
      (and (some? e-head) (nil? e-call) (some? v-call) (nil? v-head))
      {:attr attr, :head-pos e-head, :call-pos v-call, :bound-side :v}

      (and (some? v-head) (nil? v-call) (some? e-call) (nil? e-head))
      {:attr attr, :head-pos v-head, :call-pos e-call, :bound-side :e})))

(def ^:private output-source-call 0)
(def ^:private output-source-eav 1)

(defn- compile-head-sources
  [head-vars call-pos eav-plans]
  (let [n        (count head-vars)
        types    (long-array n)
        idxs     (int-array n)
        eav-mask (boolean-array n)]
    (doseq [[i {:keys [head-pos]}] (map-indexed vector eav-plans)]
      (aset-boolean eav-mask (int head-pos) true)
      (aset-long types (int head-pos) (long output-source-eav))
      (aset-int idxs (int head-pos) (int i)))
    (loop [i 0]
      (if (< i n)
        (if (aget eav-mask i)
          (recur (unchecked-inc i))
          (let [head-var (nth head-vars i)]
            (when-let [call-idx (call-pos head-var)]
              (aset-long types i (long output-source-call))
              (aset-int idxs i (int call-idx))
              (recur (unchecked-inc i)))))
        {:types types, :idxs idxs}))))

(defn- linear-eav-branch-plan
  [context branch]
  (let [head       (first branch)
        head-vars  (vec (rest head))
        clauses    (vec (rest branch))
        rule-calls (filterv #(and (sequential? %) (rule-call? context %))
                            clauses)
        eavs       (filterv simple-eav-clause? clauses)]
    (when (and (seq head-vars)
               (<= 1 (count eavs) 2)
               (= 1 (count rule-calls))
               (= (count clauses) (inc (count eavs)))
               (every? qu/binding-var? head-vars)
               (distinct-vars? head-vars))
      (let [call      (first rule-calls)
            rname     (rule-head call)
            rel       (get (:rule-rels context) rname)
            branches  (get (:rules context) rname)
            rhead     (when branches (vec (rest (ffirst branches))))
            call-args (vec (rule-args call))
            source    (get-in context [:sources '$])]
        (when (and rel
                   source
                   (db/-searchable? source)
                   (= (count rhead) (count call-args))
                   (every? qu/binding-var? call-args)
                   (distinct-vars? call-args)
                   (every? #(contains? (:attrs rel) %) rhead))
          (let [rel-attrs     (:attrs rel)
                call-rel-idxs (int-array
                                (map (fn [v] (int (rel-attrs v))) rhead))
                head-pos      (zipmap head-vars (range))
                call-pos      (zipmap call-args (range))
                eav-plans     (mapv #(eav-link-plan head-pos call-pos %) eavs)]
            (when (and (= (count eavs) (count eav-plans))
                       (every? some? eav-plans)
                       (= (count eav-plans)
                          (count (distinct (map :head-pos eav-plans)))))
              (let [head-source (compile-head-sources head-vars call-pos
                                                      eav-plans)]
                (when head-source
                  {:db            source
                   :call-rule     rname
                   :delta-rel     rel
                   :call-rel-idxs call-rel-idxs
                   :head-vars     head-vars
                   :head-types    (:types head-source)
                   :head-idxs     (:idxs head-source)
                   :eav-plans     eav-plans})))))))))

(defn- add-adjacency-value!
  [^HashMap adjacency bound-value out-value]
  (if-let [^FastList values (.get adjacency bound-value)]
    (.add values out-value)
    (let [values (FastList.)]
      (.add values out-value)
      (.put adjacency bound-value values))))

(defn- build-eav-adjacency
  [^DB db attr bound-side]
  (let [adjacency (HashMap.)
        tuples    ^List (db/-search-tuples db [nil attr nil])]
    (when tuples
      (let [bound-idx (case bound-side :e 0 :v 1)
            out-idx   (case bound-side :e 1 :v 0)]
        (dotimes [i (.size tuples)]
          (let [^objects tuple (.get tuples i)]
            (add-adjacency-value! adjacency
                                  (aget tuple bound-idx)
                                  (aget tuple out-idx))))))
    adjacency))

(defn- cached-eav-adjacency
  [context ^DB db {:keys [attr bound-side]}]
  (if-let [cache (:linear-eav-cache context)]
    (let [k [db attr bound-side]]
      (if (contains? @cache k)
        (get @cache k)
        (locking cache
          (if (contains? @cache k)
            (get @cache k)
            (let [adjacency (build-eav-adjacency db attr bound-side)]
              (swap! cache assoc k adjacency)
              adjacency)))))
    (build-eav-adjacency db attr bound-side)))

(defn- cached-eav-adjacencies
  [context ^DB db eav-plans]
  (let [n (count eav-plans)
        res (object-array n)]
    (dotimes [i n]
      (aset res i (cached-eav-adjacency context db (eav-plans i))))
    res))

(defn- eav-call-positions
  [eav-plans]
  (let [n   (count eav-plans)
        res (int-array n)]
    (dotimes [i n]
      (aset res i (int (:call-pos (eav-plans i)))))
    res))

(defn- call-value
  [^objects delta-tuple ^ints call-rel-idxs call-pos]
  (aget delta-tuple (aget call-rel-idxs (int call-pos))))

(def ^:private empty-group-key (Object.))

(defn- group-values!
  [^HashMap groups ^objects tuple ^ints tuple-idxs ^objects scratch lookup]
  (case (alength tuple-idxs)
    0
    (or (.get groups empty-group-key)
        (let [values (HashSet.)]
          (.put groups empty-group-key values)
          values))

    1
    (let [key (aget tuple (aget tuple-idxs 0))]
      (or (.get groups key)
          (let [values (HashSet.)]
            (.put groups key values)
            values)))

    (do
      (dotimes [i (alength tuple-idxs)]
        (aset scratch i (aget tuple (aget tuple-idxs i))))
      (or (.get groups (r/reset-array-lookup! lookup scratch))
          (let [key    (aclone scratch)
                values (HashSet.)]
            (.put groups (r/wrap-array key) values)
            values)))))

(defn- build-keyed-eav-seen
  [{:keys [^Relation delta-rel ^ints call-rel-idxs ^longs head-types
           ^ints head-idxs eav-plans]}
   ^HashMap eav-index]
  (let [head-count       (alength head-types)
        eav-head-pos     (int (:head-pos (first eav-plans)))
        key-count        (dec head-count)
        candidate-idxs   (int-array key-count)
        initial-idxs     (int-array key-count)
        valid?           (loop [head-pos 0, key-pos 0]
                           (if (< head-pos head-count)
                             (if (= head-pos eav-head-pos)
                               (recur (unchecked-inc-int head-pos) key-pos)
                               (if (= output-source-call
                                      (aget head-types head-pos))
                                 (let [call-pos (aget head-idxs head-pos)]
                                   (aset candidate-idxs key-pos
                                         (aget call-rel-idxs call-pos))
                                   (aset initial-idxs key-pos
                                         (aget call-rel-idxs head-pos))
                                   (recur (unchecked-inc-int head-pos)
                                          (unchecked-inc-int key-pos)))
                                 false))
                             true))]
    (when valid?
      (let [groups          (HashMap.)
            domain         (HashSet.)
            ^List tuples    (:tuples delta-rel)
            initial-out-idx (aget call-rel-idxs eav-head-pos)
            scratch         (object-array key-count)
            lookup          (r/array-lookup)]
        (when tuples
          (dotimes [i (.size tuples)]
            (let [^objects tuple  (.get tuples i)
                  ^HashSet values (group-values! groups tuple initial-idxs
                                                  scratch lookup)]
              (.add values (aget tuple initial-out-idx))
              (.add domain (aget tuple initial-out-idx)))))
        (doseq [^List values (.values eav-index)]
          (dotimes [i (.size values)]
            (.add domain (.get values i))))
        {:groups         groups
         :candidate-idxs candidate-idxs
         :domain-size    (.size domain)}))))

(defn- keyed-eav-seen
  [context branch plan ^HashMap eav-index]
  (when (and (:single-recursive-branch? context)
             (= 1 (count (:eav-plans plan)))
             (= (:current-rule context) (:call-rule plan)))
    (let [cache (:linear-eav-keyed-seen-cache context)
          k     [(:current-rule context) branch]]
      (if (contains? @cache k)
        (get @cache k)
        (locking cache
          (if (contains? @cache k)
            (get @cache k)
            (let [state (build-keyed-eav-seen plan eav-index)]
              (swap! cache assoc k state)
              state)))))))

(defn- add-fast-output-with-probe!
  [^FastList acc ^HashSet seen-set seen-lookup ^objects scratch
   ^longs head-types ^ints head-idxs ^objects delta-tuple ^ints call-rel-idxs
   eav0 eav1]
  (let [h (ArrayUtil/fillRuleOutputAndHash
            scratch head-types head-idxs delta-tuple call-rel-idxs eav0 eav1)]
    (when-not (.contains seen-set
                         (r/reset-array-lookup-with-hash!
                           seen-lookup scratch h))
      (let [tuple (aclone scratch)]
        (.add seen-set (r/wrap-array-with-hash tuple h))
        (.add acc tuple)))))

(defn- add-fast-output-known-new!
  [^FastList acc ^HashSet seen-set ^objects scratch ^longs head-types
   ^ints head-idxs ^objects delta-tuple ^ints call-rel-idxs eav0 eav1]
  (let [h     (ArrayUtil/fillRuleOutputAndHash
                scratch head-types head-idxs delta-tuple call-rel-idxs
                eav0 eav1)
        tuple (aclone scratch)]
    (when (.add seen-set (r/wrap-array-with-hash tuple h))
      (.add acc tuple))))

(defn- eval-linear-eav-branch-with-dedup
  [context branch ^HashSet seen-set]
  (when-let [plan (linear-eav-branch-plan context branch)]
    (let [{:keys [^DB db ^Relation delta-rel ^ints call-rel-idxs head-vars
                  ^longs head-types ^ints head-idxs eav-plans]} plan
          ^List delta-tuples (:tuples delta-rel)
          acc                (FastList.)
          eav-count          (count eav-plans)
          ^objects eav-indexes
          (cached-eav-adjacencies context db eav-plans)
          ^ints eav-call-idxs
          (eav-call-positions eav-plans)
          output-scratch     (object-array (alength head-types))
          seen-lookup        (r/array-lookup)
          keyed-seen         (when (= 1 eav-count)
                               (keyed-eav-seen
                                 context branch plan
                                 (aget eav-indexes 0)))
          keyed-scratch      (when keyed-seen
                               (object-array
                                 (alength ^ints (:candidate-idxs keyed-seen))))
          keyed-lookup       (when keyed-seen (r/array-lookup))]
      (when (and delta-tuples (pos? (.size delta-tuples)))
        (dotimes [i (.size delta-tuples)]
          (let [^objects delta-tuple (.get delta-tuples i)]
            (case eav-count
              1
              (let [values0  ^List (.get ^HashMap (aget eav-indexes 0)
                                          (call-value
                                            delta-tuple call-rel-idxs
                                            (aget eav-call-idxs 0)))]
                (when values0
                  (if keyed-seen
                    (let [^HashSet values-seen
                          (group-values!
                            (:groups keyed-seen) delta-tuple
                            (:candidate-idxs keyed-seen)
                            keyed-scratch keyed-lookup)
                          domain-size (long (:domain-size keyed-seen))]
                      (loop [j 0]
                        (when (and (< j (.size values0))
                                   (< (.size values-seen) domain-size))
                          (let [value (.get values0 j)]
                            (when (.add values-seen value)
                              (add-fast-output-known-new!
                                acc seen-set output-scratch head-types head-idxs
                                delta-tuple call-rel-idxs value nil))
                            (recur (unchecked-inc-int j))))))
                    (dotimes [j (.size values0)]
                      (add-fast-output-with-probe!
                        acc seen-set seen-lookup output-scratch
                        head-types head-idxs delta-tuple call-rel-idxs
                        (.get values0 j) nil)))))

              2
              (let [values0  ^List (.get ^HashMap (aget eav-indexes 0)
                                          (call-value
                                            delta-tuple call-rel-idxs
                                            (aget eav-call-idxs 0)))
                    values1  ^List (.get ^HashMap (aget eav-indexes 1)
                                          (call-value
                                            delta-tuple call-rel-idxs
                                            (aget eav-call-idxs 1)))]
                (when (and values0 values1)
                  (dotimes [j (.size values0)]
                    (let [v0 (.get values0 j)]
                      (dotimes [k (.size values1)]
                        (add-fast-output-with-probe!
                          acc seen-set seen-lookup output-scratch
                          head-types head-idxs delta-tuple call-rel-idxs
                          v0 (.get values1 k)))))))))))
      (r/relation! (zipmap head-vars (range)) acc))))

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
              body-res-context       (reduce resolve-fn branch-context
                                             ordered-clauses)
              body-rel               (reduce j/hash-join (:rels body-res-context))
              projected-rel          (project-rule-result body-rel args)]
          (r/sum-rel rel projected-rel)))
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

(defn- unique-seeds
  [rel idx]
  (let [^List tuples (:tuples rel)
        size         (.size tuples)]
    (if tuples
      (let [seen (HashSet. size)
            res  (FastList. size)]
        (dotimes [i size]
          (let [^objects tuple (.get tuples i)
                v              (aget tuple idx)]
            (when (.add seen v)
              (.add res (object-array [v])))))
        res)
      (FastList.))))

(defn- rename-rel-attrs
  [rel head-vars]
  (assoc rel :attrs (zipmap head-vars (range))))

(defn- bound-arg-indices
  "Indices of args that are already bound (via outer context or constants)."
  [args context]
  (let [bound (context-bound-vars context)]
    (into #{}
          (keep-indexed (fn [idx arg]
                          (cond
                            (not (qu/free-var? arg)) idx
                            (bound arg)              idx
                            :else                    nil)))
          args)))

(def ^:dynamic *temporal-elimination* false)
(def ^:dynamic *auto-optimize-temporal* true)
(def ^:dynamic *keep-temporal-intermediates* false)
(def ^:dynamic *magic-rewrite?* true)

;; magic set rewrite

(def ^:private magic-special-heads #{'or 'or-join 'and 'not 'not-join})

(defn- magic-head? [sym] (str/starts-with? (name sym) "magic__"))

(defn- magic-rules-size
  "Sum the tuple count of all magic-prefixed rules in the given relations map."
  ^long [rule-rels]
  (reduce-kv
    (fn [^long acc rname rel]
      (if (and (symbol? rname) (magic-head? rname))
        (let [^List tuples (:tuples rel)]
          (if tuples
            (+ acc (.size tuples))
            acc))
        acc))
    0 rule-rels))

(defn- flatten-head-vars
  [head-clause]
  (let [args (rest head-clause)]
    (if (and (seq args) (vector? (first args)))
      (concatv (first args) (rest args))
      (vec args))))

(defn- bound-arg?
  [arg bound-vars]
  (and (not= arg '_)
       (or (not (qu/free-var? arg))
           (contains? bound-vars arg))))

(defn- binding-pattern
  [args bound-vars]
  (mapv (fn [arg] (if (bound-arg? arg bound-vars) :b :f)) args))

(defn- bound-indices
  [pattern]
  (keep-indexed (fn [idx p] (when (= p :b) idx)) pattern))

(defn- pattern-suffix
  [pattern]
  (apply str (mapv #(if (= % :b) "b" "f") pattern)))

(defn- adorned-name
  [rule-name pattern]
  (symbol (str (name rule-name) "__" (pattern-suffix pattern))))

(defn- magic-name
  [adorned-rule]
  (symbol (str "magic__" (name adorned-rule))))

(defn- replace-rule-head
  [clause new-head]
  (if (qu/source? (first clause))
    (list* (first clause) new-head (nnext clause))
    (list* new-head (rest clause))))

(defn- predicate-clause?
  [clause]
  (and (vector? clause)
       (sequential? (first clause))
       (= 1 (count clause))))

(defn- fn-binding-clause?
  [clause]
  (and (vector? clause)
       (sequential? (first clause))
       (< 1 (count clause))))

(defn- complex-clause?
  [clause]
  (and (sequential? clause)
       (let [head (if (qu/source? (first clause))
                    (second clause)
                    (first clause))]
         (contains? magic-special-heads head))))

(defn- clause-output-vars
  [clause context]
  (cond
    (rule-call? context clause)
    (into #{} (filter qu/free-var?) (rule-args clause))

    (fn-binding-clause? clause)
    (into #{} (filter qu/free-var?) (rest clause))

    (predicate-clause? clause)
    #{}

    (vector? clause)
    (into #{} (filter qu/free-var?) clause)

    :else #{}))

(defn- magic-seed-rel
  [context head-vars bound-args]
  (let [values (mapv (fn [arg]
                       (if (qu/free-var? arg)
                         (values-for-var context arg)
                         [arg]))
                     bound-args)
        attrs  (zipmap head-vars (range))]
    (if (or (empty? head-vars) (some empty? values))
      (r/relation! attrs (FastList.))
      (r/relation! attrs (r/many-tuples values)))))

(defn- magic-bind-clauses
  [head-vars bound-args]
  (mapv (fn [hv arg]
          [(list 'identity arg) hv])
        head-vars bound-args))

(defn- simple-branches?
  [branches]
  (every?
    (fn [branch]
      (every? (complement complex-clause?) (rest branch)))
    branches))

(defn- positive-recursive?
  [rules stratum]
  (let [stratum-set (set stratum)]
    (and
      (every?
        (fn [rname]
          (every?
            (fn [branch]
              (every?
                (fn [clause]
                  (not (and (sequential? clause)
                            (let [head (if (qu/source? (first clause))
                                         (second clause)
                                         (first clause))]
                              (#{'not 'not-join} head)))))
                (rest branch)))
            (rules rname)))
        stratum-set)
      (every? (comp simple-branches? rules) stratum-set))))

(defn- magic-effective?
  "Check if magic set rewrite would be effective for the given rule and bound
   pattern. Magic is ineffective when bound head vars don't appear in any
   non-recursive body clause of a recursive branch - they can't filter
   intermediate results, causing cross-product scans."
  [rules rule-name bound-idxs stratum-set]
  (let [branches  (rules rule-name)
        head-vars (vec (rest (ffirst branches)))]
    (every?
      (fn [branch]
        (let [body-clauses (rest branch)
              ;; Check if this branch has any recursive calls
              has-recursive? (some (fn [clause]
                                     (and (sequential? clause)
                                          (stratum-set (rule-head clause))))
                                   body-clauses)]
          (if has-recursive?
            ;; For recursive branches, bound vars must appear in non-recursive
            ;; clauses to effectively filter intermediate results
            (let [non-rec-clauses (remove (fn [clause]
                                            (and (sequential? clause)
                                                 (stratum-set (rule-head clause))))
                                          body-clauses)
                  non-rec-vars    (into #{} (mapcat clause-free-vars)
                                        non-rec-clauses)
                  bound-vars      (into #{} (map head-vars) bound-idxs)]
              ;; At least one bound var must appear in non-recursive clauses
              (seq (set/intersection bound-vars non-rec-vars)))
            ;; Non-recursive branches are always OK
            true)))
      branches)))

(declare stable-head-idxs)

(defn- magic-rewrite-program
  [rules goal-name goal-pattern stratum-set]
  (let [rules-context {:rules rules}
        seen          (volatile! #{})
        queue         (volatile! [[goal-name goal-pattern]])
        adorned-rules (volatile! {})
        magic-rules   (volatile! {})
        magic-heads   (volatile! {})
        ;; Precompute stable indices for rules in the stratum to avoid
        ;; over-adornment of recursive calls
        stable-cache  (reduce (fn [m rname]
                                (let [branches (rules rname)]
                                  (assoc m rname
                                         (if branches
                                           (stable-head-idxs branches stratum-set)
                                           #{}))))
                              {} stratum-set)]
    (letfn [(ensure-magic-heads
              [magic-name n]
              (if-let [vars (@magic-heads magic-name)]
                vars
                (let [vars (mapv (fn [_] (gensym "?magic")) (range n))]
                  (vswap! magic-heads assoc magic-name vars)
                  vars)))

            (add-magic-branch
              [magic-name branch]
              (vswap! magic-rules
                      (fn [m]
                        (update m magic-name (fnil conj []) branch))))]
      (loop []
        (if-let [[rule-name pattern] (first @queue)]
          (do
            (vswap! queue subvec 1)
            (when-not (contains? @seen [rule-name pattern])
              (vswap! seen conj [rule-name pattern])
              (let [adorned-rule (adorned-name rule-name pattern)
                    magic-rule   (magic-name adorned-rule)
                    branches     (get rules rule-name)
                    head-clause  (ffirst branches)
                    head-vars    (flatten-head-vars head-clause)
                    b-idxs       (vec (bound-indices pattern))
                    bound-vars   (into #{} (map head-vars) b-idxs)
                    magic-call   (when (seq b-idxs)
                                   (list* magic-rule (map head-vars b-idxs)))]
                (when (seq b-idxs)
                  (ensure-magic-heads magic-rule (count b-idxs)))
                (doseq [branch branches]
                  (let [head     (first branch)
                        body     (rest branch)
                        new-head (replace-rule-head head adorned-rule)]
                    (loop [clauses      body
                           prefix       (cond-> [] magic-call (conj magic-call))
                           magic-prefix (cond-> [] magic-call (conj magic-call))
                           bound        bound-vars
                           out          []]
                      (if (empty? clauses)
                        (vswap! adorned-rules
                                (fn [m]
                                  (update m adorned-rule (fnil conj [])
                                          (into [new-head]
                                                (concat (when magic-call
                                                          [magic-call])
                                                        out)))))
                        (let [clause (first clauses)]
                          (if (rule-call? rules-context clause)
                            (let [args        (rule-args clause)
                                  call-head   (rule-head clause)
                                  raw-pattern (binding-pattern args bound)
                                  ;; For recursive calls within stratum, filter
                                  ;; pattern to stable indices only to avoid
                                  ;; over-adornment that causes seed explosion.
                                  ;; Only filter when stable indices exist; if
                                  ;; empty, keep original pattern to allow magic
                                  ;; propagation.
                                  call-pattern
                                  (let [stable (stable-cache call-head)]
                                    (if (seq stable)
                                      (vec
                                        (map-indexed
                                          (fn [idx p]
                                            (if (and (= p :b)
                                                     (not (contains?
                                                            stable idx)))
                                              :f
                                              p))
                                          raw-pattern))
                                      raw-pattern))
                                  call-bound? (some #{:b} call-pattern)
                                  call-name   (if call-bound?
                                                (adorned-name call-head
                                                              call-pattern)
                                                call-head)
                                  replaced    (replace-rule-head clause
                                                                 call-name)]
                              (when call-bound?
                                (let [call-magic-name (magic-name call-name)
                                      call-b-idxs     (vec (bound-indices
                                                             call-pattern))
                                      bound-args      (mapv #(nth args %)
                                                            call-b-idxs)
                                      magic-vars      (ensure-magic-heads
                                                        call-magic-name
                                                        (count call-b-idxs))
                                      bind-clauses    (magic-bind-clauses
                                                        magic-vars bound-args)
                                      magic-body      (concat magic-prefix
                                                              bind-clauses)]
                                  (add-magic-branch
                                    call-magic-name
                                    (into [(list* call-magic-name magic-vars)]
                                          magic-body))
                                  (vswap! queue conj [(rule-head clause)
                                                      call-pattern])))
                              (recur (rest clauses)
                                     (conj prefix replaced)
                                     (conj magic-prefix clause)
                                     (into bound (clause-output-vars
                                                   clause rules-context))
                                     (conj out replaced)))
                            (recur (rest clauses)
                                   (conj prefix clause)
                                   (conj magic-prefix clause)
                                   (into bound (clause-output-vars
                                                 clause rules-context))
                                   (conj out clause))))))))))
            (recur))
          (let [final-magic-rules
                (reduce-kv
                  (fn [m magic-name magic-vars]
                    (if (contains? m magic-name)
                      m
                      (assoc m magic-name
                             [[(list* magic-name magic-vars)
                               [(list '= 0 1)]]])))
                  @magic-rules @magic-heads)]
            {:rules       (merge rules @adorned-rules final-magic-rules)
             :magic-heads @magic-heads
             :goal        (adorned-name goal-name goal-pattern)}))))))

(defn- recursive-branch?
  [branch scc]
  (let [clauses (rest branch)]
    (some
      (fn [clause]
        (when (sequential? clause)
          (scc (rule-head clause))))
      clauses)))

(defn- rule-call-heads
  [form rules-context]
  (into #{}
        (keep (fn [node]
                (when (rule-call? rules-context node)
                  (rule-head node))))
        (tree-seq sequential? seq form)))

(defn- external-rule-heads
  [branches rules-context stratum-set]
  (into #{}
        (comp
          (mapcat rest)
          (mapcat #(rule-call-heads % rules-context))
          (remove stratum-set))
        branches))

(defn- vector-binds-var?
  "True if a binding clause produces the given var as an output."
  [v clause]
  (and (vector? clause)
       (some #(and (symbol? %) (= v %))
             (remove sequential? (rest clause)))))

(defn- stable-head-idxs
  "Conservative check for head vars that stay unchanged through recursion.
   Only used for single-rule strata to safely push bound arguments
   (magic-ish seeds)."
  [branches stratum-set]
  (if (not= 1 (count stratum-set))
    #{}
    (let [head-vars (rest (ffirst branches))]
      (into #{}
            (keep-indexed
              (fn [idx hv]
                (when (every?
                        (fn [branch]
                          (let [clauses (rest branch)]
                            (and
                              (not-any? #(vector-binds-var? hv %) clauses)
                              (every? (fn [clause]
                                        (let [args (rule-args clause)]
                                          (= hv (nth args idx))))
                                      ;; recursive calls
                                      (filterv
                                        (fn [clause]
                                          (when (sequential? clause)
                                            (stratum-set (rule-head clause))))
                                        clauses)))))
                        branches)
                  idx)))
            head-vars))))

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

(declare recursive?)

(defn- solve-stratified*
  [context rule-name args resolve-clause-fn]
  (let [rules      (:rules context)
        deps       (or (:rules-deps context) (dependency-graph rules))
        cached-rel (get-in context [:rule-rels rule-name])
        head-vars  (rest (ffirst (rules rule-name)))]
    (if cached-rel
      (map-rule-result cached-rel head-vars args)
      (let [sccs               (dependency-sccs deps)
            stratum            (some #(when (% rule-name) %) sccs)
            stratum-recursive? (recursive-stratum? stratum deps rule-name)]
        (if-not stratum
          (raise "Rule not found in strata" {:rule rule-name})
          (let [stratum-set    (set stratum)
                temporal-cache (:temporal-idx-cache (meta deps))
                ;; 1. Rename rules in stratum to avoid collision & freshen vars
                renamed-rules-map
                (reduce
                  (fn [m rname]
                    (assoc m rname (rename-rule (rules rname))))
                  {} stratum)

                full-renamed-rules (merge rules renamed-rules-map)
                rules-context      (assoc context :rules full-renamed-rules)
                stratum-branches   (mapcat identity (vals renamed-rules-map))
                external-heads     (external-rule-heads stratum-branches
                                                        rules-context
                                                        stratum-set)
                ;; Check if any args are bound (either constants or vars with values)
                ;; If so, skip precomputation - filtering will be more efficient
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

                required-indices (required-seeds entry-renamed-branches
                                                 entry-renamed-head context)
                bound-indices    (bound-arg-indices args context)

                ;; When recursion preserves certain head vars unchanged, we can
                ;; safely seed on those bound args (lightweight magic-set style)
                stable-indices (stable-head-idxs entry-renamed-branches
                                                 stratum-set)
                magic-indices  (set/intersection bound-indices stable-indices)

                ;; 3. Build Seed Relations (only for required vars or constants)
                constant-indices     (into #{}
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
                                                           (when-let [idx ((:attrs rel) arg)]
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
                  [] seed-indices)

	                clean-context
	                (assoc (select-keys context
	                                    [:sources :rule-rels :rules-deps
	                                     :magic-seeds])
	                       :rules-deps deps
	                       :rule-rels base-rule-rels
	                       :linear-eav-cache (atom {})
	                       :linear-eav-keyed-seen-cache (atom {}))

                ;; Split branches into base (non-recursive) and recursive
                base-branches-map
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
                              stratum))

                ;; 4. Evaluate Base Cases
                start-totals
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
                               start-totals)

                ;; Track initial magic seed size for explosion detection
                magic-seeds     (:magic-seeds context)
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

                start-totals (if magic-seeds
                               (reduce
                                 (fn [acc [rname seed-rel]]
                                   (if (and seed-rel (stratum-set rname))
                                     (let [head-vars (rest (ffirst
                                                             (full-renamed-rules
                                                               rname)))
                                           seed-rel  (rename-rel-attrs
                                                       seed-rel head-vars)]
                                       (update acc rname r/sum-rel seed-rel))
                                     acc))
                                 start-totals magic-seeds)
                               start-totals)

                ;; Maintain seen-sets for deduplication across iterations.
                ;; This is critical for cyclic graphs where the same tuple can be
                ;; reached through paths of different lengths.
                seen-sets
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
                  {} stratum)

                final-totals
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
                                  (throw (ex-info "Magic explosion"
                                                  {:type         ::magic-explosion
                                                   :current-size cur-size
                                                   :threshold    magic-threshold})))))]

                      (recur new-totals new-deltas
                             (seq new-deltas) (inc iter)))))]

            (map-rule-result (final-totals rule-name)
                             entry-renamed-head args)))))))

(defn solve-stratified
  [context rule-name args resolve-clause-fn]
  (let [rules       (:rules context)
        deps        (or (:rules-deps context) (dependency-graph rules))
        sccs        (dependency-sccs deps)
        stratum     (some #(when (% rule-name) %) sccs)
        recursive?  (when stratum (recursive-stratum? stratum deps rule-name))
        bound-vars  (context-bound-vars context)
        base-pattern (binding-pattern args bound-vars)
        base-bound?  (some #{:b} base-pattern)
        stratum-set  (when stratum (set stratum))
        branches     (when stratum (rules rule-name))
        head-vars    (when branches (rest (ffirst branches)))
        required-idxs (if (and recursive? branches)
                        (required-seeds branches head-vars context)
                        #{})
        stable-idxs  (if (and recursive? branches)
                       (stable-head-idxs branches stratum-set)
                       #{})
        bound-idxs   (set (bound-indices base-pattern))
        magic-idxs   (set/intersection bound-idxs stable-idxs)
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
        has-bound?  (some #{:b} magic-pattern)]
    (if (and *magic-rewrite?*
             has-bound?
             recursive?
             (positive-recursive? rules stratum)
             (not (magic-head? rule-name))
             (magic-effective? rules rule-name
                               (bound-indices magic-pattern) stratum-set))
      ;; Try magic evaluation; fall back to non-magic if explosion detected
      (let [{:keys [rules magic-heads goal]}
            (magic-rewrite-program rules rule-name magic-pattern stratum-set)
            magic-goal   (magic-name goal)
            b-idxs       (vec (bound-indices magic-pattern))
            bound-args   (mapv #(nth args %) b-idxs)
            head-vars    (get magic-heads magic-goal)
            seed-rel     (magic-seed-rel context head-vars bound-args)
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
                (solve-stratified* context rule-name args resolve-clause-fn))
              ;; Re-throw other exceptions
              (throw e)))))
      (binding [*magic-rewrite?* false]
        (solve-stratified* context rule-name args resolve-clause-fn)))))

;; rewrite

(defn- recursive?
  [deps rule-name]
  (let [sccs    (dependency-sccs deps)
        scc-map (into {} (mapcat (fn [scc] (map #(vector % scc) scc))) sccs)]
    (recursive-stratum? (scc-map rule-name) deps rule-name)))

(declare expand-clauses)

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
              (expand-clauses context to-rm rules deps new-body)))
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

(defn- expand-clauses
  "expand rules in clauses if they are non-recursive"
  [context to-rm rules deps clauses]
  (into
    []
    (mapcat
      (fn [clause]
        (cond+
          :let [src  (source clause)
                head (rule-head clause)
                args (rule-args clause)]

          ;; Non-recursive rule call
          (and (rule-call? context clause) (not (recursive? deps head)))
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
                           (expand-clauses context to-rm rules deps [branch])]
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
                     (expand-clauses context to-rm rules deps sub-clauses)))])

          ;; (not ...)
          (and (sequential? head) (= 'not (first head)))
          (let [[_ & sub-clauses] head]
            [(ensure-src
               src
               (cons 'not
                     (expand-clauses context to-rm rules deps sub-clauses)))])

          ;; not-join
          (= 'not-join head)
          (let [[vars & sub-clauses] args]
            [(ensure-src
               src
               (apply list 'not-join (filterv qu/free-var? vars)
                      (expand-clauses context to-rm rules deps sub-clauses)))])

          ;; (not-join ...)
          (and (sequential? head) (= 'not-join (first head)))
          (let [[_ vars & sub-clauses] head]
            [(ensure-src
               src
               (apply list 'not-join (filterv qu/free-var? vars)
                      (expand-clauses context to-rm rules deps sub-clauses)))])

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
                           (expand-clauses context to-rm rules deps [branch])]
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
                      context to-remove rules rules-deps old-where)]
      (-> context
          (assoc :rules-deps (dependency-graph rules))
          (assoc-in [:parsed-q :qorig-where] new-where)
          (assoc-in [:parsed-q :qwhere] (dp/parse-where new-where))))))
