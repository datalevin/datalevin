;;
;; Copyright (c) Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns ^:no-doc datalevin.query.optimizer.bound-patterns
  "Materialization and projected cost of patterns constrained by bound
  input relations."
  (:require
   [clojure.set :as set]
   [datalevin.constants :as c]
   [datalevin.db :as db]
   [datalevin.interface :refer [av-size]]
   [datalevin.query-util :as qu]
   [datalevin.query.optimizer.estimates
    :refer [cap-output-count materialize-pattern-with-cost
            materialized-output-cost projected-pattern-materialization-cost
            relation-size]]
   [datalevin.query.optimizer.rewrite
    :refer [clause-source-symbol collection-input-plugin-threshold
            find-var-symbols or-join-var? pattern-form pattern-var-symbol
            with-var-symbols]]
   [datalevin.query.plan :as qplan]
   [datalevin.query.resolve :as qresolve]
   [datalevin.util :as u])
  (:import
   [java.util HashSet List]
   [datalevin.db DB]
   [datalevin.parser Constant Pattern]
   [org.eclipse.collections.impl.list.mutable FastList]))

(def ^:private ^:const ^long collection-input-materialize-threshold
  100000)

(defn- rel-for-var
  [context sym]
  (when (qu/binding-var? sym)
    (some #(when (contains? (:attrs %) sym) %)
          (:rels context))))

(defn- small-bound-relation?
  [context sym]
  (when-let [rel (rel-for-var context sym)]
    (let [n (relation-size rel)]
      (and (pos? n)
           (<= n collection-input-materialize-threshold)))))

(defn- bound-relation?
  [context sym]
  (when-let [rel (rel-for-var context sym)]
    (pos? (relation-size rel))))

(defn- relation-has-unresolved-ref?
  [context source attr entity? sym]
  (when (and (qu/binding-var? sym)
             (or entity? (db/ref? source attr)))
    (when-let [rel (rel-for-var context sym)]
      (let [^List tuples (:tuples rel)
            idx          (int ((:attrs rel) sym))]
        (when tuples
          (loop [i 0]
            (when (< i (.size tuples))
              (let [^objects tuple (.get tuples i)
                    value          (aget tuple idx)]
                (if (or (qu/lookup-ref? value) (keyword? value))
                  true
                  (recur (unchecked-inc-int i)))))))))))

(defn- unique-attr?
  [source attr]
  (contains? #{:db.unique/identity :db.unique/value}
             (get-in (db/-schema source) [attr :db/unique])))

(defn- bound-unique-entity-var?
  [parsed-q sym]
  (or (contains? (find-var-symbols parsed-q) sym)
      (contains? (with-var-symbols parsed-q) sym)
      (some #(or-join-var? % sym) (:qorig-where parsed-q))))

(defn- protected-unique-constant-patterns
  [context]
  (let [{:keys [parsed-q sources]} context]
    (keep-indexed
      (fn [clause-idx [parsed-clause orig-clause]]
        (when (and (instance? Pattern parsed-clause)
                   (vector? orig-clause))
          (let [pattern (:pattern parsed-clause)
                e-sym   (pattern-var-symbol (first pattern))
                attr    (second pattern)
                value   (when (<= 3 (count pattern)) (nth pattern 2))]
            (when (and e-sym
                       (instance? Constant attr)
                       (keyword? (:value attr))
                       (instance? Constant value)
                       (bound-unique-entity-var? parsed-q e-sym))
              (when-let [source (get sources
                                     (clause-source-symbol
                                       (:source parsed-clause)))]
                (when (and (db/-searchable? source)
                           (unique-attr? source (:value attr)))
                  {:clause-idx clause-idx
                   :source     source
                   :pattern    (pattern-form orig-clause)
                   :entity-sym e-sym}))))))
      (map vector (:qwhere parsed-q)
           (:qorig-where parsed-q)))))

(defn- connected-vars
  [clause-vars seed]
  (loop [connected #{seed}]
    (let [expanded
          (reduce
            (fn [connected vars]
              (if (seq (set/intersection connected vars))
                (set/union connected vars)
                connected))
            connected clause-vars)]
      (if (= connected expanded)
        connected
        (recur expanded)))))

(defn- connected-unique-anchors
  [parsed-q candidates]
  (let [clause-vars (mapv qu/collect-vars (:qorig-where parsed-q))
        anchor-vars (into #{} (map :entity-sym) candidates)]
    (filterv
      (fn [{:keys [entity-sym]}]
        (<= 2 (count (set/intersection
                       anchor-vars
                       (connected-vars clause-vars entity-sym)))))
      candidates)))

(defn- materialize-protected-unique-anchors
  [context]
  (let [candidates (->> (protected-unique-constant-patterns context)
                        vec
                        (connected-unique-anchors (:parsed-q context)))]
    ;; A single unique literal is already an ideal selective planner root.
    ;; Eager binding is useful when multiple protected roots must constrain the
    ;; same join component, as in two-endpoint path queries.
    (if (empty? candidates)
      context
      (let [context (reduce
                      (fn [context {:keys [source pattern]}]
                        (let [rel (qresolve/lookup-pattern
                                    (assoc context :rels-bound-cache
                                           (volatile! {}))
                                    source pattern)]
                          (update context :rels qresolve/collapse-rels rel)))
                      context candidates)
            idxs    (into #{} (map :clause-idx) candidates)]
        (-> context
            (update-in [:parsed-q :qwhere] #(u/remove-idxs idxs %))
            (update-in [:parsed-q :qorig-where]
                       #(u/remove-idxs idxs %)))))))

(defn- materializable-bound-patterns
  [context bound?]
  (let [{:keys [parsed-q sources]} context
        qwhere (:qwhere parsed-q)]
    (keep-indexed
      (fn [clause-idx [parsed-clause orig-clause]]
        (when (and (instance? Pattern parsed-clause)
                   (vector? orig-clause))
          (let [pattern    (:pattern parsed-clause)
                e-sym     (pattern-var-symbol (first pattern))
                v-sym     (when (<= 3 (count pattern))
                            (pattern-var-symbol (nth pattern 2)))
                attr      (second pattern)
                e-bound?  (bound? context e-sym)
                v-bound?  (bound? context v-sym)
                source-sym (clause-source-symbol (:source parsed-clause))]
            (when (and (instance? Constant attr)
                       (keyword? (:value attr)))
              (when-let [source (get sources source-sym)]
                (let [attr-value (:value attr)
                      input-bound?
                      (and (or e-bound? v-bound?)
                           (not (and e-bound? v-bound?)))
                      unresolved-ref?
                      (or (and e-bound?
                               (relation-has-unresolved-ref?
                                 context source attr-value true e-sym))
                          (and v-bound?
                               (relation-has-unresolved-ref?
                                 context source attr-value false v-sym)))]
                  (when (and (db/-searchable? source)
                             input-bound?
                             (not unresolved-ref?))
                    {:clause-idx   clause-idx
                     :source       source
                     :source-sym   source-sym
                     :pattern      (pattern-form orig-clause)
                     :attr         attr-value
                     :entity-sym   e-sym
                     :value-sym    v-sym
                     :entity-bound? (boolean e-bound?)
                     :value-bound?  (boolean v-bound?)})))))))
      (map vector qwhere (:qorig-where parsed-q)))))

(defn- materializable-bound-pattern
  [context bound?]
  (first (materializable-bound-patterns context bound?)))

(defn- materializable-input-bound-pattern
  [context]
  (materializable-bound-pattern context small-bound-relation?))

(defn- cost-materializable-bound-pattern
  [context]
  (materializable-bound-pattern context bound-relation?))

(defn- integer-entity-relation?
  [rel entity-sym]
  (when-let [idx (get (:attrs rel) entity-sym)]
    (let [^List tuples (:tuples rel)
          ^long n      (if tuples (.size tuples) 0)]
      (when (pos? n)
        (loop [i (long 0)]
          (if (< i n)
            (let [entity (aget ^objects (.get tuples (int i)) (int idx))]
              (and (integer? entity)
                   (not (neg? (long entity)))
                   (recur (unchecked-inc i))))
            true))))))

(defn- distinct-value-candidates
  [candidates]
  (second
    (reduce
      (fn [[seen result] {:keys [value-sym] :as candidate}]
        (if (contains? seen value-sym)
          [seen result]
          [(conj seen value-sym) (conj result candidate)]))
      [#{} []]
      candidates)))

(defn- input-bound-eav-group
  [context]
  (let [candidates
        (->> (materializable-bound-patterns
               context small-bound-relation?)
             (filter
               (fn [{:keys [source attr entity-sym value-sym
                            entity-bound? value-bound?]}]
                 (and entity-bound?
                      (not value-bound?)
                      (qu/binding-var? entity-sym)
                      (qu/binding-var? value-sym)
                      (not= entity-sym value-sym)
                      (not (bound-relation? context value-sym))
                      (some? (get-in (db/-schema source) [attr :db/aid])))))
             vec)]
    (some
      (fn [{:keys [source-sym entity-sym]}]
        (let [group (->> candidates
                         (filter #(and (= source-sym (:source-sym %))
                                       (= entity-sym (:entity-sym %))))
                         distinct-value-candidates)]
          (when (<= 2 (count group))
            (let [rel (rel-for-var context entity-sym)]
              (when (integer-entity-relation? rel entity-sym)
                group)))))
      candidates)))

(defn- materialize-input-bound-eav-group
  [context candidates]
  (let [{:keys [source source-sym entity-sym]} (first candidates)
        schema       (db/-schema source)
        candidates   (vec
                       (sort-by #(get-in schema [(:attr %) :db/aid])
                                candidates))
        rel          (rel-for-var context entity-sym)
        ^List input  (:tuples rel)
        eid-idx      (long (get (:attrs rel) entity-sym))
        attrs-v      (mapv (fn [{:keys [attr]}]
                             [attr {:skip? false}])
                           candidates)
        tuples       (or (db/-eav-scan-v-list source input eid-idx attrs-v)
                         (FastList.))
        base-idx     (long (count (:attrs rel)))
        attrs        (reduce-kv
                       (fn [attrs idx {:keys [value-sym]}]
                         (assoc attrs value-sym
                                (unchecked-add base-idx (long idx))))
                       (:attrs rel) candidates)
        enriched     (assoc rel :attrs attrs :tuples tuples)
        clause-idxs  (into #{} (map :clause-idx) candidates)
        stage        {:strategy      :grouped-bound-eav
                      :source        source-sym
                      :entity        entity-sym
                      :attributes    (mapv :attr candidates)
                      :patterns      (mapv :pattern candidates)
                      :input-tuples  (.size input)
                      :output-tuples (.size ^List tuples)}]
    {:context
     (-> context
         (update :rels
                 (fn [rels]
                   (mapv #(if (identical? % rel) enriched %) rels)))
         (update-in [:parsed-q :qwhere]
                    #(u/remove-idxs clause-idxs %))
         (update-in [:parsed-q :qorig-where]
                    #(u/remove-idxs clause-idxs %)))
     :stage stage}))

(defn- record-input-bound-eav-group!
  [stage]
  (when qplan/*explain*
    (vswap! qplan/*explain* update :input-bound-eav-groups
            (fnil conj []) stage)))

(defn materialize-input-bound-patterns
  "Materialize patterns constrained by small input-bound relations before
   planning. When a query has multiple unique constant anchors whose entity
   variables must remain bound, seed all anchors first so they constrain the
   same join component instead of leaving one as a late filter."
  [context]
  (loop [context (materialize-protected-unique-anchors context)]
    (if-let [group (input-bound-eav-group context)]
      (let [{:keys [context stage]}
            (materialize-input-bound-eav-group context group)]
        (record-input-bound-eav-group! stage)
        (recur context))
      (if-let [{:keys [^long clause-idx source pattern]}
               (materializable-input-bound-pattern context)]
        (let [rel (qresolve/lookup-pattern
                    (assoc context :rels-bound-cache (volatile! {}))
                    source pattern)]
          (recur (-> context
                     (update :rels qresolve/collapse-rels rel)
                     (update-in [:parsed-q :qwhere]
                                #(u/remove-idxs #{clause-idx} %))
                     (update-in [:parsed-q :qorig-where]
                                #(u/remove-idxs #{clause-idx} %)))))
        context))))

(defn- bound-pattern-probe-count
  ^long [context pattern]
  (let [e-rel (rel-for-var context (first pattern))
        v-rel (rel-for-var context (nth pattern 2 nil))
        n     (long (cond
                      e-rel (relation-size e-rel)
                      v-rel (relation-size v-rel)
                      :else 1))]
    (max 1 n)))

(defn resolved-bound-entity
  [source entity]
  (cond
    (integer? entity) entity
    (or (qu/lookup-ref? entity) (keyword? entity)) (db/entid source entity)
    :else nil))

(defn resolved-bound-value
  [source attr value]
  (if (and (db/ref? source attr)
           (or (qu/lookup-ref? value) (keyword? value)))
    (db/entid source value)
    value))

(defn- capped-count-sum
  ^long [values count-value ^long cap]
  (let [value-count (.size ^HashSet values)
        sample-size (long (min value-count
                               collection-input-plugin-threshold))
        sampled     (take sample-size values)
        sample-sum
        (long
          (unreduced
            (reduce
              (fn [^long total value]
                (let [n     (long (count-value value))
                      total (if (< (- cap total) n)
                              (unchecked-inc cap)
                              (+ total n))]
                  (if (< cap total) (reduced total) total)))
              0 sampled)))]
    (cond
      (< cap sample-sum) (unchecked-inc cap)
      (= sample-size value-count) sample-sum
      (zero? sample-size) 0
      :else
      (long
        (min (unchecked-inc cap)
             (Math/ceil
               (* (double sample-sum)
                  (/ (double value-count) (double sample-size)))))))))

(defn- bounded-pattern-output-count
  ^long [context source pattern ^long cap]
  (let [pattern (qresolve/resolve-pattern-lookup-refs source pattern)
        [e attr v] pattern
        e-rel   (rel-for-var context e)
        v-rel   (rel-for-var context v)]
    (cond
      e-rel
      (let [entities (qu/relation-distinct-values e-rel e)
            entity-count (.size ^HashSet entities)
            value-var? (qu/binding-var? v)
            existence? (or (= v '_) (qu/placeholder? v))
            cardinality-many?
            (= :db.cardinality/many
               (get-in (db/-schema source) [attr :db/cardinality]))]
        ;; A cardinality-one attribute, an existence lookup, or a concrete
        ;; value can emit at most one row per bound entity. Apart from being
        ;; cheaper, this avoids treating stale per-entity count metadata as an
        ;; expansion signal.
        (if (or existence? (not value-var?) (not cardinality-many?))
          (long entity-count)
          (capped-count-sum
            entities
            (fn [entity]
              (if-let [entity (resolved-bound-entity source entity)]
                (db/-count source [entity attr nil])
                0))
            cap)))

      v-rel
      (let [values (qu/relation-distinct-values v-rel v)]
        (capped-count-sum
          values
          (fn [value]
            (let [value (resolved-bound-value source attr value)]
              (if (qu/binding-var? e)
                (av-size (.-store ^DB source) attr value)
                (if-let [entity (resolved-bound-entity source e)]
                  (db/-count source [entity attr value])
                  0))))
          cap))

      :else 0)))

(defn remove-materialized-clause
  [context ^long clause-idx]
  (-> context
      (update-in [:parsed-q :qwhere]
                 #(u/remove-idxs #{clause-idx} %))
      (update-in [:parsed-q :qorig-where]
                 #(u/remove-idxs #{clause-idx} %))))

(defn- affordable-output-cap
  "Return the largest one-column output that could fit in the remaining cost
   budget. This cap only bounds cardinality probing; the complete lookup and
   join estimate below makes the actual decision."
  ^long [^double budget]
  (let [per-row (+ (double c/magic-cost-hash-join-output-tuple)
                   (double c/magic-cost-hash-join-output-cell))]
    (if (or (not (pos? budget)) (not (pos? per-row)))
      0
      (long (min (double (dec Long/MAX_VALUE))
                 (Math/floor (/ budget per-row)))))))

(defn- abstract-bound-groups
  [context]
  (into []
        (keep (fn [rel]
                (let [rows (relation-size rel)]
                  (when (and (pos? rows) (seq (:attrs rel)))
                    {:vars   (set (keys (:attrs rel)))
                     :rows   rows
                     :exact? true}))))
        (:rels context)))

(defn- abstract-group-index
  [groups sym]
  (when (qu/binding-var? sym)
    (first
      (keep-indexed
        (fn [idx group]
          (when (contains? (:vars group) sym) idx))
        groups))))

(defn- abstract-materializable-bound-pattern
  [{:keys [parsed-q sources]} groups consumed]
  (first
    (keep-indexed
      (fn [clause-idx [parsed-clause orig-clause]]
        (when (and (not (contains? consumed clause-idx))
                   (instance? Pattern parsed-clause)
                   (vector? orig-clause))
          (let [parsed-pattern (:pattern parsed-clause)
                e-sym          (pattern-var-symbol (first parsed-pattern))
                v-sym          (when (<= 3 (count parsed-pattern))
                                 (pattern-var-symbol
                                   (nth parsed-pattern 2)))
                attr           (second parsed-pattern)
                e-group        (abstract-group-index groups e-sym)
                v-group        (abstract-group-index groups v-sym)]
            (when (and (instance? Constant attr)
                       (keyword? (:value attr))
                       (not= (some? e-group) (some? v-group)))
              (when-let [source (get sources
                                     (clause-source-symbol
                                       (:source parsed-clause)))]
                (when (db/-searchable? source)
                  {:clause-idx clause-idx
                   :source     source
                   :pattern    (qresolve/resolve-pattern-lookup-refs
                                 source (pattern-form orig-clause))
                   :group-idx  (long (or e-group v-group))
                   :entity-bound? (some? e-group)}))))))
      (map vector (:qwhere parsed-q)
           (:qorig-where parsed-q)))))

(defn- abstract-pattern-output-count
  [context source pattern group entity-bound? cap]
  (let [cap (long cap)]
    (if (:exact? group)
      (bounded-pattern-output-count context source pattern cap)
      (let [[_ attr value] pattern
            input-rows (long (:rows group))
            scan-rows  (long (db/-count source [nil attr nil]))
            cardinality-many?
            (= :db.cardinality/many
               (get-in (db/-schema source) [attr :db/cardinality]))
            value-var? (qu/binding-var? value)
            existence? (or (= value '_) (qu/placeholder? value))
            estimated
            (if (and entity-bound? (or existence?
                                       (not value-var?)
                                       (not cardinality-many?)))
              (min input-rows scan-rows)
              (long
                (min (double scan-rows)
                     (Math/ceil (* (double input-rows)
                                   (double c/magic-link-ratio))))))]
        (cap-output-count estimated cap)))))

(defn project-bound-patterns-with-cost
  "Project a complete propagation chain without reading its output tuples.
   This prevents a cheap first lookup from triggering an expensive speculative
   stage when a later, already-predictable stage exhausts the cost budget."
  [context ^double budget]
  (loop [groups   (abstract-bound-groups context)
         consumed #{}
         cost     0.0
         stages   []]
    (if-let [{:keys [^long clause-idx source pattern ^long group-idx
                     entity-bound?]}
             (abstract-materializable-bound-pattern
               context groups consumed)]
      (let [group          (nth groups group-idx)
            probe-count    (long (:rows group))
            remaining-cost (max 0.0 (- budget cost))
            output-count   (long
                             (abstract-pattern-output-count
                               context source pattern group entity-bound?
                               (affordable-output-cap remaining-cost)))
            projected-cost (projected-pattern-materialization-cost
                             source (second pattern)
                             probe-count output-count)
            new-cost       (+ cost projected-cost)
            stage          {:pattern pattern
                            :probes probe-count
                            :projected-rows output-count
                            :projected-cost projected-cost}]
        (if (<= budget new-cost)
          {:eligible? false
           :cost cost
           :stages (conj stages stage)
           :guardrail {:phase :propagation-preflight
                       :pattern pattern
                       :projected-rows output-count
                       :projected-cost projected-cost
                       :accumulated-cost cost
                       :budget budget}}
          (let [pattern-vars (into #{}
                                   (filter qu/binding-var?)
                                   [(first pattern) (nth pattern 2 nil)])
                next-group  {:vars   (set/union (:vars group) pattern-vars)
                             :rows   output-count
                             :exact? false}]
            (recur (assoc groups group-idx next-group)
                   (conj consumed clause-idx)
                   new-cost
                   (conj stages stage)))))
      {:eligible? true :cost cost :stages stages})))

(defn- grouped-bound-eav-projected-cost
  ^double [context candidates ^double budget]
  (loop [candidates candidates
         cost       0.0]
    (if-let [{:keys [source attr pattern]} (first candidates)]
      (let [remaining    (max 0.0 (- budget cost))
            output-count (bounded-pattern-output-count
                           context source pattern
                           (affordable-output-cap remaining))
            probe-count  (bound-pattern-probe-count context pattern)
            stage-cost   (projected-pattern-materialization-cost
                           source attr probe-count output-count)]
        (recur (next candidates) (+ cost stage-cost)))
      cost)))

(defn materialize-bound-patterns-with-cost
  [context ^double budget]
  (loop [context context
         cost    0.0
         stages  []]
    (if-let [group (input-bound-eav-group context)]
      (let [remaining-cost (double (max 0.0 (- budget cost)))
            projected-cost (double
                             (grouped-bound-eav-projected-cost
                               context group remaining-cost))]
        (if (<= budget (+ cost projected-cost))
          {:context context
           :cost cost
           :stages stages
           :eligible? false
           :guardrail {:strategy :grouped-bound-eav
                       :patterns (mapv :pattern group)
                       :projected-cost projected-cost
                       :accumulated-cost cost
                       :budget budget}}
          (let [{:keys [context stage]}
                (materialize-input-bound-eav-group context group)
                output-cost (double
                              (materialized-output-cost
                                (long (:output-tuples stage))
                                (count group)))
                charged-cost (double (max projected-cost output-cost))
                new-cost (double (+ cost charged-cost))
                stage (assoc stage
                             :projected-cost projected-cost
                             :output-cost output-cost
                             :charged-cost charged-cost)]
            (if (<= budget new-cost)
              {:context context
               :cost new-cost
               :stages (conj stages stage)
               :eligible? false
               :guardrail {:strategy :grouped-bound-eav
                           :patterns (:patterns stage)
                           :actual-cost new-cost
                           :budget budget}}
              (recur context new-cost (conj stages stage))))))
      (if-let [{:keys [^long clause-idx source pattern]}
               (cost-materializable-bound-pattern context)]
        (let [remaining-cost (max 0.0 (- budget cost))
              output-count (bounded-pattern-output-count
                             context source pattern
                             (affordable-output-cap remaining-cost))
              probe-count (bound-pattern-probe-count context pattern)
              projected-cost (projected-pattern-materialization-cost
                               source (second pattern)
                               probe-count output-count)]
          (if (<= budget (+ cost projected-cost))
            {:context context
             :cost cost
             :stages stages
             :eligible? false
             :guardrail {:pattern pattern
                         :projected-rows output-count
                         :projected-cost projected-cost
                         :accumulated-cost cost
                         :budget budget}}
            (let [result   (materialize-pattern-with-cost
                             context source pattern probe-count)
                  charged-cost (max projected-cost (double (:cost result)))
                  new-cost (+ cost charged-cost)]
              (if (<= budget new-cost)
                {:context (:context result)
                 :cost new-cost
                 :stages (conj stages (:stage result))
                 :eligible? false
                 :guardrail {:pattern pattern
                             :actual-cost new-cost
                             :budget budget}}
                (recur (remove-materialized-clause
                         (:context result) clause-idx)
                       new-cost
                       (conj stages (:stage result)))))))
        {:context context :cost cost :stages stages :eligible? true}))))
