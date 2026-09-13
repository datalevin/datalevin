;;
;; Copyright (c) Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns ^:no-doc datalevin.query.optimizer.access-plan
  "Correlated access scheduling, indexed joins, and bounded sampling
  of physical access plans."
  (:require
   [clojure.set :as set]
   [datalevin.constants :as c]
   [datalevin.db :as db]
   [datalevin.interface
    :refer [av-size]]
   [datalevin.parser :as dp]
   [datalevin.query-util :as qu]
   [datalevin.query.access :as qaccess]
   [datalevin.query.optimizer.bound-patterns
    :refer [resolved-bound-entity resolved-bound-value]]
   [datalevin.query.optimizer.estimates
    :refer [access-variable-domain-estimates capped-tuple-count-sum
            count-only-output-cap estimate-link-cost estimate-round
            estimate-row-evaluation-cost materialized-output-cost
            over-count-cap relation-size scaled-row-estimate]]
   [datalevin.query.optimizer.plan-cost
    :refer [adjusted-scan-ratio fast-clause-count]]
   [datalevin.query.optimizer.rewrite
    :refer [clause-source-symbol]]
   [datalevin.query.optimizer.sampling
    :refer [access-sample-relation estimated-access-sample-stage-cost
            sample-context-size sample-relation-projection-count
            sample-safe-residual? sample-stage-output-cap
            sampled-access-output]]
   [datalevin.query.plan :as qplan]
   [datalevin.query.resolve :as qresolve]
   [datalevin.relation :as r]
   [datalevin.util :as u])
  (:import
   [java.util List]
   [datalevin.db DB]
   [datalevin.parser Constant Function Or Variable Pattern Predicate Not RuleExpr]
   [org.eclipse.collections.impl.list.mutable FastList]))

(declare access-clause-deps access-clause-ready?)

(defn- access-join-candidate
  [db access-source covered clause-idx clause]
  (when (and (not (contains? covered clause))
             (instance? Pattern clause)
             (= (or (qaccess/source-symbol access-source) '$)
                (clause-source-symbol (:source ^Pattern clause))))
    (let [pattern (:pattern ^Pattern clause)]
      (when (= 3 (count pattern))
        (let [e    (nth pattern 0)
              a    (nth pattern 1)
              v    (nth pattern 2)
              attr (when (instance? Constant a) (:value ^Constant a))
              evar (when (instance? Variable e) (:symbol ^Variable e))
              vvar (when (instance? Variable v) (:symbol ^Variable v))]
          (when (and evar (keyword? attr))
            (let [val  (when (instance? Constant v) (:value ^Constant v))
                  rows (fast-clause-count db nil {:attr attr :val val}
                                          Long/MAX_VALUE)]
              {:clause-idx clause-idx
               :clause     clause
               :attr       attr
               :entity-var evar
               :value-var  vvar
               :value      val
               :constant-value? (instance? Constant v)
               :cols       (cond-> [evar] vvar (conj vvar))
               :vars       (cond-> #{evar} vvar (conj vvar))
               :rows       rows})))))))

(defn- eligible-access-join
  [bound {:keys [entity-var value-var] :as join}]
  (let [entity-bound? (contains? bound entity-var)
        value-bound?  (and value-var (contains? bound value-var))]
    (when (or entity-bound? value-bound?)
      join)))

(defn- access-join-candidates
  [db parsed-q {:keys [expr]}]
  (into []
        (keep-indexed
          #(access-join-candidate
             db (:source expr) (:covers expr) %1 %2))
        (:qwhere parsed-q)))

(defn access-join-from-bound
  [bound {:keys [cols vars rows entity-var value-var] :as candidate}]
  (when-let [join (eligible-access-join bound candidate)]
    (-> join
        (dissoc :vars :rows)
        (assoc :requires (set/intersection bound vars)
               :produces (set/difference vars bound)
               :produces-cols (into [] (remove bound) cols)
               :entity-bound? (contains? bound entity-var)
               :value-bound? (and value-var (contains? bound value-var))
               :estimate {:rows rows
                          :confidence :medium}))))

(defn- order-access-joins
  [db parsed-q {:keys [expr] :as plan}]
  (let [initial-bound (into (:requires expr)
                            (filter qu/binding-var?)
                            (:cols expr))
        candidates    (access-join-candidates db parsed-q plan)]
    (loop [bound     initial-bound
           candidates candidates
           joins      []]
      (let [eligible (keep #(eligible-access-join bound %) candidates)]
        (if (seq eligible)
          (let [{:keys [clause-idx vars] :as chosen}
                ;; On equal catalog counts, apply a constant-value filter
                ;; before a value-producing lookup that may fan out.
                (apply u/min-key-comp
                       (juxt :rows #(if (:constant-value? %) 0 1))
                       eligible)
                join (access-join-from-bound bound chosen)]
            (recur (into bound vars)
                   (filterv #(not= clause-idx (:clause-idx %)) candidates)
                   (conj joins join)))
          joins)))))

(defn- input-bound-vars
  [parsed-q inputs]
  (qresolve/bound-vars
    (qresolve/resolve-ins (qplan/make-context parsed-q false) inputs)))

(defn- order-correlated-bindings
  [db parsed-q inputs {:keys [expr]}]
  (let [required   (set (:requires expr))
        input-bound (set (input-bound-vars parsed-q inputs))
        candidates
        (into []
              (keep-indexed
                (fn [i clause]
                  (let [orig-clause (nth (:qorig-where parsed-q) i)]
                    (when-not
                      (or (contains? (:covers expr) clause)
                          (contains? (:covered-originals expr) orig-clause))
                      (let [pattern
                            (access-join-candidate
                              db (:source expr) #{} i clause)
                            deps (access-clause-deps orig-clause)
                            vars (set/union
                                   (set (:requires deps))
                                   (set (:requires-any deps))
                                   (set (:provides deps)))]
                        (assoc deps
                               :clause-idx i
                               :clause clause
                               :orig-clause orig-clause
                               :vars vars
                               :rows (long
                                       (or (:rows pattern)
                                           Long/MAX_VALUE))))))))
              (:qwhere parsed-q))
        needed
        (loop [needed required]
          (let [needed'
                (reduce
                  (fn [needed {:keys [requires provides]}]
                    (if (seq (set/intersection needed (set provides)))
                      (into needed requires)
                      needed))
                  needed candidates)]
            (if (= needed needed')
              needed
              (recur needed'))))]
    (loop [bound      input-bound
           candidates candidates
           outer      []]
      (if (qaccess/access-ready? expr bound)
        {:outer-joins outer
         :outer-cols  (vec
                        (sort-by str
                                 (set/union required
                                            (into #{} (mapcat :vars) outer))))}
        (let [eligible
              (filter
                (fn [{:keys [provides] :as candidate}]
                  (and (access-clause-ready? bound candidate)
                       (seq (set/intersection
                              (set provides)
                              (set/difference needed bound)))))
                candidates)]
          (when (seq eligible)
            (let [chosen (apply min-key :rows eligible)]
              (recur (into bound (:provides chosen))
                     (filterv #(not= (:clause-idx chosen)
                                     (:clause-idx %))
                              candidates)
                     (conj outer chosen)))))))))

(defn schedule-correlated-access
  "Schedule a correlated access source after a bounded outer subset has
   produced every variable in AccessExpr.requires. Returns nil when no safe
   outer subset can provide the requirements."
  [db parsed-q inputs
   {:keys [expr path demand bounds work access-source] :as plan}]
  (when (satisfies? qaccess/ICorrelatedAccessMethod
                    (:implementation path))
    (when-let [{:keys [outer-cols] :as schedule}
               (order-correlated-bindings db parsed-q inputs plan)]
      (let [finite-rows (keep (fn [{:keys [rows]}]
                                (when (< (long rows) Long/MAX_VALUE)
                                  (long rows)))
                              (:outer-joins schedule))
            minimum    (long (or (when (seq finite-rows)
                                   (apply min finite-rows))
                                 1))
            outer-rows (max 1 minimum)
            outer-cost (* (double outer-rows)
                          (double (max 1
                                       (count (:outer-joins schedule)))))]
        (merge plan schedule
               {:correlated? true
                :outer-estimate {:rows outer-rows
                                 :cost outer-cost
                                 :confidence :medium}
                :step (qplan/access-step
                        expr path demand bounds work outer-cols
                        access-source)})))))

(defn- access-join-pattern
  [parsed-q clause-idx]
  (let [pattern (nth (:qorig-where parsed-q) clause-idx)]
    (if (and (vector? pattern) (qu/source? (first pattern)))
      (subvec pattern 1)
      pattern)))

(defn- access-clause-vars
  [form]
  (into #{} (filter qu/binding-var?) (qu/collect-vars form)))

(defn- access-clause-deps
  [clause]
  (let [clause (if (and (sequential? clause)
                        (qu/source? (first clause)))
                 (next clause)
                 clause)
        head   (when (sequential? clause) (first clause))]
    (cond
      (and (sequential? head) (= 1 (count clause)))
      {:requires (access-clause-vars head)}

      (and (sequential? head) (= 2 (count clause)))
      {:requires (access-clause-vars head)
       :provides (access-clause-vars (second clause))}

      (= 'not head)
      {:requires-any (access-clause-vars (next clause))}

      (= 'not-join head)
      {:requires (access-clause-vars (second clause))}

      (= 'or-join head)
      (let [vars-form (second clause)
            req-form  (when (and (sequential? vars-form)
                                 (sequential? (first vars-form)))
                        (first vars-form))]
        {:requires (access-clause-vars req-form)
         :provides (access-clause-vars vars-form)})

      :else
      {:provides (access-clause-vars clause)})))

(defn- access-clause-ready?
  [bound {:keys [requires requires-any]}]
  (and (every? bound requires)
       (or (empty? requires-any)
           (some bound requires-any))))

(defn- order-access-residuals
  [bound entries]
  (let [entries (mapv #(merge % (access-clause-deps (:orig-clause %)))
                      entries)]
    (loop [bound bound
           todo  entries
           acc   []]
      (if (empty? todo)
        acc
        (if-let [idx (first
                       (keep-indexed
                         (fn [i entry]
                           (when (access-clause-ready? bound entry) i))
                         todo))]
          (let [entry (nth todo idx)]
            (recur (into bound (:provides entry))
                   (u/vec-remove todo idx)
                   (conj acc entry)))
          ;; Preserve query order for clauses whose dependencies require
          ;; runtime rule/or semantics that this lightweight sorter cannot
          ;; prove. Resolution will still enforce binding correctness.
          (into acc todo))))))

(defn- residual-operation
  [clause]
  (cond
    (instance? Pattern clause)   :indexed-join
    (instance? Predicate clause) :predicate
    (instance? Function clause)  :function
    (instance? Not clause)       :not
    (instance? Or clause)        :or
    (instance? RuleExpr clause)  :rule
    :else                        :residual))

(defn- access-residual-entries
  [parsed-q step joins]
  (let [covered-clauses (or (get-in step [:expr :covers]) #{})
        covered-originals
        (or (get-in step [:expr :covered-originals]) #{})
        joined (into #{} (map :clause-idx) joins)]
    (into []
          (keep-indexed
            (fn [i [clause orig-clause]]
              (when-not (or (joined i)
                            (contains? covered-clauses clause)
                            (contains? covered-originals orig-clause))
                {:clause-idx i
                 :clause clause
                 :orig-clause orig-clause})))
          (map vector (:qwhere parsed-q) (:qorig-where parsed-q)))))

(defn- sample-access-residuals
  [parsed-q context step joins stages]
  (let [bound   (qresolve/bound-vars context)
        entries (order-access-residuals
                  bound (access-residual-entries parsed-q step joins))]
    (reduce
      (fn [[context stages] {:keys [clause-idx clause orig-clause]}]
        (let [before   (sample-context-size (:rels context))
              safe?    (sample-safe-residual? clause)
              context' (if safe?
                         (qresolve/resolve-clause context orig-clause)
                         context)
              after    (sample-context-size (:rels context'))]
          [context'
           (conj stages
                 {:clause-idx clause-idx
                  :operation  (if safe?
                                (residual-operation clause)
                                :opaque-predicate)
                  :modeled?   safe?
                  :input      before
                  :output     after})]))
      [context stages]
      entries)))

(defn- safely-resolvable-access-residuals?
  [parsed-q context step joins]
  (let [bound   (qresolve/bound-vars context)
        entries (order-access-residuals
                  bound (access-residual-entries parsed-q step joins))]
    (loop [bound bound
           entries entries]
      (if-let [{:keys [clause provides] :as entry} (first entries)]
        (and (access-clause-ready? bound entry)
             (instance? Predicate clause)
             (sample-safe-residual? clause)
             (recur (into bound provides) (next entries)))
        true))))

(defn- terminal-access-join-countable?
  "A terminal EAV lookup can be sampled by counting without materializing its
   values when the projected entity/value pair proves distinct result rows."
  [parsed-q context step planned-joins remaining-joins
   {:keys [entity-var value-var]}]
  (and (empty? remaining-joins)
       entity-var
       value-var
       (let [find-vars (set (dp/find-vars (:qfind parsed-q)))]
         (and (contains? find-vars entity-var)
              (contains? find-vars value-var)))
       (safely-resolvable-access-residuals?
         parsed-q context step planned-joins)))

(defn- projected-access-join-output
  "Estimate a bound pattern's joined output from the actual access sample,
  without materializing that output. Counts are weighted by duplicate sample
  rows, since those duplicates participate independently in the subsequent
  hash join."
  ^long [^DB db relation
         {:keys [attr entity-var value-var value entity-bound? value-bound?
                 constant-value?]}
         ^long cap]
  (let [attrs       (:attrs relation)
        ^List tuples (:tuples relation)
        entity-idx  (get attrs entity-var)
        value-idx   (get attrs value-var)
        store       (.-store db)
        count-tuple
        (cond
          (and entity-bound? value-bound? entity-idx value-idx)
          (fn [^objects tuple]
            (let [entity (resolved-bound-entity db
                                                (aget tuple (long entity-idx)))
                  value  (resolved-bound-value db attr
                                               (aget tuple (long value-idx)))]
              (if (and entity (some? value))
                (if (db/-populated? db :eav entity attr value) 1 0)
                0)))

          (and entity-bound? constant-value? entity-idx)
          (fn [^objects tuple]
            (let [entity (resolved-bound-entity db
                                                (aget tuple (long entity-idx)))
                  value  (resolved-bound-value db attr value)]
              (if (and entity (some? value))
                (if (db/-populated? db :eav entity attr value) 1 0)
                0)))

          (and entity-bound? entity-idx)
          (fn [^objects tuple]
            (if-let [entity (resolved-bound-entity
                              db (aget tuple (long entity-idx)))]
              (if (identical? :db.cardinality/many
                              (get-in (db/-schema db)
                                      [attr :db/cardinality]))
                (count (db/-datoms db :eav entity attr))
                (if (db/-ea-populated? db entity attr) 1 0))
              0))

          (and value-bound? value-idx)
          (fn [^objects tuple]
            (let [value (resolved-bound-value
                          db attr (aget tuple (long value-idx)))]
              (if (some? value) (av-size store attr value) 0)))

          :else nil)]
    (cond
      (nil? tuples) 0
      count-tuple
      (capped-tuple-count-sum tuples count-tuple cap)

      :else
      (min (long (.size tuples)) (over-count-cap cap)))))

(defn- terminal-access-count-sample
  [db parsed-q context step planned-joins joins join access-var sample
   sample-batch sample-rows stages spent budget]
  (let [sample-rows (long sample-rows)
        spent       (double spent)
        budget      (double budget)]
    (when (terminal-access-join-countable?
            parsed-q context step planned-joins (next joins) join)
      (let [[context stages]
            (sample-access-residuals
              parsed-q context step planned-joins stages)
            relation      (access-sample-relation context access-var)
            before        (relation-size relation)
            find-vars     (dp/find-vars (:qfind parsed-q))
            unique-input? (= before
                             (sample-relation-projection-count
                               find-vars relation))
            output-cap    (count-only-output-cap (- budget spent) before)
            output        (when unique-input?
                            (projected-access-join-output
                              db relation join output-cap))]
        (when (some? output)
          (let [output     (long output)
                count-cost (double (estimate-link-cost before output))
                total-cost (+ spent count-cost)]
            (when (<= total-cost budget)
              (let [other-output
                    (long (reduce
                            (fn [n other]
                              (if (identical? relation other)
                                n
                                (estimate-round
                                  (* (double n)
                                     (double
                                       (sample-relation-projection-count
                                         find-vars other))))))
                            1 (:rels context)))
                    sample-output
                    (estimate-round (* (double output)
                                       (double other-output)))]
                {:sample        sample
                 :sample-batch  sample-batch
                 :sample-rows   sample-rows
                 :sample-output sample-output
                 :sample-cost   total-cost
                 :stages
                 (conj stages
                       {:clause-idx (:clause-idx join)
                        :attr       (:attr join)
                        :operation  :indexed-join
                        :input      before
                        :output     output
                        :cost       count-cost
                        :sampling   :counted})}))))))))

(defn- preflight-access-join-output
  ^long [^long input-size domains
         {:keys [entity-var value-var entity-bound? value-bound? estimate]}]
  (let [rows          (long (or (:rows estimate) 0))
        entity-domain (long (or (get domains entity-var) rows 1))
        value-domain  (long (or (get domains value-var) rows 1))
        entity-ratio  (if (pos? entity-domain)
                        (min 1.0 (/ (double rows) entity-domain))
                        1.0)
        value-ratio   (if (pos? value-domain)
                        (/ (double rows) value-domain)
                        1.0)
        ratio         (cond
                        (and entity-bound? value-bound?) entity-ratio
                        entity-bound?                    entity-ratio
                        value-bound?                     value-ratio
                        :else                            1.0)]
    (scaled-row-estimate input-size ratio)))

(defn- access-sample-preflight
  "Project the indexed fragment's sample work from catalog counts before
   opening its cursor. In particular, a reverse lookup is charged by the
   attribute row count divided by the tightest known domain for its bound
   value. The projection uses the same budget and operator cost model as the
   materialized sample, so an over-budget fragment can be rejected without
   reading speculative tuples."
  [step joins join-candidates sample-size budget estimate range-rows]
  (let [sample-size (long sample-size)
        budget      (double budget)
        range-rows  (long range-rows)
        domains     (access-variable-domain-estimates
                      step join-candidates range-rows)
        source-cost (+ (double (or (:startup estimate) 0.0))
                       (* (double sample-size)
                          (double (or (:per-row estimate) 0.0))))]
    (if (> source-cost budget)
      {:aborted? true
       :stages []
       :abort {:reason :sample-work-budget
               :phase :access-propagation-preflight
               :budget budget
               :cost 0.0
               :projected-cost source-cost
               :projected-output sample-size}}
      (loop [input-size sample-size
             output-width (count (:cols step))
             spent source-cost
             joins joins
             stages []]
        (if-let [{:keys [clause-idx attr produces-cols] :as join}
                 (first joins)]
          (let [output-size (preflight-access-join-output
                              input-size domains join)
                output-width (+ output-width (count produces-cols))
                projected-cost
                (estimated-access-sample-stage-cost
                  input-size output-size output-width)
                stage {:clause-idx clause-idx
                       :attr attr
                       :operation :indexed-join
                       :input input-size
                       :output output-size
                       :projected-output output-size
                       :projected-cost projected-cost
                       :sampling :preflight}]
            (if (> (+ spent projected-cost) budget)
              {:aborted? true
               :stages (conj stages stage)
               :abort {:reason :sample-work-budget
                       :phase :access-propagation-preflight
                       :clause-idx clause-idx
                       :attr attr
                       :budget budget
                       :cost spent
                       :projected-cost projected-cost
                       :projected-output output-size}}
              (recur output-size output-width (+ spent projected-cost)
                     (next joins) (conj stages stage))))
          {:aborted? false :stages stages :cost spent})))))

(defn- sample-access-joins
  [db parsed-q inputs step planned-joins sample-size sample-cost-budget estimate]
  (if (pos? (long sample-size))
    (let [sample-work (assoc (:work step)
                             :sample-size sample-size
                             :batch-size sample-size)
          sample-step (assoc step :work sample-work)
          sample-batch  (qplan/access-sample-batch sample-step db)
          ^List sample  (:tuples sample-batch)
          initial       (r/relation! (qplan/cols->attrs (:cols step)) sample)
          context       (-> (qplan/make-context parsed-q false)
                            (qresolve/resolve-ins inputs)
                            (update :rels qresolve/collapse-rels initial))
          access-var    (first (:cols step))
          sample-rows   (long (.size sample))
          source-cost   (+ (double (or (:startup estimate) 0.0))
                           (* (double sample-rows)
                              (double (or (:per-row estimate) 0.0))))
          bounded?      (and (number? sample-cost-budget)
                             (Double/isFinite
                               (double sample-cost-budget)))
          budget        (if bounded?
                          (max 0.0 (double sample-cost-budget))
                          Double/POSITIVE_INFINITY)]
      (loop [context context
             joins   planned-joins
             stages  []
             spent   source-cost]
        (let [relation (access-sample-relation context access-var)
              before   (relation-size relation)]
          (cond
            (and bounded? (> spent budget))
            {:sample        sample
             :sample-batch  nil
             :sample-rows   sample-rows
             :sample-output 0
             :sample-cost   spent
             :stages        stages
             :aborted?      true
             :abort         {:reason :sample-work-budget
                             :budget budget
                             :cost spent}}

            (or (zero? before) (empty? joins))
            (let [[context stages]
                  (sample-access-residuals
                    parsed-q context step planned-joins stages)]
              {:sample        sample
               :sample-batch  sample-batch
               :sample-rows   sample-rows
               :sample-output (sampled-access-output parsed-q (:rels context))
               :sample-cost   spent
               :stages        stages})

            :else
            (let [{:keys [clause-idx attr produces-cols] :as join}
                  (first joins)
                  output-width (+ (count (:attrs relation))
                                  (count produces-cols))
                  remaining    (- budget spent)
                  output-cap   (if bounded?
                                 (sample-stage-output-cap
                                   remaining output-width)
                                 Long/MAX_VALUE)
                  projected    (when bounded?
                                 (projected-access-join-output
                                   db relation join output-cap))
                  projected-cost
                  (when projected
                    (estimated-access-sample-stage-cost
                      before projected output-width))]
              (if (and bounded?
                       (> (+ spent (double projected-cost)) budget))
                (or
                  (terminal-access-count-sample
                    db parsed-q context step planned-joins joins join access-var
                    sample sample-batch sample-rows stages spent budget)
                  {:sample        sample
                   :sample-batch  nil
                   :sample-rows   sample-rows
                   :sample-output 0
                   :sample-cost   spent
                   :stages
                   (conj stages
                         {:clause-idx clause-idx
                          :attr       attr
                          :operation  :indexed-join
                          :input      before
                          :output     projected
                          :projected-output projected
                          :projected-cost projected-cost
                          :sampling   :aborted})
                   :aborted?      true
                   :abort         {:reason :sample-work-budget
                                   :clause-idx clause-idx
                                   :attr attr
                                   :budget budget
                                   :cost spent
                                   :projected-cost projected-cost
                                   :projected-output projected}})
                (let [pattern  (access-join-pattern parsed-q clause-idx)
                      new-rel  (qresolve/lookup-pattern
                                 (assoc context :rels-bound-cache
                                        (volatile! {}))
                                 db pattern)
                      rels     (qresolve/collapse-rels
                                 (:rels context) new-rel)
                      relation (access-sample-relation
                                 (assoc context :rels rels) access-var)
                      after    (relation-size relation)
                      actual-cost
                      (+ (double
                           (estimate-link-cost before
                                               (relation-size new-rel)))
                         (materialized-output-cost
                           after (count (:attrs relation))))]
                  (recur (assoc context :rels rels)
                         (next joins)
                         (conj stages
                               {:clause-idx clause-idx
                                :attr       attr
                                :operation  :indexed-join
                                :input      before
                                :output     after
                                :cost       actual-cost})
                         (+ spent actual-cost)))))))))
    {:sample        (FastList.)
     :sample-batch  nil
     :sample-rows   0
     :sample-output 0
     :sample-cost   0.0
     :stages        []}))

(defn- adjusted-access-yield
  ^double
  [^long sample-rows ^long sample-output]
  (if (zero? sample-rows)
    0.0
    (adjusted-scan-ratio sample-rows sample-output)))

(defn- unsampled-access-yield
  ^double
  [parsed-q step joins ^long range-rows]
  (let [^double join-yield
        (reduce
          (fn [^double yield join]
            (let [rows  (long (or (get-in join [:estimate :rows]) 0))
                  ratio (if (pos? range-rows)
                          (min 1.0
                               (adjusted-scan-ratio range-rows rows))
                          0.0)]
              (* yield ratio)))
          1.0 joins)
        residual-count
        (count (access-residual-entries parsed-q step joins))]
    (* join-yield
       (Math/pow (double c/magic-scan-ratio)
                 (double residual-count)))))

(defn- access-candidate-budget
  ^long
  [^long required-count ^long range-rows ^double yield]
  (if (or (zero? range-rows) (zero? yield))
    0
    (min range-rows
         (max 1 (long
                  (estimate-round (/ (double required-count) yield)))))))

(defn- proportional-scan-rows
  ^long
  [^long candidate-rows ^long range-rows ^long scan-rows]
  (cond
    (or (zero? candidate-rows) (zero? range-rows) (zero? scan-rows)) 0
    (<= range-rows candidate-rows) scan-rows
    :else
    (min scan-rows
         (max 1
              (long
                (Math/ceil
                  (* (double scan-rows)
                     (/ (double candidate-rows)
                        (double range-rows)))))))))

(defn- adjusted-access-cost
  [estimate scan-rows input-rows stages]
  (let [base-cost (if (pos? (long scan-rows))
                    (+ (double (:startup estimate))
                       (* (double scan-rows)
                          (double (:per-row estimate))))
                    0.0)]
    (first
      (reduce
        (fn [[cost input-size] {:keys [operation input output]}]
          (let [ratio       (adjusted-scan-ratio (long input) (long output))
                output-size (estimate-round (* (double input-size) ratio))
                stage-cost  (if (#{:predicate :function :opaque-predicate}
                                  operation)
                              (estimate-row-evaluation-cost input-size)
                              (estimate-link-cost input-size output-size))]
            [(+ (double cost)
                (double stage-cost))
             output-size]))
        [base-cost input-rows]
        stages))))

(defn- adjust-access-sample
  [db parsed-q inputs plan step sample? bounded-sample? sample-size
   sample-cost-budget estimate range-rows join-candidates]
  (let [preflight (when bounded-sample?
                    (access-sample-preflight
                      step (:joins plan) join-candidates sample-size
                      (max 0.0 (double sample-cost-budget)) estimate
                      range-rows))]
    (cond
      (:aborted? preflight)
      {:sample        (FastList.)
       :sample-batch  nil
       :sample-rows   0
       :sample-output 0
       :sample-cost   0.0
       :stages        (:stages preflight)
       :aborted?      true
       :abort         (:abort preflight)}

      sample?
      (sample-access-joins
        db parsed-q inputs step (:joins plan) sample-size
        sample-cost-budget estimate)

      :else
      {:sample        (FastList.)
       :sample-batch  nil
       :sample-rows   0
       :sample-output 0
       :sample-cost   0.0
       :stages        []})))

(defn- access-remaining-budget
  [sample-rows candidate-budget range-rows point-scan-rows scan-rows reusable?]
  (let [reused-rows (long (if reusable? sample-rows 0))
        candidate-remaining (- (long candidate-budget) reused-rows)
        range-remaining (- (long range-rows) reused-rows)
        point-scan-remaining (- (long point-scan-rows) reused-rows)
        scan-remaining (- (long scan-rows) reused-rows)]
    {:reused-rows          reused-rows
     :remaining-candidates (if (pos? candidate-remaining)
                             candidate-remaining
                             0)
     :remaining-range      (if (pos? range-remaining) range-remaining 0)
     :remaining-point-scan (if (pos? point-scan-remaining)
                             point-scan-remaining
                             0)
     :remaining-scan       (if (pos? scan-remaining) scan-remaining 0)}))

(defn- access-selection-cost
  [estimate adaptive? sample-rows sample-output range-rows
   remaining-point-scan point-output-rows remaining-scan output-rows stages]
  (let [point-cost (adjusted-access-cost
                     estimate remaining-point-scan point-output-rows stages)
        upper-cost (adjusted-access-cost
                     estimate remaining-scan output-rows stages)
        selection-cost (if (or (not adaptive?)
                               (and (pos? (long sample-rows))
                                    (zero? (long sample-output))
                                    (< (long sample-rows) (long range-rows))))
                         upper-cost
                         point-cost)]
    {:point-cost     point-cost
     :upper-cost     upper-cost
     :selection-cost selection-cost}))

(defn- apply-access-adjustments
  [plan step work estimate sample sample-batch reusable? aborted? abort]
  (cond-> (assoc plan
                 :work work
                 :estimate estimate
                 :step (assoc step
                              :work work
                              :sample sample
                              :sample-batch (when reusable? sample-batch))
                 :sample-batch (when reusable? sample-batch))
    aborted?
    (assoc :unavailable? true
           :unavailable-reason (:reason abort))))

(defn- adjust-access-plan
  [db parsed-q inputs
   {:keys [step path demand work estimate sample-cost-budget join-candidates]
    :as plan}]
  (if step
    (let [range-rows      (qaccess/estimate-range-rows estimate)
          scan-rows       (qaccess/estimate-scan-rows estimate)
          output-rows     (qaccess/estimate-output-rows estimate)
          adaptive?       (qaccess/adaptive-demand? path demand)
          sample?         (and (qaccess/planning-sample? path)
                               (pos? range-rows))
          sample-size     (if sample?
                            (long (min range-rows
                                       (long c/init-exec-size-threshold)))
                            0)
          bounded-sample? (and sample?
                               (number? sample-cost-budget)
                               (Double/isFinite
                                 (double sample-cost-budget)))
          {:keys [sample sample-batch sample-rows sample-output sample-cost
                  stages aborted? abort]}
          (adjust-access-sample db parsed-q inputs plan step sample?
                                bounded-sample? sample-size sample-cost-budget
                                estimate range-rows join-candidates)
          sample-rows     (long sample-rows)
          sample-output   (long sample-output)
          yield           (if (pos? sample-rows)
                            (adjusted-access-yield sample-rows sample-output)
                            (double
                              (if (contains? estimate :yield)
                                (:yield estimate)
                                (unsampled-access-yield
                                  parsed-q step (:joins plan) range-rows))))
          heuristic-yield?
          (and (zero? sample-rows)
               (not (contains? estimate :yield))
               (< yield 1.0))
          complete?       (nil? (:required-count demand))
          initial-candidate-budget
          (if complete?
            range-rows
            (access-candidate-budget (long (:required-count demand))
                                     range-rows yield))
          candidate-budget
          (long
            (if (and adaptive? heuristic-yield?)
              (min range-rows
                   (* 2 (long initial-candidate-budget)))
              initial-candidate-budget))
          point-output-rows
          (long (if adaptive?
                  (min output-rows candidate-budget)
                  output-rows))
          point-scan-rows
          (long (if adaptive?
                  (proportional-scan-rows
                    candidate-budget range-rows scan-rows)
                  scan-rows))
          reusable?       (and sample?
                               (qaccess/reusable-sample? path)
                               adaptive?
                               (some? sample-batch))
          {:keys [reused-rows remaining-candidates remaining-range
                  remaining-point-scan remaining-scan]}
          (access-remaining-budget sample-rows candidate-budget range-rows
                                   point-scan-rows scan-rows reusable?)
          {:keys [point-cost upper-cost selection-cost]}
          (access-selection-cost estimate adaptive? sample-rows sample-output
                                 range-rows remaining-point-scan
                                 point-output-rows remaining-scan output-rows
                                 stages)
          estimate        (assoc estimate
                                 :rows point-output-rows
                                 :range-rows range-rows
                                 :scan-rows scan-rows
                                 :output-rows output-rows
                                 :point-scan-rows point-scan-rows
                                 :point-output-rows point-output-rows
                                 :cost selection-cost
                                 :point-cost point-cost
                                 :upper-cost upper-cost
                                 :confidence (cond
                                               aborted? :bounded-sample
                                               sample?  :sampled
                                               :else
                                               (or (:confidence estimate) :low))
                                 :sample-rows sample-rows
                                 :sample-output sample-output
                                 :planning-sample-cost sample-cost
                                 :sampling-abort abort
                                 :reused-candidates reused-rows
                                 :remaining-candidates remaining-candidates
                                 :remaining-range remaining-range
                                 :remaining-scan-rows remaining-scan
                                 :remaining-point-scan-rows
                                 remaining-point-scan
                                 :yield yield
                                 :join-stages stages)
          work
          (cond->
              (assoc work :max-candidates candidate-budget)
            (and adaptive?
                 (pos? (long initial-candidate-budget)))
            (assoc :batch-size
                   (min
                     (long (or (:batch-size work)
                               initial-candidate-budget))
                     (long initial-candidate-budget))))]
      (apply-access-adjustments plan step work estimate sample sample-batch
                                reusable? aborted? abort))
    plan))

(defn plan-access-joins
  "Add a greedy order for indexed pattern joins reachable from each access
   plan's initially produced variables. This is called only when access plans
   exist; ordinary query planning stays on the existing path. The optional
   sample-cost-budget is shared across alternatives so rejected planning
   samples cannot collectively cost more than the conventional root."
  ([parsed-q inputs plans]
   (plan-access-joins parsed-q inputs plans nil))
  ([parsed-q inputs plans sample-cost-budget]
   (let [input-db (first (filter db/db? inputs))
         bounded? (and (number? sample-cost-budget)
                       (Double/isFinite (double sample-cost-budget)))]
     (loop [remaining (when bounded?
                        (max 0.0 (double sample-cost-budget)))
            todo      (seq plans)
            prepared  (transient [])]
       (if-some [plan (first todo)]
         (let [plan-db (or (:access-source plan)
                           (get-in plan [:path :options :db])
                           input-db)
               plan
               (if plan-db
                 (if-let [plan
                          (or (when (:step plan) plan)
                              (schedule-correlated-access
                                plan-db parsed-q inputs plan))]
                   (let [plan (assoc
                                plan
                                :sample-cost-budget remaining
                                :joins
                                (order-access-joins plan-db parsed-q plan)
                                :join-candidates
                                (access-join-candidates plan-db parsed-q plan))]
                     (if (:correlated? plan)
                       plan
                       (adjust-access-plan plan-db parsed-q inputs plan)))
                   (assoc plan :unavailable? true))
                 plan)
               sample-cost
               (double (or (get-in plan [:estimate :planning-sample-cost])
                           0.0))
               remaining
               (when bounded? (max 0.0 (- (double remaining) sample-cost)))]
           (recur remaining (next todo) (conj! prepared plan)))
         (persistent! prepared))))))
