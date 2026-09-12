;;
;; Copyright (c) Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns ^:no-doc datalevin.query.optimizer.plan-build
  "Join-plan construction, dynamic-programming enumeration, deferred
  attribute placement, and plan caching."
  (:require
   [clojure.set :as set]
   [datalevin.constants :as c]
   [datalevin.db :as db]
   [datalevin.lmdb :as l]
   [datalevin.parser :as dp]
   [datalevin.query-util :as qu]
   [datalevin.query.optimizer.estimates
    :refer [count-init-follows count-init-follows-summary-cached
            count-or-join-follows estimate-hash-join-cost estimate-link-cost
            estimate-round final-plan-size incoming-link-counts recount-node]]
   [datalevin.query.optimizer.graph :as qog]
   [datalevin.query.optimizer.plan-cost
    :refer [estimate-base-cost estimate-scan-v-cost estimate-scan-v-size
            fast-clause-count final-plan-cost zero-count-clause-size]]
   [datalevin.query.optimizer.range :as qor]
   [datalevin.query.optimizer.rewrite
    :refer [get-not-join-source get-not-join-vars]]
   [datalevin.query.optimizer.sampling
    :refer [-sample]]
   [datalevin.query.plan :as qplan]
   [datalevin.query.predicate :as qpred]
   [datalevin.query.resolve :as qresolve]
   [datalevin.util :as u
    :refer [concatv map+]])
  (:import
   [java.util HashMap HashSet IdentityHashMap List]
   [java.util.concurrent ConcurrentHashMap]
   [datalevin.db DB]
   [datalevin.storage Store]
   [datalevin.utl DPKey LRUCache]
   [org.eclipse.collections.impl.list.mutable FastList]))

(declare aid find-index plans)

(defonce ^:private plan-cache-provider* (atom nil))

(defn set-plan-cache-provider!
  [f]
  (reset! plan-cache-provider* f))

(def ^:private fallback-plan-cache (LRUCache. c/query-result-cache-size))

(def ^:private ^:const ^long max-deferred-attribute-groups
  4)

(def ^:private ^:const ^long max-deferred-attribute-plan-nodes
  10)

(defn- -execute [step db source]
  (qplan/step-execute step db source))

(defn- -type [step]
  (qplan/step-type step))

(defn- map->init-step [m]
  (qplan/map->InitStep m))

(defn- mk-merge-scan-step
  [index attrs-v vars in out cols strata seen-or-joins result sample]
  (qplan/->MergeScanStep index attrs-v vars in out cols strata seen-or-joins
                         result sample))

(defn- mk-link-step [type index attr var fidx in out cols strata seen-or-joins]
  (qplan/->LinkStep type index attr var fidx in out cols strata
                    seen-or-joins))

(defn- mk-identity-step [in out cols strata seen-or-joins]
  (qplan/->IdentityStep in out cols strata seen-or-joins))

(defn- mk-hash-join-step
  [link link-e in out in-cols cols strata seen-or-joins tgt-steps in-size tgt-size]
  (qplan/->HashJoinStep link link-e in out in-cols cols strata seen-or-joins
                        tgt-steps in-size tgt-size))

(defn- mk-semi-join-step
  [in out in-cols cols strata seen-or-joins join-steps]
  (qplan/->SemiJoinStep in out in-cols cols strata seen-or-joins join-steps))

(defn- mk-or-join-step
  [clause bound-var bound-idx free-vars tgt tgt-attr sources rules in out cols strata seen-or-joins]
  (qplan/->OrJoinStep clause bound-var bound-idx free-vars tgt tgt-attr
                      sources rules in out cols strata seen-or-joins))

(defn- mk-not-join-step
  [clause vars sources rules in out cols strata seen-or-joins]
  (qplan/->NotJoinStep clause vars sources rules in out cols strata
                       seen-or-joins))

(defn- make-plan [steps cost size recency]
  (qplan/->Plan steps cost size recency))

(defn- plan-cache ^LRUCache []
  (if-let [f @plan-cache-provider*] (f) fallback-plan-cache))

(def ^:private add-pred qor/add-pred)

(def ^:private range->inequality qor/range->inequality)

(defn- activate-var-pred
  [var clause]
  (qor/activate-var-pred {:make-call qresolve/make-call
                          :resolve-pred qresolve/resolve-pred}
                         var clause))

(defn- attr-var [{:keys [var]}] (or var '_))

(defn- count-node-datoms
  [^DB db {:keys [free bound] :as node}]
  (reduce
    (fn [{:keys [mcount] :as node} [k i clause]]
      (let [c (fast-clause-count db nil clause (long mcount))
            c (if (zero? c) (zero-count-clause-size db nil clause) c)]
        (cond
          (zero? c)          (reduced (assoc node :mcount 0))
          (< c ^long mcount) (-> node
                                 (assoc-in [k i :count] c)
                                 (assoc :mcount c :mpath [k i]))
          :else              (assoc-in node [k i :count] c))))
    (assoc node :mcount Long/MAX_VALUE)
    (let [flat (fn [k m] (map-indexed (fn [i clause] [k i clause]) m))]
      (concat (flat :bound bound) (flat :free free)))))

(defn- count-known-e-datoms
  [db e {:keys [bound free] :as node}]
  (reduce
    (fn [{:keys [mcount] :as node} [k i clause]]
      (let [c (fast-clause-count db e clause (long mcount))
            c (if (zero? c) (zero-count-clause-size db e clause) c)]
        (cond
          (zero? c)          (reduced (assoc node :mcount 0))
          (< c ^long mcount) (-> node
                                 (assoc-in [k i :count] c)
                                 (assoc :mcount c :mpath [k i]))
          :else              (assoc-in node [k i :count] c))))
    (assoc node :mcount Long/MAX_VALUE)
    (let [flat (fn [k clauses]
                 (map-indexed (fn [i clause] [k i clause]) clauses))]
      (concat (flat :bound bound) (flat :free free)))))

(defn- count-datoms
  [db e node]
  (unreduced (if (int? e)
               (count-known-e-datoms db e node)
               (count-node-datoms db node))))

(defn- add-back-range
  [v {:keys [pred range]}]
  (if range
    (let [range-pred
          (reduce
            (fn [p r]
              (if r
                (add-pred p (activate-var-pred v (range->inequality v r)) true)
                p))
            nil range)]
      (add-pred pred range-pred))
    pred))

(defn- simple-range-pred?
  "Whether add-back-range produces only one index-derived range predicate.
  Keep residual predicates and disjoint ranges on the ordinary predicate cost
  path: both can do materially more work per scanned entity."
  [{:keys [pred range]}]
  (and (nil? pred) (vector? range) (= 1 (count range))))

(defn merge-pred-options
  [v clause]
  (let [pred (add-back-range v clause)]
    (cond-> {:pred pred}
      (and pred (simple-range-pred? clause)) (assoc :range-pred? true))))

(defn- attrs-vec
  [attrs pred-options skips fidxs]
  (mapv (fn [a options f]
          [a (cond-> (assoc options :skip? false :fidx nil)
               (skips a) (assoc :skip? true)
               f         (assoc :fidx f :skip? true))])
        attrs pred-options fidxs))

(defn- aid [db] #(((db/-schema db) %) :db/aid))

(defn- predicate-only-clause?
  [materialized-vars {:keys [var pred range]}]
  (and (some? materialized-vars)
       (qu/binding-var? var)
       (not (qu/placeholder? var))
       (not (contains? materialized-vars var))
       (or pred range)))

(defn- init-steps
  [db e node single? sample? materialized-vars]
  (let [{:keys [bound free mpath mcount]}            node
        {:keys [attr var val range pred] :as clause} (get-in node mpath)

        know-e? (int? e)
        no-var? (or (not var) (qu/placeholder? var))
        pred    (if (and know-e? var)
                  (add-back-range var clause)
                  pred)

        init (cond-> (map->init-step
                       {:attr attr :vars [e] :out [e]
                        :mcount (:count clause)})
               var     (assoc :pred pred
                              :vars (cond-> [e]
                                      (not no-var?) (conj var))
                              :range range)
               (predicate-only-clause? materialized-vars clause)
               (assoc :predicate-only? true)
               (some? val) (assoc :val val)
               know-e? (assoc :know-e? true)
               true    (#(let [vars (:vars %)]
                           (assoc % :cols (if (= 1 (count vars))
                                            [e]
                                            [e #{attr var}])
                                  :strata [(set vars)]
                                  :seen-or-joins #{})))

               (and (not single?) sample?)
               (#(if (< ^long c/init-exec-size-threshold ^long mcount)
                   (assoc % :sample (-sample % db nil))
                   (assoc % :result (-execute % db nil)))))]
    (cond-> [init]
      (< 1 (+ (count bound) (count free)))
      (conj
        (let [[k i]   mpath
              bound1  (mapv (fn [{:keys [val] :as b}]
                              (-> b
                                  (update :pred add-pred
                                          (qpred/shareable-predicate
                                            #(= val %)))
                                  (assoc :var (gensym "?bound"))))
                            (if (= k :bound) (u/vec-remove bound i) bound))
              all     (->> (concatv bound1
                                    (if (= k :free) (u/vec-remove free i) free))
                           (sort-by (fn [{:keys [attr]}] ((aid db) attr))))
              attrs   (mapv :attr all)
              vars    (mapv attr-var all)
              skips   (cond-> (set (sequence
                                     (comp (map (fn [a v]
                                               (when (or (= v '_)
                                                         (qu/placeholder? v))
                                                 a)))
                                        (remove nil?))
                                     attrs vars))
                        no-var? (conj attr))
              pred-options
              (mapv (fn [v clause]
                      (cond-> (merge-pred-options v clause)
                        (predicate-only-clause?
                          materialized-vars clause)
                        (assoc :predicate-only? true)))
                    vars all)
              attrs-v      (attrs-vec attrs pred-options skips (repeat nil))
              cols         (into (:cols init)
                                 (sequence
                                   (comp
                                     (map (fn [a v]
                                            (when-not (skips a) #{a v})))
                                     (remove nil?))
                                   attrs vars))
              strata       (conj (:strata init) (set vars))
              ires         (:result init)
              isp          (:sample init)
              step         (mk-merge-scan-step
                             0 attrs-v vars [e] [e] cols strata #{} nil nil)]
          (cond-> step
            ires (assoc :result (-execute step db ires))
            isp  (assoc :sample (-sample step db isp))))))))

(defn- base-plan
  ([db nodes e]
   (base-plan db nodes e false true))
  ([db nodes e single?]
   (base-plan db nodes e single? true))
  ([db nodes e single? sample?]
   (base-plan db nodes e single? sample? nil))
  ([db nodes e single? sample? materialized-vars]
   (let [node   (get nodes e)
         mcount (:mcount node)]
     (when (and (:mpath node) (not (zero? ^long mcount)))
       (let [isteps (cond->> (init-steps db e node single? sample?
                                        materialized-vars)
                      (not sample?)
                      (mapv #(assoc % :deferred-projection? true)))]
         (if single?
           (make-plan isteps nil nil 0)
           (make-plan isteps
                      (estimate-base-cost node isteps)
                      (if sample?
                        (estimate-scan-v-size mcount isteps)
                        mcount)
                      0)))))))

(defn writing? [db] (l/writing? (.-lmdb ^Store (.-store ^DB db))))

(defn- update-nodes
  [db nodes]
  (if (= (count nodes) 1)
    (let [[e node] (first nodes)] {e (count-datoms db e node)})
    (let [f (bound-fn [e] [e (count-datoms db e (get nodes e))])]
      (into {} (if (writing? db)
                 (map f (keys nodes))
                 (map+ f (keys nodes)))))))

(defn- component-connection-vars
  [nodes component]
  (into (set component)
        (comp
          (mapcat #(get-in nodes [% :links]))
          (keep :var))
        component))

(defn- plan-materialized-vars
  "Variables whose values must survive their originating index scan. Base
   samples retain every value; this set only marks predicate-local values for
   removal when a base scan is fused into an expansion."
  [nodes required-vars]
  (let [clauses      (mapcat #(concat (:bound %) (:free %)) (vals nodes))
        occurrences (frequencies
                      (keep (fn [{:keys [var]}]
                              (when (qu/binding-var? var) var))
                            clauses))]
    (reduce-kv
      (fn [vars e {:keys [links]}]
        (cond-> (into vars (qu/collect-vars links))
          (qu/binding-var? e) (conj e)))
      (into (set required-vars)
            (keep (fn [[var n]] (when (< 1 ^long n) var)))
            occurrences)
      nodes)))

(defn- movable-attribute-clause?
  [db connection-vars {:keys [attr var val]}]
  (and (qu/binding-var? var)
       (not (qu/placeholder? var))
       (not (contains? connection-vars var))
       (nil? val)
       (not= :db.cardinality/many
             (get-in (db/-schema db) [attr :db/cardinality]))))

(defn- attribute-group-role
  [{:keys [range pred]}]
  (if (or range pred) :local-filter :projection))

(defn- attribute-clause-selectivity
  [db {:keys [attr range] :as clause}]
  (if range
    (let [total   (long (fast-clause-count db nil {:attr attr}
                                           Long/MAX_VALUE))
          matched (long (fast-clause-count db nil clause Long/MAX_VALUE))]
      (if (pos? total)
        (max (double c/magic-scan-ratio)
             (min 1.0 (/ (double matched) (double total))))
        0.0))
    ;; A projection can still reject an entity missing the property, and a
    ;; residual predicate can be selective. Without a representative sample,
    ;; treating either as a reducer would make deferral look artificially
    ;; attractive. The complete-plan comparison therefore prices it as
    ;; cardinality preserving.
    1.0))

(defn- classify-attribute-groups
  "Split movable cardinality-one EAV clauses from their entity nodes. Groups
  are dependency nodes in the join DP, so their placement is costed against
  the complete component rather than decided at the originating scan."
  ([db nodes component]
   (classify-attribute-groups db nodes component #{}))
  ([db nodes component already-late-attrs]
   (when (< 1 (count component))
     (let [connection-vars (component-connection-vars nodes component)
           candidates
           (into {}
                 (keep
                   (fn [e]
                     (let [{:keys [free mcount]} (get nodes e)
                           clauses
                           (filterv #(and (not (contains? already-late-attrs
                                                         (:attr %)))
                                          (movable-attribute-clause?
                                            db connection-vars %))
                                    free)]
                       (when (and (not (int? e))
                                  (< (long c/init-exec-size-threshold)
                                     (long mcount))
                                  (seq clauses))
                         [e clauses]))))
                 component)
           raw-groups
           (for [[e clauses] (sort-by (comp str key) candidates)
                 [role grouped] (sort-by
                                  (comp str key)
                                  (group-by attribute-group-role clauses))]
             (let [id [::attribute-group e role]]
               [id {:id id
                    :owner e
                    :role role
                    :clauses (vec (sort-by (comp (aid db) :attr) grouped))
                    :selectivity
                    (reduce * 1.0
                            (map #(attribute-clause-selectivity db %)
                                 grouped))}]))
           groups (into {} raw-groups)
           node-count (+ (count component) (count groups))]
       (when (and (seq groups)
                  (<= (count groups) max-deferred-attribute-groups)
                  (<= node-count max-deferred-attribute-plan-nodes))
         (let [nodes'
               (reduce-kv
                 (fn [nodes e clauses]
                   (let [deferred (set clauses)]
                     (update nodes e
                             (fn [node]
                               (-> node
                                   (update :free
                                           (fn [free]
                                             (filterv
                                               #(not (contains? deferred %))
                                               free)))
                                   recount-node)))))
                 nodes candidates)]
           {:nodes nodes'
            :groups groups
            :component (into (vec component) (map first raw-groups))}))))))

(defn- selective-anchor-node?
  [{:keys [bound mcount]}]
  (and (= 1 (long mcount)) (seq bound)))

(defn- projection-only-clause?
  [db projected-vars connection-vars {:keys [attr var val range pred]}]
  (and (qu/binding-var? var)
       (not (qu/placeholder? var))
       (contains? projected-vars var)
       (not (contains? connection-vars var))
       (nil? val)
       (nil? range)
       (nil? pred)
       (not= :db.cardinality/many
             (get-in (db/-schema db) [attr :db/cardinality]))))

(defn- deferred-base-sample-entities
  "Return unbound, cardinality-one projection nodes whose global root sample
   cannot compete with an exact one-row anchor in the same connected
   component. Their catalog count remains available to join enumeration; only
   the speculative 1,000-row value sample is deferred."
  [db nodes component projected-vars]
  (if (some #(selective-anchor-node? (get nodes %)) component)
    (let [connection-vars (component-connection-vars nodes component)]
      (into #{}
            (filter
              (fn [e]
                (let [{:keys [bound free mcount]} (get nodes e)]
                  (and (empty? bound)
                       (< (long c/init-exec-size-threshold) (long mcount))
                       (seq free)
                       (every? #(projection-only-clause?
                                  db projected-vars connection-vars %)
                               free)))))
            component))
    #{}))

(defn- build-base-plans
  [db nodes component deferred materialized-vars]
  (let [f (bound-fn [e]
            (when-let [plan (base-plan db nodes e false
                                      (not (contains? deferred e))
                                      materialized-vars)]
              [[e] plan]))]
    (into {} (if (writing? db)
               (keep f component)
               (keep identity (map+ f component))))))

(def find-index qplan/find-index)

(defn- proven-eav-clause?
  [step entity-idx attr value-idx]
  (= value-idx (get-in step [:eav-provenance entity-idx attr])))

(defn- merge-scan-step
  [db last-step index new-key new-steps]
  (let [in               (:out last-step)
        out              (if (set? in) (set new-key) new-key)
        lcols            (:cols last-step)
        lstrata          (:strata last-step)
        ncols            (:cols (peek new-steps))
        [s1 s2]          new-steps
        val1             (:val s1)
        [_ v1]           (:vars s1)
        logical-emit-v1? (and v1 (some? (find-index v1 ncols)))
        predicate-v1?    (and logical-emit-v1? (:predicate-only? s1))
        emit-v1?         (and logical-emit-v1? (not predicate-v1?))
        a1               (:attr s1)
        ip-options       (merge-pred-options v1 s1)
        ip               (cond-> (:pred ip-options)
                           (some? val1)
                           (add-pred
                             (qpred/shareable-predicate #(= % val1))))
        ip-options       (cond-> (assoc ip-options :pred ip)
                           (some? val1) (dissoc :range-pred?))
        attrs-v2         (:attrs-v s2)
        get-a            (fn [coll] (some #(when (keyword? %) %) coll))
        [raw-attrs-v vars cols elided predicate-vars]
        (reduce
          (fn [[attrs-v vars cols elided predicate-vars] col]
            (let [v (some #(when (symbol? %) %) col)]
              (if (and ip (= v v1))
                [attrs-v vars cols elided predicate-vars]
                (let [a            (get-a col)
                      attr-options
                      (or (some (fn [[attr options]]
                                  (when (and (= a attr) (:pred options))
                                    options))
                                attrs-v2)
                          (some (fn [[attr options]]
                                  (when (= a attr) options))
                                attrs-v2))
                      options      (if attr-options
                                     (select-keys
                                       attr-options
                                       [:pred :range-pred?
                                        :predicate-only?])
                                     {:pred nil})
                      skip?        (boolean
                                     (some (fn [[attr options]]
                                             (when (= a attr)
                                               (:skip? options)))
                                           attrs-v2))
                      predicate?   (boolean (:predicate-only? options))]
                  (if-let [f (find-index v lcols)]
                    (if (and (nil? (:pred options))
                             (proven-eav-clause? last-step index a f))
                      [attrs-v vars cols
                       (conj elided
                             [a (assoc options :skip? true :fidx f)])
                       predicate-vars]
                      [(conj attrs-v
                             [a (assoc options :skip? true :fidx f)])
                       vars cols elided predicate-vars])
                    (if predicate?
                      [(conj attrs-v
                             [a (assoc options :skip? true :fidx nil)])
                       vars cols elided (conj predicate-vars v)]
                      [(conj attrs-v
                             [a (assoc options
                                       :skip? skip?
                                       :fidx nil)])
                       (conj vars v) (conj cols col) elided
                       predicate-vars]))))))
          (if (or ip (nil? v1))
            [[[a1 (assoc ip-options
                         :skip? (not emit-v1?)
                         :fidx nil)]]
             (if emit-v1? [v1] [])
             (if emit-v1? [#{a1 v1}] [])
             []
             (if predicate-v1? [v1] [])]
            [[] [] [] [] []])
          (rest ncols))
        ;; MergeScanStep requires at least one physical attribute. Retain one
        ;; otherwise redundant check for degenerate duplicate-clause queries.
        [scan-attrs-v remaining-elided]
        (if (and (empty? raw-attrs-v) (seq elided))
          [[(first elided)] (vec (rest elided))]
          [raw-attrs-v elided])
        elided-attrs     (mapv first remaining-elided)
        cost-attrs-v     (when (seq remaining-elided)
                           (into scan-attrs-v remaining-elided))
        ;; The tuple-dependent equality check disabled the entity cache. Keep
        ;; that allocation-free mode after provenance makes the check itself
        ;; redundant; linked expansion inputs are normally unique by entity.
        attrs-v          (if (seq remaining-elided)
                           (mapv (fn [[attr options]]
                                   [attr (assoc options :cache-eids? false)])
                                 scan-attrs-v)
                           scan-attrs-v)
        fcols            (into lcols (sort-by (comp (aid db) get-a) cols))
        strata           (conj lstrata (set vars))
        lseen            (:seen-or-joins last-step)
        step             (mk-merge-scan-step index attrs-v vars in out fcols
                                             strata lseen nil nil)]
    (cond-> step
      (seq remaining-elided)
      (assoc :elided-eav-attrs elided-attrs
             :cost-attrs-v cost-attrs-v)

      (seq predicate-vars)
      (assoc :predicate-only-vars predicate-vars
             :cost-var-count (+ (count vars) (count predicate-vars))))))

(defn- index-by-link
  [cols link-e link]
  (case (:type link)
    :ref     (or (find-index (:tgt link) cols)
                 (find-index (:attr link) cols))
    :_ref    (find-index link-e cols)
    :val-eq  (or (find-index (:var link) cols)
                 (find-index ((:attrs link) link-e) cols))
    ;; For or-join, return index where tgt will be after step adds free-vars
    ;; and tgt
    :or-join (+ (count cols) (count (:free-vars link)))))

(defn- enrich-cols
  [cols index attr]
  (let [pa (cols index)]
    (mapv (fn [e] (if (and (= e pa) (set? e)) (conj e attr) e)) cols)))

(defn- col-var
  [col]
  (if (set? col)
    (some #(when (symbol? %) %) col)
    col))

(defn- col-attr
  [col]
  (when (set? col)
    (some #(when (keyword? %) %) col)))

(defn multi-key-result-size
  "Dampen a join estimate for equality keys already present on both sides but
  not represented by the selected graph link. Full independence is too
  aggressive for graph data, so each additional key contributes only the
  square root of its observed value-domain cardinality."
  [db link-e link prev-plan new-base-plan result-size]
  (let [result-size (long result-size)
        in-cols     (:cols (peek (:steps prev-plan)))
        tgt-cols    (some-> new-base-plan :steps peek :cols)
        primary-var (case (:type link)
                      :val-eq (:var link)
                      :_ref   link-e
                      :ref    (:tgt link)
                      nil)
        in-vars     (into #{} (keep col-var) in-cols)
        extra-cols  (filter (fn [col]
                              (let [v (col-var col)]
                                (and v
                                     (not= v primary-var)
                                     (contains? in-vars v)
                                     (col-attr col))))
                            tgt-cols)
        divisor     (double
                      (reduce
                        (fn ^double [^double d col]
                          (let [cardinality
                                (long (db/-cardinality db (col-attr col)))]
                            (* d (Math/sqrt
                                   (double (max 1 cardinality))))))
                        (double 1.0) extra-cols))]
    (if (> divisor 1.0)
      (max 1 (estimate-round (/ (double result-size) divisor)))
      result-size)))

(defn- merge-join-cols
  "Merge input and target cols for hash join output, preserving input order.
   Returns [merged-cols new-vars]."
  [in-cols tgt-cols]
  (let [^HashMap tgt-map    (HashMap.)
        ^HashSet in-var-set (HashSet.)]
    (doseq [col tgt-cols]
      (.put tgt-map (col-var col) col))
    (let [merged-in
          (mapv (fn [col]
                  (let [v (col-var col)]
                    (.add in-var-set v)
                    (if (.containsKey tgt-map v)
                      (let [tcol (.get tgt-map v)]
                        (cond
                          (set? col)  (if (set? tcol) (into col tcol) col)
                          (set? tcol) tcol
                          :else       v))
                      col)))
                in-cols)
          [new-cols new-vars]
          (loop [i        0
                 new-cols (transient [])
                 new-vars (transient #{})]
            (if (< i (count tgt-cols))
              (let [col (nth tgt-cols i)
                    v   (col-var col)]
                (if (.contains in-var-set v)
                  (recur (u/long-inc i) new-cols new-vars)
                  (recur (u/long-inc i) (conj! new-cols col)
                         (conj! new-vars v))))
              [(persistent! new-cols) (persistent! new-vars)]))]
      [(into merged-in new-cols) new-vars])))

(defn- required-new-join-var?
  [required-vars in-cols tgt-cols]
  (let [^HashSet in-vars (HashSet.)]
    (doseq [col in-cols]
      (.add in-vars (col-var col)))
    (boolean
      (some (fn [col]
              (let [v (col-var col)]
                (and (required-vars v) (not (.contains in-vars v)))))
            tgt-cols))))

(defn- link-step
  [type last-step index attr tgt new-key]
  (let [in      (:out last-step)
        out     (if (set? in) (set new-key) new-key)
        lcols   (:cols last-step)
        lstrata (:strata last-step)
        lseen   (:seen-or-joins last-step)
        fidx    (find-index tgt lcols)
        cols    (cond-> (enrich-cols lcols index attr)
                  (nil? fidx) (conj tgt))
        step    (mk-link-step type index attr tgt fidx in out cols
                              (conj lstrata #{tgt}) lseen)
        tgt-idx (find-index tgt cols)
        step    (cond-> step
                  (and (#{:_ref :val-eq} type) (some? tgt-idx))
                  (assoc :eav-provenance {tgt-idx {attr index}}))]
    [step
     (or fidx (dec (count cols)))]))

(defn- identity-link-step
  [last-step new-key]
  (let [in   (:out last-step)
        out  (if (set? in) (set new-key) new-key)]
    (mk-identity-step in out (:cols last-step) (:strata last-step)
                      (:seen-or-joins last-step))))

(defn- rev-ref-plan
  [db last-step index {:keys [type attr tgt]} new-key new-steps]
  (let [[step n-index] (link-step type last-step index attr tgt new-key)]
    (if (<= (count new-steps) 1)
      [step]
      [step (merge-scan-step db step n-index new-key new-steps)])))

(defn- val-eq-plan
  [db last-step index {:keys [type attrs tgt]} new-key new-steps]
  (let [attr           (attrs tgt)
        [step n-index] (link-step type last-step index attr tgt new-key)]
    (if (<= (count new-steps) 1)
      [step]
      [step (merge-scan-step db step n-index new-key new-steps)])))

(defn- hash-join-plan
  [_db {:keys [steps cost size]} link-e link new-key
   new-base-plan result-size]
  (let [last-step       (peek steps)
        in              (:out last-step)
        out             (if (set? in) (set new-key) new-key)
        lcols           (:cols last-step)
        lstrata         (:strata last-step)
        lseen           (:seen-or-joins last-step)
        tgt-steps       (:steps new-base-plan)
        in-size         (or size 0)
        tgt-size        (or (:size new-base-plan) 0)
        tgt-cols        (:cols (peek tgt-steps))
        [cols new-vars] (merge-join-cols lcols tgt-cols)
        step            (mk-hash-join-step link link-e in out lcols cols
                                           (conj lstrata new-vars) lseen
                                           tgt-steps in-size tgt-size)
        base-cost       (or (:cost new-base-plan) 0)
        join-cost       (estimate-hash-join-cost
                          in-size tgt-size result-size (count cols))]
    (make-plan [step]
               (+ ^long cost ^long base-cost ^long join-cost)
               result-size
               (- ^long (find-index link-e (:strata last-step))))))

(defn- semi-join-eligible?
  [nodes incoming-link-counts required-vars prev-plan link-e new-e link
   new-base-plan]
  (when (and new-base-plan
             (#{:ref :_ref :val-eq} (:type link))
             (= 1 (count (get-in nodes [new-e :links])))
             (= link-e (get-in nodes [new-e :links 0 :tgt]))
             (= 1 (long (get incoming-link-counts new-e 0))))
    (let [in-cols  (:cols (peek (:steps prev-plan)))
          tgt-cols (:cols (peek (:steps new-base-plan)))]
      (not (required-new-join-var? required-vars in-cols tgt-cols)))))

(defn- or-join-plan*
  [db sources rules last-step
   {:keys [clause bound-var free-vars tgt tgt-attr]} new-key new-base]
  (let [in        (:out last-step)
        out       (if (set? in) (set new-key) new-key)
        lcols     (:cols last-step)
        lstrata   (:strata last-step)
        lseen     (:seen-or-joins last-step)
        bound-idx (find-index bound-var lcols)
        or-cols   (-> lcols (into free-vars) (conj tgt))
        or-seen   (conj lseen clause)
        or-step   (mk-or-join-step clause
                                   bound-var
                                   bound-idx
                                   free-vars
                                   tgt
                                   tgt-attr
                                   sources
                                   rules
                                   in out or-cols
                                   (conj lstrata #{tgt})
                                   or-seen)
        tgt-idx   (dec (count or-cols))
        free-idx  (find-index (first free-vars) or-cols)
        or-step   (cond-> or-step
                    (some? free-idx)
                    (assoc :eav-provenance
                           {tgt-idx {tgt-attr free-idx}}))]
    (if new-base
      (let [new-steps (:steps new-base)]
        [or-step (merge-scan-step db or-step tgt-idx new-key new-steps)])
      [or-step])))

(defn- link-ratio-key
  [link-e {:keys [type attr attrs tgt]}]
  (case type
    :val-eq [type (attrs link-e) (attrs tgt)]
    :_ref   [type attr]
    [type attr]))

(defn- estimate-link-size
  [db link-e {:keys [type attr attrs tgt var]} ^ConcurrentHashMap ratios
   ^IdentityHashMap build-cache prev-size prev-plan index]
  (let [prev-steps              (:steps prev-plan)
        attr                    (or attr (attrs tgt))
        ratio-key               (link-ratio-key link-e {:type  type
                                                        :attr  attr
                                                        :var   var
                                                        :attrs attrs
                                                        :tgt   tgt})
        {:keys [result sample]} (peek prev-steps)
        ^long ssize             (if sample (.size ^List sample) 0)
        ^long rsize             (if result (.size ^List result) 0)]
    (estimate-round
      (cond
        (< 0 ssize)
        (let [{:keys [^long n ^double sum]}
              (count-init-follows-summary-cached
                db build-cache sample attr index)
              mean       (if (pos? n) (/ sum (double n)) 0.0)
              base-ratio (double (db/-default-ratio db attr))
              ratio      (max mean base-ratio
                              (double c/magic-link-ratio))]
          (.put ratios ratio-key ratio)
          (* (double prev-size) ratio))

        (< 0 rsize)
        (let [^long size (count-init-follows db result attr index)
              ratio      (/ size rsize)]
          (.put ratios ratio-key ratio)
          size)

        (.containsKey ratios ratio-key)
        (* ^long prev-size ^double (.get ratios ratio-key))

        :else
        (let [ratio (db/-default-ratio db attr)]
          (.put ratios ratio-key ratio)
          (* ^long prev-size ^double ratio))))))

(defn- estimate-or-join-size
  [db sources rules ^ConcurrentHashMap ratios
   ^IdentityHashMap build-cache prev-plan link]
  (let [prev-size               (:size prev-plan)
        prev-steps              (:steps prev-plan)
        last-step               (peek prev-steps)
        bound-idx               (find-index (:bound-var link) (:cols last-step))
        ratio-key               [:or-join (:bound-var link) (:tgt link)]
        {:keys [result sample]} last-step
        ^long ssize             (if sample (.size ^List sample) 0)
        ^long rsize             (if result (.size ^List result) 0)]
    (estimate-round
      (cond
        (< 0 ssize)
        (let [^long size (count-or-join-follows db sources rules build-cache
                                                sample link bound-idx)
              ratio      (max (double (/ size ssize))
                              ^double c/magic-or-join-ratio)]
          (.put ratios ratio-key ratio)
          (* ^long prev-size ratio))

        (< 0 rsize)
        (let [^long size (count-or-join-follows db sources rules build-cache
                                                result link bound-idx)
              ratio      (/ size rsize)]
          (.put ratios ratio-key ratio)
          size)

        (.containsKey ratios ratio-key)
        (* ^long prev-size ^double (.get ratios ratio-key))

        :else
        (do (.put ratios ratio-key c/magic-or-join-ratio)
            (* ^long prev-size ^double c/magic-or-join-ratio))))))

(defn- estimate-join-size
  [db sources rules link-e link ratios build-cache prev-plan index
   new-base-plan]
  (let [prev-size (:size prev-plan)
        steps     (:steps new-base-plan)]
    (case (:type link)
      :ref     [nil (estimate-scan-v-size prev-size steps)]
      :or-join (let [or-size (estimate-or-join-size db sources rules ratios
                                                    build-cache prev-plan
                                                    link)]
                 ;; or-join doesn't have new-base-plan steps to merge
                 [or-size or-size])
      ;; :_ref and :val-eq
      (let [e-size (estimate-link-size db link-e link ratios build-cache
                                       prev-size prev-plan index)
            result-size (estimate-scan-v-size e-size steps)]
        [e-size (multi-key-result-size db link-e link prev-plan new-base-plan
                                       result-size)]))))

(defn- estimate-e-plan-cost
  [prev-size e-size cur-steps]
  (let [step1 (first cur-steps)]
    (if (= 1 (count cur-steps))
      (case (-type step1)
        :identity 0
        :merge    (estimate-scan-v-cost step1 prev-size)
        (estimate-link-cost prev-size e-size))
      (+ ^long (estimate-link-cost prev-size e-size)
         ^long (estimate-scan-v-cost (peek cur-steps) e-size)))))

(defn- e-plan
  [db {:keys [steps cost size]} index link-e link new-key new-base-plan e-size
   result-size]
  (let [new-steps (:steps new-base-plan)
        last-step (peek steps)
        cur-steps
        (case (:type link)
          :ref    (if (seq new-steps)
                    [(merge-scan-step db last-step index new-key new-steps)]
                    [(identity-link-step last-step new-key)])
          :_ref   (rev-ref-plan db last-step index link new-key new-steps)
          :val-eq (val-eq-plan db last-step index link new-key new-steps))]
    (make-plan cur-steps
               (+ ^long cost ^long (estimate-e-plan-cost size e-size cur-steps))
               result-size
               (- ^long (find-index link-e (:strata last-step))))))

(defn- attribute-group-plan
  [{:keys [steps cost size recency]} new-key
   {:keys [id owner role clauses selectivity]}]
  (let [last-step (peek steps)
        index     (find-index owner (:cols last-step))]
    (when (some? index)
      (let [in     (:out last-step)
            out    (if (set? in) (set new-key) new-key)
            lcols  (:cols last-step)
            [attrs-v vars cols]
            (reduce
              (fn [[attrs-v vars cols] {:keys [attr var] :as clause}]
                (let [options (merge-pred-options var clause)]
                  (if-let [fidx (find-index var lcols)]
                    [(conj attrs-v
                           [attr (assoc options :skip? true :fidx fidx)])
                     vars cols]
                    [(conj attrs-v
                           [attr (assoc options :skip? false :fidx nil)])
                     (conj vars var)
                     (conj cols #{attr var})])))
              [[] [] []] clauses)
            step   (assoc
                     (mk-merge-scan-step
                       index attrs-v vars in out (into lcols cols)
                       (conj (:strata last-step) (set vars))
                       (:seen-or-joins last-step) nil nil)
                     :attribute-group id
                     :attribute-group-owner owner
                     :attribute-group-role role)
            result-size
            (cond
              (zero? (long size)) 0
              (zero? (double selectivity)) 0
              :else (max 1 (estimate-round
                             (* (double size) (double selectivity)))))]
        (make-plan [step]
                   (+ ^long cost ^long (estimate-scan-v-cost step size))
                   result-size recency)))))

(defn- index-semi-join-plan
  [db prev-plan link-e link new-key new-base-plan e-size result-size]
  (let [last-step (peek (:steps prev-plan))
        index     (index-by-link (:cols last-step) link-e link)
        in-size   (:size prev-plan)]
    (when (< ^long in-size ^long result-size)
      (let [join-plan (e-plan db prev-plan index link-e link new-key
                              new-base-plan e-size result-size)
            in        (:out last-step)
            out       (if (set? in) (set new-key) new-key)
            cols      (:cols last-step)
            step      (mk-semi-join-step in out cols cols
                                         (:strata last-step)
                                         (:seen-or-joins last-step)
                                         (:steps join-plan))]
        (make-plan [step]
                   (:cost join-plan)
                   in-size
                   (:recency join-plan))))))

(defn- compare-plans
  "Compare two plans. Prefer lower cost, then lower size as tiebreaker."
  [p1 p2]
  (let [c1 ^long (:cost p1)
        c2 ^long (:cost p2)]
    (if (= c1 c2)
      (if (< ^long (:size p2) ^long (:size p1)) p2 p1)
      (if (< ^long c2 ^long c1) p2 p1))))

(defn- or-join-plan
  [base-plans new-e db sources rules ratios build-cache prev-plan link
   last-step new-key link-e]
  (let [new-base  (base-plans [new-e])
        or-size   (estimate-or-join-size db sources rules ratios build-cache
                                         prev-plan link)
        cur-steps (or-join-plan* db sources rules last-step link new-key
                                 new-base)
        or-cost   (estimate-e-plan-cost (:size prev-plan) or-size cur-steps)]
    (make-plan cur-steps
               (+ ^long (:cost prev-plan) ^long or-cost)
               or-size
               (- ^long (find-index link-e (:strata last-step))))))

(defn- binary-plan*
  [db sources rules base-plans ratios build-cache prev-plan link-e new-e link
   new-key semi-join?]
  (let [last-step (peek (:steps prev-plan))
        index     (index-by-link (:cols last-step) link-e link)
        link-type (:type link)]
    (if (identical? :or-join link-type)
      (or-join-plan base-plans new-e db sources rules ratios build-cache
                    prev-plan link last-step new-key link-e)
      (let [new-base (base-plans [new-e])
            [e-size result-size]
            (estimate-join-size db sources rules link-e link ratios build-cache
                                prev-plan index new-base)
            link-plan  (e-plan db prev-plan index link-e link new-key
                               new-base e-size result-size)
            regular    (if (and (#{:_ref :val-eq} link-type)
                                new-base
                                (<= ^long c/hash-join-min-input-size
                                    ^long (:size prev-plan)))
                         (compare-plans
                           link-plan
                           (hash-join-plan db prev-plan link-e link new-key
                                           new-base result-size))
                         link-plan)
            index-semi (when semi-join?
                         (index-semi-join-plan
                           db prev-plan link-e link new-key new-base e-size
                           result-size))]
        (reduce compare-plans regular
                (cond-> []
                  index-semi (conj index-semi)))))))

(defn- binary-plan
  [db sources rules nodes incoming-link-counts required-vars base-plans ratios
   build-cache prev-plan link-e new-e new-key]
  ;; A nil base plan can mean either a deliberately property-free link target
  ;; or an unsatisfiable node. Only the former may use IdentityStep.
  (when-not (zero? (long (get-in nodes [new-e :mcount])))
    (let [last-step     (peek (:steps prev-plan))
          seen-or-joins (or (:seen-or-joins last-step) #{})
          links         (get-in nodes [link-e :links])
          candidate-key (juxt :recency :cost :size)]
      (reduce
        (fn [best link]
          (if (and (= new-e (:tgt link))
                   (or (not= :or-join (:type link))
                       (not (contains? seen-or-joins (:clause link)))))
            (let [new-base  (base-plans [new-e])
                  semi-join?
                  (semi-join-eligible? nodes incoming-link-counts required-vars
                                       prev-plan link-e new-e link new-base)
                  candidate (binary-plan* db sources rules base-plans ratios
                                          build-cache prev-plan link-e new-e link
                                          new-key semi-join?)]
              (if best
                (u/min-key-comp candidate-key best candidate)
                candidate))
            best))
        nil links))))

(defn- plans
  [db sources rules nodes node-ids incoming-link-counts required-vars pairs
   base-plans groups predecessors prev-plans ratios build-cache]
  (persistent!
    (reduce
      (fn [plans [prev-key prev-plan]]
        (let [prev-key-set (when-not node-ids (set prev-key))]
          (reduce
            (fn [plans [link-e new-e link-id new-id]]
              (if (and
                    (if node-ids
                      (and (.contains ^DPKey prev-key (int link-id))
                           (not (.contains ^DPKey prev-key (int new-id))))
                      (and (prev-key-set link-e) (not (prev-key-set new-e))))
                    (every?
                      (fn [required-e]
                        (if node-ids
                          (.contains ^DPKey prev-key
                                     (int (node-ids required-e)))
                          (prev-key-set required-e)))
                      (get predecessors new-e)))
                (let [plan-key  (if node-ids
                                  (.append ^DPKey prev-key (int new-id))
                                  (conj prev-key new-e))
                      new-key   (if node-ids
                                  (conj (:out (peek (:steps prev-plan))) new-e)
                                  plan-key)
                      cur-plan  (plans plan-key)
                      new-plan
                      (if-let [group (get groups new-e)]
                        (attribute-group-plan prev-plan new-key group)
                        (binary-plan db sources rules nodes incoming-link-counts
                                     required-vars base-plans ratios build-cache
                                     prev-plan link-e new-e new-key))]
                  (if (and new-plan
                           (or (nil? cur-plan)
                               (identical?
                                 new-plan
                                 (compare-plans cur-plan new-plan))))
                    (assoc! plans plan-key new-plan)
                    plans))
                plans))
            plans pairs)))
      (transient {}) prev-plans)))

(def ^:private connected-pairs qog/connected-pairs)

(defn- dp-node-ids
  [component]
  (when (<= (count component) Long/SIZE)
    (into {} (map-indexed (fn [i e] [e i])) component)))

(defn- dp-key
  [node-ids entities ordered?]
  (if ordered?
    (reduce (fn [key e]
              (let [node (int (node-ids e))]
                (if key
                  (.append ^DPKey key node)
                  (DPKey/ordered node))))
            nil entities)
    (DPKey/canonical
      (reduce (fn [^long members e]
                (bit-set members (long (node-ids e))))
              0 entities))))

(defn- dp-initial-plans
  [base-plans node-ids]
  (persistent!
    (reduce-kv (fn [plans [e] plan]
                 (assoc! plans (DPKey/ordered (int (node-ids e))) plan))
               (transient {}) base-plans)))

(defn- dp-pairs
  [pairs node-ids]
  (mapv (fn [[link-e new-e]]
          [link-e new-e (node-ids link-e) (node-ids new-e)])
        pairs))

(defn- shrink-space
  [plans node-ids]
  (persistent!
    (reduce-kv
      (fn [m k ps]
        (assoc! m (if node-ids (DPKey/canonical (long k)) k)
                (-> (peek (apply min-key (fn [p] (:cost (peek p))) ps))
                    (update :steps (fn [ss]
                                     (if (= 1 (count ss))
                                       [(update (first ss) :out set)]
                                       [(first ss)
                                        (update (peek ss) :out set)]))))))
      (transient {})
      (group-by (fn [p]
                  (if node-ids
                    (.members ^DPKey (nth p 0))
                    (set (nth p 0))))
                plans))))

(defn- dp-table-key
  [table node-ids entities]
  (if node-ids
    (dp-key node-ids entities (.isOrdered ^DPKey (ffirst table)))
    entities))

(defn- trace-steps
  [^List tables ^long n-1 node-ids]
  (let [final-plans (vals (.get tables n-1))]
    (reduce
      (fn [plans i]
        (let [table (.get tables i)
              in    (:in (first (:steps (first plans))))]
          (cons (table (dp-table-key table node-ids in)) plans)))
      [(apply min-key :cost final-plans)]
      (range (dec n-1) -1 -1))))

(defn- plan-component
  ([db sources rules nodes incoming-link-counts required-vars component deferred]
   (plan-component db sources rules nodes incoming-link-counts required-vars
                   component deferred {} component nil))
  ([db sources rules nodes incoming-link-counts required-vars component deferred
    groups dp-component]
   (plan-component db sources rules nodes incoming-link-counts required-vars
                   component deferred groups dp-component nil))
  ([db sources rules nodes incoming-link-counts required-vars component deferred
    groups dp-component entity-order]
   (let [n                 (count dp-component)
         materialized-vars (plan-materialized-vars nodes required-vars)]
     (if (= n 1)
       [(base-plan db nodes (first component) true true materialized-vars)]
       (let [base-plans (build-base-plans db nodes component deferred
                                          materialized-vars)
             node-ids   (dp-node-ids dp-component)]
         (if (empty? base-plans)
           [nil]
           (let [raw-pairs     (into (vec (connected-pairs nodes component))
                                     (map (fn [[id {:keys [owner]}]]
                                            [owner id]))
                                     groups)
                 pairs         (if node-ids
                                 (dp-pairs raw-pairs node-ids)
                                 raw-pairs)
                 predecessors  (when (seq entity-order)
                                 (into {}
                                       (map-indexed
                                         (fn [i e]
                                           [e (set (take i entity-order))]))
                                       entity-order))
                 initial-bases (if (seq entity-order)
                                  (select-keys base-plans
                                               [[(first entity-order)]])
                                  base-plans)
                 initial-plans (if node-ids
                                 (dp-initial-plans initial-bases node-ids)
                                 initial-bases)
                 tables        (FastList. n)
                 ratios        (ConcurrentHashMap.)
                 build-cache   (IdentityHashMap.)
                 n-1           (dec n)
                 pn            ^long (min (long c/plan-search-max)
                                          (long (u/n-permutations n 2)))]
             (.add tables initial-plans)
             (dotimes [i n-1]
               (let [plans (plans db sources rules nodes node-ids
                                  incoming-link-counts required-vars pairs
                                  base-plans groups predecessors
                                  (.get tables i) ratios build-cache)]
                 (if (< pn (count plans))
                   (.add tables (shrink-space plans node-ids))
                   (.add tables plans))))
             (if (empty? (.get tables n-1))
               [nil]
               (trace-steps tables n-1 node-ids)))))))))

(def ^:private connected-components qog/connected-components)

(defn- attribute-group-placements
  [plan-trace]
  (loop [plans plan-trace
         step-index (long 0)
         hash-count (long 0)
         placements {}]
    (if-let [{:keys [steps size]} (first plans)]
      (let [[step-index hash-count placements]
            (reduce
              (fn [[step-index hash-count placements] step]
                [(u/long-inc (long step-index))
                 (if (= :hash-join (-type step))
                   (u/long-inc (long hash-count))
                   (long hash-count))
                 (if-let [id (:attribute-group step)]
                   (assoc placements id
                          {:step-index step-index
                           :after-hash-join? (pos? (long hash-count))
                           :estimated-output size})
                   placements)])
              [step-index hash-count placements] steps)]
        (recur (next plans) (long step-index) (long hash-count) placements))
      placements)))

(defn- hash-join-plan-trace?
  [plan-trace]
  (boolean (some #(= :hash-join (-type %))
                 (mapcat :steps plan-trace))))

(defn- post-hash-merge-attrs
  [plan-trace]
  (loop [steps (seq (mapcat :steps plan-trace))
         after-hash? false
         attrs #{}]
    (if-let [step (first steps)]
      (let [step-type (-type step)]
        (recur (next steps)
               (or after-hash? (= :hash-join step-type))
               (if (and after-hash? (= :merge step-type))
                 (into attrs (map first) (:attrs-v step))
                 attrs)))
      attrs)))

(defn- plan-entity-order
  [plan-trace component]
  (let [entities (set component)
        order
        (reduce
          (fn [order {:keys [steps]}]
            (let [out   (set (:out (peek steps)))
                  seen  (set order)
                  added (sort-by str (set/difference
                                       (set/intersection entities out)
                                       seen))]
              (into order added)))
          [] plan-trace)]
    (if (= (count order) (count component)) order (vec component))))

(defn- attribute-group-potential-savings
  "Estimate an optimistic bound on work removable from existing fused merge
  scans. Late group scans are deliberately priced at zero here; if even this
  bound is immaterial, expanding the placement state space cannot pay."
  [plan-trace groups]
  (let [candidate-attrs (into #{} (mapcat #(map :attr (:clauses %)))
                              (vals groups))
        candidate-vars  (into #{} (mapcat #(map :var (:clauses %)))
                              (vals groups))]
    (loop [plans plan-trace
           previous-size (long 0)
           seen #{}
           savings (double 0.0)]
      (if-let [{:keys [steps size]} (first plans)]
        (let [input-size (max previous-size (long (or size 0)))
              [seen savings]
              (reduce
                (fn [[seen savings] step]
                  (if (= :merge (-type step))
                    (let [attrs-v       (:attrs-v step)
                          movable       (into #{}
                                              (comp (map first)
                                                    (filter candidate-attrs))
                                              attrs-v)
                          remaining     (filterv
                                          #(not (contains? movable (first %)))
                                          attrs-v)
                          remaining-vars
                          (filterv #(not (contains? candidate-vars %))
                                   (:vars step))
                          full-cost     (double
                                          (estimate-scan-v-cost step input-size))
                          remaining-cost
                          (if (seq remaining)
                            (double
                              (estimate-scan-v-cost
                                (assoc step :attrs-v remaining
                                            :vars remaining-vars)
                                input-size))
                            0.0)]
                      [(into seen movable)
                       (+ (double savings)
                          (max 0.0 (- full-cost remaining-cost)))])
                    [seen savings]))
                [seen savings] steps)]
          (recur (next plans) (long (or size previous-size)) seen
                 (double savings)))
        (if (= seen candidate-attrs)
          savings
          Double/POSITIVE_INFINITY)))))

(defn- unplanned-attribute-group-summary
  [groups]
  (mapv
    (fn [[id {:keys [owner role clauses selectivity]}]]
      {:id id
       :owner owner
       :role role
       :attrs (mapv :attr clauses)
       :vars (mapv :var clauses)
       :estimated-selectivity selectivity})
    (sort-by (comp str key) groups)))

(defn- component-plan-alternatives
  [db sources rules nodes incoming-counts required-vars component deferred]
  (let [eager (plan-component db sources rules nodes incoming-counts
                              required-vars component deferred)
        classified (when (hash-join-plan-trace? eager)
                     (classify-attribute-groups
                       db nodes component (post-hash-merge-attrs eager)))]
    (if-not classified
      {:plan eager}
      (let [eager-valid? (not (some nil? eager))
            eager-cost   (when eager-valid? (final-plan-cost eager))
            potential    (attribute-group-potential-savings
                           eager (:groups classified))
            min-saving   (* (double eager-cost)
                            (double c/deferred-eav-min-cost-improvement))]
        (if (< (double potential) min-saving)
          {:plan eager
           :attribute-group-planning
           {:groups (unplanned-attribute-group-summary (:groups classified))
            :alternatives
            [{:kind :eager
              :cost eager-cost
              :size (final-plan-size eager)}
             {:kind :deferred
              :planned? false
              :optimistic-cost (max 0.0 (- (double eager-cost)
                                            (double potential)))
              :potential-savings potential}]
            :selected :eager
            :reason :insufficient-potential-savings}}
          (let [grouped (plan-component
                          db sources rules (:nodes classified) incoming-counts
                          required-vars component deferred (:groups classified)
                          (:component classified)
                          (plan-entity-order eager component))
                grouped-placements (attribute-group-placements grouped)
                grouped-valid? (and (not (some nil? grouped))
                                    (some :after-hash-join?
                                          (vals grouped-placements)))
                grouped-cost   (when grouped-valid?
                                 (final-plan-cost grouped))
                selected-kind  (if (and grouped-valid?
                                        (or (not eager-valid?)
                                            (< (double grouped-cost)
                                               (- (double eager-cost)
                                                  min-saving))))
                                 :deferred
                                 :eager)
                selected       (if (= :deferred selected-kind) grouped eager)
                placements     (when (= :deferred selected-kind)
                                 grouped-placements)
                group-summary
                (mapv
                  (fn [[id {:keys [owner role clauses selectivity]}]]
                    (merge {:id id
                            :owner owner
                            :role role
                            :attrs (mapv :attr clauses)
                            :vars (mapv :var clauses)
                            :estimated-selectivity selectivity}
                           (get placements id)))
                  (sort-by (comp str key) (:groups classified)))]
            {:plan selected
             :attribute-group-planning
             {:groups group-summary
              :alternatives
              (cond-> [{:kind :eager
                        :cost eager-cost
                        :size (when eager-valid? (final-plan-size eager))}]
                grouped-valid?
                (conj {:kind :deferred
                       :cost grouped-cost
                       :size (final-plan-size grouped)}))
              :selected selected-kind}}))))))

(defn- target-plan-scans-attr?
  [steps attr]
  (boolean
    (some
      (fn [step]
        (case (-type step)
          :init  (= attr (:attr step))
          :merge (boolean (some #(= attr (first %)) (:attrs-v step)))
          false))
      steps)))

(defn- reusable-domain-sip-candidate?
  [database or-step hash-step domain-var target-attr]
  (and (identical? (get-in hash-step [:link :type]) :val-eq)
       (= domain-var (first (:free-vars or-step)))
       (some? (find-index domain-var (:in-cols hash-step)))
       target-attr
       (= :db.type/ref
          (get-in (db/-schema database) [target-attr :db/valueType]))
       (<= (long c/hash-join-min-input-size)
           (long (:in-size hash-step)))
       (<= (long c/hash-join-min-input-size)
           (long (:tgt-size hash-step)))
       (target-plan-scans-attr? (:tgt-steps hash-step) target-attr)))

(defn- reusable-domain-sip-candidate
  [database sources used hash-step]
  (when (identical? (get-in hash-step [:link :type]) :val-eq)
    (some
      (fn [col]
        (let [domain-var  (col-var col)
              target-attr (col-attr col)
              source      (get sources domain-var)
              source-id   (when source
                            [(:plan-index source) (:step-index source)])]
          (when (and source
                     (not (contains? used source-id))
                     (reusable-domain-sip-candidate?
                       database (:step source) hash-step domain-var
                       target-attr))
            {:domain-var domain-var
             :target-attr target-attr
             :source source
             :source-id source-id})))
      (:cols (peek (:tgt-steps hash-step))))))

(defn- annotate-reusable-sip-domains
  "Connect a fused or-join's pre-expansion value domain to a later val-eq
   hash join. Runtime cardinality guards decide whether to use the bitmap."
  [database plan-trace]
  (if (some nil? plan-trace)
    plan-trace
    (let [trace-vector? (vector? plan-trace)
          plan-trace   (vec plan-trace)
          positions
          (vec
            (mapcat
              (fn [plan-index plan]
                (map-indexed
                  (fn [step-index step]
                    {:plan-index plan-index
                     :step-index step-index
                     :step       step})
                  (:steps plan)))
              (range) plan-trace))
          annotated
          (-> (reduce
                (fn [{:keys [plan sources used] :as state}
                     {:keys [plan-index step-index step] :as position}]
                  (case (-type step)
                    :or-join
                    (if-let [domain-var (first (:free-vars step))]
                      (assoc state :sources
                             (assoc sources domain-var position))
                      state)

                    :hash-join
                    (if-let [{:keys [domain-var target-attr source source-id]}
                             (reusable-domain-sip-candidate
                               database sources used step)]
                      (let [domain-id (Object.)
                            capture-step
                            (assoc (:step source)
                                   :sip-domain-id domain-id
                                   :sip-domain-var domain-var)
                            hash-step
                            (assoc step
                                   :sip-domain-id domain-id
                                   :sip-domain-var domain-var
                                   :sip-target-attr target-attr)]
                        {:plan (-> plan
                                   (assoc-in [(:plan-index source) :steps
                                              (:step-index source)]
                                             capture-step)
                                   (assoc-in [plan-index :steps step-index]
                                             hash-step))
                         :sources sources
                         :used (conj used source-id)})
                      state)

                    state))
                {:plan plan-trace :sources {} :used #{}}
                positions)
              :plan)]
      (if trace-vector? annotated (seq annotated)))))

(defn build-plan*
  [db sources rules nodes required-vars projected-vars]
  (let [cc              (connected-components nodes)
        incoming-counts (incoming-link-counts nodes)
        components      (mapv
                          (fn [component]
                            [component
                             (deferred-base-sample-entities
                               db nodes component projected-vars)])
                          cc)
        deferred        (into #{} (mapcat second) components)
        alternatives
        (if (= 1 (count components))
          (let [[component deferred] (first components)]
            [(component-plan-alternatives
               db sources rules nodes incoming-counts required-vars
               component deferred)])
          (map+ (bound-fn [[component deferred]]
                  (component-plan-alternatives
                    db sources rules nodes incoming-counts required-vars
                    component deferred))
                components))]
    {:plans (mapv #(annotate-reusable-sip-domains db (:plan %)) alternatives)
     :deferred deferred
     :attribute-group-planning
     (not-empty (mapv :attribute-group-planning
                      (filter :attribute-group-planning alternatives)))}))

(defn- required-plan-vars
  [{:keys [parsed-q rels late-clauses optimizable-not-joins graph]} src]
  (set/union
    (set (dp/find-vars (:qfind parsed-q)))
    (set (map :symbol (:qwith parsed-q)))
    (into #{} (mapcat (comp keys :attrs)) rels)
    (qu/collect-vars late-clauses)
    (qu/collect-vars optimizable-not-joins)
    (reduce-kv (fn [vars other-src nodes]
                 (if (= src other-src)
                   vars
                   (into vars (qu/collect-vars nodes))))
               #{} graph)))

(defn- strip-step-result
  [step]
  (let [sample-size (when (instance? List (:sample step))
                      (.size ^List (:sample step)))
        step (cond-> step
               (contains? step :tgt-steps)
               (update :tgt-steps (fn [steps]
                                    (mapv strip-step-result steps)))

               (contains? step :join-steps)
               (update :join-steps (fn [steps]
                                     (mapv strip-step-result steps)))

               sample-size
               (assoc :sample-size sample-size))]
    (assoc step :result nil :sample nil)))

(defn- strip-result
  [plans]
  (mapv (fn [plan-vec]
          (mapv #(update % :steps (fn [steps]
                                    (mapv strip-step-result steps)))
                plan-vec))
        plans))

(defn- assoc-source-plan
  [context src plans deferred attribute-group-planning]
  (let [context (assoc-in context [:plan src] plans)
        context
        (if (seq deferred)
          (assoc-in context [:deferred-base-samples src]
                    (vec (sort-by str deferred)))
          (let [remaining (not-empty
                            (dissoc (:deferred-base-samples context) src))]
            (cond-> (dissoc context :deferred-base-samples)
              remaining (assoc :deferred-base-samples remaining))))]
    (if (seq attribute-group-planning)
      (assoc-in context [:attribute-group-planning src]
                attribute-group-planning)
      (let [remaining (not-empty
                        (dissoc (:attribute-group-planning context) src))]
        (cond-> (dissoc context :attribute-group-planning)
          remaining (assoc :attribute-group-planning remaining))))))

(defn build-plan
  "Generate a query plan that looks like this:

  [{:op :init :attr :name :val \"Tom\" :out #{?e} :vars [?e]
    :cols [?e]}
   {:op :merge-scan  :attrs [:age :friend] :preds [(< ?a 20) nil]
    :vars [?a ?f] :in #{?e} :index 0 :out #{?e} :cols [?e :age :friend]}
   {:op :link :attr :friend :var ?e1 :in #{?e} :index 2
    :out #{?e ?e1} :cols [?e :age :friend ?e1]}
   {:op :merge-scan :attrs [:name] :preds [nil] :vars [?n] :index 3
    :in #{?e ?e1} :out #{?e ?e1} :cols [?e :age :friend ?e1 :name]}]

  :op here means step type.
  :result-set will be #{} if there is any clause that matches nothing."
  [{:keys [graph sources rules parsed-q] :as context}]
  (if graph
    (unreduced
      (reduce-kv
        (fn [c src nodes]
          (let [^DB db        (sources src)
                required-vars (required-plan-vars context src)
                projected-vars (set (dp/find-vars (:qfind parsed-q)))
                k             [(.-store db) nodes required-vars
                               projected-vars]]
            (if-let [cached (.get ^LRUCache (plan-cache) k)]
              (assoc-source-plan c src (:plans cached) (:deferred cached)
                                 (:attribute-group-planning cached))
              (let [nodes (update-nodes db nodes)]
                ;; A zero count has already been verified against the actual
                ;; index by zero-count-clause-size. Since every graph node is
                ;; conjunctive, no base sampling or join enumeration can make
                ;; this source component satisfiable.
                (if (some #(zero? (long (:mcount %))) (vals nodes))
                  (reduced (assoc c :result-set #{}))
                  (let [{:keys [plans deferred attribute-group-planning]}
                        (if (< 1 (count nodes))
                          (build-plan* db sources rules nodes required-vars
                                       projected-vars)
                          {:plans [[(base-plan
                                      db nodes (ffirst nodes) true true
                                      (plan-materialized-vars
                                        nodes required-vars))]]
                           :deferred #{}
                           :attribute-group-planning nil})]
                    (if (some #(some nil? %) plans)
                      (reduced (assoc c :result-set #{}))
                      (do (.put ^LRUCache (plan-cache) k
                                {:plans (strip-result plans)
                                 :deferred deferred
                                 :attribute-group-planning
                                 attribute-group-planning})
                          (assoc-source-plan c src plans deferred
                                             attribute-group-planning)))))))))
        context graph))
    context))

(defn- component-binds-vars?
  [plans vars]
  (when-let [step (some-> plans last :steps last)]
    (let [cols (:cols step)]
      (every? #(some? (find-index % cols)) vars))))

(defn- add-not-join-step
  [plans clause sources rules]
  (let [plans     (vec plans)
        plan-idx  (dec (count plans))
        last-plan (plans plan-idx)
        last-step (some-> last-plan :steps peek)
        vars      (get-not-join-vars clause)
        nstep     (mk-not-join-step clause vars sources rules
                                    (:out last-step) (:out last-step)
                                    (:cols last-step) (:strata last-step)
                                    (:seen-or-joins last-step))]
    (assoc plans plan-idx (update last-plan :steps conj nstep))))

(defn plan-not-joins
  "Attach optimizable not-join clauses to source plans when all join vars are
   bound by a single component. Unlinked clauses remain in :late-clauses."
  [{:keys [plan sources rules optimizable-not-joins] :as context}]
  (if (seq optimizable-not-joins)
    (let [plan'                (into {} (map (fn [[src comps]]
                                               [src (mapv vec comps)]))
                                    plan)
          [planned unlinked]
          (reduce
            (fn [[p u] clause]
              (let [src        (get-not-join-source clause)
                    vars       (get-not-join-vars clause)
                    components (get p src)]
                (if (and (seq vars) (seq components))
                  (let [idxs (keep-indexed
                               (fn [i comp]
                                 (when (component-binds-vars? comp vars) i))
                               components)]
                    (if (= 1 (count idxs))
                      (let [idx (first idxs)]
                        [(assoc-in p [src idx]
                                   (add-not-join-step
                                     (nth components idx) clause sources rules))
                         u])
                      [p (conj u clause)]))
                  [p (conj u clause)])))
            [plan' []]
            optimizable-not-joins)]
      (-> context
          (assoc :plan planned)
          (update :late-clauses into unlinked)
          (assoc :optimizable-not-joins [])))
    context))
