(ns ^:no-doc datalevin.query.optimizer.existence-placement
  "Bounded work samples for placing keyed existence filters."
  (:require [clojure.set :as set]
            [datalevin.bits :as bits]
            [datalevin.constants :as c]
            [datalevin.db :as db]
            [datalevin.interface :as i]
            [datalevin.query.optimizer.existence :as existence]
            [datalevin.query.plan :as plan]
            [datalevin.query.resolve :as resolve]
            [datalevin.query-util :as qu]
            [datalevin.relation :as r]
            [datalevin.join :as join]
            [datalevin.storage.scan :as scan]
            [datalevin.timeout :as timeout]
            [datalevin.util :as u])
  (:import [java.util List]
           [org.eclipse.collections.impl.list.mutable FastList]))

(def ^:dynamic *strategy* :cost)
(def ^:dynamic *sample-size* 128)
(def ^:dynamic *max-rows* 32768)
(def ^:dynamic *min-improvement* 1.15)

(defn settings []
  [*strategy* *sample-size* *max-rows* *min-improvement*
   c/magic-cost-link-probe c/magic-cost-link-retrieval
   c/magic-cost-merge-scan-v c/magic-cost-pred])

(defn- unsupported!
  ([] (unsupported! :unsupported-step))
  ([reason] (throw (ex-info "Existence placement sample unavailable"
                           {::unavailable true :reason reason}))))

(defn- charge! [budget n]
  (timeout/assert-time-left)
  (when (> (vswap! budget + (long n)) (long *max-rows*)) (unsupported! :sample-budget)))

(defn- bounded [^List tuples]
  (if (<= (.size tuples) (long *sample-size*)) tuples
      (FastList. (mapv #(.get tuples (int %))
                      (u/reservoir-sampling (.size tuples) *sample-size*)))))

(defn- sampled-props [database attr]
  (let [props (get (db/-schema database) attr)]
    (when-not (and props (not (:db/noindex props))
                   (#{:db.type/long :db.type/ref :db.type/boolean :db.type/string
                      :db.type/instant :db.type/double :db.type/float} (:db/valueType props)))
      (unsupported! :unsupported-attribute))
    props))

(defn- facts
  "Controlled cursors admit each row before decoding. Giant/custom values and
  oversized fanout abandon the estimate and unwind the native reader."
  [database attr values entity? budget]
  (let [schema (db/-schema database) props (sampled-props database attr)
        _ (when (some #(and (string? %) (> (count %) 128)) values) (unsupported!))
        keys (FastList. (mapv #(object-array [%]) (distinct values)))
        output (FastList.)
        policy {:schema schema :check! timeout/assert-time-left
                :unsupported! unsupported! :admit-row! (fn [])
                :with-key!
                (fn [key read!]
                  (charge! budget 1)
                  (if entity?
                    (read! {})
                    (read! {:index-range [(scan/ave-key-range
                                           (:db/aid props) (:db/valueType props)
                                           [[:closed (aget ^objects key 0)]
                                            [:closed (aget ^objects key 0)]]) :avg]})))
                :visit-value!
                (fn [raw _ emit]
                  (charge! budget 1)
                  (when (or (> (.remaining ^java.nio.ByteBuffer raw) 512)
                            (not= c/normal (bits/avg->giant-id raw)))
                    (unsupported! :giant-or-oversized-value))
                  (emit (bits/avg->inline-value raw)))}]
    (if entity?
      (db/-lookup-tuples-controlled database output attr keys true policy)
      (db/-value-tuples-controlled database output attr keys policy))
    output))

(defn- lookup
  [database rel entity attr value budget]
  (let [_ (sampled-props database attr)
        attrs (:attrs rel) tuples ^List (:tuples rel)
        entity? (contains? attrs entity)
        key (if entity? entity value)
        _ (when-not (contains? attrs key) (unsupported! :unbound-sample-key))
        keys (mapv #(aget ^objects % (int (attrs key))) tuples)
        ;; Limit the input cohort before a wide indexed expansion. Actual row
        ;; admission still enforces the bound if counted metadata is stale.
        n (if entity? (.size tuples)
              (loop [n 0 total 0]
                (if (< n (.size tuples))
                  (let [cnt (long (i/av-size (:store database) attr (nth keys n)))]
                    (if (> (+ total cnt) (min 4096 (max 1 (quot (long *max-rows*) 4))))
                      (max 1 n) (recur (inc n) (+ total cnt)))) n)))
        input (if (= n (.size tuples)) rel
                  (assoc rel :tuples (FastList. (.subList tuples 0 n))))
        keys (subvec keys 0 n)
        found ^List (facts database attr keys entity? budget)
        produced (if entity? value entity)
        free? (qu/binding-var? produced)
        fact-attrs (if entity? {entity 0} {value 0})
        fact-attrs (if free? (assoc fact-attrs produced 1) fact-attrs)
        found (if (or free? (= produced '_) (qu/placeholder? produced)) found
                  (FastList. (filterv #(= produced (aget ^objects % 1)) found)))
        ;; Bound the join's multiplicity before allocating output tuples.
        frequencies (frequencies keys)
        pairs (reduce + 0 (map #(long (get frequencies (aget ^objects % 0) 0)) found))
        _ (charge! budget pairs)
        result (join/hash-join input (r/relation! fact-attrs found))
        work (+ (* n (double (if entity? c/magic-cost-merge-scan-v c/magic-cost-link-probe)))
                (* (.size found) (double c/magic-cost-link-retrieval)))]
    {:rel result :input n :work work}))

(defn- predicate [rel clause budget]
  (charge! budget (.size ^List (:tuples rel)))
  (let [result (resolve/filter-by-pred {:rels [rel]} clause)]
    {:rel (first (:rels result))
     :work (* (.size ^List (:tuples rel)) (double c/magic-cost-pred))}))

(declare filter-existence)

(defn- body [database rel clauses budget]
  (reduce
    (fn [{:keys [rel work]} clause]
      (let [result (cond
                     (and (vector? clause) (= 3 (count clause)) (keyword? (second clause)))
                     (lookup database rel (first clause) (second clause) (nth clause 2) budget)
                     (existence/simple-predicate? clause) (predicate rel clause budget)
                     :else (unsupported! :unsupported-body))]
        ;; Subsampling inside an existence body would change its meaning.
        (when (and (:input result) (not= (:input result) (.size ^List (:tuples rel))))
          (unsupported! :body-fanout))
        {:rel (:rel result) :work (+ work (:work result))}))
    {:rel rel :work 0.0} clauses))

(defn- filter-existence [database outer clause budget]
  (let [[head vars & clauses] clause
        _ (when-not (= head 'not-join) (unsupported!))
        semi? (and (= 1 (count clauses)) (= 'not-join (ffirst clauses))
                   (= vars (second (first clauses))))
        clauses (if semi? (nnext (first clauses)) clauses)
        keys (r/project-distinct outer vars)
        sampled (body database keys clauses budget)
        matched (r/project-distinct (:rel sampled) vars)
        rejected (if semi? (join/subtract-rel keys matched) matched)]
    {:rel (join/subtract-rel outer rejected)
     :work (+ (:work sampled) (.size ^List (:tuples outer)))}))

(defn- filters [database rel clauses budget]
  (reduce (fn [{:keys [rel work]} clause]
            (let [result (filter-existence database rel clause budget)]
              {:rel (:rel result) :work (+ work (:work result))}))
          {:rel rel :work 0.0} clauses))

(defn- step [database rel previous physical budget]
  (let [names (fn [cols] (mapv (fn [col] (if (set? col) (some #(when (symbol? %) %) col) col)) cols))]
    (case (plan/step-type physical)
      :tuple-predicate (predicate rel (:clause physical) budget)
      :identity {:rel rel :work 0.0}
      :link
      (let [input-var (nth (names (:cols previous)) (:index physical))
            output-var (or (when (:fidx physical) (nth (names (:cols previous)) (:fidx physical)))
                           (when (int? (:var physical)) (nth (names (:cols previous)) (:var physical)))
                           (:var physical))]
        (lookup database rel output-var (:attr physical) input-var budget))
      :merge
      (reduce
        (fn [{:keys [rel work input]} [attr opts]]
          (let [entity (nth (names (:cols previous)) (:index physical))
                v (some #(when (and (set? %) (contains? % attr))
                           (some (fn [x] (when (symbol? x) x)) %)) (:cols physical))
                ;; Predicate-only checks use a local column that is discarded.
                v (or v (gensym "?placement"))
                result (lookup database rel entity attr v budget)
                result-rel (:rel result)
                value-idx ((:attrs result-rel) v)
                pred (:pred opts)
                selected (if pred
                           (assoc result-rel :tuples
                                  (r/select-tuples #(pred (aget ^objects % (int value-idx)))
                                                   (:tuples result-rel))) result-rel)]
            {:rel selected :input input :work (+ work (:work result))}))
        {:rel rel :work 0.0} (sort-by #(get-in (db/-schema database) [(first %) :db/aid]) (:attrs-v physical)))
      (unsupported! (keyword (str "unsupported-" (name (plan/step-type physical))))))))

(defn- initial [database physical budget]
  (let [entity (first (:vars physical)) attr (:attr physical) value (:val physical)]
    (when (:know-e? physical) (unsupported! :unsupported-initialization))
    (if (some? value)
      (let [found (facts database attr [value] false budget)
            rows (FastList. (mapv #(object-array [(aget ^objects % 1)]) found))]
        {:rel (r/relation! {entity 0} (bounded rows)) :rows (.size rows)})
      (let [props (get (db/-schema database) attr)
            _ (when-not (#{:db.type/long :db.type/ref :db.type/boolean
                            :db.type/instant :db.type/double :db.type/float} (:db/valueType props))
                (unsupported! :unsupported-initialization))
            _ (charge! budget (min (long *sample-size*) (long (:mcount physical))))
            tuples (binding [c/init-exec-size-threshold *sample-size*]
                     (plan/step-sample (assoc physical :result nil) database nil))]
        {:rel (r/relation! (plan/cols->attrs (:cols physical)) (bounded tuples))
         :rows (:mcount physical)}))))

(defn choose-placement
  "Require a material improvement over the last placement. Uncertain costs
  retain the conventional order; sample zeros never establish emptiness."
  [alternatives]
  (let [late (last alternatives) best (apply min-key :cost alternatives)]
    (if (< (* (double *min-improvement*) (:cost best)) (:cost late)) best late)))

(defn select
  [database plans clauses]
  (let [clauses (mapv #(if (qu/source? (first %)) (next %) %) clauses)
        last-site (dec (count plans))
        vars (qu/collect-vars (map second clauses))
        sites (vec (keep-indexed
                     #(when (set/subset? vars (set (keys (plan/cols->attrs (:cols (peek (:steps %2))))))) %1)
                     plans))
        fallback {:site last-site :strategy :late :reason :unsupported-or-thin-sample}]
    (cond
      (= *strategy* :late) (assoc fallback :reason :forced)
      (= *strategy* :early) {:site (first sites) :strategy :early :reason :forced}
      (< (count sites) 2) (assoc fallback :reason :single-legal-site)
      :else
      (try
        (let [budget (volatile! 0)
              first-step (first (:steps (first plans)))
              start (initial database first-step budget)
              snapshots
              (loop [idx 0 rel (:rel start) rows (double (:rows start)) work 0.0
                     previous first-step snapshots []]
                (if (= idx (count plans)) snapshots
                    (let [physical (:steps (nth plans idx))
                          physical (if (zero? idx) (next physical) physical)
                          state (reduce
                                  (fn [{:keys [rel rows work previous]} s]
                                    (let [n (.size ^List (:tuples rel))
                                          _ (when (zero? n) (unsupported! :empty-prefix))
                                          result (step database rel previous s budget)
                                          input (long (or (:input result) n))
                                          output (:rel result)
                                          factor (/ rows input)]
                                      {:rel (assoc output :tuples (bounded (:tuples output)))
                                       :rows (* factor (.size ^List (:tuples output)))
                                       :work (+ work (* factor (:work result))) :previous s}))
                                  {:rel rel :rows rows :work work :previous previous} physical)]
                      (recur (inc idx) (:rel state) (:rows state) (:work state) (:previous state)
                             (conj snapshots state)))))
              total-work (:work (peek snapshots))
              alternatives
              (mapv (fn [site]
                      (let [{:keys [rel rows work]} (snapshots site)
                            n (.size ^List (:tuples rel))
                            _ (when (< n 24) (unsupported! :thin-sample))
                            result (filters database rel clauses budget)
                            survivors (.size ^List (:tuples (:rel result)))
                            ;; Smooth rare outcomes and keep all estimates finite.
                            yield (/ (double (inc survivors)) (+ n 2.0))
                            check-cost (* (/ rows n) (:work result))
                            suffix (- total-work work)]
                        {:site site :input-rows rows :sample-input n :sample-output survivors
                         :yield yield :existence-cost check-cost :suffix-cost suffix
                         :cost (+ work check-cost (* yield suffix))})) sites)
              chosen (choose-placement alternatives)]
          {:site (:site chosen) :strategy :cost :alternatives alternatives
           :sample-work @budget :reason :estimated-total-work})
        (catch clojure.lang.ExceptionInfo e
          (if (::unavailable (ex-data e))
            (assoc fallback :reason (:reason (ex-data e))) (throw e)))))))
