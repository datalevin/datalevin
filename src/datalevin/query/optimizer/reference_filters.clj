(ns ^:no-doc datalevin.query.optimizer.reference-filters
  "Place scalar reference filters using a bounded sample of their input."
  (:require [clojure.set :as set]
            [datalevin.constants :as c]
            [datalevin.db :as db]
            [datalevin.query.optimizer.estimates :refer [estimate-round]]
            [datalevin.query.optimizer.plan-cost :refer [estimate-scan-v-cost]]
            [datalevin.query.plan :as p]
            [datalevin.util :as u])
  (:import [java.util List]
           [org.eclipse.collections.impl.list.mutable FastList]))

(def ^:private native-types
  #{:db.type/string :db.type/long :db.type/ref :db.type/boolean
    :db.type/float :db.type/double :db.type/bigint :db.type/bigdec
    :db.type/instant :db.type/uuid :db.type/keyword :db.type/symbol})

(defn- safe-merge? [schema step scalar?]
  (and (= :merge (p/step-type step))
       (every? (fn [[attr {:keys [pred range-pred? constant-pred?]}]]
                 (let [props (schema attr)]
                   (and (contains? native-types (:db/valueType props))
                        (or (not scalar?)
                            (not= :db.cardinality/many (:db/cardinality props)))
                        (or (nil? pred) constant-pred?
                            (and range-pred?
                                 (contains? #{:db.type/string :db.type/long :db.type/ref
                                              :db.type/float :db.type/double :db.type/bigint
                                              :db.type/bigdec :db.type/instant :db.type/uuid}
                                            (:db/valueType props)))))))
               (:attrs-v step))))

(defn- filter-only? [schema step]
  (and (safe-merge? schema step true)
       (empty? (:vars step))
       (every? (comp :skip? second) (:attrs-v step))
       (not (:result step)) (not (:sample step))))

(defn- binding-vars [col]
  (if (set? col) (into #{} (filter symbol?) col) #{col}))

(defn- dependencies-ready? [filter-step previous]
  (let [cols (:cols previous) required (:cols filter-step)
        indices (cons (:index filter-step)
                      (keep (comp :fidx second) (:attrs-v filter-step)))]
    (every? (fn [index]
              (and (integer? index) (< -1 (long index) (count cols))
                   (seq (set/intersection (binding-vars (nth required index))
                                          (binding-vars (nth cols index))))))
            indices)))

(defn- prefix-sample
  "Reuse the initialization sample and follow only nonexpanding scalar lookups.
  Duplicate rows retain their original weight. No joins or user code are run."
  [db schema trace end]
  (let [steps (vec (mapcat :steps (subvec trace 0 end)))
        initial (first steps)
        ^List input (or (:sample initial) (:result initial))]
    (when (and (= :init (p/step-type initial)) input (pos? (.size input))
               (every? #(safe-merge? schema % true) (next steps)))
      (let [n (min 128 (.size input))
            cohort (FastList. (int n))]
        (doseq [index (u/reservoir-sampling (.size input) n)]
          (.add cohort (.get input (int index))))
        (reduce #(p/step-sample %2 db %1) cohort (next steps))))))

(defn- add-entities [entities added]
  (into entities (remove (set entities)) added))

(defn- relocate
  [trace index position step ratio]
  (let [previous (peek (:steps (nth trace (dec position))))
        prefix (nth trace (dec position))
        old-filter (nth trace index)
        old-before (nth trace (dec index))
        size (long (:size prefix))
        filter-cost (estimate-round (estimate-scan-v-cost step size))
        crossed-cost (- (long (:cost old-before)) (long (:cost prefix)))
        old-work (- (long (:cost old-filter)) (long (:cost prefix)))
        new-work (+ filter-cost (* ratio crossed-cost))
        added (set/difference (set (:out step)) (set (:in step)))
        early (-> step
                  (dissoc :eav-provenance)
                  (assoc :in (:out previous) :out (add-entities (:out previous) added))
                  (merge (select-keys previous [:cols :strata :seen-or-joins :eav-provenance])))]
    (when (< new-work (* old-work (- 1.0 (double c/deferred-eav-min-cost-improvement))))
      (loop [remaining (seq (concat (subvec trace position index)
                                    (subvec trace (inc index))))
             old-cost (long (:cost prefix))
             new-cost (+ (long (:cost prefix)) filter-cost)
             result (conj (subvec trace 0 position)
                          (assoc old-filter :steps [early] :cost new-cost
                                 :size (estimate-round (* size ratio))))]
        (if-let [plan (first remaining)]
          (let [crossed? (< (count result) (inc index))
                prior-cost (if crossed? old-cost
                               (if (= (count result) (inc index))
                                 (long (:cost old-filter)) old-cost))
                cost (+ new-cost (estimate-round (* ratio (- (long (:cost plan)) prior-cost))))
                plan' (cond-> (assoc plan :cost cost
                                         :size (estimate-round (* ratio (long (:size plan)))))
                        crossed?
                        (update :steps #(mapv (fn [s] (-> s
                                                         (update :in add-entities added)
                                                         (update :out add-entities added))) %)))]
            (recur (next remaining) (long (:cost plan)) cost (conj result plan')))
          result)))))

(defn- place-filter [db schema trace index]
  (let [candidate (nth trace index)
        step (first (:steps candidate))]
    (when (and (= 1 (count (:steps candidate))) (filter-only? schema step)
               ;; The ordinary estimate gives this check no selectivity credit.
               (= (:size candidate) (:size (nth trace (dec index)))))
      (when-let [position
                 (first (filter #(dependencies-ready?
                                   step (peek (:steps (nth trace (dec %)))))
                                (range 1 index)))]
        (let [crossed (mapcat :steps (subvec trace position index))]
          (when (and (< (long c/init-exec-size-threshold)
                        (long (:size (nth trace (dec position)))))
                     (every? #(and (not (:result %)) (not (:sample %))
                                   (or (and (= :link (p/step-type %))
                                            (contains? native-types
                                                       (get-in schema [(:attr %) :db/valueType])))
                                       (safe-merge? schema % false))) crossed))
            (when-let [^List sample (prefix-sample db schema trace position)]
              (when (pos? (.size sample))
                (let [n (.size sample)
                      output (p/step-sample step db sample)
                      ratio (/ (double (.size ^List output)) n)]
                  ;; A zero-hit sample is not evidence that the query is empty.
                  (when (< 0.0 ratio 1.0)
                    (relocate trace index position step ratio)))))))))))

(defn order-filters
  "Reorder proven scalar checks before expansions when observed savings justify
  it. Keep unsupported prefixes, arbitrary predicates, and column-remapping
  operators unchanged. This pass only composes existing execution operators."
  [db trace]
  (let [schema (db/-schema db)]
    (loop [trace (vec trace) index 1]
      (if (< index (count trace))
        (recur (or (place-filter db schema trace index) trace) (inc index))
        trace))))
