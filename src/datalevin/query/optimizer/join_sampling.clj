;;
;; Copyright (c) Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns ^:no-doc datalevin.query.optimizer.join-sampling
  "Bounded, lazy samples of native join prefixes. Samples retain complete
  tuples and multiplicity; physical hash builds are sampled by indexed probes."
  (:require
   [datalevin.interface :as i]
   [datalevin.query.optimizer.estimates :refer [estimate-round]]
   [datalevin.query.optimizer.sampling :as sampling]
   [datalevin.query.plan :as plan]
   [datalevin.util :as u])
  (:import
   [java.util HashMap List]
   [org.eclipse.collections.impl.list.mutable FastList]))

(def ^:dynamic *sample-size*
  "Maximum retained tuples per prefix; ranks are sampled without replacement."
  128)
(def ^:dynamic *max-probes* 32768)
(def ^:dynamic *max-rows* 32768)
(def ^:dynamic *max-cost* 1000000.0)
(def ^:dynamic *budget* nil)
(def ^:dynamic *enabled?*
  "Experimental complete-join sampling; opt in until plan benefits are established."
  false)
(def ^:dynamic *cached-only?* false)

(defn new-budget [] (atom {:probes 0 :rows 0 :cost 0.0}))

(defn- admit!
  "Reserve before reading or materializing. Shared across sources, components
  and rejected alternatives, including parallel component planning."
  [probes rows cost]
  (when *budget*
    (loop []
      (let [old @*budget*
            next {:probes (+ (long (:probes old)) (long probes))
                  :rows (+ (long (:rows old)) (long rows))
                  :cost (+ (double (:cost old)) (double cost))}]
        (when (and (<= (long (:probes next)) (long *max-probes*))
                   (<= (long (:rows next)) (long *max-rows*))
                   (<= (double (:cost next)) (double *max-cost*)))
          (if (compare-and-set! *budget* old next) true (recur)))))))

(defn- bounded-tuples
  ^List [^List tuples]
  (let [n (.size tuples)
        size (min n (long *sample-size*))]
    (if (= n size)
      tuples
      (let [out (FastList. (int size))]
        (doseq [idx (u/reservoir-sampling n size)]
          (.add out (.get tuples (int idx))))
        out))))

(defn- source-key [candidate]
  (or (::key candidate) #{(:steps candidate)}))

(defn with-source
  "Remember the indexed equivalent only during enumeration. The output column
  layout can differ from a hash join's layout, so it is projected explicitly."
  ([candidate parent steps] (with-source candidate parent steps nil))
  ([candidate parent steps request]
   (if-not *budget*
     candidate
     (assoc candidate
            ::source {:parent parent :steps steps :request request}
            ;; Inner joins are associative: the same set of restricted entity
            ;; relations has the same multiplicity, regardless of join order or
            ;; primary index. Keep layout separately from this logical identity.
            ::key (conj (source-key parent)
                        (if request
                          (or (:steps (:target request)) [:link (:link request)])
                          steps))))))

(defn strip-source [candidate] (dissoc candidate ::source ::key))

(defn- single-valued-merge?
  [schema step]
  (every? (fn [[attr _]]
            (let [props (schema attr)]
              (and props (not= :db.cardinality/many (:db/cardinality props)))))
          (:attrs-v step)))

(defn- countable-link?
  [schema step]
  (let [props (schema (:attr step))]
    ;; Giant/custom values can make av-size scan the duplicate list. Only use
    ;; index counts with bounded lookup work; other joins retain the heuristic.
    (and (not (:db/noindex props))
         (#{:db.type/long :db.type/ref :db.type/float :db.type/double
            :db.type/boolean :db.type/instant :db.type/uuid}
          (:db/valueType props)))))

(defn- link-candidates
  [database step ^List tuples]
  (let [store (:store database)
        n (.size tuples)]
    (when (admit! n 0 (sampling/estimated-access-sample-stage-cost n 0 0))
      (loop [idx 0 total 0]
        (if (< idx n)
          (let [^objects tuple (.get tuples idx)
                cnt (long (i/av-size store (:attr step)
                                    (aget tuple (long (:index step)))))
                total (+ total cnt)]
            (when (<= total (- (long *max-rows*)
                               (long (:rows @*budget*))))
              (recur (inc idx) total)))
          total)))))

(defn- sample-steps
  [database steps ^List input prefix?]
  (let [schema (i/schema (:store database))]
    (loop [steps steps tuples input candidates (.size input) complete? true]
      (if-let [step (first steps)]
        (let [kind (plan/step-type step)
              n (.size ^List tuples)
              bound (case kind
                      :identity n
                      :link (when (countable-link? schema step)
                              (link-candidates database step tuples))
                      :merge (when (single-valued-merge? schema step) n)
                      nil)
              probes (if (= kind :merge) (* n (count (:attrs-v step))) n)
              width (count (:cols step))]
          (when (and (some? bound)
                     (<= (long bound)
                         (sampling/sample-stage-output-cap
                           (- (double *max-cost*)
                              (double (:cost @*budget*))) width))
                     (admit! probes bound
                             (sampling/estimated-access-sample-stage-cost
                               probes bound width)))
            (let [output (if (= kind :identity) tuples
                             (plan/step-execute step database tuples))
                  kept (if prefix? (bounded-tuples output) output)]
              (recur (next steps) kept
                     (if (= kind :link) (long bound) candidates)
                     (and complete? (= (.size ^List output) (.size ^List kept)))))))
        {:tuples tuples :candidates candidates :complete? complete?}))))

(defn- project-tuples
  [^List tuples from-cols to-cols]
  (let [from (plan/cols->attrs from-cols)
        to (plan/cols->attrs to-cols)]
    (if (= from to)
      tuples
      (let [indices (mapv (fn [col]
                            (from (if (set? col)
                                    (some #(when (symbol? %) %) col) col)))
                          to-cols)]
        (when (every? some? indices)
          (let [out (FastList. (.size tuples))]
            (doseq [^objects tuple tuples]
              (.add out (object-array (map #(aget tuple (long %)) indices))))
            out))))))

(defn- cache-for
  ^HashMap [^java.util.Map cache key]
  (or (.get cache key)
      (let [entries (HashMap.)]
        (.put cache key entries)
        entries)))

(defn prefix-sample
  "Obtain a representative sample lazily. A failed or empty prefix is cached
  too, but never establishes that the query is empty."
  [database cache candidate]
  (let [samples (cache-for cache ::prefixes)
        key (source-key candidate)]
    (if (.containsKey samples key)
      (when-let [sample (.get samples key)]
        (when-let [tuples (project-tuples (:tuples sample) (:cols sample)
                                         (:cols (peek (:steps candidate))))]
          (assoc sample :tuples tuples)))
      (let [last-step (peek (:steps candidate))
            {:keys [parent steps]} (::source candidate)
            result
            (if parent
              (when-let [input (prefix-sample database cache parent)]
                (when-let [output (sample-steps database steps (:tuples input) true)]
                  (let [tuples (bounded-tuples (:tuples output))]
                    (when (pos? (.size tuples))
                      (when-let [projected
                                 (project-tuples tuples (:cols (peek steps))
                                                 (:cols last-step))]
                        {:tuples projected
                         :complete? (and (:complete? input) (:complete? output)
                                         (= (.size tuples)
                                            (.size ^List (:tuples output))))})))))
              (when-let [^List tuples (or (:result last-step) (:sample last-step))]
                (when (pos? (.size tuples))
                  (let [sample (bounded-tuples tuples)]
                    (when (admit! 0 (.size sample) 0.0)
                      {:tuples sample
                       :complete? (and (some? (:result last-step))
                                       (= (.size sample) (.size tuples)))})))))]
        (.put samples key (when result (assoc result :cols (:cols last-step))))
        result))))

(defn- scale-estimate
  [sample parent link]
  (when sample
    (let [{:keys [input matches candidates candidate-link]} sample
          factor (/ (double (:size parent)) (long input))]
      (cond-> (assoc sample :size (max 1 (estimate-round (* factor (long matches)))))
        (= candidate-link link)
        (assoc :candidate-size (estimate-round (* factor (long candidates))))))))

(defn join-estimate
  "Measure complete matches, without the primary-link fanout floor. A zero or
  thin sample falls back; it is never evidence of an unsatisfiable query."
  [database cache parent target link indexed-steps]
  (when (and *enabled?* *budget* (pos? (long *sample-size*)))
    (let [parents  (cache-for cache ::matches)
          targets  (cache-for parents (source-key parent))
          failures (cache-for targets ::failures)
          key      (or (:steps target) target)]
      ;; Every indexed alternative checks the same full conjunction. Reuse
      ;; its complete-match yield across primary links, but not the candidate
      ;; count: another index can examine a different number of rows.
      (if (.containsKey targets key)
        (scale-estimate (.get targets key) parent link)
        (when-not (or *cached-only?* (.containsKey failures [key link]))
          (let [result
                (when-let [{:keys [tuples complete?]}
                           (prefix-sample database cache parent)]
                  (when-let [output (sample-steps database indexed-steps tuples false)]
                    (let [n (.size ^List tuples)
                          matches (.size ^List (:tuples output))]
                      (when (and (pos? matches)
                                 (or complete? (and (>= n 32) (>= matches 2))))
                        {:input n :matches matches
                         :candidates (:candidates output)
                         :candidate-link link}))))]
            ;; A failed probe on one index must not rule out a cheaper index.
            (if result
              (.put targets key result)
              (.put failures [key link] true))
            (scale-estimate result parent link)))))))

(defn same-plan?
  [left right]
  (= (mapv #(vector (source-key %) (:size %)) left)
     (mapv #(vector (source-key %) (:size %)) right)))

(defn prime-plan!
  "Give the conventional winner first use of the budget before exploring
  other alternatives. Otherwise enumeration order can starve useful samples."
  [database cache trace]
  (reduce
    (fn [sampled candidate]
      (let [{:keys [parent steps request]} (::source candidate)]
        (if (and (:multi? request)
                 (join-estimate database cache parent (:target request)
                                (:link request) steps))
          true sampled)))
    false trace))
