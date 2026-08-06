;;
;; Copyright (c) Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice.
;;
(ns ^:no-doc datalevin.query.access.vector
  "Ranked and resumable access paths over existing approximate vector results."
  (:require
   [datalevin.built-ins :as built-ins]
   [datalevin.constants :as c]
   [datalevin.interface :refer [vecs-info]]
   [datalevin.query.access :as access]
   [datalevin.query.access.function :as function]
   [datalevin.vector :as vector])
  (:import
   [datalevin.db DB]
   [datalevin.storage Store]
   [datalevin.vector VectorIndex]
   [org.eclipse.collections.impl.list.mutable FastList]))

(def ^:private ^:const ^long default-batch-size 128)

(defn- argument-slots
  [args]
  (case (count args)
    1 [(nth args 0) nil nil]
    2 [(nth args 0) (nth args 1) nil]
    3 [(nth args 0) (nth args 1) (nth args 2)]
    nil))

(defn- display-width
  [display]
  (case display
    :refs       3
    :refs+dists 4
    nil))

(defn- effective-display
  [^VectorIndex index opts]
  (or (:display opts)
      (:display (.-search-opts index))
      (:display vector/default-search-opts)))

(defn- effective-top
  ^long [^VectorIndex index opts]
  (long
    (or (:top opts)
        (:top (.-search-opts index))
        (:top vector/default-search-opts))))

(defn- request-for
  [^DB source spec]
  (when-let [[arg1 arg2 arg3]
             (argument-slots (function/resolve-arguments spec))]
    (let [opts (if (keyword? arg1) arg3 arg2)]
      (when (or (nil? opts) (map? opts))
        (try
          (case (:function spec)
            vec-neighbors
            (built-ins/vec-neighbors-request
              source arg1 arg2 arg3 (get-in spec [:projection :needed]))

            embedding-neighbors
            (built-ins/embedding-neighbors-request
              source arg1 arg2 arg3 (get-in spec [:projection :needed]))

            nil)
          (catch clojure.lang.ExceptionInfo _
            nil))))))

(defn- request-indices
  [{:keys [indices domains]}]
  (into []
        (keep (fn [domain]
                (when-let [index (indices domain)]
                  [domain index])))
        domains))

(defn- request-compatible?
  [request spec]
  (let [domain-indices (request-indices request)
        source-width   (get-in spec [:projection :source-width])
        opts           (:opts request)]
    (and (seq domain-indices)
         (every? (fn [[_ index]]
                   (= source-width
                      (display-width (effective-display index opts))))
                 domain-indices))))

(defn- request-range-rows
  ^long [request]
  (let [opts (:opts request)]
    (reduce
      (fn [^long total [_ index]]
        (let [size (long (:size (vecs-info index)))
              top  (effective-top index opts)]
          (+ total (min size top))))
      0
      (request-indices request))))

(defn- request-distance-var
  [request spec]
  (let [domain-indices (request-indices request)]
    (when (and (= 1 (count domain-indices))
               (= :refs+dists
                  (effective-display (second (first domain-indices))
                                     (:opts request))))
      (first
        (keep (fn [[sym source-idx]]
                (when (= 3 source-idx) sym))
              (get-in spec [:projection :source-attrs]))))))

(defn- continuation-index
  ^long [resume]
  (long
    (or (some-> resume :continuation :index)
        (:index resume)
        0)))

(defn- tuple-distance
  [^objects tuple ^long distance-idx]
  (aget tuple (int distance-idx)))

(deftype VectorCursor
    [request ^long batch-size max-candidates ^long distance-idx state closed?]

  access/IAccessCursor
  (-next-batch [_]
    (if @closed?
      (access/->AccessBatch (FastList.) nil true)
      (let [{:keys [remaining initialized? index scanned]} @state
            candidate-remaining
            (when (some? max-candidates)
              (max 0 (- (long max-candidates) (long scanned))))
            page-size
            (long
              (if (some? candidate-remaining)
                (min batch-size (long candidate-remaining))
                batch-size))]
        (if (zero? page-size)
          (access/->AccessBatch (FastList.) nil false)
          (let [remaining
                (if initialized?
                  remaining
                  (seq
                    (drop (long index)
                          (built-ins/vector-request-results request))))
                batch (FastList. (int page-size))
                remaining
                (loop [n 0
                       remaining remaining]
                  (if (and (< (long n) page-size) (seq remaining))
                    (do
                      (.add batch (first remaining))
                      (recur (unchecked-inc n) (next remaining)))
                    remaining))
                batch-count (long (.size batch))
                index       (+ (long index) batch-count)
                exhausted?  (nil? (seq remaining))
                boundary
                (when (and (<= 0 distance-idx) (pos? batch-count))
                  (tuple-distance (.get batch (dec (int batch-count)))
                                  distance-idx))
                boundary-complete?
                (and boundary
                     (or exhausted?
                         (not= boundary
                               (tuple-distance (first remaining)
                                               distance-idx))))
                frontier
                (when (pos? batch-count)
                  (access/->AccessFrontier
                    {:index index}
                    (when boundary-complete? boundary)))]
            (vreset! state {:remaining remaining
                            :initialized? true
                            :index index
                            :scanned (+ (long scanned) batch-count)})
            (when exhausted? (vreset! closed? true))
            (assoc
              (access/->AccessBatch batch frontier exhausted?)
              :scanned batch-count))))))

  (-close-cursor [_]
    (vreset! closed? true)))

(defrecord VectorAccessMethod []
  function/IFunctionAccessBackend
  (-function-access-plans
    [this _planning-context spec]
    (let [source (:source-value spec)]
      (if (and (empty? (:requires spec))
               (instance? DB source)
               (instance? Store (.-store ^DB source)))
        (if-let [request (request-for source spec)]
          (if (request-compatible? request spec)
            (let [range-rows  (request-range-rows request)
                  distance-var (request-distance-var request spec)
                  distance-idx
                  (long
                    (if distance-var
                      (.indexOf ^java.util.List (:cols spec) distance-var)
                      -1))
                  ordered?    (<= 0 distance-idx)
                  ordering    (when ordered? [[distance-var :asc]])
                  capabilities
                  (cond-> #{:complete :resumable}
                    ordered?
                    (into access/top-k-proof-capabilities))
                  startup      (double
                                 (max 1.0
                                      (* (double range-rows)
                                         (double c/magic-cost-pred))))
                  per-row      1.0
                  cost         (if (zero? range-rows)
                                 0.0
                                 (+ startup
                                    (* (double range-rows) per-row)))
                  expr         (function/access-expr
                                 spec :vector (:produces spec))
                  path
                  (assoc
                    (access/->AccessPath
                      :vector this :approximate-ranked-scan ordering
                      capabilities
                      {:request request
                       :spec spec
                       :distance-idx distance-idx}
                      (access/access-policy :none :execution))
                    ;; Exact means equivalent to the existing approximate
                    ;; query function, not exact nearest-neighbor search.
                    :quality :exact)
                  estimate
                  (assoc
                    (access/->AccessEstimate
                      startup per-row range-rows cost :low)
                    :range-rows range-rows
                    :scan-rows range-rows
                    :output-rows range-rows
                    :conventional-cost cost)]
              [(access/->AccessPlan
                 expr path (access/source-bounds)
                 (access/access-work default-batch-size) estimate)])
            [])
          [])
        [])))

  access/IAccessMethod
  (-access-plans [_ _planning-context]
    [])

  (-open-access [_ path _demand _bounds work source]
    (let [request (get-in path [:options :request])
          source  (or source (get-in path [:options :spec :source-value]))
          _       (when-not (instance? DB source)
                    (throw
                      (ex-info "Vector access requires a database source"
                               {:source source})))
          resume  (:resume work)
          index   (continuation-index resume)
          emitted (long (or (:emitted work) index))]
      (VectorCursor.
        request
        (max 1 (long (or (:batch-size work) default-batch-size)))
        (:max-candidates work)
        (long (get-in path [:options :distance-idx] -1))
        (volatile! {:remaining nil
                    :initialized? false
                    :index index
                    :scanned emitted})
        (volatile! false))))

  (-frontier-satisfies? [_ _path _demand frontier cutoff]
    (when-let [certificate (:certificate frontier)]
      (not (neg? (compare certificate (:primary-value cutoff)))))))

(def access-method (->VectorAccessMethod))
