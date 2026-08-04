;;
;; Copyright (c) Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice.
;;
(ns ^:no-doc datalevin.query.access.fulltext
  "Ranked and resumable access paths for fulltext query functions."
  (:require
   [datalevin.built-ins :as built-ins]
   [datalevin.constants :as c]
   [datalevin.interface :refer [doc-count]]
   [datalevin.query.access :as access]
   [datalevin.query.access.function :as function]
   [datalevin.search :as search])
  (:import
   [datalevin.db DB]
   [datalevin.storage Store]
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
    :refs          3
    :refs+scores   4
    :texts         4
    :offsets       4
    :texts+offsets 5
    nil))

(defn- effective-display
  [engine opts]
  (or (:display opts)
      (get-in engine [:search-opts :display])
      c/default-display))

(defn- request-for
  [^DB source spec]
  (when-let [[arg1 arg2 arg3]
             (argument-slots (function/resolve-arguments spec))]
    (let [opts (if (keyword? arg1) arg3 arg2)]
      (when (or (nil? opts) (map? opts))
        (try
          (let [request
                (built-ins/fulltext-request
                  source arg1 arg2 arg3 (get-in spec [:projection :needed]))]
            (assoc request :domains (vec (:domains request))))
          (catch clojure.lang.ExceptionInfo _
            nil))))))

(defn- request-compatible?
  [request spec]
  (let [{:keys [engines domains opts]} request
        source-width (get-in spec [:projection :source-width])]
    (and (seq domains)
         (every? engines domains)
         (every? #(= source-width
                     (display-width (effective-display (engines %) opts)))
                 domains))))

(defn- request-range-rows
  ^long [{:keys [engines domains opts]}]
  (reduce
    (fn [^long total domain]
      (let [engine (engines domain)
            {:keys [offset limit]}
            (search/search-page opts (get engine :search-opts))
            available (max 0 (- (long (doc-count engine)) (long offset)))]
        (+ total (min available (long limit)))))
    0 domains))

(defn- request-score-var
  [{:keys [engines domains opts]} spec]
  (when (and (= 1 (count domains))
             (= :refs+scores
                (effective-display (engines (first domains)) opts)))
    (first
      (keep (fn [[sym source-idx]]
              (when (= 3 source-idx) sym))
            (get-in spec [:projection :source-attrs])))))

(defn- continuation-index
  ^long [resume]
  (long
    (or (some-> resume :continuation :index)
        (:index resume)
        0)))

(defn- tuple-score
  [^objects tuple ^long score-idx]
  (aget tuple (int score-idx)))

(deftype FulltextCursor
    [request ^long batch-size max-candidates ^long score-idx state closed?]

  access/IAccessCursor
  (-next-batch [_]
    (if @closed?
      (access/->AccessBatch (FastList.) nil true)
      (let [{:keys [remaining initialized? index scanned]} @state
            candidate-remaining
            (when (some? max-candidates)
              (max 0 (- (long max-candidates) (long scanned))))
            page-size  (long
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
                          (built-ins/fulltext-request-results request))))
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
                index        (+ (long index) batch-count)
                exhausted?   (nil? (seq remaining))
                boundary
                (when (and (<= 0 score-idx) (pos? batch-count))
                  (tuple-score (.get batch (dec (int batch-count)))
                               score-idx))
                boundary-complete?
                (and boundary
                     (or exhausted?
                         (not= boundary
                               (tuple-score (first remaining) score-idx))))
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

(defrecord FulltextAccessMethod []
  function/IFunctionAccessBackend
  (-function-access-plans
    [this _planning-context spec]
    (let [source (:source-value spec)]
      (if (and (empty? (:requires spec))
               (instance? DB source)
               (instance? Store (.-store ^DB source)))
        (if-let [request (request-for source spec)]
          (if (request-compatible? request spec)
            (let [range-rows (request-range-rows request)
                  score-var  (request-score-var request spec)
                  score-idx  (long
                               (if score-var
                                 (.indexOf ^java.util.List (:cols spec)
                                           score-var)
                                 -1))
                  ordered?   (<= 0 score-idx)
                  ordering   (when ordered? [[score-var :desc]])
                  capabilities
                  (cond-> #{:complete :resumable}
                    ordered?
                    (into access/top-k-proof-capabilities))
                  startup     (double
                                (max 1.0
                                     (* (double range-rows)
                                        (double c/magic-cost-pred))))
                  per-row     1.0
                  cost        (if (zero? range-rows)
                                0.0
                                (+ startup (* (double range-rows) per-row)))
                  expr        (function/access-expr
                                spec :fulltext (:produces spec))
                  path
                  (assoc
                    (access/->AccessPath
                      :fulltext this :ranked-scan ordering capabilities
                      {:request request :spec spec :score-idx score-idx}
                      (access/access-policy :none :execution))
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
    (let [request   (get-in path [:options :request])
          source    (or source (get-in path [:options :spec :source-value]))
          _         (when-not (instance? DB source)
                      (throw
                        (ex-info "Fulltext access requires a database source"
                                 {:source source})))
          resume    (:resume work)
          index     (continuation-index resume)
          emitted   (long (or (:emitted work) index))]
      (FulltextCursor.
        request
        (max 1 (long (or (:batch-size work) default-batch-size)))
        (:max-candidates work)
        (long (get-in path [:options :score-idx] -1))
        (volatile! {:remaining nil
                    :initialized? false
                    :index index
                    :scanned emitted})
        (volatile! false))))

  (-frontier-satisfies? [_ _path _demand frontier cutoff]
    (when-let [certificate (:certificate frontier)]
      (not (pos? (compare certificate (:primary-value cutoff)))))))

(def access-method (->FulltextAccessMethod))
