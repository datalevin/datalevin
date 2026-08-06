;;
;; Copyright (c) Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns ^:no-doc datalevin.query.access
  "Logical and physical abstractions for query access methods."
  (:import
   [datalevin.parser BindScalar DefaultSrc SrcVar]))

(defrecord AccessExpr
    [method covers requires produces join-vars cols source])

(defrecord AccessDemand
    [ordering offset limit required-count quality required-vars])

(defrecord AccessBounds
    [offset limit required-count])

(defrecord AccessWork
    [sample-size batch-size max-candidates resume emitted])

(defrecord AccessPolicy
    [sampling preparation])

(defrecord AccessPath
    [method implementation strategy ordering capabilities options policy])

(defrecord AccessFrontier
    [continuation certificate])

(defrecord AccessBatch
    [tuples frontier exhausted?])

(defrecord AccessEstimate
    [startup per-row rows cost confidence])

(defrecord AccessPlan
    [expr path bounds work estimate])

(defn estimate-range-rows
  "Return the logical upper bound on rows an access cursor can emit."
  ^long [estimate]
  (max 0 (long (or (:range-rows estimate) (:rows estimate) 0))))

(defn estimate-scan-rows
  "Return the estimated physical rows inspected by an access path. Paths with
  one-to-one scan and emission behavior can omit `:scan-rows`."
  ^long [estimate]
  (max 0 (long (or (:scan-rows estimate)
                   (estimate-range-rows estimate)))))

(defn estimate-output-rows
  "Return the expected logical rows emitted by an access path. Paths with
  one-to-one scan and emission behavior can omit `:output-rows`."
  ^long [estimate]
  (max 0 (long (or (:output-rows estimate)
                   (estimate-range-rows estimate)))))

(def top-k-proof-capabilities
  #{:ordered :resumable :monotone :tie-complete :exact-frontier})

(defprotocol IAccessMethod
  (-access-plans [method planning-context]
    "Return applicable physical access plans.")
  (-open-access [method path demand bounds work source]
    "Open a physical access path for a root query demand, source-local logical
     bounds, work controls, and the runtime source.")
  (-frontier-satisfies? [method path demand frontier cutoff]
    "Return true when the frontier certifies that unseen candidates cannot
     improve the supplied cutoff."))

(defprotocol ICorrelatedAccessMethod
  (-open-correlated-access [method path demand bounds work source bindings]
    "Open an access path after substituting variables supplied by an outer
     logical subset."))

(defprotocol IAccessCursor
  (-next-batch [cursor]
    "Return the next AccessBatch.")
  (-close-cursor [cursor]
    "Release resources held by the cursor."))

(defn top-k-demand
  [ordering offset limit]
  (let [offset (long (or offset 0))
        limit  (long limit)]
    (->AccessDemand ordering offset limit (+ offset limit) :exact nil)))

(defn limit-demand
  [offset limit quality required-vars]
  (let [offset (long (or offset 0))
        limit  (long limit)]
    (->AccessDemand nil offset limit (+ offset limit) quality required-vars)))

(defn complete-demand
  ([quality required-vars]
   (complete-demand nil 0 nil quality required-vars))
  ([ordering offset limit quality required-vars]
   (->AccessDemand ordering (long (or offset 0)) limit nil quality
                   required-vars)))

(defn source-bounds
  ([]
   (->AccessBounds 0 nil nil))
  ([offset limit]
   (let [offset (long (or offset 0))]
     (->AccessBounds offset limit
                     (when (some? limit)
                       (+ offset (long limit)))))))

(defn access-policy
  ([]
   (->AccessPolicy :restart :planning))
  ([sampling preparation]
   (when-not (#{:resumable :restart :none} sampling)
     (throw (ex-info "Invalid access sampling policy"
                     {:sampling sampling})))
   (when-not (#{:planning :execution} preparation)
     (throw (ex-info "Invalid access preparation policy"
                     {:preparation preparation})))
   (->AccessPolicy sampling preparation)))

(defn source-symbol
  "Return the query source symbol represented by a parsed source or symbol."
  [source]
  (cond
    (symbol? source)                source
    (instance? SrcVar source)       (:symbol source)
    (instance? DefaultSrc source)   '$
    :else                           nil))

(defn scalar-input-values
  "Return scalar query input symbols mapped to their runtime values. Collection
  and relation inputs are deliberately excluded because they can produce more
  than one binding."
  [parsed-q inputs]
  (into {}
        (keep (fn [[binding value]]
                (when (instance? BindScalar binding)
                  [(get-in binding [:variable :symbol]) value])))
        (map vector (:qin parsed-q) inputs)))

(defn planning-input-values
  "Return the cached scalar input map from an access planning context."
  [{:keys [parsed-q inputs input-values]}]
  (cond
    (instance? clojure.lang.Delay input-values) @input-values
    (some? input-values)                        input-values
    :else (scalar-input-values parsed-q inputs)))

(defn resolve-source
  "Resolve an access expression's source identity against query inputs."
  ([input-values source]
   (when-let [sym (source-symbol source)]
     (get input-values sym)))
  ([parsed-q inputs source]
   (resolve-source (scalar-input-values parsed-q inputs) source)))

(defn path-policy
  [path]
  (or (:policy path) (access-policy)))

(defn planning-sample?
  [path]
  (let [{:keys [sampling preparation]} (path-policy path)]
    (and (not= :none sampling)
         (= :planning preparation))))

(defn reusable-sample?
  [path]
  (= :resumable (:sampling (path-policy path))))

(defn access-work
  ([]
   (->AccessWork nil nil nil nil 0))
  ([batch-size]
   (->AccessWork nil batch-size nil nil 0)))

(defn open-access
  ([^AccessPath path demand]
   (open-access path demand nil nil))
  ([^AccessPath path demand work]
   (open-access path demand work nil))
  ([^AccessPath path demand work source]
   (open-access path demand (source-bounds) work source nil))
  ([^AccessPath path demand work source bindings]
   (open-access path demand (source-bounds) work source bindings))
  ([^AccessPath path demand bounds work source bindings]
   (let [method (:implementation path)]
     (if (satisfies? ICorrelatedAccessMethod method)
       (-open-correlated-access method path demand bounds work source bindings)
       (if (seq bindings)
         (throw (ex-info "Access method does not support correlated bindings"
                         {:method   (:method path)
                          :bindings (keys bindings)}))
         (-open-access method path demand bounds work source))))))

(defn frontier-satisfies?
  [^AccessPath path demand frontier cutoff]
  (and frontier
       (-frontier-satisfies? (:implementation path)
                             path demand frontier cutoff)))

(defn- ordering-terms
  [ordering]
  (if (every? sequential? ordering)
    (vec ordering)
    (mapv vec (partition-all 2 ordering))))

(defn- ordering-prefix?
  [provided required]
  (let [provided (ordering-terms provided)
        required (ordering-terms required)]
    (and (seq provided)
         (<= (count provided) (count required))
         (= provided (subvec required 0 (count provided))))))

(defn adaptive-top-k-properties?
  [ordering capabilities demand]
  (and (seq (:ordering demand))
       (some? (:limit demand))
       (ordering-prefix? ordering (:ordering demand))
       (set? capabilities)
       (every? capabilities top-k-proof-capabilities)))

(defn adaptive-top-k?
  [^AccessPath path demand]
  (adaptive-top-k-properties?
    (:ordering path) (:capabilities path) demand))

(defn adaptive-limit-properties?
  [capabilities demand]
  (and (empty? (:ordering demand))
       (some? (:limit demand))
       (pos? (long (:limit demand)))
       (some? (:required-count demand))
       (set? capabilities)
       (contains? capabilities :complete)
       (contains? capabilities :resumable)))

(defn adaptive-limit?
  [^AccessPath path demand]
  (adaptive-limit-properties? (:capabilities path) demand))

(defn adaptive-demand?
  [^AccessPath path demand]
  (or (adaptive-top-k? path demand)
      (adaptive-limit? path demand)))

(defn next-batch
  [cursor]
  (-next-batch cursor))

(defn batch-work
  "Return physical candidates inspected for a batch. Access methods whose
  candidate rechecks can discard rows should attach `:scanned`; one-to-one
  paths can rely on the tuple count."
  ^long [batch]
  (long (or (:scanned batch) (count (:tuples batch)))))

(defn close-cursor
  [cursor]
  (-close-cursor cursor))

(defn access-plans
  [methods planning-context]
  (into []
        (mapcat #(-access-plans % planning-context))
        methods))

(defn access-ready?
  [expr bound-vars]
  (every? (set bound-vars) (:requires expr)))

(defn best-plan
  [plans]
  (when (seq plans)
    (apply min-key #(get-in % [:estimate :cost] Double/POSITIVE_INFINITY)
           plans)))

(defn plan-summary
  [{:keys [expr path demand bounds work estimate joins step sample-batch
           correlated? outer-cols outer-joins unavailable?]}]
  {:method         (:method expr)
   :source         (:source expr)
   :strategy       (:strategy path)
   :covers         (or (:covered-originals expr) (:covers expr))
   :requires       (:requires expr)
   :produces       (:produces expr)
   :join-vars      (:join-vars expr)
   :ordering       (:ordering demand)
   :path-ordering  (:ordering path)
   :offset         (:offset demand)
   :limit          (:limit demand)
   :required-count (:required-count demand)
   :source-offset  (:offset bounds)
   :source-limit   (:limit bounds)
   :source-required-count (:required-count bounds)
   :sample-size    (:sample-size work)
   :batch-size     (:batch-size work)
   :candidate-budget (:max-candidates work)
   :quality        (:quality demand)
   :path-quality   (:quality path)
   :policy          (path-policy path)
   :capabilities   (:capabilities path)
   :estimate       estimate
   :joins          joins
   :sample-prefix-size
   (long (or (some-> sample-batch :tuples count) 0))
   :sample-resumable? (boolean (some-> sample-batch :frontier))
   :correlated?    (boolean correlated?)
   :outer-cols     outer-cols
   :outer-clauses  (mapv :orig-clause outer-joins)
   :unavailable?   (boolean unavailable?)
   :step            (when step
                      {:type :access
                       :cols (:cols step)
                       :out  (:out step)})})
