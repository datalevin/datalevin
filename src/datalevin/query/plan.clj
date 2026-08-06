;;
;; Copyright (c) Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns ^:no-doc datalevin.query.plan
  "Shared query plan, step, and step-execution helpers."
  (:refer-clojure :exclude [update assoc])
  (:require
   [clojure.set :as set]
   [datalevin.bits :as b]
   [datalevin.constants :as c]
   [datalevin.db :as db]
   [datalevin.inline :refer [update assoc]]
   [datalevin.join :as j]
   [datalevin.lmdb :as l]
   [datalevin.pipe :as p]
   [datalevin.query.access :as qaccess]
   [datalevin.query.optimizer.range :as qor]
   [datalevin.query.resolve :as qresolve]
   [datalevin.query-util :as qu]
   [datalevin.relation :as r]
   [datalevin.timeout :as timeout]
   [datalevin.util :as u])
  (:import
   [java.util AbstractCollection Collection Collections HashSet List]
   [java.util.concurrent Callable ExecutorService Executors Future]
   [datalevin.db DB]
   [datalevin.storage Store]
   [org.eclipse.collections.impl.list.mutable FastList]))

(def ^:dynamic *explain* nil)

(def ^:dynamic *intermediate-counts?* true)

(def ^:dynamic *start-time* nil)

(defrecord Context [parsed-q rels sources rules opt-clauses late-clauses
                    optimizable-or-joins graph plan intermediates run?
                    result-set])

(defrecord Plan [steps cost size recency])

(defrecord AccessRootPlan
    [mode source residual-query demand work properties cost size fallback])

(defrecord ConventionalRootPlan
    [context properties cost size])

(defrecord PhysicalProperties
    [ordering resumable? complete? quality capabilities])

(defrecord PlanAlternative
    [kind logical-key properties plan cost size payload])

(defrecord PropertyMemo
    [logical-key demand alternatives selected subsets])

(defprotocol IStep
  (-type [step] "return the type of step as a keyword")
  (-execute [step db source] "execute query step and return tuples")
  (-execute-pipe [step db source sink] "execute as part of pipeline")
  (-sample [step db source] "sample the step, not all steps implement")
  (-explain [step context] "explain the query step"))

(declare cols->attrs execute-steps hash-join-execute hash-join-execute-into
         sip-execute-pipe sip-hash-join-execute index-semi-join-execute
         index-semi-join-execute-into)

(defn- read-access-prefix
  ([path demand bounds work source]
   (read-access-prefix path demand bounds work source nil))
  ([path demand bounds work source bindings]
   (let [cursor (qaccess/open-access
                  path demand bounds work source bindings)]
     (try
       (qaccess/next-batch cursor)
       (finally
         (qaccess/close-cursor cursor))))))

(defn- read-access-all
  ([path demand bounds work source]
   (read-access-all path demand bounds work source nil))
  ([path demand bounds work source bindings]
   (let [cursor (qaccess/open-access
                  path demand bounds (assoc work :max-candidates nil)
                  source bindings)
         tuples (FastList.)]
    (try
      (loop []
        (let [{batch-tuples :tuples
               :keys        [exhausted?]} (qaccess/next-batch cursor)]
          (.addAll tuples ^Collection batch-tuples)
          (if exhausted?
            tuples
            (recur))))
      (finally
        (qaccess/close-cursor cursor))))))

(defn- tuple-bindings
  [cols tuple]
  (let [get-value (if (u/array? tuple)
                    #(aget ^objects tuple (long %))
                    #(nth tuple %))]
    (persistent!
      (reduce-kv
        (fn [bindings i col]
          (assoc! bindings col (get-value i)))
        (transient {}) cols))))

(defn- merge-access-tuple
  [input-cols access-cols output-cols input-tuple access-tuple]
  (let [input-bindings  (tuple-bindings input-cols input-tuple)
        access-bindings (tuple-bindings access-cols access-tuple)
        shared          (set/intersection (set input-cols)
                                          (set access-cols))]
    (when (every? #(= (get input-bindings %)
                      (get access-bindings %))
                  shared)
      (let [bindings (merge input-bindings access-bindings)]
        (object-array (map bindings output-cols))))))

(defn- read-correlated-all
  [path demand bounds work db input-cols access-cols output-cols source]
  (let [tuples (FastList.)]
    (doseq [input-tuple source]
      (let [bindings   (tuple-bindings input-cols input-tuple)
            candidates (read-access-all
                         path demand bounds work db bindings)]
        (doseq [access-tuple candidates]
          (when-let [tuple
                     (merge-access-tuple input-cols access-cols output-cols
                                         input-tuple access-tuple)]
            (.add tuples tuple)))))
    tuples))

(defrecord AccessStep
    [expr path demand bounds work in out cols input-cols access-cols
     access-source strata seen-or-joins result sample]

  IStep
  (-type [_] :access)

  (-execute [_ db source]
    (let [db (or access-source db)]
      (or result
          (if (seq input-cols)
            (read-correlated-all path demand bounds work db input-cols
                                 access-cols cols source)
            (read-access-all path demand bounds work db)))))

  (-execute-pipe [_ db source sink]
    (let [db (or access-source db)]
      (p/add-batch sink
                   (or result
                       (if (seq input-cols)
                         (read-correlated-all
                           path demand bounds work db input-cols access-cols
                           cols source)
                         (read-access-all path demand bounds work db))))))

  (-sample [_ db _source]
    (or sample
        (:tuples
          (read-access-prefix
            path demand bounds
            (assoc work :sample-size (or (:sample-size work)
                                         (:batch-size work)))
            (or access-source db)))))

  (-explain [_ _]
    (str "Access " cols " by " (:method expr) "/" (:strategy path)
         (when-let [required-count (:required-count demand)]
           (str " for " required-count " candidates"))
         (when-let [source-limit (:limit bounds)]
           (str " within source limit " source-limit))
         (when-let [ordering (:ordering demand)]
           (str " ordered by " ordering))
         ".")))

(defn access-step
  ([expr path demand]
   (access-step expr path demand (qaccess/source-bounds)
                (qaccess/access-work) []))
  ([expr path demand work]
   (access-step expr path demand (qaccess/source-bounds) work []))
  ([expr path demand work input-cols]
   (access-step expr path demand (qaccess/source-bounds) work input-cols))
  ([expr path demand bounds work input-cols]
   (access-step expr path demand bounds work input-cols nil))
  ([expr path demand bounds work input-cols access-source]
   (let [access-cols (or
                       (:cols expr)
                       (throw
                         (ex-info "Access expression requires an output schema"
                                  {:expr expr})))
         input-cols  (vec input-cols)
         requires    (set (:requires expr))
         supplied    (set input-cols)]
     (when-not (set/subset? requires supplied)
       (throw
         (ex-info "Access expression requirements are not bound"
                  {:requires requires :supplied supplied})))
     (let [cols (into input-cols (remove supplied) access-cols)
           out  (set cols)]
       (->AccessStep expr path demand bounds work supplied out cols
                     input-cols access-cols access-source
                     [out] #{} nil nil)))))

(defn access-sample-batch
  [step db]
  (or (:sample-batch step)
      (read-access-prefix
        (:path step) (:demand step) (:bounds step)
        (assoc (:work step)
               :sample-size (or (get-in step [:work :sample-size])
                                (get-in step [:work :batch-size])))
        (or (:access-source step) db))))

(defrecord InitStep
    [attr pred val range vars in out know-e? cols strata seen-or-joins mcount
     result sample]

  IStep
  (-type [_] :init)

  (-execute [_ db _]
    (let [get-v? (< 1 (count vars))
          e      (first vars)]
      (if result
        result
        (cond
          know-e?
          (let [src (doto (FastList.) (.add (object-array [e])))]
            (if get-v?
              (db/-eav-scan-v-list db src 0 [[attr {:skip? false}]])
              src))
          (nil? val)
          (db/-init-tuples-list
            db attr (or range [[[:closed c/v0] [:closed c/vmax]]]) pred get-v?)
          :else
          (db/-init-tuples-list
            db attr [[[:closed val] [:closed val]]] nil false)))))

  (-execute-pipe [_ db _ sink]
    (let [get-v? (< 1 (count vars))
          e      (first vars)]
      (if result
        (p/add-batch sink result)
        (cond
          know-e?
          (let [pipe (if (and *explain* *intermediate-counts?*)
                       (p/counted-tuple-pipe)
                       (p/tuple-pipe))
                src  (doto ^Collection pipe
                       (.add (object-array [e]))
                       (p/finish))]
            (if get-v?
              (db/-eav-scan-v db src sink 0 [[attr {:skip? false}]])
              (p/drain-to src sink)))
          (nil? val)
          (db/-init-tuples
            db sink attr
            (or range [[[:closed c/v0] [:closed c/vmax]]]) pred get-v?)
          :else
          (db/-init-tuples
            db sink attr [[[:closed val] [:closed val]]] nil false)))))

  (-sample [_ db _]
    (let [get-v? (< 1 (count vars))]
      (cond
        (some? val)
        (db/-sample-init-tuples-list
          db attr mcount [[[:closed val] [:closed val]]] nil false)
        range (db/-sample-init-tuples-list db attr mcount range pred get-v?)
        :else (cond-> (db/-e-sample db attr)
                get-v?
                (#(db/-eav-scan-v-list db % 0
                                       [[attr {:skip? false :pred pred}]]))
                (not get-v?)
                (#(db/-eav-scan-v-list db % 0
                                       [[attr {:skip? true :pred pred}]]))))))

  (-explain [_ _]
    (str "Initialize " vars " " (cond
                                  know-e? "by a known entity id."

                                  (nil? val)
                                  (if range
                                    (str "by range " range " on " attr ".")
                                    (str "by " attr "."))

                                  (some? val)
                                  (str "by " attr " = " val ".")))))

(defrecord MergeScanStep [index attrs-v vars in out cols strata seen-or-joins
                          result sample]

  IStep
  (-type [_] :merge)

  (-execute [_ db source]
    (if result
      result
      (db/-eav-scan-v-list db source index attrs-v)))

  (-execute-pipe [_ db source sink]
    (if result
      (do (when source
            (loop []
              (when (p/produce source)
                (recur))))
          (p/add-batch sink result))
      (let [batch-size (long c/query-pipe-batch-size)]
        (if (zero? batch-size)
          (db/-eav-scan-v db source sink index attrs-v)
          (let [buffer (p/batch-buffer)]
            (loop []
              (if-let [tuple (p/produce source)]
                (do (.add buffer tuple)
                    (when (>= (.size buffer) batch-size)
                      (p/add-batch
                        sink (db/-eav-scan-v-list db buffer index attrs-v))
                      (.clear buffer))
                    (recur))
                (when (pos? (.size buffer))
                  (p/add-batch
                    sink (db/-eav-scan-v-list db buffer index attrs-v))))))))))

  (-sample [_ db tuples]
    (if (< 0 (.size ^List tuples))
      (db/-eav-scan-v-list db tuples index attrs-v)
      (FastList.)))

  (-explain [_ _]
    (if (seq vars)
      (str "Merge " (vec vars) " by scanning " (mapv first attrs-v) ".")
      (str "Filter by predicates on " (mapv first attrs-v) "."))))

(defrecord LinkStep [type index attr var fidx in out cols strata seen-or-joins]

  IStep
  (-type [_] :link)

  (-execute [_ db src]
    (cond
      (int? var) (db/-val-eq-scan-e-list db src index attr var)
      fidx       (db/-val-eq-filter-e-list db src index attr fidx)
      :else      (db/-val-eq-scan-e-list db src index attr)))

  (-execute-pipe [_ db src sink]
    (let [batch-size (long c/query-pipe-batch-size)]
      (if (zero? batch-size)
        (cond
          (int? var) (db/-val-eq-scan-e db src sink index attr var)
          fidx       (db/-val-eq-filter-e db src sink index attr fidx)
          :else      (db/-val-eq-scan-e db src sink index attr))
        (let [buffer (p/batch-buffer)]
          (loop []
            (if-let [tuple (p/produce src)]
              (do (.add buffer tuple)
                  (when (>= (.size buffer) batch-size)
                    (p/add-batch
                      sink
                      (cond
                        (int? var)
                        (db/-val-eq-scan-e-list db buffer index attr var)
                        fidx
                        (db/-val-eq-filter-e-list db buffer index attr fidx)
                        :else
                        (db/-val-eq-scan-e-list db buffer index attr)))
                    (.clear buffer))
                  (recur))
              (when (pos? (.size buffer))
                (p/add-batch
                  sink
                  (cond
                    (int? var)
                    (db/-val-eq-scan-e-list db buffer index attr var)
                    fidx
                    (db/-val-eq-filter-e-list db buffer index attr fidx)
                    :else
                    (db/-val-eq-scan-e-list db buffer index attr))))))))))

  (-explain [_ _]
    (str "Obtain " var " by "
         (if (identical? type :_ref) "reverse reference" "equal values")
         " of " attr ".")))

(defrecord HashJoinStep [link link-e in out in-cols cols strata seen-or-joins
                         tgt-steps in-size tgt-size]

  IStep
  (-type [_] :hash-join)

  (-execute [_ db src]
    (let [use-sip? (and (identical? (:type link) :_ref)
                        (> (long tgt-size) (* (long in-size)
                                              (long c/sip-ratio-threshold))))]
      (if use-sip?
        (sip-hash-join-execute db link link-e in-cols tgt-steps src)
        (hash-join-execute db in-cols tgt-steps src))))

  (-execute-pipe [_ db src sink]
    (let [use-sip? (and (identical? (:type link) :_ref)
                        (> (long tgt-size) (* (long in-size)
                                              (long c/sip-ratio-threshold))))]
      (if use-sip?
        (let [input (FastList.)]
          (when src
            (loop []
              (when-let [tuple (p/produce src)]
                (.add input tuple)
                (recur))))
          (when (pos? (.size input))
            (sip-execute-pipe db link link-e in-cols tgt-steps input sink)))
        (let [tgt-rel (execute-steps nil db tgt-steps)
              input   (FastList.)]
          (when src
            (loop []
              (when-let [tuple (p/produce src)]
                (.add input tuple)
                (recur))))
          (hash-join-execute-into in-cols tgt-rel input sink)))))

  (-explain [_ _]
    (let [use-sip? (and (identical? (:type link) :_ref)
                        (> (long tgt-size) (* (long in-size)
                                              (long c/sip-ratio-threshold))))]
      (str "Hash join to " (:tgt link) " by " (case (:type link)
                                                :_ref   "reverse reference"
                                                :val-eq "equal values"
                                                "link")
           (when use-sip? " with SIP") "."))))

(defrecord SemiJoinStep [in out in-cols cols strata seen-or-joins join-steps]

  IStep
  (-type [_] :semi-join)

  (-execute [_ db src]
    (index-semi-join-execute db in-cols join-steps src))

  (-execute-pipe [_ db src sink]
    (let [input (FastList.)]
      (when src
        (loop []
          (when-let [tuple (p/produce src)]
            (.add input tuple)
            (recur))))
      (index-semi-join-execute-into db in-cols join-steps input sink)))

  (-explain [_ _]
    "Semi-join by indexed link scan."))

(defrecord OrJoinStep [clause bound-var bound-idx free-vars tgt tgt-attr
                       sources rules in out cols strata seen-or-joins]

  IStep
  (-type [_] :or-join)

  (-execute [_ db tuples]
    (qresolve/or-join-execute-link db sources rules tuples clause bound-var
                                   bound-idx free-vars tgt-attr))

  (-execute-pipe [_ db src sink]
    (let [input (FastList.)]
      (when src
        (loop []
          (when-let [tuple (p/produce src)]
            (.add input tuple)
            (recur))))
      (qresolve/or-join-execute-link-into db sources rules input clause
                                          bound-var bound-idx free-vars
                                          tgt-attr sink)))

  (-explain [_ _]
    (str "Or-join from " bound-var " to " tgt " via " tgt-attr ".")))

(defrecord NotJoinStep [clause vars sources rules in out cols strata seen-or-joins]

  IStep
  (-type [_] :not-join)

  (-execute [_ _ tuples]
    (if (and tuples (pos? (.size ^List tuples)))
      (let [context {:sources sources
                     :rules   rules
                     :rels    [(r/relation! (cols->attrs cols) tuples)]}
            result  (binding [qu/*implicit-source* (get sources '$)]
                      (qresolve/resolve-clause context clause))
            rels    (:rels result)]
        (if (seq rels)
          (:tuples (if (< 1 (count rels))
                     (reduce j/hash-join rels)
                     (first rels)))
          (FastList.)))
      (FastList.)))

  (-execute-pipe [this db src sink]
    (let [input (FastList.)]
      (when src
        (loop []
          (when-let [tuple (p/produce src)]
            (.add input tuple)
            (recur))))
      (p/add-batch sink (-execute this db input))))

  (-explain [_ _]
    (str "Anti-join by " vars ".")))

(defrecord Node [links mpath mcount bound free])

(defrecord Link [type tgt var attrs attr])

(defn step-type [step]
  (-type step))

(defn step-execute [step db source]
  (timeout/assert-time-left)
  (let [result (-execute step db source)]
    (timeout/assert-time-left)
    result))

(defn step-execute-pipe [step db source sink]
  (timeout/assert-time-left)
  (let [result (-execute-pipe step db source sink)]
    (timeout/assert-time-left)
    result))

(defn step-sample [step db source]
  (timeout/assert-time-left)
  (let [result (-sample step db source)]
    (timeout/assert-time-left)
    result))

(defn step-explain [step context]
  (-explain step context))

(defn make-context
  [parsed-q run?]
  (Context. parsed-q [] {} {} [] nil nil nil nil (volatile! {}) run? nil))

(defn plan?
  [x]
  (instance? Plan x))

(defn cols->attrs
  [cols]
  (persistent!
    (reduce-kv
      (fn [m i col]
        (let [v (if (set? col)
                  (some #(when (symbol? %) %) col)
                  col)]
          (assoc! m v i)))
      (transient {}) cols)))

(defn hash-join-execute
  [db in-cols tgt-steps ^List tuples]
  (let [out (FastList. (.size tuples))]
    (hash-join-execute-into db in-cols tgt-steps tuples out)
    out))

(defn hash-join-execute-into
  ([db in-cols tgt-steps tuples sink]
   (when (and tuples (pos? (.size ^List tuples)))
     (let [tgt-rel (execute-steps nil db tgt-steps)]
       (hash-join-execute-into in-cols tgt-rel tuples sink))))
  ([in-cols tgt-rel tuples sink]
   (when (and tuples (pos? (.size ^List tuples)))
     (let [in-rel (r/relation! (cols->attrs in-cols) tuples)]
       (j/hash-join-into in-rel tgt-rel sink)))))

(defn find-index
  [a-or-v cols]
  (when a-or-v
    (u/index-of (fn [x] (if (set? x) (x a-or-v) (= x a-or-v))) cols)))

(defn- build-sip-bitmap
  "Build a 64-bit bitmap from the values at col-idx in input tuples"
  [^List input ^long col-idx]
  (let [bm (b/bitmap64)]
    (dotimes [i (.size input)]
      (let [tuple ^objects (.get input i)
            v     (aget tuple col-idx)]
        (when (integer? v)
          (b/bitmap64-add bm (long v)))))
    bm))

(defn- values->ranges
  "Convert a collection of values to single-value ranges"
  [values]
  (mapv (fn [v] [[:closed v] [:closed v]]) values))

(defn- compose-pred
  "Compose a new predicate with an existing one"
  [existing-pred new-pred]
  (if existing-pred
    (fn [v] (and (existing-pred v) (new-pred v)))
    new-pred))

(defn- find-attr-in-attrs-v
  "Find the index of attr in attrs-v and return [index opts]"
  [attrs-v attr]
  (reduce-kv
    (fn [_ i [a opts]]
      (when (= a attr)
        (reduced [i opts])))
    nil attrs-v))

(defn- modify-init-step-for-sip
  "Modify InitStep with SIP optimization - either ranges or bitmap pred"
  [init-step bm]
  (let [cardinality (b/bitmap64-cardinality bm)]
    (if (<= cardinality ^long c/sip-range-threshold)
      (let [values     (b/bitmap64->longs bm)
            new-ranges (values->ranges values)
            old-range  (:range init-step)]
        (assoc init-step :range (if old-range
                                  (qor/intersect-ranges old-range new-ranges)
                                  new-ranges)))
      (let [min-v     (b/bitmap64-min bm)
            max-v     (b/bitmap64-max bm)
            new-range [[[:closed min-v] [:closed max-v]]]
            old-range (:range init-step)
            bm-pred   (fn [v] (b/bitmap64-contains? bm v))
            old-pred  (:pred init-step)]
        (assoc init-step
               :range (if old-range
                        (qor/intersect-ranges old-range new-range)
                        new-range)
               :pred (compose-pred old-pred bm-pred))))))

(defn- modify-merge-scan-step-for-sip
  "Modify MergeScanStep attrs-v to add bitmap predicate for join attr"
  [merge-step bm join-attr]
  (let [attrs-v (:attrs-v merge-step)
        bm-pred (fn [v] (b/bitmap64-contains? bm v))
        new-attrs-v
        (mapv (fn [[a opts :as entry]]
                (if (= a join-attr)
                  [a (update opts :pred #(compose-pred % bm-pred))]
                  entry))
              attrs-v)]
    (assoc merge-step :attrs-v new-attrs-v)))

(defn- apply-sip-to-tgt-steps
  "Apply SIP optimization to target steps for :_ref link type"
  [tgt-steps bm join-attr]
  (let [init-step (first tgt-steps)
        init-attr (:attr init-step)]
    (if (= init-attr join-attr)
      (assoc (vec tgt-steps) 0
             (modify-init-step-for-sip init-step bm))
      (if (< 1 (count tgt-steps))
        (let [merge-step (second tgt-steps)
              attrs-v    (:attrs-v merge-step)]
          (if (find-attr-in-attrs-v attrs-v join-attr)
            (assoc (vec tgt-steps) 1
                   (modify-merge-scan-step-for-sip merge-step bm join-attr))
            tgt-steps))
        tgt-steps))))

(defn sip-execute-pipe
  "Execute hash join with SIP (Sideways Information Passing) optimization.
   Called when SIP is determined to be beneficial."
  [db link link-e in-cols tgt-steps ^FastList input sink]
  (let [join-attr   (:attr link)
        col-idx     (find-index link-e in-cols)
        bm          (build-sip-bitmap input col-idx)
        cardinality (b/bitmap64-cardinality bm)]
    (when (pos? cardinality)
      (let [modified-tgt-steps (apply-sip-to-tgt-steps tgt-steps bm join-attr)
            tgt-rel            (execute-steps nil db modified-tgt-steps)]
        (hash-join-execute-into in-cols tgt-rel input sink)))))

(defn sip-hash-join-execute
  "Execute hash join with SIP optimization (for -execute path)"
  [db link link-e in-cols tgt-steps input]
  (when (and input (pos? (.size ^List input)))
    (let [join-attr   (:attr link)
          col-idx     (find-index link-e in-cols)
          bm          (build-sip-bitmap input col-idx)
          cardinality (b/bitmap64-cardinality bm)]
      (if (pos? cardinality)
        (let [modified-tgt-steps (apply-sip-to-tgt-steps tgt-steps bm join-attr)
              tgt-rel            (execute-steps nil db modified-tgt-steps)
              out                (FastList. cardinality)]
          (hash-join-execute-into in-cols tgt-rel input out)
          out)
        (FastList.)))))

(def pipe-thread-pool (Executors/newCachedThreadPool))

(defn- save-intermediates
  [context steps ^objects sinks ^List tuples]
  (when-let [res (and *explain* *intermediate-counts?*
                      (:intermediates context))]
    (vswap! res merge
            (u/reduce-indexed
              (fn [m step i]
                (assoc m (:out step)
                       {:tuples-count
                        (let [sink (aget sinks i)]
                          (if (p/pipe? sink)
                            (p/total sink)
                            (.size ^Collection (p/remove-end-scan sink))))}))
              {(:out (peek steps)) {:tuples-count (.size tuples)}}
              (butlast steps)))))

(defn- writing?
  [db]
  (l/writing? (.-lmdb ^Store (.-store ^DB db))))

(defn- prefix-set-sink
  [^long width]
  (let [^HashSet keys (HashSet.)
        ^objects scratch (when (< 1 width) (object-array width))
        lookup (when scratch (r/array-lookup))
        add-prefix!
        (if (= width 1)
          (fn [^objects tuple]
            (.add keys (aget tuple 0)))
          (fn [^objects tuple]
            (dotimes [i width]
              (aset scratch i (aget tuple i)))
            (r/reset-array-lookup! lookup scratch)
            (if (.contains keys lookup)
              false
              (.add keys (r/wrap-array (aclone scratch))))))
        contains-prefix?
        (if (= width 1)
          (fn [^objects tuple]
            (.contains keys (aget tuple 0)))
          (fn [^objects tuple]
            (dotimes [i width]
              (aset scratch i (aget tuple i)))
            (r/reset-array-lookup! lookup scratch)
            (.contains keys lookup)))
        sink
        (proxy [AbstractCollection] []
          (iterator [] (.iterator (Collections/emptyList)))
          (size [] (.size keys))
          (add [tuple] (boolean (add-prefix! tuple))))]
    [sink contains-prefix?]))

(defn- execute-join-steps-into
  [db join-steps ^List tuples sink]
  (let [join-steps (vec join-steps)]
    (case (count join-steps)
      1
      (step-execute-pipe (first join-steps) db (p/list-tuple-pipe tuples) sink)

      2
      (if (writing? db)
        (let [middle (step-execute (first join-steps) db tuples)]
          (step-execute-pipe (peek join-steps) db
                             (p/list-tuple-pipe middle) sink))
        (let [middle   (p/tuple-pipe)
              bindings (get-thread-bindings)
              tasks    [^Callable
                        #(with-bindings bindings
                           (try
                             (step-execute-pipe
                               (first join-steps) db
                               (p/list-tuple-pipe tuples) middle)
                             (finally (p/finish middle))))
                        ^Callable
                        #(with-bindings bindings
                           (step-execute-pipe (peek join-steps) db middle
                                              sink))]]
          (doseq [^Future f (.invokeAll ^ExecutorService pipe-thread-pool tasks)]
            (.get f))))

      (u/raise "Unsupported indexed semi-join step count"
               {:step-count (count join-steps)}))))

(defn index-semi-join-execute-into
  [db in-cols join-steps tuples sink]
  (when (and tuples (pos? (.size ^List tuples)))
    (let [[prefix-sink contains-prefix?] (prefix-set-sink (count in-cols))]
      (execute-join-steps-into db join-steps tuples prefix-sink)
      (dotimes [i (.size ^List tuples)]
        (let [tuple (.get ^List tuples i)]
          (when (contains-prefix? tuple)
            (.add ^Collection sink tuple))))))
  sink)

(defn index-semi-join-execute
  [db in-cols join-steps tuples]
  (index-semi-join-execute-into
    db in-cols join-steps tuples
    (FastList. (int (if tuples (.size ^List tuples) 0)))))

(defn- pipelining
  [context db attrs steps n]
  (let [n-1    (dec ^long n)
        tuples (FastList. (int c/init-exec-size-threshold))
        pipes  (object-array (repeatedly n-1 #(if (and *explain*
                                                       *intermediate-counts?*)
                                                (p/counted-tuple-pipe)
                                                (p/tuple-pipe))))
        work   (fn [step ^long i]
                 (if (zero? i)
                   (step-execute-pipe step db nil (aget pipes 0))
                   (let [src (aget pipes (dec i))]
                     (if (= i n-1)
                       (step-execute-pipe step db src tuples)
                       (step-execute-pipe step db src (aget pipes i))))))
        finish #(when (not= % n-1) (p/finish (aget pipes %)))]
    (if (writing? db)
      (dotimes [i n]
        (let [step (nth steps i)]
          (try
            (work step i)
            (finally (finish i)))))
      (let [bindings (get-thread-bindings)
            tasks    (mapv (fn [step i]
                             ^Callable
                             #(with-bindings bindings
                                (try
                                  (work step i)
                                  (finally
                                    (finish i)))))
                           steps (range))]
        (doseq [^Future f (.invokeAll ^ExecutorService pipe-thread-pool tasks)]
          (.get f))))
    (p/remove-end-scan tuples)
    (save-intermediates context steps pipes tuples)
    (r/relation! attrs tuples)))

(defn execute-steps
  "Execute all steps of a component's plan to obtain a relation."
  [context db steps]
  (let [steps (vec steps)
        n     (count steps)
        attrs (cols->attrs (:cols (peek steps)))]
    (case n
      1 (let [tuples (step-execute (first steps) db nil)]
          (save-intermediates context steps nil tuples)
          (r/relation! attrs tuples))
      2 (let [src    (step-execute (first steps) db nil)
              tuples (step-execute (peek steps) db src)]
          (save-intermediates context steps (object-array [src]) tuples)
          (r/relation! attrs tuples))
      (pipelining context db attrs steps n))))

(defn count-steps
  "Execute a component plan and count its output without retaining the final
  tuples. This is intended for offline exact-cardinality measurements."
  [db steps]
  (let [steps    (vec steps)
        n        (count steps)
        pipes    (object-array
                   (mapv (fn [i]
                           (if (= i (dec n))
                             (p/counted-tuple-pipe)
                             (p/tuple-pipe)))
                         (range n)))
        bindings (get-thread-bindings)
        workers
        (mapv
          (fn [step i]
            ^Callable
            #(with-bindings bindings
               (try
                 (step-execute-pipe
                   step db
                   (when (pos? i) (aget pipes (dec i)))
                   (aget pipes i))
                 (finally
                   (p/finish (aget pipes i))))))
          steps (range))
        output   (aget pipes (dec n))
        drain    ^Callable
        #(with-bindings bindings
           (loop []
             (when (p/produce output)
               (recur))))]
    (when (zero? n)
      (u/raise "Cannot count an empty query plan" {}))
    (when (writing? db)
      (u/raise "Exact plan counting requires a read-only database" {}))
    (doseq [^Future f (.invokeAll ^ExecutorService pipe-thread-pool
                                  (conj workers drain))]
      (.get f))
    (p/total output)))

(defn step-attrs
  "Return the relation attributes produced by the final step in `steps`."
  [steps]
  (let [steps (vec steps)]
    (when (empty? steps)
      (u/raise "Cannot inspect an empty query plan" {}))
    (cols->attrs (:cols (peek steps)))))

(defn reduce-step-batches
  "Execute a component plan and reduce bounded batches of its output. The
  reducing function receives the accumulator and a `FastList` of tuples. This
  is intended for offline measurements that must apply post-plan clauses
  without retaining the complete intermediate relation."
  [db steps batch-size rf init]
  (let [steps      (vec steps)
        n          (count steps)
        batch-size (long batch-size)
        pipes      (object-array (repeatedly n p/tuple-pipe))
        bindings   (get-thread-bindings)
        workers
        (mapv
          (fn [step i]
            ^Callable
            #(with-bindings bindings
               (try
                 (step-execute-pipe
                   step db
                   (when (pos? i) (aget pipes (dec i)))
                   (aget pipes i))
                 (finally
                   (p/finish (aget pipes i))))))
          steps (range))
        output     (aget pipes (dec n))
        drain      ^Callable
        #(with-bindings bindings
           (loop [acc   init
                  batch (FastList. (int batch-size))]
             (if-some [tuple (p/produce output)]
               (do
                 (.add batch tuple)
                 (if (>= (.size batch) batch-size)
                   (recur (rf acc batch) (FastList. (int batch-size)))
                   (recur acc batch)))
               (if (pos? (.size batch)) (rf acc batch) acc))))]
    (when (zero? n)
      (u/raise "Cannot reduce an empty query plan" {}))
    (when-not (pos? batch-size)
      (u/raise "Plan reduction batch size must be positive"
               {:batch-size batch-size}))
    (when (writing? db)
      (u/raise "Exact plan reduction requires a read-only database" {}))
    (let [^java.util.List futures
          (.invokeAll ^ExecutorService pipe-thread-pool
                      (conj workers drain))]
      (doseq [^Future f (butlast futures)]
        (.get f))
      (.get ^Future (.get futures (unchecked-dec-int (.size futures)))))))
