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
   [datalevin.bits :as b]
   [datalevin.constants :as c]
   [datalevin.db :as db]
   [datalevin.inline :refer [update assoc]]
   [datalevin.join :as j]
   [datalevin.lmdb :as l]
   [datalevin.pipe :as p]
   [datalevin.query.optimizer.range :as qor]
   [datalevin.query.resolve :as qresolve]
   [datalevin.query-util :as qu]
   [datalevin.relation :as r]
   [datalevin.util :as u])
  (:import
   [java.util Collection List]
   [java.util.concurrent Callable ExecutorService Executors Future]
   [datalevin.db DB]
   [datalevin.storage Store]
   [org.eclipse.collections.impl.list.mutable FastList]))

(def ^:dynamic *explain* nil)

(def ^:dynamic *start-time* nil)

(defrecord Context [parsed-q rels sources rules opt-clauses late-clauses
                    optimizable-or-joins graph plan intermediates run?
                    result-set])

(defrecord Plan [steps cost size recency])

(defprotocol IStep
  (-type [step] "return the type of step as a keyword")
  (-execute [step db source] "execute query step and return tuples")
  (-execute-pipe [step db source sink] "execute as part of pipeline")
  (-sample [step db source] "sample the step, not all steps implement")
  (-explain [step context] "explain the query step"))

(declare cols->attrs execute-steps hash-join-execute hash-join-execute-into
         sip-execute-pipe sip-hash-join-execute)

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
        (.addAll ^Collection sink result)
        (cond
          know-e?
          (let [pipe (if *explain*
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
          (.addAll ^Collection sink result))
      (let [batch-size (long c/query-pipe-batch-size)]
        (if (zero? batch-size)
          (db/-eav-scan-v db source sink index attrs-v)
          (let [buffer (p/batch-buffer)]
            (loop []
              (if-let [tuple (p/produce source)]
                (do (.add buffer tuple)
                    (when (>= (.size buffer) batch-size)
                      (.addAll ^Collection sink
                               (db/-eav-scan-v-list db buffer index attrs-v))
                      (.clear buffer))
                    (recur))
                (when (pos? (.size buffer))
                  (.addAll ^Collection sink
                           (db/-eav-scan-v-list db buffer index attrs-v))))))))))

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
                    (.addAll
                      ^Collection sink
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
                (.addAll
                  ^Collection sink
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
      (.addAll ^Collection sink (-execute this db input))))

  (-explain [_ _]
    (str "Anti-join by " vars ".")))

(defrecord Node [links mpath mcount bound free])

(defrecord Link [type tgt var attrs attr])

(defn step-type [step]
  (-type step))

(defn step-execute [step db source]
  (-execute step db source))

(defn step-execute-pipe [step db source sink]
  (-execute-pipe step db source sink))

(defn step-sample [step db source]
  (-sample step db source))

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
  (reduce-kv
    (fn [m i col]
      (let [v (if (set? col)
                (some #(when (symbol? %) %) col)
                col)]
        (assoc m v i)))
    {} cols))

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
  (when-let [res (:intermediates context)]
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

(defn- pipelining
  [context db attrs steps n]
  (let [n-1    (dec ^long n)
        tuples (FastList. (int c/init-exec-size-threshold))
        pipes  (object-array (repeatedly n-1 #(if *explain*
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
