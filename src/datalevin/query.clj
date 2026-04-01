(ns ^:no-doc datalevin.query
  "Datalog query entry points."
  (:require
   [datalevin.db :as db]
   [datalevin.query.cache :as qcache]
   [datalevin.query.execute :as qexec]
   [datalevin.query.plan :as qplan]
   [datalevin.remote :as rt])
  (:import
   [datalevin.db DB]
   [datalevin.parser FindRel]
   [datalevin.remote DatalogStore]))

(defn- apply-limit-offset
  [parsed-q result]
  (if (instance? FindRel (:qfind parsed-q))
    (let [limit  (:qlimit parsed-q)
          offset (:qoffset parsed-q)]
      (->> result
           (#(if offset (drop offset %) %))
           (#(if (or (nil? limit) (= limit -1)) % (take limit %)))))
    result))

(defn- perform
  [q & inputs]
  (let [parsed-q (qcache/parsed-q q)]
    (qexec/mark-parsing-finished!)
    (apply-limit-offset parsed-q (qcache/q-result parsed-q inputs))))

(defn- plan-only
  [q & inputs]
  (let [parsed-q (qcache/parsed-q q)]
    (qexec/mark-parsing-finished!)
    (qexec/plan* parsed-q inputs)))

(defn- explain*
  [{:keys [run?] :or {run? false}} & args]
  (binding [qplan/*explain*    (volatile! {})
            qcache/*cache?*    false
            qplan/*start-time* (System/nanoTime)]
    (if run?
      (do (apply perform args) @qplan/*explain*)
      (do (apply plan-only args)
          (dissoc @qplan/*explain* :actual-result-size :execution-time)))))

(defn- only-remote-db
  "Return [remote-db [updated-inputs]] if the inputs contain only one db
  and its backing store is a remote one, where the remote-db in the inputs is
  replaced by `:remote-db-placeholder, otherwise return `nil`"
  [inputs]
  (let [dbs (filter db/-searchable? inputs)]
    (when-let [rdb (first dbs)]
      (let [rstore (.-store ^DB rdb)]
        (when (and (= 1 (count dbs))
                   (instance? DatalogStore rstore)
                   (db/db? rdb))
          [rstore (vec (replace {rdb :remote-db-placeholder} inputs))])))))

(defn q
  [query & inputs]
  (if-let [[store inputs'] (only-remote-db inputs)]
    (rt/q store query inputs')
    (apply perform query inputs)))

(defn explain
  [opts query & inputs]
  (if-let [[store inputs'] (only-remote-db inputs)]
    (rt/explain store opts query inputs')
    (apply explain* opts query inputs)))

(comment
;;
;; Copyright (c) Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns ^:no-doc datalevin.query
  "Datalog query engine"
  (:refer-clojure :exclude [update assoc])
  (:require
   [clojure.set :as set]
   [clojure.string :as str]
   [clojure.core.reducers :as rd]
   [clojure.walk :as w]
   [datalevin.db :as db]
   [datalevin.lmdb :as l]
   [datalevin.query.cache :as qcache]
   [datalevin.query.execute :as qexec]
   [datalevin.query.resolve :as qresolve]
   [datalevin.query-util :as qu]
   [datalevin.query-optimizer :as qo]
   [datalevin.relation :as r]
   [datalevin.join :as j]
   [datalevin.rules :as rules]
   [datalevin.storage :as s]
   [datalevin.built-ins :as built-ins]
   [datalevin.util :as u :refer [cond+ raise conjv concatv map+]]
   [datalevin.inline :refer [update assoc]]
   [datalevin.spill :as sp]
   [datalevin.pipe :as p]
   [datalevin.remote :as rt]
   [datalevin.parser :as dp]
   [datalevin.pull-api :as dpa]
   [datalevin.constants :as c]
   [datalevin.interface
    :refer [av-size dir db-name]])
  (:import
   [java.util Arrays List Collection Comparator HashSet HashMap]
   [java.util.concurrent ConcurrentHashMap ExecutorService Executors Future
    Callable]
   [datalevin.utl LikeFSM LRUCache]
   [datalevin.remote DatalogStore]
   [datalevin.db DB]
   [datalevin.relation Relation]
   [datalevin.storage Store]
   [datalevin.parser And BindColl BindIgnore BindScalar BindTuple Constant
    DefaultSrc FindColl FindRel FindScalar FindTuple Function Or PlainSymbol
    RulesVar SrcVar Variable Pattern Predicate Not RuleExpr]
   [org.eclipse.collections.impl.list.mutable FastList]))

(declare resolve-clause execute-steps hash-join-execute
         hash-join-execute-into sip-execute-pipe sip-hash-join-execute
         cols->attrs q* q-result perform plan-only explain*)

(def ^:dynamic *cache?* true)

;; Records

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
          (let [pipe (if qexec/*explain*
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

(declare sip-execute-pipe)

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

(declare or-join-execute-link or-join-execute-link-into)

(defrecord OrJoinStep [clause bound-var bound-idx free-vars tgt tgt-attr
                       sources rules in out cols strata seen-or-joins]

  IStep
  (-type [_] :or-join)

  (-execute [_ db tuples]
    (or-join-execute-link db sources rules tuples clause bound-var
                          bound-idx free-vars tgt-attr))

  (-execute-pipe [_ db src sink]
    (let [input (FastList.)]
      (when src
        (loop []
          (when-let [tuple (p/produce src)]
            (.add input tuple)
            (recur))))
      (or-join-execute-link-into db sources rules input clause bound-var
                                 bound-idx free-vars tgt-attr sink)))

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
                      (resolve-clause context clause))
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

(defrecord OrJoinLink [type tgt clause bound-var free-vars tgt-attr source])

(defrecord Clause [attr val var range count pred])

(def resolve-ins qresolve/resolve-ins)

(def resolve-pattern-lookup-refs qresolve/resolve-pattern-lookup-refs)

(def make-call qresolve/make-call)

(def resolve-pred qresolve/resolve-pred)

(def context-resolve-val qresolve/context-resolve-val)

(def resolve-sym qresolve/resolve-sym)

(def single qresolve/single)

(def collapse-rels qresolve/collapse-rels)

(defn resolve-clause
  [context clause]
  (qresolve/resolve-clause context clause))

(defn- or-join-build
  [sources rules tuples clause bound-var bound-idx free-vars]
  (qresolve/or-join-build sources rules tuples clause bound-var bound-idx
                          free-vars))

(defn- or-join-execute-link
  [db sources rules tuples clause bound-var bound-idx free-vars tgt-attr]
  (qresolve/or-join-execute-link db sources rules tuples clause bound-var
                                 bound-idx free-vars tgt-attr))

(defn- or-join-execute-link-into
  [db sources rules tuples clause bound-var bound-idx free-vars tgt-attr sink]
  (qresolve/or-join-execute-link-into db sources rules tuples clause bound-var
                                      bound-idx free-vars tgt-attr sink))

(defn- execute-step-deps
  []
  {:step-execute      -execute
   :step-execute-pipe -execute-pipe
   :step-explain      -explain})

(defn- cols->attrs
  [cols]
  (qexec/cols->attrs cols))

(defn- hash-join-execute
  [db in-cols tgt-steps tuples]
  (binding [qexec/*execute-deps* (execute-step-deps)]
    (qexec/hash-join-execute db in-cols tgt-steps tuples)))

(defn- hash-join-execute-into
  [& args]
  (binding [qexec/*execute-deps* (execute-step-deps)]
    (apply qexec/hash-join-execute-into args)))

(defn- sip-execute-pipe
  [db link link-e in-cols tgt-steps input sink]
  (binding [qexec/*execute-deps* (execute-step-deps)]
    (qexec/sip-execute-pipe db link link-e in-cols tgt-steps input sink)))

(defn- sip-hash-join-execute
  [db link link-e in-cols tgt-steps input]
  (binding [qexec/*execute-deps* (execute-step-deps)]
    (qexec/sip-hash-join-execute db link link-e in-cols tgt-steps input)))

(defn- execute-steps
  [context db steps]
  (binding [qexec/*execute-deps* (execute-step-deps)]
    (qexec/execute-steps context db steps)))

;; optimizer

(def ^:private plugin-inputs qo/plugin-inputs)

(def ^:private rewrite-unused-vars qo/rewrite-unused-vars)

(def intersect-ranges qo/intersect-ranges)

(def ^:private find-index qo/find-index)

(def ^:private writing? qo/writing?)

(def ^:private unused-var-replacements qo/unused-var-replacements)

(def combine-ranges qo/combine-ranges)

(def flip-ranges qo/flip-ranges)

(def ^:private optimizer-deps
  {:resolve-pattern-lookup-refs resolve-pattern-lookup-refs
   :make-call make-call
   :resolve-pred resolve-pred
   :or-join-execute-link or-join-execute-link
   :execute-steps execute-steps
   :resolve-clause resolve-clause
   :step-sample -sample
   :step-execute -execute
   :step-type -type
   :plan-cache qexec/*plan-cache*
   :map->Clause map->Clause
   :map->Node map->Node
   :link (fn [type tgt var attrs attr] (Link. type tgt var attrs attr))
   :or-join-link (fn [type tgt clause bound-var free-vars tgt-attr source]
                   (OrJoinLink. type tgt clause bound-var free-vars tgt-attr source))
   :map->InitStep map->InitStep
   :merge-scan-step
   (fn [index attrs-v vars in out cols strata seen-or-joins result sample]
     (MergeScanStep. index attrs-v vars in out cols strata seen-or-joins result sample))
   :link-step
   (fn [type index attr var fidx in out cols strata seen-or-joins]
     (LinkStep. type index attr var fidx in out cols strata seen-or-joins))
   :hash-join-step
   (fn [link link-e in out in-cols cols strata seen-or-joins tgt-steps in-size tgt-size]
     (HashJoinStep. link link-e in out in-cols cols strata seen-or-joins tgt-steps in-size tgt-size))
   :or-join-step
   (fn [clause bound-var bound-idx free-vars tgt tgt-attr sources rules in out cols strata seen-or-joins]
     (OrJoinStep. clause bound-var bound-idx free-vars tgt tgt-attr sources rules in out cols strata seen-or-joins))
   :not-join-step
   (fn [clause vars sources rules in out cols strata seen-or-joins]
     (NotJoinStep. clause vars sources rules in out cols strata seen-or-joins))
   :plan (fn [steps cost size recency] (Plan. steps cost size recency))})

(defn- build-graph
  [context]
  (qo/build-graph optimizer-deps context))

(defn- build-plan
  [context]
  (qo/build-plan optimizer-deps context))

(defn- plan-not-joins
  [context]
  (qo/plan-not-joins optimizer-deps context))

(defn- build-explain
  []
  (when qexec/*explain*
    (let [{:keys [^long parsing-time]} @qexec/*explain*]
      (vswap! qexec/*explain* assoc :building-time
              (- ^long (System/nanoTime)
                 (+ ^long qexec/*start-time* parsing-time))))))

;; TODO improve plan cache
(defn- planning
  [context]
  (-> context
      build-graph
      ((fn [c] (build-explain) c))
      build-plan
      plan-not-joins))

(defn- make-context
  [parsed-q run?]
  (Context. parsed-q [] {} {} [] nil nil nil nil (volatile! {}) run? nil))

(defn- execute-deps
  []
  {:step-execute      -execute
   :step-execute-pipe -execute-pipe
   :step-explain      -explain
   :planning          planning
   :make-context      make-context
   :plan?             #(instance? Plan %)
   :parsed-q          qcache/parsed-q
   :q-result          q-result})

(defn- cache-deps
  []
  {:q*             q*
   :cache-enabled? (fn [] *cache?*)})

(defn- q*
  [parsed-q inputs]
  (binding [qexec/*execute-deps* (execute-deps)]
    (qexec/q* parsed-q inputs)))

(defn- q-result
  [parsed-q inputs]
  (binding [qcache/*cache-deps* (cache-deps)]
    (qcache/q-result parsed-q inputs)))

(defn- perform
  [q & inputs]
  (binding [qexec/*execute-deps* (execute-deps)]
    (apply qexec/perform q inputs)))

(defn- plan-only
  [q & inputs]
  (binding [qexec/*execute-deps* (execute-deps)]
    (apply qexec/plan-only q inputs)))

(defn- explain*
  [opts & args]
  (binding [*cache?* false
            qexec/*execute-deps* (execute-deps)]
    (apply qexec/explain* opts args)))

(defn- only-remote-db
  "Return [remote-db [updated-inputs]] if the inputs contain only one db
  and its backing store is a remote one, where the remote-db in the inputs is
  replaced by `:remote-db-placeholder, otherwise return `nil`"
  [inputs]
  (let [dbs (filter db/-searchable? inputs)]
    (when-let [rdb (first dbs)]
      (let [rstore (.-store ^DB rdb)]
        (when (and (= 1 (count dbs))
                   (instance? DatalogStore rstore)
                   (db/db? rdb))
          [rstore (vec (replace {rdb :remote-db-placeholder} inputs))])))))

(defn q
  [query & inputs]
  (if-let [[store inputs'] (only-remote-db inputs)]
    (rt/q store query inputs')
    (apply perform query inputs)))

(defn explain
  [opts query & inputs]
  (if-let [[store inputs'] (only-remote-db inputs)]
    (rt/explain store opts query inputs')
    (apply explain* opts query inputs)))
)
