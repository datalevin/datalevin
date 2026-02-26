;;
;; Copyright (c) Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns ^:no-doc datalevin.storage
  "Storage layer of Datalog store"
  (:refer-clojure :exclude [update assoc])
  (:require
   [datalevin.lmdb :as lmdb :refer [IWriting]]
   [datalevin.binding.cpp]
   [datalevin.inline :refer [update assoc]]
   [datalevin.util :as u :refer [conjs conjv]]
   [datalevin.relation :as r]
   [datalevin.bits :as b]
   [datalevin.pipe :as p]
   [datalevin.scan :as scan :refer [visit-list*]]
   [datalevin.search :as s]
   [datalevin.idoc :as idoc]
   [datalevin.vector :as v]
   [datalevin.prepare :as prep]
   [datalevin.constants :as c]
   [datalevin.datom :as d]
   [datalevin.async :as a]
   [datalevin.index :as idx
    :refer [value-type datom->indexable index->dbi index->ktype index->vtype
            index->k index->v gt->datom retrieved->v encode-giant-datom]]
   [datalevin.validate :as vld]
   [datalevin.interface
    :refer [transact-kv get-range get-first get-value visit-list-sample
            visit-list-key-range near-list env-dir close-kv closed-kv?
            visit entries list-range list-range-first list-range-count
            list-count key-range-list-count key-range-count rschema
            list-range-first-n get-list list-range-filter-count max-aid
            list-range-some list-range-keep visit-list-range max-gt max-tx
            open-list-dbi open-dbi attrs add-doc remove-doc opts swap-attr
            add-vec remove-vec schema closed? a-size db-name populated?
            get-env-flags set-env-flags]]
   [clojure.string :as str])
  (:import
   [java.util List Comparator Collection HashMap UUID]
   [java.util.concurrent TimeUnit ScheduledExecutorService ConcurrentHashMap
    ScheduledFuture]
   [java.util.concurrent.locks ReentrantReadWriteLock]
   [java.nio ByteBuffer]
   [java.lang AutoCloseable]
   [org.eclipse.collections.impl.list.mutable FastList]
   [org.eclipse.collections.impl.map.mutable.primitive LongObjectHashMap]
   [datalevin.datom Datom]
   [datalevin.interface IStore]
   [datalevin.async IAsyncWork]
   [datalevin.bits Retrieved Indexable]))

(defn- attr->properties [k v]
  (case v
    :db.unique/identity  [:db/unique :db.unique/identity]
    :db.unique/value     [:db/unique :db.unique/value]
    :db.cardinality/many [:db.cardinality/many]
    (case k
      :db/tupleAttrs [:db.type/tuple :db/tupleAttrs]
      :db/tupleType  [:db.type/tuple :db/tupleType]
      :db/tupleTypes [:db.type/tuple :db/tupleTypes]
      (cond
        (and (identical? :db/valueType k)
             (identical? :db.type/ref v)) [:db.type/ref]
        (and (identical? :db/isComponent k)
             (true? v))                   [:db/isComponent]
        :else                             []))))

(defn attr-tuples
  "e.g. :reg/semester => #{:reg/semester+course+student ...}"
  [schema rschema]
  (reduce
    (fn [m tuple-attr] ;; e.g. :reg/semester+course+student
      (u/reduce-indexed
        (fn [m src-attr idx] ;; e.g. :reg/semester
          (update m src-attr assoc tuple-attr idx))
        m ((schema tuple-attr) :db/tupleAttrs)))
    {} (rschema :db/tupleAttrs)))

(defn schema->rschema
  ":db/unique           => #{attr ...}
   :db.unique/identity  => #{attr ...}
   :db.unique/value     => #{attr ...}
   :db.cardinality/many => #{attr ...}
   :db.type/ref         => #{attr ...}
   :db/isComponent      => #{attr ...}
   :db.type/tuple       => #{attr ...}
   :db/tupleAttr        => #{attr ...}
   :db/tupleType        => #{attr ...}
   :db/tupleTypes       => #{attr ...}
   :db/attrTuples       => {attr => {tuple-attr => idx}}"
  [schema]
  (let [rschema (reduce-kv
                  (fn [rschema attr attr-schema]
                    (reduce-kv
                      (fn [rschema key value]
                        (reduce
                          (fn [rschema prop]
                            (update rschema prop conjs attr))
                          rschema (attr->properties key value)))
                      rschema attr-schema))
                  {} schema)]
    (assoc rschema :db/attrTuples (attr-tuples schema rschema))))

(defn- transact-schema
  [lmdb schema]
  (transact-kv
    lmdb
    (conj (for [[attr props] schema]
            (lmdb/kv-tx :put c/schema attr props :attr :data))
          (lmdb/kv-tx :put c/meta :last-modified
                      (System/currentTimeMillis) :attr :long))))

(defn- load-schema
  [lmdb]
  (into {} (get-range lmdb c/schema [:all] :attr :data)))

(defn- init-max-aid
  [schema]
  (inc ^long (apply max (map :db/aid (vals schema)))))

(defn- update-schema
  [old schema]
  (let [^long init-aid (init-max-aid old)
        i              (volatile! 0)]
    (into {}
          (map (fn [[attr props]]
                 (if-let [old-props (old attr)]
                   [attr (assoc props :db/aid (old-props :db/aid))]
                   (let [res [attr (assoc props :db/aid (+ init-aid ^long @i))]]
                     (vswap! i u/long-inc)
                     res))))
          schema)))

(defn- init-schema
  [lmdb schema]
  (when (empty? (load-schema lmdb))
    (transact-schema lmdb c/implicit-schema))
  (when schema
    (transact-schema lmdb (update-schema (load-schema lmdb) schema)))
  (let [now (load-schema lmdb)]
    (when-not (now :db/created-at)
      (transact-schema lmdb (update-schema now c/entity-time-schema))))
  (load-schema lmdb))

(defn- init-attrs [schema]
  (into {} (map (fn [[k v]] [(v :db/aid) k])) schema))

(defn- init-max-gt
  [lmdb]
  (or (when-let [gt (-> (get-first lmdb c/giants [:all-back] :id :ignore)
                        first)]
        (inc ^long gt))
      c/g0))

(defn- init-max-tx
  [lmdb]
  (or (get-value lmdb c/meta :max-tx :attr :long)
      c/tx0))

(defn e-aid-v->datom
  [store e-aid-v]
  (d/datom (nth e-aid-v 0) ((attrs store) (nth e-aid-v 1)) (peek e-aid-v)))

(defn- retrieved->attr [attrs ^Retrieved r] (attrs (.-a r)))

(defn- ave-kv->retrieved
  [lmdb ^Retrieved r ^long e]
  (Retrieved. e (.-a r) (retrieved->v lmdb r) (.-g r)))

(defn- kv->datom
  [lmdb attrs ^long k ^Retrieved v]
  (let [g (.-g v)]
    (if (= g c/normal)
      (d/datom k (attrs (.-a v)) (.-v v))
      (gt->datom lmdb g))))

(defn- retrieved->datom
  [lmdb attrs [k v :as kv]]
  (when kv
    (if (integer? k)
      (let [r ^Retrieved v]
        (if (.-g r)
          (kv->datom lmdb attrs k r)
          (d/datom (.-e r) (attrs (.-a r)) k)))
      (kv->datom lmdb attrs v k))))

(defn- ae-retrieved->datom
  [attrs v ^Retrieved r]
  (d/datom (.-e r) (attrs (.-a r)) v))

(defn- datom-pred->kv-pred
  [lmdb attrs index pred]
  (fn [kv]
    (let [k (b/read-buffer (lmdb/k kv) (index->ktype index))
          v (b/read-buffer (lmdb/v kv) (index->vtype index))]
      (pred (retrieved->datom lmdb attrs [k v])))))

(defn- ave-key-range
  [aid vt val-range]
  (let [[[cl lv] [ch hv]] val-range
        op                (cond
                            (and (identical? cl :closed)
                                 (identical? ch :closed)) :closed
                            (identical? ch :closed)       :open-closed
                            (identical? cl :closed)       :closed-open
                            :else                         :open)]
    [op (b/indexable nil aid lv vt c/gmax) (b/indexable nil aid hv vt c/gmax)]))

(defn- retrieve-ave
  [lmdb kv]
  (ave-kv->retrieved
    lmdb (b/read-buffer (lmdb/k kv) :avg) (b/read-buffer (lmdb/v kv) :id)))

(defn- ave-tuples-scan*
  [lmdb aid vt val-ranges sample-indices work]
  (if sample-indices
    (doseq [val-range val-ranges]
      (visit-list-sample
        lmdb c/ave sample-indices work (ave-key-range aid vt val-range) :avg :id))
    (doseq [val-range val-ranges]
      (visit-list-key-range
        lmdb c/ave work (ave-key-range aid vt val-range) :avg :id))))

(defn- ave-tuples-scan-need-v
  [lmdb ^Collection out aid vt val-ranges sample-indices]
  (ave-tuples-scan*
    lmdb aid vt val-ranges sample-indices
    (fn [kv]
      (let [^Retrieved r (retrieve-ave lmdb kv)]
        (.add out (object-array [(.-e r) (.-v r)]))))))

(defn- ave-tuples-scan-need-v-vpred
  [lmdb ^Collection out vpred aid vt val-ranges sample-indices]
  (ave-tuples-scan*
    lmdb aid vt val-ranges sample-indices
    (fn [kv]
      (let [^Retrieved r (retrieve-ave lmdb kv)
            v            (.-v r)]
        (when (vpred v)
          (.add out (object-array [(.-e r) v])))))))

(defn- ave-tuples-scan-no-v
  [lmdb ^Collection out aid vt val-ranges sample-indices]
  (ave-tuples-scan*
    lmdb aid vt val-ranges sample-indices
    (fn [kv]
      (.add out (object-array [(b/read-buffer (lmdb/v kv) :id)])))))

(defn- ave-tuples-scan-no-v-vpred
  [lmdb ^Collection out vpred aid vt val-ranges sample-indices]
  (ave-tuples-scan*
    lmdb aid vt val-ranges sample-indices
    (fn [kv]
      (let [^Retrieved r (retrieve-ave lmdb kv)
            v            (.-v r)]
        (when (vpred v)
          (.add out (object-array [(.-e r)])))))))

(defn- sort-tuples-by-eid
  [^List tuples ^long eid-idx]
  (doto tuples
    (.sort (reify Comparator
             (compare [_ a b]
               (Long/compare ^long (aget ^objects a eid-idx)
                             ^long (aget ^objects b eid-idx)))))))

(defn- sort-tuples-by-val
  [^List tuples ^long v-idx]
  (doto tuples
    (.sort (reify Comparator
             (compare [_ a b]
               (d/compare-with-type (aget ^objects a v-idx)
                                    (aget ^objects b v-idx)))))))

(defn- group-counts
  [aids]
  (sequence (comp (partition-by identity) (map count)) aids))

(defn- group-starts
  [counts]
  (int-array (->> counts (reductions +) butlast (into [0]))))

(defn- eav-scan-v-single*
  [lmdb iter na ^Collection out ^objects tuple eid-idx
   ^LongObjectHashMap seen ^ints aids ^objects preds ^objects fidxs
   ^booleans skips]
  (let [te        ^long (aget tuple eid-idx)
        has-fidx? (< 0 (alength fidxs))
        ts        (when-not has-fidx? (.get seen te))]
    (if ts
      (if (identical? ts :skip)
        (.add out tuple)
        (.add out (r/join-tuples tuple ts)))
      (let [vs (FastList. (int na))]
        (loop [next? (lmdb/seek-key iter te :id)
               ai    0]
          (if (and next? (< ^long ai ^long na))
            (let [vb (lmdb/next-val iter)
                  a  (b/read-buffer vb :int)]
              (if (== ^int a ^int (aget aids ai))
                (let [v    (retrieved->v lmdb (b/avg->r vb))
                      pred (aget preds ai)
                      fidx (aget fidxs ai)]
                  (if (and (or (nil? pred) (pred v))
                           (or (nil? fidx) (= v (aget tuple (int fidx)))))
                    (do (when-not (aget skips ai) (.add vs v))
                        (recur (lmdb/has-next-val iter) (u/long-inc ai)))
                    :reject))
                (recur (lmdb/has-next-val iter) ai)))
            (when (== ^long ai ^long na)
              (if (.isEmpty vs)
                (do (.put seen te :skip)
                    (.add out tuple))
                (let [vst (.toArray vs)]
                  (.put seen te vst)
                  (.add out (r/join-tuples tuple vst)))))))))))

(defn- eav-scan-v-multi*
  [lmdb iter na ^Collection out ^objects tuple eid-idx
   ^LongObjectHashMap seen ^ints aids ^objects preds ^objects fidxs
   ^booleans skips ^ints gstarts ^ints gcounts]
  (let [te        ^long (aget tuple eid-idx)
        has-fidx? (< 0 (alength fidxs))
        ts        (when-not has-fidx? (.get seen te))]
    (if ts
      (.addAll out (r/prod-tuples (r/single-tuples tuple) ts))
      (let [vs (object-array na)
            fa (aget aids 0)
            la (aget aids (dec ^long na))]
        (dotimes [i na] (aset vs i (FastList.)))
        (loop [next? (lmdb/seek-key iter te :id)
               gi    0
               pa    (int (aget aids 0))
               in?   false]
          (when next?
            (let [vb ^ByteBuffer (lmdb/next-val iter)
                  a  ^int (b/read-buffer vb :int)]
              (cond
                (< a ^int fa)
                (recur (lmdb/has-next-val iter) gi pa false)
                (<= a ^int la)
                (let [gi (if (== pa ^int a)
                           gi
                           (if in? (inc gi) gi))
                      s  (aget gstarts gi)]
                  (if (== ^int a ^int (aget aids s))
                    (let [v (retrieved->v lmdb (b/avg->r vb))]
                      (dotimes [i (aget gcounts gi)]
                        (let [aj   (+ s i)
                              pred (aget preds aj)
                              fidx (aget fidxs aj)]
                          (when (and (or (nil? pred) (pred v))
                                     (or (nil? fidx)
                                         (= v (aget tuple (int fidx)))))
                            (.add ^FastList (aget vs aj) v)
                            (when-not (aget skips gi)
                              (.add ^FastList (aget vs aj) v)))))
                      (recur (lmdb/has-next-val iter) gi (int a) true))
                    (recur (lmdb/has-next-val iter) gi pa false)))
                :else :done))))
        (when-not (some #(.isEmpty ^FastList %) vs)
          (let [vst (r/many-tuples (sequence
                                     (comp (map (fn [v s] (when-not s v)))
                                        (remove nil?))
                                     vs skips))]
            (.put seen te vst)
            (.addAll out (r/prod-tuples (r/single-tuples tuple)
                                        vst))))))))

(defn- val-eq-scan-e*
  [iter ^Collection out tuple ^HashMap seen aid v vt]
  (if-let [ts (.get seen v)]
    (when-not (identical? ts :no-result)
      (.addAll out (r/prod-tuples (r/single-tuples tuple) ts)))
    (let [ts (FastList.)]
      (visit-list* iter
                   (fn [vb] (.add ts (object-array [(b/read-buffer vb :id)])))
                   (b/indexable nil aid v vt nil) :avg vt true)
      (if (.isEmpty ts)
        (.put seen v :no-result)
        (do (.put seen v ts)
            (.addAll out (r/prod-tuples (r/single-tuples tuple) ts)))))))

(defn- val-eq-scan-e-bound*
  [iter ^Collection out tuple aid v vt bound]
  (visit-list* iter (fn [vb]
                      (let [e (b/read-buffer vb :id)]
                        (when (= ^long e ^long bound)
                          (.add out (r/conj-tuple tuple e)))))
               (b/indexable nil aid v vt nil) :avg vt true))

(defn- val-eq-filter-e*
  [iter ^Collection out tuple aid v vt old-e]
  (visit-list* iter
               (fn [vb]
                 (when (== ^long (b/read-buffer vb :id) ^long old-e)
                   (.add out tuple)))
               (b/indexable nil aid v vt nil) :avg vt true))

(defn- single-attrs?
  [schema attrs-v]
  (not-any? #(identical? (-> % schema :db/cardinality) :db.cardinality/many)
            (mapv first attrs-v)))

(defn- ea->r
  [schema lmdb e a]
  (when-let [aid (:db/aid (schema a))]
    (when-let [^ByteBuffer bf (near-list lmdb c/eav e aid :id :int)]
      (when (= ^int aid ^int (b/read-buffer bf :int))
        (b/read-buffer (.rewind bf) :avg)))))

(declare insert-datom delete-datom fulltext-index vector-index idoc-index check
         migrate-attr-values transact-opts ->SamplingWork e-sample*
         default-ratio* analyze*)

(deftype Store [lmdb
                search-engines
                vector-indices
                idoc-indices
                ^ConcurrentHashMap counts   ; aid -> touched times
                ^:volatile-mutable opts
                ^:volatile-mutable schema
                ^:volatile-mutable rschema
                ^:volatile-mutable attrs    ; aid -> attr
                ^:volatile-mutable max-aid
                ^:volatile-mutable max-gt
                ^:volatile-mutable max-tx
                scheduled-sampling
                write-txn
                ^ReentrantReadWriteLock sampling-lock]

  IWriting

  (write-txn [_] write-txn)

  IStore

  (opts [_] opts)

  (assoc-opt [_ k v]
    (let [k' (c/canonical-wal-option-key k)
          _  (vld/validate-option-mutation k' v)
          new-opts (-> opts
                       (dissoc k)
                       (assoc k' v))]
      (set! opts new-opts)
      (transact-opts lmdb new-opts)))

  (db-name [_] (:db-name opts))

  (dir [_] (env-dir lmdb))

  (close [this]
    (let [wlock (.writeLock sampling-lock)]
      (.lock wlock)
      (try
        (.stop-sampling this)
        (close-kv lmdb)
        (finally
          (.unlock wlock)))))

  (closed? [_] (closed-kv? lmdb))

  (last-modified [_] (get-value lmdb c/meta :last-modified :attr :long))

  (max-gt [_] max-gt)

  (advance-max-gt [_] (set! max-gt (inc ^long max-gt)))

  (max-tx [_] max-tx)

  (advance-max-tx [_] (set! max-tx (inc ^long max-tx)))

  (max-aid [_] max-aid)

  (schema [_] schema)

  (rschema [_] rschema)

  (set-schema [this new-schema]
    (when new-schema (vld/validate-schema new-schema))
    (doseq [[attr new] new-schema
            :let       [old (schema attr)]
            :when      old]
      (check this attr old new))
    (doseq [[attr new] new-schema
            :let       [old (schema attr)]
            :when      old
            :let       [old-vt (value-type old)
                        new-vt (value-type new)]
            :when      (and (identical? old-vt :data)
                            (not (identical? new-vt :data)))]
      ;; Re-encode stored values before persisting schema change.
      (migrate-attr-values this attr new-vt))
    (set! schema (init-schema lmdb new-schema))
    (set! rschema (schema->rschema schema))
    (set! attrs (init-attrs schema))
    (set! max-aid (init-max-aid schema))
    schema)

  (attrs [_] attrs)

  (init-max-eid [_]
    (let [e    (volatile! c/e0)
          read (fn [kv]
                 (when-let [res (b/read-buffer (lmdb/k kv) :id)]
                   (vreset! e res)
                   :datalevin/terminate-visit))]
      (visit lmdb c/eav read [:all-back])
      @e))

  (swap-attr [this attr f]
    (.swap-attr this attr f nil nil))
  (swap-attr [this attr f x]
    (.swap-attr this attr f x nil))
  (swap-attr [this attr f x y]
    (let [o (or (schema attr)
                (let [m {:db/aid max-aid}]
                  (set! max-aid (inc ^long max-aid))
                  m))
          p (cond
              (and x y) (f o x y)
              x         (f o x)
              :else     (f o))]
      (check this attr o p)
      (transact-schema lmdb {attr p})
      (set! schema (assoc schema attr p))
      (set! rschema (schema->rschema schema))
      (set! attrs (assoc attrs (p :db/aid) attr))
      p))

  (del-attr [this attr]
    (vld/validate-attr-deletable
      (.populated?
        this :ave (d/datom c/e0 attr c/v0) (d/datom c/emax attr c/vmax)))
    (let [aid ((schema attr) :db/aid)]
      (transact-kv
        lmdb [(lmdb/kv-tx :del c/schema attr :attr)
              (lmdb/kv-tx :put c/meta :last-modified
                          (System/currentTimeMillis) :attr :long)])
      (set! schema (dissoc schema attr))
      (set! rschema (schema->rschema schema))
      (set! attrs (dissoc attrs aid))
      attrs))

  (rename-attr [_ attr new-attr]
    (let [props (schema attr)]
      (transact-kv
        lmdb [(lmdb/kv-tx :del c/schema attr :attr)
              (lmdb/kv-tx :put c/schema new-attr props :attr)
              (lmdb/kv-tx :put c/meta :last-modified
                          (System/currentTimeMillis) :attr :long)])
      (set! schema (-> schema (dissoc attr) (assoc new-attr props)))
      (set! rschema (schema->rschema schema))
      (set! attrs (assoc attrs (props :db/aid) new-attr))
      attrs))

  (datom-count [_ index]
    (entries lmdb (if (string? index) index (index->dbi index))))

  (load-datoms [this datoms]
    (let [txs    (FastList. (* 3 (count datoms)))
          ;; fulltext [:a d [e aid v]], [:d d [e aid v]], [:g d [gt v]],
          ;; or [:r d gt]
          ft-ds  (FastList.)
          ;; vector, same
          vi-ds  (FastList.)
          ;; idoc [:a d [e aid v]], [:d d [e aid v]], [:g d [gt v]],
          ;; or [:r d [gt v]]
          id-ds  (FastList.)
          giants (HashMap.)]
      (locking (lmdb/write-txn lmdb)
        (doseq [datom datoms]
          (if (d/datom-added datom)
            (insert-datom this datom txs ft-ds vi-ds id-ds giants)
            (delete-datom this datom txs ft-ds vi-ds id-ds giants)))
        (.add txs (lmdb/kv-tx :put c/meta :max-tx
                              (.advance-max-tx this) :attr :long))
        (.add txs (lmdb/kv-tx :put c/meta :last-modified
                              (System/currentTimeMillis) :attr :long))
        (fulltext-index search-engines ft-ds)
        (vector-index vector-indices vi-ds)
        (idoc-index idoc-indices id-ds)
        (transact-kv lmdb txs))))

  (fetch [_ datom]
    (mapv #(retrieved->datom lmdb attrs %)
          (let [lk (index->k :eav schema datom false)
                hk (index->k :eav schema datom true)
                lv (index->v :eav schema datom false)
                hv (index->v :eav schema datom true)]
            (list-range lmdb (index->dbi :eav)
                        [:closed lk hk] :id [:closed lv hv] :avg))))

  (populated? [_ index low-datom high-datom]
    (let [lk (index->k index schema low-datom false)
          hk (index->k index schema high-datom true)
          lv (index->v index schema low-datom false)
          hv (index->v index schema high-datom true) ]
      (list-range-first
        lmdb (index->dbi index)
        [:closed lk hk] (index->ktype index)
        [:closed lv hv] (index->vtype index))))

  (size [_ index low-datom high-datom]
    (list-range-count
      lmdb (index->dbi index)
      [:closed
       (index->k index schema low-datom false)
       (index->k index schema high-datom true)] (index->ktype index)
      [:closed
       (index->v index schema low-datom false)
       (index->v index schema high-datom true)] (index->vtype index)))

  (e-size [_ e] (list-count lmdb c/eav e :id))

  (a-size [this a]
    (if (:db/aid (schema a))
      (when-not (.closed? this)
        (key-range-list-count
          lmdb c/ave
          [:closed
           (datom->indexable schema (d/datom c/e0 a nil) false)
           (datom->indexable schema (d/datom c/emax a nil) true)] :avg))
      0))

  (e-sample [this a]
    (let [aid ( :db/aid (schema a))]
      (or (when-let [res (not-empty
                           (get-range lmdb c/meta
                                      [:closed-open [aid 0]
                                       [aid c/init-exec-size-threshold]]
                                      :int-int :id))]
            (r/vertical-tuples (sequence (map peek) res)))
          (e-sample* this a aid))))

  (default-ratio [this a]
    (let [aid ( :db/aid (schema a))]
      (or (get-value lmdb c/meta [aid :ratio] :data :double)
          (default-ratio* this a aid))))

  (start-sampling [this]
    (when (:background-sampling? opts)
      (when-not @scheduled-sampling
        (let [scheduler ^ScheduledExecutorService (u/get-scheduler)
              fut       (.scheduleWithFixedDelay
                          scheduler
                          ^Runnable #(let [exe (a/get-executor)]
                                       (when (a/running? exe)
                                         (a/exec exe (->SamplingWork this exe))))
                          ^long (rand-int c/sample-processing-interval)
                          ^long c/sample-processing-interval
                          TimeUnit/SECONDS)]
          (vreset! scheduled-sampling fut)))))

  (stop-sampling [_]
    (when-let [fut @scheduled-sampling]
      (.cancel ^ScheduledFuture fut true)
      (vreset! scheduled-sampling nil)))

  (analyze [this a]
    (if a
      (analyze* this a)
      (doseq [attr (remove (set (keys c/implicit-schema)) (keys schema))]
        (analyze* this attr)))
    :done)

  (v-size [_ v]
    (reduce-kv
      (fn [total _ props]
        (if (identical? (:db/valueType props) :db.type/ref)
          (let [aid (:db/aid props)
                vt  (value-type props)]
            (+ ^long total
               ^long (list-count
                       lmdb c/ave (b/indexable nil aid v vt c/gmax) :avg)))
          total))
      0 schema))

  (av-size [_ a v]
    (list-count
      lmdb c/ave (datom->indexable schema (d/datom c/e0 a v) false) :avg))

  (av-range-size ^long [_ a lv hv]
    (key-range-list-count
      lmdb c/ave
      [:closed
       (datom->indexable schema (d/datom c/e0 a lv) false)
       (datom->indexable schema (d/datom c/emax a hv) true)]
      :avg))

  (cardinality [_ a]
    (if (:db/aid (schema a))
      (key-range-count
        lmdb c/ave
        [:closed
         (datom->indexable schema (d/datom c/e0 a nil) false)
         (datom->indexable schema (d/datom c/emax a nil) true)]
        :avg)
      0))

  (head [this index low-datom high-datom]
    (retrieved->datom lmdb attrs
                      (.populated? this index low-datom high-datom)))

  (tail [_ index high-datom low-datom]
    (retrieved->datom
      lmdb attrs
      (list-range-first
        lmdb (index->dbi index)
        [:closed-back (index->k index schema high-datom true)
         (index->k index schema low-datom false)] (index->ktype index)
        [:closed-back
         (index->v index schema high-datom true)
         (index->v index schema low-datom false)] (index->vtype index))))

  (slice [_ index low-datom high-datom]
    (mapv #(retrieved->datom lmdb attrs %)
          (list-range
            lmdb (index->dbi index)
            [:closed (index->k index schema low-datom false)
             (index->k index schema high-datom true)] (index->ktype index)
            [:closed (index->v index schema low-datom false)
             (index->v index schema high-datom true)] (index->vtype index))))
  (slice [_ index low-datom high-datom n]
    (mapv #(retrieved->datom lmdb attrs %)
          (scan/list-range-first-n
            lmdb (index->dbi index) n
            [:closed (index->k index schema low-datom false)
             (index->k index schema high-datom true)] (index->ktype index)
            [:closed (index->v index schema low-datom false)
             (index->v index schema high-datom true)] (index->vtype index))))

  (rslice [_ index high-datom low-datom]
    (mapv #(retrieved->datom lmdb attrs %)
          (list-range
            lmdb (index->dbi index)
            [:closed-back (index->k index schema high-datom true)
             (index->k index schema low-datom false)] (index->ktype index)
            [:closed-back (index->v index schema high-datom true)
             (index->v index schema low-datom false)] (index->vtype index))))
  (rslice [_ index high-datom low-datom n]
    (mapv #(retrieved->datom lmdb attrs %)
          (list-range-first-n
            lmdb (index->dbi index) n
            [:closed-back (index->k index schema high-datom true)
             (index->k index schema low-datom false)] (index->ktype index)
            [:closed-back(index->v index schema high-datom true)
             (index->v index schema low-datom false)] (index->vtype index))))

  (e-datoms [_ e]
    (mapv #(kv->datom lmdb attrs e %)
          (get-list lmdb c/eav e :id :avg)))

  (e-first-datom [_ e]
    (when-let [avg (get-value lmdb c/eav e :id :avg true)]
      (kv->datom lmdb attrs e avg)))

  (av-datoms [_ a v]
    (mapv #(d/datom % a v)
          (get-list
            lmdb c/ave (datom->indexable schema (d/datom c/e0 a v) false)
            :avg :id)))

  (av-first-e [_ a v]
    (get-value
      lmdb c/ave
      (datom->indexable schema (d/datom c/e0 a v) false)
      :avg :id true))

  (av-first-datom [this a v]
    (when-let [e (.av-first-e this a v)] (d/datom e a v)))

  (ea-first-datom [_ e a]
    (when-let [r (ea->r schema lmdb e a)]
      (kv->datom lmdb attrs e r)))

  (ea-first-v [_ e a]
    (when-let [r (ea->r schema lmdb e a)]
      (retrieved->v lmdb r)))

  (v-datoms [_ v]
    (mapcat
      (fn [[attr props]]
        (when (identical? (:db/valueType props) :db.type/ref)
          (let [aid (:db/aid props)
                vt  (value-type props)]
            (when-let [es (not-empty (get-list
                                       lmdb c/ave
                                       (b/indexable nil aid v vt c/gmax)
                                       :avg :id))]
              (map #(d/datom % attr v) es)))))
      schema))

  (size-filter [_ index pred low-datom high-datom]
    (list-range-filter-count
      lmdb (index->dbi index)
      (datom-pred->kv-pred lmdb attrs index pred)
      [:closed (index->k index schema low-datom false)
       (index->k index schema high-datom true)] (index->ktype index)
      [:closed (index->v index schema low-datom false)
       (index->v index schema high-datom true)] (index->vtype index)
      true))

  (head-filter [_ index pred low-datom high-datom]
    (list-range-some
      lmdb (index->dbi index)
      (datom-pred->kv-pred lmdb attrs index pred)
      [:closed (index->k index schema low-datom false)
       (index->k index schema high-datom true)] (index->ktype index)
      [:closed (index->v index schema low-datom false)
       (index->v index schema high-datom true)] (index->vtype index)))

  (tail-filter [_ index pred high-datom low-datom]
    (list-range-some
      lmdb (index->dbi index)
      (datom-pred->kv-pred lmdb attrs index pred)
      [:closed-back (index->k index schema high-datom true)
       (index->k index schema low-datom false)] (index->ktype index)
      [:closed-back (index->v index schema high-datom true)
       (index->v index schema low-datom false)] (index->vtype index)))

  (slice-filter [_ index pred low-datom high-datom]
    (list-range-keep
      lmdb (index->dbi index)
      (datom-pred->kv-pred lmdb attrs index pred)
      [:closed (index->k index schema low-datom false)
       (index->k index schema high-datom true)] (index->ktype index)
      [:closed (index->v index schema low-datom false)
       (index->v index schema high-datom true)] (index->vtype index)))

  (rslice-filter [_ index pred high-datom low-datom]
    (list-range-keep
      lmdb (index->dbi index)
      (datom-pred->kv-pred lmdb attrs index pred)
      [:closed-back (index->k index schema high-datom true)
       (index->k index schema low-datom false)] (index->ktype index)
      [:closed-back (index->v index schema high-datom true)
       (index->v index schema low-datom false)] (index->vtype index)))

  (ave-tuples [store out attr val-range]
    (.ave-tuples store out attr val-range nil false nil))
  (ave-tuples [store out attr val-range vpred]
    (.ave-tuples store out attr val-range vpred false nil))
  (ave-tuples [store out attr val-range vpred get-v?]
    (.ave-tuples store out attr val-range vpred get-v? nil))
  (ave-tuples [_ out attr val-ranges vpred get-v? indices]
    (when-let [props (schema attr)]
      (let [aid (props :db/aid)
            vt  (value-type props)]
        (cond
          (and get-v? vpred)
          (ave-tuples-scan-need-v-vpred lmdb out vpred aid vt val-ranges
                                        indices)
          vpred
          (ave-tuples-scan-no-v-vpred lmdb out vpred aid vt val-ranges indices)
          get-v?
          (ave-tuples-scan-need-v lmdb out aid vt val-ranges indices)
          :else
          (ave-tuples-scan-no-v lmdb out aid vt val-ranges indices)))))

  (ave-tuples-list [store attr val-ranges vpred get-v?]
    (let [out (FastList.)]
      (.ave-tuples store out attr val-ranges vpred get-v? nil)
      (p/remove-end-scan out)
      out))

  (sample-ave-tuples [store out attr mcount val-ranges vpred get-v?]
    (when mcount
      (let [indices (u/reservoir-sampling mcount c/init-exec-size-threshold)]
        (.ave-tuples store out attr val-ranges vpred get-v? indices)
        (p/remove-end-scan out))))

  (sample-ave-tuples-list [store attr mcount val-ranges vpred get-v?]
    (let [out (FastList. (int c/init-exec-size-threshold))]
      (.sample-ave-tuples store out attr mcount val-ranges vpred get-v?)
      out))

  (eav-scan-v
    [_ in out eid-idx attrs-v]
    (if (seq attrs-v)
      (let [attr->aid #(:db/aid (schema %))
            get-aid   (comp attr->aid first)
            attrs-v   (sort-by get-aid attrs-v)
            aids      (mapv get-aid attrs-v)
            na        (count aids)
            maps      (mapv peek attrs-v)
            skips     (boolean-array (map :skip? maps))
            preds     (object-array (map :pred maps))
            fidxs     (object-array (map :fidx maps))
            aids      (int-array aids)
            seen      (LongObjectHashMap.)
            dbi-name  c/eav]
        (scan/scan
          (with-open [^AutoCloseable iter
                      (lmdb/val-iterator
                        (lmdb/iterate-list-val-full dbi rtx cur))]
            (if (single-attrs? schema attrs-v)
              (loop [tuple (p/produce in)]
                (when tuple
                  (eav-scan-v-single* lmdb iter na out tuple eid-idx
                                      seen aids preds fidxs skips)
                  (recur (p/produce in))))
              (let [gcounts (group-counts aids)
                    gstarts ^ints (group-starts gcounts)
                    gcounts (int-array gcounts)]
                (loop [tuple (p/produce in)]
                  (when tuple
                    (eav-scan-v-multi* lmdb iter na out tuple eid-idx
                                       seen aids preds fidxs skips gstarts
                                       gcounts)
                    (recur (p/produce in)))))))
          (u/raise "Fail to eav-scan-v: " e
                   {:eid-idx eid-idx :attrs-v attrs-v})))
      (loop []
        (when (p/produce in)
          (recur)))))

  (eav-scan-v-list [_ in eid-idx attrs-v]
    (when (seq attrs-v)
      (let [attr->aid #(:db/aid (schema %))
            get-aid   (comp attr->aid first)
            attrs-v   (sort-by get-aid attrs-v)
            aids      (mapv get-aid attrs-v)
            na        (count aids)
            in        (sort-tuples-by-eid in eid-idx)
            nt        (.size ^List in)
            out       (FastList. nt)
            maps      (mapv peek attrs-v)
            skips     (boolean-array (map :skip? maps))
            preds     (object-array (map :pred maps))
            fidxs     (object-array (map :fidx maps))
            aids      (int-array aids)
            seen      (LongObjectHashMap. nt)
            dbi-name  c/eav]
        (scan/scan
          (with-open [^AutoCloseable iter
                      (lmdb/val-iterator
                        (lmdb/iterate-list-val-full dbi rtx cur))]
            (if (single-attrs? schema attrs-v)
              (dotimes [i nt]
                (eav-scan-v-single*
                  lmdb iter na out (.get ^List in i) eid-idx seen aids
                  preds fidxs skips))
              (let [gcounts (group-counts aids)
                    gstarts ^ints (group-starts gcounts)
                    gcounts (int-array gcounts)]
                (dotimes [i nt]
                  (eav-scan-v-multi*
                    lmdb iter na out (.get ^List in i) eid-idx seen aids
                    preds fidxs skips gstarts gcounts)))))
          (u/raise "Fail to eav-scan-v: " e
                   {:eid-idx eid-idx :attrs-v attrs-v}))
        out)))

  (val-eq-scan-e [_ in out v-idx attr]
    (if attr
      (when-let [props (schema attr)]
        (let [vt       (value-type props)
              aid      (props :db/aid)
              seen     (HashMap.)
              dbi-name c/ave]
          (scan/scan
            (with-open [^AutoCloseable iter
                        (lmdb/val-iterator
                          (lmdb/iterate-list-val-full dbi rtx cur))]
              (loop [^objects tuple (p/produce in)]
                (when tuple
                  (let [v (aget tuple v-idx)]
                    (val-eq-scan-e* iter out tuple seen aid v vt)
                    (recur (p/produce in))))))
            (u/raise "Fail to val-eq-scan-e: " e {:v-idx v-idx :attr attr}))))
      (loop []
        (when (p/produce in)
          (recur)))))

  (val-eq-scan-e-list [_ in v-idx attr]
    (when attr
      (when-let [props (schema attr)]
        (let [vt       (value-type props)
              aid      (props :db/aid)
              in       (sort-tuples-by-val in v-idx)
              nt       (.size ^List in)
              out      (FastList. (* 2 nt))
              seen     (HashMap. nt)
              dbi-name c/ave]
          (scan/scan
            (with-open [^AutoCloseable iter
                        (lmdb/val-iterator
                          (lmdb/iterate-list-val-full dbi rtx cur))]
              (dotimes [i nt]
                (let [^objects tuple (.get ^List in i)
                      v              (aget tuple v-idx)]
                  (val-eq-scan-e* iter out tuple seen aid v vt))))
            (u/raise "Fail to val-eq-scan-e-list: " e {:v-idx v-idx :attr attr}))
          out))))

  (val-eq-scan-e [_ in out v-idx attr bound]
    (if attr
      (when-let [props (schema attr)]
        (let [vt       (value-type props)
              aid      (props :db/aid)
              dbi-name c/ave]
          (scan/scan
            (with-open [^AutoCloseable iter
                        (lmdb/val-iterator
                          (lmdb/iterate-list-val-full dbi rtx cur))]
              (loop [^objects tuple (p/produce in)]
                (when tuple
                  (let [v (aget tuple v-idx)]
                    (val-eq-scan-e-bound* iter out tuple aid v vt bound)
                    (recur (p/produce in))))))
            (u/raise "Fail to val-eq-scan-e-bound: " e
                     {:v-idx v-idx :attr attr}))))
      (loop []
        (when (p/produce in)
          (recur)))))

  (val-eq-scan-e-list [_ in v-idx attr bound]
    (when attr
      (when-let [props (schema attr)]
        (let [vt       (value-type props)
              in       (sort-tuples-by-val in v-idx)
              nt       (.size ^List in)
              aid      (props :db/aid)
              dbi-name c/ave
              out      (FastList. nt)]
          (scan/scan
            (with-open [^AutoCloseable iter
                        (lmdb/val-iterator
                          (lmdb/iterate-list-val-full dbi rtx cur))]
              (dotimes [i nt]
                (let [^objects tuple (.get ^List in i)
                      v              (aget tuple v-idx)]
                  (val-eq-scan-e-bound* iter out tuple aid v vt bound))))
            (u/raise "Fail to val-eq-scan-e-list-bound: " e
                     {:v-idx v-idx :attr attr}))
          out))))

  (val-eq-filter-e [_ in out v-idx attr f-idx]
    (if attr
      (when-let [props (schema attr)]
        (let [vt       (value-type props)
              dbi-name c/ave
              aid      (props :db/aid)]
          (scan/scan
            (with-open [^AutoCloseable iter
                        (lmdb/val-iterator
                          (lmdb/iterate-list-val-full dbi rtx cur))]
              (loop [^objects tuple (p/produce in)]
                (when tuple
                  (let [old-e (aget tuple f-idx)
                        v     (aget tuple v-idx)]
                    (val-eq-filter-e* iter out tuple aid v vt old-e)
                    (recur (p/produce in))))))
            (u/raise "Fail to val-eq-filter-e: " e
                     {:v-idx v-idx :attr attr}))))
      (loop []
        (when (p/produce in)
          (recur)))))

  (val-eq-filter-e-list [_ in v-idx attr f-idx]
    (when attr
      (when-let [props (schema attr)]
        (let [vt       (value-type props)
              in       (sort-tuples-by-val in v-idx)
              nt       (.size ^List in)
              out      (FastList. nt)
              dbi-name c/ave
              aid      (props :db/aid)]
          (scan/scan
            (with-open [^AutoCloseable iter
                        (lmdb/val-iterator
                          (lmdb/iterate-list-val-full dbi rtx cur))]
              (dotimes [i nt]
                (let [^objects tuple (.get ^List in i)
                      old-e          (aget tuple f-idx)
                      v              (aget tuple v-idx)]
                  (val-eq-filter-e* iter out tuple aid v vt old-e))))
            (u/raise "Fail to val-eq-filter-e-list: " e
                     {:v-idx v-idx :attr attr}))
          out)))))

(defn fulltext-index
  [search-engines ft-ds]
  (doseq [res    ft-ds
          :let   [op (peek res)
                  d (nth op 1)]
          domain (nth res 0)
          :let   [engine (search-engines domain)]]
    (case (nth op 0)
      :a (add-doc engine d (peek d) false)
      :d (remove-doc engine d)
      :g (add-doc engine [:g (nth d 0)] (peek d) false)
      :r (remove-doc engine [:g d]))))

(defn vector-index
  [vector-indices vi-ds]
  (doseq [res    vi-ds
          :let   [op (peek res)
                  d (nth op 1)]
          domain (nth res 0)
          :let   [index (vector-indices domain)]]
    (case (nth op 0)
      :a (add-vec index d (peek d))
      :d (remove-vec index d)
      :g (add-vec index [:g (nth d 0)] (peek d))
      :r (remove-vec index [:g d]))))

(defn idoc-index
  [idoc-indices id-ds]
  (let [updates (volatile! {})
        others  (FastList.)]
    (doseq [res  id-ds
            :let [op     (peek res)
                  d      (nth op 1)
                  domain (nth res 0)
                  kind   (nth op 0)]]
      (case kind
        (:a :d)
        (let [k [(nth d 0) (nth d 1)]]
          (vswap! updates update-in [domain k kind] (fnil conj []) op))
        (:g :r)
        (.add others res)))
    (doseq [[domain ops] @updates
            :let         [index (idoc-indices domain)]]
      (doseq [[_ {:keys [a d]}] ops]
        (if (and (= 1 (count a)) (= 1 (count d)))
          (let [old-d   (nth (first d) 1)
                new-d   (nth (first a) 1)
                old-ref old-d
                new-ref new-d
                old-doc (peek old-d)
                new-doc (peek new-d)
                patch   (some-> (meta (first a)) :idoc/patch)
                res     (if patch
                          (idoc/patch-doc index old-ref old-doc new-ref new-doc
                                          patch)
                          (idoc/update-doc index old-ref old-doc new-ref new-doc))]
            (when (= res :doc-missing)
              (idoc/remove-doc index old-ref old-doc)
              (idoc/add-doc index new-ref new-doc false)))
          (let [adds (mapv (fn [op]
                             (let [d (nth op 1)]
                               [d (peek d)]))
                           a)
                rems (mapv (fn [op]
                             (let [d (nth op 1)]
                               [d (peek d)]))
                           d)]
            (idoc/add-docs index adds false)
            (idoc/remove-docs index rems)))))
    (doseq [res  others
            :let [op     (peek res)
                  d      (nth op 1)
                  domain (nth res 0)
                  index  (idoc-indices domain)]]
      (case (nth op 0)
        :g (idoc/add-doc index [:g (nth d 0)] (peek d) false)
        :r (idoc/remove-doc index [:g (nth d 0)] (peek d))))))

(defn e-sample*
  [^Store store a aid]
  (when-not (.closed? store)
    (let [lmdb   (.-lmdb store)
          counts ^ConcurrentHashMap (.-counts store)
          as     (.a-size store a)
          ts     (FastList. (int c/init-exec-size-threshold))]
      (.put counts aid as)
      (.sample-ave-tuples store ts a as [[[:closed c/v0] [:closed c/vmax]]]
                          nil false)
      (when-not (.closed? store)
        (transact-kv lmdb (map-indexed
                            (fn [i ^objects t]
                              [:put c/meta [aid i] ^long (aget t 0)
                               :int-int :id])
                            ts)))
      ts)))

(defn default-ratio*
  [^Store store a aid]
  (when-not (.closed? store)
    (let [card ^long (.cardinality store a)]
      (if (zero? card)
        1.0
        (let [ratio (double (/ ^long (.a-size store a) card))
              lmdb  (.-lmdb store)]
          (when-not (.closed? store)
            (transact-kv lmdb [[:put c/meta [aid :ratio] ratio :data :double]]))
          ratio)))))

(defn- analyze*
  [^Store store attr]
  (when-let [aid (:db/aid ((schema store) attr))]
    (default-ratio* store attr aid)
    (e-sample* store attr aid)))

(defn sampling
  "sample a random changed attribute at a time"
  [^Store store]
  (let [n          (count (attrs store))
        [aid attr] (nth (seq (attrs store)) (rand-int n))
        counts     ^ConcurrentHashMap (.-counts store)
        acount     ^long (.getOrDefault counts aid 0)]
    (when-let [^long new-acount (a-size store attr)]
      (when (< (* acount ^double c/sample-change-ratio)
               (Math/abs (- new-acount acount)))
        (analyze* store attr)))))

(deftype SamplingWork [^Store store exe]
  IAsyncWork
  (work-key [_] (->> (db-name store) hash (str "sampling") keyword))
  (do-work [_]
    (when (a/running? exe)
      (let [rlock (.readLock ^ReentrantReadWriteLock (.-sampling-lock store))]
        (when (.tryLock rlock)
          (try
            (when-not (closed? store)
              (sampling store))
            (catch Throwable _)
            (finally
              (.unlock rlock)))))))
  (combine [_] nil)
  (callback [_] nil))

(defn- check [store attr old new]
  (vld/validate-schema-mutation store (.-lmdb ^Store store) attr old new))

(defn migrate-attr-values
  "Re-encode all datoms for `attr` from :data (untyped) to `new-vt`.
   Validates every value can be coerced first. Deletes old datoms with
   the old :data encoding, then inserts new datoms with the new typed
   encoding, all in a single atomic `transact-kv` call."
  [^Store store attr new-vt]
  (let [lmdb   (.-lmdb store)
        s      (schema store)
        props  (s attr)
        old-vt (value-type props)
        aid    (props :db/aid)
        datoms (.slice store :ave
                       (d/datom c/e0 attr c/v0)
                       (d/datom c/emax attr c/vmax))]
    (when (seq datoms)
      (let [errors  (volatile! [])
            coerced (mapv
                      (fn [^Datom datom]
                        (try
                          (let [v     (.-v datom)
                                new-v (prep/type-coercion new-vt v)]
                            [datom new-v])
                          (catch Exception ex
                            (vswap! errors conj
                                    {:entity (.-e datom)
                                     :value  (.-v datom)
                                     :error  (.getMessage ex)})
                            nil)))
                      datoms)]
        (when (seq @errors)
          (u/raise "Cannot migrate attribute values to new type"
                   {:attribute   attr
                    :target-type new-vt
                    :errors      @errors}))
        (let [txs (FastList.)]
          ;; 1) delete old datoms using old :data encoding
          (doseq [[^Datom datom _] coerced]
            (let [e  (.-e datom)
                  v  (.-v datom)
                  i  ^Indexable (b/indexable e aid v old-vt c/g0)
                  gt (when (b/giant? i)
                       (let [[_ ^Retrieved r]
                             (nth
                               (list-range
                                 lmdb c/eav [:closed e e] :id
                                 [:closed
                                  i
                                  (Indexable. e aid v (.-f i) (.-b i) c/gmax)]
                                 :avg)
                               0)]
                         (.-g r)))
                  ii (Indexable. e aid v (.-f i) (.-b i) (or gt c/normal))]
              (.add txs (lmdb/kv-tx :del-list c/ave ii [e] :avg :id))
              (.add txs (lmdb/kv-tx :del-list c/eav e [ii] :id :avg))
              (when gt
                (.add txs (lmdb/kv-tx :del c/giants gt :id)))))
          ;; 2) insert new datoms using new typed encoding
          (doseq [[^Datom datom new-v] coerced]
            (let [e      (.-e datom)
                  cur-gt (max-gt store)
                  i      (b/indexable e aid new-v new-vt cur-gt)
                  giant? (b/giant? i)]
              (.add txs (lmdb/kv-tx :put c/ave i e :avg :id))
              (.add txs (lmdb/kv-tx :put c/eav e i :id :avg))
              (when giant?
                (.advance-max-gt store)
                (let [{:keys [value vtype]} (encode-giant-datom
                                              (d/datom e attr new-v))]
                  (.add txs (lmdb/kv-tx :put c/giants cur-gt value
                                        :id vtype [:append]))))))
          ;; 3) single atomic write
          (locking (lmdb/write-txn lmdb)
            (transact-kv lmdb txs)))))))

(defn- collect-fulltext
  [^FastList ft-ds attr props v op]
  (when-not (str/blank? v)
    (.add ft-ds
          [(cond-> (or (props :db.fulltext/domains) [c/default-domain])
             (props :db.fulltext/autoDomain) (conj (u/keyword->string attr)))
           op])))

(defn- insert-datom
  [^Store store ^Datom d ^FastList txs ^FastList ft-ds ^FastList vi-ds
   ^FastList id-ds ^HashMap giants]
  (let [schema (schema store)
        opts   (opts store)
        attr   (.-a d)
        _      (vld/validate-closed-schema schema opts attr (.-v d))
        e      (.-e d)
        v      (.-v d)
        props  (or (schema attr)
                   (swap-attr store attr identity))
        vt     (value-type props)
        aid    (props :db/aid)
        max-gt (max-gt store)
        i      (b/indexable e aid v vt max-gt)
        giant? (b/giant? i)]
    (.add txs (lmdb/kv-tx :put c/ave i e :avg :id))
    (.add txs (lmdb/kv-tx :put c/eav e i :id :avg))
    (when giant?
      (.advance-max-gt store)
      (let [gd [e attr v]
            {:keys [value vtype]} (encode-giant-datom (apply d/datom gd))]
        (.put giants gd max-gt)
        (.add txs (lmdb/kv-tx :put c/giants max-gt value
                              :id vtype [:append]))))
    (when (identical? vt :db.type/vec)
      (.add vi-ds [(conjv (props :db.vec/domains) (v/attr-domain attr))
                   (if giant? [:g [max-gt v]] [:a [e aid v]])]))
    (when (identical? vt :db.type/idoc)
      (let [domain (or (props :db/domain) (u/keyword->string attr))]
        (let [op    (if giant?
                      [:g [max-gt v]]
                      [:a [e aid v]])
              patch (some-> (meta d) :idoc/patch)
              op    (if patch (with-meta op {:idoc/patch patch}) op)]
          (.add id-ds [domain op]))))
    (when (props :db/fulltext)
      (let [v (str v)]
        (collect-fulltext ft-ds attr props v
                          (if giant? [:g [max-gt v]] [:a [e aid v]]))))))

(defn- delete-datom
  [^Store store ^Datom d ^FastList txs ^FastList ft-ds ^FastList vi-ds
   ^FastList id-ds ^HashMap giants]
  (let [schema (schema store)
        e      (.-e d)
        attr   (.-a d)
        v      (.-v d)
        d-eav  [e attr v]
        props  (schema attr)
        vt     (value-type props)
        aid    (props :db/aid)
        i      ^Indexable (b/indexable e aid v vt c/g0)
        gt-cur (.get giants d-eav)
        gt     (when (b/giant? i)
                 (or gt-cur
                     (let [[_ ^Retrieved r]
                           (nth
                             (list-range
                               (.-lmdb store) c/eav [:closed e e] :id
                               [:closed
                                i
                                (Indexable. e aid v (.-f i) (.-b i) c/gmax)]
                               :avg)
                             0)]
                       (.-g r))))]
    (when (props :db/fulltext)
      (let [v (str v)]
        (collect-fulltext ft-ds attr props v (if gt [:r gt] [:d [e aid v]]))))
    (when (identical? vt :db.type/idoc)
      (let [domain (or (props :db/domain) (u/keyword->string attr))]
        (.add id-ds [domain
                     (if gt
                       [:r [gt v]]
                       [:d [e aid v]])])))
    (let [ii (Indexable. e aid v (.-f i) (.-b i) (or gt c/normal))]
      (.add txs (lmdb/kv-tx :del-list c/ave ii [e] :avg :id))
      (.add txs (lmdb/kv-tx :del-list c/eav e [ii] :id :avg))
      (when gt
        (when gt-cur (.remove giants d-eav))
        (.add txs (lmdb/kv-tx :del c/giants gt :id)))
      (when (identical? vt :db.type/vec)
        (.add vi-ds [(conjv (props :db.vec/domains) (v/attr-domain attr))
                     (if gt [:r gt] [:d [e aid v]])])))))

(defn vpred
  [v]
  (cond
    (string? v)  (fn [x] (if (string? x) (.equals ^String v x) false))
    (integer? v) (fn [x] (if (integer? x) (= (long v) (long x)) false))
    (keyword? v) (fn [x] (.equals ^Object v x))
    (nil? v)     (fn [x] (nil? x))
    :else        (fn [x] (= v x))))

(defn ea-tuples
  [^Store store e a]
  (let [lmdb       (.-lmdb store)
        schema     (schema store)
        low-datom  (d/datom e a c/v0)
        high-datom (d/datom e a c/vmax)
        coll       (list-range
                     lmdb c/eav
                     [:closed (index->k :eav schema low-datom false)
                      (index->k :eav schema high-datom true)] :id
                     [:closed (index->v :eav schema low-datom false)
                      (index->v :eav schema high-datom true)] :avg)
        size       (.size ^Collection coll)
        res        (FastList. size)]
    (doseq [[_ r] coll]
      (.add res (object-array [(retrieved->v lmdb r)])))
    res))

(defn ev-tuples
  [^Store store e v]
  (let [lmdb       (.-lmdb store)
        attrs      (attrs store)
        low-datom  (d/datom e nil nil)
        high-datom low-datom
        pred       (fn [kv]
                     (let [^ByteBuffer vb (lmdb/v kv)
                           ^Retrieved r   (b/read-buffer vb :avg)
                           rv             (retrieved->v lmdb r)]
                       (when ((vpred rv) v) (attrs (.-a r)))))
        coll       (list-range-keep
                     lmdb (index->dbi :eav) pred
                     [:closed (index->k :eav schema low-datom false)
                      (index->k :eav schema high-datom true)] :id
                     [:closed (index->v :eav schema low-datom false)
                      (index->v :eav schema high-datom true)] :avg)
        size       (.size ^Collection coll)
        res        (FastList. size)]
    (doseq [attr coll] (.add res (object-array [attr])))
    res))

(defn e-tuples
  [^Store store e]
  (let [lmdb  (.-lmdb store)
        attrs (attrs store)
        coll  (get-list lmdb c/eav e :id :avg)
        size  (.size ^Collection coll)
        res   (FastList. size)]
    (doseq [^Retrieved r coll]
      (.add res (object-array [(attrs (.-a r)) (retrieved->v lmdb r)])))
    res))

(defn av-tuples
  [^Store store a v]
  (let [lmdb   (.-lmdb store)
        schema (schema store)
        coll   (get-list
                 lmdb c/ave (datom->indexable schema (d/datom c/e0 a v) false)
                 :avg :id)
        size   (.size ^Collection coll)
        res    (FastList. size)]
    (doseq [e coll] (.add res (object-array [e])))
    res))

(defn a-tuples
  [^Store store a]
  (.ave-tuples-list store a [[[:closed c/v0] [:closed c/vmax]]] nil true))

(defn v-tuples
  [^Store store v]
  (let [lmdb       (.-lmdb store)
        attrs      (attrs store)
        low-datom  (d/datom c/e0 nil nil)
        high-datom (d/datom c/emax nil nil)
        pred       (fn [kv]
                     (let [^ByteBuffer kb (lmdb/k kv)
                           e              (b/read-buffer kb :id)
                           ^ByteBuffer vb (lmdb/v kv)
                           ^Retrieved r   (b/read-buffer vb :avg)
                           rv             (retrieved->v lmdb r)]
                       (when ((vpred rv) v) [e (attrs (.-a r))])))
        coll       (list-range-keep
                     lmdb (index->dbi :eav) pred
                     [:closed (index->k :eav schema low-datom false)
                      (index->k :eav schema high-datom true)] :id
                     [:closed (index->v :eav schema low-datom false)
                      (index->v :eav schema high-datom true)] :avg)
        size       (.size ^Collection coll)
        res        (FastList. size)]
    (doseq [[e attr] coll] (.add res (object-array [e attr])))
    res))

(defn all-tuples
  [^Store store]
  (let [lmdb       (.-lmdb store)
        schema     (schema store)
        attrs      (attrs store)
        low-datom  (d/datom c/e0 nil nil)
        high-datom (d/datom c/emax nil nil)
        coll       (list-range
                     lmdb c/eav
                     [:closed (index->k :eav schema low-datom false)
                      (index->k :eav schema high-datom true)] :id
                     [:closed (index->v :eav schema low-datom false)
                      (index->v :eav schema high-datom true)] :avg)
        size       (.size ^Collection coll)
        res        (FastList. size)]
    (doseq [[e r] coll]
      (.add res (object-array [e
                               (retrieved->attr attrs r)
                               (retrieved->v lmdb r)])))
    res))

(defn- transact-opts
  [lmdb opts]
  (let [opts (c/canonicalize-wal-opts opts)]
    (when (true? (:wal? opts))
      (let [flags (or (get-env-flags lmdb) #{})]
        (when (and (not (contains? flags :nosync))
                   (not (contains? flags :rdonly)))
          (set-env-flags lmdb #{:nosync} true))))
    (transact-kv
      lmdb (conj (for [[k v] opts]
                   (lmdb/kv-tx :put c/opts k v :attr :data))
                 (lmdb/kv-tx :put c/meta :last-modified
                             (System/currentTimeMillis) :attr :long)))))

(defn- load-opts
  [lmdb]
  (c/canonicalize-wal-opts
   (into {} (get-range lmdb c/opts [:all] :attr :data))))

(defn- open-dbis
  [lmdb]
  (open-list-dbi lmdb c/ave {:key-size c/+max-key-size+
                             :val-size c/+id-bytes+})
  (open-list-dbi lmdb c/eav {:key-size c/+id-bytes+
                             :val-size c/+max-key-size+})
  (open-dbi lmdb c/giants {:key-size c/+id-bytes+})
  (open-dbi lmdb c/meta {:key-size c/+max-key-size+})
  (open-dbi lmdb c/opts {:key-size c/+max-key-size+})
  (open-dbi lmdb c/schema {:key-size c/+max-key-size+}))

(defn- default-search-domain
  [dms search-opts search-domains]
  (let [new-opts (assoc (or (get search-domains c/default-domain)
                            search-opts
                            {})
                        :domain c/default-domain)]
    (assoc dms c/default-domain (if-let [opts (dms c/default-domain)]
                                  (merge opts new-opts)
                                  new-opts))))

(defn- listed-search-domains
  [dms domains search-domains]
  (reduce (fn [m domain]
            (let [new-opts (assoc (get search-domains domain {})
                                  :domain domain)]
              (assoc m domain (if-let [opts (m domain)]
                                (merge opts new-opts)
                                new-opts))))
          dms domains))

(defn- init-search-domains
  [search-domains0 schema search-opts search-domains]
  (reduce-kv
    (fn [dms attr
        {:keys [db/fulltext db.fulltext/domains db.fulltext/autoDomain]}]
      (if fulltext
        (cond-> (if (seq domains)
                  (listed-search-domains dms domains search-domains)
                  (default-search-domain dms search-opts search-domains))
          autoDomain (#(let [domain (u/keyword->string attr)]
                         (assoc
                           % domain
                           (let [new-opts (assoc (get search-domains domain {})
                                                 :domain domain)]
                             (if-let [opts (% domain)]
                               (merge opts new-opts)
                               new-opts))))))
        dms))
    (or search-domains0 {}) schema))

(defn- init-engines
  [lmdb domains]
  (reduce-kv
    (fn [m domain opts]
      (assoc m domain (s/new-search-engine lmdb opts)))
    {} domains))

(defn- listed-vector-domains
  [dms domains vector-opts vector-domains]
  (reduce (fn [m domain]
            (let [new-opts (assoc (get vector-domains domain vector-opts)
                                  :domain domain)]
              (assoc m domain (if-let [opts (m domain)]
                                (merge opts new-opts)
                                new-opts))))
          dms domains))

(defn- init-vector-domains
  [vector-domains0 schema vector-opts vector-domains]
  (reduce-kv
    (fn [dms attr {:keys [db/valueType db.vec/domains]}]
      (if (identical? valueType :db.type/vec)
        (if (seq domains)
          (listed-vector-domains dms domains vector-opts vector-domains)
          (let [domain (v/attr-domain attr)]
            (assoc dms domain (assoc (get vector-domains domain vector-opts)
                                     :domain domain))))
        dms))
    (or vector-domains0 {}) schema))

(defn- init-indices
  [lmdb domains]
  (reduce-kv
    (fn [m domain opts]
      (assoc m domain (v/new-vector-index lmdb opts)))
    {} domains))

(defn- init-idoc-domains
  [schema]
  (reduce-kv
    (fn [dms attr {:keys [db/valueType db/domain db/idocFormat]}]
      (if (identical? valueType :db.type/idoc)
        (let [domain (or domain (u/keyword->string attr))
              fmt    (or idocFormat :edn)
              prior  (get dms domain)]
          (cond
            (nil? prior) (assoc dms domain {:domain domain :format fmt})
            (= (:format prior) fmt) dms
            :else (assoc dms domain (assoc prior :format :mixed))))
        dms))
    {} schema))

(defn- init-idoc-indices
  [lmdb domains]
  (reduce-kv
    (fn [m domain opts]
      (assoc m domain (idoc/new-idoc-index lmdb opts)))
    {} domains))

(defn- propagate-top-level-txlog-opts-to-kv-opts
  [opts]
  (let [opts      (or opts {})
        kv-opts   (c/canonicalize-wal-opts (or (:kv-opts opts) {}))
        txlog-opts (into {}
                         (keep (fn [[k v]]
                                 (let [k' (c/canonical-wal-option-key k)]
                                   (when (and (c/wal-option-key? k)
                                              (not (contains? kv-opts k')))
                                     [k' v]))))
                         opts)]
    (-> (c/canonicalize-wal-opts opts)
        (assoc :kv-opts (if (seq txlog-opts)
                          (merge kv-opts txlog-opts)
                          kv-opts)))))

(defn open
  "Open and return the storage."
  ([]
   (open nil nil))
  ([dir]
   (open dir nil))
  ([dir schema]
   (open dir schema nil))
  ([dir schema opts0]
   (let [opts (propagate-top-level-txlog-opts-to-kv-opts opts0)
         {:keys [kv-opts search-opts search-domains vector-opts vector-domains]}
         opts
         dir  (or dir (u/tmp-dir (str "datalevin-" (UUID/randomUUID))))
         data-file (java.io.File. (str dir u/+separator+ c/data-file-name))
         new-db? (not (.exists ^java.io.File data-file))
         wal-default-kv-opts (when new-db?
                               {:wal? c/*datalog-wal?*
                                :wal-durability-profile
                                c/*datalog-wal-durability-profile*})
         kv-opts (if wal-default-kv-opts
                   (merge wal-default-kv-opts kv-opts)
                   kv-opts)
         lmdb (lmdb/open-kv dir kv-opts)]
     (open-dbis lmdb)
     (let [opts0     (load-opts lmdb)
           opts1     (if (empty opts0)
                       {:validate-data?       false
                        :auto-entity-time?    false
                        :closed-schema?       false
                        :background-sampling? c/*db-background-sampling?*
                        :wal?             c/*datalog-wal?*
                        :wal-rollout-mode c/*wal-rollout-mode*
                        :wal-rollback?    c/*wal-rollback?*
                        :wal-durability-profile
                        c/*datalog-wal-durability-profile*
                        :wal-commit-marker? c/*wal-commit-marker?*
                        :wal-commit-marker-version
                        c/*wal-commit-marker-version*
                        :wal-sync-mode            c/*wal-sync-mode*
                        :wal-group-commit         c/*wal-group-commit*
                        :wal-group-commit-ms      c/*wal-group-commit-ms*
                        :wal-meta-flush-max-txs
                        c/*wal-meta-flush-max-txs*
                        :wal-meta-flush-max-ms
                        c/*wal-meta-flush-max-ms*
                        :wal-commit-wait-ms       c/*wal-commit-wait-ms*
                        :wal-sync-adaptive?       c/*wal-sync-adaptive?*
                        :wal-segment-max-bytes c/*wal-segment-max-bytes*
                        :wal-segment-max-ms    c/*wal-segment-max-ms*
                        :wal-segment-prealloc?
                        c/*wal-segment-prealloc?*
                        :wal-segment-prealloc-mode
                        c/*wal-segment-prealloc-mode*
                        :wal-segment-prealloc-bytes
                        c/*wal-segment-prealloc-bytes*
                        :wal-retention-bytes c/*wal-retention-bytes*
                        :wal-retention-ms    c/*wal-retention-ms*
                        :wal-retention-pin-backpressure-threshold-ms
                        c/*wal-retention-pin-backpressure-threshold-ms*
                        :wal-vec-checkpoint-interval-ms
                        c/*wal-vec-checkpoint-interval-ms*
                        :wal-vec-max-lsn-delta
                        c/*wal-vec-max-lsn-delta*
                        :wal-vec-max-buffer-bytes
                        c/*wal-vec-max-buffer-bytes*
                        :wal-vec-chunk-bytes
                        c/*wal-vec-chunk-bytes*
                        :db-name              (str (UUID/randomUUID))
                        :cache-limit          512}
                       opts0)
           opts2     (c/canonicalize-wal-opts
                      (merge opts1 opts))
           schema    (init-schema lmdb schema)
           s-domains (init-search-domains (:search-domains opts2)
                                          schema search-opts search-domains)
           v-domains (init-vector-domains (:vector-domains opts2)
                                          schema vector-opts vector-domains)
           i-domains (init-idoc-domains schema)]
       (transact-opts lmdb opts2)
       (->Store lmdb
                (init-engines lmdb s-domains)
                (init-indices lmdb v-domains)
                (init-idoc-indices lmdb i-domains)
                (ConcurrentHashMap.)
                (load-opts lmdb)
                schema
                (schema->rschema schema)
                (init-attrs schema)
                (init-max-aid schema)
                (init-max-gt lmdb)
                (init-max-tx lmdb)
                (volatile! nil)
                (volatile! :storage-mutex)
                (ReentrantReadWriteLock.))))))

(defn- transfer-engines
  [engines lmdb]
  (zipmap (keys engines) (map #(s/transfer % lmdb) (vals engines))))

(defn- transfer-indices
  [indices lmdb]
  (zipmap (keys indices) (map #(v/transfer % lmdb) (vals indices))))

(defn- transfer-idoc-indices
  [indices lmdb]
  (zipmap (keys indices) (map #(idoc/transfer % lmdb) (vals indices))))

(defn transfer
  "transfer state of an existing store to a new store that has a different
  LMDB instance"
  [^Store old lmdb]
  (->Store lmdb
           (transfer-engines (.-search-engines old) lmdb)
           (transfer-indices (.-vector-indices old) lmdb)
           (transfer-idoc-indices (.-idoc-indices old) lmdb)
           (.-counts old)
           (opts old)
           (schema old)
           (rschema old)
           (attrs old)
           (max-aid old)
           (max-gt old)
           (max-tx old)
           (.-scheduled-sampling old)
           (.-write-txn old)
           (ReentrantReadWriteLock.)))
