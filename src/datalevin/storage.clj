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
   [datalevin.kv :as kv]
   [datalevin.remote :as remote]
   [datalevin.util :as u :refer [conjs conjv]]
   [datalevin.buffer :as bf]
   [datalevin.relation :as r]
   [datalevin.bits :as b]
   [datalevin.pipe :as p]
   [datalevin.scan :as scan :refer [visit-list*]]
   [datalevin.search :as s]
   [datalevin.secondary-index :as si]
   [datalevin.idoc :as idoc]
   [datalevin.embedding :as emb]
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
            list-range-some list-range-keep visit-list-range
            max-gt advance-max-gt max-tx
            open-list-dbi open-dbi attrs add-doc remove-doc opts env-opts kv-info swap-attr
            add-vec remove-vec close-vecs vec-closed? schema closed? a-size db-name populated?
            get-env-flags set-env-flags]]
   [clojure.string :as str])
  (:import
   [java.util List Comparator Collection HashMap IdentityHashMap UUID]
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
   [datalevin.bits Retrieved Indexable]
   [datalevin.lmdb DatomKVTxData]))

(declare with-open-opts close-store-resources! release-shared-local-store!)
(declare enqueue-secondary-index-work! enqueue-secondary-index-work-if-needed!)

(def ^:private async-secondary-index-option-keys
  #{:async-secondary-index-worker-max-jobs
    :async-secondary-index-worker-lease-ms
    :async-secondary-index-retry-base-ms
    :async-secondary-index-retry-max-ms})

(defn- apply-option-mutations
  [opts kvs]
  (when-not (map? kvs)
    (u/raise "Option mutations must be a map" {:value kvs}))
  (reduce-kv
   (fn [m k v]
     (let [k' (c/canonical-wal-option-key k)]
       (vld/validate-option-mutation k' v)
       (-> m
           (dissoc k)
           (assoc k' v))))
   opts
   kvs))

(defonce ^:private shared-local-stores (atom {}))

(defn- patch-attr-schema
  [old-props property-patch]
  (reduce-kv
    (fn [props k v]
      (cond
        ;; Attribute IDs are allocated and owned by storage. Ignoring incoming
        ;; IDs makes the result of `schema` safe to reuse as schema input.
        (identical? k :db/aid) props

        ;; A schema property is removed only when explicitly retracted.
        (identical? v :db/retract) (dissoc props k)

        :else (assoc props k v)))
    (or old-props {})
    property-patch))

(defn- apply-schema-patch
  [old-schema schema-update]
  (reduce-kv
    (fn [updates attr property-patch]
      (assoc updates attr
             (patch-attr-schema (old-schema attr) property-patch)))
    {}
    (or schema-update {})))

(defn- infer-tuple-attr-types
  "Add typed tuple encoding to composite attributes whose schema only declares
  :db/tupleAttrs. Component types come from the source attributes. Attributes
  with untyped or non-scalar sources retain the legacy :data encoding until
  those source types are declared."
  [old-schema schema-update]
  (let [full-schema (merge old-schema schema-update)]
    (reduce-kv
      (fn [updates attr props]
        (if (and (sequential? (:db/tupleAttrs props))
                 (not (contains? props :db/tupleType))
                 (not (contains? props :db/tupleTypes)))
          (let [types (mapv #(get-in full-schema [% :db/valueType])
                            (:db/tupleAttrs props))]
            (if (and (< 1 (count types))
                     (every? c/tuple-value-types types))
              (assoc updates attr
                     (assoc props
                            :db/valueType :db.type/tuple
                            :db/tupleTypes types))
              updates))
          updates))
      (or schema-update {})
      full-schema)))

(defn- prepare-schema-update
  [old-schema schema-update]
  (vld/validate-schema-update schema-update)
  (infer-tuple-attr-types
    old-schema (apply-schema-patch old-schema schema-update)))

(defn- shared-local-store-key
  [dir]
  (when (and (string? dir) (not (remote/dtlv-uri? dir)))
    (.getCanonicalPath ^java.io.File (u/file dir))))

(defn- current-shared-local-store
  [dir]
  (when-let [dir-key (shared-local-store-key dir)]
    (locking shared-local-stores
      (when-let [store (get-in @shared-local-stores [dir-key :store])]
        (if (closed? store)
          (do
            (swap! shared-local-stores dissoc dir-key)
            nil)
          store)))))

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
        (and (identical? :db.attr/preds k)
             (some? v))                   [:db.attr/preds]
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
   :db.attr/preds       => #{attr ...}
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

(defn- effective-schema-update
  [old schema]
  (into {}
        (map (fn [[attr props]]
               [attr (if-let [old-props (old attr)]
                       (assoc props :db/aid (old-props :db/aid))
                       props)]))
        schema))

(defn- schema-update-required?
  [old schema]
  (boolean
    (some (fn [[attr props]]
            (not= (old attr) props))
          (effective-schema-update old schema))))

(defn- init-schema
  [lmdb schema]
  (let [now     (load-schema lmdb)
        missing (reduce-kv
                  (fn [acc attr props]
                    (if (contains? now attr) acc (assoc acc attr props)))
                  {} c/implicit-schema)]
    (cond
      (empty? now)
      (transact-schema lmdb c/implicit-schema)

      (seq missing)
      (transact-schema lmdb (update-schema now missing))))
  (when schema
    (let [old-schema   (load-schema lmdb)
          schema       (prepare-schema-update old-schema schema)
          full-schema  (merge old-schema schema)]
      (vld/validate-schema full-schema)
      (when (schema-update-required? old-schema schema)
        (transact-schema lmdb (update-schema old-schema schema)))))
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

(defn- init-state-sync-ms
  [lmdb]
  (long (or (get-value lmdb c/meta :last-modified :attr :long) 0)))

(defn- ensure-open-last-modified!
  ([lmdb]
   (ensure-open-last-modified! lmdb false))
  ([lmdb raw?]
   (when-not (get-value lmdb c/meta :last-modified :attr :long)
     (let [tx-data [(lmdb/kv-tx :put c/meta :last-modified
                                 (System/currentTimeMillis) :attr :long)]]
       (if raw?
         (kv/transact-kv-without-txlog! lmdb tx-data)
         (transact-kv lmdb tx-data))))))

(defn e-aid-v->datom
  [store e-aid-v]
  (d/datom (nth e-aid-v 0) ((attrs store) (nth e-aid-v 1)) (peek e-aid-v)))

(defn- retrieved->attr [attrs ^Retrieved r] (attrs (.-a r)))

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
      (let [e (.getLong ^ByteBuffer (lmdb/v kv) 0)
            v (idx/avg-buffer->v lmdb (lmdb/k kv))]
        (.add out (object-array [e v]))))))

(defn- ave-tuples-scan-need-v-vpred
  [lmdb ^Collection out vpred aid vt val-ranges sample-indices]
  (ave-tuples-scan*
    lmdb aid vt val-ranges sample-indices
    (fn [kv]
      (let [v (idx/avg-buffer->v lmdb (lmdb/k kv))]
        (when (vpred v)
          (.add out (object-array [(.getLong ^ByteBuffer (lmdb/v kv) 0)
                                  v])))))))

(defn- ave-tuples-scan-no-v
  [lmdb ^Collection out aid vt val-ranges sample-indices]
  (ave-tuples-scan*
    lmdb aid vt val-ranges sample-indices
    (fn [kv]
      (.add out (object-array [(.getLong ^ByteBuffer (lmdb/v kv) 0)])))))

(defn- ave-tuples-scan-no-v-vpred
  [lmdb ^Collection out vpred aid vt val-ranges sample-indices]
  (ave-tuples-scan*
    lmdb aid vt val-ranges sample-indices
    (fn [kv]
      (let [v (idx/avg-buffer->v lmdb (lmdb/k kv))]
        (when (vpred v)
          (.add out (object-array [(.getLong ^ByteBuffer (lmdb/v kv) 0)])))))))

(defn- sort-tuples-by-eid
  [^List tuples ^long eid-idx]
  (doto tuples
    (.sort (reify Comparator
             (compare [_ a b]
               (Long/compare ^long (aget ^objects a eid-idx)
                             ^long (aget ^objects b eid-idx)))))))

(defn- sort-tuples-by-val
  [^List tuples ^long v-idx vt]
  (if (or (identical? vt :db.type/ref)
          (identical? vt :db.type/long))
    (sort-tuples-by-eid tuples v-idx)
    (doto tuples
      (.sort (reify Comparator
               (compare [_ a b]
                 (d/compare-with-type (aget ^objects a v-idx)
                                      (aget ^objects b v-idx))))))))

(defn- group-counts
  [aids]
  (sequence (comp (partition-by identity) (map count)) aids))

(defn- group-starts
  [counts]
  (int-array (->> counts (reductions +) butlast (into [0]))))

(defn- eav-scan-v-single*
  [lmdb iter na nvs ^Collection out ^objects tuple eid-idx
   ^LongObjectHashMap seen ^ints aids ^objects preds ^objects fidxs
   ^booleans skips]
  (let [te        ^long (aget tuple eid-idx)
        has-fidx? (< 0 (alength fidxs))
        ts        (when-not has-fidx? (.get seen te))]
    (if ts
      (if (identical? ts :skip)
        (.add out tuple)
        (.add out (r/join-tuples tuple ts)))
      (let [vs (object-array (int nvs))]
        (loop [next? (lmdb/seek-key iter te :id)
               ai    0
               vi    0]
          (if (and next? (< ^long ai ^long na))
            (let [vb ^ByteBuffer (lmdb/next-val iter)
                  a  (.getInt vb 0)]
              (if (== ^int a ^int (aget aids ai))
                (let [v    (idx/avg-buffer->v lmdb vb)
                      pred (aget preds ai)
                      fidx (aget fidxs ai)]
                  (if (and (or (nil? pred) (pred v))
                           (or (nil? fidx) (= v (aget tuple (int fidx)))))
                    (if (aget skips ai)
                      (recur (lmdb/has-next-val iter) (u/long-inc ai) vi)
                      (do (aset vs (int vi) v)
                          (recur (lmdb/has-next-val iter) (u/long-inc ai)
                                 (u/long-inc vi))))
                    :reject))
                (recur (lmdb/has-next-val iter) ai vi)))
            (when (== ^long ai ^long na)
              (if (zero? ^long nvs)
                (do (.put seen te :skip)
                    (.add out tuple))
                (do (.put seen te vs)
                    (.add out (r/join-tuples tuple vs)))))))))))

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
            fa ^int (aget aids 0)
            la ^int (aget aids (dec ^long na))]
        (dotimes [i na] (aset vs i (FastList.)))
        (loop [next? (lmdb/seek-key iter te :id)
               gi    0
               pa    (int (aget aids 0))
               in?   false]
          (when next?
            (let [vb ^ByteBuffer (lmdb/next-val iter)
                  a  (.getInt vb 0)]
              (cond
                (neg? (Integer/compare a fa))
                (recur (lmdb/has-next-val iter) gi pa false)
                (not (pos? (Integer/compare a la)))
                (let [gi (if (== pa ^int a)
                           gi
                           (if in? (inc gi) gi))
                      s  (aget gstarts gi)]
                  (if (== ^int a ^int (aget aids s))
                    (let [v (idx/avg-buffer->v lmdb vb)]
                      (dotimes [i (aget gcounts gi)]
                        (let [aj   (+ s i)
                              pred (aget preds aj)
                              fidx (aget fidxs aj)]
                          (when (and (or (nil? pred) (pred v))
                                     (or (nil? fidx)
                                         (= v (aget tuple (int fidx)))))
                            (.add ^FastList (aget vs aj) v))))
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
                   (fn [^ByteBuffer vb]
                     (.add ts (object-array [(.getLong vb 0)])))
                   (b/indexable nil aid v vt nil) :avg vt true)
      (if (.isEmpty ts)
        (.put seen v :no-result)
        (do (.put seen v ts)
            (.addAll out (r/prod-tuples (r/single-tuples tuple) ts)))))))

(defn- val-eq-scan-e-bound*
  [iter ^Collection out tuple aid v vt bound]
  (visit-list* iter (fn [^ByteBuffer vb]
                      (let [e (.getLong vb 0)]
                        (when (= ^long e ^long bound)
                          (.add out (r/conj-tuple tuple e)))))
               (b/indexable nil aid v vt nil) :avg vt true))

(defn- val-eq-filter-e*
  [iter ^Collection out tuple aid v vt old-e]
  (visit-list* iter
               (fn [^ByteBuffer vb]
                 (when (== (.getLong vb 0) ^long old-e)
                   (.add out tuple)))
               (b/indexable nil aid v vt nil) :avg vt true))

(defn- single-attrs?
  [schema attrs-v]
  (let [attrs (mapv first attrs-v)]
    (and (apply distinct? attrs)
         (not-any? #(identical? (-> % schema :db/cardinality)
                                :db.cardinality/many)
                   attrs))))

(defn- ea->avg-buffer
  [schema lmdb e a]
  (when-let [aid (:db/aid (schema a))]
    (when-let [^ByteBuffer bf (near-list lmdb c/eav e aid :id :int)]
      (when (= ^int aid (.getInt bf 0))
        bf))))

(defprotocol IStateSync
  (mark-state-current! [this last-modified-ms])
  (ensure-current! [this]))

(defn maybe-ensure-current!
  [this]
  (if (satisfies? IStateSync this)
    (ensure-current! this)
    this))

(declare insert-datom delete-datom fulltext-index vector-index embedding-index
         idoc-index check
         load-datoms-with-plan! prepare-embedding-plan
         prepare-datoms-kv-plan
         commit-datoms-kv-plan!
         ensure-embedding-vector!
         migrate-attr-values transact-opts ->SamplingWork e-sample*
         default-ratio* analyze*
         init-idoc-domains init-idoc-indices
         apply-schema-update! transfer-current)

(defn- merge-missing-idoc-indices
  [lmdb idoc-indices schema opts]
  (let [missing (into {}
                      (remove (fn [[domain _]]
                                (contains? idoc-indices domain)))
                      (init-idoc-domains schema opts))]
    (if (seq missing)
      (merge idoc-indices (init-idoc-indices lmdb missing))
      idoc-indices)))

(defprotocol ^:no-doc IStoreIdocIndices
  (store-idoc-indices [store]))

(deftype Store [lmdb
                search-engines
                vector-indices
                embedding-indices
                ^:volatile-mutable idoc-indices
                embedding-providers
                ^ConcurrentHashMap counts   ; aid -> touched times
                ^:volatile-mutable opts
                ^:volatile-mutable schema
                ^:volatile-mutable rschema
                ^:volatile-mutable attrs    ; aid -> attr
                ^:volatile-mutable max-aid
                ^:volatile-mutable max-gt
                ^:volatile-mutable max-tx
                ^:volatile-mutable state-sync-ms
                scheduled-sampling
                write-txn
                ^ReentrantReadWriteLock sampling-lock
                ^:volatile-mutable local-closed?
                shared-dir-key]

  IStoreIdocIndices

  (store-idoc-indices [_] idoc-indices)

  IWriting

  (write-txn [_] write-txn)

  IStateSync

  (mark-state-current! [this last-modified-ms]
    (set! state-sync-ms (long (or last-modified-ms 0)))
    this)

  (ensure-current! [this]
    (when-not (closed? this)
      (let [last-modified-ms (init-state-sync-ms lmdb)]
        (when (< ^long state-sync-ms ^long last-modified-ms)
          (let [schema* (load-schema lmdb)]
            (set! schema schema*)
            (set! rschema (schema->rschema schema*))
            (set! attrs (init-attrs schema*))
            (set! max-aid (init-max-aid schema*))
            (set! idoc-indices
                  (merge-missing-idoc-indices lmdb idoc-indices schema* opts))
            (mark-state-current! this last-modified-ms)))))
    this)

  IStore

  (opts [_] opts)

  (assoc-opt [this k v]
    (let [k'       (c/canonical-wal-option-key k)
          new-opts (apply-option-mutations opts {k v})]
      (vld/validate-ha-store-opts new-opts)
      (if (= opts new-opts)
        opts
        (do
          (set! opts new-opts)
          (let [res (transact-opts lmdb new-opts)]
            (when (contains? async-secondary-index-option-keys k')
              (enqueue-secondary-index-work! this)
              nil)
            res)))))

  (assoc-opts [this kvs]
    (let [new-opts (apply-option-mutations opts kvs)]
      (vld/validate-ha-store-opts new-opts)
      (if (= opts new-opts)
        opts
        (do
          (set! opts new-opts)
          (let [res (transact-opts lmdb new-opts)]
            (when (some async-secondary-index-option-keys
                        (map c/canonical-wal-option-key (keys kvs)))
              (enqueue-secondary-index-work! this))
            res)))))

  (db-name [_] (:db-name opts))

  (dir [_] (env-dir lmdb))

  (close [this]
    (when-not local-closed?
      (case (release-shared-local-store! this)
        :detached
        (set! local-closed? true)

        :close
        (do
          (set! local-closed? true)
          (close-store-resources! this))))
    nil)

  (closed? [_] (or local-closed? (closed-kv? lmdb)))

  (last-modified [_] (get-value lmdb c/meta :last-modified :attr :long))

  (max-gt [_] max-gt)

  (advance-max-gt [_] (set! max-gt (inc ^long max-gt)))

  (max-tx [_] max-tx)

  (advance-max-tx [_] (set! max-tx (inc ^long max-tx)))

  (max-aid [_] max-aid)

  (schema [_] schema)

  (rschema [_] rschema)

  (set-schema [this new-schema]
    (if-not (lmdb/writing? lmdb)
      ;; Route direct callers through the same serialized, atomic operation.
      (datalevin.interface/set-schema this new-schema nil nil)
      (let [new-schema (prepare-schema-update schema new-schema)]
        (when (seq new-schema)
          (vld/validate-schema (merge schema new-schema)))
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
        (when (schema-update-required? schema new-schema)
          ;; `new-schema` is already the complete effective definition for
          ;; every updated attribute. Persist it directly so an explicitly
          ;; retracted property is not reintroduced by applying the patch a
          ;; second time.
          (transact-schema lmdb (update-schema schema new-schema))
          (set! schema (load-schema lmdb))
          (set! rschema (schema->rschema schema))
          (set! attrs (init-attrs schema))
          (set! max-aid (init-max-aid schema))
          (mark-state-current! this (init-state-sync-ms lmdb)))
        schema)))

  (set-schema [this schema-update del-attrs rename-map]
    ;; Keep the LMDB write lock through commit and in-memory state adoption.
    ;; The nested lock taken by with-transaction-kv is reentrant.
    (locking (lmdb/write-txn lmdb)
      (let [committed-state (volatile! nil)
            result
            (lmdb/with-transaction-kv [tx-lmdb lmdb]
              (let [tx-store (transfer-current this tx-lmdb)
                    result   (apply-schema-update!
                               tx-store schema-update del-attrs rename-map)]
                (vreset! committed-state
                         {:schema  result
                          :max-aid (datalevin.interface/max-aid tx-store)
                          :max-gt  (datalevin.interface/max-gt tx-store)})
                result))
            {schema*  :schema
             max-aid* :max-aid
             max-gt*  :max-gt} @committed-state]
        (set! schema schema*)
        (set! rschema (schema->rschema schema*))
        (set! attrs (init-attrs schema*))
        (set! max-aid (max ^long max-aid ^long max-aid*))
        (set! max-gt (max ^long max-gt ^long max-gt*))
        ;; Opening a new idoc domain creates auxiliary DBIs, which must happen
        ;; only after the outermost LMDB write transaction has committed.
        (when-not (lmdb/writing? lmdb)
          (set! idoc-indices
                (merge-missing-idoc-indices lmdb idoc-indices schema* opts)))
        (mark-state-current! this (init-state-sync-ms lmdb))
        result)))

  (attrs [_] attrs)

  (init-max-eid [_]
    (let [e (volatile! c/e0)]
      (scan/visit-key-range
        lmdb c/eav
        (fn [eid]
          (vreset! e eid)
          :datalevin/terminate-visit)
        [:all-back] :id false)
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
      (mark-state-current! this (init-state-sync-ms lmdb))
      p))

  (del-attr [this attr]
    (locking (lmdb/write-txn lmdb)
      (if-let [props (schema attr)]
        (do
          (vld/validate-attr-deletable
            (.populated?
              this :ave (d/datom c/e0 attr c/v0) (d/datom c/emax attr c/vmax)))
          (let [aid (props :db/aid)]
            (transact-kv
              lmdb [(lmdb/kv-tx :del c/schema attr :attr)
                    (lmdb/kv-tx :put c/meta :last-modified
                                (System/currentTimeMillis) :attr :long)])
            (set! schema (dissoc schema attr))
            (set! rschema (schema->rschema schema))
            (set! attrs (dissoc attrs aid))
            (mark-state-current! this (init-state-sync-ms lmdb))
            attrs))
        attrs)))

  (rename-attr [this attr new-attr]
    (locking (lmdb/write-txn lmdb)
      (let [props     (schema attr)
            new-props (schema new-attr)]
        (cond
          (= attr new-attr)
          attrs

          (and props new-props)
          (u/raise "Cannot rename attribute: target already exists"
                   {:error     :schema/rename-conflict
                    :attribute attr
                    :target    new-attr})

          props
          (do
            (transact-kv
              lmdb [(lmdb/kv-tx :del c/schema attr :attr)
                    (lmdb/kv-tx :put c/schema new-attr props :attr)
                    (lmdb/kv-tx :put c/meta :last-modified
                                (System/currentTimeMillis) :attr :long)])
            (set! schema (-> schema (dissoc attr) (assoc new-attr props)))
            (set! rschema (schema->rschema schema))
            (set! attrs (assoc attrs (props :db/aid) new-attr))
            (mark-state-current! this (init-state-sync-ms lmdb))
            attrs)

          ;; A replay after a successful rename sees only the target.
          new-props
          attrs

          :else
          (u/raise "Cannot rename missing attribute"
                   {:error     :schema/missing-attribute
                    :attribute attr
                    :target    new-attr})))))

  (datom-count [_ index]
    (entries lmdb (if (string? index) index (index->dbi index))))

  (load-datoms [this datoms]
    (load-datoms-with-plan! this datoms (prepare-embedding-plan this datoms)))

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
       (index->k index schema high-datom true)] (index->ktype index)))

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
    (when-let [bf (ea->avg-buffer schema lmdb e a)]
      (d/datom e a (idx/avg-buffer->v lmdb bf))))

  (ea-first-v [_ e a]
    (when-let [bf (ea->avg-buffer schema lmdb e a)]
      (idx/avg-buffer->v lmdb bf)))

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
            nvs       (count (remove :skip? maps))
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
                  (eav-scan-v-single* lmdb iter na nvs out tuple eid-idx
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
            nvs       (count (remove :skip? maps))
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
                  lmdb iter na nvs out (.get ^List in i) eid-idx seen aids
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
              in       (sort-tuples-by-val in v-idx vt)
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
              in       (sort-tuples-by-val in v-idx vt)
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
              in       (sort-tuples-by-val in v-idx vt)
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

(defn- fulltext-op-ref
  [op]
  (let [kind (nth op 0)
        d    (nth op 1)]
    (case kind
      ;; Keep e and aid in giant refs so projected reads need not load the value.
      (:g :r) [:g (nth d 2) (nth d 0) (nth d 1)]
      (:a :d) d)))

(defn- apply-fulltext-op!
  [search-engines res]
  (let [op (peek res)
        d  (nth op 1)
        ref (fulltext-op-ref op)]
    (doseq [domain (nth res 0)
            :let   [engine (search-engines domain)]]
      (case (nth op 0)
        (:a :g) (add-doc engine ref (peek d) false)
        (:d :r) (remove-doc engine ref)))))

(defn- fulltext-entry
  [ref text]
  (let [entry (object-array 2)]
    (aset entry 0 ref)
    (aset entry 1 text)
    entry))

(defn- fulltext-op-entry
  [kind ref text]
  (let [entry (object-array 3)]
    (aset entry 0 kind)
    (aset entry 1 ref)
    (aset entry 2 text)
    entry))

(def ^:private ^:const max-fulltext-batch-size 1024)

(defn- add-fulltext-batches!
  [engine ^FastList entries]
  (let [n (long (.size entries))]
    (loop [start (long 0)]
      (when (< start n)
        (let [end (long (min n (+ start max-fulltext-batch-size)))]
          (s/add-docs engine (.subList entries (int start) (int end)))
          (recur end))))))

(defn- transact-fulltext-batches!
  [engine ^FastList entries]
  (let [n (long (.size entries))]
    (loop [start (long 0)]
      (when (< start n)
        (let [end (long (min n (+ start max-fulltext-batch-size)))]
          (s/transact-docs engine (.subList entries (int start) (int end)))
          (recur end))))))

(defn fulltext-index
  [search-engines ft-ds]
  (let [^FastList ft-ds ft-ds
        n               (.size ft-ds)]
    (if (= n 1)
      (apply-fulltext-op! search-engines (.get ft-ds 0))
      (let [add-only? (loop [idx 0]
                        (if (< idx n)
                          (let [op   (peek (.get ft-ds idx))
                                kind (nth op 0)]
                            (if (or (identical? kind :a)
                                    (identical? kind :g))
                              (recur (unchecked-inc-int idx))
                              false))
                          true))]
        (if add-only?
          (let [batches (IdentityHashMap.)]
            (doseq [res    ft-ds
                    :let   [op   (peek res)
                            d    (nth op 1)]
                    domain (nth res 0)
                    :let   [engine (search-engines domain)
                            ^FastList entries
                            (or (.get batches engine)
                                (let [entries (FastList.)]
                                  (.put batches engine entries)
                                  entries))]]
              (.add entries
                    (fulltext-entry
                      (fulltext-op-ref op)
                      (peek d))))
            (doseq [[engine entries] batches]
              (add-fulltext-batches! engine entries)))
          (let [batches (IdentityHashMap.)]
            (doseq [res    ft-ds
                    :let   [op   (peek res)
                            d    (nth op 1)
                            kind (nth op 0)]
                    domain (nth res 0)
                    :let   [engine (search-engines domain)
                            ^FastList entries
                            (or (.get batches engine)
                                (let [entries (FastList.)]
                                  (.put batches engine entries)
                                  entries))]]
              (.add entries
                    (case kind
                      :a (fulltext-op-entry :add d (peek d))
                      :d (fulltext-op-entry :delete d nil)
                      :g (fulltext-op-entry :add (fulltext-op-ref op)
                                            (peek d))
                      :r (fulltext-op-entry :delete (fulltext-op-ref op)
                                            nil))))
            (doseq [[engine entries] batches]
              (transact-fulltext-batches! engine entries))))))))

(defn- apply-vector-op!
  [vector-indices res]
  (let [op (peek res)
        d  (nth op 1)]
    (doseq [domain (nth res 0)
            :let   [index (vector-indices domain)]]
      (case (nth op 0)
        :a (add-vec index d (peek d))
        :d (remove-vec index d)
        :g (add-vec index [:g (nth d 2) (nth d 0) (nth d 1)] (peek d))
        :r (remove-vec index [:g (nth d 2) (nth d 0) (nth d 1)])))))

(defn- vector-entry
  [ref value]
  (let [entry (object-array 2)]
    (aset entry 0 ref)
    (aset entry 1 value)
    entry))

(defn vector-index
  [vector-indices vi-ds]
  (let [^FastList vi-ds vi-ds
        n               (.size vi-ds)]
    (if (= n 1)
      (apply-vector-op! vector-indices (.get vi-ds 0))
      (let [add-only? (loop [idx 0]
                        (if (< idx n)
                          (let [op   (peek (.get vi-ds idx))
                                kind (nth op 0)]
                            (if (or (identical? kind :a)
                                    (identical? kind :g))
                              (recur (unchecked-inc-int idx))
                              false))
                          true))]
        (if add-only?
          (let [batches (IdentityHashMap.)]
            (doseq [res    vi-ds
                    :let   [op   (peek res)
                            d    (nth op 1)
                            kind (nth op 0)]
                    domain (nth res 0)
                    :let   [index   (vector-indices domain)
                            ^FastList entries
                            (or (.get batches index)
                                (let [entries (FastList.)]
                                  (.put batches index entries)
                                  entries))]]
              (.add entries (vector-entry
                             (if (identical? kind :g)
                               [:g (nth d 2) (nth d 0) (nth d 1)]
                               d)
                             (peek d))))
            (doseq [[index entries] batches]
              (v/add-vecs index entries)))
          (doseq [res vi-ds]
            (apply-vector-op! vector-indices res)))))))

(defn embedding-index
  [embedding-indices em-ds]
  (doseq [res em-ds
          :let [[domain op] res
                index       (embedding-indices domain)]]
    (case (nth op 0)
      :a (let [[doc-ref vec-data] (nth op 1)]
           (add-vec index doc-ref vec-data))
      :d (remove-vec index (nth op 1)))))

(defn- idoc-op-ref
  [op]
  (let [kind (nth op 0)
        d    (nth op 1)]
    (case kind
      ;; Keep e and aid in giant refs so projected reads need not load the value.
      (:g :r) [:g (nth d 2) (nth d 0) (nth d 1)]
      (:a :d) d)))

(defn- plan-idoc-update!
  [index txs state-actions pending-paths pending-doc-ids old-op new-op]
  (let [old-d   (nth old-op 1)
        new-d   (nth new-op 1)
        old-ref (idoc-op-ref old-op)
        new-ref (idoc-op-ref new-op)
        old-doc (peek old-d)
        new-doc (peek new-d)
        patch   (some-> (meta new-op) :idoc/patch)
        res     (if patch
                  (idoc/patch-doc-plan!
                    index txs state-actions
                    pending-paths pending-doc-ids
                    old-ref old-doc new-ref new-doc patch)
                  (idoc/update-doc-plan!
                    index txs state-actions
                    pending-paths pending-doc-ids
                    old-ref old-doc new-ref new-doc))]
    (when (= res :doc-missing)
      (idoc/remove-doc-plan! index txs state-actions
                             pending-paths pending-doc-ids
                             old-ref old-doc)
      (idoc/add-doc-plan! index txs state-actions
                          pending-paths pending-doc-ids
                          new-ref new-doc false))))

(defn- fast-idoc-update!
  [idoc-indices ^FastList id-ds txs state-actions]
  (when (= 2 (.size id-ds))
    (let [res0    (.get id-ds 0)
          res1    (.get id-ds 1)
          domain0 (nth res0 0)
          domain1 (nth res1 0)
          op0     (peek res0)
          op1     (peek res1)
          kind0   (nth op0 0)
          kind1   (nth op1 0)
          [old-op new-op]
          (cond
            (and (or (identical? kind0 :d) (identical? kind0 :r))
                 (or (identical? kind1 :a) (identical? kind1 :g)))
            [op0 op1]

            (and (or (identical? kind1 :d) (identical? kind1 :r))
                 (or (identical? kind0 :a) (identical? kind0 :g)))
            [op1 op0])]
      (when (and old-op
                 (= domain0 domain1)
                 (let [old-d (nth old-op 1)
                       new-d (nth new-op 1)]
                   (and (= (nth old-d 0) (nth new-d 0))
                        (= (nth old-d 1) (nth new-d 1)))))
        (let [index (idoc-indices domain0)]
          (plan-idoc-update! index txs state-actions
                              (HashMap.) (HashMap.) old-op new-op))
        true))))

(defn idoc-index
  [idoc-indices id-ds txs]
  (let [state-actions (FastList.)]
    (if (fast-idoc-update! idoc-indices id-ds txs state-actions)
      state-actions
      (let [updates (volatile! {})
            path-plans (IdentityHashMap.)
            doc-plans  (IdentityHashMap.)
            path-plan  (fn [index]
                         (or (.get path-plans index)
                             (let [m (HashMap.)]
                               (.put path-plans index m)
                               m)))
            doc-plan   (fn [index]
                         (or (.get doc-plans index)
                             (let [m (HashMap.)]
                               (.put doc-plans index m)
                               m)))]
        (doseq [res  id-ds
                :let [op     (peek res)
                      d      (nth op 1)
                      domain (nth res 0)
                      kind   (nth op 0)]]
          (case kind
            (:a :g)
            (let [k [(nth d 0) (nth d 1)]]
              (vswap! updates update-in [domain k :a] (fnil conj []) op))
            (:d :r)
            (let [k [(nth d 0) (nth d 1)]]
              (vswap! updates update-in [domain k :d] (fnil conj []) op))))
        (doseq [[domain domain-ops] @updates
                :let         [index (idoc-indices domain)
                              pending-paths (path-plan index)
                              pending-doc-ids (doc-plan index)]]
          (doseq [[_ {:keys [a d]}] domain-ops
                  :let              [na (count a)
                                     nd (count d)]]
            (cond
              (and (= 1 na) (= 1 nd))
              (plan-idoc-update! index txs state-actions
                                  pending-paths pending-doc-ids
                                  (first d) (first a))

              (and (= 1 na) (zero? nd))
              (let [op  (first a)
                    od  (nth op 1)]
                (idoc/add-doc-plan! index txs state-actions
                                    pending-paths pending-doc-ids
                                    (idoc-op-ref op) (peek od) false))

              (and (zero? na) (= 1 nd))
              (let [op  (first d)
                    od  (nth op 1)]
                (idoc/remove-doc-plan! index txs state-actions
                                       pending-paths pending-doc-ids
                                       (idoc-op-ref op) (peek od)))

              :else
              (let [adds (mapv (fn [op]
                                 (let [d (nth op 1)]
                                   [(idoc-op-ref op) (peek d)]))
                               a)
                    rems (mapv (fn [op]
                                 (let [d (nth op 1)]
                                   [(idoc-op-ref op) (peek d)]))
                               d)]
                (idoc/add-docs-plan! index txs state-actions
                                     pending-paths pending-doc-ids adds false)
                (idoc/remove-docs-plan! index txs state-actions
                                        pending-paths pending-doc-ids rems)))))
        state-actions))))

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
        ;; Sampling metadata is an advisory cache; query reads should still
        ;; succeed if persisting it loses a WAL race or times out.
        (try
          (transact-kv lmdb (map-indexed
                              (fn [i ^objects t]
                                [:put c/meta [aid i] ^long (aget t 0)
                                 :int-int :id])
                              ts))
          (catch Exception _)))
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
            (try
              (transact-kv lmdb [[:put c/meta [aid :ratio] ratio :data :double]])
              (catch Exception _)))
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

(defn- validate-schema-operations
  [schema-update del-attrs rename-map]
  (vld/validate-schema-update schema-update)
  (when-not (or (nil? del-attrs)
                (set? del-attrs)
                (sequential? del-attrs))
    (u/raise "Schema attributes to delete must be a set or sequence"
             {:error :schema/validation
              :value del-attrs}))
  (doseq [attr del-attrs]
    (when-not (keyword? attr)
      (u/raise "Schema attribute to delete must be a keyword"
               {:error     :schema/validation
                :attribute attr})))
  (when-not (or (nil? rename-map) (map? rename-map))
    (u/raise "Schema attribute renames must be a map"
             {:error :schema/validation
              :value rename-map}))
  (doseq [[old new] rename-map]
    (when-not (and (keyword? old) (keyword? new))
      (u/raise "Schema rename attributes must be keywords"
               {:error     :schema/validation
                :attribute old
                :target    new}))))

(defn- normalize-schema-renames
  [rename-map]
  (let [renames (into {} (remove (fn [[old new]] (= old new))) rename-map)
        targets (vec (vals renames))]
    (when-not (= (count targets) (count (set targets)))
      (u/raise "Schema rename targets must be unique"
               {:error      :schema/rename-conflict
                :rename-map rename-map}))
    (let [sources (set (keys renames))
          overlap (set (filter sources targets))]
      (when (seq overlap)
        (u/raise "Schema rename chains and cycles are not supported"
                 {:error      :schema/rename-conflict
                  :attributes overlap
                  :rename-map rename-map})))
    renames))

(defn- schema-rename-plans
  [current-schema schema-update renames]
  (reduce-kv
    (fn [plans old new]
      (let [old?       (contains? current-schema old)
            new?       (contains? current-schema new)
            patch-old? (contains? schema-update old)]
        (cond
          (and old? new?)
          (u/raise "Cannot rename attribute: target already exists"
                   {:error     :schema/rename-conflict
                    :attribute old
                    :target    new})

          old?
          (conj plans {:old old :new new :canonical old :pending? true})

          new?
          (conj plans {:old old :new new :canonical new :pending? false})

          patch-old?
          (conj plans {:old old :new new :canonical old :pending? true})

          :else
          (u/raise "Cannot rename missing attribute"
                   {:error     :schema/missing-attribute
                    :attribute old
                    :target    new}))))
    [] renames))

(defn- resolve-renamed-schema-patches
  [schema-update rename-plans]
  (let [aliases
        (reduce
          (fn [m {:keys [old new canonical]}]
            (-> m (assoc old canonical) (assoc new canonical)))
          {} rename-plans)]
    (reduce-kv
      (fn [resolved attr property-patch]
        (let [canonical (get aliases attr attr)]
          (when (contains? resolved canonical)
            (u/raise "Schema patches resolve to the same renamed attribute"
                     {:error     :schema/rename-conflict
                      :attribute canonical}))
          (assoc resolved canonical property-patch)))
      {} schema-update)))

(defn- populated-attr?
  [store attr]
  (populated? store :ave
              (d/datom c/e0 attr c/v0)
              (d/datom c/emax attr c/vmax)))

(defn- plan-schema-update
  [^Store store schema-update del-attrs rename-map]
  (validate-schema-operations schema-update del-attrs rename-map)
  (let [schema-update (or schema-update {})
        deletions     (vec (set (or del-attrs [])))
        deletion-set (set deletions)
        renames       (normalize-schema-renames (or rename-map {}))
        endpoints     (concat (keys renames) (vals renames))]
    (when-let [attr (first (filter deletion-set (keys schema-update)))]
      (u/raise "Cannot patch and delete the same schema attribute"
               {:error     :schema/update-conflict
                :attribute attr}))
    (when-let [attr (first (filter deletion-set endpoints))]
      (u/raise "Cannot delete an attribute participating in a rename"
               {:error     :schema/update-conflict
                :attribute attr}))
    (let [current-schema  (schema store)
          rename-plans   (schema-rename-plans
                           current-schema schema-update renames)
          resolved-update (resolve-renamed-schema-patches
                            schema-update rename-plans)
          prepared-update (prepare-schema-update
                            current-schema resolved-update)]
      ;; Check every property mutation before any value re-encoding begins.
      (when (seq prepared-update)
        (vld/validate-schema (merge current-schema prepared-update)))
      (doseq [[attr new] prepared-update
              :let       [old (current-schema attr)]
              :when      old]
        (check store attr old new))
      (let [patched-schema
            (if (seq prepared-update)
              (merge current-schema
                     (update-schema current-schema prepared-update))
              current-schema)
            deletions-to-apply
            (filterv #(contains? patched-schema %) deletions)]
        (doseq [attr deletions-to-apply]
          (vld/validate-attr-deletable (populated-attr? store attr)))
        (let [after-deletions (apply dissoc patched-schema deletions)
              final-schema
              (reduce
                (fn [result {:keys [old new pending?]}]
                  (if pending?
                    (let [props (result old)]
                      (when (or (nil? props) (contains? result new))
                        (u/raise "Schema rename cannot be applied"
                                 {:error     :schema/rename-conflict
                                  :attribute old
                                  :target    new}))
                      (-> result (dissoc old) (assoc new props)))
                    result))
                after-deletions rename-plans)
              renames-to-apply
              (into {} (keep (fn [{:keys [old new pending?]}]
                               (when pending? [old new])))
                    rename-plans)]
          (vld/validate-schema final-schema)
          {:schema-update resolved-update
           :del-attrs     deletions-to-apply
           :rename-map    renames-to-apply
           :final-schema  final-schema})))))

(defn- apply-schema-update!
  [store schema-update del-attrs rename-map]
  (let [{:keys [schema-update del-attrs rename-map final-schema]}
        (plan-schema-update store schema-update del-attrs rename-map)]
    (datalevin.interface/set-schema store schema-update)
    (doseq [attr del-attrs]
      (datalevin.interface/del-attr store attr))
    (doseq [[old new] rename-map]
      (datalevin.interface/rename-attr store old new))
    (let [result (schema store)]
      (when-not (= final-schema result)
        (u/raise "Schema update result differed from its validated plan"
                 {:error    :schema/update-conflict
                  :expected final-schema
                  :actual   result}))
      result)))

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
  [^Store store ^FastList ft-ds ^FastList ft-jobs attr props text ref job-op op]
  (when-not (str/blank? text)
    (doseq [domain (vec
                     (distinct
                       (cond-> (or (seq (props :db.fulltext/domains))
                                   [c/default-domain])
                         (props :db.fulltext/autoDomain)
                         (conj (u/keyword->string attr)))))]
      (if (si/async-indexing?
           (or (get-in (opts store) [:search-domains domain])
               (when (= c/default-domain domain) (:search-opts (opts store)))
               {}))
        (.add ft-jobs {:type :fulltext
                       :domain domain
                       :op job-op
                       :ref ref
                       :value text})
        (.add ft-ds [[domain] op])))))

(defn- embedding-attr-domains
  [attr props]
  (vec
    (distinct
      (cond-> (or (seq (props :db.embedding/domains))
                  [c/default-domain])
        (props :db.embedding/autoDomain) (conj (v/attr-domain attr))))))

(defn embedding-domain-config
  [^Store store domain]
  (get-in (opts store) [:embedding-domains domain]))

(defn- async-embedding-domain?
  [^Store store domain]
  (si/async-indexing? (embedding-domain-config store domain)))

(defn- vector-domain-config
  [^Store store domain]
  (or (get-in (opts store) [:vector-domains domain])
      (:vector-opts (opts store))
      {}))

(defn- async-vector-domain?
  [^Store store domain]
  (si/async-indexing? (vector-domain-config store domain)))

(defn embedding-provider
  [^Store store domain]
  (or (get-in (opts store) [:embedding-domain-providers domain])
      ((.-embedding-providers store) domain)))

(defn embedding-index-by-domain
  [^Store store domain]
  ((.-embedding-indices store) domain))

(defn secondary-index-jobs
  [^Store store]
  (mapv second
        (get-range (.-lmdb store)
                   c/secondary-index-jobs
                   [:all]
                   :data
                   :data)))

(defn- secondary-index-job
  [^Store store job-id]
  (get-value (.-lmdb store) c/secondary-index-jobs job-id :data :data))

(defn- max-long-value
  [a b]
  (if (some? a)
    (max (long a) (long b))
    (long b)))

(defn- min-long-value
  [a b]
  (if (some? a)
    (min (long a) (long b))
    (long b)))

(defn- latest-updated-job
  [a b]
  (if (or (nil? a)
          (< (long (or (:job/updated-ms a) 0))
             (long (or (:job/updated-ms b) 0))))
    b
    a))

(defn- maybe-update-stat
  [m k f v]
  (if (some? v)
    (update m k f v)
    m))

(defn- secondary-index-status-init
  []
  {:total-count 0
   :pending-count 0
   :running-count 0
   :completed-count 0
   :failed-count 0})

(defn- add-job-to-secondary-index-status
  [status job]
  (let [status (update status :total-count (fnil inc 0))
        tx (:job/tx job)
        status (maybe-update-stat status :last-enqueued-tx max-long-value tx)]
    (case (:job/status job)
      :pending
      (-> status
          (update :pending-count (fnil inc 0))
          (maybe-update-stat :oldest-pending-ms
                             min-long-value
                             (:job/created-ms job)))

      :completed
      (-> status
          (update :completed-count (fnil inc 0))
          (maybe-update-stat :last-completed-tx max-long-value tx))

      :running
      (-> status
          (update :running-count (fnil inc 0))
          (maybe-update-stat :oldest-running-ms
                             min-long-value
                             (:job/claimed-ms job))
          (maybe-update-stat :next-lease-ms
                             min-long-value
                             (:job/lease-until-ms job)))

      :failed
      (-> status
          (update :failed-count (fnil inc 0))
          (maybe-update-stat :last-failed-tx max-long-value tx)
          (maybe-update-stat :next-retry-ms
                             min-long-value
                             (:job/next-retry-ms job))
          (update :latest-failed-job latest-updated-job job))

      status)))

(defn- finalize-secondary-index-status
  [now-ms status]
  (let [failed-job (:latest-failed-job status)
        oldest-ms (:oldest-pending-ms status)
        oldest-running-ms (:oldest-running-ms status)]
    (cond-> (dissoc status :latest-failed-job)
      failed-job
      (assoc :last-error (:job/last-error failed-job))

      oldest-ms
      (assoc :oldest-pending-age-ms
             (max 0 (- (long now-ms) (long oldest-ms))))

      oldest-running-ms
      (assoc :oldest-running-age-ms
             (max 0 (- (long now-ms) (long oldest-running-ms)))))))

(defn secondary-index-status
  [^Store store]
  (let [jobs (secondary-index-jobs store)
        now-ms (System/currentTimeMillis)
        init-status (secondary-index-status-init)
        counts (reduce add-job-to-secondary-index-status init-status jobs)
        by-domain (reduce
                   (fn [acc job]
                     (let [k [(:job/type job) (:job/domain job)]]
                       (update acc k
                               #(add-job-to-secondary-index-status
                                 (or % init-status)
                                 job))))
                   {}
                   jobs)]
    (assoc (finalize-secondary-index-status now-ms counts)
           :by-domain
           (into {}
                 (map (fn [[k status]]
                        [k (finalize-secondary-index-status now-ms status)]))
                 by-domain))))

(defn- update-secondary-index-job!
  [^Store store job]
  (transact-kv (.-lmdb store) [(si/job-tx job)]))

(defn- embedding-job-item
  [job]
  {:text (:job/value job)
   :ref (:job/ref job)
   :kind :document
   :domain (:job/domain job)})

(defn- embedding-job-application
  [^Store store job]
  (let [domain (:job/domain job)
        ref (:job/ref job)
        index (or (embedding-index-by-domain store domain)
                  (u/raise "Embedding index is not initialized"
                           {:domain domain
                            :job job}))]
    (case (:job/op job)
      :add
      (let [provider (or (embedding-provider store domain)
                         (u/raise "Embedding provider is not initialized"
                                  {:domain domain
                                   :job job}))
            dimensions (get-in (embedding-domain-config store domain)
                               [:dimensions])
            vec-data (ensure-embedding-vector!
                      domain
                      dimensions
                      (first (emb/embedding provider
                                            [(embedding-job-item job)]
                                            nil)))]
        (fn []
          (remove-vec index ref)
          (add-vec index ref vec-data)))

      :delete
      (fn []
        (remove-vec index ref))

      (u/raise "Unsupported embedding secondary index op"
               {:op (:job/op job)
                :job job}))))

(defn- vector-job-application
  [^Store store job]
  (let [domain (:job/domain job)
        ref    (:job/ref job)
        index  (or ((.-vector-indices store) domain)
                   (u/raise "Vector index is not initialized"
                            {:domain domain
                             :job job}))]
    (case (:job/op job)
      :add
      (let [vec-data (:job/value job)]
        (fn []
          (remove-vec index ref)
          (add-vec index ref vec-data)))

      :delete
      (fn []
        (remove-vec index ref))

      (u/raise "Unsupported vector secondary index op"
               {:op (:job/op job)
                :job job}))))

(defn- remove-fulltext-doc-idempotently!
  [engine ref]
  (try
    (remove-doc engine ref)
    (catch clojure.lang.ExceptionInfo e
      (when-not (= "Document does not exist." (ex-message e))
        (throw e)))))

(defn- fulltext-job-application
  [^Store store job]
  (let [domain (:job/domain job)
        ref    (:job/ref job)
        engine (or ((.-search-engines store) domain)
                   (u/raise "Fulltext search engine is not initialized"
                            {:domain domain
                             :job job}))]
    (case (:job/op job)
      :add
      (let [doc-text (:job/value job)]
        (fn []
          (add-doc engine ref doc-text true)))

      :delete
      (fn []
        (remove-fulltext-doc-idempotently! engine ref))

      (u/raise "Unsupported fulltext secondary index op"
               {:op (:job/op job)
                :job job}))))

(defn- secondary-index-job-application
  [^Store store job]
  (case (:job/type job)
    :fulltext (fulltext-job-application store job)
    :vector (vector-job-application store job)
    :embedding (embedding-job-application store job)
    (u/raise "Unsupported secondary index job type"
             {:type (:job/type job)
              :job job})))

(defn- secondary-index-retry-delay-ms
  [^Store store job]
  (let [base-ms (long (get (opts store)
                           :async-secondary-index-retry-base-ms
                           c/*async-secondary-index-retry-base-ms*))
        max-ms  (long (get (opts store)
                           :async-secondary-index-retry-max-ms
                           c/*async-secondary-index-retry-max-ms*))
        attempts (inc (long (or (:job/attempts job) 0)))
        exp      (min 10 (dec attempts))
        delay-ms (* base-ms (bit-shift-left 1 exp))]
    (min max-ms delay-ms)))

(defn- due-failed-secondary-index-job?
  [now-ms job]
  (and (si/failed-job? job)
       (<= (long (or (:job/next-retry-ms job) 0))
           (long now-ms))))

(defn- expired-secondary-index-job-lease?
  [now-ms job]
  (and (si/running-job? job)
       (<= (long (or (:job/lease-until-ms job) 0))
           (long now-ms))))

(defn- previously-failed-secondary-index-job?
  [job]
  (pos? (long (or (:job/attempts job) 0))))

(defn- claimable-secondary-index-job?
  [now-ms retry-failed? retry-due-only? reclaim-failed-running? job]
  (or (si/pending-job? job)
      (expired-secondary-index-job-lease? now-ms job)
      (and retry-failed?
           reclaim-failed-running?
           (si/running-job? job)
           (previously-failed-secondary-index-job? job))
      (and retry-failed?
           (si/failed-job? job)
           (or (not retry-due-only?)
               (due-failed-secondary-index-job? now-ms job)))))

(defn- secondary-index-job-matches?
  [{:keys [tx type domain]} job]
  (and (or (nil? tx)
           (<= (long (:job/tx job)) (long tx)))
       (or (nil? type)
           (= type (:job/type job)))
       (or (nil? domain)
           (= domain (:job/domain job)))))

(defn- claim-secondary-index-job!
  [^Store store job owner lease-ms retry-failed? retry-due-only?
   reclaim-failed-running?]
  (locking (.-write-txn store)
    (let [now-ms (System/currentTimeMillis)]
      (when-let [current (secondary-index-job store (:job/id job))]
        (when (claimable-secondary-index-job? now-ms
                                              retry-failed?
                                              retry-due-only?
                                              reclaim-failed-running?
                                              current)
          (let [claimed (si/claimed-job current
                                        owner
                                        (+ (long now-ms) (long lease-ms))
                                        now-ms)]
            (update-secondary-index-job! store claimed)
            claimed))))))

(defn- claimed-secondary-index-job?
  [job owner]
  (and (si/running-job? job)
       (= owner (:job/lease-owner job))))

(defn- complete-claimed-secondary-index-job!
  [^Store store job owner apply-job!]
  (locking (.-write-txn store)
    (when-let [current (secondary-index-job store (:job/id job))]
      (when (claimed-secondary-index-job? current owner)
        (apply-job!)
        (update-secondary-index-job! store (si/completed-job current))
        true))))

(defn- fail-claimed-secondary-index-job!
  [^Store store job owner error]
  (locking (.-write-txn store)
    (when-let [current (secondary-index-job store (:job/id job))]
      (when (claimed-secondary-index-job? current owner)
        (update-secondary-index-job!
         store
         (si/failed-job current
                        error
                        (System/currentTimeMillis)
                        (secondary-index-retry-delay-ms store current)))
        true))))

(defn process-secondary-index-jobs!
  ([^Store store]
   (process-secondary-index-jobs! store nil))
  ([^Store store {:keys [max-jobs retry-due-only? reclaim-failed-running?]
                  :or {max-jobs Long/MAX_VALUE}
                  :as opts}]
   (let [now-ms (System/currentTimeMillis)
         owner (or (:owner opts)
                   (str (db-name store) "/" (UUID/randomUUID)))
         lease-ms (long (get (opts store)
                             :async-secondary-index-worker-lease-ms
                             c/*async-secondary-index-worker-lease-ms*))
         retry-failed? (true? (:retry-failed? opts))
         processable? #(claimable-secondary-index-job? now-ms
                                                       retry-failed?
                                                       retry-due-only?
                                                       reclaim-failed-running?
                                                       %)
         jobs (take (long max-jobs)
                    (filter #(and (secondary-index-job-matches? opts %)
                                  (processable? %))
                            (secondary-index-jobs store)))
         result (volatile! {:processed-count 0
                            :claimed-count 0
                            :completed-count 0
                            :failed-count 0
                            :skipped-count 0})
         inc-result! (fn [k]
                       (vswap! result update k (fnil u/long-inc 0)))]
     (doseq [job jobs]
       (inc-result! :processed-count)
       (if-let [claimed (claim-secondary-index-job! store
                                                    job
                                                    owner
                                                    lease-ms
                                                    retry-failed?
                                                    retry-due-only?
                                                    reclaim-failed-running?)]
         (do
           (inc-result! :claimed-count)
           (try
             (let [apply-job! (secondary-index-job-application store claimed)]
               (if (complete-claimed-secondary-index-job! store
                                                          claimed
                                                          owner
                                                          apply-job!)
                 (inc-result! :completed-count)
                 (inc-result! :skipped-count)))
             (catch Throwable e
               (if (fail-claimed-secondary-index-job! store claimed owner e)
                 (inc-result! :failed-count)
                 (inc-result! :skipped-count)))))
         (inc-result! :skipped-count)))
     (assoc @result :status (secondary-index-status store)))))

(defn- unfinished-secondary-index-jobs
  [^Store store opts]
  (filter #(and (secondary-index-job-matches? opts %)
                (si/unfinished-job? %))
          (secondary-index-jobs store)))

(defn wait-for-secondary-index
  ([^Store store]
   (wait-for-secondary-index store nil))
  ([^Store store {:keys [tx timeout-ms poll-ms process? max-jobs retry-failed?]
                  :or {timeout-ms 0
                       poll-ms 50}
                  :as opts}]
   (let [target-tx (long (or tx (max-tx store)))
         timeout-ms (max 0 (long timeout-ms))
         poll-ms (max 1 (long poll-ms))
         deadline-ms (+ (System/currentTimeMillis) timeout-ms)
         opts (assoc opts :tx target-tx)
         process-opts (merge (select-keys opts [:tx :type :domain])
                             {:max-jobs (or max-jobs Long/MAX_VALUE)
                              :retry-failed? retry-failed?
                              :reclaim-failed-running? retry-failed?})]
     (loop []
       (when process?
         (process-secondary-index-jobs! store process-opts))
       (let [unfinished (vec (unfinished-secondary-index-jobs store opts))
             status (secondary-index-status store)]
         (if (empty? unfinished)
           {:caught-up? true
            :target-tx target-tx
            :unfinished-count 0
            :failed-count 0
            :status status}
           (let [now-ms (System/currentTimeMillis)
                 failed-count (count (filter si/failed-job? unfinished))]
             (if (>= now-ms deadline-ms)
               {:caught-up? false
                :target-tx target-tx
                :unfinished-count (count unfinished)
                :failed-count failed-count
                :status status}
               (do
                 (Thread/sleep (min poll-ms
                                    (max 1 (- deadline-ms now-ms))))
                 (recur))))))))))

(defn- async-secondary-index-worker-opts
  [^Store store]
  {:max-jobs (long (get (opts store)
                        :async-secondary-index-worker-max-jobs
                        c/*async-secondary-index-worker-max-jobs*))
   :retry-failed? true
   :retry-due-only? true})

(defn- wait-for-secondary-index-time!
  [^Store store target-ms]
  (loop []
    (let [remaining-ms (- (long target-ms) (System/currentTimeMillis))]
      (when (and (pos? remaining-ms) (not (closed? store)))
        (Thread/sleep (min 1000 remaining-ms))
        (recur)))))

(deftype SecondaryIndexWork [^Store store exe]
  IAsyncWork
  (work-key [_]
    (->> (db-name store) hash (str "secondary-index") keyword))
  (do-work [_]
    (let [^Store store (or (current-shared-local-store (env-dir (.-lmdb store)))
                           store)]
      (when (and (a/running? exe)
                 (not (closed? store)))
        (try
          (let [result (process-secondary-index-jobs!
                        store
                        (async-secondary-index-worker-opts store))
                status (:status result)
                pending? (pos? (long (or (:pending-count status) 0)))
                next-retry-ms (:next-retry-ms status)
                next-lease-ms (:next-lease-ms status)]
            (cond
              (and pending? (not (closed? store)))
              (enqueue-secondary-index-work! store)

              (and next-retry-ms (not (closed? store)))
              (do
                (wait-for-secondary-index-time! store next-retry-ms)
                (enqueue-secondary-index-work! store))

              (and next-lease-ms (not (closed? store)))
              (do
                (wait-for-secondary-index-time! store next-lease-ms)
                (enqueue-secondary-index-work! store))))
          (catch Throwable _)))))
  (combine [_]
    (fn [works]
      (peek (vec works))))
  (callback [_] nil))

(defn enqueue-secondary-index-work!
  [^Store store]
  (when-not (closed? store)
    (let [exe (a/get-executor)]
      (when (a/running? exe)
        (a/exec-noresult exe (->SecondaryIndexWork store exe)))))
  store)

(defn ^:no-doc enqueue-secondary-index-work-if-needed!
  [^Store store]
  (when (some si/unfinished-job? (secondary-index-jobs store))
    (enqueue-secondary-index-work! store))
  store)

(declare provider-spec-for-domain)

(def ^:private persisted-embedding-space-keys
  #{:dimensions :embedding-metadata})

(defn- runtime-provider-space
  [dir runtime-providers domain domain-opts]
  (let [provider-spec (provider-spec-for-domain
                        dir
                        runtime-providers
                        domain
                        (apply dissoc domain-opts persisted-embedding-space-keys))]
    (emb/provider-space provider-spec)))

(defn- vector-dim
  [vec-data]
  (cond
    (u/array? vec-data)
    (java.lang.reflect.Array/getLength vec-data)

    (instance? java.util.List vec-data)
    (.size ^java.util.List vec-data)

    (sequential? vec-data)
    (count vec-data)

    :else
    (u/raise "Embedding provider returned an unsupported vector value"
             {:vector vec-data})))

(defn- ensure-embedding-vector!
  [domain expected-dimensions vec-data]
  (let [dimensions (vector-dim vec-data)]
    (when (and expected-dimensions
               (not= (long expected-dimensions) (long dimensions)))
      (u/raise "Embedding vector dimensions do not match domain configuration"
               {:domain              domain
                :expected-dimensions expected-dimensions
                :actual-dimensions   dimensions}))
    vec-data))

(defn prepare-embedding-plan
  [^Store store datoms]
  (let [schema  (schema store)
        batches (reduce
                  (fn [m ^Datom datom]
                    (let [attr  (.-a datom)
                          props (schema attr)
                          v     (.-v datom)]
                      (if (and props
                               (props :db/embedding)
                               (d/datom-added datom)
                               (string? v))
                        (reduce
                          (fn [m domain]
                            (update m domain conj
                                    {:datom datom
                                     :text  v
                                     :attr  attr
                                     :ref   [(.-e datom) attr v]
                                     :kind  :document
                                     :domain domain}))
                          m
                          (remove #(async-embedding-domain? store %)
                                  (embedding-attr-domains attr props)))
                        m)))
                  {}
                  datoms)]
    (when (seq batches)
      (let [plan (IdentityHashMap.)]
        (doseq [[domain items] batches
                :let [provider    (or (embedding-provider store domain)
                                      (u/raise "Embedding provider is not initialized"
                                               {:domain domain}))
                      dimensions (get-in (embedding-domain-config store domain)
                                         [:dimensions])
                      vectors    (emb/embedding provider
                                                (mapv #(dissoc % :datom) items)
                                                nil)]]
          (when-not (= (count items) (count vectors))
            (u/raise "Embedding provider returned the wrong number of vectors"
                     {:domain  domain
                      :items   (count items)
                      :vectors (count vectors)}))
          (doseq [[item vec-data] (map vector items vectors)]
            (let [datom      (:datom item)
                  domain-map (or (.get plan datom)
                                 (let [m (HashMap.)]
                                   (.put plan datom m)
                                   m))]
              (.put ^HashMap domain-map domain
                    (ensure-embedding-vector! domain dimensions vec-data)))))
        plan))))

(defn load-datoms-with-plan!
  ([^Store store datoms embedding-plan]
   (load-datoms-with-plan! store datoms embedding-plan nil))
  ([^Store store datoms embedding-plan {:keys [extra-kv-txs last-modified-ms]}]
   (let [[res secondary-index-job-count]
         (locking (.-write-txn store)
           (let [plan (prepare-datoms-kv-plan store
                                              datoms
                                              embedding-plan
                                              extra-kv-txs
                                              last-modified-ms)
                 res  (commit-datoms-kv-plan!
                       (.-lmdb store)
                       (.-search-engines store)
                       (.-vector-indices store)
                       (.-embedding-indices store)
                       (store-idoc-indices store)
                       plan)]
             [res (:secondary-index-job-count plan)]))]
     (when (pos? (long (or secondary-index-job-count 0)))
       (enqueue-secondary-index-work! store))
     res)))

(defn- write-attr-info
  [^Store store ^HashMap attr-infos attr value insert?]
  (or (.get attr-infos attr)
      (let [schema (schema store)
            props  (schema attr)
            _      (when insert?
                     (vld/validate-closed-schema
                       schema (opts store) attr value))
            props  (if insert?
                     (or props (swap-attr store attr identity))
                     props)
            info   (object-array
                     [props
                      (value-type props)
                      (:db/aid props)
                      (:db/embedding props)
                      (:db/fulltext props)])]
        (when props (.put attr-infos attr info))
        info)))

(defn- insert-datom
  [^Store store ^Datom d ^FastList txs ^FastList ft-ds ^FastList vi-ds
   ^FastList ft-jobs ^FastList vi-jobs ^FastList em-ds ^FastList em-jobs
   ^FastList id-ds ^HashMap giants ^HashMap attr-infos embedding-plan
   ^ByteBuffer avg-bf]
  (let [attr       (.-a d)
        e          (.-e d)
        v          (.-v d)
        ^objects ai (write-attr-info store attr-infos attr v true)
        props      (aget ai 0)
        vt         (aget ai 1)
        aid        (aget ai 2)
        embedding? (aget ai 3)
        fulltext?  (aget ai 4)
        max-gt     (max-gt store)
        i          (b/indexable nil aid v vt max-gt)
        giant?     (b/giant? i)]
    (.add txs (DatomKVTxData. e (b/indexable-bytes i avg-bf) true))
    (when giant?
      (.advance-max-gt store)
      (let [gd [e attr v]
            {:keys [value vtype]} (encode-giant-datom (apply d/datom gd))]
        (.put giants gd max-gt)
        (.add txs (lmdb/kv-tx :put c/giants max-gt value
                              :id vtype [:append]))))
    (when (identical? vt :db.type/vec)
      (let [ref     (if giant? [:g max-gt e aid] [e aid v])
            op      (if giant? [:g [e aid max-gt v]] [:a [e aid v]])
            domains (conjv (props :db.vec/domains) (v/attr-domain attr))]
        (doseq [domain domains]
          (if (async-vector-domain? store domain)
            (.add vi-jobs {:type :vector
                           :domain domain
                           :op :add
                           :ref ref
                           :value v})
            (.add vi-ds [[domain] op])))))
    (when embedding?
      (let [doc-ref     (if giant? [:g max-gt e aid] [e aid v])
            domain-vecs (some-> ^IdentityHashMap embedding-plan (.get d))]
        (doseq [domain (embedding-attr-domains attr props)]
          (if (async-embedding-domain? store domain)
            (.add em-jobs {:type :embedding
                           :domain domain
                           :op :add
                           :ref doc-ref
                           :value v})
            (when-let [vec-data (some-> ^HashMap domain-vecs (.get domain))]
              (.add em-ds [domain [:a [doc-ref vec-data]]]))))))
    (when (identical? vt :db.type/idoc)
      (let [domain (or (props :db/domain) (u/keyword->string attr))]
        (let [op    (if giant?
                      [:g [e aid max-gt v]]
                      [:a [e aid v]])
              patch (some-> (meta d) :idoc/patch)
              op    (if patch (with-meta op {:idoc/patch patch}) op)]
          (.add id-ds [domain op]))))
    (when fulltext?
      (let [text (str v)
            ref  (if giant? [:g max-gt e aid] [e aid text])]
        (collect-fulltext store
                          ft-ds
                          ft-jobs
                          attr
                          props
                          text
                          ref
                          :add
                          (if giant? [:g [e aid max-gt text]] [:a ref]))))))

(defn- delete-datom
  [^Store store ^Datom d ^FastList txs ^FastList ft-ds ^FastList vi-ds
   ^FastList ft-jobs ^FastList vi-jobs ^FastList em-ds ^FastList em-jobs
   ^FastList id-ds ^HashMap giants ^HashMap attr-infos ^ByteBuffer avg-bf]
  (let [e          (.-e d)
        attr       (.-a d)
        v          (.-v d)
        ^objects ai (write-attr-info store attr-infos attr v false)
        props      (aget ai 0)
        vt         (aget ai 1)
        aid        (aget ai 2)
        embedding? (aget ai 3)
        fulltext?  (aget ai 4)
        i          ^Indexable (b/indexable nil aid v vt c/g0)
        giant?     (b/giant? i)
        d-eav      (when giant? [e attr v])
        gt-cur     (when giant? (.get giants d-eav))
        gt         (when giant?
                     (or gt-cur
                         (let [[_ ^Retrieved r]
                               (nth
                                (list-range
                                 (.-lmdb store) c/eav [:closed e e] :id
                                 [:closed
                                  i
                                  (Indexable. nil aid v (.-f i) (.-b i) c/gmax)]
                                 :avg)
                                0)]
                           (.-g r))))]
    (when fulltext?
      (let [text (str v)
            ref  (if gt [:g gt e aid] [e aid text])]
        (collect-fulltext store
                          ft-ds
                          ft-jobs
                          attr
                          props
                          text
                          ref
                          :delete
                          (if gt [:r [e aid gt]] [:d ref]))))
    (when embedding?
      (let [doc-ref (if gt [:g gt e aid] [e aid v])]
        (doseq [domain (embedding-attr-domains attr props)]
          (if (async-embedding-domain? store domain)
            (.add em-jobs {:type :embedding
                           :domain domain
                           :op :delete
                           :ref doc-ref
                           :value v})
            (.add em-ds [domain [:d doc-ref]])))))
    (when (identical? vt :db.type/idoc)
      (let [domain (or (props :db/domain) (u/keyword->string attr))]
        (.add id-ds [domain
                     (if gt
                       [:r [e aid gt v]]
                       [:d [e aid v]])])))
    (let [ii (Indexable. nil aid v (.-f i) (.-b i) (or gt c/normal))]
      (.add txs (DatomKVTxData. e (b/indexable-bytes ii avg-bf) false))
      (when gt
        (when gt-cur (.remove giants d-eav))
        (.add txs (lmdb/kv-tx :del c/giants gt :id)))
      (when (identical? vt :db.type/vec)
        (let [ref     (if gt [:g gt e aid] [e aid v])
              op      (if gt [:r [e aid gt]] [:d [e aid v]])
              domains (conjv (props :db.vec/domains) (v/attr-domain attr))]
          (doseq [domain domains]
            (if (async-vector-domain? store domain)
              (.add vi-jobs {:type :vector
                             :domain domain
                             :op :delete
                             :ref ref
                             :value v})
              (.add vi-ds [[domain] op]))))))))

(defn- prepare-datoms-kv-plan
  "Prepare KV write plan for a datom batch.
   This is an extraction step toward sharing DL/KV commit flow."
  ([^Store store datoms]
   (prepare-datoms-kv-plan store datoms nil))
  ([^Store store datoms embedding-plan]
   (prepare-datoms-kv-plan store datoms embedding-plan nil nil))
  ([^Store store datoms embedding-plan extra-kv-txs last-modified-ms]
   ;; Datom operations lead the batch so LMDB can select the primitive-EID
   ;; executor once; generic giant, job, and metadata operations follow.
   (let [txs    (FastList. (+ 2 (count datoms) (count extra-kv-txs)))
         ;; fulltext [:a d [e aid v]], [:d d [e aid v]],
         ;; [:g d [e aid gt v]], or [:r d [e aid gt]]
         ft-ds  (FastList.)
         ft-jobs (FastList.)
         ;; vector [:a d [e aid v]], [:d d [e aid v]],
         ;; [:g d [e aid gt v]], or [:r d [e aid gt]]
         vi-ds  (FastList.)
         vi-jobs (FastList.)
         ;; embedding [:a [doc-ref vec]], [:d doc-ref]
         em-ds  (FastList.)
         ;; durable async secondary index jobs
         em-jobs (FastList.)
         ;; idoc [:a d [e aid v]], [:d d [e aid v]],
         ;; [:g d [e aid gt v]], or [:r d [e aid gt v]]
         id-ds  (FastList.)
         giants (HashMap.)
         attr-infos (HashMap.)
         avg-bf     (bf/get-array-buffer)]
     (try
       (doseq [datom datoms]
         (if (d/datom-added datom)
           (insert-datom store datom txs ft-ds vi-ds ft-jobs vi-jobs
                         em-ds em-jobs id-ds giants attr-infos embedding-plan
                         avg-bf)
           (delete-datom store datom txs ft-ds vi-ds ft-jobs vi-jobs em-ds
                         em-jobs id-ds giants attr-infos avg-bf)))
       (finally
         (bf/return-array-buffer avg-bf)))
     (let [tx-id (long (.advance-max-tx store))
           modified-ms (long (or last-modified-ms
                                 (System/currentTimeMillis)))]
       (when (or (not (.isEmpty ft-jobs))
                 (not (.isEmpty vi-jobs))
                 (not (.isEmpty em-jobs)))
         (doseq [[ordinal job] (map-indexed vector
                                            (concat ft-jobs vi-jobs em-jobs))]
           (.add txs (si/job-tx (assoc job
                                       :tx tx-id
                                       :ordinal ordinal
                                       :created-ms modified-ms
                                       :updated-ms modified-ms)))))
       (.add txs (lmdb/kv-tx :put c/meta :max-tx tx-id :attr :long))
       (.add txs (lmdb/kv-tx :put c/meta :last-modified
                              modified-ms
                              :attr :long)))
     (doseq [tx extra-kv-txs]
       (.add txs tx))
     {:txs txs
      :ft-ds ft-ds
      :vi-ds vi-ds
      :em-ds em-ds
      :id-ds id-ds
      :secondary-index-job-count (+ (.size ft-jobs)
                                    (.size vi-jobs)
                                    (.size em-jobs))})))

(defn- commit-datoms-kv-plan!
  "Commit a prepared datom KV plan."
  [lmdb search-engines vector-indices embedding-indices idoc-indices
   {:keys [txs ft-ds vi-ds em-ds id-ds]}]
  (when-not (.isEmpty ^FastList ft-ds)
    (fulltext-index search-engines ft-ds))
  (when-not (.isEmpty ^FastList vi-ds)
    (vector-index vector-indices vi-ds))
  (when-not (.isEmpty ^FastList em-ds)
    (embedding-index embedding-indices em-ds))
  (let [idoc-state-actions (when-not (.isEmpty ^FastList id-ds)
                             (idoc-index idoc-indices id-ds txs))]
    (transact-kv lmdb txs)
    (idoc/apply-state-actions! idoc-state-actions)))

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

(def ^:private nippy-meta-protocol-key
  :taoensso.nippy/meta-protocol-key)

(def ^:private legacy-ha-nil-sentinel-keys
  [:ha-mode
   :ha-control-plane
   :ha-members
   :ha-fencing-hook
   :ha-clock-skew-hook
   :ha-membership-hash])

(def ^:private non-persistable-ha-option-keys
  [:ha-node-id
   :ha-client-credentials
   :ha-fencing-hook
   :ha-clock-skew-hook])

(def ^:private non-persistable-ha-control-plane-option-keys
  [:local-peer-id
   :raft-dir])

(def ^:private raw-persist-open-opts-key
  ::raw-persist-open-opts?)

(defn- encode-legacy-ha-nil-sentinels
  [opts]
  (reduce
    (fn [m k]
      (if (and (contains? m k) (nil? (get m k)))
        (assoc m k nippy-meta-protocol-key)
        m))
    (or opts {})
    legacy-ha-nil-sentinel-keys))

(defn- persistable-provider-spec
  [spec]
  (cond-> (or spec {})
    (map? spec) (dissoc :dir :embed-dir :api-key :headers)))

(defn- maybe-persistable-provider-spec
  [spec]
  (when spec
    (persistable-provider-spec spec)))

(defn- compact-persisted-kv-opts
  [opts]
  (let [opts (or opts {})
        kv-opts (c/canonicalize-wal-opts (or (:kv-opts opts) {}))
        compact-kv-opts
        (into {}
              (remove (fn [[k v]]
                        (and (not= k :wal?)
                             (contains? opts k)
                             (= v (get opts k)))))
              kv-opts)]
    (cond-> (dissoc opts :kv-opts)
      (contains? opts :kv-opts)
      (assoc :kv-opts compact-kv-opts))))

(defn- persistable-ha-control-plane-opts
  [cp]
  (cond-> (or cp {})
    (map? cp) (dissoc :local-peer-id :raft-dir)))

(defn- persistable-ha-opts
  [opts]
  (let [opts (apply dissoc (or opts {}) non-persistable-ha-option-keys)]
    (cond-> opts
      (contains? opts :ha-control-plane)
      (update :ha-control-plane persistable-ha-control-plane-opts))))

(defn- store-visible-opts
  [opts]
  (-> (persistable-ha-opts opts)
      (dissoc :embedding-providers
              :embedding-domain-providers
              :runtime-opts
              raw-persist-open-opts-key)))

(defn- persistable-opts
  [opts]
  (let [opts (-> opts
                 compact-persisted-kv-opts
                 persistable-ha-opts
                 (dissoc :embedding-providers
                         :embedding-domain-providers
                         :runtime-opts
                         raw-persist-open-opts-key))
        opts (cond-> opts
               (contains? opts :embedding-opts)
               (assoc :embedding-opts
                      (maybe-persistable-provider-spec (:embedding-opts opts)))

               (contains? opts :embedding-domains)
               (assoc :embedding-domains
                      (when-let [domains (:embedding-domains opts)]
                        (into {}
                              (map (fn [[domain cfg]]
                                     [domain (persistable-provider-spec cfg)]))
                              domains))))]
    (cond-> opts
    true c/canonicalize-wal-opts
    true encode-legacy-ha-nil-sentinels)))

(declare load-opts)

(defn- transact-opts
  [lmdb opts]
  (let [opts (persistable-opts opts)
        current (some-> (load-opts lmdb) persistable-opts)]
    (when (not= current opts)
      (when (true? (:wal? opts))
        (let [flags (or (get-env-flags lmdb) #{})]
          (when (and (not (contains? flags :nosync))
                     (not (contains? flags :rdonly)))
            (set-env-flags lmdb #{:nosync} true))))
      (transact-kv
        lmdb (conj (for [[k v] opts]
                     (lmdb/kv-tx :put c/opts k v :attr :data))
                   (lmdb/kv-tx :put c/meta :last-modified
                               (System/currentTimeMillis) :attr :long))))))

(defn- raw-lmdb
  [db]
  db)

(defn- transact-opts-raw
  [lmdb opts]
  (let [opts (persistable-opts opts)
        current (some-> (load-opts lmdb) persistable-opts)
        raw-db (raw-lmdb lmdb)]
    (when (not= current opts)
      (when (true? (:wal? opts))
        (let [flags (or (get-env-flags raw-db) #{})]
          (when (and (not (contains? flags :nosync))
                     (not (contains? flags :rdonly)))
            (set-env-flags raw-db #{:nosync} true))))
      (kv/transact-kv-without-txlog!
        raw-db
        (conj (for [[k v] opts]
                (lmdb/kv-tx :put c/opts k v :attr :data))
              (lmdb/kv-tx :put c/meta :last-modified
                          (System/currentTimeMillis) :attr :long))))))

(defn- normalize-legacy-ha-nil-sentinels
  [opts]
  (reduce
    (fn [m k]
      (if (= nippy-meta-protocol-key (get m k))
        (assoc m k nil)
        m))
    (or opts {})
    legacy-ha-nil-sentinel-keys))

(defn- load-opts
  [lmdb]
  (-> (into {} (get-range lmdb c/opts [:all] :attr :data))
      c/canonicalize-wal-opts
      normalize-legacy-ha-nil-sentinels))

(defn- sync-wal-runtime-opts!
  [lmdb opts]
  (let [opts (c/canonicalize-wal-opts opts)]
    (when (true? (:wal? opts))
      (let [runtime-opts (or (env-opts lmdb) {})
            info-v       (kv-info lmdb)
            wal-opts     (into {}
                               (filter (fn [[k _]]
                                         (c/wal-option-key? k)))
                               opts)
            runtime-missing?
            (some (fn [[k v]]
                    (not= v (get runtime-opts k)))
                  wal-opts)
            persisted-missing?
            (some (fn [[k v]]
                    (not= v
                          (get-value lmdb c/kv-info k :keyword :data)))
                  wal-opts)]
        (when (and info-v runtime-missing?)
          (vswap! info-v merge wal-opts))
        (when (and info-v
                   persisted-missing?
                   (not (contains? (or (get-env-flags lmdb) #{}) :rdonly)))
          (kv/transact-kv-without-txlog!
            lmdb
            (mapv (fn [[k v]]
                    (lmdb/kv-tx :put c/kv-info k v :keyword :data))
                  wal-opts)))))))

(defn- open-dbis
  [lmdb]
  (open-list-dbi lmdb c/ave {:key-size c/+max-key-size+
                             :val-size c/+id-bytes+})
  (open-list-dbi lmdb c/eav {:key-size c/+id-bytes+
                             :val-size c/+max-key-size+})
  (open-dbi lmdb c/giants {:key-size c/+id-bytes+})
  (open-dbi lmdb c/ha-client-ops)
  (open-dbi lmdb c/meta {:key-size c/+max-key-size+})
  (open-dbi lmdb c/opts {:key-size c/+max-key-size+})
  (open-dbi lmdb c/schema {:key-size c/+max-key-size+})
  (open-dbi lmdb c/secondary-index-jobs {:key-size c/+max-key-size+}))

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
  [lmdb domains runtime-opts]
  (reduce-kv
    (fn [m domain opts]
      (assoc m domain
             (s/new-search-engine
               lmdb
               (cond-> opts
                 (:udf-registry runtime-opts)
                 (assoc :udf-registry (:udf-registry runtime-opts))))))
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

(def ^:private default-embedding-opts
  {:provider    :default
   :metric-type :cosine})

(def ^:private embedding-index-prefix
  "__embedding__")

(defn- embedding-index-domain
  [domain]
  (str embedding-index-prefix "/" domain))

(defn- default-embedding-domain
  [dms embedding-opts]
  (if (contains? dms c/default-domain)
    dms
    (assoc dms c/default-domain
           (assoc (merge default-embedding-opts (or embedding-opts {}))
                  :domain c/default-domain))))

(defn- listed-embedding-domains
  [dms domains embedding-opts embedding-domains]
  (reduce
    (fn [m domain]
      (if (contains? m domain)
        m
        (assoc m domain
               (assoc (merge default-embedding-opts
                             (get embedding-domains domain)
                             embedding-opts)
                      :domain domain))))
    dms
    domains))

(defn- init-embedding-domain-refs
  [embedding-domains0 schema embedding-opts embedding-domains]
  (reduce-kv
    (fn [dms attr
         {:keys [db/embedding db.embedding/domains db.embedding/autoDomain]}]
      (if embedding
        (let [dms (if (seq domains)
                    (listed-embedding-domains dms domains embedding-opts
                                              embedding-domains)
                    (default-embedding-domain dms embedding-opts))]
          (if autoDomain
            (listed-embedding-domains dms [(v/attr-domain attr)] embedding-opts
                                      embedding-domains)
            dms))
        dms))
    (or embedding-domains0 {})
    schema))

(defn- provider-spec-for-domain
  [dir runtime-providers domain {:keys [provider] :as domain-opts}]
  (let [provider-id (or provider :default)
        runtime     (get runtime-providers provider-id)]
    (cond
      (satisfies? emb/IEmbeddingProvider runtime)
      runtime

      (or (map? runtime) (keyword? runtime))
      (merge (if (map? runtime) runtime {:provider runtime})
             domain-opts
             {:provider provider-id :dir dir})

      runtime
      (u/raise "Embedding provider registry entry is invalid"
               {:domain domain
                :provider provider-id
                :entry runtime})

      (#{:default :llama.cpp :openai-compatible} provider-id)
      (assoc domain-opts :provider provider-id :dir dir)

      :else
      (u/raise "Embedding provider is not configured"
               {:domain domain :provider provider-id}))))

(defn- resolve-embedding-domain
  [dir runtime-providers [domain domain-opts]]
  (let [domain-opts                 (merge default-embedding-opts domain-opts)
        {:keys [dimensions
                embedding-metadata]} (runtime-provider-space dir runtime-providers
                                                             domain domain-opts)
        provider-dimensions         dimensions
        provider-metadata           embedding-metadata
        stored-dimensions           (:dimensions domain-opts)
        stored-metadata             (:embedding-metadata domain-opts)
        dimensions                  (or stored-dimensions provider-dimensions)
        embedding-metadata          (or stored-metadata provider-metadata)]
    (when (and stored-dimensions provider-dimensions
               (not= (long stored-dimensions) (long provider-dimensions)))
      (u/raise "Embedding domain dimensions do not match the runtime provider"
               {:domain              domain
                :provider            (:provider domain-opts)
                :stored-dimensions   stored-dimensions
                :provider-dimensions provider-dimensions}))
    (when stored-metadata
      (emb/ensure-compatible-metadata stored-metadata provider-metadata))
    (when-not dimensions
      (u/raise "Embedding domain dimensions could not be resolved"
               {:domain domain :provider (:provider domain-opts)}))
    [domain
     (-> domain-opts
         (assoc :provider (or (:provider domain-opts) :default)
                :dimensions dimensions
                :embedding-metadata embedding-metadata))]))

(defn- init-embedding-domains
  [dir embedding-domains0 schema embedding-opts embedding-domains runtime-providers]
  (let [domains (init-embedding-domain-refs embedding-domains0 schema
                                            embedding-opts embedding-domains)]
    (into {}
          (map #(resolve-embedding-domain dir runtime-providers %))
          domains)))

(defn- init-embedding-providers
  [dir domains runtime-providers]
  (reduce-kv
    (fn [m domain domain-opts]
      (assoc m domain
             (emb/init-embedding-provider
               (provider-spec-for-domain dir runtime-providers domain domain-opts))))
    {}
    domains))

(defn- init-indices
  [lmdb domains]
  (reduce-kv
    (fn [m domain opts]
      (assoc m domain (v/new-vector-index lmdb opts)))
    {} domains))

(defn- init-embedding-indices
  [lmdb domains]
  (reduce-kv
    (fn [m domain opts]
      (assoc m domain
             (v/new-vector-index
               lmdb
               (assoc opts :domain (embedding-index-domain domain)))))
    {}
    domains))

(defn- idoc-schema-domain-opts
  [props]
  (cond-> {}
    (contains? props :db.idoc/indexedPaths)
    (assoc :indexed-paths (:db.idoc/indexedPaths props))

    (contains? props :db.idoc/excludedPaths)
    (assoc :excluded-paths (:db.idoc/excludedPaths props))))

(defn- merge-idoc-path-option
  [a b]
  (cond
    (nil? a) b
    (nil? b) a
    :else (vec (distinct (concat a b)))))

(defn- merge-idoc-domain-opts
  [a b]
  (-> (merge a b)
      (assoc :indexed-paths
             (merge-idoc-path-option (:indexed-paths a)
                                     (:indexed-paths b)))
      (assoc :excluded-paths
             (merge-idoc-path-option (:excluded-paths a)
                                     (:excluded-paths b)))))

(defn- init-idoc-domains
  [schema opts]
  (let [default-opts (:idoc-opts opts)
        domain-opts  (:idoc-domains opts)]
    (reduce-kv
      (fn [dms attr {:keys [db/valueType db/domain db/idocFormat] :as props}]
        (if (identical? valueType :db.type/idoc)
          (let [domain      (or domain (u/keyword->string attr))
                fmt         (or idocFormat :edn)
                prior       (get dms domain)
                schema-opts (idoc-schema-domain-opts props)
                opts        (merge default-opts
                                   schema-opts
                                   (get domain-opts domain))
                opts        (assoc opts :domain domain :format fmt)]
            (cond
              (nil? prior) (assoc dms domain opts)
              (= (:format prior) fmt)
              (assoc dms domain (merge-idoc-domain-opts prior opts))
              :else
              (assoc dms domain
                     (merge-idoc-domain-opts prior (assoc opts :format :mixed)))))
          dms))
      {}
      schema)))

(defn- init-idoc-indices
  [lmdb domains]
  (reduce-kv
    (fn [m domain opts]
      (assoc m domain (idoc/new-idoc-index lmdb opts)))
    {} domains))

(defn- propagate-top-level-txlog-opts-to-kv-opts
  [opts]
  (let [opts      (or opts {})
        kv-opts?  (contains? opts :kv-opts)
        kv-opts   (c/canonicalize-wal-opts (or (:kv-opts opts) {}))
        txlog-opts (into {}
                         (keep (fn [[k v]]
                                 (let [k' (c/canonical-wal-option-key k)]
                                   (when (and (c/wal-option-key? k)
                                              (not (contains? kv-opts k')))
                                     [k' v]))))
                         opts)]
    (cond-> (c/canonicalize-wal-opts opts)
      (or kv-opts? (seq txlog-opts))
      (assoc :kv-opts (if (seq txlog-opts)
                        (merge kv-opts txlog-opts)
                        kv-opts)))))

(def ^:private ha-wal-durability-profile :strict)

(defn- kv-wal-opts
  [opts]
  (when-let [kv-opts (:kv-opts opts)]
    (into {}
          (filter (fn [[k _]] (c/wal-option-key? k)))
          kv-opts)))

(defn- promote-kv-wal-opts
  [opts]
  (let [wal-opts (kv-wal-opts opts)]
    (cond-> opts
      (seq wal-opts) (merge wal-opts))))

(defn- ha-wal-durability-profile-for
  [opts]
  (let [profile (or (get-in opts [:kv-opts :wal-durability-profile])
                    (:wal-durability-profile opts)
                    ha-wal-durability-profile)]
    (when (= :relaxed profile)
      (u/raise "Consensus-lease HA requires :wal-durability-profile :strict or :extra"
               {:error :ha/validation
                :option :wal-durability-profile
                :value profile}))
    profile))

(defn- force-ha-wal-opts
  [opts]
  (let [profile (ha-wal-durability-profile-for opts)]
    (-> opts
        (assoc :wal? true
               :wal-durability-profile profile)
        (update :kv-opts
                (fn [kv-opts]
                  (assoc (or kv-opts {})
                         :wal? true
                         :wal-durability-profile profile))))))

(defn- normalize-ha-open-opts
  [opts]
  (cond-> opts
    (= :consensus-lease (:ha-mode opts))
    force-ha-wal-opts

    (= :consensus-lease (:ha-mode opts))
    ;; Background sampling performs follower-local metadata writes. In HA mode
    ;; that extra local write traffic obscures replicated progress and can race
    ;; with follower replay. Keep it disabled on consensus-lease stores.
    (assoc :background-sampling? false)))

(defn- txlog-dir-path
  [dir]
  (str dir u/+separator+ "txlog"))

(defn- existing-store?
  [dir]
  (or (u/file-exists (str dir u/+separator+ c/data-file-name))
      (u/file-exists (txlog-dir-path dir))))

(defn- load-existing-store-opts
  [dir _kv-opts]
  (when (existing-store? dir)
    ;; Reuse persisted opts from an already-open handle when available, but do
    ;; not probe closed stores just to read them; the real open can load opts.
    (when-let [probe (or (some-> ^Store (current-shared-local-store dir) .-lmdb)
                         (datalevin.binding.cpp/open-local-kv-handle dir))]
      (do
        (open-dbis probe)
        (not-empty (load-opts probe))))))

(defn open
  "Open and return the storage."
  ([]
   (open nil nil))
  ([dir]
   (open dir nil))
  ([dir schema]
   (open dir schema nil))
  ([dir schema opts0]
   (let [incoming-opts0 opts0
         opts (-> opts0
                  propagate-top-level-txlog-opts-to-kv-opts
                  normalize-ha-open-opts)
         raw-persist-open-opts? (true? (get opts raw-persist-open-opts-key))
         opts (dissoc opts raw-persist-open-opts-key)
         {:keys [kv-opts search-opts search-domains vector-opts vector-domains
                 embedding-opts embedding-domains embedding-providers]}
         opts
         dir  (or dir (u/tmp-dir (str "datalevin-" (UUID/randomUUID))))
         persisted-opts (load-existing-store-opts dir kv-opts)
         persisted-kv-opts
         (c/canonicalize-wal-opts
          (or (:kv-opts (some-> persisted-opts
                                propagate-top-level-txlog-opts-to-kv-opts))
              {}))
         new-db? (not (existing-store? dir))
         wal-default-kv-opts (when new-db?
                               {:wal? c/*datalog-wal?*
                                :wal-durability-profile
                                c/*datalog-wal-durability-profile*})
         kv-opts (cond-> (merge persisted-kv-opts kv-opts)
                   wal-default-kv-opts (#(merge wal-default-kv-opts %)))
         opened-with-wal? (true? (:wal? kv-opts))
         ^Store shared-store (current-shared-local-store dir)
         lmdb (or (some-> shared-store .-lmdb)
                  (lmdb/open-kv dir kv-opts))]
     (open-dbis lmdb)
     (let [loaded-opts (when-not persisted-opts
                         (not-empty (load-opts lmdb)))
           opts0     (or persisted-opts
                         loaded-opts
                         {})
           opts1     (if (empty? opts0)
                       {:validate-data?       false
                        :auto-entity-time?    false
                        :closed-schema?       false
                        :background-sampling? c/*db-background-sampling?*
                        :async-secondary-index-worker-max-jobs
                        c/*async-secondary-index-worker-max-jobs*
                        :async-secondary-index-worker-lease-ms
                        c/*async-secondary-index-worker-lease-ms*
                        :async-secondary-index-retry-base-ms
                        c/*async-secondary-index-retry-base-ms*
                        :async-secondary-index-retry-max-ms
                        c/*async-secondary-index-retry-max-ms*
                        :ha-mode c/*ha-mode*
                        :ha-lease-renew-ms c/*ha-lease-renew-ms*
                        :ha-lease-timeout-ms c/*ha-lease-timeout-ms*
                        :ha-promotion-base-delay-ms c/*ha-promotion-base-delay-ms*
                        :ha-promotion-rank-delay-ms c/*ha-promotion-rank-delay-ms*
                        :ha-max-promotion-lag-lsn c/*ha-max-promotion-lag-lsn*
                        :ha-demotion-drain-ms c/*ha-demotion-drain-ms*
                        :ha-clock-skew-budget-ms c/*ha-clock-skew-budget-ms*
                        :ha-control-plane c/*ha-control-plane*
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
           opts2-base (-> (merge opts1 opts)
                          c/canonicalize-wal-opts
                          normalize-ha-open-opts
                          promote-kv-wal-opts)
           opts2     (-> (if (and (or (some? persisted-opts)
                                      (some? loaded-opts))
                                  (empty? (or incoming-opts0 {})))
                           (propagate-top-level-txlog-opts-to-kv-opts
                             opts2-base)
                           opts2-base)
                         normalize-ha-open-opts
                         promote-kv-wal-opts)
           db-identity (or (:db-identity opts2)
                           (:db-name opts2)
                           (str (UUID/randomUUID)))
           opts3     (assoc opts2 :db-identity db-identity)
           _         (vld/validate-ha-store-opts opts3)
           _         (vld/validate-secondary-index-worker-options opts3)
           _         (vld/validate-search-options opts3)
           _         (vld/validate-vector-options opts3)
           _         (vld/validate-embedding-options opts3)
           _         (vld/validate-idoc-options opts3)
           _         (when (= "1" (System/getenv "DTLV_DEBUG_STORAGE_OPEN"))
                       (prn :storage-open
                            {:dir dir
                             :incoming-opts opts
                             :persisted-opts (select-keys opts0
                                                          [:ha-mode
                                                           :db-name
                                                           :db-identity
                                                           :ha-node-id
                                                           :ha-members
                                                           :ha-control-plane
                                                           :ha-demotion-drain-ms
                                                           :ha-fencing-hook
                                                           :wal?
                                                           :kv-opts])
                             :opts3 (select-keys opts3
                                                 [:ha-mode
                                                  :db-name
                                                  :db-identity
                                                  :ha-node-id
                                                  :ha-members
                                                  :ha-control-plane
                                                  :ha-demotion-drain-ms
                                                  :ha-fencing-hook
                                                  :wal?
                                                  :kv-opts])}))
           raw-open-metadata? (or raw-persist-open-opts?
                                  (= :consensus-lease (:ha-mode opts3)))
           _         (sync-wal-runtime-opts! lmdb opts3)
           _         (when (and (not opened-with-wal?)
                                (true? (:wal? opts3)))
                       (kv/ensure-txlog-ready! lmdb))
           schema    (if shared-store
                       (datalevin.interface/set-schema shared-store schema)
                       (init-schema lmdb schema))
           s-domains (init-search-domains (:search-domains opts3)
                                          schema search-opts search-domains)
           v-domains (init-vector-domains (:vector-domains opts3)
                                          schema vector-opts vector-domains)
           e-domains (init-embedding-domains dir
                                             (:embedding-domains opts3)
                                             schema
                                             embedding-opts
                                             embedding-domains
                                             embedding-providers)
           i-domains (init-idoc-domains schema opts3)]
       (let [opts4       (cond-> opts3
                           (seq e-domains)
                           (assoc :embedding-opts (merge default-embedding-opts
                                                         (or (:embedding-opts opts3)
                                                             embedding-opts))
                                  :embedding-domains e-domains))
             store-opts  (store-visible-opts opts4)
             dir-key     (shared-local-store-key dir)]
         (if raw-open-metadata?
           (transact-opts-raw lmdb opts4)
           (transact-opts lmdb opts4))
         (ensure-open-last-modified! lmdb raw-open-metadata?)
         (if shared-store
           (let [runtime-opts (:runtime-opts opts4)
                 wrapper      (with-open-opts
                                shared-store
                                store-opts
                                (cond-> {}
                                  (:udf-registry runtime-opts)
                                  (assoc :search-engines
                                         (init-engines lmdb s-domains
                                                       runtime-opts))))]
             (when dir-key
               (locking shared-local-stores
                 (swap! shared-local-stores
                        assoc dir-key
                        {:store wrapper
                         :refs  (unchecked-inc
                                 (long (get-in @shared-local-stores
                                               [dir-key :refs]
                                               0)))})))
             (enqueue-secondary-index-work-if-needed! wrapper))
           (let [e-providers (init-embedding-providers dir e-domains
                                                       embedding-providers)
                 store (->Store lmdb
                                (init-engines lmdb s-domains
                                              (:runtime-opts opts4))
                                (init-indices lmdb v-domains)
                                (init-embedding-indices lmdb e-domains)
                                (init-idoc-indices lmdb i-domains)
                                e-providers
                                (ConcurrentHashMap.)
                                store-opts
                                schema
                                (schema->rschema schema)
                                (init-attrs schema)
                                (init-max-aid schema)
                                (init-max-gt lmdb)
                                (init-max-tx lmdb)
                                (init-state-sync-ms lmdb)
                                (volatile! nil)
                                (volatile! :storage-mutex)
                                (ReentrantReadWriteLock.)
                                false
                                dir-key)]
             ;; Upgrade composite tuple attributes after the Store exists so
             ;; legacy :data values can be re-encoded through set-schema.
             (datalevin.interface/set-schema store nil)
             (when dir-key
               (locking shared-local-stores
                 (swap! shared-local-stores
                        assoc dir-key {:store store :refs 1})))
             (enqueue-secondary-index-work-if-needed! store))))))))

(defn- transfer-engines
  [engines lmdb]
  (zipmap (keys engines) (map #(s/transfer % lmdb) (vals engines))))

(defn- transfer-indices
  [indices lmdb]
  (zipmap (keys indices) (map #(v/transfer % lmdb) (vals indices))))

(defn- transfer-idoc-indices
  [indices lmdb]
  (zipmap (keys indices) (map #(idoc/transfer % lmdb) (vals indices))))

(defn- transfer-with-schema
  [^Store old lmdb schema*]
  (let [opts*         (opts old)
        idoc-indices (transfer-idoc-indices (store-idoc-indices old) lmdb)
        idoc-indices (if (lmdb/writing? lmdb)
                       idoc-indices
                       (merge-missing-idoc-indices
                         lmdb idoc-indices schema* opts*))]
    (->Store lmdb
             (transfer-engines (.-search-engines old) lmdb)
             (transfer-indices (.-vector-indices old) lmdb)
             (transfer-indices (.-embedding-indices old) lmdb)
             idoc-indices
             (.-embedding-providers old)
             (.-counts old)
             opts*
             schema*
             (schema->rschema schema*)
             (init-attrs schema*)
             (init-max-aid schema*)
             (max-gt old)
             (max-tx old)
             (init-state-sync-ms lmdb)
             (.-scheduled-sampling old)
             (.-write-txn old)
             ;; Sampling work may still be queued against an older Store wrapper.
             ;; Keep close/sampling coordination on a shared lock across wrappers
             ;; that refer to the same logical store/LMDB lifecycle.
             (.-sampling-lock old)
             false
             (.-shared-dir-key old))))

(defn transfer
  "transfer state of an existing store to a new store that has a different
  LMDB instance"
  [^Store old lmdb]
  (transfer-with-schema old lmdb (schema old)))

(defn- transfer-current
  "Transfer a Store while taking its schema from the LMDB transaction. The
  caller must hold the write lock so planning cannot race another schema
  mutation."
  [^Store old lmdb]
  (transfer-with-schema old lmdb (load-schema lmdb)))

(defn with-open-opts
  "Return a Store wrapper over the same open LMDB state but with different
  in-memory opts. This does not persist opts back into LMDB."
  ([^Store old new-opts]
   (with-open-opts old new-opts nil))
  ([^Store old new-opts {:keys [search-engines vector-indices
                                embedding-indices idoc-indices
                                embedding-providers]}]
   (let [schema* (schema old)]
     (->Store (.-lmdb old)
              (or search-engines (.-search-engines old))
              (or vector-indices (.-vector-indices old))
              (or embedding-indices (.-embedding-indices old))
              (or idoc-indices (store-idoc-indices old))
              (or embedding-providers (.-embedding-providers old))
              (.-counts old)
              (store-visible-opts new-opts)
              schema*
              (schema->rschema schema*)
              (init-attrs schema*)
              (init-max-aid schema*)
              (max-gt old)
              (max-tx old)
              (init-state-sync-ms (.-lmdb old))
              (.-scheduled-sampling old)
              (.-write-txn old)
              (.-sampling-lock old)
              false
              (.-shared-dir-key old)))))

(defn- close-store-resources!
  [^Store this]
  (let [^ReentrantReadWriteLock sampling-lock (.-sampling-lock this)
        wlock (.writeLock sampling-lock)]
    (.lock wlock)
    (try
      (.stop-sampling this)
      (doseq [index (vals (.-vector-indices this))]
        (when-not (vec-closed? index)
          (close-vecs index)))
      (doseq [index (vals (.-embedding-indices this))]
        (when-not (vec-closed? index)
          (close-vecs index)))
      (doseq [provider (vals (.-embedding-providers this))]
        (emb/close-provider provider))
      (close-kv (.-lmdb this))
      (finally
        (.unlock wlock)))))

(defn- release-shared-local-store!
  [^Store store]
  (if-let [dir-key (.-shared-dir-key store)]
    (locking shared-local-stores
      (if-let [{shared-store :store refs :refs}
               (get @shared-local-stores dir-key)]
        (if (> ^long refs 1)
          (let [replacement (if (identical? shared-store store)
                              (with-open-opts shared-store (opts shared-store))
                              shared-store)]
            (swap! shared-local-stores
                   assoc dir-key {:store replacement
                                  :refs  (unchecked-dec (long refs))})
            :detached)
          (do
            (swap! shared-local-stores dissoc dir-key)
            :close))
        :close))
    :close))

(defn retire-shared-local-store!
  "Remove and close any shared local Store registered for dir."
  [dir]
  (when-let [dir-key (shared-local-store-key dir)]
    (when-let [^Store store (locking shared-local-stores
                              (let [store (get-in @shared-local-stores
                                                  [dir-key :store])]
                                (swap! shared-local-stores dissoc dir-key)
                                store))]
      (when-not (closed? store)
        (datalevin.interface/close store)))))

(defn sync-max-gt-floor!
  "Advance an open store's in-memory giant-id cursor to at least `next-gt`.
  HA follower replay writes raw giant rows directly into LMDB, so the cursor
  must be kept in sync without reopening the store."
  [^Store store next-gt]
  (locking (.-write-txn store)
    (loop [current (long (max-gt store))
           target (long next-gt)]
      (if (< current target)
        (recur (long (advance-max-gt store)) target)
        current))))

(defn sync-max-tx-floor!
  "Advance an open store's in-memory transaction cursor to at least `next-tx`.
  HA replay can materialize durable metadata through raw KV rows, bypassing the
  normal local transaction path that advances this volatile cursor."
  [^Store store next-tx]
  (locking (.-write-txn store)
    (loop [current (long (max-tx store))
           target (long next-tx)]
      (if (< current target)
        (recur (long (.advance-max-tx store)) target)
        current))))
