;;
;; Copyright (c) Nikita Prokopov, Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns ^:no-doc datalevin.db
  "Datalog DB abstraction"
  (:refer-clojure :exclude [update assoc sync])
  (:require
   [clojure.walk]
   [clojure.data]
   [clojure.set]
   [datalevin.bits :as b]
   [datalevin.client-op :as cop]
   [datalevin.client.request :as cr]
   [datalevin.constants :as c :refer [e0 emax v0 vmax]]
   [datalevin.datom :as d :refer [datom datom?]]
   [datalevin.custom-datalog :as cd]
   [datalevin.db.range-cache :as range-cache]
   [datalevin.db.result-cache :as result-cache]
   [datalevin.native-value :as nv]
   [datalevin.remote-cache :as remote-cache]
   [datalevin.db.tx.add :as txadd]
   [datalevin.db.tx.common :as txcommon]
   [datalevin.db.tx.compile :as txcompile]
   [datalevin.db.tx.plan :as txplan]
   [datalevin.db.tx.engine :as txengine]
   [datalevin.db.tx.execute :as txexec]
   [datalevin.db.tx.prepare :as txprep]
   [datalevin.idoc :as idoc]
   [datalevin.index :as idx]
   [datalevin.util :as u
    :refer [ case-tree defrecord-updatable raise]]
   [datalevin.lmdb :as l]
   [datalevin.kv.txlog :as kvtx]
   [datalevin.storage :as s]
   [datalevin.storage.metadata :as metadata]
   [datalevin.storage.scan :as scans]
   [datalevin.prepare :as prepare]
   [datalevin.query-util :as qu]
   [datalevin.udf :as udf]
   [datalevin.validate :as vld]
   [datalevin.relation :as rel]
   [datalevin.inline :refer [update assoc]]
   [datalevin.interface :as i
    :refer [last-modified dir opts schema rschema ave-tuples ave-tuples-list
            sample-ave-tuples sample-ave-tuples-list e-sample eav-scan-v
            eav-scan-v-list val-eq-scan-e val-eq-scan-e-list val-eq-filter-e
            val-eq-filter-e-list fetch slice slice-filter e-datoms av-datoms
            ea-first-datom head-filter e-first-datom av-first-datom head
            size size-filter e-size av-size a-size v-size
            datom-count populated? rslice cardinality default-ratio
            av-range-size init-max-eid db-name start-sampling load-datoms
            stop-sampling close assoc-opt
            max-tx get-env-flags set-env-flags sync abort-transact-kv
            kv-info]])
  (:import
   [datalevin.datom Datom]
   [datalevin.db.tx.plan BlindEntity TxPlan]
   [datalevin.interface IRemoteDB IRemotePrepared IStore]
   [datalevin.storage Store WriteGroup]
   [datalevin.utl LRUCache QueryKey]
   [java.nio ByteBuffer]
   [java.util Collection Comparator Date]
   [java.util.concurrent ConcurrentHashMap]
   [java.util.concurrent.atomic AtomicBoolean]
   [java.util.function Function]
   [org.eclipse.collections.impl.list.mutable FastList]
   [org.eclipse.collections.impl.set.sorted.mutable TreeSortedSet]))

;;;;;;;;;; Protocols

(defprotocol ISearch
  (-search [db pattern])
  (-search-tuples [db pattern])
  (-count [db pattern] [data pattern cap])
  (-first [db pattern]))

(defprotocol IIndexAccess
  (-populated? [db index c1 c2 c3])
  (-datoms [db index] [db index c1] [db index c1 c2] [db index c1 c2 c3]
    [db index c1 c2 c3 n])
  (-e-datoms [db e])
  (-av-datoms [db attr v])
  (-range-datoms [db index start-datom end-datom])
  (-seek-datoms [db index c1 c2 c3] [db index c1 c2 c3 n])
  (-rseek-datoms [db index c1 c2 c3] [db index c1 c2 c3 n])
  (-cardinality [db attr])
  (-index-range [db attr start end])
  (-index-range-size [db attr start end]))

(defprotocol IDB
  (-schema [db])
  (-rschema [db])
  (-attrs-by [db property])
  (-is-attr? [db attr property])
  (-clear-tx-cache [db]))

(defprotocol ISearchable (-searchable? [_]))

(extend-type Object ISearchable (-searchable? [_] false))
(extend-type nil ISearchable (-searchable? [_] false))

(defprotocol IControlledTuples
  (-init-tuples-controlled [db out attr get-v? policy]
    "Enumerate an attribute with explicit admission and bounded decoding.")
  (-lookup-tuples-controlled [db out attr entities get-v? policy]
    "Read one attribute for distinct entity keys in the pinned schema.")
  (-value-tuples-controlled [db out attr values policy]
    "Read one indexed attribute for distinct value keys in the pinned schema."))

(defprotocol ITuples
  (-init-tuples [db out a v-range pred get-v?])
  (-init-tuples-list [db a v-range pred get-v?])
  (-sample-init-tuples [db out a mcount v-range pred get-v?])
  (-sample-init-tuples-list [db a mcount v-range pred get-v?])
  (-e-sample [db a])
  (-default-ratio [db a])
  (-eav-scan-v [db in out eid-idx attrs-v])
  (-eav-scan-v-list [db in eid-idx attrs-v] [db in eid-idx attrs-v prepared])
  (-eav-filter-presence-list [db in eid-idx attr])
  (-val-eq-scan-e [db in out v-idx attr] [db in out v-idx attr bound])
  (-val-eq-scan-e-list [db in v-idx attr] [db in v-idx attr bound])
  (-val-eq-filter-e [db in out v-idx attr f-idx])
  (-val-eq-filter-e-list [db in v-idx attr f-idx]))

;; ----------------------------------------------------------------------------

(declare empty-db resolve-datom validate-attr components->pattern
         components->end-datom)

(defrecord TxReport [db-before db-after tx-data tempids tx-meta])

(def ^:dynamic *tx-compiler-observer*
  "Optional diagnostic observer, outside compilation and uninstrumented timings."
  nil)

(defrecord PreparedBlindTx
  [schema-epoch
   opts-epoch
   auto-entity-time?
   entities
   unique-avs
   has-unique?
   has-many?
   fuse-unique-inserts?
   identity-upsert-av
   identity-upsert-read])

(defrecord PreparedLocalPatchIdocTx
  [schema-epoch
   opts-epoch
   entity
   e
   attr
   ops])

(defrecord PreparedScalarUpdate
  [schema-epoch
   opts-epoch
   entries
   entity-reads])

;; (defmethod print-method TxReport [^TxReport rp, ^java.io.Writer w]
;;   (binding [*out* w]
;;     (pr {:datoms-transacted (count (:tx-data rp))})))

(defonce dbs (atom {}))

;; read caches
(defn remote-store?
  "Fast Datalog routing for built-in stores. Keep protocol extensions available
  to other store implementations without paying their lookup cost locally."
  [store]
  (cond
    (instance? Store store) false
    (instance? IRemoteDB store) true
    :else (satisfies? i/IRemoteDB store)))

(defn remote-prepared-store?
  "Whether a Datalog store provides remote prepared reads."
  [store]
  (cond
    (instance? Store store) false
    (instance? IRemotePrepared store) true
    :else (satisfies? i/IRemotePrepared store)))

(defonce ^:private caches (ConcurrentHashMap.))
(defonce ^:private cache-owners (atom {}))
(defonce ^:private remote-cache-check-ms (ConcurrentHashMap.))
(defonce ^:private remote-cache-max-tx (ConcurrentHashMap.))
(defonce ^:private remote-cache-schema-revision (ConcurrentHashMap.))

(declare cache-key-dependencies refresh-remote-cache!)

(defn- cache-key-pattern
  [k]
  (or (::index-pattern (meta k)) k))

(def ^:private cache-dependencies
  (reify Function
    (apply [_ k] (cache-key-dependencies (cache-key-pattern k)))))

(defn- mark-remote-cache-check!
  [store]
  (when (remote-store? store)
    (.put ^ConcurrentHashMap remote-cache-check-ms
          (dir store)
          (System/currentTimeMillis))))

(defn- cached-remote-cache-max-tx
  [store]
  (when (remote-store? store)
    (.get ^ConcurrentHashMap remote-cache-max-tx (dir store))))

(defn- mark-remote-cache-max-tx!
  [store remote-max-tx]
  (when (and (remote-store? store)
             (some? remote-max-tx))
    (.put ^ConcurrentHashMap remote-cache-max-tx
          (dir store)
          (long remote-max-tx))))

(defn- remote-schema-changed?
  [store revision]
  (and (some? revision)
       (not= revision (.get ^ConcurrentHashMap remote-cache-schema-revision (dir store)))))

(defn- mark-remote-schema-revision!
  [store revision]
  (when (and (remote-store? store) (some? revision))
    (.put ^ConcurrentHashMap remote-cache-schema-revision (dir store) revision)))

(defn- should-check-remote-cache?
  [store cache]
  (let [interval-ms (long c/*remote-db-last-modified-check-interval-ms*)]
    (or (nil? cache)
        (not (pos? interval-ms))
        (let [last-check-ms (.get ^ConcurrentHashMap remote-cache-check-ms
                                  (dir store))]
          (or (nil? last-check-ms)
              (let [elapsed-ms (- (System/currentTimeMillis)
                                  (long last-check-ms))]
                (>= elapsed-ms interval-ms)))))))

(defn refresh-cache
  ([store]
   (refresh-cache store (last-modified store) nil))
  ([store target]
   (refresh-cache store target nil))
  ([store target remote-max-tx]
   ;; An older DB can retain a Store wrapper from before a schema change.
   ;; Refresh its attribute-ID map along with the cached query results.
   (s/maybe-ensure-current! store)
   (let [target (long (or target 0))
         old    ^LRUCache (.get ^ConcurrentHashMap caches (dir store))
         cache  (LRUCache. (:cache-limit (opts store)) target cache-dependencies)]
     ;; Schema changes can replace the cache inside an explicit transaction.
     ;; Keep it disabled until that transaction exits, so staged reads never
     ;; enter the shared cache (including when the transaction aborts).
     (when (and old (.isDisabled old))
       (.disable cache))
     (.put ^ConcurrentHashMap caches (dir store) cache)
     (mark-remote-cache-max-tx! store remote-max-tx)
     (mark-remote-cache-check! store))))

(defn- ensure-cache
  ([store target]
   (ensure-cache store target nil))
  ([store target remote-max-tx]
   (ensure-cache store target remote-max-tx nil))
  ([store target remote-max-tx schema-revision]
   (let [target        (long (or target 0))
         cached-max-tx (cached-remote-cache-max-tx store)]
     (if-some [^LRUCache cache (.get ^ConcurrentHashMap caches (dir store))]
       (if (or (< ^long (.target cache) ^long target)
               (remote-schema-changed? store schema-revision)
               (and (some? remote-max-tx)
                    (some? cached-max-tx)
                    (< (long cached-max-tx)
                       (long remote-max-tx))))
         (refresh-cache store target remote-max-tx)
         (do
           (.setTarget cache target)
           (mark-remote-cache-max-tx! store remote-max-tx)
           (mark-remote-cache-check! store)))
       (refresh-cache store target remote-max-tx))
     (mark-remote-schema-revision! store schema-revision))))

(defn refresh-cache-if-stale
  "Refresh a local read cache only when the store has advanced past its target.
  Writers already invalidate affected entries and advance that target. Empty
  capacity and transaction-disabled caches need no read-side validation."
  [store]
  (if-some [^LRUCache cache (.get ^ConcurrentHashMap caches (dir store))]
    (when (and (pos? (.capacity cache)) (not (.isDisabled cache)))
      (let [target (long (or (last-modified store) 0))]
        (when (< (.target cache) target)
          (refresh-cache store target))))
    (refresh-cache store)))

(defn cache-disabled?
  [store]
  (.isDisabled ^LRUCache (.get ^ConcurrentHashMap caches (dir store))))

(defn ^:no-doc cache-active?
  "Whether results can be retained. Zero capacity avoids the LRU monitor."
  [store]
  (let [cache ^LRUCache (.get ^ConcurrentHashMap caches (dir store))]
    (and cache (pos? (.capacity cache)) (not (.isDisabled cache)))))

(defn disable-cache
  [store]
  (.disable ^LRUCache (.get ^ConcurrentHashMap caches (dir store))))

(defn enable-cache
  [store]
  (.enable ^LRUCache (.get ^ConcurrentHashMap caches (dir store))))

(defn cache-get
  [store k]
  (when-let [^LRUCache cache (.get ^ConcurrentHashMap caches (dir store))]
    (when-some [entry (.get cache k)]
      (result-cache/restore entry))))

(defn cache-put
  [store k v]
  (when-some [entry (result-cache/snapshot v)]
    (.put ^LRUCache (.get ^ConcurrentHashMap caches (dir store)) k entry)))

(defn ^:no-doc cache-token
  [store]
  (when-let [^LRUCache cache (.get ^ConcurrentHashMap caches (dir store))]
    [cache (.generation cache)]))

(defn- cache-entry-if-current
  [store [^LRUCache cache generation] k entry]
  (and cache
       (identical? cache
                   (.get ^ConcurrentHashMap caches (dir store)))
       (.putIfGeneration cache k entry (long generation))))

(defn ^:no-doc cache-put-if-current
  [store token k v]
  (if-some [entry (result-cache/snapshot v)]
    (cache-entry-if-current store token k entry)
    false))

(def ^:private owned-tuple-cache-ops
  ;; These native readers allocate their result lists and rows.
  #{:init-tuples :search-tuples})

(defn ^:no-doc cache-result
  "Publish an owned snapshot. Fresh native tuple lists already belong to the
  caller; other results retain normalization through the cache entry."
  [store token k result]
  (if-some [entry (result-cache/snapshot result)]
    (do
      (cache-entry-if-current store token k entry)
      (if (and (instance? Store store) (instance? FastList result)
               (contains? owned-tuple-cache-ops (first k)))
        result
        (result-cache/restore entry)))
    result))

(defn ^:no-doc check-remote-read!
  "Validate local lifecycle and cancellation before a remote read, without an RPC."
  [store]
  ;; Cache hits may run without a request budget. Do not clear the interrupt.
  (when (.isInterrupted (Thread/currentThread))
    (throw (InterruptedException. "Request interrupted")))
  (cr/check! :request)
  (when (satisfies? i/IRemoteReadLifecycle store)
    (i/check-remote-read-open! store)))

(defn- remote-read-cacheable?
  ([store] (remote-read-cacheable? store false))
  ([store prepare?]
   (and (not nv/*wire-reader*)
        (or (not (satisfies? i/IRemoteReadCache store))
            (do
              (when prepare? (i/prepare-remote-read-cache! store))
              (i/remote-read-cacheable? store))))))

(defn ^:no-doc cached-remote-read
  "Retain an RPC result in the DB's bounded read cache. Callers either validate
  freshness before entering or supply a validator, run only for prospective
  hits. All writes invalidate these opaque results. Transaction reads,
  receiver bindings, and mutable inputs/results bypass retention. Local lifecycle
  and cancellation checks apply independently of the freshness interval. Request
  identity uses serialized bytes."
  ([store key read-result] (cached-remote-read store key nil read-result))
  ([store key validate read-result]
   (check-remote-read! store)
   (when (and validate (nil? (.get ^ConcurrentHashMap caches (dir store))))
     (validate))
   (if (and (cache-active? store)
            (not (and (satisfies? l/IWriting store) (l/writing? store)))
            (remote-cache/immutable-data? key)
            (remote-read-cacheable? store true))
     (let [key [:remote-read (remote-cache/serialized-key key)]
           cached (cache-get store key)
           _ (when (and validate cached) (validate))
           token (cache-token store)
           cached (if (and validate cached) (cache-get store key) cached)]
       (if-some [cached cached]
         (if (remote-read-cacheable? store)
           (do
             ;; Validation and policy checks can overlap close or cancellation.
             (check-remote-read! store)
             (first cached))
           (read-result))
         (let [result (read-result)]
           ;; Publication only inspects local policy. Missing metadata skips
           ;; retention without another RPC that could discard this success.
           ;; Wrap nil and false so both can be distinguished from a miss.
           (when (and (remote-read-cacheable? store)
                      (remote-cache/immutable-data? result))
             (cache-put-if-current store token key [result]))
           result)))
     (read-result))))

(defn- remove-cache-by-dir!
  [cache-dir]
  (when-let [^LRUCache cache (.remove ^ConcurrentHashMap caches cache-dir)]
    (locking cache
      (.beginInvalidation cache (.target cache))
      (.clear cache)))
  (.remove ^ConcurrentHashMap remote-cache-max-tx cache-dir)
  (.remove ^ConcurrentHashMap remote-cache-schema-revision cache-dir)
  (.remove ^ConcurrentHashMap remote-cache-check-ms cache-dir))

(defn remove-cache
  [store]
  (remove-cache-by-dir! (dir store)))

(defn ^:no-doc retain-cache!
  "Retain a connection's shared result cache and return an idempotent release.
  The last release retires results and freshness metadata. Capture only the
  directory so closed connections do not retain their Store through this hook."
  [store]
  (let [cache-dir (dir store)
        released? (AtomicBoolean.)]
    (locking cache-owners
      ;; A prior connection can close between new-db and connection creation.
      (when-not (.containsKey ^ConcurrentHashMap caches cache-dir)
        (refresh-cache store))
      (swap! cache-owners update cache-dir (fnil inc 0)))
    (fn []
      (when (.compareAndSet released? false true)
        (locking cache-owners
          (if (> (long (get @cache-owners cache-dir 0)) 1)
            (swap! cache-owners update cache-dir dec)
            (do
              (remove-cache-by-dir! cache-dir)
              (swap! cache-owners dissoc cache-dir))))))))

(def ^:private stored-value-cache-ops
  #{:search :search-tuples :first :count :populated? :datoms :av-datoms
    :e-datoms :range-datoms :seek :rseek :index-range :index-range-size})

(defn- representation-sensitive?
  [value]
  (if (d/datom? value)
    (or (representation-sensitive? (.-a ^Datom value))
        (representation-sensitive? (.-v ^Datom value)))
    (or (some? (meta value))
        (not (or (nil? value) (instance? Long value) (string? value)
                 (keyword? value) (symbol? value) (boolean? value)
                 (char? value) (uuid? value))))))

(defn- portable-index-input?
  [value]
  (if (d/datom? value)
    (and (portable-index-input? (.-a ^Datom value))
         (portable-index-input? (.-v ^Datom value))
         (remote-cache/immutable-data? (meta value)))
    (or (bytes? value) (= Date (class value))
        (remote-cache/immutable-data? value))))

(defn- mutable-index-input?
  [value]
  (if (d/datom? value)
    (or (mutable-index-input? (.-a ^Datom value))
        (mutable-index-input? (.-v ^Datom value)))
    (or (bytes? value) (= Date (class value)))))

(defn- storage-cache-dependencies
  "Keep lookup identity intact while matching invalidation at storage precision.
  Numeric tuple reports can retain uncoerced components, so use their attribute
  bucket rather than comparing those components with decoded native values."
  [store key]
  (if (instance? Store store)
    (let [pattern (cache-key-pattern key)
          position (case (first pattern)
                     (:search :search-tuples :first :count) 3
                     :av-datoms 2
                     (:datoms :populated?) (case (get pattern 1) :ave 3 :eav 4 nil)
                     nil)
          value (when position (get pattern position))]
      (if (or (number? value) (sequential? value))
        (let [attr (get pattern (dec (long position)))
              vt (when attr (idx/value-type ((schema store) attr)))
              normalized (case vt
                           (:db.type/long :db.type/ref)
                           (if (and (number? value) (not (instance? Long value)))
                             (long value) value)

                           :db.type/double
                           (if (and (number? value) (not (instance? Double value)))
                             (double value) value)

                           :db.type/float
                           (if (and (number? value) (not (instance? Float value)))
                             (float value) value)

                           (if (and (vector? vt)
                                    (some #{:db.type/long :db.type/ref
                                            :db.type/double :db.type/float} vt)
                                    (not (qu/lookup-ref? value)))
                             nil value))]
          (if (identical? value normalized)
            key
            (with-meta key (assoc (meta key) ::index-pattern
                                 (assoc pattern position normalized)))))
        key))
    key))

(defn ^:no-doc index-cache-key
  "Distinguish storage representations and custom-codec generations. Keep an
  owned snapshot of mutable lookup inputs for dependency and range checks.
  Opaque representation-sensitive inputs bypass retention."
  [store pattern]
  (when-let [pattern
             (if (and (stored-value-cache-ops (first pattern))
                      (some representation-sensitive? (next pattern)))
               (when (every? portable-index-input? pattern)
                 (let [mutable? (some mutable-index-input? pattern)
                       owned (if mutable?
                               (result-cache/restore (result-cache/snapshot pattern))
                               pattern)
                       ;; Keep immutable components, including signed zero,
                       ;; which serialization alone does not distinguish.
                       identity (if mutable?
                                  (mapv #(if (mutable-index-input? %)
                                           ::mutable-input %) owned)
                                  owned)
                       key (conj identity ::storage-key
                                 (remote-cache/serialized-key owned))]
                   ;; Metadata is excluded from key equality and hashing.
                   (if mutable? (with-meta key {::index-pattern owned}) key)))
               pattern)]
    (storage-cache-dependencies
      store
      (if-let [registry (when (instance? Store store)
                         (get-in @(kv-info (.-lmdb ^Store store))
                                 [:runtime-opts :udf-registry]))]
        (if (cd/custom-schema? (schema store))
          (conj pattern ::codec [registry (udf/generation registry)])
          pattern)
        pattern))))

(defmacro wrap-cache
  [store pattern body]
  `(let [store# ~store
         remote?# (remote-store? store#)
         ;; Direct index reads can bypass db?, including on retained DB views.
         _# (when (and remote?# (cache-active? store#))
              (refresh-remote-cache! store#))
         cache# ^LRUCache (.get ^ConcurrentHashMap caches (dir store#))
         cache?# (and cache# (pos? (.capacity cache#)) (not (.isDisabled cache#))
                      (or (not remote?#) (remote-read-cacheable? store# true)))
         generation# (if cache?# (.generation cache#) 0)
         key# (when cache?# (index-cache-key store# ~pattern))]
     ;; A zero entry limit bypasses key construction and the LRU monitor as
     ;; well as retention. Keep the native read and transaction-overlay paths.
     (if-some [cached# (when key# (.get ^LRUCache cache# key#))]
       (do
         ;; A positive freshness interval can skip the RPC's termination checks.
         (when remote?# (check-remote-read! store#))
         (result-cache/restore cached#))
       (let [res# ~body]
         (if (and key# (or (not remote?#) (remote-read-cacheable? store#)))
           (cache-result store# [cache# generation#] key# res#)
           res#)))))

(defn- sample-init-cache-key
  [a mcount v-ranges pred get-v?]
  [:sample-init-tuples a mcount v-ranges pred get-v?
   u/*reservoir-sampling-seed*])

(defn cached-string-capacity
  "Cache advisory string-dictionary eligibility until the attribute changes.
  compute-capacity is called outside the cache monitor on a miss."
  [store attr compute-capacity]
  (wrap-cache store [:string-cache-capacity attr] (compute-capacity)))

(defn- e-sample-cache-key
  [a mcount]
  [:e-sample a mcount c/init-exec-size-threshold
   u/*reservoir-sampling-seed*])

(defn- tx-touch-summary
  ([tx-data] (tx-touch-summary tx-data true true nil))
  ([tx-data ^LRUCache cache]
   ;; The caller holds the cache monitor through dependency inspection,
   ;; summarization and removal, excluding newly published dependencies.
   (tx-touch-summary tx-data (.hasDependency cache ::all-values)
                     (or (.hasDependencyGroup cache ::attribute-value)
                         (.hasDependencyGroup cache ::value-filter)) cache))
  ([tx-data all-values? attribute-values? ^LRUCache cache]
   (loop [datoms (seq tx-data)
          eids (transient #{})
          attrs (transient #{})
          values (when all-values? (LRUCache/dependencySet))
          values-by-attr nil]
     (if-let [^Datom datom (first datoms)]
       (let [e (.-e datom)
             a (.-a datom)
             v (.-v datom)
             bucket (when attribute-values?
                      (or (get values-by-attr a)
                          ;; Consult the dependency index once per new touched
                          ;; attribute. Needed buckets still deduplicate eagerly.
                          (when (and (not (contains? attrs a))
                                     (or (nil? cache)
                                         (.hasDependencyGroup cache ::attribute-value a)
                                         (.hasDependencyGroup cache ::value-filter a)))
                            (LRUCache/dependencySet))))]
         (when values (.add ^java.util.Set values v))
         (recur (next datoms) (conj! eids e) (conj! attrs a)
                values
                (if bucket
                  (do
                    (.add ^java.util.Set bucket v)
                    (assoc! (or values-by-attr (transient {})) a bucket))
                  values-by-attr)))
       {:eids (persistent! eids)
        :attrs (persistent! attrs)
        :values (or values #{})
        :values-by-attr
        (if values-by-attr
          (persistent! values-by-attr)
          {})}))))

(defn- full-index-value-match?
  [db ^Datom low]
  (let [schema (-schema db)]
    (or (cd/custom-type? (:db/valueType (schema (.-a low))))
        (b/giant? (idx/datom->indexable schema low false)))))

(defn- tx-affects-pattern?
  [{:keys [eids attrs values values-by-attr]} e a v]
  (and (or (nil? e) (contains? eids e))
       (or (nil? a) (contains? attrs a))
       (or (nil? v)
           (let [vs (if (nil? a) values (get values-by-attr a #{}))]
             ;; Stored datoms decode fresh arrays, so identity-based set
             ;; membership cannot invalidate a cached byte-value lookup.
             (if (bytes? v)
               (boolean (some #(d/value-equal? v %) vs))
               (contains? vs v))))))

(defn- unresolved-entid?
  [x]
  (or (qu/lookup-ref? x)
      (keyword? x)))

(defn runtime-opts
  [x]
  (or (:runtime-opts (meta x)) {}))

(defn runtime-opt
  [x k]
  (get (runtime-opts x) k))

(defn with-runtime-opts
  [x opts]
  (with-meta x
    (cond-> (or (meta x) {})
      (some? opts) (assoc :runtime-opts opts)
      (nil? opts)  (dissoc :runtime-opts))))

(defn carry-runtime-opts
  [x source]
  (let [opts (:runtime-opts (meta source))]
    (if (or (some? opts) (contains? (meta x) :runtime-opts))
      (with-runtime-opts x opts)
      x)))

(defn udf-registry
  [x]
  (runtime-opt x :udf-registry))

(defn udf-cache-token
  [x]
  (when-some [registry (udf-registry x)]
    [(System/identityHashCode registry) (udf/generation registry)]))

(defn- unresolved-pattern?
  [e v]
  (or (unresolved-entid? e)
      (unresolved-entid? v)))

(defn- index-components->pattern
  [index c1 c2 c3]
  (case index
    :eav [c1 c2 c3]
    :ave [c3 c1 c2]
    nil))

(defn- tx-affects-attrs-v?
  [{:keys [attrs]} attrs-v]
  (boolean
    (some
      (fn [av]
        (if (sequential? av)
          (contains? attrs (first av))
          (contains? attrs av)))
      attrs-v)))

(defn- exact-cache-value?
  "Whether an exact pattern value can own a value-level dependency bucket.
   Immutable values have stable equality and hashing; ranges, lookup refs and
   mutable containers stay on their broader attribute bucket."
  [v]
  (or (string? v)
      (number? v)
      (boolean? v)
      (symbol? v)
      (uuid? v)
      (and (vector? v) (every? exact-cache-value? v))))

(defn- pattern-cache-dependencies
  ([e a v] (pattern-cache-dependencies e a v true))
  ([e a v exact?]
   (let [unresolved? (unresolved-pattern? e v)
         value-bucket? (and (not unresolved?) (nil? e) exact? (some? a)
                            (exact-cache-value? v))
         deps (cond
                unresolved? [::all]
                (some? e) [[::entity e]]
                value-bucket? [[::attribute-value a v]]
                (some? a) [[::attribute a]]
                :else [::all])]
     ;; These tokens describe precise-filter requirements, not candidates.
     ;; Entity and mutable-value buckets also need values for exact matching.
     (if (and exact? (not unresolved?) (some? v))
       (if (some? a)
         (if value-bucket?
           deps
           (conj deps [::value-filter a nil]))
         (conj deps ::all-values))
       deps))))

(defn- cache-key-dependencies
  "Candidate buckets, computed once when a key enters the LRU. Exact immutable
  patterns bucket by attribute and value; ranges, unresolved patterns and
  mutable values stay on their coarser attribute/entity/all bucket. Exact
  invalidation still checks the key. Fixed-entity native ranges use the entity
  index locally; remote range invalidation and broader ranges stay conservative."
  [k]
  (if (and (vector? k) (keyword? (first k)))
    (case (first k)
      (:search :search-tuples :first :count)
      (let [[_ e a v] k] (pattern-cache-dependencies e a v))

      (:populated? :datoms)
      (let [[_ index c1 c2 c3] k]
        (if-some [[e a v] (index-components->pattern index c1 c2 c3)]
          (pattern-cache-dependencies e a v)
          [::all]))

      ;; Seek ranges start at the given components; any later value for the
      ;; attribute can enter the window, so they stay attribute-wide.
      (:seek :rseek)
      (let [[_ index c1 c2 c3] k]
        (if-some [[e a v] (index-components->pattern index c1 c2 c3)]
          (pattern-cache-dependencies e a v false)
          [::all]))

      :e-datoms
      (pattern-cache-dependencies (get k 1) nil nil)

      :av-datoms
      (pattern-cache-dependencies nil (get k 1) (get k 2))

      :range-datoms
      (let [[_ index low high] k]
        (if (and (#{:eav :ave} index)
                 (instance? Datom low) (instance? Datom high)
                 (= (.-e ^Datom low) (.-e ^Datom high)))
          [::native-range [::entity (.-e ^Datom low)]]
          [::all]))

      (:init-tuples :sample-init-tuples :e-sample :default-ratio :cardinality
       :string-cache-capacity)
      [[::attribute (get k 1)]]

      (:val-eq-scan-e :val-eq-filter-e)
      [[::attribute (get k 3)]]

      :eav-scan-v
      (mapv (fn [av] [::attribute (if (sequential? av) (first av) av)])
            (get k 3))

      :query-result
      (let [deps (get k 1)]
        (if (and (map? deps) (not (:all? deps)))
          (mapv (fn [a] [::attribute a]) (:attrs deps))
          [::all]))

      [::all])
    [::all]))

(defn- cache-invalidation-dependencies
  [{:keys [eids attrs values-by-attr]} local?]
  (let [deps (reduce (fn [deps e] (conj! deps [::entity e]))
                     (transient (if local? [::all] [::all ::native-range])) eids)
        deps (reduce (fn [deps a] (conj! deps [::attribute a])) deps attrs)]
    (persistent!
      (reduce-kv
        (fn [deps a vs]
          (reduce (fn [deps v]
                    (if (exact-cache-value? v)
                      (conj! deps [::attribute-value a v])
                      deps)) deps vs))
        deps values-by-attr))))

(defn- tx-affects-cache-key?
  [touches k]
  (if (and (vector? k) (keyword? (first k)))
    (let [{:keys [attrs]} touches
          tag             (first k)]
      (case tag
        :init-tuples
        (let [[_ a] k]
          (contains? attrs a))

        :sample-init-tuples
        (let [[_ a] k]
          (contains? attrs a))

        :e-sample
        (let [[_ a] k]
          (contains? attrs a))

        :default-ratio
        (let [[_ a] k]
          (contains? attrs a))

        :eav-scan-v
        (let [[_ _ _ attrs-v] k]
          (tx-affects-attrs-v? touches attrs-v))

        :val-eq-scan-e
        (let [[_ _ _ a] k]
          (contains? attrs a))

        :val-eq-filter-e
        (let [[_ _ _ a] k]
          (contains? attrs a))

        :search
        (let [[_ e a v] k]
          (or (unresolved-pattern? e v)
              (tx-affects-pattern? touches e a v)))

        :search-tuples
        (let [[_ e a v] k]
          (or (unresolved-pattern? e v)
              (tx-affects-pattern? touches e a v)))

        :first
        (let [[_ e a v] k]
          (or (unresolved-pattern? e v)
              (tx-affects-pattern? touches e a v)))

        :count
        (let [[_ e a v] k]
          (or (unresolved-pattern? e v)
              (tx-affects-pattern? touches e a v)))

        :populated?
        (let [[_ index c1 c2 c3] k]
          (if-some [[e a v] (index-components->pattern index c1 c2 c3)]
            (or (unresolved-pattern? e v)
                (tx-affects-pattern? touches e a v))
            true))

        :datoms
        (let [[_ index c1 c2 c3] k]
          (if-some [[e a v] (index-components->pattern index c1 c2 c3)]
            (or (unresolved-pattern? e v)
                (tx-affects-pattern? touches e a v))
            true))

        :e-datoms
        (let [[_ e] k]
          (or (unresolved-entid? e)
              (tx-affects-pattern? touches e nil nil)))

        :av-datoms
        (let [[_ a v] k]
          (or (unresolved-entid? v)
              (tx-affects-pattern? touches nil a v)))

        :range-datoms
        true

        :seek
        (let [[_ index c1 c2 c3] k]
          (if-some [[_ a _] (index-components->pattern index c1 c2 c3)]
            (let [[e* _ v*] (index-components->pattern index c1 c2 c3)]
              (or (unresolved-pattern? e* v*)
                  (tx-affects-pattern? touches e* a nil)))
            true))

        :rseek
        (let [[_ index c1 c2 c3] k]
          (if-some [[_ a _] (index-components->pattern index c1 c2 c3)]
            (let [[e* _ v*] (index-components->pattern index c1 c2 c3)]
              (or (unresolved-pattern? e* v*)
                  (tx-affects-pattern? touches e* a nil)))
            true))

        (:cardinality :string-cache-capacity)
        (let [[_ a] k]
          (contains? attrs a))

        (:index-range :index-range-size)
        (let [[_ a start end] k]
          (or (unresolved-pattern? nil start)
              (unresolved-pattern? nil end)
              (contains? attrs a)))

        :query-result
        (let [[_ deps] k]
          (if (map? deps)
            (or (:all? deps)
                (boolean (some attrs (:attrs deps))))
            true))

        true))
    true))

(defn- remove-affected-cache-entries!
  [store ^LRUCache cache touches tx-data]
  (let [ranges (delay (range-cache/context
                       (when (instance? Store store) (schema store)) tx-data))]
    (doseq [k (.candidateKeys cache (cache-invalidation-dependencies
                                     touches (instance? Store store)))
            :let [pattern (cache-key-pattern k)]
            :when (if (and (instance? Store store) (range-cache/range-key? pattern))
                    (range-cache/affected? @ranges pattern)
                    (tx-affects-cache-key? touches pattern))]
      (.remove cache k))))

(defn- invalidate-cache
  ([store tx-data target]
   (invalidate-cache store tx-data target nil))
  ([store tx-data target remote-max-tx]
   (invalidate-cache store tx-data target remote-max-tx nil))
  ([store tx-data target remote-max-tx datom-count]
   (if-some [^LRUCache cache (.get ^ConcurrentHashMap caches (dir store))]
     (do
       (if (remote-store? store)
         ;; Reports omit other clients' commits and in-flight remote reads.
         (locking cache
           (.beginInvalidation cache (long (or target 0)))
           (.clear cache))
         (if (if (some? datom-count) (pos? (long datom-count)) (seq tx-data))
           (locking cache
             (.beginInvalidation cache (long (or target 0)))
             (when-not (.isEmpty cache)
               ;; Bound invalidation work for bulk writes. Counted requests and
               ;; grouped metadata decide this without scanning their datoms.
               (let [limit (max 4096 (* 8 (long (.capacity cache))))
                     n (or datom-count
                           (when (instance? Collection tx-data)
                             (.size ^Collection tx-data))
                           (bounded-count limit tx-data))]
                 (if (or (nil? tx-data) (>= (long n) limit))
                   (.clear cache)
                   (let [touches (tx-touch-summary tx-data cache)]
                     (remove-affected-cache-entries! store cache touches tx-data))))))
           (.setTarget cache (long (or target 0)))))
       (mark-remote-cache-max-tx! store remote-max-tx))
     (refresh-cache store target remote-max-tx))))

(defn- invalidate-remote-cache!
  "Discard results after an uncertain write without fetching remote metadata."
  [store]
  (let [cache-dir (dir store)]
    (when-let [^LRUCache cache (.get ^ConcurrentHashMap caches cache-dir)]
      (locking cache
        ;; Even an empty cache may have readers awaiting pre-commit responses.
        ;; Preserve the target until the next successful metadata refresh.
        (.beginInvalidation cache (.target cache))
        (.clear cache)))
    (.remove ^ConcurrentHashMap remote-cache-check-ms cache-dir)))

(defn ^:no-doc make-write-group
  "Create a group after disabling its read cache. Empty disabled caches cannot
  admit entries, so only pending resolver work needs request datom vectors."
  [^Store store]
  (let [^LRUCache cache (.get ^ConcurrentHashMap caches (dir store))
        retain? (if cache
                  (locking cache
                    (not (and (.isDisabled cache) (.isEmpty cache))))
                  true)]
    (s/write-group (.-lmdb store) retain?)))

(defn ^:no-doc invalidate-write-group!
  "Invalidate committed Datalog writes using their consolidated metadata."
  [store ^WriteGroup group]
  (let [^longs metadata (.-metadata group)]
    (when-not (neg? (aget metadata 0))
      (invalidate-cache store
                        (when (.-retain-cache-datoms? group)
                          (eduction cat (.-datom-batches group)))
                        (aget metadata 1) nil (aget metadata 2)))))

(defn ^:no-doc execute-write-group
  "Execute logical writes on a private writing connection with caching disabled.
  Flush final metadata and invalidate combined dependencies before native commit."
  [conn execute]
  (let [^Store store (:store @conn)
        _ (assert (cache-disabled? store))
        ^WriteGroup group (make-write-group store)]
    (binding [s/*write-group* group]
      (let [result (execute conn)]
        (s/flush-write-group! group)
        (invalidate-write-group! (:store @conn) group)
        {:result result :changed? (pos? (aget ^longs (.-metadata group) 2))}))))

(defn- tx-cache-empty?
  [db]
  (let [^TreeSortedSet eavt (:eavt db)
        ^TreeSortedSet avet (:avet db)]
    (and (or (nil? eavt) (.isEmpty eavt))
         (or (nil? avet) (.isEmpty avet)))))

(defn- tx-cache-active?
  [db]
  (and (not (tx-cache-empty? db))
       (> (long (:max-tx db)) (long (or (max-tx (:store db)) 0)))))

(defn ^:no-doc pending-tx-cache?
  [db]
  (tx-cache-active? db))

(defn- tx-cache-e-datoms
  [db e]
  (filter #(= e (.-e ^Datom %)) (:eavt db)))

(defn- cmp-attrs-by-aid
  [schema a b]
  (let [aid-a (:db/aid (schema a))
        aid-b (:db/aid (schema b))]
    (if (and aid-a aid-b)
      (Long/compare (long aid-a) (long aid-b))
      (d/nil-cmp a b))))

(defn- cmp-datoms-eavt-schema
  [schema value-cmp ^Datom d1 ^Datom d2]
  (u/combine-cmp
    (Long/compare (.-e d1) (.-e d2))
    (cmp-attrs-by-aid schema (.-a d1) (.-a d2))
    (value-cmp (.-a d1) (.-v d1) (.-v d2))
    (Long/compare (d/datom-tx d1) (d/datom-tx d2))))

(defn- cmp-datoms-avet-schema
  [schema value-cmp ^Datom d1 ^Datom d2]
  (u/combine-cmp
    (cmp-attrs-by-aid schema (.-a d1) (.-a d2))
    (value-cmp (.-a d1) (.-v d1) (.-v d2))
    (Long/compare (.-e d1) (.-e d2))
    (Long/compare (d/datom-tx d1) (d/datom-tx d2))))

(defn- tx-cache-index-comparator
  [db index]
  (let [schema (-schema db)
        store (:store db)
        kv (when (instance? Store store) (.-lmdb ^Store store))
        value-cmp (cd/value-comparator kv (constantly schema))]
    (case index
      :eav #(cmp-datoms-eavt-schema schema value-cmp %1 %2)
      :ave #(cmp-datoms-avet-schema schema value-cmp %1 %2)
      nil)))

(defn- tx-cache-range-datoms
  [db index start-datom end-datom]
  (let [store (:store db)
        kv (when (instance? Store store) (.-lmdb ^Store store))]
    (filter (idx/datom-range-predicate kv (-schema db) start-datom end-datom)
            (case index :eav (:eavt db) :ave (:avet db) nil))))

(defn- datom-eav-key
  [^Datom datom]
  (let [v (.-v datom)]
    ;; Native reads decode fresh arrays; retract them by content, not identity.
    (if (bytes? v)
      [(.-e datom) (.-a datom) ::bytes (ByteBuffer/wrap ^bytes v)]
      [(.-e datom) (.-a datom) v])))

(defn- merge-tx-cache-datoms
  [db index base cached]
  (let [cached    (vec cached)
        retracted (into #{} (comp (remove d/datom-added)
                                  (map datom-eav-key))
                        cached)
        additions (filter d/datom-added cached)]
    (sort (tx-cache-index-comparator db index)
          (concat (if (seq retracted)
                    (remove #(contains? retracted (datom-eav-key %)) base)
                    base)
                  additions))))

(defn- with-tx-cache-range
  [db index start-datom end-datom base]
  (if-not (tx-cache-active? db)
    base
    (if-let [cached (seq (tx-cache-range-datoms db index start-datom end-datom))]
      (merge-tx-cache-datoms db index base cached)
      base)))

(defn- with-tx-cache-e-datoms
  [db e base]
  (if-not (tx-cache-active? db)
    base
    (let [cached (tx-cache-e-datoms db e)]
      (if (seq cached)
        (merge-tx-cache-datoms db :eav base cached)
        base))))

(defn- with-tx-cache-search
  [db [e a v] base]
  (if-not (tx-cache-active? db)
    base
    (let [value? (when (some? v) (s/vpred v))
          cached (filter (fn [^Datom datom]
                           (and (or (nil? e) (= e (.-e datom)))
                                (or (nil? a) (= a (.-a datom)))
                                (or (nil? value?) (value? (.-v datom)))))
                         (:eavt db))]
      (merge-tx-cache-datoms db :eav base cached))))

(defn- content-search? [store attr value]
  (when-let [props ((schema store) attr)]
    (scans/content-scan? (idx/value-type props) value)))

(defn- content-search-tuples [store entity attr value]
  (if (some? entity)
    (when (some #(QueryKey/equiv value (aget ^objects % 0))
                (s/ea-tuples store entity attr))
      (rel/single-tuples (object-array [entity attr value])))
    (s/av-tuples store attr value)))

(defrecord-updatable DB [^IStore store
                         ^long max-eid
                         ^long max-tx
                         ^TreeSortedSet eavt
                         ^TreeSortedSet avet
                         pull-patterns
                         pull-readers]

  ISearchable
  (-searchable? [_] true)

  IDB
  (-schema [_] (schema store))
  (-rschema [_] (rschema store))
  (-attrs-by [db property] ((-rschema db) property))
  (-is-attr? [db attr property] (contains? (-attrs-by db property) attr))
  (-clear-tx-cache
    [db]
    (let [clear #(.clear ^TreeSortedSet %)]
      (clear eavt)
      (clear avet)
      db))

  IControlledTuples
  (-init-tuples-controlled
    [_ out attr get-v? policy]
    ((:check! policy))
    (when-let [props (get (:schema policy) attr)]
      (let [kv (.-lmdb ^datalevin.storage.Store store)]
        ((if (:db/noindex props) scans/eav-attribute-tuples-scan-controlled scans/ave-tuples-scan-controlled)
          kv out (:db/aid props) get-v? policy))))

  (-lookup-tuples-controlled
    [_ out attr entities get-v? policy]
    ((:check! policy))
    (when-let [props (get (:schema policy) attr)]
      (scans/eav-tuples-scan-controlled
        (.-lmdb ^datalevin.storage.Store store) out (:db/aid props) entities get-v? policy)))

  (-value-tuples-controlled
    [_ out attr values policy]
    ((:check! policy))
    (when-let [props (get (:schema policy) attr)]
      (let [kv (.-lmdb ^datalevin.storage.Store store)]
        (if (:db/noindex props)
          (dotimes [n (.size ^java.util.List values)]
            ((:check! policy))
            (scans/eav-attribute-tuples-scan-controlled kv out (:db/aid props) true
              (assoc policy :key (.get ^java.util.List values n) :value-first? true)))
          (scans/ave-value-tuples-scan-controlled kv out (:db/aid props) values policy)))))

  ITuples
  (-init-tuples
    [_ out a v-ranges pred get-v?]
    (ave-tuples store out a v-ranges pred get-v?))

  (-init-tuples-list
    [_ a v-ranges pred get-v?]
    (wrap-cache
      store [:init-tuples a v-ranges pred get-v?]
      (ave-tuples-list store a v-ranges pred get-v?)))

  (-sample-init-tuples
    [_ out a mcount v-ranges pred get-v?]
    (sample-ave-tuples store out a mcount v-ranges pred get-v?))

  (-sample-init-tuples-list
    [_ a mcount v-ranges pred get-v?]
    (wrap-cache
      store (sample-init-cache-key a mcount v-ranges pred get-v?)
      (sample-ave-tuples-list store a mcount v-ranges pred get-v?)))

  (-e-sample
    [_ a]
    (if (some? u/*reservoir-sampling-seed*)
      (let [mcount (a-size store a)]
        (wrap-cache
          store (e-sample-cache-key a mcount)
          (sample-ave-tuples-list
            store a mcount [[[:closed c/v0] [:closed c/vmax]]] nil false)))
      (wrap-cache
        store [:e-sample a]
        (e-sample store a))))

  (-default-ratio
    [_ a]
    (wrap-cache
      store [:default-ratio a]
      (default-ratio store a)))

  (-eav-scan-v
    [_ in out eid-idx attrs-v]
    (eav-scan-v store in out eid-idx attrs-v))

  (-eav-scan-v-list
    [_ in eid-idx attrs-v]
    ;; Derived rows belong to this query's operators. Their keys contain
    ;; mutable input tuples, not stable lookup parameters. Keep them out of
    ;; the shared read cache. Presence filters may reuse input rows.
    (eav-scan-v-list store in eid-idx attrs-v))

  (-eav-scan-v-list
    [_ in eid-idx attrs-v prepared]
    (if (instance? Store store)
      (prepared (.-lmdb ^Store store) in eid-idx)
      (eav-scan-v-list store in eid-idx attrs-v)))

  (-eav-filter-presence-list
    [_ in eid-idx attr]
    (eav-scan-v-list store in eid-idx [[attr {:skip? true}]]))

  (-val-eq-scan-e
    [_ in out v-idx attr]
    (val-eq-scan-e store in out v-idx attr))

  (-val-eq-scan-e-list
    [_ in v-idx attr]
    (val-eq-scan-e-list store in v-idx attr))

  (-val-eq-scan-e
    [_ in out v-idx attr bound]
    (val-eq-scan-e store in out v-idx attr bound))

  (-val-eq-scan-e-list
    [_ in v-idx attr bound]
    (val-eq-scan-e-list store in v-idx attr bound))

  (-val-eq-filter-e
    [_ in out v-idx attr f-idx]
    (val-eq-filter-e store in out v-idx attr f-idx))

  (-val-eq-filter-e-list
    [_ in v-idx attr f-idx]
    (val-eq-filter-e-list store in v-idx attr f-idx))

  ISearch
  (-search
    [db pattern]
    (let [[e a v _] pattern]
      (when (nil? e) (vld/validate-indexed-attr (schema store) a))
      (with-tx-cache-search
        db pattern
        (wrap-cache
          store [:search e a v]
          (case-tree
            [e a (some? v)]
            [(fetch store (datom e a v)) ; e a v
             (slice store :eav (datom e a c/v0) (datom e a c/vmax)) ; e a _
             (slice-filter store :eav
                           (fn [^Datom d] (when ((s/vpred v) (.-v d)) d))
                           (datom e nil nil)
                           (datom e nil nil))  ; e _ v
             (e-datoms store e) ; e _ _
             (av-datoms store a v) ; _ a v
             (if (remote-store? store)
               ;; Client-side overlays merge remote datoms with pending writes.
               ;; The tuple storage primitive is available only to local stores.
               (slice store :ave (datom e0 a c/v0) (datom emax a c/vmax))
               (mapv #(datom (aget ^objects % 0) a (aget ^objects % 1))
                     (ave-tuples-list
                       store a [[[:closed c/v0] [:closed c/vmax]]] nil true))) ; _ a _
             (slice-filter store :eav
                           (fn [^Datom d] (when ((s/vpred v) (.-v d)) d))
                           (datom e0 nil nil)
                           (datom emax nil nil)) ; _ _ v
             (slice store :eav (datom e0 nil nil) (datom emax nil nil))]))))) ; _ _ _

  (-search-tuples
    [_ pattern]
    (let [[e a v _] pattern]
      (when (nil? e) (vld/validate-indexed-attr (schema store) a))
      (wrap-cache
        store [:search-tuples e a v]
        (if (content-search? store a v)
          (content-search-tuples store e a v)
          (case-tree
            [e a (some? v)]
            [(when (if (cd/custom-type? (:db/valueType ((schema store) a)))
                     (seq (fetch store (d/datom e a v)))
                     (populated? store :eav (d/datom e a v) (d/datom e a v)))
               (rel/single-tuples (object-array [e a v]))) ; e a v
             (s/ea-tuples store e a) ; e a _
             (s/ev-tuples store e v)  ; e _ v
             (s/e-tuples store e) ; e _ _
             (s/av-tuples store a v) ; _ a v
             (s/a-tuples store a) ; _ a _
             (s/v-tuples store v) ; _ _ v
             (s/all-tuples store)]))))) ; _ _ _

  (-first
    [db pattern]
    (if (tx-cache-active? db)
      (first (-search db pattern))
      (let [[e a v _] pattern]
        (when (nil? e) (vld/validate-indexed-attr (schema store) a))
        (wrap-cache
          store [:first e a v]
          (case-tree
            [e a (some? v)]
            [(first (fetch store (datom e a v))) ; e a v
             (ea-first-datom store e a) ; e a _
             (head-filter store :eav
                          (fn [^Datom d]
                            (when ((s/vpred v) (.-v d)) d))
                          (datom e nil nil)
                          (datom e nil nil))  ; e _ v
             (e-first-datom store e) ; e _ _
             (av-first-datom store a v) ; _ a v
             (head store :ave (datom e0 a nil) (datom emax a nil)) ; _ a _
             (head-filter store :eav
                          (fn [^Datom d]
                            (when ((s/vpred v) (.-v d)) d))
                          (datom e0 nil nil)
                          (datom emax nil nil)) ; _ _ v
             (head store :eav (datom e0 nil nil) (datom emax nil nil))]))))) ; _ _ _

  (-count
    [db pattern]
    (.-count db pattern nil))
  (-count
    [_ pattern cap]
    (let [[e a v] pattern]
      (when (nil? e) (vld/validate-indexed-attr (schema store) a))
      (wrap-cache
        store [:count e a v cap]
        (if (content-search? store a v)
          (count (content-search-tuples store e a v))
          (case-tree
            [e a (some? v)]
            [(if (cd/custom-type? (:db/valueType ((schema store) a)))
               (count (fetch store (datom e a v)))
               (size store :eav (datom e a v) (datom e a v))) ; e a v
             (size store :eav (datom e a c/v0) (datom e a c/vmax)) ; e a _
             (size-filter store :eav
                          (fn [^Datom d] ((s/vpred v) (.-v d)))
                          (datom e nil nil) (datom e nil nil))  ; e _ v
             (e-size store e) ; e _ _
             (av-size store a v) ; _ a v
             (a-size store a) ; _ a _
             (v-size store v) ; _ _ v, for ref only
             (datom-count store :eav)]))))) ; _ _ _

  IIndexAccess
  (-populated?
    [db index c1 c2 c3]
    (wrap-cache
      store [:populated? index c1 c2 c3]
      (populated? store index
                  (components->pattern db index c1 c2 c3 e0 v0)
                  (components->pattern db index c1 c2 c3 emax vmax))))

  (-datoms
    [db index]
    (-datoms db index nil nil nil))
  (-datoms
    [db index c1]
    (-datoms db index c1 nil nil))
  (-datoms
    [db index c1 c2]
    (-datoms db index c1 c2 nil))
  (-datoms
    [db index c1 c2 c3]
    (wrap-cache
      store [:datoms index c1 c2 c3]
      (let [v  (case index :eav c3 :ave c2)
            low (components->pattern db index c1 c2 c3 e0 v0)
            ds (slice store index
                      low
                      (components->pattern db index c1 c2 c3 emax vmax))]
        ;; Giant physical keys retain only a prefix and an allocated ID.
        ;; Match the complete resolved value, including byte-array contents.
        (if (and (some? v) (full-index-value-match? db low))
          (filterv #(d/value-equal? (:v low) (:v %)) ds)
          ds))))
  (-datoms
    [db index c1 c2 c3 n]
    (wrap-cache
      store [:datoms index c1 c2 c3 n]
      (let [v (case index :eav c3 :ave c2)
            low (components->pattern db index c1 c2 c3 e0 v0)]
        (if (and (some? v) (full-index-value-match? db low))
          (take n (-datoms db index c1 c2 c3))
          (slice store index low
                 (components->pattern db index c1 c2 c3 emax vmax)
                 n)))))

  (-e-datoms
    [db e]
    (if (tx-cache-empty? db)
      (wrap-cache store [:e-datoms e] (e-datoms store e))
      (with-tx-cache-e-datoms db e (e-datoms store e))))

  (-av-datoms
    [_ attr v]
    (vld/validate-indexed-attr (schema store) attr)
    (wrap-cache store [:av-datoms attr v] (av-datoms store attr v)))

  (-range-datoms
    [db index start-datom end-datom]
    (if (tx-cache-empty? db)
      (wrap-cache
        store [:range-datoms index start-datom end-datom]
        (slice store index start-datom end-datom))
      (with-tx-cache-range
        db index start-datom end-datom
        (slice store index start-datom end-datom))))

  (-seek-datoms
    [db index c1 c2 c3]
    (wrap-cache
      store [:seek index c1 c2 c3]
      (slice store index
             (components->pattern db index c1 c2 c3 e0 v0)
             (components->end-datom db index c1 c2 c3 emax vmax))))
  (-seek-datoms
    [db index c1 c2 c3 n]
    (wrap-cache
      store [:seek index c1 c2 c3 n]
      (slice store index
             (components->pattern db index c1 c2 c3 e0 v0)
             (components->end-datom db index c1 c2 c3 emax vmax)
             n)))

  (-rseek-datoms
    [db index c1 c2 c3]
    (wrap-cache
      store [:rseek index c1 c2 c3]
      (rslice store index
              (components->pattern db index c1 c2 c3 emax vmax)
              (components->end-datom db index c1 c2 c3 e0 v0))))
  (-rseek-datoms
    [db index c1 c2 c3 n]
    (wrap-cache
      store [:rseek index c1 c2 c3 n]
      (rslice store index
              (components->pattern db index c1 c2 c3 emax vmax)
              (components->end-datom db index c1 c2 c3 e0 v0)
              n)))

  (-cardinality
    [_ attr]
    (vld/validate-indexed-attr (schema store) attr)
    (wrap-cache store [:cardinality attr]
                (cardinality store attr)))

  (-index-range
    [db attr start end]
    (vld/validate-indexed-attr (schema store) attr)
    (wrap-cache
      store [:index-range attr start end]
      (do (vld/validate-attr attr (list '-index-range 'db attr start end))
          (slice store :ave (resolve-datom db nil attr start e0 v0)
                 (resolve-datom db nil attr end emax vmax)))))

  (-index-range-size
    [_ attr start end]
    (vld/validate-indexed-attr (schema store) attr)
    (wrap-cache
      store [:index-range-size attr start end]
      (av-range-size store attr start end))))

(defn ^:no-doc -ea-populated?
  "Test whether an entity has an attribute without caching or retrieving its
   value. Intended for batched existence probes with mostly unique entities."
  [^DB db e a]
  (let [store (.-store db)]
    (populated? store :eav
                (d/datom e a c/v0)
                (d/datom e a c/vmax))))

(defn ^:no-doc local-ref-attr-adjacency?
  [^DB db]
  (instance? Store (.-store db)))

(defn ^:no-doc ref-attr-adjacency
  [^DB db attr bound-side]
  (when (local-ref-attr-adjacency? db)
    (s/ref-attr-adjacency (.-store db) attr bound-side)))

;; (defmethod print-method DB [^DB db, ^java.io.Writer w]
;;   (binding [*out* w]
;;     (let [{:keys [store eavt max-eid max-tx]} db]
;;       (pr {:db-name       (s/db-name store)
;;            :last-modified (s/last-modified store)
;;            :datom-count   (count eavt)
;;            :max-eid       max-eid
;;            :max-tx        max-tx}))))

(defn- refresh-remote-cache!
  [store]
  (let [cache (.get ^ConcurrentHashMap caches (dir store))]
    (when (should-check-remote-cache? store cache)
      (let [{:keys [last-modified max-tx schema-revision]} (i/db-info store)
            target        (long (or last-modified 0))
            cached-max-tx (cached-remote-cache-max-tx store)]
        (if (or (nil? cache)
                (remote-schema-changed? store schema-revision)
                (< ^long (.target ^LRUCache cache) ^long target)
                (and (some? max-tx)
                     (some? cached-max-tx)
                     (< (long cached-max-tx)
                        (long max-tx))))
          (refresh-cache store target max-tx)
          (do
            (mark-remote-cache-max-tx! store max-tx)
            (mark-remote-cache-check! store)))
        (mark-remote-schema-revision! store schema-revision)))))

(defn db?
  "Check if x is an instance of DB.
  For remote DBs, refresh the local cache when the server reports newer state.
  Local DB cache freshness is maintained by local transaction invalidation."
  [x]
  (when (-searchable? x)
    (let [store (.-store ^DB x)]
      (when (remote-store? store)
        (refresh-remote-cache! store)))
    true))

(defn search-datoms [db e a v] (-search db [e a v]))

(defn count-datoms [db e a v] (-count db [e a v] nil))

(defn seek-datoms
  ([db index]
   (-seek-datoms db index nil nil nil))
  ([db index c1]
   (-seek-datoms db index c1 nil nil))
  ([db index c1 c2]
   (-seek-datoms db index c1 c2 nil))
  ([db index c1 c2 c3]
   (-seek-datoms db index c1 c2 c3))
  ([db index c1 c2 c3 n]
   (-seek-datoms db index c1 c2 c3 n)))

(defn rseek-datoms
  ([db index]
   (-rseek-datoms db index nil nil nil))
  ([db index c1]
   (-rseek-datoms db index c1 nil nil))
  ([db index c1 c2]
   (-rseek-datoms db index c1 c2 nil))
  ([db index c1 c2 c3]
   (-rseek-datoms db index c1 c2 c3))
  ([db index c1 c2 c3 n]
   (-rseek-datoms db index c1 c2 c3 n)))

(defn max-eid [db] (init-max-eid (:store db)))

(defn analyze
  ([db] {:pre [(db? db)]}
   (i/analyze (:store db) nil))
  ([db attr] {:pre [(db? db)]}
   (i/analyze (:store db) attr)))

;; ----------------------------------------------------------------------------

(defn- split-runtime-opts
  [opts]
  (if (map? opts)
    [(dissoc opts :runtime-opts) (:runtime-opts opts)]
    [opts nil]))

(defn- open-store
  [dir schema opts]
  (if (u/dtlv-uri? dir)
    (i/open-remote-store dir schema opts)
    (s/open dir schema opts)))

(defn- custom-tx-datom-comparator [store index schema-fn resolver-kv]
  (let [kv (when (instance? Store store) (.-lmdb ^Store store))
        value-cmp (cd/value-comparator kv schema-fn
                                       (or resolver-kv (constantly kv)))]
    (if (= index :eav)
      (reify Comparator
        (compare [_ x y]
          (let [^Datom x x ^Datom y y]
            (int (u/combine-cmp
                   (Long/compare (.-e x) (.-e y))
                   (d/nil-cmp (.-a x) (.-a y))
                   (value-cmp (.-a x) (.-v x) (.-v y))
                   (Long/compare (d/datom-tx x) (d/datom-tx y)))))))
      (reify Comparator
        (compare [_ x y]
          (let [^Datom x x ^Datom y y]
            (int (u/combine-cmp
                   (d/nil-cmp (.-a x) (.-a y))
                   (value-cmp (.-a x) (.-v x) (.-v y))
                   (Long/compare (.-e x) (.-e y))
                   (Long/compare (d/datom-tx x) (d/datom-tx y))))))))))


(deftype ^:private TxDatomComparator
  [store index schema-fn resolver-kv ordinary ^:volatile-mutable state]
  Comparator
  (compare [_ x y]
    ;; Store schemas can change independently across transaction wrappers.
    ;; Keep the target binding, but allocate custom ordering only on first use.
    (let [current (if schema-fn (schema-fn) (schema store))
          cached state
          comparator (if (identical? current (nth cached 0 nil))
                       (nth cached 1)
                       (let [custom? (cd/custom-schema? current)
                             ;; Preserve equal-prefix ordinals across schema
                             ;; changes without reordering the live overlay.
                             custom (or (nth cached 2 nil)
                                        (when custom?
                                          (custom-tx-datom-comparator
                                            store index
                                            (or schema-fn #(schema store))
                                            resolver-kv)))
                             comparator (if custom? custom ordinary)]
                         ;; Publish the schema, comparator and custom state as
                         ;; one snapshot, including for shared same-store views.
                         (set! state [current comparator custom])
                         comparator))]
      (.compare ^Comparator comparator x y))))

(defn- tx-datom-comparator
  ([store index] (tx-datom-comparator store index nil nil))
  ([store index schema-fn] (tx-datom-comparator store index schema-fn nil))
  ([store index schema-fn resolver-kv]
   (TxDatomComparator. store index schema-fn resolver-kv
                       (if (= index :eav) d/cmp-datoms-eavt d/cmp-datoms-avet)
                       nil)))

(defn ^:no-doc batch-tx-indexes
  "Create batch-owned resolver indexes. Custom comparisons use the owner's
  unexposed native Store for cache identity; resolver-kv supplies the active
  member's scoped handle for custom functions."
  [store schema-fn resolver-kv]
  [(TreeSortedSet. ^Comparator (tx-datom-comparator store :eav schema-fn resolver-kv))
   (TreeSortedSet. ^Comparator (tx-datom-comparator store :ave schema-fn resolver-kv))])

(defn new-db
  "Construct a current DB view. Rebuilds of the same database can supply
  `previous` to retain its parsed pull patterns; pull checks schema identity
  before using each entry. Transaction-local datom overlays are always fresh.
  Secondary-index recovery is owned by store open, and newly queued jobs wake
  the worker in `load-datoms-with-plan!`; publishing a transaction must not
  rescan the durable job table."
  ([^IStore store] (new-db store nil))
  ([^IStore store info] (new-db store info nil))
  ([^IStore store info ^DB previous]
   (let [info (or info
                  (when (remote-store? store)
                    (i/db-info store)))
         db   (map->DB
                {:store         store
                 :max-eid       (if info (:max-eid info) (init-max-eid store))
                 :max-tx        (if info (:max-tx info) (max-tx store))
                 :eavt          (TreeSortedSet. ^Comparator (tx-datom-comparator store :eav))
                 :avet          (TreeSortedSet. ^Comparator (tx-datom-comparator store :ave))
                 :pull-patterns (if previous
                                  (.-pull-patterns previous)
                                  (LRUCache. 64))
                 :pull-readers  (if previous
                                  (.-pull-readers previous)
                                  (LRUCache. 64))})]
     (swap! dbs assoc (db-name store) db)
     (ensure-cache store
                   (if info (:last-modified info) (last-modified store))
                   (when info (:max-tx info))
                   (when info (:schema-revision info)))
     (start-sampling store)
     db)))

(defn transfer
  [^DB old store]
  (let [same-store? (identical? store (.-store old))
        ^Comparator eav-cmp (if same-store?
                              (.comparator ^TreeSortedSet (.-eavt old))
                              (tx-datom-comparator store :eav))
        ^Comparator ave-cmp (if same-store?
                              (.comparator ^TreeSortedSet (.-avet old))
                              (tx-datom-comparator store :ave))]
    (carry-runtime-opts
      ;; eavt/avet are mutable transaction-local overlays, not durable indexes.
      ;; A transferred DB view must get fresh caches so local write transactions
      ;; do not leak in-memory datoms across connection/transaction wrappers.
      (DB. store
           (.-max-eid old)
           (.-max-tx old)
           (TreeSortedSet. eav-cmp)
           (TreeSortedSet. ave-cmp)
           (.-pull-patterns old)
           (.-pull-readers old))
      old)))

(defn ^:no-doc transfer-report-db
  "Publish a report view using the committed store's index comparators.
  Each view still owns fresh mutable indexes, while comparator setup is shared."
  [^DB old ^DB committed]
  (carry-runtime-opts
    (DB. (.-store committed)
         (.-max-eid old)
         (.-max-tx old)
         (TreeSortedSet. (.comparator ^TreeSortedSet (.-eavt committed)))
         (TreeSortedSet. (.comparator ^TreeSortedSet (.-avet committed)))
         (.-pull-patterns old)
         (.-pull-readers old))
    old))

(defn ^:no-doc adopt-current-db!
  "Register an already constructed current DB value without repeating the
  store-open lifecycle work performed by `new-db`."
  [^DB db]
  (swap! dbs assoc (db-name (.-store db)) db)
  db)

(defn empty-db
  ([] (empty-db nil nil))
  ([dir] (empty-db dir nil))
  ([dir schema] (empty-db dir schema nil))
  ([dir schema opts]
   {:pre [(or (nil? schema) (map? schema))]}
   (vld/validate-schema-update schema)
   (let [[_ runtime-opts] (split-runtime-opts opts)]
     (cond-> (new-db (open-store dir schema opts))
       (some? runtime-opts) (with-runtime-opts runtime-opts)))))

(def coerce-inst prepare/coerce-inst)

(def coerce-uuid prepare/coerce-uuid)

(defn- correct-datom
  [store ^Datom datom]
  (prepare/correct-datom store datom))

(defn- pour
  [store datoms]
  (let [batch-size (long c/*fill-db-batch-size*)]
    (when-not (<= 1 batch-size Integer/MAX_VALUE)
      (raise "Invalid fill-db batch size" {:batch-size batch-size}))
    (let [;; Remote schema/options are RPCs: fetch them once per load, not
          ;; twice for every datom.
          correct (if (remote-store? store)
                    (let [store-opts (opts store)
                          schema     (schema store)]
                      (fn [^Datom datom]
                        (prepare/correct-datom*
                          datom
                          (prepare/correct-value-with-props
                            store-opts (schema (.-a datom))
                            (.-a datom) (.-v datom)))))
                    #(correct-datom store %))
          ;; Allocate backing storage only as datoms arrive.
          ^FastList batch (FastList.)]
      (doseq [datom datoms]
        ;; Check each element here so lazy input is not walked before loading.
        (vld/validate-datom datom)
        (.add batch (correct datom))
        (when (= batch-size (.size batch))
          (load-datoms store batch)
          (.clear batch)))
      (when-not (.isEmpty batch)
        (load-datoms store batch)))))

(defn close-db [^DB db]
  (let [store ^IStore (.-store db)]
    (stop-sampling store)
    (remove-cache store)
    (swap! dbs dissoc (db-name store))
    (close store)
    nil))

(defn- local-store
  [^DB db op]
  (let [store (.-store db)]
    (if (instance? Store store)
      store
      (raise "Secondary index job APIs require a local Datalog store"
               {:op op
                :store (some-> store class str)}))))

(defn secondary-index-status
  [^DB db]
  (s/secondary-index-status (local-store db :secondary-index-status)))

(defn process-secondary-index-jobs!
  ([^DB db]
   (process-secondary-index-jobs! db nil))
  ([^DB db opts]
   (s/process-secondary-index-jobs!
    (local-store db :process-secondary-index-jobs!)
    opts)))

(defn wait-for-secondary-index
  ([^DB db]
   (wait-for-secondary-index db nil))
  ([^DB db opts]
   (s/wait-for-secondary-index
    (local-store db :wait-for-secondary-index)
    opts)))

(defn- quick-fill-local
  [^Store store datoms]
  (let [lmdb (.-lmdb store)]
    ;; Own the shared writer before taking either settings snapshot. Another
    ;; load must not save temporary WAL/:nosync values as its restore target.
    ;; The existing guard also keeps runtime administration in lock order.
    (kvtx/with-runtime-txlog-rollback
      lmdb
      (fn []
        (let [flags   (get-env-flags lmdb)
              nosync? (:nosync flags)
              loaded  (volatile! nil)]
          (set-env-flags lmdb #{:nosync} true)
          (try
            (binding [c/*ordered-datom-writes?* true]
              (l/with-transaction-kv [writer lmdb]
                (let [tx-store (s/transfer store writer)]
                  (pour tx-store datoms)
                  (vreset! loaded tx-store))))
            (finally
              (when-not nosync? (set-env-flags lmdb #{:nosync} false))))
          (sync lmdb)
          (s/transfer @loaded lmdb))))))

(defn- quick-fill
  [store datoms]
  (if (remote-store? store)
    ;; Each bounded batch uses the server's regular write path, preserving
    ;; its WAL/HA settings. Large batches use the copy-in wire protocol.
    (do (pour store datoms) store)
    (quick-fill-local store datoms)))

(defn init-db
  ([datoms] (init-db datoms nil nil nil))
  ([datoms dir] (init-db datoms dir nil nil))
  ([datoms dir schema] (init-db datoms dir schema nil))
  ([datoms dir schema opts]
   {:pre [(or (nil? schema) (map? schema))]}
   (vld/validate-schema-update schema)
   (let [[_ runtime-opts] (split-runtime-opts opts)
         store            (open-store dir schema opts)]
     (try
       (let [loaded-store
             (if (remote-store? store)
               (do
                 (locking (l/write-txn store)
                   ;; Map growth discards earlier batches in the native
                   ;; transaction. Abort and replay the entire import.
                   (u/repeat-try-catch c/+in-tx-overflow-times+ l/resized?
                     (let [tx-store (i/open-transact store)]
                       (try
                         (pour tx-store datoms)
                         (i/close-transact store)
                         (catch Throwable failure
                           (try
                             (i/abort-transact store)
                             (catch Throwable abort-error
                               (when-not (identical? failure abort-error)
                                 (.addSuppressed failure abort-error))))
                           (throw failure))))))
                 store)
               (quick-fill store datoms))]
         (cond-> (new-db loaded-store)
           (some? runtime-opts) (with-runtime-opts runtime-opts)))
       (catch Throwable failure
         ;; Only the returned DB can own this opened store. Release it after
         ;; transaction retries have ended, including lazy input failures.
         (try
           (i/close store)
           (catch Throwable close-error
             (when-not (identical? failure close-error)
               (.addSuppressed failure close-error))))
         (throw failure))))))

(defn fill-db
  [db datoms]
  (let [store (.-store ^DB db)]
    (try
      (let [loaded-store (quick-fill store datoms)
            target (last-modified loaded-store)]
        (when (instance? Store loaded-store)
          (s/mark-state-current! ^Store loaded-store target))
        (refresh-cache loaded-store target)
        (carry-runtime-opts (new-db loaded-store nil db) db))
      (catch Throwable failure
        (when (remote-store? store)
          ;; Earlier remote batches can commit before a later batch fails.
          ;; Clear without RPCs so cleanup preserves the original failure,
          ;; and prevent pre-import readers from repopulating this cache.
          (let [cache-key (dir store)]
            (when-let [^LRUCache cache (.get ^ConcurrentHashMap caches cache-key)]
              (locking cache
                (.beginInvalidation cache (.target cache))
                (.clear cache)))
            (.remove ^ConcurrentHashMap remote-cache-check-ms cache-key)))
        (throw failure)))))

;; ----------------------------------------------------------------------------

(declare entid-strict entid-some ref?)

(defn- resolve-datom
  [db e a v default-e default-v]
  (when a (vld/validate-attr a (list 'resolve-datom 'db e a v default-e default-v)))
  (let [v? (some? v)]
    (datom
      (or (entid-some db e) default-e)  ;; e
      a                                 ;; a
      (if (and v? (ref? db a))          ;; v
        (entid-strict db v)
        (if v? v default-v)))))

(defn- components->pattern
  [db index c0 c1 c2 default-e default-v]
  (when (identical? index :ave)
    (vld/validate-indexed-attr (-schema db) c0))
  (case index
    :eav (resolve-datom db c0 c1 c2 default-e default-v)
    :ave (resolve-datom db c2 c0 c1 default-e default-v)))

(defn- components->end-datom
  [_ index c0 c1 _ default-e default-v]
  (datom default-e
         (case index
           :eav c1
           :ave c0)
         default-v))

;; ----------------------------------------------------------------------------

(defn multival?
  ^Boolean [db attr]
  (txcommon/multival? db attr))

(defn multi-value?
  ^Boolean [db attr value]
  (txcommon/multi-value? db attr value))

(defn ref?
  ^Boolean [db attr]
  (txcommon/ref? db attr))

(defn component?
  ^Boolean [db attr]
  (txcommon/component? db attr))

(defn tuple-attr?
  ^Boolean [db attr]
  (txcommon/tuple-attr? db attr))

(defn tuple-type?
  ^Boolean [db attr]
  (txcommon/tuple-type? db attr))

(defn tuple-types?
  ^Boolean [db attr]
  (txcommon/tuple-types? db attr))

(defn tuple-source?
  ^Boolean [db attr]
  (txcommon/tuple-source? db attr))

(defn entid
  [db eid]
  (txcommon/entid db eid))

(defn entid-strict
  [db eid]
  (txcommon/entid-strict db eid))

(defn entid-some
  [db eid]
  (txcommon/entid-some db eid))

(defn reverse-ref?
  ^Boolean [attr]
  (txcommon/reverse-ref? attr))

(defn reverse-ref
  [attr]
  (txcommon/reverse-ref attr))

;;;;;;;;;; Transacting

(declare commit-prepared-tx-data!)

(defn- auto-tempid? ^Boolean [x] (txprep/auto-tempid? x))

(defn- tempid?
  ^Boolean [x]
  (txprep/tempid? x))

(def ^:private tx-ensures-key
  ::txexec/ensures)

(defn- report-ensures
  [report]
  (when report
    (seq (get report tx-ensures-key))))

(defn- ensure-udf-reference?
  [pred]
  (or (udf/descriptor? pred) (keyword? pred)))

(defn- ensure-udf-context
  [db]
  {:db        db
   :kind      :predicate
   :embedded? true
   :store     (:store db)})

(defn- resolve-ensure-udf
  [db pred]
  (let [registry   (udf-registry db)
        descriptor (or (txprep/installed-udf-descriptor db :predicate pred)
                       (udf/descriptor-or-registered registry :predicate pred))]
    (udf/materialize registry (ensure-udf-context db) descriptor)))

(defn- resolve-ensure-pred
  [db pred entity]
  (vld/validate-ensure-predicate pred entity)
  (if (ensure-udf-reference? pred)
    (resolve-ensure-udf db pred)
    (let [callable (if (symbol? pred)
                     (requiring-resolve pred)
                     pred)
          callable (if (var? callable) @callable callable)]
      (if (ifn? callable)
        callable
        (vld/validate-ensure-predicate callable entity)))))

(defn- resolve-ensure-arg
  [tempids arg entity]
  (cond
    (identical? :db/current-tx arg)
    (get tempids :db/current-tx)

    (tempid? arg)
    (if (contains? tempids arg)
      (get tempids arg)
      (vld/validate-ensure-tempid arg entity))

    :else
    arg))

(defn- run-report-ensures!
  [db report]
  (doseq [[pred args entity] (report-ensures report)]
    (let [tempids       (:tempids report)
          resolved-args (mapv #(resolve-ensure-arg tempids % entity) args)
          callable      (resolve-ensure-pred db pred entity)
          result        (apply callable db resolved-args)]
      (vld/validate-ensure-result result pred resolved-args entity))))

(defn installed-udf-descriptor
  ([db target]
   (installed-udf-descriptor db nil target))
  ([db allowed target]
   (txprep/installed-udf-descriptor db allowed target)))

(defn- mark-simulated-tx-cache!
  [report]
  (let [db      (:db-after report)
        noindex (:db/noindex (rschema (:store db)))]
    (doseq [^Datom datom (:tx-data report)]
      (.add ^TreeSortedSet (:eavt db) datom)
      (when-not (contains? noindex (.-a datom))
        (.add ^TreeSortedSet (:avet db) datom)))
    report))

(defn- committed-client-op-response
  [report last-modified-ms]
  (let [tx-meta (:tx-meta report)
        client-op-id (:client-op/id tx-meta)
        request-type (:client-op/request-type tx-meta)
        request-hash (:client-op/hash tx-meta)
        response-kind (:client-op/response-kind tx-meta)
        db-after (:db-after report)]
    (when (and client-op-id request-type request-hash response-kind)
      (let [response
            ;; Persist replay payloads without Datom objects so reopening a DB
            ;; doesn't depend on the custom Datom Nippy reader being loaded.
            (cond-> (if (= response-kind cop/tx-data-ack-response-kind)
                      {:result :transacted}
                      {:tx-data (mapv (fn [datom]
                                       [(d/datom-e datom)
                                        (d/datom-a datom)
                                        (d/datom-v datom)
                                        (d/datom-tx datom)
                                        (d/datom-added datom)])
                                     (:tx-data report))
                       :tempids (assoc (:tempids report)
                                       :max-eid
                                       (:max-eid db-after))})
              (and (not= response-kind cop/tx-data-ack-response-kind)
                   (:new-attributes report))
              (assoc :new-attributes (:new-attributes report))

              (#{cop/tx-data+db-info-response-kind
                 cop/tx-data-ack-response-kind} response-kind)
              (assoc :db-info {:max-eid         (:max-eid db-after)
                               :max-tx          (:max-tx db-after)
                               :last-modified   last-modified-ms
                               :schema-revision (metadata/schema-revision
                                                  (.-lmdb ^Store (:store db-after)))}))]
        (cop/committed-record-tx
          client-op-id
          (cop/committed-record request-type
                                request-hash
                                response-kind
                                response))))))

(defn ^:no-doc commit-prepared-tx-data!
  "Persist already prepared tx-data to the given DB store. independent? may
   only be supplied for resolved scalar transitions with distinct E/A keys."
  ([^DB db tx-data]
   (commit-prepared-tx-data! db tx-data nil))
  ([^DB db tx-data report]
   (commit-prepared-tx-data! db tx-data report false))
  ([^DB db tx-data report independent?]
   (let [store   (.-store db)
         ensures (report-ensures report)]
     (if (and ensures
              (instance? Store store)
              (not (l/writing? (.-lmdb ^Store store))))
       (let [kv              (.-lmdb ^Store store)
             prepared-store  (volatile! nil)
             prepared-db     (volatile! nil)]
         (l/with-transaction-kv [kv1 kv]
           (let [store1 ^Store (s/transfer ^Store store kv1)
                 db1    ^DB    (transfer db store1)
                 db2    ^DB    (commit-prepared-tx-data!
                                 db1 tx-data report independent?)]
             (vreset! prepared-store (.-store db2))
             (vreset! prepared-db db2)))
         (let [new-store ^Store (s/transfer ^Store @prepared-store kv)]
           (carry-runtime-opts
             (transfer ^DB @prepared-db new-store)
             db)))
       (if (instance? Store store)
         (let [tx-meta (:tx-meta report)
               staged-ms (when (and txcommon/*batch-prepare* (not ensures)
                                    (not (:client-op/id tx-meta)))
                           (s/stage-group-datoms! store tx-data independent?))
               modified-ms
               (long
                 (or staged-ms
                     ;; Staging already reserves grouped metadata. Only check
                     ;; for an empty report before the full storage planner.
                     (when (and (empty? tx-data) (not ensures)
                                (not s/*enforce-blind-unique-inserts?*)
                                (not (:client-op/id tx-meta)))
                       (s/commit-empty-tx! store))
                     (let [{:keys [embedding-plan embedding-skips]}
                           (s/prepare-secondary-index-plan store tx-data)
                           ;; Preparation may create attributes and advance the
                           ;; schema version. Storage chooses the final floor.
                           commit-ms (max (long (s/observed-state-sync-ms store))
                                          (System/currentTimeMillis))
                           client-op? (and (:client-op/id tx-meta)
                                           (:client-op/request-type tx-meta)
                                           (:client-op/hash tx-meta)
                                           (:client-op/response-kind tx-meta))
                           commit-opts
                           (cond-> {:last-modified-ms commit-ms
                                    ;; Raw bulk loads can add an existing fact
                                    ;; before deleting it. Only resolved reports
                                    ;; prove the initial index membership here.
                                    :coalesce-fulltext? (some? report)
                                    :return-modified-ms? true}
                             embedding-skips
                             (assoc :embedding-skips embedding-skips)

                             client-op?
                             (assoc :extra-kv-txs-fn
                                    (fn [modified-ms]
                                      [(committed-client-op-response
                                         report modified-ms)])))]
                       (s/load-datoms-with-plan! store tx-data embedding-plan
                                                commit-opts))))]
           (when ensures
             (txcommon/flush-batch-prepare!)
             (run-report-ensures! (transfer (:db-after report) store)
                                  report))
           (s/mark-state-current! ^Store store modified-ms)
           (if-let [^WriteGroup group
                    (s/current-write-group (.-lmdb ^Store store))]
             (let [n (count tx-data)]
               (when (pos? n)
                 (let [^longs metadata (.-metadata group)]
                   (aset metadata 2 (+ (aget metadata 2) (long n))))
                 (when (or (.-retain-cache-datoms? group) txcommon/*batch-prepare*)
                   ;; Reports own persistent vectors. Snapshot external mutable
                   ;; collections before retaining their pending history.
                   (.add ^FastList (.-datom-batches group)
                         (if (vector? tx-data) tx-data (vec tx-data))))))
             (invalidate-cache store tx-data modified-ms))
           db)
         (do
           (load-datoms store tx-data)
           (when ensures
             (run-report-ensures! (transfer (:db-after report) store)
                                  report))
           (invalidate-cache store tx-data (last-modified store))
           db))))))

(defn- remote-tx-result
  [res]
  (if (map? res)
    (let [{:keys [tx-data tempids new-attributes]} res]
      [tx-data (dissoc tempids :max-eid) (tempids :max-eid) new-attributes])
    (let [[tx-data tempids] (split-with datom? res)
          tempids           (into {} tempids)]
      [tx-data (dissoc tempids :max-eid) (:max-eid tempids) nil])))

(defn- resolved-remote-db-info
  [store db db-info max-eid simulated?]
  {:max-eid       (or max-eid
                      (:max-eid db-info)
                      (:max-eid db)
                      (when-not simulated? (init-max-eid store)))
   :max-tx        (or (:max-tx db-info)
                      (when-not simulated? (max-tx store))
                      (:max-tx db))
   :last-modified (or (:last-modified db-info)
                      (when-not simulated? (last-modified store))
                      0)})

(defn- expand-transactable-entity
  [entity]
  (txprep/expand-transactable-entity entity))

;; Identity probes in the blind path share the LMDB write transaction. Large
;; batches enable them from two entities onward so ingestion avoids the old
;; discontinuity. A single entity is routed to the identity-upsert
;; specialization when it names a unique-identity attribute, on both the WAL
;; and non-WAL direct paths, so simple upserts avoid the ordinary resolver.
;; Value-unique inserts reject duplicates instead of upserting, so they can use
;; the blind path at any batch size.
(def ^:private ^:const ^long blind-unique-write-threshold 2)

(defn- blind-write-multiple-values?
  [many? unique-identity-attrs value]
  (and many?
       (coll? value)
       (not (map? value))
       (not (and (= (count value) 2)
                 (contains? unique-identity-attrs (first value))))))

(defn- append-blind-value!
  ^Boolean [^FastList attrs ^FastList unique-avs
            seen-avs set-capacity fuse-unique-inserts?
            store-opts ^objects info attr entity value]
  (if (or (map? value)
          (datom? value)
          (and (coll? value) (not (bytes? value))))
    false
    (let [props   (aget info 0)
          value'  (if (txcompile/blind-value-ready? info value)
                    value
                    (do
                      (vld/validate-val value entity)
                      (if (aget info 5)
                        (prepare/correct-scalar-value
                          (:validate-data? store-opts) (aget info 4) value)
                        (prepare/correct-value-with-props
                          store-opts props attr value))))]
      (txcompile/append-blind-canonical-value!
        attrs unique-avs seen-avs set-capacity fuse-unique-inserts? info attr value'))))

(defn- prepare-blind-write-entity
  [store-opts unique-identity-attrs ^FastList unique-avs seen-avs set-capacity
   has-many? fuse-unique-inserts? attr-infos entity]
  (let [old-eid   (:db/id entity)
        ;; An implicit entity does not need a transaction-scoped tempid: the
        ;; blind stamper assigns its final EID directly and never exposes an
        ;; implicit mapping in the report. Retain explicit tempids so repeated
        ;; user IDs still fall back to the resolver and merge correctly.
        entity-id old-eid]
    (when (if (nil? old-eid)
            (not (contains? entity :db/id))
            (tempid? old-eid))
      (let [^FastList attrs (FastList. (int (* 2 (count entity))))
            supported?
            (reduce-kv
              (fn [_ attr raw-value]
                (if (identical? attr :db/id)
                  true
                  ;; Upfront preflight supplies a map; bulk preparation uses
                  ;; a value-aware lookup without adding per-entity state.
                  (if-some [^objects info (if (instance? java.util.HashMap attr-infos)
                                           (.get ^java.util.HashMap attr-infos attr)
                                           (attr-infos attr raw-value))]
                    (let [props (aget info 0)
                          many? (identical? :db.cardinality/many (:db/cardinality props))
                          _ (when (and (not (aget info 2)) fuse-unique-inserts?)
                              (vreset! fuse-unique-inserts? false))
                          supported-values?
                          (if (blind-write-multiple-values?
                                many? unique-identity-attrs raw-value)
                            (let [vt (aget info 4)
                                  ^java.util.Set seen-values (txcompile/blind-value-set vt)
                                  values (seq raw-value)]
                              (when values (vreset! has-many? true))
                              (loop [values values]
                                ;; A nil member must reach value validation.
                                (if values
                                  (if (append-blind-value!
                                        attrs unique-avs seen-avs set-capacity
                                        fuse-unique-inserts? store-opts info attr entity
                                        (first values))
                                    (if (.add seen-values (.get attrs (dec (.size attrs))))
                                      (recur (next values))
                                      (if (:db/unique props)
                                        false
                                        (do
                                          ;; Keep the first normalized occurrence.
                                          ;; Unique AV collection remains untouched.
                                          (.remove attrs (int (dec (.size attrs))))
                                          (.remove attrs (int (dec (.size attrs))))
                                          (recur (next values)))))
                                    false)
                                  true)))
                            (do
                              (when many? (vreset! has-many? true))
                              (append-blind-value!
                                attrs unique-avs seen-avs set-capacity
                                fuse-unique-inserts? store-opts info attr entity
                                raw-value)))]
                      (if-not supported-values?
                        (reduced false)
                        true))
                    (reduced false))))
              true
              entity)]
        (when supported?
          (BlindEntity. entity-id attrs))))))

(defn- blind-attributes-supported?
  "Preflight attributes and retain unique/many flags without correcting values."
  [entities check-attr]
  (reduce (fn [kinds entity]
            (if-some [kinds (reduce-kv check-attr kinds entity)]
              kinds
              (reduced nil)))
          0 entities))

(defn- local-tx-form-kinds
  "Shape-only candidates: scalar 1, blind 2, mixed 4, patch 8; tempids use 16."
  ^long [entity]
  (cond
    (map? entity)
    (let [absent-id? (not (contains? entity :db/id))
          eid (:db/id entity)
          tempid? (and (not absent-id?) (tempid? eid))]
      (cond
        (or absent-id? tempid?)
        ;; Mixed preparation validates map contents and IDs on the writer.
        (bit-or 6 (if tempid? 16 0))

        (and (nil? (meta entity)) (> (count entity) 1)
             (txplan/resolved-positive-ref? eid)) 1

        :else 0))

    (and (sequential? entity) (= 4 (count entity)))
    (let [op (nth entity 0)]
      (cond
        (identical? op :db.fn/patchIdoc) 8
        (and (identical? op :db/add) (nil? (meta entity)))
        (if (vector? entity) 5 1)
        :else 0))

    :else 0))

(defn ^:no-doc classify-local-tx
  "Choose a local specialization without schema reads, correction or entity
   resolution. Mixed preparation checks the suffix once a mixture is proven;
   a general decision needs no further specialization gates."
  [tx-data]
  (if (and (not (txengine/interpreter?)) (seqable? tx-data))
    (let [kinds (long
                  (if (and (sequential? tx-data) (counted? tx-data) (= 1 (count tx-data)))
                    (local-tx-form-kinds (nth tx-data 0))
                    (reduce (fn [previous entity]
                            (let [previous (long previous)
                                  current (local-tx-form-kinds entity)]
                              (cond
                                (= current 8) (if (= previous 15) 8 (reduced 0))
                                ;; A homogeneous prefix leaves the proof intact.
                                (= previous current) previous
                                :else
                                (let [candidates (bit-and previous current 15)]
                                  (cond
                                    (zero? candidates) (reduced 0)
                                    ;; The mixed planner validates the suffix
                                    ;; before publishing any resolver state.
                                    (= candidates 4) (reduced 4)
                                    :else
                                    (bit-or candidates
                                            (bit-and (bit-or previous current) 16)))))))
                            15 tx-data)))]
      (case (bit-and kinds 15)
        (1 5) :scalar
        (2 6) (if (zero? (bit-and kinds 16)) :blind :blind-tempids)
        4 :mixed
        8 :patch-idoc
        :general))
    :general))

(defn- prepare-blind-local-tx*
  ([^DB db initial-es]
   (prepare-blind-local-tx* db initial-es false true))
  ([^DB db initial-es allow-small-unique?]
   (prepare-blind-local-tx* db initial-es allow-small-unique? true))
  ([^DB db initial-es allow-small-unique? check-unique-avs?]
   (prepare-blind-local-tx* db initial-es allow-small-unique? check-unique-avs? nil))
  ([^DB db initial-es allow-small-unique? check-unique-avs? entity-kinds]
   ;; Classified callers retain the implicit-ID/tempid proof from their one
   ;; shape pass. Other callers still establish that proof here.
   ;; Reject mixed requests and ineligible IDs before allocating preparation
   ;; state or correcting values that the general or mixed preparer repeats.
   ;; Only an absent :db/id is implicit; an explicit nil needs general validation.
   ;; Zero is a valid proof of implicit IDs; one marks an explicit tempid.
   (when-let [entity-kinds
               (if (some? entity-kinds)
                 entity-kinds
                 (reduce (fn [kinds entity]
                         (if (map? entity)
                           (let [e (:db/id entity)]
                             (cond
                               (nil? e) (if (contains? entity :db/id)
                                          (reduced false) kinds)
                               (tempid? e) 1
                               :else (reduced false)))
                           (reduced false)))
                       0 initial-es))]
     (let [store             (.-store db)
           store-schema      (schema store)
           store-opts        (opts store)
           store-rschema     (rschema store)
           tuple-source-attrs (:db/attrTuples store-rschema)
           unique-identity-attrs (:db.unique/identity store-rschema)
           auto-entity-time? (boolean (:auto-entity-time? store-opts))
           allow-unique?     (or allow-small-unique?
                                 (>= (long (bounded-count
                                            blind-unique-write-threshold
                                            initial-es))
                                     blind-unique-write-threshold))
           initial-count     (int
                               (if (instance? java.util.Collection initial-es)
                                 (.size ^java.util.Collection initial-es)
                                 0))
           ;; Amortize the lazy preflight in bulk requests. Small requests
           ;; retain their upfront attribute check and direct metadata lookup.
           incremental?      (if (instance? java.util.Collection initial-es)
                               (>= initial-count 32)
                               (>= (long (bounded-count 32 initial-es)) 32))
           attr-infos        (java.util.HashMap.)
           check-attr        (fn [kinds attr value]
                               (if (identical? attr :db/id)
                                 kinds
                                 (let [^objects info
                                       (txcompile/blind-attr-info!
                                         attr-infos store-schema tuple-source-attrs
                                         allow-unique? attr)]
                                   (if (and (aget info 1)
                                            (or (not (identical? (aget info 4) :db.type/ref))
                                                (if (blind-write-multiple-values?
                                                      (identical?
                                                        :db.cardinality/many
                                                        (:db/cardinality (aget info 0)))
                                                      unique-identity-attrs value)
                                                  (every? txplan/resolved-positive-ref? value)
                                                  (txplan/resolved-positive-ref? value))))
                                     (bit-or (long kinds) (long (aget info 1)))
                                     (reduced nil)))))
           attribute-kinds   (when-not incremental?
                               (blind-attributes-supported? initial-es check-attr))]
       (when (or incremental? (some? attribute-kinds))
         (let [;; Entity preparation receives an Object; box once per batch.
               set-capacity      (Integer/valueOf
                                   (int (max 1 (inc (quot (* 4 (long initial-count))
                                                         3)))))
               ;; Small requests already proved which workspace is needed.
               ;; Incremental bulk preflight retains its existing shared state.
               ^FastList unique-avs (when (or incremental?
                                             (not (zero? (bit-and (long attribute-kinds) 1))))
                                      (FastList.))
               seen-avs          (when (and unique-avs check-unique-avs?)
                                   (volatile! nil))
               seen-tempids      (when (and (pos? (long entity-kinds)) (not (== initial-count 1)))
                                   (java.util.HashSet. (int set-capacity)))
               has-many?         (when (or incremental?
                                          (not (zero? (bit-and (long attribute-kinds) 2))))
                                   (volatile! false))
               fuse-unique-inserts? (when unique-avs (volatile! true))
               ;; Canonical scalar values are inert. Check and prepare those
               ;; together; complete the attribute check before any coercion
               ;; or collection handling that could invoke user code or fail.
               preflight         (when incremental?
                                   (delay (blind-attributes-supported?
                                            initial-es check-attr)))
               attr-lookup       (when incremental?
                                   (fn [attr value]
                                     ;; A failed preflight stops preparation.
                                     ;; Once it succeeds, use the populated map.
                                     (if (realized? preflight)
                                       (.get attr-infos attr)
                                       (let [^objects info
                                             (txcompile/blind-attr-info!
                                               attr-infos store-schema
                                               tuple-source-attrs allow-unique? attr)]
                                         (when (and (aget info 1)
                                                    (or (txcompile/blind-value-ready? info value)
                                                        @preflight))
                                           info)))))
               entity-attr-infos (or attr-lookup attr-infos)
               entities
               (reduce
                 (fn [^FastList acc entity]
                   (if-let [^BlindEntity prepared
                            (prepare-blind-write-entity
                              store-opts unique-identity-attrs unique-avs
                              seen-avs set-capacity has-many?
                              fuse-unique-inserts? entity-attr-infos entity)]
                     (if (and (.-tempid prepared) seen-tempids
                              (not (.add seen-tempids (.-tempid prepared))))
                       (reduced nil)
                       (if (or (not (.isEmpty ^FastList (.-attrs prepared)))
                               auto-entity-time?)
                         (doto acc (.add prepared))
                         acc))
                     (reduced nil)))
                 (FastList. initial-count)
                 initial-es)
               has-unique?       (boolean (and unique-avs (not (.isEmpty unique-avs))))
               fuse-unique-inserts? (and has-unique? @fuse-unique-inserts?)
               identity-upsert-av
               (when (and has-unique? entities
                          (not auto-entity-time?)
                          (= 1 (.size ^FastList entities)))
                 (txcompile/blind-identity-upsert-av
                   store-schema (.get ^FastList entities 0)))]
           (when (and entities (not (.isEmpty ^FastList entities))
                      ;; Fused writes enforce uniqueness natively. Other writes
                      ;; deduplicate only the AVs collected during preparation.
                      (or check-unique-avs? fuse-unique-inserts? (not has-unique?)
                          (= (.size unique-avs)
                             (.size (java.util.HashSet. ^java.util.Collection unique-avs)))))
             (->PreparedBlindTx store-schema
                                store-opts
                                auto-entity-time?
                                entities
                                unique-avs
                                has-unique?
                                (boolean (and has-many? @has-many?))
                                fuse-unique-inserts?
                                identity-upsert-av
                                (when identity-upsert-av
                                  (let [^BlindEntity entity
                                        (.get ^FastList entities 0)
                                        ^FastList attrs (.-attrs entity)
                                        identity-attr (first identity-upsert-av)]
                                    (txcompile/prepare-scalar-entity-read
                                      store-schema
                                      (for [index (range 0 (.size attrs) 2)
                                            :let [attr (.get attrs index)]
                                            :when (not= attr identity-attr)]
                                        attr))))))))))))

(defn ^:no-doc prepare-blind-local-tx
  "Select compiled canonical or safely coercible new maps when opted in; other
  shapes retain the blind specialization. Neither route binds persistent IDs here."
  ([db entities] (prepare-blind-local-tx db entities false true))
  ([db entities allow-small-unique?]
   (prepare-blind-local-tx db entities allow-small-unique? true))
  ([db entities allow-small-unique? check-unique-avs?]
   (prepare-blind-local-tx db entities allow-small-unique? check-unique-avs? nil))
  ([db entities allow-small-unique? check-unique-avs? entity-kinds]
   (or (when (and (txengine/compiler?) (instance? Store (:store db)))
         (when-let [plan (txcompile/compile-tx
                           (prepare/make-prepare-ctx db) entities
                           {:request-shape :blind :allow-small-unique? allow-small-unique?
                            :check-unique-avs? check-unique-avs?})]
           (when *tx-compiler-observer* (*tx-compiler-observer* :prepared plan))
           plan))
       (prepare-blind-local-tx* db entities allow-small-unique? check-unique-avs? entity-kinds))))

(defn- blind-tx-entities [prepared]
  (if (instance? TxPlan prepared) (:entries prepared) (:entities prepared)))

(defn ^:no-doc blind-local-tx-unique-values-absent?
  "Check a prepared batch's unique values against the current store snapshot.
   The caller invokes this after opening its LMDB write transaction, so all
   probes share that transaction and no concurrent insert can race the check."
  [^DB db prepared]
  (let [^FastList avs (:unique-avs prepared)]
    (or (nil? avs)
        (loop [i 0]
          (or (>= i (.size avs))
              (let [[a v] (.get avs i)]
                (and (nil? (txcommon/av-first-e db a v))
                     (recur (unchecked-inc i)))))))))

(defn ^:no-doc blind-local-tx-valid?
  [^DB db prepared]
  (let [store (.-store db)]
    (if (instance? TxPlan prepared)
      (and (identical? :blind (:strategy prepared))
           (identical? (:schema (:ctx prepared)) (schema store))
           (identical? (:opts (:ctx prepared)) (opts store)))
      (and (identical? (:schema-epoch prepared) (schema store))
           (identical? (:opts-epoch prepared) (opts store))))))

(defn- append-blind-identity-upsert!
  [^DB db ^BlindEntity entity identity-attr eid tx-id layout tx-data]
  (let [^FastList attrs (.-attrs entity)
        old-values (when layout (txexec/read-scalar-entity (.-store db) eid layout))]
    (loop [i 0 tx-data tx-data]
      (if (< i (.size attrs))
        (let [attr  (.get attrs i)
              value (.get attrs (unchecked-inc i))]
          (if (= attr identity-attr)
            (recur (+ i 2) tx-data)
            (let [old-value (if layout
                              (get old-values attr)
                              (txcommon/preparation-first-v db eid attr))]
              (cond
                (nil? old-value)
                (recur (+ i 2)
                       (conj! tx-data
                              (d/datom eid attr value tx-id)))

                (d/value-equal? (idx/logical-value old-value) value)
                (recur (+ i 2) tx-data)

                :else
                (recur (+ i 2)
                       (-> tx-data
                           (conj! (idx/retraction eid attr old-value tx-id))
                           (conj! (d/datom eid attr value tx-id))))))))
        tx-data))))

(defn- stamp-existing-blind-local-identity-upsert
  [^DB db prepared tx-meta identity-attr eid]
  (let [^FastList entities (blind-tx-entities prepared)
        ^BlindEntity entity (.get entities 0)
        tx-id           (inc (long (:max-tx db)))
        layout          (when-not txcommon/*batch-prepare* (:identity-upsert-read prepared))
        tx-data         (persistent! (append-blind-identity-upsert!
                                       db entity identity-attr eid tx-id layout
                                       (transient [])))
        tempid          (.-tempid entity)
        tempids         (cond-> {:db/current-tx tx-id}
                          (and tempid (not (auto-tempid? tempid)))
                          (assoc tempid eid))
        db-after        (cond-> (assoc db :max-tx tx-id)
                          (> (long eid) (long (:max-eid db)))
                          (assoc :max-eid eid))]
    (->TxReport db db-after tx-data tempids tx-meta)))

(defn ^:no-doc stamp-blind-local-upsert-tx
  "Reuse a collided blind batch's normalized scalar values. Independent maps
   with one identity each can resolve insert versus upsert under the same
   writer. Repeated owners and ambiguous identities retain general resolution."
  [^DB db prepared tx-meta]
  (let [^FastList entities (blind-tx-entities prepared)
        compiled? (instance? TxPlan prepared)
        ^FastList identity-avs (when (and compiled? (:independent-identity-upserts? prepared))
                                (:unique-avs prepared))]
    (when (and (not (:auto-entity-time? prepared))
               (not (:has-many? prepared))
               (:has-unique? prepared)
               (= (.size entities) (.size ^FastList (:unique-avs prepared)))
               (or (not compiled?) identity-avs)
               (blind-local-tx-valid? db prepared))
      (let [seen-avs (java.util.HashSet.)
            seen-eids (java.util.HashSet.)
            ^FastList resolutions (FastList. (.size entities))
            max-eid
            (loop [i 0 max-eid (long (:max-eid db))]
              (if (< i (.size entities))
                (when-let [[a v :as av]
                           (if identity-avs
                             (.get identity-avs i)
                             (txcompile/blind-identity-upsert-av (:schema-epoch prepared)
                               (.get entities i)))]
                  (when (.add seen-avs av)
                    (let [stored-eid (txcommon/av-first-e db a v)
                          eid (if stored-eid (long stored-eid) (inc max-eid))]
                      (when (.add seen-eids eid)
                        (.add resolutions [a eid (some? stored-eid)])
                        (recur (inc i) (if stored-eid max-eid eid))))))
                max-eid))]
        (when max-eid
          (let [tx-id (inc (long (:max-tx db)))
                [tx-data tempids]
                (loop [i 0 tx-data (transient []) tempids (transient {})]
                  (if (< i (.size entities))
                    (let [^BlindEntity entity (.get entities i)
                          [identity-attr eid stored?] (.get resolutions i)
                          ^FastList attrs (.-attrs entity)
                          tx-data (if stored?
                                    (append-blind-identity-upsert!
                                      db entity identity-attr eid tx-id nil tx-data)
                                    (loop [j 0 tx-data tx-data]
                                      (if (< j (.size attrs))
                                        (recur (+ j 2)
                                               (conj! tx-data
                                                      (d/datom eid (.get attrs j)
                                                               (.get attrs (inc j)) tx-id)))
                                        tx-data)))
                          tempid (.-tempid entity)]
                      (recur (inc i) tx-data
                             (if (and tempid (not (auto-tempid? tempid)))
                               (assoc! tempids tempid eid) tempids)))
                    [(persistent! tx-data)
                     (assoc! tempids :db/current-tx tx-id)]))]
            (when (and (instance? TxPlan prepared) *tx-compiler-observer*)
              (*tx-compiler-observer* :execute-blind-upsert prepared))
            (->TxReport db (assoc db :max-eid max-eid :max-tx tx-id)
                        tx-data (persistent! tempids) tx-meta)))))))

(defn ^:no-doc stamp-blind-local-tx
  "Bind legacy or compiled blind operands through the shared emission loop."
  [db prepared tx-meta]
  (when (or (not (instance? TxPlan prepared)) (blind-local-tx-valid? db prepared))
    (let [report (txexec/execute-blind-inserts
                   (->TxReport db db [] {} tx-meta) (blind-tx-entities prepared)
                   (:auto-entity-time? prepared))]
      (when (and (instance? TxPlan prepared) *tx-compiler-observer*)
        (*tx-compiler-observer* :execute-blind prepared))
      report)))

(defn ^:no-doc stamp-blind-local-identity-tx
  "Stamp one eligible simple identity transaction, resolving insert versus
   upsert inside the caller's LMDB write transaction. Returns
   [report upsert?], or nil when the prepared shape is not eligible."
  [^DB db prepared tx-meta]
  (when (or (not (instance? TxPlan prepared)) (blind-local-tx-valid? db prepared))
    (when-let [[identity-attr identity-value] (:identity-upsert-av prepared)]
      (if-some [eid (txcommon/av-first-e db identity-attr identity-value)]
        (let [report (stamp-existing-blind-local-identity-upsert
                       db prepared tx-meta identity-attr eid)]
          (when (and (instance? TxPlan prepared) *tx-compiler-observer*)
            (*tx-compiler-observer* :execute-blind-upsert prepared))
          [report true])
        [(stamp-blind-local-tx db prepared tx-meta) false]))))

(defn ^:no-doc prepare-local-patch-idoc-tx
  "Recognize one simple non-unique cardinality-one patchIdoc request without
   consulting mutable entity state. The caller must stamp it against the
   current LMDB write snapshot before committing it. Returns nil for shapes
   that retain the general transaction path."
  [^DB db initial-es]
  (let [store      (.-store db)
        store-opts (opts store)
        forms      (seq initial-es)
        entity     (when (and forms (nil? (next forms))) (first forms))]
    (when (and (sequential? entity)
               (= 4 (count entity))
               (not (:auto-entity-time? store-opts)))
      (let [[op e attr patch-ops] entity
            props                 ((schema store) attr)]
        (when (and (identical? op :db.fn/patchIdoc)
                   (integer? e)
                   (<= 1 e Long/MAX_VALUE)
                   (keyword? attr)
                   props
                   (identical? (:db/valueType props) :db.type/idoc)
                   (not (identical? (:db/cardinality props)
                                    :db.cardinality/many))
                   (nil? (:db/unique props))
                   (not (:db.attr/preds props))
                   (or (nil? patch-ops) (sequential? patch-ops)))
          (->PreparedLocalPatchIdocTx
            (schema store) store-opts entity (long e) attr patch-ops))))))

(defn ^:no-doc local-patch-idoc-tx-valid?
  [^DB db ^PreparedLocalPatchIdocTx prepared]
  (let [store (.-store db)]
    (and (identical? (:schema-epoch prepared) (schema store))
         (identical? (:opts-epoch prepared) (opts store)))))

(defn- stamp-local-patch-idoc-doc
  [^DB db ^PreparedLocalPatchIdocTx prepared tx-meta old-doc]
  (let [before old-doc
        old-doc (idx/logical-value before)
        {:keys [entity e attr ops]} prepared
        store       (.-store db)
        props       ((schema store) attr)
        tx-id       (inc (long (:max-tx db)))
        base-report (->TxReport db
                                (assoc db :max-tx tx-id)
                                []
                                {:db/current-tx tx-id}
                                tx-meta)]
    (if (empty? ops)
      {:report base-report :doc old-doc}
      (let [{raw-doc :doc paths :paths}
            (idoc/apply-patch (or old-doc {}) ops)
            _       (vld/validate-val raw-doc entity)
            new-doc (prepare/correct-value-with-props
                      (opts store) props attr raw-doc)
            add     (with-meta (datom e attr new-doc tx-id)
                      {:idoc/patch {:paths paths}})
            db-after (cond-> (:db-after base-report)
                       (nil? old-doc)
                       (assoc :max-eid
                              (max (long (:max-eid db)) (long e))))
            tx-data (cond
                      (nil? old-doc) [add]
                      (= old-doc new-doc) []
                      :else [(idx/retraction e attr before tx-id) add])]
        {:report (assoc base-report
                        :db-after db-after
                        :tx-data tx-data)
         :doc new-doc}))))

(defn ^:no-doc stamp-local-patch-idoc-tx
  "Stamp a prepared patchIdoc request against the current store snapshot.
   Returns {:report TxReport :doc resulting-doc}, or nil when its immutable
   schema/options epochs are stale. An empty patch in the three-argument form
   leaves :doc nil without reading the stored document. The four-argument arity
   accepts a virtual current document so queued patches to the same entity can
   be prepared in order before any side index is mutated."
  ([^DB db ^PreparedLocalPatchIdocTx prepared tx-meta]
   (when (local-patch-idoc-tx-valid? db prepared)
     (let [e         (entid-strict db (:e prepared))
           old-datom (when (seq (:ops prepared))
                       (txcommon/ea-first-datom db e (:attr prepared)))
           old-doc   (idx/datom-prepared-value old-datom)]
       (stamp-local-patch-idoc-doc db prepared tx-meta old-doc))))
  ([^DB db ^PreparedLocalPatchIdocTx prepared tx-meta old-doc]
   (when (local-patch-idoc-tx-valid? db prepared)
     (entid-strict db (:e prepared))
     (stamp-local-patch-idoc-doc db prepared tx-meta old-doc))))

(defn- scalar-update-entity-id
  "Resolve an existing entity id for a scalar update, or nil to fall back.
  Numeric ids use the in-memory maximum; lookup references and idents read db."
  ^Long [^DB db e]
  (let [eid (cond
              (integer? e)
              (when (pos? (long e)) (long e))

              (and (sequential? e) (= 2 (count e)))
              (try (txcommon/entid db e) (catch Throwable _ nil))

              (keyword? e)
              (try (txcommon/entid db e) (catch Throwable _ nil))

              :else nil)]
    (when (and eid (pos? (long eid)) (<= (long eid) (long (:max-eid db))))
      (long eid))))

(defn- resolve-scalar-update-reference!
  "Reuse an entity resolution within one read-only preparation request."
  [db ^java.util.HashMap entities reference]
  (or (.get entities reference)
      (when-let [eid (scalar-update-entity-id db reference)]
        (.put entities reference eid)
        eid)))

(defn- resolve-scalar-update-references
  "Resolve distinct references in order, discarding results after this request."
  [db references]
  (let [entities (java.util.HashMap.)]
    (when (reduce (fn [_ reference]
                    (if (resolve-scalar-update-reference! db entities reference)
                      true
                      (reduced false)))
                  true references)
      entities)))

(defn- resolve-scalar-update-candidates!
  "Resolve eager candidates before correction, retaining the alias/E-A check."
  [db ^FastList candidates]
  (if (= 4 (.size candidates))
    ;; Singleton requests already satisfy distinctness.
    (when-let [eid (scalar-update-entity-id db (.get candidates 0))]
      (.set candidates 0 eid)
      true)
    (when-let [entities (resolve-scalar-update-references
                         db (map #(.get candidates %) (range 0 (.size candidates) 4)))]
      (let [seen (java.util.HashSet.)]
        (loop [i 0]
          (if (< i (.size candidates))
            (let [eid (get entities (.get candidates i))
                  attr (.get candidates (inc i))]
              (when (.add seen [eid attr])
                (.set candidates i eid)
                (recur (+ i 4))))
            true))))))

(defn- append-scalar-update-candidate!
  "Retain runtime entity checks around the shared pure candidate workers."
  [db store-schema tuple-source-attrs candidates seen e attr value]
  (when-let [props (txcompile/scalar-attribute-props
                    store-schema tuple-source-attrs attr value)]
    (when-let [eid (if (or (keyword? e) (and (sequential? e) (= 2 (count e))))
                    e (scalar-update-entity-id db e))]
      (txcompile/append-scalar-candidate!
        candidates seen eid attr value (:db/valueType props)))))

(defn- prepare-scalar-update-tx*
  [^DB db initial-es defer-entity-resolution?]
  (let [store        (.-store db)
        store-schema (schema store)
        store-opts   (opts store)
        validate?    (:validate-data? store-opts)
        tuple-source-attrs (:db/attrTuples (rschema store))]
    (when-not (:auto-entity-time? store-opts)
      (let [^FastList candidates
            (FastList. (int (* 4 (if (instance? java.util.Collection initial-es)
                                  (long (.size ^java.util.Collection initial-es)) 0))))]
        ;; Retain entity inputs and types. Resolve eager aliases before any
        ;; correction, after proving eligibility and distinct E/A pairs.
        (loop [es      (seq initial-es)
               seen    nil
               resolve-entities? false]
          (if es
            (let [entity (first es)
                  remaining (next es)]
              (if (map? entity)
                (let [e (:db/id entity)
                      seen (or seen
                               (when (or remaining (> (count entity) 2))
                                 (java.util.HashSet.)))]
                  ;; Request preflight already proved that this ID fits a long.
                  (when (and (<= (long e) (.-max-eid db))
                             (reduce-kv
                               (fn [_ attr value]
                                 (if (identical? attr :db/id)
                                   true
                                   (if (and (keyword? attr) (pos? (count (name attr)))
                                            (not (txcommon/reverse-ref? attr))
                                            (append-scalar-update-candidate!
                                              db store-schema tuple-source-attrs candidates
                                              seen e attr value))
                                     true
                                     (reduced false))))
                               true entity))
                    (recur remaining seen resolve-entities?)))
                (let [[_ e attr value] entity]
                  (when-let [eid (append-scalar-update-candidate!
                                   db store-schema tuple-source-attrs candidates seen e attr value)]
                    (recur remaining
                           (or seen
                               (when remaining
                                 (doto (java.util.HashSet.) (.add [eid attr]))))
                           (or resolve-entities? (not (integer? eid))))))))
            (when (and (not (.isEmpty candidates))
                       (or defer-entity-resolution? (not resolve-entities?)
                           (resolve-scalar-update-candidates! db candidates)))
              (let [entries
                    ;; Coercion remains legacy orchestration. Keep its errors
                    ;; at the original call site; compilation proves identity
                    ;; and uses the shared prepare value semantics separately.
                    (loop [i 0 entries (transient [])]
                      (if (< i (.size candidates))
                        (recur (+ i 4)
                               (conj! entries
                                      [(.get candidates i)
                                       (.get candidates (+ i 1))
                                       (prepare/correct-scalar-value
                                         validate? (.get candidates (+ i 3))
                                         (.get candidates (+ i 2)))]))
                        (persistent! entries)))
                    deferred? (and defer-entity-resolution?
                                   (some #(not (integer? (first %))) entries))]
                (cond-> (->PreparedScalarUpdate
                          store-schema store-opts entries
                          (when-not deferred?
                            (txcompile/scalar-update-entity-reads store-schema entries)))
                  deferred? (assoc :deferred-entity-resolution? true))))))))))

(defn ^:no-doc prepare-scalar-update-tx
  "Recognize ordinary scalar add operations and explicit numeric-ID maps
  updating existing entities with simple cardinality-one, non-unique attributes.
  Reference values must already be positive integer IDs. The caller must stamp
  it against the current LMDB write snapshot before committing. Unsupported
  shapes return nil. With :defer-entity-resolution?, lookup references
  and idents are resolved when stamping, for callers preparing before the writer.
  Internal :shape-checked? callers retain the proof from classify-local-tx."
  ([db initial-es]
   (prepare-scalar-update-tx db initial-es nil))
  ([db initial-es {:keys [defer-entity-resolution? shape-checked?]}]
   ;; Reject unsupported request shapes before correcting values or
   ;; resolving entities, including preparation on the collector's caller.
   (when (or shape-checked? (txcompile/scalar-update-request? initial-es))
     (if defer-entity-resolution?
       ;; Collector preparation reads no persisted identities. The owner resolves
       ;; them and consults the pending transaction indexes when stamping.
       (prepare-scalar-update-tx* db initial-es true)
       (txcommon/with-lookup-ref-cache db
         (prepare-scalar-update-tx* db initial-es false))))))

(defn ^:no-doc scalar-update-tx-valid?
  [^DB db ^PreparedScalarUpdate prepared]
  (let [store (.-store db)]
    (and (identical? (:schema-epoch prepared) (schema store))
         (identical? (:opts-epoch prepared) (opts store)))))

(defn- bind-scalar-entities
  "Resolve all entity operands before old-value reads or datom emission.
  Shared by legacy deferred preparation and compiled symbolic operands."
  [db {:keys [entries] :as prepared} store-schema]
  (txcommon/with-lookup-ref-cache db
    (if (= 1 (count entries))
      (let [[e attr value] (first entries)]
        (when-let [eid (scalar-update-entity-id db e)]
          (assoc prepared :entries [[eid attr value]] :entity-reads nil)))
      (let [entities (java.util.HashMap.)
            seen (java.util.HashSet.)
            resolved
            (reduce (fn [resolved [e attr value]]
                      (if-let [eid (resolve-scalar-update-reference! db entities e)]
                        ;; Distinct references can alias the same entity.
                        ;; Repeated E/A updates need general ordering.
                        (if (.add seen [eid attr])
                          (conj resolved [eid attr value])
                          (reduced nil))
                        (reduced nil)))
                    [] entries)]
        (when resolved
          (assoc prepared :entries resolved
                 :entity-reads (txcompile/scalar-update-entity-reads store-schema resolved)))))))

(defn- resolve-scalar-update-entities
  [db prepared]
  (if (:deferred-entity-resolution? prepared)
    (bind-scalar-entities db prepared (:schema-epoch prepared))
    prepared))

(def ^:dynamic *specialize-scalar-stamping?*
  "Internal comparison switch for resolved scalar stamping."
  true)

(defn ^:no-doc stamp-scalar-update-tx
  "Stamp a prepared scalar-update batch against the current store snapshot.
  Returns a TxReport without mutable index overlays, or nil when schema/options
  epochs are stale or entity references require general transaction resolution."
  [^DB db ^PreparedScalarUpdate prepared tx-meta]
  (when-let [prepared (when (scalar-update-tx-valid? db prepared)
                        (if (and *specialize-scalar-stamping?*
                                 (not (:deferred-entity-resolution? prepared)))
                          prepared
                          (resolve-scalar-update-entities db prepared)))]
    (let [tx-id   (inc (long (:max-tx db)))
          entries (:entries prepared)
          tx-data (txexec/execute-scalar-region
                    db entries (:entity-reads prepared) tx-id
                    *specialize-scalar-stamping?*)
          db-after (assoc db :max-tx tx-id)]
      (->TxReport db db-after tx-data {:db/current-tx tx-id} tx-meta))))

(defn ^:no-doc prepare-mixed-add-tx
  "Prepare ordinary mixed updates/inserts. An owned writer may supply its
  current maximum entity ID to avoid rescanning the native index."
  ([db tx-data tx-meta]
   (prepare-mixed-add-tx db tx-data tx-meta nil))
  ([db tx-data tx-meta owned-max-eid]
   (prepare-mixed-add-tx db tx-data tx-meta owned-max-eid false))
  ([db tx-data tx-meta owned-max-eid shape-checked?]
   (let [report (->TxReport db db [] {} tx-meta)
         plan (when (and (txengine/compiler?) (instance? Store (:store db)))
                (txcompile/compile-tx (prepare/make-prepare-ctx db) tx-data
                                      {:request-shape :mixed}))]
     (if plan
       (do
         (when *tx-compiler-observer* (*tx-compiler-observer* :prepared plan))
         (when-let [result (txadd/execute-mixed-region report plan owned-max-eid)]
           (when *tx-compiler-observer* (*tx-compiler-observer* :execute-mixed plan))
           result))
       (txadd/prepare-mixed-report report tx-data owned-max-eid shape-checked?)))))

(defn ^:no-doc prepare-local-request
  "Pure preparation portion of the DB request hook. Only standalone local
  callers opt into Stage 1. A rejection retains original input and admission
  isolation; unsupported shapes keep their existing route."
  [db tx-data origin]
  (when (and (identical? clojure.lang.PersistentVector (class tx-data))
             (pos? (count tx-data))
             (txengine/compiler?) (identical? origin :standalone-local)
             (nil? (l/explicit-transaction-timeout))
             (instance? Store (:store db))
             ;; Validation-enabled requests retain caller-side legacy rejection
             ;; until the deferred-validation route meets its own rollout gates.
             (not (:validate-data? (opts (:store db))))
             (let [lmdb (.-lmdb ^Store (:store db))]
               (and (not (l/writing? lmdb))
                    (not (Thread/holdsLock (l/write-txn lmdb))))))
    (when-let [shape (txcompile/literal-scalar-shape tx-data)]
      (let [ctx (prepare/make-prepare-ctx db)]
        ;; Options may change between the cheap admission check and capture.
        ;; Apply the rollout scope to the exact snapshot compilation will use.
        (when-let [outcome (when-not (:validate-data? (:opts ctx))
                            (txcompile/compile-tx
                          ctx tx-data
                          (if (identical? shape :scalar-operations)
                            {:literal-shape :scalar-operations} {:literal-shape :scalar})))]
      ;; A caller's old scalar preparer declines IDs above its published floor
      ;; before correction. Those failures belong to ordinary writer execution,
      ;; so a new pure correction rejection must not narrow their batch abort.
      ;; This admission check grants no execution proof; the writer rechecks IDs.
        (when (or (instance? datalevin.db.tx.plan.TxPlan outcome)
                  (every? (fn [form]
                            (<= (long (if (map? form) (:db/id form) (nth form 1)))
                                (long (:max-eid db))))
                          tx-data))
          (let [outcome (if (instance? datalevin.db.tx.plan.TxPlan outcome)
                          outcome
                          (assoc outcome :origin origin :isolated? true))]
            (when *tx-compiler-observer*
              (*tx-compiler-observer* :prepared outcome))
            outcome)))))))

(defn ^:no-doc prepare-compiled-request
  "Capture one pure decision for materialized local/server input. Writer
  admission and error isolation remain the caller's existing responsibility."
  [db forms]
  (when (and (txengine/compiler?) (instance? Store (:store db)))
    (when-let [shape (txcompile/literal-request-shape forms)]
      (let [outcome (txcompile/compile-tx (prepare/make-prepare-ctx db) forms
                      {:request-shape shape :allow-small-unique? true})]
        ;; Generic adapters admit successful plans only. Stage 1's explicitly
        ;; isolated scalar adapter retains ownership of deferred rejections.
        (when (instance? TxPlan outcome)
          (when *tx-compiler-observer* (*tx-compiler-observer* :prepared outcome))
          outcome)))))

(defn ^:no-doc prepare-local-scalar-request
  "Capture a successful Stage 2 plan after the caller's scalar classification.
  Provisional rejections retain legacy caller correction and admission."
  [db forms]
  (when (and (txengine/compiler?) (nil? (l/explicit-transaction-timeout))
             (instance? Store (:store db))
             (let [lmdb (.-lmdb ^Store (:store db))]
               (and (not (l/writing? lmdb))
                    (not (Thread/holdsLock (l/write-txn lmdb))))))
    (prepare-compiled-request db forms)))

(def ^:private fresh-request-keys
  #{:db-before :db-after :tx-data :tempids :tx-meta ::txexec/max-eid-before})

(defn- fresh-request-report? [report]
  (and (empty? (:tx-data report)) (empty? (:tempids report))
       (every? fresh-request-keys (keys report))))

(defn ^:no-doc compiled-request-route [report]
  (::compiled-route report))

(defn ^:no-doc clear-compiled-request-route [report]
  (dissoc report ::compiled-route ::independent-scalar?))

(defn- compiled-report
  [initial report route independent?]
  (when report
    (cond-> (-> initial
                (dissoc ::txexec/max-eid-before)
                (assoc :db-after (:db-after report) :tx-data (:tx-data report)
                       :tempids (:tempids report) ::compiled-route route))
      independent? (assoc ::independent-scalar? true))))

(defn- execute-compiled-request
  [initial plan {:keys [owned-max-eid identity-upsert?] :or {identity-upsert? true}}]
  (let [db (:db-after initial)
        before (max (long (:max-eid db))
                    (long (or owned-max-eid (::txexec/max-eid-before initial)
                              (i/init-max-eid (:store db)))))
        db (assoc db :max-eid before)
        initial (assoc initial :db-after db)
        tx-id (inc (long (:max-tx db)))]
    (case (:strategy plan)
      (:scalar :scalar-operations)
      (let [offset (if (identical? :scalar (:strategy plan)) 0 1)]
        (when (txexec/scalar-entities-within-bound? (:entries plan) before offset)
          (let [datoms (txexec/execute-scalar-region
                         db (:entries plan) (:entity-reads plan) tx-id
                         *specialize-scalar-stamping?* offset)]
            (when *tx-compiler-observer* (*tx-compiler-observer* :execute-scalar plan))
            (compiled-report initial
              (assoc initial :db-after (assoc db :max-tx tx-id) :tx-data datoms
                     :tempids {:db/current-tx tx-id})
              :compiled-scalar (and txcommon/*batch-prepare* (> (count datoms) 2))))))

      :scalar-aliases
      (when-let [{:keys [entries entity-reads]}
                 (bind-scalar-entities db plan (:schema (:ctx plan)))]
        (let [datoms (txexec/execute-scalar-region
                       db entries entity-reads tx-id *specialize-scalar-stamping?*)]
          (when *tx-compiler-observer* (*tx-compiler-observer* :execute-scalar plan))
          (compiled-report initial
            (assoc initial :db-after (assoc db :max-tx tx-id) :tx-data datoms
                   :tempids {:db/current-tx tx-id})
            :compiled-scalar (and txcommon/*batch-prepare* (> (count datoms) 2)))))

      :blind
      (when-let [[report upsert?]
                 (if (and identity-upsert? (:identity-upsert-av plan))
                   (stamp-blind-local-identity-tx db plan (:tx-meta initial))
                   (if (blind-local-tx-unique-values-absent? db plan)
                     [(stamp-blind-local-tx db plan (:tx-meta initial)) false]
                     (when identity-upsert?
                       (when-let [report (stamp-blind-local-upsert-tx db plan (:tx-meta initial))]
                         [report true]))))]
        (compiled-report initial report (if upsert? :identity-upsert :blind-insert)
                         (or upsert? (false? (:has-many? plan)))))

      :mixed
      (when-let [report (txadd/execute-mixed-region initial plan before)]
        (when *tx-compiler-observer* (*tx-compiler-observer* :execute-mixed plan))
        (compiled-report initial report :mixed-add true))
      nil)))

(defn- prepare-local-report
  [initial forms tx-time outcome {:keys [stage? route?] :as options}]
  (let [db (:db-after initial) captured (:ctx outcome)
        valid? (and outcome (identical? (:schema captured) (schema (:store db)))
                    (identical? (:opts captured) (opts (:store db))))
        eligible? (and (instance? Store (:store db)) (fresh-request-report? initial)
                       ;; Queryable simulation inside a live batch still uses
                       ;; the established pending-view/flush integration.
                       (not (and stage? txcommon/*batch-prepare*)))
        ctx (if (and eligible? valid?) captured
                (prepare/make-prepare-ctx (if eligible? db (:db-before initial))))
        selected (when (and eligible? (or outcome (txengine/compiler?)))
                   (if valid? outcome
                       (do
                         (when (and outcome *tx-compiler-observer*)
                           (*tx-compiler-observer* :recompile forms))
                         (let [selected (txcompile/compile-tx ctx forms {:allow-small-unique? true})]
                           (when (and selected (nil? outcome) *tx-compiler-observer*)
                             (*tx-compiler-observer* :prepared selected))
                           selected))))
        prepared (when (instance? TxPlan selected)
                   (execute-compiled-request initial selected options))
        report (if prepared
                 (if stage? (txexec/stage-compiled-report! prepared) prepared)
                 (do
                   (when (and outcome txcommon/*batch-prepare*)
                     (txcommon/materialize-scalar-pending! txcommon/*batch-prepare* db))
                   (when (and (or selected outcome) *tx-compiler-observer*)
                     (*tx-compiler-observer* :ordered forms))
                   (cond-> (txexec/prepare-and-transact-tx-data initial forms tx-time ctx)
                     route? (assoc ::compiled-route :general))))]
    (cond-> report
      (not route?) (dissoc ::compiled-route)
      stage? (dissoc ::independent-scalar?))))

(defn ^:no-doc prepare-local-tx-data
  "Resolve one request through shared compilation and writer execution.
  Three-argument preparation returns a queryable request-local view; writer
  adapters may reuse a captured outcome and retain the existing commit staging."
  ([initial forms]
   (prepare-local-tx-data initial forms (System/currentTimeMillis)))
  ([initial forms tx-time]
   (if (txengine/compiler?)
     (prepare-local-report initial forms tx-time nil {:stage? true})
     (txexec/prepare-and-transact-tx-data
       initial forms tx-time (prepare/make-prepare-ctx (:db-before initial)))))
  ([db forms tx-meta tx-time outcome]
   (prepare-local-tx-data db forms tx-meta tx-time outcome nil))
  ([db forms tx-meta tx-time outcome initial]
   (prepare-local-tx-data db forms tx-meta tx-time outcome initial
                          {:stage? false :owned-max-eid (:max-eid db)}))
  ([db forms tx-meta tx-time outcome initial options]
   (prepare-local-report (or initial (->TxReport db db [] {} tx-meta))
                         forms tx-time outcome options)))

(defn transact-tx-data
  "Resolve transaction data. owned-max-eid may be supplied only for a current
  local DB view whose native writer is owned by the caller."
  ([initial-report initial-es simulated?]
   (transact-tx-data initial-report initial-es simulated? nil))
  ([initial-report initial-es simulated? owned-max-eid]
   (let [^DB db  (:db-before initial-report)
         store   (.-store db)
         tx-time (System/currentTimeMillis)]
     (if (remote-store? store)
       (try
         (let [txs                                    (sequence
                                                       (mapcat
                                                         expand-transactable-entity)
                                                       initial-es)
               res                                    (i/tx-data store txs simulated?)
               db-info                                (when (map? res) (:db-info res))
               res                                    (if db-info (dissoc res :db-info) res)
               [tx-data tempids max-eid new-attributes] (remote-tx-result res)
               info (resolved-remote-db-info
                      store db (or db-info {}) max-eid simulated?)]
           (when-not simulated?
             (invalidate-cache store
                               tx-data
                               (:last-modified info)
                               (:max-tx info)))
           (cond-> (assoc initial-report
                          :db-after (-> (carry-runtime-opts (new-db store info db) db)
                                        (assoc :max-eid (:max-eid info))
                                      (#(if simulated?
                                          (update % :max-tx u/long-inc)
                                          %)))
                          :tx-data tx-data
                          :tempids tempids)
             (seq new-attributes) (assoc :new-attributes new-attributes)
             simulated? mark-simulated-tx-cache!))
         (catch Throwable e
           (when-not simulated?
             (invalidate-remote-cache! store))
           (throw e)))
       (let [initial-report (cond-> initial-report
                              (some? owned-max-eid)
                              (assoc ::txexec/max-eid-before owned-max-eid))
             report (prepare-local-tx-data initial-report initial-es tx-time)]
         (if simulated?
           (mark-simulated-tx-cache! report)
           (assoc report :db-after
                  (commit-prepared-tx-data!
                    (:db-after report) (:tx-data report) report))))))))

(defn transact-ack
  "Transact on a remote store without fetching a transaction report.
   Return the refreshed DB view after clearing cached reads."
  [^DB db tx-data]
  (vld/validate-tx-data-shape tx-data)
  (let [store (.-store db)
        txs (sequence (mapcat expand-transactable-entity) tx-data)]
    (try
      (let [response (i/tx-data-ack store txs)
            info (:db-info response)]
        (when-not (= :transacted (:result response))
          (raise "Invalid transaction acknowledgement" {:response response}))
        ;; Without changed datoms we cannot selectively invalidate cached reads.
        ;; Reuse the cache's capacity without an extra remote `opts` request, and
        ;; advance its generation so pre-commit readers cannot repopulate it.
        (if-some [^LRUCache cache (.get ^ConcurrentHashMap caches (dir store))]
          (do
            (locking cache
              (.beginInvalidation cache (long (:last-modified info)))
              (.clear cache))
            (mark-remote-cache-max-tx! store (:max-tx info))
            (mark-remote-cache-check! store))
          (refresh-cache store (:last-modified info) (:max-tx info)))
        (carry-runtime-opts (new-db store info db) db))
      (catch Throwable e
        (invalidate-remote-cache! store)
        (throw e)))))

(defn tx-data->simulated-report
  [db tx-data]
  {:pre [(db? db)]}
  (vld/validate-tx-data-shape tx-data)
  (let [initial-report (map->TxReport
                         {:db-before db
                          :db-after  (transfer db (.-store ^DB db))
                          :tx-data   []
                          :tempids   {}
                          :tx-meta   nil})]
    (transact-tx-data initial-report tx-data true)))

(defn abort-transact
  [conn]
  (let [s (.-store ^DB (deref conn))]
    (if (remote-store? s)
      (i/abort-transact s)
      (abort-transact-kv (.-lmdb ^Store s)))))

(defn datalog-index-cache-limit
  ([^DB db]
   (let [^Store store (.-store db)]
     (:cache-limit (opts store))))
  ([^DB db ^long n]
   (let [^Store store (.-store db)]
     (assoc-opt store :cache-limit n)
     (refresh-cache store (System/currentTimeMillis)))))
