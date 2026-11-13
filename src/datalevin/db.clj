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
   [datalevin.db.tx.execute :as txexec]
   [datalevin.db.tx.prepare :as txprep]
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
   [datalevin.db.tx.plan TxPlan MixedOperands MixedAttrInfo]
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

(defn- remote-tx-input
  "Retain recognized materialized input for server compilation. Such forms
  cannot be Entity objects requiring client-side expansion. Other inputs keep
  the existing sequence demand and expansion; compilation remains server-owned."
  [forms]
  (if (txcompile/literal-request-shape forms)
    forms
    (sequence (mapcat expand-transactable-entity) forms)))

(declare stamp-prepared-request)

(defn ^:no-doc blind-local-tx-unique-values-absent?
  "Check a prepared batch's unique values against the current store snapshot.
   The caller invokes this after opening its LMDB write transaction, so all
   probes share that transaction and no concurrent insert can race the check."
  [db prepared]
  (txexec/blind-unique-values-absent? db (:unique-avs prepared)))

(defn ^:no-doc blind-local-tx-valid?
  [^DB db prepared]
  (let [store (.-store db)]
    (and (identical? :blind (:strategy prepared))
         (identical? (:schema (:ctx prepared)) (schema store))
         (identical? (:opts (:ctx prepared)) (opts store)))))

(defn ^:no-doc stamp-blind-local-upsert-tx
  "Reuse a collided blind batch's normalized scalar values. Independent maps
   with one identity each can resolve insert versus upsert under the same
   writer. Repeated owners and ambiguous identities retain general resolution."
  [^DB db prepared tx-meta]
  (let [^FastList entities (:entries prepared)
        bound? (::bound-operands? prepared)
        ^FastList identity-avs (when (:independent-identity-upserts? prepared)
                                (:unique-avs prepared))]
    (when (and (not (:auto-entity-time? prepared))
               (not (:has-many? prepared))
               (:has-unique? prepared)
               (= (.size entities) (.size ^FastList (:unique-avs prepared)))
               (or bound? identity-avs)
               (blind-local-tx-valid? db prepared))
      (when-let [report (txexec/execute-blind-upserts
                          (->TxReport db db [] {} tx-meta) entities identity-avs
                          (:schema (:ctx prepared)))]
        (when *tx-compiler-observer*
          (*tx-compiler-observer* :execute-blind-upsert prepared))
        report))))

(defn ^:no-doc stamp-blind-local-identity-tx
  "Stamp one eligible simple identity transaction, resolving insert versus
   upsert inside the caller's LMDB write transaction. Returns
   [report upsert?], or nil when the prepared shape is not eligible."
  [^DB db prepared tx-meta]
  (when (blind-local-tx-valid? db prepared)
    (when-let [[identity-attr identity-value] (:identity-upsert-av prepared)]
      (if-some [eid (txcommon/av-first-e db identity-attr identity-value)]
        (let [report (txexec/execute-blind-identity-upsert
                       (->TxReport db db [] {} tx-meta)
                       (.get ^FastList (:entries prepared) 0)
                       identity-attr eid (:identity-upsert-read prepared))]
          (when *tx-compiler-observer*
            (*tx-compiler-observer* :execute-blind-upsert prepared))
          [report true])
        [(stamp-prepared-request db prepared tx-meta) false]))))

(defn ^:no-doc scalar-update-tx-valid?
  [^DB db ^TxPlan prepared]
  (let [store (.-store db)]
    (and (::bound-operands? prepared)
         (identical? :scalar (:strategy prepared))
         (identical? (:schema (:ctx prepared)) (schema store))
         (identical? (:opts (:ctx prepared)) (opts store)))))

(defn- bind-scalar-entities
  "Attach writer-bound operands/layouts to the adapter-owned representation."
  [db {:keys [entries] :as prepared} store-schema]
  (when-let [entries (txexec/resolve-scalar-update-entries db entries)]
    (assoc prepared :entries entries
           :entity-reads (when (> (count entries) 1)
                           (txcompile/scalar-update-entity-reads store-schema entries)))))

(def ^:dynamic *specialize-scalar-stamping?*
  "Internal comparison switch for resolved scalar stamping."
  true)

(defn- binding-context
  "Capture metadata after the caller's shape/ID preflight."
  [db]
  (let [store (:store db)
        store-schema (schema store)
        store-opts (opts store)
        store-rschema (rschema store)]
    (prepare/->PrepareCtx store-schema store-rschema store-opts)))

(defn ^:no-doc prepare-admitted-request
  "Bind admitted operands at their established caller checkpoint. Scalar/blind
  correction retains explicit runtime provenance; patch operands use the pure
  patch rule. Ownership and old-value reads remain current-writer work."
  [db forms {:keys [strategy shape-checked? defer-entity-resolution?
                    allow-small-unique? check-unique-avs? entity-kinds]
             :or {check-unique-avs? true}}]
  (case strategy
    :scalar
    (when (or shape-checked? (txcompile/scalar-update-request? forms))
      (let [prepare-plan
            (fn []
              (let [ctx (binding-context db)]
                (when-let [entries (txexec/prepare-scalar-operands
                                    db ctx forms defer-entity-resolution?)]
                  (let [deferred? (and defer-entity-resolution?
                                       (some #(not (integer? (first %))) entries))]
                    (cond-> (assoc (txplan/->TxPlan
                                     ctx :scalar entries
                                     (when-not deferred?
                                       (txcompile/scalar-update-entity-reads (:schema ctx) entries)))
                                   ::bound-operands? true)
                      deferred? (assoc :deferred-entity-resolution? true))))))]
        (if defer-entity-resolution?
          (prepare-plan)
          (txcommon/with-lookup-ref-cache db (prepare-plan)))))

    :blind
    (when-let [kinds (txexec/blind-entity-kinds forms entity-kinds)]
      (let [ctx (binding-context db)]
        (when-let [operands (txexec/prepare-blind-operands
                             ctx forms allow-small-unique? check-unique-avs? kinds)]
          (let [{:keys [entries identity-upsert-av]} operands]
            (assoc (txplan/map->TxPlan (assoc operands :ctx ctx :strategy :blind))
                   ::bound-operands? true
                   :identity-upsert-read
                   (txcompile/blind-identity-entity-read (:schema ctx) entries identity-upsert-av))))))
    :patch (txexec/prepare-patch-operands (binding-context db) forms)
    nil))

(defn ^:no-doc stamp-prepared-request
  "Execute shared scalar/blind/patch operands against the current writer snapshot.
  Bound scalar operands retain epoch and entity-binding checks. Request adapters
  use the authoritative compiler hook with their original forms."
  [db prepared tx-meta]
  (case (:strategy prepared)
    :scalar
    (when-let [prepared (when (scalar-update-tx-valid? db prepared)
                         (if (:deferred-entity-resolution? prepared)
                           (bind-scalar-entities db prepared (:schema (:ctx prepared)))
                           prepared))]
      (txexec/execute-scalar-report
        (->TxReport db db [] {} tx-meta) (:entries prepared) (:entity-reads prepared)
        (inc (long (:max-tx db))) *specialize-scalar-stamping?*))

    :blind
    (when (blind-local-tx-valid? db prepared)
      (let [report (txexec/execute-blind-inserts
                     (->TxReport db db [] {} tx-meta) (:entries prepared)
                     (:auto-entity-time? prepared))]
        (when *tx-compiler-observer*
          (*tx-compiler-observer* :execute-blind prepared))
        report))
    :patch
    (when (and (identical? (:schema (:ctx prepared)) (schema (:store db)))
               (identical? (:opts (:ctx prepared)) (opts (:store db))))
      (txexec/execute-patch-region (->TxReport db db [] {} tx-meta) prepared))
    nil))

(defn ^:no-doc prepare-local-request
  "Capture isolated scalar outcomes at standalone admission. A rejection keeps
  original input and failure isolation; other forms compile at their checkpoint."
  [db tx-data origin]
  (when (and (identical? clojure.lang.PersistentVector (class tx-data))
             (pos? (count tx-data))
             (identical? origin :standalone-local)
             (nil? (l/explicit-transaction-timeout))
             (instance? Store (:store db))
             ;; Validated inputs retain caller-side correction so a rejection
             ;; does not widen the existing collector failure partition.
             (not (:validate-data? (opts (:store db))))
             (let [lmdb (.-lmdb ^Store (:store db))]
               (and (not (l/writing? lmdb))
                    (not (Thread/holdsLock (l/write-txn lmdb))))))
    (when-let [shape (txcompile/literal-scalar-shape tx-data)]
      (let [ctx (prepare/make-prepare-ctx db)]
        ;; Options may change between the cheap admission check and capture.
        ;; Recheck validation against the snapshot compilation will use.
        (when-let [outcome (when-not (:validate-data? (:opts ctx))
                            (txcompile/compile-tx
                              ctx tx-data
                              (if (identical? shape :scalar-operations)
                                {:literal-shape :scalar-operations}
                                {:literal-shape :scalar})))]
          ;; IDs above the published floor belong to writer-bound resolution.
          ;; A pure correction rejection must preserve their failure partition.
          ;; Admission grants no execution proof; the writer rechecks IDs.
          (when (or (instance? TxPlan outcome)
                    (every? (fn [form]
                              (<= (long (if (map? form) (:db/id form) (nth form 1)))
                                  (long (:max-eid db))))
                            tx-data))
            (let [outcome (if (instance? TxPlan outcome)
                            outcome
                            (assoc outcome :origin origin :isolated? true))]
              (when *tx-compiler-observer*
                (*tx-compiler-observer* :prepared outcome))
              outcome)))))))

(defn- successful-compiled-request
  [db forms shape {:keys [allow-small-unique? check-unique-avs? ordered?]
                   :or {allow-small-unique? true check-unique-avs? true ordered? true}}]
  (let [outcome (txcompile/compile-tx (prepare/make-prepare-ctx db) forms
                  {:request-shape shape :allow-small-unique? allow-small-unique?
                   :check-unique-avs? check-unique-avs? :ordered? ordered?})]
    ;; Generic adapters admit successful plans only. The isolated scalar
    ;; admission hook owns deferred rejections.
    (when (and (instance? TxPlan outcome)
               ;; Datom/general callers retain their allocation/report boundary
               ;; when a literal ID is above the published floor. This grants
               ;; no ownership proof; accepted plans still bind on the writer.
               (or (not (:explicit-tx? outcome))
                   (every? (fn [[e]]
                             (or (not (instance? Long e))
                                 (<= (long e) (long (:max-eid db)))))
                           (:entries outcome))))
      (when *tx-compiler-observer* (*tx-compiler-observer* :prepared outcome))
      outcome)))

(defn ^:no-doc prepare-compiled-request
  "Capture one pure decision for materialized local/server input. Writer
  admission and error isolation remain the caller's existing responsibility.
  Blind callers may retain their small-unique and unique-value collection policy;
  request shape always comes from the forms."
  ([db forms] (prepare-compiled-request db forms nil))
  ([db forms options]
   (when (instance? Store (:store db))
     (when-let [shape (txcompile/literal-request-shape forms)]
       (successful-compiled-request db forms shape options)))))

(defn ^:no-doc prepare-local-compiled-request
  "Capture inert plans before caller binding. Timed scalar requests retain their
  pre-writer preparation phase; provisional rejections retain caller correction."
  [db forms]
  (when (and (instance? Store (:store db))
             (let [lmdb (.-lmdb ^Store (:store db))]
               (and (not (l/writing? lmdb))
                    (not (Thread/holdsLock (l/write-txn lmdb))))))
    (when-let [shape (txcompile/literal-request-shape forms)]
      (when (and (not (identical? :ordered shape))
                 (or (nil? (l/explicit-transaction-timeout))
                     (#{:scalar :scalar-operations :scalar-aliases :explicit-additions} shape)))
        (successful-compiled-request db forms shape {:ordered? false})))))

(def ^:private fresh-request-keys
  #{:db-before :db-after :tx-data :tempids :tx-meta ::txexec/max-eid-before})

(defn- fresh-request-report? [report]
  (and (empty? (:tx-data report)) (empty? (:tempids report))
       (every? fresh-request-keys (keys report))))

(defn ^:no-doc compiled-request-route [report]
  (::compiled-route report))

(defn ^:no-doc clear-compiled-request-route [report]
  (dissoc report ::compiled-route ::independent-scalar?))

(defn ^:no-doc request-storage-independent?
  "Validate resolved scalar-transition evidence against its exact logical trace
  and current storage schema. An input plan alone supplies neither proof."
  [report]
  (let [proof (::independent-scalar? report)]
    (and (vector? proof) (= 2 (count proof))
         (identical? (nth proof 1) (:tx-data report))
         (identical? (nth proof 0) (schema (:store (:db-after report)))))))

(defn ^:no-doc compiled-storage-attributes
  "Expose existing logical bindings only within the owned commit scope.
  Storage receives fresh entries for its existing table, never compiler scratch."
  [plan]
  (when (and (instance? TxPlan plan) (identical? :mixed (:strategy plan)))
    (let [^java.util.HashMap infos (.-infos ^MixedOperands (:entries plan))]
      [(:schema (:ctx plan))
       (fn [attr]
         (when-let [^MixedAttrInfo info (.get infos attr)]
           (when (.-ordinary? info)
             (let [props (.-props info)
                   binding (object-array 4)]
               (aset binding 0 (:db/aid props))
               (aset binding 1 (.-storage-type info))
               (aset binding 2 (:db/noindex props))
               (aset binding 3 props)
               binding))))])))

(defn ^:no-doc request-storage-attributes
  "Use the compiler's metadata-reuse decision only for an owned group whose
  table needs population. Warm tables and small requests avoid a binding scope."
  [db plan]
  (when (and txcommon/*batch-prepare* (instance? TxPlan plan)
             (identical? :mixed (:strategy plan))
             (.-storage? ^MixedOperands (:entries plan)))
    (when-let [^WriteGroup group (s/current-write-group (.-lmdb ^Store (:store db)))]
      (let [^objects context (.-storage-context group)]
        (when (or (nil? (aget context 4))
                  (not (identical? (:schema (:ctx plan)) (aget context 5))))
          (compiled-storage-attributes plan))))))

(defn- compiled-report
  [initial report route independent? ctx]
  (when report
    (cond-> (-> initial
                (dissoc ::txexec/max-eid-before)
                (assoc :db-after (:db-after report) :tx-data (:tx-data report)
                       :tempids (:tempids report) ::compiled-route route))
      independent? (assoc ::independent-scalar? [(:schema ctx) (:tx-data report)]))))

(defn- request-execution-report
  "Give ordered work its own resolver overlays. Collected preparation owns its
  pending indexes and restore boundary; other DB values retain their old view."
  [report]
  (if txcommon/*batch-prepare*
    report
    (let [before (:db-after report)
          after (reduce
                  (fn [view key]
                    (if-let [^java.util.SortedSet index (key before)]
                      (assoc view key
                             (doto (TreeSortedSet. (.comparator index))
                               (.addAll index)))
                      view))
                  before [:eavt :avet])]
      (assoc report :db-after after))))

(defn- execute-compiled-request
  [initial plan {:keys [owned-max-eid identity-upsert? tx-time] :or {identity-upsert? true}}]
  (if (identical? :dynamic (:strategy plan))
    (let [report (txexec/execute-dynamic-region (request-execution-report initial) plan tx-time)]
      (when *tx-compiler-observer* (*tx-compiler-observer* :execute-dynamic plan))
      (assoc report ::compiled-route :compiled-dynamic))
    (let [db (:db-after initial)
          before (max (long (:max-eid db))
                      (long (or owned-max-eid (::txexec/max-eid-before initial)
                                (i/init-max-eid (:store db)))))
          db (assoc db :max-eid before)
          initial (assoc initial :db-after db)
          tx-id (inc (long (:max-tx db)))]
      (case (:strategy plan)
        (:scalar :scalar-operations :scalar-aliases)
        (let [offset (if (identical? :scalar-operations (:strategy plan)) 1 0)
              bound (if (or (identical? :scalar-aliases (:strategy plan))
                            (:deferred-entity-resolution? plan))
                      (bind-scalar-entities db plan (:schema (:ctx plan)))
                      (when (txexec/scalar-entities-within-bound? (:entries plan) before offset)
                        plan))]
          (when bound
            (when-let [report (txexec/execute-scalar-report
                               initial (:entries bound) (:entity-reads bound) tx-id
                               (and *specialize-scalar-stamping?* (not (:explicit-tx? plan))) offset)]
              (when *tx-compiler-observer* (*tx-compiler-observer* :execute-scalar plan))
              (compiled-report initial report :compiled-scalar
                               (and txcommon/*batch-prepare* (> (count (:tx-data report)) 2)) (:ctx plan)))))

        :blind
        (when-let [[report upsert?]
                   (if (and identity-upsert? (:identity-upsert-av plan))
                     (stamp-blind-local-identity-tx db plan (:tx-meta initial))
                     (if (blind-local-tx-unique-values-absent? db plan)
                       [(stamp-prepared-request db plan (:tx-meta initial)) false]
                       (when identity-upsert?
                         (when-let [report (stamp-blind-local-upsert-tx db plan (:tx-meta initial))]
                           [report true]))))]
          (compiled-report initial report (if upsert? :identity-upsert :blind-insert)
                           (or upsert? (false? (:has-many? plan))) (:ctx plan)))

        :ordered
        (let [report (txexec/execute-ordered-region (request-execution-report initial) plan tx-time)]
          (when *tx-compiler-observer* (*tx-compiler-observer* :execute-ordered plan))
          (compiled-report initial report :compiled-ordered false (:ctx plan)))

        :patch
        (when-let [report (txexec/execute-patch-region initial plan)]
          (when *tx-compiler-observer* (*tx-compiler-observer* :execute-patch plan))
          (assoc report ::compiled-route :patch-idoc))

        :mixed
        (when-let [report (txadd/execute-additions initial plan before)]
          (when *tx-compiler-observer* (*tx-compiler-observer* :execute-mixed plan))
          (compiled-report initial report :mixed-add true (:ctx plan)))
        nil))))

(defn- prepare-local-report
  [initial forms tx-time outcome {:keys [stage? route?] :as options}]
  (let [db (:db-after initial) captured (:ctx outcome)
        valid? (and outcome
                    (identical? (:schema captured) (schema (:store db)))
                    (identical? (:opts captured) (opts (:store db))))
        eligible? (and (instance? Store (:store db)) (fresh-request-report? initial)
                       ;; Queryable simulation inside a live batch still uses
                       ;; the established pending-view/flush integration.
                       (not (and stage? txcommon/*batch-prepare*)))
        ctx (if (and eligible? valid?) captured
                (prepare/make-prepare-ctx (if eligible? db (:db-before initial))))
        selected (when eligible?
                   (if valid? outcome
                       (do
                         (when (and outcome *tx-compiler-observer*)
                           (*tx-compiler-observer* :recompile forms))
                         (let [selected (txcompile/compile-tx ctx forms {:allow-small-unique? true :dynamic? true})]
                           (when (and selected (nil? outcome) *tx-compiler-observer*)
                             (*tx-compiler-observer* :prepared selected))
                           selected))))
        prepared (when (instance? TxPlan selected)
                   (execute-compiled-request initial selected (assoc options :tx-time tx-time)))
        report (if prepared
                 (if (and stage? (not= :dynamic (:strategy selected)))
                   (txexec/stage-compiled-report! prepared) prepared)
                 (do
                   (when (and outcome (satisfies? txcommon/ScalarPendingIndex txcommon/*batch-prepare*))
                     (txcommon/materialize-scalar-pending! txcommon/*batch-prepare* db))
                   (let [program (txcompile/compile-program ctx forms)
                         report (txexec/execute-dynamic-region
                                  (request-execution-report initial) program tx-time)]
                     (when *tx-compiler-observer* (*tx-compiler-observer* :execute-dynamic program))
                     (cond-> report route? (assoc ::compiled-route :compiled-dynamic)))))]
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
   (prepare-local-report initial forms tx-time nil {:stage? true}))
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
         (let [txs                                    (remote-tx-input initial-es)
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
        txs (remote-tx-input tx-data)]
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
