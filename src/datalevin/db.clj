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
   [datalevin.constants :as c :refer [e0 tx0 emax txmax v0 vmax]]
   [datalevin.datom :as d :refer [datom datom?]]
   [datalevin.db.tx.common :as txcommon]
   [datalevin.db.tx.execute :as txexec]
   [datalevin.db.tx.prepare :as txprep]
   [datalevin.ha.client-op :as hcop]
   [datalevin.util :as u
    :refer [case-tree defrecord-updatable conjv concatv]]
   [datalevin.lmdb :as l]
   [datalevin.storage :as s]
   [datalevin.prepare :as prepare]
   [datalevin.query-util :as qu]
   [datalevin.udf :as udf]
   [datalevin.validate :as vld]
   [datalevin.remote :as r]
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
   [datalevin.interface IStore]
   [datalevin.storage Store]
   [datalevin.remote DatalogStore]
   [datalevin.utl LRUCache]
   [java.util SortedSet Comparator Date]
   [java.util.concurrent ConcurrentHashMap]
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

(defprotocol ITuples
  (-init-tuples [db out a v-range pred get-v?])
  (-init-tuples-list [db a v-range pred get-v?])
  (-sample-init-tuples [db out a mcount v-range pred get-v?])
  (-sample-init-tuples-list [db a mcount v-range pred get-v?])
  (-e-sample [db a])
  (-default-ratio [db a])
  (-eav-scan-v [db in out eid-idx attrs-v])
  (-eav-scan-v-list [db in eid-idx attrs-v])
  (-val-eq-scan-e [db in out v-idx attr] [db in out v-idx attr bound])
  (-val-eq-scan-e-list [db in v-idx attr] [db in v-idx attr bound])
  (-val-eq-filter-e [db in out v-idx attr f-idx])
  (-val-eq-filter-e-list [db in v-idx attr f-idx]))

;; ----------------------------------------------------------------------------

(declare empty-db resolve-datom validate-attr components->pattern
         components->end-datom)

(defrecord TxReport [db-before db-after tx-data tempids tx-meta])

(defrecord PreparedBlindTx
  [schema-epoch
   opts-epoch
   auto-entity-time?
   entities])

;; (defmethod print-method TxReport [^TxReport rp, ^java.io.Writer w]
;;   (binding [*out* w]
;;     (pr {:datoms-transacted (count (:tx-data rp))})))

(defn- sf [^SortedSet s] (when-not (.isEmpty s) (.first s)))

(defonce dbs (atom {}))

;; read caches
(defonce ^:private caches (ConcurrentHashMap.))
(defonce ^:private remote-cache-check-ms (ConcurrentHashMap.))
(defonce ^:private remote-cache-max-tx (ConcurrentHashMap.))

(defn- mark-remote-cache-check!
  [store]
  (when (instance? DatalogStore store)
    (.put ^ConcurrentHashMap remote-cache-check-ms
          (dir store)
          (System/currentTimeMillis))))

(defn- cached-remote-cache-max-tx
  [store]
  (when (instance? DatalogStore store)
    (.get ^ConcurrentHashMap remote-cache-max-tx (dir store))))

(defn- mark-remote-cache-max-tx!
  [store remote-max-tx]
  (when (and (instance? DatalogStore store)
             (some? remote-max-tx))
    (.put ^ConcurrentHashMap remote-cache-max-tx
          (dir store)
          (long remote-max-tx))))

(defn- should-check-remote-cache?
  [store cache]
  (if (instance? DatalogStore store)
    (let [interval-ms (long c/*remote-db-last-modified-check-interval-ms*)]
      (or (nil? cache)
          (not (pos? interval-ms))
          (let [last-check-ms (.get ^ConcurrentHashMap remote-cache-check-ms
                                    (dir store))]
            (or (nil? last-check-ms)
                (let [elapsed-ms (- (System/currentTimeMillis)
                                    (long last-check-ms))]
                  (>= elapsed-ms interval-ms))))))
    true))

(defn refresh-cache
  ([store]
   (refresh-cache store (last-modified store) nil))
  ([store target]
   (refresh-cache store target nil))
  ([store target remote-max-tx]
   (let [target (long (or target 0))]
     (.put ^ConcurrentHashMap caches (dir store)
           (LRUCache. (:cache-limit (opts store)) target))
     (mark-remote-cache-max-tx! store remote-max-tx)
     (mark-remote-cache-check! store))))

(defn- ensure-cache
  ([store target]
   (ensure-cache store target nil))
  ([store target remote-max-tx]
   (let [target        (long (or target 0))
         cached-max-tx (cached-remote-cache-max-tx store)]
     (if-some [^LRUCache cache (.get ^ConcurrentHashMap caches (dir store))]
       (if (or (< ^long (.target cache) ^long target)
               (and (some? remote-max-tx)
                    (some? cached-max-tx)
                    (< (long cached-max-tx)
                       (long remote-max-tx))))
         (refresh-cache store target remote-max-tx)
         (do
           (.setTarget cache target)
           (mark-remote-cache-max-tx! store remote-max-tx)
           (mark-remote-cache-check! store)))
       (refresh-cache store target remote-max-tx)))))

(defn cache-disabled?
  [store]
  (.isDisabled ^LRUCache (.get ^ConcurrentHashMap caches (dir store))))

(defn disable-cache
  [store]
  (.disable ^LRUCache (.get ^ConcurrentHashMap caches (dir store))))

(defn enable-cache
  [store]
  (.enable ^LRUCache (.get ^ConcurrentHashMap caches (dir store))))

(defn cache-get
  [store k]
  (.get ^LRUCache (.get ^ConcurrentHashMap caches (dir store)) k))

(defn cache-put
  [store k v]
  (.put ^LRUCache (.get ^ConcurrentHashMap caches (dir store)) k v))

(defn remove-cache
  [store]
  (.remove ^ConcurrentHashMap caches (dir store))
  (.remove ^ConcurrentHashMap remote-cache-max-tx (dir store))
  (.remove ^ConcurrentHashMap remote-cache-check-ms (dir store)))

(defmacro wrap-cache
  [store pattern body]
  `(let [cache# (.get ^ConcurrentHashMap caches (dir ~store))]
     (if-some [cached# (.get ^LRUCache cache# ~pattern)]
       cached#
       (let [res# ~body]
         (.put ^LRUCache cache# ~pattern res#)
         res#))))

(defn- tx-touch-summary
  [tx-data]
  (reduce
    (fn [acc ^Datom datom]
      (let [e (.-e datom)
            a (.-a datom)
            v (.-v datom)]
        (-> acc
            (update :eids conj e)
            (update :attrs conj a)
            (update :values conj v)
            (update-in [:values-by-attr a] (fnil conj #{}) v))))
    {:eids #{} :attrs #{} :values #{} :values-by-attr {}}
    tx-data))

(defn- tx-affects-pattern?
  [{:keys [eids attrs values values-by-attr]} e a v]
  (and (or (nil? e) (contains? eids e))
       (or (nil? a) (contains? attrs a))
       (or (nil? v)
           (if (nil? a)
             (contains? values v)
             (contains? (get values-by-attr a #{}) v)))))

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
  (with-runtime-opts x (runtime-opts source)))

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
          (if-some [[e a _] (index-components->pattern index c1 c2 c3)]
            (let [[e* _ v*] (index-components->pattern index c1 c2 c3)]
              (or (unresolved-pattern? e* v*)
                  (tx-affects-pattern? touches e* a nil)))
            true))

        :rseek
        (let [[_ index c1 c2 c3] k]
          (if-some [[e a _] (index-components->pattern index c1 c2 c3)]
            (let [[e* _ v*] (index-components->pattern index c1 c2 c3)]
              (or (unresolved-pattern? e* v*)
                  (tx-affects-pattern? touches e* a nil)))
            true))

        :cardinality
        (let [[_ a] k]
          (contains? attrs a))

        :index-range
        (let [[_ a start end] k]
          (or (unresolved-pattern? nil start)
              (unresolved-pattern? nil end)
              (contains? attrs a)))

        :index-range-size
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

(defn- invalidate-cache
  ([store tx-data target]
   (invalidate-cache store tx-data target nil))
  ([store tx-data target remote-max-tx]
   (if-some [^LRUCache cache (.get ^ConcurrentHashMap caches (dir store))]
     (do
       (when (seq tx-data)
         (let [touches (tx-touch-summary tx-data)]
           (doseq [k (.keys cache)
                   :when (tx-affects-cache-key? touches k)]
             (.remove cache k))))
       (.setTarget cache target)
       (mark-remote-cache-max-tx! store remote-max-tx))
     (refresh-cache store target remote-max-tx))))

(defrecord-updatable DB [^IStore store
                         ^long max-eid
                         ^long max-tx
                         ^TreeSortedSet eavt
                         ^TreeSortedSet avet
                         pull-patterns]

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

  ITuples
  (-init-tuples
    [db out a v-ranges pred get-v?]
    (ave-tuples store out a v-ranges pred get-v?))

  (-init-tuples-list
    [db a v-ranges pred get-v?]
    (wrap-cache
        store [:init-tuples a v-ranges pred get-v?]
      (ave-tuples-list store a v-ranges pred get-v?)))

  (-sample-init-tuples
    [db out a mcount v-ranges pred get-v?]
    (sample-ave-tuples store out a mcount v-ranges pred get-v?))

  (-sample-init-tuples-list
    [db a mcount v-ranges pred get-v?]
    (wrap-cache
        store [:sample-init-tuples a mcount v-ranges pred get-v?]
      (sample-ave-tuples-list store a mcount v-ranges pred get-v?)))

  (-e-sample
    [db a]
    (wrap-cache
        store [:e-sample a]
      (e-sample store a)))

  (-default-ratio
    [db a]
    (wrap-cache
        store [:default-ratio a]
      (default-ratio store a)))

  (-eav-scan-v
    [db in out eid-idx attrs-v]
    (eav-scan-v store in out eid-idx attrs-v))

  (-eav-scan-v-list
    [db in eid-idx attrs-v]
    (wrap-cache
        store [:eav-scan-v in eid-idx attrs-v]
      (eav-scan-v-list store in eid-idx attrs-v)))

  (-val-eq-scan-e
    [db in out v-idx attr]
    (val-eq-scan-e store in out v-idx attr))

  (-val-eq-scan-e-list
    [db in v-idx attr]
    (wrap-cache
        store [:val-eq-scan-e in v-idx attr]
      (val-eq-scan-e-list store in v-idx attr)))

  (-val-eq-scan-e
    [db in out v-idx attr bound]
    (val-eq-scan-e store in out v-idx attr bound))

  (-val-eq-scan-e-list
    [db in v-idx attr bound]
    (wrap-cache
        store [:val-eq-scan-e in v-idx attr bound]
      (val-eq-scan-e-list store in v-idx attr bound)))

  (-val-eq-filter-e
    [db in out v-idx attr f-idx]
    (val-eq-filter-e store in out v-idx attr f-idx))

  (-val-eq-filter-e-list
    [db in v-idx attr f-idx]
    (wrap-cache
        store [:val-eq-filter-e in v-idx attr f-idx]
      (val-eq-filter-e-list store in v-idx attr f-idx)))

  ISearch
  (-search
    [db pattern]
    (let [[e a v _] pattern]
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
           (mapv #(datom (aget ^objects % 0) a (aget ^objects % 1))
                 (ave-tuples-list
                   store a [[[:closed c/v0] [:closed c/vmax]]] nil true)) ; _ a _
           (slice-filter store :eav
                         (fn [^Datom d] (when ((s/vpred v) (.-v d)) d))
                         (datom e0 nil nil)
                         (datom emax nil nil)) ; _ _ v
           (slice store :eav (datom e0 nil nil) (datom emax nil nil))])))) ; _ _ _

  (-search-tuples
    [db pattern]
    (let [[e a v _] pattern]
      (wrap-cache
          store [:search-tuples e a v]
        (case-tree
          [e a (some? v)]
          [(when (populated? store :eav (d/datom e a v) (d/datom e a v))
             (rel/single-tuples (object-array [e a v]))) ; e a v
           (s/ea-tuples store e a) ; e a _
           (s/ev-tuples store e v)  ; e _ v
           (s/e-tuples store e) ; e _ _
           (s/av-tuples store a v) ; _ a v
           (s/a-tuples store a) ; _ a _
           (s/v-tuples store v) ; _ _ v
           (s/all-tuples store)])))) ; _ _ _

  (-first
    [db pattern]
    (let [[e a v _] pattern]
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
           (head store :eav (datom e0 nil nil) (datom emax nil nil))])))) ; _ _ _

  (-count
    [db pattern]
    (.-count db pattern nil))
  (-count
    [db pattern cap]
    (let [[e a v] pattern]
      (wrap-cache
          store [:count e a v cap]
        (case-tree
          [e a (some? v)]
          [(size store :eav (datom e a v) (datom e a v)) ; e a v
           (size store :eav (datom e a c/v0) (datom e a c/vmax)) ; e a _
           (size-filter store :eav
                        (fn [^Datom d] ((s/vpred v) (.-v d)))
                        (datom e nil nil) (datom e nil nil))  ; e _ v
           (e-size store e) ; e _ _
           (av-size store a v) ; _ a v
           (a-size store a) ; _ a _
           (v-size store v) ; _ _ v, for ref only
           (datom-count store :eav)])))) ; _ _ _

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
      (slice store index
             (components->pattern db index c1 c2 c3 e0 v0)
             (components->pattern db index c1 c2 c3 emax vmax))))
  (-datoms
    [db index c1 c2 c3 n]
    (wrap-cache
        store [:datoms index c1 c2 c3 n]
      (slice store index
             (components->pattern db index c1 c2 c3 e0 v0)
             (components->pattern db index c1 c2 c3 emax vmax)
             n)))

  (-e-datoms [db e] (wrap-cache store [:e-datoms e] (e-datoms store e)))

  (-av-datoms
    [db attr v]
    (wrap-cache store [:av-datoms attr v] (av-datoms store attr v)))

  (-range-datoms
    [db index start-datom end-datom]
    (wrap-cache
        store [:range-datoms index start-datom end-datom]
      (slice store index start-datom end-datom)))

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
    [db attr]
    (wrap-cache store [:cardinality attr]
      (cardinality store attr)))

  (-index-range
    [db attr start end]
    (wrap-cache
        store [:index-range attr start end]
      (do (vld/validate-attr attr (list '-index-range 'db attr start end))
          (slice store :ave (resolve-datom db nil attr start e0 v0)
                 (resolve-datom db nil attr end emax vmax)))))

  (-index-range-size
    [db attr start end]
    (wrap-cache
        store [:index-range-size attr start end]
      (av-range-size store attr start end))))

;; (defmethod print-method DB [^DB db, ^java.io.Writer w]
;;   (binding [*out* w]
;;     (let [{:keys [store eavt max-eid max-tx]} db]
;;       (pr {:db-name       (s/db-name store)
;;            :last-modified (s/last-modified store)
;;            :datom-count   (count eavt)
;;            :max-eid       max-eid
;;            :max-tx        max-tx}))))

(defn db?
  "Check if x is an instance of DB, also refresh its cache if it's stale.
  Often used in the :pre condition of a DB access function"
  [x]
  (when (-searchable? x)
    (let [store  (.-store ^DB x)
          cache  (.get ^ConcurrentHashMap caches (dir store))]
      (when (should-check-remote-cache? store cache)
        (if (instance? DatalogStore store)
          (let [{:keys [last-modified max-tx]} (r/db-info store)
                target        (long (or last-modified 0))
                cached-max-tx (cached-remote-cache-max-tx store)]
            (if (or (nil? cache)
                    (< ^long (.target ^LRUCache cache) ^long target)
                    (and (some? max-tx)
                         (some? cached-max-tx)
                         (< (long cached-max-tx)
                            (long max-tx))))
              (refresh-cache store target max-tx)
              (do
                (mark-remote-cache-max-tx! store max-tx)
                (mark-remote-cache-check! store))))
          (let [target (long (or (last-modified store) 0))]
            (mark-remote-cache-check! store)
            (when (or (nil? cache)
                      (< ^long (.target ^LRUCache cache) ^long target))
              (refresh-cache store target))))))
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

(defn- open-store
  [dir schema opts]
  (if (r/dtlv-uri? dir)
    (r/open dir schema opts)
    (s/open dir schema opts)))

(defn- split-runtime-opts
  [opts]
  (if (map? opts)
    [(dissoc opts :runtime-opts) (:runtime-opts opts)]
    [opts nil]))

(defn new-db
  ([^IStore store] (new-db store nil))
  ([^IStore store info]
   (let [info (or info
                  (when (instance? datalevin.remote.DatalogStore store)
                    (r/db-info store)))
         db   (map->DB
                {:store         store
                 :max-eid       (if info (:max-eid info) (init-max-eid store))
                 :max-tx        (if info (:max-tx info) (max-tx store))
                 :eavt          (TreeSortedSet. ^Comparator d/cmp-datoms-eavt)
                 :avet          (TreeSortedSet. ^Comparator d/cmp-datoms-avet)
                 :pull-patterns (LRUCache. 64)})]
     (swap! dbs assoc (db-name store) db)
     (ensure-cache store
                   (if info (:last-modified info) (last-modified store))
                   (when info (:max-tx info)))
     (start-sampling store)
     db)))

(defn transfer
  [^DB old store]
  (carry-runtime-opts
    ;; eavt/avet are mutable transaction-local overlays, not durable indexes.
    ;; A transferred DB view must get fresh caches so local write transactions
    ;; do not leak in-memory datoms across connection/transaction wrappers.
    (DB. store
         (.-max-eid old)
         (.-max-tx old)
         (TreeSortedSet. ^Comparator d/cmp-datoms-eavt)
         (TreeSortedSet. ^Comparator d/cmp-datoms-avet)
         (.-pull-patterns old))
    old))

(defn ^DB empty-db
  ([] (empty-db nil nil))
  ([dir] (empty-db dir nil))
  ([dir schema] (empty-db dir schema nil))
  ([dir schema opts]
   {:pre [(or (nil? schema) (map? schema))]}
   (vld/validate-schema schema)
   (let [[store-opts runtime-opts] (split-runtime-opts opts)]
     (cond-> (new-db (open-store dir schema store-opts))
       (some? runtime-opts) (with-runtime-opts runtime-opts)))))

(def coerce-inst prepare/coerce-inst)

(def coerce-uuid prepare/coerce-uuid)

(defn- type-coercion
  [vt v]
  (prepare/type-coercion vt v))

(defn- correct-datom*
  [^Datom datom v]
  (prepare/correct-datom* datom v))

(defn- correct-datom
  [store ^Datom datom]
  (prepare/correct-datom store datom))

(defn- correct-value
  [store a v]
  (prepare/correct-value store a v))

(defn- pour
  [store datoms]
  (doseq [batch (sequence (comp
                            (map #(correct-datom store %))
                            (partition-all c/*fill-db-batch-size*))
                          datoms)]
    (load-datoms store batch)))

(defn close-db [^DB db]
  (let [store ^IStore (.-store db)]
    (stop-sampling store)
    (remove-cache store)
    (swap! dbs dissoc (db-name store))
    (close store)
    nil))

(defn- with-bulk-load-direct-write-path
  [lmdb f]
  (if-let [info-v (kv-info lmdb)]
    (let [info @info-v]
      (if (true? (:wal? info))
        (let [had-rollout?  (contains? info :wal-rollout-mode)
              had-rollback? (contains? info :wal-rollback?)
              prev-rollout  (:wal-rollout-mode info)
              prev-rollback (:wal-rollback? info)]
          ;; Bulk loads prefer throughput over WAL append/replay guarantees.
          ;; Keep the override runtime-only and restore after loading.
          (vswap! info-v assoc :wal-rollout-mode :rollback :wal-rollback? true)
          (try
            (f)
            (finally
              (vswap! info-v
                      (fn [m]
                        (cond-> m
                          had-rollout?
                          (assoc :wal-rollout-mode prev-rollout)

                          (not had-rollout?)
                          (dissoc :wal-rollout-mode)

                          had-rollback?
                          (assoc :wal-rollback? prev-rollback)

                          (not had-rollback?)
                          (dissoc :wal-rollback?)))))))
        (f)))
    (f)))

(defn- quick-fill
  [^Store store datoms]
  (let [lmdb    (.-lmdb store)
        flags   (get-env-flags lmdb)
        nosync? (:nosync flags)]
    (set-env-flags lmdb #{:nosync} true)
    (try
      (with-bulk-load-direct-write-path
        lmdb
        #(l/with-transaction-kv [_ lmdb]
           (pour store datoms)))
      (finally
        (when-not nosync? (set-env-flags lmdb #{:nosync} false))))
    (sync lmdb)))

(defn ^DB init-db
  ([datoms] (init-db datoms nil nil nil))
  ([datoms dir] (init-db datoms dir nil nil))
  ([datoms dir schema] (init-db datoms dir schema nil))
  ([datoms dir schema opts]
   {:pre [(or (nil? schema) (map? schema))]}
   (vld/validate-datom-list datoms)
   (vld/validate-schema schema)
   (let [[store-opts runtime-opts] (split-runtime-opts opts)
         ^Store store              (open-store dir schema store-opts)]
     (quick-fill store datoms)
     (cond-> (new-db store)
       (some? runtime-opts) (with-runtime-opts runtime-opts)))))

(defn fill-db [db datoms] (quick-fill (.-store ^DB db) datoms) db)

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

(defn ^Boolean multi-value?
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

(defn- auto-tempid [] (txprep/auto-tempid))

(defn- auto-tempid? ^Boolean [x] (txprep/auto-tempid? x))

(defn- tempid?
  ^Boolean [x]
  (txprep/tempid? x))

(defn installed-udf-descriptor
  ([db target]
   (installed-udf-descriptor db nil target))
  ([db allowed target]
   (txprep/installed-udf-descriptor db allowed target)))

(defn- local-transact-tx-data
  ([initial-report initial-es tx-time]
   (local-transact-tx-data initial-report initial-es tx-time false))
  ([initial-report initial-es tx-time simulated?]
   (let [report (txexec/local-transact-tx-data
                  initial-report initial-es tx-time)]
     (when-not simulated?
       (commit-prepared-tx-data! (:db-after report) (:tx-data report) report))
     report)))

(defn- committed-client-op-response
  [report last-modified-ms]
  (let [tx-meta (:tx-meta report)
        client-op-id (:ha/client-op-id tx-meta)
        request-type (:ha/client-op-request-type tx-meta)
        request-hash (:ha/client-op-hash tx-meta)
        response-kind (:ha/client-op-response-kind tx-meta)
        db-after (:db-after report)]
    (when (and client-op-id request-type request-hash response-kind)
      (let [response
            ;; Persist replay payloads without Datom objects so reopening a DB
            ;; doesn't depend on the custom Datom Nippy reader being loaded.
            (cond-> {:tx-data (mapv (fn [datom]
                                      [(d/datom-e datom)
                                       (d/datom-a datom)
                                       (d/datom-v datom)
                                       (d/datom-tx datom)
                                       (d/datom-added datom)])
                                    (:tx-data report))
                     :tempids (assoc (:tempids report)
                                     :max-eid
                                     (:max-eid db-after))}
              (:new-attributes report)
              (assoc :new-attributes (:new-attributes report))

              (= response-kind hcop/tx-data+db-info-response-kind)
              (assoc :db-info {:max-eid       (:max-eid db-after)
                               :max-tx        (:max-tx db-after)
                               :last-modified last-modified-ms}))]
        (hcop/committed-record-tx
          client-op-id
          (hcop/committed-record request-type
                                 request-hash
                                 response-kind
                                 response))))))

(defn ^:no-doc commit-prepared-tx-data!
  "Persist already prepared tx-data to the given DB store."
  ([^DB db tx-data]
   (commit-prepared-tx-data! db tx-data nil))
  ([^DB db tx-data report]
  (let [store (.-store db)]
    (if (instance? Store store)
      (let [embedding-plan    (s/prepare-embedding-plan ^Store store tx-data)
            last-modified-ms  (System/currentTimeMillis)
            extra-kv-tx       (some-> report
                                      (committed-client-op-response
                                        last-modified-ms))
            commit-opts       (cond-> {:last-modified-ms last-modified-ms}
                                extra-kv-tx
                                (assoc :extra-kv-txs [extra-kv-tx]))]
        (s/load-datoms-with-plan! ^Store store tx-data embedding-plan
                                  commit-opts))
      (load-datoms store tx-data))
    (invalidate-cache store tx-data (last-modified store))
    db)))

(defn- remote-tx-result
  [res]
  (if (map? res)
    (let [{:keys [tx-data tempids new-attributes]} res]
      [tx-data (dissoc tempids :max-eid) (tempids :max-eid) new-attributes])
    (let [[tx-data tempids] (split-with datom? res)
          max-eid           (-> tempids last second)
          tempids           (into {} (butlast tempids))]
      [tx-data tempids max-eid nil])))

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

(defn- prepare-entities
  [^DB db entities tx-time]
  (txprep/prepare-entities db entities tx-time))

(def ^:private blind-write-unsupported
  ::blind-write-unsupported)

(defn- blind-write-attr-supported?
  [db attr props]
  (and props
       (keyword? attr)
       (not (contains? #{:db/id :db/created-at :db/updated-at} attr))
       (not= "db" (namespace attr))
       (not (reverse-ref? attr))
       (not (ref? db attr))
       (not (-is-attr? db attr :db/unique))
       (not (tuple-attr? db attr))
       (not (tuple-type? db attr))
       (not (tuple-types? db attr))
       (not (tuple-source? db attr))))

(defn- blind-write-values
  [db attr value]
  (let [values (txprep/maybe-wrap-multival db attr value)]
    (if (and (multi-value? db attr value)
             (not (and (coll? values) (not (map? values)))))
      blind-write-unsupported
      (reduce
        (fn [acc v]
          (cond
            (identical? acc blind-write-unsupported)
            blind-write-unsupported

            (or (map? v)
                (datom? v)
                (and (coll? v)
                     (not (bytes? v))))
            blind-write-unsupported

            :else
            (conj acc v)))
        []
        values))))

(defn- prepare-blind-write-entity
  [^DB db entity]
  (let [store (.-store db)
        entity-id (or (:db/id entity) (auto-tempid))]
    (when (or (nil? (:db/id entity))
              (tempid? (:db/id entity)))
      (let [prepared
            (reduce-kv
              (fn [acc attr raw-value]
                (if (or (identical? acc blind-write-unsupported)
                        (identical? attr :db/id))
                  acc
                  (let [props ((schema store) attr)]
                    (if-not (blind-write-attr-supported? db attr props)
                      blind-write-unsupported
                      (let [values (blind-write-values db attr raw-value)]
                        (if (identical? values blind-write-unsupported)
                          blind-write-unsupported
                          (reduce
                            (fn [pairs v]
                              (vld/validate-attr attr entity)
                              (vld/validate-val v entity)
                              (conj pairs [attr (correct-value store attr v)]))
                            acc
                            values)))))))
              []
              entity)]
        (when-not (identical? prepared blind-write-unsupported)
          {:tempid entity-id
           :attrs  prepared})))))

(defn ^:no-doc prepare-blind-local-tx
  [^DB db initial-es]
  (let [store (.-store db)
        auto-entity-time? (boolean (:auto-entity-time? (opts store)))
        entities
        (reduce
          (fn [acc entity]
            (if (map? entity)
              (if-let [prepared (prepare-blind-write-entity db entity)]
                (let [attrs (:attrs prepared)]
                  (if (or (seq attrs) auto-entity-time?)
                    (conj acc prepared)
                    acc))
                (reduced nil))
              (reduced nil)))
          []
          initial-es)]
    (when (and entities (seq entities))
      (->PreparedBlindTx (schema store)
                         (opts store)
                         auto-entity-time?
                         entities))))

(defn ^:no-doc blind-local-tx-valid?
  [^DB db ^PreparedBlindTx prepared]
  (let [store (.-store db)]
    (and (identical? (:schema-epoch prepared) (schema store))
         (identical? (:opts-epoch prepared) (opts store)))))

(defn ^:no-doc stamp-blind-local-tx
  [^DB db ^PreparedBlindTx prepared tx-meta]
  (let [tx-id              (inc (long (:max-tx db)))
        tx-time            (System/currentTimeMillis)
        auto-entity-time?  (:auto-entity-time? prepared)
        entities           (:entities prepared)
        [tx-data tempids max-eid]
        (reduce
          (fn [[tx-data tempids next-eid] {:keys [tempid attrs]}]
            (let [^long next-eid next-eid
                  eid         (unchecked-inc next-eid)
                  tx-data     (cond-> tx-data
                                true
                                (conj (datom eid :db/created-at tx-time tx-id))

                                auto-entity-time?
                                (conj (datom eid :db/updated-at tx-time tx-id)))
                  tx-data     (reduce
                                (fn [acc [attr value]]
                                  (conj acc (datom eid attr value tx-id)))
                                tx-data
                                attrs)
                  tempids     (cond-> tempids
                                (and tempid (not (auto-tempid? tempid)))
                                (assoc tempid eid))]
              [tx-data tempids eid]))
          [[] {} (long (:max-eid db))]
          entities)
        tempids            (assoc tempids :db/current-tx tx-id)
        info               {:max-eid max-eid
                            :max-tx  tx-id}
        db-after           (-> db
                               (assoc :max-eid max-eid)
                               (assoc :max-tx tx-id))
        report             (->TxReport db db-after tx-data tempids tx-meta)]
    [report info]))

(defn transact-tx-data
  [initial-report initial-es simulated?]
  (let [^DB db  (:db-before initial-report)
        store   (.-store db)
        tx-time (System/currentTimeMillis)]
    (if (instance? datalevin.remote.DatalogStore store)
      (try
        (let [txs                                    (sequence
                                                      (mapcat
                                                        expand-transactable-entity)
                                                      initial-es)
              res                                    (r/tx-data store txs simulated?)
              db-info                                (when (map? res) (:db-info res))
              res                                    (if db-info (dissoc res :db-info) res)
              [tx-data tempids max-eid new-attributes] (remote-tx-result res)]
          (let [info (resolved-remote-db-info
                       store db (or db-info {}) max-eid simulated?)]
            (when-not simulated?
              (invalidate-cache store
                                tx-data
                                (:last-modified info)
                                (:max-tx info)))
            (cond-> (assoc initial-report
                           :db-after (-> (carry-runtime-opts (new-db store info) db)
                                         (assoc :max-eid (:max-eid info))
                                       (#(if simulated?
                                           (update % :max-tx u/long-inc)
                                           %)))
                           :tx-data tx-data
                           :tempids tempids)
              (seq new-attributes) (assoc :new-attributes new-attributes))))
        (catch Exception e
          (throw e)))
      (let [entities (prepare-entities db initial-es tx-time)]
        (local-transact-tx-data initial-report entities tx-time simulated?)))))

(defn tx-data->simulated-report
  [db tx-data]
  {:pre [(db? db)]}
  (vld/validate-tx-data-shape tx-data)
  (let [initial-report (map->TxReport
                         {:db-before db
                          :db-after  db
                          :tx-data   []
                          :tempids   {}
                          :tx-meta   nil})]
    (transact-tx-data initial-report tx-data true)))

(defn abort-transact
  [conn]
  (let [s (.-store ^DB (deref conn))]
    (if (instance? DatalogStore s)
      (r/abort-transact s)
      (abort-transact-kv (.-lmdb ^Store s)))))

(defn datalog-index-cache-limit
  ([^DB db]
   (let [^Store store (.-store db)]
     (:cache-limit (opts store))))
  ([^DB db ^long n]
   (let [^Store store (.-store db)]
     (assoc-opt store :cache-limit n)
     (refresh-cache store (System/currentTimeMillis)))))
