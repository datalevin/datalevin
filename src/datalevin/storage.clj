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
  "Datalog Store implementation, transactions, workers, and resource lifecycle."
  (:refer-clojure :exclude [update assoc])
  (:require
   [clojure.string :as str]
   [datalevin.async :as a]
   [datalevin.binding.cpp :as cpp]
   [datalevin.bits :as b]
   [datalevin.buffer :as bf]
   [datalevin.constants :as c]
   [datalevin.custom-datalog :as cd]
   [datalevin.datom :as d]
   [datalevin.embedding :as emb]
   [datalevin.idoc :as idoc]
   [datalevin.index :as idx
    :refer [value-type datom->indexable index->dbi index->ktype index->vtype
            index->k index->v retrieved->v encode-giant-datom]]
   [datalevin.inline :refer [update assoc]]
   [datalevin.interface
    :refer [transact-kv get-range get-first get-value env-dir close-kv
            closed-kv? entries list-range list-range-first list-range-count
            list-count key-range-list-count key-range-count rschema
            list-range-first-n get-list list-range-filter-count max-aid
            list-range-some list-range-keep max-gt advance-max-gt max-tx
            open-list-dbi open-dbi attrs add-doc opts swap-attr add-vec
            remove-vec close-vecs vec-closed? schema closed? a-size db-name]]
   [datalevin.kv :as kv]
   [datalevin.lmdb :as lmdb :refer [IWriting]]
   [datalevin.pipe :as p]
   [datalevin.prepare :as prep]
   [datalevin.query.predicate :as qpred]
   [datalevin.relation :as r]
   [datalevin.remote :as remote]
   [datalevin.scan :as scan]
   [datalevin.secondary-index :as si]
   [datalevin.storage.domains
    :refer [embedding-attr-domains ensure-embedding-vector!
            init-embedding-indices init-embedding-providers init-engines
            init-idoc-domains init-idoc-indices init-indices
            init-store-domains transfer-engines transfer-idoc-indices
            transfer-indices]]
   [datalevin.storage.indexing :as indexing
    :refer [remove-fulltext-doc-idempotently!]]
   [datalevin.storage.jobs
    :refer [add-job-to-secondary-index-status claimable-secondary-index-job?
            claimed-secondary-index-job? embedding-job-item
            finalize-secondary-index-status secondary-index-job-matches?
            secondary-index-status-init]]
   [datalevin.storage.options :as options
    :refer [apply-option-mutations async-secondary-index-option-keys
            normalize-ha-open-opts raw-persist-open-opts-key
            resolve-store-opts store-visible-opts sync-wal-runtime-opts!
            transact-opts-raw]]
   [datalevin.storage.scan :as scans
    :refer [av-entities ave-filter-bound-id-chunk ave-filter-tuple-id-chunk
            ave-tuples-scan* ave-tuples-scan-need-v
            ave-tuples-scan-need-v-vpred ave-tuples-scan-no-v
            ave-tuples-scan-no-v-vpred datom-pred->kv-pred ea->avg-buffer
            eav-filter-presence-chunk eav-scan-v-list-chunk eav-scan-v-multi*
            eav-scan-v-single* group-counts group-starts kv->datom
            ordered-parallel-list-chunks parallel-scan-participant-count
            retrieved->attr retrieved->datom single-attrs? sort-tuples-by-eid
            sort-tuples-by-val sorted-distinct-tuple-values val-eq-filter-e*
            val-eq-scan-e* val-eq-scan-e-bound*
            ;; Resolved here by query-not-test in the sibling test project.
            #_{:clj-kondo/ignore [:unused-referred-var]}
            parallel-scan-participant-capacity]]
   [datalevin.storage.schema :as schemas
    :refer [init-attrs init-max-aid init-schema load-schema
            normalize-schema-renames populated-attr? prepare-schema-update
            resolve-renamed-schema-patches schema-rename-plans
            schema-update-required? transact-schema update-schema
            validate-schema-operations]]
   [datalevin.util :as u :refer [conjv raise]]
   [datalevin.validate :as vld]
   [datalevin.vector :as v])
  (:import
   [java.util List Collection HashMap IdentityHashMap UUID]
   [java.util.concurrent TimeUnit ScheduledExecutorService ConcurrentHashMap ScheduledFuture]
   [java.util.concurrent.locks ReentrantReadWriteLock]
   [java.nio ByteBuffer]
   [java.lang AutoCloseable]
   [org.eclipse.collections.impl.list.mutable FastList]
   [org.eclipse.collections.impl.list.mutable.primitive LongArrayList]
   [org.eclipse.collections.impl.map.mutable.primitive LongObjectHashMap]
   [datalevin.datom Datom]
   [datalevin.interface IStore]
   [datalevin.async IAsyncWork]
   [datalevin.bits Retrieved Indexable]
   [datalevin.lmdb DatomKVTxData]))

(declare with-open-opts close-store-resources! release-shared-local-store!
         enqueue-secondary-index-work! enqueue-secondary-index-work-if-needed!
         insert-datom delete-datom check load-datoms-with-plan!
         prepare-embedding-plan prepare-datoms-kv-plan commit-datoms-kv-plan!
         migrate-attr-values ->SamplingWork e-sample* default-ratio* analyze*
         apply-schema-update! transfer-current)

;; Retain the existing entry points for storage callers.
(def attr-tuples schemas/attr-tuples)
(def schema->rschema schemas/schema->rschema)
(def e-aid-v->datom scans/e-aid-v->datom)
(def fulltext-index indexing/fulltext-index)
(def vector-index indexing/vector-index)
(def embedding-index indexing/embedding-index)
(def idoc-index indexing/idoc-index)
(def vpred scans/vpred)

;; Retain interned helpers used by server code and sibling tests.
(def ^:private existing-store? options/existing-store?)
(def ^:private load-opts options/load-opts)
(def ^:private propagate-top-level-txlog-opts-to-kv-opts
  options/propagate-top-level-txlog-opts-to-kv-opts)
(def ^:private transact-opts options/transact-opts)

(def ^:dynamic ^:no-doc *enforce-blind-unique-inserts?* false)

(defonce ^:private shared-local-stores (atom {}))

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

(defn- ^:redef init-max-gt
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

(def ^:dynamic *parallel-list-scan?*
  "Whether a list storage operation may create its own parallel scan chunks."
  true)

(defn- scan-list-in-chunks
  ([lmdb ^List in f]
   (scan-list-in-chunks lmdb in (.size in) f))
  ([lmdb ^List in work-count f]
   (let [participants (if (or (lmdb/writing? lmdb)
                              (not *parallel-list-scan?*))
                        1
                        (parallel-scan-participant-count work-count))]
     (if (== 1 participants)
       (f in)
       (ordered-parallel-list-chunks in participants f)))))

(defn- eav-filter-presence-list*
  [lmdb ^List in eid-idx aid]
  (scan-list-in-chunks
    lmdb in
    (fn [chunk]
      (eav-filter-presence-chunk lmdb chunk eid-idx aid))))

(defn- ave-filter-bound-id-list*
  [lmdb ^List in value-idx aid value-type bound-id]
  (scan-list-in-chunks
    lmdb in (sorted-distinct-tuple-values in value-idx)
    (fn [chunk]
      (ave-filter-bound-id-chunk
        lmdb chunk value-idx aid value-type bound-id))))

(defn- ave-filter-tuple-id-list*
  [lmdb ^List in value-idx entity-idx aid value-type]
  (scan-list-in-chunks
    lmdb in
    (fn [chunk]
      (ave-filter-tuple-id-chunk
        lmdb chunk value-idx entity-idx aid value-type))))

(defprotocol IStateSync
  (mark-state-current! [this last-modified-ms])
  (observed-state-sync-ms [this])
  (ensure-current! [this])
  (sync-giant-id! [this]
    "Refresh the giant ID floor while holding the shared LMDB write lock."))

(defn maybe-ensure-current!
  [this]
  (if (satisfies? IStateSync this)
    (ensure-current! this)
    this))

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

  (observed-state-sync-ms [_] state-sync-ms)

  (sync-giant-id! [_]
    (set! max-gt (max (long max-gt) (long (init-max-gt lmdb)))))

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
          (vld/validate-schema (merge schema new-schema))
          (cd/validate-schema! lmdb (merge schema new-schema)))
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
    (cd/validate-schema! lmdb (prepare-schema-update schema schema-update))
    (cd/initialize! lmdb (prepare-schema-update schema schema-update))
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
          (raise "Cannot rename attribute: target already exists"
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
          (raise "Cannot rename missing attribute"
                   {:error     :schema/missing-attribute
                    :attribute attr
                    :target    new-attr})))))

  (datom-count [_ index]
    (entries lmdb (if (string? index) index (index->dbi index))))

  (load-datoms [this datoms]
    (load-datoms-with-plan! this datoms (prepare-embedding-plan this datoms)))

  (fetch [_ datom]
    (cd/with-snapshot lmdb
      (let [lk (index->k :eav lmdb schema datom false)
            hk (index->k :eav lmdb schema datom true)
            lv (index->v :eav lmdb schema datom false)
            hv (index->v :eav lmdb schema datom true)
            ds (mapv #(retrieved->datom lmdb attrs %)
                     (list-range lmdb c/eav [:closed lk hk] :id [:closed lv hv] :avg))]
        (if (and (some? (:v datom))
                 (cd/custom-type? (:db/valueType (schema (:a datom)))))
          (filterv #(= (:v datom) (:v %)) ds)
          ds))))

  (populated? [_ index low-datom high-datom]
    (let [^Datom low-datom  low-datom
          ^Datom high-datom high-datom
          e                 (.-e low-datom)
          a                 (.-a low-datom)]
      (if (and (identical? index :eav)
               (== e (.-e high-datom))
               (keyword? a)
               (= a (.-a high-datom))
               (identical? (.-v low-datom) c/v0)
               (identical? (.-v high-datom) c/vmax))
        (when (ea->avg-buffer schema lmdb e a) true)
        (let [lk (index->k index lmdb schema low-datom false)
              hk (index->k index lmdb schema high-datom true)
              lv (index->v index lmdb schema low-datom false)
              hv (index->v index lmdb schema high-datom true)]
          (list-range-first
            lmdb (index->dbi index)
            [:closed lk hk] (index->ktype index)
            [:closed lv hv] (index->vtype index))))))

  (size [_ index low-datom high-datom]
    (list-range-count
      lmdb (index->dbi index)
      [:closed
       (index->k index lmdb schema low-datom false)
       (index->k index lmdb schema high-datom true)] (index->ktype index)))

  (e-size [_ e] (list-count lmdb c/eav e :id))

  (a-size [this a]
    (if (:db/aid (schema a))
      (when-not (.closed? this)
        (key-range-list-count
          lmdb c/ave
          [:closed
           (datom->indexable lmdb schema (d/datom c/e0 a nil) false)
           (datom->indexable lmdb schema (d/datom c/emax a nil) true)] :avg))
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
                vt  (idx/storage-type lmdb props)]
            (+ ^long total
               ^long (list-count
                       lmdb c/ave (b/indexable nil aid v vt c/gmax) :avg)))
          total))
      0 schema))

  (av-size [_ a v]
    (if (cd/custom-type? (:db/valueType (schema a)))
      (count (av-entities lmdb schema a v))
      (list-count lmdb c/ave
                  (datom->indexable lmdb schema (d/datom c/e0 a v) false) :avg)))

  (av-range-size ^long [_ a lv hv]
    (key-range-list-count
      lmdb c/ave
      [:closed
       (datom->indexable lmdb schema (d/datom c/e0 a lv) false)
       (datom->indexable lmdb schema (d/datom c/emax a hv) true)]
      :avg))

  (cardinality [_ a]
    (if (:db/aid (schema a))
      (key-range-count
        lmdb c/ave
        [:closed
         (datom->indexable lmdb schema (d/datom c/e0 a nil) false)
         (datom->indexable lmdb schema (d/datom c/emax a nil) true)]
        :avg)
      0))

  (head [this index low-datom high-datom]
    (cd/with-snapshot lmdb
      (retrieved->datom lmdb attrs
                        (.populated? this index low-datom high-datom))))

  (tail [_ index high-datom low-datom]
    (cd/with-snapshot lmdb
      (retrieved->datom
        lmdb attrs
        (list-range-first
          lmdb (index->dbi index)
          [:closed-back (index->k index lmdb schema high-datom true)
           (index->k index lmdb schema low-datom false)] (index->ktype index)
          [:closed-back
           (index->v index lmdb schema high-datom true)
           (index->v index lmdb schema low-datom false)] (index->vtype index)))))

  (slice [_ index low-datom high-datom]
    (cd/with-snapshot lmdb
      (mapv #(retrieved->datom lmdb attrs %)
            (list-range
              lmdb (index->dbi index)
              [:closed (index->k index lmdb schema low-datom false)
               (index->k index lmdb schema high-datom true)] (index->ktype index)
              [:closed (index->v index lmdb schema low-datom false)
               (index->v index lmdb schema high-datom true)] (index->vtype index)))))
  (slice [_ index low-datom high-datom n]
    (cd/with-snapshot lmdb
      (mapv #(retrieved->datom lmdb attrs %)
            (scan/list-range-first-n
              lmdb (index->dbi index) n
              [:closed (index->k index lmdb schema low-datom false)
               (index->k index lmdb schema high-datom true)] (index->ktype index)
              [:closed (index->v index lmdb schema low-datom false)
               (index->v index lmdb schema high-datom true)] (index->vtype index)))))

  (rslice [_ index high-datom low-datom]
    (cd/with-snapshot lmdb
      (mapv #(retrieved->datom lmdb attrs %)
            (list-range
              lmdb (index->dbi index)
              [:closed-back (index->k index lmdb schema high-datom true)
               (index->k index lmdb schema low-datom false)] (index->ktype index)
              [:closed-back (index->v index lmdb schema high-datom true)
               (index->v index lmdb schema low-datom false)] (index->vtype index)))))
  (rslice [_ index high-datom low-datom n]
    (cd/with-snapshot lmdb
      (mapv #(retrieved->datom lmdb attrs %)
            (list-range-first-n
              lmdb (index->dbi index) n
              [:closed-back (index->k index lmdb schema high-datom true)
               (index->k index lmdb schema low-datom false)] (index->ktype index)
              [:closed-back(index->v index lmdb schema high-datom true)
               (index->v index lmdb schema low-datom false)] (index->vtype index)))))

  (e-datoms [_ e]
    (cd/with-snapshot lmdb
      (mapv #(kv->datom lmdb attrs e %)
            (get-list lmdb c/eav e :id :avg))))

  (e-first-datom [_ e]
    (cd/with-snapshot lmdb
      (when-let [avg (get-value lmdb c/eav e :id :avg true)]
        (kv->datom lmdb attrs e avg))))

  (av-datoms [_ a v]
    (mapv #(d/datom % a v) (av-entities lmdb schema a v)))

  (av-first-e [_ a v]
    (if (cd/custom-type? (:db/valueType (schema a)))
      (first (av-entities lmdb schema a v))
      (let [^Indexable i
            (datom->indexable lmdb schema (d/datom c/e0 a v) false)]
        (if (b/giant? i)
          ;; Giant AVE keys contain an allocated giant ID. Search the keys that
          ;; share the logical value's truncated prefix, then compare the value
          ;; loaded from the giants DB instead of assuming the first giant ID.
          (list-range-some
            lmdb c/ave
            (fn [kv]
              (let [^Retrieved r (b/read-buffer (lmdb/k kv) :avg)]
                (when (= v (retrieved->v lmdb r))
                  (b/read-buffer (lmdb/v kv) :id))))
            [:closed
             i
             (Indexable. nil (.-a i) v (.-f i) (.-b i) c/gmax)]
            :avg
            [:all]
            :id
            true)
          (get-value lmdb c/ave i :avg :id true)))))

  (av-first-datom [this a v]
    (when-let [e (.av-first-e this a v)] (d/datom e a v)))

  (ea-first-datom [_ e a]
    (cd/with-snapshot lmdb
      (when-let [bf (ea->avg-buffer schema lmdb e a)]
        (d/datom e a (idx/avg-buffer->v lmdb bf)))))

  (ea-first-v [_ e a]
    (cd/with-snapshot lmdb
      (when-let [bf (ea->avg-buffer schema lmdb e a)]
        (idx/avg-buffer->v lmdb bf))))

  (v-datoms [_ v]
    (mapcat
      (fn [[attr props]]
        (when (identical? (:db/valueType props) :db.type/ref)
          (let [aid (:db/aid props)
                vt  (idx/storage-type lmdb props)]
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
      [:closed (index->k index lmdb schema low-datom false)
       (index->k index lmdb schema high-datom true)] (index->ktype index)
      [:closed (index->v index lmdb schema low-datom false)
       (index->v index lmdb schema high-datom true)] (index->vtype index)
      true))

  (head-filter [_ index pred low-datom high-datom]
    (list-range-some
      lmdb (index->dbi index)
      (datom-pred->kv-pred lmdb attrs index pred)
      [:closed (index->k index lmdb schema low-datom false)
       (index->k index lmdb schema high-datom true)] (index->ktype index)
      [:closed (index->v index lmdb schema low-datom false)
       (index->v index lmdb schema high-datom true)] (index->vtype index)))

  (tail-filter [_ index pred high-datom low-datom]
    (list-range-some
      lmdb (index->dbi index)
      (datom-pred->kv-pred lmdb attrs index pred)
      [:closed-back (index->k index lmdb schema high-datom true)
       (index->k index lmdb schema low-datom false)] (index->ktype index)
      [:closed-back (index->v index lmdb schema high-datom true)
       (index->v index lmdb schema low-datom false)] (index->vtype index)))

  (slice-filter [_ index pred low-datom high-datom]
    (list-range-keep
      lmdb (index->dbi index)
      (datom-pred->kv-pred lmdb attrs index pred)
      [:closed (index->k index lmdb schema low-datom false)
       (index->k index lmdb schema high-datom true)] (index->ktype index)
      [:closed (index->v index lmdb schema low-datom false)
       (index->v index lmdb schema high-datom true)] (index->vtype index)))

  (rslice-filter [_ index pred high-datom low-datom]
    (list-range-keep
      lmdb (index->dbi index)
      (datom-pred->kv-pred lmdb attrs index pred)
      [:closed-back (index->k index lmdb schema high-datom true)
       (index->k index lmdb schema low-datom false)] (index->ktype index)
      [:closed-back (index->v index lmdb schema high-datom true)
       (index->v index lmdb schema low-datom false)] (index->vtype index)))

  (ave-tuples [store out attr val-range]
    (.ave-tuples store out attr val-range nil false nil))
  (ave-tuples [store out attr val-range vpred]
    (.ave-tuples store out attr val-range vpred false nil))
  (ave-tuples [store out attr val-range vpred get-v?]
    (.ave-tuples store out attr val-range vpred get-v? nil))
  (ave-tuples [_ out attr val-ranges vpred get-v? indices]
    (when-let [props (schema attr)]
      (let [aid (props :db/aid)
            vt  (idx/storage-type lmdb props)]
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
            has-fidx?   (boolean (some :fidx maps))
            cache-eids? (and (not has-fidx?)
                             (not-any? #(false? (:cache-eids? %)) maps))
            fidxs       (object-array (map :fidx maps))
            aids        (int-array aids)
            seen        (when cache-eids? (LongObjectHashMap.))
            dbi-name    c/eav]
        (scan/scan lmdb dbi-name
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
          (raise "Fail to eav-scan-v: " e
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
            maps      (mapv peek attrs-v)
            nvs       (count (remove :skip? maps))
            skips     (boolean-array (map :skip? maps))
            preds     (object-array (map :pred maps))
            parallel? (every? qpred/forkable-predicate? preds)
            has-fidx?   (boolean (some :fidx maps))
            cache-eids? (and (not has-fidx?)
                             (not-any? #(false? (:cache-eids? %)) maps))
            fidxs       (object-array (map :fidx maps))
            aids        (int-array aids)
            presence-only?
            (and (== 1 na)
                 (zero? nvs)
                 (aget ^booleans skips 0)
                 (nil? (aget ^objects preds 0))
                 (nil? (aget ^objects fidxs 0)))
            single?   (and (not presence-only?)
                           (single-attrs? schema attrs-v))
            gcounts   (when (and (not presence-only?) (not single?))
                        (int-array (group-counts aids)))
            gstarts   (when gcounts (group-starts gcounts))]
        (if presence-only?
          (eav-filter-presence-list*
            lmdb in eid-idx (long (aget ^ints aids 0)))
          (let [scan-chunk
                (fn [chunk]
                  (eav-scan-v-list-chunk
                    lmdb chunk eid-idx attrs-v single? na nvs aids preds fidxs
                    skips cache-eids? gstarts gcounts))]
            ;; Generated query predicates carry factories for independent
            ;; chunk-local instances. Opaque predicates stay serial.
            (if parallel?
              (scan-list-in-chunks lmdb in scan-chunk)
              (scan-chunk in)))))))

  (val-eq-scan-e [_ in out v-idx attr]
    (if attr
      (when-let [props (schema attr)]
        (let [vt       (idx/storage-type lmdb props)
              aid      (props :db/aid)
              seen     (HashMap.)
              dbi-name c/ave]
          (scan/scan lmdb dbi-name
            (with-open [^AutoCloseable iter
                        (lmdb/val-iterator
                          (lmdb/iterate-list-val-full dbi rtx cur))]
              (loop [^objects tuple (p/produce in)]
                (when tuple
                  (let [v (aget tuple v-idx)]
                    (val-eq-scan-e* lmdb iter out tuple seen aid v vt)
                    (recur (p/produce in))))))
            (raise "Fail to val-eq-scan-e: " e {:v-idx v-idx :attr attr}))))
      (loop []
        (when (p/produce in)
          (recur)))))

  (val-eq-scan-e-list [_ in v-idx attr]
    (when attr
      (when-let [props (schema attr)]
        (let [vt       (idx/storage-type lmdb props)
              aid      (props :db/aid)
              in       (sort-tuples-by-val in v-idx vt)
              nt       (.size ^List in)
              out      (FastList. (* 2 nt))
              seen     (HashMap. nt)
              dbi-name c/ave]
          (scan/scan lmdb dbi-name
            (with-open [^AutoCloseable iter
                        (lmdb/val-iterator
                          (lmdb/iterate-list-val-full dbi rtx cur))]
              (dotimes [i nt]
                (let [^objects tuple (.get ^List in i)
                      v              (aget tuple v-idx)]
                  (val-eq-scan-e* lmdb iter out tuple seen aid v vt))))
            (raise "Fail to val-eq-scan-e-list: " e {:v-idx v-idx :attr attr}))
          out))))

  (val-eq-scan-e [_ in out v-idx attr bound]
    (if attr
      (when-let [props (schema attr)]
        (let [vt       (idx/storage-type lmdb props)
              aid      (props :db/aid)
              dbi-name c/ave]
          (scan/scan lmdb dbi-name
            (loop [^objects tuple (p/produce in)]
              (when tuple
                (let [v (aget tuple v-idx)]
                  (val-eq-scan-e-bound*
                    lmdb rtx cur out tuple aid v vt bound)
                  (recur (p/produce in)))))
            (raise "Fail to val-eq-scan-e-bound: " e
                     {:v-idx v-idx :attr attr}))))
      (loop []
        (when (p/produce in)
          (recur)))))

  (val-eq-scan-e-list [_ in v-idx attr bound]
    (when attr
      (when-let [props (schema attr)]
        (let [vt       (idx/storage-type lmdb props)
              in       (sort-tuples-by-val in v-idx vt)
              aid      (props :db/aid)]
          (ave-filter-bound-id-list*
            lmdb in v-idx aid vt bound)))))

  (val-eq-filter-e [_ in out v-idx attr f-idx]
    (if attr
      (when-let [props (schema attr)]
        (let [vt       (idx/storage-type lmdb props)
              dbi-name c/ave
              aid      (props :db/aid)]
          (scan/scan lmdb dbi-name
            (loop [^objects tuple (p/produce in)]
              (when tuple
                (let [old-e (aget tuple f-idx)
                      v     (aget tuple v-idx)]
                  (val-eq-filter-e*
                    lmdb rtx cur out tuple aid v vt old-e)
                  (recur (p/produce in)))))
            (raise "Fail to val-eq-filter-e: " e
                     {:v-idx v-idx :attr attr}))))
      (loop []
        (when (p/produce in)
          (recur)))))

  (val-eq-filter-e-list [_ in v-idx attr f-idx]
    (when attr
      (when-let [props (schema attr)]
        (let [vt       (idx/storage-type lmdb props)
              in       (sort-tuples-by-val in v-idx vt)
              aid      (props :db/aid)]
          (ave-filter-tuple-id-list*
            lmdb in v-idx f-idx aid vt))))))

(defn- make-store
  "Build a Store from a field map, avoiding long positional constructor calls.
   Keys mirror the `Store` deftype fields."
  [{:keys [lmdb search-engines vector-indices embedding-indices idoc-indices
           embedding-providers counts opts schema rschema attrs max-aid max-gt
           max-tx state-sync-ms scheduled-sampling write-txn sampling-lock
           local-closed? shared-dir-key]}]
  (Store. lmdb search-engines vector-indices embedding-indices idoc-indices
          embedding-providers counts opts schema rschema attrs max-aid max-gt
          max-tx state-sync-ms scheduled-sampling write-txn sampling-lock
          local-closed? shared-dir-key))

(defn ^:no-doc ref-attr-adjacency
  "Scan a ref-valued AVE attribute directly into a primitive adjacency map.
   `bound-side` selects whether entity IDs or ref values are map keys."
  [^Store store attr bound-side]
  (let [props      ((schema store) attr)
        capacity   (int (min (long Integer/MAX_VALUE)
                             (max 16 (long (.cardinality store attr)))))
        adjacency  (LongObjectHashMap. capacity)
        entity-key? (= bound-side :e)]
    (when props
      (let [lmdb (.-lmdb store)
            aid  (:db/aid props)
            vt   (idx/storage-type lmdb props)]
        (ave-tuples-scan*
          lmdb aid vt [[[:closed c/v0] [:closed c/vmax]]] nil
          (fn [kv]
            (let [entity (long (.getLong ^ByteBuffer (lmdb/v kv) 0))
                  value  (long (idx/avg-buffer->v lmdb (lmdb/k kv)))
                  k      (if entity-key? entity value)
                  v      (if entity-key? value entity)
                  values ^LongArrayList (.get adjacency k)]
              (if values
                (.add values v)
                (let [values (LongArrayList.)]
                  (.add values v)
                  (.put adjacency k values))))))))
    adjacency))

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

(defn- plan-schema-update
  [^Store store schema-update del-attrs rename-map]
  (validate-schema-operations schema-update del-attrs rename-map)
  (let [schema-update (or schema-update {})
        deletions     (vec (set (or del-attrs [])))
        deletion-set (set deletions)
        renames       (normalize-schema-renames (or rename-map {}))
        endpoints     (concat (keys renames) (vals renames))]
    (when-let [attr (first (filter deletion-set (keys schema-update)))]
      (raise "Cannot patch and delete the same schema attribute"
               {:error     :schema/update-conflict
                :attribute attr}))
    (when-let [attr (first (filter deletion-set endpoints))]
      (raise "Cannot delete an attribute participating in a rename"
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
                        (raise "Schema rename cannot be applied"
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
          (cd/validate-schema! (.-lmdb store) final-schema)
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
        (raise "Schema update result differed from its validated plan"
                 {:error    :schema/update-conflict
                  :expected final-schema
                  :actual   result}))
      result)))

(defn- migrate-attr-values*
  [^Store store attr new-vt]
  (let [lmdb   (.-lmdb store)
        s      (schema store)
        props  (s attr)
        old-vt (idx/storage-type lmdb props)
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
          (raise "Cannot migrate attribute values to new type"
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
          (transact-kv lmdb txs))))))

(defn migrate-attr-values
  "Re-encode all datoms for `attr` from :data (untyped) to `new-vt`.
   Validates every value can be coerced first. Deletes old datoms with
   the old :data encoding, then inserts new datoms with the new typed
   encoding, all in a single atomic `transact-kv` call."
  [^Store store attr new-vt]
  (locking (.-write-txn store)
    (when-not (lmdb/writing? (.-lmdb store))
      (sync-giant-id! store))
    (migrate-attr-values* store attr new-vt)))

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

(defn- embedding-job-application
  [^Store store job]
  (let [domain (:job/domain job)
        ref (:job/ref job)
        index (or (embedding-index-by-domain store domain)
                  (raise "Embedding index is not initialized"
                           {:domain domain
                            :job job}))]
    (case (:job/op job)
      :add
      (let [provider (or (embedding-provider store domain)
                         (raise "Embedding provider is not initialized"
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

      (raise "Unsupported embedding secondary index op"
               {:op (:job/op job)
                :job job}))))

(defn- vector-job-application
  [^Store store job]
  (let [domain (:job/domain job)
        ref    (:job/ref job)
        index  (or ((.-vector-indices store) domain)
                   (raise "Vector index is not initialized"
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

      (raise "Unsupported vector secondary index op"
               {:op (:job/op job)
                :job job}))))

(defn- fulltext-job-application
  [^Store store job]
  (let [domain (:job/domain job)
        ref    (:job/ref job)
        engine (or ((.-search-engines store) domain)
                   (raise "Fulltext search engine is not initialized"
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

      (raise "Unsupported fulltext secondary index op"
               {:op (:job/op job)
                :job job}))))

(defn- secondary-index-job-application
  [^Store store job]
  (case (:job/type job)
    :fulltext (fulltext-job-application store job)
    :vector (vector-job-application store job)
    :embedding (embedding-job-application store job)
    (raise "Unsupported secondary index job type"
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
        ;; Fast local transactions enqueue this work while their outer LMDB
        ;; transaction is still open. Wait for that commit before looking for
        ;; jobs, otherwise an empty pre-commit read can consume the only wakeup.
        ;; Do not retain this monitor while processing: job claiming takes the
        ;; Store lock before writing LMDB, so retaining it would invert locks.
        (locking (lmdb/write-txn (.-lmdb store)) nil)
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
            (catch Throwable _))))))
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

(defn prepare-embedding-plan
  [^Store store datoms]
  ;; Most Datalog stores have no embedding domains. Avoid walking every datom
  ;; in every commit when there cannot be an embedding operation to prepare.
  (when (seq (.-embedding-indices store))
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
                                        (raise "Embedding provider is not initialized"
                                                 {:domain domain}))
                        dimensions (get-in (embedding-domain-config store domain)
                                           [:dimensions])
                        vectors    (emb/embedding provider
                                                  (mapv #(dissoc % :datom) items)
                                                  nil)]]
            (when-not (= (count items) (count vectors))
              (raise "Embedding provider returned the wrong number of vectors"
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
          plan)))))

(defn load-datoms-with-plan!
  ([^Store store datoms embedding-plan]
   (load-datoms-with-plan! store datoms embedding-plan nil))
  ([^Store store datoms embedding-plan {:keys [extra-kv-txs last-modified-ms]}]
   (let [[res secondary-index-job-count]
         (locking (.-write-txn store)
           ;; Transaction stores refresh when the write lock is acquired.
           ;; Direct writes acquire it here, before allocating any giant IDs.
           (when-not (lmdb/writing? (.-lmdb store))
             (sync-giant-id! store))
           (let [run (fn [tx-lmdb]
                       (let [plan (prepare-datoms-kv-plan store datoms embedding-plan
                                                          extra-kv-txs last-modified-ms)
                             res (commit-datoms-kv-plan!
                                  tx-lmdb (.-search-engines store)
                                  (.-vector-indices store) (.-embedding-indices store)
                                  (store-idoc-indices store) plan)]
                         [res (:secondary-index-job-count plan)]))]
             (if (cd/custom-schema? (schema store))
               (lmdb/with-transaction-kv [tx-lmdb (.-lmdb store)] (run tx-lmdb))
               (run (.-lmdb store)))))]
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
                      (idx/storage-type (.-lmdb store) props)
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
    (.add txs (DatomKVTxData.
                e
                (b/indexable-bytes i avg-bf)
                true
                (boolean
                  (and *enforce-blind-unique-inserts?*
                       (identical? (:db/unique props)
                                   :db.unique/identity)))))
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
      (let [domain (or (props :db/domain) (u/keyword->string attr))
            op     (if giant?
                     [:g [e aid max-gt v]]
                     [:a [e aid v]])
            patch  (some-> (meta d) :idoc/patch)
            op     (if patch (with-meta op {:idoc/patch patch}) op)]
        (.add id-ds [domain op])))
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
      (.add txs (DatomKVTxData. e (b/indexable-bytes ii avg-bf) false false))
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
       (doseq [^Datom datom datoms]
         (let [^objects ai (write-attr-info store attr-infos (.-a datom) (.-v datom)
                                           (d/datom-added datom))
               vt (aget ai 1)]
           (if-let [type (when (map? vt) (:custom/type vt))]
             ((if (d/datom-added datom) cd/put-datom! cd/delete-datom!)
              (lmdb/mark-write (.-lmdb store)) (.-e datom) (aget ai 2) type (.-v datom))
             (if (d/datom-added datom)
               (insert-datom store datom txs ft-ds vi-ds ft-jobs vi-jobs
                             em-ds em-jobs id-ds giants attr-infos embedding-plan
                             avg-bf)
               (delete-datom store datom txs ft-ds vi-ds ft-jobs vi-jobs em-ds
                             em-jobs id-ds giants attr-infos avg-bf)))))
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

(defn ea-tuples
  [^Store store e a]
  (cd/with-snapshot (.-lmdb store)
    (let [lmdb       (.-lmdb store)
          schema     (schema store)
          low-datom  (d/datom e a c/v0)
          high-datom (d/datom e a c/vmax)
          coll       (list-range
                       lmdb c/eav
                       [:closed (index->k :eav lmdb schema low-datom false)
                        (index->k :eav lmdb schema high-datom true)] :id
                       [:closed (index->v :eav lmdb schema low-datom false)
                        (index->v :eav lmdb schema high-datom true)] :avg)
          size       (.size ^Collection coll)
          res        (FastList. size)]
      (doseq [[_ r] coll]
        (.add res (object-array [(retrieved->v lmdb r)])))
      res)))

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
                     [:closed (index->k :eav lmdb schema low-datom false)
                      (index->k :eav lmdb schema high-datom true)] :id
                     [:closed (index->v :eav lmdb schema low-datom false)
                      (index->v :eav lmdb schema high-datom true)] :avg)
        size       (.size ^Collection coll)
        res        (FastList. size)]
    (doseq [attr coll] (.add res (object-array [attr])))
    res))

(defn e-tuples
  [^Store store e]
  (cd/with-snapshot (.-lmdb store)
    (let [lmdb  (.-lmdb store)
          attrs (attrs store)
          coll  (get-list lmdb c/eav e :id :avg)
          size  (.size ^Collection coll)
          res   (FastList. size)]
      (doseq [^Retrieved r coll]
        (.add res (object-array [(attrs (.-a r)) (retrieved->v lmdb r)])))
      res)))

(defn av-tuples
  [^Store store a v]
  (let [lmdb   (.-lmdb store)
        schema (schema store)
        coll   (av-entities lmdb schema a v)
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
                     [:closed (index->k :eav lmdb schema low-datom false)
                      (index->k :eav lmdb schema high-datom true)] :id
                     [:closed (index->v :eav lmdb schema low-datom false)
                      (index->v :eav lmdb schema high-datom true)] :avg)
        size       (.size ^Collection coll)
        res        (FastList. size)]
    (doseq [[e attr] coll] (.add res (object-array [e attr])))
    res))

(defn all-tuples
  [^Store store]
  (cd/with-snapshot (.-lmdb store)
    (let [lmdb       (.-lmdb store)
          schema     (schema store)
          attrs      (attrs store)
          low-datom  (d/datom c/e0 nil nil)
          high-datom (d/datom c/emax nil nil)
          coll       (list-range
                       lmdb c/eav
                       [:closed (index->k :eav lmdb schema low-datom false)
                        (index->k :eav lmdb schema high-datom true)] :id
                       [:closed (index->v :eav lmdb schema low-datom false)
                        (index->v :eav lmdb schema high-datom true)] :avg)
          size       (.size ^Collection coll)
          res        (FastList. size)]
      (doseq [[e r] coll]
        (.add res (object-array [e
                                 (retrieved->attr attrs r)
                                 (retrieved->v lmdb r)])))
      res)))

(defn- open-dbis
  [lmdb]
  ;; AVE duplicate values are fixed-width entity IDs. The binding keeps
  ;; DUPFIXED DBI values raw even when the environment compresses values.
  (open-list-dbi lmdb c/ave {:key-size c/+max-key-size+
                             :val-size c/+id-bytes+
                             :flags (conj c/default-dbi-flags :dupfixed)})
  (open-list-dbi lmdb c/eav {:key-size c/+id-bytes+
                             :val-size c/+max-key-size+})
  (open-dbi lmdb c/giants {:key-size c/+id-bytes+})
  (open-dbi lmdb c/ha-client-ops)
  (open-dbi lmdb c/meta {:key-size c/+max-key-size+})
  (open-dbi lmdb c/opts {:key-size c/+max-key-size+})
  (open-dbi lmdb c/schema {:key-size c/+max-key-size+})
  (open-dbi lmdb c/secondary-index-jobs {:key-size c/+max-key-size+}))

(defn- load-existing-store-opts
  [dir _kv-opts]
  (when (existing-store? dir)
    ;; Reuse persisted opts from an already-open handle when available, but do
    ;; not probe closed stores just to read them; the real open can load opts.
    (when-let [probe (or (some-> ^Store (current-shared-local-store dir) .-lmdb)
                         (datalevin.binding.cpp/open-local-kv-handle dir))]
      (open-dbis probe)
      (not-empty (load-opts probe)))))

(defn- close-failed-open!
  [dir shared-store lmdb]
  (when-not shared-store
    (try
      (if-let [^Store store (current-shared-local-store dir)]
        (if (identical? lmdb (.-lmdb store))
          (datalevin.interface/close store)
          (close-kv lmdb))
        (close-kv lmdb))
      (catch Throwable _))))

(defn- with-open-failure-cleanup
  [dir shared-store lmdb f]
  (try
    (f)
    (catch Throwable t
      (close-failed-open! dir shared-store lmdb)
      (throw t))))

(defn- attach-shared-store!
  [shared-store lmdb s-domains opts4 store-opts dir-key]
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
    (enqueue-secondary-index-work-if-needed! wrapper)))

(defn- create-new-store!
  [lmdb dir s-domains v-domains e-domains i-domains embedding-providers
   opts4 store-opts schema dir-key]
  (let [e-providers (init-embedding-providers dir e-domains
                                              embedding-providers)
        store       (make-store
                      {:lmdb lmdb
                       :search-engines
                       (init-engines lmdb s-domains (:runtime-opts opts4))
                       :vector-indices (init-indices lmdb v-domains)
                       :embedding-indices (init-embedding-indices lmdb e-domains)
                       :idoc-indices (init-idoc-indices lmdb i-domains)
                       :embedding-providers e-providers
                       :counts (ConcurrentHashMap.)
                       :opts store-opts
                       :schema schema
                       :rschema (schema->rschema schema)
                       :attrs (init-attrs schema)
                       :max-aid (init-max-aid schema)
                       :max-gt (init-max-gt lmdb)
                       :max-tx (init-max-tx lmdb)
                       :state-sync-ms (init-state-sync-ms lmdb)
                       :scheduled-sampling (volatile! nil)
                       ;; Keep allocation and commit under the same
                       ;; lock as explicit transactions and KV writes.
                       ;; A separate store mutex would invert the lock
                       ;; order when direct and explicit writes race.
                       :write-txn (lmdb/write-txn lmdb)
                       :sampling-lock (ReentrantReadWriteLock.)
                       :local-closed? false
                       :shared-dir-key dir-key})]
    ;; Upgrade composite tuple attributes after the Store exists so
    ;; legacy :data values can be re-encoded through set-schema.
    (datalevin.interface/set-schema store nil)
    (when dir-key
      (locking shared-local-stores
        (swap! shared-local-stores
               assoc dir-key {:store store :refs 1})))
    (cpp/register-shutdown-close!
      (kv/raw-lmdb lmdb)
      #(close-store-resources! store))
    (enqueue-secondary-index-work-if-needed! store)))

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
                  (lmdb/open-kv dir (cond-> kv-opts
                                     (:runtime-opts opts)
                                     (assoc :runtime-opts
                                            (:runtime-opts opts)))))]
     (with-open-failure-cleanup
       dir
       shared-store
       lmdb
       (fn []
         (open-dbis lmdb)
         (let [loaded-opts (when-not persisted-opts
                             (not-empty (load-opts lmdb)))
               opts3       (resolve-store-opts dir incoming-opts0 opts
                                               persisted-opts loaded-opts)
               raw-open-metadata? (or raw-persist-open-opts?
                                      (= :consensus-lease (:ha-mode opts3)))]
           (sync-wal-runtime-opts! lmdb opts3)
           (when (and (not opened-with-wal?)
                      (true? (:wal? opts3)))
             (kv/ensure-txlog-ready! lmdb))
           (let [schema (if shared-store
                          (datalevin.interface/set-schema shared-store schema)
                          (init-schema lmdb schema))
                 {:keys [s-domains v-domains e-domains i-domains opts4]}
                 (init-store-domains dir schema opts3
                                     search-opts search-domains
                                     vector-opts vector-domains
                                     embedding-opts embedding-domains
                                     embedding-providers)
                 store-opts (store-visible-opts opts4)
                 dir-key    (shared-local-store-key dir)]
             (if raw-open-metadata?
               (transact-opts-raw lmdb opts4)
               (transact-opts lmdb opts4))
             (ensure-open-last-modified! lmdb raw-open-metadata?)
             (if shared-store
               (attach-shared-store! shared-store lmdb s-domains opts4
                                     store-opts dir-key)
               (create-new-store! lmdb dir s-domains v-domains e-domains
                                  i-domains embedding-providers opts4
                                  store-opts schema dir-key)))))))))

(defn- transfer-with-schema
  [^Store old lmdb schema* reuse-derived-schema-state?]
  (let [writing?     (lmdb/writing? lmdb)
        ;; Refresh under the shared write lock on transaction entry.
        ;; Nested transfers carry the allocator forward.
        max-gt*      (if (and writing? (not (lmdb/writing? (.-lmdb old))))
                       (locking (lmdb/write-txn lmdb)
                         (max (long (max-gt old)) (long (init-max-gt lmdb))))
                       (max-gt old))
        opts*        (opts old)
        idoc-indices (transfer-idoc-indices (store-idoc-indices old) lmdb)
        idoc-indices (if writing?
                       idoc-indices
                       (merge-missing-idoc-indices
                         lmdb idoc-indices schema* opts*))]
    (make-store
      {:lmdb lmdb
       :search-engines (transfer-engines (.-search-engines old) lmdb)
       :vector-indices (transfer-indices (.-vector-indices old) lmdb)
       :embedding-indices (transfer-indices (.-embedding-indices old) lmdb)
       :idoc-indices idoc-indices
       :embedding-providers (.-embedding-providers old)
       :counts (.-counts old)
       :opts opts*
       :schema schema*
       :rschema (if reuse-derived-schema-state?
                  (rschema old)
                  (schema->rschema schema*))
       :attrs (if reuse-derived-schema-state?
                (attrs old)
                (init-attrs schema*))
       :max-aid (if reuse-derived-schema-state?
                  (max-aid old)
                  (init-max-aid schema*))
       :max-gt max-gt*
       :max-tx (max-tx old)
       :state-sync-ms (if reuse-derived-schema-state?
                        (observed-state-sync-ms old)
                        (init-state-sync-ms lmdb))
       :scheduled-sampling (.-scheduled-sampling old)
       :write-txn (.-write-txn old)
       ;; Sampling work may still be queued against an older Store wrapper.
       ;; Keep close/sampling coordination on a shared lock across wrappers
       ;; that refer to the same logical store/LMDB lifecycle.
       :sampling-lock (.-sampling-lock old)
       :local-closed? false
       :shared-dir-key (.-shared-dir-key old)})))

(defn transfer
  "transfer state of an existing store to a new store that has a different
  LMDB instance"
  [^Store old lmdb]
  ;; Ordinary transaction transfers preserve the exact in-memory schema.
  ;; Reuse its immutable reverse/aid maps and the already observed state
  ;; timestamp instead of rebuilding them (and rereading :last-modified) on
  ;; both sides of every write transaction.
  (transfer-with-schema old lmdb (schema old) true))

(defn- transfer-current
  "Transfer a Store while taking its schema from the LMDB transaction. The
  caller must hold the write lock so planning cannot race another schema
  mutation."
  [^Store old lmdb]
  (transfer-with-schema old lmdb (load-schema lmdb) false))

(defn with-open-opts
  "Return a Store wrapper over the same open LMDB state but with different
  in-memory opts. This does not persist opts back into LMDB."
  ([^Store old new-opts]
   (with-open-opts old new-opts nil))
  ([^Store old new-opts {:keys [search-engines vector-indices
                                embedding-indices idoc-indices
                                embedding-providers]}]
    (let [schema* (schema old)]
      (make-store
        {:lmdb (.-lmdb old)
         :search-engines (or search-engines (.-search-engines old))
         :vector-indices (or vector-indices (.-vector-indices old))
         :embedding-indices (or embedding-indices (.-embedding-indices old))
         :idoc-indices (or idoc-indices (store-idoc-indices old))
         :embedding-providers (or embedding-providers
                                  (.-embedding-providers old))
         :counts (.-counts old)
         :opts (store-visible-opts new-opts)
         :schema schema*
         :rschema (schema->rschema schema*)
         :attrs (init-attrs schema*)
         :max-aid (init-max-aid schema*)
         :max-gt (max-gt old)
         :max-tx (max-tx old)
         :state-sync-ms (init-state-sync-ms (.-lmdb old))
         :scheduled-sampling (.-scheduled-sampling old)
         :write-txn (.-write-txn old)
         :sampling-lock (.-sampling-lock old)
         :local-closed? false
         :shared-dir-key (.-shared-dir-key old)}))))

(defn- close-store-resources!
  [^Store this]
  (let [^ReentrantReadWriteLock sampling-lock (.-sampling-lock this)
        wlock (.writeLock sampling-lock)]
    (.lock wlock)
    (try
      (when-not (closed-kv? (.-lmdb this))
        (.stop-sampling this)
        (doseq [index (vals (.-vector-indices this))]
          (when-not (vec-closed? index)
            (close-vecs index)))
        (doseq [index (vals (.-embedding-indices this))]
          (when-not (vec-closed? index)
            (close-vecs index)))
        (doseq [provider (vals (.-embedding-providers this))]
          (emb/close-provider provider))
        (close-kv (.-lmdb this)))
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
