;;
;; Copyright (c) Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns ^:no-doc datalevin.overlay
  "WAL overlay helpers: in-memory env + delete maps + merged iterators."
  (:require
   [datalevin.binding.cpp :as cpp]
   [datalevin.bits :as b]
   [datalevin.buffer :as bf]
   [datalevin.constants :as c]
   [datalevin.interface :as i]
   [datalevin.lmdb :as l]
   [datalevin.util :as u])
  (:import
   [datalevin.binding.cpp DBI KV Rtx]
   [datalevin.cpp BufVal Dbi Txn]
   [java.lang AutoCloseable]
   [java.nio ByteBuffer BufferOverflowException]
   [java.util Arrays Collections HashSet Iterator NoSuchElementException]
   [java.util.concurrent ConcurrentHashMap]))

(defn- encode-bytes
  [x t]
  (let [t (or t :data)]
    (loop [^ByteBuffer buf (bf/get-tl-buffer)]
      (let [result (try
                     (b/put-bf buf x t)
                     (b/get-bytes buf)
                     (catch BufferOverflowException _
                       ::overflow))]
        (if (identical? result ::overflow)
          (let [new-buf (ByteBuffer/allocate (* 2 (.capacity buf)))]
            (bf/set-tl-buffer new-buf)
            (recur new-buf))
          result)))))

(defn range-forward?
  [k-range]
  (let [rt (first k-range)]
    (not (and rt (.endsWith (name rt) "-back")))))

(defn- normalize-ops-k-bytes
  [op]
  (or (:k-bytes op)
      (when (bytes? (:k op)) (:k op))
      (encode-bytes (:k op) (:kt op))))

(defn- normalize-ops-v-bytes
  [op]
  (let [v  (:v op)
        vt (:vt op)]
    (cond
      (contains? op :v-bytes) (:v-bytes op)
      (and (:raw? op) (bytes? v)) v
      :else (encode-bytes v vt))))

(defn- normalize-ops-vs-bytes
  [op]
  (let [v        (:v op)
        vt       (:vt op)
        v-bytes  (:v-bytes op)
        vs       (cond
                   v-bytes v-bytes
                   (and (:raw? op) (bytes? v)) (b/deserialize v)
                   :else v)]
    (if (sequential? vs)
      (if v-bytes
        vs
        (mapv #(encode-bytes % vt) vs))
      (u/raise "List values must be a sequential collection"
               {:error :overlay/invalid-list :value vs}))))

(deftype KeyBytes [^bytes k]
  Object
  (hashCode [_] (Arrays/hashCode k))
  (equals [_ other]
    (and (instance? KeyBytes other)
         (Arrays/equals k (.-k ^KeyBytes other)))))

(deftype KeyValBytes [^bytes k ^bytes v]
  Object
  (hashCode [_]
    (let [kh (Arrays/hashCode k)
          vh (Arrays/hashCode v)]
      (unchecked-add-int (unchecked-multiply-int 31 kh) vh)))
  (equals [_ other]
    (and (instance? KeyValBytes other)
         (Arrays/equals k (.-k ^KeyValBytes other))
         (Arrays/equals v (.-v ^KeyValBytes other)))))

(defn- ->key-bytes
  ^KeyBytes [^bytes k]
  (KeyBytes. k))

(defn- ->keyval-bytes
  ^KeyValBytes [^bytes k ^bytes v]
  (KeyValBytes. k v))

(defn- key-bytes
  ^bytes [^KeyBytes kb]
  (.-k kb))

(defn- keyval-key
  ^bytes [^KeyValBytes kvb]
  (.-k kvb))

(defn- keyval-val
  ^bytes [^KeyValBytes kvb]
  (.-v kvb))

(defn- get-delete-set
  [overlay dbi-name]
  (when overlay
    (.get ^ConcurrentHashMap (:deletes overlay) dbi-name)))

(defn- ensure-delete-set
  [overlay dbi-name]
  (let [^ConcurrentHashMap deletes (:deletes overlay)]
    (or (.get deletes dbi-name)
        (let [created (Collections/synchronizedSet (HashSet.))
              existing (.putIfAbsent deletes dbi-name created)]
          (or existing created)))))

(defn- remove-keyvals-for-key!
  [^java.util.Set deletes ^bytes k]
  (let [it (.iterator deletes)]
    (while (.hasNext it)
      (let [entry (.next it)]
        (when (instance? KeyValBytes entry)
          (when (Arrays/equals k (keyval-key entry))
            (.remove it)))))))

(defn- mark-key-deleted!
  [^java.util.Set deletes ^bytes k]
  (.add deletes (->key-bytes k)))

(defn- unmark-key-deleted!
  [^java.util.Set deletes ^bytes k]
  (.remove deletes (->key-bytes k)))

(defn- mark-keyval-deleted!
  [^java.util.Set deletes ^bytes k ^bytes v]
  (.add deletes (->keyval-bytes k v)))

(defn- unmark-keyval-deleted!
  [^java.util.Set deletes ^bytes k ^bytes v]
  (.remove deletes (->keyval-bytes k v)))

(defn- overlay-env-opts
  [base]
  (let [opts         (i/env-opts base)
        base-mapsize (or (:mapsize opts) c/*init-db-size*)
        mapsize      (min base-mapsize c/*overlay-init-db-size*)
        flags        (-> (or (:flags opts) c/default-env-flags)
                         set
                         (disj :rdonly-env)
                         (conj :inmemory :nosync))]
    {:flags       flags
     :mapsize     mapsize
     :max-readers (or (:max-readers opts) c/*max-readers*)
     :max-dbs     (or (:max-dbs opts) c/*max-dbs*)
     :kv-wal?     false
     :temp?       true}))

(defn create-overlay
  [base]
  (let [env (cpp/open-cpp-kv nil (overlay-env-opts base))]
    ;; Temp/inmemory envs skip set-max-val-size in open-cpp-kv, so allocate
    ;; the write buffer explicitly to avoid NPEs on open-transact-kv.
    (i/set-max-val-size env (i/max-val-size base))
    (vswap! (l/kv-info env) assoc :max-val-size-changed? false)
    {:env          env
     :dbis         (ConcurrentHashMap.)
     :dbi-opts     (ConcurrentHashMap.)
     :deletes      (ConcurrentHashMap.)}))

(defn close-overlay!
  [overlay]
  (when-let [env (:env overlay)]
    (i/close-kv env))
  nil)

(defn reset-deletes!
  [overlay]
  (when overlay
    (.clear ^ConcurrentHashMap (:deletes overlay)))
  overlay)

(defn reset-overlay!
  [overlay]
  (when overlay
    (let [env (:env overlay)
          ^ConcurrentHashMap dbis (:dbis overlay)
          dbi-names (or (seq (.keySet dbis)) (i/list-dbis env))]
      (doseq [dbi-name dbi-names]
        (try
          (i/clear-dbi env dbi-name)
          (catch Exception _ nil)))
      (reset-deletes! overlay)))
  overlay)

(defn delete-view
  [overlay dbi-name]
  (when-let [deletes (get-delete-set overlay dbi-name)]
    {:deletes deletes}))

(defn key-deleted?
  [overlay dbi-name ^bytes k-bytes]
  (when-let [^java.util.Set deletes (get-delete-set overlay dbi-name)]
    (.contains deletes (->key-bytes k-bytes))))

(defn list-deleted?
  [overlay dbi-name ^bytes k-bytes ^bytes v-bytes]
  (when-let [^java.util.Set deletes (get-delete-set overlay dbi-name)]
    (or (.contains deletes (->key-bytes k-bytes))
        (.contains deletes (->keyval-bytes k-bytes v-bytes)))))

(defn- base-dbi-opts
  [base dbi-name]
  (if (= dbi-name c/kv-info)
    {:flags #{} :validate-data? false}
    (or (i/dbi-opts base dbi-name)
        (binding [c/*bypass-wal* true]
          (i/get-value base c/kv-info [:dbis dbi-name]
                       [:keyword :string] :data)))))

(defn overlay-dbi-opts
  [overlay dbi-name]
  (when-let [^ConcurrentHashMap dbi-opts (:dbi-opts overlay)]
    (.get dbi-opts dbi-name)))

(defn set-dbi-opts!
  [overlay dbi-name opts]
  (when-let [^ConcurrentHashMap dbi-opts (:dbi-opts overlay)]
    (.put dbi-opts dbi-name opts))
  overlay)

(defn- remove-dbi-opts!
  [overlay dbi-name]
  (when-let [^ConcurrentHashMap dbi-opts (:dbi-opts overlay)]
    (.remove dbi-opts dbi-name))
  overlay)

(defn drop-dbi!
  [overlay dbi-name]
  (when overlay
    (let [env  (:env overlay)
          ^ConcurrentHashMap dbis (:dbis overlay)]
      (try
        (i/drop-dbi env dbi-name)
        (catch Exception _ nil))
      (.remove dbis dbi-name)
      (remove-dbi-opts! overlay dbi-name)
      (when-let [^ConcurrentHashMap deletes (:deletes overlay)]
        (.remove deletes dbi-name))))
  overlay)

(defn clear-dbi!
  [overlay dbi-name]
  (when overlay
    (when-let [env (:env overlay)]
      (try
        (i/clear-dbi env dbi-name)
        (catch Exception _ nil)))
    (when-let [^ConcurrentHashMap deletes (:deletes overlay)]
      (.remove deletes dbi-name)))
  overlay)

(defn ensure-dbi!
  [overlay base dbi-name]
  (let [^ConcurrentHashMap dbis (:dbis overlay)
        env                (:env overlay)]
    (or (.get dbis dbi-name)
        (when-let [dbi (try
                         (i/get-dbi env dbi-name false)
                         (catch Exception _ nil))]
          (.put dbis dbi-name dbi)
          dbi)
        (let [opts (or (overlay-dbi-opts overlay dbi-name)
                       (base-dbi-opts base dbi-name))]
          (when-not opts
            (u/raise (str dbi-name " is not open") {}))
          (let [opts (assoc opts :validate-data? false)
                list? (boolean (some #{:dupsort} (:flags opts)))
                dbi (if list?
                      (i/open-list-dbi env dbi-name opts)
                      (i/open-dbi env dbi-name opts))]
            (.put dbis dbi-name dbi)
            dbi)))))

(defn get-value
  [overlay base dbi-name k k-type v-type ignore-key?]
  (ensure-dbi! overlay base dbi-name)
  (let [env (or (:wenv overlay) (:env overlay))
        wtxn (when (l/writing? env)
               (when-let [wref (l/write-txn env)]
                 @wref))
        rtx (or wtxn (i/get-rtx env))
        own-rtx? (nil? wtxn)
        dbi (i/get-dbi env dbi-name false)]
    (try
      (l/put-key rtx k k-type)
      (when-let [^ByteBuffer bb (l/get-kv dbi rtx)]
        (if ignore-key?
          (b/read-buffer bb v-type)
          [(b/expected-return k k-type) (b/read-buffer bb v-type)]))
      (catch Throwable e
        (u/raise "Fail to get-value: " e
                 {:dbi dbi-name :k k :k-type k-type :v-type v-type}))
      (finally
        (when own-rtx?
          (i/return-rtx env rtx))))))

(defn apply-delete-ops!
  [overlay ops]
  (doseq [op ops]
    (let [op-type (:op op)
          dbi-name (:dbi op)
          list?    (boolean (:dupsort? op))
          k-bytes  (normalize-ops-k-bytes op)
          deletes  (ensure-delete-set overlay dbi-name)]
      (case op-type
        :put
        (do
          ;; For dupsort/list DBIs, keep key tombstone once set so a delete+put
          ;; in WAL tail still suppresses older base values for the same key.
          (when-not list?
            (unmark-key-deleted! deletes k-bytes))
          (when list?
            (when-let [v-bytes (normalize-ops-v-bytes op)]
              (unmark-keyval-deleted! deletes k-bytes v-bytes))))

        :del
        (do
          (mark-key-deleted! deletes k-bytes)
          (when list?
            (remove-keyvals-for-key! deletes k-bytes)))

        :put-list
        (let [vs-bytes (normalize-ops-vs-bytes op)]
          (doseq [v-bytes vs-bytes]
            (unmark-keyval-deleted! deletes k-bytes v-bytes)))

        :del-list
        (let [vs-bytes (normalize-ops-vs-bytes op)]
          (when-not (.contains deletes (->key-bytes k-bytes))
            (doseq [v-bytes vs-bytes]
              (mark-keyval-deleted! deletes k-bytes v-bytes))))

        (u/raise "Unsupported overlay op" {:error :overlay/invalid-op
                                           :op op-type}))))
  overlay)

(defn apply-ops!
  [overlay base ops]
  (let [env (:env overlay)]
    (doseq [op ops]
      (let [op-type  (:op op)
            dbi-name (:dbi op)
            k-bytes  (normalize-ops-k-bytes op)]
        (when (= dbi-name c/kv-info)
          (let [k* (if (:raw? op)
                     (b/read-buffer (ByteBuffer/wrap ^bytes k-bytes) (:kt op))
                     (:k op))]
            (when (and (vector? k*) (= 2 (count k*)) (= :dbis (first k*)))
              (let [dbi-name* (second k*)]
                (case op-type
                  :put (let [v* (if (:raw? op)
                                  (b/read-buffer (ByteBuffer/wrap
                                                   ^bytes (normalize-ops-v-bytes op))
                                                 (:vt op))
                                  (:v op))]
                         (when (map? v*)
                           (set-dbi-opts! overlay dbi-name* v*)))
                  :del (remove-dbi-opts! overlay dbi-name*)
                  nil)))))
        (ensure-dbi! overlay base dbi-name)
        (case op-type
          :put
          (let [v-bytes (normalize-ops-v-bytes op)
                tx      (l/kv-tx :put dbi-name k-bytes v-bytes :raw :raw
                                 (:flags op))]
            (i/transact-kv env [tx]))

          :del
          (i/transact-kv env [(l/kv-tx :del dbi-name k-bytes :raw)])

          :put-list
          (let [vs-bytes (normalize-ops-vs-bytes op)
                tx       (l/kv-tx :put-list dbi-name k-bytes vs-bytes
                                  :raw :raw (:flags op))]
            (i/transact-kv env [tx]))

          :del-list
          (let [vs-bytes (normalize-ops-vs-bytes op)
                tx       (l/kv-tx :del-list dbi-name k-bytes vs-bytes
                                  :raw :raw (:flags op))]
            (i/transact-kv env [tx]))

          (u/raise "Unsupported overlay op" {:error :overlay/invalid-op
                                             :op op-type})))))
  (apply-delete-ops! overlay ops)
  overlay)

(defn- dbi-dupsort?
  [base dbi-name]
  (boolean (some #{:dupsort} (:flags (base-dbi-opts base dbi-name)))))

(declare bb->bytes)

(defn- copy-private-dbi!
  [^DBI priv-dbi ^Rtx priv-rtx ^DBI comm-dbi ^Txn comm-txn list?
   ^java.util.Set comm-deletes]
  (let [cur      (l/get-cursor priv-dbi priv-rtx)
        iterable (if list?
                   (l/iterate-list priv-dbi priv-rtx cur [:all] :raw [:all] :raw)
                   (l/iterate-kv priv-dbi priv-rtx cur [:all] :raw :raw))
        iter     (.iterator ^Iterable iterable)
        ^Dbi tgt (l/dbi comm-dbi)]
    (try
      (while (.hasNext ^Iterator iter)
        (let [^KV kv (.next ^Iterator iter)
              ^BufVal k (.-kp kv)
              ^BufVal v (.-vp kv)]
          (.put tgt comm-txn k v 0)
          (when comm-deletes
            (let [k-bytes (bb->bytes (l/k kv))]
              (when list?
                (unmark-keyval-deleted! comm-deletes k-bytes
                                        (bb->bytes (l/v kv))))))))
      (finally
        (when (instance? AutoCloseable iter)
          (.close ^AutoCloseable iter))
        (if (l/read-only? priv-rtx)
          (l/return-cursor priv-dbi cur)
          (l/close-cursor priv-dbi cur))))))

(defn- apply-delete-set-to-env!
  [dbi ^java.util.Set deletes ^Txn txn]
  (when (and deletes (not (.isEmpty deletes)))
    (doseq [entry deletes]
      (cond
        (instance? KeyBytes entry)
        (let [k-bytes (key-bytes entry)]
          (l/put-key dbi k-bytes :raw)
          (l/del dbi txn true))

        (instance? KeyValBytes entry)
        (let [k-bytes (keyval-key entry)
              v-bytes (keyval-val entry)]
          (l/put-key dbi k-bytes :raw)
          (l/put-val dbi v-bytes :raw)
          (l/del dbi txn false))

        :else nil))))

(defn- apply-delete-set-to-map!
  [^java.util.Set comm-deletes ^java.util.Set priv-deletes]
  (doseq [entry priv-deletes]
    (cond
      (instance? KeyBytes entry)
      (let [k-bytes (key-bytes entry)]
        (mark-key-deleted! comm-deletes k-bytes)
        (remove-keyvals-for-key! comm-deletes k-bytes))

      (instance? KeyValBytes entry)
      (let [k-bytes (keyval-key entry)
            v-bytes (keyval-val entry)]
        (when-not (.contains comm-deletes (->key-bytes k-bytes))
          (mark-keyval-deleted! comm-deletes k-bytes v-bytes)))

      :else nil)))

(defn merge-private->committed!
  [private committed base ^Rtx priv-rtx]
  (when (and private committed)
    (let [comm-env  (:env committed)
          priv-env  (:env private)
          dbi-names (or (seq (.keySet ^ConcurrentHashMap (:dbis private)))
                        (i/list-dbis priv-env))
          dbi-states (mapv
                       (fn [dbi-name]
                         (let [list?    (dbi-dupsort? base dbi-name)
                               priv-dbi (ensure-dbi! private base dbi-name)
                               comm-dbi (ensure-dbi! committed base dbi-name)
                               priv-del (get-delete-set private dbi-name)
                               comm-del (or (get-delete-set committed dbi-name)
                                            (when priv-del
                                              (ensure-delete-set committed
                                                                 dbi-name)))]
                           {:dbi-name dbi-name
                            :list? list?
                            :priv-dbi priv-dbi
                            :comm-dbi comm-dbi
                            :priv-del priv-del
                            :comm-del comm-del}))
                       dbi-names)]
      (locking (l/write-txn comm-env)
        (i/open-transact-kv comm-env)
        (let [comm-rtx @(l/write-txn comm-env)
              ^Txn comm-txn (l/txn comm-rtx)]
          (try
            (doseq [{:keys [list? priv-dbi comm-dbi priv-del comm-del]}
                    dbi-states]
              (when priv-del
                (apply-delete-set-to-env! comm-dbi priv-del comm-txn))
              (copy-private-dbi! priv-dbi priv-rtx comm-dbi comm-txn list?
                                 comm-del)
              (when (and priv-del comm-del)
                (apply-delete-set-to-map! comm-del priv-del)))
            (i/close-transact-kv comm-env)
            (catch Exception e
              (try
                (i/abort-transact-kv comm-env)
                (catch Exception _ nil))
              (try
                (i/close-transact-kv comm-env)
                (catch Exception _ nil))
              (throw e)))))))
  committed)

(defn prune-committed-overlay!
  [committed base ops]
  (when (and committed (seq ops))
    (let [env (:env committed)
          dbi-map (reduce
                    (fn [m {:keys [dbi]}]
                      (if (contains? m dbi)
                        m
                        (assoc m dbi (ensure-dbi! committed base dbi))))
                    {}
                    ops)]
      (locking (l/write-txn env)
        (i/open-transact-kv env)
        (let [rtx @(l/write-txn env)
              ^Txn txn (l/txn rtx)]
          (try
            (doseq [op ops]
              (let [op-type (:op op)
                    dbi-name (:dbi op)
                    k-bytes  (normalize-ops-k-bytes op)
                    dbi      (get dbi-map dbi-name)
                    deletes  (get-delete-set committed dbi-name)]
                (case op-type
                  :put
                  (when-let [v-bytes (normalize-ops-v-bytes op)]
                    (l/put-key dbi k-bytes :raw)
                    (l/put-val dbi v-bytes :raw)
                    (l/del dbi txn false))

                  :put-list
                  (let [vs-bytes (normalize-ops-vs-bytes op)]
                    (doseq [v-bytes vs-bytes]
                      (l/put-key dbi k-bytes :raw)
                      (l/put-val dbi v-bytes :raw)
                      (l/del dbi txn false)))

                  :del
                  (when deletes
                    (unmark-key-deleted! deletes k-bytes)
                    (remove-keyvals-for-key! deletes k-bytes))

                  :del-list
                  (when deletes
                    (doseq [v-bytes (normalize-ops-vs-bytes op)]
                      (unmark-keyval-deleted! deletes k-bytes v-bytes)))

                  nil)))
            (i/close-transact-kv env)
            (catch Exception e
              (try
                (i/abort-transact-kv env)
                (catch Exception _ nil))
              (try
                (i/close-transact-kv env)
                (catch Exception _ nil))
              (throw e)))))))
  committed)

(defn entry-count
  [overlay]
  (let [env (:env overlay)]
    (try
      (reduce
        (fn [^long acc dbi-name]
          (+ acc (long (i/entries env dbi-name))))
        0
        (i/list-dbis env))
      (catch Exception _
        0))))

(defn- overlay-iterable
  [overlay base dbi-name iter-fn & args]
  (when overlay
    (ensure-dbi! overlay base dbi-name)
    (let [env (or (:wenv overlay) (:env overlay))]
      (reify Iterable
        (iterator [_]
          (let [dbi (i/get-dbi env dbi-name false)
                wtxn (when (l/writing? env)
                       (when-let [wref (l/write-txn env)]
                         @wref))
                rtx (or wtxn (i/get-rtx env))
                own-rtx? (nil? wtxn)
                cur (l/get-cursor dbi rtx)
                iter (.iterator ^Iterable (apply iter-fn dbi rtx cur args))]
            (reify
              Iterator
              (hasNext [_] (.hasNext ^Iterator iter))
              (next [_] (.next ^Iterator iter))
              AutoCloseable
              (close [_]
                (when (instance? AutoCloseable iter)
                  (.close ^AutoCloseable iter))
                (if (l/read-only? rtx)
                  (l/return-cursor dbi cur)
                  (l/close-cursor dbi cur))
                (when own-rtx?
                  (i/return-rtx env rtx))))))))))

(defn iter-key
  [overlay base dbi-name k-range k-type]
  (overlay-iterable overlay base dbi-name l/iterate-key k-range k-type))

(defn iter-kv
  [overlay base dbi-name k-range k-type v-type]
  (overlay-iterable overlay base dbi-name l/iterate-kv k-range k-type v-type))

(defn iter-list
  [overlay base dbi-name k-range k-type v-range v-type]
  (overlay-iterable overlay base dbi-name l/iterate-list
                    k-range k-type v-range v-type))

(defn iter-list-key-range-val-full
  [overlay base dbi-name k-range k-type]
  (overlay-iterable overlay base dbi-name
                    l/iterate-list-key-range-val-full k-range k-type))

(definterface ISourceState
  (^Object kv [])
  (^void setKv [^Object v])
  (^java.util.Iterator iter [])
  (^long priority []))

(deftype SourceState [iter ^:unsynchronized-mutable kv ^long priority]
  ISourceState
  (kv [_] kv)
  (setKv [_ v] (set! kv v))
  (iter [_] iter)
  (priority [_] priority))

(defn- state-kv
  ^Object [^ISourceState s]
  (.kv s))

(defn- state-iter
  ^Iterator [^ISourceState s]
  (.iter s))

(defn- state-priority
  ^long [^ISourceState s]
  (.priority s))

(declare copy-kv)

(defn- advance-state!
  [^ISourceState s need-val?]
  (.setKv s
          (when (.hasNext ^Iterator (state-iter s))
            (copy-kv (.next ^Iterator (state-iter s)) need-val?)))
  s)

(defn- cmp-kv-raw
  [kv1 kv2 list?]
  (let [kc (bf/compare-buffer (l/k kv1) (l/k kv2))]
    (if (zero? kc)
      (if list?
        (bf/compare-buffer (l/v kv1) (l/v kv2))
        0)
      kc)))

(defn- cmp-kv-order
  [kv1 kv2 list? key-forward? val-forward?]
  (let [kc (bf/compare-buffer (l/k kv1) (l/k kv2))]
    (if (zero? kc)
      (if list?
        (let [vc (bf/compare-buffer (l/v kv1) (l/v kv2))]
          (if val-forward? vc (unchecked-negate-int vc)))
        0)
      (if key-forward? kc (unchecked-negate-int kc)))))

(defn- bb->bytes
  ^bytes [^ByteBuffer bb]
  (let [dup (.duplicate bb)]
    (.rewind dup)
    (b/get-bytes dup)))

(defn- deleted-by-higher?
  [higher-deletes kv list?]
  (let [kb    (bb->bytes (l/k kv))
        key   (->key-bytes kb)
        kvdel (when list?
                (->keyval-bytes kb (bb->bytes (l/v kv))))]
    (boolean
      (some
        (fn [{:keys [deletes]}]
          (when deletes
            (or (.contains ^java.util.Set deletes key)
                (and list?
                     (.contains ^java.util.Set deletes kvdel)))))
        higher-deletes))))

(deftype OverlayKV [^ByteBuffer kb ^ByteBuffer vb]
  l/IKV
  (k [_] kb)
  (v [_] vb))

(defn- copy-buffer
  ^ByteBuffer [^ByteBuffer bb]
  (let [dup (.duplicate bb)
        _   (.rewind dup)
        bs  (b/get-bytes dup)]
    (ByteBuffer/wrap bs)))

(defn- copy-kv
  [kv with-val?]
  (let [kb (copy-buffer (l/k kv))
        vb (when with-val? (copy-buffer (l/v kv)))]
    (OverlayKV. kb vb)))

(defn merge-iterables
  ([sources delete-views key-forward? list? with-val?]
   (merge-iterables sources delete-views key-forward? list? with-val? true))
  ([sources delete-views key-forward? list? with-val? val-forward?]
  (let [sources      (vec (remove nil? sources))
        delete-views (vec (sort-by :priority delete-views))]
    (cond
      (empty? sources)
      (reify Iterable
        (iterator [_] (java.util.Collections/emptyIterator)))

      (and (= 1 (count sources)) (empty? delete-views))
      (:iterable (first sources))

      :else
      (reify Iterable
        (iterator [_]
          (let [need-state-val? (or list? with-val?)
                states
                (mapv (fn [{:keys [iterable priority]}]
                        (let [iter (.iterator ^Iterable iterable)
                              kv   (when (.hasNext ^Iterator iter)
                                     (copy-kv (.next ^Iterator iter)
                                              need-state-val?))]
                          (SourceState. iter kv (long priority))))
                      sources)
                higher-by-priority
                (into {}
                      (map (fn [p]
                             [p (filter #(< ^long (:priority %) ^long p)
                                        delete-views)])
                           (map :priority sources)))]
            (let [sentinel (Object.)
                  buffered (volatile! sentinel)
                  pick-next!
                  (fn pick-next! []
                    (loop []
                      (let [best-idx
                            (reduce
                              (fn [best i]
                                (let [^ISourceState s (nth states i)
                                      kv (state-kv s)]
                                  (cond
                                    (nil? kv) best
                                    (nil? best) i
                                    :else
                                    (let [^ISourceState b (nth states best)
                                          c (cmp-kv-order kv (state-kv b)
                                                          list?
                                                          key-forward?
                                                          val-forward?)]
                                      (cond
                                        (< c 0) i
                                        (and (zero? c)
                                             (< (state-priority s)
                                                (state-priority b))) i
                                        :else best)))))
                              nil
                              (range (count states)))]
                        (if (nil? best-idx)
                          nil
                          (let [^ISourceState best (nth states best-idx)
                                kv (state-kv best)
                                higher (get higher-by-priority
                                            (state-priority best) [])]
                            (if (deleted-by-higher? higher kv list?)
                              (do (advance-state! best need-state-val?) (recur))
                              (let [ret (copy-kv kv with-val?)]
                                (doseq [i (reduce
                                            (fn [acc i]
                                              (let [^ISourceState other
                                                    (nth states i)]
                                                (if (and (some? (state-kv other))
                                                         (zero?
                                                          (cmp-kv-raw
                                                             kv
                                                             (state-kv other)
                                                             list?)))
                                                  (conj acc i)
                                                  acc)))
                                            []
                                            (range (count states)))]
                                  (advance-state! (nth states i)
                                                  need-state-val?))
                                ret)))))))
                  ensure-buffer!
                  (fn []
                    (if (identical? @buffered sentinel)
                      (if-let [item (pick-next!)]
                        (do (vreset! buffered item) true)
                        false)
                      true))]
              (reify
                Iterator
                (hasNext [_]
                  (ensure-buffer!))
                (next [_]
                  (if (ensure-buffer!)
                    (let [ret @buffered]
                      (vreset! buffered sentinel)
                      ret)
                    (throw (NoSuchElementException.))))
                AutoCloseable
                (close [_]
                  (doseq [^ISourceState s states]
                    (when-let [iter (state-iter s)]
                      (when (instance? AutoCloseable iter)
                        (.close ^AutoCloseable iter))))))))))))))
