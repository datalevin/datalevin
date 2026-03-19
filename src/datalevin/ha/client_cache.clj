;;
;; Copyright (c) Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns ^:no-doc datalevin.ha.client-cache
  "Pooled HA transport clients and cache lifecycle helpers."
  (:require
   [clojure.string :as s]
   [datalevin.client :as cl]
   [datalevin.constants :as c]
   [datalevin.util :as u]
   [taoensso.timbre :as log])
  (:import
   [java.util.concurrent CompletableFuture CompletionException ConcurrentHashMap
    ConcurrentLinkedDeque ExecutionException ExecutorService Executors
    ThreadFactory TimeUnit]
   [java.util.concurrent.atomic AtomicBoolean AtomicLong]
   [java.util.function BiConsumer BiFunction]))

(defn ^:redef ha-now-ms
  []
  (System/currentTimeMillis))

(defn- long-max2 ^long
  [a b]
  (let [a (long a)
        b (long b)]
    (if (> a b) a b)))

(defn- nonnegative-long-diff ^long
  [a b]
  (let [a (long a)
        b (long b)]
    (if (> a b) (- a b) 0)))

(defn ^:redef close-ha-client!
  [client]
  (when client
    (try
      ;; Internal HA probes may reuse pooled clients; when we evict one from the
      ;; cache, close the pool directly instead of running the network
      ;; disconnect handshake, which reacquires a pooled connection and can mask
      ;; a successful request with a pool timeout.
      (cl/close-pool (cl/get-pool client))
      (catch Throwable _
        (try
          (cl/disconnect client)
          (catch Throwable _ nil))))))

(def ^:dynamic *ha-client-cache-max-size* 256)

(def ^:dynamic *ha-client-cache-ttl-ms* (* 5 60 1000))

(def ^:dynamic *ha-client-cache-prune-interval-ms* 10000)

(def ^:dynamic *ha-client-cache-access-bucket-ms* 1000)

(def ^:dynamic *ha-client-cache-max-access-records* nil)

(defonce ^:private ^ConcurrentHashMap ha-client-cache
  (ConcurrentHashMap.))

(defonce ^:private ^AtomicLong ha-client-cache-last-prune-ms
  (AtomicLong. 0))

(defonce ^:private ^AtomicLong ha-client-cache-access-seq
  (AtomicLong. 0))

(defonce ^:private ^ConcurrentLinkedDeque ha-client-cache-access-order
  (ConcurrentLinkedDeque.))

(defonce ^:private ^AtomicLong ha-client-cache-access-order-size
  (AtomicLong. 0))

(def ^:private ^AtomicLong ha-client-cache-prune-thread-seq
  (AtomicLong. 0))

(defonce ^:private ^AtomicBoolean ha-client-cache-prune-scheduled?
  (AtomicBoolean. false))

(defonce ^:private ^AtomicLong ha-client-cache-prune-generation
  (AtomicLong. 0))

(defn- new-ha-client-cache-prune-thread-factory
  ([] (new-ha-client-cache-prune-thread-factory nil))
  ([label]
   (let [^ThreadFactory delegate (Executors/defaultThreadFactory)]
     (reify ThreadFactory
       (newThread [_ runnable]
         (doto (.newThread delegate runnable)
           (.setName
            (str "datalevin-ha-client-cache-prune"
                 (when label (str "-" label))
                 "-"
                 (.incrementAndGet ^AtomicLong
                                   ha-client-cache-prune-thread-seq)))
           (.setDaemon true)))))))

(defonce ^:private ^ExecutorService ha-client-cache-prune-executor
  (Executors/newSingleThreadExecutor
   (new-ha-client-cache-prune-thread-factory)))

(defonce ^:private default-ha-client-cache-state
  {:cache ha-client-cache
   :last-prune-ms ha-client-cache-last-prune-ms
   :access-seq ha-client-cache-access-seq
   :access-order ha-client-cache-access-order
   :access-order-size ha-client-cache-access-order-size
   :prune-scheduled? ha-client-cache-prune-scheduled?
   :prune-generation ha-client-cache-prune-generation
   :prune-executor ha-client-cache-prune-executor})

(def ^:dynamic *ha-client-cache-state* nil)

(defn- ha-thread-label
  [db-name]
  (when (some? db-name)
    (let [label (-> (str db-name)
                    (s/replace #"[^A-Za-z0-9._-]+" "-")
                    (s/replace #"(^-+|-+$)" ""))]
      (when-not (s/blank? label)
        label))))

(defn new-ha-client-cache-state
  [db-name]
  {:cache (ConcurrentHashMap.)
   :last-prune-ms (AtomicLong. 0)
   :access-seq (AtomicLong. 0)
   :access-order (ConcurrentLinkedDeque.)
   :access-order-size (AtomicLong. 0)
   :prune-scheduled? (AtomicBoolean. false)
   :prune-generation (AtomicLong. 0)
   :prune-executor
   (Executors/newSingleThreadExecutor
    (new-ha-client-cache-prune-thread-factory
     (ha-thread-label db-name)))})

(defn- ha-client-cache-state
  ([] (or *ha-client-cache-state*
          default-ha-client-cache-state))
  ([m-or-state]
   (cond
     (nil? m-or-state)
     (or *ha-client-cache-state*
         default-ha-client-cache-state)

     (and (map? m-or-state)
          (contains? m-or-state :cache)
          (contains? m-or-state :prune-executor))
     m-or-state

     :else
     (or (:ha-client-cache-state m-or-state)
         default-ha-client-cache-state))))

(defn ha-client-cache-key
  [uri client-opts]
  [uri
   (long (or (:pool-size client-opts) 1))
   (long (or (:time-out client-opts) c/default-connection-timeout))])

(defn- next-ha-client-cache-access-id! ^long
  ([]
   (next-ha-client-cache-access-id! nil))
  ([m-or-state]
   (.incrementAndGet ^AtomicLong (:access-seq
                                  (ha-client-cache-state m-or-state)))))

(def ^:private ha-client-cache-default-max-access-records-per-entry 8)

(def ^:private ha-client-cache-min-max-access-records 1024)

(defn- ha-client-cache-max-access-records ^long
  []
  (long
   (or *ha-client-cache-max-access-records*
       (long-max2
        ha-client-cache-min-max-access-records
        (long (* (long-max2 1 (long *ha-client-cache-max-size*))
                 (long ha-client-cache-default-max-access-records-per-entry)))))))

(defn- ha-client-cache-access-order-full?
  ([]
   (ha-client-cache-access-order-full? nil))
  ([m-or-state]
   (>= (.get ^AtomicLong (:access-order-size
                          (ha-client-cache-state m-or-state)))
       (ha-client-cache-max-access-records))))

(defn- enqueue-ha-client-cache-access-record!
  ([record]
   (enqueue-ha-client-cache-access-record! nil record))
  ([m-or-state record]
   (.addLast ^ConcurrentLinkedDeque (:access-order
                                     (ha-client-cache-state m-or-state))
             record)
   (.incrementAndGet ^AtomicLong (:access-order-size
                                  (ha-client-cache-state m-or-state)))
   record))

(defn- prepend-ha-client-cache-access-record!
  ([record]
   (prepend-ha-client-cache-access-record! nil record))
  ([m-or-state record]
   (.addFirst ^ConcurrentLinkedDeque (:access-order
                                      (ha-client-cache-state m-or-state))
              record)
   (.incrementAndGet ^AtomicLong (:access-order-size
                                  (ha-client-cache-state m-or-state)))
   record))

(defn- poll-ha-client-cache-access-record!
  ([]
   (poll-ha-client-cache-access-record! nil))
  ([m-or-state]
   (let [cache-state (ha-client-cache-state m-or-state)]
     (when-let [record (.pollFirst ^ConcurrentLinkedDeque
                                   (:access-order cache-state))]
       (.decrementAndGet ^AtomicLong (:access-order-size cache-state))
       record))))

(defn- ha-client-cache-entry-access-id ^long
  [entry]
  (long (.get ^AtomicLong (:access-id entry))))

(defn- record-ha-client-cache-access!
  ([cache-key entry now-ms]
   (record-ha-client-cache-access! nil cache-key entry now-ms))
  ([m-or-state cache-key entry now-ms]
   (.set ^AtomicLong (:last-used-ms entry) (long now-ms))
   (when (or (zero? (ha-client-cache-entry-access-id entry))
             (not (ha-client-cache-access-order-full? m-or-state)))
     (let [access-id (next-ha-client-cache-access-id! m-or-state)]
       (.set ^AtomicLong (:access-id entry) access-id)
       (enqueue-ha-client-cache-access-record! m-or-state
                                               [cache-key access-id])))
   entry))

(defn- ha-client-cache-entry
  [_cache-key future _now-ms]
  {:future future
   :last-used-ms (AtomicLong. 0)
   :access-id (AtomicLong. 0)})

(defn- pending-ha-client-cache-entry?
  [entry]
  (and entry
       (let [^CompletableFuture future (:future entry)]
         (not (.isDone future)))))

(defn- ha-client-cache-entry-client
  [entry]
  (when entry
    (let [^CompletableFuture future (:future entry)]
      (when (.isDone future)
        (try
          (.getNow future nil)
          (catch CompletionException _
            nil))))))

(defn- ha-client-cache-entry-last-used-ms ^long
  [entry]
  (long (.get ^AtomicLong (:last-used-ms entry))))

(defn- touch-ha-client-cache-entry!
  ([cache-key entry now-ms]
   (touch-ha-client-cache-entry! nil cache-key entry now-ms))
  ([m-or-state cache-key entry now-ms]
   (when entry
     (let [^AtomicLong last-used-ms (:last-used-ms entry)
           previous-ms (.get last-used-ms)
           bucket-ms (max 0 (long *ha-client-cache-access-bucket-ms*))]
       (.set last-used-ms (long now-ms))
       (when (or (zero? (ha-client-cache-entry-access-id entry))
                 (zero? bucket-ms)
                 (>= (nonnegative-long-diff now-ms previous-ms)
                     bucket-ms))
         (record-ha-client-cache-access! m-or-state cache-key entry now-ms))))
   entry))

(defn- ^:redef live-ha-client?
  [client]
  (and client
       (try
         (not (cl/disconnected? client))
         (catch Throwable _
           false))))

(defn- reusable-ha-client-cache-entry?
  [entry now-ms]
  (and entry
       (if (pending-ha-client-cache-entry? entry)
         true
         (and (live-ha-client? (ha-client-cache-entry-client entry))
              (<= (nonnegative-long-diff now-ms
                                         (ha-client-cache-entry-last-used-ms entry))
                  (long *ha-client-cache-ttl-ms*))))))

(defn- close-ha-client-cache-entry!
  [entry]
  (let [client (ha-client-cache-entry-client entry)]
    (if client
      (close-ha-client! client)
      (when-let [^CompletableFuture future (:future entry)]
        (when (pending-ha-client-cache-entry? entry)
          (.whenComplete future
                         (reify BiConsumer
                           (accept [_ opened-client _]
                             (when opened-client
                               (close-ha-client! opened-client))))))))))

(defn- failed-ha-client-cache-entry?
  [entry]
  (and entry
       (not (pending-ha-client-cache-entry? entry))
       (nil? (ha-client-cache-entry-client entry))))

(defn- expired-ha-client-cache-entry?
  [entry now-ms]
  (and entry
       (not (pending-ha-client-cache-entry? entry))
       (> (nonnegative-long-diff now-ms
                                 (ha-client-cache-entry-last-used-ms entry))
          (long *ha-client-cache-ttl-ms*))))

(defn- restore-ha-client-cache-access-records!
  ([records]
   (restore-ha-client-cache-access-records! nil records))
  ([m-or-state records]
   (doseq [record (rseq (vec records))]
     (prepend-ha-client-cache-access-record! m-or-state record))))

(defn- current-ha-client-cache-access-record?
  [entry access-id]
  (= (long access-id)
     (ha-client-cache-entry-access-id entry)))

(defn- prune-expired-ha-client-cache-entries!
  ([now-ms protected-entry]
   (prune-expired-ha-client-cache-entries! nil now-ms protected-entry))
  ([m-or-state now-ms protected-entry]
   (let [cache-state (ha-client-cache-state m-or-state)
         ^ConcurrentHashMap cache (:cache cache-state)]
     (loop [deferred []]
       (if-let [[cache-key access-id :as record]
                (poll-ha-client-cache-access-record! cache-state)]
         (let [entry (.get cache cache-key)]
           (cond
             (nil? entry)
             (recur deferred)

             (not (current-ha-client-cache-access-record? entry access-id))
             (recur deferred)

             (or (identical? entry protected-entry)
                 (pending-ha-client-cache-entry? entry))
             (recur (conj deferred record))

             (or (failed-ha-client-cache-entry? entry)
                 (expired-ha-client-cache-entry? entry now-ms))
             (do
               (when (.remove cache cache-key entry)
                 (close-ha-client-cache-entry! entry))
               (recur deferred))

             :else
             (restore-ha-client-cache-access-records! cache-state
                                                      (conj deferred record))))
         (restore-ha-client-cache-access-records! cache-state deferred))))))

(defn- prune-overflow-ha-client-cache-entries!
  ([protected-entry]
   (prune-overflow-ha-client-cache-entries! nil protected-entry))
  ([m-or-state protected-entry]
   (let [cache-state (ha-client-cache-state m-or-state)
         ^ConcurrentHashMap cache (:cache cache-state)
         max-size (max 0 (long *ha-client-cache-max-size*))]
     (loop [deferred []]
       (if (> (.size cache) max-size)
         (if-let [[cache-key access-id :as record]
                  (poll-ha-client-cache-access-record! cache-state)]
           (let [entry (.get cache cache-key)]
             (cond
               (nil? entry)
               (recur deferred)

               (not (current-ha-client-cache-access-record? entry access-id))
               (recur deferred)

               (or (identical? entry protected-entry)
                   (pending-ha-client-cache-entry? entry))
               (recur (conj deferred record))

               :else
               (do
                 (when (.remove cache cache-key entry)
                   (close-ha-client-cache-entry! entry))
                 (recur deferred))))
           (restore-ha-client-cache-access-records! cache-state deferred))
         (restore-ha-client-cache-access-records! cache-state deferred))))))

(defn ^:redef prune-ha-client-cache!
  ([now-ms protected-entry]
   (prune-ha-client-cache! nil now-ms protected-entry))
  ([m-or-state now-ms protected-entry]
   (prune-expired-ha-client-cache-entries! m-or-state now-ms protected-entry)
   (prune-overflow-ha-client-cache-entries! m-or-state protected-entry)))

(defn- ha-client-cache-prune-due?
  ([now-ms]
   (ha-client-cache-prune-due? nil now-ms))
  ([m-or-state now-ms]
   (let [cache-state (ha-client-cache-state m-or-state)
         ^ConcurrentHashMap cache (:cache cache-state)
         max-size (max 0 (long *ha-client-cache-max-size*))
         prune-interval-ms (max 0 (long *ha-client-cache-prune-interval-ms*))
         access-order-size (.get ^AtomicLong (:access-order-size cache-state))]
     (or (> (.size cache) max-size)
         (>= access-order-size
             (ha-client-cache-max-access-records))
         (>= (nonnegative-long-diff now-ms
                                    (.get ^AtomicLong
                                          (:last-prune-ms cache-state)))
             prune-interval-ms)))))

(defn- schedule-ha-client-cache-prune!
  ([protected-entry]
   (schedule-ha-client-cache-prune! nil protected-entry))
  ([m-or-state protected-entry]
   (let [cache-state (ha-client-cache-state m-or-state)
         generation (.get ^AtomicLong (:prune-generation cache-state))]
     (when (.compareAndSet ^AtomicBoolean (:prune-scheduled? cache-state)
                           false true)
       (try
         (.submit
          ^ExecutorService (:prune-executor cache-state)
          ^Runnable
          (fn []
            (try
              (when (= generation
                       (.get ^AtomicLong (:prune-generation cache-state)))
                (let [now-ms (ha-now-ms)]
                  (binding [*ha-client-cache-state* cache-state]
                    (prune-ha-client-cache! now-ms protected-entry))
                  (when (= generation
                           (.get ^AtomicLong
                                 (:prune-generation cache-state)))
                    (.set ^AtomicLong (:last-prune-ms cache-state)
                          (long now-ms)))))
              (catch Throwable e
                (log/warn e "Failed to prune HA client cache"))
              (finally
                (.set ^AtomicBoolean (:prune-scheduled? cache-state) false)
                (when (and (= generation
                              (.get ^AtomicLong
                                    (:prune-generation cache-state)))
                           (ha-client-cache-prune-due? cache-state
                                                       (ha-now-ms)))
                  (schedule-ha-client-cache-prune! cache-state nil))))))
         (catch Throwable e
           (.set ^AtomicBoolean (:prune-scheduled? cache-state) false)
           (log/warn e "Failed to schedule HA client cache pruning")))))))

(defn- maybe-prune-ha-client-cache!
  ([now-ms]
   (maybe-prune-ha-client-cache! nil now-ms nil))
  ([now-ms protected-entry]
   (maybe-prune-ha-client-cache! nil now-ms protected-entry))
  ([m-or-state now-ms protected-entry]
   (when (ha-client-cache-prune-due? m-or-state now-ms)
     (schedule-ha-client-cache-prune! m-or-state protected-entry))))

(defn ^:redef open-ha-client
  [uri db-name client-opts]
  (let [client (cl/new-client uri client-opts)]
    (cl/open-database client db-name c/db-store-kv)
    client))

(defn ^:redef ha-client-request
  [client type args writing?]
  (cl/normal-request client type args writing?))

(defn ^:redef await-ha-client-cache-entry-client!
  [entry]
  (let [^CompletableFuture future (:future entry)]
    (try
      (.get future)
      (catch ExecutionException e
        (throw (or (.getCause e) e)))
      (catch InterruptedException e
        (.interrupt (Thread/currentThread))
        (throw e)))))

(defn cached-ha-client
  ([uri db-name client-opts]
   (cached-ha-client nil uri db-name client-opts))
  ([m-or-state uri db-name client-opts]
   (let [cache-state (ha-client-cache-state m-or-state)
         cache-key (ha-client-cache-key uri client-opts)
         ^ConcurrentHashMap cache (:cache cache-state)]
     (maybe-prune-ha-client-cache! cache-state (ha-now-ms) nil)
     (let [pending-entry (ha-client-cache-entry cache-key
                                                (CompletableFuture.)
                                                (ha-now-ms))
           stale-entry-v (volatile! nil)
           cache-entry (.compute cache cache-key
                                 (reify BiFunction
                                   (apply [_ _ existing]
                                     (let [now-ms (ha-now-ms)]
                                       (if (reusable-ha-client-cache-entry?
                                            existing now-ms)
                                         (touch-ha-client-cache-entry!
                                          cache-state cache-key existing now-ms)
                                         (do
                                           (when existing
                                             (vreset! stale-entry-v existing))
                                           (touch-ha-client-cache-entry!
                                            cache-state cache-key
                                            pending-entry now-ms)))))))
           installer? (identical? cache-entry pending-entry)]
       (when-let [stale-entry @stale-entry-v]
         (when-not (identical? stale-entry cache-entry)
           (close-ha-client-cache-entry! stale-entry)))
       (let [client (if installer?
                      (let [^CompletableFuture future (:future pending-entry)]
                        (try
                          (let [client (open-ha-client uri db-name client-opts)]
                            (.complete future client)
                            client)
                          (catch Throwable e
                            (.remove cache cache-key pending-entry)
                            (.completeExceptionally future e)
                            (throw e))))
                      (await-ha-client-cache-entry-client! cache-entry))]
         (maybe-prune-ha-client-cache! cache-state (ha-now-ms) cache-entry)
         client)))))

(defn- invalidate-cached-ha-client!
  ([uri client-opts client]
   (invalidate-cached-ha-client! nil uri client-opts client))
  ([m-or-state uri client-opts client]
   (let [cache-state (ha-client-cache-state m-or-state)
         cache-key (ha-client-cache-key uri client-opts)
         ^ConcurrentHashMap cache (:cache cache-state)
         stale-entry-v (volatile! nil)]
     (.compute cache cache-key
               (reify BiFunction
                 (apply [_ _ existing]
                   (if (and existing
                            (identical? client
                                        (ha-client-cache-entry-client existing)))
                     (do
                       (vreset! stale-entry-v existing)
                       nil)
                     existing))))
     (when-let [stale-entry @stale-entry-v]
       (close-ha-client-cache-entry! stale-entry)))))

(defn with-cached-ha-client
  ([uri db-name client-opts f]
   (with-cached-ha-client nil uri db-name client-opts f))
  ([m-or-state uri db-name client-opts f]
   (let [client (cached-ha-client m-or-state uri db-name client-opts)]
     (try
       (f client)
       (catch Exception e
         (invalidate-cached-ha-client! m-or-state uri client-opts client)
         (throw e))))))

(defn unknown-ha-watermark-command?
  [e]
  (let [message (or (ex-message e) "")]
    (or (= :unknown-message-type (:error (ex-data e)))
        (s/includes? message "Unknown message type :ha-watermark"))))

(defn clear-ha-client-cache!
  ([]
   (clear-ha-client-cache! nil))
  ([m-or-state]
   (let [cache-state (ha-client-cache-state m-or-state)
         ^ConcurrentHashMap cache (:cache cache-state)]
     (.incrementAndGet ^AtomicLong (:prune-generation cache-state))
     (.set ^AtomicBoolean (:prune-scheduled? cache-state) false)
     (.set ^AtomicLong (:access-seq cache-state) 0)
     (.clear ^ConcurrentLinkedDeque (:access-order cache-state))
     (.set ^AtomicLong (:access-order-size cache-state) 0)
     (loop []
       (let [entries (vec (.entrySet cache))]
         (when (seq entries)
           (doseq [entry entries]
             (let [entry ^java.util.Map$Entry entry
                   cache-key (.getKey entry)
                   cache-entry (.getValue entry)]
               (when (.remove cache cache-key cache-entry)
                 (close-ha-client-cache-entry! cache-entry))))
           (when-not (.isEmpty cache)
             (recur)))))
     (.set ^AtomicLong (:last-prune-ms cache-state) 0))))

(defn shutdown-ha-executor!
  [^ExecutorService executor description context]
  (when executor
    (try
      (.shutdown executor)
      (when-not (.awaitTermination executor 100 TimeUnit/MILLISECONDS)
        (.shutdownNow executor))
      (catch InterruptedException e
        (.interrupt (Thread/currentThread))
        (.shutdownNow executor)
        (log/warn e (str "Interrupted while stopping " description) context))
      (catch Throwable e
        (log/warn e (str "Failed to stop " description) context)))))

(defn stop-ha-client-cache-state!
  [db-name cache-state]
  (when (and cache-state
             (not (identical? cache-state default-ha-client-cache-state)))
    (clear-ha-client-cache! cache-state)
    (shutdown-ha-executor! (:prune-executor cache-state)
                           "HA client cache prune executor"
                           {:db-name db-name})))
