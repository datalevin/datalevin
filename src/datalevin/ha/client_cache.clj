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
   [datalevin.ha.util :as hu]
   [taoensso.timbre :as log])
  (:import
   [datalevin.utl LRUCache]
   [java.util.concurrent CompletableFuture CompletionException ConcurrentHashMap
    ExecutionException ExecutorService Executors ForkJoinPool ThreadFactory]
   [java.util.concurrent.atomic AtomicBoolean AtomicLong]
   [java.util.function BiConsumer BiFunction]))

(defn ^:redef ha-now-ms
  []
  (System/currentTimeMillis))

(def ^:private long-max2 hu/long-max2)

(def ^:private nonnegative-long-diff hu/nonnegative-long-diff)

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

(defn- ha-client-cache-lru-capacity ^long
  []
  (long-max2
   1
   (long-max2 (long *ha-client-cache-max-size*)
              (long (or *ha-client-cache-max-access-records* 0)))))

(defonce ^:private ^ConcurrentHashMap ha-client-cache
  (ConcurrentHashMap.))

(defonce ^:private ^AtomicLong ha-client-cache-last-prune-ms
  (AtomicLong. 0))

(defonce ^:private ^LRUCache ha-client-cache-lru
  (LRUCache. (int (ha-client-cache-lru-capacity))))

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

(def ^:private ^ExecutorService default-ha-client-cache-prune-executor
  (ForkJoinPool/commonPool))

(defonce ^:private default-ha-client-cache-state
  {:cache ha-client-cache
   :last-prune-ms ha-client-cache-last-prune-ms
   :lru ha-client-cache-lru
   :prune-scheduled? ha-client-cache-prune-scheduled?
   :prune-generation ha-client-cache-prune-generation
   :prune-executor default-ha-client-cache-prune-executor})

(def ^:dynamic *ha-client-cache-state* nil)

(def ^:private ha-thread-label hu/ha-thread-label)

(defn new-ha-client-cache-state
  [db-name]
  {:cache (ConcurrentHashMap.)
   :last-prune-ms (AtomicLong. 0)
   :lru (LRUCache. (int (ha-client-cache-lru-capacity)))
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

(defn- ha-client-cache-ordered-keys
  ([]
   (ha-client-cache-ordered-keys nil))
  ([m-or-state]
   (.orderedKeys ^LRUCache (:lru (ha-client-cache-state m-or-state)))))

(defn- ha-client-cache-entry
  [_cache-key future _now-ms]
  {:future future
   :last-used-ms (AtomicLong. 0)})

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
       (when (or (zero? previous-ms)
                 (zero? bucket-ms)
                 (>= (nonnegative-long-diff now-ms previous-ms)
                     bucket-ms))
         (.put ^LRUCache (:lru (ha-client-cache-state m-or-state))
               cache-key entry))))
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

(defn- prune-expired-ha-client-cache-entries!
  ([now-ms protected-entry]
   (prune-expired-ha-client-cache-entries! nil now-ms protected-entry))
  ([m-or-state now-ms protected-entry]
   (let [cache-state (ha-client-cache-state m-or-state)
         ^ConcurrentHashMap cache (:cache cache-state)
         ^LRUCache lru (:lru cache-state)]
     (doseq [cache-key (ha-client-cache-ordered-keys cache-state)]
       (let [entry (.get cache cache-key)]
         (cond
           (nil? entry)
           (.remove lru cache-key)

           (or (identical? entry protected-entry)
               (pending-ha-client-cache-entry? entry))
           nil

           (or (failed-ha-client-cache-entry? entry)
               (expired-ha-client-cache-entry? entry now-ms))
           (when (.remove cache cache-key entry)
             (.remove lru cache-key)
             (close-ha-client-cache-entry! entry))

           :else
           nil))))))

(defn- prune-overflow-ha-client-cache-entries!
  ([protected-entry]
   (prune-overflow-ha-client-cache-entries! nil protected-entry))
  ([m-or-state protected-entry]
   (let [cache-state (ha-client-cache-state m-or-state)
         ^ConcurrentHashMap cache (:cache cache-state)
         ^LRUCache lru (:lru cache-state)
         max-size (max 0 (long *ha-client-cache-max-size*))]
     (loop [[cache-key & remaining] (seq (ha-client-cache-ordered-keys
                                          cache-state))]
       (when (and (> (.size cache) max-size) cache-key)
         (let [entry (.get cache cache-key)]
           (cond
             (nil? entry)
             (.remove lru cache-key)

             (or (identical? entry protected-entry)
                 (pending-ha-client-cache-entry? entry))
             nil

             :else
             (when (.remove cache cache-key entry)
               (.remove lru cache-key)
               (close-ha-client-cache-entry! entry))))
         (recur remaining))))))

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
         prune-interval-ms (max 0 (long *ha-client-cache-prune-interval-ms*))]
     (or (> (.size cache) max-size)
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
                            (.remove ^LRUCache (:lru cache-state)
                                     cache-key)
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
       (.remove ^LRUCache (:lru cache-state) cache-key)
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
     (.clear ^LRUCache (:lru cache-state))
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

(def shutdown-ha-executor! hu/shutdown-ha-executor!)

(defn stop-ha-client-cache-state!
  [db-name cache-state]
  (when (and cache-state
             (not (identical? cache-state default-ha-client-cache-state)))
    (clear-ha-client-cache! cache-state)
    (when-not (identical? (:prune-executor cache-state)
                          default-ha-client-cache-prune-executor)
      (shutdown-ha-executor! (:prune-executor cache-state)
                             "HA client cache prune executor"
                             {:db-name db-name}))))
