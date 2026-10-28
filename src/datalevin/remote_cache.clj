;; Copyright (c) Huahai Yang. All rights reserved.
;; Distributed under the Eclipse Public License 2.0.
(ns ^:no-doc datalevin.remote-cache
  "Bounded client read results, validated against committed store revisions."
  (:require [clojure.string :as str]
            [datalevin.bits :as bits]
            [datalevin.client.request :as cr])
  (:import [datalevin.utl LRUCache]
           [java.nio ByteBuffer]))

(def ^:private immutable-collection-types
  ;; IPersistentCollection also covers mutable spillable collections. Generic
  ;; sequence views can expose mutable backing arrays or collections.
  #{clojure.lang.PersistentVector clojure.lang.APersistentVector$SubVector
    clojure.lang.PersistentArrayMap clojure.lang.PersistentHashMap
    clojure.lang.PersistentTreeMap clojure.lang.PersistentStructMap
    clojure.lang.PersistentHashSet clojure.lang.PersistentTreeSet
    clojure.lang.PersistentList clojure.lang.PersistentList$EmptyList
    clojure.lang.PersistentQueue clojure.lang.MapEntry
    datalevin.utl.UniqueVectorSet})

(defn immutable-data?
  "Accept known immutable values, recursively checking contents and metadata."
  [x]
  (and (or (nil? x) (string? x) (keyword? x) (symbol? x) (boolean? x)
           (char? x) (uuid? x) (integer? x) (float? x) (decimal? x) (ratio? x)
           (and (contains? immutable-collection-types (class x))
                (if (map? x)
                  (every? (fn [[k v]]
                            (and (immutable-data? k) (immutable-data? v))) x)
                  (every? immutable-data? x))))
       (if-some [metadata (meta x)]
         (immutable-data? metadata)
         true)))

(defn serialized-key
  "Own serialized request bytes for representation-sensitive cache identity.
  Keep the read-only buffer's position fixed for stable equality and hashing."
  ^ByteBuffer [key]
  (.asReadOnlyBuffer (ByteBuffer/wrap (bits/serialize key))))

(def ^:private read-ops
  ;; Schema snapshots refresh the store's metadata cache after revision changes;
  ;; retaining them here would introduce a second, independent freshness window.
  #{:get-value :get-rank :get-by-rank :get-first :get-first-n :get-range
    :key-range :key-range-count :key-range-list-count :range-count
    :get-list :list-count :in-list? :list-range :list-range-count
    :list-range-first :list-range-first-n :entries :list-dbis
    :schema :rschema})

(defn cacheable-call?
  [op args]
  (or (contains? read-ops op)
      (and (= :batch-kv op)
           (every? #(contains? read-ops (first %)) (second args)))))

(defrecord ReadCache [^LRUCache entries state interval-ms])

(defn read-cache
  [limit interval-ms]
  (when-not (and (integer? limit) (<= 0 limit Integer/MAX_VALUE))
    (throw (ex-info "read-cache-limit must be a non-negative integer"
                    {:read-cache-limit limit})))
  (when-not (or (nil? interval-ms)
                (and (integer? interval-ms) (<= 0 interval-ms Long/MAX_VALUE)))
    (throw (ex-info "read-cache-check-interval-ms must be a non-negative integer"
                    {:read-cache-check-interval-ms interval-ms})))
  (->ReadCache (LRUCache. (int limit))
               (atom {:token nil :checked-ns 0 :writers 0 :legacy? false})
               interval-ms))

(defn- invalidate!
  [^ReadCache cache writer-delta]
  (let [^LRUCache entries (:entries cache)]
    (locking entries
      (.beginInvalidation entries 0)
      (.clear entries)
      (swap! (:state cache) #(-> % (assoc :token nil :checked-ns 0)
                                  (update :writers + writer-delta))))))

(defn mutate
  "Keep in-flight reads from publishing across a mutation, including failure."
  [cache execute]
  (if cache
    (do
      (invalidate! cache 1)
      (try (execute) (finally (invalidate! cache -1))))
    (execute)))

(defn- check-revision!
  [^ReadCache cache revision]
  (let [^LRUCache entries (:entries cache)
        generation (.generation entries)
        started (System/nanoTime)
        token (try (revision)
                   (catch clojure.lang.ExceptionInfo e
                     (if (str/includes? (or (:server-message (ex-data e))
                                            (ex-message e) "")
                                        "Unknown message type :kv-read-cache-token")
                       ::legacy
                       (throw e))))]
    (locking entries
      (when (= generation (.generation entries))
        (when (not= token (:token @(:state cache)))
          (.beginInvalidation entries 0)
          (.clear entries))
        (swap! (:state cache) assoc :token token :checked-ns started
               :legacy? (= ::legacy token))))))

(defn- custom-type-read?
  [token key]
  (let [dbis (:custom-dbis (get token 2))]
    (when (seq dbis)
      (let [[op args] key]
        (if (= op :batch-kv)
          (some (fn [[_ dbi]] (contains? dbis dbi)) (second args))
          (contains? dbis (second args)))))))

(defn read-result
  "Validate hits; unique misses avoid repeated revision RPCs. Positive intervals
  allow hits without an RPC. Identity follows serialized request bytes, preserving
  distinctions such as vector/list :data keys. Mutable data, custom DBIs whose
  codecs can change independently of commits, and older servers bypass retention."
  [^ReadCache cache key interval-ms revision execute]
  (let [^LRUCache entries (:entries cache)
        state @(:state cache)]
    (if (or (zero? (.capacity entries)) (:legacy? state)
            (pos? (long (:writers state))) (not (immutable-data? key)))
      (execute)
      (let [cache-key (serialized-key key)
            cached (.get entries cache-key)
            interval (long (or (:interval-ms cache) interval-ms))]
        (when (or (nil? (:token state))
                  (and cached
                       (>= (quot (- (System/nanoTime) (long (:checked-ns state)))
                                 1000000)
                           interval)))
          (check-revision! cache revision))
        (let [generation (.generation entries)
              state @(:state cache)]
          (if (or (:legacy? state) (pos? (long (:writers state)))
                  (custom-type-read? (:token state) key))
            (execute)
            (if-some [cached (when cached (.get entries cache-key))]
              (do (cr/check! :request) (first cached))
              (let [result (execute)]
                (when (immutable-data? result)
                  (.putIfGeneration entries cache-key [result] generation))
                result))))))))
