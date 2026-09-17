(ns ^:no-doc datalevin.db.range-cache
  "Conservative overlap checks for cached native index ranges."
  (:require [datalevin.bits :as b]
            [datalevin.constants :as c]
            [datalevin.custom-datalog :as cd]
            [datalevin.datom :as d]
            [datalevin.index :as idx])
  (:import [java.util Arrays IdentityHashMap]
           [datalevin.bits Indexable]
           [datalevin.datom Datom]))

(defn range-key? [k]
  (and (vector? k)
       (contains? #{:range-datoms :index-range :index-range-size} (first k))))

(deftype Context [schema datoms ^IdentityHashMap encoded])

(defn context
  "Created lazily once per invalidation. A nil schema keeps remote invalidation
  conservative, without adding metadata RPCs to the commit path."
  [schema datoms]
  (when schema (Context. schema datoms (IdentityHashMap.))))

(defn- custom-attr? [schema a]
  (cd/custom-type? (:db/valueType (get schema a))))

(defn- encoded-datom
  "The giant ID is unavailable after a retraction. Encode its entire possible
  ID interval, sharing the already encoded value prefix. Inline values have
  identical lower and upper bytes. Never invoke custom resolvers here."
  [^Context context ^Datom datom]
  (let [^IdentityHashMap encoded (.-encoded context)]
    (or (.get encoded datom)
        (let [^Indexable low (idx/datom->indexable (.-schema context) datom false)
              lo (b/indexable-bytes low)
              hi (if (b/giant? low)
                   (b/indexable-bytes
                     (Indexable. (.-e low) (.-a low) (.-v low) (.-f low)
                                 (.-b low) c/gmax))
                   lo)
              bounds [lo hi]]
          (.put encoded datom bounds)
          bounds))))

(defn- bound-aid [schema ^Datom bound high?]
  (if-some [a (.-a bound)]
    (long (or (:db/aid (get schema a)) c/a0))
    (if high? c/amax c/a0)))

(defn- unresolved-ref-bound? [schema attr value]
  (and (= :db.type/ref (:db/valueType (get schema attr)))
       (some? value)
       (not (integer? value))))

(defn- overlaps?
  [^Context context ^Datom low ^Datom high]
  (let [schema (.-schema context)
        le (long (or (.-e low) c/e0))
        he (long (or (.-e high) c/emax))
        la (bound-aid schema low false)
        ha (bound-aid schema high true)
        ;; Reuse bounds for all changed datoms in this range. Most invalidation
        ;; checks finish using entity/attribute IDs and never encode values.
        lo (delay (b/indexable-bytes (idx/datom->indexable schema low false)))
        hi (delay (b/indexable-bytes (idx/datom->indexable schema high true)))]
    (boolean
      (some
        (fn [^Datom datom]
          (let [e (.-e datom)
                a (.-a datom)
                aid (:db/aid (get schema a))]
            (and (<= le e he)
                 (or (nil? aid)
                     (and (<= la (long aid) ha)
                          (or (< la (long aid) ha)
                              ;; Custom order functions can access a transaction
                              ;; or throw. A commit must not call them again.
                              (custom-attr? schema a)
                              (custom-attr? schema (.-a low))
                              (custom-attr? schema (.-a high))
                              (let [[^bytes dl ^bytes dh] (encoded-datom context datom)]
                                (and (or (< la (long aid))
                                         (not (neg? (Arrays/compareUnsigned dh ^bytes @lo))))
                                     (or (< (long aid) ha)
                                         (not (pos? (Arrays/compareUnsigned dl ^bytes @hi))))))))))))
        (.-datoms context)))))

(defn affected?
  "Return false only when no changed datom can fall within both inclusive
  native ranges: entity IDs and AVG bytes. EAV and AVE apply these independently,
  rather than comparing complete datoms lexicographically."
  [^Context context k]
  (if-not context
    true
    (try
      (case (first k)
        :range-datoms
        (let [[_ index low high] k]
          (if (#{:eav :ave} index)
            (overlaps? context low high)
            true))

        (:index-range :index-range-size)
        (let [[_ attr start end] k
              schema (.-schema context)]
          ;; Lookup refs can move when their identifying attribute changes,
          ;; even if no datom in the queried ref attribute was touched.
          (or (unresolved-ref-bound? schema attr start)
              (unresolved-ref-bound? schema attr end)
              (overlaps? context (d/datom c/e0 attr start)
                                  (d/datom c/emax attr end))))
        true)
      ;; A failed comparison must evict, never fail an already committed write.
      (catch Exception _ true))))
