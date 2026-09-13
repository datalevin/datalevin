;;
;; Copyright (c) Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns ^:no-doc datalevin.spill
  "Mutable data structures that spills to disk automatically
  when memory pressure is high. Presents a `IPersistent*` API
  for compatibility and convenience."
  (:require
   [datalevin.constants :as c]
   [datalevin.util :as u]
   [datalevin.lmdb :as l]
   [datalevin.interface :as i]
   [datalevin.native-value :as nv]
   [taoensso.nippy :as nippy]
   [clojure.set :as set])
  (:import
   [java.util Iterator List UUID NoSuchElementException Map Set Collection AbstractSet HashMap]
   [java.io DataInput DataOutput]
   [java.lang.ref Cleaner Cleaner$Cleanable]
   [java.lang.management ManagementFactory]
   [javax.management NotificationEmitter NotificationListener Notification]
   [com.sun.management GarbageCollectionNotificationInfo]
   [org.eclipse.collections.impl.map.mutable UnifiedMap]
   [org.eclipse.collections.impl.list.mutable FastList]
   [datalevin.utl UniqueVectorSet]
   [clojure.lang ISeq IPersistentVector MapEntry Util Sequential
    IPersistentMap MapEquivalence IObj IFn IPersistentSet]))

(defonce memory-pressure (volatile! 0))

(defonce ^Runtime runtime (Runtime/getRuntime))

(defn- set-memory-pressure []
  (let [fm (.freeMemory runtime)
        tm (.totalMemory runtime)
        mm (.maxMemory runtime)
        pr (int (/ (- tm fm) mm))]
    ;; (println "used" pr "% of" (int (/ mm (* 1024 1024))))
    (vreset! memory-pressure pr)))

(defonce listeners (volatile! {}))

(defn install-memory-updater []
  (doseq [^NotificationEmitter gcbean
          (ManagementFactory/getGarbageCollectorMXBeans)]
    (let [^NotificationListener listener
          (reify NotificationListener
            (^void handleNotification [_ ^Notification notif _]
             (when (= (.getType notif)
                      GarbageCollectionNotificationInfo/GARBAGE_COLLECTION_NOTIFICATION)
               (set-memory-pressure))))]
      (vswap! listeners assoc gcbean listener)
      (.addNotificationListener gcbean listener nil nil))))

(defn uninstall-memory-updater []
  (doseq [[^NotificationEmitter gcbean listener] @listeners]
    (vswap! listeners dissoc gcbean)
    (.removeNotificationListener gcbean listener nil nil)))

(def memory-updater (memoize install-memory-updater)) ; do it once

(defprotocol ISpillable
  (memory-count [this] "The number of items reside in memory")
  (disk-count [this] "The number of items reside on disk")
  (spill [this] "Spill to disk"))

(defonce cleaner (delay (Cleaner/create)))

(defn- spill-cleaner
  ^Cleaner []
  @cleaner)

(defn- close-spill-resources!
  [disk spill-dir]
  (let [db  @disk
        dir @spill-dir]
    (vreset! disk nil)
    (vreset! spill-dir nil)
    (when db
      (try
        (i/close-kv db)
        (catch Throwable _)))
    (when dir
      (try
        (u/delete-files dir)
        (catch Throwable _)))))

(defn- spill-cleanup-action
  [disk spill-dir]
  (reify Runnable
    (run [_] (close-spill-resources! disk spill-dir))))

(defn- register-spill-cleanup!
  [referent cleanable disk spill-dir]
  (vreset! cleanable
           (.register (spill-cleaner)
                      referent
                      (spill-cleanup-action disk spill-dir))))

(defn- clean-spill!
  [cleanable disk spill-dir]
  (if-let [^Cleaner$Cleanable c @cleanable]
    (do
      (vreset! cleanable nil)
      (.clean c))
    (close-spill-resources! disk spill-dir)))

(declare ->SVecSeq ->RSVecSeq)

(defn- spill-tx [db bindings txs]
  (binding [nv/*spill-bindings* bindings nv/*wire-native-value* false]
    (i/transact-kv db txs)))

(defn- spill-value [db bindings k kt]
  (binding [nv/*spill-bindings* bindings nv/*wire-native-value* false]
    (i/get-value db c/tmp-dbi k kt)))

(deftype SpillableVector [^long spill-threshold
                          ^String spill-root
                          spill-dir
                          ^FastList memory
                          disk
                          cleanable
                          ^HashMap native-bindings
                          total
                          ^:unsynchronized-mutable meta]
  ISpillable

  (memory-count ^long [_] (.size memory))

  (disk-count ^long [_]
    (if @disk (i/entries @disk c/tmp-dbi) 0))

  (spill [this]
    (when-not @disk
      (let [dir (str spill-root "dtlv-spill-vec-" (UUID/randomUUID))
            db  (l/open-kv dir {:temp? true :spill? true})]
        (vreset! spill-dir dir)
        (vreset! disk db)
        (register-spill-cleanup! this cleanable disk spill-dir)
        (i/open-dbi db c/tmp-dbi {:key-size Long/BYTES})))
    this)

  List

  (get [this i]
    (.valAt this i))

  (size [this]
    (.count this))

  (add [this item]
    (.cons this item)
    true)

  (addAll [this other]
    (let [n (.size ^List other)]
      (dotimes [i n]
        (.cons this (.get ^List other i)))
      (< 0 n)))

  IPersistentVector

  (assocN [this i v]
    (let [^long mc (memory-count this)
          ^long tc @total]
      (cond
        (= i tc) (.cons this v)
        (< i mc) (.set memory i v)
        (< i tc) (spill-tx @disk native-bindings
                          [(l/kv-tx :put c/tmp-dbi i [v] :id)])
        :else    (throw (IndexOutOfBoundsException.))))
    this)

  (cons [this v]
    (let [mem? (nil? @disk)]
      (if (and (< ^long @memory-pressure spill-threshold) mem?)
        (.add memory v)
        (do (when mem? (.spill this))
            (spill-tx @disk native-bindings
                      [(l/kv-tx :put c/tmp-dbi @total [v] :id)]))))
    (vswap! total u/long-inc)
    this)

  (length [_] @total)

  (assoc [this k v]
    (if (integer? k)
      (.assocN this k v)
      (throw (IllegalArgumentException. "Key must be integer"))))

  (containsKey [_ k]
    (and (integer? k) (<= 0 ^long k) (< ^long k ^long @total)))

  (entryAt [this k]
    (when (.containsKey this k)
      (MapEntry. k (.valAt this k))))

  (valAt [_ k nf]
    (if (and (integer? k) (<= 0 ^long k) (< ^long k ^long @total))
      (cond
        (< ^long k (.size memory)) (.get memory k)
        @disk                      (first (spill-value @disk native-bindings k :id))
        :else                      nf)
      nf))
  (valAt [this k]
    (.valAt this k nil))

  (peek [this]
    (if (zero? ^long (disk-count this))
      (.getLast memory)
      (first (binding [nv/*spill-bindings* native-bindings nv/*wire-native-value* false]
               (i/get-first @disk c/tmp-dbi [:all-back] :id :data true)))))

  (pop [this]
    (cond
      (zero? ^long @total)
      (throw (IllegalStateException. "Can't pop empty vector"))

      (< 0 ^long (disk-count this))
      (let [[lk _] (i/get-first @disk c/tmp-dbi [:all-back]
                                :id :ignore)]
        (i/transact-kv @disk [(l/kv-tx :del c/tmp-dbi lk :id)]))

      :else (.remove memory (dec ^long (memory-count this))))
    (vswap! total #(dec ^long %))
    this)

  (count [_] @total)

  (empty [this]
    (.clear memory)
    (when @disk
      (clean-spill! cleanable disk spill-dir))
    (.clear native-bindings)
    (vreset! total 0)
    this)

  (equiv [this other]
    (cond
      (identical? this other) true

      (or (instance? Sequential other) (instance? List other))
      (if (not= (count this) (count other))
        false
        (every? true? (map #(Util/equiv %1 %2) this other)))

      :else false))

  (seq ^ISeq [this] (when (< 0 ^long @total) (->SVecSeq this 0)))

  (rseq ^ISeq [this]
    (when (< 0 ^long @total) (->RSVecSeq this (dec ^long @total))))

  (nth [this i]
    (if (and (<= 0 i) (< i ^long @total))
      (.valAt this i)
      (throw (IndexOutOfBoundsException.))))
  (nth [this i nf]
    (if (and (<= 0 i) (< i ^long @total))
      (.nth this i)
      nf))

  Iterable

  (iterator [this]
    (let [i (volatile! 0)]
      (reify
        Iterator
        (hasNext [_] (< ^long @i ^long @total))
        (next [_]
          (if (< ^long @i ^long @total)
            (let [res (.nth this @i)]
              (vswap! i u/long-inc)
              res)
            (throw (NoSuchElementException.)))))))

  IFn

  (invoke [this arg1] (.valAt this arg1))

  IObj

  (withMeta [this m] (set! meta m) this)

  (meta [_] meta)

  Object

  (toString [this] (str (into [] this))))

(deftype SVecSeq [^SpillableVector v
                  ^long i]
  ISeq

  (seq ^ISeq [this] this)

  (first ^Object [_] (nth v i))

  (next ^ISeq [_]
    (let [i+1 (inc i)]
      (when (< i+1 (count v)) (->SVecSeq v i+1))))

  (more ^ISeq [this] (let [s (.next this)] (if s s '())))

  (cons [_ _]
    (throw (UnsupportedOperationException.
             "Changing SpillableVector.SVecSeq is not supported")))

  (count [_] (- (count v) i))

  (equiv [this other]
    (if (or (instance? List other) (instance? Sequential other))
      (if (not= (count this) (count other))
        false
        (every? true? (map #(Util/equiv %1 %2) this other)))
      false)))

(deftype RSVecSeq [^SpillableVector v
                   ^long i]
  ISeq

  (seq ^ISeq [this] this)

  (first ^Object [_] (nth v i))

  (next ^ISeq [_] (when (< 0 i) (->RSVecSeq v (dec i))))

  (more ^ISeq [this] (let [s (.next this)] (if s s '())))

  (cons [_ _]
    (throw (UnsupportedOperationException.
             "Changing SpillableVector.RSVecSeq is not supported")))

  (count [_] (inc i))

  (equiv [this other]
    (if (or (instance? List other)
            (instance? Sequential other))
      (if (not= (count this) (count other))
        false
        (every? true? (map #(Util/equiv %1 %2) this other)))
      false)))

(defn new-spillable-vector
  ([] (new-spillable-vector nil nil))
  ([vs] (new-spillable-vector vs nil))
  ([vs {:keys [spill-threshold spill-root]
        :or   {spill-threshold c/default-spill-threshold
               spill-root      c/default-spill-root}}]
   (when (empty? @listeners) (memory-updater))
   (let [^SpillableVector svec (SpillableVector. spill-threshold
                                                 spill-root
                                                 (volatile! nil)
                                                 (FastList. (count vs))
                                                 (volatile! nil)
                                                 (volatile! nil)
                                                 (nv/spill-bindings)
                                                 (volatile! 0)
                                                 nil)]
     (doseq [v vs] (.cons svec v))
     svec)))

(defn map-sv
  [f sv]
  (reduce
    (fn [^SpillableVector acc r] (.cons acc (f r)))
    (new-spillable-vector)
    sv))

(nippy/extend-freeze
    SpillableVector :spillable-vec
    [^SpillableVector x ^DataOutput out]
  (let [n (count x)]
    (.writeLong out n)
    (dotimes [i n] (nippy/freeze-to-out! out (nth x i)))))

(nippy/extend-thaw
  :spillable-vec
  [^DataInput in]
  (let [n                   (.readLong in)
        ^SpillableVector vs (new-spillable-vector)]
    (dotimes [_ n] (.cons vs (nippy/thaw-from-in! in)))
    vs))

(defn- map-bucket [db bindings k]
  (or (spill-value db bindings (hash k) :int) []))

(defn- bucket-index [bucket k]
  (first (keep-indexed (fn [idx [stored-key]]
                         (when (Util/equiv k stored-key) idx))
                       bucket)))

(defn- disk-map-entry [db bindings k]
  (when db
    (let [bucket (map-bucket db bindings k)]
      (when-some [idx (bucket-index bucket k)]
        (let [[stored-key value] (nth bucket idx)]
          (MapEntry. stored-key value))))))

(defn- disk-map-put! [db bindings disk-total k v]
  (let [bucket (map-bucket db bindings k)
        idx    (bucket-index bucket k)
        entry  (when (some? idx) (nth bucket idx))]
    (spill-tx db bindings
              [(l/kv-tx :put c/tmp-dbi (hash k)
                        (if entry
                          (assoc bucket idx [(first entry) v])
                          (conj bucket [k v]))
                        :int)])
    (when-not entry (vswap! disk-total u/long-inc))
    (second entry)))

(defn- disk-map-remove! [db bindings disk-total k]
  (when db
    (let [bucket (map-bucket db bindings k)]
      (when-some [idx (bucket-index bucket k)]
        (let [entry (nth bucket idx)
              rest  (into (subvec bucket 0 idx) (subvec bucket (inc ^long idx)))]
          (spill-tx db bindings
                    [(if (seq rest)
                       (l/kv-tx :put c/tmp-dbi (hash k) rest :int)
                       (l/kv-tx :del c/tmp-dbi (hash k) :int))])
          (vswap! disk-total u/long-dec)
          (second entry))))))

(defn- disk-map-entries [db bindings]
  (when db
    (let [buckets (binding [nv/*spill-bindings* bindings nv/*wire-native-value* false]
                    (i/get-range db c/tmp-dbi [:all] :int :data true))]
      (mapcat identity buckets))))

(deftype SpillableMap [^long spill-threshold
                       ^String spill-root
                       spill-dir
                       ^UnifiedMap memory
                       disk
                       cleanable
                       ^HashMap native-bindings
                       disk-total
                       ^:unsynchronized-mutable meta]
  ISpillable

  (memory-count ^long [_] (.size memory))

  (disk-count ^long [_] @disk-total)

  (spill [this]
    (when-not @disk
      (let [dir (str spill-root "dtlv-spill-map-" (UUID/randomUUID))
            db  (l/open-kv dir {:temp? true :spill? true})]
        (vreset! spill-dir dir)
        (vreset! disk db)
        (register-spill-cleanup! this cleanable disk spill-dir)
        (i/open-dbi db c/tmp-dbi {:key-size Integer/BYTES})))
    this)

  IPersistentMap

  (assoc [this k v] (.put this k v) this)

  (without [this k] (.remove this k) this)

  (count [_] (+ (.size memory) ^long @disk-total))

  (containsKey [_ k]
    (if (.containsKey memory k)
      true
      (some? (disk-map-entry @disk native-bindings k))))

  (entryAt [_ k]
    (if (.containsKey memory k)
      (MapEntry. k (.get memory k))
      (disk-map-entry @disk native-bindings k)))

  (valAt [_ k nf]
    (if (.containsKey memory k)
      (.get memory k)
      (if-some [entry (disk-map-entry @disk native-bindings k)]
        (val entry)
        nf)))
  (valAt [this k]
    (.valAt this k nil))

  (cons [this [k v]]
    (.put this k v)
    this)

  (empty [this]
    (.clear memory)
    (when @disk
      (clean-spill! cleanable disk spill-dir))
    (.clear native-bindings)
    (vreset! disk-total 0)
    this)

  MapEquivalence

  (keySet [_]
    (set/union (set (.keySet memory))
               (set (map first (disk-map-entries @disk native-bindings)))))

  (equiv [this other]
    (cond
      (identical? this other) true

      (map? other)
      (if (not= (count this) (count other))
        false
        (every? true? (map #(Util/equiv (get this %) (get other %))
                           (.keySet this))))

      :else false))

  (seq ^ISeq [this]
    (when (< 0 (count this))
      (map #(MapEntry. % (.get this %)) (.keySet this))))

  Map

  (entrySet [this]
    (let [owner this]
      (proxy [AbstractSet] []
        (size [] (count owner))
        (iterator [] (.iterator owner)))))

  (size [this] (count this))

  (put [this k v]
    ;; Keep existing in-memory keys in memory. Once a disk portion exists,
    ;; new keys stay there even when pressure falls, so equal keys cannot
    ;; acquire separate entries in the two portions.
    (if (or (.containsKey memory k)
            (and (nil? @disk) (< ^long @memory-pressure spill-threshold)))
      (.put memory k v)
      (do
        (when (nil? @disk) (.spill this))
        (disk-map-put! @disk native-bindings disk-total k v))))

  (get [this k] (.valAt this k))

  (remove [_ k]
    (if (.containsKey memory k)
      (.remove memory k)
      (disk-map-remove! @disk native-bindings disk-total k)))

  (isEmpty [this] (= 0 (count this)))

  IFn

  (invoke [this arg1] (.valAt this arg1))

  IObj

  (withMeta [this m] (set! meta m) this)

  (meta [_] meta)

  Iterable

  (iterator [this]
    (locking this
      (let [i        (volatile! 0)
            kl       (FastList. (.keySet memory))
            mn       (.size kl)
            db       @disk
            di       (clojure.lang.RT/iter
                       (disk-map-entries db native-bindings))]
        (reify
          Iterator
          (hasNext [_]
            (or (< ^long @i mn) (.hasNext di)))
          (next [iter]
            (if (.hasNext iter)
              (let [^long idx @i
                    res      (if (< idx mn)
                               (let [k (.get kl idx)]
                                 (MapEntry. k (.get memory k)))
                               (let [[k v] (.next di)]
                                 (MapEntry. k v)))]
                (vswap! i u/long-inc)
                res)
              (throw (NoSuchElementException.))))))))

  Object

  (toString [this] (str (into {} this))))

(defn new-spillable-map
  ([] (new-spillable-map nil nil))
  ([m] (new-spillable-map m nil))
  ([m {:keys [spill-threshold spill-root]
       :or   {spill-threshold c/default-spill-threshold
              spill-root      c/default-spill-root}}]
   (when (empty? @listeners) (memory-updater))
   (let [smap (SpillableMap. spill-threshold
                             spill-root
                             (volatile! nil)
                             (UnifiedMap.)
                             (volatile! nil)
                             (volatile! nil)
                             (nv/spill-bindings)
                             (volatile! 0)
                             nil)]
     (doseq [[k v] m] (.put smap k v))
     smap)))

(nippy/extend-freeze
  SpillableMap :spillable-map
  [^SpillableMap x ^DataOutput out]
  (nippy/freeze-to-out! out (into {} x)))

(nippy/extend-thaw
  :spillable-map
  [^DataInput in]
  (new-spillable-map (nippy/thaw-from-in! in)))

(deftype SpillableSet [^SpillableMap impl
                       ^:unsynchronized-mutable meta]
  ISpillable

  (memory-count ^long [_] (memory-count impl))

  (disk-count ^long [_] (disk-count impl))

  (spill [this] (spill impl) this)

  IPersistentSet

  (count [_] (count impl))

  (contains[_ o] (.containsKey impl o))

  (cons [this o] (.put impl o c/slash) this)

  (empty [this] (.empty impl) this)

  (disjoin [this o] (.without impl o) this)

  (equiv [this other]
    (cond
      (identical? this other) true

      (set? other)
      (if (not= (count this) (count other))
        false
        (every? true? (map #(.contains ^Set other %) (seq this))))

      :else false))

  (seq ^ISeq [_] (seq (.keySet impl)))

  (get [_ o] (when (.valAt impl o) o))

  Set

  (size [_] (count impl))

  (isEmpty [this] (= 0 (count this)))

  IFn

  (invoke [_ arg1] (when (.valAt impl arg1) arg1))

  IObj

  (withMeta [this m] (set! meta m) this)

  (meta [_] meta)

  Iterable

  (iterator [_]
    (let [iter (.iterator impl)]
      (reify
        Iterator
        (hasNext [_] (.hasNext iter))
        (next [_] (key (.next iter))))))

  Collection

  (toArray [this] (object-array (seq this)))

  Object

  (toString [_] (str (into #{} (keys impl)))))

(defn new-spillable-set
  ([] (new-spillable-set nil nil))
  ([s] (new-spillable-set s nil))
  ([s {:keys [spill-threshold spill-root initial-capacity]
       :or   {spill-threshold c/default-spill-threshold
              spill-root      c/default-spill-root}}]
   (when (empty? @listeners) (memory-updater))
   (let [impl (SpillableMap. spill-threshold
                             spill-root
                             (volatile! nil)
                             (if (some? initial-capacity)
                               (UnifiedMap.
                                 (int
                                   (min (long Integer/MAX_VALUE)
                                        (max 0 (long initial-capacity)))))
                               (UnifiedMap.))
                             (volatile! nil)
                             (volatile! nil)
                             (nv/spill-bindings)
                             (volatile! 0)
                             nil)]
     (doseq [e s] (.put impl e c/slash))
     (SpillableSet. impl nil))))

(nippy/extend-freeze
  SpillableSet :spillable-set
  [^SpillableSet x ^DataOutput out]
  (nippy/freeze-to-out! out (into #{} x)))

(nippy/extend-thaw
  :spillable-set
  [^DataInput in]
  (new-spillable-set (nippy/thaw-from-in! in)))

(nippy/extend-freeze
  UniqueVectorSet :spillable-set
  [^UniqueVectorSet x ^DataOutput out]
  (nippy/freeze-to-out! out (into #{} x)))
