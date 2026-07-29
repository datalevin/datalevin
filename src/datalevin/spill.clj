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
   [s-exp.hako.ext :as hext]
   [clojure.set :as set])
  (:import
   [com.s_exp.hako Reader Writer]
   [java.util Iterator List UUID NoSuchElementException Map Set Collection]
   [java.lang.ref Cleaner Cleaner$Cleanable]
   [java.lang.management ManagementFactory]
   [javax.management NotificationEmitter NotificationListener Notification]
   [com.sun.management GarbageCollectionNotificationInfo]
   [org.eclipse.collections.impl.map.mutable UnifiedMap]
   [org.eclipse.collections.impl.list.mutable FastList]
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

(defn- ^Cleaner spill-cleaner
  []
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

(deftype SpillableVector [^long spill-threshold
                          ^String spill-root
                          spill-dir
                          ^FastList memory
                          disk
                          cleanable
                          total
                          ^:unsynchronized-mutable meta]
  ISpillable

  (memory-count ^long [_] (.size memory))

  (disk-count ^long [_]
    (if @disk (i/entries @disk c/tmp-dbi) 0))

  (spill [this]
    (when-not @disk
      (let [dir (str spill-root "dtlv-spill-vec-" (UUID/randomUUID))
            db  (l/open-kv dir {:temp? true})]
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
        (< i mc) (.add memory i v)
        (< i tc) (i/transact-kv @disk [(l/kv-tx :put c/tmp-dbi i v :id)])
        :else    (throw (IndexOutOfBoundsException.))))
    this)

  (cons [this v]
    (let [mem? (nil? @disk)]
      (if (and (< ^long @memory-pressure spill-threshold) mem?)
        (.add memory v)
        (do (when mem? (.spill this))
            (i/transact-kv @disk [(l/kv-tx :put c/tmp-dbi @total v :id)]))))
    (vswap! total u/long-inc)
    this)

  (length [_] @total)

  (assoc [this k v]
    (if (integer? k)
      (.assocN this k v)
      (throw (IllegalArgumentException. "Key must be integer"))))

  (containsKey [this k]
    (if (integer? k)
      (if (some? (get this k))
        true
        (if @disk
          (some? (i/get-value @disk c/tmp-dbi k :id))
          false))
      false))

  (entryAt [_ k]
    (when (integer? k)
      (if-some [v (.get memory k)]
        (MapEntry. k v)
        (when-some [v (i/get-value @disk c/tmp-dbi k :id)]
          (MapEntry. k v)))))

  (valAt [_ k nf]
    (if (integer? k)
      (cond
        (< ^long k (.size memory)) (.get memory k)
        @disk                      (i/get-value @disk c/tmp-dbi k :id)
        :else                      nf)
      nf))
  (valAt [this k]
    (.valAt this k nil))

  (peek [this]
    (if (zero? ^long (disk-count this))
      (.getLast memory)
      (i/get-first @disk c/tmp-dbi [:all-back] :id :data true)))

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

(hext/register-user-tag!
 0x10000002                             ; private range, subtag 2 = spillable-vec
 SpillableVector
 (fn write-spillable-vec [^Writer w ^SpillableVector x]
   (let [n (count x)]
     (.putI64 w n)
     (dotimes [i n] (.writeAny w (nth x i)))))
 (fn read-spillable-vec [^Reader r]
   (let [n (.getI64 r)
         ^SpillableVector vs (new-spillable-vector)]
     (dotimes [_ n] (.cons vs (.readAny r)))
     vs)))

(deftype SpillableMap [^long spill-threshold
                       ^String spill-root
                       spill-dir
                       ^UnifiedMap memory
                       disk
                       cleanable
                       ^:unsynchronized-mutable meta]
  ISpillable

  (memory-count ^long [_] (.size memory))

  (disk-count ^long [_] (if @disk (i/entries @disk c/tmp-dbi) 0))

  (spill [this]
    (when-not @disk
      (let [dir (str spill-root "dtlv-spill-map-" (UUID/randomUUID))
            db  (l/open-kv dir {:temp? true})]
        (vreset! spill-dir dir)
        (vreset! disk db)
        (register-spill-cleanup! this cleanable disk spill-dir)
        (i/open-dbi db c/tmp-dbi {:key-size Integer/BYTES})))
    this)

  IPersistentMap

  (assoc [this k v] (.put this k v) this)

  (without [this k] (.remove this k) this)

  (count [_]
    (cond-> (.size memory) @disk (+ ^long (i/entries @disk c/tmp-dbi))))

  (containsKey [_ k]
    (if (.containsKey memory k)
      true
      (if @disk
        (some? (i/get-value @disk c/tmp-dbi k))
        false)))

  (entryAt [_ k]
    (if-some [v (.get memory k)]
      (MapEntry. k v)
      (when-some [v (i/get-value @disk c/tmp-dbi k)]
        (MapEntry. k v))))

  (valAt [_ k nf]
    (if-some [v (.get memory k)]
      v
      (if @disk
        (if-some [v (i/get-value @disk c/tmp-dbi k)]
          v
          nf)
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
    this)

  MapEquivalence

  (keySet [_]
    (set/union (set (.keySet memory))
               (when @disk (set (i/key-range @disk c/tmp-dbi [:all])))))

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

  (size [this] (count this))

  (put [this k v]
    (if (< ^long @memory-pressure spill-threshold)
      (.put memory k v)
      (when-not (= (.get memory k) v)
        (when (nil? @disk) (.spill this))
        (i/transact-kv @disk [(l/kv-tx :put c/tmp-dbi k v)]))))

  (get [this k] (.valAt this k))

  (remove [_ k]
    (if (.containsKey memory k)
      (.remove memory k)
      (when (and @disk (i/get-value @disk c/tmp-dbi k))
        (i/transact-kv @disk [(l/kv-tx :del c/tmp-dbi k)]))))

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
            ^long dn (if db (i/entries db c/tmp-dbi) 0)
            dl       (when-not (zero? dn)
                       (i/get-range db c/tmp-dbi [:all]))]
        (reify
          Iterator
          (hasNext [_]
            (let [^long di @i]
              (or (< di mn)
                  (if db (< di (+ mn dn)) false))))
          (next [iter]
            (if (.hasNext iter)
              (let [^long di @i
                    res      (if (< di mn)
                               (let [k (.get kl di)]
                                 (MapEntry. k (.get memory k)))
                               (let [[k v] (nth dl (- di mn))]
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
                             nil)]
     (doseq [[k v] m] (assoc smap k v))
     smap)))

(hext/register-user-tag!
 0x10000003                             ; private range, subtag 3 = spillable-map
 SpillableMap
 (fn write-spillable-map [^Writer w ^SpillableMap x]
   (.writeAny w (into {} x)))
 (fn read-spillable-map [^Reader r]
   (new-spillable-map (.readAny r))))

(deftype SpillableSet [^SpillableMap impl
                       ^:unsynchronized-mutable meta]
  ISpillable

  (memory-count ^long [_] (memory-count impl))

  (disk-count ^long [_] (disk-count impl))

  (spill [this] (spill impl) this)

  IPersistentSet

  (count [_] (count impl))

  (contains [_ o] (.containsKey impl o))

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
  ([s {:keys [spill-threshold spill-root]
       :or   {spill-threshold c/default-spill-threshold
              spill-root      c/default-spill-root}}]
   (when (empty? @listeners) (memory-updater))
   (let [impl (SpillableMap. spill-threshold
                             spill-root
                             (volatile! nil)
                             (UnifiedMap.)
                             (volatile! nil)
                             (volatile! nil)
                             nil)]
     (doseq [e s] (.put impl e c/slash))
     (SpillableSet. impl nil))))

(hext/register-user-tag!
 0x10000004                             ; private range, subtag 4 = spillable-set
 SpillableSet
 (fn write-spillable-set [^Writer w ^SpillableSet x]
   (.writeAny w (into #{} x)))
 (fn read-spillable-set [^Reader r]
   (new-spillable-set (.readAny r))))
