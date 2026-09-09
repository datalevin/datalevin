(ns datalevin.test.spill
  (:require
   [clojure.test :refer [deftest is]]
   [datalevin.binding.cpp]
   [datalevin.bits :as b]
   [datalevin.interface :as i]
   [datalevin.lmdb :as l]
   [datalevin.native-value :as nv]
   [datalevin.spill :as sp]
   [datalevin.util :as u]
   [taoensso.nippy :as nippy])
  (:import
   [datalevin NativeValue]
   [datalevin.spill SpillableMap SpillableSet SpillableVector]
   [datalevin.utl UniqueVectorSet]
   [java.io File]
   [java.lang.reflect Method]
   [java.util.function BiPredicate]
   [java.util.concurrent ExecutorService]
   [org.eclipse.collections.impl.list.mutable FastList]))

(defn- finalize-methods
  [^Class klass]
  (filter (fn [^Method m] (= "finalize" (.getName m)))
          (.getDeclaredMethods klass)))

(defn- child-paths
  [root]
  (set (map #(.getPath ^File %) (or (u/list-files root) []))))

(deftest spillable-types-do-not-declare-finalizers
  (doseq [klass [SpillableVector SpillableMap SpillableSet]]
    (is (empty? (finalize-methods klass)))))

(deftest spillable-set-supports-initial-capacity
  (let [s (sp/new-spillable-set (range 20) {:initial-capacity 32})]
    (is (= (set (range 20)) s))))

(deftest unique-vector-set-roundtrip
  (let [tuples (doto (FastList.)
                 (.add (object-array [0 31]))
                 (.add (object-array [1 0])))
        expected #{[0 31] [1 0]}
        s        (UniqueVectorSet/fromUniqueTuples tuples)
        thawed   (nippy/thaw (nippy/freeze s))]
    (is (= expected s))
    (is (= s expected))
    (is (= (hash expected) (hash s)))
    (is (= (.hashCode expected) (.hashCode s)))
    (is (contains? s [1 0]))
    (is (= {:source :test}
           (meta (conj (with-meta s {:source :test}) [2 2]))))
    (is (= s thawed))))

(defn- assert-spill-dir-cleaned!
  [root cleanup-fn]
  (let [paths (child-paths root)]
    (is (= 1 (count paths)))
    (cleanup-fn)
    (is (empty? (child-paths root)))
    (doseq [p paths]
      (is (not (u/file-exists p))))))

(deftest explicit-cleanup-removes-spill-dirs
  (let [root (u/tmp-dir (str "spill-cleanup-test-" (System/nanoTime)))
        _    (.mkdirs (File. ^String root))
        opts {:spill-threshold -1
              :spill-root      (str root u/+separator+)}]
    (try
      (let [^SpillableVector v (sp/new-spillable-vector nil opts)]
        (.cons v :a)
        (is (= 1 (sp/disk-count v)))
        (assert-spill-dir-cleaned!
         root
         (fn []
           (.empty v)
           (is (zero? (sp/disk-count v))))))
      (let [^SpillableMap m (sp/new-spillable-map nil opts)]
        (.put m 1 :a)
        (is (= 1 (sp/disk-count m)))
        (assert-spill-dir-cleaned!
         root
         (fn []
           (.empty m)
           (is (zero? (sp/disk-count m))))))
      (finally
        (when (u/file-exists root)
          (u/delete-files root))))))

(defn- native-value [codec logical nonce]
  (NativeValue. codec ":app/test" (b/serialize [codec logical nonce])
                (reify BiPredicate
                  (test [_ left right]
                    (= (second (b/deserialize (.payload ^NativeValue left)))
                       (second (b/deserialize (.payload ^NativeValue right))))))))

(deftest native-spill-codec-requires-owning-bindings
  (let [value    (native-value "one" :a 1)
        bindings (nv/spill-bindings)
        bytes    (binding [nv/*spill-bindings* bindings]
                   (nippy/fast-freeze [value]))]
    (is (thrown? Exception (nippy/fast-freeze value)))
    (is (thrown? Exception (nippy/fast-thaw bytes)))
    (is (thrown? Exception
                 (binding [nv/*spill-bindings* (nv/spill-bindings)]
                   (nippy/fast-thaw bytes))))
    (let [[^NativeValue restored]
          (binding [nv/*spill-bindings* bindings] (nippy/fast-thaw bytes))]
      (is (= value restored))
      (is (= "one" (.codecId restored)))
      (is (= (seq (.payload value)) (seq (.payload restored)))))))

(deftest native-spill-vector-roundtrip-and-reuse
  (let [a (native-value "one" :a 1)
        b (native-value "two" :b 2)
        ^SpillableVector v (sp/new-spillable-vector nil {:spill-threshold -1})]
    (try
      (.cons v {:nested [a b]})
      (.cons v nil)
      (is (= 2 (sp/disk-count v)))
      (is (= {:nested [a b]} (nth v 0)))
      (is (contains? v 1))
      (is (= [1 nil] (find v 1)))
      (.assocN v 0 [b a])
      (is (= [[b a] nil] (vec v)))
      (is (= [nil [b a]] (vec (rseq v))))
      (.pop v)
      (is (= [b a] (peek v)))
      (is (thrown? Exception (conj v (Object.))))
      (is (= 1 (count v)))
      (.empty v)
      (is (zero? (count v)))
      (.cons v b)
      (is (= [b] (vec v)))
      (finally (.empty v)))))

(deftest spilled-native-map-and-set-use-logical-key-equality
  (let [a  (native-value "one" :a 1)
        a2 (native-value "two" :a 2)
        b  (native-value "one" :b 3)
        ^SpillableMap m (sp/new-spillable-map nil {:spill-threshold -1})
        ^SpillableSet s (sp/new-spillable-set nil {:spill-threshold -1})]
    (try
      ;; Nested native keys may have equal values but different payloads/codecs.
      (.put m [a] nil)
      (.put m [b] false)
      (is (= 2 (sp/disk-count m)))
      (is (contains? m [a2]))
      (is (nil? (get m [a2] :missing)))
      (is (= [[a] nil] (find m [a2])))
      (.put m [a2] :updated)
      (is (= 2 (count m)))
      (is (= :updated (get m [a])))
      (is (= {[a] :updated [b] false} (into {} m)))
      (is (thrown? Exception (assoc m [a2] (Object.))))
      (is (= :updated (get m [a])))
      (.without m [a2])
      (is (= {[b] false} (into {} m)))
      (.without m [b])
      (is (zero? (sp/disk-count m)))
      (.cons s [a])
      (.cons s [a2])
      (.cons s [b])
      (is (= 2 (count s)))
      (is (= #{[a] [b]} (set (seq s))))
      (.disjoin s [a2])
      (is (= #{[b]} (set (seq s))))
      (finally (.empty m) (.empty s)))))

(deftest spill-map-preserves-entries-across-pressure-changes
  (with-redefs [sp/memory-pressure (volatile! 0)]
    (let [^SpillableMap m (sp/new-spillable-map)
          a (native-value "one" :a 1)
          b (native-value "one" :b 2)]
      (try
        (.put m a nil)
        (vreset! sp/memory-pressure 99)
        (.put m b false)
        (.put m (native-value "two" :a 3) :memory)
        (vreset! sp/memory-pressure 0)
        (.put m (native-value "two" :b 4) :disk)
        (is (= 1 (sp/memory-count m)))
        (is (= 1 (sp/disk-count m)))
        (is (= {a :memory b :disk} (into {} m)))
        (.empty m)
        (is (zero? (count m)))
        (.put m :reused nil)
        (is (= {:reused nil} (into {} m)))
        (finally (.empty m))))))

(deftest live-spill-does-not-prevent-application-executor-shutdown
  (let [db (l/open-kv (u/tmp-dir (str "spill-lifetime-" (System/nanoTime)))
                      {:temp? true})
        ^SpillableVector v (sp/new-spillable-vector nil {:spill-threshold -1})
        ^ExecutorService pool (u/get-worker-thread-pool)
        value (native-value "lifetime" :a 1)]
    (try
      (.cons v value)
      (i/close-kv db)
      (is (.isShutdown pool))
      ;; A caller may still consume an already-produced spilled result.
      (is (= value (first v)))
      (is (= 1 (sp/disk-count v)))
      (finally
        (.empty v)
        (i/close-kv db)))))
