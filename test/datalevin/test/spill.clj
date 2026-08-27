(ns datalevin.test.spill
  (:require
   [clojure.test :refer [deftest is]]
   [datalevin.binding.cpp]
   [datalevin.spill :as sp]
   [datalevin.util :as u]
   [taoensso.nippy :as nippy])
  (:import
   [datalevin.spill SpillableMap SpillableSet SpillableVector]
   [datalevin.utl UniqueVectorSet]
   [java.io File]
   [java.lang.reflect Method]
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
