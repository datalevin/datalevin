(ns datalevin.test.spill
  (:require
   [clojure.test :refer [deftest is]]
   [datalevin.binding.cpp]
   [datalevin.spill :as sp]
   [datalevin.util :as u])
  (:import
   [datalevin.spill SpillableMap SpillableSet SpillableVector]
   [java.io File]
   [java.lang.reflect Method]))

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
