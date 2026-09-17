(ns ^:no-doc datalevin.query.plan-cache
  "Lifecycle management for shared query plans. Independent of the planner so
  storage can evict plans without loading the query engine."
  (:require [datalevin.interface :as i])
  (:import [java.util WeakHashMap]
           [datalevin.utl LRUCache]))

;; Include dynamically bound caches without keeping them alive after their
;; callers release them. Registration happens only when a plan is published.
(defonce ^:private caches (WeakHashMap.))

(defn store-key [store]
  [(i/dir store) (i/db-name store)])

(defn put!
  [^LRUCache cache key entry current?]
  (locking caches
    ;; Coordinate publication with close, including a close during planning.
    (when (current?)
      (.put caches cache true)
      (.put cache key entry))))

(defn evict-store!
  "Remove plans depending on this directory, including earlier names and
  plans held in dynamically bound caches."
  [store]
  (let [dir (i/dir store)]
    (locking caches
      (doseq [^LRUCache cache (.keySet caches)
              key (.keys cache)
              :let [entry (.get cache key)]
              :when (some #(= dir (first %)) (:store-keys entry))]
        (.remove cache key)))))
