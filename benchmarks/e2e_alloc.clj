(ns e2e-alloc
  "Per-op allocation profile for Datalevin workloads.
  Measures bytes allocated via ThreadMXBean.getThreadAllocatedBytes
  over a warmed loop. Run same script on master + hako-codec, diff.

  Run: clj -M:bench -m e2e-alloc"
  (:require [datalevin.core :as d])
  (:import (com.sun.management ThreadMXBean)
           (java.lang.management ManagementFactory)))

(defn- ^ThreadMXBean tmx []
  (ManagementFactory/getThreadMXBean))

(defn- allocated-bytes ^long [^ThreadMXBean b]
  (.getThreadAllocatedBytes b (.getId (Thread/currentThread))))

(defn- fresh-conn []
  (let [dir (str "/tmp/dtlv-alloc-" (System/currentTimeMillis) "-" (rand-int 100000))]
    (d/get-conn dir
                {:name    {:db/valueType :db.type/string}
                 :email   {:db/valueType :db.type/string}
                 :age     {:db/valueType :db.type/long}
                 :active? {:db/valueType :db.type/boolean}
                 :owner   {:db/valueType :db.type/ref}
                 :tags    {:db/valueType :db.type/keyword
                           :db/cardinality :db.cardinality/many}})))

(defn- gen-people [n]
  (vec (for [i (range n)]
         {:db/id (- (inc i))
          :name (str "person-" i)
          :email (str "p" i "@example.com")
          :age (+ 18 (mod i 50))
          :active? (odd? i)
          :tags [(keyword (str "tag-" (mod i 10)))
                 (keyword (str "cat-" (mod i 5)))]})))

(defn- profile
  "Warm, then measure alloc delta over N iterations. Returns bytes/op."
  [label f warmup n]
  (dotimes [_ warmup] (f))
  (System/gc)
  (let [b (tmx)
        start (allocated-bytes b)
        _ (dotimes [_ n] (f))
        end (allocated-bytes b)
        total (- end start)
        per-op (/ (double total) n)]
    (println (format "  %-30s %12.0f B/op  (%d ops)" label per-op n))
    per-op))

(defn -main [& _]
  (println "=== Datalevin per-op allocation profile ===\n")

  (println "--- batch transact (100 people) ---")
  (profile "batch transact"
           (fn []
             (let [conn (fresh-conn)]
               (try (d/transact! conn (gen-people 100))
                    (finally (d/close conn)))))
           5 50)

  (println "\n--- range scan (100 people, :name query) ---")
  (let [conn (fresh-conn)
        _ (d/transact! conn (gen-people 100))]
    (try
      (profile "range scan"
               (fn [] (d/q '[:find ?e ?n :where [?e :name ?n]] @conn))
               100 10000)
      (finally (d/close conn))))

  (println "\n--- entity pull ---")
  (let [conn (fresh-conn)
        _ (d/transact! conn (gen-people 100))]
    (try
      (profile "entity pull"
               (fn [] (into {} (d/touch (d/entity @conn 50))))
               100 10000)
      (finally (d/close conn))))

  (println "\n--- full round-trip (transact + read 100) ---")
  (profile "roundtrip"
           (fn []
             (let [conn (fresh-conn)]
               (try
                 (d/transact! conn (gen-people 100))
                 (d/q '[:find ?e ?n ?a
                        :where [?e :name ?n] [?e :age ?a]] @conn)
                 (finally (d/close conn)))))
           5 50))
