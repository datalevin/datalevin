(ns alloc-read-profile
  "Fine-grained alloc profile for Datalevin read paths. Break down where
  the ~8 KB/op range-scan allocation goes.

  Run: clj -M:bench -m alloc-read-profile"
  (:require [datalevin.core :as d]
            [datalevin.bits :as b])
  (:import (com.sun.management ThreadMXBean)
           (java.lang.management ManagementFactory)))

(defn- ^ThreadMXBean tmx []
  (ManagementFactory/getThreadMXBean))

(defn- allocated-bytes ^long [^ThreadMXBean b]
  (.getThreadAllocatedBytes b (.getId (Thread/currentThread))))

(defn- profile [label f warmup n]
  (dotimes [_ warmup] (f))
  (System/gc)
  (let [b (tmx)
        start (allocated-bytes b)
        _ (dotimes [_ n] (f))
        end (allocated-bytes b)
        per-op (/ (double (- end start)) n)]
    (println (format "  %-40s %10.1f B/op" label per-op))
    per-op))

(defn- fresh-conn []
  (let [dir (str "/tmp/dtlv-alloc-read-" (System/currentTimeMillis) "-" (rand-int 100000))]
    (d/get-conn dir
                {:name {:db/valueType :db.type/string}
                 :age  {:db/valueType :db.type/long}}
                {:kv-opts {:flags #{:nosync}}})))

(defn- gen-people [n]
  (vec (for [i (range n)]
         {:db/id (- (inc i)) :name (str "p-" i) :age (+ 18 (mod i 50))})))

(defn -main [& _]
  (println "=== Datalevin read-path alloc profile ===\n")
  (let [conn (fresh-conn)]
    (d/transact! conn (gen-people 100))
    (try
      (println "-- baseline --")
      (profile "empty fn" (fn [] nil) 1000 100000)
      (profile "db deref"     (fn [] @conn) 1000 100000)

      (println "\n-- query variants --")
      (profile "q :find ?e"
               (fn [] (d/q '[:find ?e :where [?e :name]] @conn)) 100 10000)
      (profile "q :find ?e ?n"
               (fn [] (d/q '[:find ?e ?n :where [?e :name ?n]] @conn)) 100 10000)
      (profile "q :find ?n"
               (fn [] (d/q '[:find ?n :where [?e :name ?n]] @conn)) 100 10000)

      (println "\n-- entity paths --")
      (profile "d/entity"
               (fn [] (d/entity @conn 50)) 100 10000)
      (profile "d/entity + touch"
               (fn [] (d/touch (d/entity @conn 50))) 100 10000)
      (profile "d/entity + touch + into map"
               (fn [] (into {} (d/touch (d/entity @conn 50)))) 100 10000)

      (println "\n-- direct datom access --")
      (profile "-datoms :eav (100)"
               (fn []
                 (let [ds (into [] (datalevin.db/-datoms @conn :eav nil nil nil))]
                   (count ds)))
               100 1000)

      (println "\n-- codec-only micro (b/deserialize) --")
      (let [encoded (b/serialize {:name "Alice" :age 30})]
        (profile "b/deserialize small map"
                 (fn [] (b/deserialize encoded)) 1000 100000))

      (finally (d/close conn)))))
