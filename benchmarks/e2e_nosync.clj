(ns e2e-nosync
  "End-to-end Datalevin bench with LMDB :nosync mode.
  Removes fsync overhead so codec share of latency is visible.

  Run: clj -M:bench -m e2e-nosync"
  (:require [criterium.core :as c]
            [datalevin.core :as d]))

(defn- fresh-conn []
  (let [dir (str "/tmp/dtlv-nosync-" (System/currentTimeMillis) "-" (rand-int 100000))]
    (d/get-conn dir
                {:name    {:db/valueType :db.type/string}
                 :email   {:db/valueType :db.type/string}
                 :age     {:db/valueType :db.type/long}
                 :active? {:db/valueType :db.type/boolean}
                 :owner   {:db/valueType :db.type/ref}
                 :tags    {:db/valueType :db.type/keyword
                           :db/cardinality :db.cardinality/many}}
                {:kv-opts {:flags #{:nosync}}})))

(defn- gen-people [n]
  (vec (for [i (range n)]
         {:db/id (- (inc i))
          :name (str "person-" i)
          :email (str "p" i "@example.com")
          :age (+ 18 (mod i 50))
          :active? (odd? i)
          :tags [(keyword (str "tag-" (mod i 10)))
                 (keyword (str "cat-" (mod i 5)))]})))

(defn -main [& _]
  (println "=== e2e Datalevin bench (LMDB :nosync) ===\n")

  ;; Warm
  (let [conn (fresh-conn)]
    (d/transact! conn (gen-people 100))
    (d/close conn))

  (println "--- batch transact (100 people) ---")
  (c/quick-bench
   (let [conn (fresh-conn)]
     (try
       (d/transact! conn (gen-people 100))
       (finally (d/close conn)))))

  (println "\n--- range scan (100 people) ---")
  (let [conn (fresh-conn)]
    (d/transact! conn (gen-people 100))
    (try
      (c/quick-bench
       (d/q '[:find ?e ?n :where [?e :name ?n]] @conn))
      (finally (d/close conn))))

  (println "\n--- entity pull ---")
  (let [conn (fresh-conn)]
    (d/transact! conn (gen-people 100))
    (try
      (c/quick-bench
       (into {} (d/touch (d/entity @conn 50))))
      (finally (d/close conn))))

  (println "\n--- transact + read roundtrip (100) ---")
  (c/quick-bench
   (let [conn (fresh-conn)]
     (try
       (d/transact! conn (gen-people 100))
       (d/q '[:find ?e ?n ?a
              :where [?e :name ?n] [?e :age ?a]] @conn)
       (finally (d/close conn))))))
