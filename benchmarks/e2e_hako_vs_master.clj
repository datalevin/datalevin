(ns e2e-hako-vs-master
  "End-to-end Datalevin bench for the hako branch.

  Measure realistic workloads: batch transact, range scan, entity pull.
  Run twice — once on this branch (hako), once on master (nippy) —
  and diff the numbers.

  Run: clj -M:bench -m e2e-hako-vs-master"
  (:require [criterium.core :as c]
            [datalevin.core :as d]))

(defn- fresh-conn []
  (let [dir (str "/tmp/dtlv-bench-" (System/currentTimeMillis) "-" (rand-int 100000))]
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

(defn -main [& _]
  (println "=== e2e Datalevin bench ===")
  (println "hako branch — measure vs master baseline separately.\n")

  ;; Warm the LMDB layer.
  (let [conn (fresh-conn)]
    (d/transact! conn (gen-people 100))
    (d/close conn))

  (println "--- batch transact (100 people, schema-typed) ---")
  (c/quick-bench
   (let [conn (fresh-conn)]
     (try
       (d/transact! conn (gen-people 100))
       (finally (d/close conn)))))

  (println "\n--- range scan (100 people, :find ?e ?n :where [?e :name ?n]) ---")
  (let [conn (fresh-conn)]
    (d/transact! conn (gen-people 100))
    (try
      (c/quick-bench
       (d/q '[:find ?e ?n :where [?e :name ?n]] @conn))
      (finally (d/close conn))))

  (println "\n--- entity pull (single :name lookup + touch) ---")
  (let [conn (fresh-conn)]
    (d/transact! conn (gen-people 100))
    (try
      (c/quick-bench
       (into {} (d/touch (d/entity @conn 50))))
      (finally (d/close conn))))

  (println "\n--- transact then read: full round-trip (100 people) ---")
  (c/quick-bench
   (let [conn (fresh-conn)]
     (try
       (d/transact! conn (gen-people 100))
       (d/q '[:find ?e ?n ?a
              :where [?e :name ?n] [?e :age ?a]] @conn)
       (finally (d/close conn))))))
