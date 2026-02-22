(ns datalevin-bench.datalevin
  (:require
   [datalevin.core :as d]
   [datalevin.query :as q]
   [datalevin.util :as u]
   [datalevin-bench.core :as core])
  (:import
   [java.util UUID]))


(def schema
  {:follows   {:db/valueType   :db.type/ref
               :db/cardinality :db.cardinality/many }
   :name      {:db/valueType :db.type/string}
   :last-name {:db/valueType :db.type/string}
   :sex       {:db/valueType :db.type/keyword}
   :age       {:db/valueType :db.type/long}
   :salary    {:db/valueType :db.type/long}})

(def bench-kv-opts
  {:flags         #{:nordahead :writemap :nosync}
   :wal-sync-mode :none})


(defn- wide-db
  "depth = 3 width = 2

   1
   ├ 2
   │ ├ 4
   │ │ ├ 8
   │ │ └ 9
   │ └ 5
   │   ├ 10
   │   └ 11
   └ 3
     ├ 6
     │ ├ 12
     │ └ 13
     └ 7
       ├ 14
       └ 15"
  ([depth width] (d/db-with (d/empty-db nil schema) (wide-db 1 depth width)))
  ([id depth width]
   (if (pos? depth)
     (let [children (map #(+ (* id width) %) (range width))]
       (concat
         (map #(array-map
                 :db/add  id
                 :name    "Ivan"
                 :follows %) children)
         (mapcat #(wide-db % (dec depth) width) children)))
     [{:db/id id :name "Ivan"}])))


(defn- long-db
  "depth = 3 width = 5

   1  4  7  10  13
   ↓  ↓  ↓  ↓   ↓
   2  5  8  11  14
   ↓  ↓  ↓  ↓   ↓
   3  6  9  12  15"
  [depth width]
  (d/db-with (d/empty-db nil schema)
             (apply concat
                    (for [x    (range width)
                          y    (range depth)
                          :let [from (+ (* x (inc depth)) y)
                                to   (+ (* x (inc depth)) y 1)]]
                      [{:db/id   from
                        :name    "Ivan"
                        :follows to}
                       {:db/id to
                        :name  "Ivan"}]))))


(def db100k-1
  (d/db-with (d/empty-db (u/tmp-dir (str "datalevin-bench-query1"
                                         (UUID/randomUUID)))
                         schema)
             core/people20k))

(def db100k-2
  (d/db-with (d/empty-db (u/tmp-dir (str "datalevin-bench-query2"
                                         (UUID/randomUUID)))
                         schema)
             core/people20k))

(def db100k-2s
  (d/db-with (d/empty-db (u/tmp-dir (str "datalevin-bench-query2s"
                                         (UUID/randomUUID)))
                         schema)
             core/people20k))

(def db100k-3
  (d/db-with (d/empty-db (u/tmp-dir (str "datalevin-bench-query3"
                                         (UUID/randomUUID)))
                         schema)
             core/people20k))

(def db100k-4
  (d/db-with (d/empty-db (u/tmp-dir (str "datalevin-bench-query4"
                                         (UUID/randomUUID)))
                         schema)
             core/people20k))

(def db100k-5
  (d/db-with (d/empty-db (u/tmp-dir (str "datalevin-bench-query5"
                                         (UUID/randomUUID)))
                         schema)
             core/people20k))

(def db100k-p1
  (d/db-with (d/empty-db (u/tmp-dir (str "datalevin-bench-queryp1"
                                         (UUID/randomUUID)))
                         schema)
             core/people20k))
(def db100k-p2
  (d/db-with (d/empty-db (u/tmp-dir (str "datalevin-bench-queryp2"
                                         (UUID/randomUUID)))
                         schema)
             core/people20k))

(defn ^:export add-1 []
  (core/bench-10
    (d/close-db
      (reduce
        (fn [db p]
          (-> db
              (d/db-with [[:db/add (:db/id p) :name      (:name p)]])
              (d/db-with [[:db/add (:db/id p) :last-name (:last-name p)]])
              (d/db-with [[:db/add (:db/id p) :sex       (:sex p)]])
              (d/db-with [[:db/add (:db/id p) :age       (:age p)]])
              (d/db-with [[:db/add (:db/id p) :salary    (:salary p)]])))
        (d/empty-db (u/tmp-dir (str "datalevin-bench-add-1" (UUID/randomUUID)))
                    schema {:kv-opts bench-kv-opts})
        core/people20k))))


(defn ^:export add-5 []
  (core/bench-10
    (d/close-db
              (reduce (fn [db p] (d/db-with db [p]))
              (d/empty-db (u/tmp-dir (str "datalevin-bench-add-5"
                                          (UUID/randomUUID)))
                          schema {:kv-opts bench-kv-opts})
              core/people20k))))


(defn ^:export add-all []
  (core/bench-10
    (d/close-db
      (d/db-with
        (d/empty-db (u/tmp-dir (str "datalevin-bench-add-all"
                                    (UUID/randomUUID)))
                    schema {:kv-opts bench-kv-opts})
        core/people20k))))


(defn ^:export init []
  (let [datoms (into []
                     (for [p     core/people20k
                           :let  [id (#?(:clj Integer/parseInt :cljs js/parseInt)
                                       (:db/id p))]
                           [k v] p
                           :when (not= k :db/id)]
                       (d/datom id k v)))]
    (core/bench-10
      (d/close-db
        (d/init-db datoms (u/tmp-dir (str "datalevin-bench-init"
                                          (UUID/randomUUID)))
                   schema {:kv-opts bench-kv-opts})))))


(defn ^:export retract-5 []
  (let [db   (d/db-with
               (d/empty-db (u/tmp-dir (str "datalevin-bench-retract"
                                           (UUID/randomUUID)))
                           schema {:kv-opts bench-kv-opts})
               core/people20k)
        eids (->> (d/datoms db :ave :name) (map :e) (shuffle))
        res  (core/bench-10
               (reduce (fn [db eid] (d/db-with db [[:db.fn/retractEntity eid]]))
                       db eids))]
    (d/close-db db)
    res))

;; each query gets an identical DB, to remove caching effect

(defn ^:export q1 []
  (let [res (core/bench
              (d/q '[:find ?e
                     :where [?e :name "Ivan"]]
                   db100k-1))]
    (d/close-db db100k-1)
    res))

(defn ^:export q2 []
  (let [res (core/bench
              (d/q '[:find ?e ?a
                     :where
                     [?e :name "Ivan"]
                     [?e :age ?a]]
                   db100k-2))]
    (d/close-db db100k-2)
    res))

(defn ^:export q2-switch []
  (let [res (core/bench
              (d/q '[:find ?e ?a
                     :where
                     [?e :age ?a]
                     [?e :name "Ivan"]]
                   db100k-2s))]
    (d/close-db db100k-2s)
    res))

(defn ^:export q3 []
  (let [res (core/bench
              (d/q '[:find ?e ?a
                     :where [?e :name "Ivan"]
                     [?e :age ?a]
                     [?e :sex :male]]
                   db100k-3))]
    (d/close-db db100k-3)
    res))


(defn ^:export q4 []
  (let [res (core/bench
              (d/q '[:find ?e ?l ?a
                     :where [?e :name "Ivan"]
                     [?e :last-name ?l]
                     [?e :age ?a]
                     [?e :sex :male]]
                   db100k-4))]
    (d/close-db db100k-4)
    res))

(defn ^:export q5 []
  (let [res (core/bench
              (d/q '[:find ?e1 ?l ?a
                     :where [?e :name "Ivan"]
                     [?e :age ?a]
                     [?e1 :age ?a]
                     [?e1 :last-name ?l]]
                   db100k-5))]
    (d/close-db db100k-5)
    res))

(defn ^:export qpred1 []
  (let [res (core/bench
              (d/q '[:find ?e ?s
                     :where [?e :salary ?s]
                     [(> ?s 50000)]]
                   db100k-p1))]
    (d/close-db db100k-p1)
    res))

(defn ^:export qpred2 []
  (let [res (core/bench
              (d/q '[:find ?e ?s
                     :in   $ ?min_s
                     :where [?e :salary ?s]
                     [(> ?s ?min_s)]]
                   db100k-p2 50000))]
    (d/close-db db100k-p2)
    res))

(defn bench-rules [db]
  (d/q '[:find ?e ?e2
         :in   $ %
         :where (follows ?e ?e2)]
       db
       '[[(follows ?x ?y)
          [?x :follows ?y]]
         [(follows ?x ?y)
          [?x :follows ?t]
          (follows ?t ?y)]]))

(defn ^:export rules-wide-3x3 []
  (let [db  (wide-db 3 3)
        res (core/bench (bench-rules db))]
    (d/close-db db)
    res))

(defn ^:export rules-wide-5x3 []
  (let [db  (wide-db 5 3)
        res (core/bench (bench-rules db))]
    (d/close-db db)
    res))

(defn ^:export rules-wide-7x3 []
  (let [db  (wide-db 7 3)
        res (core/bench (bench-rules db))]
    (d/close-db db)
    res))

(defn ^:export rules-wide-4x6 []
  (let [db  (wide-db 4 6)
        res (core/bench (bench-rules db))]
    (d/close-db db)
    res))

(defn ^:export rules-long-10x3 []
  (let [db  (long-db 10 3)
        res (core/bench (bench-rules db))]
    (d/close-db db)
    res))

(defn ^:export rules-long-30x3 []
  (let [db  (long-db 30 3)
        res (core/bench (bench-rules db))]
    (d/close-db db)
    res))

(defn ^:export rules-long-30x5 []
  (let [db  (long-db 30 5)
        res (core/bench (bench-rules db))]
    (d/close-db db)
    res))

(defn ^:export -main [& names]
  (doseq [n names]
    (if-some [benchmark (ns-resolve 'datalevin-bench.datalevin (symbol n))]
      (let [perf (if q/*cache?*
                   (binding [q/*cache?* false] (benchmark))
                   (benchmark))]
        (print (core/round perf) "\t")
        (flush))
      (do
        (print "---" "\t")
        (flush))))
  (println))
