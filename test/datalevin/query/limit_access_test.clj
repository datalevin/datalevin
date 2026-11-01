(ns datalevin.query.limit-access-test
  (:require [clojure.test :refer [deftest is]]
            [datalevin.core :as d]
            [datalevin.query :as q]
            [datalevin.query.access :as access]
            [datalevin.query.access.ave :as ave]
            [datalevin.query.plan :as plan]))

(def opts {:wal? false :background-sampling? false
           :kv-opts {:inmemory? true :wal? false}})

(deftest unordered-limit-avoids-materializing-fanout
  (with-open [conn (d/create-conn
                    nil {:keep {:db/valueType :db.type/boolean}
                         :tag {:db/valueType :db.type/string
                               :db/cardinality :db.cardinality/many}} opts)]
    (let [tags (mapv #(str "tag-" %) (range 32))
          query '[:find ?e ?tag :where [?e :keep true] [?e :tag ?tag]
                  :limit 10]
          explanation (volatile! {})]
      (d/transact! conn (mapv #(hash-map :db/id % :keep true :tag tags)
                             (range 1 1001)))
      (let [rows (binding [q/*cache?* false plan/*explain* explanation
                           plan/*intermediate-counts?* false
                           plan/*start-time* (System/nanoTime)]
                   (d/q query @conn))
            explained @explanation]
        (is (= 10 (count rows) (count (set rows))))
        (is (every? (fn [[e tag]] (and (<= 1 e 1000) ((set tags) tag))) rows))
        (is (= :adaptive-limit (get-in explained [:executed-plan-alternative :mode])))
        (is (= :unordered-scan (get-in explained [:preferred-access-plan :strategy])))
        (is (= 10 (get-in explained [:plan :candidate-work])))
        (is (= 10 (:actual-result-size explained)))))))

(deftest ave-cursor-resumes-false-values
  (with-open [conn (d/create-conn nil {:flag {:db/valueType :db.type/boolean}} opts)]
    (d/transact! conn (mapv #(hash-map :db/id % :flag (> % 7)) (range 1 11)))
    (let [path (ave/ordered-path @conn :flag :asc nil '?flag)
          demand (access/limit-demand 0 10 :exact #{'?e '?flag})
          work (access/->AccessWork nil 3 20 nil 0)
          cursor (access/open-access path demand work @conn)]
      (try
        (let [sample (access/next-batch cursor)
              resumed (access/open-access
                        path demand (assoc work :resume (:frontier sample) :emitted 3)
                        @conn)]
          (is (= false (get-in sample [:frontier :continuation :key])))
          (try
            (let [rows (loop [rows (mapv vec (:tuples sample)) n 0]
                         (if (= n 5)
                           rows
                           (let [batch (access/next-batch resumed)
                                 rows (into rows (map vec) (:tuples batch))]
                             (if (:exhausted? batch) rows (recur rows (inc n))))))]
              (is (= 10 (count rows) (count (set rows))))
              (is (= (set (map #(vector % (> % 7)) (range 1 11))) (set rows))))
            (finally (access/close-cursor resumed))))
        (finally (access/close-cursor cursor))))))
