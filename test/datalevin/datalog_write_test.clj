(ns datalevin.datalog-write-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [datalevin.constants :as c]
            [datalevin.datom :as d]
            [datalevin.interface :as i]
            [datalevin.storage :as s]
            [datalevin.test.core :as test-core]
            [datalevin.util :as u]))

(use-fixtures :each test-core/db-fixture)

(deftest datom-batches-preserve-both-indexes-and-giants
  ;; Without WAL, load-datoms uses one-shot prepared operations; WAL exercises
  ;; the executor inside an open write transaction. Both support ordered writes.
  (doseq [wal? [false true]
          ordered? [false true]]
    (testing (str "WAL " wal? ", ordered " ordered?)
      (binding [c/*ordered-datom-writes?* ordered?]
        (let [dir (u/tmp-dir (str "datom-write-" (random-uuid)))
              schema {:text {:db/valueType :db.type/string}
                      :blob {:db/valueType :db.type/string}}
              store (s/open dir schema {:kv-opts {:wal? wal?}})
              before [(d/datom 3 :text "third")
                      (d/datom 1 :text "first")
                      (d/datom 1 :blob (apply str (repeat 1000 "old")))
                      (d/datom 2 :text "second")]
              after [(d/datom 3 :text "third")
                     (d/datom 1 :text "replacement")
                     (d/datom 1 :blob (apply str (repeat 1000 "new")))
                     (d/datom 2 :text "second")]
              check! (fn [store expected]
                       (is (= (count expected) (i/datom-count store :eav)))
                       (is (= (count expected) (i/datom-count store :ave)))
                       (is (= (set expected)
                              (set (mapcat #(i/e-datoms store %) [1 2 3]))))
                       (doseq [datom expected]
                         (is (= [datom] (i/slice store :ave datom datom)))
                         (is (= [datom] (i/av-datoms store (:a datom) (:v datom))))))]
          (try
            (i/load-datoms store before)
            (check! store before)
            (i/load-datoms store [(d/delete (nth before 1)) (nth after 1)
                                 (d/delete (nth before 2)) (nth after 2)])
            (check! store after)
            (is (empty? (i/av-datoms store :text "first")))
            (is (empty? (i/fetch store (nth before 2))))
            (i/load-datoms store [])
            (check! store after)
            (i/close store)
            (let [reopened (s/open dir)]
              (try
                (check! reopened after)
                (i/load-datoms reopened (mapv d/delete after))
                (check! reopened [])
                (finally (i/close reopened))))
            (finally
              (i/close store)
              (u/delete-files dir))))))))
