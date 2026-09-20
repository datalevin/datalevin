(ns datalevin.giant-lookup-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [datalevin.constants :as c]
            [datalevin.core :as d]
            [datalevin.db :as db]
            [datalevin.interface :as i]
            [datalevin.pipe :as p]
            [datalevin.storage :as s]
            [datalevin.test.core :as test-core]
            [datalevin.util :as u])
  (:import [java.util ArrayList]))

(use-fixtures :each test-core/db-fixture)

(defn- tuples [rows]
  (ArrayList. ^java.util.Collection (mapv object-array rows)))

(defn- check-query-scans [store value expected]
  (let [input (tuples (for [e (range 1 5)] [value e]))
        out (ArrayList.)]
    (is (= expected (set (map #(aget ^objects % 1)
                             (i/val-eq-scan-e-list store (tuples [[value]])
                                                   0 :value)))))
    (is (= expected (set (map #(aget ^objects % 1)
                             (i/val-eq-filter-e-list store input 0 :value 1)))))
    (i/val-eq-filter-e store (p/list-tuple-pipe input) out 0 :value 1)
    (is (= expected (set (map #(aget ^objects % 1) out)))))
  (doseq [e (range 1 5)]
    (let [input (tuples [[value]])
          out (ArrayList.)
          expected (if (contains? expected e) #{e} #{})]
      (is (= expected (set (map #(aget ^objects % 1)
                               (i/val-eq-scan-e-list store input 0 :value e)))))
      (i/val-eq-scan-e store (p/list-tuple-pipe input) out 0 :value e)
      (is (= expected (set (map #(aget ^objects % 1) out)))))))

(defn- check-lookups [conn value expected]
  (let [store (:store @conn)
        expected (set expected)]
    (check-query-scans store value expected)
    (is (= expected (set (map :e (i/av-datoms store :value value)))) "AV datoms")
    (is (= (count expected) (i/av-size store :value value)) "AV count")
    (is (= expected (set (map #(aget ^objects % 0) (s/av-tuples store :value value))))
        "AV tuples")
    (is (= expected (set (map :e (db/-search @conn [nil :value value]))))
        "AV search")
    (is (= expected (set (d/q '[:find [?e ...] :in $ ?v :where [?e :value ?v]]
                              @conn value))) "query with bound value")
    (is (= expected (set (d/q (vector :find '[?e ...] :where ['?e :value value])
                              @conn))) "query with constant value")
    (is (= expected (set (d/q '[:find [?e ...] :in $ [?v ...]
                                :where [?e :value ?v]] @conn [value])))
        "query with collection input")
    (is (= expected (set (d/q '[:find [?e ...] :in $ ?v
                                :where [?e :tag :row] [?e :value ?v]]
                              @conn value))) "join on entity")
    (let [first-e (i/av-first-e store :value value)]
      (is (if (seq expected) (contains? expected first-e) (nil? first-e))
          "first matching entity"))))

(deftest giant-equality-lookup-and-count
  (doseq [wal? [false true]
          compressed? [false true]]
    (testing (str "WAL " wal? ", compressed giants " compressed?)
      (binding [c/*giants-zstd-threshold* (if compressed? 0 Long/MAX_VALUE)]
        (let [dir (u/tmp-dir (str "giant-lookup-" (random-uuid)))
              schema {:value {:db/valueType :db.type/string}
                      :tag {:db/valueType :db.type/keyword}}
              conn (d/create-conn dir schema {:kv-opts {:wal? wal?}})
              prefix (.repeat "shared" 200)
              a (str prefix "a")
              b (str prefix "b")
              missing (str prefix "missing")]
          (try
            ;; The first giant deliberately shares a truncated prefix with
            ;; different values. Matching entities get separate giant IDs.
            (d/transact! conn [{:db/id 1 :value a :tag :row}
                               {:db/id 2 :value b :tag :row}
                               {:db/id 3 :value b :tag :row}
                               {:db/id 4 :value "inline" :tag :row}])
            (check-lookups conn a #{1})
            (check-lookups conn b #{2 3})
            (check-lookups conn missing #{})
            (check-lookups conn "inline" #{4})
            (d/transact! conn [[:db/add 1 :value b]])
            (check-lookups conn a #{})
            (check-lookups conn b #{1 2 3})
            (d/with-transaction [tx conn]
              (d/transact! tx [[:db/add 2 :value a]])
              (check-lookups tx a #{2})
              (check-lookups tx b #{1 3})
              (d/abort-transact tx))
            (check-lookups conn b #{1 2 3})
            (d/transact! conn [[:db/retractEntity 3]])
            (check-lookups conn b #{1 2})
            (d/close conn)
            (let [reopened (d/create-conn dir)]
              (try
                (check-lookups reopened a #{})
                (check-lookups reopened b #{1 2})
                (check-lookups reopened missing #{})
                (finally (d/close reopened))))
            (finally (d/close conn) (u/delete-files dir))))))))

(deftest giant-probes-preserve-adjacent-inline-probes
  (let [conn (d/create-conn nil
                           {:value {:db/valueType :db.type/string
                                    :db/cardinality :db.cardinality/many}}
                           {:kv-opts {:inmemory? true :wal? false}})
        prefix (.repeat "m" 1200)
        a (str prefix "a")
        b (str prefix "b")]
    (try
      (d/transact! conn [{:db/id 1 :value [a "zzzz"]}
                         {:db/id 2 :value [b "zzzz"]}])
      (let [store (:store @conn)
            input [[a 1 :a1] [a 1 :a1-repeat] [a 2 :miss-a2]
                   [b 2 :b2] [b 1 :miss-b1]
                   ["zzzz" 1 :z1] ["zzzz" 2 :z2] ["zzzz" 2 :z2-repeat]]
            kept (i/val-eq-filter-e-list store (tuples input) 0 :value 1)
            joined (i/val-eq-scan-e-list
                     store (tuples [[a :a] [b :miss-b] ["zzzz" :z]]) 0 :value 1)]
        (is (= [:a1 :a1-repeat :b2 :z1 :z2 :z2-repeat]
               (mapv #(aget ^objects % 2) kept)))
        (is (= [:a :z] (mapv #(aget ^objects % 1) joined))))
      (finally (d/close conn)))))

(deftest giant-data-and-byte-values-use-complete-equality
  (doseq [[props make-value] [[{} #(hash-map :payload %)]
                             [{:db/valueType :db.type/bytes} #(.getBytes ^String %)]]]
    (let [conn (d/create-conn nil {:value props}
                             {:kv-opts {:inmemory? true :wal? false}})
          prefix (.repeat "m" 1200)
          a (make-value (str prefix "a"))
          b (make-value (str prefix "b"))
          missing (make-value (str prefix "missing"))]
      (try
        (d/transact! conn [{:db/id 1 :value a}
                           {:db/id 2 :value b} {:db/id 3 :value b}])
        (let [store (:store @conn)]
          (doseq [[value expected] [[a #{1}] [b #{2 3}] [missing #{}]]]
            (is (= expected (set (map :e (i/av-datoms store :value value)))))
            (is (= (count expected) (i/av-size store :value value)))
            (is (= expected (set (d/q '[:find [?e ...] :in $ ?v
                                        :where [?e :value ?v]] @conn value))))
            (check-query-scans store value expected)))
        (finally (d/close conn))))))
