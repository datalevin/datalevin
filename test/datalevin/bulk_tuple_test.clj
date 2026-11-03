(ns datalevin.bulk-tuple-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [datalevin.constants :as c]
            [datalevin.core :as d]
            [datalevin.datom :as datom]
            [datalevin.interface :as i]
            [datalevin.test.core :refer [server-fixture]]
            [datalevin.util :as u]))

(use-fixtures :once server-fixture)

(def ^:private tuple-schema
  {:item/number {:db/valueType :db.type/long}
   :item/region {:db/valueType :db.type/string}
   :item/owner  {:db/valueType :db.type/ref}
   :item/key    {:db/tupleAttrs [:item/number :item/region]
                 :db/unique :db.unique/identity}
   :item/owned  {:db/tupleAttrs [:item/number :item/owner]}})

(defn- all-datoms [db]
  (set (map (juxt :e :a :v) (d/datoms db :eav))))

(defn- with-location [remote? f]
  (let [location (if remote?
                   (str "dtlv://datalevin:datalevin@localhost/bulk-tuples-"
                        (random-uuid))
                   (u/tmp-dir (str "bulk-tuples-" (random-uuid))))]
    (try
      (f location)
      (finally
        (when (and (not remote?) (u/file-exists location))
          (u/delete-files location))))))

(deftest composites-follow-bulk-source-changes
  (doseq [remote? [false true]
          wal? [false true]]
    (testing (str "remote=" remote? ", WAL=" wal?)
      (with-location remote?
        (fn [location]
          (binding [c/*fill-db-batch-size* 1]
            (let [db (d/init-db [(d/datom 1 :item/number 42)
                                (d/datom 2 :item/number 42)
                                (d/datom 1 :item/region "A")
                                (d/datom 2 :item/region "B")
                                (d/datom 1 :item/owner 2)]
                               location tuple-schema {:wal? wal?})
                  conn (d/conn-from-db db)]
              (try
                (is (= #{[1 [42 "A"]] [2 [42 "B"]]}
                       (d/q '[:find ?e ?v :where [?e :item/key ?v]] @conn)))
                (is (= [42 2] (:item/owned (d/entity @conn 1))))
                (is (= 1 (:db/id (d/entity @conn [:item/key [42 "A"]]))))
                (reset! conn
                        (d/fill-db @conn
                                   [(datom/delete (d/datom 1 :item/region "A"))
                                    (d/datom 1 :item/region "C")
                                    ;; A nonmatching retract leaves the source.
                                    (datom/delete (d/datom 1 :item/region "X"))]))
                (is (= #{[1 [42 "C"]] [2 [42 "B"]]}
                       (d/q '[:find ?e ?v :where [?e :item/key ?v]] @conn)))
                (is (nil? (d/entity @conn [:item/key [42 "A"]])))
                ;; All operations in one batch must use the final source value.
                (binding [c/*fill-db-batch-size* 100]
                  (reset! conn
                          (d/fill-db @conn
                                     [(datom/delete (d/datom 1 :item/number 42))
                                      (d/datom 1 :item/number 43)
                                      (datom/delete (d/datom 1 :item/number 43))
                                      (d/datom 1 :item/number 44)])))
                (is (= [44 "C"] (:item/key (d/entity @conn 1))))
                (is (= [44 2] (:item/owned (d/entity @conn 1))))
                (reset! conn
                        (d/fill-db @conn
                                   [(datom/delete (d/datom 1 :item/number 44))
                                    (datom/delete (d/datom 1 :item/region "C"))
                                    (datom/delete (d/datom 1 :item/owner 2))]))
                (is (empty? (d/datoms @conn :eav 1)))
                ;; Ordinary transactions continue from the loaded state.
                (d/transact! conn [[:db/add 2 :item/region "D"]])
                (is (= [42 "D"] (:item/key (d/entity @conn 2))))
                (finally (d/close conn))))
            (let [conn (d/create-conn location)]
              (try
                (is (= [42 "D"] (:item/key (d/entity @conn 2))))
                (is (empty? (d/datoms @conn :eav 1)))
                (finally (d/close conn))))))))))

(deftest composite-datoms-in-imports-are-derived-from-sources
  (doseq [remote? [false true]
          batch-size [1 100]]
    (with-location remote?
      (fn [location]
        (binding [c/*fill-db-batch-size* batch-size]
          (let [db (d/init-db [(d/datom 1 :item/key [999 "stale"])
                              (d/datom 1 :item/owned [999 99])
                              (d/datom 1 :item/number 42)
                              (d/datom 1 :item/region "A")
                              (d/datom 1 :item/key [42 "A"])]
                             location tuple-schema)
                expected #{[1 :item/number 42] [1 :item/region "A"]
                           [1 :item/key [42 "A"]] [1 :item/owned [42 nil]]}]
            (try
              (is (= expected (all-datoms db)))
              (let [filled (d/fill-db db [(datom/delete
                                           (d/datom 1 :item/key [42 "A"]))])]
                (is (= expected (all-datoms filled))))
              (finally (d/close-db db)))))))))

(deftest failed-bulk-loads-do-not-leave-composites-behind
  (doseq [remote? [false true]
          wal? [false true]]
    (with-location remote?
      (fn [location]
        (let [conn (d/create-conn location tuple-schema {:wal? wal?})]
          (try
            (is (thrown? Exception
                         (binding [c/*fill-db-batch-size* 1]
                           (d/init-db [(d/datom 1 :item/number 42) nil]
                                      location))))
            (is (empty? (d/datoms @conn :eav)))
            (is (thrown? Exception
                         (binding [c/*fill-db-batch-size* 1]
                           (d/fill-db @conn [(d/datom 1 :item/number 42) nil]))))
            (is (= (if remote?
                     #{[1 :item/number 42] [1 :item/key [42 nil]]
                       [1 :item/owned [42 nil]]}
                     #{})
                   (all-datoms @conn)))
            (finally (d/close conn))))))))

(deftest bulk-composite-giants-survive-dump-roundtrips
  (doseq [remote? [false true]
          wal? [false true]]
    (with-location remote?
      (fn [location]
        (let [old-region (apply str (repeat 2000 "a"))
              new-region (apply str (repeat 2000 "b"))
              db (d/init-db [(d/datom 1 :item/number 42)
                             (d/datom 1 :item/region old-region)]
                            ;; Untyped sources retain general tuple encoding;
                            ;; typed tuple components are limited to 255 bytes.
                            location (assoc tuple-schema :item/region {})
                            {:wal? wal?})]
          (try
            (with-location remote?
              (fn [target]
                (binding [c/*fill-db-batch-size* 1]
                  ;; Composite datoms precede their sources in this export.
                  (let [restored (d/init-db (reverse (d/datoms db :eav))
                                            target (i/schema (:store db))
                                            {:wal? wal?})]
                    (try
                      (is (= (all-datoms db) (all-datoms restored)))
                      (let [filled (d/fill-db
                                     restored
                                     [(datom/delete
                                        (d/datom 1 :item/region old-region))
                                      (d/datom 1 :item/region new-region)])]
                        (is (= [42 new-region]
                               (:item/key (d/entity filled 1))))
                        (is (nil? (d/entity filled
                                           [:item/key [42 old-region]])))
                        (is (= 1 (:db/id (d/entity
                                          filled [:item/key [42 new-region]])))))
                      (finally (d/close-db restored)))))))
            (finally (d/close-db db))))))))

(deftest bulk-composites-support-untyped-and-false-sources
  (let [db (binding [c/*fill-db-batch-size* 1]
             (d/init-db [(d/datom 1 :a false) (d/datom 1 :b "value")]
                        nil {:a+b {:db/tupleAttrs [:a :b]}}))]
    (try
      (is (= [false "value"] (:a+b (d/entity db 1))))
      (let [filled (d/fill-db db [(datom/delete (d/datom 1 :a false))])]
        (is (= [nil "value"] (:a+b (d/entity filled 1)))))
      (finally (d/close-db db)))))
