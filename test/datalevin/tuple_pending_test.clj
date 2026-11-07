(ns datalevin.tuple-pending-test
  (:require [clojure.test :refer [deftest is testing]]
            [datalevin.constants :as c]
            [datalevin.core :as d]
            [datalevin.util :as u]))

(def ^:private schema
  {:parent {:db/valueType :db.type/ref}
   :part {:db/valueType :db.type/long}
   :key {:db/tupleAttrs [:parent :part] :db/unique :db.unique/value}
   :label {:db/valueType :db.type/string}})

(deftest new-entity-tuples-follow-the-pending-index
  (doseq [[wal? prepare?] [[false false] [true false] [true true]]]
    (testing (str "WAL " wal? ", preparation " prepare?)
      (let [dir (u/tmp-dir (str "tuple-pending-" (random-uuid)))
            conn (d/create-conn dir schema
                   {:wal? wal? :wal-durability-profile :strict
                    :snapshot-scheduler? false})]
        (try
          (binding [c/*use-prepare-path* prepare?]
            (d/transact! conn [{:db/id 1 :label "parent"}])
            (let [report
                  (d/transact! conn
                    [{:db/id 10 :parent 1 :part 7}
                     {:db/id 11 :parent 1 :part 11}
                     {:db/id 10 :part 8}
                     [:db/retract 10 :parent 1]
                     [:db/retract 10 :part 8]
                     ;; Exercise the native preparation flush before returning
                     ;; further changes to the same newly allocated entity.
                     [:db.fn/call (fn [_] [{:db/id 10 :part 9}])]
                     {:db/id 10 :parent 1}
                     {:db/id "line" :parent 1}
                     {:db/id "line" :part 12}
                     {:db/id "line" :part 13}])
                  line (get (:tempids report) "line")]
              (is (= #{[10 [1 9]] [11 [1 11]] [line [1 13]]}
                     (set (map (juxt :e :v) (d/datoms @conn :ave :key)))))
              (is (= [nil 9]
                     (->> (:tx-data report)
                          (filter #(and (= 10 (:e %)) (= :key (:a %))
                                        (:added %) (= [nil 9] (:v %))))
                          first :v))))
            ;; A later request in an explicit transaction has a new allocation
            ;; boundary and must still see the first request's tuple values.
            (d/with-transaction [tx conn]
              (d/transact! tx [{:db/id "outer" :parent 1 :part 20}])
              (let [eid (d/entid @tx [:key [1 20]])]
                (d/transact! tx [{:db/id eid :part 21}])
                (is (= eid (d/entid @tx [:key [1 21]])))))
            (is (nil? (d/entid @conn [:key [1 20]]))))
          (finally (d/close conn) (u/delete-files dir)))))))
