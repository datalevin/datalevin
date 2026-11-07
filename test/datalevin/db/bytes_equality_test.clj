(ns datalevin.db.bytes-equality-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [datalevin.constants :as c]
            [datalevin.core :as d]
            [datalevin.datom :as datom]
            [datalevin.db :as db]
            [datalevin.test.core :refer [db-fixture]]
            [datalevin.tx-group.batch.embedded :as embedded]
            [datalevin.tx-group.compat :as group]))

(use-fixtures :each db-fixture)

(def ^:private byte-attrs
  [:blob :private :blob2 :blob3 :blob4 :blob5 :blob6 :blob7])

(def ^:private schema
  (merge (zipmap byte-attrs (repeat {:db/valueType :db.type/bytes}))
         {:private {:db/valueType :db.type/bytes :db/noindex true}
          :key {:db/valueType :db.type/string :db/unique :db.unique/identity}
          :many {:db/valueType :db.type/bytes :db/cardinality :db.cardinality/many}
          :byte-key {:db/valueType :db.type/bytes :db/unique :db.unique/identity}
          :opaque {}
          :counter {:db/valueType :db.type/long}}))

(defn- payload [size] (byte-array size (byte 7)))
(defn- copy [^bytes value] (aclone value))
(defn- different [^bytes value]
  (if (zero? (alength value))
    (byte-array [8])
    (doto (copy value) (aset-byte (dec (alength value)) (byte 8)))))

(defn- with-store [wal? size f]
  (with-open [cn (d/create-conn nil schema
                  {:wal? wal? :background-sampling? false :snapshot-scheduler? false})]
    (let [value (payload size)]
      (d/transact! cn [(merge {:db/id 1 :key "one" :counter 0 :many value :opaque value}
                             (zipmap byte-attrs (repeatedly #(copy value))))])
      (f cn value))))

(defn- byte-changes [report]
  (filterv #(contains? (set byte-attrs) (:a %)) (:tx-data report)))

(defn- changes [report attr]
  (mapv (fn [row] [(vec (:v row)) (datom/datom-added row)])
        (filter #(= attr (:a %)) (:tx-data report))))

(deftest byte-values-and-datoms-use-content-equality-and-hashing
  (doseq [size [0 3 2048]]
    (let [a (payload size) b (copy a)
          da (datom/datom 1 :blob a 1)
          db (datom/datom 1 :blob b 2 false)]
      (is (not (identical? a b)))
      (is (datom/value-equal? a b))
      (is (not (datom/value-equal? a (different a))))
      (is (not (datom/value-equal? a (vec a))))
      (is (not (datom/value-equal? (vec a) a)))
      (is (= da db))
      (is (= (hash da) (hash db)))
      (is (contains? #{da} db))
      (is (= 1 (count (set [da db]))))))
  (let [a (datom/datom 1 :blob (byte-array [0 31]))
        b (datom/datom 1 :blob (byte-array [1 0]))]
    (is (= (hash a) (hash b)))
    (is (not= a b))
    (is (= 2 (count #{a b}))))
  (doseq [[a b] [[nil nil] [false false] [false nil] [1 1] [1 1.0]
                 [1M 1.00M] ["one" "one"] [[1 2] '(1 2)]]]
    (is (= (= a b) (datom/value-equal? a b)))))

(deftest identical-bytes-do-not-rewrite-any-preparation-path
  (doseq [wal? [false true] size [0 3 2048]]
    (testing (str "WAL " wal? ", bytes " size)
      (with-store wal? size
        (fn [cn value]
          (let [noop! (fn [txs]
                        (let [report (d/transact! cn txs)]
                          (is (empty? (:tx-data report)))
                          (is (= (inc (:max-tx (:db-before report)))
                                 (:max-tx (:db-after report))))
                          report))]
            (noop! [[:db/add 1 :blob (copy value)]])
            (noop! [[:db/add 1 :blob (vec value)]])
            (noop! [[:db/add 1 :opaque (copy value)]])
            (noop! [[:db/add 1 :blob (copy value)]
                    [:db/add 1 :private (copy value)]])
            (noop! (mapv (fn [attr] [:db/add 1 attr (copy value)]) byte-attrs))
            (binding [db/*specialize-scalar-stamping?* false]
              (noop! [[:db/add 1 :blob (copy value)]
                      [:db/add 1 :private (copy value)]]))
            (let [report (d/transact! cn
                           [[:db/add 1 :blob (copy value)]
                            {:db/id "new" :counter 99}])]
              (is (empty? (byte-changes report)))
              (is (= 1 (count (:tx-data report)))))
            (noop! [{:db/id "alias" :key "one" :blob (copy value)
                     :private (copy value)}])
            (noop! [(merge {:db/id "alias" :key "one"}
                           (zipmap byte-attrs (repeatedly #(copy value))))])
            (noop! [{:db/id 1 :blob (copy value) :private (copy value)}])
            (noop! [[:db.fn/call (fn [_] [])] [:db/add 1 :blob (copy value)]])
            (noop! [[:db/add 1 :many (copy value)]])
            (let [notifications (atom [])]
              (d/listen! cn ::noop #(swap! notifications conj %))
              (let [report (noop! [[:db/add 1 :blob (copy value)]])]
                (is (= [report] @notifications)))
              (d/unlisten! cn ::noop))
            (let [changed (different value)
                  report (d/transact! cn [[:db/add 1 :blob changed]])]
              (is (= [[(vec value) false] [(vec changed) true]]
                     (changes report :blob)))
              (is (= (vec changed) (vec (:blob (d/entity @cn 1)))))
              (noop! [[:db/add 1 :blob (copy changed)]]))))))))

(deftest byte-retractions-and-readditions-keep-their-order
  (doseq [wal? [false true] size [3 2048] prepare? [false true]
          direct? [false true]]
    (binding [c/*use-prepare-path* prepare?
              embedded/*defer-attachment?* direct?
              group/*enabled?* (not direct?)]
      (with-store wal? size
        (fn [cn value]
          (let [report (d/transact! cn [[:db/retract 1 :blob (copy value)]
                                       [:db/add 1 :blob (copy value)]])]
            (is (= [[(vec value) false] [(vec value) true]]
                   (changes report :blob)))
            (is (= (vec value) (vec (:blob (d/entity @cn 1))))))
          (let [report (d/transact! cn [[:db/add 1 :blob (copy value)]
                                       [:db/retract 1 :blob (copy value)]])]
            (is (= [[(vec value) false]] (changes report :blob)))
            (is (nil? (:blob (d/entity @cn 1))))))))))

(deftest pending-byte-values-and-cas-use-content-equality
  (doseq [wal? [false true] size [3 2048]]
    (with-store wal? size
      (fn [cn value]
        (let [changed (different value)]
          (is (empty? (:tx-data (d/transact! cn [[:db/cas 1 :blob (copy value)
                                                 (copy value)]]))))
          (is (empty? (:tx-data (d/transact! cn [[:db/cas 1 :many (copy value)
                                                 (copy value)]]))))
          (is (thrown? Exception
                (d/transact! cn [[:db/cas 1 :blob changed (copy value)]])))
          (d/with-transaction [tx cn]
            (d/transact! tx [[:db/add 1 :blob (copy changed)]])
            (is (empty? (:tx-data (d/transact! tx [[:db/add 1 :blob (copy changed)]]))))
            (is (empty? (:tx-data (d/transact! tx [{:key "one" :blob (copy changed)}]))))
            (let [report (d/transact! tx [[:db/add 1 :blob (copy changed)]
                                         {:counter 1}])]
              (is (empty? (byte-changes report))))
            (is (empty? (:tx-data
                          (d/transact! tx [[:db.fn/call
                                           (fn [_] [[:db/add 1 :blob (copy changed)]])]]))))
            (is (= (vec changed) (vec (:blob (d/entity @tx 1)))))
            (is (= (vec value) (vec (:blob (d/entity @cn 1)))))
            (d/abort-transact tx))
          (is (= (vec value) (vec (:blob (d/entity @cn 1)))))
          (let [report (d/transact! cn [[:db/cas 1 :blob (copy value) changed]])]
            (is (= [[(vec value) false] [(vec changed) true]]
                   (changes report :blob)))))))))

(deftest byte-identity-upserts-recognize-copied-small-and-giant-values
  (doseq [wal? [false true] size [3 2048]]
    (with-store wal? size
      (fn [cn value]
        (d/transact! cn [{:db/id 2 :byte-key (copy value) :private (copy value)}])
        (let [report (d/transact! cn [{:db/id "alias" :byte-key (copy value)
                                      :private (copy value)}])]
          (is (= 2 (get (:tempids report) "alias")))
          (is (empty? (:tx-data report))))))))
