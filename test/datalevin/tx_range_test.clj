(ns datalevin.tx-range-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [datalevin.constants :as c]
            [datalevin.core :as d]
            [datalevin.datom :as datom]
            [datalevin.db :as db]
            [datalevin.interface :as i]
            [datalevin.interpret :as inter]
            [datalevin.test.core :refer [db-fixture]])
  (:import [java.util Date UUID]))

(use-fixtures :each db-fixture)

(defn- rows [datoms]
  (mapv (fn [{:keys [e a v]}] [e a (if (bytes? v) (vec v) v)]) datoms))

(defn- check-ranges! [conn txs bounds]
  (let [pending (:db-after (d/tx-data->simulated-report @conn txs))
        cases (for [index [:eav :ave], [low high] bounds] [index low high])
        simulated (mapv (fn [[index low high]]
                          (rows (db/-range-datoms pending index low high))) cases)]
    (is (db/pending-tx-cache? pending))
    (d/transact! conn txs)
    (doseq [[[index low high] actual] (map vector cases simulated)]
      (testing (pr-str [index low high])
        ;; Pending giant/custom values do not yet have persisted payload IDs.
        ;; Compare membership and multiplicity without imposing their ID order.
        (is (= (frequencies (rows (i/slice (:store @conn) index low high)))
               (frequencies actual)))))))

(deftest simulated-ranges-include-boundary-retractions-and-additions
  (doseq [cache-limit [0 512], wal? [false true]]
    (testing (str "cache=" cache-limit ", WAL=" wal?)
      (let [conn (d/create-conn nil {:n {:db/valueType :db.type/long}}
                               {:cache-limit cache-limit
                                :kv-opts {:inmemory? true :wal? wal?}})]
        (try
          (d/transact! conn [{:db/id 1 :n 10} {:db/id 2 :n 20}])
          (let [txs [[:db/add 1 :n 12] [:db/retract 2 :n 20] [:db/add 3 :n 30]]
                pending (:db-after (d/tx-data->simulated-report @conn txs))]
            (doseq [index [:eav :ave]
                    [low high] [[(datom/datom 1 :n nil) (datom/datom 2 :n nil)]
                                [(datom/datom 1 :n c/v0) (datom/datom 2 :n c/vmax)]]]
              (is (= [[1 :n 12]] (rows (db/-range-datoms pending index low high)))))
            (is (= {:n 20} (d/pull @conn [:n] 2)))
            (is (nil? (d/pull pending [:n] 2)))
            (check-ranges! conn txs
                           [[(datom/datom 1 :n nil) (datom/datom 2 :n nil)]
                            [(datom/datom 1 :n c/v0) (datom/datom 2 :n c/vmax)]
                            [(datom/datom c/e0 nil nil) (datom/datom c/emax nil nil)]
                            [(datom/datom 1 :n 10) (datom/datom 2 :n 20)]
                            [(datom/datom 3 :n 30) (datom/datom 3 :n 30)]
                            [(datom/datom 3 :n nil) (datom/datom 1 :n nil)]
                            [(datom/datom 1 :n 30) (datom/datom 3 :n 10)]]))
          (finally (d/close conn)))))))

(deftest simulated-ranges-use-independent-entity-and-avg-bounds
  (let [conn (d/create-conn nil {:z {:db/valueType :db.type/long}}
                           {:kv-opts {:inmemory? true}})]
    (try
      ;; Native attribute order is ID order, deliberately unlike keyword order.
      (d/update-schema conn {:a {:db/valueType :db.type/long}})
      (d/update-schema conn {:outside {:db/valueType :db.type/long}})
      (d/transact! conn [{:db/id 1 :z 10 :a 10 :outside 10}
                        {:db/id 2 :z 20 :a 20 :outside 20}
                        {:db/id 3 :z 30 :a 30 :outside 30}])
      (check-ranges! conn [[:db/add 1 :z 5] [:db/add 2 :z 15] [:db/add 3 :z 35]
                          [:db/add 1 :a 11] [:db/add 2 :a 21] [:db/add 3 :a 31]
                          [:db/add 1 :outside 11] [:db/add 4 :z 15]]
                     [[(datom/datom 1 :z 10) (datom/datom 3 :z 20)]
                      [(datom/datom 2 :z c/v0) (datom/datom 2 :a c/vmax)]
                      [(datom/datom 1 :z nil) (datom/datom 3 :a nil)]
                      [(datom/datom c/e0 :z nil) (datom/datom c/emax :a nil)]
                      [(datom/datom 1 :a nil) (datom/datom 3 :z nil)]])
      (finally (d/close conn)))))

(deftest simulated-range-membership-follows-native-value-encoding
  (doseq [[props values]
          [[{:db/valueType :db.type/string} ["a" "z" "\uE000" "\uD800\uDC00"]]
           [{:db/valueType :db.type/keyword} [:a/a :a/z :b/a :z]]
           [{:db/valueType :db.type/bytes}
            [(byte-array [0]) (byte-array [127]) (byte-array [-128]) (byte-array [-1])]]
           [{:db/valueType :db.type/uuid}
            [(UUID. 0 0) (UUID. 1 0) (UUID. Long/MIN_VALUE 0) (UUID. -1 -1)]]
           [{:db/valueType :db.type/instant} [(Date. -1) (Date. 0) (Date. 10) (Date. 20)]]
           [{:db/valueType :db.type/double} [-2.0 -0.0 0.0 3.0]]
           [{:db/valueType :db.type/bigdec} [-2M 0M 1.000000000000000001M 2M]]
           [{:db/valueType :db.type/tuple :db/tupleTypes [:db.type/long :db.type/string]}
            [[1 "a"] [1 "z"] [2 "a"] [3 "z"]]]
           [{} [{:a 1} {:a 2} {:b 1} {:z [1 2 3]}]]]]
    (testing (str props)
      (let [conn (d/create-conn nil {:v props} {:kv-opts {:inmemory? true}})]
        (try
          (d/transact! conn [{:db/id 1 :v (first values)} {:db/id 2 :v (second values)}])
          (check-ranges!
            conn [[:db/retract 1 :v (first values)] [:db/add 2 :v (nth values 2)]
                  [:db/add 3 :v (last values)] [:db/add 4 :v (first values)]]
            (for [[lo hi] [[nil nil] [c/v0 c/vmax]
                          [(first values) (last values)]
                          [(second values) (nth values 2)]
                          [(last values) (last values)]
                          [nil (second values)] [(nth values 2) nil]]]
              [(datom/datom 1 :v lo) (datom/datom 4 :v hi)]))
          (finally (d/close conn)))))))

(deftest simulated-ranges-include-entire-giant-prefix-buckets
  (let [conn (d/create-conn nil {:v {:db/valueType :db.type/string
                                   :db/cardinality :db.cardinality/many}}
                           {:kv-opts {:inmemory? true}})
        prefix (.repeat "m" 1000)
        a (str prefix "a")
        b (str prefix "b")
        z (.repeat "z" 1000)]
    (try
      (d/transact! conn [[:db/add 1 :v a] [:db/add 1 :v b] [:db/add 2 :v z]])
      (check-ranges! conn [[:db/retract 1 :v a] [:db/add 2 :v b] [:db/add 3 :v a]]
                     [[(datom/datom 1 :v a) (datom/datom 3 :v a)]
                      [(datom/datom 2 :v b) (datom/datom 2 :v b)]
                      [(datom/datom 1 :v nil) (datom/datom 3 :v nil)]])
      (finally (d/close conn)))))

(deftest simulated-custom-ranges-preserve-native-buckets
  (let [conn (d/create-conn nil nil {:kv-opts {:inmemory? true}})
        a {:rank 1 :name "a"}
        b {:rank 1 :name "b"}
        c {:rank 2 :name "c"}]
    (try
      (d/register-type conn :app/ranked
                       {:index {:type :long :order-fn (inter/inter-fn [v] (:rank v))}})
      (d/update-schema conn {:v {:db/valueType :app/ranked
                                :db/cardinality :db.cardinality/many}})
      (d/update-schema conn {:outside {:db/valueType :db.type/long}})
      (d/transact! conn [[:db/add 1 :v a] [:db/add 2 :v c]])
      (check-ranges! conn [[:db/retract 1 :v a] [:db/add 1 :v b] [:db/add 2 :v a]
                          [:db/add 3 :v c] [:db/add 2 :outside 100]]
                     [[(datom/datom 1 :v a) (datom/datom 3 :v a)]
                      [(datom/datom 2 :v c/v0) (datom/datom 2 :v c/vmax)]
                      [(datom/datom 1 :v nil) (datom/datom 3 :v nil)]
                      [(datom/datom 2 :v b) (datom/datom 3 :v c)]
                      [(datom/datom 3 :v nil) (datom/datom 1 :v nil)]])
      (finally (d/close conn)))))
