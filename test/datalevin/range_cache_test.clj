(ns datalevin.range-cache-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [datalevin.constants :as c]
            [datalevin.core :as d]
            [datalevin.datom :as datom]
            [datalevin.db :as db]
            [datalevin.db.range-cache :as ranges]
            [datalevin.interface :as i]
            [datalevin.test.core :refer [db-fixture]])
  (:import [java.util Date UUID]))

(use-fixtures :each db-fixture)

(defn- normalize [rows]
  (mapv (fn [datom]
          [(:e datom) (:a datom)
           (let [v (:v datom)] (if (bytes? v) (vec v) v))])
        rows))

(defn- range-reader [index low high]
  {:key [:range-datoms index low high]
   ;; Transaction reports can retain datom overlays. A fresh committed view
   ;; uses the range cache, as does the DB rebuilt on explicit transaction exit.
   :read #(db/-range-datoms (db/transfer % (:store %)) index low high)
   :fresh #(i/slice (:store %) index low high)
   :normalize normalize})

(defn- index-readers [attr start end]
  [{:key [:index-range attr start end]
    :read #(d/index-range % attr start end)
    :fresh #(i/slice (:store %) :ave
                     (datom/datom c/e0 attr start)
                     (datom/datom c/emax attr end))
    :normalize normalize}
   {:key [:index-range-size attr start end]
    :read #(db/-index-range-size % attr start end)
    :fresh #(i/av-range-size (:store %) attr start end)
    :contents #(normalize (i/slice (:store %) :ave
                                   (datom/datom c/e0 attr start)
                                   (datom/datom c/emax attr end)))
    :normalize identity}])

(defn- check-write! [conn readers txs]
  (let [contents (fn [reader database]
                   (if-let [f (:contents reader)]
                     (f database)
                     ((:normalize reader) ((:fresh reader) database))))
        before (mapv #(vector ((:read %) @conn) (contents % @conn)) readers)]
    (doseq [reader readers]
      (is (some? (db/cache-get (:store @conn) (:key reader)))))
    (d/transact! conn txs)
    (doseq [[reader [old old-contents]] (map vector readers before)]
      (let [{:keys [key read fresh normalize]} reader
            actual (fresh @conn)
            saved (db/cache-get (:store @conn) key)]
        (if (= old-contents (contents reader @conn))
          (is (identical? old saved) (str "preserve " key " after " txs))
          (is (nil? saved) (str "invalidate " key " after " txs)))
        (is (= (normalize actual) (normalize (read @conn)))
            (str "match native scan " key " after " txs))))))

(deftest numeric-range-boundaries-and-counts
  (doseq [wal? [false true]]
    (let [conn (d/create-conn nil {:n {:db/valueType :db.type/long}}
                             {:kv-opts {:inmemory? true :wal? wal?}})
          readers (vec (mapcat (fn [[lo hi]] (index-readers :n lo hi))
                              [[10 20] [nil 10] [20 nil] [nil nil]
                               [10 10] [21 29] [20 10]]))]
      (try
        (d/transact! conn [{:db/id 1 :n 10} {:db/id 2 :n 20}])
        (doseq [txs [[[:db/add 3 :n 5]] [[:db/add 4 :n 30]]
                     [[:db/add 5 :n 10]] [[:db/add 6 :n 20]]
                     [[:db/add 7 :n 15]] [[:db/add 7 :n 25]]
                     [[:db/retract 7 :n 25]] [[:db/retract 1 :n 10]]
                     [[:db/retract 2 :n 20]]]]
          (check-write! conn readers txs))
        (finally (d/close conn))))))

(deftest native-ranges-use-independent-entity-and-avg-bounds
  (let [conn (d/create-conn nil {:z {:db/valueType :db.type/long}}
                           {:kv-opts {:inmemory? true}})]
    (try
      ;; Deliberately make attribute-ID order differ from keyword order.
      (d/update-schema conn {:a {:db/valueType :db.type/long}})
      (d/update-schema conn {:outside {:db/valueType :db.type/long}})
      (let [schema (i/schema (:store @conn))
            readers (vec
                      (for [index [:eav :ave]
                            [low high]
                            [[(datom/datom 10 :z 10) (datom/datom 20 :z 20)]
                             [(datom/datom 10 :z c/v0) (datom/datom 20 :a c/vmax)]
                             [(datom/datom c/e0 nil nil) (datom/datom c/emax nil nil)]]]
                        (range-reader index low high)))]
        (is (< (get-in schema [:z :db/aid]) (get-in schema [:a :db/aid])))
        (d/transact! conn [{:db/id 15 :z 15 :a 15}])
        (doseq [txs [[[:db/add 9 :z 15]] [[:db/add 21 :a 15]]
                     [[:db/add 15 :outside 15]] [[:db/add 15 :z 5]]
                     [[:db/add 10 :z 10]] [[:db/add 20 :z 20]]
                     [[:db/retract 10 :z 10]] [[:db/retract 20 :z 20]]]]
          (check-write! conn readers txs)))
      (finally (d/close conn)))))

(deftest fixed-entity-ranges-preserve-overlap-invalidation
  (doseq [wal? [false true]]
    (let [conn (d/create-conn nil {:a {:db/valueType :db.type/long}}
                             {:kv-opts {:inmemory? true :wal? wal?}})]
      (try
        (d/update-schema conn {:b {:db/valueType :db.type/long}})
        (d/update-schema conn {:outside {:db/valueType :db.type/long}})
        (d/transact! conn [{:db/id 1 :a 10 :b 20} {:db/id 2 :a 30 :b 40}])
        (let [readers (vec (for [index [:eav :ave], e [1 2]]
                             (range-reader index (datom/datom e :a nil)
                                           (datom/datom e :b nil))))]
          (doseq [txs [[[:db/add 3 :a 50]]
                       [[:db/add 1 :outside 60]]
                       [[:db/add 1 :a 11]]
                       [[:db/add 2 :b 41]]
                       [[:db/retract 1 :a 11]]]]
            (check-write! conn readers txs)))
        (finally (d/close conn))))))

(deftest range-comparisons-follow-native-value-encoding
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
      (let [conn (d/create-conn nil {:v props} {:kv-opts {:inmemory? true}})
            readers (vec (mapcat (fn [[lo hi]] (index-readers :v lo hi))
                                [[(first values) (last values)]
                                 [(second values) (nth values 2)]
                                 [(second values) (second values)]
                                 [nil (second values)] [(nth values 2) nil]]))]
        (try
          (doseq [[e v] (map vector (range 1 5) values)]
            (check-write! conn readers [[:db/add e :v v]]))
          (doseq [[e v] (map vector (range 1 5) values)]
            (check-write! conn readers [[:db/retract e :v v]]))
          (finally (d/close conn)))))))

(deftest giant-prefix-overlap-survives-retraction
  (let [conn (d/create-conn nil {:v {:db/valueType :db.type/string}}
                           {:kv-opts {:inmemory? true}})
        prefix (.repeat "m" 1000)
        a (str prefix "a")
        b (str prefix "b")
        readers (index-readers :v a a)]
    (try
      (d/transact! conn [[:db/add 1 :v a]])
      ;; Both values occupy the same truncated prefix range, even though their
      ;; complete strings differ. Their allocated giant IDs need not be read.
      (check-write! conn readers [[:db/add 2 :v b]])
      (check-write! conn readers [[:db/add 3 :v (.repeat "z" 1000)]])
      (check-write! conn readers [[:db/retract 1 :v a]])
      (check-write! conn readers [[:db/retract 2 :v b]])
      (finally (d/close conn)))))

(deftest range-cache-commit-abort-and-schema-change
  (let [conn (d/create-conn nil {:n {:db/valueType :db.type/long}}
                           {:kv-opts {:inmemory? true}})]
    (try
      (d/transact! conn [[:db/add 1 :n 15]])
      (let [read #(d/index-range % :n 10 20)
            cached (read @conn)]
        (d/with-transaction [tx conn]
          (d/transact! tx [[:db/add 2 :n 30]]))
        (is (identical? cached (read @conn)))
        (d/with-transaction [tx conn]
          (d/transact! tx [[:db/add 3 :n 17]])
          (is (= [15 17] (mapv :v (read @tx))))
          (d/abort-transact tx))
        (is (= [15] (mapv :v (read @conn))))
        (d/update-schema conn {:n {:db/cardinality :db.cardinality/many}})
        (d/transact! conn [[:db/add 1 :n 18]])
        (is (= [15 18] (mapv :v (read @conn)))))
      (finally (d/close conn)))))

(deftest unresolved-ref-bounds-remain-conservative
  (let [conn (d/create-conn nil {:name {:db/unique :db.unique/identity}
                               :ref {:db/valueType :db.type/ref}}
                           {:kv-opts {:inmemory? true}})]
    (try
      (d/transact! conn [{:db/id 1 :name "target"} {:db/id 2 :name "other"}
                        {:db/id 3 :ref 1} {:db/id 4 :ref 2}])
      (let [bound [:name "target"]
            read #(d/index-range % :ref bound bound)]
        (is (= [3] (mapv :e (read @conn))))
        (d/transact! conn [[:db/retract 1 :name "target"]])
        (is (nil? (db/cache-get (:store @conn) [:index-range :ref bound bound])))
        (d/transact! conn [[:db/add 2 :name "target"]])
        (is (= [4] (mapv :e (read @conn)))))
      (finally (d/close conn)))))

(deftest custom-order-checks-never-call-resolvers
  (let [schema {:v {:db/aid 1 :db/valueType :app/custom}}
        context (ranges/context schema [(datom/datom 1 :v {:rank 100})])]
    (is (ranges/affected? context [:index-range :v {:rank 1} {:rank 2}]))
    (is (not (ranges/affected? context
               [:range-datoms :eav (datom/datom 10 :v nil)
                (datom/datom 20 :v nil)])))))
