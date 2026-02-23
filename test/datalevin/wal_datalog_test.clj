(ns datalevin.wal-datalog-test
  (:require
   [clojure.set :as set]
   [clojure.string :as str]
   [clojure.test :refer [deftest testing is use-fixtures]]
   [clojure.test.check.generators :as gen]
   [clojure.test.check.clojure-test :as test]
   [clojure.test.check.properties :as prop]
   [datalevin.core :as d]
   [datalevin.lmdb :as l]
   [datalevin.interface :as i]
   [datalevin.util :as u]
   [datalevin.constants :as c]
   [datalevin.test.core :as tdc :refer [db-fixture]])
  (:import
   [java.util UUID]
   [datalevin.db DB]
   [datalevin.storage Store]
   [java.util.concurrent Executors Callable]))

(defn- wal-db-fixture
  [f]
  (binding [c/*enable-kv-wal* true]
    (db-fixture f)))

(use-fixtures :each wal-db-fixture)

(defn- dl-wal-conn
  "Create a Datalog connection with default WAL settings."
  ([dir] (dl-wal-conn dir nil))
  ([dir schema]
   (d/create-conn dir schema
                  {:kv-opts {:flags (conj c/default-env-flags :nosync)}})))

(defn- conn-lmdb
  "Get the LMDB instance from a Datalog connection."
  [conn]
  (.-lmdb ^Store (.-store ^DB @conn)))

(deftest dl-wal-enabled-by-default-test
  (let [dir  (u/tmp-dir (str "dl-wal-default-" (UUID/randomUUID)))
        conn (d/create-conn dir
                            {:name {:db/unique :db.unique/identity}}
                            {:kv-opts {:flags (conj c/default-env-flags
                                                    :nosync)}})]
    (try
      (is (true? (:kv-wal? (i/env-opts (conn-lmdb conn)))))
      (d/transact! conn [{:name "Alice" :age 30}])
      (is (= 30
             (d/q '[:find ?a . :where [?e :name "Alice"] [?e :age ?a]]
                  @conn)))
      (finally
        (d/close conn)
        (u/delete-files dir)))))

;; ---------------------------------------------------------------------------
;; Test 1: Basic CRUD with WAL
;; ---------------------------------------------------------------------------
(deftest dl-wal-basic-crud-test
  (let [dir  (u/tmp-dir (str "dl-wal-crud-" (UUID/randomUUID)))
        conn (dl-wal-conn dir {:name {:db/unique :db.unique/identity}})]
    (try
      (testing "add entities"
        (d/transact! conn [{:name "Alice" :age 30}
                           {:name "Bob" :age 25}])
        (is (= #{["Alice" 30] ["Bob" 25]}
               (set (d/q '[:find ?n ?a :where [?e :name ?n] [?e :age ?a]]
                         @conn)))))

      (testing "update entity"
        (d/transact! conn [{:name "Alice" :age 31}])
        (is (= 31
               (d/q '[:find ?a . :where [?e :name "Alice"] [?e :age ?a]]
                    @conn))))

      (testing "retract attribute"
        (let [alice-id (d/q '[:find ?e . :where [?e :name "Alice"]] @conn)]
          (d/transact! conn [[:db/retract alice-id :age 31]])
          (is (nil? (d/q '[:find ?a . :where [?e :name "Alice"] [?e :age ?a]]
                         @conn)))))

      (testing "retract entity"
        (let [bob-id (d/q '[:find ?e . :where [?e :name "Bob"]] @conn)]
          (d/transact! conn [[:db.fn/retractEntity bob-id]])
          (is (empty? (d/q '[:find ?n :where [?e :name ?n] [?e :age ?a]]
                           @conn)))))

      (finally
        (d/close conn)
        (u/delete-files dir)))))

;; ---------------------------------------------------------------------------
;; Test 2: Read-your-writes (overlay serves data before indexer)
;; ---------------------------------------------------------------------------
(deftest dl-wal-read-your-writes-test
  (let [dir  (u/tmp-dir (str "dl-wal-ryw-" (UUID/randomUUID)))
        conn (dl-wal-conn dir {:name {:db/unique :db.unique/identity}})]
    (try
      (d/transact! conn [{:name "Alice" :age 30}])

      (testing "data visible immediately via overlay (before indexer)"
        (is (= "Alice"
               (d/q '[:find ?n . :where [?e :name ?n]] @conn)))
        (is (= 30
               (d/q '[:find ?a . :where [?e :name "Alice"] [?e :age ?a]]
                    @conn))))

      (testing "datoms accessible"
        (let [datoms (d/datoms @conn :eav)]
          (is (pos? (count datoms)))))

      (testing "entity API"
        (let [eid (d/q '[:find ?e . :where [?e :name "Alice"]] @conn)]
          (is (some? eid))
          (let [ent (d/entity @conn eid)]
            (is (= "Alice" (:name ent)))
            (is (= 30 (:age ent))))))

      (finally
        (d/close conn)
        (u/delete-files dir)))))

;; ---------------------------------------------------------------------------
;; Test 3: Retractions suppress base entries
;; ---------------------------------------------------------------------------
(deftest dl-wal-retractions-test
  (let [dir  (u/tmp-dir (str "dl-wal-retract-" (UUID/randomUUID)))
        conn (dl-wal-conn dir {:name {:db/unique :db.unique/identity}})]
    (try
      ;; Transact, then flush indexer to write to LMDB base
      (d/transact! conn [{:name "Alice" :age 30}
                         {:name "Bob" :age 25}])
      (d/flush-kv-indexer! (conn-lmdb conn))

      (testing "data in base is queryable"
        (is (= #{["Alice" 30] ["Bob" 25]}
               (set (d/q '[:find ?n ?a :where [?e :name ?n] [?e :age ?a]]
                         @conn)))))

      (testing "retract from base, verify suppression via overlay"
        (let [alice-id (d/q '[:find ?e . :where [?e :name "Alice"]] @conn)]
          (d/transact! conn [[:db/retract alice-id :age 30]])
          (is (nil? (d/q '[:find ?a . :where [?e :name "Alice"] [?e :age ?a]]
                         @conn)))
          ;; Bob is still visible
          (is (= 25
                 (d/q '[:find ?a . :where [?e :name "Bob"] [?e :age ?a]]
                      @conn)))))

      (finally
        (d/close conn)
        (u/delete-files dir)))))

;; ---------------------------------------------------------------------------
;; Test 4: Indexer replay — flush indexer, verify data in LMDB, overlay pruned
;; ---------------------------------------------------------------------------
(deftest dl-wal-indexer-replay-test
  (let [dir  (u/tmp-dir (str "dl-wal-replay-" (UUID/randomUUID)))
        conn (dl-wal-conn dir {:name {:db/unique :db.unique/identity}})]
    (try
      (d/transact! conn [{:name "Alice" :age 30}
                         {:name "Bob" :age 25}])

      (testing "data visible via overlay before flush"
        (is (= 2 (count (d/q '[:find ?n :where [?e :name ?n]] @conn)))))

      (testing "flush indexer replays into LMDB"
        (let [lmdb (conn-lmdb conn)
              res  (d/flush-kv-indexer! lmdb)]
          (is (:drained? res))))

      (testing "data still visible after flush (now from LMDB base)"
        (is (= #{["Alice" 30] ["Bob" 25]}
               (set (d/q '[:find ?n ?a :where [?e :name ?n] [?e :age ?a]]
                         @conn)))))

      (finally
        (d/close conn)
        (u/delete-files dir)))))

;; ---------------------------------------------------------------------------
;; Test 5: Multi-tx overlay — multiple transactions visible before indexer
;; ---------------------------------------------------------------------------
(deftest dl-wal-multi-tx-overlay-test
  (let [dir  (u/tmp-dir (str "dl-wal-multi-tx-" (UUID/randomUUID)))
        conn (dl-wal-conn dir {:name {:db/unique :db.unique/identity}})]
    (try
      (d/transact! conn [{:name "Alice" :age 30}])
      (d/transact! conn [{:name "Bob" :age 25}])
      (d/transact! conn [{:name "Carol" :age 35}])

      (testing "all three transactions visible via overlay"
        (is (= #{["Alice" 30] ["Bob" 25] ["Carol" 35]}
               (set (d/q '[:find ?n ?a :where [?e :name ?n] [?e :age ?a]]
                         @conn)))))

      (testing "update across txs"
        (d/transact! conn [{:name "Alice" :age 31}])
        (is (= 31
               (d/q '[:find ?a . :where [?e :name "Alice"] [?e :age ?a]]
                    @conn))))

      (testing "retract from earlier overlay tx"
        (let [bob-id (d/q '[:find ?e . :where [?e :name "Bob"]] @conn)]
          (d/transact! conn [[:db.fn/retractEntity bob-id]])
          (is (= #{["Alice" 31] ["Carol" 35]}
                 (set (d/q '[:find ?n ?a :where [?e :name ?n] [?e :age ?a]]
                           @conn))))))

      (testing "flush indexer catches up all"
        (let [res (d/flush-kv-indexer! (conn-lmdb conn))]
          (is (:drained? res)))
        (is (= #{["Alice" 31] ["Carol" 35]}
               (set (d/q '[:find ?n ?a :where [?e :name ?n] [?e :age ?a]]
                         @conn)))))

      (finally
        (d/close conn)
        (u/delete-files dir)))))

;; ---------------------------------------------------------------------------
;; Test 6: with-transaction in WAL mode
;; ---------------------------------------------------------------------------
(deftest dl-wal-with-transaction-test
  (let [dir   (u/tmp-dir (str "dl-wal-with-tx-" (UUID/randomUUID)))
        conn  (dl-wal-conn dir {})
        query '[:find ?c .
                :in $ ?e
                :where [?e :counter ?c]]]
    (try
      (d/transact! conn [{:db/id 1 :counter 0}])
      (is (= 0 (d/q query @conn 1)))

      (testing "with-transaction commit"
        (d/with-transaction [cn conn]
          (is (= 0 (d/q query @cn 1)))
          (d/transact! cn [{:db/id 1 :counter 1}])
          (is (= 1 (d/q query @cn 1))))
        (is (= 1 (d/q query @conn 1))))

      (testing "with-transaction abort"
        (d/with-transaction [cn conn]
          (d/transact! cn [{:db/id 1 :counter 2}])
          (is (= 2 (d/q query @cn 1)))
          (d/abort-transact cn))
        (is (= 1 (d/q query @conn 1))))

      (finally
        (d/close conn)
        (u/delete-files dir)))))

;; ---------------------------------------------------------------------------
;; Test 7: Schema changes in WAL mode
;; ---------------------------------------------------------------------------
(deftest dl-wal-schema-test
  (let [dir    (u/tmp-dir (str "dl-wal-schema-" (UUID/randomUUID)))
        schema {:name {:db/unique :db.unique/identity}}
        conn   (dl-wal-conn dir schema)]
    (try
      (d/transact! conn [{:name "Alice" :age 30}])

      (testing "update schema: add cardinality-many attribute"
        (d/update-schema conn {:tags {:db/cardinality :db.cardinality/many}})
        (d/transact! conn [{:name "Alice" :tags ["a" "b"]}])
        (is (= #{"a" "b"}
               (set (d/q '[:find [?t ...] :where [?e :name "Alice"] [?e :tags ?t]]
                         @conn)))))

      (testing "schema persists after flush"
        (d/flush-kv-indexer! (conn-lmdb conn))
        (is (= #{"a" "b"}
               (set (d/q '[:find [?t ...] :where [?e :name "Alice"] [?e :tags ?t]]
                         @conn)))))

      (finally
        (d/close conn)
        (u/delete-files dir)))))

;; ---------------------------------------------------------------------------
;; Test 8: Various Datalog operations with WAL
;; ---------------------------------------------------------------------------
(deftest dl-wal-datoms-and-index-range-test
  (let [dir  (u/tmp-dir (str "dl-wal-datoms-" (UUID/randomUUID)))
        conn (dl-wal-conn dir {:name {:db/unique :db.unique/identity}
                               :age  {:db/index true}})]
    (try
      (d/transact! conn [{:name "Alice" :age 30}
                         {:name "Bob" :age 25}
                         {:name "Carol" :age 35}])

      (testing "datoms :eav"
        (let [ds (d/datoms @conn :eav)]
          (is (pos? (count ds)))))

      (testing "datoms :ave by attribute"
        (let [ds (d/datoms @conn :ave :age)]
          (is (= 3 (count ds)))
          ;; Values should be sorted ascending
          (is (= [25 30 35] (mapv :v ds)))))

      (testing "seek-datoms"
        (let [ds (d/seek-datoms @conn :ave :age 30)]
          (is (>= (count ds) 2))
          (is (every? #(>= (long (:v %)) 30) ds))))

      (testing "index-range"
        (let [ds (d/index-range @conn :age 25 35)]
          (is (= 3 (count ds)))
          (is (= [25 30 35] (mapv :v ds)))))

      (testing "pull"
        (let [eid (d/q '[:find ?e . :where [?e :name "Alice"]] @conn)]
          (is (= {:name "Alice" :age 30 :db/id eid}
                 (d/pull @conn '[*] eid)))))

      (finally
        (d/close conn)
        (u/delete-files dir)))))

;; ---------------------------------------------------------------------------
;; Test 9: Cardinality-many with WAL
;; ---------------------------------------------------------------------------
(deftest dl-wal-cardinality-many-test
  (let [dir  (u/tmp-dir (str "dl-wal-card-many-" (UUID/randomUUID)))
        conn (dl-wal-conn dir {:name {:db/unique :db.unique/identity}
                               :tags {:db/cardinality :db.cardinality/many}})]
    (try
      (d/transact! conn [{:name "Alice" :tags ["a" "b" "c"]}])

      (testing "all values visible"
        (is (= #{"a" "b" "c"}
               (set (d/q '[:find [?t ...] :where [?e :name "Alice"] [?e :tags ?t]]
                         @conn)))))

      (testing "retract one value"
        (let [eid (d/q '[:find ?e . :where [?e :name "Alice"]] @conn)]
          (d/transact! conn [[:db/retract eid :tags "b"]])
          (is (= #{"a" "c"}
                 (set (d/q '[:find [?t ...] :where [?e :name "Alice"] [?e :tags ?t]]
                           @conn))))))

      (testing "add more values"
        (d/transact! conn [{:name "Alice" :tags ["d"]}])
        (is (= #{"a" "c" "d"}
               (set (d/q '[:find [?t ...] :where [?e :name "Alice"] [?e :tags ?t]]
                         @conn)))))

      (finally
        (d/close conn)
        (u/delete-files dir)))))

;; ---------------------------------------------------------------------------
;; Test 10: Ref type with WAL
;; ---------------------------------------------------------------------------
(deftest dl-wal-ref-test
  (let [dir  (u/tmp-dir (str "dl-wal-ref-" (UUID/randomUUID)))
        conn (dl-wal-conn dir {:name    {:db/unique :db.unique/identity}
                               :friends {:db/valueType   :db.type/ref
                                         :db/cardinality :db.cardinality/many}})]
    (try
      (d/transact! conn [{:db/id -1 :name "Alice"}
                         {:db/id -2 :name "Bob"}
                         {:db/id -3 :name "Carol" :friends [-1 -2]}])

      (testing "ref query"
        (is (= #{"Alice" "Bob"}
               (set (d/q '[:find [?n ...]
                           :where [?e :name "Carol"]
                           [?e :friends ?f]
                           [?f :name ?n]]
                         @conn)))))

      (testing "reverse lookup"
        (let [carol-eid (d/q '[:find ?e . :where [?e :name "Carol"]] @conn)
              ent       (d/entity @conn carol-eid)]
          (is (= 2 (count (:friends ent))))))

      (finally
        (d/close conn)
        (u/delete-files dir)))))

;; ---------------------------------------------------------------------------
;; Test 11: Persistence across close/reopen with WAL replay
;; ---------------------------------------------------------------------------
(deftest dl-wal-persistence-test
  (let [dir    (u/tmp-dir (str "dl-wal-persist-" (UUID/randomUUID)))
        schema {:name {:db/unique :db.unique/identity}}]
    (try
      ;; First session: transact data, flush indexer, close
      (let [conn (dl-wal-conn dir schema)]
        (d/transact! conn [{:name "Alice" :age 30}
                           {:name "Bob" :age 25}])
        (d/flush-kv-indexer! (conn-lmdb conn))
        (d/close conn))

      ;; Second session: reopen and verify data persisted
      (let [conn (dl-wal-conn dir schema)]
        (is (= #{["Alice" 30] ["Bob" 25]}
               (set (d/q '[:find ?n ?a :where [?e :name ?n] [?e :age ?a]]
                         @conn))))
        (d/close conn))

      (finally
        (u/delete-files dir)))))

;; ---------------------------------------------------------------------------
;; Generative / fuzz tests
;; ---------------------------------------------------------------------------

(def ^:private gen-name
  "Generate a non-empty alphanumeric name string."
  (gen/fmap (fn [s] (if (empty? s) "a" s))
            gen/string-alphanumeric))

(def ^:private gen-entity
  "Generate a map entity with :name (identity) and :age."
  (gen/fmap (fn [[n a]] {:name n :age a})
            (gen/tuple gen-name (gen/choose 1 120))))

(def ^:private gen-entity-batch
  "Generate a non-empty batch of entities to transact."
  (gen/not-empty (gen/vector gen-entity 1 6)))

(def ^:private gen-tx-batches
  "Generate a sequence of 1-5 entity batches (each batch = one tx)."
  (gen/not-empty (gen/vector gen-entity-batch 1 5)))

(defn- expected-dl-state
  "Compute expected {name -> age} from a sequence of entity batches.
   Last-write-wins per :name."
  [batches]
  (reduce (fn [acc batch]
            (reduce (fn [m {:keys [name age]}]
                      (assoc m name age))
                    acc batch))
          {} batches))

;; Fuzz test 1: multi-tx write-read consistency in WAL mode
;;
;; Transact randomly generated entity batches, then verify that the
;; final DB state matches a simple last-write-wins model.  Also
;; verifies that datoms, pull, and entity APIs all agree.
(test/defspec dl-wal-fuzz-write-read-test
  100
  (prop/for-all
    [batches gen-tx-batches]
    (let [dir  (u/tmp-dir (str "dl-wal-fuzz-wr-" (UUID/randomUUID)))
          conn (dl-wal-conn dir {:name {:db/unique :db.unique/identity}})]
      (try
        (doseq [batch batches]
          (d/transact! conn batch))
        (let [expected (expected-dl-state batches)
              db       @conn]

          ;; 1. Query returns exactly the expected name->age pairs
          (let [result (set (d/q '[:find ?n ?a
                                   :where [?e :name ?n] [?e :age ?a]]
                                 db))]
            (is (= (set (map (fn [[n a]] [n a]) expected))
                   result)
                "Query should return all expected name/age pairs"))

          ;; 2. Entity count matches
          (let [names (d/q '[:find [?n ...] :where [?e :name ?n]] db)]
            (is (= (count expected) (count names))
                "Number of unique entities should match"))

          ;; 3. Pull returns consistent data for each entity
          (let [pulls-ok
                (every?
                  (fn [[n a]]
                    (let [eid (d/q '[:find ?e . :in $ ?n :where [?e :name ?n]]
                                   db n)]
                      (when eid
                        (let [pulled (d/pull db '[:name :age] eid)]
                          (and (= n (:name pulled))
                               (= a (:age pulled)))))))
                  expected)]
            (is pulls-ok "Pull should return consistent data for every entity"))

          ;; 4. datoms :eav has entries
          (is (pos? (count (d/datoms db :eav)))
              "datoms :eav should be non-empty"))
        (finally
          (d/close conn)
          (u/delete-files dir))))))

;; Fuzz test 2: transact + retract consistency
;;
;; Put some entities, then retract a subset, and verify
;; the remaining entities are correct.
(test/defspec dl-wal-fuzz-retract-test
  100
  (prop/for-all
    [entities (gen/not-empty (gen/vector gen-entity 2 10))]
    (let [dir  (u/tmp-dir (str "dl-wal-fuzz-ret-" (UUID/randomUUID)))
          conn (dl-wal-conn dir {:name {:db/unique :db.unique/identity}})]
      (try
        ;; Transact all entities
        (d/transact! conn entities)
        (let [db1       @conn
              all-eids  (d/q '[:find [?e ...] :where [?e :name _]] db1)
              to-del    (take (max 1 (quot (count all-eids) 2))
                             (shuffle all-eids))
              del-names (set (map (fn [eid]
                                    (d/q '[:find ?n . :in $ ?e
                                           :where [?e :name ?n]]
                                         db1 eid))
                                  to-del))]

          ;; Retract entities
          (d/transact! conn (mapv (fn [eid] [:db.fn/retractEntity eid])
                                  to-del))
          (let [db2    @conn
                result (set (d/q '[:find [?n ...] :where [?e :name ?n]]
                                 db2))]
            ;; Deleted names should be gone
            (is (empty? (set/intersection result del-names))
                "Retracted entities should not appear in query results")

            ;; Remaining entities should still be queryable
            (let [surviving          (set (d/q '[:find [?n ...]
                                        :where [?e :name ?n]] db1))
                  expected-remaining (set/difference surviving del-names)]
              (is (= expected-remaining result)
                  "Surviving entities should remain after retractions"))))
        (finally
          (d/close conn)
          (u/delete-files dir))))))

;; Fuzz test 3: flush-indexer consistency
;;
;; Transact data, flush the WAL indexer to push data to LMDB base,
;; then verify the data is still correct.
(test/defspec dl-wal-fuzz-flush-test
  50
  (prop/for-all
    [batches gen-tx-batches]
    (let [dir  (u/tmp-dir (str "dl-wal-fuzz-flush-" (UUID/randomUUID)))
          conn (dl-wal-conn dir {:name {:db/unique :db.unique/identity}})]
      (try
        (doseq [batch batches]
          (d/transact! conn batch))
        (let [expected (expected-dl-state batches)
              lmdb     (conn-lmdb conn)]

          ;; Verify data before flush (served from overlay)
          (let [before (set (d/q '[:find ?n ?a
                                   :where [?e :name ?n] [?e :age ?a]]
                                 @conn))]
            (is (= (set (map (fn [[n a]] [n a]) expected)) before)
                "Data should be correct before flush"))

          ;; Flush indexer
          (let [res (d/flush-kv-indexer! lmdb)]
            (is (:drained? res)
                "Indexer should drain all committed WAL entries"))

          ;; Verify data after flush (served from LMDB base)
          (let [after (set (d/q '[:find ?n ?a
                                  :where [?e :name ?n] [?e :age ?a]]
                                @conn))]
            (is (= (set (map (fn [[n a]] [n a]) expected)) after)
                "Data should be correct after flush")))
        (finally
          (d/close conn)
          (u/delete-files dir))))))

;; Fuzz test 4: interleaved transact + flush
;;
;; Alternate between transacting and flushing to exercise the overlay
;; merge path where some data is in LMDB base and some in overlay.
(test/defspec dl-wal-fuzz-interleaved-flush-test
  50
  (prop/for-all
    [batches (gen/not-empty (gen/vector gen-entity-batch 2 6))]
    (let [dir  (u/tmp-dir (str "dl-wal-fuzz-interleave-" (UUID/randomUUID)))
          conn (dl-wal-conn dir {:name {:db/unique :db.unique/identity}})]
      (try
        (let [lmdb     (conn-lmdb conn)
              mid      (max 1 (quot (count batches) 2))
              first-h  (subvec (vec batches) 0 mid)
              second-h (subvec (vec batches) mid)]

          ;; Transact first half and flush to LMDB base
          (doseq [batch first-h]
            (d/transact! conn batch))
          (d/flush-kv-indexer! lmdb)

          ;; Transact second half (goes to overlay)
          (doseq [batch second-h]
            (d/transact! conn batch))

          ;; Expected state is all batches combined
          (let [expected (expected-dl-state batches)
                result   (set (d/q '[:find ?n ?a
                                     :where [?e :name ?n] [?e :age ?a]]
                                   @conn))]
            (is (= (set (map (fn [[n a]] [n a]) expected)) result)
                "Mixed base+overlay data should be queryable correctly"))

          ;; Final flush should drain
          (let [res (d/flush-kv-indexer! lmdb)]
            (is (:drained? res)))

          ;; Verify after second flush
          (let [expected (expected-dl-state batches)
                result   (set (d/q '[:find ?n ?a
                                     :where [?e :name ?n] [?e :age ?a]]
                                   @conn))]
            (is (= (set (map (fn [[n a]] [n a]) expected)) result)
                "All data correct after final flush")))
        (finally
          (d/close conn)
          (u/delete-files dir))))))

;; ---------------------------------------------------------------------------
;; Test 12: Fulltext search with WAL
;; ---------------------------------------------------------------------------
(deftest dl-wal-fulltext-test
  (let [dir  (u/tmp-dir (str "dl-wal-ft-" (UUID/randomUUID)))
        conn (dl-wal-conn dir {:name {:db/valueType :db.type/string
                                      :db/unique    :db.unique/identity}
                               :bio  {:db/valueType :db.type/string
                                      :db/fulltext  true}})]
    (try
      (testing "add fulltext entities"
        (d/transact! conn [{:name "Alice"
                            :bio  "A quick brown fox jumps over the lazy dog"}
                           {:name "Bob"
                            :bio  "The rain in Spain stays mainly in the plain"}])

        (testing "fulltext query returns results from overlay"
          (is (= 1 (count (d/fulltext-datoms @conn "brown fox"))))
          (is (= "A quick brown fox jumps over the lazy dog"
                 (peek (first (d/fulltext-datoms @conn "brown fox"))))))

        (testing "fulltext in Datalog query"
          (is (= 1 (count (d/q '[:find ?v
                                 :in $ ?q
                                 :where [(fulltext $ ?q) [[?e ?a ?v]]]]
                               @conn "brown fox")))))

        (testing "fulltext query for second doc"
          (is (pos? (count (d/fulltext-datoms @conn "Spain"))))))

      (testing "update fulltext entity"
        (d/transact! conn [{:name "Alice"
                            :bio  "Exploring the vast universe of knowledge"}])
        (is (empty? (d/fulltext-datoms @conn "brown fox"))
            "old text should not match after update")
        (is (pos? (count (d/fulltext-datoms @conn "universe")))
            "new text should match"))

      (testing "retract fulltext attribute"
        (let [bob-id (d/q '[:find ?e . :where [?e :name "Bob"]] @conn)]
          (d/transact! conn [[:db/retract bob-id :bio
                              "The rain in Spain stays mainly in the plain"]])
          (is (empty? (d/fulltext-datoms @conn "Spain"))
              "retracted fulltext should not match")))

      (testing "fulltext survives flush"
        (d/flush-kv-indexer! (conn-lmdb conn))
        (is (pos? (count (d/fulltext-datoms @conn "universe")))
            "fulltext should work after flush"))

      (finally
        (d/close conn)
        (u/delete-files dir)))))

;; ---------------------------------------------------------------------------
;; Test 13: Fulltext with multiple txs before flush
;; ---------------------------------------------------------------------------
(deftest dl-wal-fulltext-multi-tx-test
  (let [dir  (u/tmp-dir (str "dl-wal-ft-multi-" (UUID/randomUUID)))
        conn (dl-wal-conn dir {:name {:db/valueType :db.type/string
                                      :db/unique    :db.unique/identity}
                               :bio  {:db/valueType :db.type/string
                                      :db/fulltext  true}})]
    (try
      (d/transact! conn [{:name "Alice" :bio "Introduction to Clojure programming"}])
      (d/transact! conn [{:name "Bob" :bio "Advanced Java virtual machine internals"}])
      (d/transact! conn [{:name "Carol" :bio "Functional programming with Haskell"}])

      (testing "all three docs searchable via fulltext"
        (is (= 1 (count (d/fulltext-datoms @conn "Clojure"))))
        (is (= 1 (count (d/fulltext-datoms @conn "Java"))))
        (is (= 1 (count (d/fulltext-datoms @conn "Haskell")))))

      (testing "fulltext Datalog query works"
        (is (= #{["Introduction to Clojure programming"]}
               (set (d/q '[:find ?v
                           :in $ ?q
                           :where [(fulltext $ ?q) [[?e ?a ?v]]]]
                         @conn "Clojure")))))

      (testing "update one doc via identity"
        (d/transact! conn [{:name "Alice" :bio "Exploring Clojure web development"}])
        ;; "programming" should now only match Haskell + Carol
        (is (= 1 (count (d/fulltext-datoms @conn "programming")))
            "only Haskell doc matches 'programming' now")
        (is (= 1 (count (d/fulltext-datoms @conn "web development")))
            "new bio term present"))

      (testing "flush and verify"
        (d/flush-kv-indexer! (conn-lmdb conn))
        (is (= 1 (count (d/fulltext-datoms @conn "Java"))))
        (is (= 1 (count (d/fulltext-datoms @conn "web development")))))

      (finally
        (d/close conn)
        (u/delete-files dir)))))

;; ---------------------------------------------------------------------------
;; Test 14: Fulltext persistence across close/reopen
;; ---------------------------------------------------------------------------
(deftest dl-wal-fulltext-persistence-test
  (let [dir    (u/tmp-dir (str "dl-wal-ft-persist-" (UUID/randomUUID)))
        schema {:title {:db/valueType :db.type/string
                        :db/unique    :db.unique/identity
                        :db/fulltext  true}}]
    (try
      ;; Session 1: transact, flush, close
      (let [conn (dl-wal-conn dir schema)]
        (d/transact! conn [{:title "Clojure for the brave and true"}
                           {:title "Structure and interpretation"}])
        (d/flush-kv-indexer! (conn-lmdb conn))
        (d/close conn))

      ;; Session 2: reopen and verify fulltext works
      (let [conn (dl-wal-conn dir schema)]
        (is (= 1 (count (d/fulltext-datoms @conn "brave"))))
        (is (= 1 (count (d/fulltext-datoms @conn "interpretation"))))
        (d/close conn))

      (finally
        (u/delete-files dir)))))

;; ---------------------------------------------------------------------------
;; Test 15: Idoc (document index) with WAL
;; ---------------------------------------------------------------------------
(deftest dl-wal-idoc-test
  (let [dir  (u/tmp-dir (str "dl-wal-idoc-" (UUID/randomUUID)))
        conn (dl-wal-conn dir {:name    {:db/valueType :db.type/string
                                         :db/unique    :db.unique/identity}
                               :profile {:db/valueType :db.type/idoc}})]
    (try
      (testing "add idoc entities"
        (d/transact! conn [{:name    "Alice"
                            :profile {:status "active" :age 30 :city "NYC"}}
                           {:name    "Bob"
                            :profile {:status "active" :age 25 :city "LA"}}])

        (testing "idoc-match query via overlay"
          (is (= 2 (count
                     (d/q '[:find ?e
                            :in $ ?q
                            :where
                            [(idoc-match $ :profile ?q) [[?e ?a ?v]]]]
                          @conn {:status "active"})))))

        (testing "idoc-match with value filter"
          (is (= 1 (count
                     (d/q '[:find ?e
                            :where
                            [(idoc-match $ :profile {:age (> 28)})
                             [[?e ?a ?v]]]]
                          @conn))))))

      (testing "update idoc entity"
        (d/transact! conn [{:name    "Alice"
                            :profile {:status "inactive" :age 31 :city "SF"}}])
        (is (= 1 (count
                   (d/q '[:find ?e
                          :in $ ?q
                          :where
                          [(idoc-match $ :profile ?q) [[?e ?a ?v]]]]
                        @conn {:status "active"})))
            "only Bob is 'active' now")
        (is (= 1 (count
                   (d/q '[:find ?e
                          :where
                          [(idoc-match $ :profile {:city "SF"})
                           [[?e ?a ?v]]]]
                        @conn)))
            "Alice now in SF"))

      (testing "idoc survives flush"
        (d/flush-kv-indexer! (conn-lmdb conn))
        (is (= 1 (count
                   (d/q '[:find ?e
                          :where
                          [(idoc-match $ :profile {:city "SF"})
                           [[?e ?a ?v]]]]
                        @conn)))
            "idoc works after flush"))

      (finally
        (d/close conn)
        (u/delete-files dir)))))

;; ---------------------------------------------------------------------------
;; Test 16: Vector index with WAL mode (KV-level)
;; ---------------------------------------------------------------------------
(deftest dl-wal-vector-kv-test
  (when-not (u/windows?)
    (let [dir  (u/tmp-dir (str "dl-wal-vec-kv-" (UUID/randomUUID)))
          lmdb (d/open-kv dir {:flags   (conj c/default-env-flags :nosync)
                               :kv-wal? true})
          n    200
          v1   (float-array (repeatedly n #(float (rand))))
          v2   (float-array (repeatedly n #(float (rand))))]
      (try
        (let [index (d/new-vector-index lmdb {:dimensions n})]
          (testing "WAL-mode vector add and search"
            (d/add-vec index :ok v1)
            (is (= 1 (:size (d/vector-index-info index))))
            (is (i/vec-indexed? index :ok))
            (is (= [:ok] (d/search-vec index v1)))
            (is (= [(vec v1)] (mapv vec (i/get-vec index :ok)))))

          (testing "add second vector"
            (d/add-vec index :nice v2)
            (is (= 2 (:size (d/vector-index-info index))))
            (is (= [:nice] (d/search-vec index v2 {:top 1}))))

          (testing "no .vid file in WAL mode"
            (is (not (u/file-exists
                       (str (i/env-dir lmdb) u/+separator+ c/default-domain
                            c/vector-index-suffix)))))

          (testing "close and reopen preserves vector index"
            (d/close-vector-index index))

          (let [index2 (d/new-vector-index lmdb {:dimensions n})]
            (is (= 2 (:size (d/vector-index-info index2))))
            (is (i/vec-indexed? index2 :ok))
            (is (i/vec-indexed? index2 :nice))
            (is (= [:ok] (d/search-vec index2 v1 {:top 1})))
            (is (= [:nice] (d/search-vec index2 v2 {:top 1})))

            (testing "remove vector in WAL mode"
              (d/remove-vec index2 :ok)
              (is (= 1 (:size (d/vector-index-info index2))))
              (is (not (i/vec-indexed? index2 :ok)))
              (is (= [:nice] (d/search-vec index2 v2))))

            (testing "clear vector index in WAL mode"
              (d/clear-vector-index index2))

            (let [index3 (d/new-vector-index lmdb {:dimensions n})]
              (is (= 0 (:size (d/vector-index-info index3))))
              (d/close-vector-index index3))))
        (finally
          (d/close-kv lmdb)
          (u/delete-files dir))))))

;; ---------------------------------------------------------------------------
;; Test: Concurrent writes stress test for WAL mode
;; ---------------------------------------------------------------------------
(deftest dl-wal-concurrent-writes-stress-test
  (let [dir       (u/tmp-dir (str "dl-wal-conc-write-" (UUID/randomUUID)))
        conn      (dl-wal-conn dir {:instance/id
                                    {:db/valueType   :db.type/long
                                     :db/unique      :db.unique/identity
                                     :db/cardinality :db.cardinality/one}})
        n-threads 5
        n-per     100]
    (try
      (testing "concurrent transact! with WAL"
        (dorun (pmap #(d/transact! conn [{:instance/id %}])
                     (range (* n-threads n-per))))
        (is (= (* n-threads n-per)
               (count (d/q '[:find ?e :where [?e :instance/id _]] @conn))))
        (is (= (* n-threads n-per)
               (d/q '[:find (count ?e) . :where [?e :instance/id _]] @conn))))

      (testing "data correct after flush"
        (d/flush-kv-indexer! (conn-lmdb conn))
        (is (= (* n-threads n-per)
               (d/q '[:find (count ?e) . :where [?e :instance/id _]] @conn))))

      (finally
        (d/close conn)
        (u/delete-files dir)))))

;; ---------------------------------------------------------------------------
;; Test: Concurrent reads + writes with with-transaction in WAL mode
;; ---------------------------------------------------------------------------
(deftest dl-wal-concurrent-read-write-stress-test
  (let [dir       (u/tmp-dir (str "dl-wal-conc-rw-" (UUID/randomUUID)))
        conn      (dl-wal-conn dir {})
        q+        '[:find ?v .
                  :in $ ?i ?j
                  :where [?e :i+j ?v] [?e :i ?i] [?e :j ?j]]
        q*        '[:find ?v .
                  :in $ ?i ?j
                  :where [?e :i*j ?v] [?e :i ?i] [?e :j ?j]]
        trials    (atom 0)
        n-threads 5
        n-iters   50
        futures   (mapv (fn [^long i]
                        (future
                          (dotimes [j n-iters]
                            (d/transact! conn [{:i+j (+ i j) :i i :j j}])
                            (d/with-transaction [cn conn]
                              (is (= (+ i j) (d/q q+ (d/db cn) i j)))
                              (swap! trials u/long-inc)
                              (d/transact! cn [{:i*j (* i j) :i i :j j}])
                              (is (= (* i j) (d/q q* (d/db cn) i j)))))))
                      (range n-threads))]
    (try
      (doseq [f futures] @f)

      (testing "all trials completed"
        (is (= (* n-threads n-iters) @trials)))

      (testing "all datoms present"
        ;; each iteration creates 2 entities with 3 attrs each = 6 datoms
        (is (= (* 6 n-threads n-iters)
               (count (d/datoms @conn :eav)))))

      (testing "query correctness after concurrent writes"
        (dorun (for [i (range n-threads) j (range n-iters)]
                 (do (is (= (+ ^long i ^long j) (d/q q+ @conn i j)))
                     (is (= (* ^long i ^long j) (d/q q* @conn i j)))))))

      (testing "data survives flush"
        (d/flush-kv-indexer! (conn-lmdb conn))
        (dorun (for [i (range n-threads) j (range n-iters)]
                 (do (is (= (+ ^long i ^long j) (d/q q+ @conn i j)))
                     (is (= (* ^long i ^long j) (d/q q* @conn i j)))))))

      (finally
        (d/close conn)
        (u/delete-files dir)))))

;; ---------------------------------------------------------------------------
;; Test: Concurrent reads during writes in WAL mode
;; ---------------------------------------------------------------------------
(deftest dl-wal-concurrent-readers-test
  (let [dir  (u/tmp-dir (str "dl-wal-conc-read-" (UUID/randomUUID)))
        conn (dl-wal-conn dir {:id {:db/unique :db.unique/identity}})
        n    200]
    (try
      ;; Seed data
      (d/transact! conn (mapv (fn [i] {:id i :value (* ^long i 10)}) (range n)))

      (testing "concurrent reads are consistent during writes"
        (let [writer  (future
                        (dotimes [i n]
                          (d/transact! conn [{:id (+ n i) :value (* (+ n i) 10)}])))
              readers (mapv (fn [_]
                              (future
                                (dotimes [i n]
                                  (let [v (d/q '[:find ?v .
                                                 :in $ ?i
                                                 :where [?e :id ?i] [?e :value ?v]]
                                               @conn i)]
                                    (is (= (* i 10) v))))))
                            (range 5))]
          @writer
          (doseq [r readers] @r)))

      (testing "all data present after concurrent activity"
        (is (= (* 2 n)
               (d/q '[:find (count ?e) . :where [?e :id _]] @conn))))

      (testing "data correct after flush"
        (d/flush-kv-indexer! (conn-lmdb conn))
        (is (= (* 2 n)
               (d/q '[:find (count ?e) . :where [?e :id _]] @conn))))

      (finally
        (d/close conn)
        (u/delete-files dir)))))

;; ---------------------------------------------------------------------------
;; Test 17: Vector index file-spool mode (forced by low buffer threshold)
;; ---------------------------------------------------------------------------
(deftest dl-wal-vector-file-spool-test
  (when-not (u/windows?)
    (let [dir  (u/tmp-dir (str "dl-wal-vec-spool-" (UUID/randomUUID)))
          lmdb (d/open-kv dir {:flags   (conj c/default-env-flags :nosync)
                               :kv-wal? true})
          n    200
          v1   (float-array (repeatedly n #(float (rand))))]
      (try
        ;; Force file-spool mode by setting very low buffer threshold
        (binding [c/*wal-vec-max-buffer-bytes* 1]
          (let [index (d/new-vector-index lmdb {:dimensions n})]
            (d/add-vec index :spool-test v1)
            (is (= 1 (:size (d/vector-index-info index))))
            (is (= [:spool-test] (d/search-vec index v1)))

            ;; Close triggers file-spool checkpoint
            (d/close-vector-index index))

          ;; Reopen with same low threshold - should file-spool load
          (let [index2 (d/new-vector-index lmdb {:dimensions n})]
            (is (= 1 (:size (d/vector-index-info index2))))
            (is (i/vec-indexed? index2 :spool-test))
            (is (= [:spool-test] (d/search-vec index2 v1)))
            (d/close-vector-index index2)))
        (finally
          (d/close-kv lmdb)
          (u/delete-files dir))))))

;; ---------------------------------------------------------------------------
;; Test 18: Vector index Datalog WAL integration
;; ---------------------------------------------------------------------------
(deftest dl-wal-vector-datalog-test
  (when-not (u/windows?)
    (let [dir    (u/tmp-dir (str "dl-wal-vec-dl-" (UUID/randomUUID)))
          schema {:chunk/id        {:db/valueType :db.type/string
                                    :db/unique    :db.unique/identity}
                  :chunk/embedding {:db/valueType :db.type/vec}}
          conn   (d/create-conn dir schema
                                {:kv-opts     {:flags (conj c/default-env-flags
                                                            :nosync)}
                                 :vector-opts {:dimensions 3}})]
      (try
        (testing "add vector entities in WAL mode"
          (d/transact! conn [{:chunk/id        "cat"
                              :chunk/embedding [0.1 0.2 0.3]}
                             {:chunk/id        "dog"
                              :chunk/embedding [0.4 0.5 0.6]}
                             {:chunk/id        "fish"
                              :chunk/embedding [0.7 0.8 0.9]}])

          (is (= 3 (count (d/q '[:find ?e
                                 :where [?e :chunk/id _]]
                               @conn)))))

        (testing "vec-neighbors query works in WAL mode"
          (let [results (d/q '[:find [?i ...]
                               :in $ ?q
                               :where
                               [(vec-neighbors $ :chunk/embedding ?q {:top 1})
                                [[?e _ _]]]
                               [?e :chunk/id ?i]]
                             (d/db conn) [0.1 0.2 0.3])]
            (is (= ["cat"] results))))

        (testing "vector data persists after flush"
          (d/flush-kv-indexer! (conn-lmdb conn))
          (let [results (d/q '[:find [?i ...]
                               :in $ ?q
                               :where
                               [(vec-neighbors $ :chunk/embedding ?q {:top 1})
                                [[?e _ _]]]
                               [?e :chunk/id ?i]]
                             (d/db conn) [0.4 0.5 0.6])]
            (is (= ["dog"] results))))

        (finally
          (d/close conn)
          (u/delete-files dir))))))

;; ---------------------------------------------------------------------------
;; Test 19: Vector index non-WAL to WAL transition (LMDB blob in both modes)
;; ---------------------------------------------------------------------------
(deftest dl-wal-vector-migration-test
  (when-not (u/windows?)
    (let [dir (u/tmp-dir (str "dl-wal-vec-mig-" (UUID/randomUUID)))
          n   200
          v1  (float-array (repeatedly n #(float (rand))))
          v2  (float-array (repeatedly n #(float (rand))))]
      (try
        ;; Session 1: Create vector index without WAL (stored in LMDB blob)
        (let [lmdb  (d/open-kv dir {:flags (conj c/default-env-flags :nosync)})
              index (d/new-vector-index lmdb {:dimensions n})]
          (d/add-vec index :alpha v1)
          (d/add-vec index :beta v2)
          (is (= 2 (:size (d/vector-index-info index))))
          (d/close-vector-index index)
          ;; No .vid file — vector data is in LMDB blob
          (is (not (u/file-exists
                     (str (i/env-dir lmdb) u/+separator+ c/default-domain
                          c/vector-index-suffix))))
          (d/close-kv lmdb))

        ;; Session 2: Reopen with WAL enabled - data persists from LMDB blob
        (let [lmdb  (d/open-kv dir {:flags   (conj c/default-env-flags :nosync)
                                    :kv-wal? true})
              index (d/new-vector-index lmdb {:dimensions n})]
          ;; Data should be intact
          (is (= 2 (:size (d/vector-index-info index))))
          (is (i/vec-indexed? index :alpha))
          (is (i/vec-indexed? index :beta))
          (is (= [:alpha] (d/search-vec index v1 {:top 1})))
          (is (= [:beta] (d/search-vec index v2 {:top 1})))
          (d/close-vector-index index)
          (d/close-kv lmdb))

        (finally
          (u/delete-files dir))))))

;; ---------------------------------------------------------------------------
;; Test 20: Vector WAL persistence across close/reopen
;; ---------------------------------------------------------------------------
(deftest dl-wal-vector-persistence-test
  (when-not (u/windows?)
    (let [dir (u/tmp-dir (str "dl-wal-vec-persist-" (UUID/randomUUID)))
          n   200
          v1  (float-array (repeatedly n #(float (rand))))
          v2  (float-array (repeatedly n #(float (rand))))]
      (try
        ;; Session 1: Create and populate vector index in WAL mode
        (let [lmdb  (d/open-kv dir {:flags   (conj c/default-env-flags :nosync)
                                    :kv-wal? true})
              index (d/new-vector-index lmdb {:dimensions n})]
          (d/add-vec index :first v1)
          (d/add-vec index :second v2)
          (d/close-vector-index index)
          (d/close-kv lmdb))

        ;; Session 2: Reopen and verify data persisted
        (let [lmdb  (d/open-kv dir {:flags   (conj c/default-env-flags :nosync)
                                    :kv-wal? true})
              index (d/new-vector-index lmdb {:dimensions n})]
          (is (= 2 (:size (d/vector-index-info index))))
          (is (i/vec-indexed? index :first))
          (is (i/vec-indexed? index :second))
          (is (= [:first] (d/search-vec index v1 {:top 1})))
          (is (= [:second] (d/search-vec index v2 {:top 1})))
          (d/close-vector-index index)
          (d/close-kv lmdb))

        (finally
          (u/delete-files dir))))))
