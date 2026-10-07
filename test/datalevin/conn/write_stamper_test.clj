(ns ^{:clj-kondo/config
      '{:lint-as {datalevin.conn.write-stamper-test/with-conn clojure.core/let}}}
  datalevin.conn.write-stamper-test
  (:require
   [clojure.test :refer [deftest is testing use-fixtures]]
   [datalevin.binding.cpp :as cpp]
   [datalevin.conn :as conn]
   [datalevin.core :as d]
   [datalevin.db :as db]
   [datalevin.datom :as datom]
   [datalevin.interface :as i]
   [datalevin.lmdb :as l]
   [datalevin.prepare :as prepare]
   [datalevin.test.core :refer [db-fixture]]
   [datalevin.tx-group.batch :as batch]
   [datalevin.tx-group.phase :as phase]
   [datalevin.util :as u])
  (:import [datalevin.tx_group.batch Collector]
           [java.util.concurrent ConcurrentLinkedQueue]))

(use-fixtures :each db-fixture)

(def fields (mapv #(keyword "item" (str "field" %)) (range 10)))
(def schema
  (assoc (zipmap fields (repeat {:db/valueType :db.type/long}))
         :item/key {:db/valueType :db.type/string :db/unique :db.unique/identity}
         :item/token {:db/valueType :db.type/string :db/unique :db.unique/value}))

(defmacro with-conn [[conn [schema opts]] & body]
  `(let [dir# (u/tmp-dir (str "writing-stamper-" (random-uuid)))
         ~conn (d/create-conn dir# ~schema ~opts)]
     (try ~@body (finally (d/close ~conn) (u/delete-files dir#)))))

(defn- report-data [report]
  {:datoms (mapv (juxt :e :a :v :tx datom/datom-added) (:tx-data report))
   :tempids (:tempids report) :tx-meta (:tx-meta report)})

(defn- correction-outcome [f]
  (try
    (let [v (f)] [:value (if (bytes? v) (vec v) v)])
    (catch Exception e [:error (class e) (ex-message e) (ex-data e)])))

(deftest scalar-preparation-preserves-built-in-validation-and-coercion
  (let [cases {:db.type/long [1 (int 2) 3.5 "bad"]
               :db.type/string ["text" :text 42]
               :db.type/keyword [:text "text" 42]
               :db.type/symbol ['text "text" 42]
               :db.type/float [(float 1) 2.5 "bad"]
               :db.type/double [1.0 (float 2) "bad"]
               :db.type/boolean [false true 0 :text]
               :db.type/instant [(java.util.Date. 123) (java.time.Instant/ofEpochMilli 456)
                                 789 "bad"]
               :db.type/uuid [(random-uuid) "123e4567-e89b-12d3-a456-426614174000" "bad"]
               :db.type/bytes [(byte-array [1 2]) [3 4] "bad"]
               :db.type/bigint [1N (biginteger 2) "123" "bad"]
               :db.type/bigdec [1M 2 "123.5" "bad"]
               nil [false [] {:key "value"}]}
        schema (into {:seed {:db/valueType :db.type/long}}
                     (map (fn [[vt _]] [(keyword "scalar" (if vt (name vt) "untyped"))
                                       (if vt {:db/valueType vt} {})])) cases)]
    (doseq [validate? [false true]]
      (with-conn [conn [schema {:wal? true :validate-data? validate?}]]
        (d/transact! conn [{:db/id 1 :seed 0}])
        (doseq [[vt values] cases
                :let [attr (keyword "scalar" (if vt (name vt) "untyped"))
                      props ((d/schema conn) attr)]
                value values]
          (is (= (correction-outcome #(prepare/correct-value-with-props
                                       {:validate-data? validate?} props attr value))
                 (correction-outcome #(let [prepared (db/prepare-scalar-update-tx
                                                      @conn [[:db/add 1 attr value]])]
                                        (assert (some? prepared))
                                        (nth (first (:entries prepared)) 2))))
              (str "type=" vt " validation=" validate? " value=" (pr-str value))))
        (is (nil? (db/prepare-scalar-update-tx @conn [[:db/add 1 :seed nil]])))
        (is (nil? (db/prepare-scalar-update-tx @conn [[:db/add 1 "seed" 1]])))
        (is (thrown-with-msg? Exception #"Cannot store nil"
                             (d/transact! conn [[:db/add 1 :seed nil]])))
        (is (thrown-with-msg? Exception #"Bad entity attribute"
                             (d/transact! conn [[:db/add 1 "seed" 1]])))
        (is (= 0 (:seed (d/entity @conn 1))))))))

(defn- general-transact [db tx-data tx-meta]
  (#'conn/with-isolated-tx-cache db tx-data tx-meta false))

(defn- exercise-explicit [conn]
  (d/with-transaction [tx conn]
    (mapv (fn [idx tx-data]
            (let [before @tx
                  report (d/transact! tx tx-data {:request idx})]
              (is (identical? before (:db-before report)))
              (is (identical? @tx (:db-after report)))
              (is (not (identical? (:eavt before) (:eavt @tx))))
              (is (identical? (:store before) (:store @tx)))
              (report-data report)))
          (range)
          [[{:item/key "one" (fields 0) 1}]
           [(assoc (zipmap fields (repeat 2)) :db/id "upsert" :item/key "one")]
           [(assoc (zipmap fields (repeat 2)) :db/id "upsert" :item/key "one")]
           [[:db/add [:item/key "one"] (fields 0) 3]]
           [[:db.fn/call (fn [tx-db]
                          (is (= 3 (get (d/entity tx-db 1) (fields 0))))
                          [[:db/add 1 (fields 0) 4]])]]
           [[:db/add 1 (fields 0) 5]]
           [{:db/id "new" :item/key "two" (fields 0) 7}]
           [[:db/add [:item/key "two"] (fields 0) 8]]
           [[:db/add 1 (fields 0) 6] [:db/add 1 (fields 0) 7]]
           [{:item/key "one" (fields 0) 9}]
           [{:db/id "plain" (fields 0) 11}]])))

(deftest explicit-stampers-match-the-general-interpreter
  (doseq [wal? [false true]]
    (with-conn [fast [schema {:wal? wal?}]]
      (with-conn [general [schema {:wal? wal?}]]
        (doseq [conn [fast general]]
          (d/transact! conn [(assoc (zipmap fields (repeat 0))
                                   :db/id 1 :item/key "one")]))
        (let [expected (with-redefs-fn {#'conn/transact-local-in-write-txn! general-transact}
                         #(exercise-explicit general))
              paths (atom [])
              commits (atom 0)
              actual (binding [conn/*local-wal-tx-path-observer* #(swap! paths conj %)
                               cpp/*before-write-commit-fn* (fn [_] (swap! commits inc))]
                       (exercise-explicit fast))]
          (is (= expected actual))
          ;; The observer records general fallbacks as well as specialized stamps.
          (is (= [:identity-upsert :identity-upsert :identity-upsert
                  :scalar-update :general :scalar-update :blind-insert
                  :scalar-update :general :identity-upsert :blind-insert]
                 @paths))
          (is (= 1 @commits))
          (is (= 9 (get (d/entity @fast 1) (fields 0))))
          (is (= 8 (get (d/entity @fast [:item/key "two"]) (fields 0))))
          (is (= (:max-eid @general) (:max-eid @fast))))))))

(deftest explicit-stampers-preserve-abort-and-caught-validation-errors
  (doseq [wal? [false true]]
    (with-conn [conn [schema {:wal? wal?}]]
      (d/transact! conn [{:db/id 1 :item/key "one" :item/token "taken" (fields 0) 0}])
      (d/with-transaction [tx conn]
        (let [before @tx]
          (is (thrown? Exception
                       (d/transact! tx [[:db/add 1 (fields 0) 1]
                                        [:db/add 1 (fields 1) "wrong type"]])))
          (is (identical? before @tx))
          (is (= 0 (get (d/entity @tx 1) (fields 0)))))
        ;; A blind collision must fall back before staging any part of a batch.
        (is (thrown? Exception
                     (d/transact! tx [{:item/token "free"}
                                      {:item/token "taken"}])))
        (is (nil? (d/entid @tx [:item/token "free"])))
        (d/transact! tx [{:item/key "one" (fields 0) 2}])
        (is (= 2 (get (d/entity @tx 1) (fields 0))))
        (d/abort-transact tx))
      (is (= 0 (get (d/entity @conn 1) (fields 0))))
      (is (thrown-with-msg? clojure.lang.ExceptionInfo #"rollback"
                           (d/with-transaction [tx conn]
                             (d/transact! tx [{:item/key "one" (fields 0) 3}])
                             (throw (ex-info "rollback" {})))))
      (is (= 0 (get (d/entity @conn 1) (fields 0))))
      (d/with-transaction [tx conn]
        (d/transact! tx [{:item/key "one" (fields 0) 4}]))
      (is (= 4 (get (d/entity @conn 1) (fields 0)))))))

(deftest explicit-stampers-retain-schema-and-tuple-guards
  (with-conn [conn [(assoc schema :item/pair {:db/valueType :db.type/tuple
                                            :db/tupleAttrs [(fields 0) (fields 1)]})
                   {:wal? true}]]
    (d/transact! conn [{:db/id 1 :item/key "one" (fields 0) 0 (fields 1) 1}])
    (let [paths (atom [])]
      (binding [conn/*local-wal-tx-path-observer* #(swap! paths conj %)]
        (d/with-transaction [tx conn]
          (d/transact! tx [[:db/add 1 (fields 0) 2]])
          (d/transact! tx [{:item/key "one" (fields 1) 3}])
          (is (= [2 3] (:item/pair (d/entity @tx 1))))
          (d/transact! tx [[:db/add 1 (fields 2) 4]])
          (d/update-schema tx {(fields 2) {:db/cardinality :db.cardinality/many}})
          (d/transact! tx [[:db/add 1 (fields 2) 5]])
          (is (= #{4 5} (get (d/entity @tx 1) (fields 2))))))
      (is (= [:general :general :scalar-update :general] @paths))
      (is (= [2 3] (:item/pair (d/entity @conn 1))))
      (is (= #{4 5} (get (d/entity @conn 1) (fields 2)))))))

(deftest explicit-stampers-preserve-global-timeout-wrapper
  (with-conn [conn [schema {:wal? true}]]
    (d/transact! conn [{:db/id 1 :item/key "one" (fields 0) 0}])
    (let [previous (l/explicit-transaction-timeout)
          paths (atom [])]
      (try
        (l/set-explicit-transaction-timeout! 5000)
        (binding [conn/*local-wal-tx-path-observer* #(swap! paths conj %)]
          (d/with-transaction [tx conn]
            (d/transact! tx [{:item/key "one" (fields 0) 1}])
            (d/transact! tx [[:db/add 1 (fields 0) 2]])
            (is (= 2 (get (d/entity @tx 1) (fields 0))))))
        (is (= [:identity-upsert :scalar-update] @paths))
        (finally (l/set-explicit-transaction-timeout! previous))))))

(deftest explicit-document-patches-read-preceding-writes
  (with-conn [conn [{:doc {:db/valueType :db.type/idoc :db/domain "docs"}}
                   {:wal? true}]]
    (d/transact! conn [{:db/id 1 :doc {:counter 0}}])
    (let [paths (atom [])]
      (binding [conn/*local-wal-tx-path-observer* #(swap! paths conj %)]
        (d/with-transaction [tx conn]
          (d/transact! tx [{:db/id 1 :doc {:counter 1}}])
          (d/transact! tx [[:db.fn/patchIdoc 1 :doc [[:set [:counter] 2]]]])
          (is (= {:counter 2} (:doc (d/entity @tx 1))))))
      (is (= [:general :patch-idoc] @paths))
      (is (= #{1} (set (d/q '[:find [?e ...] :in $ ?query :where
                              [(idoc-match $ :doc ?query) [[?e ?a ?v]]]]
                            @conn {:counter 2})))))))

(defn- queued-batch! [conn txs]
  (let [^Collector collector (get-in @(i/kv-info (d/datalog-kv conn))
                                    [:independent-control :collector])
        ^ConcurrentLinkedQueue queue (.-ready collector)
        entered (promise) release (promise)
        jobs (atom [])
        unobserve (phase/observe!
                    (fn [event context]
                      (when (and (= :batch-sealed event)
                                 (identical? collector (batch/batch-collector context))
                                 (not (realized? entered)))
                        (deliver entered true)
                        (assert (deref release 10000 false)))))]
    (try
      (doseq [[idx tx] (map-indexed vector txs)]
        (swap! jobs conj (future (try (d/transact! conn tx {:request idx})
                                     (catch Throwable t t))))
        (when (zero? (long idx)) (is (deref entered 10000 false)))
        (is (loop [attempt 0]
              (cond
                (= (dec (count @jobs)) (.size queue)) true
                (= 1000 attempt) false
                :else (do (Thread/sleep 5) (recur (inc attempt)))))))
      (deliver release true)
      (mapv (fn [job]
              (let [result (deref job 10000 ::timeout)]
                (is (not= ::timeout result))
                (if (instance? Throwable result)
                  {:report nil :error result}
                  {:report result :error nil})))
            @jobs)
      (finally (deliver release true) (unobserve)))))

(deftest general-queued-batches-use-stampers-in-request-order
  (with-conn [conn [schema {:wal? true :wal-durability-profile :strict}]]
    (let [paths (atom [])
          before (:last-committed-lsn (d/txlog-watermarks (d/datalog-kv conn)))
          results
          (binding [conn/*local-wal-tx-path-observer* #(swap! paths conj %)]
            (queued-batch!
              conn [[{:item/key "new" (fields 0) 0}]
                    [[:db/add [:item/key "new"] (fields 0) 1]]
                    [[:db.fn/call (fn [tx-db]
                                   (is (= 1 (get (d/entity tx-db [:item/key "new"])
                                                 (fields 0))))
                                   [[:db/add [:item/key "new"] (fields 0) 2]])]]
                    [{:item/key "new" (fields 0) 3}]
                    [[:db/add [:item/key "new"] (fields 0) 4]]]))]
      (is (every? nil? (map :error results)))
      (is (= [:blind-insert :scalar-update :general :identity-upsert
              :scalar-update] @paths))
      (is (= (inc (long before))
             (:last-committed-lsn (d/txlog-watermarks (d/datalog-kv conn)))))
      (is (= [nil 0 1 2 3]
             (mapv (fn [{:keys [report]}]
                     (some #(when-not (datom/datom-added %) (:v %)) (:tx-data report)))
                   results)))
      (doseq [{:keys [report]} results]
        (is (= 4 (get (d/entity (:db-after report) [:item/key "new"]) (fields 0))))
        (is (not (l/writing? (d/datalog-kv (:db-before report)))))))
    (testing "a failed group rolls back without retrying bodies"
      (let [before (:last-committed-lsn (d/txlog-watermarks (d/datalog-kv conn)))
            results (queued-batch! conn
                                   [[[:db/add [:item/key "new"] (fields 0) 5]]
                                    [{:item/key "new" (fields 0) 6}]
                                    [[:db.fn/call (fn [_] (throw (ex-info "rollback" {})))]]])]
        (is (every? :error results))
        (is (apply identical? (map :error (take 2 results))))
        (is (= 4 (get (d/entity @conn [:item/key "new"]) (fields 0))))
        (is (= (long before)
               (:last-committed-lsn (d/txlog-watermarks (d/datalog-kv conn)))))))))

(deftest collected-scalars-fall-back-after-preceding-schema-change
  (with-conn [conn [schema {:wal? true :wal-durability-profile :strict}]]
    (d/transact! conn [{:db/id 1 :item/key "one" (fields 0) 0}])
    (let [prepared (db/prepare-scalar-update-tx
                     @conn [[:db/add 1 (fields 0) 1]]
                     {:defer-entity-resolution? true})
          paths (atom [])
          results (binding [conn/*local-wal-tx-path-observer* #(swap! paths conj %)]
                    (queued-batch! conn
                                   [[{:db/id 1 :item/new-field 10}]
                                    [[:db/add 1 (fields 0) 1]]
                                    [[:db/add [:item/key "one"] (fields 0) 2]]]))]
      (is (some? prepared))
      (is (not (db/scalar-update-tx-valid? @conn prepared)))
      (is (every? nil? (map :error results)))
      (is (= [:general :scalar-update :scalar-update] @paths))
      (is (= [0 1]
             (mapv (fn [{:keys [report]}]
                     (some #(when-not (datom/datom-added %) (:v %)) (:tx-data report)))
                   (rest results))))
      (is (= 2 (get (d/entity @conn 1) (fields 0))))
      (doseq [{:keys [report]} results]
        (is (= 2 (get (d/entity (:db-after report) 1) (fields 0))))
        (is (not (l/writing? (d/datalog-kv (:db-after report)))))))))
