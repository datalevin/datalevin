(ns datalevin.db.mixed-add-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [clojure.test.check :as tc]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [datalevin.conn :as conn]
            [datalevin.constants :as c]
            [datalevin.core :as d]
            [datalevin.datom :as datom]
            [datalevin.db :as db]
            [datalevin.db.tx.common :as txcommon]
            [datalevin.interface :as i]
            [datalevin.test.core :refer [db-fixture]]
            [datalevin.tx-group.batch :as batch]
            [datalevin.tx-group.phase :as phase]
            [datalevin.util :as u]))

(use-fixtures :each db-fixture)

(def ^:private schema
  {:stock/value {:db/valueType :db.type/long :db/noindex true}
   :stock/flag {:db/valueType :db.type/boolean}
   :line/parent {:db/valueType :db.type/ref}
   :line/number {:db/valueType :db.type/long :db/noindex true}
   :line/key {:db/tupleAttrs [:line/parent :line/number] :db/unique :db.unique/value}
   :line/alias {:db/tupleTypes [:db.type/ref :db.type/long]}
   :line/name {:db/valueType :db.type/string}
   :identity {:db/valueType :db.type/string :db/unique :db.unique/identity}
   :unique-bytes {:db/valueType :db.type/bytes :db/unique :db.unique/value}})

(defn- with-store [wal? f]
  (let [dir (u/tmp-dir (str "mixed-add-" (random-uuid)))
        conn (d/create-conn dir schema {:wal? wal? :wal-durability-profile :strict
                                        :snapshot-scheduler? false})]
    (try
      (d/transact! conn [{:db/id 1 :stock/value 5 :stock/flag false}
                        {:db/id 2 :identity "existing"}])
      (f conn)
      (finally (d/close conn) (u/delete-files dir)))))

(defn- datoms [report]
  (mapv (juxt :e :a :v :tx datom/datom-added) (:tx-data report)))

(defn- rows [db]
  (set (map (juxt :e :a :v) (d/datoms db :eav))))

(defn- failure-of [f]
  (try (f) nil (catch Exception e e)))

(deftest mixed-add-preparation-matches-the-interpreter
  (with-store true
    (fn [conn]
      (let [before @conn
            eav (vec (:eavt before)) ave (vec (:avet before))
            result
            (tc/quick-check 100
              (prop/for-all [value (gen/choose -10 10)
                             flag gen/boolean
                             size (gen/choose 1 12)
                             first-map? gen/boolean]
                (let [insert {:db/id "parent" :line/parent 1 :line/number 30
                              :line/alias [1 30] :line/name 42}
                      updates [[:db/add 1 :stock/value value]
                               [:db/add 1 :stock/flag flag]]
                      txs (into (if first-map? (into [insert] updates)
                                    (conj updates insert))
                                (map (fn [n] {:db/id (str "line-" n)
                                              :line/parent "parent"
                                              :line/number n}) (range size)))
                      fast (db/prepare-mixed-add-tx before txs {:test true})
                      slow (db/prepare-local-tx-data
                             (db/->TxReport before (db/transfer before (:store before))
                                            [] {} {:test true}) txs)]
                  (and fast (= (datoms fast) (datoms slow))
                       (= (:tempids fast) (:tempids slow))
                       (= (:tx-meta fast) (:tx-meta slow))
                       (= (select-keys (:db-after fast) [:max-tx :max-eid])
                          (select-keys (:db-after slow) [:max-tx :max-eid])))))
              :seed 42)]
        (is (:pass? result) (pr-str result))
        (is (= eav (vec (:eavt before))))
        (is (= ave (vec (:avet before))))))))

(deftest mixed-add-preserves-visibility-rollback-and-report-ownership
  (doseq [wal? [false true] prepare? [false true]]
    (testing (str "WAL " wal? ", preparation " prepare?)
      (with-store wal?
        (fn [conn]
          (binding [c/*use-prepare-path* prepare?]
            (let [paths (atom [])
                  report
                  (binding [conn/*local-wal-tx-path-observer* #(swap! paths conj %)]
                    (d/with-transaction [tx conn]
                      (let [r1 (d/transact! tx
                                 [[:db/add 1 :stock/value 6]
                                  [:db/add 1 :stock/flag false]
                                  {:db/id "line" :line/parent 1 :line/number 7}])
                            eid (get (:tempids r1) "line")
                            saved (datoms r1)]
                        (is (= eid (d/entid @tx [:line/key [1 7]])))
                        (let [r2 (d/transact! tx
                                   [[:db/add 1 :stock/value 7]
                                    {:db/id "line" :line/parent eid :line/number 8}])]
                          (is (some #{[1 :stock/value 6 (:max-tx (:db-after r2)) false]}
                                    (datoms r2)))
                          (is (= saved (datoms r1)))
                          r2))))]
              (is (= 7 (:stock/value (d/entity @conn 1))))
              (is (vector? (:tx-data report)))
              (is (= [:mixed-add :mixed-add] @paths)))
            (let [before (rows @conn)]
              (d/with-transaction [tx conn]
                (d/transact! tx [[:db/add 1 :stock/value 99]
                                {:db/id -1 :line/parent 1 :line/number 9}])
                (d/abort-transact tx))
              (is (= before (rows @conn)))
              (is (instance? Exception
                    (failure-of #(d/transact! conn
                                   [[:db/add 1 :stock/value 99]
                                    {:db/id -1 :line/parent 1 :line/number 7}]))))
              (is (= before (rows @conn))))))))))

(deftest mixed-add-declines-dependent-and-complex-inputs-without-staging
  (with-store true
    (fn [conn]
      (let [before @conn
            insert {:db/id "new" :line/parent 1 :line/number 7}
            update [:db/add 1 :stock/value 6]
            eav (vec (:eavt before)) ave (vec (:avet before))]
        (doseq [txs [[update insert update]
                     [update insert {:db/id "new" :line/number 8}]
                     [update insert [:db.fn/call (fn [_] [])]]
                     [update insert [:db/retract 1 :stock/value 6]]
                     [update (assoc insert :line/parent "later") {:db/id "later" :line/name "x"}]
                     [update (assoc insert :identity "existing")]
                     [update insert {:db/id -2 :line/parent 1 :line/number 7}]
                     [update (assoc insert :line/key [1 7])]
                     [update (assoc insert :unique-bytes (byte-array [1]))]
                     [update (assoc insert :db/id nil)]
                     [update nil insert]
                     [update {:db/id 2 :line/name "existing"}]]]
          (is (nil? (db/prepare-mixed-add-tx before txs nil)) (pr-str txs))
          (is (= eav (vec (:eavt before))))
          (is (= ave (vec (:avet before)))))
        ;; A late identity upsert retries the whole general transaction.
        (d/transact! conn [[:db/add 1 :stock/value 8]
                           {:db/id "alias" :line/parent 1 :line/number 7}
                           {:db/id "alias" :identity "existing"}])
        (is (= 2 (d/entid @conn [:line/key [1 7]])))
        (is (= 8 (:stock/value (d/entity @conn 1))))))))

(defn- collected! [conn ops]
  (let [collector (get-in @(i/kv-info (d/datalog-kv conn)) [:independent-control :collector])
        queue (.-ready ^datalevin.tx_group.batch.Collector collector)
        entered (promise) release (promise) jobs (atom [])
        unobserve (phase/observe!
                    (fn [event b]
                      (when (and (= :batch-sealed event)
                                 (identical? collector (batch/batch-collector b))
                                 (not (realized? entered)))
                        (deliver entered true)
                        (assert (deref release 10000 false)))))]
    (try
      (doseq [[idx op] (map-indexed vector ops)]
        (swap! jobs conj (future (try (op) (catch Throwable t t))))
        (when (zero? (long idx)) (is (deref entered 10000 false)))
        (is (loop [attempt 0]
              (cond (= (dec (count @jobs)) (.size ^java.util.Queue queue)) true
                    (= attempt 1000) false
                    :else (do (Thread/sleep 5) (recur (inc attempt)))))))
      (deliver release true)
      (mapv #(deref % 10000 ::timeout) @jobs)
      (finally (deliver release true)
               (doseq [job @jobs] (deref job 10000 nil))
               (unobserve)))))

(deftest collected-mixed-adds-compose-with-scalar-and-general-requests
  (with-store true
    (fn [conn]
      (let [modes (atom [])
            reports
            (binding [conn/*local-wal-tx-path-observer*
                      (fn [path]
                        (when (= :mixed-add path)
                          (swap! modes conj (some? (txcommon/scalar-pending-index)))))]
              (collected! conn
                [#(d/transact! conn [[:db/add 1 :stock/value 6]])
                 #(d/transact! conn [[:db/add 1 :stock/value 7]
                                    {:db/id "line" :line/parent 1 :line/number 7}])
                 #(d/transact! conn [[:db/add [:line/key [1 7]] :line/name "pending"]])
                 #(d/transact! conn [[:db/cas 1 :stock/value 7 8]])
                 #(d/transact! conn [[:db/add 1 :stock/value 9]
                                    {:db/id "line" :line/parent 1 :line/number 8}])]))]
        (is (every? map? reports) (pr-str reports))
        (is (= [true false] @modes))
        (is (= [[[5 false] [6 true]] [[6 false] [7 true]]
                [[7 false] [8 true]] [[8 false] [9 true]]]
               (mapv (fn [r] (mapv (juxt :v datom/datom-added)
                                    (filter #(= :stock/value (:a %)) (:tx-data r))))
                     [(reports 0) (reports 1) (reports 3) (reports 4)])))
        (is (= "pending" (:line/name (d/entity @conn [:line/key [1 7]]))))
        (is (= 9 (:stock/value (d/entity @conn 1))))
        (is (not= (d/entid @conn [:line/key [1 7]])
                  (d/entid @conn [:line/key [1 8]])))))))
