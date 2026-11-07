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
            [datalevin.tx-group.batch.embedded :as embedded]
            [datalevin.tx-group.compat :as group]
            [datalevin.tx-group.phase :as phase]
            [datalevin.util :as u])
  (:import [org.eclipse.collections.impl.map.mutable.primitive LongObjectHashMap]))

(use-fixtures :each db-fixture)

(def ^:private schema
  {:stock/value {:db/valueType :db.type/long :db/noindex true}
   :stock/flag {:db/valueType :db.type/boolean}
   :stock/ytd {:db/valueType :db.type/long :db/noindex true}
   :stock/count {:db/valueType :db.type/long :db/noindex true}
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
      (d/transact! conn [{:db/id 1 :stock/value 5 :stock/flag false
                         :stock/ytd 10 :stock/count 20}
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
                             eid (gen/elements [1 2])
                             flag gen/boolean
                             size (gen/choose 1 12)
                             first-map? gen/boolean]
                (let [insert {:db/id "parent" :line/parent 1 :line/number 30
                              :line/alias [1 30] :line/name 42}
                      updates [[:db/add eid :stock/value value]
                               [:db/add eid :stock/flag flag]
                               [:db/add eid :stock/ytd (inc value)]
                               [:db/add eid :stock/count (+ value 2)]]
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

(deftest blind-preparation-rejects-mixed-shapes-before-correcting-values
  (with-store false
    (fn [conn]
      (let [corrections (atom 0)
            value (proxy [Object] []
                    (toString [] (swap! corrections inc) "corrected"))
            insert {:db/id "line" :line/name value}
            update [:db/add 1 :stock/value 6]
            update-map {:db/id 1 :stock/value 6}]
        (doseq [txs [[insert update] [update insert]
                     (list insert update) (map identity [insert update])
                     [insert update-map] [update-map insert]
                     (list insert update-map) (map identity [insert update-map])]]
          (is (nil? (db/prepare-blind-local-tx @conn txs true)))
          (is (zero? @corrections)))
        (testing "explicit-ID batches decline before accessing the store"
          (let [store (reify i/IStore
                        (schema [_]
                          (throw (ex-info "Unexpected blind preparation" {}))))
                database (db/transfer @conn store)
                updates (mapv #(hash-map :db/id % :stock/value 6) (range 1 257))]
            (doseq [collect-unique-avs? [false true]]
              (is (nil? (db/prepare-blind-local-tx
                          database updates true collect-unique-avs?))))))
        (is (some? (db/prepare-blind-local-tx @conn [insert] true)))
        (is (pos? @corrections))))))

(deftest top-level-mixed-adds-use-the-owned-writer
  (doseq [wal? [false true] prepare? [false true] map-first? [false true]
          grouped? [false true]]
    (testing (str "WAL " wal? ", preparation " prepare? ", map first " map-first?
                  ", grouped " grouped?)
      (binding [embedded/*defer-attachment?* (not grouped?)]
        (with-store wal?
          (fn [conn]
            (binding [c/*use-prepare-path* prepare? group/*enabled?* grouped?]
              (let [before @conn
                    indexes [(vec (:eavt before)) (vec (:avet before))]
                    paths (atom [])
                    notifications (atom [])
                    insert {:db/id "line" :line/parent 1 :line/number 7}
                    updates [[:db/add 1 :stock/value 6]
                             [:db/add 1 :stock/flag true]]
                    txs (if map-first? (into [insert] updates) (conj updates insert))]
                (d/listen! conn :mixed #(swap! notifications conj %))
                (let [report (binding [conn/*local-wal-tx-path-observer* #(swap! paths conj %)]
                               (d/transact! conn txs {:source :top-level}))
                      eid (get (:tempids report) "line")
                      saved (datoms report)]
                  (is (= [:mixed-add] @paths))
                  (is (= (select-keys before [:max-eid :max-tx])
                         (select-keys (:db-before report) [:max-eid :max-tx])))
                  (is (identical? @conn (:db-after report)))
                  (is (= {:source :top-level} (:tx-meta report)))
                  (is (= [report] @notifications))
                  (is (= eid (d/entid @conn [:line/key [1 7]])))
                  (is (= 6 (:stock/value (d/entity @conn 1))))
                  (is (true? (:stock/flag (d/entity @conn 1))))
                  (is (= indexes [(vec (:eavt before)) (vec (:avet before))]))
                  (d/transact! conn [[:db/add 1 :stock/value 7]])
                  (is (= saved (datoms report))))
                (let [before (rows @conn)]
                  (is (instance? Exception
                        (failure-of #(d/transact! conn
                                       [[:db/add 1 :stock/value 99]
                                        {:db/id "conflict" :line/parent 1 :line/number 7}]))))
                  (is (= before (rows @conn))))))))))))

(deftest top-level-mixed-adds-refresh-a-stale-connection-under-the-writer
  (doseq [wal? [false true]]
    (let [dir (u/tmp-dir (str "mixed-stale-" (random-uuid)))
          opts {:wal? wal? :snapshot-scheduler? false :background-sampling? false}]
      (try
        (binding [embedded/*defer-attachment?* true group/*enabled?* false]
          (with-open [first-conn (d/create-conn dir schema opts)
                      stale (d/create-conn dir schema opts)]
            (d/transact! first-conn [{:db/id 1 :stock/value 5}
                                    {:db/id 100 :line/name "peer"}])
            (d/transact! first-conn [[:db/add 1 :stock/value 6]])
            (is (< (:max-eid @stale) 100))
            (let [tx (:max-tx @first-conn)
                  report (d/transact! stale
                           [{:db/id "new" :line/name "new"}
                            [:db/add 1 :stock/value 7]])]
              (is (= 101 (get (:tempids report) "new")))
              (is (= (inc tx) (get (:tempids report) :db/current-tx)))
              (is (= [6 7] (mapv :v (filter #(= :stock/value (:a %))
                                             (:tx-data report)))))
              (is (= "peer" (:line/name (d/entity @stale 100))))
              (is (= "new" (:line/name (d/entity @stale 101)))))))
        (finally (u/delete-files dir))))))

(deftest direct-mixed-fallback-preserves-pending-values-and-new-attributes
  (doseq [wal? [false true]]
    (binding [embedded/*defer-attachment?* true group/*enabled?* false]
      (with-store wal?
        (fn [conn]
          (let [report (d/transact! conn
                         [[:db/add 1 :stock/value 6]
                          {:db/id "new" :identity "fresh" :line/name "before" :new/flag true}
                          [:db/add 1 :stock/value 7]
                          [:db.fn/call
                           (fn [database]
                             (let [eid (d/entid database [:identity "fresh"])]
                               (is (some? eid))
                               [[:db/cas 1 :stock/value 7 8]
                                [:db/add eid :line/name "after"]]))]])
                eid (get (:tempids report) "new")]
            (is (= "after" (:line/name (d/entity @conn eid))))
            (is (= 8 (:stock/value (d/entity @conn 1))))
            (is (true? (:new/flag (d/entity @conn eid))))
            (d/transact! conn [[:db/add eid :new/flag false]])
            (is (false? (:new/flag (d/entity @conn eid))))))))))

(deftest writer-old-values-survive-buffer-reuse-and-abort
  (with-store true
    (fn [conn]
      (d/update-schema conn {:stock/text {:db/valueType :db.type/string :db/noindex true}
                             :stock/bytes {:db/valueType :db.type/bytes :db/noindex true}})
      (let [text (apply str (repeat 2000 "value"))
            bytes (byte-array 2000 (byte 3))]
        (d/transact! conn [{:db/id 1 :stock/text text :stock/bytes bytes}])
        (d/with-transaction [tx conn]
          (let [store (:store @tx)
                saved (i/ea-first-v store 1 :stock/bytes)]
            (dotimes [_ 4]
              (is (= text (i/ea-first-v store 1 :stock/text)))
              (is (false? (i/ea-first-v store 1 :stock/flag)))
              (is (nil? (i/ea-first-v store 2 :stock/text)))
              (d/transact! tx [[:db/add 1 :stock/value 8]])
              (is (= 8 (i/ea-first-v store 1 :stock/value))))
            (is (= (vec bytes) (vec saved)))
            (d/abort-transact tx)))
        (is (= 5 (:stock/value (d/entity @conn 1))))))))

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
                     [[:db/add 1N :stock/value 6] insert update]
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

(deftest mixed-noncanonical-entity-ids-retain-ordered-resolution
  (doseq [wal? [false true]]
    (with-store wal?
      (fn [conn]
        (let [report (d/transact! conn
                                [[:db/add 1N :stock/value 6]
                                 {:db/id "line" :line/parent 1 :line/number 9}
                                 [:db/add 1 :stock/value 7]])]
          (is (= [7] (mapv :v (d/datoms @conn :eav 1 :stock/value))))
          (is (= [[5 false] [6 true] [6 false] [7 true]]
                 (mapv (juxt :v datom/datom-added)
                       (filter #(= :stock/value (:a %)) (:tx-data report))))))))))

(deftest mixed-old-value-reads-merge-native-and-pending-values
  (doseq [mode [:scalar :sorted]]
    (testing (name mode)
      (with-store true
        (fn [conn]
          (d/transact! conn [[:db/add 1 :stock/flag true]])
          (d/with-transaction [tx conn]
            (let [view (db/transfer @tx (:store @tx))
                  pending (LongObjectHashMap.)
                  context (if (= mode :scalar)
                            (reify txcommon/ScalarPendingIndex
                              (pending-scalar-index [_] pending)
                              (pending-unique-index [_] nil)
                              (retain-pending-index? [_] false)
                              (prepare-pending-index! [_ _])
                              (materialize-scalar-pending! [_ _])
                              (discard-scalar-pending! [_]))
                            {:test true})]
              (binding [txcommon/*batch-prepare* context]
                (doseq [row [(datom/datom 1 :stock/value 5 100 false)
                              (datom/datom 1 :stock/value 8 100 true)
                              (datom/datom 1 :stock/flag true 100 false)
                              (datom/datom 1 :stock/flag false 100 true)
                              (datom/datom 1 :stock/ytd 10 100 false)]]
                  (txcommon/stage-batch-datom! view row))
                (let [eav (vec (:eavt view)) ave (vec (:avet view))
                      report (db/prepare-mixed-add-tx
                               view
                               [[:db/add 1 :stock/value 9]
                                [:db/add 1 :stock/flag true]
                                [:db/add 1 :stock/ytd 11]
                                [:db/add 1 :stock/count 21]
                                {:db/id "line" :line/parent 1 :line/number 9}] nil)]
                  (is (some? report))
                  (is (= [[:stock/value 8 false] [:stock/value 9 true]
                          [:stock/flag false false] [:stock/flag true true]
                          [:stock/ytd 11 true]
                          [:stock/count 20 false] [:stock/count 21 true]]
                         (mapv (juxt :a :v datom/datom-added)
                               (filter #(= 1 (:e %)) (:tx-data report)))))
                  (is (= eav (vec (:eavt view))))
                  (is (= ave (vec (:avet view)))))))))))))

(deftest mixed-old-value-reads-own-giant-values
  (with-store true
    (fn [conn]
      (d/update-schema conn {:stock/text {:db/valueType :db.type/string}
                             :stock/bytes {:db/valueType :db.type/bytes}})
      (let [text (apply str (repeat 2000 "value"))
            bytes (byte-array 2000 (byte 3))]
        (d/transact! conn [{:db/id 1 :stock/text text :stock/bytes bytes}])
        (d/with-transaction [tx conn]
          (let [txs [[:db/add 1 :stock/text "new"]
                     [:db/add 1 :stock/bytes (byte-array [1 2])]
                     {:db/id "line" :line/parent 1 :line/number 9}]
                report (db/prepare-mixed-add-tx @tx txs nil)
                retractions (filter (comp not datom/datom-added) (:tx-data report))
                old (into {} (map (juxt :a :v)) retractions)]
            (dotimes [_ 4] (db/prepare-mixed-add-tx @tx txs nil))
            (is (= text (:stock/text old)))
            (is (= (vec bytes) (vec (:stock/bytes old))))
            (d/abort-transact tx)))
        (is (= text (:stock/text (d/entity @conn 1))))))))

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
                [#(d/transact! conn [[:db/add 1 :stock/value 6]
                                      [:db/add 1 :stock/ytd 11]])
                 #(d/transact! conn [[:db/add 1 :stock/value 7]
                                    [:db/add 1 :stock/ytd 12]
                                    [:db/add 1 :stock/count 21]
                                    [:db/add 1 :stock/flag true]
                                    {:db/id "line" :line/parent 1 :line/number 7}])
                 #(d/transact! conn [[:db/add [:line/key [1 7]] :line/name "pending"]
                                    [:db/retract 1 :stock/count 21]])
                 #(d/transact! conn [[:db/cas 1 :stock/value 7 8]])
                 #(d/transact! conn [[:db/add 1 :stock/value 9]
                                    [:db/add 1 :stock/ytd 13]
                                    [:db/add 1 :stock/count 22]
                                    [:db/add 1 :stock/flag false]
                                    {:db/id "line" :line/parent 1 :line/number 8}])]))]
        (is (every? map? reports) (pr-str reports))
        (is (= [true false] @modes))
        ;; A scalar-pending update wins over native state; a sorted-index
        ;; tombstone must prevent the following mixed request retracting it again.
        (is (= [[11 false] [12 true]]
               (mapv (juxt :v datom/datom-added)
                     (filter #(= :stock/ytd (:a %)) (:tx-data (reports 1))))))
        (is (= [[22 true]]
               (mapv (juxt :v datom/datom-added)
                     (filter #(= :stock/count (:a %)) (:tx-data (reports 4))))))
        (is (false? (:stock/flag (d/entity @conn 1))))
        (is (= [[[5 false] [6 true]] [[6 false] [7 true]]
                [[7 false] [8 true]] [[8 false] [9 true]]]
               (mapv (fn [r] (mapv (juxt :v datom/datom-added)
                                    (filter #(= :stock/value (:a %)) (:tx-data r))))
                     [(reports 0) (reports 1) (reports 3) (reports 4)])))
        (is (= "pending" (:line/name (d/entity @conn [:line/key [1 7]]))))
        (is (= 9 (:stock/value (d/entity @conn 1))))
        (is (not= (d/entid @conn [:line/key [1 7]])
                  (d/entid @conn [:line/key [1 8]])))))))
