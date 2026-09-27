(ns datalevin.db.wal-prepare-test
  (:require
   [clojure.test :refer [deftest is use-fixtures]]
   [datalevin.conn :as conn]
   [datalevin.constants :as c]
   [datalevin.core :as d]
   [datalevin.datom :as datom]
   [datalevin.db :as db]
   [datalevin.test.core :refer [db-fixture]]
   [datalevin.util :as u]))

(def ^:dynamic *conn* nil)

(use-fixtures :each db-fixture
  (fn [f]
    (let [path (u/tmp-dir (str "wal-prepare-" (random-uuid)))
          conn (d/get-conn path
                          {:key {:db/valueType :db.type/long
                                 :db/unique :db.unique/identity}
                           :value {:db/valueType :db.type/string}
                           :tags {:db/cardinality :db.cardinality/many
                                  :db/noindex true}}
                          {:wal? true})]
      (try
        (d/transact! conn [{:db/id 1 :key 1 :value "before" :tags #{"old" "kept"}}])
        (binding [*conn* conn] (f))
        (finally (d/close conn) (u/delete-files path))))))

(defn- initial-report [database]
  (db/->TxReport database (db/transfer database (:store database)) [] {} {:test true}))

(defn- datoms [report]
  (mapv (juxt :e :a :v :tx datom/datom-added) (:tx-data report)))

(deftest internal-preparation-skips-only-the-final-simulated-overlay
  (doseq [prepare? [false true]]
    (binding [c/*use-prepare-path* prepare?]
      (let [before @*conn*
            before-eav (vec (:eavt before))
            before-ave (vec (:avet before))
            token (db/cache-token (:store before))
            txs [[:db/add 1 :value "after"]
                 [:db/retract 1 :tags "old"]
                 {:db/id "new" :key 2 :value "added" :extra "new-attribute"}]
            prepared (db/prepare-local-tx-data (initial-report before) txs)
            simulated (d/tx-data->simulated-report before txs)]
        (is (= (datoms simulated) (datoms prepared)))
        (is (= (:tempids simulated) (:tempids prepared)))
        (is (= (:new-attributes simulated) (:new-attributes prepared) [:extra]))
        (is (= {:test true} (:tx-meta prepared)))
        (is (= (select-keys (:db-after simulated) [:max-eid :max-tx])
               (select-keys (:db-after prepared) [:max-eid :max-tx])))
        (is (seq (filter #(not (datom/datom-added %)) (:tx-data prepared))))
        (is (not-any? #(not (datom/datom-added %)) (:eavt (:db-after prepared))))
        (is (some #(not (datom/datom-added %)) (:eavt (:db-after simulated))))
        (is (= #{["after"] ["added"]}
               (d/q '[:find ?v :where [_ :value ?v]] (:db-after simulated))))
        (is (= #{["kept"]}
               (d/q '[:find ?v :where [1 :tags ?v]] (:db-after simulated))))
        (is (= "before" (:value (d/entity @*conn* 1))))
        (is (nil? (d/entid @*conn* [:key 2])))
        (is (= token (db/cache-token (:store before))))
        (is (= before-eav (vec (:eavt before))))
        (is (= before-ave (vec (:avet before))))))))

(deftest wal-preparation-keeps-commit-time-ensures-and-rollback
  (let [paths (atom [])
        checked (atom [])
        ensure-new (fn [database eid]
                     (let [values [(:value (d/entity database 1))
                                   (:value (d/entity database eid))]]
                       (swap! checked conj values)
                       (= ["after" "added"] values)))
        report (binding [conn/*local-wal-tx-path-observer* #(swap! paths conj %)]
                 (d/transact! *conn*
                              [[:db/add 1 :value "after"]
                               [:db/retract 1 :tags "old"]
                               {:db/id "new" :key 2 :value "added"}
                               [:db/ensure ensure-new "new"]]
                              {:test true}))]
    (is (= [:general] @paths))
    (is (= [["after" "added"]] @checked))
    (is (= {:test true} (:tx-meta report)))
    (is (identical? @*conn* (:db-after report)))
    (is (= "added" (:value (d/entity (:db-after report) (get (:tempids report) "new")))))
    (is (= #{"kept"} (:tags (d/entity @*conn* 1))))
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #":db/ensure failed"
          (d/transact! *conn*
                       [[:db/add 1 :value "rejected"]
                        [:db/ensure (fn [database eid]
                                      (is (= "rejected" (:value (d/entity database eid))))
                                      false) 1]])))
    (is (= "after" (:value (d/entity @*conn* 1))))))

(deftest wal-preparation-retains-in-flight-transaction-state
  (let [seen (atom [])
        paths (atom [])
        report (binding [conn/*local-wal-tx-path-observer* #(swap! paths conj %)]
                 (d/transact! *conn*
                              [[:db/add 1 :value "intermediate"]
                               {:db/id "new" :key 2 :value "added"}
                               [:db.fn/call
                                (fn [database]
                                  (swap! seen conj (d/entid database [:key 2]))
                                  [[:db/cas 1 :value "intermediate" "final"]])]]))]
    (is (= [:general] @paths))
    (is (= [(get (:tempids report) "new")] @seen))
    (is (= "final" (:value (d/entity @*conn* 1))))))
