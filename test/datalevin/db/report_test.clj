(ns datalevin.db.report-test
  (:require [clojure.test :refer [deftest is]]
            [datalevin.datom :as datom]
            [datalevin.db :as db]
            [datalevin.db.tx.execute :as execute]
            [datalevin.interface :as i]
            [datalevin.prepare :as prepare]))

(deftest compiled-reports-retain-executor-extensions-and-caller-context
  (let [before {:view :caller}
        working {:view :executor}
        after {:view :result}
        tx-meta {:source :caller}
        tx-data [(datom/datom 1 :value 7 91)]
        tempids {:db/current-tx 91}
        initial (assoc (db/->TxReport before working [] {} tx-meta)
                       ::caller-detail :retained ::shared-detail :initial)
        added {:new-attributes [:value] ::executor-detail {:future :field}
               ::nil-detail nil ::false-detail false ::shared-detail :executor}
        executed (merge (db/->TxReport working after tx-data tempids {:source :executor}) added)
        report (#'db/compiled-report initial executed :compiled-scalar false nil)]
    (is (instance? datalevin.db.TxReport report))
    (is (identical? before (:db-before report)))
    (is (identical? tx-meta (:tx-meta report)))
    (is (identical? after (:db-after report)))
    (is (identical? tx-data (:tx-data report)))
    (is (identical? tempids (:tempids report)))
    (is (= :retained (::caller-detail report)))
    (is (= added (select-keys report (keys added))))))

(deftest compiled-reports-clear-attempt-state-and-replace-routing-evidence
  (let [schema {:value {:db/valueType :db.type/long}}
        ctx (prepare/->PrepareCtx schema {} {})
        database {:store (reify i/IStore (schema [_] schema))}
        initial (assoc (db/->TxReport database database [] {} nil)
                       ::execute/max-eid-before 10
                       ::db/compiled-route :old-initial-route
                       ::db/independent-scalar? :old-initial-proof)
        tx-data [(datom/datom 1 :value 7 91)]
        executed (assoc initial :tx-data tx-data :tempids {:db/current-tx 91}
                        ::execute/max-eid-before 20
                        ::db/compiled-route :old-executor-route
                        ::db/independent-scalar? :old-executor-proof)]
    (doseq [independent? [false true]]
      (let [report (#'db/compiled-report initial executed :compiled-scalar independent? ctx)]
        (is (not (contains? report ::execute/max-eid-before)))
        (is (= :compiled-scalar (db/compiled-request-route report)))
        (is (= independent? (contains? report ::db/independent-scalar?)))
        (is (= independent? (boolean (db/request-storage-independent? report))))
        (when independent?
          (is (identical? schema (first (::db/independent-scalar? report))))
          (is (identical? tx-data (second (::db/independent-scalar? report)))))
        (is (= (dissoc report ::db/compiled-route ::db/independent-scalar?)
               (db/clear-compiled-request-route report)))))
    (is (nil? (#'db/compiled-report initial nil :compiled-scalar true ctx)))))
