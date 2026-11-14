(ns datalevin.tx-compiler-route-test
  (:require [clojure.java.io :as io]
            [clojure.test :refer [deftest is use-fixtures]]
            [datalevin.core :as d]
            [datalevin.datom :as datom]
            [datalevin.db :as db]
            [datalevin.db.tx.compile :as compile]
            [datalevin.test.core :refer [db-fixture]]))

(use-fixtures :each db-fixture)

(deftest compiler-is-the-only-production-request-planner
  (is (nil? (io/resource "datalevin/db/tx/engine.clj")))
  (doseq [wal? [false true]]
    (with-open [cn (d/create-conn nil {:value {:db/valueType :db.type/long}}
                    {:wal? wal? :background-sampling? false :snapshot-scheduler? false})]
      (d/transact! cn [{:db/id 1 :value 0}])
      (let [events (atom [])
            report (binding [db/*tx-compiler-observer* (fn [event _] (swap! events conj event))]
                     (d/transact! cn [[:db/add 1 :value 7]]))]
        (is (= 7 (:value (d/entity @cn 1))))
        (is (= [0 7] (mapv :v (:tx-data report))))
        (is (some #{:execute-scalar} @events)))
      (let [events (atom []) calls (atom 0)]
        (binding [db/*tx-compiler-observer* (fn [event _] (swap! events conj event))]
          (d/transact! cn [[:db.fn/call (fn [_] (swap! calls inc)
                                        [[:db/cas 1 :value 7 9]])]]))
        (is (= 1 @calls))
        (is (= 9 (:value (d/entity @cn 1))))
        (is (some #{:execute-dynamic} @events))))))

(deftest caller-bound-operands-enter-the-owned-compiler-hook
  (with-open [cn (d/create-conn nil {:value {:db/valueType :db.type/double}}
                  {:validate-data? true :background-sampling? false :snapshot-scheduler? false})]
    (d/transact! cn [{:db/id 1 :value 0.0}])
    (d/with-transaction [tx cn]
      (let [forms [[:db/add 1 :value 7]]
            plan (db/prepare-admitted-request @tx forms {:strategy :scalar})
            events (atom [])
            report (binding [db/*tx-compiler-observer* (fn [event _] (swap! events conj event))]
                     (db/prepare-local-tx-data @tx forms nil 0 plan))]
        (is (:datalevin.db/bound-operands? plan))
        (is (= [0.0 7.0] (mapv :v (:tx-data report))))
        (is (= [:execute-scalar] @events))))))

(deftest caller-bound-blind-operands-retain-correction-tempids-and-report-metadata
  (doseq [wal? [false true]]
    (with-open [cn (d/create-conn nil {:value {:db/valueType :db.type/double}
                                     :name {:db/valueType :db.type/string}}
                    {:wal? wal? :validate-data? false
                     :background-sampling? false :snapshot-scheduler? false})]
      (d/with-transaction [tx cn]
        (let [corrections (atom 0)
              value (proxy [Object] [] (toString [] (swap! corrections inc) "bound"))
              forms (list {:db/id "new" :value 7 :name value})
              plan (db/prepare-admitted-request @tx forms {:strategy :blind})
              events (atom [])
              metadata {:source :bound-blind}
              report (binding [db/*tx-compiler-observer* #(swap! events conj [%1 %2])]
                       (db/prepare-local-tx-data @tx forms metadata 0 plan))
              eid (get (:tempids report) "new")]
          (is (true? (::db/bound-operands? plan)))
          (is (= 1 @corrections))
          (is (= [:execute-blind] (mapv first @events)))
          (is (identical? plan (second (first @events))))
          (is (= {:value 7.0 :name "bound"}
                 (into {} (map (juxt :a :v)) (:tx-data report))))
          (is (every? datom/datom-added (:tx-data report)))
          (is (pos-int? eid))
          (is (= #{eid} (set (map :e (:tx-data report)))))
          (is (= eid (:max-eid (:db-after report))))
          (is (identical? @tx (:db-before report)))
          (is (= metadata (:tx-meta report))))))))

(deftest caller-bound-patches-use-the-patch-route-and-match-dynamic-execution
  (doseq [wal? [false true]
          [operations values] [[[] []] [[[:set [:n] 1]] []]
                               [[[:set [:n] 2]] [{:n 1} {:n 2}]]]]
    (with-open [cn (d/create-conn nil {:doc {:db/valueType :db.type/idoc}}
                    {:wal? wal? :background-sampling? false :snapshot-scheduler? false})]
      (d/transact! cn [{:db/id 1 :doc {:n 1}}])
      (d/with-transaction [tx cn]
        (let [demands (atom 0)
              forms [[:db.fn/patchIdoc 1 :doc (lazy-seq (swap! demands inc) (seq operations))]]
              plan (db/prepare-admitted-request @tx forms {:strategy :patch})
              events (atom [])
              dynamic-events (atom [])
              metadata {:source :bound-patch}
              options {:stage? false :route? true :owned-max-eid (:max-eid @tx)}
              _ (is (zero? @demands))
              report (binding [db/*tx-compiler-observer* #(swap! events conj [%1 %2])]
                       (db/prepare-local-tx-data @tx forms metadata 0 plan nil options))
              dynamic-report
              (binding [db/*tx-compiler-observer* (fn [event _] (swap! dynamic-events conj event))]
                (db/prepare-local-tx-data @tx forms metadata 0
                  (compile/compile-program (:ctx plan) forms) nil options))
              datoms #(mapv (juxt :e :a :v datom/datom-tx datom/datom-added meta)
                            (:tx-data %))]
          (is (true? (::db/bound-operands? plan)))
          (is (= 1 @demands))
          (is (= [:execute-patch] (mapv first @events)))
          (is (identical? plan (second (first @events))))
          (is (= :patch-idoc (db/compiled-request-route report)))
          (is (= [:execute-dynamic] @dynamic-events))
          (is (= :compiled-dynamic (db/compiled-request-route dynamic-report)))
          (is (= (datoms dynamic-report) (datoms report)))
          (is (= (select-keys dynamic-report [:tempids :tx-meta])
                 (select-keys report [:tempids :tx-meta])))
          (is (= (select-keys (:db-after dynamic-report) [:max-eid :max-tx])
                 (select-keys (:db-after report) [:max-eid :max-tx])))
          (is (= values (mapv :v (:tx-data report))))
          (when (seq values)
            (is (= [false true] (mapv datom/datom-added (:tx-data report))))
            (is (= {:idoc/patch {:paths [[:n]]}} (meta (last (:tx-data report))))))
          (is (identical? @tx (:db-before report)))
          (is (= metadata (:tx-meta report)))
          (is (= (inc (:max-tx @tx)) (:max-tx (:db-after report))))
          (is (= {:n 1} (:doc (d/entity @tx 1)))))))))

(deftest normalized-patch-operands-drive-owned-execution
  (with-open [cn (d/create-conn nil {:doc {:db/valueType :db.type/idoc}
                                   :other-doc {:db/valueType :db.type/idoc}}
                  {:background-sampling? false :snapshot-scheduler? false})]
    (d/transact! cn [{:db/id 1 :doc {:n 1}} {:db/id 2 :other-doc {:n 10}}])
    (d/with-transaction [tx cn]
      (let [source [:db.fn/patchIdoc 1 :doc [[:set [:n] 2]]]
            forms [source]
            plan (db/prepare-admitted-request @tx forms {:strategy :patch})
            rewritten (assoc plan :entries [[2 :other-doc [[:set [:n] 3]] source]])
            report (db/prepare-local-tx-data @tx forms nil 0 rewritten)]
        (is (= [[2 :other-doc {:n 10} false] [2 :other-doc {:n 3} true]]
               (mapv (juxt :e :a :v datom/datom-added) (:tx-data report))))
        (is (identical? source (nth (first (:entries rewritten)) 3)))
        (is (= {:idoc/patch {:paths [[:n]]}} (meta (last (:tx-data report)))))
        (is (= {:n 1} (:doc (d/entity @tx 1))))))))

(deftest bound-plan-routing-rejects-unsupported-strategies-and-mismatched-provenance
  (with-open [cn (d/create-conn nil {:value {:db/valueType :db.type/long}}
                  {:background-sampling? false :snapshot-scheduler? false})]
    (d/transact! cn [{:db/id 1 :value 0}])
    (let [forms [[:db/add 1 :value 7]]
          plan (db/prepare-admitted-request @cn forms {:strategy :scalar})
          initial (db/->TxReport @cn @cn [] {} nil)
          events (atom [])]
      (binding [db/*tx-compiler-observer* (fn [event _] (swap! events conj event))]
        (doseq [strategy [:scalar-operations :scalar-aliases :blind-insert :mixed :ordered :dynamic]]
          (is (thrown-with-msg? IllegalArgumentException #"Unsupported caller-bound transaction strategy"
                (db/prepare-local-tx-data @cn forms nil 0 (assoc plan :strategy strategy))))))
      (is (empty? @events))
      (is (thrown-with-msg? AssertionError #"Expected caller-bound transaction operands"
            (#'db/execute-bound-request initial (dissoc plan ::db/bound-operands?) {})))
      (is (thrown-with-msg? AssertionError #"Caller-bound plan cannot enter the compiler route"
            (#'db/execute-compiled-request initial plan {})))
      (is (= 0 (:value (d/entity @cn 1)))))))

(deftest compiler-requests-preserve-resolver-overlays-on-success-and-failure
  (doseq [wal? [false true]]
    (with-open [cn (d/create-conn nil {:value {:db/valueType :db.type/long}
                                     :token {:db/valueType :db.type/string :db/unique :db.unique/value}}
                    {:wal? wal? :background-sampling? false :snapshot-scheduler? false})]
      (d/transact! cn [{:db/id 1 :value 0 :token "taken"}])
      (d/with-transaction [tx cn]
        (d/transact! tx [[:db.fn/call (fn [_] [[:db/add 1 :value 1]])]])
        (let [before @tx indexes (vec (:eavt before))]
          (d/transact! tx [[:db/add 1 :value 2] [:db/add 1 :value 3]])
          (is (= indexes (vec (:eavt before))))
          (is (= 3 (:value (d/entity @tx 1)))))
        (is (thrown? Exception (d/transact! tx [{:token "free"} {:token "taken"}])))
        (is (nil? (d/entid @tx [:token "free"])))
        (d/abort-transact tx))
      (is (= 0 (:value (d/entity @cn 1)))))))
