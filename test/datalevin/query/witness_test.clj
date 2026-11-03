(ns datalevin.query.witness-test
  (:require [clojure.test :refer [deftest is testing]]
            [datalevin.core :as d]
            [datalevin.query :as q]
            [datalevin.query.resolve.witness :as witness]))

(def schema
  {:candidate/id {:db/valueType :db.type/long}
   :candidate/order {:db/valueType :db.type/long}
   :candidate/supplier {:db/valueType :db.type/long}
   :candidate/payload {:db/valueType :db.type/string}
   :line/order {:db/valueType :db.type/long}
   :line/supplier {:db/valueType :db.type/long}
   :line/receipt {:db/valueType :db.type/long}
   :line/commit {:db/valueType :db.type/long}})

(def body
  '[[?w :line/order ?order]
    [?w :line/supplier ?other]
    [(not= ?other ?supplier)]
    [?w :line/receipt ?receipt]
    [?w :line/commit ?commit]
    [(> ?receipt ?commit)]])

(def prefix
  '[:find ?id ?payload
    :where [?c :candidate/id ?id]
    [?c :candidate/order ?order]
    [?c :candidate/supplier ?supplier]
    [?c :candidate/payload ?payload]])

(def anti-query (conj prefix (list* 'not-join '[?order ?supplier] body)))
(def semi-query (conj prefix (list 'not-join '[?order ?supplier] (last anti-query))))

(defn- fixture [schema f]
  (with-open [^java.lang.AutoCloseable conn (d/create-conn nil schema {:background-sampling? false})]
    (d/transact! conn
      (concat
        (for [^long order (range 1 5) ^long supplier [1 2] ^long copy [0 1]]
          {:candidate/id (+ (* order 10) (* supplier 2) copy)
           :candidate/order order :candidate/supplier supplier
           :candidate/payload (str "P" copy)})
        (for [^long order [1 2 3] ^long i (range 1024)]
          (cond-> {:line/order order :line/supplier (if (= order 2) 1 3)
                   :line/receipt (if (= order 3) 10 20)}
            ;; Missing commit values cannot establish a witness.
            (not (zero? (long (mod i 5)))) (assoc :line/commit 10)))))
    (binding [q/*cache?* false] (f conn))))

(deftest grouped-witnesses-preserve-correlation-and-outer-rows
  (fixture schema
    (fn [conn]
      (let [facts (mapv (fn [datom] [(:e datom) (:a datom) (:v datom)])
                       (d/datoms @conn :eav))]
        (doseq [query [anti-query semi-query]]
          (let [expected (set (d/q query facts))
                conventional (binding [witness/*enabled?* false] (set (d/q query @conn)))
                explanation (d/explain {:run? true} query @conn)
                plans (:existence-witness-plans explanation)
                result (set (:result explanation))]
            (is (= expected conventional result))
            (is (seq plans))
            (is (every? #(<= (long (:matched-keys %)) (long (:input-keys %))) plans))
            (is (every? #(<= (long (:max-buffered-witnesses %)) 256) plans))
            (is (< (long (reduce + (map :visited-witnesses plans))) 3072)
                "once every key for an anchor matches, stop visiting its witnesses")))
        (is (= 10 (count (d/q anti-query @conn))))
        (is (= 6 (count (d/q semi-query @conn))))))))

(deftest witness-many-valued-attributes-use-the-conventional-body
  (fixture (assoc-in schema [:line/receipt :db/cardinality] :db.cardinality/many)
    (fn [conn]
      (let [explanation (d/explain {:run? true} anti-query @conn)
            conventional (binding [witness/*enabled?* false] (d/q anti-query @conn))]
        (is (= (set conventional) (set (:result explanation))))
        (is (empty? (:existence-witness-plans explanation)))))))

(deftest witness-reads-respect-simulated-transactions
  (fixture schema
    (fn [conn]
      (let [pending (:db-after (d/with @conn [{:line/order 4 :line/supplier 3
                                               :line/receipt 20 :line/commit 10}]
                                      {} true))
            explanation (d/explain {:run? true} anti-query pending)]
        (testing "the added witness removes all four order-4 candidates"
          (is (= 6 (count (:result explanation))))
          (is (= (set (binding [witness/*enabled?* false] (d/q anti-query pending)))
                 (set (:result explanation))))
          (is (empty? (:existence-witness-plans explanation)))
          (is (= 10 (count (d/q anti-query @conn)))))))))

(deftest witness-body-in-write-transactions-uses-the-current-view
  (fixture schema
    (fn [conn]
      (d/with-transaction [tx conn]
        (d/transact! tx [{:line/order 4 :line/supplier 3
                          :line/receipt 20 :line/commit 10}])
        (let [explanation (d/explain {:run? true} anti-query @tx)]
          (is (= 6 (count (:result explanation))))
          (is (empty? (:existence-witness-plans explanation))))))))
