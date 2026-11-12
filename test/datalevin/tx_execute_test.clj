(ns datalevin.tx-execute-test
  (:require [clojure.test :refer [deftest is]]
            [datalevin.datom :as datom]
            [datalevin.db.tx.execute :as execute]
            [datalevin.interface :as i]
            [datalevin.prepare :as prepare]))

(deftest blind-preparation-normalizes-with-only-captured-context
  (let [calls (atom [])
        host (proxy [Object] [] (toString [] (swap! calls conj :correct) "normalized"))
        ctx (prepare/->PrepareCtx
              {:text {:db/aid 1 :db/valueType :db.type/string}
               :numbers {:db/aid 2 :db/valueType :db.type/long
                         :db/cardinality :db.cardinality/many}}
              {} {:validate-data? false})
        forms [(array-map :db/id -1 :text host :numbers [1.2 1.8 2.9])]
        kinds (execute/blind-entity-kinds forms nil)
        parts (execute/prepare-blind-operands ctx forms true true kinds)
        entity (first (:entries parts))]
    (is (= 1 kinds))
    (is (= -1 (.-tempid ^datalevin.db.tx.plan.BlindEntity entity)))
    (is (= [:text "normalized" :numbers 1 :numbers 2]
           (vec (.-attrs ^datalevin.db.tx.plan.BlindEntity entity))))
    (is (true? (:has-many? parts)))
    (is (false? (:has-unique? parts)))
    (is (not (contains? parts :ctx)))
    (is (= [:correct] @calls))
    (is (identical? host (:text (first forms))))
    (is (= [1.2 1.8 2.9] (:numbers (first forms))))))

(deftest blind-id-proof-precedes-normalization-and-retains-classified-input
  (let [input (lazy-seq (throw (AssertionError. "Demand after classified proof")))]
    (is (= 0 (execute/blind-entity-kinds input 0)))
    (is (= 1 (execute/blind-entity-kinds input 1))))
  (is (false? (execute/blind-entity-kinds [{:db/id nil :value 1}] nil)))
  (is (false? (execute/blind-entity-kinds [{:value 1} {:db/id 1 :value 2}] nil))))

(deftest scalar-preparation-owns-correction-and-accepts-associative-views
  (let [calls (atom [])
        host (proxy [Object] []
               (hashCode [] (swap! calls conj :hash) 0)
               (toString [] (swap! calls conj :correct) "normalized"))
        context (prepare/->PrepareCtx
                  {:text {:db/aid 1 :db/valueType :db.type/string}
                   :value {:db/aid 2 :db/valueType :db.type/long}}
                  {} {:validate-data? false})
        database {:max-eid 2 :store (reify i/IStore
                                     (ea-first-v [_ _ _]
                                       (throw (AssertionError. "Preparation read old values"))))}
        forms [{:db/id (int 1) :text host} [:db/add 2 :value 1.9]]
        entries (execute/prepare-scalar-operands database context forms false)]
    (is (= [[1 :text "normalized"] [2 :value 1]] entries))
    (is (every? #(instance? Long (first %)) entries))
    (is (= [:correct] @calls))
    (is (identical? host (:text (first forms))))
    (is (instance? Integer (:db/id (first forms))))
    (is (= 1.9 (nth (second forms) 3)))
    (is (nil? (execute/prepare-scalar-operands
                database (assoc-in context [:opts :auto-entity-time?] true)
                (lazy-seq (throw (AssertionError. "Input demand after context decline"))) false)))))

(deftest scalar-report-worker-preserves-the-adapter-attempt-and-both-layouts
  (doseq [offset [0 1] specialize? [false true]]
    (let [reads (atom [])
          store (reify i/IStore
                  (ea-first-v [_ e a] (swap! reads conj [e a]) (if (= e 1) 0 2)))
          database {:store store :max-eid 2 :max-tx 6}
          initial {:db-before database :db-after database :tx-data [] :tempids {}
                   :tx-meta {:source :scalar} :retained :adapter-owned}
          entries (if (zero? offset) [[1 :value 7] [2 :other 8]]
                      [[:db/add 1 :value 7] [:db/add 2 :other 8]])
          result (execute/execute-scalar-report initial entries nil 7 specialize? offset)]
      (is (identical? database (:db-before result)))
      (is (= {:source :scalar} (:tx-meta result)))
      (is (= :adapter-owned (:retained result)))
      (is (= {:db/current-tx 7} (:tempids result)))
      (is (= 7 (:max-tx (:db-after result))))
      (is (= 2 (:max-eid (:db-after result))))
      (is (= [[1 :value] [2 :other]] @reads))
      (is (= [[1 :value 0 7 false] [1 :value 7 7 true]
              [2 :other 2 7 false] [2 :other 8 7 true]]
             (mapv (juxt :e :a :v :tx datom/datom-added) (:tx-data result)))))))

(deftest scalar-report-worker-declines-prefixes-before-demand-or-reads
  (let [reads (atom [])
        database {:max-tx 6 :max-eid 2
                  :store (reify i/IStore (ea-first-v [_ _ _] (swap! reads conj :old) 0))}
        initial {:db-before database :db-after database :tx-data [] :tempids {}}
        entries (lazy-seq (throw (AssertionError. "Operand demand after prefix")))]
    (doseq [prefix [(assoc initial :tx-data [(datom/datom 1 :value 1 6)])
                    (assoc initial :tempids {"prefix" 2})]]
      (is (nil? (execute/execute-scalar-report prefix entries nil 7 true))))
    (is (empty? @reads))
    (is (= 6 (:max-tx database)))))
