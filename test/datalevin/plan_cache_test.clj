(ns datalevin.plan-cache-test
  (:require [clojure.test :refer [deftest is use-fixtures]]
            [datalevin.core :as d]
            [datalevin.db :as db]
            [datalevin.interface :as i]
            [datalevin.query :as q]
            [datalevin.query.optimizer.plan-build :as pb]
            [datalevin.query.plan :as plan]
            [datalevin.query.resolve :as resolve]
            [datalevin.test.core :refer [db-fixture]]
            [datalevin.udf :as udf]
            [datalevin.util :as u])
  (:import [datalevin.utl LRUCache]))

(use-fixtures :each db-fixture)

(def ^:private schema
  {:name {:db/valueType :db.type/string}
   :value {:db/valueType :db.type/long}})

(def ^:private query
  '[:find ?name ?value :where [?e :name ?name] [?e :value ?value]])

(defn- cached-entry [^LRUCache cache]
  (is (= 1 (count (.keys cache))))
  (.get cache (first (.keys cache))))

(deftest plans-survive-transaction-views
  (doseq [wal? [false true]]
    (let [dir (u/tmp-dir (str "plan-cache-tx-" (random-uuid)))
          conn (d/create-conn dir schema {:kv-opts {:wal? wal?}})
          cache (LRUCache. 32)]
      (try
        (d/transact! conn [{:db/id 1 :name "one" :value 1}])
        (binding [q/*cache?* false q/*plan-cache* cache]
          (is (= #{["one" 1]} (d/q query @conn)))
          (let [entry (cached-entry cache)]
            (d/transact! conn [[:db/add 1 :value 2]])
            (is (= #{["one" 2]} (d/q query @conn)))
            (is (identical? entry (cached-entry cache)))
            (d/with-transaction [tx conn]
              (d/transact! tx [[:db/add 1 :value 3]])
              (is (= #{["one" 3]} (d/q query @tx)))
              (is (identical? entry (cached-entry cache)))
              (d/with-transaction [nested tx]
                (d/transact! nested [[:db/add 1 :value 4]])
                (is (= #{["one" 4]} (d/q query @nested)))
                (is (identical? entry (cached-entry cache)))))
            (is (= #{["one" 4]} (d/q query @conn)))
            (is (identical? entry (cached-entry cache)))
            (d/with-transaction [tx conn]
              (d/transact! tx [[:db/add 1 :value 5]])
              (is (= #{["one" 5]} (d/q query @tx)))
              (d/abort-transact tx))
            (is (= #{["one" 4]} (d/q query @conn)))
            (is (identical? entry (cached-entry cache)))))
        ;; Close outside the dynamic binding must evict this cache too.
        (d/close conn)
        (is (.isEmpty cache))
        (finally (d/close conn) (u/delete-files dir))))))

(deftest plans-check-schema-and-runtime
  (let [conn (d/create-conn nil schema {:kv-opts {:inmemory? true}})
        cache (LRUCache. 32)]
    (try
      (d/transact! conn [{:db/id 1 :name "one" :value 1}])
      (binding [q/*cache?* false q/*plan-cache* cache]
        (d/q query @conn)
        (let [before (cached-entry cache)
              staged (volatile! nil)]
          (is (thrown-with-msg? clojure.lang.ExceptionInfo #"abort schema"
                (d/with-transaction [tx conn]
                  (d/update-schema tx {:value {:db/cardinality :db.cardinality/many}})
                  (d/transact! tx [[:db/add 1 :value 2]])
                  (is (= #{["one" 1] ["one" 2]} (d/q query @tx)))
                  (vreset! staged (cached-entry cache))
                  (is (not (identical? before @staged)))
                  (throw (ex-info "abort schema" {})))))
          (is (= #{["one" 1]} (d/q query @conn)))
          (is (not (identical? @staged (cached-entry cache)))))
        (d/update-schema conn {:value {:db/cardinality :db.cardinality/many}})
        (d/transact! conn [[:db/add 1 :value 3]])
        (is (= #{["one" 1] ["one" 3]} (d/q query @conn)))
        (let [before (cached-entry cache)]
          (i/assoc-opt (:store @conn) :validate-data? true)
          (d/q query @conn)
          (is (not (identical? before (cached-entry cache)))))
        (let [registry (udf/create-registry)
              database (db/with-runtime-opts @conn {:udf-registry registry})]
          (d/q query database)
          (let [before (cached-entry cache)]
            (swap! registry update :generation inc)
            (d/q query database)
            (is (not (identical? before (cached-entry cache)))))))
      (finally (d/close conn)))))

(deftest plan-identity-and-reopen
  (let [root (u/tmp-dir (str "plan-cache-identity-" (random-uuid)))
        left (str root "/left")
        right (str root "/right")
        opts {:db-name "same-name"}
        a (d/create-conn left schema opts)
        b (d/create-conn right schema opts)
        cache (LRUCache. 32)]
    (try
      (d/transact! a [{:db/id 1 :name "A" :value 1}])
      (d/transact! b [{:db/id 1 :name "B" :value 2}])
      (binding [q/*cache?* false q/*plan-cache* cache]
        (is (= #{["A" 1]} (d/q query @a)))
        (is (= #{["B" 2]} (d/q query @b)))
        (is (= 2 (count (.keys cache)))))
      (d/close a)
      (is (= 1 (count (.keys cache))))
      (let [reopened (d/create-conn left)]
        (try
          (binding [q/*cache?* false q/*plan-cache* cache]
            (d/transact! reopened [[:db/add 1 :value 3]])
            (is (= #{["A" 3]} (d/q query @reopened)))
            (is (= 2 (count (.keys cache)))))
          (finally (d/close reopened))))
      (finally (d/close a) (d/close b) (u/delete-files root)))))

(def ^:redef positive-value? pos?)

(deftest plans-respect-function-bindings-and-resolver-mode
  (let [conn (d/create-conn nil schema {:kv-opts {:inmemory? true}})
        cache (LRUCache. 32)
        pred-query '[:find ?name :where [?e :name ?name] [?e :value ?v]
                     [(datalevin.plan-cache-test/positive-value? ?v)]]]
    (try
      (d/transact! conn [{:db/id 1 :name "positive" :value 1}
                        {:db/id 2 :name "negative" :value -1}])
      (binding [q/*cache?* false q/*plan-cache* cache]
        (is (= #{["positive"]} (d/q pred-query @conn)))
        (with-redefs [positive-value? neg?]
          (is (= #{["negative"]} (d/q pred-query @conn))))
        (is (= #{["positive"]} (d/q pred-query @conn)))
        (binding [resolve/*resolver-mode* :server-safe]
          (is (thrown? Exception (d/q pred-query @conn)))))
      (finally (d/close conn)))))

(deftest plans-track-all-database-sources
  (let [a (d/create-conn nil schema {:kv-opts {:inmemory? true}})
        b (d/create-conn nil schema {:kv-opts {:inmemory? true}})
        cache (LRUCache. 32)
        query '[:find ?name ?other :in $a $b
                :where [$a ?e :name ?name] [$a ?e :value ?v]
                [$b ?o :name ?other] [$b ?o :value ?v]]]
    (try
      (d/transact! a [{:db/id 1 :name "A" :value 1}])
      (d/transact! b [{:db/id 1 :name "B" :value 1}])
      (binding [q/*cache?* false q/*plan-cache* cache]
        (is (= #{["A" "B"]} (d/q query @a @b)))
        (let [entries (mapv #(.get cache %) (.keys cache))]
          (is (= 2 (count entries)))
          (d/with-transaction [tx b]
            (d/transact! tx [[:db/add 1 :name "current B"]])
            (is (= #{["A" "current B"]} (d/q query @a @tx))))
          (is (= #{["A" "current B"]} (d/q query @a @b)))
          (is (every? (fn [entry]
                        (some #(identical? entry (.get cache %)) (.keys cache)))
                      entries))))
      ;; Both source plans depend on B, including the plan keyed by A.
      (d/close b)
      (is (.isEmpty cache))
      (finally (d/close a) (d/close b)))))

(deftest cached-join-steps-rebind-all-sources
  ;; Exercise nested target/join steps as well as the top-level fused step.
  ;; The execution regression below verifies the same path through d/q.
  (let [step (plan/->OrJoinStep '(or-join [?a ?b]) '?a 0 ['?b] '?e :ref
                               {'$ :old} {:old :rules} #{} #{} [] #{} #{})
        nested (plan/->HashJoinStep {} nil #{} #{} [] [] #{} #{}
                                   [step] 1 1)
        plans [[(plan/->Plan [nested] 1 1 1)]]
        cached (#'pb/strip-result plans)
        bound (#'pb/bind-plan-sources cached {'$ :current '$other :other}
                                     {:current :rules})
        cached-step (-> cached ffirst :steps first :tgt-steps first)
        bound-step (-> bound ffirst :steps first :tgt-steps first)]
    (is (nil? (:sources cached-step)))
    (is (nil? (:rules cached-step)))
    (is (= {'$ :current '$other :other} (:sources bound-step)))
    (is (= {:current :rules} (:rules bound-step)))))

(deftest fused-join-plan-survives-commit-and-abort
  (let [conn (d/create-conn
               nil
               {:start/id {}
                :edge/from {:db/valueType :db.type/ref}
                :edge/to {:db/valueType :db.type/ref}
                :membership/person {:db/valueType :db.type/ref}
                :membership/forum {}}
               {:kv-opts {:inmemory? true}})
        cache (LRUCache. 32)
        query '[:find ?forum
                :where [?start :start/id 1]
                (or-join [?start ?person]
                  (and [?edge :edge/from ?start] [?edge :edge/to ?person])
                  (and [?edge1 :edge/from ?start] [?edge1 :edge/to ?mid]
                       [?edge2 :edge/from ?mid] [?edge2 :edge/to ?person]))
                [?membership :membership/person ?person]
                [?membership :membership/forum ?forum]]]
    (try
      (d/transact! conn
        (into [{:db/id 1 :start/id 1}]
              (mapcat (fn [n]
                        [{:db/id (+ 100 n) :edge/from 1 :edge/to (+ 10 n)}
                         {:db/id (+ 200 n) :membership/person (+ 10 n)
                          :membership/forum n}])
                      (range 20))))
      (binding [q/*cache?* false q/*plan-cache* cache]
        (is (= (set (map vector (range 20))) (d/q query @conn)))
        (let [entry (cached-entry cache)]
          (is (:bind-sources? entry))
          (d/with-transaction [tx conn]
            (d/transact! tx [[:db/retract 100 :edge/to 10]])
            (is (= (set (map vector (range 1 20))) (d/q query @tx)))
            (is (identical? entry (cached-entry cache))))
          (is (= (set (map vector (range 1 20))) (d/q query @conn)))
          (is (identical? entry (cached-entry cache)))
          (d/with-transaction [tx conn]
            (d/transact! tx [[:db/retract 101 :edge/to 11]])
            (is (= (set (map vector (range 2 20))) (d/q query @tx)))
            (is (identical? entry (cached-entry cache)))
            (d/abort-transact tx))
          (is (= (set (map vector (range 1 20))) (d/q query @conn)))
          (is (identical? entry (cached-entry cache)))))
      (finally (d/close conn)))))
