(ns datalevin.prepared-query-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [datalevin.core :as d]
            [datalevin.bits :as bits]
            [datalevin.db :as db]
            [datalevin.interpret :as inter]
            [datalevin.protocol :as protocol]
            [datalevin.query :as q]
            [datalevin.query.cache :as cache]
            [datalevin.server :as server]
            [datalevin.test.core :refer [allocate-port db-fixture]]
            [datalevin.timeout :as timeout]
            [datalevin.udf :as udf]
            [datalevin.util :as u])
  (:import [datalevin.read_encode ReadResult]
           [datalevin.utl LRUCache]
           [java.nio ByteBuffer BufferOverflowException]))

(use-fixtures :each db-fixture)

(def ^:private schema
  {:key {:db/unique :db.unique/identity}
   :name {} :value {:db/valueType :db.type/long}
   :friend {:db/valueType :db.type/ref}})

(def ^:private rows
  [{:db/id 1 :key "one" :name "ONE" :value 10 :friend 2}
   {:db/id 2 :key "two" :name "TWO" :value 20}])

(def ^:private projection
  '[:find ?name ?value :in $ ?key
    :where [?e :key ?key] [?e :name ?name] [?e :value ?value]])

(def ^:private range-query
  '{:find [?value (pull ?e ?pattern)]
    :in [$ ?start ?limit ?pattern]
    :where [[?e :value ?value] [(>= ?value ?start)]]
    :order-by [?value] :limit ?limit})

(defn- check-query-results [conn]
  (doseq [[query inputs]
          [[projection [["one"] ["two"] ["absent"] ["one"]]]
           ['[:find [?v ?n] :in $ ?key
              :where [?e :key ?key] [?e :name ?n] [?e :value ?v]]
            [["one"] ["two"] ["absent"]]]
           ['[:find ?n . :in $ ?key
              :where [?e :key ?key] [?e :name ?n]]
            [["one"] ["two"] ["absent"]]]
           ['[:find [?n ...] :in $ [?key ...]
              :where [?e :key ?key] [?e :name ?n]]
            [[["one" "two"]] [[]] [["two"]]]]
           ['[:find ?n ?other :in $ [?min ?max]
              :where [?e :value ?v] [(<= ?min ?v ?max)]
              [?e :name ?n] [?e :friend ?f] [?f :name ?other]]
            [[[0 15]] [[20 30]]]]
           ['[:find (sum ?v) . :in $ ?min
              :where [?e :value ?v] [(>= ?v ?min)]]
            [[0] [15] [50]]]
           ['[:find ?n :in $ % ?key
              :where [?e :key ?key] (named ?e ?n)]
            [['[[(named ?e ?n) [?e :name ?n]]] "one"]
             ['[[(named ?e ?n) [?e :name ?n]]] "two"]]]
           ['[:find ?n :where [?e :name ?n]] [[]]]
           ['[:find ?n :in $people $keys
              :where [$people ?e :key ?key] [$people ?e :name ?n]
              [$keys ?key]]
            [[[["one"]]]]]]]
    (let [db @conn
          reader (d/prepare-q db query)]
      (doseq [params inputs]
        (is (= (apply d/q query db params) (d/execute-prepared reader params))
            (str query " " params)))))
  (let [reader (d/prepare-q @conn projection)
        range-reader (d/prepare-q @conn range-query)]
    (doseq [inputs [[0 1 [:name]] [15 3 [:name :value]] [30 2 [:name]]]]
      (is (= (apply d/q range-query @conn inputs) (range-reader inputs))))
    (is (= #{["ONE" 10]} (reader ["one"])))
    (is (= #{["TWO" 20]} (apply reader [["two"]])))
    (d/transact! conn [[:db/add 1 :name "updated"]])
    (is (= #{["updated" 10]} (reader ["one"])))
    (is (= [[10 {:name "updated"}]] (range-reader [0 1 [:name]])))
    (d/update-schema conn {:value {:db/cardinality :db.cardinality/many}})
    (d/transact! conn [[:db/add 1 :value 11]])
    (is (= #{["updated" 10] ["updated" 11]} (reader ["one"])))
    (is (= (d/q projection @conn "one") (reader ["one"])))
    (is (= (d/q range-query @conn 0 3 [:name :value])
           (range-reader [0 3 [:name :value]])))
    (is (every? #(= #{["TWO" 20]} %)
                (mapv deref (repeatedly 4 #(future (reader ["two"]))))))
    (doseq [bad [nil "one" '("one") [] ["one" "two"]]]
      (is (thrown-with-msg? Exception #"inputs must be a vector" (reader bad)))))
  (d/with-transaction [tx conn]
    (let [reader (d/prepare-q @tx projection)]
      (is (= #{["TWO" 20]} (reader ["two"])))
      (d/transact! tx [[:db/add 2 :name "inside"]])
      (is (= #{["inside" 20]} (reader ["two"])))
      (d/abort-transact tx)))
  (is (= #{["TWO" 20]} ((d/prepare-q @conn projection) ["two"]))))

(deftest prepared-query-local-and-remote-results
  (let [root (u/tmp-dir (str "prepared-query-" (random-uuid)))
        port (allocate-port)
        srv (server/create {:root (str root "/server") :port port})]
    (try
      (server/start srv)
      (doseq [location [(str root "/local")
                       (str "dtlv://datalevin:datalevin@localhost:" port "/queries")]]
        (let [conn (d/create-conn location schema {:client-opts {:pool-size 1}})]
          (try
            (d/transact! conn rows)
            (check-query-results conn)
            (let [other (d/create-conn nil {} {:kv-opts {:inmemory? true}})
                  query '[:find ?n :in $people $keys
                          :where [$people ?e :key ?key] [$people ?e :name ?n]
                          [$keys ?x :key ?key]]
                  reader (d/prepare-q @conn query)]
              (try
                (d/transact! other [{:key "two"}])
                (if (.startsWith ^String location "dtlv:")
                  (is (thrown-with-msg? Exception #"exactly one database source"
                                       (reader [@other])))
                  (do
                    (is (= #{["TWO"]} (reader [@other])))
                    (d/transact! other [{:key "one"}])
                    (is (= #{["updated"] ["TWO"]} (reader [@other])))))
                (finally (d/close other))))
            (finally (d/close conn)))))
      (finally (server/stop srv) (u/delete-files root)))))

(def ^:redef cache-probe identity)

(deftest prepared-query-keeps-runtime-cache-and-function-bindings
  (let [conn (d/create-conn nil schema {:kv-opts {:inmemory? true}})
        query '[:find ?out . :in $ ?key
                :where [?e :key ?key] [?e :name ?n]
                [(datalevin.prepared-query-test/cache-probe ?n) ?out]]]
    (try
      (d/transact! conn rows)
      (let [reader (d/prepare-q @conn query)
            calls (atom 0)]
        (with-redefs [cache-probe #(do (swap! calls inc) (str % "!"))]
          (is (= "ONE!" (reader ["one"]) (reader ["one"])))
          (is (= 1 @calls))
          (binding [q/*cache?* false]
            (is (= "ONE!" (reader ["one"]) (reader ["one"]))))
          (is (= 3 @calls))
          (binding [cache/*cache?* false] (reader ["one"]))
          (is (= 4 @calls)))
        (with-redefs [cache-probe #(str % "?")]
          (is (= "ONE?" (reader ["one"]))))
        (is (= "ONE" (reader ["one"]))))
      (testing "nested query dependencies and execution deadlines remain current"
        (let [query '[:find ?n . :where
                      [(q [:find (count ?e) . :where [?e :name]] $) ?n]]
              reader (d/prepare-q @conn query)]
          (is (= 2 (reader [])))
          (d/transact! conn [{:db/id 3 :name "THREE"}])
          (is (= 3 (reader []))))
        (let [reader (d/prepare-q @conn '[:find ?deadline . :in $ ?capture
                                         :where [(?capture) ?deadline]
                                         :timeout 10000])
              deadline (+ (System/currentTimeMillis) 1000)]
          (binding [timeout/*deadline* deadline]
            (is (= deadline (reader [(fn [] timeout/*deadline*)]))))
          (is (nil? timeout/*deadline*))))
      (is (thrown-with-msg? Exception #"first :in binding"
                           (d/prepare-q @conn '[:find ?x . :in ?x])))
      (finally (d/close conn)))))

(deftest prepared-query-refreshes-udf-bindings
  (let [descriptor {:udf/lang :test :udf/kind :query-fn :udf/id :decorate}
        registry (doto (udf/create-registry)
                   (udf/register! descriptor #(str % "!")))
        conn (d/create-conn nil schema {:kv-opts {:inmemory? true}
                                       :runtime-opts {:udf-registry registry}})]
    (try
      (d/transact! conn rows)
      (let [reader (d/prepare-q @conn '[:find ?out . :in $ ?key
                                       :where [?e :key ?key] [?e :name ?n]
                                       [(udf :decorate ?n) ?out]])]
        (is (= "ONE!" (reader ["one"])))
        (udf/register! registry descriptor #(str % "?"))
        (is (= "ONE?" (reader ["one"]))))
      (finally (d/close conn)))))

(deftest prepared-query-with-additional-database-source
  (let [a (d/create-conn nil schema {:kv-opts {:inmemory? true}})
        b (d/create-conn nil {} {:kv-opts {:inmemory? true}})
        query '[:find ?n :in $a $b
                :where [$a ?e :key ?key] [$a ?e :name ?n] [$b ?x :key ?key]]]
    (try
      (d/transact! a rows)
      (d/transact! b [{:key "one"}])
      (let [reader (d/prepare-q @a query)]
        (is (= (d/q query @a @b) (reader [@b])))
        (is (= #{["ONE"]} (reader [@b])))
        (d/transact! b [{:key "two"}])
        (is (= #{["ONE"] ["TWO"]} (d/q query @a @b)))
        (is (= #{["ONE"] ["TWO"]} (reader [@b]))))
      (finally (d/close a) (d/close b)))))

(def ^:private tuple-query
  '[:find [?name ?value] :in $ ?key
    :where [?e :key ?key] [?e :name ?name] [?e :value ?value]])

(deftest zero-capacity-queries-skip-cache-keys-and-monitor
  (let [conn (d/create-conn nil schema {:cache-limit 0 :wal? false})
        task (atom nil)
        unhashable (reify Object
                     (hashCode [_] (throw (ex-info "Must not hash query input" {}))))
        query '[:find ?name :in $ ?unused :where [_ :name ?name]]]
    (try
      (d/transact! conn rows)
      (let [database @conn
            reader (d/prepare-q database query)
            [^LRUCache lru] (db/cache-token (:store database))]
        (locking lru
          (reset! task (future [(d/q query database unhashable)
                               (reader [unhashable])]))
          (is (= [#{["ONE"] ["TWO"]} #{["ONE"] ["TWO"]}]
                 (deref @task 5000 ::timeout))))
        (is (.isEmpty lru))
        (d/datalog-index-cache-limit database 16)
        (is (db/cache-active? (:store database)))
        (let [result (reader [nil])]
          (is (identical? result (reader [nil]))))
        (db/disable-cache (:store database))
        (try
          (is (not (db/cache-active? (:store database))))
          (is (= #{["ONE"] ["TWO"]} (reader [unhashable])))
          (finally (db/enable-cache (:store database))))
        (d/transact! conn [[:db/add 1 :name "changed"]])
        (is (= #{["changed"] ["TWO"]} (reader [nil]))))
      (finally
        (when-let [f @task] (future-cancel f))
        (d/close conn)))))

(deftest prepared-tuples-encode-required-fields-in-find-order
  (let [conn (d/create-conn nil (merge schema {:name {:db/noindex true}
                                             :value {:db/noindex true}})
                           {:cache-limit 0 :wal? false})
        reordered '[:find [?value ?name] :in $ ?key
                    :where [?e :key ?key] [?e :name ?name] [?e :value ?value]]
        repeated '[:find [?a ?b] :in $ ?key
                   :where [?e :key ?key] [?e :name ?a] [?e :name ?b]]
        single '[:find [?name] :in $ ?key
                 :where [?e :key ?key] [?e :name ?name]]]
    (try
      (d/transact! conn (into rows [{:key "no-name" :value 30}
                                  {:key "no-value" :name "missing"}]))
      (doseq [query [tuple-query reordered repeated single]]
        (let [reader (q/query-reader query)]
          (doseq [key ["one" "two" "no-name" "no-value" "absent"]]
            (let [result (reader @conn [@conn key] true)]
              (when (not= key "absent") (is (instance? ReadResult result)))
              (is (= (d/q query @conn key)
                     (bits/deserialize (bits/serialize result))))))
          (d/transact! conn [[:db/add 1 :name "updated"]])
          (is (= (d/q query @conn "one")
                 (bits/deserialize (bits/serialize (reader @conn [@conn "one"] true)))))))
      (let [reader (q/query-reader tuple-query)
            decode #(bits/deserialize (bits/serialize (reader % [% "one"] true)))]
        (is (= ["updated" 10] (decode @conn)))
        (let [pending (:db-after (d/tx-data->simulated-report
                                  @conn [[:db/add 1 :name "overlay"]]))]
          (is (not (instance? ReadResult (reader pending [pending "one"] true))))
          (is (= ["overlay" 10] (decode pending))))
        (d/with-transaction [tx conn]
          (d/transact! tx [[:db/add 1 :name "inside"]])
          (is (= ["inside" 10] (decode @tx)))
          (d/abort-transact tx))
        (is (= ["updated" 10] (decode @conn)))
        (d/update-schema conn {:value {:db/cardinality :db.cardinality/many}})
        (d/transact! conn [[:db/add 1 :value 11]])
        (is (not (instance? ReadResult (reader @conn [@conn "one"] true))))
        (is (= (d/q tuple-query @conn "one") (decode @conn))))
      (finally (d/close conn)))))

(deftest prepared-tuple-encoding-retries-and-deadlines
  (let [conn (d/create-conn nil schema {:cache-limit 0 :wal? false})
        reader (q/query-reader tuple-query)]
    (try
      (d/transact! conn [{:key "one" :name (.repeat "x" 10000) :value 10}])
      (let [result (reader @conn [@conn "one"] true)
            small (ByteBuffer/allocate 32)
            large (ByteBuffer/allocate 20000)]
        (is (instance? ReadResult result))
        (is (thrown? BufferOverflowException
                     (protocol/write-message-bf small {:result result})))
        ;; Reusing an unsent writer acquires a new snapshot, including after
        ;; overflow. No cursor or borrowed storage buffer survives the call.
        (d/transact! conn [[:db/add [:key "one"] :name "new snapshot"]])
        (protocol/write-message-bf large {:result result})
        (is (= ["new snapshot" 10]
               (:result (first (protocol/receive-one-message large))))))
      (binding [timeout/*deadline* 1]
        (is (thrown? Exception (reader @conn [@conn "one"] true))))
      (let [timed (q/query-reader (conj tuple-query :timeout 10000))]
        (is (= ["new snapshot" 10] (timed @conn [@conn "one"] true))))
      (finally (d/close conn)))))

(deftest prepared-tuple-custom-values-use-materialized-execution
  (let [conn (d/create-conn nil {} {:wal? false :cache-limit 0})]
    (try
      (d/register-type conn :app/ranked
                       {:index {:type :long :order-fn (inter/inter-fn [v] (:rank v))}})
      (d/update-schema conn {:key {:db/unique :db.unique/identity}
                             :name {} :value {:db/valueType :app/ranked}})
      (d/transact! conn [{:key "one" :name "ONE" :value {:rank 42 :label "value"}}])
      (let [reader (q/query-reader tuple-query)
            result (reader @conn [@conn "one"] true)]
        (is (= ["ONE" {:rank 42 :label "value"}] result))
        (is (= result (d/q tuple-query @conn "one"))))
      (finally (d/close conn)))))
