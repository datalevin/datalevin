(ns datalevin.prepared-query-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [datalevin.core :as d]
            [datalevin.query :as q]
            [datalevin.query.cache :as cache]
            [datalevin.server :as server]
            [datalevin.test.core :refer [allocate-port db-fixture]]
            [datalevin.timeout :as timeout]
            [datalevin.udf :as udf]
            [datalevin.util :as u]))

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
