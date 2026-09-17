(ns datalevin.query-pull-cache-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [datalevin.core :as d]
            [datalevin.db :as db]
            [datalevin.pull-api :as pull]
            [datalevin.query :as q]
            [datalevin.query.cache :as cache]
            [datalevin.test.core :refer [db-fixture]])
  (:import [datalevin.query.cache CacheAnalysis]
           [datalevin.utl LRUCache]))

(use-fixtures :each db-fixture)

(def ^:private schema
  {:key {:db/unique :db.unique/identity}
   :kind {} :name {} :age {} :note {} :label {} :data {} :child-name {}
   :aliases {:db/cardinality :db.cardinality/many}
   :friend {:db/valueType :db.type/ref :db/cardinality :db.cardinality/many}
   :part {:db/valueType :db.type/ref :db/isComponent true}})

(defn- connection []
  (let [conn (d/create-conn nil schema {:kv-opts {:inmemory? true}})]
    (d/transact! conn [{:db/id 1 :key "one" :kind "person" :name "One"
                       :data {:config {:enabled true}} :friend [2] :part 4}
                      {:db/id 2 :key "two" :name "Two"}
                      {:db/id 3 :key "three" :name "Three" :age 30 :friend [1]}
                      {:db/id 4 :child-name "Part"}])
    conn))

(defn- reader [conn mode query & inputs]
  (if (= :prepared mode)
    (let [prepared (d/prepare-q @conn query)]
      #(prepared (vec inputs)))
    #(apply d/q query @conn inputs)))

(defn- query-entries [conn]
  (let [[^LRUCache cache] (db/cache-token (:store @conn))]
    (filterv #(= :query-result (first %)) (.keys cache))))

(defn- prime! [conn read]
  (db/refresh-cache (:store @conn))
  (let [result (read)
        keys (query-entries conn)
        key (first keys)]
    (is (= 1 (count keys)))
    (is (identical? result (db/cache-get (:store @conn) key)))
    [key result]))

(defn- check-write! [conn read txs invalidate?]
  (let [[key before] (prime! conn read)]
    (d/transact! conn txs)
    (let [cached (db/cache-get (:store @conn) key)]
      (if invalidate?
        (is (nil? cached) (str "invalidate after " txs))
        (is (identical? before cached) (str "preserve after " txs))))
    (let [result (read)]
      (is (= result (binding [q/*cache?* false] (read))))
      result)))

(deftest explicit-pull-depends-on-query-and-projected-attributes
  (doseq [mode [:ordinary :prepared]]
    (let [conn (connection)
          read (reader conn mode
                 '[:find ?e (pull ?e [:name :data])
                   :where [?e :kind "person"]])]
      (try
        (check-write! conn read [[:db/add 1 :note "unrelated"]] false)
        (is (= #{[1 {:name "Updated" :data {:config {:enabled true}}}]}
               (set (check-write! conn read [[:db/add 1 :name "Updated"]] true))))
        (is (= #{[1 {:name "Updated" :data {:config {:enabled false}}}]}
               (set (check-write! conn read
                      [[:db/add 1 :data {:config {:enabled false}}]] true))))
        (is (= #{[1 {:name "Updated" :data {:config {:enabled false}}}]
                 [2 {:name "Two"}]}
               (set (check-write! conn read [[:db/add 2 :kind "person"]] true))))
        (check-write! conn read [[:db/retract 2 :name "Two"]] true)
        (check-write! conn read [[:db/retract 2 :kind "person"]] true)
        (finally (d/close conn))))))

(deftest nested-reverse-and-recursive-pull-dependencies
  (doseq [mode [:ordinary :prepared]]
    (let [conn (connection)
          read (reader conn mode
                 '[:find (pull ?e [:name {:friend [:name]} {:_friend [:age]}]) .
                   :where [?e :key "one"]])]
      (try
        (check-write! conn read [[:db/add 2 :note "unrelated"]] false)
        (is (= {:name "One" :friend [{:name "Two!"}] :_friend [{:age 30}]}
               (check-write! conn read [[:db/add 2 :name "Two!"]] true)))
        (check-write! conn read [[:db/add 3 :age 31]] true)
        (check-write! conn read [[:db/retract 3 :friend 1]] true)
        (check-write! conn read [[:db/add 3 :friend 1]] true)
        (check-write! conn read [[:db/retract 1 :friend 2]] true)
        (d/transact! conn [[:db/add 1 :friend 2] [:db/add 2 :friend 1]])
        (doseq [limit ['... 2]]
          (let [read (reader conn mode
                       {:find [(list 'pull '?e [:name {:friend limit}]) '.]
                        :where '[[?e :key "one"]]})]
            (check-write! conn read [[:db/add 2 :note (str limit)]] false)
            (check-write! conn read [[:db/add 2 :name (str "Two " limit)]] true)))
        (finally (d/close conn))))))

(deftest aliases-defaults-and-limits-use-the-stored-attribute
  (let [conn (connection)
        read (reader conn :prepared
               '[:find (pull ?e [[:name :as :label] [:age :default 0]
                                 [:aliases :limit 1]]) .
                 :where [?e :key "one"]])]
    (try
      (is (= {:label "One" :age 0}
             (check-write! conn read [[:db/add 1 :label "not the name"]] false)))
      (check-write! conn read [[:db/add 1 :age 10]] true)
      (check-write! conn read [[:db/add 1 :aliases "A"]] true)
      (check-write! conn read [[:db/retract 1 :aliases "A"]] true)
      (check-write! conn read [[:db/retract 1 :age 10]] true)
      (finally (d/close conn)))))

(deftest scalar-input-patterns-have-separate-dependencies
  (doseq [mode [:ordinary :prepared]
          pattern-var ['?pattern 'pattern]]
    (let [conn (connection)
          query {:find [(list 'pull '?e pattern-var) '.]
                 :in ['$ pattern-var] :where '[[?e :key "one"]]}
          names (reader conn mode query [:name])
          data (reader conn mode query [:data])]
      (try
        (let [[name-key _] (prime! conn names)
              old-data (data)
              data-key (first (remove #{name-key} (query-entries conn)))]
          (is (some? data-key))
          (d/transact! conn [[:db/add 1 :name "Changed"]])
          (is (nil? (db/cache-get (:store @conn) name-key)))
          (is (identical? old-data (db/cache-get (:store @conn) data-key)))
          (is (= {:name "Changed"} (names)))
          (is (= old-data (data))))
        (finally (d/close conn))))))

(deftest multiple-pulls-union-dependencies-for-a-named-source
  (let [conn (connection)
        read (reader conn :prepared
               '[:find [(pull $people ?e [:name]) (pull $people ?f [:age])]
                 :in $people :where [$people ?e :key "one"]
                 [$people ?e :friend ?f]])]
    (try
      (check-write! conn read [[:db/add 2 :note "unrelated"]] false)
      (is (= [{:name "One"} {:age 20}]
             (check-write! conn read [[:db/add 2 :age 20]] true)))
      (is (= [{:name "Renamed"} {:age 20}]
             (check-write! conn read [[:db/add 1 :name "Renamed"]] true)))
      (finally (d/close conn)))))

(deftest wildcard-and-component-expansion-remain-conservative
  (let [conn (connection)]
    (try
      (doseq [pattern ['[*] '[:part] '[{:friend [*]}]]]
        (let [read (reader conn :prepared
                     {:find [(list 'pull '?e pattern) '.]
                      :where '[[?e :key "one"]]})]
          (check-write! conn read [[:db/add 3 :note (pr-str pattern)]] true)
          (check-write! conn read [[:db/add 4 :child-name (pr-str pattern)]] true)))
      (let [read (reader conn :prepared
                   '[:find (pull ?e [{:part [:child-name]}]) .
                     :where [?e :key "one"]])]
        (check-write! conn read [[:db/add 4 :note "unselected"]] false)
        (is (= {:part {:child-name "Updated part"}}
               (check-write! conn read [[:db/add 4 :child-name "Updated part"]] true))))
      (finally (d/close conn)))))

(deftest schema-change-during-dependency-analysis-cannot-publish-stale-deps
  (let [conn (connection)
        query '[:find (pull ?e [:friend]) . :where [?e :key "one"]]
        captured (promise)
        resume (promise)
        analysis (CacheAnalysis.
                   false {:all? false :attrs #{:key}} false false
                   (fn [inputs]
                     (let [deps (pull/pattern-deps (first inputs) [:friend])]
                       (deliver captured true)
                       @resume
                       deps)))]
    (try
      (db/refresh-cache (:store @conn))
      (let [parsed (cache/parsed-q query)
            pending (future (cache/q-result parsed [@conn] analysis nil))]
        (try
          (is (= true (deref captured 10000 :timed-out)))
          (d/update-schema conn {:friend {:db/isComponent true}})
          (deliver resume true)
          (is (= "Two" (-> (deref pending 10000 :timed-out) :friend first :name)))
          (is (empty? (query-entries conn)))
          (d/transact! conn [[:db/add 2 :name "Current"]])
          (is (= "Current" (-> (d/q query @conn) :friend first :name)))
          (finally
            (deliver resume true)
            (future-cancel pending))))
      (finally (d/close conn)))))

(deftest hidden-dependencies-retain-conservative-invalidation
  (let [conn (connection)]
    (try
      (let [read (reader conn :prepared
                   '[:find (pull ?e [:name]) . :in $ %
                     :where [?e :key "one"] (eligible ?e)]
                   '[[(eligible ?e) [?e :age 10]]])]
        (d/transact! conn [[:db/add 1 :age 10]])
        (is (nil? (check-write! conn read [[:db/add 1 :age 11]] true))))
      (let [read (reader conn :prepared
                   '[:find (pull ?e [:name]) . :where [?e :key "one"]
                     [(get-else $ ?e :age 0) ?age] [(< ?age 12)]])]
        (is (nil? (check-write! conn read [[:db/add 1 :age 12]] true))))
      (let [pattern [[:name :xform (fn [name] (str name (:note (d/pull @conn [:note] 1))))]]
            read (reader conn :prepared
                   {:find [(list 'pull '?e pattern) '.]
                    :where '[[?e :key "one"]]})]
        (is (= {:name "One!"}
               (check-write! conn read [[:db/add 1 :note "!"]] true))))
      (testing "lookup-ref roots depend on their identifying attribute"
        (let [read (reader conn :prepared
                     '[:find [(pull ?e [:name]) ...] :in $ [?e ...]]
                     [[:key "one"]])]
          (is (= [nil] (check-write! conn read [[:db/retract 1 :key "one"]] true)))
          (is (= [{:name "Two"}]
                 (check-write! conn read [[:db/add 2 :key "one"]] true)))))
      (finally (d/close conn)))))

(deftest pull-dependencies-follow-schema-and-transaction-state
  (let [conn (connection)
        pattern [:friend]
        read (reader conn :prepared
               '[:find (pull ?e [:friend]) . :where [?e :key "one"]])]
    (try
      (let [deps (pull/pattern-deps @conn pattern)]
        (is (= {:all? false :attrs #{:friend}} deps))
        (check-write! conn read [[:db/add 2 :note "unrelated"]] false)
        (is (identical? deps (pull/pattern-deps @conn pattern)))
        (d/update-schema conn {:friend {:db/isComponent true}})
        (is (:all? (pull/pattern-deps @conn pattern)))
        (is (= "Two" (-> (read) :friend first :name)))
        (check-write! conn read [[:db/add 2 :name "Two!"]] true))
      (let [query '[:find (pull ?e [:name]) . :where [?e :key "one"]]
            read (reader conn :ordinary query)]
        (prime! conn read)
        (d/with-transaction [tx conn]
          (d/transact! tx [[:db/add 1 :name "Aborted"]])
          (is (= {:name "Aborted"} (d/q query @tx)))
          (d/abort-transact tx))
        (is (= {:name "One"} (read)))
        (d/with-transaction [tx conn]
          (d/transact! tx [[:db/add 1 :name "Committed"]])
          (is (= {:name "Committed"} (d/q query @tx))))
        (is (= {:name "Committed"} (read))))
      (finally (d/close conn)))))
