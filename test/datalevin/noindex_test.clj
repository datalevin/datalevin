(ns datalevin.noindex-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [datalevin.constants :as c]
            [datalevin.core :as d]
            [datalevin.datom :as datom]
            [datalevin.interface :as i]
            [datalevin.interpret :as inter]
            [datalevin.storage :as s]
            [datalevin.test.core :refer [db-fixture]]
            [datalevin.util :as u]))

(use-fixtures :each db-fixture)

(def schema
  {:name {:db/valueType :db.type/string :db/unique :db.unique/identity}
   :body {:db/valueType :db.type/string :db/noindex true}
   :tags {:db/valueType :db.type/string :db/cardinality :db.cardinality/many
          :db/noindex true}})

(defn- ave-attrs [conn]
  (set (map :a (d/datoms @conn :ave))))

(deftest unindexed-writes-and-backfill
  (doseq [wal? [false true]
          ordered? [false true]]
    (binding [c/*ordered-datom-writes?* ordered?]
      (let [dir (u/tmp-dir (str "noindex-" (random-uuid)))
            conn (d/create-conn dir schema {:wal? wal?})
            giant (apply str (repeat 1000 "body"))]
        (try
          (d/transact! conn [[:db/add 1 :body giant]
                             [:db/add 1 :name "one"]
                             [:db/add 2 :body giant]
                             [:db/add 2 :name (str "name-" giant)]
                             [:db/add 1 :tags "a"] [:db/add 1 :tags "b"]])
          (is (= #{:name} (ave-attrs conn)))
          (is (= giant (:body (d/entity @conn 1))))
          (is (= {:body giant :tags ["a" "b"]}
                 (d/pull @conn '[:body :tags] 1)))
          (d/transact! conn [[:db/add 1 :body "replacement"]
                             [:db/retract 1 :tags "a"]])
          (is (= #{:name} (ave-attrs conn)))
          (is (= #{"b"} (:tags (d/entity @conn 1))))
          (is (= "replacement" (:body (d/entity @conn 1))))
          (d/close conn)
          (let [conn (d/create-conn dir)
                query '[:find ?e :in $ ?v :where [?e :body ?v]]
                prepared (d/prepare-q @conn query)]
            (try
              (is (= #{:name} (ave-attrs conn)))
              (is (thrown-with-msg? Exception #"index-attr" (prepared [giant])))
              (testing "aborted backfills publish neither schema nor index"
                (d/with-transaction [tx conn]
                  (d/index-attr tx :body)
                  (is (= #{[2]} (d/q query @tx giant)))
                  (d/abort-transact tx))
                (is (true? (get-in (d/schema conn) [:body :db/noindex])))
                (is (= #{:name} (ave-attrs conn))))
              (d/index-attr conn :body)
              (d/index-attr conn :tags)
              (is (nil? (get-in (d/schema conn) [:body :db/noindex])))
              (is (= #{[2]} (prepared [giant])))
              (is (= #{[1 "b"]}
                     (d/q '[:find ?e ?v :where [?e :tags ?v]] @conn)))
              (is (= (set (d/datoms @conn :eav)) (set (d/datoms @conn :ave))))
              (is (= (d/schema conn) (d/index-attr conn :body)))
              (d/transact! conn [[:db/add 1 :body "indexed"]
                                 [:db/retractEntity 2]])
              (is (= #{[1]} (d/q query @conn "indexed")))
              (is (= #{} (d/q query @conn giant)))
              (d/close conn)
              (let [reopened (d/create-conn dir)]
                (try
                  (is (= #{[1]} (d/q query @reopened "indexed")))
                  (is (= #{:name :body :tags} (ave-attrs reopened)))
                  (finally (d/close reopened))))
              (finally (d/close conn))))
          (finally (d/close conn) (u/delete-files dir)))))))

(deftest unindexed-query-conditions-fail
  (let [conn (d/create-conn nil schema {:wal? false})]
    (try
      (d/transact! conn [{:db/id 1 :name "one" :body "text"}])
      (doseq [[query inputs]
              [['[:find ?e :where [?e :body "text"]] []]
               ['[:find ?e :where [?e :body ?v]] []]
               ['[:find ?v :where [1 :body ?v]] []]
               ['[:find ?e :where [?e :name "one"] [?e :body "text"]] []]
               ['[:find ?v :in $ ?name :where [?e :name ?name] [?e :body ?v]] ["one"]]
               ['[:find ?e :in $ ?a :where [?e ?a "text"]] [:body]]
               ['[:find ?e :in $ [?a ...] :where [?e ?a "text"]] [[:name :body]]]
               ['[:find ?e :where (or [?e :body "text"] [?e :name "one"])] []]
               ['[:find ?e :where [?e :name _] (not [?e :body "absent"])] []]
               ['[:find ?e :in $ % :where (has-body ?e)]
                ['[[(has-body ?e) [?e :body "text"]]]]]]]
        (testing (str query)
          (is (thrown-with-msg? Exception #"index-attr"
                               (apply d/q query @conn inputs)))
          (is (thrown-with-msg? Exception #"index-attr"
                               ((d/prepare-q @conn query) inputs)))))
      (is (thrown-with-msg? Exception #"index-attr"
                           (d/index-range @conn :body "a" "z")))
      (is (thrown-with-msg? Exception #"index-attr"
                           (d/datoms @conn :ave :body)))
      (doseq [read [#(d/search-datoms @conn nil :body nil)
                    #(d/count-datoms @conn nil :body "text")
                    #(d/cardinality @conn :body)]]
        (is (thrown-with-msg? Exception #"index-attr" (read))))
      (is (= 1 (count (d/search-datoms @conn 1 :body nil))))
      (is (= #{{:body "text"}}
             (set (d/q '[:find [(pull ?e [:body]) ...]
                         :where [?e :name "one"]] @conn))))
      (is (= [[1 :body "text"]]
             (mapv (juxt :e :a :v) (d/datoms @conn :eav 1 :body))))
      (finally (d/close conn)))))

(deftest noindex-schema-validation
  (let [dir (u/tmp-dir (str "noindex-schema-" (random-uuid)))
        conn (d/create-conn dir schema {:wal? false})]
    (try
      (doseq [props [{:db/noindex :yes}
                    {:db/noindex true :db/unique :db.unique/value}
                    {:db/noindex true :db/valueType :db.type/ref}]]
        (is (thrown? Exception (d/update-schema conn {:invalid props}))))
      (is (thrown? Exception (d/index-attr conn :missing)))
      (d/transact! conn [{:db/id 1 :body "text" :tags ["a" "b"] :name "one"}])
      (doseq [patch [{:body {:db/noindex false}}
                    {:body {:db/noindex :db/retract}}
                    {:body {:db/valueType :db.type/long}}
                    {:tags {:db/cardinality :db.cardinality/one}}]]
        (is (thrown? Exception (d/update-schema conn patch))))
      (is (thrown? Exception (d/update-schema conn nil [:body])))
      (d/update-schema conn nil nil {:body :content})
      (is (= "text" (:content (d/entity @conn 1))))
      (d/index-attr conn :content)
      (is (= #{[1]} (d/q '[:find ?e :where [?e :content "text"]] @conn)))
      (d/close conn)
      (is (thrown? Exception (d/create-conn dir {:tags {:db/noindex false}})))
      (let [reopened (d/create-conn dir)]
        (try
          (is (= #{"a" "b"} (:tags (d/entity @reopened 1))))
          (d/transact! reopened [[:db/retract 1 :tags]])
          (d/update-schema reopened {:tags {:db/noindex false}})
          (d/transact! reopened [[:db/add 1 :tags "indexed"]])
          (is (= #{[1]} (d/q '[:find ?e :where [?e :tags "indexed"]] @reopened)))
          (finally (d/close reopened))))
      (finally (d/close conn) (u/delete-files dir)))))

(deftest unindexed-custom-and-untyped-values
  (let [conn (d/create-conn nil {:data {:db/noindex true}} {:wal? true})]
    (try
      (d/register-type conn :app/value
                       {:index {:type :long :order-fn (inter/inter-fn [v] (:rank v))}})
      (d/update-schema conn {:custom {:db/valueType :app/value :db/noindex true}})
      (d/transact! conn [{:db/id 1 :custom {:rank 1 :text "old"} :data 42}
                         {:db/id 2 :custom {:rank 1 :text "two"}}])
      (d/transact! conn [[:db/add 1 :custom {:rank 1 :text "new"}]])
      (is (= #{} (ave-attrs conn)))
      (is (= {:rank 1 :text "new"} (:custom (d/entity @conn 1))))
      (d/update-schema conn {:data {:db/valueType :db.type/long}})
      (is (= 42 (:data (d/entity @conn 1))))
      (is (= #{} (ave-attrs conn)))
      (d/index-attr conn :custom)
      (d/index-attr conn :data)
      (is (= #{[2]} (d/q '[:find ?e :in $ ?v :where [?e :custom ?v]]
                          @conn {:rank 1 :text "two"})))
      (is (= #{[1]} (d/q '[:find ?e :where [?e :data 42]] @conn)))
      (is (= 2 (d/entries (d/datalog-kv conn) c/custom-values)))
      (finally (d/close conn)))))

(deftest bulk-load-unindexed-attributes
  (let [store (s/open nil schema {:wal? false})]
    (try
      (i/load-datoms store [(datom/datom 1 :body "body")
                            (datom/datom 1 :name "one")])
      (is (= 2 (i/datom-count store :eav)))
      (is (= 1 (i/datom-count store :ave)))
      (i/index-attr store :body)
      (is (= 2 (i/datom-count store :ave)))
      (finally (i/close store)))))

(deftest index-attr-refreshes-other-connections
  (let [dir (u/tmp-dir (str "noindex-shared-" (random-uuid)))
        conn (d/create-conn dir schema {:wal? true})
        other (d/create-conn dir)]
    (try
      (d/transact! conn [{:db/id 1 :body "one"}])
      (d/with-transaction [tx conn]
        (d/update-schema tx {:body {:db/cardinality :db.cardinality/many}}))
      (binding [c/*fill-db-batch-size* 1]
        (d/with-transaction [tx other]
          (d/index-attr tx :body)))
      (is (= :db.cardinality/many
             (get-in (d/schema other) [:body :db/cardinality])))
      ;; No intervening query/pull refresh on the original connection.
      (d/transact! conn [{:db/id 2 :body "two"}])
      (is (= #{[1 "one"] [2 "two"]}
             (d/q '[:find ?e ?v :where [?e :body ?v]] @other)))
      (is (= #{[1 "one"] [2 "two"]}
             (set (map (juxt :e :v) (d/datoms @conn :ave :body)))))
      (finally (d/close other) (d/close conn) (u/delete-files dir)))))
