(ns ^:no-doc pod.huahaiy.datalevin-test
  (:require [datalevin.util :as u]
            [datalevin.constants :as c]
            [datalevin.interpret :as i]
            [babashka.pods :as pods]
            [datalevin.test.core :as tdc :refer [db-fixture]]
            [clojure.test :refer [deftest testing is use-fixtures]])
  (:import [java.util UUID Date Random]))

(use-fixtures :each db-fixture)

(pods/load-pod ["lein" "run" "-m" "pod.huahaiy.datalevin"])
;; This doesn't work on Java 17.
;; (pods/load-pod ["clj" "-Xpod"])

(require '[pod.huahaiy.datalevin :as pd])

(deftest with-transaction-kv-test
  (let [dir  (u/tmp-dir (str "pod-with-tx-kv-test-" (UUID/randomUUID)))
        lmdb (pd/open-kv dir)]
    (pd/open-dbi lmdb "a")

    (testing "new value is invisible to outside readers"
      (pd/with-transaction-kv [db lmdb]
        (is (nil? (pd/get-value db "a" 1 :data :data false)))
        (pd/transact-kv db [[:put "a" 1 2]
                            [:put "a" :counter 0]])
        (is (= [1 2] (pd/get-value db "a" 1 :data :data false)))
        (is (nil? (pd/get-value lmdb "a" 1 :data :data false)))))
    (is (= [1 2] (pd/get-value lmdb "a" 1 :data :data false)))

    (testing "abort"
      (pd/with-transaction-kv [db lmdb]
        (pd/transact-kv db [[:put "a" 1 3]])
        (is (= [1 3] (pd/get-value db "a" 1 :data :data false)))
        (pd/abort-transact-kv db))
      (is (= [1 2] (pd/get-value lmdb "a" 1 :data :data false))))

    (pd/close-kv lmdb)
    (u/delete-files dir)))

(deftest with-txn-kv-map-resize-test
  (let [dir  (u/tmp-dir (str "pod-with-tx-kv-test-" (UUID/randomUUID)))
        lmdb (pd/open-kv dir {:mapsize 1 :kv-wal? false})
        data {:description "this is going to be bigger than 1MB"
              :numbers     (range 1000000)}]
    (pd/open-dbi lmdb "a")

    (pd/with-transaction-kv [db lmdb]
      (pd/transact-kv db [[:put "a" 0 :prior]])
      (is (= :prior (pd/get-value db "a" 0)))
      (pd/transact-kv db [[:put "a" 1 data]])
      (is (= data (pd/get-value db "a" 1))))

    (is (= :prior (pd/get-value lmdb "a" 0)))
    (is (= data (pd/get-value lmdb "a" 1)))

    (pd/close-kv lmdb)
    (u/delete-files dir)))

(deftest with-transaction-test
  (let [dir   (u/tmp-dir (str "pod-with-tx-test-" (UUID/randomUUID)))
        conn  (pd/create-conn dir)
        query '[:find ?c .
                :in $ ?e
                :where [?e :counter ?c]]]
    (is (nil? (pd/q query (pd/db conn) 1)))
    (testing "new value is invisible to outside readers"
      (pd/with-transaction [cn conn]
        (is (nil? (pd/q query (pd/db cn) 1)))
        (pd/transact! cn [{:db/id 1 :counter 1}])
        (is (= 1 (pd/q query (pd/db cn) 1)))
        #_(is (nil? (pd/q query (pd/db conn) 1))))
      (is (= 1 (pd/q query (pd/db conn) 1))))

    (testing "abort"
      (pd/with-transaction [cn conn]
        (pd/transact! cn [{:db/id 1 :counter 2}])
        (is (= 2 (pd/q query (pd/db cn) 1)))
        (pd/abort-transact cn))
      (is (= 1 (pd/q query (pd/db conn) 1))))

    (pd/close conn)
    (u/delete-files dir)))

(deftest with-txn-map-resize-test
  (let [dir    (u/tmp-dir (str "pod-with-tx-test-" (UUID/randomUUID)))
        conn   (pd/create-conn dir nil {:kv-opts {:mapsize 1
                                                  :kv-wal? false}})
        query1 '[:find ?d .
                 :in $ ?e
                 :where [?e :content ?d]]
        query2 '[:find ?d .
                 :in $ ?e
                 :where [?e :description ?d]]
        prior  "prior data"
        big    "bigger than 1MB"]

    (pd/with-transaction [cn conn]
      (pd/transact! cn [{:content prior}])
      (is (= prior (pd/q query1 (pd/db cn) 1)))
      (pd/transact! cn [{:description big
                         :numbers     (range 1000000)}])
      (is (= big (pd/q query2 (pd/db cn) 2))))

    (is (= prior (pd/q query1 (pd/db conn) 1)))
    (is (= big (pd/q query2 (pd/db conn) 2)))

    (pd/close conn)
    (u/delete-files dir)))

(pd/defpodfn age [birthday today]
  (quot (-  (.getTime today) (.getTime birthday))
        (* 1000 60 60 24 365)))

(deftest defpodfn-test
  (let [dir    (u/tmp-dir (str "datalevin-podfn-test-" (UUID/randomUUID)))
        schema (i/load-edn "test/data/movie-schema.edn")
        data   (i/load-edn "test/data/movie-data.edn")
        conn   (pd/get-conn dir schema)
        q      '[:find ?age .
                 :in $ ?today
                 :where
                 [?e :person/name ?name]
                 [?e :person/born ?dob]
                 [(age ?dob ?today) ?age]] ]
    (pd/transact! conn data)
    (is (= (pd/q q (pd/db conn) #inst "2013-08-02T00:00:00.000-00:00") 72))
    (pd/close conn)
    (u/delete-files dir)))

(deftest datalog-readme-test
  (let [dir  (u/tmp-dir (str "datalevin-pod-test-" (UUID/randomUUID)))
        conn (pd/get-conn dir {:aka  {:db/cardinality :db.cardinality/many}
                               :name {:db/valueType :db.type/string
                                      :db/unique    :db.unique/identity}})
        rp   (pd/transact! conn
                           [{:name "Frege", :db/id -1, :nation "France",
                             :aka  ["foo" "fred"]}
                            {:name "Peirce", :db/id -2, :nation "france"}
                            {:name "De Morgan", :db/id -3, :nation "English"}])]
    (is (= 8 (count (:tx-data rp))))
    (is (= 3 (pd/max-eid (pd/db conn))))
    (is (pd/analyze (pd/db conn)))
    (is (= #{["France"]}
           (pd/q '[:find ?nation
                   :in $ ?alias
                   :where
                   [?e :aka ?alias]
                   [?e :nation ?nation]]
                 (pd/db conn)
                 "fred")))
    (pd/transact! conn [[:db/retract 1 :name "Frege"]])
    (is (= [[{:db/id 1, :aka ["foo" "fred"], :nation "France"}]]
           (pd/q '[:find (pull ?e [*])
                   :in $ ?alias
                   :where
                   [?e :aka ?alias]]
                 (pd/db conn)
                 "fred")))
    (is (= #{} (pd/q '[:find ?dummy
                       :in $
                       :where
                       [?e :dummy ?dummy]] ;; non-existent attr
                     (pd/db conn))))
    (pd/close conn)
    (u/delete-files dir)))

(deftest idoc-pod-test
  (let [dir    (u/tmp-dir (str "datalevin-pod-idoc-test-" (UUID/randomUUID)))
        schema {:doc/idoc {:db/valueType :db.type/idoc
                           :db/domain    "profiles"}}
        conn   (pd/create-conn dir schema)]
    (pd/transact!
      conn
      [{:db/id    1
        :doc/idoc {:status  "active"
                   :profile {:age 30}
                   :tags    ["a" "b"]}}
       {:db/id    2
        :doc/idoc {:status  "inactive"
                   :profile {:age 40}}}])
    (let [db (pd/db conn)]
      (is (= #{[1]}
             (pd/q '[:find ?e
                     :in $
                     :where
                     [(idoc-match $ :doc/idoc {:status "active"})
                      [[?e ?a ?v]]]]
                   db)))
      (is (= #{[1]}
             (pd/q '[:find ?e
                     :in $
                     :where
                     [(idoc-match $ :doc/idoc {:tags "b"})
                      [[?e ?a ?v]]]]
                   db)))
      (is (= #{[2]}
             (pd/q '[:find ?e
                     :in $ ?q
                     :where
                     [(idoc-match $ :doc/idoc ?q) [[?e ?a ?v]]]]
                   db
                   '(> [:profile :age] 35))))
      (is (= #{[1]}
             (pd/q '[:find ?e
                     :in $ ?q
                     :where
                     [(idoc-match $ ?q {:domains ["profiles"]})
                      [[?e ?a ?v]]]]
                   db
                   {:status "active"}))))
    (pd/close conn)
    (u/delete-files dir)))

(deftest entity-test
  (let [dir  (u/tmp-dir (str "datalevin-pod-test-" (UUID/randomUUID)))
        conn (pd/get-conn dir
                          {:aka {:db/cardinality :db.cardinality/many}})]
    (pd/transact! conn [{:db/id 1, :name "Ivan", :age 19, :aka ["X" "Y"]}
                        {:db/id 2, :name "Ivan", :sex "male", :aka ["Z"]}
                        [:db/add 3 :huh? false]])
    (let [e (pd/touch (pd/entity (pd/db conn) 1))]
      (is (= (:db/id e) 1))
      (is (= (:name e) "Ivan"))
      (is (= (e :name) "Ivan"))
      (is (= (:age e) 19))
      (is (= (:aka e) #{"X" "Y"}))
      (is (= true (contains? e :age)))
      (is (= false (contains? e :not-found))))
    (pd/close conn)
    (u/delete-files dir)))

(pd/defpodfn custom-fn [n] (str "hello " n))

(deftest custom-fn-test
  (let [dir  (u/tmp-dir (str "datalevin-pod-test-" (UUID/randomUUID)))
        conn (pd/get-conn dir)]
    (is (= #{["hello world"]}
           (pd/q '[:find ?greeting :where [(custom-fn "world") ?greeting]])))
    (pd/close conn)
    (u/delete-files dir)))

(deftest pull-test
  (let [datoms [[1 :name  "Petr"]
                [1 :aka   "Devil"]
                [1 :aka   "Tupen"]
                [2 :name  "David"]
                [3 :name  "Thomas"]
                [4 :name  "Lucy"]
                [5 :name  "Elizabeth"]
                [6 :name  "Matthew"]
                [7 :name  "Eunan"]
                [8 :name  "Kerri"]
                [9 :name  "Rebecca"]
                [1 :child 2]
                [1 :child 3]
                [2 :father 1]
                [3 :father 1]
                [6 :father 3]
                [10 :name  "Part A"]
                [11 :name  "Part A.A"]
                [10 :part 11]
                [12 :name  "Part A.A.A"]
                [11 :part 12]
                [13 :name  "Part A.A.A.A"]
                [12 :part 13]
                [14 :name  "Part A.A.A.B"]
                [12 :part 14]
                [15 :name  "Part A.B"]
                [10 :part 15]
                [16 :name  "Part A.B.A"]
                [15 :part 16]
                [17 :name  "Part A.B.A.A"]
                [16 :part 17]
                [18 :name  "Part A.B.A.B"]
                [16 :part 18]]
        schema {:aka    { :db/cardinality :db.cardinality/many }
                :child  { :db/cardinality :db.cardinality/many
                         :db/valueType    :db.type/ref }
                :friend { :db/cardinality :db.cardinality/many
                         :db/valueType    :db.type/ref }
                :enemy  { :db/cardinality :db.cardinality/many
                         :db/valueType    :db.type/ref }
                :father { :db/valueType :db.type/ref }

                :part { :db/valueType  :db.type/ref
                       :db/isComponent true
                       :db/cardinality :db.cardinality/many }
                :spec { :db/valueType  :db.type/ref
                       :db/isComponent true
                       :db/cardinality :db.cardinality/one }}
        dir     (u/tmp-dir (str "pod-pull-" (UUID/randomUUID)))
        test-db (pd/init-db datoms dir schema)]
    (is (= {:name "Petr" :aka ["Devil" "Tupen"]}
           (pd/pull test-db '[:name :aka] 1)))

    (is (= {:name "Matthew" :father {:db/id 3} :db/id 6}
           (pd/pull test-db '[:name :father :db/id] 6)))

    (is (= [{:name "Petr"} {:name "Elizabeth"}
            {:name "Eunan"} {:name "Rebecca"}]
           (pd/pull-many test-db '[:name] [1 5 7 9])))
    (pd/close-db test-db)
    (u/delete-files dir)))

(deftest datoms-test
  (let [datoms (set [[1 :name "Oleg"]
                     [1 :age 17]
                     [1 :aka "x"]])
        dir    (u/tmp-dir (str "query-or-" (UUID/randomUUID)))
        db     (pd/init-db datoms dir)
        res    (set (pd/datoms db :eav))]
    (is (= datoms res))
    (pd/close-db db)
    (u/delete-files dir)))

(deftest schema-test
  (let [dir   (u/tmp-dir (str "pod-schema-test-" (UUID/randomUUID)))
        conn1 (pd/get-conn dir)
        s     {:a/b {:db/valueType :db.type/string}}
        s1    {:c/d {:db/valueType :db.type/string}}
        txs   [{:c/d "cd" :db/id -1}]
        conn2 (pd/create-conn nil s)]
    (is (= (pd/schema conn2) (pd/update-schema conn1 s)))
    (pd/update-schema conn1 s1)
    (pd/transact! conn1 txs)
    (is (not (nil? (second (first (pd/datoms (pd/db conn1) :eav))))))
    (pd/close conn1)
    (pd/close conn2)
    (u/delete-files dir)))

(deftest kv-test
  (let [dir        (u/tmp-dir (str "pod-kv-test-" (UUID/randomUUID)))
        db         (pd/open-kv dir)
        misc-table "misc-test-table"
        date-table "date-test-table"]
    (is (not (pd/closed-kv? db)))
    (is (= c/default-env-flags (pd/get-env-flags db)))
    (pd/set-env-flags db #{:nosync} true)
    (is (= (conj c/default-env-flags :nosync) (pd/get-env-flags db)))

    (pd/open-dbi db misc-table)
    (pd/open-dbi db date-table)
    (pd/transact-kv
      db
      [[:put misc-table :datalevin "Hello, world!"]
       [:put misc-table 42 {:saying "So Long, and thanks for all the fish"
                            :source "The Hitchhiker's Guide to the Galaxy"}]
       [:put date-table #inst "1991-12-25" "USSR broke apart" :instant]
       [:put date-table #inst "1989-11-09" "The fall of the Berlin Wall"
        :instant]])
    (is (= "Hello, world!" (pd/get-value db misc-table :datalevin)))
    (pd/transact-kv db [[:del misc-table 42]])
    (is (nil? (pd/get-value db misc-table 42)))
    (is (= [[#inst "1989-11-09T00:00:00.000-00:00" "The fall of the Berlin Wall"]
            [#inst "1991-12-25T00:00:00.000-00:00" "USSR broke apart"]]
           (pd/get-range db date-table [:closed (Date. 0) (Date.)] :instant)))
    (pd/close-kv db)
    (u/delete-files dir)))

;; (def sum (volatile! 0))

;; (pd/defpodfn visitor [kv]
;;   (let [^long v (b/read-buffer (l/v kv) :long)]
;;     (vswap! sum #(+ ^long %1 ^long %2) v)))

(deftest list-basic-ops-test
  (let [dir  (u/tmp-dir (str "list-test-" (UUID/randomUUID)))
        lmdb (pd/open-kv dir)]
    (pd/open-list-dbi lmdb "list")

    (pd/put-list-items lmdb "list" "a" [1 2 3 4] :string :long)
    (pd/put-list-items lmdb "list" "b" [5 6 7] :string :long)
    (pd/put-list-items lmdb "list" "c" [3 6 9] :string :long)

    (is (= (pd/entries lmdb "list") 10))

    (is (= [["a" 1] ["a" 2] ["a" 3] ["a" 4] ["b" 5] ["b" 6] ["b" 7]
            ["c" 3] ["c" 6] ["c" 9]]
           (pd/get-range lmdb "list" [:all] :string :long)))
    (is (= [["a" 1] ["a" 2] ["a" 3] ["a" 4] ["b" 5] ["b" 6] ["b" 7]]
           (pd/get-range lmdb "list" [:closed "a" "b"] :string :long)))
    (is (= [["b" 5] ["b" 6] ["b" 7]]
           (pd/get-range lmdb "list" [:closed "b" "b"] :string :long)))
    (is (= [["b" 5] ["b" 6] ["b" 7]]
           (pd/get-range lmdb "list" [:open-closed "a" "b"] :string :long)))
    (is (= [["c" 3] ["c" 6] ["c" 9] ["b" 5] ["b" 6] ["b" 7]
            ["a" 1] ["a" 2] ["a" 3] ["a" 4]]
           (pd/get-range lmdb "list" [:all-back] :string :long)))

    (is (= ["a" 1]
           (pd/get-first lmdb "list" [:closed "a" "a"] :string :long)))
    (is (= [["a" 1] ["a" 2]]
           (pd/get-first-n lmdb "list" 2 [:closed "a" "c"] :string :long)))
    (is (= [["a" 1] ["a" 2]]
           (pd/list-range-first-n lmdb "list" 2 [:closed "a" "c"] :string
                                  [:closed 1 5]:long)))

    (is (= [3 6 9]
           (pd/get-list lmdb "list" "c" :string :long)))

    (is (= [["a" 1] ["a" 2] ["a" 3] ["a" 4] ["b" 5] ["b" 6] ["b" 7]
            ["c" 3] ["c" 6] ["c" 9]]
           (pd/list-range lmdb "list" [:all] :string [:all] :long)))
    (is (= [["a" 2] ["a" 3] ["a" 4] ["c" 3]]
           (pd/list-range lmdb "list" [:closed "a" "c"] :string
                          [:closed 2 4] :long)))
    (is (= [["c" 9] ["c" 6] ["c" 3] ["b" 7] ["b" 6] ["b" 5]
            ["a" 4] ["a" 3] ["a" 2] ["a" 1]]
           (pd/list-range lmdb "list" [:all-back] :string [:all-back] :long)))
    (is (= [["c" 3]]
           (pd/list-range lmdb "list" [:at-least "b"] :string
                          [:at-most-back 4] :long)))

    (is (= [["b" 5]]
           (pd/list-range lmdb "list" [:open "a" "c"] :string
                          [:less-than 6] :long)))

    (is (= (pd/list-count lmdb "list" "a" :string) 4))
    (is (= (pd/list-count lmdb "list" "b" :string) 3))

    (is (not (pd/in-list? lmdb "list" "a" 7 :string :long)))
    (is (pd/in-list? lmdb "list" "b" 7 :string :long))

    (is (= (pd/get-list lmdb "list" "a" :string :long) [1 2 3 4]))
    (is (= (pd/get-list lmdb "list" "a" :string :long) [1 2 3 4]))

    ;; (pd/visit-list lmdb "list" visitor "a" :string)
    ;; (is (= @sum 10))

    (pd/del-list-items lmdb "list" "a" :string)

    (is (= (pd/list-count lmdb "list" "a" :string) 0))
    (is (not (pd/in-list? lmdb "list" "a" 1 :string :long)))
    (is (empty? (pd/get-list lmdb "list" "a" :string :long)))

    (pd/put-list-items lmdb "list" "b" [1 2 3 4] :string :long)

    (is (= [1 2 3 4 5 6 7]
           (pd/get-list lmdb "list" "b" :string :long)))
    (is (= (pd/list-count lmdb "list" "b" :string) 7))
    (is (pd/in-list? lmdb "list" "b" 1 :string :long))

    (pd/del-list-items lmdb "list" "b" [1 2] :string :long)

    (is (= (pd/list-count lmdb "list" "b" :string) 5))
    (is (not (pd/in-list? lmdb "list" "b" 1 :string :long)))
    (is (= [3 4 5 6 7]
           (pd/get-list lmdb "list" "b" :string :long)))

    (pd/close-kv lmdb)
    (u/delete-files dir)))

(deftest re-index-datalog-test
  (let [dir  (u/tmp-dir (str "pod-datalog-re-index-" (UUID/randomUUID)))
        conn (pd/get-conn dir {:aka  {:db/cardinality :db.cardinality/many}
                               :name {:db/valueType :db.type/string
                                      :db/unique    :db.unique/identity}})]
    (let [rp (pd/transact!
               conn
               [{:name "Frege", :db/id -1, :nation "France",
                 :aka  ["foo" "fred"]}
                {:name "Peirce", :db/id -2, :nation "france"}
                {:name "De Morgan", :db/id -3, :nation "English"}])]
      (is (= 8 (count (:tx-data rp))))
      (is (zero? (count (pd/fulltext-datoms (pd/db conn) "peirce")))))
    (let [conn1 (pd/re-index
                  conn {:name {:db/valueType :db.type/string
                               :db/unique    :db.unique/identity
                               :db/fulltext  true}} {})]
      (is (= #{["France"]}
             (pd/q '[:find ?nation
                     :in $ ?alias
                     :where
                     [?e :aka ?alias]
                     [?e :nation ?nation]]
                   (pd/db conn1)
                   "fred")))
      (pd/transact! conn1 [[:db/retract 1 :name "Frege"]])
      (is (= [[{:db/id 1, :aka ["foo" "fred"], :nation "France"}]]
             (pd/q '[:find (pull ?e [*])
                     :in $ ?alias
                     :where
                     [?e :aka ?alias]]
                   (pd/db conn1)
                   "fred")))
      (is (= 1 (count (pd/fulltext-datoms (pd/db conn1) "peirce"))))
      (pd/close conn1))
    (u/delete-files dir)))

(deftest issue-283-test
  (let [dir  (u/tmp-dir (str "pod-issue-283-test-" (UUID/randomUUID)))
        conn (pd/create-conn
               dir {:account/username {:db/valueType :db.type/string
                                       :db/unique    :db.unique/identity}
                    :account/name     {:db/valueType :db.type/string}})]
    (pd/transact! conn [{:account/username "teodorlu"
                         :account/name     "Teodor"}])

    (is (= {:account/username "teodorlu", :account/name "Teodor"}
           (select-keys (pd/entity (pd/db conn) [:account/username "teodorlu"])
                        [:account/username :account/name])))
    (pd/close conn)
    (u/delete-files dir)))

(deftest search-test
  (let [dir    (u/tmp-dir (str "pod-search-test-" (UUID/randomUUID)))
        lmdb   (pd/open-kv dir)
        engine (pd/new-search-engine lmdb {:index-position? true})
        docs   {1 "The quick red fox jumped over the lazy red dogs."
                2 "Mary had a little lamb whose fleece was red as fire."
                3 "Moby Dick is a story of a whale and a man obsessed."}]
    (pd/add-doc engine 1 (docs 1))
    (pd/add-doc engine 2 (docs 2))
    (pd/add-doc engine 3 (docs 3))

    (is (= [1 2] (pd/search engine "red")))
    (is (= [[1 [["red" [10 39]]]] [2 [["red" [40]]]]]
           (pd/search engine "red" {:display :offsets})))
    (pd/close-kv lmdb)
    (u/delete-files dir)))

(deftest transact-async-test
  (let [dir  (u/tmp-dir (str "transact-async-test-" (UUID/randomUUID)))
        conn (pd/get-conn dir {:aka  {:db/cardinality :db.cardinality/many}
                               :name {:db/valueType :db.type/string
                                      :db/unique    :db.unique/identity}})]
    (pd/transact-async
      conn
      [{:name "Frege", :db/id -1, :nation "France",
        :aka  ["foo" "fred"]}
       {:name "Peirce", :db/id -2, :nation "france"}
       {:name "De Morgan", :db/id -3, :nation "English"}]
      nil (fn [rp] (is (= 8 (count (:tx-data rp))))))
    (is (= #{["France"]}
           (pd/q '[:find ?nation
                   :in $ ?alias
                   :where
                   [?e :aka ?alias]
                   [?e :nation ?nation]]
                 (pd/db conn)
                 "fred")))
    (pd/transact-async
      conn
      [[:db/retract 1 :name "Frege"]]
      nil identity)
    (is (= [[{:db/id 1, :aka ["foo" "fred"], :nation "France"}]]
           (pd/q '[:find (pull ?e [*])
                   :in $ ?alias
                   :where
                   [?e :aka ?alias]]
                 (pd/db conn)
                 "fred")))
    (is (= #{} (pd/q '[:find ?dummy
                       :in $
                       :where
                       [?e :dummy ?dummy]] ;; non-existent attr
                     (pd/db conn))))
    (pd/close conn)
    (u/delete-files dir)))

(deftest transact-kv-async-test
  (let [dir (u/tmp-dir (str "pod-kv-async-test-" (UUID/randomUUID)))
        db  (pd/open-kv dir)
        dbi "a"]
    (pd/open-dbi db dbi)
    (pd/transact-kv-async
      db dbi
      [[:put "hello" "world"]]
      :string :string
      (fn [res] (is (= res :transacted))))
    (is (= "world" (pd/get-value db dbi "hello" :string :string)))
    (pd/close-kv db)
    (u/delete-files dir)))

(def random (Random.))

(defn- rand-float-vec
  [^long n]
  (let [v (float-array n)]
    (dotimes [i n] (aset v i (.nextFloat ^Random random)))
    v))

(deftest vector-test
  (let [dir   (u/tmp-dir (str "pod-vector-test-" (UUID/randomUUID)))
        db    (pd/open-kv dir)
        n     200
        v1    (rand-float-vec n)
        v2    (rand-float-vec n)
        index (pd/new-vector-index db {:dimensions n})
        info  (pd/vector-index-info index)]
    (is (= (info :size) 0))
    (is (= (info :capacity) 0))
    (is (<= 0 (info :memory)))
    (is (string? (info :hardware)))
    (is (= (info :dimensions) n))
    (is (= (info :metric-type) c/default-metric-type))
    (is (= (info :quantization) c/default-quantization))
    (is (= (info :connectivity) c/default-connectivity))
    (is (= (info :expansion-add) c/default-expansion-add))
    (is (= (info :expansion-search) c/default-expansion-search))

    (pd/add-vec index :ok v1)
    (let [info1 (pd/vector-index-info index)]
      (is (= (info1 :size) 1))
      (is (<= 1 (info1 :capacity)))
      (is (<= 1 (info1 :memory))))
    (is (= [:ok] (pd/search-vec index v1)))
    (is (= [[:ok 0.0]] (pd/search-vec index v1 {:display :refs+dists})))

    (pd/close-vector-index index)
    (pd/close-vector-index index) ;; close should be idempotent

    (let [index1 (pd/new-vector-index db {:dimensions n})
          info1  (pd/vector-index-info index1)]
      (is (= (info1 :size) 1))
      (is (= [:ok] (pd/search-vec index1 v1)))
      (is (= [[:ok 0.0]] (pd/search-vec index1 v1 {:display :refs+dists})))

      (pd/add-vec index1 :nice v2)
      (let [info2 (pd/vector-index-info index1)]
        (is (= 2 (info2 :size))))
      (is (= [:nice] (pd/search-vec index1 v2 {:top 1})))
      (is (= [[:nice 0.0]] (pd/search-vec index1 v2 {:top 1 :display :refs+dists})))
      (is (= [:nice :ok] (pd/search-vec index1 v2)))

      (pd/remove-vec index1 :ok)
      (is (= 1 (:size (pd/vector-index-info index1))))
      (is (= [:nice] (pd/search-vec index1 v2)))
      (is (= [[:nice 0.0]] (pd/search-vec index1 v2 {:display :refs+dists})))

      (pd/close-vector-index index1))

    (let [index2 (pd/new-vector-index db {:dimensions n})]
      (is (= 1 (:size (pd/vector-index-info index2))))
      (is (= [:nice] (pd/search-vec index2 v2)))
      (is (= [[:nice 0.0]] (pd/search-vec index2 v2 {:display :refs+dists})))
      (pd/clear-vector-index index2))

    (let [index3 (pd/new-vector-index db {:dimensions n})]
      (is (= 0 (:size (pd/vector-index-info index3))))
      (pd/close-vector-index index3))
    (pd/close-kv db)
    (u/delete-files dir)))

(deftest vector-re-index-test
  (when-not (u/windows?)
    (let [dir   (u/tmp-dir (str "pod-vector-test-" (UUID/randomUUID)))
          lmdb  (pd/open-kv dir)
          n     800
          v1    (rand-float-vec n)
          v2    (rand-float-vec n)
          v3    (rand-float-vec n)
          index (pd/new-vector-index lmdb {:dimensions n})
          info  (pd/vector-index-info index)]
      (pd/add-vec index 1 v1)
      (pd/add-vec index 2 v2)
      (pd/add-vec index 3 v3)
      (let [new-index (pd/re-index index {:dimensions   n
                                          :connectivity 32
                                          :metric-type  :cosine})
            new-info  (pd/vector-index-info new-index)]
        (is (= (new-info :size) 3))
        (is (<= 3 (new-info :capacity)))
        (is (<= 0 (new-info :memory)))
        (is (= (info :hardware) (new-info :hardware)))
        (is (= (info :filename) (new-info :filename)))
        (is (= (info :dimensions) (new-info :dimensions)))
        (is (= (new-info :metric-type) :cosine))
        (is (= (info :quantization) (new-info :quantization)))
        (is (= (new-info :connectivity) 32))
        (is (= (info :expansion-add) (new-info :expansion-add)))
        (is (= (info :expansion-search) (new-info :expansion-search)))

        (is (= [1] (pd/search-vec new-index v1 {:top 1})))
        (is (= [2] (pd/search-vec new-index v2 {:top 1})))
        (is (= [3] (pd/search-vec new-index v3 {:top 1})))
        (pd/close-vector-index new-index))

      (pd/close-kv lmdb)
      (u/delete-files dir))))
