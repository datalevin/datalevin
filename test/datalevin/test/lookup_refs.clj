(ns datalevin.test.lookup-refs
  (:require
   [datalevin.test.core :as tdc :refer [db-fixture]]
   [clojure.test :refer [deftest testing is are use-fixtures]]
   [datalevin.core :as d]
   [datalevin.util :as u])
  (:import [clojure.lang ExceptionInfo]
           [java.util UUID]))

(use-fixtures :each db-fixture)

(deftest test-lookup-refs
  (let [dir (u/tmp-dir (str "query-or-" (UUID/randomUUID)))
        db  (d/db-with
              (d/empty-db dir {:name  { :db/unique :db.unique/identity }
                               :email { :db/unique :db.unique/value }})
              [{:db/id 1 :name "Ivan" :email "@1" :age 35}
               {:db/id 2 :name "Petr" :email "@2" :age 22}])]

    (are [eid res] (= (tdc/entity-map db eid) res)
      [:name "Ivan"]   {:db/id 1 :name "Ivan" :email "@1" :age 35}
      [:email "@1"]    {:db/id 1 :name "Ivan" :email "@1" :age 35}
      [:name "Sergey"] nil
      [:name nil]      nil)

    (are [eid msg] (thrown-msg? msg (d/entity db eid))
      [:name]     "Lookup ref should contain 2 elements: [:name]"
      [:name 1 2] "Lookup ref should contain 2 elements: [:name 1 2]"
      [:age 10]   "Lookup ref attribute should be marked as :db/unique: [:age 10]")
    (d/close-db db)
    (u/delete-files dir)))

(deftest test-lookup-refs-transact-1
  (let [dir (u/tmp-dir (str "query-or-" (UUID/randomUUID)))
        db  (d/db-with
              (d/empty-db dir {:name   { :db/unique :db.unique/identity }
                               :friend { :db/valueType :db.type/ref }})
              [{:db/id 1 :name "Ivan"}
               {:db/id 2 :name "Petr"}])]
    (are [tx res] (= res (tdc/entity-map (d/db-with db tx) 1))
      ;; Additions
      [[:db/add [:name "Ivan"] :age 35]]
      {:db/id 1 :name "Ivan" :age 35}

      [{:db/id [:name "Ivan"] :age 35}]
      {:db/id 1 :name "Ivan" :age 35})

    (are [tx msg] (thrown-msg? msg (d/db-with db tx))
      [{:db/id [:name "Oleg"], :age 10}]
      "Nothing found for entity id [:name \"Oleg\"]"

      [[:db/add [:name "Oleg"] :age 10]]
      "Nothing found for entity id [:name \"Oleg\"]")
    (d/close-db db)
    (u/delete-files dir)))

(deftest test-lookup-refs-transact-2
  (let [dir (u/tmp-dir (str "query-or-" (UUID/randomUUID)))
        db  (d/db-with
              (d/empty-db dir {:name   { :db/unique :db.unique/identity }
                               :friend { :db/valueType :db.type/ref }})
              [{:db/id 1 :name "Ivan"}
               {:db/id 2 :name "Petr"}])]
    (are [tx res] (= res (tdc/entity-map (d/db-with db tx) 1))
      ;; Additions
      [[:db/add 1 :friend [:name "Petr"]]]
      {:db/id 1 :name "Ivan" :friend {:db/id 2}}

      [[:db/add 1 :friend [:name "Petr"]]]
      {:db/id 1 :name "Ivan" :friend {:db/id 2}}

      [{:db/id 1 :friend [:name "Petr"]}]
      {:db/id 1 :name "Ivan" :friend {:db/id 2}}

      [{:db/id 2 :_friend [:name "Ivan"]}]
      {:db/id 1 :name "Ivan" :friend {:db/id 2}})
    (d/close-db db)
    (u/delete-files dir)))

(deftest test-lookup-refs-transact-3
  (let [dir (u/tmp-dir (str "query-or-" (UUID/randomUUID)))
        db  (d/db-with
              (d/empty-db dir {:name   { :db/unique :db.unique/identity }
                               :friend { :db/valueType :db.type/ref }})
              [{:db/id 1 :name "Ivan"}
               {:db/id 2 :name "Petr"}])]
    (are [tx res] (= res (tdc/entity-map (d/db-with db tx) 1))
      ;; lookup refs are resolved at intermediate DB value
      [[:db/add 3 :name "Oleg"]
       [:db/add 1 :friend [:name "Oleg"]]]
      {:db/id 1 :name "Ivan" :friend {:db/id 3}})
    (d/close-db db)
    (u/delete-files dir)))

(deftest test-lookup-refs-transact-4
  (let [dir (u/tmp-dir (str "query-or-" (UUID/randomUUID)))
        db  (d/db-with
              (d/empty-db dir {:name   { :db/unique :db.unique/identity }
                               :friend { :db/valueType :db.type/ref }})
              [{:db/id 1 :name "Ivan"}
               {:db/id 2 :name "Petr"}])]
    (are [tx res] (= res (tdc/entity-map (d/db-with db tx) 1))
      ;; CAS
      [[:db.fn/cas [:name "Ivan"] :name "Ivan" "Oleg"]]
      {:db/id 1 :name "Oleg"})
    (d/close-db db)
    (u/delete-files dir)))

(deftest test-lookup-refs-transact-5
  (let [dir (u/tmp-dir (str "query-or-" (UUID/randomUUID)))
        db  (d/db-with
              (d/empty-db dir {:name   { :db/unique :db.unique/identity }
                               :friend { :db/valueType :db.type/ref }})
              [{:db/id 1 :name "Ivan"}
               {:db/id 2 :name "Petr"}])]
    (are [tx res] (= res (tdc/entity-map (d/db-with db tx) 1))

      [[:db/add 1 :friend 1]
       [:db.fn/cas 1 :friend [:name "Ivan"] 2]]
      {:db/id 1 :name "Ivan" :friend {:db/id 2}})
    (d/close-db db)
    (u/delete-files dir)))

(deftest test-lookup-refs-transact-6
  (let [dir (u/tmp-dir (str "query-or-" (UUID/randomUUID)))
        db  (d/db-with
              (d/empty-db dir {:name   { :db/unique :db.unique/identity }
                               :friend { :db/valueType :db.type/ref }})
              [{:db/id 1 :name "Ivan"}
               {:db/id 2 :name "Petr"}])]
    (are [tx res] (= res (tdc/entity-map (d/db-with db tx) 1))

      [[:db/add 1 :friend 1]
       [:db.fn/cas 1 :friend 1 [:name "Petr"]]]
      {:db/id 1 :name "Ivan" :friend {:db/id 2}})
    (d/close-db db)
    (u/delete-files dir)))

(deftest test-lookup-refs-transact-7
  (let [dir (u/tmp-dir (str "query-or-" (UUID/randomUUID)))
        db  (d/db-with
              (d/empty-db dir {:name   { :db/unique :db.unique/identity }
                               :friend { :db/valueType :db.type/ref }})
              [{:db/id 1 :name "Ivan"}
               {:db/id 2 :name "Petr"}])]
    (are [tx res] (= res (tdc/entity-map (d/db-with db tx) 1))

      ;; Retractions
      [[:db/add 1 :age 35]
       [:db/retract [:name "Ivan"] :age 35]]
      {:db/id 1 :name "Ivan"}

      [[:db/add 1 :friend 2]
       [:db/retract 1 :friend [:name "Petr"]]]
      {:db/id 1 :name "Ivan"})
    (d/close-db db)
    (u/delete-files dir)))

(deftest test-lookup-refs-transact-8
  (let [dir (u/tmp-dir (str "query-or-" (UUID/randomUUID)))
        db  (d/db-with
              (d/empty-db dir {:name   { :db/unique :db.unique/identity }
                               :friend { :db/valueType :db.type/ref }})
              [{:db/id 1 :name "Ivan"}
               {:db/id 2 :name "Petr"}])]
    (are [tx res] (= res (tdc/entity-map (d/db-with db tx) 1))

      [[:db/add 1 :age 35]
       [:db.fn/retractAttribute [:name "Ivan"] :age]]
      {:db/id 1 :name "Ivan"}

      [[:db.fn/retractEntity [:name "Ivan"]]]
      {:db/id 1})
    (d/close-db db)
    (u/delete-files dir)))

(deftest test-lookup-refs-transact-multi-1
  (let [dir (u/tmp-dir (str "query-or-" (UUID/randomUUID)))
        db  (d/db-with
              (d/empty-db dir {:name    { :db/unique :db.unique/identity }
                               :friends { :db/valueType  :db.type/ref
                                         :db/cardinality :db.cardinality/many }})
              [{:db/id 1 :name "Ivan"}
               {:db/id 2 :name "Petr"}
               {:db/id 3 :name "Oleg"}
               {:db/id 4 :name "Sergey"}])]
    (are [tx res] (= (tdc/entity-map (d/db-with db tx) 1) res)
      ;; Additions
      [[:db/add 1 :friends [:name "Petr"]]]
      {:db/id 1 :name "Ivan" :friends #{{:db/id 2}}}

      [[:db/add 1 :friends [:name "Petr"]]
       [:db/add 1 :friends [:name "Oleg"]]]
      {:db/id 1 :name "Ivan" :friends #{{:db/id 2} {:db/id 3}}})
    (d/close-db db)
    (u/delete-files dir)))

(deftest test-lookup-refs-transact-multi-2
  (let [dir (u/tmp-dir (str "query-or-" (UUID/randomUUID)))
        db  (d/db-with
              (d/empty-db dir {:name    { :db/unique :db.unique/identity }
                               :friends { :db/valueType  :db.type/ref
                                         :db/cardinality :db.cardinality/many }})
              [{:db/id 1 :name "Ivan"}
               {:db/id 2 :name "Petr"}
               {:db/id 3 :name "Oleg"}
               {:db/id 4 :name "Sergey"}])]
    (are [tx res] (= (tdc/entity-map (d/db-with db tx) 1) res)
      ;; Additions

      [{:db/id 1 :friends [:name "Petr"]}]
      {:db/id 1 :name "Ivan" :friends #{{:db/id 2}}}

      [{:db/id 1 :friends [[:name "Petr"]]}]
      {:db/id 1 :name "Ivan" :friends #{{:db/id 2}}}

      [{:db/id 1 :friends [[:name "Petr"] [:name "Oleg"]]}]
      {:db/id 1 :name "Ivan" :friends #{{:db/id 2} {:db/id 3}}}

      [{:db/id 1 :friends [2 [:name "Oleg"]]}]
      {:db/id 1 :name "Ivan" :friends #{{:db/id 2} {:db/id 3}}}

      [{:db/id 1 :friends [[:name "Petr"] 3]}]
      {:db/id 1 :name "Ivan" :friends #{{:db/id 2} {:db/id 3}}})
    (d/close-db db)
    (u/delete-files dir)))

(deftest test-lookup-refs-transact-multi-3
  (let [dir (u/tmp-dir (str "query-or-" (UUID/randomUUID)))
        db  (d/db-with
              (d/empty-db dir {:name    { :db/unique :db.unique/identity }
                               :friends { :db/valueType  :db.type/ref
                                         :db/cardinality :db.cardinality/many }})
              [{:db/id 1 :name "Ivan"}
               {:db/id 2 :name "Petr"}
               {:db/id 3 :name "Oleg"}
               {:db/id 4 :name "Sergey"}])]
    (are [tx res] (= (tdc/entity-map (d/db-with db tx) 1) res)

      ;; reverse refs
      [{:db/id 2 :_friends [:name "Ivan"]}]
      {:db/id 1 :name "Ivan" :friends #{{:db/id 2}}}

      [{:db/id 2 :_friends [[:name "Ivan"]]}]
      {:db/id 1 :name "Ivan" :friends #{{:db/id 2}}}

      [{:db/id 2 :_friends [[:name "Ivan"] [:name "Oleg"]]}]
      {:db/id 1 :name "Ivan" :friends #{{:db/id 2}}})
    (d/close-db db)
    (u/delete-files dir)))

(deftest lookup-refs-index-access
  (let [dir (u/tmp-dir (str "query-or-" (UUID/randomUUID)))
        db  (d/db-with
              (d/empty-db dir {:name    { :db/unique :db.unique/identity }
                               :friends { :db/valueType  :db.type/ref
                                         :db/cardinality :db.cardinality/many}})
              [{:db/id 1 :name "Ivan" :friends [2 3]}
               {:db/id 2 :name "Petr" :friends 3}
               {:db/id 3 :name "Oleg"}])]
    (are [index attrs datoms]
        (= (set (map (juxt :e :a :v) (apply d/datoms db index attrs)))
           (set datoms))
      :eav [[:name "Ivan"]]
      [[1 :friends 2] [1 :friends 3] [1 :name "Ivan"]]

      :eav [[:name "Ivan"] :friends]
      [[1 :friends 2] [1 :friends 3]]

      :eav [[:name "Ivan"] :friends [:name "Petr"]]
      [[1 :friends 2]]

      :ave [:friends [:name "Oleg"]]
      [[1 :friends 3] [2 :friends 3]]

      :ave [:friends [:name "Oleg"] [:name "Ivan"]]
      [[1 :friends 3]]
      )

    (are [index attrs resolved-attrs] (= (vec (apply d/seek-datoms db index attrs))
                                         (vec (apply d/seek-datoms db index resolved-attrs)))
      ;; :eav [[:name "Ivan"]] [1]
      :eav [[:name "Ivan"] :name]                   [1 :name]
      :eav [[:name "Ivan"] :friends [:name "Oleg"]] [1 :friends 3]

      :ave [:friends [:name "Oleg"]]                [:friends 3]
      :ave [:friends [:name "Oleg"] [:name "Petr"]] [:friends 3 2]
      )

    (are [attr start end datoms] (= (map (juxt :e :a :v) (d/index-range db attr start end)) datoms)
      :friends [:name "Oleg"] [:name "Oleg"]
      [[1 :friends 3] [2 :friends 3]]

      :friends [:name "Petr"] [:name "Petr"]
      [[1 :friends 2]]

      :friends [:name "Petr"] [:name "Oleg"]
      [[1 :friends 2] [1 :friends 3] [2 :friends 3]])
    (d/close-db db)
    (u/delete-files dir)))

(deftest lookup-ref-cache-invalidation-test
  (let [dir    (u/tmp-dir (str "lookup-ref-cache-" (UUID/randomUUID)))
        schema {:index/case  {:db/valueType :db.type/long}
                :index/key   {:db/valueType :db.type/string
                              :db/unique :db.unique/identity}
                :index/name  {:db/valueType :db.type/string}
                :index/value {:db/valueType :db.type/long}
                :index/ref   {:db/valueType :db.type/ref}
                :index/tags  {:db/valueType :db.type/string
                              :db/cardinality :db.cardinality/many}}
        norm   (fn [db datoms]
                 (into #{}
                       (map (fn [datom]
                              [(:a datom)
                               (if (= :index/ref (:a datom))
                                 (:index/key (d/entity db (:v datom)))
                                 (:v datom))]))
                       datoms))]
    (d/with-conn [conn dir schema]
      (d/transact! conn
                   [{:db/id "root"
                     :index/case 1
                     :index/key "index-00000001-root"
                     :index/name "root-index-00000001"
                     :index/value 1
                     :index/tags ["index-00000001-tag-a"
                                  "index-00000001-tag-b"]
                     :index/ref "child-a"}
                    {:db/id "child-a"
                     :index/case 1
                     :index/key "index-00000001-child-a"
                     :index/name "child-a-index-00000001"}])

      ;; Prime caches using lookup refs; the second transaction must invalidate
      ;; these slices even though the cached key stores unresolved entity ids.
      (d/datoms @conn :eav [:index/key "index-00000001-root"])
      (d/datoms @conn :ave :index/ref [:index/key "index-00000001-child-a"])

      (d/transact! conn
                   [{:db/id "child-b"
                     :index/case 1
                     :index/key "index-00000001-child-b"
                     :index/name "child-b-index-00000001"}
                    [:db/retract [:index/key "index-00000001-root"]
                     :index/tags "index-00000001-tag-a"]
                    [:db/add [:index/key "index-00000001-root"]
                     :index/tags "index-00000001-tag-c"]
                    [:db/retract [:index/key "index-00000001-root"]
                     :index/ref [:index/key "index-00000001-child-a"]]
                    [:db/add [:index/key "index-00000001-root"]
                     :index/ref "child-b"]
                    [:db/cas [:index/key "index-00000001-root"] :index/value 1 2]])

      (is (= #{[:index/case 1]
               [:index/key "index-00000001-root"]
               [:index/name "root-index-00000001"]
               [:index/value 2]
               [:index/ref "index-00000001-child-b"]
               [:index/tags "index-00000001-tag-b"]
               [:index/tags "index-00000001-tag-c"]}
             (norm @conn
                   (d/datoms @conn :eav [:index/key "index-00000001-root"]))))

      (is (= #{}
             (into #{} (d/datoms @conn :ave :index/ref
                                 [:index/key "index-00000001-child-a"]))))

      (is (= #{[:index/ref "index-00000001-child-b"]}
             (norm @conn
                   (d/datoms @conn :ave :index/ref
                             [:index/key "index-00000001-child-b"])))))
    (u/delete-files dir)))

(deftest test-lookup-refs-query
  (let [schema {:name   { :db/unique :db.unique/identity }
                :friend { :db/valueType :db.type/ref }}
        dir    (u/tmp-dir (str "lookup-query-" (UUID/randomUUID)))
        db     (d/db-with (d/empty-db dir schema)
                          [{:db/id 1 :id 1 :name "Ivan" :age 11 :friend 2}
                           {:db/id 2 :id 2 :name "Petr" :age 22 :friend 3}
                           {:db/id 3 :id 3 :name "Oleg" :age 33 }])]

    (is (= (set (d/q '[:find ?e ?v
                       :in $ ?e
                       :where [?e :age ?v]]
                     db [:name "Ivan"]))
           #{[[:name "Ivan"] 11]}))

    (is (= (set (d/q '[:find ?v
                       :in $ ?e
                       :where [?e :age ?v]]
                     db [:name "Ivan"]))
           #{[11]}))

    (is (= (set (d/q '[:find [?v ...]
                       :in $ [?e ...]
                       :where [?e :age ?v]]
                     db [[:name "Ivan"] [:name "Petr"]]))
           #{11 22}))

    (is (= (set (d/q '[:find [?e ...]
                       :in $ ?v
                       :where [?e :friend ?v]]
                     db [:name "Petr"]))
           #{1}))

    (is (= (set (d/q '[:find [?e ...]
                       :in $ [?v ...]
                       :where [?e :friend ?v]]
                     db [[:name "Petr"] [:name "Oleg"]]))
           #{1 2}))

    (is (= (d/q '[:find ?e ?v
                  :in $ ?e ?v
                  :where
                  [?e :friend ?v]]
                db [:name "Ivan"] [:name "Petr"])
           #{[[:name "Ivan"] [:name "Petr"]]}))

    (is (= (d/q '[:find ?e ?v
                  :in $ [?e ...] [?v ...]
                  :where [?e :friend ?v]]
                db [[:name "Ivan"] [:name "Petr"] [:name "Oleg"]]
                [[:name "Ivan"] [:name "Petr"] [:name "Oleg"]])
           #{[[:name "Ivan"] [:name "Petr"]]
             [[:name "Petr"] [:name "Oleg"]]}))

    ;; https://github.com/tonsky/datalevin/issues/214
    (is (= (d/q '[:find ?e
                  :in $ [?e ...]
                  :where [?e :friend 3]]
                db [1 2 3 "A"])
           #{[2]}))

    (let [db2 (d/db-with (d/empty-db nil schema)
                         [{:db/id 3 :name "Ivan" :id 3}
                          {:db/id 1 :name "Petr" :id 1}
                          {:db/id 2 :name "Oleg" :id 2}])]
      (is (= (d/q '[:find ?e ?e1 ?e2
                    :in $1 $2 [?e ...]
                    :where [$1 ?e :id ?e1]
                    [$2 ?e :id ?e2]]
                  db db2 [[:name "Ivan"] [:name "Petr"] [:name "Oleg"]])
             #{[[:name "Ivan"] 1 3]
               [[:name "Petr"] 2 1]
               [[:name "Oleg"] 3 2]})))

    (testing "inline refs"
      (is (= (d/q '[:find ?v
                    :where [[:name "Ivan"] :friend ?v]]
                  db)
             #{[2]}))

      (is (= (d/q '[:find ?e
                    :where [?e :friend [:name "Petr"]]]
                  db)
             #{[1]}))

      (is (thrown-msg? "Nothing found for entity id [:name \"Valery\"]"
                       (d/q '[:find ?e
                              :where [[:name "Valery"] :friend ?e]]
                            db))))
    (d/close-db db)
    (u/delete-files dir)))

(deftest data-lookup-refs-test
  (let [dir (u/tmp-dir (str "issue-194-" (UUID/randomUUID)))
        db  (d/db-with
              (d/empty-db dir
                          {:ref-id    {:db/valueType :db.type/ref}
                           :lookup-id {:db/unique :db.unique/identity}})
              [{:a 0, :lookup-id [0 0]}
               {:a 0, :lookup-id [0 1]}
               {:a 0, :lookup-id [1 0]}
               {:a 0, :lookup-id [1 1]}
               {:ref-id [:lookup-id [0 1]]}])]
    (is (= 9 (count (d/datoms db :eav))))
    (d/close-db db)
    (u/delete-files dir)))
