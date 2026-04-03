(ns datalevin.test.entity
  (:require
   [clojure.edn :as edn]
   [clojure.test :as t :refer [is deftest testing]]
   [datalevin.core :as d]
   [datalevin.db :as ddb]
   [datalevin.util :as u]
   [datalevin.test.core :as tdc])
  (:import [java.util UUID]))

(t/use-fixtures :once tdc/no-namespace-maps)

(deftest test-entity
  (let [dir (u/tmp-dir (str "entity-test-" (UUID/randomUUID)))
        db  (-> (d/empty-db dir {:aka {:db/cardinality :db.cardinality/many}})
                (d/db-with [{:db/id 1, :name "Ivan", :age 19, :aka ["X" "Y"]}
                            {:db/id 2, :name "Ivan", :sex "male", :aka ["Z"]}
                            [:db/add 3 :huh? false]]))
        e   (d/entity db 1)]
    (is (= (:db/id e) 1))
    (is (identical? (d/entity-db e) db))
    (is (= (:name e) "Ivan"))
    (is (= (e :name) "Ivan"))                               ; IFn form
    (is (= (:age e) 19))
    (is (= (:aka e) #{"X" "Y"}))
    (is (= true (contains? e :age)))
    (is (= false (contains? e :not-found)))
    (is (= (into {} e)
           {:name "Ivan", :age 19, :aka #{"X" "Y"}}))
    (is (= (into {} (d/entity db 1))
           {:name "Ivan", :age 19, :aka #{"X" "Y"}}))
    (is (= (into {} (d/entity db 2))
           {:name "Ivan", :sex "male", :aka #{"Z"}}))
    (let [e3 (d/entity db 3)]
      (is (= (into {} e3) {:huh? false}))                   ; Force caching.
      (is (false? (:huh? e3))))

    (is (= (pr-str (d/entity db 1)) "{:db/id 1}"))
    (is (= (pr-str (let [e (d/entity db 1)] (:unknown e) e)) "{:db/id 1}"))
    ;; read back in to account for unordered-ness
    (is (= (edn/read-string (pr-str (let [e (d/entity db 1)] (:name e) e)))
           (edn/read-string "{:name \"Ivan\", :db/id 1}")))
    (d/close-db db)
    (u/delete-files dir)))

(deftest test-transactable-entity
  (let [dir (u/tmp-dir (str "query-or-" (UUID/randomUUID)))
        db  (-> (d/empty-db
                  dir {:user/handle  #:db {:valueType :db.type/string
                                           :unique    :db.unique/identity}
                       :user/friends #:db{:valueType   :db.type/ref
                                          :cardinality :db.cardinality/many}}
                  {:auto-entity-time? true})
                (d/db-with [{:user/handle  "ava"
                             :user/friends [{:user/handle "fred"}
                                            {:user/handle "jane"}]}]))
        ava (d/entity db [:user/handle "ava"])]
    (testing "cardinality/one"
      (testing "nil attr"
        (is (nil? (:user/age ava))))
      (testing "add/assoc attr"
        (let [ava-with-age (assoc ava :user/age 42)]
          (is (= 42 (:user/age ava-with-age)) "lookup works on tx stage")
          (is (nil? (:user/age ava)) "is immutable")
          (testing "and transact"
            (let [db-ava-with-age        (d/db-with db [(-> ava-with-age
                                                            (d/add :user/foo "bar"))])
                  ava-db-entity-with-age (d/entity db-ava-with-age [:user/handle "ava"])]
              (is (= 42 (:user/age ava-db-entity-with-age)) "value was transacted into db")
              (is (= "bar" (:user/foo ava-db-entity-with-age)) "value was transacted into db")
              (testing "update attr"
                (let [ava-with-age    (update ava-db-entity-with-age :user/age inc)
                      ava-with-points (-> ava-with-age
                                          (assoc :user/points 100)
                                          (update :user/points inc))]
                  (is (= 43 (:user/age ava-with-age)) "update works on entity")
                  (is (= 101 (:user/points ava-with-points)) "update works on stage")
                  (testing "and transact"
                    (let [db-ava-with-age (d/db-with db [ava-with-points])
                          ava-db-entity   (d/entity db-ava-with-age [:user/handle "ava"])]
                      (is (= 43 (:user/age ava-db-entity)) "value was transacted into db")
                      (is (= 101 (:user/points ava-db-entity)) "value was transacted into db")))))))))
      (testing "retract/dissoc attr"
        (let [ava        (d/entity db [:user/handle "ava"])
              dissoc-age (-> ava
                             (dissoc :user/age)
                             (d/retract :user/foo))]
          (is (= 43 (:user/age ava)) "has age")
          (is (nil? (:user/age dissoc-age)))
          (is (nil? (:user/foo dissoc-age)))
          (testing "and transact"
            (let [db-ava-with-age      (d/db-with db [(dissoc ava :user/age)])
                  ava-db-entity-no-age (d/entity db-ava-with-age [:user/handle "ava"])]
              (is (nil? (:user/age ava-db-entity-no-age)) "attrs was retracted from db"))))))

    (testing "cardinality/many"
      (testing "add/retract"
        (let [find-fred             (fn [ent]
                                      (some
                                        #(when (= (:user/handle %) "fred") %)
                                        (:user/friends ent)))
              fred                  (find-fred ava)
              ava-no-fred-friend    (d/retract ava :user/friends fred)
              db-no-fred            (d/db-with db [ava-no-fred-friend])
              ava-db-no-fred-friend (d/entity db-no-fred [:user/handle "ava"])]
          (is (some? fred) "fred is a friend")
          (is (nil? (find-fred ava-db-no-fred-friend)) "fred is not a friend anymore :(")
          ;; ava and fred make up
          (let [ava-friends-with-fred (d/add ava-db-no-fred-friend :user/friends fred)]
                                        ; tx-stage does not handle cardinality properly yet:
                                        ;(is (some? (find-fred ava-friends-with-fred))) ;; fails
            (let [db-with-friends (d/db-with db [ava-friends-with-fred])
                  ava             (d/entity db-with-friends [:user/handle "ava"])]
              (is (some? (find-fred ava)) "officially friends again"))))))

    (d/close-db db)
    (u/delete-files dir)))

(deftest test-entity-refs
  (let [dir (u/tmp-dir (str "entity-" (UUID/randomUUID)))
        db  (-> (d/empty-db dir {:father   {:db/valueType :db.type/ref}
                                 :children {:db/valueType   :db.type/ref
                                            :db/cardinality :db.cardinality/many}})
                (d/db-with
                  [{:db/id 1, :children [10]}
                   {:db/id 10, :father 1, :children [100 101]}
                   {:db/id 100, :father 10}
                   {:db/id 101, :father 10}]))
        e   #(d/entity db %)]

    (is (= (:children (e 1)) #{(e 10)}))
    (is (= (:children (e 10)) #{(e 100) (e 101)}))

    (testing "empty attribute"
      (is (= (:children (e 100)) nil)))

    (testing "nested navigation"
      (is (= (-> (e 1) :children first :children) #{(e 100) (e 101)}))
      (is (= (-> (e 10) :children first :father) (e 10)))
      (is (= (-> (e 10) :father :children) #{(e 10)}))

      (testing "after touch"
        (let [e1  (e 1)
              e10 (e 10)]
          (d/touch e1)
          (d/touch e10)
          (is (= (-> e1 :children first :children) #{(e 100) (e 101)}))
          (is (= (-> e10 :children first :father) (e 10)))
          (is (= (-> e10 :father :children) #{(e 10)})))))

    (testing "backward navigation"
      (is (= (:_children (e 1)) nil))
      (is (= (:_father (e 1)) #{(e 10)}))
      (is (= (:_children (e 10)) #{(e 1)}))
      (is (= (:_father (e 10)) #{(e 100) (e 101)}))
      (is (= (-> (e 100) :_children first :_children) #{(e 1)}))
      )
    (d/close-db db)
    (u/delete-files dir)))

(deftest test-entity-misses
  (let [dir (u/tmp-dir (str "query-or-" (UUID/randomUUID)))
        db  (-> (d/empty-db dir {:name {:db/unique :db.unique/identity}})
                (d/db-with [{:db/id 1, :name "Ivan"}
                            {:db/id 2, :name "Oleg"}]))]
    (is (nil? (d/entity db nil)))
    (is (nil? (d/entity db "abc")))
    (is (nil? (d/entity db :keyword)))
    (is (nil? (d/entity db [:name "Petr"])))
    (is (= 777 (:db/id (d/entity db 777))))
    (is (thrown-msg? "Lookup ref attribute should be marked as :db/unique: [:not-an-attr 777]"
                     (d/entity db [:not-an-attr 777])))
    (d/close-db db)
    (u/delete-files dir)))

(deftest test-entity-equality
  (let [dir (u/tmp-dir (str "query-or-" (UUID/randomUUID)))
        db1 (-> (d/empty-db dir)
                (d/db-with [{:db/id 1, :name "Ivan"}]))
        e1  (d/entity db1 1)
        db2 (d/db-with db1 [])
        db3 (d/db-with db2 [{:db/id 2, :name "Oleg"}])]

    (testing "Two entities are equal if they have the same :db/id"
      (is (= e1 e1))
      (is (= e1 (d/entity db1 1)))

      (testing "and refer to the same database"
        (is (not= e1 (d/entity db2 1)))
        (is (not= e1 (d/entity db3 1)))))
    (d/close-db db1)
    (u/delete-files dir)))

(deftest auto-entity-time-test
  (let [dir  (u/tmp-dir (str "auto-entity-time-test-" (UUID/randomUUID)))
        conn (d/create-conn dir
                            {:id {:db/unique    :db.unique/identity
                                  :db/valueType :db.type/long}}
                            {:auto-entity-time? true})]
    (d/transact! conn [{:id 1}])
    (is (= (count (d/datoms @conn :eav)) 3))
    (is (:db/created-at (d/touch (d/entity @conn [:id 1]))))
    (is (:db/updated-at (d/touch (d/entity @conn [:id 1]))))

    (d/transact! conn [[:db/retractEntity [:id 1]]])
    (is (= (count (d/datoms @conn :eav)) 0))
    (is (nil? (d/entity @conn [:id 1])))

    (d/close conn)
    (u/delete-files dir)))

(deftest retract-then-transact-again-test
  (let [dir  (u/tmp-dir (str "retract-transact-test-" (UUID/randomUUID)))
        conn (d/create-conn dir
                            {:id       {:db/unique    :db.unique/identity
                                        :db/valueType :db.type/string}
                             :foo/refs {:db/valueType   :db.type/ref
                                        :db/cardinality :db.cardinality/many}})]
    (d/transact! conn [{:id "foo" :foo/refs [{:id "bar"}]}])
    (is (= 3 (count (d/datoms @conn :eav))))
    (is (= #{(d/entity @conn 2)}
           (:foo/refs (d/touch (d/entity @conn [:id "foo"])))))

    (d/transact! conn [[:db/retract [:id "foo"] :foo/refs]
                       {:id       "foo"
                        :foo/refs [{:id "bar"}]}])
    (is (= 3 (count (d/datoms @conn :eav))))
    (is (= #{(d/entity @conn 2)}
           (:foo/refs (d/touch (d/entity @conn [:id "foo"])))))

    (d/close conn)
    (u/delete-files dir)))

(deftest entity-as-value-test
  (let [dir  (u/tmp-dir (str "entity-as-value-test-" (UUID/randomUUID)))
        conn (d/create-conn dir
                            {:id {:db/unique    :db.unique/identity
                                  :db/valueType :db.type/string}})]

    (d/transact! conn [{:id "ent-a"}])
    (d/transact! conn
                 [{:id       "ent-b"
                   :some-edn {:entity (d/entity @conn [:id "ent-a"])}}])
    (is (= 3 (count (d/datoms @conn :eav))))
    (d/transact! conn [{:id "ent-b" :some-edn :replace}])
    (is (= 3 (count (d/datoms @conn :eav))))
    (is (= :replace (:some-edn (d/entity @conn [:id "ent-b"]))))
    (d/close conn)
    (u/delete-files dir)))

(deftest stale-db-snapshot-sees-new-attrs
  (let [dir  (u/tmp-dir (str "entity-stale-db-test-" (UUID/randomUUID)))
        conn (d/create-conn dir {:company/name {}
                                 :company/linkedin {}})
        db   @conn]
    (d/transact! conn [{:db/id 1
                        :company/name "8AM Golf"
                        :company/linkedin
                        "https://www.linkedin.com/company/8am-golf/"
                        :company/url "https://8amgolf.com/"}])

    (ddb/refresh-cache (:store db))

    (is (= [[1 :company/name "8AM Golf"]
            [1 :company/linkedin
             "https://www.linkedin.com/company/8am-golf/"]
            [1 :company/url "https://8amgolf.com/"]]
           (mapv (fn [datom] [(:e datom) (:a datom) (:v datom)])
                 (ddb/-search db [1 nil nil]))))

    (is (= #:company{:name "8AM Golf"
                     :linkedin
                     "https://www.linkedin.com/company/8am-golf/"
                     :url "https://8amgolf.com/"}
           (into {} (d/touch (d/entity db 1)))))

    (d/close conn)
    (u/delete-files dir)))

(deftest staged-entity-refs-test
  (let [dir  (u/tmp-dir (str "staged-entity-ref-test-" (UUID/randomUUID)))
        conn (d/create-conn dir
                            {:dev-id {:db/valueType :db.type/string
                                      :db/unique    :db.unique/identity}
                             :a-ref  {:db/valueType   :db.type/ref
                                      :db/cardinality :db.cardinality/many}})]

    (d/transact! conn [{:dev-id "foo"}])
    (d/transact! conn [{:dev-id "bar"}])
    (d/transact! conn [(assoc (d/entity @conn [:dev-id "foo"])
                              :a-ref [:dev-id "bar"])])
    (is (= #{(d/entity @conn 2)}
           (:a-ref (d/entity @conn [:dev-id "foo"]))))
    (d/close conn)
    (u/delete-files dir)))
