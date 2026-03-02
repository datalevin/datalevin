(ns datalevin.test.transact
  (:require
   [datalevin.test.core :as tdc :refer [db-fixture]]
   [clojure.test :refer [deftest testing are is use-fixtures]]
   [datalevin.core :as d]
   [datalevin.datom :as dd]
   [datalevin.interpret :as i]
   [datalevin.util :as u]
   [datalevin.constants :as c :refer [tx0]])
  (:import
   [java.util UUID]))

(use-fixtures :each db-fixture)

(defn testing-fn []  "test-value")

(deftest test-fn
  (let [dir                (u/tmp-dir (str "test-fn-" (UUID/randomUUID)))
        conn               (d/create-conn
                             dir {}
                             {:kv-opts {:flags (conj c/default-env-flags :nosync)}})
        test-tx            (fn [_ tempid]
                             [{:db/id tempid :node/value (testing-fn)}])
        {:keys [db-after]} (d/transact! conn [[:db.fn/call test-tx -1]])
        e                  (d/entity db-after 1)]
    (is (= (:node/value e) "test-value"))
    (d/close conn)
    (u/delete-files dir)))

(deftest test-db-fn
  (let [dir     (u/tmp-dir (str "skip-" (UUID/randomUUID)))
        conn    (d/create-conn
                  dir {:aka {:db/cardinality :db.cardinality/many}}
                  {:kv-opts {:flags (conj c/default-env-flags :nosync)}})
        inc-age (fn [db name]
                  (if-let [[eid age] (first (d/q '{:find  [?e ?age]
                                                   :in    [$ ?name]
                                                   :where [[?e :name ?name]
                                                           [?e :age ?age]]}
                                                 db name))]
                    [{:db/id eid :age (inc ^long age)} [:db/add eid :had-birthday true]]
                    (throw (ex-info (str "No entity with name: " name) {}))))]
    (d/transact! conn [{:db/id 1 :name "Ivan" :age 31}])
    (d/transact! conn [[:db/add 1 :name "Petr"]])
    (d/transact! conn [[:db/add 1 :aka "Devil"]])
    (d/transact! conn [[:db/add 1 :aka "Tupen"]])
    (is (= (d/q '[:find ?v ?a
                  :where [?e :name ?v]
                  [?e :age ?a]] @conn)
           #{["Petr" 31]}))
    (is (= (d/q '[:find ?v
                  :where [?e :aka ?v]] @conn)
           #{["Devil"] ["Tupen"]}))
    (is (thrown-msg? "No entity with name: Bob"
                     (d/transact! conn [[:db.fn/call inc-age "Bob"]])))
    (let [{:keys [db-after]} (d/transact! conn [[:db.fn/call inc-age "Petr"]])
          e                  (d/entity db-after 1)]
      (is (= (:age e) 32))
      (is (:had-birthday e)))

    (let [{:keys [db-after tempids]}
          (d/transact! conn [[:db.fn/call (fn [_] [{:db/id -1 :name "Vera"}])]])
          e (d/entity db-after (tempids -1))]
      (is (= "Vera" (:name e))))

    (d/close conn)
    (u/delete-files dir)))

(deftest test-db-ident-fn
  (let [dir     (u/tmp-dir (str "skip-" (UUID/randomUUID)))
        conn    (d/create-conn
                  dir {:name {:db/unique :db.unique/identity}}
                  {:kv-opts {:flags (conj c/default-env-flags :nosync)}})
        inc-age (i/inter-fn [db name]
                  (if-some [ent (d/entity db [:name name])]
                    [{:db/id (:db/id ent)
                      :age   (inc ^long (:age ent))}
                     [:db/add (:db/id ent) :had-birthday true]]
                    (throw (ex-info (str "No entity with name: " name) {}))))]
    (d/transact! conn [{:db/id    1
                        :name     "Petr"
                        :age      31
                        :db/ident :Petr}
                       {:db/ident :inc-age
                        :db/fn    inc-age}])
    (is (thrown-msg? "Can’t find entity for transaction fn :unknown-fn"
                     (d/transact! conn [[:unknown-fn]])))
    (is (thrown-msg? "Entity :Petr expected to have :db/fn attribute with fn? value"
                     (d/transact! conn [[:Petr]])))
    (is (thrown-msg? "No entity with name: Bob"
                     (d/transact! conn [[:inc-age "Bob"]])))
    (d/transact! conn [[:inc-age "Petr"]])
    (let [e (d/entity @conn 1)]
      (is (= (:age e) 32))
      (is (:had-birthday e)))
    (d/close conn)
    (u/delete-files dir)))

(deftest test-resolve-eid-1
  (let [dir    (u/tmp-dir (str "eid-" (UUID/randomUUID)))
        db     (d/empty-db
                 dir {:name {:db/unique :db.unique/identity}
                      :aka  {:db/unique      :db.unique/identity
                             :db/cardinality :db.cardinality/many}
                      :ref  {:db/valueType :db.type/ref}})
        report (d/with db [[:db/add -1 :name "Ivan"]
                           [:db/add -1 :age 19]
                           [:db/add -2 :name "Petr"]
                           [:db/add -2 :age 22]
                           [:db/add "Serg" :name "Sergey"]
                           [:db/add "Serg" :age 30]])]
    (is (= (:tempids report)
           {-1             1
            -2             2
            "Serg"         3
            :db/current-tx (+ c/tx0 1) }))
    (is (= #{[1 :name "Ivan"]
             [1 :age 19]
             [2 :name "Petr"]
             [2 :age 22]
             [3 :name "Sergey"]
             [3 :age 30]}
           (tdc/all-datoms (:db-after report))))
    (d/close-db db)
    (u/delete-files dir)))

(deftest test-resolve-eid-2
  (let [dir (u/tmp-dir (str "eid-" (UUID/randomUUID)))
        db  (-> (d/empty-db
                  dir {:name {:db/unique :db.unique/identity}
                       :aka  {:db/unique      :db.unique/identity
                              :db/cardinality :db.cardinality/many}
                       :ref  {:db/valueType :db.type/ref}})
                (d/db-with [[:db/add -1 :name "Ivan"]
                            [:db/add -2 :ref -1]]))]
    (is (= #{[1 :name "Ivan"] [2 :ref 1]}
           (tdc/all-datoms db)))
    (d/close-db db)
    (u/delete-files dir)))

(deftest test-resolve-eid-3
  (let [dir (u/tmp-dir (str "eid-" (UUID/randomUUID)))
        db  (-> (d/empty-db
                  dir {:name {:db/unique :db.unique/identity}
                       :aka  {:db/unique      :db.unique/identity
                              :db/cardinality :db.cardinality/many}
                       :ref  {:db/valueType :db.type/ref}})
                (d/db-with [[:db/add -1 :name "Ivan"]])
                (d/db-with [[:db/add -1 :name "Ivan"]
                            [:db/add -2 :ref -1]]))]
    (is (= #{[1 :name "Ivan"] [2 :ref 1]} (tdc/all-datoms db)))
    (d/close-db db)
    (u/delete-files dir)))

(deftest test-resolve-eid-4
  (let [dir (u/tmp-dir (str "eid-" (UUID/randomUUID)))
        db  (-> (d/empty-db
                  dir {:name {:db/unique :db.unique/identity}
                       :aka  {:db/unique      :db.unique/identity
                              :db/cardinality :db.cardinality/many}
                       :ref  {:db/valueType :db.type/ref}})
                (d/db-with [[:db/add -1 :aka "Batman"]])
                (d/db-with [[:db/add -1 :aka "Batman"]
                            [:db/add -2 :ref -1]]))]
    (is (= #{[1 :aka "Batman"] [2 :ref 1]} (tdc/all-datoms db)))
    (d/close-db db)
    (u/delete-files dir)))

(deftest test-tempid-ref-295
  (let [dir (u/tmp-dir (str "skip-" (UUID/randomUUID)))
        db  (-> (d/empty-db dir {:ref {:db/unique    :db.unique/identity
                                       :db/valueType :db.type/ref}})
                (d/db-with [[:db/add -1 :name "Ivan"]
                            [:db/add -2 :name "Petr"]
                            [:db/add -1 :ref -2]]))]
    (is (= #{[1 :name "Ivan"]
             [1 :ref 2]
             [2 :name "Petr"]}
           (tdc/all-datoms db)))
    (d/close-db db)
    (u/delete-files dir)))

(deftest test-resolve-eid-refs
  (let [dir  (u/tmp-dir (str "resolve-" (UUID/randomUUID)))
        conn (d/create-conn
               dir {:friend {:db/valueType   :db.type/ref
                             :db/cardinality :db.cardinality/many}}
               {:kv-opts {:flags (conj c/default-env-flags :nosync)}})
        tx   (d/transact! conn [{:name   "Sergey"
                                 :friend [-1 -2]}
                                [:db/add -1 :name "Ivan"]
                                [:db/add -2 :name "Petr"]
                                [:db/add "B" :name "Boris"]
                                [:db/add "B" :friend -3]
                                [:db/add -3 :name "Oleg"]
                                [:db/add -3 :friend "B"]])
        q    '[:find ?fn
               :in $ ?n
               :where [?e :name ?n]
               [?e :friend ?fe]
               [?fe :name ?fn]]]
    (is (= (:tempids tx)
           {-1 2, -2 3, "B" 4, -3 5, :db/current-tx (+ tx0 1)}))
    (is (= (d/q q @conn "Sergey") #{["Ivan"] ["Petr"]}))
    (is (= (d/q q @conn "Boris") #{["Oleg"]}))
    (is (= (d/q q @conn "Oleg") #{["Boris"]}))
    (d/close conn)
    (u/delete-files dir)))

(deftest test-tempid
  (let [dir (u/tmp-dir (str "tempid-" (UUID/randomUUID)))
        db  (d/empty-db
              dir
              {:friend {:db/valueType :db.type/ref}
               :comp   {:db/valueType :db.type/ref, :db/isComponent true}
               :multi  {:db/cardinality :db.cardinality/many}})]
    (testing "Unused tempid" ;; #304
      (is (thrown-msg? "Tempids used only as value in transaction: (-2)"
                       (d/db-with db [[:db/add -1 :friend -2]])))
      (is (thrown-msg? "Tempids used only as value in transaction: (-2)"
                       (d/db-with db [{:db/id -1 :friend -2}])))
      (is (thrown-msg? "Tempids used only as value in transaction: (-1)"
                       (d/db-with db [{:db/id -1}
                                      [:db/add -2 :friend -1]])))
      (is (thrown-msg? "Tempids used only as value in transaction: (-1)"
                       (d/db-with db [{:db/id -1 :multi []}
                                      [:db/add -2 :friend -1]]))))
    (d/close-db db)
    (u/delete-files dir)))

(deftest test-transient-294
  "db.fn/retractEntity retracts attributes of adjacent entities https://github.com/tonsky/datalevin/issues/294"
  (let [dir    (u/tmp-dir (str "skip-" (UUID/randomUUID)))
        db     (reduce #(d/db-with %1 [{:db/id %2 :a1 1 :a2 2 :a3 3}])
                       (d/empty-db dir)
                       (range 1 10))
        report (d/with db [[:db.fn/retractEntity 1]
                           [:db.fn/retractEntity 2]])]
    (is (= [(d/datom 1 :a1 1)
            (d/datom 1 :a2 2)
            (d/datom 1 :a3 3)
            (d/datom 2 :a1 1)
            (d/datom 2 :a2 2)
            (d/datom 2 :a3 3)]
           (:tx-data report)))
    (d/close-db db)
    (u/delete-files dir)))

(deftest test-transact-same
  "same data, transacted twice"
  (let [dir1 (u/tmp-dir (str "skip-" (UUID/randomUUID)))
        dir2 (u/tmp-dir (str "skip-" (UUID/randomUUID)))
        es   [{:db/id -1 :company "IBM" :country "US"}
              {:db/id -2 :company "PwC" :country "Germany"}]
        db1  (d/db-with (d/empty-db dir1) es)
        dts1 (d/datoms db1 :eav)
        db2  (d/db-with (d/empty-db dir2) es)
        dts2 (d/datoms db2 :eav)]
    (is (= dts1 dts2))
    (d/close-db db1)
    (d/close-db db2)
    (u/delete-files dir1)
    (u/delete-files dir2)))

(deftest validate-data
  "validate data during transact"
  (let [sc  {:company {:db/valueType :db.type/string}
             :id      {:db/valueType :db.type/uuid}
             :raw     {:db/valueType :db.type/bytes}
             :code    {:db/valueType :db.type/long}}
        dir (u/tmp-dir (str "skip-" (UUID/randomUUID)))
        db  (d/empty-db dir sc {:validate-data? true})]
    (is (thrown-with-msg?
          Exception #"Invalid data, expecting"
          (d/db-with db [{:db/id -1 :company (byte-array [1]) :raw (byte-array [1 2])}])))
    (is (thrown-with-msg?
          Exception #"Invalid data, expecting"
          (d/db-with db [{:db/id -1 :company "IBM" :raw 1 :code 1}])))
    (is (thrown-with-msg?
          Exception #"Invalid data, expecting"
          (d/db-with db [{:db/id -1 :company "IBM" :id "ibm" :code 1}])))
    (is (thrown-with-msg?
          Exception #"Invalid data, expecting"
          (d/db-with db [{:db/id -2 :company 1 :id (UUID/randomUUID) :code 1}])))
    (is (thrown-with-msg?
          Exception #"Invalid data, expecting"
          (d/db-with db [{:db/id -3 :company :abc :id (UUID/randomUUID) :code 1}])))
    (is (thrown-with-msg?
          Exception #"Invalid data, expecting"
          (d/db-with db [{:db/id -4 :company 1.0 :id (UUID/randomUUID) :code 1}])))
    (is (thrown-with-msg?
          Exception #"Invalid data, expecting"
          (d/db-with db [{:db/id -5 :company "XYZ" :id (UUID/randomUUID) :code "1"}])))
    (d/close-db db)
    (u/delete-files dir)))

(deftest closed-schema
  "closed schema during transact"
  (let [sc  {:id      {:db/valueType :db.type/uuid}
             :company {}}
        dir (u/tmp-dir (str "skip-" (UUID/randomUUID)))
        db  (d/empty-db dir sc {:closed-schema? true})]
    (is (thrown-with-msg?
          Exception
          #"Attribute is not defined"
          (d/db-with db
                     [{:db/id          -1 :company "IBM" :id (UUID/randomUUID)
                       :undefined-attr "ibm"}])))
    (d/close-db db)
    (u/delete-files dir)))

(deftest test-transact-bytes
  "requires comparing byte-arrays"
  (let [schema      {:bytes {:db/valueType :db.type/bytes}}
        byte-arrays (mapv #(.getBytes ^String %) ["foo" "bar" "foo"])]
    (testing "equal bytes"
      (let [dir  (u/tmp-dir (str "skip-" (UUID/randomUUID)))
            db   (d/empty-db dir schema)
            ents (mapv (fn [ba] {:bytes ba}) byte-arrays)]
        (is (every? true?
                    (map #(java.util.Arrays/equals ^bytes %1 ^bytes %2)
                         byte-arrays
                         (map :v (:tx-data (d/with db ents))))))
        (d/close-db db)
        (u/delete-files dir)))))


(deftest issue-127-test
  (let [schema {:foo/id    {:db/valueType   :db.type/string
                            :db/cardinality :db.cardinality/one
                            :db/unique      :db.unique/identity}
                :foo/stats {:db/doc "Blob of additional stats"}}
        dir    (u/tmp-dir (str "issue-127-" (UUID/randomUUID)))
        conn   (d/create-conn
                 dir schema
                 {:kv-opts {:flags (conj c/default-env-flags :nosync)}})]
    (d/transact! conn [{:foo/id "foo" :foo/stats {:lul "bar"}}])
    (dotimes [n 1000]
      (d/transact! conn [{:foo/id (str "foo" n) :foo/stats {:lul "bar"}}]))
    (is (= 1001 (count (d/q '[:find ?e :where [?e :foo/id _]] @conn))))
    (d/close conn)
    (u/delete-files dir)))

(deftest test-uncomparable-356-1
  (let [dir (u/tmp-dir (str "issue-356-" (UUID/randomUUID)))
        db  (-> (d/empty-db dir
                            {:multi {:db/cardinality :db.cardinality/many}
                             :index {:db/index true}})
                (d/db-with [[:db/add     1 :single {:map 1}]])
                (d/db-with [[:db/retract 1 :single {:map 1}]])
                (d/db-with [[:db/add     1 :single {:map 2}]])
                (d/db-with [[:db/add     1 :single {:map 3}]]))]
    (is (= #{[1 :single {:map 3}]}
           (tdc/all-datoms db)))
    (is (= [(dd/datom 1 :single {:map 3})]
           (vec (d/datoms db :eav 1 :single {:map 3}))))
    (is (= [(dd/datom 1 :single {:map 3})]
           (vec (d/datoms db :ave :single {:map 3} 1))))
    (d/close-db db)
    (u/delete-files dir)))

(deftest test-uncomparable-356-2
  (let [dir (u/tmp-dir (str "issue-356-" (UUID/randomUUID)))
        db  (-> (d/empty-db dir
                            {:multi {:db/cardinality :db.cardinality/many}
                             :index {:db/index true}})
                (d/db-with [[:db/add 1 :multi {:map 1}]])
                (d/db-with [[:db/add 1 :multi {:map 1}]])
                (d/db-with [[:db/add 1 :multi {:map 2}]]))]
    (is (= #{[1 :multi {:map 1}] [1 :multi {:map 2}]}
           (tdc/all-datoms db)))
    (is (= [(dd/datom 1 :multi {:map 2})]
           (vec (d/datoms db :eav 1 :multi {:map 2}))))
    (is (= [(dd/datom 1 :multi {:map 2})]
           (vec (d/datoms db :ave :multi {:map 2} 1))))
    (d/close-db db)
    (u/delete-files dir)))

(deftest test-uncomparable-356-3
  (let [dir (u/tmp-dir (str "issue-356-" (UUID/randomUUID)))
        db  (-> (d/empty-db dir
                            {:multi {:db/cardinality :db.cardinality/many}})
                (d/db-with [[:db/add     1 :index {:map 1}]])
                (d/db-with [[:db/retract 1 :single {:map 1}]])
                (d/db-with [[:db/add     1 :index {:map 2}]])
                (d/db-with [[:db/add     1 :index {:map 3}]]))]
    (is (= #{[1 :index {:map 3}]}
           (tdc/all-datoms db)))
    (is (= [(dd/datom 1 :index {:map 3})]
           (vec (d/datoms db :eav 1 :index {:map 3}))))
    (is (= [(dd/datom 1 :index {:map 3})]
           (vec (d/datoms db :ave :index {:map 3} 1 ))))
    (d/close-db db)
    (u/delete-files dir)))

(deftest unchanged-datoms-test
  (let [dir  (u/tmp-dir (str "unchanged-datoms-" (UUID/randomUUID)))
        conn (d/create-conn
               dir {}
               {:kv-opts {:flags (conj c/default-env-flags :nosync)}})
        rp1  (d/transact! conn [{:foo "bar"}])]
    (is (= [true] (map :added (:tx-data rp1))))

    ;; tx-data is empty since datom is unchanged
    (let [rp2 (d/transact! conn [{:db/id 1 :foo "bar"}])]
      (is (= [] (:tx-data rp2))))

    (d/close conn)
    (u/delete-files dir)))

(deftest unthawable-datoms-test
  (let [dir  (u/tmp-dir (str "unthawable-datoms-" (UUID/randomUUID)))
        conn (d/create-conn
               dir {} {:kv-opts {:flags (conj c/default-env-flags :nosync)}})]

    (is (thrown? Exception (d/transact! conn [{:bar (defn bar [] :bar)}])))

    (d/close conn)
    (u/delete-files dir)))

(deftest issue-338-test
  (let [dir  (u/tmp-dir (str "issue-338-" (UUID/randomUUID)))
        conn (d/create-conn
               dir
               {:person/id       {:db/valueType :db.type/string
                                  :db/unique    :db.unique/identity}
                :passport/person {:db/valueType   :db.type/ref
                                  :db/cardinality :db.cardinality/one
                                  :db/unique      :db.unique/identity}}
               {:kv-opts {:flags (conj c/default-env-flags :nosync)}})]
    (d/transact! conn [{:person/id "person-123"}])
    (d/transact! conn [{:passport/person [:person/id "person-123"]}])
    (is (= 1 (d/q '[:find (count ?pass) .
                    :in $ ?pid
                    :where
                    [?p :person/id ?pid]
                    [?pass :passport/person ?p]]
                  (d/db conn) "person-123")))
    (d/close conn)
    (u/delete-files dir)))

;; TODO
#_(deftest test-transitive-type-compare-386
    (let [txs    [[{:block/uid "2LB4tlJGy"}]
                  [{:block/uid "2ON453J0Z"}]
                  [{:block/uid "2KqLLNbPg"}]
                  [{:block/uid "2L0dcD7yy"}]
                  [{:block/uid "2KqFNrhTZ"}]
                  [{:block/uid "2KdQmItUD"}]
                  [{:block/uid "2O8BcBfIL"}]
                  [{:block/uid "2L4ZbI7nK"}]
                  [{:block/uid "2KotiW36Z"}]
                  [{:block/uid "2O4o-y5J8"}]
                  [{:block/uid "2KimvuGko"}]
                  [{:block/uid "dTR20ficj"}]
                  [{:block/uid "wRmp6bXAx"}]
                  [{:block/uid "rfL-iQOZm"}]
                  [{:block/uid "tya6s422-"}]
                  [{:block/uid 45619}]]
          schema {:block/uid {:db/unique :db.unique/identity}}
          dir    (u/tmp-dir (str "issue-386-" (UUID/randomUUID)))
          conn   (d/create-conn dir schema)
          _      (doseq [tx txs] (d/transact! conn tx))
          db     @conn]
      (is (empty? (->> (seq db)
                       (map (fn [[_ a v]] [a v]))
                       (remove #(d/entity db %)))))
      (d/close conn)
      (u/delete-files dir)))
