(ns datalevin.test.transact
  (:require
   [datalevin.test.core :as tdc :refer [db-fixture]]
   [clojure.test :refer [deftest testing are is use-fixtures]]
   [datalevin.core :as d]
   [datalevin.datom :as dd]
   [datalevin.interface :as iface]
   [datalevin.interpret :as i]
   [datalevin.udf :as udf]
   [datalevin.util :as u]
   [datalevin.constants :as c :refer [tx0]]
   [inter-fn-host])
  (:import
   [java.util UUID]))

(use-fixtures :each db-fixture)

(defn testing-fn []  "test-value")

(defn user-name?
  [s]
  (<= 3 (count s) 15))

(defn has-at?
  [s]
  (boolean (re-find #"@" s)))

(defn short-string?
  [s]
  (<= (count s) 5))

(defn truthy-string?
  [_]
  :truthy)

(defn account-open?
  [db eid]
  (= :open (:account/status (d/entity db eid))))

(defn account-balance?
  [db eid expected]
  (= expected (:account/balance (d/entity db eid))))

(defn account-open-and-unlocked?
  [db eid]
  (let [e (d/entity db eid)]
    (and (= :open (:account/status e))
         (not (:account/locked? e)))))

(defn ensure-throws
  [_db _eid]
  (throw (ex-info "ensure exploded" {})))

(i/definterfn host-var-tx [_db tempid]
  [{:db/id      tempid
    :node/value (inter-fn-host/testing-fn)}])

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

(deftest test-db-attr-preds
  (let [dir    (u/tmp-dir (str "attr-preds-" (UUID/randomUUID)))
        schema {:user/name   {:db/valueType   :db.type/string
                              :db.attr/preds 'datalevin.test.transact/user-name?}
                :user/email  {:db/valueType   :db.type/string
                              :db/cardinality :db.cardinality/many
                              :db.attr/preds ['datalevin.test.transact/has-at?
                                              'datalevin.test.transact/short-string?]}
                :user/truthy {:db/valueType   :db.type/string
                              :db.attr/preds 'datalevin.test.transact/truthy-string?}}
        conn   (d/create-conn
                 dir schema
                 {:kv-opts {:flags (conj c/default-env-flags :nosync)}})]
    (try
      (d/transact! conn [{:db/id 1
                          :user/name "alice"
                          :user/email ["a@b"]}])
      (is (= "alice" (:user/name (d/entity @conn 1))))
      (is (thrown-with-msg?
            Exception
            #"failed pred datalevin.test.transact/user-name\?"
            (d/transact! conn [{:db/id 2 :user/name "al"}])))
      (is (thrown-with-msg?
            Exception
            #"failed pred datalevin.test.transact/short-string\?"
            (d/transact! conn [{:db/id 3 :user/email ["abcdef@"]}])))
      (try
        (d/transact! conn [{:db/id 4 :user/truthy "anything"}])
        (is false "truthy, non-true predicate returns should fail")
        (catch clojure.lang.ExceptionInfo e
          (is (= :transact/attr-pred (:error (ex-data e))))
          (is (= :truthy (:db.error/pred-return (ex-data e))))
          (is (= 'datalevin.test.transact/truthy-string?
                 (:predicate (ex-data e))))))
      (finally
        (d/close conn)
        (u/delete-files dir)))))

(deftest test-db-attr-preds-udf
  (let [dir       (u/tmp-dir (str "attr-preds-udf-" (UUID/randomUUID)))
        registry  (udf/create-registry)
        high-desc {:udf/lang :clojure
                   :udf/kind :predicate
                   :udf/id   :score/high?}
        even-desc {:udf/lang :clojure
                   :udf/kind :predicate
                   :udf/id   :score/even?}
        schema    {:score/high {:db/valueType   :db.type/long
                                 :db.attr/preds high-desc}
                   :score/even {:db/valueType   :db.type/long
                                 :db.attr/preds :score/even?}}
        conn      (do
                    (udf/register! registry high-desc #(<= 10 (long %)))
                    (udf/register! registry even-desc #(even? (long %)))
                    (d/create-conn
                      dir schema
                      {:kv-opts     {:flags (conj c/default-env-flags :nosync)}
                       :runtime-opts {:udf-registry registry}}))]
    (try
      (d/transact! conn [{:db/id      1
                          :score/high 12
                          :score/even 4}])
      (is (= 12 (:score/high (d/entity @conn 1))))
      (is (thrown-with-msg?
            Exception
            #"failed pred"
            (d/transact! conn [{:db/id 2 :score/high 9}])))
      (is (thrown-with-msg?
            Exception
            #"failed pred :score/even\?"
            (d/transact! conn [{:db/id 3 :score/even 5}])))
      (finally
        (d/close conn)
        (u/delete-files dir)))))

(deftest test-db-attr-preds-schema-validation
  (let [dir1 (u/tmp-dir (str "attr-preds-schema-empty-" (UUID/randomUUID)))
        dir2 (u/tmp-dir (str "attr-preds-schema-unqualified-" (UUID/randomUUID)))]
    (try
      (is (thrown-with-msg?
            Exception
            #":db.attr/preds cannot be empty"
            (d/create-conn dir1 {:bad/attr {:db.attr/preds []}})))
      (is (thrown-with-msg?
            Exception
            #":db.attr/preds entries must be qualified symbols"
            (d/create-conn dir2 {:bad/attr {:db.attr/preds ['unqualified?]}})))
      (finally
        (when (u/file-exists dir1) (u/delete-files dir1))
        (when (u/file-exists dir2) (u/delete-files dir2))))))

(deftest test-db-attr-preds-not-retroactive-but-upserts-validate
  (let [dir    (u/tmp-dir (str "attr-preds-upsert-" (UUID/randomUUID)))
        schema {:user/name {:db/valueType :db.type/string
                            :db/unique    :db.unique/identity}
                :user/age  {:db/valueType :db.type/long}}
        conn   (d/create-conn
                 dir schema
                 {:kv-opts {:flags (conj c/default-env-flags :nosync)}})]
    (try
      (d/transact! conn [{:db/id 1 :user/name "legacy"}])
      (d/update-schema
        conn
        {:user/name {:db/valueType   :db.type/string
                     :db/unique      :db.unique/identity
                     :db.attr/preds 'datalevin.test.transact/has-at?}})
      (is (= "legacy" (:user/name (d/entity @conn 1))))
      (is (thrown-with-msg?
            Exception
            #"failed pred datalevin.test.transact/has-at\?"
            (d/transact! conn [{:user/name "legacy"
                                :user/age  42}])))
      (d/transact! conn [[:db/add 1 :user/name "a@b"]])
      (is (= "a@b" (:user/name (d/entity @conn 1))))
      (finally
        (d/close conn)
        (u/delete-files dir)))))

(deftest test-db-ensure
  (let [dir    (u/tmp-dir (str "ensure-" (UUID/randomUUID)))
        schema {:account/status  {:db/valueType :db.type/keyword}
                :account/balance {:db/valueType :db.type/long}
                :account/locked? {:db/valueType :db.type/boolean}}
        descriptor {:udf/lang :test
                    :udf/kind :predicate
                    :udf/id   :account/open?}
        installed-descriptor {:udf/lang :test
                              :udf/kind :predicate
                              :udf/id   :account/installed-open?}
        registry (doto (udf/create-registry)
                   (udf/register! descriptor account-open?)
                   (udf/register! installed-descriptor account-open?))
        conn   (d/create-conn
                 dir schema
                 {:runtime-opts {:udf-registry registry}
                  :kv-opts      {:flags (conj c/default-env-flags :nosync)}})]
    (try
      (let [report (d/transact!
                     conn
                     [{:db/id           "acct"
                       :account/status  :open
                       :account/balance 10}
                      [:db/ensure
                       'datalevin.test.transact/account-open?
                       "acct"]
                      [:db/ensure account-balance? "acct" 10]
                      [:db/ensure descriptor "acct"]
                      [:db/ensure :account/open? "acct"]])
            eid    (get (:tempids report) "acct")]
        (is (some? eid))
        (is (= :open (:account/status (d/entity @conn eid))))
        (is (not-any? #(= :db/ensure (:a %)) (:tx-data report))))

      (let [report (d/transact!
                     conn
                     [{:db/ident :account/installed-open?
                       :db/udf   installed-descriptor}
                      {:db/id "installed" :account/status :open}
                      [:db/ensure :account/installed-open? "installed"]])
            eid    (get (:tempids report) "installed")]
        (is (= :open (:account/status (d/entity @conn eid)))))

      (binding [c/*use-prepare-path* true]
        (let [report (d/transact!
                       conn
                       [{:db/id "prep" :account/status :open}
                        [:db/ensure account-open? "prep"]
                        [:db/ensure :account/open? "prep"]])
              eid    (get (:tempids report) "prep")]
          (is (= :open (:account/status (d/entity @conn eid))))))

      (let [dir-auto  (u/tmp-dir (str "ensure-auto-time-"
                                      (UUID/randomUUID)))
            conn-auto (d/create-conn
                        dir-auto schema
                        {:auto-entity-time? true
                         :kv-opts {:flags (conj c/default-env-flags
                                                :nosync)}})]
        (try
          (let [report (d/transact!
                         conn-auto
                         [{:db/id "auto" :account/status :open}
                          [:db/ensure account-open? "auto"]])
                eid    (get (:tempids report) "auto")]
            (is (= :open (:account/status (d/entity @conn-auto eid)))))
          (finally
            (d/close conn-auto)
            (u/delete-files dir-auto))))

      (is (thrown-with-msg?
            Exception
            #":db/ensure failed"
            (d/transact!
              conn
              [{:db/id "bad" :account/status :open}
               [:db/ensure account-open-and-unlocked? "bad"]
               [:db/add "bad" :account/locked? true]])))
      (is (empty? (d/q '[:find ?e
                         :where [?e :account/locked? true]]
                       @conn)))

      (is (thrown-with-msg?
            Exception
            #"ensure exploded"
            (d/transact!
              conn
              [{:db/id "boom" :account/status :open :account/balance 99}
               [:db/ensure ensure-throws "boom"]])))
      (is (empty? (d/q '[:find ?e
                         :where [?e :account/balance 99]]
                       @conn)))

      (is (thrown-with-msg?
            Exception
            #"could not resolve tempid argument"
            (d/transact! conn [[:db/ensure account-open? "missing"]])))
      (finally
        (d/close conn)
        (u/delete-files dir)))))

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

(deftest test-db-ident-inter-fn-host-var
  (let [dir  (u/tmp-dir (str "inter-fn-host-var-" (UUID/randomUUID)))
        conn (d/create-conn
               dir {}
               {:kv-opts {:flags (conj c/default-env-flags :nosync)}})]
    (try
      (d/transact! conn [{:db/ident :add-host-var-node
                          :db/fn    host-var-tx}])
      (d/close conn)
      (let [conn' (d/create-conn
                    dir {}
                    {:kv-opts {:flags (conj c/default-env-flags :nosync)}})]
        (try
          (let [{:keys [db-after tempids]}
                (d/transact! conn' [[:add-host-var-node -1]])]
            (is (= "test-value"
                   (:node/value (d/entity db-after (tempids -1))))))
          (finally
            (d/close conn'))))
      (finally
        (when-not (d/closed? conn)
          (d/close conn))
        (u/delete-files dir)))))

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

(deftest test-db-ident-fn-closed-schema
  (let [dir     (u/tmp-dir (str "skip-" (UUID/randomUUID)))
        conn    (d/create-conn
                  dir {:name {:db/unique :db.unique/identity}
                       :age  {:db/valueType :db.type/long}}
                  {:closed-schema? true
                   :kv-opts        {:flags (conj c/default-env-flags :nosync)}})
        inc-age (i/inter-fn [db name]
                  (if-some [ent (d/entity db [:name name])]
                    [{:db/id (:db/id ent)
                      :age   (inc ^long (:age ent))}]
                    []))]
    (d/transact! conn [{:db/id    1
                        :name     "Petr"
                        :age      31}
                       {:db/ident :inc-age
                        :db/fn    inc-age}])
    (d/transact! conn [[:inc-age "Petr"]])
    (is (= 32 (:age (d/entity @conn 1))))
    (d/close conn)
    (u/delete-files dir)))

(deftest test-db-udf-call
  (let [dir        (u/tmp-dir (str "udf-tx-" (UUID/randomUUID)))
        descriptor {:udf/lang :test
                    :udf/kind :tx-fn
                    :udf/id   :people/inc-age}
        registry   (doto (udf/create-registry)
                     (udf/register! descriptor
                       (fn [db name]
                         (if-let [[eid age] (first (d/q '{:find  [?e ?age]
                                                          :in    [$ ?name]
                                                          :where [[?e :name ?name]
                                                                  [?e :age ?age]]}
                                                        db name))]
                           [{:db/id eid :age (inc ^long age)}]
                           []))))
        conn       (d/create-conn
                     dir {:name {:db/unique :db.unique/identity}}
                     {:runtime-opts {:udf-registry registry}
                      :kv-opts      {:flags (conj c/default-env-flags :nosync)}})]
    (d/transact! conn [{:db/id 1 :name "Petr" :age 31}])
    (d/transact! conn [[:db.fn/call descriptor "Petr"]])
    (is (= 32 (:age (d/entity @conn 1))))

    (udf/register! registry descriptor
      (fn [db name]
        (if-let [[eid age] (first (d/q '{:find  [?e ?age]
                                         :in    [$ ?name]
                                         :where [[?e :name ?name]
                                                 [?e :age ?age]]}
                                       db name))]
          [{:db/id eid :age (+ 10 ^long age)}]
          [])))
    (d/transact! conn [[:db.fn/call descriptor "Petr"]])
    (is (= 42 (:age (d/entity @conn 1))))
    (d/close conn)
    (u/delete-files dir)))

(deftest test-db-ident-udf
  (let [dir        (u/tmp-dir (str "udf-ident-" (UUID/randomUUID)))
        descriptor {:udf/lang :test
                    :udf/kind :tx-fn
                    :udf/id   :people/inc-age}
        registry   (doto (udf/create-registry)
                     (udf/register! descriptor
                       (fn [db name]
                         (if-let [[eid age] (first (d/q '{:find  [?e ?age]
                                                          :in    [$ ?name]
                                                          :where [[?e :name ?name]
                                                                  [?e :age ?age]]}
                                                        db name))]
                           [{:db/id eid :age (inc ^long age)}]
                           []))))
        conn       (d/create-conn
                     dir {:name {:db/unique :db.unique/identity}
                          :age  {:db/valueType :db.type/long}}
                     {:closed-schema? true
                      :runtime-opts  {:udf-registry registry}
                      :kv-opts       {:flags (conj c/default-env-flags :nosync)}})]
    (d/transact! conn [{:db/id 1 :name "Petr" :age 31}
                       {:db/ident :people/inc-age
                        :db/udf   descriptor}])
    (d/transact! conn [[:people/inc-age "Petr"]])
    (is (= 32 (:age (d/entity @conn 1))))
    (d/transact! conn [[:db.fn/call :people/inc-age "Petr"]])
    (is (= 33 (:age (d/entity @conn 1))))
    (is (thrown-with-msg?
          Exception
          #"cannot have both :db/fn and :db/udf"
          (d/transact! conn [{:db/ident :bad/fn
                              :db/fn    identity
                              :db/udf   descriptor}])))
    (d/close conn)
    (u/delete-files dir)))

(deftest test-conn-cas-with-lookup-ref
  (let [dir  (u/tmp-dir (str "cas-" (UUID/randomUUID)))
        conn (d/create-conn
               dir {:name {:db/unique :db.unique/identity}}
               {:kv-opts {:flags (conj c/default-env-flags :nosync)}})]
    (d/transact! conn [{:db/id 1 :name "Petr" :age 31}])
    (d/transact! conn [[:db/cas [:name "Petr"] :age 31 32]])
    (d/transact! conn [[:db/cas [:name "Petr"] :age 32 33]
                       [:db/cas [:name "Petr"] :age 33 34]])
    (is (= 34 (:age (d/entity @conn [:name "Petr"]))))
    (d/close conn)
    (u/delete-files dir)))

(deftest test-cas-cardinality-one-uses-validated-old-datom
  (let [dir  (u/tmp-dir (str "cas-cardinality-one-" (UUID/randomUUID)))
        conn (d/create-conn
               dir
               {:name {:db/unique :db.unique/identity}
                :age  {:db/valueType :db.type/long}}
               {:kv-opts {:flags (conj c/default-env-flags :nosync)}})]
    (try
      (d/transact! conn [{:db/id 1 :name "Petr" :age 31}])
      (with-redefs [iface/ea-first-datom (fn [_ _ _] nil)]
        (let [report (d/transact! conn [[:db/cas [:name "Petr"] :age 31 32]
                                        [:db/cas [:name "Petr"] :age 32 33]])]
          (is (= [[:age 31 false]
                  [:age 32 true]
                  [:age 32 false]
                  [:age 33 true]]
                 (mapv (fn [datom]
                         [(dd/datom-a datom)
                          (dd/datom-v datom)
                          (dd/datom-added datom)])
                       (:tx-data report))))))
      (is (= #{["Petr" 33]}
             (d/q '[:find ?name ?age
                    :where
                    [?e :name ?name]
                    [?e :age ?age]]
                  @conn)))
      (finally
        (d/close conn)
        (u/delete-files dir)))))

(deftest test-wal-with-transaction-cas-with-lookup-ref
  (let [dir    (u/tmp-dir (str "wal-cas-" (UUID/randomUUID)))
        schema {:name {:db/unique :db.unique/identity}}
        conn   (d/create-conn
                 dir
                 schema
                 {:wal? true
                  :wal-durability-profile :strict})]
    (try
      (d/transact! conn [{:db/id 1 :name "Petr" :age 31}])
      (d/transact! conn [[:db/cas [:name "Petr"] :age 31 32]
                         [:db/cas [:name "Petr"] :age 32 33]])
      (is (= 33 (:age (d/entity @conn [:name "Petr"]))))
      (d/close conn)
      (let [conn' (d/create-conn
                    dir
                    schema
                    {:wal? true
                     :wal-durability-profile :strict})]
        (try
          (is (= 33 (:age (d/entity @conn' [:name "Petr"]))))
          (finally
            (d/close conn'))))
      (finally
        (when-not (d/closed? conn)
          (d/close conn))
        (u/delete-files dir)))))

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
        (u/delete-files dir))))
    (testing "leading zero bytes remain visible through range scans"
      (let [conn (d/create-conn nil {:v {:db/valueType :db.type/bytes}})]
        (try
          (d/transact! conn [{:db/id 1 :v (byte-array [0 0 0 5])}
                             {:db/id 2 :v (byte-array [5 0 0 0])}
                             {:db/id 3 :v (byte-array [0])}
                             {:db/id 4 :v (byte-array [0 0])}])
          (let [db (d/db conn)
                b1 (byte-array [0 0 0 5])]
            (is (java.util.Arrays/equals ^bytes b1 ^bytes (:v (d/pull db '[:v] 1))))
            (is (java.util.Arrays/equals ^bytes b1 ^bytes (:v (d/entity db 1))))
            (is (= [1] (mapv dd/datom-e (d/datoms db :eav 1 :v))))
            (is (= [1]
                   (mapv dd/datom-e (d/search-datoms db 1 :v nil))))
            (is (= [1 2 3 4]
                   (vec (sort (map dd/datom-e (d/datoms db :ave :v))))))
            (is (= 4 (d/count-datoms db nil :v nil)))
            (is (= [1 2 3 4]
                   (vec (sort (d/q '[:find [?e ...] :where [?e :v]] db)))))
            (is (= [1]
                   (vec (sort (d/q '[:find [?e ...]
                                      :in $ ?v
                                      :where [?e :v ?v]]
                                    db b1))))))
          (finally
            (d/close conn))))))


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
