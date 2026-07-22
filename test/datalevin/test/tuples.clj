(ns datalevin.test.tuples
  (:require
   [clojure.test :as t :refer [is are deftest testing]]
   [datalevin.core :as d]
   [datalevin.db :as db]
   [datalevin.util :as u]
   [datalevin.test.core :as tdc])
  (:import
   [clojure.lang ExceptionInfo]
   [java.util UUID]))

(deftest test-schema
  (let [dir  (u/tmp-dir (str "tuples-" (UUID/randomUUID)))
        dir1 (u/tmp-dir (str "tuples-" (UUID/randomUUID)))
        dir2 (u/tmp-dir (str "tuples-" (UUID/randomUUID)))
        dir3 (u/tmp-dir (str "tuples-" (UUID/randomUUID)))
        dir4 (u/tmp-dir (str "tuples-" (UUID/randomUUID)))
        dir5 (u/tmp-dir (str "tuples-" (UUID/randomUUID)))
        dir6 (u/tmp-dir (str "tuples-" (UUID/randomUUID)))
        dir7 (u/tmp-dir (str "tuples-" (UUID/randomUUID)))
        dir8 (u/tmp-dir (str "tuples-" (UUID/randomUUID)))
        dir9 (u/tmp-dir (str "tuples-" (UUID/randomUUID)))
        db   (d/empty-db
               dir
               {:year+session    {:db/tupleAttrs [:year :session]}
                :semester+course+student
                {:db/tupleAttrs [:semester :course :student]}
                :session+student {:db/tupleAttrs [:session :student]
                                  :db/valueType  :db.type/tuple}})]
    (is (= #{:year+session :semester+course+student :session+student}
           (:db.type/tuple (db/-rschema db))))

    (is (= {:year     {:year+session 0}
            :session  {:year+session 1, :session+student 0}
            :semester {:semester+course+student 0}
            :course   {:semester+course+student 1}
            :student  {:semester+course+student 2, :session+student 1}}
           (:db/attrTuples (db/-rschema db))))

    (is (thrown-msg?
          ":t2 :db/tupleAttrs can\u2019t depend on another tuple attribute: :t1"
          (d/empty-db dir1 {:t1 {:db/tupleAttrs [:a :b]}
                            :t2 {:db/tupleAttrs [:c :d :e :t1]}})))

    (is (thrown-msg?
          ":t1 :db/tupleAttrs must be a sequential collection, got: :a"
          (d/empty-db dir2 {:t1 {:db/tupleAttrs :a}})))

    (is (thrown-msg? ":t1 :db/tupleAttrs can’t be empty"
                     (d/empty-db dir3 {:t1 {:db/tupleAttrs ()}})))

    (is (thrown-msg?
          ":t1 has :db/tupleAttrs, must be :db.cardinality/one"
          (d/empty-db dir4 {:t1 {:db/tupleAttrs  [:a :b :c]
                                 :db/cardinality :db.cardinality/many}})))

    (is (thrown-msg?
          ":t1 :db/tupleAttrs can’t depend on :db.cardinality/many attribute: :a"
          (d/empty-db dir5 {:a  {:db/cardinality :db.cardinality/many}
                            :t1 {:db/tupleAttrs [:a :b :c]}})))

    (is (thrown-msg?
          "Bad attribute specification for :foo+bar: {:db/valueType :db.type/tuple} should also have :db/tupleAttrs, :db/tupleTypes, or :db/tupleType"
          (d/empty-db dir6 {:foo+bar {:db/valueType :db.type/tuple}})))

    (is (thrown-with-msg?
          ExceptionInfo #"unsupported tuple component type.*:source"
          (d/empty-db dir7 {:source {:db/valueType :db.type/bytes}
                            :t1     {:db/tupleAttrs [:source :other]}})))

    (is (thrown-with-msg?
          ExceptionInfo #"supported scalar tuple component type.*:db.type/bytes"
          (d/empty-db dir8 {:t1 {:db/valueType :db.type/tuple
                                 :db/tupleType :db.type/bytes}})))

    (is (thrown-with-msg?
          ExceptionInfo #"supported scalar tuple component types.*:db.type/bytes"
          (d/empty-db dir9 {:t1 {:db/valueType  :db.type/tuple
                                 :db/tupleTypes [:db.type/string
                                                 :db.type/bytes]}})))

    (d/close-db db)
    (u/delete-files dir)))

(deftest test-tx
  (let [dir  (u/tmp-dir (str "tuples-" (UUID/randomUUID)))
        conn (d/create-conn
               dir {:a+b   {:db/tupleAttrs [:a :b]}
                    :a+c+d {:db/tupleAttrs [:a :c :d]}})]
    (are [tx datoms] (= datoms
                        (tdc/all-datoms (:db-after (d/transact! conn tx))))
      [[:db/add 1 :a "a"]]
      #{[1 :a     "a"]
        [1 :a+b   ["a" nil]]
        [1 :a+c+d ["a" nil nil]]}

      [[:db/add 1 :b "b"]]
      #{[1 :a     "a"]
        [1 :b     "b"]
        [1 :a+b   ["a" "b"]]
        [1 :a+c+d ["a" nil nil]]}

      [[:db/add 1 :a "A"]]
      #{[1 :a     "A"]
        [1 :b     "b"]
        [1 :a+b   ["A" "b"]]
        [1 :a+c+d ["A" nil nil]]}

      [[:db/add 1 :c "c"]
       [:db/add 1 :d "d"]]
      #{[1 :a     "A"]
        [1 :b     "b"]
        [1 :a+b   ["A" "b"]]
        [1 :c     "c"]
        [1 :d     "d"]
        [1 :a+c+d ["A" "c" "d"]]}

      [[:db/add 1 :a "a"]]
      #{[1 :a     "a"]
        [1 :b     "b"]
        [1 :a+b   ["a" "b"]]
        [1 :c     "c"]
        [1 :d     "d"]
        [1 :a+c+d ["a" "c" "d"]]}

      [[:db/add 1 :a "A"]
       [:db/add 1 :b "B"]
       [:db/add 1 :c "C"]
       [:db/add 1 :d "D"]]
      #{[1 :a     "A"]
        [1 :b     "B"]
        [1 :a+b   ["A" "B"]]
        [1 :c     "C"]
        [1 :d     "D"]
        [1 :a+c+d ["A" "C" "D"]]}

      [[:db/retract 1 :a "A"]]
      #{[1 :b     "B"]
        [1 :a+b   [nil "B"]]
        [1 :c     "C"]
        [1 :d     "D"]
        [1 :a+c+d [nil "C" "D"]]}

      [[:db/retract 1 :b "B"]]
      #{[1 :c     "C"]
        [1 :d     "D"]
        [1 :a+c+d [nil "C" "D"]]})

    (is (thrown-msg?
          "Can’t modify tuple attrs directly: [:db/add 1 :a+b [\"A\" \"B\"]]"
          (d/transact! conn [{:db/id 1 :a+b ["A" "B"]}])))
    (d/close conn)
    (u/delete-files dir)))

(deftest test-ignore-correct
  (let [dir  (u/tmp-dir (str "tuples-" (UUID/randomUUID)))
        conn (d/create-conn dir {:a+b {:db/tupleAttrs [:a :b]}})]
    (testing "insert"
      (d/transact! conn [{:db/id 1 :a "a" :b "b" :a+b ["a" "b"]}])
      (is (thrown-msg? "Can’t modify tuple attrs directly: [:db/add 2 :a+b [\"a\" \"b\"]]"
                       (d/transact! conn [{:db/id 2 :a "x" :b "y" :a+b ["a" "b"]}])))
      (is (thrown-msg? "Can’t modify tuple attrs directly: [:db/add 2 :a+b [\"a\" \"b\"]]"
                       (d/transact! conn [{:db/id 2 :a+b ["a" "b"] :a "x" :b "y"}])))
      (is (thrown-msg? "Can’t modify tuple attrs directly: [:db/add 2 :a+b [\"a\"]]"
                       (d/transact! conn [{:db/id 2 :a "a" :b "b" :a+b ["a"]}])))
      (is (thrown-msg? "Can’t modify tuple attrs directly: [:db/add 2 :a+b [\"a\" nil]]"
                       (d/transact! conn [{:db/id 2 :a "a" :b "b" :a+b ["a" nil]}]))))

    (testing "update"
      (is (thrown-msg? "Can’t modify tuple attrs directly: [:db/add 1 :a+b [\"a\" \"b\"]]"
                       (d/transact! conn [{:db/id 1 :a "x" :a+b ["a" "b"]}])))
      (is (thrown-msg? "Can’t modify tuple attrs directly: [:db/add 1 :a+b [\"a\" \"B\"]]"
                       (d/transact! conn [{:db/id 1 :a+b ["a" "B"]}])))
      (is (thrown-msg? "Can’t modify tuple attrs directly: [:db/add 1 :a+b [\"a\"]]"
                       (d/transact! conn [{:db/id 1 :a "a" :b "b" :a+b ["a"]}])))
      (is (thrown-msg? "Can’t modify tuple attrs directly: [:db/add 1 :a+b [\"a\" nil]]"
                       (d/transact! conn [{:db/id 1 :a "a" :b "b" :a+b ["a" nil]}])))
      (d/transact! conn [{:db/id 1 :a+b ["a" "b"]}])
      (d/transact! conn [{:db/id 1 :b "B" :a+b ["a" "B"]}])
      (d/transact! conn [{:db/id 1 :a+b ["A" "B"] :a "A"}]))
    (is (= #{[1 :b "B"] [1 :a+b ["A" "B"]] [1 :a "A"]}
           (tdc/all-datoms (d/db conn))))
    (d/close conn)
    (u/delete-files dir)))

(deftest test-unique
  (let [dir  (u/tmp-dir (str "tuples-" (UUID/randomUUID)))
        conn (d/create-conn
               dir {:a+b {:db/tupleAttrs [:a :b]
                          :db/unique     :db.unique/identity}})]
    (d/transact! conn [[:db/add 1 :a "a"]])
    (d/transact! conn [[:db/add 2 :a "A"]])
    (is (thrown-with-msg? ExceptionInfo #"Cannot add .* because of unique constraint: .*"
                          (d/transact! conn [[:db/add 1 :a "A"]])))

    (d/transact! conn [[:db/add 1 :b "b"]
                       [:db/add 2 :b "b"]
                       {:db/id 3 :a "a" :b "B"}])

    (is (= #{[1 :a "a"]
             [1 :b "b"]
             [1 :a+b ["a" "b"]]
             [2 :a "A"]
             [2 :b "b"]
             [2 :a+b ["A" "b"]]
             [3 :a "a"]
             [3 :b "B"]
             [3 :a+b ["a" "B"]]}
           (tdc/all-datoms (d/db conn))))

    (is (thrown-with-msg? ExceptionInfo #"Cannot add .* because of unique constraint: .*"
                          (d/transact! conn [[:db/add 1 :a "A"]])))
    (is (thrown-with-msg? ExceptionInfo #"Cannot add .* because of unique constraint: .*"
                          (d/transact! conn [[:db/add 1 :b "B"]])))
    (is (thrown-with-msg? ExceptionInfo #"Cannot add .* because of unique constraint: .*"
                          (d/transact! conn [[:db/add 1 :a "A"]
                                             [:db/add 1 :b "B"]])))

    (testing "multiple tuple updates"
      ;; changing both tuple components in a single operation
      (d/transact! conn [{:db/id 1 :a "A" :b "B"}])
      (is (= {:db/id 1 :a "A" :b "B" :a+b ["A" "B"]}
             (d/pull (d/db conn) '[*] 1)))

      ;; adding entity with two tuple components in a single operation
      (d/transact! conn [{:db/id 4 :a "a" :b "b"}])
      (is (= {:db/id 4 :a "a" :b "b" :a+b ["a" "b"]}
             (d/pull (d/db conn) '[*] 4))))
    (d/close conn)
    (u/delete-files dir)))

(deftest test-upsert
  (let [dir  (u/tmp-dir (str "tuples-" (UUID/randomUUID)))
        conn (d/create-conn
               dir {:a+b {:db/tupleAttrs [:a :b]
                          :db/unique     :db.unique/identity}
                    :c   {:db/unique :db.unique/identity}})]
    (d/transact! conn
                 [{:db/id 1 :a "A" :b "B"}
                  {:db/id 2 :a "a" :b "b"}])

    (d/transact! conn [{:a+b ["A" "B"] :c "C"}
                       {:a+b ["a" "b"] :c "c"}])
    (is (= #{[1 :a "A"]
             [1 :b "B"]
             [1 :a+b ["A" "B"]]
             [1 :c "C"]
             [2 :a "a"]
             [2 :b "b"]
             [2 :a+b ["a" "b"]]
             [2 :c "c"]}
           (tdc/all-datoms (d/db conn))))

    (is (thrown-msg? "Conflicting upserts: [:a+b [\"A\" \"B\"]] resolves to 1, but [:c \"c\"] resolves to 2"
                     (d/transact! conn [{:a+b ["A" "B"] :c "c"}])))

    ;; change tuple + upsert
    (d/transact! conn
                 [{:a+b ["A" "B"]
                   :b   "b"
                   :d   "D"}])

    (is (= #{[1 :a "A"]
             [1 :b "b"]
             [1 :a+b ["A" "b"]]
             [1 :c "C"]
             [1 :d "D"]
             [2 :a "a"]
             [2 :b "b"]
             [2 :a+b ["a" "b"]]
             [2 :c "c"]}
           (tdc/all-datoms (d/db conn))))
    (d/close conn)
    (u/delete-files dir)))

(deftest test-upsert-by-tuple-components
  (let [dir (u/tmp-dir (str "tuples-" (UUID/randomUUID)))
        db  (d/empty-db dir {:a+b {:db/tupleAttrs [:a :b]
                                   :db/unique     :db.unique/identity}})
        db' (d/db-with db [{:a "A" :b "B" :name "Ivan"}])]
    (is (= #{[1 :a "A"]
             [1 :b "B"]
             [1 :a+b ["A" "B"]]
             [1 :name "Oleg"]}
           (tdc/all-datoms
             (d/db-with db'
                        [{:db/id -1 :a "A" :b "B" :name "Oleg"}]))))
    (is (= #{[1 :a "A"]
             [1 :b "B"]
             [1 :a+b ["A" "B"]]
             [1 :name "Oleg"]}
           (tdc/all-datoms
             (d/db-with db'
                        [{:a "A" :b "B" :name "Oleg"}]))))
    (is (= #{[1 :a "A"]
             [1 :b "B"]
             [1 :a+b ["A" "B"]]
             [1 :name "Oleg"]}
           (tdc/all-datoms
             (d/db-with db'
                        [[:db/add -1 :a "A"]
                         [:db/add -1 :b "B"]
                         [:db/add -1 :name "Oleg"]]))))
    (d/close-db db)
    (u/delete-files dir)))

(deftest test-upsert-by-tuple-components-through-reverse-ref
  (let [dir  (u/tmp-dir (str "reverse-ref-tuple-upsert-" (UUID/randomUUID)))
        conn (d/create-conn
               dir
               {:a/name           {:db/valueType :db.type/string
                                   :db/unique    :db.unique/identity}
                :rel/type+from+to {:db/unique     :db.unique/identity
                                   :db/tupleAttrs [:rel/type
                                                   :rel/from
                                                   :rel/to]}
                :rel/from         {:db/valueType :db.type/ref}
                :rel/to           {:db/valueType :db.type/ref}
                :rel/type         {:db/valueType :db.type/keyword}})]
    (d/transact! conn
                 [{:db/id 1 :a/name "A1"}
                  {:db/id 2 :a/name "A2"}
                  {:db/id 3 :a/name "A3"}
                  {:rel/from 1 :rel/to 2 :rel/type :alias}
                  {:rel/from 1 :rel/to 3 :rel/type :alias}])

    (let [before (tdc/all-datoms (d/db conn))]
      (doseq [relations [[{:rel/to 2 :rel/type :alias}
                           {:rel/to 3 :rel/type :alias}]
                         [{:db/id -1 :rel/to 2 :rel/type :alias}
                          {:db/id -2 :rel/to 3 :rel/type :alias}]]]
        (d/transact! conn [{:db/id 1 :rel/_from relations}])
        (is (= before (tdc/all-datoms (d/db conn)))))
      (is (= #{[:alias 1 2] [:alias 1 3]}
             (into #{} (map :v)
                   (d/datoms (d/db conn) :ave :rel/type+from+to)))))
    (d/close conn)
    (u/delete-files dir)))

(deftest test-lookup-refs
  (let [dir  (u/tmp-dir (str "tuples-" (UUID/randomUUID)))
        conn (d/create-conn
               dir {:a+b {:db/tupleAttrs [:a :b]
                          :db/unique     :db.unique/identity}
                    :c   {:db/unique :db.unique/identity}})]
    (d/transact! conn
                 [{:db/id 1 :a "A" :b "B"}
                  {:db/id 2 :a "a" :b "b"}])

    (d/transact! conn [[:db/add [:a+b ["A" "B"]] :c "C"]
                       {:db/id [:a+b ["a" "b"]] :c "c"}])
    (is (= #{[1 :a "A"]
             [1 :b "B"]
             [1 :a+b ["A" "B"]]
             [1 :c "C"]
             [2 :a "a"]
             [2 :b "b"]
             [2 :a+b ["a" "b"]]
             [2 :c "c"]}
           (tdc/all-datoms (d/db conn))))

    (is (thrown-with-msg? ExceptionInfo #"Cannot add .* because of unique constraint: .*"
                          (d/transact! conn [[:db/add [:a+b ["A" "B"]] :c "c"]])))

    (is (thrown-msg? "Conflicting upsert: [:c \"c\"] resolves to 2, but entity already has :db/id 1"
                     (d/transact! conn [{:db/id [:a+b ["A" "B"]] :c "c"}])))

    ;; change tuple + upsert
    (d/transact! conn
                 [{:db/id [:a+b ["A" "B"]]
                   :b     "b"
                   :d     "D"}])

    (is (= #{[1 :a "A"]
             [1 :b "b"]
             [1 :a+b ["A" "b"]]
             [1 :c "C"]
             [1 :d "D"]
             [2 :a "a"]
             [2 :b "b"]
             [2 :a+b ["a" "b"]]
             [2 :c "c"]}
           (tdc/all-datoms (d/db conn))))

    (is (= {:db/id 2
            :a     "a"
            :b     "b"
            :a+b   ["a" "b"]
            :c     "c"}
           (d/pull (d/db conn) '[*] [:a+b ["a" "b"]])))
    (d/close conn)
    (u/delete-files dir)))

(deftest test-validation
  (let [dir (u/tmp-dir (str "tuples-" (UUID/randomUUID)))
        db  (d/empty-db dir {:a+b {:db/tupleAttrs [:a :b]}})
        db1 (d/db-with db [[:db/add 1 :a "a"]])]
    (is (thrown-msg? "Can’t modify tuple attrs directly: [:db/add 1 :a+b [nil nil]]"
                     (d/db-with db [[:db/add 1 :a+b [nil nil]]])))
    (is (thrown-msg? "Can’t modify tuple attrs directly: [:db/add 1 :a+b [\"a\" nil]]"
                     (d/db-with db1 [[:db/add 1 :a+b ["a" nil]]])))
    (is (thrown-msg? "Can’t modify tuple attrs directly: [:db/add 1 :a+b [\"a\" nil]]"
                     (d/db-with db [[:db/add 1 :a "a"]
                                    [:db/add 1 :a+b ["a" nil]]])))
    (is (thrown-msg? "Can’t modify tuple attrs directly: [:db/retract 1 :a+b [\"a\" nil]]"
                     (d/db-with db1 [[:db/retract 1 :a+b ["a" nil]]])))
    (d/close-db db)
    (u/delete-files dir)))

(deftest test-byte-source-schema-update-is-rejected
  (let [dir  (u/tmp-dir (str "tuple-byte-source-" (UUID/randomUUID)))
        conn (d/create-conn
               dir {:a+b {:db/tupleAttrs [:a :b]}})]
    (is (thrown-with-msg?
          ExceptionInfo #"unsupported tuple component type.*:a.*:db.type/bytes"
          (d/update-schema conn {:a {:db/valueType :db.type/bytes}})))
    (d/close conn)
    (u/delete-files dir)))

(deftest test-indexes
  (let [dir (u/tmp-dir (str "tuples-" (UUID/randomUUID)))
        db  (-> (d/empty-db dir {:a+b+c {:db/tupleAttrs [:a :b :c]}})
                (d/db-with
                  [{:db/id 1 :a "a" :b "b" :c "c"}
                   {:db/id 2 :a "A" :b "b" :c "c"}
                   {:db/id 3 :a "a" :b "B" :c "c"}
                   {:db/id 4 :a "A" :b "B" :c "c"}
                   {:db/id 5 :a "a" :b "b" :c "C"}
                   {:db/id 6 :a "A" :b "b" :c "C"}
                   {:db/id 7 :a "a" :b "B" :c "C"}
                   {:db/id 8 :a "A" :b "B" :c "C"}]))]
    (is (= [6]
           (mapv :e (d/datoms db :ave :a+b+c ["A" "b" "C"]))))
    (is (= []
           (mapv :e (d/datoms db :ave :a+b+c ["A" "b" nil]))))
    (is (= [8 4 6 2]
           (mapv :e (d/index-range db :a+b+c ["A" "B" "C"] ["A" "b" "c"]))))
    (is (= [8 4]
           (mapv :e (d/index-range db :a+b+c ["A" "B" nil] ["A" "b" nil]))))
    (d/close-db db)
    (u/delete-files dir)))

(deftest test-composite-tuple-infers-component-types
  (let [dir  (u/tmp-dir (str "typed-composite-tuples-" (UUID/randomUUID)))
        conn (d/create-conn
               dir
               {:order/sku      {:db/valueType :db.type/long}
                :order/type     {:db/valueType :db.type/keyword}
                :order/sku+type {:db/tupleAttrs [:order/sku :order/type]}})]
    (is (= {:db/valueType :db.type/tuple
            :db/tupleAttrs [:order/sku :order/type]
            :db/tupleTypes [:db.type/long :db.type/keyword]}
           (dissoc (get (d/schema conn) :order/sku+type) :db/aid)))

    (d/transact! conn [{:db/id 1 :order/sku -10 :order/type :type/b}
                       {:db/id 2 :order/sku -2 :order/type :type/z}
                       {:db/id 3 :order/sku -2 :order/type :type/a}
                       {:db/id 4 :order/sku 0 :order/type :type/m}
                       {:db/id 5 :order/sku 10 :order/type :type/a}
                       {:db/id 6 :order/sku -2}])

    (let [db (d/db conn)]
      (is (= [[-10 :type/b]
              [-2 nil]
              [-2 :type/a]
              [-2 :type/z]
              [0 :type/m]
              [10 :type/a]]
             (mapv :v (d/datoms db :ave :order/sku+type))))
      (is (= [6 3 2 4]
             (mapv :e
                   (d/index-range db :order/sku+type
                                  [-2 nil] [0 :type/m])))))
    (d/close conn)
    (u/delete-files dir)))

(deftest test-composite-tuple-type-update-migrates-existing-values
  (let [dir  (u/tmp-dir (str "migrate-composite-tuples-" (UUID/randomUUID)))
        conn (d/create-conn
               dir
               {:order/sku+type {:db/tupleAttrs [:order/sku :order/type]}})]
    (d/transact! conn [{:db/id 1 :order/sku 10 :order/type :type/a}
                       {:db/id 2 :order/sku -2 :order/type :type/z}
                       {:db/id 3 :order/sku 0 :order/type :type/m}])

    (d/update-schema conn
                     {:order/sku  {:db/valueType :db.type/long}
                      :order/type {:db/valueType :db.type/keyword}})

    (is (= [:db.type/long :db.type/keyword]
           (get-in (d/schema conn) [:order/sku+type :db/tupleTypes])))
    (is (= [[-2 :type/z] [0 :type/m] [10 :type/a]]
           (mapv :v (d/datoms (d/db conn) :ave :order/sku+type))))

    (d/close conn)
    (let [conn' (d/create-conn dir)]
      (is (= [:db.type/long :db.type/keyword]
             (get-in (d/schema conn') [:order/sku+type :db/tupleTypes])))
      (is (= [[-2 :type/z] [0 :type/m] [10 :type/a]]
             (mapv :v (d/datoms (d/db conn') :ave :order/sku+type))))
      (d/close conn'))
    (u/delete-files dir)))

(deftest test-queries
  (let [dir (u/tmp-dir (str "tuples-" (UUID/randomUUID)))
        db  (-> (d/empty-db dir {:a+b {:db/tupleAttrs [:a :b]
                                       :db/unique     :db.unique/identity}})
                (d/db-with [{:db/id 1 :a "A" :b "B"}
                            {:db/id 2 :a "A" :b "b"}
                            {:db/id 3 :a "a" :b "B"}
                            {:db/id 4 :a "a" :b "b"}]))]
    (is (= #{[3]}
           (d/q '[:find ?e
                  :where [?e :a+b ["a" "B"]]] db)))

    (is (= #{[["a" "B"]]}
           (d/q '[:find ?a+b
                  :where [[:a+b ["a" "B"]] :a+b ?a+b]] db)))

    (is (= #{[["A" "B"]] [["A" "b"]] [["a" "B"]] [["a" "b"]]}
           (d/q '[:find ?a+b
                  :where [?e :a ?a]
                  [?e :b ?b]
                  [(tuple ?a ?b) ?a+b]] db)))

    (is (= #{["A" "B"] ["A" "b"] ["a" "B"] ["a" "b"]}
           (d/q '[:find ?a ?b
                  :where [?e :a+b ?a+b]
                  [(untuple ?a+b) [?a ?b]]] db)))
    (d/close-db db)
    (u/delete-files dir)))

(deftest test-homogeneous-and-heterogeneous-tuple
  (let [dir (u/tmp-dir (str "homo-tuples-" (UUID/randomUUID)))
        db  (-> (d/empty-db dir
                            {:a {:db/valueType :db.type/tuple
                                 :db/tupleType :db.type/string
                                 :db/unique    :db.unique/identity}
                             :b {:db/valueType  :db.type/tuple
                                 :db/tupleTypes [:db.type/keyword
                                                 :db.type/long
                                                 :db.type/string]}}
                            {:validate-data? true})
                (d/db-with [{:db/id 1 :a ["A" "B"] :b [:id 1 "AB"]}
                            {:db/id 2 :a ["A" "b"] :b [:id 2 "Ab"]}
                            {:db/id 3 :a ["a" "B"] :b [:id 3 "aB"]}
                            {:db/id 4 :a ["a" "b"] :b [:id 4 "ab"]}]))]
    (is (= #{[3]}
           (d/q '[:find ?e
                  :where [?e :a ["a" "B"]]] db)))
    (is (= #{[["a" "B"]]}
           (d/q '[:find ?a
                  :where [[:a ["a" "B"]] :a ?a]] db)))
    (is (= #{[3] [4]}
           (d/q '[:find ?e
                  :where
                  [?e :b ?b]
                  [(untuple ?b) [_ ?l _]]
                  [(< 2 ?l)]] db)))
    (d/close-db db)
    (u/delete-files dir)))

(deftest test-validated-composite-tuple-with-missing-component
  (let [dir  (u/tmp-dir (str "partial-composite-tuple-" (UUID/randomUUID)))
        attr :accessMethod/custProfile+name+acct
        conn (d/create-conn
               dir
               {:accessMethod/name
                {:db/valueType :db.type/string}
                :accessMethod/forAccount
                {:db/valueType :db.type/ref}
                :accessMethod/forCustProfile
                {:db/valueType :db.type/ref}
                attr
                {:db/valueType  :db.type/tuple
                 :db/tupleAttrs [:accessMethod/forCustProfile
                                 :accessMethod/name
                                 :accessMethod/forAccount]
                 :db/unique     :db.unique/identity}}
               {:validate-data? true})]
    (try
      (d/transact! conn [{:db/id                       1
                          :accessMethod/forCustProfile 12
                          :accessMethod/name           "test"}])
      (is (= [12 "test" nil]
             (d/q '[:find ?value .
                    :in $ ?attr
                    :where [1 ?attr ?value]]
                  (d/db conn) attr)))
      (finally
        (d/close conn)
        (u/delete-files dir)))))

(deftest test-variable-width-final-component-prefix-order
  (let [dir  (u/tmp-dir (str "tuple-prefix-order-" (UUID/randomUUID)))
        conn (d/create-conn
               dir
               {:homogeneous {:db/valueType :db.type/tuple
                              :db/tupleType :db.type/string}
                :heterogeneous {:db/valueType  :db.type/tuple
                                :db/tupleTypes [:db.type/long
                                                :db.type/string]}})
        tx   [{:db/id 1 :homogeneous [""]   :heterogeneous [0 ""]}
              {:db/id 2 :homogeneous ["a"]  :heterogeneous [0 "a"]}
              {:db/id 3 :homogeneous ["ab"] :heterogeneous [0 "ab"]}
              {:db/id 4 :homogeneous ["b"]  :heterogeneous [0 "b"]}]]
    (d/transact! conn tx)
    (let [db (d/db conn)]
      (is (= [[""] ["a"] ["ab"] ["b"]]
             (mapv :v (d/datoms db :ave :homogeneous))))
      (is (= [[0 ""] [0 "a"] [0 "ab"] [0 "b"]]
             (mapv :v (d/datoms db :ave :heterogeneous))))
      (is (= [1 2 3]
             (mapv :e (d/index-range db :homogeneous [""] ["ab"]))))
      (is (= [1 2 3]
             (mapv :e
                   (d/index-range db :heterogeneous [0 ""] [0 "ab"])))))
    (d/close conn)

    (let [conn (d/create-conn dir)]
      (is (= [[""] ["a"] ["ab"] ["b"]]
             (mapv :v (d/datoms (d/db conn) :ave :homogeneous))))
      (is (= [[0 ""] [0 "a"] [0 "ab"] [0 "b"]]
             (mapv :v (d/datoms (d/db conn) :ave :heterogeneous))))
      (d/close conn))
    (u/delete-files dir)))

(deftest test-homogeneous-and-heterogeneous-tuple-with-ref
  (let [dir (u/tmp-dir (str "homo-tuples-with-ref-" (UUID/randomUUID)))
        db  (-> (d/empty-db dir
                            {:a {:db/valueType :db.type/tuple
                                 :db/tupleType :db.type/ref
                                 :db/unique    :db.unique/identity}
                             :b {:db/valueType  :db.type/tuple
                                 :db/tupleTypes [:db.type/ref
                                                 :db.type/string]}
                             :c {:db/unique :db.unique/identity}})
                (d/db-with [{:db/id -1 :a [-2] :c "A"}
                            {:db/id -2 :b [-1 "B"] :c "B"}]))]
    (is (= #{["A"]}
           (d/q '[:find ?c
                  :where
                  [?e1 :c ?c]
                  [?e1 :a ?a]
                  [(untuple ?a) [?e2 ...]]
                  [?e2 :c "B"]]
                db)))
    (is (= #{["B"]}
           (d/q '[:find ?c
                  :where
                  [?e1 :c ?c]
                  [?e1 :b ?b]
                  [(untuple ?b) [?e2 _]]
                  [?e2 :c "A"]]
                db)))
    (d/close-db db)
    (u/delete-files dir)))

(deftest test-ident-in-tuple
  (let [dir  (u/tmp-dir (str "ident-in-tuple" (UUID/randomUUID)))
        conn (d/get-conn
               dir
               {:artist/name {:db/valueType   :db.type/string
                              :db/cardinality :db.cardinality/one
                              :db/unique      :db.unique/identity}

                :artist/country
                {:db/valueType   :db.type/ref
                 :db/cardinality :db.cardinality/one
                 :db/doc         "An artist's country of residence"}

                :artist/l11n {:db/valueType   :db.type/tuple
                              :db/tupleTypes  [:db.type/string :db.type/ref]
                              :db/cardinality :db.cardinality/many}}
               {:validate-data? true})]
    (d/transact conn [{:db/ident :country/CA}
                      {:db/ident :country/JP}
                      {:db/ident :country/CN}])
    (d/transact conn [{:artist/name    "Leonard Cohen"
                       :artist/country :country/CA
                       :artist/l11n    [["レナード・コーエン" :country/JP]
                                        ["列昂纳德·科恩" :country/CN]]}])
    (is (= 1 (d/q '[:find (count ?a) .
                    :in $ ?n
                    :where
                    [?a :artist/name ?n]]
                  (d/db conn) "Leonard Cohen")))
    (d/close conn)
    (u/delete-files dir)))
