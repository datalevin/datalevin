(ns datalevin.idoc-root-test
  (:require
   [clojure.test :refer [deftest is testing use-fixtures]]
   [datalevin.built-ins :as bi]
   [datalevin.core :as d]
   [datalevin.idoc :as idoc]
   [datalevin.interface :as i]
   [datalevin.storage :as s]
   [datalevin.test.core :refer [db-fixture]]
   [datalevin.util :as u])
  (:import [datalevin.idoc IdocIndex]))

(use-fixtures :each db-fixture)

(deftest legacy-string-path-decoding-test
  (doseq [key ["#01" "#00" "#999999999999999999999999999999"]]
    (is (= [key] (idoc/decode-path (str "/" key))))
    (is (= [key] (idoc/decode-path (idoc/encode-path [key])))))
  (doseq [position [0 1 10 Long/MAX_VALUE]]
    (is (= [position] (idoc/decode-path (idoc/encode-path [position]))))))

(defn- match-ids [db attr query]
  (d/q '[:find [?e ...]
         :in $ ?attr ?query
         :where [(idoc-match $ ?attr ?query) [[?e _ _]]]]
       db attr query))

(deftest legacy-path-does-not-shadow-vector-position-test
  (let [dir (u/tmp-dir (str "idoc-legacy-path-" (random-uuid)))
        schema {:doc {:db/valueType :db.type/idoc}}
        conn (d/create-conn dir schema)]
    (try
      (d/transact! conn [{:db/id 1 :doc {:tags [8 9]}}
                         {:db/id 2 :doc {:tags {"#01" 7}}}])
      (let [^IdocIndex index ((s/store-idoc-indices (:store @conn)) "doc")]
        ;; Legacy string keys were unescaped. Retain an old dictionary entry
        ;; alongside the new positional entries, as an index rebuild can do.
        (i/transact-kv (.-lmdb index)
                       [[:put (.-path-dict-dbi index) "/:tags/#01"
                         10000 :string :int]]))
      (d/close conn)
      (let [reopened (d/create-conn dir schema)]
        (try
          (is (= #{1} (set (match-ids @reopened :doc {:tags {1 9}}))))
          (is (= #{2} (set (match-ids @reopened :doc {:tags {"#01" 7}}))))
          (finally (d/close reopened))))
      (finally
        (d/close conn)
        (u/delete-files dir)))))

(deftest vector-root-parsing-test
  (doseq [props [{} {:db/idocFormat :edn} {:db/idocFormat :json}]]
    (is (= [1 2 5 3 5]
           (idoc/parse-value :attr1 props {} [1 2 5 3 5])))
    (is (= [1 2 5 3 5]
           (idoc/parse-value :attr1 props {} "[1,2,5,3,5]"))
        (str props)))
  (is (= [1 :json/null {:tags [2 2]}]
         (idoc/parse-value :attr1 {} {} [1 nil {:tags [2 2]}])))
  (is (= [1 :json/null]
         (idoc/parse-value :attr1 {:db/idocFormat :json} {} "[1,null]")))
  (doseq [value [1 true nil :value '(1 2) "1" "(1 2)"]]
    (is (thrown-with-msg? Exception #"Idoc root must be a map or vector"
                          (idoc/parse-value :attr1 {} {} value))))
  (is (thrown-with-msg? Exception #"Lists are not valid idoc values"
                        (idoc/parse-value :attr1 {} {} [1 '(2 3)]))))

(deftest vector-root-query-test
  (let [dir    (u/tmp-dir (str "idoc-root-query-" (random-uuid)))
        schema {:attr1 {:db/valueType :db.type/idoc}
                :doc   {:db/valueType :db.type/idoc}}
        conn   (d/create-conn dir schema)]
    (try
      (d/transact! conn [{:db/id 1 :attr1 [1 2 5 3 5]}
                         {:db/id 2 :attr1 [2 4]}
                         {:db/id 3 :attr1 []}
                         {:db/id 4 :attr1 [0 6]}
                         {:db/id 8 :doc {:tags [1 2 5 3 5]}}])
      (testing "membership returns the original vector once"
        (is (= #{[1 [1 2 5 3 5]]}
               (d/q '[:find ?e ?v
                      :where [(idoc-match $ :attr1 5) [[?e _ ?v]]]]
                    @conn)))
        (is (= {:attr1 [1 2 5 3 5]} (d/pull @conn [:attr1] 1)))
        (is (= [1 2 5 3 5] (:attr1 (d/entity @conn 1))))
        (is (= [] (:attr1 (d/entity @conn 3))))
        (is (= [1 2 5 3 5] (bi/idoc-get (:attr1 (d/entity @conn 1)) [])))
        (is (= #{[1]}
               (d/q '[:find ?e :where [?e :attr1 [1 2 5 3 5]]] @conn)))
        (is (= #{[1 :attr1 [1 2 5 3 5]]}
               (d/q '[:find ?e ?a ?v
                      :where [(idoc-match $ 5) [[?e ?a ?v]]]] @conn))))
      (testing "logical queries and predicates reuse root membership"
        (doseq [[query expected]
                [[5 #{1}]
                 [[:and 1 5] #{1}]
                 [[:or 2 4] #{1 2}]
                 [[:not 5] #{2 3 4}]
                 [{1 2} #{1}]
                 [{(int 1) 2} #{1}]
                 [{1 3} #{}]
                 [{3 3} #{1}]
                 [{5 3} #{}]
                 ['(> [1] 3) #{2 4}]
                 ['(< 0 [1] 4) #{1}]
                 ['(> [] 3) #{1 2 4}]
                 ['(>= [] 5) #{1 4}]
                 ['(< 2 [] 5) #{1 2}]
                 ['(<= 2 [] 4) #{1 2}]
                 ['(< [] 0) #{}]
                 ['(nil? []) #{}]]]
          (is (= expected (set (match-ids @conn :attr1 query))) (str query)))
        (is (thrown-with-msg? Exception #"Use \(nil\? :field\)"
                              (bi/idoc-match @conn :attr1 nil))))
      (testing "existing nested vector queries keep their semantics"
        (is (= #{8} (set (match-ids @conn :doc {:tags 5}))))
        (is (= #{8} (set (match-ids @conn :doc {:tags [:and 1 5]}))))
        (is (= #{8} (set (match-ids @conn :doc '(< 2 [:tags] 5)))))
        (is (empty? (match-ids @conn :doc 5))))
      (testing "replacement and indexed patch paths update membership"
        (d/transact! conn [{:db/id 1 :attr1 [9 9 2]}])
        (is (empty? (match-ids @conn :attr1 5)))
        (is (= #{1} (set (match-ids @conn :attr1 9))))
        (d/transact! conn [[:db.fn/patchIdoc 1 :attr1
                            [[:set [0] 7] [:unset [1]]]]])
        (is (= [7 2] (:attr1 (d/entity @conn 1))))
        (is (empty? (match-ids @conn :attr1 9)))
        (is (= #{1} (set (match-ids @conn :attr1 7)))))
      (d/close conn)
      (let [reopened (d/create-conn dir schema)]
        (try
          (testing "root path index survives reopening and retraction"
            (is (= [7 2] (:attr1 (d/entity @reopened 1))))
            (is (= #{1} (set (match-ids @reopened :attr1 7))))
            (d/transact! reopened [[:db/retract 1 :attr1 [7 2]]])
            (is (empty? (match-ids @reopened :attr1 7))))
          (finally (d/close reopened))))
      (finally
        (d/close conn)
        (u/delete-files dir)))))

(deftest vector-root-large-value-and-format-test
  (let [dir    (u/tmp-dir (str "idoc-root-formats-" (random-uuid)))
        schema {:attr1 {:db/valueType :db.type/idoc}
                :json  {:db/valueType :db.type/idoc :db/idocFormat :json}
                :many  {:db/valueType :db.type/idoc
                        :db/cardinality :db.cardinality/many}}
        conn   (d/create-conn dir schema)
        prefix (apply str (repeat 1000 "x"))
        large  (vec (range 2000))]
    (try
      (d/transact! conn [{:db/id 1 :attr1 [(str prefix "a")]}
                         {:db/id 2 :attr1 [(str prefix "b")]}
                         {:db/id 3 :attr1 large}
                         {:db/id 4 :json "[1,2,5,3,5,null]"}
                         {:db/id 5 :many [[1 5 5] [2 5]]}])
      (testing "truncated index values are verified against the root vector"
        (is (= #{1} (set (match-ids @conn :attr1 (str prefix "a")))))
        (is (= #{2} (set (match-ids @conn :attr1 (str prefix "b")))))
        (is (empty? (match-ids @conn :attr1 (str prefix "c")))))
      (testing "giant documents retain vector values"
        (is (= #{[3 large]}
               (d/q '[:find ?e ?v
                      :where [(idoc-match $ :attr1 1999) [[?e _ ?v]]]] @conn)))
        (d/transact! conn [{:db/id 3 :attr1 [2000]}])
        (is (empty? (match-ids @conn :attr1 1999)))
        (is (= #{3} (set (match-ids @conn :attr1 2000)))))
      (testing "JSON arrays and null predicates"
        (is (= [1 2 5 3 5 :json/null] (:json (d/entity @conn 4))))
        (is (= #{4} (set (match-ids @conn :json 5))))
        (is (= #{4} (set (match-ids @conn :json '(nil? [])))))
        (is (= #{4} (set (match-ids @conn :json '(nil? [5]))))))
      (testing "cardinality many stores whole vectors as separate values"
        (is (= #{[5 [1 5 5]] [5 [2 5]]}
               (d/q '[:find ?e ?v
                      :where [(idoc-match $ :many 5) [[?e _ ?v]]]] @conn)))
        (d/transact! conn [[:db/retract 5 :many [1 5 5]]])
        (is (= #{[5 [2 5]]}
               (d/q '[:find ?e ?v
                      :where [(idoc-match $ :many 5) [[?e _ ?v]]]] @conn))))
      (finally
        (d/close conn)
        (u/delete-files dir)))))
