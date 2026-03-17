(ns datalevin.test.query-fns
  (:require
   [datalevin.test.core :as tdc :refer [db-fixture]]
   [clojure.test :refer [deftest testing are is use-fixtures]]
   [clojure.string :as str]
   [datalevin.util :as u]
   [datalevin.core :as d]
   [datalevin.udf :as udf])
  (:import [clojure.lang ExceptionInfo]
           [java.util UUID]))

(use-fixtures :each db-fixture)

(deftest test-like-in-or
  (let [dir (u/tmp-dir (str "fns-test-" (UUID/randomUUID)))
        db  (-> (d/empty-db dir {:text {:db/valueType :db.type/string}
                                 :same {:db/valueType   :db.type/ref
                                        :db/cardinality :db.cardinality/many}})
                (d/db-with
                  [{:db/id 1, :text "Champions Forever: The Latin Legends"
                    :same  [2 4 5]}
                   {:db/id 2, :text "Champions" :same [1 4 5]}
                   {:db/id 3, :text "Loser" :same [11]}
                   {:db/id 4, :text "Champion" :same [1 2 5]}
                   {:db/id 5, :text "Champion" :same [1 2 4]}
                   {:db/id 6, :text "Losers Lounge" :same [10]}
                   {:db/id 7, :text "Carman: The Champion" :same [9]}
                   {:db/id 8, :text "Losers Take All"}
                   {:db/id 9, :text "Champion Road: Arena" :same [7]}
                   {:db/id 10, :text "Loser's End" :same [6]}
                   {:db/id 11, :text "Lucky Losers" :same [3]}
                   {:db/id 12, :text "USA:1 August 2007" :same [14]}
                   {:db/id 13, :text "USA:16 September 1999"}
                   {:db/id 14, :text "USA:27 April 2007" :same [12]}
                   {:db/id 15, :text "USA:March 2003" }
                   ]))
        q   '[:find [?e1 ...]
              :in $ ?pat1 ?pat2
              :where
              [?e :text ?t]
              [?e1 :same ?e]
              [(or (like ?t ?pat1) (like ?t ?pat2))]]
        q-t '[:find [?e1 ...]
              :in $ ?pat1 ?pat2 ?pat3
              :where
              [?e :text ?t]
              [?e1 :same ?e]
              [(or (like ?t ?pat1) (like ?t ?pat2) (like ?t ?pat3))]]
        q-e '[:find [?e1 ...]
              :in $ ?pat1 ?pat2 ?pat3 ?pat4
              :where
              [?e :text ?t]
              [?e1 :same ?e]
              [(or (and (like ?t ?pat1) (like ?t ?pat2))
                   (and (like ?t ?pat3) (like ?t ?pat4)))]]
        q-a '[:find [?e ...]
              :in $ ?pat1 ?pat2 ?pat3
              :where
              [?e1 :text ?t]
              [?e :same ?e1]
              [(and (like ?t ?pat1)
                    (or (like ?t ?pat2) (like ?t ?pat3)))]]
        ]
    (are [pat1 pat2 ids] (= (set (d/q q db pat1 pat2)) (set ids))
      "Champion"   "Loser"      [1 2 4 5 11]
      "Champion%"  "Loser"      [1 2 4 5 7 11]
      "Champion"   "Loser%"     [1 2 4 5 6 10 11]
      "Champion%"  "Loser%"     [1 2 4 5 6 7 10 11]
      "%Champion%" "Loser%"     [1 2 4 5 6 7 9 10 11]
      "%Champion%" "%Loser%"    [1 2 3 4 5 6 7 9 10 11]
      "USA:% 199%" "USA:% 200%" [] ;; our fsm is eager, no backtracking
      "USA:%199%"  "USA:%200%"  [14]
      )
    (is (= (set [12 14])
           (set (d/q q-e db "USA:%" "%199_" "USA:%" "%200_"))))
    (is (= (set [12 14])
           (set (d/q q-a db "USA:%" "%199_" "%200_"))))
    (is (= (set [3])
           (set (d/q q-t db "%Lucky%" "%book%" "%random%"))))
    (d/close-db db)
    (u/delete-files dir)))

(deftest test-query-udf
  (let [dir        (u/tmp-dir (str "query-udf-" (UUID/randomUUID)))
        schema     {:email {:db/valueType :db.type/string}}
        descriptor {:udf/lang :test
                    :udf/kind :query-fn
                    :udf/id   :normalize-email}
        registry-1 (doto (udf/create-registry)
                     (udf/register! descriptor str/lower-case))
        registry-2 (doto (udf/create-registry)
                     (udf/register! descriptor str/upper-case))
        conn-1     (d/get-conn
                     dir schema
                     {:runtime-opts {:udf-registry registry-1}})
        _          (d/transact! conn-1 [{:db/id 1 :email "A@B.COM"}])
        query      '[:find ?email .
                     :in $
                     :where
                     [?e :email ?raw]
                     [(udf :normalize-email ?raw) ?email]]
        conn-2     (d/get-conn
                     dir schema
                     {:runtime-opts {:udf-registry registry-2}})]
    (is (not (identical? conn-1 conn-2)))
    (is (= "a@b.com" (d/q query @conn-1)))
    (is (= "A@B.COM" (d/q query @conn-2)))

    (udf/register! registry-1 descriptor #(str/replace % "@" "+"))
    (is (= "A+B.COM" (d/q query @conn-1)))

    (d/close conn-1)
    (d/close conn-2)
    (u/delete-files dir)))

(deftest test-installed-query-udf
  (let [dir        (u/tmp-dir (str "query-installed-udf-" (UUID/randomUUID)))
        schema     {:email {:db/valueType :db.type/string}}
        descriptor {:udf/lang :test
                    :udf/kind :query-fn
                    :udf/id   :normalize-email}
        registry   (doto (udf/create-registry)
                     (udf/register! descriptor str/lower-case))
        conn       (d/create-conn
                     dir schema
                     {:closed-schema? true
                      :runtime-opts  {:udf-registry registry}})]
    (d/transact! conn [{:db/ident :normalize-email
                        :db/udf   descriptor}
                       {:db/id 1 :email "A@B.COM"}])
    (is (= "a@b.com"
           (d/q '[:find ?email .
                  :in $
                  :where
                  [?e :email ?raw]
                  [(udf :normalize-email ?raw) ?email]]
                @conn)))
    (d/close conn)
    (u/delete-files dir)))

(deftest test-inline-query-udf-descriptor
  (let [dir        (u/tmp-dir (str "query-inline-udf-" (UUID/randomUUID)))
        schema     {:email {:db/valueType :db.type/string}}
        descriptor {:udf/lang :test
                    :udf/kind :query-fn
                    :udf/id   :normalize-email}
        registry   (doto (udf/create-registry)
                     (udf/register! descriptor str/lower-case))
        conn       (d/create-conn
                     dir schema
                     {:runtime-opts {:udf-registry registry}})]
    (d/transact! conn [{:db/id 1 :email "A@B.COM"}])
    (is (= "a@b.com"
           (d/q '[:find ?email .
                  :in $ ?descriptor
                  :where
                  [?e :email ?raw]
                  [(udf ?descriptor ?raw) ?email]]
                @conn
                descriptor)))
    (d/close conn)
    (u/delete-files dir)))

(deftest test-like-fn
  (let [dir (u/tmp-dir (str "fns-test-" (UUID/randomUUID)))
        db  (-> (d/empty-db dir {:text {:db/valueType :db.type/string}})
                (d/db-with
                  [{:db/id 1 :text "commander"}
                   {:db/id 2 :text "manner"}
                   ]))
        q   '[:find [?e ...]
              :in $ ?pat
              :where
              [?e :text ?t]
              [(like ?t ?pat)]]]
    (testing "like"
      (are [pattern ids] (= (set (d/q q db pattern)) (set ids))
        "%man%"  [1 2]
        "%mman%" [1]
        ))
    (d/close-db db)
    (u/delete-files dir)))

(deftest test-like-fn-1
  (let [dir (u/tmp-dir (str "fns-test-" (UUID/randomUUID)))
        db  (-> (d/empty-db dir {:text {:db/valueType :db.type/string}})
                (d/db-with
                  [
                   {:db/id 1,
                    :text  "is 好 boy"}
                   {:db/id 2,
                    :text
                    "Mary had a little lamb whose fleece was red as fire."}
                   {:db/id 3,
                    :text
                    "Moby Dick is a story of a whale and a man obsessed?"}
                   {:db/id 4
                    :text  "testing escaping wildcards _, %, and !"}
                   ]))
        q   '[:find [?e ...]
              :in $ ?pat
              :where
              [?e :text ?t]
              [(like ?t ?pat)]]
        q-n '[:find [?e ...]
              :in $ ?pat
              :where
              [?e :text ?t]
              [(not-like ?t ?pat)]]]
    (testing "like"
      (are [pattern ids] (= (set (d/q q db pattern)) (set ids))
        "testing%"   [4]
        "%esc%!_%"   [4]
        "%!%%"       [4]
        "%!!%"       [4]
        "%!!"        [4]
        "%!_%!!"     [4]
        "t%!_%!!"    [4]
        "%t%!_%!!"   [4]
        "%!%, and%"  [4]
        "%and_!!"    [4]
        "%and_!"     nil
        "%a%a%a%"    [2 3 4]
        "%M%"        [2 3]
        "%M%a%"      [2 3]
        "M%"         [2 3]
        "M%y%"       [2 3]
        "M__y%"      [2 3]
        "M_y%"       nil
        "obs%"       nil
        "_M%y%"      nil
        "M%y_"       nil
        "is%"        [1]
        "is%boy"     [1]
        "%is%"       [1 3]
        "_o%y%"      [3]
        "%?"         [3]
        "%red%fire%" [2]
        "%lamb%"     [2]
        "%fire_"     [2]
        "%fire."     [2]
        "%fire%"     [2]
        "%litt%"     [2]
        "%lit%"      [2]
        "%l%it%"     [2]
        "%l%i%t%"    [2]
        "%l%%t%"     [2]
        "%li%e%"     [2]
        "%lit%e%"    [2]
        "%litt%e%"   [2]
        "%li_t_e%"   [2]
        "%obse%tion" nil
        "%boy"       [1]
        "%boy%"      [1]
        "%bo_"       [1]
        "%b%y%"      [1 3]
        "%b_y"       [1]
        "%好%"       [1]
        "%好__oy"    [1]
        "is 好 boy"  [1]
        "好 boy"     nil
        "好"         nil
        ))
    (testing "not-like"
      (are [pattern ids] (= (set (d/q q-n db pattern)) (set ids))
        "testing%"   [1 2 3]
        "%esc%!_%"   [1 2 3]
        "%!%%"       [1 2 3]
        "%!!%"       [1 2 3]
        "%!!"        [1 2 3]
        "%!_%!!"     [1 2 3]
        "t%!_%!!"    [1 2 3]
        "%t%!_%!!"   [1 2 3]
        "%!%, and%"  [1 2 3]
        "%and_!!"    [1 2 3]
        "%and_!"     [1 2 3 4]
        "%a%a%a%"    [1]
        "%M%"        [1 4]
        "%M%a%"      [1 4]
        "M%"         [1 4]
        "M%y%"       [1 4]
        "M__y%"      [1 4]
        "M_y%"       [1 4]
        "obs%"       [1 2 3 4]
        "_M%y%"      [1 2 3 4]
        "M%y_"       [1 4]
        "is%"        [2 3 4]
        "is%boy"     [2 3 4]
        "%is%"       [2 4]
        "_o%y%"      [1 2 4]
        "%?"         [1 2 4]
        "%red%fire%" [1 3 4]
        "%lamb%"     [1 3 4]
        "%fire_"     [1 3 4]
        "%fire."     [1 3 4]
        "%fire%"     [1 3 4]
        "%litt%"     [1 3 4]
        "%lit%"      [1 3 4]
        "%l%it%"     [1 3 4]
        "%l%i%t%"    [1 3 4]
        "%l%%t%"     [1 3 4]
        "%li%e%"     [1 3 4]
        "%lit%e%"    [1 3 4]
        "%litt%e%"   [1 3 4]
        "%li_t_e%"   [1 3 4]
        "%obse%tion" [1 2 3 4]
        "%boy"       [2 3 4]
        "%boy%"      [2 3 4]
        "%bo_"       [2 3 4]
        "%b%y%"      [2 4]
        "%b_y"       [2 3 4]
        "%好%"       [2 3 4]
        "%好__oy"    [2 3 4]
        "is 好 boy"  [2 3 4]
        "好 boy"     [1 2 3 4]
        "好"         [1 2 3 4]
        ))
    (testing "escape character"
      (let [q-e   '[:find [?e ...]
                    :in $ ?pat ?esc
                    :where
                    [?e :text ?t]
                    [(like ?t ?pat {:escape ?esc})]]
            q-e-n '[:find [?e ...]
                    :in $ ?pat ?esc
                    :where
                    [?e :text ?t]
                    [(not-like ?t ?pat {:escape ?esc})]]
            db    (-> db
                      (d/db-with
                        [
                         {:db/id 5
                          :text  "home_value increases 25% in a year!"}
                         {:db/id 6
                          :text  "home value"}
                         {:db/id 7
                          :text  "home_value"}
                         {:db/id 8
                          :text  "book |No. 1|"}
                         {:db/id 9
                          :text  "%_%"}
                         {:db/id 10
                          :text  "_1000_"}
                         {:db/id 11
                          :text  "|title|"}
                         ]))]
        (are [ids pattern] (= (set ids) (set (d/q q-e db pattern \|)))
          [11]         "||%||"
          [11]         "||%||%"
          [11]         "||%"
          [8 11]       "%||"
          [8]          "%||%||%"
          [8]          "%||%||"
          [8 11]       "%||%|"
          [8 11]       "%||%"
          [10]         "%0|_%"
          [10]         "|_%"
          [4 5 7 9 10] "%|_%"
          [10]         "%|_"
          [10]         "|_%|_"
          [9]          "|%|_|%"
          [9]          "|%_|%"
          [9]          "|%%|%"
          [9]          "|%%"
          [4]          "%cards |_%"
          [4 5 9]      "%|%%"
          [5]          "%year!"
          [5]          "%25|%%"
          [8]          "%||No%||"
          [8]          "book ||No. 1||%"
          [8]          "book ||No. 1||"
          [8]          "book ||No. 1_"
          [8]          "book ||No. %"
          [8]          "book ||%"
          [8]          "book %||%"
          [8]          "book %||"
          nil          "%book%||"
          nil          "%book_||"
          [8]          "%book%||%||"
          [8]          "%book%||%||%"
          [8]          "%book%||%"
          [8]          "%book ||%"
          [8]          "_ook ||%"
          [5 6 7]      "home_value%"
          [5 7]        "%home|_value%"
          [5 7]        "home|_value%"
          [7]          "home|_value"
          [6]          "home value"
          [6 7]        "home_value"
          [6 7]        "%value"
          nil          "book |%"
          nil          "home-value%"
          )
        (are [ids pattern] (= (set ids) (set (d/q q-e-n db pattern \|)))
          [1 2 3 4 5 6 7 8 9 10]    "||%||"
          [1 2 3 4 5 6 7 8 9 10]    "||%||%"
          [1 2 3 4 5 6 7 8 9 10]    "||%"
          [1 2 3 4 5 6 7 9 10]      "%||"
          [1 2 3 4 5 6 7 9 10 11]   "%||%||%"
          [1 2 3 4 5 6 7 9 10 11]   "%||%||"
          [1 2 3 4 5 6 7 9 10]      "%||%|"
          [1 2 3 4 5 6 7 9 10]      "%||%"
          [1 2 3 4 5 6 7 8 9 11]    "%0|_%"
          [1 2 3 4 5 6 7 8 9 11]    "|_%"
          [1 2 3 6 8 11]            "%|_%"
          [1 2 3 4 5 6 7 8 9 11]    "%|_"
          [1 2 3 4 5 6 7 8 9 11]    "|_%|_"
          [1 2 3 4 5 6 7 8 10 11]   "|%|_|%"
          [1 2 3 4 5 6 7 8 10 11]   "|%_|%"
          [1 2 3 4 5 6 7 8 10 11]   "|%%|%"
          [1 2 3 4 5 6 7 8 10 11]   "|%%"
          [1 2 3 5 6 7 8 9 10 11]   "%cards |_%"
          [1 2 3 6 7 8 10 11]       "%|%%"
          [1 2 3 4 6 7 8 9 10 11]   "%year!"
          [1 2 3 4 6 7 8 9 10 11]   "%25|%%"
          [1 2 3 4 5 6 7 9 10 11]   "%||No%||"
          [1 2 3 4 5 6 7 9 10 11]   "book ||No. 1||%"
          [1 2 3 4 5 6 7 9 10 11]   "book ||No. 1||"
          [1 2 3 4 5 6 7 9 10 11]   "book ||No. 1_"
          [1 2 3 4 5 6 7 9 10 11]   "book ||No. %"
          [1 2 3 4 5 6 7 9 10 11]   "book ||%"
          [1 2 3 4 5 6 7 9 10 11]   "book %||%"
          [1 2 3 4 5 6 7 9 10 11]   "book %||"
          [1 2 3 4 5 6 7 8 9 10 11] "%book%||"
          [1 2 3 4 5 6 7 8 9 10 11] "%book_||"
          [1 2 3 4 5 6 7 9 10 11]   "%book%||%||"
          [1 2 3 4 5 6 7 9 10 11]   "%book%||%||%"
          [1 2 3 4 5 6 7 9 10 11]   "%book%||%"
          [1 2 3 4 5 6 7 9 10 11]   "%book ||%"
          [1 2 3 4 5 6 7 9 10 11]   "_ook ||%"
          [1 2 3 4 8 9 10 11]       "home_value%"
          [1 2 3 4 6 8 9 10 11]     "%home|_value%"
          [1 2 3 4 6 8 9 10 11]     "home|_value%"
          [1 2 3 4 5 6 8 9 10 11]   "home|_value"
          [1 2 3 4 5 7 8 9 10 11]   "home value"
          [1 2 3 4 5 8 9 10 11]     "home_value"
          [1 2 3 4 5 8 9 10 11]     "%value"
          [1 2 3 4 5 6 7 8 9 10 11] "book |%"
          [1 2 3 4 5 6 7 8 9 10 11] "home-value%"
          )
        (is (thrown-with-msg?
              Exception #"Can only escape"
              (d/q q db
                   "What?! this throws for ! can only escape %, _ and !")))
        (is (thrown-with-msg?
              Exception #"Can only escape" (d/q q-e db "book |N%" \|)))
        (is (thrown-with-msg?
              Exception #"Can only escape" (d/q q-e db "book |||N%" \|)))
        ))
    (d/close-db db)
    (u/delete-files dir)))

(deftest test-in-fn
  (let [dir (u/tmp-dir (str "fns-test-" (UUID/randomUUID)))
        db  (-> (d/empty-db dir {:key {:db/valueType :db.type/long}})
                (d/db-with [{:db/id 1, :key 1}
                            {:db/id 2, :key 2}
                            {:db/id 3, :key 3}
                            {:db/id 4, :key 4}
                            {:db/id 5, :key 5}
                            {:db/id 6, :key 6}
                            {:db/id 7, :key 7}
                            {:db/id 8, :key 8}
                            {:db/id 9, :key 9}]))]
    (is (= (set [2 5 7]) (set (d/q '[:find [?e ...]
                                     :in $ ?coll
                                     :where
                                     [?e :key ?v]
                                     [(in ?v ?coll)]]
                                   db [5 7 2]))))
    (is (= (set [3 4 5 9]) (set (d/q '[:find [?e ...]
                                       :in $ ?coll
                                       :where
                                       [?e :key ?v]
                                       [(not-in ?v ?coll)]]
                                     db [1 7 8 2 6]))))
    (d/close-db db)
    (u/delete-files dir)))

(deftest test-string-fn
  (let [dir (u/tmp-dir (str "fns-test-" (UUID/randomUUID)))
        db  (-> (d/empty-db dir {:text {:db/valueType :db.type/string}})
                (d/db-with
                  [{:db/id 1,
                    :text  "The quick red fox jumped over the lazy red dogs."}
                   {:db/id 2,
                    :text  "Mary had a little lamb whose fleece was red as fire."}
                   {:db/id 3,
                    :text  "Moby Dick is a story of a whale and a man obsessed?"}]))]
    (is (= (d/q '[:find ?e .
                  :in $ ?ext
                  :where
                  [?e :text ?v]
                  [(.endsWith ^String ?v ?ext)]]
                db "?") 3))
    (is (= (d/q '[:find ?e .
                  :in $ ?ext
                  :where
                  [?e :text ?v]
                  [(clojure.string/ends-with? ?v ?ext)]]
                db "?") 3))
    (is (thrown-with-msg? Exception #"Unknown"
                          (d/q '[:find ?e .
                                 :in $ ?ext
                                 :where
                                 [?e :text ?v]
                                 [(ends-with? ?v ?ext)]]
                               db "?") 3))
    (d/close-db db)
    (u/delete-files dir)))

(deftest test-fulltext-fns
  (let [dir (u/tmp-dir (str "fns-test-" (UUID/randomUUID)))
        db  (-> (d/empty-db dir
                            {:text {:db/valueType           :db.type/string
                                    :db/fulltext            true
                                    :db.fulltext/autoDomain true}}
                            {:search-domains
                             {"text" {:index-position? true}}})
                (d/db-with
                  [{:db/id 1,
                    :text  "The quick red fox jumped over the lazy red dogs."}
                   {:db/id 2,
                    :text  "Mary had a little lamb whose fleece was red as fire."}
                   {:db/id 3,
                    :text  "Moby Dick is a story of a whale and a man obsessed."}]))]
    (is (= (d/q '[:find ?e ?a ?v
                  :in $ ?q
                  :where [(fulltext $ ?q) [[?e ?a ?v]]]]
                db "red fox")
           #{[1 :text "The quick red fox jumped over the lazy red dogs."]
             [2 :text "Mary had a little lamb whose fleece was red as fire."]}))
    (is (= (d/q '[:find ?e ?a ?v
                  :in $ ?q
                  :where [(fulltext $ :text ?q) [[?e ?a ?v]]]]
                db "red fox")
           #{[1 :text "The quick red fox jumped over the lazy red dogs."]
             [2 :text "Mary had a little lamb whose fleece was red as fire."]}))
    (is (= (d/q '[:find ?e ?a ?v
                  :in $ ?q
                  :where [(fulltext $ :text ?q) [[?e ?a ?v]]]]
                db [:and {:phrase "little lamb"} "fleece"])
           #{[2 :text "Mary had a little lamb whose fleece was red as fire."]}))
    (let [scores (into {}
                       (d/q '[:find ?e ?score
                              :in $ ?q
                              :where [(fulltext $ ?q {:display :refs+scores})
                                      [[?e ?a ?v ?score]]]]
                            db "red fox"))]
      (is (= #{1 2} (set (keys scores))))
      (is (every? number? (vals scores)))
      (is (> (scores 1) (scores 2))))
    (d/close-db db)
    (u/delete-files dir)))

(deftest test-query-fns
  (testing "predicate without free variables"
    (is (= (d/q '[:find ?x
                  :in [?x ...]
                  :where [(> 2 1)]] [:a :b :c])
           #{[:a] [:b] [:c]})))

  (let [dir (u/tmp-dir (str "query-" (UUID/randomUUID)))
        db  (-> (d/empty-db dir {:parent {:db/valueType :db.type/ref}})
                (d/db-with
                  [{ :db/id 1, :name "Ivan", :age 15 }
                   { :db/id 2, :name "Petr", :age 22, :height 240, :parent 1}
                   { :db/id 3, :name "Slava", :age 37, :parent 2}]))]

    (testing "ground"
      (is (= (d/q '[:find ?vowel
                    :where [(ground [:a :e :i :o :u]) [?vowel ...]]])
             #{[:a] [:e] [:i] [:o] [:u]})))

    (testing "get-else"
      (is (= (d/q '[:find ?e ?age ?height
                    :where [?e :age ?age]
                    [(get-else $ ?e :height 300) ?height]] db)
             #{[1 15 300] [2 22 240] [3 37 300]}))

      (is (thrown-with-msg? ExceptionInfo #"get-else: nil default value is not supported"
                            (d/q '[:find ?e ?height
                                   :where [?e :age]
                                   [(get-else $ ?e :height nil) ?height]] db))))

    (testing "get-some"
      (is (= (d/q '[:find ?e ?a ?v
                    :where [?e :name _]
                    [(get-some $ ?e :height :age) [?a ?v]]] db)
             #{[1 :age 15]
               [2 :height 240]
               [3 :age 37]})))
    (testing "missing?"
      (is (= (d/q '[:find ?e ?age
                    :in $
                    :where [?e :age ?age]
                    [(missing? $ ?e :height)]] db)
             #{[1 15] [3 37]})))

    (testing "missing? back-ref"
      (is (= (d/q '[:find ?e
                    :in $
                    :where [?e :age ?age]
                    [(missing? $ ?e :_parent)]] db)
             #{[3]})))

    (testing "q built-in"
      (is (= (d/q '[:find ?e ?age
                    :where
                    [(q '[:find (min ?age)
                          :where [_ :age ?age]]
                        $) [[?age]]]
                    [?e :age ?age]]
                  db)
             #{[1 15]})))

    (testing "Built-ins"
      (is (= (d/q '[:find  ?e1 ?e2
                    :where [?e1 :age ?a1]
                    [?e2 :age ?a2]
                    [(< ?a1 18 ?a2)]] db)
             #{[1 2] [1 3]}))

      (is (= (d/q '[:find  ?x ?c
                    :in    [?x ...]
                    :where [(count ?x) ?c]]
                  ["a" "abc"])
             #{["a" 1] ["abc" 3]}))

      (is (= (d/q '[:find  ?a1
                    :where [_ :age ?a1]
                    [(< ?a1 22)]] db)
             #{[15]}))
      (is (= (d/q '[:find  ?a1
                    :where [_ :age ?a1]
                    [(<= ?a1 22)]] db)
             #{[15] [22]}))
      (is (= (d/q '[:find  ?a1
                    :where [_ :age ?a1]
                    [(> ?a1 22)]] db)
             #{[37]}))
      (is (= (d/q '[:find  ?a1
                    :where [_ :age ?a1]
                    [(>= ?a1 22)]] db)
             #{[22] [37]})))

    (testing "Built-in vector, hashmap"
      (is (= (d/q '[:find [?tx-data ...]
                    :where
                    [(ground :db/add) ?op]
                    [(vector ?op -1 :attr 12) ?tx-data]])
             [[:db/add -1 :attr 12]]))

      (is (= (d/q '[:find [?tx-data ...]
                    :where
                    [(hash-map :db/id -1 :age 92 :name "Aaron") ?tx-data]])
             [{:db/id -1 :age 92 :name "Aaron"}])))


    (testing "Passing predicate as source"
      (is (= (d/q '[:find  ?e
                    :in    $ ?adult
                    :where [?e :age ?a]
                    [(?adult ?a)]]
                  db
                  #(> ^long % 18))
             #{[2] [3]})))

    (testing "Calling a function"
      (is (= (d/q '[:find  ?e1 ?e2 ?e3
                    :where [?e1 :age ?a1]
                    [?e2 :age ?a2]
                    [?e3 :age ?a3]
                    [(+ ?a1 ?a2) ?a12]
                    [(= ?a12 ?a3)]]
                  db)
             #{[1 2 3] [2 1 3]})))

    (testing "Two conflicting function values for one binding."
      (is (= (d/q '[:find  ?n
                    :where [(identity 1) ?n]
                    [(identity 2) ?n]])
             #{})))

    (testing "Destructured conflicting function values for two bindings."
      (is (= (d/q '[:find  ?n ?x
                    :where [(identity [3 4]) [?n ?x]]
                    [(identity [1 2]) [?n ?x]]])
             #{})))

    (testing "Rule bindings interacting with function binding. (fn, rule)"
      (is (= (d/q '[:find  ?n
                    :in $ %
                    :where [(identity 2) ?n]
                    (my-vals ?n)]
                  db
                  '[[(my-vals ?x)
                     [(identity 1) ?x]]
                    [(my-vals ?x)
                     [(identity 2) ?x]]
                    [(my-vals ?x)
                     [(identity 3) ?x]]])
             #{[2]})))

    (testing "Rule bindings interacting with function binding. (rule, fn)"
      (is (= (d/q '[:find  ?n
                    :in $ %
                    :where (my-vals ?n)
                    [(identity 2) ?n]]
                  db
                  '[[(my-vals ?x)
                     [(identity 1) ?x]]
                    [(my-vals ?x)
                     [(identity 2) ?x]]
                    [(my-vals ?x)
                     [(identity 3) ?x]]])
             #{[2]})))

    (testing "Conflicting relational bindings with function binding. (rel, fn)"
      (is (= (d/q '[:find  ?age
                    :where [_ :age ?age]
                    [(identity 100) ?age]]
                  db)
             #{})))

    (testing "Conflicting relational bindings with function binding. (fn, rel)"
      (is (= (d/q '[:find  ?age
                    :where [(identity 100) ?age]
                    [_ :age ?age]]
                  db)
             #{})))

    (testing "Function on empty rel"
      (is (= (d/q '[:find  ?e ?y
                    :where [?e :salary ?x]
                    [(+ ?x 100) ?y]]
                  [[0 :age 15] [1 :age 35]])
             #{})))

    (testing "Returning nil from function filters out tuple from result"
      (is (= (d/q '[:find ?x
                    :in    [?in ...] ?f
                    :where [(?f ?in) ?x]]
                  [1 2 3 4]
                  #(when (even? %) %))
             #{[2] [4]})))

    (testing "Result bindings"
      (is (= (d/q '[:find ?a ?c
                    :in ?in
                    :where [(ground ?in) [?a _ ?c]]]
                  [:a :b :c])
             #{[:a :c]}))

      (is (= (d/q '[:find ?in
                    :in ?in
                    :where [(ground ?in) _]]
                  :a)
             #{[:a]}))

      (is (= (d/q '[:find ?x ?z
                    :in ?in
                    :where [(ground ?in) [[?x _ ?z]...]]]
                  [[:a :b :c] [:d :e :f]])
             #{[:a :c] [:d :f]}))

      (is (= (d/q '[:find ?in
                    :in [?in ...]
                    :where [(ground ?in) _]]
                  [])
             #{})))
    (d/close-db db)
    (u/delete-files dir)))

(deftest test-predicates
  (let [entities [{:db/id 1 :name "Ivan" :age 10}
                  {:db/id 2 :name "Ivan" :age 20}
                  {:db/id 3 :name "Oleg" :age 10}
                  {:db/id 4 :name "Oleg" :age 20}]
        dir      (u/tmp-dir (str "query-predicates-" (UUID/randomUUID)))
        db       (d/db-with (d/empty-db dir) entities)]
    (are [q res] (= (d/q (quote q) db) res)
      ;; plain predicate
      [:find  ?e ?a
       :where [?e :age ?a]
       [(> ?a 10)]]
      #{[2 20] [4 20]}

      ;; join in predicate
      [:find  ?e ?e2
       :where [?e  :name]
       [?e2 :name]
       [(< ?e ?e2)]]
      #{[1 2] [1 3] [1 4] [2 3] [2 4] [3 4]}

      ;; join with extra symbols
      [:find  ?e ?e2
       :where [?e  :age ?a]
       [?e2 :age ?a2]
       [(< ?e ?e2)]]
      #{[1 2] [1 3] [1 4] [2 3] [2 4] [3 4]}

      ;; empty result
      [:find  ?e ?e2
       :where [?e  :name "Ivan"]
       [?e2 :name "Oleg"]
       [(= ?e ?e2)]]
      #{}

      ;; pred over const, true
      [:find  ?e
       :where [?e :name "Ivan"]
       [?e :age 20]
       [(= ?e 2)]]
      #{[2]}

      ;; pred over const, false
      [:find  ?e
       :where [?e :name "Ivan"]
       [?e :age 20]
       [(= ?e 1)]]
      #{})

    (let [pred (fn [db e a]
                 (= a (:age (d/entity db e))))]
      (is (= (d/q '[:find ?e
                    :in $ ?pred
                    :where [?e :age ?a]
                    [(?pred $ ?e 10)]]
                  db pred)
             #{[1] [3]})))
    (d/close-db db)
    (u/delete-files dir)))


(deftest test-exceptions
  (is (thrown-with-msg? ExceptionInfo
                        #"Unknown"
                        (d/q '[:find ?e
                               :in   [?e ...]
                               :where [(fun ?e)]]
                             [1])))

  (is (thrown-with-msg? ExceptionInfo
                        #"Unknown"
                        (d/q '[:find ?e ?x
                               :in   [?e ...]
                               :where [(fun ?e) ?x]]
                             [1])))

  (is (thrown-with-msg? ExceptionInfo
                        #"Insufficient bindings"
                        (d/q '[:find ?x
                               :where [(zero? ?x)]])))

  (is (thrown-with-msg? ExceptionInfo
                        #"Insufficient bindings"
                        (d/q '[:find ?x
                               :where [(inc ?x) ?y]])))

  (is (thrown-with-msg? ExceptionInfo
                        #"Where uses unknown source vars:"
                        (d/q '[:find ?x
                               :where [?x] [(zero? $2 ?x)]])))

  (is (thrown-with-msg? ExceptionInfo
                        #"Where uses unknown source vars:"
                        (d/q '[:find  ?x
                               :in    $2
                               :where [$2 ?x] [(zero? $ ?x)]]))))

;; we probably don't want to support this
#_(deftest test-issue-180
    (let [dir (u/tmp-dir (str "query-or-" (UUID/randomUUID)))
          db  (d/empty-db dir)]
      (is (= #{}
             (d/q '[:find ?e ?a
                    :where [_ :pred ?pred]
                    [?e :age ?a]
                    [(?pred ?a)]]
                  (d/db-with db [[:db/add 1 :age 20]]))))
      (d/close-db db)
      (u/delete-files dir)))

(defn sample-query-fn [] 42)

(defn sum-and-double
  "Custom function for testing apply with fully qualified functions"
  [& nums]
  (* 2 (long (reduce + nums))))

(deftest test-symbol-resolution
  (is (= 42
         (d/q '[:find ?x .
                :where [(datalevin.test.query-fns/sample-query-fn) ?x]]))))

(deftest test-issue-445
  (let [dir (u/tmp-dir (str "query-fns-" (UUID/randomUUID)))
        db  (-> (d/empty-db dir {:name {:db/unique :db.unique/identity}})
                (d/db-with [{:db/id 1 :name "Ivan" :age 15}
                            {:db/id 2 :name "Petr" :age 22 :height 240}]))]
    (testing "get-else using lookup ref"
      (is (= "Unknown"
             (d/q '[:find ?height .
                    :in $ ?e
                    :where [(get-else $ ?e :height "Unknown") ?height]]
                  db
                  [:name "Ivan"]))))

    (testing "get-some using lookup ref"
      (is (= #{[[:name "Petr"] :age 22]}
             (d/q '[:find ?e ?a ?v
                    :in $ ?e
                    :where [(get-some $ ?e :weight :age :height) [?a ?v]]]
                  db
                  [:name "Petr"]))))
    (d/close-db db)
    (u/delete-files dir)))

(deftest issue-287-test
  (let [dir (u/tmp-dir (str "issue-287-" (UUID/randomUUID)))
        db  (-> (d/empty-db dir {:message/content
                                 {:db/valueType :db.type/string}
                                 :message/number
                                 {:db/valueType :db.type/long}})
                (d/db-with [
                            {:db/id           1
                             :message/number  7
                             :message/content "
两个黄鹂鸣翠柳， 一行白鹭上青天。
窗含西岭千秋雪， 门泊东吴万里船。"}
                            {:db/id           2
                             :message/number  5
                             :message/content "
红豆生南国，春来发几枝。
愿君多采撷，此物最相思。"}]))]
    (is (= 1
           (d/q '[:find ?e .
                  :where
                  [?e :message/content ?content]
                  [?e :message/number ?number]
                  [(< (count ?content) (* ?number ?number))]]
                db)))
    (is (= 1
           (d/q '[:find ?e .
                  :where
                  [?e :message/content ?content]
                  [?e :message/number ?number]
                  [(< 40 (* ?number ?number))]]
                db)))
    (is (= #{[1 49]}
           (d/q '[:find ?e ?n
                  :where
                  [?e :message/content ?content]
                  [?e :message/number ?number]
                  [(* ?number ?number) ?n]
                  [(< (count ?content) ?n)]]
                db)))
    (is (= 1
           (d/q '[:find ?e .
                  :where
                  [?e :message/content ?content]
                  [(< 10 (clojure.string/index-of ?content "门"))]]
                db)))
    (is (= 1
           (d/q '[:find ?e .
                  :where
                  [?e :message/content ?content]
                  [(> (count ?content) 30)]]
                db)))
    (is (= 1
           (d/q '[:find ?e .
                  :where
                  [?e :message/content ?content]
                  [(and (< 10 (clojure.string/index-of ?content "门"))
                        (> (count ?content) 30))]]
                db)))
    (is (= 1
           (d/q '[:find ?e .
                  :where
                  [?e :message/content ?content]
                  [(count ?content) ?n]
                  [(> ?n 30)]]
                db)))
    (is (= #{[1 36]}
           (d/q '[:find ?e ?n
                  :where
                  [?e :message/content ?content]
                  [(count ?content) ?n]
                  [(> ?n 30)]]
                db)))
    (d/close-db db)
    (u/delete-files dir)))

(deftest test-apply-fn
  (testing "apply with built-in functions"
    (is (= (d/q '[:find ?result .
                  :in ?nums
                  :where [(apply + ?nums) ?result]]
                [1 2 3])
           6))
    (is (= (d/q '[:find ?result .
                  :in ?nums
                  :where [(apply * ?nums) ?result]]
                [2 3 4])
           24))
    (is (= (d/q '[:find ?result .
                  :in ?items
                  :where [(apply str ?items) ?result]]
                ["a" "b" "c"])
           "abc"))
    (is (= (d/q '[:find ?result .
                  :in ?nums
                  :where [(apply max ?nums) ?result]]
                [3 7 2 9 1])
           9)))

  (testing "apply with fully qualified functions"
    (is (= (d/q '[:find ?result .
                  :in ?nums
                  :where [(apply clojure.core/+ ?nums) ?result]]
                [1 2 3])
           6))
    (is (= (d/q '[:find ?result .
                  :in ?nums
                  :where [(apply datalevin.test.query-fns/sum-and-double ?nums) ?result]]
                [1 2 3])
           12)))

  (testing "apply in query with data"
    (let [dir (u/tmp-dir (str "apply-test-" (UUID/randomUUID)))
          db  (-> (d/empty-db dir {:nums {:db/valueType   :db.type/tuple
                                          :db/tupleType   :db.type/long}})
                  (d/db-with [{:db/id 1 :nums [1 2 3]}
                              {:db/id 2 :nums [10 20 30]}]))]
      (is (= (d/q '[:find ?e ?sum
                    :where
                    [?e :nums ?nums]
                    [(apply + ?nums) ?sum]]
                  db)
             #{[1 6] [2 60]}))
      (d/close-db db)
      (u/delete-files dir))))
