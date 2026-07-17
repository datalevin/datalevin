(ns datalevin.test.query
  (:require
   [datalevin.test.core :as tdc :refer [db-fixture]]
   [clojure.test :refer [deftest testing is use-fixtures]]
   [datalevin.core :as d]
   [datalevin.constants :as c]
   [datalevin.pipe :as pipe]
   [datalevin.query :as q]
   [datalevin.query.cache :as qcache]
   [datalevin.query-optimizer :as qo]
   [datalevin.util :as u])
  (:import
   [clojure.lang ExceptionInfo]
   [java.util ArrayList Collection UUID]))

(use-fixtures :each db-fixture)

(deftest test-linear-recursive-keyed-seen
  (let [conn  (d/create-conn
                nil
                {:edge  {:db/valueType   :db.type/ref
                         :db/cardinality :db.cardinality/many}
                 :scope {:db/valueType :db.type/keyword}
                 :seed  {:db/valueType :db.type/long}}
                {:kv-opts {:inmemory? true}})
        rules '[[(reach ?scope ?a ?b)
                 [?a :scope ?scope]
                 [?a :seed ?b]]
                [(reach ?scope ?a ?b)
                 [?a :edge ?x]
                 (reach ?scope ?x ?b)]]]
    (try
      (d/transact! conn [{:db/id 1 :scope :left :edge 2}
                         {:db/id 2 :scope :left :edge 3 :seed 10}
                         {:db/id 3 :scope :left}
                         {:db/id 4 :scope :right :seed 20}])
      (is (= #{[:left 1 10] [:left 2 10] [:right 4 20]}
             (set (d/q '[:find ?scope ?a ?b
                         :in $ %
                         :where (reach ?scope ?a ?b)]
                       (d/db conn) rules))))
      (finally
        (d/close conn)))))

(deftest test-eav-scan-decodes-giant-value
  (let [dir     (u/tmp-dir (str "query-giant-eav-" (UUID/randomUUID)))
        conn    (d/get-conn dir {:lookup  {:db/valueType :db.type/string}
                                 :payload {:db/valueType :db.type/string}})
        payload (apply str (repeat (+ c/+val-bytes-wo-hdr+ 100) "x"))]
    (try
      (d/transact! conn [{:db/id 1 :lookup "target" :payload payload}])
      (is (= payload
             (d/q '[:find ?payload .
                    :in $ ?lookup
                    :where
                    [?e :lookup ?lookup]
                    [?e :payload ?payload]]
                  (d/db conn) "target")))
      (finally
        (d/close conn)
        (u/delete-files dir)))))

(deftest test-batched-tuple-pipe
  (testing "collection addAll retains normal copy semantics"
    (binding [c/query-pipe-batch-size 3
              c/query-pipe-capacity   20]
      (let [p      (pipe/counted-tuple-pipe)
            tuples (ArrayList. [1 2 3])]
        (.addAll ^Collection p tuples)
        (.clear tuples)
        (pipe/add-batch p [4 5 6 7])
        (pipe/finish p)
        (is (= [1 2 3 4 5 6 7]
               (loop [result []]
                 (if-let [tuple (pipe/produce p)]
                   (recur (conj result tuple))
                   result))))
        (is (= 7 (pipe/total p))))))

  (testing "tuple capacity still provides backpressure across batches"
    (let [p        (binding [c/query-pipe-batch-size 3
                             c/query-pipe-capacity   4]
                     (pipe/tuple-pipe))
          producer (future
                     (doseq [tuple (range 10)]
                       (.add ^Collection p tuple))
                     (pipe/finish p)
                     :done)]
      (is (= (vec (range 10))
             (loop [result []]
               (if-let [tuple (pipe/produce p)]
                 (recur (conj result tuple))
                 result))))
      (is (= :done (deref producer 5000 :timeout))))))

(deftest test-query-cache-vars-remain-compatible
  (is (var? (ns-resolve 'datalevin.query '*cache?*)))
  (is (var? (ns-resolve 'datalevin.query '*query-cache*)))
  (is (var? (ns-resolve 'datalevin.query '*plan-cache*)))
  (is (identical? q/*query-cache* qcache/*query-cache*))
  (is (identical? q/*plan-cache* qo/*plan-cache*))
  (binding [q/*cache?* false]
    (is (false? q/*cache?*))))

(deftest test-query-cache-stores-exact-result-window
  (let [dir (u/tmp-dir (str "query-window-cache-" (UUID/randomUUID)))
        conn (d/get-conn dir)
        q1  '[:find ?score
              :where [?e :score ?score]
              :order-by ?score
              :limit 2]
        q2  '[:find ?score
              :where [?e :score ?score]
              :order-by ?score
              :offset 1
              :limit 2]]
    (try
      (d/transact! conn [{:db/id 1 :score 30}
                         {:db/id 2 :score 10}
                         {:db/id 3 :score 20}
                         {:db/id 4 :score 40}])
      (let [r1 (qcache/q-result (qcache/parsed-q q1) [(d/db conn)])
            r2 (qcache/q-result (qcache/parsed-q q2) [(d/db conn)])]
        (is (= [[10] [20]] r1))
        (is (= [[20] [30]] r2))
        (is (vector? r1))
        (is (vector? r2)))
      (d/transact! conn [{:db/id 5 :score 5}])
      (is (= [[5] [10]]
             (qcache/q-result (qcache/parsed-q q1) [(d/db conn)])))
      (finally
        (d/close conn)
        (u/delete-files dir)))))

(deftest test-ordered-limit-stops-after-candidate-window
  (let [dir   (u/tmp-dir (str "query-top-k-pushdown-" (UUID/randomUUID)))
        conn  (d/get-conn dir)
        calls (atom 0)
        pred  (fn [_]
                (swap! calls inc)
                true)
        query '[:find ?score
                :in $ ?accept? ?max-score
                :where
                [?e :score ?score]
                [(?accept? ?e)]
                [(<= ?score ?max-score)]
                :order-by [?score :desc]
                :limit 3]]
    (try
      (d/transact! conn (mapv (fn [e] {:db/id e :score e}) (range 1 2501)))
      (binding [q/*cache?* false]
        (is (= [[2500] [2499] [2498]]
               (d/q query (d/db conn) pred 2500))))
      (is (<= @calls 1024))
      (finally
        (d/close conn)
        (u/delete-files dir)))))

(deftest test-ordered-limit-completes-primary-key-tie
  (let [dir   (u/tmp-dir (str "query-top-k-tie-" (UUID/randomUUID)))
        conn  (d/get-conn dir)
        query '[:find ?score ?id
                :in $ ?max-score
                :where
                [?e :score ?score]
                [?e :id ?id]
                [(<= ?score ?max-score)]
                :order-by [?score :desc ?id :asc]
                :limit 3]]
    (try
      (d/transact! conn
                   (mapv (fn [e] {:db/id e :score 10 :id e})
                         (range 1 1101)))
      (binding [q/*cache?* false]
        (is (= [[10 1] [10 2] [10 3]]
               (d/q query (d/db conn) 10))))
      (finally
        (d/close conn)
        (u/delete-files dir)))))

(deftest test-ordered-limit-pages-independently-of-entity-order
  (let [dir        (u/tmp-dir (str "query-top-k-page-" (UUID/randomUUID)))
        conn       (d/get-conn dir)
        desc-query '[:find ?score
                     :in $ ?max-score
                     :where
                     [?e :score ?score]
                     [?e :keep-desc true]
                     [(<= ?score ?max-score)]
                     :order-by [?score :desc]
                     :limit 1]
        asc-query  '[:find ?score
                     :in $ ?min-score
                     :where
                     [?e :score ?score]
                     [?e :keep-asc true]
                     [(>= ?score ?min-score)]
                     :order-by [?score :asc]
                     :limit 1]]
    (try
      (d/transact! conn
                   (mapv (fn [^long score]
                           (cond-> {:db/id (- 3000 score) :score score}
                             (= score 1000) (assoc :keep-desc true)
                             (= score 1500) (assoc :keep-asc true)))
                         (range 1 2501)))
      (binding [q/*cache?* false]
        (is (= [[1000]] (d/q desc-query (d/db conn) 2500)))
        (is (= [[1500]] (d/q asc-query (d/db conn) 1))))
      (finally
        (d/close conn)
        (u/delete-files dir)))))

(deftest test-ordered-limit-includes-pending-transaction-datoms
  (let [dir   (u/tmp-dir (str "query-top-k-pending-" (UUID/randomUUID)))
        conn  (d/get-conn dir)
        query '[:find ?score
                :in $ ?max-score
                :where
                [?e :score ?score]
                [(<= ?score ?max-score)]
                :order-by [?score :desc]
                :limit 1]]
    (try
      (d/transact! conn [{:db/id 1 :score 1}])
      (let [db (d/db-with (d/db conn) [{:db/id 2 :score 2}])]
        (binding [q/*cache?* false]
          (is (= [[2]] (d/q query db 2)))))
      (finally
        (d/close conn)
        (u/delete-files dir)))))

(deftest test-instant
  (let [dir (u/tmp-dir (str "test-instant-" (UUID/randomUUID)))
        db  (-> (d/empty-db dir
                            {:person/born {:db/valueType :db.type/instant}})
                (d/db-with [{:person/born #inst "1969-01-01"}
                            {:person/born #inst "1971-01-01"}]))]
    (is (= 2 (count (d/datoms db :eav))))
    (is (= 2 (count
               (d/q '[:find [?born ...]
                      :where [?e :person/born ?born]] db))))
    (d/close-db db)
    (u/delete-files dir)))

(deftest test-query-false-value
  (let [dir (u/tmp-dir (str "test-query-false-" (UUID/randomUUID)))
        db  (-> (d/empty-db dir {:flag {:db/valueType :db.type/boolean}})
                (d/db-with [{:db/id 1 :flag false}
                            {:db/id 2 :flag true}]))]
    (is (= #{[1]}
           (d/q '[:find ?e
                  :where [?e :flag false]]
                db)))
    (is (= #{[1 false] [2 true]}
           (d/q '[:find ?e ?flag
                  :where [?e :flag ?flag]]
                db)))
    (d/close-db db)
    (u/delete-files dir)))

(deftest test-bound-ref-keeps-all-value-predicates
  (let [dir (u/tmp-dir (str "test-bound-ref-value-predicates-"
                            (UUID/randomUUID)))
        db  (-> (d/empty-db
                  dir
                  {:item/owner {:db/valueType :db.type/ref}
                   :item/text  {:db/valueType :db.type/string}})
                (d/db-with [{:db/id 1
                             :item/owner 100
                             :item/text "USA:25 February 2013"}
                            {:db/id 2
                             :item/owner 100
                             :item/text "USA:1 June 2007"}
                            {:db/id 3
                             :item/owner 100
                             :item/text "Turkey:24 September 2009"}]))
        combined
        '[:find ?text
          :in $ ?owner
          :where
          [?item :item/owner ?owner]
          [?item :item/text ?text]
          [(and (like ?text "USA:%") (like ?text "% 200%"))]]
        separate
        '[:find ?text
          :in $ ?owner
          :where
          [?item :item/owner ?owner]
          [?item :item/text ?text]
          [(like ?text "USA:%")]
          [(like ?text "% 200%")]]]
    (is (= #{["USA:1 June 2007"]} (d/q combined db 100)))
    (is (= #{["USA:1 June 2007"]} (d/q separate db 100)))
    (d/close-db db)
    (u/delete-files dir)))

(deftest test-many-joins
  (let [data (->> (range 1000)
                  (map (fn [^long i]
                         {:db/id i
                          :a     (str (UUID/randomUUID))
                          :b     (str (UUID/randomUUID))
                          :c     (str (UUID/randomUUID))
                          :d     (str (UUID/randomUUID))
                          :e     (rand-int 3)
                          :f     (rand-int 3)
                          :g     (rand-int 3)
                          :h     (rand-int 3)})))
        dir  (u/tmp-dir (str "test-many-" (UUID/randomUUID)))
        db   (-> (d/empty-db dir {:a {:db/valueType :db.type/string}
                                  :b {:db/valueType :db.type/string}
                                  :c {:db/valueType :db.type/string}
                                  :d {:db/valueType :db.type/string}
                                  :e {:db/valueType :db.type/long}
                                  :f {:db/valueType :db.type/long}
                                  :g {:db/valueType :db.type/long}
                                  :h {:db/valueType :db.type/long}})
                 (d/db-with data))]
    (is (number? (d/q '[:find ?eid1 .
                        :where
                        [?eid1 :a ?a1]
                        [?eid1 :b ?b1]
                        [?eid1 :c ?c1]
                        [?eid1 :d ?d1]
                        [?eid1 :e ?e1]
                        [?eid1 :f ?f1]
                        [?eid1 :g ?g1]
                        [?eid1 :h ?h1]
                        [?eid2 :e ?e1]]
                      db)))
    (d/close-db db)
    (u/delete-files dir)))

(deftest test-joins
  (let [dir (u/tmp-dir (str "test-instant-" (UUID/randomUUID)))
        db  (-> (d/empty-db dir)
                (d/db-with [ { :db/id 1, :name "Ivan", :age 15 }
                            { :db/id 2, :name "Petr", :age 37 }
                            { :db/id 3, :name "Ivan", :age 37 }
                            { :db/id 4, :age 15 }]))]

    (is (= (d/q '[:find ?e
                  :where [?e :name]] db)
           #{[1] [2] [3]}))
    (is (= (d/q '[:find  ?e ?v
                  :where [?e :name "Ivan"]
                  [?e :age ?v]] db)
           #{[1 15] [3 37]}))
    (is (= (d/q '[:find  ?e1 ?e2
                  :where [?e1 :name ?n]
                  [?e2 :name ?n]] db)
           #{[1 1] [2 2] [3 3] [1 3] [3 1]}))
    (is (= (d/q '[:find  ?e ?e2 ?n
                  :where [?e :name "Ivan"]
                  [?e :age ?a]
                  [?e2 :age ?a]
                  [?e2 :name ?n]] db)
           #{[1 1 "Ivan"]
             [3 3 "Ivan"]
             [3 2 "Petr"]}))
    (d/close-db db)
    (u/delete-files dir)))

(deftest test-indexed-semi-join-for-dead-leaf
  (let [conn      (d/create-conn
                    nil
                    {:credit/title {:db/valueType :db.type/ref}}
                    {:kv-opts {:inmemory? true}})
        dead-q    '[:find ?title
                    :where
                    [?t :title/name ?title]
                    [?credit :credit/title ?t]
                    [?credit :credit/role :actor]]
        projected-q
        '[:find ?title ?credit
          :where
          [?t :title/name ?title]
          [?credit :credit/title ?t]
          [?credit :credit/role :actor]]
        value-q   '[:find ?name
                    :where
                    [?left :left/name ?name]
                    [?left :left/key ?key]
                    [?right :right/key ?key]
                    [?right :right/role :actor]]
        plan-steps
        (fn [q db]
          (->> (d/explain {} q db)
               :plan
               vals
               (mapcat identity)
               (mapcat identity)
               (mapcat :steps)))]
    (try
      (d/transact!
        conn
        (into [{:db/id 1 :title/name "Wanted"}
               {:db/id 2 :title/name "No credits"}
               {:db/id 3 :left/name "Value match" :left/key :match}
               {:db/id 4 :left/name "No value match" :left/key :other}]
              (concat
                (map (fn [^long i]
                       {:db/id        (+ 100 i)
                        :credit/title 1
                        :credit/role  :actor})
                     (range 50))
                (map (fn [^long i]
                       {:db/id      (+ 200 i)
                        :right/key  :match
                        :right/role :actor})
                     (range 50)))))
      (let [db (d/db conn)]
        ;; Plan the dead-leaf form first to exercise the projection-sensitive
        ;; plan-cache key before planning the otherwise identical query graph.
        (is (some #(= "Semi-join by indexed link scan." %)
                  (plan-steps dead-q db)))
        (is (= #{["Wanted"]} (d/q dead-q db)))
        (is (not-any? #(= "Semi-join by indexed link scan." %)
                      (plan-steps projected-q db)))
        (is (= 50 (count (d/q projected-q db))))
        (is (some #(= "Semi-join by indexed link scan." %)
                  (plan-steps value-q db)))
        (is (= #{["Value match"]} (d/q value-q db))))
      (finally
        (d/close conn)))))

(deftest test-semi-join-keeps-cross-source-vars
  (let [left  (d/create-conn
                nil
                {:leaf/root {:db/valueType :db.type/ref}}
                {:kv-opts {:inmemory? true}})
        right (d/create-conn nil {} {:kv-opts {:inmemory? true}})
        query '[:find ?name
                :in $left $right
                :where
                [$left ?root :root/name ?name]
                [$left ?leaf :leaf/root ?root]
                [$left ?leaf :leaf/key ?key]
                [$right ?other :other/key ?key]]]
    (try
      (d/transact! left [{:db/id 1 :root/name "Wanted"}
                         {:db/id 2 :root/name "No match"}
                         {:db/id 10 :leaf/root 1 :leaf/key :match}
                         {:db/id 11 :leaf/root 2 :leaf/key :miss}])
      (d/transact! right [{:db/id 20 :other/key :match}])
      (let [left-db  (d/db left)
            right-db (d/db right)
            steps    (->> (d/explain {} query left-db right-db)
                          :plan
                          vals
                          (mapcat identity)
                          (mapcat identity)
                          (mapcat :steps))]
        (is (not-any? #(= "Semi-join by indexed link scan." %) steps))
        (is (= #{["Wanted"]} (d/q query left-db right-db))))
      (finally
        (d/close left)
        (d/close right)))))

(deftest test-duplicate-attr-merge-scan
  (let [dir (u/tmp-dir (str "test-duplicate-attr-merge-scan-"
                            (UUID/randomUUID)))
        db  (-> (d/empty-db dir)
                (d/db-with [{:db/id 1 :name "Ann" :email "a@x"}
                            {:db/id 2 :name "Bob"}]))]
    (is (= #{[1]}
           (d/q '[:find ?e
                  :where
                  [?e :name ?x]
                  [?e :email ?y]]
                db)))
    (is (= #{[1] [2]}
           (d/q '[:find ?e
                  :where
                  [?e :name ?x]
                  [?e :name ?z]]
                db)))
    (doseq [query ['[:find ?e
                     :where
                     [?e :name ?x]
                     [?e :email ?y]
                     [?e :name ?z]]
                   '[:find ?e
                     :where
                     [?e :name ?x]
                     [?e :name ?z]
                     [?e :email ?y]]
                   '[:find ?e
                     :where
                     [?e :email ?y]
                     [?e :name ?x]
                     [?e :name ?z]]]]
      (is (= #{[1]} (d/q query db))))
    (d/close-db db)
    (u/delete-files dir)))


(deftest test-q-many
  (let [dir (u/tmp-dir (str "test-query-" (UUID/randomUUID)))
        db  (-> (d/empty-db dir {:aka {:db/cardinality :db.cardinality/many}})
                (d/db-with [ [:db/add 1 :name "Ivan"]
                            [:db/add 1 :aka  "ivolga"]
                            [:db/add 1 :aka  "pi"]
                            [:db/add 2 :name "Petr"]
                            [:db/add 2 :aka  "porosenok"]
                            [:db/add 2 :aka  "pi"] ]))]
    (is (= (d/q '[:find  ?n1 ?n2
                  :where [?e1 :aka ?x]
                  [?e2 :aka ?x]
                  [?e1 :name ?n1]
                  [?e2 :name ?n2]] db)
           #{["Ivan" "Ivan"]
             ["Petr" "Petr"]
             ["Ivan" "Petr"]
             ["Petr" "Ivan"]}))
    (d/close-db db)
    (u/delete-files dir)))


(deftest test-q-coll
  (let [db [[1 :name "Ivan"]
            [1 :age  19]
            [1 :aka  "dragon_killer_94"]
            [1 :aka  "-=autobot=-"] ] ]
    (is (= (d/q '[ :find  ?n ?a
                  :where [?e :aka "dragon_killer_94"]
                  [?e :name ?n]
                  [?e :age  ?a]] db)
           #{["Ivan" 19]})))

  (testing "Query over long tuples"
    (let [db [ [1 :name "Ivan" 945 :db/add]
              [1 :age  39     999 :db/retract]] ]
      (is (= (d/q '[ :find  ?e ?v
                    :where [?e :name ?v]] db)
             #{[1 "Ivan"]}))
      (is (= (d/q '[ :find  ?e ?a ?v ?t
                    :where [?e ?a ?v ?t :db/retract]] db)
             #{[1 :age 39 999]})))))


(deftest test-q-in
  (let [dir   (u/tmp-dir (str "test-q-in-" (UUID/randomUUID)))
        db    (-> (d/empty-db dir)
                  (d/db-with [ { :db/id 1, :name "Ivan", :age 15 }
                              { :db/id 2, :name "Petr", :age 37 }
                              { :db/id 3, :name "Ivan", :age 37 }]))
        query '{:find  [?e]
                :in    [$ ?attr ?value]
                :where [[?e ?attr ?value]]}]
    (is (= (d/q query db :name "Ivan")
           #{[1] [3]}))
    (is (= (d/q query db :age 37)
           #{[2] [3]}))

    (testing "Named DB"
      (is (= (d/q '[:find  ?a ?v
                    :in    $db ?e
                    :where [$db ?e ?a ?v]] db 1)
             #{[:name "Ivan"]
               [:age 15]})))

    (testing "DB join with collection"
      (is (= (d/q '[:find  ?e ?email
                    :in    $ $b
                    :where [?e :name ?n]
                    [$b ?n ?email]]
                  db
                  [["Ivan" "ivan@mail.ru"]
                   ["Petr" "petr@gmail.com"]])
             #{[1 "ivan@mail.ru"]
               [2 "petr@gmail.com"]
               [3 "ivan@mail.ru"]})))

    (testing "Query without DB"
      (is (= (d/q '[:find ?a ?b
                    :in   ?a ?b]
                  10 20)
             #{[10 20]})))
    (d/close-db db)
    (u/delete-files dir)))

(deftest test-collection-binding-value-lookups
  (let [dir (u/tmp-dir (str "test-q-in-coll-lookup-" (UUID/randomUUID)))
        db  (-> (d/empty-db dir)
                (d/db-with [{:db/id 10 :country/name "Canada"}
                            {:db/id 11 :country/name "Japan"}
                            {:db/id 12 :country/name "USA"}
                            {:db/id 20
                             :artist/name "A"
                             :artist/country 10}
                            {:db/id 21
                             :artist/name "B"
                             :artist/country 11}
                            {:db/id 22
                             :artist/name "C"
                             :artist/country 12}]))
        issue-q '[:find [(pull ?a [:artist/name]) ...]
                  :in $ [?c ...]
                  :where
                  [?a :artist/country ?country]
                  [?country :country/name ?c]]
        chained-q '[:find [?name ...]
                    :in $ [?c ...]
                    :where
                    [?a :artist/country ?country]
                    [?country :country/name ?c]
                    [?a :artist/name ?name]]
        countries ["Canada" "Japan"]]
    (try
      (is (= #{{:artist/name "A"} {:artist/name "B"}}
             (set (d/q issue-q db countries))))
      (is (empty? (:late-clauses (d/explain {} issue-q db countries))))

      (is (= #{"A" "B"} (set (d/q chained-q db countries))))
      (is (empty? (:late-clauses (d/explain {} chained-q db countries))))
      (finally
        (d/close-db db)
        (u/delete-files dir)))))

(deftest test-late-clauses-sort-by-dependencies
  (let [dir (u/tmp-dir (str "test-q-late-sort-" (UUID/randomUUID)))
        db  (-> (d/empty-db dir)
                (d/db-with [{:db/id 1
                             :name  "Ivan"
                             :age   15
                             :flag  :active}
                            {:db/id 2
                             :name  "Bob"
                             :age   37
                             :flag  :inactive}
                            {:db/id 3
                             :name  "Oleg"
                             :age   21}]))
        q   '[:find [?v ...]
              :with ?e
              :where
              [(= ?a :name)]
              [(string? ?v)]
              [?e ?a ?v]]
        q2  '[:find [?n ...]
              :with ?e ?a ?age ?s
              :where
              [?e :name ?n]
              [(= ?s "Ivan!")]
              [(str ?n "!") ?s]
              [(= ?a :age)]
              [?e ?a ?age]]
        q3  '[:find [?label ...]
              :with ?e ?a ?age ?base
              :where
              [(= ?label "Ivan-15!")]
              [(str ?base "!") ?label]
              [(str ?n "-" ?age) ?base]
              [(= ?a :age)]
              [?e ?a ?age]
              [?e :name ?n]]
        q4  '[:find [?n ...]
              :with ?e ?a
              :where
              (not [?e :flag :inactive])
              [(= ?a :name)]
              [?e ?a ?n]]
        q5  '[:find [?n ...]
              :with ?e ?a
              :where
              (not-join [?e]
                (not [?e :age 37]))
              [(= ?a :name)]
              [?e ?a ?n]]]
    (try
      (is (= #{"Ivan" "Bob" "Oleg"} (set (d/q q db))))
      (is (= '[[?e ?a ?v] [(= ?a :name)] [(string? ?v)]]
             (:late-clauses (d/explain {} q db))))

      (is (= #{"Ivan"} (set (d/q q2 db))))
      (is (= '[[(str ?n "!") ?s] [(= ?s "Ivan!")]
               [?e ?a ?age] [(= ?a :age)]]
             (:late-clauses (d/explain {} q2 db))))

      (is (= #{"Ivan-15!"} (set (d/q q3 db))))
      (is (= '[[?e ?a ?age]
               [(str ?n "-" ?age) ?base]
               [(str ?base "!") ?label]
               [(= ?label "Ivan-15!")]
               [(= ?a :age)]]
             (:late-clauses (d/explain {} q3 db))))

      (is (= #{"Ivan" "Oleg"} (set (d/q q4 db))))
      (is (= '[[?e ?a ?n]
               (not [?e :flag :inactive])
               [(= ?a :name)]]
             (:late-clauses (d/explain {} q4 db))))

      (is (= #{"Bob"} (set (d/q q5 db))))
      (is (= '[[?e ?a ?n]
               (not-join [?e] (not [?e :age 37]))
               [(= ?a :name)]]
             (:late-clauses (d/explain {} q5 db))))
      (finally
        (d/close-db db)
        (u/delete-files dir)))))

(deftest test-bindings
  (let [dir (u/tmp-dir (str "test-instant-" (UUID/randomUUID)))
        db  (-> (d/empty-db dir)
                (d/db-with [ { :db/id 1, :name "Ivan", :age 15 }
                            { :db/id 2, :name "Petr", :age 37 }
                            { :db/id 3, :name "Ivan", :age 37 }]))]
    (testing "Relation binding"
      (is (= (d/q '[:find  ?e ?email
                    :in    $ [[?n ?email]]
                    :where [?e :name ?n]]
                  db
                  [["Ivan" "ivan@mail.ru"]
                   ["Petr" "petr@gmail.com"]])
             #{[1 "ivan@mail.ru"]
               [2 "petr@gmail.com"]
               [3 "ivan@mail.ru"]})))

    (testing "Tuple binding"
      (is (= (d/q '[:find  ?e
                    :in    $ [?name ?age]
                    :where [?e :name ?name]
                    [?e :age ?age]]
                  db ["Ivan" 37])
             #{[3]})))

    (testing "Collection binding"
      (is (= (d/q '[:find  ?attr ?value
                    :in    $ ?e [?attr ...]
                    :where [?e ?attr ?value]]
                  db 1 [:name :age])
             #{[:name "Ivan"] [:age 15]})))

    (testing "Empty coll handling"
      (is (= (d/q '[:find ?id
                    :in $ [?id ...]
                    :where [?id :age _]]
                  [[1 :name "Ivan"]
                   [2 :name "Petr"]]
                  [])
             #{}))
      (is (= (d/q '[:find ?id
                    :in $ [[?id]]
                    :where [?id :age _]]
                  [[1 :name "Ivan"]
                   [2 :name "Petr"]]
                  [])
             #{})))

    (testing "Placeholders"
      (is (= (d/q '[:find ?x ?z
                    :in [?x _ ?z]]
                  [:x :y :z])
             #{[:x :z]}))
      (is (= (d/q '[:find ?x ?z
                    :in [[?x _ ?z]]]
                  [[:x :y :z] [:a :b :c]])
             #{[:x :z] [:a :c]})))

    (testing "Error reporting"
      (is (thrown-with-msg? ExceptionInfo #"Cannot bind value :a to tuple \[\?a \?b\]"
                            (d/q '[:find ?a ?b :in [?a ?b]] :a)))
      (is (thrown-with-msg? ExceptionInfo #"Cannot bind value :a to collection \[\?a \.\.\.\]"
                            (d/q '[:find ?a :in [?a ...]] :a)))
      (is (thrown-with-msg? ExceptionInfo #"Not enough elements in a collection \[:a\] to bind tuple \[\?a \?b\]"
                            (d/q '[:find ?a ?b :in [?a ?b]] [:a]))))

    (d/close-db db)
    (u/delete-files dir)))

(deftest test-nested-bindings
  (is (= (d/q '[:find  ?k ?v
                :in    [[?k ?v] ...]
                :where [(> ?v 1)]]
              {:a 1, :b 2, :c 3})
         #{[:b 2] [:c 3]}))

  (is (= (d/q '[:find  ?k ?min ?max
                :in    [[?k ?v] ...] ?minmax
                :where [(?minmax ?v) [?min ?max]]
                [(> ?max ?min)]]
              {:a [1 2 3 4]
               :b [5 6 7]
               :c [3]}
              (fn [v] [(reduce min v) (reduce max v)]))
         #{[:a 1 4] [:b 5 7]}))

  (is (= (d/q '[:find  ?k ?x
                :in    [[?k [?min ?max]] ...] ?range
                :where [(?range ?min ?max) [?x ...]]
                [(even? ?x)]]
              {:a [1 7]
               :b [2 4]}
              range)
         #{[:a 2] [:a 4] [:a 6]
           [:b 2]})))

(deftest test-built-in-regex
  (is (= (d/q '[:find  ?name
                :in    [?name ...] ?key
                :where [(re-pattern ?key) ?pattern]
                [(re-find ?pattern ?name)]]
              #{"abc" "abcX" "aXb"}
              "X")
         #{["abcX"] ["aXb"]})))

(deftest test-some-strings
  (let [dir  (u/tmp-dir (str "test-instant-" (UUID/randomUUID)))
        conn (d/create-conn dir {:id   {:db/valueType :db.type/long}
                                 :text {:db/valueType :db.type/string}})]
    (d/transact! conn [{:text "[7/3, 15:36]"
                        :id   3}])
    (is (= '([{:db/id 1, :id 3, :text "[7/3, 15:36]"}])
           (d/q '[:find (pull ?e [*])
                  :where
                  [?e :id 3]]
                @conn)))
    (d/close conn)
    (u/delete-files dir)))

(deftest test-issue-359-vector-value-passed-to-udf
  (let [dir  (u/tmp-dir (str "test-issue-359-" (UUID/randomUUID)))
        conn (d/get-conn dir)
        seen (atom ::unset)
        pred (fn [coord]
               (reset! seen coord)
               (= coord [1 2 4]))]
    (try
      (d/transact! conn [{:db/id -1 :at/coord [1 2 4]}])
      (is (= [{:db/id 1 :at/coord [1 2 4]}]
             (d/q '[:find [(pull ?e [*]) ...]
                    :in $ ?coord-in-range
                    :where
                    [?e :at/coord ?coord]
                    [(?coord-in-range ?coord)]]
                  @conn
                  pred)))
      (is (= [1 2 4] @seen))
      (finally
        (d/close conn)
        (u/delete-files dir)))))

(deftest test-built-in-get
  (is (= (d/q '[:find ?m ?m-value
                :in [[?k ?m] ...] ?m-key
                :where [(get ?m ?m-key) ?m-value]]
              {:a {:b 1}
               :c {:d 2}}
              :d)
         #{[{:d 2} 2]})))

(deftest test-join-unrelated
  (let [dir (u/tmp-dir (str "test-query-" (UUID/randomUUID)))
        db  (d/empty-db dir)]
    (is (= #{}
           (d/q '[:find ?name
                  :in $ ?my-fn
                  :where [?e :person/name ?name]
                  [(?my-fn) ?result]
                  [(< ?result 3)]]
                (d/db-with db [{:person/name "Joe"}])
                (fn [] 5))))
    (d/close-db db)
    (u/delete-files dir)))

(deftest test-symbol-comparison
  (is (= [2]
         (d/q
           '[:find [?e ...]
             :where [?e :s b]]
           '[[1 :s a]
             [2 :s b]])))
  (let [db (-> (d/empty-db)
               (d/db-with '[{:db/id 1, :s a}
                            {:db/id 2, :s b}]))]
    (is (= [2]
           (d/q
             '[:find [?e ...]
               :where [?e :s b]]
             db)))))
