(ns datalevin.rules-test
  (:require
   [clojure.test :refer [deftest are is use-fixtures]]
   [datalevin.test.core :as tdc :refer [db-fixture]]
   [datalevin.rules :as sut]
   [datalevin.core :as d]
   [datalevin.constants :as c]
   [datalevin.util :as u])
  (:import
   [java.util UUID]))

(use-fixtures :each db-fixture)

(deftest strongly-connected-components-test
  (are [graph sccs] (= (set (sut/tarjans-scc graph)) (set sccs))
    {:node        #{:unreachable}
     :reachable   #{:unreachable :reachable}
     :unreachable #{}}
    [#{:reachable} #{:node} #{:unreachable}]

    {1 [2]
     2 [3 4]
     3 [4 6]
     4 [1 5]
     5 [6]
     6 [7]
     7 [5]}
    [#{1 2 3 4} #{5 6 7}]

    {:a [:c :h]
     :b [:a :g]
     :c [:d]
     :d [:f]
     :e [:a :i]
     :f [:j]
     :g [:i]
     :h [:f :g]
     :i [:h]
     :j [:c]}
    [#{:e} #{:b} #{:a} #{:h :i :g} #{:c :j :f :d}]))

(deftest reachability-data-test
  (let [dir (u/tmp-dir (str "reachability-test" (UUID/randomUUID)))
        db  (-> (d/empty-db
                  dir
                  {:node {:db/unique    :db.unique/identity
                          :db/valueType :db.type/symbol}
                   :link {:db/cardinality :db.cardinality/many
                          :db/valueType   :db.type/ref}}
                  {:kv-opts {:flags (conj c/default-env-flags :nosync :nolock)}})
                (d/db-with [{:db/id -1 :node 'a :link -2}
                            {:db/id -2 :node 'b :link -3}
                            {:db/id -3 :node 'c :link [-3 -4]}
                            {:db/id -4 :node 'd}]))]
    (is (= #{['a 'b]
             ['b 'c]
             ['c 'c]
             ['c 'd]
             ['a 'c]
             ['b 'd]
             ['a 'd]}
           (d/q '[:find ?n ?m
                  :in $ %
                  :where
                  (reachable ?X ?Y)
                  [?X :node ?n]
                  [?Y :node ?m]]
                db '[[(reachable ?X ?Y)
                      [?X :link ?Y]]
                     [(reachable ?X ?Y)
                      [?X :link ?Z]
                      (reachable ?Z ?Y)]])
           (d/q '[:find ?n ?m
                  :in $ %
                  :where
                  (reachable ?n ?m)]
                db '[[(reachable ?n ?m)
                      [?X :link ?Y]
                      [?X :node ?n]
                      [?Y :node ?m]
                      ]
                     [(reachable ?n ?m)
                      [?X :node ?n]
                      [?Y :node ?m]
                      [?Z :node ?o]
                      [?X :link ?Z]
                      (reachable ?o ?m)]])))
    (d/close-db db)
    (u/delete-files dir)))

(deftest math-genealogy-test
  (let [dir (u/tmp-dir (str "math-genealogy-test-" (UUID/randomUUID)))
        db  (-> (d/empty-db
                  dir
                  {:dissertation/cid  {:db/valueType :db.type/ref}
                   :dissertation/univ {:db/valueType :db.type/string}
                   :dissertation/area {:db/valueType :db.type/string}
                   :person/name       {:db/valueType :db.type/string}
                   :person/advised    {:db/valueType   :db.type/ref
                                       :db/cardinality :db.cardinality/many}}
                  {:kv-opts {:flags (conj c/default-env-flags :nosync :nolock)}})
                (d/db-with
                  [{:db/id       -1
                    :person/name "David Scott Warren"}
                   {:db/id             -2
                    :dissertation/cid  -1
                    :dissertation/univ "University of Michigan"
                    :dissertation/area "68—Computer science"}
                   {:db/id          -3
                    :person/name    "Joyce Barbara Friedman"
                    :person/advised [-2]}
                   {:db/id             -4
                    :dissertation/cid  -3
                    :dissertation/univ "Harvard University"
                    :dissertation/area "68—Computer science"}
                   {:db/id          -5
                    :person/name    "Stål Olav Aanderaa"
                    :person/advised [-4]}
                   {:db/id             -6
                    :dissertation/cid  -5
                    :dissertation/univ "Harvard University"
                    :dissertation/area "03—Mathematical logic and foundations"}
                   {:db/id          -7
                    :person/name    "Hao Wang"
                    :person/advised [-6]}
                   {:db/id             -8
                    :dissertation/cid  -7
                    :dissertation/univ "Harvard University",
                    :dissertation/area "03—Mathematical logic and foundations"}
                   {:db/id          -9
                    :person/name    "Willard Van Orman Quine"
                    :person/advised [-8]}
                   {:db/id             -10
                    :dissertation/cid  -9
                    :dissertation/univ "Harvard University"}
                   {:db/id          -11
                    :person/name    "Alfred North Whitehead"
                    :person/advised [-10]}
                   {:db/id            -12
                    :dissertation/cid -11}]))

        rule-author '[[(author ?d ?c)
                       [?d :dissertation/cid ?c]]]
        rule-adv    '[[(adv ?x ?y)
                       [?x :person/advised ?d]
                       (author ?d ?y)]]
        rule-area   '[[(area ?c ?a)
                       [?d :dissertation/cid ?c]
                       [?d :dissertation/area ?a]]]
        rule-univ   '[[(univ ?c ?u)
                       [?d :dissertation/cid ?c]
                       [?d :dissertation/univ ?u]]]
        rule-anc    '[[(anc ?x ?y)
                       (adv ?x ?y)]
                      [(anc ?x ?y)
                       (adv ?x ?z)
                       (anc ?z ?y)]]
        rule-q1     (into rule-author rule-adv)
        rule-q2     (into rule-q1 rule-univ)
        rule-q3     (into rule-q1 rule-area)
        rule-q4     (into rule-q1 rule-anc)]
    (is (= ["Stål Olav Aanderaa"]
           (d/q '[:find [?n ...]
                  :in $ %
                  :where
                  [?d :person/name "David Scott Warren"]
                  (adv ?x ?d)
                  (adv ?y ?x)
                  [?y :person/name ?n]]
                db rule-q1)
           (d/q '[:find [?n ...]
                  :in $
                  :where
                  [?d :person/name "David Scott Warren"]
                  [?x :person/advised ?z]
                  [?z :dissertation/cid ?d]
                  [?y :person/advised ?a]
                  [?a :dissertation/cid ?x]
                  [?y :person/name ?n]]
                db)))
    (is (= #{"Hao Wang" "Stål Olav Aanderaa" "Joyce Barbara Friedman"}
           (set (d/q '[:find [?n ...]
                       :in $ %
                       :where
                       (adv ?x ?y)
                       (univ ?x ?u)
                       (univ ?y ?u)
                       [?y :person/name ?n]]
                     db rule-q2))
           (set (d/q '[:find [?n ...]
                       :in $
                       :where
                       (?x :person/advised ?a)
                       (?a :dissertation/cid ?y)
                       (?b :dissertation/cid ?x)
                       (?b :dissertation/univ ?u)
                       (?c :dissertation/cid ?y)
                       (?c :dissertation/univ ?u)
                       [?y :person/name ?n]]
                     db))))
    (is (= ["Joyce Barbara Friedman"]
           (d/q '[:find [?n ...]
                  :in $ %
                  :where
                  (adv ?x ?y)
                  (area ?x ?a1)
                  (area ?y ?a2)
                  [(!= ?a1 ?a2)]
                  [?y :person/name ?n]]
                db rule-q3)
           (d/q '[:find [?n ...]
                  :in $
                  :where
                  [?x :person/advised ?a]
                  [?a :dissertation/cid ?y]
                  (?b :dissertation/cid ?x)
                  (?b :dissertation/area ?a1)
                  (?c :dissertation/cid ?y)
                  (?c :dissertation/area ?a2)
                  [(!= ?a1 ?a2)]
                  [?y :person/name ?n]]
                db)))
    (is (= #{"Hao Wang" "Stål Olav Aanderaa" "Joyce Barbara Friedman"
             "Alfred North Whitehead" "Willard Van Orman Quine"}
           (set (d/q '[:find [?n ...]
                       :in $ %
                       :where
                       [?x :person/name "David Scott Warren"]
                       (anc ?y ?x)
                       [?y :person/name ?n]]
                     db rule-q4))))
    (d/close-db db)
    (u/delete-files dir)))

(deftest magic-set-test
  (let [dir   (u/tmp-dir (str "magic-set-test-" (UUID/randomUUID)))
        conn  (d/get-conn
                dir {:node {:db/unique :db.unique/identity}
                     :to   {:db/cardinality :db.cardinality/many}}
                {:kv-opts {:flags (conj c/default-env-flags :nosync :nolock)}})
        txs   [{:db/id 1 :node 1 :to [2 3]}
               {:db/id 2 :node 2 :to [4]}
               {:db/id 3 :node 3 :to [5]}
               {:db/id 4 :node 4 :to [6]}
               {:db/id 5 :node 5 :to []}
               {:db/id 6 :node 6 :to []}
               {:db/id 10 :node 10 :to [11]}
               {:db/id 11 :node 11 :to [12]}
               {:db/id 12 :node 12 :to []}]
        rules '[[(path ?x ?y)
                 [?e :node ?x]
                 [?e :to ?z]
                 [?z :node ?y]]
                [(path ?x ?y)
                 [?e :node ?x]
                 [?e :to ?z]
                 [?z :node ?z_val]
                 (path ?z_val ?y)]]]
    (d/transact! conn txs)
    (is (= #{2 3 4 5 6}
           (set (d/q '[:find [?y ...]
                       :in $ % ?x
                       :where (path ?x ?y)]
                     (d/db conn) rules 1))))

    ;; Verify it works for 10
    (is (= #{11 12}
           (set (d/q '[:find [?y ...]
                       :in $ % ?x
                       :where (path ?x ?y)]
                     (d/db conn) rules 10))))

    (d/close conn)
    (u/delete-files dir)))

(deftest magic-set-unstable-bound-test
  (let [dir   (u/tmp-dir (str "magic-set-unstable-" (UUID/randomUUID)))
        conn  (d/get-conn
                dir {:node   {:db/unique :db.unique/identity}
                     :parent {:db/valueType :db.type/long}
                     :root   {:db/valueType :db.type/boolean}}
                {:kv-opts {:flags (conj c/default-env-flags :nosync :nolock)}})
        txs   [{:db/id 1 :node 1 :root true}
               {:db/id 2 :node 2 :parent 1}
               {:db/id 3 :node 3 :parent 2}]
        rules '[[(root ?m ?r)
                 [?e :node ?m]
                 [?e :root true]
                 [(identity ?m) ?r]]
                [(root ?m ?r)
                 [?e :node ?m]
                 [?e :parent ?p]
                 (root ?p ?r)]]]
    (d/transact! conn txs)
    (is (= #{1}
           (set (d/q '[:find [?r ...]
                       :in $ % ?m
                       :where (root ?m ?r)]
                     (d/db conn) rules 3))))
    (d/close conn)
    (u/delete-files dir)))

(deftest sequence-generation-memory-test
  (let [dir (u/tmp-dir (str "memory-test-" (UUID/randomUUID)))
        db  (d/empty-db dir nil
                        {:kv-opts {:flags (conj c/default-env-flags
                                                :nosync :nolock)}})]
    (try
      (binding [sut/*auto-optimize-temporal* false]
        ;; Generates a sequence from 0 to limit
        (let [limit 2000
              rules '[[(chain ?limit ?n)
                       [(ground 0) ?n]
                       [(>= ?limit 0)]]
                      [(chain ?limit ?n)
                       (chain ?limit ?prev)
                       [(< ?prev ?limit)]
                       [(inc ?prev) ?n]]]
              res   (d/q '[:find ?n
                           :in $ % ?limit
                           :where (chain ?limit ?n)]
                         db rules limit)]
          (is (= (inc limit) (count res)))
          (is (contains? res [limit]))))
      (finally
        (d/close-db db)
        (u/delete-files dir)))))

(deftest temporal-elimination-test
  (let [dir (u/tmp-dir (str "temporal-test-" (UUID/randomUUID)))
        db  (d/empty-db dir nil
                        {:kv-opts {:flags (conj c/default-env-flags
                                                :nosync :nolock)}})]
    (try
      ;; Generates a sequence from 0 to limit, but discards history
      (binding [sut/*temporal-elimination* true]
        (let [limit 10000
              rules '[[(chain ?limit ?n)
                       [(ground 0) ?n]
                       [(>= ?limit 0)]]
                      [(chain ?limit ?n)
                       (chain ?limit ?prev)
                       [(< ?prev ?limit)]
                       [(inc ?prev) ?n]]]
              res   (d/q '[:find ?n
                           :in $ % ?limit
                           :where (chain ?limit ?n)]
                         db rules limit)]
          ;; Should only contain the last element(s) from the final frontier
          (is (= 1 (count res)))
          (is (= #{[limit]} res))))
      (finally
        (d/close-db db)
        (u/delete-files dir)))))

(deftest auto-temporal-optimization-test
  (let [dir (u/tmp-dir (str "auto-temporal-test-" (UUID/randomUUID)))
        db  (d/empty-db dir nil
                        {:kv-opts {:flags (conj c/default-env-flags
                                                :nosync :nolock)}})]
    (try
      ;; Generates a sequence from 0 to limit
      ;; Should AUTO-DETECT temporal pattern and prune history
      (let [limit 10000
            rules '[[(chain ?limit ?n)
                     [(ground 0) ?n]
                     [(>= ?limit 0)]]
                    [(chain ?limit ?n)
                     (chain ?limit ?prev)
                     [(< ?prev ?limit)]
                     [(inc ?prev) ?n]]]
            res   (d/q '[:find ?n
                         :in $ % ?limit
                         :where (chain ?limit ?n)]
                       db rules limit)]
        ;; Should behave like temporal elimination
        (is (= 1 (count res)))
        (is (= #{[limit]} res)))
      (finally
        (d/close-db db)
        (u/delete-files dir)))))

(deftest mutually-recursive-rules-test
  (let [dir  (u/tmp-dir (str "mutual-recursion-test-" (UUID/randomUUID)))
        conn (d/get-conn dir {:id    {:db/unique :db.unique/identity}
                              :edge1 {:db/valueType :db.type/long}
                              :edge2 {:db/valueType :db.type/long}}
                         {:kv-opts {:flags (conj c/default-env-flags
                                                 :nosync :nolock)}})]
    (d/transact! conn [{:db/id 1 :id 1 :edge1 2}
                       {:db/id 2 :id 2 :edge2 3}
                       {:db/id 3 :id 3 :edge1 4}
                       {:db/id 4 :id 4 :edge2 5}
                       {:db/id 5 :id 5}])
    (let [rules '[[(foo ?id ?y)
                   [?e :id ?id]
                   [?e :edge1 ?y]]
                  [(foo ?id ?y)
                   [?e :id ?id]
                   [?e :edge1 ?z]
                   (bar ?z ?y)]
                  [(bar ?id ?y)
                   [?e :id ?id]
                   [?e :edge2 ?y]]
                  [(bar ?id ?y)
                   [?e :id ?id]
                   [?e :edge2 ?z]
                   (foo ?z ?y)]]]
      (is (= #{2 3 4 5}
             (set (d/q '[:find [?y ...]
                         :in $ % ?x
                         :where (foo ?x ?y)]
                       (d/db conn) rules 1))))
      (is (= #{}
             (set (d/q '[:find [?y ...]
                         :in $ % ?x
                         :where (bar ?x ?y)]
                       (d/db conn) rules 1))))
      (is (= #{3 4 5}
             (set (d/q '[:find [?y ...]
                         :in $ % ?x
                         :where (bar ?x ?y)]
                       (d/db conn) rules 2)))))
    (d/close conn)
    (u/delete-files dir)))

(deftest diamond-graph-path-test
  (let [dir          (u/tmp-dir (str "diamond-path-test-" (UUID/randomUUID)))
        conn         (d/get-conn dir {:node {:db/unique :db.unique/identity}
                                      :from {:db/valueType :db.type/ref}
                                      :to   {:db/valueType :db.type/ref}}
                                 {:kv-opts {:flags (conj c/default-env-flags
                                                         :nosync :nolock)}})
        temp-a       -1
        temp-b       -2
        temp-c       -3
        temp-d       -4
        temp-edge-ab -5
        temp-edge-ac -6
        temp-edge-bd -7
        temp-edge-cd -8]
    (d/transact! conn [{:db/id temp-a :node 'a}
                       {:db/id temp-b :node 'b}
                       {:db/id temp-c :node 'c}
                       {:db/id temp-d :node 'd}
                       {:db/id temp-edge-ab :from temp-a :to temp-b}
                       {:db/id temp-edge-ac :from temp-a :to temp-c}
                       {:db/id temp-edge-bd :from temp-b :to temp-d}
                       {:db/id temp-edge-cd :from temp-c :to temp-d}])
    (let [rules '[[(path ?from ?to)
                   [?e :from ?from]
                   [?e :to ?to]]
                  [(path ?from ?to)
                   [?e :from ?from]
                   [?e :to ?mid]
                   (path ?mid ?to)]]]
      (is (= #{(vec ['a 'd])}
             (set (d/q '[:find ?from-node ?to-node
                         :in $ % ?start-node-sym ?end-node-sym
                         :where
                         [?start-eid :node ?start-node-sym]
                         [?end-eid :node ?end-node-sym]
                         (path ?start-eid ?end-eid)
                         [?start-eid :node ?from-node]
                         [?end-eid :node ?to-node]]
                       (d/db conn) rules 'a 'd))))
      (is (= #{(vec ['a 'b])
               (vec ['a 'c])
               (vec ['b 'd])
               (vec ['c 'd])
               (vec ['a 'd])}
             (set (d/q '[:find ?from-node ?to-node
                         :in $ %
                         :where
                         (path ?from-eid ?to-eid)
                         [?from-eid :node ?from-node]
                         [?to-eid :node ?to-node]]
                       (d/db conn) rules)))))
    (d/close conn)
    (u/delete-files dir)))

(deftest multiple-derivations-test
  (let [dir  (u/tmp-dir (str "multiple-derivations-test-" (UUID/randomUUID)))
        conn (d/get-conn dir {:foo/from {:db/valueType :db.type/long}
                              :foo/to   {:db/valueType :db.type/long}
                              :bar/from {:db/valueType :db.type/long}
                              :bar/to   {:db/valueType :db.type/long}}
                         {:kv-opts {:flags (conj c/default-env-flags
                                                 :nosync :nolock)}})]
    (d/transact! conn [{:db/id -1 :foo/from 1 :foo/to 2}
                       {:db/id -2 :bar/from 1 :bar/to 2}])
    (let [rules '[[(derived ?x ?y)
                   [?e :foo/from ?x]
                   [?e :foo/to ?y]]
                  [(derived ?x ?y)
                   [?e :bar/from ?x]
                   [?e :bar/to ?y]]]]
      (is (= #{[1 2]}
             (set (d/q '[:find ?x ?y
                         :in $ %
                         :where (derived ?x ?y)]
                       (d/db conn) rules)))))
    (d/close conn)
    (u/delete-files dir)))

(deftest stratified-negation-test
  (let [dir  (u/tmp-dir (str "stratified-negation-test-" (UUID/randomUUID)))
        conn (d/get-conn dir {:node {:db/unique :db.unique/identity}
                              :edge {:db/valueType :db.type/ref}
                              :bad  {:db/unique :db.unique/identity}}
                         {:kv-opts {:flags (conj c/default-env-flags
                                                 :nosync :nolock)}})]
    (d/transact! conn [{:db/id -1 :node 1 :edge -2} ;; 1 -> 2
                       {:db/id -2 :node 2 :edge -3} ;; 2 -> 3
                       {:db/id -3 :node 3}
                       {:db/id -4 :node 4 :edge -5} ;; 4 -> 5
                       {:db/id -5 :node 5}
                       {:db/id -6 :node 6}
                       {:db/id -7 :bad 3}]) ;; 3 is initially bad

    (let [rules '[[(bad ?x)
                   [?e :node ?x]
                   [?e :edge ?y]
                   [?y :node ?y_val]
                   (bad ?y_val)]
                  [(bad ?x)
                   [?e :bad ?x]]
                  [(good ?x)
                   [?e :node ?x]
                   (not (bad ?x))]]]
      (is (= #{1 2 3}
             (set (d/q '[:find [?x ...]
                         :in $ %
                         :where (bad ?x)]
                       (d/db conn) rules))))
      (is (= #{4 5 6}
             (set (d/q '[:find [?x ...]
                         :in $ %
                         :where (good ?x)]
                       (d/db conn) rules)))))
    (d/close conn)
    (u/delete-files dir)))

(deftest unsafe-negation-rejection-test
  (let [dir  (u/tmp-dir (str "unsafe-negation-test-" (UUID/randomUUID)))
        conn (d/get-conn dir {:q {:db/valueType :db.type/long}}
                         {:kv-opts {:flags (conj c/default-env-flags
                                                 :nosync :nolock)}})]
    (d/transact! conn [{:db/id -1 :q 1} ;; q(1) is a fact
                       {:db/id -2 :q 2}]) ;; q(2) is a fact

    (let [rules '[[(p ?x)
                   (not (q ?x))]
                  [(p ?x)
                   (q ?x)
                   (p ?x)]
                  [(q ?x)
                   [?e :q ?x]]]]
      ;; Expect an exception when trying to query p(x) due to unsafe negation
      (is (thrown-with-msg?
            clojure.lang.ExceptionInfo
            #"Insufficient bindings: none of #\{\?x.*\} is bound"
            (d/q '[:find ?x
                   :in $ %
                   :where (p ?x)]
                 (d/db conn) rules))))
    (d/close conn)
    (u/delete-files dir)))

(deftest stratified-negation-safe-test
  (let [dir  (u/tmp-dir (str "stratified-negation-safe-test-" (UUID/randomUUID)))
        conn (d/get-conn dir {:id        {:db/unique :db.unique/identity}
                              :value     {:db/valueType :db.type/long}
                              :seen-flag {:db/valueType :db.type/boolean}}
                         {:kv-opts {:flags (conj c/default-env-flags
                                                 :nosync :nolock)}})]
    (d/transact! conn [{:db/id -1 :id 1 :value 10}
                       {:db/id -2 :id 2 :value 20 :seen-flag true}
                       {:db/id -3 :id 3 :value 30}
                       {:db/id -4 :id 4 :value 40 :seen-flag true}
                       {:db/id -5 :id 5 :value 50}])

    (let [rules '[[(domain ?x)
                   [?e :id ?x]]
                  [(seen ?x)
                   [?e :id ?x]
                   [?e :seen-flag true]]
                  [(missing ?x)
                   (domain ?x)
                   (not (seen ?x))]]]
      (is (= #{1 3 5}
             (set (d/q '[:find [?x ...]
                         :in $ %
                         :where (missing ?x)]
                       (d/db conn) rules)))))
    (d/close conn)
    (u/delete-files dir)))

(deftest double-negation-test
  (let [dir  (u/tmp-dir (str "double-negation-test-" (UUID/randomUUID)))
        conn (d/get-conn dir {:entity-id {:db/unique :db.unique/identity}
                              :has-q     {:db/valueType :db.type/boolean}}
                         {:kv-opts {:flags (conj c/default-env-flags
                                                 :nosync :nolock)}})]
    (d/transact! conn [{:db/id -1 :entity-id 1 :has-q true}
                       {:db/id -2 :entity-id 2 :has-q false}
                       {:db/id -3 :entity-id 3 :has-q true}
                       {:db/id -4 :entity-id 4 :has-q false}])

    (let [rules      '[[(base-q ?x)
                        [?e :entity-id ?x]
                        [?e :has-q true]]
                       [(p ?x)
                        [?e :entity-id ?x]
                        (not (not (base-q ?x)))]]
          expected-q (set (d/q '[:find [?x ...]
                                 :in $ %
                                 :where (base-q ?x)]
                               (d/db conn) rules))]
      (is (= expected-q
             (set (d/q '[:find [?x ...]
                         :in $ %
                         :where (p ?x)]
                       (d/db conn) rules)))))
    (d/close conn)
    (u/delete-files dir)))

(deftest join-not-equal-test
  (let [dir  (u/tmp-dir (str "join-not-equal-test-" (UUID/randomUUID)))
        conn (d/get-conn dir {:node-val {:db/unique :db.unique/identity}}
                         {:kv-opts {:flags (conj c/default-env-flags
                                                 :nosync :nolock)}})]
    (d/transact! conn [{:db/id -1 :node-val 1}
                       {:db/id -2 :node-val 2}
                       {:db/id -3 :node-val 3}])

    (let [rules '[[(node ?x)
                   [?e :node-val ?x]]
                  [(pair ?x ?y)
                   (node ?x)
                   (node ?y)
                   [(not= ?x ?y)]]]]
      ;; Expected pairs: all combinations where x != y
      (is (= #{[1 2] [1 3] [2 1] [2 3] [3 1] [3 2]}
             (set (d/q '[:find ?x ?y
                         :in $ %
                         :where (pair ?x ?y)]
                       (d/db conn) rules)))))
    (d/close conn)
    (u/delete-files dir)))

(deftest aggregation-with-recursion-test
  (let [dir  (u/tmp-dir (str "aggregation-recursion-test-" (UUID/randomUUID)))
        conn (d/get-conn dir {:node {:db/unique :db.unique/identity}
                              :edge {:db/valueType   :db.type/ref
                                     :db/cardinality :db.cardinality/many}}
                         {:kv-opts {:flags (conj c/default-env-flags
                                                 :nosync :nolock)}})]
    ;; Graph: 1->2->3->4, and 1->5
    (d/transact! conn [{:db/id -1 :node 1 :edge -2}
                       {:db/id -2 :node 2 :edge -3}
                       {:db/id -3 :node 3 :edge -4}
                       {:db/id -4 :node 4}
                       {:db/id -5 :node 5}])
    ;; Add edge 1->5 separately
    (d/transact! conn [{:db/id [:node 1] :edge [:node 5]}])

    (let [rules '[[(path ?x ?y)
                   [?e :node ?x]
                   [?e :edge ?z]
                   [?z :node ?y]]
                  [(path ?x ?y)
                   [?e :node ?x]
                   [?e :edge ?z]
                   [?z :node ?z_val]
                   (path ?z_val ?y)]]]
      ;; Reachable from 1: 2, 3, 4, 5. Total 4.
      (is (= [[4]]
             (d/q '[:find (count ?y)
                    :in $ % ?start
                    :where (path ?start ?y)]
                  (d/db conn) rules 1))))
    (d/close conn)
    (u/delete-files dir)))

(deftest star-join-test
  (let [dir  (u/tmp-dir (str "star-join-test-" (UUID/randomUUID)))
        conn (d/get-conn dir {:hub   {:db/unique :db.unique/identity}
                              :spoke {:db/valueType   :db.type/ref
                                      :db/cardinality :db.cardinality/many}}
                         {:kv-opts {:flags (conj c/default-env-flags
                                                 :nosync :nolock)}})]
    (d/transact! conn [{:db/id -1 :hub 'h :spoke -2}
                       {:db/id -2 :hub 's1}
                       {:db/id -3 :hub 'h :spoke -4}
                       {:db/id -4 :hub 's2}
                       {:db/id -5 :hub 'h :spoke -6}
                       {:db/id -6 :hub 's3}])

    (let [rules '[[(connected ?hub ?spoke)
                   [?e :hub ?hub]
                   [?e :spoke ?s]
                   [?s :hub ?spoke]]]]
      (is (= #{['h 's1] ['h 's2] ['h 's3]}
             (set (d/q '[:find ?h ?s
                         :in $ %
                         :where (connected ?h ?s)]
                       (d/db conn) rules)))))
    (d/close conn)
    (u/delete-files dir)))

(deftest chain-join-test
  (let [dir  (u/tmp-dir (str "chain-join-test-" (UUID/randomUUID)))
        conn (d/get-conn dir {:val  {:db/unique :db.unique/identity}
                              :next {:db/valueType :db.type/ref}}
                         {:kv-opts {:flags (conj c/default-env-flags
                                                 :nosync :nolock)}})]
    ;; Data: a -> b -> c -> d
    (d/transact! conn [{:db/id -1 :val 'a :next -2}
                       {:db/id -2 :val 'b :next -3}
                       {:db/id -3 :val 'c :next -4}
                       {:db/id -4 :val 'd}])

    (let [rules '[[(r1 ?x ?y)
                   [?e :val ?x]
                   [?e :next ?n]
                   [?n :val ?y]]
                  [(r2 ?x ?y)
                   (r1 ?x ?y)]
                  [(r3 ?x ?y)
                   (r1 ?x ?y)]
                  [(chain ?a ?d)
                   (r1 ?a ?b)
                   (r2 ?b ?c)
                   (r3 ?c ?d)]]]
      (is (= #{['a 'd]}
             (set (d/q '[:find ?start ?end
                         :in $ %
                         :where (chain ?start ?end)]
                       (d/db conn) rules)))))
    (d/close conn)
    (u/delete-files dir)))

(deftest complex-graph-rules-test
  (let [dir    (u/tmp-dir (str "complex-graph-rules-test-" (UUID/randomUUID)))
        schema {:node/id            {:db/unique :db.unique/identity}
                :node/edge          {:db/valueType   :db.type/ref
                                     :db/cardinality :db.cardinality/many}
                :node/blocked-to    {:db/valueType   :db.type/ref
                                     :db/cardinality :db.cardinality/many}
                :node/admin?        {:db/valueType :db.type/boolean}
                :node/suspicious?   {:db/valueType :db.type/boolean}
                :node/service-ports {:db/valueType   :db.type/long
                                     :db/cardinality :db.cardinality/many}
                :user/id            {:db/unique :db.unique/identity}
                :user/owns          {:db/valueType   :db.type/ref
                                     :db/cardinality :db.cardinality/many}
                :user/reported?     {:db/valueType :db.type/boolean}}
        conn   (d/get-conn dir schema
                           {:kv-opts {:flags (conj c/default-env-flags
                                                   :nosync :nolock)}})]

    (d/transact! conn [;; Nodes
                       {:db/id           -1
                        :node/id         "a"
                        :node/edge       [-2 -3]
                        :node/blocked-to [-5]
                        :node/admin?     true}
                       {:db/id              -2
                        :node/id            "b"
                        :node/edge          [-4]
                        :node/service-ports [80 443]}
                       {:db/id     -3
                        :node/id   "c"
                        :node/edge [-4]}
                       {:db/id              -4
                        :node/id            "d"
                        :node/edge          [-5]
                        :node/service-ports [80]}
                       {:db/id              -5
                        :node/id            "e"
                        :node/edge          [-2 -6]
                        :node/blocked-to    [-6]
                        :node/service-ports [22]}
                       {:db/id              -6
                        :node/id            "f"
                        :node/edge          [-6]
                        :node/suspicious?   true
                        :node/service-ports [22]}
                       {:db/id -7 :node/id "g"}

                       ;; Users
                       {:db/id     -10
                        :user/id   "u1"
                        :user/owns [-1]}
                       {:db/id          -11
                        :user/id        "u2"
                        :user/owns      [-6]
                        :user/reported? true}
                       {:db/id   -12
                        :user/id "u3"}])

    (let [rules '[;; Base
                  [(edge ?x ?y)
                   [?e :node/id ?x]
                   [?e :node/edge ?t]
                   [?t :node/id ?y]]
                  [(blocked ?x ?y)
                   [?e :node/id ?x]
                   [?e :node/blocked-to ?t]
                   [?t :node/id ?y]]
                  [(admin ?x)
                   [?e :node/id ?x]
                   [?e :node/admin? true]]
                  [(suspicious ?x)
                   [?e :node/id ?x]
                   [?e :node/suspicious? true]]
                  [(service ?x ?port)
                   [?e :node/id ?x]
                   [?e :node/service-ports ?port]]
                  [(user ?u)
                   [?e :user/id ?u]]
                  [(owns ?u ?n)
                   [?e :user/id ?u]
                   [?e :user/owns ?t]
                   [?t :node/id ?n]]
                  [(reported ?u)
                   [?e :user/id ?u]
                   [?e :user/reported? true]]

                  ;; 1) & 2) Reachability with recursion and duplicate rule
                  [(reach ?x ?y) (edge ?x ?y)]
                  [(reach ?x ?y) (edge ?x ?z) (reach ?z ?y)]
                  [(reach ?x ?y) (edge ?x ?z) (edge ?z ?y)] ;; Dedupe check

                  ;; 3) Same SCC
                  [(same_scc ?x ?y) (reach ?x ?y) (reach ?y ?x)]

                  ;; 4) Policy-filtered reachability (Stratified Negation)
                  [(allowed_edge ?x ?y) (edge ?x ?y) (not (blocked ?x ?y))]
                  [(allowed_reach ?x ?y) (allowed_edge ?x ?y)]
                  [(allowed_reach ?x ?y) (allowed_edge ?x ?z) (allowed_reach ?z ?y)]

                  ;; 5) At Risk
                  [(at_risk ?x) (allowed_reach ?x ?y) (suspicious ?y)]
                  [(at_risk ?x) (same_scc ?x ?y) (suspicious ?y)]

                  ;; 6) Alerting
                  [(needs_alert ?u) (user ?u) (owns ?u ?n) (at_risk ?n)
                   (not (reported ?u))]

                  ;; 7) Mutual Web Pair (Join + Inequality)
                  [(mutual_web_pair ?x ?y)
                   (service ?x 80) (service ?y 80)
                   (same_scc ?x ?y)
                   [(!= ?x ?y)]]

                  ;; 8) Privilege Exception
                  [(admin_user ?u) (owns ?u ?n) (admin ?n)]
                  [(final_alert ?u) (needs_alert ?u) (not (admin_user ?u))]]]

      ;; Reachability - Check diamond a->d
      (is (contains? (set (d/q '[:find [?y ...]
                                 :in $ %
                                 :where (reach "a" ?y)]
                               (d/db conn) rules))
                     "d"))

      ;; Reachability - Check cycle b->e
      (is (contains? (set (d/q '[:find [?y ...]
                                 :in $ %
                                 :where (reach "b" ?y)]
                               (d/db conn) rules))
                     "e"))

      ;; SCC - {b, d, e}
      (is (= #{["b" "d"] ["b" "e"] ["b" "b"]}
             (set (d/q '[:find ?x ?y
                         :in $ %
                         :where
                         (same_scc ?x ?y)
                         [(= ?x "b")]
                         (or [(= ?y "b")] [(= ?y "d")] [(= ?y "e")])]
                       (d/db conn) rules))))

      ;; Allowed Reach - a->f should be blocked via e->f
      (is (empty? (d/q '[:find [?y ...]
                         :in $ %
                         :where
                         (allowed_reach "a" ?y)
                         [(= ?y "f")]]
                       (d/db conn) rules)))

      ;; Allowed Reach - a->e should be allowed (a->b->d->e),
      ;; even though blocked(a,e) exists (no edge a->e)
      (is (contains? (set (d/q '[:find [?y ...]
                                 :in $ %
                                 :where (allowed_reach "a" ?y)]
                               (d/db conn) rules))
                     "e"))

      ;; Mutual Web Pair
      (is (= #{["b" "d"] ["d" "b"]}
             (set (d/q '[:find ?x ?y
                         :in $ %
                         :where (mutual_web_pair ?x ?y)]
                       (d/db conn) rules))))

      ;; At Risk / Alerting
      ;; f is suspicious.
      ;; same_scc(f, f) is true (f->f). So at_risk(f) is true.
      ;; owns(u2, f). reported(u2) is true. needs_alert(u2) should be false.
      (is (empty? (d/q '[:find [?u ...]
                         :in $ %
                         :where (needs_alert ?u)]
                       (d/db conn) rules)))

      ;; Check at_risk population
      (is (= #{"f"}
             (set (d/q '[:find [?x ...]
                         :in $ %
                         :where (at_risk ?x)]
                       (d/db conn) rules)))))

    (d/close conn)
    (u/delete-files dir)))

;; TODO Need to extend the Datalog syntax to allow aggregation function in
;; the rule head
#_(deftest single-linear-regression-test
    (let [dir (u/tmp-dir (str "single-regression-test-" (UUID/randomUUID)))
          db  (-> (d/empty-db
                    dir
                    {:time {:db/valueType :db.type/long}
                     :mass {:db/valueType :db.type/long}}
                    {:kv-opts {:flags (conj c/default-env-flags :nosync)}})
                  (d/db-with [{:time 5 :mass 40}
                              {:time 7 :mass 120}
                              {:time 12 :mass 180}
                              {:time 16 :mass 210}
                              {:time 20 :mass 240}]))]
      (is (< 10
             (d/q '[:find ?p .
                    :in $ %
                    :where
                    (linear-regression ?p)]
                  db '[[(xtrain ?id ?v)
                        [?id :time ?v]]
                       [(ytrain ?id ?y)
                        [?id :mass ?y]]
                       [(predict ?t ?id (sum ?y))
                        (model ?t ?p)
                        (xtrain ?id ?v)
                        [(* ?v ?p) ?y]]
                       [(gradient ?t (sum ?g))
                        (ytrain ?id ?y)
                        (predict ?t ?id ?y')
                        (xtrain ?id ?v)
                        [(* 2 (- ?y' ?y) v?) ?g]]
                       [(model ?t ?p)
                        [(ground 0) ?t]
                        [(ground 0.01 ?p)]]
                       [(model ?t+1 ?p')
                        [(inc ?t) ?t+1]
                        (model ?t ?p)
                        (gradient ?t ?g)
                        [(- ?p (/ (* 0.1 ?g) 5)) ?p']
                        [(< ?t 100)]]
                       ])
             13))
      (d/close-db db)
      (u/delete-files dir)))

(deftest or-in-rules-test
  (let [dir  (u/tmp-dir (str "or-in-rules-test-" (UUID/randomUUID)))
        conn (d/get-conn dir {:name   {:db/unique :db.unique/identity}
                              :father {:db/valueType :db.type/ref}
                              :mother {:db/valueType :db.type/ref}}
                         {:kv-opts {:flags (conj c/default-env-flags
                                                 :nosync :nolock)}})]
    (d/transact! conn [{:db/id -1 :name "Dad"}
                       {:db/id -2 :name "Mom"}
                       {:db/id -3 :name "Child1" :father -1 :mother -2}
                       {:db/id -4 :name "Child2" :father -1}
                       {:db/id -5 :name "Child3" :mother -2}])
    (let [rules '[[(parent ?p ?c)
                   (or [?c :father ?p]
                       [?c :mother ?p])]]]
      (is (= #{["Dad" "Child1"] ["Mom" "Child1"]
               ["Dad" "Child2"]
               ["Mom" "Child3"]}
             (set (d/q '[:find ?p-name ?c-name
                         :in $ %
                         :where
                         (parent ?p ?c)
                         [?p :name ?p-name]
                         [?c :name ?c-name]]
                       (d/db conn) rules)))))
    (d/close conn)
    (u/delete-files dir)))

(deftest and-in-rules-test
  (let [dir  (u/tmp-dir (str "and-in-rules-test-" (UUID/randomUUID)))
        conn (d/get-conn dir {:name   {:db/unique :db.unique/identity}
                              :father {:db/valueType :db.type/ref}
                              :mother {:db/valueType :db.type/ref}}
                         {:kv-opts {:flags (conj c/default-env-flags
                                                 :nosync :nolock)}})]
    (d/transact!
      conn [{:db/id -1 :name "Dad"}
            {:db/id -2 :name "Mom"}
            {:db/id -3 :name "Child1" :father -1 :mother -2}
            {:db/id -4 :name "Child2" :father -1}
            {:db/id -5 :name "Child3" :mother -2}])
    (let [rules '[[(child ?c-name)
                   (and [?c :father ?f]
                        [?c :mother ?m]
                        [?c :name ?c-name])]]]
      (is (= #{"Child1"}
             (set (d/q '[:find [?c-name ...]
                         :in $ %
                         :where
                         (child ?c-name)]
                       (d/db conn) rules)))))
    (d/close conn)
    (u/delete-files dir)))

(deftest not-join-in-rules-test
  (let [dir  (u/tmp-dir (str "not-join-in-rules-test-" (UUID/randomUUID)))
        conn (d/get-conn dir {:name  {:db/unique :db.unique/identity}
                              :likes {:db/valueType   :db.type/string
                                      :db/cardinality :db.cardinality/many}}
                         {:kv-opts {:flags (conj c/default-env-flags
                                                 :nosync :nolock)}})]
    (d/transact! conn [{:db/id -1 :name "Alice" :likes ["Reading" "Hiking"]}
                       {:db/id -2 :name "Bob" :likes ["Hiking" "Swimming"]}
                       {:db/id -3 :name "Charlie" :likes ["Swimming"]}
                       {:db/id -4 :name "David" :likes []}])

    (let [rules '[[(non-reader ?name)
                   [?p :name ?name]
                   (not-join [?p]
                             [?p :likes "Reading"])]]]
      (is (= #{"Bob" "Charlie" "David"}
             (set (d/q '[:find [?name ...]
                         :in $ %
                         :where
                         (non-reader ?name)]
                       (d/db conn) rules)))))
    (d/close conn)
    (u/delete-files dir)))

(deftest not-join-in-rule-with-constant-arg-354-test
  (let [query '[:find ?id
                :in $ %
                :where
                (entity-by-category ?e "cat")
                [?e :id ?id]]
        rules '[[(entity-by-category ?e ?cat)
                 [?e :category ?cat]
                 [?e :score ?score]
                 (not-join [?score ?cat]
                           [?other :category ?cat]
                           [?other :score ?other-score]
                           [(> ?other-score ?score)])]]]
    (is (= #{}
           (d/q query [] rules)))))

(deftest or-join-in-rules-test
  (let [dir  (u/tmp-dir (str "or-join-in-rules-test-" (UUID/randomUUID)))
        conn (d/get-conn dir {:name    {:db/unique :db.unique/identity}
                              :age     {:db/valueType :db.type/long}
                              :student {:db/valueType :db.type/boolean}}
                         {:kv-opts {:flags (conj c/default-env-flags
                                                 :nosync :nolock)}})]
    (d/transact! conn [{:db/id -1 :name "Alice" :age 20 :student false}
                       {:db/id -2 :name "Bob" :age 16 :student true}
                       {:db/id -3 :name "Charlie" :age 22 :student true}
                       {:db/id -4 :name "David" :age 15 :student false}])

    (let [rules '[[(eligible ?name)
                   [?p :name ?name]
                   (or-join [?p]
                            (and [?p :age ?a] [(> ?a 18)])
                            [?p :student true])]]]
      (is (= #{"Alice" "Bob" "Charlie"}
             (set (d/q '[:find [?name ...]
                         :in $ %
                         :where
                         (eligible ?name)]
                       (d/db conn) rules)))))
    (d/close conn)
    (u/delete-files dir)))

(deftest cyclic-transitive-closure-test
  (let [dir  (u/tmp-dir (str "cyclic-tc-test-" (UUID/randomUUID)))
        conn (d/get-conn dir {:node {:db/unique :db.unique/identity}
                              :edge {:db/valueType   :db.type/ref
                                     :db/cardinality :db.cardinality/many}}
                         {:kv-opts {:flags (conj c/default-env-flags
                                                 :nosync :nolock)}})]
    ;; Create a small cyclic graph: 0 -> 1 -> 2 -> 0 (triangle)
    ;; and 0 -> 3 (additional branch)
    (d/transact! conn [{:db/id -1 :node 0 :edge [-2 -4]}
                       {:db/id -2 :node 1 :edge -3}
                       {:db/id -3 :node 2 :edge -1}
                       {:db/id -4 :node 3}])

    (let [rules '[[(tc ?a ?b)
                   [?x :node ?a]
                   [?x :edge ?y]
                   [?y :node ?b]]
                  [(tc ?a ?b)
                   [?x :node ?a]
                   [?x :edge ?z]
                   [?z :node ?mid]
                   (tc ?mid ?b)]]]
      ;; From node 0, should reach: 1, 2, 0 (via cycle), 3
      (is (= #{0 1 2 3}
             (set (d/q '[:find [?b ...]
                         :in $ % ?start
                         :where (tc ?start ?b)]
                       (d/db conn) rules 0))))

      ;; Full TC should have no duplicates
      (let [all-pairs (d/q '[:find ?a ?b
                             :in $ %
                             :where (tc ?a ?b)]
                           (d/db conn) rules)]
        ;; 0 reaches: 0, 1, 2, 3
        ;; 1 reaches: 0, 1, 2, 3
        ;; 2 reaches: 0, 1, 2, 3
        ;; 3 reaches: nothing
        (is (= 12 (count all-pairs)))
        (is (= (count all-pairs) (count (set all-pairs))))))

    (d/close conn)
    (u/delete-files dir)))

(deftest cyclic-mesh-tc-test
  (let [dir  (u/tmp-dir (str "cyclic-mesh-tc-test-" (UUID/randomUUID)))
        conn (d/get-conn dir {:node {:db/unique :db.unique/identity}
                              :edge {:db/valueType   :db.type/ref
                                     :db/cardinality :db.cardinality/many}}
                         {:kv-opts {:flags (conj c/default-env-flags
                                                 :nosync :nolock)}})
        ;; Create a cyclic mesh:
        ;;  each node i connects to (i+1) mod n and (i+step) mod n
        n    (long 10)
        step (long 3)
        txs  (for [i (range n)]
               (let [i (long i)]
                 {:db/id (- (inc i))
                  :node  i
                  :edge  (vec (distinct [(- (inc (long (mod (inc i) n))))
                                         (- (inc (long (mod (+ i step) n))))]))}))]
    (d/transact! conn txs)

    (let [rules '[[(tc ?a ?b)
                   [?x :node ?a]
                   [?x :edge ?y]
                   [?y :node ?b]]
                  [(tc ?a ?b)
                   [?x :node ?a]
                   [?x :edge ?z]
                   [?z :node ?mid]
                   (tc ?mid ?b)]]]
      ;; Full TC on a cyclic mesh of n nodes should have n^2 pairs
      ;; (every node reaches every other node, including itself via cycles)
      (let [all-pairs (d/q '[:find ?a ?b
                             :in $ %
                             :where (tc ?a ?b)]
                           (d/db conn) rules)]
        (is (= (* n n) (count all-pairs)))
        (is (= (count all-pairs) (count (set all-pairs))))))

    (d/close conn)
    (u/delete-files dir)))
