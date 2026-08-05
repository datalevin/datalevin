;;
;; Copyright (c) Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice.
;;
(ns datalevin.test.query-access-idoc
  (:require
   [clojure.test :refer [deftest is]]
   [datalevin.built-ins :as built-ins]
   [datalevin.constants :as c]
   [datalevin.core :as d]
   [datalevin.idoc :as idoc]
   [datalevin.query :as q]
   [datalevin.query.access :as qaccess]
   [datalevin.query.access.function :as qfunction]
   [datalevin.query.access.idoc :as qidoc]
   [datalevin.query.cache :as qcache]
   [datalevin.query.execute :as qexec]))

(deftest test-idoc-match-request-defers-candidate-preparation
  (let [conn (d/create-conn
               nil
               {:doc/idoc {:db/valueType :db.type/idoc
                           :db/domain    "profiles"}}
               {:kv-opts {:inmemory? true}})]
    (try
      (d/transact! conn [{:db/id    1
                          :doc/idoc {:status "active" :rank 10}}
                         {:db/id    2
                          :doc/idoc {:status "inactive" :rank 20}}])
      (let [db      (d/db conn)
            needed (int-array [0 2])
            request
            (with-redefs [idoc/candidate-ids*
                          (fn [& _]
                            (throw
                              (ex-info "Candidate preparation is premature"
                                       {})))]
              (built-ins/idoc-match-request
                db :doc/idoc {:status "active"} nil needed))
            tuples  (built-ins/execute-idoc-match-request request)]
        (is (= ["profiles"] (:domains request)))
        (is (= [[1 {:status "active" :rank 10}]]
               (mapv vec tuples))))
      (finally
        (d/close conn)))))

(deftest test-idoc-access-discovery-does-not-prepare-candidates
  (let [conn (d/create-conn
               nil
               {:doc/idoc {:db/valueType :db.type/idoc
                           :db/domain    "profiles"}}
               {:kv-opts {:inmemory? true}})
        query
        '[:find ?e
          :where
          [(idoc-match $ :doc/idoc {:status "active"}) [[?e _ _]]]]]
    (try
      (d/transact! conn [{:db/id    1
                          :doc/idoc {:status "active"}}
                         {:db/id    2
                          :doc/idoc {:status "inactive"}}])
      (let [explain
            (with-redefs [idoc/candidate-ids*
                          (fn [& _]
                            (throw
                              (ex-info "Planning prepared idoc candidates"
                                       {})))]
              (d/explain {} query (d/db conn)))
            preferred (:preferred-access-plan explain)]
        (is (= :idoc (:method preferred)))
        (is (= :complete-scan (:strategy preferred)))
        (is (= 2 (get-in preferred [:estimate :scan-rows])))
        (is (= 1 (get-in preferred [:estimate :output-rows])))
        (is (= {:sampling :none :preparation :execution}
               (select-keys (:policy preferred)
                            [:sampling :preparation])))
        (is (false? (:access-path-selected? explain))))
      (finally
        (d/close conn)))))

(deftest test-idoc-access-is-selected-for-cheaper-joint-plan
  (let [conn (d/create-conn
               nil
               {:doc/idoc {:db/valueType :db.type/idoc
                           :db/domain    "profiles"}}
               {:kv-opts {:inmemory? true}})
        query
        '[:find ?e ?name
          :where
          [(idoc-match $ :doc/idoc {:status "active"}) [[?e _ _]]]
          [?e :name ?name]]]
    (try
      (d/transact!
        conn
        (into
          [{:db/id 1001
            :name "Ada"
            :doc/idoc {:status "active"}}
           {:db/id 1002
            :name "Grace"
            :doc/idoc {:status "inactive"}}]
          (map (fn [e] {:db/id e :name (str "name-" e)}))
          (range 1 1001)))
      (let [db       (d/db conn)
            expected (binding [qexec/*access-methods* []
                               q/*cache?*              false]
                       (d/q query db))
            explain  (binding [q/*cache?* false]
                       (d/explain {} query db))
            actual   (binding [q/*cache?* false]
                       (d/q query db))]
        (is (= #{[1001 "Ada"]} expected))
        (is (= expected actual))
        (is (true? (:access-path-selected? explain)))
        (is (= :access
               (get-in explain [:selected-plan-alternative :kind])))
        (is (= :idoc
               (get-in explain [:preferred-access-plan :method])))
        (is (pos? (get-in explain
                          [:preferred-access-plan :estimate :scan-rows])))
        (is (= c/magic-cost-pred
               (get-in explain
                       [:preferred-access-plan :estimate :per-row]))))
      (finally
        (d/close conn)))))

(deftest test-idoc-cursor-batches-and-resumes-without-duplicates
  (let [conn (d/create-conn
               nil
               {:doc/idoc {:db/valueType :db.type/idoc
                           :db/domain    "profiles"}}
               {:kv-opts {:inmemory? true}})
        query
        '[:find ?e
          :where
          [(idoc-match $ :doc/idoc {:status "active"}) [[?e _ _]]]]]
    (try
      (d/transact! conn
                   (mapv (fn [e]
                           {:db/id e :doc/idoc {:status "active"}})
                         (range 1 11)))
      (let [db         (d/db conn)
            parsed     (qcache/parsed-q query)
            demand     (qaccess/limit-demand 0 5 :exact #{'?e})
            dispatcher (qfunction/access-method
                         {:idoc qidoc/access-method})
            plan       (first
                         (qaccess/access-plans
                           [dispatcher]
                           {:parsed-q parsed
                            :inputs [db]
                            :demand demand}))
            path       (:path plan)
            events     (atom [])]
        (is (contains? (:capabilities path) :resumable))
        (binding [idoc/*trace* #(swap! events conj %)]
          (let [work   (qaccess/->AccessWork nil 2 5 nil 0)
                cursor (qaccess/open-access
                         path demand (:bounds plan) work db nil)]
            (try
              (let [batch1 (qaccess/next-batch cursor)
                    batch2 (qaccess/next-batch cursor)
                    batch3 (qaccess/next-batch cursor)
                    tuples (mapv vec
                                 (concat (:tuples batch1)
                                         (:tuples batch2)
                                         (:tuples batch3)))]
                (is (= [2 2 1]
                       (mapv qaccess/batch-work
                             [batch1 batch2 batch3])))
                (is (= 5 (count tuples)))
                (is (= 5 (count (set tuples))))
                (is (false? (:exhausted? batch3)))
                (let [resumed
                      (qaccess/open-access
                        path demand (:bounds plan)
                        (assoc work
                               :resume (:frontier batch1)
                               :emitted (qaccess/batch-work batch1))
                        db nil)]
                  (try
                    (is (= (mapv vec (:tuples batch2))
                           (mapv vec
                                 (:tuples
                                   (qaccess/next-batch resumed)))))
                    (finally
                      (qaccess/close-cursor resumed)))))
              (finally
                (qaccess/close-cursor cursor))))
          (is (= [2 5] (sort (map :inspected-count @events))))
          (is (every? :partial? @events))))
      (finally
        (d/close conn)))))

(deftest test-idoc-cursor-pages-across-domains
  (let [conn (d/create-conn
               nil
               {:doc/profile {:db/valueType :db.type/idoc
                              :db/domain    "profiles"}
                :doc/order   {:db/valueType :db.type/idoc
                              :db/domain    "orders"}}
               {:kv-opts {:inmemory? true}})
        query
        '[:find ?e ?a
          :where
          [(idoc-match $ {:status "active"}
             {:domains ["profiles" "orders"]}) [[?e ?a _]]]]]
    (try
      (d/transact! conn [{:db/id 1 :doc/profile {:status "active"}}
                         {:db/id 2 :doc/profile {:status "active"}}
                         {:db/id 3 :doc/order {:status "active"}}
                         {:db/id 4 :doc/order {:status "active"}}])
      (let [db         (d/db conn)
            parsed     (qcache/parsed-q query)
            demand     (qaccess/complete-demand :exact #{'?e '?a})
            dispatcher (qfunction/access-method
                         {:idoc qidoc/access-method})
            plan       (first
                         (qaccess/access-plans
                           [dispatcher]
                           {:parsed-q parsed
                            :inputs [db]
                            :demand demand}))
            cursor     (qaccess/open-access
                         (:path plan) demand (:bounds plan)
                         (qaccess/access-work 3) db nil)]
        (try
          (let [batch1 (qaccess/next-batch cursor)
                batch2 (qaccess/next-batch cursor)
                tuples (set (map vec
                                 (concat (:tuples batch1)
                                         (:tuples batch2))))]
            (is (= [3 1] (mapv qaccess/batch-work [batch1 batch2])))
            (is (false? (:exhausted? batch1)))
            (is (true? (:exhausted? batch2)))
            (is (= #{[1 :doc/profile]
                     [2 :doc/profile]
                     [3 :doc/order]
                     [4 :doc/order]}
                   tuples)))
          (finally
            (qaccess/close-cursor cursor))))
      (finally
        (d/close conn)))))

(deftest test-idoc-limit-preserves-distinct-window-on-budget-fallback
  (let [conn (d/create-conn
               nil
               {:doc/profile {:db/valueType :db.type/idoc
                              :db/domain    "profiles"}
                :doc/order   {:db/valueType :db.type/idoc
                              :db/domain    "orders"}}
               {:kv-opts {:inmemory? true}})
        query
        '[:find ?e
          :where
          [(idoc-match $ {:status "active"}
             {:domains ["profiles" "orders"]}) [[?e _ _]]]
          :offset 5
          :limit 10]
        events (atom [])]
    (try
      ;; The first eight entities occur in both domains. If the candidate
      ;; budget ends before there are offset-plus-limit distinct projected
      ;; rows, the adaptive executor must use the conventional fallback rather
      ;; than return a short page.
      (d/transact!
        conn
        (mapv (fn [^long e]
                (cond-> {:db/id e
                         :doc/order {:status "active"}}
                  (<= e 8)
                  (assoc :doc/profile {:status "active"})))
              (range 1 101)))
      (binding [q/*cache?* false]
        (let [db       (d/db conn)
              raw      (mapv vec
                             (built-ins/idoc-match
                               db {:status "active"}
                               {:domains ["profiles" "orders"]}))
              entities (set (map first raw))
              explain  (d/explain {} query db)
              actual   (binding [idoc/*trace* #(swap! events conj %)]
                         (d/q query db))]
          (is (= 108 (count raw)))
          (is (= 100 (count entities)))
          (is (true? (:access-path-selected? explain)))
          (is (= :adaptive-limit
                 (get-in explain [:selected-plan-alternative :mode])))
          (is (= 15
                 (get-in explain
                         [:preferred-access-plan :required-count])))
          (is (= 15
                 (get-in explain
                         [:preferred-access-plan :candidate-budget])))
          (is (= 10 (count actual)))
          (is (= 10 (count (set actual))))
          (is (every? #(contains? entities (first %)) actual))
          (is (= 15 (reduce + 0 (keep :inspected-count @events))))
          (is (some #(not (contains? % :inspected-count)) @events))))
      (finally
        (d/close conn)))))

(deftest test-idoc-only-limit-selects-adaptive-access
  (let [conn (d/create-conn
               nil
               {:doc/idoc {:db/valueType :db.type/idoc
                           :db/domain    "profiles"}}
               {:kv-opts {:inmemory? true}})
        query
        '[:find ?e
          :where
          [(idoc-match $ :doc/idoc {:status "active"}) [[?e _ _]]]
          :offset 2
          :limit 5]
        events (atom [])]
    (try
      (d/transact! conn
                   (mapv (fn [e]
                           {:db/id e :doc/idoc {:status "active"}})
                         (range 1 101)))
      (binding [q/*cache?* false]
        (let [explain (d/explain {} query (d/db conn))
              result  (binding [idoc/*trace* #(swap! events conj %)]
                        (d/q query (d/db conn)))]
          (is (true? (:access-path-selected? explain)))
          (is (= :adaptive-limit
                 (get-in explain [:selected-plan-alternative :mode])))
          (is (= 7
                 (get-in explain
                         [:preferred-access-plan
                          :required-count])))
          (is (= 7
                 (get-in explain
                         [:preferred-access-plan
                          :candidate-budget])))
          (is (= 5 (count result)))
          (is (= [7] (mapv :inspected-count @events)))
          (is (true? (:partial? (first @events))))))
      (finally
        (d/close conn)))))

(deftest test-idoc-limit-overfetches-for-filtering-join
  (let [conn (d/create-conn
               nil
               {:doc/idoc {:db/valueType :db.type/idoc
                           :db/domain    "profiles"}}
               {:kv-opts {:inmemory? true}})
        query
        '[:find ?e
          :where
          [(idoc-match $ :doc/idoc {:status "active"}) [[?e _ _]]]
          [?e :keep true]
          :offset 1
          :limit 3]
        events (atom [])]
    (try
      (d/transact!
        conn
        (mapv (fn [e]
                (cond-> {:db/id e :doc/idoc {:status "active"}}
                  (zero? (long (mod (long e) 10))) (assoc :keep true)))
              (range 1 101)))
      (binding [q/*cache?* false]
        (let [explain (d/explain {} query (d/db conn))
              result  (binding [idoc/*trace* #(swap! events conj %)]
                        (d/q query (d/db conn)))]
          (is (true? (:access-path-selected? explain)))
          (is (= :adaptive-limit
                 (get-in explain [:selected-plan-alternative :mode])))
          (is (= 80
                 (get-in explain
                         [:preferred-access-plan :candidate-budget])))
          (is (= 3 (count result)))
          (is (every? #(zero? (long (mod (long (first %)) 10)))
                      result))
          (is (= [40] (mapv :inspected-count @events)))
          (is (true? (:partial? (first @events))))))
      (finally
        (d/close conn)))))

(deftest test-idoc-limit-does-not-batch-global-aggregation
  (let [conn (d/create-conn
               nil
               {:doc/idoc {:db/valueType :db.type/idoc
                           :db/domain    "profiles"}}
               {:kv-opts {:inmemory? true}})
        query
        '[:find (count ?e)
          :where
          [(idoc-match $ :doc/idoc {:status "active"}) [[?e _ _]]]
          :limit 1]]
    (try
      (d/transact! conn
                   (mapv (fn [e]
                           {:db/id e :doc/idoc {:status "active"}})
                         (range 1 21)))
      (binding [q/*cache?* false]
        (let [explain (d/explain {} query (d/db conn))]
          (is (nil? (get-in explain
                            [:preferred-access-plan :required-count])))
          (is (not= :adaptive-limit
                    (get-in explain
                            [:selected-plan-alternative :mode])))
          (is (= [[20]] (d/q query (d/db conn))))))
      (finally
        (d/close conn)))))

(deftest test-correlated-idoc-clause-stays-on-conventional-path
  (let [conn (d/create-conn
               nil
               {:doc/idoc {:db/valueType :db.type/idoc
                           :db/domain    "profiles"}}
               {:kv-opts {:inmemory? true}})
        query
        '[:find ?e
          :where
          [?seed :query ?query]
          [(idoc-match $ :doc/idoc ?query) [[?e _ _]]]]]
    (try
      (d/transact! conn [{:db/id 1 :query {:status "active"}}
                         {:db/id 2 :doc/idoc {:status "active"}}])
      (let [explain (binding [q/*cache?* false]
                      (d/explain {} query (d/db conn)))]
        (is (empty? (:access-plans explain)))
        (is (false? (:access-path-selected? explain))))
      (finally
        (d/close conn)))))

(deftest test-idoc-access-method-is-registered-once
  (is (= 1
         (count
           (filter #(identical? qidoc/access-method %)
                   (mapcat
                     (fn [method]
                       (vals (or (:backends method) {})))
                     qexec/*access-methods*))))))

(deftest test-idoc-match-request-preserves-verification-and-tracing
  (let [conn (d/create-conn
               nil
               {:doc/idoc {:db/valueType :db.type/idoc
                           :db/domain    "profiles"}}
               {:kv-opts {:inmemory? true}})
        events (atom [])]
    (try
      (d/transact! conn [{:db/id    1
                          :doc/idoc {:rank 10}}
                         {:db/id    2
                          :doc/idoc {:rank 20}}])
      (let [request (built-ins/idoc-match-request
                      (d/db conn) :doc/idoc '{:rank (> 10)} nil nil)
            tuples  (binding [idoc/*trace* #(swap! events conj %)]
                      (built-ins/execute-idoc-match-request request))]
        (is (= [[2 :doc/idoc {:rank 20}]]
               (mapv vec tuples)))
        (is (= 1 (count @events)))
        (is (= :idoc-match-domain (:event (first @events))))
        (is (= "profiles" (:domain (first @events))))
        (is (= 1 (:match-count (first @events)))))
      (finally
        (d/close conn)))))
