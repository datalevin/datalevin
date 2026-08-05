;;
;; Copyright (c) Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice.
;;
(ns datalevin.test.query-access-vector
  (:require
   [clojure.test :refer [deftest is]]
   [datalevin.built-ins :as built-ins]
   [datalevin.core :as d]
   [datalevin.embedding :as emb]
   [datalevin.parser :as dp]
   [datalevin.query :as q]
   [datalevin.query.access :as qaccess]
   [datalevin.query.access.function :as qfunction]
   [datalevin.query.access.vector :as qvector]
   [datalevin.query.execute :as qexec]
   [datalevin.util :as u])
  (:import
   [java.util UUID]))

(def ^:private vector-schema
  {:embedding {:db/valueType :db.type/vec}
   :query     {:db/valueType :db.type/vec}})

(defn- vector-value
  [^long e]
  (float-array [(float e) (float (rem e 3))]))

(defn- vector-docs
  [^long n]
  (mapv (fn [^long e]
          {:db/id e :embedding (vector-value e)})
        (range 1 (inc n))))

(defn- create-vector-conn
  [dir]
  (d/create-conn
    dir vector-schema
    {:wal?        false
     :vector-opts {:dimensions 2 :metric-type :euclidean}
     :kv-opts     {:inmemory? true :wal? false}}))

(defn- batch-rows
  [batch]
  (mapv vec (:tuples batch)))

(deftest test-vector-cursor-preserves-approximate-stream-and-resumes
  (let [dir   (u/tmp-dir (str "query-access-vector-cursor-"
                              (UUID/randomUUID)))
        conn  (create-vector-conn dir)
        query (float-array [0.0 0.0])
        opts  {:top 5 :display :refs+dists}
        form  '[:find ?e ?dist
                :in $ ?query
                :where
                [(vec-neighbors $ :embedding ?query
                                {:top 5 :display :refs+dists})
                 [[?e _ _ ?dist]]]]]
    (try
      (d/transact! conn (vector-docs 12))
      (let [db       (d/db conn)
            parsed   (dp/parse-query form)
            method   (qfunction/access-method
                       {:vector qvector/access-method})
            demand   (qaccess/complete-demand :exact #{'?e '?dist})
            plan     (first
                       (qaccess/access-plans
                         [method]
                         {:parsed-q parsed
                          :inputs [db query]
                          :demand demand}))
            expected (mapv
                       (fn [^objects tuple]
                         [(aget tuple 0) (aget tuple 3)])
                       (built-ins/vec-neighbors
                         db :embedding query opts))
            work     (qaccess/->AccessWork nil 2 5 nil 0)
            cursor   (qaccess/open-access
                       (:path plan) demand (:bounds plan) work db nil)]
        (try
          (let [batch1 (qaccess/next-batch cursor)
                batch2 (qaccess/next-batch cursor)
                batch3 (qaccess/next-batch cursor)]
            (is (= expected
                   (into [] cat
                         (map batch-rows [batch1 batch2 batch3]))))
            (is (= [2 2 1]
                   (mapv qaccess/batch-work [batch1 batch2 batch3])))
            (is (false? (:exhausted? batch1)))
            (is (true? (:exhausted? batch3)))
            (let [resumed
                  (qaccess/open-access
                    (:path plan) demand (:bounds plan)
                    (assoc work
                           :resume (:frontier batch1)
                           :emitted (qaccess/batch-work batch1))
                    db nil)]
              (try
                (is (= (subvec expected 2 4)
                       (batch-rows (qaccess/next-batch resumed))))
                (finally
                  (qaccess/close-cursor resumed)))))
          (finally
            (qaccess/close-cursor cursor))))
      (finally
        (d/close conn)
        (u/delete-files dir)))))

(deftest test-vector-limit-selects-adaptive-access
  (let [dir   (u/tmp-dir (str "query-access-vector-limit-"
                              (UUID/randomUUID)))
        conn  (create-vector-conn dir)
        query (float-array [0.0 0.0])
        form  '[:find ?e
                :in $ ?query
                :where
                [(vec-neighbors $ :embedding ?query {:top 30}) [[?e _ _]]]
                :limit 4]]
    (try
      (d/transact! conn (vector-docs 30))
      (binding [q/*cache?* false]
        (let [db      (d/db conn)
              explain (d/explain {} form db query)
              result  (d/q form db query)]
          (is (true? (:access-path-selected? explain)))
          (is (= :vector
                 (get-in explain [:preferred-access-plan :method])))
          (is (= :adaptive-limit
                 (get-in explain [:selected-plan-alternative :mode])))
          (is (= 4
                 (get-in explain
                         [:preferred-access-plan :candidate-budget])))
          (is (= :exact
                 (get-in explain [:preferred-access-plan :path-quality])))
          (is (= 4 (count result)))))
      (finally
        (d/close conn)
        (u/delete-files dir)))))

(deftest test-vector-ranked-access-preserves-existing-approximation
  (let [dir   (u/tmp-dir (str "query-access-vector-ranked-"
                              (UUID/randomUUID)))
        conn  (create-vector-conn dir)
        query (float-array [0.0 0.0])
        form  '[:find ?e ?dist
                :in $ ?query
                :where
                [(vec-neighbors $ :embedding ?query
                                {:top 30 :display :refs+dists})
                 [[?e _ _ ?dist]]]
                :order-by [?dist :asc ?e :asc]
                :limit 5]]
    (try
      (d/transact! conn (vector-docs 30))
      (binding [q/*cache?* false]
        (let [db        (d/db conn)
              expected  (binding [qexec/*access-methods* []]
                          (d/q form db query))
              explain   (d/explain {} form db query)
              actual    (d/q form db query)
              preferred (:preferred-access-plan explain)]
          (is (= expected actual))
          (is (= 5 (count actual)))
          (is (true? (:access-path-selected? explain)))
          (is (= :adaptive-top-k
                 (get-in explain [:selected-plan-alternative :mode])))
          (is (= [['?dist :asc]] (:path-ordering preferred)))
          (is (= :approximate-ranked-scan (:strategy preferred)))
          (is (every? (:capabilities preferred)
                      qaccess/top-k-proof-capabilities))))
      (finally
        (d/close conn)
        (u/delete-files dir)))))

(deftest test-vector-frontier-completes-distance-ties
  (let [dir   (u/tmp-dir (str "query-access-vector-ties-"
                              (UUID/randomUUID)))
        conn  (create-vector-conn dir)
        query (float-array [1.0 1.0])
        form  '[:find ?e ?dist
                :in $ ?query
                :where
                [(vec-neighbors $ :embedding ?query
                                {:top 5 :display :refs+dists})
                 [[?e _ _ ?dist]]]
                :order-by [?dist :asc ?e :asc]
                :limit 2]]
    (try
      (d/transact!
        conn
        (mapv (fn [e]
                {:db/id e :embedding (float-array [1.0 1.0])})
              (range 1 6)))
      (let [db      (d/db conn)
            parsed  (dp/parse-query form)
            demand  (qaccess/top-k-demand '[?dist :asc ?e :asc] 0 2)
            method  (qfunction/access-method
                      {:vector qvector/access-method})
            plan    (first
                      (qaccess/access-plans
                        [method]
                        {:parsed-q parsed
                         :inputs [db query]
                         :demand demand}))
            cursor  (qaccess/open-access
                      (:path plan) demand (:bounds plan)
                      (qaccess/->AccessWork nil 2 5 nil 0) db nil)]
        (try
          (let [batch1 (qaccess/next-batch cursor)
                batch2 (qaccess/next-batch cursor)
                batch3 (qaccess/next-batch cursor)
                dist   (second (first (batch-rows batch1)))
                cutoff {:primary-value dist}]
            (is (nil? (:certificate (:frontier batch1))))
            (is (nil? (:certificate (:frontier batch2))))
            (is (= dist (:certificate (:frontier batch3))))
            (is (false?
                  (boolean
                    (qaccess/frontier-satisfies?
                      (:path plan) demand (:frontier batch1) cutoff))))
            (is (true?
                  (boolean
                    (qaccess/frontier-satisfies?
                      (:path plan) demand (:frontier batch3) cutoff))))
            (is (true? (:exhausted? batch3))))
          (finally
            (qaccess/close-cursor cursor))))
      (finally
        (d/close conn)
        (u/delete-files dir)))))

(deftest test-vector-access-retains-source-local-top
  (let [dir   (u/tmp-dir (str "query-access-vector-source-top-"
                              (UUID/randomUUID)))
        conn  (create-vector-conn dir)
        query (float-array [0.0 0.0])
        form  '[:find ?e ?dist
                :in $ ?query
                :where
                [(vec-neighbors $ :embedding ?query
                                {:top 2 :display :refs+dists})
                 [[?e _ _ ?dist]]]
                :order-by [?dist :asc ?e :asc]
                :limit 10]]
    (try
      (d/transact! conn (vector-docs 20))
      (binding [q/*cache?* false]
        (let [db       (d/db conn)
              expected (binding [qexec/*access-methods* []]
                         (d/q form db query))
              explain  (d/explain {} form db query)
              actual   (d/q form db query)]
          (is (= expected actual))
          (is (= 2 (count actual)))
          (is (= 2
                 (get-in explain
                         [:preferred-access-plan :estimate :range-rows])))))
      (finally
        (d/close conn)
        (u/delete-files dir)))))

(deftest test-vector-top-k-matches-conventional-after-filter-and-offset
  (let [dir   (u/tmp-dir (str "query-access-vector-filter-offset-"
                              (UUID/randomUUID)))
        conn  (create-vector-conn dir)
        query (float-array [0.0 0.0])
        form  '[:find ?e ?dist
                :in $ ?query
                :where
                [(vec-neighbors $ :embedding ?query
                                {:top 160 :display :refs+dists})
                 [[?e _ _ ?dist]]]
                [?e :keep true]
                :order-by [?dist :asc ?e :asc]
                :offset 3
                :limit 7]]
    (try
      (d/transact!
        conn
        (mapv (fn [^long e]
                (cond-> {:db/id e :embedding (vector-value e)}
                  (zero? (rem e 5)) (assoc :keep true)))
              (range 1 161)))
      (binding [q/*cache?* false]
        (let [db       (d/db conn)
              expected (binding [qexec/*access-methods* []]
                         (d/q form db query))
              explain  (d/explain {} form db query)
              actual   (d/q form db query)]
          (is (= expected actual))
          (is (= 7 (count actual)))
          (is (every? #(zero? (rem (long (first %)) 5)) actual))
          (is (true? (:access-path-selected? explain)))
          (is (= :adaptive-top-k
                 (get-in explain [:selected-plan-alternative :mode])))
          (is (= 10
                 (get-in explain
                         [:preferred-access-plan :required-count])))))
      (finally
        (d/close conn)
        (u/delete-files dir)))))

(deftest test-vector-options-input-preserves-filtered-approximate-window
  (let [dir   (u/tmp-dir (str "query-access-vector-options-"
                              (UUID/randomUUID)))
        conn  (create-vector-conn dir)
        query (float-array [0.0 0.0])
        opts  {:top 120
               :display :refs+dists
               :vec-filter (fn [ref]
                             (even? (long (first ref))))}
        form  '[:find ?e ?dist
                :in $ ?query ?opts
                :where
                [(vec-neighbors $ :embedding ?query ?opts)
                 [[?e _ _ ?dist]]]
                :order-by [?dist :asc ?e :asc]
                :offset 4
                :limit 8]]
    (try
      (d/transact! conn (vector-docs 120))
      (binding [q/*cache?* false]
        (let [db       (d/db conn)
              expected (binding [qexec/*access-methods* []]
                         (d/q form db query opts))
              explain  (d/explain {} form db query opts)
              actual   (d/q form db query opts)]
          (is (= expected actual))
          (is (= 8 (count actual)))
          (is (every? (comp even? first) actual))
          (is (true? (:access-path-selected? explain)))
          (is (= :adaptive-top-k
                 (get-in explain [:selected-plan-alternative :mode])))))
      (finally
        (d/close conn)
        (u/delete-files dir)))))

(deftest test-vector-top-k-deduplicates-before-offset-and-limit
  (let [dir   (u/tmp-dir (str "query-access-vector-distinct-"
                              (UUID/randomUUID)))
        conn  (d/create-conn
                dir
                {:embedding {:db/valueType   :db.type/vec
                             :db/cardinality :db.cardinality/many}}
                {:wal?        false
                 :vector-opts {:dimensions 2 :metric-type :euclidean}
                 :kv-opts     {:inmemory? true :wal? false}})
        query (float-array [0.0 1.0])
        form  '[:find ?e ?dist
                :in $ ?query
                :where
                [(vec-neighbors $ :embedding ?query
                                {:top 100 :display :refs+dists})
                 [[?e _ _ ?dist]]]
                :order-by [?dist :asc ?e :asc]
                :offset 5
                :limit 10]]
    (try
      ;; Symmetric vectors produce two physical hits with the same projected
      ;; [e distance] tuple for each entity.
      (d/transact!
        conn
        (into []
              (mapcat
                (fn [^long e]
                  [[:db/add e :embedding
                    (float-array [(float e) 1.0])]
                   [:db/add e :embedding
                    (float-array [(float (- e)) 1.0])]])
                (range 1 51))))
      (binding [q/*cache?* false]
        (let [db       (d/db conn)
              raw      (mapv
                         (fn [^objects tuple]
                           [(aget tuple 0) (aget tuple 3)])
                         (built-ins/vec-neighbors
                           db :embedding query
                           {:top 100 :display :refs+dists}))
              expected (binding [qexec/*access-methods* []]
                         (d/q form db query))
              explain  (d/explain {} form db query)
              actual   (d/q form db query)]
          (is (= 100 (count raw)))
          (is (= 50 (count (set raw))))
          (is (= expected actual))
          (is (= 10 (count actual)))
          (is (= 10 (count (set (map first actual)))))
          (is (true? (:access-path-selected? explain)))
          (is (= :adaptive-top-k
                 (get-in explain [:selected-plan-alternative :mode])))))
      (finally
        (d/close conn)
        (u/delete-files dir)))))

(deftest test-vector-multi-domain-cursor-preserves-concatenation
  (let [dir   (u/tmp-dir (str "query-access-vector-domains-"
                              (UUID/randomUUID)))
        conn  (create-vector-conn dir)
        query (float-array [0.0 0.0])
        opts  {:domains ["embedding" "query"]
               :top 3
               :display :refs+dists}
        form  '[:find ?e ?a ?dist
                :in $ ?query-vector
                :where
                [(vec-neighbors $ ?query-vector
                                {:domains ["embedding" "query"]
                                 :top 3
                                 :display :refs+dists})
                 [[?e ?a _ ?dist]]]]]
    (try
      (d/transact!
        conn
        (vec
          (concat
            (map (fn [e] {:db/id e :embedding (vector-value e)})
                 (range 1 7))
            (map (fn [e] {:db/id e :query (vector-value e)})
                 (range 11 17)))))
      (let [db       (d/db conn)
            parsed   (dp/parse-query form)
            demand   (qaccess/complete-demand :exact #{'?e '?a '?dist})
            method   (qfunction/access-method
                       {:vector qvector/access-method})
            plan     (first
                       (qaccess/access-plans
                         [method]
                         {:parsed-q parsed
                          :inputs [db query]
                          :demand demand}))
            expected (mapv
                       (fn [^objects tuple]
                         [(aget tuple 0) (aget tuple 1) (aget tuple 3)])
                       (built-ins/vec-neighbors db query opts))
            cursor   (qaccess/open-access
                       (:path plan) demand (:bounds plan)
                       (qaccess/access-work 2) db nil)]
        (try
          (let [batches (loop [batches []]
                          (let [batch (qaccess/next-batch cursor)
                                batches (conj batches batch)]
                            (if (:exhausted? batch)
                              batches
                              (recur batches))))]
            (is (nil? (get-in plan [:path :ordering])))
            (is (= expected (into [] cat (map batch-rows batches))))
            (is (= [2 2 2] (mapv qaccess/batch-work batches))))
          (finally
            (qaccess/close-cursor cursor))))
      (finally
        (d/close conn)
        (u/delete-files dir)))))

(deftest test-correlated-vector-stays-on-conventional-path
  (let [dir   (u/tmp-dir (str "query-access-vector-correlated-"
                              (UUID/randomUUID)))
        conn  (create-vector-conn dir)
        form  '[:find ?e
                :where
                [?seed :query ?query]
                [(vec-neighbors $ :embedding ?query {:top 10}) [[?e _ _]]]]]
    (try
      (d/transact! conn [{:db/id 1 :query (float-array [0.0 0.0])}
                         {:db/id 2 :embedding (float-array [0.0 0.0])}])
      (binding [q/*cache?* false]
        (let [explain (d/explain {} form (d/db conn))]
          (is (empty? (:access-plans explain)))
          (is (false? (:access-path-selected? explain)))))
      (finally
        (d/close conn)
        (u/delete-files dir)))))

(defn- fake-embedding-provider
  []
  (let [value (fn [text]
                (float
                  (case text
                    "zero"      0.0
                    "one"       1.0
                    "two"       2.0
                    "near-zero" 0.1
                    3.0)))]
    (reify
      emb/IEmbeddingProvider
      (embedding [_ items _]
        (mapv (fn [{:keys [text]}]
                (float-array [(value text) 0.0]))
              items))
      (embedding-metadata [_]
        {:embedding/provider {:kind :test :id :test}
         :embedding/output   {:dimensions 2}})
      (embedding-dimensions [_] 2)
      (close-provider [_] nil))))

(deftest test-embedding-neighbors-uses-vector-access
  (let [dir      (u/tmp-dir (str "query-access-embedding-"
                                 (UUID/randomUUID)))
        provider (fake-embedding-provider)
        conn     (d/create-conn
                   dir
                   {:text {:db/valueType            :db.type/string
                           :db/embedding            true
                           :db.embedding/autoDomain true}}
                   {:wal?                false
                    :embedding-opts      {:provider    :test
                                          :metric-type :euclidean}
                    :embedding-providers {:test provider}
                    :kv-opts             {:inmemory? true :wal? false}})
        form     '[:find ?e ?dist
                   :in $ ?query
                   :where
                   [(embedding-neighbors $ :text ?query
                                         {:top 20 :display :refs+dists})
                    [[?e _ _ ?dist]]]
                   :order-by [?dist :asc ?e :asc]
                   :limit 2]]
    (try
      (d/transact!
        conn
        (into [{:db/id 1 :text "zero"}
               {:db/id 2 :text "one"}
               {:db/id 3 :text "two"}]
              (map (fn [e] {:db/id e :text "far"})
                   (range 4 21))))
      (binding [q/*cache?* false]
        (let [db       (d/db conn)
              expected (binding [qexec/*access-methods* []]
                         (d/q form db "near-zero"))
              explain  (d/explain {} form db "near-zero")
              actual   (d/q form db "near-zero")]
          (is (= expected actual))
          (is (= [1 2] (mapv first actual)))
          (is (true? (:access-path-selected? explain)))
          (is (= :vector
                 (get-in explain [:preferred-access-plan :method])))
          (is (= :adaptive-top-k
                 (get-in explain [:selected-plan-alternative :mode])))))
      (finally
        (d/close conn)
        (u/delete-files dir)))))
