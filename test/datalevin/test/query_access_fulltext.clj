;;
;; Copyright (c) Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice.
;;
(ns datalevin.test.query-access-fulltext
  (:require
   [clojure.string :as str]
   [clojure.test :refer [deftest is]]
   [datalevin.built-ins :as built-ins]
   [datalevin.core :as d]
   [datalevin.parser :as dp]
   [datalevin.query :as q]
   [datalevin.query.access :as qaccess]
   [datalevin.query.access.fulltext :as qfulltext]
   [datalevin.query.access.function :as qfunction]
   [datalevin.query.execute :as qexec]
   [datalevin.util :as u])
  (:import
   [java.util UUID]))

(def ^:private fulltext-schema
  {:text {:db/valueType           :db.type/string
          :db/fulltext            true
          :db.fulltext/autoDomain true}})

(defn- fulltext-docs
  [^long n]
  (mapv
    (fn [^long e]
      (cond-> {:db/id e
               :text  (str (apply str (repeat e "red ")) "document")}
        (== 0 (rem e 5)) (assoc :keep true)))
    (range 1 (inc n))))

(defn- batch-rows
  [batch]
  (mapv vec (:tuples batch)))

(deftest test-fulltext-cursor-preserves-function-window-and-resumes
  (let [dir  (u/tmp-dir (str "query-access-fulltext-cursor-"
                             (UUID/randomUUID)))
        conn (d/create-conn
               dir fulltext-schema {:search-domains {"text" {}}})
        opts {:display :refs+scores :offset 1 :limit 5}
        form '[:find ?e ?score
               :where
               [(fulltext $ :text "red"
                          {:display :refs+scores :offset 1 :limit 5})
                [[?e _ _ ?score]]]]]
    (try
      (d/transact! conn (fulltext-docs 12))
      (let [db       (d/db conn)
            parsed   (dp/parse-query form)
            method   (qfunction/access-method
                       {:fulltext qfulltext/access-method})
            demand   (qaccess/complete-demand :exact #{'?e '?score})
            plan     (first
                       (qaccess/access-plans
                         [method]
                         {:parsed-q parsed :inputs [db] :demand demand}))
            expected (mapv
                       (fn [^objects tuple]
                         [(aget tuple 0) (aget tuple 3)])
                       (built-ins/fulltext db :text "red" opts))
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

(deftest test-fulltext-limit-selects-adaptive-access
  (let [dir  (u/tmp-dir (str "query-access-fulltext-limit-"
                             (UUID/randomUUID)))
        conn (d/create-conn
               dir fulltext-schema {:search-domains {"text" {}}})
        query
        '[:find ?e
          :where
          [(fulltext $ :text "red" {:top 40}) [[?e _ _]]]
          :limit 4]]
    (try
      (d/transact! conn (fulltext-docs 40))
      (binding [q/*cache?* false]
        (let [db      (d/db conn)
              explain (d/explain {} query db)
              result  (d/q query db)]
          (is (true? (:access-path-selected? explain)))
          (is (= :fulltext
                 (get-in explain [:preferred-access-plan :method])))
          (is (= :adaptive-limit
                 (get-in explain [:selected-plan-alternative :mode])))
          (is (= 4
                 (get-in explain
                         [:preferred-access-plan :candidate-budget])))
          (is (= 4 (count result)))
          (is (every? #(<= 1 (long (first %)) 40) result))))
      (finally
        (d/close conn)
        (u/delete-files dir)))))

(deftest test-fulltext-access-preserves-text-display-projection
  (let [dir  (u/tmp-dir (str "query-access-fulltext-text-display-"
                             (UUID/randomUUID)))
        conn (d/create-conn
               dir fulltext-schema
               {:search-domains {"text" {:include-text? true}}})
        query
        '[:find ?e ?raw-text
          :where
          [(fulltext $ :text "red" {:top 12 :display :texts})
           [[?e _ _ ?raw-text]]]
          :limit 3]]
    (try
      (d/transact! conn (fulltext-docs 12))
      (binding [q/*cache?* false]
        (let [db      (d/db conn)
              explain (d/explain {} query db)
              result  (d/q query db)]
          (is (true? (:access-path-selected? explain)))
          (is (= :adaptive-limit
                 (get-in explain [:selected-plan-alternative :mode])))
          (is (nil? (get-in explain
                            [:preferred-access-plan :path-ordering])))
          (is (= 3 (count result)))
          (is (every? (fn [[_ raw-text]]
                        (and (string? raw-text)
                             (str/includes? raw-text "red")))
                      result))))
      (finally
        (d/close conn)
        (u/delete-files dir)))))

(deftest test-fulltext-limit-stops-consuming-ranked-window
  (let [dir  (u/tmp-dir (str "query-access-fulltext-work-"
                             (UUID/randomUUID)))
        conn (d/create-conn
               dir fulltext-schema {:search-domains {"text" {}}})
        query
        '[:find ?e
          :in $ ?opts
          :where
          [(fulltext $ :text "red" ?opts) [[?e _ _]]]
          :limit 4]
        pushed-calls (atom 0)
        normal-calls (atom 0)
        pushed-opts {:top 200
                     :doc-filter (fn [_]
                                   (swap! pushed-calls inc)
                                   true)}
        normal-opts {:top 200
                     :doc-filter (fn [_]
                                   (swap! normal-calls inc)
                                   true)}]
    (try
      (d/transact! conn (fulltext-docs 200))
      (binding [q/*cache?* false]
        (let [db     (d/db conn)
              pushed (d/q query db pushed-opts)
              normal (binding [qexec/*access-methods* []]
                       (d/q query db normal-opts))]
          (is (= 4 (count pushed) (count normal)))
          ;; Search results are realized in a 32-element chunk plus one
          ;; frontier lookahead, instead of consuming the complete window.
          (is (<= @pushed-calls 33))
          (is (= 200 @normal-calls))
          (is (< @pushed-calls @normal-calls))))
      (finally
        (d/close conn)
        (u/delete-files dir)))))

(deftest test-fulltext-ranked-access-preserves-ordered-query
  (let [dir  (u/tmp-dir (str "query-access-fulltext-ranked-"
                             (UUID/randomUUID)))
        conn (d/create-conn
               dir fulltext-schema {:search-domains {"text" {}}})
        query
        '[:find ?e ?score
          :where
          [(fulltext $ :text "red" {:top 30 :display :refs+scores})
           [[?e _ _ ?score]]]
          :order-by [?score :desc ?e :asc]
          :limit 5]]
    (try
      (d/transact! conn (fulltext-docs 30))
      (binding [q/*cache?* false]
        (let [db       (d/db conn)
              expected (binding [qexec/*access-methods* []]
                         (d/q query db))
              explain  (d/explain {} query db)
              actual   (d/q query db)
              preferred (:preferred-access-plan explain)]
          (is (= expected actual))
          (is (= 5 (count actual)))
          (is (true? (:access-path-selected? explain)))
          (is (= :adaptive-top-k
                 (get-in explain [:selected-plan-alternative :mode])))
          (is (= [['?score :desc]] (:path-ordering preferred)))
          (is (every? (:capabilities preferred)
                      qaccess/top-k-proof-capabilities))))
      (finally
        (d/close conn)
        (u/delete-files dir)))))

(deftest test-fulltext-frontier-completes-score-ties
  (let [dir  (u/tmp-dir (str "query-access-fulltext-ties-"
                             (UUID/randomUUID)))
        conn (d/create-conn
               dir fulltext-schema {:search-domains {"text" {}}})
        form
        '[:find ?e ?score
          :where
          [(fulltext $ :text "red" {:top 5 :display :refs+scores})
           [[?e _ _ ?score]]]
          :order-by [?score :desc ?e :asc]
          :limit 2]]
    (try
      (d/transact! conn
                   (mapv (fn [e] {:db/id e :text "red document"})
                         (range 1 6)))
      (let [db      (d/db conn)
            parsed  (dp/parse-query form)
            demand  (qaccess/top-k-demand '[?score :desc ?e :asc] 0 2)
            method  (qfunction/access-method
                      {:fulltext qfulltext/access-method})
            plan    (first
                      (qaccess/access-plans
                        [method]
                        {:parsed-q parsed :inputs [db] :demand demand}))
            cursor  (qaccess/open-access
                      (:path plan) demand (:bounds plan)
                      (qaccess/->AccessWork nil 2 5 nil 0) db nil)]
        (try
          (let [batch1 (qaccess/next-batch cursor)
                batch2 (qaccess/next-batch cursor)
                batch3 (qaccess/next-batch cursor)
                score  (second (first (batch-rows batch1)))
                cutoff {:primary-value score}]
            (is (nil? (:certificate (:frontier batch1))))
            (is (nil? (:certificate (:frontier batch2))))
            (is (= score (:certificate (:frontier batch3))))
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

(deftest test-fulltext-access-retains-source-local-top
  (let [dir  (u/tmp-dir (str "query-access-fulltext-source-top-"
                             (UUID/randomUUID)))
        conn (d/create-conn
               dir fulltext-schema {:search-domains {"text" {}}})
        query
        '[:find ?e ?score
          :where
          [(fulltext $ :text "red" {:top 2 :display :refs+scores})
           [[?e _ _ ?score]]]
          :order-by [?score :desc ?e :asc]
          :limit 10]]
    (try
      (d/transact! conn (fulltext-docs 20))
      (binding [q/*cache?* false]
        (let [db       (d/db conn)
              expected (binding [qexec/*access-methods* []]
                         (d/q query db))
              explain  (d/explain {} query db)
              actual   (d/q query db)]
          (is (= expected actual))
          (is (= 2 (count actual)))
          (is (= 2
                 (get-in explain
                         [:preferred-access-plan :estimate :range-rows])))))
      (finally
        (d/close conn)
        (u/delete-files dir)))))

(deftest test-fulltext-top-k-matches-conventional-after-filter-and-offset
  (let [dir  (u/tmp-dir (str "query-access-fulltext-filter-offset-"
                             (UUID/randomUUID)))
        conn (d/create-conn
               dir fulltext-schema {:search-domains {"text" {}}})
        query
        '[:find ?e ?score
          :where
          [(fulltext $ :text "red" {:top 160 :display :refs+scores})
           [[?e _ _ ?score]]]
          [?e :keep true]
          :order-by [?score :desc ?e :asc]
          :offset 3
          :limit 7]]
    (try
      (d/transact! conn (fulltext-docs 160))
      (binding [q/*cache?* false]
        (let [db       (d/db conn)
              expected (binding [qexec/*access-methods* []]
                         (d/q query db))
              explain  (d/explain {} query db)
              actual   (d/q query db)]
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

(deftest test-fulltext-top-k-deduplicates-before-offset-and-limit
  (let [dir  (u/tmp-dir (str "query-access-fulltext-distinct-"
                             (UUID/randomUUID)))
        conn (d/create-conn
               dir
               {:tags {:db/valueType   :db.type/string
                       :db/cardinality :db.cardinality/many
                       :db/fulltext    true
                       :db.fulltext/autoDomain true}}
               {:search-domains {"tags" {}}})
        query
        '[:find ?e ?score
          :where
          [(fulltext $ :tags "red"
                     {:top 160 :display :refs+scores})
           [[?e _ _ ?score]]]
          :order-by [?score :desc ?e :asc]
          :offset 5
          :limit 10]]
    (try
      ;; Each entity contributes two physical search hits with the same term
      ;; frequency and document length. Their projected [e score] rows must be
      ;; deduplicated before the root offset and limit are applied.
      (d/transact!
        conn
        (mapv (fn [e]
                {:db/id e :tags ["red alpha" "red bravo"]})
              (range 1 81)))
      (binding [q/*cache?* false]
        (let [db       (d/db conn)
              raw      (mapv
                         (fn [^objects tuple]
                           [(aget tuple 0) (aget tuple 3)])
                         (built-ins/fulltext
                           db :tags "red"
                           {:top 160 :display :refs+scores}))
              expected (binding [qexec/*access-methods* []]
                         (d/q query db))
              explain  (d/explain {} query db)
              actual   (d/q query db)]
          (is (= 160 (count raw)))
          (is (= 80 (count (set raw))))
          (is (= expected actual))
          (is (= 10 (count actual)))
          (is (= 10 (count (set (map first actual)))))
          (is (true? (:access-path-selected? explain)))
          (is (= :adaptive-top-k
                 (get-in explain [:selected-plan-alternative :mode])))))
      (finally
        (d/close conn)
        (u/delete-files dir)))))

(deftest test-fulltext-multi-domain-cursor-preserves-concatenation
  (let [dir  (u/tmp-dir (str "query-access-fulltext-domains-"
                             (UUID/randomUUID)))
        conn (d/create-conn
               dir
               {:left  {:db/valueType :db.type/string
                        :db/fulltext true
                        :db.fulltext/autoDomain true}
                :right {:db/valueType :db.type/string
                        :db/fulltext true
                        :db.fulltext/autoDomain true}}
               {:search-domains {"left" {} "right" {}}})
        opts {:domains ["left" "right"]
              :top 3
              :display :refs+scores}
        form
        '[:find ?e ?a ?score
          :where
          [(fulltext $ "red"
                     {:domains ["left" "right"]
                      :top 3
                      :display :refs+scores})
           [[?e ?a _ ?score]]]]]
    (try
      (d/transact!
        conn
        (vec
          (concat
            (map (fn [e] {:db/id e :left (str "red left " e)})
                 (range 1 6))
            (map (fn [e] {:db/id e :right (str "red right " e)})
                 (range 11 16)))))
      (let [db       (d/db conn)
            parsed   (dp/parse-query form)
            method   (qfunction/access-method
                       {:fulltext qfulltext/access-method})
            demand   (qaccess/complete-demand :exact #{'?e '?a '?score})
            plan     (first
                       (qaccess/access-plans
                         [method]
                         {:parsed-q parsed :inputs [db] :demand demand}))
            expected (mapv
                       (fn [^objects tuple]
                         [(aget tuple 0) (aget tuple 1) (aget tuple 3)])
                       (built-ins/fulltext db "red" opts))
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

(deftest test-fulltext-access-preserves-text-and-offset-projection
  (let [dir  (u/tmp-dir (str "query-access-fulltext-text-offsets-"
                             (UUID/randomUUID)))
        conn (d/create-conn
               dir fulltext-schema
               {:search-domains
                {"text" {:include-text? true :index-position? true}}})
        query
        '[:find ?e ?raw-text ?offsets
          :where
          [(fulltext $ :text "red"
                     {:top 40 :display :texts+offsets})
           [[?e _ _ ?raw-text ?offsets]]]
          :limit 4]]
    (try
      (d/transact! conn (fulltext-docs 40))
      (binding [q/*cache?* false]
        (let [db       (d/db conn)
              expected (set
                         (map (fn [^objects tuple]
                                [(aget tuple 0)
                                 (aget tuple 3)
                                 (aget tuple 4)])
                              (built-ins/fulltext
                                db :text "red"
                                {:top 40 :display :texts+offsets})))
              explain  (d/explain {} query db)
              actual   (d/q query db)]
          (is (= 4 (count actual)))
          (is (every? expected actual))
          (is (every? (fn [[_ text offsets]]
                        (and (string? text) (some? offsets)))
                      actual))
          (is (true? (:access-path-selected? explain)))
          (is (= :adaptive-limit
                 (get-in explain [:selected-plan-alternative :mode])))))
      (finally
        (d/close conn)
        (u/delete-files dir)))))

(deftest test-correlated-fulltext-stays-on-conventional-path
  (let [dir  (u/tmp-dir (str "query-access-fulltext-correlated-"
                             (UUID/randomUUID)))
        conn (d/create-conn
               dir fulltext-schema {:search-domains {"text" {}}})
        query
        '[:find ?e
          :where
          [?seed :query ?query]
          [(fulltext $ :text ?query {:top 10}) [[?e _ _]]]]]
    (try
      (d/transact! conn [{:db/id 1 :query "red"}
                         {:db/id 2 :text "red document"}])
      (binding [q/*cache?* false]
        (let [explain (d/explain {} query (d/db conn))]
          (is (empty? (:access-plans explain)))
          (is (false? (:access-path-selected? explain)))))
      (finally
        (d/close conn)
        (u/delete-files dir)))))
