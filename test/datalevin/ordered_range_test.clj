(ns datalevin.ordered-range-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [datalevin.core :as d]
            [datalevin.db :as db]
            [datalevin.query :as q]
            [datalevin.query.access :as access]
            [datalevin.query.execute :as execute]
            [datalevin.query.execute.ordered-range :as ordered-range]
            [datalevin.query.cache :as cache]
            [datalevin.query.execute.point-lookup :as point-lookup]
            [datalevin.test.core :refer [db-fixture]]
            [datalevin.timeout :as timeout]
            [datalevin.util :as u]))

(use-fixtures :each db-fixture)

(defn- with-records [f]
  (let [dir (u/tmp-dir (str "ordered-range-" (random-uuid)))
        conn (d/get-conn dir
                        {:rank {:db/valueType :db.type/long}
                         :name {:db/valueType :db.type/string}
                         :keep {:db/valueType :db.type/boolean}}
                        {:cache-limit 0})]
    (try
      (d/transact! conn
                   (mapv (fn [n]
                           {:db/id (+ 1 (* 3 n)) :rank (* 2 n)
                            :name "same" :keep (zero? (mod n 3))})
                         (range 2500)))
      (binding [q/*cache?* false]
        (f conn (db/-clear-tx-cache @conn)))
      (finally (d/close conn) (u/delete-files dir)))))

(defn- conventional [query & inputs]
  (binding [execute/*access-methods* []]
    (apply d/q query inputs)))

(deftest ordered-range-page-test
  (with-records
    (fn [_ database]
      (doseq [[predicate direction]
              [['(>= ?rank ?start) :asc] ['(> ?rank ?start) :asc]
               ['(<= ?start ?rank) :asc] ['(< ?start ?rank) :asc]
               ['(<= ?rank ?start) :desc] ['(< ?rank ?start) :desc]
               ['(>= ?start ?rank) :desc] ['(> ?start ?rank) :desc]]
              offset [0 3]]
        (testing (str predicate " " direction " offset=" offset)
          (let [query {:find '[?e ?rank]
                       :in '[$ ?start]
                       :where [['?e :rank '?rank] [predicate]]
                       :order-by [['?rank direction]] :limit 5 :offset offset}
                explain (d/explain {:run? true} query database 2000)]
            (is (= (conventional query database 2000)
                   (d/q query database 2000)
                   ((d/prepare-q database query) [2000])))
            (is (true? (:access-path-selected? explain)))
            (is (= :ave (get-in explain [:plan :access-plan :method])))
            (is (<= (get-in explain [:plan :candidate-count])
                    (* 2 (+ offset 5))))
            (is (nil? (get-in explain [:plan :fallback])))))))))

(deftest ordered-range-pulls-only-selected-page-test
  (with-records
    (fn [_ database]
      (let [pulled (atom 0)
            pattern [[:name :xform (fn [value] (swap! pulled inc) value)]]
            query '[:find ?rank (pull ?e ?pattern)
                    :in $ ?start ?pattern
                    :where [?e :rank ?rank] [(>= ?rank ?start)]
                    :order-by ?rank :limit 5 :offset 2]
            reader (d/prepare-q database query)]
        (doseq [start [2000 100]]
          (reset! pulled 0)
          (let [actual (reader [start pattern])
                pull-count @pulled]
            (is (= (conventional query database start [:name]) actual))
            (is (= (count actual) pull-count))))
        (doseq [start [4998 6000]]
          (is (= (conventional query database start [:name])
                 (reader [start [:name]])))))
      (let [query '[:find ?rank (pull ?e [:name])
                    :in $ ?start
                    :where [?e :rank ?rank] [(>= ?rank ?start)]
                    :order-by ?rank :limit 5]
            explain (d/explain {:run? true} query database 2000)]
        (is (true? (:access-path-selected? explain)))
        (is (= 5 (get-in explain [:plan :candidate-count])))
        (is (= 5 (count (:result explain))))))))

(deftest ordered-range-ties-and-filtered-pull-test
  (with-records
    (fn [conn _]
      ;; A page boundary lies inside a tied value group. The secondary order
      ;; must see all ties; identical pulled maps must not collapse entities.
      (d/transact! conn (mapv #(hash-map :db/id (+ 10000 %) :rank 2000
                                       :name "same" :keep true)
                             (range 12)))
      (let [database (db/-clear-tx-cache @conn)
            tied '[:find ?e ?rank (pull ?e [:name])
                   :in $ ?start
                   :where [?e :rank ?rank] [(>= ?rank ?start)]
                   :order-by [?rank :asc ?e :desc] :limit 5 :offset 2]
            projected '[:find ?rank (pull ?e [:name])
                        :in $ ?start
                        :where [?e :rank ?rank] [(>= ?rank ?start)]
                        :order-by ?rank :limit 5]
            filtered '[:find ?e ?rank (pull ?e [:name])
                       :in $ ?start
                       :where [?e :rank ?rank] [(>= ?rank ?start)]
                              [?e :keep true]
                       :order-by [?rank :asc ?e :asc] :limit 5 :offset 2]]
        (doseq [query [tied projected filtered]]
          (is (= (conventional query database 2000)
                 (d/q query database 2000)
                 ((d/prepare-q database query) [2000]))))
        (is (= (vec (repeat 5 [2000 {:name "same"}]))
               (d/q projected database 2000)))
        (is (true? (:access-path-selected?
                     (d/explain {} filtered database 2000))))
        (testing "a pull pattern computed by where clauses stays conventional"
          (let [query '[:find ?rank ?pattern (pull ?e ?pattern)
                        :where [?e :rank ?rank] [(>= ?rank 2000)]
                               [(ground [:name]) ?pattern]
                        :order-by ?rank :limit 5]]
            (is (false? (:access-path-selected? (d/explain {} query database))))
            (is (= (conventional query database) (d/q query database)))))
        (testing "global aggregation and ordering by a pull stay conventional"
          (doseq [query ['[:find (count ?e) ?rank
                          :where [?e :rank ?rank] [(>= ?rank 2000)]
                          :order-by ?rank :limit 5]
                         '[:find (pull ?e [:name]) ?rank
                           :where [?e :rank ?rank] [(>= ?rank 2000)]
                           :order-by [?rank :asc ?e :asc] :limit 5]]]
            (is (false? (:access-path-selected? (d/explain {} query database))))))))))

(deftest dynamic-window-inputs-test
  (with-records
    (fn [_ database]
      (let [dynamic '{:find [?e ?rank]
                      :in [$ ?start ?limit]
                      :where [[?e :rank ?rank] [(>= ?rank ?start)]]
                      :order-by [?rank] :limit ?limit}
            reader (d/prepare-q database dynamic)
            dynamic+off '{:find [?e ?rank]
                          :in [$ ?start ?offset ?limit]
                          :where [[?e :rank ?rank] [(>= ?rank ?start)]]
                          :order-by [?rank] :offset ?offset :limit ?limit}
            reader+off (d/prepare-q database dynamic+off)]
        (doseq [start [0 2000 4998 6000]
                limit [1 5 17]]
          (let [literal (assoc dynamic :in '[$ ?start] :limit limit)]
            (is (= (d/q dynamic database start limit) (reader [start limit])))
            (is (= (d/q literal database start) (reader [start limit])))
            (is (= (conventional literal database start)
                   (reader [start limit])))))
        (doseq [offset [0 3 9]
                limit [1 5]]
          (is (= (d/q dynamic+off database 2000 offset limit)
                 (reader+off [2000 offset limit]))))
        (testing "the resolved window drives access planning"
          (let [explain (d/explain {:run? true} dynamic database 2000 4)]
            (is (true? (:access-path-selected? explain)))
            (is (= 4 (get-in explain [:plan :candidate-count])))
            (is (= 4 (count (:result explain))))))
        (testing "invalid dynamic values are rejected"
          (doseq [bad [0 -1 1.5 "3" nil [2]]]
            (is (thrown-with-msg? Exception
                                  #"Dynamic limit must be a positive integer"
                                  (reader [2000 bad]))))
          (is (thrown-with-msg? Exception
                                #"Dynamic offset must be a nonnegative integer"
                                (reader+off [2000 -1 5]))))
        (testing "the window variable must be a scalar :in binding"
          (let [unbound (assoc dynamic :in '[$ ?start])]
            (is (thrown-with-msg? Exception
                                  #"Dynamic \"limit\" variable must be bound in :in"
                                  (d/q unbound database 0)))))))))

(deftest dynamic-window-result-cache-test
  (let [dir (u/tmp-dir (str "ordered-window-cache-" (random-uuid)))
        conn (d/get-conn dir {:rank {:db/valueType :db.type/long}}
                         {:cache-limit 512})]
    (try
      (d/transact! conn (mapv (fn [n] {:rank n}) (range 1000)))
      (let [q '{:find [?e ?rank]
                :in [$ ?start ?limit]
                :where [[?e :rank ?rank] [(>= ?rank ?start)]]
                :order-by [?rank] :limit ?limit}]
        (is (= 3 (count (d/q q @conn 100 3))))
        (is (= 8 (count (d/q q @conn 100 8))))
        (is (= 3 (count (d/q q @conn 100 3))))
        (is (= 8 (count (d/q q @conn 100 8)))))
      (finally (d/close conn) (u/delete-files dir)))))

(deftest prepared-range-runtime-state-test
  (with-records
    (fn [conn database]
      (let [query '{:find [?rank (pull ?e ?pattern)]
                    :in [$ ?start ?offset ?limit ?pattern]
                    :where [[?e :rank ?rank] [(>= ?rank ?start)]]
                    :order-by [?rank] :offset ?offset :limit ?limit}
            inputs [2000 1 4 [:name :keep]]
            reader (d/prepare-q database query)
            parsed (cache/parsed-q query)
            executor (ordered-range/prepared-executor parsed)
            current #(apply d/q query @conn %)]
        (is (some? executor))
        (is (= (current inputs) (reader inputs)))
        (testing "a compiled access path accepts changing sources and inputs"
          (let [inputs' (into [database] inputs)]
            (is (= (current inputs)
                   (executor (execute/resolve-window parsed inputs') inputs')))))
        (d/transact! conn [[:db/add 3004 :name "changed"]])
        (is (= (current inputs) (reader inputs)))
        (d/update-schema conn {:name {:db/cardinality :db.cardinality/many}})
        (d/transact! conn [[:db/add 3004 :name "also"]])
        (is (= (current inputs) (reader inputs)))
        (testing "simulated transaction overlays use ordinary planning"
          (let [pending (:db-after (d/tx-data->simulated-report
                                    @conn [[:db/add 3004 :rank 1999]]))
                inputs' (into [pending] inputs)]
            (is (identical? point-lookup/unsupported
                            (executor (execute/resolve-window parsed inputs') inputs')))
            (is (= (apply d/q query pending inputs)
                   ((d/prepare-q pending query) inputs)))))
        (testing "transaction readers see writes and aborts"
          (d/with-transaction [tx conn]
            (let [read-tx (d/prepare-q @tx query)]
              (d/transact! tx [[:db/add 3004 :rank 1999]])
              (is (= (apply d/q query @tx inputs) (read-tx inputs)))
              (d/abort-transact tx)))
          (is (= (current inputs) (reader inputs))))
        (testing "access method bindings still take effect"
          (let [calls (atom 0)
                method (reify access/IAccessMethod
                         (-access-plans [_ _] (swap! calls inc) [])
                         (-open-access [_ _ _ _ _ _] nil)
                         (-frontier-satisfies? [_ _ _ _ _] false))]
            (binding [execute/*access-methods* (conj execute/*access-methods* method)]
              (is (= (current inputs) (reader inputs))))
            (is (pos? @calls))))
        (testing "a shared preparation retains no cursor or result state"
          (let [cases (mapv #(vector % 0 3 [:name]) [0 100 2000 4900])
                expected (mapv current cases)]
            (is (= expected (mapv deref (mapv #(future (reader %)) cases))))))
        (binding [timeout/*deadline* 1]
          (is (thrown-with-msg? Exception #"took too long" (reader inputs))))))))

(deftest prepared-range-field-projection-test
  (let [conn (d/create-conn nil
                           {:rank {:db/valueType :db.type/long}
                            :body {:db/valueType :db.type/string :db/noindex true}
                            :tag {:db/valueType :db.type/string :db/noindex true}}
                           {:wal? false :cache-limit 0})
        query '{:find [?rank ?tag ?body]
                :in [$ ?start ?offset ?limit]
                :where [[?e :tag ?tag] [?e :rank ?rank]
                        [(>= ?rank ?start)] [?e :body ?body]]
                :order-by [[?rank :asc ?tag :desc]]
                :offset ?offset :limit ?limit}]
    (try
      ;; EIDs, key order, attribute IDs and projection order all differ.
      ;; Missing fields remove rows before offset/limit; equal tuples dedupe.
      (d/transact! conn [{:db/id 90 :rank 0 :body "same"}
                         {:db/id 10 :rank 1 :body "same" :tag "a"}
                         {:db/id 60 :rank 1 :body "same" :tag "b"}
                         {:db/id 40 :rank 1 :body "same" :tag "b"}
                         {:db/id 20 :rank 2 :body "same" :tag "c"}
                         {:db/id 30 :rank 3 :body "last" :tag "d"}])
      (let [database (db/-clear-tx-cache @conn)
            reader (d/prepare-q database query)
            parsed (cache/parsed-q query)
            executor (ordered-range/prepared-executor parsed)]
        (is (some? executor))
        (is (= [[1 "b" "same"] [1 "a" "same"] [2 "c" "same"]]
               (reader [0 0 3])))
        (doseq [start [0 1 2 3 9] offset [0 1 2] limit [1 2 5]]
          (let [inputs [start offset limit]
                expected (apply conventional query database inputs)
                full-inputs (into [database] inputs)]
            (is (= expected (reader inputs)))
            (is (identical? point-lookup/unsupported
                            (executor (execute/resolve-window parsed full-inputs)
                                      full-inputs)))))
        (testing "descending ranges and strict starts"
          (let [reverse (assoc query :where '[[?e :tag ?tag] [?e :rank ?rank]
                                             [(< ?rank ?start)] [?e :body ?body]]
                                    :order-by '[[?rank :desc ?tag :asc]])
                reader (d/prepare-q database reverse)]
            (is (= (conventional reverse database 3 1 3) (reader [3 1 3])))))
        (testing "prepared projections see new values and schema changes"
          (d/transact! conn [[:db/add 10 :body "changed"]])
          (is (= (d/q query @conn 0 0 5) (reader [0 0 5])))
          (d/update-schema conn {:tag {:db/cardinality :db.cardinality/many}})
          (d/transact! conn [[:db/add 10 :tag "z"]])
          (let [inputs [0 0 5]
                full-inputs (into [(db/-clear-tx-cache @conn)] inputs)]
            (is (identical? point-lookup/unsupported
                            (executor (execute/resolve-window parsed full-inputs)
                                      full-inputs)))
            (is (= (apply conventional query @conn inputs) (reader inputs)))))
        (testing "transaction overlays retain bound-field semantics"
          (let [pending (:db-after (d/tx-data->simulated-report
                                    @conn [[:db/add 90 :tag "added"]]))]
            (is (= (conventional query pending 0 0 3)
                   ((d/prepare-q pending query) [0 0 3])))
            (is (= [0 "added" "same"]
                   (first ((d/prepare-q pending query) [0 0 3]))))))
        (testing "concurrent executions share no results or cursors"
          (let [inputs [[0 0 2] [1 1 3] [3 0 1]]]
            (is (= (mapv #(apply conventional query @conn %) inputs)
                   (mapv deref (mapv #(future (reader %)) inputs)))))))
      (finally (d/close conn)))))

(deftest prepared-unique-key-page-before-field-scan-test
  (let [conn (d/create-conn nil
                           {:rank {:db/valueType :db.type/long
                                   :db/unique :db.unique/value}
                            :body {:db/valueType :db.type/string :db/noindex true}
                            :tag {:db/valueType :db.type/string :db/noindex true}}
                           {:wal? false :cache-limit 0})
        query '{:find [?rank ?tag ?body]
                :in [$ ?start ?limit]
                :where [[?e :body ?body] [?e :rank ?rank]
                        [(>= ?rank ?start)] [?e :tag ?tag]]
                :order-by [?rank] :limit ?limit}]
    (try
      (d/transact! conn (mapv (fn [n]
                               {:db/id (inc (mod (* n 997) 3001))
                                :rank (* 2 n) :tag "same" :body (str n)})
                             (range 2100)))
      (is (= #{:rank} (set (map :a (d/datoms @conn :ave)))))
      (doseq [[predicate direction] [['(>= ?rank ?start) :asc]
                                     ['(> ?rank ?start) :asc]
                                     ['(<= ?rank ?start) :desc]
                                     ['(< ?rank ?start) :desc]]]
        (let [query (assoc query :where [['?e :body '?body] ['?e :rank '?rank]
                                         [predicate] ['?e :tag '?tag]]
                                 :order-by [['?rank direction]])
              parsed (cache/parsed-q query)
              executor (ordered-range/prepared-executor parsed)
              database (db/-clear-tx-cache @conn)
              reader (d/prepare-q database query)]
          (doseq [start [0 1 2100 5000] limit [1 7 1025]]
            (let [inputs [database start limit]
                  expected (conventional query database start limit)]
              (is (= expected (reader [start limit])))
              (is (= expected (executor (execute/resolve-window parsed inputs)
                                       inputs)))))))
      (testing "a missing field invalidates projection-only execution"
        (d/transact! conn [[:db/retract 1 :tag "same"]])
        (let [database (db/-clear-tx-cache @conn)
              parsed (cache/parsed-q query)
              executor (ordered-range/prepared-executor parsed)
              inputs [database 0 3]]
          (is (identical? point-lookup/unsupported
                          (executor (execute/resolve-window parsed inputs) inputs)))
          (is (= [[2 "same" "1"] [4 "same" "2"] [6 "same" "3"]]
                 ((d/prepare-q database query) [0 3])))))
      (finally (d/close conn)))))
