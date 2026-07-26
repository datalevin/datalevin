(ns datalevin.test.query
  (:require
   [datalevin.test.core :as tdc :refer [db-fixture]]
   [clojure.test :refer [deftest testing is use-fixtures]]
   [datalevin.core :as d]
   [datalevin.constants :as c]
   [datalevin.pipe :as pipe]
   [datalevin.query :as q]
   [datalevin.query.access :as qaccess]
   [datalevin.query.access.ave :as qave]
   [datalevin.query.cache :as qcache]
   [datalevin.query.execute :as qexec]
   [datalevin.query.plan :as qplan]
   [datalevin.query-optimizer :as qo]
   [datalevin.util :as u])
  (:import
   [clojure.lang ExceptionInfo]
   [java.util ArrayList Collection UUID]))

(use-fixtures :each db-fixture)

(defn access-source-ids
  [_db]
  [[1]])

(defn access-projected-tuples
  [x]
  [[x :ignored]])

(defn keep-access-id?
  [e]
  (even? e))

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

(deftest test-index-scans-decode-giant-value
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
      (is (= payload
             (d/q '[:find ?payload .
                    :where
                    [?e :payload ?payload]]
                  (d/db conn))))
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

(deftest test-explain-without-intermediate-counts
  (let [conn  (d/create-conn nil {} {:kv-opts {:inmemory? true}})
        query '[:find ?name
                :where
                [?e :person/name ?name]
                [?e :person/age ?age]
                [?e :person/active true]]
        plan-maps #(filter map? (tree-seq coll? seq (:plan %)))]
    (try
      (d/transact! conn [{:db/id 1 :person/name "Ada"
                          :person/age 30 :person/active true}
                         {:db/id 2 :person/name "Bea"
                          :person/age 40 :person/active true}
                         {:db/id 3 :person/name "Cid"
                          :person/age 50 :person/active false}])
      (let [db        (d/db conn)
            counted   (d/explain {:run? true} query db)
            uncounted (d/explain {:run? true :intermediate-counts? false}
                                 query db)]
        (is (= #{["Ada"] ["Bea"]} (:result counted) (:result uncounted)))
        (is (= 2 (:actual-result-size counted)
               (:actual-result-size uncounted)))
        (is (some #(contains? % :actual-size) (plan-maps counted)))
        (is (not-any? #(contains? % :actual-size) (plan-maps uncounted))))
      (finally
        (d/close conn)))))

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

(deftest test-ordered-limit-offset-pushdown-retains-work-reduction
  (let [dir          (u/tmp-dir
                       (str "query-top-k-work-" (UUID/randomUUID)))
        conn         (d/get-conn dir)
        pushed-calls (atom 0)
        normal-calls (atom 0)
        pushed-pred  (fn [_]
                       (swap! pushed-calls inc)
                       true)
        normal-pred  (fn [_]
                       (swap! normal-calls inc)
                       true)
        query        '[:find ?score
                       :in $ ?accept? ?max-score
                       :where
                       [?e :score ?score]
                       [(?accept? ?e)]
                       [(<= ?score ?max-score)]
                       :order-by [?score :desc]
                       :offset 100
                       :limit 3]]
    (try
      (d/transact! conn
                   (mapv (fn [e] {:db/id e :score e})
                         (range 1 2501)))
      (let [db       (d/db conn)
            pushed  (binding [q/*cache?* false]
                      (d/q query db pushed-pred 2500))
            normal   (binding [q/*cache?* false
                               qexec/*access-methods* []]
                       (d/q query db normal-pred 2500))]
        (is (= [[2400] [2399] [2398]] pushed normal))
        ;; A timer assertion would be noisy in CI. Predicate evaluations are a
        ;; deterministic proxy for the dominant candidate-processing work.
        (is (<= @pushed-calls c/init-exec-size-threshold))
        (is (= 2500 @normal-calls))
        (is (< @pushed-calls @normal-calls)))
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

(deftest test-access-planning-sample-is-hard-bounded-across-ties
  (let [dir   (u/tmp-dir (str "query-access-sample-bound-"
                              (UUID/randomUUID)))
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
      ;; Exact execution must account for the whole tie, but planning must not
      ;; turn a bounded sample into a scan of the entire tie.
      (d/transact! conn
                   (mapv (fn [e] {:db/id e :score 10 :id e})
                         (range 1 (+ 2 c/init-exec-size-threshold))))
      (let [preferred (:preferred-access-plan
                        (d/explain {} query (d/db conn) 10))]
        (is (<= (get-in preferred [:estimate :sample-rows])
                c/init-exec-size-threshold)))
      (finally
        (d/close conn)
        (u/delete-files dir)))))

(deftest test-ave-frontier-pages-large-ties-within-work-budget
  (let [dir  (u/tmp-dir (str "query-access-tie-budget-"
                             (UUID/randomUUID)))
        conn (d/get-conn dir)]
    (try
      (d/transact! conn
                   (mapv (fn [e] {:db/id e :score 10})
                         (range 1 1101)))
      (let [db     (d/db conn)
            path   (qave/ordered-path db :score :desc 10 '?score)
            demand (qaccess/top-k-demand
                     '[?score :desc ?id :asc] 0 3)
            work   (qaccess/->AccessWork nil 64 100 nil 0)
            cursor (qaccess/open-access path demand work db)]
        (try
          (let [batch1 (qaccess/next-batch cursor)
                batch2 (qaccess/next-batch cursor)
                batch3 (qaccess/next-batch cursor)
                cutoff {:primary-value 10}]
            (is (= 64 (count (:tuples batch1))))
            (is (= 36 (count (:tuples batch2))))
            (is (empty? (:tuples batch3)))
            (is (false? (:exhausted? batch3)))
            ;; No page certifies the primary-key boundary until every member
            ;; of the tie has been consumed.
            (is (false? (boolean
                          (qaccess/frontier-satisfies?
                            path demand (:frontier batch1) cutoff))))
            (is (false? (boolean
                          (qaccess/frontier-satisfies?
                            path demand (:frontier batch2) cutoff)))))
          (finally
            (qaccess/close-cursor cursor))))
      (finally
        (d/close conn)
        (u/delete-files dir)))))

(deftest test-ave-access-resumes-after-sampled-prefix
  (let [dir  (u/tmp-dir (str "query-access-resume-"
                             (UUID/randomUUID)))
        conn (d/get-conn dir)]
    (try
      (d/transact! conn
                   (mapv (fn [score] {:db/id score :score score})
                         (range 1 6)))
      (let [db      (d/db conn)
            path    (qave/ordered-path db :score :desc 5 '?score)
            demand  (qaccess/top-k-demand '[?score :desc] 0 2)
            sample-cursor
            (qaccess/open-access
              path demand (qaccess/->AccessWork 2 2 nil nil 0) db)
            sample  (try
                      (qaccess/next-batch sample-cursor)
                      (finally
                        (qaccess/close-cursor sample-cursor)))
            resumed-cursor
            (qaccess/open-access
              path demand
              (assoc (qaccess/->AccessWork nil 2 5 nil 0)
                     :resume (:frontier sample)
                     :emitted (count (:tuples sample)))
              db)]
        (try
          (is (= [[5 5] [4 4]]
                 (mapv vec (:tuples sample))))
          (is (= [[3 3] [2 2]]
                 (mapv vec
                       (:tuples (qaccess/next-batch resumed-cursor)))))
          (finally
            (qaccess/close-cursor resumed-cursor))))
      (finally
        (d/close conn)
        (u/delete-files dir)))))

(deftest test-access-policy-controls-planning-sampling-and-reuse
  (let [path (fn [policy]
               (qaccess/map->AccessPath {:policy policy}))]
    (is (true? (qaccess/planning-sample?
                 (path (qaccess/access-policy :restart :planning)))))
    (is (false? (qaccess/reusable-sample?
                  (path (qaccess/access-policy :restart :planning)))))
    (is (true? (qaccess/reusable-sample?
                 (path (qaccess/access-policy :resumable :planning)))))
    (is (false? (qaccess/planning-sample?
                  (path (qaccess/access-policy :resumable :execution)))))
    (is (false? (qaccess/planning-sample?
                  (path (qaccess/access-policy :none :planning)))))
    (is (thrown? ExceptionInfo
                 (qaccess/access-policy :unknown :planning)))
    (is (thrown? ExceptionInfo
                 (qaccess/access-policy :restart :unknown)))))

(deftest test-access-step-uses-explicit-named-source
  (let [left-dir  (u/tmp-dir (str "query-access-left-"
                                  (UUID/randomUUID)))
        right-dir (u/tmp-dir (str "query-access-right-"
                                  (UUID/randomUUID)))
        left-conn (d/get-conn left-dir)
        right-conn (d/get-conn right-dir)
        opened    (atom nil)
        query
        '[:find ?e ?name
          :in $left $right
          :where
          [(datalevin.test.query/access-source-ids $right) [[?e]]]
          [$right ?e :name ?name]]
        method
        (reify
          qaccess/IAccessMethod
          (-access-plans [this {:keys [parsed-q]}]
            (let [covered (first (:qwhere parsed-q))
                  expr
                  (qaccess/map->AccessExpr
                    {:method :named-source-test
                     :covers #{covered}
                     :covered-originals
                     #{(first (:qorig-where parsed-q))}
                     :requires #{}
                     :produces #{'?e}
                     :join-vars #{'?e}
                     :cols ['?e]
                     :source '$right})
                  path
                  (assoc
                    (qaccess/->AccessPath
                      :named-source-test this :complete nil #{:complete} {}
                      (qaccess/access-policy :none :execution))
                    :quality :exact)]
              [(qaccess/->AccessPlan
                 expr path (qaccess/source-bounds) (qaccess/access-work)
                 (assoc
                   (qaccess/->AccessEstimate 0.0 0.0 1 0.0 :medium)
                   :range-rows 1))]))

          (-open-access [_ _ _ _ _ source]
            (reset! opened source)
            (let [done? (atom false)]
              (reify
                qaccess/IAccessCursor
                (-next-batch [_]
                  (if (compare-and-set! done? false true)
                    (qaccess/->AccessBatch
                      (ArrayList. [(object-array [1])]) nil true)
                    (qaccess/->AccessBatch (ArrayList.) nil true)))
                (-close-cursor [_]))))

          (-frontier-satisfies? [_ _ _ _ _] false))]
    (try
      (d/transact! left-conn [{:db/id 1 :name "left"}])
      (d/transact! right-conn [{:db/id 1 :name "right"}])
      (let [left  (d/db left-conn)
            right (d/db right-conn)]
        (binding [qexec/*access-methods* [method]
                  q/*cache?*              false]
          (let [explain (d/explain {} query left right)]
            (is (= '$right
                   (get-in explain [:preferred-access-plan :source])))
            (is (some
                  #(= 1 (count (:operators %)))
                  (filter
                    #(= :access (:kind %))
                    (:physical-plan-alternatives explain)))))
          (is (= #{[1 "right"]}
                 (set (d/q query left right)))))
        (is (identical? right @opened)))
      (finally
        (d/close left-conn)
        (d/close right-conn)
        (u/delete-files left-dir)
        (u/delete-files right-dir)))))

(deftest test-function-binding-reuses-tuple-projection
  (let [query
        '[:find ?y
          :in [?x ...]
          :where
          [(datalevin.test.query/access-projected-tuples ?x) [[?y _]]]]]
    (is (= #{[1] [2] [3]}
           (set (d/q query [1 2 3]))))))

(deftest test-selected-access-reuses-planning-prefix
  (let [dir        (u/tmp-dir (str "query-access-prefix-root-"
                                   (UUID/randomUUID)))
        conn       (d/get-conn dir)
        opens      (atom [])
        total      1100
        candidates (vec
                     (map (fn [score] [score score])
                          (range total 0 -1)))
        query      '[:find ?score
                     :where
                     [?e :score ?score]
                     :order-by [?score :desc]
                     :limit 1001]
        method
        (reify
          qaccess/IAccessMethod
          (-access-plans [this {:keys [parsed-q demand]}]
            (let [covered (first (:qwhere parsed-q))
                  expr
                  (qaccess/map->AccessExpr
                    {:method :prefix-test
                     :covers #{covered}
                     :covered-originals
                     #{(first (:qorig-where parsed-q))}
                     :requires #{}
                     :produces #{'?e '?score}
                     :join-vars #{'?e}
                     :cols ['?e '?score]})
                  path
                  (assoc
                    (qaccess/->AccessPath
                      :prefix-test this :ordered
                      '[[?score :desc]]
                      #{:complete :ordered :resumable :monotone
                        :tie-complete :exact-frontier}
                      {}
                      (qaccess/access-policy :resumable :planning))
                    :quality :exact)
                  rows (:required-count demand)
                  estimate
                  (assoc
                    (qaccess/->AccessEstimate
                      0.0 0.0 rows 0.0 :medium)
                    :range-rows total)]
              [(qaccess/->AccessPlan
                 expr path (qaccess/source-bounds)
                 (qaccess/->AccessWork nil 1000 nil nil 0)
                 estimate)]))

          (-open-access [_ _ _ _ work _]
            (swap! opens conj work)
            (let [done?   (atom false)
                  start   (long
                            (or (get-in work
                                        [:resume :continuation :index])
                                0))
                  emitted (long (or (:emitted work) 0))
                  allowed (if-some [maximum (:max-candidates work)]
                            (max 0 (- (long maximum) emitted))
                            Long/MAX_VALUE)
                  n       (long
                            (min (long (or (:sample-size work)
                                           (:batch-size work)
                                           total))
                                 allowed
                                 (- total start)))
                  end     (+ start n)]
              (reify
                qaccess/IAccessCursor
                (-next-batch [_]
                  (if (compare-and-set! done? false true)
                    (let [tuples (ArrayList.
                                   (mapv (fn [[e score]]
                                           (object-array [e score]))
                                         (subvec candidates start end)))
                          score  (when (pos? n)
                                   (second (nth candidates (dec end))))]
                      (qaccess/->AccessBatch
                        tuples
                        (when score
                          (qaccess/->AccessFrontier
                            {:index end} score))
                        (= end total)))
                    (qaccess/->AccessBatch (ArrayList.) nil (= end total))))
                (-close-cursor [_]))))

          (-frontier-satisfies? [_ _ _ frontier cutoff]
            (not (pos? (compare (:certificate frontier)
                                (:primary-value cutoff))))))]
    (try
      (d/transact! conn
                   (mapv (fn [score]
                           {:db/id score :score score})
                         (range 1 (inc total))))
      (binding [qexec/*access-methods* [method]
                q/*cache?*              false]
        (let [result (d/q query (d/db conn))]
          (is (= 1001 (count result)))
          (is (= [1100] (first result)))
          (is (= [100] (last result)))))
      (is (= 2 (count @opens)))
      (is (nil? (:resume (first @opens))))
      (is (= 1000 (:emitted (second @opens))))
      (is (= 1000
             (get-in (second @opens)
                     [:resume :continuation :index])))
      (finally
        (d/close conn)
        (u/delete-files dir)))))

(deftest test-correlated-access-step-opens-once-per-outer-binding
  (let [opens  (atom [])
        method
        (reify
          qaccess/IAccessMethod
          (-access-plans [_ _] [])
          (-open-access [_ _ _ _ _ _]
            (throw (ex-info "Expected correlated open" {})))
          (-frontier-satisfies? [_ _ _ _ _] false)

          qaccess/ICorrelatedAccessMethod
          (-open-correlated-access [_ _ _ _ _ _ bindings]
            (swap! opens conj bindings)
            (let [done? (atom false)
                  x     (get bindings '?x)]
              (reify
                qaccess/IAccessCursor
                (-next-batch [_]
                  (if (compare-and-set! done? false true)
                    (qaccess/->AccessBatch
                      (ArrayList.
                        [(object-array [x (* 10 x)])])
                      nil true)
                    (qaccess/->AccessBatch (ArrayList.) nil true)))
                (-close-cursor [_])))))
        expr   (qaccess/map->AccessExpr
                 {:method    :correlated-test
                  :covers    #{:logical-access}
                  :requires  #{'?x}
                  :produces  #{'?x '?y}
                  :join-vars #{'?x}
                  :cols      ['?x '?y]})
        path   (assoc
                 (qaccess/->AccessPath
                   :correlated-test method :lookup nil
                   #{:complete} {} (qaccess/access-policy))
                 :quality :exact)
        demand (qaccess/->AccessDemand nil 0 nil nil :exact #{'?x '?y})
        step   (qplan/access-step
                 expr path demand (qaccess/access-work) ['?x])
        source (ArrayList.
                 [(object-array [1])
                  (object-array [2])])]
    (is (thrown-with-msg?
          ExceptionInfo #"requirements are not bound"
          (qplan/access-step expr path demand)))
    (is (= [[1 10] [2 20]]
           (mapv vec (qplan/step-execute step nil source))))
    (is (= [{'?x 1} {'?x 2}] @opens))))

(deftest test-correlated-access-is-scheduled-after-required-subset
  (let [dir   (u/tmp-dir (str "query-correlated-schedule-"
                              (UUID/randomUUID)))
        conn  (d/get-conn dir)
        query '[:find ?term ?doc
                :where
                [?q :raw ?raw]
                [(+ ?raw 0) ?term]
                [?doc :synthetic ?term]]
        parsed (qcache/parsed-q query)
        covered (nth (:qwhere parsed) 2)
        method
        (reify
          qaccess/IAccessMethod
          (-access-plans [_ _] [])
          (-open-access [_ _ _ _ _ _]
            (throw (ex-info "Expected correlated open" {})))
          (-frontier-satisfies? [_ _ _ _ _] false)

          qaccess/ICorrelatedAccessMethod
          (-open-correlated-access [_ _ _ _ _ _ bindings]
            (let [done? (atom false)
                  term  (get bindings '?term)
                  doc   ({1 101 2 202} term)]
              (reify
                qaccess/IAccessCursor
                (-next-batch [_]
                  (if (compare-and-set! done? false true)
                    (qaccess/->AccessBatch
                      (ArrayList. [(object-array [term doc])])
                      nil true)
                    (qaccess/->AccessBatch (ArrayList.) nil true)))
                (-close-cursor [_])))))
        expr (qaccess/map->AccessExpr
               {:method :correlated-test
                :covers #{covered}
                :covered-originals
                #{(nth (:qorig-where parsed) 2)}
                :requires #{'?term}
                :produces #{'?term '?doc}
                :join-vars #{'?term}
                :cols ['?term '?doc]})
        path (assoc
               (qaccess/->AccessPath
                 :correlated-test method :lookup nil #{:complete} {}
                 (qaccess/access-policy))
               :quality :exact)
        demand (qaccess/->AccessDemand
                 nil 0 nil nil :exact #{'?term '?doc})
        access-plan
        (assoc
          (qaccess/->AccessPlan
            expr path (qaccess/source-bounds) (qaccess/access-work)
            (qaccess/->AccessEstimate 1.0 1.0 2 3.0 :medium))
          :demand demand)]
    (try
      (d/transact! conn [{:db/id 1 :raw 1}
                         {:db/id 2 :raw 2}])
      (let [db        (d/db conn)
            scheduled (qo/schedule-correlated-access
                        db parsed [db] access-plan)
            outer-q   ((ns-resolve 'datalevin.query.execute
                                   'access-outer-query)
                       parsed scheduled)
            residual-q ((ns-resolve 'datalevin.query.execute
                                    'access-batch-query)
                        parsed (assoc scheduled :joins []) false)
            execute-q (ns-resolve 'datalevin.query.execute 'execute-query)
            outer     (execute-q outer-q [db])
            tuples    (qplan/step-execute
                        (:step scheduled) db outer)]
        (is (= #{'?term} (get-in scheduled [:expr :requires])))
        (is (= 2 (count (:outer-joins scheduled))))
        (is (qaccess/access-ready?
              (:expr scheduled) (:outer-cols scheduled)))
        (is (= #{[1 101] [2 202]}
               (set (execute-q residual-q [db tuples])))))
      (finally
        (d/close conn)
        (u/delete-files dir)))))

(deftest test-correlated-access-root-is-selected-and-executed
  (let [dir   (u/tmp-dir (str "query-correlated-root-"
                              (UUID/randomUUID)))
        conn  (d/get-conn dir)
        opens (atom 0)
        query '[:find ?term ?doc
                :where
                [?q :raw ?raw]
                [(+ ?raw 0) ?term]
                [?doc :synthetic ?term]]
        method
        (reify
          qaccess/IAccessMethod
          (-access-plans [this {:keys [parsed-q]}]
            (let [covered (nth (:qwhere parsed-q) 2)
                  expr
                  (qaccess/map->AccessExpr
                    {:method :correlated-test
                     :covers #{covered}
                     :covered-originals
                     #{(nth (:qorig-where parsed-q) 2)}
                     :requires #{'?term}
                     :produces #{'?term '?doc}
                     :join-vars #{'?term}
                     :cols ['?term '?doc]})
                  path
                  (assoc
                    (qaccess/->AccessPath
                      :correlated-test this :lookup nil #{:complete} {}
                      (qaccess/access-policy))
                    :quality :exact)]
              [(qaccess/->AccessPlan
                 expr path (qaccess/source-bounds) (qaccess/access-work)
                 (qaccess/->AccessEstimate 0.0 0.0 2 0.0 :medium))]))
          (-open-access [_ _ _ _ _ _]
            (throw (ex-info "Expected correlated open" {})))
          (-frontier-satisfies? [_ _ _ _ _] false)

          qaccess/ICorrelatedAccessMethod
          (-open-correlated-access [_ _ _ _ _ _ bindings]
            (swap! opens inc)
            (let [done? (atom false)
                  term  (get bindings '?term)
                  doc   ({1 101 2 202} term)]
              (reify
                qaccess/IAccessCursor
                (-next-batch [_]
                  (if (compare-and-set! done? false true)
                    (qaccess/->AccessBatch
                      (ArrayList. [(object-array [term doc])])
                      nil true)
                    (qaccess/->AccessBatch (ArrayList.) nil true)))
                (-close-cursor [_])))))]
    (try
      (d/transact! conn [{:db/id 1 :raw 1}
                         {:db/id 2 :raw 2}
                         {:db/id 101 :synthetic 1}
                         {:db/id 202 :synthetic 2}])
      (binding [qexec/*access-methods* [method]
                q/*cache?*              false]
        (is (= #{[1 101] [2 202]}
               (set (d/q query (d/db conn))))))
      (is (= 2 @opens))
      (finally
        (d/close conn)
        (u/delete-files dir)))))

(deftest test-complete-access-demand-keeps-source-bounds-and-skips-preparation
  (let [dir   (u/tmp-dir (str "query-complete-access-"
                              (UUID/randomUUID)))
        conn  (d/get-conn dir)
        opens (atom [])
        query '[:find ?e
                :where
                [?e :synthetic true]]
        method
        (reify
          qaccess/IAccessMethod
          (-access-plans [this {:keys [parsed-q]}]
            (let [covered (first (:qwhere parsed-q))
                  expr
                  (qaccess/map->AccessExpr
                    {:method :complete-test
                     :covers #{covered}
                     :covered-originals
                     #{(first (:qorig-where parsed-q))}
                     :requires #{}
                     :produces #{'?e}
                     :join-vars #{'?e}
                     :cols ['?e]})
                  path
                  (assoc
                    (qaccess/->AccessPath
                      :complete-test this :lookup nil #{:complete} {}
                      (qaccess/access-policy :none :execution))
                    :quality :exact)
                  estimate
                  (assoc
                    (qaccess/->AccessEstimate 0.0 0.0 2 0.0 :medium)
                    :range-rows 2)]
              [(qaccess/->AccessPlan
                 expr path (qaccess/source-bounds 0 2)
                 (qaccess/access-work) estimate)]))

          (-open-access [_ _ demand bounds work _]
            (swap! opens conj {:demand demand :bounds bounds :work work})
            (let [done? (atom false)]
              (reify
                qaccess/IAccessCursor
                (-next-batch [_]
                  (if (compare-and-set! done? false true)
                    (qaccess/->AccessBatch
                      (ArrayList.
                        [(object-array [1])
                         (object-array [2])])
                      nil true)
                    (qaccess/->AccessBatch (ArrayList.) nil true)))
                (-close-cursor [_]))))

          (-frontier-satisfies? [_ _ _ _ _] false))]
    (try
      (d/transact! conn [{:db/id 1 :synthetic true}
                         {:db/id 2 :synthetic true}])
      (binding [qexec/*access-methods* [method]
                q/*cache?*              false]
        (let [explain (d/explain {} query (d/db conn))
              plan    (first (:access-plans explain))]
          (is (nil? (:required-count plan)))
          (is (= 2 (:source-limit plan)))
          (is (= :none (get-in plan [:policy :sampling])))
          (is (empty? @opens)))
        (is (= #{[1] [2]} (d/q query (d/db conn)))))
      (is (= 1 (count @opens)))
      (is (nil? (get-in @opens [0 :demand :required-count])))
      (is (= 2 (get-in @opens [0 :bounds :limit])))
      (finally
        (d/close conn)
        (u/delete-files dir)))))

(deftest test-unordered-limit-adapts-after-residual-filtering
  (let [conn       (d/create-conn nil {} {:kv-opts {:inmemory? true}})
        opens      (atom [])
        inspected  (atom 0)
        candidates (vec (range 1 21))
        query
        '[:find ?e
          :where
          [?e :candidate true]
          [(datalevin.test.query/keep-access-id? ?e)]
          :offset 1
          :limit 3]
        exhaustion-query
        '[:find ?e
          :where
          [?e :candidate true]
          [(datalevin.test.query/keep-access-id? ?e)]
          :limit 12]
        method
        (reify
          qaccess/IAccessMethod
          (-access-plans [this {:keys [parsed-q]}]
            (let [covered (first (:qwhere parsed-q))
                  expr
                  (qaccess/map->AccessExpr
                    {:method :unordered-limit-test
                     :covers #{covered}
                     :covered-originals
                     #{(first (:qorig-where parsed-q))}
                     :requires #{}
                     :produces #{'?e}
                     :join-vars #{'?e}
                     :cols ['?e]})
                  path
                  (assoc
                    (qaccess/->AccessPath
                      :unordered-limit-test this :unordered-scan nil
                      #{:complete :resumable} {}
                      (qaccess/access-policy :none :execution))
                    :quality :exact)
                  estimate
                  (assoc
                    (qaccess/->AccessEstimate 0.0 0.0 20 0.0 :medium)
                    :range-rows 20
                    :scan-rows 20
                    :output-rows 20
                    :yield 0.5)]
              [(qaccess/->AccessPlan
                 expr path (qaccess/source-bounds)
                 (qaccess/access-work 2) estimate)]))

          (-open-access [_ _ demand bounds work _]
            (swap! opens conj {:demand demand :bounds bounds :work work})
            (let [position (volatile! 0)
                  closed?  (volatile! false)
                  maximum  (:max-candidates work)]
              (reify
                qaccess/IAccessCursor
                (-next-batch [_]
                  (if @closed?
                    (qaccess/->AccessBatch (ArrayList.) nil true)
                    (let [start     (long @position)
                          remaining (if (some? maximum)
                                      (max 0 (- (long maximum) start))
                                      (- (count candidates) start))
                          n         (long
                                      (min 2 remaining
                                           (- (count candidates) start)))
                          end       (+ start n)
                          tuples    (ArrayList.
                                      (mapv #(object-array [%])
                                            (subvec candidates start end)))
                          exhausted? (= end (count candidates))]
                      (vreset! position end)
                      (swap! inspected + n)
                      (qaccess/->AccessBatch
                        tuples
                        (when (pos? n)
                          (qaccess/->AccessFrontier
                            {:index end} nil))
                        exhausted?))))
                (-close-cursor [_]
                  (vreset! closed? true)))))

          (-frontier-satisfies? [_ _ _ _ _] false))]
    (try
      (d/transact! conn
                   (mapv (fn [e] {:db/id e :candidate true})
                         candidates))
      (binding [qexec/*access-methods* [method]
                q/*cache?*              false]
        (let [explain (d/explain {} query (d/db conn))
              result  (d/q query (d/db conn))]
          (is (= :adaptive-limit
                 (get-in explain [:selected-plan-alternative :mode])))
          (is (= 4
                 (get-in explain
                         [:selected-plan-alternative
                          :cost-breakdown :required-count])))
          (is (= 3 (count result)))
          (is (every? (comp even? first) result))))
      (is (= 1 (count @opens)))
      (is (= 4 (get-in @opens [0 :demand :required-count])))
      (is (= 0 (get-in @opens [0 :bounds :offset])))
      (is (nil? (get-in @opens [0 :bounds :limit])))
      (is (= 8 (get-in @opens [0 :work :max-candidates])))
      (is (= 8 @inspected))
      (reset! opens [])
      (reset! inspected 0)
      (binding [qexec/*access-methods* [method]
                q/*cache?*              false]
        (let [explain (d/explain {} exhaustion-query (d/db conn))
              result  (d/q exhaustion-query (d/db conn))]
          (is (= :adaptive-limit
                 (get-in explain [:selected-plan-alternative :mode])))
          (is (= 10 (count result)))))
      (is (= 20 @inspected))
      (finally
        (d/close conn)))))

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

(deftest test-ordered-limit-applies-offset-after-qualification
  (let [dir   (u/tmp-dir (str "query-top-k-offset-" (UUID/randomUUID)))
        conn  (d/get-conn dir)
        query '[:find ?score
                :in $ ?max-score
                :where
                [?e :score ?score]
                [?e :keep true]
                [(<= ?score ?max-score)]
                :order-by [?score :desc]
                :offset 1
                :limit 2]]
    (try
      (d/transact! conn
                   (mapv (fn [^long score]
                           (cond-> {:db/id score :score score}
                             (#{100 1100 2200} score) (assoc :keep true)))
                         (range 1 2501)))
      (binding [q/*cache?* false]
        (is (= [[1100] [100]]
               (d/q query (d/db conn) 2500))))
      (finally
        (d/close conn)
        (u/delete-files dir)))))

(deftest test-ordered-limit-access-plan-is-explained
  (let [dir   (u/tmp-dir (str "query-top-k-explain-" (UUID/randomUUID)))
        conn  (d/get-conn dir)
        query '[:find ?score
                :in $ ?max-score
                :where
                [?e :score ?score]
                [?e :keep true]
                [(<= ?score ?max-score)]
                :order-by [?score :desc]
                :offset 1
                :limit 2]]
    (try
      (d/transact! conn [{:db/id 1 :score 1 :keep true}])
      (let [explain   (d/explain {} query (d/db conn) 100)
            preferred (:preferred-access-plan explain)]
        (is (= 1 (count (:access-plans explain))))
        (is (= :ave (:method preferred)))
        (is (= :ordered-scan (:strategy preferred)))
        (is (= '[?score :desc] (:ordering preferred)))
        (is (= [['?score :desc]] (:path-ordering preferred)))
        (is (= 3 (:required-count preferred)))
        (is (= :exact (:quality preferred)))
        (is (contains? (:capabilities preferred) :exact-frontier))
        (is (= :sampled (get-in preferred [:estimate :confidence])))
        (is (= (:selected-plan-alternative explain)
               (:recommended-plan-alternative explain)))
        (is (= :not-run
               (get-in explain [:executed-plan-alternative :kind])))
        (is (= {:type :access
                :cols ['?e '?score]
                :out  #{'?e '?score}}
               (:step preferred))))
      (finally
        (d/close conn)
        (u/delete-files dir)))))

(deftest test-access-plan-orders-selective-bound-joins-first
  (let [dir   (u/tmp-dir (str "query-access-joins-" (UUID/randomUUID)))
        conn  (d/get-conn dir)
        query '[:find ?score
                :in $ ?max-score
                :where
                [?e :score ?score]
                [?e :common true]
                [?e :rare true]
                [(<= ?score ?max-score)]
                :order-by [?score :desc]
                :limit 1]]
    (try
      (d/transact! conn
                   (mapv (fn [^long score]
                           (cond-> {:db/id score
                                    :score score
                                    :common true}
                             (= score 1) (assoc :rare true)))
                         (range 1 1001)))
      (let [db        (d/db conn)
            explain   (d/explain {} query db 1000)
            preferred (:preferred-access-plan explain)]
        (is (= [:rare :common] (mapv :attr (:joins preferred))))
        (is (= 1000 (:candidate-budget preferred)))
        (is (= 1000 (get-in preferred [:estimate :sample-rows])))
        (is (= 1 (get-in preferred [:estimate :sample-output])))
        (is (= :sampled (get-in preferred [:estimate :confidence])))
        (is (false? (:access-path-selected? explain)))
        (is (= #{:conventional :access}
               (set (map :kind (:physical-plan-alternatives explain)))))
        ;; Source, each independently reachable join subset, and their union
        ;; are memoized separately.
        (is (<= 4 (count (:physical-plan-subsets explain))))
        (let [access-alternatives
              (filter #(= :access (:kind %))
                      (:physical-plan-alternatives explain))
              operator-counts (set (map (comp count :operators)
                                        access-alternatives))]
          ;; Retained source-only, partial, and complete fragments are all
          ;; connected to executable roots. Each root exposes the schema
          ;; produced by its selected physical fragment.
          (is (every? operator-counts #{0 1 2}))
          (is (every? seq (map :fragment-cols access-alternatives))))
        (is (= :conventional
               (get-in explain [:selected-plan-alternative :kind])))
        (let [discover (ns-resolve 'datalevin.query.execute
                                   'discover-access-plans)
              plan     (ns-resolve 'datalevin.query.execute
                                   'access-query-plan)
              parsed-q (qcache/parsed-q query)
              plans    (discover parsed-q [db 1000])
              context  (plan parsed-q [db 1000] plans)
              selected (qo/selected-alternative context)
              access   (first
                         (filter #(= :access (:kind %))
                                 (get-in context
                                         [:property-memo :alternatives])))]
          (is (instance? datalevin.query.plan.ConventionalRootPlan
                         (:plan selected)))
          (is (instance? datalevin.query.plan.Context
                         (get-in selected [:plan :context])))
          ;; Adaptive fallback and conventional selection share the same
          ;; already-planned conventional root.
          (is (identical? (:plan selected)
                          (get-in access [:plan :fallback]))))
        (binding [q/*cache?* false]
          (is (= [[1]] (d/q query db 1000)))))
      (finally
        (d/close conn)
        (u/delete-files dir)))))

(deftest test-access-step-samples-leading-prefix
  (let [dir  (u/tmp-dir (str "query-access-step-" (UUID/randomUUID)))
        conn (d/get-conn dir)]
    (try
      (d/transact! conn
                   (mapv (fn [^long score]
                           {:db/id score :score score})
                         (range 1 6)))
      (let [db     (d/db conn)
            expr   (qaccess/map->AccessExpr
                     {:method     :ave
                      :requires   #{}
                      :produces   #{'?e '?score}
                      :join-vars  #{'?e}
                      :cols       ['?e '?score]})
            path   (qave/ordered-path db :score :desc 5)
            demand (qaccess/top-k-demand '[?score :desc] 0 2)
            work   (qaccess/->AccessWork 2 2 nil nil 0)
            step   (qplan/access-step expr path demand work)]
        (is (= :access (qplan/step-type step)))
        (is (= [[5 5] [4 4]]
               (mapv vec (qplan/step-sample step db nil))))
        ;; Sampling reads a bounded prefix. Normal step execution implements
        ;; the complete logical access expression.
        (is (= [[5 5] [4 4] [3 3] [2 2] [1 1]]
               (mapv vec (qplan/step-execute step db nil))))
        (let [relation (qplan/execute-steps nil db [step])]
          (is (= {'?e 0 '?score 1} (:attrs relation)))
          (is (= [[5 5] [4 4] [3 3] [2 2] [1 1]]
                 (mapv vec (:tuples relation)))))
        (is (re-find #":ave/:ordered-scan"
                     (qplan/step-explain step nil))))
      (finally
        (d/close conn)
        (u/delete-files dir)))))

(deftest test-incomplete-access-alternative-requires-top-k-proof
  (let [satisfies? (ns-resolve 'datalevin.query-optimizer
                               'alternative-satisfies?)
        demand     (qaccess/top-k-demand '[?score :desc] 0 1)
        alternative
        (fn [capabilities]
          (qplan/->PlanAlternative
            :access
            :query-root
            (qplan/->PhysicalProperties
              '[?score :desc] true false :exact capabilities)
            nil 1.0 1 nil))]
    (is (false? (satisfies? (alternative #{:ordered}) demand)))
    (is (true?
          (satisfies?
            (alternative #{:ordered :resumable :monotone :tie-complete
                           :exact-frontier})
            demand)))))

(deftest test-property-frontier-retains-subset-variants
  (let [logical-key #{:access :join}
        properties
        (fn [ordering]
          (qplan/->PhysicalProperties
            ordering true true :exact
            #{:complete :resumable}))
        alternative
        (fn [ordering cost]
          (qplan/->PlanAlternative
            :access-fragment logical-key (properties ordering)
            {:ordering ordering} cost 10 nil))
        unordered (alternative nil 5.0)
        ordered-7 (alternative '[[?score :desc]] 7.0)
        ordered-6 (alternative '[[?score :desc]] 6.0)
        frontier (-> []
                     (qo/retain-property-alternative unordered)
                     (qo/retain-property-alternative ordered-7)
                     (qo/retain-property-alternative ordered-6))
        ordered-properties (properties '[[?score :desc]])
        hash-properties
        (qo/propagate-physical-properties
          ordered-properties {:type :hash-join})]
    (is (= 2 (count frontier)))
    (is (= #{5.0 6.0} (set (map :cost frontier))))
    (is (some #(seq (get-in % [:properties :ordering])) frontier))
    (is (some #(empty? (get-in % [:properties :ordering])) frontier))
    (is (= ordered-properties
           (qo/propagate-physical-properties
             ordered-properties
             {:type :index-join :preserves-outer-order? true})))
    (is (nil? (:ordering hash-properties)))
    (is (false? (:resumable? hash-properties)))
    (is (not (contains? (:capabilities hash-properties) :resumable)))))

(deftest test-access-path-selected-when-prefix-is-cheaper
  (let [dir   (u/tmp-dir (str "query-access-selected-" (UUID/randomUUID)))
        conn  (d/get-conn dir)
        query '[:find ?score
                :in $ ?max-score
                :where
                [?e :score ?score]
                [(<= ?score ?max-score)]
                :order-by [?score :desc]
                :limit 1]]
    (try
      (d/transact! conn
                   (mapv (fn [^long score]
                           {:db/id score :score score})
                         (range 1 101)))
      (let [db          (d/db conn)
            explain     (d/explain {} query db 100)
            run-explain (d/explain {:run? true} query db 100)]
        (is (true? (:access-path-selected? explain)))
        (is (< (get-in explain [:preferred-access-plan :estimate :cost])
               (:conventional-plan-cost explain)))
        (is (= :access
               (get-in explain [:selected-plan-alternative :kind])))
        (is (pos? (get-in explain
                          [:preferred-access-plan :sample-prefix-size])))
        (is (true? (get-in explain
                           [:preferred-access-plan :sample-resumable?])))
        (is (= (get-in explain
                       [:preferred-access-plan :estimate :sample-rows])
               (get-in explain
                       [:preferred-access-plan
                        :estimate :reused-candidates])))
        (is (seq (:physical-plan-subsets explain)))
        (is (every? seq (vals (:physical-plan-subsets explain))))
        (is (= :adaptive-top-k
               (get-in explain [:selected-plan-alternative :mode])))
        (is (map?
              (get-in explain
                      [:selected-plan-alternative :cost-breakdown])))
        (is (number?
              (get-in explain
                      [:selected-plan-alternative
                       :cost-breakdown :enforcer])))
        (is (= :access
               (get-in explain [:recommended-plan-alternative :kind])))
        (is (= :not-run
               (get-in explain [:executed-plan-alternative :kind])))
        (is (= :access
               (get-in run-explain [:recommended-plan-alternative :kind])))
        (is (= :conventional
               (get-in run-explain [:executed-plan-alternative :kind])))
        (is (= '[?score :desc]
               (get-in explain
                       [:selected-plan-alternative :properties :ordering])))
        ;; The retained access fragment remains resumable, while the executable
        ;; root has consumed it and materialized a complete result.
        (is (true?
              (get-in explain
                      [:selected-plan-alternative
                       :fragment-properties :resumable?])))
        (is (false?
              (get-in explain
                      [:selected-plan-alternative :properties :resumable?])))
        (binding [q/*cache?* false]
          (is (= [[100]] (d/q query db 100)))))
      (finally
        (d/close conn)
        (u/delete-files dir)))))

(deftest test-conventional-alternative-costs-late-functions
  (let [dir   (u/tmp-dir (str "query-access-late-function-cost-"
                              (UUID/randomUUID)))
        conn  (d/get-conn dir)
        query '[:find ?score ?copy
                :in $ ?max-score
                :where
                [?e :score ?score]
                [(identity ?score) ?copy]
                [(<= ?score ?max-score)]
                :order-by [?score :desc]
                :limit 1]]
    (try
      (d/transact! conn
                   (mapv (fn [^long score]
                           {:db/id score :score score})
                         (range 1 101)))
      (let [explain
            (d/explain {} query (d/db conn) 100)
            conventional
            (first
              (filter #(= :conventional (:kind %))
                      (:physical-plan-alternatives explain)))
            {:keys [late late-stages]} (:cost-breakdown conventional)
            function-stage
            (first (filter #(= :function (:operation %)) late-stages))]
        (is (some? function-stage))
        (is (= (:size conventional) (:input function-stage)))
        (is (= (long (* (double (:input function-stage))
                        (double c/magic-cost-pred)))
               (:cost function-stage)))
        (is (== (double (reduce + 0 (map :cost late-stages)))
                (double late)))
        (is (==
              (+ (double (get-in conventional [:cost-breakdown :base]))
                 (double late)
                 (double
                   (get-in conventional [:cost-breakdown :enforcer])))
              (double (:cost conventional)))))
      (finally
        (d/close conn)
        (u/delete-files dir)))))

(deftest test-access-fallback-work-is-bounded-by-conventional-plan
  (let [dir          (u/tmp-dir (str "query-access-work-budget-"
                                     (UUID/randomUUID)))
        conn         (d/get-conn dir)
        row-count    32769
        probe-budget 1024
        calls        (atom 0)
        accept?      (fn [e]
                       (swap! calls inc)
                       (= e 1))
        query        '[:find ?score
                       :in $ ?accept? ?max-score
                       :where
                       [?e :score ?score]
                       [(?accept? ?e)]
                       [(<= ?score ?max-score)]
                       :order-by [?score :desc]
                       :limit 1]]
    (try
      (d/transact! conn
                   (mapv (fn [score] {:db/id score :score score})
                         (range 1 (inc row-count))))
      (binding [q/*cache?* false]
        (is (= [[1]]
               (d/q query (d/db conn) accept? row-count))))
      ;; A low-confidence access attempt may spend one bounded probe before
      ;; choosing the conventional plan, but it must not scan 32 batches and
      ;; then repeat the complete conventional scan.
      (is (<= @calls (+ row-count probe-budget)))
      (finally
        (d/close conn)
        (u/delete-files dir)))))

(deftest test-access-step-zero-yield-uses-existing-scan-adjustment
  (let [dir   (u/tmp-dir (str "query-access-zero-yield-"
                              (UUID/randomUUID)))
        conn  (d/get-conn dir)
        query '[:find ?score
                :in $ ?max-score
                :where
                [?e :score ?score]
                [?e :keep true]
                [(<= ?score ?max-score)]
                :order-by [?score :desc]
                :limit 1]]
    (try
      (d/transact! conn
                   (mapv (fn [^long score]
                           {:db/id score :score score})
                         (range 1 2001)))
      (let [preferred (:preferred-access-plan
                        (d/explain {} query (d/db conn) 2000))]
        (is (= 1000 (get-in preferred [:estimate :sample-rows])))
        (is (zero? (get-in preferred [:estimate :sample-output])))
        (is (= c/magic-scan-ratio
               (get-in preferred [:estimate :yield])))
        (is (= 1000 (:candidate-budget preferred))))
      (finally
        (d/close conn)
        (u/delete-files dir)))))

(deftest test-access-estimate-separates-scan-work-from-output-rows
  (let [dir   (u/tmp-dir (str "query-access-scan-output-"
                              (UUID/randomUUID)))
        conn  (d/get-conn dir)
        query '[:find ?e ?name
                :where
                [(datalevin.test.query/access-source-ids $) [[?e]]]
                [?e :name ?name]]
        method
        (reify
          qaccess/IAccessMethod
          (-access-plans [this {:keys [parsed-q]}]
            (let [covered (first (:qwhere parsed-q))
                  expr
                  (qaccess/map->AccessExpr
                    {:method :scan-output-test
                     :covers #{covered}
                     :covered-originals
                     #{(first (:qorig-where parsed-q))}
                     :requires #{}
                     :produces #{'?e}
                     :join-vars #{'?e}
                     :cols ['?e]
                     :source '$})
                  path
                  (assoc
                    (qaccess/->AccessPath
                      :scan-output-test this :complete nil #{:complete} {}
                      (qaccess/access-policy :none :execution))
                    :quality :exact)
                  estimate
                  (assoc
                    (qaccess/->AccessEstimate
                      2.0 3.0 100 302.0 :low)
                    :range-rows 100
                    :scan-rows 100
                    :output-rows 4
                    :yield 1.0)]
              [(qaccess/->AccessPlan
                 expr path (qaccess/source-bounds)
                 (qaccess/access-work)
                 estimate)]))

          (-open-access [_ _ _ _ _ _]
            (throw (ex-info "Explain should not execute access" {})))

          (-frontier-satisfies? [_ _ _ _ _] false))]
    (try
      (d/transact! conn [{:db/id 1 :name "Ada"}])
      (binding [qexec/*access-methods* [method]
                q/*cache?*              false]
        (let [explain   (d/explain {} query (d/db conn))
              preferred (:preferred-access-plan explain)
              source
              (first
                (for [[logical-key alternatives]
                      (:physical-plan-subsets explain)
                      :when (= 1 (count logical-key))
                      alternative alternatives
                      :when (= :access-fragment (:kind alternative))]
                  alternative))]
          (is (= 100 (get-in preferred [:estimate :scan-rows])))
          (is (= 4 (get-in preferred [:estimate :output-rows])))
          (is (= 4 (get-in preferred [:estimate :rows])))
          (is (= 302.0 (get-in preferred [:estimate :upper-cost])))
          (is (= 4 (:size source)))
          (is (= 302.0 (:cost source)))))
      (finally
        (d/close conn)
        (u/delete-files dir)))))

(deftest test-ordinary-query-skips-access-join-planning
  (let [conn (d/create-conn nil {} {:kv-opts {:inmemory? true}})]
    (try
      (d/transact! conn [{:db/id 1 :name "Ada"}])
      (with-redefs [qo/plan-access-joins
                    (fn [& _]
                      (throw (ex-info "Access join planner should be skipped"
                                      {})))
                    qo/build-property-memo
                    (fn [& _]
                      (throw (ex-info "Property memo should be skipped"
                                      {})))]
        (binding [q/*cache?* false]
          (is (= #{["Ada"]}
                 (d/q '[:find ?name
                        :where
                        [?e :name ?name]]
                      (d/db conn))))))
      (finally
        (d/close conn)))))

(deftest test-ordered-limit-falls-back-after-candidate-batch-cap
  (let [dir       (u/tmp-dir (str "query-top-k-fallback-" (UUID/randomUUID)))
        conn      (d/get-conn dir)
        max-score 32769
        query     '[:find ?score
                    :in $ ?max-score
                    :where
                    [?e :score ?score]
                    [?e :keep true]
                    [(<= ?score ?max-score)]
                    :order-by [?score :desc]
                    :limit 1]]
    (try
      (d/transact! conn
                   (mapv (fn [^long score]
                           (cond-> {:db/id score :score score}
                             (= score 1) (assoc :keep true)))
                         (range 1 (inc max-score))))
      (binding [q/*cache?* false]
        (is (= [[1]]
               (d/q query (d/db conn) max-score))))
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
      (is (= (d/q '[:find [?id ...]
                    :in $ [?id ...]
                    :where [?id :age _]]
                  db
                  nil)
             []))
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
