(ns datalevin.jepsen.workload.index-consistency
  (:require
   [datalevin.core :as d]
   [datalevin.jepsen.local :as local]
   [datalevin.jepsen.workload.util :as workload.util]
   [jepsen.checker :as checker]
   [jepsen.client :as client]
   [jepsen.generator :as gen]))

(def schema
  {:index/case  {:db/valueType :db.type/long}
   :index/key   {:db/valueType :db.type/string
                 :db/unique :db.unique/identity}
   :index/name  {:db/valueType :db.type/string}
   :index/value {:db/valueType :db.type/long}
   :index/ref   {:db/valueType :db.type/ref}
   :index/tags  {:db/valueType :db.type/string
                 :db/cardinality :db.cardinality/many}})

(defn- case-prefix
  [case-id]
  (format "index-%08d" (long case-id)))

(defn- root-key
  [case-id]
  (str (case-prefix case-id) "-root"))

(defn- child-a-key
  [case-id]
  (str (case-prefix case-id) "-child-a"))

(defn- child-b-key
  [case-id]
  (str (case-prefix case-id) "-child-b"))

(defn- root-name
  [case-id]
  (str "root-" (case-prefix case-id)))

(defn- child-a-name
  [case-id]
  (str "child-a-" (case-prefix case-id)))

(defn- child-b-name
  [case-id]
  (str "child-b-" (case-prefix case-id)))

(defn- tag-a
  [case-id]
  (str (case-prefix case-id) "-tag-a"))

(defn- tag-b
  [case-id]
  (str (case-prefix case-id) "-tag-b"))

(defn- tag-c
  [case-id]
  (str (case-prefix case-id) "-tag-c"))

(defn- normalized-root-view
  [{:keys [index/key index/name index/value index/ref index/tags] :as ent}]
  (if ent
    {:status  :present
     :key     key
     :name    name
     :value   value
     :ref-key (some-> ref :index/key)
     :tags    (into #{} tags)}
    {:status  :missing
     :key     nil
     :name    nil
     :value   nil
     :ref-key nil
     :tags    #{}}))

(defn- entity-view
  [db k]
  (normalized-root-view (d/entity db [:index/key k])))

(defn- pull-view
  [db k]
  (normalized-root-view
    (d/pull db
            '[:index/key
              :index/name
              :index/value
              :index/tags
              {:index/ref [:index/key]}]
            [:index/key k])))

(defn- query-view
  [db k]
  (if-let [[name value]
           (first (d/q '[:find ?name ?value
                         :in $ ?k
                         :where
                         [?e :index/key ?k]
                         [?e :index/name ?name]
                         [?e :index/value ?value]]
                       db
                       k))]
    {:status  :present
     :key     k
     :name    name
     :value   value
     :ref-key (d/q '[:find ?ref-key .
                     :in $ ?k
                     :where
                     [?e :index/key ?k]
                     [?e :index/ref ?ref]
                     [?ref :index/key ?ref-key]]
                   db
                   k)
     :tags    (into #{} (d/q '[:find [?tag ...]
                               :in $ ?k
                               :where
                               [?e :index/key ?k]
                               [?e :index/tags ?tag]]
                             db
                             k))}
    {:status  :missing
     :key     nil
     :name    nil
     :value   nil
     :ref-key nil
     :tags    #{}}))

(defn- eid->key
  [db eid]
  (:index/key (d/entity db eid)))

(defn- normalize-datom
  [db datom]
  [(:a datom)
   (if (= :index/ref (:a datom))
     (eid->key db (:v datom))
     (:v datom))])

(defn- entity-datoms
  [db k]
  (if (d/entity db [:index/key k])
    (into #{}
          (map #(normalize-datom db %))
          (d/datoms db :eav [:index/key k]))
    #{}))

(defn- ref-index-view
  [db case-id]
  (let [child-keys (->> [(child-a-key case-id) (child-b-key case-id)]
                        (filter #(d/entity db [:index/key %])))
        ave        (mapcat #(d/datoms db :ave :index/ref [:index/key %]) child-keys)
        range      (mapcat #(d/index-range db :index/ref
                                          [:index/key %]
                                          [:index/key %])
                           child-keys)
        norm       (fn [datoms]
                     (into #{}
                           (map (fn [datom]
                                  [(eid->key db (:e datom))
                                   (eid->key db (:v datom))]))
                           datoms))]
    {:ave   (norm ave)
     :range (norm range)}))

(defn- tag-index-view
  [db case-id]
  (let [tags    [(tag-a case-id) (tag-b case-id) (tag-c case-id)]
        ave     (mapcat #(d/datoms db :ave :index/tags %) tags)
        range   (d/index-range db :index/tags (first tags) (last tags))
        norm    (fn [datoms]
                  (into #{}
                        (map (fn [datom]
                               [(eid->key db (:e datom))
                                (:v datom)]))
                        datoms))]
    {:ave   (norm ave)
     :range (norm range)}))

(defn- case-snapshot
  [db case-id]
  {:entity-count (count (d/q '[:find [?e ...]
                               :in $ ?case-id
                               :where
                               [?e :index/case ?case-id]]
                             db
                             (long case-id)))
   :root         {:entity (entity-view db (root-key case-id))
                  :pull   (pull-view db (root-key case-id))
                  :query  (query-view db (root-key case-id))}
   :datoms       {:root    (entity-datoms db (root-key case-id))
                  :child-a (entity-datoms db (child-a-key case-id))
                  :child-b (entity-datoms db (child-b-key case-id))}
   :ref-index    (ref-index-view db case-id)
   :tag-index    (tag-index-view db case-id)})

(defn- root-state
  [case-id & {:keys [value ref-key tags]
              :or   {value nil
                     ref-key nil
                     tags #{}}}]
  {:status  :present
   :key     (root-key case-id)
   :name    (root-name case-id)
   :value   value
   :ref-key ref-key
   :tags    tags})

(defn- missing-root-state
  []
  {:status  :missing
   :key     nil
   :name    nil
   :value   nil
   :ref-key nil
   :tags    #{}})

(defn- datom-set
  [& xs]
  (into #{} xs))

(defn- entity-snapshot
  [case-id k name]
  (datom-set
    [:index/case (long case-id)]
    [:index/key k]
    [:index/name name]))

(defn- root-datoms
  [case-id value tags & {:keys [ref-key]}]
  (cond-> (datom-set
            [:index/case (long case-id)]
            [:index/key (root-key case-id)]
            [:index/name (root-name case-id)]
            [:index/value (long value)])
    (seq tags)
    (into (map (fn [tag]
                 [:index/tags tag])
               tags))

    ref-key
    (conj [:index/ref ref-key])))

(defn- expected-snapshot
  [case-id & {:keys [entity-count
                     root-view
                     root-datoms
                     child-a
                     child-b
                     ref-index
                     tag-index]
              :or   {entity-count 0
                     root-view (missing-root-state)
                     root-datoms #{}
                     child-a #{}
                     child-b #{}
                     ref-index #{}
                     tag-index #{}}}]
  {:entity-count entity-count
   :root         {:entity root-view
                  :pull root-view
                  :query root-view}
   :datoms       {:root root-datoms
                  :child-a child-a
                  :child-b child-b}
   :ref-index    {:ave ref-index
                  :range ref-index}
   :tag-index    {:ave tag-index
                  :range tag-index}})

(defn- case-txns
  [{:keys [f index/case-id] :as _op}]
  (let [case-id (long case-id)
        root    (root-key case-id)
        child-a (child-a-key case-id)
        child-b (child-b-key case-id)
        tag-a   (tag-a case-id)
        tag-b   (tag-b case-id)
        tag-c   (tag-c case-id)]
    (case f
      :ref-create
      [[{:db/id "root"
         :index/case case-id
         :index/key root
         :index/name (root-name case-id)
         :index/value 1
         :index/tags [tag-a tag-b]
         :index/ref "child-a"}
        {:db/id "child-a"
         :index/case case-id
         :index/key child-a
         :index/name (child-a-name case-id)}]]

      :ref-retarget
      [[{:db/id "root"
         :index/case case-id
         :index/key root
         :index/name (root-name case-id)
         :index/value 1
         :index/tags [tag-a tag-b]
         :index/ref "child-a"}
        {:db/id "child-a"
         :index/case case-id
         :index/key child-a
         :index/name (child-a-name case-id)}]
       [{:db/id "child-b"
         :index/case case-id
         :index/key child-b
         :index/name (child-b-name case-id)}
        [:db/retract [:index/key root] :index/tags tag-a]
        [:db/add [:index/key root] :index/tags tag-c]
        [:db/retract [:index/key root] :index/ref [:index/key child-a]]
        [:db/add [:index/key root] :index/ref "child-b"]
        [:db/cas [:index/key root] :index/value 1 2]]]

      :tag-swap
      [[{:db/id "root"
         :index/case case-id
         :index/key root
         :index/name (root-name case-id)
         :index/value 1
         :index/tags [tag-a tag-b]}]
       [[:db/retract [:index/key root] :index/tags tag-a]
        [:db/add [:index/key root] :index/tags tag-c]
        [:db/cas [:index/key root] :index/value 1 2]]]

      ::unsupported)))

(defn- expected-states
  [{:keys [f index/case-id] :as _op}]
  (let [case-id (long case-id)
        root    (root-key case-id)
        child-a (child-a-key case-id)
        child-b (child-b-key case-id)
        tag-a   (tag-a case-id)
        tag-b   (tag-b case-id)
        tag-c   (tag-c case-id)
        child-a-datoms (entity-snapshot case-id child-a (child-a-name case-id))
        child-b-datoms (entity-snapshot case-id child-b (child-b-name case-id))]
    (case f
      :ref-create
      [(expected-snapshot
         case-id
         :entity-count 2
         :root-view (root-state case-id
                                :value 1
                                :ref-key child-a
                                :tags #{tag-a tag-b})
         :root-datoms (root-datoms case-id
                                   1
                                   #{tag-a tag-b}
                                   :ref-key child-a)
         :child-a child-a-datoms
         :ref-index #{[root child-a]}
         :tag-index #{[root tag-a] [root tag-b]})]

      :ref-retarget
      [(expected-snapshot
         case-id
         :entity-count 2
         :root-view (root-state case-id
                                :value 1
                                :ref-key child-a
                                :tags #{tag-a tag-b})
         :root-datoms (root-datoms case-id
                                   1
                                   #{tag-a tag-b}
                                   :ref-key child-a)
         :child-a child-a-datoms
         :ref-index #{[root child-a]}
         :tag-index #{[root tag-a] [root tag-b]})
       (expected-snapshot
         case-id
         :entity-count 3
         :root-view (root-state case-id
                                :value 2
                                :ref-key child-b
                                :tags #{tag-b tag-c})
         :root-datoms (root-datoms case-id
                                   2
                                   #{tag-b tag-c}
                                   :ref-key child-b)
         :child-a child-a-datoms
         :child-b child-b-datoms
         :ref-index #{[root child-b]}
         :tag-index #{[root tag-b] [root tag-c]})]

      :tag-swap
      [(expected-snapshot
         case-id
         :entity-count 1
         :root-view (root-state case-id
                                :value 1
                                :tags #{tag-a tag-b})
         :root-datoms (root-datoms case-id 1 #{tag-a tag-b})
         :tag-index #{[root tag-a] [root tag-b]})
       (expected-snapshot
         case-id
         :entity-count 1
         :root-view (root-state case-id
                                :value 2
                                :tags #{tag-b tag-c})
         :root-datoms (root-datoms case-id 2 #{tag-b tag-c})
         :tag-index #{[root tag-b] [root tag-c]})]

      ::unsupported)))

(defn- expected-final-state
  [op]
  (last (expected-states op)))

(defn- probe-snapshot
  [db]
  (into {}
        (map (fn [case-id]
               (let [case-id (long case-id)]
                 [case-id (case-snapshot db case-id)])))
        (d/q '[:find [?case-id ...]
               :where
               [?e :index/case ?case-id]]
             db)))

(defn- execute-op!
  [conn op]
  (if (= :probe (:f op))
    (probe-snapshot @conn)
    (let [txns    (case-txns op)
          case-id (long (:index/case-id op))]
      (if (= ::unsupported txns)
        [:unsupported-client-op (:f op)]
        (mapv (fn [tx]
                (d/transact! conn tx)
                (case-snapshot @conn case-id))
              txns)))))

(defn- op-error
  [e]
  (if (= :transact/cas (:error (ex-data e)))
    :cas-failed
    (or (ex-message e)
        (.getName (class e)))))

(defn- build-op
  [case-id]
  (let [f (rand-nth [:ref-create
                     :ref-retarget
                     :tag-swap])]
    {:type           :invoke
     :f              f
     :value          nil
     :index/case-id  case-id}))

(defn- index-consistency-checker
  []
  (reify checker/Checker
    (check [_ test history _opts]
      (let [terminal-history (filter (comp #{:ok :fail :info} :type) history)
            probe-terminal
            (->> terminal-history
                 (filter (fn [{:keys [f]}]
                           (= :probe f)))
                 vec)
            successful-probes
            (->> probe-terminal
                 (filter (fn [{:keys [type value]}]
                           (and (= :ok type)
                                (map? value))))
                 (map :value)
                 vec)
            probe-failures
            (->> probe-terminal
                 (remove (fn [{:keys [type value]}]
                           (and (= :ok type)
                                (map? value))))
                 vec)
            final-probe (peek successful-probes)
            completed  (filter (comp some? :index/case-id) history)
            terminal   (filter (comp #{:ok :fail :info} :type) completed)
            disruption-failures
            (->> terminal
                 (filter (fn [{:keys [error]}]
                           (local/expected-disruption-write-failure?
                             test
                             error)))
                 vec)
            indeterminate
            (->> terminal
                 (remove (set disruption-failures))
                 (filter (comp #{:info} :type))
                 vec)
            checked-terminal
            (remove (set (concat disruption-failures indeterminate)) terminal)
            oks        (filter (comp #{:ok} :type) checked-terminal)
            failures   (filter (comp #{:fail} :type) checked-terminal)
            mismatches (->> checked-terminal
                            (keep (fn [op]
                                    (let [expected {:type  :ok
                                                    :value (expected-states op)}
                                          actual   (cond-> {:type (:type op)}
                                                     (= :ok (:type op))
                                                     (assoc :value (:value op))

                                                     (not= :ok (:type op))
                                                     (assoc :error (:error op)))]
                                      (when (not= expected actual)
                                        {:f        (:f op)
                                         :case-id  (:index/case-id op)
                                         :expected expected
                                         :actual   actual}))))
                            vec)
            probe-mismatches
            (if-not final-probe
              []
              (->> oks
                   (keep (fn [op]
                           (let [case-id  (:index/case-id op)
                                 expected (expected-final-state op)
                                 actual   (get final-probe case-id ::missing)]
                             (when (not= expected actual)
                               {:case-id  case-id
                                :expected expected
                                :actual   actual}))))
                   vec))
            probe-valid?
            (or (empty? probe-terminal)
                (and (seq successful-probes)
                     (empty? probe-failures)
                     (empty? probe-mismatches)))]
        {:valid?           (and (empty? mismatches)
                                probe-valid?
                                (pos? (+ (count oks)
                                         (count disruption-failures))))
         :ok-count         (count oks)
         :failure-count    (count failures)
         :indeterminate-count (count indeterminate)
         :indeterminate-samples
         (vec (take 10
                    (map #(select-keys % [:f :index/case-id :error])
                         indeterminate)))
         :disruption-failure-count (count disruption-failures)
         :disruption-failure-samples
         (vec (take 10
                    (map #(select-keys % [:f :index/case-id :error])
                         disruption-failures)))
         :mismatch-count   (count mismatches)
         :mismatch-samples (vec (take 10 mismatches))
         :probe-count (count successful-probes)
         :probe-failure-count (count probe-failures)
         :probe-failure-samples
         (vec (take 10
                    (map #(select-keys % [:type :error])
                         probe-failures)))
         :probe-mismatch-count (count probe-mismatches)
         :probe-mismatch-samples (vec (take 10 probe-mismatches))}))))

(defrecord Client [node]
  client/Client
  (open! [this _test node]
    (assoc this :node node))

  (setup! [this _test]
    this)

  (invoke! [_this test op]
    (try
      (local/with-leader-conn
        test
        schema
        (fn [conn]
          (let [value (execute-op! conn op)]
            (if (and (vector? value)
                     (= :unsupported-client-op (first value)))
              (assoc op
                     :type :fail
                     :error value)
              (assoc op
                     :type :ok
                     :value value)))))
      (catch Throwable e
        (workload.util/assoc-exception-op op e (op-error e)))))

  (teardown! [this _test]
    this)

  (close! [_this _test]
    nil))

(defn workload
  [_opts]
  (let [next-case-id (atom 0)]
    {:client    (->Client nil)
     :generator (->> (repeatedly #(build-op (swap! next-case-id inc)))
                     (gen/on-threads #{0}))
     :final-generator {:type :invoke :f :probe}
     :checker   (index-consistency-checker)
     :schema    schema}))
