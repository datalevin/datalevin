(ns datalevin.jepsen.workload.identity-upsert
  (:require
   [datalevin.core :as d]
   [datalevin.jepsen.local :as local]
   [datalevin.jepsen.workload.util :as workload.util]
   [jepsen.checker :as checker]
   [jepsen.client :as client]
   [jepsen.generator :as gen]))

(def schema
  {:identity/case  {:db/valueType :db.type/long}
   :identity/key   {:db/valueType :db.type/string
                    :db/unique :db.unique/identity}
   :identity/email {:db/valueType :db.type/string
                    :db/unique :db.unique/identity}
   :identity/value {:db/valueType :db.type/long}
   :identity/tag   {:db/valueType :db.type/string}
   :identity/ref   {:db/valueType :db.type/ref}})

(defn- main-key
  [case-id]
  (str "identity-" case-id))

(defn- child-key
  [case-id]
  (str "identity-child-" case-id))

(defn- main-email
  [case-id]
  (str "identity-" case-id "@example.com"))

(defn- entity-state
  [db k]
  (let [matches (->> (d/q '[:find [?e ...]
                            :in $ ?k
                            :where
                            [?e :identity/key ?k]]
                          db
                          k)
                     sort
                     vec)
        ent     (when-let [eid (first matches)]
                  (d/entity db eid))]
    {:status      (if ent :present :missing)
     :key         k
     :match-count (count matches)
     :value       (:identity/value ent)
     :email       (:identity/email ent)
     :tag         (:identity/tag ent)
     :ref-key     (some-> (:identity/ref ent) :identity/key)}))

(defn- case-state
  [db case-id]
  {:entity-count (count (d/q '[:find [?e ...]
                               :in $ ?case-id
                               :where
                               [?e :identity/case ?case-id]]
                             db
                             (long case-id)))
   :main         (entity-state db (main-key case-id))
   :child        (entity-state db (child-key case-id))})

(defn- case-txns
  [{:keys [f identity/case-id] :as _op}]
  (let [case-id (long case-id)
        k       (main-key case-id)
        ck      (child-key case-id)
        email   (main-email case-id)]
    (case f
      :upsert-same-tempid
      [[{:db/id -1
         :identity/case case-id
         :identity/key k
         :identity/value 1}
        {:db/id -1
         :identity/key k
         :identity/tag "same-tempid"}]]

      :upsert-two-tempids
      [[{:db/id -1
         :identity/case case-id
         :identity/key k
         :identity/value 1}
        {:db/id -2
         :identity/case case-id
         :identity/key k
         :identity/tag "two-tempids"}]]

      :upsert-intermediate
      [[{:identity/case case-id
         :identity/key k
         :identity/value 1}
        {:identity/case case-id
         :identity/key k
         :identity/value 2
         :identity/tag "intermediate"}]]

      :lookup-ref-intermediate
      [[[:db/add "entity" :identity/case case-id]
        [:db/add "entity" :identity/key k]
        [:db/add "entity" :identity/value 1]
        [:db/add "child" :identity/case case-id]
        [:db/add "child" :identity/key ck]
        [:db/add [:identity/key k] :identity/ref "child"]]]

      :string-tempid-upsert-ref
      [[{:db/id -1
         :identity/case case-id
         :identity/key k}
        {:db/id "user"
         :identity/case case-id
         :identity/key k}
        {:db/id -2
         :identity/case case-id
         :identity/key ck
         :identity/value 36
         :identity/tag "child"
         :identity/ref "user"}]]

      :lookup-ref-cas
      [[[:db/add "entity" :identity/case case-id]
        [:db/add "entity" :identity/key k]
        [:db/add "entity" :identity/value 0]]
       [[:db/cas [:identity/key k] :identity/value 0 1]
        [:db/cas [:identity/key k] :identity/value 1 2]]
       [[:db/add [:identity/key k] :identity/tag "cas-chain"]]]

      :dual-unique-upsert
      [[{:db/id -1
         :identity/case case-id
         :identity/key k
         :identity/email email
         :identity/value 1}
        {:db/id -2
         :identity/case case-id
         :identity/key k
         :identity/email email
         :identity/tag "dual-unique"}]]

      ::unsupported)))

(defn- expected-state
  [case-id & {:keys [entity-count main child]
              :or   {entity-count 1
                     child {:status      :missing
                            :key         (child-key case-id)
                            :match-count 0
                            :value       nil
                            :email       nil
                            :tag         nil
                            :ref-key     nil}}}]
  {:entity-count entity-count
   :main         (merge {:status      :present
                         :key         (main-key case-id)
                         :match-count 1
                         :value       nil
                         :email       nil
                         :tag         nil
                         :ref-key     nil}
                        main)
   :child        child})

(defn- expected-states
  [{:keys [f identity/case-id] :as _op}]
  (let [case-id (long case-id)
        email   (main-email case-id)]
    (case f
      :upsert-same-tempid
      [(expected-state case-id
                       :main {:value 1
                              :tag "same-tempid"})]

      :upsert-two-tempids
      [(expected-state case-id
                       :main {:value 1
                              :tag "two-tempids"})]

      :upsert-intermediate
      [(expected-state case-id
                       :main {:value 2
                              :tag "intermediate"})]

      :lookup-ref-intermediate
      [(expected-state case-id
                       :entity-count 2
                       :main {:value 1
                              :ref-key (child-key case-id)}
                       :child {:status      :present
                               :key         (child-key case-id)
                               :match-count 1
                               :value       nil
                               :email       nil
                               :tag         nil
                               :ref-key     nil})]

      :string-tempid-upsert-ref
      [(expected-state case-id
                       :entity-count 2
                       :main {}
                       :child {:status      :present
                               :key         (child-key case-id)
                               :match-count 1
                               :value       36
                               :email       nil
                               :tag         "child"
                               :ref-key     (main-key case-id)})]

      :lookup-ref-cas
      [(expected-state case-id
                       :main {:value 0})
       (expected-state case-id
                       :main {:value 2})
       (expected-state case-id
                       :main {:value 2
                              :tag "cas-chain"})]

      :dual-unique-upsert
      [(expected-state case-id
                       :main {:value 1
                              :email email
                              :tag "dual-unique"})]

      ::unsupported)))

(defn- expected-final-state
  [op]
  (last (expected-states op)))

(defn- probe-snapshot
  [db]
  (into {}
        (map (fn [case-id]
               (let [case-id (long case-id)]
                 [case-id (case-state db case-id)])))
        (d/q '[:find [?case-id ...]
               :where
               [?e :identity/case ?case-id]]
             db)))

(defn- execute-op!
  [conn op]
  (if (= :probe (:f op))
    (probe-snapshot @conn)
    (let [txns    (case-txns op)
          case-id (long (:identity/case-id op))]
        (if (= ::unsupported txns)
          [:unsupported-client-op (:f op)]
          (mapv (fn [tx]
                  (let [report (d/transact! conn tx)]
                    (case-state (workload.util/tx-report-db conn report)
                                case-id)))
              txns)))))

(defn- op-error
  [e]
  (if (= :transact/cas (:error (ex-data e)))
    :cas-failed
    (or (ex-message e)
        (.getName (class e)))))

(defn- build-op
  [case-id]
  (let [f (rand-nth [:upsert-same-tempid
                     :upsert-two-tempids
                     :upsert-intermediate
                     :lookup-ref-intermediate
                     :string-tempid-upsert-ref
                     :lookup-ref-cas
                     :dual-unique-upsert])]
    {:type             :invoke
     :f                f
     :value            nil
     :identity/case-id case-id}))

(defn- identity-upsert-checker
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
            completed  (filter (comp some? :identity/case-id) history)
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
                                         :case-id  (:identity/case-id op)
                                         :expected expected
                                         :actual   actual}))))
                            vec)
            probe-mismatches
            (if-not final-probe
              []
              (->> oks
                   (keep (fn [op]
                           (let [case-id  (:identity/case-id op)
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
                    (map #(select-keys % [:f :identity/case-id :error])
                         indeterminate)))
         :disruption-failure-count (count disruption-failures)
         :disruption-failure-samples
         (vec (take 10
                    (map #(select-keys % [:f :identity/case-id :error])
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
     :checker   (identity-upsert-checker)
     :schema    schema}))
