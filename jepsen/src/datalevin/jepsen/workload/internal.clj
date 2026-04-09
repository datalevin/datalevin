(ns datalevin.jepsen.workload.internal
  (:require
   [datalevin.core :as d]
   [datalevin.interpret :as i]
   [datalevin.jepsen.local :as local]
   [datalevin.jepsen.workload.util :as workload.util]
   [jepsen.checker :as checker]
   [jepsen.client :as client]
   [jepsen.generator :as gen]))

(def schema
  {:internal/key   {:db/valueType :db.type/string
                    :db/unique :db.unique/identity}
   :internal/value {:db/valueType :db.type/long}
   :internal/ref   {:db/valueType :db.type/ref}})

(def ^:private increment-value
  (i/inter-fn [db k]
    (if-some [ent (d/entity db [:internal/key k])]
      [{:db/id          (:db/id ent)
        :internal/value (inc (long (or (:internal/value ent) 0)))}]
      [])))

(def ^:private tx-fn-entities
  [{:db/ident :internal/increment
    :db/fn    increment-value}])

(defn- ensure-tx-fns!
  [conn]
  (let [db      @conn
        idents  (mapv :db/ident tx-fn-entities)
        present (set (d/q '[:find [?ident ...]
                            :in $ [?ident ...]
                            :where
                            [?e :db/ident ?ident]
                            [?e :db/fn ?fn]]
                          db
                          idents))
        missing (->> tx-fn-entities
                     (remove (comp present :db/ident))
                     vec)]
    (when (seq missing)
      (d/transact! conn missing))))

(defn- main-key
  [case-id]
  (str "internal-" case-id))

(defn- child-key
  [case-id]
  (str "internal-child-" case-id))

(defn- state-for-key
  [db k]
  (if-some [ent (d/entity db [:internal/key k])]
    {:status  :present
     :key     k
     :value   (:internal/value ent)
     :ref-key (some-> (:internal/ref ent) :internal/key)}
    {:status  :missing
     :key     k
     :value   nil
     :ref-key nil}))

(defn- case-txns
  [{:keys [f internal/case-id] :as _op}]
  (let [k  (main-key case-id)
        ck (child-key case-id)]
    (case f
      :lookup-ref-same
      [[[:db/add "entity" :internal/key k]
        [:db/add [:internal/key k] :internal/value 1]]]

      :tx-fn-after-add
      [[[:db/add "entity" :internal/key k]
        [:db/add "entity" :internal/value 0]
        [:internal/increment k]]]

      :tx-fn-twice
      [[[:db/add "entity" :internal/key k]
        [:db/add "entity" :internal/value 0]
        [:internal/increment k]
        [:internal/increment k]]]

      :cas-chain
      [[[:db/add "entity" :internal/key k]
        [:db/add "entity" :internal/value 0]]
       [[:db/cas [:internal/key k] :internal/value 0 1]
        [:db/cas [:internal/key k] :internal/value 1 2]]]

      :retract-add
      [[[:db/add "entity" :internal/key k]
        [:db/add "entity" :internal/value 0]
        [:db/retract [:internal/key k] :internal/value 0]
        [:db/add [:internal/key k] :internal/value 1]]]

      :tempid-ref
      [[[:db/add "entity" :internal/key k]
        [:db/add "entity" :internal/value 0]
        [:db/add "child" :internal/key ck]
        [:db/add "entity" :internal/ref "child"]]]

      ::unsupported)))

(defn- expected-states
  [{:keys [f internal/case-id] :as _op}]
  (let [k  (main-key case-id)
        ck (child-key case-id)]
    (case f
      :lookup-ref-same
      [{:status :present :key k :value 1 :ref-key nil}]

      :tx-fn-after-add
      [{:status :present :key k :value 1 :ref-key nil}]

      :tx-fn-twice
      [{:status :present :key k :value 1 :ref-key nil}]

      :cas-chain
      [{:status :present :key k :value 0 :ref-key nil}
       {:status :present :key k :value 2 :ref-key nil}]

      :retract-add
      [{:status :present :key k :value 1 :ref-key nil}]

      :tempid-ref
      [{:status :present :key k :value 0 :ref-key ck}]

      ::unsupported)))

(defn- expected-outcome
  [op]
  (case (:f op)
    {:type  :ok
     :value (expected-states op)}))

(defn- expected-final-state
  [op]
  (last (expected-states op)))

(defn- parse-case-id
  [k]
  (when-some [[_ case-id] (re-matches #"internal-(\d+)" k)]
    (Long/parseLong case-id)))

(defn- probe-snapshot
  [db]
  (into {}
        (keep (fn [k]
                (when-some [case-id (parse-case-id k)]
                  [case-id (state-for-key db k)])))
        (d/q '[:find [?k ...]
               :where
               [?e :internal/key ?k]]
             db)))

(defn- execute-op!
  [conn op]
  (if (= :probe (:f op))
    (probe-snapshot @conn)
    (do
      (ensure-tx-fns! conn)
      (let [txns (case-txns op)
            k    (main-key (:internal/case-id op))]
        (if (= ::unsupported txns)
          [:unsupported-client-op (:f op)]
          (mapv (fn [tx]
                  (let [report (d/transact! conn tx)]
                    (state-for-key (workload.util/tx-report-db conn report)
                                   k)))
                txns))))))

(defn- op-error
  [e]
  (if (= :transact/cas (:error (ex-data e)))
    :cas-failed
    (or (ex-message e)
        (.getName (class e)))))

(defn- build-op
  [case-id]
  (let [f (rand-nth [:lookup-ref-same
                     :tx-fn-after-add
                     :tx-fn-twice
                     :cas-chain
                     :retract-add
                     :tempid-ref])]
    {:type             :invoke
     :f                f
     :value            nil
     :internal/case-id case-id}))

(defn- internal-checker
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
            completed  (filter (comp some? :internal/case-id) history)
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
                                    (let [expected (expected-outcome op)
                                          actual   (cond-> {:type (:type op)}
                                                     (= :ok (:type op))
                                                     (assoc :value (:value op))

                                                     (not= :ok (:type op))
                                                     (assoc :error (:error op)))]
                                      (when (not= expected actual)
                                        {:f        (:f op)
                                         :case-id  (:internal/case-id op)
                                         :expected expected
                                         :actual   actual}))))
                            vec)
            probe-mismatches
            (if-not final-probe
              []
              (->> oks
                   (keep (fn [op]
                           (let [case-id  (:internal/case-id op)
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
                    (map #(select-keys % [:f :internal/case-id :error])
                         indeterminate)))
         :disruption-failure-count (count disruption-failures)
         :disruption-failure-samples
         (vec (take 10
                    (map #(select-keys % [:f :internal/case-id :error])
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

  (invoke! [this test op]
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
     :checker   (internal-checker)
     :schema    schema}))
