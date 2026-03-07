(ns datalevin.jepsen.workload.grant
  (:require
   [datalevin.core :as d]
   [datalevin.interpret :as i]
   [datalevin.jepsen.local :as local]
   [jepsen.checker :as checker]
   [jepsen.client :as client]))

(def schema
  {:grant/id           {:db/valueType :db.type/long
                        :db/unique :db.unique/identity}
   :grant/amount       {:db/valueType :db.type/long}
   :grant/requested-at {:db/valueType :db.type/long}
   :grant/approved-at  {:db/valueType :db.type/long}
   :grant/denied-at    {:db/valueType :db.type/long}})

(def ^:private create-grant
  (i/inter-fn [db grant-id amount requested-at]
    (if-some [_ent (d/entity db [:grant/id grant-id])]
      []
      [{:db/id              (str "grant-" grant-id)
        :grant/id           (long grant-id)
        :grant/amount       (long amount)
        :grant/requested-at (long requested-at)}])))

(def ^:private approve-grant
  (i/inter-fn [db grant-id approved-at]
    (if-some [ent (d/entity db [:grant/id grant-id])]
      (if (or (:grant/approved-at ent) (:grant/denied-at ent))
        []
        [{:db/id              (:db/id ent)
          :grant/approved-at  (long approved-at)}])
      [])))

(def ^:private deny-grant
  (i/inter-fn [db grant-id denied-at]
    (if-some [ent (d/entity db [:grant/id grant-id])]
      (if (or (:grant/approved-at ent) (:grant/denied-at ent))
        []
        [{:db/id            (:db/id ent)
          :grant/denied-at  (long denied-at)}])
      [])))

(def ^:private tx-fn-entities
  [{:db/ident :grant/create
    :db/fn    create-grant}
   {:db/ident :grant/approve
    :db/fn    approve-grant}
   {:db/ident :grant/deny
    :db/fn    deny-grant}])

(defn- now-ms
  []
  (System/currentTimeMillis))

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

(defn- grant-state
  [db grant-id]
  (if-some [ent (d/entity db [:grant/id (long grant-id)])]
    (let [approved-at  (:grant/approved-at ent)
          denied-at    (:grant/denied-at ent)
          requested-at (:grant/requested-at ent)]
      {:grant-id     (long grant-id)
       :status       (cond
                       (and approved-at denied-at) :invalid
                       approved-at                 :approved
                       denied-at                   :denied
                       requested-at                :pending
                       :else                       :missing)
       :amount       (:grant/amount ent)
       :requested-at requested-at
       :approved-at  approved-at
       :denied-at    denied-at})
    {:grant-id     (long grant-id)
     :status       :missing
     :amount       nil
     :requested-at nil
     :approved-at  nil
     :denied-at    nil}))

(defn- all-grant-states
  [db]
  (->> (d/q '[:find [?grant-id ...]
              :where
              [?e :grant/id ?grant-id]]
            db)
       sort
       (mapv #(grant-state db %))))

(defn- execute-op!
  [conn op]
  (ensure-tx-fns! conn)
  (case (:f op)
    :create
    (let [{:keys [grant-id amount]} (:value op)]
      (d/transact! conn [[:grant/create
                          (long grant-id)
                          (long amount)
                          (now-ms)]])
      (grant-state @conn grant-id))

    :approve
    (let [{:keys [grant-id]} (:value op)]
      (d/transact! conn [[:grant/approve (long grant-id) (now-ms)]])
      (grant-state @conn grant-id))

    :deny
    (let [{:keys [grant-id]} (:value op)]
      (d/transact! conn [[:grant/deny (long grant-id) (now-ms)]])
      (grant-state @conn grant-id))

    :read
    (grant-state @conn (:grant-id (:value op)))

    :read-all
    (all-grant-states @conn)

    [:unsupported-client-op (:f op)]))

(defn- op-error
  [e]
  (or (ex-message e)
      (.getName (class e))))

(defn- invalid-state-reason
  [{:keys [status amount requested-at approved-at denied-at]}]
  (cond
    (and approved-at denied-at)                :double-decision
    (and (not= status :missing) (nil? amount)) :missing-amount
    (and (not= status :missing) (nil? requested-at))
    :missing-request
    (not (contains? #{:missing :pending :approved :denied :invalid} status))
    :unknown-status
    :else nil))

(defn- grant-checker
  []
  (reify checker/Checker
    (check [_ _test history _opts]
      (let [op-states       (->> history
                                 (filter (fn [{:keys [type f value]}]
                                           (and (= :ok type)
                                                (contains? #{:create :approve :deny :read} f)
                                                (map? value))))
                                 (map :value))
            final-snapshots (->> history
                                 (filter (fn [{:keys [type f value]}]
                                           (and (= :ok type)
                                                (= :read-all f)
                                                (vector? value))))
                                 (map :value)
                                 vec)
            observed-states (concat op-states (mapcat identity final-snapshots))
            invalid         (->> observed-states
                                 (keep (fn [state]
                                         (when-let [reason (invalid-state-reason state)]
                                           (assoc state :error reason))))
                                 vec)]
        {:valid?               (empty? invalid)
         :observed-state-count (count observed-states)
         :final-read-count     (count final-snapshots)
         :invalid-count        (count invalid)
         :invalid-samples      (vec (take 10 invalid))}))))

(defn- random-op
  [key-count]
  (let [grant-id (rand-int (int key-count))]
    (case (rand-int 10)
      0 {:type :invoke
         :f :create
         :value {:grant-id grant-id
                 :amount   (inc (rand-int 1000))}}
      1 {:type :invoke :f :create :value {:grant-id grant-id
                                          :amount   (inc (rand-int 1000))}}
      2 {:type :invoke :f :approve :value {:grant-id grant-id}}
      3 {:type :invoke :f :approve :value {:grant-id grant-id}}
      4 {:type :invoke :f :deny :value {:grant-id grant-id}}
      5 {:type :invoke :f :deny :value {:grant-id grant-id}}
      6 {:type :invoke :f :read :value {:grant-id grant-id}}
      7 {:type :invoke :f :read :value {:grant-id grant-id}}
      8 {:type :invoke :f :read :value {:grant-id grant-id}}
      {:type :invoke :f :approve :value {:grant-id grant-id}})))

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
        (assoc op
               :type :fail
               :error (op-error e)))))

  (teardown! [this _test]
    this)

  (close! [_this _test]
    nil))

(defn workload
  [opts]
  (let [key-count (long (or (:key-count opts) 8))]
    {:client          (->Client nil)
     :generator       (repeatedly #(random-op key-count))
     :final-generator {:type :invoke :f :read-all}
     :checker         (grant-checker)
     :schema          schema}))
