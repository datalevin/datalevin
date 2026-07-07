(ns datalevin.jepsen.workload.bank
  (:require
   [datalevin.core :as d]
   [datalevin.interpret :as i]
   [datalevin.jepsen.init-cache :as init-cache]
   [datalevin.jepsen.local :as local]
   [datalevin.jepsen.workload.util :as workload.util]
   [jepsen.checker :as checker]
   [jepsen.client :as client]
   [taoensso.timbre :as log])
  (:import
   [java.util.concurrent Callable ExecutionException Executors ThreadFactory
    TimeUnit TimeoutException]))

(def schema
  {:bank/id {:db/valueType :db.type/long
             :db/unique :db.unique/identity}
   :bank/balance {:db/valueType :db.type/long}})

(def ^:private transfer-balance
  (let [account-query '[:find ?e ?balance
                       :in $ ?account-id
                       :where
                       [?e :bank/id ?account-id]
                       [?e :bank/balance ?balance]]]
    (i/inter-fn [db from-id to-id amount]
                  (let [from-id (long from-id)
                      to-id (long to-id)
                      amount (long amount)]
                  (if (= from-id to-id)
                    []
                    (if-some [[from-entid from-bal] (first (d/q account-query db from-id))]
                      (if-some [[to-entid to-bal] (first (d/q account-query db to-id))]
                        (if (and (integer? from-bal)
                                 (integer? to-bal)
                                 (not (neg? amount))
                                 (>= (long from-bal) amount))
                          [{:db/id from-entid
                            :bank/balance (- (long from-bal) amount)}
                           {:db/id to-entid
                            :bank/balance (+ (long to-bal) amount)}]
                          [])
                        [])
                      []))))))

(def ^:private tx-fn-entities
  [{:db/ident :bank/transfer
    :db/fn transfer-balance}])

(defonce ^:private initialized-clusters (init-cache/cluster-cache))
(def ^:private default-setup-timeout-ms 15000)
(def ^:dynamic *close-conn!* d/close)
(def ^:dynamic *read-timeout-ms* 10000)
(def ^:dynamic *op-timeout-ms* 10000)
(def ^:private tx-fn-query
  '[:find ?fn .
    :where
    [?e :db/ident :bank/transfer]
    [?e :db/fn ?fn]])
(def ^:private balances-query
  '[:find ?account-id ?balance
    :where
    [?e :bank/id ?account-id]
    [?e :bank/balance ?balance]])
(def ^:private account-balance-query
  '[:find ?balance
    :in $ ?account-id
    :where
    [?e :bank/id ?account-id]
    [?e :bank/balance ?balance]])

(defn- account-balance
  [db account-id]
  (some->> (d/q account-balance-query db (long account-id))
           first
           first
           long))

(defn- all-balances
  [db account-count]
  (let [account-count (long account-count)
        balances (into {}
                       (map (fn [[account-id balance]]
                              [(long account-id) (long balance)]))
                       (d/q '[:find ?account-id ?balance
                              :where
                              [?e :bank/id ?account-id]
                              [?e :bank/balance ?balance]]
                            db))]
    (mapv balances (range account-count))))

(defn- ensure-tx-fns!
  [conn]
  (let [db @conn
        idents (mapv :db/ident tx-fn-entities)
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

(defn- ensure-accounts!
  [conn account-count initial-balance]
  (let [present (set (d/q '[:find [?account-id ...]
                            :where
                            [?e :bank/id ?account-id]]
                          @conn))
        missing (->> (range (long account-count))
                     (remove present)
                     (mapv (fn [account-id]
                             {:db/id (str "bank-account-" account-id)
                              :bank/id (long account-id)
                              :bank/balance (long initial-balance)})))]
    (when (seq missing)
      (d/transact! conn missing))))

(defn- local-node-bank-state
  [cluster-id logical-node account-count]
  (let [transfer-fn (local/local-query cluster-id
                                       logical-node
                                       tx-fn-query)
        rows        (local/local-query cluster-id
                                       logical-node
                                       balances-query)]
    (cond
      (= ::local/unavailable transfer-fn)
      {:transfer-fn-visible? false
       :balances ::local/unavailable
       :node-diagnostics (local/node-diagnostics cluster-id logical-node)
       :ready? false}

      (= ::local/unavailable rows)
      {:transfer-fn-visible? (fn? transfer-fn)
       :balances ::local/unavailable
       :node-diagnostics (local/node-diagnostics cluster-id logical-node)
       :ready? false}

      :else
      (let [balances-by-account (into {}
                                     (map (fn [[account-id balance]]
                                            [(long account-id) (long balance)]))
                                     rows)
            balances            (mapv balances-by-account
                                      (range (long account-count)))]
        {:transfer-fn-visible? (fn? transfer-fn)
         :balances balances
         :node-diagnostics (local/node-diagnostics cluster-id logical-node)
         :ready? (and (fn? transfer-fn)
                      (= (long account-count) (count balances))
                      (every? integer? balances))}))))

(defn- wait-for-bank-visible-on-live-nodes!
  [cluster-id account-count initial-balance]
  (let [timeout-ms (local/workload-setup-timeout-ms cluster-id
                                                    default-setup-timeout-ms)
        deadline (+ (System/currentTimeMillis) timeout-ms)
        expected-balances (vec (repeat (long account-count)
                                       (long initial-balance)))]
    (loop [last-snapshot nil]
      (let [live-nodes (-> (local/cluster-state cluster-id) :live-nodes sort)
            snapshot   (into {}
                             (map (fn [logical-node]
                                    [logical-node
                                     (local-node-bank-state
                                      cluster-id
                                      logical-node
                                      account-count)]))
                             live-nodes)]
        (cond
          (every? (fn [[_ {:keys [ready? balances]}]]
                    (and ready?
                         (= expected-balances balances)))
                  snapshot)
          snapshot

          (< (System/currentTimeMillis) deadline)
          (do
            (Thread/sleep 250)
            (recur snapshot))

          :else
          (let [data {:cluster-id cluster-id
                      :timeout-ms timeout-ms
                      :expected-balances expected-balances
                      :snapshot snapshot
                      :previous-snapshot last-snapshot}]
            (log/warn "Timed out waiting for bank seed state on live nodes"
                      data)
            (throw (ex-info "Timed out waiting for bank seed state on live nodes"
                            data))))))))

(defn- ensure-bank-initialized!
  [test account-count initial-balance]
  (let [cluster-id (:datalevin/cluster-id test)]
    (when-not (contains? @initialized-clusters cluster-id)
      (locking initialized-clusters
        (when-not (contains? @initialized-clusters cluster-id)
          (workload.util/with-retrying-leader-conn
            :setup
            test
            schema
            (local/workload-setup-timeout-ms cluster-id
                                             default-setup-timeout-ms)
            (fn [conn]
              (ensure-tx-fns! conn)
              (ensure-accounts! conn account-count initial-balance)))
          (wait-for-bank-visible-on-live-nodes!
            cluster-id
            account-count
            initial-balance)
          (swap! initialized-clusters conj cluster-id))))))

(defn- execute-op!
  [conn account-count op]
  (case (:f op)
    :transfer
    (let [{:keys [from to amount]} (:value op)
          from (long from)
          to (long to)
          amount (long amount)
          report (d/transact! conn [[:bank/transfer from to amount]])
          db-snapshot (or (:db-after report) (:db-before report))
          applied? (boolean (seq (:tx-data report)))
          after-from (account-balance db-snapshot from)
          after-to (account-balance db-snapshot to)]
        {:from from
         :to to
         :amount amount
         :applied? applied?
         :from-balance after-from
         :to-balance after-to})

    :read-all
    (all-balances @conn account-count)

    [:unsupported-client-op (:f op)]))

(defn- op-error
  [e]
  (let [data     (ex-data e)
        err-data (or (:err-data data) data)]
    (cond
      (= :ha/read-rejected (:error err-data))
      :ha/read-rejected

      (when-some [message (ex-message e)]
        (re-find #"HA read admission rejected" message))
      :ha/read-rejected

      :else
      (or (ex-message e)
          (.getName (class e))))))

(defn- read-rejection-op?
  [op error]
  (and (= :read-all (:f op))
       (= :ha/read-rejected error)))

(defn- assoc-bank-exception-op
  [op e]
  (let [error (op-error e)]
    (if (read-rejection-op? op error)
      (assoc op
             :type :info
             :error error)
      (workload.util/assoc-exception-op op e error))))

(defn- close-conn-quietly!
  [conn]
  (when conn
    (try
      (*close-conn!* conn)
      (catch Throwable _
        nil))))

(defn- async-close-conn-quietly!
  [conn]
  (when conn
    (let [close! (bound-fn []
                   (close-conn-quietly! conn))]
      (.start
       (doto (Thread.
              (reify Runnable
                (run [_]
                  (close!)))
              (str "datalevin-jepsen-bank-close-timeout-"
                   (System/nanoTime)))
         (.setDaemon true))))))

(defn- call-with-timeout-ms
  [timeout-ms f]
  (let [executor (Executors/newSingleThreadExecutor
                  (reify ThreadFactory
                    (newThread [_ runnable]
                      (doto (Thread. runnable
                                     (str "datalevin-jepsen-bank-op-timeout-"
                                          (System/nanoTime)))
                        (.setDaemon true)))))
        bound-f  (bound-fn* f)
        task     (.submit executor
                          ^Callable
                          (reify Callable
                            (call [_]
                              (bound-f))))]
    (try
      {:status :completed
       :value (.get task (long timeout-ms) TimeUnit/MILLISECONDS)}
      (catch TimeoutException _
        (.cancel task true)
        {:status :timeout})
      (catch ExecutionException e
        (throw (or (.getCause e) e)))
      (catch InterruptedException e
        (.cancel task true)
        (.interrupt (Thread/currentThread))
        (throw e))
      (finally
        (.shutdownNow executor)))))

(defn- complete-op
  [op value]
  (if (and (vector? value)
           (= :unsupported-client-op (first value)))
    (assoc op
           :type :fail
           :error value)
    (assoc op
           :type :ok
           :value value)))

(defn- timeout-ms-for-op
  [op]
  (if (= :read-all (:f op))
    *read-timeout-ms*
    *op-timeout-ms*))

(defn- timeout-error-for-op
  [op]
  (if (= :read-all (:f op))
    :read-timeout
    :op-timeout))

(defn- invoke-with-fresh-leader-conn-timeout
  [test account-count op]
  (let [timeout-ms (timeout-ms-for-op op)
        conn*      (atom nil)
        result     (call-with-timeout-ms
                    timeout-ms
                    (fn []
                      (let [conn (workload.util/*open-leader-conn* test schema)]
                        (reset! conn* conn)
                        (try
                          (complete-op op (execute-op! conn account-count op))
                          (finally
                            (close-conn-quietly! conn))))))]
    (if (= :timeout (:status result))
      (do
        (async-close-conn-quietly! @conn*)
        (assoc op
               :type :info
               :error (timeout-error-for-op op)
               :bank/timeout-ms timeout-ms))
      (:value result))))

(defn- balances-invalid-reason
  [balances expected-count expected-total]
  (cond
    (not= expected-count (count balances))
    :wrong-account-count

    (some nil? balances)
    :missing-account

    (some (fn [balance]
            (or (not (integer? balance))
                (neg? (long balance))))
          balances)
    :negative-balance

    (not= expected-total (reduce + 0 balances))
    :total-mismatch

    :else nil))

(defn- bank-checker
  [account-count initial-balance]
  (let [account-count (long account-count)
        expected-total (* account-count (long initial-balance))]
    (reify checker/Checker
      (check [_ _test history _opts]
        (let [successful-reads (->> history
                                    (filter (fn [{:keys [type f value]}]
                                              (and (= :ok type)
                                                   (= :read-all f)
                                                   (vector? value))))
                                    (map :value)
                                    vec)
              invalid-reads (->> successful-reads
                                 (keep-indexed
                                  (fn [idx balances]
                                    (when-let [reason
                                               (balances-invalid-reason
                                                balances
                                                account-count
                                                expected-total)]
                                      {:read-index idx
                                       :error reason
                                       :balances balances})))
                                 vec)]
          {:valid? (if (seq successful-reads)
                     (empty? invalid-reads)
                     :unknown)
           :read-count (count successful-reads)
           :invalid-count (count invalid-reads)
           :invalid-samples (vec (take 10 invalid-reads))
           :expected-total expected-total
           :expected-accounts account-count})))))

(defn- random-transfer-op
  [account-count max-transfer]
  (let [account-count (long account-count)
        from (rand-int (int account-count))
        to-base (rand-int (max 1 (dec (int account-count))))
        to (if (>= to-base from)
             (inc to-base)
             to-base)]
    {:type :invoke
     :f :transfer
     :value {:from from
             :to (long to)
             :amount (long (inc (rand-int (int max-transfer))))}}))

(defn- random-op
  [account-count max-transfer]
  (if (< (rand) 0.25)
    {:type :invoke :f :read-all}
    (random-transfer-op account-count max-transfer)))

(defrecord Client [node account-count initial-balance]
  client/Client
  (open! [this _test node]
    (assoc this :node node))

  (setup! [this test]
    (ensure-bank-initialized! test account-count initial-balance)
    this)

  (invoke! [this test op]
    (try
      (ensure-bank-initialized! test account-count initial-balance)
      (invoke-with-fresh-leader-conn-timeout test account-count op)
      (catch Throwable e
        (assoc-bank-exception-op op e))))

  (teardown! [this _test]
    this)

  (close! [this _test]
    this))

(defn workload
  [opts]
  (let [account-count (long (or (:key-count opts) 8))
        initial-balance (long (or (:account-balance opts) 100))
        max-transfer (long (or (:max-transfer opts) 5))]
    (when (< account-count 2)
      (throw (ex-info "bank workload requires at least 2 accounts"
                      {:key-count account-count})))
    (when (not (pos? max-transfer))
      (throw (ex-info "bank workload requires a positive max transfer"
                      {:max-transfer max-transfer})))
    {:client (->Client nil account-count initial-balance)
     :generator (repeatedly #(random-op account-count max-transfer))
     :final-generator {:type :invoke :f :read-all}
     :checker (bank-checker account-count initial-balance)
     :schema schema}))
