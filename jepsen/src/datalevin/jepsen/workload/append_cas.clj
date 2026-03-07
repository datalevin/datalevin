(ns datalevin.jepsen.workload.append-cas
  (:require
   [datalevin.core :as d]
   [datalevin.jepsen.local :as local]
   [jepsen.client :as client]
   [jepsen.tests.cycle.append :as append]))

(def schema
  {:append/key          {:db/valueType :db.type/long
                         :db/index true}
   :append/value        {:db/valueType :db.type/long}
   :append.meta/key     {:db/valueType :db.type/long
                         :db/unique :db.unique/identity}
   :append.meta/version {:db/valueType :db.type/long}})

(defonce ^:private initialized-clusters (atom #{}))

(defn- append-op?
  [[f]]
  (= :append f))

(defn- append-tx-datum
  [[_ k v]]
  {:append/key   (long k)
   :append/value (long v)})

(defn- read-list
  [db k]
  (->> (d/datoms db :ave :append/key k)
       (map (fn [key-datom]
              (let [value-datom (first (d/datoms db
                                                 :eav
                                                 (:e key-datom)
                                                 :append/value))]
                [(:tx key-datom) (:e key-datom) (:v value-datom)])))
       (remove (fn [[_ _ value]] (nil? value)))
       (sort-by (juxt first second))
       (mapv (fn [[_ _ value]] value))))

(defn- simulate-txn
  [db micro-ops]
  (reduce
   (fn [[pending realized] [f k v :as micro-op]]
     (let [current-list (if (contains? pending k)
                          (get pending k)
                          (read-list db k))]
       (case f
         :append
         (let [updated-list (conj current-list v)]
           [(assoc pending k updated-list)
            (conj realized micro-op)])

         :r
         [pending
          (conj realized [:r k current-list])])))
   [{} []]
   micro-ops))

(defn- current-version
  [db k]
  (d/q '[:find ?v .
         :in $ ?k
         :where
         [?e :append.meta/key ?k]
         [?e :append.meta/version ?v]]
       db
       (long k)))

(defn- ensure-meta-entities!
  [test]
  (let [cluster-id (:datalevin/cluster-id test)]
    (when-not (contains? @initialized-clusters cluster-id)
      (locking initialized-clusters
        (when-not (contains? @initialized-clusters cluster-id)
          (local/with-leader-conn
            test
            schema
            (fn [conn]
              (let [db       @conn
                    present  (set (d/q '[:find [?k ...]
                                         :where
                                         [?e :append.meta/key ?k]]
                                       db))
                    missing  (remove present (range (long (:key-count test))))
                    tx-data  (mapv (fn [k]
                                     {:append.meta/key     (long k)
                                      :append.meta/version 0})
                                   missing)]
                (when (seq tx-data)
                  (d/transact! conn tx-data)))))
          (swap! initialized-clusters conj cluster-id))))))

(defn- cas-tx-data
  [db micro-ops]
  (let [keys-written (->> micro-ops
                          (filter append-op?)
                          (map second)
                          distinct
                          sort)
        [meta-ops cas-ops]
        (reduce
         (fn [[meta-ops cas-ops] k]
           (let [v         (current-version db k)
                 current-v (long (or v 0))]
             [(cond-> meta-ops
                (nil? v)
                (conj {:append.meta/key     (long k)
                       :append.meta/version 0}))
              (conj cas-ops
                    [:db/cas
                     [:append.meta/key (long k)]
                     :append.meta/version
                     current-v
                     (inc current-v)])]))
         [[] []]
         keys-written)
        append-ops   (->> micro-ops
                          (filter append-op?)
                          (mapv append-tx-datum))]
    (into meta-ops (concat cas-ops append-ops))))

(defn- execute-txn!
  [conn micro-ops]
  (let [db           @conn
        [_ realized] (simulate-txn db micro-ops)
        tx-data      (cas-tx-data db micro-ops)]
    (when (seq tx-data)
      (d/transact! conn tx-data))
    realized))

(defn- op-error
  [e]
  (if (= :transact/cas (:error (ex-data e)))
    :cas-failed
    (or (ex-message e)
        (.getName (class e)))))

(defrecord Client [node]
  client/Client
  (open! [this _test node]
    (assoc this :node node))

  (setup! [this test]
    (ensure-meta-entities! test)
    this)

  (invoke! [this test op]
    (try
      (if (not= :txn (:f op))
        (assoc op
               :type :fail
               :error [:unsupported-client-op (:f op)])
        (do
          (ensure-meta-entities! test)
          (local/with-leader-conn
            test
            schema
            (fn [conn]
              (assoc op
                     :type :ok
                     :value (execute-txn! conn (:value op)))))))
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
  (let [opts' (merge {:min-txn-length 1
                      :max-txn-length 1}
                     (select-keys opts [:key-count
                                        :min-txn-length
                                        :max-txn-length
                                        :max-writes-per-key]))]
    (-> (append/test opts')
        (assoc :client (->Client nil)
               :checker (append/checker
                         {:consistency-models
                          [:strong-session-snapshot-isolation]})
               :schema schema))))
