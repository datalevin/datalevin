(ns datalevin.jepsen.workload.append
  (:require
   [datalevin.core :as d]
   [datalevin.jepsen.local :as local]
   [datalevin.jepsen.workload.util :as workload.util]
   [jepsen.client :as client]
   [jepsen.tests.cycle.append :as cycle.append]))

(def schema
  {:append/key   {:db/valueType :db.type/long
                  :db/index true}
   :append/value {:db/valueType :db.type/long}})

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

(defn- execute-txn!
  [conn micro-ops]
  (let [[_ realized] (simulate-txn @conn micro-ops)
        tx-data      (->> micro-ops
                          (filter append-op?)
                          (mapv append-tx-datum))]
    (when (seq tx-data)
      (d/transact! conn tx-data))
    realized))

(defrecord Client [node]
  client/Client
  (open! [this test node]
    (assoc this :node node))

  (setup! [this _test]
    this)

  (invoke! [this test op]
    (try
      (if (not= :txn (:f op))
        (assoc op
               :type :fail
               :error [:unsupported-client-op (:f op)])
        (local/with-leader-conn
          test
          schema
          (fn [conn]
            (assoc op
                   :type :ok
                   :value (execute-txn! conn (:value op))))))
      (catch Throwable e
        (assoc op
               :type :fail
               :error (or (ex-message e)
                          (.getName (class e)))))))

  (teardown! [this _test]
    this)

  (close! [_this _test]
    nil))

(defn workload
  [opts]
  (let [opts'          (merge {:min-txn-length 1
                               :max-txn-length 1}
                              (select-keys opts [:key-count
                                                 :min-txn-length
                                                 :max-txn-length
                                                 :max-writes-per-key]))
        base-workload  (cycle.append/test opts')]
    (-> base-workload
        (assoc :checker
               (workload.util/wrap-empty-graph-checker
                 (:checker base-workload)
                 (fn [op]
                   (= :txn (:f op)))
                 [:f :error]))
        (assoc :client (->Client nil)
               :schema schema))))
