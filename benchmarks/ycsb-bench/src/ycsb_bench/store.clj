(ns ycsb-bench.store
  "KV and Datalog implementations of the same logical record operations."
  (:require [datalevin.core :as d]
            [datalevin.server :as server]
            [datalevin.util :as u]
            [ycsb-bench.workload :as w])
  (:import [datalevin.server Server]
           [java.net InetSocketAddress URI]
           [java.nio.channels ServerSocketChannel]
           [java.nio.file Files]
           [java.nio.file.attribute FileAttribute]))

(set! *warn-on-reflection* true)

(defprotocol Records
  (put-records! [store records] "Insert [id values] pairs in one transaction.")
  (read-record [store id] "Return all fields, eagerly.")
  (update-field! [store id field value] "Blind update of one field.")
  (modify-field! [store id field] "Atomically read the record and modify a field.")
  (scan-records [store start n] "Read up to n records from the dense ID range.")
  (record-count [store] "Count logical records, detecting partial KV records.")
  (storage-info [store] "Return storage settings and engine details for reports.")
  (close-store! [store]))

(defn- kv-read [handle fields id]
  (vec (d/get-range handle "records"
                    [:closed-open (* (long id) (long fields))
                     (* (inc (long id)) (long fields))]
                    :long :string true)))

(defn- kv-update! [handle fields id field value]
  (d/transact-kv handle "records"
                 [[:put (+ (* (long id) (long fields)) (long field)) value]]
                 :long :string))

(defrecord KVRecords [handle fields]
  Records
  (put-records! [_ records]
    (d/transact-kv
      handle "records"
      (vec (mapcat (fn [[id values]]
                     (map-indexed
                       (fn [field value]
                         [:put (+ (* (long id) (long fields)) (long field)) value])
                       values)) records))
      :long :string))
  (read-record [_ id] (kv-read handle fields id))
  (update-field! [_ id field value] (kv-update! handle fields id field value))
  (modify-field! [_ id field]
    (d/with-transaction-kv [tx handle]
      (let [values (kv-read tx fields id)]
        (kv-update! tx fields id field (w/modified-value (nth values field))))))
  (scan-records [_ start n]
    (->> (d/get-range handle "records"
                      [:closed-open (* (long start) (long fields))
                       (* (+ (long start) (long n)) (long fields))]
                      :long :string)
         (partition-all fields)
         (mapv (fn [entries]
                 [(quot (long (ffirst entries)) (long fields))
                  (mapv second entries)]))))
  (record-count [_]
    (let [entries (long (d/entries handle "records"))]
      (when-not (zero? (rem entries (long fields)))
        (throw (ex-info "Incomplete KV record" {:entries entries :fields fields})))
      (quot entries (long fields))))
  (storage-info [_]
    {:layout :field-keys :wal? true :initial-mapsize-mb 4096 :atomic-rmw? true})
  (close-store! [_] (d/close-kv handle)))

(defn- datalog-read [conn attributes id]
  (let [record (d/pull @conn attributes [:ycsb/id id])]
    (mapv record attributes)))

(def scan-query
  '[:find ?id (pull ?e pattern)
    :in $ ?lo ?hi pattern
    :where [?e :ycsb/id ?id]
    [(<= ?lo ?id)] [(< ?id ?hi)]])

(defrecord DatalogRecords [conn attributes]
  Records
  (put-records! [_ records]
    (d/transact! conn (mapv (fn [[id values]]
                             (assoc (zipmap attributes values) :ycsb/id id))
                           records)))
  (read-record [_ id] (datalog-read conn attributes id))
  (update-field! [_ id field value]
    (d/transact! conn [[:db/add [:ycsb/id id] (nth attributes field) value]]))
  (modify-field! [_ id field]
    (d/with-transaction [tx conn]
      (let [values (datalog-read tx attributes id)]
        (d/transact! tx [[:db/add [:ycsb/id id] (nth attributes field)
                         (w/modified-value (nth values field))]]))))
  (scan-records [_ start n]
    (->> (d/q scan-query @conn start (+ (long start) (long n)) attributes)
         (sort-by first)
         (mapv (fn [[id record]] [id (mapv record attributes)]))))
  (record-count [_] (d/q '[:find (count ?e) . :where [?e :ycsb/id]] @conn))
  (storage-info [_]
    {:layout :entity :wal? true :initial-mapsize-mb 4096 :atomic-rmw? true})
  (close-store! [_] (d/close conn)))

(defn- open-store! [api path {:keys [field-count pool-size timeout-ms durability]}]
  (let [kv-opts {:mapsize 4096}
        common  {:wal? true :wal-durability-profile durability
                 :client-opts {:pool-size pool-size :time-out timeout-ms}}]
    (case api
      :kv (let [handle (d/open-kv path (merge kv-opts common))]
            (try
              (d/open-dbi handle "records")
              (->KVRecords handle field-count)
              (catch Throwable t (d/close-kv handle) (throw t))))
      :datalog
      (let [attributes (mapv #(keyword "ycsb" (str "field" %)) (range field-count))
            schema (assoc (zipmap attributes (repeat {:db/valueType :db.type/string}))
                          :ycsb/id {:db/valueType :db.type/long
                                    :db/unique :db.unique/identity})]
        (->DatalogRecords
          (d/create-conn path schema (assoc common :kv-opts kv-opts
                                           :background-sampling? false))
          attributes)))))

(defn with-store
  "Create a fresh owned temporary directory and optionally a loopback server.
  Cleanup runs on success and failure. No existing database is opened/deleted."
  [{:keys [api mode keep-db? threads] :as options} f]
  (let [root (str (Files/createTempDirectory "datalevin-ycsb-"
                                            (make-array FileAttribute 0)))
        srv  (atom nil)
        db   (atom nil)]
    (try
      (let [path (if (= mode :embedded)
                   (str root "/db")
                   (let [s (server/create {:root (str root "/server")
                                           :host "127.0.0.1" :port 0 :verbose false
                                           :worker-threads (max 4 threads)})]
                     (reset! srv s)
                     (server/start s)
                     (let [socket (.-server-socket ^Server s)
                           address (.getLocalAddress ^ServerSocketChannel socket)
                           port (.getPort ^InetSocketAddress address)]
                       (.toASCIIString
                         (URI. "dtlv" (str "datalevin:" (server/get-default-password))
                               "127.0.0.1" port "/ycsb" nil nil)))))
            store (open-store! api path options)]
        (reset! db store)
        (cond-> (f store) keep-db? (assoc :database-directory root)))
      (finally
        (try
          (when-let [store @db] (close-store! store))
          (finally
            (try
              (when-let [s @srv] (server/stop s))
              (finally (when-not keep-db? (u/delete-files root))))))))))
