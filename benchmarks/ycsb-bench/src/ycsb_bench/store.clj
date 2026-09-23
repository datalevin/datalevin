(ns ycsb-bench.store
  "KV and Datalog implementations of the same logical record operations."
  (:require [datalevin.core :as d]
            [datalevin.interface :as i]
            [datalevin.interpret :as inter]
            [datalevin.kv :as kv]
            [datalevin.server :as server]
            [datalevin.util :as u]
            [ycsb-bench.server :as owned-server]
            [ycsb-bench.workload :as w])
  (:import [java.net URI]
           [java.nio.file Files]
           [java.nio.file.attribute FileAttribute]))

(set! *warn-on-reflection* true)

(defprotocol Records
  (put-records! [store records] "Insert [id values] pairs in one transaction.")
  (read-record [store id] "Return all fields, eagerly.")
  (update-field! [store id field value] "Replace one field, preserving the other fields.")
  (modify-field! [store id field] "Atomically read the record and modify a field.")
  (scan-records [store start n] "Read up to n records from the dense ID range.")
  (record-count [store] "Count logical records.")
  (storage-info [store] "Return storage settings and engine details for reports.")
  (close-store! [store]))

(defprotocol WorkerStores
  (worker-store [store worker] "Return the handle assigned to this worker."))

(defn for-worker [store worker]
  (if (satisfies? WorkerStores store) (worker-store store worker) store))

(defn- wal-info [handle]
  (select-keys (kv/txlog-watermarks handle)
               [:wal? :write-path-enabled? :durability-profile
                :last-committed-lsn :last-durable-lsn]))

(defn- kv-read [reader id]
  (or (d/execute-prepared reader id)
      (throw (ex-info "Missing KV record" {:id id}))))

(defn- local-replace-field [values field value]
  (when-not values (throw (ex-info "Missing KV record" {})))
  (assoc values field value))

(defn- local-modify-field [values field]
  (when-not values (throw (ex-info "Missing KV record" {})))
  (update values field w/modified-value))

(def ^:private replace-field
  (inter/inter-fn [values field value]
    (when-not values (throw (ex-info "Missing KV record" {})))
    (assoc values field value)))

(def ^:private modify-field
  (inter/inter-fn [values field]
    (when-not values (throw (ex-info "Missing KV record" {})))
    (update values field ycsb-bench.workload/modified-value)))

(def ^:private datalog-rmw
  (inter/inter-fn [db attributes id field]
    (let [reader (ycsb-bench.workload/rmw-reader db attributes)
          record (datalevin.core/execute-prepared reader db id)
          values (mapv record attributes)]
      [[:db/add id (nth attributes field)
        (ycsb-bench.workload/modified-value (nth values field))]])))

;; Benchmark keys are strictly below Integer/MAX_VALUE. Keep the installed
;; callable outside that dense keyspace so scans and record counts stay intact.
(def ^:private rmw-function-eid Integer/MAX_VALUE)

(defrecord KVRecords [handle value-reader remote?]
  Records
  (put-records! [_ records]
    (d/transact-kv
      handle "records"
      (mapv (fn [[id values]] [:put id (vec values)]) records)
      :id :data))
  (read-record [_ id] (kv-read value-reader id))
  (update-field! [_ id field value]
    (d/update-kv handle "records" id
                 (if remote? replace-field local-replace-field)
                 :id :data field value))
  (modify-field! [_ id field]
    (d/update-kv handle "records" id
                 (if remote? modify-field local-modify-field)
                 :id :data field))
  (scan-records [_ start n]
    (vec (d/get-range handle "records"
                      [:closed-open start (+ (long start) (long n))]
                      :id :data)))
  (record-count [_] (d/entries handle "records"))
  (storage-info [_]
    (merge {:layout :record-value :key-type :id :value-type :data
            :read-api :prepare-get-value
            :rmw-execution (if remote? :server-function :transaction-function)
            :initial-mapsize-mb 4096 :atomic-rmw? true}
           (wal-info handle)))
  (close-store! [_] (d/close-kv handle)))

(defn- datalog-read
  ([reader attributes id]
   (mapv (d/execute-prepared reader id) attributes))
  ([reader view attributes id]
   (mapv (d/execute-prepared reader view id) attributes)))

(defn- local-datalog-rmw [db reader attributes id field]
  (let [values (datalog-read reader db attributes id)]
    [[:db/add id (nth attributes field)
      (w/modified-value (nth values field))]]))

(defn- datalog-scan [conn attribute-positions start n]
  (if (pos? (long n))
    (let [datoms (i/slice (:store @conn) :eav
                          (d/datom start nil nil)
                          (d/datom (dec (+ (long start) (long n))) nil nil))
          field-count (count attribute-positions)]
      ;; EAV already orders the entities. Attribute IDs need not follow the
      ;; benchmark's field order, so place each value in its declared column.
      (mapv (fn [entity-datoms]
              (let [values (object-array field-count)]
                (doseq [datom entity-datoms]
                  (aset values (int (attribute-positions (d/datom-a datom)))
                        (d/datom-v datom)))
                [(d/datom-e (first entity-datoms)) (vec values)]))
            (partition-by d/datom-e datoms)))
    []))

(defrecord DatalogRecords [conn attributes attribute-positions pull-reader remote?]
  Records
  (put-records! [_ records]
    (d/transact! conn (mapv (fn [[id values]]
                             (assoc (zipmap attributes values) :db/id id))
                           records)))
  (read-record [_ id] (datalog-read pull-reader attributes id))
  (update-field! [_ id field value]
    (d/transact! conn [[:db/add id (nth attributes field) value]]))
  (modify-field! [_ id field]
    (if remote?
      (d/transact! conn [[:ycsb/rmw attributes id field]])
      (d/transact! conn [[:db.fn/call local-datalog-rmw
                         pull-reader attributes id field]])))
  (scan-records [_ start n]
    (datalog-scan conn attribute-positions start n))
  (record-count [_] (d/count-datoms @conn nil :ycsb/field0 nil))
  (storage-info [_]
    (merge {:layout :entity :initial-mapsize-mb 4096 :atomic-rmw? true
            :rmw-execution (if remote? :server-function :transaction-function)
            :cache-limit (d/datalog-index-cache-limit @conn)
            :record-key :db/id :read-api :prepare-pull :scan-api :slice}
           (wal-info (d/datalog-kv conn))))
  (close-store! [_] (d/close conn)))

(defn- verify-wal! [store durability]
  (let [actual (select-keys (storage-info store)
                           [:wal? :write-path-enabled? :durability-profile])
        expected {:wal? true :write-path-enabled? true :durability-profile durability}]
    (when-not (= expected actual)
      (throw (ex-info "Datalevin WAL settings do not match the benchmark"
                      {:expected expected :actual actual}))))
  store)

(defn- open-store! [api path {:keys [field-count pool-size timeout-ms durability]}]
  (let [kv-opts {:mapsize 4096}
        common  {:wal? true :wal-durability-profile durability
                 :client-opts {:pool-size pool-size :time-out timeout-ms}}
        store
        (case api
          :kv (let [handle (d/open-kv path (merge kv-opts common))]
                (try
                  (d/open-dbi handle "records")
                  (->KVRecords handle (d/prepare-get-value handle "records" :id :data)
                               (u/dtlv-uri? path))
                  (catch Throwable t (d/close-kv handle) (throw t))))
          :datalog
          (let [attributes (mapv #(keyword "ycsb" (str "field" %)) (range field-count))
                schema (zipmap attributes (repeat {:db/valueType :db.type/string}))
                remote? (u/dtlv-uri? path)
                conn (d/create-conn path schema (assoc common :kv-opts kv-opts
                                                       :cache-limit 0
                                                       :background-sampling? false))]
            (try
              (when (and remote?
                         (nil? (d/pull @conn [:db/ident] rmw-function-eid)))
                (d/transact! conn [{:db/id rmw-function-eid
                                    :db/ident :ycsb/rmw :db/fn datalog-rmw}]))
              (->DatalogRecords conn attributes
                                (zipmap attributes (range field-count))
                                (d/prepare-pull @conn attributes) remote?)
              (catch Throwable t (d/close conn) (throw t)))))]
    (try (verify-wal! store durability)
         (catch Throwable t
           (try (close-store! store) (catch Throwable cleanup (.addSuppressed t cleanup)))
           (throw t)))))

(defn- close-stores! [stores]
  (let [failure (volatile! nil)]
    (doseq [store (reverse stores)]
      (try (close-store! store)
           (catch Throwable t
             (if-let [^Throwable primary @failure]
               (.addSuppressed primary t)
               (vreset! failure t)))))
    (when-let [t @failure] (throw t))))

(defrecord StoreGroup [stores info]
  WorkerStores
  (worker-store [_ worker] (nth stores (if (= 1 (count stores)) 0 worker)))
  Records
  (put-records! [_ records] (put-records! (first stores) records))
  (read-record [_ id] (read-record (first stores) id))
  (update-field! [_ id field value] (update-field! (first stores) id field value))
  (modify-field! [_ id field] (modify-field! (first stores) id field))
  (scan-records [_ start n] (scan-records (first stores) start n))
  (record-count [_] (record-count (first stores)))
  (storage-info [_] (merge (storage-info (first stores)) info))
  (close-store! [_] (close-stores! stores)))

(defn with-store
  "Create a fresh owned temporary directory and optionally a loopback server.
  Cleanup runs on success and failure. No existing database is opened/deleted."
  [{:keys [api mode keep-db? threads pool-size datalog-handles durability] :as options} f]
  (let [root (str (Files/createTempDirectory "datalevin-ycsb-"
                                            (make-array FileAttribute 0)))
        srv  (atom nil)
        stores (atom [])
        independent? (and (= api :datalog) (= mode :remote)
                          (= datalog-handles :independent))
        handle-count (if independent? threads 1)
        failure (volatile! nil)]
    (try
      (let [path (if (= mode :embedded)
                   (str root "/db")
                   (let [s (owned-server/start! root options)]
                     (reset! srv s)
                     (.toASCIIString
                       (URI. "dtlv" (str "datalevin:" (server/get-default-password))
                             "127.0.0.1" (int (get-in s [:info :port])) "/ycsb" nil nil))))]
        (dotimes [_ handle-count]
          (swap! stores conj (open-store! api path (cond-> options independent? (assoc :pool-size 1)))))
        (let [info (cond-> {:client-topology
                           {:handles (if independent? :independent :shared)
                            :handle-count handle-count}}
                     (= mode :remote)
                     (assoc :server (:info @srv)
                            :client-topology
                            {:handles (if independent? :independent :shared)
                             :handle-count handle-count :read-connections pool-size
                             :dedicated-transaction-connections
                             (if (and (not independent?) (> (long pool-size) 1)) 1 0)}))
              result (f (->StoreGroup @stores info))]
          (doseq [store @stores] (verify-wal! store durability))
          (cond-> result keep-db? (assoc :database-directory root))))
      (catch Throwable t (vreset! failure t) (throw t))
      (finally
        (try
          (try
            (close-stores! @stores)
            (finally
              ;; A server that did not terminate may still own native files.
              (when-let [s @srv] ((:close s)))
              (when-not keep-db? (u/delete-files root))))
          (catch Throwable cleanup
            (if-let [^Throwable primary @failure]
              (.addSuppressed primary cleanup)
              (throw cleanup))))))))
