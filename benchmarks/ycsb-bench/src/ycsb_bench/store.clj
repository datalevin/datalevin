(ns ycsb-bench.store
  "KV and Datalog implementations of the same logical record operations."
  (:require [datalevin.core :as d]
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
  (put-records! [store records] "Insert [key values] pairs; SQL commits each record separately.")
  (read-record [store id] "Return all fields, eagerly.")
  (update-field! [store id field value] "Replace one field, preserving the other fields.")
  (scan-records [store start n]
    "Read up to n ordered [key values] records starting at start (inclusive).")
  (record-count [store] "Count logical records.")
  (storage-info [store] "Return storage settings and engine details for reports.")
  (close-store! [store]))

(defprotocol WorkerStores
  (worker-store [store worker] "Return the handle assigned to this worker."))

(defn for-worker [store worker]
  (if (satisfies? WorkerStores store) (worker-store store worker) store))

(defn- wal-info [handle]
  (assoc (select-keys (kv/txlog-watermarks handle)
                      [:wal? :write-path-enabled? :durability-profile
                       :last-committed-lsn :last-durable-lsn])
         :env-flags (d/get-env-flags handle)))

(defn- kv-read [reader id]
  (or (d/execute-prepared reader id)
      (throw (ex-info "Missing KV record" {:id id}))))

(defn- local-replace-field [values field value]
  (when-not values (throw (ex-info "Missing KV record" {})))
  (assoc values field value))

(def ^:private replace-field
  (inter/inter-fn [values field value]
    (when-not values (throw (ex-info "Missing KV record" {})))
    (assoc values field value)))

(def application-key-schema
  {:ycsb/key {:db/valueType :db.type/string :db/unique :db.unique/value}})

(defn scan-query
  "Select an application key range and return an ordered page with pulls.
  LIMIT is a query literal; stores prepare and reuse each configured page size."
  [attributes limit]
  {:find ['?key (list 'pull '?entity attributes)]
   :in '[$ ?start]
   :where '[[?entity :ycsb/key ?key]
            [(>= ?key ?start)]]
   :order-by '[?key]
   :limit limit})

(defn application-key-info [workload]
  {:workload-model (w/workload-model workload)
   :record-key :ycsb/key
   :insert-semantics :reject-duplicates
   :payload-indexes :none
   :key-type :string :key-generator :ycsb-fnv64-decimal
   :rmw-execution :client-read-update :atomic-rmw? false
   :scan-order :key :scan-start :inclusive :initial-mapsize-mb 4096})

(defrecord KVRecords [handle reader workload remote?]
  Records
  (put-records! [_ records]
    (d/transact-kv handle "records"
                   (mapv (fn [[key values]] [:put key (vec values) #{:nooverwrite}]) records)
                   :string :data))
  (read-record [_ key] (kv-read reader key))
  (update-field! [_ key field value]
    (d/update-kv handle "records" key
                 (if remote? replace-field local-replace-field)
                 :string :data field value))
  (scan-records [_ start n]
    (if (pos? (long n))
      (vec (d/get-first-n handle "records" n [:at-least start] :string :data))
      []))
  (record-count [_] (d/entries handle "records"))
  (storage-info [_]
    (merge (application-key-info workload)
           {:layout :record-value :value-type :data
            :read-api :prepare-get-value :scan-api :get-first-n}
           (wal-info handle)))
  (close-store! [_] (d/close-kv handle)))

(defrecord DatalogRecords [conn attributes reader scan-readers workload local?]
  Records
  (put-records! [_ records]
    (d/transact! conn
                  (mapv (fn [[key values]]
                          (assoc (zipmap attributes values) :ycsb/key key))
                        records)))
  (read-record [_ key]
    (mapv (if local?
            (d/execute-prepared reader @conn [:ycsb/key key])
            (d/execute-prepared reader [:ycsb/key key]))
          attributes))
  (update-field! [_ key field value]
    (d/transact! conn [[:db/add [:ycsb/key key] (nth attributes field) value]]))
  (scan-records [_ start n]
    (if (pos? (long n))
      (let [query (or (get scan-readers n)
                      (throw (ex-info "Page size exceeds configured scan-length" {:limit n})))]
        (mapv (fn [[key record]] [key (mapv record attributes)])
              (d/execute-prepared @query [start])))
      []))
  (record-count [_] (d/count-datoms @conn nil :ycsb/key nil))
  (storage-info [_]
    (merge (application-key-info workload)
           {:layout :entity :read-api :prepare-pull
            :scan-api :prepare-q :scan-selection :attribute-value-range
            :cache-limit (d/datalog-index-cache-limit @conn)}
           (wal-info (d/datalog-kv conn))))
  (close-store! [_] (d/close conn)))

(defn- open-application-key-store!
  [api path {:keys [field-count scan-length workload] :as opts}]
  (let [common (select-keys opts [:wal? :wal-durability-profile :client-opts])]
    (case api
      :kv
      (let [handle (d/open-kv path (assoc common :mapsize 4096))]
        (try
          (d/open-dbi handle "records")
          (->KVRecords handle (d/prepare-get-value handle "records" :string :data)
                       workload (u/dtlv-uri? path))
          (catch Throwable t (d/close-kv handle) (throw t))))
      :datalog
      (let [attributes (mapv #(keyword "ycsb" (str "field" %)) (range field-count))
            schema (merge (zipmap attributes
                                 (repeat {:db/valueType :db.type/string
                                          :db/noindex true}))
                          application-key-schema)
            conn (d/create-conn path schema (assoc common :kv-opts {:mapsize 4096}
                                                  :cache-limit 0 :background-sampling? false))]
        (try
          (let [scan-readers (into [nil]
                                   (map #(delay (d/prepare-q @conn (scan-query attributes %))))
                                   (range 1 (inc (long scan-length))))]
            ;; E prepares every page size before timing. Point workloads do not
            ;; register unused queries; diagnostic scans prepare on first use.
            (when (= workload :e) (run! force (next scan-readers)))
            (->DatalogRecords conn attributes (d/prepare-pull @conn attributes)
                              scan-readers workload (not (u/dtlv-uri? path))))
          (catch Throwable t (d/close conn) (throw t)))))))

(defn- verify-wal! [store durability]
  (let [actual (select-keys (storage-info store)
                           [:wal? :write-path-enabled? :durability-profile])
        expected {:wal? true :write-path-enabled? true :durability-profile durability}]
    (when-not (= expected actual)
      (throw (ex-info "Datalevin WAL settings do not match the benchmark"
                      {:expected expected :actual actual}))))
  store)

(defn- open-store! [api path {:keys [pool-size timeout-ms durability] :as opts}]
  (let [common  {:wal? true :wal-durability-profile durability
                 :client-opts {:pool-size pool-size :time-out timeout-ms}}
        store (open-application-key-store! api path (merge opts common))]
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
  (scan-records [_ start n] (scan-records (first stores) start n))
  (record-count [_] (record-count (first stores)))
  (storage-info [_] (merge (storage-info (first stores)) info))
  (close-store! [_] (close-stores! stores)))

(defn with-stores
  "Call f with a function that opens a fresh database for each callback.
  Databases have separate files; remote phases share one owned server process.
  Each callback's handles close before the next database is opened."
  [{:keys [api mode keep-db? threads pool-size datalog-handles durability] :as options} f]
  (let [root (str (Files/createTempDirectory "datalevin-ycsb-"
                                            (make-array FileAttribute 0)))
        srv  (atom nil)
        database-id (atom 0)
        independent? (and (= api :datalog) (= mode :remote)
                          (= datalog-handles :independent))
        handle-count (if independent? threads 1)
        failure (volatile! nil)]
    (try
      (when (= mode :remote) (reset! srv (owned-server/start! root options)))
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
            with-fresh-store
            (fn [callback]
              (let [database-name (str "ycsb-" (swap! database-id inc))
                    path (if (= mode :embedded)
                           (str root "/" database-name)
                           (.toASCIIString
                             (URI. "dtlv" (str "datalevin:" (server/get-default-password))
                                   "127.0.0.1" (int (get-in @srv [:info :port]))
                                   (str "/" database-name) nil nil)))
                    stores (atom [])
                    failure (volatile! nil)]
                (try
                  (dotimes [_ handle-count]
                    (swap! stores conj
                           (open-store! api path (cond-> options independent? (assoc :pool-size 1)))))
                  (let [result (callback (->StoreGroup @stores (assoc info :database-name database-name)))]
                    (doseq [store @stores] (verify-wal! store durability))
                    result)
                  (catch Throwable t (vreset! failure t) (throw t))
                  (finally
                    (try (close-stores! @stores)
                         (catch Throwable cleanup
                           (if-let [^Throwable primary @failure]
                             (.addSuppressed primary cleanup)
                             (throw cleanup))))))))
            result (f with-fresh-store)]
        (cond-> result keep-db? (assoc :database-directory root)))
      (catch Throwable t (vreset! failure t) (throw t))
      (finally
        (try
          ;; A server that did not terminate may still own native files.
          (when-let [s @srv] ((:close s)))
          (when-not keep-db? (u/delete-files root))
          (catch Throwable cleanup
            (if-let [^Throwable primary @failure]
              (.addSuppressed primary cleanup)
              (throw cleanup))))))))

(defn with-store
  "Create one fresh database, closing its handles and owned server on exit."
  [options f]
  (with-stores options (fn [with-fresh-store] (with-fresh-store f))))
