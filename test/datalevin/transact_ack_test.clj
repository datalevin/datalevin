(ns datalevin.transact-ack-test
  (:require
   [clojure.test :refer [deftest is use-fixtures]]
   [datalevin.bits :as b]
   [datalevin.client :as client]
   [datalevin.client-op :as cop]
   [datalevin.constants :as c]
   [datalevin.core :as d]
   [datalevin.db :as db]
   [datalevin.interface :as i]
   [datalevin.interpret :as inter]
   [datalevin.server :as server]
   [datalevin.test.core :refer [allocate-port db-fixture]]
   [datalevin.util :as u])
  (:import
   [datalevin.remote DatalogStore]
   [datalevin.storage Store]
   [datalevin.utl LRUCache]
   [java.util.concurrent ConcurrentHashMap]
   [java.util.concurrent.atomic AtomicLong]))

(def ^:dynamic *server* nil)
(def ^:dynamic *base-uri* nil)

(use-fixtures :once
  db-fixture
  (fn [f]
    (let [root (u/tmp-dir (str "transact-ack-" (random-uuid)))
          port (allocate-port)
          srv (server/create {:root root :port port})]
      (try
        (server/start srv)
        (binding [*server* srv
                  *base-uri* (str "dtlv://datalevin:datalevin@localhost:" port "/")]
          (f))
        (finally (server/stop srv) (u/delete-files root))))))

(def ^:private schema
  {:item/key {:db/valueType :db.type/string :db/unique :db.unique/value}
   :item/value {:db/valueType :db.type/string :db/noindex true}
   :counter {:db/valueType :db.type/long}})

(defn- trace-requests [f]
  (let [calls (atom [])
        handlers @#'server/message-handler-map
        tracked (into {} (map (fn [[type handler]]
                               [type (fn [srv key message]
                                       (swap! calls conj message)
                                       (handler srv key message))])) handlers)]
    (with-redefs-fn {#'server/message-handler-map tracked}
      #(let [result (f)] {:result result :requests @calls}))))

(defn- saved-record [message]
  (let [store ^Store (#'server/get-store *server* (first (:args message)) false)]
    (d/get-value (.-lmdb store) c/ha-client-ops
                 (cop/kv-info-key (:client-op-id message)) :string :data)))

(defn- read-cache ^LRUCache [conn]
  (.get ^ConcurrentHashMap @#'db/caches (i/dir (:store @conn))))

(def increment-counter
  (inter/inter-fn [db eid]
    [[:db/add eid :counter
      (inc (long (:counter (datalevin.core/pull db [:counter] eid))))]]))

(deftest ack-preserves-listeners-errors-and-explicit-transactions
  (doseq [remote? [false true]
          wal? [false true]]
    (let [conn (d/create-conn (when remote? (str *base-uri* (random-uuid)))
                              schema
                              (cond-> {:wal? wal?}
                                (not remote?) (assoc :kv-opts {:inmemory? true})))
          events (atom [])]
      (try
        (d/listen! conn :test #(swap! events conj %))
        (let [before @conn
              {:keys [result requests]}
              (trace-requests
                #(d/transact-ack! conn [{:db/id "new" :item/key "one"
                                        :item/value "original"}]
                                  {:source :test}))
              report (first @events)]
          (is (= :transacted result))
          (is (= 1 (count @events)))
          (is (= (:max-tx before) (:max-tx (:db-before report))))
          (is (identical? @conn (:db-after report)))
          (is (= {:source :test} (:tx-meta report)))
          (is (= 2 (count (:tx-data report))))
          (is (pos-int? (get-in report [:tempids "new"])))
          (when remote?
            (is (identical? before (:db-before report)))
            (is (= [:tx-data+db-info] (mapv :type requests)))
            (is (= 2 (count (:tx-data (cop/record-response
                                      (saved-record (first requests)))))))))
        (d/unlisten! conn :test)
        (let [before @conn]
          (is (thrown-with-msg? Exception #"unique constraint"
                (d/transact-ack! conn [{:item/key "discarded"}
                                      {:item/key "one"}])))
          (is (identical? before @conn)))
        (is (nil? (d/entity @conn [:item/key "discarded"])))
        (d/with-transaction [tx conn]
          (is (= :transacted
                 (d/transact-ack! tx [{:item/key "committed" :item/value "kept"}]))))
        (is (= "kept" (:item/value (d/entity @conn [:item/key "committed"]))))
        (d/with-transaction [tx conn]
          (is (= :transacted
                 (d/transact-ack! tx [{:item/key "aborted" :item/value "staged"}])))
          (is (= "staged" (:item/value (d/entity @tx [:item/key "aborted"]))))
          (d/abort-transact tx))
        (is (nil? (d/entity @conn [:item/key "aborted"])))
        ;; Abort also discards updates and newly inferred schema.
        (d/with-transaction [tx conn]
          (is (= :transacted
                 (d/transact-ack! tx [{:db/id [:item/key "one"]
                                      :item/value "staged" :new/attribute 1}])))
          (is (= "staged" (:item/value (d/entity @tx [:item/key "one"]))))
          (d/abort-transact tx))
        (is (= "original" (:item/value (d/entity @conn [:item/key "one"]))))
        (is (not (contains? (d/schema conn) :new/attribute)))
        (is (= 1 (count @events)))
        (finally (d/close conn))))))

(deftest remote-ack-refreshes-cache-and-saves-a-compact-replay
  (doseq [wal? [false true]]
    (let [conn (d/create-conn (str *base-uri* (random-uuid)) schema {:wal? wal?})
          payload (apply str (map (fn [_] (random-uuid)) (range 100)))]
      (try
        (let [{full-requests :requests}
              (trace-requests #(d/transact! conn [{:item/key "one"
                                                  :item/value payload}]))
              full-record (saved-record (first full-requests))
              eid (:db/id (d/entity @conn [:item/key "one"]))]
          (binding [c/*remote-db-last-modified-check-interval-ms* 60000]
            (is (= 2 (count (d/datoms @conn :eav eid))))
            (let [cache-token (db/cache-token (:store @conn))
                  before (:max-tx @conn)
                  {:keys [result requests]}
                  (trace-requests
                    #(d/transact-ack! conn [[:db/add eid :item/value (str payload "new")]]))
                  message (first requests)
                  record (saved-record message)
                  response (cop/record-response record)
                  store ^DatalogStore (:store @conn)]
              (is (= :transacted result))
              (is (= [:tx-data-ack] (mapv :type requests)))
              (is (= :tx-data-ack (:response-kind record) (:request-type record)))
              (is (= #{:result :db-info} (set (keys response))))
              (is (= :transacted (:result response)))
              (is (< (alength ^bytes (b/serialize record))
                     (/ (alength ^bytes (b/serialize full-record)) 4)))
              (is (< before (:max-tx @conn)))
              (is (= (:max-eid @conn) (get-in response [:db-info :max-eid])))
              (is (= (:max-tx @conn) (get-in response [:db-info :max-tx])
                     (.get ^AtomicLong (.-read-floor-tx store))))
              (is (.isEmpty (read-cache conn)))
              (is (false? (db/cache-put-if-current
                            (:store @conn) cache-token :stale :value)))
              (is (= (str payload "new")
                     (:item/value (d/entity @conn [:item/key "one"])))))))
        (is (= :transacted (d/transact-ack! conn [{:item/key "new-schema" :inferred 7}])))
        (is (= 7 (:inferred (d/entity @conn [:item/key "new-schema"]))))
        (is (contains? (d/schema conn) :inferred))
        (let [{:keys [result requests]} (trace-requests #(d/transact-ack! conn []))
              response (cop/record-response (saved-record (first requests)))]
          (is (= :transacted result))
          (is (= (:max-tx @conn) (get-in response [:db-info :max-tx]))))
        (finally (d/close conn))))))

(deftest remote-ack-replay-survives-reopen-and-rejects-conflicts
  (let [name (str (random-uuid))
        uri (str *base-uri* name)
        conn (d/create-conn uri schema {:wal? true})
        request (atom nil)
        response (atom nil)]
    (try
      (d/transact-ack! conn [{:db/id 1 :item/key "one" :counter 0}])
      (let [{:keys [requests]}
            (trace-requests #(d/transact-ack! conn [[:db.fn/call increment-counter 1]]))
            message (first requests)
            client (.-client ^DatalogStore (:store @conn))]
        (reset! request message)
        (reset! response (cop/record-response (saved-record message)))
        (is (= @response (:result (client/request client message))))
        (is (= 1 (:counter (d/entity @conn 1))))
        (doseq [changed [(assoc message :client-op-hash "different")
                         (assoc message :type :tx-data+db-info
                                        :client-op-response-kind :tx-data+db-info)]]
          (is (= :ha/client-op-conflict
                 (get-in (client/request client changed) [:err-data :error])))))
      (finally (d/close conn)))
    (let [admin (client/new-client *base-uri*)]
      (try (client/close-database admin name)
           (finally (client/disconnect admin))))
    (let [conn (d/create-conn uri)]
      (try
        (let [client (.-client ^DatalogStore (:store @conn))
              replay (client/request client @request)]
          (is (= :command-complete (:type replay)))
          (is (= @response (:result replay)))
          (is (= 1 (:counter (d/entity @conn 1)))))
        (finally (d/close conn))))))

(deftest remote-ack-streaming-replay-and-failure
  (let [conn (d/create-conn (str *base-uri* (random-uuid)) schema)
        txs (mapv #(hash-map :item/key (str %) :item/value "value")
                   (range c/+wire-datom-batch-size+))]
    (try
      (let [{:keys [result requests]} (trace-requests #(d/transact-ack! conn txs))
            message (first requests)
            client (.-client ^DatalogStore (:store @conn))
            replay (client/copy-in client message txs c/+wire-datom-batch-size+)]
        (is (= :transacted result))
        (is (= [:tx-data-ack] (mapv :type requests)))
        (is (= :copy-in (:mode message)))
        (is (= :command-complete (:type replay)))
        (is (= (cop/record-response (saved-record message)) (:result replay)))
        (let [conflict (assoc message :client-op-hash "different")]
          (is (= :ha/client-op-conflict
                 (get-in (client/copy-in client conflict txs c/+wire-datom-batch-size+)
                         [:err-data :error])))))
      (is (= (count txs) (d/count-datoms @conn nil :item/key nil)))
      (let [{:keys [result requests]}
            (trace-requests
              #(try (d/transact-ack! conn [{:item/key "discarded"}
                                          {:item/key "0"}])
                    (catch Exception e e)))]
        (is (instance? Exception result))
        (is (nil? (saved-record (first requests)))))
      (is (nil? (d/entity @conn [:item/key "discarded"])))
      (let [{:keys [result requests]}
            (trace-requests
              #(try (d/transact-ack! conn
                      [{:db/id [:item/key "0"] :item/value "failed"}
                       [:db/ensure (inter/inter-fn [_] false)]])
                    (catch Exception e e)))]
        (is (instance? Exception result))
        (is (nil? (saved-record (first requests)))))
      (is (= "value" (:item/value (d/entity @conn [:item/key "0"]))))
      (finally (d/close conn)))))

(deftest listener-registration-waits-for-an-in-flight-ack
  (let [conn (d/create-conn (str *base-uri* (random-uuid)) schema
                            {:client-opts {:pool-size 2}})
        handlers @#'server/message-handler-map
        handler (:tx-data-ack handlers)
        entered (promise)
        release (promise)
        registering (promise)
        events (atom [])
        jobs (atom [])]
    (try
      (with-redefs-fn
        {#'server/message-handler-map
         (assoc handlers :tx-data-ack
                (fn [srv key message]
                  (deliver entered true)
                  (assert (deref release 10000 false))
                  (handler srv key message)))}
        (fn []
          (let [write (future (d/transact-ack! conn [{:item/key "first"}]))]
            (swap! jobs conj write)
            (is (true? (deref entered 10000 false)))
            (let [listen (future
                           (deliver registering true)
                           (d/listen! conn :test #(swap! events conj %)))]
              (swap! jobs conj listen)
              (is (true? (deref registering 10000 false)))
              (is (= ::pending (deref listen 100 ::pending)))
              (deliver release true)
              (is (= :transacted (deref write 10000 ::timeout)))
              (is (= :test (deref listen 10000 ::timeout)))
              (is (empty? @events))))))
      (is (= :transacted (d/transact-ack! conn [{:item/key "second"}])))
      (is (= 1 (count @events)))
      (is (= ["second"] (mapv :v (:tx-data (first @events)))))
      (finally
        (deliver release true)
        (doseq [job @jobs] (deref job 10000 nil))
        (d/close conn)))))
