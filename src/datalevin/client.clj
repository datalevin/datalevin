;;
;; Copyright (c) Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns datalevin.client
  "Datalevin client to Datalevin server, blocking API, with a connection pool"
  (:require
   [datalevin.datom :as dd]
   [datalevin.util :as u]
   [datalevin.constants :as c]
   [clojure.string :as s]
   [datalevin.buffer :as bf]
   [datalevin.protocol :as p])
  (:import
   [java.nio ByteBuffer BufferOverflowException]
   [java.nio.channels SocketChannel Selector SelectionKey]
   [java.util UUID WeakHashMap Collections]
   [java.util.concurrent ConcurrentLinkedQueue ConcurrentHashMap]
   [java.net InetSocketAddress StandardSocketOptions URI]))

(defprotocol ^:no-doc IConnection
  (send-n-receive [conn msg]
    "Send a message to server and return the response, a blocking call")
  (send-only [conn msg] "Send a message without waiting for a response")
  (receive [conn] "Receive a message, a blocking call")
  (close [conn]))

(defonce ^:private ^ConcurrentHashMap connection-wire-opts
  (ConcurrentHashMap.))

(defonce ^:private ^java.util.Map ha-preferred-endpoints
  (Collections/synchronizedMap (WeakHashMap.)))

(defonce ^:private ^java.util.Map ha-retry-clients
  (Collections/synchronizedMap (WeakHashMap.)))

(defonce ^:private ^java.util.Map ha-retry-open-targets
  (Collections/synchronizedMap (WeakHashMap.)))

(defonce ^:private ^java.util.Map ha-retry-disabled-clients
  (Collections/synchronizedMap (WeakHashMap.)))

(defn- conn-wire-opts
  [^SocketChannel ch]
  (or (.get connection-wire-opts ch)
      (p/default-wire-opts)))

(defn- set-conn-wire-opts!
  [^SocketChannel ch wire-opts]
  (.put connection-wire-opts ch wire-opts))

(defn- clear-conn-wire-opts!
  [^SocketChannel ch]
  (.remove connection-wire-opts ch))

(deftype ^:no-doc Connection [^SocketChannel ch
                              ^long time-out
                              ^:volatile-mutable ^ByteBuffer bf]
  IConnection
  (send-n-receive [this msg]
    (try
      (locking bf
        (p/write-message-blocking ch bf msg (conn-wire-opts ch))
        (.clear bf)
        (let [[resp bf'] (p/receive-ch ch bf (conn-wire-opts ch) time-out)]
          (when-not (identical? bf' bf) (set! bf bf'))
          resp))
      (catch BufferOverflowException _
        (let [size (* ^long c/+buffer-grow-factor+ (.capacity bf))]
          (set! bf (bf/allocate-buffer size))
          (send-n-receive this msg)))
      (catch Exception e
        (u/raise "Error sending message and receiving response: "
                 e {:msg msg}))))

  (send-only [this msg]
    (try
      (p/write-message-blocking ch bf msg (conn-wire-opts ch))
      (catch BufferOverflowException _
        (let [size (* ^long c/+buffer-grow-factor+ (.capacity bf))]
          (set! bf (bf/allocate-buffer size))
          (send-only this msg)))
      (catch Exception e
        (u/raise "Error sending message: " e {:msg msg}))))

  (receive [this]
    (try
      (let [[resp bf'] (p/receive-ch ch bf (conn-wire-opts ch) time-out)]
        (when-not (identical? bf' bf) (set! bf bf'))
        resp)
      (catch Exception e
        (u/raise "Error receiving data:" e {}))))

  (close [this]
    (try
      (.close ch)
      (finally
        (clear-conn-wire-opts! ch)))))

(defn ^:no-doc ->Connection
  ([^SocketChannel ch ^ByteBuffer bf]
   (Connection. ch (long c/default-connection-timeout) bf))
  ([^SocketChannel ch time-out ^ByteBuffer bf]
   (Connection. ch (long time-out) bf)))

(defn- ^SocketChannel connect-socket
  "connect to server and return the client socket channel"
  [^String host port timeout-ms]
  (let [timeout-ms       (long (max 1 (long timeout-ms)))
        deadline-ms      (+ (System/currentTimeMillis) timeout-ms)
        ^SocketChannel ch (SocketChannel/open)]
    (try
      (.setOption ch StandardSocketOptions/SO_KEEPALIVE true)
      (.setOption ch StandardSocketOptions/TCP_NODELAY true)
      (.configureBlocking ch false)
      (let [address (InetSocketAddress. host ^int port)]
        (if (.connect ch address)
          (do
            (.configureBlocking ch true)
            ch)
          (let [connected?
                (with-open [^Selector selector (Selector/open)]
                  (.register ch selector SelectionKey/OP_CONNECT)
                  (loop []
                    (let [remaining-ms (- deadline-ms
                                          (System/currentTimeMillis))]
                      (when-not (pos? remaining-ms)
                        (u/raise "Unable to connect to server: timed out"
                                 {:host host
                                  :port port
                                  :timeout-ms timeout-ms
                                  :error :socket/timeout}))
                      (if (pos? (.select selector remaining-ms))
                        (do
                          (.clear (.selectedKeys selector))
                          (if (.finishConnect ch)
                            true
                            (recur)))
                        (recur)))))]
            (when connected?
              (.configureBlocking ch true)
              ch))))
      (catch Exception e
        (try
          (.close ch)
          (catch Exception _ nil))
        (u/raise "Unable to connect to server: " e
                 {:host host
                  :port port
                  :timeout-ms timeout-ms})))))

(defn- new-connection
  ([host port time-out]
   (new-connection host port time-out time-out))
  ([host port connect-time-out receive-time-out]
   (let [ch (connect-socket host port connect-time-out)]
    (set-conn-wire-opts! ch (p/default-wire-opts))
    (->Connection ch (long receive-time-out)
                  (bf/allocate-buffer c/+buffer-size+)))))

(defn- set-client-id
  [conn client-id]
  (let [{:keys [type message wire-capabilities]}
        (send-n-receive conn {:type              :set-client-id
                              :client-id         client-id
                              :wire-capabilities (p/local-wire-capabilities)})]
    (when-not (= type :set-client-id-ok) (u/raise message {}))
    (set-conn-wire-opts! (.-ch ^Connection conn)
                         (p/negotiate-wire-opts wire-capabilities))))

(defprotocol ^:no-doc IConnectionPool
  (get-connection [this] "Get a connection from the pool")
  (release-connection [this connection] "Return the connection back to pool")
  (close-pool [this])
  (closed-pool? [this]))

(deftype ^:no-doc ConnectionPool [host port client-id pool-size time-out
                                  ^ConcurrentLinkedQueue available
                                  ^ConcurrentLinkedQueue used]
  IConnectionPool
  (get-connection [this]
    (if (closed-pool? this)
      (u/raise "This client is closed" {:client-id client-id})
      (let [start (System/currentTimeMillis)]
        (loop []
          (if (.isEmpty available)
            (if (>= (- (System/currentTimeMillis) start) ^long time-out)
              (u/raise "Timeout in obtaining a connection" {})
              (do (Thread/sleep 1000)
                  (recur)))
            (let [^Connection conn (.poll available)]
              (if (.isOpen ^SocketChannel (.-ch conn))
                (do (.add used conn)
                    conn)
                (let [conn (new-connection host port time-out)]
                  (set-client-id conn client-id)
                  (.add used conn)
                  conn))))))))

  (release-connection [this conn]
    (locking this
      (when (.contains used conn)
        (.remove used conn)
        (.add available conn))))

  (close-pool [this]
    (dotimes [_ (.size used)] (close ^Connection (.poll used)))
    (.clear used)
    (dotimes [_ (.size available)] (close ^Connection (.poll available)))
    (.clear available))

  (closed-pool? [this]
    (and (.isEmpty used) (.isEmpty available))))

(defn- authenticate
  "Send an authenticate message to server, and wait to receive the response.
  If authentication succeeds,  return a client id.
  Otherwise, close connection, raise exception"
  [host port username password time-out]
  (let [conn (new-connection host
                             port
                             time-out
                             (max (long time-out)
                                  (long c/default-connection-timeout)))

        {:keys [type client-id message]}
        (send-n-receive conn {:type     :authentication
                              :username username
                              :password password})]
    (close conn)
    (if (= type :authentication-ok)
      client-id
      (u/raise "Authentication failure: " message {}))))

(defn- new-connectionpool
  [host port client-id pool-size time-out]
  (assert (> ^long pool-size 0)
          "Number of connections must be greater than zero")
  (let [^ConnectionPool pool             (->ConnectionPool
                                           host port client-id
                                           pool-size time-out
                                           (ConcurrentLinkedQueue.)
                                           (ConcurrentLinkedQueue.))
        ^ConcurrentLinkedQueue available (.-available pool)]
    (dotimes [_ pool-size]
      (let [conn (new-connection host port time-out)]
        (set-client-id conn client-id)
        (.add available conn)))
    pool))

(defprotocol ^:no-doc IClient
  (request [client req]
    "Send a request to server and return the response. The response could
     also initiate a copy out")
  (copy-in [client req data batch-size]
    "Copy data to the server. `req` is a request type message,
     `data` is a sequence, `batch-size` decides how to partition the data
      so that each batch fits in buffers along the way. The response could
      also initiate a copy out")
  (disconnect [client])
  (disconnected? [client])
  (get-pool [client])
  (get-id [client]))

(defn ^:no-doc parse-user-info
  [^URI uri]
  (when-let [user-info (.getUserInfo uri)]
    (let [idx (.indexOf user-info ":")]
      (when (and (pos? idx) (< idx (dec (count user-info))))
        {:username (subs user-info 0 idx)
         :password (subs user-info (inc idx))}))))

(def ^:dynamic *default-port*
  c/default-port)

(defn ^:no-doc parse-port
  [^URI uri]
  (let [p (.getPort uri)] (if (= -1 p) *default-port* p)))

(defn ^:no-doc parse-db
  "Extract the identifier of database from URI. A database is uniquely
  identified by its name (after being converted to its kebab case)."
  [^URI uri]
  (let [path (.getPath uri)]
    (when-not (or (s/blank? path) (= path "/"))
      (u/lisp-case (subs path 1)))))

(defn ^:no-doc parse-query
  [^URI uri]
  (when-let [query (.getQuery uri)]
    (->> (s/split query #"&")
         (map #(s/split % #"="))
         (into {}))))

(def ^:private ha-endpoint-pattern
  #"^([^:]+):(\d+)$")

(defn- parse-ha-endpoint
  [endpoint]
  (when-let [[_ host port-str]
             (and (string? endpoint)
                  (re-matches ha-endpoint-pattern endpoint))]
    {:endpoint endpoint
     :host     host
     :port     (Long/parseLong port-str)}))

(defn- copy-out
  ([conn req]
   (copy-out conn req nil))
  ([conn req copy-out-response]
   (try
     (let [data (transient [])]
       (loop []
         (let [msg (receive conn)]
           (if (map? msg)
             (let [{:keys [type]} msg]
               (if (= type :copy-done)
                 (merge
                   {:type :command-complete
                    :result (persistent! data)}
                   (when (map? copy-out-response)
                     (dissoc copy-out-response :type))
                   (dissoc msg :type))
                 (u/raise "Server error while copying out data" {:msg msg})))
             (do (doseq [d msg] (conj! data d))
                 (recur))))))
     (catch Exception e
       (u/raise "Unable to receive copy:" e {:req req})))))

(defn- copy-in*
  [conn req data batch-size ]
  (try
    (doseq [batch (partition batch-size batch-size nil data)]
      (send-only conn batch))
    (let [{:keys [type] :as result} (send-n-receive conn {:type :copy-done})]
      (if (= type :copy-out-response)
        (copy-out conn req result)
        result))
    (catch Exception e
      (send-n-receive conn {:type :copy-fail})
      (u/raise "Unable to copy in:" e
               {:req req :count (count data)}))))

(declare open-database disconnect-retry-clients!)

(deftype ^:no-doc Client [username password host port pool-size time-out
                          ^:volatile-mutable ^UUID id
                          ^:volatile-mutable ^ConnectionPool pool]
  IClient
  (request [client req]
    (let [success? (volatile! false)
          start    (System/currentTimeMillis)]
      (loop []
        (let [^ConnectionPool pool' pool
              conn                 (get-connection pool')
              response             (try
                                     (send-n-receive conn req)
                                     (catch Exception _
                                       (close conn)
                                       nil))
              res                  (try
                                     (when-let [{:keys [type] :as result}
                                                response]
                                       (vreset! success? true)
                                       (case type
                                         :copy-out-response (copy-out conn req result)
                                         :command-complete  result
                                         :error-response    result
                                         :reopen
                                         (let [{:keys [db-name db-type]} result]
                                           (vreset! success? false)
                                           {:request-status :reopen
                                            :db-name        db-name
                                            :db-type        db-type})
                                         :reconnect
                                         (let [client-id
                                               (authenticate host port username
                                                             password
                                                             time-out)]
                                           (close conn)
                                           (vreset! success? false)
                                           {:request-status :reconnect
                                            :client-id      client-id})))
                                     (finally
                                       (release-connection pool' conn)))
              res'                 (case (:request-status res)
                                     :reconnect
                                     (let [client-id (:client-id res)]
                                       (set! id client-id)
                                       (set! pool (new-connectionpool
                                                    host port client-id
                                                    pool-size time-out))
                                       nil)

                                     :reopen
                                     (let [{:keys [db-name db-type]} res]
                                       (open-database client db-name db-type)
                                       nil)

                                     res)]
          (if (>= (- (System/currentTimeMillis) start)
                  ^long (.-time-out pool'))
            (u/raise "Timeout in making request" {})
            (if @success?
              res'
              (recur)))))))

  (copy-in [client req data batch-size]
    (let [conn (get-connection pool)]
      (try
        (let [{:keys [type]} (send-n-receive conn req)]
          (if (= type :copy-in-response)
            (copy-in* conn req data batch-size)
            (u/raise "Server refuses to accept copy in" {:req req})))
        (finally (release-connection pool conn)))))

  (disconnect [client]
    (try
      (let [conn (get-connection pool)]
        (send-only conn {:type :disconnect})
        (release-connection pool conn))
      (finally
        (.remove ha-retry-disabled-clients client)
        (.remove ha-preferred-endpoints client)
        (disconnect-retry-clients! client disconnect)))
    (close-pool pool))

  (disconnected? [client]
    (closed-pool? pool))

  (get-pool [client] pool)

  (get-id [client] id))

(defn open-database
  "Open a database on server. `db-type` can be \"datalog\", \"kv\",
  or \"engine\""
  ([client db-name db-type]
   (open-database client db-name db-type nil nil false))
  ([client db-name db-type opts]
   (open-database client db-name db-type nil opts false))
  ([client db-name db-type schema opts]
   (open-database client db-name db-type schema opts false))
  ([client db-name db-type schema opts return-db-info?]
   (let [{:keys [type message result]}
         (request client
                  (cond
                    (= db-type c/db-store-kv)
                    {:type :open-kv :db-name db-name :opts opts}
                    (= db-type c/db-store-datalog)
                    (cond-> {:type :open :db-name db-name}
                      schema (assoc :schema schema)
                      opts   (assoc :opts (assoc opts :db-name db-name))
                      return-db-info? (assoc :return-db-info? true))
                    :else
                    {:type :new-search-engine :db-name db-name :opts opts}))]
     (when (= type :error-response)
       (u/raise "Unable to open database:" db-name " " message
                {:db-type db-type}))
     (when return-db-info?
       result))))

(defn new-client
  "Create a new client that maintains pooled connections to a remote
  Datalevin database server. This operation takes at least 0.5 seconds
  in order to perform a secure password hashing that defeats cracking.

  Fields in the `uri-str` should be properly URL encoded, e.g. user and
  password need to be URL encoded if they contain special characters.

  The following can be set in the optional map:
  * `:pool-size` determines number of connections maintained in the connection
  pool, default is 3.
  * `:time-out` specifies the time (milliseconds) before an exception is thrown
  when obtaining an open network connection, default is 60000."
  ([uri-str]
   (new-client uri-str {:pool-size c/default-connection-pool-size
                        :time-out  c/default-connection-timeout}))
  ([uri-str {:keys [pool-size time-out]
             :or   {pool-size c/default-connection-pool-size
                    time-out  c/default-connection-timeout}}]
   (let [uri                         (URI. uri-str)
         {:keys [username password]} (parse-user-info uri)

         host      (.getHost uri)
         port      (parse-port uri)
         client-id (authenticate host port username password time-out)
         pool      (new-connectionpool host port client-id pool-size time-out)]
     (->Client username password host port pool-size time-out
	               client-id pool))))

(defn ^:no-doc dedicated-transaction-client
  [client]
  (if (instance? Client client)
    (let [^Client client client]
      (if (= 1 (.-pool-size client))
        client
        (let [username  (.-username client)
              password  (.-password client)
              host      (.-host client)
              port      (.-port client)
              time-out  (.-time-out client)
              client-id (authenticate host port username password time-out)
              pool      (new-connectionpool host port client-id 1 time-out)]
          (->Client username password host port 1 time-out
                    client-id pool))))
    client))

(defn- endpoint-key
  [host port]
  (str host ":" port))

(defn- retryable-ha-write-reject?
  [err-data]
  (and (map? err-data)
       (= :ha/write-rejected (:error err-data))
       (true? (:retryable? err-data))))

(defn ^:no-doc disable-ha-write-retry!
  [client]
  (when client
    (.put ha-retry-disabled-clients client true))
  client)

(defn ^:no-doc enable-ha-write-retry!
  [client]
  (when client
    (.remove ha-retry-disabled-clients client))
  client)

(defn- ha-write-retry-disabled?
  [client]
  (boolean
    (and client
         (.get ha-retry-disabled-clients client))))

(defn- sanitize-error-data
  [x]
  (cond
    (dd/datom? x)
    {:e     (dd/datom-e x)
     :a     (dd/datom-a x)
     :v     (dd/datom-v x)
     :tx    (dd/datom-tx x)
     :added (dd/datom-added x)}

    (map? x)
    (into {}
          (map (fn [[k v]]
                 [(sanitize-error-data k)
                  (sanitize-error-data v)]))
          x)

    (instance? java.util.Map x)
    (sanitize-error-data (into {} x))

    (vector? x)
    (mapv sanitize-error-data x)

    (set? x)
    (into #{} (map sanitize-error-data) x)

    (sequential? x)
    (mapv sanitize-error-data x)

    (instance? java.util.Collection x)
    (mapv sanitize-error-data x)

    :else
    x))

(defn- raise-normal-request-error
  [req message err-data extra-data]
  (u/raise "Request to Datalevin server failed: "
           message
           (merge req
                  {:err-data (sanitize-error-data err-data)
                   :server-message message}
                  extra-data)))

(defn- collect-ha-retry-endpoints
  [seen endpoints]
  (reduce
    (fn [[acc seen'] endpoint]
      (if-let [{:keys [host port] :as parsed} (parse-ha-endpoint endpoint)]
        (let [ek (endpoint-key host port)]
          (if (contains? seen' ek)
            [acc seen']
            [(conj acc parsed) (conj seen' ek)]))
        [acc seen']))
    [[] seen]
    endpoints))

(defn- client-routing-context
  [client]
  (when (instance? Client client)
    {:username  (.-username ^Client client)
     :password  (.-password ^Client client)
     :pool-size (.-pool-size ^Client client)
     :time-out  (.-time-out ^Client client)
     :host      (.-host ^Client client)
     :port      (.-port ^Client client)
     :client    client}))

(defn- ^:redef client-retry-context
  [client]
  (when-not (ha-write-retry-disabled? client)
    (client-routing-context client)))

(defn- read-preferred-ha-endpoint
  [client]
  (let [endpoint (.get ha-preferred-endpoints client)]
    (when (and (string? endpoint) (not (s/blank? endpoint)))
      endpoint)))

(defn- set-preferred-ha-endpoint!
  [client endpoint]
  (if (and (some? endpoint) (string? endpoint) (not (s/blank? endpoint)))
    (.put ha-preferred-endpoints client endpoint)
    (.remove ha-preferred-endpoints client)))

(defn- clear-preferred-ha-endpoint!
  [client]
  (set-preferred-ha-endpoint! client nil))

(defn- preferred-ha-endpoint
  [client retry-context]
  (let [self-endpoint (endpoint-key (:host retry-context) (:port retry-context))
        endpoint      (read-preferred-ha-endpoint client)]
    (when (and endpoint (not= endpoint self-endpoint))
      endpoint)))

(defn- new-client-for-endpoint
  [{:keys [username password pool-size time-out]} host port]
  (let [client-id (authenticate host port username password time-out)
        pool      (new-connectionpool host port client-id pool-size time-out)]
    (->Client username password host port pool-size time-out
              client-id pool)))

(def ^:private ha-kv-retry-request-types
  #{:transact-kv
    :open-transact-kv
    :close-transact-kv
    :abort-transact-kv
    :open-dbi
    :clear-dbi
    :drop-dbi
    :kv-re-index})

(def ^:private ha-datalog-retry-request-types
  #{:assoc-opt
    :set-schema
    :init-max-eid
    :del-attr
    :rename-attr
    :load-datoms
    :tx-data
    :tx-data+db-info
    :open-transact
    :close-transact
    :abort-transact
    :datalog-re-index})

(def ^:private ha-engine-retry-request-types
  #{:add-doc
    :remove-doc
    :clear-docs
    :search-re-index})

(defn- request-db-type
  [req]
  (or (:db-type req)
      (let [req-type (:type req)]
        (cond
          (contains? ha-kv-retry-request-types req-type)
          c/db-store-kv

          (contains? ha-datalog-retry-request-types req-type)
          c/db-store-datalog

          (contains? ha-engine-retry-request-types req-type)
          "engine"

          :else nil))))

(defn- request-db-target
  [req]
  (when-let [db-type (request-db-type req)]
    (let [db-name (or (:db-name req) (first (:args req)))]
      (when (string? db-name)
        [db-name db-type]))))

(defn- ^java.util.Set retry-client-open-target-set
  [retry-client]
  (locking ha-retry-open-targets
    (or (.get ha-retry-open-targets retry-client)
        (let [targets (Collections/newSetFromMap (ConcurrentHashMap.))]
          (.put ha-retry-open-targets retry-client targets)
          targets))))

(defn- clear-retry-client-open-targets!
  [retry-client]
  (.remove ha-retry-open-targets retry-client))

(defn- ensure-retry-client-open!
  [retry-client req]
  (when (satisfies? IClient retry-client)
    (when-let [[db-name db-type :as target] (request-db-target req)]
      (let [targets (retry-client-open-target-set retry-client)]
        (when (.add targets target)
          (try
            (open-database retry-client db-name db-type)
            (catch Exception e
              (.remove targets target)
              (throw e)))))))
  retry-client)

(defn- ^ConcurrentHashMap retry-client-cache
  [client]
  (locking ha-retry-clients
    (or (.get ha-retry-clients client)
        (let [cache (ConcurrentHashMap.)]
          (.put ha-retry-clients client cache)
          cache))))

(defn- cached-retry-client
  [client endpoint]
  (when-let [^ConcurrentHashMap cache (.get ha-retry-clients client)]
    (.get cache endpoint)))

(defn- cache-retry-client!
  [client endpoint retry-client]
  (let [^ConcurrentHashMap cache (retry-client-cache client)]
    (.put cache endpoint retry-client))
  retry-client)

(defn- retry-client-disconnected?
  [retry-client]
  (and retry-client
       (satisfies? IClient retry-client)
       (disconnected? retry-client)))

(defn- safe-disconnect-retry-client!
  [retry-client disconnect-fn]
  (when retry-client
    (clear-retry-client-open-targets! retry-client)
    (try
      (disconnect-fn retry-client)
      (catch Exception _ nil))))

(defn- evict-retry-client!
  [client endpoint disconnect-fn]
  (when client
    (when-let [^ConcurrentHashMap cache (.get ha-retry-clients client)]
      (when-let [retry-client (.remove cache endpoint)]
        (safe-disconnect-retry-client! retry-client disconnect-fn)))))

(defn- disconnect-retry-clients!
  [client disconnect-fn]
  (when-let [cache (.remove ha-retry-clients client)]
    (doseq [retry-client (.values ^ConcurrentHashMap cache)]
      (safe-disconnect-retry-client! retry-client disconnect-fn))
    (.clear ^ConcurrentHashMap cache)))

(defn- prepare-retry-client
  [req retry-context host port disconnect-fn new-client-fn]
  (let [endpoint    (endpoint-key host port)
        base-client (:client retry-context)]
    (if base-client
      (locking (retry-client-cache base-client)
        (if-let [cached (cached-retry-client base-client endpoint)]
          (if (retry-client-disconnected? cached)
            (do
              (evict-retry-client! base-client endpoint disconnect-fn)
              (let [retry-client (-> (new-client-fn retry-context host port)
                                     (ensure-retry-client-open! req))]
                (cache-retry-client! base-client endpoint retry-client)
                {:client retry-client
                 :cached? true
                 :endpoint endpoint
                 :base-client base-client}))
            {:client (ensure-retry-client-open! cached req)
             :cached? true
             :endpoint endpoint
             :base-client base-client})
          (let [retry-client (-> (new-client-fn retry-context host port)
                                 (ensure-retry-client-open! req))]
            (cache-retry-client! base-client endpoint retry-client)
            {:client retry-client
             :cached? true
             :endpoint endpoint
             :base-client base-client})))
      {:client (-> (new-client-fn retry-context host port)
                   (ensure-retry-client-open! req))
       :cached? false
       :endpoint endpoint
       :base-client nil})))

(defn- attempt-ha-endpoint-request
  [req retry-context host port request-fn disconnect-fn new-client-fn]
  (try
    (let [{:keys [client cached? endpoint base-client]}
          (prepare-retry-client
            req retry-context host port disconnect-fn new-client-fn)]
      (try
        (let [{:keys [type message result err-data]}
              (request-fn client req)]
          (if (= type :error-response)
            {:kind :error
             :message message
             :err-data err-data}
            {:kind :success
             :result result}))
        (catch Exception e
          (when cached?
            (evict-retry-client! base-client endpoint disconnect-fn))
          (throw e))
        (finally
          (when-not cached?
            (safe-disconnect-retry-client! client disconnect-fn)))))
    (catch Exception e
      {:kind :exception
       :exception e})))

(defn- retry-ha-write-request*
  ([req message err-data retry-context request-fn disconnect-fn new-client-fn]
   (retry-ha-write-request*
     req
     message
     err-data
     retry-context
     request-fn
     disconnect-fn
     new-client-fn
     (constantly nil)))
  ([req message err-data retry-context request-fn disconnect-fn new-client-fn
    on-success-endpoint!]
   (let [self-key (endpoint-key (:host retry-context) (:port retry-context))
         [pending seen]
         (collect-ha-retry-endpoints
           #{self-key}
           (:ha-retry-endpoints err-data))]
     (loop [remaining    pending
            seen         seen
            last-message message
            last-err     err-data
            attempts     []]
       (if-let [{:keys [endpoint host port]} (first remaining)]
         (let [outcome (attempt-ha-endpoint-request
                         req retry-context host port
                         request-fn disconnect-fn new-client-fn)]
           (cond
             (= :success (:kind outcome))
             (do
               (on-success-endpoint! endpoint)
               (:result outcome))

             (= :exception (:kind outcome))
             (recur (rest remaining)
                    seen
                    last-message
                    last-err
                    (conj attempts
                          {:endpoint endpoint
                           :type :exception
                           :message (ex-message (:exception outcome))}))

             :else
             (let [retry-err     (:err-data outcome)
                   retry-message (:message outcome)]
               (if (retryable-ha-write-reject? retry-err)
                 (let [[extra seen']
                       (collect-ha-retry-endpoints
                         seen
                         (:ha-retry-endpoints retry-err))]
                   (recur (concat (rest remaining) extra)
                          seen'
                          retry-message
                          retry-err
                          (conj attempts
                                {:endpoint endpoint
                                 :type :error-response
                                 :reason (:reason retry-err)})))
                 (raise-normal-request-error
                   req retry-message retry-err
                   {:ha-retry-attempts
                    (conj attempts
                          {:endpoint endpoint
                           :type :error-response
                           :reason (:reason retry-err)})})))))
        (raise-normal-request-error req last-message last-err
                                    {:ha-retry-attempts attempts}))))))

(defn- ^:redef try-preferred-ha-write-request*
  [client req retry-context request-fn disconnect-fn new-client-fn retry-fn]
  (when-let [endpoint (preferred-ha-endpoint client retry-context)]
    (if-let [{:keys [host port]} (parse-ha-endpoint endpoint)]
      (let [outcome (attempt-ha-endpoint-request
                      req retry-context host port
                      request-fn disconnect-fn new-client-fn)]
        (case (:kind outcome)
          :success
          (do
            (set-preferred-ha-endpoint! client endpoint)
            {:handled? true
             :result (:result outcome)})

          :error
          (if (retryable-ha-write-reject? (:err-data outcome))
            {:handled? true
             :result (retry-fn client req
                               (:message outcome)
                               (:err-data outcome))}
            (raise-normal-request-error
              req (:message outcome) (:err-data outcome) nil))

          :exception
          (do
            (clear-preferred-ha-endpoint! client)
            {:handled? false})))
      (do
        (clear-preferred-ha-endpoint! client)
        {:handled? false}))))

(defn- ^:redef retry-ha-write-request
  [client req message err-data]
  (if-let [retry-context (client-retry-context client)]
    (retry-ha-write-request*
      req
      message
      err-data
      retry-context
      request
      disconnect
      new-client-for-endpoint
      #(set-preferred-ha-endpoint! client %))
    (raise-normal-request-error req message err-data nil)))

(defn ^:no-doc normal-request
  "Send request to server and returns results. Does not use the
  copy-in protocol. `call` is a keyword, `args` is a vector,
  `writing?` is a boolean indicating if write-txn should be used"
  ([client call args]
   (normal-request client call args false))
  ([client call args writing?]
   (let [req                 {:type call :args args :writing? writing?}
         routing-context     (and writing? (client-routing-context client))
         retry-context       (and writing? (client-retry-context client))
         preferred-endpoint  (and writing?
                                 (not retry-context)
                                 (read-preferred-ha-endpoint client))
         preferred-attempt   (when retry-context
                               (try-preferred-ha-write-request*
                                 client
                                 req
                                 retry-context
                                 request
                                 disconnect
                                 new-client-for-endpoint
                                 retry-ha-write-request))]
     (if (:handled? preferred-attempt)
       (:result preferred-attempt)
       (if (and preferred-endpoint routing-context)
         (let [{:keys [host port]} (parse-ha-endpoint preferred-endpoint)
               outcome (and host port
                            (attempt-ha-endpoint-request
                              req
                              routing-context
                              host
                              port
                              request
                              disconnect
                              new-client-for-endpoint))]
           (case (:kind outcome)
             :success
             (:result outcome)

             :error
             (raise-normal-request-error
               req
               (:message outcome)
               (:err-data outcome)
               {:ha-pinned-endpoint preferred-endpoint})

             :exception
             (raise-normal-request-error
               req
               (or (some-> outcome :exception ex-message)
                   "Pinned HA request failed")
               {:error :ha/pinned-request-failed
                :endpoint preferred-endpoint}
               nil)

             (let [{:keys [type message result err-data]} (request client req)]
               (if (= type :error-response)
                 (raise-normal-request-error req message err-data nil)
                 result))))
         (let [{:keys [type message result err-data]} (request client req)]
           (if (= type :error-response)
             (if (and writing?
                      (retryable-ha-write-reject? err-data))
               (retry-ha-write-request client req message err-data)
               (raise-normal-request-error req message err-data nil))
             (do
               (when retry-context
                 (clear-preferred-ha-endpoint! client))
               result))))))))

;; we do input validation and normalization in the server, as
;; 3rd party clients may be written

(defn create-user
  "Create a user that can login. `username` will be converted to Kebab case
  (i.e. all lower case and words connected with dashes)."
  [client username password]
  (normal-request client :create-user [username password]))

(defn reset-password
  "Reset a user's password."
  [client username password]
  (normal-request client :reset-password [username password]))

(defn drop-user
  "Delete a user."
  [client username]
  (normal-request client :drop-user [username]))

(defn list-users
  "List all users."
  [client]
  (normal-request client :list-users []))

(defn create-role
  "Create a role. `role-key` is a keyword."
  [client role-key]
  (normal-request client :create-role [role-key]))

(defn drop-role
  "Delete a role. `role-key` is a keyword."
  [client role-key]
  (normal-request client :drop-role [role-key]))

(defn list-roles
  "List all roles."
  [client]
  (normal-request client :list-roles []))

(defn create-database
  "Create a database. `db-type` can be `:datalog` or `:key-value`.
  `db-name` will be converted to Kebab case (i.e. all lower case and
  words connected with dashes)."
  [client db-name db-type]
  (normal-request client :create-database [db-name db-type]))

(defn close-database
  "Force close a database. Connected clients that are using it
  will be disconnected.

  See [[disconnect-client]]"
  [client db-name]
  (normal-request client :close-database [db-name]))

(defn drop-database
  "Delete a database. May not be successful if currently in use.

  See [[close-database]]"
  [client db-name]
  (normal-request client :drop-database [db-name]))

(defn list-databases
  "List all databases."
  [client]
  (normal-request client :list-databases []))

(defn list-databases-in-use
  "List databases that are in use."
  [client]
  (normal-request client :list-databases-in-use []))

(defn assign-role
  "Assign a role to a user. "
  [client role-key username]
  (normal-request client :assign-role [role-key username]))

(defn withdraw-role
  "Withdraw a role from a user. "
  [client role-key username]
  (normal-request client :withdraw-role [role-key username]))

(defn list-user-roles
  "List the roles assigned to a user. "
  [client username]
  (normal-request client :list-user-roles [username]))

(defn grant-permission
  "Grant a permission to a role.

  `perm-act` indicates the permitted action. It can be one of
  `:datalevin.server/view`, `:datalevin.server/alter`,
  `:datalevin.server/create`, or `:datalevin.server/control`, with each
  subsumes the former.

  `perm-obj` indicates the object type of the securable. It can be one of
  `:datalevin.server/database`, `:datalevin.server/user`,
  `:datalevin.server/role`, or `:datalevin.server/server`, where the last one
  subsumes all the others.

  `perm-tgt` indicate the concrete securable target. It can be a database name,
  a username, or a role key, depending on `perm-obj`. If it is `nil`, the
  permission applies to all securables in that object type."
  [client role-key perm-act perm-obj perm-tgt]
  (normal-request client :grant-permission
                  [role-key perm-act perm-obj perm-tgt]))

(defn revoke-permission
  "Revoke a permission from a role.

  See [[grant-permission]]."
  [client role-key perm-act perm-obj perm-tgt]
  (normal-request client :revoke-permission
                  [role-key perm-act perm-obj perm-tgt]))

(defn list-role-permissions
  "List the permissions granted to a role.

  See [[grant-permission]]."
  [client role-key]
  (normal-request client :list-role-permissions [role-key]))

(defn list-user-permissions
  "List the permissions granted to a user through the roles assigned."
  [client username]
  (normal-request client :list-user-permissions [username]))

(defn query-system
  "Issue arbitrary Datalog query to the system database on the server.
  Note that unlike `q` function, the arguments here should NOT include db,
  as the server will supply it."
  [client query & arguments]
  (normal-request client :query-system [query arguments]))

(defn show-clients
  "Show information about the currently connected clients on the server."
  [client]
  (normal-request client :show-clients []))

(defn disconnect-client
  "Force disconnect a client from the server."
  [client client-id]
  (assert (instance? UUID client-id) "")
  (normal-request client :disconnect-client [client-id]))
