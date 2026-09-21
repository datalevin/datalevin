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
   [datalevin.util :as u :refer [raise]]
   [datalevin.constants :as c]
   [datalevin.command :as cmd]
   [datalevin.native-value :as nv]
   [datalevin.udf :as udf]
   [clojure.string :as s]
   [datalevin.buffer :as bf]
   [datalevin.protocol :as p]
   [datalevin.protocol.context :as context]
   [datalevin.prepared :as prepared])
  (:import
   [java.nio ByteBuffer BufferOverflowException]
   [java.nio.channels SocketChannel Selector SelectionKey]
   [java.util UUID WeakHashMap Collections LinkedHashMap]
   [datalevin.prepared Request]
   [java.util.concurrent ConcurrentHashMap]
   [java.util.concurrent.atomic AtomicBoolean AtomicInteger AtomicReference]
   [java.net InetSocketAddress StandardSocketOptions URI]))

(defprotocol ^:no-doc IConnection
  (send-n-receive [conn msg]
    "Send a message to server and return the response, a blocking call")
  (send-only [conn msg] "Send a message without waiting for a response")
  (receive [conn] "Receive a message, a blocking call")
  (close [conn]))

(definterface IConnectionIO
  (exchange [message wire-options]))

(defonce ^:private ^ConcurrentHashMap connection-wire-opts
  ;; Lifecycle/reset registry only. I/O reads the connection's own reference.
  (ConcurrentHashMap.))

(deftype ^:no-doc ClientState [^AtomicReference readers
                               ^ConcurrentHashMap read-endpoints
                               ^AtomicReference endpoint])

(definterface IClientInternals
  (^datalevin.client.ClientState clientState [])
  (requestBound [req]))

;; These weak registries support reset-client-state! and alternate IClient
;; implementations. A Client reads its own state without consulting either.
(defonce ^:private ^java.util.Map client-state-refs
  (Collections/synchronizedMap (WeakHashMap.)))

(defonce ^:private ^java.util.Map fallback-client-states
  (Collections/synchronizedMap (WeakHashMap.)))

(defn- new-client-state
  []
  (let [state (ClientState. (AtomicReference.) (ConcurrentHashMap.)
                            (AtomicReference.))]
    (.put client-state-refs state true)
    state))

(defn- client-state ^ClientState
  [client]
  (when client
    (if (instance? IClientInternals client)
      (.clientState ^IClientInternals client)
      (locking fallback-client-states
        (or (.get fallback-client-states client)
            (let [state (new-client-state)]
              (.put fallback-client-states client state)
              state))))))

(defn- native-reader-cache ^ConcurrentHashMap
  [client]
  (let [^AtomicReference ref (.-readers (client-state client))]
    (or (.get ref)
        (let [readers (ConcurrentHashMap.)]
          (if (.compareAndSet ref nil readers) readers (.get ref))))))

(defonce ^:private ^java.util.Map ha-retry-clients
  (Collections/synchronizedMap (WeakHashMap.)))

(defonce ^:private ^java.util.Map ha-known-db-endpoints
  (Collections/synchronizedMap (WeakHashMap.)))

(defonce ^:private ^java.util.Map ha-retry-open-targets
  (Collections/synchronizedMap (WeakHashMap.)))

(defonce ^:private ^java.util.Map ha-retry-disabled-clients
  (Collections/synchronizedMap (WeakHashMap.)))

(defonce ^:private ^java.util.Map ha-write-retry-settings
  (Collections/synchronizedMap (WeakHashMap.)))

(defonce ^:private ^java.util.Map index-open-options
  (Collections/synchronizedMap (WeakHashMap.)))

(defn reset-client-state!
  "Clear the process-local client and HA endpoint caches. Intended for tests
  that need a clean slate or that exercise reconnect/retry behavior."
  []
  (doseq [^AtomicReference options (.values connection-wire-opts)]
    (.set options (p/default-wire-opts)))
  (.clear connection-wire-opts)
  (locking client-state-refs
    (doseq [^ClientState state (.keySet client-state-refs)]
      (.set ^AtomicReference (.-readers state) nil)
      (.clear ^ConcurrentHashMap (.-read-endpoints state))
      (.set ^AtomicReference (.-endpoint state) nil)))
  (doseq [^java.util.Map m [fallback-client-states ha-retry-clients
                            ha-known-db-endpoints
                            ha-retry-open-targets ha-retry-disabled-clients
                            ha-write-retry-settings
                            index-open-options]]
    (.clear m))
  nil)

(defn- request-native-reader [client req]
  (when-let [readers (.get ^AtomicReference (.-readers (client-state client)))]
    (get readers (or (:db-name req) (first (:args req))))))

(defn- inherit-native-readers! [client source]
  (when source
    (.set ^AtomicReference (.-readers (client-state client))
          (native-reader-cache source)))
  client)

(defn- inherit-index-options! [client source]
  (when source
    (locking index-open-options
      (let [options (or (.get index-open-options source) (ConcurrentHashMap.))]
        (.put index-open-options source options)
        (.put index-open-options client options))))
  client)

(defn- remember-index-options! [client call args]
  (when-let [db-type (case call
                      (:new-search-engine :search-re-index) "engine"
                      (:new-vector-index :vec-re-index) "index"
                      nil)]
    (inherit-index-options! client client)
    (let [^ConcurrentHashMap options (.get index-open-options client)]
      (.put options [(first args) db-type] (or (second args) {})))))

(def ^:dynamic *ha-read-min-tx*
  "Minimum datalog tx a remote HA read must observe."
  nil)

(declare read-preferred-ha-endpoint
         set-preferred-ha-endpoint!
         clear-preferred-ha-endpoint!
         preferred-ha-read-endpoint
         known-ha-db-endpoints
         cache-known-ha-db-endpoints!
         sync-ha-routing!
         cached-retry-client
         retry-client-disconnected?
         evict-retry-client!
         cleanup-after-failure!
         disconnect)

(defn- clear-conn-wire-opts!
  [^SocketChannel ch]
  (.remove connection-wire-opts ch))

(deftype ^:no-doc Connection [^SocketChannel ch
                              ^long time-out
                              ^:volatile-mutable ^ByteBuffer bf
                              read-selector
                              context
                              ^ByteBuffer probe-bf
                              ^AtomicReference wire-options
                              receive-buffer
                              ^LinkedHashMap prepared-handles]
  IConnectionIO
  (exchange [_ msg wire-opts]
    ;; The caller owns the connection monitor. Grow only before sending.
    (context/with-context context
      (loop []
        (when-not (try
                    (p/write-message-owned ch bf msg wire-opts)
                    true
                    (catch BufferOverflowException _ false))
          (set! bf (bf/allocate-buffer
                     (* ^long c/+buffer-grow-factor+ (.capacity bf))))
          (recur)))
      (.clear bf)
      (vreset! receive-buffer bf)
      (try
        (p/receive-ch! ch receive-buffer wire-opts time-out
                      (when-not (.isBlocking ch) read-selector))
        (finally (set! bf @receive-buffer)))))

  IConnection
  (send-n-receive [this msg]
    (locking this
      (try
        (let [wire-opts (.get wire-options)
              prepared-id (::prepared/id (meta msg))
              enabled? (and prepared-id (:prepared-read? wire-opts)
                            (or (not= :q (:type msg)) (:prepared-query? wire-opts)))
              id (when enabled? prepared-id)]
          (loop [register? (and enabled? (nil? (.get prepared-handles id)))]
            (let [wire-msg
                  (if enabled?
                    (if register?
                      (with-meta (assoc msg :prepare-id id) nil)
                      (cond-> {:type :execute-prepared :handle id
                               :value (nth (:args msg) 2)
                               :writing? (:writing? msg)}
                        (:ha-read-min-tx msg)
                        (assoc :ha-read-min-tx (:ha-read-min-tx msg))))
                    (if prepared-id (with-meta msg nil) msg))
                  response (.exchange ^IConnectionIO this wire-msg wire-opts)]
              (if (and enabled? (not register?)
                       (= :error-response (:type response))
                       (= :prepared/missing (get-in response [:err-data :error])))
                (do (.remove prepared-handles id) (recur true))
                (do
                  (when (and register?
                             (#{:command-complete :copy-out-response} (:type response)))
                    (prepared/remember! prepared-handles id true))
                  response)))))
        (catch Exception e
          (when (nv/decoding-error? e) (throw e))
          (raise "Error sending message and receiving response: "
                 e {:msg msg})))))

  (send-only [this msg]
    (locking this
      (try
        (context/with-context context
          (loop []
            (when-not (try
                        (p/write-message-owned ch bf msg (.get wire-options))
                        true
                        (catch BufferOverflowException _ false))
              (set! bf (bf/allocate-buffer
                         (* ^long c/+buffer-grow-factor+ (.capacity bf))))
              (recur))))
        (catch Exception e
          (raise "Error sending message: " e {:msg msg})))))

  (receive [this]
    (try
      (locking this
        (context/with-context context
          (vreset! receive-buffer bf)
          (try
            (p/receive-ch! ch receive-buffer (.get wire-options) time-out
                          (when-not (.isBlocking ch) read-selector))
            (finally (set! bf @receive-buffer)))))
      (catch Exception e
        (when (nv/decoding-error? e) (throw e))
        (raise "Error receiving data:" e {}))))

  (close [this]
    ;; Closing the channel wakes an in-flight read. Serialize only lazy
    ;; selector creation/closure, never the blocking receive itself.
    (try
      (locking read-selector
        (let [^Selector selector @read-selector]
          (try
            (try
              (.close ch)
              (catch Throwable t
                (when selector
                  (cleanup-after-failure! t #(.close selector)))
                (throw t)))
            (when selector (.close selector))
            (finally
              (vreset! read-selector nil)
              (clear-conn-wire-opts! ch)))))
      (finally
        (locking this (.close ^java.io.Closeable context))))))

#_{:clj-kondo/ignore [:redefined-var]}
(defn ^:no-doc ->Connection
  ([^SocketChannel ch ^ByteBuffer bf]
   (->Connection ch c/default-connection-timeout bf))
  ([^SocketChannel ch time-out ^ByteBuffer bf]
   (let [options (AtomicReference. (p/default-wire-opts))
         conn (Connection. ch (long time-out) bf (volatile! nil)
                           (context/create)
                           (ByteBuffer/allocate 1) options (volatile! bf)
                           (prepared/handle-cache))]
     (.put connection-wire-opts ch options)
     conn)))

(defn- set-conn-wire-opts!
  [^Connection conn wire-opts]
  (.clear ^LinkedHashMap (.-prepared-handles conn))
  (.set ^AtomicReference (.-wire-options conn) wire-opts))

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
                        (raise "Unable to connect to server: timed out"
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
        (raise "Unable to connect to server: " e
                 {:host host
                  :port port
                  :timeout-ms timeout-ms})))))

(defn- ^:redef new-connection
  ([host port time-out]
   (new-connection host port time-out time-out))
  ([host port connect-time-out receive-time-out]
   (let [ch (connect-socket host port connect-time-out)]
     (try
       (.configureBlocking ch false)
       (->Connection ch (long receive-time-out)
                     (bf/allocate-buffer c/+buffer-size+))
       (catch Throwable t
         (cleanup-after-failure! t #(try (.close ch)
                                        (finally (clear-conn-wire-opts! ch))))
         (throw t))))))

(defn- set-client-id
  [conn client-id wire-options]
  (let [{:keys [type message wire-capabilities]}
        (send-n-receive conn {:type              :set-client-id
                              :client-id         client-id
                              :wire-capabilities (p/local-wire-capabilities wire-options)})]
    (when-not (= type :set-client-id-ok) (raise message {}))
    (set-conn-wire-opts! conn
                         (p/negotiate-wire-opts wire-capabilities wire-options))))

(defn- cleanup-after-failure!
  [^Throwable failure cleanup]
  (try
    (cleanup)
    (catch Throwable cleanup-error
      (when-not (identical? failure cleanup-error)
        (.addSuppressed failure cleanup-error)))))

(defn- new-registered-connection
  "Own a new connection until registration succeeds and a pool can adopt it."
  [host port client-id time-out wire-options]
  (let [conn (new-connection host port time-out)]
    (try
      (set-client-id conn client-id wire-options)
      conn
      (catch Throwable t
        (cleanup-after-failure! t #(close conn))
        (throw t)))))

(defprotocol ^:no-doc IConnectionPool
  (get-connection [this] "Get a connection from the pool")
  (release-connection [this connection] "Return the connection back to pool")
  (close-pool [this])
  (closed-pool? [this]))

(defn- ^:redef connection-ready?
  "Probe an exclusively borrowed, idle socket without sending a request.
  isOpen only describes the local channel; a peer's FIN can already be waiting.
  An idle connection has no outstanding response, so unexpected bytes also
  mean it cannot safely carry the next request."
  [^Connection conn]
  (let [^SocketChannel ch (.-ch conn)]
    (and (.isOpen ch)
         (try
           (let [blocking? (.isBlocking ch)
                 ^ByteBuffer probe (.-probe-bf conn)]
             (try
               (when blocking? (.configureBlocking ch false))
               (.clear probe)
               (zero? (.read ch probe))
               (finally
                 (when (and blocking? (.isOpen ch))
                   (.configureBlocking ch true)))))
           (catch Exception _ false)))))

(deftype ^:no-doc ConnectionSlot [^AtomicReference connection
                                  ^AtomicBoolean borrowed?])

(declare borrow-connection)

(deftype ^:no-doc ConnectionPool [host port client-id pool-size time-out wire-options
                                  ^objects slots
                                  ^ConcurrentHashMap owners
                                  ^ThreadLocal preferred
                                  ^AtomicBoolean closed?
                                  ^AtomicInteger waiters]
  IConnectionPool
  (get-connection [this]
    (borrow-connection this true))

  (release-connection [_ conn]
    (when-let [^ConnectionSlot slot (.get owners conn)]
      (when (.compareAndSet ^AtomicBoolean (.-borrowed? slot) true false)
        (when (pos? (.get waiters))
          (locking waiters (.notify waiters))))))

  (close-pool [this]
    (locking this
      (when (.compareAndSet closed? false true)
        (locking waiters (.notifyAll waiters))
        (let [failure (volatile! nil)]
          (doseq [^ConnectionSlot slot slots]
            (when-let [conn (.getAndSet ^AtomicReference (.-connection slot) nil)]
              (try
                (close conn)
                (catch Throwable t
                  (if-let [primary @failure]
                    (when-not (identical? primary t)
                      (.addSuppressed ^Throwable primary t))
                    (vreset! failure t))))))
          (.clear owners)
          (.remove preferred)
          (when-let [t @failure] (throw t))))))

  (closed-pool? [_]
    (.get closed?)))

(defn- pool-wire-options [pool]
  (if (instance? ConnectionPool pool)
    (.-wire-options ^ConnectionPool pool)
    (p/client-wire-opts nil)))

(defn- check-pool-open!
  [^ConnectionPool pool]
  (when (.get ^AtomicBoolean (.-closed? pool))
    (raise "This client is closed" {:client-id (.-client-id pool)})))

(defn- claim-connection-slot
  "Prefer this worker's last socket, without reserving it between requests."
  [^ConnectionPool pool]
  (let [^objects slots (.-slots pool)
        ^ThreadLocal preferred (.-preferred pool)
        index (.get preferred)
        ^ConnectionSlot previous (when index (aget slots (int index)))]
    (if (and previous
             (.compareAndSet ^AtomicBoolean (.-borrowed? previous) false true))
      previous
      (loop [i 0]
        (when (< i (alength slots))
          (let [^ConnectionSlot slot (aget slots i)]
            (if (.compareAndSet ^AtomicBoolean (.-borrowed? slot) false true)
              (do (.set preferred (Integer/valueOf (int i))) slot)
              (recur (inc i)))))))))

(defn- await-connection-slot
  [^ConnectionPool pool]
  (let [^AtomicInteger waiters (.-waiters pool)
        deadline (+ (System/nanoTime) (* 1000000 (long (.-time-out pool))))]
    (locking waiters
      (.incrementAndGet waiters)
      (try
        (loop []
          (check-pool-open! pool)
          ;; Recheck after publishing the waiter so a concurrent release
          ;; between the first scan and this monitor cannot be lost.
          (or (claim-connection-slot pool)
              (let [remaining (- deadline (System/nanoTime))]
                (when (<= remaining 0)
                  (raise "Timeout in obtaining a connection" {}))
                (.wait waiters (quot remaining 1000000)
                       (int (rem remaining 1000000)))
                (recur))))
        (finally (.decrementAndGet waiters))))))

(defn- install-pool-connection!
  [^ConnectionPool pool ^ConnectionSlot slot conn]
  (let [^AtomicReference reference (.-connection slot)
        ^ConcurrentHashMap owners (.-owners pool)]
    (when-let [previous (.get reference)]
      (.remove owners previous))
    (.put owners conn slot)
    (.set reference conn))
  conn)

(defn- replace-pool-connection
  [^ConnectionPool pool ^ConnectionSlot slot conn]
  (close conn)
  (let [replacement (new-registered-connection
                      (.-host pool) (.-port pool) (.-client-id pool)
                      (.-time-out pool) (.-wire-options pool))]
    (try
      ;; Registration can block. Only publication and shutdown share a monitor.
      (locking pool
        (check-pool-open! pool)
        (install-pool-connection! pool slot replacement))
      (catch Throwable t
        (cleanup-after-failure! t #(close replacement))
        (throw t)))))

(defn- borrow-connection
  [^ConnectionPool pool probe?]
  (check-pool-open! pool)
  (let [^ConnectionSlot slot (or (claim-connection-slot pool)
                                 (await-connection-slot pool))]
    (try
      (let [^Connection conn (.get ^AtomicReference (.-connection slot))
            _ (check-pool-open! pool)
            conn (if (if probe?
                       (connection-ready? conn)
                       (.isOpen ^SocketChannel (.-ch conn)))
                   conn
                   (replace-pool-connection pool slot conn))]
        (check-pool-open! pool)
        conn)
      (catch Throwable t
        ;; Failed replacement retains the old slot, allowing a later borrower
        ;; to retry registration without shrinking the pool.
        (.set ^AtomicBoolean (.-borrowed? slot) false)
        (let [^AtomicInteger waiters (.-waiters pool)]
          (when (pos? (.get waiters))
            (locking waiters (.notify waiters))))
        (throw t)))))

(defn- connection-pool
  ([host port client-id pool-size time-out]
   (connection-pool host port client-id pool-size time-out (p/client-wire-opts nil)))
  ([host port client-id pool-size time-out wire-options]
   (->ConnectionPool host port client-id pool-size time-out wire-options
                      (into-array ConnectionSlot
                                  (repeatedly pool-size
                                              #(ConnectionSlot.
                                                 (AtomicReference.)
                                                 (AtomicBoolean. false))))
                      (ConcurrentHashMap.) (ThreadLocal.) (AtomicBoolean. false)
                      (AtomicInteger. 0))))

(defn- authenticate
  "Send an authenticate message to server, and wait to receive the response.
  Always close the temporary connection. Return a client id on success."
  [host port username password time-out]
  (let [conn (new-connection host
                             port
                             time-out
                             (max (long time-out)
                                  (long c/default-connection-timeout)))

        client-id
        (try
          (let [{:keys [type client-id message]}
                (send-n-receive conn {:type     :authentication
                                      :username username
                                      :password password})]
            (if (= type :authentication-ok)
              client-id
              (raise "Authentication failure: " message {})))
          (catch Throwable t
            (cleanup-after-failure! t #(close conn))
            (throw t)))]
    (close conn)
    client-id))

(defn- new-connectionpool
  ([host port client-id pool-size time-out]
   (new-connectionpool host port client-id pool-size time-out (p/client-wire-opts nil)))
  ([host port client-id pool-size time-out wire-options]
   (assert (> ^long pool-size 0)
           "Number of connections must be greater than zero")
   (let [^ConnectionPool pool (connection-pool
                               host port client-id pool-size time-out wire-options)
         ^objects slots (.-slots pool)]
     (try
       (dotimes [i pool-size]
         (let [conn (new-registered-connection host port client-id time-out wire-options)]
           (install-pool-connection! pool (aget slots i) conn)))
       pool
       (catch Throwable t
         ;; A server can disappear while the initial pool is being populated.
         ;; Do not leak connections that were established earlier in the loop.
         (cleanup-after-failure! t #(close-pool pool))
         (throw t))))))

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
                 (raise "Server error while copying out data" {:msg msg})))
             (do (doseq [d msg] (conj! data d))
                 (recur))))))
     (catch Exception e
       (close conn)
       (when (nv/decoding-error? e) (throw e))
       (raise "Unable to receive copy:" e {:req req})))))

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
      (when (nv/decoding-error? e)
        (close conn)
        (throw e))
      (send-n-receive conn {:type :copy-fail})
      (raise "Unable to copy in:" e
               {:req req :count (count data)}))))

(declare open-database disconnect-retry-clients!)

(defn- transport-replay-safe?
  [{:keys [type writing? client-op-id client-op-hash client-op-response-kind]}]
  (or (and (not writing?) (cmd/read-only? type))
      (and (cmd/supports-client-op? type)
           (every? #(and (string? %) (not (s/blank? %)))
                   [client-op-id client-op-hash])
           (keyword? client-op-response-kind))))

(defn- write-indeterminate?
  [e]
  (let [data (ex-data e)]
    (= :ha/write-indeterminate (or (:error data) (:error (:err-data data))))))

(defn- reject-unsafe-transport-replay!
  [req endpoint e]
  (when (or (nv/decoding-error? e) (write-indeterminate? e))
    (throw e))
  (when-not (transport-replay-safe? req)
    (let [message "Write outcome is unknown after transport failure; request was not retried"
          error   {:error :ha/write-indeterminate
                   :reason :transport-failure
                   :indeterminate? true
                   :retryable? false
                   :db-name (or (:db-name req) (first (:args req)))
                   :type (:type req)
                   :endpoint endpoint}]
      (throw (ex-info message
                      (merge req {:err-data error
                                  :server-message message
                                  :indeterminate? true})
                      e)))))

(deftype ^:no-doc Client [username password host port pool-size time-out
                          ^:volatile-mutable ^UUID id
                          ^:volatile-mutable ^ConnectionPool pool
                          ^ClientState state]
  IClientInternals
  (clientState [_] state)
  (requestBound [client req]
    (let [success? (volatile! false)
          start    (System/currentTimeMillis)
          read?    (and (not (:writing? req)) (cmd/read-only? (:type req)))]
      (loop []
        (let [^ConnectionPool pool' pool
              ;; A read can retry after EOF; mutations still probe idle
              ;; sockets before sending anything with an ambiguous outcome.
              conn                 (if (and read? (instance? ConnectionPool pool'))
                                     (borrow-connection pool' false)
                                     (get-connection pool'))
              response             (try
                                     (send-n-receive conn req)
                                     (catch Exception e
                                       (close conn)
                                       (when (or (nv/decoding-error? e)
                                                 (write-indeterminate? e)
                                                 (not (transport-replay-safe? req)))
                                         (release-connection pool' conn)
                                         (reject-unsafe-transport-replay!
                                          req (str host ":" port) e))
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
                                         (do
                                           (close conn)
                                           (vreset! success? false)
                                           {:request-status :reconnect})))
                                     (catch Exception e
                                       (reject-unsafe-transport-replay!
                                        req (str host ":" port) e)
                                       (throw e))
                                     (finally
                                       (release-connection pool' conn)))
              res'                 (case (:request-status res)
                                     :reconnect
                                     (do
                                       ;; Several in-flight requests can learn
                                       ;; that the same server session is stale.
                                       ;; Only the request that still owns the
                                       ;; observed pool should replace it.
                                       (locking client
                                         (when (identical? pool pool')
                                           (let [client-id
                                                 (authenticate
                                                   host port username password
                                                   time-out)
                                                 new-pool
                                                 (new-connectionpool
                                                   host port client-id
                                                   pool-size time-out
                                                   (pool-wire-options pool'))]
                                             ;; Explicit field access is needed
                                             ;; for mutable deftype fields inside
                                             ;; the locking form.
                                             (set! (.-id ^Client client) client-id)
                                             (set! (.-pool ^Client client) new-pool)
                                             (close-pool pool'))))
                                       nil)

                                     :reopen
                                     (let [{:keys [db-name db-type]} res]
                                       (open-database client db-name db-type)
                                       nil)

                                     res)]
          ;; The deadline limits retries, not an already completed response.
          (if @success?
            res'
            (if (>= (- (System/currentTimeMillis) start)
                    ^long (.-time-out pool'))
              (raise "Timeout in making request" {})
              (recur)))))))

  IClient
  (request [client req]
    (let [reader (or (request-native-reader client req) nv/*wire-reader*)]
      (if (identical? reader nv/*wire-reader*)
        (.requestBound client req)
        (binding [nv/*wire-reader* reader]
          (.requestBound client req)))))

  (copy-in [client req data batch-size]
    (binding [nv/*wire-reader* (or (request-native-reader client req)
                                 nv/*wire-reader*)]
      (let [conn (get-connection pool)]
        (try
          (let [{:keys [type]} (send-n-receive conn req)]
            (if (= type :copy-in-response)
              (copy-in* conn req data batch-size)
              (raise "Server refuses to accept copy in" {:req req})))
          (finally (release-connection pool conn))))))

  (disconnect [client]
    (try
      (let [conn (get-connection pool)]
        (send-only conn {:type :disconnect})
        (release-connection pool conn))
      (finally
        (try
          (.remove ha-write-retry-settings client)
          (.set ^AtomicReference (.-readers state) nil)
          (.remove index-open-options client)
          (.remove ha-retry-disabled-clients client)
          (.set ^AtomicReference (.-endpoint state) nil)
          (.clear ^ConcurrentHashMap (.-read-endpoints state))
          (.remove ha-known-db-endpoints client)
          (disconnect-retry-clients! client disconnect)
          (finally
            ;; A failed best-effort disconnect must still make the local
            ;; lifecycle transition final and release every socket.
            (close-pool pool))))))

  (disconnected? [client]
    (closed-pool? pool))

  (get-pool [client] pool)

  (get-id [client] id))

#_{:clj-kondo/ignore [:redefined-var]}
(defn ^:no-doc ->Client
  [username password host port pool-size time-out id pool]
  (Client. username password host port pool-size time-out id pool
           (new-client-state)))

(def ^:private minimum-ha-write-retry-timeout-ms 5000)
(def ^:private default-ha-write-retry-delay-ms 100)

(defn- derived-default-ha-write-retry-timeout-ms
  [time-out]
  (let [connection-budget-ms
        (long (or time-out c/default-connection-timeout))
        failover-budget-ms
        (+ (long (or c/*ha-lease-timeout-ms* 0))
           (long (or c/*ha-demotion-drain-ms* 0))
           (long (or c/*ha-promotion-base-delay-ms* 0))
           (long (or c/*ha-promotion-rank-delay-ms* 0)))
        default-budget-ms
        (long (max (long minimum-ha-write-retry-timeout-ms)
                   (long failover-budget-ms)))]
    (long (max 0
               (min connection-budget-ms default-budget-ms)))))

(defn- normalize-ha-write-retry-settings
  [time-out opts]
  {:ha-write-retry-timeout-ms
   (long
     (max 0
          (long
            (or (:ha-write-retry-timeout-ms opts)
                (derived-default-ha-write-retry-timeout-ms time-out)))))
   :ha-write-retry-delay-ms
   (long
     (max 0
          (long
            (or (:ha-write-retry-delay-ms opts)
                default-ha-write-retry-delay-ms))))})

(defn- set-client-ha-write-retry-settings!
  [client time-out opts]
  (.put ha-write-retry-settings client
        (normalize-ha-write-retry-settings time-out opts))
  client)

(defn- client-ha-write-retry-settings
  [client]
  (or (.get ha-write-retry-settings client)
      (when (instance? Client client)
        (normalize-ha-write-retry-settings
          (.-time-out ^Client client)
          nil))))

(defn open-database
  "Open a database on server. `db-type` can be \"datalog\", \"kv\",
  \"engine\", or \"index\""
  ([client db-name db-type]
   (open-database client db-name db-type nil nil false))
  ([client db-name db-type opts]
   (open-database client db-name db-type nil opts false))
  ([client db-name db-type schema opts]
   (open-database client db-name db-type schema opts false))
  ([client db-name db-type schema opts return-db-info?]
   (let [_ (when-let [registry (get-in opts [:runtime-opts :udf-registry])]
             (.put (native-reader-cache client) db-name
                   (udf/native-value-reader registry)))
         ;; Runtime handles belong to the caller and must never cross the wire.
         opts (dissoc (or opts (get (.get index-open-options client)
                                    [db-name db-type]))
                      :runtime-opts :client-opts)
         {:keys [type message result]}
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
                    {:type (if (= db-type "index")
                             :new-vector-index :new-search-engine)
                     :args [db-name opts]}))]
     (when (= type :error-response)
       (raise "Unable to open database:" db-name " " message
                {:db-type db-type}))
     (when (#{"engine" "index"} db-type)
       (remember-index-options! client
                                (if (= db-type "index")
                                  :new-vector-index :new-search-engine)
                                [db-name opts]))
     (cache-known-ha-db-endpoints! client db-name
                                   (concat
                                     (or (map :endpoint (:ha-members opts)) [])
                                     (or (map :endpoint
                                              (get-in result [:opts :ha-members]))
                                         [])
                                     (or (:ha-retry-endpoints result) [])
                                     (when-let [endpoint
                                                (:ha-authoritative-leader-endpoint
                                                  result)]
                                       [endpoint])))
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
  when obtaining an open network connection, default is 60000.
  * `:wire-compression` selects `:none` (default) or `:zstd`, in both directions.
  * `:wire-compression-threshold` sets the minimum serialized payload bytes.
  * `:wire-compression-level` sets the Zstd level (default 3).
  These settings are captured at creation and retained when connections reconnect.
  * `:ha-write-retry-timeout-ms` bounds extra HA failover retry time after a
  retryable write rejection. By default it is derived from HA lease/promotion
  timing and capped by `:time-out`.
  * `:ha-write-retry-delay-ms` sleeps between HA failover retry rounds,
  default is 100."
  ([uri-str]
   (new-client uri-str {:pool-size c/default-connection-pool-size
                        :time-out  c/default-connection-timeout}))
  ([uri-str {:keys [pool-size time-out]
             :as   opts
             :or   {pool-size c/default-connection-pool-size
                    time-out  c/default-connection-timeout}}]
   (let [wire-options                (p/client-wire-opts opts)
         uri                         (URI. uri-str)
         {:keys [username password]} (parse-user-info uri)

         host      (.getHost uri)
         port      (parse-port uri)
         client-id (authenticate host port username password time-out)
         pool      (new-connectionpool host port client-id pool-size time-out wire-options)]
     (-> (->Client username password host port pool-size time-out
                   client-id pool)
         (set-client-ha-write-retry-settings! time-out opts)))))

(defn ^:no-doc wire-client-options
  "Connection policy expressed as client options, for derived clients."
  [client]
  (when (instance? Client client)
    (let [opts (pool-wire-options (get-pool client))]
      {:wire-compression (or (:compression opts) :none)
       :wire-compression-threshold (:compression-threshold opts)
       :wire-compression-level (:compression-level opts)})))

(defn close-client
  "Close a remote client and release its connection pool. Safe to call more
  than once."
  [client]
  (when-not (disconnected? client)
    (disconnect client)))

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
              ha-settings (client-ha-write-retry-settings client)
              client-id (authenticate host port username password time-out)
              pool      (new-connectionpool host port client-id 1 time-out
                                            (pool-wire-options (get-pool client)))]
          (-> (->Client username password host port 1 time-out
                        client-id pool)
              (set-client-ha-write-retry-settings! time-out ha-settings)
              (inherit-native-readers! client)
              (inherit-index-options! client)
              (as-> tx-client (sync-ha-routing! client tx-client))))))
    client))

(defn- endpoint-key
  [host port]
  (str host ":" port))

(defn ^:no-doc retryable-ha-write-reject?
  "Internal retry API: true only for a server HA write rejection explicitly
  marked retryable. Other errors must retain their original failure semantics."
  [err-data]
  (and (map? err-data)
       (= :ha/write-rejected (:error err-data))
       (true? (:retryable? err-data))))

(defn- retryable-ha-read-reject?
  [err-data]
  (and (map? err-data)
       (= :ha/read-rejected (:error err-data))
       (true? (:retryable? err-data))))

(defn- retryable-ha-reject?
  [err-data]
  (or (retryable-ha-write-reject? err-data)
      (retryable-ha-read-reject? err-data)))

(defn ^:no-doc disable-ha-write-retry!
  "Internal routing API: pin subsequent writes to the selected server session.
  Returns client; pair with `enable-ha-write-retry!` after transaction cleanup."
  [client]
  (when client
    (.put ha-retry-disabled-clients client true))
  client)

(defn ^:no-doc enable-ha-write-retry!
  "Internal routing API: allow HA write rerouting again. Returns client."
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
  (raise "Request to Datalevin server failed: "
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

(defn- append-ha-retry-endpoint
  [endpoints endpoint]
  (cond-> (vec (or endpoints []))
    (and (string? endpoint)
         (not (s/blank? endpoint))
         (not (some #(= endpoint %) endpoints)))
    (conj endpoint)))

(defn- ^ConcurrentHashMap known-ha-db-endpoint-cache
  [client]
  (locking ha-known-db-endpoints
    (or (.get ha-known-db-endpoints client)
        (let [cache (ConcurrentHashMap.)]
          (.put ha-known-db-endpoints client cache)
          cache))))

(defn- ^ConcurrentHashMap preferred-ha-read-endpoint-cache
  [client]
  (.-read-endpoints (client-state client)))

(defn- known-ha-db-endpoints
  [client db-name]
  (when (and client (string? db-name))
    (when-let [^ConcurrentHashMap cache (.get ha-known-db-endpoints client)]
      (let [endpoints (->> (.get cache db-name)
                           (keep (fn [endpoint]
                                   (cond
                                     (string? endpoint) endpoint
                                     (map? endpoint) (:endpoint endpoint)
                                     :else nil)))
                           vec)]
        (when (seq endpoints)
          endpoints)))))

(defn- cache-known-ha-db-endpoints!
  [client db-name endpoints]
  (when (and client (string? db-name))
    (let [[merged _]
          (collect-ha-retry-endpoints
            #{}
            (concat (or (known-ha-db-endpoints client db-name) [])
                    (or endpoints [])))]
      (if (seq merged)
        (.put (known-ha-db-endpoint-cache client) db-name (mapv :endpoint merged))
        (when-let [^ConcurrentHashMap cache (.get ha-known-db-endpoints client)]
          (.remove cache db-name)))))
  client)

(defn- read-preferred-ha-read-endpoint
  [client db-name]
  (when (and client (string? db-name))
    (when-let [^ConcurrentHashMap cache (preferred-ha-read-endpoint-cache client)]
      (let [endpoint (.get cache db-name)]
        (when (and (string? endpoint) (not (s/blank? endpoint)))
          endpoint)))))

(defn- set-preferred-ha-read-endpoint!
  [client db-name endpoint]
  (when (and client (string? db-name))
    (if (and (string? endpoint) (not (s/blank? endpoint)))
      (.put (preferred-ha-read-endpoint-cache client) db-name endpoint)
      (when-let [^ConcurrentHashMap cache (preferred-ha-read-endpoint-cache client)]
        (.remove cache db-name))))
  client)

(defn- clear-preferred-ha-read-endpoint!
  [client db-name]
  (set-preferred-ha-read-endpoint! client db-name nil))

(defn- preferred-ha-read-endpoint
  [client db-name]
  (or (read-preferred-ha-read-endpoint client db-name)
      (read-preferred-ha-endpoint client)))

(defn- ha-retry-after-ms
  [err-data]
  (when (map? err-data)
    (when-let [retry-after-ms (:ha-retry-after-ms err-data)]
      (long (max 0 (long retry-after-ms))))))

(defn- merge-ha-retry-after-ms
  [current-ms err-data]
  (if-let [retry-after-ms (ha-retry-after-ms err-data)]
    (let [current-ms (long (or current-ms 0))
          retry-after-ms (long retry-after-ms)]
      (long (if (> retry-after-ms current-ms)
              retry-after-ms
              current-ms)))
    (long (or current-ms 0))))

(defn- next-ha-write-retry-round-delay-ms
  [retry-context remaining-ms round-retry-after-ms]
  (let [base-delay-ms
        (long (or (:ha-write-retry-delay-ms retry-context)
                  default-ha-write-retry-delay-ms))
        delay-ms
        (long (max base-delay-ms
                   (long (or round-retry-after-ms 0))))]
    (long (min delay-ms (long remaining-ms)))))

(defn- retry-context-with-attempt-timeout
  [retry-context remaining-ms]
  (let [attempt-timeout-ms
        (long
          (max 1
               (min (long remaining-ms)
                    (long (or (:time-out retry-context)
                              c/default-connection-timeout)))))]
    (assoc retry-context
           :time-out attempt-timeout-ms
           :ha-write-retry-timeout-ms attempt-timeout-ms)))

(defn ^:no-doc sync-ha-routing!
  "Internal routing API: copy the preferred write endpoint from source-client
  to target-client, clearing stale routing when source has none. Returns target."
  [source-client target-client]
  (when (and source-client target-client)
    (if-let [endpoint (read-preferred-ha-endpoint source-client)]
      (set-preferred-ha-endpoint! target-client endpoint)
      (clear-preferred-ha-endpoint! target-client)))
  target-client)

(defn ^:no-doc active-ha-request-client
  "Internal routing API: return the live cached client for the preferred write
  endpoint, or client itself. Explicit transactions must use this owning session."
  [client]
  (if-let [endpoint (read-preferred-ha-endpoint client)]
    (if-let [retry-client (cached-retry-client client endpoint)]
      (if (retry-client-disconnected? retry-client)
        (do
          (evict-retry-client! client endpoint disconnect)
          client)
        retry-client)
      client)
    client))

(defn- client-routing-context
  [client]
  (when (instance? Client client)
    (merge
      {:username  (.-username ^Client client)
       :password  (.-password ^Client client)
       :pool-size (.-pool-size ^Client client)
       :time-out  (.-time-out ^Client client)
       :host      (.-host ^Client client)
       :port      (.-port ^Client client)
       :client    client
       :wire-options (pool-wire-options (get-pool client))}
      (client-ha-write-retry-settings client))))

(defn- ^:redef client-retry-context
  [client]
  (when-not (ha-write-retry-disabled? client)
    (client-routing-context client)))

(defn- read-preferred-ha-endpoint
  [client]
  (let [endpoint (when-let [state (client-state client)]
                   (.get ^AtomicReference (.-endpoint state)))]
    (when (and (string? endpoint) (not (s/blank? endpoint)))
      endpoint)))

(defn- set-preferred-ha-endpoint!
  [client endpoint]
  (when-let [state (client-state client)]
    (.set ^AtomicReference (.-endpoint state)
          (when (and (string? endpoint) (not (s/blank? endpoint))) endpoint))))

(defn ^:no-doc clear-preferred-ha-endpoint!
  "Internal routing API: forget a client's preferred write endpoint. Used when
  pinning an explicit transaction so a nested preference cannot change sessions."
  [client]
  (set-preferred-ha-endpoint! client nil))

(defn- preferred-ha-endpoint
  [client retry-context]
  (let [self-endpoint (endpoint-key (:host retry-context) (:port retry-context))
        endpoint      (read-preferred-ha-endpoint client)]
    (when (and endpoint (not= endpoint self-endpoint))
      endpoint)))

(defn- new-client-for-endpoint
  [{:keys [username password pool-size time-out wire-options]
    :as   retry-context} host port]
  (let [client-id (authenticate host port username password time-out)
        pool      (new-connectionpool host port client-id pool-size time-out
                                      (or wire-options (p/client-wire-opts nil)))]
    (-> (->Client username password host port pool-size time-out
                  client-id pool)
        (inherit-native-readers! (:client retry-context))
        (inherit-index-options! (:client retry-context))
        (set-client-ha-write-retry-settings! time-out retry-context))))

(def ^:private ha-kv-retry-request-types
  #{:transact-kv
    :open-transact-kv
    :close-transact-kv
    :abort-transact-kv
    :open-dbi
    :register-type
    :clear-dbi
    :drop-dbi
    :kv-re-index})

(def ^:private ha-datalog-retry-request-types
  #{:assoc-opt
    :assoc-opts
    :opts
    :closed?
    :last-modified
    :schema
    :rschema
    :set-schema
    :datalog-register-type
    :init-max-eid
    :max-tx
    :datom-count
    :fetch
    :populated?
    :size
    :head
    :tail
    :slice
    :rslice
    :start-sampling
    :stop-sampling
    :analyze
    :e-datoms
    :e-first-datom
    :av-datoms
    :av-first-datom
    :av-first-e
    :ea-first-datom
    :ea-first-v
    :v-datoms
    :size-filter
    :head-filter
    :tail-filter
    :slice-filter
    :rslice-filter
    :q
    :pull
    :pull-many
    :explain
    :fulltext-datoms
    :del-attr
    :rename-attr
    :load-datoms
    :db-info
    :tx-data
    :tx-data+db-info
    :open-transact
    :close-transact
    :abort-transact
    :datalog-re-index})

(defn- request-db-name
  [req]
  (let [db-name (or (:db-name req) (first (:args req)))]
    (when (string? db-name)
      db-name)))

(defn- request-db-type
  [req]
  (or (:db-type req)
      (cmd/db-type (:type req))
      (let [req-type (:type req)]
        (cond
          (contains? ha-kv-retry-request-types req-type)
          c/db-store-kv

          (contains? ha-datalog-retry-request-types req-type)
          c/db-store-datalog

          :else nil))))

(defn- request-db-target
  [req]
  (when-let [db-type (request-db-type req)]
    (let [db-name (request-db-name req)]
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

(defn- retry-client-timeout-mismatch?
  [retry-client retry-context]
  (and (instance? Client retry-client)
       (let [expected-timeout-ms
             (long (or (:time-out retry-context)
                       c/default-connection-timeout))
             expected-retry-timeout-ms
             (long (or (:ha-write-retry-timeout-ms retry-context)
                       (derived-default-ha-write-retry-timeout-ms
                         expected-timeout-ms)))
             settings (client-ha-write-retry-settings retry-client)]
         (or (not= expected-timeout-ms
                   (long (.-time-out ^Client retry-client)))
             (not= expected-retry-timeout-ms
                   (long (:ha-write-retry-timeout-ms settings)))))))

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
          (if (or (retry-client-timeout-mismatch? cached retry-context)
                  (retry-client-disconnected? cached))
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
          ;; Once request-fn has started, a lost reply can hide a commit. Only
          ;; reads and writes with supported deduplication may visit another
          ;; endpoint. Failures while preparing the client remain safe to retry.
          (reject-unsafe-transport-replay! req endpoint e)
          (throw e))
        (finally
          (when-not cached?
            (safe-disconnect-retry-client! client disconnect-fn)))))
    (catch Exception e
      (when (or (nv/decoding-error? e) (write-indeterminate? e)) (throw e))
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
         deadline-ms
         (+ (System/currentTimeMillis)
            (long (or (:ha-write-retry-timeout-ms retry-context)
                      (derived-default-ha-write-retry-timeout-ms
                        (:time-out retry-context)))))
         [pending _]
         (collect-ha-retry-endpoints
           #{self-key}
           (:ha-retry-endpoints err-data))
         [round-order seen]
         (collect-ha-retry-endpoints
           #{}
           (append-ha-retry-endpoint
             (:ha-retry-endpoints err-data)
             self-key))]
     (loop [round        1
            remaining    pending
            round-order  round-order
            seen         seen
            round-retry-after-ms
            (merge-ha-retry-after-ms nil err-data)
            last-message message
            last-err     err-data
            attempts     []]
       (if-let [{:keys [endpoint host port]} (first remaining)]
         (let [remaining-ms (- deadline-ms (System/currentTimeMillis))]
           (if (<= remaining-ms 0)
             (raise-normal-request-error
               req last-message last-err
               {:ha-retry-attempts attempts
                :ha-retry-rounds round})
             (let [attempt-context
                   (retry-context-with-attempt-timeout retry-context
                                                       remaining-ms)
                   outcome
                   (attempt-ha-endpoint-request
                     req attempt-context host port
                     request-fn disconnect-fn new-client-fn)]
               (cond
                 (= :success (:kind outcome))
                 (do
                   (on-success-endpoint! endpoint)
                   (:result outcome))

                 (= :exception (:kind outcome))
                 (recur round
                        (rest remaining)
                        round-order
                        seen
                        round-retry-after-ms
                        last-message
                        last-err
                        (conj attempts
                              {:endpoint endpoint
                               :type :exception
                               :message (ex-message (:exception outcome))}))

                 :else
                 (let [retry-err     (:err-data outcome)
                       retry-message (:message outcome)]
                   (if (retryable-ha-reject? retry-err)
                     (let [[extra seen']
                           (collect-ha-retry-endpoints
                             seen
                             (:ha-retry-endpoints retry-err))
                           round-order'
                           (into round-order extra)]
                       (recur round
                              (concat (rest remaining) extra)
                              round-order'
                              seen'
                              (merge-ha-retry-after-ms
                                round-retry-after-ms retry-err)
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
                               :reason (:reason retry-err)})})))))))
         (let [remaining-ms (- deadline-ms (System/currentTimeMillis))]
           (if (or (empty? round-order)
                   (<= remaining-ms 0))
             (raise-normal-request-error
               req last-message last-err
               {:ha-retry-attempts attempts
                :ha-retry-rounds round})
             (do
               (let [retry-delay-ms
                     (long
                       (next-ha-write-retry-round-delay-ms
                         retry-context remaining-ms round-retry-after-ms))]
                 (when (pos? ^long retry-delay-ms)
                   (Thread/sleep ^long retry-delay-ms)))
               (recur (inc round)
                      round-order
                      round-order
                      seen
                      0
                      last-message
                      last-err
                      attempts)))))))))

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
                               (update (:err-data outcome)
                                       :ha-retry-endpoints
                                       append-ha-retry-endpoint
                                       endpoint))}
            (raise-normal-request-error
              req (:message outcome) (:err-data outcome) nil))

          :exception
          (do
            (clear-preferred-ha-endpoint! client)
            {:handled? false})))
      (do
        (clear-preferred-ha-endpoint! client)
        {:handled? false}))))

(defn ^:no-doc ^:redef retry-ha-write-request
  "Internal retry API: handle a failed write using the client's HA settings.
  Returns the successful result and remembers its endpoint. Raises a normal
  request error if the rejection is not retryable, retries are disabled, or the
  deadline expires. An ambiguous unprotected write raises :ha/write-indeterminate.
  Optional request-fn takes [client request] and returns a wire
  response; remote stores supply it to replay copy-in payloads."
  ([client req message err-data]
   (retry-ha-write-request client req message err-data request))
  ([client req message err-data request-fn]
   (if-let [retry-context (and (retryable-ha-write-reject? err-data)
                              (client-retry-context client))]
     (#'retry-ha-write-request*
      req message err-data retry-context request-fn disconnect
      #'new-client-for-endpoint
      #(#'set-preferred-ha-endpoint! client %))
     (raise-normal-request-error req message err-data nil))))

(defn ^:no-doc retry-ha-transport-failure
  "Internal retry API for replay-safe remote writes after a transport failure.
  The caller must establish replay safety (for example, a client operation ID).
  request-fn takes [client request] and returns a wire response. Starts with known
  endpoints other than the failed client's endpoint, returning a command-complete
  response with transaction metadata, or nil when no retry is available. Native
  decoding errors, indeterminate writes, and exhausted retries propagate."
  [client req request-fn known-endpoints throwable]
  (when (or (nv/decoding-error? throwable) (write-indeterminate? throwable))
    (throw throwable))
  (when-let [retry-context (client-retry-context client)]
    (let [self-endpoint (endpoint-key (:host retry-context) (:port retry-context))
          retry-endpoints (->> known-endpoints
                               (remove #(= self-endpoint %))
                               vec)]
      (when (seq retry-endpoints)
        (let [retry-result
              (#'retry-ha-write-request*
               req
               (or (ex-message throwable) "HA write target became unavailable")
               {:error :ha/write-rejected
                :reason :endpoint-unreachable
                :retryable? true
                :ha-retry-endpoints retry-endpoints}
               retry-context request-fn disconnect #'new-client-for-endpoint
               #(#'set-preferred-ha-endpoint! client %))]
          (cond-> {:type :command-complete :result retry-result}
            (map? retry-result)
            (merge (select-keys retry-result [:db-info :new-attributes]))))))))

(defn ^:no-doc request-ha-open
  "Internal retry API: send a transaction-open request, trying the preferred HA
  endpoint first and retrying eligible write rejections. Returns the result and
  records the winning endpoint. The caller must then pin the transaction to
  `active-ha-request-client` before sending transaction data or control messages."
  [client req]
  (if-let [retry-context (client-retry-context client)]
    (let [preferred-attempt
          (try-preferred-ha-write-request*
           client req retry-context request disconnect
           #'new-client-for-endpoint #'retry-ha-write-request)]
      (if (:handled? preferred-attempt)
        (:result preferred-attempt)
        (let [{:keys [type message result err-data]} (request client req)]
          (if (= type :error-response)
            (retry-ha-write-request client req message err-data)
            (do
              (clear-preferred-ha-endpoint! client)
              result)))))
    (let [{:keys [type message result err-data]} (request client req)]
      (if (= type :error-response)
        (raise-normal-request-error req message err-data nil)
        result))))

(defn- try-preferred-ha-read-request*
  [client req routing-context request-fn disconnect-fn new-client-fn]
  (when (and routing-context
             (request-db-name req))
    (when-let [db-name (request-db-name req)]
      (when-let [endpoint (preferred-ha-read-endpoint client db-name)]
        (let [self-endpoint (endpoint-key (:host routing-context)
                                          (:port routing-context))]
          (if (= endpoint self-endpoint)
            {:handled? false}
            (if-let [{:keys [host port]} (parse-ha-endpoint endpoint)]
              (let [outcome (attempt-ha-endpoint-request
                              req routing-context host port
                              request-fn disconnect-fn new-client-fn)]
                (case (:kind outcome)
                  :success
                  {:handled? true
                   :result (:result outcome)}

                  :error
                  (if (retryable-ha-read-reject? (:err-data outcome))
                    (do
                      (clear-preferred-ha-read-endpoint! client db-name)
                      {:handled? false})
                    (raise-normal-request-error
                      req (:message outcome) (:err-data outcome)
                      {:ha-pinned-endpoint endpoint}))

                  :exception
                  (do
                    (clear-preferred-ha-read-endpoint! client db-name)
                    {:handled? false})))
              (do
                (clear-preferred-ha-read-endpoint! client db-name)
                {:handled? false}))))))))

(defn- retry-ha-read-request*
  [client req message routing-context known-endpoints
   request-fn disconnect-fn new-client-fn]
  (when-let [db-name (request-db-name req)]
    (let [self-endpoint (endpoint-key (:host routing-context)
                                      (:port routing-context))
          retry-endpoints (->> (or known-endpoints
                                   (known-ha-db-endpoints client db-name))
                               (remove #(= self-endpoint %))
                               vec)]
      (when (seq retry-endpoints)
        (retry-ha-write-request*
          req
          message
          {:ha-retry-endpoints retry-endpoints}
          routing-context
          request-fn
          disconnect-fn
          new-client-fn
          #(set-preferred-ha-read-endpoint! client db-name %))))))

(defn- retry-ha-read-request
  [client req message known-endpoints]
  (when-let [routing-context (client-routing-context client)]
    (retry-ha-read-request*
      client
      req
      message
      routing-context
      known-endpoints
      request
      disconnect
      new-client-for-endpoint)))

(defn- retry-ha-read-reject
  [client req message err-data]
  (when-let [db-name (request-db-name req)]
    (when-let [routing-context (client-routing-context client)]
      (retry-ha-write-request*
        req
        message
        err-data
        routing-context
        request
        disconnect
        new-client-for-endpoint
        #(set-preferred-ha-read-endpoint! client db-name %)))))

(defn- normal-request*
  "Send request to server and returns results. Does not use the
  copy-in protocol. `call` is a keyword, `args` is a vector,
  `writing?` is a boolean indicating if write-txn should be used"
  ([client call args]
   (normal-request* client call args false))
  ([client call args writing?]
   (normal-request* client call args writing? nil))
  ([client call args writing? ^Request prepared-request]
   (let [write-route?        (or writing? (cmd/ha-write? call))
         read-route?         (and (not writing?) (cmd/read-only? call))
         read-min-tx         (when (and read-route?
                                       (integer? *ha-read-min-tx*))
                              (long *ha-read-min-tx*))
         req                 (cond-> {:type call
                                      :args args
                                      :writing? writing?}
                               prepared-request
                               (with-meta (.-metadata prepared-request))
                               read-min-tx
                               (assoc :ha-read-min-tx read-min-tx))
         db-name             (request-db-name req)
         read-routing-context (when (and read-route? db-name
                                         (preferred-ha-read-endpoint client db-name))
                                (client-routing-context client))
         routing-context     (and write-route? (client-routing-context client))
         retry-context       (and write-route? (client-retry-context client))
         preferred-endpoint  (and write-route?
                                 (not retry-context)
                                 (read-preferred-ha-endpoint client))
         preferred-read-attempt
         (when read-route?
           (try-preferred-ha-read-request*
             client
             req
             read-routing-context
             request
             disconnect
             new-client-for-endpoint))
         preferred-attempt   (when retry-context
                               (try-preferred-ha-write-request*
                                 client
                                 req
                                 retry-context
                                 request
                                 disconnect
                                 new-client-for-endpoint
                                 retry-ha-write-request))]
     (cond
       (:handled? preferred-read-attempt)
       (:result preferred-read-attempt)

       (:handled? preferred-attempt)
       (:result preferred-attempt)

       (and preferred-endpoint routing-context)
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

       :else
       (try
         (let [{:keys [type message result err-data]} (request client req)]
           (if (= type :error-response)
             (cond
               (and write-route?
                    (retryable-ha-write-reject? err-data))
               (do
                 (cache-known-ha-db-endpoints! client db-name
                                               (:ha-retry-endpoints err-data))
                 (retry-ha-write-request client req message err-data))

               (and read-route?
                    (retryable-ha-read-reject? err-data))
               (do
                 (cache-known-ha-db-endpoints! client db-name
                                               (:ha-retry-endpoints err-data))
                 (or (retry-ha-read-reject client req message err-data)
                     (raise-normal-request-error req message err-data nil)))

               :else
               (raise-normal-request-error req message err-data nil))
             (do
               (when retry-context
                 (clear-preferred-ha-endpoint! client))
               result)))
         (catch Exception e
           (if (or (not read-route?) (nv/decoding-error? e))
             (throw e)
             (or (retry-ha-read-request
                   client
                   req
                   (or (ex-message e)
                       "HA read target became unavailable")
                   (when db-name (known-ha-db-endpoints client db-name)))
                 (throw e)))))))))

(defn ^:no-doc normal-prepared-request
  "Use normal routing and replay rules with connection-local preparation."
  [client call ^Request prepared-request value writing?]
  (normal-request* client call (assoc (.-args prepared-request) 2 value)
                   writing? prepared-request))

(defn ^:no-doc normal-request
  "Send a command, routing mutations by command properties. `writing?` only
  selects an existing transaction on the server; it is preserved on the wire.
  Mutations are retried on explicit HA rejection, never as transport-failed reads."
  ([client call args]
   (normal-request client call args false))
  ([client call args writing?]
   (let [result (normal-request* client call args writing?)]
     (remember-index-options! client call args)
     result)))

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

(defn replica-status
  "Return async read-replica status for an open database."
  [client db-name]
  (normal-request client :replica-status [db-name]))

(defn ha-update-membership!
  "Operator-driven consensus HA membership update for an open database.

  `spec` may include `:ha-members`, `:ha-control-plane {:voters [...]}`,
  `:ha-control-plane-voters`, `:expected-membership-hash`, `:clear-leases?`,
  `:replace-voters?`, and `:timeout-ms`."
  [client db-name spec]
  (normal-request client :ha-update-membership! [db-name spec]))

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
