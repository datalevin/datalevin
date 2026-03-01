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
   [datalevin.util :as u]
   [datalevin.constants :as c]
   [clojure.string :as s]
   [datalevin.buffer :as bf]
   [datalevin.protocol :as p])
  (:import
   [java.nio ByteBuffer BufferOverflowException]
   [java.nio.channels SocketChannel]
   [java.util UUID]
   [java.util.concurrent ConcurrentLinkedQueue]
   [java.net InetSocketAddress StandardSocketOptions URI]))

(defprotocol ^:no-doc IConnection
  (send-n-receive [conn msg]
    "Send a message to server and return the response, a blocking call")
  (send-only [conn msg] "Send a message without waiting for a response")
  (receive [conn] "Receive a message, a blocking call")
  (close [conn]))

(deftype ^:no-doc Connection [^SocketChannel ch
                              ^:volatile-mutable ^ByteBuffer bf]
  IConnection
  (send-n-receive [this msg]
    (try
      (locking bf
        (p/write-message-blocking ch bf msg)
        (.clear bf)
        (let [[resp bf'] (p/receive-ch ch bf)]
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
      (p/write-message-blocking ch bf msg)
      (catch BufferOverflowException _
        (let [size (* ^long c/+buffer-grow-factor+ (.capacity bf))]
          (set! bf (bf/allocate-buffer size))
          (send-only this msg)))
      (catch Exception e
        (u/raise "Error sending message: " e {:msg msg}))))

  (receive [this]
    (try
      (let [[resp bf'] (p/receive-ch ch bf)]
        (when-not (identical? bf' bf) (set! bf bf'))
        resp)
      (catch Exception e
        (u/raise "Error receiving data:" e {}))))

  (close [this]
    (.close ch)))

(defn- ^SocketChannel connect-socket
  "connect to server and return the client socket channel"
  [^String host port]
  (try
    (doto (SocketChannel/open)
      (.setOption StandardSocketOptions/SO_KEEPALIVE true)
      (.setOption StandardSocketOptions/TCP_NODELAY true)
      (.connect (InetSocketAddress. host ^int port)))
    (catch Exception e
      (u/raise "Unable to connect to server: " e
               {:host host :port port}))))

(defn- new-connection
  [host port]
  (->Connection (connect-socket host port)
                (bf/allocate-buffer c/+buffer-size+)))

(defn- set-client-id
  [conn client-id]
  (let [{:keys [type message]}
        (send-n-receive conn {:type      :set-client-id
                              :client-id client-id})]
    (when-not (= type :set-client-id-ok) (u/raise message {}))))

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
                (let [conn (new-connection host port)]
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
  [host port username password]
  (let [conn (new-connection host port)

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
      (let [conn (new-connection host port)]
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
    (when-let [[_ username password] (re-find #"(.+):(.+)" user-info)]
      {:username username :password password})))

(defn ^:no-doc parse-port
  [^URI uri]
  (let [p (.getPort uri)] (if (= -1 p) c/default-port p)))

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
                 (merge {:type :command-complete
                         :result (persistent! data)}
                        (when (map? copy-out-response)
                          (dissoc copy-out-response :type)))
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

(declare open-database)

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
                                           (open-database client db-name db-type))
                                         :reconnect
                                         (let [client-id
                                               (authenticate host port username
                                                             password)]
                                           (close conn)
                                           (vreset! success? false)
                                           {:request-status :reconnect
                                            :client-id      client-id})))
                                     (finally
                                       (release-connection pool' conn)))
              res'                 (if (= :reconnect (:request-status res))
                                     (let [client-id (:client-id res)]
                                       (set! id client-id)
                                       (set! pool (new-connectionpool
                                                    host port client-id
                                                    pool-size time-out))
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
    (let [conn (get-connection pool)]
      (send-only conn {:type :disconnect})
      (release-connection pool conn))
    (close-pool pool))

  (disconnected? [client]
    (closed-pool? pool))

  (get-pool [client] pool)

  (get-id [client] id))

(defn open-database
  "Open a database on server. `db-type` can be \"datalog\", \"kv\",
  or \"engine\""
  ([client db-name db-type]
   (open-database client db-name db-type nil nil))
  ([client db-name db-type opts]
   (open-database client db-name db-type nil opts))
  ([client db-name db-type schema opts]
   (let [{:keys [type message]}
         (request client
                  (cond
                    (= db-type c/db-store-kv)
                    {:type :open-kv :db-name db-name :opts opts}
                    (= db-type c/db-store-datalog)
                    (cond-> {:type :open :db-name db-name}
                      schema (assoc :schema schema)
                      opts   (assoc :opts (assoc opts :db-name db-name)))
                    :else
                    {:type :new-search-engine :db-name db-name :opts opts}))]
     (when (= type :error-response)
       (u/raise "Unable to open database:" db-name " " message
                {:db-type db-type})))))

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
         client-id (authenticate host port username password)
         pool      (new-connectionpool host port client-id pool-size time-out)]
     (->Client username password host port pool-size time-out client-id pool))))

(defn ^:no-doc normal-request
  "Send request to server and returns results. Does not use the
  copy-in protocol. `call` is a keyword, `args` is a vector,
  `writing?` is a boolean indicating if write-txn should be used"
  ([client call args]
   (normal-request client call args false))
  ([client call args writing?]
   (let [req {:type call :args args :writing? writing?}

         {:keys [type message result]} (request client req)]
     (if (= type :error-response)
       (u/raise "Request to Datalevin server failed: " message req)
       result))))

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
