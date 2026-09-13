(ns datalevin.client-connection-test
  (:require
   [clojure.test :refer [deftest is testing]]
   [datalevin.client :as client]
   [datalevin.constants :as c]
   [datalevin.protocol :as p])
  (:import
   [datalevin.client Connection ConnectionPool]
   [java.io IOException]
   [java.net InetSocketAddress]
   [java.nio ByteBuffer]
   [java.nio.channels ServerSocketChannel SocketChannel]
   [java.nio.channels.spi SelectorProvider]
   [java.util UUID]
   [java.util.concurrent ConcurrentHashMap ConcurrentLinkedQueue]
   [java.util.concurrent.atomic AtomicBoolean]))

(def ^:private registered {:type :set-client-id-ok})
(def ^:private completed {:type :command-complete :result :pong})

(defn- malformed-response []
  ;; A complete frame with an unknown format fails decoding without EOF.
  (doto (ByteBuffer/allocate 6)
    (.put (byte 0))
    (.putInt 6)
    (.put (byte 0))
    (.flip)))

(defn- test-connection
  ([responses] (test-connection responses nil))
  ([responses close-error]
   (let [frames  (ConcurrentLinkedQueue.)
         closed  (atom 0)
         sent    (atom [])
         channel (proxy [SocketChannel] [(SelectorProvider/provider)]
                   (write [^ByteBuffer src]
                     (let [n (.remaining src)
                           copy (.duplicate src)
                           fmt (.get copy)
                           bytes (byte-array (- (.getInt copy) c/message-header-size))]
                       (.get copy bytes)
                       (swap! sent conj (p/read-value fmt bytes))
                       (.position src (.limit src))
                       n))
                   (read [^ByteBuffer dst]
                     (let [frame (.poll frames)]
                       (cond
                         (instance? Throwable frame) (throw frame)
                         frame (let [^ByteBuffer frame frame
                                     n (.remaining frame)]
                                 (.put dst frame)
                                 n)
                         :else (throw (IOException. "No response")))))
                   (implConfigureBlocking [_])
                   (implCloseSelectableChannel []
                     (swap! closed inc)
                     (when close-error (throw close-error))))
         conn    (client/->Connection channel 1000 (ByteBuffer/allocate 65536))]
     (doseq [response responses]
       (if (or (instance? Throwable response) (instance? ByteBuffer response))
         (.add frames response)
         (let [frame (ByteBuffer/allocate 65536)]
           (p/write-message-bf frame response c/message-format-nippy)
           (.flip frame)
           (.add frames frame))))
     (#'client/set-conn-wire-opts! channel (p/default-wire-opts))
     {:conn conn :channel channel :closed closed :sent sent})))

(defn- with-connections [connections f]
  (let [pending (ConcurrentLinkedQueue. (mapv :conn connections))]
    (try
      ;; Retain the real registration, wire decoder, pool, and channel close
      ;; while injecting transport failures deterministically.
      (with-redefs [client/new-connection
                    (fn [& _]
                      (or (.poll pending)
                          (throw (ex-info "Unexpected connection attempt" {}))))
                    ;; These scripted sockets preload responses before writes.
                    ;; The real idle-socket probe is covered with TCP below.
                    client/connection-ready?
                    (fn [^Connection conn] (.isOpen ^SocketChannel (.-ch conn)))]
        (f))
      (finally
        (doseq [{:keys [conn]} connections]
          (try (client/close conn) (catch Throwable _ nil)))))))

(defn- assert-closed! [{:keys [channel closed]}]
  (is (not (.isOpen ^SocketChannel channel)))
  (is (= 1 @closed))
  (is (not (.containsKey ^ConcurrentHashMap @#'client/connection-wire-opts
                        channel))
      "closing the socket also releases its wire-options cache entry"))

(defn- thrown-by [f]
  (try (f) nil (catch Throwable t t)))

(deftest initial-registration-failure-closes-every-created-connection-test
  (doseq [response [{:type :error-response :message "Registration rejected"}
                    {:type :set-client-id-ok
                     :wire-capabilities {:compression 42}}
                    (malformed-response)
                    (IOException. "Registration response lost")
                    (AssertionError. "Registration failed")]]
    (testing (str response)
      (let [first-conn (test-connection [registered])
            failed     (test-connection [response])]
        (with-connections
          [first-conn failed]
          (fn []
            (is (some? (thrown-by
                         #(#'client/new-connectionpool
                            "localhost" 19001 (UUID/randomUUID) 2 1000))))
            (assert-closed! first-conn)
            (assert-closed! failed)))))))

(deftest replacement-registration-failures-do-not-leak-or-shrink-pool-test
  (let [initial (test-connection [registered completed])
        failures (mapv #(test-connection [%])
                       [{:type :error-response :message "Registration rejected"}
                        {:type :set-client-id-ok
                         :wire-capabilities {:compression 42}}
                        (malformed-response)
                        (IOException. "Registration response lost")])
        recovered (test-connection [registered completed])]
    (with-connections
      (into [initial] (conj failures recovered))
      (fn []
        (let [^ConnectionPool pool (#'client/new-connectionpool
                                     "localhost" 19001 (UUID/randomUUID) 1 1000)
              ^ConcurrentLinkedQueue available (.-available pool)
              ^ConcurrentLinkedQueue used (.-used pool)]
          (try
            (let [conn (client/get-connection pool)]
              (is (identical? (:conn initial) conn))
              (is (= completed (client/send-n-receive conn {:type :ping})))
              (client/release-connection pool conn)
              (client/close conn))
            (doseq [failed failures]
              (is (some? (thrown-by #(client/get-connection pool))))
              (assert-closed! failed)
              (is (not (client/closed-pool? pool)))
              (is (.isEmpty used))
              (is (= 1 (.size available)))
              (is (identical? (:conn initial) (.peek available))))
            (let [conn (client/get-connection pool)]
              (is (identical? (:conn recovered) conn))
              (is (.isOpen ^SocketChannel (:channel recovered)))
              (is (zero? @(:closed recovered)))
              (is (= completed (client/send-n-receive conn {:type :ping})))
              (client/release-connection pool conn)
              (is (.isEmpty used))
              (is (identical? conn (.peek available))))
            (finally (client/close-pool pool)))
          (assert-closed! initial)
          (assert-closed! recovered))))))

(deftest authentication-always-closes-its-temporary-connection-test
  (let [client-id (UUID/randomUUID)]
    (doseq [response [{:type :authentication-ok :client-id client-id}
                      {:type :error-response :message "Authentication rejected"}
                      (malformed-response)
                      (IOException. "Authentication response lost")
                      (AssertionError. "Authentication failed")]]
      (let [connection (test-connection [response])]
        (with-connections
          [connection]
          (fn []
            (let [authenticate #(#'client/authenticate
                                   "localhost" 19001 "user" "password" 1000)]
              (if (= :authentication-ok (:type response))
                (is (= client-id (authenticate)))
                (is (some? (thrown-by authenticate)))))
            (assert-closed! connection)))))))

(deftest cleanup-errors-do-not-replace-registration-or-authentication-error-test
  (doseq [operation [#(#'client/new-connectionpool
                        "localhost" 19001 (UUID/randomUUID) 1 1000)
                     #(#'client/authenticate
                        "localhost" 19001 "user" "password" 1000)]]
    (let [failure (AssertionError. "Handshake failed")
          close-error (IOException. "Socket close failed")
          connection (test-connection [failure] close-error)]
      (with-connections
        [connection]
        (fn []
          (is (identical? failure (thrown-by operation)))
          (is (= [close-error] (vec (.getSuppressed failure))))
          (assert-closed! connection))))))

(deftest failed-pool-creation-closes-remaining-sockets-after-cleanup-error-test
  (let [close-error (IOException. "Socket close failed")
        first-conn (test-connection [registered] close-error)
        second-conn (test-connection [registered])
        failure (AssertionError. "Handshake failed")
        failed (test-connection [failure])]
    (with-connections
      [first-conn second-conn failed]
      (fn []
        (is (identical? failure
                        (thrown-by #(#'client/new-connectionpool
                                      "localhost" 19001 (UUID/randomUUID) 3 1000))))
        (is (= [close-error] (vec (.getSuppressed failure))))
        (doseq [connection [first-conn second-conn failed]]
          (assert-closed! connection))))))

(defn- with-socket-pair [f]
  (with-open [listener (ServerSocketChannel/open)]
    (.bind listener (InetSocketAddress. "127.0.0.1" 0))
    (with-open [channel (SocketChannel/open (.getLocalAddress listener))
                peer (.accept listener)]
      (let [conn (client/->Connection channel 1000 (ByteBuffer/allocate 65536))]
        (try (f conn peer)
             (finally (client/close conn)))))))

(deftest idle-socket-probe-preserves-live-connections-test
  (with-socket-pair
    (fn [^Connection conn ^SocketChannel peer]
      (let [^SocketChannel channel (.-ch conn)]
        (doseq [blocking? [true false]]
          (.configureBlocking channel blocking?)
          (is (true? (#'client/connection-ready? conn)))
          (is (= blocking? (.isBlocking channel)))))
      ;; Probing performs no round trip and sends no bytes to the peer.
      (.configureBlocking peer false)
      (is (zero? (.read peer (ByteBuffer/allocate 1)))))))

(deftest peer-closed-socket-is-replaced-before-an-unsafe-request-test
  (doseq [request [{:type :open-kv :db-name "db"}
                   {:type :open-transact :args ["db"]}
                   {:type :transact-kv :args ["db"]}]]
    (with-socket-pair
      (fn [^Connection stale ^SocketChannel peer]
        (.shutdownOutput peer)
        ;; Wait for FIN deterministically. EOF does not close the local channel.
        (is (= -1 (.read ^SocketChannel (.-ch stale) (ByteBuffer/allocate 1))))
        (is (.isOpen ^SocketChannel (.-ch stale)))
        (let [fresh (test-connection [registered completed])
              available (doto (ConcurrentLinkedQueue.) (.add stale))
              used (ConcurrentLinkedQueue.)
              pool (client/->ConnectionPool "localhost" 19001 nil 1 1000
                                             available used (AtomicBoolean. false))
              base (client/->Client "user" "password" "localhost" 19001
                                     1 1000 nil pool)
              replacements (atom 0)]
          (try
            (with-redefs [client/new-connection
                          (fn [& _] (swap! replacements inc) (:conn fresh))]
              (is (= completed (client/request base request))))
            (is (= 1 @replacements))
            (is (= [:set-client-id (:type request)] (mapv :type @(:sent fresh))))
            (is (.isEmpty used))
            (is (identical? (:conn fresh) (.peek available)))
            ;; The old server must never receive even the first mutation byte.
            (.configureBlocking peer false)
            (is (= -1 (.read peer (ByteBuffer/allocate 1))))
            (finally (client/close-pool pool))))))))

(deftest transport-failure-after-borrowing-does-not-replay-a-write-test
  (let [connection (test-connection [registered])]
    (with-connections
      [connection]
      (fn []
        (let [pool (#'client/new-connectionpool "localhost" 19001 nil 1 1000)
              base (client/->Client "user" "password" "localhost" 19001
                                     1 1000 nil pool)]
          (try
            (let [error (thrown-by #(client/request base {:type :open-kv :db-name "db"}))]
              (is (= :ha/write-indeterminate (get-in (ex-data error) [:err-data :error])))
              (is (= [:set-client-id :open-kv] (mapv :type @(:sent connection)))))
            (finally (client/close-pool pool))))))))
