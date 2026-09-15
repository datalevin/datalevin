(ns datalevin.client-connection-test
  (:require
   [clojure.test :refer [deftest is testing]]
   [datalevin.client :as client]
   [datalevin.constants :as c]
   [datalevin.protocol :as p])
  (:import
   [datalevin.client Connection ConnectionPool ConnectionSlot]
   [java.io IOException]
   [java.net InetSocketAddress StandardSocketOptions]
   [java.nio ByteBuffer]
   [java.nio.channels Selector ServerSocketChannel SocketChannel]
   [java.nio.channels.spi SelectorProvider]
   [java.util Arrays UUID]
   [java.util.concurrent ConcurrentHashMap ConcurrentLinkedQueue]
   [java.util.concurrent.atomic AtomicBoolean AtomicInteger AtomicReference]))

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
     (#'client/set-conn-wire-opts! conn (p/default-wire-opts))
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

(defn- pool-with-connections [connections time-out]
  (let [^ConnectionPool pool (#'client/connection-pool
                               "localhost" 19001 nil (count connections) time-out)]
    (doseq [[i conn] (map-indexed vector connections)]
      (#'client/install-pool-connection! pool (aget ^objects (.-slots pool) (int i)) conn))
    pool))

(defn- idle-connections [^ConnectionPool pool]
  (into []
        (keep (fn [^ConnectionSlot slot]
                (when-not (.get ^AtomicBoolean (.-borrowed? slot))
                  (.get ^AtomicReference (.-connection slot)))))
        (.-slots pool)))

(defn- thrown-by [f]
  (try (f) nil (catch Throwable t t)))

(defn- await-pool-waiter [^ConnectionPool pool]
  (let [deadline (+ (System/nanoTime) 2000000000)]
    (loop []
      (cond
        (pos? (.get ^AtomicInteger (.-waiters pool))) true
        (>= (System/nanoTime) deadline) false
        :else (do (Thread/sleep 1) (recur))))))

(deftest worker-prefers-its-last-connection-without-reserving-it-test
  (let [connections (repeatedly 2 #(test-connection []))]
    (with-connections
      connections
      (fn []
        (let [pool (pool-with-connections (mapv :conn connections) 1000)]
          (try
            (let [first (client/get-connection pool)
                  nested (client/get-connection pool)]
              (is (not (identical? first nested)) "nested borrows remain exclusive")
              (client/release-connection pool first)
              (client/release-connection pool nested)
              (let [again (client/get-connection pool)]
                (is (identical? nested again))
                (client/release-connection pool again)))
            (let [other (future
                          (let [a (client/get-connection pool)
                                b (client/get-connection pool)]
                            (try #{a b}
                                 (finally
                                   (client/release-connection pool a)
                                   (client/release-connection pool b)))))]
              (is (= (set (map :conn connections)) (deref other 2000 ::timeout))
                  "an idle worker does not retain either connection"))
            (finally (client/close-pool pool))))))))

(deftest ordinary-checkout-and-return-do-not-take-the-pool-monitor-test
  (let [connection (test-connection [])]
    (with-connections
      [connection]
      (fn []
        (let [pool (pool-with-connections [(:conn connection)] 1000)]
          (try
            (let [work (locking pool
                         (let [work (future
                                      (let [conn (client/get-connection pool)]
                                        (client/release-connection pool conn)
                                        conn))]
                           (is (identical? (:conn connection) (deref work 1000 ::timeout)))
                           work))]
              (is (identical? (:conn connection) (deref work 2000 ::timeout))))
            (finally (client/close-pool pool))))))))

(deftest return-and-shutdown-wake-waiting-borrowers-test
  (doseq [shutdown? [false true]]
    (let [connection (test-connection [])]
      (with-connections
        [connection]
        (fn []
          (let [pool (pool-with-connections [(:conn connection)] 30000)
                conn (client/get-connection pool)
                waiting (future
                          (try
                            (let [borrowed (client/get-connection pool)]
                              (client/release-connection pool borrowed)
                              borrowed)
                            (catch Exception e e)))]
            (try
              (is (await-pool-waiter pool))
              (if shutdown?
                (client/close-pool pool)
                (client/release-connection pool conn))
              (let [result (deref waiting 2000 ::timeout)]
                (if shutdown?
                  (is (= "This client is closed" (some-> result ex-message)))
                  (is (identical? conn result))))
              (finally
                (client/close-pool pool)
                (client/release-connection pool conn)
                (deref waiting 2000 nil)))))))))

(deftest shutdown-closes-a-connection-being-probed-test
  (let [connection (test-connection [])
        pool (pool-with-connections [(:conn connection)] 1000)
        entered (promise)
        proceed (promise)]
    (with-redefs [client/connection-ready?
                  (fn [_] (deliver entered true) @proceed true)]
      (let [borrow (future (thrown-by #(client/get-connection pool)))]
        (try
          (is (true? (deref entered 2000 false)))
          (client/close-pool pool)
          (assert-closed! connection)
          (deliver proceed true)
          (is (= "This client is closed" (some-> (deref borrow 2000 nil) ex-message)))
          (finally
            (deliver proceed true)
            (deref borrow 2000 nil)
            (client/close-pool pool)))))))

(deftest shutdown-during-replacement-closes-the-unpublished-socket-test
  (let [initial (test-connection [registered])
        replacement (test-connection [registered])
        entered (promise)
        proceed (promise)]
    (with-connections
      [initial replacement]
      (fn []
        (let [pool (#'client/new-connectionpool "localhost" 19001 nil 1 1000)
              connect @#'client/new-connection]
          (client/close (:conn initial))
          (with-redefs [client/new-connection
                        (fn [& args]
                          (deliver entered true)
                          @proceed
                          (apply connect args))]
            (let [borrow (future (thrown-by #(client/get-connection pool)))]
              (try
                (is (true? (deref entered 2000 false)))
                (client/close-pool pool)
                (deliver proceed true)
                (is (= "This client is closed" (some-> (deref borrow 2000 nil) ex-message)))
                (assert-closed! initial)
                (assert-closed! replacement)
                (finally
                  (deliver proceed true)
                  (deref borrow 2000 nil)
                  (client/close-pool pool))))))))))

(deftest contending-workers-never-share-a-borrowed-connection-test
  (let [connections (vec (repeatedly 4 #(test-connection [])))]
    (with-connections
      connections
      (fn []
        (let [pool (pool-with-connections (mapv :conn connections) 10000)
              active (ConcurrentHashMap.)
              duplicates (ConcurrentLinkedQueue.)
              start (promise)
              workers (mapv (fn [worker]
                              (future
                                @start
                                (dotimes [iteration 500]
                                  (let [conn (client/get-connection pool)]
                                    (try
                                      (when (.putIfAbsent active conn worker)
                                        (.add duplicates [worker iteration]))
                                      (when (zero? (bit-and (long iteration) 15)) (Thread/yield))
                                      (.remove active conn)
                                      (finally (client/release-connection pool conn)))))
                                :done))
                            (range 16))]
          (try
            (deliver start true)
            (is (= (repeat 16 :done) (mapv #(deref % 10000 ::timeout) workers)))
            (is (.isEmpty duplicates))
            (is (= (set (map :conn connections)) (set (idle-connections pool))))
            (finally (client/close-pool pool))))))))

(deftest retry-safe-reads-skip-the-idle-probe-but-writes-still-probe-test
  (let [connection (test-connection [registered completed completed completed])
        probes (atom 0)]
    (with-connections
      [connection]
      (fn []
        (let [pool (#'client/new-connectionpool "localhost" 19001 nil 1 1000)
              base (client/->Client "user" "password" "localhost" 19001 1 1000 nil pool)]
          (try
            (with-redefs [client/connection-ready? (fn [_] (swap! probes inc) true)]
              (is (= completed (client/request base {:type :doc-count :args ["db"]})))
              (is (zero? (long @probes)))
              (is (= completed (client/request base {:type :doc-count :args ["db"] :writing? true})))
              (is (= 1 @probes))
              (is (= completed (client/request base {:type :open-kv :db-name "db"})))
              (is (= 2 @probes)))
            (finally (client/close-pool pool))))))))

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
                                     "localhost" 19001 (UUID/randomUUID) 1 1000)]
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
              (is (= [(:conn initial)] (idle-connections pool))))
            (let [conn (client/get-connection pool)]
              (is (identical? (:conn recovered) conn))
              (is (.isOpen ^SocketChannel (:channel recovered)))
              (is (zero? @(:closed recovered)))
              (is (= completed (client/send-n-receive conn {:type :ping})))
              (client/release-connection pool conn)
              (is (= [conn] (idle-connections pool))))
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

(deftest connection-owned-options-survive-registry-removal-test
  (with-socket-pair
    (fn [^Connection conn ^SocketChannel peer]
      (.remove ^ConcurrentHashMap @#'client/connection-wire-opts (.-ch conn))
      (doseq [options [{:compression :zstd :compression-threshold 0}
                       (p/default-wire-opts)]]
        (#'client/set-conn-wire-opts! conn options)
        (let [request {:type :echo :args [(apply str (repeat 100000 "x"))]}
              response {:type :command-complete :result (:args request)}
              reading (future (client/send-n-receive conn request))
              [received _] (p/receive-ch peer (ByteBuffer/allocate 1024)
                                        options 1000)]
          (is (= request received))
          (p/write-message-blocking peer (ByteBuffer/allocate 200000) response options)
          (is (= response (deref reading 2000 ::timeout))))))))

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
              pool (pool-with-connections [stale] 1000)
              base (client/->Client "user" "password" "localhost" 19001
                                     1 1000 nil pool)
              replacements (atom 0)]
          (try
            (with-redefs [client/new-connection
                          (fn [& _] (swap! replacements inc) (:conn fresh))]
              (is (= completed (client/request base request))))
            (is (= 1 @replacements))
            (is (= [:set-client-id (:type request)] (mapv :type @(:sent fresh))))
            (is (= [(:conn fresh)] (idle-connections pool)))
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

(deftest a-read-retries-on-a-peer-closed-socket-without-an-idle-probe-test
  (with-socket-pair
    (fn [^Connection stale ^SocketChannel peer]
      (.shutdownOutput peer)
      (is (= -1 (.read ^SocketChannel (.-ch stale) (ByteBuffer/allocate 1))))
      (let [fresh (test-connection [registered completed])
            pool (pool-with-connections [stale] 1000)
            base (client/->Client "user" "password" "localhost" 19001 1 1000 nil pool)
            probes (atom 0)]
        (try
          (with-redefs [client/new-connection (fn [& _] (:conn fresh))
                        client/connection-ready? (fn [_] (swap! probes inc) true)]
            (is (= completed (client/request base {:type :doc-count :args ["db"]}))))
          (is (zero? (long @probes)))
          (is (= [:set-client-id :doc-count] (mapv :type @(:sent fresh))))
          (is (= [(:conn fresh)] (idle-connections pool)))
          (finally (client/close-pool pool)))))))

(defn- await-read-selector [^Connection conn]
  (let [deadline (+ (System/nanoTime) 2000000000)]
    (loop []
      (or @(.-read-selector conn)
          (when (< (System/nanoTime) deadline)
            (Thread/sleep 1)
            (recur))))))

(deftest timed-receives-reuse-selector-across-fragmented-and-large-frames-test
  (with-socket-pair
    (fn [^Connection conn ^SocketChannel peer]
      (.configureBlocking ^SocketChannel (.-ch conn) false)
      (let [selectors (atom [])]
        (doseq [message [{:result :small}
                         {:result (apply str (repeat 100000 "x"))}
                         {:result :small-again}]]
          (let [reading (future (client/receive conn))
                selector (await-read-selector conn)
                frame (ByteBuffer/allocate 200000)]
            (is (some? selector))
            (swap! selectors conj selector)
            (p/write-message-bf frame message c/message-format-nippy)
            (.flip frame)
            (let [prefix (doto (.duplicate frame) (.limit 3))]
              (p/send-all peer prefix))
            (.position frame 3)
            (p/send-all peer frame)
            (is (= message (deref reading 2000 ::timeout)))
            (is (false? (.isBlocking ^SocketChannel (.-ch conn))))
            (is (true? (#'client/connection-ready? conn)))))
        (is (every? #(identical? (first @selectors) %) @selectors))
        (is (= 1 (.size (.keys ^Selector (first @selectors)))))
        (client/close conn)
        (is (not (.isOpen ^Selector (first @selectors))))))))

(deftest reusable-receive-selector-honors-a-fresh-deadline-test
  (with-socket-pair
    (fn [^Connection conn ^SocketChannel peer]
      (let [^SocketChannel channel (.-ch conn)
            selector (.-read-selector conn)
            buffer (ByteBuffer/allocate 256)]
        (.configureBlocking channel false)
        (dotimes [_ 2]
          (let [error (thrown-by #(p/receive-ch channel buffer nil 25 selector))]
            (is (= :socket/timeout (:error (ex-data error))))
            (is (.isOpen ^Selector @selector))))
        (p/write-message-blocking peer (ByteBuffer/allocate 256) completed)
        (is (= completed (first (p/receive-ch channel buffer nil 1000 selector))))))))

(deftest close-releases-selector-and-unblocks-a-pending-receive-test
  (with-socket-pair
    (fn [^Connection conn _]
      (.configureBlocking ^SocketChannel (.-ch conn) false)
      (let [reading (future (thrown-by #(client/receive conn)))
            selector (await-read-selector conn)]
        (is (some? selector))
        (client/close conn)
        (is (instance? Throwable (deref reading 1000 ::timeout)))
        (is (nil? @(.-read-selector conn)))
        (is (not (.isOpen ^Selector selector)))))))

(deftest interrupted-select-does-not-spin-until-the-receive-deadline-test
  (with-socket-pair
    (fn [^Connection conn _]
      (.configureBlocking ^SocketChannel (.-ch conn) false)
      (let [result (promise)
            reader (Thread. ^Runnable #(deliver result (thrown-by (fn [] (client/receive conn)))))]
        (try
          (.start reader)
          (is (some? (await-read-selector conn)))
          (.interrupt reader)
          (.join reader 500)
          (is (not (.isAlive reader)))
          (is (instance? Throwable (deref result 1000 ::timeout)))
          (finally
            (client/close conn)
            (.join reader 1000)))))))

(deftest nonblocking-send-preserves-bytes-under-backpressure-test
  (with-socket-pair
    (fn [^Connection conn ^SocketChannel peer]
      (let [^SocketChannel channel (.-ch conn)
            bytes (byte-array (* 8 1024 1024))
            received (ByteBuffer/allocate (alength bytes))]
        (.configureBlocking channel false)
        (.setOption channel StandardSocketOptions/SO_SNDBUF (int 4096))
        (dotimes [i (alength bytes)] (aset-byte bytes i (unchecked-byte i)))
        (let [sending (future (p/send-all channel (ByteBuffer/wrap bytes)) :sent)]
          (is (= ::pending (deref sending 50 ::pending)))
          (let [reading (future
                          (while (.hasRemaining received)
                            (when (neg? (.read peer received))
                              (throw (IOException. "Unexpected EOF"))))
                          :received)]
            (try
              (is (= :sent (deref sending 10000 ::timeout)))
              (is (= :received (deref reading 10000 ::timeout)))
              (is (Arrays/equals bytes (.array received)))
              (finally
                (client/close conn)
                (.close peer)
                (future-cancel sending)
                (future-cancel reading)))))))))

(deftest interrupted-send-closes-a-partially-written-connection-test
  (with-socket-pair
    (fn [^Connection conn _]
      (let [^SocketChannel channel (.-ch conn)
            buffer (ByteBuffer/allocate (* 8 1024 1024))
            result (promise)
            writer (Thread. ^Runnable
                            #(deliver result (thrown-by (fn [] (p/send-all channel buffer)))))]
        (.configureBlocking channel false)
        (.setOption channel StandardSocketOptions/SO_SNDBUF (int 4096))
        (try
          (.start writer)
          (is (= ::pending (deref result 50 ::pending)))
          (.interrupt writer)
          (.join writer 1000)
          (is (not (.isAlive writer)))
          (is (instance? InterruptedException (deref result 1000 ::timeout)))
          (is (not (.isOpen channel)))
          (finally
            (client/close conn)
            (.join writer 1000)))))))
