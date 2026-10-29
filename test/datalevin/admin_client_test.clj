(ns datalevin.admin-client-test
  (:require [clojure.test :refer [deftest is]]
            [datalevin.client :as client]
            [datalevin.constants :as c]
            [datalevin.protocol :as protocol])
  (:import [datalevin.client ConnectionPool]
           [java.nio ByteBuffer]
           [java.nio.channels SocketChannel]
           [java.nio.channels.spi SelectorProvider]
           [java.util.concurrent ConcurrentLinkedQueue]
           [java.util.concurrent.atomic AtomicReference]))

(defn- transport [responses & [{:keys [timeout-ms read-delay-ms] :or {timeout-ms 5000 read-delay-ms 0}}]]
  (let [frames (ConcurrentLinkedQueue.) requests (atom []) received (atom 0)
        channel (proxy [SocketChannel] [(SelectorProvider/provider)]
                  (write [^ByteBuffer src]
                    (let [copy (.duplicate src)
                          fmt (.get copy)
                          _ (.getInt copy)]
                      (swap! requests conj (protocol/read-value fmt copy)))
                    (let [n (.remaining src)] (.position src (.limit src)) n))
                  (read [^ByteBuffer dst]
                    (if (< @received (count @requests))
                      (if-let [^ByteBuffer frame (.poll frames)]
                        (let [n (.remaining frame)]
                          (when (pos? read-delay-ms) (Thread/sleep read-delay-ms))
                          (.put dst frame) (swap! received inc) n)
                        0)
                      0))
                  (implConfigureBlocking [_])
                  (implCloseSelectableChannel []))
        connection (client/->Connection channel 5000 (ByteBuffer/allocate 65536))
        ^ConnectionPool pool (#'client/connection-pool "localhost" 19001 "session" 1 5000)
        _ (#'client/install-pool-connection! pool (aget ^objects (.-slots pool) 0) connection)
        client (client/->Client "user" "password" "localhost" 19001 1 timeout-ms "session" pool)]
    (doseq [response responses]
      (let [frame (ByteBuffer/allocate 65536)]
        (protocol/write-message-bf frame response c/message-format-nippy)
        (.flip frame)
        (.add frames frame)))
    {:client client :pool pool :connection connection :requests requests}))

(def target {:db-name "orders" :db-type :key-value :db-id 42})
(def support {:type :command-complete
              :result (assoc target :admin-capabilities #{:guarded-maintenance :wal-gc-dry-run})})
(def gc-result {:type :command-complete :result {:ok? false :skipped? true :reason :wal-disabled}})

(deftest status-reopen-response-never-sends-an-open
  (let [frame (ByteBuffer/allocate 65536)
        sent (atom 0)
        _ (protocol/write-message-bf frame {:type :reopen :db-name "closed" :db-type "kv"}
                                     c/message-format-nippy)
        _ (.flip frame)
        channel (proxy [SocketChannel] [(SelectorProvider/provider)]
                  (write [^ByteBuffer src]
                    (swap! sent inc)
                    (let [n (.remaining src)] (.position src (.limit src)) n))
                  (read [^ByteBuffer dst]
                    (if (and (pos? @sent) (.hasRemaining frame))
                      (let [n (.remaining frame)] (.put dst frame) n)
                      0))
                  (implConfigureBlocking [_])
                  (implCloseSelectableChannel []))
        connection (client/->Connection channel 5000 (ByteBuffer/allocate 65536))
        ^ConnectionPool pool (#'client/connection-pool "localhost" 19001 nil 1 5000)
        _ (#'client/install-pool-connection! pool (aget ^objects (.-slots pool) 0) connection)
        transport (client/->Client "user" "password" "localhost" 19001 1 5000 nil pool)]
    (try
      (let [error (try (client/admin-status transport "closed") nil (catch Exception e e))]
        (is (= :admin/unexpected-reopen (:error (ex-data error))))
        (is (= 1 @sent)))
      (finally (client/close-pool pool)))))

(deftest unsupported-servers-never-receive-maintenance
  (doseq [response [{:type :error-response :message "Unknown message type :admin-status"}
                   {:type :command-complete :result (assoc target :admin-capabilities #{})}
                   {:type :command-complete
                    :result (assoc target :admin-capabilities #{:guarded-maintenance})}]]
    (let [{:keys [client pool requests]} (transport [response])]
      (try
        (let [error (try (client/admin-gc! client target {:dry-run? true})
                         nil (catch Exception e e))]
          (is (= :admin/unsupported-server (:error (ex-data error))))
          (is (= [:admin-status] (mapv :type @requests))))
        (finally (client/close-pool pool))))))

(deftest reopen-preserves-identity-and-never-uses-a-creating-open
  (let [{:keys [client pool requests]}
        (transport [support {:type :reopen :db-name "orders" :db-type "kv"}
                    {:type :command-complete} gc-result])]
    (try
      (is (= :wal-disabled (:reason (client/admin-gc! client target {:dry-run? true}))))
      (is (= [:admin-status :gc-txlog-segments! :open-kv :gc-txlog-segments!]
             (mapv :type @requests)))
      (doseq [request (rest @requests)]
        (is (= 42 (:expected-db-id request)))
        (is (= :key-value (:expected-db-type request)))
        (is (= #{:guarded-maintenance :wal-gc-dry-run} (:admin-required-capabilities request))))
      (is (true? (:existing-only? (nth @requests 2))))
      (finally (client/close-pool pool)))))

(deftest capabilities-are-invalidated-on-session-publication
  (let [old (transport [support gc-result])
        replacement (transport [{:type :command-complete
                                 :result (assoc target :admin-capabilities #{})}])
        client (:client old)]
    (try
      (client/admin-gc! client target {})
      (let [attempt {}
            state (.clientState ^datalevin.client.IClientInternals client)]
        (.set ^AtomicReference (.-renewal ^datalevin.client.ClientState state) attempt)
        (is (.publishSession ^datalevin.client.IClientInternals client
                             (:pool old) attempt "renewed" (:pool replacement))))
      (let [error (try (client/admin-gc! client target {}) nil (catch Exception e e))]
        (is (= :admin/unsupported-server (:error (ex-data error))))
        (is (= [:admin-status] (mapv :type @(:requests replacement)))))
      (finally (client/close-pool (:pool old)) (client/close-pool (:pool replacement))))))

(deftest replacing-a-pooled-connection-rechecks-capabilities
  (let [old (transport [support gc-result])
        replacement (transport [{:type :command-complete
                                 :result (assoc target :admin-capabilities #{})}])]
    (try
      (client/admin-gc! (:client old) target {})
      (#'client/install-pool-connection!
        (:pool old) (aget ^objects (.-slots ^ConnectionPool (:pool old)) 0) (:connection replacement))
      (let [error (try (client/admin-gc! (:client old) target {}) nil (catch Exception e e))]
        (is (= :admin/unsupported-server (:error (ex-data error))))
        (is (= [:admin-status] (mapv :type @(:requests replacement)))))
      (finally (client/close-pool (:pool old)) (client/close-pool (:pool replacement))))))

(deftest maintenance-busy-rejections-are-not-replayed
  (let [{:keys [client pool requests]}
        (transport [support {:type :error-response :message "Busy"
                            :err-data {:error :server/busy :retryable? true}}])]
    (try
      (let [error (try (client/admin-gc! client target {}) nil (catch Exception e e))]
        (is (= :server/busy (get-in (ex-data error) [:err-data :error])))
        (is (= [:admin-status :gc-txlog-segments!] (mapv :type @requests))))
      (finally (client/close-pool pool)))))

(deftest successive-top-level-requests-have-independent-budgets
  (let [{:keys [client pool requests]} (transport [support gc-result gc-result]
                                                 {:timeout-ms 400 :read-delay-ms 250})
        started (System/nanoTime)]
    (try
      (is (= :wal-disabled (:reason (client/admin-gc! client target {}))))
      (is (= :wal-disabled (:reason (client/admin-gc! client target {}))))
      (is (> (/ (- (System/nanoTime) started) 1e6) 400))
      (is (= [:admin-status :gc-txlog-segments! :gc-txlog-segments!] (mapv :type @requests)))
      (finally (client/close-pool pool)))))

(deftest implicit-open-does-not-reset-the-maintenance-budget
  (let [{:keys [client pool requests]}
        (transport [support {:type :reopen :db-name "orders" :db-type "kv"}
                    {:type :command-complete} gc-result]
                   {:timeout-ms 400 :read-delay-ms 250})]
    (try
      (let [error (try (client/admin-gc! client target {}) nil (catch Exception e e))]
        (is error)
        (is (= :deadline-exceeded (:termination-reason (ex-data error))))
        (is (= [:admin-status :gc-txlog-segments! :open-kv] (mapv :type @requests))))
      (finally (client/close-pool pool)))))
