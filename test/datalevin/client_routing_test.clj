(ns datalevin.client-routing-test
  (:require
   [clojure.test :refer [deftest is testing]]
   [datalevin.client :as client]
   [datalevin.command :as command]
   [datalevin.constants :as c]
   [datalevin.native-value :as nv]
   [datalevin.protocol :as p])
  (:import
   [datalevin.client ConnectionPool]
   [java.io IOException]
   [java.nio ByteBuffer]
   [java.nio.channels SocketChannel]
   [java.nio.channels.spi SelectorProvider]
   [java.util.concurrent ConcurrentLinkedQueue]))

(defn- with-expired-request-budget [responses f]
  (let [frames    (ConcurrentLinkedQueue.)
        sent      (atom 0)
        channel   (proxy [SocketChannel] [(SelectorProvider/provider)]
                    (write [^ByteBuffer src]
                      (swap! sent inc)
                      (let [n (.remaining src)]
                        (.position src (.limit src))
                        n))
                    (read [^ByteBuffer dst]
                      (if-let [^ByteBuffer frame (.poll frames)]
                        (let [n (.remaining frame)]
                          (.put dst frame)
                          n)
                        (throw (IOException. "Response lost"))))
                    (implConfigureBlocking [_])
                    (implCloseSelectableChannel []))
        conn      (client/->Connection channel 5000 (ByteBuffer/allocate 65536))
        ;; A zero cumulative budget deterministically reaches the deadline
        ;; after the first attempt, without sleeps or changing the clock.
        ^ConnectionPool pool (#'client/connection-pool "localhost" 19001 nil 1 0)
        _         (#'client/install-pool-connection!
                    pool (aget ^objects (.-slots pool) 0) conn)
        base      (client/->Client "user" "password" "localhost" 19001
                                    1 0 nil pool)]
    (doseq [response responses]
      (let [frame (ByteBuffer/allocate 65536)]
        (p/write-message-bf frame response c/message-format-nippy)
        (.flip frame)
        (.add frames frame)))
    (try
      ;; Frames here are preloaded responses, not unsolicited socket data.
      (with-redefs [client/connection-ready? (constantly true)]
        (f base sent)
        (let [returned (client/get-connection pool)]
          (is (identical? conn returned) "the completed attempt releases its connection")
          (client/release-connection pool returned)))
      (is (.isEmpty frames) "all response frames were consumed")
      (finally (client/close-pool pool)))))

(deftest completed-responses-survive-expired-request-budget-test
  (doseq [[op responses expected]
          [[:add-vec [{:type :command-complete :result :added}]
            {:type :command-complete :result :added}]
           [:doc-count [{:type :command-complete :result 42}]
            {:type :command-complete :result 42}]
           [:add-vec [{:type :error-response :message "Rejected"
                       :err-data {:reason :not-leader}}]
            {:type :error-response :message "Rejected"
             :err-data {:reason :not-leader}}]
           [:search [{:type :copy-out-response} [:one :two] {:type :copy-done}]
            {:type :command-complete :result [:one :two]}]]]
    (testing (str op " " responses)
      (with-expired-request-budget
        responses
        (fn [base sent]
          (is (= expected (client/request base {:type op :args ["db"]})))
          (is (= 1 @sent) "a completed response must not cause a resend"))))))

(deftest unfinished-requests-still-respect-expired-request-budget-test
  (testing "a replay-safe transport failure stops at the deadline"
    (with-expired-request-budget
      []
      (fn [base sent]
        (is (thrown-with-msg? clojure.lang.ExceptionInfo
                             #"Timeout in making request"
                             (client/request base {:type :doc-count
                                                   :args ["db"]})))
        (is (= 1 @sent)))))
  (testing "reopening a database does not complete the original request"
    (with-expired-request-budget
      [{:type :reopen :db-name "db" :db-type "kv"}
       {:type :command-complete}]
      (fn [base sent]
        (is (thrown-with-msg? clojure.lang.ExceptionInfo
                             #"Timeout in making request"
                             (client/request base {:type :doc-count
                                                   :args ["db"]})))
        (is (= 2 @sent) "only the original request and the reopen are sent")))))

(def ^:private mutations
  [:add-doc :remove-doc :clear-docs :search-re-index
   :add-vec :remove-vec :clear-vecs :persist-vecs :close-vecs :vec-re-index
   :new-search-engine :new-vector-index])

(def ^:private administrative-mutations
  [:create-user :reset-password :drop-user :create-role :drop-role
   :create-database :assign-role :withdraw-role :grant-permission
   :revoke-permission :close-database :disconnect-client :open :close
   :open-kv :close-kv :start-sampling :stop-sampling])

(defn- request-client [send]
  (reify client/IClient
    (request [_ req] (send req))
    (copy-in [_ _ _ _] nil)
    (disconnect [_] nil)
    (disconnected? [_] false)
    (get-pool [_] nil)
    (get-id [_] nil)))

(deftest successful-unrouted-reads-do-not-build-retry-state-test
  (with-expired-request-budget
    [{:type :command-complete :result :ok}]
    (fn [base _]
      (let [result (locking @#'client/ha-known-db-endpoints
                     (locking @#'client/ha-write-retry-settings
                       (let [result (future (client/normal-request base :doc-count ["db"]))]
                         (is (= :ok (deref result 1000 ::timeout)))
                         result)))]
        (is (= :ok (deref result 2000 ::timeout)))))))

(deftest ordinary-client-requests-do-not-lock-global-state-test
  (with-expired-request-budget
    [{:type :command-complete :result :ok}]
    (fn [base _]
      (let [result (locking @#'client/client-state-refs
                     (locking @#'client/fallback-client-states
                       (let [result (future (client/normal-request base :doc-count ["db"]))]
                         (is (= :ok (deref result 1000 ::timeout)))
                         result)))]
        (is (= :ok (deref result 2000 ::timeout)))))))

(deftest client-native-readers-remain-shared-and-scoped-test
  (let [conn (reify client/IConnection
               (send-n-receive [_ _]
                 {:type :command-complete :result nv/*wire-reader*})
               (close [_] nil))
        pool (reify client/IConnectionPool
               (get-connection [_] conn)
               (release-connection [_ _] nil)
               (close-pool [_] nil)
               (closed-pool? [_] false))
        base (client/->Client "user" "password" "localhost" 19001 1 1000 nil pool)
        target (client/->Client "user" "password" "localhost" 19002 1 1000 nil pool)
        reader (fn [_ _] :database-reader)
        outer (fn [_ _] :outer-reader)
        request (fn [client db-name]
                  (:result (client/request client {:type :pull :args [db-name]})))]
    (#'client/inherit-native-readers! target base)
    ;; Registrations made after inheritance must reach the retry/transaction client.
    (.put ^java.util.concurrent.ConcurrentHashMap
          (#'client/native-reader-cache base) "db" reader)
    (binding [nv/*wire-reader* outer]
      (is (identical? reader (request base "db")))
      (is (identical? reader (request target "db")))
      (is (identical? outer (request target "other-db")))
      (is (identical? outer nv/*wire-reader*)))
    (#'client/set-preferred-ha-endpoint! base "localhost:19003")
    (#'client/set-preferred-ha-read-endpoint! base "db" "localhost:19004")
    (is (= "localhost:19004" (#'client/preferred-ha-read-endpoint base "db")))
    (is (= "localhost:19003" (#'client/preferred-ha-read-endpoint base "other-db")))
    (client/reset-client-state!)
    (is (nil? (request base "db")))
    (is (nil? (request target "db")))
    (is (nil? (#'client/preferred-ha-read-endpoint base "db")))))

(deftest index-mutations-use-write-retries-without-selecting-a-transaction-test
  (doseq [op mutations
          writing? [false true]]
    (testing (str op " writing?=" writing?)
      (let [requests (atom [])
            retries  (atom [])
            base     (request-client
                      (fn [req]
                        (swap! requests conj req)
                        {:type :error-response :message "not leader"
                         :err-data {:error :ha/write-rejected
                                    :retryable? true}}))]
        (with-redefs [client/retry-ha-write-request
                      (fn [_ req _ _]
                        (swap! retries conj req)
                        :retried)]
          (binding [client/*ha-read-min-tx* 42]
            (is (= :retried
                   (if writing?
                     (client/normal-request base op ["db"] true)
                     (client/normal-request base op ["db"])))))
          (is (= [{:type op :args ["db"] :writing? writing?}] @requests))
          (is (= @requests @retries)))))))

(deftest transport-failure-only-fails-over-reads-test
  (let [failure (ex-info "connection lost" {})
        pool    (reify client/IConnectionPool
                  (get-connection [_] (throw failure))
                  (release-connection [_ _] nil)
                  (close-pool [_] nil)
                  (closed-pool? [_] false))
        base    (client/->Client "user" "password" "127.0.0.1" 19001
                                 1 1000 nil pool)
        creates (atom 0)
        target  (request-client (constantly {:type :command-complete
                                             :result :read-result}))]
    (#'client/cache-known-ha-db-endpoints! base "db" ["127.0.0.1:19002"])
    (with-redefs [client/new-client-for-endpoint
                  (fn [& _] (swap! creates inc) target)]
      (doseq [op (into (conj mutations :assoc-opt) administrative-mutations)]
        (is (identical? failure
                        (try (client/normal-request base op ["db"])
                             (catch Exception e e))) (str op)))
      (is (zero? @creates) "mutations cannot be replayed as failed reads")
      (is (= :read-result (client/normal-request base :doc-count ["db"])))
      (is (= 1 @creates)))))

(deftest reopening-indexes-preserves-options-and-wire-shape-test
  (doseq [[op re-index db-type opts]
          [[:new-search-engine :search-re-index "engine"
            {:domain "custom-search" :include-text? true}]
           [:new-vector-index :vec-re-index "index"
            {:domain "custom-vector" :dimensions 7 :metric-type :euclidean}]]]
    (let [requests (atom [])
          base     (request-client
                    (fn [req]
                      (swap! requests conj req)
                      {:type :command-complete}))
          target   (request-client
                    (fn [req]
                      (swap! requests conj req)
                      {:type :command-complete}))]
      (client/normal-request base op ["db" opts])
      (#'client/inherit-index-options! target base)
      (client/open-database target "db" db-type)
      (is (= {:type op :args ["db" opts]} (last @requests)))
      (let [updated (assoc opts :search-opts {:top 3})]
        (client/normal-request target re-index ["db" updated])
        (client/open-database base "db" db-type)
        (is (= {:type op :args ["db" updated]} (last @requests)))))))

(defn- retry-context [base]
  {:client base :host "127.0.0.1" :port 19001 :time-out 1000
   :ha-write-retry-timeout-ms 1000})

(def ^:private client-op
  {:client-op-id "operation-1" :client-op-hash "payload-hash"
   :client-op-response-kind :kv-result})

(deftest preferred-write-lost-reply-does-not-fall-back-test
  (doseq [op mutations]
    (testing (str op)
      (let [commits  (atom 0)
            fallback (atom 0)
            lost     (IOException. "reply lost after commit")
            base     (request-client
                      (fn [_]
                        (swap! fallback inc)
                        {:type :command-complete :result :duplicated}))
            target   (request-client
                      (fn [req]
                        (when (= op (:type req))
                          (swap! commits inc)
                          (throw lost))
                        {:type :command-complete}))]
        (#'client/set-preferred-ha-endpoint! base "127.0.0.1:19002")
        (with-redefs [client/client-retry-context (fn [_] (retry-context base))
                      client/new-client-for-endpoint (fn [& _] target)]
          (let [e (try (client/normal-request base op ["db" :one [1.0 0.0]])
                       (catch Exception e e))
                err (:err-data (ex-data e))]
            (is (= :ha/write-indeterminate (:error err)))
            (is (false? (:retryable? err)))
            (is (true? (:indeterminate? (ex-data e))))
            (is (= "127.0.0.1:19002" (:endpoint err)))
            (is (identical? lost (ex-cause e)))
            (is (= 1 @commits))
            (is (zero? @fallback))))))))

(deftest connection-failure-before-preferred-request-can-fall-back-test
  (let [calls (atom [])
        base  (request-client
               (fn [req]
                 (swap! calls conj req)
                 {:type :command-complete :result :added}))]
    (#'client/set-preferred-ha-endpoint! base "127.0.0.1:19002")
    (with-redefs [client/client-retry-context (fn [_] (retry-context base))
                  client/new-client-for-endpoint
                  (fn [& _] (throw (IOException. "connection refused")))]
      (is (= :added (client/normal-request base :add-vec ["db" :one [1.0]]))))
    (is (= [{:type :add-vec :args ["db" :one [1.0]] :writing? false}]
           @calls))
    (is (identical? base (client/active-ha-request-client base)))))

(deftest ha-retry-stops-after-an-ambiguous-mutation-test
  ;; IDs only protect handlers that actually deduplicate them. Neither forged
  ;; metadata on add-vec nor incomplete transaction metadata permits a replay.
  (doseq [req [{:type :add-vec :args ["db"]}
               (merge client-op {:type :add-vec :args ["db"]})
               {:type :transact-kv :args ["db"]}
               {:type :transact-kv :args ["db"] :client-op-id "operation-1"}]]
    (let [base     (client/->Client "user" "password" "127.0.0.1" 19001
                                    1 1000 nil nil)
          attempts (atom [])
          send     (fn [endpoint _]
                     (swap! attempts conj endpoint)
                     (if (= 1 (count @attempts))
                       (throw (IOException. "reply lost after commit"))
                       {:type :command-complete :result :duplicated}))]
      (with-redefs [client/new-client-for-endpoint (fn [_ _ port] port)]
        (let [e (try
                  (client/retry-ha-write-request
                   base req "not leader"
                   {:error :ha/write-rejected :retryable? true
                    :ha-retry-endpoints ["127.0.0.1:19002" "127.0.0.1:19003"]}
                   send)
                  (catch Exception e e))]
          (is (= :ha/write-indeterminate (:error (:err-data (ex-data e)))))))
      (is (= [19002] @attempts)))))

(deftest replay-safe-requests-can-recover-a-lost-reply-test
  (doseq [req (cons {:type :doc-count :args ["db"]}
                   (for [op [:tx-data :tx-data+db-info :transact-kv]]
                     (merge client-op
                            {:type op :args ["db"]
                             :client-op-response-kind
                             (if (= op :transact-kv) :kv-result op)})))]
    (let [base     (Object.)
          attempts (atom [])
          commits  (atom 0)
          results  (atom {})
          send     (fn [_ request]
                     (swap! attempts conj request)
                     ;; Simulate the server's atomic mutation/result record.
                     (when-let [id (:client-op-id request)]
                       (when-not (contains? @results id)
                         (swap! commits inc)
                         (swap! results assoc id :saved)))
                     (if (= 1 (count @attempts))
                       (throw (IOException. "reply lost after commit"))
                       {:type :command-complete
                        :result (get @results (:client-op-id request) :read)}))]
      (is (= (if (:client-op-id req) :saved :read)
             (#'client/retry-ha-write-request*
              req "not leader"
              {:error :ha/write-rejected :retryable? true
               :ha-retry-endpoints ["127.0.0.1:19002" "127.0.0.1:19003"]}
              (retry-context base) send (constantly nil)
              (fn [_ _ port] port))))
      (is (= [req req] @attempts) "retries preserve the deduplication metadata")
      (is (= (if (:client-op-id req) 1 0) @commits)))))

(deftest pooled-request-does-not-resend-an-ambiguous-mutation-test
  (doseq [op (concat (keys (filter (comp :replica-write? val) command/properties))
                    administrative-mutations
                    [:unknown-command])]
    (testing (str op)
      (let [sent     (atom 0)
            released (atom 0)
            closed   (atom 0)
            lost     (IOException. "reply lost after commit")
            conn     (reify client/IConnection
                       (send-n-receive [_ _] (swap! sent inc) (throw lost))
                       (send-only [_ _] nil)
                       (receive [_] nil)
                       (close [_] (swap! closed inc)))
            pool     (reify client/IConnectionPool
                       (get-connection [_] conn)
                       (release-connection [_ c]
                         (is (identical? conn c))
                         (swap! released inc))
                       (close-pool [_] nil)
                       (closed-pool? [_] false))
            base     (client/->Client "user" "password" "127.0.0.1" 19001
                                      1 1000 nil pool)
            e        (try (client/request base {:type op :args ["db"]})
                          (catch Exception e e))]
        (is (= :ha/write-indeterminate (:error (:err-data (ex-data e)))))
        (is (identical? lost (ex-cause e)))
        (is (= 1 @sent @released @closed))))))
