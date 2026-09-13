(ns datalevin.server.admission-test
  (:require
   [clojure.test :refer [deftest is use-fixtures]]
   [datalevin.client :as client]
   [datalevin.server :as server]
   [datalevin.test.core :refer [allocate-port db-fixture]]
   [datalevin.util :as u])
  (:import
   [datalevin.server Server]
   [java.util UUID]
   [java.util.concurrent CountDownLatch ExecutorService Semaphore
    ThreadPoolExecutor TimeUnit]))

(use-fixtures :once db-fixture)

(defn- await! [task]
  (let [v (deref task 5000 ::timeout)]
    (when (= ::timeout v) (throw (ex-info "Admission test timed out" {})))
    v))

(defn- await-condition! [pred]
  (let [deadline (+ (System/currentTimeMillis) 5000)]
    (loop []
      (when-not (pred)
        (when (>= (System/currentTimeMillis) deadline)
          (throw (ex-info "Admission condition timed out" {})))
        (Thread/sleep 5)
        (recur)))))

(defn- with-server [f]
  (let [root (u/tmp-dir (str "server-admission-" (UUID/randomUUID)))
        port (allocate-port)
        ^Server srv (server/create {:root root :port port
                                    :worker-threads 1 :worker-queue-size 1
                                    :transaction-threads 2 :background-threads 2
                                    :transaction-lock-timeout-ms 50})
        clients (atom [])]
    (try
      (server/start srv)
      (dotimes [_ 3]
        (swap! clients conj
               (client/new-client (str "dtlv://datalevin:datalevin@localhost:" port)
                                  {:pool-size 1 :time-out 3000})))
      (f srv @clients port)
      (finally
        (doseq [c @clients]
          (try (client/disconnect c) (catch Exception _ nil)))
        (server/stop srv)
        (u/delete-files root)))))

(defn- request! [c type db-name]
  (client/request c {:type type :args [db-name]}))

(defn- complete! [response]
  (is (= :command-complete (:type response)) (pr-str response)))

(defn- busy! [response reason]
  (is (= :error-response (:type response)) (pr-str response))
  (is (= :server/busy (get-in response [:err-data :error])))
  (is (true? (get-in response [:err-data :retryable?])))
  (is (= reason (get-in response [:err-data :reason]))))

(defn- assert-released! [^Server srv db-name]
  (await-condition! #(nil? (:runner (get (.-dbs srv) db-name))))
  (is (= 1 (.availablePermits ^Semaphore (:lock (get (.-dbs srv) db-name))))))

(defn- saturate! [^ThreadPoolExecutor executor]
  (let [started (promise)
        release (CountDownLatch. 1)]
    (.execute executor
              ^Runnable #(do (deliver started true) (.await release)))
    (await! started)
    (.execute executor ^Runnable (fn []))
    (is (zero? (.remainingCapacity (.getQueue executor))))
    #(.countDown release)))

(deftest transaction-contention-is-bounded-for-both-store-types-test
  (with-server
    (fn [^Server srv [owner contender observer] _]
      (doseq [[db-type open abort]
              [["kv" :open-transact-kv :abort-transact-kv]
               ["datalog" :open-transact :abort-transact]]]
        (let [db-name (str "busy-" db-type)]
          (doseq [c [owner contender observer]]
            (client/open-database c db-name db-type))
          (complete! (request! owner open db-name))
          (busy! (await! (future (request! contender open db-name)))
                 :write-transaction-open)
          (when (= db-type "datalog")
            ;; The direct-write helper must have the same deadline as opens.
            (busy! (await!
                     (future (client/request contender
                                             {:type :tx-data :mode :request
                                              :args [db-name [] false]})))
                   :write-transaction-open))
          (is (zero? (.availablePermits
                       ^Semaphore (:lock (get (.-dbs srv) db-name))))
              "a timed-out contender must not release the owner's writer slot")
          (complete! (request! observer :list-databases nil))
          (complete! (request! owner abort db-name))
          (assert-released! srv db-name))))))

(deftest idle-transactions-have-a-cap-and-do-not-occupy-request-workers-test
  (with-server
    (fn [^Server srv [first-client second-client observer] _]
      (doseq [[c db-name] [[first-client "first"] [second-client "second"]
                           [observer "third"]]]
        (client/open-database c db-name "kv"))
      (complete! (request! first-client :open-transact-kv "first"))
      (complete! (request! second-client :open-transact-kv "second"))
      (busy! (request! observer :open-transact-kv "third") :transaction-capacity)
      (is (nil? (:runner (get (.-dbs srv) "third"))))
      (is (nil? (:lock (get (.-dbs srv) "third"))))
      (complete! (request! observer :list-databases nil))
      (complete! (request! first-client :close-transact-kv "first"))
      (complete! (request! second-client :abort-transact-kv "second"))
      (assert-released! srv "first")
      (assert-released! srv "second")
      (await-condition! #(zero? (.getActiveCount ^ThreadPoolExecutor
                                                (:transactions (.-execution srv)))))
      (complete! (request! observer :open-transact-kv "third"))
      (complete! (request! observer :close-transact-kv "third")))))

(deftest saturated-request-workers-still-allow-control-and-socket-registration-test
  (with-server
    (fn [^Server srv [owner observer] port]
      (doseq [[open end db-type] [[:open-transact-kv :close-transact-kv "kv"]
                                  [:open-transact-kv :abort-transact-kv "kv"]
                                  [:open-transact :close-transact "datalog"]
                                  [:open-transact :abort-transact "datalog"]]]
        (let [db-name (str "control-" (name end))]
          (client/open-database owner db-name db-type)
          (complete! (request! owner open db-name))
          (let [release! (saturate! (.-work-executor srv))]
            (try
              (busy! (request! observer :list-databases nil) :worker-capacity)
              ;; A new socket must be accepted and its handshake processed even
              ;; with the request executor and its queue completely occupied.
              (let [pool (#'client/new-connectionpool
                           "localhost" port (client/get-id observer) 1 3000)]
                (client/close-pool pool))
              (complete! (await! (future (request! owner end db-name))))
              (assert-released! srv db-name)
              (finally (release!))))
          (await-condition! #(and (zero? (.getActiveCount ^ThreadPoolExecutor
                                                          (.-work-executor srv)))
                                  (.isEmpty (.getQueue ^ThreadPoolExecutor
                                                       (.-work-executor srv)))))))
      (complete! (request! observer :list-databases nil)))))

(deftest saturated-routing-closes-rejected-connection-and-aborts-its-transaction-test
  (with-server
    (fn [^Server srv [owner observer] _]
      (client/open-database owner "disconnect" "kv")
      (complete! (request! owner :open-transact-kv "disconnect"))
      (let [release! (saturate! (:routing (.-execution srv)))]
        (try
          ;; No request can run inline on the event loop. Rejection closes this
          ;; socket and signals cleanup directly to its independent runner.
          (is (thrown? Exception
                       (client/request owner {:type :close-transact-kv
                                              :args ["disconnect"]})))
          (assert-released! srv "disconnect")
          (finally (release!))))
      (await-condition! #(zero? (.getActiveCount ^ThreadPoolExecutor
                                                (:routing (.-execution srv)))))
      (complete! (request! observer :list-databases nil)))))

(deftest background-jobs-do-not-occupy-request-workers-test
  (with-server
    (fn [^Server srv [_ observer] _]
      (let [started (CountDownLatch. 2)
            release (CountDownLatch. 1)
            ^ExecutorService executor (:background (.-execution srv))]
        (try
          (dotimes [_ 2]
            (.execute executor ^Runnable #(do (.countDown started) (.await release))))
          (is (.await started 5 TimeUnit/SECONDS))
          (complete! (request! observer :list-databases nil))
          (finally (.countDown release)))))))
