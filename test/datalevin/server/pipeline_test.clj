(ns datalevin.server.pipeline-test
  (:require
   [clojure.test :refer [deftest is testing use-fixtures]]
   [datalevin.client :as client]
   [datalevin.constants :as c]
   [datalevin.protocol :as p]
   [datalevin.server :as server]
   [datalevin.test.core :refer [allocate-port db-fixture]]
   [datalevin.util :as u])
  (:import
   [datalevin.server Runner Server]
   [java.net InetSocketAddress]
   [java.nio ByteBuffer]
   [java.nio.channels SocketChannel]
   [java.util UUID]
   [java.util.concurrent CountDownLatch LinkedBlockingQueue Semaphore
    ThreadPoolExecutor TimeUnit]))

(use-fixtures :once db-fixture)

(defn- with-server [f]
  (let [root (u/tmp-dir (str "server-pipeline-" (UUID/randomUUID)))
        port (allocate-port)
        srv (server/create {:root root :port port :worker-threads 2
                            :worker-queue-size 2 :transaction-threads 2})]
    (try
      (server/start srv)
      (let [observer (client/new-client
                       (str "dtlv://datalevin:datalevin@localhost:" port)
                       {:pool-size 1 :time-out 5000})]
        (try (f srv observer port)
             (finally (client/disconnect observer))))
      (finally
        (server/stop srv)
        (u/delete-files root)))))

(defn- connect ^SocketChannel [port]
  (SocketChannel/open (InetSocketAddress. "localhost" (int port))))

(defn- frames ^ByteBuffer [messages]
  (let [bf (ByteBuffer/allocate (* 4 (long c/+buffer-size+)))]
    (doseq [message messages] (p/write-message-bf bf message))
    (.flip bf)))

(defn- send! [ch messages]
  (p/send-all ch (frames messages)))

(defn- receiver [ch]
  (let [buffer (volatile! (ByteBuffer/allocate 4096))]
    (fn []
      (let [[response bf] (p/receive-ch ch @buffer nil 5000)]
        (vreset! buffer bf)
        response))))

(defn- handshake [observer]
  {:type :set-client-id :client-id (client/get-id observer)})

(defn- complete! [response]
  (is (= :command-complete (:type response)) (pr-str response)))

(defn- await-condition! [pred]
  (let [deadline (+ (System/currentTimeMillis) 5000)]
    (loop []
      (when-not (pred)
        (when (>= (System/currentTimeMillis) deadline)
          (throw (ex-info "Pipeline condition timed out" {})))
        (Thread/sleep 5)
        (recur)))))

(deftest extract-compacts-before-transferring-buffer-ownership-test
  (let [bf (frames [{:type :first} {:type :second}])
        decoded (atom [])]
    (.position bf (.limit bf))
    (.limit bf (.capacity bf))
    (is (true?
          (p/extract-message
            bf (fn [fmt bytes]
                 (swap! decoded conj (p/read-value fmt bytes))
                 ;; A handler such as copy-in must see only its following
                 ;; frames, already in write mode, when it takes ownership.
                 (is (true?
                       (p/extract-message
                         bf (fn [fmt bytes]
                              (swap! decoded conj (p/read-value fmt bytes))))))))))
    (is (= [{:type :first} {:type :second}] @decoded))
    (is (zero? (.position bf)))))

(deftest coalesced-handshake-and-dependent-writes-test
  (with-server
    (fn [_ observer port]
      (client/open-database observer "pipeline" "kv")
      (with-open [ch (connect port)]
        (let [receive! (receiver ch)]
          ;; A single send, with no further bytes to wake the selector.
          (send! ch [(handshake observer)
                     {:type :open-dbi :args ["pipeline" "data"]}
                     {:type :transact-kv :mode :request
                      :args ["pipeline" nil [[:put "data" :key :first]]]}
                     {:type :transact-kv :mode :request
                      :args ["pipeline" nil [[:put "data" :key :second]]]}
                     {:type :get-value
                      :args ["pipeline" "data" :key :data :data true]}])
          (is (= :set-client-id-ok (:type (receive!))))
          (dotimes [_ 3] (complete! (receive!)))
          (is (= {:type :command-complete :result :second} (receive!))))))))

(deftest slow-request-retains-connection-until-handler-returns-test
  (with-server
    (fn [_ observer port]
      (client/open-database observer "slow-pipeline" "kv")
      (doseq [writing? [false true]]
        (let [started (promise)
              release (CountDownLatch. 1)
              calls (atom [])
              handler (fn [_ skey {:keys [id]}]
                        (swap! calls conj id)
                        (when (= id 1)
                          ;; Even an already-written response must not let the
                          ;; next handler race this one's remaining work.
                          (#'server/write-message skey {:type :command-complete :result id})
                          (deliver started true)
                          (.await release 5 TimeUnit/SECONDS))
                        (when-not (= id 1)
                          (#'server/write-message skey {:type :command-complete :result id})))]
          (with-redefs-fn
            {#'server/message-handler-map (assoc @#'server/message-handler-map
                                                 ::probe handler)}
            #(with-open [ch (connect port)]
               (let [receive! (receiver ch)]
                 (try
                   (send! ch [(handshake observer)])
                   (is (= :set-client-id-ok (:type (receive!))))
                   (when writing?
                     (send! ch [{:type :open-transact-kv :args ["slow-pipeline"]}])
                     (complete! (receive!)))
                   (send! ch [{:type ::probe :id 1 :writing? writing?
                               :args ["slow-pipeline"]}])
                   (is (= 1 (:result (receive!))))
                   (is (= true (deref started 5000 ::timeout)))
                   ;; A later socket read must also remain behind the first call.
                   (send! ch [{:type ::probe :id 2} {:type ::probe :id 3}])
                   (complete! (client/request observer {:type :list-databases}))
                   (is (= [1] @calls))
                   (.countDown release)
                   (is (= [2 3] (mapv :result [(receive!) (receive!)])))
                   (is (= [1 2 3] @calls))
                   (when writing?
                     (send! ch [{:type :abort-transact-kv :args ["slow-pipeline"]}])
                     (complete! (receive!)))
                   (finally (.countDown release)))))))))))

(deftest fragmented-tail-and-large-frame-test
  (with-server
    (fn [_ observer port]
      (with-redefs-fn
        {#'server/message-handler-map
         (assoc @#'server/message-handler-map ::echo
                (fn [_ skey message]
                  (#'server/write-message skey
                   {:type :command-complete :result (:id message)})))}
        #(with-open [ch (connect port)]
           (let [receive! (receiver ch)
                 tail (frames [{:type ::echo :id 2
                                :padding (apply str (repeat (* 2 (long c/+buffer-size+)) \x))}])
                 full-limit (.limit tail)]
             (send! ch [(handshake observer) {:type ::echo :id 1}])
             (.limit tail 3)
             (p/send-all ch tail)
             (is (= :set-client-id-ok (:type (receive!))))
             (is (= 1 (:result (receive!))))
             (.limit tail full-limit)
             (p/send-all ch tail)
             (is (= 2 (:result (receive!))))))))))

(deftest pipelined-transactions-test
  (with-server
    (fn [^Server srv observer port]
      (doseq [[db-type open close abort]
              [["kv" :open-transact-kv :close-transact-kv :abort-transact-kv]
               ["datalog" :open-transact :close-transact :abort-transact]]
              end [close abort]]
        (testing (str db-type " " end)
          (let [db-name (str (name end) "-pipeline")
                write {:type (if (= db-type "kv") :transact-kv :tx-data)
                       :mode :request :writing? true
                       :args (if (= db-type "kv")
                               [db-name nil [[:put "data" :key :value]]]
                               [db-name [{:db/id 1 :value :value}] false])}]
            (client/open-database observer db-name db-type)
            (when (= db-type "kv")
              (client/normal-request observer :open-dbi [db-name "data"]))
            (with-open [ch (connect port)]
              (let [receive! (receiver ch)]
                (send! ch [(handshake observer)
                           {:type open :args [db-name]}
                           write {:type end :args [db-name]}
                           {:type :list-databases}])
                (is (= :set-client-id-ok (:type (receive!))))
                (dotimes [_ 4] (complete! (receive!)))
                (is (nil? (:runner (get (.-dbs srv) db-name))))
                (is (= 1 (.availablePermits
                           ^Semaphore (:lock (get (.-dbs srv) db-name)))))
                (is (= (when (= end close) :value)
                       (if (= db-type "kv")
                         (client/normal-request observer :get-value
                           [db-name "data" :key :data :data true])
                         (client/normal-request observer :q
                           [db-name '[:find ?v . :where [1 :value ?v]]
                            [:remote-db-placeholder]]))))))))))))

(deftest copy-transfers-retain-buffered-frames-and-response-order-test
  (with-server
    (fn [_ observer port]
      (client/open-database observer "copy-pipeline" "kv")
      (client/normal-request observer :open-dbi ["copy-pipeline" "data"])
      (with-redefs-fn
        {#'server/message-handler-map
         (assoc @#'server/message-handler-map ::copy-out
                (fn [_ skey _]
                  (#'server/copy-out skey (range 7) 2)))}
        #(with-open [ch (connect port)]
           (let [receive! (receiver ch)]
             (send! ch [(handshake observer)
                        {:type :open-transact-kv :args ["copy-pipeline"]}
                        {:type :transact-kv :mode :copy-in :writing? true
                         :args ["copy-pipeline" nil]}
                        [[:put "data" :key :copied]]
                        {:type :copy-done}
                        {:type :close-transact-kv :args ["copy-pipeline"]}
                        {:type ::copy-out}
                        {:type :get-value
                         :args ["copy-pipeline" "data" :key :data :data true]}])
             (is (= :set-client-id-ok (:type (receive!))))
             (complete! (receive!))
             (is (= :copy-in-response (:type (receive!))))
             (complete! (receive!))
             (complete! (receive!))
             (is (= :copy-out-response (:type (receive!))))
             (is (= [[0 1] [2 3] [4 5] [6]] (vec (repeatedly 4 receive!))))
             (is (= :copy-done (:type (receive!))))
             (is (= :copied (:result (receive!))))))))))

(deftest rejected-and-invalid-requests-do-not-strand-pipeline-test
  (with-server
    (fn [^Server srv observer port]
      (with-open [ch (connect port)]
        (let [receive! (receiver ch)]
          (send! ch [(handshake observer)])
          (is (= :set-client-id-ok (:type (receive!))))
          (let [^ThreadPoolExecutor executor (.-work-executor srv)
                started (CountDownLatch. 2)
                release (CountDownLatch. 1)]
            (try
              (dotimes [_ 2]
                (.execute executor ^Runnable #(do (.countDown started) (.await release))))
              (is (.await started 5 TimeUnit/SECONDS))
              (dotimes [_ 2] (.execute executor ^Runnable (fn [])))
              (send! ch [{:type :list-databases} {:type :list-databases}])
              (dotimes [_ 2]
                (is (= :server/busy (get-in (receive!) [:err-data :error]))))
              (finally (.countDown release)))
            (await-condition! #(and (zero? (.getActiveCount executor))
                                    (.isEmpty (.getQueue executor)))))
          (send! ch [[:invalid-request] {:type ::unknown} {:type :list-databases}])
          (is (= :error-response (:type (receive!))))
          (is (= :error-response (:type (receive!))))
          (complete! (receive!)))))))

(deftest disconnect-after-copy-in-releases-transaction-test
  (with-server
    (fn [^Server srv observer port]
      (client/open-database observer "copy-disconnect" "kv")
      (client/normal-request observer :open-dbi ["copy-disconnect" "data"])
      (with-open [ch (connect port)]
        (let [receive! (receiver ch)]
          (send! ch [(handshake observer)
                     {:type :open-transact-kv :args ["copy-disconnect"]}])
          (is (= :set-client-id-ok (:type (receive!))))
          (complete! (receive!))
          ;; Repeated copies cancel and replace the selection key each time.
          (dotimes [n 3]
            (send! ch [{:type :transact-kv :mode :copy-in :writing? true
                       :args ["copy-disconnect" nil]}
                      [[:put "data" :key n]] {:type :copy-done}])
            (is (= :copy-in-response (:type (receive!))))
            (complete! (receive!)))))
      (await-condition! #(nil? (:runner (get (.-dbs srv) "copy-disconnect"))))
      (is (= 1 (.availablePermits
                 ^Semaphore (:lock (get (.-dbs srv) "copy-disconnect")))))
      (is (nil? (client/normal-request observer :get-value
                  ["copy-disconnect" "data" :key :data :data true]))))))

(deftest closing-runner-closes-discarded-calls-on-other-pooled-sockets-test
  (with-server
    (fn [^Server srv observer port]
      (client/open-database observer "runner-queue" "kv")
      (client/normal-request observer :open-dbi ["runner-queue" "data"])
      (let [started (promise)
            release (CountDownLatch. 1)]
        (with-redefs-fn
          {#'server/message-handler-map
           (assoc @#'server/message-handler-map ::hold-runner
                  (fn [_ skey _]
                    (deliver started true)
                    (.await release 5 TimeUnit/SECONDS)
                    (#'server/write-message skey {:type :command-complete})))}
          #(with-open [owner (connect port)
                       closer (connect port)
                       pending (connect port)]
             (let [owner-receive! (receiver owner)
                   close-receive! (receiver closer)
                   pending-receive! (receiver pending)]
               (try
                 (doseq [[ch receive!] [[owner owner-receive!]
                                       [closer close-receive!]
                                       [pending pending-receive!]]]
                   (send! ch [(handshake observer)])
                   (is (= :set-client-id-ok (:type (receive!)))))
                 (send! owner [{:type :open-transact-kv :args ["runner-queue"]}
                               {:type ::hold-runner :writing? true
                                :args ["runner-queue"]}])
                 (complete! (owner-receive!))
                 (is (= true (deref started 5000 ::timeout)))
                 (let [^Runner runner (:runner (get (.-dbs srv) "runner-queue"))
                       ^LinkedBlockingQueue queue (.-queue runner)]
                   (send! closer [{:type :close-transact-kv :args ["runner-queue"]}])
                   (await-condition! (fn [] (= 1 (.size queue))))
                   (send! pending [{:type :get-value :writing? true
                                    :args ["runner-queue" "data" :key :data :data true]}])
                   (await-condition! (fn [] (= 2 (.size queue)))))
                 (.countDown release)
                 (complete! (owner-receive!))
                 (complete! (close-receive!))
                 (is (thrown-with-msg? Exception #"Socket channel is closed"
                                      (pending-receive!)))
                 (is (nil? (:runner (get (.-dbs srv) "runner-queue"))))
                 (finally (.countDown release))))))))))
