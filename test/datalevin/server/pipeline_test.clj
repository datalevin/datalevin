(ns datalevin.server.pipeline-test
  (:require
   [clojure.test :refer [deftest is testing use-fixtures]]
   [datalevin.client :as client]
   [datalevin.constants :as c]
   [datalevin.native-value :as nv]
   [datalevin.protocol :as p]
   [datalevin.protocol.context :as codec]
   [datalevin.server :as server]
   [datalevin.test.core :refer [allocate-port db-fixture]]
   [datalevin.util :as u])
  (:import
   [datalevin.server Server]
   [java.net InetSocketAddress]
   [java.nio ByteBuffer]
   [java.nio.channels SelectionKey SocketChannel]
   [java.util UUID]
   [java.util.concurrent CountDownLatch Semaphore TimeUnit]))

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

(deftest connection-codec-context-survives-errors-without-leaking-wire-mode-test
  (with-server
    (fn [_ observer port]
      (let [seen (atom [])
            handler (fn [_ ^SelectionKey key {:keys [value fail?]}]
                      (swap! seen conj
                             [codec/*context* (:codec-context @(.attachment key))
                              nv/*wire-native-value*])
                      (when fail? (throw (ex-info "Requested failure" {:error ::requested})))
                      (#'server/write-message key {:type :command-complete :result value}))]
        (with-redefs-fn
          {#'server/message-handler-map (assoc @#'server/message-handler-map ::echo handler)}
          #(dotimes [connection 2]
             (with-open [ch (connect port)]
               (let [receive! (receiver ch)]
                 (send! ch [(handshake observer)
                            {:type ::echo :value {:connection connection :data [:a :a]}}
                            {:type ::echo :fail? true}
                            {:type ::echo :value {:connection connection :data [:b :b]}}])
                 (is (= :set-client-id-ok (:type (receive!))))
                 (is (= {:connection connection :data [:a :a]} (:result (receive!))))
                 (is (= :error-response (:type (receive!))))
                 (is (= {:connection connection :data [:b :b]} (:result (receive!))))))))
        (is (= 6 (count @seen)))
        (doseq [[bound attached wire?] @seen]
          (is (some? bound))
          (is (identical? bound attached))
          (is (false? wire?)))
        (is (= 1 (count (set (map first (take 3 @seen))))))
        (is (= 1 (count (set (map first (drop 3 @seen))))))
        (is (not (identical? (ffirst @seen) (first (nth @seen 3)))))))))

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

(deftest invalid-requests-do-not-strand-pipeline-test
  (with-server
    (fn [_ observer port]
      (with-open [ch (connect port)]
        (let [receive! (receiver ch)]
          (send! ch [(handshake observer)])
          (is (= :set-client-id-ok (:type (receive!))))
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
          ;; Repeated copies remain on the same owning connection thread.
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

(deftest other-sockets-in-the-same-session-cannot-use-a-transaction-test
  (with-server
    (fn [^Server srv observer port]
      (client/open-database observer "pinned" "kv")
      (client/normal-request observer :open-dbi ["pinned" "data"])
      (with-open [owner (connect port) other (connect port)]
        (let [receive-owner! (receiver owner) receive-other! (receiver other)]
          (doseq [[ch receive!] [[owner receive-owner!] [other receive-other!]]]
            (send! ch [(handshake observer)])
            (is (= :set-client-id-ok (:type (receive!)))))
          (send! owner [{:type :open-transact-kv :args ["pinned"]}])
          (complete! (receive-owner!))
          (doseq [message [{:type :close-transact-kv :args ["pinned"]}
                           {:type :abort-transact-kv :args ["pinned"]}
                           {:type :get-value :writing? true
                            :args ["pinned" "data" :key :data :data true]}]]
            (send! other [message])
            (is (= :transaction-owner-mismatch
                   (get-in (receive-other!) [:err-data :reason]))))
          (is (some? (:runner (get (.-dbs srv) "pinned"))))
          (send! owner [{:type :transact-kv :mode :request :writing? true
                         :args ["pinned" nil [[:put "data" :key :committed]]]}
                        {:type :close-transact-kv :args ["pinned"]}])
          (complete! (receive-owner!))
          (complete! (receive-owner!))
          (is (= :committed (client/normal-request observer :get-value
                              ["pinned" "data" :key :data :data true]))))))))
