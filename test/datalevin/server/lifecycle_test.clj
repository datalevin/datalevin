(ns datalevin.server.lifecycle-test
  (:require
   [clojure.test :refer [deftest is use-fixtures]]
   [datalevin.client :as client]
   [datalevin.core :as d]
   [datalevin.ha.control :as control]
   [datalevin.interface :as i]
   [datalevin.server :as server]
   [datalevin.server.ha :as ha]
   [datalevin.server.session :as session]
   [datalevin.test.core :refer [allocate-port db-fixture]]
   [datalevin.util :as u])
  (:import
   [datalevin.server Server]
   [java.io IOException]
   [java.nio.channels ClosedSelectorException Selector SelectionKey
    ServerSocketChannel SocketChannel]
   [java.util LinkedHashSet UUID]
   [java.util.concurrent ArrayBlockingQueue ConcurrentHashMap ConcurrentLinkedQueue
    CountDownLatch ExecutorService Executors Future FutureTask LinkedBlockingQueue
    RejectedExecutionException Semaphore ThreadPoolExecutor
    ThreadPoolExecutor$CallerRunsPolicy TimeUnit]
   [java.util.concurrent.atomic AtomicBoolean]))

(use-fixtures :once db-fixture)

(defn- await! [task]
  (let [result (deref task 5000 ::timeout)]
    (when (= ::timeout result)
      (throw (ex-info "Server lifecycle test timed out" {})))
    result))

(defn- fake-server [{:keys [on-execute on-select on-close work-executor
                           selector selected-keys]}]
  (let [^Selector delegate (or selector (Selector/open))
        submitted (atom 0)
        closed    (atom 0)
        selector  (or selector (proxy [Selector] []
                    (isOpen [] (.isOpen delegate))
                    (provider [] (.provider delegate))
                    (keys [] (.keys delegate))
                    (selectedKeys [] (or selected-keys (.selectedKeys delegate)))
                    (selectNow [] (.selectNow delegate))
                    (select
                      ([] (when on-select (on-select)) (.select delegate))
                      ([timeout] (when on-select (on-select))
                                 (.select delegate (long timeout))))
                    (wakeup [] (.wakeup delegate))
                    (close []
                      (.close delegate)
                      (swap! closed inc)
                      (when on-close (on-close)))))
        dispatcher
        (proxy [ThreadPoolExecutor]
            [1 1 0 TimeUnit/MILLISECONDS (LinkedBlockingQueue.)]
          (execute [task]
            (swap! submitted inc)
            (when on-execute (on-execute))
            (let [^ThreadPoolExecutor this this]
              (proxy-super execute task))))
        srv (server/->Server
              (AtomicBoolean. false) 0 "" 0
              (ServerSocketChannel/open) selector (ConcurrentLinkedQueue.)
              dispatcher (or work-executor (Executors/newSingleThreadExecutor)) nil
              (ConcurrentHashMap.) (ConcurrentHashMap.))]
    {:server srv :submitted submitted :closed closed}))

(defn- assert-stopped [^Server srv]
  (is (false? (.get ^AtomicBoolean (.-running srv))))
  (is (false? (.isOpen ^Selector (.-selector srv))))
  (is (false? (.isOpen ^ServerSocketChannel (.-server-socket srv))))
  (is (.isTerminated ^ExecutorService (.-dispatcher srv)))
  (is (.isTerminated ^ExecutorService (.-work-executor srv)))
  (doseq [executor (vals (select-keys (.-execution srv)
                                     [:routing :transactions :background]))]
    (is (.isTerminated ^ExecutorService executor)))
  (is (d/closed? (.-sys-conn srv))))

(defn- start-error [srv]
  (try (server/start srv) nil (catch Exception e (:error (ex-data e)))))

(defn- await-condition! [pred]
  (let [deadline (+ (System/currentTimeMillis) 5000)]
    (loop []
      (when-not (pred)
        (when (>= (System/currentTimeMillis) deadline)
          (throw (ex-info "Server lifecycle condition timed out" {})))
        (Thread/sleep 5)
        (recur)))))

(defn- fake-key [channel selector ops]
  (let [valid (atom true)]
    (doto (proxy [SelectionKey] []
            (channel [] channel)
            (selector [] selector)
            (isValid [] @valid)
            (cancel [] (reset! valid false))
            (interestOps ([] ops) ([new-ops] new-ops))
            (readyOps [] ops))
      (.attach (volatile! {})))))

(deftest stop-before-start-releases-created-resources-test
  (let [root (u/tmp-dir (str "unstarted-server-" (UUID/randomUUID)))
        opts {:port (allocate-port) :root root}
        srv  (server/create opts)]
    (try
      (is (nil? (server/stop srv)))
      (assert-stopped srv)
      (is (nil? (server/stop srv)))
      (is (= :server/stopped (start-error srv)))
      ;; Rebinding the port and reopening the system DB proves both were freed.
      (let [replacement (server/create opts)]
        (server/stop replacement)
        (assert-stopped replacement))
      (finally
        (server/stop srv)
        (u/delete-files root)))))

(deftest stop-flushes-request-activity-before-restarting-test
  (let [root (u/tmp-dir (str "server-session-restart-" (UUID/randomUUID)))
        port (allocate-port)
        opts {:root root :port port :idle-timeout 1000}
        uri (str "dtlv://datalevin:datalevin@localhost:" port)
        clock (atom 1)
        ^Server srv (server/create opts)]
    (try
      (with-redefs-fn
        {#'server/deps-now-ms #(deref clock)}
        (fn []
          (server/start srv)
          (let [c (client/new-client uri {:pool-size 1})]
            (try
              (let [id (first (keys (.-clients srv)))
                    lmdb (session/session-lmdb (.-sys-conn srv))]
                (doseq [now-ms [200 1001 1200]]
                  (reset! clock now-ms)
                  (is (= [] (client/list-databases c))))
                (is (= 1001 (:last-active (d/get-value lmdb session/session-dbi
                                                       id :uuid :data))))
                (is (= 1200 (:last-active (get (.-clients srv) id))))
                (server/stop srv)
                (reset! clock 2002)
                (let [^Server replacement (server/create opts)]
                  (try
                    (server/start replacement)
                    ;; A real request ensures the first idle sweep has run.
                    (let [probe (client/new-client uri {:pool-size 1})]
                      (try
                        (is (= [] (client/list-databases probe)))
                        (is (= 1200 (:last-active (get (.-clients replacement) id))))
                        (is (= 1200 (:last-active-checkpoint-until
                                     (get (.-clients replacement) id))))
                        (finally (client/close-pool (client/get-pool probe)))))
                    (finally (server/stop replacement)))))
              (finally (client/close-pool (client/get-pool c)))))))
      (finally
        (server/stop srv)
        (u/delete-files root)))))

(deftest restart-uses-a-new-instance-and-retains-data-test
  (let [root (u/tmp-dir (str "server-lifecycle-" (UUID/randomUUID)))
        port (allocate-port)
        opts {:port port :root root}
        uri  (str "dtlv://datalevin:datalevin@localhost:" port "/data")
        srv  (server/create opts)]
    (try
      (server/start srv)
      (is (nil? (server/start srv)))
      (let [kv (d/open-kv uri)]
        (try
          (d/open-dbi kv "data")
          (d/transact-kv kv [[:put "data" :key :saved]])
          (finally (d/close-kv kv))))
      (let [c (client/new-client uri {:pool-size 1 :time-out 5000})]
        (try
          (client/open-database c "data" "kv")
          (client/normal-request c :open-transact-kv ["data"])
          (is (= :command-complete
                 (:type (client/request
                          c {:type :transact-kv :mode :request :writing? true
                             :args ["data" nil [[:put "data" :key :discarded]]]}))))
          ;; Shutdown must drain the owner runner and abort its pending write.
          (is (nil? (server/stop srv)))
          (is (nil? (server/stop srv)))
          (assert-stopped srv)
          (finally (client/close-pool (client/get-pool c)))))
      (is (= :server/stopped (start-error srv)))
      (let [replacement (server/create opts)]
        (try
          (server/start replacement)
          (let [kv (d/open-kv uri)]
            (try
              (d/open-dbi kv "data")
              (is (= :saved (d/get-value kv "data" :key)))
              (finally (d/close-kv kv))))
          (finally (server/stop replacement))))
      (finally
        (server/stop srv)
        (u/delete-files root)))))

(deftest concurrent-starts-submit-one-event-loop-test
  (let [entered (promise)
        release (promise)
        other-started (promise)
        {:keys [server submitted]}
        (fake-server {:on-execute #(do (deliver entered true) (await! release))})
        first-start (future (server/start server))
        other (atom nil)]
    (try
      (await! entered)
      (reset! other (future
                      (deliver other-started true)
                      (server/start server)))
      (await! other-started)
      (is (= ::pending (deref @other 100 ::pending)))
      (deliver release true)
      (await! first-start)
      (is (nil? (await! @other)))
      (is (= 1 @submitted))
      (finally
        (deliver release true)
        (await! first-start)
        (when @other (await! @other))
        (server/stop server)))))

(deftest stop-waits-for-in-flight-start-test
  (let [entered (promise)
        release (promise)
        stopping (promise)
        {:keys [server submitted]}
        (fake-server {:on-execute #(do (deliver entered true) (await! release))})
        starting (future (server/start server))
        stopped (atom nil)]
    (try
      (await! entered)
      (reset! stopped (future (deliver stopping true) (server/stop server)))
      (await! stopping)
      (is (= ::pending (deref @stopped 100 ::pending)))
      (deliver release true)
      (await! starting)
      (is (nil? (await! @stopped)))
      (is (= 1 @submitted))
      (assert-stopped server)
      (is (= :server/stopped (start-error server)))
      (finally
        (deliver release true)
        (await! starting)
        (when @stopped (await! @stopped))
        (server/stop server)))))

(deftest concurrent-stop-and-start-wait-for-cleanup-test
  (let [entered (promise)
        release (promise)
        stop-called (promise)
        start-called (promise)
        {:keys [server submitted closed]}
        (fake-server {:on-close #(do (deliver entered true) (await! release))})
        first-stop (future (server/stop server))
        others (atom [])]
    (try
      (await! entered)
      (let [second-stop (future (deliver stop-called true) (server/stop server))
            starting (future (deliver start-called true) (start-error server))]
        (reset! others [second-stop starting])
        (await! stop-called)
        (await! start-called)
        (is (= ::pending (deref second-stop 100 ::pending)))
        (is (= ::pending (deref starting 100 ::pending)))
        (deliver release true)
        (is (nil? (await! first-stop)))
        (is (nil? (await! second-stop)))
        (is (= :server/stopped (await! starting))))
      (is (= 1 @closed))
      (is (zero? @submitted))
      (assert-stopped server)
      (finally
        (deliver release true)
        (await! first-stop)
        (doseq [task @others] (await! task))
        (server/stop server)))))

(deftest failed-start-submission-releases-resources-test
  (let [{:keys [server submitted]}
        (fake-server
          {:on-execute #(throw (RejectedExecutionException. "Rejected"))})]
    (try
      (is (thrown? RejectedExecutionException (server/start server)))
      (assert-stopped server)
      (is (= :server/stopped (start-error server)))
      (is (= 1 @submitted))
      (is (nil? (server/stop server)))
      (finally (server/stop server)))))

(deftest failed-dispatcher-submission-stops-already-started-ha-loops-test
  (let [renewed (promise)
        followed (promise)
        stopped (atom 0)
        state-v (atom nil)
        authority (reify control/ILeaseAuthority
                    (stop-authority! [_] (swap! stopped inc)))
        {:keys [server]}
        (fake-server
          {:work-executor (Executors/newFixedThreadPool 2)
           :on-execute #(do (await! renewed)
                            (await! followed)
                            (throw (RejectedExecutionException. "Rejected")))})]
    (.put ^ConcurrentHashMap (.-dbs ^Server server) "data" {:ha-authority authority})
    (with-redefs [server/*ha-renew-step-fn*
                  (fn [_ state] (swap! state-v merge state) (deliver renewed true) state)
                  server/*ha-follower-sync-step-fn*
                  (fn [_ state] (swap! state-v merge state) (deliver followed true) state)]
      (try
        (is (thrown? RejectedExecutionException (server/start server)))
        (assert-stopped server)
        (is (= 1 @stopped))
        (is (= :server/stopped (start-error server)))
        ;; Both tasks really started; their finally blocks must have run.
        (doseq [k [:ha-renew-loop-stopped-latch :ha-follower-loop-stopped-latch]]
          (is (zero? (.getCount ^CountDownLatch (get @state-v k)))))
        (doseq [k [:ha-renew-loop-future :ha-follower-loop-future]]
          (is (.isDone ^Future (get @state-v k))))
        (finally (server/stop server))))))

(deftest saturated-worker-pool-cannot-run-ha-loop-on-starting-thread-test
  (let [executor (ThreadPoolExecutor.
                   1 1 0 TimeUnit/MILLISECONDS (ArrayBlockingQueue. 1)
                   (ThreadPoolExecutor$CallerRunsPolicy.))
        stopped (atom 0)
        authority (reify control/ILeaseAuthority
                    (stop-authority! [_] (swap! stopped inc)))
        {:keys [server submitted]} (fake-server {:work-executor executor})]
    (doseq [db-name ["first" "second"]]
      (.put ^ConcurrentHashMap (.-dbs ^Server server) db-name
            {:ha-authority authority}))
    (with-redefs [server/*ha-renew-step-fn* (fn [_ state] state)
                  server/*ha-follower-sync-step-fn* (fn [_ state] state)]
      (let [starting (future (try (server/start server) (catch Exception e e)))]
        (try
          (is (instance? RejectedExecutionException (await! starting)))
          (assert-stopped server)
          (is (= 2 @stopped))
          (is (zero? @submitted))
          (finally
            ;; Also release a caller-run loop if this regression returns.
            (.set ^AtomicBoolean (.-running ^Server server) false)
            (await! starting)
            (server/stop server)))))))

(deftest ha-task-can-run-later-on-the-submitting-worker-test
  (let [executor (ThreadPoolExecutor.
                   1 1 0 TimeUnit/MILLISECONDS (ArrayBlockingQueue. 1)
                   (ThreadPoolExecutor$CallerRunsPolicy.))
        ran (promise)
        task (FutureTask. ^Runnable #(deliver ran true) nil)]
    (try
      (.get (.submit executor
                     ^Runnable #(#'ha/submit-ha-loop! executor task))
            5 TimeUnit/SECONDS)
      (is (true? (await! ran)))
      (.get task 5 TimeUnit/SECONDS)
      (.shutdown executor)
      (is (thrown? RejectedExecutionException
                   (#'ha/submit-ha-loop! executor
                    (FutureTask. ^Runnable (fn []) nil))))
      (finally
        (.shutdownNow executor)
        (.awaitTermination executor 5 TimeUnit/SECONDS)))))

(deftest event-loop-failure-retries-without-another-submission-test
  (let [selects (atom 0)
        retried (promise)
        {:keys [server submitted]}
        (fake-server
          {:on-select #(if (= 1 (swap! selects inc))
                         (throw (IOException. "Select failed"))
                         (deliver retried true))})]
    (try
      (server/start server)
      (await! retried)
      (is (= 1 @submitted))
      (server/stop server)
      (assert-stopped server)
      (finally (server/stop server)))))

(deftest persistent-select-failures-have-a-real-delay-test
  (let [attempts (atom [])
        retried (promise)
        {:keys [server submitted]}
        (fake-server
          {:on-select #(do
                         (when (= 3 (count (swap! attempts conj (System/nanoTime))))
                           (deliver retried true))
                         (throw (IOException. "Persistent select failure")))})]
    (try
      (server/start server)
      (await! retried)
      (is (>= (- (long (nth @attempts 2)) (long (first @attempts))) 250000000)
          "repeated failures must wait instead of spinning")
      (server/stop server)
      (is (= 1 @submitted))
      (assert-stopped server)
      (finally (server/stop server)))))

(deftest network-backoff-is-capped-and-resets-after-a-successful-cycle-test
  (let [selects (atom 0)
        delays (atom [])
        complete (promise)
        srv-v (volatile! nil)
        {:keys [server submitted]}
        (fake-server
          {:on-select #(if (= 10 (swap! selects inc))
                         (.wakeup ^Selector (.-selector ^Server @srv-v))
                         (throw (IOException. "Select failed")))})
        wait! @#'server/wait-event-loop-retry!]
    (vreset! srv-v server)
    (with-redefs-fn
      {#'server/wait-event-loop-retry!
       (fn [srv delay-ms]
         (when (= 10 (count (swap! delays conj delay-ms)))
           (deliver complete true)
           (wait! srv 5000)))}
      #(try
         (server/start server)
         (await! complete)
         (is (= [100 200 400 800 1600 3200 5000 5000 5000 100] @delays))
         (server/stop server)
         (is (= 1 @submitted))
         (assert-stopped server)
         (finally (server/stop server))))))

(deftest stop-wakes-maximum-event-loop-backoff-test
  (let [waiting (promise)
        wait! @#'server/wait-event-loop-retry!
        {:keys [server]} (fake-server
                          {:on-select #(throw (IOException. "Persistent failure"))})]
    (with-redefs-fn
      {#'server/event-loop-backoff-ms (fn ^long [_] 5000)
       #'server/wait-event-loop-retry!
       (fn [srv delay-ms] (deliver waiting true) (wait! srv delay-ms))}
      #(try
         (server/start server)
         (await! waiting)
         (let [stopping (future (server/stop server))]
           (is (nil? (deref stopping 1000 ::timeout)))
           (await! stopping))
         (assert-stopped server)
         (finally (server/stop server))))))

(deftest failed-selected-key-does-not-skip-other-clients-test
  (doseq [accept-failure? [false true]]
    (let [selected (LinkedHashSet.)
          {:keys [server]} (fake-server {:selected-keys selected})
          selector (.-selector ^Server server)
          bad (if accept-failure? (.-server-socket ^Server server)
                                 (SocketChannel/open))
          good (SocketChannel/open)
          bad-key (fake-key bad selector (if accept-failure? SelectionKey/OP_ACCEPT
                                                            SelectionKey/OP_READ))
          good-key (fake-key good selector SelectionKey/OP_READ)
          served (atom [])
          failure (IOException. "Key handler failed")]
      (.add selected bad-key)
      (.add selected good-key)
      (try
        (with-redefs-fn
          {#'server/handle-accept (fn [_ _] (throw failure))
           #'server/handle-read (fn [_ key]
                                 (if (identical? key bad-key)
                                   (throw failure)
                                   (swap! served conj key)))}
          #(let [error (try (#'server/handle-selected-keys server)
                            (catch Exception e e))]
             (is (if accept-failure? (identical? failure error) (nil? error)))))
        (is (= [good-key] @served))
        (is (.isEmpty selected))
        (is (.isOpen good))
        (is (= accept-failure? (.isOpen ^java.nio.channels.Channel bad)))
        (finally
          (.close ^java.nio.channels.Channel bad)
          (.close good)
          (server/stop server))))))

(deftest idle-sweep-failure-does-not-stop-network-processing-test
  (let [root (u/tmp-dir (str "server-sweep-failure-" (UUID/randomUUID)))
        port (allocate-port)
        srv (server/create {:root root :port port})
        failing? (atom true)
        failures (atom 0)
        recovered (promise)
        c-v (volatile! nil)]
    (with-redefs-fn
      {#'server/remove-idle-sessions
       (fn [_]
         (if @failing?
           (do (swap! failures inc)
               (throw (IOException. "Session store unavailable")))
           (deliver recovered true)))}
      #(try
         (server/start srv)
         (vreset! c-v (client/new-client
                       (str "dtlv://datalevin:datalevin@localhost:" port)
                       {:pool-size 1 :time-out 3000}))
         (dotimes [_ 10]
           (is (= :command-complete
                  (:type (client/request @c-v {:type :list-databases :args []})))))
         (is (pos? @failures))
         (reset! failing? false)
         (await! recovered)
         (finally
           (when @c-v (client/disconnect @c-v))
           (server/stop srv)
           (u/delete-files root))))))

(deftest terminal-selector-failure-releases-sockets-and-active-transaction-test
  (let [root (u/tmp-dir (str "server-selector-failure-" (UUID/randomUUID)))
        opts {:root root :port (allocate-port)}
        ^Server srv (server/create opts)
        c-v (volatile! nil)]
    (try
      (server/start srv)
      (vreset! c-v (client/new-client
                    (str "dtlv://datalevin:datalevin@localhost:" (:port opts))
                    {:pool-size 1 :time-out 3000}))
      (client/open-database @c-v "data" "kv")
      (client/normal-request @c-v :open-transact-kv ["data"])
      (let [{:keys [store lock]} (get (.-dbs srv) "data")
            channels (vec (:connections (.-execution srv)))]
        (is (seq channels))
        (.close ^Selector (.-selector srv))
        (await-condition! #(d/closed? (.-sys-conn srv)))
        ;; Join cleanup through the public lifecycle call, including its race
        ;; with the dispatcher handing terminal cleanup to another thread.
        (is (nil? (await! (future (server/stop srv)))))
        (assert-stopped srv)
        (is (every? #(not (.isOpen ^SocketChannel %)) channels))
        (is (i/closed-kv? store))
        (is (= 1 (.availablePermits ^Semaphore lock)))
        (is (= :server/stopped (start-error srv))))
      (let [replacement (server/create opts)]
        (server/stop replacement)
        (assert-stopped replacement))
      (finally
        (when @c-v (client/close-pool (client/get-pool @c-v)))
        (server/stop srv)
        (u/delete-files root)))))

(deftest terminal-event-loop-error-can-race-explicit-stop-test
  (let [entered (promise)
        release (promise)
        closing (promise)
        {:keys [server]}
        (fake-server {:on-select #(do (deliver entered true)
                                     (await! release)
                                     (throw (ClosedSelectorException.)))
                      :on-close #(deliver closing true)})
        stopping (atom nil)]
    (try
      (server/start server)
      (await! entered)
      (reset! stopping (future (server/stop server)))
      (await! closing)
      (deliver release true)
      (is (nil? (await! @stopping)))
      (assert-stopped server)
      (finally
        (deliver release true)
        (when @stopping (await! @stopping))
        (server/stop server)))))

(deftest interrupted-event-loop-stops-instead-of-spinning-on-select-test
  (let [selects (atom 0)
        {:keys [server]}
        (fake-server {:on-select #(do (swap! selects inc)
                                     (.interrupt (Thread/currentThread)))})]
    (try
      (server/start server)
      (await-condition! #(.isTerminated ^ExecutorService
                                       (.-work-executor ^Server server)))
      (server/stop server)
      (is (= 1 @selects))
      (assert-stopped server)
      (is (= :server/stopped (start-error server)))
      (finally (server/stop server)))))

(deftest cleanup-failure-does-not-skip-other-resources-test
  (let [failure (IOException. "Selector close failed")
        {:keys [server closed]} (fake-server {:on-close #(throw failure)})]
    (try
      (is (identical? failure
                      (try (server/stop server) (catch Exception e e))))
      (assert-stopped server)
      (is (nil? (server/stop server)))
      (is (= 1 @closed))
      (is (= :server/stopped (start-error server)))
      (finally (server/stop server)))))
