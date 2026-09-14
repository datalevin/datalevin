(ns datalevin.server.initialization-test
  (:require
   [clojure.java.io :as io]
   [clojure.test :refer [deftest is use-fixtures]]
   [datalevin.core :as d]
   [datalevin.db :as db]
   [datalevin.ha.control :as control]
   [datalevin.interface :as i]
   [datalevin.lmdb :as l]
   [datalevin.server :as server]
   [datalevin.server.resources :as resources]
   [datalevin.server.session :as session]
   [datalevin.test.core :refer [allocate-port db-fixture]]
   [datalevin.util :as u])
  (:import
   [datalevin.server Server]
   [java.io IOException]
   [java.net InetSocketAddress]
   [java.nio.channels ServerSocketChannel]
   [java.util UUID]
   [java.util.concurrent CountDownLatch ExecutorService Future ThreadPoolExecutor TimeUnit]
   [java.util.concurrent.atomic AtomicBoolean]))

(use-fixtures :once db-fixture)

(defn- await! [task]
  (let [result (deref task 5000 ::timeout)]
    (when (= ::timeout result)
      (throw (ex-info "Server initialization test timed out" {})))
    result))

(defn- persist-stores! [opts stores]
  (let [^Server srv (server/create opts)]
    (try
      (d/transact-kv (session/session-lmdb (.-sys-conn srv))
                     [[:put server/session-dbi (UUID/randomUUID)
                       {:stores stores :engines #{} :indices #{} :dt-dbs #{}}
                       :uuid :data]])
      (finally (server/stop srv)))))

(defn- assert-port-free! [port]
  (with-open [socket (ServerSocketChannel/open)]
    (.bind socket (InetSocketAddress. "localhost" (int port)))
    (is (.isOpen socket))))

(deftest acquisition-failure-preserves-cause-and-attempts-every-cleanup-test
  (let [failure (IOException. "initialization failed")
        cleanup-failure (IOException. "cleanup failed")
        closed (atom [])]
    (is (identical?
          failure
          (try
            (resources/with-acquired
              (fn [own!]
                (own! :first #(swap! closed conj %))
                (own! :second #(do (swap! closed conj %) (throw cleanup-failure)))
                (own! :third #(swap! closed conj %))
                (throw failure)))
            (catch Throwable t t))))
    (is (= [:third :second :first] @closed))
    (is (= [cleanup-failure] (vec (.getSuppressed failure)))))
  (let [closed (atom [])
        failure (IOException. "first close failed")]
    (is (identical?
          failure
          (try
            (resources/close-all! [#(do (swap! closed conj :first) (throw failure))
                                   #(swap! closed conj :second)])
            (catch Throwable t t))))
    (is (= [:first :second] @closed))))

(deftest invalid-execution-options-do-not-acquire-server-resources-test
  (doseq [invalid [{:worker-threads 0} {:worker-queue-size 0}
                  {:transaction-threads 0} {:background-threads 0}
                  {:transaction-lock-timeout-ms -1}]]
    (let [root (u/tmp-dir (str "invalid-server-workers-" (UUID/randomUUID)))
          port (allocate-port)
          opened @l/lmdb-dirs]
      (try
        (let [failure (try (server/create (merge {:root root :port port} invalid))
                           (catch Exception e e))]
          (is (= invalid (ex-data failure)))
          (is (not (.exists (io/file root))))
          (is (= opened @l/lmdb-dirs))
          (assert-port-free! port))
        (finally
          (when (.exists (io/file root)) (u/delete-files root)))))))

(deftest failures-inside-open-helpers-release-their-native-store-test
  (doseq [open! [#(#'server/init-sys-db % nil)
                #(#'server/open-store % "data" [42] false)]]
    (let [root (u/tmp-dir (str "failed-server-open-" (UUID/randomUUID)))
          opened @l/lmdb-dirs]
      (try
        (is (thrown? Exception (open! root)))
        (is (= opened @l/lmdb-dirs))
        (finally (u/delete-files root))))))

(deftest failed-reopen-closes-published-and-unpublished-stores-test
  (doseq [datalog? [false true]]
    (let [root (u/tmp-dir (str "failed-server-reopen-" (UUID/randomUUID)))
          opts {:root root :port (allocate-port)}
          stores (atom [])
          failure (IOException. "runtime restore failed")
          close-failure (IOException. "index close failed")
          index (reify i/IVectorIndex (close-vecs [_] (throw close-failure)))
          closed? (if datalog? i/closed? i/closed-kv?)
          original-deps @#'server/session-deps]
      (try
        (persist-stores! opts (array-map "first" {:datalog? datalog? :dbis #{"data"}}
                                        "second" {:datalog? datalog? :dbis #{"data"}}))
        (let [opened @l/lmdb-dirs
              registered (set (keys @db/dbs))
              result
              (with-redefs-fn
                {#'server/session-deps
                 (assoc original-deps
                        :ensure-ha-runtime-fn
                        (fn [_ _ state _]
                          (when (= 2 (count @stores)) (throw failure))
                          (assoc state :index index))
                        :open-store-fn
                        (fn [& args]
                          (let [store (apply (:open-store-fn original-deps) args)]
                            (swap! stores conj store)
                            store)))}
                #(try (server/create opts) (catch Exception e e)))]
          (is (identical? failure (ex-cause result)))
          (is (= [close-failure] (vec (.getSuppressed failure))))
          (is (= 2 (count @stores)))
          (is (every? closed? @stores))
          (is (= opened @l/lmdb-dirs) "the system DB is closed too")
          (is (= registered (set (keys @db/dbs))) "Datalog DBs are unregistered too")
          (assert-port-free! (:port opts)))
        (let [replacement (server/create opts)]
          (server/stop replacement))
        (finally
          (doseq [store @stores]
            (when-not (closed? store) ((:close-store-fn original-deps) store)))
          (u/delete-files root))))))

(deftest restored-ha-loops-start-only-with-the-server-test
  (let [root (u/tmp-dir (str "deferred-server-ha-" (UUID/randomUUID)))
        opts {:root root :port (allocate-port) :worker-threads 2}
        renewed (promise)
        followed (promise)
        stopped (atom 0)
        authority (reify control/ILeaseAuthority
                    (stop-authority! [_] (swap! stopped inc)))
        srv-v (atom nil)]
    (try
      (persist-stores! opts {"data" {:datalog? false :dbis #{}}})
      (with-redefs-fn
        {#'server/session-deps
         (assoc @#'server/session-deps :ensure-ha-runtime-fn
                (fn [_ _ state _] (assoc state :ha-authority authority)))
         #'server/*ha-renew-step-fn* (fn [_ state] (deliver renewed true) state)
         #'server/*ha-follower-sync-step-fn* (fn [_ state] (deliver followed true) state)}
        (fn []
          (let [^Server srv (server/create opts)
                ^ThreadPoolExecutor executor (:background (.-execution srv))]
            (reset! srv-v srv)
            (try
              (is (zero? (.getTaskCount executor)))
              (is (nil? (:ha-renew-loop-future (get (.-dbs srv) "data"))))
              ;; Give a worker a chance to run while the server is still idle.
              (.get (.submit ^ExecutorService executor ^Runnable (fn []))
                    5 TimeUnit/SECONDS)
              (is (not (realized? renewed)))
              (is (not (realized? followed)))
              (server/start srv)
              (await! renewed)
              (await! followed)
              (is (= 2 (.getActiveCount executor)))
              (is (empty? (:connection-threads (.-execution srv))))
              (let [state (get (.-dbs srv) "data")
                    loop-keys [:ha-renew-loop-future :ha-follower-loop-future]]
                (is (nil? (server/start srv)))
                (doseq [k loop-keys]
                  (is (identical? (get state k) (get-in (.-dbs srv) ["data" k]))))
                (server/stop srv)
                (doseq [k loop-keys]
                  (is (.isDone ^Future (get state k))))
                (doseq [k [:ha-renew-loop-running? :ha-follower-loop-running?]]
                  (is (false? (.get ^AtomicBoolean (get state k)))))
                (doseq [k [:ha-renew-loop-stopped-latch :ha-follower-loop-stopped-latch]]
                  (is (zero? (.getCount ^CountDownLatch (get state k))))))
              (is (= 1 @stopped))
              (is (.isTerminated executor))
              (finally (server/stop srv))))))
      (finally
        (when @srv-v (server/stop @srv-v))
        (u/delete-files root)))))
