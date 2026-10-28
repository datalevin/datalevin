(ns datalevin.server.shutdown-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [datalevin.core :as d]
            [datalevin.server :as server]
            [datalevin.test.core :refer [db-fixture]]
            [datalevin.util :as u])
  (:import [datalevin.server Server]
           [java.nio.channels Selector ServerSocketChannel]
           [java.util.concurrent ConcurrentHashMap ConcurrentLinkedQueue
            CountDownLatch ExecutorService Executors TimeUnit]
           [java.util.concurrent.atomic AtomicBoolean]))

(use-fixtures :once db-fixture)

(defn- await-condition! [pred]
  (let [deadline (+ (System/nanoTime) 5000000000)]
    (loop []
      (when-not (pred)
        (when (>= (System/nanoTime) deadline)
          (throw (ex-info "Shutdown test timed out" {})))
        (Thread/sleep 5)
        (recur)))))

(defn- await-release! [^CountDownLatch release]
  ;; Model synchronous work that must retain its store until it returns.
  (loop []
    (when-not (try (.await release 5 TimeUnit/SECONDS)
                  (catch InterruptedException _ false))
      (recur))))

(defn- check-interrupted-stop! [phase initially-interrupted?]
  (let [root (u/tmp-dir (str "server-shutdown-" (random-uuid)))
        conn (d/create-conn root)
        ^ExecutorService dispatcher (Executors/newSingleThreadExecutor)
        ^Server srv (server/->Server
                      (AtomicBoolean. false) 0 root 60000
                      (ServerSocketChannel/open) (Selector/open)
                      (ConcurrentLinkedQueue.) dispatcher
                      (Executors/newSingleThreadExecutor) conn
                      (ConcurrentHashMap.) (ConcurrentHashMap.))
        release (CountDownLatch. 1)
        entered (promise)
        stopped (promise)
        owner (Thread. ^Runnable #(do (deliver entered true)
                                      (await-release! release)))
        stopper (Thread.
                  ^Runnable
                  #(do
                     (when initially-interrupted?
                       (.interrupt (Thread/currentThread)))
                     (let [failure (try (server/stop srv) nil
                                        (catch Throwable t t))]
                       (deliver stopped
                                {:failure failure
                                 :interrupted? (.isInterrupted (Thread/currentThread))}))))]
    (try
      (case phase
        :connection
        (do
          (.put ^ConcurrentHashMap (:connection-threads (.-execution srv))
                ::owner owner)
          (.start owner))
        :dispatcher
        (.submit dispatcher ^Runnable #(do (deliver entered true)
                                            (await-release! release))))
      (is (= true (deref entered 5000 ::timeout)))
      (.start stopper)
      (when-not initially-interrupted?
        ;; Interrupt the specific wait, after shutdown has begun.
        (await-condition!
          #(some (fn [^StackTraceElement frame]
                   (= (case phase :connection "join" :dispatcher "awaitTermination")
                      (.getMethodName frame)))
                 (.getStackTrace stopper)))
        (.interrupt stopper))
      (is (= ::pending (deref stopped 200 ::pending))
          "stop must finish waiting before it closes stores")
      (is (not (d/closed? conn)) "the store remains open for its owner")
      (.countDown release)
      (let [result (deref stopped 5000 ::timeout)]
        (is (map? result))
        (is (nil? (:failure result)))
        (is (true? (:interrupted? result)) "stop preserves the caller's interrupt"))
      (is (d/closed? conn))
      (is (.isTerminated dispatcher))
      (finally
        (.countDown release)
        (.join owner 5000)
        (.join stopper 5000)
        (server/stop srv)
        (d/close conn)
        (u/delete-files root)))))

(deftest interruption-cannot-skip-a-connection-owner-during-shutdown
  (check-interrupted-stop! :connection false))

(deftest interruption-cannot-skip-dispatcher-termination-during-shutdown
  (check-interrupted-stop! :dispatcher false))

(deftest an-interrupted-caller-still-completes-shutdown
  (doseq [phase [:connection :dispatcher]]
    (testing (name phase)
      (check-interrupted-stop! phase true))))
