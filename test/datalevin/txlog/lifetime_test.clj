(ns datalevin.txlog.lifetime-test
  (:require [clojure.test :refer [deftest is]]
            [datalevin.tx-state.lifetime :as lifetime])
  (:import [java.lang AutoCloseable]
           [java.util.concurrent CountDownLatch CyclicBarrier]
           [java.util.concurrent.atomic AtomicBoolean]))

(deftest nested-borrows-and-cross-thread-returns-retain-quiescence
  (let [guard (lifetime/create)
        a (lifetime/borrow! guard)
        b (lifetime/borrow! guard)]
    (is (identical? a b) "Repeated borrows reuse the calling thread's slot")
    (is (= 2 (:native-users (lifetime/state guard))))
    @(future (.close ^AutoCloseable a))
    (is (= 1 (:native-users (lifetime/state guard))))
    (lifetime/fence! guard)
    @(future (.close ^AutoCloseable b))
    (lifetime/close! guard 1000 (fn []))
    (is (= {:phase :closed :native-users 0} (lifetime/state guard)))))

(deftest entry-racing-close-never-uses-a-torn-down-environment
  (dotimes [_ 1000]
    (let [guard (lifetime/create)
          barrier (CyclicBarrier. 2)
          closed (AtomicBoolean.)
          owner (future
                  ;; Register first so the race tests the lock-free fast path.
                  (.close (lifetime/borrow! guard))
                  (.await barrier)
                  (try
                    (with-open [_lease (lifetime/borrow! guard)]
                      (not (.get closed)))
                    (catch clojure.lang.ExceptionInfo e
                      (= :txlog/native-fenced (:error (ex-data e))))))]
      (.await barrier)
      (lifetime/close! guard 5000 #(.set closed true))
      (is (true? (deref owner 5000 ::timeout))))))

(defn- caught [f] (try (f) (catch Throwable e e)))

(deftest exited-borrowing-thread-does-not-discard-an-outstanding-lease
  (let [guard (lifetime/create)
        borrowed (promise)
        thread (Thread. ^Runnable #(deliver borrowed (lifetime/borrow! guard)))]
    (.start thread)
    (.join thread 5000)
    (is (not (.isAlive thread)))
    (with-open [_lease (lifetime/borrow! guard)]
      (is (= 2 (:native-users (lifetime/state guard)))))
    (is (= :txlog/native-not-quiescent
           (:error (ex-data (caught #(lifetime/close! guard 1 (fn [])))))))
    (.close ^AutoCloseable @borrowed)
    (lifetime/close! guard 1000 (fn []))
    (is (= {:phase :closed :native-users 0} (lifetime/state guard)))))

(deftest uninterruptible-native-user-prevents-close-and-reopen
  (let [guard (lifetime/create)
        entered (promise) release (CountDownLatch. 1)
        owner-thread (promise) events (atom [])
        reader (lifetime/enter! guard)
        owner (future
                (with-open [_lease (lifetime/enter! guard)]
                  (deliver owner-thread (Thread/currentThread))
                  (deliver entered true)
                  ;; A fake JNI call that records interrupts but cannot exit
                  ;; until released. Closing its resources cannot unblock it.
                  (loop []
                    (when-not (try (.await release) true
                                   (catch InterruptedException _ false))
                      (recur)))
                  (swap! events conj :native-exited)))
        teardown #(swap! events into [:env-close :unregister :lease-release])]
    (try
      (is (deref entered 5000 false))
      (.interrupt ^Thread @owner-thread)
      (dotimes [_ 2]
        (is (= :txlog/native-not-quiescent
               (:error (ex-data (caught #(lifetime/close! guard 10 teardown)))))))
      (is (= {:phase :fenced :native-users 2} (lifetime/state guard)))
      (is (= :txlog/native-fenced
             (:error (ex-data (caught #(lifetime/enter! guard))))))
      (is (empty? @events))
      (.countDown release)
      (is (not= ::timeout (deref owner 5000 ::timeout)))
      (is (= :txlog/native-not-quiescent
             (:error (ex-data (caught #(lifetime/close! guard 10 teardown))))))
      (is (= [:native-exited] @events) "A live reader still prevents teardown")
      (.close ^AutoCloseable reader)
      (lifetime/close! guard 1000 teardown)
      (lifetime/close! guard 1000 teardown)
      (is (= [:native-exited :env-close :unregister :lease-release] @events))
      (is (= {:phase :closed :native-users 0} (lifetime/state guard)))
      (finally
        (.countDown release)
        (deref owner 5000 nil)
        (.close ^AutoCloseable reader)))))

(deftest interrupted-close-keeps-the-environment-fenced
  (let [guard (lifetime/create)
        reader (lifetime/enter! guard)
        closed (atom false)
        result (future
                 (.interrupt (Thread/currentThread))
                 (try
                   [(ex-data (caught #(lifetime/close! guard 1000
                                                       (fn [] (reset! closed true)))))
                    (.isInterrupted (Thread/currentThread))]
                   (finally (Thread/interrupted))))]
    (try
      (let [[error interrupted?] (deref result 5000 nil)]
        (is (= :txlog/native-not-quiescent (:error error)))
        (is interrupted?))
      (is (false? @closed))
      (is (= :fenced (:phase (lifetime/state guard))))
      (finally (.close ^AutoCloseable reader)))))
