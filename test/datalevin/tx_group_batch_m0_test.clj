(ns datalevin.tx-group-batch-m0-test
  "Private M0 concurrency exits on the real WAL/native path."
  (:require [clojure.test :refer [deftest is testing]]
            [datalevin.bits :as bits]
            [datalevin.interface :as i]
            [datalevin.lmdb :as l]
            [datalevin.tx-group.batch :as batch]
            [datalevin.tx-group.batch.charge :as charge]
            [datalevin.tx-group.batch.env :as env]
            [datalevin.tx-group.batch.private :as private]
            [datalevin.tx-group.batch.stage :as stage]
            [datalevin.tx-group.phase :as phase]
            [datalevin.txlog :as wal]
            [datalevin.util :as u])
  (:import [java.nio ByteBuffer]
           [java.util.concurrent CountDownLatch TimeUnit]
           [java.util.concurrent.atomic AtomicBoolean]))

(def ^:dynamic *request-marker* :root)

(defn- encoded-long [value]
  (let [buffer (ByteBuffer/allocate 9)]
    (bits/put-buffer buffer value :long)
    (.array buffer)))

(defn- start-thread [f]
  (let [outcome (promise)
        thread (doto (Thread. ^Runnable
                             (fn []
                               (deliver outcome
                                        (try (f) (catch Throwable t t)))))
                 (.start))]
    {:thread thread :outcome outcome}))

(defn- blind-put! [collector]
  (batch/submit!
   collector
   {:allowance (charge/blind-allowance {:declared-bytes 128 :scratch-bytes 16384})
    :prepare (fn [_]
               (let [rows (java.util.Collections/singletonList
                           (l/kv-tx :put "data" (encoded-long 0)
                                    (encoded-long 0) :raw :raw))]
                 {:rows rows :wal-body (wal/prepare-append-body rows {})
                  :result :seed}))}))

(deftest ordered-bodies-use-explicit-context-on-a-foreign-leader
  (let [dir (u/tmp-dir (str "wal-m0-context-" (random-uuid)))
        environment (private/open! {:dir dir :db-identity (str (random-uuid))
                                    :wal-durability-profile :strict
                                    :wal-sync-mode :fsync
                                    :snapshot-scheduler? false
                                    :wal-segment-prealloc? false})
        {:keys [raw wal-state]} (env/resources environment)
        collector (env/collector environment)
        paused (CountDownLatch. 1)
        release (CountDownLatch. 1)
        queued (CountDownLatch. 2)
        hold-first (AtomicBoolean. true)
        local (ThreadLocal.)
        bodies (atom [])
        confirmations (atom [])
        uninstall
        (phase/observe!
         (fn [event context]
           (case event
             :joint-publication
             (when (.compareAndSet hold-first true false)
               (.countDown paused)
               (.await release 5 TimeUnit/SECONDS))
             :ready-published
             (when (#{:leader :follower} (:id (batch/context context)))
               (.countDown queued))
             nil)))]
    (try
      (i/open-dbi raw "data")
      (let [seed (start-thread #(blind-put! collector))]
        (is (.await paused 5 TimeUnit/SECONDS))
        (let [submit
              (fn [id value]
                (binding [*request-marker* id]
                  (.set local id)
                  (try
                    (batch/submit!
                     collector
                     {:context {:id id :value value
                                :confirm! (fn [context result]
                                            (swap! confirmations conj
                                                   {:id (:id context) :value result
                                                    :thread (Thread/currentThread)
                                                    :binding *request-marker*
                                                    :local (.get local)}))}
                      :op (fn [tx]
                            (let [context (stage/tx-context tx)]
                              (swap! bodies conj {:id (:id context)
                                                 :thread (Thread/currentThread)
                                                 :binding *request-marker*
                                                 :local (.get local)})
                              (stage/tx-put! tx "data" (encoded-long value)
                                             (encoded-long (:value context)))
                              (:value context)))})
                    (finally (.remove local)))))
              leader (start-thread #(submit :leader 10))]
          ;; Publication order chooses the first queued caller as the leader.
          (loop [remaining 5000]
            (when (and (= 2 (.getCount queued)) (pos? remaining))
              (Thread/sleep 1)
              (recur (dec remaining))))
          (is (= 1 (.getCount queued)))
          (let [follower (start-thread #(submit :follower 11))]
            (is (.await queued 5 TimeUnit/SECONDS))
            (.countDown release)
            (is (= :seed (deref (:outcome seed) 5000 ::timeout)))
            (is (= 10 (deref (:outcome leader) 5000 ::timeout)))
            (is (= 11 (deref (:outcome follower) 5000 ::timeout)))
            (is (= [:leader :follower] (mapv :id @bodies)))
            (is (every? #(identical? (:thread leader) (:thread %)) @bodies))
            (is (every? #(= :leader (:binding %)) @bodies)
                "the follower's dynamic bindings are not conveyed")
            (is (every? #(= :leader (:local %)) @bodies)
                "the follower's thread-local value is not conveyed")
            (is (= [:leader :follower] (mapv :id @confirmations)))
            (is (= [10 11] (mapv :value @confirmations)))
            (is (every? #(identical? (:thread leader) (:thread %)) @confirmations))
            (is (every? #(= :leader (:binding %)) @confirmations))
            (is (every? #(= :leader (:local %)) @confirmations))
            (is (= 10 (i/get-value raw "data" 10 :long :long)))
            (is (= 11 (i/get-value raw "data" 11 :long :long)))
            (is (= 3 (long @(:next-lsn wal-state))))
            (is (zero? (:requests (batch/usage collector)))))))
      (finally
        (.countDown release)
        (uninstall)
        (env/close! environment)
        (u/delete-files dir)))))

(deftest confirmation-releases-execution-and-can-resubmit
  (let [dir (u/tmp-dir (str "wal-m0-confirm-" (random-uuid)))
        environment (private/open! {:dir dir :db-identity (str (random-uuid))
                                    :wal-durability-profile :strict
                                    :wal-sync-mode :fsync
                                    :snapshot-scheduler? false
                                    :wal-segment-prealloc? false})
        {:keys [raw]} (env/resources environment)
        collector (env/collector environment)
        entered (CountDownLatch. 1)
        release (CountDownLatch. 1)
        calls (atom 0)]
    (try
      (i/open-dbi raw "data")
      (let [owner (start-thread
                   #(batch/submit!
                     collector
                     {:context {:confirm! (fn [_ result]
                                           (swap! calls inc)
                                           (is (= :joined result))
                                           (.countDown entered)
                                           (.await release 5 TimeUnit/SECONDS)
                                           (blind-put! collector))}
                      :op (fn [tx]
                            (stage/tx-put! tx "data" (encoded-long 1)
                                           (encoded-long 1))
                            :joined)}))]
        (is (.await entered 5 TimeUnit/SECONDS))
        (is (= 1 (batch/published-lsn collector)))
        (is (pos? (:requests (batch/usage collector))))
        (let [follower (start-thread #(blind-put! collector))]
          (is (= :seed (deref (:outcome follower) 5000 ::timeout))
              "a slow confirmation cannot hold up the next native/WAL batch"))
        (.countDown release)
        (is (= :joined (deref (:outcome owner) 5000 ::timeout)))
        (is (= 1 @calls))
        (is (= 3 (batch/published-lsn collector)))
        (is (zero? (:requests (batch/usage collector)))))
      (let [error (try
                    (batch/submit! collector
                                   {:context {:confirm! (fn [_ _]
                                                         (throw (ex-info "callback" {})))}
                                    :op (fn [tx]
                                          (stage/tx-put! tx "data" (encoded-long 2)
                                                         (encoded-long 2)))})
                    (catch Throwable t t))]
        (is (= :committed (:outcome (ex-data error))))
        (is (batch/serving? collector))
        (is (= 2 (i/get-value raw "data" 2 :long :long)))
        (is (zero? (:requests (batch/usage collector)))))
      (finally
        (.countDown release)
        (env/close! environment)
        (u/delete-files dir)))))

(deftest a-stuck-real-body-retains-ownership-until-it-exits
  (let [dir (u/tmp-dir (str "wal-m0-stuck-body-" (random-uuid)))
        environment (private/open! {:dir dir :db-identity (str (random-uuid))
                                    :wal-durability-profile :strict
                                    :wal-sync-mode :fsync
                                    :snapshot-scheduler? false
                                    :wal-segment-prealloc? false})
        {:keys [raw wal-state]} (env/resources environment)
        collector (env/collector environment)
        entered (CountDownLatch. 1)
        release (CountDownLatch. 1)
        calls (atom 0)]
    (try
      (i/open-dbi raw "data")
      (let [owner (start-thread
                   #(batch/submit!
                     collector
                     {:timeout-ms 100
                      :op (fn [tx]
                            (swap! calls inc)
                            (.countDown entered)
                            (.await release 5 TimeUnit/SECONDS)
                            (stage/tx-put! tx "data" (encoded-long 1)
                                           (encoded-long 1)))}))]
        (is (.await entered 5 TimeUnit/SECONDS))
        (let [follower (start-thread
                        #(batch/submit! collector
                                        {:timeout-ms 300 :op (fn [_] :unrun)}))
              error (deref (:outcome follower) 5000 ::timeout)]
          (is (instance? Throwable error))
          (is (not (batch/serving? collector)))
          (testing "the observer fences but cannot release the native owner"
            (is (pos? (:requests (batch/usage collector))))
            (is (pos? (:bytes (batch/usage collector))))
            (is (false? (batch/await-quiescence! collector 10)))
            (is (false? (i/closed-kv? raw))))
          (.countDown release)
          (is (instance? Throwable (deref (:outcome owner) 5000 ::timeout)))
          (is (= 1 @calls))
          (is (= 1 (long @(:next-lsn wal-state)))
              "neither the cancelled body nor its follower appends WAL")
          (is (= :txlog/native-fenced
                 (:error (ex-data (try (i/get-value raw "data" 1 :long :long)
                                       (catch clojure.lang.ExceptionInfo e e)))))
              "a terminal failure fence stops reads until recovery")
          (is (batch/await-quiescence! collector 5000))
          (is (zero? (:requests (batch/usage collector))))))
      (finally
        (.countDown release)
        (env/close! environment)
        (u/delete-files dir)))))
