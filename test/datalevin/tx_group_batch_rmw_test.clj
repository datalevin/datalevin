(ns datalevin.tx-group-batch-rmw-test
  (:require [clojure.test :refer [deftest is]]
            [datalevin.interface :as i]
            [datalevin.lmdb :as l]
            [datalevin.scan :as scan]
            [datalevin.tx-group.batch :as batch]
            [datalevin.tx-group.batch.env :as env]
            [datalevin.tx-group.batch.private :as private]
            [datalevin.tx-group.batch.stage :as tx]
            [datalevin.tx-group.phase :as phase]
            [datalevin.util :as u])
  (:import [java.io IOException]
           [java.nio ByteBuffer]
           [java.util.concurrent CountDownLatch TimeUnit]
           [java.util.concurrent.atomic AtomicBoolean]))

(defn- encoded ^bytes [n] (.array (.putLong (ByteBuffer/allocate 8) (long n))))
(defn- decoded [^bytes bs] (when bs (.getLong (ByteBuffer/wrap bs))))

(defn- with-runtime [f]
  (let [dir (u/tmp-dir (str "native-rmw-" (random-uuid)))
        opts {:dir dir :db-identity (str (random-uuid)) :snapshot-scheduler? false}
        environment (private/open! opts)]
    (try (f environment opts)
         (finally (env/close! environment) (u/delete-files dir)))))

(defn- group! [collector requests]
  (let [paused (CountDownLatch. 1) release (CountDownLatch. 1)
        first? (AtomicBoolean. true)
        ready (mapv (fn [_] (CountDownLatch. 1)) requests)
        uninstall (phase/observe!
                   (fn [event context]
                     (case event
                       :joint-publication
                       (when (.compareAndSet first? true false)
                         (.countDown paused) (.await release 10 TimeUnit/SECONDS))
                       :ready-published
                       (when-let [idx (:idx (batch/context context))]
                         (.countDown ^CountDownLatch (ready idx)))
                       nil)))]
    (try
      (let [seed (future (batch/submit! collector {:op (fn [_] :seed)}))]
        (is (.await paused 10 TimeUnit/SECONDS))
        (let [jobs (mapv (fn [idx request]
                          (let [job (future
                                      (try
                                        {:value (batch/submit! collector
                                                              (assoc request :context {:idx idx}))}
                                        (catch Throwable t
                                          {:error t :interrupted? (.isInterrupted (Thread/currentThread))})
                                        (finally (Thread/interrupted))))]
                            (is (.await ^CountDownLatch (ready idx) 10 TimeUnit/SECONDS))
                            job))
                        (range) requests)]
          (.countDown release)
          (is (= :seed (deref seed 10000 ::timeout)))
          (mapv #(deref % 10000 ::timeout) jobs)))
      (finally (.countDown release) (uninstall)))))

(deftest a-body-failure-aborts-every-request-and-skips-the-suffix
  (with-runtime
    (fn [environment opts]
      (let [c (env/collector environment) raw (:raw (env/resources environment))
            bodies (atom []) failure (ex-info "application rejected" {})
            results
            (group! c
                    [{:op (fn [db]
                            (swap! bodies conj :first)
                            (is (l/writing? db))
                            (i/transact-kv db [(l/kv-tx :put "data" (encoded 1) (encoded 11) :raw :raw)])
                            (is (= 11 (decoded (i/get-value db "data" (encoded 1) :raw :raw))))
                            (is (nil? (i/get-value raw "data" (encoded 1) :raw :raw)))
                            :first)}
                     {:op (fn [db]
                            (swap! bodies conj :reject)
                            (tx/tx-put! db "data" (encoded 1) (encoded 99))
                            (tx/tx-del! db "data" (encoded 1))
                            (tx/tx-put! db "data" (encoded 2) (encoded 22))
                            (throw failure))}
                     {:op (fn [db]
                            (swap! bodies conj :successor)
                            (is (= 11 (decoded (i/get-value db "data" (encoded 1) :raw :raw))))
                            (is (nil? (i/get-value db "data" (encoded 2) :raw :raw)))
                            (is (= 1 (i/range-count db "data" [:all] :raw)))
                            (tx/tx-put! db "data" (encoded 3) (encoded 33))
                            :last)}])]
        (is (= [:first :reject] @bodies))
        (is (every? #(= :not-committed (:outcome (ex-data (:error %)))) results))
        (is (every? #(identical? failure (.getCause ^Throwable (:error %))) results))
        (is (every? #(nil? (i/get-value raw "data" (encoded %) :raw :raw)) [1 2 3]))
        (is (batch/serving? c))
        (is (zero? (:requests (batch/usage c))))
        (env/close! environment)
        (let [reopened (private/open! opts)]
          (try
            (let [db (:raw (env/resources reopened))]
              (is (every? #(nil? (i/get-value db "data" (encoded %) :raw :raw)) [1 2 3])))
            (finally (env/close! reopened))))))))

(deftest owner-interruption-cancels-the-undispatched-native-batch
  (doseq [wrapped? [false true]]
    (with-runtime
      (fn [environment _]
        (let [c (env/collector environment) raw (:raw (env/resources environment))
              ran? (atom false)
              results (group! c [{:op (fn [db]
                                       (tx/tx-put! db "data" (encoded 1) (encoded 11))
                                       (let [interruption (InterruptedException. "owner")]
                                         (throw (if wrapped?
                                                  (ex-info "wrapped owner interruption" {} interruption)
                                                  interruption))))}
                                {:op (fn [_] (reset! ran? true))}])]
          (is (false? @ran?))
          (is (:interrupted? (results 0)))
          (is (batch/serving? c))
          (is (nil? (i/get-value raw "data" (encoded 1) :raw :raw)))
          (is (= :next (batch/submit! c {:op (fn [_] :next)}))))))))

(deftest caught-abort-or-allowance-exhaustion-still-aborts-the-whole-batch
  (doseq [kind [:abort :allowance :close]]
    (with-runtime
      (fn [environment _]
        (let [c (env/collector environment) raw (:raw (env/resources environment))
              ran? (atom false)
              results
              (group! c
                      [{:op #(tx/tx-put! % "data" (encoded 1) (encoded 11))}
                       {:allowance (if (= kind :allowance) 4096 1048576)
                        :op (fn [db]
                              (try
                                (case kind
                                  :abort (i/abort-transact-kv db)
                                  :close (i/close-transact-kv db)
                                  :allowance (tx/tx-put! db "data" (encoded 2) (byte-array 8192)))
                                (catch Throwable _ :caught)))}
                       {:op (fn [_] (reset! ran? true))}])]
          (is (false? @ran?))
          (is (every? #(= :not-committed (:outcome (ex-data (:error %)))) results))
          (is (every? #(nil? (i/get-value raw "data" (encoded %) :raw :raw)) [1 2]))
          (is (batch/serving? c))
          (is (= 1 @(:next-lsn (:wal-state (env/resources environment)))))
          (is (zero? (:requests (batch/usage c)))))))))

(deftest failure-after-wal-completion-recovers-the-entire-native-batch
  (with-runtime
    (fn [environment opts]
      (let [c (env/collector environment)
            calls (atom 0)
            uninstall (phase/observe!
                       (fn [event _]
                         (when (= event :wal-complete)
                           (throw (IOException. "native completion failed")))))]
        (try
          (let [failure (try
                          (batch/submit! c
                                         {:op (fn [db]
                                                (swap! calls inc)
                                                (tx/tx-put! db "data" (encoded 1) (encoded 11))
                                                (tx/tx-put! db "data" (encoded 2) (encoded 22)))})
                          (catch Throwable t t))]
            (is (= :committed (:outcome (ex-data failure))))
            (is (= :durable (:wal-status (ex-data failure))))
            (is (not (batch/serving? c))))
          (finally (uninstall)))
        (env/close! environment)
        (let [reopened (private/open! opts)]
          (try
            (let [db (:raw (env/resources reopened))]
              (is (= 11 (decoded (i/get-value db "data" (encoded 1) :raw :raw))))
              (is (= 22 (decoded (i/get-value db "data" (encoded 2) :raw :raw))))
              (is (= 1 @calls) "recovery never evaluates the body"))
            (finally (env/close! reopened))))))))

(deftest infrastructure-error-fences-and-never-runs-the-successor
  (doseq [caught? [false true]]
    (with-runtime
      (fn [environment _]
        (let [c (env/collector environment) raw (:raw (env/resources environment))
              ran? (atom false) failure (IOException. "native reader")
              results
              (group! c
                      [{:op (fn [db]
                              (tx/tx-put! db "data" (encoded 1) (encoded 11))
                              (let [native (i/get-dbi db "data" false)
                                    broken (reify l/IDB
                                             (put-read-key [_ rtx key kt]
                                               (l/put-read-key native rtx key kt))
                                             (get-kv [_ _] (throw failure)))
                                    reader (with-meta (reify
                                             i/ILMDB
                                             (check-ready [_] (i/check-ready db))
                                             (get-dbi [_ _ _] broken)
                                             l/IWriting
                                             (writing? [_] true)
                                             (write-txn [_] (l/write-txn db)))
                                             (meta db))]
                                (if caught?
                                  (try (scan/get-value reader "data" (encoded 1) :raw :raw true)
                                       (catch Throwable _ :caught))
                                  (scan/get-value reader "data" (encoded 1) :raw :raw true))))}
                       {:op (fn [_] (reset! ran? true))}])]
          (is (false? @ran?))
          (is (not (batch/serving? c)))
          (is (instance? Throwable (:error (results 0))))
          (is (zero? (:requests (batch/usage c))))
          (is (thrown? Throwable (batch/submit! c {:op (fn [_] :must-not-run)})))
          (is (thrown? Throwable (i/get-value raw "data" (encoded 1) :raw :raw))))))))

(deftest wrapped-infrastructure-errors-still-fence-the-batch
  (with-runtime
    (fn [environment _]
      (let [collector (env/collector environment)
            successor? (atom false)
            results (group! collector
                            [{:op (fn [_]
                                    (throw (ex-info "native reader failed" {}
                                                    (IOException. "reader"))))}
                             {:op (fn [_] (reset! successor? true))}])]
        (is (false? @successor?))
        (is (not (batch/serving? collector)))
        (is (every? #(instance? IOException (ex-cause (:error %))) results))
        (is (zero? (:requests (batch/usage collector))))))))
