(ns datalevin.tx-group-batch-env-test
  "Environment routing invariants for the private new-protocol wiring.

  One canonical environment selects one collector protocol, every handle shares
  that selection, and a public entry point cannot introduce a second collector
  into the same environment."
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [datalevin.tx-group.batch :as batch]
            [datalevin.tx-group.batch.env :as env]
            [datalevin.tx-state.protocol :as protocol])
  (:import [java.io File]
           [java.nio.file Files]
           [java.nio.file.attribute FileAttribute]
           [java.util.concurrent CountDownLatch TimeUnit]))

(defn- echo-values
  [batch]
  (let [n (batch/batch-count batch)
        values (object-array n)]
    (dotimes [i n]
      (aset values i (batch/data (batch/batch-at batch i))))
    values))

(defn- temp-dir
  ^String []
  (.toString (Files/createTempDirectory "dtlv-batch-env"
                                        (make-array FileAttribute 0))))

(defn- delete-tree!
  [^File file]
  (when (.isDirectory file)
    (doseq [child (.listFiles file)] (delete-tree! child)))
  (.delete file))

(def ^:private dirs (atom []))

(defn- fresh-dir! []
  (let [dir (temp-dir)]
    (swap! dirs conj dir)
    dir))

(use-fixtures :each (fn [f] (f) nil))

(defn- with-dirs-cleaned
  [f]
  (try (f) (finally
            ;; A deliberately un-drained close retains its record; drop it so a
            ;; later test cannot observe a stale environment.
            (env/reset-registry!)
            (doseq [dir @dirs] (delete-tree! (File. dir)))
            (reset! dirs []))))

(use-fixtures :each with-dirs-cleaned)

;; ---------------------------------------------------------------------------
;; Mode selection

(deftest a-batch-environment-keeps-its-protocol-across-handles
  (let [dir (fresh-dir!)
        opts {:dir dir :db-identity "db-1" :executor echo-values}
        first-handle (env/open-batch! opts)
        second-handle (env/open-batch! opts)]
    (is (identical? first-handle second-handle))
    (is (= :kv-independent-v1 (env/mode-name first-handle)))
    (is (= 2 (env/handles first-handle)))
    (testing "every handle shares one collector"
      (is (identical? (env/collector first-handle) (env/collector second-handle))))
    (testing "limits are resolved once and frozen"
      (is (= (:batch-limit (env/limits first-handle)) 256))
      (is (= (:batch-max-bytes (env/limits first-handle)) 8388608))
      (is (= (:shared-reserved (env/limits first-handle)) 1252176)))
    (env/close! first-handle)
    (testing "the runtime stays open while a handle remains"
      (is (= 1 (env/handles first-handle)))
      (is (= :a (batch/submit! (env/collector first-handle)
                               {:allowance 1024 :data :a}))))
    (env/close! second-handle)
    (is (zero? (env/handles second-handle)))
    (testing "the last release stops admission"
      (is (not (batch/serving? (env/collector second-handle)))))))

(deftest runtime-opens-once-under-the-lease-and-closes-after-last-handle
  (let [dir (fresh-dir!)
        events (atom [])
        opts {:dir dir :db-identity "private-db"
              :open-runtime!
              (fn []
                (is (= :kv-independent-v1
                       (:mode (protocol/read-write-protocol-marker dir))))
                (is (thrown? clojure.lang.ExceptionInfo
                             (protocol/acquire-write-protocol-lease!
                              dir :legacy-writer-v1 "private-db")))
                (swap! events conj :open)
                {:executor echo-values :resources :native
                 :close! #(do (swap! events conj :close) true)})}
        first-handle (env/open-batch! opts)
        alias (env/open-batch! (assoc opts :dir (str dir "/.")))]
    (is (identical? first-handle alias))
    (is (= :native (env/resources alias)))
    (is (= [:open] @events))
    (env/close! first-handle)
    (is (= [:open] @events))
    (is (= :ok (batch/submit! (env/collector alias) {:allowance 1024 :data :ok})))
    (env/close! alias)
    (is (= [:open :close] @events))))

(deftest a-failed-runtime-open-releases-its-lease
  (let [dir (fresh-dir!)
        opts {:dir dir :db-identity "private-db"}]
    (is (thrown-with-msg?
         clojure.lang.ExceptionInfo #"open failed"
         (env/open-batch! (assoc opts :open-runtime!
                                #(throw (ex-info "open failed" {}))))))
    (is (not (some #{dir} (env/active-environments))))
    (let [environment (env/open-batch! (assoc opts :executor echo-values))]
      (is (= 1 (env/handles environment)))
      (env/close! environment))))

(deftest last-close-drains-the-executor-after-collector-quiescence
  (let [dir (fresh-dir!)
        events (atom [])
        env* (atom nil)
        environment (env/open-batch!
                     {:dir dir
                      :db-identity "db-1"
                      :executor (fn [batch]
                                  (swap! events conj :batch)
                                  (echo-values batch))
                      :executor-close! (fn []
                                         (swap! events conj
                                                [:executor-close
                                                 (batch/serving?
                                                  (env/collector @env*))])
                                         true)})]
    (reset! env* environment)
    (batch/submit! (env/collector environment) {:allowance 1024 :data :a})
    (env/close! environment)
    (is (some #(= :batch %) @events))
    (is (= [:executor-close false] (last @events))
        "the executor drains only after the collector stopped serving")))

(deftest close-does-not-stop-the-executor-when-quiescence-times-out
  ;; Regression: close stopped the executor even after await-quiescence! timed
  ;; out, discarding WAL work a live batch was still waiting on.
  (let [dir (fresh-dir!)
        entered (CountDownLatch. 1)
        release (CountDownLatch. 1)
        executor-closed? (atom false)
        environment (env/open-batch!
                     {:dir dir
                      :db-identity "db-1"
                      :executor (fn [batch]
                                  (.countDown entered)
                                  (.await release 5 TimeUnit/SECONDS)
                                  (echo-values batch))
                      :executor-close! (fn []
                                         (reset! executor-closed? true)
                                         true)
                      :write-close-timeout-ms 50})
        submission (future (try (batch/submit! (env/collector environment)
                                               {:allowance 1024 :data :a})
                                (catch Throwable t t)))]
    (try
      (is (.await entered 5 TimeUnit/SECONDS))
      (env/close! environment)
      (testing "a timed-out quiescence leaves the live executor running"
        (is (false? @executor-closed?))
        (is (some #{(.getCanonicalPath (File. dir))}
                  (env/active-environments))))
      (finally
        (.countDown release)
        (deref submission 5000 nil)))))

(deftest concurrent-close-and-open-cannot-attach-to-a-retired-runtime
  ;; Regression: the closed flag was set after the registry lock was released,
  ;; so an open could attach between the last-handle decrement and retirement.
  (dotimes [_ 50]
    (let [dir (fresh-dir!)
          environment (env/open-batch! {:dir dir :db-identity "db-1"
                                        :executor echo-values})
          start (CountDownLatch. 1)
          closer (future (.await start 5 TimeUnit/SECONDS)
                         (env/close! environment))
          opener (future (.await start 5 TimeUnit/SECONDS)
                         (try (env/open-batch! {:dir dir :db-identity "db-1"
                                                :executor echo-values})
                              (catch Throwable t t)))]
      (.countDown start)
      (let [opened (deref opener 5000 nil)]
        (deref closer 5000 nil)
        (when (and (some? opened) (not (instance? Throwable opened)))
          (is (batch/serving? (env/collector opened))
              "a successful open must not attach to a retired runtime")
          (env/close! opened))))))

(deftest byte-measure-overrides-reach-the-runtime
  (let [dir (fresh-dir!)
        environment (env/open-batch! {:dir dir
                                      :db-identity "db-1"
                                      :executor echo-values
                                      :wal-pending-max-requests 64
                                      :write-batch-size 4
                                      :write-batch-max-bytes 4096
                                      :wal-rmw-max-bytes 1024})
        limits (env/limits environment)]
    (is (= 64 (:max-requests limits)))
    (is (= 4 (:batch-limit limits)))
    (is (= 4096 (:batch-max-bytes limits)))
    (is (= 64 (:waiter-limit limits)))
    (is (< (:batch-max-bytes limits) (:request-budget limits)))
    (env/close! environment)))

(deftest invalid-limits-are-rejected-before-opening
  (let [dir (fresh-dir!)
        thrown (try (env/open-batch! {:dir dir
                                      :db-identity "db-1"
                                      :executor echo-values
                                      :wal-pending-max-bytes 65536})
                    nil
                    (catch clojure.lang.ExceptionInfo e e))]
    (is (some? thrown))
    (is (= :txlog/write-protocol-limits (:error (ex-data thrown))))
    (testing "a rejected opener leaves no environment registered"
      (is (not (some #{(.getCanonicalPath (File. dir))}
                     (env/active-environments)))))))

;; ---------------------------------------------------------------------------
;; One environment, one protocol

(deftest a-batch-environment-refuses-the-compatibility-protocol
  (let [dir (fresh-dir!)
        batch-env (env/open-batch! {:dir dir :db-identity "db-1"
                                    :executor echo-values})]
    (testing "compatibility cannot be introduced into a new-protocol environment"
      (let [thrown (try (env/open-compat! {:dir dir :db-identity "db-1"})
                        nil
                        (catch clojure.lang.ExceptionInfo e e))]
        (is (some? thrown))
        (is (= :txlog/write-protocol-mismatch (:error (ex-data thrown))))
        (is (false? (:retryable? (ex-data thrown))))
        (is (= :kv-independent-v1 (:actual (ex-data thrown))))))
    (testing "the rejected attempt did not add a handle or change the mode"
      (is (= 1 (env/handles batch-env)))
      (is (= :kv-independent-v1 (env/mode-name batch-env)))
      (is (identical? batch-env (env/open-batch! {:dir dir :db-identity "db-1"
                                                  :executor echo-values}))))
    (env/close! (env/open-batch! {:dir dir :db-identity "db-1" :executor echo-values}))
    (env/close! batch-env)))

(deftest a-compatibility-environment-refuses-the-new-protocol
  (let [dir (fresh-dir!)
        compat-env (env/open-compat! {:dir dir :db-identity "db-1"})]
    (testing "a new-protocol opener cannot activate the new collector here"
      (let [thrown (try (env/open-batch! {:dir dir :db-identity "db-1"
                                          :executor echo-values})
                        nil
                        (catch clojure.lang.ExceptionInfo e e))]
        (is (some? thrown))
        (is (= :txlog/write-protocol-mismatch (:error (ex-data thrown))))
        (is (= :legacy-writer-v1 (:actual (ex-data thrown))))))
    (testing "compatibility keeps its own collector and has no batch collector"
      (is (some? (env/compat-handle compat-env)))
      (let [thrown (try (env/collector compat-env)
                        nil
                        (catch clojure.lang.ExceptionInfo e e))]
        (is (some? thrown))
        (is (= :txlog/write-protocol-mismatch (:error (ex-data thrown))))))
    (is (= 1 (env/handles compat-env)))
    (env/close! compat-env)))

(deftest separate-environments-select-their-protocols-independently
  (let [batch-dir (fresh-dir!)
        compat-dir (temp-dir)
        _ (swap! dirs conj compat-dir)
        batch-env (env/open-batch! {:dir batch-dir :db-identity "db-1"
                                    :executor echo-values})
        compat-env (env/open-compat! {:dir compat-dir :db-identity "db-2"})]
    (is (= :kv-independent-v1 (env/mode-name batch-env)))
    (is (= :legacy-writer-v1 (env/mode-name compat-env)))
    (testing "aliases resolve to the same canonical environment"
      (is (identical? batch-env
                      (env/open-batch! {:dir (str batch-dir File/separator ".")
                                        :db-identity "db-1"
                                        :executor echo-values}))))
    (testing "a replacement runtime gets its own collector and cells"
      ;; Two opens above (original + alias), so two releases retire it.
      (env/close! batch-env)
      (env/close! batch-env)
      (let [replacement (env/open-batch! {:dir batch-dir :db-identity "db-1"
                                          :executor echo-values})]
        (is (not (identical? batch-env replacement)))
        (is (= :kv-independent-v1 (env/mode-name replacement)))
        (is (batch/serving? (env/collector replacement)))
        (is (= :a (batch/submit! (env/collector replacement)
                                 {:allowance 1024 :data :a})))
        (env/close! replacement)))
    (env/close! compat-env)))

;; ---------------------------------------------------------------------------
;; Identity and quiescent close

(deftest an-open-with-a-different-identity-is-rejected
  (let [dir (fresh-dir!)
        environment (env/open-batch! {:dir dir :db-identity "db-1"
                                      :executor echo-values})]
    (testing "a new-protocol attach cannot change the runtime's identity"
      (let [thrown (try (env/open-batch! {:dir dir :db-identity "db-2"
                                          :executor echo-values})
                        nil
                        (catch clojure.lang.ExceptionInfo e e))]
        (is (some? thrown))
        (is (= :txlog/database-identity-mismatch (:error (ex-data thrown))))
        (is (false? (:retryable? (ex-data thrown))))))
    (testing "the rejected attach did not add a handle"
      (is (= 1 (env/handles environment)))
      (is (identical? environment
                      (env/open-batch! {:dir dir :db-identity "db-1"
                                        :executor echo-values}))))
    (env/close! environment)))

(deftest a-compat-open-with-a-different-identity-is-rejected
  (let [dir (fresh-dir!)
        environment (env/open-compat! {:dir dir :db-identity "db-1"})]
    (let [thrown (try (env/open-compat! {:dir dir :db-identity "db-2"})
                      nil
                      (catch clojure.lang.ExceptionInfo e e))]
      (is (some? thrown))
      (is (= :txlog/database-identity-mismatch (:error (ex-data thrown)))))
    (testing "the rejected attach did not add a handle"
      (is (= 1 (env/handles environment))))
    (env/close! environment)))

(deftest a-close-with-a-live-batch-does-not-release-the-runtime
  (let [entered (CountDownLatch. 1)
        release (CountDownLatch. 1)
        dir (fresh-dir!)
        environment (env/open-batch!
                     {:dir dir :db-identity "db-1"
                      :write-close-timeout-ms 50
                      :executor (fn [b]
                                  (.countDown entered)
                                  (.await release 5 TimeUnit/SECONDS)
                                  (echo-values b))})
        write (future (try (batch/submit! (env/collector environment)
                                          {:allowance 1024 :data :a})
                           (catch Throwable t t)))]
    (try
      (is (.await entered 5 TimeUnit/SECONDS))
      (env/close! environment)
      (testing "the runtime is fenced"
        (is (not (batch/serving? (env/collector environment)))))
      (testing "a runtime whose batch did not drain cannot be reopened"
        (let [thrown (try (env/open-batch! {:dir dir :db-identity "db-1"
                                            :executor echo-values})
                          nil
                          (catch clojure.lang.ExceptionInfo e e))]
          (is (some? thrown))
          (is (= :txlog/runtime-closed (:error (ex-data thrown))))))
      (finally
        (.countDown release)
        @write))))

(deftest a-drained-close-releases-the-runtime
  (let [dir (fresh-dir!)
        environment (env/open-batch! {:dir dir :db-identity "db-1"
                                      :executor echo-values})]
    (is (= :a (batch/submit! (env/collector environment)
                             {:allowance 1024 :data :a})))
    (env/close! environment)
    (testing "a quiescent close removes the runtime so a fresh one can open"
      (let [replacement (env/open-batch! {:dir dir :db-identity "db-1"
                                          :executor echo-values})]
        (is (not (identical? environment replacement)))
        (is (batch/serving? (env/collector replacement)))
        (env/close! replacement)))))
