(ns datalevin.server.ha-hot-path-test
  (:require
   [clojure.test :refer [deftest is testing use-fixtures]]
   [datalevin.binding.cpp :as cpp]
   [datalevin.client-op :as cop]
   [datalevin.constants :as c]
   [datalevin.core :as d]
   [datalevin.db :as db]
   [datalevin.ha :as dha]
   [datalevin.ha.control :as ctrl]
   [datalevin.ha.publisher :as publisher]
   [datalevin.interface :as i]
   [datalevin.kv :as kv]
   [datalevin.kv.txlog :as kvtx]
   [datalevin.lmdb :as l]
   [datalevin.server.ha :as ha]
   [datalevin.server.handlers :as handlers]
   [datalevin.test.core :refer [db-fixture]]
   [datalevin.tx-group.compat :as group]
   [datalevin.tx-group.batch :as batch]
   [datalevin.util :as u])
  (:import
   [datalevin.tx_group Group]
   [java.util.concurrent ConcurrentHashMap ConcurrentLinkedQueue Semaphore]
   [java.util.function BiFunction]))

(use-fixtures :each db-fixture)

(deftest ha-reads-retain-current-and-disabled-caches
  (doseq [capacity [0 16]]
    (let [path (str "/tmp/datalevin-ha-cache-" (random-uuid))
          modified (atom 1)
          checks (atom 0)
          store (reify i/IStore
                  (dir [_] path)
                  (opts [_] {:cache-limit capacity})
                  (max-tx [_] 12)
                  (last-modified [_] (swap! checks inc) @modified))
          deps {:db-state (fn [_ _] {:ha-role :follower})}
          read! #(#'handlers/ensure-ha-read-floor!
                   deps nil "db" false {:ha-read-min-tx 12} store)]
      (try
        (db/refresh-cache store 1)
        (let [token (db/cache-token store)]
          (db/cache-put store :sentinel :cached)
          (dotimes [_ 3] (read!))
          (is (= token (db/cache-token store)))
          (when (pos? (long capacity))
            (is (= :cached (db/cache-get store :sentinel))))
          (reset! modified 2)
          (read!)
          (is (nil? (db/cache-get store :sentinel)))
          (if (zero? (long capacity))
            (do (is (= token (db/cache-token store)))
                (is (zero? @checks)))
            (do (is (not= token (db/cache-token store)))
                (is (false? (db/cache-put-if-current
                              store token :sentinel :stale))))))
        (db/disable-cache store)
        (let [token (db/cache-token store)
              before @checks]
          (reset! modified 3)
          (read!)
          (is (= before @checks))
          (is (= token (db/cache-token store)))
          (is (db/cache-disabled? store)))
        (db/enable-cache store)
        (read!)
        (is (not (db/cache-disabled? store)))
        (finally (db/remove-cache store))))))

(defn- await-queued! [^Group g n]
  (let [deadline (+ (System/nanoTime) 10000000000)]
    (loop []
      (cond
        (= n (.size ^ConcurrentLinkedQueue (.-queue g))) true
        (> (System/nanoTime) deadline) false
        :else (do (Thread/sleep 1) (recur))))))

(defn- authority-probe
  ([outcome] (authority-probe outcome (fn [])))
  ([outcome on-renew]
  (let [now (System/currentTimeMillis)
        lease (atom {:db-identity "db" :leader-node-id 1 :term 1
                     :leader-endpoint "127.0.0.1:19001"
                     :lease-until-ms (+ now 600000)
                     :leader-last-applied-lsn 0})
        reads (atom 0)
        version (atom 0)
        renews (atom [])
        entered (promise) release (promise)
        confirming (promise) confirmed (promise)
        authority
        (reify ctrl/ILeaseAuthority
          (read-lease [_ _]
            (swap! reads inc)
            {:lease @lease :version @version})
          (read-membership-hash [_] nil)
          (read-voters [_] [])
          (renew-lease [_ req]
            (on-renew)
            (swap! renews conj (:leader-last-applied-lsn req))
            (when (= 1 (count @renews))
              (deliver entered true)
              (assert (deref release 10000 false)))
            (when (= 2 (count @renews))
              (deliver confirming true)
              (assert (deref confirmed 10000 false)))
            (let [ok? (not (and (= outcome :indeterminate)
                                (= 2 (count @renews))))]
              (when ok?
                (swap! version inc)
                (swap! lease assoc :leader-last-applied-lsn
                       (:leader-last-applied-lsn req)))
              {:ok? ok? :reason (when-not ok? :timeout)
               :lease @lease :version @version
               :authority-now-ms (System/currentTimeMillis)})))
        state {:ha-authority authority :ha-role :leader :ha-node-id 1
               :ha-renewal-publisher (publisher/create)
               :ha-authority-version 0
               :ha-db-identity "db" :ha-authority-lease @lease
               :ha-authority-owner-node-id 1
               :ha-leader-term 1 :ha-authority-term 1
               :ha-authority-read-ok? true
               :ha-last-authority-refresh-ms now
               :ha-lease-renew-ms 60000 :ha-lease-timeout-ms 600000
               :ha-lease-local-deadline-ms (:lease-until-ms @lease)}
        dbs (doto (ConcurrentHashMap.) (.put "db" state))
        admission-lock (Object.)
        deps {:dbs-fn (constantly dbs)
              :db-state (fn [_ name] (.get dbs name))
              :update-db-fn (fn [_ name f]
                              (let [m (f (.get dbs name))]
                                (.put dbs name m) m))
              :ensure-udf-readiness-state-fn identity
              :udf-admission-exempt-write-types #{}
              :udf-write-admission-error-fn (fn [_ _] nil)
              :db-write-admission-lock-fn (fn [_ _] admission-lock)
              :replace-db-state-if-current-fn
              (fn [_ name expected pred next-state]
                (let [updated? (and (pred (.get dbs name))
                                    (.replace dbs name expected next-state))]
                  {:updated? updated? :state (.get dbs name)}))
              :transform-db-state-when-fn
              (fn [_ name pred f]
                (.compute dbs name
                          (reify BiFunction
                            (apply [_ _ state]
                              (if (pred state) (f state) state))))
                {:state (.get dbs name)})}]
    {:deps deps :dbs dbs :reads reads :renews renews
     :entered entered :release release
     :confirming confirming :confirmed confirmed})))

(deftest confirmed-group-skips-renewal-and-admission-lock
  (let [{:keys [deps reads renews release confirmed] :as probe}
        (authority-probe :ok)
        ^ConcurrentHashMap dbs (:dbs probe)
        locks (atom 0)
        lock-fn (:db-write-admission-lock-fn deps)
        deps (assoc deps :db-write-admission-lock-fn
                    (fn [server db-name]
                      (swap! locks inc)
                      (lock-fn server db-name)))
        publish (ha/ha-write-commit-publish-fn
                  deps nil {:type :tx-data :args ["db"]})]
    (deliver release true)
    (deliver confirmed true)
    (is (nil? (publish {:txlog-lsn 42})))
    (is (= [42] @renews))
    (doseq [lsn [41 42]]
      (is (nil? (publish {:txlog-lsn lsn}))))
    (is (= [42] @renews))
    (is (zero? @locks))
    ;; Local progress alone cannot justify skipping authority publication.
    (.put dbs "db" (assoc (.get dbs "db") :ha-leader-last-applied-lsn 100))
    (is (nil? (publish {:txlog-lsn 43})))
    (is (= [42 100] @renews))
    (is (zero? @locks))
    (is (zero? @reads))))

(deftest periodic-and-write-confirmation-share-inflight-renewal
  (doseq [first-caller [:periodic :write]]
    (let [{:keys [deps reads renews entered release] :as probe} (authority-probe :ok)
          ^ConcurrentHashMap dbs (:dbs probe)
          state (assoc (.get dbs "db") :ha-leader-last-applied-lsn 42)
          publication (:ha-renewal-publisher state)
          joined (promise)
          periodic #(let [next-state (dha/ha-renew-step "db" state)]
                      (ha/publish-ha-renew-state!
                        deps nil "db" state next-state nil)
                      next-state)
          _ (.put dbs "db" state)
          publish (ha/ha-write-commit-publish-fn
                    deps nil {:type :tx-data :args ["db"]})
          write #(publish {:txlog-lsn 42})
          first-job (future ((if (= first-caller :periodic) periodic write)))
          jobs (atom [first-job])]
      (try
        (is (deref entered 10000 false))
        (add-watch publication ::joined
                   (fn [_ _ old new]
                     (when (and (:pending old)
                                (identical? (:pending old) (:pending new)))
                       (deliver joined true))))
        (swap! jobs conj (future ((if (= first-caller :periodic) write periodic))))
        (is (deref joined 10000 false))
        (is (not-any? realized? @jobs))
        (is (= [42] @renews))
        (deliver release true)
        (doseq [job @jobs] (is (not= ::timeout (deref job 10000 ::timeout))))
        (is (= [42] @renews))
        (is (zero? @reads))
        (let [outcomes (mapv deref @jobs)
              periodic-state (nth outcomes (if (= first-caller :periodic) 0 1))]
          (is (= (:ha-lease-local-deadline-nanos periodic-state)
                 (:ha-lease-local-deadline-nanos (.get dbs "db")))))
        (finally
          (remove-watch publication ::joined)
          (deliver release true)
          (doseq [job @jobs] (deref job 10000 nil)))))))

(deftest covered-watermark-requires-fresh-matching-lease-proof
  (doseq [[label patch reason]
          [[:stale #(assoc % :ha-last-authority-refresh-ms 0) nil]
           [:unknown-age #(dissoc % :ha-last-authority-refresh-ms) nil]
           [:read-failed #(assoc % :ha-authority-read-ok? false) nil]
           [:expired #(assoc % :ha-lease-local-deadline-nanos 0) nil]
           [:wrong-owner #(assoc-in % [:ha-authority-lease :leader-node-id] 2) nil]
           [:wrong-term #(assoc-in % [:ha-authority-lease :term] 0) nil]
           [:wrong-db #(assoc-in % [:ha-authority-lease :db-identity] "other") nil]
           [:follower #(assoc % :ha-role :follower) :not-leader]
           [:new-term #(-> % (assoc :ha-leader-term 2 :ha-authority-term 2)
                           (assoc-in [:ha-authority-lease :term] 2))
            :leadership-changed]
           [:new-runtime #(assoc % :ha-renew-loop-running? (Object.))
            :leadership-changed]]]
    (testing (name label)
      (let [{:keys [deps reads renews release confirmed] :as probe}
            (authority-probe :ok)
            ^ConcurrentHashMap dbs (:dbs probe)
            publish (ha/ha-write-commit-publish-fn
                      deps nil {:type :tx-data :args ["db"]})]
        (deliver release true)
        (deliver confirmed true)
        (publish {:txlog-lsn 42})
        (.put dbs "db" (patch (.get dbs "db")))
        (let [error (try (publish {:txlog-lsn 42}) nil
                         (catch clojure.lang.ExceptionInfo e (ex-data e)))]
          (if reason
            (do (is (= :ha/write-indeterminate (:error error)))
                (is (= reason (:reason error)))
                (is (= [42] @renews)))
            (do (is (nil? error))
                (is (= [42 42] @renews)))))
        (is (zero? @reads))))))

(deftest cached-commit-admission-fails-closed-without-authority-io
  (doseq [[patch reason]
          [[{:ha-role :follower} :not-leader]
           [{:ha-authority-owner-node-id 2} :owner-mismatch]
           [{:ha-authority-read-ok? false} :authority-read-failed]
           [{:ha-last-authority-refresh-ms 0} :authority-read-stale]
           [{:ha-lease-local-deadline-nanos 0} :lease-expired]
           [{:ha-clock-skew-paused? true} :clock-skew-paused]
           [{:ha-leader-fencing-pending? true} :fencing-pending]
           [{:ha-leader-term 2 :ha-authority-term 2} :leadership-changed]]]
    (let [{:keys [deps reads renews] :as probe} (authority-probe :ok)
          ^ConcurrentHashMap dbs (:dbs probe)
          check (ha/ha-write-commit-check-fn
                  deps nil {:type :tx-data :args ["db"]})]
      (.put dbs "db" (merge (.get dbs "db") patch))
      (let [error (try (check nil) nil
                       (catch clojure.lang.ExceptionInfo e (ex-data e)))]
        (is (= :ha/write-rejected (:error error)))
        (is (= reason (:reason error))))
      (is (zero? @reads))
      (is (empty? @renews)))))

(deftest periodic-renewal-still-publishes-the-idle-leaders-watermark
  (let [{:keys [deps reads renews release] :as probe} (authority-probe :ok)
        ^ConcurrentHashMap dbs (:dbs probe)
        state (assoc (.get dbs "db") :ha-leader-last-applied-lsn 42)]
    (.put dbs "db" state)
    (deliver release true)
    (let [next-state (dha/ha-renew-step "db" state)]
      (ha/publish-ha-renew-state! deps nil "db" state next-state nil)
      (is (= :leader (:ha-role (.get dbs "db"))))
      (is (= [42] @renews))
      (is (zero? @reads))
      (is (= 42 (get-in (.get dbs "db")
                       [:ha-authority-lease :leader-last-applied-lsn]))))))

(deftest ha-groups-commit-while-earlier-confirmation-waits
  (doseq [api [:kv :datalog]
          outcome [:ok :indeterminate]]
    (testing (str api " " outcome)
      (let [path (u/tmp-dir (str "datalevin-ha-group-" (random-uuid)))
            opts {:wal? true :wal-durability-profile :strict :wal-shared? false
                  :wal-segment-prealloc? false :snapshot-bootstrap-force? false}
            conn (when (= api :datalog)
                   (d/create-conn path {:counter {:db/valueType :db.type/long}}
                                  opts))
            store (if conn (d/datalog-kv conn) (d/open-kv path opts))
            monitors (atom [])
            {:keys [deps dbs reads renews entered release confirming confirmed]}
            (authority-probe
              outcome
              #(swap! monitors conj [(Thread/holdsLock (l/write-txn store))
                                     (Thread/holdsLock (i/kv-info store))]))
            slot (Semaphore. 1)
            deps (assoc deps :get-lock (fn [_ _] slot)
                             :transaction-lock-timeout-ms (constantly 10000))
            message {:type (if conn :tx-data :update-kv) :args ["db"]}
            committing (promise) commit-release (promise)
            commits (atom 0) second-commit (promise) third-commit (promise)
            jobs (atom [])]
        (try
          (if conn
            (d/transact! conn [{:db/id 1 :counter 0}])
            (do (d/open-dbi store "counter")
                (d/transact-kv store [[:put "counter" 1 0 :id :long]])))
          (vswap! (i/kv-info store) assoc :ha-mode :consensus-lease)
          (is (nil? (kv/write-group store :kv)))
          (is (nil? (#'handlers/server-write-group deps nil "db" store api)))
          (binding [cpp/*before-write-commit-fn*
                    (let [check (ha/ha-write-commit-check-fn deps nil message)]
                      (fn [ctx]
                        (check ctx)
                        (case (long (swap! commits inc))
                          1 (do (deliver committing true)
                                (assert (deref commit-release 10000 false)))
                          2 (assert (deref entered 10000 false))
                          nil)))
                    kvtx/*after-txlog-append-fn*
                    (let [publish (ha/ha-write-commit-publish-fn deps nil message)]
                      (fn [ctx]
                        (publish ctx)
                        (case (long @commits)
                          2 (deliver second-commit true)
                          3 (deliver third-commit true)
                          nil)))]
            (let [g (#'handlers/server-write-group deps nil "db" store api)
                  before (long (:last-committed-lsn (d/txlog-watermarks store)))
                  run-tx (fn [execute]
                           (#'handlers/with-direct-db-transaction-slot
                             deps nil "db" false
                             #(if conn
                                (d/with-transaction [tx conn]
                                  (if (group/batched?)
                                    (:result (db/execute-write-group tx execute))
                                    (execute tx)))
                                (l/with-transaction-kv [tx store] (execute tx)))))
                  op (if conn
                       (fn [tx]
                         (d/transact! tx [[:db/add 1 :counter
                                          (inc (long (:counter (d/pull @tx [:counter] 1))))]]))
                       #(d/update-kv % "counter" 1 inc :id :long))
                  submit #(future
                            (try (group/submit! g run-tx op) :ok
                                 (catch Throwable t t)))]
              (is (some? g))
              (swap! jobs conj (submit))
              (is (deref committing 10000 false))
              (dotimes [idx 3]
                (swap! jobs conj (submit))
                (is (await-queued! g (inc idx))))
              (deliver commit-release true)
              (is (deref entered 10000 false))
              ;; A second physical group commits while the first renewal waits.
              (is (deref second-commit 10000 false))
              (is (= (+ before 2) (:last-applied-lsn (d/txlog-watermarks store))))
              (is (not-any? realized? @jobs))
              (deliver release true)
              (is (deref confirming 10000 false))
              (is (= :ok (deref (first @jobs) 10000 ::timeout)))
              (is (not-any? realized? (rest @jobs)))
              ;; The queued leader also hands off before waiting. The next
              ;; direct caller can commit, but needs its own higher LSN proof.
              (swap! jobs conj (submit))
              (is (deref third-commit 10000 false))
              (is (= (+ before 3) (:last-applied-lsn (d/txlog-watermarks store))))
              (let [acquired? (.tryAcquire slot 10 java.util.concurrent.TimeUnit/SECONDS)]
                (is acquired?)
                (when acquired? (.release slot)))
              (is (not-any? realized? (rest @jobs)))
              (deliver confirmed true)
              (let [results (mapv #(deref % 10000 ::timeout) @jobs)
                    batch (subvec results 1 4)]
                (is (= :ok (last results)))
                (if (= outcome :ok)
                  (is (every? #{:ok} results))
                  (do
                    (is (every? #(= :ha/write-indeterminate (:error (ex-data %))) batch))
                    (is (identical? (first batch) (second batch)))
                    (is (identical? (first batch) (last batch)))))
                (is (= [false false] (first @monitors)))
                (is (every? #{[false false]} @monitors))
                (is (= (mapv #(+ before (long %)) [1 2 3]) @renews))
                (is (= 3 @commits))
                (is (zero? @reads))
                (is (= 5 (if conn (:counter (d/pull @conn [:counter] 1))
                             (d/get-value store "counter" 1 :id :long)))))
              (.put ^ConcurrentHashMap dbs "db"
                    (assoc (.get ^ConcurrentHashMap dbs "db") :ha-role :follower))
              (is (nil? (#'handlers/server-write-group deps nil "db" store api)))))
          (finally
            (deliver commit-release true)
            (deliver release true)
            (deliver confirmed true)
            (doseq [job @jobs] (deref job 10000 nil))
            (vswap! (i/kv-info store) dissoc :ha-mode)
            (if conn (d/close conn) (d/close-kv store))
            (u/delete-files path)))))))

(deftest successful-renewal-requires-current-covered-lease
  (doseq [patch [#(assoc % :ha-role :follower)
                 #(assoc % :ha-leader-term 2 :ha-authority-term 2)
                 #(assoc % :ha-renew-loop-running? (Object.))]]
    (let [{:keys [deps entered release] :as probe} (authority-probe :ok)
          ^ConcurrentHashMap dbs (:dbs probe)
          publish (ha/ha-write-commit-publish-fn
                    deps nil {:type :tx-data :args ["db"]})
          job (future (try (publish {:txlog-lsn 42}) :ok
                           (catch Throwable t (ex-data t))))]
      (try
        (is (deref entered 10000 false))
        (.put dbs "db" (patch (.get dbs "db")))
        (deliver release true)
        (is (= :ha/write-indeterminate (:error (deref job 10000 nil))))
        (finally (deliver release true) (deref job 10000 nil))))))

(deftest direct-writes-and-client-op-replays-wait-outside-the-writer-slot
  (let [path (u/tmp-dir (str "datalevin-ha-direct-" (random-uuid)))
        store (d/open-kv path {:wal? true :wal-durability-profile :strict
                              :wal-shared? false :wal-segment-prealloc? false
                              :snapshot-bootstrap-force? false})
        slot (Semaphore. 1)
        monitors (atom [])
        {:keys [deps entered release confirmed renews] :as probe}
        (authority-probe
          :ok #(swap! monitors conj [(Thread/holdsLock (l/write-txn store))
                                    (Thread/holdsLock (i/kv-info store))]))
        ^ConcurrentHashMap dbs (:dbs probe)
        deps (assoc deps :get-lock (fn [_ _] slot)
                         :transaction-lock-timeout-ms (constantly 10000)
                         :lmdb (fn [_ _ _ _] store)
                         :update-db (:update-db-fn deps))
        message {:type :transact-kv :args ["db"]
                 :client-op-id "pending" :client-op-hash "hash"
                 :client-op-response-kind cop/command-complete-response-kind}
        record (cop/committed-record :transact-kv "hash"
                                      cop/command-complete-response-kind :transacted)
        replay-entered (promise)
        jobs (atom [])]
    (try
      (d/open-dbi store c/ha-client-ops)
      (binding [cpp/*before-write-commit-fn*
                (ha/ha-write-commit-check-fn deps nil message)
                kvtx/*after-txlog-append-fn*
                (ha/ha-write-commit-publish-fn deps nil message)]
        (swap! jobs conj
               (future
                 (#'handlers/with-direct-db-transaction-slot
                   deps nil "db" false
                   #(d/transact-kv store [(cop/committed-record-tx "pending" record)]))))
        (is (deref entered 10000 false))
        (is (= [[false false]] @monitors))
        (is (= 1 (.availablePermits slot)))
        (is (not (realized? (first @jobs))))
        (let [publish kvtx/*after-txlog-append-fn*]
          (swap! jobs conj
                 (future
                   (binding [kvtx/*after-txlog-append-fn*
                             (fn [ctx]
                               (deliver replay-entered (:txlog-lsn ctx))
                               (publish ctx))]
                     (#'handlers/with-idempotent-client-op
                       deps nil nil "db" false message
                       (fn [_] (throw (AssertionError. "Replay executed twice"))))))))
        (is (= (first @renews) (deref replay-entered 10000 ::timeout)))
        (is (not-any? realized? @jobs))
        (deliver release true)
        (is (= :transacted (deref (first @jobs) 10000 ::timeout)))
        (is (= {:replay? true :response-kind cop/command-complete-response-kind
                :response :transacted}
               (deref (second @jobs) 10000 ::timeout)))
        (is (= 1 (count @renews)))
        (is (= (first @renews)
               (get-in (.get dbs "db") [:ha-authority-lease :leader-last-applied-lsn]))))
      (finally
        (deliver release true)
        (deliver confirmed true)
        (doseq [job @jobs] (deref job 10000 nil))
        (d/close-kv store)
        (u/delete-files path)))))

(deftest waiting-confirmations-coalesce-local-commit-progress
  (let [{:keys [deps entered release confirmed renews]} (authority-probe :ok)
        publish (ha/ha-write-commit-publish-fn
                  deps nil {:type :tx-data :args ["db"]})
        registered (repeatedly 2 promise)
        jobs (atom [(future (publish {:txlog-lsn 42}))])]
    (try
      (is (deref entered 10000 false))
      (doseq [[lsn ready] (map vector [43 44] registered)]
        (swap! jobs conj
               (future
                 (group/with-confirmation
                   #(do (publish {:txlog-lsn lsn}) (deliver ready true)))))
        (is (deref ready 10000 false)))
      (is (not-any? realized? @jobs))
      (deliver confirmed true)
      (deliver release true)
      (doseq [job @jobs] (is (not= ::timeout (deref job 10000 ::timeout))))
      (is (= [42 44] @renews))
      (finally
        (deliver release true)
        (deliver confirmed true)
        (doseq [job @jobs] (deref job 10000 nil))))))

(deftest periodic-renewal-reuses-recent-write-proof-without-extending-deadlines
  (let [{:keys [deps release renews] :as probe} (authority-probe :ok)
        ^ConcurrentHashMap dbs (:dbs probe)
        publish (ha/ha-write-commit-publish-fn deps nil {:type :tx-data :args ["db"]})]
    (deliver release true)
    (publish {:txlog-lsn 42})
    (let [before (.get dbs "db")
          after (dha/ha-renew-step "db" before)]
      (is (= [42] @renews))
      (is (= (select-keys before [:ha-authority-version :ha-last-authority-refresh-ms
                                  :ha-lease-local-deadline-ms :ha-lease-local-deadline-nanos])
             (select-keys after [:ha-authority-version :ha-last-authority-refresh-ms
                                 :ha-lease-local-deadline-ms :ha-lease-local-deadline-nanos]))))))

(deftest waiting-publisher-samples-progress-before-its-next-command
  (let [{:keys [deps entered release confirming confirmed renews] :as probe}
        (authority-probe :ok)
        ^ConcurrentHashMap dbs (:dbs probe)
        publish (ha/ha-write-commit-publish-fn deps nil {:type :tx-data :args ["db"]})
        publication (:ha-renewal-publisher (.get dbs "db"))
        joined (promise)
        jobs (atom [(future (publish {:txlog-lsn 42}))])]
    (try
      (is (deref entered 10000 false))
      (add-watch publication ::joined
                 (fn [_ _ _ new]
                   (when (= 43 (:wanted-lsn new)) (deliver joined true))))
      (swap! jobs conj (future (publish {:txlog-lsn 43})))
      (is (deref joined 10000 false))
      ;; A later physical commit has recorded progress but its confirmation
      ;; callback has not joined the publisher yet.
      (.compute dbs "db"
                (reify BiFunction
                  (apply [_ _ m] (assoc m :ha-leader-last-applied-lsn 44))))
      (deliver release true)
      (is (deref confirming 10000 false))
      (is (= [42 44] @renews))
      (is (not (realized? (second @jobs))))
      (deliver confirmed true)
      (doseq [job @jobs] (is (nil? (deref job 10000 ::timeout))))
      (finally
        (remove-watch publication ::joined)
        (deliver release true)
        (deliver confirmed true)
        (doseq [job @jobs] (deref job 10000 nil))))))

(deftest publication-waiter-timeout-does-not-cancel-the-owner
  (let [{:keys [entered release renews] :as probe} (authority-probe :ok)
        state (.get ^ConcurrentHashMap (:dbs probe) "db")
        owner (future (publisher/renew! state 42 5000))]
    (try
      (is (deref entered 10000 false))
      (is (= :ha/control-timeout
             (try (publisher/renew! state 42 1) nil
                  (catch clojure.lang.ExceptionInfo e (:error (ex-data e))))))
      (is (not (realized? owner)))
      (is (= [42] @renews))
      (deliver release true)
      (is (true? (get-in (deref owner 10000 nil) [:result :ok?])))
      (finally (deliver release true) (deref owner 10000 nil)))))

(deftest late-periodic-result-preserves-newer-proof-and-clock-pause
  (let [{:keys [deps release confirmed renews] :as probe} (authority-probe :ok)
        ^ConcurrentHashMap dbs (:dbs probe)
        before (assoc (.get dbs "db") :ha-leader-last-applied-lsn 42)]
    (.put dbs "db" before)
    (deliver release true)
    (deliver confirmed true)
    (let [periodic (dha/ha-renew-step "db" before)
          publish (ha/ha-write-commit-publish-fn deps nil {:type :tx-data :args ["db"]})]
      (publish {:txlog-lsn 43})
      (let [newer (.get dbs "db")]
        (ha/publish-ha-renew-state!
          deps nil "db" before (assoc periodic :ha-clock-skew-paused? true) nil)
        (let [current (.get dbs "db")]
          (is (= [42 43] @renews))
          (is (= (:ha-authority-lease newer) (:ha-authority-lease current)))
          (is (= (:ha-authority-version newer) (:ha-authority-version current)))
          (is (= (:ha-lease-local-deadline-nanos newer)
                 (:ha-lease-local-deadline-nanos current)))
          (is (true? (:ha-clock-skew-paused? current))))))))

(deftest server-collector-confirms-after-releasing-native-and-server-ownership
  (doseq [outcome [:ok :indeterminate]]
    (let [path (u/tmp-dir (str "ha-collector-" (random-uuid)))
          store (l/open-kv path {:wal? true :snapshot-scheduler? false
                                :wal-segment-prealloc? false})
          slot (Semaphore. 1)
          ownership (atom [])
          {:keys [deps entered release confirmed]}
          (authority-probe outcome
            #(swap! ownership conj [(Thread/holdsLock (l/write-txn store))
                                    (Thread/holdsLock (i/kv-info store))
                                    (.availablePermits slot)]))
          deps (assoc deps :get-lock (fn [_ _] slot)
                           :transaction-lock-timeout-ms (constantly 1000))
          message {:type :update-kv :args ["db"]}
          jobs (atom [])
          second-applied (promise)]
      (try
        (d/open-dbi store "counter")
        (d/transact-kv store "counter" [[:put 1 0]] :id :long)
        (vswap! (i/kv-info store) assoc :ha-mode :consensus-lease)
        (binding [kvtx/*commit-payload-ha-term* 1
                  cpp/*before-write-commit-fn* (ha/ha-write-commit-check-fn deps nil message)
                  kvtx/*after-txlog-append-fn*
                  (let [publish (ha/ha-write-commit-publish-fn deps nil message)]
                    (fn [context]
                      (when (= 2 (d/get-value store "counter" 1 :id :long))
                        (deliver second-applied true))
                      (publish context)))]
          (let [control (#'handlers/server-write-control deps nil "db" store)
                before (:last-committed-lsn (d/txlog-watermarks store))
                submit #(future (try ((:body! control)
                                       (fn [tx] (d/update-kv tx "counter" 1 inc :id :long)) nil)
                                     :ok (catch Throwable t t)))]
            (is (:server? control))
            (swap! jobs conj (submit))
            (is (deref entered 10000 false))
            (is (not (realized? (first @jobs))))
            (is (= 1 (.availablePermits slot)))
            (swap! jobs conj (submit))
            (is (deref second-applied 10000 false))
            (is (= 2 (d/get-value store "counter" 1 :id :long)))
            (let [records (vec (kv/open-tx-log store (inc (long before))))]
              (is (= 2 (count records)))
              (is (= [1 1] (mapv :ha-term records))))
            (is (every? #(= [false false 1] %) @ownership))
            (deliver release true)
            (deliver confirmed true)
            (let [results (mapv #(deref % 10000 ::timeout) @jobs)]
              (is (not-any? #{::timeout} results))
              (if (= outcome :ok)
                (is (= [:ok :ok] results))
                (let [failures (filter #(instance? Throwable %) results)]
                  (is (seq failures))
                  (doseq [failure failures]
                    (is (= :ha/write-indeterminate (:error (ex-data failure))))
                    (is (= :committed (:outcome (ex-data failure))))))))
            (is (batch/serving? (:collector control)))))
        (finally
          (deliver release true)
          (deliver confirmed true)
          (doseq [job @jobs] (deref job 10000 nil))
          (d/close-kv store)
          (u/delete-files path))))))

(deftest server-collector-semaphore-timeout-does-not-fence-runtime
  (let [path (u/tmp-dir (str "server-collector-slot-" (random-uuid)))
        store (l/open-kv path {:wal? true :snapshot-scheduler? false
                              :wal-segment-prealloc? false})
        slot (Semaphore. 1)
        deps {:db-state (fn [_ _] {}) :get-lock (fn [_ _] slot)
              :transaction-lock-timeout-ms (constantly 10)}]
    (try
      (d/open-dbi store "data")
      (let [control (#'handlers/server-write-control deps nil "db" store)]
        (.acquire slot)
        (try
          (is (thrown? Exception ((:body! control)
                                  #(d/transact-kv % "data" [[:put 1 :blocked]]) nil)))
          (finally (.release slot)))
        (is (batch/serving? (:collector control)))
        (is (nil? (d/get-value store "data" 1)))
        ((:body! control) #(d/transact-kv % "data" [[:put 1 :accepted]]) nil)
        (is (= :accepted (d/get-value store "data" 1)))
        ;; Standalone administration owns the same semaphore and must not
        ;; enqueue recursively into a batch waiting to acquire it.
        (#'handlers/with-direct-db-transaction-slot deps nil "db" false
          #(d/transact-kv store c/kv-info [[:put :server/probe true]] :keyword :data))
        (is (= 1 (.availablePermits slot))))
      (finally (d/close-kv store) (u/delete-files path)))))

(deftest server-collector-rechecks-leadership-before-wal-append
  (let [path (u/tmp-dir (str "ha-collector-admission-" (random-uuid)))
        store (l/open-kv path {:wal? true :snapshot-scheduler? false
                              :wal-segment-prealloc? false})
        {:keys [deps dbs release confirmed]} (authority-probe :ok)
        slot (Semaphore. 1)
        deps (assoc deps :get-lock (fn [_ _] slot)
                         :transaction-lock-timeout-ms (constantly 1000))
        message {:type :transact-kv :args ["db"]}]
    (try
      (d/open-dbi store "data")
      (vswap! (i/kv-info store) assoc :ha-mode :consensus-lease)
      (binding [kvtx/*commit-payload-ha-term* 1
                cpp/*before-write-commit-fn* (ha/ha-write-commit-check-fn deps nil message)
                kvtx/*after-txlog-append-fn* (ha/ha-write-commit-publish-fn deps nil message)]
        (let [control (#'handlers/server-write-control deps nil "db" store)
              before (.get ^ConcurrentHashMap dbs "db")
              lsn (:last-committed-lsn (d/txlog-watermarks store))
              failure (try
                        ((:body! control)
                         (fn [tx]
                           (d/transact-kv tx "data" [[:put 1 :rejected]])
                           (.put ^ConcurrentHashMap dbs "db"
                                 (assoc before :ha-leader-term 2 :ha-authority-term 2))) nil)
                        nil (catch Throwable t t))]
          (is (some? failure))
          (is (some #(= :leadership-changed (:reason (ex-data %)))
                    (take-while some? (iterate ex-cause failure))))
          (is (= lsn (:last-committed-lsn (d/txlog-watermarks store))))
          (is (nil? (d/get-value store "data" 1)))
          (is (batch/serving? (:collector control)))
          (is (= 1 (.availablePermits slot)))))
      (finally (deliver release true) (deliver confirmed true)
               (d/close-kv store) (u/delete-files path)))))
