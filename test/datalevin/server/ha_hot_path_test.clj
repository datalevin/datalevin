(ns datalevin.server.ha-hot-path-test
  (:require
   [clojure.test :refer [deftest is testing use-fixtures]]
   [datalevin.binding.cpp :as cpp]
   [datalevin.core :as d]
   [datalevin.db :as db]
   [datalevin.ha :as dha]
   [datalevin.ha.control :as ctrl]
   [datalevin.interface :as i]
   [datalevin.kv :as kv]
   [datalevin.kv.txlog :as kvtx]
   [datalevin.lmdb :as l]
   [datalevin.server.ha :as ha]
   [datalevin.server.handlers :as handlers]
   [datalevin.test.core :refer [db-fixture]]
   [datalevin.tx-group :as group]
   [datalevin.util :as u])
  (:import
   [datalevin.tx_group Group]
   [java.util.concurrent ConcurrentHashMap ConcurrentLinkedQueue]))

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

(defn- authority-probe [outcome]
  (let [now (System/currentTimeMillis)
        lease (atom {:db-identity "db" :leader-node-id 1 :term 1
                     :leader-endpoint "127.0.0.1:19001"
                     :lease-until-ms (+ now 600000)
                     :leader-last-applied-lsn 0})
        reads (atom 0)
        renews (atom [])
        entered (promise) release (promise)
        confirming (promise) confirmed (promise)
        authority
        (reify ctrl/ILeaseAuthority
          (read-lease [_ _]
            (swap! reads inc)
            {:lease @lease :version 1})
          (read-membership-hash [_] nil)
          (read-voters [_] [])
          (renew-lease [_ req]
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
                (swap! lease assoc :leader-last-applied-lsn
                       (:leader-last-applied-lsn req)))
              {:ok? ok? :reason (when-not ok? :timeout)
               :lease @lease :version 1
               :authority-now-ms (System/currentTimeMillis)})))
        state {:ha-authority authority :ha-role :leader :ha-node-id 1
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
                (let [state (.get dbs name)]
                  (when (pred state) (.put dbs name (f state))))
                {:state (.get dbs name)})}]
    {:deps deps :dbs dbs :reads reads :renews renews
     :entered entered :release release
     :confirming confirming :confirmed confirmed}))

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
    (is (= 1 @locks))
    ;; Local progress alone cannot justify skipping authority publication.
    (.put dbs "db" (assoc (.get dbs "db") :ha-leader-last-applied-lsn 100))
    (is (nil? (publish {:txlog-lsn 43})))
    (is (= [42 100] @renews))
    (is (= 2 @locks))
    (is (zero? @reads))))

(deftest confirmation-is-rechecked-after-waiting-for-admission-lock
  (let [{:keys [deps reads renews release] :as probe} (authority-probe :ok)
        ^ConcurrentHashMap dbs (:dbs probe)
        state (assoc (.get dbs "db") :ha-leader-last-applied-lsn 42)
        gate (Object.)
        waiting (promise)
        deps (assoc deps :db-write-admission-lock-fn
                    (fn [_ _] (deliver waiting true) gate))]
    (.put dbs "db" state)
    (deliver release true)
    (let [publish (ha/ha-write-commit-publish-fn
                    deps nil {:type :tx-data :args ["db"]})
          job (locking gate
                (let [job (future (publish {:txlog-lsn 42}))]
                  (is (deref waiting 10000 false))
                  ;; The periodic path publishes while the foreground caller
                  ;; waits. Only that background renewal should reach authority.
                  (let [next-state (dha/ha-renew-step "db" state)]
                    (ha/publish-ha-renew-state!
                      deps nil "db" state next-state nil))
                  job))]
      (is (nil? (deref job 10000 ::timeout)))
      (is (= [42] @renews))
      (is (zero? @reads)))))

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

(deftest ha-groups-share-authority-checks-and-confirm-before-completion
  (doseq [api [:kv :datalog]
          outcome [:ok :rejected :indeterminate]]
    (testing (str api " " outcome)
      (let [path (u/tmp-dir (str "datalevin-ha-group-" (random-uuid)))
            opts {:wal? true :wal-durability-profile :strict :wal-shared? false
                  :wal-segment-prealloc? false :snapshot-bootstrap-force? false}
            conn (when (= api :datalog)
                   (d/create-conn path {:counter {:db/valueType :db.type/long}}
                                  opts))
            store (if conn (d/datalog-kv conn) (d/open-kv path opts))
            {:keys [deps dbs reads renews entered release confirming confirmed]}
            (authority-probe outcome)
            message {:type (if conn :tx-data :update-kv) :args ["db"]}
            jobs (atom [])]
        (try
          (if conn
            (d/transact! conn [{:db/id 1 :counter 0}])
            (do (d/open-dbi store "counter")
                (d/transact-kv store [[:put "counter" 1 0 :id :long]])))
          ;; Install runtime HA after store initialization, without starting a
          ;; background authority loop. The real server commit guards run below.
          (vswap! (i/kv-info store) assoc :ha-mode :consensus-lease)
          (is (nil? (kv/write-group store :kv)))
          (is (nil? (#'handlers/server-write-group
                      deps nil "db" store api)))
          (binding [cpp/*before-write-commit-fn*
                    (ha/ha-write-commit-check-fn deps nil message)
                    kvtx/*after-txlog-append-fn*
                    (let [publish (ha/ha-write-commit-publish-fn deps nil message)]
                      (fn [context]
                        (publish context)
                        (when (= outcome :rejected)
                          (.put ^ConcurrentHashMap dbs "db"
                                (assoc (.get ^ConcurrentHashMap dbs "db")
                                       :ha-role :follower)))))]
            (let [g (#'handlers/server-write-group deps nil "db" store api)
                  before (long (:last-committed-lsn (d/txlog-watermarks store)))
                  run-tx (if conn
                           (fn [execute]
                             (d/with-transaction [tx conn]
                               (if group/*batched?*
                                 (:result (db/execute-write-group tx execute))
                                 (execute tx))))
                           (fn [execute]
                             (l/with-transaction-kv [tx store] (execute tx))))
                  op (if conn
                       (fn [tx]
                         (d/transact! tx [[:db/add 1 :counter
                                          (inc (long (:counter (d/pull @tx [:counter] 1))))]]))
                       #(d/update-kv % "counter" 1 inc :id :long))
                  submit #(future
                            (try (group/submit! g run-tx op) :ok
                                 (catch Throwable t (ex-data t))))]
              (is (some? g))
              (swap! jobs conj (submit))
              (is (deref entered 10000 false))
              (dotimes [idx 3]
                (swap! jobs conj (submit))
                (is (await-queued! g (inc idx))))
              (is (not-any? realized? @jobs))
              (deliver release true)
              (when-not (= outcome :rejected)
                (is (deref confirming 10000 false))
                (is (not-any? realized? (rest @jobs)))
                (deliver confirmed true))
              (let [results (mapv #(deref % 10000 ::timeout) @jobs)
                    marks (d/txlog-watermarks store)
                    rejected? (= outcome :rejected)]
                (is (= :ok (first results)))
                (is (zero? @reads))
                (is (= (if rejected? 1 2) (count @renews)))
                (is (= (+ before (if rejected? 1 2)) (:last-committed-lsn marks)))
                (is (= (:last-committed-lsn marks) (:last-durable-lsn marks)))
                (is (= (if rejected? 1 4)
                       (if conn (:counter (d/pull @conn [:counter] 1))
                           (d/get-value store "counter" 1 :id :long))))
                (if (= outcome :ok)
                  (is (every? #{:ok} results))
                  (is (every? #(= (if rejected? :ha/write-rejected
                                                  :ha/write-indeterminate)
                                  (:error %))
                              (rest results)))))
              (.put ^ConcurrentHashMap dbs "db"
                    (assoc (.get ^ConcurrentHashMap dbs "db") :ha-role :follower))
              (is (nil? (#'handlers/server-write-group deps nil "db" store api)))))
          (finally
            (deliver release true)
            (deliver confirmed true)
            (doseq [job @jobs] (deref job 10000 nil))
            (vswap! (i/kv-info store) dissoc :ha-mode)
            (if conn (d/close conn) (d/close-kv store))
            (u/delete-files path)))))))
