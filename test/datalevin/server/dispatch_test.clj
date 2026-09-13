(ns datalevin.server.dispatch-test
  (:require
   [clojure.test :refer [deftest is testing]]
   [datalevin.binding.cpp :as cpp]
   [datalevin.command :as cmd]
   [datalevin.constants :as c]
   [datalevin.ha :as ha]
   [datalevin.kv.txlog :as kvtx]
   [datalevin.protocol :as p]
   [datalevin.server.dispatch :as dispatch]
   [datalevin.server.ha :as sha]
   [datalevin.server.handlers :as handlers]
   [datalevin.txlog :as txlog])
  (:import
   [java.nio ByteBuffer]
   [java.nio.channels SelectionKey SocketChannel]
   [java.nio.channels.spi SelectorProvider]
   [java.util.concurrent ConcurrentHashMap]))

;; An independent inventory of mutations: changing a property to read-only
;; must not silently remove a handler from the guard tests below.
(def ^:private database-writes
  #{:set-schema :register-type :datalog-register-type
    :swap-attr :del-attr :rename-attr :load-datoms :tx-data :tx-data+db-info
    :open-transact :close-transact :abort-transact
    :open-transact-kv :close-transact-kv :abort-transact-kv :transact-kv
    :open-dbi :clear-dbi :drop-dbi :set-env-flags :analyze
    :add-doc :remove-doc :clear-docs :search-re-index
    :add-vec :remove-vec :persist-vecs :close-vecs :clear-vecs :vec-re-index
    :kv-re-index :datalog-re-index})

(def ^:private local-writes
  #{:drop-database :assoc-opt :assoc-opts :sync :ha-update-membership!
    :force-txlog-sync! :force-lmdb-sync! :create-snapshot! :gc-txlog-segments!
    :txlog-update-snapshot-floor! :txlog-clear-snapshot-floor!
    :txlog-update-replica-floor! :txlog-clear-replica-floor!
    :txlog-pin-backup-floor! :txlog-unpin-backup-floor!})

(deftest every-handler-has-command-properties-test
  (is (= (set (keys handlers/handler-map)) (set (keys cmd/properties)))
      "adding a wire handler requires an explicit command classification")
  (doseq [[type properties] cmd/properties]
    (testing (str type)
      (is (boolean? (:ha-write? properties)))
      (is (boolean? (:replica-write? properties)))))
  (is (= database-writes
         (set (filter cmd/ha-write? (keys handlers/handler-map)))))
  (is (= (into database-writes local-writes)
         (set (filter cmd/replica-write? (keys handlers/handler-map)))))
  (doseq [type (keys handlers/handler-map)]
    (is (= (contains? database-writes type)
           (ha/ha-write-message? {:type type})) (str type))))

(defn- leader-state []
  {:ha-authority ::authority
   :ha-role :leader
   :ha-node-id 1
   :ha-authority-owner-node-id 1
   :ha-leader-term 7
   :ha-authority-term 7
   :ha-authority-read-ok? true
   :ha-last-authority-refresh-ms (System/currentTimeMillis)
   :ha-lease-renew-ms 60000
   :ha-lease-timeout-ms 600000
   :ha-lease-until-ms (+ (System/currentTimeMillis) 600000)
   :ha-lease-local-deadline-ms (+ (System/currentTimeMillis) 600000)
   :ha-members [{:node-id 1 :endpoint "127.0.0.1:19001"}]
   :ha-authority-lease {:leader-endpoint "127.0.0.1:19001"}})

(defn- dispatch-probe
  "Run the real wire decoder, dispatch, and HA admission with inert handlers
  and an in-memory response channel. No stores or network services are needed."
  ([db-state message]
   (dispatch-probe db-state message nil))
  ([db-state message before-commit]
   (let [events    (atom [])
         response  (ByteBuffer/allocate 65536)
         channel   (proxy [SocketChannel] [(SelectorProvider/provider)]
                     (write [^ByteBuffer src]
                       (let [n (.remaining src)]
                         (.put response src)
                         n)))
         skey      (doto (proxy [SelectionKey] []
                           (channel [] channel))
                     (.attach (volatile! {:message-lock (Object.)
                                          :write-bf (ByteBuffer/allocate 65536)})))
         dbs       (doto (ConcurrentHashMap.) (.put "db" db-state))
         ha-deps   {:dbs-fn (constantly dbs)
                    :update-db-fn (fn [_ db-name f]
                                    (let [m (f (.get dbs db-name))]
                                      (.put dbs db-name m)
                                      m))
                    :ensure-udf-readiness-state-fn identity
                    :udf-admission-exempt-write-types #{}
                    :udf-write-admission-error-fn (fn [_ _] nil)}
         deps
         {:dbs-fn (constantly dbs)
          :with-db-runtime-read-access-fn
          (fn [_ _ f]
            (swap! events conj :runtime-enter)
            (try (f) (finally (swap! events conj :runtime-exit))))
          :with-ha-write-admission-fn
          (fn [server message f]
            (swap! events conj :admission-enter)
            (try
              (sha/with-ha-write-admission ha-deps server message f)
              (finally (swap! events conj :admission-exit))))
          :ha-write-commit-check-fn-fn
          (fn [server message]
            (let [check (sha/ha-write-commit-check-fn ha-deps server message)]
              (fn [txn]
                (swap! events conj :commit-check)
                (check txn))))
          :ha-write-commit-publish-fn-fn
          (fn [_ _] (fn [& _] (swap! events conj :commit-publish)))
          :cleanup-rejected-close-transact!-fn
          (fn [_ _] (swap! events conj :rejected-cleanup))
          :message-handler-map
          (into {}
                (map (fn [type]
                       [type
                        (fn [_ _ _]
                          (swap! events conj [:handler type
                                              txlog/*commit-payload-ha-term*])
                          (when before-commit (before-commit dbs))
                          (when cpp/*before-write-commit-fn*
                            (cpp/*before-write-commit-fn* nil))
                          (when kvtx/*after-txlog-append-fn*
                            (kvtx/*after-txlog-append-fn* nil)))]))
                (keys handlers/handler-map))}]
     (binding [cpp/*before-write-commit-fn* nil
               kvtx/*after-txlog-append-fn* nil
               txlog/*commit-payload-ha-term* nil]
       (dispatch/handle-message deps ::server skey c/message-format-transit
                                (p/write-transit-bytes message)))
     {:events @events
      :response (when (pos? (.position response))
                  (first (p/receive-one-message response)))})))

(deftest follower-rejects-every-database-write-before-handler-test
  (doseq [type database-writes
          :when (not (contains? #{:abort-transact :abort-transact-kv} type))]
    (testing (str type)
      (let [{:keys [events response]}
            (dispatch-probe (assoc (leader-state) :ha-role :follower)
                            {:type type :args ["db"]})]
        (is (= [:runtime-enter :admission-enter :admission-exit
                :rejected-cleanup :runtime-exit] events))
        (is (= :ha/write-rejected (get-in response [:err-data :error])))
        (is (= :not-leader (get-in response [:err-data :reason])))))))

(deftest leader-binds-commit-guards-for-every-database-write-test
  (doseq [type database-writes
          :when (not (contains? #{:abort-transact :abort-transact-kv
                                 :open-transact :open-transact-kv} type))]
    (testing (str type)
      (let [{:keys [events response]}
            (dispatch-probe (leader-state) {:type type :args ["db"]})]
        (is (nil? response))
        (is (= [:runtime-enter :admission-enter [:handler type 7]
                :commit-check :commit-publish :admission-exit :runtime-exit]
               events))))))

(deftest re-index-commit-guards-recheck-leadership-test
  (doseq [type [:search-re-index :vec-re-index]]
    (testing (str type)
      (let [{:keys [events response]}
            (dispatch-probe
             (leader-state) {:type type :args ["db"]}
             (fn [^ConcurrentHashMap dbs]
               (.put dbs "db" (assoc (.get dbs "db") :ha-role :follower))))]
        (is (= [:runtime-enter :admission-enter [:handler type 7]
                :commit-check :admission-exit :runtime-exit] events))
        (is (= :ha/write-rejected (get-in response [:err-data :error])))
        (is (= :not-leader (get-in response [:err-data :reason])))))))

(deftest replicas-reject-every-mutating-handler-before-dispatch-test
  (doseq [type (into database-writes local-writes)
          writing? [false true]]
    (testing (str type " writing?=" writing?)
      (let [{:keys [events response]}
            (dispatch-probe {:replica/read-only? true}
                            {:type type :args ["db"] :writing? writing?})]
        (is (empty? events) "replica rejection must precede every dispatch path")
        (is (= :replica/read-only (get-in response [:err-data :error])))
        (is (= type (get-in response [:err-data :type])))))))

(deftest transaction-control-guard-exceptions-test
  (testing "abort still cleans up after leadership is lost"
    (doseq [type [:abort-transact :abort-transact-kv]]
      (let [{:keys [events response]}
            (dispatch-probe (assoc (leader-state) :ha-role :follower)
                            {:type type :args ["db"]})]
        (is (nil? response))
        (is (= [:runtime-enter [:handler type nil] :runtime-exit] events)))))
  (testing "open checks admission before starting the transaction runner"
    (doseq [type [:open-transact :open-transact-kv]]
      (let [{:keys [events response]}
            (dispatch-probe (leader-state) {:type type :args ["db"]})]
        (is (nil? response))
        (is (= [:runtime-enter :admission-enter :admission-exit
                [:handler type nil] :runtime-exit] events))))))

(deftest reads-and-local-administration-do-not-get-ha-commit-guards-test
  (doseq [state [(assoc (leader-state) :ha-role :follower)
                {:replica/read-only? true}]
          type [:search :search-vec :doc-count :vecs-info :get-value]]
    (testing (str type " " (:ha-role state))
      (let [{:keys [events response]}
            (dispatch-probe state {:type type :args ["db"]})]
        (is (nil? response))
        (is (= [:runtime-enter :admission-enter [:handler type nil]
                :admission-exit :runtime-exit] events)))))
  (doseq [type local-writes]
    (let [{:keys [events response]}
          (dispatch-probe (assoc (leader-state) :ha-role :follower)
                          {:type type :args ["db"]})]
      (is (nil? response) (str type))
      (is (= [:runtime-enter :admission-enter [:handler type nil]
              :admission-exit :runtime-exit] events) (str type)))))
