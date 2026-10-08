(ns datalevin.prepared-update-test
  (:require [clojure.test :refer [deftest is testing]]
            [datalevin.client :as client]
            [datalevin.core :as d]
            [datalevin.interpret :refer [inter-fn]]
            [datalevin.prepared :as prepared]
            [datalevin.protocol :as p]
            [datalevin.server :as server]
            [datalevin.tx-group.batch :as batch]
            [datalevin.tx-group.phase :as phase]
            [datalevin.util :as u])
  (:import [datalevin.remote KVStore]
           [java.net ServerSocket]
           [datalevin.client Connection]
           [datalevin.server Server]
           [java.nio.channels SelectionKey SocketChannel]
           [java.util.concurrent ConcurrentHashMap]))

(def ^:dynamic *update-amount* 1)

(deftest large-wal-update-defers-native-application
  (let [dir (u/tmp-dir (str "wal-update-tail-" (random-uuid)))
        db (d/open-kv dir {:wal? true})
        events (atom [])]
    (try
      (d/open-dbi db "values")
      (let [stop (phase/observe! (fn [event _] (swap! events conj event)))
            value (apply str (repeat 8192 \x))]
        (try
          (is (= :transacted
                 (d/update-kv db "values" 1 (constantly value) :long :string)))
          (is (= value (d/get-value db "values" 1 :long :string)))
          (let [^java.util.List trace @events]
            (is (some #{:worker-dispatch} trace))
            (is (< (.indexOf trace :native-dispatch-start)
                   (.indexOf trace :native-tail-start))))
          (finally (stop))))
      (finally (d/close-kv db) (u/delete-files dir)))))

(deftest later-update-sees-deferred-native-write
  (let [dir (u/tmp-dir (str "wal-update-visible-" (random-uuid)))
        db (d/open-kv dir {:wal? true})
        entered (promise)
        ready (promise)
        value (apply str (repeat 8192 \x))]
    (try
      (d/open-dbi db "values")
      (let [stop (phase/observe!
                   (fn [event _]
                     (when (and (= event :ready-published) (realized? entered))
                       (deliver ready true))))]
        (try
          (let [first-update
                (future
                  (d/update-kv db "values" 1
                               (fn [_]
                                 (deliver entered true)
                                 (when (= ::timeout (deref ready 5000 ::timeout))
                                   (throw (ex-info "Second update never became ready" {})))
                                 value)
                               :long :string))]
            (is (= true (deref entered 5000 ::timeout)))
            (let [second-update
                  (future (d/update-kv db "values" 1 #(str % "y") :long :string))]
              (is (= :transacted (deref first-update 10000 ::timeout)))
              (is (= :transacted (deref second-update 10000 ::timeout)))
              (is (= (str value "y") (d/get-value db "values" 1 :long :string)))))
          (finally (stop))))
      (finally (d/close-kv db) (u/delete-files dir)))))

(deftest standalone-wal-update-preserves-function-semantics
  (let [dir (u/tmp-dir (str "wal-update-" (random-uuid)))
        calls (atom 0)]
    (try
      (let [db (d/open-kv dir {:wal? true})]
        (try
          (d/open-dbi db "counts")
          (binding [*update-amount* 3]
            (dotimes [_ 2]
              (is (= :transacted
                     (d/update-kv db "counts" 1
                                  (fn [old extra]
                                    (swap! calls inc)
                                    (+ (or old 0) *update-amount* extra))
                                  :long :long 2)))))
          (is (= 2 @calls))
          (is (= 10 (d/get-value db "counts" 1 :long :long)))
          (is (thrown-with-msg? clojure.lang.ExceptionInfo #"update failed"
                               (d/update-kv db "counts" 1
                                            (fn [_] (throw (ex-info "update failed" {})))
                                            :long :long)))
          (is (= 10 (d/get-value db "counts" 1 :long :long)))
          (is (= :transacted (d/update-kv db "counts" 1 inc :long :long)))
          (finally (d/close-kv db))))
      (let [db (d/open-kv dir {:wal? true})]
        (try (is (= 11 (d/get-value db "counts" 1 :long :long)))
             (finally (d/close-kv db))))
      (finally (u/delete-files dir)))))

(deftest prepared-update-local-explicit-transaction
  (let [dir (u/tmp-dir (str "prepared-update-" (random-uuid)))
        db (d/open-kv dir)]
    (try
      (d/open-dbi db "counts")
      (let [update! (d/prepare-update-kv db "counts" (fnil + 0) :long :long 3)]
        (is (= :transacted (update! 1)))
        (d/with-transaction-kv [tx db]
          (is (= :transacted (d/execute-prepared update! tx 1)))
          (is (= 6 (d/get-value tx "counts" 1 :long :long)))
          (d/abort-transact-kv tx))
        (is (= 3 (d/get-value db "counts" 1 :long :long))))
      (finally (d/close-kv db) (u/delete-files dir)))))

(defn- server-handles [^Server srv db]
  (let [pool (client/get-pool (.-client ^KVStore db))
        ^Connection socket (client/get-connection pool)]
    (try
      (let [address (.getLocalAddress ^SocketChannel (.-ch socket))
            ^ConcurrentHashMap keys (:connection-keys (.-execution srv))]
        (some (fn [^SelectionKey key]
                (when (= address (.getRemoteAddress ^SocketChannel (.channel key)))
                  (into {} (:prepared-handles @(.attachment key)))))
              (.values keys)))
      (finally (client/release-connection pool socket)))))

(deftest prepared-update-remote-reuses-function-and-recovers-handles
  (let [root (u/tmp-dir (str "prepared-update-server-" (random-uuid)))
        port (with-open [socket (ServerSocket. 0)] (.getLocalPort socket))
        srv (server/create {:root root :port port})]
    (try
      (server/start srv)
      (let [db (d/open-kv (str "dtlv://datalevin:datalevin@localhost:" port "/kv")
                          {:wal? true :client-opts {:pool-size 1}})
            pool (client/get-pool (.-client ^KVStore db))
            on-socket (fn [action]
                        (let [socket (client/get-connection pool)]
                          (try (action socket)
                               (finally (client/release-connection pool socket)))))]
        (try
          (d/open-dbi db "counts")
          (let [update! (d/prepare-update-kv
                          db "counts" (inter-fn [old amount] (+ (or old 0) amount))
                          :long :long 2)]
            (let [specialized? (atom false)
                  stop (phase/observe!
                         (fn [event b]
                           (when (= event :resolution-start)
                             (when (:kv-update? (batch/context (batch/batch-at b 0)))
                               (reset! specialized? true)))))]
              (try
                (is (= :transacted (update! 1)))
                (is @specialized?)
                (finally (stop))))
            (let [[id entry] (first (server-handles srv db))]
              (is (some? entry))
              (dotimes [_ 2] (is (= :transacted (d/execute-prepared update! 1))))
              (is (= 6 (d/get-value db "counts" 1 :long :long)))
              (is (identical? entry (get (server-handles srv db) id)))
              (testing "eviction re-registers without losing or duplicating an update"
                (dotimes [_ prepared/max-handles]
                  ((d/prepare-get-value db "counts" :long :long) 1))
                (is (nil? (get (server-handles srv db) id)))
                (is (= :transacted (update! 1)))
                (is (= 8 (d/get-value db "counts" 1 :long :long)))
                (is (some? (get (server-handles srv db) id)))
                (is (not (identical? entry (get (server-handles srv db) id)))))
              (testing "replacement sockets register the function again"
                (on-socket client/close)
                (is (= :transacted (update! 2)))
                (is (= 2 (d/get-value db "counts" 2 :long :long)))
                (is (= #{id} (set (keys (server-handles srv db))))))
              (testing "legacy capability fallback retains update semantics"
                (on-socket
                  #(#'client/set-conn-wire-opts!
                     % (dissoc (p/negotiate-wire-opts (p/local-wire-capabilities))
                               :prepared-update?)))
                (dotimes [_ 2] (is (= :transacted (update! 2))))
                (is (= 6 (d/get-value db "counts" 2 :long :long)))))
            (d/with-transaction-kv [tx db]
              (let [update-tx! (d/prepare-update-kv
                                tx "counts" (inter-fn [old] (inc old)) :long :long)]
                (dotimes [_ 2] (is (= :transacted (update-tx! 1))))
                (is (= 10 (d/get-value tx "counts" 1 :long :long)))
                (d/abort-transact-kv tx)))
            (is (= 8 (d/get-value db "counts" 1 :long :long)))
            (d/close-kv db)
            (is (thrown? Exception (update! 1))))
          (finally (d/close-kv db))))
      (finally (server/stop srv) (u/delete-files root)))))
