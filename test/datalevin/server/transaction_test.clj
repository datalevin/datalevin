(ns datalevin.server.transaction-test
  (:require
   [clojure.test :refer [deftest is testing use-fixtures]]
   [datalevin.client :as client]
   [datalevin.server :as server]
   [datalevin.test.core :refer [allocate-port db-fixture]]
   [datalevin.util :as u])
  (:import
   [datalevin.server Server]
   [java.util UUID]
   [java.util.concurrent Semaphore]))

(use-fixtures :once db-fixture)

(def ^:private transaction-types
  [["kv" :open-transact-kv :close-transact-kv :abort-transact-kv]
   ["datalog" :open-transact :close-transact :abort-transact]])

(defn- with-server-clients [f]
  (let [root (u/tmp-dir (str "transaction-permissions-" (UUID/randomUUID)))
        port (allocate-port)
        srv  (server/create {:root root :port port})
        uri  #(str "dtlv://" % "@localhost:" port)
        opts {:pool-size 1 :time-out 5000}]
    (try
      (server/start srv)
      (let [admin (client/new-client (uri "datalevin:datalevin") opts)]
        (try
          (client/create-user admin "writer" "secret")
          (client/create-role admin :writer)
          (client/assign-role admin :writer "writer")
          (client/grant-permission admin :writer :datalevin.server/view
                                   :datalevin.server/database nil)
          (let [writer (client/new-client (uri "writer:secret") opts)]
            (try (f srv admin writer)
                 (finally (client/disconnect writer))))
          (finally (client/disconnect admin))))
      (finally
        (server/stop srv)
        (u/delete-files root)))))

(defn- request! [client message]
  (let [response (client/request client message)]
    (is (= :command-complete (:type response)) (pr-str response))
    (:result response)))

(defn- control-message [db-name type wire]
  (merge {:type type :args [db-name]} wire))

(defn- prepare-db! [admin writer db-name db-type]
  (client/open-database admin db-name db-type)
  (client/grant-permission admin :writer :datalevin.server/alter
                           :datalevin.server/database db-name)
  (client/open-database writer db-name db-type)
  (when (= db-type "kv")
    (client/normal-request admin :open-dbi [db-name "data"])))

(defn- put-value! [client db-name db-type value writing?]
  (request! client
            (merge {:mode :request :writing? writing?}
                   (if (= db-type "kv")
                     {:type :transact-kv
                      :args [db-name nil [[:put "data" :key value]]]}
                     {:type :tx-data
                      :args [db-name [{:db/id 1 :value value}] false]}))))

(defn- read-value [client db-name db-type]
  (if (= db-type "kv")
    (client/normal-request client :get-value
                           [db-name "data" :key :data :data true])
    (client/normal-request client :q
                           [db-name '[:find ?v . :where [1 :value ?v]]
                            [:remote-db-placeholder]])))

(defn- assert-released [^Server srv db-name]
  (let [state (get (.-dbs srv) db-name)]
    (is (not-any? #(contains? state %)
                  [:runner :runner-skey :wlmdb :wstore :wdt-db]))
    (is (= 1 (.availablePermits ^Semaphore (:lock state))))))

(deftest revoked-permission-does-not-strand-transaction-test
  (with-server-clients
    (fn [srv admin writer]
      (doseq [[db-type open close abort] transaction-types
              end [close abort]
              wire [{:writing? true} {:writing? false} {}]]
        (testing (str db-type " " end " " wire)
          (let [db-name (str "revoked-" (UUID/randomUUID))]
            (prepare-db! admin writer db-name db-type)
            (put-value! admin db-name db-type :before false)
            (request! writer (control-message db-name open {}))
            (put-value! writer db-name db-type :discarded true)
            (client/revoke-permission admin :writer :datalevin.server/alter
                                      :datalevin.server/database db-name)
            (let [response (client/request
                             writer (control-message db-name end wire))]
              (if (= end close)
                (do
                  (is (= :error-response (:type response)))
                  (is (re-find #"Don't have permission to alter"
                               (:message response))))
                (is (= :command-complete (:type response)))))
            (assert-released srv db-name)
            (is (= :before (read-value admin db-name db-type)))
            ;; Cleanup must let another writer finish without disconnecting
            ;; or restoring the original owner's permission.
            (request! admin (control-message db-name open {}))
            (put-value! admin db-name db-type :after true)
            (request! admin (control-message db-name close wire))
            (assert-released srv db-name)
            (is (= :after (read-value admin db-name db-type)))))))))

(deftest transaction-controls-always-check-owner-test
  (with-server-clients
    (fn [^Server srv admin writer]
      (doseq [[db-type open close abort] transaction-types]
        (let [db-name (str "owner-" (UUID/randomUUID))]
          (prepare-db! admin writer db-name db-type)
          (request! writer (control-message db-name open {}))
          (put-value! writer db-name db-type :committed true)
          (let [state (get (.-dbs srv) db-name)]
            (doseq [end [close abort]
                    wire [{:writing? true} {:writing? false} {}]]
              (testing (str end " " wire)
                (let [response (client/request
                                 admin (control-message db-name end wire))]
                  (is (= :error-response (:type response)))
                  (is (= :transaction-owner-mismatch
                         (get-in response [:err-data :reason])))
                  (is (identical? (:runner state)
                                  (:runner (get (.-dbs srv) db-name))))
                  (is (zero? (.availablePermits ^Semaphore (:lock state))))))))
          (request! writer (control-message db-name close {}))
          (assert-released srv db-name)
          (is (= :committed (read-value admin db-name db-type))))))))

(deftest abort-and-wrapper-close-remain-idempotent-test
  (with-server-clients
    (fn [srv admin writer]
      (doseq [[db-type open close abort] transaction-types
              wire [{:writing? true} {:writing? false} {}]]
        (let [db-name (str "abort-" (UUID/randomUUID))]
          (prepare-db! admin writer db-name db-type)
          (request! writer (control-message db-name open {}))
          (put-value! writer db-name db-type :discarded true)
          (client/revoke-permission admin :writer :datalevin.server/alter
                                    :datalevin.server/database db-name)
          (request! writer (control-message db-name abort wire))
          (request! writer (control-message db-name abort wire))
          (request! writer (control-message db-name close wire))
          (assert-released srv db-name)
          (is (nil? (read-value admin db-name db-type)))
          (is (= :missing-transaction
                 (get-in (client/request
                           writer (control-message db-name close wire))
                         [:err-data :reason])))
          (assert-released srv db-name))))))
