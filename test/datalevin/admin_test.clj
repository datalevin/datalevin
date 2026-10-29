(ns datalevin.admin-test
  (:require [clojure.edn :as edn]
            [clojure.string :as str]
            [clojure.test :refer [deftest is testing]]
            [datalevin.admin :as admin]
            [datalevin.client :as client]
            [datalevin.command :as command]
            [datalevin.main :as main]))

(defn- status [name id]
  {:db-name name :db-id id :db-type :key-value :open? false
   :stat nil :replica nil :watermarks nil :admin-capabilities #{}})

(defn- run-admin [options request & [overrides]]
  (let [results (atom []) diagnostics (atom []) calls (atom []) closed (atom 0)
        dependencies (merge {:getenv (constantly "dtlv://user:secret@localhost")
                             :connect (fn [_ opts] (swap! calls conj [:connect opts]) :connection)
                             :disconnect (fn [_] (swap! closed inc))
                             :request (fn [_ op args]
                                        (swap! calls conj [op args])
                                        (request op args))
                             :emit #(swap! results conj %)
                             :diagnostic #(swap! diagnostics conj %)} overrides)
        exit (admin/run! options ["status"] dependencies)]
    {:exit exit :results @results :diagnostics @diagnostics :calls @calls :closed @closed}))

(deftest admin-command-line-validation
  (let [parsed (main/validate-args ["admin" "status" "--database" "Orders_DB"
                                    "--database" "users" "--database" "orders-db"
                                    "--timeout-ms" "1250"])]
    (is (= "admin" (:command parsed)))
    (is (= ["orders-db" "users"] (get-in parsed [:options :database])))
    (is (= 1250 (get-in parsed [:options :timeout-ms]))))
  (doseq [args [["admin"] ["admin" "status"]
                ["admin" "status" "--database" "one" "--all-databases"]
                ["admin" "status" "--database" " "]
                ["admin" "status" "--database" "one" "--timeout-ms" "0"]
                ["admin" "status" "--database" "one" "--timeout-ms" "no"]
                ["admin" "backup" "--database" "one" "dest"]
                ["admin" "status" "--all"]]]
    (testing (pr-str args)
      (is (= 2 (:exit-code (main/validate-args args))))))
  (is (:ok? (main/validate-args ["admin" "--help"])))
  (is (command/read-only? :admin-status))
  (is (not (command/ha-write? :admin-status))))

(deftest missing-uri-and-connect-failures-do-not-expose-secrets
  (doseq [uri [nil "" "  "]]
    (let [result (run-admin {:database ["one"]} (fn [& _] nil)
                            {:getenv (constantly uri)})]
      (is (= 2 (:exit result)))
      (is (empty? (:calls result)))
      (is (empty? (:results result)))))
  (let [result (run-admin {:database ["one"]} (fn [& _] nil)
                          {:connect (fn [& _]
                                      (throw (ex-info "dtlv://user:secret@localhost" {})))})]
    (is (= 2 (:exit result)))
    (is (not (str/includes? (pr-str result) "secret")))))

(deftest named-status-does-not-enumerate-and-continues-target-failures
  (let [result (run-admin {:database ["missing" "good"] :timeout-ms 25}
                          (fn [op [name]]
                            (is (= :admin-status op))
                            (if (= name "missing")
                              (throw (ex-info "No database" {:error :admin/database-not-found}))
                              (status name 2))))]
    (is (= 1 (:exit result)))
    (is (= ["missing" "good"] (mapv :db-name (:results result))))
    (is (get-in result [:results 0 :error]))
    (is (= 2 (get-in result [:results 1 :result :db-id])))
    (is (= [:connect {:time-out 25}] (first (:calls result))))
    (is (= 1 (:closed result)))))

(deftest transport-and-unsupported-servers-stop-the-sweep
  (doseq [error [(java.io.EOFException. "EOF")
                 (ex-info "Authentication failure" {})
                 (ex-info "Unknown message type :admin-status" {})]]
    (let [result (run-admin {:database ["first" "later"]}
                            (fn [& _] (throw error)))]
      (is (= 1 (:exit result)))
      (is (= ["first"] (mapv :db-name (:results result))))
      (is (= 1 (:closed result))))))

(deftest global-status-compares-catalog-identities
  (let [result (run-admin {:all-databases true}
                          (fn [op args]
                            (case op
                              :list-databases ["missing" "replaced" "good"]
                              :list-databases-in-use []
                              :query-system [["replaced" :key-value 1] ["good" :key-value 3]]
                              :admin-status (status (first args)
                                                    (if (= "replaced" (first args)) 2 3)))))]
    (is (= 1 (:exit result)))
    (is (= ["good" "missing" "replaced"] (mapv :db-name (:results result))))
    (is (= :admin/database-not-found (get-in result [:results 1 :error :data :error])))
    (is (= :admin/identity-mismatch (get-in result [:results 2 :error :data :error])))
    (is (= 1 (count (filter #(= :query-system (first %)) (:calls result)))))))

(deftest output-is-edn-and-removes-paths-and-uri-credentials
  (let [value {:watermarks {:dir "/private/server/txlog" :last-applied-lsn 7}
               :replica {:replica/source "dtlv://other:p%40ss@host/orders"}
               :nested [{:path "/private/server/copy-temp" :ok? true}]
               :error (ex-info "Could not read /private/server/data.mdb"
                               {:uri "dtlv://user:secret@localhost"})}
        clean (admin/sanitize "dtlv://user:secret@localhost" value)
        text (pr-str clean)]
    (is (= clean (edn/read-string text)))
    (is (= 7 (get-in clean [:watermarks :last-applied-lsn])))
    (is (= "dtlv://[redacted]@host/orders" (get-in clean [:replica :replica/source])))
    (is (not (str/includes? text "/private/server")))
    (is (not (str/includes? text "secret")))
    (is (not (str/includes? text "p%40ss")))))

(deftest admin-client-read-does-not-follow-ha-routing
  (let [requests (atom [])
        transport (reify client/IClient
                    (request [_ req]
                      (swap! requests conj req)
                      {:type :command-complete :result (status "orders-db" 1)}))]
    (is (= 1 (:db-id (client/admin-status transport "Orders_DB" {:runtime? false}))))
    (is (= [{:type :admin-status :args ["orders-db" {:runtime? false}] :writing? false}]
           @requests))))
