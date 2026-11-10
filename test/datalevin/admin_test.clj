(ns datalevin.admin-test
  (:require [clojure.edn :as edn]
            [clojure.string :as str]
            [clojure.test :refer [deftest is testing]]
            [datalevin.admin :as admin]
            [datalevin.client :as client]
            [datalevin.command :as command]
            [datalevin.main :as main]
            [datalevin.util :as u]))

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
                ["admin" "status" "--database" "one" "--dry-run"]
                ["--dry-run" "--dir" "/tmp/example" "drop" "values"]
                ["--dry-run" "--dir" "/tmp/example" "copy" "/tmp/destination"]
                ["admin" "backup" "--database" "one"]
                ["admin" "status" "--all"]]]
    (testing (pr-str args)
      (is (= 2 (:exit-code (main/validate-args args))))))
  (is (:ok? (main/validate-args ["admin" "--help"])))
  (is (= ["backup" "dest"]
         (:arguments (main/validate-args ["admin" "backup" "--database" "one" "dest"]))))
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

(deftest admin-validation-normalizes-names-before-deduplication
  (doseq [arguments [["status"] ["wal-gc"] ["backup" "destination"]]]
    (let [options {:database ["Orders_DB" "users" "orders-db" "ORDERS-DB" "Users"]}
          validated (admin/validate-options options arguments)]
      (is (= ["orders-db" "users"] (get-in validated [:options :database])))
      (is (= validated (admin/validate-options (:options validated) arguments)))))
  (doseq [names [[""] [" "] [nil]]]
    (is (= 2 (:exit-code (admin/validate-options {:database names} ["status"]))))))

(deftest programmatic-status-normalizes-deduplicates-and-preserves-target-order
  (let [result (run-admin {:database ["Orders_DB" "users" "orders-db" "ORDERS-DB" "Users"]}
                          (fn [_ [name]] (status (u/lisp-case name) 1)))
        requests (filter #(= :admin-status (first %)) (:calls result))]
    (is (= 0 (:exit result)))
    (is (= [["orders-db" {:runtime? true}] ["users" {:runtime? true}]]
           (mapv second requests)))
    (is (= ["orders-db" "users"] (mapv :db-name (:results result))))
    (is (every? :result (:results result)))
    (is (empty? (:diagnostics result)))
    (is (= 1 (:closed result)))))

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
    (is (= [:connect {:pool-size 1 :time-out 25}] (first (:calls result))))
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

(def ^:private diagnostic-paths
  ["\\\\server\\share"
   "\\\\server\\share\\dir\\file"
   "\\\\server\\shared reports\\private folder\\data.mdb"
   "//server/shared reports/private folder/data.mdb"
   "C:\\server\\data.mdb"
   "c:/Program Files/Datalevin/data.mdb"
   "C:\\Program Files (x86)\\Datalevin\\data.mdb"
   "C:\\Users\\O'Brien\\Private Files\\data.mdb"
   "\\\\?\\UNC\\server\\share\\private folder\\data.mdb"
   "\\\\?\\C:\\Program Files\\Datalevin\\data.mdb"
   "/private/server data/old (backup)/data.mdb"])

(deftest sanitization-removes-complete-paths-from-messages-and-unknown-keys
  (doseq [path diagnostic-paths]
    (testing path
      (is (= "[redacted-path]" (admin/sanitize nil path)))
      (is (= "Cannot read [redacted-path]"
             (admin/sanitize nil (str "Cannot read " path))))
      (doseq [quote ["\"" "'"]]
        (is (= (str "Cannot read " quote "[redacted-path]" quote ": access denied")
               (admin/sanitize nil (str "Cannot read " quote path quote ": access denied")))))
      (when (or (str/starts-with? path "\\\\") (re-find #"^[A-Za-z]:" path))
        (is (= "source:[redacted-path]" (admin/sanitize nil (str "source:" path)))))
      (let [value {:details {path [path true 42]}
                   :error (ex-info (str "Cannot read " path) {:opaque path})
                   :paths #{path}}
            clean (admin/sanitize nil value)]
        (is (= {:details {"[redacted-path]" ["[redacted-path]" true 42]}
                :error {:message "Cannot read [redacted-path]"
                        :data {:opaque "[redacted-path]"}}
                :paths #{"[redacted-path]"}}
               clean))
        (is (= clean (edn/read-string (pr-str clean)))))))
  (doseq [text ["ordinary status text" "view/alter" "dtlv://host/orders"
                "https://example.com/docs/server" "last-applied-lsn: 42"]]
    (is (= text (admin/sanitize nil text)))))

(deftest admin-errors-sanitize-paths-in-results-and-discovery-diagnostics
  (doseq [path diagnostic-paths]
    (testing path
      (let [fail (fn [& _] (throw (ex-info (str "Cannot read " path) {:opaque path})))
            named (run-admin {:database ["one"]} fail)
            global (run-admin {:all-databases true} fail)]
        (is (= 1 (:exit named)))
        (is (= {:message "Cannot read [redacted-path]" :data {:opaque "[redacted-path]"}}
               (get-in named [:results 0 :error])))
        (is (= 2 (:exit global)))
        (is (= ["Admin command failed: Cannot read [redacted-path]"] (:diagnostics global)))
        (is (= 1 (:closed named) (:closed global)))))))

(deftest admin-client-read-does-not-follow-ha-routing
  (let [requests (atom [])
        transport (reify client/IClient
                    (request [_ req]
                      (swap! requests conj req)
                      {:type :command-complete :result (status "orders-db" 1)}))]
    (is (= 1 (:db-id (client/admin-status transport "Orders_DB" {:runtime? false}))))
    (is (= [{:type :admin-status :args ["orders-db" {:runtime? false}] :writing? false}]
           @requests))))

(defn- run-gc [options request maintenance]
  (let [results (atom []) diagnostics (atom [])
        exit (admin/run! options ["wal-gc"]
                         {:getenv (constantly "dtlv://user:secret@localhost")
                          :connect (fn [& _] :client) :disconnect (fn [_])
                          :request (fn [_ op args] (request op args))
                          :maintenance maintenance :emit #(swap! results conj %)
                          :diagnostic #(swap! diagnostics conj %)})]
    {:exit exit :results @results :diagnostics @diagnostics}))

(defn- supported [name]
  (assoc (status name 1) :admin-capabilities #{:guarded-maintenance :wal-gc-dry-run}))

(deftest wal-gc-validates-cli-and-requires-capabilities-for-both-selectors
  (is (= "admin" (:command (main/validate-args ["admin" "wal-gc" "--database" "one" "--dry-run"]))))
  (doseq [all? [false true]]
    (let [sent (atom 0)
          result (run-gc (if all? {:all-databases true :dry-run true} {:database ["one"] :dry-run true})
                         (fn [op _]
                           (case op
                             :list-databases ["one"] :list-databases-in-use []
                             :query-system [["one" :key-value 1]]
                             :admin-status (status "one" 1)))
                         (fn [& _] (swap! sent inc)))]
      (is (= 2 (:exit result)))
      (is (zero? @sent))
      (is (= :admin/unsupported-server (get-in result [:results 0 :error :data :error]))))))

(deftest wal-gc-preserves-partial-results-and-continues
  (let [native {:ok? false :deleted-count 2 :deleted-bytes 100 :failed-count 1
                :segment-results [{:segment-id 1 :deleted? true :bytes 100 :path "/server/txn-1"}]
                :errors [{:error :gc/delete-failed :message "unlink failed" :data {}}]
                :after nil :durability {:status :synced}}
        calls (atom [])
        result (run-gc {:database ["partial" "good"]}
                       (fn [op [name opts]]
                         (is (= :admin-status op))
                         (is (= {:runtime? false} opts))
                         (supported name))
                       (fn [_ target opts]
                         (swap! calls conj [target opts])
                         (if (= "partial" (:db-name target)) native {:ok? true})))]
    (is (= 1 (:exit result)))
    (is (= ["partial" "good"] (mapv :db-name (:results result))))
    (is (= [:wal-gc :wal-gc] (mapv :operation (:results result))))
    (is (= (admin/sanitize nil native) (get-in result [:results 0 :result])))
    (is (= [{:db-name "partial" :db-type :key-value :db-id 1}
            {:db-name "good" :db-type :key-value :db-id 1}]
           (mapv first @calls)))))

(deftest gc-exit-mapping-preserves-native-skips-and-failures
  (doseq [[reason exit] [[:wal-disabled 0] [:rollback 0] [:maintenance-busy 1]
                         [:directory-sync-unsupported 1] [:report-failed 1]]]
    (let [native {:ok? false :skipped? true :reason reason}
          result (run-gc {:database ["one"]}
                         (fn [_ [name]] (supported name))
                         (fn [& _] native))]
      (is (= exit (:exit result)))
      (is (= native (get-in result [:results 0 :result]))))))

(deftest unsupported-after-earlier-maintenance-preserves-results-and-exits-one
  (let [sent (atom [])
        result (run-gc {:database ["first" "older" "later"]}
                       (fn [_ [name]] (if (= name "older") (status name 1) (supported name)))
                       (fn [_ target _] (swap! sent conj (:db-name target)) {:ok? true}))]
    (is (= 1 (:exit result)))
    (is (= ["first"] @sent))
    (is (= ["first" "older"] (mapv :db-name (:results result))))))

(deftest support-loss-before-the-first-send-exits-two
  (let [result (run-gc {:database ["first" "later"]}
                       (fn [_ [name]] (supported name))
                       (fn [& _]
                         (throw (ex-info "Server changed after discovery"
                                         {:error :admin/unsupported-server
                                          :admin/maintenance-sent? false}))))]
    (is (= 2 (:exit result)))
    (is (= 1 (count (:results result))))))

(deftest indeterminate-gc-never-starts-a-later-mutation
  (let [sent (atom [])
        result (run-gc {:database ["first" "later"]}
                       (fn [_ [name]] (supported name))
                       (fn [_ target _]
                         (swap! sent conj (:db-name target))
                         (throw (java.net.SocketTimeoutException. "Reply lost"))))]
    (is (= 1 (:exit result)))
    (is (= ["first"] @sent))
    (is (= 1 (count (:results result))))))
