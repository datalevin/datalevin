(ns datalevin.server.admin-status-test
  (:require [clojure.test :refer [deftest is use-fixtures]]
            [datalevin.core :as d]
            [datalevin.server.auth :as auth]
            [datalevin.server.handlers :as handlers]
            [datalevin.test.core :refer [db-fixture]]
            [datalevin.util :as u])
  (:import [java.nio.channels SelectionKey]))

(use-fixtures :once db-fixture)

(defn- with-catalog [f]
  (let [root (u/tmp-dir (str "admin-status-auth-" (random-uuid)))
        conn (d/create-conn root auth/server-schema {:wal? false})]
    (try
      (d/transact! conn [{:db/id 1 :database/name "orders" :database/type :key-value}
                         {:db/id 2 :database/name "other" :database/type :key-value}])
      (f conn)
      (finally (d/close conn) (u/delete-files root)))))

(defn- status-key []
  (doto (proxy [SelectionKey] []
          (channel [] nil)
          (selector [] nil)
          (isValid [] true)
          (cancel [])
          (interestOps ([] 0) ([_] nil))
          (readyOps [] 0))
    (.attach (volatile! {:client-id ::client}))))

(defn- status-deps [conn permissions locks responses reads before-read!]
  {:sys-conn (fn [_] conn)
   :get-client (fn [_ _] {:permissions @permissions})
   :with-db-lifecycle-access (fn [_ name f]
                               (swap! locks conj name)
                               (before-read!)
                               (f))
   :get-store (fn [_ name] (swap! reads conj name) nil)
   :write-message (fn [_ response] (swap! responses conj response))})

(defn- request-error [deps name runtime?]
  (try
    (handlers/admin-status deps nil (status-key) {:args [name {:runtime? runtime?}]})
    nil
    (catch clojure.lang.ExceptionInfo e
      {:message (ex-message e) :data (ex-data e)})))

(defn- permission [act id]
  (cond-> {:permission/act act :permission/obj auth/database-obj}
    id (assoc :permission/tgt {:db/id id})))

(deftest status-does-not-distinguish-missing-and-forbidden-databases
  (with-catalog
    (fn [conn]
      (let [permissions (atom [])
            locks (atom []) responses (atom []) reads (atom [])
            deps (status-deps conn permissions locks responses reads (fn []))]
        (doseq [runtime? [false true]]
          (let [missing (request-error deps "missing" runtime?)
                existing (request-error deps "orders" runtime?)]
            (is (some? missing))
            (is (= missing existing))
            (is (= {} (:data missing)))))
        (is (empty? @locks))
        (is (empty? @responses))
        (is (empty? @reads))
        (reset! permissions [(permission auth/view-act 1)])
        (doseq [runtime? [false true]]
          (is (nil? (request-error deps "orders" runtime?)))
          (let [forbidden (request-error deps "other" runtime?)]
            (is (some? forbidden))
            (is (= forbidden (request-error deps "missing" runtime?)))))
        (is (= ["orders" "orders"] @locks))
        (is (= ["orders"] @reads))
        (is (= [1 1] (mapv #(get-in % [:result :db-id]) @responses)))
        (is (false? (get-in (last @responses) [:result :open?])))
        (reset! permissions [])
        (is (= (request-error deps "other" true) (request-error deps "orders" true)))
        (is (= ["orders" "orders"] @locks))))))

(deftest globally-authorized-missing-status-allocates-no-lifecycle-locks
  (with-catalog
    (fn [conn]
      (let [permissions (atom [(permission auth/create-act nil)])
            locks (atom []) responses (atom []) reads (atom [])
            deps (status-deps conn permissions locks responses reads (fn []))]
        (doseq [runtime? [false true] n (range 16)]
          (let [name (str "missing-" n)
                error (request-error deps name runtime?)]
            (is (= :admin/database-not-found (get-in error [:data :error])))
            (is (= name (get-in error [:data :db-name])))))
        (is (empty? @locks))
        (is (empty? @responses))
        (is (empty? @reads))))))

(deftest status-rechecks-permission-after-lifecycle-access
  (doseq [drop? [false true]]
    (with-catalog
      (fn [conn]
        (let [permissions (atom [(permission auth/view-act 1)])
              locks (atom []) responses (atom []) reads (atom [])
              deps (status-deps conn permissions locks responses reads
                     #(if drop?
                        (d/transact! conn [[:db/retractEntity 1]])
                        (reset! permissions [])))
              error (request-error deps "orders" true)]
          (is (= (pr-str "Don't have permission to view database status") (:message error)))
          (is (= {} (:data error)))
          (is (empty? @responses))
          (is (empty? @reads))
          (is (= ["orders"] @locks)))))))
