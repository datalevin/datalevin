(ns datalevin.server.context-test
  (:require
   [clojure.test :refer [deftest is testing]]
   [datalevin.server.context :as context]))

(defn- fixture-context []
  (let [calls (atom [])
        base (into {}
                   (map (fn [k]
                          [k (fn [& args]
                               (swap! calls conj [k args])
                               ::fallback)]))
                   [:get-client :db-state :db-store :get-db :store :lmdb])]
    (assoc (context/create
             ::server ::key base
             (fn [state writing?]
               (swap! calls conj [:resolve-store writing?])
               (:store state))
             (fn [state writing?]
               (swap! calls conj [:resolve-db writing?])
               (:db state))
             :lmdb)
           :calls calls)))

(deftest one-resolution-per-read-and-fresh-state-on-the-next-test
  (let [{:keys [context deps calls]} (fixture-context)]
    (doseq [n [1 2]]
      (let [session {:stores {"db" {}} :permissions [n]}
            store {:lmdb n}
            state {:store store :db n :ha-role (if (= n 1) :leader :follower)}]
        (reset! calls [])
        (context/prepare! context ::client session "db" state true)
        (is (identical? session ((:get-client deps) ::server ::client)))
        (is (identical? state ((:db-state deps) ::server "db")))
        (is (identical? store ((:store deps) ::server ::key "db" false)))
        (is (= n ((:lmdb deps) ::server ::key "db" false)))
        (is (= n ((:get-db deps) ::server "db")))
        (is (= n ((:get-db deps) ::server "db" false)))
        (is (= [[:resolve-store false] [:resolve-db false]] @calls))
        (context/clear! context)))
    (testing "released state cannot be used by a later callback"
      (is (= ::fallback ((:get-client deps) ::server ::client)))
      (is (= ::fallback ((:db-state deps) ::server "db")))
      (is (= ::fallback ((:db-store deps) ::server ::key "db")))
      (is (= ::fallback ((:get-db deps) ::server "db"))))))

(deftest context-does-not-cross-session-database-connection-or-transaction-test
  (let [{:keys [context deps calls]} (fixture-context)]
    (context/prepare! context ::client {:stores {"db" {}}} "db"
                      {:store {:lmdb :current} :db :current} true)
    (doseq [[k args] [[:get-client [::server ::other-client]]
                     [:db-state [::server "other-db"]]
                     [:db-state [::other-server "db"]]
                     [:db-store [::server ::other-key "db"]]
                     [:db-store [::server ::key "other-db"]]
                     [:store [::server ::key "db" true]]
                     [:lmdb [::server ::key "db" true]]
                     [:get-db [::server "other-db" true]]]]
      (reset! calls [])
      (is (= ::fallback (apply (get deps k) args)))
      (is (= [[k args]] @calls)))
    (context/clear! context)
    (doseq [session [nil {:stores {}}]]
      (context/prepare! context ::client session "db" {:store :inaccessible} true)
      (reset! calls [])
      (is (nil? ((:db-store deps) ::server ::key "db")))
      (doseq [[k db-type] [[:store "datalog"] [:lmdb "kv"]]]
        (let [error (try
                      ((get deps k) ::server ::key "db" false)
                      (catch clojure.lang.ExceptionInfo e (ex-data e)))]
          (is (= {:type :reopen :db-name "db" :db-type db-type} error))))
      (is (empty? @calls) "a removed session or membership never resolves a store")
      (context/clear! context))))

(deftest mutations-refresh-session-and-both-database-views-test
  (let [session (atom {:stores {"db" {}} :permissions [:initial]})
        state (atom {:store :original-store :db :original-db :wdb :original-wdb})
        base {:get-client (fn [_ _] @session)
              :db-state (fn [_ _] @state)}
        {:keys [context deps]}
        (context/create ::server ::key base
                        (fn [m _] (:store m))
                        (fn [m writing?] (get m (if writing? :wdb :db)))
                        identity)]
    (context/prepare! context ::client nil "db" nil false)
    (is (= :original-store ((:db-store deps) ::server ::key "db")))
    (is (= :original-db ((:get-db deps) ::server "db")))
    (is (= :original-wdb ((:get-db deps) ::server "db" true)))
    ;; A handler can publish a new store or transaction view before making
    ;; another callback. The connection remains the same throughout.
    (reset! state {:store :replacement-store :db :replacement-db :wdb :replacement-wdb})
    (is (= :replacement-wdb ((:get-db deps) ::server "db" true)))
    (is (= :replacement-db ((:get-db deps) ::server "db")))
    (is (= :replacement-store ((:db-store deps) ::server ::key "db")))
    (swap! session assoc :permissions [:updated])
    (is (= [:updated] (:permissions ((:get-client deps) ::server ::client))))
    (swap! session assoc :stores {})
    (is (nil? ((:db-store deps) ::server ::key "db")))
    (reset! session nil)
    (is (nil? ((:get-client deps) ::server ::client)))
    (is (nil? ((:db-store deps) ::server ::key "db")))
    (context/clear! context)))
