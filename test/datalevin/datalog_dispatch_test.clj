(ns datalevin.datalog-dispatch-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [datalevin.constants :as c]
            [datalevin.core :as d]
            [datalevin.db :as db]
            [datalevin.interface :as i]
            [datalevin.query :as q]
            [datalevin.test.core :refer [db-fixture]])
  (:import [java.util.concurrent ConcurrentHashMap]))

(use-fixtures :each db-fixture)

;; Use protocol extension rather than implementing the generated interfaces.
(defrecord ExtendedRemote [location info calls])

(extend-type ExtendedRemote
  i/IStore
  (dir [store] (:location store))
  (opts [_] {:cache-limit 64})
  i/IRemoteDB
  (db-info [store]
    (swap! (:calls store) inc)
    @(:info store))
  (q [_ query inputs] {:query query :inputs inputs})
  (pull [_ pattern id opts] [pattern id opts])
  (pull-many [_ pattern ids opts] [pattern ids opts])
  i/IRemotePrepared
  (prepare-remote-read [_ operation args]
    (fn [input] [operation args input])))

(defn- extended-remote []
  (->ExtendedRemote (str "dtlv://dispatch/" (random-uuid))
                    (atom {:last-modified 1 :max-tx 1}) (atom 0)))

(deftest built-in-and-extended-store-classification
  (let [conn (d/create-conn nil {} {:kv-opts {:inmemory? true}})
        direct (reify i/IRemoteDB (db-info [_] {})
                      i/IRemotePrepared (prepare-remote-read [_ _ _] identity))
        extended (extended-remote)]
    (try
      (doseq [store [(:store @conn) nil (Object.)]]
        (is (false? (db/remote-store? store)))
        (is (false? (db/remote-prepared-store? store))))
      (doseq [store [direct extended]]
        (is (true? (db/remote-store? store)))
        (is (true? (db/remote-prepared-store? store))))
      (is (not (instance? datalevin.interface.IRemoteDB extended)))
      (is (not (instance? datalevin.interface.IRemotePrepared extended)))
      (is (true? (d/db? @conn)))
      (doseq [x [nil {} [] 1 "db"]] (is (not (d/db? x))))
      (finally (d/close conn)))))

(deftest local-db-check-needs-no-store-metadata
  (let [conn (d/create-conn nil {} {:kv-opts {:inmemory? true}})
        database @conn]
    (try
      (with-redefs-fn
        {#'db/caches (proxy [ConcurrentHashMap] []
                      (get [_] (throw (ex-info "Local db? accessed result cache" {}))))}
        #(is (true? (d/db? database))))
      (finally (d/close conn)))
    ;; Classification also remains valid after close.
    (is (true? (d/db? database)))))

(deftest remote-db-check-preserves-freshness-and-throttling
  (let [store (extended-remote)
        database (db/map->DB {:store store :max-eid 0 :max-tx 0})]
    (try
      (binding [c/*remote-db-last-modified-check-interval-ms* 60000]
        (is (true? (d/db? database)))
        (is (= 1 @(:calls store)))
        (db/cache-put store :sentinel :cached)
        (is (true? (d/db? database)))
        (is (= 1 @(:calls store))))
      (binding [c/*remote-db-last-modified-check-interval-ms* 0]
        (testing "a new transaction with the same timestamp still invalidates"
          (swap! (:info store) assoc :max-tx 2)
          (is (true? (d/db? database)))
          (is (= 2 @(:calls store)))
          (is (nil? (db/cache-get store :sentinel))))
        (testing "an unchanged revision preserves cached reads"
          (db/cache-put store :sentinel :current)
          (is (true? (d/db? database)))
          (is (= :current (db/cache-get store :sentinel))))
        (testing "a newer timestamp also invalidates"
          (swap! (:info store) assoc :last-modified 2)
          (is (true? (d/db? database)))
          (is (nil? (db/cache-get store :sentinel)))))
      (finally (db/remove-cache store)))))

(deftest query-routing-handles-source-position-and-multiple-databases
  (let [conn (d/create-conn nil {} {:kv-opts {:inmemory? true}})
        store (extended-remote)
        remote (db/map->DB {:store store :max-eid 0 :max-tx 0})]
    (try
      (doseq [inputs [[@conn] [42 @conn] [@conn remote] [remote @conn]
                      [remote remote] [42 :no-db]]]
        (is (nil? (#'q/only-remote-db inputs))))
      (is (zero? @(:calls store)))
      (binding [c/*remote-db-last-modified-check-interval-ms* 0]
        (doseq [inputs [[remote 42] [42 remote] [42 remote :last]]]
          (let [[rstore routed] (#'q/only-remote-db inputs)]
            (is (identical? store rstore))
            (is (= (mapv #(if (identical? remote %) :remote-db-placeholder %) inputs)
                   routed))))
        (is (= {:query :query :inputs [42 :remote-db-placeholder]}
               (d/q :query 42 remote)))
        (is (= [[:name] 1 {}] (d/pull remote [:name] 1)))
        (is (= [[:name] [1 2] {}] (d/pull-many remote [:name] [1 2])))
        (is (= [:pull [[:name] nil nil] 1]
               ((d/prepare-pull remote [:name]) 1)))
        (is (= [:q ['[:find ?v :in $ ?v] nil] [:remote-db-placeholder 42]]
               ((d/prepare-q remote '[:find ?v :in $ ?v]) [42]))))
      (finally (d/close conn) (db/remove-cache store)))))
