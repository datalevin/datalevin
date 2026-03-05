(ns datalevin.test.conn
  (:require
   [datalevin.test.core :as tdc :refer [db-fixture]]
   [clojure.test :refer [deftest testing is use-fixtures]]
   [datalevin.conn :as dc]
   [datalevin.core :as d]
   [datalevin.db :as db]
   [datalevin.constants :as c]
   [datalevin.util :as u])
  (:import [java.util Date UUID]))

(use-fixtures :each db-fixture)

(deftest test-close
  (let [dir  (u/tmp-dir (str "test-" (UUID/randomUUID)))
        conn (d/create-conn dir)]
    (is (not (d/closed? conn)))
    (d/close conn)
    (is (d/closed? conn))
    (u/delete-files dir)))

(deftest test-update-schema
  (let [dir1  (u/tmp-dir (str "test-" (UUID/randomUUID)))
        dir2  (u/tmp-dir (str "test-" (UUID/randomUUID)))
        conn1 (d/create-conn dir1)
        s     {:a/b {:db/valueType :db.type/string}}
        s1    {:c/d {:db/valueType :db.type/string}}
        txs   [{:c/d "cd" :db/id -1}
               {:a/b "ab" :db/id -2}]
        conn2 (d/create-conn dir2 s)]
    (is (= (d/schema conn2) (d/update-schema conn1 s)))
    (d/update-schema conn1 s1)
    (is (= (d/schema conn1) (-> (merge c/implicit-schema s s1)
                                (assoc-in [:a/b :db/aid] 3)
                                (assoc-in [:c/d :db/aid] 4))))
    (d/transact! conn1 txs)
    (is (= 2 (count (d/datoms @conn1 :eav))))

    (is (thrown-with-msg? Exception #"Cannot delete attribute"
                          (d/update-schema conn1 {} #{:c/d})))

    (d/transact! conn1 [[:db/retractEntity 1]])
    (is (= (d/schema conn2)
           (d/update-schema conn1 {} #{:c/d})
           (d/schema conn1)))

    (d/update-schema conn1 nil nil {:a/b :e/f})
    (is (= (d/schema conn1) (assoc c/implicit-schema :e/f
                                   {:db/valueType :db.type/string :db/aid 3})))

    (d/close conn1)
    (d/close conn2)
    (u/delete-files dir1)
    (u/delete-files dir2)))

(deftest test-update-schema-1
  (let [dir  (u/tmp-dir (str "test-" (UUID/randomUUID)))
        conn (d/create-conn dir)]
    (d/update-schema conn {:things {}})
    (is (= (d/schema conn) (-> c/implicit-schema
                               (assoc-in [:things :db/aid] 3))))
    (d/update-schema conn {:stuff {}})
    (is (= (d/schema conn) (-> c/implicit-schema
                               (assoc-in [:things :db/aid] 3)
                               (assoc-in [:stuff :db/aid] 4))))
    (d/update-schema conn {} [:things])
    (is (= (d/schema conn) (-> c/implicit-schema
                               (assoc-in [:stuff :db/aid] 4))))
    (d/update-schema conn {:things {}})
    (is (= (d/schema conn) (-> c/implicit-schema
                               (assoc-in [:stuff :db/aid] 4)
                               (assoc-in [:things :db/aid] 5))))
    (d/close conn)
    (u/delete-files dir)))

(deftest test-update-schema-ensure-no-duplicate-aids
  (let [dir  (u/tmp-dir (str "test-" (UUID/randomUUID)))
        conn (d/create-conn dir)]
    (d/update-schema conn {:up/a {}})
    (d/transact! conn [{:foo 1}])
    (let [aids (map :db/aid (vals (d/schema conn)))]
      (is (= (count aids) (count (set aids))))
      (d/close conn)
      (u/delete-files dir))))

(deftest test-update-schema-validates-new-attrs
  (let [dir  (u/tmp-dir (str "test-" (UUID/randomUUID)))
        conn (d/create-conn dir)]
    ;; :db/isComponent true requires :db/valueType :db.type/ref
    (is (thrown-with-msg?
          Exception #"isComponent.*should also have.*ref"
          (d/update-schema conn {:bad/attr {:db/isComponent true
                                            :db/valueType   :db.type/string}})))
    ;; invalid :db/valueType
    (is (thrown-with-msg?
          Exception #"Bad attribute specification"
          (d/update-schema conn {:bad/attr {:db/valueType :db.type/bogus}})))
    ;; invalid :db/cardinality
    (is (thrown-with-msg?
          Exception #"Bad attribute specification"
          (d/update-schema conn {:bad/attr {:db/cardinality :db.cardinality/bogus}})))
    ;; valid schema still works
    (d/update-schema conn {:good/attr {:db/valueType :db.type/string}})
    (is (:good/attr (d/schema conn)))
    (d/close conn)
    (u/delete-files dir)))

(deftest test-ways-to-create-conn-1
  (let [dir  (u/tmp-dir (str "test-" (UUID/randomUUID)))
        conn (d/create-conn dir)]
    (is (= #{} (set (d/datoms @conn :eav))))
    (is (= c/implicit-schema (db/-schema @conn)))
    (d/close conn)
    (u/delete-files dir)))

(deftest test-ways-to-create-conn-2
  (let [schema { :aka { :db/cardinality :db.cardinality/many :db/aid 3}}
        dir    (u/tmp-dir (str "test-" (UUID/randomUUID)))
        conn   (d/create-conn dir schema)]
    (is (= #{} (set (d/datoms @conn :eav))))
    (is (= (db/-schema @conn) (merge schema c/implicit-schema)))
    (d/close conn)
    (u/delete-files dir)))

(deftest test-ways-to-create-conn-3
  (let [datoms #{(d/datom 1 :age  17)
                 (d/datom 1 :name "Ivan")}
        dir    (u/tmp-dir (str "test-" (UUID/randomUUID)))
        conn   (d/conn-from-datoms datoms dir)]
    (is (= datoms (set (d/datoms @conn :eav))))
    (is (= (d/schema conn) (db/-schema @conn)))
    (d/close conn)
    (u/delete-files dir))

  (let [schema { :aka { :db/cardinality :db.cardinality/many :db/aid 1}}
        datoms #{(d/datom 1 :age  17)
                 (d/datom 1 :name "Ivan")}
        dir    (u/tmp-dir (str "test-" (UUID/randomUUID)))
        conn   (d/conn-from-datoms datoms dir schema)]
    (is (= datoms (set (d/datoms @conn :eav))))
    (is (= (d/schema conn) (db/-schema @conn)))
    (d/close conn)
    (u/delete-files dir))

  (let [datoms #{(d/datom 1 :age  17)
                 (d/datom 1 :name "Ivan")}
        dir    (u/tmp-dir (str "test-" (UUID/randomUUID)))
        conn   (d/conn-from-db (d/init-db datoms dir))]
    (is (thrown-with-msg? Exception
                          #"init-db expects list of Datoms, got "
                          (d/init-db [[:add -1 :name "Ivan"]
                                      {:add -1 :age 35}])))
    (is (= datoms (set (d/datoms @conn :eav))))
    (is (= (d/schema conn) (db/-schema @conn)))
    (d/close conn)
    (u/delete-files dir))

  (let [schema { :aka { :db/cardinality :db.cardinality/many :db/aid 1}}
        datoms #{(d/datom 1 :age  17)
                 (d/datom 1 :name "Ivan")
                 (d/datom 1 :aka "danger")
                 (d/datom 1 :aka "fun")}
        dir    (u/tmp-dir (str "test-" (UUID/randomUUID)))
        conn   (d/conn-from-db (-> (d/empty-db dir schema)
                                   (d/fill-db datoms)))]
    (is (= datoms (set (d/datoms @conn :eav))))
    (is (= (d/schema conn) (db/-schema @conn)))
    (d/close conn)
    (u/delete-files dir)))

(deftest test-recreate-conn
  (let [schema {:name          {:db/valueType :db.type/string}
                :dt/updated-at {:db/valueType :db.type/instant}}
        dir    (u/tmp-dir (str "recreate-conn-test-" (UUID/randomUUID)))
        conn   (d/create-conn dir schema)]
    (d/transact! conn [{:db/id         -1
                        :name          "Namebo"
                        :dt/updated-at (Date.)}])
    (d/close conn)

    (let [conn2 (d/create-conn dir schema)]
      (d/transact! conn2 [{:db/id         -2
                           :name          "Another name"
                           :dt/updated-at (Date.)}])
      (is (= 4 (count (d/datoms @conn2 :eav))))
      (d/close conn2))
    (u/delete-files dir)))

(deftest test-get-conn
  (let [schema {:name          {:db/valueType :db.type/string}
                :dt/updated-at {:db/valueType :db.type/instant}}
        dir    (u/tmp-dir (str "get-conn-test-" (UUID/randomUUID)))
        conn   (d/get-conn dir schema)]
    (d/transact! conn [{:db/id         -1
                        :name          "Namebo"
                        :dt/updated-at (Date.)}])
    (d/close conn)

    (let [conn2 (d/get-conn dir schema)]
      (d/transact! conn2 [{:db/id         -2
                           :name          "Another name"
                           :dt/updated-at (Date.)}])
      (is (= 4 (count (d/datoms @conn2 :eav))))
      (d/close conn2))
    (u/delete-files dir)))

(deftest test-with-conn
  (let [dir (u/tmp-dir (str "with-conn-test-" (UUID/randomUUID)))]
    (d/with-conn [conn dir]
      (d/transact! conn [{:db/id      -1
                          :name       "something"
                          :updated-at (Date.)}])
      (is (= 2 (count (d/datoms @conn :eav)))))
    (u/delete-files dir)))

(deftest test-relaxed-transact-uses-queued-path
  (let [conn  (d/create-conn nil
                             {:k {:db/valueType :db.type/long}}
                             {:wal? true
                              :wal-durability-profile :relaxed
                              :kv-opts {:inmemory? true}})
        paths (atom [])]
    (try
      (binding [dc/*txlog-sync-path-observer*
                (fn [path] (swap! paths conj path))]
        (dotimes [i 32]
          (d/transact! conn [{:db/id i :k i}])))
      (is (= 32 (count @paths)))
      (is (every? #{:queued-relaxed} @paths))
      (finally
        (dc/shutdown-transact-async-executor!)
        (d/close conn)))))

(deftest test-strict-transact-async-no-stall
  (let [n    256
        conn (d/create-conn nil
                            {:k {:db/valueType :db.type/long}}
                            {:wal? true
                             :wal-durability-profile :strict
                             :kv-opts {:inmemory? true}})]
    (try
      (let [futs    (doall
                      (for [i (range n)]
                        (d/transact-async conn [{:db/id i :k i}])))
            results (doall (map #(deref % 10000 ::timeout) futs))]
        (is (not-any? #{::timeout} results))
        (is (= n
               (d/q '[:find (count ?e) .
                      :where [?e :k]]
                    (d/db conn)))))
      (finally
        (dc/shutdown-transact-async-executor!)
        (d/close conn)))))

(defn- wait-until
  [pred timeout-ms]
  (let [^long timeout-ms timeout-ms]
    (loop [elapsed 0]
      (cond
        (pred) true
        (>= ^long elapsed timeout-ms) false
        :else
        (do
          (Thread/sleep 25)
          (recur (+ elapsed 25)))))))

(defn- transact-async-executor-atom
  []
  (some-> (ns-resolve 'datalevin.conn 'transact-async-executor-atom)
          var-get))

(deftest test-strict-with-transaction-transact-uses-direct-path
  (let [conn  (d/create-conn nil
                             {:k {:db/valueType :db.type/long}}
                             {:wal? true
                              :wal-durability-profile :strict
                              :kv-opts {:inmemory? true}})
        paths (atom [])]
    (try
      (binding [dc/*txlog-sync-path-observer*
                (fn [path] (swap! paths conj path))]
        (d/with-transaction [cn conn]
          (dotimes [i 8]
            (d/transact! cn [{:db/id (- (inc i)) :k i}]))))
      (is (= 8 (count @paths)))
      (is (every? #{:direct-no-wal} @paths))
      (is (= 8
             (d/q '[:find (count ?e) .
                    :where [?e :k]]
                  (d/db conn))))
      (finally
        (d/close conn)))))

(deftest test-last-lmdb-close-shuts-down-transact-async-executor
  (let [dir     (u/tmp-dir (str "strict-async-shutdown-test-" (UUID/randomUUID)))
        ex-atom (transact-async-executor-atom)
        conn    (d/create-conn dir
                               {:k {:db/valueType :db.type/long}}
                               {:wal? true
                                :wal-durability-profile :strict})]
    (try
      (d/transact! conn [{:db/id -1 :k 1}])
      @(d/transact-async conn [{:db/id -2 :k 2}])
      (is (some? @ex-atom))
      (d/close conn)
      (is (wait-until #(nil? @ex-atom) 2000))
      (finally
        (dc/shutdown-transact-async-executor!)
        (u/delete-files dir)))))
