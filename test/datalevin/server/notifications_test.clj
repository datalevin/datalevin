(ns datalevin.server.notifications-test
  (:require [clojure.test :refer [deftest is use-fixtures]]
            [datalevin.client :as client]
            [datalevin.core :as d]
            [datalevin.server :as server]
            [datalevin.server.notifications :as notifications]
            [datalevin.test.core :refer [allocate-port db-fixture]]
            [datalevin.util :as u])
  (:import [java.util.concurrent LinkedBlockingQueue TimeUnit]))

(use-fixtures :each db-fixture)

(defn- with-server [f]
  (let [root (u/tmp-dir (str "notifications-" (random-uuid)))
        port (allocate-port)
        srv (server/create {:root root :port port})]
    (try
      (server/start srv)
      (f (str "dtlv://datalevin:datalevin@localhost:" port))
      (finally (server/stop srv) (u/delete-files root)))))

(defn- event [^LinkedBlockingQueue queue]
  (.poll queue 5000 TimeUnit/MILLISECONDS))

(defn- quiet? [^LinkedBlockingQueue queue]
  (nil? (.poll queue 150 TimeUnit/MILLISECONDS)))

(deftest topic-broadcast-and-coalescing-test
  (let [topic (notifications/topic)
        token (:token (notifications/await-change topic nil 0))
        a (future (notifications/await-change topic token 1000))
        b (future (notifications/await-change topic token 1000))]
    (notifications/publish! topic)
    (is (:changed? (deref a 2000 nil)))
    (is (:changed? (deref b 2000 nil)))
    (notifications/publish! topic)
    (notifications/publish! topic)
    (let [result (notifications/await-change topic token 0)]
      (is (:changed? result))
      (is (not (:changed? (notifications/await-change topic (:token result) 0)))))))

(deftest shared-notifications-follow-commit-and-database-test
  (with-server
    (fn [base]
      (let [url (str base "/shared")
            writer (d/create-conn url)
            a (d/create-conn url nil {:client-opts {:pool-size 1}})
            b (d/create-conn url)
            other (d/create-conn (str base "/other"))
            qa (LinkedBlockingQueue.)
            qb (LinkedBlockingQueue.)
            qo (LinkedBlockingQueue.)
            local (atom [])
            expected {:type :db-changed :db-name "shared"}
            kv (d/datalog-kv writer)]
        (try
          (d/open-dbi kv "state")
          (d/listen! a :local #(swap! local conj %))
          (is (= :a (d/listen-db! a :a #(.offer qa %))))
          (d/listen-db! b :b #(.offer qb %))
          (d/listen-db! other :other #(.offer qo %))
          (is (= :transacted (d/transact-ack! writer [{:db/id 1 :value :committed}])))
          (is (= expected (event qa)))
          (is (= expected (event qb)))
          (is (quiet? qo))
          (is (empty? @local))
          (is (= :committed (:value (d/pull @a [:value] 1))))
          (d/with-transaction [tx writer]
            (d/transact! tx [[:db/add 1 :value :inside]])
            (d/transact-kv (d/datalog-kv tx) "state" [[:put :key :inside]])
            (is (quiet? qa))
            (is (quiet? qb)))
          (is (= expected (event qa)))
          (is (= expected (event qb)))
          (is (quiet? qa))
          (d/with-transaction [tx writer]
            (d/transact! tx [[:db/add 1 :value :aborted]])
            (d/transact-kv (d/datalog-kv tx) "state" [[:put :key :aborted]])
            (d/abort-transact tx))
          (is (quiet? qa))
          (is (quiet? qb))
          (is (thrown-with-msg? Exception #"rollback"
                (d/with-transaction [tx writer]
                  (d/transact! tx [[:db/add 1 :value :failed]])
                  (throw (ex-info "rollback" {})))))
          (is (quiet? qa))
          (d/tx-data->simulated-report @writer [[:db/add 1 :value :simulated]])
          (is (quiet? qa))
          (is (quiet? qb))
          (d/transact-kv kv "state" [[:put :key :kv-only]])
          (is (= expected (event qa)))
          (is (= expected (event qb)))
          (d/unlisten-db! a :a)
          (d/close b)
          (d/transact! writer [[:db/add 1 :value :after-unsubscribe]])
          (is (quiet? qa))
          (is (quiet? qb))
          (is (quiet? qo))
          (finally
            (doseq [conn [writer a b other]] (d/close conn))))))))

(deftest subscription-callback-can-query-and-replace-test
  (with-server
    (fn [base]
      (let [url (str base "/callbacks")
            writer (d/create-conn url)
            reader (d/create-conn url nil {:client-opts {:pool-size 1}})
            old (LinkedBlockingQueue.)
            results (LinkedBlockingQueue.)]
        (try
          (d/listen-db! reader :listener #(.offer old %))
          (d/listen-db! reader :listener
                        (fn [_]
                          (.offer results (d/q '[:find ?v . :where [1 :value ?v]]
                                               @reader))))
          (d/transact! writer [{:db/id 1 :value :first}])
          (is (= :first (event results)))
          (is (quiet? old))
          (d/transact! writer [[:db/add 1 :value :second]])
          (is (= :second (event results)))
          (d/listen-db! reader :listener
                        (fn [_]
                          (.offer results :callback-ran)
                          (throw (ex-info "bad callback" {}))))
          (d/transact! writer [[:db/add 1 :value :third]])
          (is (= :callback-ran (event results)))
          (d/transact! writer [[:db/add 1 :value :fourth]])
          (is (= :callback-ran (event results)))
          (d/listen-db! reader :listener
                        (fn [_]
                          (d/unlisten-db! reader :listener)
                          (.offer results (d/q '[:find ?v . :where [1 :value ?v]]
                                               @reader))))
          (d/transact! writer [[:db/add 1 :value :self-unsubscribed]])
          (is (= :self-unsubscribed (event results)))
          (d/transact! writer [[:db/add 1 :value :done]])
          (is (quiet? results))
          (finally (d/close reader) (d/close writer)))))))

(deftest subscription-permissions-are-rechecked-test
  (with-server
    (fn [base]
      (let [writer (d/create-conn (str base "/private"))
            admin (client/new-client base)
            queue (LinkedBlockingQueue.)]
        (try
          (client/create-user admin "reader" "secret")
          (client/create-role admin :reader)
          (client/assign-role admin :reader "reader")
          (client/grant-permission admin :reader :datalevin.server/view
                                   :datalevin.server/database "private")
          (let [uri (str "dtlv://reader:secret@localhost:"
                         (.getPort (java.net.URI. base)) "/private")
                reader (d/create-conn uri)]
            (try
              (d/listen-db! reader :listener #(.offer queue %))
              (client/revoke-permission admin :reader :datalevin.server/view
                                        :datalevin.server/database "private")
              (d/transact! writer [{:db/id 1 :value :secret}])
              (is (= :subscription-error (:type (event queue))))
              (is (quiet? queue))
              (is (thrown? Exception (d/listen-db! reader :denied identity)))
              (finally (d/close reader))))
          (finally (client/disconnect admin) (d/close writer)))))))
