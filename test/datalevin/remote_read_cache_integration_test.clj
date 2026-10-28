(ns datalevin.remote-read-cache-integration-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [datalevin.client.request :as cr]
            [datalevin.constants :as c]
            [datalevin.core :as d]
            [datalevin.interface :as i]
            [datalevin.remote :as remote]
            [datalevin.test.core :refer [server-fixture]]))

(use-fixtures :once server-fixture)

(defn- uri [] (str "dtlv://datalevin:datalevin@localhost/cache-" (random-uuid)))

(def opts {:client-opts {:pool-size 1 :time-out 5000}})

(deftest kv-cached-reads-follow-commits-and-dbi-changes
  (let [uri (uri)
        first (d/open-kv uri opts)
        second (d/open-kv uri opts)]
    (try
      (d/open-dbi first "values")
      (d/open-dbi second "values")
      (d/open-list-dbi first "lists")
      (d/transact-kv first [[:put "values" 1 :one :long :data]
                            [:put-list "lists" 1 [10 20] :long :long]])
      (let [read (d/prepare-get-value first "values" :long :data)]
        (binding [c/*remote-db-last-modified-check-interval-ms* 60000]
          (dotimes [_ 2]
            (is (= :one (d/get-value first "values" 1 :long :data) (read 1))))
          (is (nil? (read 2)))
          (is (= [[1 :one]] (vec (d/get-range first "values" [:all] :long :data))))
          (is (= [10 20] (vec (d/get-list first "lists" 1 :long :long))))
          (d/transact-kv first [[:put "values" 1 :two :long :data]
                                [:put "values" 2 false :long :data]])
          (is (= :two (read 1)))
          (is (false? (read 2)))
          (is (= [:two false]
                 (remote/batch-kv first [[:get-value "values" 1 :long :data true]
                                         [:get-value "values" 2 :long :data true]])))
          (testing "transaction reads observe staged data and abort cannot leak it"
            (d/with-transaction-kv [tx first]
              (d/transact-kv tx [[:put "values" 1 :staged :long :data]])
              (dotimes [_ 2] (is (= :staged (d/get-value tx "values" 1 :long :data))))
              (d/abort-transact-kv tx)))
          (is (= :two (read 1)))
          (d/with-transaction-kv [tx first]
            (d/transact-kv tx [[:put "values" 1 :committed :long :data]]))
          (is (= :committed (read 1))))
        (testing "strict checks see another client's writes"
          (d/transact-kv second [[:put "values" 1 :external :long :data]])
          (binding [c/*remote-db-last-modified-check-interval-ms* 0]
            (is (= :external (read 1)))))
        (binding [c/*remote-db-last-modified-check-interval-ms* 60000]
          (d/clear-dbi first "values")
          (is (nil? (read 1)))
          (d/drop-dbi first "values")
          (d/open-dbi first "values")
          (d/transact-kv first [[:put "values" 1 :recreated :long :data]])
          (is (= :recreated (read 1)))))
      (finally (d/close-kv first) (d/close-kv second)))))

(deftest datalog-cached-results-follow-local-and-external-writes
  (let [uri (uri)
        first (d/create-conn uri {:name {}} opts)
        second (d/create-conn uri nil opts)
        query '[:find ?name . :where [1 :name ?name]]]
    (try
      (d/transact! first [{:db/id 1 :name "one"}])
      (let [read-query (d/prepare-q @first query)
            read-pull (d/prepare-pull @first [:name])]
        (binding [c/*remote-db-last-modified-check-interval-ms* 60000]
          (dotimes [_ 2]
            (is (= "one" (d/q query @first) (read-query []))))
          (dotimes [_ 2]
            (is (= {:name "one"} (d/pull @first [:name] 1) (read-pull 1))))
          (dotimes [_ 2] (is (= "one" (:name (d/entity @first 1)))))
          (d/transact! first [[:db/add 1 :name "two"]])
          (is (= "two" (read-query [])))
          (is (= {:name "two"} (read-pull 1)))
          (is (= "two" (:name (d/entity @first 1)))))
        (d/transact! second [[:db/add 1 :name "external"]])
        (binding [c/*remote-db-last-modified-check-interval-ms* 0]
          (is (= "external" (read-query [])))
          (is (= {:name "external"} (read-pull 1)))
          (is (= [{:name "external"}] (d/pull-many @first [:name] [1])))))
      (finally (d/close first) (d/close second)))))

(defn- outcome [f]
  (try (f) (catch Throwable t t)))

(defn- interrupted-outcome [f]
  (let [^Thread thread (Thread/currentThread)]
    (try
      (.interrupt thread)
      [(outcome f) (.isInterrupted thread)]
      (finally (Thread/interrupted)))))

(deftest datalog-cache-hits-respect-deadlines-and-interruption
  (let [conn (d/create-conn (uri) {:name {}} opts)
        query '[:find ?name . :in $ ?id :where [?id :name ?name]]]
    (try
      (d/transact! conn [{:db/id 1 :name "one"}])
      (binding [c/*remote-db-last-modified-check-interval-ms* 60000]
        (let [view @conn
              store (:store view)
              read-query (d/prepare-q view query)
              read-pull (d/prepare-pull view [:name])
              direct-query #(i/q store query [:remote-db-placeholder 1])
              direct-pull #(i/pull store [:name] 1 {})
              reads {:query ["one" #(d/q query view 1) direct-query]
                     :prepared-query ["one" #(read-query [1]) direct-query]
                     :pull [{:name "one"} #(d/pull view [:name] 1) direct-pull]
                     :prepared-pull [{:name "one"} #(read-pull 1) direct-pull]
                     :pull-many [[{:name "one"}] #(d/pull-many view [:name] [1])
                                 #(i/pull-many store [:name] [1] {})]}]
          (doseq [[operation [expected read direct]] reads]
            (testing (name operation)
              (is (= expected (read) (read) (direct)))
              (doseq [[path request] [[:cached read] [:direct direct]]]
                (testing (name path)
                  (let [error (cr/with-budget (cr/budget 0 0 nil nil)
                                (outcome request))]
                    (is (= :socket/timeout (:error (ex-data error))))
                    (is (= :deadline-exceeded (:termination-reason (ex-data error)))))
                  ;; No enclosing request budget: cached calls must still
                  ;; recognize cancellation before starting a wire request.
                  (let [[error interrupted?] (interrupted-outcome request)]
                    (is (instance? InterruptedException error))
                    (is (true? interrupted?)))))
              (is (= expected (read)) "a rejected call leaves the handle usable")))))
      (finally (d/close conn)))))
