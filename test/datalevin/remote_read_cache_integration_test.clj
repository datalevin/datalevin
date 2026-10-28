(ns datalevin.remote-read-cache-integration-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [datalevin.client :as client]
            [datalevin.constants :as c]
            [datalevin.core :as d]
            [datalevin.db :as db]
            [datalevin.interface :as i]
            [datalevin.query :as q]
            [datalevin.remote :as remote]
            [datalevin.test.core :refer [server-fixture]])
  (:import [datalevin.remote DatalogStore]))

(use-fixtures :once server-fixture)

(defn- uri [] (str "dtlv://datalevin:datalevin@localhost/cache-" (random-uuid)))

(def opts {:client-opts {:pool-size 1 :time-out 5000}})

(deftest cached-datalog-reads-reject-closed-connections
  (doseq [[kind close!]
          [[:connection (fn [conn _] (d/close conn))]
           [:client (fn [_ store] (client/close-client (.-client ^DatalogStore store)))]
           [:store (fn [_ store] (i/close store))]]]
    (testing (name kind)
      (with-open [^java.io.Closeable conn (d/create-conn (uri) {:name {}} opts)]
        (let [view @conn
              store (:store view)
              query '[:find ?name . :where [1 :name ?name]]]
          (try
            (d/transact! conn [{:db/id 1 :name "one"}])
            (binding [c/*remote-db-last-modified-check-interval-ms* 60000]
              (let [puller (d/prepare-pull view [:name])
                    reader (d/prepare-q view query)
                    reads {:pull [{:name "one"} #(d/pull view [:name] 1)]
                           :prepared-pull [{:name "one"} #(puller 1)]
                           :pull-many [[{:name "one"}] #(d/pull-many view [:name] [1])]
                           :query ["one" #(d/q query view)]
                           :prepared-query ["one" #(reader [])]}]
                (doseq [[_ [expected read]] reads]
                  (dotimes [_ 2] (is (= expected (read)))))
                (close! conn store)
                (is (thrown-with-msg? Exception #"This client is closed"
                                     (i/pull store [:name] 1 {})))
                (doseq [[operation [_ read]] reads]
                  (testing (name operation)
                    (is (thrown-with-msg? Exception #"This client is closed" (read)))))))
            (finally (i/close store))))))))

(deftest cached-datalog-reads-reject-closed-stores-with-borrowed-clients
  (let [uri (uri)
        client (client/new-client uri (:client-opts opts))
        store (remote/open client uri {:name {}} opts)]
    (try
      (with-open [^java.io.Closeable conn (d/conn-from-db (db/new-db store))]
        (d/transact! conn [{:db/id 1 :name "one"}])
        (let [view @conn]
          (binding [c/*remote-db-last-modified-check-interval-ms* 60000]
            (let [puller (d/prepare-pull view [:name])]
              (is (= {:name "one"} (d/pull view [:name] 1) (puller 1)))
              (d/close conn)
              (is (not (client/disconnected? client)))
              (is (thrown-with-msg? Exception #"Database is closed"
                                   (d/pull view [:name] 1)))
              (is (thrown-with-msg? Exception #"Database is closed" (puller 1)))))))
      (finally
        (i/close store)
        (client/close-client client)))))

(deftest closing-a-shared-client-store-preserves-other-stores-caching
  (doseq [kind [:kv :datalog]]
    (testing (name kind)
      (let [client (client/new-client
                     (uri) (assoc (:client-opts opts)
                                  :read-cache-check-interval-ms 60000))
            close! (case kind :kv d/close-kv :datalog i/close)]
        (try
          (let [first (case kind
                        :kv (remote/open-kv client (uri) opts)
                        :datalog (remote/open client (uri) nil opts))]
            (try
              (let [second (remote/open-kv client (uri) opts)]
                (try
                  (d/open-dbi second "values")
                  (d/transact-kv second
                                 [[:put "values" 1 {:value :one} :long :data]
                                  [:put "values" 2 {:value :two} :long :data]])
                  (let [read (d/prepare-get-value second "values" :long :data)
                        cached (read 1)]
                    (is (= {:value :one} cached))
                    (is (identical? cached (read 1)))
                    (close! first)
                    (is (not (client/disconnected? client)))
                    (testing "existing results remain cached after closing another store"
                      (is (identical? cached (read 1)))
                      (is (identical? cached
                                      (d/get-value second "values" 1 :long :data))))
                    (testing "new misses are still retained"
                      (let [result (read 2)]
                        (is (= {:value :two} result))
                        (is (identical? result (read 2)))))
                    (testing "the remaining store's transaction client still invalidates"
                      (d/with-transaction-kv [tx second]
                        (d/transact-kv tx
                                       [[:put "values" 1 {:value :updated} :long :data]]))
                      (let [result (read 1)]
                        (is (= {:value :updated} result))
                        (is (identical? result (read 1))))))
                  (finally (d/close-kv second))))
              (finally (close! first))))
          (finally (client/close-client client)))))))

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

(deftest mutable-query-results-cannot-poison-later-reads
  (let [conn (d/create-conn (uri) {:name {}} opts)
        query '[:find ?e :where [?e :name]]]
    (try
      (d/transact! conn [{:db/id 1 :name "one"}])
      (let [database @conn
            prepared (d/prepare-q database query)]
        (doseq [interval [0 60000]
                read [#(d/q query database) #(prepared [])]]
          (binding [c/*remote-db-last-modified-check-interval-ms* interval]
            (let [result (read)]
              (is (= #{[1]} result))
              (is (contains? (conj result [999]) [999]))
              (is (= #{[1]} (d/q query database) (prepared [])))
              (is (= (binding [q/*cache?* false] (d/q query database))
                     (read)))))))
      (finally (d/close conn)))))

(deftest kv-cached-reads-distinguish-serialized-data-keys
  (let [uri (uri)
        cached (d/open-kv uri (assoc-in opts [:client-opts :read-cache-check-interval-ms] 60000))
        uncached (d/open-kv uri (assoc-in opts [:client-opts :read-cache-limit] 0))]
    (try
      (doseq [[label stored missing]
              [[:sequence [1 2] '(1 2)]
               [:nested {:key [1 2]} {:key '(1 2)}]
               [:integer 1 (bigint 1)]
               [:decimal 1.0M 1.00M]
               [:float (float 1.5) (double 1.5)]]]
        (testing (name label)
          (let [dbi (name label)]
            (d/open-dbi cached dbi)
            (d/open-dbi uncached dbi)
            (d/transact-kv cached [[:put dbi stored :stored :data :data]])
            (let [prepared (d/prepare-get-value cached dbi :data :data)]
              (testing "ordinary and prepared point reads match uncached reads"
                (dotimes [_ 2]
                  (is (= :stored (d/get-value cached dbi stored :data :data)))
                  (is (nil? (d/get-value uncached dbi missing :data :data)))
                  (is (nil? (d/get-value cached dbi missing :data :data)))
                  (is (= :stored (prepared stored)))
                  (is (nil? (prepared missing)))))
              (testing "batch arguments and range bounds preserve key identity"
                (is (= [:stored] (remote/get-values cached dbi [stored])))
                (is (= [nil] (remote/get-values cached dbi [missing])))
                (is (= 1 (d/range-count cached dbi [:closed stored stored] :data)))
                (is (= 0 (d/range-count uncached dbi [:closed missing missing] :data)))
                (is (= 0 (d/range-count cached dbi [:closed missing missing] :data))))
              (testing "distinct encoded keys can both have cached values"
                (d/transact-kv cached [[:put dbi missing :other :data :data]])
                (dotimes [_ 2]
                  (is (= :stored (prepared stored)))
                  (is (= :other (prepared missing)))))))))
      (finally (d/close-kv cached) (d/close-kv uncached)))))
