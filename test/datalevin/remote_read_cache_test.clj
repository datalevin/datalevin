(ns datalevin.remote-read-cache-test
  (:require [clojure.test :refer [deftest is testing]]
            [datalevin.client :as client]
            [datalevin.constants :as c]
            [datalevin.core :as d]
            [datalevin.datom :as datom]
            [datalevin.db :as db]
            [datalevin.interface :as i]
            [datalevin.lmdb :as l]
            [datalevin.prepared :as prepared]
            [datalevin.query :as q]
            [datalevin.remote-cache :as cache]))

(defn- with-remote [limit f]
  (let [calls (atom {})
        info (atom {:last-modified 1 :max-tx 1})
        result (atom {:name "one"})
        writing? (atom false)
        location (str "dtlv://cache/" (random-uuid))
        read! (fn [op] (swap! calls update op (fnil inc 0)) @result)
        store (reify
                i/IStore
                (dir [_] location)
                (opts [_] {:cache-limit limit})
                l/IWriting
                (writing? [_] @writing?)
                i/IRemoteDB
                (db-info [_] (swap! calls update :db-info (fnil inc 0)) @info)
                (q [_ _ _] (read! :q))
                (pull [_ _ _ _] (read! :pull))
                (pull-many [_ _ _ _] (read! :pull-many))
                i/IRemotePrepared
                (prepare-remote-read [_ op _] (prepared/prepared-read (fn [_] (read! op)))))
        database (db/map->DB {:store store :max-eid 1 :max-tx 1})]
    (try
      (f {:db database :store store :calls calls :info info :result result
          :writing? writing?})
      (finally (db/remove-cache store)))))

(def query '[:find ?name . :in $ ?id :where [?id :name ?name]])

(deftest ordinary-and-prepared-datalog-reads-share-results
  (with-remote 16
    (fn [{:keys [db calls]}]
      (binding [c/*remote-db-last-modified-check-interval-ms* 60000]
        (let [read-query (d/prepare-q db query)
              read-pull (d/prepare-pull db [:name])]
          (dotimes [_ 3]
            (is (= {:name "one"} (d/q query db 1) (read-query [1])))
            (is (= {:name "one"} (d/pull db [:name] 1) (read-pull 1)))
            (is (= {:name "one"} (d/pull-many db [:name] [1]))))
          (is (= {:db-info 1 :q 1 :pull 1 :pull-many 1} @calls)))))))

(deftest datalog-strict-freshness-and-local-writes-invalidate-results
  (with-remote 16
    (fn [{:keys [db store calls info result]}]
      (binding [c/*remote-db-last-modified-check-interval-ms* 0]
        (let [read (d/prepare-pull db [:name])]
          (is (= {:name "one"} (read 1)))
          (is (= {:name "one"} (read 1)))
          (is (= 1 (:pull @calls)))
          ;; Two commits may have the same wall-clock timestamp.
          (swap! info update :max-tx inc)
          (reset! result {:name "two"})
          (is (= {:name "two"} (read 1)))
          (is (= 2 (:pull @calls)))
          (swap! info assoc :schema-revision 2)
          (reset! result {:name ["two"]})
          (is (= {:name ["two"]} (read 1)))
          (is (= 3 (:pull @calls)))
          (binding [c/*remote-db-last-modified-check-interval-ms* 60000]
            (reset! result {:name "three"})
            (#'db/invalidate-cache store [(datom/datom 1 :name "three")] 2 3)
            (is (= {:name "three"} (read 1)))
            (is (= 4 (:pull @calls)))))))))

(deftest datalog-cache-bypasses-transactions-and-disabled-caching
  (doseq [limit [0 16]]
    (with-remote limit
      (fn [{:keys [db calls result writing?]}]
        (binding [c/*remote-db-last-modified-check-interval-ms* 60000]
          (dotimes [_ 2] (d/q query db 1))
          (is (= (if (zero? limit) 2 1) (:q @calls)))
          (binding [q/*cache?* false]
            (dotimes [_ 2] (d/q query db 1)))
          (is (= (if (zero? limit) 4 3) (:q @calls)))
          (reset! writing? true)
          (reset! result {:name "staged"})
          (dotimes [_ 2] (is (= {:name "staged"} (d/pull db [:name] 1))))
          (reset! writing? false)
          (reset! result {:name "committed"})
          (is (= {:name "committed"} (d/pull db [:name] 1)))
          (is (= 3 (:pull @calls))))))))

(deftest datalog-prepared-misses-use-only-the-read-rpc
  (with-remote 16
    (fn [{:keys [db calls]}]
      (binding [c/*remote-db-last-modified-check-interval-ms* 0]
        (let [read (d/prepare-pull db [:name])]
          (read 1)
          (read 2)
          (is (= {:db-info 1 :pull 2} @calls))
          (read 1)
          (is (= {:db-info 2 :pull 2} @calls)))))))

(deftest kv-cache-retains-negative-results-and-validates-revisions
  (let [cache (cache/read-cache 4 nil)
        token (atom 1)
        checks (atom 0)
        reads (atom 0)
        result (atom nil)
        read #(cache/read-result cache [:get-value 1] 0
                                 (fn [] (swap! checks inc) @token)
                                 (fn [] (swap! reads inc) @result))]
    (is (nil? (read)))
    (is (nil? (read)))
    (is (= [2 1] [@checks @reads]))
    (swap! token inc)
    (reset! result false)
    (is (false? (read)))
    (is (false? (read)))
    (is (= 2 @reads))
    (cache/mutate cache #(reset! result :updated))
    (is (= :updated (read)))
    (is (= 3 @reads))))

(deftest kv-cache-window-eviction-and-mutable-data
  (let [cache (cache/read-cache 2 60000)
        checks (atom 0)
        reads (atom 0)
        read (fn [key result]
               (cache/read-result cache key 0 #(swap! checks inc)
                                  #(do (swap! reads inc) result)))]
    (doseq [key [:a :a :b :a :c :b]] (read key key))
    (is (= 1 @checks))
    (is (= 4 @reads))
    (testing "mutable values cannot poison a later response"
      (let [bytes (byte-array [1])]
        (dotimes [_ 2] (read :bytes bytes))
        (dotimes [_ 2] (read bytes :value))
        (is (= 8 @reads))))))

(deftest kv-in-flight-read-cannot-publish-across-a-write
  (let [cache (cache/read-cache 4 60000)
        captured (promise)
        resume (promise)
        reader (future
                 (cache/read-result cache :key 0 (constantly 1)
                                    #(do (deliver captured true) @resume :old)))]
    (try
      (is (= true (deref captured 5000 :timeout)))
      (cache/mutate cache (constantly nil))
      (deliver resume true)
      (is (= :old (deref reader 5000 :timeout)))
      (is (= :new (cache/read-result cache :key 0 (constantly 2) (constantly :new))))
      (finally (deliver resume true) (future-cancel reader)))))

(deftest kv-old-servers-fall-back-once
  (let [cache (cache/read-cache 4 60000)
        checks (atom 0)
        reads (atom 0)]
    (dotimes [_ 3]
      (is (= :value
             (cache/read-result
               cache :key 0
               #(do (swap! checks inc)
                    (throw (ex-info "Unknown message type :kv-read-cache-token" {})))
               #(do (swap! reads inc) :value)))))
    (is (= [1 3] [@checks @reads]))))

(deftest kv-failed-mutations-and-zero-capacity-do-not-retain-results
  (let [cache (cache/read-cache 4 60000)
        read #(cache/read-result cache :key 0 (constantly 1) (constantly %))]
    (is (= :before (read :before)))
    (is (thrown? Exception
                 (cache/mutate cache #(throw (ex-info "uncertain commit" {})))))
    (is (= :after (read :after))))
  (let [cache (cache/read-cache 0 60000)
        checks (atom 0)
        reads (atom 0)]
    (dotimes [_ 2]
      (cache/read-result cache :key 0 #(swap! checks inc) #(swap! reads inc)))
    (is (= [0 2] [@checks @reads])))
  (is (not (cache/cacheable-call? :sample-kv ["db" "values" 1])))
  (is (not (cache/cacheable-call? :batch-kv
                                 ["db" [[:get-value "values" 1]
                                         [:sample-kv "values" 1]]]))))

(deftest client-caches-ordinary-and-prepared-kv-requests-together
  (let [calls (atom {})
        transport (reify client/IClient
                    (disconnected? [_] false)
                    (request [_ {:keys [type]}]
                      (swap! calls update type (fnil inc 0))
                      {:type :command-complete
                       :result (case type :kv-read-cache-token [:epoch 1]
                                          :get-value :value nil)}))
        opts {:client-opts {:read-cache-check-interval-ms 60000}}
        request (prepared/request ["cache" "values" nil :long :data true])]
    (client/open-database transport "cache" c/db-store-kv opts)
    (dotimes [_ 3]
      (is (= :value (client/normal-request transport :get-value
                                           ["cache" "values" 1 :long :data true])))
      (is (= :value (client/normal-prepared-request transport :get-value request 1 false))))
    (is (= {:open-kv 1 :kv-read-cache-token 1 :get-value 1} @calls))))
