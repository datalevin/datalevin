(ns datalevin.remote-read-cache-test
  (:require [clojure.test :refer [deftest is testing]]
            [datalevin.bits :as bits]
            [datalevin.client :as client]
            [datalevin.constants :as c]
            [datalevin.core :as d]
            [datalevin.datom :as datom]
            [datalevin.db :as db]
            [datalevin.interface :as i]
            [datalevin.lmdb :as l]
            [datalevin.prepared :as prepared]
            [datalevin.query :as q]
            [datalevin.remote-cache :as cache])
  (:import [java.util Arrays]))

(defn- with-remote [limit f]
  (let [calls (atom {})
        info (atom {:last-modified 1 :max-tx 1})
        result (atom {:name "one"})
        writing? (atom false)
        closed? (atom false)
        location (str "dtlv://cache/" (random-uuid))
        read! (fn [op] (swap! calls update op (fnil inc 0)) @result)
        store (reify
                i/IStore
                (dir [_] location)
                (opts [_] {:cache-limit limit})
                l/IWriting
                (writing? [_] @writing?)
                i/IRemoteReadLifecycle
                (check-remote-read-open! [_]
                  (when @closed? (throw (ex-info "This client is closed" {}))))
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
          :writing? writing? :closed? closed?})
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

(deftest cached-datalog-hits-recheck-lifecycle-after-validation
  (with-remote 16
    (fn [{:keys [db store closed?]}]
      (binding [c/*remote-db-last-modified-check-interval-ms* 60000]
        (is (= {:name "one"} (d/pull db [:name] 1)))
        (is (thrown-with-msg?
              Exception #"This client is closed"
              (db/cached-remote-read store [:pull [:name] 1 {}]
                                     #(reset! closed? true)
                                     #(throw (AssertionError. "Unexpected cache miss")))))))))

(deftest remote-index-queries-observe-progress-without-data-changes
  (doseq [interval [0 60000]
          call '[(idoc-match $ :indexed ?needle)
                 (fulltext $ :indexed ?needle)
                 (vec-neighbors $ :indexed ?needle)
                 (embedding-neighbors $ :indexed ?needle)]]
    (testing (str call ", freshness interval " interval)
      (with-remote 16
        (fn [{:keys [db calls info result]}]
          (binding [c/*remote-db-last-modified-check-interval-ms* interval]
            (let [query (conj '[:find ?e :in $ ?needle :where] [call '[[?e _ _]]])
                  read (d/prepare-q db query)
                  before @info]
              (reset! result #{})
              (is (= #{} (d/q query db :needle) (read [:needle])))
              (reset! result #{[1]})
              (is (= #{[1]} (d/q query db :needle) (read [:needle])))
              (is (= 4 (:q @calls)))
              (is (= before @info)))))))))

(deftest remote-index-query-inputs-bypass-result-caching
  (doseq [[query inputs]
          [['[:find ?e :in $ % ?needle :where (matching $ ?needle ?e)]
            ['[[(matching $ ?needle ?e)
                [(idoc-match $ :indexed ?needle) [[?e _ _]]]]]
             :needle]]
           ['[:find ?e :in $ ?query ?needle
              :where [(q ?query $ ?needle) [[?e]]]]
            ['[:find ?e :in $ ?needle
               :where [(idoc-match $ :indexed ?needle) [[?e _ _]]]]
             :needle]]]]
    (with-remote 16
      (fn [{:keys [db calls result]}]
        (binding [c/*remote-db-last-modified-check-interval-ms* 0]
          (let [read (d/prepare-q db query)]
            (reset! result #{})
            (is (= #{} (apply d/q query db inputs) (read inputs)))
            (reset! result #{[1]})
            (is (= #{[1]} (apply d/q query db inputs) (read inputs)))
            (is (= 4 (:q @calls)))))))))

(deftest scalar-idoc-queries-do-not-retain-nil-results
  (with-remote 16
    (fn [{:keys [db calls info result]}]
      (let [query '[:find ?e . :in $ ?query
                    :where [(idoc-match $ :doc ?query) [[?e _ _]]]]
            read (d/prepare-q db query)
            before @info]
        (reset! result nil)
        (dotimes [_ 2]
          (is (nil? (d/q query db {:status "ready"})))
          (is (nil? (read [{:status "ready"}]))))
        (reset! result 1)
        (dotimes [_ 2]
          (is (= 1 (d/q query db {:status "ready"}) (read [{:status "ready"}]))))
        (is (= 8 (:q @calls)))
        (is (= before @info))))))

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

(deftest random-pull-transforms-execute-on-every-read
  (doseq [interval [0 60000]
          pattern '[[[:value :xform rand]]
                    [[:value :xform rand-int]]
                    [(:value :xform rand)]
                    [{:friend [[:value :xform rand-int]]}]
                    [[:value :as :random :default 100 :xform rand]]
                    [[:value :xform clojure.core/rand]]]]
    (testing (str pattern ", freshness interval " interval)
      (with-remote 16
        (fn [{:keys [db calls result]}]
          (binding [c/*remote-db-last-modified-check-interval-ms* interval]
            (let [read (d/prepare-pull db pattern)]
              (dotimes [n 4]
                (reset! result {:value n})
                (is (= {:value n} (d/pull db pattern 1) (read 1)))
                (reset! result [{:value n}])
                (is (= [{:value n}] (d/pull-many db pattern [1]))))
              (is (= 8 (:pull @calls)))
              (is (= 4 (:pull-many @calls))))))))))

(deftest deterministic-pull-transforms-retain-result-caching
  (with-remote 16
    (fn [{:keys [db calls]}]
      (binding [c/*remote-db-last-modified-check-interval-ms* 60000]
        (let [pattern '[[:name :xform vector]]
              read (d/prepare-pull db pattern)]
          (dotimes [_ 3]
            (is (= {:name "one"} (d/pull db pattern 1) (read 1)))
            (is (= {:name "one"} (d/pull-many db pattern [1]))))
          (is (= 1 (:pull @calls)))
          (is (= 1 (:pull-many @calls))))))))

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

(deftest kv-cache-identity-preserves-serialized-representations
  (doseq [[left right] [[[1 2] '(1 2)]
                       [[] '()]
                       [{:nested [1 2]} {:nested '(1 2)}]
                       [(array-map :a 1 :b 2) (array-map :b 2 :a 1)]
                       [1 (bigint 1)]
                       [1.0M 1.00M]
                       [(float 1.5) (double 1.5)]]]
    (testing (str (class left) " / " (class right) ": " left " / " right)
      (is (= left right))
      (is (not (Arrays/equals ^bytes (bits/serialize left)
                              ^bytes (bits/serialize right))))
      (let [cache (cache/read-cache 16 60000)
            reads (atom 0)
            read (fn [key result]
                   (cache/read-result
                     cache [:get-value ["db" "values" key :data :data true]]
                     0 (constantly 1) #(do (swap! reads inc) result)))]
        (dotimes [_ 2]
          (is (= :left (read left :left)))
          (is (= :right (read right :right))))
        (is (= 2 @reads))))))
