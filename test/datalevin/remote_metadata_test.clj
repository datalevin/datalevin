(ns datalevin.remote-metadata-test
  (:require [clojure.test :refer [deftest is testing]]
            [datalevin.client :as client]
            [datalevin.constants :as c]
            [datalevin.core :as d]
            [datalevin.db :as db]
            [datalevin.interface :as i]
            [datalevin.remote :as remote]
            [datalevin.test.core :refer [server-fixture]])
  (:import [datalevin.remote DatalogStore]
           [java.util.concurrent.atomic AtomicBoolean]))

(def ^:private lookup-schema
  (assoc c/implicit-schema :user/id
         {:db/aid (count c/implicit-schema)
          :db/valueType :db.type/string
          :db/unique :db.unique/identity}))

(defn- remote-store
  [opening-schema?]
  (let [calls (atom [])
        info (atom {:max-eid 1 :max-tx c/tx0 :last-modified 1
                    :schema-revision 1
                    :opts {:cache-limit 64 :background-sampling? false}})
        db-info-opts? (atom true)
        snapshot (atom {:schema lookup-schema :revision 1})
        client (reify client/IClient
                 (request [_ request]
                   (swap! calls conj (:type request))
                   {:type :command-complete
                    :result (case (:type request)
                              :db-info (cond-> @info (not @db-info-opts?) (dissoc :opts))
                              :tx-data+db-info {:db-info (dissoc @info :opts)}
                              :schema-snapshot @snapshot
                              :av-first-e (when (= "one" (nth (:args request) 2)) 1)
                              :opts (:opts @info)
                              :assoc-opt (swap! info assoc-in
                                               [:opts (nth (:args request) 1)]
                                               (nth (:args request) 2))
                              :close nil)})
                 (disconnected? [_] false)
                 (disconnect [_] nil)
                 (get-pool [_] nil)
                 (get-id [_] nil))
        store (remote/->DatalogStore
                (str "dtlv://metadata/" (random-uuid)) "metadata" client
                (volatile! nil) false
                (volatile! (cond-> @info opening-schema? (assoc :schema lookup-schema)))
                (AtomicBoolean.) false (AtomicBoolean.))]
    {:store store :info info :db-info-opts? db-info-opts?
     :snapshot snapshot :calls calls}))

(deftest lookup-refs-only-request-the-entity-id-within-freshness-window
  (let [{:keys [store calls]} (remote-store true)
        database (binding [c/*remote-db-last-modified-check-interval-ms* 60000]
                   (db/new-db store))]
    (try
      (binding [c/*remote-db-last-modified-check-interval-ms* 60000]
        (doseq [_ (range 3)]
          (is (= 1 (d/entid database [:user/id "one"])))
          (is (nil? (d/entid database [:user/id "missing"])))))
      (is (= (vec (repeat 6 :av-first-e)) @calls)
          "opening metadata supplies schema, reverse schema and DB initialization")
      (finally (db/remove-cache store) (i/close store)))))

(deftest older-opening-responses-fetch-schema-once
  (let [{:keys [store calls]} (remote-store false)
        database (binding [c/*remote-db-last-modified-check-interval-ms* 60000]
                   (db/new-db store))]
    (try
      (binding [c/*remote-db-last-modified-check-interval-ms* 60000]
        (dotimes [_ 3] (is (= 1 (d/entid database [:user/id "one"]))))
        (is (= [:schema-snapshot :av-first-e :av-first-e :av-first-e] @calls))
        (is (= lookup-schema (i/schema store)))
        (is (= #{:db/ident :user/id} (:db/unique (i/rschema store))))
        (is (= 4 (count @calls))))
      (finally (db/remove-cache store) (i/close store)))))

(deftest freshness-checks-refresh-schema-only-when-its-revision-changes
  (let [{:keys [store info snapshot calls]} (remote-store true)]
    (try
      (i/db-info store)
      (binding [c/*remote-db-last-modified-check-interval-ms* 60000]
        (dotimes [_ 3] (is (= @info (i/db-info store))))
        (is (empty? @calls)))
      (binding [c/*remote-db-last-modified-check-interval-ms* 0]
        (testing "ordinary data changes preserve the schema cache"
          (swap! info update :max-tx inc)
          (i/db-info store)
          (is (identical? lookup-schema (i/schema store)))
          (is (= [:db-info :db-info] @calls)))
        (testing "another client's schema change refreshes both maps"
          (swap! snapshot update :schema dissoc :user/id)
          (swap! snapshot assoc :revision 2)
          (swap! info assoc :schema-revision 2 :last-modified 2)
          (i/db-info store)
          (is (not (contains? (i/schema store) :user/id)))
          (is (not (contains? (:db/unique (i/rschema store)) :user/id)))
          (is (= 1 (:schema-snapshot (frequencies @calls)))))
        (testing "servers without revision metadata invalidate on data changes"
          (swap! info dissoc :schema-revision)
          (swap! info update :last-modified inc)
          (i/db-info store)
          (i/schema store)
          (is (= [:db-info :schema-snapshot] (take-last 2 @calls)))))
      (finally (i/close store)))))

(deftest option-updates-discard-cached-options
  (let [{:keys [store]} (remote-store true)]
    (try
      (is (= 64 (:cache-limit (i/opts store))))
      (i/assoc-opt store :cache-limit 8)
      (is (= 8 (:cache-limit (i/opts store))))
      (finally (i/close store)))))

(deftest rpc-result-cache-policy-follows-custom-schema
  (let [{:keys [store info snapshot calls]} (remote-store true)]
    (try
      (i/db-info store)
      (is (true? (i/remote-read-cacheable? store)))
      (is (empty? @calls) "a warm plain schema needs no policy RPC")
      (swap! snapshot update :schema assoc :task/value {:db/valueType :test/value})
      (swap! snapshot assoc :revision 2)
      (swap! info assoc :schema-revision 2)
      (binding [c/*remote-db-last-modified-check-interval-ms* 0]
        (i/db-info store))
      (i/prepare-remote-read-cache! store)
      (dotimes [_ 3] (is (false? (i/remote-read-cacheable? store))))
      (is (= [:db-info :schema-snapshot] @calls)
          "a cold schema is fetched once; decoder policy then stays local")
      (finally (i/close store)))))

(deftest rpc-result-cache-policy-does-not-fetch-missing-metadata
  (let [{:keys [store calls]} (remote-store false)]
    (try
      (i/db-info store)
      (dotimes [_ 3]
        (is (false? (i/remote-read-cacheable? store))))
      (is (empty? @calls) "an eligibility check must stay local")
      (i/prepare-remote-read-cache! store)
      (is (true? (i/remote-read-cacheable? store)))
      (is (= [:schema-snapshot] @calls)
          "only explicit preparation fetches missing policy metadata")
      (finally (i/close store)))))

(deftest rpc-result-cache-publication-skips-missing-metadata
  (doseq [result [{:name "completed"} nil false]]
    (let [{:keys [store calls]} (remote-store true)]
      (try
        (binding [c/*remote-db-last-modified-check-interval-ms* 60000]
          (db/new-db store)
          (is (= result (db/cached-remote-read
                          store [:completed-read]
                          #(do (i/assoc-opt store :cache-limit 64) result))))
          (is (= [:assoc-opt] @calls)
              "metadata invalidation after decoding must not cause another RPC")
          (let [reads (atom 0)
                read #(db/cached-remote-read
                        store [:completed-read]
                        (fn [] (swap! reads inc) :next))]
            (is (= :next (read) (read)))
            (is (= 1 @reads) "a later read can prepare metadata and retain results")
            (is (= [:assoc-opt :schema-snapshot] @calls))))
        (finally (db/remove-cache store) (i/close store))))))

(deftest direct-options-honor-the-freshness-window
  (let [{:keys [store info calls]} (remote-store true)]
    (try
      (i/db-info store)
      (binding [c/*remote-db-last-modified-check-interval-ms* 60000]
        (is (= 64 (:cache-limit (i/opts store))))
        (swap! info assoc-in [:opts :cache-limit] 8)
        (dotimes [_ 3] (is (= 64 (:cache-limit (i/opts store)))))
        (is (empty? @calls))
        ;; Expire the check deterministically without waiting for the interval.
        (swap! (.-metadata-cache ^DatalogStore store) update :opts-checked-at
               (fn [^long checked-at] (- checked-at 60000000001)))
        (is (= 8 (:cache-limit (i/opts store))))
        (dotimes [_ 3] (is (= 8 (:cache-limit (i/opts store)))))
        (is (= [:db-info] @calls)))
      (binding [c/*remote-db-last-modified-check-interval-ms* 0]
        (swap! info assoc-in [:opts :cache-limit] 4)
        (dotimes [_ 3] (is (= 4 (:cache-limit (i/opts store)))))
        (is (= (vec (repeat 4 :db-info)) @calls)))
      (finally (i/close store)))))

(deftest options-refresh-falls-back-when-db-info-omits-options
  (let [{:keys [store info db-info-opts? calls]} (remote-store true)]
    (try
      (i/db-info store)
      (binding [c/*remote-db-last-modified-check-interval-ms* 0]
        (is (= 64 (:cache-limit (i/opts store))))
        (reset! db-info-opts? false)
        (swap! info assoc-in [:opts :cache-limit] 8)
        (is (= 8 (:cache-limit (i/opts store))))
        (is (= [:db-info :db-info :opts] @calls)))
      (finally (i/close store)))))

(deftest transaction-metadata-without-options-does-not-extend-options-freshness
  (let [{:keys [store info calls]} (remote-store true)]
    (try
      (i/db-info store)
      (binding [c/*remote-db-last-modified-check-interval-ms* 60000]
        (is (= 64 (:cache-limit (i/opts store))))
        (swap! (.-metadata-cache ^DatalogStore store)
               (fn [state]
                 (let [expired (- (long (:checked-at state)) 60000000001)]
                   (assoc state :checked-at expired :opts-checked-at expired))))
        (swap! info assoc-in [:opts :cache-limit] 8)
        (i/tx-data store [] false)
        (is (= [:tx-data+db-info] @calls))
        (is (= 8 (:cache-limit (i/opts store))))
        (is (= [:tx-data+db-info :db-info] @calls)))
      (finally (i/close store)))))

(deftest remote-schema-cache-follows-commits-and-rollbacks
  (server-fixture
    (fn []
      (let [uri (str "dtlv://datalevin:datalevin@localhost/metadata-" (random-uuid))
            conn (d/create-conn uri {:user/id {:db/unique :db.unique/identity}})
            store (:store @conn)]
        (try
          (d/transact! conn [{:db/id 1 :user/id "one" :auto-added 42}])
          (is (= 1 (d/entid @conn [:user/id "one"])))
          (is (contains? (d/schema conn) :auto-added))
          (is (thrown-with-msg?
                clojure.lang.ExceptionInfo #"rollback"
                (d/with-transaction [tx conn]
                  (d/update-schema tx {:aborted {:db/unique :db.unique/identity}})
                  (is (contains? (:db/unique (i/rschema (:store @tx))) :aborted))
                  (throw (ex-info "rollback" {})))))
          (is (not (contains? (d/schema conn) :aborted)))
          (is (not (contains? (:db/unique (i/rschema store)) :aborted)))
          (d/with-transaction [tx conn]
            (d/update-schema tx {:committed {:db/unique :db.unique/identity}}))
          (is (contains? (:db/unique (i/rschema store)) :committed))
          (with-open [^java.io.Closeable other (d/create-conn uri)]
            (d/update-schema other nil nil {:user/id :user/key})
            (is (contains? (d/schema conn) :user/key))
            (is (= 1 (d/entid @conn [:user/key "one"])))
            (is (not (contains? (:db/unique (i/rschema store)) :user/id))))
          (finally (d/close conn)))))))

(deftest direct-lookups-follow-external-uniqueness-changes
  (server-fixture
    (fn []
      (binding [c/*remote-db-last-modified-check-interval-ms* 0]
        (let [uri (str "dtlv://datalevin:datalevin@localhost/direct-metadata-" (random-uuid))]
          (with-open [^java.io.Closeable reader
                      (d/create-conn uri {:user/id {:db/unique :db.unique/identity}}
                                     {:client-opts {:read-cache-check-interval-ms 60000}})
                      ^java.io.Closeable writer (d/create-conn uri)]
            (d/transact! writer [{:db/id 1 :user/id "one"}])
            (let [view @reader]
              (is (= 1 (d/entid view [:user/id "one"])))
              (is (thrown-with-msg? clojure.lang.ExceptionInfo #"unique"
                                   (d/entid view [:external/id "external"])))
              (testing "a newly unique attribute is usable without calling db?"
                (d/update-schema writer {:external/id {:db/unique :db.unique/identity}})
                (d/transact! writer [[:db/add 1 :external/id "external"]])
                (is (= 1 (d/entid view [:external/id "external"])))
                (is (= 1 (:db/id (d/entity view [:external/id "external"])))))
              (testing "removing uniqueness rejects lookup refs without calling db?"
                (d/update-schema writer {:user/id {:db/unique :db/retract}})
                (is (thrown-with-msg? clojure.lang.ExceptionInfo #"unique"
                                     (d/entid view [:user/id "one"])))))))))))

(deftest direct-schema-consumers-observe-external-revisions
  (doseq [read [i/schema i/rschema]
          revision? [true false]]
    (let [{:keys [store info snapshot]} (remote-store true)]
      (try
        (i/db-info store)
        (when-not revision? (swap! info dissoc :schema-revision))
        (binding [c/*remote-db-last-modified-check-interval-ms* 0]
          (read store)
          (swap! snapshot update :schema dissoc :user/id)
          (swap! snapshot assoc :revision 2)
          (swap! info assoc (if revision? :schema-revision :last-modified) 2)
          ;; Do not use db? or db-info between the schema change and the read.
          (let [result (read store)]
            (is (not (contains? (if (= read i/schema) result (:db/unique result))
                                :user/id)))))
        (finally (i/close store))))))

(deftest direct-schema-consumers-honor-the-freshness-window
  (doseq [read [i/schema i/rschema]]
    (let [{:keys [store info snapshot calls]} (remote-store true)
          attributes #(if (= read i/schema) % (:db/unique %))]
      (try
        (i/db-info store)
        (binding [c/*remote-db-last-modified-check-interval-ms* 60000]
          (is (contains? (attributes (read store)) :user/id))
          (swap! snapshot update :schema dissoc :user/id)
          (swap! snapshot assoc :revision 2)
          (swap! info assoc :schema-revision 2)
          (is (contains? (attributes (read store)) :user/id))
          (is (empty? @calls))
          ;; Expire the cached check deterministically without sleeping.
          (swap! (.-metadata-cache ^DatalogStore store) update :checked-at
                 (fn [^long checked-at] (- checked-at 60000000001)))
          (is (not (contains? (attributes (read store)) :user/id)))
          (is (= [:db-info :schema-snapshot] @calls)))
        (finally (i/close store))))))
