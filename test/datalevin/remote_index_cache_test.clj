(ns datalevin.remote-index-cache-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [datalevin.bits :as bits]
            [datalevin.client :as client]
            [datalevin.constants :as c]
            [datalevin.core :as d]
            [datalevin.datom :as datom]
            [datalevin.db :as db]
            [datalevin.interface :as i]
            [datalevin.server :as server]
            [datalevin.test.core :refer [db-fixture server-port]]
            [datalevin.udf :as udf]
            [datalevin.util :as u])
  (:import [datalevin.server Server]
           [datalevin.storage Store]))

(def ^:dynamic *server* nil)

(use-fixtures :once db-fixture
  (fn [f]
    (let [root (u/tmp-dir (str "index-cache-codec-" (random-uuid)))
          srv (server/create {:root root :port 0})]
      (binding [*server* srv client/*default-port* (server-port srv)]
        (try
          (server/start srv)
          (f)
          (finally
            (server/stop srv)
            (u/delete-files root)))))))

(defn- descriptor [kind]
  {:udf/lang :test :udf/kind kind :udf/id :test/index-cache})

(defn- install-runtime! [db-name registry]
  (let [^java.util.Map dbs (.-dbs ^Server *server*)
        state (get dbs db-name)
        ^Store store (:store state)
        runtime {:udf-registry registry}]
    (.put dbs db-name (assoc state :runtime-opts runtime))
    (vswap! (i/kv-info (.-lmdb store)) assoc :runtime-opts runtime)))

(def ^:private index-reads
  [[:eav #(d/datoms % :eav 1 :value)]
   [:ave #(d/datoms % :ave :value)]
   [:search #(d/search-datoms % 1 :value nil)]
   [:e-datoms #(db/-e-datoms % 1)]
   [:seek #(d/seek-datoms % :eav 1 :value)]
   [:rseek #(d/rseek-datoms % :eav 1 :value)]
   [:index-range #(d/index-range % :value nil nil)]
   [:range-datoms #(db/-range-datoms % :eav
                                    (datom/datom 1 :value c/v0)
                                    (datom/datom 1 :value c/vmax))]
   [:first #(vector (db/-first % [1 :value nil]))]])

(deftest remote-index-reads-observe-server-decoder-rebindings
  (doseq [interval [0 60000]]
    (testing (str "freshness interval " interval)
      (let [db-name (str "index-decoder-" (random-uuid))
            uri (str "dtlv://datalevin:datalevin@localhost/" db-name)
            registry (udf/create-registry)
            decode (descriptor :deserializer)]
        (with-open [^java.io.Closeable conn
                    (d/create-conn uri {} {:background-sampling? false
                                          :client-opts {:pool-size 1 :time-out 5000}})]
          (install-runtime! db-name registry)
          (udf/register! registry (descriptor :order-fn) :rank)
          (udf/register! registry (descriptor :serializer) bits/serialize)
          (udf/register! registry decode bits/deserialize)
          (d/register-type conn :test/index-value
                           {:index {:type :long :order-fn (descriptor :order-fn)}
                            :payload {:serialize (descriptor :serializer)
                                      :deserialize decode}})
          (d/update-schema conn {:value {:db/valueType :test/index-value}})
          (d/transact! conn [{:db/id 1 :value {:rank 1}}])
          (binding [c/*remote-db-last-modified-check-interval-ms* interval]
            ;; Clear transaction overlays so range reads exercise the cache.
            (let [view (db/transfer @conn (:store @conn))
                  store (:store view)
                  revision #(select-keys (i/db-info store)
                                         [:max-tx :last-modified :schema-revision])
                  before (revision)]
              (doseq [[operation read] index-reads]
                (testing (str "warm " operation)
                  (dotimes [_ 2] (is (= [{:rank 1}] (mapv :v (read view)))))))
              (udf/register! registry decode #(assoc (bits/deserialize %) :decoder :new))
              (is (= before (revision)))
              (is (= {:rank 1 :decoder :new} (:v (i/ea-first-datom store 1 :value))))
              (doseq [[operation read] index-reads]
                (testing (str "rebound " operation)
                  (dotimes [_ 2]
                    (is (= [{:rank 1 :decoder :new}] (mapv :v (read view)))))))
              (udf/unregister! registry decode)
              (doseq [[operation read] index-reads]
                (testing (str "unbound " operation)
                  (is (thrown-with-msg? Exception #"UDF|binding" (read view)))))
              (udf/register! registry decode bits/deserialize)
              (doseq [[operation read] index-reads]
                (testing (str "restored " operation)
                  (is (= [{:rank 1}] (mapv :v (read view))))))
              (is (= before (revision))))))))))
