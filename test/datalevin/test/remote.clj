(ns datalevin.test.remote
  (:require
   [clojure.test :refer [deftest is]]
   [datalevin.bits :as b]
   [datalevin.client :as client]
   [datalevin.constants :as c]
   [datalevin.interface :as i]
   [datalevin.lmdb :as l]
   [datalevin.remote :as remote])
  (:import
   [datalevin.lmdb DatomKVTxData KVTxData]
   [java.util Arrays]
   [java.util.concurrent.atomic AtomicBoolean]))

(defrecord FakeClient [calls]
  client/IClient
  (request [_ req]
    (swap! calls conj req)
    {:type :command-complete})
  (copy-in [_ _req _data _batch-size])
  (disconnect [_])
  (disconnected? [_] false)
  (get-pool [_] nil)
  (get-id [_] nil))

(defrecord FakeOpenClient [calls open-result]
  client/IClient
  (request [_ req]
    (swap! calls conj req)
    (cond-> {:type :command-complete}
      (= :open (:type req)) (assoc :result open-result)))
  (copy-in [_ _req _data _batch-size])
  (disconnect [_])
  (disconnected? [_] false)
  (get-pool [_] nil)
  (get-id [_] nil))

(deftest expand-datom-kv-txs-for-transport-test
  (let [insert-avg (b/indexable nil 1 42 :db.type/long c/g0)
        delete-avg (b/indexable nil 1 43 :db.type/long c/g0)
        insert-bs  (b/indexable-bytes insert-avg)
        delete-bs  (b/indexable-bytes delete-avg)
        txs        [(DatomKVTxData. 1000 insert-bs true)
                    (DatomKVTxData. 1001 delete-bs false)]
        expanded   (l/expand-datom-kv-txs txs)
        ^bytes insert-key (.-k ^KVTxData (nth expanded 0))
        ^bytes delete-key (.-k ^KVTxData (nth expanded 2))]
    (is (= 4 (count expanded)))
    (is (every? #(instance? KVTxData %) expanded))
    (is (Arrays/equals insert-bs insert-key))
    (is (Arrays/equals delete-bs delete-key))
    (is (= [[:put c/ave insert-key 1000 :raw :id]
            [:put c/eav 1000 insert-key :id :raw]
            [:del-list c/ave delete-key [1001] :raw :id]
            [:del-list c/eav 1001 [delete-key] :id :raw]]
           (mapv (fn [^KVTxData tx]
                   [(.-op tx) (.-dbi-name tx) (.-k tx) (.-v tx)
                    (.-kt tx) (.-vt tx)])
                 expanded))))
  (let [txs [(l/kv-tx :put "generic" 1 2 :long :long)]]
    (is (identical? txs (l/expand-datom-kv-txs txs)))))

(defn- remote-store
  [db-name client open-db-info]
  (remote/->DatalogStore
    (str "dtlv://localhost/" db-name)
    db-name
    client
    (volatile! :remote-dl-mutex)
    false
    (volatile! open-db-info)
    (AtomicBoolean. false)
    false
    (AtomicBoolean. false)))

(deftest remote-start-sampling-respects-db-option-test
  (let [calls (atom [])
        client (->FakeClient calls)]
    (let [disabled (remote-store
                     "disabled"
                     client
                     {:max-tx 0
                      :opts {:background-sampling? false}})]
      (i/start-sampling disabled)
      (is (empty? @calls)))
    (let [ha-store (remote-store
                     "ha"
                     client
                     {:max-tx 0
                      :opts {:ha-mode :consensus-lease}})]
      (i/start-sampling ha-store)
      (is (empty? @calls)))
    (let [enabled (remote-store
                    "enabled"
                    client
                    {:max-tx 0
                     :opts {:background-sampling? true}})]
      (i/start-sampling enabled)
      (i/start-sampling enabled)
      (is (= 1 (count @calls)))
      (is (= :start-sampling (:type (first @calls)))))))

(deftest remote-open-uses-client-ha-opts-for-sampling-decision-test
  (let [calls  (atom [])
        client (->FakeOpenClient calls {:max-tx 0
                                        :opts {}})]
    (let [store (remote/open client
                             "dtlv://localhost/ha"
                             nil
                             {:ha-mode :consensus-lease}
                             false)]
      (is (= {:max-tx 0
              :opts {:ha-mode :consensus-lease}}
             (remote/db-info store)))
      (i/start-sampling store)
      (is (= [:open] (mapv :type @calls))))))
