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
   [datalevin.bits Indexable]
   [datalevin.lmdb DatomKVTxData KVTxData]
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
        txs        [(DatomKVTxData. 1000 insert-avg true)
                    (DatomKVTxData. 1001 delete-avg false)]
        expanded   (l/expand-datom-kv-txs txs)
        ^Indexable insert-i (.-k ^KVTxData (nth expanded 0))
        ^Indexable delete-i (.-k ^KVTxData (nth expanded 2))]
    (is (= 4 (count expanded)))
    (is (every? #(instance? KVTxData %) expanded))
    (is (= (assoc (b/pr-indexable insert-avg) 0 1000)
           (b/pr-indexable insert-i)))
    (is (= (assoc (b/pr-indexable delete-avg) 0 1001)
           (b/pr-indexable delete-i)))
    (is (= [[:put c/ave insert-i 1000 :avg :id]
            [:put c/eav 1000 insert-i :id :avg]
            [:del-list c/ave delete-i [1001] :avg :id]
            [:del-list c/eav 1001 [delete-i] :id :avg]]
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
