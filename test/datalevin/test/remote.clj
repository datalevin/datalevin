(ns datalevin.test.remote
  (:require
   [clojure.test :refer [deftest is]]
   [datalevin.client :as client]
   [datalevin.interface :as i]
   [datalevin.remote :as remote])
  (:import
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
