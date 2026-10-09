(ns datalevin.commit-map-growth-test
  (:require [clojure.test :refer [deftest is testing]]
            [datalevin.binding.cpp.write :as write]
            [datalevin.core :as d]
            [datalevin.kv.txlog :as kvtx]
            [datalevin.tx-group.phase :as phase]
            [datalevin.util :as u])
  (:import [datalevin.cpp Util$MapFullException]))

(deftest commit-map-growth-replays-frozen-rows-with-the-same-token
  (doseq [fault [:metadata :commit]]
    (testing (name fault)
      (let [dir (u/tmp-dir (str "commit-map-growth-" (random-uuid)))
            opts {:wal? true :wal-durability-profile :strict
                  :wal-segment-prealloc? false :snapshot-scheduler? false
                  :background-sampling? false}
            conn (d/create-conn dir {} opts)
            original kvtx/write-batch-commit-metadata!
            tokens (atom [])
            bodies (atom 0)
            appends (atom 0)
            unobserve (phase/observe!
                        (fn [event _]
                          (when (= :wal-complete event) (swap! appends inc))))]
        (try
          (with-redefs [kvtx/write-batch-commit-metadata!
                        (fn [wdb state token]
                          (swap! tokens conj token)
                          (if (= 1 (count @tokens))
                            (if (= fault :commit)
                              ;; Exercise close-native-write!'s map-full path.
                              (do
                                (original wdb state token)
                                (throw (Util$MapFullException. "one-shot commit fault")))
                              ;; Metadata's ordinary write grows the map and
                              ;; resets the writer before throwing :resized.
                              (with-redefs [write/transact*
                                            (fn [& _]
                                              (throw (Util$MapFullException.
                                                       "one-shot metadata fault")))]
                                (original wdb state token)))
                            (original wdb state token)))]
            (d/with-transaction [tx conn]
              (swap! bodies inc)
              (d/transact! tx [{:db/id 1 :value 42}]))
            (is (= 1 @bodies))
            (is (= 1 @appends))
            (is (= 2 (count @tokens)))
            (is (some? (first @tokens)))
            (is (identical? (first @tokens) (second @tokens)))
            (is (= 42 (:value (d/entity @conn 1)))))
          ;; A subsequent write proves the collector was not fenced.
          (d/transact! conn [{:db/id 2 :value 43}])
          (is (= 43 (:value (d/entity @conn 2))))
          (d/close conn)
          (let [reopened (d/create-conn dir {} opts)]
            (try
              (is (= 42 (:value (d/entity @reopened 1))))
              (is (= 43 (:value (d/entity @reopened 2))))
              (finally (d/close reopened))))
          (finally
            (unobserve)
            (d/close conn)
            (u/delete-files dir)))))))
