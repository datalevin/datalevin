(ns datalevin.client-routing-test
  (:require
   [clojure.test :refer [deftest is testing]]
   [datalevin.client :as client]))

(def ^:private mutations
  [:add-doc :remove-doc :clear-docs :search-re-index
   :add-vec :remove-vec :clear-vecs :persist-vecs :close-vecs :vec-re-index
   :new-search-engine :new-vector-index])

(defn- request-client [send]
  (reify client/IClient
    (request [_ req] (send req))
    (copy-in [_ _ _ _] nil)
    (disconnect [_] nil)
    (disconnected? [_] false)
    (get-pool [_] nil)
    (get-id [_] nil)))

(deftest index-mutations-use-write-retries-without-selecting-a-transaction-test
  (doseq [op mutations
          writing? [false true]]
    (testing (str op " writing?=" writing?)
      (let [requests (atom [])
            retries  (atom [])
            base     (request-client
                      (fn [req]
                        (swap! requests conj req)
                        {:type :error-response :message "not leader"
                         :err-data {:error :ha/write-rejected
                                    :retryable? true}}))]
        (with-redefs [client/retry-ha-write-request
                      (fn [_ req _ _]
                        (swap! retries conj req)
                        :retried)]
          (binding [client/*ha-read-min-tx* 42]
            (is (= :retried
                   (if writing?
                     (client/normal-request base op ["db"] true)
                     (client/normal-request base op ["db"])))))
          (is (= [{:type op :args ["db"] :writing? writing?}] @requests))
          (is (= @requests @retries)))))))

(deftest transport-failure-only-fails-over-reads-test
  (let [failure (ex-info "connection lost" {})
        pool    (reify client/IConnectionPool
                  (get-connection [_] (throw failure))
                  (release-connection [_ _] nil)
                  (close-pool [_] nil)
                  (closed-pool? [_] false))
        base    (client/->Client "user" "password" "127.0.0.1" 19001
                                 1 1000 nil pool)
        creates (atom 0)
        target  (request-client (constantly {:type :command-complete
                                             :result :read-result}))]
    (#'client/cache-known-ha-db-endpoints! base "db" ["127.0.0.1:19002"])
    (with-redefs [client/new-client-for-endpoint
                  (fn [& _] (swap! creates inc) target)]
      (doseq [op (conj mutations :assoc-opt)]
        (is (identical? failure
                        (try (client/normal-request base op ["db"])
                             (catch Exception e e))) (str op)))
      (is (zero? @creates) "mutations cannot be replayed as failed reads")
      (is (= :read-result (client/normal-request base :doc-count ["db"])))
      (is (= 1 @creates)))))

(deftest reopening-indexes-preserves-options-and-wire-shape-test
  (doseq [[op re-index db-type opts]
          [[:new-search-engine :search-re-index "engine"
            {:domain "custom-search" :include-text? true}]
           [:new-vector-index :vec-re-index "index"
            {:domain "custom-vector" :dimensions 7 :metric-type :euclidean}]]]
    (let [requests (atom [])
          base     (request-client
                    (fn [req]
                      (swap! requests conj req)
                      {:type :command-complete}))
          target   (request-client
                    (fn [req]
                      (swap! requests conj req)
                      {:type :command-complete}))]
      (client/normal-request base op ["db" opts])
      (#'client/inherit-index-options! target base)
      (client/open-database target "db" db-type)
      (is (= {:type op :args ["db" opts]} (last @requests)))
      (let [updated (assoc opts :search-opts {:top 3})]
        (client/normal-request target re-index ["db" updated])
        (client/open-database base "db" db-type)
        (is (= {:type op :args ["db" updated]} (last @requests)))))))
