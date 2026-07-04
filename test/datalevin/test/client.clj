(ns datalevin.test.client
  (:require
   [clojure.test :refer [deftest is]]
   [datalevin.client :as client])
  (:import
   [java.util.concurrent ConcurrentHashMap]))

(deftest ha-retry-does-not-start-endpoint-after-deadline-test
  (let [attempt-contexts (atom [])
        req              {:type :q
                          :args ["db"]
                          :writing? false}
        err-data         {:error :ha/read-rejected
                          :retryable? true
                          :ha-retry-endpoints ["127.0.0.1:19001"
                                               "127.0.0.1:19002"]}
        retry-context    {:host "127.0.0.1"
                          :port 19000
                          :time-out 1000
                          :ha-write-retry-timeout-ms 50
                          :ha-write-retry-delay-ms 0}
        thrown           (try
                           (#'client/retry-ha-write-request*
                            req
                            "HA read admission rejected"
                            err-data
                            retry-context
                            (fn [_client _req]
                              (Thread/sleep 80)
                              {:type :error-response
                               :message "HA read admission rejected"
                               :err-data err-data})
                            (fn [_client] nil)
                            (fn [context _host _port]
                              (swap! attempt-contexts conj context)
                              ::client))
                           nil
                           (catch Exception e
                             e))]
    (is (some? thrown))
    (is (= 1 (count @attempt-contexts)))
    (is (<= 1
            (:time-out (first @attempt-contexts))
            50))
    (is (<= 1
            (:ha-write-retry-timeout-ms (first @attempt-contexts))
            50))))

(deftest ha-retry-recreates-cached-client-when-timeout-budget-shrinks-test
  (let [base-client   (Object.)
        cached-client (client/->Client nil nil "127.0.0.1" 19001
                                       1 1000 nil nil)
        clients       (atom [])
        disconnected  (atom [])
        endpoint      "127.0.0.1:19001"
        req           {:type :unknown}
        err-data      {:error :ha/read-rejected
                       :retryable? true
                       :ha-retry-endpoints [endpoint]}
        retry-context {:host "127.0.0.1"
                       :port 19000
                       :client base-client
                       :time-out 50
                       :ha-write-retry-timeout-ms 50
                       :ha-write-retry-delay-ms 0}]
    (let [^ConcurrentHashMap cache (#'client/retry-client-cache base-client)]
      (.put cache endpoint cached-client))
    (is (= ::ok
           (#'client/retry-ha-write-request*
            req
            "HA read admission rejected"
            err-data
            retry-context
            (fn [request-client _req]
              (swap! clients conj request-client)
              {:type :command-complete
               :result ::ok})
            (fn [request-client]
              (swap! disconnected conj request-client))
            (fn [_context _host _port]
              ::fresh-client))))
    (is (= [::fresh-client] @clients))
    (is (= [cached-client] @disconnected))))
