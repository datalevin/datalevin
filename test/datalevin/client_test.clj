;;
;; Copyright (c) Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns datalevin.client-test
  (:require
   [clojure.test :refer [deftest is use-fixtures]]
   [datalevin.client :as cl]
   [datalevin.core :as d]
   [datalevin.test.core :refer [server-fixture]])
  (:import
   [java.util UUID]))

(use-fixtures :each server-fixture)

(defn- ha-write-rejected-response
  [message endpoints]
  {:type :error-response
   :message message
   :err-data {:error :ha/write-rejected
              :reason :not-leader
              :retryable? true
              :ha-retry-endpoints endpoints}})

(defn- mk-client
  []
  (cl/->Client "user" "pw" "10.0.0.12" 8898 1 1000 nil nil))

(deftest normal-request-reopen-works-with-single-connection-pool-test
  (let [db-name (str "reopen-pool-" (UUID/randomUUID))
        uri     (str "dtlv://datalevin:datalevin@localhost/" db-name)
        conn    (d/create-conn uri {}
                               {:client-opts {:pool-size 1
                                              :time-out 5000}})]
    (try
      (d/transact! conn [{:db/id -1 :name "v1"}])
      (let [client (cl/new-client "dtlv://datalevin:datalevin@localhost"
                                  {:pool-size 1
                                   :time-out 5000})]
        (try
          (let [watermarks (cl/normal-request
                             client
                             :txlog-watermarks
                             [db-name]
                             false)
                records    (cl/normal-request
                             client
                             :open-tx-log-rows
                             [db-name 1 32]
                             false)]
            (is (map? watermarks))
            (is (pos? (long (or (:last-applied-lsn watermarks) 0))))
            (is (seq records))
            (is (sequential? (:rows (first records)))))
          (finally
            (cl/disconnect client))))
      (finally
        (d/close conn)))))

(deftest closing-one-remote-conn-keeps-server-store-open-test
  (let [db-name (str "close-shared-store-" (UUID/randomUUID))
        uri     (str "dtlv://datalevin:datalevin@localhost/" db-name)
        schema  {:name {:db/valueType :db.type/string}}
        conn1   (d/create-conn uri schema {:client-opts {:time-out 5000}})
        conn2   (d/create-conn uri schema {:client-opts {:time-out 5000}})]
    (try
      (d/transact! conn1 [{:db/id -1 :name "v1"}])
      (is (= #{["v1"]}
             (d/q '[:find ?n
                    :where
                    [?e :name ?n]]
                  @conn2)))
      (d/close conn2)
      (d/transact! conn1 [{:db/id -2 :name "v2"}])
      (is (= #{"v1" "v2"}
             (set (map first
                       (d/q '[:find ?n
                              :where
                              [?e :name ?n]]
                            @conn1)))))
      (finally
        (d/close conn1)))))

(deftest retry-ha-write-request-successful-failover-test
  (let [req          {:type :transact-kv
                      :args ["orders" [[:put "k" "v"]]]
                      :writing? true}
        created      (atom [])
        disconnected (atom [])
        calls        (atom [])
        winner       (atom nil)
        result       (#'cl/retry-ha-write-request*
                      req
                      "HA write admission rejected"
                      (:err-data
                       (ha-write-rejected-response
                        "HA write admission rejected"
                        ["10.0.0.11:8898"]))
                      {:host "10.0.0.12" :port 8898}
                      (fn [client _]
                        (swap! calls conj client)
                        {:type :command-complete :result :ok})
                      (fn [client]
                        (swap! disconnected conj client))
                      (fn [_ host port]
                        (let [client {:endpoint (str host ":" port)}]
                          (swap! created conj [host port])
                          client))
                      (fn [endpoint]
                        (reset! winner endpoint)))]
    (is (= :ok result))
    (is (= [["10.0.0.11" 8898]] @created))
    (is (= [{:endpoint "10.0.0.11:8898"}] @calls))
    (is (= [{:endpoint "10.0.0.11:8898"}] @disconnected))
    (is (= "10.0.0.11:8898" @winner))))

(deftest retry-ha-write-request-exhaustion-captures-attempts-test
  (let [req          {:type :transact-kv
                      :args ["orders" [[:put "k" "v"]]]
                      :writing? true}
        created      (atom [])
        disconnected (atom [])
        ex           (try
                       (#'cl/retry-ha-write-request*
                        req
                        "HA write admission rejected"
                        (:err-data
                         (ha-write-rejected-response
                          "HA write admission rejected"
                          ["10.0.0.11:8898" "10.0.0.13:8898"]))
                        {:host "10.0.0.12" :port 8898}
                        (fn [client _]
                          (if (= "10.0.0.13:8898" (:endpoint client))
                            (ha-write-rejected-response "still not leader" [])
                            (throw (ex-info "connection refused" {}))))
                        (fn [client]
                          (swap! disconnected conj client))
                        (fn [_ host port]
                          (let [client {:endpoint (str host ":" port)}]
                            (swap! created conj [host port])
                            client)))
                       nil
                       (catch Exception e e))]
    (is (instance? clojure.lang.ExceptionInfo ex))
    (is (= [["10.0.0.11" 8898] ["10.0.0.13" 8898]] @created))
    (is (= [{:endpoint "10.0.0.11:8898"}
            {:endpoint "10.0.0.13:8898"}]
           @disconnected))
    (is (= [{:endpoint "10.0.0.11:8898"
             :type :exception
             :message "connection refused"}
            {:endpoint "10.0.0.13:8898"
             :type :error-response
             :reason :not-leader}]
           (:ha-retry-attempts (ex-data ex))))))

(deftest preferred-ha-write-request-success-test
  (let [client       (mk-client)
        req          {:type :transact-kv
                      :args ["orders" [[:put "k" "v"]]]
                      :writing? true}
        created      (atom [])
        disconnected (atom [])
        retried?     (atom false)]
    (#'cl/set-preferred-ha-endpoint! client "10.0.0.11:8898")
    (let [res (#'cl/try-preferred-ha-write-request*
               client
               req
               {:host "10.0.0.12" :port 8898}
               (fn [retry-client _]
                 {:type :command-complete
                  :result [:ok (:endpoint retry-client)]})
               (fn [retry-client]
                 (swap! disconnected conj retry-client))
               (fn [_ host port]
                 (let [retry-client {:endpoint (str host ":" port)}]
                   (swap! created conj [host port])
                   retry-client))
               (fn [& _]
                 (reset! retried? true)
                 :should-not-run))]
      (is (= {:handled? true
              :result [:ok "10.0.0.11:8898"]}
             res))
      (is (= [["10.0.0.11" 8898]] @created))
      (is (= [{:endpoint "10.0.0.11:8898"}] @disconnected))
      (is (false? @retried?))
      (is (= "10.0.0.11:8898"
             (#'cl/read-preferred-ha-endpoint client))))))

(deftest preferred-ha-write-request-exception-clears-stickiness-test
  (let [client (mk-client)
        req    {:type :transact-kv
                :args ["orders" [[:put "k" "v"]]]
                :writing? true}]
    (#'cl/set-preferred-ha-endpoint! client "10.0.0.11:8898")
    (is (= {:handled? false}
           (#'cl/try-preferred-ha-write-request*
            client
            req
            {:host "10.0.0.12" :port 8898}
            (fn [& _] {:type :command-complete :result :ok})
            (fn [& _] nil)
            (fn [& _]
              (throw (ex-info "connection failed" {})))
            (fn [& _] :unused))))
    (is (nil? (#'cl/read-preferred-ha-endpoint client)))))

(deftest preferred-ha-write-request-invalid-endpoint-clears-stickiness-test
  (let [client (mk-client)
        req    {:type :transact-kv
                :args ["orders" [[:put "k" "v"]]]
                :writing? true}]
    (#'cl/set-preferred-ha-endpoint! client "bad-endpoint")
    (is (= {:handled? false}
           (#'cl/try-preferred-ha-write-request*
            client
            req
            {:host "10.0.0.12" :port 8898}
            (fn [& _] {:type :command-complete :result :ok})
            (fn [& _] nil)
            (fn [& _] (throw (ex-info "should-not-run" {})))
            (fn [& _] :unused))))
    (is (nil? (#'cl/read-preferred-ha-endpoint client)))))

(deftest retry-success-learns-preferred-endpoint-for-next-write-test
  (let [client            (mk-client)
        req               {:type :transact-kv
                           :args ["orders" [[:put "k" "v"]]]
                           :writing? true}
        retry-created     (atom [])
        pref-created      (atom [])
        pref-disconnected (atom [])
        retried?          (atom false)]
    (is (= :ok
           (#'cl/retry-ha-write-request*
            req
            "HA write admission rejected"
            (:err-data
             (ha-write-rejected-response
              "HA write admission rejected"
              ["10.0.0.11:8898"]))
            {:host "10.0.0.12" :port 8898}
            (fn [_ _] {:type :command-complete :result :ok})
            (fn [_] nil)
            (fn [_ host port]
              (let [retry-client {:endpoint (str host ":" port)}]
                (swap! retry-created conj [host port])
                retry-client))
            #( #'cl/set-preferred-ha-endpoint! client %))))
    (is (= [["10.0.0.11" 8898]] @retry-created))
    (is (= "10.0.0.11:8898" (#'cl/read-preferred-ha-endpoint client)))
    (is (= {:handled? true
            :result [:ok "10.0.0.11:8898"]}
           (#'cl/try-preferred-ha-write-request*
            client
            req
            {:host "10.0.0.12" :port 8898}
            (fn [retry-client _]
              {:type :command-complete :result [:ok (:endpoint retry-client)]})
            (fn [retry-client]
              (swap! pref-disconnected conj retry-client))
            (fn [_ host port]
              (let [retry-client {:endpoint (str host ":" port)}]
                (swap! pref-created conj [host port])
                retry-client))
            (fn [& _]
              (reset! retried? true)
              :should-not-run))))
    (is (= [["10.0.0.11" 8898]] @pref-created))
    (is (= [{:endpoint "10.0.0.11:8898"}] @pref-disconnected))
    (is (false? @retried?))))

(deftest normal-request-non-writing-error-includes-server-payload-test
  (let [client (reify cl/IClient
                 (request [_ _]
                   (ha-write-rejected-response
                    "HA write admission rejected"
                    ["10.0.0.11:8898"]))
                 (copy-in [_ _ _ _] nil)
                 (disconnect [_] nil)
                 (disconnected? [_] false)
                 (get-pool [_] nil)
                 (get-id [_] nil))
        ex     (try
                 (#'cl/normal-request client :list-databases [] false)
                 nil
                 (catch Exception e e))]
    (is (instance? clojure.lang.ExceptionInfo ex))
    (is (= :ha/write-rejected (get-in (ex-data ex) [:err-data :error])))
    (is (= "HA write admission rejected"
           (:server-message (ex-data ex))))))

(deftest normal-request-write-failover-uses-preferred-route-after-learn-test
  (let [req               {:type :transact-kv
                           :args ["orders" [[:put "k" "v"]]]
                           :writing? true}
        base-request-cnt  (atom 0)
        preferred-cnt     (atom 0)
        retry-cnt         (atom 0)
        sticky-learned?   (atom false)
        client            (reify cl/IClient
                            (request [_ _]
                              (swap! base-request-cnt inc)
                              (ha-write-rejected-response
                               "HA write admission rejected"
                               ["10.0.0.11:8898"]))
                            (copy-in [_ _ _ _] nil)
                            (disconnect [_] nil)
                            (disconnected? [_] false)
                            (get-pool [_] nil)
                            (get-id [_] nil))]
    (with-redefs [cl/client-retry-context
                  (fn [_] {:host "10.0.0.12" :port 8898})
                  cl/try-preferred-ha-write-request*
                  (fn [& _]
                    (swap! preferred-cnt inc)
                    (when @sticky-learned?
                      {:handled? true :result :ok}))
                  cl/retry-ha-write-request
                  (fn [_ _ _ _]
                    (swap! retry-cnt inc)
                    (reset! sticky-learned? true)
                    :ok)]
      (is (= :ok (#'cl/normal-request client (:type req) (:args req) true)))
      (is (= :ok (#'cl/normal-request client (:type req) (:args req) true)))
      (is (= 1 @base-request-cnt))
      (is (= 2 @preferred-cnt))
      (is (= 1 @retry-cnt)))))
