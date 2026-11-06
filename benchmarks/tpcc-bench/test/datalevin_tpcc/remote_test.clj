(ns datalevin-tpcc.remote-test
  (:require [clojure.test :refer [deftest is]]
            [datalevin.core :as d]
            [datalevin.server :as server]
            [datalevin.util :as u]
            [datalevin-tpcc.check :as check]
            [datalevin-tpcc.datalevin :as bench]
            [datalevin-tpcc.generate :as g]
            [datalevin-tpcc.txns :as tx]
            [datalevin-tpcc.schema :as schema]
            [datalevin-tpcc.test-data :as td])
  (:import [java.net ServerSocket]))

(deftest separate-remote-terminal-connections-preserve-mixed-transaction-accounting
  (let [root (u/tmp-dir (str "tpcc-remote-" (random-uuid)))
        port (with-open [socket (ServerSocket. 0)] (.getLocalPort socket))
        srv (server/create {:root root :port port :verbose false
                            :transaction-lock-timeout-ms 60000})
        uri (str "dtlv://datalevin:datalevin@127.0.0.1:" port "/tpcc")
        connections (atom [])]
    (try
      (server/start srv)
      (let [conn (d/create-conn uri bench/schema
                                {:wal? true :wal-durability-profile :strict
                                 :wal-sync-mode :fsync :client-opts {:pool-size 1}})]
        (swap! connections conj conn)
        (let [customer (first (g/customer-rows 42 1))
              stock (first (g/stock-rows 42 1))
              rows {"warehouse" (g/warehouse-rows 42 2)
                    "district" (g/district-rows 42 2)
                    "customer" (for [w [1 2]] (assoc customer 0 1 1 1 2 w))
                    "item" (take 2 (g/item-rows 42))
                    "stock" (for [w [1 2] item [1 2]] (assoc stock 0 item 1 w 2 100))}
              layout (schema/entity-layout 2)]
          (with-out-str
            (doseq [[table rows] rows] (bench/load-table! @conn table rows layout))))
        (let [peers (mapv (fn [_] (#'bench/open-bench-conn uri)) (range 4))
              _ (swap! connections into peers)
              _ (is (apply distinct? (map #(System/identityHashCode %) peers)))
              _ (is (true? (:wal? (d/opts conn))))
              before (check/payment-state conn)
              order {:w 1 :d 1 :c 1 :ol [{:i-id 1 :supply-w 1 :qty 2}
                                         {:i-id 2 :supply-w 2 :qty 3}]}
              payment {:w 1 :d 1 :c 1 :c-w 2 :c-d 1 :amount 5.0 :by-name? false}
              start (promise)
              jobs [(fn [c] (tx/new-order! c order))
                    (fn [c] (tx/new-order! c (assoc-in order [:ol 1 :i-id] 100001)))
                    (fn [c] (tx/payment! c payment))
                    (fn [c] (tx/payment! c payment))]
              workers (mapv (fn [c f] (future @start (f c))) peers jobs)]
          (deliver start true)
          (let [results (mapv #(try @% (catch Throwable e e)) workers)]
            (doseq [result results :when (instance? Throwable result)]
              (println "REMOTE TRANSACTION ERROR" (ex-message result) (ex-data (ex-cause result))))
            (is (= [:ok :invalid-item :ok :ok] (mapv :status results))))
          (is (= 3002 (tx/district-next-o-id conn 1 1)))
          (is (= 1 (tx/order-count conn 1 1)))
          (is (empty? (check/payment-errors before (check/payment-state conn)
                                            {[1 1] {:amount 10.0 :count 2}}
                                            {[2 1] {:amount 10.0 :count 2}})))
          (is (= 98 (:stock/quantity (d/pull @conn [:stock/quantity] (td/lookup-ref @conn "stock" [1 1])))))
          (is (= 97 (:stock/quantity (d/pull @conn [:stock/quantity] (td/lookup-ref @conn "stock" [2 2])))))
          (is (= 2 (:lines (tx/order-status! conn {:w 1 :d 1 :c 1}))))
          (is (= 2 (:low-stock (tx/stock-level! conn {:w 1 :d 1 :threshold 101}))))
          (is (= 1 (:delivered (tx/delivery! conn {:w 1 :carrier 2}))))
          (is (= 0 (tx/new-order-count conn 1 1)))
          ;; Force the aborted wrapper to close while a different terminal owns
          ;; the writer. Its completion must neither fail nor commit that peer.
          (let [aborted (promise) owned (promise) release (promise)
                a (future
                    (d/with-transaction [tx (peers 0)]
                      (d/abort-transact tx)
                      (deliver aborted true)
                      @owned
                      :aborted))
                b (future
                    @aborted
                    (d/with-transaction [tx (peers 1)]
                      (d/transact! tx [[:db/add (td/lookup-ref @conn "stock" [1 1]) :stock/quantity 96]])
                      (deliver owned true)
                      @release
                      :committed))]
            (try
              (is (= :aborted (deref a 10000 :timeout)))
              (is (= 98 (:stock/quantity (d/pull @conn [:stock/quantity] (td/lookup-ref @conn "stock" [1 1])))))
              (finally (deliver release true)))
            (is (= :committed @b))
            (is (= 96 (:stock/quantity (d/pull @conn [:stock/quantity] (td/lookup-ref @conn "stock" [1 1]))))))))
      (finally
        (doseq [conn @connections] (d/close conn))
        (server/stop srv)
        (u/delete-files root)))))
