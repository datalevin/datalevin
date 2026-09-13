(ns datalevin-tpcc.txns-test
  (:require
   [clojure.test :refer [deftest is testing use-fixtures]]
   [datalevin.core :as d]
   [datalevin.util :as u]
   [datalevin-tpcc.check :as check]
   [datalevin-tpcc.datalevin :as bench]
   [datalevin-tpcc.txns :as t])
  (:import
   [java.lang.management ManagementFactory ThreadInfo]))

(def ^:dynamic *test-dir* nil)

(use-fixtures :each
  (fn [f]
    (let [dir (u/tmp-dir (str "datalevin-tpcc-test-" (random-uuid)))]
      (try
        (binding [*test-dir* dir] (f))
        (finally
          (when (.exists (u/file dir))
            (u/delete-files dir)))))))

(defn- customer-eid [district customer]
  (+ 100 (* 10 district) customer))

(defn- with-db [opts f]
  (let [conn (d/get-conn (str *test-dir* "/" (random-uuid)) bench/schema opts)]
    (try
      (d/transact!
       conn
       (into [{:db/id 1 :warehouse/id 1 :warehouse/ytd 300000.0 :warehouse/tax 0.1}
              {:db/id 2 :item/id 1 :item/price 2.0}
              {:db/id 3 :stock/w-id 1 :stock/i-id 1 :stock/quantity 100
               :stock/ytd 0 :stock/order-cnt 0 :stock/remote-cnt 0}]
             (concat
              (for [did (range 1 11)]
                {:db/id (+ 10 did) :district/id did :district/w-id 1
                 :district/ytd 30000.0 :district/tax 0.1 :district/next-o-id 3001})
              (for [did (range 1 11) cid [1 2]]
                {:db/id (customer-eid did cid) :customer/id cid
                 :customer/w-id 1 :customer/d-id did
                 :customer/first (str cid) :customer/last (str "CUSTOMER" cid)
                 :customer/discount 0.0 :customer/credit (if (= cid 2) "BC" "GC")
                 :customer/data "initial" :customer/balance -10.0
                 :customer/ytd-payment 10.0 :customer/payment-cnt 1
                 :customer/delivery-cnt 0})
              (for [did (range 1 11) cid [1 2]]
                {:history/c-id cid :history/c-w-id 1 :history/c-d-id did
                 :history/w-id 1 :history/d-id did :history/date "initial"
                 :history/amount 10.0 :history/data "initial"}))))
      (f conn)
      (finally (d/close conn)))))

(defn- value [conn eid attr]
  (get (d/entity (d/db conn) eid) attr))

(defn- concurrently
  "Hold the connection until every worker reaches its transaction boundary.
  In the old implementation both workers have already read the shared totals
  at this point. In the fixed implementation they wait before those reads."
  [conn jobs]
  (let [threads (mapv (fn [_] (promise)) jobs)
        bean (ManagementFactory/getThreadMXBean)
        futures
        (locking conn
          (let [fs (mapv (fn [job thread]
                           (future
                             (deliver thread (Thread/currentThread))
                             (job)))
                         jobs threads)
                deadline (+ (System/nanoTime) 10000000000)]
            (is
             (loop []
               (if (every?
                    (fn [thread]
                      (when (realized? thread)
                        (let [^ThreadInfo info (.getThreadInfo bean (.getId ^Thread @thread))]
                          (and info
                               (= Thread$State/BLOCKED (.getThreadState info))
                               (= (System/identityHashCode conn)
                                  (some-> info .getLockInfo .getIdentityHashCode))))))
                    threads)
                 true
                 (when (< (System/nanoTime) deadline)
                   (Thread/sleep 1)
                   (recur))))
             "all workers reached the connection's transaction boundary")
            fs))]
    (mapv deref futures)))

(defn- payment [district customer]
  {:w 1 :d district :c customer :amount 5.0 :by-name? false})

(defn- committed-payments [results]
  (reduce (fn [m {:keys [w d amount]}]
            (-> m
                (update-in [[w d] :amount] (fnil + 0.0) amount)
                (update-in [[w d] :count] (fnil inc 0))))
          {} results))

(deftest concurrent-payments-preserve-accounting
  (doseq [kv-opts [{:wal? false}
                  {:wal? true :wal-durability-profile :strict}
                  {:wal? true :wal-durability-profile :relaxed}]
          inputs [[(payment 1 1) (payment 1 2)]
                  [(payment 1 1) (payment 2 2)]
                  [(payment 1 1) (payment 1 1)]]]
    (testing (str kv-opts " " inputs)
      (with-db {:kv-opts kv-opts}
        (fn [conn]
          (let [before (check/payment-state conn)
                results (concurrently conn (mapv #(fn [] (t/payment! conn %)) inputs))]
            (is (= [:ok :ok] (mapv :status results)))
            (is (= 300010.0 (value conn 1 :warehouse/ytd)))
            (is (empty? (check/payment-errors before (check/payment-state conn)
                                              (committed-payments results))))
            (doseq [[[did cid] n] (frequencies (map (juxt :d :c) inputs))]
              (let [eid (customer-eid did cid)]
                (is (= (- -10.0 (* 5.0 n)) (value conn eid :customer/balance)))
                (is (= (+ 10.0 (* 5.0 n)) (value conn eid :customer/ytd-payment)))
                (is (= (inc n) (value conn eid :customer/payment-cnt)))))
            (is (= 2 (- (reduce + (vals (:history-count (check/payment-state conn))))
                        (reduce + (vals (:history-count before))))))))))))

(deftest payment-by-name-records-selected-customer
  (with-db {}
    (fn [conn]
      (let [before (check/payment-state conn)
            result (t/payment! conn (assoc (payment 1 1)
                                          :by-name? true :last-name "CUSTOMER2"))]
        (is (= 2 (:c result)))
        (is (= -15.0 (value conn (customer-eid 1 2) :customer/balance)))
        (is (= -10.0 (value conn (customer-eid 1 1) :customer/balance)))
        (is (= #{[2]} (d/q '[:find ?c
                             :where [?h :history/amount 5.0] [?h :history/c-id ?c]]
                           (d/db conn))))
        (is (empty? (check/payment-errors before (check/payment-state conn)
                                          (committed-payments [result]))))))))

(def order-input
  {:w 1 :d 1 :c 1 :ol [{:i-id 1 :supply-w 1 :qty 2}]})

(deftest payment-and-delivery-preserve-customer-balance
  (with-db {}
    (fn [conn]
      (t/new-order! conn order-input)
      (let [before (check/payment-state conn)
            [pay delivery] (concurrently conn [#(t/payment! conn (payment 1 1))
                                                #(t/delivery! conn {:w 1 :carrier 1})])]
        (is (= :ok (:status pay)))
        (is (= 1 (:delivered delivery)))
        (is (= -11.0 (value conn (customer-eid 1 1) :customer/balance)))
        (is (= 1 (value conn (customer-eid 1 1) :customer/delivery-cnt)))
        (is (empty? (check/payment-errors before (check/payment-state conn)
                                          (committed-payments [pay]))))))))

(deftest concurrent-new-orders-and-deliveries
  (with-db {}
    (fn [conn]
      (let [results (concurrently conn [#(t/new-order! conn order-input)
                                        #(t/new-order! conn order-input)])]
        (is (= #{3001 3002} (set (map :o-id results))))
        (is (= 3003 (t/district-next-o-id conn 1 1)))
        (is (= 2 (t/order-count conn 1 1)))
        (is (= 96 (value conn 3 :stock/quantity)))
        (is (= 4 (value conn 3 :stock/ytd)))
        (is (= 2 (value conn 3 :stock/order-cnt))))
      (let [results (concurrently conn [#(t/delivery! conn {:w 1 :carrier 1})
                                        #(t/delivery! conn {:w 1 :carrier 2})])]
        (is (= [1 1] (mapv :delivered results)))
        (is (= 0 (t/new-order-count conn 1 1)))
        (is (= -2.0 (value conn (customer-eid 1 1) :customer/balance)))
        (is (= 2 (value conn (customer-eid 1 1) :customer/delivery-cnt)))))))

(deftest failed-payment-rolls-back-and-propagates
  (with-db {}
    (fn [conn]
      (let [before (check/payment-state conn)
            calls (atom 0)]
        (d/listen! conn :fail-payment
                   (fn [_]
                     (swap! calls inc)
                     (throw (ex-info "injected transaction failure" {}))))
        (is (thrown-with-msg? clojure.lang.ExceptionInfo #"injected transaction failure"
                             (t/payment! conn (payment 1 1))))
        (d/unlisten! conn :fail-payment)
        (is (= 1 @calls) "transaction errors are not swallowed and retried")
        (is (= before (check/payment-state conn)))
        (is (= -10.0 (value conn (customer-eid 1 1) :customer/balance)))))))

(deftest payment-checks-detect-lost-totals
  (with-db {}
    (fn [conn]
      (let [before (check/payment-state conn)
            results [(t/payment! conn (payment 1 1)) (t/payment! conn (payment 1 2))]
            expected (committed-payments results)]
        (is (empty? (check/payment-errors before (check/payment-state conn) expected)))
        ;; Recreate the original lost update while preserving both history rows.
        (d/transact! conn [[:db/add 1 :warehouse/ytd 300005.0]
                           [:db/add 11 :district/ytd 30005.0]])
        (is (= #{:warehouse-ytd :district-ytd}
               (set (map :invariant
                         (check/payment-errors before (check/payment-state conn)
                                               expected)))))))))

(deftest stock-level-examines-last-20-orders
  (with-db {}
    (fn [conn]
      ;; District 1 has :district/next-o-id 3001, so the TPC-C 2.8 window is
      ;; [2981, 3001). Item 1 is low and appears in every order of the window,
      ;; so it must be counted once, not once per order. Item 3 is low but its
      ;; only order (2980) is outside the window and must not be counted.
      (d/transact!
       conn
       (concat
        [{:db/id 50 :item/id 2 :item/price 3.0}
         {:db/id 51 :item/id 3 :item/price 4.0}
         {:db/id 52 :stock/w-id 1 :stock/i-id 2 :stock/quantity 300
          :stock/ytd 0 :stock/order-cnt 0 :stock/remote-cnt 0}
         {:db/id 53 :stock/w-id 1 :stock/i-id 3 :stock/quantity 50
          :stock/ytd 0 :stock/order-cnt 0 :stock/remote-cnt 0}]
        (mapcat (fn [oid]
                  [{:db/id (+ 100000 oid) :order-line/o-id oid :order-line/d-id 1
                    :order-line/w-id 1 :order-line/i-id 1 :order-line/number 1}
                   {:db/id (+ 200000 oid) :order-line/o-id oid :order-line/d-id 1
                    :order-line/w-id 1 :order-line/i-id 2 :order-line/number 2}])
                (range 2981 3001))
        [{:db/id 300001 :order-line/o-id 2980 :order-line/d-id 1
          :order-line/w-id 1 :order-line/i-id 3 :order-line/number 1}]))
      (is (= 1 (:low-stock (t/stock-level! conn {:w 1 :d 1 :threshold 200})))))))

(deftest order-status-selects-latest-order
  (with-db {}
    (fn [conn]
      ;; Customer 1 in district 1 has three orders; the most recent (30) has
      ;; three lines, while the older ones have fewer. Order-Status must
      ;; inspect order 30 rather than an arbitrary order from the result set.
      (d/transact!
       conn
       (concat
        [{:db/id 500010 :orders/id 10 :orders/d-id 1 :orders/w-id 1
          :orders/c-id 1 :orders/entry-d "2026-01-01T00:00:00"
          :orders/ol-cnt 1 :orders/all-local 1}
         {:db/id 500020 :orders/id 20 :orders/d-id 1 :orders/w-id 1
          :orders/c-id 1 :orders/entry-d "2026-01-01T00:00:00"
          :orders/ol-cnt 2 :orders/all-local 1}
         {:db/id 500030 :orders/id 30 :orders/d-id 1 :orders/w-id 1
          :orders/c-id 1 :orders/entry-d "2026-01-01T00:00:00"
          :orders/ol-cnt 3 :orders/all-local 1}]
        (mapcat (fn [[oid lines]]
                  (for [n (range 1 (inc lines))]
                    {:db/id (+ 400000 (* oid 10) n)
                     :order-line/o-id oid :order-line/d-id 1
                     :order-line/w-id 1 :order-line/number n
                     :order-line/i-id 1}))
                [[10 1] [20 2] [30 3]])))
      (is (= 3 (:lines (t/order-status! conn {:w 1 :d 1 :c 1})))))))
