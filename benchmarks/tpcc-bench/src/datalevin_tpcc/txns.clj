(ns datalevin-tpcc.txns
  "TPC-C transactions over normalized entities linked by references.
  All except the intentionally relaxed Stock-Level read use one explicit
  transaction, including Order-Status's dependent reads."
  (:require [datalevin.core :as d]
            [datalevin-tpcc.common :as common]
            [datalevin-tpcc.schema :as schema])
  (:import [java.time Instant]))

(defn- now-str [] (str (Instant/now)))

(defn- warehouse-id [db w] (d/entid db [:warehouse/id w]))

(defn- district-id [db w district]
  (when-let [warehouse (warehouse-id db w)]
    (d/entid db [:district/key [warehouse district]])))

(defn- customer-ref [db district customer last-name by-name?]
  (when district
    (if by-name?
      (some->> (d/q '[:find ?e ?first ?id :in $ ?key
                      :where [?e :customer/name-key ?key]
                      [?e :customer/first ?first] [?e :customer/id ?id]]
                    db [district last-name])
               (sort-by (juxt second peek))
               common/middle-customer first)
      [:customer/key [district customer]])))

(defn- edge
  "Read at most one index entry and reject a neighboring district/customer."
  [db attr prefix backwards?]
  (let [bound (conj prefix (if backwards? Long/MAX_VALUE Long/MIN_VALUE))
        datom (first ((if backwards? d/rseek-datoms d/seek-datoms)
                      db :ave attr bound nil 1))]
    (when (and (= attr (:a datom)) (= prefix (pop (:v datom)))) datom)))

(defn- new-order-tx!
  "Execute on a transaction-bound connection. The observer sees staged writes
  before an invalid item aborts; every valid prefix is actually performed."
  [tx {:keys [w d c ol]} before-rollback]
  (let [db (d/db tx)
        warehouse (d/pull db [:db/id :warehouse/tax] [:warehouse/id w])
        w-eid (:db/id warehouse)
        district (when w-eid
                   (d/pull db [:db/id :district/tax :district/next-o-id]
                           [:district/key [w-eid d]]))
        d-eid (:db/id district)
        customer (when d-eid
                   (d/pull db [:db/id :customer/discount :customer/last :customer/credit]
                           [:customer/key [d-eid c]]))
        o-id (:district/next-o-id district)
        entry (now-str)
        dist-attr (common/stock-dist-attr d)
        stock-pattern [:db/id dist-attr :stock/quantity :stock/ytd
                       :stock/order-cnt :stock/remote-cnt :stock/data]
        all-local (long (if (every? #(= w (:supply-w %)) ol) 1 0))]
    (when (nil? d-eid) (throw (ex-info "No such district" {:w w :d d})))
    (let [report (d/transact!
                   tx
                   [[:db/add d-eid :district/next-o-id (inc o-id)]
                    {:db/id -1 :orders/id o-id :orders/district d-eid
                     :orders/customer (:db/id customer) :orders/entry-d entry
                     :orders/ol-cnt (count ol) :orders/all-local all-local}
                    {:db/id -2 :new-order/order -1 :new-order/key [d-eid o-id]}])
          order (get-in report [:tempids -1])]
      ;; Keep the inserted entity ID. Lines link straight to it, without
      ;; resolving the order's business key for each insert.
      (loop [lines (seq ol) number 1 results []]
        (if-let [{:keys [i-id supply-w qty]} (first lines)]
          (let [db (d/db tx)]
            (if-let [item (d/pull db [:db/id :item/price :item/name :item/data] [:item/id i-id])]
              (let [supply (if (= supply-w w) w-eid (warehouse-id db supply-w))
                    stock (d/pull db stock-pattern [:stock/key [supply (:db/id item)]])
                    [quantity ytd ocnt rcnt]
                    (common/stock-after-lines
                      ((juxt :stock/quantity :stock/ytd :stock/order-cnt :stock/remote-cnt) stock)
                      [qty] (not= supply-w w))
                    result (common/new-order-line-result
                             number i-id supply-w qty (:item/name item)
                             (:stock/quantity stock) (:item/price item)
                             (:item/data item) (:stock/data stock))
                    s-eid (:db/id stock)]
                (d/transact! tx
                  [[:db/add s-eid :stock/quantity quantity]
                   [:db/add s-eid :stock/ytd ytd]
                   [:db/add s-eid :stock/order-cnt ocnt]
                   [:db/add s-eid :stock/remote-cnt rcnt]
                   {:db/id -1 :order-line/order order :order-line/number number
                    :order-line/stock s-eid :order-line/quantity (long qty)
                    :order-line/amount (:amount result)
                    :order-line/dist-info (get stock dist-attr)}])
                (recur (next lines) (inc number) (conj results result)))
              (do
                (when before-rollback (before-rollback db))
                (d/abort-transact tx)
                (common/new-order-rollback-result
                  w d c o-id i-id (:customer/last customer) (:customer/credit customer)))))
          (common/new-order-result
            w d c o-id entry (:customer/last customer) (:customer/credit customer)
            (:customer/discount customer) (:warehouse/tax warehouse)
            (:district/tax district) results))))))

(defn new-order! [conn input]
  (d/with-transaction [tx conn] (new-order-tx! tx input nil)))

(def ^:private warehouse-payment-pattern
  (into [:db/id :warehouse/ytd] (schema/attrs "warehouse" common/payment-location-fields)))
(def ^:private district-payment-pattern
  (into [:db/id :district/ytd] (schema/attrs "district" common/payment-location-fields)))
(def ^:private customer-payment-pattern
  (into [:db/id] (schema/attrs "customer" common/payment-customer-fields)))

(defn- row-values [table fields row]
  (zipmap fields (map row (schema/attrs table fields))))

(defn payment! [conn {:keys [w d c c-w c-d last-name amount by-name?]}]
  (d/with-transaction [tx conn]
    (let [db (d/db tx) c-w (or c-w w) c-d (or c-d d)
          warehouse (d/pull db warehouse-payment-pattern [:warehouse/id w])
          district (d/pull db district-payment-pattern [:district/key [(:db/id warehouse) d]])
          customer-district (if (and (= c-w w) (= c-d d)) (:db/id district)
                              (district-id db c-w c-d))
          ref (customer-ref db customer-district c last-name by-name?)
          customer (when ref (d/pull db customer-payment-pattern ref))]
      (if-not customer
        {:type :payment :status :no-customer :w w :d d}
        (let [c-eid (:db/id customer) c-id (:customer/id customer) date (now-str)
              new-data (when (= "BC" (:customer/credit customer))
                         (common/bad-credit-data
                           c-id c-d c-w d w amount
                           (:customer/data (d/pull db [:customer/data] c-eid))))
              updates
              (cond-> [[:db/add (:db/id warehouse) :warehouse/ytd
                        (+ (:warehouse/ytd warehouse) (double amount))]
                       [:db/add (:db/id district) :district/ytd
                        (+ (:district/ytd district) (double amount))]
                       [:db/add c-eid :customer/balance (- (:customer/balance customer) (double amount))]
                       [:db/add c-eid :customer/ytd-payment (+ (:customer/ytd-payment customer) (double amount))]
                       [:db/add c-eid :customer/payment-cnt (inc (:customer/payment-cnt customer))]
                       {:db/id -1 :history/customer c-eid :history/district (:db/id district)
                        :history/date date :history/amount (double amount)
                        :history/data (common/payment-history-data
                                        (:warehouse/name warehouse) (:district/name district))}]
                new-data (conj [:db/add c-eid :customer/data new-data]))]
          (d/transact! tx updates)
          (common/payment-result
            w d c-w c-d amount date
            (row-values "warehouse" common/payment-location-fields warehouse)
            (row-values "district" common/payment-location-fields district)
            (row-values "customer" common/payment-customer-fields customer) new-data))))))

(def ^:private order-lines-query
  '[:find ?number ?item ?warehouse ?quantity ?amount ?date
    :in $ [?line ...]
    :order-by [0 :asc]
    :where [?line :order-line/stock ?stock]
    [?stock :stock/item ?i] [?i :item/id ?item]
    [?stock :stock/warehouse ?w] [?w :warehouse/id ?warehouse]
    [?line :order-line/number ?number] [?line :order-line/quantity ?quantity]
    [?line :order-line/amount ?amount]
    [(get-else $ ?line :order-line/delivery-d "") ?date]])

(defn order-status! [conn {:keys [w d c last-name by-name?]}]
  (d/with-transaction [tx conn]
    (let [db (d/db tx)
          ref (customer-ref db (district-id db w d) c last-name by-name?)
          customer (when ref
                     (d/pull db [:db/id :customer/id :customer/balance :customer/first
                                 :customer/middle :customer/last] ref))]
      (if-not customer
        {:type :order-status :status :no-customer}
        (if-let [latest (edge db :orders/customer-key [(:db/id customer)] true)]
          (let [order (d/pull db [:orders/id :orders/entry-d :orders/carrier-id] (:e latest))
                ;; Bind the small line set before projecting its reference
                ;; paths. This uses grouped entity reads without planning a
                ;; new join graph for every order number.
                line-ids (mapv :e (d/datoms db :ave :order-line/order (:e latest)))
                lines (d/q order-lines-query db line-ids)]
            (common/order-status-result
              ((juxt :customer/id :customer/balance :customer/first :customer/middle :customer/last) customer)
              ((juxt :orders/id :orders/entry-d :orders/carrier-id) order)
              (map #(update % 5 (fn [date] (when (seq date) date))) lines)))
          {:type :order-status :status :no-order})))))

(defn delivery! [conn {:keys [w carrier]}]
  (d/with-transaction [tx conn]
    (let [db (d/db tx)
          warehouse (warehouse-id db w)
          districts (when warehouse (d/datoms db :ave :district/warehouse warehouse))
          plans (into [] (keep #(edge db :new-order/key [(:e %)] false)) districts)
          ;; Project all selected districts together. Each pending entity is
          ;; known, so reference traversal uses bound reads and two queries
          ;; replace the former pair of queries per district.
          headers (when (seq plans)
                    (d/q '[:find ?pending ?order ?customer ?balance ?count
                           :in $ [?pending ...]
                           :where [?pending :new-order/order ?order]
                           [?order :orders/customer ?customer]
                           [?customer :customer/balance ?balance]
                           [?customer :customer/delivery-cnt ?count]]
                         db (mapv :e plans)))
          lines (when (seq headers)
                  (group-by first
                    (d/q '[:find ?order ?line ?amount :in $ [?order ...]
                           :where [?line :order-line/order ?order]
                           [?line :order-line/amount ?amount]]
                         db (mapv second headers))))
          now (now-str)
          updates
          (into []
            (mapcat
              (fn [[pending order customer balance count]]
                (let [order-lines (get lines order)]
                  (into [[:db.fn/retractEntity pending]
                         [:db/add order :orders/carrier-id carrier]
                         [:db/add customer :customer/balance
                          (+ balance (reduce + 0.0 (map #(nth % 2) order-lines)))]
                         [:db/add customer :customer/delivery-cnt (inc count)]]
                        (map (fn [[_ line _]] [:db/add line :order-line/delivery-d now]))
                        order-lines))) headers))]
      (when (seq updates) (d/transact! tx updates))
      {:type :delivery :status :ok :delivered (count plans)})))

(def ^:private district-next-order-query
  '[:find [?district ?next] :in $ ?key
    :where [?district :district/key ?key] [?district :district/next-o-id ?next]])

(def ^:private stock-level-query
  '[:find (count-distinct ?item) .
    :in $ ?warehouse [?order ...] ?threshold
    :where [?line :order-line/order ?order] [?line :order-line/stock ?supply]
    [?supply :stock/item ?item]
    ;; Remote supply identifies the item, but the threshold uses HOME stock.
    [?stock :stock/item ?item] [?stock :stock/warehouse ?warehouse]
    [?stock :stock/quantity ?quantity] [(< ?quantity ?threshold)]])

(defn stock-level! [conn {:keys [w d threshold]}]
  (let [db (d/db conn) warehouse (warehouse-id db w)
        [district next-id] (when warehouse (d/q district-next-order-query db [warehouse d]))]
    {:type :stock-level :status :ok
     :low-stock (if next-id
                  (let [orders (mapv :e (d/index-range db :orders/key
                                          [district (- next-id 20)] [district (dec next-id)]))]
                    (or (d/q stock-level-query db warehouse orders threshold) 0))
                  0)}))

;; Verification helpers run while benchmark terminals are stopped.
(defn district-next-o-id [conn w district]
  (let [db (d/db conn)]
    (when-let [eid (district-id db w district)]
      (:district/next-o-id (d/pull db [:district/next-o-id] eid)))))

(defn new-order-count [conn w district]
  (let [db (d/db conn)]
    (if-let [eid (district-id db w district)]
      (count (d/index-range db :new-order/key [eid 0] [eid Long/MAX_VALUE])) 0)))

(defn order-count [conn w district]
  (let [db (d/db conn)]
    (if-let [eid (district-id db w district)]
      (count (d/datoms db :ave :orders/district eid)) 0)))
