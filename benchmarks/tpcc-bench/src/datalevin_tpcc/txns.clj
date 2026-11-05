(ns datalevin-tpcc.txns
  "TPC-C-derived transactions using business-key lookups and ordered indexes.
  All transactions except the intentionally relaxed Stock-Level read run inside
  one explicit transaction, including Order-Status's dependent reads."
  (:require
   [datalevin.core :as d]
   [datalevin-tpcc.common :as common]
   [datalevin-tpcc.schema :as schema])
  (:import [java.time Instant]))

(defn- now-str [] (str (Instant/now)))

(defn- customer-ref [db w district customer last-name by-name?]
  (if by-name?
    ;; Select the lower median from the surname index, without reading balances
    ;; or the large C_DATA payload of every matching customer.
    (some->> (d/datoms db :ave :customer/name-key [w district last-name])
             (map :e)
             (d/pull-many db [:db/id :customer/id :customer/first])
             (sort-by (juxt :customer/first :customer/id))
             common/middle-customer
             :db/id)
    [:customer/key [w district customer]]))

(defn- edge
  "Read at most one index entry and reject a neighboring district/customer."
  [db attr prefix backwards?]
  (let [bound (conj prefix (if backwards? Long/MAX_VALUE Long/MIN_VALUE))
        datom (first ((if backwards? d/rseek-datoms d/seek-datoms)
                      db :ave attr bound nil 1))]
    (when (and (= attr (:a datom))
               (= prefix (pop (:v datom))))
      datom)))

(defn- order-lines [db w district order pattern]
  (d/pull-many db pattern
               (map :e (d/index-range db :order-line/key
                                      [w district order 0]
                                      [w district order Long/MAX_VALUE]))))

(defn- new-order-tx!
  "Execute on a transaction-bound connection. An optional observer can inspect
  the staged work immediately before an invalid item aborts the transaction."
  [tx {:keys [w d c ol]} before-rollback]
  (let [db (d/db tx)
        warehouse (d/pull db [:warehouse/tax] [:warehouse/id w])
        district (d/pull db [:db/id :district/tax :district/next-o-id]
                         [:district/key [w d]])
        customer (d/pull db [:customer/discount :customer/last :customer/credit]
                         [:customer/key [w d c]])
        o-id (:district/next-o-id district)
        entry (now-str)
        dist-attr (common/stock-dist-attr d)
        stock-pattern [:db/id dist-attr :stock/quantity :stock/ytd
                       :stock/order-cnt :stock/remote-cnt :stock/data]
        all-local (long (if (every? #(= w (:supply-w %)) ol) 1 0))]
    (when (nil? (:db/id district))
      (throw (ex-info "No such district" {:w w :d d})))
    (d/transact!
     tx
     [[:db/add (:db/id district) :district/next-o-id (inc o-id)]
      {:db/id -1 :orders/id o-id :orders/d-id d :orders/w-id w
       :orders/c-id c :orders/entry-d entry
       :orders/ol-cnt (count ol) :orders/all-local all-local}
      {:db/id -2 :new-order/o-id o-id :new-order/d-id d :new-order/w-id w}])
    ;; Preserve valid-prefix work before rollback and read our own stock writes
    ;; for repeated items. No item prevalidation or cached stock snapshots.
    (loop [lines (seq ol) number 1 results []]
      (if-let [{:keys [i-id supply-w qty]} (first lines)]
        (let [db (d/db tx)]
          (if-let [item (d/pull db [:item/price :item/name :item/data] [:item/id i-id])]
            (let [stock (d/pull db stock-pattern [:stock/key [supply-w i-id]])
                  [quantity ytd ocnt rcnt]
                  (common/stock-after-lines
                   ((juxt :stock/quantity :stock/ytd :stock/order-cnt :stock/remote-cnt) stock)
                   [qty] (not= supply-w w))
                  result (common/new-order-line-result
                          number i-id supply-w qty (:item/name item)
                          (:stock/quantity stock) (:item/price item)
                          (:item/data item) (:stock/data stock))
                  s-eid (:db/id stock)]
              (d/transact!
               tx
               [[:db/add s-eid :stock/quantity quantity]
                [:db/add s-eid :stock/ytd ytd]
                [:db/add s-eid :stock/order-cnt ocnt]
                [:db/add s-eid :stock/remote-cnt rcnt]
                {:db/id -1 :order-line/o-id o-id :order-line/d-id d
                 :order-line/w-id w :order-line/number number
                 :order-line/i-id i-id :order-line/supply-w-id supply-w
                 :order-line/quantity (long qty) :order-line/amount (:amount result)
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
         (:district/tax district) results)))))

(defn new-order! [conn input]
  (d/with-transaction [tx conn]
    (new-order-tx! tx input nil)))

(def ^:private warehouse-payment-pattern
  (into [:db/id :warehouse/ytd] (schema/attrs "warehouse" common/payment-location-fields)))
(def ^:private district-payment-pattern
  (into [:db/id :district/ytd] (schema/attrs "district" common/payment-location-fields)))
(def ^:private customer-payment-pattern
  (into [:db/id] (schema/attrs "customer" common/payment-customer-fields)))

(defn- row-values [table fields row]
  (zipmap fields (map row (schema/attrs table fields))))

(defn payment!
  [conn {:keys [w d c c-w c-d last-name amount by-name?]}]
  (d/with-transaction [tx conn]
    (let [db (d/db tx)
          c-w (or c-w w)
          c-d (or c-d d)
          warehouse (d/pull db warehouse-payment-pattern [:warehouse/id w])
          district (d/pull db district-payment-pattern [:district/key [w d]])
          ref (customer-ref db c-w c-d c last-name by-name?)
          customer (when ref (d/pull db customer-payment-pattern ref))]
      (if-not customer
        {:type :payment :status :no-customer :w w :d d}
        (let [c-eid (:db/id customer)
              c-id (:customer/id customer)
              date (now-str)
              new-data (when (= "BC" (:customer/credit customer))
                         (common/bad-credit-data
                          c-id c-d c-w d w amount
                          (:customer/data (d/pull db [:customer/data] c-eid))))
              updates
              (cond-> [[:db/add (:db/id warehouse) :warehouse/ytd
                        (+ (:warehouse/ytd warehouse) (double amount))]
                       [:db/add (:db/id district) :district/ytd
                        (+ (:district/ytd district) (double amount))]
                       [:db/add c-eid :customer/balance
                        (- (:customer/balance customer) (double amount))]
                       [:db/add c-eid :customer/ytd-payment
                        (+ (:customer/ytd-payment customer) (double amount))]
                       [:db/add c-eid :customer/payment-cnt
                        (inc (:customer/payment-cnt customer))]
                       {:db/id -1 :history/c-id c-id :history/c-d-id c-d
                        :history/c-w-id c-w :history/d-id d :history/w-id w
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

(defn order-status!
  [conn {:keys [w d c last-name by-name?]}]
  ;; A DB value alone does not pin an LMDB snapshot across multiple API calls.
  (d/with-transaction [tx conn]
    (let [db (d/db tx)
          ref (customer-ref db w d c last-name by-name?)
          customer (when ref
                     (d/pull db [:customer/id :customer/balance :customer/first
                                 :customer/middle :customer/last] ref))]
      (if-not customer
        {:type :order-status :status :no-customer}
        (if-let [latest (edge db :orders/customer-key [w d (:customer/id customer)] true)]
          (let [order (d/pull db [:orders/id :orders/entry-d :orders/carrier-id] (:e latest))
                lines (order-lines db w d (:orders/id order)
                                   [:order-line/number :order-line/i-id :order-line/supply-w-id
                                    :order-line/quantity :order-line/amount :order-line/delivery-d])]
            (common/order-status-result
             ((juxt :customer/id :customer/balance :customer/first :customer/middle :customer/last) customer)
             ((juxt :orders/id :orders/entry-d :orders/carrier-id) order)
             (map (juxt :order-line/number :order-line/i-id :order-line/supply-w-id
                        :order-line/quantity :order-line/amount :order-line/delivery-d) lines)))
          {:type :order-status :status :no-order})))))

(defn delivery! [conn {:keys [w carrier]}]
  (d/with-transaction [tx conn]
    (let [db (d/db tx)
          districts (d/index-range db :district/key [w 0] [w Long/MAX_VALUE])
          plans (into [] (keep (fn [district]
                                 (let [did (peek (:v district))]
                                   (when-let [oldest (edge db :new-order/key [w did] false)]
                                     [did oldest])))) districts)
          now (now-str)
          updates
          (into []
                (mapcat
                 (fn [[did oldest]]
                   (let [o-id (peek (:v oldest))
                         order (d/pull db [:db/id :orders/c-id] [:orders/key [w did o-id]])
                         lines (order-lines db w did o-id [:db/id :order-line/amount])
                         customer (d/pull db [:db/id :customer/balance :customer/delivery-cnt]
                                          [:customer/key [w did (:orders/c-id order)]])]
                     (into [[:db.fn/retractEntity (:e oldest)]
                            [:db/add (:db/id order) :orders/carrier-id carrier]
                            [:db/add (:db/id customer) :customer/balance
                             (+ (:customer/balance customer) (reduce + 0.0 (map :order-line/amount lines)))]
                            [:db/add (:db/id customer) :customer/delivery-cnt
                             (inc (:customer/delivery-cnt customer))]]
                           (map (fn [line] [:db/add (:db/id line) :order-line/delivery-d now]))
                           lines)))) plans)]
      (when (seq updates) (d/transact! tx updates))
      {:type :delivery :status :ok :delivered (count plans)})))

(def ^:private district-next-order-query
  '[:find [?next]
    :in $ ?district-key
    :where [?district :district/key ?district-key]
    [?district :district/next-o-id ?next]])

(def ^:private stock-level-query
  '[:find (count-distinct ?item) .
    :in $ ?w [?line ...] ?threshold
    :where [?line :order-line/i-id ?item]
    [(tuple ?w ?item) ?stock-key]
    [(tuple :stock/key ?stock-key) ?stock]
    [?stock :stock/quantity ?quantity]
    [(< ?quantity ?threshold)]])

(defn stock-level! [conn {:keys [w d threshold]}]
  (let [db (d/db conn)
        next-id (first (d/q district-next-order-query db [w d]))]
    {:type :stock-level :status :ok
     :low-stock
     (if next-id
       ;; Supply only the last 20 orders' line IDs to a fixed query. Tuple
       ;; :in bounds currently do not become index ranges in the planner.
       ;; The stock join, quantity filter, and distinct count run in the DB.
       (let [lines (mapv :e (d/index-range db :order-line/key
                                          [w d (- next-id 20) 0]
                                          [w d next-id 0]))]
         (or (d/q stock-level-query db w lines threshold) 0))
       0)}))

;; Verification helpers run while benchmark terminals are stopped.
(defn district-next-o-id [conn w district]
  (:district/next-o-id (d/pull (d/db conn) [:district/next-o-id] [:district/key [w district]])))

(defn new-order-count [conn w district]
  (count (d/index-range (d/db conn) :new-order/key [w district 0] [w district Long/MAX_VALUE])))

(defn order-count [conn w district]
  (count (d/index-range (d/db conn) :orders/key [w district 0] [w district Long/MAX_VALUE])))
