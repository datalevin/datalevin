(ns datalevin-tpcc.txns
  "TPC-C-derived transactions for Datalevin.

  Each write transaction performs all reads and writes inside one explicit
  Datalevin transaction. The transaction-bound connection keeps shared totals,
  counters, and dependent reads consistent while concurrent terminals wait."
  (:require
   [datalevin.core :as d]
   [datalevin-tpcc.common :as common])
  (:import
   [java.time Instant]
   [java.util Random]))

(defn- rint ^long [^Random r ^long lo ^long hi]
  (+ lo (.nextInt r (inc (- hi lo)))))

(defn nurand
  "NURand(A, x, y) as defined by TPC-C, with constant `c` chosen per run."
  [r a x y c]
  (+ x (mod (+ (bit-or (.nextInt r (inc a)) (rint r x y)) c)
            (inc (- y x)))))

(defn- now-str [] (str (Instant/now)))

;; ---------------------------------------------------------------------------
;; Lookups

(defn- warehouse [db w]
  (first (d/q '[:find ?e ?ytd ?tax :in $ ?w
                :where [?e :warehouse/id ?w]
                       [?e :warehouse/ytd ?ytd]
                       [?e :warehouse/tax ?tax]]
              db w)))

(defn- district [db w d]
  (first (d/q '[:find ?e ?ytd ?tax ?next :in $ ?w ?d
                :where
                [?e :district/w-id ?w]
                [?e :district/id ?d]
                [?e :district/ytd ?ytd]
                [?e :district/tax ?tax]
                [?e :district/next-o-id ?next]]
              db w d)))

(defn- customer-by-id [db w d c]
  (first (d/q '[:find ?e ?c ?discount ?credit ?data :in $ ?w ?d ?c
                :where
                [?e :customer/w-id ?w]
                [?e :customer/d-id ?d]
                [?e :customer/id ?c]
                [?e :customer/discount ?discount]
                [?e :customer/credit ?credit]
                [?e :customer/data ?data]]
              db w d c)))

(defn- customer-by-last
  "The middle customer by c_first with the given last name in the district.
  Returns the customer entity and its real business id, not the transaction
  input."
  [db w d last-name]
  (let [rows (d/q '[:find ?e ?id ?discount ?credit ?data ?first
                    :in $ ?w ?d ?last
                    :where
                    [?e :customer/w-id ?w]
                    [?e :customer/d-id ?d]
                    [?e :customer/last ?last]
                    [?e :customer/id ?id]
                    [?e :customer/discount ?discount]
                    [?e :customer/credit ?credit]
                    [?e :customer/data ?data]
                    [?e :customer/first ?first]]
                  db w d last-name)
        sorted (sort-by #(nth % 5) rows)]
    (common/middle-customer sorted)))

(defn- item [db i-id]
  (first (d/q '[:find ?e ?price :in $ ?i
                :where [?e :item/id ?i] [?e :item/price ?price]]
              db i-id)))

(defn- stock [db w i-id district]
  (first (d/q '[:find ?e ?dist ?qty ?ytd ?ocnt ?rcnt
                :in $ ?w ?i ?dist-attr
                :where
                [?e :stock/w-id ?w]
                [?e :stock/i-id ?i]
                [?e ?dist-attr ?dist]
                [?e :stock/quantity ?qty]
                [?e :stock/ytd ?ytd]
                [?e :stock/order-cnt ?ocnt]
                [?e :stock/remote-cnt ?rcnt]]
              db w i-id (common/stock-dist-attr district))))

;; ---------------------------------------------------------------------------
;; New-Order

(defn new-order!
  [conn {:keys [w d c ol]}]
  (d/with-transaction [tx-conn conn]
    (let [db                    (d/db tx-conn)
          [_ _ w-tax]           (warehouse db w)
          [d-eid _ d-tax o-id]   (district db w d)
          [_c-eid _c-id c-disc]  (customer-by-id db w d c)
          all-local             (long (if (every? #(= w (:supply-w %)) ol) 1 0))]
      (when (nil? d-eid)
        (throw (ex-info "No such district" {:w w :d d})))
      (d/transact!
       tx-conn
       [[:db/add d-eid :district/next-o-id (inc o-id)]
        {:db/id -1 :orders/id o-id :orders/d-id d :orders/w-id w
         :orders/c-id c :orders/entry-d (now-str)
         :orders/ol-cnt (count ol) :orders/all-local all-local}
        {:db/id -2 :new-order/o-id o-id :new-order/d-id d :new-order/w-id w}])
      ;; TPC-C 2.4.2.3 requires the valid prefix to perform its writes before
      ;; discovering the invalid item. Prevalidating the order skips that work.
      (loop [lines (seq ol) number 1 subtotal 0.0]
        (if-let [{:keys [i-id supply-w qty]} (first lines)]
          (let [db (d/db tx-conn)]
            (if-let [[_ price] (item db i-id)]
              (let [[s-eid dist-info & values] (stock db supply-w i-id d)
                    [quantity ytd ocnt rcnt]
                    (common/stock-after-lines values [qty] (not= supply-w w))
                    amount (* (double qty) (double price))]
                (d/transact!
                 tx-conn
                 [[:db/add s-eid :stock/quantity quantity]
                  [:db/add s-eid :stock/ytd ytd]
                  [:db/add s-eid :stock/order-cnt ocnt]
                  [:db/add s-eid :stock/remote-cnt rcnt]
                  {:db/id -1 :order-line/o-id o-id :order-line/d-id d
                   :order-line/w-id w :order-line/number number
                   :order-line/i-id i-id :order-line/supply-w-id supply-w
                   :order-line/quantity (long qty) :order-line/amount amount
                   :order-line/dist-info dist-info}])
                (recur (next lines) (inc number) (+ subtotal amount)))
              (do
                (d/abort-transact tx-conn)
                {:type :new-order :status :invalid-item :w w :d d
                 :o-id o-id :i-id i-id})))
          {:type :new-order :status :ok :w w :d d :o-id o-id
           :amount (common/new-order-total subtotal c-disc w-tax d-tax)})))))

;; ---------------------------------------------------------------------------
;; Payment

(defn payment!
  [conn {:keys [w d c last-name amount by-name?]}]
  (d/with-transaction [tx-conn conn]
    (let [db                      (d/db tx-conn)
          [w-eid w-ytd _]         (warehouse db w)
          [d-eid d-ytd _ _]       (district db w d)
          cust                    (if by-name?
                                    (customer-by-last db w d last-name)
                                    (customer-by-id db w d c))]
      (if (nil? cust)
        {:type :payment :status :no-customer :w w :d d}
        (let [[c-eid c-id _ c-credit c-data] cust
              w-name (:warehouse/name (d/entity db w-eid))
              d-name (:district/name (d/entity db d-eid))
              [cb]  (first (d/q '[:find ?v :in $ ?e
                                  :where [?e :customer/balance ?v]] db c-eid))
              [cyp] (first (d/q '[:find ?v :in $ ?e
                                  :where [?e :customer/ytd-payment ?v]] db c-eid))
              [cpc] (first (d/q '[:find ?v :in $ ?e
                                  :where [?e :customer/payment-cnt ?v]] db c-eid))
              new-data (when (= "BC" c-credit)
                         (common/bad-credit-data c-id d w d w amount c-data))
              tx (cond-> [[:db/add w-eid :warehouse/ytd
                           (+ (double w-ytd) (double amount))]
                          [:db/add d-eid :district/ytd
                           (+ (double d-ytd) (double amount))]
                          [:db/add c-eid :customer/balance
                           (- (double cb) (double amount))]
                          [:db/add c-eid :customer/ytd-payment
                           (+ (double cyp) (double amount))]
                          [:db/add c-eid :customer/payment-cnt (inc (long cpc))]
                          {:db/id -1 :history/c-id c-id :history/c-d-id d
                           :history/c-w-id w :history/d-id d :history/w-id w
                           :history/date (now-str) :history/amount (double amount)
                           :history/data (common/payment-history-data w-name d-name)}]
                    new-data (conj [:db/add c-eid :customer/data new-data]))]
          (d/transact! tx-conn tx)
          {:type :payment :status :ok :w w :d d :c c-id :amount amount
           :credit c-credit})))))

;; ---------------------------------------------------------------------------
;; Order-Status (read only)

(defn order-status!
  [conn {:keys [w d c last-name by-name?]}]
  (let [db   (d/db conn)
        rows (d/q '[:find ?id ?balance ?first ?middle ?last
                     :in $ ?w ?d ?attr ?value
                     :where
                     [?e :customer/w-id ?w]
                     [?e :customer/d-id ?d]
                     [?e ?attr ?value]
                     [?e :customer/id ?id]
                     [?e :customer/balance ?balance]
                     [?e :customer/first ?first]
                     [?e :customer/middle ?middle]
                     [?e :customer/last ?last]]
                   db w d (if by-name? :customer/last :customer/id)
                   (if by-name? last-name c))
        cust (if by-name?
               (common/middle-customer (sort-by (juxt #(nth % 2) first) rows))
               (first rows))]
    (if (nil? cust)
      {:type :order-status :status :no-customer}
      (let [c-id (first cust)
            ;; TPC-C 2.6: inspect the customer's most recent order, i.e. the
            ;; greatest order id, matching the SQL backends' ORDER BY o_id DESC.
            o-id (ffirst (d/q '[:find (max ?id)
                                :in $ ?w ?d ?c
                                :where
                                [?e :orders/w-id ?w]
                                [?e :orders/d-id ?d]
                                [?e :orders/c-id ?c]
                                [?e :orders/id ?id]]
                              db w d c-id))]
        (if o-id
          ;; Pull keeps orders and lines whose optional carrier/delivery
          ;; attributes are absent, the equivalent of SQL NULL.
          (let [order (ffirst
                       (d/q '[:find (pull ?e [:orders/id :orders/entry-d
                                              :orders/carrier-id])
                              :in $ ?w ?d ?o
                              :where
                              [?e :orders/w-id ?w]
                              [?e :orders/d-id ?d]
                              [?e :orders/id ?o]]
                            db w d o-id))
                lines (d/q '[:find (pull ?l [:order-line/number :order-line/i-id
                                            :order-line/supply-w-id
                                            :order-line/quantity :order-line/amount
                                            :order-line/delivery-d])
                             :in $ ?w ?d ?o
                             :where
                             [?l :order-line/w-id ?w]
                             [?l :order-line/d-id ?d]
                             [?l :order-line/o-id ?o]]
                           db w d o-id)]
            (common/order-status-result
             cust ((juxt :orders/id :orders/entry-d :orders/carrier-id) order)
             (->> lines
                  (map first)
                  (sort-by :order-line/number)
                  (map (juxt :order-line/number :order-line/i-id
                             :order-line/supply-w-id :order-line/quantity
                             :order-line/amount :order-line/delivery-d)))))
          {:type :order-status :status :no-order})))))

;; ---------------------------------------------------------------------------
;; Delivery

(defn delivery!
  [conn {:keys [w carrier]}]
  (d/with-transaction [tx-conn conn]
    (let [db     (d/db tx-conn)
          dids   (d/q '[:find ?d :in $ ?w
                        :where [?e :district/w-id ?w] [?e :district/id ?d]]
                      db w)
          plans  (for [[did] dids
                       :let [no (ffirst (d/q '[:find (min ?o) :in $ ?w ?d
                                               :where
                                               [?n :new-order/w-id ?w]
                                               [?n :new-order/d-id ?d]
                                               [?n :new-order/o-id ?o]]
                                             db w did))]
                       :when no]
                   [did no])]
      (if (empty? plans)
        {:type :delivery :status :ok :delivered 0}
        (let [now (now-str)
              tx  (into []
                        (mapcat
                         (fn [[did no]]
                           (let [n-eid (ffirst (d/q '[:find ?n :in $ ?w ?d ?o
                                                      :where [?n :new-order/w-id ?w]
                                                             [?n :new-order/d-id ?d]
                                                             [?n :new-order/o-id ?o]]
                                                    db w did no))
                                 [o-eid c-id] (first
                                               (d/q '[:find ?e ?c :in $ ?w ?d ?o
                                                      :where
                                                      [?e :orders/w-id ?w]
                                                      [?e :orders/d-id ?d]
                                                      [?e :orders/id ?o]
                                                      [?e :orders/c-id ?c]]
                                                    db w did no))
                                 lines (d/q '[:find ?l ?amount :in $ ?w ?d ?o
                                              :where
                                              [?l :order-line/w-id ?w]
                                              [?l :order-line/d-id ?d]
                                              [?l :order-line/o-id ?o]
                                              [?l :order-line/amount ?amount]]
                                            db w did no)
                                 total (reduce + 0.0 (map second lines))
                                 [c-eid c-bal c-dc] (first
                                                     (d/q '[:find ?e ?bal ?dc
                                                            :in $ ?w ?d ?c
                                                            :where
                                                            [?e :customer/w-id ?w]
                                                            [?e :customer/d-id ?d]
                                                            [?e :customer/id ?c]
                                                            [?e :customer/balance ?bal]
                                                            [?e :customer/delivery-cnt ?dc]]
                                                          db w did c-id))]
                             (concat
                              [[:db.fn/retractEntity n-eid]]
                              [[:db/add o-eid :orders/carrier-id carrier]]
                              (map (fn [[l-eid _]]
                                     [:db/add l-eid :order-line/delivery-d now])
                                   lines)
                              [[:db/add c-eid :customer/balance
                                (+ (double c-bal) (double total))]]
                              [[:db/add c-eid :customer/delivery-cnt
                                (inc (long c-dc))]])))
                         plans))]
          (d/transact! tx-conn tx)
          {:type :delivery :status :ok :delivered (count plans)})))))

;; ---------------------------------------------------------------------------
;; Stock-Level (read only)

(defn stock-level!
  [conn {:keys [w d threshold]}]
  (let [db      (d/db conn)
        next-id (ffirst (d/q '[:find ?n :in $ ?w ?d
                               :where
                               [?e :district/w-id ?w]
                               [?e :district/id ?d]
                               [?e :district/next-o-id ?n]]
                             db w d))
        low     (if next-id
                  ;; TPC-C 2.8: examine the last 20 orders, [d_next_o_id - 20,
                  ;; d_next_o_id), including orders already delivered.
                  (count (d/q '[:find ?i :in $ ?w ?d ?lo ?hi ?t
                                :where
                                [?l :order-line/w-id ?w]
                                [?l :order-line/d-id ?d]
                                [?l :order-line/o-id ?o]
                                [(>= ?o ?lo)]
                                [(< ?o ?hi)]
                                [?l :order-line/i-id ?i]
                                [?s :stock/w-id ?w]
                                [?s :stock/i-id ?i]
                                [?s :stock/quantity ?q]
                                [(< ?q ?t)]]
                              db w d (- (long next-id) 20) (long next-id)
                              threshold))
                  0)]
    {:type :stock-level :status :ok :low-stock low}))

;; ---------------------------------------------------------------------------
;; Verification helpers

(defn district-next-o-id [conn w d]
  (ffirst (d/q '[:find ?n :in $ ?w ?d
                 :where [?e :district/w-id ?w]
                        [?e :district/id ?d]
                        [?e :district/next-o-id ?n]]
               (d/db conn) w d)))

(defn new-order-count [conn w d]
  (count (d/q '[:find ?o :in $ ?w ?d
                :where [?n :new-order/w-id ?w]
                       [?n :new-order/d-id ?d]
                       [?n :new-order/o-id ?o]]
              (d/db conn) w d)))

(defn order-count [conn w d]
  (count (d/q '[:find ?o :in $ ?w ?d
                :where [?e :orders/w-id ?w]
                       [?e :orders/d-id ?d]
                       [?e :orders/id ?o]]
              (d/db conn) w d)))
