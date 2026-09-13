(ns datalevin-tpcc.txns
  "TPC-C-derived transactions for Datalevin.

  Each write transaction performs all reads and writes inside one explicit
  Datalevin transaction. The transaction-bound connection keeps shared totals,
  counters, and dependent reads consistent while concurrent terminals wait."
  (:require
   [datalevin.core :as d])
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
  (first (d/q '[:find ?e ?discount ?credit ?data :in $ ?w ?d ?c
                :where
                [?e :customer/w-id ?w]
                [?e :customer/d-id ?d]
                [?e :customer/id ?c]
                [?e :customer/discount ?discount]
                [?e :customer/credit ?credit]
                [?e :customer/data ?data]]
              db w d c)))

(defn- customer-by-last
  "The middle customer by c_first with the given last name in the district."
  [db w d last-name]
  (let [rows (d/q '[:find ?e ?discount ?credit ?data ?first
                    :in $ ?w ?d ?last
                    :where
                    [?e :customer/w-id ?w]
                    [?e :customer/d-id ?d]
                    [?e :customer/last ?last]
                    [?e :customer/discount ?discount]
                    [?e :customer/credit ?credit]
                    [?e :customer/data ?data]
                    [?e :customer/first ?first]]
                  db w d last-name)
        sorted (sort-by #(nth % 4) rows)]
    (when (seq sorted)
      (nth sorted (quot (count sorted) 2)))))

(defn- item [db i-id]
  (first (d/q '[:find ?e ?price :in $ ?i
                :where [?e :item/id ?i] [?e :item/price ?price]]
              db i-id)))

(defn- stock [db w i-id]
  (first (d/q '[:find ?e ?qty ?ytd ?ocnt ?rcnt :in $ ?w ?i
                :where
                [?e :stock/w-id ?w]
                [?e :stock/i-id ?i]
                [?e :stock/quantity ?qty]
                [?e :stock/ytd ?ytd]
                [?e :stock/order-cnt ?ocnt]
                [?e :stock/remote-cnt ?rcnt]]
              db w i-id)))

;; ---------------------------------------------------------------------------
;; New-Order

(defn new-order!
  [conn {:keys [w d c ol]}]
  (d/with-transaction [conn conn]
    (let [db                   (d/db conn)
          [_ _ _w-tax]          (warehouse db w)
          [d-eid _ _d-tax d-next] (district db w d)
          [_c-eid _c-disc]      (customer-by-id db w d c)
          line-data            (mapv (fn [{:keys [i-id supply-w qty]}]
                                       {:i-id i-id :supply-w supply-w :qty qty
                                        :item (item db i-id)
                                        :stock (stock db supply-w i-id)})
                                     ol)
          missing              (some #(when (nil? (:item %)) %) line-data)]
      (cond
        (nil? d-eid)
        (throw (ex-info "No such district" {:w w :d d}))

        ;; A single invalid item rolls the whole New-Order back.
        missing
        {:type :new-order :status :invalid-item :w w :d d :i-id (:i-id missing)}

        :else
        (let [o-id      d-next
              all-local (long (if (every? #(= w (:supply-w %)) line-data) 1 0))
              ;; A TPC-C order may repeat an item id. Aggregate by stock row so
              ;; each row receives exactly one quantity update per order.
              stock-aggs (reduce
                          (fn [m {:keys [i-id supply-w qty stock]}]
                            (let [k [supply-w i-id]]
                              (-> m
                                  (update-in [k :qty] (fnil + 0) qty)
                                  (update-in [k :lines] (fnil inc 0))
                                  (assoc-in [k :stock] stock)
                                  (assoc-in [k :remote?] (not= supply-w w)))))
                          {} line-data)
              stock-txs (mapcat
                         (fn [[[_ _] {:keys [qty lines stock remote?]}]]
                           (let [[s-eid s-qty s-ytd s-ocnt s-rcnt] stock
                                 qty   (long qty)
                                 new-q (if (>= (long s-qty) qty)
                                         (- (long s-qty) qty)
                                         (+ (- (long s-qty) qty) 91))]
                             [[:db/add s-eid :stock/quantity new-q]
                              [:db/add s-eid :stock/ytd (+ (long s-ytd) qty)]
                              [:db/add s-eid :stock/order-cnt
                               (+ (long s-ocnt) (long lines))]
                              [:db/add s-eid :stock/remote-cnt
                               (if remote? (inc (long s-rcnt)) (long s-rcnt))]]))
                         stock-aggs)
              line-txs  (map-indexed
                         (fn [n {:keys [i-id supply-w qty item]}]
                           (let [[_ price] item
                                 amount (* (double qty) (double price))]
                             {:db/id (- (+ 3 n)) :order-line/o-id o-id
                              :order-line/d-id d :order-line/w-id w
                              :order-line/number (inc n) :order-line/i-id i-id
                              :order-line/supply-w-id supply-w
                              :order-line/quantity (long qty)
                              :order-line/amount amount
                              :order-line/dist-info "distinfo-distinfo-distinfo"}))
                         line-data)
              tx (into
                  [[:db/add d-eid :district/next-o-id (inc d-next)]
                   {:db/id -1 :orders/id o-id :orders/d-id d :orders/w-id w
                    :orders/c-id c :orders/entry-d (now-str)
                    :orders/ol-cnt (count line-data)
                    :orders/all-local all-local}
                   {:db/id -2 :new-order/o-id o-id :new-order/d-id d
                    :new-order/w-id w}]
                  (concat stock-txs line-txs))]
          (d/transact! conn tx)
          {:type :new-order :status :ok :w w :d d :o-id o-id
           :amount (reduce + 0.0
                           (map #(* (double (:qty %))
                                    (double (second (:item %))))
                                line-data))})))))

;; ---------------------------------------------------------------------------
;; Payment

(defn payment!
  [conn {:keys [w d c last-name amount by-name?]}]
  (d/with-transaction [conn conn]
    (let [db                      (d/db conn)
          [w-eid w-ytd _]         (warehouse db w)
          [d-eid d-ytd _ _]       (district db w d)
          cust                    (if by-name?
                                    (customer-by-last db w d last-name)
                                    (customer-by-id db w d c))]
      (if (nil? cust)
        {:type :payment :status :no-customer :w w :d d}
        (let [[c-eid _ c-credit c-data] cust
              c-id (:customer/id (d/entity db c-eid))
              [cb]  (first (d/q '[:find ?v :in $ ?e
                                  :where [?e :customer/balance ?v]] db c-eid))
              [cyp] (first (d/q '[:find ?v :in $ ?e
                                  :where [?e :customer/ytd-payment ?v]] db c-eid))
              [cpc] (first (d/q '[:find ?v :in $ ?e
                                  :where [?e :customer/payment-cnt ?v]] db c-eid))
              new-data (when (= "BC" c-credit)
                         (let [s (str (format "|%d %d %d %d %s" w d c-id w (now-str))
                                      c-data)]
                           (subs s 0 (min 500 (count s)))))
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
                           :history/data "hdata-hdata-hdata-hdata"}]
                    new-data (conj [:db/add c-eid :customer/data new-data]))]
          (d/transact! conn tx)
          {:type :payment :status :ok :w w :d d :c c-id :amount amount
           :credit c-credit})))))

;; ---------------------------------------------------------------------------
;; Order-Status (read only)

(defn order-status!
  [conn {:keys [w d c last-name by-name?]}]
  (let [db   (d/db conn)
        cust (if by-name?
               (customer-by-last db w d last-name)
               (customer-by-id db w d c))]
    (if (nil? cust)
      {:type :order-status :status :no-customer}
      (let [order (first (d/q '[:find ?e ?id ?entry ?carrier
                                :in $ ?w ?d ?c
                                :where
                                [?e :orders/w-id ?w]
                                [?e :orders/d-id ?d]
                                [?e :orders/c-id ?c]
                                [?e :orders/id ?id]
                                [?e :orders/entry-d ?entry]
                                [(get-else $ ?e :orders/carrier-id -1) ?carrier]]
                              db w d c))]
        (if order
          (let [o-id (second order)
                lines (d/q '[:find ?number :in $ ?w ?d ?o
                             :where
                             [?l :order-line/w-id ?w]
                             [?l :order-line/d-id ?d]
                             [?l :order-line/o-id ?o]
                             [?l :order-line/number ?number]]
                           db w d o-id)]
            {:type :order-status :status :ok :lines (count lines)})
          {:type :order-status :status :no-order})))))

;; ---------------------------------------------------------------------------
;; Delivery

(defn delivery!
  [conn {:keys [w carrier]}]
  (d/with-transaction [conn conn]
    (let [db     (d/db conn)
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
          (d/transact! conn tx)
          {:type :delivery :status :ok :delivered (count plans)})))))

;; ---------------------------------------------------------------------------
;; Stock-Level (read only)

(defn stock-level!
  [conn {:keys [w d threshold]}]
  (let [db   (d/db conn)
        o-id (ffirst (d/q '[:find (max ?o) :in $ ?w ?d
                            :where
                            [?n :new-order/w-id ?w]
                            [?n :new-order/d-id ?d]
                            [?n :new-order/o-id ?o]]
                          db w d))
        low  (if o-id
               (count (d/q '[:find ?i :in $ ?w ?d ?o ?t
                             :where
                             [?l :order-line/w-id ?w]
                             [?l :order-line/d-id ?d]
                             [?l :order-line/o-id ?o]
                             [?l :order-line/i-id ?i]
                             [?s :stock/w-id ?w]
                             [?s :stock/i-id ?i]
                             [?s :stock/quantity ?q]
                             [(< ?q ?t)]]
                           db w d o-id threshold))
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
