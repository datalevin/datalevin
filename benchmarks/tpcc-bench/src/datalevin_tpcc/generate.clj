(ns datalevin-tpcc.generate
  "Deterministic TPC-C population.

  Each table draws from its own seeded PRNG, so generation is reproducible and
  independent of the order in which tables are loaded. The field widths and
  value ranges follow the TPC-C specification closely; exact dictionary text
  is not reproduced, which does not affect transaction semantics."
  (:require
   [datalevin-tpcc.common :as c])
  (:import
   [java.util ArrayList Collections Random]))

(def ^:private alphabet
  "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")

(defn- rint ^long [^Random r ^long lo ^long hi]
  (+ lo (.nextInt r (inc (- hi lo)))))

(defn- rand-string [^Random r ^long min-len ^long max-len]
  (let [n  (rint r min-len max-len)
        sb (StringBuilder.)]
    (dotimes [_ n]
      (.append sb (.charAt alphabet (.nextInt r (count alphabet)))))
    (.toString sb)))

(defn- rand-numeric [^Random r ^long min-len ^long max-len]
  (let [n  (rint r min-len max-len)
        sb (StringBuilder.)]
    (dotimes [_ n]
      (.append sb (char (+ (int \0) (.nextInt r 10)))))
    (.toString sb)))

(defn- maybe-original [^Random r ^String s]
  (if (= 1 (rint r 1 10))
    (let [pos (.nextInt r (- (count s) 7))]
      (str (subs s 0 pos) "ORIGINAL" (subs s (+ pos 8))))
    s))

(def ^:private syllables
  ["BAR" "OUGHT" "ABLE" "PRI" "PRES" "ESE" "ANTI" "CALLY" "ATION" "EING"])

(defn last-name-from-index [^long n]
  (str (nth syllables (quot n 100))
       (nth syllables (mod (quot n 10) 10))
       (nth syllables (mod n 10))))

(def last-names
  "The 1000 TPC-C customer last names, indexed 0..999."
  (mapv last-name-from-index (range 1000)))

(defn- customer-last
  "TPC-C 4.3.3: the first 1000 customers in each district carry the 1000
  distinct standard names, one each. The remaining 2000 draw from the same
  name pool. Without this, name-based Payment and Order-Status lookups miss
  and take the :no-customer path."
  [^Random r ^long c-id]
  (if (<= c-id 1000)
    (nth last-names (dec c-id))
    (nth last-names (rint r 0 999))))

(defn- address-row [^Random r]
  [(rand-string r 10 20) (rand-string r 10 20) (rand-string r 10 20)
   (rand-string r 2 2) (rand-numeric r 4 4)])

(defn- base-time [^long n]
  (format "2026-01-01T%02d:%02d:%02d"
          (mod (quot n 3600) 24) (mod (quot n 60) 60) (mod n 60)))

;; ---------------------------------------------------------------------------
;; Independent tables

(defn item-rows [seed]
  (let [r (Random. (+ seed 11))]
    (map (fn [i]
           (let [nm (rand-string r 14 24)]
             [i (rint r 1 10000) nm
              (/ (rint r 100 10000) 100.0)
              (maybe-original r (rand-string r 26 50))]))
         (range 1 (inc c/item-count)))))

(defn warehouse-rows [seed w]
  (let [r (Random. (+ seed 12))]
    (for [id (range 1 (inc w))]
      (let [[s1 s2 city state zip] (address-row r)]
        [id 300000.00 (/ (rint r 0 2000) 10000.0)
         (rand-string r 6 10) s1 s2 city state zip]))))

(defn district-rows [seed w]
  (let [r (Random. (+ seed 13))]
    (for [wid (range 1 (inc w))
          did (range 1 (inc c/districts-per-warehouse))]
      (let [[s1 s2 city state zip] (address-row r)]
        [did wid 30000.00 (/ (rint r 0 2000) 10000.0) 3001
         (rand-string r 6 10) s1 s2 city state zip]))))

(defn stock-rows [seed w]
  (let [r (Random. (+ seed 14))]
    (for [wid (range 1 (inc w))
          iid (range 1 (inc c/item-count))]
      (into [iid wid (rint r 10 100)]
            (concat
             (repeatedly 10 #(rand-string r 24 24))
             [(long 0) (long 0) (long 0)
              (maybe-original r (rand-string r 26 50))])))))

(defn customer-rows [seed w]
  (let [r (Random. (+ seed 15))]
    (for [wid (range 1 (inc w))
          did (range 1 (inc c/districts-per-warehouse))
          cid (range 1 (inc c/customers-per-district))]
      (let [[s1 s2 city state zip] (address-row r)]
        [cid did wid
         (rand-string r 8 16) "OE" (customer-last r cid)
         s1 s2 city state zip
         (rand-numeric r 16 16)
         "2026-01-01T00:00:00" (if (= 1 (rint r 1 10)) "BC" "GC")
         50000.00 (/ (rint r 0 5000) 10000.0)
         -10.00 10.00 (long 1) (long 0)
         (rand-string r 300 500)]))))

(defn history-rows [seed w]
  (let [r (Random. (+ seed 16))]
    (for [wid (range 1 (inc w))
          did (range 1 (inc c/districts-per-warehouse))
          cid (range 1 (inc c/customers-per-district))]
      [cid did wid did wid "2026-01-01T00:00:00" 10.00
       (rand-string r 12 24)])))

;; ---------------------------------------------------------------------------
;; Orders share one pass because they are generated together per district.

(defn order-data
  "Return {:orders [...] :new_order [...] :order_line [...]} for `w`
  warehouses. Each district's 3000 orders map bijectively to its 3000
  customers, as the specification requires."
  [seed w]
  (let [r          (Random. (+ seed 17))
        orders     (ArrayList.)
        new-orders (ArrayList.)
        lines      (ArrayList.)]
    (doseq [wid (range 1 (inc w))
            did (range 1 (inc c/districts-per-warehouse))]
      (let [custs (ArrayList. ^java.util.Collection
                              (vec (range 1 (inc c/customers-per-district))))]
        (Collections/shuffle custs r)
        (doseq [oid (range 1 (inc c/initial-orders-per-district))]
          (let [cid     (nth custs (dec oid))
                ol-cnt  (rint r 5 15)
                entry   (base-time (+ (* (dec oid) 60) (* (dec did) 7)))
                carrier (when (<= oid 2100) (rint r 1 10))]
            (.add orders [oid did wid cid entry carrier ol-cnt (long 1)])
            (when (> oid 2100)
              (.add new-orders [oid did wid]))
            (dotimes [i ol-cnt]
              (let [ln         (inc i)
                    iid        (rint r 1 c/item-count)
                    delivered? (<= oid 2100)]
                (.add lines
                      [oid did wid ln iid wid
                       (when delivered? entry)
                       5
                       (if delivered? 0.00 (/ (rint r 1 999999) 100.0))
                       (rand-string r 24 24)])))))))
    {:orders     (vec orders)
     :new_order  (vec new-orders)
     :order_line (vec lines)}))

(defn population-rows
  "Map of table name to a sequence of row vectors."
  [seed w]
  (let [{:keys [orders new_order order_line]} (order-data seed w)]
    (array-map
     "item"       (item-rows seed)
     "warehouse"  (warehouse-rows seed w)
     "stock"      (stock-rows seed w)
     "district"   (district-rows seed w)
     "customer"   (customer-rows seed w)
     "history"    (history-rows seed w)
     "orders"     orders
     "new_order"  new_order
     "order_line" order_line)))
