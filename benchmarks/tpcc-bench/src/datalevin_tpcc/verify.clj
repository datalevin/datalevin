(ns datalevin-tpcc.verify
  "Reconstruct SQL rows from references and compare the complete initial population."
  (:require [clojure.java.io :as io]
            [clojure.pprint :as pp]
            [datalevin.core :as d]
            [datalevin-tpcc.common :as c]
            [datalevin-tpcc.generate :as g]
            [datalevin-tpcc.schema :as schema])
  (:import [java.math BigInteger]
           [java.nio.charset StandardCharsets]
           [java.security MessageDigest]))

(def column-paths
  {:district/w-id [:district/warehouse :warehouse/id]
   :stock/w-id [:stock/warehouse :warehouse/id]
   :stock/i-id [:stock/item :item/id]
   :customer/d-id [:customer/district :district/id]
   :customer/w-id [:customer/district :district/warehouse :warehouse/id]
   :orders/d-id [:orders/district :district/id]
   :orders/w-id [:orders/district :district/warehouse :warehouse/id]
   :orders/c-id [:orders/customer :customer/id]
   :new-order/o-id [:new-order/order :orders/id]
   :new-order/d-id [:new-order/order :orders/district :district/id]
   :new-order/w-id [:new-order/order :orders/district :district/warehouse :warehouse/id]
   :order-line/o-id [:order-line/order :orders/id]
   :order-line/d-id [:order-line/order :orders/district :district/id]
   :order-line/w-id [:order-line/order :orders/district :district/warehouse :warehouse/id]
   :order-line/i-id [:order-line/stock :stock/item :item/id]
   :order-line/supply-w-id [:order-line/stock :stock/warehouse :warehouse/id]
   :history/c-id [:history/customer :customer/id]
   :history/c-d-id [:history/customer :customer/district :district/id]
   :history/c-w-id [:history/customer :customer/district :district/warehouse :warehouse/id]
   :history/d-id [:history/district :district/id]
   :history/w-id [:history/district :district/warehouse :warehouse/id]})

(def table-anchors
  {"warehouse" :warehouse/id "item" :item/id "district" :district/key
   "customer" :customer/key "stock" :stock/key "orders" :orders/key
   "new_order" :new-order/order "order_line" :order-line/key "history" :history/customer})

(defn table-query
  "Include entity identity so identical history rows remain separate. Reference
  paths are shared; SQL NULLs are projected without dropping their entities."
  [table]
  (let [columns (c/attrs table)
        variables (mapv #(symbol (str "?c" %)) (range (count columns)))
        [_ where]
        (reduce
          (fn [[bound where] [attr variable]]
            (let [path (get column-paths attr [attr])
                  [bound where prefix]
                  (reduce (fn [[bound where prefix] ref]
                            (let [next-prefix (conj prefix ref)]
                              (if (contains? bound next-prefix)
                                [bound where next-prefix]
                                (let [next-var (symbol (str "?r" (count bound)))]
                                  [(assoc bound next-prefix next-var)
                                   (conj where [(bound prefix) ref next-var]) next-prefix]))))
                          [bound where []] (butlast path))
                  clause (if (#{:orders/carrier-id :order-line/delivery-d} attr)
                           [(list 'get-else '$ (bound prefix) (peek path) ::missing) variable]
                           [(bound prefix) (peek path) variable])]
              [bound (conj where clause)]))
          [{[] '?e} [['?e (table-anchors table)]]]
          (map vector columns variables))]
    {:find (into ['?e] variables) :where where}))

(defn table-rows [db table]
  (map (fn [row] (mapv #(when-not (= ::missing %) %) (rest row)))
       (d/q (table-query table) db)))

(defn fingerprint
  "Count plus a commutative sum of SHA-256 row digests (mod 2^256). Duplicates
  contribute independently. This compares data irrespective of entity IDs/order."
  [rows]
  (let [digest (MessageDigest/getInstance "SHA-256")
        modulus (.shiftLeft BigInteger/ONE 256)
        [n sum] (reduce (fn [[n ^BigInteger sum] row]
                          [(inc n) (.mod (.add sum (BigInteger. 1 (.digest digest (.getBytes (pr-str row) StandardCharsets/UTF_8))))
                                         modulus)])
                        [0 BigInteger/ZERO] rows)]
    {:rows n :sha256-sum (format "%064x" sum)}))

(defn reference-errors
  "Check all reference targets, order/customer districts, and the derived queue index."
  [db]
  (into []
    (concat
      (keep (fn [[attr [target]]]
              (let [n (or (d/q {:find '[(count ?e) .]
                                :where [['?e attr '?target]
                                        (list 'not ['?target (table-anchors target)])]} db) 0)]
                (when (pos? n) {:attribute attr :missing-targets n}))) schema/foreign-keys)
      (for [[order] (d/q '[:find ?o :where [?o :orders/district ?d] [?o :orders/customer ?c]
                          [?c :customer/district ?cd] [(not= ?d ?cd)]] db)]
        {:order order :error :customer-district})
      (for [[pending] (d/q '[:find ?pending :where [?pending :new-order/order ?o]
                            (not [?pending :new-order/key])] db)]
        {:pending pending :error :missing-queue-key})
      (for [[pending] (d/q '[:find ?pending :where [?pending :new-order/order ?o]
                            [?pending :new-order/key ?key] [?o :orders/district ?d] [?o :orders/id ?id]
                            [(tuple ?d ?id) ?expected] [(not= ?key ?expected)]] db)]
        {:pending pending :error :queue-key}))))

(defn verify
  "Verify a fresh seed, before warmup or transactions. Options: :dir, :warehouses,
  :seed and optional :out EDN report. Throws on missing references or row mismatches."
  [{:keys [dir warehouses seed out] :or {dir "db-w1" warehouses 1 seed 42}}]
  (with-open [conn (d/create-conn (c/local-path dir))]
    (let [db @conn
          errors (reference-errors db)
          _ (when (seq errors) (throw (ex-info "Invalid TPC-C reference graph" {:errors errors})))
          population (g/population-rows seed warehouses)
          tables
          (into (array-map)
            (for [table c/table-order]
              (let [expected (fingerprint (map #(mapv c/parse-value (:types (c/table-specs table)) %) (population table)))
                    actual (fingerprint (table-rows db table))
                    result {:expected expected :actual actual :match? (= expected actual)}]
                (println table (if (:match? result) "PASS" "FAIL") (:rows actual))
                (flush)
                [table result])))
          report {:warehouses warehouses :seed seed :reference-errors errors :tables tables
                  :pass? (every? :match? (vals tables))}]
      (when out (io/make-parents out) (spit out (with-out-str (pp/pprint report))))
      (when-not (:pass? report) (throw (ex-info "TPC-C seed rows differ from generator" report)))
      report)))
