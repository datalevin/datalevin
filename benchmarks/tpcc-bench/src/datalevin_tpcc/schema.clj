(ns datalevin-tpcc.schema
  "Normalized TPC-C entities, reference relationships, and seed entity IDs."
  (:require [datalevin-tpcc.common :as c]))

(def foreign-keys
  "Reference attribute -> [target table, source SQL-column attributes]. Composite
  SQL foreign keys become one reference, not independent references per column."
  {:district/warehouse ["warehouse" [:district/w-id]]
   :customer/district ["district" [:customer/w-id :customer/d-id]]
   :stock/warehouse ["warehouse" [:stock/w-id]]
   :stock/item ["item" [:stock/i-id]]
   :orders/district ["district" [:orders/w-id :orders/d-id]]
   :orders/customer ["customer" [:orders/w-id :orders/d-id :orders/c-id]]
   :new-order/order ["orders" [:new-order/w-id :new-order/d-id :new-order/o-id]]
   :order-line/order ["orders" [:order-line/w-id :order-line/d-id :order-line/o-id]]
   :order-line/stock ["stock" [:order-line/supply-w-id :order-line/i-id]]
   :history/customer ["customer" [:history/c-w-id :history/c-d-id :history/c-id]]
   :history/district ["district" [:history/w-id :history/d-id]]})

(def foreign-key-columns (set (mapcat (comp second val) foreign-keys)))

(def primary-keys
  {:district/key [:district/warehouse :district/id]
   :customer/key [:customer/district :customer/id]
   :stock/key [:stock/warehouse :stock/item]
   :orders/key [:orders/district :orders/id]
   :order-line/key [:order-line/order :order-line/number]})

(def secondary-keys
  {:customer/name-key [:customer/district :customer/last]
   :orders/customer-key [:orders/customer :orders/id]})

(def schema
  (merge
   (into {}
         (for [table c/table-order
               [a t] (map vector (c/attrs table) (:types (c/table-specs table)))
               :when (not (foreign-key-columns a))]
           [a (if (#{:warehouse/id :item/id} a)
                {:db/valueType (c/type->db-type t) :db/unique :db.unique/value}
                {:db/valueType (c/type->db-type t) :db/noindex true})]))
   (into {} (map (fn [a] [a {:db/valueType :db.type/ref}])) (keys foreign-keys))
   (into {} (map (fn [[a attrs]]
                   [a {:db/valueType :db.type/tuple :db/tupleAttrs attrs
                       :db/unique :db.unique/value}])) primary-keys)
   (into {} (map (fn [[a attrs]]
                   [a {:db/valueType :db.type/tuple :db/tupleAttrs attrs}])) secondary-keys)
   ;; A pending-order entity is identified by its order reference. The explicit
   ;; [district-ref order-number] index preserves ordered queue access without
   ;; retaining the three scalar foreign-key columns on that entity.
   {:new-order/order {:db/valueType :db.type/ref :db/unique :db.unique/value}
    :new-order/key {:db/valueType :db.type/tuple
                    :db/tupleTypes [:db.type/ref :db.type/long]}}))

(defn attrs [table fields]
  (mapv #(keyword table (name %)) fields))

(defn entity-layout
  "Reserve disjoint seed ranges from known TPC-C dimensions, as in JOB. No
  entity lookup map is built. Runtime inserts use ordinary transaction tempids;
  these formulas are only for the initial population. Order-line slots include
  all 15 possible line numbers, so sparse and differently ordered input is safe."
  [warehouses]
  (when-not (pos-int? warehouses)
    (throw (ex-info ":warehouses must be a positive integer" {:warehouses warehouses})))
  (let [districts (* warehouses c/districts-per-warehouse)
        customers (* districts c/customers-per-district)
        orders (* districts c/initial-orders-per-district)
        capacities {"item" c/item-count "warehouse" warehouses
                    "stock" (* warehouses c/item-count) "district" districts
                    "customer" customers "history" customers "orders" orders
                    "new_order" orders "order_line" (* orders 15)}]
    (first
      (reduce (fn [[layout base] table]
                (let [size (capacities table)]
                  [(assoc layout table {:base base :size size :warehouses warehouses})
                   (+ base size)]))
              [{} 1] c/table-order))))

(defn- ordinal
  [keys bounds]
  (when-not (and (= (count keys) (count bounds))
                 (every? true? (map #(and (integer? %1) (<= 1 %1 %2)) keys bounds)))
    (throw (ex-info "TPC-C seed key outside its table dimensions" {:key keys :bounds bounds})))
  (reduce (fn [n [key bound]] (+ (* n bound) (dec key))) 0 (map vector keys bounds)))

(defn entity-id
  "Compute a seed entity ID from its complete business key; history takes its
  one-based row ordinal. The same calculation constructs every foreign key."
  [layout table key]
  (let [{:keys [base size warehouses]} (layout table)
        bounds (case table
                 "item" [c/item-count]
                 "warehouse" [warehouses]
                 "stock" [warehouses c/item-count]
                 "district" [warehouses c/districts-per-warehouse]
                 "customer" [warehouses c/districts-per-warehouse c/customers-per-district]
                 "history" [size]
                 ("orders" "new_order") [warehouses c/districts-per-warehouse c/initial-orders-per-district]
                 "order_line" [warehouses c/districts-per-warehouse c/initial-orders-per-district 15])]
    (+ base (ordinal key bounds))))

(def ^:private row-keys
  {"item" [:item/id] "warehouse" [:warehouse/id]
   "stock" [:stock/w-id :stock/i-id]
   "district" [:district/w-id :district/id]
   "customer" [:customer/w-id :customer/d-id :customer/id]
   "orders" [:orders/w-id :orders/d-id :orders/id]
   "new_order" [:new-order/w-id :new-order/d-id :new-order/o-id]
   "order_line" [:order-line/w-id :order-line/d-id :order-line/o-id :order-line/number]})

(def ^:private table-attributes (into {} (map (juxt identity c/attrs)) c/table-order))

(def ^:private table-references
  (into {} (for [table c/table-order
                 :let [ns (namespace (first (table-attributes table)))]]
             [table (into {} (filter #(= ns (namespace (key %)))) foreign-keys)])))

(defn normalize-entity
  "Translate a SQL-shaped entity map using an explicit target-ID resolver.
  Used by the loader; partial foreign keys are rejected rather than discarded."
  [table raw resolve-id]
  (let [entity (into {} (remove (comp foreign-key-columns key)) raw)
        entity (reduce-kv
                 (fn [entity attr [target columns]]
                   (if (some #(contains? raw %) columns)
                     (let [values (mapv raw columns)]
                       (when-not (every? some? values)
                         (throw (ex-info "Incomplete TPC-C foreign key"
                                         {:attribute attr :columns columns :values values})))
                       (assoc entity attr (resolve-id target values)))
                     entity)) entity (table-references table))]
    (cond-> entity
      (and (= table "new_order") (contains? raw :new-order/o-id))
      (assoc :new-order/key [(resolve-id "district" ((juxt :new-order/w-id :new-order/d-id) raw))
                             (:new-order/o-id raw)]))))

(defn entity-map
  "Translate one generated SQL row to normalized datoms with explicit IDs.
  Nil SQL columns remain absent. The pending-order queue key is a derived index."
  [layout table row row-number]
  (let [raw (into {} (keep (fn [[a t v]] (when (some? v) [a (c/parse-value t v)])))
                  (map vector (table-attributes table) (:types (c/table-specs table)) row))
        business-key (if (= table "history") [row-number] (mapv raw (row-keys table)))]
    (assoc (normalize-entity table raw #(entity-id layout %1 %2))
           :db/id (entity-id layout table business-key))))
