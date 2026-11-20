(ns datalevin-tpcc.test-data
  "SQL-shaped fixture conversion. Benchmark transactions use native entities directly."
  (:require [clojure.string :as str]
            [datalevin.core :as d]
            [datalevin-tpcc.common :as c]
            [datalevin-tpcc.schema :as schema]))

(declare eid)
(defn lookup-ref [db table [w district id number :as key]]
  (case table
    "warehouse" [:warehouse/id w]
    "item" [:item/id w]
    "district" [:district/key [(eid db "warehouse" [w]) district]]
    "customer" [:customer/key [(eid db "district" [w district]) id]]
    "stock" [:stock/key [(eid db "warehouse" [w]) (eid db "item" [district])]]
    "orders" [:orders/key [(eid db "district" [w district]) id]]
    "new_order" [:new-order/order (eid db "orders" key)]
    "order_line" [:order-line/key [(eid db "orders" [w district id]) number]]))

(defn eid [db table key]
  (d/entid db (lookup-ref db table key)))

(defn- entity-table [entity]
  (some #(when-not (= :db/id %) (str/replace (namespace %) "-" "_")) (keys entity)))

(def ^:private row-keys
  {"warehouse" [:warehouse/id] "item" [:item/id]
   "district" [:district/w-id :district/id]
   "customer" [:customer/w-id :customer/d-id :customer/id]
   "stock" [:stock/w-id :stock/i-id]
   "orders" [:orders/w-id :orders/d-id :orders/id]})

(defn transact!
  "Normalize hand-written fixtures while preserving their explicit entity IDs.
  Every referenced parent must be in the batch or already in the database."
  [conn tx]
  (let [tx (mapv (fn [i row] (if (map? row) (assoc row :db/id (or (:db/id row) (- (inc i)))) row))
                 (range) tx)
        keys (into {} (keep (fn [row]
                             (when (map? row)
                               (let [table (entity-table row) columns (row-keys table)]
                                 (when (and columns (every? #(contains? row %) columns))
                                   [[table (mapv row columns)] (:db/id row)]))))) tx)
        db (d/db conn)
        resolve-id (fn [table key]
                     (or (get keys [table key]) (eid db table key)
                         (throw (ex-info "Missing fixture reference" {:table table :key key}))))]
    (d/transact! conn (mapv #(if (map? %) (schema/normalize-entity (entity-table %) % resolve-id) %) tx))))

(defn transact-rows! [conn rows]
  (let [warehouses (reduce max 1
                    (for [[table rows] rows row rows
                          [attr value] (map vector (c/attrs table) row)
                          :when (or (= attr :warehouse/id) (str/ends-with? (name attr) "w-id"))]
                      value))
        layout (schema/entity-layout warehouses)]
    (d/transact! conn
      (into [] (mapcat (fn [table]
                        (map-indexed #(schema/entity-map layout table %2 (inc %1)) (get rows table))))
            c/table-order))))
