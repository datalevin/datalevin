(ns datalevin-tpcc.schema
  "Business keys and projections for the Datalevin TPC-C tables."
  (:require [datalevin-tpcc.common :as c]))

(def primary-keys
  {:district/key [:district/w-id :district/id]
   :customer/key [:customer/w-id :customer/d-id :customer/id]
   :stock/key [:stock/w-id :stock/i-id]
   :orders/key [:orders/w-id :orders/d-id :orders/id]
   :new-order/key [:new-order/w-id :new-order/d-id :new-order/o-id]
   :order-line/key [:order-line/w-id :order-line/d-id
                    :order-line/o-id :order-line/number]})

(def secondary-keys
  {:customer/name-key [:customer/w-id :customer/d-id :customer/last]
   :orders/customer-key [:orders/w-id :orders/d-id :orders/c-id :orders/id]})

(def schema
  (merge
   (into {}
         (for [table c/table-order
               [a t] (map vector (c/attrs table) (:types (c/table-specs table)))]
           ;; All other scalar fields are read through the business keys below.
           ;; Composite keys do not require value indexes on their components.
           [a (cond-> {:db/valueType (c/type->db-type t)}
                (not (#{:warehouse/id :item/id} a))
                (assoc :db/noindex true)
                (#{:warehouse/id :item/id} a)
                (assoc :db/unique :db.unique/value))]))
   (into {} (map (fn [[a attrs]]
                   [a {:db/valueType :db.type/tuple :db/tupleAttrs attrs
                       :db/unique :db.unique/value}])) primary-keys)
   (into {} (map (fn [[a attrs]]
                   [a {:db/valueType :db.type/tuple :db/tupleAttrs attrs}]))
         secondary-keys)))

(defn attrs [table fields]
  (mapv #(keyword table (name %)) fields))
