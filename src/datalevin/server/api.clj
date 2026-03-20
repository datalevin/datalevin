;;
;; Copyright (c) Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2.0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns ^:no-doc datalevin.server.api
  "Datalevin query/search/vector server API handlers."
  (:require
   [datalevin.built-ins :as dbq]
   [datalevin.constants :as c]
   [datalevin.core :as d]
   [datalevin.dump :as dump]
   [datalevin.interface :as i]
   [datalevin.query :as q]
   [datalevin.search :as sc]
   [datalevin.util :as u]
   [datalevin.vector :as v])
  (:import
   [datalevin.db DB]))

(defn- write-or-copy-result!
  [write-message-fn copy-out-fn skey data]
  (if (coll? data)
    (if (< (count data) ^long c/+wire-datom-batch-size+)
      (write-message-fn skey {:type :command-complete :result data})
      (copy-out-fn skey data c/+wire-datom-batch-size+))
    (write-message-fn skey {:type :command-complete :result data})))

(defn q
  [{:keys [get-db write-message copy-out]} server skey {:keys [args writing?]}]
  (let [[db-name query inputs] args
        db                     (get-db server db-name writing?)
        inputs                 (replace {:remote-db-placeholder db} inputs)
        data                   (apply q/q query inputs)]
    (write-or-copy-result! write-message copy-out skey data)))

(defn pull
  [{:keys [get-db write-message copy-out]} server skey {:keys [args writing?]}]
  (let [[db-name pattern id opts] args
        db                        (get-db server db-name writing?)
        data                      (d/pull db pattern id opts)]
    (write-or-copy-result! write-message copy-out skey data)))

(defn pull-many
  [{:keys [get-db write-message copy-out]} server skey
   {:keys [args writing?]}]
  (let [[db-name pattern id opts] args
        db                        (get-db server db-name writing?)
        data                      (d/pull-many db pattern id opts)]
    (write-or-copy-result! write-message copy-out skey data)))

(defn explain
  [{:keys [get-db write-message]} server skey {:keys [args writing?]}]
  (let [[db-name opts query inputs] args
        db                        (get-db server db-name writing?)
        inputs                    (replace {:remote-db-placeholder db} inputs)
        data                      (apply q/explain opts query inputs)]
    (write-message skey {:type :command-complete :result data})))

(defn fulltext-datoms
  [{:keys [get-db write-message copy-out]} server skey
   {:keys [args writing?]}]
  (let [[db-name query opts] args
        db                   (get-db server db-name writing?)
        data                 (dbq/fulltext-datoms db query opts)]
    (write-or-copy-result! write-message copy-out skey data)))

(defn new-search-engine
  [{:keys [search-engine* get-store update-client update-db write-message]}
   server skey client-id {:keys [args]}]
  (let [[db-name opts] args
        engine         (or (search-engine* server skey db-name)
                           (if-let [store (get-store server db-name)]
                             (sc/new-search-engine store opts)
                             (u/raise "engine store not found"
                                      {:type :reopen
                                       :db-name db-name
                                       :db-type "kv"})))]
    (update-client server client-id #(update % :engines conj db-name))
    (update-db server db-name #(assoc % :engine engine))
    (write-message skey {:type :command-complete})))

(defn search-call
  [{:keys [search-engine write-message]} server skey {:keys [args]} op-fn]
  (write-message skey
                 {:type   :command-complete
                  :result (apply op-fn
                                 (search-engine server skey (nth args 0))
                                 (rest args))}))

(defn search-re-index
  [{:keys [search-engine update-db write-message]}
   server skey {:keys [args]}]
  (let [[db-name opts] args
        engine         (i/re-index (search-engine server skey db-name) opts)]
    (update-db server db-name #(assoc % :engine engine))
    (write-message skey {:type :command-complete})))

(defn new-vector-index
  [{:keys [get-store update-client update-db write-message]}
   server skey client-id {:keys [args]}]
  (let [[db-name opts] args
        index          (if-let [store (get-store server db-name)]
                         (v/new-vector-index store opts)
                         (u/raise "vector store not found"
                                  {:type :reopen
                                   :db-name db-name
                                   :db-type "kv"}))]
    (update-client server client-id #(update % :indices conj db-name))
    (update-db server db-name #(assoc % :index index))
    (write-message skey {:type :command-complete})))

(defn vector-call
  [{:keys [vector-index write-message]} server skey {:keys [args]} op-fn]
  (write-message skey
                 {:type   :command-complete
                  :result (apply op-fn
                                 (vector-index server skey (nth args 0))
                                 (rest args))}))

(defn vec-re-index
  [{:keys [vector-index update-db write-message]}
   server skey {:keys [args]}]
  (let [[db-name opts] args
        old            (vector-index server skey db-name)
        new            (i/re-index old opts)]
    (update-db server db-name #(assoc % :index new))
    (write-message skey {:type :command-complete})))

(defn kv-re-index
  [{:keys [lmdb update-db write-message]} server skey {:keys [args]}]
  (let [[db-name opts] args
        db             (i/re-index (lmdb server skey db-name false) opts)]
    (update-db server db-name #(assoc % :store db))
    (write-message skey {:type :command-complete})))

(defn datalog-re-index
  [{:keys [db-state update-db write-message]} server skey {:keys [args]}]
  (let [[db-name schema opts] args
        db                    (:dt-db (db-state server db-name))
        conn                  (atom db)
        conn1                 (dump/re-index-datalog conn schema opts)
        ^DB db1               @conn1
        store1                (.-store db1)]
    (update-db server db-name #(assoc % :store store1 :dt-db db1))
    (write-message skey {:type :command-complete})))
