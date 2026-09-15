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
   [datalevin.pull-api :as pull]
   [datalevin.query :as q]
   [datalevin.query.resolve :as qresolve]
   [datalevin.search :as sc]
   [datalevin.server.prepared :as prepared]
   [datalevin.util :refer [raise]]
   [datalevin.vector :as v])
  (:import
   [java.nio.channels SelectionKey]
   [datalevin.db DB]))

(defn- write-or-copy-result!
  [write-message-fn copy-out-fn skey data]
  (if (coll? data)
    (if (< (count data) ^long c/+wire-datom-batch-size+)
      (write-message-fn skey {:type :command-complete :result data})
      (copy-out-fn skey data c/+wire-datom-batch-size+))
    (write-message-fn skey {:type :command-complete :result data})))

(defn q
  [{:keys [get-db write-message copy-out]} server skey
   {:keys [args writing?] :as message}]
  (let [[db-name query inputs] args
        db                     (get-db server db-name writing?)
        inputs                 (replace {:remote-db-placeholder db} inputs)
        data                   (binding [qresolve/*resolver-mode* :server-safe]
                                 (if-let [reader (prepared/reader! skey message)]
                                   (reader db inputs false)
                                   (apply q/q query inputs)))]
    (write-or-copy-result! write-message copy-out skey data)))

(defn pull
  [{:keys [get-db write-message copy-out]} server ^SelectionKey skey
   {:keys [args writing?] :as message}]
  (let [[db-name pattern id opts] args
        db                        (get-db server db-name writing?)
        reader                    (prepared/reader! skey message)
        encoded?                  (get-in @(.attachment skey) [:wire-opts :storage-read?])
        data                      (cond
                                    reader (reader db id encoded?)
                                    encoded? (pull/read-result db pattern id opts)
                                    :else (d/pull db pattern id opts))]
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
        data                      (binding [qresolve/*resolver-mode* :server-safe]
                                    (apply q/explain opts query inputs))]
    (write-message skey {:type :command-complete :result data})))

(defn fulltext-datoms
  [{:keys [get-db write-message copy-out]} server skey
   {:keys [args writing?]}]
  (let [[db-name query opts] args
        db                   (get-db server db-name writing?)
        data                 (dbq/fulltext-datoms db query opts)]
    (write-or-copy-result! write-message copy-out skey data)))

(defn new-search-engine
  [{:keys [search-engine* get-store update-client update-db write-message
           with-index-write-admission]}
   server skey client-id {:keys [args] :as message}]
  (let [[db-name opts] args
        engine         (or (search-engine* server skey db-name)
                           (if-let [store (get-store server db-name)]
                             (or (sc/open-search-engine store opts)
                                 (with-index-write-admission
                                   server message
                                   #(sc/new-search-engine store opts)))
                             (raise "engine store not found"
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
  [{:keys [db-state get-store update-client update-db write-message
           with-index-write-admission]}
   server skey client-id {:keys [args] :as message}]
  (let [[db-name opts] args]
    (if-let [store (get-store server db-name)]
      ;; Dispatch holds runtime read access, keeping this store stable. Serialize
      ;; the shared-state check, initialization, and publication across clients:
      ;; reloading a live index can discard vectors whose checkpoint is pending.
      (locking store
        (let [index (:index (db-state server db-name))]
          (when (or (nil? index) (i/vec-closed? index))
            (let [index (or (v/open-vector-index store opts)
                            (with-index-write-admission
                              server message
                              #(v/new-vector-index store opts)))]
              (update-db server db-name #(assoc % :index index))))))
      (raise "vector store not found"
               {:type :reopen
                :db-name db-name
                :db-type "kv"}))
    (update-client server client-id #(update % :indices conj db-name))
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
