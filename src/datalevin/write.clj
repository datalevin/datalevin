;; Copyright (c) Huahai Yang. All rights reserved.
;; Distributed under the Eclipse Public License 2.0.
(ns ^:no-doc datalevin.write
  "Caller-owned prepared Datalog writes. Only metadata is retained across calls."
  (:require [datalevin.conn :as conn]
            [datalevin.db :as db]
            [datalevin.db.tx.template :as template]
            [datalevin.prepare :as prepare]
            [datalevin.prepared :as prepared])
  (:import [datalevin.storage Store]))

(def tx-param template/parameter)

(defn- local-db [target connection?]
  (let [database (cond (conn/conn? target) @target
                       (and (not connection?) (db/db? target)) target)]
    (when-not (and database (instance? Store (:store database)))
      (throw (ex-info "Prepared writes require a local Datalog connection"
                      {:error :prepared/local-connection})))
    (when (and (conn/conn? target) (conn/closed? target))
      (throw (IllegalStateException. "Prepared write connection is closed")))
    database))

(defn prepare-tx-op
  "Prepare an immutable operation that can be shared by transaction templates."
  [target operation]
  (template/prepare-operation (prepare/make-prepare-ctx (local-db target false)) operation))

(defn prepare-tx
  "Prepare a local parameterized transaction, optionally repeated for a batch."
  ([connection operations] (prepare-tx connection operations nil))
  ([connection operations options]
   (when (seq (dissoc options :batch?))
     (throw (ex-info "Unknown prepared transaction options" {:error :prepared/options})))
   (let [ctx (prepare/make-prepare-ctx (local-db connection true))
         state (atom (template/prepare-template ctx operations options))
         run (fn [scope input tx-meta]
               (let [ctx (prepare/make-prepare-ctx (local-db scope true))
                     captured @state
                     compiled (if (template/compatible? (:ctx captured) ctx)
                                captured
                                (let [updated (template/prepare-template ctx operations options)]
                                  (reset! state updated)
                                  updated))
                     rows (template/input-rows compiled input)]
                 (if-let [outcome (template/bind-template compiled rows)]
                   (conn/transact-prepared! scope (template/request-input compiled rows) tx-meta outcome)
                   (conn/transact! scope (template/materialize compiled rows) tx-meta))))]
     (prepared/prepared-write #(run connection %1 %2) run))))
