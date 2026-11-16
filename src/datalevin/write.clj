;; Copyright (c) Huahai Yang. All rights reserved.
;; Distributed under the Eclipse Public License 2.0.
(ns ^:no-doc datalevin.write
  "Caller-owned prepared Datalog writes. Only metadata is retained across calls."
  (:require [datalevin.conn :as conn]
            [datalevin.db]
            [datalevin.db.tx.template :as template]
            [datalevin.db.tx.wire :as txwire]
            [datalevin.interface :as i]
            [datalevin.prepare :as prepare]
            [datalevin.prepared :as prepared])
  (:import [datalevin.db DB]
           [datalevin.storage Store]))

(def tx-param template/parameter)

(defn- write-db [target connection?]
  (let [database (cond (instance? clojure.lang.IDeref target) @target
                       (and (not connection?) (instance? DB target)) target)]
    (when-not (and (instance? DB database)
                  (or (instance? Store (:store database))
                      (satisfies? i/IRemotePreparedWrite (:store database))))
      (throw (ex-info "Prepared writes require a Datalog connection" {:error :prepared/connection})))
    (if (instance? Store (:store database))
      (when (i/closed? (:store database))
        (throw (IllegalStateException. "Prepared write connection is closed")))
      (i/check-remote-read-open! (:store database)))
    database))

(defn prepare-tx-op
  "Prepare an immutable operation that can be shared by transaction templates."
  [target operation]
  (let [database (write-db target false)]
    (if (instance? Store (:store database))
      (template/prepare-operation (prepare/make-prepare-ctx database) operation)
      (txwire/operation operation))))

(defn- prepare-local [connection database operations options]
  (let [ctx (prepare/make-prepare-ctx database)
        state (atom (template/prepare-template ctx operations options))
        run (fn [scope input tx-meta]
               (let [target (write-db scope true)
                     _ (when-not (instance? Store (:store target))
                         (throw (ex-info "Local prepared writes require a local target" {:error :prepared/local-connection})))
                     ctx (prepare/make-prepare-ctx target)
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
    (prepared/prepared-write #(run connection %1 %2) run)))

(defn- prepare-remote [connection database operations options]
  (let [request (txwire/request (i/db-name (:store database)) operations options)
        run (fn [scope input tx-meta]
              (let [target (write-db scope true)]
                (when-not (and (satisfies? i/IRemotePreparedWrite (:store target))
                               (= (i/dir (:store database)) (i/dir (:store target))))
                  (throw (ex-info "Prepared write requires the same remote database" {:error :prepared/database-mismatch})))
                (txwire/input-rows request input)
                (conn/transact-remote-prepared! scope request input tx-meta)))]
    (prepared/prepared-write #(run connection %1 %2) run)))

(defn prepare-tx
  "Prepare a local or remote parameterized transaction, optionally batched."
  ([connection operations] (prepare-tx connection operations nil))
  ([connection operations options]
   (when (seq (dissoc options :batch?))
     (throw (ex-info "Unknown prepared transaction options" {:error :prepared/options})))
   (let [database (write-db connection true)]
     (if (instance? Store (:store database))
       (prepare-local connection database operations options)
       (prepare-remote connection database operations options)))))
