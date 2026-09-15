;; Copyright (c) Huahai Yang. All rights reserved.
;; Distributed under the Eclipse Public License 2.0.
(ns ^:no-doc datalevin.server.context
  "A connection owns one context and dependency map for all its commands.
  Protected reads share a request snapshot; mutations refresh live state before
  resolving a view. Request state is cleared before returning to the socket loop."
  (:require [datalevin.util :refer [raise]]))

(defprotocol IConnectionContext
  (prepare! [context client-id session db-name state snapshot?])
  (clear! [context])
  (client [context server client-id])
  (db-state [context server db-name])
  (db-store [context server skey db-name])
  (get-db [context server db-name writing?]))

(deftype ConnectionContext [owner key base-deps store-from-state db-from-state
                            ^:unsynchronized-mutable active?
                            ^:unsynchronized-mutable snapshot?
                            ^:unsynchronized-mutable client-id
                            ^:unsynchronized-mutable session
                            ^:unsynchronized-mutable db-name
                            ^:unsynchronized-mutable state
                            ^:unsynchronized-mutable store-resolved?
                            ^:unsynchronized-mutable store
                            ^:unsynchronized-mutable db-resolved?
                            ^:unsynchronized-mutable db]
  IConnectionContext
  (prepare! [_ id current-session name current-state stable?]
    (set! snapshot? stable?)
    (set! client-id id)
    (set! session current-session)
    (set! db-name name)
    (set! state current-state)
    (set! active? true))
  (clear! [_]
    (set! active? false)
    (set! snapshot? false)
    (set! client-id nil)
    (set! session nil)
    (set! db-name nil)
    (set! state nil)
    (set! store nil)
    (set! store-resolved? false)
    (set! db nil)
    (set! db-resolved? false))
  (client [_ server id]
    (if (and active? (identical? owner server) (= client-id id))
      (if snapshot?
        session
        (let [current ((:get-client base-deps) server id)]
          (when-not (identical? (:stores session) (:stores current))
            (set! store nil)
            (set! store-resolved? false))
          (set! session current)
          current))
      ((:get-client base-deps) server id)))
  (db-state [_ server name]
    (if (and active? (identical? owner server) (= db-name name))
      (if snapshot?
        state
        (let [current ((:db-state base-deps) server name)]
          (when-not (identical? state current)
            (set! state current)
            (set! store nil)
            (set! store-resolved? false)
            (set! db nil)
            (set! db-resolved? false))
          current))
      ((:db-state base-deps) server name)))
  (db-store [this server skey name]
    (if (and active? (identical? owner server) (identical? key skey)
             (= db-name name))
      (do
        (when-not snapshot?
          (client this server client-id)
          (db-state this server name))
        (when-not store-resolved?
          (set! store (when (get (:stores session) name)
                        (store-from-state state false)))
          (set! store-resolved? true))
        store)
      ((:db-store base-deps) server skey name)))
  (get-db [this server name writing?]
    (if (and active? (identical? owner server) (= db-name name))
      (if writing?
        ;; Transaction views may be replaced by earlier work in this command.
        (db-from-state (db-state this server name) true)
        (do
          (when-not snapshot? (db-state this server name))
          (when-not db-resolved?
            ;; HA readers still invalidate their native reader and build a fresh
            ;; view. Only repeated resolution within this request is avoided.
            (set! db (db-from-state state false))
            (set! db-resolved? true))
          db))
      ((:get-db base-deps) server name writing?))))

(defn create
  "Build the reusable context and its dependency map for one TCP connection.
  Other databases and connections use the original callbacks; transaction
  accesses always resolve the current writing view."
  [server skey base-deps store-from-state db-from-state store->lmdb]
  (let [context (ConnectionContext.
                  server skey base-deps store-from-state db-from-state
                  false false nil nil nil nil false nil false nil)]
    {:context context
     :deps
     (assoc base-deps
            :get-client (fn [server id] (client context server id))
            :db-state (fn [server name] (db-state context server name))
            :db-store (fn [server skey name] (db-store context server skey name))
            :store
            (fn [server skey name writing?]
              (if writing?
                ((:store base-deps) server skey name writing?)
                (or (db-store context server skey name)
                    (raise "Store not found"
                           {:type :reopen :db-name name :db-type "datalog"}))))
            :lmdb
            (fn [server skey name writing?]
              (if writing?
                ((:lmdb base-deps) server skey name writing?)
                (or (some-> (db-store context server skey name) store->lmdb)
                    (raise "LMDB store not found"
                           {:type :reopen :db-name name :db-type "kv"}))))
            :get-db
            (fn
              ([server name] (get-db context server name false))
              ([server name writing?] (get-db context server name writing?))))}))
