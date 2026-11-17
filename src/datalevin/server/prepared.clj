;; Copyright (c) Huahai Yang. All rights reserved.
;; Distributed under the Eclipse Public License 2.0.
(ns ^:no-doc datalevin.server.prepared
  "Connection-local prepared operations. Expansion precedes authorization,
  admission and transaction routing; functions are installed after those checks."
  (:require [datalevin.bits :as b]
            [datalevin.client-op :as cop]
            [datalevin.kv :as kv]
            [datalevin.prepared :as prepared]
            [datalevin.db.tx.template :as template]
            [datalevin.db.tx.wire :as txwire]
            [datalevin.pull-api :as pull]
            [datalevin.pull-wire :as wire]
            [datalevin.query :as q]
            [datalevin.util :refer [raise]])
  (:import [java.nio.channels SelectionKey]
           [java.util LinkedHashMap]))

(deftype Entry [type args reader response-writer])

(deftype WriteOperation [source definition-hash compiled])
(deftype WriteProgram [operations options compiled])

(defn- enabled? [^SelectionKey skey]
  (true? (get-in @(.attachment skey) [:wire-opts :prepared-read?])))

(defn- check-id! [id]
  (when-not (and (integer? id) (<= 1 id Long/MAX_VALUE))
    (raise "Invalid prepared handle" {:error :prepared/invalid-handle})))

(defn expand-message
  "Restore routing fields from this connection's handle. Ignore caller-supplied
  preparation metadata. Deferred native requests are expanded again after
  authorized decoding supplies their actual key/entity identifier."
  [^SelectionKey skey message]
  (let [message (if (contains? (meta message) ::entry)
                  (vary-meta message dissoc ::entry)
                  message)]
    (if (= :execute-prepared (:type message))
      (let [id (:handle message)
            _ (check-id! id)
            ^LinkedHashMap cache (:prepared-handles @(.attachment skey))
            ^Entry entry (when (and (or (enabled? skey)
                                     (true? (get-in @(.attachment skey) [:wire-opts :prepared-write?]))
                                     (true? (get-in @(.attachment skey)
                                                    [:wire-opts :prepared-update?])))
                                 cache) (.get cache id))]
        (when-not entry
          (raise "Prepared handle is not registered on this connection"
                 {:error :prepared/missing :handle id}))
        (when (= :prepared-tx-op (.-type entry))
          (raise "An operation handle cannot execute a transaction" {:error :prepared/invalid-handle}))
        (when-not (case (.-type entry)
                    :tx-data-prepared (true? (get-in @(.attachment skey) [:wire-opts :prepared-write?]))
                    :update-kv (true? (get-in @(.attachment skey) [:wire-opts :prepared-update?]))
                    :q (true? (get-in @(.attachment skey) [:wire-opts :prepared-query?]))
                    (enabled? skey))
          (raise "Prepared operation was not negotiated" {:error :prepared/unsupported}))
        (with-meta
          (cond-> {:type (.-type entry)
                   :args (assoc (.-args entry) 2 (:value message))
                   :writing? (:writing? message)}
            (#{:update-kv :tx-data-prepared} (.-type entry))
            (merge (select-keys message [:client-op-id :client-op-hash
                                         :client-op-response-kind]))
            (:ha-read-min-tx message)
            (assoc :ha-read-min-tx (:ha-read-min-tx message)))
          (assoc (meta message) ::entry entry)))
      message)))

(defn reader!
  "Return a prepared reader, registering fixed arguments on a cold execution.
  Call only inside the original operation's authorized handler."
  [^SelectionKey skey {:keys [type args prepare-id] :as message}]
  (if-let [^Entry entry (::entry (meta message))]
    (.-reader entry)
    (when (and prepare-id (enabled? skey))
      (check-id! prepare-id)
      (when-not (case type
                  :get-value (= 6 (count args))
                  :pull (= 4 (count args))
                  :q (and (= 3 (count args))
                          (true? (get-in @(.attachment skey)
                                         [:wire-opts :prepared-query?])))
                  false)
        (raise "Unsupported prepared read" {:error :prepared/unsupported :type type}))
      (let [response-writer (when (and (= type :pull)
                                       (true? (get-in @(.attachment skey)
                                                      [:wire-opts :prepared-pull?])))
                              (wire/writer prepare-id))
            reader (case type
                     :get-value (kv/value-reader (nth args 1) (nth args 3)
                                                 (nth args 4) (nth args 5))
                     :pull (pull/pull-reader (nth args 1) (nth args 3) response-writer)
                     :q (q/query-reader (nth args 1)))
            attachment (.attachment skey)
            ^LinkedHashMap cache (or (:prepared-handles @attachment)
                                     (let [cache (prepared/handle-cache)]
                                       (vswap! attachment assoc :prepared-handles cache)
                                       cache))]
        (prepared/remember! cache prepare-id
                            (Entry. type (assoc args 2 nil) reader response-writer))
        reader))))

(defn update-function!
  "Resolve an update function only inside the authorized update handler."
  [^SelectionKey skey {:keys [args prepare-id] :as message}]
  (if-let [^Entry entry (::entry (meta message))]
    (.-reader entry)
    (let [f (b/deserialize (nth args 3))]
      (when (and prepare-id
                 (true? (get-in @(.attachment skey) [:wire-opts :prepared-update?])))
        (check-id! prepare-id)
        (let [attachment (.attachment skey)
              ^LinkedHashMap cache (or (:prepared-handles @attachment)
                                      (let [cache (prepared/handle-cache)]
                                        (vswap! attachment assoc :prepared-handles cache)
                                        cache))]
          (prepared/remember! cache prepare-id
                             (Entry. :update-kv (assoc args 2 nil) f nil))))
      f)))

(defn- handles! [^SelectionKey skey]
  (let [attachment (.attachment skey)]
    (or (:prepared-handles @attachment)
        (let [cache (prepared/handle-cache)] (vswap! attachment assoc :prepared-handles cache) cache))))

(defn- current-operation [^WriteOperation operation ctx]
  (let [captured @(.-compiled operation)]
    (if (and captured (template/compatible? (:ctx captured) ctx)) captured
        (let [compiled (template/prepare-operation ctx (.-source operation))]
          (reset! (.-compiled operation) compiled) compiled))))

(defn- current-program [^WriteProgram program ctx]
  (let [captured @(.-compiled program)]
    (if (and captured (template/compatible? (:ctx captured) ctx)) captured
        (let [compiled (template/prepare-template ctx
                         (mapv #(current-operation % ctx) (.-operations program)) (.-options program))]
          (reset! (.-compiled program) compiled) compiled))))

(defn write-template!
  "Compile/refresh only inside the authorized write handler. Explicit shared
  operations occupy the existing bounded connection registry with templates.
  Reusing an operation id in one database requires the same encoded definition."
  [^SelectionKey skey {:keys [args prepare-id] :as message} ctx]
  (if-let [^Entry entry (::entry (meta message))]
    (current-program (.-reader entry) ctx)
    (let [[db-name {:keys [version operations options]}] args]
      (when-not (and (= version 1) (vector? operations) (seq operations)
                     (empty? (dissoc options :batch?)))
        (raise "Invalid prepared transaction definition" {:error :prepared/definition}))
      (when-not (true? (get-in @(.attachment skey) [:wire-opts :prepared-write?]))
        (raise "Prepared writes were not negotiated" {:error :prepared/unsupported}))
      (let [^LinkedHashMap cache (handles! skey)
            operations (mapv (fn [{:keys [id] :as descriptor}]
                                (when id (check-id! id))
                                (let [definition-hash (when id
                                                        (cop/request-hash
                                                          [(:form descriptor) (:parameters descriptor)]))
                                      ^Entry prior (when id (.get cache id))]
                                  (if (and prior (= :prepared-tx-op (.-type prior))
                                           (= db-name (first (.-args prior))))
                                    (let [^WriteOperation operation (.-reader prior)]
                                      (when-not (= definition-hash (.-definition-hash operation))
                                        (raise "Prepared operation id already has a different definition"
                                               {:error :prepared/operation-conflict :handle id}))
                                      operation)
                                    (let [operation (WriteOperation. (txwire/restore-operation descriptor)
                                                                     definition-hash (atom nil))]
                                      (when id
                                        (prepared/remember! cache id (Entry. :prepared-tx-op [db-name] operation nil)))
                                      operation)))) operations)
            program (WriteProgram. operations options (atom nil))
            compiled (current-program program ctx)]
        (when prepare-id
          (check-id! prepare-id)
          (prepared/remember! cache prepare-id (Entry. :tx-data-prepared (assoc args 2 nil) program nil)))
        compiled))))

(defn response-written!
  "Acknowledge a layout only after writing the complete response frame."
  [^SelectionKey skey message]
  (when-let [^Entry entry (or (::entry (meta message))
                              (when-let [id (:prepare-id message)]
                                (when-let [^LinkedHashMap cache
                                           (:prepared-handles @(.attachment skey))]
                                  (.get cache id))))]
    (when-let [writer (.-response-writer entry)]
      (wire/response-written! writer))))
