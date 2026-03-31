;;
;; Copyright (c) Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2.0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns ^:no-doc datalevin.server.dispatch
  "Message dispatch and request handlers."
  (:require
   [clojure.string :as s]
   [datalevin.binding.cpp :as cpp]
   [datalevin.buffer :as bf]
   [datalevin.constants :as c]
   [datalevin.ha :as dha]
   [datalevin.protocol :as p]
   [datalevin.txlog :as txlog]
   [datalevin.util :as u]
   [taoensso.timbre :as log])
  (:import
   [java.nio ByteBuffer BufferOverflowException]
   [java.nio.channels ClosedChannelException SelectionKey SocketChannel
    ServerSocketChannel]
   [java.util.concurrent Executor]))

(def ^:private ha-abort-cleanup-types
  #{:abort-transact
    :abort-transact-kv})

(def ^:private idempotent-withtxn-control-types
  #{:close-transact
    :abort-transact
    :close-transact-kv
    :abort-transact-kv})

(defn handle-accept
  [^SelectionKey skey]
  (when-let [client-socket (.accept ^ServerSocketChannel (.channel skey))]
    (doto ^SocketChannel client-socket
      (.configureBlocking false)
      (.register (.selector skey) SelectionKey/OP_READ
                 ;; attach a connection state
                 ;; { read-bf, write-bf, client-id }
                 (volatile! {:read-bf  (bf/allocate-buffer
                                         c/+buffer-size+)
                             :write-bf (bf/allocate-buffer
                                         c/+buffer-size+)
                             ;; Preserve client message order per connection.
                             ;; Authentication/session setup must not race
                             ;; with subsequent requests like :open.
                             :message-lock (Object.)
                             :wire-opts (p/default-wire-opts)})))))

(defn client-disconnect?
  [e]
  (boolean
   (some
    (fn [cause]
      (let [message (ex-message cause)]
        (or (instance? ClosedChannelException cause)
            (= message "Socket channel is closed.")
            (and (string? message)
                 (or (s/includes? message "Connection reset by peer")
                     (s/includes? message "Broken pipe"))))))
    (take-while some? (iterate ex-cause e)))))

(defn- handled-request-error?
  [e]
  (let [data     (ex-data e)
        err-data (:err-data data)]
    (and (instance? clojure.lang.ExceptionInfo e)
         (map? data)
         (nil? (ex-cause e))
         (or (:type data)
             (:error data)
             (:resized data)
             (map? err-data)))))

(defn- log-handled-request-error!
  [e]
  (let [data     (or (ex-data e) {})
        err-data (:err-data data)
        details  (cond-> {:message (ex-message e)}
                   (:type data) (assoc :type (:type data))
                   (:error data) (assoc :error (:error data))
                   (:db-name data) (assoc :db-name (:db-name data))
                   (map? err-data)
                   (cond->
                     (:type err-data) (assoc :err-type (:type err-data))
                     (:error err-data) (assoc :err-error (:error err-data))))]
    (if (handled-request-error? e)
      ;; These request failures are returned to the client and are often
      ;; asserted in tests. Keep them out of stderr unless debug logging is on.
      (log/debug "Handled request error" details)
      (log/error e))))

(defn- close-conn-quietly
  [deps ^SelectionKey skey]
  (try
    ((:close-conn-fn deps) skey)
    (catch Exception _ nil)))

(defn- error-response
  [^SelectionKey skey error-msg error-data]
  (let [{:keys [^ByteBuffer write-bf wire-opts]} @(.attachment skey)
        ^SocketChannel ch (.channel skey)]
    (p/write-message-blocking ch write-bf
                              {:type     :error-response
                               :message  error-msg
                               :err-data error-data}
                              wire-opts)))

(defn- reopen-response
  [^SelectionKey skey msg]
  (let [{:keys [^ByteBuffer write-bf wire-opts]} @(.attachment skey)
        ^SocketChannel ch (.channel skey)]
    (p/write-message-blocking ch write-bf msg wire-opts)))

(defn handle-message-error!
  [deps ^SelectionKey skey e]
  (let [data (ex-data e)]
    (cond
      (client-disconnect? e)
      (close-conn-quietly deps skey)

      (= (:type data) :reopen)
      (try
        (reopen-response skey data)
        (catch Exception reopen-e
          (when-not (client-disconnect? reopen-e)
            (log/error reopen-e "Failed to send reopen response"))
          (close-conn-quietly deps skey)))

      :else
      (do
        (log-handled-request-error! e)
        (try
          (error-response skey (ex-message e) data)
          (catch Exception response-e
            (when-not (client-disconnect? response-e)
              (log/error response-e "Failed to send error response"))
            (close-conn-quietly deps skey)))))))

(defn current-ha-txlog-term
  [deps server db-name]
  (when-let [db-state (and db-name (get ((:dbs-fn deps) server) db-name))]
    (let [authority-term (:ha-authority-term db-state)]
      (when (and (:ha-authority db-state)
                 (= :leader (:ha-role db-state))
                 (integer? authority-term)
                 (pos? ^long authority-term))
        (long authority-term)))))

(declare dispatch-message)

(defn dispatch-message-with-ha-write-admission
  [deps server ^SelectionKey skey message]
  (let [type          (:type message)
        cleanup-only? (ha-abort-cleanup-types type)
        write?        (and (not cleanup-only?) (dha/ha-write-message? message))
        db-name       (nth (:args message) 0 nil)
        ha-txlog-term (current-ha-txlog-term deps server db-name)
        precheck-only? (contains? #{:open-transact :open-transact-kv} type)
        {:keys [ok? error]}
        (if cleanup-only?
          {:ok? true}
          ((:with-ha-write-admission-fn deps)
           server
           message
           #(cond
              precheck-only?
              nil

              write?
              (binding [txlog/*commit-payload-ha-term* ha-txlog-term
                        cpp/*before-write-commit-fn*
                        ((:ha-write-commit-check-fn-fn deps) server message)]
                (dispatch-message deps server skey message))

              :else
              (dispatch-message deps server skey message))))]
    (cond
      (not ok?)
      (do
        ((:cleanup-rejected-close-transact!-fn deps) server message)
        (error-response skey "HA write admission rejected" error))

      cleanup-only?
      (dispatch-message deps server skey message)

      precheck-only?
      (dispatch-message deps server skey message))))

(defn dispatch-message
  [deps server ^SelectionKey skey message]
  (if-let [handler (get (:message-handler-map deps) (:type message))]
    (handler server skey message)
    (error-response skey
                    (str "Unknown message type " (:type message))
                    {})))

(defn execute
  "Execute a function in a thread from the worker thread pool"
  [deps server f]
  (.execute ^Executor ((:work-executor-fn deps) server) f))

(defn handle-writing
  [deps server ^SelectionKey skey {:keys [args] :as message}]
  (try
    (let [db-name  (nth args 0)
          type     (:type message)
          _        ((:trace-remote-tx-fn deps) "handle-writing" type db-name)
          _        ((:get-kv-store-fn deps) server db-name)
          runner   (get-in ((:dbs-fn deps) server) [db-name :runner])]
      (cond
        runner
        ((:new-message-fn deps) runner skey message)

        (idempotent-withtxn-control-types type)
        ((:write-message-fn deps) skey {:type :command-complete})

        :else
        (u/raise "No active with-transaction runner"
                 {:db-name db-name
                  :type    type})))
    (catch Exception e
      (error-response skey
                      (str "Error Handling with-transaction message:"
                           (ex-message e))
                      {}))))

(defn- set-last-active
  [deps server ^SelectionKey skey]
  (let [{:keys [client-id]} @(.attachment skey)]
    (when client-id
      ;; Avoid durable session writes on every request; this path is hot.
      (when-let [session ((:get-client-fn deps) server client-id)]
        (.put ^java.util.Map ((:clients-fn deps) server)
              client-id
              (assoc session :last-active (System/currentTimeMillis)))))))

(defn handle-message
  [deps server ^SelectionKey skey fmt msg]
  (try
    (let [state (.attachment skey)
          {:keys [message-lock]} @state
          message
          (locking message-lock
            (let [wire-opts (:wire-opts @state)
                  {:keys [type] :as message}
                  (p/read-value fmt msg wire-opts)]
              (if (= type :set-client-id)
                (do
                  (log/debug "Message received:" (dissoc message :password :args))
                  (dispatch-message deps server skey message)
                  ::handled)
                message)))]
      (when-not (= ::handled message)
        (let [{:keys [writing?]} message]
          (log/debug "Message received:" (dissoc message :password :args))
          (set-last-active deps server skey)
          (if writing?
            (handle-writing deps server skey message)
            ((:with-db-runtime-read-access-fn deps)
             server
             message
             #(dispatch-message-with-ha-write-admission
               deps server skey message))))))
    (catch Exception e
      (log/error "Error Handling message:" e))))

(defn handle-read
  [deps server ^SelectionKey skey]
  (try
    (let [state                         (.attachment skey)
          {:keys [^ByteBuffer read-bf]} @state
          capacity                      (.capacity read-bf)
          ^SocketChannel ch             (.channel skey)
          ^int readn                    (p/read-ch ch read-bf)]
      (when (pos? readn)
        ((:trace-remote-tx-fn deps) "handle-read" readn (.hashCode skey)))
      (when (> (.position read-bf) c/message-header-size)
        (let [^ByteBuffer probe (.duplicate read-bf)
              pos (.position probe)]
          (.flip probe)
          ((:trace-remote-tx-fn deps)
           "buffer-header"
           (.hashCode skey)
           "pos" pos
           "len" (.getInt (doto probe (.get))))))
      (cond
        (> readn 0)
        (do
          (p/extract-message
           read-bf
           (fn [fmt msg]
             (execute deps server #(handle-message deps server skey fmt msg))))
          (when (= (.position read-bf) capacity)
            (let [size (* ^long c/+buffer-grow-factor+ capacity)
                  bf   (bf/allocate-buffer size)]
              (.flip read-bf)
              (bf/buffer-transfer read-bf bf)
              (vswap! state assoc :read-bf bf))))

        (= readn 0)
        :continue

        (= readn -1)
        (.close ch)))
    (catch java.io.IOException e
      (if (s/includes? (ex-message e) "Connection reset by peer")
        (.close (.channel skey))
        (log/error "Read IOException:" (ex-message e))))
    (catch Exception e
      (log/error "Read error:" (ex-message e)))))
