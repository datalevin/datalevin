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
   [datalevin.command :as cmd]
   [datalevin.constants :as c]
   [datalevin.kv.txlog :as kvtx]
   [datalevin.protocol :as p]
   [datalevin.server.deps :as sdeps]
   [datalevin.txlog :as txlog]
   [datalevin.util :as u :refer [raise]]
   [taoensso.timbre :as log])
  (:import
   [java.nio ByteBuffer]
   [java.nio.channels ClosedChannelException SelectionKey SocketChannel]))

(def dispatch-deps-contract
  "Callbacks (and the handler table) `datalevin.server` must inject for
  message dispatch."
  {:callbacks
   #{:cleanup-connection-transactions-fn :cleanup-rejected-close-transact!-fn
     :touch-client-fn :close-conn-fn :dbs-fn :get-kv-store-fn
     :ha-write-commit-check-fn-fn :ha-write-commit-publish-fn-fn
     :new-message-fn :trace-remote-tx-fn :update-db-fn
     :with-db-runtime-read-access-fn :with-ha-write-admission-fn
     :write-message-fn}
   :values {:message-handler-map map?}})

(defn- missing-withtxn-error
  [db-name type reason]
  {:error :ha/write-indeterminate
   :indeterminate? true
   :reason reason
   :db-name db-name
   :type type})

(defn- selection-key-client-id
  [^SelectionKey skey]
  (some-> skey .attachment deref :client-id))

(defn- transaction-owner?
  [skey runner-skey]
  ;; A transaction belongs to this exact TCP connection and its native thread.
  ;; Another socket, even in the same authenticated session, cannot inherit it.
  (identical? skey runner-skey))

(defn- close-type-for-abort
  [type]
  (case type
    :abort-transact :close-transact
    :abort-transact-kv :close-transact-kv
    nil))

(defn- aborted-close-marker
  [^SelectionKey skey close-type]
  (when-let [client-id (selection-key-client-id skey)]
    {:client-id client-id
     :type close-type}))

(defn- remember-idempotent-abort!
  [deps server db-name skey abort-type]
  (when-let [marker (aborted-close-marker skey
                                          (close-type-for-abort abort-type))]
    ((:update-db-fn deps) server db-name
     (fn [state]
       (if (:runner state)
         state
         (assoc state :aborted-transaction-close marker))))))

(defn- consume-aborted-close!
  [deps server db-name skey close-type]
  (let [marker    (aborted-close-marker skey close-type)
        consumed? (volatile! false)]
    (when marker
      ((:update-db-fn deps) server db-name
       (fn [state]
         (if (and (nil? (:runner state))
                  (= marker (:aborted-transaction-close state)))
           (do
             (vreset! consumed? true)
             (dissoc state :aborted-transaction-close))
           state))))
    @consumed?))

(defn- message-db-name
  [{:keys [args db-name]}]
  (when-let [db-name (or db-name (nth args 0 nil))]
    (if (string? db-name)
      (u/lisp-case db-name)
      db-name)))

(defn- replica-read-only-error
  [deps server message]
  (when (cmd/replica-write? (:type message))
    (let [db-name (message-db-name message)
          m       (and db-name (get ((:dbs-fn deps) server) db-name))]
      (when (:replica/read-only? m)
        {:error :replica/read-only
         :db-name db-name
         :type (:type message)
         :message "Replica is read-only"}))))

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

(defn- call-with-write-guards
  [deps server message f]
  (binding [txlog/*commit-payload-ha-term*
            (current-ha-txlog-term deps server (first (:args message)))
            cpp/*before-write-commit-fn*
            ((:ha-write-commit-check-fn-fn deps) server message)
            kvtx/*after-txlog-append-fn*
            ((:ha-write-commit-publish-fn-fn deps) server message)]
    (f)))

(defn with-index-write-admission
  "Admit the persistent initialization branch of an index-open request.
  Existing-only opens never enter this path. Rejections throw before `f` runs."
  [deps server message f]
  (when-let [err (replica-read-only-error deps server message)]
    (raise "Replica is read-only" err))
  (let [{:keys [ok? error result]}
        ((:with-ha-write-admission-fn deps)
         server message #(call-with-write-guards deps server message f))]
    (if ok?
      result
      (raise "HA write admission rejected" error))))

(defn dispatch-message-with-ha-write-admission
  [deps server ^SelectionKey skey message]
  (if (or (cmd/deferred-write? (:type message))
          (cmd/read-only? (:type message)))
    (dispatch-message deps server skey message)
    (let [type          (:type message)
          transaction   (cmd/transaction-control type)
          cleanup-only? (= :abort transaction)
          write?        (and (not cleanup-only?) (cmd/ha-write? type))
          precheck-only? (= :open transaction)
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
                (call-with-write-guards
                 deps server message
                 (fn [] (dispatch-message deps server skey message)))

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
        (dispatch-message deps server skey message)))))

(defn dispatch-message
  [deps server ^SelectionKey skey message]
  (if-let [handler (get (sdeps/value deps :message-handler-map)
                        (:type message))]
    (handler server skey message)
    (error-response skey
                    (str "Unknown message type " (:type message))
                    {})))

(defn- runtime-read-access-message?
  [{:keys [type writing?]}]
  (and (not writing?)
       (not (cmd/runtime-read-access-exempt? type))))

(defn handle-writing
  [deps server ^SelectionKey skey {:keys [args] :as message}]
  (try
    (let [db-name     (nth args 0)
          type        (:type message)
          _           ((:trace-remote-tx-fn deps) "handle-writing" type db-name)
          _           ((:get-kv-store-fn deps) server db-name)
          db-state    (get ((:dbs-fn deps) server) db-name)
          runner      (:runner db-state)
          runner-skey (:runner-skey db-state)]
      (cond
        (and runner (transaction-owner? skey runner-skey))
        ((:new-message-fn deps) runner skey message)

        runner
        (raise "Active transaction belongs to another client"
                 (missing-withtxn-error db-name type
                                        :transaction-owner-mismatch))

        (= :abort (cmd/transaction-control type))
        (do
          (remember-idempotent-abort! deps server db-name skey type)
          ((:write-message-fn deps) skey {:type :command-complete}))

        (and (= :close (cmd/transaction-control type))
             (consume-aborted-close! deps server db-name skey type))
        ((:write-message-fn deps) skey {:type :command-complete})

        (= :close (cmd/transaction-control type))
        (raise "Cannot confirm a transaction that is no longer active"
                 (missing-withtxn-error db-name type
                                        :missing-transaction))

        :else
        (raise "No active with-transaction runner"
                 (missing-withtxn-error db-name type
                                        :missing-transaction))))
    (catch Exception e
      (error-response skey
                      (str "Error Handling with-transaction message:"
                           (ex-message e))
                      (or (ex-data e) {})))))

(defn- set-last-active
  [deps server ^SelectionKey skey]
  (let [{:keys [client-id]} @(.attachment skey)]
    (when client-id
      ((:touch-client-fn deps) server client-id))))

(defn- read-message
  [deps server ^SelectionKey skey fmt msg]
  (try
    (let [{:keys [wire-opts request-decoder]} @(.attachment skey)
          {:keys [type] :as message}
          (p/read-request fmt msg wire-opts (or request-decoder (p/request-decoder)))]
      (if (= type :set-client-id)
        (do (dispatch-message deps server skey message) ::handled)
        message))
    (catch InterruptedException e (throw e))
    (catch Exception e
      (handle-message-error! deps skey e)
      ::handled)))

(defn- transaction-message?
  [message]
  (or (:writing? message)
      (#{:close :abort} (cmd/transaction-control (:type message)))))

(defn- handle-decoded-message
  [deps server skey message]
  (when-not (= ::handled message)
    (log/debug "Message received:" (dissoc message :password :args))
    (set-last-active deps server skey)
    (if-let [err (when-not (and (not (:writing? message))
                               (cmd/deferred-write? (:type message)))
                  (replica-read-only-error deps server message))]
      (error-response skey "Replica is read-only" err)
      ;; Ownership and native transaction affinity also apply when a client
      ;; omits :writing? on close/abort.
      (if (transaction-message? message)
        (handle-writing deps server skey message)
        (let [dispatch! #(dispatch-message-with-ha-write-admission
                          deps server skey message)]
          (if (runtime-read-access-message? message)
            ((:with-db-runtime-read-access-fn deps) server message dispatch!)
            (dispatch!)))))))

(defn handle-message
  "Decode, execute and reply on the connection's owning thread."
  ([deps server skey message]
   (try
     (handle-decoded-message deps server skey message)
     (catch InterruptedException e (throw e))
     (catch Exception e
       (handle-message-error! deps skey e))))
  ([deps server skey fmt msg]
   (handle-message deps server skey (read-message deps server skey fmt msg))))

(defn- close-read-connection!
  [deps server skey]
  (try
    (try
      ((:cleanup-connection-transactions-fn deps) server skey)
      (finally ((:close-conn-fn deps) skey)))
    (catch Exception e
      ;; The channel has already been closed in finally. Do not run cleanup
      ;; again through the surrounding read-error handler.
      (log/warn "Client connection cleanup failed" {:message (ex-message e)})
      (log/debug e "Client connection cleanup failure"))))

(defn handle-read
  "Serve a blocking connection until EOF or cancellation. Process buffered
  frames in order, finishing each handler (including bulk transfers) before
  reading the next request. No request queue or selector rearming is needed."
  [deps server ^SelectionKey skey]
  (let [state (.attachment skey)
        ^SocketChannel ch (.channel skey)]
    (try
      (loop []
        (when (and (.isOpen ch) (not (.isInterrupted (Thread/currentThread))))
          (let [^ByteBuffer read-bf (:read-bf @state)]
            (if (p/extract-message read-bf
                                  #(read-message deps server skey %1 %2)
                                  (fn [_ message]
                                    (handle-message deps server skey message)))
              ;; Copy-in may have grown the shared buffer. Fetch it afresh.
              (recur)
              (let [^ByteBuffer read-bf
                    (if (.hasRemaining read-bf)
                      read-bf
                      (let [buffer (bf/allocate-buffer
                                     (* (long c/+buffer-grow-factor+)
                                        (.capacity read-bf)))]
                        (.flip read-bf)
                        (bf/buffer-transfer read-bf buffer)
                        (vswap! state assoc :read-bf buffer)
                        buffer))
                    ^int readn (p/read-ch ch read-bf)]
                (when (pos? readn) (recur)))))))
      (catch InterruptedException _ nil)
      (catch Exception e
        (when-not (client-disconnect? e)
          (log/debug e "Closing failed client read")))
      (finally
        ;; Interrupts cancel transport waits; native abort still runs here on
        ;; the same thread that opened the transaction.
        (Thread/interrupted)
        (close-read-connection! deps server skey)))))
