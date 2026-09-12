;;
;; Copyright (c) Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns ^:no-doc datalevin.remote
  "Proxy for remote stores"
  (:refer-clojure :exclude [sync])
  (:require
   [datalevin.client-op :as cop]
   [datalevin.util :as u :refer [ deftype+ raise]]
   [datalevin.constants :as c]
   [datalevin.interface]
   [datalevin.client :as cl]
   [datalevin.bits :as b]
   [datalevin.datom :as d]
   [datalevin.lmdb :as l :refer [IWriting]]
   [datalevin.native-value :as nv]
   [clojure.string :as str])
  (:import
   [datalevin.client Client]
   [datalevin.interface ICustomTypes ILMDB ITxLog IList IAdmin IStore ISearchEngine IVectorIndex]
   [clojure.lang Seqable IReduceInit]
   [java.lang AutoCloseable]
   [java.util.concurrent ConcurrentHashMap]
   [java.util.concurrent.atomic AtomicBoolean AtomicLong]
   [java.nio.file Files Paths StandardOpenOption LinkOption]
   [java.security MessageDigest]
   [java.net URI]))

(def ^:dynamic *chatty-kv-detect-threshold*
  "Minimum streak length before recording a chatty remote point-read detection."
  32)

(def ^:dynamic *chatty-kv-detect-window-ms*
  "Maximum gap between sequential calls in a streak."
  100)

(defonce ^:private ^ConcurrentHashMap chatty-kv-seq-state
  (ConcurrentHashMap.))

(defonce ^:private ^ConcurrentHashMap chatty-kv-stats*
  (ConcurrentHashMap.))

(defn reset-chatty-kv-stats!
  "Reset runtime chatty-KV detection state and counters."
  []
  (.clear chatty-kv-seq-state)
  (.clear chatty-kv-stats*)
  nil)

(defn chatty-kv-stats
  "Return detected chatty remote KV calls as {[db-name dbi-name op] count}."
  []
  (into {}
        (map (fn [^java.util.Map$Entry e]
               [(.getKey e) (.getValue e)]))
        (.entrySet chatty-kv-stats*)))

(defn- inc-chatty-kv-stat!
  [stat-key]
  (let [n (.get chatty-kv-stats* stat-key)]
    (.put chatty-kv-stats* stat-key (if n (inc ^long n) 1))))

(defn- detect-chatty-kv!
  [db-name dbi-name op]
  (let [now-ms    (System/currentTimeMillis)
        tid       (.getId (Thread/currentThread))
        state-key [tid db-name dbi-name op]
        prev      (.get chatty-kv-seq-state state-key)
        streak    (if (and prev
                           (<= (- now-ms ^long (:ts prev))
                               ^long *chatty-kv-detect-window-ms*))
                    (inc ^long (:streak prev))
                    1)]
    (.put chatty-kv-seq-state state-key {:ts now-ms :streak streak})
    (when (>= streak ^long *chatty-kv-detect-threshold*)
      (inc-chatty-kv-stat! [db-name dbi-name op]))))

(defn dtlv-uri?
  "return true if the given string is a Datalevin connection string"
  [s]
  (when s (str/starts-with? s "dtlv://")))

(defn redact-uri
  [s]
  (if (dtlv-uri? s)
    (str/replace-first s #"(dtlv://.+):(.+)@" "$1:***@")
    s))

(defn- normalize-remote-tx-data
  [tx-data]
  (mapv (fn [tx]
          (if (d/datom? tx)
            tx
            (apply d/datom tx)))
        tx-data))

(defn- copy-out-tx-response-meta
  [response]
  (select-keys response [:db-info :new-attributes]))

(defn- cached-ha-member-endpoints
  [db-info-or-opts]
  (let [opts (cond
               (and (instance? clojure.lang.IDeref db-info-or-opts)
                    (map? @db-info-or-opts))
               (or (:opts @db-info-or-opts) @db-info-or-opts)

               (map? db-info-or-opts)
               (or (:opts db-info-or-opts) db-info-or-opts)

               :else
               nil)]
    (->> (:ha-members opts)
         (keep :endpoint)
         distinct
         vec)))

(defn- retry-ha-transport-failure
  [client req request-fn known-endpoints throwable]
  (when (nv/decoding-error? throwable) (throw throwable))
  (when-let [retry-context (#'cl/client-retry-context client)]
    (let [self-endpoint (str (:host retry-context) ":" (:port retry-context))
          retry-endpoints (->> known-endpoints
                               (remove #(= self-endpoint %))
                               vec)]
      (when (seq retry-endpoints)
        (let [retry-result
              (#'cl/retry-ha-write-request*
               req
               (or (ex-message throwable)
                   "HA write target became unavailable")
               {:error :ha/write-rejected
                :reason :endpoint-unreachable
                :retryable? true
                :ha-retry-endpoints retry-endpoints}
               retry-context
               request-fn
               cl/disconnect
               #'cl/new-client-for-endpoint
               #(#'cl/set-preferred-ha-endpoint! client %))]
          (cond-> {:type :command-complete
                   :result retry-result}
            (map? retry-result)
            (merge (select-keys retry-result [:db-info :new-attributes]))))))))

(defn- request-ha-open
  [client req]
  (if-let [retry-context (#'cl/client-retry-context client)]
    (let [preferred-attempt
          (#'cl/try-preferred-ha-write-request*
           client
           req
           retry-context
           cl/request
           cl/disconnect
           #'cl/new-client-for-endpoint
           #'cl/retry-ha-write-request)]
      (if (:handled? preferred-attempt)
        (:result preferred-attempt)
        (let [{:keys [type message result err-data]} (cl/request client req)]
          (if (= type :error-response)
            (if (#'cl/retryable-ha-write-reject? err-data)
              (#'cl/retry-ha-write-request client req message err-data)
              (#'cl/raise-normal-request-error req message err-data nil))
            (do
              (#'cl/clear-preferred-ha-endpoint! client)
              result)))))
    (let [{:keys [type message result err-data]} (cl/request client req)]
      (if (= type :error-response)
        (#'cl/raise-normal-request-error req message err-data nil)
        result))))

(defn- disable-ha-transaction-retry!
  [routing-client]
  (let [active-client (#'cl/active-ha-request-client routing-client)]
    ;; The open request has already selected the exact endpoint client whose
    ;; server session owns this transaction. A cached endpoint client can have
    ;; a stale preference of its own, so remove that nested route before using
    ;; it for transaction data and control messages. Keep both the routing
    ;; facade and the selected endpoint pinned for the lifetime of the explicit
    ;; transaction.
    (#'cl/clear-preferred-ha-endpoint! active-client)
    (#'cl/disable-ha-write-retry! routing-client)
    (when-not (identical? routing-client active-client)
      (#'cl/disable-ha-write-retry! active-client))
    active-client))

(defn- enable-ha-transaction-retry!
  [routing-client]
  (let [active-client (#'cl/active-ha-request-client routing-client)]
    (#'cl/enable-ha-write-retry! routing-client)
    (when-not (identical? routing-client active-client)
      (#'cl/enable-ha-write-retry! active-client))))

(defn- close-ha-transaction!
  [routing-client request-type db-name]
  ;; On failure, keep routing pinned so the with-transaction cleanup path can
  ;; send abort to the session that still owns the transaction.
  (let [result (#'cl/normal-request routing-client request-type [db-name] true)]
    (enable-ha-transaction-retry! routing-client)
    result))

(defn- abort-ha-transaction!
  [routing-client request-type db-name]
  (try
    (#'cl/normal-request routing-client request-type [db-name] true)
    (finally
      (enable-ha-transaction-retry! routing-client))))

(defn- load-datoms*
  ([client db-name datoms datom-type simulated?]
   (load-datoms* client db-name datoms datom-type simulated? false nil))
  ([client db-name datoms datom-type simulated? writing?]
   (load-datoms* client db-name datoms datom-type simulated? writing? nil))
  ([client db-name datoms datom-type simulated? writing? ha-source]
   (let [tx? (#{:txs :txs+info} datom-type)
         t   (case datom-type
               :txs      :tx-data
               :txs+info :tx-data+db-info
               :load-datoms)
         client-op-id (when (and writing? tx?)
                        (cop/new-client-op-id))
         client-op-hash (when client-op-id
                          (cop/request-hash
                           (cop/tx-request-payload t db-name datoms
                                                   simulated?)))
         response-kind (case t
                         :tx-data+db-info cop/tx-data+db-info-response-kind
                         :tx-data cop/tx-data-response-kind
                         nil)
         req (if (< (count datoms) ^long c/+wire-datom-batch-size+)
               (cond-> {:type     t
                        :mode     :request
                        :writing? writing?
                        :args     (if tx?
                                    [db-name datoms simulated?]
                                    [db-name datoms])}
                 client-op-id
                 (assoc :client-op-id client-op-id
                        :client-op-hash client-op-hash
                        :client-op-response-kind response-kind))
               (cond-> {:type     t
                        :mode     :copy-in
                        :writing? writing?
                        :args     (if tx?
                                    [db-name simulated?]
                                    [db-name])}
                 client-op-id
                 (assoc :client-op-id client-op-id
                        :client-op-hash client-op-hash
                        :client-op-response-kind response-kind)))
         request-fn
         (fn [retry-client req]
           (if (= :copy-in (:mode req))
             (cl/copy-in retry-client req datoms c/+wire-datom-batch-size+)
             (cl/request retry-client req)))
         response
         (try
           (request-fn client req)
           (catch Exception e
             (or (and client-op-id
                      (retry-ha-transport-failure
                       client
                       req
                       request-fn
                       (cached-ha-member-endpoints ha-source)
                       e))
                 (throw e))))
         {:keys [type message result err-data]
          :as   response}
         response]
     (if (= type :error-response)
       (if (:resized err-data)
         (raise message err-data)
         (if (#'cl/retryable-ha-write-reject? err-data)
           (if-let [retry-context (#'cl/client-retry-context client)]
             (#'cl/retry-ha-write-request*
             req
              message
              err-data
              retry-context
              request-fn
              cl/disconnect
              #'cl/new-client-for-endpoint
              #(#'cl/set-preferred-ha-endpoint! client %))
             (#'cl/raise-normal-request-error req message err-data nil))
           (#'cl/raise-normal-request-error req message err-data nil)))
       (cond
         (and tx? (map? result) (contains? result :tx-data))
         (merge
           (update result :tx-data normalize-remote-tx-data)
           (copy-out-tx-response-meta response))

         (and tx? (sequential? result))
         (let [[tx-data tempids] (split-with d/datom? result)]
           (merge
             {:tx-data (normalize-remote-tx-data tx-data)
              :tempids (apply hash-map tempids)}
             (copy-out-tx-response-meta response)))

         :else
         result)))))

;; remote datalog db

(defprotocol IRemoteDB
  (q [store query inputs]
    "For special case of queries with a single remote store as source,
     send the query and inputs over to remote server")
  (pull [store pattern id opts])
  (pull-many [store pattern id opts])
  (explain [store opts query inputs])
  (fulltext-datoms [store query opts])
  (db-info [store]
    "Fetch all DB initialization info in a single round trip")
  (tx-data [store data simulated?]
    "Send to remote server the data from call to `db/transact-tx-data`")
  (open-transact [store])
  (abort-transact [store])
  (close-transact [store])
  )

(declare ->DatalogStore)

(defn- update-read-floor-tx!
  [^AtomicLong read-floor-tx tx]
  (when (integer? tx)
    (let [tx (long tx)]
      (loop []
        (let [current (.get read-floor-tx)]
          (when (and (> tx current)
                     (not (.compareAndSet read-floor-tx current tx)))
            (recur))))))
  read-floor-tx)

(defn- current-read-floor-tx
  [^AtomicLong read-floor-tx]
  (let [tx (.get read-floor-tx)]
    (when (pos? tx) tx)))

(defn- background-sampling?
  [open-db-info]
  (let [opts (:opts @open-db-info)]
    (and (not= :consensus-lease (:ha-mode opts))
         (not (false? (:background-sampling? opts))))))

(def ^:private sampling-open-info? ::sampling-open-info?)

(defn- sampling-open-info
  [db-info]
  (when-let [opts (not-empty
                    (select-keys (:opts db-info)
                                 [:ha-mode :background-sampling?]))]
    {sampling-open-info? true
     :opts opts}))

(defn- merge-client-sampling-opts
  [db-info opts]
  (let [sampling-opts (select-keys opts [:ha-mode :background-sampling?])]
    (cond-> db-info
      (and (map? db-info) (seq sampling-opts))
      (update :opts #(merge (or % {}) sampling-opts)))))

(defn- datalog-request
  [^AtomicLong read-floor-tx client call args writing?]
  (binding [cl/*ha-read-min-tx* (when-not writing?
                                  (current-read-floor-tx read-floor-tx))]
    (cl/normal-request client call args writing?)))

(defn- remote-forward-method
  [request [mname args & flags]]
  (let [opts      (apply hash-map flags)
        wire-op   (or (:op opts) (keyword (name mname)))
        freeze    (:serialize opts)
        frozen    (some-> freeze (as-> f (symbol (str "frozen-" (name f)))))
        wire-args (mapv #(if (and frozen (= % freeze)) frozen %) (rest args))
        call      (concat request
                          [wire-op (into ['db-name] wire-args) 'writing?])
        call      (if (:chatty opts)
                    (list 'do
                          (list 'detect-chatty-kv! 'db-name 'dbi-name wire-op)
                          call)
                    call)]
    (list mname args
          (if frozen
            (list 'let [frozen (list 'b/serialize freeze)] call)
            call))))

(defmacro defremote-forward
  "Expand to remote protocol method forms that forward to a wire request.

  `request` is a call prefix such as `(cl/normal-request client)` or
  `(datalog-request read-floor-tx client)`. Each spec is
  `(method-name [args...] & flags)` where flags are key/value pairs:
    `:op op`          wire op keyword, defaults to the method name
    `:chatty true`    record a chatty-KV streak before the request
    `:serialize sym`  serialize `sym` and send it as `frozen-sym`

  The wire arguments are `db-name` followed by the method arguments after
  the leading store argument."
  [request & specs]
  (cons 'do (map #(remote-forward-method request %) specs)))

(defmacro defremote-txlog-methods
  "Expand to the ITxLog methods shared by the remote store types."
  []
  (cons
    'do
    (concat
      (map #(remote-forward-method '(cl/normal-request client) %)
           '[(txlog-watermarks [_])
             (open-tx-log [_ from-lsn upto-lsn])
             (force-txlog-sync! [_])
             (force-lmdb-sync! [_])
             (create-snapshot! [_])
             (list-snapshots [_])
             (snapshot-scheduler-state [_])
             (read-commit-marker [_])
             (verify-commit-marker! [_])
             (txlog-retention-state [_])
             (gc-txlog-segments! [_ retain-floor-lsn])
             (txlog-update-snapshot-floor! [_ snapshot-lsn
                                            previous-snapshot-lsn])
             (txlog-clear-snapshot-floor! [_])
             (txlog-update-replica-floor! [_ replica-id applied-lsn])
             (txlog-clear-replica-floor! [_ replica-id])
             (txlog-pin-backup-floor! [_ pin-id floor-lsn expires-ms])
             (txlog-unpin-backup-floor! [_ pin-id])])
      '[(open-tx-log [this from-lsn]
          (.open-tx-log this from-lsn nil))
        (gc-txlog-segments! [this]
          (.gc-txlog-segments! this nil))
        (txlog-update-snapshot-floor! [this snapshot-lsn]
          (.txlog-update-snapshot-floor! this snapshot-lsn nil))
        (txlog-pin-backup-floor! [this pin-id floor-lsn]
          (.txlog-pin-backup-floor! this pin-id floor-lsn nil))])))

(deftype+ DatalogStore [^String uri
                       ^String db-name
                       ^Client client
                       ^Client tx-client
                       write-txn
                       writing?
                       open-db-info
                       ^AtomicLong read-floor-tx
                       ^AtomicBoolean sampling-started?
                       owns-client?
                       ^AtomicBoolean closed?]
  ICustomTypes
  (defremote-forward (datalog-request read-floor-tx client)
    (register-type [_ type-name definition]
      :op :datalog-register-type :serialize definition))

  IWriting
  (writing? [_] writing?)

  (write-txn [_] write-txn)

  (mark-write [_]
    (DatalogStore. uri db-name tx-client tx-client
                   (volatile! :remote-dl-mutex) true
                   open-db-info
                   read-floor-tx
                   sampling-started?
                   owns-client?
                   closed?))

  IStore
  (defremote-forward (datalog-request read-floor-tx client)
    (opts [_])
    (assoc-opt [_ k v])
    (assoc-opts [_ kvs]))

  (db-name [_] db-name)

  (dir [_] uri)

  (close [_]
    (when (.compareAndSet closed? false true)
      (when-not (cl/disconnected? client)
        ;; Closing a remote store detaches the client/session view of the DB.
        ;; It is not a with-transaction control message and must not route
        ;; through the server's write-runner path.
        (datalog-request read-floor-tx client :close [db-name] false))
      (when (and owns-client? (not (cl/disconnected? client)))
        (cl/disconnect client))
      (when (and (not (identical? tx-client client))
                 (not (cl/disconnected? tx-client)))
        (cl/disconnect tx-client))))

  (closed? [_]
    (if (or (.get closed?) (cl/disconnected? client))
      true
      (datalog-request read-floor-tx client :closed? [db-name] writing?)))

  (defremote-forward (datalog-request read-floor-tx client)
    (last-modified [_])
    (schema [_])
    (rschema [_])
    (set-schema [_ new-schema del-attrs rename-map])
    (init-max-eid [_])
    (max-tx [_]))

  (set-schema [this new-schema]
    (.set-schema this new-schema nil nil))

  (swap-attr [this attr f]
    (.swap-attr this attr f nil nil))
  (swap-attr [this attr f x]
    (.swap-attr this attr f x nil))

  (defremote-forward (datalog-request read-floor-tx client)
    (swap-attr [_ attr f x y] :serialize f)
    (del-attr [_ attr])
    (rename-attr [_ attr new-attr])
    (datom-count [_ index]))

  (load-datoms [_ datoms]
    (load-datoms* client db-name datoms :raw false writing?))

  (defremote-forward (datalog-request read-floor-tx client)
    (fetch [_ datom])
    (populated? [_ index low-datom high-datom])
    (size [_ index low-datom high-datom])
    (head [_ index low-datom high-datom])
    (tail [_ index high-datom low-datom])
    (slice [_ index low-datom high-datom])
    (rslice [_ index high-datom low-datom])
    (e-datoms [_ e])
    (e-first-datom [_ e]))

  (start-sampling [_]
    (when (and (background-sampling? open-db-info)
               (.compareAndSet sampling-started? false true))
      (try
        (datalog-request read-floor-tx client :start-sampling
                         [db-name] writing?)
        (catch Exception e
          (.set sampling-started? false)
          (throw e)))))

  (stop-sampling [_]
    (when (.compareAndSet sampling-started? true false)
      (try
        (datalog-request read-floor-tx client :stop-sampling
                         [db-name] writing?)
        (catch Exception e
          (.set sampling-started? true)
          (throw e)))))

  (defremote-forward (datalog-request read-floor-tx client)
    (analyze [_ attr])
    (av-datoms [_ a v])
    (av-first-datom [_ a v])
    (av-first-e [_ a v])
    (ea-first-datom [_ e a])
    (ea-first-v [_ e a])
    (v-datoms [_ v]))

  (defremote-forward (datalog-request read-floor-tx client)
    (size-filter [_ index pred low-datom high-datom] :serialize pred)
    (head-filter [_ index pred low-datom high-datom] :serialize pred)
    (tail-filter [_ index pred high-datom low-datom] :serialize pred)
    (slice-filter [_ index pred low-datom high-datom] :serialize pred)
    (rslice-filter [_ index pred high-datom low-datom] :serialize pred))

  IRemoteDB
  (defremote-forward (datalog-request read-floor-tx client)
    (q [_ query inputs])
    (pull [_ pattern id opts])
    (pull-many [_ pattern id opts])
    (explain [_ opts query inputs])
    (fulltext-datoms [_ query opts]))

  (db-info [_]
    (if-let [cached (let [cached @open-db-info]
                      (when-not (get cached sampling-open-info?)
                        cached))]
      (do
        (vreset! open-db-info (sampling-open-info cached))
        (update-read-floor-tx! read-floor-tx (:max-tx cached))
        cached)
      (let [info (datalog-request read-floor-tx client :db-info
                                  [db-name] writing?)]
        (update-read-floor-tx! read-floor-tx (:max-tx info))
        info)))

  (tx-data [_ data simulated?]
    (let [result (load-datoms* client db-name data :txs+info simulated?
                               writing? open-db-info)]
      (update-read-floor-tx! read-floor-tx (get-in result [:db-info :max-tx]))
      result))

  (open-transact [this]
    (#'cl/sync-ha-routing! client tx-client)
    (#'request-ha-open tx-client {:type :open-transact
                                  :args [db-name]
                                  :writing? false})
    (#'cl/sync-ha-routing! tx-client client)
    (let [active-client (disable-ha-transaction-retry! tx-client)]
      (DatalogStore. uri db-name active-client active-client
                     (volatile! :remote-dl-mutex) true
                     open-db-info
                     (AtomicLong. (long (or (current-read-floor-tx
                                               read-floor-tx)
                                             0)))
                     sampling-started?
                     owns-client?
                     closed?)))

  (abort-transact [this]
    (abort-ha-transaction! tx-client :abort-transact db-name))

  (close-transact [_]
    (close-ha-transaction! tx-client :close-transact db-name))

  ILMDB
  (kv-info [_] nil)
  (sync [this] (.sync this 1))
  (sync [_ force]
    (cl/normal-request client :sync [db-name force] writing?))

  ITxLog
  (defremote-txlog-methods)

  IAdmin
  (re-index [_ schema opts]
    (cl/normal-request client :datalog-re-index [db-name schema opts])))

#_{:clj-kondo/ignore [:redefined-var]}
(defn ->DatalogStore
  ([uri db-name client write-txn writing? open-db-info
    sampling-started? owns-client? closed?]
   (DatalogStore. uri db-name client client
                  write-txn writing?
                  open-db-info
                  (AtomicLong. (long (or (:max-tx @open-db-info) 0)))
                  sampling-started?
                  owns-client?
                  closed?))
  ([uri db-name client tx-client write-txn writing? open-db-info
    sampling-started? owns-client? closed?]
   (DatalogStore. uri db-name client tx-client
                  write-txn writing?
                  open-db-info
                  (AtomicLong. (long (or (:max-tx @open-db-info) 0)))
                  sampling-started?
                  owns-client?
                  closed?)))

(defn open
  "Open a remote Datalog store"
  ([uri-str]
   (open uri-str nil))
  ([uri-str schema]
   (let [client (cl/new-client uri-str)]
     (open client uri-str schema nil true)))
  ([uri-str schema opts]
   (let [client (cl/new-client uri-str (:client-opts opts))]
     (open client uri-str schema opts true)))
  ([client uri-str schema opts]
   (open client uri-str schema opts false))
  ([client uri-str schema opts owns-client?]
   (let [uri (URI. uri-str)]
     (if-let [db-name (cl/parse-db uri)]
       (let [store (or (get (cl/parse-query uri) "store")
                       c/db-store-datalog)
             db-info (merge-client-sampling-opts
                       (cl/open-database client db-name store schema opts true)
                       opts)]
         (->DatalogStore uri-str db-name client
                         (cl/dedicated-transaction-client client)
                         (volatile! :remote-dl-mutex) false
                         (volatile! db-info)
                         (AtomicBoolean. false)
                         owns-client?
                         (AtomicBoolean. false)))
       (raise "URI should contain a database name" {})))))

;; remote kv store

(declare ->KVStore)

(defn- backward-range-type?
  [range-type]
  (str/ends-with? (name range-type) "-back"))

(defn- base-range-type
  [range-type]
  (if (backward-range-type? range-type)
    (keyword (subs (name range-type) 0 (- (count (name range-type)) 5)))
    range-type))

(defn- k-range->interval
  [[range-type k1 k2 :as k-range]]
  (let [back? (backward-range-type? range-type)
        base  (base-range-type range-type)]
    (case base
      :all         {:back? back? :lower nil :upper nil}
      :at-least    {:back? back? :lower {:v k1 :incl? true}  :upper nil}
      :greater-than {:back? back? :lower {:v k1 :incl? false} :upper nil}
      :at-most     {:back? back? :lower nil :upper {:v k1 :incl? true}}
      :less-than   {:back? back? :lower nil :upper {:v k1 :incl? false}}
      :closed      {:back? back?
                    :lower {:v (if back? k2 k1) :incl? true}
                    :upper {:v (if back? k1 k2) :incl? true}}
      :closed-open {:back? back?
                    :lower {:v (if back? k2 k1) :incl? true}
                    :upper {:v (if back? k1 k2) :incl? false}}
      :open        {:back? back?
                    :lower {:v (if back? k2 k1) :incl? false}
                    :upper {:v (if back? k1 k2) :incl? false}}
      :open-closed {:back? back?
                    :lower {:v (if back? k2 k1) :incl? false}
                    :upper {:v (if back? k1 k2) :incl? true}}
      (raise "Unsupported key range type for remote range-seq"
               {:k-range k-range}))))

(defn- interval->k-range
  [{:keys [back? lower upper]}]
  (let [base (cond
               (and (nil? lower) (nil? upper))
               :all

               (and lower (nil? upper))
               (if (:incl? lower) :at-least :greater-than)

               (and (nil? lower) upper)
               (if (:incl? upper) :at-most :less-than)

               (and lower upper)
               (cond
                 (and (:incl? lower) (:incl? upper))  :closed
                 (and (:incl? lower) (not (:incl? upper))) :closed-open
                 (and (not (:incl? lower)) (not (:incl? upper))) :open
                 :else :open-closed))
        range-type (if back?
                     (keyword (str (name base) "-back"))
                     base)]
    (cond
      (and (nil? lower) (nil? upper))
      [range-type]

      (and lower (nil? upper))
      [range-type (:v lower)]

      (and (nil? lower) upper)
      [range-type (:v upper)]

      back?
      [range-type (:v upper) (:v lower)]

      :else
      [range-type (:v lower) (:v upper)])))

(defn- advance-k-range
  [k-range next-key]
  (let [{:keys [back?] :as interval} (k-range->interval k-range)
        interval'                    (if back?
                                       (assoc interval :upper {:v next-key :incl? true})
                                       (assoc interval :lower {:v next-key :incl? true}))]
    (interval->k-range interval')))

(defn- project-range-item
  [item ignore-key? v-type]
  (if ignore-key?
    (if (= v-type :ignore) true (second item))
    item))

(defn- range-page
  [page]
  (if (vector? page) page (vec page)))

(defn- project-range-batch
  [chunk ignore-key? v-type]
  (if ignore-key?
    (persistent!
      (reduce (fn [acc item]
                (conj! acc (project-range-item item true v-type)))
              (transient [])
              chunk))
    chunk))

(defn- find-index
  [item coll]
  (second (u/some-indexed #(= item %) coll)))

(defn- trailing-run-count
  [chunk]
  (if (seq chunk)
    (let [last-k (first (peek chunk))]
      (loop [i (dec (count chunk))
             n 0]
        (if (and (>= i 0) (= last-k (first (nth chunk i))))
          (recur (dec i) (unchecked-inc n))
          n)))
    0))

(defn- remote-range-seq*
  [fetch-first-n dbi-name k-range k-type v-type ignore-key? opts]
  (let [batch-size (max 1 (long (or (:batch-size opts) 100)))
        fetch      (fn [{:keys [k-range resume resume-run-count]}]
                     (loop [request-n (unchecked-inc
                                       (unchecked-add batch-size
                                                      (long resume-run-count)))]
                       (let [raw       (range-page
                                         (fetch-first-n dbi-name request-n
                                                        k-range k-type v-type))
                             raw-count (count raw)]
                         (if resume
                           (if-some [idx (find-index resume raw)]
                             (let [idx*       (long idx)
                                   tail-count (- raw-count
                                                 (unchecked-inc idx*))]
                               (if (and (= raw-count request-n)
                                        (<= tail-count batch-size))
                                 ;; The current page still ends inside the
                                 ;; duplicate-key run anchored at `resume`.
                                 ;; Grow the bounded fetch and retry instead
                                 ;; of materializing the entire remaining range.
                                 (recur (unchecked-add request-n batch-size))
                                 (let [tail    (subvec raw
                                                       (int (unchecked-inc idx*)))
                                       chunk   (if (> tail-count batch-size)
                                                 (subvec tail 0 batch-size)
                                                 tail)
                                       prefix  (subvec raw 0
                                                       (int (unchecked-add
                                                              (unchecked-inc idx*)
                                                              (count chunk))))
                                       batch   (project-range-batch
                                                 chunk ignore-key? v-type)
                                       more?   (> tail-count batch-size)
                                       next-k  (when more? (first (peek chunk)))]
                                   {:batch      batch
                                    :next-state (when next-k
                                                  {:k-range          (advance-k-range
                                                                       k-range
                                                                       next-k)
                                                   :resume           (peek chunk)
                                                   :resume-run-count (trailing-run-count
                                                                       prefix)})})))
                             (if (= raw-count request-n)
                               (recur (unchecked-add request-n batch-size))
                               {:batch [] :next-state nil}))
                           (let [chunk  (if (> raw-count batch-size)
                                          (subvec raw 0 batch-size)
                                          raw)
                                 batch  (project-range-batch chunk ignore-key?
                                                             v-type)
                                 more?  (> raw-count batch-size)
                                 next-k (when more? (first (peek chunk)))]
                             {:batch      batch
                              :next-state (when next-k
                                            {:k-range          (advance-k-range
                                                                 k-range next-k)
                                             :resume           (peek chunk)
                                             :resume-run-count (trailing-run-count
                                                                 chunk)})})))))
        init-state {:k-range k-range :resume nil :resume-run-count 0}]
    (reify
      Seqable
      (seq [_]
        (u/lazy-concat
          ((fn next-page [ret]
             (when (seq (:batch ret))
               (cons (:batch ret)
                     (when-some [state (:next-state ret)]
                       (lazy-seq (next-page (fetch state)))))))
           (fetch init-state))))

      IReduceInit
      (reduce [_ rf init]
        (loop [acc init
               ret (fetch init-state)]
          (if (seq (:batch ret))
            (let [acc (rf acc (:batch ret))]
              (if (reduced? acc)
                @acc
                (if-some [state (:next-state ret)]
                  (recur acc (fetch state))
                  acc)))
            acc)))

      AutoCloseable
      (close [_] nil)

      Object
      (toString [this] (str (apply list this))))))

(deftype+ KVStore [^String uri
                  ^String db-name
                  ^Client client
                  write-txn
                  writing?
                  open-db-opts
                  owns-client?
                  ^AtomicBoolean closed?]
  ICustomTypes
  (defremote-forward (cl/normal-request client)
    (register-type [_ type-name definition] :serialize definition))

  IWriting
  (writing? [_] writing?)

  (write-txn [_] write-txn)

  (mark-write [_]
    (->KVStore uri db-name client (volatile! :remote-kv-mutex) true
               open-db-opts
               owns-client? closed?))

  ILMDB

  (close-kv [_]
    (when (.compareAndSet closed? false true)
      (when-not (cl/disconnected? client)
        (cl/normal-request client :close-kv [db-name]))
      (when (and owns-client? (not (cl/disconnected? client)))
        (cl/disconnect client))))

  (closed-kv? [_]
    (if (or (.get closed?) (cl/disconnected? client))
      true
      (cl/normal-request client :closed-kv? [db-name])))

  (env-dir [_] uri)
  (kv-info [_] nil)

  (open-dbi [db dbi-name]
    (.open-dbi db dbi-name nil))

  (defremote-forward (cl/normal-request client)
    (open-dbi [_ dbi-name opts])
    (clear-dbi [_ dbi-name])
    (drop-dbi [_ dbi-name])
    (list-dbis [_]))

  (copy [db dest] (.copy db dest false))
  (copy [_ dest compact?]
    (let [{:keys [type message result copy-meta copy-format checksum
                  checksum-algo bytes]}
          (cl/request client {:type     :copy
                              :mode     :request
                              :writing? writing?
                              :args     [db-name compact?]})
          _    (when (= type :error-response)
                 (raise "Request to Datalevin server failed: "
                          message
                          {:type :copy
                           :args [db-name compact?]
                           :writing? writing?}))
          dir  (Paths/get dest (into-array String []))
          file (Paths/get (str dest u/+separator+ c/data-file-name)
                          (into-array String []))
          opts (into-array StandardOpenOption
                           [StandardOpenOption/CREATE
                            StandardOpenOption/TRUNCATE_EXISTING
                            StandardOpenOption/WRITE])]
      (when-not (Files/exists dir (into-array LinkOption []))
        (u/create-dirs dest))
      (if (= :binary-chunks copy-format)
        (let [^MessageDigest md (MessageDigest/getInstance "SHA-256")
              written-bytes
              (with-open [out (Files/newOutputStream file opts)]
                (reduce
                  (fn ^long [^long n chunk]
                    (let [^bytes bs chunk
                          len       (alength bs)]
                      (.write out bs 0 len)
                      (.update md bs 0 len)
                      (unchecked-add n len)))
                  0 result))
              actual-checksum (u/hexify (.digest md))]
          (when (and checksum-algo (not= :sha-256 checksum-algo))
            (raise "Unsupported checksum algorithm from server"
                     {:checksum-algo checksum-algo}))
          (when (and bytes (not= (long bytes) (long written-bytes)))
            (raise "Copy size mismatch"
                     {:expected-bytes (long bytes)
                      :actual-bytes   (long written-bytes)}))
          (when (and checksum (not= checksum actual-checksum))
            (raise "Copy checksum mismatch"
                     {:expected-checksum checksum
                      :actual-checksum   actual-checksum})))
        (let [bs (->> result
                      (apply str)
                      b/decode-base64)]
          (Files/write file ^bytes bs
                       ^"[Ljava.nio.file.StandardOpenOption;" opts)))
      (spit (str dest u/+separator+ c/version-file-name) c/version)
      copy-meta))

  (stat [db] (.stat db nil))

  (defremote-forward (cl/normal-request client)
    (stat [_ dbi-name])
    (entries [_ dbi-name])
    (set-env-flags [_ ks on-off])
    (get-env-flags [_]))

  (sync [this] (.sync this 1))

  (defremote-forward (cl/normal-request client)
    (sync [_ force]))

  ITxLog
  (defremote-txlog-methods)

  (open-transact-kv [db]
    (#'request-ha-open client {:type :open-transact-kv
                               :args [db-name]
                               :writing? false})
    (let [active-client (disable-ha-transaction-retry! client)]
      (->KVStore uri db-name active-client
                 (volatile! :remote-kv-mutex) true
                 open-db-opts
                 owns-client?
                 closed?)))

  (close-transact-kv [_]
    (close-ha-transaction! client :close-transact-kv db-name))

  (abort-transact-kv [_]
    (abort-ha-transaction! client :abort-transact-kv db-name))

  (transact-kv [this txs] (.transact-kv this nil txs))
  (transact-kv [this dbi-name txs]
    (.transact-kv this dbi-name txs :data :data))
  (transact-kv [this dbi-name txs k-type]
    (.transact-kv this dbi-name txs k-type :data))
  (transact-kv [_ dbi-name txs k-type v-type]
    (let [txs            (l/expand-datom-kv-txs txs)
          client-op-id   (when writing? (cop/new-client-op-id))
          client-op-hash (when client-op-id
                           (cop/request-hash
                            (cop/kv-request-payload db-name dbi-name txs
                                                    k-type v-type)))
          base-req       (if (< (count txs) ^long c/+wire-datom-batch-size+)
                           {:type     :transact-kv
                            :mode     :request
                            :writing? writing?
                            :args     [db-name dbi-name txs k-type v-type]}
                           {:type     :transact-kv
                            :mode     :copy-in
                            :writing? writing?
                            ;; Keep arg positions aligned with :request mode.
                            ;; Slot 2 is reserved for txs when they travel in-band.
                            :args     [db-name dbi-name nil k-type v-type]})
          req            (cond-> base-req
                           client-op-id
                           (assoc :client-op-id client-op-id
                                  :client-op-hash client-op-hash
                                  :client-op-response-kind
                                  (if (= :request (:mode base-req))
                                    cop/kv-result-response-kind
                                    cop/command-complete-response-kind)))
          request-fn     (fn [retry-client retry-req]
                           (if (= :copy-in (:mode retry-req))
                             (cl/copy-in retry-client retry-req txs
                                         c/+wire-datom-batch-size+)
                             (cl/request retry-client retry-req)))
          {:keys [type message err-data]}
          (try
            (request-fn client req)
            (catch Exception e
              (or (and client-op-id
                       (retry-ha-transport-failure
                        client
                        req
                        request-fn
                        (cached-ha-member-endpoints open-db-opts)
                        e)
                       {:type :command-complete})
                  (throw e))))]
      (when (= type :error-response)
        (if (:resized err-data)
          (raise message err-data)
          (if (#'cl/retryable-ha-write-reject? err-data)
            (if-let [retry-context (#'cl/client-retry-context client)]
              (#'cl/retry-ha-write-request*
               req
               message
               err-data
               retry-context
               request-fn
               cl/disconnect
               #'cl/new-client-for-endpoint
               #(#'cl/set-preferred-ha-endpoint! client %))
              (#'cl/raise-normal-request-error req message err-data nil))
            (raise "Error transacting kv to server:" message {:uri uri}))))))

  (get-value [db dbi-name k]
    (.get-value db dbi-name k :data :data true))
  (get-value [db dbi-name k k-type]
    (.get-value db dbi-name k k-type :data true))
  (get-value [db dbi-name k k-type v-type]
    (.get-value db dbi-name k k-type v-type true))

  (get-rank [db dbi-name k]
    (.get-rank db dbi-name k :data))

  (get-by-rank [db dbi-name rank]
    (.get-by-rank db dbi-name rank :data :data true))
  (get-by-rank [db dbi-name rank k-type]
    (.get-by-rank db dbi-name rank k-type :data true))
  (get-by-rank [db dbi-name rank k-type v-type]
    (.get-by-rank db dbi-name rank k-type v-type true))

  (sample-kv [db dbi-name n]
    (.sample-kv db dbi-name n :data :data true))
  (sample-kv [db dbi-name n k-type]
    (.sample-kv db dbi-name n k-type :data true))
  (sample-kv [db dbi-name n k-type v-type]
    (.sample-kv db dbi-name n k-type v-type true))

  (get-first [db dbi-name k-range]
    (.get-first db dbi-name k-range :data :data false))
  (get-first [db dbi-name k-range k-type]
    (.get-first db dbi-name k-range k-type :data false))
  (get-first [db dbi-name k-range k-type v-type]
    (.get-first db dbi-name k-range k-type v-type false))

  (get-first-n [this dbi-name n k-range]
    (.get-first-n this dbi-name n k-range :data :data false))
  (get-first-n [this dbi-name n k-range k-type]
    (.get-first-n this dbi-name n k-range k-type :data false))
  (get-first-n [this dbi-name n k-range k-type v-type]
    (.get-first-n this dbi-name n k-range k-type v-type false))

  (get-range [db dbi-name k-range]
    (.get-range db dbi-name k-range :data :data false))
  (get-range [db dbi-name k-range k-type]
    (.get-range db dbi-name k-range k-type :data false))
  (get-range [db dbi-name k-range k-type v-type]
    (.get-range db dbi-name k-range k-type v-type false))

  (defremote-forward (cl/normal-request client)
    (get-value [_ dbi-name k k-type v-type ignore-key?] :chatty true)
    (get-rank [_ dbi-name k k-type] :chatty true)
    (get-by-rank [_ dbi-name rank k-type v-type ignore-key?] :chatty true)
    (sample-kv [_ dbi-name n k-type v-type ignore-key?])
    (get-first [_ dbi-name k-range k-type v-type ignore-key?])
    (get-first-n [_ dbi-name n k-range k-type v-type ignore-key?])
    (get-range [_ dbi-name k-range k-type v-type ignore-key?]))

  (key-range [db dbi-name k-range]
    (.key-range db dbi-name k-range :data))

  (key-range-count [db dbi-name k-range]
    (.key-range-count db dbi-name k-range :data))

  (visit-key-range [db dbi-name visitor k-range]
    (.visit-key-range db dbi-name visitor k-range :data true))
  (visit-key-range [db dbi-name visitor k-range k-type]
    (.visit-key-range db dbi-name visitor k-range k-type true))

  (defremote-forward (cl/normal-request client)
    (key-range [_ dbi-name k-range k-type])
    (key-range-count [_ dbi-name k-range k-type])
    (key-range-list-count [_ dbi-name k-range k-type])
    (visit-key-range [_ dbi-name visitor k-range k-type raw-pred?]
      :serialize visitor))

  (range-seq [db dbi-name k-range]
    (.range-seq db dbi-name k-range :data :data false nil))
  (range-seq [db dbi-name k-range k-type]
    (.range-seq db dbi-name k-range k-type :data false nil))
  (range-seq [db dbi-name k-range k-type v-type]
    (.range-seq db dbi-name k-range k-type v-type false nil))
  (range-seq [db dbi-name k-range k-type v-type ignore-key?]
    (.range-seq db dbi-name k-range k-type v-type ignore-key? nil))
  (range-seq [_ dbi-name k-range k-type v-type ignore-key? opts]
    (remote-range-seq*
      (fn [dbi-name n k-range k-type v-type]
        (let [[res]
              (cl/normal-request
                client :batch-kv
                [db-name [[:get-first-n dbi-name n k-range k-type v-type false]]]
                writing?)]
          res))
      dbi-name k-range k-type v-type ignore-key? opts))

  (range-count [db dbi-name k-range]
    (.range-count db dbi-name k-range :data))

  (get-some [db dbi-name pred k-range]
    (.get-some db dbi-name pred k-range :data :data false true))
  (get-some [db dbi-name pred k-range k-type]
    (.get-some db dbi-name pred k-range k-type :data false true))
  (get-some [db dbi-name pred k-range k-type v-type]
    (.get-some db dbi-name pred k-range k-type v-type false true))
  (get-some [db dbi-name pred k-range k-type v-type ignore-key?]
    (.get-some db dbi-name pred k-range k-type v-type  ignore-key? true))

  (range-filter [db dbi-name pred k-range]
    (.range-filter db dbi-name pred k-range :data :data false true))
  (range-filter [db dbi-name pred k-range k-type]
    (.range-filter db dbi-name pred k-range k-type :data false true))
  (range-filter [db dbi-name pred k-range k-type v-type]
    (.range-filter db dbi-name pred k-range k-type v-type false true))
  (range-filter [db dbi-name pred k-range k-type v-type ignore-key?]
    (.range-filter db dbi-name pred k-range k-type v-type  ignore-key? true))

  (range-keep [this dbi-name pred k-range]
    (.range-keep this dbi-name pred k-range :data :data true))
  (range-keep [this dbi-name pred k-range k-type]
    (.range-keep this dbi-name pred k-range k-type :data true))
  (range-keep [this dbi-name pred k-range k-type v-type]
    (.range-keep this dbi-name pred k-range k-type v-type true))

  (range-some [this dbi-name pred k-range]
    (.range-some this dbi-name pred k-range :data :data true))
  (range-some [this dbi-name pred k-range k-type]
    (.range-some this dbi-name pred k-range k-type :data true))
  (range-some [this dbi-name pred k-range k-type v-type]
    (.range-some this dbi-name pred k-range k-type v-type true))

  (range-filter-count [db dbi-name pred k-range]
    (.range-filter-count db dbi-name pred k-range :data :data true))
  (range-filter-count [db dbi-name pred k-range k-type]
    (.range-filter-count db dbi-name pred k-range k-type :data true))
  (range-filter-count [db dbi-name pred k-range k-type v-type]
    (.range-filter-count db dbi-name pred k-range k-type v-type true))

  (visit [db dbi-name visitor k-range]
    (.visit db dbi-name visitor k-range :data :data true))
  (visit [db dbi-name visitor k-range k-type]
    (.visit db dbi-name visitor k-range k-type :data true))

  (defremote-forward (cl/normal-request client)
    (range-count [_ dbi-name k-range k-type])
    (get-some [_ dbi-name pred k-range k-type v-type ignore-key? raw-pred?]
      :serialize pred)
    (range-filter [_ dbi-name pred k-range k-type v-type ignore-key?
                   raw-pred?]
      :serialize pred)
    (range-keep [_ dbi-name pred k-range k-type v-type raw-pred?]
      :serialize pred)
    (range-some [_ dbi-name pred k-range k-type v-type raw-pred?]
      :serialize pred)
    (range-filter-count [_ dbi-name pred k-range k-type v-type raw-pred?]
      :serialize pred)
    (visit [_ dbi-name visitor k-range k-type v-type raw-pred?]
      :serialize visitor))

  (open-list-dbi [db dbi-name {:keys [key-size val-size flags]
                               :or   {key-size c/+max-key-size+
                                      val-size c/+max-key-size+
                                      flags    c/default-dbi-flags}
                               :as   opts}]
    (.open-dbi db dbi-name
               (merge opts
                      {:key-size key-size :val-size val-size
                       :flags    (conj flags :dupsort)})))
  (open-list-dbi [db dbi-name] (.open-list-dbi db dbi-name nil))

  IList
  (put-list-items [db dbi-name k vs kt vt]
    (.transact-kv db [[:put-list dbi-name k vs kt vt]]))

  (del-list-items [db dbi-name k kt]
    (.transact-kv db [[:del dbi-name k kt]]))
  (del-list-items [db dbi-name k vs kt vt]
    (.transact-kv db [[:del-list dbi-name k vs kt vt]]))

  (visit-list [db list-name visitor k k-type]
    (.visit-list db list-name visitor k k-type nil true))
  (visit-list [db list-name visitor k k-type v-type]
    (.visit-list db list-name visitor k k-type v-type true))

  (list-range-filter [db list-name pred k-range k-type v-range v-type]
    (.list-range-filter db list-name pred k-range k-type v-range v-type true))

  (list-range-keep [this dbi-name pred k-range kt v-range vt]
    (.list-range-keep this dbi-name pred k-range kt v-range vt true))

  (list-range-some [db list-name pred k-range k-type v-range v-type]
    (.list-range-some db list-name pred k-range k-type v-range v-type true))

  (list-range-filter-count [db list-name pred k-range k-type v-range v-type]
    (.list-range-filter-count db list-name pred k-range k-type v-range v-type
                              true))

  (visit-list-range [db list-name visitor k-range k-type v-range v-type]
    (.visit-list-range db list-name visitor k-range k-type v-range v-type true))

  (defremote-forward (cl/normal-request client)
    (get-list [_ dbi-name k kt vt] :chatty true)
    (visit-list [_ dbi-name visitor k kt vt raw-pred?] :serialize visitor)
    (list-count [_ dbi-name k kt] :chatty true)
    (in-list? [_ dbi-name k v kt vt] :chatty true)
    (list-range [_ dbi-name k-range kt v-range vt])
    (list-range-count [_ dbi-name k-range kt])
    (list-range-first [_ dbi-name k-range kt v-range vt])
    (list-range-first-n [_ dbi-name n k-range kt v-range vt])
    (list-range-filter [_ dbi-name pred k-range kt v-range vt raw-pred?]
      :serialize pred)
    (list-range-keep [_ dbi-name pred k-range kt v-range vt raw-pred?]
      :serialize pred)
    (list-range-some [_ dbi-name pred k-range kt v-range vt raw-pred?]
      :serialize pred)
    (list-range-filter-count [_ dbi-name pred k-range kt v-range vt raw-pred?]
      :serialize pred)
    (visit-list-range [_ dbi-name visitor k-range kt v-range vt raw-pred?]
      :serialize visitor))

  IAdmin
  (re-index [db opts]
    (cl/normal-request client :kv-re-index [db-name opts])
    db))

#_{:clj-kondo/ignore [:redefined-var]}
(defn ->KVStore
  ([uri db-name client]
   (KVStore. uri db-name client
             (volatile! :remote-kv-mutex)
             false
             (volatile! nil)
             false
             (AtomicBoolean. false)))
  ([uri db-name client write-txn]
   (KVStore. uri db-name client
             write-txn
             false
             (volatile! nil)
             false
             (AtomicBoolean. false)))
  ([uri db-name client write-txn writing?]
   (KVStore. uri db-name client
             write-txn
             writing?
             (volatile! nil)
             false
             (AtomicBoolean. false)))
  ([uri db-name client write-txn writing? open-db-opts]
   (KVStore. uri db-name client write-txn writing? open-db-opts
             false (AtomicBoolean. false)))
  ([uri db-name client write-txn writing? open-db-opts owns-client?]
   (KVStore. uri db-name client write-txn writing? open-db-opts
             owns-client? (AtomicBoolean. false)))
  ([uri db-name client write-txn writing? open-db-opts owns-client? closed?]
   (KVStore. uri db-name client write-txn writing? open-db-opts owns-client?
             closed?)))

(defn open-kv
  "Open a remote kv store."
  ([uri-str]
   (open-kv uri-str nil))
  ([uri-str opts]
   (let [client (cl/new-client uri-str (:client-opts opts))]
     (open-kv client uri-str opts true)))
  ([client uri-str opts]
   (open-kv client uri-str opts false))
  ([client uri-str opts owns-client?]
   (let [uri     (URI. uri-str)
         uri-str (str uri-str
                      (if (cl/parse-query uri) "&" "?")
                      "store=" c/db-store-kv)]
     (if-let [db-name (cl/parse-db uri)]
       (do (cl/open-database client db-name c/db-store-kv opts)
           (->KVStore uri-str db-name client
                      (volatile! :remote-kv-mutex) false
                      ;; KV stores do not implement IStore/opts on the server.
                      ;; Preserve caller-supplied open opts only for HA retry hints.
                      (volatile! opts)
                      owns-client?
                      (AtomicBoolean. false)))
       (raise "URI should contain a database name" {})))))

(defn batch-kv
  "Run multiple KV calls against a remote KV store in one RPC round trip.

  Each call is a vector `[op & args]`, where `op` is one of:
  `:get-value`, `:get-rank`, `:get-by-rank`, `:sample-kv`, `:get-first`,
  `:get-first-n`, `:get-range`, `:key-range`, `:key-range-count`,
  `:key-range-list-count`, `:range-count`, `:get-list`, `:list-count`,
  `:in-list?`, `:list-range`, `:list-range-count`, `:list-range-first`,
  or `:list-range-first-n`."
  [^KVStore store calls]
  (cl/normal-request (.-client store)
                     :batch-kv
                     [(.-db-name store) calls]
                     (.-writing? store)))

(defn get-values
  "Batch key lookups for remote KV store in one RPC.

  Returns a vector of results in the same order as `ks`."
  ([^KVStore store dbi-name ks]
   (get-values store dbi-name ks :data :data true))
  ([^KVStore store dbi-name ks k-type]
   (get-values store dbi-name ks k-type :data true))
  ([^KVStore store dbi-name ks k-type v-type]
   (get-values store dbi-name ks k-type v-type true))
  ([^KVStore store dbi-name ks k-type v-type ignore-key?]
   (let [batch-size (max 1 (long c/+wire-datom-batch-size+))]
     (->> (partition-all batch-size ks)
          (mapcat
            (fn [batch]
              (batch-kv store
                        (mapv (fn [k]
                                [:get-value dbi-name k k-type v-type
                                 ignore-key?])
                              batch))))
          vec))))

;; remote search

(declare ->SearchEngine)

(deftype SearchEngine [^KVStore store]
  ISearchEngine
  (add-doc [_ doc-ref doc-text]
    (cl/normal-request (.-client store) :add-doc
                       [(.-db-name store) doc-ref doc-text]))

  (remove-doc [_ doc-ref]
    (cl/normal-request (.-client store) :remove-doc
                       [(.-db-name store) doc-ref]))

  (clear-docs [_]
    (cl/normal-request (.-client store) :clear-docs [(.-db-name store)]))

  (doc-indexed? [_ doc-ref]
    (cl/normal-request (.-client store) :doc-indexed?
                       [(.-db-name store) doc-ref]))

  (doc-count [_]
    (cl/normal-request (.-client store) :doc-count [(.-db-name store)]))

  (search [this query]
    (.search this query {}))
  (search [_ query opts]
    (cl/normal-request (.-client store) :search
                       [(.-db-name store) query opts]))

  IAdmin
  (re-index [this opts]
    (cl/normal-request (.-client store) :search-re-index
                       [(.-db-name store) opts])
    this))

(defn new-search-engine
  ([store]
   (new-search-engine store nil))
  ([^KVStore store opts]
   (cl/normal-request (.-client store) :new-search-engine
                      [(.-db-name store) opts])
   (->SearchEngine store)))

;; remote vector index

(declare ->VectorIndex new-vector-index)

(deftype VectorIndex [^KVStore store]
  IVectorIndex
  (add-vec [_ vec-ref vec-data]
    (cl/normal-request (.-client store) :add-vec
                       [(.-db-name store) vec-ref vec-data]))

  (remove-vec [_ vec-ref]
    (cl/normal-request (.-client store) :remove-vec
                       [(.-db-name store) vec-ref]))

  (persist-vecs [_]
    (cl/normal-request (.-client store) :persist-vecs [(.-db-name store)]))

  (close-vecs [_]
    (cl/normal-request (.-client store) :close-vecs [(.-db-name store)]))

  (clear-vecs [_]
    (cl/normal-request (.-client store) :clear-vecs [(.-db-name store)]))

  (vecs-info [_]
    (cl/normal-request (.-client store) :vecs-info [(.-db-name store)]))

  (vec-indexed? [_ vec-ref]
    (cl/normal-request (.-client store) :vec-indexed?
                       [(.-db-name store) vec-ref]))

  (search-vec [this query]
    (.search-vec this query {}))
  (search-vec [_ query opts]
    (cl/normal-request (.-client store) :search-vec
                       [(.-db-name store) query opts]))

  IAdmin
  (re-index [this opts]
    (cl/normal-request (.-client store) :vec-re-index
                       [(.-db-name store) opts])
    this))

(defn new-vector-index
  [^KVStore store opts]
  (cl/normal-request (.-client store) :new-vector-index
                     [(.-db-name store) opts])
  (->VectorIndex store))
