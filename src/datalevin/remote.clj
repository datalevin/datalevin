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
   [datalevin.util :as u]
   [datalevin.constants :as c]
   [datalevin.interface]
   [datalevin.client :as cl]
   [datalevin.bits :as b]
   [datalevin.datom :as d]
   [datalevin.lmdb :as l :refer [IWriting]]
   [clojure.string :as str])
  (:import
   [datalevin.client Client]
   [datalevin.interface ILMDB ITxLog IList IAdmin IStore ISearchEngine IVectorIndex]
   [clojure.lang Seqable IReduceInit]
   [java.lang AutoCloseable]
   [java.util.concurrent ConcurrentHashMap]
   [java.util.concurrent.atomic AtomicBoolean]
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

(defn- load-datoms*
  ([client db-name datoms datom-type simulated?]
   (load-datoms* client db-name datoms datom-type simulated? false))
  ([client db-name datoms datom-type simulated? writing?]
   (let [tx? (#{:txs :txs+info} datom-type)
         t   (case datom-type
               :txs      :tx-data
               :txs+info :tx-data+db-info
               :load-datoms)
         {:keys [type message result err-data]
          :as   response}
         (if (< (count datoms) ^long c/+wire-datom-batch-size+)
           (cl/request client {:type     t
                               :mode     :request
                               :writing? writing?
                               :args     (if tx?
                                           [db-name datoms simulated?]
                                           [db-name datoms])})
           (cl/copy-in client {:type     t
                               :mode     :copy-in
                               :writing? writing?
                               :args     (if tx?
                                           [db-name simulated?]
                                           [db-name])}
                       datoms c/+wire-datom-batch-size+))]
     (if (= type :error-response)
       (if (:resized err-data)
         (u/raise message err-data)
         (u/raise "Error loading datoms to server:" message {}))
       (if (and tx?
                (or (contains? response :db-info)
                    (contains? response :new-attributes)))
         (let [[tx-data tempids] (if (map? result)
                                   [(:tx-data result) (:tempids result)]
                                   (let [[tx-data tempids] (split-with d/datom?
                                                                       result)]
                                     [tx-data (apply hash-map tempids)]))]
           (cond-> {:tx-data tx-data :tempids tempids}
             (contains? response :db-info)
             (assoc :db-info (:db-info response))
             (contains? response :new-attributes)
             (assoc :new-attributes (:new-attributes response))))
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

(deftype DatalogStore [^String uri
                       ^String db-name
                       ^Client client
                       write-txn
                       writing?
                       open-db-info
                       ^AtomicBoolean sampling-started?]
  IWriting
  (writing? [_] writing?)

  (write-txn [_] write-txn)

  (mark-write [_]
    (->DatalogStore uri db-name client (volatile! :remote-dl-mutex) true
                    open-db-info
                    sampling-started?))

  IStore
  (opts [_] (cl/normal-request client :opts [db-name] writing?))

  (assoc-opt [_ k v]
    (cl/normal-request client :assoc-opt [db-name k v] writing?))

  (db-name [_] db-name)

  (dir [_] uri)

  (close [_]
    (when-not (cl/disconnected? client)
      (cl/normal-request client :close [db-name] writing?)))

  (closed? [_]
    (if (cl/disconnected? client)
      true
      (cl/normal-request client :closed? [db-name] writing?)))

  (last-modified [_]
    (cl/normal-request client :last-modified [db-name] writing?))

  (schema [_] (cl/normal-request client :schema [db-name] writing?))

  (rschema [_] (cl/normal-request client :rschema [db-name] writing?))

  (set-schema [_ new-schema]
    (cl/normal-request client :set-schema [db-name new-schema] writing?))

  (init-max-eid [_]
    (cl/normal-request client :init-max-eid [db-name] writing?))

  (max-tx [_]
    (cl/normal-request client :max-tx [db-name] writing?))

  (swap-attr [this attr f]
    (.swap-attr this attr f nil nil))
  (swap-attr [this attr f x]
    (.swap-attr this attr f x nil))
  (swap-attr [_ attr f x y]
    (let [frozen-f (b/serialize f)]
      (cl/normal-request
        client :swap-attr [db-name attr frozen-f x y] writing?)))

  (del-attr [_ attr]
    (cl/normal-request client :del-attr [db-name attr] writing?))

  (rename-attr [_ attr new-attr]
    (cl/normal-request client :rename-attr [db-name attr new-attr] writing?))

  (datom-count [_ index]
    (cl/normal-request client :datom-count [db-name index] writing?))

  (load-datoms [_ datoms]
    (load-datoms* client db-name datoms :raw false writing?))

  (fetch [_ datom] (cl/normal-request client :fetch [db-name datom] writing?))

  (populated? [_ index low-datom high-datom]
    (cl/normal-request
      client :populated? [db-name index low-datom high-datom] writing?))

  (size [_ index low-datom high-datom]
    (cl/normal-request
      client :size [db-name index low-datom high-datom] writing?))

  (head [_ index low-datom high-datom]
    (cl/normal-request
      client :head [db-name index low-datom high-datom] writing?))

  (tail [_ index high-datom low-datom]
    (cl/normal-request
      client :tail [db-name index high-datom low-datom] writing?))

  (slice [_ index low-datom high-datom]
    (cl/normal-request
      client :slice [db-name index low-datom high-datom] writing?))

  (rslice [_ index high-datom low-datom]
    (cl/normal-request
      client :rslice [db-name index high-datom low-datom] writing?))

  (e-datoms [_ e]
    (cl/normal-request client :e-datoms [db-name e] writing?))

  (e-first-datom [_ e]
    (cl/normal-request client :e-first-datom [db-name e] writing?))

  (start-sampling [_]
    (when (.compareAndSet sampling-started? false true)
      (try
        (cl/normal-request client :start-sampling [db-name] writing?)
        (catch Exception e
          (.set sampling-started? false)
          (throw e)))))

  (stop-sampling [_]
    (when (.compareAndSet sampling-started? true false)
      (try
        (cl/normal-request client :stop-sampling [db-name] writing?)
        (catch Exception e
          (.set sampling-started? true)
          (throw e)))))

  (analyze [_ attr]
    (cl/normal-request client :analyze [db-name attr]))

  (av-datoms [_ a v]
    (cl/normal-request client :av-datoms [db-name a v] writing?))

  (av-first-datom [_ a v]
    (cl/normal-request client :av-first-datom [db-name a v] writing?))

  (av-first-e [_ a v]
    (cl/normal-request client :av-first-e [db-name a v] writing?))

  (ea-first-datom [_ e a]
    (cl/normal-request client :ea-first-datom [db-name e a] writing?))

  (ea-first-v [_ e a]
    (cl/normal-request client :ea-first-v [db-name e a] writing?))

  (v-datoms [_ v]
    (cl/normal-request client :v-datoms [db-name v] writing?))

  (size-filter [_ index pred low-datom high-datom]
    (let [frozen-pred (b/serialize pred)]
      (cl/normal-request
        client :size-filter
        [db-name index frozen-pred low-datom high-datom] writing?)))

  (head-filter [_ index pred low-datom high-datom]
    (let [frozen-pred (b/serialize pred)]
      (cl/normal-request
        client :head-filter
        [db-name index frozen-pred low-datom high-datom] writing?)))

  (tail-filter [_ index pred high-datom low-datom]
    (let [frozen-pred (b/serialize pred)]
      (cl/normal-request
        client :tail-filter
        [db-name index frozen-pred high-datom low-datom] writing?)))

  (slice-filter [_ index pred low-datom high-datom]
    (let [frozen-pred (b/serialize pred)]
      (cl/normal-request
        client :slice-filter
        [db-name index frozen-pred low-datom high-datom] writing?)))

  (rslice-filter [_ index pred high-datom low-datom]
    (let [frozen-pred (b/serialize pred)]
      (cl/normal-request
        client :rslice-filter
        [db-name index frozen-pred high-datom low-datom] writing?)))

  IRemoteDB
  (q [_ query inputs]
    (cl/normal-request client :q [db-name query inputs] writing?))

  (pull [_ pattern id opts]
    (cl/normal-request client :pull [db-name pattern id opts] writing?))

  (pull-many [_ pattern id opts]
    (cl/normal-request client :pull-many [db-name pattern id opts] writing?))

  (explain [_ opts query inputs]
    (cl/normal-request client :explain [db-name opts query inputs] writing?))

  (fulltext-datoms [_ query opts]
    (cl/normal-request client :fulltext-datoms [db-name query opts] writing?))

  (db-info [_]
    (if-let [cached @open-db-info]
      (do
        (vreset! open-db-info nil)
        cached)
      (cl/normal-request client :db-info [db-name] writing?)))

  (tx-data [_ data simulated?]
    (load-datoms* client db-name data :txs+info simulated? writing?))

  (open-transact [this]
    (cl/normal-request client :open-transact [db-name])
    (.mark-write this))

  (abort-transact [this]
    (cl/normal-request client :abort-transact [db-name] true))

  (close-transact [_]
    (cl/normal-request client :close-transact [db-name] true))

  ILMDB
  (kv-info [_] nil)
  (sync [this] (.sync this 1))
  (sync [_ force]
    (cl/normal-request client :sync [db-name force] writing?))

  ITxLog
  (txlog-watermarks [_]
    (cl/normal-request client :txlog-watermarks [db-name] writing?))
  (open-tx-log [this from-lsn]
    (.open-tx-log this from-lsn nil))
  (open-tx-log [_ from-lsn upto-lsn]
    (cl/normal-request client :open-tx-log [db-name from-lsn upto-lsn] writing?))
  (force-txlog-sync! [_]
    (cl/normal-request client :force-txlog-sync! [db-name] writing?))
  (force-lmdb-sync! [_]
    (cl/normal-request client :force-lmdb-sync! [db-name] writing?))
  (create-snapshot! [_]
    (cl/normal-request client :create-snapshot! [db-name] writing?))
  (list-snapshots [_]
    (cl/normal-request client :list-snapshots [db-name] writing?))
  (snapshot-scheduler-state [_]
    (cl/normal-request client :snapshot-scheduler-state [db-name] writing?))
  (read-commit-marker [_]
    (cl/normal-request client :read-commit-marker [db-name] writing?))
  (verify-commit-marker! [_]
    (cl/normal-request client :verify-commit-marker! [db-name] writing?))
  (txlog-retention-state [_]
    (cl/normal-request client :txlog-retention-state [db-name] writing?))
  (gc-txlog-segments! [this]
    (.gc-txlog-segments! this nil))
  (gc-txlog-segments! [_ retain-floor-lsn]
    (cl/normal-request client :gc-txlog-segments!
                       [db-name retain-floor-lsn] writing?))
  (txlog-update-snapshot-floor! [this snapshot-lsn]
    (.txlog-update-snapshot-floor! this snapshot-lsn nil))
  (txlog-update-snapshot-floor! [_ snapshot-lsn previous-snapshot-lsn]
    (cl/normal-request client :txlog-update-snapshot-floor!
                       [db-name snapshot-lsn previous-snapshot-lsn] writing?))
  (txlog-clear-snapshot-floor! [_]
    (cl/normal-request client :txlog-clear-snapshot-floor!
                       [db-name] writing?))
  (txlog-update-replica-floor! [_ replica-id applied-lsn]
    (cl/normal-request client :txlog-update-replica-floor!
                       [db-name replica-id applied-lsn] writing?))
  (txlog-clear-replica-floor! [_ replica-id]
    (cl/normal-request client :txlog-clear-replica-floor!
                       [db-name replica-id] writing?))
  (txlog-pin-backup-floor! [this pin-id floor-lsn]
    (.txlog-pin-backup-floor! this pin-id floor-lsn nil))
  (txlog-pin-backup-floor! [_ pin-id floor-lsn expires-ms]
    (cl/normal-request client :txlog-pin-backup-floor!
                       [db-name pin-id floor-lsn expires-ms] writing?))
  (txlog-unpin-backup-floor! [_ pin-id]
    (cl/normal-request client :txlog-unpin-backup-floor!
                       [db-name pin-id] writing?))

  IAdmin
  (re-index [_ schema opts]
    (cl/normal-request client :datalog-re-index [db-name schema opts])))

(defn open
  "Open a remote Datalog store"
  ([uri-str]
   (open uri-str nil))
  ([uri-str schema]
   (open (cl/new-client uri-str) uri-str schema nil))
  ([uri-str schema opts]
   (open (cl/new-client uri-str (:client-opts opts)) uri-str schema opts))
  ([client uri-str schema opts]
   (let [uri (URI. uri-str)]
     (if-let [db-name (cl/parse-db uri)]
       (let [store (or (get (cl/parse-query uri) "store")
                       c/db-store-datalog)
             db-info (cl/open-database client db-name store schema opts true)]
         (->DatalogStore uri-str db-name client
                         (volatile! :remote-dl-mutex) false
                         (volatile! db-info)
                         (AtomicBoolean. false)))
       (u/raise "URI should contain a database name" {})))))

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
      (u/raise "Unsupported key range type for remote range-seq"
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

(defn- find-index
  [item coll]
  (second (u/some-indexed #(= item %) coll)))

(defn- remote-range-seq*
  [fetch-first-n get-range dbi-name k-range k-type v-type ignore-key? opts]
  (let [batch-size (max 1 (long (or (:batch-size opts) 100)))
        request-n  (inc batch-size)
        fetch      (fn [{:keys [k-range resume]}]
                     (let [raw     (vec (fetch-first-n dbi-name request-n k-range
                                                       k-type v-type))
                           tail    (if resume
                                     (if-let [idx (find-index resume raw)]
                                       (subvec raw (inc idx))
                                       ;; Dense duplicate-key ranges (e.g. dupsort/list DBI)
                                       ;; may not include `resume` in a small page. Fall back
                                       ;; to one eager range request to preserve correctness.
                                       (let [all (vec (get-range dbi-name k-range
                                                                 k-type v-type))]
                                         (if-let [idx2 (find-index resume all)]
                                           (subvec all (inc idx2))
                                           [])))
                                     raw)
                           chunk   (if (> (count tail) batch-size)
                                     (subvec tail 0 batch-size)
                                     tail)
                           batch   (mapv #(project-range-item % ignore-key? v-type)
                                         chunk)
                           more?   (> (count tail) batch-size)
                           next-k  (when more?
                                     (first (peek chunk)))]
                       {:batch      batch
                        :next-state (when next-k
                                      {:k-range (advance-k-range k-range next-k)
                                       :resume  (peek chunk)})}))
        init-state {:k-range k-range :resume nil}]
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

(deftype KVStore [^String uri
                  ^String db-name
                  ^Client client
                  write-txn
                  writing?]
  IWriting
  (writing? [_] writing?)

  (write-txn [_] write-txn)

  (mark-write [_]
    (->KVStore uri db-name client (volatile! :remote-kv-mutex) true))

  ILMDB

  (close-kv [_]
    (when-not (cl/disconnected? client)
      (cl/normal-request client :close-kv [db-name])))

  (closed-kv? [_]
    (if (cl/disconnected? client)
      true
      (cl/normal-request client :closed-kv? [db-name])))

  (env-dir [_] uri)
  (kv-info [_] nil)

  (open-dbi [db dbi-name]
    (.open-dbi db dbi-name nil))
  (open-dbi [_ dbi-name opts]
    (cl/normal-request client :open-dbi [db-name dbi-name opts] writing?))

  (clear-dbi [db dbi-name]
    (cl/normal-request client :clear-dbi [db-name dbi-name] writing?))

  (drop-dbi [db dbi-name]
    (cl/normal-request client :drop-dbi [db-name dbi-name] writing?))

  (list-dbis [db] (cl/normal-request client :list-dbis [db-name] writing?))

  (copy [db dest] (.copy db dest false))
  (copy [_ dest compact?]
    (let [{:keys [type message result copy-meta copy-format checksum
                  checksum-algo bytes]}
          (cl/request client {:type     :copy
                              :mode     :request
                              :writing? writing?
                              :args     [db-name compact?]})
          _    (when (= type :error-response)
                 (u/raise "Request to Datalevin server failed: "
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
            (u/raise "Unsupported checksum algorithm from server"
                     {:checksum-algo checksum-algo}))
          (when (and bytes (not= (long bytes) (long written-bytes)))
            (u/raise "Copy size mismatch"
                     {:expected-bytes (long bytes)
                      :actual-bytes   (long written-bytes)}))
          (when (and checksum (not= checksum actual-checksum))
            (u/raise "Copy checksum mismatch"
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
  (stat [_ dbi-name]
    (cl/normal-request client :stat [db-name dbi-name] writing?))

  (entries [_ dbi-name]
    (cl/normal-request client :entries [db-name dbi-name] writing?))

  (set-env-flags [_ ks on-off]
    (cl/normal-request client :set-env-flags [db-name ks on-off] writing?))

  (get-env-flags [_]
    (cl/normal-request client :get-env-flags [db-name] writing?))

  (sync [this] (.sync this 1))
  (sync [_ force]
    (cl/normal-request client :sync [db-name force] writing?))

  ITxLog
  (txlog-watermarks [_]
    (cl/normal-request client :txlog-watermarks [db-name] writing?))

  (open-tx-log [this from-lsn]
    (.open-tx-log this from-lsn nil))
  (open-tx-log [_ from-lsn upto-lsn]
    (cl/normal-request client :open-tx-log [db-name from-lsn upto-lsn] writing?))

  (force-txlog-sync! [_]
    (cl/normal-request client :force-txlog-sync! [db-name] writing?))

  (force-lmdb-sync! [_]
    (cl/normal-request client :force-lmdb-sync! [db-name] writing?))

  (create-snapshot! [_]
    (cl/normal-request client :create-snapshot! [db-name] writing?))

  (list-snapshots [_]
    (cl/normal-request client :list-snapshots [db-name] writing?))

  (snapshot-scheduler-state [_]
    (cl/normal-request client :snapshot-scheduler-state [db-name] writing?))

  (read-commit-marker [_]
    (cl/normal-request client :read-commit-marker [db-name] writing?))

  (verify-commit-marker! [_]
    (cl/normal-request client :verify-commit-marker! [db-name] writing?))

  (txlog-retention-state [_]
    (cl/normal-request client :txlog-retention-state [db-name] writing?))

  (gc-txlog-segments! [this]
    (.gc-txlog-segments! this nil))
  (gc-txlog-segments! [_ retain-floor-lsn]
    (cl/normal-request client :gc-txlog-segments!
                       [db-name retain-floor-lsn] writing?))
  (txlog-update-snapshot-floor! [this snapshot-lsn]
    (.txlog-update-snapshot-floor! this snapshot-lsn nil))
  (txlog-update-snapshot-floor! [_ snapshot-lsn previous-snapshot-lsn]
    (cl/normal-request client :txlog-update-snapshot-floor!
                       [db-name snapshot-lsn previous-snapshot-lsn] writing?))
  (txlog-clear-snapshot-floor! [_]
    (cl/normal-request client :txlog-clear-snapshot-floor!
                       [db-name] writing?))
  (txlog-update-replica-floor! [_ replica-id applied-lsn]
    (cl/normal-request client :txlog-update-replica-floor!
                       [db-name replica-id applied-lsn] writing?))
  (txlog-clear-replica-floor! [_ replica-id]
    (cl/normal-request client :txlog-clear-replica-floor!
                       [db-name replica-id] writing?))
  (txlog-pin-backup-floor! [this pin-id floor-lsn]
    (.txlog-pin-backup-floor! this pin-id floor-lsn nil))
  (txlog-pin-backup-floor! [_ pin-id floor-lsn expires-ms]
    (cl/normal-request client :txlog-pin-backup-floor!
                       [db-name pin-id floor-lsn expires-ms] writing?))
  (txlog-unpin-backup-floor! [_ pin-id]
    (cl/normal-request client :txlog-unpin-backup-floor!
                       [db-name pin-id] writing?))

  (open-transact-kv [db]
    (cl/normal-request client :open-transact-kv [db-name])
    (.mark-write db))

  (close-transact-kv [_]
    (cl/normal-request client :close-transact-kv [db-name] true))

  (abort-transact-kv [_]
    (cl/normal-request client :abort-transact-kv [db-name] true))

  (transact-kv [this txs] (.transact-kv this nil txs))
  (transact-kv [this dbi-name txs]
    (.transact-kv this dbi-name txs :data :data))
  (transact-kv [this dbi-name txs k-type]
    (.transact-kv this dbi-name txs k-type :data))
  (transact-kv [_ dbi-name txs k-type v-type]
    (let [{:keys [type message err-data]}
          (if (< (count txs) ^long c/+wire-datom-batch-size+)
            (cl/request client
                        {:type     :transact-kv
                         :mode     :request
                         :writing? writing?
                         :args     [db-name dbi-name txs k-type v-type]})
            (cl/copy-in client
                        {:type     :transact-kv
                         :mode     :copy-in
                         :writing? writing?
                         :args     [db-name dbi-name k-type v-type]}
                        txs c/+wire-datom-batch-size+))]
      (when (= type :error-response)
        (if (:resized err-data)
          (u/raise message err-data)
          (u/raise "Error transacting kv to server:" message {:uri uri})))))

  (get-value [db dbi-name k]
    (.get-value db dbi-name k :data :data true))
  (get-value [db dbi-name k k-type]
    (.get-value db dbi-name k k-type :data true))
  (get-value [db dbi-name k k-type v-type]
    (.get-value db dbi-name k k-type v-type true))
  (get-value [_ dbi-name k k-type v-type ignore-key?]
    (detect-chatty-kv! db-name dbi-name :get-value)
    (cl/normal-request
      client :get-value
      [db-name dbi-name k k-type v-type ignore-key?] writing?))

  (get-rank [db dbi-name k]
    (.get-rank db dbi-name k :data))
  (get-rank [_ dbi-name k k-type]
    (detect-chatty-kv! db-name dbi-name :get-rank)
    (cl/normal-request
      client :get-rank
      [db-name dbi-name k k-type] writing?))

  (get-by-rank [db dbi-name rank]
    (.get-by-rank db dbi-name rank :data :data true))
  (get-by-rank [db dbi-name rank k-type]
    (.get-by-rank db dbi-name rank k-type :data true))
  (get-by-rank [db dbi-name rank k-type v-type]
    (.get-by-rank db dbi-name rank k-type v-type true))
  (get-by-rank [_ dbi-name rank k-type v-type ignore-key?]
    (detect-chatty-kv! db-name dbi-name :get-by-rank)
    (cl/normal-request
      client :get-by-rank
      [db-name dbi-name rank k-type v-type ignore-key?] writing?))

  (sample-kv [db dbi-name n]
    (.sample-kv db dbi-name n :data :data true))
  (sample-kv [db dbi-name n k-type]
    (.sample-kv db dbi-name n k-type :data true))
  (sample-kv [db dbi-name n k-type v-type]
    (.sample-kv db dbi-name n k-type v-type true))
  (sample-kv [_ dbi-name n k-type v-type ignore-key?]
    (cl/normal-request
      client :sample-kv
      [db-name dbi-name n k-type v-type ignore-key?] writing?))

  (get-first [db dbi-name k-range]
    (.get-first db dbi-name k-range :data :data false))
  (get-first [db dbi-name k-range k-type]
    (.get-first db dbi-name k-range k-type :data false))
  (get-first [db dbi-name k-range k-type v-type]
    (.get-first db dbi-name k-range k-type v-type false))
  (get-first [_ dbi-name k-range k-type v-type ignore-key?]
    (cl/normal-request
      client :get-first
      [db-name dbi-name k-range k-type v-type ignore-key?] writing?))

  (get-first-n [this dbi-name n k-range]
    (.get-first-n this dbi-name n k-range :data :data false))
  (get-first-n [this dbi-name n k-range k-type]
    (.get-first-n this dbi-name n k-range k-type :data false))
  (get-first-n [this dbi-name n k-range k-type v-type]
    (.get-first-n this dbi-name n k-range k-type v-type false))
  (get-first-n [_ dbi-name n k-range k-type v-type ignore-key?]
    (cl/normal-request
      client :get-first-n
      [db-name dbi-name n k-range k-type v-type ignore-key?] writing?))

  (get-range [db dbi-name k-range]
    (.get-range db dbi-name k-range :data :data false))
  (get-range [db dbi-name k-range k-type]
    (.get-range db dbi-name k-range k-type :data false))
  (get-range [db dbi-name k-range k-type v-type]
    (.get-range db dbi-name k-range k-type v-type false))
  (get-range [_ dbi-name k-range k-type v-type ignore-key?]
    (cl/normal-request
      client :get-range
      [db-name dbi-name k-range k-type v-type ignore-key?] writing?))

  (key-range [db dbi-name k-range]
    (.key-range db dbi-name k-range :data))
  (key-range [_ dbi-name k-range k-type]
    (cl/normal-request client :key-range
                       [db-name dbi-name k-range k-type] writing?))

  (key-range-count [db dbi-name k-range]
    (.key-range-count db dbi-name k-range :data))
  (key-range-count [_ dbi-name k-range k-type]
    (cl/normal-request client :key-range-count
                       [db-name dbi-name k-range k-type] writing?))

  (key-range-list-count [_ dbi-name k-range k-type]
    (cl/normal-request client :key-range-list-count
                       [db-name dbi-name k-range k-type] writing?))

  (visit-key-range [db dbi-name visitor k-range]
    (.visit-key-range db dbi-name visitor k-range :data true))
  (visit-key-range [db dbi-name visitor k-range k-type]
    (.visit-key-range db dbi-name visitor k-range k-type true))
  (visit-key-range [_ dbi-name visitor k-range k-type raw-pred?]
    (let [frozen-visitor (b/serialize visitor)]
      (cl/normal-request
        client :visit-key-range
        [db-name dbi-name frozen-visitor k-range k-type raw-pred?]
        writing?)))

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
      (fn [dbi-name k-range k-type v-type]
        (cl/normal-request
          client :get-range
          [db-name dbi-name k-range k-type v-type false]
          writing?))
      dbi-name k-range k-type v-type ignore-key? opts))

  (range-count [db dbi-name k-range]
    (.range-count db dbi-name k-range :data))
  (range-count [_ dbi-name k-range k-type]
    (cl/normal-request
      client :range-count [db-name dbi-name k-range k-type] writing?))

  (get-some [db dbi-name pred k-range]
    (.get-some db dbi-name pred k-range :data :data false true))
  (get-some [db dbi-name pred k-range k-type]
    (.get-some db dbi-name pred k-range k-type :data false true))
  (get-some [db dbi-name pred k-range k-type v-type]
    (.get-some db dbi-name pred k-range k-type v-type false true))
  (get-some [db dbi-name pred k-range k-type v-type ignore-key?]
    (.get-some db dbi-name pred k-range k-type v-type  ignore-key? true))
  (get-some [_ dbi-name pred k-range k-type v-type ignore-key? raw-pred?]
    (let [frozen-pred (b/serialize pred)]
      (cl/normal-request
        client :get-some
        [db-name dbi-name frozen-pred k-range k-type v-type ignore-key?
         raw-pred?]
        writing?)))

  (range-filter [db dbi-name pred k-range]
    (.range-filter db dbi-name pred k-range :data :data false true))
  (range-filter [db dbi-name pred k-range k-type]
    (.range-filter db dbi-name pred k-range k-type :data false true))
  (range-filter [db dbi-name pred k-range k-type v-type]
    (.range-filter db dbi-name pred k-range k-type v-type false true))
  (range-filter [db dbi-name pred k-range k-type v-type ignore-key?]
    (.range-filter db dbi-name pred k-range k-type v-type  ignore-key? true))
  (range-filter [db dbi-name pred k-range k-type v-type ignore-key? raw-pred?]
    (let [frozen-pred (b/serialize pred)]
      (cl/normal-request
        client :range-filter
        [db-name dbi-name frozen-pred k-range k-type v-type ignore-key?
         raw-pred?]
        writing?)))

  (range-keep [this dbi-name pred k-range]
    (.range-keep this dbi-name pred k-range :data :data true))
  (range-keep [this dbi-name pred k-range k-type]
    (.range-keep this dbi-name pred k-range k-type :data true))
  (range-keep [this dbi-name pred k-range k-type v-type]
    (.range-keep this dbi-name pred k-range k-type v-type true))
  (range-keep [this dbi-name pred k-range k-type v-type raw-pred?]
    (let [frozen-pred (b/serialize pred)]
      (cl/normal-request
        client :range-keep
        [db-name dbi-name frozen-pred k-range k-type v-type raw-pred?]
        writing?)))

  (range-some [this dbi-name pred k-range]
    (.range-some this dbi-name pred k-range :data :data true))
  (range-some [this dbi-name pred k-range k-type]
    (.range-some this dbi-name pred k-range k-type :data true))
  (range-some [this dbi-name pred k-range k-type v-type]
    (.range-some this dbi-name pred k-range k-type v-type true))
  (range-some [this dbi-name pred k-range k-type v-type raw-pred?]
    (let [frozen-pred (b/serialize pred)]
      (cl/normal-request
        client :range-some
        [db-name dbi-name frozen-pred k-range k-type v-type raw-pred?]
        writing?)))

  (range-filter-count [db dbi-name pred k-range]
    (.range-filter-count db dbi-name pred k-range :data :data true))
  (range-filter-count [db dbi-name pred k-range k-type]
    (.range-filter-count db dbi-name pred k-range k-type :data true))
  (range-filter-count [db dbi-name pred k-range k-type v-type]
    (.range-filter-count db dbi-name pred k-range k-type v-type true))
  (range-filter-count [_ dbi-name pred k-range k-type v-type raw-pred?]
    (let [frozen-pred (b/serialize pred)]
      (cl/normal-request
        client :range-filter-count
        [db-name dbi-name frozen-pred k-range k-type v-type raw-pred?]
        writing?)))

  (visit [db dbi-name visitor k-range]
    (.visit db dbi-name visitor k-range :data :data true))
  (visit [db dbi-name visitor k-range k-type]
    (.visit db dbi-name visitor k-range k-type :data true))
  (visit
    [_ dbi-name visitor k-range k-type v-type raw-pred?]
    (let [frozen-visitor (b/serialize visitor)]
      (cl/normal-request
        client :visit
        [db-name dbi-name frozen-visitor k-range k-type v-type raw-pred?]
        writing?)))

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

  (get-list [_ dbi-name k kt vt]
    (detect-chatty-kv! db-name dbi-name :get-list)
    (cl/normal-request client :get-list
                       [db-name dbi-name k kt vt] writing?))

  (visit-list [db list-name visitor k k-type]
    (.visit-list db list-name visitor k k-type nil true))
  (visit-list [db list-name visitor k k-type v-type]
    (.visit-list db list-name visitor k k-type v-type true))
  (visit-list [_ dbi-name visitor k kt vt raw-pred?]
    (let [frozen-visitor (b/serialize visitor)]
      (cl/normal-request
        client :visit-list
        [db-name dbi-name frozen-visitor k kt vt raw-pred?] writing?)))

  (list-count [_ dbi-name k kt]
    (detect-chatty-kv! db-name dbi-name :list-count)
    (cl/normal-request client :list-count
                       [db-name dbi-name k kt] writing?))

  (in-list? [_ dbi-name k v kt vt]
    (detect-chatty-kv! db-name dbi-name :in-list?)
    (cl/normal-request client :in-count?
                       [db-name dbi-name k v kt vt] writing?))

  (list-range [_ dbi-name k-range kt v-range vt]
    (cl/normal-request client :list-range
                       [db-name dbi-name k-range kt v-range vt] writing?))

  (list-range-count [_ dbi-name k-range kt v-range vt]
    (cl/normal-request client :list-range-count
                       [db-name dbi-name k-range kt v-range vt] writing?))

  (list-range-first [_ dbi-name k-range kt v-range vt]
    (cl/normal-request client :list-range-first
                       [db-name dbi-name k-range kt v-range vt] writing?))

  (list-range-first-n [_ dbi-name n k-range kt v-range vt]
    (cl/normal-request client :list-range-first-n
                       [db-name dbi-name n k-range kt v-range vt] writing?))

  (list-range-filter [db list-name pred k-range k-type v-range v-type]
    (.list-range-filter db list-name pred k-range k-type v-range v-type true))
  (list-range-filter [_ dbi-name pred k-range kt v-range vt raw-pred?]
    (let [frozen-pred (b/serialize pred)]
      (cl/normal-request
        client :list-range-filter
        [db-name dbi-name frozen-pred k-range kt v-range vt raw-pred?]
        writing?)))

  (list-range-keep [this dbi-name pred k-range kt v-range vt]
    (.list-range-keep this dbi-name pred k-range kt v-range vt true))
  (list-range-keep [this dbi-name pred k-range kt v-range vt raw-pred?]
    (let [frozen-pred (b/serialize pred)]
      (cl/normal-request
        client :list-range-keep
        [db-name dbi-name frozen-pred k-range kt v-range vt raw-pred?]
        writing?)))

  (list-range-some [db list-name pred k-range k-type v-range v-type]
    (.list-range-some db list-name pred k-range k-type v-range v-type true))
  (list-range-some [_ dbi-name pred k-range kt v-range vt raw-pred?]
    (let [frozen-pred (b/serialize pred)]
      (cl/normal-request
        client :list-range-some
        [db-name dbi-name frozen-pred k-range kt v-range vt raw-pred?]
        writing?)))

  (list-range-filter-count [db list-name pred k-range k-type v-range v-type]
    (.list-range-filter-count db list-name pred k-range k-type v-range v-type
                              true))
  (list-range-filter-count [_ dbi-name pred k-range kt v-range vt raw-pred?]
    (let [frozen-pred (b/serialize pred)]
      (cl/normal-request
        client :list-range-filter-count
        [db-name dbi-name frozen-pred k-range kt v-range vt raw-pred?]
        writing?)))

  (visit-list-range [db list-name visitor k-range k-type v-range v-type]
    (.visit-list-range db list-name visitor k-range k-type v-range v-type true))
  (visit-list-range [_ dbi-name visitor k-range kt v-range vt raw-pred?]
    (let [frozen-visitor (b/serialize visitor)]
      (cl/normal-request
        client :visit-list-range
        [db-name dbi-name frozen-visitor k-range kt v-range vt raw-pred?]
        writing?)))

  IAdmin
  (re-index [db opts]
    (cl/normal-request client :kv-re-index [db-name opts])
    db))

(defn open-kv
  "Open a remote kv store."
  ([uri-str]
   (open-kv uri-str nil))
  ([uri-str opts]
   (open-kv (cl/new-client uri-str (:client-opts opts)) uri-str opts))
  ([client uri-str opts]
   (let [uri     (URI. uri-str)
         uri-str (str uri-str
                      (if (cl/parse-query uri) "&" "?")
                      "store=" c/db-store-kv)]
     (if-let [db-name (cl/parse-db uri)]
       (do (cl/open-database client db-name c/db-store-kv opts)
           (->KVStore uri-str db-name client
                      (volatile! :remote-kv-mutex) false))
       (u/raise "URI should contain a database name" {})))))

(defn batch-kv
  "Run multiple KV calls against a remote KV store in one RPC round trip.

  Each call is a vector `[op & args]`, where `op` is one of:
  `:get-value`, `:get-rank`, `:get-by-rank`, `:sample-kv`, `:get-first`,
  `:get-first-n`, `:get-range`, `:key-range`, `:key-range-count`,
  `:key-range-list-count`, `:range-count`, `:get-list`, `:list-count`,
  `:in-count?`, `:list-range`, `:list-range-count`, `:list-range-first`,
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
