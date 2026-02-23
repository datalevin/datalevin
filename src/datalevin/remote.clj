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
   [datalevin.lmdb :as l :refer [IWriting]]
   [clojure.string :as str])
  (:import
   [datalevin.client Client]
   [datalevin.interface ILMDB IList IAdmin IStore ISearchEngine IVectorIndex]
   [java.nio.file Files Paths StandardOpenOption LinkOption]
   [java.net URI]))

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
   (let [t (if (= datom-type :txs)
                  :tx-data
                  :load-datoms)
         {:keys [type message result err-data]}
         (if (< (count datoms) ^long c/+wire-datom-batch-size+)
           (cl/request client {:type     t
                               :mode     :request
                               :writing? writing?
                               :args     (if (= datom-type :txs)
                                           [db-name datoms simulated?]
                                           [db-name datoms])})
           (cl/copy-in client {:type     t
                               :mode     :copy-in
                               :writing? writing?
                               :args     (if (= datom-type :txs)
                                           [db-name simulated?]
                                           [db-name])}
                       datoms c/+wire-datom-batch-size+))]
     (if (= type :error-response)
       (if (:resized err-data)
         (u/raise message err-data)
         (u/raise "Error loading datoms to server:" message {}))
       result))))

;; remote datalog db

(defprotocol IRemoteDB
  (q [store query inputs]
    "For special case of queries with a single remote store as source,
     send the query and inputs over to remote server")
  (pull [store pattern id opts])
  (pull-many [store pattern id opts])
  (explain [store opts query inputs])
  (fulltext-datoms [store query opts])
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
                       writing?]
  IWriting
  (writing? [_] writing?)

  (write-txn [_] write-txn)

  (mark-write [_]
    (->DatalogStore uri db-name client (volatile! :remote-dl-mutex) true))

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
    (cl/normal-request client :start-sampling [db-name] writing?))

  (stop-sampling [_]
    (cl/normal-request client :stop-sampling [db-name] writing?))

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

  (tx-data [_ data simulated?]
    (load-datoms* client db-name data :txs simulated? writing?))

  (open-transact [this]
    (cl/normal-request client :open-transact [db-name])
    (.mark-write this))

  (abort-transact [this]
    (cl/normal-request client :abort-transact [db-name] true))

  (close-transact [_]
    (cl/normal-request client :close-transact [db-name] true))

  ILMDB
  (sync [this] (.sync this 1))
  (sync [_ force]
    (cl/normal-request client :sync [db-name force] writing?))

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
                       c/db-store-datalog)]
         (cl/open-database client db-name store schema opts)
         (->DatalogStore uri-str db-name client
                         (volatile! :remote-dl-mutex) false))
       (u/raise "URI should contain a database name" {})))))

;; remote kv store

(declare ->KVStore)

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

  (open-dbi [db dbi-name]
    (.open-dbi db dbi-name nil))
  (open-dbi [_ dbi-name opts]
    (cl/normal-request client :open-dbi [db-name dbi-name opts] writing?))

  (clear-dbi [db dbi-name]
    (cl/normal-request client :clear-dbi [db-name dbi-name] writing?))

  (drop-dbi [db dbi-name]
    (cl/normal-request client :drop-dbi [db-name dbi-name] writing?))

  (list-dbis [db] (cl/normal-request client :list-dbis [db-name] writing?))

  ;; TODO need to zip the dir and checksum
  (copy [db dest] (.copy db dest false))
  (copy [_ dest compact?]
    (let [bs   (->> (cl/normal-request client :copy [db-name compact?] writing?)
                    (apply str)
                    b/decode-base64)
          dir  (Paths/get dest (into-array String []))
          file (Paths/get (str dest u/+separator+ c/data-file-name)
                          (into-array String []))]
      (when-not (Files/exists dir (into-array LinkOption []))
        (u/create-dirs dest))
      (Files/write file ^bytes bs
                   ^"[Ljava.nio.file.StandardOpenOption;"
                   (into-array StandardOpenOption []))
      (spit (str dest u/+separator+ c/version-file-name) c/version)))

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
                         :args     [db-name dbi-name txs k-type v-type]}
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
    (cl/normal-request
      client :get-value
      [db-name dbi-name k k-type v-type ignore-key?] writing?))

  (get-rank [db dbi-name k]
    (.get-rank db dbi-name k :data))
  (get-rank [_ dbi-name k k-type]
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

  ;; TODO implements batch remote request
  ;; (range-seq [db dbi-name k-range]
  ;;   (.range-seq db dbi-name k-range :data :data false nil))
  ;; (range-seq [db dbi-name k-range k-type]
  ;;   (.range-seq db dbi-name k-range k-type :data false nil))
  ;; (range-seq [db dbi-name k-range k-type v-type]
  ;;   (.range-seq db dbi-name k-range k-type v-type false nil))
  ;; (range-seq [db dbi-name k-range k-type v-type ignore-key?]
  ;;   (.range-seq db dbi-name k-range k-type v-type ignore-key? nil))
  ;; (range-seq [_ dbi-name k-range k-type v-type ignore-key? opts]
  ;;   (cl/normal-request
  ;;     client :get-range
  ;;     [db-name dbi-name k-range k-type v-type ignore-key?] writing?))

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
    (cl/normal-request client :list-count
                       [db-name dbi-name k kt] writing?))

  (in-list? [_ dbi-name k v kt vt]
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
