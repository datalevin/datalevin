;;
;; Copyright (c) Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns ^:no-doc pod.huahaiy.datalevin
  "Implement babashka pod"
  (:refer-clojure :exclude [sync read read-string])
  (:require
   [bencode.core :as bencode]
   [sci.core :as sci]
   [datalevin.core :as d]
   [datalevin.util :as u]
   [datalevin.lmdb :as l]
   [datalevin.interpret :as i]
   [datalevin.protocol :as p]
   [datalevin.bits :as b]
   [datalevin.csv :as csv]
   [datalevin.datom :as dd]
   [datalevin.db :as db]
   [datalevin.interface :as if]
   [datalevin.storage :as st]
   [clojure.java.io :as io]
   [clojure.walk :as w])
  (:import
   [java.io PushbackInputStream]
   [java.nio.charset StandardCharsets]
   [datalevin.storage Store]
   [datalevin.entity Entity]
   [datalevin.db DB]
   [datalevin.search SearchEngine]
   [datalevin.vector VectorIndex]
   [java.util UUID])
  (:gen-class))

(def pod-ns "pod.huahaiy.datalevin")

(def debug? false)

(defn debug [& args]
  (when debug?
    (binding [*out* (io/writer "/tmp/datalevin-pod-debug.log" :append true)]
      (apply println args))))

(def stdin (PushbackInputStream. System/in))

(defn- write-response [v]
  (bencode/write-bencode System/out v)
  (.flush System/out))

(defn- read-string [^bytes v]
  (String. v StandardCharsets/UTF_8))

(defn- read []
  (bencode/read-bencode stdin))

;; dbs

;; uuid -> conn
(defonce ^:private dl-conns (atom {}))

;; uuid -> writing wconn
(defonce ^:private wdl-conns (atom {}))

;; uuid -> dl db
(defonce ^:private dl-dbs (atom {}))

;; uuid -> writing dl db
(defonce ^:private wdl-dbs (atom {}))

;; uuid -> kv db
(defonce ^:private kv-dbs (atom {}))

;; uuid -> writing kv db
(defonce ^:private wkv-dbs (atom {}))

;; uuid -> search engine
(defonce ^:private engines (atom {}))

;; uuid -> vector index
(defonce ^:private indices (atom {}))

;; uuid -> embedding provider
(defonce ^:private embedding-providers (atom {}))

;; uuid -> LLM provider
(defonce ^:private llm-providers (atom {}))

;; uuid -> search index writer
(defonce ^:private search-writers (atom {}))

;; exposed functions

(defn pod-fn [fn-name args & body]
  (intern 'pod.huahaiy.datalevin (symbol fn-name)
          (sci/eval-form i/ctx (apply list 'fn args body)))
  {::inter-fn fn-name})

(defn- get-cn [{:keys [::conn writing?]}]
  (if writing? (get @wdl-conns conn) (get @dl-conns conn)))

(defn- get-db [{:keys [::db writing?]}]
  (if writing? (get @wdl-dbs db) (get @dl-dbs db)))

(defn- get-kv [{:keys [::kv-db writing?]}]
  (if writing? (get @wkv-dbs kv-db) (get @kv-dbs kv-db)))

(defn- get-engine [{:keys [::engine]}] (get @engines engine))

(defn- get-index [{:keys [::index]}] (get @indices index))

(defn- get-embedding-provider [{:keys [::embedding-provider]}]
  (get @embedding-providers embedding-provider))

(defn- get-llm-provider [{:keys [::llm-provider]}]
  (get @llm-providers llm-provider))

(defn- get-search-writer [{:keys [::search-writer]}]
  (get @search-writers search-writer))

(defn- db-ref
  ([db]
   (db-ref db false))
  ([db writing?]
   (let [id (UUID/randomUUID)]
     (swap! (if writing? wdl-dbs dl-dbs) assoc id db)
     (cond-> {::db id}
       writing? (assoc :writing? true)))))

(defn- rp->res
  ([rp]
   (rp->res rp false))
  ([rp include-dbs?]
   (cond-> {:tx-data (:tx-data rp)
            :tempids (:tempids rp)
            :tx-meta (:tx-meta rp)}
     include-dbs?
     (assoc :db-before (db-ref (:db-before rp))
            :db-after (db-ref (:db-after rp))))))

(defn- embedding-result
  [xs]
  (mapv vec xs))

(defn entid [dl eid] (when-let [d (get-db dl)] (d/entid d eid)))

(defn entity [{:keys [::db] :as dl} eid]
  (when-let [^DB d (get-db dl)]
    (let [^Entity e (d/touch (d/entity d eid))]
      (assoc @(.-cache e) :db/id (.-eid e) :db-name db))))

(defn- entity-id
  [ent]
  (or (:db/id ent)
      (when (instance? Entity ent)
        (.-eid ^Entity ent))
      (u/raise "Entity must have :db/id" {:entity ent})))

(defn add
  [ent attr value]
  (if (instance? Entity ent)
    (d/add ent attr value)
    [:db/add (entity-id ent) attr value]))

(defn retract
  ([ent attr]
   (if (instance? Entity ent)
     (d/retract ent attr)
     [:db.fn/retractAttribute (entity-id ent) attr]))
  ([ent attr value]
   (if (instance? Entity ent)
     (d/retract ent attr value)
     [:db/retract (entity-id ent) attr value])))

(defn entity-db
  [ent]
  (cond
    (instance? Entity ent)
    (db-ref (d/entity-db ent))

    (:db-name ent)
    {::db (:db-name ent)}

    :else
    (u/raise "Entity must have :db-name" {:entity ent})))

(defn touch [{:keys [db-name db/id]}]
  (when-let [d (get @dl-dbs db-name)]
    (let [^Entity e (d/touch (d/entity d id))]
      (assoc @(.-cache e) :db/id id :db-name db-name))))

(defn pull
  ([dl selector eid]
   (when-let [d (get-db dl)]
     (d/pull d selector eid)))
  ([dl selector eid opts]
   (when-let [d (get-db dl)]
     (d/pull d selector eid opts))))

(defn pull-many
  ([dl selector eids]
   (when-let [d (get-db dl)]
     (d/pull-many d selector eids)))
  ([dl selector eids opts]
   (when-let [d (get-db dl)]
     (d/pull-many d selector eids opts))))

(defn q [q & inputs]
  (apply d/q q (w/postwalk #(if (::db %) (get-db %) %) inputs)))

(defn explain [opts q & inputs]
  (apply d/explain opts q (w/postwalk #(if (::db %) (get-db %) %) inputs)))

(defn datom
  ([e a v] [e a v])
  ([e a v tx] [e a v tx])
  ([e a v tx added] [e a v tx added]))

(defn datom?
  [x]
  (or (dd/datom? x)
      (and (vector? x)
           (<= 3 (count x) 5))))

(defn datom-e
  [d]
  (if (dd/datom? d) (dd/datom-e d) (nth d 0)))

(defn datom-a
  [d]
  (if (dd/datom? d) (dd/datom-a d) (nth d 1)))

(defn datom-v
  [d]
  (if (dd/datom? d) (dd/datom-v d) (nth d 2)))

(defn- ->datom
  [d]
  (if (dd/datom? d) d (apply dd/datom d)))

(defn empty-db
  ([] (empty-db nil nil))
  ([dir] (empty-db dir nil))
  ([dir schema] (empty-db dir schema nil))
  ([dir schema opts]
   (let [id (UUID/randomUUID)
         db (d/empty-db dir schema (if opts
                                     (assoc opts :db-name id)
                                     {:db-name id}))]
     (swap! dl-dbs assoc id db)
     {::db id})))

(defn db? [dl] (when-let [d (get-db dl)] (d/db? d)))

(defn init-db
  ([datoms]
   (init-db datoms nil nil nil))
  ([datoms dir]
   (init-db datoms dir nil nil))
  ([datoms dir schema]
   (init-db datoms dir schema nil))
  ([datoms dir schema opts]
   (let [id (UUID/randomUUID)
         db (d/init-db (map ->datom datoms)
                       dir
                       schema
                       (if opts
                         (assoc opts :db-name id)
                         {:db-name id}))]
     (swap! dl-dbs assoc id db)
     {::db id})))

(defn fill-db
  [{:keys [::db writing?] :as dl} datoms]
  (when-let [old-db (get-db dl)]
    (let [db' (d/fill-db old-db (map ->datom datoms))
          dbs (if writing? wdl-dbs dl-dbs)]
      (swap! dbs assoc db db')
      (cond-> {::db db}
        writing? (assoc :writing? true)))))

(defn conn-from-datoms
  ([datoms]
   (conn-from-datoms datoms nil nil nil))
  ([datoms dir]
   (conn-from-datoms datoms dir nil nil))
  ([datoms dir schema]
   (conn-from-datoms datoms dir schema nil))
  ([datoms dir schema opts]
   (let [id   (UUID/randomUUID)
         conn (d/conn-from-datoms
                (map ->datom datoms)
                dir
                schema
                (if opts
                  (assoc opts :db-name id)
                  {:db-name id}))]
     (swap! dl-dbs assoc id @conn)
     (swap! dl-conns assoc id conn)
     {::conn id})))

(defn tempid
  ([part] (d/tempid part))
  ([part x] (d/tempid part x)))

(defn resolve-tempid
  [db tempids tempid]
  (d/resolve-tempid db tempids tempid))

(defn squuid
  ([] (d/squuid))
  ([msec] (d/squuid msec)))

(defn squuid-time-millis
  [uuid]
  (d/squuid-time-millis uuid))

(defn hexify-string
  [s]
  (d/hexify-string s))

(defn unhexify-string
  [s]
  (d/unhexify-string s))

(defn explicit-transaction-timeout
  ([] (d/explicit-transaction-timeout))
  ([timeout-ms] (d/explicit-transaction-timeout timeout-ms)))

(defn set-explicit-transaction-timeout!
  [timeout-ms]
  (d/set-explicit-transaction-timeout! timeout-ms))

(defn close-db [dl] (when-let [d (get-db dl)] (d/close-db d)))

(defn datoms
  ([dl index]
   (when-let [d (get-db dl)] (map dd/datom-eav (d/datoms d index))))
  ([dl index c1]
   (when-let [d (get-db dl)] (map dd/datom-eav (d/datoms d index c1))))
  ([dl index c1 c2]
   (when-let [d (get-db dl)] (map dd/datom-eav (d/datoms d index c1 c2))))
  ([dl index c1 c2 c3]
   (when-let [d (get-db dl)]
     (map dd/datom-eav (d/datoms d index c1 c2 c3))))
  ([dl index c1 c2 c3 n]
   (when-let [d (get-db dl)]
     (map dd/datom-eav (d/datoms d index c1 c2 c3 n)))))

(defn search-datoms
  [dl e a v]
  (when-let [d (get-db dl)]
    (map dd/datom-eav (d/search-datoms d e a v))))

(defn count-datoms
  [dl e a v]
  (when-let [d (get-db dl)] (d/count-datoms d e a v)))

(defn cardinality
  [dl a]
  (when-let [d (get-db dl)] (d/cardinality d a)))

(defn max-eid
  [dl]
  (when-let [d (get-db dl)] (d/max-eid d)))

(defn analyze
  ([dl]
   (when-let [d (get-db dl)] (d/analyze d nil)))
  ([dl attr]
   (when-let [d (get-db dl)] (d/analyze d attr))))

(defn seek-datoms
  ([dl index]
   (when-let [d (get-db dl)] (map dd/datom-eav (d/seek-datoms d index))))
  ([dl index c1]
   (when-let [d (get-db dl)]
     (map dd/datom-eav (d/seek-datoms d index c1))))
  ([dl index c1 c2]
   (when-let [d (get-db dl)]
     (map dd/datom-eav (d/seek-datoms d index c1 c2))))
  ([dl index c1 c2 c3]
   (when-let [d (get-db dl)]
     (map dd/datom-eav (d/seek-datoms d index c1 c2 c3))))
  ([dl index c1 c2 c3 n]
   (when-let [d (get-db dl)]
     (map dd/datom-eav (d/seek-datoms d index c1 c2 c3 n)))))

(defn fulltext-datoms
  ([dl query]
   (when-let [d (get-db dl)]
     (d/fulltext-datoms d query)))
  ([dl query opts]
   (when-let [d (get-db dl)]
     (d/fulltext-datoms d query opts))))

(defn rseek-datoms
  ([dl index]
   (when-let [d (get-db dl)] (map dd/datom-eav (d/rseek-datoms d index))))
  ([dl index c1]
   (when-let [d (get-db dl)]
     (map dd/datom-eav (d/rseek-datoms d index c1))))
  ([dl index c1 c2]
   (when-let [d (get-db dl)]
     (map dd/datom-eav (d/rseek-datoms d index c1 c2))))
  ([dl index c1 c2 c3]
   (when-let [d (get-db dl)]
     (map dd/datom-eav (d/rseek-datoms d index c1 c2 c3))))
  ([dl index c1 c2 c3 n]
   (when-let [d (get-db dl)]
     (map dd/datom-eav (d/rseek-datoms d index c1 c2 c3 n)))))

(defn index-range [dl attr start end]
  (when-let [d (get-db dl)]
    (map dd/datom-eav (d/index-range d attr start end))))

(defn conn? [cn] (when-let [c (get-cn cn)] (d/conn? c)))

(defn conn-from-db [{:keys [::db] :as dl}]
  (when-let [d (get-db dl)]
    (let [conn (d/conn-from-db d)]
      (swap! dl-conns assoc db conn)
      {::conn db})))

(defn create-conn
  ([] (conn-from-db (empty-db)))
  ([dir] (conn-from-db (empty-db dir)))
  ([dir schema] (conn-from-db (empty-db dir schema)))
  ([dir schema opts] (conn-from-db (empty-db dir schema opts))))

(defn close [{:keys [::conn]}]
  (let [[old _] (swap-vals! dl-conns dissoc conn)]
    (when-let [c (get old conn)]
      (d/close c))))

(defn closed? [cn] (when-let [c (get-cn cn)] (d/closed? c)))

(defn datalog-index-cache-limit
  ([cn] (when-let [c (get-cn cn)] (d/datalog-index-cache-limit c)))
  ([cn n] (when-let [c (get-cn cn)] (d/datalog-index-cache-limit c n))))

(defn transact
  ([cn tx-data] (when-let [c (get-cn cn)] (d/transact c tx-data)))
  ([cn tx-data tx-meta]
   (when-let [c (get-cn cn)] (d/transact c tx-data tx-meta))))

(defn transact-async*
  [cn tx-data tx-meta]
  (when-let [c (get-cn cn)]
    (let [fut (d/transact-async c tx-data tx-meta)]
      (rp->res @fut))))

(defn transact!
  ([cn tx-data]
   (transact! cn tx-data nil))
  ([{:keys [::conn writing?] :as cn} tx-data tx-meta]
   (when-let [c (get-cn cn)]
     (let [rp (try
                (d/transact! c tx-data tx-meta)
                (catch Exception e
                  (when (:resized (ex-data e))
                    (if writing?
                      (let [outer-c  (get @dl-conns conn)
                            outer-db ^DB @outer-c
                            outer-s  (.-store outer-db)
                            write-s  (.-store ^DB @c)
                            write-l  (.-lmdb ^Store write-s)
                            d        (db/transfer outer-db
                                                  (st/transfer outer-s write-l))]
                        (swap! wdl-dbs assoc conn d)
                        (swap! wdl-conns assoc conn
                               (atom d :meta (meta outer-c))))
                      (let [s (.-store ^DB @c)
                            d (db/new-db s)]
                        (swap! dl-dbs assoc conn d)
                        (swap! dl-conns assoc conn
                               (atom d :meta (meta c))))))
                  (throw e)))]
       (rp->res rp)))))

(defn with
  ([dl tx-data]
   (with dl tx-data {} false))
  ([dl tx-data tx-meta]
   (with dl tx-data tx-meta false))
  ([dl tx-data tx-meta simulated?]
   (when-let [d (get-db dl)]
     (rp->res (d/with d tx-data tx-meta simulated?) true))))

(defn db-with
  [dl tx-data]
  (when-let [d (get-db dl)]
    (db-ref (d/db-with d tx-data))))

(defn tx-data->simulated-report
  [dl tx-data]
  (when-let [d (get-db dl)]
    (rp->res (d/tx-data->simulated-report d tx-data) true)))

(defn reset-conn!
  ([cn dl]
   (reset-conn! cn dl nil))
  ([{:keys [::conn]} dl tx-meta]
   (when-let [c (get-cn {::conn conn})]
     (when-let [d (get-db dl)]
       (let [db' (d/reset-conn! c d tx-meta)]
         (swap! dl-dbs assoc conn db')
         {::db conn})))))

(defn- callback-fn
  [callback]
  (cond
    (ifn? callback)
    callback

    (::inter-fn callback)
    (or (ns-resolve 'pod.huahaiy.datalevin (symbol (::inter-fn callback)))
        (u/raise "Pod function not found: " (::inter-fn callback)
                 {:callback callback}))

    (symbol? callback)
    (or (ns-resolve 'pod.huahaiy.datalevin callback)
        (u/raise "Pod function not found: " callback
                 {:callback callback}))

    :else
    (u/raise "Callback must be a function, pod function token, or symbol"
             {:callback callback})))

(defn listen!
  ([cn callback]
   (listen! cn (rand) callback))
  ([cn key callback]
   (when-let [c (get-cn cn)]
     (let [f (callback-fn callback)]
       (d/listen! c key #(f (rp->res % true)))))))

(defn unlisten!
  [cn key]
  (when-let [c (get-cn cn)]
    (d/unlisten! c key)))

(defn db [{:keys [::conn] :as cn}]
  (when-let [c (get-cn cn)]
    (let [db       (d/db c)
          writing? (:writing? cn)
          dbs      (if writing? wdl-dbs dl-dbs)
          ref      (cond-> {::db conn}
                     writing? (assoc :writing? true))]
      (swap! dbs assoc conn db)
      ref)))

(defn opts [cn] (when-let [c (get-cn cn)] (d/opts c)))

(defn schema [cn] (when-let [c (get-cn cn)] (d/schema c)))

(defn update-schema
  ([cn schema-update]
   (when-let [c (get-cn cn)] (d/update-schema c schema-update)))
  ([cn schema-update del-attrs]
   (when-let [c (get-cn cn)] (d/update-schema c schema-update del-attrs)))
  ([cn schema-update del-attrs rename-map]
   (when-let [c (get-cn cn)]
     (d/update-schema c schema-update del-attrs rename-map))))

(defn secondary-index-status
  [cn]
  (when-let [c (get-cn cn)] (d/secondary-index-status c)))

(defn process-secondary-index-jobs!
  ([cn]
   (when-let [c (get-cn cn)] (d/process-secondary-index-jobs! c)))
  ([cn opts]
   (when-let [c (get-cn cn)] (d/process-secondary-index-jobs! c opts))))

(defn wait-for-secondary-index
  ([cn]
   (when-let [c (get-cn cn)] (d/wait-for-secondary-index c)))
  ([cn opts]
   (when-let [c (get-cn cn)] (d/wait-for-secondary-index c opts))))

(defn get-conn
  ([dir]
   (get-conn dir nil nil))
  ([dir schema]
   (get-conn dir schema nil))
  ([dir schema opts]
   (let [conn (d/get-conn dir schema opts)]
     (if-let [id (some (fn [[id c]] (when (= conn c) id)) @dl-conns)]
       {::conn id}
       (let [id (UUID/randomUUID)]
         (swap! dl-dbs assoc id @conn)
         (swap! dl-conns assoc id conn)
         {::conn id})))))

(defn clear [cn] (when-let [c (get-cn cn)] (d/clear c)))

(defn open-kv
  ([dir]
   (open-kv dir nil))
  ([dir opts]
   (let [db (d/open-kv dir opts)
         id (UUID/randomUUID)]
     (swap! kv-dbs assoc id db)
     {::kv-db id})))

(defn datalog-kv
  [{:keys [writing?] :as conn-or-db}]
  (when-let [kv (or (some-> (get-cn conn-or-db) d/datalog-kv)
                    (some-> (get-db conn-or-db) d/datalog-kv))]
    (let [id  (UUID/randomUUID)
          dbs (if writing? wkv-dbs kv-dbs)]
      (swap! dbs assoc id kv)
      (cond-> {::kv-db id}
        writing? (assoc :writing? true)))))

(defn k [kv] (l/k kv))

(defn v [kv] (l/v kv))

(defn put-buffer
  ([bf x]
   (b/put-buffer bf x))
  ([bf x x-type]
   (b/put-buffer bf x x-type)))

(defn read-buffer
  ([bf]
   (b/read-buffer bf))
  ([bf v-type]
   (b/read-buffer bf v-type)))

(defn close-kv [db] (when-let [d (get-kv db)] (d/close-kv d)))

(defn closed-kv? [db] (when-let [d (get-kv db)] (d/closed-kv? d)))

(defn dir [db] (when-let [d (get-kv db)] (d/dir d)))

(defn open-dbi
  ([db dbi-name]
   (when-let [d (get-kv db)] (d/open-dbi d dbi-name) nil))
  ([db dbi-name opts]
   (when-let [d (get-kv db)] (d/open-dbi d dbi-name opts) nil)))

(defn clear-dbi [db dbi-name]
  (when-let [d (get-kv db)] (d/clear-dbi d dbi-name)))

(defn drop-dbi [db dbi-name]
  (when-let [d (get-kv db)] (d/drop-dbi d dbi-name)))

(defn list-dbis [db] (when-let [d (get-kv db)] (d/list-dbis d)))

(defn copy
  ([db dest]
   (copy db dest false))
  ([db dest compact?]
   (when-let [d (get-kv db)] (d/copy d dest compact?))))

(defn stat
  ([db]
   (when-let [d (get-kv db)] (d/stat d)))
  ([db dbi-name]
   (when-let [d (get-kv db)] (d/stat d dbi-name))))

(defn entries [db dbi-name] (when-let [d (get-kv db)] (d/entries d dbi-name)))

(defn sync [db] (when-let [d (get-kv db)] (d/sync d)))

(defn txlog-watermarks [db]
  (when-let [d (get-kv db)] (d/txlog-watermarks d)))

(defn open-tx-log
  ([db from-lsn]
   (open-tx-log db from-lsn nil))
  ([db from-lsn upto-lsn]
   (when-let [d (get-kv db)] (d/open-tx-log d from-lsn upto-lsn))))

(defn create-snapshot! [db]
  (when-let [d (get-kv db)] (d/create-snapshot! d)))

(defn list-snapshots [db]
  (when-let [d (get-kv db)] (d/list-snapshots d)))

(defn gc-txlog-segments!
  ([db]
   (gc-txlog-segments! db nil))
  ([db retain-floor-lsn]
   (when-let [d (get-kv db)] (d/gc-txlog-segments! d retain-floor-lsn))))

(defn get-env-flags [db] (when-let [d (get-kv db)] (d/get-env-flags d)))

(defn set-env-flags [db ks on-off]
  (when-let [d (get-kv db)] (d/set-env-flags d ks on-off)))

(defn open-transact-kv [{:keys [::kv-db] :as db}]
  (when-let [d (get @kv-dbs kv-db)]
    (let [wdb (if/open-transact-kv d)]
      (swap! wkv-dbs assoc kv-db wdb)
      (assoc db :writing? true))))

(defn close-transact-kv [{:keys [::kv-db]}]
  (when-let [d (get @kv-dbs kv-db)]
    (swap! wkv-dbs dissoc kv-db)
    (if/close-transact-kv d)))

(defn abort-transact-kv [{:keys [::kv-db]}]
  (when-let [d (get @wkv-dbs kv-db)]
    (d/abort-transact-kv d)))

(defn begin-kv-transaction
  [db]
  (open-transact-kv db))

(defn commit-kv-transaction
  [db]
  (close-transact-kv db))

(defn abort-kv-transaction
  [db]
  (abort-transact-kv db)
  (close-transact-kv db))

(defn open-transact [{:keys [::conn] :as cn}]
  (when-let [c (get @dl-conns conn)]
    (let [db ^DB @c
          s  (.-store db)
          l  (.-lmdb ^Store s)
          wl (if/open-transact-kv l)
          ws (st/transfer s wl)
          wd (db/transfer db ws)]
      (swap! wdl-dbs assoc conn wd)
      (swap! wdl-conns assoc conn (atom wd :meta (meta c)))
      (assoc cn :writing? true))))

(defn close-transact [{:keys [::conn]}]
  (when-let [c (get @dl-conns conn)]
    (let [outer-db ^DB @c
          wc       (get @wdl-conns conn)
          ws       (.-store ^DB @wc)
          l        (.-lmdb ^Store (.-store outer-db))]
      (if/close-transact-kv l)
      (reset! c (db/carry-runtime-opts
                  (db/new-db (st/transfer ws l))
                  outer-db))
      (swap! dl-dbs assoc conn @c)
      (swap! wdl-dbs dissoc conn)
      (swap! wdl-conns dissoc conn)
      nil)))

(defn abort-transact [{:keys [::conn]}]
  (when-let [c (get @dl-conns conn)]
    (let [wc (get @wdl-conns conn)
          ws (.-store ^DB @wc)
          wl (.-lmdb ^Store ws)]
      (d/abort-transact-kv wl))))

(defn transact-kv
  ([db txs]
   (when-let [d (get-kv db)] (d/transact-kv d txs)))
  ([db dbi-name txs]
   (when-let [d (get-kv db)] (d/transact-kv d dbi-name txs)))
  ([db dbi-name txs k-type]
   (when-let [d (get-kv db)] (d/transact-kv d dbi-name txs k-type)))
  ([db dbi-name txs k-type v-type]
   (when-let [d (get-kv db)] (d/transact-kv d dbi-name txs k-type v-type))))

(defn transact-kv-async*
  [db dbi-name txs k-type v-type]
  (when-let [d (get-kv db)]
    @(d/transact-kv-async d dbi-name txs k-type v-type)))

(defn get-value
  ([db dbi-name k]
   (when-let [d (get-kv db)] (d/get-value d dbi-name k)))
  ([db dbi-name k k-type]
   (when-let [d (get-kv db)] (d/get-value d dbi-name k k-type)))
  ([db dbi-name k k-type v-type]
   (when-let [d (get-kv db)]
     (d/get-value d dbi-name k k-type v-type)))
  ([db dbi-name k k-type v-type ignore-key?]
   (when-let [d (get-kv db)]
     (d/get-value d dbi-name k k-type v-type ignore-key?))))

(defn get-rank
  ([db dbi-name k]
   (when-let [d (get-kv db)] (d/get-rank d dbi-name k)))
  ([db dbi-name k k-type]
   (when-let [d (get-kv db)] (d/get-rank d dbi-name k k-type))))

(defn get-by-rank
  ([db dbi-name rank]
   (when-let [d (get-kv db)] (d/get-by-rank d dbi-name rank)))
  ([db dbi-name rank k-type]
   (when-let [d (get-kv db)] (d/get-by-rank d dbi-name rank k-type)))
  ([db dbi-name rank k-type v-type]
   (when-let [d (get-kv db)]
     (d/get-by-rank d dbi-name rank k-type v-type)))
  ([db dbi-name rank k-type v-type ignore-key?]
   (when-let [d (get-kv db)]
     (d/get-by-rank d dbi-name rank k-type v-type ignore-key?))))

(defn sample-kv
  ([db dbi-name n]
   (when-let [d (get-kv db)] (d/sample-kv d dbi-name n)))
  ([db dbi-name n k-type]
   (when-let [d (get-kv db)] (d/sample-kv d dbi-name n k-type)))
  ([db dbi-name n k-type v-type]
   (when-let [d (get-kv db)]
     (d/sample-kv d dbi-name n k-type v-type)))
  ([db dbi-name n k-type v-type ignore-key?]
   (when-let [d (get-kv db)]
     (d/sample-kv d dbi-name n k-type v-type ignore-key?))))

(defn get-first
  ([db dbi-name k-range]
   (when-let [d (get-kv db)] (d/get-first d dbi-name k-range)))
  ([db dbi-name k-range k-type]
   (when-let [d (get-kv db)] (d/get-first d dbi-name k-range k-type)))
  ([db dbi-name k-range k-type v-type]
   (when-let [d (get-kv db)]
     (d/get-first d dbi-name k-range k-type v-type)))
  ([db dbi-name k-range k-type v-type ignore-key?]
   (when-let [d (get-kv db)]
     (d/get-first d dbi-name k-range k-type v-type ignore-key?))))

(defn get-first-n
  ([db dbi-name n k-range]
   (when-let [d (get-kv db)]
     (into [] (d/get-first-n d dbi-name n k-range))))
  ([db dbi-name n k-range k-type]
   (when-let [d (get-kv db)]
     (into [] (d/get-first-n d dbi-name n k-range k-type))))
  ([db dbi-name n k-range k-type v-type]
   (when-let [d (get-kv db)]
     (into [] (d/get-first-n d dbi-name n k-range k-type v-type))))
  ([db dbi-name n k-range k-type v-type ignore-key?]
   (when-let [d (get-kv db)]
     (into [] (d/get-first-n d dbi-name n k-range k-type v-type ignore-key?)))))

(defn get-range
  ([db dbi-name k-range]
   (when-let [d (get-kv db)]
     (into [] (d/get-range d dbi-name k-range))))
  ([db dbi-name k-range k-type]
   (when-let [d (get-kv db)]
     (into [] (d/get-range d dbi-name k-range k-type))))
  ([db dbi-name k-range k-type v-type]
   (when-let [d (get-kv db)]
     (into [] (d/get-range d dbi-name k-range k-type v-type))))
  ([db dbi-name k-range k-type v-type ignore-key?]
   (when-let [d (get-kv db)]
     (into [] (d/get-range d dbi-name k-range k-type v-type ignore-key?)))))

(defn- realize-range-seq
  [^java.lang.AutoCloseable rs]
  (with-open [rs rs]
    (into [] cat rs)))

(defn range-seq
  ([db dbi-name k-range]
   (when-let [d (get-kv db)]
     (realize-range-seq (d/range-seq d dbi-name k-range))))
  ([db dbi-name k-range k-type]
   (when-let [d (get-kv db)]
     (realize-range-seq (d/range-seq d dbi-name k-range k-type))))
  ([db dbi-name k-range k-type v-type]
   (when-let [d (get-kv db)]
     (realize-range-seq (d/range-seq d dbi-name k-range k-type v-type))))
  ([db dbi-name k-range k-type v-type ignore-key?]
   (when-let [d (get-kv db)]
     (realize-range-seq
       (d/range-seq d dbi-name k-range k-type v-type ignore-key?))))
  ([db dbi-name k-range k-type v-type ignore-key? opts]
   (when-let [d (get-kv db)]
     (realize-range-seq
       (d/range-seq d dbi-name k-range k-type v-type ignore-key? opts)))))

(defn key-range
  ([db dbi-name k-range]
   (when-let [d (get-kv db)]
     (into [] (d/key-range d dbi-name k-range))))
  ([db dbi-name k-range k-type]
   (when-let [d (get-kv db)]
     (into [] (d/key-range d dbi-name k-range k-type)))))

(defn key-range-count
  ([db dbi-name k-range]
   (when-let [d (get-kv db)] (d/key-range-count d dbi-name k-range)))
  ([db dbi-name k-range k-type]
   (when-let [d (get-kv db)] (d/key-range-count d dbi-name k-range k-type))))

(defn key-range-list-count
  ([db dbi-name k-range k-type]
   (when-let [d (get-kv db)]
     (d/key-range-list-count d dbi-name k-range k-type))))

(defn visit-key-range
  ([db dbi-name visitor k-range]
   (when-let [d (get-kv db)]
     (d/visit-key-range d dbi-name visitor k-range)))
  ([db dbi-name visitor k-range k-type]
   (when-let [d (get-kv db)]
     (d/visit-key-range d dbi-name visitor k-range k-type)))
  ([db dbi-name visitor k-range k-type raw-pred?]
   (when-let [d (get-kv db)]
     (d/visit-key-range d dbi-name visitor k-range k-type raw-pred?))))

(defn range-count
  ([db dbi-name k-range]
   (when-let [d (get-kv db)] (d/range-count d dbi-name k-range)))
  ([db dbi-name k-range k-type]
   (when-let [d (get-kv db)] (d/range-count d dbi-name k-range k-type))))

(defn get-some
([db dbi-name pred k-range]
 (when-let [d (get-kv db)] (d/get-some d dbi-name pred k-range)))
([db dbi-name pred k-range k-type]
 (when-let [d (get-kv db)]
   (d/get-some d dbi-name pred k-range k-type)))
([db dbi-name pred k-range k-type v-type]
 (when-let [d (get-kv db)]
   (d/get-some d dbi-name pred k-range k-type v-type)))
([db dbi-name pred k-range k-type v-type ignore-key?]
 (when-let [d (get-kv db)]
   (d/get-some d dbi-name pred k-range k-type v-type ignore-key?)))
([db dbi-name pred k-range k-type v-type ignore-key? raw-pred?]
 (when-let [d (get-kv db)]
   (d/get-some d dbi-name pred k-range k-type v-type ignore-key? raw-pred?))))

(defn range-filter
([db dbi-name pred k-range]
 (when-let [d (get-kv db)]
   (into [] (d/range-filter d dbi-name pred k-range))))
([db dbi-name pred k-range k-type]
 (when-let [d (get-kv db)]
   (into [] (d/range-filter d dbi-name pred k-range k-type))))
([db dbi-name pred k-range k-type v-type]
 (when-let [d (get-kv db)]
   (into [] (d/range-filter d dbi-name pred k-range k-type v-type))))
([db dbi-name pred k-range k-type v-type ignore-key?]
 (when-let [d (get-kv db)]
   (into
     []
     (d/range-filter d dbi-name pred k-range k-type v-type ignore-key?))))
([db dbi-name pred k-range k-type v-type ignore-key? raw-pred?]
 (when-let [d (get-kv db)]
   (into
     []
     (d/range-filter d dbi-name pred k-range k-type v-type ignore-key?
                     raw-pred?)))))

(defn range-keep
([db dbi-name pred k-range]
 (when-let [d (get-kv db)]
   (into [] (d/range-keep d dbi-name pred k-range))))
([db dbi-name pred k-range k-type]
 (when-let [d (get-kv db)]
   (into [] (d/range-keep d dbi-name pred k-range k-type))))
([db dbi-name pred k-range k-type v-type]
 (when-let [d (get-kv db)]
   (into [] (d/range-keep d dbi-name pred k-range k-type v-type))))
([db dbi-name pred k-range k-type v-type raw-pred?]
 (when-let [d (get-kv db)]
   (into
     []
     (d/range-keep d dbi-name pred k-range k-type v-type raw-pred?)))))

(defn range-some
  ([db dbi-name pred k-range]
   (when-let [d (get-kv db)]
     (into [] (d/range-some d dbi-name pred k-range))))
  ([db dbi-name pred k-range k-type]
   (when-let [d (get-kv db)]
     (into [] (d/range-some d dbi-name pred k-range k-type))))
  ([db dbi-name pred k-range k-type v-type]
   (when-let [d (get-kv db)]
     (into [] (d/range-some d dbi-name pred k-range k-type v-type))))
  ([db dbi-name pred k-range k-type v-type raw-pred?]
   (when-let [d (get-kv db)]
     (into
       []
       (d/range-some d dbi-name pred k-range k-type v-type raw-pred?)))))

(defn range-filter-count
  ([db dbi-name pred k-range]
   (when-let [d (get-kv db)]
     (d/range-filter-count d dbi-name pred k-range)))
  ([db dbi-name pred k-range k-type]
   (when-let [d (get-kv db)]
     (d/range-filter-count d dbi-name pred k-range k-type)))
  ([db dbi-name pred k-range k-type v-type]
   (when-let [d (get-kv db)]
     (d/range-filter-count d dbi-name pred k-range k-type v-type)))
  ([db dbi-name pred k-range k-type v-type raw-pred?]
   (when-let [d (get-kv db)]
     (d/range-filter-count d dbi-name pred k-range k-type v-type raw-pred?))))

(defn visit
  ([db dbi-name pred k-range]
   (when-let [d (get-kv db)] (d/visit d dbi-name pred k-range)))
  ([db dbi-name pred k-range k-type]
   (when-let [d (get-kv db)] (d/visit d dbi-name pred k-range k-type)))
  ([db dbi-name pred k-range k-type v-type]
   (when-let [d (get-kv db)] (d/visit d dbi-name pred k-range k-type v-type)))
  ([db dbi-name pred k-range k-type v-type raw-pred?]
   (when-let [d (get-kv db)]
     (d/visit d dbi-name pred k-range k-type v-type raw-pred?))))

(defn open-list-dbi
  ([db dbi-name]
   (when-let [d (get-kv db)] (d/open-list-dbi d dbi-name) nil))
  ([db dbi-name opts]
   (when-let [d (get-kv db)] (d/open-list-dbi d dbi-name opts) nil)))

(defn put-list-items
  [db dbi-name k vs kt vt]
  (when-let [d (get-kv db)] (d/put-list-items d dbi-name k vs kt vt) nil))

(defn del-list-items
  ([db dbi-name k kt]
   (when-let [d (get-kv db)] (d/del-list-items d dbi-name k kt) nil))
  ([db dbi-name k vs kt vt]
   (when-let [d (get-kv db)] (d/del-list-items d dbi-name k vs kt vt) nil)))

(defn get-list
  [db dbi-name k kt vt]
  (when-let [d (get-kv db)]
    (when-let [res (d/get-list d dbi-name k kt vt)]
      (into [] res))))

(defn visit-list
  ([db dbi-name visitor k kt]
   (when-let [d (get-kv db)] (d/visit-list d dbi-name visitor k kt)))
  ([db dbi-name visitor k kt vt]
   (when-let [d (get-kv db)] (d/visit-list d dbi-name visitor k kt vt)))
  ([db dbi-name visitor k kt vt raw-pred?]
   (when-let [d (get-kv db)]
     (d/visit-list d dbi-name visitor k kt vt raw-pred?))))

(defn list-count
  [db dbi-name k kt]
  (when-let [d (get-kv db)] (d/list-count d dbi-name k kt)))

(defn in-list?
  [db dbi-name k v kt vt]
  (when-let [d (get-kv db)] (d/in-list? d dbi-name k v kt vt)))

(defn list-range
  [db dbi-name k-range kt v-range vt]
  (when-let [d (get-kv db)]
    (into [] (d/list-range d dbi-name k-range kt v-range vt))))

(defn list-range-count
  [db dbi-name k-range kt]
  (when-let [d (get-kv db)]
    (d/list-range-count d dbi-name k-range kt)))

(defn list-range-first
  [db dbi-name k-range kt v-range vt]
  (when-let [d (get-kv db)]
    (d/list-range-first d dbi-name k-range kt v-range vt)))

(defn list-range-first-n
  [db dbi-name n k-range kt v-range vt]
  (when-let [d (get-kv db)]
    (into [] (d/list-range-first-n d dbi-name n k-range kt v-range vt))))

(defn list-range-filter
  ([db dbi-name pred k-range kt v-range vt]
   (when-let [d (get-kv db)]
     (into [] (d/list-range-filter d dbi-name pred k-range kt v-range vt))))
  ([db dbi-name pred k-range kt v-range vt raw-pred?]
   (when-let [d (get-kv db)]
     (into
       []
       (d/list-range-filter d dbi-name pred k-range kt v-range vt raw-pred?)))))

(defn list-range-keep
  ([db dbi-name pred k-range kt v-range vt]
   (when-let [d (get-kv db)]
     (into [] (d/list-range-keep d dbi-name pred k-range kt v-range vt))))
  ([db dbi-name pred k-range kt v-range vt raw-pred?]
   (when-let [d (get-kv db)]
     (into
       []
       (d/list-range-keep d dbi-name pred k-range kt v-range vt raw-pred?)))))

(defn list-range-some
  ([db dbi-name pred k-range kt v-range vt]
   (when-let [d (get-kv db)]
     (d/list-range-some d dbi-name pred k-range kt v-range vt)))
  ([db dbi-name pred k-range kt v-range vt raw-pred?]
   (when-let [d (get-kv db)]
     (d/list-range-some d dbi-name pred k-range kt v-range vt raw-pred?))))

(defn list-range-filter-count
  ([this dbi-name pred k-range kt v-range vt]
   (when-let [d (get-kv db)]
     (d/list-range-filter-count d dbi-name pred k-range kt v-range vt)))
  ([this dbi-name pred k-range kt v-range vt raw-pred?]
   (when-let [d (get-kv db)]
     (d/list-range-filter-count d dbi-name pred k-range kt v-range vt
                                raw-pred?))))

(defn visit-list-range
  ([this dbi-name visitor k-range kt v-range vt]
   (when-let [d (get-kv db)]
     (d/visit-list-range d dbi-name visitor k-range kt v-range vt)))
  ([this dbi-name visitor k-range kt v-range vt raw-pred?]
   (when-let [d (get-kv db)]
     (d/visit-list-range d dbi-name visitor k-range kt v-range vt raw-pred?))))

(defn new-search-engine
  ([db]
   (new-search-engine db nil))
  ([db opts]
   (when-let [d (get-kv db)]
     (let [engine (d/new-search-engine d opts)
           id     (UUID/randomUUID)]
       (swap! engines assoc id engine)
       {::engine id}))))

(defn search-index-writer
  ([db]
   (search-index-writer db nil))
  ([db opts]
   (when-let [d (get-kv db)]
     (let [writer (d/search-index-writer d opts)
           id     (UUID/randomUUID)]
       (swap! search-writers assoc id writer)
       {::search-writer id}))))

(defn write
  [writer doc-ref doc-text]
  (when-let [w (get-search-writer writer)]
    (d/write w doc-ref doc-text)))

(defn commit
  [writer]
  (when-let [w (get-search-writer writer)]
    (d/commit w)))

(defn add-doc
  ([engine doc-ref doc-text]
   (add-doc engine doc-ref doc-text true))
  ([engine doc-ref doc-text check-exist?]
   (when-let [e (get-engine engine)]
     (d/add-doc e doc-ref doc-text check-exist?))))

(defn remove-doc
  [engine doc-ref]
  (when-let [e (get-engine engine)] (d/remove-doc e doc-ref)))

(defn clear-docs
  [engine]
  (when-let [e (get-engine engine)] (d/clear-docs e)))

(defn doc-indexed?
  [engine doc-ref]
  (when-let [e (get-engine engine)] (d/doc-indexed? e doc-ref)))

(defn doc-count
  [engine]
  (when-let [e (get-engine engine)] (d/doc-count e)))

(defn search
  ([engine query] (search engine query {}))
  ([engine query opts]
   (when-let [e (get-engine engine)] (d/search e query opts))))

(defn new-vector-index
  ([db]
   (new-vector-index db nil))
  ([db opts]
   (when-let [d (get-kv db)]
     (let [index (d/new-vector-index d opts)
           id    (UUID/randomUUID)]
       (swap! indices assoc id index)
       {::index id}))))

(defn add-vec
  [index vec-ref vec-data]
  (when-let [i (get-index index)] (d/add-vec i vec-ref vec-data)))

(defn remove-vec
  [index vec-ref]
  (when-let [i (get-index index)] (d/remove-vec i vec-ref)))

(defn close-vector-index
  [index]
  (when-let [i (get-index index)] (d/close-vector-index i)))

(defn clear-vector-index
  [index]
  (when-let [i (get-index index)] (d/clear-vector-index i)))

(defn vector-index-info
  [index]
  (when-let [i (get-index index)] (d/vector-index-info i)))

(defn force-vec-checkpoint!
  [index]
  (when-let [i (get-index index)] (d/force-vec-checkpoint! i)))

(defn vector-checkpoint-state
  [index]
  (when-let [i (get-index index)] (d/vector-checkpoint-state i)))

(defn search-vec
  ([index query]
   (when-let [i (get-index index)] (d/search-vec i query)))
  ([index query opts]
   (when-let [i (get-index index)] (d/search-vec i query opts))))

(defn re-index
  ([db opts] (re-index db {} opts))
  ([db schema opts]
   (when-let [e (or (get-cn db) (get-kv db) (get-engine db) (get-index db))]
     (let [e1 (d/re-index e schema opts)]
       (cond
         (d/conn? e1)                (do (swap! dl-conns assoc db e1)
                                         {::conn db})
         (instance? SearchEngine e1) (do (swap! engines assoc db e1)
                                         {::engine db})
         (instance? VectorIndex e1)  (do (swap! indices assoc db e1)
                                         {::index db})
         :else                       (do (swap! kv-dbs assoc db e1)
                                         {::kv-db db}))))))

(defn new-embedding-provider
  ([provider-spec]
   (new-embedding-provider provider-spec nil))
  ([provider-spec opts]
   (let [provider (d/new-embedding-provider provider-spec opts)
         id       (UUID/randomUUID)]
     (swap! embedding-providers assoc id provider)
     {::embedding-provider id})))

(defn embedding-metadata
  [provider]
  (when-let [p (get-embedding-provider provider)]
    (d/embedding-metadata p)))

(defn embedding-dimensions
  [provider]
  (when-let [p (get-embedding-provider provider)]
    (d/embedding-dimensions p)))

(defn embed-text
  ([provider text]
   (embed-text provider text nil))
  ([provider text opts]
   (when-let [p (get-embedding-provider provider)]
     (vec (d/embed-text p text opts)))))

(defn embed-texts
  ([provider texts]
   (embed-texts provider texts nil))
  ([provider texts opts]
   (when-let [p (get-embedding-provider provider)]
     (embedding-result (d/embed-texts p texts opts)))))

(defn token-count
  ([provider item]
   (token-count provider item nil))
  ([provider item opts]
   (when-let [p (get-embedding-provider provider)]
     (d/token-count p item opts))))

(defn token-counts
  ([provider items]
   (token-counts provider items nil))
  ([provider items opts]
   (when-let [p (get-embedding-provider provider)]
     (d/token-counts p items opts))))

(defn truncate-item
  ([provider item max-tokens]
   (truncate-item provider item max-tokens nil))
  ([provider item max-tokens opts]
   (when-let [p (get-embedding-provider provider)]
     (d/truncate-item p item max-tokens opts))))

(defn truncate-text
  ([provider text max-tokens]
   (truncate-text provider text max-tokens nil))
  ([provider text max-tokens opts]
   (when-let [p (get-embedding-provider provider)]
     (d/truncate-text p text max-tokens opts))))

(defn close-embedding-provider
  [provider]
  (let [[old _] (swap-vals! embedding-providers dissoc (::embedding-provider provider))]
    (when-let [p (get old (::embedding-provider provider))]
      (d/close-embedding-provider p))))

(defn new-llm-provider
  ([provider-spec]
   (new-llm-provider provider-spec nil))
  ([provider-spec opts]
   (let [provider (d/new-llm-provider provider-spec opts)
         id       (UUID/randomUUID)]
     (swap! llm-providers assoc id provider)
     {::llm-provider id})))

(defn llm-metadata
  [provider]
  (when-let [p (get-llm-provider provider)]
    (d/llm-metadata p)))

(defn llm-context-size
  [provider]
  (when-let [p (get-llm-provider provider)]
    (d/llm-context-size p)))

(defn generate-text
  ([provider prompt max-tokens]
   (generate-text provider prompt max-tokens nil))
  ([provider prompt max-tokens opts]
   (when-let [p (get-llm-provider provider)]
     (d/generate-text p prompt max-tokens opts))))

(defn summarize-text
  ([provider text max-tokens]
   (summarize-text provider text max-tokens nil))
  ([provider text max-tokens opts]
   (when-let [p (get-llm-provider provider)]
     (d/summarize-text p text max-tokens opts))))

(defn llm-token-count
  ([provider text]
   (llm-token-count provider text nil))
  ([provider text opts]
   (when-let [p (get-llm-provider provider)]
     (d/llm-token-count p text opts))))

(defn close-llm-provider
  [provider]
  (let [[old _] (swap-vals! llm-providers dissoc (::llm-provider provider))]
    (when-let [p (get old (::llm-provider provider))]
      (d/close-llm-provider p))))

(defn read-csv
  [input & opts]
  (mapv vec (apply csv/read-csv input opts)))

(defn write-csv
  [writer data & opts]
  (if (nil? writer)
    (let [w (java.io.StringWriter.)]
      (apply csv/write-csv w data opts)
      (str w))
    (apply csv/write-csv writer data opts)))

(defmacro with-conn
  [spec & body]
  `(let [r#      (list ~@(rest spec))
         dir#    (first r#)
         schema# (second r#)
         opts#   (second (rest r#))
         conn#   (get-conn dir# schema# opts#)]
     (try
       (let [~(first spec) conn#] ~@body)
       (finally (close conn#)))))

(defmacro with-kv
  [spec & body]
  `(let [r#    (list ~@(rest spec))
         dir#  (first r#)
         opts# (second r#)
         db#   (open-kv dir# opts#)]
     (try
       (let [~(first spec) db#] ~@body)
       (finally (close-kv db#)))))

(defn- apply-resource-fn
  [f resource]
  (if (instance? java.util.function.Function f)
    (.apply ^java.util.function.Function f resource)
    (f resource)))

(defn with-transaction-kv-fn
  ([kv f]
   (let [tx-kv (open-transact-kv kv)]
     (try
       (let [res (apply-resource-fn f tx-kv)]
         (close-transact-kv kv)
         res)
       (catch Throwable t
         (try
           (abort-transact-kv kv)
           (catch Throwable abort-error
             (.addSuppressed t abort-error)))
         (throw t)))))
  ([kv _timeout-ms f]
   (with-transaction-kv-fn kv f)))

(defn with-transaction-fn
  ([cn f]
   (let [tx-cn (open-transact cn)]
     (try
       (let [res (apply-resource-fn f tx-cn)]
         (close-transact cn)
         res)
       (catch Throwable t
         (try
           (abort-transact cn)
           (catch Throwable abort-error
             (.addSuppressed t abort-error)))
         (throw t)))))
  ([cn _timeout-ms f]
   (with-transaction-fn cn f)))

;; pods

(def ^:private exposed-vars
  {'pod-fn                    pod-fn
   'entid                     entid
   'entity                    entity
   'add                       add
   'retract                   retract
   'entity-db                 entity-db
   'touch                     touch
   'pull                      pull
   'pull-many                 pull-many
   'datom                     datom
   'datom?                    datom?
   'datom-e                   datom-e
   'datom-a                   datom-a
   'datom-v                   datom-v
   'empty-db                  empty-db
   'db?                       db?
   'init-db                   init-db
   'fill-db                   fill-db
   'close-db                  close-db
   'datoms                    datoms
   'search-datoms             search-datoms
   'count-datoms              count-datoms
   'cardinality               cardinality
   'max-eid                   max-eid
   'analyze                   analyze
   'seek-datoms               seek-datoms
   'fulltext-datoms           fulltext-datoms
   'rseek-datoms              rseek-datoms
   'index-range               index-range
   'conn?                     conn?
   'conn-from-db              conn-from-db
   'conn-from-datoms          conn-from-datoms
   'create-conn               create-conn
   'close                     close
   'datalog-index-cache-limit datalog-index-cache-limit
   'closed?                   closed?
   'transact!                 transact!
   'transact                  transact
   'transact-async*           transact-async*
   'with                      with
   'db-with                   db-with
   'tx-data->simulated-report tx-data->simulated-report
   'reset-conn!               reset-conn!
   'listen!                   listen!
   'unlisten!                 unlisten!
   'db                        db
   'opts                      opts
   'schema                    schema
   'update-schema             update-schema
   'secondary-index-status    secondary-index-status
   'process-secondary-index-jobs! process-secondary-index-jobs!
   'wait-for-secondary-index  wait-for-secondary-index
   'get-conn                  get-conn
   'clear                     clear
   'q                         q
   'explain                   explain
   'tempid                    tempid
   'resolve-tempid            resolve-tempid
   'squuid                    squuid
   'squuid-time-millis        squuid-time-millis
   'hexify-string             hexify-string
   'unhexify-string           unhexify-string
   'explicit-transaction-timeout explicit-transaction-timeout
   'set-explicit-transaction-timeout! set-explicit-transaction-timeout!
   'open-kv                   open-kv
   'datalog-kv                datalog-kv
   'k                         k
   'v                         v
   'put-buffer                put-buffer
   'read-buffer               read-buffer
   'close-kv                  close-kv
   'closed-kv?                closed-kv?
   'dir                       dir
   'open-dbi                  open-dbi
   'clear-dbi                 clear-dbi
   'drop-dbi                  drop-dbi
   'list-dbis                 list-dbis
   'copy                      copy
   'stat                      stat
   'entries                   entries
   'open-transact-kv          open-transact-kv
   'sync                      sync
   'txlog-watermarks          txlog-watermarks
   'open-tx-log               open-tx-log
   'create-snapshot!          create-snapshot!
   'list-snapshots            list-snapshots
   'gc-txlog-segments!        gc-txlog-segments!
   'set-env-flags             set-env-flags
   'get-env-flags             get-env-flags
   'close-transact-kv         close-transact-kv
   'abort-transact-kv         abort-transact-kv
   'begin-kv-transaction      begin-kv-transaction
   'commit-kv-transaction     commit-kv-transaction
   'abort-kv-transaction      abort-kv-transaction
   'open-transact             open-transact
   'close-transact            close-transact
   'abort-transact            abort-transact
   'transact-kv               transact-kv
   'transact-kv-async*        transact-kv-async*
   'get-value                 get-value
   'get-rank                  get-rank
   'get-by-rank               get-by-rank
   'sample-kv                 sample-kv
   'get-first                 get-first
   'get-first-n               get-first-n
   'get-range                 get-range
   'range-seq                 range-seq
   'key-range                 key-range
   'key-range-count           key-range-count
   'key-range-list-count      key-range-list-count
   'visit-key-range           visit-key-range
   'range-count               range-count
   'get-some                  get-some
   'range-filter              range-filter
   'range-keep                range-keep
   'range-some                range-some
   'range-filter-count        range-filter-count
   'visit                     visit
   'open-list-dbi             open-list-dbi
   'put-list-items            put-list-items
   'del-list-items            del-list-items
   'get-list                  get-list
   'visit-list                visit-list
   'list-count                list-count
   'in-list?                  in-list?
   'list-range                list-range
   'list-range-count          list-range-count
   'list-range-first          list-range-first
   'list-range-first-n        list-range-first-n
   'list-range-filter         list-range-filter
   'list-range-keep           list-range-keep
   'list-range-some           list-range-some
   'list-range-filter-count   list-range-filter-count
   'visit-list-range          visit-list-range
   'new-search-engine         new-search-engine
   'search-index-writer       search-index-writer
   'write                     write
   'commit                    commit
   'add-doc                   add-doc
   'remove-doc                remove-doc
   'clear-docs                clear-docs
   'doc-indexed?              doc-indexed?
   'doc-count                 doc-count
   'search                    search
   'new-vector-index          new-vector-index
   'add-vec                   add-vec
   'remove-vec                remove-vec
   'clear-vector-index        clear-vector-index
   'close-vector-index        close-vector-index
   'vector-index-info         vector-index-info
   'force-vec-checkpoint!     force-vec-checkpoint!
   'vector-checkpoint-state   vector-checkpoint-state
   'search-vec                search-vec
   're-index                  re-index
   'new-embedding-provider    new-embedding-provider
   'embedding-metadata        embedding-metadata
   'embedding-dimensions      embedding-dimensions
   'embed-text                embed-text
   'embed-texts               embed-texts
   'token-count               token-count
   'token-counts              token-counts
   'truncate-item             truncate-item
   'truncate-text             truncate-text
   'close-embedding-provider  close-embedding-provider
   'new-llm-provider          new-llm-provider
   'llm-metadata              llm-metadata
   'llm-context-size          llm-context-size
   'generate-text             generate-text
   'summarize-text            summarize-text
   'llm-token-count           llm-token-count
   'close-llm-provider        close-llm-provider
   'read-csv                  read-csv
   'write-csv                 write-csv
   })

(def ^:private lookup
  (zipmap (map (fn [sym] (symbol pod-ns (name sym))) (keys exposed-vars))
          (vals exposed-vars)))

(defn- all-vars []
  (u/concatv
    (mapv (fn [k] {"name" (name k)}) (keys exposed-vars))
    [{"name" "defpodfn"
      "code"
      "(defmacro defpodfn
          [fn-name args & body]
          `(pod-fn '~fn-name
                  '~args
                  '~@body))"}
     {"name" "with-transaction-kv"
      "code"
      "(defmacro with-transaction-kv
          [binding & body]
          `(let [db# ~(second binding)]
            (try
              (let [res# (let [~(first binding) (open-transact-kv db#)]
                           (try
                             ~@body
                             (catch Exception ~'e
                               (if (:resized (ex-data ~'e))
                                 (do ~@body)
                                 (throw ~'e)))))]
                (close-transact-kv db#)
                res#)
              (catch Throwable t#
                (try
                  (abort-transact-kv db#)
                  (catch Throwable abort-error#
                    (.addSuppressed t# abort-error#)))
                (throw t#)))))"}
     {"name" "with-transaction"
      "code"
      "(defmacro with-transaction
          [binding & body]
          `(let [conn# ~(second binding)]
            (try
              (let [res# (let [~(first binding) (open-transact conn#)]
                           (try
                             ~@body
                             (catch Exception ~'e
                               (if (:resized (ex-data ~'e))
                                 (do ~@body)
                                 (throw ~'e)))))]
                (close-transact conn#)
                res#)
              (catch Throwable t#
                (try
                  (abort-transact conn#)
                  (catch Throwable abort-error#
                    (.addSuppressed t# abort-error#)))
                (throw t#)))))"}
     {"name" "with-conn"
      "code"
      "(defmacro with-conn
          [spec & body]
          `(let [r#      (list ~@(rest spec))
                 dir#    (first r#)
                 schema# (second r#)
                 opts#   (second (rest r#))
                 conn#   (get-conn dir# schema# opts#)]
             (try
               (let [~(first spec) conn#] ~@body)
               (finally (close conn#)))))"}
     {"name" "with-kv"
      "code"
      "(defmacro with-kv
          [spec & body]
          `(let [r#    (list ~@(rest spec))
                 dir#  (first r#)
                 opts# (second r#)
                 db#   (open-kv dir# opts#)]
             (try
               (let [~(first spec) db#] ~@body)
               (finally (close-kv db#)))))"}
     {"name" "with-transaction-fn"
      "code"
      "(defn with-transaction-fn
          ([conn f]
           (with-transaction [tx-conn conn]
             (if (instance? java.util.function.Function f)
               (.apply ^java.util.function.Function f tx-conn)
               (f tx-conn))))
          ([conn timeout-ms f]
           (with-transaction [tx-conn conn {:timeout-ms timeout-ms}]
             (if (instance? java.util.function.Function f)
               (.apply ^java.util.function.Function f tx-conn)
               (f tx-conn)))))"}
     {"name" "with-transaction-kv-fn"
      "code"
      "(defn with-transaction-kv-fn
          ([kv f]
           (with-transaction-kv [tx-kv kv]
             (if (instance? java.util.function.Function f)
               (.apply ^java.util.function.Function f tx-kv)
               (f tx-kv))))
          ([kv timeout-ms f]
           (with-transaction-kv [tx-kv kv {:timeout-ms timeout-ms}]
             (if (instance? java.util.function.Function f)
               (.apply ^java.util.function.Function f tx-kv)
               (f tx-kv)))))"}
     {"name" "transact-async"
      "code"
      "(defn transact-async
          [conn tx-data tx-meta callback]
          (babashka.pods/invoke
            \"pod.huahaiy.datalevin\"
            'pod.huahaiy.datalevin/transact-async*
            [conn tx-data tx-meta]
            {:handlers {:success (fn [res] (callback res))
                        :error   (fn [{:keys [:ex-message :ex-data]}]
                                    (binding [*out* *err*]
                                      (println \"ERROR:\" ex-message)))}})
          nil)"}
     {"name" "transact-kv-async"
      "code"
      "(defn transact-kv-async
          [db dbi-name txs k-type v-type callback]
          (babashka.pods/invoke
            \"pod.huahaiy.datalevin\"
            'pod.huahaiy.datalevin/transact-kv-async*
            [db dbi-name txs k-type v-type]
            {:handlers {:success (fn [res] (callback res))
                        :error   (fn [{:keys [:ex-message :ex-data]}]
                                    (binding [*out* *err*]
                                      (println \"ERROR:\" ex-message)))}})
          nil)"}]))

(defn run [& _]
  (loop []
    (let [message (try (read)
                       (catch java.io.EOFException _
                         ::EOF))]
      (when-not (identical? ::EOF message)
        (let [op (-> message (get "op") read-string keyword)
              id (or (some-> message (get "id") read-string) "unknown")]
          (case op
            :describe
            (do (write-response {"format"     "transit+json"
                                  "namespaces" [{"name" "pod.huahaiy.datalevin"
                                                 "vars" (all-vars)}]
                                  "id"         id
                                  "ops"        {"shutdown" {}}})
                (recur))
            :invoke
            (do (try
                  (let [var  (-> (get message "var")
                                 read-string
                                 symbol)
                        args (-> (get message "args")
                                 read-string
                                 p/read-transit-string)]
                    (debug "id" id "var" var "args" args)
                    (if-let [f (lookup var)]
                      (let [res   (apply f args)
                            value (p/write-transit-string res)
                            reply {"value"  value
                                   "id"     id
                                   "status" ["done"]}]
                        (write-response reply))
                      (throw (ex-info (str "Var not found: " var) {}))))
                  (catch Throwable e
                    (let [edata (ex-data e)
                          reply {"ex-message" (.getMessage e)
                                 "ex-data"    (p/write-transit-string
                                                (assoc edata
                                                       :type
                                                       (str (class e))))
                                 "id"         id
                                 "status"     ["done" "error"]}]
                      (when-not (:resized edata)
                        (binding [*out* *err*] (println e)))
                      (write-response reply))))
                (recur))
            :shutdown
            (do (doseq [conn (vals @dl-conns)] (d/close conn))
                (doseq [db (vals @kv-dbs)] (d/close-kv db))
                (doseq [provider (vals @embedding-providers)]
                  (d/close-embedding-provider provider))
                (doseq [provider (vals @llm-providers)]
                  (d/close-llm-provider provider))
                (System/exit 0))
            (do
              (write-response {"err" (str "unknown op:" (name op))})
              (recur))))))))

(defn -main [& _]
  (run))
