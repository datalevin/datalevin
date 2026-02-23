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

(defn- write [v]
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

(defn entid [dl eid] (when-let [d (get-db dl)] (d/entid d eid)))

(defn entity [{:keys [::db] :as dl} eid]
  (when-let [^DB d (get-db dl)]
    (let [^Entity e (d/touch (d/entity d eid))]
      (assoc @(.-cache e) :db/id (.-eid e) :db-name db))))

(defn touch [{:keys [db-name db/id]}]
  (when-let [d (get @dl-dbs db-name)]
    (let [^Entity e (d/touch (d/entity d id))]
      (assoc @(.-cache e) :db/id id :db-name db-name))))

(defn pull [dl selector eid]
  (when-let [d (get-db dl)]
    (d/pull d selector eid)))

(defn pull-many [dl selector eids]
  (when-let [d (get-db dl)]
    (d/pull-many d selector eids)))

(defn q [q & inputs]
  (apply d/q q (w/postwalk #(if (::db %) (get-db %) %) inputs)))

(defn explain [opts q & inputs]
  (apply d/explain opts q (w/postwalk #(if (::db %) (get-db %) %) inputs)))

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
   (init-db datoms nil nil))
  ([datoms dir]
   (init-db datoms dir nil))
  ([datoms dir schema]
   (let [db (d/init-db (map #(apply dd/datom %) datoms) dir schema)
         id (UUID/randomUUID)]
     (swap! dl-dbs assoc id db)
     {::db id})))

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

(defn- rp->res
  [rp]
  {:tx-data (:tx-data rp)
   :tempids (:tempids rp)
   :tx-meta (:tx-meta rp)})

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
                    (let [s (.-store ^DB @c)
                          d (db/new-db s)]
                      (swap! (if writing? wdl-dbs dl-dbs)
                             assoc conn d)
                      (swap! (if writing? wdl-conns dl-conns)
                             assoc conn (atom d :meta (meta c)))))
                  (throw e)))]
       (rp->res rp)))))

(defn db [{:keys [::conn] :as cn}]
  (when-let [c (get-cn cn)]
    (let [db (d/db c)]
      (if-let [i (some (fn [[i d]] (when (= db d) i)) @dl-dbs)]
        {::db i}
        (do (swap! dl-dbs assoc conn db)
            {::db conn})))))

(defn schema [cn] (when-let [c (get-cn cn)] (d/schema c)))

(defn update-schema
  [cn schema-update]
  (when-let [c (get-cn cn)] (d/update-schema c schema-update)))

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

(defn close-kv [db] (when-let [d (get-kv db)] (d/close-kv d)))

(defn closed-kv? [db] (when-let [d (get-kv db)] (d/closed-kv? d)))

(defn dir [db] (when-let [d (get-kv db)] (d/dir d)))

(defn open-dbi
  ([db dbi-name]
   (when-let [d (get-kv db)] (d/open-dbi d dbi-name) nil))
  ([db dbi-name opts]
   (when-let [d (get-kv db)] (d/open-dbi d dbi-name opts) nil)))

(defn clear-dbi [db] (when-let [d (get-kv db)] (d/clear-dbi d)))

(defn drop-dbi [db] (when-let [d (get-kv db)] (d/drop-dbi d)))

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

(defn open-transact [{:keys [::conn] :as cn}]
  (when-let [c (get @dl-conns conn)]
    (let [s  (.-store ^DB @c)
          l  (.-lmdb ^Store s)
          wl (if/open-transact-kv l)
          ws (st/transfer s wl)
          wd (db/new-db ws)]
      (swap! wdl-dbs assoc conn wd)
      (swap! wdl-conns assoc conn (atom wd :meta (meta c)))
      (assoc cn :writing? true))))

(defn close-transact [{:keys [::conn]}]
  (when-let [c (get @dl-conns conn)]
    (let [wc (get @wdl-conns conn)
          ws (.-store ^DB @wc)
          l  (.-lmdb ^Store (.-store ^DB @c))]
      (reset! c (db/new-db (st/transfer ws l)))
      (swap! wdl-dbs dissoc conn)
      (swap! wdl-conns dissoc conn)
      (if/close-transact-kv l))))

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
  [db dbi-name k-range kt v-range vt]
  (when-let [d (get-kv db)]
    (d/list-range-count d dbi-name k-range kt v-range vt)))

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

;; pods

(def ^:private exposed-vars
  {'pod-fn                    pod-fn
   'entid                     entid
   'entity                    entity
   'touch                     touch
   'pull                      pull
   'pull-many                 pull-many
   'empty-db                  empty-db
   'db?                       db?
   'init-db                   init-db
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
   'create-conn               create-conn
   'close                     close
   'datalog-index-cache-limit datalog-index-cache-limit
   'closed?                   closed?
   'transact!                 transact!
   'transact                  transact
   'transact-async*           transact-async*
   'db                        db
   'schema                    schema
   'update-schema             update-schema
   'get-conn                  get-conn
   'clear                     clear
   'q                         q
   'explain                   explain
   'open-kv                   open-kv
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
   'set-env-flags             set-env-flags
   'get-env-flags             get-env-flags
   'close-transact-kv         close-transact-kv
   'abort-transact-kv         abort-transact-kv
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
   'key-range                 key-range
   'key-range-count           key-range-count
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
   'search-vec                search-vec
   're-index                  re-index
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
              (let [~(first binding) (open-transact-kv db#)]
                (try
                  ~@body
                  (catch Exception ~'e
                    (if (:resized (ex-data ~'e))
                      (do ~@body)
                      (throw ~'e)))))
              (finally
                (close-transact-kv db#)))))"}
     {"name" "with-transaction"
      "code"
      "(defmacro with-transaction
          [binding & body]
          `(let [conn# ~(second binding)]
            (try
              (let [~(first binding) (open-transact conn#)]
                (try
                  ~@body
                  (catch Exception ~'e
                    (if (:resized (ex-data ~'e))
                      (do ~@body)
                      (throw ~'e)))))
              (finally (close-transact conn#)))))"}
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
            (do (write {"format"     "transit+json"
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
                        (write reply))
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
                      (write reply))))
                (recur))
            :shutdown
            (do (doseq [conn (vals @dl-conns)] (d/close conn))
                (doseq [db (vals @kv-dbs)] (d/close-kv db))
                (System/exit 0))
            (do
              (write {"err" (str "unknown op:" (name op))})
              (recur))))))))

(defn -main [& _]
  (run))
