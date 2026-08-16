;;
;; Copyright (c) Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns ^:no-doc datalevin.dump
  "dump, load and re-index database"
  (:refer-clojure :exclude [load sync])
  (:require
   [clojure.string :as str]
   [clojure.pprint :as p]
   [clojure.edn :as edn]
   [datalevin.hako-codec :as codec]
   [datalevin.util :as u]
   [datalevin.constants :as c]
   [datalevin.conn :as conn]
   [datalevin.db :as db]
   [datalevin.datom :as dd]
   [datalevin.lmdb :as l]
   [datalevin.interface :as i :refer [dir]])
  (:import
   [datalevin.db DB]
   [datalevin.datom Datom]
   [datalevin.storage Store]
   [datalevin.remote DatalogStore]
   [java.io PushbackReader FileOutputStream FileInputStream DataOutputStream
    DataInputStream IOException]))

(def ^:private mixed-dump-format-key
  :datalevin/dump-format)

(def ^:private mixed-dump-format
  :datalevin/mixed-v1)

(def ^:private section-key
  :datalevin.dump/section)

(def ^:private section-end-key
  :datalevin.dump/section-end)

(defn- mixed-dump-header?
  [form]
  (= mixed-dump-format (get form mixed-dump-format-key)))

(defn- kv-dump-header?
  [form]
  (and (map? form) (contains? form :dbi) (contains? form :entries)))

(defn- datalevin-internal-dbi?
  [dbi]
  (str/starts-with? dbi "datalevin/"))

(defn- attr-domain
  [attr]
  (str/replace (u/keyword->string attr) "/" "_"))

(defn- fulltext-attr-domains
  [attr props]
  (when (:db/fulltext props)
    (vec
     (distinct
      (cond-> (vec (or (seq (:db.fulltext/domains props))
                       [c/default-domain]))
        (:db.fulltext/autoDomain props) (conj (u/keyword->string attr)))))))

(defn- vector-attr-domains
  [attr props]
  (when (identical? :db.type/vec (:db/valueType props))
    (vec (distinct (conj (vec (:db.vec/domains props)) (attr-domain attr))))))

(defn- embedding-attr-domains
  [attr props]
  (when (:db/embedding props)
    (vec
     (distinct
      (cond-> (vec (or (seq (:db.embedding/domains props))
                       [c/default-domain]))
        (:db.embedding/autoDomain props) (conj (attr-domain attr)))))))

(defn- idoc-attr-domain
  [attr props]
  (when (identical? :db.type/idoc (:db/valueType props))
    (or (:db/domain props) (u/keyword->string attr))))

(defn- domains-from-schema
  [schema f]
  (reduce-kv
   (fn [domains attr props]
     (let [v (f attr props)]
       (cond
         (nil? v) domains
         (coll? v) (into domains v)
         :else (conj domains v))))
   #{}
   schema))

(defn- fulltext-dbis
  [domain]
  [(str domain "/" c/terms)
   (str domain "/" c/docs)
   (str domain "/" c/positions)
   (str domain "/" c/rawtext)])

(defn- vector-dbis
  [domain]
  [(str domain "/" c/vec-refs)])

(defn- idoc-dbis
  [domain]
  [(str domain "/" c/idoc-doc-ref)
   (str domain "/" c/idoc-doc-index)
   (str domain "/" c/idoc-path-dict)])

(defn- embedding-index-domain
  [domain]
  (str "__embedding__/" domain))

(defn- datalog-derived-dbis
  [schema opts]
  (let [search-domains    (into (set (keys (:search-domains opts)))
                                (domains-from-schema schema fulltext-attr-domains))
        vector-domains    (into (set (keys (:vector-domains opts)))
                                (domains-from-schema schema vector-attr-domains))
        embedding-domains (into (set (keys (:embedding-domains opts)))
                                (domains-from-schema schema embedding-attr-domains))
        idoc-domains      (domains-from-schema schema idoc-attr-domain)]
    (set
     (concat
      (mapcat fulltext-dbis search-domains)
      (mapcat vector-dbis vector-domains)
      (mapcat #(vector-dbis (embedding-index-domain %)) embedding-domains)
      (mapcat idoc-dbis idoc-domains)))))

(defn- user-kv-dbis
  [dbis schema opts]
  (let [derived-dbis (datalog-derived-dbis schema opts)]
    (sort (remove #(or (datalevin-internal-dbi? %)
                       (contains? derived-dbis %))
                  dbis))))

(defn- idoc-attr?
  [schema attr]
  (= :db.type/idoc (get-in schema [attr :db/valueType])))

(defn- idoc-dump-value
  [v]
  (cond
    (identical? v :json/null)
    nil

    (map? v)
    (reduce-kv
     (fn [m k v]
       (assoc m k (idoc-dump-value v)))
     {}
     v)

    (vector? v)
    (mapv idoc-dump-value v)

    :else
    v))

(defn- datom-dump-row
  [schema ^Datom datom]
  (let [attr (.-a datom)
        v    (.-v datom)]
    [(.-e datom) attr (if (idoc-attr? schema attr)
                        (idoc-dump-value v)
                        v)]))

(defn- load-datom
  [schema d]
  (let [[_ attr value] d]
    (apply dd/datom
           (if (idoc-attr? schema attr)
             (assoc (vec d) 2 (idoc-dump-value value))
             d))))

(defn dump-datalog
  ([conn]
   (binding [u/*datalevin-print* true]
     (let [schema (conn/schema conn)]
       (p/pprint (conn/opts conn))
       (p/pprint schema)
       (doseq [^Datom datom (db/-datoms @conn :eav nil nil nil)]
         (prn (datom-dump-row schema datom))))))
  ([conn data-output]
   (if data-output
     (let [schema (conn/schema conn)]
       (codec/freeze-to-out!
        data-output
        [(conn/opts conn)
         schema
         (map (fn [^Datom datom] (datom-dump-row schema datom))
              (db/-datoms @conn :eav nil nil nil))]))
     (dump-datalog conn))))

(defn- dump-datalog-section
  [conn]
  (binding [u/*datalevin-print* true]
    (let [schema (conn/schema conn)]
      (p/pprint {section-key :datalog})
      (p/pprint (conn/opts conn))
      (p/pprint schema)
      (doseq [^Datom datom (db/-datoms @conn :eav nil nil nil)]
        (prn (datom-dump-row schema datom)))
      (p/pprint {section-end-key :datalog}))))

(defn dump-auto
  ([src-dir]
   (dump-auto src-dir nil))
  ([src-dir data-output]
   (let [dbis     (let [lmdb (l/open-kv src-dir)]
                    (try
                      (set (i/list-dbis lmdb))
                      (finally
                        (i/close-kv lmdb))))
         datalog? (contains? dbis c/eav)]
     (if datalog?
       (do
         (when data-output
           (u/raise "Auto dump of mixed Datalog/KV stores is not supported "
                    "in nippy format; use text auto dump or explicit -g/-a."
                    {}))
         (p/pprint {mixed-dump-format-key mixed-dump-format
                    :version              1})
         (let [[schema opts]
               (let [conn (conn/create-conn src-dir)]
                 (try
                   (let [schema (conn/schema conn)
                         opts   (conn/opts conn)]
                     (dump-datalog-section conn)
                     [schema opts])
                   (finally
                     (conn/close conn))))]
           (let [lmdb (l/open-kv src-dir)]
             (try
               (doseq [dbi (user-kv-dbis dbis schema opts)]
                 (l/dump-dbi-section lmdb dbi))
               (finally
                 (i/close-kv lmdb))))))
       (let [lmdb (l/open-kv src-dir)]
         (try
           (l/dump-all lmdb data-output)
           (finally
             (i/close-kv lmdb))))))))

(def ^:private nippy-meta-protocol-key
  :taoensso.nippy/meta-protocol-key)

(def ^:private legacy-ha-nil-sentinel-keys
  [:ha-mode
   :ha-control-plane
   :ha-members
   :ha-fencing-hook
   :ha-clock-skew-hook
   :ha-membership-hash])

(defn- normalize-legacy-ha-nil-sentinels
  [opts]
  (reduce
   (fn [m k]
     (if (= nippy-meta-protocol-key (get m k))
       (assoc m k nil)
       m))
   (or opts {})
   legacy-ha-nil-sentinel-keys))

(defn- load-datalog-from-first*
  [dir read-form first-form schema opts stop?]
  (let [read-maps             #(if (:db/ident first-form)
                                 [nil first-form]
                                 [first-form (read-form)])
        [old-opts old-schema] (read-maps)
        new-opts              (merge old-opts opts)
        new-schema            (merge old-schema schema)
        datoms                (->> (repeatedly read-form)
                                   (take-while #(and (not= ::EOF %)
                                                     (not (stop? %))))
                                   (map #(load-datom new-schema %)))
        db                    (db/init-db datoms dir new-schema new-opts)]
    (db/close-db db)))

(defn- load-datalog-from-first
  [dir read-form first-form schema opts]
  (load-datalog-from-first* dir read-form first-form schema opts (constantly false)))

(defn- load-datalog-section
  [dir read-form schema opts]
  (load-datalog-from-first*
   dir read-form (read-form) schema opts
   #(and (map? %) (= :datalog (get % section-end-key)))))

(defn- load-mixed
  [dir read-form schema opts]
  (loop [form (read-form)]
    (when-not (= ::EOF form)
      (cond
        (= :datalog (get form section-key))
        (do
          (load-datalog-section dir read-form schema opts)
          (recur (read-form)))

        (= :kv (get form section-key))
        (do
          (let [lmdb (l/open-kv dir)]
            (try
              (l/load-dbi-section lmdb form read-form)
              (finally
                (i/close-kv lmdb))))
          (recur (read-form)))

        :else
        (u/raise "Unexpected section in mixed dump" {:form form} {})))))

(defn load-auto
  [dir in schema opts]
  (try
    (with-open [^PushbackReader r in]
      (let [read-form  #(edn/read {:eof     ::EOF
                                   :readers *data-readers*} r)
            first-form (read-form)]
        (cond
          (= ::EOF first-form)
          nil

          (mixed-dump-header? first-form)
          (load-mixed dir read-form schema opts)

          (kv-dump-header? first-form)
          (let [lmdb (l/open-kv dir)]
            (try
              (l/load-all-forms
               lmdb
               (cons first-form
                     (take-while #(not= ::EOF %)
                                 (repeatedly read-form))))
              (finally
                (i/close-kv lmdb))))

          :else
          (load-datalog-from-first dir read-form first-form schema opts))))
    (catch IOException e
      (u/raise "IO error while loading auto-detected data: " e {}))
    (catch RuntimeException e
      (u/raise "Parse error while loading auto-detected data: " e {}))
    (catch Exception e
      (u/raise "Error loading auto-detected data: " e {}))))

(defn- dump
  [conn ^String dumpfile]
  (let [d (DataOutputStream. (FileOutputStream. dumpfile))]
    (dump-datalog conn d)
    (.flush d)
    (.close d)))

(defn load-datalog
  ([dir in schema opts nippy?]
   (if nippy?
     (try
       (let [[old-opts old-schema datoms] (codec/thaw-from-in! in)
             old-opts                     (normalize-legacy-ha-nil-sentinels
                                           old-opts)
             new-opts                     (merge old-opts opts)
             new-schema                   (merge old-schema schema)
             db                           (db/init-db
                                           (for [d datoms]
                                             (load-datom new-schema d))
                                           dir new-schema new-opts)]
         (db/close-db db))
       (catch Exception e
         (u/raise
          "Failed to load binary dump into Datalog DB. If this dump was
           produced by a pre-hako Datalevin, re-dump it with the old
           version to the EDN format (drop the `--nippy` flag on
           `dtlv dump`) and reload here."
          e {})))
     (load-datalog dir in schema opts)))
  ([dir in schema opts]
   (try
     (with-open [^PushbackReader r in]
       (let [read-form             #(edn/read {:eof     ::EOF
                                               :readers *data-readers*} r)
             read-maps             #(let [m1 (read-form)]
                                      (if (:db/ident m1)
                                        [nil m1]
                                        [m1 (read-form)]))
             [old-opts old-schema] (read-maps)
             new-opts              (merge old-opts opts)
             new-schema            (merge old-schema schema)
             datoms                (->> (repeatedly read-form)
                                        (take-while #(not= ::EOF %))
                                        (map #(load-datom new-schema %)))
             db                    (db/init-db datoms dir new-schema new-opts)]
         (db/close-db db)))
     (catch IOException e
       (u/raise "IO error while loading Datalog data: " e {}))
     (catch RuntimeException e
       (u/raise "Parse error while loading Datalog data: " e {}))
     (catch Exception e
       (u/raise "Error loading Datalog data: " e {})))))

(defn- load
  [dir schema opts ^String dumpfile]
  (let [f  (FileInputStream. dumpfile)
        in (DataInputStream. f)]
    (load-datalog dir in schema opts true)
    (.close f)))

(defn re-index-datalog
  [conn schema opts]
  (let [d (dir (.-store ^DB @conn))]
    (try
      (let [dumpfile (str d u/+separator+ "dl-dump")]
        (dump conn dumpfile)
        (conn/clear conn)
        (load d schema opts dumpfile)
        (conn/create-conn d))
      (catch Exception e
        (u/raise "Unable to re-index Datalog database" e {:dir d})))))

(defn copy
  ([db dest]
   (copy db dest false))
  ([db dest compact?]
   (let [lmdb (if (instance? DB db)
                (.-lmdb ^Store (.-store ^DB db))
                db)]
     (i/copy lmdb dest compact?))))

(defn re-index
  ([db opts]
   (re-index db {} opts))
  ([db schema opts]
   (let [bk (when (:backup? opts)
              (u/tmp-dir (str "dtlv-re-index-" (System/currentTimeMillis))))]
     (if (conn/conn? db)
       (let [store (.-store ^DB @db)]
         (if (instance? DatalogStore store)
           (do (i/re-index store schema opts) db)
           (do (when bk (copy @db bk true))
               (re-index-datalog db schema opts))))
       (i/re-index db opts)))))
