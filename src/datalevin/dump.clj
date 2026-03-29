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
   [clojure.pprint :as p]
   [clojure.edn :as edn]
   [taoensso.nippy :as nippy]
   [datalevin.util :as u]
   [datalevin.conn :as conn]
   [datalevin.db :as db]
   [datalevin.datom :as dd]
   [datalevin.storage :as s]
   [datalevin.interface :as i :refer [dir]])
  (:import
   [datalevin.db DB]
   [datalevin.datom Datom]
   [datalevin.storage Store]
   [datalevin.remote DatalogStore]
   [java.io PushbackReader FileOutputStream FileInputStream DataOutputStream
    DataInputStream IOException]))

(defn dump-datalog
  ([conn]
   (binding [u/*datalevin-print* true]
     (p/pprint (conn/opts conn))
     (p/pprint (conn/schema conn))
     (doseq [^Datom datom (db/-datoms @conn :eav nil nil nil)]
       (prn [(.-e datom) (.-a datom) (.-v datom)]))))
  ([conn data-output]
   (if data-output
     (nippy/freeze-to-out!
       data-output
       [(conn/opts conn)
        (conn/schema conn)
        (map (fn [^Datom datom] [(.-e datom) (.-a datom) (.-v datom)])
             (db/-datoms @conn :eav nil nil nil))])
     (dump-datalog conn))))

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
       (let [[old-opts old-schema datoms] (nippy/thaw-from-in! in)
             old-opts                     (normalize-legacy-ha-nil-sentinels
                                            old-opts)
             new-opts                     (merge old-opts opts)
             new-schema                   (merge old-schema schema)
             db                           (db/init-db
                                            (for [d datoms]
                                              (apply dd/datom d))
                                            dir new-schema new-opts)]
         (db/close-db db))
       (catch Exception e
         (u/raise "Error loading nippy file into Datalog DB: " e {})))
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
                                        (map #(apply dd/datom %)))
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
