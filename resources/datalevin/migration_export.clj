(require '[clojure.string :as str]
         '[datalevin.bits :as b]
         '[datalevin.core :as d]
         '[datalevin.db :as db]
         '[taoensso.nippy :as nippy])

(import '[datalevin.datom Datom]
        '[java.io BufferedOutputStream DataOutputStream]
        '[java.util List])

(def max-kv-batch-bytes (* 4 1024 1024))
(def max-kv-batch-entries 4096)
(def max-datom-batch-entries 25000)
(def stream-format :datalevin/mixed-migration-v1)
(def raw-key (resolve 'datalevin.lmdb/k))
(def raw-value (resolve 'datalevin.lmdb/v))

(defn dbi-options
  [kv dbi]
  (let [opts ((or (resolve 'datalevin.interface/dbi-opts)
                  (resolve 'datalevin.lmdb/dbi-opts))
              kv dbi)]
    (cond-> opts
      (:dupsort? opts) (update :flags (fnil conj #{}) :dupsort))))

(defn keyword->string
  [k]
  (if-let [n (namespace k)]
    (str n "/" (name k))
    (name k)))

(defn attr-domain
  [attr]
  (str/replace (keyword->string attr) "/" "_"))

(defn fulltext-attr-domains
  [attr props]
  (when (:db/fulltext props)
    (vec
      (distinct
        (cond-> (vec (or (seq (:db.fulltext/domains props)) ["datalevin"]))
          (:db.fulltext/autoDomain props) (conj (keyword->string attr)))))))

(defn vector-attr-domains
  [attr props]
  (when (identical? :db.type/vec (:db/valueType props))
    (vec (distinct (conj (vec (:db.vec/domains props)) (attr-domain attr))))))

(defn embedding-attr-domains
  [attr props]
  (when (:db/embedding props)
    (vec
      (distinct
        (cond-> (vec (or (seq (:db.embedding/domains props)) ["datalevin"]))
          (:db.embedding/autoDomain props) (conj (attr-domain attr)))))))

(defn idoc-attr-domain
  [attr props]
  (when (identical? :db.type/idoc (:db/valueType props))
    (or (:db/domain props) (keyword->string attr))))

(defn domains-from-schema
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

(defn datalog-derived-dbis
  [schema opts]
  (let [search-domains    (into (set (keys (:search-domains opts)))
                                (domains-from-schema schema
                                                     fulltext-attr-domains))
        vector-domains    (into (set (keys (:vector-domains opts)))
                                (domains-from-schema schema
                                                     vector-attr-domains))
        embedding-domains (into (set (keys (:embedding-domains opts)))
                                (domains-from-schema schema
                                                     embedding-attr-domains))
        idoc-domains      (domains-from-schema schema idoc-attr-domain)]
    (set
      (concat
        (mapcat #(map (fn [suffix] (str % "/" suffix))
                      ["terms" "docs" "positions" "rawtext"])
                search-domains)
        (map #(str % "/vec-refs") vector-domains)
        (map #(str "__embedding__/" % "/vec-refs") embedding-domains)
        (mapcat #(map (fn [suffix] (str % "/" suffix))
                      ["doc-ref" "doc-index" "path-dict"])
                idoc-domains)))))

(defn user-kv-dbis
  [dbis schema opts]
  ;; Keep this classification aligned with datalevin.dump/user-kv-dbis.
  (let [derived-dbis (datalog-derived-dbis schema opts)]
    (->> dbis
         (remove (fn [{:keys [dbi]}]
                   (or (str/starts-with? dbi "datalevin/")
                       (contains? derived-dbis dbi))))
         (sort-by :dbi)
         vec)))

(declare dupsort-dbi?)

(defn read-kv-dbis
  [dir]
  (let [kv (d/open-kv dir)]
    (try
      (mapv (fn [dbi]
              (let [opts (dbi-options kv dbi)]
                (if (dupsort-dbi? opts)
                  (d/open-list-dbi kv dbi opts)
                  (d/open-dbi kv dbi opts))
                {:dbi     dbi
                 :entries (d/entries kv dbi)
                 :opts    opts}))
            (d/list-dbis kv))
      (finally
        (d/close-kv kv)))))

(defn raw-size
  [[k v]]
  (+ (alength ^bytes k) (alength ^bytes v)))

(defn batch-writer
  [out]
  (let [batch (volatile! (transient []))
        state (long-array 3)
        flush! (fn []
                 (when (pos? (aget state 1))
                   (nippy/freeze-to-out! out (persistent! @batch))
                   (vreset! batch (transient []))
                   (aset state 0 0)
                   (aset state 1 0)))
        add! (fn [item]
               (let [item-size (raw-size item)]
                 (when (and (pos? (aget state 1))
                            (or (= (aget state 1) max-kv-batch-entries)
                                (> (+ (aget state 0) item-size)
                                   max-kv-batch-bytes)))
                   (flush!))
                 (vreset! batch (conj! @batch item))
                 (aset state 0 (+ (aget state 0) item-size))
                 (aset state 1 (inc (aget state 1)))
                 (aset state 2 (inc (aget state 2)))))
        finish! (fn []
                  (flush!)
                  (aget state 2))]
    [add! finish!]))

(defn dupsort-dbi?
  [{:keys [flags dupsort?]}]
  (or dupsort? (contains? flags :dupsort)))

(defn write-kv-dbi
  [out kv {:keys [dbi entries] :as expected}]
  (nippy/freeze-to-out! out (assoc expected :frame :dbi))
  (let [[add! finish!] (batch-writer out)
        opts           (:opts expected)
        dupsort?       (dupsort-dbi? opts)
        _              (if dupsort?
                         (d/open-list-dbi kv dbi opts)
                         (d/open-dbi kv dbi opts))
        written
        (if dupsort?
          (do
            (d/visit-list-range
              kv dbi
              (fn [item]
                (add! [(b/get-bytes (raw-key item))
                       (b/get-bytes (raw-value item))]))
              [:all] :raw [:all] :raw true)
            (finish!))
          (with-open [items (d/range-seq kv dbi [:all] :raw :raw false
                                        {:batch-size 256})]
            (doseq [item (seq items)]
              (add! item))
            (finish!)))]
    (when-not (= entries written)
      (throw
        (ex-info "Exported DBI entry count does not match source"
                 {:dbi dbi :source-count entries :dump-count written})))
    (nippy/freeze-to-out!
      out {:frame :dbi-end :dbi dbi :entry-count written})
    written))

(defn datom-row
  [^Datom datom]
  [(.-e datom) (.-a datom) (.-v datom)])

(defn write-datoms
  [out db]
  (loop [start-eid nil
         written   0]
    (let [^List found (db/-seek-datoms db :eav start-eid nil nil
                                       max-datom-batch-entries)
          size        (.size found)]
      (if (zero? size)
        written
        (let [last-eid     (.-e ^Datom (.get found (dec size)))
              suffix-size  (loop [i (dec size), n 0]
                             (if (and (not (neg? i))
                                      (= last-eid
                                         (.-e ^Datom (.get found i))))
                               (recur (dec i) (inc n))
                               n))
              ^List entity (db/-e-datoms db last-eid)
              entity-size  (.size entity)
              batch         (persistent!
                              (loop [i 0, batch (transient [])]
                                (if (< i size)
                                  (recur
                                    (inc i)
                                    (conj! batch
                                           (datom-row (.get found i))))
                                  (loop [i suffix-size, batch batch]
                                    (if (< i entity-size)
                                      (recur
                                        (inc i)
                                        (conj! batch
                                               (datom-row (.get entity i))))
                                      batch)))))
              batch-size    (count batch)]
          (when (> suffix-size entity-size)
            (throw
              (ex-info "EAV page contains more datoms than its last entity"
                       {:eid         last-eid
                        :page-count  suffix-size
                        :entity-count entity-size})))
          (nippy/freeze-to-out! out batch)
          (let [written (+ written batch-size)]
            (if (= size max-datom-batch-entries)
              (recur (inc last-eid) written)
              written)))))))

(binding [*out* *err*]
  (let [[dir]        *command-line-args*
        all-kv-dbis  (read-kv-dbis dir)
        conn         (volatile! (d/get-conn dir))
        datom-count  (volatile! 0)
        kv-count     (volatile! 0)]
    (try
      (let [db           (d/db @conn)
            source-count (d/count-datoms db nil nil nil)
            opts         (d/opts @conn)
            schema       (d/schema @conn)
            kv-dbis      (user-kv-dbis all-kv-dbis schema opts)
            kv-source-count (reduce + 0 (map :entries kv-dbis))]
        (d/datalog-index-cache-limit db 0)
        (with-open [out (DataOutputStream.
                          (BufferedOutputStream. System/out))]
          (nippy/freeze-to-out!
            out
            {:format          stream-format
             :opts            opts
             :schema          schema
             :source-count    source-count
             :kv-dbis         kv-dbis
             :kv-source-count kv-source-count})
          (vreset! datom-count (write-datoms out db))
          (when-not (= source-count @datom-count)
            (throw
              (ex-info "Exported datom count does not match source"
                       {:source-count source-count
                        :dump-count   @datom-count})))
          (nippy/freeze-to-out!
            out {:frame :datalog-end :datom-count @datom-count})
          (let [conn-to-close @conn]
            (vreset! conn nil)
            (d/close conn-to-close))
          (let [kv (d/open-kv dir)]
            (try
              (doseq [dbi kv-dbis]
                (vswap! kv-count + (write-kv-dbi out kv dbi)))
              (when-not (= kv-source-count @kv-count)
                (throw
                  (ex-info "Exported KV entry count does not match source"
                           {:source-count kv-source-count
                            :dump-count   @kv-count})))
              (nippy/freeze-to-out!
                out {:frame :end :entry-count @kv-count})
              (finally
                (d/close-kv kv))))))
      (finally
        (when-let [conn @conn]
          (d/close conn))))))
