(require '[clojure.string :as str]
         '[datalevin.constants :as c]
         '[datalevin.core :as d]
         '[datalevin.datom :as dd]
         '[datalevin.db :as db]
         '[datalevin.interface :as i]
         '[taoensso.nippy :as nippy])

(import '[datalevin.datom Datom]
        '[java.io BufferedOutputStream DataOutputStream]
        '[java.util List])

(def max-kv-batch-bytes (* 4 1024 1024))
(def max-kv-batch-entries 4096)
(def stream-format :datalevin/mixed-migration-v1)

(defn backing-kv
  [conn]
  (if-let [datalog-kv (resolve 'datalevin.core/datalog-kv)]
    (datalog-kv conn)
    (let [db    @conn
          store (clojure.lang.Reflector/getInstanceField db "store")]
      (clojure.lang.Reflector/getInstanceField store "lmdb"))))

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
  [kv schema opts]
  ;; Keep this classification aligned with datalevin.dump/user-kv-dbis.
  (let [derived-dbis (datalog-derived-dbis schema opts)]
    (->> (d/list-dbis kv)
         (remove #(or (str/starts-with? % "datalevin/")
                      (contains? derived-dbis %)))
         sort
         (mapv (fn [dbi]
                 {:dbi     dbi
                  :entries (d/entries kv dbi)
                  :opts    (i/dbi-opts kv dbi)})))))

(defn raw-size
  [[k v]]
  (+ (alength ^bytes k) (alength ^bytes v)))

(defn write-kv-dbi
  [out kv {:keys [dbi entries] :as expected}]
  (nippy/freeze-to-out! out (assoc expected :frame :dbi))
  (let [written
        (with-open [items (d/range-seq kv dbi [:all] :raw :raw false
                                      {:batch-size 256})]
          (loop [items       (seq items)
                 batch       (transient [])
                 batch-bytes 0
                 batch-count 0
                 total       0]
            (if-some [items items]
              (let [item      (first items)
                    item-size (raw-size item)]
                (if (and (pos? batch-count)
                         (or (= batch-count max-kv-batch-entries)
                             (> (+ batch-bytes item-size)
                                max-kv-batch-bytes)))
                  (do
                    (nippy/freeze-to-out! out (persistent! batch))
                    (recur items (transient []) 0 0 total))
                  (recur (next items)
                         (conj! batch item)
                         (+ batch-bytes item-size)
                         (inc batch-count)
                         (inc total))))
              (do
                (when (pos? batch-count)
                  (nippy/freeze-to-out! out (persistent! batch)))
                total))))]
    (when-not (= entries written)
      (throw
        (ex-info "Exported DBI entry count does not match source"
                 {:dbi dbi :source-count entries :dump-count written})))
    (nippy/freeze-to-out!
      out {:frame :dbi-end :dbi dbi :entry-count written})
    written))

(binding [*out* *err*]
  (let [[dir]        *command-line-args*
        entity-span  25000
        conn         (d/get-conn dir)
        datom-count  (volatile! 0)
        kv-count     (volatile! 0)]
    (try
      (let [db           (d/db conn)
            max-eid      (d/max-eid db)
            source-count (d/count-datoms db nil nil nil)
            opts         (d/opts conn)
            schema       (d/schema conn)
            kv           (backing-kv conn)
            kv-dbis      (user-kv-dbis kv schema opts)
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
          (loop [start-eid 0]
            (when (<= start-eid max-eid)
              (let [end-eid (min max-eid (+ start-eid (dec entity-span)))
                    ^List found
                    (db/-range-datoms
                      db :eav
                      (dd/datom start-eid nil c/v0)
                      (dd/datom end-eid nil c/vmax))
                    size  (.size found)
                    batch (persistent!
                            (loop [i 0, batch (transient [])]
                              (if (< i size)
                                (let [^Datom datom (.get found i)]
                                  (recur
                                    (inc i)
                                    (conj! batch
                                           [(.-e datom) (.-a datom)
                                            (.-v datom)])))
                                batch)))]
                (when (pos? size)
                  (nippy/freeze-to-out! out batch))
                (vswap! datom-count + size)
                (recur (inc end-eid)))))
          (when-not (= source-count @datom-count)
            (throw
              (ex-info "Exported datom count does not match source"
                       {:source-count source-count
                        :dump-count   @datom-count})))
          (nippy/freeze-to-out!
            out {:frame :datalog-end :datom-count @datom-count})
          (doseq [dbi kv-dbis]
            (vswap! kv-count + (write-kv-dbi out kv dbi)))
          (when-not (= kv-source-count @kv-count)
            (throw
              (ex-info "Exported KV entry count does not match source"
                       {:source-count kv-source-count
                        :dump-count   @kv-count})))
          (nippy/freeze-to-out!
            out {:frame :end :entry-count @kv-count})))
      (finally
        (d/close conn)))))
