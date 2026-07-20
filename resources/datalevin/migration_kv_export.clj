(require '[datalevin.constants :as c]
         '[datalevin.core :as d]
         '[datalevin.bits :as b]
         '[taoensso.nippy :as nippy])

(import '[java.io BufferedOutputStream DataOutputStream])

(def max-batch-bytes (* 4 1024 1024))
(def max-batch-entries 4096)
(def stream-format :datalevin/kv-migration-v1)
(def raw-key (resolve 'datalevin.lmdb/k))
(def raw-value (resolve 'datalevin.lmdb/v))

(defn environment-options
  [kv]
  ((or (resolve 'datalevin.interface/env-opts)
       (resolve 'datalevin.lmdb/opts))
   kv))

(defn dbi-options
  [kv dbi]
  (let [opts ((or (resolve 'datalevin.interface/dbi-opts)
                  (resolve 'datalevin.lmdb/dbi-opts))
              kv dbi)]
    (cond-> opts
      (:dupsort? opts) (update :flags (fnil conj #{}) :dupsort))))

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
                            (or (= (aget state 1) max-batch-entries)
                                (> (+ (aget state 0) item-size)
                                   max-batch-bytes)))
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

(defn write-dbi
  [out kv {:keys [dbi entries] :as expected}]
  (nippy/freeze-to-out!
    out (assoc expected :frame :dbi))
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

(binding [*out* *err*]
  (let [[dir] *command-line-args*
        kv    (d/open-kv dir)]
    (try
      (let [dbis (-> (d/list-dbis kv) set (disj c/kv-info) sort vec)
            dbis (mapv (fn [dbi]
                         (let [opts (dbi-options kv dbi)]
                           (if (dupsort-dbi? opts)
                             (d/open-list-dbi kv dbi opts)
                             (d/open-dbi kv dbi opts))
                           {:dbi     dbi
                            :entries (d/entries kv dbi)
                            :opts    opts}))
                       dbis)
            source-count (reduce + 0 (map :entries dbis))]
        (with-open [out (DataOutputStream.
                          (BufferedOutputStream. System/out))]
          (nippy/freeze-to-out!
            out {:format       stream-format
                 :opts         (environment-options kv)
                 :dbis         dbis
                 :source-count source-count})
          (let [dump-count
                (reduce
                  (fn [total dbi]
                    (+ total (write-dbi out kv dbi)))
                  0
                  dbis)]
            (when-not (= source-count dump-count)
              (throw
                (ex-info "Exported KV entry count does not match source"
                         {:source-count source-count
                          :dump-count   dump-count})))
            (nippy/freeze-to-out!
              out {:frame :end :entry-count dump-count}))))
      (finally
        (d/close-kv kv)))))
