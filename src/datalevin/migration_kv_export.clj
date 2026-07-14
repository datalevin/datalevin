(require '[datalevin.constants :as c]
         '[datalevin.core :as d]
         '[datalevin.interface :as i]
         '[taoensso.nippy :as nippy])

(import '[java.io BufferedOutputStream DataOutputStream])

(def max-batch-bytes (* 4 1024 1024))
(def max-batch-entries 4096)
(def stream-format :datalevin/kv-migration-v1)

(defn raw-size
  [[k v]]
  (+ (alength ^bytes k) (alength ^bytes v)))

(defn write-dbi
  [out kv {:keys [dbi entries] :as expected}]
  (nippy/freeze-to-out!
    out (assoc expected :frame :dbi))
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
                         (or (= batch-count max-batch-entries)
                             (> (+ batch-bytes item-size) max-batch-bytes)))
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
  (let [[dir] *command-line-args*
        kv    (d/open-kv dir)]
    (try
      (let [dbis (-> (d/list-dbis kv) set (disj c/kv-info) sort vec)
            dbis (mapv (fn [dbi]
                         {:dbi     dbi
                          :entries (d/entries kv dbi)
                          :opts    (i/dbi-opts kv dbi)})
                       dbis)
            source-count (reduce + 0 (map :entries dbis))]
        (with-open [out (DataOutputStream.
                          (BufferedOutputStream. System/out))]
          (nippy/freeze-to-out!
            out {:format       stream-format
                 :opts         (i/env-opts kv)
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
