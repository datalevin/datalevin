(ns ycsb-bench.audit
  "Optional value auditing. Histories are retained during execution and checked
  after workers stop. Instrumented timings are not performance baselines."
  (:require [ycsb-bench.store :as store]
            [ycsb-bench.workload :as w])
  (:import [java.util ArrayList]
           [java.util.concurrent ConcurrentLinkedQueue]))

(set! *warn-on-reflection* true)

(defn history
  "Give each calling thread its own log; recording does not order workers."
  []
  (let [logs (ConcurrentLinkedQueue.)]
    {:logs logs
     :local (proxy [ThreadLocal] []
              (initialValue []
                (let [log (ArrayList.)]
                  (.add logs log)
                  log)))}))

(defn- record! [{:keys [^ThreadLocal local]} event]
  (.add ^ArrayList (.get local) event))

(defrecord AuditedRecords [db history]
  store/Records
  (put-records! [_ records]
    (let [start (System/nanoTime)
          result (store/put-records! db records)
          end (System/nanoTime)]
      (record! history {:op :insert :start start :end end :records records})
      result))
  (read-record [_ id]
    (let [start (System/nanoTime)
          values (store/read-record db id)
          end (System/nanoTime)]
      (record! history {:op :read :start start :end end :id id :values values})
      values))
  (update-field! [_ id field value]
    (let [start (System/nanoTime)
          result (store/update-field! db id field value)
          end (System/nanoTime)]
      (record! history {:op :update :start start :end end
                        :id id :field field :value value})
      result))
  (scan-records [_ start-key n]
    (let [start (System/nanoTime)
          rows (store/scan-records db start-key n)
          end (System/nanoTime)]
      (record! history {:op :scan :start start :end end :rows rows})
      rows))
  (record-count [_] (store/record-count db))
  (storage-info [_] (store/storage-info db))
  (close-store! [_] (store/close-store! db))
  store/WorkerStores
  (worker-store [_ worker]
    (->AuditedRecords (store/for-worker db worker) history)))

(defn events
  "Read only after all workers have joined. Returned payloads are immutable."
  [{:keys [^ConcurrentLinkedQueue logs]}]
  (vec (mapcat seq logs)))

(defn- add-record [versions id values start end]
  (reduce-kv (fn [versions field value]
               (update-in versions [id field] (fnil conj []) [start end value]))
             versions (vec values)))

(defn- field-index [versions]
  (let [ordered (sort-by second versions)]
    {:ends (long-array (map second ordered))
     :starts (long-array (rest (reductions max Long/MIN_VALUE (map first ordered))))
     :by-value (group-by #(nth % 2) versions)}))

(defn prepare
  "Index initial values and acknowledged writes by key and field. Intervals,
  rather than response order, determine which values may remain visible."
  [events {:keys [records seed] :as opts} key-fn]
  (let [initial (reduce (fn [versions ordinal]
                         (add-record versions (key-fn ordinal)
                                     (w/initial-values seed ordinal opts)
                                     Long/MIN_VALUE Long/MIN_VALUE))
                       {} (range records))
        versions
        (reduce (fn [versions {:keys [op id field value records start end]}]
                  (case op
                    :insert (reduce (fn [versions [id values]]
                                      (add-record versions id values start end))
                                    versions records)
                    :update (update-in versions [id field] (fnil conj []) [start end value])
                    versions))
                initial events)]
    (update-vals versions #(update-vals % field-index))))

(defn- preceding-start
  "Maximum invocation time of writes completed strictly before this read.
  Equal timestamps are conservatively treated as overlapping."
  ^long [{:keys [^longs ends ^longs starts]} ^long read-start]
  (loop [lo 0, hi (alength ends)]
    (if (< lo hi)
      (let [mid (unsigned-bit-shift-right (+ lo hi) 1)]
        (if (< (aget ends mid) read-start)
          (recur (inc mid) hi)
          (recur lo mid)))
      (if (zero? lo) Long/MIN_VALUE (aget starts (dec lo))))))

(defn check-values!
  "Reject values absent from the key/field history, from future writes, or
  superseded by a nonoverlapping write before the read. This checks necessary
  per-field real-time constraints, not whole-history linearizability."
  [index id values start end]
  (doseq [[field value] (map-indexed vector values)]
    (let [versions (get-in index [id field])
          threshold (when versions (preceding-start versions start))]
      (when-not (and versions
                     (some (fn [[write-start write-end _]]
                             (and (<= (long write-start) (long end))
                                  (>= (long write-end) (long threshold))))
                           (get-in versions [:by-value value])))
        (throw (ex-info "Value does not match the record's write history"
                        {:id id :field field :actual value :start start :end end}))))))

(defn check-observations!
  "Compare every captured read and scan payload outside measured phases."
  [index events check-record!]
  (doseq [{:keys [op id values rows start end]} events]
    (case op
      :read (do (check-record! values) (check-values! index id values start end))
      :scan (doseq [[id values] rows]
              (check-record! values)
              (check-values! index id values start end))
      nil))
  {:status :passed :model :per-field-real-time-constraints
   :point-reads (count (filter #(= :read (:op %)) events))
   :scan-rows (reduce + 0 (keep #(when (= :scan (:op %)) (count (:rows %))) events))
   :write-calls (count (filter #(#{:insert :update} (:op %)) events))
   :comparisons :post-measurement
   :linearizability-checked? false
   :overwritten-writes-verified? false})
