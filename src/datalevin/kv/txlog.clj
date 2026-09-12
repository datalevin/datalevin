;;
;; Copyright (c) Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns ^:no-doc datalevin.kv.txlog
  "Txn-log record reading, segment caching, and hydration."
  (:require
   [datalevin.constants :as c]
   [datalevin.txlog :as txlog]
   [datalevin.util :refer [raise]])
  (:import
   [org.eclipse.collections.impl.list.mutable FastList]))

(defn ensure-fast-list
  ^FastList [x]
  (cond
    (instance? FastList x)
    x

    (instance? java.util.Collection x)
    (FastList. ^java.util.Collection x)

    (nil? x)
    (FastList.)

    :else
    (let [^FastList out (FastList.)]
      (doseq [v x]
        (.add out v))
      out)))

(defn rows-vector
  [rows]
  (cond
    (vector? rows)
    rows

    (nil? rows)
    []

    :else
    (vec rows)))

(defn- txlog-record-payload
  [record]
  (try
    (let [payload (txlog/decode-commit-row-payload ^bytes (:body record))]
      (when-not (map? payload)
        (raise "Malformed txn-log payload" {:record record}))
      payload)
    (catch Exception e
      (raise "Malformed txn-log payload"
             e
             {:type :txlog/corrupt
              :record record}))))

(defn- txlog-record-payload-header
  [record]
  (try
    (let [payload (txlog/decode-commit-row-payload-header
                   ^bytes (:body record))]
      (when-not (map? payload)
        (raise "Malformed txn-log payload" {:record record}))
      payload)
    (catch Exception e
      (raise "Malformed txn-log payload"
             e
             {:type :txlog/corrupt
              :record record}))))

(defn txlog-record-lsn
  [record]
  (let [payload (txlog-record-payload-header record)
        lsn (long (or (:lsn payload) 0))]
    (when-not (pos? lsn)
      (raise "Txn-log payload missing valid positive LSN"
             {:type :txlog/corrupt
              :record record}))
    lsn))

(defn- txlog-record-entry
  ([segment-id path record]
   (txlog-record-entry segment-id path record true))
  ([segment-id path record include-rows?]
   (let [payload (if include-rows?
                   (txlog-record-payload record)
                   (txlog-record-payload-header record))
        lsn (long (or (:lsn payload) 0))
        tx-time (long (or (:tx-time payload)
                          (:ts payload)
                          0))
        ha-term (some-> (:ha-term payload) long)
        rows (when include-rows?
               (rows-vector (:ops payload)))
        tx-kind (when include-rows?
                  (txlog/classify-record-kind rows))
        payload-bytes (long (or (:body-len record)
                                (some-> ^bytes (:body record) alength)
                                0))
        next-offset (long (or (:next-offset record)
                              (+ (long (:offset record))
                                 txlog/record-header-size
                                 payload-bytes)))]
     (when-not (pos? lsn)
       (raise "Txn-log payload missing valid positive LSN"
              {:type :txlog/corrupt
               :record record}))
     (cond-> {:lsn lsn
              :tx-time tx-time
              :segment-id (long segment-id)
              :offset (long (:offset record))
              :next-offset next-offset
              :checksum (long (:checksum record))
              :path path}
       include-rows?
       (assoc :tx-kind tx-kind
              :rows rows)

       (pos? payload-bytes)
       (assoc :payload-bytes payload-bytes)

       (some? ha-term)
       (assoc :ha-term ha-term)))))

(defn- txlog-segment-scan-bytes
  [state segment-id file-bytes]
  (let [active-segment-id (some-> state :segment-id deref long)
        active-segment-offset (some-> state :segment-offset deref long)]
    (if (and (some? active-segment-id)
             (= (long segment-id) active-segment-id)
             (some? active-segment-offset))
      (long (min (long file-bytes)
                 (long active-segment-offset)))
      (long file-bytes))))

(defn- txlog-segment-records
  [segment start-offset scan-bytes]
  (let [{:keys [id file]} segment
        segment-id (long id)
        path (.getPath ^java.io.File file)
        start-offset (long (max 0 (long (or start-offset 0))))
        scan-bytes (long (max start-offset (long (or scan-bytes 0))))
        acc (FastList.)]
    (txlog/scan-segment
     path
     {:allow-preallocated-tail? true
      :start-offset start-offset
      :max-offset scan-bytes
      :collect-records? false
      :on-record (fn [record]
                   (.add acc
                         (txlog-record-entry
                          segment-id path record false)))})
    (vec acc)))

(defn- txlog-records-cache-max-segments
  []
  (let [n (long c/*wal-records-cache-segments*)]
    (if (neg? n) 0 n)))

(defn- limit-txlog-records-cache-map
  [cache]
  (let [cache (or cache {})
        max-segments (long (txlog-records-cache-max-segments))]
    (cond
      (zero? max-segments)
      {}

      (<= (long (count cache)) max-segments)
      cache

      :else
      (let [keep-ids (into #{}
                           (take-last (int max-segments)
                                      (sort (keys cache))))]
        (reduce-kv (fn [acc sid entry]
                     (if (contains? keep-ids sid)
                       (assoc acc sid entry)
                       acc))
                   {}
                   cache)))))

(defn- txlog-segment-cache-valid?
  [entry path file-bytes modified-ms active-segment? active-offset]
  (and entry
       (= path (:path entry))
       (if (and active-segment? (some? active-offset))
         (= (long (min (long file-bytes)
                       (long active-offset)))
            (long (or (:scan-bytes entry) -1)))
         (and ;; Active-segment scans can race a stale runtime offset and cache a
              ;; partial record set for bytes that are already visible on disk.
              ;; Once that segment closes, do not trust the cached entry unless
              ;; it covered the full file at cache time.
          (= (long (or (:scan-bytes entry)
                       (:file-bytes entry)
                       -1))
             (long (:file-bytes entry)))
          (= file-bytes (long (:file-bytes entry)))
          (= modified-ms (long (:modified-ms entry)))))))

(defn- txlog-segment-cache-extendable?
  [entry path file-bytes active-segment? active-offset]
  (let [target-scan-bytes (when (and active-segment? (some? active-offset))
                            (long (min (long file-bytes)
                                       (long active-offset))))
        scan-bytes (some-> (:scan-bytes entry) long)
        records (:records entry)
        last-next-offset (some-> records peek :next-offset long)]
    (and entry
         active-segment?
         (some? target-scan-bytes)
         (= path (:path entry))
         (some? scan-bytes)
         (<= ^long scan-bytes ^long target-scan-bytes)
         (<= ^long scan-bytes ^long file-bytes)
         (or (zero? ^long scan-bytes)
             (= ^long scan-bytes ^long (or last-next-offset -1))))))

(defn- txlog-segment-cache-entry
  [state segment]
  (let [{:keys [id file]} segment
        path (.getPath ^java.io.File file)
        file-bytes (long (.length ^java.io.File file))
        modified-ms (long (.lastModified ^java.io.File file))
        scan-bytes (txlog-segment-scan-bytes state (long id) file-bytes)
        records (txlog-segment-records segment 0 scan-bytes)]
    {:segment-id (long id)
     :path path
     :file-bytes file-bytes
     :modified-ms modified-ms
     :scan-bytes scan-bytes
     :min-lsn (some-> records first :lsn long)
     :records records}))

(defn- txlog-extend-segment-cache-entry
  [state segment cached]
  (let [{:keys [id file]} segment
        segment-id (long id)
        path (.getPath ^java.io.File file)
        file-bytes (long (.length ^java.io.File file))
        modified-ms (long (.lastModified ^java.io.File file))
        scan-bytes (txlog-segment-scan-bytes state segment-id file-bytes)
        cached-scan-bytes (long (:scan-bytes cached))
        tail-records (if (< ^long cached-scan-bytes ^long scan-bytes)
                       (txlog-segment-records
                        segment cached-scan-bytes scan-bytes)
                       [])
        records (into (vec (:records cached)) tail-records)]
    {:segment-id segment-id
     :path path
     :file-bytes file-bytes
     :modified-ms modified-ms
     :scan-bytes scan-bytes
     :min-lsn (some-> records first :lsn long)
     :records records}))

(defn- txlog-segment-records-entry
  [state segment cache-v]
  (if cache-v
    (let [{:keys [id file]} segment
          segment-id (long id)
          path (.getPath ^java.io.File file)
          file-bytes (long (.length ^java.io.File file))
          modified-ms (long (.lastModified ^java.io.File file))
          active-segment-id (some-> state :segment-id deref long)
          active-segment-offset (some-> state :segment-offset deref long)
          active-segment? (and (some? active-segment-id)
                               (= segment-id active-segment-id))
          cache0 (or @cache-v {})
          cached (get cache0 segment-id)]
      (if (txlog-segment-cache-valid? cached
                                      path
                                      file-bytes
                                      modified-ms
                                      active-segment?
                                      active-segment-offset)
        cached
        (let [entry (if (txlog-segment-cache-extendable?
                         cached
                         path
                         file-bytes
                         active-segment?
                         active-segment-offset)
                      (txlog-extend-segment-cache-entry
                       state segment cached)
                      (txlog-segment-cache-entry state segment))]
          (vreset! cache-v
                   (limit-txlog-records-cache-map
                    (assoc cache0 segment-id entry)))
          entry)))
    (txlog-segment-cache-entry state segment)))

(defn- prune-txlog-records-cache!
  [cache-v segments]
  (when cache-v
    (let [cache0 (or @cache-v {})]
      (when (seq cache0)
        (let [segment-ids (into #{} (map (comp long :id)) segments)
             cache1 (reduce-kv
                      (fn [acc sid entry]
                        (if (contains? segment-ids sid)
                          (assoc acc sid entry)
                          acc))
                      {}
                      cache0)
              cache2 (limit-txlog-records-cache-map cache1)]
          (when (not= cache0 cache2)
            (vreset! cache-v cache2)))))))

(defn- txlog-record-lower-bound-index
  [records lsn]
  (let [records (if (vector? records) records (vec records))
        lsn (long lsn)]
    (loop [lo (long 0)
           hi (long (count records))]
      (if (< ^long lo ^long hi)
        (let [mid (long (quot (+ ^long lo ^long hi) 2))
              mid-lsn (long (:lsn (nth records mid)))]
          (if (< ^long mid-lsn ^long lsn)
            (recur (inc ^long mid) hi)
            (recur lo mid)))
        lo))))

(defn- txlog-records-validation-tail
  [records from]
  (let [records (if (vector? records) records (vec records))
        n (long (count records))]
    (if (zero? ^long n)
      records
      (let [idx (long (txlog-record-lower-bound-index records from))
            start (long (if (pos? ^long idx) (dec ^long idx) 0))]
        (subvec records start n)))))

(defn- txlog-safe-inc-lsn
  [lsn]
  (if (= (long lsn) Long/MAX_VALUE)
    Long/MAX_VALUE
    (inc (long lsn))))

(defn- txlog-record-window
  [records from upto include-predecessor? include-successor?]
  (let [records (if (vector? records) records (vec records))
        n (long (count records))
        start0 (long (txlog-record-lower-bound-index records from))
        start (long (if include-predecessor?
                      (if (pos? ^long start0) (dec ^long start0) 0)
                      start0))
        end0 (long (if (some? upto)
                     (txlog-record-lower-bound-index
                      records
                      (txlog-safe-inc-lsn upto))
                     n))
        end (long (if (and include-successor? (some? upto))
                    (min ^long n (inc ^long end0))
                    end0))
        end (long (if (< ^long end ^long start) start end))]
    (subvec records start end)))

(defn- collect-txlog-records
  ([state segments cache-v from]
   (collect-txlog-records state segments cache-v from true))
  ([state segments cache-v from trim-terminal?]
  (let [from (long from)]
    (loop [remaining (seq (rseq segments))
           collected '()
           earliest-collected-lsn nil]
      (if-let [segment (first remaining)]
        (let [{:keys [records]}
              (txlog-segment-records-entry state segment cache-v)
              records' (if (and (seq records)
                                (some? earliest-collected-lsn))
                         ;; Segment rollover and snapshot fallback can retain an
                         ;; overlapping boundary record in both the older and
                         ;; newer segment. Keep the newer copy and trim the
                         ;; older prefix so sequence validation still catches
                         ;; real gaps without failing on duplicate boundaries.
                         (->> records
                              (take-while
                               #(< (long (:lsn %))
                                   ^long earliest-collected-lsn))
                              vec)
                         records)
              earliest' (or (some-> records' first :lsn long)
                            earliest-collected-lsn)
              terminal? (and (some? earliest')
                             (<= ^long earliest' from))
              records'' (if (and trim-terminal? terminal? (seq records'))
                          (txlog-records-validation-tail records' from)
                          records')
              collected' (if (seq records')
                           (cons records'' collected)
                           collected)]
          (if terminal?
            (mapcat identity collected')
            (recur (next remaining) collected' earliest')))
        (mapcat identity collected))))))

(defn- validate-txlog-record-sequence!
  [records]
  (loop [prev nil
         records (seq records)]
    (when-let [record (first records)]
      (let [lsn (:lsn record)]
        (when (or (not (pos? ^long lsn))
                  (and prev (not= lsn (inc ^long prev))))
          (raise "Txn-log sequence is invalid"
                 {:type :txlog/corrupt
                  :previous-lsn prev
                  :lsn lsn
                  :record record}))
        (recur lsn (next records))))))

(defn- assert-hydrated-record-matches-summary!
  [summary record]
  (let [expected-lsn (some-> (:lsn summary) long)
        actual-lsn (some-> (:lsn record) long)
        expected-checksum (some-> (:checksum summary) long)
        actual-checksum (some-> (:checksum record) long)]
    (when (or (not= expected-lsn actual-lsn)
              (and (some? expected-checksum)
                   (some? actual-checksum)
                   (not= expected-checksum actual-checksum)))
      (raise "Txn-log record index no longer matches segment contents"
             {:type :txlog/corrupt
              :expected summary
              :actual (select-keys record
                                   [:lsn :segment-id :offset :checksum
                                    :path])}))))

(defn- hydrate-txlog-record-group
  [records]
  (let [records (vec records)]
    (if (every? #(contains? % :rows) records)
      records
      (let [{:keys [path offset]} (first records)
            end-offset (:next-offset (peek records))
            segment-id (:segment-id (first records))]
        (when (or (nil? path)
                  (nil? offset)
                  (nil? end-offset)
                  (nil? segment-id))
          (raise "Txn-log record cache entry is missing segment offsets"
                 {:type :txlog/corrupt
                  :record (first records)}))
        (let [by-offset (into {}
                              (map (fn [record]
                                     [(long (:offset record)) record]))
                              records)
              acc (FastList.)]
          (txlog/scan-segment
           path
           {:allow-preallocated-tail? true
            :start-offset (long offset)
            :max-offset (long end-offset)
            :collect-records? false
            :on-record
            (fn [record]
              (when-let [summary (get by-offset (long (:offset record)))]
                (let [hydrated (txlog-record-entry
                                (long segment-id) path record true)]
                  (assert-hydrated-record-matches-summary!
                   summary hydrated)
                  (.add acc hydrated))))})
          (let [hydrated (vec acc)]
            (when-not (= (count records) (count hydrated))
              (raise "Txn-log record hydration missed indexed records"
                     {:type :txlog/corrupt
                      :path path
                      :expected (mapv :offset records)
                      :actual (mapv :offset hydrated)}))
            hydrated))))))

(defn- hydrate-txlog-records
  [records]
  (->> records
       (partition-by (juxt :segment-id :path))
       (mapcat hydrate-txlog-record-group)
       vec))

(defn- txlog-records-read
  [state from-lsn upto-lsn recovery?]
  (let [dir (:dir state)
        cache-v (:txlog-records-cache state)
        from (long (max 0 (long (or from-lsn 0))))
        upto (some-> upto-lsn long)]
    (when (and (some? upto) (< ^long upto from))
      (raise "Invalid txlog range: upto-lsn is smaller than from-lsn"
             {:type :txlog/invalid-range
              :from-lsn from
              :upto-lsn upto}))
    (loop [retries-left (if cache-v 1 0)]
      (let [segments (vec (txlog/segment-files dir))
            _ (prune-txlog-records-cache! cache-v segments)
            raw-records (vec (collect-txlog-records
                              state segments cache-v from
                              (not recovery?)))
            ;; `from-lsn` is inclusive for the public txn-log APIs. Recovery
            ;; keeps the retained segment prefix so marker references and
            ;; snapshot floors can be validated before replay drops applied
            ;; records.
            validation-records (if recovery?
                                 raw-records
                                 (txlog-record-window
                                  raw-records from upto true true))
            selected-records (if recovery?
                               validation-records
                               (txlog-record-window
                                validation-records from upto false false))
            [records error] (try
                              (validate-txlog-record-sequence!
                               validation-records)
                              [(hydrate-txlog-records selected-records) nil]
                              (catch clojure.lang.ExceptionInfo e
                                [nil e]))]
        (if error
          (if (and (pos? retries-left)
                   cache-v
                   (= :txlog/corrupt (:type (ex-data error))))
            (do
              ;; Segment scans can be cached off a stale active/closed boundary.
              ;; When sequence validation fails, drop the cache once and rescan
              ;; from disk before surfacing corruption.
              (vreset! cache-v {})
              (recur 0))
            (throw error))
          (do
            (prune-txlog-records-cache! cache-v segments)
            records))))))

(defn txlog-records
  ([state]
   (txlog-records state nil))
  ([state from-lsn]
   (txlog-records state from-lsn nil))
  ([state from-lsn upto-lsn]
   (txlog-records-read state from-lsn upto-lsn false)))

(defn txlog-records-for-recovery
  [state from-lsn]
  (txlog-records-read state from-lsn nil true))
