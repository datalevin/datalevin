;;
;; Copyright (c) Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns ^:no-doc datalevin.txlog.recovery
  "Txn-log recovery, retention, and reporting helpers."
  (:require
   [datalevin.txlog.codec :as codec]
   [datalevin.txlog.segment :as seg]
   [datalevin.util :as u :refer [raise]])
  (:import
   [java.io File]
   [org.eclipse.collections.impl.list.mutable FastList]))

(defn decode-scanned-record-entry
  [^long segment-id ^String path record]
  (try
    (let [^bytes body (:body record)
          payload (codec/decode-commit-row-payload body)
          lsn     (long (or (:lsn payload) 0))]
      (when-not (pos? lsn)
        (raise "Txn-log payload missing valid positive LSN"
               {:type :txlog/corrupt
                :segment-id segment-id
                :path path
                :offset (long (:offset record))
                :record record}))
      (let [tx-time (long (or (:tx-time payload)
                              (:ts payload)
                              0))
            ha-term (some-> (:ha-term payload) long)
            rows    (let [ops (:ops payload)]
                      (cond
                        (vector? ops) ops
                        (nil? ops) []
                        :else (vec ops)))
            tx-kind (codec/classify-record-kind rows)
            payload-bytes (long (or (:body-len record)
                                    (some-> body alength)
                                    0))]
        (cond-> {:lsn lsn
                 :tx-kind tx-kind
                 :tx-time tx-time
                 :rows rows
                 :payload-bytes payload-bytes
                 :segment-id segment-id
                 :offset (long (:offset record))
                 :checksum (long (:checksum record))
                 :path path}
          (some? ha-term)
          (assoc :ha-term ha-term))))
    (catch Exception e
      (raise "Malformed txn-log payload"
             e
             {:type :txlog/corrupt
              :segment-id segment-id
              :path path
              :offset (long (:offset record))}))))

(defn txlog-records-cache-entry
  [segment-id path file-bytes modified-ms records]
  {:segment-id segment-id
   :path path
   :file-bytes file-bytes
   :modified-ms modified-ms
   :min-lsn (some-> records first :lsn long)
   :records records})

(defn scan-segment-records-cache-entry
  [^long segment-id ^File file allow-preallocated-tail?]
  (let [path (.getPath file)
        file-bytes (long (.length file))
        modified-ms (long (.lastModified file))
        acc (FastList.)
        scan (seg/scan-segment path
                               {:allow-preallocated-tail? allow-preallocated-tail?
                                :collect-records? false
                                :on-record
                                (fn [record]
                                  (.add acc
                                        (decode-scanned-record-entry
                                         segment-id
                                         path
                                         record)))})
        records (vec acc)]
    {:cache-entry (txlog-records-cache-entry
                   segment-id path file-bytes modified-ms records)
     :partial-tail? (boolean (:partial-tail? scan))
     :last-lsn (some-> records peek :lsn long)}))

(def ^:const no-floor-lsn
  Long/MAX_VALUE)

(defn safe-inc-lsn
  [^long lsn]
  (if (= lsn Long/MAX_VALUE)
    lsn
    (inc lsn)))

(defn parse-floor-lsn
  [v floor-k]
  (if (nil? v)
    no-floor-lsn
    (let [lsn (try
                (long v)
                (catch Exception _
                  (raise "Invalid floor LSN value"
                         {:type :txlog/invalid-floor
                          :floor floor-k
                          :value v})))]
      (when (neg? (long lsn))
        (raise "Floor LSN must be non-negative"
               {:type :txlog/invalid-floor
                :floor floor-k
                :value v}))
      lsn)))

(defn parse-optional-floor-lsn
  [v floor-k]
  (when (some? v)
    (parse-floor-lsn v floor-k)))

(defn parse-non-negative-long
  [v field-k]
  (when (some? v)
    (let [n (try
              (long v)
              (catch Exception _
                (raise "Invalid non-negative long value"
                       {:type :txlog/invalid-floor-provider-state
                        :field field-k
                        :value v})))]
      (when (neg? (long n))
        (raise "Negative value is not allowed"
               {:type :txlog/invalid-floor-provider-state
                :field field-k
                :value v}))
      n)))

(defn ensure-floor-provider-id
  [id kind]
  (when (nil? id)
    (raise "Floor provider id cannot be nil"
           {:type :txlog/invalid-floor-provider-state
            :kind kind
            :id id}))
  id)

(defn parse-floor-provider-map
  [v k]
  (cond
    (nil? v) {}
    (map? v) v
    :else
    (raise "Floor provider map in kv-info must be a map"
           {:type :txlog/invalid-floor-provider-state
            :key k
            :value v})))

(defn snapshot-floor-update-plan
  [snapshot-lsn previous-snapshot-lsn old-current-raw old-previous-raw
   current-floor-k previous-floor-k]
  (let [snapshot-lsn (parse-floor-lsn snapshot-lsn current-floor-k)
        old-current-lsn (parse-optional-floor-lsn old-current-raw
                                                  current-floor-k)
        old-previous-lsn (parse-optional-floor-lsn old-previous-raw
                                                   previous-floor-k)
        previous-lsn (if (some? previous-snapshot-lsn)
                       (parse-optional-floor-lsn previous-snapshot-lsn
                                                 previous-floor-k)
                       old-current-lsn)]
    (when (and (some? old-current-lsn)
               (< (long snapshot-lsn) (long old-current-lsn)))
      (raise "Snapshot current LSN cannot move backward"
             {:type :txlog/invalid-floor-provider-state
              :old-current-lsn old-current-lsn
              :new-current-lsn snapshot-lsn}))
    (when (and (some? previous-lsn)
               (> (long previous-lsn) (long snapshot-lsn)))
      (raise "Snapshot previous LSN cannot be greater than current LSN"
             {:type :txlog/invalid-floor-provider-state
              :snapshot-previous-lsn previous-lsn
              :snapshot-current-lsn snapshot-lsn}))
    (when (and (some? old-previous-lsn)
               (some? previous-lsn)
               (< (long previous-lsn) (long old-previous-lsn)))
      (raise "Snapshot previous LSN cannot move backward"
             {:type :txlog/invalid-floor-provider-state
              :old-previous-lsn old-previous-lsn
              :new-previous-lsn previous-lsn}))
    {:txs (cond-> [[:put current-floor-k snapshot-lsn]]
            (some? previous-lsn)
            (conj [:put previous-floor-k previous-lsn])

            (nil? previous-lsn)
            (conj [:del previous-floor-k]))
     :ok? true
     :snapshot-current-lsn snapshot-lsn
     :snapshot-previous-lsn previous-lsn
     :rotated? (and (nil? previous-snapshot-lsn)
                    (some? old-current-lsn))
     :old-current-lsn old-current-lsn
     :old-previous-lsn old-previous-lsn}))

(defn snapshot-floor-clear-plan
  [old-current-raw old-previous-raw current-floor-k previous-floor-k]
  (let [old-current-lsn (parse-optional-floor-lsn old-current-raw
                                                  current-floor-k)
        old-previous-lsn (parse-optional-floor-lsn old-previous-raw
                                                   previous-floor-k)]
    {:txs [[:del current-floor-k]
           [:del previous-floor-k]]
     :ok? true
     :cleared? (boolean (or (some? old-current-lsn)
                            (some? old-previous-lsn)))
     :old-current-lsn old-current-lsn
     :old-previous-lsn old-previous-lsn}))

(defn- replica-entry-lsn
  [replica-state replica-id]
  (let [old (get replica-state replica-id)
        old-lsn-raw (if (map? old)
                      (or (:applied-lsn old)
                          (:ack-lsn old)
                          (:lsn old)
                          (:floor-lsn old))
                      old)]
    (parse-optional-floor-lsn old-lsn-raw [:replica replica-id :lsn])))

(defn replica-floor-update-plan
  [replica-id applied-lsn now-ms entries entries-k]
  (let [replica-id (ensure-floor-provider-id replica-id :replica)
        lsn (parse-floor-lsn applied-lsn [:replica replica-id :lsn])
        now-ms (long now-ms)
        m0 (parse-floor-provider-map entries entries-k)
        old-lsn (replica-entry-lsn m0 replica-id)]
    (when (and (some? old-lsn) (< (long lsn) (long old-lsn)))
      (raise "Replica floor LSN cannot move backward"
             {:type :txlog/invalid-floor-provider-state
              :replica-id replica-id
              :old-lsn old-lsn
              :new-lsn lsn}))
    (let [m1 (assoc m0 replica-id {:applied-lsn lsn
                                   :updated-ms now-ms})]
      {:entries m1
       :ok? true
       :replica-id replica-id
       :applied-lsn lsn
       :updated-ms now-ms
       :replica-count (count m1)})))

(defn replica-floor-clear-plan
  [replica-id entries entries-k]
  (let [replica-id (ensure-floor-provider-id replica-id :replica)
        m0 (parse-floor-provider-map entries entries-k)
        existed? (contains? m0 replica-id)
        m1 (dissoc m0 replica-id)]
    {:entries m1
     :ok? true
     :replica-id replica-id
     :removed? existed?
     :replica-count (count m1)}))

(defn backup-pin-floor-update-plan
  [pin-id floor-lsn expires-ms now-ms entries entries-k]
  (let [pin-id (ensure-floor-provider-id pin-id :backup-pin)
        floor-lsn (parse-floor-lsn floor-lsn
                                   [:backup-pin pin-id :floor-lsn])
        expires-ms (parse-non-negative-long expires-ms
                                            [:backup-pin pin-id :expires-ms])
        now-ms (long now-ms)
        m0 (parse-floor-provider-map entries entries-k)
        pin (cond-> {:floor-lsn floor-lsn
                     :updated-ms now-ms}
              (some? expires-ms) (assoc :expires-ms expires-ms))
        m1 (assoc m0 pin-id pin)]
    {:entries m1
     :ok? true
     :pin-id pin-id
     :floor-lsn floor-lsn
     :expires-ms expires-ms
     :updated-ms now-ms
     :pin-count (count m1)}))

(defn ^:redef backup-pin-floor-clear-plan
  [pin-id entries entries-k]
  (let [pin-id (ensure-floor-provider-id pin-id :backup-pin)
        m0 (parse-floor-provider-map entries entries-k)
        existed? (contains? m0 pin-id)
        m1 (dissoc m0 pin-id)]
    {:entries m1
     :ok? true
     :pin-id pin-id
     :removed? existed?
     :pin-count (count m1)}))

(defn min-floor-lsn
  [coll]
  (if-let [xs (seq coll)]
    (reduce min no-floor-lsn xs)
    no-floor-lsn))

(defn snapshot-floor-state
  ([current-raw previous-raw applied-lsn]
   (snapshot-floor-state current-raw previous-raw applied-lsn
                         :wal-snapshot-current-lsn
                         :wal-snapshot-previous-lsn))
  ([current-raw previous-raw applied-lsn current-floor-k previous-floor-k]
   (let [current-lsn (parse-optional-floor-lsn current-raw current-floor-k)
         previous-lsn (parse-optional-floor-lsn previous-raw previous-floor-k)
         [floor-lsn source]
         (cond
           (some? previous-lsn)
           [(safe-inc-lsn previous-lsn) :snapshot-previous]

           (some? current-lsn)
           [(safe-inc-lsn current-lsn) :snapshot-current]

           :else
           [(safe-inc-lsn applied-lsn) :applied-lsn])]
     {:floor-lsn floor-lsn
      :source source
      :snapshot-current-lsn current-lsn
      :snapshot-previous-lsn previous-lsn})))

(defn vector-domain-floor-state
  [domain meta]
  (let [previous-raw (or (:previous-snapshot-lsn meta)
                         (:vec-previous-snapshot-lsn meta))
        current-raw (or (:current-snapshot-lsn meta)
                        (:vec-current-snapshot-lsn meta))
        replay-raw (:vec-replay-floor-lsn meta)
        previous-lsn (parse-optional-floor-lsn previous-raw
                                               [:vec-meta domain
                                                :previous-snapshot-lsn])
        current-lsn (parse-optional-floor-lsn current-raw
                                              [:vec-meta domain
                                               :current-snapshot-lsn])
        replay-lsn (parse-optional-floor-lsn replay-raw
                                             [:vec-meta domain
                                              :vec-replay-floor-lsn])]
    (cond
      (some? previous-lsn)
      {:floor-lsn (safe-inc-lsn previous-lsn)
       :source :previous-snapshot
       :snapshot-lsn previous-lsn}

      (some? current-lsn)
      {:floor-lsn (safe-inc-lsn current-lsn)
       :source :current-snapshot
       :snapshot-lsn current-lsn}

      (some? replay-lsn)
      {:floor-lsn replay-lsn
       :source :replay-floor
       :replay-floor-lsn replay-lsn}

      :else
      {:floor-lsn no-floor-lsn
       :source :none})))

(defn vector-floor-state
  [meta-by-domain configured-domains legacy-floor-raw legacy-floor-k]
  (let [meta-by-domain (or meta-by-domain {})
        domains (into (set (keys meta-by-domain))
                      (or configured-domains #{}))
        domain-floors (into {}
                            (map (fn [domain]
                                   [domain
                                    (vector-domain-floor-state
                                     domain (get meta-by-domain domain {}))]))
                            domains)
        computed-floor (min-floor-lsn (map (comp :floor-lsn val)
                                           domain-floors))
        legacy-floor (parse-floor-lsn legacy-floor-raw legacy-floor-k)
        floor-lsn (min (long computed-floor) (long legacy-floor))]
    {:floor-lsn floor-lsn
     :computed-floor-lsn computed-floor
     :legacy-floor-lsn legacy-floor
     :domain-count (count domains)
     :domains domain-floors}))

(defn replica-floor-state
  [entries ttl-ms now-ms legacy-floor-raw legacy-floor-k]
  (let [ttl-ms (long ttl-ms)
        entries (if (map? entries) entries {})
        parsed (->> entries
                    (keep
                     (fn [[replica-id state]]
                       (let [lsn-raw (if (map? state)
                                       (or (:applied-lsn state)
                                           (:ack-lsn state)
                                           (:lsn state)
                                           (:floor-lsn state))
                                       state)]
                         (when (some? lsn-raw)
                           (let [lsn (parse-floor-lsn
                                      lsn-raw
                                      [:replica replica-id :lsn])
                                 updated-ms (when (map? state)
                                              (parse-non-negative-long
                                               (or (:updated-ms state)
                                                   (:heartbeat-ms state))
                                               [:replica replica-id
                                                :updated-ms]))
                                 stale? (and (some? updated-ms)
                                             (pos? ttl-ms)
                                             (> (- (long now-ms) (long updated-ms))
                                                ttl-ms))]
                             {:replica-id replica-id
                              :floor-lsn lsn
                              :updated-ms updated-ms
                              :stale? stale?}))))))
        active (->> parsed (remove :stale?) vec)
        computed-floor (min-floor-lsn (map :floor-lsn active))
        legacy-floor (parse-floor-lsn legacy-floor-raw legacy-floor-k)
        floor-lsn (min (long computed-floor) (long legacy-floor))]
    {:floor-lsn floor-lsn
     :computed-floor-lsn computed-floor
     :legacy-floor-lsn legacy-floor
     :ttl-ms ttl-ms
     :active-count (count active)
     :stale-count (- (count parsed) (count active))
     :replicas parsed}))

(defn backup-pin-floor-state
  [entries now-ms legacy-floor-raw legacy-floor-k]
  (let [entries (if (map? entries) entries {})
        parsed (->> entries
                    (keep
                     (fn [[pin-id state]]
                       (let [lsn-raw (if (map? state)
                                       (or (:floor-lsn state)
                                           (:lsn state))
                                       state)]
                         (when (some? lsn-raw)
                           (let [floor-lsn (parse-floor-lsn
                                            lsn-raw
                                            [:backup-pin pin-id
                                             :floor-lsn])
                                 expires-ms (when (map? state)
                                              (parse-non-negative-long
                                               (:expires-ms state)
                                               [:backup-pin pin-id
                                                :expires-ms]))
                                 expired? (and (some? expires-ms)
                                               (< (long expires-ms)
                                                  (long now-ms)))]
                             {:pin-id pin-id
                              :floor-lsn floor-lsn
                              :expires-ms expires-ms
                              :expired? expired?}))))))
        active (->> parsed (remove :expired?) vec)
        computed-floor (min-floor-lsn (map :floor-lsn active))
        legacy-floor (parse-floor-lsn legacy-floor-raw legacy-floor-k)
        floor-lsn (min (long computed-floor) (long legacy-floor))]
    {:floor-lsn floor-lsn
     :computed-floor-lsn computed-floor
     :legacy-floor-lsn legacy-floor
     :active-count (count active)
     :expired-count (- (count parsed) (count active))
     :pins parsed}))

(defn annotate-gc-segments
  [segments active-segment-id newest-segment-id retention-ms gc-safety-watermark]
  (mapv
   (fn [segment]
     (let [segment-id (:segment-id segment)
           max-lsn (:max-lsn segment)
           active? (= segment-id active-segment-id)
           newest? (= segment-id newest-segment-id)
           age-exceeded? (>= ^long (:age-ms segment) ^long (long retention-ms))
           safety-deletable? (and (some? max-lsn)
                                  (<= ^long max-lsn
                                      ^long (long gc-safety-watermark))
                                  (not active?)
                                  (not newest?))]
       (assoc segment
              :active? active?
              :newest? newest?
              :age-exceeded? age-exceeded?
              :safety-deletable? safety-deletable?)))
   segments))

(defn select-gc-target-segments
  [segments total-bytes retention-bytes explicit-gc?]
  (let [safe-segments (->> segments (filter :safety-deletable?) vec)]
    (if explicit-gc?
      safe-segments
      (let [age-target-ids
            (into #{} (map :segment-id (filter :age-exceeded? safe-segments)))
            byte-target-ids
            (if (> (long total-bytes) (long retention-bytes))
              (loop [remaining total-bytes
                     [segment & more] safe-segments
                     ids #{}]
                (if (and segment (> (long remaining) (long retention-bytes)))
                  (recur (- (long remaining) ^long (:bytes segment))
                         more
                         (conj ids (:segment-id segment)))
                  ids))
              #{})
            target-ids (into age-target-ids byte-target-ids)]
        (->> safe-segments
             (filter #(contains? target-ids (:segment-id %)))
             vec)))))

(defn- segment-summary-from-cache-entry
  [entry now-ms]
  (let [created-ms (long (:created-ms entry))]
    {:segment-id (long (:segment-id entry))
     :path (:path entry)
     :bytes (long (:bytes entry))
     :created-ms created-ms
     :age-ms (long (max 0 (- (long now-ms) (long created-ms))))
     :record-count (long (:record-count entry))
     :min-lsn (some-> (:min-lsn entry) long)
     :max-lsn (some-> (:max-lsn entry) long)
     :partial-tail? (boolean (:partial-tail? entry))
     :preallocated-tail? (boolean (:preallocated-tail? entry))}))

(defn- scan-segment-summary-entry
  [segment-id ^File file allow-preallocated-tail? record->lsn marker-offset
   max-offset]
  (let [path (.getPath file)
        file-bytes (long (.length file))
        created-ms (long (.lastModified file))
        record-count-v (volatile! 0)
        first-record-v (volatile! nil)
        last-record-v (volatile! nil)
        marker-rec-v (volatile! nil)
        on-record (fn [record]
                    (vreset! record-count-v (inc (long @record-count-v)))
                    (when (nil? @first-record-v)
                      (vreset! first-record-v record))
                    (vreset! last-record-v record)
                    (when (and (some? marker-offset)
                               (= (long marker-offset)
                                  (long (:offset record))))
                      (vreset! marker-rec-v record)))
        scan (seg/scan-segment
              path
              {:allow-preallocated-tail? allow-preallocated-tail?
               :max-offset max-offset
               :collect-records? false
               :on-record on-record})
        bytes (if (:partial-tail? scan)
                (long (:valid-end scan))
                (long (:size scan)))
        first-record @first-record-v
        last-record @last-record-v
        marker-record* @marker-rec-v
        min-lsn (some-> first-record record->lsn long)
        max-lsn (some-> last-record record->lsn long)
        marker-rec (when marker-record*
                     {:segment-id segment-id
                      :offset (long (:offset marker-record*))
                      :checksum (long (:checksum marker-record*))
                      :lsn (some-> marker-record* record->lsn long)})]
    {:entry {:segment-id segment-id
             :path path
             :file-bytes file-bytes
             :created-ms created-ms
             :bytes bytes
             :record-count (long @record-count-v)
             :min-lsn min-lsn
             :max-lsn max-lsn
             :partial-tail? (boolean (:partial-tail? scan))
             :preallocated-tail? (boolean (:preallocated-tail? scan))
             :marker-resolved? (some? marker-offset)
             :marker-offset marker-offset
             :marker-record marker-rec}
     :marker-record marker-rec}))

(defn segment-summaries
  [^String dir
   {:keys [allow-preallocated-tail?
           record->lsn
           marker-segment-id
           marker-offset
           active-segment-id
           active-segment-offset
           cache-v
           cache-key
           min-retained-fallback]
    :or {allow-preallocated-tail? true
         record->lsn #(or (:lsn %) nil)}}]
  (let [now-ms (System/currentTimeMillis)
        segments (seg/segment-files dir)
        marker-seg-id (some-> marker-segment-id long)
        active-seg-id (some-> active-segment-id long)
        active-offset (some-> active-segment-offset long)
        cache? (and (some? cache-v) (some? cache-key))
        bucket-k (when cache? [cache-key allow-preallocated-tail?])
        cache-root0 (if cache? @cache-v {})
        cache0 (if cache? (get cache-root0 bucket-k {}) {})
        [summaries marker-record cache1]
        (reduce
         (fn [[acc marker cache*] {:keys [id file]}]
           (let [segment-id (long id)
                 file ^File file
                 path (.getPath file)
                 marker-offset* (when (and (some? marker-seg-id)
                                           (some? marker-offset)
                                           (= segment-id marker-seg-id))
                                  (long marker-offset))
                 cached-entry (get cache0 segment-id)
                 path-ok? (and cached-entry
                               (= path (:path cached-entry)))
                 marker-ok? (or (nil? marker-offset*)
                                (and cached-entry
                                     (:marker-resolved? cached-entry)
                                     (= marker-offset*
                                        (:marker-offset
                                         cached-entry))))
                 active-id-known? (some? active-seg-id)
                 active-segment? (and active-id-known?
                                      (= segment-id active-seg-id))
                 reuse-closed? (and path-ok?
                                    marker-ok?
                                    active-id-known?
                                    (not active-segment?))
                 reuse-active-by-offset?
                 (and path-ok?
                      marker-ok?
                      active-segment?
                      (some? active-offset)
                      (= active-offset
                         (long (or (:bytes cached-entry) -1))))
                 file-bytes* (delay (long (.length file)))
                 created-ms* (delay (long (.lastModified file)))
                 reuse-by-metadata?
                 (and path-ok?
                      marker-ok?
                      (= @file-bytes*
                         (long (:file-bytes cached-entry)))
                      (= @created-ms*
                         (long (:created-ms cached-entry))))
                 reusable-entry? (or reuse-closed?
                                     reuse-active-by-offset?
                                     reuse-by-metadata?)
                 max-offset* (when (and active-segment?
                                        (some? active-offset))
                               active-offset)
                 {:keys [entry marker-record]}
                 (if reusable-entry?
                   {:entry cached-entry
                    :marker-record
                    (when (some? marker-offset*)
                      (:marker-record cached-entry))}
                   (scan-segment-summary-entry segment-id
                                               file
                                               allow-preallocated-tail?
                                               record->lsn
                                               marker-offset*
                                               max-offset*))]
             [(conj acc (segment-summary-from-cache-entry entry now-ms))
              (or marker marker-record)
              (assoc cache* segment-id entry)]))
         [[] nil {}]
         segments)
        _ (when cache?
            (let [cache-root1 (if (seq cache1)
                                (assoc cache-root0 bucket-k cache1)
                                (dissoc cache-root0 bucket-k))]
              (when-not (= cache-root0 cache-root1)
                (vreset! cache-v cache-root1))))
        min-retained (or (some :min-lsn summaries) min-retained-fallback)
        total-bytes (reduce (fn [acc {:keys [bytes]}] (+ ^long acc ^long bytes))
                            0 summaries)
        newest-id (some-> (peek summaries) :segment-id)]
    {:segments summaries
     :marker-record marker-record
     :min-retained-lsn (some-> min-retained long)
     :total-bytes total-bytes
     :newest-segment-id (some-> newest-id long)}))

(defn retention-state
  [{:keys [segments
           total-bytes
           retention-bytes
           retention-ms
           active-segment-id
           newest-segment-id
           floors
           explicit-gc?]}]
  (let [required-floor-lsn (min-floor-lsn (vals floors))
        gc-safety-watermark (dec ^long required-floor-lsn)
        segments* (annotate-gc-segments
                   segments active-segment-id newest-segment-id
                   retention-ms gc-safety-watermark)
        safety-deletable (->> segments* (filter :safety-deletable?) vec)
        safety-bytes (reduce (fn [acc {:keys [bytes]}]
                               (+ ^long acc ^long bytes))
                             0 safety-deletable)
        bytes-exceeded? (> (long total-bytes) (long retention-bytes))
        age-exceeded? (boolean
                       (some (fn [{:keys [active? age-exceeded?]}]
                               (and (not active?) age-exceeded?))
                             segments*))
        pressure? (or bytes-exceeded? age-exceeded?)
        gc-targets (select-gc-target-segments
                    segments* total-bytes retention-bytes explicit-gc?)
        gc-target-bytes (reduce (fn [acc {:keys [bytes]}]
                                  (+ ^long acc ^long bytes))
                                0 gc-targets)
        retained-bytes-after (- ^long total-bytes ^long gc-target-bytes)
        floor-limiters (->> floors
                            (filter (fn [[_ floor]]
                                      (= ^long floor ^long required-floor-lsn)))
                            (map key)
                            sort
                            vec)
        degraded? (and pressure? (empty? gc-targets))]
    {:segments segments*
     :required-retained-floor-lsn required-floor-lsn
     :gc-safety-watermark-lsn gc-safety-watermark
     :floor-limiters floor-limiters
     :safety-deletable-segment-count (count safety-deletable)
     :safety-deletable-segment-bytes safety-bytes
     :pressure {:bytes-exceeded? bytes-exceeded?
                :age-exceeded? age-exceeded?
                :pressure? pressure?
                :degraded? degraded?}
     :gc-target-segments gc-targets
     :gc-target-segment-ids (mapv :segment-id gc-targets)
     :gc-target-segment-count (count gc-targets)
     :gc-target-segment-bytes gc-target-bytes
     :retained-bytes-after-gc retained-bytes-after
     :explicit-gc? explicit-gc?}))

(defn valid-commit-marker
  [marker marker-record]
  (when (and marker marker-record
             (= (long (:applied-lsn marker))
                (long (:lsn marker-record)))
             (= (long (:txlog-record-crc marker))
                (long (:checksum marker-record))))
    marker))

(defn newer-commit-marker
  [slot-a slot-b]
  (cond
    (and slot-a slot-b)
    (if (>= ^long (:revision slot-a) ^long (:revision slot-b))
      slot-a
      slot-b)

    slot-a slot-a
    slot-b slot-b
    :else nil))

(defn- marker-record-by-reference
  [marker records]
  (when marker
    (let [segment-id (long (:txlog-segment-id marker))
          offset (long (:txlog-record-offset marker))]
      (some (fn [r]
              (when (and (= segment-id (long (:segment-id r)))
                         (= offset (long (:offset r))))
                r))
            records))))

(defn validate-commit-marker-reference
  [marker records]
  (valid-commit-marker marker (marker-record-by-reference marker records)))

(defn resolve-applied-lsn
  [{:keys [commit-marker?
           marker
           marker-record
           has-records?
           meta-last-applied-lsn
           min-retained-lsn]}]
  (let [valid-marker (valid-commit-marker marker marker-record)
        meta-applied-lsn (long (or meta-last-applied-lsn 0))
        marker-applied-lsn (long (or (:applied-lsn valid-marker) 0))
        applied-lsn (if commit-marker?
                      (max marker-applied-lsn meta-applied-lsn)
                      meta-applied-lsn)
        min-retained-lsn (long (or min-retained-lsn 0))]
    (when (and commit-marker?
               has-records?
               (nil? valid-marker)
               (< (long (safe-inc-lsn meta-applied-lsn))
                  min-retained-lsn))
      (raise
       "Commit marker is missing or invalid for existing txn-log records"
       {:type :txlog/recovery-marker-invalid
        :meta-last-applied-lsn meta-applied-lsn
        :min-retained-lsn min-retained-lsn}))
    (when (> (long min-retained-lsn) (long (safe-inc-lsn applied-lsn)))
      (raise
       "Retained txn-log floor exceeds recovery cursor coverage"
       {:type :txlog/recovery-floor-gap}
       :min-retained-lsn min-retained-lsn
       :applied-lsn applied-lsn))
    {:valid-marker valid-marker
     :applied-lsn applied-lsn}))

(defn recovery-state
  [{:keys [commit-marker?
           marker-state
           records
           meta-last-applied-lsn
           next-lsn]}]
  (let [marker (:current marker-state)
        marker-record (marker-record-by-reference marker records)
        has-records? (boolean (seq records))
        min-retained (if (seq records)
                       (long (:lsn (first records)))
                       (long (or next-lsn 0)))
        {:keys [valid-marker applied-lsn]}
        (resolve-applied-lsn
         {:commit-marker? commit-marker?
          :marker marker
          :marker-record marker-record
          :has-records? has-records?
          :meta-last-applied-lsn meta-last-applied-lsn
          :min-retained-lsn min-retained})]
    {:marker marker
     :marker-record marker-record
     :has-records? has-records?
     :min-retained-lsn min-retained
     :applied-lsn (long applied-lsn)
     :valid-marker valid-marker}))

(defn- select-open-records*
  [records from-lsn upto-lsn transform-record]
  (let [from (max 0 (long (or from-lsn 0)))
        upto (when (some? upto-lsn) (long upto-lsn))]
    (when (and upto (< (long upto) (long from)))
      (raise "Invalid txlog range: upto-lsn is smaller than from-lsn"
             {:type :txlog/invalid-range
              :from-lsn from
              :upto-lsn upto}))
    (persistent!
     (reduce (fn [acc record]
               (let [lsn (long (:lsn record))]
                 (cond
                   (< lsn from)
                   acc

                   (and upto (> lsn (long upto)))
                   (reduced acc)

                   :else
                   (conj! acc (transform-record record)))))
             (transient [])
             records))))

(defn- attach-open-record-payload-bytes
  [record]
  (let [payload-bytes (or (:payload-bytes record)
                          (:body-len record))]
    (cond-> (dissoc record :path :body-len)
      (some? payload-bytes)
      (assoc :payload-bytes (long payload-bytes)))))

(defn select-open-records
  [records from-lsn upto-lsn]
  (select-open-records*
   records
   from-lsn
   upto-lsn
   (fn [{:keys [rows] :as r}]
     (-> r
         (dissoc :rows)
         (assoc :ops rows)
         (attach-open-record-payload-bytes)))))

(defn select-open-record-rows
  [records from-lsn upto-lsn]
  (select-open-records*
   records
   from-lsn
   upto-lsn
   attach-open-record-payload-bytes))

(defn retention-floors
  [{:keys [snapshot-state
           vector-state
           replica-state
           backup-state
           operator-retain-floor-lsn]}]
  {:snapshot-floor-lsn (:floor-lsn snapshot-state)
   :vector-global-floor-lsn (:floor-lsn vector-state)
   :replica-floor-lsn (:floor-lsn replica-state)
   :backup-pin-floor-lsn (:floor-lsn backup-state)
   :operator-retain-floor-lsn
   (parse-floor-lsn operator-retain-floor-lsn :operator-retain-floor-lsn)})

(defn retention-state-report
  [{:keys [dir
           retention-bytes
           retention-ms
           segments
           total-bytes
           active-segment-id
           newest-segment-id
           min-retained-lsn
           applied-lsn
           marker-state
           valid-marker
           floors
           floor-providers
           explicit-gc?]}]
  (let [core-state (retention-state
                    {:segments segments
                     :total-bytes total-bytes
                     :retention-bytes retention-bytes
                     :retention-ms retention-ms
                     :active-segment-id active-segment-id
                     :newest-segment-id newest-segment-id
                     :floors floors
                     :explicit-gc? explicit-gc?})]
    {:wal? true
     :dir dir
     :retention-bytes retention-bytes
     :retention-ms retention-ms
     :segment-count (count (:segments core-state))
     :segment-bytes total-bytes
     :segments (:segments core-state)
     :active-segment-id active-segment-id
     :newest-segment-id newest-segment-id
     :min-retained-lsn min-retained-lsn
     :applied-lsn applied-lsn
     :commit-marker marker-state
     :valid-marker valid-marker
     :floors floors
     :floor-providers floor-providers
     :required-retained-floor-lsn
     (:required-retained-floor-lsn core-state)
     :gc-safety-watermark-lsn
     (:gc-safety-watermark-lsn core-state)
     :floor-limiters (:floor-limiters core-state)
     :safety-deletable-segment-count
     (:safety-deletable-segment-count core-state)
     :safety-deletable-segment-bytes
     (:safety-deletable-segment-bytes core-state)
     :pressure (:pressure core-state)
     :gc-target-segments (:gc-target-segments core-state)
     :gc-target-segment-ids (:gc-target-segment-ids core-state)
     :gc-target-segment-count (:gc-target-segment-count core-state)
     :gc-target-segment-bytes (:gc-target-segment-bytes core-state)
     :retained-bytes-after-gc (:retained-bytes-after-gc core-state)
     :explicit-gc? (:explicit-gc? core-state)}))
