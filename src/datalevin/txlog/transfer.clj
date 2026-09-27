(ns ^:no-doc datalevin.txlog.transfer
  "Encoded WAL transfer batches and bounded, runtime-local reuse."
  (:require
   [datalevin.constants :as c]
   [datalevin.txlog.codec :as codec]
   [datalevin.util :refer [raise]])
  (:import [java.nio ByteBuffer]))

(def ^:private ^:const record-prefix-bytes 26)

;; v1 is big endian: record count, then segment ID (i64), offset (i64), WAL
;; major (u8), flags (u8), body length (i32), checksum (u32), and body bytes.
;; WAL framing/payload versions and their checksums remain unchanged.

(defn encode-batch
  "Pack verified WAL bodies without decoding rows. Callers must not mutate the bytes."
  [records]
  (let [size (reduce (fn [^long n record]
                       (+ n record-prefix-bytes (alength ^bytes (:body record))))
                     4 records)]
    (when (> (long size) Integer/MAX_VALUE)
      (raise "WAL transfer batch is too large" {:type :txlog/transfer-too-large}))
    (let [buffer (ByteBuffer/allocate (int size))]
      (.putInt buffer (count records))
      (doseq [{:keys [segment-id offset major flags checksum body]} records]
        (.putLong buffer (long segment-id))
        (.putLong buffer (long offset))
        (.put buffer (byte major))
        (.put buffer (byte flags))
        (.putInt buffer (alength ^bytes body))
        (.putInt buffer (unchecked-int checksum))
        (.put buffer ^bytes body))
      {:format :datalevin/wal-batch-v1 :data (.array buffer)})))

(defn decode-batch
  "Verify transferred WAL checksums and materialize rows on the receiving node."
  [{:keys [format data]}]
  (when-not (and (= format :datalevin/wal-batch-v1) (bytes? data))
    (raise "Unsupported WAL transfer batch" {:type :txlog/corrupt :format format}))
  (try
    (let [buffer (ByteBuffer/wrap ^bytes data)
          n (.getInt buffer)]
      (when (or (neg? n) (> n (quot (.remaining buffer) record-prefix-bytes)))
        (raise "Invalid WAL transfer record count" {:type :txlog/corrupt :count n}))
      (let [records
            (loop [i 0 records (transient [])]
              (if (= i n)
                (persistent! records)
                (let [segment-id (.getLong buffer)
                      offset (.getLong buffer)
                      major (bit-and 0xff (.get buffer))
                      flags (bit-and 0xff (.get buffer))
                      size (.getInt buffer)
                      checksum (bit-and 0xffffffff (.getInt buffer))]
                  (when (or (neg? size) (> size (.remaining buffer)))
                    (raise "Invalid WAL transfer record size" {:type :txlog/corrupt}))
                  (let [body (byte-array size)
                        _ (.get buffer body)
                        actual (codec/record-checksum-for-major major flags size body)]
                    (when-not (= checksum actual)
                      (raise "WAL transfer checksum mismatch" {:type :txlog/corrupt}))
                    (let [{:keys [lsn ts ha-term ops]} (codec/decode-commit-row-payload body)]
                      (when-not (pos? (long lsn))
                        (raise "Invalid WAL transfer LSN" {:type :txlog/corrupt}))
                      (recur (inc i)
                             (conj! records
                                    (cond-> {:lsn lsn :tx-time ts :rows ops
                                             :tx-kind (codec/classify-record-kind ops)
                                             :segment-id segment-id :offset offset
                                             :checksum checksum :payload-bytes (long size)}
                                      ha-term (assoc :ha-term ha-term)))))))))]
        (when (.hasRemaining buffer)
          (raise "Trailing bytes in WAL transfer batch" {:type :txlog/corrupt}))
        records))
    (catch Exception e
      (throw (ex-info "Malformed WAL transfer batch" {:type :txlog/corrupt} e)))))

(defn create-cache []
  (atom {:entries {} :order [] :bytes 0 :pending {}}))

(defn- touch [state key]
  (update state :order #(conj (into [] (remove #{key}) %) key)))

(defn- trim-cache [state max-bytes max-batches]
  (loop [state state]
    (if (or (> (long (:bytes state)) (long max-bytes))
            (> (count (:entries state)) (long max-batches)))
      (let [key (first (:order state))
            size (get-in state [:entries key :size])]
        (recur (-> state
                   (update :entries dissoc key)
                   (update :order #(into [] (rest %)))
                   (update :bytes - size))))
      state)))

(defn cached-batch
  "Share a load for the same validated record summaries. Disk I/O and waiting
  happen outside the cache monitor; writers never acquire this monitor."
  [cache records load-batch]
  (let [max-bytes (long c/*wal-transfer-cache-bytes*)
        max-batches (long c/*wal-transfer-cache-batches*)
        ;; Account for the retained index keys as well as encoded payloads.
        size (reduce (fn [^long n record]
                       (+ n 256 (long (or (:payload-bytes record) 0))))
                     4 records)]
    (if (or (nil? cache) (empty? records) (not (pos? max-batches))
            (> (long size) max-bytes))
      (load-batch)
      (let [key (into [] records)
            {:keys [batch pending owner?]}
            (locking cache
              (let [state @cache]
                (cond
                  (get-in state [:entries key])
                  (let [batch (get-in state [:entries key :batch])]
                    (reset! cache (touch state key))
                    {:batch batch})

                  (get-in state [:pending key])
                  {:pending (get-in state [:pending key])}

                  (>= (count (:pending state)) max-batches)
                  {}

                  :else
                  (let [pending (promise)]
                    (swap! cache assoc-in [:pending key] pending)
                    {:pending pending :owner? true}))))]
        (cond
          batch batch
          (nil? pending) (load-batch)
          :else
          (let [outcome
                (if owner?
                  (let [outcome (try {:batch (load-batch)}
                                     (catch Throwable t {:error t}))]
                    (locking cache
                      (swap! cache
                             (fn [state]
                               (let [state (update state :pending dissoc key)]
                                 (if-let [batch (:batch outcome)]
                                   (-> state
                                       (assoc-in [:entries key] {:batch batch :size size})
                                       (update :bytes + size)
                                       (touch key)
                                       (trim-cache max-bytes max-batches))
                                   state)))))
                    (deliver pending outcome)
                    outcome)
                  @pending)]
            (if-let [error (:error outcome)] (throw error) (:batch outcome))))))))
