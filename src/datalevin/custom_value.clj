;;
;; Copyright (c) Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0/)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns ^:no-doc datalevin.custom-value
  "Shared ordered references and transactional custom payload storage.

  Internal index descriptors name an already opened raw DBI:
  {:dbi name :position :key} or {:dbi name :position :item :key encoded-key}.
  Each index entry owns one payload. Secondary indexes must share that entry's
  reference, rather than allocate another payload. Public DBI/schema routing
  is implemented above these primitives."
  (:require
   [datalevin.bits :as b]
   [datalevin.buffer :as bf]
   [datalevin.constants :as c]
   [datalevin.interface :as i]
   [datalevin.lmdb :as l]
   [datalevin.util :refer [raise]])
  (:import
   [java.lang AutoCloseable]
   [java.nio BufferOverflowException ByteBuffer]
   [java.util Arrays Iterator]))

(def ^:const id-key :custom-value-id)
(def ^:const min-id 0)
(def ^:const max-id Long/MAX_VALUE)
(def ^:private ^:const reference-overhead 11)

(defn encode-order
  "Encode a validated order key with its existing KV backing codec."
  ^bytes [backing value]
  (loop [size c/+max-key-size+]
    (let [^ByteBuffer buffer (bf/get-array-buffer size)
          encoded (try
                    (b/put-buffer buffer value backing)
                    (Arrays/copyOf (.array buffer) (.position buffer))
                    (catch BufferOverflowException _ nil)
                    (finally (bf/return-array-buffer buffer)))]
      (or encoded (recur (* 2 size))))))

(defn reference-prefix
  "Frame native order bytes before the ID, within a total reference size limit.
  Zero becomes 00 FF; 00 00 terminates a complete key, 00 01 a truncated key.
  This preserves unsigned byte order even when one native key prefixes another.
  The payload, rather than this possibly truncated key, reconstructs the value."
  (^bytes [order-bytes] (reference-prefix order-bytes c/+max-key-size+))
  (^bytes [^bytes order-bytes max-size]
   (when-not (and (int? max-size)
                  (<= reference-overhead max-size c/+max-key-size+))
     (raise "Invalid custom reference size budget"
            {:error :custom-type/reference-size :max-size max-size}))
   (let [buffer (ByteBuffer/allocate (- (long max-size) Long/BYTES))
         limit (- (.capacity buffer) 2)
         n (alength order-bytes)]
     (.put buffer (byte c/type-custom))
     (loop [offset 0]
       (if (= offset n)
         (do (.putShort buffer (short 0))
             (Arrays/copyOf (.array buffer) (.position buffer)))
         (let [v (aget order-bytes offset)
               width (if (zero? v) 2 1)]
           (if (> (+ (.position buffer) width) limit)
             (do (.putShort buffer (short 1))
                 (Arrays/copyOf (.array buffer) (.position buffer)))
             (do (.put buffer v)
                 (when (zero? v) (.put buffer (byte -1)))
                 (recur (inc offset))))))))))

(defn reference
  "Append a nonnegative ID, including the two reserved range sentinels."
  ^bytes [^bytes prefix id]
  (when-not (and (int? id) (<= min-id id max-id))
    (raise "Invalid custom value ID" {:error :custom-type/value-id :id id}))
  (let [buffer (ByteBuffer/allocate (+ (alength prefix) Long/BYTES))]
    (.put buffer prefix)
    (.putLong buffer (long id))
    (.array buffer)))

(defn decode-reference
  "Validate an internal reference and return its ID and native order prefix."
  [^bytes ref]
  (let [n (alength ref)
        end (- n Long/BYTES)
        fail #(raise "Malformed custom value reference"
                     {:error :custom-type/reference})]
    (when (or (< n reference-overhead) (> n c/+max-key-size+)
              (not= c/type-custom (aget ref 0)))
      (fail))
    (let [buffer (ByteBuffer/allocate end)
          id (.getLong (ByteBuffer/wrap ref) end)]
      (when (neg? id) (fail))
      (loop [offset 1]
        (when (>= offset end) (fail))
        (let [v (aget ref offset)]
          (if (zero? v)
            (do
              (when (>= (inc offset) end) (fail))
              (let [next-byte (aget ref (inc offset))]
                (case (int next-byte)
                  -1 (do (.put buffer (byte 0)) (recur (+ offset 2)))
                  (0 1) (do
                          (when (not= (+ offset 2) end) (fail))
                          {:id id :truncated? (= next-byte 1)
                           :order-bytes (Arrays/copyOf (.array buffer)
                                                     (.position buffer))})
                  (fail))))
            (do (.put buffer v) (recur (inc offset)))))))))

(defn reference-id
  "Read a validated, stored reference's ID without allocating its order key."
  ^long [^bytes ref]
  (let [n (alength ref)]
    (when (or (< n reference-overhead) (> n c/+max-key-size+)
              (not= (aget ref 0) c/type-custom))
      (raise "Malformed custom value reference" {:error :custom-type/reference}))
    (let [id (.getLong (ByteBuffer/wrap ref) (- n Long/BYTES))]
      (when-not (< min-id id max-id)
        (raise "Reserved or invalid stored custom value ID"
               {:error :custom-type/value-id :id id}))
      id)))

(defn boundary
  "An endpoint includes or excludes the whole order-key bucket. Side is :lower
  or :upper, independent of scan direction. The returned bound can be closed:
  neither sentinel is a stored ID."
  ^bytes [prefix side inclusive?]
  (reference prefix
             (case side
               :lower (if inclusive? min-id max-id)
               :upper (if inclusive? max-id min-id))))

(defn order-prefix
  "Apply the registered order function and prepare an indexed key or bound."
  (^bytes [type value] (order-prefix type value c/+max-key-size+))
  (^bytes [type value max-size]
   (let [key ((:order-fn type) value)
         backing (get-in type [:definition :index :type])
         encoded (try (encode-order backing key)
                      (catch Exception e
                        (throw (ex-info "Cannot encode custom order key"
                                        {:error :custom-type/order-key
                                         :type-name (:type-name type)
                                         :backing-type backing :value key} e))))]
     (reference-prefix encoded max-size))))

(defn open-store!
  "Open the shared payload DBI before starting an explicit write transaction.
  Native DBI opening owns a transaction, so initialization cannot be nested."
  [kv]
  (i/check-ready kv)
  (let [info (i/kv-info kv)
        check-writer! #(when (and (not (:custom-payload-dbi-open? @info))
                                  (some? @(l/write-txn kv)))
                         (raise "Open custom payload storage before starting a transaction"
                                {:error :custom-type/storage-not-open}))]
    ;; Check before waiting: a writer must never wait for initialization that
    ;; needs its native write lock. Use a separate monitor, because open-dbi
    ;; acquires the DBI map lock before the write-transaction monitor.
    (check-writer!)
    (locking (:custom-type-cache @info)
      (when-not (:custom-payload-dbi-open? @info)
        (check-writer!)
        (i/open-dbi kv c/custom-values)
        (vswap! info assoc :custom-payload-dbi-open? true))))
  kv)

(defn- require-store! [kv]
  (i/check-ready kv)
  (when-not (:custom-payload-dbi-open? @(i/kv-info kv))
    (raise "Custom payload storage is not open; call open-store! first"
           {:error :custom-type/storage-not-open})))

(defn- with-snapshot [kv f]
  (require-store! kv)
  (let [payload-dbi (i/get-dbi kv c/custom-values false)
        writing? (l/writing? kv)
        rtx (if writing? @(l/write-txn kv) (i/get-rtx kv))]
    (try (f payload-dbi rtx)
         (finally (when-not writing? (i/return-rtx kv rtx))))))

(defn- read-payload [payload-dbi rtx id]
  (l/put-key rtx id :id)
  (if-let [buffer (l/get-kv payload-dbi rtx)]
    (b/read-buffer buffer :raw)
    (raise "Missing custom value payload"
           {:error :custom-type/missing-payload :id id})))

(defn read-value
  "Load a logical value from a reference using the owning type's deserializer."
  [kv type ref]
  (with-snapshot kv
    (fn [payload-dbi rtx]
      ((:deserialize type) (read-payload payload-dbi rtx (reference-id ref))))))

(defn read-value-at
  "Resolve a reference using a caller-owned index snapshot."
  [kv type ref rtx]
  ((:deserialize type)
   (read-payload (i/get-dbi kv c/custom-values false) rtx (reference-id ref))))

(defn- check-index! [kv {:keys [dbi position key]}]
  (when (or (not (string? dbi)) (= dbi c/custom-values) (= dbi c/kv-info)
            (not (#{:key :item} position))
            (and (= position :item) (not (bytes? key))))
    (raise "Invalid custom index descriptor" {:error :custom-type/index}))
  (let [opts (i/dbi-opts kv dbi)
        flags (set (:flags opts))]
    (when (or (nil? opts)
              (some flags [:reversekey :integerkey :reversedup :integerdup :dupfixed]))
      (raise "Custom index requires an opened DBI with ordinary byte ordering"
             {:error :custom-type/index :dbi dbi :flags flags})))
  (when (and (= position :item) (not (i/list-dbi? kv dbi)))
    (raise "Custom index position disagrees with DBI duplicate flags"
           {:error :custom-type/index :dbi dbi :position position})))

(defn- reduce-index
  "Keep index and payload reads in one snapshot. Never retain cursor buffers."
  [kv {:keys [dbi position key] :as index} ref-range f init]
  (check-index! kv index)
  (let [index-dbi (i/get-dbi kv dbi false)]
    (with-snapshot kv
      (fn [payload-dbi rtx]
        (let [cur (l/get-cursor index-dbi rtx)]
          (try
            (let [iterable (if (= position :key)
                             (l/iterate-kv index-dbi rtx cur ref-range :raw :raw)
                             (l/iterate-list index-dbi rtx cur
                                             [:closed key key] :raw
                                             ref-range :raw))]
              (with-open [^AutoCloseable iter (.iterator ^Iterable iterable)]
                (loop [acc init]
                  (if (.hasNext ^Iterator iter)
                    (let [row (.next ^Iterator iter)
                          k (b/read-buffer (l/k row) :raw)
                          v (b/read-buffer (l/v row) :raw)
                          entry (if (= position :key)
                                  {:reference k :associated v}
                                  {:reference v})
                          res (f acc entry payload-dbi rtx)]
                      (if (reduced? res) @res (recur res)))
                    acc))))
            (finally
              (if (l/read-only? rtx)
                (l/return-cursor index-dbi cur)
                (l/close-cursor index-dbi cur)))))))))

(defn- find-prefix [kv index type value prefix]
  (reduce-index
   kv index [:closed (reference prefix min-id) (reference prefix max-id)]
   (fn [_ entry payload-dbi rtx]
     (let [candidate ((:deserialize type)
                      (read-payload payload-dbi rtx
                                    (reference-id (:reference entry))))]
       (when (= value candidate)
         (reduced (assoc entry :value candidate)))))
   nil))

(defn find-value
  "Find the complete value within its order bucket, including staged writes.
  A prepared prefix avoids evaluating and encoding the same order key again."
  ([kv index type value]
   (find-value kv index type value
               (order-prefix type value (or (:max-size index) c/+max-key-size+))))
  ([kv index type value prefix]
   (find-prefix kv index type value prefix)))

(defn- require-writer! [kv]
  (when-not (and (l/writing? kv) (Thread/holdsLock (l/write-txn kv)))
    (raise "Custom payload changes require the coordinated write transaction"
           {:error :custom-type/write-transaction})))

(defn allocate-payload
  "Prepare an ID and payload rows inside a coordinated write transaction.
  Apply the returned rows with the owning indexes before allocating another ID.
  This does not mutate an allocation cache, so abort/retry cannot leak IDs."
  [kv ^bytes payload]
  (require-store! kv)
  (require-writer! kv)
  (when-not (bytes? payload)
    (raise "Custom payload must be raw bytes" {:error :custom-type/payload}))
  (let [last-id (or (i/get-value kv c/kv-info id-key :keyword :data) 0)]
    (when-not (and (int? last-id) (<= 0 (long last-id))
                   (< (long last-id) (dec max-id)))
      (raise "Custom value ID allocation is exhausted or invalid"
             {:error :custom-type/value-id :last-id last-id}))
    (let [id (inc (long last-id))]
      (when (some? (i/get-value kv c/custom-values id :id :raw))
        (raise "Custom value ID already has a payload"
               {:error :custom-type/value-id-conflict :id id}))
      {:id id
       :txs [(l/kv-tx :put c/kv-info id-key id :keyword :data)
             (l/kv-tx :put c/custom-values id payload :id :raw)]})))

(defn delete-payload-tx
  "Remove the payload owned by an entry; remove all its index references too."
  [ref]
  (l/kv-tx :del c/custom-values (reference-id ref) :id))

(defn transact!
  "Apply payload and index rows together. A storage failure poisons an outer
  transaction too, even if its caller catches the error. Resize retries remain
  owned by with-transaction-kv. Prepare user function results before calling."
  [kv txs]
  (require-writer! kv)
  (try (binding [l/*raw-kv?* true] (i/transact-kv kv txs))
       (catch Throwable e
         (when-not (l/resized? e) (i/abort-transact-kv kv))
         (throw e))))

(defn- put-index-tx [{:keys [dbi position key flags]} ref associated]
  (if (= position :key)
    (l/kv-tx :put dbi ref associated :raw :raw flags)
    (l/kv-tx :put dbi key ref :raw :raw flags)))

(defn- delete-index-tx [{:keys [dbi position key]} ref]
  (if (= position :key)
    (l/kv-tx :del dbi ref :raw)
    (l/kv-tx :del-list dbi key [ref] :raw :raw)))

(defn put-value!
  "Insert or replace one owning entry. Key indexes take raw associated bytes;
  item indexes pass nil. Equal complete values reuse their ID, while collisions
  allocate separate payloads. Both functions receive the same logical value."
  [kv index type value associated]
  (require-store! kv)
  (check-index! kv index)
  (when (and (= (:position index) :key) (not (bytes? associated)))
    (raise "Associated index value must be raw bytes"
           {:error :custom-type/index-value}))
  (let [prefix (order-prefix type value (or (:max-size index) c/+max-key-size+))
        payload ((:serialize type) value)]
    (l/with-transaction-kv [tx kv]
      (let [match (find-prefix tx index type value prefix)
            {:keys [id txs]}
            (if match
              (let [id (reference-id (:reference match))]
                {:id id :txs [(l/kv-tx :put c/custom-values id payload :id :raw)]})
              (allocate-payload tx payload))
            ref (if match (:reference match) (reference prefix id))]
        (transact! tx (conj txs (put-index-tx index ref associated)))
        ref))))

(defn delete-value!
  "Delete only the matching complete value and its payload. Return its entry."
  [kv index type value]
  (require-store! kv)
  (check-index! kv index)
  (let [prefix (order-prefix type value (or (:max-size index) c/+max-key-size+))]
    (l/with-transaction-kv [tx kv]
      (when-let [match (find-prefix tx index type value prefix)]
        (let [ref (:reference match)]
          (transact! tx [(delete-index-tx index ref) (delete-payload-tx ref)]))
        match))))

(defn clear-values!
  "Delete all owning entries in a key index, or one ordinary key's item list,
  with their payloads. No deserializer is needed for ownership cleanup."
  [kv index]
  (require-store! kv)
  (check-index! kv index)
  (l/with-transaction-kv [tx kv]
    (let [refs (reduce-index tx index [:all]
                             (fn [refs entry _ _] (conj refs (:reference entry)))
                             [])]
      (transact! tx (into [] (mapcat #(vector (delete-index-tx index %)
                                              (delete-payload-tx %))) refs))
      (count refs))))
