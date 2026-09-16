;; Copyright (c) Huahai Yang. All rights reserved.
;; Distributed under the Eclipse Public License 2.0.
(ns ^:no-doc datalevin.read-encode
  "Encode storage reads into Nippy without materializing their values.
  Borrowed storage buffers are consumed synchronously while their snapshot is open."
  (:require
   [datalevin.bits :as b]
   [datalevin.constants :as c]
   [taoensso.nippy :as nippy]
   [taoensso.nippy.impl :as impl]
   [taoensso.nippy.io :as nio]
   [taoensso.nippy.schema :as nschema])
  (:import
   [java.io DataInput]
   [java.nio ByteBuffer]))

(deftype ReadResult [write]
  ;; Nippy 3.9's buffer writer protocol lets this stand in for the logical
  ;; result. No ReadResult type tag or object reaches the receiving application.
  nio/IWriteTypedNoMeta
  (write-typed [_ buffer _] (write buffer))
  impl/INativeFreezable
  (native-freezable? [_] true))

(defn read-result
  "Create a synchronous result writer. Each invocation must acquire and release
  its own snapshot: Nippy and the transport may retry after buffer growth."
  [write]
  (ReadResult. write))

(defn write-value!
  "Write a materialized value in the enclosing message's Nippy cache scope."
  [^ByteBuffer out value]
  (nippy/freeze-to-bb! out value))

(def ^:private stored-data-id (impl/coerce-custom-type-id ::stored-data))

(nippy/extend-thaw ::stored-data
  [^DataInput in]
  (let [length (.readInt in)]
    (when (neg? length)
      (throw (ex-info "Invalid stored Nippy payload length" {:length length})))
    (let [bytes (byte-array length)]
      (.readFully in bytes)
      ;; Stored values have independent caches and may have legacy headers.
      ;; Preserve the current allowlist and native receiver bindings.
      (b/deserialize bytes))))

(defn- write-data! [^ByteBuffer out ^ByteBuffer value]
  (.put out (unchecked-byte nschema/id-prefixed-custom-md))
  (.putShort out (short stored-data-id))
  (.putInt out (.remaining value))
  (.put out value))

(defn- write-string! [^ByteBuffer out ^ByteBuffer value]
  (let [length (.remaining value)]
    (cond
      (zero? length) (.put out (unchecked-byte nschema/id-str-0))
      (<= length Byte/MAX_VALUE)
      (do (.put out (unchecked-byte nschema/id-str-sm_))
          (.put out (byte length)))
      (<= length Short/MAX_VALUE)
      (do (.put out (unchecked-byte nschema/id-str-md))
          (.putShort out (short length)))
      :else
      (do (.put out (unchecked-byte nschema/id-str-lg))
          (.putInt out length)))
    (.put out value)))

(defn- write-bytes! [^ByteBuffer out ^ByteBuffer value]
  (.put out (unchecked-byte nschema/id-byte-array-lg))
  (.putInt out (.remaining value))
  (.put out value))

(defn write-buffer!
  "Write one KV value. Data, strings and byte arrays skip value decoding;
  other codecs retain their existing decoding and result type."
  [^ByteBuffer out ^ByteBuffer value v-type]
  (case v-type
    (:data nil) (write-data! out value)
    :string (do (.get value) (write-string! out value))
    :bytes (do (.get value) (write-bytes! out value))
    :raw (write-bytes! out value)
    (write-value! out (b/read-buffer value v-type))))

(defn buffer-writer
  "Select the response encoder once for a prepared KV read."
  [v-type]
  (case v-type
    (:data nil) write-data!
    :string (fn [out ^ByteBuffer value] (.get value) (write-string! out value))
    :bytes (fn [out ^ByteBuffer value] (.get value) (write-bytes! out value))
    :raw write-bytes!
    (let [decode (b/buffer-reader v-type)]
      (fn [out value] (write-value! out (decode value))))))

(defn write-avg!
  "Write an inline Datalog value, excluding its attribute ID and index trailer.
  Custom references and giant values must be resolved by the storage caller."
  [^ByteBuffer out ^ByteBuffer value]
  (let [header (.get value 4)
        end (.limit value)]
    (case (int header)
      ;; The fixed/structured storage tags, matching bits/get-value*.
      (-64 -63 -16 -15 -14 -13 -12 -11 -10 -9 -8 -7 -5 -4 -3)
      (write-value! out (b/avg->inline-value value))
      ;; Strings and bytes keep their storage tag. Untyped data already starts
      ;; with its Nippy tag and is forwarded as an independently cached value.
      (try
        (.limit value (- end 2))
        (.position value 4)
        (write-buffer! out value (cond
                                  (= header c/type-string) :string
                                  (= header c/type-bytes) :bytes
                                  :else :data))
        (finally (.limit value end))))))

(defn start-map!
  "Reserve a Nippy map count, returning its starting position for finish-map!."
  ^long [^ByteBuffer out]
  (let [start (.position out)]
    (.put out (unchecked-byte nschema/id-map-lg))
    (.putInt out 0)
    start))

(defn finish-map!
  "Backpatch a pull result count. A pull with no selected fields returns nil."
  [^ByteBuffer out ^long start ^long count]
  (if (zero? count)
    (do (.position out (int start)) (write-value! out nil))
    (.putInt out (int (inc start)) (int count))))

(defn start-pair!
  "Write the Nippy header for a KV [key value] result."
  [^ByteBuffer out]
  (.put out (unchecked-byte nschema/id-vec-2)))

(def ^:private spillable-vector-id (impl/coerce-custom-type-id :spillable-vec))

(defn start-range!
  "Reserve the existing spillable-vector wire header for a range result.
  Return the count position, to be filled after consuming the cursor."
  ^long [^ByteBuffer out]
  (.put out (unchecked-byte nschema/id-prefixed-custom-md))
  (.putShort out (short spillable-vector-id))
  (let [position (.position out)]
    (.putLong out 0)
    position))

(defn finish-range!
  "Fill the range count, including zero for an empty result."
  [^ByteBuffer out ^long position ^long count]
  (.putLong out (int position) count))

(defn require-copy!
  "Abandon an unsent range frame so its caller can use batched copy-out."
  []
  (throw (ex-info "Range requires batched transfer" {::copy-required true})))

(defn copy-required?
  "Whether encoding stopped at the caller's batch threshold."
  [error]
  (true? (::copy-required (ex-data error))))
