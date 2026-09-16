;;
;; Copyright (c) Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns ^:no-doc datalevin.protocol
  "Shared code of client/server"
  (:require
   [datalevin.bits :as b]
   [datalevin.buffer :as bf]
   [datalevin.constants :as c]
   [datalevin.datom :as d]
   [datalevin.native-value :as nv]
   [datalevin.protocol.context :as context]
   [datalevin.read-encode]
   [datalevin.util :refer [raise]]
   [datalevin.spill :as sp]
   [taoensso.nippy :as nippy]
   [taoensso.nippy.impl :as nippy-impl]
   [cognitect.transit :as transit])
  (:import
   [java.io ByteArrayInputStream ByteArrayOutputStream EOFException]
   [java.nio ByteBuffer BufferOverflowException]
   [java.util UUID]
   [java.nio.channels SocketChannel Selector SelectionKey]
   [datalevin.io ByteBufferInputStream ByteBufferOutputStream]
   [datalevin.spill SpillableVector]
   [datalevin.datom Datom]
   [com.github.luben.zstd Zstd]))

;; en/decode

(def transit-read-handlers
  {"datalevin/Datom" (transit/read-handler d/datom-from-reader)
   "datalevin/SpillableVector"       (transit/read-handler sp/new-spillable-vector)})

(def transit-write-handlers
  {Datom           (transit/write-handler
                     "datalevin/Datom"
                     (fn [^Datom d] [(.-e d) (.-a d) (.-v d) (.-tx d)]))
   SpillableVector (transit/write-handler
                     "datalevin/SpillableVector"
                     (fn [v] (into [] v)))})

(declare write-transit-bytes)

(defn ^:no-doc local-wire-capabilities
  []
  {:compression           [:zstd]
   :storage-read?         true
   :prepared-read?        true
   :prepared-query?       true
   :compression-threshold (long c/*wire-compression-threshold*)})

(defn ^:no-doc default-wire-opts
  []
  {:compression           nil
   :compression-threshold (long c/*wire-compression-threshold*)
   :compression-level     (int c/*wire-compression-level*)})

(defn- peer-supports-zstd?
  [peer-capabilities]
  (when (map? peer-capabilities)
    (contains? (set (:compression peer-capabilities)) :zstd)))

(defn ^:no-doc negotiate-wire-opts
  [peer-capabilities]
  (cond-> (default-wire-opts)
    (peer-supports-zstd? peer-capabilities)
    (assoc :compression :zstd)
    (true? (:storage-read? peer-capabilities))
    (assoc :storage-read? true)
    (true? (:prepared-read? peer-capabilities))
    (assoc :prepared-read? true)
    (true? (:prepared-query? peer-capabilities))
    (assoc :prepared-query? true)))

(defn- fmt-int ^long [fmt]
  (bit-and (long fmt) 0xFF))

(defn- fmt-code ^long [fmt]
  (bit-and (fmt-int fmt) c/message-format-mask))

(defn- zstd-compressed?
  [fmt]
  (pos? (bit-and (fmt-int fmt) c/message-flag-zstd)))

(defn- serialize-value
  ^bytes [fmt msg]
  (case (short fmt)
    1 (write-transit-bytes msg)
    ;; Establish the wire and Java serialization contexts in one binding.
    ;; bits/serialize can then use the already installed allowlist.
    2 (context/with-wire-bindings :freeze (b/serialization-allowlist)
        (b/serialize msg))
    (raise "Unknown wire message format"
             {:format fmt
              :format-code (fmt-code fmt)})))

(defn- maybe-pack-zstd
  [fmt ^bytes payload wire-opts]
  (let [{:keys [compression compression-threshold compression-level]
         :or {compression-threshold c/*wire-compression-threshold*
              compression-level c/*wire-compression-level*}}
        wire-opts
        threshold ^long (long compression-threshold)]
    (if (and (= compression :zstd)
             (<= threshold (alength payload)))
      (let [compressed ^bytes (Zstd/compress payload (int compression-level))
            packed-len        (+ 4 (alength compressed))]
        (if (< packed-len (alength payload))
          (let [packed (byte-array packed-len)
                bb     (ByteBuffer/wrap packed)]
            (.putInt bb (alength payload))
            (.put bb compressed)
            [(unchecked-byte (bit-or (fmt-int fmt) c/message-flag-zstd))
             packed])
          [fmt payload]))
      [fmt payload])))

(defn- unpack-zstd
  ^bytes [^bytes payload]
  (when (< (alength payload) 4)
    (raise "Wire message compression payload is corrupted"
             {:reason :missing-uncompressed-length
              :payload-bytes (alength payload)}))
  (let [bb              (ByteBuffer/wrap payload)
        uncompressed-len (.getInt bb)]
    (when (neg? uncompressed-len)
      (raise "Wire message compression payload is corrupted"
               {:reason :negative-uncompressed-length
                :uncompressed-length uncompressed-len}))
    (let [compressed (byte-array (.remaining bb))]
      (.get bb compressed)
      (let [raw ^bytes (Zstd/decompress compressed (long uncompressed-len))]
        (when-not (= uncompressed-len (alength raw))
          (raise "Wire message decompression length mismatch"
                   {:expected uncompressed-len
                    :actual   (alength raw)}))
        raw))))

(defn read-transit-string
  "Read a transit+json encoded string into a Clojure value"
  [^String s]
  (try
    (transit/read
      (transit/reader
        (ByteArrayInputStream. (.getBytes s "utf-8")) :json
        {:handlers transit-read-handlers}))
    (catch Exception e
      (raise "Unable to read transit:" e {:string s}))))

(defn write-transit-string
  "Write a Clojure value as a transit+json encoded string"
  [v]
  (try
    (let [baos (ByteArrayOutputStream.)]
      (transit/write
        (transit/writer baos :json {:handlers transit-write-handlers}) v)
      (.toString baos "utf-8"))
    (catch Exception e
      (raise "Unable to write transit:" e {:value v}))))

(defn- thaw-nippy-bf
  [^ByteBuffer bf]
  (let [pos (.position bf)]
    (try
      ;; Use the same read cache as fast-thaw. The general with-cache eagerly
      ;; allocates writer maps that a decoder does not need (Nippy 3.9).
      (context/with-cache (nippy-impl/new-thaw-cache-state)
        (nippy/thaw-from-bb! bf))
      (catch Exception e
        ;; Native reader failures must not be mistaken for legacy headers.
        (when (nv/decoding-error e) (throw e))
        (.position bf pos)
        (nippy/thaw (b/get-bytes bf))))))

(defn read-nippy-bf
  "Read one Nippy value from the buffer, with legacy header fallback."
  [^ByteBuffer bf]
  (let [allowlist (b/serialization-allowlist)]
    (try
      (if (and nv/*wire-native-value*
               (identical? allowlist nippy/*thaw-serializable-allowlist*))
        (thaw-nippy-bf bf)
        (context/with-wire-bindings :thaw allowlist
          (thaw-nippy-bf bf)))
      (catch Exception e
        (throw (or (nv/decoding-error e) e))))))

(defn read-transit-bf
  "Read from a ByteBuffer containing transit+json encoded bytes,
  return a Clojure value. Consumes the entire buffer"
  [^ByteBuffer bf]
  (transit/read (transit/reader (ByteBufferInputStream. bf)
                                :json
                                {:handlers transit-read-handlers})))

(defn- freeze-nippy-bf
  [^ByteBuffer bf v]
  (try
    (context/with-cache (nippy-impl/new-cache-state)
      (nippy/freeze-to-bb! bf v))
    (catch EOFException e
      ;; Nippy 3.9 translates buffer overflow to EOFException without a cause.
      ;; Only translate that specific error; custom serializers can throw EOF.
      (if (some-> (.getMessage e)
                  (.startsWith "ByteBuffer overflow while freezing:"))
        (throw (doto (BufferOverflowException.) (.initCause e)))
        (throw e)))))

(defn write-nippy-bf
  "Write a Clojure value as nippy encoded bytes into a ByteBuffer"
  [^ByteBuffer bf v]
  (when (instance? java.lang.Class v)
    (raise "Unfreezable type: java.lang.Class" {}))
  (let [allowlist (b/serialization-allowlist)]
    (if (and nv/*wire-native-value*
             (identical? allowlist nippy/*freeze-serializable-allowlist*))
      (freeze-nippy-bf bf v)
      (context/with-wire-bindings :freeze allowlist
        (freeze-nippy-bf bf v)))))

(defn write-transit-bf
  "Write a Clojure value as transit+json encoded bytes into a ByteBuffer"
  [^ByteBuffer bf v]
  (transit/write (transit/writer (ByteBufferOutputStream. bf)
                                 :json
                                 {:handlers transit-write-handlers})
                 v))

(defn- write-message-bytes-bf
  [^ByteBuffer bf msg fmt wire-opts]
  (let [payload ^bytes (serialize-value fmt msg)
        [fmt' body]   (maybe-pack-zstd fmt payload wire-opts)]
    (.put bf ^byte (unchecked-byte fmt'))
    (.putInt bf (int (+ c/message-header-size (alength ^bytes body))))
    (.put bf ^bytes body)))

(defn- compress-message-bf!
  [^ByteBuffer bf ^long start wire-opts]
  (let [payload-start (+ start c/message-header-size)
        end           (.position bf)
        size          (- end payload-start)]
    (when (and (= (:compression wire-opts) :zstd)
               (<= (long (get wire-opts :compression-threshold
                              c/*wire-compression-threshold*)) size))
      (let [payload (b/get-bytes (doto (.duplicate bf)
                                   (.limit end)
                                   (.position payload-start)))
            [fmt body] (maybe-pack-zstd c/message-format-nippy payload wire-opts)]
        (when (zstd-compressed? fmt)
          (.put bf (int start) (unchecked-byte fmt))
          (.position bf payload-start)
          (.put bf ^bytes body))))))

(defn write-message-bf
  "Write a message to a ByteBuffer. First byte is format, then four bytes
  length of the whole message (include header), followed by message value"
  ([bf msg]
   (write-message-bf bf msg c/message-format-nippy nil))
  ([bf msg fmt]
   (write-message-bf bf msg fmt nil))
  ([^ByteBuffer bf msg fmt wire-opts]
   (let [start (.position bf)]
     (try
       (if (= fmt c/message-format-nippy)
         (try
           (.put bf (unchecked-byte fmt))
           (.putInt bf 0)
           (write-nippy-bf bf msg)
           (compress-message-bf! bf start wire-opts)
           (.putInt bf (int (inc start)) (int (- (.position bf) start)))
           (catch BufferOverflowException e
             (.position bf start)
             ;; A compressed frame may fit even when the raw value does not.
             (if (= (:compression wire-opts) :zstd)
               (write-message-bytes-bf bf msg fmt wire-opts)
               (throw e))))
         (write-message-bytes-bf bf msg fmt wire-opts))
       (catch Throwable t
         (.position bf start)
         (throw t))))))

(defn read-transit-bytes
  "Read transit+json encoded bytes into a Clojure value"
  [^bytes bs]
  (transit/read (transit/reader (ByteArrayInputStream. bs)
                                :json
                                {:handlers transit-read-handlers})))

(defn write-transit-bytes
  "Write a Clojure value as transit+json encoded bytes"
  [v]
  (let [baos (ByteArrayOutputStream.)]
    (transit/write (transit/writer baos :json
                                   {:handlers transit-write-handlers})
                   v)
    (.toByteArray baos)))

(defn read-value
  ([fmt bs]
   (read-value fmt bs nil))
  ([fmt bs wire-opts]
   (let [code      (fmt-code fmt)
         compressed? (zstd-compressed? fmt)
         payload   (if compressed?
                     (do
                       (when-not (= (:compression wire-opts) :zstd)
                         (raise "Received compressed wire message without negotiated support"
                                  {:compression-flag :zstd
                                   :wire-opts        wire-opts}))
                       (unpack-zstd (if (instance? ByteBuffer bs)
                                      (b/get-bytes bs)
                                      bs)))
                     bs)]
     (case (short code)
       1 (if (instance? ByteBuffer payload)
           (read-transit-bf payload)
           (read-transit-bytes payload))
       2 (read-nippy-bf (if (instance? ByteBuffer payload)
                         payload
                         (ByteBuffer/wrap payload)))
       (raise "Unknown wire message format"
                {:format fmt
                 :format-code code})))))

(deftype ^:no-doc RequestDecoder [native? bindings])

(defn ^:no-doc request-decoder
  "Create native-value detection state owned by one connection's read loop.
  It retains no request payloads and must not be used concurrently."
  []
  (let [native? (volatile! false)
        reader (fn [_ _]
                 (vreset! native? true)
                 (UUID/randomUUID))]
    (RequestDecoder. native? {#'nv/*wire-reader* reader
                             #'nv/*wire-native-value* true})))

(defn read-request
  "Read request routing fields without running native deserializers. Requests
  containing native values retain their bytes until authorized dispatch."
  ([fmt bs wire-opts]
   (read-request fmt bs wire-opts (request-decoder)))
  ([fmt bs wire-opts ^RequestDecoder decoder]
   (let [native? (.-native? decoder)
         pos     (when (instance? ByteBuffer bs) (.position ^ByteBuffer bs))]
     (vreset! native? false)
     (let [message (do
                     (clojure.lang.Var/pushThreadBindings (.-bindings decoder))
                     (try (read-value fmt bs wire-opts)
                          (finally (clojure.lang.Var/popThreadBindings))))]
       (when-not (map? message)
         (raise "Expected a request map" {}))
       ;; Only metadata supplied by this decoder can defer native decoding.
       (let [message (if (contains? (meta message) ::native-request)
                       (vary-meta message dissoc ::native-request)
                       message)]
         (if @native?
           ;; The connection will compact/reuse its buffer before dispatch.
           ;; Only deferred native requests need an owned copy of the frame.
           (let [payload (if (instance? ByteBuffer bs)
                           (b/get-bytes (doto ^ByteBuffer bs (.position (int pos))))
                           bs)]
             (vary-meta message assoc ::native-request [fmt payload wire-opts]))
           message))))))

(defn native-request?
  "True when read-request retained native values for authorized decoding."
  [message]
  (boolean (::native-request (meta message))))

(defn resolve-native-request
  "Rebuild native requests with the bound receiver before constructing keys,
  sets, or query inputs. Ordinary requests require no second decode."
  [message]
  (if-let [[fmt bs opts] (::native-request (meta message))]
    (with-meta (merge message (read-value fmt bs opts))
      (dissoc (meta message) ::native-request))
    message))

(defn send-ch
  "Send to socket channel, return the number of bytes sent. return -1 if
  something is wrong"
  [^SocketChannel ch ^ByteBuffer bf]
  (try
    (.write ch bf)
    (catch Exception e
      ;; (st/print-stack-trace e)
      -1)))

(defn- open-selector
  ^Selector [^SocketChannel ch ^long ops]
  (let [selector (Selector/open)]
    (try
      (.register ch selector (int ops))
      selector
      (catch Throwable t
        (try (.close selector)
             (catch Throwable close-error
               (when-not (identical? t close-error)
                 (.addSuppressed t close-error))))
        (throw t)))))

(defn send-all
  "Send all data in buffer to channel, will block if channel is busy.
  Close the channel and raise exception if something is wrong"
  [^SocketChannel ch ^ByteBuffer bf ]
  (let [non-blocking? (not (.isBlocking ch))
        selector      (volatile! nil)]
    (try
      (loop []
        (when (.hasRemaining bf)
          (let [n (long (send-ch ch bf))]
            (cond
              (== n -1)
              (do (.close ch)
                  (raise "Socket channel is closed." {}))

              (> n 0)
              (recur)

              non-blocking?
              (let [^Selector sel (or @selector
                                      (let [s (open-selector ch SelectionKey/OP_WRITE)]
                                        (vreset! selector s)
                                        s))]
                ;; Avoid busy-spinning on non-blocking sockets under backpressure.
                (.select sel)
                (when (.isInterrupted (Thread/currentThread))
                  ;; A partial frame cannot be reused for another request.
                  (.close ch)
                  (throw (InterruptedException. "Interrupted while sending a socket message")))
                (.clear (.selectedKeys sel))
                (recur))

              :else
              (do
                (Thread/yield)
                (recur))))))
      (finally
        (when-let [^Selector sel @selector]
          (.close sel))))))

(defn write-message-owned
  "Write using an exclusively owned buffer. The caller supplies synchronization."
  [^SocketChannel ch ^ByteBuffer bf msg wire-opts]
  (.clear bf)
  (write-message-bf bf msg c/message-format-nippy wire-opts)
  (.flip bf)
  (send-all ch bf))

(defn write-message-blocking
  "Write a message in blocking mode"
  ([^SocketChannel ch ^ByteBuffer bf msg]
   (write-message-blocking ch bf msg nil))
  ([^SocketChannel ch ^ByteBuffer bf msg wire-opts]
   (locking bf
     (write-message-owned ch bf msg wire-opts))))

(defn receive-one-message!
  "Consume a frame using a connection-owned buffer volatile. Return the message
  or nil for an incomplete frame, and publish any buffer growth to buffer-v."
  [buffer-v wire-opts]
  (let [^ByteBuffer read-bf @buffer-v
        pos (.position read-bf)]
    (when (>= pos c/message-header-size)
      (.flip read-bf)
      (let [available (.limit read-bf)
            fmt       (.get read-bf)
            length    ^int (.getInt read-bf)
            _         (when (< length c/message-header-size)
                        (raise "Message corruption: length is less than header size"
                               {:length length}))
            read-bf   (if (< (.capacity read-bf) length)
                        (let [^ByteBuffer bf
                              (ByteBuffer/allocateDirect
                                (* ^long c/+buffer-grow-factor+ length))]
                          (.rewind read-bf)
                          (bf/buffer-transfer read-bf bf)
                          (vreset! buffer-v bf)
                          bf)
                        read-bf)]
        (if (< available length)
          (do (doto read-bf
                (.limit (.capacity read-bf))
                (.position pos))
              nil)
          (try
            ;; The decoder cannot read into a following frame.
            (.limit read-bf length)
            (read-value fmt read-bf wire-opts)
            (finally
              (.limit read-bf available)
              (.position read-bf length)
              (if (= available length)
                (.clear read-bf)
                (.compact read-bf)))))))))

(defn receive-one-message
  "Consume a frame and return [message buffer], with nil for an incomplete frame.
  Connections use receive-one-message! to reuse the buffer holder."
  ([read-bf] (receive-one-message read-bf nil))
  ([read-bf wire-opts]
   (let [buffer-v (volatile! read-bf)
         message (receive-one-message! buffer-v wire-opts)]
     [message @buffer-v])))

(defn read-ch
  "Read from the socket channel, return the number of bytes read. Return -1
  if something is wrong"
  [^SocketChannel ch ^ByteBuffer bf]
  (try
    (.read ch bf)
    (catch Exception e
      ;; (st/print-stack-trace e)
      -1)))

(defn- await-read-ready!
  [^SocketChannel ch selector-v ^long deadline-ms ^long timeout-ms]
  (let [^Selector sel (or @selector-v
                          (locking selector-v
                            (or @selector-v
                                (let [s (open-selector ch SelectionKey/OP_READ)]
                                  (vreset! selector-v s)
                                  s))))]
    (loop []
      (when (.isInterrupted (Thread/currentThread))
        (throw (InterruptedException. "Interrupted while receiving a socket message")))
      (let [remaining-ms (- deadline-ms (System/currentTimeMillis))]
        (when-not (pos? remaining-ms)
          (raise "Socket channel receive timed out."
                   {:error :socket/timeout
                    :timeout-ms timeout-ms}))
        (if (pos? (.select sel remaining-ms))
          (.clear (.selectedKeys sel))
          (recur))))))

(defn receive-ch!
  "Receive one message from channel and put it in buffer, will block
  until one full message is received. When buffer is too small for a
  message, a new buffer is published to buffer-v. Return the message. The selector
  volatile belongs to the caller, which must keep the channel nonblocking and
  close the selector under the volatile's lock when closing the connection."
  [^SocketChannel ch buffer-v wire-opts timeout-ms read-selector]
  (let [timed?      (some? timeout-ms)
        timeout-ms  (if timed? (long (max 1 (long timeout-ms))) 0)
        deadline-ms (if timed?
                      (long (+ (System/currentTimeMillis) timeout-ms))
                      0)
        blocking?   (and timed? (.isBlocking ch))
        selector-v  (or read-selector (volatile! nil))]
    (when (and read-selector (.isBlocking ch))
      (raise "A reusable receive selector requires a nonblocking channel" {}))
    (try
      (when blocking? (.configureBlocking ch false))
      (loop []
        (if-let [msg (receive-one-message! buffer-v wire-opts)]
          msg
          (let [^int readn (read-ch ch @buffer-v)]
            (cond
              (> readn 0)  (recur)
              (= readn 0)  (do
                             (when timed?
                               (await-read-ready! ch selector-v
                                                  deadline-ms timeout-ms))
                             (recur))
              (= readn -1) (do (.close ch)
                               (raise "Socket channel is closed." {}))))))
      (finally
        (when-not read-selector
          (when-let [^Selector sel @selector-v]
            (.close sel)))
        (when (and blocking? (.isOpen ch))
          (.configureBlocking ch true))))))

(defn receive-ch
  "Receive a message and return [message buffer]. For repeated reads, use a
  connection-owned buffer holder with receive-ch! instead."
  ([ch bf] (receive-ch ch bf nil))
  ([ch bf wire-opts] (receive-ch ch bf wire-opts nil))
  ([ch bf wire-opts timeout-ms] (receive-ch ch bf wire-opts timeout-ms nil))
  ([ch bf wire-opts timeout-ms read-selector]
   (let [buffer-v (volatile! bf)
         message (receive-ch! ch buffer-v wire-opts timeout-ms read-selector)]
     [message @buffer-v])))

(defn extract-message
  "Extract a complete frame. The optional msg-reader decodes (fmt, buffer)
  while the buffer is limited to the payload. It must not retain buffer views.
  Compact before calling msg-handler with (fmt, decoded message), so handlers
  can reuse the read buffer for bulk transfers. Without a reader, deliver an
  owned byte array. Return true when a complete frame was extracted."
  ([read-bf msg-handler]
   (extract-message read-bf (fn [_ buffer] (b/get-bytes buffer)) msg-handler))
  ([^ByteBuffer read-bf msg-reader msg-handler]
   (let [pos (.position read-bf)]
     (when (>= pos c/message-header-size)
       (.flip read-bf)
       (let [available (.limit read-bf)
             fmt       (.get read-bf)
             length    (.getInt read-bf)]
         (when (< length c/message-header-size)
           (raise "Message corruption: length is less than header size"
                    {:length length}))
         (if (< available length)
           (do
             (doto read-bf
               (.limit (.capacity read-bf))
               (.position pos))
             false)
           (let [message (try
                           (.limit read-bf length)
                           (msg-reader fmt read-bf)
                           (finally
                             (.limit read-bf available)
                             (.position read-bf length)
                             (.compact read-bf)))]
             (msg-handler fmt message)
             true)))))))
