;; Copyright (c) Huahai Yang. All rights reserved.
;; Distributed under the Eclipse Public License 2.0.
(ns ^:no-doc datalevin.client.copy
  "Bounded file reception over the existing copy-out framing."
  (:require [clojure.string :as str]
            [datalevin.client.request :as cr]
            [datalevin.util :as u])
  (:import [java.io File]
           [java.nio ByteBuffer]
           [java.nio.channels FileChannel SeekableByteChannel]
           [java.nio.file Files LinkOption OpenOption Path Paths StandardOpenOption]
           [java.security MessageDigest]
           [java.util Base64]
           [java.util.concurrent.atomic AtomicBoolean]))

(def ^:dynamic *receiver*
  "Caller-owned file sink, local to one synchronous copy request. Never sent on wire."
  nil)

(defn path ^Path [file]
  (cond
    (instance? Path file) file
    (instance? File file) (.toPath ^File file)
    :else (Paths/get ^String file (make-array String 0))))

(defn file-sink
  "Open an owned, seekable sink. The destination is an incomplete file."
  ^SeekableByteChannel [^Path file]
  (Files/newByteChannel file (into-array OpenOption [StandardOpenOption/CREATE
                                                    StandardOpenOption/WRITE
                                                    LinkOption/NOFOLLOW_LINKS])))

(defrecord Receiver [^SeekableByteChannel sink ^MessageDigest digest
                     ^AtomicBoolean closed state require-binary?])

(defn receiver
  [file {:keys [sink-factory require-binary?] :or {sink-factory file-sink}}]
  (let [digest (MessageDigest/getInstance "SHA-256")
        sink (sink-factory (path file))]
    (when-not (instance? SeekableByteChannel sink)
      (when (instance? java.io.Closeable sink) (.close ^java.io.Closeable sink))
      (throw (ex-info "Copy sink must be a SeekableByteChannel" {:error :copy/invalid-sink})))
    (->Receiver sink digest (AtomicBoolean.)
                (volatile! {:phase :new :bytes 0 :chunks 0 :tail "" :padded? false})
                (boolean require-binary?))))

(defn close!
  [^Receiver receiver]
  (when (.compareAndSet ^AtomicBoolean (:closed receiver) false true)
    (let [^SeekableByteChannel sink (:sink receiver)]
      (try (.close sink)
           (catch Throwable t
             ;; A failing close that leaves the resource open can be retried by
             ;; the enclosing owner without replacing the original failure.
             (.set ^AtomicBoolean (:closed receiver) (not (.isOpen sink)))
             (throw t))))))

(defn reset-before-request!
  "A pre-header retry starts at byte zero. A header forbids replay, including
  when a custom transport tries to resend after the receiver has failed."
  [^Receiver receiver]
  (when-not (#{:new :waiting-header} (:phase @(:state receiver)))
    (throw (ex-info "A copy cannot be replayed after its response header"
                    {:error :copy/retry-after-header})))
  (cr/check! :copy-sink-reset)
  (let [^SeekableByteChannel sink (:sink receiver)]
    (.truncate sink 0)
    (.position sink 0)
    (.reset ^MessageDigest (:digest receiver))
    (vreset! (:state receiver) {:phase :waiting-header :bytes 0 :chunks 0 :tail "" :padded? false})))

(defn completed? [^Receiver receiver] (= :complete (:phase @(:state receiver))))

(defn- fail! [code message data]
  (throw (ex-info message (assoc data :error code))))

(defn- write-bytes!
  [^Receiver receiver ^bytes bytes]
  (let [buffer (ByteBuffer/wrap bytes)
        ^SeekableByteChannel sink (:sink receiver)
        ^MessageDigest digest (:digest receiver)]
    (loop []
      (when (.hasRemaining buffer)
        (cr/check! :copy-sink-write)
        (let [offset (.position buffer)
              written (.write sink buffer)]
          (when-not (and (pos? written) (= written (- (.position buffer) offset)))
            (fail! :copy/sink-stalled "Copy sink made no progress" {:written written}))
          (.update digest bytes offset written)
          (vswap! (:state receiver) update :bytes #(Math/addExact (long %) (long written)))
          (recur))))))

(defn- decode-base64!
  [^Receiver receiver ^String text]
  (when (seq text)
    (let [decoded (try (.decode (Base64/getDecoder) text)
                       (catch IllegalArgumentException e
                         (throw (ex-info "Invalid legacy base64 copy data"
                                         {:error :copy/invalid-base64} e))))]
      (write-bytes! receiver decoded))))

(defn- legacy-chunk!
  [^Receiver receiver chunk]
  (when-not (string? chunk)
    (fail! :copy/invalid-chunk "Expected a base64 string in legacy copy" {}))
  (let [{:keys [tail padded?]} @(:state receiver)]
    (when (and padded? (seq chunk))
      (fail! :copy/invalid-base64 "Data follows base64 padding" {}))
    (let [combined (str tail chunk)
          length (* 4 (quot (count combined) 4))
          prefix (subs combined 0 length)
          tail (subs combined length)
          padded? (or padded? (str/includes? prefix "="))]
      (when (and padded? (seq tail))
        (fail! :copy/invalid-base64 "Data follows base64 padding" {}))
      (decode-base64! receiver prefix)
      (vswap! (:state receiver) assoc :tail tail :padded? padded?))))

(defn- valid-count? [n]
  (and (integer? n) (<= 0 n Long/MAX_VALUE)))

(defn- finish!
  [^Receiver receiver header done binary?]
  (cr/check! :copy-finish)
  (when-not binary?
    (decode-base64! receiver (:tail @(:state receiver)))
    (vswap! (:state receiver) assoc :tail ""))
  (let [{written :bytes chunks :chunks} @(:state receiver)
        checksum (u/hexify (.digest ^MessageDigest (:digest receiver)))
        expected (:checksum done)
        expected-bytes (:bytes done)
        algorithm (or (:checksum-algo done) (:checksum-algo header))]
    (when (and algorithm (not= :sha-256 algorithm))
      (fail! :copy/unsupported-checksum "Unsupported copy checksum algorithm" {:checksum-algo algorithm}))
    (when (and binary? (not (and (= :sha-256 algorithm) (valid-count? expected-bytes) (string? expected)
                                  (re-matches #"(?i)[0-9a-f]{64}" expected))))
      (fail! :copy/missing-integrity "Binary copy requires SHA-256 and byte-count metadata" {}))
    (when (and (some? expected-bytes)
               (or (not (valid-count? expected-bytes)) (not= expected-bytes written)))
      (fail! :copy/size-mismatch "Copy size mismatch"
             {:expected-bytes expected-bytes :actual-bytes written}))
    (when (and expected (or (not (string? expected)) (not (.equalsIgnoreCase ^String checksum expected))))
      (fail! :copy/checksum-mismatch "Copy checksum mismatch"
             {:expected-checksum expected :actual-checksum checksum}))
    (when (and binary? (contains? done :chunks)
               (or (not (valid-count? (:chunks done))) (not= chunks (:chunks done))))
      (fail! :copy/chunk-count-mismatch "Copy chunk count mismatch"
             {:expected-chunks (:chunks done) :actual-chunks chunks}))
    (when (and (some? (:copy-format done))
               (not= (:copy-format done) (:copy-format header)))
      (fail! :copy/format-mismatch "Copy format changed during reception" {}))
    ;; Validate before forcing/closing the destination. A sink failure remains
    ;; inside the borrowed connection's transfer scope and cannot trigger replay.
    (when (instance? FileChannel (:sink receiver)) (.force ^FileChannel (:sink receiver) true))
    (close! receiver)
    (cr/check! :copy-close)
    (vswap! (:state receiver) assoc :phase :complete)
    (merge (dissoc header :type :copy-result-type :result)
           (dissoc done :type :result)
           {:type :command-complete :result nil :bytes written :chunks chunks
            :checksum-algo :sha-256 :checksum checksum
            :verified? (boolean (and expected (some? expected-bytes)))})))

(defn receive!
  "Write one decoded frame at a time. No chunk collection is retained. Legacy
  base64 keeps at most three undecoded characters between frames."
  [^Receiver receiver receive-message header]
  (when-not (= :waiting-header (:phase @(:state receiver)))
    (fail! :copy/invalid-state "Copy sink was not prepared for reception" {}))
  (vswap! (:state receiver) assoc :phase :receiving)
  (try
    (cr/check! :copy-header)
    (let [binary? (= :binary-chunks (:copy-format header))]
      (when (and (:copy-format header) (not binary?))
        (fail! :copy/unsupported-format "Unsupported copy format" {:copy-format (:copy-format header)}))
      (when (and (:require-binary? receiver)
                 (or (not binary?) (not= :sha-256 (:checksum-algo header))))
        (fail! :copy/unsupported-format "Copy requires binary chunks with SHA-256" {}))
      (when (and (:checksum-algo header) (not= :sha-256 (:checksum-algo header)))
        (fail! :copy/unsupported-checksum "Unsupported copy checksum algorithm"
               {:checksum-algo (:checksum-algo header)}))
      (loop []
        (cr/check! :copy-receive)
        (let [message (receive-message)]
          (if (map? message)
            (if (= :copy-done (:type message))
              (finish! receiver header message binary?)
              (fail! :copy/server-error "Server error during copy" {:response message}))
            (do
              (when-not (sequential? message)
                (fail! :copy/invalid-chunk "Expected a batch of copy chunks" {}))
              (doseq [chunk message]
                (if binary?
                  (do
                    (when-not (bytes? chunk)
                      (fail! :copy/invalid-chunk "Expected a binary copy chunk" {}))
                    (write-bytes! receiver chunk))
                  (legacy-chunk! receiver chunk))
                (vswap! (:state receiver) update :chunks inc))
              (recur))))))
    (catch Throwable failure
      (try (close! receiver)
           (catch Throwable cleanup
             (when-not (identical? failure cleanup) (.addSuppressed failure cleanup))))
      (throw failure))))
