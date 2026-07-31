(ns datalevin.hako-codec
  "hako-backed replacement for nippy's freeze-to-out! / thaw-from-in!.

  Matches nippy's signature so call sites can swap import + fn refs.
  Uses a thread-local reusable Writer + Reader to amortize arena
  setup across calls. The `DataOutput` / `DataInput` boundary
  forces an intermediate `byte[]` copy — genuinely unavoidable
  without refactoring callers to `MemorySegment`.

  Wire format per call:
    `<u32 compressed-length LE><zstd frame containing hako-encoded payload>`.
  Length-prefix lets multiple values interleave in the same stream.
  Zstd compression matches nippy's default `freeze` behavior — dump
  files stay compact. Hot-path LMDB records use uncompressed
  `put-data-into-buffer!` (below).

  Phase 2 additions: `put-data-into-buffer!` bypasses the byte[]
  bounce for LMDB writes — hako's arena-backed MemorySegment gets
  copied directly into the caller's ByteBuffer, no heap intermediate.

  Thread-local cleanup: long-running hosts with pooled threads have
  nothing to do — the ThreadLocals live for the thread's lifetime.
  Short-lived-thread hosts should call `close-thread-locals!` at the
  end of each thread's work to release the confined `Arena` inside
  the Writer and drop the scratch buffers."
  (:require [s-exp.hako :as hako])
  (:import (com.github.luben.zstd Zstd)
           (com.s_exp.hako Reader Writer)
           (java.io DataInput DataOutput)
           (java.lang.foreign MemorySegment ValueLayout)
           (java.nio ByteBuffer)))

(set! *warn-on-reflection* true)

;; Cap the scratch buffer so a one-off huge dump doesn't pin the
;; ThreadLocal at that size for the JVM lifetime. Above this, allocate
;; a one-shot byte[] that the GC can reclaim.
(def ^:private ^:const scratch-max-bytes (* 1024 1024))

(def ^:private ^:const zstd-level 3)

;; Hard cap on `thaw-from-in!` frame size — protects against a hostile
;; or corrupt length prefix triggering an OOM allocation. 256 MiB is
;; well above any realistic dump entry.
(def ^:private ^:const max-frame-bytes (* 256 1024 1024))

(def ^:private ^ThreadLocal tl-writer
  (proxy [ThreadLocal] []
    (initialValue [] (hako/writer 4096))))

(def ^:private ^ThreadLocal tl-reader
  (proxy [ThreadLocal] []
    (initialValue [] (hako/reader (byte-array 0)))))

(def ^:private ^ThreadLocal tl-scratch
  (proxy [ThreadLocal] []
    (initialValue [] (byte-array 4096))))

(def ^:private ^ThreadLocal tl-compressed-scratch
  (proxy [ThreadLocal] []
    (initialValue [] (byte-array 4096))))

(defn- ^bytes ensure-scratch [^long n]
  (let [buf ^bytes (.get tl-scratch)]
    (cond
      (>= (alength buf) n) buf
      ;; Above the cap, return a one-shot buffer without storing it.
      (> n scratch-max-bytes) (byte-array n)
      :else
      (let [grown (byte-array (max n (* 2 (alength buf))))]
        (.set tl-scratch grown)
        grown))))

(defn- ^bytes ensure-compressed-scratch [^long n]
  (let [buf ^bytes (.get tl-compressed-scratch)]
    (cond
      (>= (alength buf) n) buf
      (> n scratch-max-bytes) (byte-array n)
      :else
      (let [grown (byte-array (max n (* 2 (alength buf))))]
        (.set tl-compressed-scratch grown)
        grown))))

(defn close-thread-locals!
  "Release the current thread's Writer arena and drop scratch buffers.
  Call at the end of a short-lived thread's work — long-running pooled
  threads don't need this. Idempotent; safe to call more than once."
  []
  (when-let [wr ^Writer (.get tl-writer)]
    (.close wr))
  (.remove tl-writer)
  (.remove tl-reader)
  (.remove tl-scratch)
  (.remove tl-compressed-scratch))

(defn freeze-to-out!
  "Encode `v` with hako, zstd-compress the payload, and write
  `<u32 compressed-length><zstd frame>` to `out`. Compression matches
  nippy's default `freeze` behavior — dump files stay compact."
  [^DataOutput out v]
  (let [wr ^Writer (.get tl-writer)
        seg (hako/encode-into! wr v)
        n (.byteSize seg)
        _ (when (> n Integer/MAX_VALUE)
            (throw (IllegalStateException.
                    (str "hako-codec: payload exceeds Integer/MAX_VALUE: " n))))
        raw ^bytes (ensure-scratch n)
        _ (MemorySegment/copy seg ValueLayout/JAVA_BYTE 0 raw 0 n)
        bound (Zstd/compressBound n)
        _ (when (> bound Integer/MAX_VALUE)
            (throw (IllegalStateException.
                    (str "hako-codec: compress bound exceeds Integer/MAX_VALUE: "
                         bound))))
        cbuf ^bytes (ensure-compressed-scratch bound)
        cn (Zstd/compressByteArray cbuf 0 (int bound)
                                   raw 0 (int n)
                                   (int zstd-level))
        _ (when (Zstd/isError cn)
            (throw (IllegalStateException.
                    (str "hako-codec: zstd compress error: "
                         (Zstd/getErrorName cn)))))]
    (.writeInt out (int cn))
    (.write out cbuf 0 (int cn))))

(defn thaw-from-in!
  "Read a `<u32 compressed-length><zstd frame>` from `in`, decompress,
  and hako-decode."
  [^DataInput in]
  (let [cn (.readInt in)
        _ (when (or (neg? cn) (> cn max-frame-bytes))
            (throw (IllegalStateException.
                    (str "hako-codec: frame length out of range: " cn))))
        compressed ^bytes (ensure-compressed-scratch cn)
        _ (.readFully in compressed 0 cn)
        orig-size (Zstd/getFrameContentSize compressed)
        _ (when (or (neg? orig-size)
                    (> orig-size Integer/MAX_VALUE)
                    (> orig-size max-frame-bytes))
            (throw (IllegalStateException.
                    (str "hako-codec: zstd frame content size out of range: "
                         orig-size))))
        raw ^bytes (ensure-scratch orig-size)
        actual (Zstd/decompressByteArray raw 0 (int orig-size)
                                         compressed 0 cn)
        _ (when (Zstd/isError actual)
            (throw (IllegalStateException.
                    (str "hako-codec: zstd decompress error: "
                         (Zstd/getErrorName actual)))))
        _ (when (not= actual orig-size)
            (throw (IllegalStateException.
                    (str "hako-codec: zstd decompressed "
                         actual " bytes, expected " orig-size))))
        rd ^Reader (.get tl-reader)
        seg (MemorySegment/ofArray raw)]
    (hako/decode-into! rd (.asSlice ^MemorySegment seg 0 orig-size)
                       {:cache-idents true})))

(defn freeze
  "Encode `v` to a fresh byte[]. Signature-compatible with the
  former `nippy/freeze` for drop-in replacement."
  ^bytes [v]
  (hako/encode v))

(defn thaw
  "Decode a hako-encoded byte[]. Signature-compatible with the
  former `nippy/thaw` for drop-in replacement."
  [^bytes bs]
  (hako/decode bs {:cache-idents true}))

(defn fast-freeze
  "byte[]-returning encode via the thread-local reusable Writer.
  Amortizes arena setup; copies segment → byte[] on the way out."
  ^bytes [v]
  (let [wr ^Writer (.get tl-writer)
        seg (hako/encode-into! wr v)
        n (.byteSize seg)
        _ (when (> n Integer/MAX_VALUE)
            (throw (IllegalStateException.
                    (str "hako-codec: payload exceeds Integer/MAX_VALUE: " n))))
        arr (byte-array n)]
    (MemorySegment/copy seg ValueLayout/JAVA_BYTE 0 arr 0 n)
    arr))

(defn fast-thaw
  "Decode via the thread-local reusable Reader."
  [^bytes bs]
  (let [rd ^Reader (.get tl-reader)
        seg (MemorySegment/ofArray bs)]
    (hako/decode-into! rd seg {:cache-idents true})))

;; Phase 2: direct-ByteBuffer paths — skip the byte[] bounce.

(defn put-data-into-buffer!
  "Encode `v` with hako's reusable Writer, copying bytes directly into
  `bb` at its current position. Advances `bb.position` by the byte
  count written and returns it. Uses `hako/encode-into-buffer!` —
  skips both the MemorySegment slice wrapper and the `.asByteBuffer`
  view that the earlier two-step form allocated per call."
  [^ByteBuffer bb v]
  (hako/encode-into-buffer! ^Writer (.get tl-writer) bb v))

(defn get-data-from-buffer!
  "Decode `n` bytes from `bb`'s current position via the reusable
  Reader. Wraps the ByteBuffer region as a MemorySegment slice —
  zero heap copy for the wrap step. Advances `bb.position` by n."
  [^ByteBuffer bb ^long n]
  (let [rd ^Reader (.get tl-reader)
        start (.position bb)
        seg (if (.hasArray bb)
              (.asSlice (MemorySegment/ofArray ^bytes (.array bb))
                        (+ (.arrayOffset bb) start) n)
              (.asSlice (MemorySegment/ofBuffer bb) 0 n))
        v (hako/decode-into! rd seg {:cache-idents true})]
    (.position bb (+ start (int n)))
    v))
