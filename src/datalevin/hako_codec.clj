(ns datalevin.hako-codec
  "hako-backed replacement for nippy's freeze-to-out! / thaw-from-in!.

  Matches nippy's signature so call sites can swap import + fn refs.
  Uses a thread-local reusable Writer + Reader to amortize arena
  setup across calls. The `DataOutput` / `DataInput` boundary
  forces an intermediate `byte[]` copy — genuinely unavoidable
  without refactoring callers to `MemorySegment`.

  Wire format per call: `<u32 length LE><hako-encoded payload>`.
  Length-prefix lets multiple values interleave in the same stream.

  Phase 2 additions: `put-data-into-buffer!` bypasses the byte[]
  bounce for LMDB writes — hako's arena-backed MemorySegment gets
  copied directly into the caller's ByteBuffer, no heap intermediate."
  (:require [s-exp.hako :as hako])
  (:import (com.s_exp.hako Reader Writer)
           (java.io DataInput DataOutput)
           (java.lang.foreign MemorySegment ValueLayout)
           (java.nio ByteBuffer)))

(set! *warn-on-reflection* true)

(def ^:private ^ThreadLocal tl-writer
  (proxy [ThreadLocal] []
    (initialValue [] (hako/writer 4096))))

(def ^:private ^ThreadLocal tl-reader
  (proxy [ThreadLocal] []
    (initialValue [] (hako/reader (byte-array 0)))))

(def ^:private ^ThreadLocal tl-scratch
  (proxy [ThreadLocal] []
    (initialValue [] (byte-array 4096))))

(defn- ^bytes ensure-scratch [^long n]
  (let [buf ^bytes (.get tl-scratch)]
    (if (>= (alength buf) n)
      buf
      (let [grown (byte-array (max n (* 2 (alength buf))))]
        (.set tl-scratch grown)
        grown))))

(defn freeze-to-out!
  "Encode `v` with hako and write `<u32 length><payload>` to `out`."
  [^DataOutput out v]
  (let [wr ^Writer (.get tl-writer)
        seg (hako/encode-into! wr v)
        n (.byteSize seg)
        _ (when (> n Integer/MAX_VALUE)
            (throw (IllegalStateException.
                    (str "hako-codec: payload exceeds Integer/MAX_VALUE: " n))))
        buf (ensure-scratch n)]
    (MemorySegment/copy seg ValueLayout/JAVA_BYTE 0 buf 0 n)
    (.writeInt out (int n))
    (.write out buf 0 (int n))))

(defn thaw-from-in!
  "Read a `<u32 length><payload>` frame from `in` and hako-decode it."
  [^DataInput in]
  (let [n (.readInt in)
        buf ^bytes (ensure-scratch n)
        _ (.readFully in buf 0 n)
        rd ^Reader (.get tl-reader)
        seg (MemorySegment/ofArray buf)]
    (hako/decode-into! rd (.asSlice ^MemorySegment seg 0 n) {:cache-idents true})))

(defn freeze
  "Encode `v` to a fresh byte[]. Convenience mirror of nippy/freeze."
  ^bytes [v]
  (hako/encode v))

(defn thaw
  "Decode a hako-encoded byte[]. Convenience mirror of nippy/thaw."
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
  "Encode `v` with hako's reusable Writer, then copy segment bytes
  directly into `bb` at its current position. Advances `bb.position`
  by the byte count written. Skips the byte[] intermediate that
  `serialize` + `.put(bb, bs)` allocates. Returns the byte count."
  [^ByteBuffer bb v]
  (let [wr ^Writer (.get tl-writer)
        seg (hako/encode-into! wr v)
        n (.byteSize seg)
        _ (when (> n Integer/MAX_VALUE)
            (throw (IllegalStateException.
                    (str "hako-codec: payload exceeds Integer/MAX_VALUE: " n))))
        src-bb (.asByteBuffer seg)]
    (.put bb src-bb)
    (int n)))

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
