(ns datalevin.hako-codec
  "hako-backed replacement for nippy's freeze-to-out! / thaw-from-in!.

  Matches nippy's signature so call sites can swap import + fn refs.
  Uses a thread-local reusable Writer + Reader to amortize arena
  setup across calls. The `DataOutput` / `DataInput` boundary
  forces an intermediate `byte[]` copy — genuinely unavoidable
  without refactoring callers to `MemorySegment`.

  Wire format per call: `<u32 length LE><hako-encoded payload>`.
  Length-prefix lets multiple values interleave in the same stream."
  (:require [s-exp.hako :as hako])
  (:import (com.s_exp.hako Reader Writer)
           (java.io DataInput DataOutput)
           (java.lang.foreign MemorySegment ValueLayout)))

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
