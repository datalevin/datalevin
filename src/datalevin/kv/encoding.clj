(ns ^:no-doc datalevin.kv.encoding
  "Transaction-owned copies of the uncompressed bytes written to LMDB."
  (:require [datalevin.lmdb]
            [datalevin.util :refer [raise]])
  (:import [datalevin.lmdb KVTxData]
           [java.nio ByteBuffer]
           [java.util List]
           [org.eclipse.collections.impl.list.mutable FastList]))

(deftype EncodedKVTxData
  [^KVTxData tx ^ByteBuffer buffer
   ^long key-offset ^long key-length ^long value-offset ^long value-length])

(deftype WriteBatch [^List rows ^FastList encoded arena])

(defn write-batch
  [^List rows arena]
  (WriteBatch. rows (FastList. (.size rows)) arena))

(defn reset-arena!
  "Release encoded rows before calling this under the environment write lock.
  Keep a bounded buffer for the next transaction; old chunks belong to rows."
  [arena]
  (when-let [^ByteBuffer buffer @arena]
    (if (> (.capacity buffer) (* 1024 1024))
      (vreset! arena nil)
      (.clear buffer)))
  nil)

(defn capture
  "Copy input buffers before LMDB can mutate them. Their uncompressed bytes are
  in [0, limit), including when compression has consumed their positions."
  [^WriteBatch batch ^KVTxData tx ^ByteBuffer key ^ByteBuffer value]
  (let [key-length (.limit key)
        value-length (int (if value (.limit value) 0))
        needed (+ (long key-length) value-length)
        arena (.-arena batch)
        ^ByteBuffer previous @arena
        ^ByteBuffer buffer
        (if (and previous (>= (.remaining previous) needed))
          previous
          (let [capacity (loop [capacity (long (if previous (.capacity previous) 8192))]
                           (if (>= capacity needed)
                             capacity
                             (recur (* 2 capacity))))]
            (when (> capacity Integer/MAX_VALUE)
              (raise "Encoded KV row is too large" {:size needed}))
            ;; Earlier rows retain their old chunks. Never overwrite or copy
            ;; those chunks while the transaction may still append them to WAL.
            (let [buffer (ByteBuffer/allocate (int capacity))]
              (vreset! arena buffer)
              buffer)))
        start (.position buffer)
        value-start (+ start key-length)
        end (+ value-start value-length)]
    (.put buffer start key 0 key-length)
    (when value (.put buffer value-start value 0 value-length))
    (.position buffer end)
    (EncodedKVTxData. tx buffer start key-length value-start value-length)))
