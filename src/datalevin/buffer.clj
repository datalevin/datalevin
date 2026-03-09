;;
;; Copyright (c) Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns ^:no-doc datalevin.buffer
  "ByteBuffer operations"
  (:require
   [datalevin.constants :as c]
   [datalevin.util :as u])
  (:import
   [java.util.concurrent ConcurrentLinkedDeque]
   [java.nio ByteBuffer]
   [datalevin.utl BufOps]))

(defn buffer-transfer
  "Transfer content from one bytebuffer to another"
  ([^ByteBuffer src ^ByteBuffer dst]
   (.put dst src))
  ([^ByteBuffer src ^ByteBuffer dst n]
   (dotimes [_ n] (.put dst (.get src)))))

(defn compare-buffer
  ^long [^ByteBuffer b1 ^ByteBuffer b2]
  (BufOps/compareByteBuf b1 b2))

(defonce ^ConcurrentLinkedDeque array-buffers (ConcurrentLinkedDeque.))

(defn- allocate-array-buffer
  ^ByteBuffer [size]
  (ByteBuffer/wrap (byte-array size)))

(defn- take-buffer!
  ^ByteBuffer [^ConcurrentLinkedDeque buffers allocate ^long size]
  (let [cap (+ size Integer/BYTES)] ;; for storing length
    (loop [skipped []]
      (if-let [^ByteBuffer bf (.pollFirst buffers)]
        (if (<= cap (.capacity bf))
          (do
            (doseq [^ByteBuffer skipped-bf skipped]
              (.offer buffers skipped-bf))
            (.clear bf)
            bf)
          (recur (conj skipped bf)))
        (do
          (doseq [^ByteBuffer skipped-bf skipped]
            (.offer buffers skipped-bf))
          (allocate cap))))))

(defn get-array-buffer
  ([] (get-array-buffer (+ c/+max-key-size+ Integer/BYTES)))
  (^ByteBuffer [^long size]
   (take-buffer! array-buffers allocate-array-buffer size)))

(defn return-array-buffer [bf] (.offer array-buffers bf))

(defonce ^ConcurrentLinkedDeque direct-buffers (ConcurrentLinkedDeque.))

(defn allocate-buffer
  "Allocate JVM off-heap ByteBuffer in the Datalevin expected endian order"
  ^ByteBuffer [size]
  (ByteBuffer/allocateDirect size))

(defn get-direct-buffer
  ([] (get-direct-buffer (+ c/+max-key-size+ Integer/BYTES)))
  (^ByteBuffer [^long size]
   (take-buffer! direct-buffers allocate-buffer size)))

(defn return-direct-buffer [bf] (.offer direct-buffers bf))

(defn hex-buffer [^ByteBuffer sb]
  (let [sa (byte-array (.remaining sb))]
    (.get sb sa)
    (u/hexify sa)))
