;;
;; Copyright (c) Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns ^:no-doc datalevin.compress
  "key value compressors"
  (:require
   [clojure.java.io :as io]
   [datalevin.constants :as c]
   [datalevin.lmdb :as l]
   [datalevin.bits :as b]
   [datalevin.util :as u]
   [datalevin.interface :as i
    :refer [open-dbi close-kv list-dbi? entries visit-key-sample
            visit-list-sample list-dbis ICompressor]]
   [datalevin.hu :as hu])
  (:import
   [java.nio ByteBuffer]
   [java.nio.file Files Paths]
   [java.util HashSet]
   [datalevin.hu HuTucker]
   [datalevin.utl BitOps]
   [org.eclipse.collections.impl.list.mutable FastList]
   [com.github.luben.zstd Zstd ZstdDictCompress ZstdDictDecompress
    ZstdDictTrainer]))

;; Value compressor using zstd

(defn train-zstd
  "given a sample of bytes, return the zstd dictionary bytes"
  [samples]
  (let [sample-size (transduce (map alength) + samples)
        dict-size   (* c/+value-compress-dict-size+ 1024)
        trainer     (ZstdDictTrainer. sample-size dict-size)]
    (doseq [sample samples]
      (.addSample trainer sample))
    (.trainSamples trainer)))

(defn- zstd-compressor
  [^bytes dict level]
  (let [dict-compress   (ZstdDictCompress. dict (int level))
        dict-decompress (ZstdDictDecompress. dict)]
    (reify
      ICompressor
      (method [_] :zstd)
      (bf-compress [_  src dst]
        (Zstd/compress ^ByteBuffer dst ^ByteBuffer src dict-compress))
      (bf-uncompress [_ src dst]
        (Zstd/decompress ^ByteBuffer dst ^ByteBuffer src dict-decompress)))))

(defn val-compressor
  [^bytes dict]
  (zstd-compressor dict 3))

(defn load-val-compressor
  [^String path]
  (let [dict (Files/readAllBytes (Paths/get path (make-array String 0)))]
    (zstd-compressor dict 3)))

;; key compressor using Hu-Tucker coding

(defn init-key-freqs
  "We assume a Dirichlet prior"
  ^longs []
  (long-array c/+key-compress-num-symbols+ (repeat 1)))

(defn- hu-compressor
  [^HuTucker ht]
  (reify
    ICompressor
    (method [_] :hu)
    (bf-compress [_ src dst] (.encode ht src dst))
    (bf-uncompress [_ src dst] (.decode ht src dst))))

(defn key-compressor
  ([^longs freqs]
   (hu-compressor (hu/new-hu-tucker freqs)))
  ([^bytes lens ^ints codes]
   (hu-compressor (hu/codes->hu-tucker lens codes))))

(defn load-key-compressor
  [^String path]
  (hu-compressor (hu/load-hu-tucker (io/input-stream path))))

;; db samplers

(defn- collect-keys
  [^longs freqs ^ByteBuffer bf]
  (while (< 0 (.remaining bf))
    (let [idx (if (= 1 (.remaining bf))
                (-> (.get bf)
                    (BitOps/intAnd 0x000000FF)
                    (bit-shift-left 8))
                (let [s (.getShort bf)]
                  (BitOps/intAnd s 0x0000FFFF)))]
      (aset freqs idx (inc (aget freqs idx))))))

(defn- pick [ratio size]
  (long (Math/ceil (* (double ratio) ^long size))))

(defn- sample-plain-keys
  [db dbi-name size ratio freqs]
  (let [in      (u/reservoir-sampling size (pick ratio size))
        visitor (fn [kv] (collect-keys freqs (l/k kv)))]
    (visit-key-sample db dbi-name in visitor [:all] :raw)))

(defn- sample-values
  [db dbi-name size ratio ^FastList valbytes]
  (let [in      (u/reservoir-sampling size (pick ratio size))
        visitor (fn [kv] (.add valbytes (b/get-bytes (l/v kv))))]
    (visit-key-sample db dbi-name in visitor [:all] :raw)))

(defn- sample-list
  [db dbi-name size ratio freqs]
  (let [in      (u/reservoir-sampling size (pick ratio size))
        key-set (HashSet.)
        visitor (fn [kv]
                  (collect-keys freqs (l/v kv))
                  (let [kb ^ByteBuffer (l/k kv)
                        bs (b/encode-base64 (b/get-bytes kb))]
                    (when-not (.contains key-set bs)
                      (.add key-set bs)
                      (collect-keys freqs (.rewind kb)))))]
    (visit-list-sample db dbi-name in visitor [:all] :raw :raw)))

(defn sample-key-freqs
  "return a long array of frequencies of 2 bytes symbols for keys,
  if there are enough key in DB, otherwise return nil"
  [db]
  (let [dbis  (list-dbis db)
        lists (map #(do (open-dbi db %) (list-dbi? db %)) dbis)
        sizes (map #(entries db %) dbis)
        total ^long (reduce + 0 sizes)]
    (when (< ^long c/*compress-sample-size* total)
      (let [freqs (init-key-freqs)
            ratio (/ ^long c/*compress-sample-size* total)]
        (mapv (fn [dbi size lst?]
                (if lst?
                  (sample-list db dbi size ratio freqs)
                  (sample-plain-keys db dbi size ratio freqs)))
              dbis sizes lists)
        freqs))))

(defn sample-value-bytes
  "return a list of bytes if there are enough values in DB,
  otherwise return nil"
  [db]
  (let [dbis  (filter #(when-not (list-dbi? db %) (open-dbi db %) %)
                      (list-dbis db))
        sizes (map #(entries db %) dbis)
        total ^long (reduce + 0 sizes)]
    (when (< ^long c/*compress-sample-size* total)
      (let [valbytes (FastList.)
            ratio    (/ ^long c/*compress-sample-size* total)]
        (mapv (fn [dbi size]
                (sample-values db dbi size ratio valbytes))
              dbis sizes)
        valbytes))))

(comment

  (def db (l/open-kv "benchmarks/JOB-bench/db"))
  (time (def freqs (sample-key-freqs db)))
  ;; cold 5400ms, hot 125ms
  (def hu (hu/new-hu-tucker freqs))
  (hu/dump-hu-tucker hu "key-code.bin")
  (def valbtyes (sample-value-bytes db))
  (i/range-count db "datalevin/giants" [:all])
  (count valbtyes)
  (count freqs)
  (u/dump-bytes "val-code.bin" (train-zstd valbtyes))

  (def k-comp (key-compressor freqs))

  (close-kv db)
  )
