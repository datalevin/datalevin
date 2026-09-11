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
   [java.io ByteArrayInputStream]
   [java.nio ByteBuffer]
   [java.nio.file Files Paths]
   [java.security MessageDigest]
   [java.util HashSet HexFormat UUID]
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
  [source]
  (hu-compressor (hu/load-hu-tucker (io/input-stream source))))

;; The manifest lives in the uncompressed kv-info DBI. Dictionary files are
;; immutable for the lifetime of an environment, including marked-write views.

(defn- compression-mode [opts option supported]
  (let [mode (get opts option)]
    (when-not (contains? #{nil :none supported} mode)
      (u/raise "Unsupported compression method"
               {:error :compression/unsupported :option option :method mode}))
    (if (or (nil? mode) (= :none mode)) :none mode)))

(defn- load-dictionary [dir stream descriptor]
  (let [{:keys [method sha256]} descriptor]
    (if (= :none method)
      [descriptor nil]
      (let [file (io/file dir (if (= stream :key)
                               c/keycode-file-name c/valcode-file-name))]
        (when-not (.isFile file)
          (u/raise "Required compression dictionary is missing"
                   {:error :compression/missing-dictionary
                    :stream stream :file (str file)}))
        ;; Hash and construct from the same bytes, not two reads of the path.
        (let [bytes (Files/readAllBytes (.toPath file))
              hash (.formatHex (HexFormat/of)
                               (.digest (MessageDigest/getInstance "SHA-256") bytes))]
          (when (and sha256 (not= sha256 hash))
            (u/raise "Compression dictionary checksum does not match the stored generation"
                     {:error :compression/dictionary-mismatch :stream stream
                      :file (str file) :expected sha256 :actual hash}))
          [(assoc descriptor :sha256 hash)
           (if (= stream :key)
             (load-key-compressor (ByteArrayInputStream. bytes))
             (val-compressor bytes))])))))

(defn open-compression
  "Validate persisted encoding and load its immutable dictionaries. New modes
  can only be selected when creating an environment; changing them needs a rebuild."
  [dir opts loaded-info]
  (let [key-mode (compression-mode opts :key-compress :hu)
        val-mode (compression-mode opts :val-compress :zstd)
        stored (:compression loaded-info)
        _ (when (and (seq loaded-info) (nil? stored)
                     (some #(not= :none %)
                           [key-mode val-mode
                            (compression-mode loaded-info :key-compress :hu)
                            (compression-mode loaded-info :val-compress :zstd)]))
            (u/raise "Changing compression requires rebuilding into a new environment"
                     {:error :compression/rebuild-required}))
        manifest (or stored {:generation (str (UUID/randomUUID))
                             :key {:method key-mode}
                             :value {:method val-mode}})]
    (when stored
      (when-not (and (string? (:generation stored))
                     (not-empty (:generation stored))
                     (every? (fn [[stream supported]]
                               (let [{:keys [method sha256]} (get stored stream)]
                                 (and (contains? #{:none supported} method)
                                      (or (= :none method)
                                          (and (string? sha256)
                                               (re-matches #"[0-9a-f]{64}" sha256))))))
                             [[:key :hu] [:value :zstd]]))
        (u/raise "Invalid or unsupported compression manifest"
                 {:error :compression/invalid-manifest}))
      (doseq [[option stream mode] [[:key-compress :key key-mode]
                                   [:val-compress :value val-mode]]
              :when (and (contains? opts option)
                         (not= mode (get-in stored [stream :method])))]
        (u/raise "Compression option conflicts with the stored generation; rebuild required"
                 {:error :compression/mode-conflict :option option
                  :requested mode :stored (get-in stored [stream :method])})))
    (when (= :zstd (get-in manifest [:value :method]))
      (doseq [[dbi {:keys [flags]}] (:dbis loaded-info)
              :let [flags (set flags)]
              :when (and (:dupsort flags) (not (:dupfixed flags)))]
        (u/raise "Value compression is not supported on ordered duplicate values"
                 {:error :compression/ordered-duplicates :dbi dbi})))
    (let [[key-desc key-codec] (load-dictionary dir :key (:key manifest))
          [val-desc val-codec] (load-dictionary dir :value (:value manifest))]
      {:manifest (assoc manifest :key key-desc :value val-desc)
       :key-codec key-codec :value-codec val-codec
       :key-compress (when-not (= :none (:method key-desc)) (:method key-desc))
       :val-compress (when-not (= :none (:method val-desc)) (:method val-desc))})))

;; db samplers

(defn- collect-keys
  [^longs freqs ^ByteBuffer bf]
  (while (< 1 (.remaining bf))
    (let [pair (bit-or (bit-shift-left (BitOps/intAnd (.get bf) 0xFF) 8)
                       (BitOps/intAnd (.get bf) 0xFF))
          idx  (hu/pair-symbol pair)]
      (aset freqs idx (inc (aget freqs idx)))))
  (let [idx (if (.hasRemaining bf)
              (hu/final-byte-symbol (BitOps/intAnd (.get bf) 0xFF))
              hu/end-symbol)]
    (aset freqs idx (inc (aget freqs idx)))))

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
  "Return frequencies of byte-pair and terminal symbols for keys,
  if there are enough keys in DB; otherwise return nil."
  [db]
  (let [dbis  (list-dbis db)
        lists (map #(do (open-dbi db %) (list-dbi? db %)) dbis)
        sizes (map #(entries db %) dbis)
        sample-size ^long (long c/*compress-sample-size*)
        total ^long (long (reduce + 0 sizes))]
    (when (< sample-size total)
      (let [freqs (init-key-freqs)
            ratio (/ (double sample-size) (double total))]
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
        sample-size ^long (long c/*compress-sample-size*)
        total ^long (long (reduce + 0 sizes))]
    (when (< sample-size total)
      (let [valbytes (FastList.)
            ratio    (/ (double sample-size) (double total))]
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
