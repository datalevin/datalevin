;;
;; Copyright (c) Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns datalevin.compress-test
  (:require
   [datalevin.compress :as sut]
   [datalevin.constants :as c]
   [datalevin.interface :as if]
   [datalevin.bits :as b]
   [datalevin.buffer :as bf]
   [datalevin.lmdb :as l]
   [datalevin.util :as u]
   [datalevin.hu :as hu]
   [datalevin.test.core :as tdc :refer [db-fixture]]
   [clojure.test :refer [deftest is use-fixtures]]
   [clojure.test.check.generators :as gen]
   [clojure.test.check.clojure-test :as test]
   [clojure.test.check.properties :as prop])
  (:import
   [java.util UUID Arrays HashSet List]
   [java.io File]
   [java.nio ByteBuffer]
   [java.nio.file Files Paths]
   [java.lang Long]
   [datalevin.lmdb IListRandKeyValIterable IListRandKeyValIterator]))

(use-fixtures :each db-fixture)

(deftest plain-sample-test
  (let [dir  (u/tmp-dir (str "sample-key-" (UUID/randomUUID)))
        lmdb (l/open-kv dir {:flags (conj c/default-env-flags :nosync)})
        m    10
        ks   (shuffle (range 0 m))
        txs  (map (fn [k v] [:put "u" k v :long :long]) ks ks)

        n    1000000
        ks1  (shuffle (range 0 n))
        txs1 (map (fn [k v] [:put "v" k v :long :long]) ks1 ks1)]

    (if/open-dbi lmdb "u")
    (if/transact-kv lmdb txs)
    (let [freqs  (sut/sample-key-freqs lmdb)
          vbytes (sut/sample-value-bytes lmdb)]
      (is (nil? freqs))
      (is (nil? vbytes)))

    (if/open-dbi lmdb "v")
    (if/transact-kv lmdb txs1)
    (let [^longs freqs   (sut/sample-key-freqs lmdb)
          ^List valbytes (sut/sample-value-bytes lmdb)]
      (is (<= c/*compress-sample-size* (count valbytes) ))
      (is (= (alength freqs) c/+key-compress-num-symbols+))
      (is (< (* 2 ^long c/*compress-sample-size*) (aget freqs 0)))
      (is (< (aget freqs 1) (aget freqs 0)))
      (is (< (count (filter #(< 1 ^long %) (seq freqs)))
             (count (filter #(= ^long % 1) (seq freqs))))))

    (if/close-kv lmdb)
    (u/delete-files dir)))

(deftest list-sample-freqs-test
  (let [dir  (u/tmp-dir (str "sample-list-" (UUID/randomUUID)))
        lmdb (l/open-kv dir {:flags (conj c/default-env-flags :nosync)})
        m    100
        n    1000
        ks   (shuffle (range 0 m))
        vs   (shuffle (range 0 n))
        txs  (for [k ks v vs] [:put "l" k v :id :id])]

    (if/open-list-dbi lmdb "l")
    (if/transact-kv lmdb txs)
    (let [^longs freqs (sut/sample-key-freqs lmdb)]
      (is (= (alength freqs) c/+key-compress-num-symbols+))
      (is (< (* 2 ^long c/*compress-sample-size*) (aget freqs 0)))
      (is (< (aget freqs 1) (aget freqs 0)))
      (is (< (count (filter #(< 1 ^long %) (seq freqs)))
             (count (filter #(= ^long % 1) (seq freqs))))))

    (if/close-kv lmdb)
    (u/delete-files dir)))

(defn data-size-less-than?
  [^long limit data]
  (< (alength ^bytes (b/serialize data)) limit))

(test/defspec key-compressed-data-generative-test
  200
  (let [freqs           (long-array (repeatedly 65536
                                                #(inc ^int (rand-int 60000))))
        compresssor     (sut/key-compressor freqs)
        ^ByteBuffer src (bf/allocate-buffer c/+max-key-size+)
        ^ByteBuffer dst (bf/allocate-buffer c/+max-key-size+)
        ^ByteBuffer res (bf/allocate-buffer c/+max-key-size+)]
    (prop/for-all
      ;; Hu-Tucker can expand; keep inputs small to avoid flakey overflow.
      [k (gen/such-that (partial data-size-less-than? 200)
                        gen/any-equatable
                        1000)]
      (.clear src)
      (.clear dst)
      (.clear res)
      (b/put-buffer src k :data)
      (if/bf-compress compresssor (.flip src) dst)
      (if/bf-uncompress compresssor (.flip dst) res)
      (zero? (bf/compare-buffer (.flip src) (.flip res))))))

(test/defspec val-compressed-data-generative-test
  200
  (let [samples         (repeatedly 5000 #(.getBytes (u/random-string 1024)))
        dict            (sut/train-zstd samples)
        compresssor     (sut/val-compressor dict)
        ^ByteBuffer src (bf/allocate-buffer c/*init-val-size*)
        ^ByteBuffer dst (bf/allocate-buffer c/*init-val-size*)
        ^ByteBuffer res (bf/allocate-buffer c/*init-val-size*)]
    (prop/for-all
      [v gen/any-equatable]
      (.clear src)
      (.clear dst)
      (.clear res)
      (b/put-buffer src v :data)
      (if/bf-compress compresssor (.flip src) dst)
      (if/bf-uncompress compresssor (.flip dst) res)
      (zero? (bf/compare-buffer (.flip src) (.flip res))))))

(deftest kv-compressor-test
  (let [orig-dir (u/tmp-dir (str "kv-origin-" (UUID/randomUUID)))
        orig-kv  (l/open-kv orig-dir
                            {:flags (conj c/default-env-flags :nosync)})
        n        70000
        ks       (repeatedly n #(u/random-string (inc ^int (rand-int 200))))
        vs       (repeatedly n #(u/random-string (inc ^int (rand-int 200))))
        txs      (map (fn [k v] [:put "a" k v :string :string]) ks vs)]
    (if/open-dbi orig-kv "a")
    (if/transact-kv orig-kv txs)

    (let [freqs   (sut/sample-key-freqs orig-kv)
          samples (sut/sample-value-bytes orig-kv)
          hu      (hu/new-hu-tucker freqs)
          dict    (sut/train-zstd samples)
          test-fn
          (fn [comp-opt]
            (let [comp-dir (u/tmp-dir (str "kv-compress-" (UUID/randomUUID)))
                  _        (u/create-dirs comp-dir)
                  _        (hu/dump-hu-tucker
                             hu
                             (str comp-dir u/+separator+ c/keycode-file-name))
                  _        (u/dump-bytes
                             (str comp-dir u/+separator+ c/valcode-file-name)
                             dict)
                  comp-kv  (l/open-kv
                             comp-dir
                             (merge {:flags (conj c/default-env-flags :nosync)}
                                    comp-opt))]
              (if/open-dbi comp-kv "a")
              (if/transact-kv comp-kv txs)
              (if/flush-kv-indexer! orig-kv)
              (if/flush-kv-indexer! comp-kv)

              (is (= (if/get-range orig-kv "a" [:all] :string :string)
                     (if/get-range comp-kv "a" [:all] :string :string)))
              (is (< (l/data-size comp-kv) (l/data-size orig-kv)))

              (if/close-kv comp-kv)
              (u/delete-files comp-dir)))]
      (doseq [opt [{:key-compress :hu}
                   {:val-compress :zstd}
                   {:key-compress :hu :val-compress :zstd}]]
        (test-fn opt)))
    (if/close-kv orig-kv)
    (u/delete-files orig-dir)))
