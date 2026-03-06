;;
;; Copyright (c) Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns datalevin.dlmdb-rank-repro-test
  (:require
   [clojure.java.io :as io]
   [clojure.string :as str]
   [clojure.test :refer [deftest is]]
   [datalevin.binding.cpp]
   [datalevin.bits :as b]
   [datalevin.constants :as c]
   [datalevin.interface :as i]
   [datalevin.lmdb :as l]
   [datalevin.util :as u])
  (:import
   [java.lang AutoCloseable]
   [java.nio ByteBuffer ByteOrder]
   [java.util UUID]))

(def ^:private ops-file "test/data/get-by-rank-ops.txt")
(def ^:private dbi-name "items")

(defn- buffer-bytes
  [^ByteBuffer bf]
  (let [dup (.duplicate bf)]
    (mapv #(bit-and (int %) 0xff) (b/get-bytes dup))))

(defn- encoded-bytes
  [x x-type]
  (let [size (max 32 (+ 16 (b/measure-size x)))
        bf   (doto (ByteBuffer/allocateDirect size)
               (.order ByteOrder/BIG_ENDIAN))]
    (b/put-bf bf x x-type)
    (buffer-bytes bf)))

(defn- direct-rank-result
  [lmdb dbi-name rank k-type v-type]
  (let [dbi (i/get-dbi lmdb dbi-name false)
        rtx (i/get-rtx lmdb)]
    (try
      (let [[kb vb] (l/get-key-by-rank dbi rtx rank)]
        {:key-bytes   (buffer-bytes kb)
         :value-bytes (buffer-bytes vb)
         :key         (b/read-buffer (.duplicate kb) k-type)
         :value       (b/read-buffer (.duplicate vb) v-type)})
      (finally
        (i/return-rtx lmdb rtx)))))

(defn- sampled-rank-result
  [lmdb dbi-name rank k-type v-type]
  (let [dbi (i/get-dbi lmdb dbi-name false)
        rtx (i/get-rtx lmdb)
        cur (l/get-cursor dbi rtx)]
    (try
      (with-open [^AutoCloseable iter
                  (.iterator ^Iterable
                             (l/iterate-key-sample dbi rtx cur
                                                   (long-array [rank])
                                                   [:all]
                                                   k-type))]
        (when-not (.hasNext ^java.util.Iterator iter)
          (throw (ex-info "Expected sampled rank iterator to yield a result."
                          {:dbi  dbi-name
                           :rank rank})))
        (let [kv (.next ^java.util.Iterator iter)
              kb (l/k kv)
              vb (l/v kv)]
          {:key-bytes   (buffer-bytes kb)
           :value-bytes (buffer-bytes vb)
           :key         (b/read-buffer (.duplicate kb) k-type)
           :value       (b/read-buffer (.duplicate vb) v-type)}))
      (finally
        (if (l/read-only? rtx)
          (l/return-cursor dbi cur)
          (l/close-cursor dbi cur))
        (i/return-rtx lmdb rtx)))))

(defn- unhex-bytes
  [s]
  (byte-array (map #(unchecked-byte (int %)) (u/unhexify s))))

(defn- load-ops
  []
  (->> (slurp (io/file ops-file))
       str/split-lines
       (map str/trim)
       (remove #(or (str/blank? %)
                    (str/starts-with? % "#")
                    (str/starts-with? % ";")))
       (mapv (fn [line]
               (let [[op k v] (str/split line #"\s+")]
                 (case op
                   "put" [:put (unhex-bytes k) (unhex-bytes v)]
                   "del" [:del (unhex-bytes k)]
                   (throw (ex-info "Unsupported repro op line."
                                   {:line line}))))))))

(defn- run-op
  [lmdb [op & args]]
  (case op
    :put          (i/transact-kv lmdb dbi-name [[op (first args) (second args)]]
                                 :raw :raw)
    :del          (i/transact-kv lmdb dbi-name [[op (first args)]] :raw :raw)
    (throw (ex-info "Unsupported repro op." {:op op :args args}))))

(defn reproduce-get-key-by-rank-bug
  []
  (let [dir  (u/tmp-dir (str "dlmdb-rank-repro-" (UUID/randomUUID)))
        lmdb (l/open-kv dir {:flags (conj c/default-env-flags :nosync)})]
    (try
      (i/open-dbi lmdb dbi-name)
      (let [ops     (load-ops)
            _       (run! #(run-op lmdb %) ops)]
        {:ops-file ops-file
         :ops      ops
         :expected {:key-bytes   (encoded-bytes "c" :string)
                    :value-bytes (encoded-bytes "gamma" :string)
                    :key         "c"
                    :value       "gamma"}
         :direct   (direct-rank-result lmdb dbi-name 2 :string :string)
         :sampled  (sampled-rank-result lmdb dbi-name 2 :string :string)})
      (finally
        (i/close-kv lmdb)
        (u/delete-files dir)))))

(deftest direct-get-key-by-rank-repro-test
  (let [{:keys [expected direct sampled]} (reproduce-get-key-by-rank-bug)]
    (is (= expected sampled))
    ;; Repro for the lower-level rank bug: direct rank lookup returns the
    ;; correct value buffer for rank 2, but the key buffer is malformed.
    (is (= (:value-bytes expected) (:value-bytes direct)))
    (is (= (:value expected) (:value direct)))
    (is (not= (:key-bytes expected) (:key-bytes direct)))
    (is (not= (:key expected) (:key direct)))))

(defn -main
  [& _]
  (prn (reproduce-get-key-by-rank-bug)))
