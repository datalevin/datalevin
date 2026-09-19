(ns ^{:clj-kondo/config '{:lint-as {datalevin.kv-encoding-test/with-db clojure.core/let}}}
  datalevin.kv-encoding-test
  (:require [clojure.test :refer [deftest is testing]]
            [datalevin.compression-test]
            [datalevin.core :as d]
            [datalevin.kv :as kv]
            [datalevin.lmdb :as l]
            [datalevin.txlog :as wal]
            [datalevin.util :as u]
            [taoensso.nippy :as nippy])
  (:import [java.io DataInput DataOutput]
           [java.nio ByteBuffer]
           [java.nio.file Files CopyOption StandardCopyOption]))

(def ^:private opts {:wal? true :wal-durability-profile :strict
                     :snapshot-bootstrap-force? false})
(def ^:dynamic *encodes* nil)
(defrecord CountedValue [label])
(nippy/extend-freeze CountedValue ::counted-value
  [value ^DataOutput out]
  (when *encodes* (swap! *encodes* update (:label value) (fnil inc 0)))
  (.writeUTF out (:label value)))
(nippy/extend-thaw ::counted-value
  [^DataInput in] (->CountedValue (.readUTF in)))

(defmacro with-db [[db options] & body]
  `(let [dir# (u/tmp-dir (str "kv-encoding-" (random-uuid)))
         ~db (d/open-kv dir# (merge opts ~options))]
     (try ~@body
          (finally (d/close-kv ~db) (u/delete-files dir#)))))

(defn- data-rows [db]
  (vec (filter #(= "data" (second %))
               (mapcat :rows (kv/open-tx-log-rows db 1)))))

(deftest wal-reuses-the-key-and-value-serialization
  (with-db [db {}]
    (d/open-dbi db "data" {:val-size 8192 :validate-data? true})
    (let [key (->CountedValue "key")
          value {:document (->CountedValue "value")}
          after-lmdb (atom nil)]
      (binding [*encodes* (atom {})]
        (d/with-transaction-kv [tx db]
          (d/transact-kv tx [[:put "data" key value :data :data]])
          (reset! after-lmdb @*encodes*)
          (is (= 1 (get @*encodes* "value"))))
        ;; Variable-size key validation may serialize to measure its size.
        ;; WAL close must perform no additional serialization of either object.
        (is (= @after-lmdb @*encodes*)))
      (is (= value (d/get-value db "data" key :data :data)))
      (is (= [[:put "data" key value :data :data]] (data-rows db))))))

(deftest encoded-rows-own-their-bytes-through-growth-and-later-writes
  (with-db [db {}]
    (d/open-dbi db "data" {:val-size 32})
    (let [mutable (byte-array [1 2 3 4])
          raw (doto (ByteBuffer/wrap (byte-array [99 5 6 7 99]))
                (.position 1) (.limit 4))
          big (apply str (repeat 200000 "x"))
          rows (mapv #(vector :put "data" % (str "row-" %) :id :string)
                     (range 10 1110))]
      (d/with-transaction-kv [tx db]
        (d/transact-kv tx [[:put "data" 1 mutable :id :bytes]
                           [:put "data" 2 raw :id :raw]])
        (aset-byte mutable 0 (byte 42))
        (.put raw 1 (byte 43))
        (d/transact-kv tx [[:put "data" 3 big :id :string]])
        (d/transact-kv tx rows)
        (is (= [1 2 3 4] (vec (d/get-value tx "data" 1 :id :bytes)))))
      (let [logged (data-rows db)]
        (is (= 1103 (count logged)))
        (is (= [1 2 3 4] (vec (nth (nth logged 0) 3))))
        (is (= [5 6 7] (vec (nth (nth logged 1) 3))))
        (is (= big (nth (nth logged 2) 3)))
        (is (= rows (subvec logged 3))))
      (is (= "row-1109" (d/get-value db "data" 1109 :id :string)))
      ;; Later transactions reuse the arena without altering earlier records.
      (d/transact-kv db [[:put "data" 4 "later" :id :string]])
      (is (= [1 2 3 4] (vec (nth (first (data-rows db)) 3)))))))

(deftest abort-and-failed-flags-do-not-leak-encoded-rows
  (with-db [db {}]
    (d/open-dbi db "data")
    (let [bytes (byte-array [1 2])
          row (l/kv-tx :put "data" 1 bytes :id :bytes)]
      (d/transact-kv db [row])
      (d/with-transaction-kv [tx db]
        (d/transact-kv tx [[:put "data" 2 "aborted" :id :string]])
        (d/abort-transact-kv tx))
      (is (thrown? Exception
                   (d/transact-kv db [[:put "data" 1 (byte-array [9])
                                      :id :bytes [:nooverwrite]]])))
      (aset-byte bytes 0 (byte 3))
      (d/transact-kv db [row])
      (d/transact-kv db [[:del "data" 1 :id]])
      (let [logged (data-rows db)]
        (is (= 3 (count logged)))
        (is (= [1 2] (vec (nth (nth logged 0) 3))))
        (is (= [3 2] (vec (nth (nth logged 1) 3))))
        (is (= [:del "data" 1 :id] (nth logged 2))))
      (is (zero? (d/entries db "data"))))))

(defn- copy-file! [from to]
  (Files/copy (.toPath (u/file from)) (.toPath (u/file to))
              (into-array CopyOption [StandardCopyOption/REPLACE_EXISTING])))

(deftest compression-and-recovery-use-uncompressed-logical-bytes
  (doseq [compression [{} {:key-compress :hu} {:val-compress :zstd}
                        {:key-compress :hu :val-compress :zstd}]]
    (testing (str compression)
      (let [root (u/tmp-dir (str "kv-encoding-recovery-" (random-uuid)))
            dir (str root "/db")
            options (merge opts compression)
            value {:config (vec (repeat 100 "repeatable test payload"))}
            row [:put "data" :doc value :keyword :data]]
        (u/create-dirs dir)
        (try
          (#'datalevin.compression-test/write-dictionaries! dir)
          (let [db (d/open-kv dir options)]
            (try (d/open-dbi db "data" {:val-size 32})
                 (finally (d/close-kv db))))
          (copy-file! (str dir "/data.mdb") (str root "/baseline.mdb"))
          (copy-file! (wal/meta-path (str dir "/txlog")) (str root "/baseline-meta"))
          (let [db (d/open-kv dir options)]
            (try
              (d/open-dbi db "data")
              (d/transact-kv db [row])
              (d/transact-kv db [[:put "data" :removed "v" :keyword :string]
                                 [:del "data" :removed :keyword]])
              (is (= value (d/get-value db "data" :doc :keyword :data)))
              (is (= row (first (data-rows db))))
              (finally (d/close-kv db))))
          ;; Restore only the LMDB checkpoint and its metadata watermark. WAL
          ;; must replay the encoded tail with the environment's dictionaries.
          (copy-file! (str root "/baseline.mdb") (str dir "/data.mdb"))
          (copy-file! (str root "/baseline-meta") (wal/meta-path (str dir "/txlog")))
          (let [db (d/open-kv dir options)]
            (try
              (d/open-dbi db "data")
              (is (= value (d/get-value db "data" :doc :keyword :data)))
              (is (nil? (d/get-value db "data" :removed :keyword :string)))
              (is (= 1 (d/entries db "data")))
              (finally (d/close-kv db))))
          (finally (u/delete-files root)))))))

(deftest map-resize-retry-and-list-operations
  (with-db [db {:mapsize 1}]
    (d/open-dbi db "data")
    (let [value (apply str (repeat 2048 "x"))
          rows (mapv #(vector :put "data" % value :id :string) (range 2000))
          attempts (atom 0)]
      (d/with-transaction-kv [tx db]
        (swap! attempts inc)
        (d/transact-kv tx rows))
      (is (> @attempts 1))
      (is (= 2000 (d/entries db "data")))
      (is (= value (d/get-value db "data" 1999 :id :string)))
      (is (= rows (data-rows db))))
    (d/open-dbi db "lists" {:flags [:create :dupsort]})
    (let [rows [[:put-list "lists" 1 ["a" "b" "c"] :id :string]
                [:del-list "lists" 1 ["b"] :id :string]]]
      (d/transact-kv db rows)
      (is (= ["a" "c"] (vec (d/get-list db "lists" 1 :id :string))))
      (is (= rows (vec (filter #(= "lists" (second %))
                               (mapcat :rows (kv/open-tx-log-rows db 1)))))))))
