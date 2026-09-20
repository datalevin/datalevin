(ns datalevin.commit-metadata-write-test
  (:require [clojure.test :refer [deftest is testing]]
            [datalevin.bits :as b]
            [datalevin.constants :as c]
            [datalevin.core :as d]
            [datalevin.interface :as i]
            [datalevin.kv :as kv]
            [datalevin.lmdb :as l]
            [datalevin.txlog :as wal]
            [datalevin.txlog.codec :as codec]
            [datalevin.util :as u])
  (:import [datalevin.kv KVLMDB]
           [datalevin.kv.encoding CommitMetadata]
           [java.nio ByteBuffer]
           [java.util Arrays]))

(def ^:private opts {:wal? true :wal-shared? false
                     :wal-durability-profile :strict
                     :snapshot-bootstrap-force? false})

(defn- stored-bytes [value type]
  (let [buffer (ByteBuffer/allocate 1024)]
    (b/put-buffer buffer value type)
    (.flip buffer)
    (b/get-bytes buffer)))

(deftest reused-marker-buffer-preserves-format
  (let [^CommitMetadata metadata (codec/new-commit-metadata)
        ^ByteBuffer slot (.-slot metadata)]
    (doseq [revision [0 1 2 Long/MAX_VALUE]
            n [0 127 128 32767 32768 Integer/MAX_VALUE Long/MAX_VALUE]]
      (let [record {:lsn n :segment-id 7 :offset n :checksum 0xffffffff
                    :now-ms 1750000000000}
            marker {:revision revision :applied-lsn n :txlog-segment-id 7
                    :txlog-record-offset n :txlog-record-crc 0xffffffff
                    :updated-ms 1750000000000}]
        ;; Reuse must clear reserved bytes as well as overwrite live fields.
        (Arrays/fill (.array slot) (byte 0x7f))
        (is (identical? metadata
                       (codec/prepare-commit-metadata!
                         metadata Long/MAX_VALUE revision record)))
        (codec/write-commit-marker-slot! slot (.-fields metadata))
        (is (= (seq (wal/encode-commit-marker-slot marker))
               (seq (.array slot))))
        (is (= marker (dissoc (wal/decode-commit-marker-slot-bytes
                               (.array slot)) :checksum)))
        (is (= revision (:revision metadata)))
        (is (= Long/MAX_VALUE (aget ^longs (.-fields metadata) 6)))))
    (codec/prepare-commit-metadata! metadata 42 nil {})
    (is (= -1 (:revision metadata)))
    (is (= 42 (aget ^longs (.-fields metadata) 6)))))

(deftest metadata-puts-retain-existing-key-and-value-encodings
  (doseq [marker? [true false]]
    (let [dir (u/tmp-dir (str "commit-metadata-format-" (random-uuid)))
          db (d/open-kv dir (assoc opts :wal-commit-marker? marker?))]
      (try
        (d/open-dbi db "data")
        (let [metadata (:commit-metadata-write (wal/state db))]
          (dotimes [n 4]
            (d/with-transaction-kv [tx db]
              (d/transact-kv tx "data" [[:put n [n "value"]]] :id :data))
            (is (identical? metadata (:commit-metadata-write (wal/state db))))
            (let [floor (d/get-value db c/kv-info c/wal-local-payload-lsn
                                     :keyword :data)
                  current (:current (kv/read-commit-marker db))]
              (is (= (seq (stored-bytes floor :data))
                     (seq (d/get-value db c/kv-info c/wal-local-payload-lsn
                                       :keyword :raw))))
              (if marker?
                (let [key (wal/commit-marker-key-for-revision (:revision current))
                      slot (wal/encode-commit-marker-slot current)]
                  (is (= (seq (stored-bytes slot :bytes))
                         (seq (d/get-value db c/kv-info
                                           (stored-bytes key :keyword)
                                           :raw :raw))))
                  (is (= floor (:applied-lsn current)))
                  (is (:ok? (kv/verify-commit-marker! db))))
                (is (nil? current)))))
          (d/close-kv db)
          (let [reopened (d/open-kv dir (assoc opts :wal-commit-marker? marker?))]
            (try
              (d/open-dbi reopened "data")
              (is (= 4 (d/entries reopened "data")))
              (is (= [3 "value"] (d/get-value reopened "data" 3 :id :data)))
              (is (not (identical? metadata
                                   (:commit-metadata-write (wal/state reopened)))))
              (finally (d/close-kv reopened)))))
        (finally (d/close-kv db) (u/delete-files dir))))))

(deftest failed-marker-write-does-not-publish-metadata
  (let [dir (u/tmp-dir (str "commit-metadata-failure-" (random-uuid)))
        db (d/open-kv dir opts)]
    (try
      (d/open-dbi db "data")
      (d/transact-kv db "data" [[:put 1 "before"]] :id :string)
      (let [raw (.-db ^KVLMDB db)
            state (wal/state db)
            cache (vec (:lmdb-commit-metadata state))
            before (:current (kv/read-commit-marker db))
            ;; Fail between the native payload-floor put and marker put.
            broken (CommitMetadata. (long-array 7)
                                    (.asReadOnlyBuffer (ByteBuffer/allocate 64)))]
        (vswap! (i/kv-info raw) assoc-in
                [:txlog-state :commit-metadata-write] broken)
        (is (thrown? Exception
                     (d/with-transaction-kv [tx db]
                       (d/transact-kv tx "data" [[:put 1 "after"]] :id :string))))
        (is (nil? @(l/write-txn raw)))
        (is (= cache (vec (:lmdb-commit-metadata state))))
        (is (= (:revision before) @(:marker-revision state)))
        (is (some? @(:fatal-error state)))
        (is (= "before" (i/get-value raw "data" 1 :id :string))))
      (d/close-kv db)
      (testing "the durable WAL payload is recovered after the failed LMDB commit"
        (let [reopened (d/open-kv dir opts)]
          (try
            (d/open-dbi reopened "data")
            (is (= "after" (d/get-value reopened "data" 1 :id :string)))
            (is (:ok? (kv/verify-commit-marker! reopened)))
            (finally (d/close-kv reopened)))))
      (finally (d/close-kv db) (u/delete-files dir)))))
