(ns datalevin.wal-payload-buffer-test
  (:require [clojure.test :refer [deftest is testing]]
            [datalevin.txlog :as wal]
            [datalevin.txlog.codec :as codec]
            [datalevin.txlog.segment :as seg]
            [datalevin.util :as u])
  (:import [datalevin.lmdb DatomKVTxData]
           [java.io Closeable IOException]
           [java.nio ByteBuffer]
           [java.nio.channels FileChannel]
           [java.nio.file Files]
           [java.util Arrays HexFormat]))

(defn- buffer-bytes [^ByteBuffer body]
  (let [out (byte-array (.remaining body))]
    (.get (.duplicate body) out)
    out))

(deftest payload-buffer-matches-existing-kv-and-datalog-format
  ;; Produced by the pre-ByteBuffer encoder, with LSN 7, time 11 and HA term 13.
  (doseq [[rows hex]
          [[[[:put "r" 42 "abc" :id :string]]
            "444c5458010100000000000000000007000000000000000b000000000000000d0000000110000172110008000000000000002a0100000004fa61626300"]
           [[(DatomKVTxData. 42 (byte-array [1 2 3]) true false)]
            "444c5458020100000000000000000007000000000000000b000000000000000d0000000118000000000000002a0003010203"]]]
    (let [body (codec/encode-commit-row-payload-buffer 7 11 rows {:ha-term 13})]
      (try
        (is (Arrays/equals (.parseHex (HexFormat/of) ^String hex) (buffer-bytes body)))
        (finally (codec/release-commit-row-payload-buffer! body))))))

(deftest borrowed-payload-survives-nested-encoding-and-growth
  (doseq [size [100 20000 1048577]]
    (let [rows [[:put "data" 1 (byte-array size (byte 42)) :id :bytes]]
          ^ByteBuffer outer (codec/encode-commit-row-payload-buffer 1 2 rows)]
      (try
        (let [expected (buffer-bytes outer)
              inner (codec/encode-commit-row-payload-buffer
                     3 4 [(DatomKVTxData. 9 (byte-array 20000) true false)])]
          (try
            (is (not (identical? outer inner)))
            (is (Arrays/equals expected (buffer-bytes outer)))
            (is (Arrays/equals expected (codec/encode-commit-row-payload 1 2 rows)))
            (is (thrown? Exception
                         (codec/encode-commit-row-payload-buffer
                          5 6 [[:unsupported "data" 1 nil :id :bytes]])))
            (is (Arrays/equals expected (buffer-bytes outer)))
            (finally (codec/release-commit-row-payload-buffer! inner))))
        (finally (codec/release-commit-row-payload-buffer! outer)))))
  (testing "returned small buffers can be reused and patches preserve position"
    ;; The first small payload after a large one shrinks the retained buffer.
    (codec/encode-commit-row-payload 0 0 [])
    (let [body (codec/encode-commit-row-payload-buffer 1 2 [])]
      (codec/release-commit-row-payload-buffer! body)
      (let [^ByteBuffer again (codec/encode-commit-row-payload-buffer 3 4 [])]
        (try
          (is (identical? body again))
          (codec/patch-commit-row-payload-buffer-header! again 5 6)
          (is (zero? (.position again)))
          (is (= {:lsn 5 :ts 6 :op-count 0}
                 (codec/decode-commit-row-payload-header (buffer-bytes again))))
          (finally (codec/release-commit-row-payload-buffer! again)))))))

(defn- positioned-body [kind ^bytes bytes]
  (let [size (alength bytes)
        ^ByteBuffer base (if (#{:direct :direct-read-only} kind)
                           (ByteBuffer/allocateDirect (+ size 19))
                           (ByteBuffer/allocate (+ size 19)))
        _ (.position base 3)
        ^ByteBuffer body (.slice base)]
    (.position body 7)
    (.put body bytes)
    (.limit body (+ size 7))
    (.position body 7)
    (if (#{:read-only :direct-read-only} kind)
      (.asReadOnlyBuffer body)
      body)))

(deftest segment-writer-consumes-only-buffer-remaining-bytes
  (let [dir (u/tmp-dir (str "wal-body-buffers-" (random-uuid)))
        path (str dir "/records.wal")]
    (u/create-dirs dir)
    (try
      (with-open [^FileChannel ch (seg/open-segment-channel path)]
        (doseq [size [0 1 8178 8179 65522 65523 1048577]
                kind [:heap :direct :read-only :direct-read-only]
                compressed? [false true]]
          (let [bytes (byte-array size (byte 42))
                ^ByteBuffer body (positioned-body kind bytes)
                opts {:compressed? compressed?}
                expected (codec/encode-record bytes opts)
                checksum (codec/current-record-checksum size compressed? bytes)]
            (.mark body)
            (is (= checksum (codec/current-record-checksum size compressed? body)))
            (is (= 7 (.position body)))
            (.reset body)
            (.truncate ch 0)
            (let [result (seg/write-record-at! ch 0 body opts)]
              (is (= checksum (:checksum result)))
              (is (= (alength ^bytes expected) (:size result))))
            (is (= (+ size 7) (.position body) (.limit body)))
            (is (Arrays/equals expected (Files/readAllBytes (.toPath (u/file path))))))))
      (finally (u/delete-files dir)))))

(deftest parallel-payload-rows-and-thread-ownership
  (let [workers
        (mapv
         (fn [worker]
           (future
             (let [rows (mapv #(DatomKVTxData. % (byte-array [worker]) true false)
                              (range 2048))
                   ^ByteBuffer body (codec/encode-commit-row-payload-buffer
                                     worker 10 rows {:ha-term 11})]
               (try
                 (let [bytes (buffer-bytes body)
                       decoded (codec/decode-commit-row-payload bytes)]
                   (is (= 2048 (:op-count (codec/decode-commit-row-payload-header bytes))))
                   (is (= worker (:lsn decoded)))
                   (is (= 4096 (count (:ops decoded))))
                   (is (every? #(= [worker] (vec (nth % 2)))
                               (take-nth 2 (:ops decoded))))
                   (is (Arrays/equals bytes (codec/encode-commit-row-payload
                                            worker 10 rows {:ha-term 11}))))
                 (finally (codec/release-commit-row-payload-buffer! body))))))
         (range 4))]
    (doseq [worker workers]
      (is (not= ::timeout (deref worker 30000 ::timeout))))))

(defn- with-runtime [profile f]
  (let [dir (u/tmp-dir (str "wal-body-runtime-" (random-uuid)))
        {:keys [state]} (wal/init-runtime-state
                        {:dir dir :wal? true :wal-shared? false
                         :wal-durability-profile profile :wal-sync-mode :fdatasync
                         :wal-segment-prealloc? false} nil)]
    (try
      (f state)
      (finally
        (doseq [k [:segment-channel :sync-lock-channel]]
          (when-let [^Closeable ch (some-> (get state k) deref)] (.close ch)))
        (u/delete-files dir)))))

(deftest pending-payload-survives-hook-append-and-hook-failure
  (doseq [profile [:strict :relaxed]]
    (with-runtime
      profile
      (fn [state]
        (let [outer [[:put "data" 1 "outer" :id :string]]
              inner [[:put "data" 2 "inner" :id :string]]
              result (binding [wal/*commit-payload-ha-term* 7]
                       (wal/append-durable!
                        state outer
                        {:before-append!
                         (fn [_]
                           (binding [wal/*commit-payload-ha-term* 9]
                             (wal/append-durable! state inner {})))}))]
          (is (= 2 (:lsn result)))
          (is (thrown-with-msg?
               IOException #"hook failed"
               (wal/append-durable!
                state [[:put "data" 3 "aborted" :id :string]]
                {:before-append! (fn [_] (throw (IOException. "hook failed")))})))
          (is (instance? ByteBuffer (.get codec/tl-commit-body-buffer)))
          (wal/append-durable! state [[:put "data" 4 "after-failure" :id :string]] {})
          (let [records (:records (seg/scan-segment (wal/segment-path (:dir state) 1)))
                decoded (mapv #(codec/decode-commit-row-payload (:body %)) records)]
            (is (= [1 2 3] (mapv :lsn decoded)))
            (is (= [9 7 nil] (mapv :ha-term decoded)))
            (is (= [inner outer [[:put "data" 4 "after-failure" :id :string]]]
                   (mapv :ops decoded)))))))))

(deftest failed-segment-write-releases-payload
  (with-runtime
    :strict
    (fn [state]
      (.close ^Closeable @(:segment-channel state))
      (is (thrown? IOException
                   (wal/append-durable! state [[:put "data" 1 "v" :id :string]] {})))
      (is (instance? ByteBuffer (.get codec/tl-commit-body-buffer)))
      (is (= 1 @(:next-lsn state)))
      (is (= 0 @(:last-durable-lsn (:sync-manager state)))))))
