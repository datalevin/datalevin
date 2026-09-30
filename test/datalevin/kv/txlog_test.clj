(ns datalevin.kv.txlog-test
  (:require
   [clojure.java.io :as io]
   [clojure.test :refer [deftest is testing use-fixtures]]
   [datalevin.constants :as c]
   [datalevin.interface :as i]
   [datalevin.kv :as kv]
   [datalevin.kv.txlog :as sut]
   [datalevin.lmdb :as l]
   [datalevin.txlog :as txlog]
   [datalevin.txlog.transfer :as transfer]
   [datalevin.util :as u])
  (:import
   [java.io RandomAccessFile]
   [java.nio.channels FileChannel]))

(def ^:dynamic *dir* nil)

(use-fixtures :each
  (fn [f]
    (binding [*dir* (u/tmp-dir (str "bounded-txlog-" (random-uuid)))]
      (u/create-dirs *dir*)
      (try (f) (finally (u/delete-files *dir*))))))

(deftest record-replay-through-a-wal-handle-does-not-append-again
  (doseq [wrapped? [true false]]
    (let [db (l/open-kv (str *dir* "/" wrapped?)
                        {:wal? true :wal-shared? false
                         :wal-segment-prealloc? false :snapshot-scheduler? false})
          raw (kv/raw-lmdb db)
          wal (txlog/state db)]
      (try
        (i/open-dbi db "data" {:validate-data? true})
        (i/transact-kv db [[:put "data" 1 "1" :long :string]])
        (let [record (last (sut/txlog-records wal))
              handle (if wrapped? db raw)
              next-lsn @(:next-lsn wal)
              segment @(:segment-id wal)
              offset @(:segment-offset wal)
              rollout #(select-keys @(i/kv-info raw)
                                    [:wal-rollout-mode :wal-rollback?])
              original-rollout (rollout)]
          ;; Remove only the materialized value; retain its WAL record to replay.
          (i/transact-kv raw [[:del "data" 1 :long]])
          (is (nil? (i/get-value db "data" 1 :long :string)))
          (is (identical? record (sut/txlog-replay-record! handle wal record)))
          (is (= "1" (i/get-value db "data" 1 :long :string)))
          (is (= next-lsn @(:next-lsn wal)))
          (is (= segment @(:segment-id wal)))
          (is (= offset @(:segment-offset wal)))
          (is (= original-rollout (rollout)))
          (is (:ok? (i/verify-commit-marker! db)))
          ;; Physical validation must still reject malformed rows, and restore
          ;; normal WAL routing even when applying a replay record throws.
          (let [malformed (assoc record :rows
                                 [[:put "data" (byte-array 0) (byte-array [1])
                                   :raw :raw]])]
            (is (thrown? Exception
                         (sut/txlog-replay-record! handle wal malformed))))
          (is (= original-rollout (rollout)))
          (is (= next-lsn @(:next-lsn wal)))
          (is (= offset @(:segment-offset wal)))
          (is (= "1" (i/get-value db "data" 1 :long :string)))
          (i/transact-kv db [[:put "data" 2 "2" :long :string]])
          (is (= (inc (long next-lsn)) @(:next-lsn wal)))
          (is (= "2" (i/get-value db "data" 2 :long :string))))
        (finally (i/close-kv db))))))

(defn- append! [id lsns]
  (let [path (txlog/segment-path *dir* id)]
    (with-open [^FileChannel ch (txlog/open-segment-channel path)]
      (doseq [lsn lsns]
        (txlog/append-record!
          ch (txlog/encode-commit-row-payload lsn 1 [[:put "dbi" lsn lsn]]))))
    (.length (io/file path))))

(defn- state []
  {:dir *dir* :txlog-records-cache (volatile! {})
   :txlog-transfer-cache (transfer/create-cache)})

(defn- lsns [state from upto]
  (mapv :lsn (sut/txlog-records state from upto)))

(deftest bounded-catch-up-does-not-collect-the-log-tip
  (doseq [^long id (range 1 11)]
    (append! id (range (inc (* (dec id) 1000)) (inc (* id 1000)))))
  (let [state (state)
        cache (:txlog-records-cache state)
        segments (txlog/segment-files *dir*)]
    (testing "cold reads scan a bounded prefix and only probe later segment starts"
      (is (= (vec (range 101 111)) (lsns state 101 110)))
      (is (< (reduce + (map #(count (:records %)) (vals @cache))) 120))
      (is (= 111 (-> @cache (get 1) :records peek :lsn))))
    (testing "catch-up extends the cached closed prefix instead of rescanning it"
      (let [first-summary (-> @cache (get 1) :records first)]
        (is (= (vec (range 111 121)) (lsns state 111 120)))
        (is (identical? first-summary (-> @cache (get 1) :records first)))
        (is (= 121 (-> @cache (get 1) :records peek :lsn)))))
    (testing "warm full indexes also collect only the requested validation window"
      (is (= 10000 (count (sut/txlog-records state))))
      (let [selected (#'sut/txlog-segments-through state segments cache 110)
            records (vec (#'sut/collect-txlog-records
                           state selected cache 101 110 true))]
        (is (= [1 2] (mapv :id selected)))
        (is (<= (count records) 13))
        (is (= (vec (range 101 111)) (lsns state 101 110)))))
    (testing "a page can straddle segments"
      (is (= (vec (range 995 1005)) (lsns state 995 1004))))))

(deftest bounded-segment-scan-stops-before-unrequested-corruption
  (append! 1 (range 1 101))
  (append! 2 (range 101 201))
  (append! 3 (range 201 301))
  ;; Neither the target segment's tail nor the tails of boundary probes are
  ;; part of this page. Corruption there must only be encountered by later reads.
  (doseq [[id record-index] [[1 50] [2 1] [3 1]]]
    (let [path (txlog/segment-path *dir* id)
          records (:records (txlog/scan-segment path))
          offset (:offset (nth records record-index))]
      (with-open [raf (RandomAccessFile. ^String path "rw")]
        (.seek raf offset)
        (.write raf (int 0)))))
  (doseq [state [(state) {:dir *dir*}]]
    (is (= [10 11] (lsns state 10 11)))
    (is (thrown? clojure.lang.ExceptionInfo (sut/txlog-records state 10)))))

(deftest bounded-reader-preserves-overlaps-and-skips-empty-segments
  (doseq [[id records] [[1 [1 2 3]] [2 []] [3 [3 4 5]]
                        [4 [3 4 5]] [5 []] [6 [6 7]] [7 []]]]
    (append! id records))
  (doseq [state [(state) {:dir *dir*}]]
    (is (= [2 3 4 5 6] (lsns state 2 6)))
    (is (= [4 4 4] (mapv :segment-id (sut/txlog-records state 3 5))))
    (is (= [7] (lsns state 7 9)))
    (is (= [] (lsns state 8 9)))
    (is (= [] (lsns state 0 0)))
    (is (= [1 2 3 4 5 6 7] (lsns state 0 100)))))

(deftest bounded-reader-keeps-gap-validation-at-both-ends
  (append! 1 [1 2 4 5])
  (append! 2 [7 8])
  (doseq [[from upto] [[4 5] [1 2] [4 6]]]
    (let [error (try (sut/txlog-records (state) from upto)
                     (catch clojure.lang.ExceptionInfo e e))]
      (is (= :txlog/corrupt (:type (ex-data error)))))))

(deftest bounded-cache-remains-correct-as-active-segments-grow-and-close
  (let [offset (append! 1 [1 2 3 4])
        state (assoc (state) :segment-id (volatile! 1)
                             :segment-offset (volatile! offset))]
    (is (= [1] (lsns state 1 1)))
    (vreset! (:segment-offset state) (append! 1 [5 6]))
    (is (= [4 5] (lsns state 4 5)))
    (vreset! (:segment-offset state) (append! 2 [7 8]))
    (vreset! (:segment-id state) 2)
    (is (= [5 6 7] (lsns state 5 7)))
    (is (= (vec (range 1 9)) (mapv :lsn (sut/txlog-records state))))))

(deftest bounded-cache-does-not-hide-truncation
  (append! 1 [1 2 3 4 5])
  (let [state (state)]
    (is (= [1 2 3] (lsns state 1 3)))
    (let [offset (-> @(:txlog-records-cache state) (get 1) :records second
                     :next-offset)]
      (with-open [raf (RandomAccessFile. ^String (txlog/segment-path *dir* 1) "rw")]
        (.setLength raf offset)))
    (is (= [1 2] (lsns state 1 3)))))

(deftest lagging-follower-retains-its-prefix-in-a-small-cache
  (doseq [^long id (range 1 11)]
    (append! id (range (inc (* (dec id) 10)) (inc (* id 10)))))
  (binding [c/*wal-records-cache-segments* 2]
    (let [state (state)
          cache (:txlog-records-cache state)]
      (is (= [99 100] (lsns state 99 100)))
      (is (= [2 3] (lsns state 2 3)))
      (let [first-summary (-> @cache (get 1) :records first)]
        (is (some? first-summary))
        (is (= [4 5] (lsns state 4 5)))
        (is (identical? first-summary (-> @cache (get 1) :records first)))
        (is (<= (count @cache) 2))))))

(deftest recovery-still-reads-the-full-retained-prefix-and-tail
  (append! 1 [1 2 3 4])
  (append! 2 [5 6 7 8])
  (let [state (state)]
    (is (= [2] (lsns state 2 2)))
    (is (= (vec (range 1 9))
           (mapv :lsn (sut/txlog-records-for-recovery state 3))))
    (is (= [5 6 7 8]
           (mapv :lsn (sut/txlog-records-for-recovery state 6))))))

(deftest bounded-reader-includes-maximum-lsn
  (append! 1 [(dec Long/MAX_VALUE) Long/MAX_VALUE])
  (is (= [(dec Long/MAX_VALUE) Long/MAX_VALUE]
         (lsns (state) (dec Long/MAX_VALUE) Long/MAX_VALUE)))
  (is (= [(dec Long/MAX_VALUE) Long/MAX_VALUE]
         (mapv :lsn (transfer/decode-batch
                      (sut/txlog-record-batch (state) (dec Long/MAX_VALUE) Long/MAX_VALUE))))))

(deftest scan-callback-can-stop-without-reporting-a-partial-tail
  (append! 1 [1 2 3])
  (doseq [collect? [true false]]
    (let [seen (atom [])
          result (txlog/scan-segment
                   (txlog/segment-path *dir* 1)
                   {:collect-records? collect?
                    :on-record (fn [record]
                                 (swap! seen conj record)
                                 (reduced nil))})]
      (is (= 1 (count @seen)))
      (is (= collect? (boolean (seq (:records result)))))
      (is (:stopped? result))
      (is (false? (:partial-tail? result)))
      (is (= (:next-offset (first @seen)) (:valid-end result)))
      (is (< (:valid-end result) (:size result))))))

(deftest encoded-batches-share-payloads-without-changing-the-row-api
  (append! 1 (range 1 11))
  (append! 2 (range 11 21))
  (let [state (state)
        batch (sut/txlog-record-batch state 8 13)]
    (is (identical? batch (sut/txlog-record-batch state 8 13)))
    (is (= (txlog/select-open-record-rows (sut/txlog-records state 8 13) 8 13)
           (transfer/decode-batch batch)))))

(deftest encoded-batch-cache-observes-append-truncation-and-retention
  (let [offset (append! 1 [1 2 3 4])
        state (assoc (state) :segment-id (volatile! 1)
                             :segment-offset (volatile! offset))
        prefix (sut/txlog-record-batch state 1 2)
        tail (sut/txlog-record-batch state 3 8)]
    (vreset! (:segment-offset state) (append! 1 [5 6]))
    (is (identical? prefix (sut/txlog-record-batch state 1 2)))
    (is (= [3 4] (mapv :lsn (transfer/decode-batch tail))))
    (is (= [3 4 5 6] (mapv :lsn (transfer/decode-batch
                                (sut/txlog-record-batch state 3 8)))))
    (let [end (-> @(:txlog-records-cache state) (get 1) :records second :next-offset)]
      (with-open [raf (RandomAccessFile. ^String (txlog/segment-path *dir* 1) "rw")]
        (.setLength raf end))
      (vreset! (:segment-offset state) end))
    (is (= [] (transfer/decode-batch (sut/txlog-record-batch state 3 8))))
    (io/delete-file (txlog/segment-path *dir* 1))
    (is (= [] (transfer/decode-batch (sut/txlog-record-batch state 1 2))))))

(deftest encoded-batch-cache-does-not-hide-a-replaced-segment
  (append! 1 [1 2 3])
  (let [state (state)
        batch (sut/txlog-record-batch state 1 2)
        path (txlog/segment-path *dir* 1)
        modified (.lastModified (io/file path))]
    (io/delete-file path)
    (append! 1 [1 3 4])
    (.setLastModified (io/file path) (+ modified 1000))
    (is (= [1 2] (mapv :lsn (transfer/decode-batch batch))))
    (is (= :txlog/corrupt
           (try (sut/txlog-record-batch state 1 2) nil
                (catch clojure.lang.ExceptionInfo e (:type (ex-data e))))))))

(deftest encoded-serving-does-not-decode-rows-on-the-source
  (let [body (txlog/encode-commit-row-payload 1 1 [[:put "dbi" 1 1]])]
    ;; Preserve a valid payload header but make its row opcode invalid. The
    ;; source verifies the WAL framing/checksum; only the receiver decodes ops.
    (aset-byte body 28 (unchecked-byte 0xff))
    (with-open [^FileChannel ch (txlog/open-segment-channel (txlog/segment-path *dir* 1))]
      (txlog/append-record! ch body))
    (let [batch (sut/txlog-record-batch (state) 1 1)]
      (is (bytes? (:data batch)))
      (is (thrown? clojure.lang.ExceptionInfo (transfer/decode-batch batch))))))
