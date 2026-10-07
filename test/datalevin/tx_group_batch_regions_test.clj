(ns datalevin.tx-group-batch-regions-test
  (:require [clojure.test :refer [deftest is]]
            [datalevin.lmdb :as l]
            [datalevin.tx-group.batch :as batch]
            [datalevin.tx-group.batch.rmw :as rmw]
            [datalevin.txlog.codec :as codec])
  (:import [datalevin.utl RowRegions]
           [java.util Arrays]
           [org.eclipse.collections.impl.list.mutable FastList]))

(deftest owned-regions-retain-row-identity-and-order
  (let [rows (mapv (fn [_] (Object.)) (range 50))
        regions (RowRegions.)
        nested (RowRegions.)]
    (.append regions [])
    (doseq [region (partition-all 3 rows)] (.append regions (vec region)))
    (.append nested regions)
    (.append nested [(last rows)])
    (is (= 51 (.size nested)))
    (doseq [idx (range 50)]
      (is (identical? (rows idx) (.get nested idx))))
    (is (= (conj rows (last rows)) (vec nested)))
    (is (thrown? IndexOutOfBoundsException (.get nested -1)))
    (is (thrown? IndexOutOfBoundsException (.get nested 51)))))

(deftest regions-encode-identically
  (doseq [n [5 1400]
          datoms? [false true]]
    (let [rows (mapv #(if datoms?
                       (l/->DatomKVTxData % (byte-array [1 2 3]) true false)
                       [:put "data" % (str %) :long :string])
                     (range n))
          regions (RowRegions.)
          ^bytes expected (codec/encode-commit-row-payload 7 11 rows {:ha-term 3})]
      (doseq [region (partition-all 17 rows)] (.append regions (vec region)))
      (is (Arrays/equals expected
                         ^bytes (codec/encode-commit-row-payload 7 11 regions
                                                              {:ha-term 3}))))))

(deftest batch-encoding-preserves-preencoded-boundaries-and-trailer
  (let [row (fn [n] [:put "data" n (str n) :long :string])
        encode (fn [rows opts] (codec/encode-commit-row-payload 0 0 rows opts))
        first-data {:wal-rows [(row 1)] :result :first}
        descriptor (fn [data]
                     (batch/->Descriptor nil (volatile! data) nil 0 0
                                         0 nil nil nil))
        members [(descriptor first-data)
                 (descriptor {:wal-body (encode [(row 2)] {}) :wal-rows [(row 3)]})
                 (descriptor {:wal-rows [(row 4) (row 5)]})]
        b (batch/->Batch 0 (FastList. ^java.util.Collection members)
                         nil 0 0 nil nil nil nil nil 0 false)
        body (#'rmw/encode-members! b encode false)]
    (is (= (mapv row (range 1 6))
           (:ops (codec/decode-commit-row-payload body))))
    (is (identical? first-data (batch/data (first members))))
    (is (nil? (:wal-body (batch/data (second members)))))))
