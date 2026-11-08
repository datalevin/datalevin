(ns datalevin.eav-delete-wal-test
  (:require [clojure.test :refer [deftest is use-fixtures]]
            [clojure.walk :as walk]
            [datalevin.constants :as c]
            [datalevin.core :as d]
            [datalevin.kv :as kv]
            [datalevin.lmdb :as l]
            [datalevin.test.core :refer [db-fixture]]
            [datalevin.tx-group.batch.public :as public]
            [datalevin.txlog.codec :as codec])
  (:import [datalevin.lmdb KVTxData]
           [java.io ByteArrayOutputStream]
           [java.nio ByteBuffer]))

(use-fixtures :each db-fixture)

(defn- comparable [value]
  (walk/postwalk #(if (bytes? %) (vec %) %) value))

(deftest validated-eav-deletions-use-the-existing-raw-pair-format
  (doseq [entity [0 1 Long/MAX_VALUE]
          size [1 107 c/+max-key-size+]]
    (let [avg (byte-array size (byte 42))
          original (l/datom-kv-tx entity avg false false true)
          scratch (object-array 1)
          ^KVTxData frozen (public/freeze-owned-eav-row original scratch)
          key (.array (.putLong (ByteBuffer/allocate 8) (long entity)))
          expected [:del-list c/eav key [avg] :raw :raw]
          body (codec/encode-commit-row-payload 1 2 [frozen])]
      (is (identical? (.-v ^KVTxData original) (.-v frozen)))
      (is (nil? (aget scratch 0)))
      (is (= codec/op-kv-del-raw-pair (aget ^bytes body 28)))
      (is (= (comparable [expected])
             (comparable (:ops (codec/decode-commit-row-payload body)))))
      (is (java.util.Arrays/equals
            body ^bytes (codec/encode-commit-row-payload 1 2 [expected]))))))

(deftest unvalidated-shapes-still-use-ordinary-kv-preparation
  (let [avg (byte-array 107)]
    (doseq [row [(l/kv-tx :del-list "other" 1 [avg] :id :raw)
                 (l/kv-tx :del-list c/eav 1 [avg] :long :raw)
                 (l/kv-tx :del-list c/eav 1 [avg] :id :data)
                 (l/kv-tx :del-list c/eav "one" [avg] :id :raw)
                 (l/kv-tx :del-list c/eav 1 [avg] :id :raw [:append])
                 (l/kv-tx :del-list c/eav 1 [] :id :raw)
                 (l/kv-tx :del-list c/eav 1 [avg avg] :id :raw)
                 (l/kv-tx :del-list c/eav 1 [(byte-array 0)] :id :raw)
                 (l/kv-tx :del-list c/eav 1 [(byte-array (inc c/+max-key-size+))] :id :raw)]]
      (is (nil? (public/freeze-owned-eav-row row (object-array 1)))))))

(deftest mixed-eav-wal-rows-encode-identically-in-serial-and-parallel
  (let [avg (byte-array 107 (byte 42))
        rows (mapv #(public/freeze-owned-eav-row
                      (l/datom-kv-tx % avg (even? %) false true) nil)
                   (range 2048))
        body (codec/encode-commit-row-payload 1 2 rows)
        output (ByteArrayOutputStream.)]
    (.write output ^bytes body 0 28)
    (doseq [row rows]
      (let [encoded (codec/encode-commit-row-payload 1 2 [row])]
        (.write output ^bytes encoded 28 (- (alength ^bytes encoded) 28))))
    (is (java.util.Arrays/equals body (.toByteArray output)))
    (is (= 2048 (count (:ops (codec/decode-commit-row-payload body)))))))

(deftest raw-pair-replay-removes-only-the-selected-eav-value
  (let [schema {:tag {:db/valueType :db.type/string :db/cardinality :db.cardinality/many
                     :db/noindex true}}
        options {:wal? true :wal-durability-profile :strict
                 :snapshot-scheduler? false :background-sampling? false}
        initial [{:db/id 1 :tag ["keep" "remove"]} {:db/id 2 :tag ["remove"]}]]
    (with-open [source (d/create-conn nil schema options)
                follower (d/create-conn nil schema options)]
      (d/transact! source initial)
      (d/transact! follower initial)
      (let [floor (:last-committed-lsn (d/txlog-watermarks (d/datalog-kv source)))]
        (d/with-transaction [tx source]
          (d/transact! tx [[:db/add 1 :tag "new"]])
          (d/transact! tx [[:db/retract 1 :tag "remove"]])
          ;; Drain staged storage through the validated EAV freezer.
          (is (= #{"keep" "new"} (:tag (d/entity @tx 1)))))
        (let [records (vec (kv/open-tx-log-rows (d/datalog-kv source) (inc floor)))
              deletions (filter #(and (= :del-list (first %)) (= c/eav (second %)))
                                (mapcat :rows records))]
          (is (= 1 (count deletions)))
          (is (= [:raw :raw] (subvec (vec (first deletions)) 4 6)))
          (dotimes [_ 2]
            (doseq [{:keys [rows lsn]} records]
              (kv/replay-txlog-rows! (d/datalog-kv follower) rows lsn)))
          (is (= #{"keep" "new"} (:tag (d/entity @source 1))
                 (:tag (d/entity @follower 1)))))
        (is (= #{"remove"} (:tag (d/entity @source 2)) (:tag (d/entity @follower 2))))))))
