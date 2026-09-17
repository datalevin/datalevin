(ns datalevin.kv-metadata-test
  (:require
   [clojure.test :refer [deftest is testing use-fixtures]]
   [datalevin.constants :as c]
   [datalevin.core :as d]
   [datalevin.interface :as i]
   [datalevin.kv :as kv]
   [datalevin.lmdb :as l]
   [datalevin.txlog :as txlog]
   [datalevin.util :as u])
  (:import
   [datalevin.binding.cpp Rtx]
   [datalevin.binding.cpp.buffer IWriteCursor]
   [datalevin.cpp Cursor Txn]
   [datalevin.kv KVLMDB]))

(def ^:dynamic *dir* nil)

(use-fixtures :each
  (fn [f]
    (binding [*dir* (u/tmp-dir (str "kv-metadata-" (random-uuid)))]
      (try (f)
           (finally
             (when (u/file-exists *dir*) (u/delete-files *dir*)))))))

(def opts {:wal? true :wal-durability-profile :strict
           :wal-commit-marker? true :snapshot-bootstrap-force? false})

(defn- raw-db [^KVLMDB db] (.-db db))
(defn- marker [db] (:current (kv/read-commit-marker db)))
(defn- payload-lsn [db]
  (i/get-value db c/kv-info c/wal-local-payload-lsn :keyword :data))
(defn- cache [db] (vec (:lmdb-commit-metadata (txlog/state db))))
(defn- put! [db n] (d/transact-kv db "data" [[:put n n]] :long :long))

(defn- metadata-rows [db revision floor]
  [[:put c/kv-info c/wal-marker-a
    (txlog/encode-commit-marker-slot (assoc (marker db) :revision revision))
    :keyword :bytes]
   [:put c/kv-info c/wal-local-payload-lsn floor :keyword :data]])

(deftest commit-metadata-survives-reopen
  (doseq [marker? [true false]]
    (let [dir (str *dir* "/" marker?)
          options (assoc opts :wal-commit-marker? marker?)]
      (dotimes [pass 2]
        (let [db (d/open-kv dir options)]
          (try
            (d/open-dbi db "data")
            (dotimes [n 4]
              (let [before (marker db)]
                (put! db (+ (* pass 4) n))
                (let [[_ revision floor] (cache db)
                      current (marker db)]
                  (is (= floor (payload-lsn db)))
                  (if marker?
                    (do
                      (is (= (inc (long (or (:revision before) -1))) revision))
                      (is (= revision (:revision current)))
                      (is (= floor (:applied-lsn current))))
                    (is (nil? current))))))
            (is (= (* (inc pass) 4) (d/entries db "data")))
            (when marker? (is (:ok? (kv/verify-commit-marker! db))))
            (finally (d/close-kv db))))))))

(deftest raw-commit-invalidates-metadata
  (let [db (d/open-kv *dir* opts)]
    (try
      (d/open-dbi db "data")
      (put! db 0)
      (i/transact-kv (raw-db db) (metadata-rows db 1000 1000000))
      (testing "an intervening raw transaction is detected by its native ID"
        (put! db 1)
        (is (= 1001 (:revision (marker db))))
        (is (= 1000000 (payload-lsn db))))
      (testing "subsequent cached commits preserve the higher payload floor"
        (put! db 2)
        (is (= 1002 (:revision (marker db))))
        (is (= 1000000 (payload-lsn db))))
      (finally (d/close-kv db)))))

(deftest metadata-writes-in-current-transaction
  (let [db (d/open-kv *dir* opts)]
    (try
      (d/open-dbi db "data")
      (put! db 0)
      (let [rows (metadata-rows db 2000 2000000)]
        (d/with-transaction-kv [tx db]
          (i/transact-kv (raw-db tx) rows)
          (put! tx 1)))
      (is (= 2001 (:revision (marker db))))
      (is (= 2000000 (payload-lsn db)))
      (testing "deleting the metadata also invalidates the cached floor"
        (d/with-transaction-kv [tx db]
          (i/transact-kv (raw-db tx)
                        [[:del c/kv-info c/wal-marker-a :keyword]
                         [:del c/kv-info c/wal-marker-b :keyword]
                         [:del c/kv-info c/wal-local-payload-lsn :keyword]])
          (put! tx 2))
        (is (= 0 (:revision (marker db))))
        (is (= (:applied-lsn (marker db)) (payload-lsn db))))
      (finally (d/close-kv db)))))

(deftest aborted-metadata-is-not-cached
  (let [db (d/open-kv *dir* opts)]
    (try
      (d/open-dbi db "data")
      (put! db 0)
      (let [before (cache db)
            revision (:revision (marker db))
            rows (metadata-rows db 3000 3000000)]
        (d/with-transaction-kv [tx db]
          (i/transact-kv (raw-db tx) rows)
          (put! tx 1)
          (d/abort-transact-kv tx))
        (is (= before (cache db)))
        (is (= revision (:revision (marker db))))
        (is (nil? (d/get-value db "data" 1 :long :long)))
        (put! db 2)
        (is (= (inc revision) (:revision (marker db))))
        (is (< (payload-lsn db) 3000000)))
      (finally (d/close-kv db)))))

(deftest metadata-cursor-writes-invalidate-cache
  (let [db (d/open-kv *dir* opts)]
    (try
      (d/open-dbi db "data")
      (put! db 0)
      (d/with-transaction-kv [tx db]
        (let [raw (raw-db tx)
              dbi (i/get-dbi raw c/kv-info false)
              ^Rtx rtx @(l/write-txn raw)
              ^Txn txn (.-txn rtx)
              ^Cursor cur (.writeCursor ^IWriteCursor dbi txn)]
          (try
            (l/put-key dbi c/wal-local-payload-lsn :keyword)
            (l/put-val dbi 4000000 :data)
            (.put cur 0)
            (finally (.close cur))))
        (put! tx 1))
      (is (= 4000000 (payload-lsn db)))
      (put! db 2)
      (is (= 4000000 (payload-lsn db)))
      (finally (d/close-kv db)))))
