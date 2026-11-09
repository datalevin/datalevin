(ns datalevin.prepared-copy-test
  (:require [clojure.test :refer [deftest is testing]]
            [datalevin.binding.cpp :as cpp]
            [datalevin.constants :as c]
            [datalevin.core :as d]
            [datalevin.interface :as i]
            [datalevin.kv :as kv]
            [datalevin.lmdb :as l]
            [datalevin.server.copy :as copy]
            [datalevin.util :as u]))

(deftest prepared-reader-keeps-the-captured-state-while-writers-commit
  (doseq [compact? [false true] wal? [false true]]
    (testing (str [compact? wal?])
      (let [root (u/tmp-dir (str "prepared-copy-" (random-uuid)))
            dest (str root "/copy")
            source (d/open-kv (str root "/source") {:wal? wal?})]
        (try
          (i/open-dbi source "data")
          (i/transact-kv source [[:put "data" :value "captured"]])
          (u/create-dirs dest)
          (with-open [prepared (locking (l/write-txn source)
                                (cpp/prepare-copy (kv/raw-lmdb source) dest compact?))]
            (let [writer (future (i/transact-kv source [[:put "data" :value "later"]]))]
              (is (not= ::timeout (deref writer 5000 ::timeout))))
            (cpp/copy-prepared! prepared)
            (is (thrown? IllegalStateException (cpp/copy-prepared! prepared))))
          (copy/finalize-copy! dest "source" {})
          (spit (str dest "/VERSION") c/version)
          (let [restored (d/open-kv dest)]
            (try (is (= "captured" (i/get-value restored "data" :value)))
                 (finally (d/close-kv restored))))
          (finally (d/close-kv source) (u/delete-files root)))))))

(deftest raw-finalization-clears-source-metadata-without-starting-a-store
  (let [root (u/tmp-dir (str "raw-copy-" (random-uuid)))
        dest (str root "/copy")
        source (d/open-kv (str root "/source") {:wal? true})]
    (try
      (i/open-dbi source "data")
      (i/open-dbi source c/opts)
      (i/transact-kv source
        [[:put "data" :value "captured"]
         [:put c/kv-info :wal-dir "/unavailable/source/wal" :keyword :data]
         [:put c/kv-info :snapshot-dir "/unavailable/source/snapshots" :data :data]
         [:put c/kv-info c/wal-backup-pins {"source" {:floor-lsn 1}} :keyword :data]
         [:put c/opts :wal-dir "/unavailable/source/wal" :attr :data]
         [:put c/opts :snapshot-dir "/unavailable/source/snapshots" :attr :data]
         [:put c/opts :kv-opts {:wal-dir "/unavailable/source/wal"
                               :snapshot-dir "/unavailable/source/snapshots"
                               :wal-durability-profile :strict} :attr :data]])
      (u/create-dirs dest)
      (with-open [prepared (cpp/prepare-copy (kv/raw-lmdb source) dest false)]
        (cpp/copy-prepared! prepared))
      (let [meta (copy/finalize-copy! dest "source" {})
            raw (cpp/open-existing-raw dest)]
        (try
          (is (pos? (:payload-last-applied-lsn meta)))
          (is (not (contains? (set (i/list-dbis raw)) c/schema)))
          (is (not (u/file-exists (str dest "/txlog"))))
          (is (not (u/file-exists (str dest "/snapshots"))))
          (doseq [key [:wal-dir :snapshot-dir c/wal-backup-pins c/wal-marker-a c/wal-marker-b]
                  kt [:keyword :data]]
            (is (nil? (i/get-value raw c/kv-info key kt :data))))
          (is (= {:wal-durability-profile :strict}
                 (i/get-value raw c/opts :kv-opts :attr :data)))
          (is (= "captured" (i/get-value raw "data" :value)))
          (finally (i/close-kv raw))))
      (finally (d/close-kv source) (u/delete-files root)))))
