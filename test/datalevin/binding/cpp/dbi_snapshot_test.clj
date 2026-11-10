(ns datalevin.binding.cpp.dbi-snapshot-test
  (:require [clojure.test :refer [deftest is use-fixtures]]
            [datalevin.binding.cpp :as cpp]
            [datalevin.core :as d]
            [datalevin.interface :as i]
            [datalevin.kv :as kv]
            [datalevin.test.core :refer [db-fixture]]
            [datalevin.util :as u]))

(use-fixtures :once db-fixture)

(deftest table-snapshots-support-thread-local-reader-slots
  (let [root (u/tmp-dir (str "dbi-snapshot-tls-" (random-uuid)))
        db (d/open-kv root {:wal? false :flags #{:nordahead} :max-readers 3})
        raw (kv/raw-lmdb db)]
    (try
      (i/open-dbi db "observed")
      (i/open-dbi db "unrelated")
      (i/transact-kv db [[:put "observed" :value 0]])
      ;; More captures than reader slots also exercise owner-thread cleanup.
      (dotimes [n 16]
        (with-open [snapshot ^java.lang.AutoCloseable (cpp/dbi-snapshot raw "observed")]
          (is (= n (i/get-value db "observed" :value)))
          (is (cpp/dbi-snapshot-current? raw snapshot))
          (i/transact-kv db [[:put "unrelated" :value n]])
          (is (cpp/dbi-snapshot-current? raw snapshot))
          (i/transact-kv db [[:put "observed" :value (inc n)]])
          (is (not (cpp/dbi-snapshot-current? raw snapshot)))))
      (is (= 16 (i/get-value db "observed" :value)))
      ;; Constructor failure must release the owner before the environment closes.
      (is (thrown? NullPointerException (cpp/dbi-snapshot raw nil)))
      (with-open [snapshot ^java.lang.AutoCloseable (cpp/dbi-snapshot raw "observed")]
        (try
          (.interrupt (Thread/currentThread))
          (.close snapshot)
          (is (.isInterrupted (Thread/currentThread)))
          (finally (Thread/interrupted))))
      (is (= 16 (i/get-value db "observed" :value)))
      (finally
        (d/close-kv db)
        (u/delete-files root)))))

(deftest table-snapshots-ignore-other-trees-and-aborted-writes
  (let [root (u/tmp-dir (str "dbi-snapshot-" (random-uuid)))
        db (d/open-kv root {:wal? false})
        raw (kv/raw-lmdb db)]
    (try
      (i/open-dbi db "observed")
      (i/open-dbi db "unrelated")
      (i/transact-kv db [[:put "observed" :value 0]])
      (with-open [snapshot ^java.lang.AutoCloseable (cpp/dbi-snapshot raw "observed")]
        (is (cpp/dbi-snapshot-current? raw snapshot))
        (i/transact-kv db [[:put "unrelated" :value 1]])
        (is (cpp/dbi-snapshot-current? raw snapshot))
        (d/with-transaction-kv [tx db]
          (i/transact-kv tx [[:put "observed" :value 1]])
          (i/abort-transact-kv tx))
        (is (cpp/dbi-snapshot-current? raw snapshot))
        ;; Counts and sizes stay the same; a committed overwrite still matters.
        (i/transact-kv db [[:put "observed" :value 1]])
        (is (not (cpp/dbi-snapshot-current? raw snapshot)))
        ;; Returning to the original data cannot reuse its pinned root page.
        (dotimes [n 16]
          (i/transact-kv db [[:put "observed" :value (mod n 2)]]))
        (i/transact-kv db [[:put "observed" :value 0]])
        (is (not (cpp/dbi-snapshot-current? raw snapshot))))
      (with-open [snapshot ^java.lang.AutoCloseable (cpp/dbi-snapshot raw "observed")]
        (i/clear-dbi db "observed")
        (is (not (cpp/dbi-snapshot-current? raw snapshot))))
      (with-open [snapshot ^java.lang.AutoCloseable (cpp/dbi-snapshot raw "late")]
        (is (cpp/dbi-snapshot-current? raw snapshot))
        (i/open-dbi db "late")
        (is (not (cpp/dbi-snapshot-current? raw snapshot))))
      (finally
        (d/close-kv db)
        (u/delete-files root)))))
