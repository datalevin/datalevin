;; Copyright (c) Huahai Yang. All rights reserved.
;; Distributed under the Eclipse Public License 2.0.
(ns ^:no-doc datalevin.tx-group.batch.stage
  "Private encoded-KV convenience calls on an ordinary native transaction.
   No staged state, overlay or undo journal."
  (:require [datalevin.interface :as i]
            [datalevin.lmdb :as l]))

(defn tx-context [db]
  (l/write-txn db)
  (l/request-context db))

(defn tx-get [db name key]
  (i/get-value db name key :raw :raw))

(defn tx-exists? [db name key]
  (pos? (long (i/range-count db name [:closed key key] :raw))))

(defn tx-range [db name lower upper limit]
  (with-open [^java.lang.AutoCloseable rows
              (i/range-seq db name [:closed lower upper] :raw :raw)]
    (vec (take limit rows))))

(defn tx-put! [db name key value]
  (i/transact-kv db [(l/kv-tx :put name key value :raw :raw)])
  nil)

(defn tx-del! [db name key]
  (i/transact-kv db [(l/kv-tx :del name key nil :raw :raw)])
  nil)

(defn tx-abort! [db]
  (i/abort-transact-kv db))
