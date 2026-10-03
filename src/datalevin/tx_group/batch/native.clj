;; Copyright (c) Huahai Yang. All rights reserved.
;; Distributed under the Eclipse Public License 2.0.
(ns ^:no-doc datalevin.tx-group.batch.native
  "INativeBranch adapter for the new executor over the raw LMDB application
  writer.

  Supports blind writes. Each accepted sealed descriptor's prepared data carries
  `:rows` (the frozen canonical rows to apply) and `:result` (this request's
  return value). A request rejected during ordered preparation carries
  `:rejection` instead: it owns no rows, so it is neither applied nor given a
  slot in the native transaction. Rows are applied in one native transaction on
  the calling leader thread; `before-commit` blocks on the executor's
  WAL-policy gate, so a WAL failure aborts the native transaction before it
  commits."
  (:require [datalevin.binding.cpp :as cpp]
            [datalevin.interface :as i]
            [datalevin.kv.encoding :as encoding]
            [datalevin.tx-group.batch :as batch]
            [datalevin.tx-group.batch.executor :as executor]
            [datalevin.tx-group.phase :as phase]))

(defn branch
  "Adapt a raw native store to the executor's `INativeBranch`.

  `opts`:
  - `:apply-fn` overrides the native application writer (default
    `cpp/apply-native-range!`); injected by tests.
  - `:transact!` applies each descriptor's rows to the writable view (default
    `i/transact-kv` of `encoding/storage-rows`); injected by tests.
  - `:write-metadata!`, when supplied, runs after the WAL gate with the writable
    view and the WAL append token, for retained append-dependent metadata that
    must be in the same native transaction."
  ([raw] (branch raw nil))
  ([raw {:keys [apply-fn write-metadata! transact!]}]
   (let [apply-fn (or apply-fn cpp/apply-native-range!)
         transact! (or transact!
                       (fn [wdb rows]
                         (i/transact-kv wdb (encoding/storage-rows rows))))]
     (reify executor/INativeBranch
       (apply-rows! [_ batch gate]
         (let [n (batch/batch-count batch)]
           (phase/phase! :native-start batch)
           (apply-fn raw
                     (fn [wdb]
                       ;; Iterate owned regions in FIFO order. A flattened row
                       ;; list would grow with row count outside shared capacity.
                       ;; A rejected request carries no rows, so it is skipped
                       ;; here and cannot reach the store through any other path.
                       (dotimes [i n]
                         (when-let [rows (:rows (batch/data (batch/batch-at batch i)))]
                           (transact! wdb rows)))
                       (phase/phase! :native-applied batch))
                     (fn [wdb _context]
                       (phase/phase! :before-commit-wait batch)
                       (let [token (gate)]
                         (phase/phase! :before-commit-ready batch)
                         (when write-metadata!
                           (write-metadata! wdb token)))))
           (phase/phase! :native-committed batch)
           (let [values (object-array n)]
             (dotimes [i n]
               (let [data (batch/data (batch/batch-at batch i))]
                 (aset values i
                       (if-let [error (:rejection data)]
                         (batch/rejected error)
                         (:result data)))))
             values)))))))
