;; Copyright (c) Huahai Yang. All rights reserved.
;; Distributed under the Eclipse Public License 2.0.
(ns ^:no-doc datalevin.txlog.append
  "Shared append metadata. An LSN selects a record in a batch; there is no
  per-transaction durability object or completion signal here."
  (:import [java.nio.channels FileChannel]))

(defprotocol IAppendBatch
  (first-lsn [batch])
  (last-lsn [batch])
  (segment-id [batch])
  (channel [batch])
  (sync-manager [batch])
  (started-ms [batch])
  (deadline-ns [batch])
  (timeout-ms [batch])
  (near-roll? [batch])
  (record-info [batch lsn] "Physical coordinates for this batch's exact LSN."))

(deftype AppendBatch [^long lo ^long hi ^long sid ^FileChannel ch manager
                      ^long started ^long deadline ^long timeout
                      ^boolean near-roll records]
  IAppendBatch
  (first-lsn [_] lo)
  (last-lsn [_] hi)
  (segment-id [_] sid)
  (channel [_] ch)
  (sync-manager [_] manager)
  (started-ms [_] started)
  (deadline-ns [_] deadline)
  (timeout-ms [_] timeout)
  (near-roll? [_] near-roll)
  (record-info [_ lsn]
    (let [lsn (long lsn)]
      (when-not (<= lo lsn hi)
        (throw (ex-info "LSN is outside its append batch"
                        {:type :txlog/foreign-lsn :lsn lsn
                         :first-lsn lo :last-lsn hi})))
      (nth records (int (- lsn lo))))))

(defn create
  "Own one immutable descriptor for consecutive, completely written records.
  records contains only physical coordinates, never request rows or results,
  so a retained descriptor cannot keep other callers' application state alive."
  [lo sid ch manager started deadline timeout near-roll records]
  (when (empty? records)
    (throw (IllegalArgumentException. "An append batch must contain a record")))
  (AppendBatch. (long lo) (+ (long lo) (dec (long (count records))))
                (long sid) ch manager (long started) (long deadline)
                (long timeout) (boolean near-roll) records))

(defn commit-info
  "Materialize the exact record's commit metadata only at an API/native boundary."
  [batch lsn]
  (assoc (record-info batch lsn) :lsn (long lsn) :segment-id (segment-id batch)))
