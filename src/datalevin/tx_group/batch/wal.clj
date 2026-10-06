;; Copyright (c) Huahai Yang. All rights reserved.
;; Distributed under the Eclipse Public License 2.0.
(ns ^:no-doc datalevin.tx-group.batch.wal
  "IWalBranch adapter over the WAL-only txlog interface.

  Each sealed descriptor's prepared data must carry an owned `:wal-body` (the
  output of `txlog/prepare-append-body`). Sealing stores those bodies in a flat
  array. The adapter appends that array as one group record under a single exclusive
  WAL ownership span, so a force cannot claim ownership between the append and
  its policy completion. The WAL runtime must have a bound runtime control
  (`txlog/bind-runtime-control!`)."
  (:require [datalevin.tx-group.batch :as batch]
            [datalevin.tx-group.batch.executor :as executor]
            [datalevin.txlog :as wal]))

(defn branch
  "Adapt a private WAL runtime state to the executor's `IWalBranch` and
  `IWalMaintenance`.

  `opts`:
  - `:append-fn` overrides the five-argument `txlog/begin-prepared-group!`
    (state, LSN, body array, absolute deadline, phase context); injected by tests.
  - `:complete-fn` overrides `txlog/finish-prepared-group!`; injected by tests.
  - `:maintenance-deadline-fn` overrides `txlog/maintenance-deadline-ns`.
  - `:service-maintenance-fn` overrides `txlog/service-maintenance!`."
  ([state] (branch state nil))
  ([state {:keys [append-fn complete-fn
                  maintenance-deadline-fn service-maintenance-fn]}]
   (let [custom-append? (some? append-fn)
         append-fn (or append-fn wal/begin-prepared-group!)
         complete-fn (or complete-fn wal/finish-prepared-group!)
         deadline-fn (or maintenance-deadline-fn wal/maintenance-deadline-ns)
         service-fn (or service-maintenance-fn wal/service-maintenance!)]
     (reify
       executor/IWalBranch
       (append-group! [_ batch lsn]
         (if custom-append?
           (append-fn state lsn (batch/wal-bodies batch)
                      (batch/batch-cutoff batch) batch)
           (let [write-count
                 (loop [idx 0 total 0]
                   (if (< idx (batch/batch-count batch))
                     (let [data (batch/data (batch/batch-at batch idx))]
                       (recur (inc idx)
                              (if (if (contains? data :rows)
                                    (seq (:rows data)) (:wal-body data))
                                (inc total) total)))
                     total))]
             (wal/begin-prepared-group! state lsn (batch/wal-bodies batch)
                                        (batch/batch-cutoff batch) batch
                                        write-count))))
       (complete-policy! [_ token deadline-ns]
         (complete-fn state token deadline-ns))
       executor/IWalMaintenance
       (maintenance-deadline-ns [_]
         (deadline-fn state))
       (service-maintenance! [_ deadline-ns]
         (service-fn state deadline-ns))))))
