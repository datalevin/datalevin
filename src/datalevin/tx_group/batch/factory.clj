;; Copyright (c) Huahai Yang. All rights reserved.
;; Distributed under the Eclipse Public License 2.0.
(ns ^:no-doc datalevin.tx-group.batch.factory
  "Assemble the new-protocol environment runtime from a private WAL state and a
  raw native application writer.

  This is the wiring seam a private environment opener uses. It owns neither the
  WAL runtime nor the LMDB writer: the opener opens them, then this namespace
  binds the WAL-only runtime control, builds the two branch adapters and the one
  reusable WAL worker, and returns the collector with a close path that stops
  the worker only after the collector has drained.

  The returned map is consumed by `datalevin.tx-group.batch.env/open-batch!` as
  its `:executor` and `:executor-close!`."
  (:require [datalevin.tx-group.batch :as batch]
            [datalevin.tx-group.batch.executor :as executor]
            [datalevin.tx-group.batch.native :as native]
            [datalevin.tx-group.batch.wal :as wal]
            [datalevin.tx-group.batch.worker :as worker]
            [datalevin.txlog :as txlog]))

(defn executor
  "Build the environment executor and worker over a WAL state and native writer.

  `opts`:
  - `:runtime-control` binds the WAL-only control (`txlog/bind-runtime-control!`)
    before anything is built; required for a private WAL runtime.
  - `:native-opts` is forwarded to `native/branch` (`:apply-fn`, `:transact!`,
    `:write-metadata!`).
  - `:worker-opts` is forwarded to `worker/for-wal` (`:name`, `:daemon?`,
    `:close-timeout-ms`).
  - `:schedule-fn` overrides the fixed schedule selector (tests/diagnostics).

  Returns `{:executor :worker :wal-branch :native-branch :close!}`. `:close!`
  stops the worker and must run only after collector quiescence."
  ([wal-state native-writer]
   (executor wal-state native-writer nil))
  ([wal-state native-writer
    {:keys [runtime-control native-opts worker-opts schedule-fn]}]
   (when runtime-control
     (txlog/bind-runtime-control! wal-state runtime-control))
   (let [wal-branch (wal/branch wal-state)
         native-branch (native/branch native-writer native-opts)
         w (apply worker/for-wal wal-branch (apply concat worker-opts))
         exec (executor/create wal-branch native-branch
                               #(long @(:next-lsn wal-state))
                               (cond-> {:wal-executor (:executor w)
                                        :wake-maintenance! (:wake! w)}
                                 schedule-fn (assoc :schedule-fn schedule-fn)))]
     {:executor exec
      :worker w
      :wal-branch wal-branch
      :native-branch native-branch
      :close! (fn [] ((:close! w)))})))

(defn collector
  "Build the collector over a factory runtime. Returns
  `{:collector :executor :worker :close!}`."
  ([wal-state native-writer] (collector wal-state native-writer nil))
  ([wal-state native-writer {:keys [limits] :as opts}]
   (let [{:keys [executor] :as runtime} (executor wal-state native-writer opts)]
     (assoc runtime
            :collector (batch/create executor (when limits {:limits limits}))))))
