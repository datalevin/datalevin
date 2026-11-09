;; Copyright (c) Huahai Yang. All rights reserved.
;; Distributed under the Eclipse Public License 2.0.
(ns ^:no-doc datalevin.server.capture
  "Shared secondary-index flush and fixed-snapshot capture for server copies."
  (:require [datalevin.binding.cpp :as cpp]
            [datalevin.constants :as c]
            [datalevin.interface :as i]
            [datalevin.kv :as kv]
            [datalevin.kv.txlog :as kvtx]
            [datalevin.lmdb :as l]
            [datalevin.secondary-index :as si]
            [datalevin.storage :as st]
            [datalevin.storage.domains :as domains]
            [datalevin.vector :as v]
            [datalevin.util :as u :refer [raise]])
  (:import [datalevin.storage Store]
           [datalevin.vector VectorIndex]
           [java.util.concurrent.locks Lock ReentrantReadWriteLock]))

(defn source-kv [source]
  (if (instance? Store source) (.-lmdb ^Store source) source))

(defn- vector-indices
  [source]
  (let [kv (source-kv source)
        dir (i/env-dir kv)
        runtime (into (sorted-map)
                      (keep (fn [[_ ^VectorIndex index]]
                              (when (= dir (i/env-dir (.-lmdb index)))
                                [(.-domain index) index])))
                      @l/vector-indices)
        runtime (if (instance? Store source)
                  (into runtime (map (fn [^VectorIndex index] [(.-domain index) index]))
                        (concat (vals (.-vector-indices ^Store source))
                                (vals (.-embedding-indices ^Store source))))
                  runtime)
        metadata (when (contains? (set (i/list-dbis kv)) c/vec-meta-dbi)
                   (i/get-range kv c/vec-meta-dbi [:all] :string :data))]
    (reduce (fn [indices [domain meta]]
              (if (contains? indices domain)
                indices
                (let [opts (:index-opts meta)
                      index (when (:dimensions opts) (v/open-vector-index kv opts))]
                  (when-not index
                    (raise "Cannot open a captured vector domain"
                           {:error :copy/vector-domain-unavailable :domain domain}))
                  (assoc indices domain index))))
            runtime metadata)))

(defn- status [source]
  (when (instance? Store source) (st/secondary-index-status source)))

(defn- index-writes? [source indices]
  (let [stats (status source)]
    (or (seq indices)
        (some #(pos? (long (get stats % 0)))
              [:pending-count :running-count :failed-count]))))

(defn- check-source! [source under-write-lock?]
  (let [raw (kv/raw-lmdb (source-kv source))]
    (when (or (i/key-compressor raw) (i/val-compressor raw))
      (raise "Remote copy does not yet transfer compression dictionaries"
             {:error :copy/compressed-source}))
    (when (or (l/writing? raw) (and under-write-lock? @(l/write-txn raw)))
      (raise "Cannot capture an open write transaction"
             {:error :copy/write-transaction-open}))))

(defn- foreign-worker? [source]
  (when (instance? Store source)
    (let [now (System/currentTimeMillis)]
      (some #(and (si/running-job? %)
                  (> (long (or (:job/lease-until-ms %) Long/MAX_VALUE)) now))
            (st/secondary-index-jobs source)))))

(defn- check-failed-jobs! [source]
  (when (pos? (long (get (status source) :failed-count 0)))
    (raise "Secondary index work failed before capture"
           {:error :copy/index-flush-failed :status (status source)})))

(defn- check-embedding-state! [source]
  (when (instance? Store source)
    (doseq [[domain config] (:embedding-domains (i/opts source))]
      (when-not (and (domains/complete-embedding-space? config)
                     (contains? (.-embedding-indices ^Store source) domain))
        (raise "Embedding space is incomplete at capture"
               {:error :copy/embedding-state-incomplete :domain domain})))))

(defn attempt!
  "Called with runtime lifetime protection, before acquiring a writer slot.
  Return :retry after contention; the caller must release runtime access before
  waiting/reacquiring. Copying retains the prepared snapshot, never the writer
  slot, LMDB monitor, or vector locks. Callbacks provide server authorization,
  write admission, and an injectable phase observer."
  [source dest compact? {:keys [standalone? with-writer authorize-write! admit-write observer]
                        :or {observer (fn [_ _])}}]
  (check-source! source false)
  (let [kv (source-kv source)
        raw (kv/raw-lmdb kv)
        indices (vector-indices source)
        writes? (index-writes? source indices)
        writable! (fn []
                    (when (some #{:rdonly :rdonly-env} (i/get-env-flags raw))
                      (raise "Capture requires writes on a read-only source"
                             {:error :copy/read-only-source}))
                    (when writes? (authorize-write!)))
        pin (volatile! nil)]
    (check-failed-jobs! source)
    (when writes?
      (writable!)
      ;; External provider calls and live workers must be able to finish before
      ;; capture acquires the gate. Never steal a live claim to drain the queue.
      (admit-write #(when (instance? Store source)
                      (st/wait-for-secondary-index source
                        {:timeout-ms 1 :process? true}))))
    (check-failed-jobs! source)
    (try
      (let [prepared
            (with-writer
              (fn []
                (locking (l/write-txn raw)
                  (observer :lmdb-locked {:source source})
                  (check-source! source true)
                  (let [indices (vector-indices source)
                        writes? (index-writes? source indices)
                        locks (volatile! [])]
                    (try
                      (if (or (foreign-worker? source)
                              (some (fn [[domain ^VectorIndex index]]
                                      (let [lock (.writeLock ^ReentrantReadWriteLock (.-vec-lock index))]
                                        (if (.tryLock lock)
                                          (do (vswap! locks conj lock)
                                              (observer :vector-locked {:domain domain}) false)
                                          true)))
                                    indices))
                        :retry
                        (let [prepare!
                              (fn []
                                (when writes?
                                  (writable!)
                                  (authorize-write!)
                                  (when (instance? Store source)
                                    (st/process-secondary-index-jobs! source)
                                    (check-failed-jobs! source)
                                    (when-not (:caught-up? (st/wait-for-secondary-index source))
                                      (raise "Secondary indexes did not catch up"
                                             {:error :copy/index-flush-failed})))
                                  (doseq [[domain index] indices]
                                    (observer :before-checkpoint {:domain domain})
                                    (i/persist-vecs index)))
                                (check-embedding-state! source)
                                (when-not standalone?
                                  (vreset! pin (kvtx/prepare-copy-backup-pin! kv raw dest compact?))
                                  (kvtx/notify-copy-backup-pin! @pin))
                                (cpp/prepare-copy raw dest compact?))]
                          (if (or writes? (and (not standalone?) (:wal? (i/env-opts kv))))
                            (do (when-not standalone? (writable!)) (admit-write prepare!))
                            (prepare!))))
                      (finally
                        (doseq [^Lock lock (rseq @locks)] (.unlock lock))))))))]
        (if (= :retry prepared)
          :retry
          (with-open [prepared prepared]
            (observer :prepared {:source source :prepared prepared})
            (cpp/copy-prepared! prepared)
            (observer :copied {:source source})
            (cond-> {:compact? (boolean compact?)}
              @pin (assoc :backup-pin {:pin-id (:pin-id @pin)
                                      :floor-lsn (:pin-floor-lsn @pin)
                                      :expires-ms (:pin-expires-ms @pin)})))))
      (finally
        (when-let [pin-id (:pin-id @pin)]
          (with-writer #(admit-write (fn [] (kv/txlog-unpin-backup-floor! kv pin-id)))))))))
