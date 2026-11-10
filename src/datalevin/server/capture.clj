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
  ([source] (vector-indices source true))
  ([source open?]
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
                 (if open?
                   (let [opts (:index-opts meta)
                         index (when (:dimensions opts) (v/open-vector-index kv opts))]
                     (when-not index
                       (raise "Cannot open a captured vector domain"
                              {:error :copy/vector-domain-unavailable :domain domain}))
                     (assoc indices domain index))
                   (reduced nil))))
             runtime metadata))))

(defn- index-writes? [stats indices]
  (or (seq indices)
      (some #(pos? (long (get stats % 0)))
            [:pending-count :running-count :failed-count])))

(defn- check-source! [source under-write-lock?]
  (let [raw (kv/raw-lmdb (source-kv source))]
    (when (or (i/key-compressor raw) (i/val-compressor raw))
      (raise "Remote copy does not yet transfer compression dictionaries"
             {:error :copy/compressed-source}))
    (when (or (l/writing? raw) (and under-write-lock? @(l/write-txn raw)))
      (raise "Cannot capture an open write transaction"
             {:error :copy/write-transaction-open}))))

(defn- unfinished-jobs? [stats]
  (some #(pos? (long (get stats % 0))) [:pending-count :running-count]))

(defn- check-failed-jobs! [stats]
  (when (pos? (long (get stats :failed-count 0)))
    (raise "Secondary index work failed before capture"
           {:error :copy/index-flush-failed :status stats})))

(defn- job-state! [source raw]
  (when (instance? Store source)
    ;; Read the version before scanning: a commit during the scan must also
    ;; invalidate this observation when capture acquires write exclusion.
    (let [txn-id (cpp/committed-txn-id raw)
          stats (st/secondary-index-status source)]
      (check-failed-jobs! stats)
      {:txn-id txn-id :status stats})))

(defn- check-embedding-state! [source]
  (when (instance? Store source)
    (let [kv (source-kv source)
          configs (:embedding-domains (i/opts source))
          stored (i/get-value kv c/opts :embedding-domains :attr :data)]
      (when-not (= (set (keys configs)) (set (keys stored)))
        (raise "Persisted embedding domains disagree with the source"
               {:error :copy/embedding-state-incomplete}))
      (doseq [[domain config] configs]
        (let [^VectorIndex index (get (.-embedding-indices ^Store source) domain)
              checkpoint (when index
                           (i/get-value kv c/vec-meta-dbi (.-domain index) :string :data))
              space [:dimensions :embedding-metadata]]
          (when-not (and (domains/complete-embedding-space? config)
                         (= (select-keys config space) (select-keys (get stored domain) space))
                         index (not (i/vec-closed? index))
                         (= (:dimensions config) (.-dimensions index)
                            (get-in checkpoint [:index-opts :dimensions]))
                         (number? (:total-bytes checkpoint)))
            (raise "Embedding space or checkpoint is incomplete at capture"
                   {:error :copy/embedding-state-incomplete :domain domain})))))))

(defn attempt!
  "Called with runtime lifetime protection, before acquiring a writer slot.
  Return :retry after contention or concurrent writes; release runtime access
  before waiting/reacquiring. Copying retains the prepared snapshot, never the
  writer slot, LMDB monitor, or vector locks. Callbacks provide server authorization,
  write admission, and an injectable phase observer."
  [source dest compact? {:keys [standalone? with-writer authorize-write! admit-write observer]
                        :or {observer (fn [_ _])}}]
  (check-source! source false)
  (let [kv (source-kv source)
        raw (kv/raw-lmdb kv)
        indices (vector-indices source)
        initial (job-state! source raw)
        writes? (index-writes? (:status initial) indices)
        writable! (fn []
                    (when (some #{:rdonly :rdonly-env} (i/get-env-flags raw))
                      (raise "Capture requires writes on a read-only source"
                             {:error :copy/read-only-source}))
                    (when writes? (authorize-write!)))
        _ (when writes? (writable!))
        checked (if (unfinished-jobs? (:status initial))
                  (do
                    ;; External providers and live workers run without capture
                    ;; locks. Retained history is inspected only in this phase.
                    (admit-write #(st/wait-for-secondary-index source
                                    {:timeout-ms 1 :process? true}))
                    (job-state! source raw))
                  initial)
        pin (volatile! nil)
        prepared-copy (volatile! nil)]
    (try
      (let [prepared
            (with-writer
              (fn []
                (locking (l/write-txn raw)
                  (observer :lmdb-locked {:source source})
                  (check-source! source true)
                  ;; A cold domain needs the store initialization monitor, which
                  ;; clients acquire before the LMDB monitor. Open it on retry
                  ;; outside write exclusion to preserve that lock order.
                  (let [indices (vector-indices source false)
                        writes? (index-writes? (:status checked) indices)
                        locks (volatile! [])]
                    (try
                      ;; Verify freshness in constant time. Never rescan job
                      ;; history while excluding writers, even after a change.
                      (if (or (nil? indices)
                              (unfinished-jobs? (:status checked))
                              (and checked
                                   (not= (:txn-id checked) (cpp/committed-txn-id raw)))
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
                                  (doseq [[domain index] indices]
                                    (when (i/vec-closed? index)
                                      (raise "A captured vector domain is closed"
                                             {:error :copy/vector-domain-unavailable :domain domain}))
                                    (observer :before-checkpoint {:domain domain})
                                    (i/persist-vecs index)))
                                (check-embedding-state! source)
                                (when-not standalone?
                                  (vreset! pin (kvtx/prepare-copy-backup-pin! kv raw dest compact?))
                                  (kvtx/notify-copy-backup-pin! @pin))
                                ;; Own cleanup before with-writer can fail its
                                ;; post-commit confirmation after this callback.
                                (vreset! prepared-copy (cpp/prepare-copy raw dest compact?)))]
                          (if (or writes? (and (not standalone?) (:wal? (i/env-opts kv))))
                            (do (when-not standalone? (writable!)) (admit-write prepare!))
                            (prepare!))))
                      (finally
                        (doseq [^Lock lock (rseq @locks)] (.unlock lock))))))))]
        (if (= :retry prepared)
          :retry
          (do
            (observer :prepared {:source source :prepared prepared})
            (cpp/copy-prepared! prepared)
            (observer :copied {:source source})
            (cond-> {:compact? (boolean compact?)}
              @pin (assoc :backup-pin {:pin-id (:pin-id @pin)
                                      :floor-lsn (:pin-floor-lsn @pin)
                                      :expires-ms (:pin-expires-ms @pin)})))))
      (finally
        (try
          (when-let [^java.lang.AutoCloseable prepared @prepared-copy]
            (.close prepared))
          (finally
            (when-let [pin-id (:pin-id @pin)]
              (try
                (with-writer #(admit-write (fn [] (kv/txlog-unpin-backup-floor! kv pin-id))))
                (catch Exception e
                  (vswap! (i/kv-info raw) assoc :copy-last-backup-unpin-error (ex-message e)))))))))))
