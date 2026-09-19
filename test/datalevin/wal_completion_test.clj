(ns datalevin.wal-completion-test
  (:require [clojure.test :refer [deftest is testing]]
            [datalevin.txlog :as wal]
            [datalevin.txlog.meta :as meta]
            [datalevin.util :as u])
  (:import [java.io Closeable IOException]))

(defn- with-runtime [f]
  (let [dir (u/tmp-dir (str "wal-completion-" (random-uuid)))
        {:keys [state]}
        (wal/init-runtime-state
          {:dir dir :wal? true :wal-shared? false
           :wal-durability-profile :strict :wal-sync-mode :fdatasync
           :wal-segment-prealloc? false}
          nil)]
    (try
      (f state)
      (finally
        (doseq [k [:segment-channel :sync-lock-channel]]
          (when-let [^Closeable ch (some-> (get state k) deref)] (.close ch)))
        (u/delete-files dir)))))

(defn- manager-metrics [state]
  (select-keys (wal/sync-manager-state (:sync-manager state))
               [:last-appended-lsn :last-durable-lsn :pending-count
                :pending-queue-size :sync-requested? :sync-in-progress?
                :healthy? :failure :forced-sync-count :batched-sync-count
                :sync-count-by-reason :last-sync-reason
                :last-fsync-ms :commit-wait-sample-count
                :commit-wait-count-by-reason]))

(deftest private-refresh-can-omit-snapshots
  (with-runtime
    (fn [state]
      (doseq [refresh [wal/refresh-shared-state! meta/refresh-shared-watermarks!]]
        (is (nil? (refresh state false)))
        (is (= (refresh state) (refresh state true)))
        (is (= 0 (:last-committed-lsn (refresh state)))))
      (let [record (wal/append-durable! state [[:put "dbi" 1 "v" :id :string]] {})]
        (wal/note-commit-applied! state record)
        (doseq [refresh [wal/refresh-shared-state! meta/refresh-shared-watermarks!]]
          (is (nil? (refresh state false)))
          (is (= {:last-committed-lsn 1 :last-durable-lsn 1 :last-applied-lsn 1}
                 (select-keys (refresh state)
                              [:last-committed-lsn :last-durable-lsn :last-applied-lsn]))))))))

(deftest shared-refresh-still-reconciles-without-snapshot
  (with-runtime
    (fn [state]
      (let [record (wal/append-durable! state [[:put "dbi" 1 "v" :id :string]] {})
            shared (assoc state :wal-shared? true)
            manager (:sync-manager state)]
        (wal/note-commit-applied! state record)
        (wal/flush-meta! state)
        (vreset! (:last-durable-lsn manager) 0)
        (vreset! (:meta-last-applied-lsn state) 0)
        (is (nil? (meta/refresh-shared-watermarks! shared false)))
        (is (= 1 @(:last-durable-lsn manager)))
        (is (= 1 @(:meta-last-applied-lsn state)))
        ;; A stale append cursor must still be reconciled with the segment.
        (vreset! (:segment-offset state) 0)
        (vreset! (:next-lsn state) 1)
        (is (nil? (wal/refresh-shared-state! shared false)))
        (is (= (:size record) @(:segment-offset state)))
        (is (= 2 @(:next-lsn state)))
        (is (= 1 (:last-committed-lsn (wal/refresh-shared-state! shared))))))))

(deftest synchronous-completion-preserves-metrics-and-hook-order
  (doseq [reason [:forced :batch-count :batch-time]]
    (testing (name reason)
      (let [results
            (for [fast? [false true]]
              (with-runtime
                (fn [state]
                  (let [;; Both channels really use DSYNC. The control invokes
                        ;; the generic completion path without an extra force.
                        state (cond-> state (not fast?)
                                (assoc :sync-on-write? false :sync-mode :none))
                        manager (:sync-manager state)
                        seen (atom [])]
                    (vreset! (:group-commit manager) (if (= reason :batch-count) 1 1000))
                    (vreset! (:group-commit-ms manager) (if (= reason :batch-time) 1 1000000))
                    (vreset! (:last-sync-ms manager)
                             (if (= reason :batch-time) 0 (System/currentTimeMillis)))
                    (let [record
                          (wal/append-durable!
                            state [[:put "dbi" 1 "v" :id :string]]
                            {:before-sync!
                             (fn [s begin]
                               (is (false? (Thread/holdsLock (:append-lock s))))
                               (is (false? (Thread/holdsLock (:monitor manager))))
                               (is (= 0 @(:last-durable-lsn manager)))
                               (is (true? @(:sync-in-progress? manager)))
                               (is (= 1 (count (:records (wal/scan-segment
                                                          (wal/segment-path (:dir s) 1))))))
                               (swap! seen conj begin))})]
                      (is (:synced? record))
                      (is (= [{:target-lsn 1 :reason reason}] @seen))
                      (is (= 2 @(:meta-dirty-count state)))
                      (is (= 1 (:commit-wait-sample-count
                                 (wal/sync-manager-state manager))))
                      (manager-metrics state))))))]
        (is (apply = results))))))

(defn- await-pending-second [manager]
  (let [deadline (+ (System/nanoTime) 5000000000)]
    (loop []
      (cond
        (= 2 @(:last-appended-lsn manager)) true
        (>= (System/nanoTime) deadline) false
        :else (do (Thread/sleep 1) (recur))))))

(deftest synchronous-completion-releases-waiters-on-success-and-failure
  (doseq [fail? [false true]]
    (testing (str "failure=" fail?)
      (with-runtime
        (fn [state]
          (let [manager (:sync-manager state)
                entered (promise)
                release (promise)
                failure (IOException. "Injected completion failure")
                fatal (atom nil)
                append (fn [key]
                         (try
                           (wal/append-durable!
                             state [[:put "dbi" key "v" :id :string]]
                             {:mark-fatal! (fn [_ e] (reset! fatal e))
                              :before-sync!
                              (fn [_ {:keys [target-lsn]}]
                                (when (= target-lsn 1)
                                  (deliver entered true)
                                  (when-not (deref release 5000 false)
                                    (throw (ex-info "Test did not release completion" {})))
                                  (when fail? (throw failure))))})
                           (catch Exception e e)))
                first-result (future (append 1))
                second-result (atom nil)]
            (try
              (is (true? (deref entered 5000 false)))
              (reset! second-result (future (append 2)))
              (is (await-pending-second manager))
              (is (= 0 @(:last-durable-lsn manager)))
              (is (= ::waiting (deref @second-result 20 ::waiting)))
              (deliver release true)
              (let [a (deref first-result 5000 ::timeout)
                    b (deref @second-result 5000 ::timeout)
                    metrics (manager-metrics state)]
                (if fail?
                  (do
                    (is (identical? failure a))
                    (is (instance? Exception b))
                    (is (identical? failure @fatal))
                    (is (false? (:healthy? metrics)))
                    (is (= 0 (:last-durable-lsn metrics)))
                    (is (= 0 (:commit-wait-sample-count metrics))))
                  (do
                    (is (= [1 2] [(:lsn a) (:lsn b)]))
                    (is (and (:synced? a) (:synced? b)))
                    (is (= 2 (:last-durable-lsn metrics)))
                    (is (= 2 (:commit-wait-sample-count metrics)))
                    (is (= 0 (:pending-count metrics)))))
                (is (false? (:sync-in-progress? metrics))))
              (finally
                (deliver release true)
                (deref first-result 5000 nil)
                (when-let [f @second-result] (deref f 5000 nil))))))))))

(deftest synchronous-append-failure-does-not-publish-durability
  (with-runtime
    (fn [state]
      (.close ^Closeable @(:segment-channel state))
      (is (thrown? Exception
                   (wal/append-durable! state [[:put "dbi" 1 "v" :id :string]] {})))
      (is (= 0 @(:last-durable-lsn (:sync-manager state))))
      (is (= 1 @(:next-lsn state))))))
