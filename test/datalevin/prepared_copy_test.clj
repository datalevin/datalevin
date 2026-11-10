(ns datalevin.prepared-copy-test
  (:require [clojure.test :refer [deftest is testing]]
            [datalevin.binding.cpp :as cpp]
            [datalevin.constants :as c]
            [datalevin.core :as d]
            [datalevin.interface :as i]
            [datalevin.kv :as kv]
            [datalevin.lmdb :as l]
            [datalevin.server.capture :as capture]
            [datalevin.server.copy :as copy]
            [datalevin.tx-group.compat :as group]
            [datalevin.util :as u])
  (:import [datalevin.binding.cpp CppLMDB PreparedCopy]
           [datalevin.cpp Env]
           [java.util.concurrent CountDownLatch TimeUnit]))

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
      (let [meta (copy/finalize-copy! dest "source" {:storage-version "0.0.0"})
            raw (cpp/open-existing-raw dest)]
        (try
          (is (= c/version (:storage-version meta)))
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

(deftest failed-post-commit-confirmation-releases-the-prepared-snapshot
  (doseq [compact? [false true] standalone? [false true]]
    (testing (str {:compact? compact? :standalone? standalone?})
      (let [root (u/tmp-dir (str "capture-confirmation-failure-" (random-uuid)))
            source (d/open-kv (str root "/source") {:wal? true})
            raw (kv/raw-lmdb source)
            ^Env env (.-env ^CppLMDB raw)
            dest (str root "/copy")
            prepared (volatile! nil)
            phases (atom [])
            failure (ex-info "HA write commit confirmation failed"
                             {:error :ha/write-indeterminate})]
        (try
          (i/open-dbi source "data")
          (i/transact-kv source [[:put "data" :value "captured"]])
          (u/create-dirs dest)
          (let [error
                (try
                  (capture/attempt! source dest compact?
                    {:standalone? standalone?
                     :authorize-write! (fn [])
                     :admit-write (fn [f] (f))
                     :observer (fn [phase _] (swap! phases conj phase))
                     :with-writer
                     (fn [f]
                       (group/with-confirmation
                         (fn []
                           (let [result (binding [kv/*server-write-slot-held?* true] (f))]
                             (when (instance? PreparedCopy result)
                               (vreset! prepared result)
                               (group/confirm-after-commit! #(throw failure)))
                             result))))})
                  nil
                  (catch Exception e e))]
            (is (identical? failure error))
            (is (some? @prepared))
            (is (not-any? #{:prepared :copied} @phases))
            (is (empty? (i/get-value raw c/kv-info c/wal-backup-pins :keyword :data)))
            (is (thrown? IllegalStateException (cpp/copy-prepared! @prepared)))
            (is (= :resized (do (.setMapSize env (* 1024 1024 1024)) :resized))))
          (finally
            ;; Keep a failing regression run from leaking its native resources.
            (when-let [^PreparedCopy snapshot @prepared] (.close snapshot))
            (d/close-kv source)
            (u/delete-files root)))))))

(deftest prepared-handles-protect-the-mapping-and-reject-owner-thread-teardown
  (let [root (u/tmp-dir (str "prepared-copy-lifetime-" (random-uuid)))
        source (d/open-kv (str root "/source")) dest (str root "/copy")
        ^Env env (.-env ^CppLMDB (kv/raw-lmdb source))
        entered (CountDownLatch. 1)]
    (try
      (u/create-dirs dest)
      (with-open [prepared (cpp/prepare-copy (kv/raw-lmdb source) dest false)]
        (is (thrown? IllegalStateException (.close env)))
        (is (thrown? IllegalStateException (.setMapSize env (* 1024 1024 1024))))
        (let [remap (future (.countDown entered) (.setMapSize env (* 1024 1024 1024)) :remapped)]
          (is (.await entered 5 TimeUnit/SECONDS))
          (is (= ::blocked (deref remap 100 ::blocked)))
          (.close prepared)
          (is (= :remapped (deref remap 5000 ::timeout)))))
      (finally (d/close-kv source) (u/delete-files root)))))
