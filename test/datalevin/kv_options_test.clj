(ns datalevin.kv-options-test
  (:require
   [clojure.test :refer [deftest is testing use-fixtures]]
   [datalevin.core :as d]
   [datalevin.interface :as i]
   [datalevin.kv.snapshot :as snapshot]
   [datalevin.kv.txlog :as kvtx]
   [datalevin.lmdb :as l]
   [datalevin.util :as u]))

(def ^:dynamic *dir* nil)

(use-fixtures :each
  (fn [f]
    (binding [*dir* (u/tmp-dir (str "kv-options-" (random-uuid)))]
      (try (f)
           (finally
             (when (u/file-exists *dir*) (u/delete-files *dir*)))))))

(deftest options-without-local-state
  (let [options (atom {:snapshot-compact? false})
        db (reify i/ILMDB
             (kv-info [_] nil)
             (env-opts [_] @options))]
    (is (identical? @options (l/read-env-opts db)))
    (is (false? (snapshot/snapshot-compact? db)))
    (reset! options {:snapshot-compact? true :snapshot-scheduler? true})
    (is (true? (snapshot/snapshot-compact? db)))
    (is (true? (snapshot/snapshot-scheduler-enabled? db)))))

(deftest shared-options-stay-current
  (doseq [wal? [false true]]
    (testing (str "WAL enabled: " wal?)
      (let [db (d/open-kv (str *dir* "/" wal?)
                         {:wal? wal? :spill-opts {:spill-threshold 100}})]
        (try
          (d/open-dbi db "data")
          (let [info (i/kv-info db)
                before (l/read-env-opts db)]
            (is (identical? @info before))
            (is (true? (snapshot/snapshot-compact? db)))
            (is (not (contains? before :snapshot-compact?)))
            (vswap! info assoc :snapshot-compact? false
                    :snapshot-scheduler? nil :wal-rollout-mode :rollback)
            (is (false? (snapshot/snapshot-compact? db)))
            (is (false? (snapshot/snapshot-scheduler-enabled? db)))
            (is (= :rollback (kvtx/txlog-rollout-mode db)))
            (is (not (contains? before :snapshot-compact?))
                "an earlier option snapshot stays immutable")
            (vswap! info assoc :wal-rollout-mode :active)
            (d/with-transaction-kv [tx db]
              (is (identical? info (i/kv-info tx)))
              (is (identical? (l/read-env-opts db) (l/read-env-opts tx)))
              (vswap! info assoc :spill-opts {:spill-threshold 90})
              (is (= 90 (get-in (l/read-env-opts tx) [:spill-opts :spill-threshold])))
              (d/transact-kv tx "data" [[:put 1 "value"]] :long :string)
              (is (= ["value"] (vec (d/get-range tx "data" [:all] :long :string true)))))
            (is (= ["value"] (vec (d/get-range db "data" [:all] :long :string true))))
            (is (= 90 (get-in (i/env-opts db) [:spill-opts :spill-threshold])))
            (doseq [key [:compression :dbis :custom-dbis :types
                        :custom-types-revision :custom-value-id :custom-type-cache
                        :custom-payload-dbi-open? :runtime-opts]]
              (is (not (contains? (i/env-opts db) key)))))
          (finally (d/close-kv db)))))))
