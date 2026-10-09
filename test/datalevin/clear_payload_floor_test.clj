(ns datalevin.clear-payload-floor-test
  (:require [clojure.test :refer [deftest is]]
            [datalevin.constants :as c]
            [datalevin.core :as d]
            [datalevin.interface :as i]
            [datalevin.kv :as kv]
            [datalevin.tx-group.batch :as batch]
            [datalevin.txlog :as wal]
            [datalevin.txlog.codec :as codec]
            [datalevin.txlog.segment :as segment]
            [datalevin.util :as u]))

(defn- appended-lsns [state floor]
  (->> (wal/segment-files (:dir state))
       (mapcat #(-> (segment/scan-segment (str (:file %))
                                         {:allow-preallocated-tail? true})
                    :records))
       (map #(codec/decode-commit-row-payload (:body %)))
       (filter #(> (:lsn %) floor))
       (mapv :lsn)))

(deftest recovered-batches-skip-general-readiness-and-validate-payload-floor
  (let [dir (u/tmp-dir (str "batch-payload-floor-" (random-uuid)))
        db (d/open-kv dir {:wal? true :snapshot-scheduler? false})
        floor 1000000]
    (try
      (d/open-dbi db "data")
      (let [raw (kv/raw-lmdb db)
            state (wal/state raw)]
        (is (:txlog-recovered? @(i/kv-info raw)))
        (kv/transact-kv-without-txlog!
          db [[:put c/kv-info c/wal-local-payload-lsn floor :keyword :data]])
        (with-redefs [kv/ensure-txlog-ready!
                      (fn [& _]
                        (throw (ex-info "Unexpected general readiness check" {})))]
          (d/transact-kv db "data" [[:put 1 :first]])
          (d/transact-kv db "data" [[:put 2 :second]]))
        (is (= (+ floor 3) @(:next-lsn state)))
        (is (= :first (d/get-value db "data" 1)))
        (is (= :second (d/get-value db "data" 2)))
        (is (= (+ floor 2)
               (i/get-value raw c/kv-info c/wal-local-payload-lsn :keyword :data))))
      (finally (d/close-kv db) (u/delete-files dir)))))

(deftest local-clear-refreshes-persisted-payload-floor-before-append
  (doseq [profile [:strict :relaxed]]
    (let [dir (u/tmp-dir (str "clear-payload-floor-" (random-uuid)))
          opts {:wal? true :wal-durability-profile profile
                :snapshot-scheduler? false}
          db (d/open-kv dir opts)
          floor 1000000]
      (try
        (d/open-dbi db "data")
        (d/transact-kv db "data" [[:put 1 :before]])
        (let [raw (kv/raw-lmdb db)
              state (wal/state raw)
              collector (:collector (:independent-control @(i/kv-info raw)))
              next-before @(:next-lsn state)]
          (is (< next-before floor))
          ;; Simulate payload metadata installed ahead of the local WAL cursor.
          (kv/transact-kv-without-txlog!
            db [[:put c/kv-info c/wal-local-payload-lsn floor :keyword :data]])
          (is (= next-before @(:next-lsn state)))
          (is (nil? (d/clear-dbi db "data")))
          (is (nil? (d/get-value db "data" 1)))
          (is (= (+ floor 2) @(:next-lsn state)))
          (is (= (inc floor) (batch/published-lsn collector)))
          (is (= (inc floor)
                 (i/get-value raw c/kv-info c/wal-local-payload-lsn :keyword :data)))
          (is (= [(inc floor)] (appended-lsns state floor)))
          ;; The next ordinary write must continue after the admin record.
          (d/transact-kv db "data" [[:put 2 :after]])
          (kv/force-txlog-sync! db)
          (is (= [(inc floor) (+ floor 2)]
                 (appended-lsns state floor)))
          (d/close-kv db)
          (let [reopened (d/open-kv dir opts)]
            (try
              (is (nil? (d/get-value reopened "data" 1)))
              (is (= :after (d/get-value reopened "data" 2)))
              (finally (d/close-kv reopened)))))
        (finally (d/close-kv db) (u/delete-files dir))))))
