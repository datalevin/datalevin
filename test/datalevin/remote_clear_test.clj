(ns datalevin.remote-clear-test
  (:require [clojure.test :refer [deftest is]]
            [datalevin.core :as d]
            [datalevin.interface :as i]
            [datalevin.kv :as kv]
            [datalevin.server :as server]
            [datalevin.txlog :as wal]
            [datalevin.txlog.segment :as segment]
            [datalevin.util :as u])
  (:import [java.net ServerSocket]))

(deftest remote-clear-is-recorded-in-wal
  (let [root (u/tmp-dir (str "remote-clear-" (random-uuid)))
        port (with-open [socket (ServerSocket. 0)] (.getLocalPort socket))
        srv (server/create {:root root :port port})
        stopped? (volatile! false)
        native-dir (atom nil)
        clear-record (atom nil)
        backup (str root "/before-clear")]
    (try
      (server/start srv)
      (let [db (d/open-kv (str "dtlv://datalevin:datalevin@localhost:" port "/kv")
                          {:wal? true :wal-segment-prealloc? false})]
        (try
          (d/open-dbi db "values")
          (d/transact-kv db [[:put "values" 1 "old" :long :string]])
          (let [raw (kv/raw-lmdb (#'server/get-kv-store srv "kv"))]
            (reset! native-dir (i/env-dir raw))
            (i/copy raw backup false))
          (let [before (:last-committed-lsn (d/txlog-watermarks db))]
            (d/clear-dbi db "values")
            (is (zero? (d/entries db "values")))
            (is (= (inc before) (:last-committed-lsn (d/txlog-watermarks db))))
            (let [raw (kv/raw-lmdb (#'server/get-kv-store srv "kv"))
                  state (wal/state raw)
                  records (:records (segment/scan-segment
                                      (wal/segment-path (:dir state) 1)))
                  record (wal/decode-commit-row-payload (:body (last records)))]
              (reset! clear-record record)
              (is (= (inc before) (:lsn record)))
              (is (some #(and (= :clear (first %)) (= "values" (second %)))
                        (:ops record)))))
          (finally (d/close-kv db))))
      (server/stop srv)
      (vreset! stopped? true)
      ;; Roll native data back to before the clear, retaining its newer WAL.
      ;; Apply the recorded physical rows through the recovery entry point.
      (u/copy-file (str backup "/data.mdb") (str @native-dir "/data.mdb"))
      (let [db (d/open-kv @native-dir {:wal? true})]
        (try (is (= "old" (d/get-value db "values" 1 :long :string)))
             (kv/replay-txlog-rows! db (:ops @clear-record) (:lsn @clear-record))
             (is (zero? (d/entries db "values")))
             (finally (d/close-kv db))))
      (finally
        (when-not @stopped? (server/stop srv))
        (u/delete-files root)))))
