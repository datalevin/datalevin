(ns datalevin.open-failure-test
  (:require [clojure.test :refer [deftest is]]
            [datalevin.core :as d]
            [datalevin.lmdb :as l]
            [datalevin.server.dispatch :as dispatch]
            [datalevin.util :as u])
  (:import [java.nio.channels ClosedByInterruptException]))

(deftest interrupted-open-preserves-cause-and-releases-ownership
  (let [dir (u/tmp-dir (str "interrupted-open-" (random-uuid)))
        wrapper @#'l/open-kv-wrapper
        original @wrapper
        interrupted (ClosedByInterruptException.)]
    (try
      (let [db (d/open-kv dir {:wal? true})]
        (d/close-kv db))
      ;; Fault after native initialization, where WAL recovery opens its files.
      (l/set-open-kv-wrapper! (fn [_] (throw interrupted)))
      (let [failure (try (d/open-kv dir {:wal? true})
                         (catch Exception e e))]
        (is (identical? interrupted (ex-cause failure)))
        (is (= dir (:dir (ex-data failure))))
        (is (dispatch/client-disconnect? failure)))
      (l/set-open-kv-wrapper! original)
      ;; Cleanup must release both the native handle and the WAL open lease.
      (let [db (d/open-kv dir {:wal? true})]
        (try (is (not (d/closed-kv? db)))
             (finally (d/close-kv db))))
      (finally
        (l/set-open-kv-wrapper! original)
        (u/delete-files dir)))))
