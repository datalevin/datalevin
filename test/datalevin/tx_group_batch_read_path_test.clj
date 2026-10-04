(ns datalevin.tx-group-batch-read-path-test
  "Private reads use LMDB directly while healthy and stop at failure fencing."
  (:require [clojure.test :refer [deftest is testing]]
            [datalevin.bits :as bits]
            [datalevin.interface :as i]
            [datalevin.lmdb :as l]
            [datalevin.tx-group.batch :as batch]
            [datalevin.tx-group.batch.charge :as charge]
            [datalevin.tx-group.batch.env :as env]
            [datalevin.tx-group.batch.private :as private]
            [datalevin.tx-state.lifetime :as lifetime]
            [datalevin.txlog :as wal]
            [datalevin.util :as u])
  (:import [java.nio ByteBuffer]))

(defn- put-long! [c k v]
  (batch/submit!
   c {:allowance (charge/blind-allowance {:declared-bytes 128 :scratch-bytes 16384})
      :prepare (fn [_]
                 (let [key (ByteBuffer/allocate 9)
                       value (ByteBuffer/allocate 9)
                       _ (bits/put-buffer key k :long)
                       _ (bits/put-buffer value v :long)
                       rows (java.util.Collections/singletonList
                             (l/kv-tx :put "data" (.array key) (.array value) :raw :raw))]
                   {:rows rows :wal-body (wal/prepare-append-body rows {})
                    :result :ok}))}))

(deftest private-native-reads-use-lmdb-and-stop-at-runtime-failure
  (let [dir (u/tmp-dir (str "wal-read-path-" (random-uuid)))
        opts {:dir dir :db-identity (str (random-uuid))
              :wal-durability-profile :strict :wal-sync-mode :fsync
              :wal-segment-prealloc? false}
        environment (private/open! opts)
        {:keys [raw]} (env/resources environment)]
    (try
      (i/open-dbi raw "data")
      (dotimes [k 8] (put-long! (env/collector environment) k (* k 2)))
      (testing "ordinary point, range and count calls use the native handle"
        (is (= 6 (i/get-value raw "data" 3 :long :long)))
        (is (nil? (i/get-value raw "data" 100 :long :long)))
        (is (= [[2 4] [3 6] [4 8]]
               (vec (i/get-range raw "data" [:closed 2 4] :long :long))))
        (is (= 5 (i/range-count raw "data" [:closed 0 4] :long))))
      (testing "a runtime failure fences writes and new LMDB reads"
        (let [failure (ex-info "fenced" {:error :txlog/runtime-fenced})
              wal-state (:wal-state (env/resources environment))]
          ((:on-failure! (wal/runtime-control wal-state)) failure))
        (is (= :txlog/native-fenced
               (:error (ex-data (try (i/get-value raw "data" 3 :long :long)
                                     (catch clojure.lang.ExceptionInfo e e))))))
        (is (= :txlog/native-fenced
               (:error (ex-data (try (i/get-range raw "data" [:all] :long :long)
                                     (catch clojure.lang.ExceptionInfo e e))))))
        (is (= :txlog/native-fenced
               (:error (ex-data (try (i/range-count raw "data" [:all] :long)
                                     (catch clojure.lang.ExceptionInfo e e))))))
        (is (thrown? clojure.lang.ExceptionInfo
                     (put-long! (env/collector environment) 3 99))))
      (testing "native close rejects reads after draining its lifetime guard"
        (env/close! environment)
        (is (thrown? clojure.lang.ExceptionInfo
                     (i/get-value raw "data" 3 :long :long))))
      (finally
        (env/close! environment)
        (u/delete-files dir)))))

(deftest borrowed-native-reader-blocks-native-teardown
  (let [dir (u/tmp-dir (str "wal-read-borrow-" (random-uuid)))
        opts {:dir dir :db-identity (str (random-uuid))
              :wal-durability-profile :strict :wal-sync-mode :fsync
              :wal-segment-prealloc? false}
        environment (private/open! opts)
        {:keys [raw]} (env/resources environment)]
    (try
      (i/open-dbi raw "data")
      (dotimes [k 8] (put-long! (env/collector environment) k (* k 2)))
      (let [native-lifetime (:native-lifetime @(i/kv-info raw))
            cursor (i/range-seq raw "data" [:all] :long :long)
            _ (is (= [0 0] (first cursor)))
            _ (is (pos? (:native-users (lifetime/state native-lifetime))))
            close-future (future
                           (try (env/close! environment) :closed
                                (catch Throwable t t)))]
        (Thread/sleep 150)
        (is (not (realized? close-future))
            "native teardown waits while a reader borrow is outstanding")
        (.close ^java.lang.AutoCloseable cursor)
        (is (= :closed (deref close-future 5000 ::timeout)))
        (is (zero? (:native-users (lifetime/state native-lifetime)))))
      (finally
        (u/delete-files dir)))))
