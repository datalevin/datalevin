(ns datalevin.tx-group-batch-view-test
  "Contract tests for the private standalone read path.

  Uses the fresh-store private opener so the native store and the collector's
  serving status are the same canonical runtime a real read handle binds to."
  (:require [clojure.test :refer [deftest is testing]]
            [datalevin.bits :as bits]
            [datalevin.interface :as i]
            [datalevin.lmdb :as l]
            [datalevin.tx-group.batch :as batch]
            [datalevin.tx-group.batch.charge :as charge]
            [datalevin.tx-group.batch.env :as env]
            [datalevin.tx-group.batch.private :as private]
            [datalevin.tx-group.batch.view :as view]
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

(deftest private-view-reads-point-range-and-count
  (let [dir (u/tmp-dir (str "wal-view-" (random-uuid)))
        opts {:dir dir :db-identity (str (random-uuid))
              :wal-durability-profile :strict :wal-sync-mode :fsync
              :wal-segment-prealloc? false}
        environment (private/open! opts)
        {:keys [raw]} (env/resources environment)
        handle (view/read-handle environment)]
    (try
      (i/open-dbi raw "data")
      (dotimes [k 8] (put-long! (env/collector environment) k (* k 2)))
      (testing "point read"
        (is (= 6 (view/get-value handle "data" 3 :long :long)))
        (is (nil? (view/get-value handle "data" 100 :long :long))))
      (testing "eager range read"
        (is (= [[2 4] [3 6] [4 8]]
               (vec (view/get-range handle "data" [:closed 2 4] :long :long)))))
      (testing "eager range count"
        (is (= 5 (view/range-count handle "data" [:closed 0 4] :long))))
      (testing "a fenced runtime rejects before native access"
        (batch/fence! (env/collector environment)
                      (ex-info "fenced" {:error :txlog/runtime-fenced}))
        (is (thrown? clojure.lang.ExceptionInfo
                     (view/get-value handle "data" 3 :long :long)))
        (is (thrown? clojure.lang.ExceptionInfo
                     (view/range-count handle "data" [:all] :long))))
      (finally
        (env/close! environment)
        (u/delete-files dir)))))

(deftest a-borrowed-native-reader-blocks-native-teardown
  (let [dir (u/tmp-dir (str "wal-view-borrow-" (random-uuid)))
        opts {:dir dir :db-identity (str (random-uuid))
              :wal-durability-profile :strict :wal-sync-mode :fsync
              :wal-segment-prealloc? false}
        environment (private/open! opts)
        {:keys [raw]} (env/resources environment)]
    (try
      (i/open-dbi raw "data")
      (dotimes [k 8] (put-long! (env/collector environment) k (* k 2)))
      (testing "the private opener binds the native lifetime guard"
        (is (some? (:native-lifetime @(i/kv-info raw)))))
      (let [cursor (i/range-seq raw "data" [:all] :long :long)
            _ (is (= [0 0] (first cursor)))
            native-lifetime (:native-lifetime @(i/kv-info raw))
            _ (testing "the lazy reader registers a native borrow"
                (is (pos? (:native-users (lifetime/state native-lifetime)))))
            close-future (future
                           (try (env/close! environment) :closed
                                (catch Throwable t t)))]
        (Thread/sleep 150)
        (is (not (realized? close-future))
            "native teardown waits while a reader borrow is outstanding")
        (.close ^java.lang.AutoCloseable cursor)
        (is (= :closed (deref close-future 5000 ::timeout)))
        (testing "teardown drains the borrow before freeing native resources"
          (is (zero? (:native-users (lifetime/state native-lifetime))))))
      (finally
        (u/delete-files dir)))))
