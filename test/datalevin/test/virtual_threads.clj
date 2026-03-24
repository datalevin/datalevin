(ns datalevin.test.virtual-threads
  (:require
   [clojure.test :refer [deftest is]]
   [datalevin.binding.cpp]
   [datalevin.bits :as b]
   [datalevin.interface :as i]
   [datalevin.lmdb :as l]
   [datalevin.util :as u])
  (:import
   [java.nio ByteBuffer]
   [java.util UUID]
   [java.util.concurrent Callable CountDownLatch Executors Future TimeUnit]))

(defn- read-string-value
  [dbi rtx k]
  (l/put-key rtx k :string)
  (some-> ^ByteBuffer (l/get-kv dbi rtx) (.rewind) (b/read-buffer :string)))

(defn- virtual-reader-result
  [db ^CountDownLatch ready ^CountDownLatch release]
  (try
    (when-not (.isVirtual (Thread/currentThread))
      (throw (ex-info "expected virtual thread" {})))
    (let [dbi (i/get-dbi db "a")
          rtx (i/get-rtx db)]
      (try
        (when-not (= "v" (read-string-value dbi rtx "k"))
          (throw (ex-info "unexpected value before park" {})))
        (.countDown ready)
        (when-not (.await release 10 TimeUnit/SECONDS)
          (throw (ex-info "timed out waiting for release" {})))
        ;; Without MDB_NOTLS, overlapping read txns on virtual threads can fail
        ;; after park/unpark with MDB_BAD_RSLOT because LMDB tracks readers by
        ;; OS thread instead of by txn object.
        (dotimes [_ 20]
          (Thread/sleep 1)
          (when-not (= "v" (read-string-value dbi rtx "k"))
            (throw (ex-info "unexpected value after park" {}))))
        :ok
        (catch Throwable t
          t)
        (finally
          (i/return-rtx db rtx))))
    (catch Throwable t
      (.countDown ready)
      t)))

(defn- result-summary
  [result]
  (if (= :ok result)
    result
    {:class   (some-> result class .getName)
     :message (ex-message result)}))

(deftest test-virtual-threads-can-overlap-live-readers
  (let [dir          (u/tmp-dir (str "virtual-threads-test-" (UUID/randomUUID)))
        reader-count 64
        ready        (CountDownLatch. reader-count)
        release      (CountDownLatch. 1)]
    (try
      (let [db (l/open-kv dir)]
        (try
          (i/open-dbi db "a")
          (i/transact-kv db "a" [[:put "k" "v"]] :string :string)
          (with-open [executor (Executors/newVirtualThreadPerTaskExecutor)]
            (let [futures (mapv
                           (fn [_]
                             (.submit executor
                                      ^Callable
                                      #(virtual-reader-result db ready release)))
                           (range reader-count))]
              (is (.await ready 10 TimeUnit/SECONDS)
                  "all virtual readers should reach the barrier")
              (.countDown release)
              (let [results (mapv #(.get ^Future % 10 TimeUnit/SECONDS)
                                  futures)]
                (is (every? #{:ok} results)
                    (pr-str (mapv result-summary (remove #{:ok} results)))))))
          (finally
            (i/close-kv db))))
      (finally
        (u/delete-files dir)))))
