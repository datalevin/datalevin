(ns datalevin.txlog.transfer-test
  (:require
   [clojure.test :refer [deftest is testing]]
   [datalevin.constants :as c]
   [datalevin.remote :as remote]
   [datalevin.txlog.codec :as codec]
   [datalevin.txlog.transfer :as transfer])
  (:import [java.nio ByteBuffer]))

(defn- record [lsn]
  (let [body (codec/encode-commit-row-payload lsn 123 [[:put "dbi" lsn lsn]])]
    {:segment-id 1 :offset 0 :major 2 :flags 0 :body body
     :checksum (codec/current-record-checksum (alength ^bytes body) false body)}))

(deftest transfer-rejects-damaged-and-malformed-batches
  (let [batch (transfer/encode-batch [(record 1) (record 2)])
        bytes ^bytes (:data batch)]
    (is (= [1 2] (mapv :lsn (transfer/decode-batch batch))))
    (doseq [bad [(assoc batch :format :unknown)
                 (assoc batch :data (byte-array 2))
                 (assoc batch :data (byte-array (concat bytes [0])))
                 (assoc batch :data (doto (aclone bytes)
                                     (aset-byte (dec (alength bytes)) (byte 99))))
                 (assoc batch :data (.array (doto (ByteBuffer/wrap (aclone bytes))
                                              (.putInt 0 Integer/MAX_VALUE))))
                 (assoc batch :data (.array (doto (ByteBuffer/wrap (aclone bytes))
                                              (.putInt 22 Integer/MAX_VALUE))))]]
      (is (= :txlog/corrupt
             (try (transfer/decode-batch bad) nil
                  (catch clojure.lang.ExceptionInfo e (:type (ex-data e)))))))))

(deftest transfer-cache-shares-loads-and-retries-after-failure
  (doseq [fail? [false true]]
    (testing (str "failed load: " fail?)
      (let [cache (transfer/create-cache)
            key [{:lsn 1 :payload-bytes 10}]
            entered (promise) release (promise)
            calls (atom 0)
            batch (transfer/encode-batch [(record 1)])
            loader #(do (swap! calls inc) (deliver entered true)
                        (assert (deref release 5000 false))
                        (if fail? (throw (ex-info "read failed" {})) batch))
            run #(try (transfer/cached-batch cache key loader) (catch Throwable t t))
            owner (future (run))]
        (try
          (is (deref entered 5000 false))
          (let [followers (mapv (fn [_] (future (run))) (range 8))]
            (deliver release true)
            (doseq [job (cons owner followers)]
              (let [result (deref job 5000 ::timeout)]
                (is (if fail? (instance? Throwable result) (identical? batch result)))))
            (when-not fail? (is (= 1 @calls))))
          (is (= {} (:pending @cache)))
          (is (identical? batch (transfer/cached-batch cache key (constantly batch))))
          (finally (deliver release true) (deref owner 5000 nil)))))))

(deftest transfer-cache-bounds-memory-and-evicts-least-recent-batches
  (let [cache (transfer/create-cache)
        batch (transfer/encode-batch [(record 1)])
        calls (atom [])
        load-key (fn [lsn]
                   (transfer/cached-batch cache [{:lsn lsn :payload-bytes 10}]
                                          #(do (swap! calls conj lsn) batch)))]
    (binding [c/*wal-transfer-cache-bytes* 540 c/*wal-transfer-cache-batches* 8]
      (doseq [lsn [1 2 1 3 1 2]] (load-key lsn))
      (is (= [1 2 3 2] @calls))
      (is (<= (:bytes @cache) 540)))
    (binding [c/*wal-transfer-cache-bytes* 0]
      (load-key 1)
      (load-key 1)
      (is (= [1 2 3 2 1 1] @calls))))
  (binding [c/*wal-transfer-cache-batches* 1]
    (let [cache (transfer/create-cache)]
      (doseq [lsn [1 2]]
        (transfer/cached-batch cache [{:lsn lsn}] #(transfer/encode-batch [(record lsn)])))
      (is (= 1 (count (:entries @cache)))))))

(deftest fetch-prefers-encoded-batches-and-keeps-legacy-fallback-narrow
  (let [batch (transfer/encode-batch [(record 1)])
        calls (atom [])
        client (Object.)
        request (fn [actual type args writing?]
                  (is (identical? client actual))
                  (is (= ["db" 1 2] args))
                  (is (false? writing?))
                  (swap! calls conj type)
                  batch)]
    (is (= [1] (mapv :lsn (remote/fetch-tx-log-rows client "db" 1 2 request))))
    (is (= [:open-tx-log-batch] @calls)))
  (let [client (Object.) calls (atom []) rows [{:lsn 1 :rows []}]
        request (fn [_ type _ _]
                  (swap! calls conj type)
                  (if (= type :open-tx-log-batch)
                    (throw (ex-info "Request failed"
                                    {:server-message "Unknown message type :open-tx-log-batch"}))
                    rows))]
    (dotimes [_ 2] (is (= rows (remote/fetch-tx-log-rows client "db" 1 2 request))))
    (is (= [:open-tx-log-batch :open-tx-log-rows :open-tx-log-rows] @calls)))
  (doseq [reply [(fn [& _] (throw (ex-info "permission denied" {:error :permission})))
                 (fn [& _] {:format :datalevin/wal-batch-v1 :data (byte-array 1)})]]
    (let [calls (atom 0)]
      (is (thrown? clojure.lang.ExceptionInfo
                   (remote/fetch-tx-log-rows (Object.) "db" 1 2
                                            #(do (swap! calls inc) (apply reply %&)))))
      (is (= 1 @calls)))))
