(ns datalevin.serialization-context-test
  (:require
   [clojure.test :refer [deftest is]]
   [datalevin.bits :as b]
   [datalevin.constants :as c]
   [datalevin.protocol :as p]
   [taoensso.nippy :as nippy])
  (:import
   [java.nio ByteBuffer]
   [java.util.concurrent.atomic AtomicInteger]))

(defn- wire-roundtrip [value opts]
  (let [buffer (ByteBuffer/allocate 65536)]
    (p/write-message-bf buffer value c/message-format-nippy opts)
    (first (p/receive-one-message buffer opts))))

(defn- storage-roundtrip [value]
  (let [buffer (ByteBuffer/allocateDirect 65536)]
    (b/put-buffer buffer value)
    (.flip buffer)
    (b/read-buffer buffer)))

(deftest serialization-context-preserves-allowlists-and-legacy-data
  (let [payload {:data [1 2 "three"]}
        value (AtomicInteger. 42)
        classes #{"java.util.concurrent.atomic.AtomicInteger"}]
    (is (= payload (b/deserialize (nippy/freeze payload))))
    (is (thrown? Exception (b/serialize AtomicInteger)))
    (binding [c/*data-serializable-classes* classes]
      (is (= 42 (.get ^AtomicInteger (b/deserialize (b/serialize value)))))
      (is (= 42 (.get ^AtomicInteger (storage-roundtrip value))))
      (is (= 42 (.get ^AtomicInteger (wire-roundtrip value nil)))))
    ;; The storage contract derives the freeze policy from the current thaw
    ;; policy, even when a caller has installed a different freeze allowlist.
    (binding [c/*data-serializable-classes* nil
              nippy/*freeze-serializable-allowlist* #{}
              nippy/*thaw-serializable-allowlist* classes]
      (is (= 42 (.get ^AtomicInteger (storage-roundtrip value))))
      (is (= 42 (.get ^AtomicInteger (wire-roundtrip value nil))))
      (is (= #{} nippy/*freeze-serializable-allowlist*)))))

(deftest wire-options-preserve-negotiation-and-partial-options
  (let [payload {:data (apply str (repeat 12000 "x"))}]
    (doseq [opts [nil (p/negotiate-wire-opts nil)
                  (p/negotiate-wire-opts {:compression [:zstd]})
                  {:compression :zstd :compression-threshold 0}
                  {:compression :zstd :compression-threshold 100000}]]
      (is (= payload (wire-roundtrip payload opts))))
    (let [buffer (ByteBuffer/allocate 65536)]
      (p/write-message-bf buffer payload c/message-format-nippy
                          {:compression :zstd :compression-threshold 0})
      (is (thrown? Exception (p/receive-one-message buffer nil))))))
