(ns datalevin.protocol-test
  (:require
   [datalevin.protocol :as sut]
   [datalevin.constants :as c]
   [datalevin.spill :as sp]
   [datalevin.bits :as b]
   [datalevin.buffer :as bf]
   [clojure.test :refer [deftest testing is]]
   [clojure.test.check.generators :as gen]
   [clojure.test.check.clojure-test :as test]
   [clojure.test.check.properties :as prop])
  (:import
   [java.nio ByteBuffer]))

(test/defspec transit-string-test
  100
  (prop/for-all [k gen/any-equatable]
                (= k (sut/read-transit-string (sut/write-transit-string k)))))

(test/defspec spillable-transit-string-test
  100
  (prop/for-all
    [k (gen/vector gen/any-equatable)]
    (let [vs (sp/new-spillable-vector k)]
      (= vs (sut/read-transit-string (sut/write-transit-string vs))))))

(test/defspec transit-bf-test
  100
  (prop/for-all [k gen/any-equatable]
                (let [bf (bf/allocate-buffer c/+buffer-size+)]
                  (sut/write-transit-bf bf k)
                  (.flip bf)
                  (= k (sut/read-transit-bf bf)))))

(test/defspec transit-bytes-test
  100
  (prop/for-all [k gen/any-equatable]
                (= k (sut/read-transit-bytes (sut/write-transit-bytes k)))))

(test/defspec transit-message-bf-test
  100
  (prop/for-all
    [v gen/any-equatable]
    (let [^ByteBuffer bf (bf/allocate-buffer 16384)]
      (sut/write-message-bf bf v c/message-format-transit)
      (let [pos (.position bf)]
        (.flip bf)
        (is (= 1 (short (.get bf))))
        (is (= pos (.getInt bf)))
        (is (= v (sut/read-transit-bf bf)))))))

(deftest wire-capability-negotiation-test
  (is (= :zstd (:compression
                (sut/negotiate-wire-opts {:compression [:zstd]}))))
  (is (nil? (:compression
             (sut/negotiate-wire-opts {:compression []}))))
  (is (nil? (:compression (sut/negotiate-wire-opts nil)))))

(deftest wire-compression-threshold-test
  (let [msg           {:payload (apply str (repeat 3000 "aaaaaaaaaaaa"))}
        no-comp-opts  {:compression :zstd :compression-threshold 1000000}
        comp-opts     {:compression :zstd :compression-threshold 64}
        ^ByteBuffer b1 (bf/allocate-buffer (* 2 c/+buffer-size+))
        ^ByteBuffer b2 (bf/allocate-buffer (* 2 c/+buffer-size+))]
    (sut/write-message-bf b1 msg c/message-format-nippy no-comp-opts)
    (.flip b1)
    (let [fmt      (.get b1)
          length   (.getInt b1)
          payload  (byte-array (- length c/message-header-size))]
      (.get b1 payload)
      (is (zero? (bit-and (bit-and (int fmt) 0xFF) c/message-flag-zstd)))
      (is (= msg (sut/read-value fmt payload nil))))

    (sut/write-message-bf b2 msg c/message-format-nippy comp-opts)
    (.flip b2)
    (let [fmt      (.get b2)
          length   (.getInt b2)
          payload  (byte-array (- length c/message-header-size))]
      (.get b2 payload)
      (is (pos? (bit-and (bit-and (int fmt) 0xFF) c/message-flag-zstd)))
      (is (thrown? Exception (sut/read-value fmt payload nil)))
      (is (= msg (sut/read-value fmt payload comp-opts))))))

(deftest small-receive-one-messages-test
  (let [src-arr         (byte-array 200)
        ^ByteBuffer src (ByteBuffer/wrap src-arr)
        ^ByteBuffer dst (bf/allocate-buffer 200)
        msg1            {:text "this is the first message" :value 888} ; 62 bytes
        msg2            {:text "the second message"}                   ; 41 bytes
        ]

    (sut/write-message-bf src msg1 c/message-format-transit)
    (sut/write-message-bf src msg2  c/message-format-transit)
    (.flip src)

    (testing "less than header length available"
      (bf/buffer-transfer src dst 2)
      (is (= [nil dst] (sut/receive-one-message dst)))
      (is (= (.position dst) 2)))

    (testing "less than message length available"
      (bf/buffer-transfer src dst 10)
      (is (= [nil dst] (sut/receive-one-message dst)))
      (is (= (.position dst) 12)))

    (testing "first message available"
      (bf/buffer-transfer src dst 60)
      (is (= [msg1 dst] (sut/receive-one-message dst)))
      (is (= (.position dst) 10))
      (is (= (.limit dst) 200)))))

(deftest large-receive-one-messages-test
  (let [src-arr         (byte-array 200)
        ^ByteBuffer src (ByteBuffer/wrap src-arr)
        ^ByteBuffer dst (bf/allocate-buffer 30)
        msg1            {:text "this is the first message" :value 888} ; 62 bytes
        msg2            {:text "the second message"}                   ; 41 bytes
        ]

    (sut/write-message-bf src msg1 c/message-format-transit)
    (sut/write-message-bf src msg2 c/message-format-transit)
    (.flip src)

    (testing "message larger than buffer, auto grow buffer"
      (bf/buffer-transfer src dst 30)
      (let [[res ^ByteBuffer dst'] (sut/receive-one-message dst)]
        (is (nil? res))
        (is (= 620 (.capacity dst') (.limit dst')))
        (is (= 30  (.position dst')))

        (testing "first message and more available"
          (bf/buffer-transfer src dst' 70)
          (is (= [msg1 dst'] (sut/receive-one-message dst')))
          (is (= (.position dst') 38)))

        (testing "second message available"
          (bf/buffer-transfer src dst' 3)
          (is (= [msg2 dst'] (sut/receive-one-message dst')))
          (is (= (.position dst') 0))
          (is (= (.limit dst') 620)))))))

(deftest extract-message-test
  (let [src-arr         (byte-array 200)
        ^ByteBuffer src (ByteBuffer/wrap src-arr)
        ^ByteBuffer dst (bf/allocate-buffer 200)
        msg1            {:text "this is the first message" :value 888} ; 62 bytes
        msg2            {:text "the second message"}                   ; 41 bytes
        sink            (atom [])
        handler         (fn [type msg]
                          (swap! sink conj (sut/read-value type msg)))]

    (sut/write-message-bf src msg1 c/message-format-transit)
    (sut/write-message-bf src msg2 c/message-format-transit)
    (.flip src)

    (testing "less than header length available"
      (bf/buffer-transfer src dst 2)
      (sut/extract-message dst handler)
      (is (= (.position dst) 2))
      (is (empty? @sink)))

    (testing "less than message length available"
      (bf/buffer-transfer src dst 10)
      (sut/extract-message dst handler)
      (is (= (.position dst) 12))
      (is (empty? @sink)))

    (testing "first message available"
      (bf/buffer-transfer src dst 60)
      (sut/extract-message dst handler)
      (is (= (.position dst) 10))
      (is (= (.limit dst) 200))
      (is (= (count @sink) 1))
      (is (= msg1 (first @sink))))

    (testing "second message still not available"
      (bf/buffer-transfer src dst 10)
      (sut/extract-message dst handler)
      (is (= (.position dst) 20))
      (is (= (.limit dst) 200))
      (is (= (count @sink) 1))
      (is (= msg1 (first @sink))))

    (testing "second message available"
      (bf/buffer-transfer src dst 21)
      (sut/extract-message dst handler)
      (is (= (.position dst) 0))
      (is (= (.limit dst) 200))
      (is (= (count @sink) 2))
      (is (= msg2 (second @sink))))))
