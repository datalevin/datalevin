(ns datalevin.client-copy-test
  (:require [clojure.test :refer [deftest is testing]]
            [datalevin.client.copy :as copy]
            [datalevin.client.request :as request]
            [datalevin.util :as u])
  (:import [java.io EOFException IOException]
           [java.nio ByteBuffer]
           [java.nio.channels SeekableByteChannel]
           [java.nio.file Files]
           [java.nio.file.attribute FileAttribute]
           [java.security MessageDigest]
           [java.util Base64]))

(def header {:type :copy-out-response :copy-format :binary-chunks :checksum-algo :sha-256})
(defn- done [^bytes data chunks]
  {:type :copy-done :checksum (u/hexify (.digest (MessageDigest/getInstance "SHA-256") data))
   :checksum-algo :sha-256 :bytes (alength data) :chunks chunks})

(defn- tracked-sink [file events {:keys [max-write reset-error write-error close-error stall?]}]
  (let [^SeekableByteChannel sink (copy/file-sink file)]
    (reify SeekableByteChannel
      (isOpen [_] (.isOpen sink))
      (read [_ buffer] (.read sink buffer))
      (size [_] (.size sink))
      (truncate [this n]
        (swap! events conj [:truncate n])
        (when reset-error (throw reset-error))
        (.truncate sink n)
        this)
      (position [_] (.position sink))
      (position [this n] (.position sink n) this)
      (write [_ buffer]
        (swap! events conj :write)
        (when write-error (throw write-error))
        (if stall? 0
            (let [^ByteBuffer buffer buffer limit (.limit buffer)]
              (try
                (when max-write (.limit buffer (+ (.position buffer) (min max-write (.remaining buffer)))))
                (.write sink buffer)
                (finally (.limit buffer limit))))))
      (close [_]
        (swap! events conj :close)
        (.close sink)
        (when close-error (throw close-error))))))

(defn- run-copy [header frames opts check]
  (let [file (Files/createTempFile "copy-receiver-" ".incomplete" (make-array FileAttribute 0))
        events (atom [])
        receiver (copy/receiver file (assoc opts :sink-factory #(tracked-sink % events opts)))
        messages (atom (seq frames))]
    (try
      ;; Existing partial bytes are removed before each pre-header attempt.
      (Files/write file (byte-array [9 9 9 9 9]) (make-array java.nio.file.OpenOption 0))
      (copy/reset-before-request! receiver)
      (let [result (try
                     (copy/receive! receiver
                                    #(let [frame (first @messages)] (swap! messages next) frame)
                                    header)
                     (catch Throwable e e))]
        (check result (Files/readAllBytes file) receiver @events))
      (finally (copy/close! receiver) (Files/delete file)))))

(deftest binary-receiver-validates-and-handles-partial-writes
  (doseq [data [(byte-array 0) (byte-array (range 100))]]
    (let [chunks (if (zero? (alength ^bytes data)) [] [(byte-array (take 17 data)) (byte-array (drop 17 data))])]
      (run-copy (assoc header :copy-meta {:db-name "orders"}) (concat (map vector chunks) [(done data (count chunks))])
                {:max-write 3}
                (fn [result actual receiver events]
                  (is (= :command-complete (:type result)))
                  (is (= (seq data) (seq actual)))
                  (is (true? (:verified? result)))
                  (is (= {:db-name "orders"} (:copy-meta result)))
                  (is (nil? (:result result)))
                  (is (= (alength ^bytes data) (:bytes result)))
                  (is (copy/completed? receiver))
                  (is (= :close (last events)))
                  (is (thrown? clojure.lang.ExceptionInfo (copy/reset-before-request! receiver))))))))

(deftest binary-receiver-rejects-incomplete-or-corrupt-transfers
  (let [data (byte-array [1 2 3]) terminal (done data 1)]
    (doseq [[h frames code]
            [[header [[data] (assoc terminal :bytes 4)] :copy/size-mismatch]
             [header [[data] (assoc terminal :checksum (apply str (repeat 64 "0")))] :copy/checksum-mismatch]
             [header [[data] (dissoc terminal :checksum)] :copy/missing-integrity]
             [header [[data] (assoc terminal :chunks 2)] :copy/chunk-count-mismatch]
             [header [[data] (assoc terminal :copy-format :other)] :copy/format-mismatch]
             [(assoc header :checksum-algo :md5) [] :copy/unsupported-checksum]
             [header [["data"] terminal] :copy/invalid-chunk]
             [header [{:type :error-response :message "failed"}] :copy/server-error]]]
      (testing (str code)
        (run-copy h frames {}
                  (fn [error _ receiver events]
                    (is (= code (:error (ex-data error))))
                    (is (not (copy/completed? receiver)))
                    (is (= :close (last events)))))))))

(deftest legacy-base64-is-decoded-across-arbitrary-frame-boundaries
  (let [data (.getBytes "a small legacy payload" "UTF-8")
        text (.encodeToString (Base64/getEncoder) data)]
    (doseq [encoded [text (.replaceAll text "=+$" "")]]
      (run-copy {:type :copy-out-response} (concat (map #(vector (str %)) encoded) [{:type :copy-done}]) {}
                (fn [result actual _ _]
                  (is (= (seq data) (seq actual)))
                  (is (false? (:verified? result)))))))
  (run-copy {:type :copy-out-response} [] {:require-binary? true}
            (fn [error _ _ _] (is (= :copy/unsupported-format (:error (ex-data error))))))
  (doseq [chunks [["Zg==AA"] ["Zg==" "AA"] ["!"] ["A"]]]
    (run-copy {:type :copy-out-response} [(vec chunks) {:type :copy-done}] {}
              (fn [error _ _ _] (is (= :copy/invalid-base64 (:error (ex-data error))))))))

(deftest sink-failures-preserve-the-primary-error-and-close-the-sink
  (let [failure (ex-info "disk full" {}) cleanup (ex-info "close failed" {})]
    (run-copy header [[(byte-array [1])]] {:write-error failure :close-error cleanup}
              (fn [error _ receiver events]
                (is (identical? failure error))
                (is (= [cleanup] (seq (.getSuppressed ^Throwable error))))
                (is (not (copy/completed? receiver)))
                (is (= [:write :close] (vec (remove vector? events)))))))
  (run-copy header [[(byte-array [1])]] {:stall? true}
            (fn [error _ _ _] (is (= :copy/sink-stalled (:error (ex-data error)))))))

(deftest expired-budget-prevents-receiving-and-closes-the-sink
  ;; Expire after preparation so the receiver owns cleanup on entry.
  (let [budget (request/budget -1 0 nil {:type :copy})
        file (Files/createTempFile "copy-deadline-" ".incomplete" (make-array FileAttribute 0))
        receiver (copy/receiver file {}) read? (atom false)]
    (try
      (copy/reset-before-request! receiver)
      (let [error (binding [request/*request* budget]
                    (try (copy/receive! receiver #(reset! read? true) header)
                         (catch Exception e e)))]
        (is (= :socket/timeout (:error (ex-data error))))
        (is (false? @read?))
        (is (not (.isOpen ^SeekableByteChannel (:sink receiver)))))
      (finally (copy/close! receiver) (Files/delete file)))))

(deftest sink-io-errors-retain-local-origin-and-original-cause
  (doseq [phase [:open :reset]]
    (testing (name phase)
      (let [file (Files/createTempFile "copy-local-io-" ".incomplete" (make-array FileAttribute 0))
            failure (IOException. "Local sink failed")
            receiver (volatile! nil)]
        (try
          (let [error (try
                        (vreset! receiver
                          (copy/receiver file
                            {:sink-factory (if (= :open phase)
                                             (fn [_] (throw failure))
                                             #(tracked-sink % (atom []) {:reset-error failure}))}))
                        (copy/reset-before-request! @receiver)
                        nil
                        (catch Exception e e))]
            (is (= :copy/local-io (:error (ex-data error))))
            (is (identical? failure (ex-cause error))))
          (finally (when @receiver (copy/close! @receiver)) (Files/delete file))))))
  (doseq [phase [:write :close]]
    (testing (name phase)
      (let [failure (IOException. "Local sink failed")
            data (byte-array [1])]
        (run-copy header [[data] (done data 1)]
                  {(if (= phase :write) :write-error :close-error) failure}
                  (fn [error _ receiver events]
                    (is (= :copy/local-io (:error (ex-data error))))
                    (is (identical? failure (ex-cause error)))
                    (is (not (copy/completed? receiver)))
                    (is (= :close (last events)))))))))

(deftest local-cleanup-errors-do-not-reclassify-a-network-failure
  (let [file (Files/createTempFile "copy-network-io-" ".incomplete" (make-array FileAttribute 0))
        network (EOFException. "Connection closed")
        cleanup (IOException. "Sink close failed")
        receiver (copy/receiver file {:sink-factory #(tracked-sink % (atom []) {:close-error cleanup})})]
    (try
      (copy/reset-before-request! receiver)
      (let [error (try (copy/receive! receiver #(throw network) header)
                       nil (catch Exception e e))
            suppressed (first (.getSuppressed ^Throwable error))]
        (is (identical? network error))
        (is (= :copy/local-io (:error (ex-data suppressed))))
        (is (identical? cleanup (ex-cause suppressed))))
      (finally (copy/close! receiver) (Files/delete file)))))
