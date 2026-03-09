(ns datalevin.buffer-test
  (:require
   [clojure.test :refer [deftest is]]
   [datalevin.buffer :as bf])
  (:import
   [java.nio ByteBuffer]
   [java.util.concurrent ConcurrentLinkedDeque]))

(defn- drain-buffers!
  [^ConcurrentLinkedDeque buffers]
  (loop []
    (when (.pollFirst buffers)
      (recur))))

(deftest concurrent-array-buffer-checkout-yields-distinct-buffers-test
  (let [^ConcurrentLinkedDeque buffers @#'datalevin.buffer/array-buffers
        rounds  64]
    (dotimes [_ rounds]
      (drain-buffers! buffers)
      (.offer buffers (ByteBuffer/wrap (byte-array 128)))
      (let [start   (promise)
            release (promise)
            holder  (fn [result]
                      (future
                        @start
                        (let [buf (bf/get-array-buffer 64)]
                          (deliver result buf)
                          @release
                          (bf/return-array-buffer buf)
                          buf)))
            r1      (promise)
            r2      (promise)
            f1      (holder r1)
            f2      (holder r2)]
        (deliver start true)
        (let [b1 @r1
              b2 @r2]
          (is (not (identical? b1 b2))))
        (deliver release true)
        @f1
        @f2))))

(deftest default-direct-buffer-is-direct-test
  (let [^ByteBuffer buf (bf/get-direct-buffer)]
    (try
      (is (.isDirect buf))
      (finally
        (bf/return-direct-buffer buf)))))
