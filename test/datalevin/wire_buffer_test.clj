(ns datalevin.wire-buffer-test
  (:require
   [clojure.test :refer [deftest is testing]]
   [datalevin.bits :as b]
   [datalevin.constants :as c]
   [datalevin.protocol :as p]
   [taoensso.nippy :as nippy])
  (:import
   [java.io EOFException]
   [java.nio ByteBuffer BufferOverflowException]))

(defn- buffer ^ByteBuffer [direct? size]
  (if direct? (ByteBuffer/allocateDirect size) (ByteBuffer/allocate size)))

(defn- raw-frame! [^ByteBuffer bf ^bytes payload]
  (.put bf (unchecked-byte c/message-format-nippy))
  (.putInt bf (+ c/message-header-size (alength payload)))
  (.put bf payload))

(deftest nippy-buffer-wire-compatibility
  (let [cached (apply str (repeat 100 "cached"))
        values [{:type :pull :args ["db" [:name :age] [:id 42]]}
                {:type :command-complete :result {:name "東京 👋" :age 42}}
                (with-meta [{:same :same} {:same :same} #{:same :other}]
                  {:same :same :metadata true})
                [(nippy/cache cached) (nippy/cache cached)]]]
    (doseq [direct? [false true]
            value values]
      (let [bf (buffer direct? 8192)
            bytes (nippy/fast-freeze value)
            expected (nippy/fast-thaw bytes)]
        ;; Appended frames must have independent caches even inside a caller's
        ;; own cache scope, and lengths must exclude the preceding bytes.
        (.putInt bf 1234)
        (nippy/with-cache
          (dotimes [_ 2] (p/write-message-bf bf value)))
        (.flip bf)
        (is (= 1234 (.getInt bf)))
        (dotimes [_ 2]
          (is (= c/message-format-nippy (.get bf)))
          (is (= (+ c/message-header-size (alength bytes)) (.getInt bf)))
          (let [payload (byte-array (alength bytes))]
            (.get bf payload)
            (is (= (seq bytes) (seq payload)))
            (is (= expected (b/deserialize payload)))
            (is (= (meta expected) (meta (p/read-value c/message-format-nippy payload))))))
        (is (not (.hasRemaining bf)))))))

(deftest nippy-buffer-reads-legacy-frames
  (doseq [direct? [false true]
          value [{:data [1 2 "three"]}
                 {:data (apply str (repeat 12000 "legacy"))}]]
    (let [bf (buffer direct? 100000)]
      (raw-frame! bf (nippy/freeze value))
      (p/write-message-bf bf {:next true})
      (is (= value (first (p/receive-one-message bf))))
      (is (= {:next true} (first (p/receive-one-message bf))))
      (is (zero? (.position bf))))))

(deftest nippy-buffer-fragmentation-and-growth
  (let [first-message {:data (apply str (repeat 200 "界"))}
        next-message {:next [:ready :ready]}
        src (doto (ByteBuffer/allocate 4096)
              (p/write-message-bf first-message)
              (p/write-message-bf next-message)
              (.flip))
        ^bytes bytes (b/get-bytes src)]
    (doseq [direct? [false true]
            split [0 1 4 5 6 15]]
      (let [dst (doto (buffer direct? 16) (.put bytes 0 split))
            [msg ^ByteBuffer dst] (p/receive-one-message dst)]
        (is (nil? msg))
        (is (= split (.position dst)))
        ;; Fill the initial buffer to force receive-side growth.
        (.put dst bytes split (- 16 (long split)))
        (let [[msg ^ByteBuffer grown] (p/receive-one-message dst)]
          (is (nil? msg))
          (is (> (.capacity grown) 16))
          (.put grown bytes 16 (- (alength bytes) 16))
          (is (= first-message (first (p/receive-one-message grown))))
          (is (= next-message (first (p/receive-one-message grown))))
          (is (zero? (.position grown))))))))

(deftest nippy-buffer-frame-boundaries-and-handler-ownership
  (doseq [direct? [false true]]
    (let [bf (buffer direct? 4096)
          first-message {:first "boundary"}
          next-message {:next true}]
      (p/write-message-bf bf first-message)
      (p/write-message-bf bf next-message)
      (is (true?
            (p/extract-message
              bf p/read-value
              (fn [_ message]
                (is (= first-message message))
                (is (= (.capacity bf) (.limit bf)))
                ;; The handler may immediately consume buffered copy-in frames.
                (is (= next-message (first (p/receive-one-message bf))))))))
      (is (zero? (.position bf)))
      (doseq [extract? [false true]]
        (let [payload (nippy/fast-freeze first-message)]
          (raw-frame! bf (byte-array (butlast payload)))
          (p/write-message-bf bf next-message)
          ;; A truncated value cannot borrow bytes from the next frame.
          (is (thrown? Exception
                       (if extract?
                         (p/extract-message bf p/read-value (fn [& _]))
                         (p/receive-one-message bf))))
          (is (= next-message (first (p/receive-one-message bf))))))
      (doseq [length [-1 0 4]]
        (.put bf (unchecked-byte c/message-format-nippy))
        (.putInt bf length)
        (is (thrown? Exception (p/receive-one-message bf)))
        (.clear bf)))))

(deftype BrokenWireValue [])

(nippy/extend-freeze BrokenWireValue ::broken-wire-value
  [_value _out] (throw (EOFException. "custom serializer failure")))

(deftest nippy-buffer-overflow-retry-and-compression
  (doseq [direct? [false true]]
    (let [value {:data (apply str (repeat 1000 "overflow")) :same :same}
          bytes (nippy/fast-freeze value)]
      (doseq [capacity [4 8 100 (alength bytes)]]
        (let [bf (doto (buffer direct? capacity) (.putInt 1234))]
          (is (thrown? BufferOverflowException (p/write-message-bf bf value)))
          (is (= 4 (.position bf)))
          (is (= 1234 (.getInt bf 0)))))
      (let [bf (buffer direct? 65536)]
        (p/write-message-bf bf value)
        (is (= value (first (p/receive-one-message bf))))
        (is (thrown-with-msg? EOFException #"custom serializer failure"
                             (p/write-message-bf bf (BrokenWireValue.))))
        (is (zero? (.position bf)))
        (is (thrown? Exception (p/write-message-bf bf String))))
      (testing "compressed frame fits even when the raw value does not"
        (let [bf (buffer direct? 256)
              opts {:compression :zstd :compression-threshold 0}]
          (p/write-message-bf bf value c/message-format-nippy opts)
          (is (pos? (bit-and (.get bf 0) c/message-flag-zstd)))
          (is (= value (first (p/receive-one-message bf opts)))))))))
