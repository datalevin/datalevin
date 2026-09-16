(ns datalevin.protocol-context-test
  (:require [clojure.test :refer [deftest is]]
            [datalevin.bits :as b]
            [datalevin.constants :as c]
            [datalevin.native-value :as nv]
            [datalevin.protocol :as p]
            [datalevin.protocol.context :as context]
            [taoensso.nippy :as nippy]
            [taoensso.nippy.impl :as impl])
  (:import [datalevin NativeValue]
           [taoensso.nippy.impl CacheState]
           [java.io DataInput DataOutput]
           [java.nio ByteBuffer BufferOverflowException]
           [java.util.concurrent.atomic AtomicInteger]
           [java.util.function BiPredicate]))

(defn- encode ^ByteBuffer [value]
  (doto (ByteBuffer/allocate 100000)
    (p/write-nippy-bf value) (.flip)))

(defrecord NestedWire [value])
(nippy/extend-freeze NestedWire ::nested-wire
  [wrapper ^DataOutput out]
  (let [^bytes bytes (b/get-bytes (encode (:value wrapper)))]
    (.writeInt out (alength bytes))
    (.write out bytes)))
(nippy/extend-thaw ::nested-wire
  [^DataInput in]
  (let [bytes (byte-array (.readInt in))]
    (.readFully in bytes)
    (->NestedWire (p/read-nippy-bf (ByteBuffer/wrap bytes)))))

(deftest cache-reuse-preserves-independent-frames-and-nested-scopes
  (let [ctx (context/create)
        text (.repeat "reference" 10)
        cached (nippy/cache text)
        value {:data [cached (->NestedWire [cached cached]) cached]}
        expected {:data [text (->NestedWire [text text]) text]}]
    (doseq [dict [nil (nippy/shared-dict [:data text]) nil]]
      (binding [nippy/*shared-dict* dict]
        (dotimes [_ 3]
          (let [^ByteBuffer encoded (context/with-context ctx (encode value))
                ordinary (encode value)]
            (is (= encoded ordinary))
            (is (= expected (p/read-nippy-bf (.duplicate encoded))))
            (is (= expected (context/with-context ctx (p/read-nippy-bf encoded))))))))
    (is (nil? (.get impl/tl:cache)))
    (is (nil? context/*context*))))

(deftest cache-reuse-recovers-after-overflow-and-malformed-messages
  (let [ctx (context/create)
        value {:data [(nippy/cache "repeat") (nippy/cache "repeat")]
               :padding (.repeat "large" 1000)}]
    (context/with-context ctx
      (is (thrown? BufferOverflowException
                   (p/write-nippy-bf (ByteBuffer/allocate 30) value)))
      (is (thrown? Exception (p/read-nippy-bf (ByteBuffer/wrap (byte-array [127])))))
      (is (= {:data ["repeat" "repeat"] :padding (:padding value)}
             (p/read-nippy-bf (encode value))))
      (let [opts {:compression :zstd :compression-threshold 0}
            buffer (ByteBuffer/allocate 256)]
        ;; Raw encoding overflows; the compressed fallback fits.
        (p/write-message-bf buffer value c/message-format-nippy opts)
        (is (= ["repeat" "repeat"] (:data (p/receive-one-message! (volatile! buffer) opts)))))
      (is (= {:legacy :header}
             (p/read-nippy-bf (ByteBuffer/wrap (nippy/freeze {:legacy :header}))))))
    (let [^CacheState cache (context/acquire-cache! ctx)]
      (try
        (is (every? empty? [(.-freeze-idxs cache) (.-kw-idxs cache)
                           (.-thaw-vals cache) (.-seen-kws cache) (.-seen-log cache)]))
        (finally (context/release-cache! ctx cache))))))

(deftest pooled-context-refreshes-allowlist-and-native-reader
  (let [ctx (context/create)
        classes #{"java.util.concurrent.atomic.AtomicInteger"}
        value (AtomicInteger. 42)
        ^ByteBuffer bytes (binding [c/*data-serializable-classes* classes]
                (context/with-context ctx (encode value)))
        native (NativeValue. "sender" ":app/value" (b/serialize 7)
                            (reify BiPredicate (test [_ a b] (identical? a b))))
        ^ByteBuffer native-bytes (encode native)]
    (binding [c/*data-serializable-classes* nil
              nippy/*thaw-serializable-allowlist* #{}]
      (context/with-context ctx
        (is (not (instance? AtomicInteger (p/read-nippy-bf (encode value)))))
        ;; Nippy represents a denied Java value as an unthawable map.
        (is (not (instance? AtomicInteger (p/read-nippy-bf (.duplicate bytes)))))))
    (binding [c/*data-serializable-classes* nil
              nippy/*freeze-serializable-allowlist* #{}
              nippy/*thaw-serializable-allowlist* classes]
      (context/with-context ctx
        (is (= 42 (.get ^AtomicInteger (p/read-nippy-bf (encode value)))))))
    ;; The same context can move to another pool borrower thread.
    (doseq [id [:first :second]]
      (is (= [id ":app/value" 7]
             @(future
                (binding [nv/*wire-reader* (fn [type payload] [id type (b/deserialize payload)])]
                  (context/with-context ctx
                    (p/read-nippy-bf (.duplicate native-bytes))))))))
    (context/with-context ctx
      (let [error (try (p/read-nippy-bf (.duplicate native-bytes))
                       (catch Exception e e))]
        (is (nv/decoding-error? error))))))

(deftest oversized-cache-is-released-and-the-next-frame-is-independent
  (let [ctx (context/create)
        values (mapv #(nippy/cache (str "value-" %)) (range 5000))]
    (context/with-context ctx
      (let [encoded (encode values)]
        (is (= (mapv #(str "value-" %) (range 5000)) (p/read-nippy-bf encoded))))
      (is (= {:data :after} (p/read-nippy-bf (encode {:data :after})))))))

(deftest server-context-keeps-wire-policy-local-and-refreshes-allowlists
  (let [ctx (context/create)
        value (AtomicInteger. 42)
        classes #{"java.util.concurrent.atomic.AtomicInteger"}]
    (binding [context/*context* ctx
              nv/*wire-native-value* false
              nippy/*freeze-serializable-allowlist* #{}]
      (doseq [allowlist [classes #{} classes]]
        (binding [nippy/*thaw-serializable-allowlist* allowlist]
          (let [restored (p/read-nippy-bf (encode value))]
            (if (seq allowlist)
              (is (= 42 (.get ^AtomicInteger restored)))
              (is (not (instance? AtomicInteger restored)))))
          (is (false? nv/*wire-native-value*))
          (is (= #{} nippy/*freeze-serializable-allowlist*))
          (is (identical? allowlist nippy/*thaw-serializable-allowlist*)))))))

(deftest reused-request-decoder-defers-native-values-until-authorized
  (let [ctx (context/create)
        decoder (p/request-decoder)
        native (NativeValue. "sender" ":app/value" (b/serialize 7)
                            (reify BiPredicate (test [_ a b] (identical? a b))))
        calls (atom 0)
        reader (fn [type payload] (swap! calls inc) [type (b/deserialize payload)])]
    (binding [context/*context* ctx nv/*wire-reader* reader]
      (doseq [value [native :ordinary native :after-native]]
        (let [before @calls
              request {:type :get-value :args ["db" "data" value]}
              parsed (p/read-request c/message-format-nippy (encode request) nil decoder)]
          (is (= before @calls) "routing decode must never invoke the native reader")
          (is (= (identical? native value) (p/native-request? parsed)))
          (is (false? nv/*wire-native-value*))
          (is (identical? reader nv/*wire-reader*))
          (is (= (if (identical? native value) [":app/value" 7] value)
                 (nth (:args (p/resolve-native-request parsed)) 2)))))
      (is (= 2 @calls)))))
