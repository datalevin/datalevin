(ns datalevin.native-value-test
  (:require [clojure.test :refer [deftest is testing]]
            [datalevin.bits :as b]
            [datalevin.constants :as c]
            [datalevin.native-value :as nv]
            [datalevin.protocol :as p]
            [taoensso.nippy :as nippy])
  (:import [datalevin NativeValue]
           [java.nio ByteBuffer]
           [java.util.function BiPredicate]))

(defn- logical [^NativeValue value]
  (first (b/deserialize (.payload value))))

(defn- snapshot [codec type-name payload]
  (NativeValue. codec type-name payload
                (reify BiPredicate
                  (test [_ left right]
                    (= (logical left) (logical right))))))

(defn- value [logical-value nonce]
  (snapshot "sender" ":app/task" (b/serialize [logical-value nonce])))

(defn- receiver [type-name payload]
  (when-not (= type-name ":app/task")
    (throw (ex-info "Unknown native type" {:type-name type-name})))
  (snapshot "receiver" type-name payload))

(defn- message-buffer [message wire-opts]
  (doto (ByteBuffer/allocate 65536)
    (p/write-message-bf message c/message-format-nippy wire-opts)))

(deftest native-wire-requires-receiver-and-context
  (let [v (value :a 1)
        bf (doto (ByteBuffer/allocate 4096)
             (p/write-nippy-bf v)
             (.flip))
        wire-bytes (b/get-bytes (.duplicate bf))
        bindings (nv/spill-bindings)
        spill-bytes (binding [nv/*spill-bindings* bindings]
                      (nippy/fast-freeze v))]
    (is (= (seq wire-bytes)
           (seq (binding [nv/*wire-native-value* true]
                  (b/serialize (snapshot "another-runtime" ":app/task"
                                         (.payload ^NativeValue v)))))))
    (is (thrown? Exception (b/serialize v)))
    (is (thrown? Exception (p/read-nippy-bf (.duplicate bf))))
    (is (thrown? Exception (b/deserialize wire-bytes)))
    (is (thrown? Exception
                 (binding [nv/*spill-bindings* bindings]
                   (b/deserialize wire-bytes))))
    (is (thrown? Exception
                 (binding [nv/*wire-native-value* true
                           nv/*wire-reader* receiver
                           nv/*spill-bindings* bindings]
                   (nippy/fast-thaw spill-bytes))))
    (let [^NativeValue restored
          (binding [nv/*wire-reader* receiver]
            (p/read-nippy-bf (.duplicate bf)))]
      (is (= "receiver" (.codecId restored)))
      (is (= (seq (.payload v)) (seq (.payload restored)))))))

(deftest native-wire-preserves-receiver-equality
  (doseq [compressed? [false true]]
    (testing (str "compression " compressed?)
      (let [a (value :a 1)
            a2 (value :a 2)
            b (value :b 1)
            message {:rows [a a2 b]
                     :map {a :a b :b}
                     :set #{a b}
                     :padding (apply str (repeat 8192 "x"))}
            opts (when compressed? {:compression :zstd :compression-threshold 0})
            bf (message-buffer message opts)
            fmt (.get bf 0)
            [restored _] (binding [nv/*wire-reader* receiver]
                           (p/receive-one-message bf opts))]
        (is (= compressed?
               (pos? (bit-and fmt c/message-flag-zstd))))
        (is (= 2 (count (set (:rows restored)))))
        (is (= :a (get (:map restored) a2)))
        (is (contains? (:set restored) a2))
        (is (every? #(= "receiver" (.codecId ^NativeValue %))
                    (:rows restored)))
        ;; The receiver's live equality survives a subsequent disk spill.
        (let [bindings (nv/spill-bindings)
              bytes (binding [nv/*spill-bindings* bindings]
                      (nippy/fast-freeze restored))
              spilled (binding [nv/*spill-bindings* bindings]
                        (nippy/fast-thaw bytes))]
          (is (= 2 (count (set (:rows spilled)))))
          (is (= :a (get (:map spilled) a2))))))))

(deftest native-wire-reader-failures-do-not-affect-later-messages
  (let [bf (message-buffer [(value :a 1)] nil)]
    (is (thrown? Exception
                 (binding [nv/*wire-reader* (fn [_ _]
                                             (throw (ex-info "Missing binding" {})))]
                   (p/receive-one-message (.duplicate bf)))))
    (let [[restored _] (binding [nv/*wire-reader* receiver]
                         (p/receive-one-message bf))]
      (is (= [:a] (mapv logical restored))))))
