(ns datalevin.serde-sites-test
  (:require
   [clojure.test :refer [deftest is]]
   [datalevin.bits :as b]
   [datalevin.buffer :as bf]
   [datalevin.client-op :as cop]
   [datalevin.constants :as c]
   [datalevin.datom :as d]
   [datalevin.ha.control :as control]
   [datalevin.index :as index]
   [datalevin.native-value :as nv]
   [datalevin.util :as u]
   [taoensso.nippy :as nippy])
  (:import
   [com.alipay.sofa.jraft Iterator StateMachine]
   [datalevin NativeValue]
   [java.io DataOutput]
   [java.nio ByteBuffer]
   [java.security MessageDigest]
   [java.util Random]
   [java.util.concurrent ConcurrentLinkedDeque]
   [java.util.concurrent.atomic AtomicInteger]
   [java.util.function BiPredicate]))

(deftest pooled-serialization-growth-nesting-and-failures
  ;; A fresh pool forces growth, independently of buffers warmed by other tests.
  (with-redefs [bf/array-buffers (ConcurrentLinkedDeque.)]
    (let [value {:schema (vec (repeat 1000 {:db/valueType :db.type/string}))}
          expected (b/serialize value)]
      (b/with-serialized-bf [outer value]
        (is (= (seq expected) (seq (b/get-bytes (.duplicate outer)))))
        (b/with-serialized-bf [inner {:nested true}]
          (is (not (identical? outer inner)))
          (is (= {:nested true} (b/deserialize-bf inner))))
        (is (= value (b/deserialize-bf outer))))
      (is (= (alength expected) (b/measure-size value)))
      (is (thrown-with-msg? Exception #"consumer failed"
                           (b/with-serialized-bf [buffer value]
                             (is (pos? (.remaining buffer)))
                             (throw (ex-info "consumer failed" {})))))
      (is (thrown? Exception (b/acquire-serialized-bf String)))
      (b/with-serialized-bf [buffer value]
        (is (= value (b/deserialize-bf buffer)))))
    (is (pos? (.size bf/array-buffers)))))

(defn- byte-array-hash [value]
  (let [md (MessageDigest/getInstance "SHA-256")
        payload (binding [nv/*wire-native-value* true] (b/serialize value))]
    (u/hexify (.digest md payload))))

(deftest request-hashes-remain-byte-compatible
  (let [cached (apply str (repeat 2000 "cached"))
        native (NativeValue. "sender" ":app/task" (b/serialize {:rank 2})
                              (reify BiPredicate (test [_ a b] (identical? a b))))
        values [[:transact-kv "db" "data" :long :data [[:put 1 {:enabled true}]]]
                (with-meta {:schema {:name {:db/valueType :db.type/string}}}
                  {:source :test})
                [(nippy/cache cached) (nippy/cache cached)]
                [native]]]
    (doseq [value values]
      (let [expected (byte-array-hash value)]
        (nippy/with-cache
          (is (= expected (cop/request-hash value)))
          (is (= expected (cop/request-hash value))))))
    (binding [c/*data-serializable-classes* #{"java.util.concurrent.atomic.AtomicInteger"}]
      (let [value {:option (AtomicInteger. 42)}]
        (is (= (byte-array-hash value) (cop/request-hash value)))))))

(deftest giant-codecs-preserve-stored-bytes
  (let [random-bytes (byte-array 8192)
        _ (.nextBytes (Random. 42) random-bytes)]
    (doseq [value [{:enabled true}
                  {:description (apply str (repeat 2000 "config"))}
                  random-bytes]
            threshold [0 Long/MAX_VALUE]]
      (let [datom (d/datom 1 :config/doc value)
            raw (b/serialize datom)
            {:keys [value vtype]}
            (binding [c/*giants-zstd-threshold* threshold]
              (index/encode-giant-datom datom))]
        (is (= :raw vtype))
        (is (bytes? value))
        (is (= (seq raw) (seq (b/serialize (index/decode-giant-datom value)))))
        (when (= threshold Long/MAX_VALUE)
          (is (= (seq raw) (seq value))))))
    (let [datom (d/datom 1 :config/doc {:legacy true})]
      (is (= datom (index/decode-giant-datom (nippy/freeze datom)))))))

(deftype CountedValue [calls])

(nippy/extend-freeze CountedValue ::counted-value
  [^CountedValue value ^DataOutput out]
  (swap! (.-calls value) inc)
  (.writeInt out 42))

(deftest uncompressed-giant-is-serialized-once
  (let [calls (atom 0)
        {:keys [value vtype]}
        (binding [c/*giants-zstd-threshold* Long/MAX_VALUE]
          (index/encode-giant-datom (d/datom 1 :value (CountedValue. calls))))]
    (b/put-buffer (ByteBuffer/allocate 4096) value vtype)
    (is (= 1 @calls))))

(deftest ha-decodes-borrowed-buffers-without-changing-the-iterator
  (doseq [direct? [false true]
          freeze [b/serialize nippy/freeze]]
    (let [command {:op :init-membership-hash :membership-hash "abc123"}
          bytes ^bytes (freeze command)
          buffer (doto (if direct? (ByteBuffer/allocateDirect 4096)
                            (ByteBuffer/allocate 4096))
                   (.putInt 1234) (.put bytes) (.flip) (.position 4))
          data (.asReadOnlyBuffer buffer)
          state (atom {:leases {} :membership-hash nil :voters []})
          ^StateMachine fsm (#'control/new-jraft-fsm state)
          more? (volatile! true)
          commits (atom 0)
          rollback (atom nil)
          iter (reify Iterator
                 (hasNext [_] @more?)
                 (getData [_] data)
                 (done [_] nil)
                 (commit [_] (swap! commits inc) true)
                 (next [_] (vreset! more? false) nil)
                 (setErrorAndRollback [_ n status] (reset! rollback [n status])))]
      (.onApply fsm iter)
      (is (= "abc123" (:membership-hash @state)))
      (is (= 1 @commits))
      (is (nil? @rollback))
      (is (= 4 (.position data)))
      (is (= (+ 4 (alength bytes)) (.limit data))))))
