(ns datalevin.read-encode-test
  (:require
   [clojure.test :refer [deftest is use-fixtures]]
   [datalevin.bits :as b]
   [datalevin.client :as client]
   [datalevin.constants :as c]
   [datalevin.core :as d]
   [datalevin.kv :as kv]
   [datalevin.native-value :as nv]
   [datalevin.protocol :as p]
   [datalevin.read-encode :as enc]
   [datalevin.remote :as remote]
   [datalevin.server :as server]
   [datalevin.test.core :refer [allocate-port db-fixture]]
   [datalevin.util :as u]
   [taoensso.nippy :as nippy])
  (:import
   [datalevin NativeValue]
   [datalevin.remote KVStore]
   [java.io DataInput DataOutput]
   [java.net InetSocketAddress]
   [java.nio ByteBuffer BufferOverflowException]
   [java.nio.channels SocketChannel]
   [java.util UUID]
   [java.util.concurrent.atomic AtomicLong]
   [java.util.function BiPredicate]))

(use-fixtures :each db-fixture)

(defn- encode-message ^ByteBuffer [value options capacity]
  (let [buffer (ByteBuffer/allocateDirect capacity)]
    (p/write-message-bf buffer {:type :command-complete :result value}
                        c/message-format-nippy options)
    buffer))

(defn- decode-message [buffer options]
  (:result (first (p/receive-one-message buffer options))))

(deftest storage-codecs-produce-logical-nippy-values
  (doseq [direct? [false true]
          [type value] [[:data {:schema {:name {:db/valueType :db.type/string}}}]
                        [:data nil] [:data false]
                        [:string ""] [:string "東京 👋"]
                        [:string (.repeat "x" 300)]
                        [:string (.repeat "x" 40000)]
                        [:long Long/MIN_VALUE] [:long Long/MAX_VALUE]
                        [:float (float 1.25)] [:double -2.5]
                        [:instant (java.util.Date. -1234)] [:uuid (UUID/randomUUID)]
                        [:keyword :app/option] [:symbol 'app/value]
                        [:boolean false] [:bytes (byte-array [0 1 -1])]]
          :let [stored (doto (if direct? (ByteBuffer/allocateDirect 100000)
                                    (ByteBuffer/allocate 100000))
                         (b/put-buffer value type) (.flip))
                result (enc/read-result #(enc/write-buffer! % (.duplicate stored) type))
                actual (decode-message (encode-message result nil 200000) nil)]]
    (is (= (seq (b/serialize value)) (seq (b/serialize actual))) (str type))))

(deftest stored-nippy-caches-legacy-headers-and-ownership
  (let [text (.repeat "cached" 100)
        value (with-meta {:a [(nippy/cache text) (nippy/cache text)]}
                {:source :schema})]
    (doseq [freeze [b/serialize nippy/freeze]
            options [nil {:compression :zstd :compression-threshold 0}]]
      (let [bytes ^bytes (freeze value)
            stored (doto (ByteBuffer/allocateDirect (alength bytes))
                     (.put bytes) (.flip))
            result (enc/read-result #(enc/write-buffer! % (.duplicate stored) :data))
            message (encode-message [result result] options 20000)]
        (.clear stored)
        (while (.hasRemaining stored) (.put stored (byte 0)))
        (let [[a b] (decode-message message options)]
          (is (= {:a [text text]} a b))
          (is (= {:source :schema} (meta a) (meta b))))))))

(deftest encoded-native-data-keeps-receiver-and-authorization-context
  (let [native (NativeValue. "sender" ":app/value" (b/serialize {:rank 4})
                            (reify BiPredicate (test [_ a b] (identical? a b))))
        bytes (binding [nv/*wire-native-value* true] (b/serialize native))
        result (enc/read-result #(enc/write-buffer! % (ByteBuffer/wrap bytes) :data))
        frame (encode-message result nil 4096)]
    (is (thrown? Exception (decode-message (.duplicate frame) nil)))
    (binding [nv/*wire-reader* (fn [type payload] [type (b/deserialize payload)])]
      (is (= [":app/value" {:rank 4}] (decode-message (.duplicate frame) nil))))
    (let [request (ByteBuffer/allocate 4096)]
      (p/write-message-bf request {:type :pull :args ["db" result]})
      (.flip request)
      (.position request c/message-header-size)
      (is (p/native-request? (p/read-request c/message-format-nippy request nil))))))

(defrecord Observed [value])
(def thaw-threads (atom []))

(nippy/extend-freeze Observed ::observed
  [value ^DataOutput out] (.writeLong out (long (:value value))))

(nippy/extend-thaw ::observed
  [^DataInput in]
  (swap! thaw-threads conj (.getName (Thread/currentThread)))
  (->Observed (.readLong in)))

(deftest point-read-growth-releases-snapshots-and-does-not-decode-on-the-server
  (let [dir (u/tmp-dir (str "encoded-kv-" (UUID/randomUUID)))
        db (d/open-kv dir)]
    (try
      (d/open-dbi db "data")
      (d/transact-kv db [[:put "data" 1 {:tracked (->Observed 42)
                                        :text (.repeat "x" 10000)} :long :data]])
      (let [result (kv/read-value-result db "data" 1 :long :data false)]
        (reset! thaw-threads [])
        (is (thrown? BufferOverflowException (encode-message result nil 16)))
        ;; An aborted encoding must release its snapshot before another write.
        (d/transact-kv db [[:put "data" 2 :after :long :data]])
        (doseq [options [nil {:compression :zstd :compression-threshold 0}]]
          (let [buffer (encode-message result options (if options 256 20000))]
            (is (empty? @thaw-threads))
            (let [[key value] (decode-message buffer options)]
              (is (= 1 key))
              (is (= (->Observed 42) (:tracked value)))
              (is (= (.repeat "x" 10000) (:text value))))
            (is (= 1 (count @thaw-threads)))
            (reset! thaw-threads []))))
      (is (nil? (decode-message
                  (encode-message (kv/read-value-result db "data" 9 :long :data true)
                                  nil 1024) nil)))
      (finally (d/close-kv db) (u/delete-files dir)))))

(deftest remote-kv-and-datalog-skip-server-thaw-with-legacy-peer-fallback
  (let [root (u/tmp-dir (str "encoded-server-" (UUID/randomUUID)))
        port (allocate-port)
        srv (server/create {:root root :port port})
        base (str "dtlv://datalevin:datalevin@localhost:" port)]
    (try
      (server/start srv)
      (let [db (d/open-kv (str base "/kv") {:client-opts {:pool-size 1}})
            conn (d/create-conn (str base "/dl")
                                {:key {:db/unique :db.unique/identity}
                                 :text {:db/valueType :db.type/string}
                                 :tracked {}}
                                {:client-opts {:pool-size 1}})]
        (try
          (d/open-dbi db "data")
          (d/transact-kv db [[:put "data" :key (->Observed 42)]])
          (d/transact! conn [{:db/id 1 :key "one" :text "東京 👋" :tracked (->Observed 42)}])
          (reset! thaw-threads [])
          (is (= (->Observed 42) (d/get-value db "data" :key)))
          (is (= [(.getName (Thread/currentThread))] @thaw-threads))
          (reset! thaw-threads [])
          (is (= {:text "東京 👋" :tracked (->Observed 42)}
                 (d/pull @conn [:text :tracked] [:key "one"])))
          (is (= [(.getName (Thread/currentThread))] @thaw-threads))
          ;; A peer that did not advertise storage reads receives ordinary values.
          (with-open [channel (SocketChannel/open (InetSocketAddress. "localhost" (int port)))]
            (let [legacy (client/->Connection channel 2000 (ByteBuffer/allocate 4096))]
              (try
                (client/send-n-receive legacy {:type :set-client-id
                                               :client-id (client/get-id (.-client ^KVStore db))})
                (reset! thaw-threads [])
                (is (= (->Observed 42)
                       (:result (client/send-n-receive
                                  legacy {:type :get-value :writing? false
                                          :args ["kv" "data" :key :data :data true]}))))
                (is (= 2 (count @thaw-threads)))
                (finally (client/close legacy)))))
          (finally (d/close-kv db) (d/close conn))))
      (finally (server/stop srv) (u/delete-files root)))))

(deftest datalog-read-floor-restores-the-caller-binding
  (let [client (reify client/IClient
                 (request [_ req] {:type :command-complete :result (:ha-read-min-tx req)}))]
    (doseq [outer [nil 4 9]
            current [0 4 9]
            writing? [false true]]
      (binding [client/*ha-read-min-tx* outer]
        (is (= (when (and (not writing?) (pos? (long current))) current)
               (#'remote/datalog-request (AtomicLong. (long current)) client :pull ["db"] writing?)))
        (is (= outer client/*ha-read-min-tx*))))))
