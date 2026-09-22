(ns datalevin.pull-wire-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [datalevin.client :as client]
            [datalevin.constants :as c]
            [datalevin.core :as d]
            [datalevin.native-value :as nv]
            [datalevin.prepared :as prepared]
            [datalevin.protocol :as p]
            [datalevin.protocol.context :as context]
            [datalevin.pull-api :as pull]
            [datalevin.pull-wire :as wire]
            [datalevin.read-encode :as enc]
            [datalevin.server :as server]
            [datalevin.test.core :refer [allocate-port db-fixture]]
            [datalevin.util :as u]
            [taoensso.nippy :as nippy]
            [taoensso.nippy.impl :as impl]
            [taoensso.nippy.schema :as schema])
  (:import [datalevin.client Connection]
           [datalevin.pull_wire Layout]
           [datalevin.remote KVStore]
           [java.nio ByteBuffer BufferOverflowException]
           [java.util UUID]))

(use-fixtures :each db-fixture)

(defn- result [writer keys values]
  (enc/read-result
    (fn [out]
      (let [position (wire/start! writer out)
            mask (reduce-kv (fn [mask index key]
                              (if (contains? values key)
                                (do (enc/write-value! out (get values key))
                                    (bit-set mask index))
                                mask))
                            0 keys)]
        (wire/finish! writer out position mask)))))

(defn- frame
  ([value] (frame value nil 65536))
  ([value opts capacity]
   (let [out (ByteBuffer/allocateDirect capacity)]
     (p/write-message-bf out {:type :command-complete :result value}
                         c/message-format-nippy opts)
     out)))

(defn- decode [ctx out opts]
  (context/with-context ctx
    (:result (first (p/receive-one-message out opts)))))

(deftest layouts-preserve-values-sparsity-and-immutable-map-semantics
  (doseq [n [1 10 63 64]
          opts [nil {:compression :zstd :compression-threshold 0}]]
    (let [keys (mapv #(keyword "field" (str %)) (range n))
          values (zipmap keys (cycle [false nil 42 "東京 👋" {:data [:a :b]}]))
          writer (wire/writer 7)
          handles (prepared/handle-cache)]
      (with-open [ctx (context/create handles)]
        (wire/select-layout! writer (wire/layout keys))
        (let [cold (frame (result writer keys values) opts 65536)
              cold-size (.position ^ByteBuffer cold)
              first-result (decode ctx cold opts)
              layout (.get handles 7)]
          (is (= values first-result))
          (is (map? first-result))
          (is (= (hash values) (hash first-result)))
          (is (= (assoc values :extra 99) (assoc first-result :extra 99)))
          (is (= (dissoc values (first keys)) (dissoc first-result (first keys))))
          (wire/response-written! writer)
          (let [hot (frame (result writer keys values) opts 65536)]
            (when (nil? opts) (is (< (.position ^ByteBuffer hot) cold-size)))
            (is (= values (decode ctx hot opts))))
          (is (identical? layout (.get handles 7)))
          (doseq [subset [(select-keys values [(first keys) (last keys)]) {}]]
            (is (= (not-empty subset)
                   (decode ctx (frame (result writer keys subset) opts 65536) opts))))
          (is (= values first-result) "later replies cannot mutate an earlier map"))))))

(deftest unsent-layouts-survive-buffer-retries-and-fallback-replies
  (let [writer (wire/writer 8)
        keys [:data]
        values {:data (.repeat "x" 5000)}]
    (with-open [ctx (context/create (prepared/handle-cache))]
      (wire/select-layout! writer (wire/layout keys))
      (is (thrown? BufferOverflowException (frame (result writer keys values) nil 40)))
      (is (= values (decode ctx (frame (result writer keys values)) nil)))
      (wire/response-written! writer)
      ;; Encode a changed layout, but never send the frame. An intervening
      ;; ordinary response must not acknowledge that unsent definition.
      (wire/begin-response! writer)
      (wire/select-layout! writer (wire/layout [:other]))
      (frame (result writer [:other] {:other 42}))
      (wire/begin-response! writer)
      (wire/response-written! writer)
      (is (= {:other 43}
             (decode ctx (frame (result writer [:other] {:other 43})) nil)))
      (wire/response-written! writer)
      (is (= {:other 44}
             (decode ctx (frame (result writer [:other] {:other 44})) nil))))))

(deftest layout-values-share-the-enclosing-nippy-cache
  (let [writer (wire/writer 9)
        value (with-meta [:a :b] {:origin :value})
        cached (nippy/cache value)
        out (ByteBuffer/allocateDirect 4096)]
    (wire/select-layout! writer (wire/layout [:data]))
    (p/write-message-bf out [cached (result writer [:data] {:data cached}) cached])
    (with-open [ctx (context/create (prepared/handle-cache))]
      (context/with-context ctx
        (let [[a m b] (first (p/receive-one-message out))]
          (is (= {:data value} m))
          (is (identical? a b))
          (is (identical? a (:data m)))
          (is (= {:origin :value} (meta (:data m)))))))))

(defn- malformed-result [id definition mask]
  (enc/read-result
    (fn [^ByteBuffer out]
      (.put out (unchecked-byte schema/id-prefixed-custom-md))
      (.putShort out (short (impl/coerce-custom-type-id :datalevin.pull-wire/result)))
      (.putLong out (long id))
      (enc/write-value! out definition)
      (.putLong out (long mask)))))

(deftest invalid-layouts-do-not-fall-through-to-legacy-thaw-or-other-connections
  (doseq [[id definition mask] [[1 nil 0] [0 [:a] 0] [1 [:a] 2]
                                [1 [:a :a] 0] [1 ["a"] 0]
                                [1 (mapv #(keyword (str %)) (range 65)) 0]]]
    (with-open [ctx (context/create (prepared/handle-cache))]
      (let [error (try (decode ctx (frame (malformed-result id definition mask)) nil)
                       nil (catch Exception e e))]
        (is (nv/decoding-error? error)))))
  (let [writer (wire/writer 10)]
    (wire/select-layout! writer (wire/layout [:a]))
    (with-open [first-ctx (context/create (prepared/handle-cache))
                other-ctx (context/create (prepared/handle-cache))]
      (is (= {:a 1} (decode first-ctx (frame (result writer [:a] {:a 1})) nil)))
      (wire/response-written! writer)
      (is (thrown? Exception (decode other-ctx (frame (result writer [:a] {:a 2})) nil))))))

(defn- with-client-connection [conn f]
  (let [^KVStore kv (d/datalog-kv conn)
        pool (client/get-pool (.-client kv))
        socket (client/get-connection pool)]
    (try (f socket) (finally (client/release-connection pool socket)))))

(defn- client-layouts [conn]
  (with-client-connection conn
    (fn [^Connection socket]
      (into {} (filter (fn [[_ value]] (instance? Layout value)))
            (.-prepared-handles socket)))))

(deftest remote-layouts-refresh-fallback-evict-and-reconnect
  (doseq [compression [:none :zstd]]
    (let [root (u/tmp-dir (str "pull-layout-" (UUID/randomUUID)))
          port (allocate-port)
          srv (server/create {:root root :port port})]
      (try
        (server/start srv)
        (let [conn (d/create-conn
                     (str "dtlv://datalevin:datalevin@localhost:" port "/layout")
                     {:key {:db/unique :db.unique/identity}
                      :name {:db/valueType :db.type/string}
                      :flag {:db/valueType :db.type/boolean}
                      :data {} :friend {:db/valueType :db.type/ref}}
                     {:client-opts {:pool-size 1 :wire-compression compression
                                     :wire-compression-threshold 0}})]
          (try
            (d/transact! conn [{:db/id 1 :key "one" :name "one" :flag false
                               :data {:nested [:a :b]} :friend 2}
                              {:db/id 2 :key "two" :name (.repeat "文" 3000)}])
            (let [pattern [:db/id :name :flag :data]
                  reader (d/prepare-pull @conn pattern)]
              (doseq [id [1 2 999 [:key "absent"] 1]]
                (is (= (d/pull @conn pattern id) (reader id))))
              (is (= 1 (count (client-layouts conn))))
              (let [layout (first (vals (client-layouts conn)))]
                (d/transact! conn [[:db/add 1 :name "updated"]])
                (is (= "updated" (:name (reader 1))))
                (is (identical? layout (first (vals (client-layouts conn))))))
              (doseq [fallback ['[*] '[[:name :as :label]] '[{:friend [:name]}]
                                [] [:unknown] '[[:unknown :default false]]]]
                (let [read-fallback (d/prepare-pull @conn fallback)]
                  (doseq [id [1 2 999]]
                    (is (= (d/pull @conn fallback id) (read-fallback id))))))
              (testing "schema changes can switch a prepared read to ordinary encoding"
                (d/update-schema conn {:name {:db/cardinality :db.cardinality/many}})
                (d/transact! conn [[:db/add 1 :name "second"]])
                (is (= (d/pull @conn pattern 1) (reader 1)))))
            (let [reader (d/prepare-pull @conn [:flag :data])]
              (is (= {:flag false :data {:nested [:a :b]}} (reader 1)))
              (let [old (client-layouts conn)]
                ;; Register fallback patterns directly on the server so the
                ;; client's handle cache still contains the now-evicted handle.
                (with-client-connection conn
                  (fn [socket]
                    (dotimes [n prepared/max-handles]
                      (is (= :command-complete
                             (:type (client/send-n-receive socket
                                       {:type :pull :args ["layout" '[*] 1 nil]
                                        :prepare-id (+ 100000 n)})))))))
                (is (= old (client-layouts conn)))
                (is (= {:flag false :data {:nested [:a :b]}} (reader 1)))
                (is (not= old (client-layouts conn))))
              (with-client-connection conn client/close)
              (is (= {:flag false :data {:nested [:a :b]}} (reader 1)))
              (d/with-transaction [tx conn]
                (let [tx-reader (d/prepare-pull @tx [:flag :data])]
                  (d/transact! tx [[:db/add 1 :flag true]])
                  (is (= {:flag true :data {:nested [:a :b]}} (tx-reader 1)))
                  (d/abort-transact tx)))
              (is (= false (:flag (reader 1)))))
            (finally (d/close conn))))
        (finally (server/stop srv) (u/delete-files root))))))

(deftest prepared-pulls-remain-compatible-without-the-layout-capability
  (let [root (u/tmp-dir (str "pull-layout-legacy-" (UUID/randomUUID)))
        port (allocate-port)
        srv (server/create {:root root :port port})]
    (try
      (server/start srv)
      (let [conn (d/create-conn
                     (str "dtlv://datalevin:datalevin@localhost:" port "/legacy")
                     {:name {}} {:client-opts {:pool-size 1}})]
          (try
            (let [^KVStore kv (d/datalog-kv conn)
                  client-id (client/get-id (.-client kv))]
              (with-client-connection conn
                (fn [socket]
                  (let [capabilities (dissoc (p/local-wire-capabilities) :prepared-pull?)
                      response (client/send-n-receive
                                 socket {:type :set-client-id
                                         :client-id client-id
                                         :wire-capabilities capabilities})]
                  (is (= :set-client-id-ok (:type response)))
                  (#'client/set-conn-wire-opts! socket (p/negotiate-wire-opts capabilities))))))
            (d/transact! conn [{:db/id 1 :name "one"}])
            (let [reader (d/prepare-pull @conn [:name])]
              (dotimes [_ 3] (is (= {:name "one"} (reader 1))))
              (is (empty? (client-layouts conn))))
            (finally (d/close conn))))
      (finally (server/stop srv) (u/delete-files root)))))

(deftest shared-prepared-pulls-retain-separate-layouts-on-pooled-connections
  (let [root (u/tmp-dir (str "pull-layout-pool-" (UUID/randomUUID)))
        port (allocate-port)
        srv (server/create {:root root :port port})]
    (try
      (server/start srv)
      (let [conn (d/create-conn
                   (str "dtlv://datalevin:datalevin@localhost:" port "/pool")
                   {:name {} :value {}} {:client-opts {:pool-size 4}})]
        (try
          (d/transact! conn (mapv (fn [id] {:db/id id :name (str id) :value id}) (range 1 17)))
          (let [full (d/prepare-pull @conn [:db/id :name :value])
                partial (d/prepare-pull @conn [:value :name])
                start (promise)
                workers (mapv (fn [worker]
                                (future
                                  @start
                                  (every? true?
                                          (for [n (range 100)
                                                :let [id (inc (mod (+ n worker) 16))]]
                                            (if (even? n)
                                              (= {:db/id id :name (str id) :value id} (full id))
                                              (= {:name (str id) :value id} (partial id)))))))
                              (range 8))]
            (deliver start true)
            (doseq [worker workers] (is (true? (deref worker 30000 ::timeout)))))
          (finally (d/close conn))))
      (finally (server/stop srv) (u/delete-files root)))))

(deftest storage-projections-handle-the-bitmap-boundary-and-large-pattern-fallback
  (let [path (u/tmp-dir (str "pull-layout-width-" (UUID/randomUUID)))
        attrs (mapv #(keyword "field" (str %)) (range 65))
        conn (d/create-conn path (zipmap attrs (repeat {:db/valueType :db.type/long})))]
    (try
      (d/transact! conn [(assoc (zipmap attrs (range 65)) :db/id 1)
                        {:db/id 2 (first attrs) 0 (last attrs) 64}])
      (doseq [n [63 64 65], id? [false true]]
        (let [pattern (into (if id? [:db/id] []) (take n attrs))
              writer (wire/writer 1)
              reader (pull/pull-reader pattern nil writer)
              handles (prepared/handle-cache)]
          (with-open [ctx (context/create handles)]
            (doseq [id [1 2 999 1]]
              (is (= (d/pull @conn pattern id)
                     (decode ctx (frame (reader @conn id true)) nil)))
              (wire/response-written! writer))
            (is (= (if (<= (count pattern) 64) 1 0) (.size handles))))))
      (finally (d/close conn) (u/delete-files path)))))
