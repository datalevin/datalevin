(ns datalevin.custom-data-test
  (:require
   [clojure.test :refer [deftest is use-fixtures]]
   [datalevin.bits :as b]
   [datalevin.constants :as c]
   [datalevin.core :as d]
   [datalevin.custom-data :as custom]
   [datalevin.custom-kv-dump :as custom-dump]
   [datalevin.custom-value :as cv]
   [datalevin.interface :as i]
   [datalevin.interpret :as inter]
   [datalevin.kv :as kv]
   [datalevin.lmdb :as l]
   [datalevin.udf :as udf]
   [datalevin.util :as u])
  (:import
   [java.util Arrays Date Random UUID]
   [java.util.concurrent TimeUnit]))

(def ^:dynamic *dir* nil)
(def ^:dynamic *handles* nil)

(use-fixtures
  :each
  (fn [f]
    (binding [*dir* (u/tmp-dir (str "custom-types-" (UUID/randomUUID)))
              *handles* (atom [])
              c/*db-background-sampling?* false]
      (u/file *dir*)
      (try (f)
           (finally
             (doseq [[handle close] (reverse @*handles*)] (close handle))
             (u/delete-files *dir*))))))

(defn- open-kv
  ([name] (open-kv name {}))
  ([name opts]
   (let [kv (d/open-kv (str *dir* "/" name) (merge {:wal? false} opts))]
     (swap! *handles* conj [kv d/close-kv])
     kv)))

(defn- task-type [offset]
  {:index {:type :long
           :order-fn (inter/inter-fn [task] (+ offset (:rank task)))}})

(defn- order-key [kv name value]
  ((:order-fn (custom/resolve-type kv name)) value))

(deftest registration-validation
  (let [kv (open-kv "validation")
        good (task-type 0)]
    (is (= {:revision 0 :types {}} (custom/registry kv)))
    (doseq [[name definition]
            [[:task good]
             [:db.type/long good]
             ["app/task" good]
             [:app/task (assoc good :version 0)]
             [:app/task (assoc good :unexpected true)]
             [:app/task (assoc-in good [:index :type] :data)]
             [:app/task (assoc-in good [:index :type] [])]
             [:app/task (assoc-in good [:index :type] [:long [:string]])]
             [:app/task (assoc-in good [:index :type] [:bytes])]
             [:app/task (assoc-in good [:index :order-fn] identity)]
             [:app/task (assoc-in good [:index :order-fn]
                                  (inter/inter-fn [a b] (+ a b)))]]]
      (is (thrown? Exception (d/register-type kv name definition))))
    (is (thrown-with-msg? Exception #"both serialize and deserialize"
                         (d/register-type kv :app/task
                                          (assoc good :payload {:serialize nil}))))
    (is (= {:revision 0 :types {}} (custom/registry kv)))))

(deftest source-structure-is-part-of-the-definition
  (let [kv (open-kv "source")]
    (d/register-type kv :app/task
                     {:index {:type :long
                              :order-fn (inter/compile-inter-fn-source
                                         '(fn [v] (:rank v)))}})
    (is (= :app/task
           (d/register-type kv :app/task
                            {:index {:type :long
                                     :order-fn (inter/inter-fn [v] (:rank v))}})))
    (is (thrown-with-msg?
         Exception #"different definition"
         (d/register-type kv :app/task
                          {:index {:type :long
                                   :order-fn (inter/inter-fn [v] [:rank v])}})))))

(deftest persisted-idempotent-registration
  (doseq [wal? [false true]]
    (let [name (str "persist-" wal?)
          kv (open-kv name {:wal? wal?})]
      (is (= :app/task (d/register-type kv :app/task (task-type 7))))
      (is (= 10 (order-key kv :app/task {:rank 3})))
      (is (= :app/task (d/register-type kv :app/task (task-type 7))))
      (is (= 1 (:revision (custom/registry kv))))
      (is (thrown-with-msg? Exception #"different definition"
                           (d/register-type kv :app/task (task-type 8))))
      (d/close-kv kv)
      (let [reopened (open-kv name {:wal? wal?})]
        (is (= 10 (order-key reopened :app/task {:rank 3})))
        (is (= :app/task (d/register-type reopened :app/task (task-type 7))))
        (is (= 1 (:revision (custom/registry reopened))))
        (is (contains? (:types @(i/kv-info reopened)) :app/task))))))

(deftest captured-values-compare-by-content
  (let [kv (open-kv "captures")
        definition (fn []
                     (let [data (byte-array [1 2 3])
                           inner (inter/inter-fn [x] (+ x 1))]
                       {:index {:type :long
                                :order-fn (inter/inter-fn [x]
                                            (+ (count data) (inner x)))}}))]
    (d/register-type kv :app/captured (definition))
    (d/register-type kv :app/captured (definition))
    (is (= 1 (:revision (custom/registry kv))))
    (is (bytes? (get-in (custom/registry kv)
                       [:types :app/captured :index :order-fn :inter-fn/source])))
    (is (= 9 (order-key kv :app/captured 5)))))

(deftest registration-obeys-outer-transaction
  (let [kv (open-kv "transactions" {:wal? true})]
    (is (= 0 (:revision (custom/registry kv))))
    (is (thrown-with-msg?
         Exception #"abort registration"
         (d/with-transaction-kv [tx kv]
           (d/register-type tx :app/task (task-type 1))
           (is (= 4 (order-key tx :app/task {:rank 3})))
           (is (= 0 (:revision (custom/registry kv))))
           (throw (ex-info "abort registration" {})))))
    (is (= {:revision 0 :types {}} (custom/registry kv)))
    (d/with-transaction-kv [tx kv]
      (d/register-type tx :app/task (task-type 8))
      (d/register-type tx :app/other (task-type 0))
      (is (= 11 (order-key tx :app/task {:rank 3}))))
    (is (= 2 (:revision (custom/registry kv))))
    (is (= 11 (order-key kv :app/task {:rank 3})))
    (d/with-transaction-kv [tx kv]
      (d/register-type tx :app/aborted (task-type 0))
      (d/abort-transact-kv tx))
    (is (= #{:app/task :app/other} (set (keys (:types (custom/registry kv))))))))

(deftest registration-is-serialized
  (let [kv (open-kv "concurrent")
        start (promise)
        workers (doall (for [_ (range 6)]
                         (future @start (d/register-type kv :app/task (task-type 2)))))]
    (deliver start true)
    (is (= (repeat 6 :app/task) (mapv deref workers)))
    (is (= 1 (:revision (custom/registry kv))))))

(deftest registry-is-shared-by-datalog-and-kv
  (let [path (str *dir* "/shared")
        a (d/create-conn path nil {:wal? false})
        other (open-kv "independent")]
    (swap! *handles* conj [a d/close])
    (let [b (d/create-conn path nil {:wal? false})]
      (swap! *handles* conj [b d/close])
      (is (= 0 (:revision (custom/registry (d/datalog-kv b)))))
      (d/register-type a :app/task (task-type 4))
      (is (= 7 (order-key (d/datalog-kv b) :app/task {:rank 3})))
      (d/register-type (d/datalog-kv b) :app/other (task-type 0))
      (is (= 2 (:revision (custom/registry (d/datalog-kv a)))))
      (is (= 0 (:revision (custom/registry other)))))))

(deftest function-output-contracts
  (let [kv (open-kv "functions")]
    (d/register-type kv :app/task (task-type 0))
    (let [{:keys [serialize deserialize]} (custom/resolve-type kv :app/task)
          task {:rank 3 :description "Payload retains all fields"}]
      (is (= task (deserialize (serialize task)))))
    (d/register-type kv :app/tuple
                     {:index {:type [:long :string]
                              :order-fn (inter/inter-fn [v] v)}})
    (is (= [nil "ok"] (order-key kv :app/tuple [nil "ok"])))
    (doseq [value [[1] [1 "ok" 2] [1 2]]]
      (is (thrown? Exception (order-key kv :app/tuple value))))
    (d/register-type kv :app/single-tuple
                     {:index {:type [:long] :order-fn (inter/inter-fn [v] v)}})
    (is (= [3] (order-key kv :app/single-tuple [3])))
    (is (thrown? Exception (order-key kv :app/single-tuple [1 2])))
    (is (thrown? Exception (order-key kv :app/single-tuple [nil])))
    (d/register-type kv :app/wrong
                     {:index {:type :long :order-fn (inter/inter-fn [_] "bad")}
                      :payload {:serialize (inter/inter-fn [v] v)
                                :deserialize (inter/inter-fn [v] v)}})
    (let [{:keys [order-fn serialize deserialize]} (custom/resolve-type kv :app/wrong)]
      (is (thrown? Exception (order-fn 1)))
      (is (thrown? Exception (serialize "bad")))
      (is (thrown? Exception (deserialize "bad"))))))

(defn- udf-desc [kind]
  {:udf/lang :test :udf/kind kind :udf/id :app/task :udf/version 1})

(deftest udf-bindings-and-runtime-options
  (let [runtime (udf/create-registry)
        kv (open-kv "udf" {:runtime-opts {:udf-registry runtime}})
        definition {:index {:type :long :order-fn (udf-desc :order-fn)}
                    :payload {:serialize (udf-desc :serializer)
                              :deserialize (udf-desc :deserializer)}}]
    (d/register-type kv :app/task definition)
    (is (thrown-with-msg? Exception #"No UDF resolver" (order-key kv :app/task {})))
    (udf/register! runtime (udf-desc :order-fn) :rank)
    (is (= 3 (order-key kv :app/task {:rank 3})))
    (udf/register! runtime (udf-desc :serializer) b/serialize)
    (udf/register! runtime (udf-desc :deserializer) b/deserialize)
    (let [{:keys [serialize deserialize]} (custom/resolve-type kv :app/task)]
      (is (= {:rank 3} (deserialize (serialize {:rank 3})))))
    (udf/register! runtime (udf-desc :order-fn) (fn [v] (+ 10 (long (:rank v)))))
    (is (= 13 (order-key kv :app/task {:rank 3})))
    (udf/unregister! runtime (udf-desc :order-fn))
    (is (thrown? Exception (order-key kv :app/task {:rank 3})))
    (is (nil? (:runtime-opts (i/env-opts kv))))
    (is (nil? (i/get-value kv c/kv-info :runtime-opts)))
    (d/close-kv kv)
    (let [reopened (open-kv "udf")]
      (is (= 1 (:revision (custom/registry reopened))))
      (is (thrown-with-msg? Exception #"No UDF registry"
                           (order-key reopened :app/task {:rank 3}))))))

(deftest udf-resolution-is-scoped-to-kv-context
  (let [runtime (udf/create-registry)
        opts {:runtime-opts {:udf-registry runtime}}
        a (open-kv "udf-a" opts)
        b (open-kv "udf-b" opts)
        calls (atom [])
        definition {:index {:type :string :order-fn (udf-desc :order-fn)}}]
    (udf/register-resolver! runtime :test
                           (fn [{:keys [kv type-name kind]} _]
                             (swap! calls conj [type-name kind])
                             (constantly (d/dir kv))))
    (doseq [kv [a b]]
      (d/register-type kv :app/context definition)
      (is (= (d/dir kv) (order-key kv :app/context nil)))
      (is (= (d/dir kv) (order-key kv :app/context nil))))
    (is (= [[:app/context :order-fn] [:app/context :order-fn]] @calls))))

(deftest registry-survives-copy-and-kv-dump
  (let [kv (open-kv "backup")
        copied-path (str *dir* "/copy")]
    (d/register-type kv :app/task (task-type 6))
    (d/open-dbi kv "user-data")
    (d/transact-kv kv "user-data" [[:put :key "value"]])
    (d/copy kv copied-path)
    (let [copied (open-kv "copy")]
      (is (= 9 (order-key copied :app/task {:rank 3}))))
    (let [dump (with-out-str (l/dump-all kv))
          restored (open-kv "restored")]
      (is (= 0 (:revision (custom/registry restored))))
      (l/load-all restored (java.io.PushbackReader. (java.io.StringReader. dump)))
      (is (= 1 (:revision (custom/registry restored))))
      (is (= 9 (order-key restored :app/task {:rank 3})))
      (is (= "value" (d/get-value restored "user-data" :key))))))

(deftest captured-functions-reopen-in-a-fresh-jvm
  (let [kv (open-kv "cold")
        path (d/dir kv)
        inner (inter/inter-fn [v] (+ v 4))]
    (d/register-type kv :app/task
                     {:index {:type :long
                              :order-fn (inter/inter-fn [v] (inner (:rank v)))}})
    (d/close-kv kv)
    (let [code (pr-str
                `(do
                   (require 'datalevin.core 'datalevin.custom-data)
                   ;; data_readers.clj creates a reader var in this namespace
                   ;; before its implementation is loaded.
                   (assert (nil? (ns-resolve 'datalevin.interpret
                                             'compile-inter-fn-source)))
                   (let [kv# (datalevin.core/open-kv ~path {:wal? false})]
                     (try
                       (assert (nil? (ns-resolve 'datalevin.interpret
                                                 'compile-inter-fn-source)))
                       (assert (= 7 ((:order-fn
                                       (datalevin.custom-data/resolve-type
                                        kv# :app/task)) {:rank 3})))
                       (finally (datalevin.core/close-kv kv#))))))
          log-file (java.io.File. (str *dir* "/cold-jvm.log"))
          command [(str (System/getProperty "java.home") "/bin/java")
                   "--add-opens=java.base/java.nio=ALL-UNNAMED"
                   "--add-opens=java.base/java.util=ALL-UNNAMED"
                   "--add-opens=java.base/java.lang=ALL-UNNAMED"
                   "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED"
                   "--enable-native-access=ALL-UNNAMED"
                   "-cp" (System/getProperty "java.class.path")
                   "clojure.main" "-e" code]
          process (.start (doto (ProcessBuilder. ^java.util.List command)
                            (.redirectErrorStream true)
                            (.redirectOutput ^java.io.File log-file)))]
      (try
        (let [finished? (.waitFor process 45 TimeUnit/SECONDS)]
          (is finished? "Fresh JVM reopen timed out")
          (when finished? (is (zero? (.exitValue process)) (slurp log-file))))
        (finally
          (when (.isAlive process) (.destroyForcibly process)))))))

(defn- unsigned-compare ^long [^bytes a ^bytes b] (Arrays/compareUnsigned a b))

(defn- stored-type [kv backing]
  (d/register-type kv :app/value
                   {:index {:type backing
                            :order-fn (inter/inter-fn [v] (:order v))}})
  (cv/open-store! kv)
  (custom/resolve-type kv :app/value))

(defn- key-index [kv name]
  (d/open-dbi kv name)
  {:dbi name :position :key})

(defn- item-index [kv name key]
  (d/open-list-dbi kv name)
  {:dbi name :position :item :key (cv/encode-order :string key)})

(defn- all-ids [kv index]
  (mapv cv/reference-id
        (if (= :key (:position index))
          (map first (d/get-range kv (:dbi index) [:all] :raw :raw))
          (d/get-list kv (:dbi index) (:key index) :raw :raw))))

(deftest reference-framing-preserves-native-byte-order
  (let [random (Random. 234)
        values (into (mapv byte-array [[] [0] [0 0] [0 -1] [1] [-1] [-1 0]])
                     (repeatedly 200
                                 #(let [bs (byte-array (.nextInt random 80))]
                                    (.nextBytes random bs) bs)))
        refs (mapv #(cv/reference (cv/reference-prefix %) 123) values)]
    (doseq [[value ref] (map vector values refs)]
      (let [{:keys [id truncated? order-bytes]} (cv/decode-reference ref)]
        (is (= 123 id))
        (is (false? truncated?))
        (is (Arrays/equals ^bytes value ^bytes order-bytes))))
    (is (= (mapv vec (sort unsigned-compare values))
           (mapv #(vec (:order-bytes (cv/decode-reference %)))
                 (sort unsigned-compare refs))))
    (doseq [budget [11 12 13 20 507 511]
            value values]
      (is (<= (alength (cv/reference (cv/reference-prefix value budget) 1))
              budget)))
    (doseq [budget [11 12 13 20 507 511]]
      (let [refs (mapv #(cv/reference (cv/reference-prefix % budget) 1)
                       (sort unsigned-compare values))]
        (is (every? (fn [[a b]] (<= (unsigned-compare a b) 0))
                     (partition 2 1 refs))
            "Truncation can merge order buckets but cannot reverse them")))
    (doseq [bad [(byte-array []) (byte-array (repeat 11 0))
                 (byte-array [-16 0 2 0 0 0 0 0 0 0 1])
                 (byte-array [-16 0 -1 0 0 0 0 0 0 0 1])]]
      (is (thrown? Exception (cv/decode-reference bad))))
    (doseq [sentinel [cv/min-id cv/max-id]]
      (is (thrown? Exception
                   (cv/reference-id (cv/reference (cv/reference-prefix
                                                   (byte-array [1])) sentinel)))))))

(deftest references-retain-each-backing-codecs-order
  (let [cases [[:long [Long/MIN_VALUE -10 -1 0 1 10 Long/MAX_VALUE]]
               [:id [0 1 Long/MAX_VALUE Long/MIN_VALUE -1]]
               [:boolean [false true]]
               [:float [-5.5 -0.0 0.0 5.5]]
               [:double [-50.5 -0.0 0.0 50.5]]
               [:instant [(Date. Long/MIN_VALUE) (Date. -1) (Date. 0)
                          (Date. Long/MAX_VALUE)]]
               [:uuid [(UUID. 0 0) (UUID. 0 1) (UUID. -1 -1)]]
               [:bigint (mapv biginteger [-123456789012345678901234567890N
                                          -1N 0N 1N
                                          123456789012345678901234567890N])]
               [:bigdec [-1234567890.123456789M -1M 0M 1M 1.00001M]]
               [:string ["" "a" "a\u0000" "a\u0000b" "ab" "é" "猫"]]
               [:keyword [:a :ab :a/b :a/bc :ab/c]]
               [:symbol ['a 'ab 'a/b 'a/bc 'ab/c]]
               [:bytes (mapv byte-array [[] [0] [0 0] [0 -1] [1] [-1]])]
               [[:long] [[Long/MIN_VALUE] [-1] [0] [Long/MAX_VALUE]]]
               [[:string] [[""] ["a"] ["ab"] ["猫"]]]
               [[:long :string] [[nil nil] [-1 "a"] [0 nil] [0 ""]
                                 [0 "a"] [0 "ab"] [1 "a"]]]]]
    (doseq [[backing values] cases]
      (let [kv (open-kv (str "backing-" (hash backing)))
            type (stored-type kv backing)
            index (key-index kv "index")
            encoded (mapv #(cv/encode-order backing %) values)
            ;; Insert backwards so an accidentally dominant ID would reverse it.
            ids (into {}
                      (for [n (reverse (range (count values)))]
                        [n (cv/reference-id
                            (cv/put-value! kv index type
                                           {:order (nth values n) :n n}
                                           (byte-array [n])))]))
            expected (sort (fn [a b]
                             (let [c (unsigned-compare (nth encoded a)
                                                       (nth encoded b))]
                               ;; Native codecs may normalize values (e.g. -0.0).
                               (if (zero? c) (compare (ids a) (ids b)) c)))
                           (range (count values)))]
        (is (= (mapv ids expected) (all-ids kv index)) (str backing))))))

(deftest collision-matching-replacement-and-ownership
  (let [kv (open-kv "collisions")
        type (stored-type kv :long)
        a (key-index kv "a")
        b (key-index kv "b")
        items (item-index kv "items" "first")
        other-items (assoc items :key (cv/encode-order :string "second"))
        x {:order 3 :name "x"}
        y {:order 3 :name "y"}
        rx (cv/put-value! kv a type x (byte-array [1]))
        ry (cv/put-value! kv a type y (byte-array [2]))]
    (is (not= (cv/reference-id rx) (cv/reference-id ry)))
    (is (= y (:value (cv/find-value kv a type y))))
    (is (= [2] (vec (:associated (cv/find-value kv a type y)))))
    (is (= (vec rx) (vec (cv/put-value! kv a type x (byte-array [9])))))
    (is (= [9] (vec (:associated (cv/find-value kv a type x)))))
    (is (= 2 (d/entries kv c/custom-values)))
    (let [refs [(cv/put-value! kv b type x (byte-array [0]))
                (cv/put-value! kv items type x nil)
                (cv/put-value! kv other-items type x nil)]]
      (is (= 5 (count (set (map cv/reference-id (concat [rx ry] refs)))))))
    (cv/put-value! kv items type y nil)
    (is (= y (:value (cv/delete-value! kv items type y))))
    (is (= x (:value (cv/find-value kv items type x))))
    (is (nil? (cv/find-value kv items type y)))
    (is (nil? (cv/delete-value! kv a type {:order 3 :name "missing"})))
    (is (= y (:value (cv/delete-value! kv a type y))))
    (is (= x (cv/read-value kv type rx)))
    (is (nil? (d/get-value kv c/custom-values (cv/reference-id ry) :id :raw)))
    (is (= 1 (cv/clear-values! kv items)))
    (is (= 1 (cv/clear-values! kv a)))
    (is (= 2 (d/entries kv c/custom-values)))
    (is (= x (:value (cv/find-value kv other-items type x))))
    (is (= x (:value (cv/find-value kv b type x))))))

(deftest range-boundaries-include-or-exclude-the-whole-id-group
  (let [kv (open-kv "bounds")
        type (stored-type kv :string)
        key-idx (key-index kv "keys")
        item-idx (item-index kv "items" "list")]
    (doseq [index [key-idx item-idx]]
      (doseq [name ["a" "b"] order ["" "a" "ab" "b"]]
        (cv/put-value! kv index type {:order order :name name} (byte-array [1])))
      (let [scan (fn [range]
                   (mapv #(cv/read-value kv type %)
                         (if (= :key (:position index))
                           (map first (d/get-range kv (:dbi index) range :raw :raw))
                           (map second (d/list-range
                                        kv (:dbi index)
                                        [:closed (:key index) (:key index)] :raw
                                        range :raw)))))
            a (cv/order-prefix type {:order "a"})
            b (cv/order-prefix type {:order "b"})]
        (doseq [include-a? [false true] include-b? [false true]]
          (let [lower (cv/boundary a :lower include-a?)
                upper (cv/boundary b :upper include-b?)
                values (scan [:closed lower upper])]
            (is (= (concat (when include-a? ["a" "a"]) ["ab" "ab"]
                           (when include-b? ["b" "b"]))
                   (map :order values)))
            (is (= (reverse values) (scan [:closed-back upper lower])))))))))

(deftest truncated-order-keys-still-match-complete-values
  (let [kv (open-kv "truncated")
        type (stored-type kv :string)
        index (key-index kv "index")
        stem (apply str (repeat 600 "x"))
        a {:order (str stem "a") :name "first"}
        b {:order (str stem "b") :name "second"}
        ra (cv/put-value! kv index type a (byte-array [1]))
        rb (cv/put-value! kv index type b (byte-array [2]))]
    (is (= 511 (alength ^bytes ra)))
    (is (:truncated? (cv/decode-reference ra)))
    (is (= (vec (:order-bytes (cv/decode-reference ra)))
           (vec (:order-bytes (cv/decode-reference rb)))))
    (is (= b (:value (cv/find-value kv index type b))))
    (cv/delete-value! kv index type b)
    (is (nil? (cv/find-value kv index type b)))
    (is (= a (:value (cv/find-value kv index type a))))))

(deftest payload-and-index-rollback-with-and-without-wal
  (doseq [wal? [false true]]
    (let [kv (open-kv (str "rollback-" wal?) {:wal? wal?})
          type (stored-type kv :long)
          index (key-index kv "index")
          a {:order 1 :name "a"}
          b {:order 1 :name "b"}
          ra (cv/put-value! kv index type a (byte-array [1]))]
      (is (thrown-with-msg?
           Exception #"rollback"
           (l/with-transaction-kv [tx kv]
             (cv/put-value! tx index type a (byte-array [9]))
             (let [rb (cv/put-value! tx index type b (byte-array [2]))]
               (is (= b (:value (cv/find-value tx index type b))))
               (is (= (vec rb) (vec (cv/put-value! tx index type b
                                                  (byte-array [3]))))))
             (cv/delete-value! tx index type a)
             (is (nil? (cv/find-value tx index type a)))
             (throw (ex-info "rollback" {})))))
      (is (= [1] (vec (:associated (cv/find-value kv index type a)))))
      (is (nil? (cv/find-value kv index type b)))
      (is (= 1 (d/entries kv c/custom-values)))
      (is (= (cv/reference-id ra) (d/get-value kv c/kv-info cv/id-key :keyword)))
      (l/with-transaction-kv [tx kv]
        (cv/put-value! tx index type b (byte-array [2]))
        (cv/delete-value! tx index type b))
      (is (= 1 (d/entries kv c/custom-values)))
      ;; Fail after allocation/payload rows have been applied. Catching this
      ;; exception must not let a containing transaction commit those rows.
      (d/open-dbi kv "too-small" {:key-size 11})
      (l/with-transaction-kv [tx kv]
        (cv/put-value! tx index type b (byte-array [2]))
        (is (thrown? Exception
                     (cv/put-value! tx {:dbi "too-small" :position :key}
                                    type a (byte-array [0])))))
      (is (nil? (cv/find-value kv index type b)))
      (is (= 1 (d/entries kv c/custom-values)))
      (is (zero? (d/entries kv "too-small"))))))

(deftest raw-serde-matches-values-instead-of-payload-bytes
  (let [runtime (udf/create-registry)
        kv (open-kv "raw-serde" {:runtime-opts {:udf-registry runtime}})
        serial (atom 0)
        order-calls (atom [])
        serde-calls (atom [])]
    (udf/register! runtime (udf-desc :order-fn)
                   (fn [v] (swap! order-calls conj v) (:order v)))
    (udf/register! runtime (udf-desc :serializer)
                   (fn [v] (swap! serde-calls conj v)
                     (b/serialize [(swap! serial inc) v])))
    (udf/register! runtime (udf-desc :deserializer) #(second (b/deserialize %)))
    (d/register-type kv :app/value
                     {:index {:type :long :order-fn (udf-desc :order-fn)}
                      :payload {:serialize (udf-desc :serializer)
                                :deserialize (udf-desc :deserializer)}})
    (cv/open-store! kv)
    (let [type (custom/resolve-type kv :app/value)
          index (key-index kv "index")
          value {:order 1 :name "same"}
          ref (cv/put-value! kv index type value (byte-array [1]))
          id (cv/reference-id ref)]
      (is (= [1 value] (b/deserialize (d/get-value kv c/custom-values id :id :raw))))
      (is (= (vec ref) (vec (cv/put-value! kv index type value (byte-array [2])))))
      (is (= [2 value] (b/deserialize (d/get-value kv c/custom-values id :id :raw))))
      (is (= @order-calls @serde-calls))
      (is (= 1 (d/entries kv c/custom-values)))
      (is (= value (:value (cv/find-value kv index type value))))
      (udf/unregister! runtime (udf-desc :serializer))
      (is (thrown? Exception
                   (cv/put-value! kv index (custom/resolve-type kv :app/value)
                                  value (byte-array [3]))))
      (is (= [2] (vec (:associated (cv/find-value kv index type value)))))
      ;; Clearing ownership does not run any application function.
      (is (= 1 (cv/clear-values! kv index)))
      (is (zero? (d/entries kv c/custom-values))))))

(deftest payload-read-and-candidate-scan-share-a-snapshot
  (let [runtime (udf/create-registry)
        kv (open-kv "snapshot" {:runtime-opts {:udf-registry runtime}})
        entered (promise)
        release (promise)
        reader-thread (atom nil)]
    (udf/register! runtime (udf-desc :serializer) b/serialize)
    (udf/register! runtime (udf-desc :deserializer)
                   (fn [bs]
                     (when (= @reader-thread (.threadId (Thread/currentThread)))
                       (reset! reader-thread nil)
                       (deliver entered true)
                       (when (= ::timeout (deref release 10000 ::timeout))
                         (throw (ex-info "snapshot test timed out" {}))))
                     (b/deserialize bs)))
    (d/register-type kv :app/value
                     {:index {:type :long :order-fn (inter/inter-fn [v] (:order v))}
                      :payload {:serialize (udf-desc :serializer)
                                :deserialize (udf-desc :deserializer)}})
    (cv/open-store! kv)
    (let [type (custom/resolve-type kv :app/value)
          index (key-index kv "index")
          a {:order 1 :name "a"}
          b {:order 1 :name "b"}]
      (cv/put-value! kv index type a (byte-array [1]))
      (cv/put-value! kv index type b (byte-array [2]))
      (let [reader (future
                     (reset! reader-thread (.threadId (Thread/currentThread)))
                     (cv/find-value kv index type b))]
        (try
          (is (= true (deref entered 10000 ::timeout)))
          (cv/delete-value! kv index type b)
          (deliver release true)
          (is (= b (:value (deref reader 10000 ::timeout))))
          (is (nil? (cv/find-value kv index type b)))
          (finally (deliver release true) (future-cancel reader)))))))

(deftest allocation-survives-reopen-copy-and-deletion
  (let [kv (open-kv "allocation")
        type (stored-type kv :long)
        index (key-index kv "index")
        value {:order 1}
        id (cv/reference-id (cv/put-value! kv index type value (byte-array [0])))]
    (cv/delete-value! kv index type value)
    (d/copy kv (str *dir* "/allocation-copy"))
    (d/close-kv kv)
    (doseq [name ["allocation" "allocation-copy"]]
      (let [kv (open-kv name)
            type (custom/resolve-type kv :app/value)
            index (key-index kv "index")]
        (cv/open-store! kv)
        (is (= (inc id) (cv/reference-id
                         (cv/put-value! kv index type value (byte-array [0])))))
        (is (nil? (:custom-value-id (i/env-opts kv))))
        (d/transact-kv kv [(l/kv-tx :put c/kv-info cv/id-key 0 :keyword :data)])
        ;; ID 1 is no longer occupied; create it, then reset the counter to
        ;; simulate conflicting restored metadata. Never overwrite that payload.
        (cv/put-value! kv index type {:order 2} (byte-array [0]))
        (d/transact-kv kv [(l/kv-tx :put c/kv-info cv/id-key 0 :keyword :data)])
        (is (thrown? Exception
                     (cv/put-value! kv index type {:order 3} (byte-array [0]))))
        (is (= 2 (d/entries kv c/custom-values)))
        (d/transact-kv kv [(l/kv-tx :put c/kv-info cv/id-key
                                  (dec cv/max-id) :keyword :data)])
        (is (thrown? Exception
                     (cv/put-value! kv index type {:order 3} (byte-array [0]))))))))

(deftest concurrent-allocation-and-repeated-insertion
  (let [kv (open-kv "concurrent-values")
        type (stored-type kv :long)
        index (key-index kv "index")
        start (promise)
        writers (mapv (fn [n]
                        (future @start
                                (cv/put-value! kv index type {:order 1 :n n}
                                               (byte-array [n]))))
                      (range 12))]
    (deliver start true)
    (is (= 12 (count (set (mapv #(cv/reference-id @%) writers)))))
    (is (= 12 (d/get-value kv c/kv-info cv/id-key :keyword)))
    (let [writers (mapv (fn [_]
                          (future (cv/put-value! kv index type {:order 1 :n 0}
                                                 (byte-array [0]))))
                        (range 8))]
      (is (= 1 (count (set (mapv #(cv/reference-id @%) writers)))))
      (is (= 12 (d/entries kv c/custom-values))))))

(deftest function-and-encoding-failures-do-not-write
  (let [runtime (udf/create-registry)
        kv (open-kv "function-failures" {:runtime-opts {:udf-registry runtime}})
        index (key-index kv "index")]
    (udf/register! runtime (udf-desc :order-fn) :order)
    (udf/register! runtime (udf-desc :serializer)
                   (fn [v] (when-not (:bad-serde? v) (b/serialize v))))
    (udf/register! runtime (udf-desc :deserializer) b/deserialize)
    (d/register-type kv :app/value
                     {:index {:type :long :order-fn (udf-desc :order-fn)}
                      :payload {:serialize (udf-desc :serializer)
                                :deserialize (udf-desc :deserializer)}})
    (cv/open-store! kv)
    (let [type (custom/resolve-type kv :app/value)]
      (l/with-transaction-kv [tx kv]
        (cv/put-value! tx index type {:order 1} (byte-array [0]))
        (doseq [value [{:order "bad"} {:order 2 :bad-serde? true}]]
          (is (thrown? Exception (cv/put-value! tx index type value (byte-array [0]))))))
      (is (= 1 (d/entries kv c/custom-values)))
      (is (= 1 (d/get-value kv c/kv-info cv/id-key :keyword))))
    (d/register-type kv :app/tuple
                     {:index {:type [:string :long]
                              :order-fn (inter/inter-fn [v] v)}})
    (is (thrown-with-msg?
         Exception #"Cannot encode custom order key"
         (cv/put-value! kv index (custom/resolve-type kv :app/tuple)
                        [(apply str (repeat 300 "x")) 1] (byte-array [0]))))
    (is (= 1 (d/entries kv c/custom-values)))
    (is (= 1 (d/entries kv "index")))))

(deftest multiple-indexes-share-one-owned-payload
  (let [kv (open-kv "shared-reference")
        type (stored-type kv :long)
        value {:order 1 :name "one fact"}]
    (d/open-list-dbi kv "primary")
    (d/open-list-dbi kv "secondary")
    (let [ref (l/with-transaction-kv [tx kv]
                (let [{:keys [id txs]} (cv/allocate-payload tx ((:serialize type) value))
                      ref (cv/reference (cv/order-prefix type value) id)]
                  (cv/transact! tx (into txs [(l/kv-tx :put "primary" 42 ref :id :raw)
                                              (l/kv-tx :put "secondary" ref 42 :raw :id)]))
                  ref))]
      (is (= 1 (d/entries kv c/custom-values)))
      (is (= value (cv/read-value kv type ref)))
      (is (= [42] (vec (d/get-list kv "secondary" ref :raw :id))))
      (is (thrown? Exception
                   (l/with-transaction-kv [tx kv]
                     (cv/transact! tx [(l/kv-tx :del "primary" 42 :id)
                                       (l/kv-tx :del "secondary" ref :raw)
                                       (cv/delete-payload-tx ref)])
                     (throw (ex-info "rollback" {})))))
      (is (= 1 (d/entries kv c/custom-values)))
      (is (= 1 (d/entries kv "primary")))
      (is (= 1 (d/entries kv "secondary")))
      (l/with-transaction-kv [tx kv]
        (cv/transact! tx [(l/kv-tx :del "primary" 42 :id)
                          (l/kv-tx :del "secondary" ref :raw)
                          (cv/delete-payload-tx ref)]))
      (is (every? zero? (map #(d/entries kv %)
                             [c/custom-values "primary" "secondary"]))))))

(deftest storage-initialization-and-index-preconditions
  (let [kv (open-kv "initialization")]
    (l/with-transaction-kv [tx kv]
      (is (thrown-with-msg? Exception #"before starting a transaction"
                           (cv/open-store! tx))))
    (cv/open-store! kv)
    (let [type (stored-type kv :long)
          index (key-index kv "index")]
      (l/with-transaction-kv [tx kv]
        (is (identical? tx (cv/open-store! tx)))
        (cv/put-value! tx index type {:order 1} (byte-array [0])))
      (is (nil? (:custom-payload-dbi-open? (i/env-opts kv))))
      (is (nil? (d/get-value kv c/kv-info :custom-payload-dbi-open?)))
      (d/open-dbi kv "reversed" {:flags (conj c/default-dbi-flags :reversekey)})
      (is (thrown-with-msg?
           Exception #"ordinary byte ordering"
           (cv/put-value! kv {:dbi "reversed" :position :key}
                          type {:order 1} (byte-array [0]))))
      (is (thrown? Exception
                   (cv/put-value! kv {:dbi "missing" :position :key}
                                  type {:order 1} (byte-array [0]))))
      (is (= 1 (d/entries kv c/custom-values))))))

(deftest public-custom-keys
  (doseq [wal? [false true]]
    (let [kv (open-kv (str "public-keys-" wal?) {:wal? wal?})
          a {:rank 1 :name "a"}
          b {:rank 1 :name "b"}
          c {:rank 2 :name "c"}]
      (d/register-type kv :app/task (task-type 0))
      (d/open-dbi kv "tasks" {:key-type :app/task})
      (d/transact-kv kv "tasks" [[:put b "b"] [:put a "a"] [:put c "c"]])
      (is (= "a" (d/get-value kv "tasks" a)))
      (is (= [b "b"] (d/get-value kv "tasks" b :app/task :data false)))
      (is (= [[b "b"] [a "a"] [c "c"]] (vec (d/get-range kv "tasks" [:all]))))
      (is (= [[b "b"] [a "a"]] (vec (d/get-range kv "tasks" [:closed a a]))))
      (is (= [[c "c"]] (vec (d/get-range kv "tasks" [:greater-than a]))))
      (is (= 2 (d/key-range-count kv "tasks" [:closed a a])))
      (is (= [b a c] (vec (d/key-range kv "tasks" [:all]))))
      (is (= 1 (d/get-rank kv "tasks" a)))
      (is (= [a "a"] (d/get-by-rank kv "tasks" 1 :app/task :data false)))
      (d/transact-kv kv [[:put "tasks" a "new" :app/task :data]])
      (is (= 3 (d/entries kv c/custom-values)))
      (is (= "new" (d/get-value kv "tasks" a)))
      (is (thrown? Exception (d/get-value kv "tasks" a :long)))
      (is (thrown? Exception (d/open-dbi kv "tasks" {:key-type :app/missing})))
      (with-open [^java.lang.AutoCloseable values (d/range-seq kv "tasks" [:all-back] :app/task :data false
                                      {:batch-size 1})]
        (is (= [[c "c"] [a "new"] [b "b"]] (vec (seq values)))))
      (d/transact-kv kv "tasks" [[:del a]])
      (is (nil? (d/get-value kv "tasks" a)))
      (is (= "b" (d/get-value kv "tasks" b)))
      (is (= 2 (d/entries kv c/custom-values)))
      (d/close-kv kv)
      (let [kv (open-kv (str "public-keys-" wal?) {:wal? wal?})]
        (is (= :app/task (:key-type (i/dbi-opts kv "tasks"))))
        (is (= "b" (d/get-value kv "tasks" b)))
        (d/open-dbi kv "tasks")
        (is (= :app/task (:key-type (i/dbi-opts kv "tasks"))))))))

(deftest public-custom-items-and-custom-list-keys
  (doseq [custom-key? [false true]]
    (let [kv (open-kv (str "public-items-" custom-key?))
          a {:rank 1 :name "a"}
          b {:rank 1 :name "b"}
          z {:rank 2 :name "z"}
          key (if custom-key? {:rank 9 :name "key"} "key")
          kt (if custom-key? :app/task :string)]
      (d/register-type kv :app/task (task-type 0))
      (d/open-list-dbi kv "items" (cond-> {:value-type :app/task}
                                   custom-key? (assoc :key-type :app/task)))
      (d/put-list-items kv "items" key [a b z] kt :app/task)
      (is (= [a b z] (vec (d/get-list kv "items" key kt :app/task))))
      (is (d/in-list? kv "items" key b kt :app/task))
      (is (not (d/in-list? kv "items" key {:rank 1 :name "missing"} kt :app/task)))
      (is (= 3 (d/list-count kv "items" key kt)))
      (is (= [[key b] [key a]]
             (vec (d/list-range kv "items" [:all] kt [:closed-back a a] :app/task))))
      (d/put-list-items kv "items" key [a a] kt :app/task)
      (is (= 3 (d/list-count kv "items" key kt)))
      (d/del-list-items kv "items" key [b] kt :app/task)
      (is (= [a z] (vec (d/get-list kv "items" key kt :app/task))))
      (let [seen (atom [])]
        (d/visit-list kv "items" #(swap! seen conj %) key kt :app/task false)
        (is (= [a z] @seen)))
      (d/del-list-items kv "items" key kt)
      (is (= 0 (d/list-count kv "items" key kt)))
      (is (= 0 (d/entries kv c/custom-values))))))

(deftest public-custom-batches-and-callbacks
  (let [kv (open-kv "public-batches")
        a {:rank 1 :name "a"}
        b {:rank 1 :name "b"}]
    (d/register-type kv :app/task (task-type 0))
    (d/open-dbi kv "tasks" {:key-type :app/task})
    (d/open-dbi kv "plain")
    (is (thrown? Exception
                 (d/transact-kv kv [[:put "plain" :k :v]
                                    [:put "tasks" a 1]
                                    [:put "tasks" {:rank "bad"} 2]])))
    (is (nil? (d/get-value kv "plain" :k)))
    (is (zero? (d/entries kv c/custom-values)))
    (d/with-transaction-kv [tx kv]
      (d/transact-kv tx [[:put "tasks" a 1] [:put "tasks" b 2]])
      (is (= 2 (d/get-value tx "tasks" b))))
    (is (= [[b 2]] (vec (d/range-filter kv "tasks" (fn [k _] (= k b))
                                       [:all] :app/task :data false false))))
    (is (= [1 2] (vec (d/range-keep kv "tasks" (fn [_ v] v)
                                   [:all] :app/task :data false))))
    (is (= b (d/range-some kv "tasks" (fn [k v] (when (= v 2) k))
                           [:all] :app/task :data false)))
    (let [seen (atom [])]
      (d/visit kv "tasks" (fn [k _] (swap! seen conj k) :datalevin/terminate-visit)
               [:all] :app/task :data false)
      (is (= [a] @seen)))))

(defn- dump-reader [s]
  (java.io.PushbackReader. (java.io.StringReader. s)))

(deftest public-custom-clear-drop-and-abort
  (doseq [wal? [false true]]
    (let [kv (open-kv (str "public-cleanup-" wal?) {:wal? wal?})
          a {:rank 1 :name "a"} b {:rank 1 :name "b"}]
      (d/register-type kv :app/task (task-type 0))
      (d/open-dbi kv "keys" {:key-type :app/task})
      (d/open-list-dbi kv "items" {:key-type :app/task :value-type :app/task})
      (d/transact-kv kv "keys" [[:put a 1] [:put b 2]])
      (d/put-list-items kv "items" a [a b] :app/task :app/task)
      (is (= 5 (d/entries kv c/custom-values)))
      (is (thrown? Exception
                   (d/with-transaction-kv [tx kv]
                     (d/clear-dbi tx "items")
                     (is (empty? (d/get-list tx "items" a :app/task :app/task)))
                     (throw (ex-info "abort" {})))))
      (is (= [a b] (vec (d/get-list kv "items" a :app/task :app/task))))
      (d/clear-dbi kv "items")
      (is (= 2 (d/entries kv c/custom-values)))
      (d/drop-dbi kv "items")
      (is (not (contains? (set (d/list-dbis kv)) "items")))
      (is (thrown? Exception (d/drop-dbi kv c/custom-values)))
      (is (thrown? Exception (d/clear-dbi kv c/kv-info)))
      (d/drop-dbi kv "keys")
      (is (zero? (d/entries kv c/custom-values))))))

(deftest custom-dump-dependencies-and-roundtrip
  (let [kv (open-kv "dump-source")
        a {:rank 1 :name "a"} b {:rank 1 :name "b"}]
    (d/register-type kv :app/task (task-type 0))
    (d/open-dbi kv "tasks" {:key-type :app/task})
    (d/open-list-dbi kv "items" {:value-type :app/task})
    (d/transact-kv kv "tasks" [[:put a 1] [:put b 2]])
    (d/put-list-items kv "items" :list [a b] :data :app/task)
    (doseq [single? [false true] binary? [false true]]
      (let [name (str "restored-" single? "-" binary?)
            dest (open-kv name)
            bytes (java.io.ByteArrayOutputStream.)
            text (if binary?
                   (with-open [out (java.io.DataOutputStream. bytes)]
                     (if single? (l/dump-dbi kv "tasks" out) (l/dump-all kv out)))
                   (with-out-str (if single? (l/dump-dbi kv "tasks") (l/dump-all kv))))
            input #(if binary?
                     (java.io.DataInputStream. (java.io.ByteArrayInputStream. (.toByteArray bytes)))
                     (dump-reader text))
            restore #(if single? (l/load-dbi dest "tasks" (input) binary?)
                         (l/load-all dest (input) binary?))]
        (restore)
        (is (= 1 (d/get-value dest "tasks" a)))
        (is (= 2 (d/get-value dest "tasks" b)))
        (is (= (if single? 2 4) (d/entries dest c/custom-values)))
        (is (= (not single?) (contains? (set (d/list-dbis dest)) "items")))
        (when-not single?
          (is (= [a b] (vec (d/get-list dest "items" :list :data :app/task)))))
        ;; An identical restore is safe and does not allocate replacement IDs.
        (restore)
        (d/transact-kv dest "tasks" [[:put {:rank 9} 9]])
        (is (= 5 (d/get-value dest c/kv-info cv/id-key :keyword)))
        (d/close-kv dest)
        (let [dest (open-kv name)]
          (is (= 1 (d/get-value dest "tasks" a))))))
    (let [bundle (custom-dump/capture kv "tasks")]
      (is (= [c/kv-info c/custom-values "tasks"]
             (mapv (comp :dbi first) (:sections bundle))))
      (is (= 2 (count (second (second (:sections bundle)))))))
    (d/copy kv (str *dir* "/public-copy"))
    (let [copy (open-kv "public-copy")]
      (is (= 2 (d/get-value copy "tasks" b))))))

(deftest custom-restore-preflights-conflicts-and-dependencies
  (let [source (open-kv "conflict-source")
        dest (open-kv "conflict-dest")
        a {:rank 1 :name "a"}]
    (doseq [kv [source dest]] (d/register-type kv :app/task (task-type 0)))
    (d/open-dbi source "tasks" {:key-type :app/task})
    (d/transact-kv source "tasks" [[:put a 1]])
    (d/open-dbi dest "unrelated" {:key-type :app/task})
    (d/transact-kv dest "unrelated" [[:put a 2]])
    (let [bundle (custom-dump/capture source "tasks")
          before (set (d/list-dbis dest))]
      ;; Identical bytes with ID 1 still belong to a different entry.
      (is (thrown-with-msg? Exception #"Conflicting custom payload ID"
                           (custom-dump/restore! dest bundle "tasks")))
      (is (= before (set (d/list-dbis dest))))
      (is (= 2 (d/get-value dest "unrelated" a)))
      (is (= 1 (d/entries dest c/custom-values)))
      (let [empty-dest (open-kv "missing-dependency")
            missing (-> bundle (assoc-in [:sections 1 1] [])
                        (assoc-in [:sections 1 0 :entries] 0))]
        (is (thrown-with-msg? Exception #"Missing referenced"
                             (custom-dump/restore! empty-dest missing "tasks")))
        (is (empty? (d/list-dbis empty-dest)))
        (is (empty? (:types (custom/registry empty-dest)))))
      (let [different (open-kv "different-definition")]
        (d/register-type different :app/task (task-type 1))
        (is (thrown-with-msg? Exception #"Conflicting custom type"
                             (custom-dump/restore! different bundle "tasks")))
        (is (empty? (d/list-dbis different)))))))

(deftest custom-lazy-reader-retains-its-cursor-and-snapshot
  (let [kv (open-kv "lazy-cursor")
        keys (mapv #(hash-map :rank %) (range 10))]
    (d/register-type kv :app/task (task-type 0))
    (d/open-dbi kv "tasks" {:key-type :app/task})
    (d/transact-kv kv "tasks" (mapv #(vector :put % (:rank %)) keys))
    (with-open [^java.lang.AutoCloseable rows (d/range-seq kv "tasks" [:all] :app/task
                                                         :data false {:batch-size 1})]
      (let [s (seq rows)]
        (is (= [(first keys) 0] (first s)))
        (is (= 10 (count (d/get-range kv "tasks" [:all]))))
        (d/transact-kv kv "tasks" [[:del (last keys)]])
        (is (= (mapv #(vector % (:rank %)) keys) (vec s)))))
    (is (= 9 (d/entries kv "tasks")))))

(deftest custom-kv-wal-replays-physical-rows
  (let [source (open-kv "replay-source" {:wal? true})
        dest (open-kv "replay-dest")
        a {:rank 1 :name "a"} b {:rank 1 :name "b"}]
    (d/register-type source :app/task (task-type 0))
    (d/open-dbi source "tasks" {:key-type :app/task})
    (d/open-list-dbi source "items" {:value-type :app/task})
    (d/transact-kv source "tasks" [[:put a 1] [:put b 2]])
    (d/put-list-items source "items" :list [a b] :data :app/task)
    (d/del-list-items source "items" :list [a] :data :app/task)
    (doseq [{:keys [rows lsn]} (kv/open-tx-log-rows source 0)]
      (kv/replay-txlog-rows! dest rows lsn))
    (is (= 1 (d/get-value dest "tasks" a)))
    (is (= 2 (d/get-value dest "tasks" b)))
    (is (= [b] (vec (d/get-list dest "items" :list :data :app/task))))
    (is (= 3 (d/entries dest c/custom-values)))
    (let [last-lsn (apply max (map :lsn (kv/open-tx-log-rows source 0)))]
      (d/clear-dbi source "items")
      (d/drop-dbi source "tasks")
      (doseq [{:keys [rows lsn]} (kv/open-tx-log-rows source (inc (long last-lsn)))]
        (kv/replay-txlog-rows! dest rows lsn))
      (is (zero? (d/entries dest c/custom-values)))
      (is (zero? (d/list-count dest "items" :list :data)))
      (is (not (contains? (set (d/list-dbis dest)) "tasks"))))))

(deftest custom-kv-counts-dumps-and-cleanup-do-not-need-user-functions
  (let [runtime (udf/create-registry)
        source (open-kv "no-callback-source" {:runtime-opts {:udf-registry runtime}})
        dest (open-kv "no-callback-dest")]
    (udf/register! runtime (udf-desc :order-fn) :rank)
    (udf/register! runtime (udf-desc :serializer) b/serialize)
    (udf/register! runtime (udf-desc :deserializer) b/deserialize)
    (d/register-type source :app/task
                     {:index {:type :long :order-fn (udf-desc :order-fn)}
                      :payload {:serialize (udf-desc :serializer)
                                :deserialize (udf-desc :deserializer)}})
    (d/open-dbi source "tasks" {:key-type :app/task})
    (d/transact-kv source "tasks" [[:put {:rank 1} "one"]])
    (doseq [kind [:order-fn :serializer :deserializer]]
      (udf/unregister! runtime (udf-desc kind)))
    (is (= 1 (d/range-count source "tasks" [:all])))
    (l/load-all dest (dump-reader (with-out-str (l/dump-all source))))
    (is (= 1 (d/key-range-count dest "tasks" [:all])))
    (is (= ["one"] (vec (d/get-range dest "tasks" [:all] :app/task :data true))))
    (d/clear-dbi dest "tasks")
    (is (zero? (d/entries dest c/custom-values)))))

(deftest custom-kv-validation-flags-and-false-results
  (let [kv (open-kv "public-validation")
        a {:rank 1}]
    (d/register-type kv :app/task (task-type 0))
    (d/open-dbi kv "tasks" {:key-type :app/task :validate-data? true})
    (d/open-dbi kv "plain")
    (d/transact-kv kv "tasks" [[:put a false]] :app/task :boolean)
    (is (= false (d/get-some kv "tasks" (fn [_ _] true) [:all]
                            :app/task :boolean true false)))
    (is (= [false] (vec (d/range-filter kv "tasks" (fn [_ _] true) [:all]
                                       :app/task :boolean true false))))
    (is (= [false] (vec (d/range-keep kv "tasks" (fn [_ _] false) [:all]
                                     :app/task :boolean false))))
    (doseq [row [[:put "tasks" a nil]
                 [:put "tasks" nil 1]
                 [:put "tasks" a "bad" :app/task :long]
                 [:put "tasks" a true :app/task :boolean #{:nooverwrite}]
                 [:put "plain" a 1 :app/task :long]]]
      (is (thrown? Exception (d/transact-kv kv [row]))))
    (is (= false (d/get-value kv "tasks" a :app/task :boolean)))
    (is (= 1 (d/entries kv c/custom-values)))
    (is (thrown? Exception (d/open-dbi kv "tasks" {:key-size 128})))
    (is (thrown? Exception (d/open-list-dbi kv "tasks")))
    (d/open-list-dbi kv "items" {:value-type :app/task})
    (d/put-list-items kv "items" :list [a] :data :app/task)
    (is (thrown? Exception
                 (d/transact-kv kv [[:put "items" :list a :data :app/task #{:nodupdata}]])))
    (is (= [a] (vec (d/get-list kv "items" :list :data :app/task))))))
