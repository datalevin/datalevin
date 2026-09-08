(ns datalevin.custom-data-test
  (:require
   [clojure.test :refer [deftest is use-fixtures]]
   [datalevin.bits :as b]
   [datalevin.constants :as c]
   [datalevin.core :as d]
   [datalevin.custom-data :as custom]
   [datalevin.interface :as i]
   [datalevin.interpret :as inter]
   [datalevin.lmdb :as l]
   [datalevin.udf :as udf]
   [datalevin.util :as u])
  (:import
   [java.util UUID]
   [java.util.concurrent TimeUnit]))

(def ^:dynamic *dir* nil)
(def ^:dynamic *handles* nil)

(use-fixtures
  :each
  (fn [f]
    (binding [*dir* (u/tmp-dir (str "custom-types-" (UUID/randomUUID)))
              *handles* (atom [])
              c/*db-background-sampling?* false]
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
