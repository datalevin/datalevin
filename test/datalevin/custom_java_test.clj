(ns datalevin.custom-java-test
  (:require [clojure.test :refer [deftest is use-fixtures]]
            [datalevin.bits :as b]
            [datalevin.constants :as c]
            [datalevin.util :as u])
  (:import [datalevin Connection Datalevin DatalevinInterop KV KVType Schema
            UdfDescriptor UdfFunction]
           [java.time LocalDate]
           [java.util List Map UUID]))

(def ^:dynamic *dir* nil)

(use-fixtures :each
  (fn [f]
    (binding [*dir* (u/tmp-dir (str "custom-java-" (UUID/randomUUID)))
              c/*db-background-sampling?* false]
      (try (f) (finally (u/delete-files *dir*))))))

(defn- jmap ^Map [& kvs]
  (Datalevin/mapOf (into-array Object kvs)))

(defn- rows [^KV kv ^String dbi ^List key-range]
  (.getRange kv dbi key-range))

(defn- unary [f]
  (reify UdfFunction
    (invoke [_ args] (f (.get ^List args 0)))))

(deftest java-custom-kv-types
  (doseq [tuple? [false true]]
    (let [registry (Datalevin/udfRegistry)
          _ (.orderFn registry "task/order"
                       (unary #(if tuple? [(get % "rank") "task"] (get % "rank"))))
          _ (.serializer registry "task/encode" (unary b/serialize))
          _ (.deserializer registry "task/decode" (unary b/deserialize))
          definition (jmap "index" (jmap "type" (if tuple?
                                                   (KVType/tuple (into-array KVType [KVType/LONG KVType/STRING]))
                                                   KVType/LONG)
                                         "order-fn" (UdfDescriptor/orderFn "task/order"))
                           "payload" (jmap "serialize" (UdfDescriptor/serializer "task/encode")
                                           "deserialize" (UdfDescriptor/deserializer "task/decode")))
          opts (jmap "runtime-opts" (jmap "udf-registry" registry))
          a (jmap "rank" 1 "label" "a")
          b (jmap "rank" 1 "label" "b")
          path (str *dir* "/" tuple?)]
      (with-open [^KV kv (Datalevin/openKV path opts)]
        (is (= :app/task (.registerType kv "app/task" definition)))
        (is (= :app/task (DatalevinInterop/registerType kv ":app/task" definition)))
        (.openDbi kv "tasks" (jmap "key-type" ":app/task"))
        (.transact kv "tasks" [[":put" a "a"] [":put" b "b"]])
        (is (= "a" (.getValue kv "tasks" a)))
        (is (= "b" (.getValue kv "tasks" b (KVType/of "app/task") KVType/DATA true)))
        (is (= [[a "a"] [b "b"]] (rows kv "tasks" [":closed" a b])))
        (with-open [tx (.beginTransaction kv)]
          (.registerType tx "app/aborted" definition)
          (.transact tx "tasks" [[":del" b]]))
        (is (= "b" (.getValue kv "tasks" b)))
        (is (= :app/aborted
               (.registerType kv "app/aborted" (assoc (into {} definition) "version" 2)))))
      (with-open [^KV kv (Datalevin/openKV path opts)]
        (.openDbi kv "tasks")
        (is (= [[a "a"] [b "b"]] (rows kv "tasks" [":all"])))))))

(deftest java-custom-datalog-types
  (let [registry (Datalevin/udfRegistry)
        _ (.orderFn registry "task/order" (unary #(get % "rank")))
        definition (jmap "index" (jmap "type" ":long"
                                       "order-fn" (UdfDescriptor/orderFn "task/order")))
        a (jmap "rank" 1 "label" "a")
        b (jmap "rank" 1 "label" "b")]
    (with-open [^Connection conn (Datalevin/createConn (str *dir*) (jmap)
                                  (jmap "runtime-opts" (jmap "udf-registry" registry)))]
      (is (= :app/task (.registerType conn "app/task" definition)))
      (.updateSchema conn (.attr (Datalevin/schema) "task/value"
                                 (.valueType (Schema/attribute) "app/task")))
      (.transact conn [(jmap "db/id" 1 "task/value" a)
                       (jmap "db/id" 2 "task/value" b)])
      (is (= #{[1]} (set (.query conn "[:find ?e :in $ ?v :where [?e :task/value ?v]]"
                                 ^objects (into-array Object [a])))))
      (is (= b (get (.pull conn (cast Object "[*]") 2) :task/value)))
      (with-open [^KV kv (Datalevin/datalogKV conn)]
        (is (= :app/task (.registerType kv "app/task" definition)))
        (.openDbi kv "tasks" (jmap "key-type" ":app/task"))
        (.transact kv "tasks" [[":put" a "a"]])
        (is (= "a" (.getValue kv "tasks" a)))))))

(deftest java-custom-kv-native-values
  (let [registry (Datalevin/udfRegistry)
        _ (.orderFn registry "date/year" (unary #(long (.getYear ^LocalDate %))))
        _ (.serializer registry "date/encode"
                       (unary #(.getBytes (str %) java.nio.charset.StandardCharsets/UTF_8)))
        _ (.deserializer registry "date/decode"
                         (unary #(LocalDate/parse (String. ^bytes % java.nio.charset.StandardCharsets/UTF_8))))
        definition (jmap "index" (jmap "type" ":long" "order-fn" (UdfDescriptor/orderFn "date/year"))
                         "payload" (jmap "serialize" (UdfDescriptor/serializer "date/encode")
                                         "deserialize" (UdfDescriptor/deserializer "date/decode")))
        a (LocalDate/of 2024 1 1)
        b (LocalDate/of 2024 2 1)]
    (with-open [^KV kv (Datalevin/openKV *dir* (jmap "runtime-opts" (jmap "udf-registry" registry)))]
      (.registerType kv "app/date" definition)
      (.openDbi kv "dates" (jmap "key-type" ":app/date"))
      (.transact kv "dates" [[":put" a "a"] [":put" b "b"]])
      (is (= "a" (.getValue kv "dates" (LocalDate/of 2024 1 1))))
      (is (= [[a "a"] [b "b"]] (rows kv "dates" [":all"])))
      (.transact kv "dates" [[":del" a]])
      (is (= [[b "b"]] (rows kv "dates" [":all"]))))))
