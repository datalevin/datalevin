(ns datalevin.interop-bridge-test
  (:require
   [clojure.test :refer [deftest is]]
   [datalevin.core :as d]
   [datalevin.util :as u])
  (:import
   [datalevin Datalevin DatalevinInterop]
   [java.util ArrayList List UUID]))

(def query-text
  "[:find ?name ?age :where [?e :name ?name] [?e :age ?age]]")

(def expected-rows
  #{["Ada" 42] ["Bob" 21]})

(deftest bridge-builds-edn-lists
  (let [items (doto (ArrayList.)
                (.add (DatalevinInterop/symbol ">="))
                (.add (List/of "profile" "age"))
                (.add 30))
        form  (DatalevinInterop/ednList items)]
    (is (list? form))
    (is (= '(>= ["profile" "age"] 30) form))))

(deftest anonymous-connection-forwards-schema-and-options
  (let [conn (DatalevinInterop/createConnection
               nil
               {:name {:db/valueType :db.type/string}}
               {:kv-opts {:inmemory? true}})]
    (try
      (is (= :db.type/string
             (get-in (d/schema conn) [:name :db/valueType])))
      (is (true? (get-in (d/opts conn) [:kv-opts :inmemory?])))
      (finally
        (d/close conn)))))

(deftest unordered-relation-results-are-bridge-safe-lists
  (let [dir  (u/tmp-dir (str "interop-query-" (UUID/randomUUID)))
        conn (Datalevin/createConn dir)]
    (try
      (.transact conn
                 [{:db/id 1 :name "Ada" :age 42}
                  {:db/id 2 :name "Bob" :age 21}])
      (let [connection-result
            (DatalevinInterop/connectionQueryBridge
              conn query-text (ArrayList.))
            core-result
            (DatalevinInterop/coreInvokeBridge
              "q" [(read-string query-text)
                   (DatalevinInterop/connectionDb conn)])]
        (is (instance? List connection-result))
        (is (= expected-rows (set (map vec connection-result))))
        (is (instance? List core-result))
        (is (= expected-rows (set (map vec core-result)))))
      (finally
        (.close conn)
        (u/delete-files dir)))))
