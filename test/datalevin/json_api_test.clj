;;
;; Copyright (c) Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns datalevin.json-api-test
  (:require
   [clojure.test :refer [deftest is use-fixtures]]
   [datalevin.client :as dc]
   [datalevin.constants :as c]
   [datalevin.json-api :as sut]
   [datalevin.json-api.shared :as shared]
   [datalevin.json-convert :as jc]
   [datalevin.test.core :as tdc]
   [datalevin.util :as u])
  (:import
   [java.math BigDecimal BigInteger]
   [java.time Instant]
   [java.util Date UUID]))

(defn- clean-json-api-state
  [f]
  (#'sut/clear-handles!)
  (try
    (f)
    (finally
      (#'sut/clear-handles!))))

(use-fixtures :each clean-json-api-state)

(defn- parse-response
  [s]
  (jc/read-json-string s))

(defn- exec-request
  [request]
  (parse-response (sut/exec (jc/write-json-string request))))

(defn- ok-result
  [request]
  (let [response (exec-request request)]
    (is (= true (get response "ok")) (pr-str response))
    (get response "result")))

(deftest json-convert-round-trip-test
  (let [uuid  (UUID/randomUUID)
        inst  (Date/from (Instant/parse "2024-01-15T00:00:00Z"))
        bytes (.getBytes "hello" "UTF-8")
        value {"name"          "Alice"
               :user/id        42
               "safe"          12
               "unsafe"        9007199254740993
               "set"           #{1 2 3}
               "date"          inst
               "uuid"          uuid
               "bytes"         bytes
               "bigdec"        (BigDecimal. "123.456")
               "bigint"        (BigInteger. "999999999999999999999")
               "reserved-key"  {"~uuid" "literal"}}
        json  (jc/write-json-string value)
        rt    (jc/read-json-string json)]
    (is (= "Alice" (get rt "name")))
    (is (= 42 (get rt :user/id)))
    (is (= 12 (get rt "safe")))
    (is (= 9007199254740993 (get rt "unsafe")))
    (is (= #{1 2 3} (get rt "set")))
    (is (= inst (get rt "date")))
    (is (= uuid (get rt "uuid")))
    (is (= (seq bytes) (seq (get rt "bytes"))))
    (is (= (BigDecimal. "123.456") (get rt "bigdec")))
    (is (= (BigInteger. "999999999999999999999") (get rt "bigint")))
    (is (= {"~uuid" "literal"} (get rt "reserved-key")))
    (is (= ":literal" (jc/read-json-string "{\"~str\":\":literal\"}")))
    (let [handle-ref (jc/read-json-string "{\"~handle\":\"conn-1\"}")
          pull-form  (jc/read-json-string
                       "{\"~pull\":{\"attr\":{\":attr\":\":friends\",\":limit\":1},\"pattern\":[\":name\"]}}")]
      (is (jc/handle-ref? handle-ref))
      (is (= "conn-1" (jc/handle-ref-value handle-ref)))
      (is (jc/pull-form? pull-form))
      (is (= {"attr" {:attr :friends
                      :limit 1}
              "pattern" [:name]}
             (jc/pull-form-value pull-form)))
      (is (= [:name]
             (get-in (jc/read-json-string (jc/write-json-string pull-form))
                     [:datalevin.json-convert/value "pattern"]))))))

(deftest handle-rebinding-test
  (let [obj1    (Object.)
        obj2    (Object.)
        handle  (#'sut/register! "search" :search obj1)]
    (is (= handle (#'sut/register! "search" :search obj1)))
    (is (identical? obj1 (#'sut/resolve-handle handle)))
    (is (= handle (#'sut/rebind! handle :search obj2)))
    (is (identical? obj2 (#'sut/resolve-handle handle)))
    (is (= {:type :search :obj obj2}
           (#'sut/resolve-entry handle)))))

(deftest exec-request-session-isolation-test
  (let [dir      (u/tmp-dir (str "json-api-session-" (UUID/randomUUID)))
        session1 (shared/new-session-state)
        session2 (shared/new-session-state)]
    (try
      (let [handle (get-in (shared/exec-request
                             session1
                             {"op" "get-conn"
                              "args" {"dir" dir}})
                           ["result"])]
        (is (string? handle))
        (let [error (try
                      (shared/exec-request session2
                                           {"op" "closed?"
                                            "args" {"conn" handle}})
                      nil
                      (catch clojure.lang.ExceptionInfo e
                        e))]
          (is (instance? clojure.lang.ExceptionInfo error))
          (is (= :invalid-handle (:code (ex-data error))))))
      (finally
        (shared/clear-handles! session1)
        (shared/clear-handles! session2)
        (u/delete-files dir)))))

(deftest exec-api-info-test
  (let [response (parse-response
                   (sut/exec "{\"op\":\"api-info\",\"args\":{}}"))]
    (is (= true (get response "ok")))
    (is (= 1 (get-in response ["result" "json-api-version"])))
    (is (= "0.10.7" (get-in response ["result" "datalevin-version"])))
    (is (= 10000 (get-in response ["result" "limits" "max-result-items"])))
    (is (= 8388608
           (get-in response ["result" "limits" "max-response-bytes"])))))

(deftest utility-ops-test
  (let [uuid-str (ok-result {"op" "squuid" "args" {}})
        millis   (ok-result {"op" "squuid-time-millis"
                             "args" {"uuid" uuid-str}})
        hex      (ok-result {"op" "hexify-string"
                             "args" {"s" "datalevin"}})
        plain    (ok-result {"op" "unhexify-string"
                             "args" {"s" hex}})]
    (is (instance? UUID (UUID/fromString uuid-str)))
    (is (integer? millis))
    (is (= "646174616C6576696E" hex))
    (is (= "datalevin" plain))))

(deftest exec-canonical-get-conn-test
  (let [dir   (u/tmp-dir (str "json-api-get-conn-" (UUID/randomUUID)))
        req   (fn [op & [args]]
                (sut/exec (jc/write-json-string {"op" op
                                                "args" (or args {})})))
        resp1 (parse-response (req "get-conn" {"dir" dir}))
        h1    (get-in resp1 ["result"])
        resp2 (parse-response (req "get-conn" {"dir" dir}))
        h2    (get-in resp2 ["result"])]
    (is (= h1 h2))
    (is (= true (get (parse-response (req "close" {"conn" h1})) "ok")))
    (is (= false
           (get (parse-response (req "closed?" {"conn" h1})) "ok")))
    (u/delete-files dir)))

(deftest oversized-response-test
  (binding [sut/*json-api-limits* {:max-result-items   10000
                                   :max-response-bytes 160}]
      (let [response (parse-response
                     (sut/exec "{\"op\":\"api-info\",\"args\":{}}"))]
      (is (= false (get response "ok")))
      (is (= :result-too-large
             (get-in response ["data" :code]))))))

(deftest datalog-core-ops-test
  (let [dir1       (u/tmp-dir (str "json-api-datalog-1-" (UUID/randomUUID)))
        dir2       (u/tmp-dir (str "json-api-datalog-2-" (UUID/randomUUID)))
        schema     {:name    {:db/valueType :db.type/string
                              :db/unique    :db.unique/identity}
                    :age     {:db/valueType :db.type/long}
                    :bio     {:db/valueType :db.type/string
                              :db/fulltext  true}
                    :counter {:db/valueType :db.type/long}
                    :friends {:db/valueType   :db.type/ref
                              :db/cardinality :db.cardinality/many}}]
    (try
      (let [conn1     (ok-result {"op" "create-conn"
                                  "args" {"dir" dir1
                                          "schema" schema}})
            conn2     (ok-result {"op" "create-conn"
                                  "args" {"dir" dir2
                                          "schema" schema}})
            report1   (ok-result
                        {"op" "transact!"
                         "args" {"conn" conn1
                                 "tx-data"
                                 [{:db/id   -1
                                   :name    "Alice"
                                   :age     30
                                   :bio     "Alice enjoys pizza"
                                   :counter 0}
                                  {:db/id   -2
                                   :name    "Bob"
                                   :age     25
                                   :bio     "Bob likes pie"
                                   :counter 0
                                   :friends [-1 -3]}
                                  {:db/id -3
                                   :name  "Carol"
                                   :age   27
                                   :bio   "Carol likes pizza"}]}})
            alice-id  (get-in report1 ["tempids" "-1"])
            bob-id    (get-in report1 ["tempids" "-2"])
            carol-id  (get-in report1 ["tempids" "-3"])
            _report2  (ok-result
                        {"op" "transact!"
                         "args" {"conn" conn2
                                 "tx-data"
                                 [{:db/id -10 :name "Alice"}
                                  {:db/id -11 :name "Zed"}]}})
            schema*   (ok-result {"op" "schema"
                                  "args" {"conn" conn1}})
            opts*     (ok-result {"op" "opts"
                                  "args" {"conn" conn1}})
            max-eid   (ok-result {"op" "max-eid"
                                  "args" {"conn" conn1}})
            cache0    (ok-result {"op" "datalog-index-cache-limit"
                                  "args" {"conn" conn1}})
            cache1    (ok-result {"op" "datalog-index-cache-limit"
                                  "args" {"conn" conn1
                                          "limit" 128}})
            bob-id*   (ok-result {"op" "entid"
                                  "args" {"conn" conn1
                                          "eid" [:name "Bob"]}})
            entity*   (ok-result {"op" "entity"
                                  "args" {"conn" conn1
                                          "eid" bob-id}})
            pull*     (ok-result
                        {"op" "pull"
                         "args" {"conn" conn1
                                 "selector"
                                 [":name"
                                  (jc/pull-form
                                    {:attr    {:attr :friends
                                               :limit 1}
                                     :pattern [":name"]})]
                                 "eid" bob-id}})
            q*        (ok-result
                        {"op" "q"
                         "args" {"conn" conn1
                                 "query"
                                 "[:find ?n :where [?e :name ?n]]"}})
            q2*       (ok-result
                        {"op" "q"
                         "args" {"conn" conn1
                                 "query"
                                 "[:find ?n :in $ $2 :where [$ ?e :name ?n] [$2 ?e2 :name ?n]]"
                                 "inputs" [(jc/handle-ref conn2)]}})
            explain*  (ok-result
                        {"op" "explain"
                         "args" {"conn" conn1
                                 "opts" "{:run? true}"
                                 "query"
                                 "[:find ?n :where [?e :name ?n]]"}})
            datoms*   (ok-result
                        {"op" "datoms"
                         "args" {"conn" conn1
                                 "index" ":ave"
                                 "c1" ":name"
                                 "limit" 2
                                 "offset" 1}})
            search*   (ok-result
                        {"op" "search-datoms"
                         "args" {"conn" conn1
                                 "e" bob-id
                                 "a" ":name"}})
            count*    (ok-result
                        {"op" "count-datoms"
                         "args" {"conn" conn1
                                 "a" ":name"}})
            seek*     (ok-result
                        {"op" "seek-datoms"
                         "args" {"conn" conn1
                                 "index" ":ave"
                                 "c1" ":name"
                                 "c2" "Bob"
                                 "limit" 1}})
            rseek*    (ok-result
                        {"op" "rseek-datoms"
                         "args" {"conn" conn1
                                 "index" ":ave"
                                 "c1" ":name"
                                 "c2" "Carol"
                                 "limit" 1}})
            index*    (ok-result
                        {"op" "index-range"
                         "args" {"conn" conn1
                                 "attr" ":age"
                                 "start" 26
                                 "end" 30}})
            card*     (ok-result
                        {"op" "cardinality"
                         "args" {"conn" conn1
                                 "attr" ":name"}})
            analyze*  (ok-result
                        {"op" "analyze"
                         "args" {"conn" conn1}})
            fulltext* (ok-result
                        {"op" "fulltext-datoms"
                         "args" {"conn" conn1
                                 "query" "pizza"}})
            simulated (ok-result
                        {"op" "tx-data->simulated-report"
                         "args" {"conn" conn1
                                 "tx-data" [{:db/id bob-id
                                             :counter 7}]}})
            schema2   (ok-result
                        {"op" "update-schema"
                         "args" {"conn" conn1
                                 "schema-update"
                                 {:nickname {:db/valueType :db.type/string}}}})
            tx-result (ok-result
                        {"op" "with-transaction"
                         "args" {"conn" conn1
                                 "ops"
                                 [{"op" "transact!"
                                   "args" {"conn" conn1
                                           "tx-data" [{:db/id bob-id
                                                       :counter 1}]}}
                                  {"op" "q"
                                   "args" {"conn" conn1
                                           "query"
                                           "[:find ?c . :in $ ?e :where [?e :counter ?c]]"
                                           "inputs" [bob-id]}}]}})
            counter1  (ok-result
                        {"op" "q"
                         "args" {"conn" conn1
                                 "query"
                                 "[:find ?c . :in $ ?e :where [?e :counter ?c]]"
                                 "inputs" [bob-id]}})
            aborted   (ok-result
                        {"op" "with-transaction"
                         "args" {"conn" conn1
                                 "ops"
                                 [{"op" "transact!"
                                   "args" {"conn" conn1
                                           "tx-data" [{:db/id bob-id
                                                       :counter 2}]}}
                                  {"op" "abort-transact"
                                   "args" {}}]}})
            counter2  (ok-result
                        {"op" "q"
                         "args" {"conn" conn1
                                 "query"
                                 "[:find ?c . :in $ ?e :where [?e :counter ?c]]"
                                 "inputs" [bob-id]}})
            schema3   (ok-result
                        {"op" "update-schema"
                         "args" {"conn" conn1
                                 "del-attrs" ["nickname"]
                                 "rename-map" {"age" "years"}}})]
        (is (seq (get report1 "tx-data")))
        (is (integer? (get report1 "tx-id")))
        (is (= alice-id (get-in report1 ["tempids" "-1"])))
        (is (= :db.type/string (get-in schema* [:name :db/valueType])))
        (is (map? opts*))
        (is (<= 3 max-eid))
        (is (integer? cache0))
        (is (= 128 cache1))
        (is (= bob-id bob-id*))
        (is (= "Bob" (get entity* :name)))
        (is (= "Bob" (get pull* :name)))
        (is (= 1 (count (:friends pull*))))
        (is (= #{"Alice" "Bob" "Carol"}
               (set (map first q*))))
        (is (= #{["Alice"]} q2*))
        (is (map? explain*))
        (is (= ["Bob" "Carol"] (mapv #(get % "v") datoms*)))
        (is (= [bob-id] (mapv #(get % "e") search*)))
        (is (= 3 count*))
        (is (= ["Bob"] (mapv #(get % "v") seek*)))
        (is (= ["Carol"] (mapv #(get % "v") rseek*)))
        (is (= [27 30] (mapv #(get % "v") index*)))
        (is (= 3 card*))
        (is (= :done analyze*))
        (is (= #{[alice-id :bio "Alice enjoys pizza"]
                 [carol-id :bio "Carol likes pizza"]}
               (set fulltext*)))
        (is (seq (get simulated "tx-data")))
        (is (contains? schema2 :nickname))
        (is (contains? schema3 :years))
        (is (not (contains? schema3 :age)))
        (is (not (contains? schema3 :nickname)))
        (is (= 1 tx-result))
        (is (= 1 counter1))
        (is (nil? aborted))
        (is (= 1 counter2)))
      (finally
        (u/delete-files dir1)
        (u/delete-files dir2)))))

(deftest kv-core-ops-test
  (let [dir (u/tmp-dir (str "json-api-kv-" (UUID/randomUUID)))]
    (try
      (let [kv        (ok-result {"op" "open-kv"
                                  "args" {"dir" dir}})
            _open     (ok-result {"op" "open-dbi"
                                  "args" {"kv" kv
                                          "dbi-name" "items"}})
            tx-result (ok-result
                        {"op" "transact-kv"
                         "args" {"kv" kv
                                 "dbi-name" "items"
                                 "k-type" ":string"
                                 "v-type" ":string"
                                 "txs" [[:put "a" "alpha"]
                                        [:put "b" "beta"]
                                        [:put "c" "gamma"]]}})
            dir*      (ok-result {"op" "dir"
                                  "args" {"kv" kv}})
            dbis      (ok-result {"op" "list-dbis"
                                  "args" {"kv" kv}})
            stat*     (ok-result {"op" "stat"
                                  "args" {"kv" kv
                                          "dbi-name" "items"}})
            entries*  (ok-result {"op" "entries"
                                  "args" {"kv" kv
                                          "dbi-name" "items"}})
            value*    (ok-result {"op" "get-value"
                                  "args" {"kv" kv
                                          "dbi" "items"
                                          "k" "b"
                                          "k-type" ":string"
                                          "v-type" ":string"}})
            pair*     (ok-result {"op" "get-value"
                                  "args" {"kv" kv
                                          "dbi" "items"
                                          "k" "b"
                                          "k-type" ":string"
                                          "v-type" ":string"
                                          "ignore-key?" false}})
            rank*     (ok-result {"op" "get-rank"
                                  "args" {"kv" kv
                                          "dbi" "items"
                                          "k" "b"
                                          "k-type" ":string"}})
            by-rank-default*
                       (ok-result {"op" "get-by-rank"
                                   "args" {"kv" kv
                                           "dbi" "items"
                                           "rank" 2
                                           "k-type" ":string"
                                           "v-type" ":string"}})
            by-rank*  (ok-result {"op" "get-by-rank"
                                  "args" {"kv" kv
                                          "dbi" "items"
                                          "rank" 2
                                          "k-type" ":string"
                                          "v-type" ":string"
                                          "ignore-key?" true}})
            by-rank-pair*
                       (ok-result {"op" "get-by-rank"
                                   "args" {"kv" kv
                                           "dbi" "items"
                                           "rank" 2
                                           "k-type" ":string"
                                           "v-type" ":string"
                                           "ignore-key?" false}})
            sample*   (ok-result {"op" "sample-kv"
                                  "args" {"kv" kv
                                          "dbi" "items"
                                          "n" 2
                                          "k-type" ":string"
                                          "v-type" ":string"
                                          "ignore-key?" false}})
            first*    (ok-result {"op" "get-first"
                                  "args" {"kv" kv
                                          "dbi" "items"
                                          "k-range" [":all"]
                                          "k-type" ":string"
                                          "v-type" ":string"}})
            first-n*  (ok-result {"op" "get-first-n"
                                  "args" {"kv" kv
                                          "dbi" "items"
                                          "n" 2
                                          "k-range" [":all"]
                                          "k-type" ":string"
                                          "v-type" ":string"}})
            range*    (ok-result {"op" "get-range"
                                  "args" {"kv" kv
                                          "dbi" "items"
                                          "k-range" [":all"]
                                          "k-type" ":string"
                                          "v-type" ":string"
                                          "limit" 2
                                          "offset" 1}})
            keys*     (ok-result {"op" "key-range"
                                  "args" {"kv" kv
                                          "dbi" "items"
                                          "k-range" [":all"]
                                          "k-type" ":string"
                                          "limit" 2
                                          "offset" 1}})
            key-cnt*  (ok-result {"op" "key-range-count"
                                  "args" {"kv" kv
                                          "dbi" "items"
                                          "k-range" [":all"]
                                          "k-type" ":string"}})
            range-cnt* (ok-result {"op" "range-count"
                                   "args" {"kv" kv
                                           "dbi" "items"
                                           "k-range" [":all"]
                                           "k-type" ":string"}})
            flags*    (ok-result {"op" "get-env-flags"
                                  "args" {"kv" kv}})
            sync*     (ok-result {"op" "sync"
                                  "args" {"kv" kv}})
            _clear    (ok-result {"op" "clear-dbi"
                                  "args" {"kv" kv
                                          "dbi-name" "items"}})
            entries0  (ok-result {"op" "entries"
                                  "args" {"kv" kv
                                          "dbi-name" "items"}})
            _drop     (ok-result {"op" "drop-dbi"
                                  "args" {"kv" kv
                                          "dbi-name" "items"}})
            dbis2     (ok-result {"op" "list-dbis"
                                  "args" {"kv" kv}})
            closed?   (ok-result {"op" "closed-kv?"
                                  "args" {"kv" kv}})
            _close    (ok-result {"op" "close-kv"
                                  "args" {"kv" kv}})]
        (is (= :transacted tx-result))
        (is (= dir dir*))
        (is (contains? (set dbis) "items"))
        (is (= 3 (:entries stat*)))
        (is (= 3 entries*))
        (is (= "beta" value*))
        (is (= ["b" "beta"] pair*))
        (is (= 1 rank*))
        (is (= ["c" "gamma"] by-rank-default*))
        (is (= "gamma" by-rank*))
        (is (= ["c" "gamma"] by-rank-pair*))
        (is (= 2 (count sample*)))
        (is (every? #{["a" "alpha"] ["b" "beta"] ["c" "gamma"]} sample*))
        (is (= ["a" "alpha"] first*))
        (is (= [["a" "alpha"] ["b" "beta"]] first-n*))
        (is (= [["b" "beta"] ["c" "gamma"]] range*))
        (is (= ["b" "c"] keys*))
        (is (= 3 key-cnt*))
        (is (= 3 range-cnt*))
        (is (set? flags*))
        (is (= true sync*))
        (is (= 0 entries0))
        (is (not (contains? (set dbis2) "items")))
        (is (= false closed?)))
      (finally
        (u/delete-files dir)))))

(deftest kv-transaction-list-and-reindex-ops-test
  (let [dir (u/tmp-dir (str "json-api-kv-advanced-" (UUID/randomUUID)))]
    (try
      (let [kv          (ok-result {"op" "open-kv"
                                    "args" {"dir" dir}})
            _open       (ok-result {"op" "open-dbi"
                                    "args" {"kv" kv
                                            "dbi-name" "items"}})
            committed   (ok-result
                          {"op" "with-transaction-kv"
                           "args" {"kv" kv
                                   "ops"
                                   [{"op" "transact-kv"
                                     "args" {"kv" kv
                                             "dbi-name" "items"
                                             "k-type" ":string"
                                             "v-type" ":long"
                                             "txs" [[:put "counter" 0]]}}
                                    {"op" "with-transaction-kv"
                                     "args" {"kv" kv
                                             "ops"
                                             [{"op" "transact-kv"
                                               "args" {"kv" kv
                                                       "dbi-name" "items"
                                                       "k-type" ":string"
                                                       "v-type" ":long"
                                                       "txs" [[:put "counter" 1]]}}
                                              {"op" "get-value"
                                               "args" {"kv" kv
                                                       "dbi" "items"
                                                       "k" "counter"
                                                       "k-type" ":string"
                                                       "v-type" ":long"}}]}}
                                    {"op" "get-value"
                                     "args" {"kv" kv
                                             "dbi" "items"
                                             "k" "counter"
                                             "k-type" ":string"
                                             "v-type" ":long"}}]}})
            aborted     (ok-result
                          {"op" "with-transaction-kv"
                           "args" {"kv" kv
                                   "ops"
                                   [{"op" "transact-kv"
                                     "args" {"kv" kv
                                             "dbi-name" "items"
                                             "k-type" ":string"
                                             "v-type" ":long"
                                             "txs" [[:put "temp" 7]]}}
                                    {"op" "abort-transact-kv"
                                     "args" {}}]}})
            after-temp  (ok-result {"op" "get-value"
                                    "args" {"kv" kv
                                            "dbi" "items"
                                            "k" "temp"
                                            "k-type" ":string"
                                            "v-type" ":long"}})
            _open-list  (ok-result {"op" "open-list-dbi"
                                    "args" {"kv" kv
                                            "list-name" "list"}})
            _put-a      (ok-result {"op" "put-list-items"
                                    "args" {"kv" kv
                                            "list-name" "list"
                                            "k" "a"
                                            "vs" [1 2 3 4]
                                            "k-type" ":string"
                                            "v-type" ":long"}})
            _put-b      (ok-result {"op" "put-list-items"
                                    "args" {"kv" kv
                                            "list-name" "list"
                                            "k" "b"
                                            "vs" [5 6 7]
                                            "k-type" ":string"
                                            "v-type" ":long"}})
            _put-c      (ok-result {"op" "put-list-items"
                                    "args" {"kv" kv
                                            "list-name" "list"
                                            "k" "c"
                                            "vs" [3 6 9]
                                            "k-type" ":string"
                                            "v-type" ":long"}})
            list-b      (ok-result {"op" "get-list"
                                    "args" {"kv" kv
                                            "list-name" "list"
                                            "k" "b"
                                            "k-type" ":string"
                                            "v-type" ":long"
                                            "limit" 1
                                            "offset" 1}})
            count-a     (ok-result {"op" "list-count"
                                    "args" {"kv" kv
                                            "list-name" "list"
                                            "k" "a"
                                            "k-type" ":string"}})
            in-list?    (ok-result {"op" "in-list?"
                                    "args" {"kv" kv
                                            "list-name" "list"
                                            "k" "b"
                                            "v" 6
                                            "k-type" ":string"
                                            "v-type" ":long"}})
            range*      (ok-result {"op" "list-range"
                                    "args" {"kv" kv
                                            "list-name" "list"
                                            "k-range" [":closed" "a" "c"]
                                            "k-type" ":string"
                                            "v-range" [":closed" 2 4]
                                            "v-type" ":long"
                                            "limit" 2
                                            "offset" 1}})
            range-cnt   (ok-result {"op" "list-range-count"
                                    "args" {"kv" kv
                                            "list-name" "list"
                                            "k-range" [":closed" "a" "c"]
                                            "k-type" ":string"}})
            first*      (ok-result {"op" "list-range-first"
                                    "args" {"kv" kv
                                            "list-name" "list"
                                            "k-range" [":closed" "a" "c"]
                                            "k-type" ":string"
                                            "v-range" [":closed" 2 4]
                                            "v-type" ":long"}})
            first-n*    (ok-result {"op" "list-range-first-n"
                                    "args" {"kv" kv
                                            "list-name" "list"
                                            "n" 2
                                            "k-range" [":closed" "a" "c"]
                                            "k-type" ":string"
                                            "v-range" [":closed" 2 4]
                                            "v-type" ":long"}})
            key-cnt     (ok-result {"op" "key-range-list-count"
                                    "args" {"kv" kv
                                            "list-name" "list"
                                            "k-range" [":all"]
                                            "k-type" ":string"}})
            _del-some   (ok-result {"op" "del-list-items"
                                    "args" {"kv" kv
                                            "list-name" "list"
                                            "k" "b"
                                            "vs" [5 7]
                                            "k-type" ":string"
                                            "v-type" ":long"}})
            list-b2     (ok-result {"op" "get-list"
                                    "args" {"kv" kv
                                            "list-name" "list"
                                            "k" "b"
                                            "k-type" ":string"
                                            "v-type" ":long"}})
            _del-a      (ok-result {"op" "del-list-items"
                                    "args" {"kv" kv
                                            "list-name" "list"
                                            "k" "a"
                                            "k-type" ":string"}})
            list-a      (ok-result {"op" "get-list"
                                    "args" {"kv" kv
                                            "list-name" "list"
                                            "k" "a"
                                            "k-type" ":string"
                                            "v-type" ":long"}})
            same-kv     (ok-result {"op" "re-index"
                                    "args" {"kv" kv
                                            "opts" {}}})
            _reopen     (ok-result {"op" "open-dbi"
                                    "args" {"kv" kv
                                            "dbi-name" "items"}})
            _reopen-list (ok-result {"op" "open-list-dbi"
                                     "args" {"kv" kv
                                             "list-name" "list"}})
            counter*    (ok-result {"op" "get-value"
                                    "args" {"kv" kv
                                            "dbi" "items"
                                            "k" "counter"
                                            "k-type" ":string"
                                            "v-type" ":long"}})
            list-c      (ok-result {"op" "get-list"
                                    "args" {"kv" kv
                                            "list-name" "list"
                                            "k" "c"
                                            "k-type" ":string"
                                            "v-type" ":long"}})]
        (is (= 1 committed))
        (is (nil? aborted))
        (is (nil? after-temp))
        (is (= [6] list-b))
        (is (= 4 count-a))
        (is (= true in-list?))
        (is (= [["a" 3] ["a" 4]] range*))
        (is (= 10 range-cnt))
        (is (= ["a" 2] first*))
        (is (= [["a" 2] ["a" 3]] first-n*))
        (is (= 10 key-cnt))
        (is (= [6] list-b2))
        (is (= [] list-a))
        (is (= kv same-kv))
        (is (= 1 counter*))
        (is (= [3 6 9] list-c))
        (ok-result {"op" "close-kv"
                    "args" {"kv" kv}}))
      (finally
        (u/delete-files dir)))))

(deftest datalog-re-index-op-test
  (let [dir    (u/tmp-dir (str "json-api-reindex-conn-" (UUID/randomUUID)))
        schema {:name {:db/valueType :db.type/string}}]
    (try
      (let [conn      (ok-result {"op" "create-conn"
                                  "args" {"dir" dir
                                          "schema" schema}})
            _tx       (ok-result {"op" "transact!"
                                  "args" {"conn" conn
                                          "tx-data" [{:db/id -1
                                                      :name  "Alice"}]}})
            before    (ok-result {"op" "fulltext-datoms"
                                  "args" {"conn" conn
                                          "query" "Alice"}})
            same-conn (ok-result {"op" "re-index"
                                  "args" {"conn" conn
                                          "schema"
                                          {:name {:db/valueType :db.type/string
                                                  :db/fulltext  true}}
                                          "opts" {}}})
            after     (ok-result {"op" "fulltext-datoms"
                                  "args" {"conn" conn
                                          "query" "Alice"}})]
        (is (= [] before))
        (is (= conn same-conn))
        (is (= 1 (count after)))
        (ok-result {"op" "close"
                    "args" {"conn" conn}}))
      (finally
        (u/delete-files dir)))))

(deftest search-and-vector-ops-test
  (let [dir (u/tmp-dir (str "json-api-search-vec-" (UUID/randomUUID)))]
    (try
      (let [kv         (ok-result {"op" "open-kv"
                                   "args" {"dir" dir}})
            search-h   (ok-result {"op" "new-search-engine"
                                   "args" {"kv" kv
                                           "opts" {:include-text? true}}})
            _add-doc-1 (ok-result {"op" "add-doc"
                                   "args" {"search" search-h
                                           "doc-ref" "doc-1"
                                           "doc-text" "pizza and pasta"}})
            _add-doc-2 (ok-result {"op" "add-doc"
                                   "args" {"search" search-h
                                           "doc-ref" "doc-2"
                                           "doc-text" "just pie"}})
            doc-count  (ok-result {"op" "doc-count"
                                   "args" {"search" search-h}})
            indexed?   (ok-result {"op" "doc-indexed?"
                                   "args" {"search" search-h
                                           "doc-ref" "doc-1"}})
            hits       (vec
                         (ok-result {"op" "search"
                                     "args" {"search" search-h
                                             "query" "pizza"
                                             "opts" {:top 2}}}))
            hit-texts  (vec
                         (ok-result {"op" "search"
                                     "args" {"search" search-h
                                             "query" "pizza"
                                             "opts" {:top 1
                                                     :display :texts}}}))
            same-h     (ok-result {"op" "search-re-index"
                                   "args" {"search" search-h
                                           "opts" {:include-text? true}}})
            hits2      (vec
                         (ok-result {"op" "search"
                                     "args" {"search" search-h
                                             "query" "pizza"
                                             "opts" {:top 2}}}))
            _remove    (ok-result {"op" "remove-doc"
                                   "args" {"search" search-h
                                           "doc-ref" "doc-1"}})
            indexed2?  (ok-result {"op" "doc-indexed?"
                                   "args" {"search" search-h
                                           "doc-ref" "doc-1"}})
            _clear     (ok-result {"op" "clear-docs"
                                   "args" {"search" search-h}})
            doc-count0 (ok-result {"op" "doc-count"
                                   "args" {"search" search-h}})]
        (is (= 2 doc-count))
        (is (= true indexed?))
        (is (= ["doc-1"] hits))
        (is (= [["doc-1" "pizza and pasta"]] hit-texts))
        (is (= search-h same-h))
        (is (= ["doc-1"] hits2))
        (is (= false indexed2?))
        (is (= 0 doc-count0))
        (ok-result {"op" "release-handle"
                    "args" {"handle" search-h}})
        (when-not (u/windows?)
          (let [vec-h      (ok-result {"op" "new-vector-index"
                                       "args" {"kv" kv
                                               "opts" {:dimensions 3}}})
                _add-vec-1 (ok-result {"op" "add-vec"
                                       "args" {"vec" vec-h
                                               "vec-ref" "v1"
                                               "vec-data" [1.0 0.0 0.0]}})
                _add-vec-2 (ok-result {"op" "add-vec"
                                       "args" {"vec" vec-h
                                               "vec-ref" "v2"
                                               "vec-data" [0.0 1.0 0.0]}})
                info       (ok-result {"op" "vector-index-info"
                                       "args" {"vec" vec-h}})
                indexedv?  (ok-result {"op" "vec-indexed?"
                                       "args" {"vec" vec-h
                                               "vec-ref" "v1"}})
                top1       (vec
                             (ok-result {"op" "search-vec"
                                         "args" {"vec" vec-h
                                                 "query-vec" [1.0 0.0 0.0]
                                                 "opts" {:top 1}}}))
                dists      (vec
                             (ok-result {"op" "search-vec"
                                         "args" {"vec" vec-h
                                                 "query-vec" [1.0 0.0 0.0]
                                                 "opts" {:top 1
                                                         :display
                                                         :refs+dists}}}))
                _persist   (ok-result {"op" "force-vec-checkpoint!"
                                       "args" {"vec" vec-h}})
                checkpoint (ok-result {"op" "vector-checkpoint-state"
                                       "args" {"vec" vec-h}})
                same-vec-h (ok-result {"op" "vec-re-index"
                                       "args" {"vec" vec-h
                                               "opts" {:dimensions 3}}})
                top1b      (vec
                             (ok-result {"op" "search-vec"
                                         "args" {"vec" vec-h
                                                 "query-vec" [1.0 0.0 0.0]
                                                 "opts" {:top 1}}}))
                _remove-v1 (ok-result {"op" "remove-vec"
                                       "args" {"vec" vec-h
                                               "vec-ref" "v1"}})
                indexedv2? (ok-result {"op" "vec-indexed?"
                                       "args" {"vec" vec-h
                                               "vec-ref" "v1"}})
                _clear-vec (ok-result {"op" "clear-vector-index"
                                       "args" {"vec" vec-h}})
                handles    (ok-result {"op" "list-handles"
                                       "args" {}})]
            (is (= 2 (:size info)))
            (is (= true indexedv?))
            (is (= ["v1"] top1))
            (is (= "v1" (first (first dists))))
            (is (= vec-h same-vec-h))
            (is (= ["v1"] top1b))
            (is (pos? (:chunk-count checkpoint)))
            (is (= false indexedv2?))
            (is (not (contains? handles vec-h)))))
        (ok-result {"op" "close-kv"
                    "args" {"kv" kv}}))
      (finally
        (u/delete-files dir)))))

(deftest search-writer-ops-test
  (let [dir (u/tmp-dir (str "json-api-search-writer-" (UUID/randomUUID)))]
    (try
      (let [kv         (ok-result {"op" "open-kv"
                                   "args" {"dir" dir}})
            writer     (ok-result {"op" "search-index-writer"
                                   "args" {"kv" kv
                                           "opts" {:include-text? true}}})
            _write-1   (ok-result {"op" "search-write"
                                   "args" {"search-writer" writer
                                           "doc-ref" "doc-1"
                                           "doc-text" "pizza and pasta"}})
            _write-2   (ok-result {"op" "search-write"
                                   "args" {"search-writer" writer
                                           "doc-ref" "doc-2"
                                           "doc-text" "just pie"}})
            committed  (ok-result {"op" "search-commit"
                                   "args" {"search-writer" writer}})
            handles    (ok-result {"op" "list-handles"
                                   "args" {}})
            bad-write  (exec-request {"op" "search-write"
                                      "args" {"search-writer" writer
                                              "doc-ref" "doc-3"
                                              "doc-text" "later"}})
            search-h   (ok-result {"op" "new-search-engine"
                                   "args" {"kv" kv
                                           "opts" {:include-text? true}}})
            hits       (vec
                         (ok-result {"op" "search"
                                     "args" {"search" search-h
                                             "query" "pizza"
                                             "opts" {:top 1
                                                     :display :texts}}}))]
        (is (= :transacted committed))
        (is (not (contains? handles writer)))
        (is (= false (get bad-write "ok")))
        (is (= :invalid-handle
               (get-in bad-write ["data" :code])))
        (is (= [["doc-1" "pizza and pasta"]] hits))
        (ok-result {"op" "release-handle"
                    "args" {"handle" search-h}})
        (ok-result {"op" "close-kv"
                    "args" {"kv" kv}}))
      (finally
        (u/delete-files dir)))))

(deftest txlog-and-snapshot-ops-test
  (when-not (u/windows?)
    (let [dir (u/tmp-dir (str "json-api-txlog-" (UUID/randomUUID)))]
      (try
        (let [kv         (ok-result {"op" "open-kv"
                                     "args" {"dir" dir
                                             "opts" {:wal?  true
                                                     :flags (conj c/default-env-flags
                                                                  :nosync)}}})
              _open      (ok-result {"op" "open-dbi"
                                     "args" {"kv" kv
                                             "dbi-name" "items"}})
              wm0        (ok-result {"op" "txlog-watermarks"
                                     "args" {"kv" kv}})
              _tx        (ok-result {"op" "transact-kv"
                                     "args" {"kv" kv
                                             "dbi-name" "items"
                                             "k-type" ":string"
                                             "v-type" ":string"
                                             "txs" [[:put "a" "alpha"]
                                                    [:put "b" "beta"]]}})
              wm1        (ok-result {"op" "txlog-watermarks"
                                     "args" {"kv" kv}})
              records    (vec
                           (ok-result {"op" "open-tx-log"
                                       "args" {"kv" kv
                                               "from-lsn" 1
                                               "limit" 1}}))
              snapshot   (ok-result {"op" "create-snapshot!"
                                     "args" {"kv" kv}})
              snapshots  (vec
                           (ok-result {"op" "list-snapshots"
                                       "args" {"kv" kv}}))
              gc-result  (ok-result {"op" "gc-txlog-segments!"
                                     "args" {"kv" kv}})]
          (is (= true (:wal? wm0)))
          (is (= true (:wal? wm1)))
          (is (<= (:last-appended-lsn wm0) (:last-appended-lsn wm1)))
          (is (= 1 (count records)))
          (is (map? (first records)))
          (is (map? snapshot))
          (is (contains? snapshot :ok?))
          (is (seq snapshots))
          (is (map? gc-result))
          (is (contains? gc-result :ok?))
          (ok-result {"op" "close-kv"
                      "args" {"kv" kv}}))
        (finally
          (u/delete-files dir))))))

(deftest client-admin-ops-test
  (tdc/server-fixture
    (fn []
      (let [suffix         (str (UUID/randomUUID))
            db-name        (str "json-api-db-" suffix)
            username       (str "json-api-user-" suffix)
            password       "secret-one"
            new-password   "secret-two"
            role-str       (str ":json-api-role-" suffix)
            role           (keyword (subs role-str 1))
            admin-client   (ok-result
                             {"op" "new-client"
                              "args" {"uri"
                                      "dtlv://datalevin:datalevin@localhost"}})
            admin-client-id*
                          (ok-result
                            {"op" "client-id"
                             "args" {"client" admin-client}})
            admin-client-id
                          (dc/get-id (#'sut/resolve-client admin-client))
            admin-open?    (ok-result
                             {"op" "disconnected?"
                              "args" {"client" admin-client}})
            _create-db     (ok-result
                             {"op" "create-database"
                              "args" {"client"  admin-client
                                      "db-name" db-name
                                      "db-type" "datalog"}})
            open-info      (ok-result
                             {"op" "open-database"
                              "args" {"client"          admin-client
                                      "db-name"         db-name
                                      "db-type"         "datalog"
                                      "return-db-info?" true}})
            databases      (vec
                             (ok-result
                               {"op" "list-databases"
                                "args" {"client" admin-client}}))
            databases-use  (vec
                             (ok-result
                               {"op" "list-databases-in-use"
                                "args" {"client" admin-client}}))
            _create-user   (ok-result
                             {"op" "create-user"
                              "args" {"client"   admin-client
                                      "username" username
                                      "password" password}})
            _reset-pw      (ok-result
                             {"op" "reset-password"
                              "args" {"client"   admin-client
                                      "username" username
                                      "password" new-password}})
            _create-role   (ok-result
                             {"op" "create-role"
                              "args" {"client" admin-client
                                      "role"   role-str}})
            roles          (vec
                             (ok-result
                               {"op" "list-roles"
                                "args" {"client" admin-client}}))
            _grant         (ok-result
                             {"op" "grant-permission"
                              "args" {"client" admin-client
                                      "role"   role-str
                                      "act"    ":datalevin.server/alter"
                                      "obj"    ":datalevin.server/database"
                                      "tgt"    db-name}})
            _assign        (ok-result
                             {"op" "assign-role"
                              "args" {"client"   admin-client
                                      "role"     role-str
                                      "username" username}})
            role-perms     (vec
                             (ok-result
                               {"op" "list-role-permissions"
                                "args" {"client" admin-client
                                        "role"   role-str}}))
            user-roles     (vec
                             (ok-result
                               {"op" "list-user-roles"
                                "args" {"client"   admin-client
                                        "username" username}}))
            user-perms     (vec
                             (ok-result
                               {"op" "list-user-permissions"
                                "args" {"client"   admin-client
                                        "username" username}}))
            system-db-names (vec
                              (ok-result
                                {"op" "query-system"
                                 "args" {"client" admin-client
                                         "query"
                                         "[:find [?name ...] :where [?e :database/name ?name]]"}}))
            user-client    (ok-result
                             {"op" "new-client"
                              "args" {"uri"
                                      "dtlv://datalevin:datalevin@localhost"}})
            user-open?     (ok-result
                             {"op" "disconnected?"
                              "args" {"client" user-client}})
            user-client-id (dc/get-id (#'sut/resolve-client user-client))
            _disconnect    (ok-result
                             {"op" "disconnect-client"
                              "args" {"client"    admin-client
                                      "client-id" (str user-client-id)}})
            clients-after  (ok-result
                             {"op" "show-clients"
                              "args" {"client" admin-client}})
            _withdraw      (ok-result
                             {"op" "withdraw-role"
                              "args" {"client"   admin-client
                                      "role"     role-str
                                      "username" username}})
            user-roles2    (vec
                             (ok-result
                               {"op" "list-user-roles"
                                "args" {"client"   admin-client
                                        "username" username}}))
            _close-user    (ok-result
                             {"op" "disconnect"
                              "args" {"client" user-client}})
            _drop-role     (ok-result
                             {"op" "drop-role"
                              "args" {"client" admin-client
                                      "role"   role-str}})
            _drop-user     (ok-result
                             {"op" "drop-user"
                              "args" {"client"   admin-client
                                      "username" username}})
            _close-db      (ok-result
                             {"op" "close-database"
                              "args" {"client"  admin-client
                                      "db-name" db-name}})
            _drop-db       (ok-result
                             {"op" "drop-database"
                              "args" {"client"  admin-client
                                      "db-name" db-name}})
            _close-admin   (ok-result
                             {"op" "close-client"
                              "args" {"client" admin-client}})]
        (is (map? open-info))
        (is (integer? (:max-eid open-info)))
        (is (contains? (set databases) db-name))
        (is (contains? (set databases-use) db-name))
        (is (contains? (set roles) role))
        (is (some #(and (= :datalevin.server/alter
                           (:permission/act %))
                        (= :datalevin.server/database
                           (:permission/obj %)))
                  role-perms))
        (is (some #(and (= :datalevin.server/alter
                           (:permission/act %))
                        (= :datalevin.server/database
                           (:permission/obj %)))
                  user-perms))
        (is (contains? (set system-db-names) db-name))
        (is (= admin-client-id* admin-client-id))
        (is (= false admin-open?))
        (is (instance? UUID user-client-id))
        (is (= false user-open?))
        (is (not (contains? clients-after user-client-id)))
        (is (not (contains? (set user-roles2) role)))))))
