(ns datalevin.mcp-test
  (:require
   [clojure.string :as str]
   [clojure.test :refer [deftest is]]
   [datalevin.constants :as c]
   [datalevin.core :as d]
   [datalevin.json-api.shared :as shared]
   [datalevin.json-convert :as jc]
   [datalevin.main :as main]
   [datalevin.mcp :as sut]
   [datalevin.test.core :as tdc]
   [datalevin.util :as u])
  (:import
   [java.io StringReader StringWriter]
   [java.util UUID]))

(defn- new-state
  ([] (new-state {}))
  ([opts]
   (atom {:phase         :new
          :initialized?  false
          :allow-writes? (true? (:allow-writes? opts))
          :session-state (shared/new-session-state)})))

(def ^:private default-initialize-params
  {"protocolVersion" "2025-06-18"
   "capabilities"    {}
   "clientInfo"      {"name"    "test-client"
                      "version" "1.0"}})

(defn- initialized-state
  ([] (initialized-state {}))
  ([opts]
   (let [state (new-state opts)]
     (sut/handle-message
       state
       {"jsonrpc" "2.0"
        "id"      0
        "method"  "initialize"
        "params"  default-initialize-params})
     (sut/handle-message
       state
       {"jsonrpc" "2.0"
        "method"  "notifications/initialized"
        "params"  {}})
     state)))

(def expected-tool-names
  ["datalevin_add_document"
   "datalevin_add_vector"
   "datalevin_api_info"
   "datalevin_clear_documents"
   "datalevin_close_database"
   "datalevin_close_kv"
   "datalevin_close_search_index"
   "datalevin_close_vector_index"
   "datalevin_count_datoms"
   "datalevin_datoms"
   "datalevin_document_count"
   "datalevin_document_indexed"
   "datalevin_entity"
   "datalevin_fulltext_datoms"
   "datalevin_kv_get"
   "datalevin_kv_range"
   "datalevin_kv_transact"
   "datalevin_open_database"
   "datalevin_open_kv"
   "datalevin_open_search_index"
   "datalevin_open_vector_index"
   "datalevin_pull"
   "datalevin_pull_many"
   "datalevin_query"
   "datalevin_remove_document"
   "datalevin_remove_vector"
   "datalevin_search_datoms"
   "datalevin_search_documents"
   "datalevin_transact"
   "datalevin_vector_index_info"
   "datalevin_vector_indexed"
   "datalevin_vector_search"])

(defn- json-byte-count
  [value]
  (count (.getBytes (jc/write-json-ready-string value) "UTF-8")))

(deftest initialize-tools-and-notification-test
  (let [state          (new-state)
        pre-init-tool  (sut/handle-message
                         state
                         {"jsonrpc" "2.0"
                          "id"      11
                          "method"  "tools/call"
                          "params"  {"name"      "datalevin_api_info"
                                      "arguments" {}}})
        initialize     (sut/handle-message
                         state
                         {"jsonrpc" "2.0"
                          "id"      1
                          "method"  "initialize"
                          "params"  default-initialize-params})
        pre-ready-list (sut/handle-message
                         state
                         {"jsonrpc" "2.0"
                          "id"      12
                          "method"  "tools/list"
                          "params"  {}})
        notified       (sut/handle-message
                         state
                         {"jsonrpc" "2.0"
                          "method"  "notifications/initialized"
                          "params"  {}})
        tools          (sut/handle-message
                         state
                         {"jsonrpc" "2.0"
                          "id"      2
                          "method"  "tools/list"
                          "params"  {}})]
    (is (= -32600 (get-in pre-init-tool ["error" "code"])))
    (is (= "Requests are not allowed before initialize."
           (get-in pre-init-tool ["error" "message"])))
    (is (= "2.0" (get initialize "jsonrpc")))
    (is (= 1 (get initialize "id")))
    (is (= "2025-06-18"
           (get-in initialize ["result" "protocolVersion"])))
    (is (= "datalevin"
           (get-in initialize ["result" "serverInfo" "name"])))
    (is (= c/version
           (get-in initialize ["result" "serverInfo" "version"])))
    (is (= -32600 (get-in pre-ready-list ["error" "code"])))
    (is (= "Requests are not allowed before initialized."
           (get-in pre-ready-list ["error" "message"])))
    (is (= expected-tool-names
           (mapv #(get % "name") (get-in tools ["result" "tools"]))))
    (is (nil? notified))
    (is (= :ready (:phase @state)))
    (is (true? (:initialized? @state)))))

(deftest api-info-tool-test
  (let [state    (initialized-state)
        response (sut/handle-message
                   state
                   {"jsonrpc" "2.0"
                    "id"      3
                    "method"  "tools/call"
                    "params"  {"name"      "datalevin_api_info"
                                "arguments" {}}})]
    (is (= "2.0" (get response "jsonrpc")))
    (is (= 3 (get response "id")))
    (is (= "stdio"
           (get-in response ["result" "structuredContent" "mcp" "transport"])))
    (is (= c/version
           (get-in response ["result" "structuredContent" "datalevin"
                             "datalevin-version"])))
    (is (= 1
           (get-in response ["result" "structuredContent" "datalevin"
                             "json-api-version"])))
    (is (= "text"
           (get-in response ["result" "content" 0 "type"])))
    (is (string? (get-in response ["result" "content" 0 "text"])))))

(deftest unknown-method-test
  (let [state    (initialized-state)
        response (sut/handle-message
                   state
                   {"jsonrpc" "2.0"
                    "id"      9
                    "method"  "frobnicate"
                    "params"  {}})]
    (is (= -32601 (get-in response ["error" "code"])))
    (is (= "Unsupported method: frobnicate"
           (get-in response ["error" "message"])))))

(deftest initialize-validation-test
  (let [missing-params     (sut/handle-message
                             (new-state)
                             {"jsonrpc" "2.0"
                              "id"      92
                              "method"  "initialize"})
        empty-params       (sut/handle-message
                             (new-state)
                             {"jsonrpc" "2.0"
                              "id"      93
                              "method"  "initialize"
                              "params"  {}})
        missing-capability (sut/handle-message
                             (new-state)
                             {"jsonrpc" "2.0"
                              "id"      94
                              "method"  "initialize"
                              "params"  {"protocolVersion" "2025-06-18"
                                          "clientInfo"      {"name"    "test-client"
                                                             "version" "1.0"}}})
        missing-client     (sut/handle-message
                             (new-state)
                             {"jsonrpc" "2.0"
                              "id"      95
                              "method"  "initialize"
                              "params"  {"protocolVersion" "2025-06-18"
                                          "capabilities"    {}}})]
    (is (= -32602 (get-in missing-params ["error" "code"])))
    (is (= "protocolVersion is required."
           (get-in missing-params ["error" "message"])))
    (is (= -32602 (get-in empty-params ["error" "code"])))
    (is (= "protocolVersion is required."
           (get-in empty-params ["error" "message"])))
    (is (= -32602 (get-in missing-capability ["error" "code"])))
    (is (= "capabilities is required."
           (get-in missing-capability ["error" "message"])))
    (is (= -32602 (get-in missing-client ["error" "code"])))
    (is (= "clientInfo is required."
           (get-in missing-client ["error" "message"])))))

(deftest request-id-required-test
  (let [missing-init-id (sut/handle-message
                          (new-state)
                          {"jsonrpc" "2.0"
                           "method"  "initialize"
                           "params"  default-initialize-params})
        missing-list-id (sut/handle-message
                          (initialized-state)
                          {"jsonrpc" "2.0"
                           "method"  "tools/list"
                           "params"  {}})
        nil-call-id     (sut/handle-message
                          (initialized-state)
                          {"jsonrpc" "2.0"
                           "id"      nil
                           "method"  "tools/call"
                           "params"  {"name"      "datalevin_api_info"
                                       "arguments" {}}})]
    (is (= -32600 (get-in missing-init-id ["error" "code"])))
    (is (= "id is required for requests."
           (get-in missing-init-id ["error" "message"])))
    (is (nil? (get missing-init-id "id")))
    (is (= -32600 (get-in missing-list-id ["error" "code"])))
    (is (= "id is required for requests."
           (get-in missing-list-id ["error" "message"])))
    (is (nil? (get missing-list-id "id")))
    (is (= -32600 (get-in nil-call-id ["error" "code"])))
    (is (= "id must be a string or number."
           (get-in nil-call-id ["error" "message"])))
    (is (nil? (get nil-call-id "id")))))

(deftest invalid-method-request-test
  (let [state             (initialized-state)
        missing-method    (sut/handle-message
                            state
                            {"jsonrpc" "2.0"
                             "id"      90
                             "params"  {}})
        non-string-method (sut/handle-message
                            state
                            {"jsonrpc" "2.0"
                             "id"      91
                             "method"  42
                             "params"  {}})]
    (is (= -32600 (get-in missing-method ["error" "code"])))
    (is (= "method must be a string."
           (get-in missing-method ["error" "message"])))
    (is (= -32600 (get-in non-string-method ["error" "code"])))
    (is (= "method must be a string."
           (get-in non-string-method ["error" "message"])))))

(deftest command-line-args-test
  (let [result (main/validate-args ["--allow-writes" "mcp"])]
    (is (= "mcp" (:command result)))
    (is (map? (:options result)))
    (is (= true (:allow-writes (:options result))))))

(deftest remote-open-tool-shaping-test
  (is (= {:target "dtlv://user:pass@localhost/app"
          :remote true}
         (#'sut/require-open-target
           {"uri" "dtlv://user:pass@localhost/app"})))
  (is (= {:target "/tmp/local-db"
          :remote false}
         (#'sut/require-open-target
           {"dir" "/tmp/local-db"})))
  (is (= {:client-opts {"pool-size" 5}}
         (#'sut/merge-open-opts
           {"clientOpts" {"pool-size" 5}})))
  (is (= {:client-opts {"pool-size" 3
                        "time-out" 120000}
          :flags [:nosync]}
         (#'sut/merge-open-opts
           {"opts"       {":flags" [":nosync"]}
            "clientOpts" {"pool-size" 3
                          "time-out" 120000}})))
  (let [both-error (sut/handle-message
                     (initialized-state)
                     {"jsonrpc" "2.0"
                      "id"      4
                      "method"  "tools/call"
                      "params"  {"name"      "datalevin_open_database"
                                  "arguments"
                                   {"dir" "/tmp/local-db"
                                    "uri" "dtlv://user:pass@localhost/app"}}})
        none-error (sut/handle-message
                     (initialized-state)
                     {"jsonrpc" "2.0"
                      "id"      5
                      "method"  "tools/call"
                      "params"  {"name"      "datalevin_open_kv"
                                  "arguments" {}}})]
    (is (= true (get-in both-error ["result" "isError"])))
    (is (= ":invalid-params"
           (get-in both-error ["result" "structuredContent" "code"])))
    (is (= true (get-in none-error ["result" "isError"])))
    (is (= ":invalid-params"
           (get-in none-error ["result" "structuredContent" "code"])))))

(deftest database-tools-test
  (let [dir    (u/tmp-dir (str "datalevin-mcp-" (UUID/randomUUID)))
        schema {:name {:db/valueType :db.type/string}
                :bio  {:db/valueType :db.type/string
                       :db/fulltext  true}}
        conn   (d/create-conn dir schema)
        report (d/transact! conn [{:db/id -1
                                   :name  "Alice"
                                   :bio   "Alice enjoys pizza"}])
        alice  (get (:tempids report) -1)
        _      (d/close conn)
        state  (initialized-state)]
    (try
      (let [open-response  (sut/handle-message
                             state
                             {"jsonrpc" "2.0"
                              "id"      10
                              "method"  "tools/call"
                              "params"  {"name"      "datalevin_open_database"
                                          "arguments" {"dir" dir}}})
            database-id    (get-in open-response ["result" "structuredContent"
                                                  "database"])
            query-response (sut/handle-message
                             state
                             {"jsonrpc" "2.0"
                              "id"      11
                              "method"  "tools/call"
                              "params"  {"name"      "datalevin_query"
                                          "arguments"
                                          {"database" database-id
                                           "query"
                                           "[:find ?n :where [?e :name ?n]]"}}})
            pull-response  (sut/handle-message
                             state
                             {"jsonrpc" "2.0"
                              "id"      12
                              "method"  "tools/call"
                              "params"  {"name"      "datalevin_pull"
                                          "arguments"
                                          {"database" database-id
                                           "selector" [:name]
                                           "eid"      alice}}})
            entity-response (sut/handle-message
                              state
                              {"jsonrpc" "2.0"
                               "id"      121
                               "method"  "tools/call"
                               "params"  {"name"      "datalevin_entity"
                                           "arguments"
                                           {"database" database-id
                                            "eid"      alice}}})
            pull-many-response
                           (sut/handle-message
                             state
                             {"jsonrpc" "2.0"
                              "id"      122
                              "method"  "tools/call"
                              "params"  {"name"      "datalevin_pull_many"
                                          "arguments"
                                          {"database" database-id
                                           "selector" [:name]
                                           "eids"     [alice]}}})
            datoms-response (sut/handle-message
                              state
                              {"jsonrpc" "2.0"
                               "id"      125
                               "method"  "tools/call"
                               "params"  {"name"      "datalevin_datoms"
                                           "arguments"
                                           {"database" database-id
                                            "index"    ":ave"
                                            "c1"       ":name"
                                            "limit"    1}}})
            fulltext-response
                           (sut/handle-message
                             state
                             {"jsonrpc" "2.0"
                              "id"      126
                              "method"  "tools/call"
                              "params"  {"name"      "datalevin_fulltext_datoms"
                                          "arguments"
                                          {"database" database-id
                                           "query"    "pizza"}}})
            search-response (sut/handle-message
                              state
                              {"jsonrpc" "2.0"
                               "id"      127
                               "method"  "tools/call"
                               "params"  {"name"      "datalevin_search_datoms"
                                           "arguments"
                                           {"database" database-id
                                            "a"        ":name"}}})
            count-response  (sut/handle-message
                              state
                              {"jsonrpc" "2.0"
                               "id"      128
                               "method"  "tools/call"
                               "params"  {"name"      "datalevin_count_datoms"
                                           "arguments"
                                           {"database" database-id
                                            "a"        ":name"}}})
            close-response (sut/handle-message
                             state
                             {"jsonrpc" "2.0"
                              "id"      13
                              "method"  "tools/call"
                              "params"  {"name"      "datalevin_close_database"
                                          "arguments"
                                          {"database" database-id}}})
            post-close     (sut/handle-message
                             state
                             {"jsonrpc" "2.0"
                              "id"      14
                              "method"  "tools/call"
                              "params"  {"name"      "datalevin_query"
                                          "arguments"
                                          {"database" database-id
                                           "query"
                                           "[:find ?n :where [?e :name ?n]]"}}})]
        (is (= database-id
               (get-in query-response ["result" "structuredContent" "database"])))
        (is (str/includes?
              (get-in query-response ["result" "content" 0 "text"])
              "Alice"))
        (is (= #{["Alice"]}
               (set (get-in query-response ["result" "structuredContent"
                                            "result" "~set"]))))
        (is (= {":name" "Alice"}
               (get-in pull-response ["result" "structuredContent" "result"])))
        (is (= "Alice"
               (get-in entity-response
                       ["result" "structuredContent" "result" ":name"])))
        (is (= [{":name" "Alice"}]
               (get-in pull-many-response
                       ["result" "structuredContent" "result"])))
        (is (= ["Alice"]
               (mapv #(get % "v")
                     (get-in datoms-response ["result" "structuredContent" "result"]))))
        (is (= #{[alice ":bio" "Alice enjoys pizza"]}
               (set (get-in fulltext-response
                            ["result" "structuredContent" "result"]))))
        (is (= [alice]
               (mapv #(get % "e")
                     (get-in search-response ["result" "structuredContent" "result"]))))
        (is (= 1
               (get-in count-response ["result" "structuredContent" "result"])))
        (is (= true
               (get-in close-response ["result" "structuredContent" "closed"])))
        (is (= true (get-in post-close ["result" "isError"])))
        (is (= ":unknown-database"
               (get-in post-close ["result" "structuredContent" "code"]))))
      (finally
        (shared/clear-handles! (:session-state @state))
        (u/delete-files dir)))))

(deftest kv-tools-test
  (let [dir   (u/tmp-dir (str "datalevin-mcp-kv-" (UUID/randomUUID)))
        kv    (d/open-kv dir)
        _     (d/open-dbi kv "items")
        _     (d/transact-kv kv [[:put "items" "a" "alpha"]
                                 [:put "items" "b" "beta"]
                                 [:put "items" "c" "gamma"]])
        _     (d/close-kv kv)
        state (initialized-state)]
    (try
      (let [open-response  (sut/handle-message
                             state
                             {"jsonrpc" "2.0"
                              "id"      30
                              "method"  "tools/call"
                              "params"  {"name"      "datalevin_open_kv"
                                          "arguments" {"dir" dir}}})
            kv-id          (get-in open-response ["result" "structuredContent" "kv"])
            get-response   (sut/handle-message
                             state
                             {"jsonrpc" "2.0"
                              "id"      31
                              "method"  "tools/call"
                              "params"  {"name"      "datalevin_kv_get"
                                          "arguments"
                                          {"kv"  kv-id
                                           "dbi" "items"
                                           "key" "b"}}})
            pair-response  (sut/handle-message
                             state
                             {"jsonrpc" "2.0"
                              "id"      32
                              "method"  "tools/call"
                              "params"  {"name"      "datalevin_kv_get"
                                          "arguments"
                                          {"kv"         kv-id
                                           "dbi"        "items"
                                           "key"        "b"
                                           "returnPair" true}}})
            range-response (sut/handle-message
                             state
                             {"jsonrpc" "2.0"
                              "id"      33
                              "method"  "tools/call"
                              "params"  {"name"      "datalevin_kv_range"
                                          "arguments"
                                          {"kv"       kv-id
                                           "dbi"      "items"
                                           "keyRange" [":all"]
                                           "limit"    2
                                           "offset"   1}}})
            close-response (sut/handle-message
                             state
                             {"jsonrpc" "2.0"
                              "id"      34
                              "method"  "tools/call"
                              "params"  {"name"      "datalevin_close_kv"
                                          "arguments"
                                          {"kv" kv-id}}})
            post-close     (sut/handle-message
                             state
                             {"jsonrpc" "2.0"
                              "id"      35
                              "method"  "tools/call"
                              "params"  {"name"      "datalevin_kv_get"
                                          "arguments"
                                          {"kv"  kv-id
                                           "dbi" "items"
                                           "key" "b"}}})]
        (is (= "beta"
               (get-in get-response ["result" "structuredContent" "result"])))
        (is (= ["b" "beta"]
               (get-in pair-response ["result" "structuredContent" "result"])))
        (is (= [["b" "beta"] ["c" "gamma"]]
               (get-in range-response ["result" "structuredContent" "result"])))
        (is (= true
               (get-in close-response ["result" "structuredContent" "closed"])))
        (is (= true (get-in post-close ["result" "isError"])))
        (is (= ":unknown-kv"
               (get-in post-close ["result" "structuredContent" "code"]))))
      (finally
        (shared/clear-handles! (:session-state @state))
        (u/delete-files dir)))))

(deftest search-tools-test
  (let [dir   (u/tmp-dir (str "datalevin-mcp-search-" (UUID/randomUUID)))
        kv    (d/open-kv dir)
        _     (d/close-kv kv)
        state (initialized-state {:allow-writes? true})]
    (try
      (let [open-kv        (sut/handle-message
                             state
                             {"jsonrpc" "2.0"
                              "id"      350
                              "method"  "tools/call"
                              "params"  {"name"      "datalevin_open_kv"
                                          "arguments" {"dir" dir}}})
            kv-id          (get-in open-kv ["result" "structuredContent" "kv"])
            open-search    (sut/handle-message
                             state
                             {"jsonrpc" "2.0"
                              "id"      351
                              "method"  "tools/call"
                              "params"  {"name"      "datalevin_open_search_index"
                                          "arguments"
                                          {"kv"   kv-id
                                           "opts" {":include-text?" true}}}})
            search-id      (get-in open-search ["result" "structuredContent"
                                                "searchIndex"])
            add-doc-1      (sut/handle-message
                             state
                             {"jsonrpc" "2.0"
                              "id"      352
                              "method"  "tools/call"
                              "params"  {"name"      "datalevin_add_document"
                                          "arguments"
                                          {"searchIndex" search-id
                                           "docRef"      "doc-1"
                                           "docText"     "pizza and pasta"}}})
            add-doc-2      (sut/handle-message
                             state
                             {"jsonrpc" "2.0"
                              "id"      353
                              "method"  "tools/call"
                              "params"  {"name"      "datalevin_add_document"
                                          "arguments"
                                          {"searchIndex" search-id
                                           "docRef"      "doc-2"
                                           "docText"     "just pie"}}})
            count-2        (sut/handle-message
                             state
                             {"jsonrpc" "2.0"
                              "id"      354
                              "method"  "tools/call"
                              "params"  {"name"      "datalevin_document_count"
                                          "arguments"
                                          {"searchIndex" search-id}}})
            indexed-doc-1  (sut/handle-message
                             state
                             {"jsonrpc" "2.0"
                              "id"      355
                              "method"  "tools/call"
                              "params"  {"name"      "datalevin_document_indexed"
                                          "arguments"
                                          {"searchIndex" search-id
                                           "docRef"      "doc-1"}}})
            search-hits    (sut/handle-message
                             state
                             {"jsonrpc" "2.0"
                              "id"      356
                              "method"  "tools/call"
                              "params"  {"name"      "datalevin_search_documents"
                                          "arguments"
                                          {"searchIndex" search-id
                                           "query"       "pizza"
                                           "opts"        {":top" 2}}}})
            search-texts   (sut/handle-message
                             state
                             {"jsonrpc" "2.0"
                              "id"      357
                              "method"  "tools/call"
                              "params"  {"name"      "datalevin_search_documents"
                                          "arguments"
                                          {"searchIndex" search-id
                                           "query"       "pizza"
                                           "opts"        {":top" 1
                                                          ":display"
                                                          ":texts"}}}})
            remove-doc-1   (sut/handle-message
                             state
                             {"jsonrpc" "2.0"
                              "id"      358
                              "method"  "tools/call"
                              "params"  {"name"      "datalevin_remove_document"
                                          "arguments"
                                          {"searchIndex" search-id
                                           "docRef"      "doc-1"}}})
            indexed-doc-1b (sut/handle-message
                             state
                             {"jsonrpc" "2.0"
                              "id"      359
                              "method"  "tools/call"
                              "params"  {"name"      "datalevin_document_indexed"
                                          "arguments"
                                          {"searchIndex" search-id
                                           "docRef"      "doc-1"}}})
            clear-docs     (sut/handle-message
                             state
                             {"jsonrpc" "2.0"
                              "id"      360
                              "method"  "tools/call"
                              "params"  {"name"      "datalevin_clear_documents"
                                          "arguments"
                                          {"searchIndex" search-id}}})
            count-0        (sut/handle-message
                             state
                             {"jsonrpc" "2.0"
                              "id"      361
                              "method"  "tools/call"
                              "params"  {"name"      "datalevin_document_count"
                                          "arguments"
                                          {"searchIndex" search-id}}})
            close-search   (sut/handle-message
                             state
                             {"jsonrpc" "2.0"
                              "id"      362
                              "method"  "tools/call"
                              "params"  {"name"      "datalevin_close_search_index"
                                          "arguments"
                                          {"searchIndex" search-id}}})
            post-close     (sut/handle-message
                             state
                             {"jsonrpc" "2.0"
                              "id"      363
                              "method"  "tools/call"
                              "params"  {"name"      "datalevin_search_documents"
                                          "arguments"
                                          {"searchIndex" search-id
                                           "query"       "pizza"}}})]
        (is (= false (get-in add-doc-1 ["result" "isError"] false)))
        (is (= false (get-in add-doc-2 ["result" "isError"] false)))
        (is (= 2
               (get-in count-2 ["result" "structuredContent" "result"])))
        (is (= true
               (get-in indexed-doc-1 ["result" "structuredContent" "result"])))
        (is (= ["doc-1"]
               (get-in search-hits ["result" "structuredContent" "result"])))
        (is (= [["doc-1" "pizza and pasta"]]
               (get-in search-texts ["result" "structuredContent" "result"])))
        (is (= true
               (get-in remove-doc-1 ["result" "structuredContent" "removed"])))
        (is (= false
               (get-in indexed-doc-1b ["result" "structuredContent" "result"])))
        (is (= true
               (get-in clear-docs ["result" "structuredContent" "cleared"])))
        (is (= 0
               (get-in count-0 ["result" "structuredContent" "result"])))
        (is (= true
               (get-in close-search ["result" "structuredContent" "closed"])))
        (is (= true (get-in post-close ["result" "isError"])))
        (is (= ":unknown-search-index"
               (get-in post-close ["result" "structuredContent" "code"]))))
      (finally
        (shared/clear-handles! (:session-state @state))
        (u/delete-files dir)))))

(deftest response-truncation-test
  (let [dir   (u/tmp-dir (str "datalevin-mcp-truncate-" (UUID/randomUUID)))
        blob  (apply str (repeat 700000 "x"))
        mid   (apply str (repeat 260000 "x"))
        kv    (d/open-kv dir)
        _     (d/open-dbi kv "items")
        _     (d/transact-kv kv [[:put "items" "a" "alpha"]
                                 [:put "items" "b" "beta"]
                                 [:put "items" "c" "gamma"]
                                 [:put "items" "mid" mid]
                                 [:put "items" "blob" blob]])
        _     (d/close-kv kv)
        state (initialized-state)]
    (try
      (let [open-kv (sut/handle-message
                      state
                      {"jsonrpc" "2.0"
                       "id"      364
                       "method"  "tools/call"
                       "params"  {"name"      "datalevin_open_kv"
                                   "arguments" {"dir" dir}}})
            kv-id   (get-in open-kv ["result" "structuredContent" "kv"])]
        (binding [sut/*mcp-limits* (assoc sut/default-mcp-limits
                                          :max-result-items 2
                                          :max-response-bytes nil)]
          (let [range-response (sut/handle-message
                                 state
                                 {"jsonrpc" "2.0"
                                  "id"      365
                                  "method"  "tools/call"
                                  "params"  {"name"      "datalevin_kv_range"
                                              "arguments"
                                              {"kv"       kv-id
                                               "dbi"      "items"
                                               "keyRange" [":all"]}}})]
            (is (= [["a" "alpha"] ["b" "beta"]]
                   (get-in range-response ["result" "structuredContent"
                                           "result"])))
            (is (= true
                   (get-in range-response ["result" "structuredContent"
                                           "meta" "truncated"])))
            (is (= "items"
                   (get-in range-response ["result" "structuredContent"
                                           "meta" "truncations" 0 "kind"])))
            (is (= 5
                   (get-in range-response ["result" "structuredContent"
                                           "meta" "truncations" 0 "original"])))))
        (binding [sut/*mcp-limits* (assoc sut/default-mcp-limits
                                          :max-result-items nil
                                          :max-response-bytes 320
                                          :preview-chars 16)]
          (let [get-response (sut/handle-message
                               state
                               {"jsonrpc" "2.0"
                                "id"      366
                                "method"  "tools/call"
                                "params"  {"name"      "datalevin_kv_get"
                                            "arguments"
                                            {"kv"  kv-id
                                             "dbi" "items"
                                             "key" "blob"}}})
                response-bytes (json-byte-count get-response)]
            (is (= "preview"
                   (get-in get-response ["result" "structuredContent"
                                         "meta" "truncations" 0 "mode"])))
            (is (= "bytes"
                   (get-in get-response ["result" "structuredContent"
                                         "meta" "truncations" 0 "kind"])))
            (is (= "xxxxxxxxxxxxxxxx..."
                   (get-in get-response ["result" "structuredContent"
                                         "result"])))
            (is (str/includes?
                  (get-in get-response ["result" "content" 0 "text"])
                  "Result truncated."))
            (is (<= response-bytes 320))))
        (binding [sut/*mcp-limits* (assoc sut/default-mcp-limits
                                          :max-result-items nil
                                          :max-response-bytes 256
                                          :preview-chars 16)]
          (let [too-small-response (sut/handle-message
                                     state
                                     {"jsonrpc" "2.0"
                                      "id"      3660
                                      "method"  "tools/call"
                                      "params"  {"name"      "datalevin_kv_get"
                                                  "arguments"
                                                  {"kv"  kv-id
                                                   "dbi" "items"
                                                   "key" "blob"}}})
                response-bytes    (json-byte-count too-small-response)]
            (is (= -32000
                   (get-in too-small-response ["error" "code"])))
            (is (= ":result-too-large"
                   (get-in too-small-response ["error" "data" ":code"])))
            (is (<= response-bytes 256))))
        (binding [sut/*mcp-limits* (assoc sut/default-mcp-limits
                                          :max-result-items nil
                                          :max-response-bytes 300000)]
          (let [mid-response    (sut/handle-message
                                  state
                                  {"jsonrpc" "2.0"
                                   "id"      3661
                                   "method"  "tools/call"
                                   "params"  {"name"      "datalevin_kv_get"
                                               "arguments"
                                               {"kv"  kv-id
                                                "dbi" "items"
                                                "key" "mid"}}})
                structured      (get-in mid-response ["result"
                                                      "structuredContent"])
                structured-bytes (json-byte-count structured)
                response-bytes   (json-byte-count mid-response)]
            (is (nil? (get-in mid-response ["result" "structuredContent"
                                            "meta" "truncated"])))
            (is (= "See structuredContent."
                   (get-in mid-response ["result" "content" 0 "text"])))
            (is (< structured-bytes 300000))
            (is (<= response-bytes 300000))))
        (let [default-get-response
              (sut/handle-message
                state
                {"jsonrpc" "2.0"
                 "id"      367
                 "method"  "tools/call"
                 "params"  {"name"      "datalevin_kv_get"
                             "arguments"
                             {"kv"  kv-id
                              "dbi" "items"
                              "key" "blob"}}})
              response-bytes (json-byte-count default-get-response)]
          (is (= "2.0" (get default-get-response "jsonrpc")))
          (is (contains? default-get-response "result"))
          (is (= true
                 (get-in default-get-response ["result" "structuredContent"
                                               "meta" "truncated"])))
          (is (= "bytes"
                 (get-in default-get-response ["result" "structuredContent"
                                               "meta" "truncations" 0
                                               "kind"])))
          (is (str/includes?
                (get-in default-get-response ["result" "content" 0 "text"])
                "Result truncated."))
          (is (<= response-bytes
                  (:max-response-bytes sut/default-mcp-limits)))))
      (finally
        (shared/clear-handles! (:session-state @state))
        (u/delete-files dir)))))

(deftest vector-tools-test
  (if (u/windows?)
    (is true)
    (let [dir   (u/tmp-dir (str "datalevin-mcp-vec-" (UUID/randomUUID)))
          kv    (d/open-kv dir)
          _     (d/close-kv kv)
          state (initialized-state {:allow-writes? true})]
      (try
        (let [open-kv       (sut/handle-message
                              state
                              {"jsonrpc" "2.0"
                               "id"      36
                               "method"  "tools/call"
                               "params"  {"name"      "datalevin_open_kv"
                                           "arguments" {"dir" dir}}})
              kv-id         (get-in open-kv ["result" "structuredContent" "kv"])
              open-vector   (sut/handle-message
                              state
                              {"jsonrpc" "2.0"
                               "id"      37
                               "method"  "tools/call"
                               "params"  {"name"      "datalevin_open_vector_index"
                                           "arguments"
                                           {"kv"   kv-id
                                            "opts" {":dimensions" 3}}}})
              vector-id     (get-in open-vector ["result" "structuredContent"
                                                 "vectorIndex"])
              add-v1        (sut/handle-message
                              state
                              {"jsonrpc" "2.0"
                               "id"      38
                               "method"  "tools/call"
                               "params"  {"name"      "datalevin_add_vector"
                                           "arguments"
                                           {"vectorIndex" vector-id
                                            "vectorRef"   "v1"
                                            "vectorData"  [1.0 0.0 0.0]}}})
              add-v2        (sut/handle-message
                              state
                              {"jsonrpc" "2.0"
                               "id"      39
                               "method"  "tools/call"
                               "params"  {"name"      "datalevin_add_vector"
                                           "arguments"
                                           {"vectorIndex" vector-id
                                            "vectorRef"   "v2"
                                            "vectorData"  [0.0 1.0 0.0]}}})
              info-response (sut/handle-message
                              state
                              {"jsonrpc" "2.0"
                               "id"      391
                               "method"  "tools/call"
                               "params"  {"name"      "datalevin_vector_index_info"
                                           "arguments"
                                           {"vectorIndex" vector-id}}})
              indexed-v1    (sut/handle-message
                              state
                              {"jsonrpc" "2.0"
                               "id"      392
                               "method"  "tools/call"
                               "params"  {"name"      "datalevin_vector_indexed"
                                           "arguments"
                                           {"vectorIndex" vector-id
                                            "vectorRef"   "v1"}}})
              search-top1   (sut/handle-message
                              state
                              {"jsonrpc" "2.0"
                               "id"      393
                               "method"  "tools/call"
                               "params"  {"name"      "datalevin_vector_search"
                                           "arguments"
                                           {"vectorIndex" vector-id
                                            "queryVector" [1.0 0.0 0.0]
                                            "opts"        {":top" 1}}}})
              search-dists  (sut/handle-message
                              state
                              {"jsonrpc" "2.0"
                               "id"      394
                               "method"  "tools/call"
                               "params"  {"name"      "datalevin_vector_search"
                                           "arguments"
                                           {"vectorIndex" vector-id
                                            "queryVector" [1.0 0.0 0.0]
                                            "opts"        {":top" 1
                                                           ":display"
                                                           ":refs+dists"}}}})
              remove-v1     (sut/handle-message
                              state
                              {"jsonrpc" "2.0"
                               "id"      395
                               "method"  "tools/call"
                               "params"  {"name"      "datalevin_remove_vector"
                                           "arguments"
                                           {"vectorIndex" vector-id
                                            "vectorRef"   "v1"}}})
              indexed-v1b   (sut/handle-message
                              state
                              {"jsonrpc" "2.0"
                               "id"      396
                               "method"  "tools/call"
                               "params"  {"name"      "datalevin_vector_indexed"
                                           "arguments"
                                           {"vectorIndex" vector-id
                                            "vectorRef"   "v1"}}})
              close-vector  (sut/handle-message
                              state
                              {"jsonrpc" "2.0"
                               "id"      397
                               "method"  "tools/call"
                               "params"  {"name"      "datalevin_close_vector_index"
                                           "arguments"
                                           {"vectorIndex" vector-id}}})
              post-close    (sut/handle-message
                              state
                              {"jsonrpc" "2.0"
                               "id"      398
                               "method"  "tools/call"
                               "params"  {"name"      "datalevin_vector_search"
                                           "arguments"
                                           {"vectorIndex" vector-id
                                            "queryVector" [1.0 0.0 0.0]}}})]
          (is (= false (get-in add-v1 ["result" "isError"] false)))
          (is (= false (get-in add-v2 ["result" "isError"] false)))
          (is (= 2
                 (get-in info-response
                         ["result" "structuredContent" "result" ":size"])))
          (is (= true
                 (get-in indexed-v1 ["result" "structuredContent" "result"])))
          (is (= ["v1"]
                 (get-in search-top1 ["result" "structuredContent" "result"])))
          (is (= "v1"
                 (first (first (get-in search-dists
                                       ["result" "structuredContent"
                                        "result"])))))
          (is (= true
                 (get-in remove-v1 ["result" "structuredContent" "removed"])))
          (is (= false
                 (get-in indexed-v1b ["result" "structuredContent" "result"])))
          (is (= true
                 (get-in close-vector ["result" "structuredContent" "closed"])))
          (is (= true (get-in post-close ["result" "isError"])))
          (is (= ":unknown-vector-index"
                 (get-in post-close ["result" "structuredContent" "code"]))))
        (finally
          (shared/clear-handles! (:session-state @state))
          (u/delete-files dir))))))

(deftest close-kv-closes-dependent-indexes-test
  (if (u/windows?)
    (is true)
    (let [dir   (u/tmp-dir (str "datalevin-mcp-kv-dependents-" (UUID/randomUUID)))
          kv    (d/open-kv dir)
          _     (d/close-kv kv)
          state (initialized-state {:allow-writes? true})]
      (try
        (let [open-kv      (sut/handle-message
                             state
                             {"jsonrpc" "2.0"
                              "id"      3981
                              "method"  "tools/call"
                              "params"  {"name"      "datalevin_open_kv"
                                          "arguments" {"dir" dir}}})
              kv-id        (get-in open-kv ["result" "structuredContent" "kv"])
              open-search  (sut/handle-message
                             state
                             {"jsonrpc" "2.0"
                              "id"      3982
                              "method"  "tools/call"
                              "params"  {"name"      "datalevin_open_search_index"
                                          "arguments" {"kv" kv-id}}})
              search-id    (get-in open-search ["result" "structuredContent"
                                                "searchIndex"])
              open-vector  (sut/handle-message
                             state
                             {"jsonrpc" "2.0"
                              "id"      3983
                              "method"  "tools/call"
                              "params"  {"name"      "datalevin_open_vector_index"
                                          "arguments"
                                          {"kv"   kv-id
                                           "opts" {":dimensions" 3}}}})
              vector-id    (get-in open-vector ["result" "structuredContent"
                                                "vectorIndex"])
              count-open   (sut/handle-message
                             state
                             {"jsonrpc" "2.0"
                              "id"      3984
                              "method"  "tools/call"
                              "params"  {"name"      "datalevin_document_count"
                                          "arguments" {"searchIndex" search-id}}})
              info-open    (sut/handle-message
                             state
                             {"jsonrpc" "2.0"
                              "id"      3985
                              "method"  "tools/call"
                              "params"  {"name"      "datalevin_vector_index_info"
                                          "arguments"
                                          {"vectorIndex" vector-id}}})
              close-kv     (sut/handle-message
                             state
                             {"jsonrpc" "2.0"
                              "id"      3986
                              "method"  "tools/call"
                              "params"  {"name"      "datalevin_close_kv"
                                          "arguments" {"kv" kv-id}}})
              count-closed (sut/handle-message
                             state
                             {"jsonrpc" "2.0"
                              "id"      3987
                              "method"  "tools/call"
                              "params"  {"name"      "datalevin_document_count"
                                          "arguments" {"searchIndex" search-id}}})
              info-closed  (sut/handle-message
                             state
                             {"jsonrpc" "2.0"
                              "id"      3988
                              "method"  "tools/call"
                              "params"  {"name"      "datalevin_vector_index_info"
                                          "arguments"
                                          {"vectorIndex" vector-id}}})
              close-search (sut/handle-message
                             state
                             {"jsonrpc" "2.0"
                              "id"      3989
                              "method"  "tools/call"
                              "params"  {"name"      "datalevin_close_search_index"
                                          "arguments"
                                          {"searchIndex" search-id}}})
              close-vector (sut/handle-message
                             state
                             {"jsonrpc" "2.0"
                              "id"      3990
                              "method"  "tools/call"
                              "params"  {"name"      "datalevin_close_vector_index"
                                          "arguments"
                                          {"vectorIndex" vector-id}}})]
          (is (= 0
                 (get-in count-open ["result" "structuredContent" "result"])))
          (is (= 0
                 (get-in info-open ["result" "structuredContent" "result"
                                    ":size"])))
          (is (= true
                 (get-in close-kv ["result" "structuredContent" "closed"])))
          (is (= true (get-in count-closed ["result" "isError"])))
          (is (= ":unknown-search-index"
                 (get-in count-closed ["result" "structuredContent" "code"])))
          (is (= true (get-in info-closed ["result" "isError"])))
          (is (= ":unknown-vector-index"
                 (get-in info-closed ["result" "structuredContent" "code"])))
          (is (= true (get-in close-search ["result" "isError"])))
          (is (= ":unknown-search-index"
                 (get-in close-search ["result" "structuredContent" "code"])))
          (is (= true (get-in close-vector ["result" "isError"])))
          (is (= ":unknown-vector-index"
                 (get-in close-vector ["result" "structuredContent" "code"]))))
        (finally
          (shared/clear-handles! (:session-state @state))
          (u/delete-files dir))))))

(deftest remote-uri-tools-test
  (tdc/server-fixture
    (fn []
      (let [db-uri (str "dtlv://datalevin:datalevin@localhost/mcp-remote-db-"
                        (UUID/randomUUID))
            kv-uri (str "dtlv://datalevin:datalevin@localhost/mcp-remote-kv-"
                        (UUID/randomUUID))
            state  (initialized-state {:allow-writes? true})]
        (try
          (let [open-db
                (sut/handle-message
                  state
                  {"jsonrpc" "2.0"
                   "id"      399
                   "method"  "tools/call"
                   "params"  {"name"      "datalevin_open_database"
                               "arguments" {"uri"    db-uri
                                            "create" true
                                            "schema" {":name"
                                                      {":db/valueType"
                                                       ":db.type/string"}}}}})
                database-id (get-in open-db ["result" "structuredContent"
                                             "database"])
                tx-db
                (sut/handle-message
                  state
                  {"jsonrpc" "2.0"
                   "id"      3991
                   "method"  "tools/call"
                   "params"  {"name"      "datalevin_transact"
                               "arguments" {"database" database-id
                                            "txData"   [{":db/id" -1
                                                         ":name"  "Alice"}]}}})
                query-db
                (sut/handle-message
                  state
                  {"jsonrpc" "2.0"
                   "id"      3992
                   "method"  "tools/call"
                   "params"  {"name"      "datalevin_query"
                               "arguments" {"database" database-id
                                            "query"
                                            "[:find ?n :where [?e :name ?n]]"}}})
                open-kv
                (sut/handle-message
                  state
                  {"jsonrpc" "2.0"
                   "id"      3993
                   "method"  "tools/call"
                   "params"  {"name"      "datalevin_open_kv"
                               "arguments" {"uri" kv-uri}}})
                kv-id (get-in open-kv ["result" "structuredContent" "kv"])
                tx-kv
                (sut/handle-message
                  state
                  {"jsonrpc" "2.0"
                   "id"      3994
                   "method"  "tools/call"
                   "params"  {"name"      "datalevin_kv_transact"
                               "arguments" {"kv"  kv-id
                                            "dbi" "items"
                                            "txs" [["put" "a" "alpha"]]}}})
                get-kv
                (sut/handle-message
                  state
                  {"jsonrpc" "2.0"
                   "id"      3995
                   "method"  "tools/call"
                   "params"  {"name"      "datalevin_kv_get"
                               "arguments" {"kv"  kv-id
                                            "dbi" "items"
                                            "key" "a"}}})]
            (is (= true
                   (get-in open-db ["result" "structuredContent" "remote"])))
            (is (= true
                   (get-in open-kv ["result" "structuredContent" "remote"])))
            (is (= false (get-in tx-db ["result" "isError"] false)))
            (is (str/includes? (get-in query-db ["result" "content" 0 "text"])
                               "Alice"))
            (is (= ":transacted"
                   (get-in tx-kv ["result" "structuredContent" "result"])))
            (is (= "alpha"
                   (get-in get-kv ["result" "structuredContent" "result"]))))
          (finally
            (shared/clear-handles! (:session-state @state))))))))

(deftest write-tools-disabled-by-default-test
  (let [dir   (u/tmp-dir (str "datalevin-mcp-write-off-" (UUID/randomUUID)))
        conn  (d/create-conn dir {:name {:db/valueType :db.type/string}})
        _     (d/close conn)
        kvdir (u/tmp-dir (str "datalevin-mcp-kv-write-off-" (UUID/randomUUID)))
        kv    (d/open-kv kvdir)
        _     (d/open-dbi kv "items")
        _     (d/close-kv kv)
        state (initialized-state)]
    (try
      (let [open-db     (sut/handle-message
                          state
                          {"jsonrpc" "2.0"
                           "id"      40
                           "method"  "tools/call"
                           "params"  {"name"      "datalevin_open_database"
                                       "arguments" {"dir" dir}}})
            database-id (get-in open-db ["result" "structuredContent" "database"])
            tx-db       (sut/handle-message
                          state
                          {"jsonrpc" "2.0"
                           "id"      41
                           "method"  "tools/call"
                           "params"  {"name"      "datalevin_transact"
                                       "arguments"
                                       {"database" database-id
                                        "txData"   [{":db/id" -1
                                                     ":name"  "Alice"}]}}})
            open-kv     (sut/handle-message
                          state
                          {"jsonrpc" "2.0"
                           "id"      42
                           "method"  "tools/call"
                           "params"  {"name"      "datalevin_open_kv"
                                       "arguments" {"dir" kvdir}}})
            kv-id       (get-in open-kv ["result" "structuredContent" "kv"])
            open-search (sut/handle-message
                          state
                          {"jsonrpc" "2.0"
                           "id"      421
                           "method"  "tools/call"
                           "params"  {"name"      "datalevin_open_search_index"
                                       "arguments" {"kv" kv-id}}})
            search-id   (get-in open-search ["result" "structuredContent"
                                             "searchIndex"])
            add-doc     (sut/handle-message
                          state
                          {"jsonrpc" "2.0"
                           "id"      422
                           "method"  "tools/call"
                           "params"  {"name"      "datalevin_add_document"
                                       "arguments"
                                       {"searchIndex" search-id
                                        "docRef"      "doc-1"
                                        "docText"     "pizza"}}})
            tx-kv       (sut/handle-message
                          state
                          {"jsonrpc" "2.0"
                           "id"      43
                           "method"  "tools/call"
                           "params"  {"name"      "datalevin_kv_transact"
                                       "arguments"
                                       {"kv"  kv-id
                                        "dbi" "items"
                                        "txs" [["put" "a" "alpha"]]}}})]
        (is (= true (get-in tx-db ["result" "isError"])))
        (is (= ":writes-disabled"
               (get-in tx-db ["result" "structuredContent" "code"])))
        (is (= true (get-in add-doc ["result" "isError"])))
        (is (= ":writes-disabled"
               (get-in add-doc ["result" "structuredContent" "code"])))
        (is (= true (get-in tx-kv ["result" "isError"])))
        (is (= ":writes-disabled"
               (get-in tx-kv ["result" "structuredContent" "code"]))))
      (finally
        (shared/clear-handles! (:session-state @state))
        (u/delete-files dir)
        (u/delete-files kvdir)))))

(deftest write-tools-enabled-test
  (let [dir   (u/tmp-dir (str "datalevin-mcp-write-on-" (UUID/randomUUID)))
        kvdir (u/tmp-dir (str "datalevin-mcp-kv-write-on-" (UUID/randomUUID)))
        state (initialized-state {:allow-writes? true})]
    (try
      (let [open-db       (sut/handle-message
                            state
                            {"jsonrpc" "2.0"
                             "id"      50
                             "method"  "tools/call"
                             "params"  {"name"      "datalevin_open_database"
                                         "arguments" {"dir"    dir
                                                      "create" true
                                                      "schema" {":name" {":db/valueType"
                                                                         ":db.type/string"}}}}})
            database-id   (get-in open-db ["result" "structuredContent" "database"])
            tx-db         (sut/handle-message
                            state
                            {"jsonrpc" "2.0"
                             "id"      51
                             "method"  "tools/call"
                             "params"  {"name"      "datalevin_transact"
                                         "arguments"
                                         {"database" database-id
                                          "txData"   [{":db/id" -1
                                                       ":name"  "Alice"}]}}})
            query-db      (sut/handle-message
                            state
                            {"jsonrpc" "2.0"
                             "id"      52
                             "method"  "tools/call"
                             "params"  {"name"      "datalevin_query"
                                         "arguments"
                                         {"database" database-id
                                          "query"    "[:find ?n :where [?e :name ?n]]"}}})
            open-kv       (sut/handle-message
                            state
                            {"jsonrpc" "2.0"
                             "id"      53
                             "method"  "tools/call"
                             "params"  {"name"      "datalevin_open_kv"
                                         "arguments" {"dir" kvdir}}})
            kv-id         (get-in open-kv ["result" "structuredContent" "kv"])
            tx-kv         (sut/handle-message
                            state
                            {"jsonrpc" "2.0"
                             "id"      54
                             "method"  "tools/call"
                             "params"  {"name"      "datalevin_kv_transact"
                                         "arguments"
                                         {"kv"  kv-id
                                          "dbi" "items"
                                          "txs" [["put" "a" "alpha"]
                                                 ["put" "b" "beta"]]}}})
            get-kv        (sut/handle-message
                            state
                            {"jsonrpc" "2.0"
                             "id"      55
                             "method"  "tools/call"
                             "params"  {"name"      "datalevin_kv_get"
                                         "arguments"
                                         {"kv"  kv-id
                                          "dbi" "items"
                                          "key" "b"}}})
            api-info      (sut/handle-message
                            state
                            {"jsonrpc" "2.0"
                             "id"      56
                             "method"  "tools/call"
                             "params"  {"name"      "datalevin_api_info"
                                         "arguments" {}}})]
        (is (= false (get-in tx-db ["result" "isError"] false)))
        (is (map? (get-in tx-db ["result" "structuredContent" "result"])))
        (is (str/includes? (get-in query-db ["result" "content" 0 "text"])
                           "Alice"))
        (is (= ":transacted"
               (get-in tx-kv ["result" "structuredContent" "result"])))
        (is (= "beta"
               (get-in get-kv ["result" "structuredContent" "result"])))
        (is (= true
               (get-in api-info ["result" "structuredContent" "mcp"
                                 "allowWrites"])))
        (is (= true (:allow-writes? @state))))
      (finally
        (shared/clear-handles! (:session-state @state))
        (u/delete-files dir)
        (u/delete-files kvdir)))))

(deftest datalog-write-failure-rolls-back-test
  (let [dir   (u/tmp-dir (str "datalevin-mcp-write-rollback-" (UUID/randomUUID)))
        conn  (d/create-conn dir {:name    {:db/valueType :db.type/string}
                                  :counter {:db/valueType :db.type/long}})
        report (d/transact! conn [{:db/id -1
                                   :name "Alice"
                                   :counter 0}])
        alice (get (:tempids report) -1)
        _     (d/close conn)
        state (initialized-state {:allow-writes? true})]
    (try
      (let [open-db     (sut/handle-message
                          state
                          {"jsonrpc" "2.0"
                           "id"      60
                           "method"  "tools/call"
                           "params"  {"name"      "datalevin_open_database"
                                       "arguments" {"dir" dir}}})
            database-id (get-in open-db ["result" "structuredContent" "database"])
            failed-tx   (sut/handle-message
                          state
                          {"jsonrpc" "2.0"
                           "id"      61
                           "method"  "tools/call"
                           "params"  {"name"      "datalevin_transact"
                                       "arguments"
                                       {"database" database-id
                                        "txData"   [[":db/add" alice ":counter" 1]
                                                    [":db/cas" alice ":counter" 999 2]]}}})
            counter-q   (sut/handle-message
                          state
                          {"jsonrpc" "2.0"
                           "id"      62
                           "method"  "tools/call"
                           "params"  {"name"      "datalevin_query"
                                       "arguments"
                                       {"database" database-id
                                        "query"
                                        "[:find ?c . :in $ ?e :where [?e :counter ?c]]"
                                        "inputs" [alice]}}})]
        (is (= true (get-in failed-tx ["result" "isError"])))
        (is (= 0
               (get-in counter-q ["result" "structuredContent" "result"]))))
      (finally
        (shared/clear-handles! (:session-state @state))
        (u/delete-files dir)))))

(deftest kv-write-failure-rolls-back-test
  (let [dir   (u/tmp-dir (str "datalevin-mcp-kv-write-rollback-" (UUID/randomUUID)))
        kv    (d/open-kv dir)
        _     (d/open-dbi kv "items")
        _     (d/close-kv kv)
        state (initialized-state {:allow-writes? true})]
    (try
      (let [open-kv   (sut/handle-message
                        state
                        {"jsonrpc" "2.0"
                         "id"      70
                         "method"  "tools/call"
                         "params"  {"name"      "datalevin_open_kv"
                                     "arguments" {"dir" dir}}})
            kv-id     (get-in open-kv ["result" "structuredContent" "kv"])
            failed-tx (sut/handle-message
                        state
                        {"jsonrpc" "2.0"
                         "id"      71
                         "method"  "tools/call"
                         "params"  {"name"      "datalevin_kv_transact"
                                     "arguments"
                                     {"kv"  kv-id
                                      "dbi" "items"
                                      "txs" [["put" "ok" "good"]
                                             ["put" nil "bad"]]}}})
            get-ok    (sut/handle-message
                        state
                        {"jsonrpc" "2.0"
                         "id"      72
                         "method"  "tools/call"
                         "params"  {"name"      "datalevin_kv_get"
                                     "arguments"
                                     {"kv"  kv-id
                                      "dbi" "items"
                                      "key" "ok"}}})]
        (is (= true (get-in failed-tx ["result" "isError"])))
        (is (nil? (get-in get-ok ["result" "structuredContent" "result"]))))
      (finally
        (shared/clear-handles! (:session-state @state))
        (u/delete-files dir)))))

(deftest serve-stdio-loop-test
  (let [messages [(jc/write-json-ready-string
                    {"jsonrpc" "2.0"
                     "id"      1
                     "method"  "initialize"
                     "params"  default-initialize-params})
                  (jc/write-json-ready-string
                    {"jsonrpc" "2.0"
                     "method"  "notifications/initialized"
                     "params"  {}})
                  (jc/write-json-ready-string
                    {"jsonrpc" "2.0"
                     "id"      2
                     "method"  "tools/list"
                     "params"  {}})]
        reader   (StringReader. (str (str/join "\n" messages) "\n"))
        writer   (StringWriter.)]
    (sut/serve reader writer)
    (let [responses (mapv jc/read-json-string
                          (remove str/blank? (str/split-lines (str writer))))]
      (is (= [1 2] (mapv #(get % "id") responses)))
      (is (= "2025-06-18"
             (get-in (first responses) ["result" "protocolVersion"])))
      (is (= expected-tool-names
             (mapv #(get % "name")
                   (get-in (second responses) ["result" "tools"])))))))

(deftest batch-request-test
  (let [state    (new-state)
        response (sut/handle-input
                   state
                   [{"jsonrpc" "2.0"
                     "id"      20
                     "method"  "initialize"
                     "params"  default-initialize-params}
                    {"jsonrpc" "2.0"
                     "id"      21
                     "method"  "tools/list"
                     "params"  {}}])]
    (is (= -32600 (get-in response ["error" "code"])))
    (is (= "JSON-RPC batch requests are not supported."
           (get-in response ["error" "message"])))
    (is (nil? (get response "id")))))
