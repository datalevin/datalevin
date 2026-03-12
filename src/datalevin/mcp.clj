;;
;; Copyright (c) Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns datalevin.mcp
  "Minimal MCP stdio server for Datalevin."
  (:require
   [clojure.string :as str]
   [clojure.walk :as walk]
   [datalevin.constants :as c]
   [datalevin.json-api.shared :as shared]
   [datalevin.json-convert :as jc])
  (:import
   [java.io BufferedReader BufferedWriter InputStreamReader OutputStreamWriter
    Reader Writer]
   [java.util UUID]))

(def ^:const protocol-version
  "2025-06-18")

(def ^:private empty-object-schema
  {"type"                 "object"
   "properties"           {}
   "additionalProperties" false})

(defn- object-schema
  [properties required]
  (cond-> {"type"                 "object"
           "properties"           properties
           "additionalProperties" false}
    (seq required) (assoc "required" required)))

(def ^:private json-value-schema
  {})

(def default-mcp-limits
  {:max-result-items    200
   :max-response-bytes  524288
   :preview-items       5
   :preview-map-entries 20
   :preview-chars       1024})

(def ^:dynamic *mcp-limits*
  default-mcp-limits)

(def ^:private tool-specs
  {"datalevin_api_info"
   {"name"        "datalevin_api_info"
    "description" "Return Datalevin JSON API and MCP server capability info."
    "inputSchema" empty-object-schema}

   "datalevin_open_database"
   {"name"        "datalevin_open_database"
    "description" "Open a local path or remote dtlv:// Datalog database for this MCP session."
    "inputSchema" (object-schema
                    {"dir"        {"type" "string"}
                     "uri"        {"type" "string"}
                     "create"     {"type" "boolean"}
                     "schema"     json-value-schema
                     "opts"       json-value-schema
                     "clientOpts" json-value-schema}
                    [])}

   "datalevin_close_database"
   {"name"        "datalevin_close_database"
    "description" "Close a previously opened Datalevin database."
    "inputSchema" (object-schema
                    {"database" {"type" "string"}}
                    ["database"])}

   "datalevin_open_kv"
   {"name"        "datalevin_open_kv"
    "description" "Open a local path or remote dtlv:// Datalevin KV store for this MCP session."
    "inputSchema" (object-schema
                    {"dir"        {"type" "string"}
                     "uri"        {"type" "string"}
                     "opts"       json-value-schema
                     "clientOpts" json-value-schema}
                    [])}

   "datalevin_close_kv"
   {"name"        "datalevin_close_kv"
    "description" "Close a previously opened Datalevin KV store."
    "inputSchema" (object-schema
                    {"kv" {"type" "string"}}
                    ["kv"])}

   "datalevin_open_search_index"
   {"name"        "datalevin_open_search_index"
    "description" "Open a text search index backed by an opened Datalevin KV store for this MCP session."
    "inputSchema" (object-schema
                    {"kv"   {"type" "string"}
                     "opts" json-value-schema}
                    ["kv"])}

   "datalevin_close_search_index"
   {"name"        "datalevin_close_search_index"
    "description" "Close a previously opened Datalevin search index."
    "inputSchema" (object-schema
                    {"searchIndex" {"type" "string"}}
                    ["searchIndex"])}

   "datalevin_add_document"
   {"name"        "datalevin_add_document"
    "description" "Add one document to an opened Datalevin search index. Disabled unless writes are enabled at startup."
    "inputSchema" (object-schema
                    {"searchIndex" {"type" "string"}
                     "docRef"      json-value-schema
                     "docText"     {"type" "string"}
                     "checkExist"  {"type" "boolean"}}
                    ["searchIndex" "docRef" "docText"])}

   "datalevin_remove_document"
   {"name"        "datalevin_remove_document"
    "description" "Remove one document from an opened Datalevin search index. Disabled unless writes are enabled at startup."
    "inputSchema" (object-schema
                    {"searchIndex" {"type" "string"}
                     "docRef"      json-value-schema}
                    ["searchIndex" "docRef"])}

   "datalevin_clear_documents"
   {"name"        "datalevin_clear_documents"
    "description" "Remove all documents from an opened Datalevin search index. Disabled unless writes are enabled at startup."
    "inputSchema" (object-schema
                    {"searchIndex" {"type" "string"}}
                    ["searchIndex"])}

   "datalevin_document_indexed"
   {"name"        "datalevin_document_indexed"
    "description" "Check whether a document reference is present in an opened Datalevin search index."
    "inputSchema" (object-schema
                    {"searchIndex" {"type" "string"}
                     "docRef"      json-value-schema}
                    ["searchIndex" "docRef"])}

   "datalevin_document_count"
   {"name"        "datalevin_document_count"
    "description" "Return the number of indexed documents in an opened Datalevin search index."
    "inputSchema" (object-schema
                    {"searchIndex" {"type" "string"}}
                    ["searchIndex"])}

   "datalevin_search_documents"
   {"name"        "datalevin_search_documents"
    "description" "Search an opened Datalevin search index."
    "inputSchema" (object-schema
                    {"searchIndex" {"type" "string"}
                     "query"       {"type" "string"}
                     "opts"        json-value-schema}
                    ["searchIndex" "query"])}

   "datalevin_open_vector_index"
   {"name"        "datalevin_open_vector_index"
    "description" "Open a vector index backed by an opened Datalevin KV store for this MCP session."
    "inputSchema" (object-schema
                    {"kv"   {"type" "string"}
                     "opts" json-value-schema}
                    ["kv" "opts"])}

   "datalevin_close_vector_index"
   {"name"        "datalevin_close_vector_index"
    "description" "Close a previously opened Datalevin vector index."
    "inputSchema" (object-schema
                    {"vectorIndex" {"type" "string"}}
                    ["vectorIndex"])}

   "datalevin_vector_index_info"
   {"name"        "datalevin_vector_index_info"
    "description" "Return info about an opened Datalevin vector index."
    "inputSchema" (object-schema
                    {"vectorIndex" {"type" "string"}}
                    ["vectorIndex"])}

   "datalevin_add_vector"
   {"name"        "datalevin_add_vector"
    "description" "Add one vector to an opened Datalevin vector index. Disabled unless writes are enabled at startup."
    "inputSchema" (object-schema
                    {"vectorIndex" {"type" "string"}
                     "vectorRef"   json-value-schema
                     "vectorData"  {"type" "array"}}
                    ["vectorIndex" "vectorRef" "vectorData"])}

   "datalevin_remove_vector"
   {"name"        "datalevin_remove_vector"
    "description" "Remove one vector from an opened Datalevin vector index. Disabled unless writes are enabled at startup."
    "inputSchema" (object-schema
                    {"vectorIndex" {"type" "string"}
                     "vectorRef"   json-value-schema}
                    ["vectorIndex" "vectorRef"])}

   "datalevin_vector_indexed"
   {"name"        "datalevin_vector_indexed"
    "description" "Check whether a vector reference is present in an opened Datalevin vector index."
    "inputSchema" (object-schema
                    {"vectorIndex" {"type" "string"}
                     "vectorRef"   json-value-schema}
                    ["vectorIndex" "vectorRef"])}

   "datalevin_vector_search"
   {"name"        "datalevin_vector_search"
    "description" "Search an opened Datalevin vector index."
    "inputSchema" (object-schema
                    {"vectorIndex" {"type" "string"}
                     "queryVector" {"type" "array"}
                     "opts"        json-value-schema}
                    ["vectorIndex" "queryVector"])}

   "datalevin_kv_get"
   {"name"        "datalevin_kv_get"
    "description" "Get one value from a named DBI in an opened Datalevin KV store."
    "inputSchema" (object-schema
                    {"kv"        {"type" "string"}
                     "dbi"       {"type" "string"}
                     "key"       json-value-schema
                     "keyType"   {"type" "string"}
                     "valueType" {"type" "string"}
                     "returnPair" {"type" "boolean"}}
                    ["kv" "dbi" "key"])}

   "datalevin_kv_range"
   {"name"        "datalevin_kv_range"
    "description" "Read a paged key range from a named DBI in an opened Datalevin KV store."
    "inputSchema" (object-schema
                    {"kv"        {"type" "string"}
                     "dbi"       {"type" "string"}
                     "keyRange"  {"type" "array"}
                     "keyType"   {"type" "string"}
                     "valueType" {"type" "string"}
                     "limit"     {"type" "integer"}
                     "offset"    {"type" "integer"}}
                    ["kv" "dbi" "keyRange"])}

   "datalevin_kv_transact"
   {"name"        "datalevin_kv_transact"
    "description" "Execute KV write operations against an opened Datalevin KV store. Disabled unless writes are enabled at startup."
    "inputSchema" (object-schema
                    {"kv"        {"type" "string"}
                     "dbi"       {"type" "string"}
                     "txs"       {"type" "array"}
                     "keyType"   {"type" "string"}
                     "valueType" {"type" "string"}}
                    ["kv" "txs"])}

   "datalevin_query"
   {"name"        "datalevin_query"
    "description" "Run a Datalog query against an opened Datalevin database."
    "inputSchema" (object-schema
                    {"database" {"type" "string"}
                     "query"    {"type" "string"}
                     "inputs"   {"type" "array"}}
                    ["database" "query"])}

   "datalevin_datoms"
   {"name"        "datalevin_datoms"
    "description" "Read a paged Datalevin datom index range from an opened database."
    "inputSchema" (object-schema
                    {"database" {"type" "string"}
                     "index"    {"type" "string"}
                     "c1"       json-value-schema
                     "c2"       json-value-schema
                     "c3"       json-value-schema
                     "limit"    {"type" "integer"}
                     "offset"   {"type" "integer"}}
                    ["database" "index"])}

   "datalevin_search_datoms"
   {"name"        "datalevin_search_datoms"
    "description" "Search datoms by entity, attribute, and/or value in an opened database."
    "inputSchema" (object-schema
                    {"database" {"type" "string"}
                     "e"        json-value-schema
                     "a"        json-value-schema
                     "v"        json-value-schema
                     "limit"    {"type" "integer"}
                     "offset"   {"type" "integer"}}
                    ["database"])}

   "datalevin_count_datoms"
   {"name"        "datalevin_count_datoms"
    "description" "Count datoms by entity, attribute, and/or value in an opened database."
    "inputSchema" (object-schema
                    {"database" {"type" "string"}
                     "e"        json-value-schema
                     "a"        json-value-schema
                     "v"        json-value-schema}
                    ["database"])}

   "datalevin_pull"
   {"name"        "datalevin_pull"
    "description" "Pull one entity from an opened Datalevin database."
    "inputSchema" (object-schema
                    {"database" {"type" "string"}
                     "selector" json-value-schema
                     "eid"      json-value-schema}
                    ["database" "selector" "eid"])}

   "datalevin_entity"
   {"name"        "datalevin_entity"
    "description" "Load one entity from an opened Datalevin database."
    "inputSchema" (object-schema
                    {"database" {"type" "string"}
                     "eid"      json-value-schema}
                    ["database" "eid"])}

   "datalevin_pull_many"
   {"name"        "datalevin_pull_many"
    "description" "Pull multiple entities from an opened Datalevin database."
    "inputSchema" (object-schema
                    {"database" {"type" "string"}
                     "selector" json-value-schema
                     "eids"     {"type" "array"}}
                    ["database" "selector" "eids"])}

   "datalevin_transact"
   {"name"        "datalevin_transact"
    "description" "Execute Datalog transaction data against an opened Datalevin database. Disabled unless writes are enabled at startup."
    "inputSchema" (object-schema
                    {"database" {"type" "string"}
                     "txData"   {"type" "array"}
                     "txMeta"   json-value-schema}
                    ["database" "txData"])}

   "datalevin_fulltext_datoms"
   {"name"        "datalevin_fulltext_datoms"
    "description" "Run a fulltext query against an opened database."
    "inputSchema" (object-schema
                    {"database" {"type" "string"}
                     "query"    {"type" "string"}
                     "opts"     json-value-schema}
                    ["database" "query"])}})

(defn- gen-id
  [prefix]
  (str prefix "-" (UUID/randomUUID)))


(defn- jsonrpc-error
  ([id code message]
   (jsonrpc-error id code message nil))
  ([id code message data]
   (cond-> {"jsonrpc" "2.0"
            "id"      id
            "error"   {"code"    code
                        "message" message}}
     (some? data) (assoc-in ["error" "data"] (jc/json-ready data)))))

(defn- jsonrpc-result
  [id result]
  {"jsonrpc" "2.0"
   "id"      id
   "result"  result})

(defn- valid-request-id?
  [id]
  (or (string? id)
      (number? id)))

(defn- result-collection
  [x]
  (or (vector? x)
      (list? x)
      (set? x)
      (seq? x)
      (instance? java.util.Collection x)))

(defn- result-size
  [x]
  (cond
    (map? x) (count x)
    (instance? java.util.Map x) (.size ^java.util.Map x)
    (result-collection x) (count x)
    :else nil))

(defn- take-result-items
  [x n]
  (cond
    (vector? x) (vec (take n x))
    (list? x) (vec (take n x))
    (set? x) (vec (take n x))
    (seq? x) (vec (take n x))
    (instance? java.util.Collection x) (vec (take n x))
    :else x))

(defn- preview-result
  [x]
  (let [{:keys [preview-items preview-map-entries preview-chars]} *mcp-limits*]
    (cond
      (string? x)
      (if (and preview-chars (> (count x) preview-chars))
        (str (subs x 0 preview-chars) "...")
        x)

      (map? x)
      (into {} (take preview-map-entries x))

      (instance? java.util.Map x)
      (into {} (take preview-map-entries (into {} x)))

      (result-collection x)
      (take-result-items x preview-items)

      :else x)))

(defn- add-truncation
  [structured truncation]
  (-> structured
      (update "meta" #(merge {"truncated" true} (or % {})))
      (update-in ["meta" "truncations"] (fnil conj []) truncation)))

(defn- result-byte-truncation?
  [truncation]
  (and (= "bytes" (get truncation "kind"))
       (= "result" (get truncation "path"))))

(defn- set-result-byte-truncation
  [structured mode]
  (let [limit      (:max-response-bytes *mcp-limits*)
        truncation {"kind"  "bytes"
                    "path"  "result"
                    "limit" limit
                    "mode"  mode}]
    (-> structured
        (update "meta" #(merge {"truncated" true} (or % {})))
        (update-in ["meta" "truncations"]
                   (fn [truncations]
                     (let [truncations (vec (or truncations []))]
                       (if (some result-byte-truncation? truncations)
                         (mapv #(if (result-byte-truncation? %)
                                  truncation
                                  %)
                               truncations)
                         (conj truncations truncation))))))))

(defn- fits-byte-limit?
  [value]
  (letfn [(result-too-large? [^Throwable t]
            (loop [e t]
              (when e
                (let [data (ex-data e)]
                  (if (= :result-too-large (:code data))
                    true
                    (recur (.getCause e)))))))]
    (try
      (jc/write-json-ready-string value *mcp-limits*)
      true
      (catch Throwable t
        (if (result-too-large? t)
          false
          (throw t))))))

(defn- maybe-truncate-result-items
  [structured]
  (let [limit  (:max-result-items *mcp-limits*)
        result (get structured "result")
        size   (result-size result)]
    (if (and limit
             (result-collection result)
             size
             (> size limit))
      (-> structured
          (assoc "result" (take-result-items result limit))
          (add-truncation {"kind"     "items"
                           "path"     "result"
                           "limit"    limit
                           "returned" limit
                           "original" size}))
      structured)))

(defn- maybe-truncate-result-bytes
  [structured]
  (if (fits-byte-limit? structured)
    structured
    (let [limit         (:max-response-bytes *mcp-limits*)
          current       (get structured "result")
          preview       (preview-result current)
          previewed     (-> structured
                            (assoc "result" preview)
                            (add-truncation {"kind"  "bytes"
                                             "path"  "result"
                                             "limit" limit
                                             "mode"  "preview"}))]
      (if (fits-byte-limit? previewed)
        previewed
        (-> structured
            (assoc "result" nil)
            (add-truncation {"kind"  "bytes"
                             "path"  "result"
                             "limit" limit
                             "mode"  "omitted"}))))))

(defn- prepare-tool-structured
  [structured]
  (if (contains? structured "result")
    (-> structured
        maybe-truncate-result-items
        maybe-truncate-result-bytes)
    structured))

(defn- tool-summary
  [value summary]
  (cond-> {"summary" summary}
    (seq (get-in value ["meta" "truncations"]))
    (assoc "truncations" (get-in value ["meta" "truncations"]))))

(defn- tool-text
  [value]
  (if (seq (get-in value ["meta" "truncations"]))
    (jc/write-json-ready-string
      (tool-summary value "Result truncated.")
      *mcp-limits*)
    (jc/write-json-ready-string value *mcp-limits*)))

(defn- compact-tool-text
  [value is-error]
  (cond
    is-error "Tool error. See structuredContent."
    (seq (get-in value ["meta" "truncations"])) "Result truncated."
    :else "See structuredContent."))

(defn- tool-envelope
  [structured text is-error]
  (cond-> {"content"           [{"type" "text"
                                 "text" text}]
           "structuredContent" structured}
    is-error (assoc "isError" true)))

(defn- fit-tool-envelope
  [structured is-error wrap-fn]
  (let [full-response (tool-envelope structured (tool-text structured) is-error)]
    (if (fits-byte-limit? (wrap-fn full-response))
      full-response
      (let [compact-response (tool-envelope structured
                                            (compact-tool-text structured is-error)
                                            is-error)]
        (when (fits-byte-limit? (wrap-fn compact-response))
          compact-response)))))

(defn- preview-tool-structured
  [structured]
  (let [current (get structured "result")
        preview (preview-result current)]
    (when (not= preview current)
      (-> structured
          (assoc "result" preview)
          (set-result-byte-truncation "preview")))))

(defn- omit-tool-structured
  [structured]
  (-> structured
      (assoc "result" nil)
      (set-result-byte-truncation "omitted")))

(defn- tool-response
  ([structured]
   (tool-response structured false identity))
  ([structured is-error]
   (tool-response structured is-error identity))
  ([structured is-error wrap-fn]
   (let [structured (if is-error
                      (jc/json-ready structured)
                      (prepare-tool-structured structured))]
     (or (fit-tool-envelope structured is-error wrap-fn)
         (when (and (not is-error) (contains? structured "result"))
           (or (when-let [previewed (preview-tool-structured structured)]
                 (fit-tool-envelope previewed false wrap-fn))
               (fit-tool-envelope (omit-tool-structured structured)
                                  false
                                  wrap-fn)))
         (throw (ex-info "Result exceeds max-response-bytes limit."
                         {:code  :result-too-large
                          :kind  :bytes
                          :limit (:max-response-bytes *mcp-limits*)}))))))

(defn- require-string
  [x field]
  (if (string? x)
    x
    (throw (ex-info (str field " must be a string.")
                    {:code  :invalid-params
                     :field field}))))

(defn- require-map
  [x field]
  (cond
    (nil? x) {}
    (map? x) x
    (instance? java.util.Map x) (into {} x)
    :else
    (throw (ex-info (str field " must be an object.")
                    {:code  :invalid-params
                     :field field}))))

(defn- require-vector
  [x field]
  (if (vector? x)
    x
    (throw (ex-info (str field " must be an array.")
                    {:code  :invalid-params
                     :field field}))))

(defn- require-boolean
  [x field]
  (if (instance? Boolean x)
    x
    (throw (ex-info (str field " must be a boolean.")
                    {:code  :invalid-params
                     :field field}))))

(defn- require-present
  [m field]
  (if (contains? m field)
    (get m field)
    (throw (ex-info (str field " is required.")
                    {:code  :invalid-params
                     :field field}))))

(defn- require-request-id
  [request]
  (cond
    (not (contains? request "id"))
    (throw (ex-info "id is required for requests."
                    {:code  :invalid-request
                     :field "id"}))

    (not (valid-request-id? (get request "id")))
    (throw (ex-info "id must be a string or number."
                    {:code  :invalid-request
                     :field "id"}))

    :else
    (get request "id")))

(defn- require-initialize-params
  [params]
  (let [protocol-version (require-string
                           (require-present params "protocolVersion")
                           "protocolVersion")
        capabilities     (require-map
                           (require-present params "capabilities")
                           "capabilities")
        client-info      (require-map
                           (require-present params "clientInfo")
                           "clientInfo")
        client-name      (require-string
                           (require-present client-info "name")
                           "clientInfo.name")
        client-version   (require-string
                           (require-present client-info "version")
                           "clientInfo.version")]
    {"protocolVersion" protocol-version
     "capabilities"    capabilities
     "clientInfo"      (cond-> {"name"    client-name
                                "version" client-version}
                         (contains? client-info "title")
                         (assoc "title" (require-string
                                          (get client-info "title")
                                          "clientInfo.title")))}))

(defn- keyword-marker-string?
  [x]
  (and (string? x)
       (> (count x) 1)
       (str/starts-with? x ":")))

(defn- decode-datavalue
  [x]
  (walk/postwalk
    (fn [value]
      (if (keyword-marker-string? value)
        (keyword (subs value 1))
        value))
    x))

(defn- remote-target?
  [target]
  (and (string? target)
       (str/starts-with? target "dtlv://")))

(defn- require-open-target
  [arguments]
  (let [dir? (contains? arguments "dir")
        uri? (contains? arguments "uri")]
    (when (= dir? uri?)
      (throw (ex-info "Exactly one of dir or uri must be provided."
                      {:code :invalid-params
                       :field "dir|uri"})))
    (let [target (require-string (get arguments (if dir? "dir" "uri"))
                                 (if dir? "dir" "uri"))]
      {:target target
       :remote (remote-target? target)})))

(defn- merge-open-opts
  [arguments]
  (let [opts        (when (contains? arguments "opts")
                      (decode-datavalue
                        (require-map (get arguments "opts") "opts")))
        client-opts (when (contains? arguments "clientOpts")
                      (decode-datavalue
                        (require-map (get arguments "clientOpts")
                                     "clientOpts")))]
    (cond
      (and opts client-opts) (assoc opts :client-opts client-opts)
      client-opts            {:client-opts client-opts}
      opts                   opts
      :else                  nil)))

(defn- require-write-enabled!
  [state]
  (when-not (:allow-writes? @state)
    (throw (ex-info "Write tools are disabled for this MCP session."
                    {:code :writes-disabled}))))

(defn- normalize-kv-tx
  [tx]
  (let [tx (decode-datavalue tx)
        op (first tx)
        op (cond
             (keyword? op) op
             (string? op)  (keyword op)
             :else         op)]
    (assoc tx 0 op)))

(defn- normalize-kv-txs
  [txs]
  (mapv normalize-kv-tx (require-vector txs "txs")))

(defn- database-entry
  [state database-id]
  (or (get-in @state [:databases database-id])
      (throw (ex-info (str "Unknown database: " database-id)
                      {:code     :unknown-database
                       :database database-id}))))

(defn- database-handle
  [state database-id]
  (:conn-handle (database-entry state database-id)))

(defn- register-database!
  [state conn-handle args]
  (if-let [database-id (get-in @state [:database-by-handle conn-handle])]
    database-id
    (let [database-id (gen-id "database")
          entry       {"database" database-id
                       "dir"      (get args "dir")}]
      (swap! state
             (fn [current]
               (-> current
                   (assoc-in [:databases database-id]
                             (assoc entry :conn-handle conn-handle))
                   (assoc-in [:database-by-handle conn-handle] database-id))))
      database-id)))

(defn- kv-entry
  [state kv-id]
  (or (get-in @state [:kvs kv-id])
      (throw (ex-info (str "Unknown kv store: " kv-id)
                      {:code :unknown-kv
                       :kv   kv-id}))))

(defn- kv-handle
  [state kv-id]
  (:kv-handle (kv-entry state kv-id)))

(defn- register-kv!
  [state kv-handle args]
  (if-let [kv-id (get-in @state [:kv-by-handle kv-handle])]
    kv-id
    (let [kv-id (gen-id "kv")
          entry {"kv"  kv-id
                 "dir" (get args "dir")}]
      (swap! state
             (fn [current]
               (-> current
                   (assoc-in [:kvs kv-id] (assoc entry :kv-handle kv-handle))
                   (assoc-in [:kv-by-handle kv-handle] kv-id))))
      kv-id)))

(defn- search-index-entry
  [state search-index-id]
  (or (get-in @state [:search-indexes search-index-id])
      (throw (ex-info (str "Unknown search index: " search-index-id)
                      {:code         :unknown-search-index
                       :search-index search-index-id}))))

(defn- search-index-handle
  [state search-index-id]
  (:search-handle (search-index-entry state search-index-id)))

(defn- register-search-index!
  [state search-handle kv-id]
  (if-let [search-index-id (get-in @state [:search-index-by-handle search-handle])]
    search-index-id
    (let [search-index-id (gen-id "search-index")
          entry           {"searchIndex" search-index-id
                           "kv"          kv-id}]
      (swap! state
             (fn [current]
               (-> current
                   (assoc-in [:search-indexes search-index-id]
                             (assoc entry :search-handle search-handle))
                   (assoc-in [:search-index-by-handle search-handle]
                             search-index-id))))
      search-index-id)))

(defn- vector-index-entry
  [state vector-index-id]
  (or (get-in @state [:vector-indexes vector-index-id])
      (throw (ex-info (str "Unknown vector index: " vector-index-id)
                      {:code         :unknown-vector-index
                       :vector-index vector-index-id}))))

(defn- vector-index-handle
  [state vector-index-id]
  (:vec-handle (vector-index-entry state vector-index-id)))

(defn- dependent-search-index-ids
  [state kv-id]
  (->> (get @state :search-indexes)
       (keep (fn [[search-index-id entry]]
               (when (= kv-id (get entry "kv"))
                 search-index-id)))
       vec))

(defn- dependent-vector-index-ids
  [state kv-id]
  (->> (get @state :vector-indexes)
       (keep (fn [[vector-index-id entry]]
               (when (= kv-id (get entry "kv"))
                 vector-index-id)))
       vec))

(declare close-search-index close-vector-index)

(defn- register-vector-index!
  [state vec-handle kv-id]
  (if-let [vector-index-id (get-in @state [:vector-index-by-handle vec-handle])]
    vector-index-id
    (let [vector-index-id (gen-id "vector-index")
          entry           {"vectorIndex" vector-index-id
                           "kv"          kv-id}]
      (swap! state
             (fn [current]
               (-> current
                   (assoc-in [:vector-indexes vector-index-id]
                             (assoc entry :vec-handle vec-handle))
                   (assoc-in [:vector-index-by-handle vec-handle]
                             vector-index-id))))
      vector-index-id)))

(defn- open-database
  [state arguments]
  (let [{:keys [target remote]} (require-open-target arguments)
        opts       (merge-open-opts arguments)
        args       {"dir" target}
        args       (cond-> args
                     (contains? arguments "schema")
                     (assoc "schema" (decode-datavalue
                                       (get arguments "schema"))))
        args       (cond-> args
                     opts
                     (assoc "opts" opts))
        op         (if (true? (get arguments "create"))
                     "create-conn"
                     "get-conn")
        conn-handle (get (shared/exec-request (:session-state @state)
                                              {"op"   op
                                               "args" args})
                         "result")
        database-id (register-database! state conn-handle {"dir" target})]
    {"database" database-id
     "created"  (= op "create-conn")
     "target"   target
     "remote"   remote}))

(defn- close-database
  [state arguments]
  (let [database-id (require-string (get arguments "database") "database")
        conn-handle (database-handle state database-id)]
    (shared/exec-request (:session-state @state)
                         {"op"   "close"
                          "args" {"conn" conn-handle}})
    (swap! state
           (fn [current]
             (-> current
                 (update :databases dissoc database-id)
                 (update :database-by-handle dissoc conn-handle))))
    {"database" database-id
     "closed"   true}))

(defn- open-kv-store
  [state arguments]
  (let [{:keys [target remote]} (require-open-target arguments)
        opts      (merge-open-opts arguments)
        kv-handle (get (shared/exec-request (:session-state @state)
                                            {"op"   "open-kv"
                                             "args" (cond-> {"dir" target}
                                                       opts
                                                       (assoc "opts" opts))})
                       "result")
        kv-id     (register-kv! state kv-handle {"dir" target})]
    {"kv"     kv-id
     "target" target
     "remote" remote}))

(defn- close-kv-store
  [state arguments]
  (let [kv-id     (require-string (get arguments "kv") "kv")
        kv-handle (kv-handle state kv-id)
        search-index-ids (dependent-search-index-ids state kv-id)
        vector-index-ids (dependent-vector-index-ids state kv-id)]
    (shared/exec-request (:session-state @state)
                         {"op"   "close-kv"
                          "args" {"kv" kv-handle}})
    (doseq [search-index-id search-index-ids]
      (close-search-index state {"searchIndex" search-index-id}))
    (doseq [vector-index-id vector-index-ids]
      (close-vector-index state {"vectorIndex" vector-index-id}))
    (swap! state
           (fn [current]
             (-> current
                 (update :kvs dissoc kv-id)
                 (update :kv-by-handle dissoc kv-handle))))
    {"kv" kv-id
     "closed" true}))

(defn- open-search-index
  [state arguments]
  (let [kv-id            (require-string (get arguments "kv") "kv")
        kv-handle*       (kv-handle state kv-id)
        args             (cond-> {"kv" kv-handle*}
                           (contains? arguments "opts")
                           (assoc "opts" (decode-datavalue
                                           (require-map (get arguments "opts")
                                                        "opts"))))
        search-handle    (get (shared/exec-request (:session-state @state)
                                                   {"op"   "new-search-engine"
                                                    "args" args})
                              "result")
        search-index-id  (register-search-index! state search-handle kv-id)]
    {"searchIndex" search-index-id
     "kv"          kv-id}))

(defn- close-search-index
  [state arguments]
  (let [search-index-id (require-string (get arguments "searchIndex")
                                        "searchIndex")
        search-handle   (search-index-handle state search-index-id)]
    (shared/exec-request (:session-state @state)
                         {"op"   "release-handle"
                          "args" {"handle" search-handle}})
    (swap! state
           (fn [current]
             (-> current
                 (update :search-indexes dissoc search-index-id)
                 (update :search-index-by-handle dissoc search-handle))))
    {"searchIndex" search-index-id
     "closed"      true}))

(defn- open-vector-index
  [state arguments]
  (let [kv-id            (require-string (get arguments "kv") "kv")
        kv-handle*       (kv-handle state kv-id)
        opts             (decode-datavalue
                           (require-map (get arguments "opts") "opts"))
        vector-handle    (get (shared/exec-request (:session-state @state)
                                                   {"op"   "new-vector-index"
                                                    "args" {"kv"   kv-handle*
                                                            "opts" opts}})
                              "result")
        vector-index-id  (register-vector-index! state vector-handle kv-id)]
    {"vectorIndex" vector-index-id
     "kv"          kv-id}))

(defn- close-vector-index
  [state arguments]
  (let [vector-index-id (require-string (get arguments "vectorIndex")
                                        "vectorIndex")
        vec-handle      (vector-index-handle state vector-index-id)]
    (shared/exec-request (:session-state @state)
                         {"op"   "close-vector-index"
                          "args" {"vec" vec-handle}})
    (swap! state
           (fn [current]
             (-> current
                 (update :vector-indexes dissoc vector-index-id)
                 (update :vector-index-by-handle dissoc vec-handle))))
    {"vectorIndex" vector-index-id
     "closed"      true}))

(defn- ensure-kv-dbi-open!
  [state kv-handle dbi]
  (shared/exec-request (:session-state @state)
                       {"op"   "open-dbi"
                        "args" {"kv" kv-handle
                                "dbi-name" dbi}})
  nil)

(defn- kv-typed-args
  [base arguments]
  (cond-> base
    (contains? arguments "keyType")
    (assoc "k-type" (require-string (get arguments "keyType") "keyType"))

    (contains? arguments "valueType")
    (assoc "v-type" (require-string (get arguments "valueType") "valueType"))))

(defn- kv-get
  [state arguments]
  (let [kv-id      (require-string (get arguments "kv") "kv")
        dbi        (require-string (get arguments "dbi") "dbi")
        kv-handle* (kv-handle state kv-id)
        _          (ensure-kv-dbi-open! state kv-handle* dbi)
        args       (cond-> (kv-typed-args {"kv"  kv-handle*
                                           "dbi" dbi
                                           "k"   (get arguments "key")}
                                          arguments)
                     (contains? arguments "returnPair")
                     (assoc "ignore-key?" (not (require-boolean
                                                 (get arguments "returnPair")
                                                 "returnPair"))))
        result     (get (shared/exec-request (:session-state @state)
                                             {"op"   "get-value"
                                              "args" args})
                        "result")]
    {"kv"     kv-id
     "dbi"    dbi
     "result" result}))

(defn- kv-range
  [state arguments]
  (let [kv-id      (require-string (get arguments "kv") "kv")
        dbi        (require-string (get arguments "dbi") "dbi")
        key-range  (decode-datavalue
                     (require-vector (get arguments "keyRange") "keyRange"))
        kv-handle* (kv-handle state kv-id)
        _          (ensure-kv-dbi-open! state kv-handle* dbi)
        args       (cond-> (kv-typed-args {"kv"      kv-handle*
                                           "dbi"     dbi
                                           "k-range" key-range}
                                          arguments)
                     (contains? arguments "limit")
                     (assoc "limit" (get arguments "limit"))

                     (contains? arguments "offset")
                     (assoc "offset" (get arguments "offset")))
        result     (get (shared/exec-request (:session-state @state)
                                             {"op"   "get-range"
                                              "args" args})
                        "result")]
    {"kv"     kv-id
     "dbi"    dbi
     "result" result}))

(defn- transact-kv-store
  [state arguments]
  (require-write-enabled! state)
  (let [kv-id      (require-string (get arguments "kv") "kv")
        kv-handle* (kv-handle state kv-id)
        dbi        (when (contains? arguments "dbi")
                     (require-string (get arguments "dbi") "dbi"))
        _          (when dbi
                     (ensure-kv-dbi-open! state kv-handle* dbi))
        args       (cond-> {"kv"  kv-handle*
                            "txs" (normalize-kv-txs (get arguments "txs"))}
                     dbi
                     (assoc "dbi-name" dbi)

                     (contains? arguments "keyType")
                     (assoc "k-type" (require-string (get arguments "keyType")
                                                     "keyType"))

                     (contains? arguments "valueType")
                     (assoc "v-type" (require-string (get arguments "valueType")
                                                     "valueType")))
        result     (get (shared/exec-request (:session-state @state)
                                             {"op"   "transact-kv"
                                              "args" args})
                        "result")]
    {"kv"     kv-id
     "result" (or result ":transacted")}))

(defn- add-document
  [state arguments]
  (require-write-enabled! state)
  (let [search-index-id (require-string (get arguments "searchIndex")
                                        "searchIndex")
        args            (cond-> {"search"   (search-index-handle
                                              state
                                              search-index-id)
                                 "doc-ref"  (decode-datavalue
                                              (get arguments "docRef"))
                                 "doc-text" (require-string
                                              (get arguments "docText")
                                              "docText")}
                          (contains? arguments "checkExist")
                          (assoc "check-exist?" (require-boolean
                                                  (get arguments "checkExist")
                                                  "checkExist")))]
    (shared/exec-request (:session-state @state)
                         {"op"   "add-doc"
                          "args" args})
    {"searchIndex" search-index-id
     "added"       true}))

(defn- remove-document
  [state arguments]
  (require-write-enabled! state)
  (let [search-index-id (require-string (get arguments "searchIndex")
                                        "searchIndex")]
    (shared/exec-request (:session-state @state)
                         {"op"   "remove-doc"
                          "args" {"search"  (search-index-handle
                                              state
                                              search-index-id)
                                  "doc-ref" (decode-datavalue
                                              (get arguments "docRef"))}})
    {"searchIndex" search-index-id
     "removed"     true}))

(defn- clear-documents
  [state arguments]
  (require-write-enabled! state)
  (let [search-index-id (require-string (get arguments "searchIndex")
                                        "searchIndex")]
    (shared/exec-request (:session-state @state)
                         {"op"   "clear-docs"
                          "args" {"search" (search-index-handle
                                             state
                                             search-index-id)}})
    {"searchIndex" search-index-id
     "cleared"     true}))

(defn- document-indexed
  [state arguments]
  (let [search-index-id (require-string (get arguments "searchIndex")
                                        "searchIndex")
        result          (get (shared/exec-request (:session-state @state)
                                                  {"op"   "doc-indexed?"
                                                   "args" {"search"  (search-index-handle
                                                                       state
                                                                       search-index-id)
                                                           "doc-ref" (decode-datavalue
                                                                       (get arguments "docRef"))}})
                             "result")]
    {"searchIndex" search-index-id
     "result"      result}))

(defn- document-count
  [state arguments]
  (let [search-index-id (require-string (get arguments "searchIndex")
                                        "searchIndex")
        result          (get (shared/exec-request (:session-state @state)
                                                  {"op"   "doc-count"
                                                   "args" {"search" (search-index-handle
                                                                      state
                                                                      search-index-id)}})
                             "result")]
    {"searchIndex" search-index-id
     "result"      result}))

(defn- search-documents
  [state arguments]
  (let [search-index-id (require-string (get arguments "searchIndex")
                                        "searchIndex")
        args            (cond-> {"search" (search-index-handle
                                            state
                                            search-index-id)
                                 "query"  (require-string
                                            (get arguments "query")
                                            "query")}
                          (contains? arguments "opts")
                          (assoc "opts" (decode-datavalue
                                          (require-map (get arguments "opts")
                                                       "opts"))))
        result          (get (shared/exec-request (:session-state @state)
                                                  {"op"   "search"
                                                   "args" args})
                             "result")]
    {"searchIndex" search-index-id
     "result"      result}))

(defn- vector-index-info
  [state arguments]
  (let [vector-index-id (require-string (get arguments "vectorIndex")
                                        "vectorIndex")
        result          (get (shared/exec-request (:session-state @state)
                                                  {"op"   "vector-index-info"
                                                   "args" {"vec" (vector-index-handle
                                                                   state
                                                                   vector-index-id)}})
                             "result")]
    {"vectorIndex" vector-index-id
     "result"      result}))

(defn- add-vector
  [state arguments]
  (require-write-enabled! state)
  (let [vector-index-id (require-string (get arguments "vectorIndex")
                                        "vectorIndex")
        args            {"vec"      (vector-index-handle state vector-index-id)
                         "vec-ref"  (decode-datavalue
                                      (get arguments "vectorRef"))
                         "vec-data" (require-vector (get arguments "vectorData")
                                                    "vectorData")}]
    (shared/exec-request (:session-state @state)
                         {"op"   "add-vec"
                          "args" args})
    {"vectorIndex" vector-index-id
     "added"       true}))

(defn- remove-vector
  [state arguments]
  (require-write-enabled! state)
  (let [vector-index-id (require-string (get arguments "vectorIndex")
                                        "vectorIndex")]
    (shared/exec-request (:session-state @state)
                         {"op"   "remove-vec"
                          "args" {"vec"     (vector-index-handle
                                              state
                                              vector-index-id)
                                  "vec-ref" (decode-datavalue
                                              (get arguments "vectorRef"))}})
    {"vectorIndex" vector-index-id
     "removed"     true}))

(defn- vector-indexed
  [state arguments]
  (let [vector-index-id (require-string (get arguments "vectorIndex")
                                        "vectorIndex")
        result          (get (shared/exec-request (:session-state @state)
                                                  {"op"   "vec-indexed?"
                                                   "args" {"vec"     (vector-index-handle
                                                                       state
                                                                       vector-index-id)
                                                           "vec-ref" (decode-datavalue
                                                                       (get arguments "vectorRef"))}})
                             "result")]
    {"vectorIndex" vector-index-id
     "result"      result}))

(defn- search-vector-index
  [state arguments]
  (let [vector-index-id (require-string (get arguments "vectorIndex")
                                        "vectorIndex")
        args            (cond-> {"vec"       (vector-index-handle
                                              state
                                              vector-index-id)
                                 "query-vec" (require-vector
                                               (get arguments "queryVector")
                                               "queryVector")}
                          (contains? arguments "opts")
                          (assoc "opts" (decode-datavalue
                                          (require-map (get arguments "opts")
                                                       "opts"))))
        result          (get (shared/exec-request (:session-state @state)
                                                  {"op"   "search-vec"
                                                   "args" args})
                             "result")]
    {"vectorIndex" vector-index-id
     "result"      result}))

(defn- query-database
  [state arguments]
  (let [database-id (require-string (get arguments "database") "database")
        query       (require-string (get arguments "query") "query")
        inputs      (if (contains? arguments "inputs")
                      (decode-datavalue
                        (require-vector (get arguments "inputs") "inputs"))
                      [])
        conn-handle (database-handle state database-id)
        result      (get (shared/exec-request (:session-state @state)
                                              {"op"   "q"
                                               "args" {"conn"   conn-handle
                                                       "query"  query
                                                       "inputs" inputs}})
                         "result")]
    {"database" database-id
     "result"   result}))

(defn- datoms-database
  [state arguments]
  (let [database-id (require-string (get arguments "database") "database")
        index       (require-string (get arguments "index") "index")
        conn-handle (database-handle state database-id)
        args        (cond-> {"conn"  conn-handle
                             "index" index}
                      (contains? arguments "c1")
                      (assoc "c1" (decode-datavalue (get arguments "c1")))

                      (contains? arguments "c2")
                      (assoc "c2" (decode-datavalue (get arguments "c2")))

                      (contains? arguments "c3")
                      (assoc "c3" (decode-datavalue (get arguments "c3")))

                      (contains? arguments "limit")
                      (assoc "limit" (get arguments "limit"))

                      (contains? arguments "offset")
                      (assoc "offset" (get arguments "offset")))
        result      (get (shared/exec-request (:session-state @state)
                                              {"op"   "datoms"
                                               "args" args})
                         "result")]
    {"database" database-id
     "result"   result}))

(defn- search-datoms-database
  [state arguments]
  (let [database-id (require-string (get arguments "database") "database")
        conn-handle (database-handle state database-id)
        args        (cond-> {"conn" conn-handle}
                      (contains? arguments "e")
                      (assoc "e" (decode-datavalue (get arguments "e")))

                      (contains? arguments "a")
                      (assoc "a" (decode-datavalue (get arguments "a")))

                      (contains? arguments "v")
                      (assoc "v" (decode-datavalue (get arguments "v")))

                      (contains? arguments "limit")
                      (assoc "limit" (get arguments "limit"))

                      (contains? arguments "offset")
                      (assoc "offset" (get arguments "offset")))
        result      (get (shared/exec-request (:session-state @state)
                                              {"op"   "search-datoms"
                                               "args" args})
                         "result")]
    {"database" database-id
     "result"   result}))

(defn- count-datoms-database
  [state arguments]
  (let [database-id (require-string (get arguments "database") "database")
        conn-handle (database-handle state database-id)
        args        (cond-> {"conn" conn-handle}
                      (contains? arguments "e")
                      (assoc "e" (decode-datavalue (get arguments "e")))

                      (contains? arguments "a")
                      (assoc "a" (decode-datavalue (get arguments "a")))

                      (contains? arguments "v")
                      (assoc "v" (decode-datavalue (get arguments "v"))))
        result      (get (shared/exec-request (:session-state @state)
                                              {"op"   "count-datoms"
                                               "args" args})
                         "result")]
    {"database" database-id
     "result"   result}))

(defn- pull-database
  [state arguments]
  (let [database-id (require-string (get arguments "database") "database")
        conn-handle (database-handle state database-id)
        result      (get (shared/exec-request (:session-state @state)
                                              {"op"   "pull"
                                               "args" {"conn"     conn-handle
                                                       "selector" (decode-datavalue
                                                                    (get arguments "selector"))
                                                       "eid"      (decode-datavalue
                                                                    (get arguments "eid"))}})
                         "result")]
    {"database" database-id
     "result"   result}))

(defn- entity-database
  [state arguments]
  (let [database-id (require-string (get arguments "database") "database")
        conn-handle (database-handle state database-id)
        result      (get (shared/exec-request (:session-state @state)
                                              {"op"   "entity"
                                               "args" {"conn" conn-handle
                                                       "eid"  (decode-datavalue
                                                                (get arguments "eid"))}})
                         "result")]
    {"database" database-id
     "result"   result}))

(defn- pull-many-database
  [state arguments]
  (let [database-id (require-string (get arguments "database") "database")
        conn-handle (database-handle state database-id)
        result      (get (shared/exec-request (:session-state @state)
                                              {"op"   "pull-many"
                                               "args" {"conn"     conn-handle
                                                       "selector" (decode-datavalue
                                                                    (get arguments "selector"))
                                                       "eids"     (decode-datavalue
                                                                    (require-vector
                                                                      (get arguments "eids")
                                                                      "eids"))}})
                         "result")]
    {"database" database-id
     "result"   result}))

(defn- transact-database
  [state arguments]
  (require-write-enabled! state)
  (let [database-id (require-string (get arguments "database") "database")
        conn-handle (database-handle state database-id)
        tx-data     (require-vector (get arguments "txData") "txData")
        args        (cond-> {"conn"    conn-handle
                             "tx-data" (decode-datavalue tx-data)}
                      (contains? arguments "txMeta")
                      (assoc "tx-meta" (decode-datavalue
                                         (get arguments "txMeta"))))
        result      (get (shared/exec-request (:session-state @state)
                                              {"op"   "transact!"
                                               "args" args})
                         "result")]
    {"database" database-id
     "result"   result}))

(defn- fulltext-datoms-database
  [state arguments]
  (let [database-id (require-string (get arguments "database") "database")
        conn-handle (database-handle state database-id)
        args        (cond-> {"conn"  conn-handle
                             "query" (require-string (get arguments "query")
                                                     "query")}
                      (contains? arguments "opts")
                      (assoc "opts" (decode-datavalue
                                      (require-map (get arguments "opts")
                                                   "opts"))))
        result      (get (shared/exec-request (:session-state @state)
                                              {"op"   "fulltext-datoms"
                                               "args" args})
                         "result")]
    {"database" database-id
     "result"   result}))

(defn- initialize-result
  []
  {"protocolVersion" protocol-version
   "capabilities"    {"tools" {}}
   "serverInfo"      {"name"    "datalevin"
                       "version" c/version}})

(defn- state-phase
  [state-map]
  (or (:phase state-map)
      (if (:initialized? state-map)
        :ready
        :new)))

(defn- session-phase
  [state]
  (state-phase @state))

(defn- require-request-phase!
  [state method]
  (let [phase (session-phase state)]
    (case phase
      :new
      (when-not (= method "initialize")
        (throw (ex-info "Requests are not allowed before initialize."
                        {:code   :invalid-request
                         :method method
                         :phase  "uninitialized"})))

      :initializing
      (cond
        (= method "initialize")
        (throw (ex-info "Initialize may only be sent once per session."
                        {:code   :invalid-request
                         :method method
                         :phase  "initializing"}))

        (= method "ping")
        nil

        :else
        (throw (ex-info "Requests are not allowed before initialized."
                        {:code   :invalid-request
                         :method method
                         :phase  "initializing"})))

      :ready
      (when (= method "initialize")
        (throw (ex-info "Initialize may only be sent once per session."
                        {:code   :invalid-request
                         :method method
                         :phase  "ready"})))

      nil)))

(defn- call-tool
  ([state name arguments]
   (call-tool state name arguments identity))
  ([state name arguments wrap-fn]
   (letfn [(respond [value]
             (tool-response value false wrap-fn))]
     (case name
       "datalevin_api_info"
       (let [result (get (shared/exec-request (:session-state @state)
                                              {"op"   "api-info"
                                               "args" {}})
                         "result")]
         (respond {"mcp"       {"protocolVersion" protocol-version
                                "transport"       "stdio"
                                "allowWrites"     (:allow-writes? @state)}
                   "datalevin" result}))

       "datalevin_open_database"
       (respond (open-database state arguments))

       "datalevin_close_database"
       (respond (close-database state arguments))

       "datalevin_open_kv"
       (respond (open-kv-store state arguments))

       "datalevin_close_kv"
       (respond (close-kv-store state arguments))

       "datalevin_open_search_index"
       (respond (open-search-index state arguments))

       "datalevin_close_search_index"
       (respond (close-search-index state arguments))

       "datalevin_add_document"
       (respond (add-document state arguments))

       "datalevin_remove_document"
       (respond (remove-document state arguments))

       "datalevin_clear_documents"
       (respond (clear-documents state arguments))

       "datalevin_document_indexed"
       (respond (document-indexed state arguments))

       "datalevin_document_count"
       (respond (document-count state arguments))

       "datalevin_search_documents"
       (respond (search-documents state arguments))

       "datalevin_open_vector_index"
       (respond (open-vector-index state arguments))

       "datalevin_close_vector_index"
       (respond (close-vector-index state arguments))

       "datalevin_vector_index_info"
       (respond (vector-index-info state arguments))

       "datalevin_add_vector"
       (respond (add-vector state arguments))

       "datalevin_remove_vector"
       (respond (remove-vector state arguments))

       "datalevin_vector_indexed"
       (respond (vector-indexed state arguments))

       "datalevin_vector_search"
       (respond (search-vector-index state arguments))

       "datalevin_kv_get"
       (respond (kv-get state arguments))

       "datalevin_kv_range"
       (respond (kv-range state arguments))

       "datalevin_kv_transact"
       (respond (transact-kv-store state arguments))

       "datalevin_query"
       (respond (query-database state arguments))

       "datalevin_datoms"
       (respond (datoms-database state arguments))

       "datalevin_search_datoms"
       (respond (search-datoms-database state arguments))

       "datalevin_count_datoms"
       (respond (count-datoms-database state arguments))

       "datalevin_pull"
       (respond (pull-database state arguments))

       "datalevin_entity"
       (respond (entity-database state arguments))

       "datalevin_pull_many"
       (respond (pull-many-database state arguments))

       "datalevin_transact"
       (respond (transact-database state arguments))

       "datalevin_fulltext_datoms"
       (respond (fulltext-datoms-database state arguments))

       (throw (ex-info (str "Unknown tool: " name)
                       {:code :unknown-tool
                        :tool name}))))))

(defn ^:no-doc handle-request
  [state request]
  (let [id     (require-request-id request)
        method (get request "method")
        params (require-map (get request "params") "params")]
    (when-not (= "2.0" (get request "jsonrpc"))
      (throw (ex-info "jsonrpc must be \"2.0\"."
                      {:code :invalid-request})))
    (when-not (string? method)
      (throw (ex-info "method must be a string."
                      {:code :invalid-request})))
    (require-request-phase! state method)
    (case method
      "initialize"
      (let [params (require-initialize-params params)]
        (swap! state assoc
               :phase :initializing
               :initialized? false
               :client-info (get params "clientInfo")
               :client-capabilities (get params "capabilities")
               :client-protocol-version (get params "protocolVersion"))
        (jsonrpc-result id (initialize-result)))

      "ping"
      (jsonrpc-result id {})

      "tools/list"
      (jsonrpc-result id {"tools" (->> tool-specs vals (sort-by #(get % "name")) vec)})

      "tools/call"
      (let [name      (require-string (get params "name") "name")
            arguments (require-map (get params "arguments") "arguments")
            wrap-fn   #(jsonrpc-result id %)]
        (try
          (jsonrpc-result id (call-tool state name arguments wrap-fn))
          (catch clojure.lang.ExceptionInfo e
            (let [data (or (ex-data e) {})]
              (jsonrpc-result
                id
                (tool-response
                  {"error"   (or (.getMessage e) (str (class e)))
                   "code"    (str (or (:code data) :tool-error))
                   "details" data}
                  true
                  wrap-fn))))))

      (throw (ex-info (str "Unsupported method: " method)
                      {:code   :method-not-found
                       :method method})))))

(defn ^:no-doc handle-message
  [state message]
  (let [message (require-map message "request")
        method  (get message "method")
        id      (get message "id")
        response-id (when (valid-request-id? id) id)]
    (try
      (cond
        (= method "notifications/initialized")
        (do
          (swap! state
                 (fn [current]
                   (if (= :initializing (state-phase current))
                     (assoc current
                            :phase :ready
                            :initialized? true)
                     current)))
          nil)

        (and (string? method)
             (str/starts-with? method "notifications/")
             (nil? id))
        nil

        :else
        (handle-request state message))
      (catch clojure.lang.ExceptionInfo e
        (let [{:keys [code] :as data} (ex-data e)
              rpc-code (case code
                         :invalid-request -32600
                         :invalid-params -32602
                         :method-not-found -32601
                         :parse-error -32700
                         -32000)]
          (jsonrpc-error response-id
                         rpc-code
                         (or (.getMessage e) (str (class e)))
                         data)))
      (catch Throwable t
        (jsonrpc-error response-id -32603
                       (or (.getMessage t) (str (class t))))))))

(defn ^:no-doc handle-input
  [state input]
  (if (vector? input)
    (jsonrpc-error nil -32600 "JSON-RPC batch requests are not supported."
                   {:code :invalid-request})
    (handle-message state input)))

(defn- read-message
  [^BufferedReader reader]
  (loop []
    (when-let [line (.readLine reader)]
      (if (str/blank? line)
        (recur)
        (jc/read-json-string line)))))

(defn- write-message!
  [^BufferedWriter writer message]
  (.write writer (jc/write-json-ready-string message))
  (.write writer "\n")
  (.flush writer))

(defn serve
  ([^Reader reader ^Writer writer]
   (serve reader writer {}))
  ([^Reader reader ^Writer writer _opts]
   (let [reader (BufferedReader. reader)
         writer (BufferedWriter. writer)
         state  (atom {:phase         :new
                       :initialized?  false
                       :allow-writes? (true? (:allow-writes _opts))
                       :session-state (shared/new-session-state)})]
     (try
       (loop []
         (when-let [message (read-message reader)]
           (when-let [response (handle-input state message)]
             (write-message! writer response))
           (recur)))
       (finally
         (shared/clear-handles! (:session-state @state)))))))

(defn run
  ([]
   (run {}))
  ([opts]
   (serve (InputStreamReader. System/in)
          (OutputStreamWriter. System/out "UTF-8")
          opts)))
