;;
;; Copyright (c) Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns datalevin.json-api.shared
  (:require
   [clojure.edn :as edn]
   [clojure.string :as str]
   [datalevin.client :as dc]
   [datalevin.constants :as c]
   [datalevin.core :as d]
   [datalevin.datom :as dd]
   [datalevin.interface :as i]
   [datalevin.json-convert :as jc])
  (:import
   [java.util IdentityHashMap UUID]
   [java.util.concurrent ConcurrentHashMap]))

(def ^:const json-api-version 1)

(def default-limits
  {:max-result-items   10000
  :max-response-bytes 8388608})

(def ^:const default-page-limit 10000)

(def ^:dynamic *json-api-limits*
  default-limits)

(def ^:private features
  ["api-info"
   "conn-lifecycle"
   "handle-registry"
   "tagged-i64"
   "datalog-core"
   "kv-core"
   "compound-transactions"
   "utility-ops"
   "admin"
   "search"
   "vector"])

(defn ^:no-doc new-session-state
  []
  {:handles         (ConcurrentHashMap.)
   :reverse-handles (IdentityHashMap.)})

(defonce ^:private default-session-state
  (new-session-state))

(def ^:dynamic *session-state*
  nil)

(defn- current-session-state
  []
  (or *session-state* default-session-state))

(defn- session-handles
  ([] (session-handles (current-session-state)))
  ([session] (:handles session)))

(defn- session-reverse-handles
  ([] (session-reverse-handles (current-session-state)))
  ([session] (:reverse-handles session)))

(defn- gen-handle
  [prefix]
  (str prefix "-" (UUID/randomUUID)))

(defn- invalid-handle
  ([h]
   (invalid-handle h nil nil))
  ([h expected actual]
   (throw
     (ex-info
       (str "Invalid or expired handle: " h)
       (cond-> {:code   :invalid-handle
                :handle h}
         expected (assoc :expected expected)
         actual (assoc :actual actual))))))

(defn ^:no-doc register!
  ([prefix type obj]
   (register! (current-session-state) prefix type obj))
  ([session prefix type obj]
   (let [handles         (session-handles session)
         reverse-handles (session-reverse-handles session)]
     (locking reverse-handles
       (if-let [h (.get ^IdentityHashMap reverse-handles obj)]
         h
         (let [h (gen-handle prefix)]
           (.put handles h {:type type :obj obj})
           (.put ^IdentityHashMap reverse-handles obj h)
           h))))))

(defn ^:no-doc rebind!
  ([h type new-obj]
   (rebind! (current-session-state) h type new-obj))
  ([session h type new-obj]
   (let [handles         (session-handles session)
         reverse-handles (session-reverse-handles session)]
     (locking reverse-handles
       (when-let [{:keys [obj]} (.get handles h)]
         (.remove ^IdentityHashMap reverse-handles obj))
       (.put handles h {:type type :obj new-obj})
       (.put ^IdentityHashMap reverse-handles new-obj h)
       h))))

(defn ^:no-doc resolve-entry
  ([h]
   (resolve-entry (current-session-state) h))
  ([session h]
   (or (.get ^ConcurrentHashMap (session-handles session) h)
       (invalid-handle h))))

(defn ^:no-doc resolve-handle
  ([h]
   (resolve-handle (current-session-state) h))
  ([session h]
   (:obj (resolve-entry session h))))

(defn- resolve-typed-handle
  [h expected-type]
  (let [{:keys [type obj]} (resolve-entry h)]
    (if (= type expected-type)
      obj
      (invalid-handle h expected-type type))))

(defn ^:no-doc release!
  ([h]
   (release! (current-session-state) h))
  ([session h]
   (let [handles         (session-handles session)
         reverse-handles (session-reverse-handles session)]
     (when-let [{:keys [type obj]} (.remove handles h)]
       (locking reverse-handles
         (.remove ^IdentityHashMap reverse-handles obj))
       (case type
         :conn          (d/close obj)
         :kv            (d/close-kv obj)
         :vec           (d/close-vector-index obj)
         :client        (dc/disconnect obj)
         :search-writer nil
         :search        nil
         nil))
     true)))

(defn ^:no-doc unregister!
  ([h]
   (unregister! (current-session-state) h))
  ([session h]
   (let [handles         (session-handles session)
         reverse-handles (session-reverse-handles session)]
     (when-let [{:keys [obj]} (.remove handles h)]
       (locking reverse-handles
         (.remove ^IdentityHashMap reverse-handles obj)))
     true)))

(defn ^:no-doc clear-handles!
  ([] (clear-handles! (current-session-state)))
  ([session]
   (let [handles         (session-handles session)
         reverse-handles (session-reverse-handles session)]
     (doseq [h (vec (.keySet handles))]
       (release! session h))
     (locking reverse-handles
       (.clear ^IdentityHashMap reverse-handles))
     (.clear handles)
     true)))

(defn- count-direct-items
  [x]
  (cond
    (map? x) (count x)
    (instance? java.util.Map x) (.size ^java.util.Map x)
    (or (vector? x) (list? x) (set? x) (seq? x)) (count x)
    (instance? java.util.Collection x) (.size ^java.util.Collection x)
    :else 0))

(defn- nested-items
  [x]
  (cond
    (map? x) (mapcat identity x)
    (instance? java.util.Map x) (mapcat identity (into {} x))
    (or (vector? x) (list? x) (set? x) (seq? x)) x
    (instance? java.util.Collection x) x
    :else nil))

(defn- enforce-result-item-limit!
  [json-result]
  (let [limit (:max-result-items *json-api-limits*)]
    (when limit
      (loop [stack (list json-result)
             seen  0]
        (when-let [x (first stack)]
          (let [seen' (+ seen (count-direct-items x))]
            (when (> seen' limit)
              (throw (ex-info "Result exceeds max-result-items limit."
                              {:code  :result-too-large
                               :kind  :items
                               :limit limit})))
            (recur (concat (nested-items x) (rest stack)) seen')))))
    json-result))

(defn- api-info
  []
  {"json-api-version" json-api-version
   "datalevin-version" c/version
   "features"          features
   "limits"            {"max-result-items"   (:max-result-items *json-api-limits*)
                        "max-response-bytes" (:max-response-bytes
                                               *json-api-limits*)}})

(defn- invoke-conn-fn
  [f args]
  (let [has-dir?    (contains? args "dir")
        has-schema? (contains? args "schema")
        has-opts?   (contains? args "opts")
        dir         (get args "dir")
        schema      (get args "schema")
        opts        (get args "opts")]
    (cond
      has-opts?   (f dir schema opts)
      has-schema? (f dir schema)
      has-dir?    (f dir)
      :else       (f))))

(def ^:private tx-pull-option-order
  [:as :limit :default :xform])

(def ^:private aborted-transaction-key
  ::aborted-transaction)

(defn- require-map
  [x]
  (cond
    (nil? x) {}
    (map? x) x
    (instance? java.util.Map x) (into {} x)
    :else
    (throw (ex-info "Request args must be a JSON object."
                    {:code :invalid-request}))))

(defn- require-string
  [x field]
  (if (string? x)
    x
    (throw (ex-info (str field " must be a string.")
                    {:code  :invalid-request
                     :field field}))))

(defn- require-vector
  [x field]
  (if (vector? x)
    x
    (throw (ex-info (str field " must be a JSON array.")
                    {:code  :invalid-request
                     :field field}))))

(defn- require-nonnegative-long
  [x field]
  (when-not (integer? x)
    (throw (ex-info (str field " must be an integer.")
                    {:code  :invalid-request
                     :field field})))
  (let [n (long x)]
    (when (neg? n)
      (throw (ex-info (str field " must be non-negative.")
                      {:code  :invalid-request
                       :field field})))
    n))

(defn- require-uuid
  [x field]
  (cond
    (instance? UUID x)
    x

    (string? x)
    (try
      (UUID/fromString x)
      (catch IllegalArgumentException e
        (throw (ex-info (str field " must be a valid UUID.")
                        {:code  :invalid-request
                         :field field}
                        e))))

    :else
    (throw (ex-info (str field " must be a UUID string.")
                    {:code  :invalid-request
                     :field field}))))

(defn- page-limit
  [args]
  (if (contains? args "limit")
    (require-nonnegative-long (get args "limit") "limit")
    default-page-limit))

(defn- page-offset
  [args]
  (if (contains? args "offset")
    (require-nonnegative-long (get args "offset") "offset")
    0))

(defn- normalize-index
  [x]
  (cond
    (keyword? x) x
    (string? x)  (keyword (if (str/starts-with? x ":")
                            (subs x 1)
                            x))
    :else        x))

(defn- require-keyword
  [x field]
  (let [k (normalize-index x)]
    (if (keyword? k)
      k
      (throw (ex-info (str field " must be a keyword.")
                      {:code  :invalid-request
                       :field field})))))

(defn- normalize-attr-set
  [x field]
  (cond
    (nil? x) nil
    (set? x) (into #{} (map #(require-keyword % field)) x)
    (sequential? x) (into #{} (map #(require-keyword % field)) x)
    (instance? java.util.Collection x)
    (into #{} (map #(require-keyword % field)) x)
    :else
    (throw (ex-info (str field " must be a collection of attributes.")
                    {:code  :invalid-request
                     :field field}))))

(defn- normalize-rename-map
  [x field]
  (cond
    (nil? x) nil
    (map? x)
    (into {}
          (map (fn [[old new]]
                 [(require-keyword old field)
                  (require-keyword new field)]))
          x)
    (instance? java.util.Map x)
    (normalize-rename-map (into {} x) field)
    :else
    (throw (ex-info (str field " must be an object.")
                    {:code  :invalid-request
                     :field field}))))

(defn- normalize-db-store-type
  [x field]
  (case x
    ("datalog" :datalog) c/db-store-datalog
    ("kv" "key-value" :kv :key-value) c/db-store-kv
    ("engine" :engine) "engine"
    (throw (ex-info (str field " must be one of datalog, kv, or engine.")
                    {:code  :invalid-request
                     :field field
                     :value x}))))

(defn- normalize-db-type
  [x field]
  (case x
    ("datalog" :datalog) c/dl-type
    ("kv" "key-value" :kv :key-value) c/kv-type
    (throw (ex-info (str field " must be one of datalog or kv.")
                    {:code  :invalid-request
                     :field field
                     :value x}))))

(defn- paged-results
  [coll args]
  (let [offset (page-offset args)
        limit  (page-limit args)]
    (->> coll
         (drop offset)
         (take limit)
         vec)))

(defn- resolve-conn
  ([h]
   (resolve-typed-handle h :conn))
  ([context h]
   (if (and (:in-transaction? context)
            (= (:conn-handle context) h))
     (:transaction-conn context)
     (resolve-conn h))))

(defn- resolve-db
  ([h]
   (d/db (resolve-conn h)))
  ([context h]
   (d/db (resolve-conn context h))))

(defn- resolve-kv
  ([h]
   (resolve-typed-handle h :kv))
  ([context h]
   (if (and (:in-kv-transaction? context)
            (= (:kv-handle context) h))
     (:transaction-kv context)
     (resolve-kv h))))

(defn- resolve-client
  [h]
  (resolve-typed-handle h :client))

(defn- resolve-search-writer
  [h]
  (resolve-typed-handle h :search-writer))

(defn- resolve-search
  [h]
  (resolve-typed-handle h :search))

(defn- resolve-vec
  [h]
  (resolve-typed-handle h :vec))

(defn- reject-active-transaction-handle!
  [context h op]
  (when (and (:in-transaction? context)
             (= (:conn-handle context) h))
    (throw (ex-info
             (str op " is not allowed on the active transaction handle.")
             {:code :invalid-op-context
              :op   op}))))

(defn- reject-active-kv-transaction-handle!
  [context h op]
  (when (and (:in-kv-transaction? context)
             (= (:kv-handle context) h))
    (throw (ex-info
             (str op " is not allowed on the active transaction handle.")
             {:code :invalid-op-context
              :op   op}))))

(defn- reject-active-handle!
  [context h op]
  (reject-active-transaction-handle! context h op)
  (reject-active-kv-transaction-handle! context h op))

(defn- read-edn-string
  [field x]
  (try
    (edn/read-string (require-string x field))
    (catch Exception e
      (throw (ex-info (str field " must be a valid EDN string.")
                      {:code  :invalid-request
                       :field field}
                      e)))))

(declare normalize-pull-selector)

(defn- normalize-pull-attr-map
  [m]
  (let [attr (or (get m :attr)
                 (get m "attr"))]
    (when-not attr
      (throw (ex-info "Tagged pull attr must include :attr."
                      {:code :invalid-request})))
    (reduce (fn [expr k]
              (if (contains? m k)
                (conj expr k (normalize-pull-selector (get m k)))
                expr))
            [attr]
            tx-pull-option-order)))

(defn- normalize-pull-selector
  [x]
  (cond
    (jc/pull-form? x)
    (let [value   (require-map (jc/pull-form-value x))
          attr    (normalize-pull-attr-map
                    (require-map (get value :attr (get value "attr"))))
          pattern (normalize-pull-selector
                    (get value :pattern (get value "pattern")))]
      {attr pattern})

    (map? x)
    (into {}
          (map (fn [[k v]]
                 [(normalize-pull-selector k)
                  (normalize-pull-selector v)]))
          x)

    (vector? x)
    (mapv normalize-pull-selector x)

    (and (string? x) (= "*" x))
    '*

    :else
    x))

(defn- normalize-query-input
  [context x]
  (cond
    (jc/handle-ref? x)
    (resolve-db context (jc/handle-ref-value x))

    (map? x)
    (into {}
          (map (fn [[k v]]
                 [(normalize-query-input context k)
                  (normalize-query-input context v)]))
          x)

    (vector? x)
    (mapv #(normalize-query-input context %) x)

    (set? x)
    (into #{} (map #(normalize-query-input context %)) x)

    (seq? x)
    (map #(normalize-query-input context %) x)

    :else
    x))

(defn- tx-report->json
  [report]
  (let [tx-data (:tx-data report)]
    {"tx-data" (or tx-data [])
     "tempids" (into {}
                     (map (fn [[k v]]
                            [(str k) v]))
                     (:tempids report))
     "tx-id"   (when-let [datom (first tx-data)]
                 (dd/datom-tx datom))
     "tx-meta" (:tx-meta report)}))

(defn- handle-create-conn
  [args _context]
  (register! "conn" :conn (invoke-conn-fn d/create-conn args)))

(defn- handle-get-conn
  [args _context]
  (register! "conn" :conn (invoke-conn-fn d/get-conn args)))

(defn- handle-close
  [args context]
  (let [h (get args "conn")]
    (reject-active-transaction-handle! context h "close")
    (resolve-conn context h)
    (release! h)))

(defn- handle-closed?
  [args context]
  (d/closed? (resolve-conn context (get args "conn"))))

(defn- handle-list-handles
  [_args _context]
  (into {}
        (map (fn [[h {:keys [type]}]]
               [h type]))
        (session-handles)))

(defn- handle-release-handle
  [args context]
  (let [h (get args "handle")]
    (reject-active-handle! context h "release-handle")
    (release! h)))

(defn- handle-open-kv
  [args _context]
  (register! "kv" :kv
             (if (contains? args "opts")
               (d/open-kv (get args "dir") (get args "opts"))
               (d/open-kv (get args "dir")))))

(defn- handle-close-kv
  [args context]
  (let [h (get args "kv")]
    (reject-active-kv-transaction-handle! context h "close-kv")
    (resolve-kv context h)
    (release! h)))

(defn- handle-closed-kv?
  [args _context]
  (d/closed-kv? (resolve-kv (get args "kv"))))

(defn- handle-dir
  [args context]
  (cond
    (contains? args "kv")
    (d/dir (resolve-kv context (get args "kv")))

    (contains? args "conn")
    (d/dir (:store (resolve-db context (get args "conn"))))

    :else
    (throw (ex-info "dir requires kv or conn."
                    {:code :invalid-request}))))

(defn- handle-open-dbi
  [args context]
  (let [kv       (resolve-kv context (get args "kv"))
        dbi-name (get args "dbi-name")]
    (if (contains? args "opts")
      (d/open-dbi kv dbi-name (get args "opts"))
      (d/open-dbi kv dbi-name))
    true))

(defn- handle-clear-dbi
  [args context]
  (d/clear-dbi (resolve-kv context (get args "kv"))
               (get args "dbi-name"))
  true)

(defn- handle-drop-dbi
  [args context]
  (d/drop-dbi (resolve-kv context (get args "kv"))
              (get args "dbi-name"))
  true)

(defn- handle-list-dbis
  [args context]
  (vec (d/list-dbis (resolve-kv context (get args "kv")))))

(defn- handle-stat
  [args context]
  (if (contains? args "dbi-name")
    (d/stat (resolve-kv context (get args "kv"))
            (get args "dbi-name"))
    (d/stat (resolve-kv context (get args "kv")))))

(defn- handle-entries
  [args context]
  (d/entries (resolve-kv context (get args "kv"))
             (get args "dbi-name")))

(defn- handle-copy
  [args context]
  (if (contains? args "compact?")
    (d/copy (resolve-kv context (get args "kv"))
            (get args "dest")
            (get args "compact?"))
    (d/copy (resolve-kv context (get args "kv"))
            (get args "dest")))
  true)

(defn- handle-set-env-flags
  [args context]
  (d/set-env-flags (resolve-kv context (get args "kv"))
                   (set (get args "flags"))
                   (get args "on-off"))
  true)

(defn- handle-get-env-flags
  [args context]
  (d/get-env-flags (resolve-kv context (get args "kv"))))

(defn- handle-sync
  [args context]
  (if (contains? args "force")
    (d/sync (resolve-kv context (get args "kv"))
            (long (get args "force")))
    (d/sync (resolve-kv context (get args "kv"))))
  true)

(defn- handle-squuid
  [_args _context]
  (str (d/squuid)))

(defn- handle-squuid-time-millis
  [args _context]
  (d/squuid-time-millis (require-uuid (get args "uuid") "uuid")))

(defn- handle-hexify-string
  [args _context]
  (d/hexify-string (require-string (get args "s") "s")))

(defn- handle-unhexify-string
  [args _context]
  (d/unhexify-string (require-string (get args "s") "s")))

(defn- handle-schema
  [args context]
  (d/schema (resolve-conn context (get args "conn"))))

(defn- handle-update-schema
  [args context]
  (d/update-schema (resolve-conn context (get args "conn"))
                   (get args "schema-update")
                   (normalize-attr-set (get args "del-attrs") "del-attrs")
                   (normalize-rename-map (get args "rename-map")
                                         "rename-map")))

(defn- handle-opts
  [args context]
  (d/opts (resolve-conn context (get args "conn"))))

(defn- handle-clear
  [args context]
  (let [h (get args "conn")]
    (reject-active-transaction-handle! context h "clear")
    (d/clear (resolve-conn context h))
    true))

(defn- handle-max-eid
  [args context]
  (d/max-eid (resolve-db context (get args "conn"))))

(defn- handle-datalog-index-cache-limit
  [args context]
  (let [db (resolve-db context (get args "conn"))]
    (if (contains? args "limit")
      (do
        (d/datalog-index-cache-limit db (long (get args "limit")))
        (d/datalog-index-cache-limit db))
      (d/datalog-index-cache-limit db))))

(defn- handle-entid
  [args context]
  (d/entid (resolve-db context (get args "conn"))
           (get args "eid")))

(defn- handle-entity
  [args context]
  (when-let [entity (d/entity (resolve-db context (get args "conn"))
                              (get args "eid"))]
    (d/touch entity)))

(defn- handle-pull
  [args context]
  (d/pull (resolve-db context (get args "conn"))
          (normalize-pull-selector (get args "selector"))
          (get args "eid")))

(defn- handle-pull-many
  [args context]
  (d/pull-many (resolve-db context (get args "conn"))
               (normalize-pull-selector (get args "selector"))
               (require-vector (get args "eids") "eids")))

(defn- query-inputs
  [context args]
  (mapv #(normalize-query-input context %)
        (require-vector (get args "inputs" []) "inputs")))

(defn- handle-q
  [args context]
  (apply d/q
         (read-edn-string "query" (get args "query"))
         (resolve-db context (get args "conn"))
         (query-inputs context args)))

(defn- handle-explain
  [args context]
  (apply d/explain
         (if (contains? args "opts")
           (read-edn-string "opts" (get args "opts"))
           {})
         (read-edn-string "query" (get args "query"))
         (resolve-db context (get args "conn"))
         (query-inputs context args)))

(defn- handle-datoms
  [args context]
  (paged-results
    (d/datoms (resolve-db context (get args "conn"))
              (normalize-index (get args "index"))
              (get args "c1")
              (get args "c2")
              (get args "c3"))
    args))

(defn- handle-search-datoms
  [args context]
  (paged-results
    (d/search-datoms (resolve-db context (get args "conn"))
                     (get args "e")
                     (get args "a")
                     (get args "v"))
    args))

(defn- handle-count-datoms
  [args context]
  (d/count-datoms (resolve-db context (get args "conn"))
                  (get args "e")
                  (get args "a")
                  (get args "v")))

(defn- handle-seek-datoms
  [args context]
  (paged-results
    (d/seek-datoms (resolve-db context (get args "conn"))
                   (normalize-index (get args "index"))
                   (get args "c1")
                   (get args "c2")
                   (get args "c3"))
    args))

(defn- handle-rseek-datoms
  [args context]
  (paged-results
    (d/rseek-datoms (resolve-db context (get args "conn"))
                    (normalize-index (get args "index"))
                    (get args "c1")
                    (get args "c2")
                    (get args "c3"))
    args))

(defn- handle-fulltext-datoms
  [args context]
  (if (contains? args "opts")
    (d/fulltext-datoms (resolve-db context (get args "conn"))
                       (get args "query")
                       (get args "opts"))
    (d/fulltext-datoms (resolve-db context (get args "conn"))
                       (get args "query"))))

(defn- handle-index-range
  [args context]
  (paged-results
    (d/index-range (resolve-db context (get args "conn"))
                   (get args "attr")
                   (get args "start")
                   (get args "end"))
    args))

(defn- handle-cardinality
  [args context]
  (d/cardinality (resolve-db context (get args "conn"))
                 (get args "attr")))

(defn- handle-analyze
  [args context]
  (if (contains? args "attr")
    (d/analyze (resolve-db context (get args "conn"))
               (get args "attr"))
    (d/analyze (resolve-db context (get args "conn")))))

(defn- invoke-kv-k-type-op
  [f kv fixed-args args]
  (if (contains? args "k-type")
    (apply f kv (concat fixed-args [(get args "k-type")]))
    (apply f kv fixed-args)))

(defn- invoke-kv-typed-op
  [f kv fixed-args args]
  (cond
    (contains? args "ignore-key?")
    (apply f kv (concat fixed-args [(get args "k-type" :data)
                                    (get args "v-type" :data)
                                    (get args "ignore-key?")]))

    (contains? args "v-type")
    (apply f kv (concat fixed-args [(get args "k-type" :data)
                                    (get args "v-type")]))

    (contains? args "k-type")
    (apply f kv (concat fixed-args [(get args "k-type")]))

    :else
    (apply f kv fixed-args)))

(defn- handle-transact-kv
  [args context]
  (let [kv  (resolve-kv context (get args "kv"))
        txs (get args "txs")]
    (cond
      (contains? args "v-type")
      (d/transact-kv kv
                     (get args "dbi-name")
                     txs
                     (get args "k-type")
                     (get args "v-type"))

      (contains? args "k-type")
      (d/transact-kv kv
                     (get args "dbi-name")
                     txs
                     (get args "k-type"))

      (contains? args "dbi-name")
      (d/transact-kv kv
                     (get args "dbi-name")
                     txs)

      :else
      (d/transact-kv kv txs))))

(defn- handle-get-value
  [args context]
  (invoke-kv-typed-op d/get-value
                      (resolve-kv context (get args "kv"))
                      [(get args "dbi") (get args "k")]
                      args))

(defn- handle-get-rank
  [args context]
  (invoke-kv-k-type-op d/get-rank
                       (resolve-kv context (get args "kv"))
                       [(get args "dbi") (get args "k")]
                       args))

(defn- handle-get-by-rank
  [args context]
  (invoke-kv-typed-op d/get-by-rank
                      (resolve-kv context (get args "kv"))
                      [(get args "dbi")
                       (require-nonnegative-long (get args "rank") "rank")]
                      (if (contains? args "ignore-key?")
                        args
                        (assoc args "ignore-key?" false))))

(defn- handle-sample-kv
  [args context]
  (invoke-kv-typed-op d/sample-kv
                      (resolve-kv context (get args "kv"))
                      [(get args "dbi")
                       (require-nonnegative-long (get args "n") "n")]
                      args))

(defn- handle-get-first
  [args context]
  (invoke-kv-typed-op d/get-first
                      (resolve-kv context (get args "kv"))
                      [(get args "dbi") (get args "k-range")]
                      args))

(defn- handle-get-first-n
  [args context]
  (vec
    (invoke-kv-typed-op d/get-first-n
                        (resolve-kv context (get args "kv"))
                        [(get args "dbi")
                         (require-nonnegative-long (get args "n") "n")
                         (get args "k-range")]
                        args)))

(defn- handle-get-range
  [args context]
  (paged-results
    (invoke-kv-typed-op d/get-range
                        (resolve-kv context (get args "kv"))
                        [(get args "dbi") (get args "k-range")]
                        args)
    args))

(defn- handle-key-range
  [args context]
  (paged-results
    (invoke-kv-k-type-op d/key-range
                         (resolve-kv context (get args "kv"))
                         [(get args "dbi") (get args "k-range")]
                         args)
    args))

(defn- handle-key-range-count
  [args context]
  (invoke-kv-k-type-op d/key-range-count
                       (resolve-kv context (get args "kv"))
                       [(get args "dbi") (get args "k-range")]
                       args))

(defn- handle-range-count
  [args context]
  (invoke-kv-k-type-op d/range-count
                       (resolve-kv context (get args "kv"))
                       [(get args "dbi") (get args "k-range")]
                       args))

(defn- handle-open-list-dbi
  [args context]
  (let [kv        (resolve-kv context (get args "kv"))
        list-name (get args "list-name")]
    (if (contains? args "opts")
      (d/open-list-dbi kv list-name (get args "opts"))
      (d/open-list-dbi kv list-name))
    true))

(defn- handle-put-list-items
  [args context]
  (d/put-list-items (resolve-kv context (get args "kv"))
                    (get args "list-name")
                    (get args "k")
                    (get args "vs")
                    (get args "k-type")
                    (get args "v-type")))

(defn- handle-del-list-items
  [args context]
  (if (contains? args "vs")
    (d/del-list-items (resolve-kv context (get args "kv"))
                      (get args "list-name")
                      (get args "k")
                      (get args "vs")
                      (get args "k-type")
                      (get args "v-type"))
    (d/del-list-items (resolve-kv context (get args "kv"))
                      (get args "list-name")
                      (get args "k")
                      (get args "k-type"))))

(defn- handle-get-list
  [args context]
  (paged-results
    (d/get-list (resolve-kv context (get args "kv"))
                (get args "list-name")
                (get args "k")
                (get args "k-type")
                (get args "v-type"))
    args))

(defn- handle-list-count
  [args context]
  (d/list-count (resolve-kv context (get args "kv"))
                (get args "list-name")
                (get args "k")
                (get args "k-type")))

(defn- handle-in-list?
  [args context]
  (boolean
    (d/in-list? (resolve-kv context (get args "kv"))
                (get args "list-name")
                (get args "k")
                (get args "v")
                (get args "k-type")
                (get args "v-type"))))

(defn- handle-list-range
  [args context]
  (paged-results
    (d/list-range (resolve-kv context (get args "kv"))
                  (get args "list-name")
                  (get args "k-range")
                  (get args "k-type")
                  (get args "v-range")
                  (get args "v-type"))
    args))

(defn- handle-list-range-count
  [args context]
  (d/list-range-count (resolve-kv context (get args "kv"))
                      (get args "list-name")
                      (get args "k-range")
                      (get args "k-type")))

(defn- handle-list-range-first
  [args context]
  (d/list-range-first (resolve-kv context (get args "kv"))
                      (get args "list-name")
                      (get args "k-range")
                      (get args "k-type")
                      (get args "v-range")
                      (get args "v-type")))

(defn- handle-list-range-first-n
  [args context]
  (vec
    (d/list-range-first-n (resolve-kv context (get args "kv"))
                          (get args "list-name")
                          (require-nonnegative-long (get args "n") "n")
                          (get args "k-range")
                          (get args "k-type")
                          (get args "v-range")
                          (get args "v-type"))))

(defn- handle-key-range-list-count
  [args context]
  (d/key-range-list-count (resolve-kv context (get args "kv"))
                          (or (get args "list-name")
                              (get args "dbi"))
                          (get args "k-range")
                          (get args "k-type")))

(defn- handle-transact!
  [args context]
  (tx-report->json
    (if (contains? args "tx-meta")
      (d/transact! (resolve-conn context (get args "conn"))
                   (get args "tx-data")
                   (get args "tx-meta"))
      (d/transact! (resolve-conn context (get args "conn"))
                   (get args "tx-data")))))

(declare raw-result-for-request)

(defn- aborted-transaction?
  [t]
  (true? (get (ex-data t) aborted-transaction-key)))

(defn- handle-abort-transact
  [_args context]
  (when-not (:in-transaction? context)
    (throw (ex-info "abort-transact is only valid inside with-transaction."
                    {:code :invalid-op-context
                     :op   "abort-transact"})))
  (d/abort-transact (:transaction-conn context))
  (throw (ex-info "Transaction aborted."
                  {:code                    :transaction-aborted
                   :op                      "abort-transact"
                   aborted-transaction-key  true})))

(defn- handle-abort-transact-kv
  [_args context]
  (when-not (:in-kv-transaction? context)
    (throw (ex-info "abort-transact-kv is only valid inside with-transaction-kv."
                    {:code :invalid-op-context
                     :op   "abort-transact-kv"})))
  (d/abort-transact-kv (:transaction-kv context))
  (throw (ex-info "Transaction aborted."
                  {:code                    :transaction-aborted
                   :op                      "abort-transact-kv"
                   aborted-transaction-key  true})))

(defn- handle-with-transaction
  [args context]
  (when (:in-transaction? context)
    (throw (ex-info "Nested with-transaction is not supported."
                    {:code :invalid-op-context
                     :op   "with-transaction"})))
  (let [h   (get args "conn")
        ops (require-vector (get args "ops") "ops")
        c   (resolve-conn context h)]
    (try
      (d/with-transaction [tx-conn c]
        (loop [pending ops
               result  nil]
          (if-let [op-request (first pending)]
            (recur (next pending)
                   (raw-result-for-request
                     op-request
                     {:in-transaction? true
                      :conn-handle     h
                      :transaction-conn tx-conn}))
            result)))
      (catch clojure.lang.ExceptionInfo e
        (if (aborted-transaction? e)
          nil
          (throw e))))))

(defn- handle-with-transaction-kv
  [args context]
  (let [h   (get args "kv")
        ops (require-vector (get args "ops") "ops")]
    (when (and (:in-kv-transaction? context)
               (not= (:kv-handle context) h))
      (throw (ex-info
               "Nested with-transaction-kv is only supported on the active kv handle."
               {:code :invalid-op-context
                :op   "with-transaction-kv"})))
    (let [kv (resolve-kv context h)]
      (try
        (d/with-transaction-kv [tx-kv kv]
          (loop [pending ops
                 result  nil]
            (if-let [op-request (first pending)]
              (recur (next pending)
                     (raw-result-for-request
                       op-request
                       (assoc context
                              :in-kv-transaction? true
                              :kv-handle          h
                              :transaction-kv     tx-kv)))
              result)))
        (catch clojure.lang.ExceptionInfo e
          (if (aborted-transaction? e)
            nil
            (throw e)))))))

(defn- handle-tx-data->simulated-report
  [args context]
  (tx-report->json
    (d/tx-data->simulated-report (resolve-db context (get args "conn"))
                                 (get args "tx-data"))))

(defn- handle-new-search-engine
  [args _context]
  (register! "search" :search
             (if (contains? args "opts")
               (d/new-search-engine (resolve-kv (get args "kv"))
                                    (get args "opts"))
               (d/new-search-engine (resolve-kv (get args "kv"))))))

(defn- handle-add-doc
  [args _context]
  (if (contains? args "check-exist?")
    (d/add-doc (resolve-search (get args "search"))
               (get args "doc-ref")
               (require-string (get args "doc-text") "doc-text")
               (get args "check-exist?"))
    (d/add-doc (resolve-search (get args "search"))
               (get args "doc-ref")
               (require-string (get args "doc-text") "doc-text")))
  true)

(defn- handle-remove-doc
  [args _context]
  (d/remove-doc (resolve-search (get args "search"))
                (get args "doc-ref"))
  true)

(defn- handle-clear-docs
  [args _context]
  (d/clear-docs (resolve-search (get args "search")))
  true)

(defn- handle-doc-indexed?
  [args _context]
  (boolean
    (d/doc-indexed? (resolve-search (get args "search"))
                    (get args "doc-ref"))))

(defn- handle-doc-count
  [args _context]
  (d/doc-count (resolve-search (get args "search"))))

(defn- handle-search
  [args _context]
  (vec
    (if (contains? args "opts")
      (d/search (resolve-search (get args "search"))
                (get args "query")
                (get args "opts"))
      (d/search (resolve-search (get args "search"))
                (get args "query")))))

(defn- handle-search-re-index
  [args _context]
  (let [h (get args "search")]
    (rebind! h :search
             (d/re-index (resolve-search h)
                        (get args "opts" {})))
    h))

(defn- handle-search-index-writer
  [args context]
  (register! "search-writer" :search-writer
             (if (contains? args "opts")
               (d/search-index-writer (resolve-kv context (get args "kv"))
                                      (get args "opts"))
               (d/search-index-writer (resolve-kv context (get args "kv"))))))

(defn- handle-search-write
  [args _context]
  (d/write (resolve-search-writer (get args "search-writer"))
           (get args "doc-ref")
           (require-string (get args "doc-text") "doc-text")))

(defn- handle-search-commit
  [args _context]
  (let [h      (get args "search-writer")
        writer (resolve-search-writer h)
        result (d/commit writer)]
    (release! h)
    result))

(defn- handle-new-vector-index
  [args _context]
  (register! "vec" :vec
             (d/new-vector-index (resolve-kv (get args "kv"))
                                 (require-map (get args "opts")))))

(defn- handle-close-vector-index
  [args _context]
  (let [h (get args "vec")]
    (resolve-vec h)
    (release! h)))

(defn- handle-clear-vector-index
  [args _context]
  (let [h     (get args "vec")
        index (resolve-vec h)]
    (d/clear-vector-index index)
    (unregister! h)))

(defn- handle-force-vec-checkpoint!
  [args _context]
  (d/force-vec-checkpoint! (resolve-vec (get args "vec")))
  true)

(defn- handle-vector-checkpoint-state
  [args _context]
  (d/vector-checkpoint-state (resolve-vec (get args "vec"))))

(defn- handle-vector-index-info
  [args _context]
  (d/vector-index-info (resolve-vec (get args "vec"))))

(defn- handle-add-vec
  [args _context]
  (d/add-vec (resolve-vec (get args "vec"))
             (get args "vec-ref")
             (get args "vec-data"))
  true)

(defn- handle-remove-vec
  [args _context]
  (d/remove-vec (resolve-vec (get args "vec"))
                (get args "vec-ref"))
  true)

(defn- handle-vec-indexed?
  [args _context]
  (boolean
    (i/vec-indexed? (resolve-vec (get args "vec"))
                    (get args "vec-ref"))))

(defn- handle-search-vec
  [args _context]
  (vec
    (if (contains? args "opts")
      (d/search-vec (resolve-vec (get args "vec"))
                    (get args "query-vec")
                    (get args "opts"))
      (d/search-vec (resolve-vec (get args "vec"))
                    (get args "query-vec")))))

(defn- handle-vec-re-index
  [args _context]
  (let [h (get args "vec")]
    (rebind! h :vec
             (d/re-index (resolve-vec h)
                        (get args "opts" {})))
    h))

(defn- handle-new-client
  [args _context]
  (register! "client" :client
             (if (contains? args "opts")
               (dc/new-client (require-string (get args "uri") "uri")
                              (get args "opts"))
               (dc/new-client (require-string (get args "uri") "uri")))))

(defn- handle-close-client
  [args _context]
  (let [h (get args "client")]
    (resolve-client h)
    (release! h)))

(defn- handle-disconnected?
  [args _context]
  (dc/disconnected? (resolve-client (get args "client"))))

(defn- handle-client-id
  [args _context]
  (dc/get-id (resolve-client (get args "client"))))

(defn- handle-open-database
  [args _context]
  (dc/open-database (resolve-client (get args "client"))
                    (require-string (get args "db-name") "db-name")
                    (normalize-db-store-type (get args "db-type") "db-type")
                    (get args "schema")
                    (get args "opts")
                    (boolean (get args "return-db-info?" false))))

(defn- handle-close-database
  [args _context]
  (dc/close-database (resolve-client (get args "client"))
                     (require-string (get args "db-name") "db-name"))
  true)

(defn- handle-create-database
  [args _context]
  (dc/create-database (resolve-client (get args "client"))
                      (require-string (get args "db-name") "db-name")
                      (normalize-db-type (get args "db-type") "db-type"))
  true)

(defn- handle-drop-database
  [args _context]
  (dc/drop-database (resolve-client (get args "client"))
                    (require-string (get args "db-name") "db-name"))
  true)

(defn- handle-list-databases
  [args _context]
  (dc/list-databases (resolve-client (get args "client"))))

(defn- handle-list-databases-in-use
  [args _context]
  (dc/list-databases-in-use (resolve-client (get args "client"))))

(defn- handle-create-user
  [args _context]
  (dc/create-user (resolve-client (get args "client"))
                  (require-string (get args "username") "username")
                  (require-string (get args "password") "password"))
  true)

(defn- handle-drop-user
  [args _context]
  (dc/drop-user (resolve-client (get args "client"))
                (require-string (get args "username") "username"))
  true)

(defn- handle-reset-password
  [args _context]
  (dc/reset-password (resolve-client (get args "client"))
                     (require-string (get args "username") "username")
                     (require-string (get args "password") "password"))
  true)

(defn- handle-list-users
  [args _context]
  (dc/list-users (resolve-client (get args "client"))))

(defn- handle-create-role
  [args _context]
  (dc/create-role (resolve-client (get args "client"))
                  (require-keyword (get args "role") "role"))
  true)

(defn- handle-drop-role
  [args _context]
  (dc/drop-role (resolve-client (get args "client"))
                (require-keyword (get args "role") "role"))
  true)

(defn- handle-list-roles
  [args _context]
  (dc/list-roles (resolve-client (get args "client"))))

(defn- handle-assign-role
  [args _context]
  (dc/assign-role (resolve-client (get args "client"))
                  (require-keyword (get args "role") "role")
                  (require-string (get args "username") "username"))
  true)

(defn- handle-withdraw-role
  [args _context]
  (dc/withdraw-role (resolve-client (get args "client"))
                    (require-keyword (get args "role") "role")
                    (require-string (get args "username") "username"))
  true)

(defn- handle-list-user-roles
  [args _context]
  (dc/list-user-roles (resolve-client (get args "client"))
                      (require-string (get args "username") "username")))

(defn- handle-grant-permission
  [args _context]
  (dc/grant-permission (resolve-client (get args "client"))
                       (require-keyword (get args "role") "role")
                       (require-keyword (get args "act") "act")
                       (require-keyword (get args "obj") "obj")
                       (get args "tgt"))
  true)

(defn- handle-revoke-permission
  [args _context]
  (dc/revoke-permission (resolve-client (get args "client"))
                        (require-keyword (get args "role") "role")
                        (require-keyword (get args "act") "act")
                        (require-keyword (get args "obj") "obj")
                        (get args "tgt"))
  true)

(defn- handle-list-role-permissions
  [args _context]
  (dc/list-role-permissions (resolve-client (get args "client"))
                            (require-keyword (get args "role") "role")))

(defn- handle-list-user-permissions
  [args _context]
  (dc/list-user-permissions (resolve-client (get args "client"))
                            (require-string (get args "username") "username")))

(defn- handle-query-system
  [args _context]
  (apply dc/query-system
         (resolve-client (get args "client"))
         (read-edn-string "query" (get args "query"))
         (require-vector (get args "args" []) "args")))

(defn- handle-show-clients
  [args _context]
  (dc/show-clients (resolve-client (get args "client"))))

(defn- handle-disconnect-client
  [args _context]
  (dc/disconnect-client (resolve-client (get args "client"))
                        (require-uuid (get args "client-id") "client-id"))
  true)

(defn- resolve-txlog-target
  [args context]
  (cond
    (contains? args "kv")
    (resolve-kv context (get args "kv"))

    (contains? args "conn")
    (:store (resolve-db context (get args "conn")))

    :else
    (throw (ex-info "Operation requires kv or conn."
                    {:code :invalid-request}))))

(defn- reject-open-tx-log-offset
  [args]
  (when (contains? args "offset")
    (throw (ex-info "open-tx-log does not support offset."
                    {:code  :invalid-request
                     :field "offset"}))))

(defn- handle-txlog-watermarks
  [args context]
  (d/txlog-watermarks (resolve-txlog-target args context)))

(defn- handle-open-tx-log
  [args context]
  (reject-open-tx-log-offset args)
  (let [target (resolve-txlog-target args context)
        from   (require-nonnegative-long (get args "from-lsn") "from-lsn")
        rows   (if (contains? args "upto-lsn")
                 (d/open-tx-log target
                                from
                                (require-nonnegative-long (get args "upto-lsn")
                                                          "upto-lsn"))
                 (d/open-tx-log target from))]
    (->> rows
         (take (page-limit args))
         vec)))

(defn- handle-create-snapshot!
  [args context]
  (d/create-snapshot! (resolve-txlog-target args context)))

(defn- handle-list-snapshots
  [args context]
  (vec (d/list-snapshots (resolve-txlog-target args context))))

(defn- handle-gc-txlog-segments!
  [args context]
  (if (contains? args "retain-floor-lsn")
    (d/gc-txlog-segments! (resolve-txlog-target args context)
                          (require-nonnegative-long
                            (get args "retain-floor-lsn")
                            "retain-floor-lsn"))
    (d/gc-txlog-segments! (resolve-txlog-target args context))))

(defn- handle-re-index
  [args context]
  (cond
    (contains? args "kv")
    (let [h (get args "kv")]
      (reject-active-kv-transaction-handle! context h "re-index")
      (rebind! h :kv
               (d/re-index (resolve-kv context h)
                           (get args "opts" {})))
      h)

    (contains? args "conn")
    (let [h (get args "conn")]
      (reject-active-transaction-handle! context h "re-index")
      (rebind! h :conn
               (if (contains? args "schema")
                 (d/re-index (resolve-conn context h)
                             (get args "schema")
                             (get args "opts" {}))
                 (d/re-index (resolve-conn context h)
                             (get args "opts" {}))))
      h)

    :else
    (throw (ex-info "re-index requires kv or conn."
                    {:code :invalid-request}))))

(def ^:private op-registry
  {"api-info"                    (fn [_args _context] (api-info))
   "squuid"                      handle-squuid
   "squuid-time-millis"          handle-squuid-time-millis
   "hexify-string"               handle-hexify-string
   "unhexify-string"             handle-unhexify-string
   "create-conn"                 handle-create-conn
   "get-conn"                    handle-get-conn
   "new-client"                  handle-new-client
   "close-client"                handle-close-client
   "disconnect"                  handle-close-client
   "disconnected?"               handle-disconnected?
   "client-id"                   handle-client-id
   "get-id"                      handle-client-id
   "open-kv"                     handle-open-kv
   "close-kv"                    handle-close-kv
   "closed-kv?"                  handle-closed-kv?
   "dir"                         handle-dir
   "open-dbi"                    handle-open-dbi
   "clear-dbi"                   handle-clear-dbi
   "drop-dbi"                    handle-drop-dbi
   "list-dbis"                   handle-list-dbis
   "stat"                        handle-stat
   "entries"                     handle-entries
   "copy"                        handle-copy
   "re-index"                    handle-re-index
   "set-env-flags"               handle-set-env-flags
   "get-env-flags"               handle-get-env-flags
   "sync"                        handle-sync
   "txlog-watermarks"            handle-txlog-watermarks
   "open-tx-log"                 handle-open-tx-log
   "create-snapshot!"            handle-create-snapshot!
   "list-snapshots"              handle-list-snapshots
   "gc-txlog-segments!"          handle-gc-txlog-segments!
   "close"                       handle-close
   "closed?"                     handle-closed?
   "list-handles"                handle-list-handles
   "release-handle"              handle-release-handle
   "schema"                      handle-schema
   "update-schema"               handle-update-schema
   "opts"                        handle-opts
   "clear"                       handle-clear
   "max-eid"                     handle-max-eid
   "datalog-index-cache-limit"   handle-datalog-index-cache-limit
   "entid"                       handle-entid
   "entity"                      handle-entity
   "pull"                        handle-pull
   "pull-many"                   handle-pull-many
   "q"                           handle-q
   "explain"                     handle-explain
   "datoms"                      handle-datoms
   "search-datoms"               handle-search-datoms
   "count-datoms"                handle-count-datoms
   "seek-datoms"                 handle-seek-datoms
   "rseek-datoms"                handle-rseek-datoms
   "fulltext-datoms"             handle-fulltext-datoms
   "index-range"                 handle-index-range
   "cardinality"                 handle-cardinality
   "analyze"                     handle-analyze
   "transact-kv"                 handle-transact-kv
   "get-value"                   handle-get-value
   "get-rank"                    handle-get-rank
   "get-by-rank"                 handle-get-by-rank
   "sample-kv"                   handle-sample-kv
   "get-first"                   handle-get-first
   "get-first-n"                 handle-get-first-n
   "get-range"                   handle-get-range
   "key-range"                   handle-key-range
   "key-range-count"             handle-key-range-count
   "range-count"                 handle-range-count
   "open-list-dbi"               handle-open-list-dbi
   "put-list-items"              handle-put-list-items
   "del-list-items"              handle-del-list-items
   "get-list"                    handle-get-list
   "list-count"                  handle-list-count
   "in-list?"                    handle-in-list?
   "list-range"                  handle-list-range
   "list-range-count"            handle-list-range-count
   "list-range-first"            handle-list-range-first
   "list-range-first-n"          handle-list-range-first-n
   "key-range-list-count"        handle-key-range-list-count
   "transact!"                   handle-transact!
   "abort-transact"              handle-abort-transact
   "with-transaction"            handle-with-transaction
   "abort-transact-kv"           handle-abort-transact-kv
   "with-transaction-kv"         handle-with-transaction-kv
   "tx-data->simulated-report"   handle-tx-data->simulated-report
   "new-search-engine"           handle-new-search-engine
   "add-doc"                     handle-add-doc
   "remove-doc"                  handle-remove-doc
   "clear-docs"                  handle-clear-docs
   "doc-indexed?"                handle-doc-indexed?
   "doc-count"                   handle-doc-count
   "search"                      handle-search
   "search-index-writer"         handle-search-index-writer
   "search-write"                handle-search-write
   "search-commit"               handle-search-commit
   "search-re-index"             handle-search-re-index
   "new-vector-index"            handle-new-vector-index
   "close-vector-index"          handle-close-vector-index
   "close-vecs"                  handle-close-vector-index
   "clear-vector-index"          handle-clear-vector-index
   "clear-vecs"                  handle-clear-vector-index
   "force-vec-checkpoint!"       handle-force-vec-checkpoint!
   "persist-vecs"                handle-force-vec-checkpoint!
   "vector-checkpoint-state"     handle-vector-checkpoint-state
   "vector-index-info"           handle-vector-index-info
   "vecs-info"                   handle-vector-index-info
   "add-vec"                     handle-add-vec
   "remove-vec"                  handle-remove-vec
   "vec-indexed?"                handle-vec-indexed?
   "search-vec"                  handle-search-vec
   "vec-re-index"                handle-vec-re-index
   "open-database"               handle-open-database
   "close-database"              handle-close-database
   "create-database"             handle-create-database
   "drop-database"               handle-drop-database
   "list-databases"              handle-list-databases
   "list-databases-in-use"       handle-list-databases-in-use
   "create-user"                 handle-create-user
   "drop-user"                   handle-drop-user
   "reset-password"              handle-reset-password
   "list-users"                  handle-list-users
   "create-role"                 handle-create-role
   "drop-role"                   handle-drop-role
   "list-roles"                  handle-list-roles
   "assign-role"                 handle-assign-role
   "withdraw-role"               handle-withdraw-role
   "list-user-roles"             handle-list-user-roles
   "grant-permission"            handle-grant-permission
   "revoke-permission"           handle-revoke-permission
   "list-role-permissions"       handle-list-role-permissions
   "list-user-permissions"       handle-list-user-permissions
   "query-system"                handle-query-system
   "show-clients"                handle-show-clients
   "disconnect-client"           handle-disconnect-client})

(defn- unsupported-op
  [op]
  (throw (ex-info (str "Unsupported JSON API op: " op)
                  {:code :unsupported-op
                   :op   op})))

(defn- raw-result-for-request
  [request context]
  (let [request (require-map request)
        op      (get request "op")
        args    (require-map (get request "args"))]
    (when-not (string? op)
      (throw (ex-info "Request op must be a string."
                      {:code :invalid-request})))
    (let [handler (or (get op-registry op) (unsupported-op op))]
      (handler args context))))

(defn ^:no-doc exec-request
  ([request]
   (exec-request (current-session-state) request))
  ([session request]
   (binding [*session-state* session]
     (let [raw-result  (raw-result-for-request request nil)
           json-result (-> raw-result
                           jc/json-ready
                           enforce-result-item-limit!)]
       {"ok" true
        "result" json-result}))))

(defn- error-data->json
  [data]
  (letfn [(sanitize [x]
            (try
              (jc/json-ready x)
              x
              (catch clojure.lang.ExceptionInfo e
                (if (= :json-unsupported-type (:code (ex-data e)))
                  (cond
                    (map? x)
                    (into {}
                          (map (fn [[k v]]
                                 [(sanitize k) (sanitize v)]))
                          x)

                    (instance? java.util.Map x)
                    (sanitize (into {} x))

                    (vector? x)
                    (mapv sanitize x)

                    (set? x)
                    (into #{} (map sanitize x))

                    (sequential? x)
                    (mapv sanitize x)

                    (instance? java.util.Collection x)
                    (mapv sanitize x)

                    :else
                    (pr-str x))
                  (throw e)))))]
    (some-> data sanitize jc/json-ready)))

(defn- error-response
  [^Throwable t]
  (let [data (error-data->json (ex-data t))]
    (cond-> {"ok"    false
             "error" (or (.getMessage t)
                         (str (class t)))
             "type"  (.getName (class t))}
      (some? data) (assoc "data" data))))

(defn- write-response
  [response]
  (jc/write-json-ready-string response *json-api-limits*))

(defn- fallback-error-response
  [^Throwable t]
  (let [code (or (:code (ex-data t)) :result-too-large)]
    {"ok"    false
     "error" "Result too large."
     "data"  {":code" (str code)}}))

(defn- write-error-response
  [^Throwable t]
  (try
    (write-response (error-response t))
    (catch clojure.lang.ExceptionInfo e
      (if (= :result-too-large (:code (ex-data e)))
        (write-response (fallback-error-response t))
        (throw e)))))

(defn exec
  [^String json]
  (try
    (let [request  (jc/read-json-string json)
          response (exec-request request)]
      (try
        (write-response response)
        (catch clojure.lang.ExceptionInfo e
          (if (= :result-too-large (:code (ex-data e)))
            (write-error-response e)
            (throw e)))))
    (catch Throwable t
      (write-error-response t))))
