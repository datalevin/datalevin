;;
;; Copyright (c) Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0/)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns ^:no-doc datalevin.custom-data
  "Database-wide custom type definitions and runtime function materialization."
  (:require
   [datalevin.bits :as b]
   [datalevin.constants :as c]
   [datalevin.interface :as i]
   [datalevin.lmdb :as l]
   [datalevin.udf :as udf]
   [datalevin.util :refer [raise]])
  (:import
   [java.util Arrays]
   [java.util.regex Pattern]))

(def ^:private scalar-types (disj c/kv-value-types :data))
(def ^:private tuple-types (disj scalar-types :bytes :id))
(def ^:private revision-key :custom-types-revision)
(def ^:private registry-key-type [:keyword :keyword])

(defn- inter-fn? [x] (= :datalevin/inter-fn (:type (meta x))))

(defn- function-source [f]
  (let [source (:source (meta f))]
    ;; inter-fn emits a let even when none of the available locals is captured.
    (if (and (#{'let 'clojure.core/let} (first source))
             (empty? (second source)))
      (nth source 2)
      source)))

(defn- check-keys! [m allowed context]
  (when-not (map? m)
    (raise "Expected a custom type definition map"
           {:error :custom-type/definition :context context :value m}))
  (when-let [unknown (seq (remove allowed (keys m)))]
    (raise "Unsupported custom type option(s) " (vec unknown)
           {:error :custom-type/definition :context context
            :unsupported (vec unknown)})))

(defn- normalize-callable [f kind]
  (cond
    (inter-fn? f)
    (let [source (function-source f)
          ;; interpret depends on core, so resolve it only at API call time.
          compile-source (requiring-resolve
                          'datalevin.interpret/compile-inter-fn-source)
          _ (compile-source source)
          form (if (#{'let 'clojure.core/let} (first source))
                 (nth source 2) source)
          args (if (symbol? (second form)) (nth form 2) (second form))
          fixed (take-while #(not= '& %) args)]
      (when-not (if (some #{'&} args) (<= (count fixed) 1) (= 1 (count args)))
        (raise "Custom type functions must accept one argument"
               {:error :custom-type/arity :kind kind :source source}))
      ;; Keep source/captures opaque during metadata loading. Captured inter-fns
      ;; must not require the interpreter's Nippy extension just to open a DB.
      {:inter-fn/source (b/serialize source)})

    (udf/descriptor? f) (udf/ensure-kind f kind)

    :else
    (raise "Custom type functions must be inter-fn functions or UDF descriptors"
           {:error :custom-type/function :kind kind})))

(defn normalize-definition
  "Validate and detach a definition, retaining function source and UDF descriptors."
  [type-name definition]
  (when-not (and (qualified-keyword? type-name)
                 (not (contains? c/datalog-value-types type-name)))
    (raise "Custom type name must be a namespaced keyword distinct from built-in types"
           {:error :custom-type/name :type-name type-name}))
  (check-keys! definition #{:version :index :payload} :definition)
  (let [{:keys [version index] :or {version 1}} definition
        payload (get definition :payload :nippy)
        backing (:type index)]
    (when-not (and (int? version) (pos? (long version)))
      (raise "Custom type version must be a positive integer"
             {:error :custom-type/version :version version}))
    (check-keys! index #{:type :order-fn} :index)
    (when-not (if (vector? backing)
                (and (seq backing) (every? tuple-types backing))
                (contains? scalar-types backing))
      (raise "Unsupported custom type backing type " backing
             {:error :custom-type/backing-type :backing-type backing}))
    (when-not (= :nippy payload)
      (check-keys! payload #{:serialize :deserialize} :payload)
      (when-not (= #{:serialize :deserialize} (set (keys payload)))
        (raise "Custom payload functions require both serialize and deserialize"
               {:error :custom-type/payload})))
    ;; A round trip validates captures and isolates mutable input from metadata.
    (b/deserialize
     (b/serialize
      {:version version
       :index {:type backing
               :order-fn (normalize-callable (:order-fn index) :order-fn)}
       :payload (if (= :nippy payload)
                  :nippy
                  {:serialize (normalize-callable (:serialize payload) :serializer)
                   :deserialize (normalize-callable (:deserialize payload)
                                                    :deserializer)})}))))

(defn- same-source?
  "Compare persisted source/captures, including arrays and captured inter-fns."
  [a b]
  (cond
    (identical? a b) true
    (and (inter-fn? a) (inter-fn? b))
    (same-source? (function-source a) (function-source b))
    (and (map? a) (map? b))
    (and (or (not (or (record? a) (record? b))) (= (class a) (class b)))
         (= (set (keys a)) (set (keys b)))
         (every? (fn [[k v]] (same-source? v (get b k))) a))
    (and (sequential? a) (sequential? b))
    (and (= (vector? a) (vector? b))
         (= (count a) (count b)) (every? true? (map same-source? a b)))
    (and (set? a) (set? b))
    (and (= (count a) (count b))
         (every? (fn [x] (some #(same-source? x %) b)) a))
    (and (some? a) (some? b) (.isArray ^Class (class a))
         (= (class a) (class b)))
    (same-source? (seq a) (seq b))
    (and (instance? Pattern a) (instance? Pattern b))
    (and (= (.pattern ^Pattern a) (.pattern ^Pattern b))
         (= (.flags ^Pattern a) (.flags ^Pattern b)))
    :else (= a b)))

(defn- same-callable? [a b]
  (if (and (:inter-fn/source a) (:inter-fn/source b))
    (or (Arrays/equals ^bytes (:inter-fn/source a) ^bytes (:inter-fn/source b))
        (do
          ;; A restore may compare captures before the interpreter is loaded.
          (requiring-resolve 'datalevin.interpret/compile-inter-fn-source)
          (same-source? (b/deserialize (:inter-fn/source a))
                        (b/deserialize (:inter-fn/source b)))))
    (= a b)))

(defn same-definition? [a b]
  (and (= (:version a) (:version b))
       (= (get-in a [:index :type]) (get-in b [:index :type]))
       (same-callable? (get-in a [:index :order-fn]) (get-in b [:index :order-fn]))
       (if (or (= :nippy (:payload a)) (= :nippy (:payload b)))
         (= (:payload a) (:payload b))
         (every? #(same-callable? (get-in a [:payload %]) (get-in b [:payload %]))
                 [:serialize :deserialize]))))

(defn- local-info [kv]
  (when-not (satisfies? i/ILMDB kv)
    (raise "Expected a KV handle"
           {:error :custom-type/handle}))
  (i/check-ready kv)
  (or (i/kv-info kv)
      (raise "Custom type registration currently requires a local KV handle"
             {:error :custom-type/remote-unsupported})))

(defn- runtime-cache [kv]
  (:custom-type-cache @(local-info kv)))

(defn- persisted-revision ^long [kv]
  (long (or (i/get-value kv c/kv-info revision-key :keyword :data) 0)))

(defn- read-types [kv]
  (into {} (map (fn [[[_ type-name] definition]] [type-name definition]))
        (i/get-range kv c/kv-info
                     [:closed [:types c/v0] [:types c/vmax]]
                     registry-key-type :data)))

(defn registry
  "Read the registry visible to this handle, refreshing committed cache state."
  [kv]
  (let [cache (runtime-cache kv)]
    (if (l/writing? kv)
      ;; Never publish definitions visible only in an uncommitted transaction.
      {:revision (persisted-revision kv) :types (read-types kv)}
      (loop []
        (let [revision (persisted-revision kv)
              cached (:registry @cache)]
          (if (= revision (:revision cached))
            cached
            (let [types (read-types kv)]
              ;; Separate native reads may straddle another process's commit.
              (if (= revision (persisted-revision kv))
                (let [snapshot {:revision revision :types types}]
                  (swap! cache (fn [state]
                                 (if (<= (long (get-in state [:registry :revision] -1))
                                         revision)
                                   {:registry snapshot}
                                   state)))
                  snapshot)
                (recur)))))))))

(defn- register-local-type
  "Atomically register a type in a local KV environment; return its name."
  [kv type-name definition]
  (local-info kv)
  (let [definition (normalize-definition type-name definition)]
    (l/with-transaction-kv [tx kv]
      (if-let [installed (i/get-value tx c/kv-info [:types type-name]
                                     registry-key-type :data)]
        (when-not (same-definition? installed definition)
          (raise "Custom type is already registered with a different definition"
                 {:error :custom-type/conflict :type-name type-name}))
        (i/transact-kv
         tx [(l/kv-tx :put c/kv-info [:types type-name] definition
                       registry-key-type :data)
             (l/kv-tx :put c/kv-info revision-key (inc (persisted-revision tx))
                       :keyword :data)])))
    ;; Registry readers publish only after observing committed metadata. This
    ;; also handles registration nested in a caller-owned write transaction.
    type-name))

(defn register-type
  "Atomically register a database-wide type; return its name."
  [kv type-name definition]
  (if (satisfies? i/ICustomTypes kv)
    (do
      ;; Validate before transport; the server validates again after decoding.
      (normalize-definition type-name definition)
      (i/register-type kv type-name definition))
    (register-local-type kv type-name definition)))

(defn- materialize [kv type-name kind descriptor runtime-opts]
  (if-let [source (:inter-fn/source descriptor)]
    (let [compile-source (requiring-resolve
                          'datalevin.interpret/compile-inter-fn-source)]
      (compile-source (b/deserialize source)))
    (udf/materialize (:udf-registry runtime-opts)
                     {:kv kv :type-name type-name :kind kind :embedded? true}
                     descriptor false)))

(defn- valid-order-key? [value backing]
  (try
    (and (if (vector? backing)
           ;; A type vector declares a fixed arity, including a one-slot tuple.
           (and (vector? value) (= (count backing) (count value)))
           (some? value))
         (b/valid-data? value backing))
    (catch Exception _ false)))

(defn- invoke-function [callable type-name kind value]
  (try
    (@callable value)
    (catch Exception e
      (let [data (ex-data e)
            ;; Resolvers receive the live handle; error responses must not.
            data (cond-> data
                   (map? (:context data)) (update :context dissoc :kv))]
        (throw (ex-info (str "Custom type " type-name " " (name kind)
                             " failed: " (ex-message e))
                        (merge {:error :custom-type/function}
                               data
                               {:type-name type-name :kind kind})
                        e))))))

(defn- compile-type [kv type-name definition runtime-opts]
  (let [backing (get-in definition [:index :type])
        order (delay (materialize kv type-name :order-fn
                                  (get-in definition [:index :order-fn]) runtime-opts))
        payload (:payload definition)
        serialize (delay (if (= :nippy payload) b/serialize
                             (materialize kv type-name :serializer
                                          (:serialize payload) runtime-opts)))
        deserialize (delay (if (= :nippy payload) b/deserialize
                               (materialize kv type-name :deserializer
                                            (:deserialize payload) runtime-opts)))]
    {:type-name type-name
     :definition definition
     :order-fn (fn [value]
                 (let [key (invoke-function order type-name :order-fn value)]
                   (when-not (valid-order-key? key backing)
                     (raise "Custom order function returned an invalid backing value"
                            {:error :custom-type/order-key :type-name type-name
                             :backing-type backing :value key}))
                   key))
     :serialize (fn [value]
                  (let [payload (invoke-function serialize type-name :serializer value)]
                    (when-not (bytes? payload)
                      (raise "Custom serializer must return a byte array"
                             {:error :custom-type/payload :type-name type-name}))
                    payload))
     :deserialize (fn [payload]
                    (when-not (bytes? payload)
                      (raise "Custom deserializer requires a byte array"
                             {:error :custom-type/payload :type-name type-name}))
                    (invoke-function deserialize type-name :deserializer payload))}))

(defn resolve-type
  "Resolve a type for an operation. Optional runtime opts supply its UDF registry.
  Functions materialize on first use; committed descriptors are cached per UDF
  registry generation. Call again for each operation to observe new bindings."
  ([kv type-name]
   (resolve-type kv type-name (:runtime-opts @(local-info kv))))
  ([kv type-name runtime-opts]
   (let [{:keys [revision types]} (registry kv)
         definition (or (get types type-name)
                        (raise "Custom type is not registered " type-name
                               {:error :custom-type/not-found :type-name type-name}))
         udf-registry (:udf-registry runtime-opts)
         token [revision udf-registry (udf/generation udf-registry)]
         cache (runtime-cache kv)
         cached (get-in @cache [:compiled type-name])]
     (if (and (not (l/writing? kv)) (= token (:token cached)))
       (:type cached)
       (let [compiled (compile-type kv type-name definition runtime-opts)]
         (when-not (l/writing? kv)
           (swap! cache assoc-in [:compiled type-name]
                  {:token token :type compiled}))
         compiled)))))
