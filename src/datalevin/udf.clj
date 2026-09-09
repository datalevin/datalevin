;;
;; Copyright (c) Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns datalevin.udf
  "Runtime registry and descriptor handling for non-Clojure UDFs."
  (:refer-clojure :exclude [resolve])
  (:require
   [datalevin.util :as u :refer [raise]]))

(def ^:private required-keys
  [:udf/lang :udf/kind :udf/id])

(def ^:private binding-key-keys
  [:udf/lang :udf/kind :udf/id :udf/version])

(def ^:private allowed-keys
  (set binding-key-keys))

(defn- scalar-version?
  [x]
  (or (nil? x)
      (keyword? x)
      (string? x)
      (integer? x)))

(defn descriptor?
  [x]
  (and (map? x) (every? #(contains? x %) required-keys)))

(defn descriptor
  [x]
  (when-not (map? x)
    (raise "UDF descriptor must be a map, got " (type x)
           {:error :udf/descriptor :value x}))
  (doseq [k required-keys]
    (when-not (contains? x k)
      (raise "UDF descriptor is missing required key " k
             {:error :udf/descriptor :value x})))
  (when-let [unknown (seq (remove allowed-keys (keys x)))]
    (raise "UDF descriptor contains unsupported key(s) " (vec unknown)
           {:error :udf/descriptor :value x :unsupported (vec unknown)}))
  (when-not (keyword? (:udf/lang x))
    (raise "UDF descriptor :udf/lang must be a keyword"
           {:error :udf/descriptor :value x}))
  (when-not (keyword? (:udf/kind x))
    (raise "UDF descriptor :udf/kind must be a keyword"
           {:error :udf/descriptor :value x}))
  (when-not (keyword? (:udf/id x))
    (raise "UDF descriptor :udf/id must be a keyword"
           {:error :udf/descriptor :value x}))
  (when-not (scalar-version? (:udf/version x))
    (raise "UDF descriptor :udf/version must be a keyword, string, integer, or nil"
           {:error :udf/descriptor :value x}))
  x)

(defn- binding-key
  [descriptor]
  (select-keys descriptor binding-key-keys))

(defn- empty-state
  []
  {:generation 0
   :cache      {}
   :bindings   {}
   :resolvers  {}})

(defn create-registry
  []
  (atom (empty-state)))

(defn generation
  [registry]
  (long (:generation (if (instance? clojure.lang.IDeref registry)
                       @registry
                       (or registry (empty-state))))))

(defn- registry-state
  [registry]
  (if (instance? clojure.lang.IDeref registry)
    @registry
    (or registry (empty-state))))

(defn- ensure-registry
  [registry]
  (when-not (instance? clojure.lang.IAtom registry)
    (raise "UDF registry must be an atom created by create-registry"
           {:error :udf/registry :value registry}))
  registry)

(defn- clear-cache
  [state]
  (assoc state :cache {}))

(defn register!
  [registry udf-desc f]
  (when-not (ifn? f)
    (raise "UDF binding must be callable"
           {:error :udf/register :descriptor udf-desc :value f}))
  (let [registry (ensure-registry registry)
        udf-desc (descriptor udf-desc)]
    (swap! registry
           (fn [state]
             (-> state
                 clear-cache
                 (update :generation (fnil inc 0))
                 (assoc-in [:bindings (binding-key udf-desc)] f)))))
  registry)

(defn register-resolver!
  [registry lang resolver]
  (when-not (keyword? lang)
    (raise "UDF resolver language must be a keyword"
           {:error :udf/register-resolver :lang lang}))
  (when-not (ifn? resolver)
    (raise "UDF resolver must be callable"
           {:error :udf/register-resolver :lang lang :value resolver}))
  (let [registry (ensure-registry registry)]
    (swap! registry
           (fn [state]
             (-> state
                 clear-cache
                 (update :generation (fnil inc 0))
                 (assoc-in [:resolvers lang] resolver)))))
  registry)

(defn unregister!
  [registry udf-desc]
  (let [registry (ensure-registry registry)
        udf-desc (descriptor udf-desc)]
    (swap! registry
           (fn [state]
             (-> state
                 clear-cache
                 (update :generation (fnil inc 0))
                 (update :bindings dissoc (binding-key udf-desc))))))
  registry)

(defn registered?
  [registry udf-desc]
  (let [udf-desc   (descriptor udf-desc)
        state      (registry-state registry)]
    (contains? (:bindings state) (binding-key udf-desc))))

(defn registered-descriptor
  [registry allowed id]
  (when-not (keyword? id)
    (raise "Registered UDF id must be a keyword"
           {:error :udf/descriptor :value id}))
  (let [allowed (if (set? allowed) allowed #{allowed})
        matches (->> (keys (:bindings (registry-state registry)))
                     (filter (fn [descriptor]
                               (and (= (:udf/id descriptor) id)
                                    (contains? allowed (:udf/kind descriptor))))))]
    (cond
      (empty? matches)
      (raise "No UDF is registered for id " id
             {:error :udf/not-found :id id :allowed allowed})

      (= 1 (count matches))
      (first matches)

      :else
      (raise "Multiple UDF descriptors match id " id
             {:error :udf/ambiguous :id id :allowed allowed}))))

(defn ensure-kind
  [udf-desc allowed]
  (let [udf-desc   (descriptor udf-desc)
        allowed    (if (set? allowed) allowed #{allowed})]
    (when-not (contains? allowed (:udf/kind udf-desc))
      (raise "UDF descriptor kind mismatch: expected " allowed ", got "
             (:udf/kind udf-desc)
             {:error      :udf/kind-mismatch
              :descriptor udf-desc
              :allowed    allowed}))
    udf-desc))

(defn descriptor-or-registered
  [registry allowed value]
  (cond
    (descriptor? value)
    (ensure-kind value allowed)

    (keyword? value)
    (registered-descriptor registry allowed value)

    :else
    (raise "Expected a UDF descriptor map or registered id keyword"
           {:error :udf/descriptor :value value :allowed allowed})))

(defn materialize
  "Resolve a callable. Set cache? to false when the caller caches by a more
  specific runtime context, such as a custom type's KV environment."
  ([registry context udf-desc]
   (materialize registry context udf-desc true))
  ([registry context udf-desc cache?]
   (let [udf-desc (descriptor udf-desc)]
     (if-not registry
       (raise "No UDF registry is configured for descriptor " udf-desc
              {:error :udf/not-found :descriptor udf-desc :context context})
       (let [registry  (ensure-registry registry)
             cache-key udf-desc
             bind-key  (binding-key udf-desc)]
         (if-some [callable (or (when cache? (get-in @registry [:cache cache-key]))
                                (get-in @registry [:bindings bind-key]))]
           (do
             (when cache? (swap! registry assoc-in [:cache cache-key] callable))
             callable)
           (let [resolver (get-in @registry [:resolvers (:udf/lang udf-desc)])]
             (if-not resolver
               (raise "No UDF resolver found for descriptor " udf-desc
                      {:error :udf/not-found
                       :descriptor udf-desc
                       :context context})
               (if-some [callable (resolver context udf-desc)]
                 (do
                   (when cache?
                     (swap! registry assoc-in [:cache cache-key] callable))
                   callable)
                 (raise "UDF descriptor could not be resolved " udf-desc
                        {:error :udf/not-found
                         :descriptor udf-desc
                         :context context}))))))))))

(defn bind-native-type!
  "Bind a native type name to its receiver deserializer in this runtime only."
  [registry type-name deserialize]
  (let [registry (ensure-registry registry)
        deserialize (ensure-kind deserialize :deserializer)]
    (when-not (qualified-keyword? type-name)
      (raise "Native type name must be a namespaced keyword" {:type-name type-name}))
    (swap! registry
           (fn [state]
             (when-let [existing (get-in state [:native-types type-name])]
               (when-not (= existing deserialize)
                 (raise "Native type already has a different deserializer"
                        {:type-name type-name})))
             (assoc-in state [:native-types type-name] deserialize)))
    registry))

(defn native-value-reader
  "Build a receiver using the live caller registry, including later rebindings."
  [registry]
  (when registry
    (fn [type-name payload]
      (let [type-name (keyword (subs type-name 1))
            descriptor (get-in (registry-state registry) [:native-types type-name])]
        (when-not descriptor
          (raise "Missing receiver native type binding" {:type-name type-name}))
        ((materialize registry {:type-name type-name :kind :deserializer
                                :embedded? true} descriptor false)
         payload)))))

(defn resolve
  [registry udf-desc]
  (materialize registry nil udf-desc))
