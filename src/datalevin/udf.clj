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
  (when-not (keyword? (:udf/lang x))
    (raise "UDF descriptor :udf/lang must be a keyword"
           {:error :udf/descriptor :value x}))
  (when-not (keyword? (:udf/kind x))
    (raise "UDF descriptor :udf/kind must be a keyword"
           {:error :udf/descriptor :value x}))
  (when-not (keyword? (:udf/id x))
    (raise "UDF descriptor :udf/id must be a keyword"
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
  [registry context udf-desc]
  (let [udf-desc (descriptor udf-desc)]
    (if-not registry
      (raise "No UDF registry is configured for descriptor " udf-desc
             {:error :udf/not-found :descriptor udf-desc :context context})
      (let [registry  (ensure-registry registry)
            cache-key udf-desc
            bind-key  (binding-key udf-desc)]
        (if-some [callable (or (get-in @registry [:cache cache-key])
                               (get-in @registry [:bindings bind-key]))]
          (do
            (swap! registry assoc-in [:cache cache-key] callable)
            callable)
          (let [resolver (get-in @registry [:resolvers (:udf/lang udf-desc)])]
            (if-not resolver
              (raise "No UDF resolver found for descriptor " udf-desc
                     {:error :udf/not-found
                      :descriptor udf-desc
                      :context context})
              (if-some [callable (resolver context udf-desc)]
                (do
                  (swap! registry assoc-in [:cache cache-key] callable)
                  callable)
                (raise "UDF descriptor could not be resolved " udf-desc
                       {:error :udf/not-found
                        :descriptor udf-desc
                        :context context})))))))))

(defn resolve
  [registry udf-desc]
  (materialize registry nil udf-desc))
