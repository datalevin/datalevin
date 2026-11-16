;; Copyright (c) Huahai Yang. All rights reserved.
;; Distributed under the Eclipse Public License 2.0.
(ns ^:no-doc datalevin.db.tx.wire
  "Portable prepared definitions. Only source data and parameter paths cross
  the wire; compiled metadata and execution operands stay on their owner."
  (:require [datalevin.db.tx.compile :as compile]
            [datalevin.client-op :as cop]
            [datalevin.db.tx.template :as template]
            [datalevin.prepare :as prepare]
            [datalevin.prepared :as prepared])
  (:import [datalevin.db.tx.plan TxParam WriteOp]))

(defrecord RemoteOperation [id source compiled definition])

(def ^:private client-context (prepare/->PrepareCtx {} {} {}))

(defn source [operation]
  (if (or (instance? RemoteOperation operation) (instance? WriteOp operation))
    (:source operation) operation))

(defn client-template [operations options]
  ;; Reuse ordinary definition/parameter validation without remote schema RPCs.
  (when-not (vector? operations)
    (throw (ex-info "A prepared transaction requires an operation vector" {:error :prepared/program-shape})))
  (when (seq (dissoc options :batch?))
    (throw (ex-info "Unknown prepared transaction options" {:error :prepared/options})))
  (template/prepare-template client-context
    (mapv #(if (instance? RemoteOperation %) (:compiled %) (source %)) operations) options))

(defn- container? [value]
  (contains? #{clojure.lang.PersistentArrayMap clojure.lang.PersistentHashMap
               clojure.lang.PersistentVector clojure.lang.APersistentVector$SubVector} (class value)))

(defn- encode-form [form]
  (let [parameters (volatile! [])]
    (letfn [(walk [value path]
              (cond
                (instance? TxParam value)
                (do (vswap! parameters conj [path (.-key ^TxParam value)]) nil)
                (container? value)
                (with-meta
                  (if (map? value)
                    (reduce-kv (fn [out k v] (assoc out k (walk v (conj path k)))) (empty value) value)
                    (mapv (fn [i] (walk (nth value i) (conj path i))) (range (count value))))
                  (meta value))
                :else value))]
      (let [data (walk form [])] {:form data :parameters @parameters}))))

(defn operation [form]
  (if (instance? RemoteOperation form)
    form
    (let [form (source form)
          compiled (template/prepare-operation client-context form)
          id (.-id ^datalevin.prepared.Request (prepared/request nil))]
      ;; Caller-owned operations retain their substitution code and portable
      ;; definition together. Composing another program needs neither walk.
      (->RemoteOperation id form compiled (assoc (encode-form form) :id id)))))

(defn- definition* [operations options]
  {:version 1 :options options
   :operations (mapv (fn [operation]
                       (if (instance? RemoteOperation operation)
                         (:definition operation)
                         (encode-form (source operation)))) operations)})

(defn definition [operations options]
  (client-template operations options)
  (definition* operations options))

(defn request [db-name operations options]
  (let [client (client-template operations options)
        definition (definition* operations options)
        request (prepared/request [db-name definition nil])]
    (datalevin.prepared.Request. (.-id ^datalevin.prepared.Request request)
                                (.-args ^datalevin.prepared.Request request)
                                (assoc (.-metadata ^datalevin.prepared.Request request) ::template client
                                       ::definition-hash (delay (cop/request-hash [db-name definition]))))))

(defn input-rows [^datalevin.prepared.Request request input]
  (template/input-rows (::template (.-metadata request)) input))

(defn materialize [^datalevin.prepared.Request request input]
  (let [template (::template (.-metadata request))]
    (template/materialize template (template/input-rows template input))))

(defn restore-operation
  "Restore markers through literal containers only, before normal compilation."
  [{:keys [form parameters]}]
  (reduce (fn [result [path key]]
            (when-not (and (vector? path) (seq path) (keyword? key))
              (throw (ex-info "Invalid prepared parameter path" {:error :prepared/definition})))
            (loop [value result keys (seq path)]
              (when-let [k (first keys)]
                (when-not (and (container? value)
                               (if (map? value)
                                 (and (compile/prepared-literal? k) (contains? value k))
                                 (and (integer? k) (<= 0 k) (< k (count value)))))
                  (throw (ex-info "Invalid prepared parameter path" {:error :prepared/definition})))
                (recur (get value k) (next keys))))
            (assoc-in result path (template/parameter key))) form parameters))
