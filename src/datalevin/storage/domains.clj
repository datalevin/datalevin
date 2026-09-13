;;
;; Copyright (c) Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns ^:no-doc datalevin.storage.domains
  "Search, vector, embedding, and document index domain initialization."
  (:refer-clojure :exclude [assoc])
  (:require
   [datalevin.constants :as c]
   [datalevin.embedding :as emb]
   [datalevin.idoc :as idoc]
   [datalevin.inline :refer [assoc]]
   [datalevin.search :as s]
   [datalevin.util :as u :refer [raise]]
   [datalevin.vector :as v]))

(declare provider-spec-for-domain)

(defn embedding-attr-domains
  [attr props]
  (vec
    (distinct
      (cond-> (or (seq (props :db.embedding/domains))
                  [c/default-domain])
        (props :db.embedding/autoDomain) (conj (v/attr-domain attr))))))

(def ^:private persisted-embedding-space-keys
  #{:dimensions :embedding-metadata})

(defn- runtime-provider-space
  [dir runtime-providers domain domain-opts]
  (let [provider-spec (provider-spec-for-domain
                        dir
                        runtime-providers
                        domain
                        (apply dissoc domain-opts persisted-embedding-space-keys))]
    (emb/provider-space provider-spec)))

(defn- vector-dim
  [vec-data]
  (cond
    (u/array? vec-data)
    (java.lang.reflect.Array/getLength vec-data)

    (instance? java.util.List vec-data)
    (.size ^java.util.List vec-data)

    (sequential? vec-data)
    (count vec-data)

    :else
    (raise "Embedding provider returned an unsupported vector value"
             {:vector vec-data})))

(defn ensure-embedding-vector!
  [domain expected-dimensions vec-data]
  (let [dimensions (vector-dim vec-data)]
    (when (and expected-dimensions
               (not= (long expected-dimensions) (long dimensions)))
      (raise "Embedding vector dimensions do not match domain configuration"
               {:domain              domain
                :expected-dimensions expected-dimensions
                :actual-dimensions   dimensions}))
    vec-data))

(defn- default-search-domain
  [dms search-opts search-domains]
  (let [new-opts (assoc (or (get search-domains c/default-domain)
                            search-opts
                            {})
                        :domain c/default-domain)]
    (assoc dms c/default-domain (if-let [opts (dms c/default-domain)]
                                  (merge opts new-opts)
                                  new-opts))))

(defn- listed-search-domains
  [dms domains search-domains]
  (reduce (fn [m domain]
            (let [new-opts (assoc (get search-domains domain {})
                                  :domain domain)]
              (assoc m domain (if-let [opts (m domain)]
                                (merge opts new-opts)
                                new-opts))))
          dms domains))

(defn- init-search-domains
  [search-domains0 schema search-opts search-domains]
  (reduce-kv
    (fn [dms attr
        {:keys [db/fulltext db.fulltext/domains db.fulltext/autoDomain]}]
      (if fulltext
        (cond-> (if (seq domains)
                  (listed-search-domains dms domains search-domains)
                  (default-search-domain dms search-opts search-domains))
          autoDomain (#(let [domain (u/keyword->string attr)]
                         (assoc
                           % domain
                           (let [new-opts (assoc (get search-domains domain {})
                                                 :domain domain)]
                             (if-let [opts (% domain)]
                               (merge opts new-opts)
                               new-opts))))))
        dms))
    (or search-domains0 {}) schema))

(defn init-engines
  [lmdb domains runtime-opts]
  (reduce-kv
    (fn [m domain opts]
      (assoc m domain
             (s/new-search-engine
               lmdb
               (cond-> opts
                 (:udf-registry runtime-opts)
                 (assoc :udf-registry (:udf-registry runtime-opts))))))
    {} domains))

(defn- listed-vector-domains
  [dms domains vector-opts vector-domains]
  (reduce (fn [m domain]
            (let [new-opts (assoc (get vector-domains domain vector-opts)
                                  :domain domain)]
              (assoc m domain (if-let [opts (m domain)]
                                (merge opts new-opts)
                                new-opts))))
          dms domains))

(defn- init-vector-domains
  [vector-domains0 schema vector-opts vector-domains]
  (reduce-kv
    (fn [dms attr {:keys [db/valueType db.vec/domains]}]
      (if (identical? valueType :db.type/vec)
        (if (seq domains)
          (listed-vector-domains dms domains vector-opts vector-domains)
          (let [domain (v/attr-domain attr)]
            (assoc dms domain (assoc (get vector-domains domain vector-opts)
                                     :domain domain))))
        dms))
    (or vector-domains0 {}) schema))

(def ^:private default-embedding-opts
  {:provider    :default
   :metric-type :cosine})

(def ^:private embedding-index-prefix
  "__embedding__")

(defn- embedding-index-domain
  [domain]
  (str embedding-index-prefix "/" domain))

(defn- default-embedding-domain
  [dms embedding-opts]
  (if (contains? dms c/default-domain)
    dms
    (assoc dms c/default-domain
           (assoc (merge default-embedding-opts (or embedding-opts {}))
                  :domain c/default-domain))))

(defn- listed-embedding-domains
  [dms domains embedding-opts embedding-domains]
  (reduce
    (fn [m domain]
      (if (contains? m domain)
        m
        (assoc m domain
               (assoc (merge default-embedding-opts
                             (get embedding-domains domain)
                             embedding-opts)
                      :domain domain))))
    dms
    domains))

(defn- init-embedding-domain-refs
  [embedding-domains0 schema embedding-opts embedding-domains]
  (reduce-kv
    (fn [dms attr
         {:keys [db/embedding db.embedding/domains db.embedding/autoDomain]}]
      (if embedding
        (let [dms (if (seq domains)
                    (listed-embedding-domains dms domains embedding-opts
                                              embedding-domains)
                    (default-embedding-domain dms embedding-opts))]
          (if autoDomain
            (listed-embedding-domains dms [(v/attr-domain attr)] embedding-opts
                                      embedding-domains)
            dms))
        dms))
    (or embedding-domains0 {})
    schema))

(defn- provider-spec-for-domain
  [dir runtime-providers domain {:keys [provider] :as domain-opts}]
  (let [provider-id (or provider :default)
        runtime     (get runtime-providers provider-id)]
    (cond
      (satisfies? emb/IEmbeddingProvider runtime)
      runtime

      (or (map? runtime) (keyword? runtime))
      (merge (if (map? runtime) runtime {:provider runtime})
             domain-opts
             {:provider provider-id :dir dir})

      runtime
      (raise "Embedding provider registry entry is invalid"
               {:domain domain
                :provider provider-id
                :entry runtime})

      (#{:default :llama.cpp :openai-compatible} provider-id)
      (assoc domain-opts :provider provider-id :dir dir)

      :else
      (raise "Embedding provider is not configured"
               {:domain domain :provider provider-id}))))

(defn- resolve-embedding-domain
  [dir runtime-providers [domain domain-opts]]
  (let [domain-opts                 (merge default-embedding-opts domain-opts)
        {:keys [dimensions
                embedding-metadata]} (runtime-provider-space dir runtime-providers
                                                             domain domain-opts)
        provider-dimensions         dimensions
        provider-metadata           embedding-metadata
        stored-dimensions           (:dimensions domain-opts)
        stored-metadata             (:embedding-metadata domain-opts)
        dimensions                  (or stored-dimensions provider-dimensions)
        embedding-metadata          (or stored-metadata provider-metadata)]
    (when (and stored-dimensions provider-dimensions
               (not= (long stored-dimensions) (long provider-dimensions)))
      (raise "Embedding domain dimensions do not match the runtime provider"
               {:domain              domain
                :provider            (:provider domain-opts)
                :stored-dimensions   stored-dimensions
                :provider-dimensions provider-dimensions}))
    (when stored-metadata
      (emb/ensure-compatible-metadata stored-metadata provider-metadata))
    (when-not dimensions
      (raise "Embedding domain dimensions could not be resolved"
               {:domain domain :provider (:provider domain-opts)}))
    [domain
     (-> domain-opts
         (assoc :provider (or (:provider domain-opts) :default)
                :dimensions dimensions
                :embedding-metadata embedding-metadata))]))

(defn- init-embedding-domains
  [dir embedding-domains0 schema embedding-opts embedding-domains runtime-providers]
  (let [domains (init-embedding-domain-refs embedding-domains0 schema
                                            embedding-opts embedding-domains)]
    (into {}
          (map #(resolve-embedding-domain dir runtime-providers %))
          domains)))

(defn init-embedding-providers
  [dir domains runtime-providers]
  (reduce-kv
    (fn [m domain domain-opts]
      (assoc m domain
             (emb/init-embedding-provider
               (provider-spec-for-domain dir runtime-providers domain domain-opts))))
    {}
    domains))

(defn init-indices
  [lmdb domains]
  (reduce-kv
    (fn [m domain opts]
      (assoc m domain (v/new-vector-index lmdb opts)))
    {} domains))

(defn init-embedding-indices
  [lmdb domains]
  (reduce-kv
    (fn [m domain opts]
      (assoc m domain
             (v/new-vector-index
               lmdb
               (assoc opts :domain (embedding-index-domain domain)))))
    {}
    domains))

(defn- idoc-schema-domain-opts
  [props]
  (cond-> {}
    (contains? props :db.idoc/indexedPaths)
    (assoc :indexed-paths (:db.idoc/indexedPaths props))

    (contains? props :db.idoc/excludedPaths)
    (assoc :excluded-paths (:db.idoc/excludedPaths props))))

(defn- merge-idoc-path-option
  [a b]
  (cond
    (nil? a) b
    (nil? b) a
    :else (vec (distinct (concat a b)))))

(defn- merge-idoc-domain-opts
  [a b]
  (-> (merge a b)
      (assoc :indexed-paths
             (merge-idoc-path-option (:indexed-paths a)
                                     (:indexed-paths b)))
      (assoc :excluded-paths
             (merge-idoc-path-option (:excluded-paths a)
                                     (:excluded-paths b)))))

(defn init-idoc-domains
  [schema opts]
  (let [default-opts (:idoc-opts opts)
        domain-opts  (:idoc-domains opts)]
    (reduce-kv
      (fn [dms attr {:keys [db/valueType db/domain db/idocFormat] :as props}]
        (if (identical? valueType :db.type/idoc)
          (let [domain      (or domain (u/keyword->string attr))
                fmt         (or idocFormat :edn)
                prior       (get dms domain)
                schema-opts (idoc-schema-domain-opts props)
                opts        (merge default-opts
                                   schema-opts
                                   (get domain-opts domain))
                opts        (assoc opts :domain domain :format fmt)]
            (cond
              (nil? prior) (assoc dms domain opts)
              (= (:format prior) fmt)
              (assoc dms domain (merge-idoc-domain-opts prior opts))
              :else
              (assoc dms domain
                     (merge-idoc-domain-opts prior (assoc opts :format :mixed)))))
          dms))
      {}
      schema)))

(defn init-idoc-indices
  [lmdb domains]
  (reduce-kv
    (fn [m domain opts]
      (assoc m domain (idoc/new-idoc-index lmdb opts)))
    {} domains))

(defn init-store-domains
  [dir schema opts3 search-opts search-domains
   vector-opts vector-domains embedding-opts embedding-domains
   embedding-providers]
  (let [s-domains (init-search-domains (:search-domains opts3)
                                       schema search-opts search-domains)
        v-domains (init-vector-domains (:vector-domains opts3)
                                       schema vector-opts vector-domains)
        e-domains (init-embedding-domains dir
                                          (:embedding-domains opts3)
                                          schema
                                          embedding-opts
                                          embedding-domains
                                          embedding-providers)
        i-domains (init-idoc-domains schema opts3)]
    {:s-domains s-domains
     :v-domains v-domains
     :e-domains e-domains
     :i-domains i-domains
     :opts4     (cond-> opts3
                  (seq e-domains)
                  (assoc :embedding-opts
                         (merge default-embedding-opts
                                (or (:embedding-opts opts3) embedding-opts))
                         :embedding-domains e-domains))}))

(defn transfer-engines
  [engines lmdb]
  (if (empty? engines)
    engines
    (zipmap (keys engines) (map #(s/transfer % lmdb) (vals engines)))))

(defn transfer-indices
  [indices lmdb]
  (if (empty? indices)
    indices
    (zipmap (keys indices) (map #(v/transfer % lmdb) (vals indices)))))

(defn transfer-idoc-indices
  [indices lmdb]
  (if (empty? indices)
    indices
    (zipmap (keys indices) (map #(idoc/transfer % lmdb) (vals indices)))))
