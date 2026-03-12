;;
;; Copyright (c) Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns datalevin.embedding
  "Text embedding providers"
  (:require
   [clojure.edn :as edn]
   [clojure.java.io :as io]
   [clojure.string :as s]
   [datalevin.util :as u :refer [raise]])
  (:import
   [datalevin LlamaEmbedder]
   [java.io InputStream]
   [java.lang AutoCloseable]
   [java.net URI]
   [java.net.http HttpClient HttpClient$Redirect HttpRequest HttpResponse
    HttpResponse$BodyHandlers]
   [java.nio.file Files Path Paths StandardCopyOption]
   [java.nio.file.attribute FileAttribute]
   [java.security MessageDigest]))

(defprotocol IEmbeddingProvider
  (embedding [this items opts]
    "Return one embedding vector per input item, in order.")
  (embedding-metadata [this]
    "Return stable metadata describing the embedding space for this provider.")
  (embedding-dimensions [this]
    "Return embedding dimensions for this provider.")
  (close-provider [this]
    "Release provider-owned resources. Must be idempotent."))

(def ^:private built-in-provider-ids
  #{:default :llama.cpp})

(def ^:const default-model-file
  "multilingual-e5-small-Q8_0.gguf")

(def ^:const default-model-dimensions
  384)

(def ^:const default-model-repo
  "keisuke-miyako/multilingual-e5-small-gguf-q8_0")

(def ^:const default-model-id
  "intfloat/multilingual-e5-small")

(def ^:const default-model-url
  (str "https://huggingface.co/"
       default-model-repo
       "/resolve/main/"
       default-model-file
       "?download=true"))

(def ^:const default-model-manifest
  {:embedding/provider
   {:kind     :local
    :id       :default
    :model-id default-model-id}
   :embedding/output
   {:dimensions      default-model-dimensions
    :pooling         :mean
    :normalize?      true
    :query-prefix    "query: "
    :document-prefix "passage: "}
   :embedding/artifact
   {:format       :gguf
    :file         default-model-file
    :quantization :q8_0}})

(def ^:private default-model-lock
  (Object.))

(declare create-llama-provider init-embedding-provider)

(defn- non-blank-string?
  [x]
  (and (string? x) (not (s/blank? x))))

(defn- ensure-item-text
  [item]
  (cond
    (string? item)
    item

    (map? item)
    (let [text (:text item)]
      (when-not (string? text)
        (raise "Embedding item map requires string :text"
               {:item item :value text}))
      text)

    :else
    (raise "Embedding items must be strings or maps with string :text"
           {:item item})))

(defn- shaped-item-text
  [metadata item]
  (let [text   (ensure-item-text item)
        output (:embedding/output metadata)
        prefix (when (map? item)
                 (case (or (:kind item) (:usage item))
                   :query    (:query-prefix output)
                   :document (:document-prefix output)
                   nil))]
    (if (and (string? prefix) (not (s/blank? prefix)))
      (str prefix text)
      text)))

(defn- ensure-provider
  [provider]
  (when-not (satisfies? IEmbeddingProvider provider)
    (raise "Expected an embedding provider"
           {:input provider})))

(defn- remove-nil-vals
  [m]
  (reduce-kv
    (fn [acc k v]
      (if (nil? v) acc (assoc acc k v)))
    {}
    (or m {})))

(defn- compact-metadata
  [metadata]
  (reduce-kv
    (fn [acc k v]
      (let [v (if (map? v) (not-empty (remove-nil-vals v)) v)]
        (if (nil? v) acc (assoc acc k v))))
    {}
    (or metadata {})))

(defn- merge-metadata
  [base override]
  (reduce-kv
    (fn [acc k v]
      (assoc acc k
             (if (and (map? (get acc k))
                      (map? v))
               (merge (get acc k) v)
               v)))
    (or base {})
    (or override {})))

(defn- validate-metadata-shape
  [metadata]
  (when-not (map? metadata)
    (raise "Embedding metadata must be a map"
           {:metadata metadata}))
  (doseq [section [:embedding/provider
                   :embedding/output
                   :embedding/artifact]]
    (when-let [value (get metadata section)]
      (when-not (map? value)
        (raise "Embedding metadata section must be a map"
               {:section section :value value}))))
  metadata)

(def ^:private metadata-missing
  ::missing)

(defn- compatibility-metadata
  [metadata]
  (let [metadata (if (nil? metadata)
                   {}
                   (-> metadata
                       validate-metadata-shape
                       compact-metadata))]
    metadata))

(defn- metadata-mismatch
  [stored runtime path]
  (cond
    (map? stored)
    (cond
      (not (map? runtime))
      {:path path
       :stored stored
       :runtime runtime
       :reason :shape}

      :else
      (some (fn [[k stored-v]]
              (let [next-path (conj path k)]
                (if (contains? runtime k)
                  (metadata-mismatch stored-v (get runtime k) next-path)
                  {:path next-path
                   :stored stored-v
                   :runtime metadata-missing
                   :reason :missing})))
            stored))

    (= stored runtime)
    nil

    :else
    {:path path
     :stored stored
     :runtime runtime
     :reason :value}))

(defn ensure-compatible-metadata
  "Ensure stored embedding metadata remains compatible with the runtime provider.

  Metadata is part of the embedding-space contract, so missing or changed
  fields must be treated as incompatibilities."
  [stored runtime]
  (let [stored*  (compatibility-metadata stored)
        runtime* (compatibility-metadata runtime)]
    (when-let [mismatch (metadata-mismatch stored* runtime* [])]
      (raise "Embedding metadata does not match the runtime provider"
             {:stored-metadata  stored
              :runtime-metadata runtime
              :mismatch         mismatch}))
    stored))

(defn- ensure-provider-spec
  [provider-spec]
  (cond
    (satisfies? IEmbeddingProvider provider-spec)
    provider-spec

    (keyword? provider-spec)
    {:provider provider-spec}

    (map? provider-spec)
    provider-spec

    :else
    (raise "Embedding provider spec must be a provider instance, keyword, or map"
           {:provider-spec provider-spec})))

(defn- model-manifest-path
  [model-path]
  (str model-path ".edn"))

(defn- read-model-manifest
  [model-path]
  (let [manifest-path (model-manifest-path model-path)
        manifest-file (io/file manifest-path)]
    (when (.exists manifest-file)
      (-> (slurp manifest-file)
          edn/read-string
          validate-metadata-shape))))

(defn- file-name
  [path]
  (.getName (io/file path)))

(defn- file-stem
  [name]
  (if-let [idx (s/last-index-of name ".")]
    (subs name 0 idx)
    name))

(defn- infer-artifact-format
  [name]
  (when-let [idx (s/last-index-of name ".")]
    (keyword (s/lower-case (subs name (inc idx))))))

(defn- infer-quantization
  [name]
  (when-let [[_ quantization]
             (re-find #"(?i)-([A-Za-z0-9_]+)\.[^.]+$" name)]
    (keyword (s/lower-case quantization))))

(defn- file-sha256
  [path]
  (with-open [in (io/input-stream path)]
    (let [^MessageDigest md (MessageDigest/getInstance "SHA-256")
          buf               (byte-array 8192)]
      (loop []
        (let [n (.read in buf)]
          (when (pos? n)
            (.update md buf 0 n)
            (recur))))
      (u/hexify (.digest md)))))

(defn- spec-output-metadata
  [spec dimensions]
  (cond-> {:dimensions dimensions}
    (contains? spec :pooling)         (assoc :pooling (:pooling spec))
    (contains? spec :normalize?)      (assoc :normalize? (:normalize? spec))
    (contains? spec :query-prefix)    (assoc :query-prefix (:query-prefix spec))
    (contains? spec :document-prefix) (assoc :document-prefix (:document-prefix spec))
    (contains? spec :max-tokens)      (assoc :max-tokens (:max-tokens spec))))

(defn- provider-model-id
  [spec model-path]
  (or (:model-id spec)
      (when (= (file-name model-path) default-model-file)
        default-model-id)
      (file-stem (file-name model-path))))

(defn- base-llama-metadata
  [spec model-path dimensions]
  (let [model-file (file-name model-path)
        artifact   (io/file model-path)]
    {:embedding/provider
     {:kind     :local
      :id       (or (:provider spec) :default)
      :model-id (provider-model-id spec model-path)
      :revision (:revision spec)}
     :embedding/output
     (spec-output-metadata spec dimensions)
     :embedding/artifact
     {:format       (infer-artifact-format model-file)
      :file         model-file
      :sha256       (file-sha256 artifact)
      :bytes        (.length artifact)
      :quantization (infer-quantization model-file)}}))

(defn- validate-metadata-dimensions
  [metadata dimensions]
  (when-let [value (get-in metadata [:embedding/output :dimensions])]
    (when-not (integer? value)
      (raise "Embedding metadata dimensions must be an integer"
             {:dimensions value :metadata metadata}))
    (when-not (= (long value) (long dimensions))
      (raise "Embedding metadata dimensions do not match provider output"
             {:metadata-dimensions value
              :provider-dimensions dimensions
              :metadata metadata})))
  metadata)

(defn- llama-provider-metadata
  [spec model-path dimensions]
  (let [base     (merge-metadata
                   (when (= (file-name model-path) default-model-file)
                     default-model-manifest)
                   (base-llama-metadata spec model-path dimensions))
        manifest (read-model-manifest model-path)
        override (some-> (:embedding-metadata spec)
                         validate-metadata-shape)
        metadata (-> base
                     (merge-metadata manifest)
                     (merge-metadata override)
                     compact-metadata
                     validate-metadata-shape)]
    (validate-metadata-dimensions metadata dimensions)))

(deftype LlamaCppProvider [^LlamaEmbedder embedder provider-spec metadata]
  IEmbeddingProvider
  (embedding [_ items _opts]
    (mapv #(.embed embedder ^String (shaped-item-text metadata %)) items))
  (embedding-metadata [_]
    metadata)
  (embedding-dimensions [_]
    (.dimensions embedder))
  (close-provider [_]
    (.close embedder))

  AutoCloseable
  (close [_]
    (.close embedder)))

(defn- default-embed-dir
  [spec]
  (or (:embed-dir spec)
      (some-> (:dir spec) (str u/+separator+ "embed"))
      (raise "Default embedding model requires :dir pointing to the DB root"
             {:provider-spec spec})))

(defn- default-model-path
  [spec]
  (str (default-embed-dir spec) u/+separator+ default-model-file))

(defn- create-http-client
  []
  (-> (HttpClient/newBuilder)
      (.followRedirects HttpClient$Redirect/NORMAL)
      (.build)))

(defn- move-file!
  [^Path source ^Path target]
  (try
    (Files/move source target
                (into-array java.nio.file.CopyOption
                            [StandardCopyOption/ATOMIC_MOVE
                             StandardCopyOption/REPLACE_EXISTING]))
    (catch Exception _
      (Files/move source target
                  (into-array java.nio.file.CopyOption
                              [StandardCopyOption/REPLACE_EXISTING])))))

(defn- download-file!
  [url target-path]
  (let [^Path target (Paths/get target-path (make-array String 0))
        ^Path parent (.getParent target)
        tmp-dir (or parent (Paths/get "." (make-array String 0)))
        _      (when parent
                 (Files/createDirectories parent (make-array FileAttribute 0)))
        prefix (str (.getFileName target) ".part-")
        suffix ".tmp"
        tmp    (Files/createTempFile tmp-dir prefix suffix
                                     (make-array FileAttribute 0))
        client (create-http-client)
        req    (-> (HttpRequest/newBuilder (URI/create url))
                   (.header "User-Agent" "Datalevin")
                   (.header "Accept" "application/octet-stream")
                   (.GET)
                   (.build))]
    (try
      (let [^HttpResponse resp (.send client req (HttpResponse$BodyHandlers/ofInputStream))
            status             (.statusCode resp)]
        (when-not (= 200 status)
          (raise "Failed to download embedding model"
                 {:url url :status status :target target-path}))
        (with-open [^InputStream in (.body resp)]
          (Files/copy in tmp
                      (into-array java.nio.file.CopyOption
                                  [StandardCopyOption/REPLACE_EXISTING])))
        (move-file! tmp target)
        target-path)
      (catch clojure.lang.ExceptionInfo e
        (throw e))
      (catch Exception e
        (raise "Unable to download default embedding model"
               {:url url :target target-path :cause (.getMessage e)}))
      (finally
        (when (Files/exists tmp (make-array java.nio.file.LinkOption 0))
          (try
            (Files/deleteIfExists tmp)
            (catch Exception _)))))))

(def ^:dynamic *download-default-model!*
  download-file!)

(defn- ensure-default-model!
  [spec]
  (let [path (default-model-path spec)]
    (locking default-model-lock
      (when-not (.exists (io/file path))
        (*download-default-model!* default-model-url path))
      path)))

(defn- create-llama-provider
  [spec]
  (let [model      (or (:model spec)
                       (:model-path spec)
                       (ensure-default-model! spec))
        gpu-layers (int (or (:gpu-layers spec) 0))
        ctx-size   (int (or (:ctx-size spec) 0))
        batch-size (int (or (:batch-size spec) 0))
        threads    (int (or (:threads spec) 0))
        embedder   (LlamaEmbedder. model gpu-layers ctx-size batch-size threads)
        metadata   (llama-provider-metadata spec model (.dimensions embedder))]
    (LlamaCppProvider.
      embedder
      (assoc spec :model-path model)
      metadata)))

(def ^:dynamic *llama-provider-factory*
  create-llama-provider)

(defn- explicit-provider-space
  [spec]
  (let [dimensions (or (:dimensions spec)
                       (get-in spec [:embedding/output :dimensions])
                       (get-in spec [:embedding-metadata
                                     :embedding/output
                                     :dimensions]))
        metadata   (some-> (:embedding-metadata spec)
                           validate-metadata-shape)]
    (when (and dimensions (not (integer? dimensions)))
      (raise "Embedding dimensions must be an integer"
             {:dimensions dimensions
              :provider-spec spec}))
    (when (and metadata dimensions)
      (validate-metadata-dimensions metadata dimensions))
    (when (or dimensions metadata)
      {:dimensions         dimensions
       :embedding-metadata metadata})))

(defn provider-space
  "Resolve stable dimensions and metadata for an embedding provider spec.

  This prefers persisted config when available and only initializes a provider
  when the vector space cannot be determined from the spec alone."
  ([provider-spec]
   (provider-space provider-spec nil))
  ([provider-spec opts]
   (let [provider-spec (ensure-provider-spec provider-spec)]
     (cond
       (satisfies? IEmbeddingProvider provider-spec)
       {:dimensions         (embedding-dimensions provider-spec)
        :embedding-metadata (embedding-metadata provider-spec)}

       :else
       (let [spec      (merge provider-spec opts)
             provider  (or (:provider spec) :default)
             explicit  (explicit-provider-space spec)
             dims      (:dimensions explicit)
             metadata  (:embedding-metadata explicit)
             built-in? (built-in-provider-ids provider)
             default?  (and built-in?
                           (nil? (:model spec))
                           (nil? (:model-path spec)))]
         (cond
           (and default? (or dims metadata))
           (let [dimensions (or dims default-model-dimensions)
                 metadata   (-> default-model-manifest
                                (merge-metadata metadata)
                                compact-metadata
                                validate-metadata-shape)]
             {:dimensions         dimensions
              :embedding-metadata
              (validate-metadata-dimensions metadata dimensions)})

           default?
           {:dimensions         default-model-dimensions
            :embedding-metadata default-model-manifest}

           (and dims metadata)
           {:dimensions         dims
            :embedding-metadata metadata}

           :else
           (with-open [provider (init-embedding-provider spec)]
             {:dimensions         (embedding-dimensions provider)
              :embedding-metadata (embedding-metadata provider)})))))))

(defn- lazy-provider
  [provider-spec init-fn]
  (let [provider* (atom nil)
        closed?   (atom false)
        ensure!   (fn []
                    (when @closed?
                      (raise "Embedding provider is closed"
                             {:provider-spec provider-spec}))
                    (or @provider*
                        (locking provider*
                          (or @provider*
                              (let [provider (init-fn provider-spec)]
                                (reset! provider* provider)
                                provider)))))]
    (reify
      IEmbeddingProvider
      (embedding [_ items opts]
        (embedding (ensure!) items opts))
      (embedding-metadata [_]
        (embedding-metadata (ensure!)))
      (embedding-dimensions [_]
        (embedding-dimensions (ensure!)))
      (close-provider [_]
        (when (compare-and-set! closed? false true)
          (when-let [provider @provider*]
            (close-provider provider))))

      AutoCloseable
      (close [this]
        (close-provider this)))))

(defn init-embedding-provider
  "Initialize an embedding provider.

  `provider-spec` may be:

  * an existing provider instance implementing `IEmbeddingProvider`
  * `:default` or `:llama.cpp`
  * a map such as:

    `{:provider :default
      :model    \"/path/to/model.gguf\"}`

  For the built-in llama.cpp provider, `:model` or `:model-path` is optional.
  When omitted, Datalevin uses the default model
  `multilingual-e5-small-Q8_0.gguf` from `dir/embed/`, where `:dir` is the DB
  root. If the file is missing, Datalevin downloads it from Hugging Face on
  first use. Providers expose stable embedding-space metadata via
  `embedding-metadata`; built-in local providers derive it from the model
  artifact, an optional adjacent `model.gguf.edn` manifest, and an optional
  `:embedding-metadata` override in `provider-spec`. Optional tuning keys are
  `:gpu-layers`, `:ctx-size`, `:batch-size`, and `:threads`. When omitted,
  they default to `0` and defer to native defaults."
  ([provider-spec]
   (init-embedding-provider provider-spec nil))
  ([provider-spec opts]
   (let [provider-spec (ensure-provider-spec provider-spec)]
     (if (satisfies? IEmbeddingProvider provider-spec)
       provider-spec
       (let [spec      (merge provider-spec opts)
             provider  (or (:provider spec) :default)]
         (when-not (built-in-provider-ids provider)
           (raise "Unknown embedding provider"
                  {:provider provider
                   :known-providers built-in-provider-ids}))
         (lazy-provider spec *llama-provider-factory*))))))

(defn embed-text
  "Embed a single text string and return a float array."
  ([provider text]
   (embed-text provider text nil))
  ([provider text opts]
   (ensure-provider provider)
   (first (embedding provider [text] opts))))

(defn embed-texts
  "Embed a batch of text strings and return one float array per input."
  ([provider texts]
   (embed-texts provider texts nil))
  ([provider texts opts]
   (ensure-provider provider)
   (let [texts (cond
                 (nil? texts) []
                 (string? texts) [texts]
                 (or (sequential? texts)
                     (instance? java.util.List texts)) texts
                 :else (raise "Texts must be a string or a sequential collection"
                              {:texts texts}))]
     (embedding provider texts opts))))
