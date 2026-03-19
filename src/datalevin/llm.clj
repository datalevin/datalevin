;;
;; Copyright (c) Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns datalevin.llm
  "Local LLM generation providers"
  (:require
   [clojure.string :as s]
   [datalevin.util :refer [raise]])
  (:import
   [datalevin LlamaGenerator]
   [java.io File]
   [java.lang AutoCloseable]))

(defprotocol ILLMProvider
  (generate-text* [this prompt max-tokens opts]
    "Generate text for a single prompt.")
  (summarize-text* [this text max-tokens opts]
    "Summarize a single text input.")
  (llm-metadata [this]
    "Return stable metadata describing the runtime provider.")
  (llm-context-size [this]
    "Return the model context size for this provider.")
  (close-llm-provider [this]
    "Release provider-owned resources. Must be idempotent."))

(defprotocol ILLMTokenCounter
  (llm-token-count* [this text opts]
    "Return the token count for a single input string."))

(def ^:private built-in-provider-ids
  #{:llama.cpp})

(defn- non-blank-string?
  [x]
  (and (string? x) (not (s/blank? x))))

(defn- ensure-text
  [text field]
  (when-not (string? text)
    (raise (str (name field) " must be a string")
           {field text}))
  text)

(defn- ensure-provider
  [provider]
  (when-not (satisfies? ILLMProvider provider)
    (raise "Expected an llm provider"
           {:input provider})))

(defn- ensure-token-counter
  [provider]
  (when-not (satisfies? ILLMTokenCounter provider)
    (raise "LLM provider does not support token counting"
           {:provider provider})))

(defn- ensure-max-tokens
  [max-tokens]
  (when-not (integer? max-tokens)
    (raise "max-tokens must be an integer"
           {:max-tokens max-tokens}))
  (when (neg? (long max-tokens))
    (raise "max-tokens must be non-negative"
           {:max-tokens max-tokens}))
  (long max-tokens))

(defn- compact-map
  [m]
  (reduce-kv
    (fn [acc k v]
      (cond
        (nil? v) acc
        (and (map? v) (empty? v)) acc
        :else (assoc acc k v)))
    {}
    (or m {})))

(defn- ensure-provider-spec
  [provider-spec]
  (cond
    (keyword? provider-spec)
    {:provider provider-spec}

    (map? provider-spec)
    provider-spec

    :else
    provider-spec))

(defn- validated-provider-spec
  [provider-spec opts]
  (let [spec     (merge provider-spec opts)
        provider (or (:provider spec) :llama.cpp)
        model    (or (:model spec) (:model-path spec))
        spec     (assoc spec :provider provider)]
    (when-not (built-in-provider-ids provider)
      (raise "Unknown llm provider"
             {:provider provider
              :known-providers built-in-provider-ids}))
    (when-not (non-blank-string? model)
      (raise "LLM provider requires :model or :model-path"
             {:provider-spec spec}))
    spec))

(defn- llm-provider-metadata
  [spec model-path context-size]
  (compact-map
    {:llm/provider
     {:kind :local
      :id   :llama.cpp}
     :llm/runtime
     (compact-map
       {:context-size context-size
        :gpu-layers   (:gpu-layers spec)
        :ctx-size     (:ctx-size spec)
        :threads      (:threads spec)})
     :llm/artifact
     {:format :gguf
      :file   (.getName (File. model-path))
      :path   model-path}}))

(deftype LlamaCppProvider [^LlamaGenerator generator provider-spec metadata]
  ILLMProvider
  (generate-text* [_ prompt max-tokens _opts]
    (.generate generator ^String prompt (int max-tokens)))
  (summarize-text* [_ text max-tokens _opts]
    (.summarize generator ^String text (int max-tokens)))
  (llm-metadata [_]
    metadata)
  (llm-context-size [_]
    (.contextSize generator))
  (close-llm-provider [_]
    (.close generator))

  ILLMTokenCounter
  (llm-token-count* [_ text _opts]
    (.tokenCount generator ^String text))

  AutoCloseable
  (close [_]
    (.close generator)))

(defn- create-llama-provider
  [spec]
  (let [model      (or (:model spec) (:model-path spec))
        gpu-layers (int (or (:gpu-layers spec) 0))
        ctx-size   (int (or (:ctx-size spec) 0))
        threads    (int (or (:threads spec) 0))
        generator  (LlamaGenerator. model gpu-layers ctx-size threads)]
    (LlamaCppProvider.
      generator
      (assoc spec :model-path model)
      (llm-provider-metadata spec model (.contextSize generator)))))

(def ^:dynamic *llama-provider-factory*
  create-llama-provider)

(defn- lazy-provider
  [provider-spec init-fn]
  (let [provider* (atom nil)
        closed?   (atom false)
        ensure!   (fn []
                    (when @closed?
                      (raise "LLM provider is closed"
                             {:provider-spec provider-spec}))
                    (or @provider*
                        (locking provider*
                          (or @provider*
                              (let [provider (init-fn provider-spec)]
                                (reset! provider* provider)
                                provider)))))]
    (reify
      ILLMProvider
      (generate-text* [_ prompt max-tokens opts]
        (generate-text* (ensure!) prompt max-tokens opts))
      (summarize-text* [_ text max-tokens opts]
        (summarize-text* (ensure!) text max-tokens opts))
      (llm-metadata [_]
        (llm-metadata (ensure!)))
      (llm-context-size [_]
        (llm-context-size (ensure!)))
      (close-llm-provider [_]
        (when (compare-and-set! closed? false true)
          (when-let [provider @provider*]
            (close-llm-provider provider))))

      ILLMTokenCounter
      (llm-token-count* [_ text opts]
        (llm-token-count* (ensure!) text opts))

      AutoCloseable
      (close [this]
        (close-llm-provider this)))))

(defn init-llm-provider
  "Initialize an LLM provider.

  `provider-spec` may be:

  * an existing provider instance implementing `ILLMProvider`
  * `:llama.cpp`
  * a map such as

    `{:provider :llama.cpp
      :model    \"/path/to/model.gguf\"}`

  The built-in llama.cpp provider requires `:model` or `:model-path`; Datalevin
  does not download or bundle a default generation model. Optional tuning keys
  are `:gpu-layers`, `:ctx-size`, and `:threads`. The returned provider is
  `AutoCloseable`; use [[close-llm-provider]] when finished."
  ([provider-spec]
   (init-llm-provider provider-spec nil))
  ([provider-spec opts]
   (let [provider-spec (ensure-provider-spec provider-spec)]
     (if (satisfies? ILLMProvider provider-spec)
       provider-spec
       (lazy-provider (validated-provider-spec provider-spec opts)
                      *llama-provider-factory*)))))

(defn generate-text
  "Generate completion text for a single prompt."
  ([provider prompt max-tokens]
   (generate-text provider prompt max-tokens nil))
  ([provider prompt max-tokens opts]
   (ensure-provider provider)
   (generate-text* provider
                   (ensure-text prompt :prompt)
                   (ensure-max-tokens max-tokens)
                   opts)))

(defn summarize-text
  "Summarize a single input string."
  ([provider text max-tokens]
   (summarize-text provider text max-tokens nil))
  ([provider text max-tokens opts]
   (ensure-provider provider)
   (summarize-text* provider
                    (ensure-text text :text)
                    (ensure-max-tokens max-tokens)
                    opts)))

(defn llm-token-count
  "Return the token count for a single LLM input string."
  ([provider text]
   (llm-token-count provider text nil))
  ([provider text opts]
   (ensure-token-counter provider)
   (llm-token-count* provider (ensure-text text :text) opts)))
