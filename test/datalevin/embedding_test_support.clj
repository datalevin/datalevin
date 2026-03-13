;;
;; Copyright (c) Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns datalevin.embedding-test-support
  (:require
   [clojure.java.io :as io]
   [clojure.string :as s]
   [datalevin.embedding :as emb]
   [datalevin.util :as u])
  (:import
   [java.nio.file Files StandardCopyOption]))

(def ^:private default-embed-cache-env
  "DTLV_TEST_EMBED_CACHE_DIR")

(def ^:private skip-real-embed-test-env
  "DTLV_SKIP_REAL_EMBED_TEST")

(defn non-blank-env
  [name]
  (let [value (System/getenv name)]
    (when (and value (not (s/blank? value)))
      value)))

(defn truthy-env?
  [name]
  (contains? #{"1" "true" "yes" "on"}
             (some-> (non-blank-env name) s/lower-case)))

(defn- ensure-dir!
  [path]
  (.mkdirs (io/file path))
  path)

(defn- default-embed-cache-dir
  []
  (or (non-blank-env default-embed-cache-env)
      (when-let [xdg-cache-home (non-blank-env "XDG_CACHE_HOME")]
        (str xdg-cache-home u/+separator+ "datalevin"
             u/+separator+ "embed"))
      (str (System/getProperty "user.home")
           u/+separator+ ".cache"
           u/+separator+ "datalevin"
           u/+separator+ "embed")))

(defn- ensure-cached-default-model!
  []
  (let [embed-dir  (ensure-dir! (default-embed-cache-dir))
        model-path (str embed-dir u/+separator+ emb/default-model-file)]
    (when-not (.exists (io/file model-path))
      (#'datalevin.embedding/ensure-default-model! {:embed-dir embed-dir}))
    model-path))

(defn- link-or-copy-file!
  [source target]
  (let [target-file (io/file target)]
    (.mkdirs (.getParentFile target-file))
    (when-not (.exists target-file)
      (let [source-path (.toPath (io/file source))
            target-path (.toPath target-file)]
        (try
          (Files/createLink target-path source-path)
          (catch Exception _
            (Files/copy source-path target-path
                        (into-array java.nio.file.CopyOption
                                    [StandardCopyOption/REPLACE_EXISTING]))))))
    target))

(defn- install-cached-model-into-db-root!
  [source-model dir]
  (let [embed-dir       (str dir u/+separator+ "embed")
        target-model    (str embed-dir u/+separator+ emb/default-model-file)
        source-manifest (str source-model ".edn")
        target-manifest (str target-model ".edn")]
    (link-or-copy-file! source-model target-model)
    (when (.exists (io/file source-manifest))
      (link-or-copy-file! source-manifest target-manifest))
    target-model))

(defn integration-provider-spec!
  [dir]
  (when-not (truthy-env? skip-real-embed-test-env)
    (if-let [model (non-blank-env "DTLV_TEST_LLAMA_EMBED_MODEL")]
      {:provider :default
       :model    model}
      (do
        (install-cached-model-into-db-root! (ensure-cached-default-model!) dir)
        {:provider :default
         :dir      dir}))))
