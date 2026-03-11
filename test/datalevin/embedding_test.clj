;;
;; Copyright (c) Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns datalevin.embedding-test
  (:require
   [clojure.java.io :as io]
   [clojure.string :as s]
   [clojure.test :refer [deftest is testing]]
   [datalevin.embedding :as sut]
   [datalevin.util :as u])
  (:import
   [java.nio.file Files StandardCopyOption]))

(def ^:private default-embed-cache-env
  "DTLV_TEST_EMBED_CACHE_DIR")

(def ^:private skip-real-embed-test-env
  "DTLV_SKIP_REAL_EMBED_TEST")

;; Integration test usage:
;; * DTLV_TEST_EMBED_CACHE_DIR overrides the cache location
;; * DTLV_SKIP_REAL_EMBED_TEST=1 skips the real default-model integration test
;; * DTLV_TEST_LLAMA_EMBED_MODEL=/path/model.gguf uses an explicit model path

(defn- non-blank-env
  [name]
  (let [value (System/getenv name)]
    (when (and value (not (s/blank? value)))
      value)))

(defn- truthy-env?
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
        model-path (str embed-dir u/+separator+ sut/default-model-file)]
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
  (let [embed-dir        (str dir u/+separator+ "embed")
        target-model     (str embed-dir u/+separator+ sut/default-model-file)
        source-manifest  (str source-model ".edn")
        target-manifest  (str target-model ".edn")]
    (link-or-copy-file! source-model target-model)
    (when (.exists (io/file source-manifest))
      (link-or-copy-file! source-manifest target-manifest))
    target-model))

(defn- integration-provider-spec!
  [dir]
  (when-not (truthy-env? skip-real-embed-test-env)
    (if-let [model (non-blank-env "DTLV_TEST_LLAMA_EMBED_MODEL")]
      {:provider :default
       :model    model}
      (do
        (install-cached-model-into-db-root! (ensure-cached-default-model!) dir)
        {:provider :default
         :dir      dir}))))

(deftest helper-dispatch-test
  (let [closed?  (atom false)
        metadata {:embedding/provider {:id :test}
                  :embedding/output   {:dimensions 1}}
        provider (reify
                   sut/IEmbeddingProvider
                   (embedding [_ items _opts]
                     (mapv (fn [item]
                             [(count (if (map? item) (:text item) item))])
                           items))
                   (embedding-metadata [_]
                     metadata)
                   (embedding-dimensions [_]
                     1)
                   (close-provider [_]
                     (reset! closed? true))

                   java.lang.AutoCloseable
                   (close [_]
                     (reset! closed? true)))]
    (is (= [[3] [5]]
           (sut/embed-texts provider ["cat" "horse"])))
    (is (= [4]
           (sut/embed-text provider "bear")))
    (is (= metadata
           (sut/embedding-metadata provider)))
    (is (= [[3] [4]]
           (sut/embedding provider [{:text "cat"} {:text "bear"}] nil)))
    (is (= 1
           (sut/embedding-dimensions provider)))
    (sut/close-provider provider)
    (is (true? @closed?))))

(deftest default-provider-is-lazy-test
  (let [created  (atom 0)
        closed?  (atom false)
        metadata {:embedding/provider {:id :default}
                  :embedding/output   {:dimensions 1}}
        delegate (reify
                   sut/IEmbeddingProvider
                   (embedding [_ items _opts]
                     (mapv (fn [item]
                             [(count (if (map? item) (:text item) item))])
                           items))
                   (embedding-metadata [_]
                     metadata)
                   (embedding-dimensions [_]
                     1)
                   (close-provider [_]
                     (reset! closed? true))

                   java.lang.AutoCloseable
                   (close [_]
                     (reset! closed? true)))]
    (binding [sut/*llama-provider-factory*
              (fn [_]
                (swap! created inc)
                delegate)]
      (let [provider (sut/init-embedding-provider
                       {:provider :default
                        :dir      "/tmp/test-db-root"})]
        (is (zero? @created))
        (is (= metadata
               (sut/embedding-metadata provider)))
        (is (= 1 @created))
        (is (= [4] (sut/embed-text provider "bear")))
        (is (= 1 @created))
        (is (= 1 (sut/embedding-dimensions provider)))
        (sut/close-provider provider)
        (is (true? @closed?))))))

(deftest default-model-download-test
  (let [dir      (u/tmp-dir (str "embed-model-" (System/nanoTime)))
        expected (str dir u/+separator+ "embed" u/+separator+ sut/default-model-file)
        calls    (atom [])]
    (try
      (binding [sut/*download-default-model!*
                (fn [url target]
                  (swap! calls conj [url target])
                  (.mkdirs (.getParentFile (io/file target)))
                  (spit target "fake-gguf")
                  target)]
        (is (= expected (#'datalevin.embedding/ensure-default-model! {:dir dir})))
        (is (= [[sut/default-model-url expected]] @calls))
        (is (.exists (io/file expected)))
        (is (= expected (#'datalevin.embedding/ensure-default-model! {:dir dir})))
        (is (= 1 (count @calls))))
      (finally
        (u/delete-files dir)))))

(deftest init-provider-validation-test
  (testing "unknown provider"
    (is (thrown-with-msg?
          clojure.lang.ExceptionInfo
          #"Unknown embedding provider"
          (sut/init-embedding-provider {:provider :bogus
                                        :model    "/tmp/model.gguf"}))))
  (testing "metadata dimensions must match provider output"
    (let [dir        (u/tmp-dir (str "embed-meta-mismatch-" (System/nanoTime)))
          model-path (str dir u/+separator+ "model-Q8_0.gguf")]
      (try
        (.mkdirs (io/file dir))
        (spit model-path "fake-gguf")
        (is (thrown-with-msg?
              clojure.lang.ExceptionInfo
              #"dimensions do not match provider output"
              (#'datalevin.embedding/llama-provider-metadata
                {:provider           :llama.cpp
                 :embedding-metadata {:embedding/output {:dimensions 3}}}
                model-path
                4)))
        (finally
          (io/delete-file (io/file dir) true))))))

(deftest default-model-metadata-test
  (let [dir           (u/tmp-dir (str "embed-meta-" (System/nanoTime)))
        model-path    (str dir u/+separator+ sut/default-model-file)
        manifest-path (str model-path ".edn")]
    (try
      (.mkdirs (io/file dir))
      (spit model-path "fake-gguf")
      (spit manifest-path
            (pr-str {:embedding/provider {:revision "hf:test-revision"}
                     :embedding/output   {:max-tokens 512}}))
      (let [metadata (#'datalevin.embedding/llama-provider-metadata
                       {:provider           :default
                        :embedding-metadata {:embedding/output
                                             {:document-prefix "doc: "}}}
                       model-path
                       384)]
        (is (= :local
               (get-in metadata [:embedding/provider :kind])))
        (is (= :default
               (get-in metadata [:embedding/provider :id])))
        (is (= sut/default-model-id
               (get-in metadata [:embedding/provider :model-id])))
        (is (= "hf:test-revision"
               (get-in metadata [:embedding/provider :revision])))
        (is (= 384
               (get-in metadata [:embedding/output :dimensions])))
        (is (= :mean
               (get-in metadata [:embedding/output :pooling])))
        (is (= true
               (get-in metadata [:embedding/output :normalize?])))
        (is (= "query: "
               (get-in metadata [:embedding/output :query-prefix])))
        (is (= "doc: "
               (get-in metadata [:embedding/output :document-prefix])))
        (is (= 512
               (get-in metadata [:embedding/output :max-tokens])))
        (is (= :gguf
               (get-in metadata [:embedding/artifact :format])))
        (is (= sut/default-model-file
               (get-in metadata [:embedding/artifact :file])))
        (is (= :q8_0
               (get-in metadata [:embedding/artifact :quantization])))
        (is (= 9
               (get-in metadata [:embedding/artifact :bytes])))
        (is (re-matches #"(?i)[0-9a-f]{64}"
                        (get-in metadata [:embedding/artifact :sha256]))))
      (finally
        (io/delete-file (io/file dir) true)))))

(deftest llama-provider-integration-test
  (when-not (u/windows?)
    (let [dir (u/tmp-dir (str "embed-integration-" (System/nanoTime)))]
      (try
        (.mkdirs (io/file dir))
        (when-let [provider-spec (integration-provider-spec! dir)]
          (with-open [provider (sut/init-embedding-provider provider-spec)]
            (let [dims  (sut/embedding-dimensions provider)
                  cat-1 (sut/embed-text provider "cat")
                  cat-2 (sut/embed-text provider "cat")
                  pair  (sut/embed-texts provider ["cat" "dog"])]
              (when (= dir (:dir provider-spec))
                (is (.exists (io/file dir "embed" sut/default-model-file))))
              (is (pos? dims))
              (is (= dims (alength ^floats cat-1)))
              (is (= dims (alength ^floats cat-2)))
              (is (= 2 (count pair)))
              (is (= dims (alength ^floats (first pair))))
              (is (= dims (alength ^floats (second pair))))
              (is (= (vec cat-1) (vec cat-2))))))
        (finally
          (io/delete-file (io/file dir) true))))))
