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
   [clojure.test :refer [deftest is testing]]
   [datalevin.embedding :as sut]
   [datalevin.embedding-test-support :as test-support]
   [datalevin.util :as u]))

;; Integration test usage:
;; * DTLV_TEST_EMBED_CACHE_DIR overrides the cache location
;; * DTLV_SKIP_REAL_EMBED_TEST=1 skips the real default-model integration test
;; * DTLV_TEST_LLAMA_EMBED_MODEL=/path/model.gguf uses an explicit model path

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

(deftest metadata-compatibility-test
  (testing "provider identity mismatch is rejected"
    (is (thrown-with-msg?
          clojure.lang.ExceptionInfo
          #"Embedding metadata does not match the runtime provider"
          (sut/ensure-compatible-metadata
            {:embedding/provider {:id :test
                                  :model-id "model-a"}
             :embedding/output   {:dimensions 1}}
            {:embedding/provider {:id :test
                                  :model-id "model-b"}
             :embedding/output   {:dimensions 1}}))))
  (testing "missing metadata fields are rejected"
    (is (thrown-with-msg?
          clojure.lang.ExceptionInfo
          #"Embedding metadata does not match the runtime provider"
          (sut/ensure-compatible-metadata
            {:embedding/provider {:id :test}
             :embedding/output   {:dimensions 1
                                  :query-prefix "query: "
                                  :document-prefix "doc: "}}
            {:embedding/provider {:id :test}
             :embedding/output   {:dimensions 1}})))))

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
        (when-let [provider-spec (test-support/integration-provider-spec! dir)]
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
