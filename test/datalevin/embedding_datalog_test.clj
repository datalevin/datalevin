;;
;; Copyright (c) Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns datalevin.embedding-datalog-test
  (:require
   [clojure.string :as s]
   [clojure.test :refer [deftest is testing]]
   [datalevin.core :as d]
   [datalevin.embedding :as emb]
   [datalevin.util :as u])
  (:import
   [java.util UUID]))

(def ^:private embedding-schema
  {:doc/id   {:db/valueType :db.type/string
              :db/unique    :db.unique/identity}
   :doc/text {:db/valueType              :db.type/string
              :db/embedding              true
              :db.embedding/domains      ["docs"]
              :db.embedding/autoDomain   true
              :db/fulltext               true
              :db.fulltext/autoDomain    true}
   :doc/tag  {:db/valueType   :db.type/string
              :db/cardinality :db.cardinality/many
              :db/embedding   true}})

(def ^:private provider-metadata
  {:embedding/provider {:id :test
                        :model-id "test/mock"}
   :embedding/output   {:dimensions 3
                        :normalize? false}})

(defn- mock-embedding
  [text]
  (let [text    (s/lower-case text)
        cat     (+ (if (re-find #"cat|kitten|feline" text) 1.0 0.0)
                   (if (re-find #"pet|animal" text) 0.4 0.0))
        dog     (+ (if (re-find #"dog|canine|puppy" text) 1.0 0.0)
                   (if (re-find #"pet|animal" text) 0.1 0.0))
        science (if (re-find #"physics|quantum|science" text) 1.0 0.0)]
    (float-array [(float cat) (float dog) (float science)])))

(defn- mock-provider
  ([] (mock-provider provider-metadata))
  ([metadata]
   (reify
     emb/IEmbeddingProvider
     (embedding [_ items _opts]
       (mapv (fn [item]
               (mock-embedding (if (map? item) (:text item) item)))
             items))
     (embedding-metadata [_]
       metadata)
     (embedding-dimensions [_]
       3)
     (close-provider [_]
       nil)

     java.lang.AutoCloseable
     (close [_]
       nil))))

(def ^:private provider-opts
  {:embedding-opts      {:provider :test
                         :metric-type :cosine}
   :embedding-providers {:test (mock-provider)}})

(deftest embedding-neighbors-fns-test
  (let [dir  (u/tmp-dir (str "embedding-fns-" (UUID/randomUUID)))
        conn (d/create-conn dir embedding-schema provider-opts)]
    (try
      (d/transact! conn [{:doc/id   "cat-1"
                          :doc/text "red cat"
                          :doc/tag  ["pet cat" "feline friend"]}
                         {:doc/id   "cat-2"
                          :doc/text "kitten animal"
                          :doc/tag  ["small pet"]}
                         {:doc/id   "dog-1"
                          :doc/text "friendly dog"
                          :doc/tag  ["canine pal"]}
                         {:doc/id   "sci-1"
                          :doc/text "quantum physics"
                          :doc/tag  ["science"]}])
      (is (= #{"cat-1" "cat-2"}
             (set (d/q '[:find [?id ...]
                         :in $ ?q
                         :where
                         [(embedding-neighbors $ ?q {:domains ["docs"] :top 2})
                          [[?e _ _]]]
                         [?e :doc/id ?id]]
                       (d/db conn) "cat"))))
      (is (= "cat-1"
             (d/q '[:find ?id .
                    :in $ ?q
                    :where
                    [(embedding-neighbors $ :doc/text ?q {:top 1}) [[?e _ _]]]
                    [?e :doc/id ?id]]
                  (d/db conn) "cat")))
      (is (= #{"pet cat" "feline friend" "small pet"}
             (set (d/q '[:find [?v ...]
                         :in $ ?q
                         :where
                         [(embedding-neighbors $ ?q
                                               {:domains ["datalevin"] :top 3})
                          [[_ _ ?v]]]]
                       (d/db conn) "cat"))))
      (is (number? (d/q '[:find ?dist .
                          :in $ ?q
                          :where
                          [(embedding-neighbors $ :doc/text ?q
                                                {:top 1
                                                 :display :refs+dists})
                           [[_ _ _ ?dist]]]]
                        (d/db conn) "cat")))
      (is (= "red cat"
             (d/q '[:find ?v .
                    :in $ ?q
                    :where
                    [(fulltext $ :doc/text ?q) [[_ _ ?v]]]]
                  (d/db conn) "red")))
      (is (thrown-with-msg?
            Exception
            #":db.embedding/autoDomain"
            (d/q '[:find ?v .
                   :in $ ?q
                   :where
                   [(embedding-neighbors $ :doc/tag ?q) [[_ _ ?v]]]]
                 (d/db conn) "cat")))
      (finally
        (d/close conn)
        (u/delete-files dir)))))

(deftest embedding-retract-and-reopen-test
  (let [dir  (u/tmp-dir (str "embedding-reopen-" (UUID/randomUUID)))
        conn (d/create-conn dir embedding-schema provider-opts)]
    (try
      (d/transact! conn [{:doc/id   "doc-1"
                          :doc/text "red cat"
                          :doc/tag  ["pet cat"]}])
      (let [db0 (d/db conn)
            eid (d/q '[:find ?e .
                       :where [?e :doc/id "doc-1"]]
                     db0)]
        (d/transact! conn [[:db/retract eid :doc/tag "pet cat"]
                           [:db/add eid :doc/tag "canine pal"]])
        (is (not (contains?
                   (set (d/q '[:find [?v ...]
                               :in $ ?q
                               :where
                               [(embedding-neighbors $ ?q
                                                     {:domains ["datalevin"]
                                                      :top 3})
                                [[_ _ ?v]]]]
                             (d/db conn) "cat"))
                   "pet cat")))
        (is (= "canine pal"
               (d/q '[:find ?v .
                      :in $ ?q
                      :where
                      [(embedding-neighbors $ ?q
                                            {:domains ["datalevin"] :top 1})
                       [[_ _ ?v]]]]
                    (d/db conn) "dog"))))
      (d/close conn)
      (let [conn2 (d/create-conn dir embedding-schema provider-opts)]
        (try
          (is (= "canine pal"
                 (d/q '[:find ?v .
                        :in $ ?q
                        :where
                        [(embedding-neighbors $ ?q
                                              {:domains ["datalevin"] :top 1})
                         [[_ _ ?v]]]]
                      (d/db conn2) "dog")))
          (finally
            (d/close conn2))))
      (finally
        (u/delete-files dir)))))

(deftest embedding-provider-validation-test
  (testing "missing runtime provider"
    (let [dir (u/tmp-dir (str "embedding-missing-provider-" (UUID/randomUUID)))]
      (try
        (is (thrown-with-msg?
              Exception
              #"Embedding provider is not configured"
              (d/create-conn dir embedding-schema
                             {:embedding-opts {:provider :missing}})))
        (finally
          (u/delete-files dir)))))
  (testing "dimension mismatch"
    (let [dir (u/tmp-dir (str "embedding-dimension-mismatch-" (UUID/randomUUID)))]
      (try
        (is (thrown-with-msg?
              Exception
              #"dimensions do not match"
              (d/create-conn dir embedding-schema
                             {:embedding-domains {"docs" {:provider :test
                                                          :dimensions 2}}
                              :embedding-providers {:test (mock-provider)}})))
        (finally
          (u/delete-files dir)))))
  (testing "persisted metadata must match runtime provider identity"
    (let [dir (u/tmp-dir (str "embedding-metadata-mismatch-" (UUID/randomUUID)))]
      (try
        (let [conn (d/create-conn
                     dir
                     embedding-schema
                     {:embedding-opts      {:provider :test}
                      :embedding-providers {:test (mock-provider)}})]
          (d/close conn))
        (is (thrown-with-msg?
              Exception
              #"Embedding metadata does not match the runtime provider"
              (d/create-conn
                dir
                embedding-schema
                {:embedding-opts      {:provider :test}
                 :embedding-providers
                 {:test (mock-provider
                          (assoc-in provider-metadata
                                    [:embedding/provider :model-id]
                                    "test/other-model"))}})))
        (finally
          (u/delete-files dir))))))

(deftest embedding-schema-mutation-requires-rebuild-test
  (let [dir  (u/tmp-dir (str "embedding-schema-mutation-" (UUID/randomUUID)))
        conn (d/create-conn dir embedding-schema provider-opts)]
    (try
      (d/transact! conn [{:doc/id "doc-1"
                          :doc/text "red cat"}])
      (is (thrown-with-msg?
            Exception
            #"Embedding schema changes require an explicit rebuild"
            (d/update-schema conn
                             {:doc/text {:db/valueType :db.type/string}})))
      (finally
        (d/close conn)
        (u/delete-files dir)))))
