(ns datalevin.storage.embedding-provider-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [datalevin.core :as d]
            [datalevin.embedding :as emb]
            [datalevin.test.core :refer [db-fixture]]
            [datalevin.util :as u]))

(use-fixtures :once db-fixture)

(def ^:private metadata
  {:embedding/provider {:id :test :model-id "test/provider-ownership"}
   :embedding/output {:dimensions 2 :normalize? false}})

(defn- observed-provider [events]
  (reify emb/IEmbeddingProvider
    (embedding [_ items _]
      (swap! events update :embeddings (fnil inc 0))
      (mapv (fn [_] (float-array [1 0])) items))
    (embedding-dimensions [_]
      (swap! events update :dimensions (fnil inc 0))
      2)
    (embedding-metadata [_] metadata)
    (close-provider [_] (swap! events update :closes (fnil inc 0)))))

(deftest supplied-providers-are-owned-before-first-use
  (doseq [use? [false true] shared? [false true] wal? [false true]]
    (testing (str {:use? use? :shared? shared? :wal? wal?})
      (let [path (u/tmp-dir (str "explicit-provider-ownership-" (random-uuid)))
            events (atom {})
            provider (observed-provider events)
            schema {:text (cond-> {:db/valueType :db.type/string :db/embedding true}
                            shared? (assoc :db.embedding/domains ["first" "second"]))}
            conn (d/create-conn path schema
                   {:wal? wal? :embedding-opts {:provider :custom}
                    :embedding-providers {:custom provider}})]
        (try
          (is (= (if shared? 2 1) (:dimensions @events)))
          (is (zero? (get @events :embeddings 0)))
          (is (zero? (get @events :closes 0)))
          (when use?
            (d/transact! conn [{:text "owned provider"}])
            (is (= (if shared? 2 1) (:embeddings @events))))
          (d/close conn)
          (is (= 1 (:closes @events)))
          (d/close conn)
          (is (= 1 (:closes @events)))
          (finally (d/close conn) (u/delete-files path)))))))

(deftest configured-providers-stay-lazy-and-close-after-use
  (doseq [provider-id [:default :openai-compatible] use? [false true]]
    (testing (str {:provider-id provider-id :use? use?})
      (let [path (u/tmp-dir (str "configured-provider-ownership-" (random-uuid)))
            created (atom 0)
            events (atom {})
            factory (fn [_]
                      (swap! created inc)
                      (observed-provider events))]
        (try
          (binding [emb/*llama-provider-factory* factory
                    emb/*openai-compatible-provider-factory* factory]
            (with-open [conn (d/create-conn path
                               {:text {:db/valueType :db.type/string :db/embedding true}}
                               {:embedding-opts {:provider provider-id}
                                :embedding-providers
                                {provider-id {:provider provider-id :model "test-model"
                                              :dimensions 2 :embedding-metadata metadata}}})]
              (is (zero? @created))
              (when use?
                (d/transact! conn [{:text "configured provider"}])
                (is (= 1 @created))
                (is (= 1 (:embeddings @events)))))
            (is (= (if use? 1 0) @created))
            (is (= (if use? 1 0) (get @events :closes 0))))
          (finally (u/delete-files path)))))))
