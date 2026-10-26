(ns datalevin.query.noindex-planning-test
  (:require [clojure.test :refer [deftest is]]
            [datalevin.core :as d]
            [datalevin.parser :as parser]
            [datalevin.query.optimizer.access-plan :as access-plan]
            [datalevin.query.source.fake :as fake]))

(deftest unindexed-native-reads-wait-for-entity-bindings
  (let [conn (d/create-conn nil {:body {:db/valueType :db.type/string
                                       :db/noindex true}}
                            {:kv-opts {:inmemory? true}})]
    (try
      (d/transact! conn [{:db/id 1 :body "wanted"} {:db/id 2 :body "other"}])
      (let [provider (fake/source (fake/row-info) [[1 :row/value "anchor"]] {:mode :scan})
            query '[:find ?e :in $n $s
                    :where [$n ?e :body "wanted"] [$s ?e :row/value _]]
            explanation (d/explain {:run? true} query @conn provider)]
        (is (= #{[1]} (d/q query @conn provider)))
        (is (= [:access :native]
               (mapv :kind (mapcat :operators (:regions explanation))))))
      (finally (d/close conn)))))

(deftest correlated-access-binds-entities-before-reading-unindexed-inputs
  (let [conn (d/create-conn nil {:body {:db/noindex true}}
                            {:kv-opts {:inmemory? true}})
        query '[:find ?text :where [(ground 1) ?e] [?e :body ?text]]]
    (try
      (let [schedule (#'access-plan/order-correlated-bindings
                       @conn (parser/parse-query query) [@conn]
                       {:expr {:source '$ :requires #{'?text}}})]
        (is (= '[[(ground 1) ?e] [?e :body ?text]]
               (mapv :orig-clause (:outer-joins schedule)))))
      (finally (d/close conn)))))

(deftest rules-propagate-unindexed-entity-requirements
  (let [conn (d/create-conn nil {:body {:db/valueType :db.type/string :db/noindex true}}
                            {:kv-opts {:inmemory? true}})]
    (try
      (d/transact! conn [{:db/id 1 :body "wanted"}])
      (doseq [rules ['[[(payload ?e ?text) [?e :body ?text]]]
                     '[[(body ?e ?text) [?e :body ?text]]
                       [(payload ?e ?text) (body ?e ?text)]]]]
        (let [provider (fake/source (fake/row-info) [[1 :row/value "anchor"]] {:mode :scan})
              query '[:find ?text :in $n $s %
                      :where ($n payload ?e ?text) [$s ?e :row/value _]]]
          (is (= #{["wanted"]} (d/q query @conn provider rules)))))
      (finally (d/close conn)))))

(deftest value-bindings-do-not-enable-unindexed-native-roots
  (let [conn (d/create-conn nil {:body {:db/valueType :db.type/string
                                       :db/noindex true}}
                            {:kv-opts {:inmemory? true}})]
    (try
      (d/transact! conn [{:db/id 1 :body "wanted"}])
      (let [provider (fake/source (fake/row-info) [[1 :row/value "wanted"]] {:mode :scan})
            query '[:find ?e :in $n $s
                    :where [$s _ :row/value ?body] [$n ?e :body ?body]]]
        (is (= :query-source/unsupported
               (try (d/q query @conn provider) nil
                    (catch clojure.lang.ExceptionInfo e (:error (ex-data e))))))
        (is (not-any? #(= :open (first %)) @(:events provider)))
        (d/index-attr conn :body)
        (is (= #{[1]} (d/q query @conn provider))))
      (finally (d/close conn)))))
