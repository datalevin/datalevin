(ns datalevin.query.cache-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [datalevin.bits :as bits]
            [datalevin.core :as d]
            [datalevin.db :as db]
            [datalevin.interface :as i]
            [datalevin.test.core :refer [db-fixture]]
            [datalevin.udf :as udf]
            [datalevin.util :as u])
  (:import [datalevin.utl LRUCache]))

(use-fixtures :each db-fixture)

(defn- descriptor [kind]
  {:udf/lang :test :udf/kind kind :udf/id :query/cache})

(defn- registry [decode]
  (doto (udf/create-registry)
    (udf/register! (descriptor :order-fn) :rank)
    (udf/register! (descriptor :serializer) bits/serialize)
    (udf/register! (descriptor :deserializer) decode)))

(deftest query-results-follow-custom-codec-registry-and-generation
  (let [root (u/tmp-dir (str "query-codec-cache-" (random-uuid)))
        original (registry bits/deserialize)
        replacement (registry #(assoc (bits/deserialize %) :decoder :replacement))
        conn (d/create-conn root nil {:runtime-opts {:udf-registry original}
                                     :background-sampling? false})]
    (try
      (d/register-type conn :test/value
                       {:index {:type :long :order-fn (descriptor :order-fn)}
                        :payload {:serialize (descriptor :serializer)
                                  :deserialize (descriptor :deserializer)}})
      (d/update-schema conn {:task/value {:db/valueType :test/value}
                             :task/name {:db/valueType :db.type/string}})
      (d/transact! conn [{:db/id 1 :task/value {:rank 1} :task/name "one"}])
      (let [view @conn
            store (:store view)
            kv (d/datalog-kv conn)
            query '[:find ?value . :where [1 :task/value ?value]]
            projection '[:find (pull ?e [:task/value]) . :where [?e :task/name "one"]]
            prepared (d/prepare-q view query)
            prepared-projection (d/prepare-q view projection)
            reads [[:ordinary #(d/q query view) identity]
                   [:prepared #(prepared []) identity]
                   [:pull #(d/q projection view) #(hash-map :task/value %)]
                   [:prepared-pull #(prepared-projection []) #(hash-map :task/value %)]]
            check! (fn [value]
                     (doseq [[operation read wrap] reads]
                       (testing (name operation)
                         (dotimes [_ 2] (is (= (wrap value) (read)))))))
            before [(i/last-modified store) (:max-tx view) (i/schema store)]]
        (check! {:rank 1})
        (let [[^LRUCache cache] (db/cache-token store)]
          (is (= 2 (count (filter #(= :query-result (first %)) (.keys cache))))
              "ordinary and prepared queries share retained results"))
        (is (= (udf/generation original) (udf/generation replacement)))
        ;; The retained DB still carries the original runtime metadata. Custom
        ;; storage decoding follows the registry in the live KV info holder.
        (vswap! (i/kv-info kv) assoc-in [:runtime-opts :udf-registry] replacement)
        (check! {:rank 1 :decoder :replacement})
        (udf/register! replacement (descriptor :deserializer)
                       #(assoc (bits/deserialize %) :decoder :new))
        (is (= {:task/value {:rank 1 :decoder :new}}
               (d/pull view [:task/value] 1)))
        (check! {:rank 1 :decoder :new})
        (udf/unregister! replacement (descriptor :deserializer))
        (is (thrown-with-msg? Exception #"UDF|binding" (d/pull view [:task/value] 1)))
        (doseq [[operation read] reads]
          (testing (name operation)
            (is (thrown-with-msg? Exception #"UDF|binding" (read)))))
        (udf/register! replacement (descriptor :deserializer) bits/deserialize)
        (check! {:rank 1})
        (is (= before [(i/last-modified store) (:max-tx view) (i/schema store)])))
      (finally
        (d/close conn)
        (u/delete-files root)))))
