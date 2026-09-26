(ns datalevin.pull-many-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [datalevin.core :as d]
            [datalevin.interpret :as inter]
            [datalevin.server :as server]
            [datalevin.test.core :refer [allocate-port db-fixture]]
            [datalevin.util :as u]))

(use-fixtures :each db-fixture)

(defn- check-pull-many-values! [conn]
  (d/register-type conn :test/ranked
                   {:index {:type :long :order-fn (inter/inter-fn [v] (:rank v))}})
  (d/update-schema conn
                   {:tags {:db/valueType :db.type/long
                           :db/cardinality :db.cardinality/many}
                    :friend {:db/valueType :db.type/ref}
                    :part {:db/valueType :db.type/ref :db/isComponent true}
                    :payload {:db/valueType :db.type/string :db/noindex true}
                    :enabled {:db/valueType :db.type/boolean :db/noindex true}
                    :custom {:db/valueType :test/ranked}
                    :customs {:db/valueType :test/ranked
                              :db/cardinality :db.cardinality/many}})
  (d/transact! conn [{:db/id 20 :tags [3 1] :friend 21 :part 22
                     :data {:nested [false 1]} :enabled false
                     :custom {:rank 1 :label "custom"}
                     :customs [{:rank 2} {:rank 3}]
                     :payload (.repeat "x" 3000)}
                    {:db/id 21 :name "friend" :tags [1 3]}
                    {:db/id 22 :name "part"}])
  (let [ids [22 20 99 20 21]
        pattern [:db/id :name :payload :enabled :data :custom]]
    (is (= (mapv #(d/pull @conn pattern %) ids)
           (d/pull-many @conn pattern ids)))
    (d/with-transaction [tx conn]
      (d/transact! tx [[:db/add 20 :custom {:rank 4 :label "pending"}]])
      (is (= (mapv #(d/pull @tx pattern %) ids)
             (d/pull-many @tx pattern ids)))
      (d/abort-transact tx))))

(deftest local-and-remote-pull-many-values
  (let [root (u/tmp-dir (str "pull-many-" (random-uuid)))
        port (allocate-port)
        srv (server/create {:root (str root "/server") :port port})]
    (try
      (server/start srv)
      (doseq [remote? [false true] cache-limit [0 512]]
        (testing (str "remote=" remote? ", cache=" cache-limit)
          (let [name (str "pull-many-" cache-limit)
                path (if remote?
                       (str "dtlv://datalevin:datalevin@localhost:" port "/" name)
                       (str root "/" name))
                conn (d/create-conn path
                                    {:name {:db/valueType :db.type/string} :data {}}
                                    {:cache-limit cache-limit
                                     :client-opts {:pool-size 1}})]
            (try
              (check-pull-many-values! conn)
              (finally (d/close conn))))))
      (finally (server/stop srv) (u/delete-files root)))))
