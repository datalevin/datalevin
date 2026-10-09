(ns datalevin.remote-smoke-test
  "Small JVM smoke check; larger remote matrices live in dtlvtest."
  (:require [clojure.test :refer [deftest is use-fixtures]]
            [datalevin.core :as d]
            [datalevin.interpret :refer [inter-fn]]
            [datalevin.server :as server]
            [datalevin.test.core :refer [allocate-port db-fixture]]
            [datalevin.util :as u]))

(use-fixtures :each db-fixture)

(deftest remote-kv-and-datalog-public-operations
  (let [root (u/tmp-dir (str "remote-smoke-" (random-uuid)))
        port (allocate-port)
        srv (server/create {:root root :port port})
        uri (str "dtlv://datalevin:datalevin@localhost:" port "/")]
    (try
      (server/start srv)
      (let [db (d/open-kv (str uri "kv") {:wal? true})]
        (try
          (d/open-dbi db "counts")
          (d/transact-kv db "counts" [[:put 1 10]] :long :long)
          (is (= 10 (d/get-value db "counts" 1 :long :long)))
          (let [update! (d/prepare-update-kv db "counts"
                                          (inter-fn [old] (inc old)) :long :long)]
            (update! 1)
            (is (= 11 ((d/prepare-get-value db "counts" :long :long) 1))))
          (d/with-transaction-kv [tx db]
            (d/transact-kv tx "counts" [[:put 1 99]] :long :long)
            (is (= 99 (d/get-value tx "counts" 1 :long :long)))
            (d/abort-transact-kv tx))
          (is (= 11 (d/get-value db "counts" 1 :long :long)))
          (finally (d/close-kv db))))
      (let [conn (d/create-conn (str uri "datalog")
                                {:name {:db/valueType :db.type/string}}
                                {:wal? true})]
        (try
          (d/transact! conn [{:db/id 1 :name "remote"}])
          (is (= "remote" (d/q '[:find ?name . :where [1 :name ?name]] @conn)))
          (is (= {:name "remote"} (d/pull @conn [:name] 1)))
          (finally (d/close conn))))
      (finally (server/stop srv) (u/delete-files root)))))
