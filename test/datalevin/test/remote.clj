(ns datalevin.test.remote
  (:require
   [clojure.test :refer [deftest is use-fixtures]]
   [datalevin.client :as cl]
   [datalevin.constants :as c]
   [datalevin.core :as d]
   [datalevin.datom :as dd]
   [datalevin.test.core :refer [server-fixture]])
  (:import
   [java.util UUID]))

(use-fixtures :each server-fixture)

(defn- remote-uri
  [db-name]
  (str "dtlv://"
       c/default-username ":"
       c/default-password
       "@localhost:" cl/*default-port*
       "/" db-name))

(deftest remote-db-state-sync-optional-test
  (let [db-name (str "remote-state-sync-" (UUID/randomUUID))
        conn    (d/get-conn (remote-uri db-name)
                            {:user/email {:db/unique :db.unique/identity}})]
    (try
      (d/transact! conn [{:user/email "eva@example.com"}])
      (is (= "eva@example.com"
             (-> (d/entity @conn [:user/email "eva@example.com"])
                 d/touch
                 :user/email)))
      (is (= [(dd/datom 1 :user/email "eva@example.com")]
             (d/datoms @conn :eav)))

      (d/update-schema conn {:identifier {:db/valueType :db.type/long}})
      (d/transact! conn [{:identifier 1} {:identifier 2}])
      (is (= [(dd/datom 1 :user/email "eva@example.com")
              (dd/datom 2 :identifier 1)
              (dd/datom 3 :identifier 2)]
             (d/datoms @conn :eav)))
      (finally
        (d/close conn)))))
