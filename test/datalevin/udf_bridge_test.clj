(ns datalevin.udf-bridge-test
  (:require
   [clojure.test :refer [deftest is]]
   [datalevin.core :as d]
   [datalevin.util :as u])
  (:import
   [datalevin DatabaseValue Datalevin UdfFunction]
   [java.util UUID]))

(deftest foreign-udf-receives-bridge-safe-database-value
  (let [dir        (u/tmp-dir (str "udf-bridge-" (UUID/randomUUID)))
        descriptor {:udf/lang :java
                    :udf/kind :tx-fn
                    :udf/id   :bridge/test}
        registry   (Datalevin/createUdfRegistry)
        db-value   (atom nil)
        function   (reify UdfFunction
                     (invoke [_ args]
                       (reset! db-value (first args))
                       []))
        conn       (d/create-conn
                     dir nil {:runtime-opts {:udf-registry registry}})]
    (try
      (Datalevin/registerUdf registry descriptor function)
      (d/transact! conn [[:db.fn/call descriptor]])
      (is (instance? DatabaseValue @db-value))
      (finally
        (d/close conn)
        (u/delete-files dir)))))
