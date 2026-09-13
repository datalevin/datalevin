(ns ^:no-doc datalevin.test-adapter.reference
  (:require
   [datalevin.core :as d]
   [datalevin.test-adapter :as adapter]))

(defrecord ReferenceBackend []
  adapter/Backend
  (invoke! [_ operation arguments]
    (case operation
      :empty-db (apply d/empty-db arguments)
      :db-with (apply d/db-with arguments)
      :q (apply d/q arguments)
      :close-db (apply d/close-db arguments)
      (adapter/unsupported! :adapter operation
                            "Operation is not exposed by the reference adapter")))
  (stop! [_])
  (backend-info [_] {:backend :reference :runtime :clojure}))

(defn start [] (->ReferenceBackend))
